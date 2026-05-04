"""
BizFit 물동량 자동화 Cloud Run 서버
1. BizFit 로그인 → 엑셀 4종 URL 수집 → 다운로드
2. 데이터 가공 (필터/치환)
3. 구글 시트 업로드 (ADC 인증)
4. GAS runProcessSequentially 호출 (GAS_WEBAPP_URL 환경변수 필요)

[GAS 준비 사항]
- runProcessSequentially 첫 줄을 아래로 변경:
    const ss = SpreadsheetApp.openById('1iK66CbfSKQC8J6HBKuYt2jKJcLXrzdS15oQoA_eIsCY');
- 아래 함수 추가 후 웹앱 배포 (Execute as: Me, Access: Anyone):
    function doPost(e) {
      runProcessSequentially();
      return ContentService.createTextOutput(JSON.stringify({status:'ok'}))
        .setMimeType(ContentService.MimeType.JSON);
    }
- Cloud Run 배포 시 --set-env-vars GAS_WEBAPP_URL=https://script.google.com/macros/s/.../exec
"""

import os
import io
import re
import time
import tempfile
import threading
import traceback

import requests
import pandas as pd
import google.auth
from google.auth.transport.requests import Request
import gspread

from flask import Flask, request, jsonify
from flask_cors import CORS
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

CONFIG = {
    "bizfit_url": "https://admin.bizfit.kr/v1/admin/pages/login/controller/",
    "bizfit_id":  os.environ.get("BIZFIT_ID", "higherad"),
    "bizfit_pw":  os.environ.get("BIZFIT_PW", "hi1107"),
    "sheet_main_id":   "1iK66CbfSKQC8J6HBKuYt2jKJcLXrzdS15oQoA_eIsCY",
    "sheet_upload_id": "1sX5QAFULG5ZIxeRpS6jG4mAZWIeOmDiKs_3oPlq31tk",
    "tab_active":   "진행중 전체",
    "tab_finished": "저장하기 종료",
    "tab_upload":   "업로드",
}

GAS_WEBAPP_URL = os.environ.get("GAS_WEBAPP_URL", "")

_lock = threading.Lock()
_pw_instance = None
_browser = None


# ── 브라우저 ────────────────────────────────────────────────

def get_browser():
    global _pw_instance, _browser
    if _browser is None or not _browser.is_connected():
        if _pw_instance:
            try: _pw_instance.stop()
            except Exception: pass
        _pw_instance = sync_playwright().start()
        _browser = _pw_instance.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox",
                  "--disable-dev-shm-usage", "--disable-gpu"],
        )
    return _browser


# ── 구글 시트 ────────────────────────────────────────────────

def get_gc():
    creds, _ = google.auth.default(scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ])
    if hasattr(creds, "expired") and creds.expired:
        creds.refresh(Request())
    return gspread.authorize(creds)


def upload_to_sheet(gc, spreadsheet_id, tab_name, df, log):
    try:
        sh = gc.open_by_key(spreadsheet_id)
        try:
            ws = sh.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title=tab_name, rows=5000, cols=50)
        ws.clear()

        df = df.copy().fillna("")
        raw = [df.columns.tolist()] + df.values.tolist()
        data = []
        for row in raw:
            processed = []
            for v in row:
                if isinstance(v, float) and v == int(v):
                    processed.append(str(int(v)))
                else:
                    processed.append(str(v) if str(v) != "nan" else "")
            data.append(processed)

        if data:
            ws.update(data, "A1")
            log(f"✅ 업로드 완료: [{tab_name}] {len(data)-1}행")
    except Exception as e:
        log(f"❌ 업로드 실패 [{tab_name}]: {e}")


# ── 파일 읽기 (LibreOffice 없이 pd.read_html 사용) ────────────

def read_xls_to_df(file_path):
    """HTML-based XLS 또는 XLSX → DataFrame"""
    with open(file_path, "rb") as f:
        header = f.read(8)

    is_html = b"<" in header[:5].lower()

    if is_html:
        for enc in ["utf-8", "cp949", "euc-kr"]:
            try:
                with open(file_path, "r", encoding=enc, errors="replace") as f:
                    content = f.read()
                tables = pd.read_html(io.StringIO(content))
                if tables:
                    df = max(tables, key=lambda x: x.shape[0] * x.shape[1])
                    return df.fillna("")
            except Exception:
                continue
        return None
    else:
        try:
            return pd.read_excel(file_path, engine="openpyxl").fillna("")
        except Exception:
            try:
                return pd.read_excel(file_path, engine="xlrd").fillna("")
            except Exception:
                return None


# ── 데이터 가공 ──────────────────────────────────────────────

def process_filter(df, log):
    remove_list = ["(주)엠제이티", "제이커브인터렉티브"]
    col = df.columns[2]
    pattern = "|".join(re.escape(s) for s in remove_list)
    before = len(df)
    df = df[~df[col].astype(str).str.contains(pattern, na=False, regex=True)].reset_index(drop=True)
    log(f"🧹 필터링: {before}행 → {len(df)}행")
    return df


def process_replace(df, log):
    col = df.columns[2]
    n = df[col].astype(str).str.contains("(주)엠제이티", regex=False).sum()
    df[col] = df[col].astype(str).str.replace("(주)엠제이티", "제이커브인터렉티브", regex=False)
    log(f"✏️ 텍스트 치환: {n}건")
    return df


# ── BizFit URL 추출 ──────────────────────────────────────────

def extract_url(onclick):
    m = re.search(r"location\.href\s*=\s*['\"]([^'\"]+)['\"]", onclick)
    if not m:
        raise Exception(f"URL 추출 실패: {onclick[:80]}")
    url = m.group(1)
    return ("https://admin.bizfit.kr" + url) if url.startswith("/") else url


def download_bytes(url, cookies):
    session = requests.Session()
    for c in cookies:
        session.cookies.set(c["name"], c["value"])
    resp = session.get(url, stream=True, timeout=30)
    resp.raise_for_status()
    return resp.content


def get_frame(page):
    if page.locator("button.status").count() > 0:
        return page
    for frame in page.frames:
        try:
            if frame.locator("button.status").count() > 0:
                return frame
        except Exception:
            pass
    return page


# ── 메인 실행 ────────────────────────────────────────────────

def run_bizfit(log):
    gc = get_gc()
    log("✅ 구글 시트 인증 완료")

    with _lock:
        browser = get_browser()
        context = browser.new_context(accept_downloads=True)
        page    = context.new_page()

        try:
            # 1. 로그인
            log("🔐 BizFit 로그인 중...")
            page.goto(CONFIG["bizfit_url"], wait_until="networkidle")
            page.wait_for_selector("#aid", state="visible", timeout=10000)
            time.sleep(0.5)
            page.locator("#aid").fill(CONFIG["bizfit_id"])
            page.locator("input[type='password']").fill(CONFIG["bizfit_pw"])
            time.sleep(0.3)
            page.locator("input[type='button'][value='LOGIN']").click()
            page.wait_for_load_state("networkidle")
            time.sleep(1.5)
            log("✅ 로그인 완료")

            # 2. 캠페인 관리 → 진행중
            log("📋 캠페인 관리 이동...")
            page.locator("a[href*='ad_manage2.php']").first.click()
            page.wait_for_load_state("networkidle")
            time.sleep(1.5)
            frame = get_frame(page)

            try:
                frame.wait_for_selector("button.status[data-val='3']", state="attached", timeout=8000)
                frame.locator("button.status[data-val='3']").click()
                page.wait_for_load_state("networkidle")
                time.sleep(1)
            except Exception:
                pass
            frame = get_frame(page)

            # 3. 지사 엑셀 URL
            log("📥 [지사] 엑셀 URL 수집...")
            jis_url = extract_url(frame.locator("button.btn-outline-green", has_text="엑셀다운").get_attribute("onclick"))

            # 4. 제이커브 엑셀 URL
            log("📥 [제이커브] 엑셀 URL 수집...")
            frame.wait_for_selector("select#category", state="attached", timeout=8000)
            frame.locator("select#category").select_option(label="영업점")
            frame.locator("input[name='keyword']").fill("제이커브인터렉티브")
            frame.locator("button[type='submit']", has_text="조회하기").click()
            page.wait_for_load_state("networkidle")
            time.sleep(1)
            frame = get_frame(page)
            jc_url = extract_url(frame.locator("button.btn-outline-green", has_text="엑셀다운").get_attribute("onclick"))

            # 5. 하이어애드 엑셀 URL
            log("📥 [하이어애드] 엑셀 URL 수집...")
            frame.locator("select#category").select_option(label="지사")
            frame.locator("input[name='keyword']").fill("higherad@naver.com")
            frame.locator("button[type='submit']", has_text="조회하기").click()
            page.wait_for_load_state("networkidle")
            time.sleep(1)
            frame = get_frame(page)
            ha_url = extract_url(frame.locator("button.btn-outline-green", has_text="엑셀다운").get_attribute("onclick"))

            # 6. NP 위치저장 종료 URL
            log("📥 [NP 위치저장] 종료 탭 URL 수집...")
            page.locator("a[href*='ad_manage2.php']").first.click()
            page.wait_for_load_state("networkidle")
            time.sleep(1.5)
            frame = get_frame(page)
            np_btn = frame.locator("button.btn-outline-blue", has_text="NP 위치저장")
            np_btn.wait_for(state="visible", timeout=8000)
            np_btn.click()
            page.wait_for_load_state("networkidle")
            time.sleep(1)
            frame = get_frame(page)
            frame.locator("button.status[data-val='5']").click()
            page.wait_for_load_state("networkidle")
            time.sleep(1)
            frame = get_frame(page)
            np_url = extract_url(frame.locator("button.btn-outline-green", has_text="엑셀다운").get_attribute("onclick"))

            cookies = context.cookies()

        finally:
            try: context.close()
            except Exception: pass

    # 7. 파일 다운로드 (메모리)
    log("⬇️ 파일 다운로드 중...")
    tmp = tempfile.mkdtemp()
    try:
        def save(name, url):
            path = os.path.join(tmp, name)
            with open(path, "wb") as f:
                f.write(download_bytes(url, cookies))
            log(f"  💾 {name} ({os.path.getsize(path):,} bytes)")
            return path

        jis_path = save("지사_진행중.xls",       jis_url)
        jc_path  = save("제이커브_물동량.xls",   jc_url)
        ha_path  = save("하이어애드_물동량.xls", ha_url)
        np_path  = save("NP_위치저장_종료.xls",  np_url)

        # 8. 가공
        log("⚙️ 데이터 가공 중...")
        df_jis = read_xls_to_df(jis_path)
        if df_jis is not None:
            df_jis = process_filter(df_jis, log)

        df_jc = read_xls_to_df(jc_path)
        if df_jc is not None:
            df_jc = process_replace(df_jc, log)

        df_ha = read_xls_to_df(ha_path)
        df_np = read_xls_to_df(np_path)

        # 9. 구글 시트 업로드
        log("📤 구글 시트 업로드 중...")
        parts = [d for d in [df_jis, df_jc] if d is not None]
        if parts:
            df_combined = pd.concat(parts, ignore_index=True)
            upload_to_sheet(gc, CONFIG["sheet_main_id"], CONFIG["tab_active"], df_combined, log)

        if df_ha is not None:
            upload_to_sheet(gc, CONFIG["sheet_upload_id"], CONFIG["tab_upload"], df_ha, log)

        if df_np is not None:
            upload_to_sheet(gc, CONFIG["sheet_main_id"], CONFIG["tab_finished"], df_np, log)

    finally:
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)

    log("✅ 구글 시트 업로드 완료!")


# ── Flask ────────────────────────────────────────────────────

app = Flask(__name__)
CORS(app, origins=[
    "https://higheradtool.kro.kr",
    "http://localhost",
    "http://127.0.0.1",
])


@app.route("/run", methods=["POST"])
def run():
    logs = []
    def log(msg):
        logs.append(msg)
        print(msg)

    try:
        log("▶ BizFit 물동량 자동화 시작")
        run_bizfit(log)

        # GAS 호출
        if GAS_WEBAPP_URL:
            log("🔄 GAS runProcessSequentially 호출 중...")
            try:
                r = requests.post(GAS_WEBAPP_URL, timeout=120, json={})
                if r.status_code == 200:
                    log("✅ GAS 실행 완료")
                else:
                    log(f"⚠️ GAS 응답: {r.status_code}")
            except Exception as e:
                log(f"⚠️ GAS 호출 실패: {e}")
        else:
            log("⚠️ GAS_WEBAPP_URL 미설정 — GAS 자동 실행 스킵")
            log("   (GAS에 doPost 추가 + Cloud Run 환경변수 설정 필요)")

        log("🏁 전체 완료!")
        return jsonify({"success": True, "logs": logs})

    except Exception as e:
        log(f"❌ 오류: {traceback.format_exc(limit=3)[-200:]}")
        return jsonify({"success": False, "logs": logs}), 500


@app.route("/ping",   methods=["GET"])
def ping():   return jsonify({"status": "ok"})

@app.route("/health", methods=["GET"])
def health(): return jsonify({"status": "ok"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"▶ macro-6-bizfit-traffic 서버 시작 (포트: {port})")
    app.run(host="0.0.0.0", port=port, threaded=False)
