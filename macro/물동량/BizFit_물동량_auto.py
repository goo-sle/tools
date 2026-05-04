"""
BizFit 물동량 자동화 스크립트
========================================
실행 방법: python BizFit_Traffic_auto.py
필요 패키지:
    pip install playwright requests gspread google-auth pandas openpyxl xlrd
    playwright install chromium
"""

import os
import re
import sys
import time
import subprocess
import tempfile
import shutil
import requests
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# ============================================================
#  설정 영역 - 여기만 수정하세요
# ============================================================
CONFIG = {
    "bizfit_url":      "https://admin.bizfit.kr/v1/admin/pages/login/controller/",
    "bizfit_id":       "higherad",
    "bizfit_password": "hi1107",

    "save_dir":    os.path.join(os.path.expanduser("~"), "Downloads", "Traffic"),
    "headless":    True,
    # credentials JSON 하드코딩 (파일 불필요)


    # 구글 시트 ID
    "sheet_main_id":   "1iK66CbfSKQC8J6HBKuYt2jKJcLXrzdS15oQoA_eIsCY",
    "sheet_upload_id": "1sX5QAFULG5ZIxeRpS6jG4mAZWIeOmDiKs_3oPlq31tk",

    # 시트 탭 이름
    "tab_active":   "진행중 전체",
    "tab_finished": "저장하기 종료",
    "tab_upload":   "업로드",
}
# ============================================================


def log(msg):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {msg}")


def get_soffice():
    if sys.platform == "win32":
        paths = [
            r"C:\Program Files\LibreOffice\program\soffice.exe",
            r"C:\Program Files (x86)\LibreOffice\program\soffice.exe",
        ]
        for p in paths:
            if os.path.exists(p):
                return p
        return r"C:\Program Files\LibreOffice\program\soffice.exe"
    elif sys.platform == "darwin":
        return "/Applications/LibreOffice.app/Contents/MacOS/soffice"
    return "libreoffice"


# ============================================================
#  파일 읽기 (LibreOffice 변환 방식 - 2번 코드 스타일)
# ============================================================
def file_to_dataframe(file_path):
    """xls(HTML 포함) / xlsx 파일을 DataFrame으로 변환"""
    with open(file_path, "rb") as f:
        header = f.read(16)

    ext = os.path.splitext(file_path)[1].lower()
    is_html = b"<" in header[:4].lower()

    if is_html or ext == ".xls":
        tmp_dir = tempfile.mkdtemp()
        try:
            subprocess.run([
                get_soffice(), "--headless", "--convert-to", "xlsx",
                "--outdir", tmp_dir, file_path
            ], capture_output=True, timeout=30)
            base = os.path.splitext(os.path.basename(file_path))[0]
            xlsx_path = os.path.join(tmp_dir, base + ".xlsx")
            if not os.path.exists(xlsx_path):
                raise Exception(f"LibreOffice 변환 실패: {file_path}")
            df = pd.read_excel(xlsx_path, engine="openpyxl")
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)
    else:
        df = pd.read_excel(file_path, engine="openpyxl")

    return df


def file_to_list(file_path):
    """xls(HTML 포함) / xlsx 파일을 2차원 리스트로 변환 (구글 시트 업로드용)"""
    with open(file_path, "rb") as f:
        header = f.read(16)

    ext = os.path.splitext(file_path)[1].lower()
    is_html = b"<" in header[:4].lower()

    if is_html or ext == ".xls":
        tmp_dir = tempfile.mkdtemp()
        try:
            subprocess.run([
                get_soffice(), "--headless", "--convert-to", "xlsx",
                "--outdir", tmp_dir, file_path
            ], capture_output=True, timeout=30)
            base = os.path.splitext(os.path.basename(file_path))[0]
            xlsx_path = os.path.join(tmp_dir, base + ".xlsx")
            if not os.path.exists(xlsx_path):
                raise Exception(f"LibreOffice 변환 실패: {file_path}")
            df = pd.read_excel(xlsx_path, header=None, engine="openpyxl").fillna("")
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)
    else:
        df = pd.read_excel(file_path, header=None, engine="openpyxl").fillna("")

    data = []
    for _, row in df.iterrows():
        processed = []
        for val in row:
            if isinstance(val, float) and val == int(val):
                processed.append(int(val))
            else:
                processed.append(str(val) if val != "" else "")
        data.append(processed)
    return data


# ============================================================
#  구글 시트 업로드
# ============================================================
SERVICE_ACCOUNT_INFO = {
    "type": "service_account",
    "project_id": "higherad-b9d62",
    "private_key_id": "c97cf06c4ea8c5b5cf0e5800bd55e1817da03663",
    "private_key": (
        "-----BEGIN PRIVATE KEY-----\n"
        "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDziNhBf9nN8alK\n"
        "9W39n7g4JCKG6pi9/9Bc52mCcqcrgG2GBUuUtgSX5nSe23jJnU/V67P+bZcNr3Cu\n"
        "nMoyxPEj9iucfHhxcWDbINBFUa8wjWuOKiZagP3fBTg9WKeoW+L+Z3gbP84KPDOx\n"
        "YJUONeER8Co8O3IiI3IxxGo9uP6t2zyYO8mCUIo68uI7pyV49suyyTYWz1LBDoEy\n"
        "LCeKVFtVPmiBZoGso3Vx1U96gQqIFdqNrmRTbrk8/sC0GpUOzYBFVWc37uDWeCVc\n"
        "wE95S0UfxwhoaqyGt93HFEzlyq1HdmjBdx3R3/EhKbr7AnOi8fzat8BqmSU0J8eT\n"
        "rQkUlSv5AgMBAAECggEASpZW5Xiq1JB3MSYKEeuhGFC44mlnbomy30Fg5zsGSCSF\n"
        "Zs6oX1t//KXwgdbmH5m2oeYWso4N/XsGH/SVWQdIc6MpqDvXB6eZ6oMaRqDF7zDh\n"
        "CCGQrZdkKbIHj4JflwjNdO1rs6zPBgN6MZFLFZca38uWo+vxANOqXeOyRkUqe0RZ\n"
        "TNQ77NIbjRex+Au02n33SwzA9C7nEuaaQXcK09M0/PjHpqxYkDfDYSxXTqAM8ezJ\n"
        "iJaiGdRmlLdDAdf4384HZllRYCs1PXgpJh8fbnkiQs0tzq8pzqCMlN89G9FzRF/3\n"
        "DIGpEG/K+/WrCtYv3U+aQBlS0ntARwoY2B7b8cXQoQKBgQD8/veJzNpwnVlxHHjs\n"
        "SGI6JWPKpBFmNOs2HFl7pD261qfglku7h4rwmceleEmgNZNd+YRZCrtQfKSKfIww\n"
        "VeiGhZQAjDvhgUjVIOgao0Uy4VkNIffFbiBQIpCm8MfdHedf0CV6u/W3XnLlIkrP\n"
        "hzpPvewXdiNpSTGkcsdy/lyKGwKBgQD2bR4uWY4SsAag+np7OlbrBaQeZs5PSsO5\n"
        "lfWdK2TGCkyZrdWA6JYTRQcgvKNsIjnltLXlhqddNRr0fm5SDZh2EjBQaJrzYuPt\n"
        "y0yOCxLQ5IvEbzLR5sUe+FYdznYFCwJ+BOZugxQ4LqgNpujjbQtZPtQxhzUct7X1\n"
        "zDamgYmDewKBgQCZXjNPnRja5fhfooQHsQWi/CGXqYhGrlPcdKkmU/V7+z6/3jzA\n"
        "zTVED+VAgUAY2AGjGWzK0b+l1jmlHkWZ06pnSjjjcB+o38f4M7+gzlNXudZTKMFc\n"
        "NRtvmNSZ7yMp/0PRCIx/78vQQnhiQTyau/50cszZmCt1WwK2D0Krilks+wKBgCOt\n"
        "CIGNVZQ/B7amjLTqbUr5NhlwqM2x9UQZAcYPUjeZph1ZnV9cTN3dUHrc1IwDKH6o\n"
        "+uyP4gsMdSqQY0hdz4TIfVYmzsgNuRHkLOEjmUXE0LdPofvhfQhOy6jlCxEP1vyH\n"
        "mRTGxVac6pePYogKcWoqPm4tNPNDZYSAXCke99mhAoGBAOpZCW/ug+D0la8enX5+\n"
        "Js8O7UBH0xAa6XmR1MhfaHZtZJuYxHXFoVTs8LDWVO8+DYbcGrYcM2GKbqaFsvMo\n"
        "n3TJV15cxLgyc671btqx+5MCNIvG2rkSymkEX9cEmSY9Osk2+McQqNZvv80xnVxJ\n"
        "/FAMml7/9NT9oZOII1eY1B8k\n"
        "-----END PRIVATE KEY-----\n"
    ),
    "client_email": "higherad-sheets@higherad-b9d62.iam.gserviceaccount.com",
    "client_id": "114348474204615774567",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/higherad-sheets%40higherad-b9d62.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com",
}


def get_gspread_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    try:
        creds = Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=scopes)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        raise RuntimeError(f"❌ 구글 인증 실패: {e}") from e


def upload_to_sheet(gc, spreadsheet_id, tab_name, file_path=None, df=None):
    """파일 경로 또는 DataFrame을 구글 시트에 업로드"""
    try:
        sh = gc.open_by_key(spreadsheet_id)
        try:
            ws = sh.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title=tab_name, rows=5000, cols=50)

        ws.clear()

        if file_path:
            data = file_to_list(file_path)
        elif df is not None:
            # DataFrame → 2차원 리스트 변환
            df_copy = df.copy().fillna("")
            for col in df_copy.columns:
                if pd.api.types.is_numeric_dtype(df_copy[col]):
                    df_copy[col] = df_copy[col].apply(
                        lambda x: int(x) if pd.notnull(x) and x != "" and float(x) == int(float(x)) else x
                    )
            raw = [df_copy.columns.tolist()] + df_copy.values.tolist()
            data = []
            for row in raw:
                new_row = []
                for v in row:
                    if isinstance(v, float) and v == int(v):
                        new_row.append(str(int(v)))
                    else:
                        new_row.append(str(v))
                data.append(new_row)
        else:
            log(f"❌ 업로드 데이터 없음: [{tab_name}]")
            return

        if data:
            ws.update(data, "A1")
            log(f"✅ 구글 시트 업로드 완료: [{tab_name}] {len(data)-1}행")
        else:
            log(f"⚠️ 데이터가 비어있음: [{tab_name}]")

    except PermissionError as e:
        log(f"❌ 구글 시트 업로드 실패 [{tab_name}]: 권한 없음 또는 프로젝트 삭제됨")
        log(f"   → GCP 프로젝트가 삭제된 경우 새 서비스 계정 키(JSON)를 발급받아 교체하세요.")
        log(f"   → 시트 ID '{spreadsheet_id}' 에 서비스 계정 이메일을 편집자로 공유했는지 확인하세요.")
        import traceback
        traceback.print_exc()
    except Exception as e:
        log(f"❌ 구글 시트 업로드 실패 [{tab_name}]: {e}")


# ============================================================
#  데이터 처리 함수
# ============================================================
def process_filter(df):
    """(주)엠제이티, 제이커브인터렉티브 행 삭제"""
    remove_list = ["(주)엠제이티", "제이커브인터렉티브"]
    target_col = df.columns[2]
    before_len = len(df)
    # regex=False 사용 불가(여러 패턴) → re.escape로 특수문자 처리 후 정규식 사용
    pattern = '|'.join(re.escape(s) for s in remove_list)
    df_filtered = df[
        ~df[target_col].astype(str).str.contains(pattern, na=False, regex=True)
    ]
    log(f"🧹 필터링: {before_len}행 → {len(df_filtered)}행 (삭제: {before_len - len(df_filtered)}행)")
    return df_filtered.reset_index(drop=True)


def process_replace(df):
    """C열 (주)엠제이티 → 제이커브인터렉티브 치환"""
    target_col = df.columns[2]
    before_count = df[target_col].astype(str).str.contains("(주)엠제이티", regex=False).sum()
    df[target_col] = df[target_col].astype(str).str.replace("(주)엠제이티", "제이커브인터렉티브", regex=False)
    log(f"✏️ 텍스트 치환: {before_count}건")
    return df


# ============================================================
#  BizFit URL 추출 및 파일 다운로드
# ============================================================
def extract_url(onclick):
    match = re.search(r"location\.href\s*=\s*['\"]([^'\"]+)['\"]", onclick)
    if not match:
        raise Exception(f"URL 추출 실패. onclick: {onclick}")
    url = match.group(1)
    if url.startswith("/"):
        url = "https://admin.bizfit.kr" + url
    return url


def download_file(url, cookies, save_path):
    session = requests.Session()
    for c in cookies:
        session.cookies.set(c["name"], c["value"])
    resp = session.get(url, stream=True, timeout=30)
    resp.raise_for_status()
    with open(save_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
    log(f"💾 다운로드 완료: {os.path.basename(save_path)} ({os.path.getsize(save_path):,} bytes)")
    return save_path


def get_frame(page):
    if page.locator("button.status").count() > 0:
        return page
    for frame in page.frames:
        try:
            if frame.locator("button.status").count() > 0:
                return frame
        except:
            pass
    return page


# ============================================================
#  BizFit 메인 실행
# ============================================================
def run_bizfit():
    if not os.path.exists(CONFIG["save_dir"]):
        os.makedirs(CONFIG["save_dir"])

    today = datetime.now().strftime("%Y%m%d")
    try:
        gc = get_gspread_client()
        log("✅ 구글 시트 연결 완료")
    except (FileNotFoundError, RuntimeError) as e:
        log(str(e))
        raise

    with sync_playwright() as p:
        log("[BizFit] 브라우저 실행...")
        browser = p.chromium.launch(headless=CONFIG["headless"])
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        # ── 1. 로그인 ──
        log("[BizFit] 로그인 중...")
        page.goto(CONFIG["bizfit_url"], wait_until="networkidle")
        page.wait_for_selector("#aid", state="visible", timeout=10_000)
        time.sleep(0.5)
        page.locator("#aid").click()
        page.locator("#aid").fill(CONFIG["bizfit_id"])
        page.locator("input[type='password']").click()
        page.locator("input[type='password']").fill(CONFIG["bizfit_password"])
        time.sleep(0.3)
        page.locator("input[type='button'][value='LOGIN']").click()
        page.wait_for_load_state("networkidle")
        time.sleep(1.5)
        log("[BizFit] 로그인 완료")

        # ── 2. 캠페인 관리 이동 ──
        log("[BizFit] 캠페인 관리 이동...")
        page.locator("a[href*='ad_manage2.php']").first.click()
        page.wait_for_load_state("networkidle")
        time.sleep(1.5)
        frame = get_frame(page)

        # ── 3. 진행중(3) 선택 ──
        try:
            frame.wait_for_selector("button.status[data-val='3']", state="attached", timeout=8_000)
            frame.locator("button.status[data-val='3']").scroll_into_view_if_needed()
            frame.locator("button.status[data-val='3']").click()
            page.wait_for_load_state("networkidle")
            time.sleep(1)
            log("[BizFit] 진행중 선택 완료")
        except:
            log("[BizFit] 진행중 이미 선택됨 (스킵)")
            time.sleep(0.5)

        frame = get_frame(page)

        # ── 4. [지사] 엑셀 URL 수집 ──
        log("[BizFit] [지사] 엑셀 URL 수집...")
        jis_excel_url = extract_url(
            frame.locator("button.btn-outline-green", has_text="엑셀다운").get_attribute("onclick")
        )
        log(f"  지사 URL: {jis_excel_url}")

        # ── 5. [제이커브] 영업점 검색 후 URL 수집 ──
        log("[BizFit] [제이커브] 영업점 검색...")
        frame.wait_for_selector("select#category", state="attached", timeout=8_000)
        frame.locator("select#category").select_option(label="영업점")
        frame.locator("input[name='keyword']").fill("제이커브인터렉티브")
        frame.locator("button[type='submit']", has_text="조회하기").click()
        page.wait_for_load_state("networkidle")
        time.sleep(1)
        frame = get_frame(page)
        jc_excel_url = extract_url(
            frame.locator("button.btn-outline-green", has_text="엑셀다운").get_attribute("onclick")
        )
        log(f"  제이커브 URL: {jc_excel_url}")

        # ── 6. [하이어애드] 지사 검색 후 URL 수집 ──
        log("[BizFit] [하이어애드] 지사 검색...")
        frame.locator("select#category").select_option(label="지사")
        frame.locator("input[name='keyword']").fill("higherad@naver.com")
        frame.locator("button[type='submit']", has_text="조회하기").click()
        page.wait_for_load_state("networkidle")
        time.sleep(1)
        frame = get_frame(page)
        ha_excel_url = extract_url(
            frame.locator("button.btn-outline-green", has_text="엑셀다운").get_attribute("onclick")
        )
        log(f"  하이어애드 URL: {ha_excel_url}")

        # ── 7. [NP 위치저장] 종료(5) URL 수집 ──
        log("[BizFit] [NP 위치저장] 종료 탭 이동...")
        page.locator("a[href*='ad_manage2.php']").first.click()
        page.wait_for_load_state("networkidle")
        time.sleep(1.5)
        frame = get_frame(page)

        np_btn = frame.locator("button.btn-outline-blue", has_text="NP 위치저장")
        np_btn.wait_for(state="visible", timeout=8_000)
        np_btn.click()
        page.wait_for_load_state("networkidle")
        time.sleep(1)
        frame = get_frame(page)

        end_btn = frame.locator("button.status[data-val='5']")
        end_btn.scroll_into_view_if_needed()
        end_btn.click()
        page.wait_for_load_state("networkidle")
        time.sleep(1)
        frame = get_frame(page)
        np_excel_url = extract_url(
            frame.locator("button.btn-outline-green", has_text="엑셀다운").get_attribute("onclick")
        )
        log(f"  NP 위치저장 종료 URL: {np_excel_url}")

        # ── 8. 쿠키 수집 후 브라우저 종료 ──
        cookies = context.cookies()
        browser.close()

    # ── 9. 파일 다운로드 ──
    log("=" * 40)
    log("파일 다운로드 시작...")
    save = CONFIG["save_dir"]

    jis_path = download_file(jis_excel_url, cookies, os.path.join(save, f"지사_진행중_{today}.xls"))
    jc_path  = download_file(jc_excel_url,  cookies, os.path.join(save, f"제이커브_물동량_{today}.xls"))
    ha_path  = download_file(ha_excel_url,  cookies, os.path.join(save, f"하이어애드_물동량_{today}.xls"))
    np_path  = download_file(np_excel_url,  cookies, os.path.join(save, f"NP_위치저장_종료_{today}.xls"))

    # ── 10. 데이터 가공 ──
    log("=" * 40)
    log("데이터 가공 시작...")

    df_active_list = []

    df_jis = file_to_dataframe(jis_path)
    if df_jis is not None:
        df_jis = process_filter(df_jis)
        df_active_list.append(df_jis)

    df_jc = file_to_dataframe(jc_path)
    if df_jc is not None:
        df_jc = process_replace(df_jc)
        df_active_list.append(df_jc)

    df_ha = file_to_dataframe(ha_path)
    df_np = file_to_dataframe(np_path)

    # ── 11. 구글 시트 업로드 ──
    log("=" * 40)
    log("구글 시트 업로드 시작...")

    if df_active_list:
        df_combined = pd.concat(df_active_list, ignore_index=True)
        upload_to_sheet(gc, CONFIG["sheet_main_id"], CONFIG["tab_active"], df=df_combined)

    if df_ha is not None:
        upload_to_sheet(gc, CONFIG["sheet_upload_id"], CONFIG["tab_upload"], df=df_ha)

    if df_np is not None:
        upload_to_sheet(gc, CONFIG["sheet_main_id"], CONFIG["tab_finished"], df=df_np)

    log("✅ 구글 시트 업로드 완료!")


# ============================================================
#  메인 실행
# ============================================================
if __name__ == "__main__":
    try:
        log("=" * 40)
        log("▶ BizFit 물동량 자동화 시작")
        log("=" * 40)

        run_bizfit()

        log("")
        log("=" * 40)
        log("✅ 모든 작업 완료!")
        log("=" * 40)
        time.sleep(3)

    except PlaywrightTimeout as e:
        log(f"❌ 시간 초과: {e}")
        time.sleep(10)
    except Exception as e:
        log(f"❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        time.sleep(10)
        raise