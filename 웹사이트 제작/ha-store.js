/**
 * HA-STORE.JS — Higher Ad 공유 데이터 레이어
 * localStorage 기반 상태 관리. 백엔드 연동 시 fetch() 호출로 교체할 지점들을 [API] 주석으로 표시.
 */

const HA = {

  // ─── 슬롯 타입 정의 ─────────────────────────────────────────
  SLOT_TYPES: ['땡초','텍사스','유토피아','갤럭시','메리','퍼블릭','리무진','이슬'],

  // ─── 슬롯 CRUD ──────────────────────────────────────────────

  getSlots() {
    // [API] GET /api/slots
    return JSON.parse(localStorage.getItem('ha_slots') || '[]');
  },

  saveSlots(slots) {
    // [API] 불필요 (서버가 관리)
    localStorage.setItem('ha_slots', JSON.stringify(slots));
    this._dispatch('ha:slots:updated');
  },

  addSlot(data) {
    // [API] POST /api/slots
    const slots = this.getSlots();
    const newSlot = {
      id: Date.now(),
      status: 'pending',          // pending | active | rejected | expired
      createdAt: new Date().toISOString(),
      agencyId: data.agencyId,
      userId: data.userId,
      slotType: data.slotType,
      startDate: data.startDate,
      endDate: data.endDate,
      storeName: data.storeName,
      rankKeyword: data.rankKeyword,
      url: data.url,
      mid: data.mid || '',
      compareUrl: data.compareUrl || '',
      compareMid: data.compareMid || '',
      workKeyword: data.workKeyword || '',
      sellerControl: data.sellerControl || '',
      memo: data.memo || '',
      rank: null,
      inflow: 0,
    };
    slots.unshift(newSlot);
    this.saveSlots(slots);
    return newSlot;
  },

  updateSlot(id, patch) {
    // [API] PATCH /api/slots/:id
    const slots = this.getSlots().map(s => s.id === id ? { ...s, ...patch } : s);
    this.saveSlots(slots);
  },

  deleteSlot(id) {
    // [API] DELETE /api/slots/:id
    const slots = this.getSlots().filter(s => s.id !== id);
    this.saveSlots(slots);
  },

  approveSlot(id) {
    this.updateSlot(id, { status: 'active' });
  },

  rejectSlot(id, reason = '') {
    this.updateSlot(id, { status: 'rejected', rejectReason: reason });
  },

  // ─── 회원 CRUD ──────────────────────────────────────────────

  getUsers() {
    // [API] GET /api/users
    const def = [
      { id: 1, username: 'higher', password: 'test1234', agency: 'had1104',
        role: 'member', unitPrice: 50000, memo: '하이어애드 테스트 계정', createdAt: '2026-01-08' }
    ];
    return JSON.parse(localStorage.getItem('ha_users') || JSON.stringify(def));
  },

  saveUsers(users) {
    localStorage.setItem('ha_users', JSON.stringify(users));
    this._dispatch('ha:users:updated');
  },

  addUser(data) {
    // [API] POST /api/users
    const users = this.getUsers();
    const newUser = {
      id: Date.now(),
      username: data.username,
      password: data.password,
      agency: data.agency,
      role: 'member',
      unitPrice: Number(data.unitPrice) || 0,
      memo: data.memo || '',
      createdAt: new Date().toISOString().slice(0,10),
    };
    users.push(newUser);
    this.saveUsers(users);
    return newUser;
  },

  updateUser(id, patch) {
    // [API] PATCH /api/users/:id
    const users = this.getUsers().map(u => u.id === id ? { ...u, ...patch } : u);
    this.saveUsers(users);
  },

  deleteUser(id) {
    // [API] DELETE /api/users/:id
    const users = this.getUsers().filter(u => u.id !== id);
    this.saveUsers(users);
  },

  // ─── 공지사항 CRUD ──────────────────────────────────────────

  getNotices() {
    // [API] GET /api/notices
    const def = [
      { id: 18, title: '3월 결산 정산 일정 확인 바랍니다.', author: '관리자', date: '2026-03-18 11:27:28', views: 13, pinned: true, content: '3월 결산 정산은 3/31까지 완료 예정입니다. 미정산 내역 확인 바랍니다.' },
      { id: 15, title: '신규 서버 증설 작업 안내 (03/20)', author: '관리자', date: '2026-03-17 13:36:34', views: 73, pinned: false, content: '03/20 새벽 2~4시 서버 점검이 있습니다. 서비스 일시 중단이 있을 수 있습니다.' },
      { id: 14, title: '회원사 등급 산정 기준 변경 공지', author: '관리자', date: '2026-03-15 11:42:51', views: 32, pinned: false, content: '2026년 2분기부터 회원사 등급 산정 기준이 변경됩니다. 자세한 내용은 첨부 문서를 참고해주세요.' },
      { id: 13, title: '슬롯 대량 신청 시 가이드 준수 요청', author: '관리자', date: '2026-03-12 18:38:13', views: 44, pinned: false, content: '슬롯 10개 이상 대량 신청 시 반드시 가이드라인을 준수해주세요.' },
      { id: 3, title: '슬롯 세팅 방법 (모든 슬롯 동일)', author: '관리자', date: '2026-01-08 18:02:24', views: 141, pinned: false, content: '슬롯 세팅 방법 안내입니다.' },
      { id: 2, title: '대행사 오픈 안내', author: '관리자', date: '2026-01-08 17:43:19', views: 62, pinned: false, content: '하이어애드 대행사 플랫폼이 오픈했습니다.' },
      { id: 1, title: '☆ 필독 공지사항 ☆', author: '관리자', date: '2026-01-08 17:42:55', views: 50, pinned: true, content: '반드시 읽어주세요.' },
    ];
    return JSON.parse(localStorage.getItem('ha_notices') || JSON.stringify(def));
  },

  saveNotices(notices) {
    localStorage.setItem('ha_notices', JSON.stringify(notices));
    this._dispatch('ha:notices:updated');
  },

  addNotice(data) {
    const notices = this.getNotices();
    const n = {
      id: Date.now(),
      title: data.title,
      content: data.content || '',
      author: '관리자',
      date: new Date().toISOString().replace('T',' ').slice(0,19),
      views: 0,
      pinned: !!data.pinned,
    };
    notices.unshift(n);
    this.saveNotices(notices);
    return n;
  },

  updateNotice(id, patch) {
    const notices = this.getNotices().map(n => n.id === id ? { ...n, ...patch } : n);
    this.saveNotices(notices);
  },

  deleteNotice(id) {
    const notices = this.getNotices().filter(n => n.id !== id);
    this.saveNotices(notices);
  },

  // ─── 대시보드 집계 ──────────────────────────────────────────

  getDashboardStats() {
    const slots = this.getSlots();
    const today = new Date();
    const in3days = new Date(today); in3days.setDate(today.getDate() + 3);

    const active    = slots.filter(s => s.status === 'active');
    const pending   = slots.filter(s => s.status === 'pending');
    const rejected  = slots.filter(s => s.status === 'rejected');
    const thisMonth = slots.filter(s => {
      const d = new Date(s.endDate);
      return s.status === 'expired' && d.getMonth() === today.getMonth() && d.getFullYear() === today.getFullYear();
    });
    const expiringSoon = active.filter(s => {
      const d = new Date(s.endDate);
      return d <= in3days && d >= today;
    });

    const agencySet = new Set(active.map(s => s.agencyId));

    return {
      activeAgencies: agencySet.size,
      activeSlots: active.length,
      expiringSoon: expiringSoon.length,
      pending: pending.length,
      rejected: rejected.length,
      endedThisMonth: thisMonth.length,
    };
  },

  // ─── 정산 집계 ──────────────────────────────────────────────

  getSettlementByType() {
    const slots = this.getSlots();
    const users = this.getUsers();
    const result = {};

    this.SLOT_TYPES.forEach(t => {
      result[t] = { sold: 0, refund: 0, settled: 0 };
    });

    slots.filter(s => s.status === 'active' || s.status === 'expired').forEach(slot => {
      const user = users.find(u => u.username === slot.userId);
      const price = user ? user.unitPrice : 0;
      if (result[slot.slotType]) {
        result[slot.slotType].sold += price;
        result[slot.slotType].settled += price;
      }
    });

    return result;
  },

  // ─── 인증 (간단한 세션 시뮬레이션) ─────────────────────────

  getCurrentUser() {
    // [API] GET /api/me  (세션/토큰 검증)
    return JSON.parse(sessionStorage.getItem('ha_current_user') || 'null');
  },

  login(username, password) {
    // [API] POST /api/auth/login
    if (username === 'admin' && password === 'admin1234') {
      const user = { id: 0, username: 'admin', role: 'admin', agency: '-' };
      sessionStorage.setItem('ha_current_user', JSON.stringify(user));
      return { ok: true, user };
    }
    const users = this.getUsers();
    const found = users.find(u => u.username === username && u.password === password);
    if (found) {
      sessionStorage.setItem('ha_current_user', JSON.stringify(found));
      return { ok: true, user: found };
    }
    return { ok: false };
  },

  logout() {
    // [API] POST /api/auth/logout
    sessionStorage.removeItem('ha_current_user');
  },

  // ─── 내부 이벤트 버스 ────────────────────────────────────────
  _dispatch(event) {
    window.dispatchEvent(new CustomEvent(event));
  },

  // ─── 샘플 데이터 시드 ────────────────────────────────────────
  seedSampleData() {
    if (localStorage.getItem('ha_seeded')) return;
    const today = new Date();
    const fmt = d => d.toISOString().slice(0,10);
    const addDays = (d, n) => { const x = new Date(d); x.setDate(x.getDate()+n); return x; };

    const samples = [
      { agencyId:'had1104', userId:'higher', slotType:'이슬',    startDate: fmt(addDays(today,-10)), endDate: fmt(addDays(today, 8)),  storeName:'하연 음식물처리기', rankKeyword:'업소용음식물처리기', url:'https://smartstore.naver.com/test1', mid:'123456', rank:2,  inflow:120 },
      { agencyId:'had1104', userId:'higher', slotType:'이슬',    startDate: fmt(addDays(today,-10)), endDate: fmt(addDays(today, 8)),  storeName:'하연 음식물처리기', rankKeyword:'업소용음식물처리기', url:'https://smartstore.naver.com/test2', mid:'123457', rank:1,  inflow:80  },
      { agencyId:'had1104', userId:'higher', slotType:'갤럭시',  startDate: fmt(addDays(today,-5)),  endDate: fmt(addDays(today, 2)),  storeName:'갤럭시 전자', rankKeyword:'노트북추천', url:'https://smartstore.naver.com/test3', mid:'234567', rank:5,  inflow:44  },
      { agencyId:'had1104', userId:'higher', slotType:'텍사스',  startDate: fmt(addDays(today,-2)),  endDate: fmt(addDays(today, 1)),  storeName:'텍사스 BBQ', rankKeyword:'바베큐그릴', url:'https://smartstore.naver.com/test4', mid:'345678', rank:3,  inflow:33  },
      { agencyId:'had2200', userId:'test01', slotType:'땡초',    startDate: fmt(addDays(today,-15)), endDate: fmt(addDays(today,-1)),  storeName:'땡초마트', rankKeyword:'고추장', url:'https://smartstore.naver.com/test5', mid:'456789', rank:null, inflow:200 },
    ];

    samples.forEach((s,i) => {
      const slot = HA.addSlot(s);
      // 처음 4개는 active, 마지막은 expired
      const st = i < 4 ? 'active' : 'expired';
      HA.updateSlot(slot.id, { status: st });
    });

    // pending 샘플 1개
    HA.addSlot({ agencyId:'had3300', userId:'newuser', slotType:'메리', startDate: fmt(today), endDate: fmt(addDays(today,30)), storeName:'메리샵', rankKeyword:'여성의류', url:'https://smartstore.naver.com/merry' });

    // rejected 샘플
    const rj = HA.addSlot({ agencyId:'had1104', userId:'higher', slotType:'유토피아', startDate: fmt(today), endDate: fmt(addDays(today,30)), storeName:'유토샵', rankKeyword:'패션잡화', url:'https://smartstore.naver.com/utopia' });
    HA.rejectSlot(rj.id, 'URL 정보 불일치');

    localStorage.setItem('ha_seeded', '1');
  }
};

// 앱 로드 시 샘플 데이터 시드
HA.seedSampleData();
