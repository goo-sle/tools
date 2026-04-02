/**
 * FIREBASE-CONFIG.JS — Firebase 공통 초기화 모듈
 * index.html / login.html 에서 import해서 사용
 */

import { initializeApp } from "https://www.gstatic.com/firebasejs/10.10.0/firebase-app.js";
import { getAuth }        from "https://www.gstatic.com/firebasejs/10.10.0/firebase-auth.js";
import { getDatabase }    from "https://www.gstatic.com/firebasejs/10.10.0/firebase-database.js";
import { firebaseConfig } from './config.js';

export const app  = initializeApp(firebaseConfig);
export const auth = getAuth(app);
export const db   = getDatabase(app);
