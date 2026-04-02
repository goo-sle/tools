/**
 * FIREBASE-CONFIG.JS — Firebase 공통 초기화 모듈
 * index.html / login.html 에서 import해서 사용
 */

import { initializeApp } from "https://www.gstatic.com/firebasejs/10.10.0/firebase-app.js";
import { getAuth }        from "https://www.gstatic.com/firebasejs/10.10.0/firebase-auth.js";
import { getDatabase }    from "https://www.gstatic.com/firebasejs/10.10.0/firebase-database.js";

const firebaseConfig = {
  apiKey:            "AIzaSyAF-Rn7tzIjQeyUDJKnvKTRNccsXUVsIjo",
  authDomain:        "higherad-b9d62.firebaseapp.com",
  databaseURL:       "https://higherad-b9d62-default-rtdb.asia-southeast1.firebasedatabase.app",
  projectId:         "higherad-b9d62",
  storageBucket:     "higherad-b9d62.firebasestorage.app",
  messagingSenderId: "938928195180",
  appId:             "1:938928195180:web:8209b1e02a8caabe643a49",
  measurementId:     "G-01T4L4ZGVV"
};

export const app  = initializeApp(firebaseConfig);
export const auth = getAuth(app);
export const db   = getDatabase(app);
