/*  charm-nest-master-admin.js — is the person signed in at this Sorter the master admin (Paul)?   (Paul, 11 Oct 2026)
 *
 *    CharmNestMasterAdmin.is()        true only while the signed-in login is the master admin AND the server has said so;
 *                                     false for every other login, while the answer is still on its way, when the server cannot
 *                                     be asked (offline, error: fail closed) and when nobody is signed in. Always answers now.
 *    CharmNestMasterAdmin.login()     the signed-in login name ("" when nobody is signed in). Display only, the same for anyone.
 *    CharmNestMasterAdmin.state()     "none" (nobody signed in) | "checking" (the server's answer is on its way) | "master" | "other"
 *    CharmNestMasterAdmin.onChange(fn)  fn({ is, login }) whenever either changes; returns a function that stops it.
 *                                     The same change is the DOM event "charmnest:master-admin" on window (detail { is, login }), which
 *                                     works whichever script loaded first.
 *    CharmNestMasterAdmin.refresh()   look again now (a name was set or cleared)
 *    Any element marked data-master-only is shown (its hidden attribute removed) only while is() is true and hidden again the moment it
 *    is not. Write it with the hidden attribute in the HTML so nothing is visible before the answer.
 *
 *  HOW THE SORTER KNOWS WHO IS SIGNED IN, plainly: the Sorter has no personal login and no password. The "login" is the NAME kept in this
 *  browser (B.employee / cn.employee: typed in the small name bar, or brought by the Design Station's own sign-in), the name every seal and
 *  every efficiency record carries. Whether that name is the master admin is the one answer the page already gets from the server: the list
 *  in Firestore config/stationAdmins (fallback "Paul K", "Paul"), asked once per sign-in through firebaseOrders { stationAdmin: name } and
 *  kept in memory only for that sign-in (charm-nest-role.js CNRole.admin(); the same Admin the idle sign-out already uses). This module adds
 *  no request, no timer and no storage. So the check is a NAME match against a server-held list: anyone who types the master admin's name here
 *  passes it. It is a clean way to keep screens and buttons out of other people's sight, not a security boundary. Anything that must be
 *  protected is protected on the server by its own secret (the manager passcode for the efficiency reads, the repo password for the Charm
 *  repo), never by this module alone.
 *  Never a password, a PIN or a secret in here. Nothing here throws into the page. */
(function (root) {
  "use strict";
  if (root.CharmNestMasterAdmin) return;
  const EVENT = "charmnest:master-admin";
  const listeners = [];
  let last = "";

  /** the signed-in name, as CNRole reads it (a bare number is a PIN, never a name) */
  function login() {
    try {
      const n = String((root.CNEmployee && typeof root.CNEmployee.name === "function" && root.CNEmployee.name()) || (root.B && root.B.employee) || "").trim();
      return /^\d+$/.test(n) ? "" : n;
    } catch (_) { return ""; }
  }
  function is() {
    try {
      const R = root.CNRole;
      return !!login() && !!R && typeof R.admin === "function" && R.admin() === true;
    } catch (_) { return false; }
  }
  function state() {
    try {
      if (!login()) return "none";
      if (is()) return "master";
      const R = root.CNRole;
      return R && typeof R.state === "function" && R.state() === "checking" ? "checking" : "other";
    } catch (_) { return "none"; }
  }

  /** hide or show every data-master-only element */
  function paint() {
    try {
      const doc = root.document, ok = is();
      if (!doc || !doc.querySelectorAll) return;
      doc.querySelectorAll("[data-master-only]").forEach(el => { if (el.hidden === ok) el.hidden = !ok; });
    } catch (_) {}
  }
  /** look again; tell the listeners only when the answer or the name changed */
  function refresh() {
    paint();
    let sig = "";
    try { sig = (is() ? "1" : "0") + "|" + login(); } catch (_) { return; }
    if (sig === last) return;
    last = sig;
    const detail = { is: is(), login: login() };
    for (const f of listeners.slice()) { try { f(Object.assign({}, detail)); } catch (_) {} }
    try { root.dispatchEvent(new root.CustomEvent(EVENT, { detail })); } catch (_) {}
  }
  function onChange(fn) {
    if (typeof fn !== "function") return () => {};
    listeners.push(fn);
    return () => { const i = listeners.indexOf(fn); if (i >= 0) listeners.splice(i, 1); };
  }

  root.CharmNestMasterAdmin = Object.freeze({ is, login, state, onChange, refresh });

  /* when it looks again: the role module tells us (the Admin answer arrived, a name was set or cleared); another tab changed the name;
     this tab came back; the network came back; the Workspace menu is opened (the one place a hidden item could be about to be seen) */
  try { if (root.CNRole && typeof root.CNRole.onChange === "function") root.CNRole.onChange(refresh); } catch (_) {}
  try {
    root.addEventListener("storage", e => { if (!e.key || e.key === "cn.employee" || e.key === "cn.role") setTimeout(refresh, 0); });
    root.addEventListener("focus", refresh);
    root.addEventListener("online", refresh);
    if (root.document) {
      root.document.addEventListener("visibilitychange", () => { if (root.document.visibilityState === "visible") refresh(); });
      root.document.addEventListener("toggle", e => { if (e.target && e.target.id === "moreMenu" && e.target.open) refresh(); }, true);
    }
  } catch (_) {}
  const start = () => refresh();
  if (root.document && root.document.readyState === "loading") root.document.addEventListener("DOMContentLoaded", start); else start();
})(typeof window !== "undefined" ? window : globalThis);
