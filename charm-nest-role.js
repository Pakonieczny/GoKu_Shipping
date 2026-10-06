/*  charm-nest-role.js — Laser or Design: whose work the Sorter app's work is counted as (Paul, 6 Oct 2026, item 4:
 *  "when signing into our app all users except for me (Admin) will need to be asked whether they are signing in as a Laser
 *  user or a Design user, so that we can track both independently").
 *
 *  The Sorter has no personal login: the person is the NAME kept in this browser (cn.employee, set through CNEmployee: the
 *  small name bar). This file decides what that sign-in is:
 *    · Admin (Paul, by the server's list; R5)  → signs in as before: station `sorter`, no role, never asked;
 *    · everyone else, and anyone the server cannot answer for (offline, an error: fail closed) → asked ONCE per sign-in,
 *      "Laser or Design?", in the same small name bar (charm-nest-bridge.js NameBar: a calm two-button step right after the
 *      name is set; never a pop-up, never on top of a pop-up, nothing on screen is blocked). The answer is the session's
 *      station (`laser` or `design`, device charm-nest-1, field `role`) and the role every event carries.
 *  Until it is answered nobody is signed in for the stations' sake (no session, no events): presses are kept (CNAct's held
 *  list) and recorded under the role once it is chosen, as presses before a name are.
 *
 *    CNRole.state()        "none" (nobody named) | "checking" (the Admin answer is on its way) | "ask" (the question is open)
 *                          | "admin" | "role"
 *    CNRole.role()         "laser" | "design" | ""   (""  for the Admin and while it is not answered)
 *    CNRole.admin()        true when this sign-in is the Admin's (the server's answer, kept for the sign-in)
 *    CNRole.ready()        the sign-in is complete: Admin, or a role is chosen
 *    CNRole.pending()      a name is set and the role is not chosen yet (and the person is not known to be the Admin)
 *    CNRole.choose(role)   the person's pick; ends nothing, starts the session under that role
 *    CNRole.switchTo(role) the quiet "Laser · switch to Design": the session under the old role ends ("switched"), the one under
 *                          the new starts; asks nothing
 *    CNRole.ask({ hint })  shows the question again (hint: nobody asked for it, so it takes no focus)
 *    CNRole.nameSet(name)  charm-nest-1.html calls it whenever a name is set (B.employee); "" = signed out
 *    CNRole.clear()        sign-out (midnight, idle, closing, a cleared name): the role goes with the name, so the next sign-in asks
 *    CNRole.setAdminLookup(fn)   fn(name) → Promise<true | false | null> (null = could not be told); the one place that asks the server
 *  What is remembered: the role a person chose, in localStorage `cn.role` = { v:1, name, admin:false, role } (it counts only for the name that is
 *  signed in; "not the Admin" is the safe side to keep). That someone IS the Admin is kept in memory for this page and sign-in only, never in
 *  storage (AD2's rule: the question is asked once per sign-in, and a reload asks it once more, quietly).
 *  The server's answer: POST firebaseOrders { stationAdmin: "<name>" } -> { ok:true, admin:true|false } (plans/stations-round2/api.md, AD2);
 *  anything else (offline, 4xx, 5xx, a bad body, a timeout) is "not told": not the Admin, so the person is asked.
 *  Never a PIN: the name only, as everywhere in the sorter. Nothing here throws into the page. */
(function () {
  "use strict";
  if (window.CNRole) return;
  const KEY = "cn.role", ROLES = { laser: "Laser", design: "Design" }, OTHER = { laser: "design", design: "laser" };
  const LOOKUP_MS = 6000;
  let checking = "", unknownFor = "", adm = "", seq = 0, lookup = null, listeners = [];

  const warn = (...a) => { try { console.warn("[CNRole]", ...a); } catch (_) {} };
  const nameNow = () => { try { const n = String((window.CNEmployee && CNEmployee.name()) || "").trim(); return /^\d+$/.test(n) ? "" : n; } catch (_) { return ""; } };
  const lsGet = k => { try { return localStorage.getItem(k) || ""; } catch (_) { return ""; } };
  const lsSet = (k, v) => { try { localStorage.setItem(k, v); } catch (_) {} };
  const lsDel = k => { try { localStorage.removeItem(k); } catch (_) {} };
  let mem = null;                                   // (kept in memory too: storage can be blocked or full)

  /** what is remembered for the name signed in now, or null */
  function read() {
    try {
      const name = nameNow(); if (!name) return null;
      let r = null; try { r = JSON.parse(lsGet(KEY) || "null"); } catch (_) {}
      if (!r || typeof r !== "object") r = mem;
      return r && r.name === name ? r : null;
    } catch (_) { return null; }
  }
  function write(rec) {
    mem = rec;
    lsSet(KEY, JSON.stringify(rec));
  }
  function drop() { mem = null; lsDel(KEY); }
  const admin = () => !!adm && adm === nameNow();                    // (this page's memory only)
  const role = () => { if (admin()) return ""; const r = read(); return r && r.admin !== true && ROLES[r.role] ? r.role : ""; };
  const ready = () => admin() || !!role();
  const state = () => {
    if (!nameNow()) return "none";
    if (admin()) return "admin";
    if (role()) return "role";
    return checking && checking === nameNow() ? "checking" : "ask";
  };
  const pending = () => !!nameNow() && !ready() && state() !== "checking";

  /* ── the bar (charm-nest-bridge.js NameBar) and the menu ── */
  const bar = spec => { try { if (window.CNEmployee && typeof CNEmployee.roleBar === "function") return CNEmployee.roleBar(spec); } catch (e) { warn("bar:", e); } return false; };
  function changed() {
    for (const f of listeners.slice()) { try { f(state(), role()); } catch (_) {} }
    paintMenu();
  }
  function paintMenu() {
    try {
      const b = document.getElementById("btnRoleSwitch"); if (!b) return;
      const r = role();
      b.hidden = !r;
      if (r) { b.textContent = `${ROLES[r]} · switch to ${ROLES[OTHER[r]]}`; b.dataset.role = r; b.title = `Your work is counted as ${ROLES[r]} now. One tap changes it to ${ROLES[OTHER[r]]}.`; }
    } catch (_) {}
  }

  /* ── the page: who is signed in, and what that means for the stations ── */
  /** the person is signed in for the stations' sake (a session runs, events are recorded): the Admin, or a role is chosen */
  function commit(resume) {
    try {
      const name = nameNow(); if (!name || !ready()) return;
      const SS = window.StationSession;
      if (SS && typeof SS.signedIn === "function") SS.signedIn({ name, id: null, resume: !!resume });        // (a role that changed ends the one session and starts the other; resume: a reload goes on with its session)
      try { if (window.CNAct && typeof CNAct.release === "function") CNAct.release(); } catch (_) {}   // (what was pressed meanwhile is recorded under it)
    } catch (e) { warn("commit:", e); }
  }
  /** a role was chosen or changed: the live board's slots of the old role end, the new ones start with the next paint */
  function reset() {
    try { if (window.StationActivity && typeof StationActivity.idle === "function") StationActivity.idle(); } catch (_) {}   // (every live card this page holds, under whichever role it was)
    try { const L = window.CNLive; if (L && typeof L.close === "function") { L.close("sorter"); L.close("laser"); } } catch (_) {}
  }

  /** The server's answer for one name (AD2's read-only door): true | false | null. Exactly { ok:true, admin:true|false } is an answer; the
      name goes in the body, never in a URL; nothing else is sent, nothing is kept. */
  async function serverSaysAdmin(name) {
    let ctl = null, timer = 0;
    try {
      if (typeof fetch !== "function") return null;
      try { if (typeof AbortController === "function") { ctl = new AbortController(); timer = setTimeout(() => { try { ctl.abort(); } catch (_) {} }, LOOKUP_MS - 500); } } catch (_) {}
      const r = await fetch("/.netlify/functions/firebaseOrders", { method: "POST", headers: { "Content-Type": "application/json" }, cache: "no-store", body: JSON.stringify({ stationAdmin: String(name).slice(0, 200) }), signal: ctl ? ctl.signal : undefined });
      const j = r && r.ok ? await r.json().catch(() => null) : null;
      return j && j.ok === true && (j.admin === true || j.admin === false) ? j.admin : null;
    } catch (_) { return null; }
    finally { clearTimeout(timer); }
  }
  /** is this name the Admin? Resolves true | false | null (null: could not be told: not Admin, asked again later). */
  async function askAdmin(name) {
    try {
      const timer = new Promise(res => setTimeout(() => res(null), LOOKUP_MS));
      const a = await Promise.race([Promise.resolve().then(() => (typeof lookup === "function" ? lookup(name) : serverSaysAdmin(name))), timer]);
      return a === true ? true : a === false ? false : null;
    } catch (e) { warn("admin lookup:", e); return null; }
  }

  /** the Admin answer for the name that was just set; then either the sign-in is complete (Admin) or the question opens */
  function check(name, quiet) {
    const my = ++seq;
    checking = name;
    // (a fast answer shows nothing: the bar the name was typed in goes, and the question or nothing follows; a slow one shows a small labelled spinner)
    const wait = quiet ? 0 : setTimeout(() => { if (my === seq && checking === name) bar({ state: "checking" }); }, 250);
    askAdmin(name).then(ans => {
      clearTimeout(wait);
      if (my !== seq || nameNow() !== name) return;       // a newer name, or signed out meanwhile
      checking = "";
      if (ans === true) {
        unknownFor = ""; adm = name; drop();                   // (the Admin: no role, nothing kept in storage)
        reset(); bar({ state: "off" }); commit(!!quiet); changed();
      } else {
        const had = read();
        if (ans === false) { unknownFor = ""; write({ v: 1, name, admin: false, role: had && ROLES[had.role] ? had.role : "" }); }    // (told: not the Admin: kept)
        else unknownFor = name;                                                                                                    // (not told: not the Admin for now, not kept as an answer, asked again)
        if (!quiet) ask({ hint: false }); else changed();
      }
    }).catch(e => { clearTimeout(wait); checking = ""; warn("check:", e); });
  }

  /** shows the question (when it is still needed) */
  function ask(o) {
    try {
      if (!nameNow() || ready() || (checking && checking === nameNow())) return false;
      return bar({ state: "ask", hint: !!(o && o.hint), onPick: r => choose(r) });
    } catch (_) { return false; }
  }

  function choose(r) {
    try {
      if (!ROLES[r]) return false;
      const name = nameNow(); if (!name || admin()) return false;
      const was = role();
      write(Object.assign({ v: 1, name, admin: false, role: r }, unknownFor === name ? { unk: true } : {}));     // (unk: the Admin answer was not had: asked again at the next load)
      bar({ state: "off" });
      if (was !== r) reset();
      commit(); changed();
      return true;
    } catch (e) { warn("choose:", e); return false; }
  }
  function switchTo(r) {
    try {
      const was = role();
      if (!was || !ROLES[r] || r === was) return false;
      return choose(r);
    } catch (_) { return false; }
  }

  /** a name was set anywhere (an approval, a label, the sheet window, the small name field, the Design Station's sign-in) */
  function nameSet(name) {
    try {
      name = String(name || "").trim();
      const SS = window.StationSession;
      if (!name) { clear(); return; }
      if (!window.CNEmployee || typeof CNEmployee.roleBar !== "function") { if (SS) SS.signedIn({ name, id: null }); return; }   // (without the bar there is nobody to ask: as before)
      if (adm && adm !== name) adm = "";
      const r = read();
      if (admin() || (r && ROLES[r.role])) { commit(); changed(); return; }                       // already known for this name: nothing is asked
      if (checking === name) return;
      if (r && r.name === name) { ask({ hint: false }); return; }                                // known not to be the Admin, no role yet
      drop(); check(name, false);                                                                 // a new person: the Admin answer first
    } catch (e) { warn("nameSet:", e); }
  }
  /** the person is working with a name kept from before (a reload, the first load of this version): the Admin answer is had quietly, nothing is shown
      (a person whose role is kept is not asked: "not the Admin" was the answer at their sign-in) */
  function start() {
    try {
      const name = nameNow(), r = read(); if (!name || checking || (r && !r.unk)) return;
      if (!window.CNEmployee || typeof CNEmployee.roleBar !== "function") return;
      if (r && r.unk) unknownFor = name;
      check(name, true);
    } catch (e) { warn("start:", e); }
  }
  function clear() {
    try {
      const had = state() !== "none" || !!lsGet(KEY);
      seq++; checking = ""; unknownFor = ""; adm = "";
      drop(); bar({ state: "off" }); reset();
      if (had) changed();
    } catch (_) {}
  }

  function setAdminLookup(fn) { lookup = typeof fn === "function" ? fn : null; }
  function onChange(fn) { if (typeof fn === "function") listeners.push(fn); }

  window.CNRole = { state, role, admin, ready, pending, choose, switchTo, ask, nameSet, start, clear, setAdminLookup, onChange, label: r => ROLES[r] || "", other: r => OTHER[r] || "", unknown: () => !!unknownFor && unknownFor === nameNow() };

  /* the Workspace menu's quiet "Laser · switch to Design" (charm-nest-1.html has the button; hidden for the Admin and until a role is chosen) */
  function wire() {
    try {
      const b = document.getElementById("btnRoleSwitch");
      if (b && !b.dataset.wired) {
        b.dataset.wired = "1";
        b.addEventListener("click", () => {
          try { const mm = document.getElementById("moreMenu"); if (mm) mm.open = false; } catch (_) {}
          const r = role(); if (r) switchTo(OTHER[r]);
        });
      }
      paintMenu();
    } catch (_) {}
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", wire); else wire();
  // the network is back and the server could not tell who this is: asked again (an Admin who was offline is then the Admin, and signs in as before)
  window.addEventListener("online", () => {
    try {
      const name = nameNow();
      if (!name || !unknownFor || unknownFor !== name || admin() || checking) return;
      const my = ++seq; checking = name;
      askAdmin(name).then(ans => {
        if (my !== seq || nameNow() !== name) return;
        checking = "";
        if (ans === true) { unknownFor = ""; adm = name; drop(); bar({ state: "off" }); reset(); commit(); changed(); }
        else if (ans === false) { unknownFor = ""; const r = read(); if (r && r.unk) write({ v: 1, name, admin: false, role: r.role || "" }); }
      }).catch(() => { checking = ""; });
    } catch (_) {}
  });
  // another tab of this computer chose, switched or signed out
  window.addEventListener("storage", e => { if (!e.key || e.key === KEY || e.key === "cn.employee") { setTimeout(() => { try { paintMenu(); } catch (_) {} }, 0); } });
})();
