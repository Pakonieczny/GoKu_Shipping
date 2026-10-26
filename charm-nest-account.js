/*  charm-nest-account.js — who is signed in, and a quick way to sign in, sign out and switch person (Paul, 15 Oct 2026:
 *  "There is no way for a user to Log_Out and Sign_In, and it is not obvious who is currently signed in ... The Sign_In and
 *  Sign_Out process needs to be absolutely seamless, fast and easy to use ... so users feel welcome and it's really easy and
 *  fast to move between accounts").
 *
 *  The person is a NAME kept in this browser (B.employee, localStorage cn.employee), and "the Admin" is a name match answered by the server
 *  (charm-nest-role.js). Until 26 Oct 2026 any name could be typed or tapped (Paul, 15 Oct: "Never a PIN"). EMPNUM (Paul, 26 Oct 2026: "for some
 *  reason anyone can switch from one user to another without putting in their password, which is a big safety concern"): a person signs in,
 *  or is switched to, ONLY with their 6-digit Employee Number, the very login of every other station (firebaseOrders {pinLogin},
 *  _stationPinLogin.js: the server answers with that person's name, rate limited; the roster never leaves it). The NAME comes from the
 *  server, never from what is typed or tapped. The page's B.employee wrapper (charm-nest-1.html, configure({ gate: true })) lets a NEW person
 *  through only with the one-shot pass this file gives after the server's yes (admit), or for the name another tab of this computer signed in
 *  (follow); every other way of setting a name (the old name fields of the dialogs, a script) is refused and this panel asks for the number.
 *  Signing out needs nothing. The number is never kept, logged or shown: it goes to the server once and the field is emptied. No answer from
 *  the server (offline, an error) signs nobody in: the person already signed in stays signed in. What it is not: a server-side identity (the
 *  records still carry the name), so a person with the browser's developer tools could still set one; it closes every way the screen offers.
 *
 *    · an identity chip in the top bar (always there): a coloured avatar with the initials, the first name and a small role pill
 *      ("Laser" | "Design" | "Admin" | "Choose role"), or an outlined "Sign in" button when nobody is signed in;
 *    · the Account panel under the chip (a bottom sheet on a phone): "Who's working?" with the people of this computer as big
 *      tiles, a "Someone else" name field and, for a person who is not the Admin, the "Laser or Design?" question in the same panel;
 *      signed in: who, since when, when it signs out by itself, "Working as Laser | Design", the other people one tap away, Sign out.
 *
 *  What it uses (nothing is rebuilt): CNEmployee (name, normalize, last), B.employee (the one place a name is set), CNRole
 *  (state/role/admin/choose/switchTo/clear/onChange), StationSession (signedOut, current, policy), CNAct.drop (held presses).
 *  The page's own clear-the-sign-in function is handed in once: CNAccount.configure({ clear }) (charm-nest-1.html), so the automatic
 *  sign-outs (midnight, idle, 5:00 pm) and the manual one run the very same body.
 *
 *    CNAccount.open() / close() / toggle() / isOpen()       the panel (opens at once, no request)
 *    CNAccount.signIn(name)                                 a name (a page without the gate only); a number or markup is refused
 *    CNAccount.signInNumber(num, { why })  → Promise<{ ok, name } | { ok:false, error }>   EMPNUM: the server checks the number, then that person is signed in
 *    CNAccount.checkNumber(num)            → Promise<{ ok, name } | { ok:false, error, tooMany?, unreachable? }>   the server's answer only, nothing set
 *    CNAccount.gated() / admit(raw) / refuse(raw)           the page's B.employee wrapper: is the gate on, may this NEW person be set (a one-shot pass), say no
 *    CNAccount.signOut()                                    ends the station session now; the screen, the run and the windows stay as they are
 *    CNAccount.view()  CNAccount.recents()  CNAccount.forget(name)
 *    hooks (charm-nest-1.html's B.employee wrapper, charm-nest-bridge.js): before(prev, raw), after(prev, name), follow(storedName),
 *    takeRole(spec), yieldTo(), pulse(), refresh()
 *  Remembered here, names and nothing else of a person: localStorage cn.people (up to 6, newest first). cn.lastRole keeps the LAST role a
 *  name chose on this computer, only to highlight it in the question (it is asked every sign-in; it signs nobody in). Never a secret, never
 *  the Admin answer. Storage blocked or full: the list is just empty and everything works.
 *  Pure helpers for tests: require("./charm-nest-account.js") in node → { initials, firstName, display, hueOf, greetingFor, clockOf, sinceOf, spanOf,
 *  keyOf, addPerson, dropPerson, cleanPeople }.                                                                                          */
(function (root) {
  "use strict";

  /* ═══ pure helpers (node and browser) ═══ */
  const NAME_MAX = 40, PEOPLE_MAX = 6, SHOW_MAX = 5;
  /** one person, one key: case, accents and punctuation folded (the same fold the server and the Design Stations use for a login) */
  const keyOf = s => String(s == null ? "" : s).normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
  /** how a name reads to a person: the login "Ana_M" is "Ana M" */
  const display = s => String(s == null ? "" : s).replace(/_+/g, " ").replace(/\s+/g, " ").trim();
  const firstName = s => display(s).split(" ")[0] || "";
  const firstChar = w => { for (const ch of Array.from(String(w || ""))) if (/[\p{L}\p{N}]/u.test(ch)) return ch.toUpperCase(); return ""; };
  /** "Tess W." → "TW", "Tess" → "T", "Ana_M" → "AM" (the first letter of the first and of the last part) */
  function initials(name) {
    const parts = display(name).split(/[\s.\-]+/).filter(p => firstChar(p));
    if (!parts.length) return "?";
    return parts.length > 1 ? firstChar(parts[0]) + firstChar(parts[parts.length - 1]) : firstChar(parts[0]);
  }
  /** a stable hue 0..359 for a name (the same name is the same colour on every computer, whatever its case) */
  function hueOf(name) {
    let h = 2166136261;
    for (const ch of keyOf(name)) h = Math.imul(h ^ ch.charCodeAt(0), 16777619);
    return (h >>> 0) % 360;
  }
  /** "Good morning" before 12:00, "Good afternoon" before 17:00, else "Good evening" (the computer's own clock) */
  function greetingFor(d) {
    const h = (d instanceof Date ? d : new Date(d == null ? Date.now() : d)).getHours();
    return h < 12 ? "Good morning" : h < 17 ? "Good afternoon" : "Good evening";
  }
  /** "9:12 am" */
  function clockOf(ms) {
    const d = new Date(ms); let h = d.getHours(); const ap = h < 12 ? "am" : "pm";
    h = h % 12 || 12;
    return h + ":" + String(d.getMinutes()).padStart(2, "0") + " " + ap;
  }
  /** "under 1 min" | "7 min" | "2 h" | "2 h 5 min" */
  function sinceOf(ms, now) {
    const m = Math.max(0, Math.floor(((now == null ? Date.now() : now) - ms) / 60000));
    if (m < 1) return "under 1 min";
    if (m < 60) return m + " min";
    return Math.floor(m / 60) + " h" + (m % 60 ? " " + (m % 60) + " min" : "");
  }
  /** minutes → "10 min" | "1 hour" | "2 hours" */
  const spanOf = m => (m >= 60 && m % 60 === 0 ? (m / 60 === 1 ? "1 hour" : m / 60 + " hours") : m + " min");
  /** a stored list of names → clean names, de-duplicated by key, newest first, at most `max` */
  function cleanPeople(list, normalize, max) {
    const out = [], seen = new Set();
    for (const x of Array.isArray(list) ? list : []) {
      if (typeof x !== "string") continue;
      const n = (normalize ? normalize(x.slice(0, 80)) : x.trim()) || "";
      const k = keyOf(n);
      if (!n || !k || seen.has(k)) continue;
      seen.add(k); out.push(n);
      if (out.length >= (max || PEOPLE_MAX)) break;
    }
    return out;
  }
  /** a name goes to the front of the list (a name already there moves up, never twice) */
  const addPerson = (list, name, normalize, max) => cleanPeople([name].concat(list || []), normalize, max);
  const dropPerson = (list, name) => (list || []).filter(n => keyOf(n) !== keyOf(name));
  const H = { NAME_MAX, PEOPLE_MAX, SHOW_MAX, keyOf, display, firstName, initials, hueOf, greetingFor, clockOf, sinceOf, spanOf, cleanPeople, addPerson, dropPerson };
  if (typeof module !== "undefined" && module.exports) module.exports = H;
  if (!root || !root.document || root.CNAccount) return;

  /* ═══ the page ═══ */
  const doc = root.document;
  const K_NAME = "cn.employee", K_LAST = "cn.lastEmployee", K_PEOPLE = "cn.people", K_ROLE = "cn.lastRole";
  const LOCK_MS = 350;                       // (a control that appears under the pointer takes no click for a moment: a double click never answers the next question)
  const cfg = { clear: null, toast: null, gate: false };
  let picked = "", checkingNum = false;      // EMPNUM: the person tapped (only the label of the number field: the number decides who signs in), a check on its way
  let chip = null, panel = null, scrim = null, isOpen = false, lastMode = "", lockUntil = 0, signingIn = false, pendingGreet = "", adopting = 0, tickT = 0, errText = "", pulseT = 0, settleT = 0;

  const lsGet = k => { try { return root.localStorage.getItem(k) || ""; } catch (_) { return ""; } };
  const lsSet = (k, v) => { try { root.localStorage.setItem(k, v); } catch (_) {} };
  const lsDel = k => { try { root.localStorage.removeItem(k); } catch (_) {} };
  const calm = () => { try { return !!(root.matchMedia && root.matchMedia("(prefers-reduced-motion: reduce)").matches); } catch (_) { return false; } };
  const SS = () => root.StationSession || null;
  const Role = () => root.CNRole || null;

  /* ── who ── */
  const nameNow = () => { try { const n = String(root.CNEmployee ? root.CNEmployee.name() : lsGet(K_NAME)).trim(); return /^\d+$/.test(n) ? "" : n; } catch (_) { return ""; } };   // (before the bridge is up the chip reads the kept name; after it, only the page's own)
  const normalize = raw => { try { return root.CNEmployee && typeof root.CNEmployee.normalize === "function" ? root.CNEmployee.normalize(raw) : (/\p{L}/u.test(String(raw)) ? String(raw).trim() : ""); } catch (_) { return ""; } };
  const lastName = () => { try { return root.CNEmployee && typeof root.CNEmployee.last === "function" ? root.CNEmployee.last() : ""; } catch (_) { return ""; } };
  const roleState = () => { try { const R = Role(); return R && typeof R.state === "function" ? R.state() : (nameNow() ? "role" : "none"); } catch (_) { return "none"; } };
  /** the one picture of "who is signed in" every part of this file paints from */
  function view() {
    const name = nameNow(), st = name ? roleState() : "none", R = Role();
    const role = st === "role" && R ? R.role() : "", admin = st === "admin";
    const tag = admin ? "Admin" : role && R ? R.label(role) : st === "ask" ? "Choose role" : "";
    return { name, st, role, admin, tag, ready: st === "admin" || st === "role", shown: display(name), first: firstName(name) };
  }

  /* ── the people of this computer ── */
  function storedPeople() { let a = []; try { a = JSON.parse(lsGet(K_PEOPLE) || "[]"); } catch (_) {} return cleanPeople(a, normalize, PEOPLE_MAX); }
  /** the people shown: the stored list, with the last name used here (cn.lastEmployee, from before this list existed) in front when it is missing */
  function recents() {
    const list = storedPeople(), last = lastName();
    return last && !list.some(n => keyOf(n) === keyOf(last)) ? cleanPeople([last].concat(list), normalize, PEOPLE_MAX) : list;
  }
  function saveStored(list) { const s = JSON.stringify(list); if (s !== lsGet(K_PEOPLE)) { if (list.length) lsSet(K_PEOPLE, s); else lsDel(K_PEOPLE); } }
  const remember = name => { const n = normalize(name); if (n) saveStored(addPerson(storedPeople(), n, normalize, PEOPLE_MAX)); };
  function forget(name) {
    const k = keyOf(name); if (!k) return;
    saveStored(dropPerson(storedPeople(), name));
    if (keyOf(lastName()) === k) lsDel(K_LAST);                    // (the name bar's "Continue as <name>" forgets it too)
    const r = roles(); for (const x of Object.keys(r)) if (x === k) delete r[x]; saveRoles(r);
    if (isOpen) render();
  }
  function roles() { try { const o = JSON.parse(lsGet(K_ROLE) || "{}"); return o && typeof o === "object" && !Array.isArray(o) ? o : {}; } catch (_) { return {}; } }
  function saveRoles(o) { const keys = Object.keys(o); for (const x of keys.slice(0, Math.max(0, keys.length - 12))) delete o[x]; if (Object.keys(o).length) lsSet(K_ROLE, JSON.stringify(o)); else lsDel(K_ROLE); }
  const lastRoleOf = name => { const r = roles()[keyOf(name)]; return r === "laser" || r === "design" ? r : ""; };
  function keepRole(v) {
    if (!v.name || v.st !== "role" || !v.role) return;
    const o = roles(), k = keyOf(v.name);
    if (o[k] === v.role) return;
    delete o[k]; o[k] = v.role; saveRoles(o);
  }

  /* ── small DOM kit (text only: a name is never HTML) ── */
  function el(tag, props, ...kids) {
    const e = doc.createElement(tag);
    for (const k of Object.keys(props || {})) {
      const v = props[k];
      if (v == null || v === false) continue;
      if (k === "class") e.className = v;
      else if (k === "text") e.textContent = v;
      else if (k.slice(0, 2) === "on") e.addEventListener(k.slice(2), v);
      else e.setAttribute(k, v === true ? "" : String(v));
    }
    for (const c of kids.flat()) if (c != null && c !== false) e.append(c.nodeType ? c : doc.createTextNode(String(c)));
    return e;
  }
  const ICON = '<svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false"><circle cx="12" cy="8.2" r="3.7"/><path d="M4.6 20c.7-3.6 3.5-5.6 7.4-5.6s6.7 2 7.4 5.6"/></svg>';
  function avatar(name, size) {
    const a = el("span", { class: "cnAcAv " + (size || "md"), "aria-hidden": "true", text: initials(name) });
    a.style.setProperty("--h", String(hueOf(name)));
    return a;
  }

  /* ═══ styles (once, like the name bar's) ═══ */
  const CSS = [
    ".cnAcChip{display:inline-flex;align-items:center;gap:7px;box-sizing:border-box;min-height:34px;max-width:200px;padding:3px 10px 3px 4px;border:1px solid transparent;border-radius:999px;background:transparent;color:var(--ink,#222);font:650 12px var(--sans,system-ui,sans-serif);white-space:nowrap;cursor:pointer;flex:0 0 auto;transition:background-color .15s,border-color .15s}",
    ".cnAcChip:hover,.cnAcChip[aria-expanded=true]{background:var(--paper2,#ebe5d9)}",
    ".cnAcChip .cnAcName{min-width:0;overflow:hidden;text-overflow:ellipsis}",
    ".cnAcChip[data-st=none]{padding:3px 12px 3px 9px;border-color:var(--ink45,#938c80);color:var(--ink,#222)}",
    ".cnAcChip[data-st=none]:hover{background:var(--card2,#faf7f1);border-color:var(--ink,#222)}",
    ".cnAcChip .cnAcIco{display:inline-flex;align-items:center;color:var(--ink70,#5b554c)}.cnAcChip .cnAcIco[hidden]{display:none}",
    ".cnAcAv{display:inline-flex;align-items:center;justify-content:center;flex:0 0 auto;border-radius:50%;background:hsl(var(--h,200) 36% 38%);color:#fff;font-family:var(--sans,system-ui,sans-serif);font-weight:650;letter-spacing:.02em;line-height:1;user-select:none}",
    ".cnAcAv.sm{width:26px;height:26px;font-size:10.5px}.cnAcAv.md{width:38px;height:38px;font-size:14px}.cnAcAv.lg{width:54px;height:54px;font-size:19px}",
    ".cnAcPill{display:inline-flex;align-items:center;padding:2px 7px;border-radius:999px;background:var(--slateSoft,#e1ebee);color:var(--slate,#4a6b78);font:700 10px var(--sans,system-ui,sans-serif);letter-spacing:.02em;line-height:1.3}",
    ".cnAcPill[data-t=Admin]{background:var(--goldSoft,#f0e6cd);color:#6b5120}.cnAcPill[data-t='Choose role']{background:var(--claySoft,#f4e3dc);color:var(--clay,#b0563f)}",
    ".cnAcPill[hidden]{display:none}",
    "@container workspace (max-width:1040px){.cnAcChip .cnAcPill:not([data-t='Choose role']){display:none}}",
    // where the top bar is short of room (the tabs would start to scroll) and on a phone: the avatar alone (a person icon, outlined, when nobody is signed in); the label says the rest
    "@container workspace (max-width:900px){.cnAcChip{padding:3px;min-width:36px;justify-content:center;gap:0}.cnAcChip[data-st=none]{padding:3px 8px}.cnAcChip .cnAcName,.cnAcChip .cnAcPill{display:none}.cnAcChip[data-st=ask] .cnAcAv{box-shadow:0 0 0 2px var(--card,#fff),0 0 0 4px var(--clay,#b0563f)}}",
    "@media (max-width:719px){.cnAcChip{padding:3px;min-width:36px;justify-content:center;gap:0}.cnAcChip[data-st=none]{padding:3px 8px}.cnAcChip .cnAcName,.cnAcChip .cnAcPill{display:none}.cnAcChip[data-st=ask] .cnAcAv{box-shadow:0 0 0 2px var(--card,#fff),0 0 0 4px var(--clay,#b0563f)}}",
    "@media (pointer:coarse){.cnAcChip{min-height:44px;min-width:44px}}",
    // (the pulse is a ring on a pseudo-element that grows and fades: transform and opacity only, as every motion of the page, since it plays 1.8 s after a load, right when a sheet or order window may be flying)
    "@keyframes cnAcPulse{0%{transform:scale(1);opacity:.85}70%{transform:scale(1.22,1.6);opacity:0}100%{transform:scale(1.22,1.6);opacity:0}}",
    ".cnAcChip.cnAcPulse{position:relative}",
    ".cnAcChip.cnAcPulse::after{content:'';position:absolute;inset:0;border-radius:inherit;pointer-events:none;opacity:0;box-shadow:0 0 0 3px rgba(169,130,63,.55);animation:cnAcPulse 1.3s ease-out 1}",
    "@keyframes cnAcSettle{0%{transform:scale(.9)}55%{transform:scale(1.06)}100%{transform:scale(1)}}",
    ".cnAcChip.cnAcSettle{animation:cnAcSettle .4s ease-out 1}",
    "@media (prefers-reduced-motion:reduce){.cnAcChip.cnAcPulse{animation:none;box-shadow:0 0 0 3px var(--goldLine,#e3d3a6)}.cnAcChip.cnAcPulse::after{animation:none;display:none}.cnAcChip.cnAcSettle{animation:none}.cnAcPanel{animation:none!important}.cnAcSpin{animation-duration:1.8s!important}}",
    // the panel
    ".cnAcPanel{position:fixed;z-index:2147482000;box-sizing:border-box;width:min(364px,calc(100vw - 16px));max-height:calc(100vh - 24px);overflow:auto;overscroll-behavior:contain;padding:16px;background:var(--card,#fffefb);color:var(--ink,#1c1a17);border:1px solid var(--line,#e4ddd0);border-radius:16px;box-shadow:0 18px 48px rgba(20,16,10,.22);font:14px/1.4 var(--sans,system-ui,sans-serif);animation:cnAcIn .14s ease-out}",
    "@keyframes cnAcIn{from{opacity:0;transform:translateY(-4px)}}@keyframes cnAcUp{from{opacity:0;transform:translateY(14px)}}",
    ".cnAcPanel:focus{outline:none}.cnAcPanel *{box-sizing:border-box}",
    ".cnAcScrim{display:none}",
    "@media (max-width:559px){.cnAcScrim{display:block;position:fixed;inset:0;z-index:2147481900;background:rgba(20,18,15,.28)}.cnAcPanel{left:0!important;right:0!important;top:auto!important;bottom:0!important;width:auto;max-height:88vh;border-radius:18px 18px 0 0;border-width:1px 0 0;padding:18px 16px calc(18px + env(safe-area-inset-bottom,0px));animation:cnAcUp .18s ease-out}}",
    ".cnAcH{margin:0;font:650 18px/1.25 var(--serif,Georgia,serif);color:var(--ink,#1c1a17)}.cnAcH:focus{outline:none}",
    ".cnAcSub{margin:3px 0 12px;color:var(--ink70,#5b554c);font-size:13px}",
    ".cnAcHead{display:flex;align-items:center;gap:13px;min-width:0}.cnAcWho{min-width:0;display:flex;flex-direction:column;align-items:flex-start;gap:5px}",
    ".cnAcNm{font:650 18px/1.2 var(--serif,Georgia,serif);overflow-wrap:anywhere}",
    ".cnAcMeta{margin:12px 0 0;color:var(--ink70,#5b554c);font-size:12.5px}.cnAcMeta+.cnAcMeta{margin-top:2px;color:var(--ink45,#938c80)}",
    ".cnAcSec{margin:16px 0 6px;font:700 10px var(--sans,system-ui,sans-serif);letter-spacing:.06em;text-transform:uppercase;color:var(--ink45,#938c80)}",
    ".cnAcSeg{display:grid;grid-template-columns:1fr 1fr;gap:4px;padding:3px;background:var(--paper2,#ebe5d9);border-radius:12px}",
    ".cnAcSeg button{min-height:44px;border:0;border-radius:9px;background:transparent;color:var(--ink70,#5b554c);font:650 13px var(--sans,system-ui,sans-serif);cursor:pointer}",
    ".cnAcSeg button[aria-pressed=true]{background:var(--card,#fffefb);color:var(--ink,#1c1a17);box-shadow:0 1px 3px rgba(20,16,10,.14)}",
    ".cnAcGrid{display:grid;grid-template-columns:1fr 1fr;gap:8px}",
    ".cnAcTile,.cnAcPick{display:flex;align-items:center;gap:10px;min-width:0;min-height:56px;padding:8px 10px;border:1px solid var(--line,#e4ddd0);border-radius:12px;background:var(--card2,#faf7f1);color:var(--ink,#1c1a17);font:600 13.5px var(--sans,system-ui,sans-serif);text-align:left;cursor:pointer;transition:background-color .12s,border-color .12s}",
    ".cnAcTile:hover,.cnAcPick:hover{background:var(--paper2,#ebe5d9);border-color:var(--ink25,#c4bdb0)}",
    ".cnAcTn{min-width:0;flex:1 1 auto;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}",
    ".cnAcTile[aria-pressed=true],.cnAcPick[aria-pressed=true]{background:var(--sageSoft,#e7eddf);border-color:var(--sage,#5f7a5b)}",   // (EMPNUM: whose Employee Number the field asks for)
    ".cnAcTile[aria-busy=true]{background:var(--sageSoft,#e7eddf);border-color:var(--sage,#5f7a5b)}",
    ".cnAcSpin{width:14px;height:14px;flex:0 0 14px;border:2px solid rgba(0,0,0,.14);border-top-color:var(--sage,#5f7a5b);border-radius:50%;animation:spin .7s linear infinite;display:inline-block}",
    ".cnAcList{display:flex;flex-direction:column;gap:6px}.cnAcRow{display:flex;align-items:center;gap:4px}.cnAcRow .cnAcPick{flex:1 1 auto;min-height:48px;padding:5px 10px}",
    ".cnAcRow .cnAcAv.md{width:32px;height:32px;font-size:12.5px}",
    ".cnAcX{flex:0 0 44px;width:44px;height:44px;border:0;border-radius:10px;background:transparent;color:var(--ink45,#938c80);font-size:15px;line-height:1;cursor:pointer}.cnAcX:hover{background:var(--paper2,#ebe5d9);color:var(--ink,#1c1a17)}",
    ".cnAcForm{margin:12px 0 0}.cnAcForm label{display:block;margin:0 0 5px;font:600 12.5px var(--sans,system-ui,sans-serif);color:var(--ink70,#5b554c)}",
    ".cnAcFRow{display:flex;gap:8px}.cnAcIn{flex:1 1 auto;min-width:0;min-height:46px;padding:8px 12px;border:1px solid var(--line,#e4ddd0);border-radius:11px;background:var(--card,#fffefb);color:var(--ink,#1c1a17);font:16px var(--sans,system-ui,sans-serif)}",
    ".cnAcIn:focus{outline:2px solid rgba(74,107,120,.4);border-color:var(--slate,#4a6b78)}.cnAcIn[aria-invalid=true]{border-color:var(--clay,#b0563f)}",
    ".cnAcGo{flex:0 0 auto;min-height:46px;padding:8px 16px;font-size:14px;border-radius:11px}",
    ".cnAcErr{margin:6px 2px 0;font-size:12.5px;color:var(--clay,#b0563f)}.cnAcErr[hidden]{display:none}",
    ".cnAcStat{display:flex;align-items:center;gap:8px;margin:10px 2px 0;font-size:12.5px;color:var(--ink70,#5b554c)}",
    ".cnAcRoles{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:12px}",
    ".cnAcRole{display:flex;flex-direction:column;align-items:center;justify-content:center;gap:2px;min-height:76px;border:1px solid var(--line,#e4ddd0);border-radius:14px;background:var(--card2,#faf7f1);color:var(--ink,#1c1a17);font:650 17px var(--serif,Georgia,serif);cursor:pointer;transition:background-color .12s,border-color .12s}",
    ".cnAcRole:hover{background:var(--paper2,#ebe5d9)}.cnAcRole small{font:600 11px var(--sans,system-ui,sans-serif);color:var(--sage,#5f7a5b)}",
    ".cnAcRole.last{border:2px solid var(--sage,#5f7a5b);background:var(--sageSoft,#e7eddf)}",
    ".cnAcOut{display:block;width:100%;min-height:48px;margin-top:16px;border:1px solid var(--line,#e4ddd0);border-radius:12px;background:transparent;color:var(--clay,#b0563f);font:650 14px var(--sans,system-ui,sans-serif);cursor:pointer;transition:background-color .12s,border-color .12s}",
    ".cnAcOut:hover{background:var(--claySoft,#f4e3dc);border-color:#e3c8bf}",
    ".cnAcLink{display:block;width:100%;min-height:44px;margin-top:10px;border:0;background:transparent;color:var(--ink70,#5b554c);font:13px var(--sans,system-ui,sans-serif);text-decoration:underline;text-underline-offset:3px;cursor:pointer}.cnAcLink:hover{color:var(--ink,#1c1a17)}",
    ".cnAcPanel :is(button,input):focus-visible{outline:2px solid var(--gold,#a9823f);outline-offset:2px}"
  ].join("\n");
  function css() {
    if (doc.getElementById("cnAccountCss")) return;
    const s = doc.createElement("style"); s.id = "cnAccountCss"; s.textContent = CSS; (doc.head || doc.documentElement).appendChild(s);
  }

  /* ═══ the chip ═══ */
  function mount() {
    css();
    if (chip && chip.isConnected) return true;
    const tools = doc.querySelector(".topbar .topTools") || doc.querySelector(".topTools");
    if (!tools) return false;
    chip = el("button", { type: "button", class: "cnAcChip", id: "cnAcctChip", "aria-haspopup": "dialog", "aria-expanded": "false", "aria-controls": "cnAcctPanel", onclick: () => toggle() },
      el("span", { class: "cnAcIco", "aria-hidden": "true" }), el("span", { class: "cnAcName" }), el("span", { class: "cnAcPill", hidden: true }));
    chip.querySelector(".cnAcIco").innerHTML = ICON;
    const more = doc.getElementById("moreMenu");
    if (more && more.parentNode === tools) tools.insertBefore(chip, more); else tools.appendChild(chip);
    chip.addEventListener("keydown", e => { if (e.key === "Tab" && !e.shiftKey && isOpen && panel) { const f = focusables(); if (f.length) { e.preventDefault(); f[0].focus(); } } });
    paintChip();
    return true;
  }
  function paintChip() {
    if (!chip) return;
    const v = view(), pill = chip.querySelector(".cnAcPill"), nm = chip.querySelector(".cnAcName");
    chip.dataset.st = v.st; chip.setAttribute("aria-expanded", isOpen ? "true" : "false");
    const old = chip.querySelector(".cnAcAv"), ico = chip.querySelector(".cnAcIco");
    if (!v.name) {
      if (old) old.remove();
      ico.hidden = false; nm.textContent = "Sign in"; pill.hidden = true; pill.textContent = ""; pill.removeAttribute("data-t");
      chip.title = "Nobody is signed in. Sign in"; chip.setAttribute("aria-label", "Sign in. Nobody is signed in.");
      return;
    }
    ico.hidden = true;
    const a = avatar(v.name, "sm");
    if (old) old.replaceWith(a); else chip.insertBefore(a, ico);
    nm.textContent = v.first || v.shown;
    pill.hidden = !v.tag; pill.textContent = v.tag; if (v.tag) pill.setAttribute("data-t", v.tag); else pill.removeAttribute("data-t");
    const who = v.st === "checking" ? `Signing in as ${v.shown}` : v.st === "ask" ? `Signed in as ${v.shown}, choosing Laser or Design` : `Signed in as ${v.shown}${v.tag ? ", " + v.tag : ""}`;
    chip.title = who + ". Open account menu"; chip.setAttribute("aria-label", who + ". Open account menu");
  }
  /** nobody is signed in when the page opens: the chip pulses once (no pop-up, nothing opens by itself) */
  function pulse() {
    if (!mount()) return false;
    if (nameNow()) return true;
    chip.classList.remove("cnAcPulse"); void chip.offsetWidth; chip.classList.add("cnAcPulse");
    clearTimeout(pulseT); pulseT = setTimeout(() => { try { chip.classList.remove("cnAcPulse"); } catch (_) {} }, 3000);
    return true;
  }
  function settle() {
    if (!chip || calm()) return;
    chip.classList.remove("cnAcSettle"); void chip.offsetWidth; chip.classList.add("cnAcSettle");
    clearTimeout(settleT); settleT = setTimeout(() => { try { chip.classList.remove("cnAcSettle"); } catch (_) {} }, 500);
  }
  function notify(msg, kind) {
    try { const f = cfg.toast || (root.CN && root.CN.toast) || root.toast; if (typeof f === "function") f(msg, kind || "", 3200, "cnAcct"); } catch (_) {}
  }

  /* ═══ the panel ═══ */
  const modeOf = v => (!v.name || v.st === "checking" ? "who" : v.st === "ask" ? "role" : "me");
  const focusables = () => (panel ? [...panel.querySelectorAll("button:not([disabled]),input:not([disabled])")].filter(x => !x.hidden && x.offsetParent !== null) : []);
  const navItems = () => (panel ? [...panel.querySelectorAll("[data-nav]")].filter(x => x.offsetParent !== null) : []);
  const locked = () => Date.now() < lockUntil;

  function place() {
    if (!panel || !chip) return;
    if (root.innerWidth < 560) { panel.style.top = ""; panel.style.right = ""; return; }
    const r = chip.getBoundingClientRect();
    panel.style.top = Math.round(r.bottom + 8) + "px";
    panel.style.right = Math.max(8, Math.round(root.innerWidth - r.right)) + "px";
    panel.style.left = "auto"; panel.style.bottom = "auto";
  }
  function open(o) {
    o = o || {};
    if (!mount()) return false;
    if (isOpen) { render(); return true; }
    css();
    isOpen = true; signingIn = false; errText = ""; picked = ""; lastMode = ""; lockUntil = 0;
    try { const m = doc.getElementById("moreMenu"); if (m && m.open) m.open = false; } catch (_) {}
    scrim = el("div", { class: "cnAcScrim" });
    panel = el("div", { class: "cnAcPanel", id: "cnAcctPanel", role: "dialog", "aria-modal": "false", "aria-label": "Account", tabindex: "-1" });
    doc.body.append(scrim, panel);
    place();
    try { root.CNEmployee && root.CNEmployee.park && root.CNEmployee.park(true); } catch (_) {}
    chip.setAttribute("aria-expanded", "true");
    doc.addEventListener("pointerdown", onOutside, true);
    doc.addEventListener("keydown", onKey, true);
    doc.addEventListener("focusin", onFocusIn, true);
    root.addEventListener("resize", place);
    clearInterval(tickT); tickT = setInterval(tickSince, 30000);
    render(true);
    return true;
  }
  /** o.focus === false: leave the focus where it is (a name bar is about to take it) */
  function close(o) {
    if (!isOpen) return false;
    const inside = !!(panel && panel.contains(doc.activeElement));
    isOpen = false; signingIn = false; clearInterval(tickT); tickT = 0;
    doc.removeEventListener("pointerdown", onOutside, true);
    doc.removeEventListener("keydown", onKey, true);
    doc.removeEventListener("focusin", onFocusIn, true);
    root.removeEventListener("resize", place);
    try { panel.remove(); scrim.remove(); } catch (_) {}
    panel = null; scrim = null;
    try { root.CNEmployee && root.CNEmployee.park && root.CNEmployee.park(false); } catch (_) {}
    if (chip) chip.setAttribute("aria-expanded", "false");
    if (!(o && o.focus === false) && chip) {
      if (o && o.focus === "later") setTimeout(() => { try { const a = doc.activeElement; if (!a || a === doc.body || a === doc.documentElement) chip.focus({ preventScroll: true }); } catch (_) {} }, 40);   // (a click outside: where the click lands keeps the focus, otherwise the chip has it)
      else if (inside || (o && o.focus === true)) { try { chip.focus({ preventScroll: true }); } catch (_) {} }
    }
    return true;
  }
  const toggle = () => (isOpen ? close({ focus: true }) : open());

  function onOutside(e) {
    if (!isOpen || !panel) return;
    const t = e.target;
    if (panel.contains(t) || (chip && chip.contains(t))) return;
    close({ focus: "later" });
  }
  /** a window (a dialog) took the focus: the panel, which is not modal, is put away rather than left under its backdrop */
  function onFocusIn(e) {
    try { const t = e.target; if (isOpen && panel && t && t.closest && !panel.contains(t) && t.closest("dialog")) close({ focus: false }); } catch (_) {}
  }
  function onKey(e) {
    if (!isOpen || !panel) return;
    if (e.key === "Escape") { e.preventDefault(); e.stopPropagation(); close({ focus: true }); return; }
    const a = doc.activeElement;
    if ((e.key === "Enter" || e.key === " ") && e.repeat && a && panel.contains(a) && a.tagName === "BUTTON") { e.preventDefault(); return; }   // (a held key never answers the question that appears next)
    if (e.key === "Tab" && a && panel.contains(a)) {
      const f = focusables();
      if (!f.length) return;
      if (!e.shiftKey && a === f[f.length - 1]) { e.preventDefault(); close({ focus: false }); try { const n = doc.querySelector("#moreMenu>summary"); (n || chip).focus(); } catch (_) {} }
      else if (e.shiftKey && a === f[0]) { e.preventDefault(); try { chip.focus(); } catch (_) {} }
      return;
    }
    if (a && panel.contains(a) && a.hasAttribute("data-nav") && /^(Arrow(Up|Down|Left|Right)|Home|End)$/.test(e.key)) {
      const items = navItems(), i = items.indexOf(a); if (i < 0) return;
      const grid = a.parentElement && a.parentElement.classList.contains("cnAcGrid");
      let j = i;
      if (e.key === "Home") j = 0; else if (e.key === "End") j = items.length - 1;
      else if (e.key === "ArrowRight") j = i + 1; else if (e.key === "ArrowLeft") j = i - 1;
      else if (e.key === "ArrowDown") j = i + (grid ? 2 : 1); else if (e.key === "ArrowUp") j = i - (grid ? 2 : 1);
      j = Math.max(0, Math.min(items.length - 1, j));
      e.preventDefault(); items[j].focus();
    }
  }

  /* ── the panel's content ── */
  function sinceLine() {
    const s = SS() && SS().current ? SS().current() : null, v = view();
    if (!s || !v.ready || !(s.startAt > 0)) return "Signed in";
    return `Signed in ${clockOf(s.startAt)} · ${sinceOf(s.startAt, Date.now())}`;
  }
  function policyLine(v) {
    if (v.admin) return "Stays signed in until midnight.";
    try {
      const p = SS() && SS().policy ? SS().policy(v.role || "sorter") : null;
      if (p && p.idleMin > 0) return `Signs out by itself after ${spanOf(p.idleMin)} without input.`;
    } catch (_) {}
    return "";
  }
  function tickSince() {
    if (!isOpen || !panel) return;
    const m = panel.querySelector("[data-since]"); if (m) m.textContent = sinceLine();
  }
  function pill(v) { return v.tag ? el("span", { class: "cnAcPill", "data-t": v.tag, text: v.tag }) : null; }

  /** EMPNUM: the 6-digit Employee Number field (masked, digits only, never kept): the only way this panel signs a person in or switches to one */
  function numberForm(label) {
    const id = "cnAcIn" + Math.random().toString(36).slice(2, 6);
    const err = el("div", { class: "cnAcErr", role: "alert", id: id + "e", hidden: !errText, text: errText });
    const input = el("input", { class: "cnAcIn", id, type: "password", inputmode: "numeric", pattern: "[0-9]*", maxlength: "6", autocomplete: "off", spellcheck: "false", enterkeyhint: "go", placeholder: "6-digit Employee Number", "aria-describedby": id + "e", "aria-invalid": errText ? "true" : null, "data-fk": "in", "data-num": "1", disabled: checkingNum ? true : null });
    input.addEventListener("input", () => { const d = input.value.replace(/\D/g, "").slice(0, 6); if (d !== input.value) input.value = d; errText = ""; err.hidden = true; input.removeAttribute("aria-invalid"); });
    const go = el("button", { type: "submit", class: "btn sage cnAcGo", "aria-busy": checkingNum ? "true" : null, disabled: checkingNum ? true : null }, checkingNum ? el("i", { class: "cnAcSpin", "aria-hidden": "true" }) : null, checkingNum ? "Checking…" : "Continue");
    const form = el("form", { class: "cnAcForm", novalidate: true, onsubmit: e => { e.preventDefault(); e.stopPropagation(); if (!locked() && !checkingNum) { const num = input.value; input.value = ""; signInNumber(num); } } },
      el("label", { for: id, text: label }),
      el("div", { class: "cnAcFRow" }, input, go),
      err);
    return form;
  }
  /** a tile or a row tapped: with the gate, it only names whose number is asked for (and puts the focus in the field) */
  function pick(n) {
    if (!cfg.gate) return signIn(n);
    picked = keyOf(n) ? display(n) : ""; errText = ""; render();
    const i = panel && panel.querySelector(".cnAcIn"); if (i) try { i.focus({ preventScroll: true }); } catch (_) {}
    return true;
  }
  const numLabel = fallback => picked ? `${firstName(picked)}’s Employee Number` : fallback;
  function nameForm(label) {
    if (cfg.gate) return numberForm(numLabel(label === "Your name" ? "Your Employee Number" : "Someone else · Employee Number"));
    const id = "cnAcIn" + Math.random().toString(36).slice(2, 6);
    const err = el("div", { class: "cnAcErr", role: "alert", id: id + "e", hidden: !errText, text: errText });
    const input = el("input", { class: "cnAcIn", id, type: "text", maxlength: String(NAME_MAX), autocomplete: "off", autocapitalize: "words", spellcheck: "false", enterkeyhint: "go", placeholder: "e.g. Tess Welder", "aria-describedby": id + "e", "aria-invalid": errText ? "true" : null, "data-fk": "in" });
    input.addEventListener("input", () => { errText = ""; err.hidden = true; input.removeAttribute("aria-invalid"); });
    const form = el("form", { class: "cnAcForm", novalidate: true, onsubmit: e => { e.preventDefault(); e.stopPropagation(); if (!locked()) signIn(input.value); } },
      el("label", { for: id, text: label }),
      el("div", { class: "cnAcFRow" }, input, el("button", { type: "submit", class: "btn sage cnAcGo", text: "Continue" })),
      err);
    return form;
  }
  function personTile(n, busy) {
    const b = el("button", { type: "button", class: "cnAcTile", "data-nav": "1", "data-fk": "p:" + keyOf(n), "aria-busy": busy ? "true" : null, "aria-pressed": cfg.gate ? (keyOf(picked) === keyOf(n) ? "true" : "false") : null, "aria-label": `Sign in as ${display(n)}`, onclick: () => { if (!locked()) pick(n); } },
      avatar(n, "md"), el("span", { class: "cnAcTn", text: display(n) }), busy ? el("i", { class: "cnAcSpin", "aria-hidden": "true" }) : null);
    return b;
  }
  function buildWho(v) {
    const list = recents().slice(0, PEOPLE_MAX), busy = v.st === "checking" ? keyOf(v.name) : "";
    const kids = [el("h2", { class: "cnAcH", id: "cnAcTitle", tabindex: "-1", text: "Who’s working?" }),
      el("p", { class: "cnAcSub", text: cfg.gate ? (list.length ? "Tap your name, then enter your Employee Number." : "Enter your Employee Number to start.") : list.length ? "Tap your name to start." : "Type your name to start." })];
    if (list.length) kids.push(el("div", { class: "cnAcGrid", role: "group", "aria-label": "People on this computer" }, list.map(n => personTile(n, busy && keyOf(n) === busy))));
    if (busy) kids.push(el("div", { class: "cnAcStat", role: "status" }, el("i", { class: "cnAcSpin", "aria-hidden": "true" }), el("span", { text: `Signing in ${firstName(v.name)}…` })));
    kids.push(nameForm(list.length ? "Someone else" : "Your name"));
    return kids;
  }
  function buildRole(v) {
    const last = lastRoleOf(v.name);
    const btn = r => el("button", { type: "button", class: "cnAcRole" + (last === r ? " last" : ""), "data-nav": "1", "data-fk": "r:" + r, "data-role": r, onclick: () => { if (locked()) return; signingIn = true; const R = Role(); if (R) R.choose(r); } },
      el("span", { text: r === "laser" ? "Laser" : "Design" }), last === r ? el("small", { text: "Last time" }) : null);
    return [
      el("div", { class: "cnAcHead" }, avatar(v.name, "lg"), el("div", { class: "cnAcWho" }, el("div", { class: "cnAcNm", text: v.shown }), pill(v))),
      el("h2", { class: "cnAcH", id: "cnAcTitle", tabindex: "-1", style: "margin-top:14px", text: "Laser or Design?" }),
      el("p", { class: "cnAcSub", style: "margin-bottom:0", text: "Your work is counted under it until you sign out." }),
      el("div", { class: "cnAcRoles", role: "group", "aria-labelledby": "cnAcTitle" }, btn("laser"), btn("design")),
      el("button", { type: "button", class: "cnAcLink", text: `Not ${v.first}? Use a different name`, onclick: () => { if (!locked()) signOut({ silent: true }); } })
    ];
  }
  function buildMe(v) {
    const kids = [el("div", { class: "cnAcHead" }, avatar(v.name, "lg"), el("div", { class: "cnAcWho" }, el("div", { class: "cnAcNm", id: "cnAcTitle", text: v.shown }), pill(v)))];
    kids.push(el("p", { class: "cnAcMeta", "data-since": "1", text: sinceLine() }));
    const pol = policyLine(v); if (pol) kids.push(el("p", { class: "cnAcMeta", text: pol }));
    if (v.role) {
      const seg = r => el("button", { type: "button", "aria-pressed": v.role === r ? "true" : "false", "data-fk": "w:" + r, text: r === "laser" ? "Laser" : "Design", onclick: () => { if (locked() || v.role === r) return; const R = Role(); if (R) R.switchTo(r); } });
      kids.push(el("div", { class: "cnAcSec", text: "Working as" }), el("div", { class: "cnAcSeg", role: "group", "aria-label": "Working as" }, seg("laser"), seg("design")));
    }
    const others = recents().filter(n => keyOf(n) !== keyOf(v.name)).slice(0, SHOW_MAX);
    kids.push(el("div", { class: "cnAcSec", text: "Switch person" }));
    if (others.length) kids.push(el("div", { class: "cnAcList", role: "group", "aria-label": "People on this computer" }, others.map(n =>
      el("div", { class: "cnAcRow" },
        el("button", { type: "button", class: "cnAcPick", "data-nav": "1", "data-fk": "p:" + keyOf(n), "aria-pressed": cfg.gate ? (keyOf(picked) === keyOf(n) ? "true" : "false") : null, "aria-label": `Switch to ${display(n)}`, onclick: () => { if (!locked()) pick(n); } }, avatar(n, "md"), el("span", { class: "cnAcTn", text: display(n) })),
        el("button", { type: "button", class: "cnAcX", "data-fk": "x:" + keyOf(n), "aria-label": `Forget ${display(n)} on this computer`, title: "Forget this name here", text: "✕", onclick: () => { if (!locked()) forget(n); } })))));
    kids.push(nameForm("Someone else"));
    kids.push(el("button", { type: "button", class: "cnAcOut", "data-fk": "out", text: "Sign out", onclick: () => { if (!locked()) signOut(); } }));
    return kids;
  }
  /** paints the panel from the page's own state; keeps a half-typed name and the focus where they were */
  function render(first) {
    if (!isOpen || !panel) return;
    const v = view(), mode = modeOf(v);
    const old = panel.querySelector(".cnAcIn");
    const keep = old ? { value: old.value, focus: doc.activeElement === old, s: old.selectionStart, e: old.selectionEnd } : null;
    const act = doc.activeElement && panel.contains(doc.activeElement) ? doc.activeElement.getAttribute("data-fk") : null;
    const fresh = mode !== lastMode;
    panel.replaceChildren(...(mode === "who" ? buildWho(v) : mode === "role" ? buildRole(v) : buildMe(v)));
    panel.dataset.mode = mode;
    if (mode === "role") signingIn = true;
    if (fresh) { if (!first) lockUntil = Date.now() + LOCK_MS; lastMode = mode; }
    const input = panel.querySelector(".cnAcIn");
    if (keep && input) { input.value = keep.value; try { input.setSelectionRange(keep.s, keep.e); } catch (_) {} }
    if (fresh) {
      let f = null;
      if (mode === "who") f = panel.querySelector(".cnAcTile") || input;
      else if (mode === "role") f = panel.querySelector(".cnAcRole.last") || panel.querySelector("#cnAcTitle");
      else f = panel;
      if (f) try { f.focus({ preventScroll: true }); } catch (_) {}
    } else if (keep && keep.focus && input) input.focus({ preventScroll: true });
    else if (act) { const t = panel.querySelector(`[data-fk="${act.replace(/"/g, "")}"]`); if (t) try { t.focus({ preventScroll: true }); } catch (_) {} }
    place();
  }

  /* ═══ sign in · sign out · switch ═══ */
  function showErr(msg) {
    errText = msg;
    if (isOpen && panel) {
      const i = panel.querySelector(".cnAcIn"), e = panel.querySelector(".cnAcErr");
      if (e) { e.textContent = msg; e.hidden = false; }
      if (i) { i.setAttribute("aria-invalid", "true"); i.focus(); }
    }
  }
  /** a person's name from a tile, a row or the field: a number, nothing, or markup is refused (calmly, in the panel); the same person again is a no-op */
  function signIn(raw) {
    try {
      const text = String(raw == null ? "" : raw).replace(/[\u0000-\u001f\u007f]/g, " ").trim().slice(0, NAME_MAX);
      const n = /[<>]/.test(text) ? "" : normalize(text);
      if (!n) { showErr("Please type your name (letters, not a number)."); return false; }
      const cur = nameNow();
      if (cur && keyOf(cur) === keyOf(n)) { if (view().ready) close({ focus: true }); return true; }
      if (cfg.gate) return pick(n);                       // (EMPNUM: a name alone signs nobody in; the panel asks for that person's number)
      errText = ""; signingIn = true;
      if (root.B) root.B.employee = n;                    // (the one place a name is set: before() ends the old person's session, after() greets the new one)
      render();
      return true;
    } catch (e) { try { console.warn("[CNAccount] signIn:", e); } catch (_) {} return false; }
  }
  /* ═══ EMPNUM · the Employee Number (Paul, 26 Oct 2026: "anyone can switch from one user to another without putting in their password, which is a big
     safety concern") ═══ The stations' own door: POST firebaseOrders { pinLogin } answers { ok:true, name } for a number on the roster, { ok:false } for one
     that is not, { ok:false, tooMany:true } after too many tries (a minute's lockout, _stationPinLogin.js). Anything without `ok` (offline, a server
     error) was not answered: nobody is signed in by it (never the old roster download of the station pages: it hands every number to the browser). */
  const PIN_MS = 15000, PASS_MS = 30000;
  const passes = new Map();                     // key → until: the NEW person the gate lets through, once (the server said yes, or another tab of this computer signed them in)
  const grant = name => { const k = keyOf(name); if (k) passes.set(k, Date.now() + PASS_MS); };
  async function checkNumber(raw) {
    const num = String(raw == null ? "" : raw).replace(/\s+/g, "");
    if (!/^\d{6}$/.test(num)) return { ok: false, error: "An Employee Number is 6 digits." };
    let r = null;
    try {
      const ctl = typeof AbortController === "function" ? new AbortController() : null, t = ctl ? setTimeout(() => ctl.abort(), PIN_MS) : 0;
      try {
        const res = await root.fetch("/.netlify/functions/firebaseOrders", { method: "POST", headers: { "Content-Type": "application/json" }, cache: "no-store", body: JSON.stringify({ pinLogin: num }), signal: ctl ? ctl.signal : undefined });
        try { r = await res.json(); } catch (_) { r = null; }
      } finally { if (t) clearTimeout(t); }
    } catch (_) { r = null; }
    if (!r || typeof r.ok !== "boolean") return { ok: false, unreachable: true, error: "Your Employee Number could not be checked right now. Try again in a moment." };
    if (r.tooMany === true) return { ok: false, tooMany: true, error: "Too many tries. Wait a minute, then try again." };
    const name = r.ok === true && typeof r.name === "string" ? normalize(r.name.replace(/\s+/g, " ").trim()) : "";
    if (!name) return { ok: false, error: "That Employee Number is not on the list. Check the 6 digits and try again." };
    return { ok: true, name };
  }
  /** the panel's field, the name bar (CNEmployee), any press that needs a person: the server checks the number, and the person it names is signed in
      (the same person again: nothing changes). Never throws; the number is not kept. */
  async function signInNumber(raw, o) {
    o = o || {};
    if (checkingNum) return { ok: false, error: "Checking…" };
    checkingNum = true; errText = ""; if (isOpen) render();
    let out;
    try {
      out = await checkNumber(raw);
      if (out.ok) {
        const cur = nameNow();
        if (cur && keyOf(cur) === keyOf(out.name)) { picked = ""; if (isOpen && view().ready) close({ focus: true }); }
        else { grant(out.name); signingIn = true; picked = ""; if (root.B) root.B.employee = out.name; passes.delete(keyOf(out.name)); }
        if (!nameNow() || keyOf(nameNow()) !== keyOf(out.name)) out = { ok: false, error: "That person could not be signed in. Try again." };
        else out = { ok: true, name: nameNow() };
      }
    } catch (e) { out = { ok: false, error: "Your Employee Number could not be checked right now. Try again in a moment." }; try { console.warn("[CNAccount] signInNumber:", e); } catch (_) {} }
    finally { checkingNum = false; }
    if (!out.ok) { signingIn = false; errText = out.error; }
    if (isOpen) {
      render();
      // (a number refused: the field has the focus again for the next try, as a refused name had; it was put out of reach, disabled, while the server was asked)
      if (!out.ok) { const i = panel && panel.querySelector(".cnAcIn"); if (i) try { i.focus({ preventScroll: true }); } catch (_) {} }
    } else if (!out.ok && o.panel !== false) refresh();
    return out;
  }
  const gated = () => !!cfg.gate;
  /** the page's B.employee wrapper asks before it sets a NEW person (not the one signed in, not a sign-out): yes only with a fresh pass, used up here */
  function admit(raw) {
    if (!cfg.gate) return true;
    const k = keyOf(normalize(raw)), until = passes.get(k);
    if (!k) return true;                         // (nothing to sign in: the setter's own refusal of a number or of nothing stands)
    passes.delete(k);
    return !!(until && until >= Date.now());
  }
  /** ... and when it says no: nobody changes, and the panel asks for the Employee Number (one calm line; the name that was offered is never signed in) */
  function refuse(raw) {
    try { console.warn("[CNAccount] a name was offered without its Employee Number; nobody was signed in"); } catch (_) {}
    const t = nameNow() ? "To switch person, enter their Employee Number." : "Sign in with your Employee Number.";
    if (!isOpen) open();
    errText = t; picked = ""; render();
    return false;
  }
  /** ends the session now, clears name, role, Admin flag and held presses; the screen, the run and the windows stay. Nobody signed in: nothing happens. */
  function signOut(o) {
    o = o || {};
    try {
      const was = nameNow(); if (!was) return false;
      try { if (SS() && SS().signedOut) SS().signedOut("signOut"); } catch (_) {}
      try { if (typeof cfg.clear === "function") cfg.clear(); else { lsDel(K_NAME); if (root.B) root.B.employee = ""; if (root.CNAct && root.CNAct.drop) root.CNAct.drop(); if (Role()) Role().clear(); } } catch (_) {}
      pendingGreet = ""; signingIn = false; errText = "";
      if (!o.remote && !o.silent) notify(`Signed out. See you, ${firstName(was) || display(was)}.`, "");
      refresh();
      if (!o.remote) { if (isOpen) render(); else open(); }
      return true;
    } catch (e) { try { console.warn("[CNAccount] signOut:", e); } catch (_) {} return false; }
  }

  /* ═══ hooks ═══ */
  /** a name is about to be set (charm-nest-1.html's B.employee wrapper): another person taking the page over ends the old session now ("switched")
      and takes none of the old person's held presses; the same person again does nothing */
  function before(prev, raw) {
    try {
      const n = normalize(raw);
      if (!prev || !n || keyOf(prev) === keyOf(n)) return;
      try { if (SS() && SS().signedOut) SS().signedOut("switched"); } catch (_) {}
      try { if (root.CNAct && root.CNAct.drop) root.CNAct.drop(); } catch (_) {}
    } catch (_) {}
  }
  /** a name was set: remembered, the welcome waits for the sign-in to be complete (the Admin answer, or the role) */
  function after(prev, name) {
    try {
      const n = String(name || "").trim();
      if (n && (!prev || keyOf(prev) !== keyOf(n))) { remember(n); if (!adopting) pendingGreet = keyOf(n); }
      if (!n) pendingGreet = "";
      refresh();
    } catch (_) {}
  }
  /** another tab of this computer changed cn.employee (the storage event, via the bridge): a sign-out or a switch there is this tab's too, without a sound */
  function follow(stored) {
    adopting++;
    try {
      const cur = nameNow(), next = stored ? normalize(stored) : "";
      if (!next) { if (cur) signOut({ remote: true }); }
      else if (!cur || keyOf(next) !== keyOf(cur)) { grant(next); if (root.B) root.B.employee = next; passes.delete(keyOf(next)); }   // (EMPNUM: the other tab let them through its own gate)
    } catch (_) {} finally { adopting--; }
    refresh();
  }
  /** the name bar's role step (NameBar.roleBar): while the panel is open the panel asks the question itself */
  function takeRole(spec) {
    try {
      paintChip();                                         // (the question opening, the wait for the Admin answer: the chip says it either way)
      if (!isOpen || !spec || (spec.state !== "ask" && spec.state !== "checking")) return false;
      render(); return true;
    } catch (_) { return false; }
  }
  /** a press needs a name (the name bar opens): the panel steps aside */
  const yieldTo = () => { if (isOpen) close({ focus: false }); };
  function greetIfDone(v) {
    if (!pendingGreet || !v.ready || keyOf(v.name) !== pendingGreet) return;
    pendingGreet = "";
    notify(`${greetingFor(new Date())}, ${v.first || v.shown}.`, "ok");
    settle();
  }
  function refresh() {
    try {
      mount(); paintChip();
      const v = view();
      keepRole(v);
      greetIfDone(v);
      if (isOpen) { if (signingIn && v.ready) close({ focus: true }); else render(); }
    } catch (e) { try { console.warn("[CNAccount] refresh:", e); } catch (_) {} }
  }
  function configure(o) { if (o && typeof o === "object") { if (typeof o.clear === "function") cfg.clear = o.clear; if (typeof o.toast === "function") cfg.toast = o.toast; if (o.gate === true) cfg.gate = true; } }   // (the gate, once on, stays on for the page)

  root.CNAccount = { open, close, toggle, isOpen: () => isOpen, signIn, switchTo: signIn, signInNumber, checkNumber, gated, admit, refuse, signOut, forget, recents, view, refresh, configure, before, after, follow, takeRole, yieldTo, pulse, adopting: () => adopting > 0, timers: () => (tickT ? 1 : 0), helpers: H };

  /* ── start ── */
  mount();
  if (!chip) doc.addEventListener("DOMContentLoaded", () => { mount(); refresh(); }, { once: true });
  root.addEventListener("load", () => refresh(), { once: true });
  try { if (Role() && typeof Role().onChange === "function") Role().onChange(() => refresh()); } catch (_) {}
  // another tab of this computer: the list of people, a role chosen there, the last name used
  root.addEventListener("storage", e => { if (!e.key || e.key === K_PEOPLE || e.key === K_LAST || e.key === K_ROLE || e.key === "cn.role") setTimeout(refresh, 0); });
})(typeof window !== "undefined" ? window : null);
