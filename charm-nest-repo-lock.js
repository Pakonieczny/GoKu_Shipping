/*  charm-nest-repo-lock.js
 *  ═══════════════════════════════════════════════════════════════════════
 *  The ONE password dialog of the Charm Repo Library (the Master files tab).
 *
 *  Paul, 11 Oct 2026: everybody may look at the library, search it and use a charm as before; uploading a master file,
 *  deleting one, downloading a charm file and changing anything in the repo ask for his login password, the same way
 *  everywhere. This file is that one way:
 *
 *    CharmNestRepoLock.ensure(action, ask)      → Promise<{token, action, expiresAt, once} | null>   (null = cancelled)
 *    CharmNestRepoLock.call(action, ask, fn)    → runs fn(token) with a token the server accepts: asks when none is held, drops a
 *                                                 token the server refused and asks once more, spends a single-use token
 *    CharmNestRepoLock.unlocked(action)         → is a usable token held right now (the lock icons read this)
 *    CharmNestRepoLock.mark(root)               → puts the locked / open padlock on every [data-repo] control under root
 *    CharmNestRepoLock.gate(el, action, ask)    → a text box or a select that asks before it takes a first touch
 *    CharmNestRepoLock.forget(action?)          → drop held tokens (all when no action, the session too)
 *    CharmNestRepoLock.submitInline(action, password, ask) → Promise<{ok, reason, info?, retryAfterSec?}>: ONE try with a typed password for a field that lives
 *                                                 in a window of its own (the designs window's save strip), with no dialog: the same server call, the same
 *                                                 repo-auth-attempt event, the same wrong / locked words and wait as the dialog, and a right answer is kept
 *                                                 exactly as the dialog keeps it (a session for everything but a delete). The caller clears its field before it
 *                                                 calls; nothing here keeps the password.
 *    CharmNestRepoLock.messages                 → the words of every outcome (empty, wrong, locked, unavailable), shared by the dialog and the inline field
 *    CharmNestRepoLock.lockNow()                → Promise<{ok, server}>: the session ends at once on this computer and, asked of the server, there
 *    CharmNestRepoLock.status()                 → { open, until }: is a session held (until = its end, ms), for the "Unlocked until ... Lock now" line
 *    CharmNestRepoLock.verify()                 → Promise<true|false|null>: asks the server whether the held session still stands (false: it ended, so it is dropped)
 *    CharmNestRepoLock.untilText(ms)            → "5:00 pm" in Toronto time
 *
 *  SESSION (Paul, 11 Oct 2026: "password protect the settings menu using my login password. If I'm logged into and working from my account then do not prompt me"):
 *  one right password buys a SESSION token for Settings AND the repo's upload / change / download. It is kept in localStorage (so reloads and new tabs of this
 *  computer share it) until the server's expiry (the earliest of 12 hours, the next 5:00 pm Toronto, midnight Toronto) or Lock now. The sorter has no way to show the
 *  server the email app's operator session (another origin), so this stands in for "logged in". A delete never rides on the session by itself: it still asks its
 *  confirm sentence, but takes a single-use token from the session with no password. Action "settings" opens the Settings menu; "modify" / "upload" / "download"
 *  are covered by the same session.
 *
 *  PERSON (Paul, 26 Oct 2026: "the Settings menu now opens without the need for a password. I did not ask for this and I need the password to be re-implemented
 *  ... users are allowed to download .ai charm files from the main charm repo, this should only be allowed with my password, same as accessing the Settings menu"):
 *  the standing session above outlived the person who unlocked it (it was kept for the whole computer until 5:00 pm), so whoever sat down next opened Settings and
 *  downloaded freely. Now, the newest rule winning over 11 Oct's:
 *    · Settings asks for the password EVERY time it is opened. Its unlock (the server's session token, which the Settings buttons need) is held in this tab's memory
 *      only while the menu is open; closing the menu ends it here and on the server (lockNow, from charm-nest-1.html).
 *    · A download asks for the password; one right password serves this tab for GRACE.download (a minute: a charm's .ai and .dxf, or a few charms, take one
 *      password), then it asks again. A change or an upload keeps the server's own few minutes (GRACE).
 *    · Every unlock belongs to the person signed in when it was given (CNEmployee): another person signing in, or a sign-out, ends them all at once
 *      (personChanged, called by the B.employee setter), and a held unlock is never used under another name.
 *    · Nothing is kept in localStorage any more: a reload or another tab asks again. A standing session left by an older page is ended on the server at load.
 *
 *  `action` is one of upload, delete, download, modify, history, other (the server's words, plans/repo-protect-1011/REPOAUTH-api.md).
 *  `ask` = { what, target, go, title, message, note }: what the dialog says, the short target recorded with the attempt, the
 *  button word, an optional title, an optional confirm sentence (a delete says it here: no second pop-up) and one plain note.
 *
 *  The dialog calls `repoUnlock` through the page's api() (charmNestLibrary), shows a small labelled spinner while the server
 *  checks, one plain line for a wrong password, a plain "try again later" for a lockout, and clears the password field on every
 *  outcome. The password is read once, sent once and never kept, shown or logged. A token (never the password) is kept for this
 *  tab in sessionStorage until its expiry; a single-use token (delete, history) is never kept at all. After every attempt it
 *  fires `repo-auth-attempt` on window (detail { action, ok, reason }) for the Alert.
 *
 *  Nothing here decides what is protected: the server refuses a protected op without a valid `repoToken` (401 repo-locked).
 *  This file only gets the token and sends it. When this file is missing the callers fail closed (they do not call).
 *  ═══════════════════════════════════════════════════════════════════════ */
(function (root) {
  "use strict";
  const doc = root.document;
  const KEY = "cn.repoLock.v1", SKEY = "cn.repoSession.v1", TAB = "cn.repoTab.v1", MARGIN = 5000;
  const ACTIONS = ["upload", "delete", "download", "modify", "history", "other", "settings"];
  /* which held token may serve which need (REPOAUTH-api.md: an upload token also serves masterPatch, "other" serves every
     upload / modify / download op and never a delete; delete and history tokens are single use and are never held) */
  const COVERS = { upload: ["upload", "other"], modify: ["modify", "upload", "other"], download: ["download", "other"], delete: ["delete"], history: ["history"], other: ["other"], settings: [] };
  const SESSION_NEEDS = new Set(["settings", "upload", "modify", "download", "other"]);   // what a standing session serves (never a delete: that takes a single-use token)
  const ONCE = new Set(["delete", "history"]);
  const norm = a => (ACTIONS.includes(a) ? a : "other");
  const WORDS = {
    upload: { title: "Add to the charm repo", what: "Adding a master file to the charm repo.", go: "Unlock" },
    delete: { title: "Remove from the charm repo", what: "Removing from the charm repo.", go: "Remove" },
    download: { title: "Download a charm file", what: "Downloading a charm file from the repo.", go: "Download" },
    modify: { title: "Change a charm in the repo", what: "Changing a charm in the repo.", go: "Unlock" },
    history: { title: "Charm repo history", what: "Changing the charm repo history.", go: "Unlock" },
    other: { title: "Charm repo password", what: "This part of the charm repo is locked.", go: "Unlock" },
    settings: { title: "Settings", what: "The Settings menu is locked. Enter your login password to open it.", go: "Unlock" }
  };

  let hook = null;                      // tests: { api }
  let mem = {};                         // action → { t, e, p }: the tokens held (a single-use one is never put here); p = the person they belong to
  let sess = null;                      // { t, e, p }: Settings' unlock while the menu is open, this tab's memory only (PERSON, 26 Oct 2026)
  /* PERSON: how long this tab uses a right password before asking again (capped by the server's own expiry) */
  const GRACE = { download: 60 * 1000, modify: 5 * 60 * 1000, upload: 15 * 60 * 1000, other: 60 * 1000 };
  /** The person signed in to the Sorter now (the key every unlock is held under); "" when nobody is, or on a page without the sign-in. */
  const personKey = () => { try { const E = root.CNEmployee, n = E && typeof E.name === "function" ? E.name() : ""; return String(n || "").trim().toLowerCase().replace(/\s+/g, " "); } catch (_) { return ""; } };
  let lockedUntil = 0;                  // after a "locked" answer: no further try from this tab before this time (no more records)
  let queue = Promise.resolve();
  const listeners = new Set();
  let expiryTimer = 0;

  /* ── tokens held for this tab ── */
  const store = () => { try { return root.sessionStorage || null; } catch (_) { return null; } };
  function load() {
    try { const s = store(), raw = s && s.getItem(KEY); if (raw) { const o = JSON.parse(raw); if (o && typeof o === "object") mem = o; } } catch (_) { /* in memory only */ }
    prune();
    return mem;
  }
  function save() { try { const s = store(); if (s) s.setItem(KEY, JSON.stringify(mem)); } catch (_) { /* the tab still holds it in memory */ } }
  const shared = () => { try { return root.localStorage || null; } catch (_) { return null; } };
  /** Settings' unlock, held in this tab's memory while the menu is open (PERSON: never in localStorage; an ended one, or one given to another person, is dropped). */
  function loadSession() {
    if (sess && (!(sess.e > Date.now() + MARGIN) || sess.p !== personKey())) sess = null;
    return sess;
  }
  function saveSession() { /* PERSON (26 Oct 2026): nothing is kept for the computer any more; see the header */ }
  /** PERSON: a standing session an older page left in localStorage stops working for this computer at once, and is ended on the server soon after the page is up. */
  function endLegacySession() {
    let old = null;
    try { const st = shared(), raw = st && st.getItem(SKEY); if (raw) { st.removeItem(SKEY); const o = JSON.parse(raw); if (o && typeof o.t === "string" && o.t) old = o.t; } } catch (_) { old = null; }
    if (old && root.setTimeout) root.setTimeout(() => { callOp("repoLock", { sessionToken: old }).catch(() => {}); }, 4000);
  }
  function prune() {
    const now = Date.now(), p = personKey();
    // (a token is used only by the person it was given to; one with no person on it is older than this rule, an older page of this tab, and keeps its own few minutes)
    for (const k of Object.keys(mem)) { const x = mem[k]; if (!ACTIONS.includes(k) || ONCE.has(k) || !x || typeof x.t !== "string" || !x.t || !(+x.e > now + MARGIN) || (x.p !== undefined && x.p !== p)) delete mem[k]; }
  }
  function find(need) {
    need = norm(need); load();
    if (SESSION_NEEDS.has(need)) { const s = loadSession(); if (s) return { action: "session", token: s.t, expiresAt: s.e, once: false, session: true }; }
    for (const a of COVERS[need] || []) { const x = mem[a]; if (x) return { action: a, token: x.t, expiresAt: +x.e, once: false }; }
    return null;
  }
  function remember(info) {
    const p = personKey();
    if (info.session) { sess = { t: info.token, e: info.expiresAt, p }; changed(); return; }
    if (info.once || ONCE.has(info.action)) return;
    const cap = GRACE[info.action], e = cap ? Math.min(+info.expiresAt || Infinity, Date.now() + cap) : info.expiresAt;
    mem[info.action] = { t: info.token, e, p }; save(); changed();
  }
  /** PERSON: the signed-in person changed (another sign-in, a sign-out): every unlock of this tab ends now, and Settings' session on the server too. */
  function personChanged(before, after) {
    const norm2 = s => String(s || "").trim().toLowerCase().replace(/\s+/g, " ");
    if (norm2(before) === norm2(after)) return;
    const s = sess; sess = null; load(); for (const k of Object.keys(mem)) if (mem[k] && mem[k].p !== undefined) delete mem[k]; save(); changed();
    if (s && s.t) callOp("repoLock", { sessionToken: s.t }).catch(() => {});
  }
  function drop(info) {
    if (!info) return;
    if (info.session || info.action === "session") { loadSession(); if (sess && sess.t === info.token) { sess = null; saveSession(); } changed(); return; }
    load(); if (mem[info.action] && mem[info.action].t === info.token) { delete mem[info.action]; save(); }
    changed();
  }
  function forget(action) {
    load(); if (action) delete mem[norm(action)]; else { mem = {}; sess = null; saveSession(); }
    save(); changed();
  }
  const unlocked = action => !!find(action);
  function onChange(fn) { listeners.add(fn); return () => listeners.delete(fn); }
  function changed() {
    clearTimeout(expiryTimer);
    const now = Date.now(); let next = Infinity; for (const k of Object.keys(mem)) { const x = mem[k]; if (x && +x.e - MARGIN > now) next = Math.min(next, +x.e - MARGIN); }
    if (sess && sess.e - MARGIN > now) next = Math.min(next, sess.e - MARGIN);
    if (next < Infinity) expiryTimer = setTimeout(() => { prune(); save(); loadSession(); saveSession(); changed(); }, Math.min(2147483000, Math.max(250, next - now + 50)));
    try { mark(); } catch (_) { /* no page yet */ }
    for (const fn of [...listeners]) { try { fn(); } catch (_) { /* a listener never breaks the lock */ } }
  }

  /* ── who and where, as the server records them (display only) ── */
  function who() {
    try { const A = root.CharmNestMasterAdmin, l = A && typeof A.login === "function" ? A.login() : ""; if (l) return String(l).slice(0, 80); } catch (_) { /* helper missing */ }
    try { const E = root.CNEmployee, n = E && typeof E.name === "function" ? E.name() : ""; if (n) return String(n).slice(0, 80); } catch (_) { /* no name yet */ }
    return "";
  }
  function tabId() {
    let id = "";
    try { const s = store(); id = (s && s.getItem(TAB)) || ""; if (!id) { id = Math.random().toString(36).slice(2, 10); if (s) s.setItem(TAB, id); } } catch (_) { /* below */ }
    return id || (tabId.mem || (tabId.mem = Math.random().toString(36).slice(2, 10)));
  }

  /* ── the server ── */
  const getApi = () => (hook && hook.api) || (root.CN && root.CN.api) || root.api;
  function answerOf(e) { return e && e.data && typeof e.data === "object" ? e.data : null; }
  const isRepoLocked = e => !!e && e.status === 401 && !!e.data && e.data.error === "repo-locked";
  const cancelledError = () => Object.assign(new Error("Not done: the charm repo password was not given"), { cancelled: true });
  async function callOp(op, body) {
    const api = getApi();
    if (typeof api !== "function") return { ok: false, error: "unavailable" };
    try { return await api("charmNestLibrary", Object.assign({ op }, body), { quiet: true }); }
    catch (e) { return answerOf(e) || { ok: false, error: "unavailable" }; }
  }
  const unlockCall = body => callOp("repoUnlock", body);

  /* the words of every outcome of a try: the dialog and the inline field (submitInline) say the same */
  const SAY = { empty: "Type the password first.", wrong: "That password is not right. Try again.", locked: "Too many wrong tries. Please try again later.", unavailable: "The check is not available right now. Please try again later." };
  /** ONE try with a typed password, shared by the dialog and submitInline: the server is asked once, `repo-auth-attempt` fires, a right answer is kept
   *  (remember), a "locked" answer starts the wait (lockedUntil). Answers { ok, reason: ok|empty|wrong|locked|unavailable, info?, retryAfterSec? }.
   *  The password is read once, sent once, and not kept. */
  async function attempt(action, pw, a) {
    a = a || {};
    if (Date.now() < lockedUntil) return { ok: false, reason: "locked", retryAfterSec: Math.max(1, Math.ceil((lockedUntil - Date.now()) / 1000)) };
    if (!pw) return { ok: false, reason: "empty" };
    // (PERSON, 26 Oct 2026: only Settings asks the server for a session, the token its own buttons need while the menu is open; a download, a change or an upload
    //  takes that action's own short token, so no long unlock exists anywhere for somebody else to find)
    let ans; try { ans = await unlockCall({ password: pw, action, target: String(a.target || "").slice(0, 80), by: who(), tab: tabId(), ...(action === "settings" ? { session: true } : {}), ...(a.overwrite ? { overwrite: true } : {}) }); } finally { pw = ""; }
    const ok = !!ans && ans.ok === true && typeof ans.token === "string" && ans.token.length > 0;
    const reason = ok ? "ok" : ans && (ans.error === "wrong" || ans.error === "locked") ? ans.error : "unavailable";
    try { root.dispatchEvent(new root.CustomEvent("repo-auth-attempt", { detail: { action, ok, reason } })); } catch (_) { /* the Alert is optional */ }
    if (ok) {
      const once = ans.once === true || ONCE.has(action), session = ans.session === true && !once;
      const info = { action: session ? "session" : ACTIONS.includes(ans.action) ? ans.action : action, token: ans.token, expiresAt: +ans.expiresAt > 0 ? +ans.expiresAt : Date.now() + 5 * 60 * 1000, once, ...(session ? { session: true } : {}) };
      remember(info);
      return { ok: true, reason: "ok", info };
    }
    if (reason === "locked") { const s = +ans.retryAfterSec; lockedUntil = Date.now() + (s > 0 ? Math.min(s, 3600) : 900) * 1000; return { ok: false, reason, retryAfterSec: Math.max(1, Math.ceil((lockedUntil - Date.now()) / 1000)) }; }
    return { ok: false, reason };
  }

  /* ── the dialog (made fresh each time and removed after: no password field is left in the page) ── */
  const SHUT = "M3 6V4.2a3 3 0 0 1 6 0V6", OPEN = "M3 6V4.2a3 3 0 0 1 5.9-.8";
  const padlock = (shackle, ink) => "data:image/svg+xml," + encodeURIComponent(`<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 12 14'><path d='${shackle}' fill='none' stroke='${ink}' stroke-width='1.7'/><rect x='1' y='6' width='10' height='7.5' rx='1.6' fill='${ink}'/></svg>`);
  const CSS = `
dialog.repoDlg{width:min(400px,94vw)}
.repoDlg .dlgBody{padding-bottom:6px}
.repoDlg .dlgBody p{margin:0 0 12px;line-height:1.5;color:var(--ink70)}
.repoDlg .repoNote{font-size:12px;color:var(--ink45);margin:-4px 0 10px}
.repoDlg .field{margin-bottom:4px}
.repoDlg .repoMsg{min-height:20px;font-size:12px;line-height:20px;color:var(--ink70);display:flex;align-items:center;gap:7px}
.repoDlg .repoMsg.bad{color:#8a3a26}
.repoDlg .repoMsg .repoSpin{width:11px;height:11px;border:2px solid rgba(0,0,0,.15);border-top-color:currentColor;border-radius:50%;animation:repoSpin .7s linear infinite;flex:0 0 auto}
@keyframes repoSpin{to{transform:rotate(360deg)}}
[data-repo]{--repoShut:url("${padlock(SHUT, "black")}");--repoOpen:url("${padlock(OPEN, "black")}")}
.btn[data-repo]::after{content:"";display:inline-block;width:9px;height:10px;margin-left:6px;vertical-align:-1px;background:currentColor;opacity:.5;-webkit-mask:var(--repoShut) center/contain no-repeat;mask:var(--repoShut) center/contain no-repeat}
.btn[data-repo].repoOpen::after{-webkit-mask-image:var(--repoOpen);mask-image:var(--repoOpen);opacity:.38}
input[data-repo]{padding-right:18px!important;background-repeat:no-repeat;background-position:right 5px center;background-size:8px 9px;background-image:url("${padlock(SHUT, "#9a9486")}")}
input[data-repo].repoOpen{background-image:url("${padlock(OPEN, "#9a9486")}")}
input[data-repo].repoLocked,select[data-repo].repoLocked{border-style:dashed!important}
`;
  function css() { try { if (!doc || doc.getElementById("cnRepoLockCss")) return; const s = doc.createElement("style"); s.id = "cnRepoLockCss"; s.textContent = CSS; doc.head.appendChild(s); } catch (_) { /* unstyled, still works */ } }
  const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

  /** via: the person holds a standing session and this is a delete: the dialog shows its confirm sentence with no password field, and takes a single-use token from the session. */
  function openDialog(action, ask, via) {
    return new Promise(resolve => {
      css();
      const W = WORDS[action] || WORDS.other, a = ask || {};
      const d = doc.createElement("dialog"); d.className = "splitDlg repoDlg"; d.dataset.repoAction = action;
      d.setAttribute("aria-labelledby", "repoT"); d.setAttribute("aria-describedby", "repoP");
      d.innerHTML = `<div class="dlg"><div class="dlgHead"><h3 id="repoT">${esc(a.title || W.title)}</h3></div>
        <div class="dlgBody"><p id="repoP">${esc(a.message || a.what || W.what)}</p>${a.note ? `<div class="repoNote">${esc(a.note)}</div>` : ""}
          <div class="field" id="repoField"><label for="repoPw">Password</label><input id="repoPw" type="password" name="repo-unlock-field" autocomplete="off" autocapitalize="off" autocorrect="off" spellcheck="false" data-lpignore="true" data-1p-ignore data-form-type="other" aria-describedby="repoMsg"></div>
          <div class="repoMsg" id="repoMsg" role="status" aria-live="polite"></div></div>
        <div class="dlgFoot"><div class="left"><button type="button" class="btn ghost sm" data-k="cancel">Cancel</button></div><button type="button" class="btn gold sm" data-k="go">${esc(a.go || W.go)}</button></div></div>`;
      (doc.body || doc.documentElement).appendChild(d);
      const input = d.querySelector("#repoPw"), msg = d.querySelector("#repoMsg"), go = d.querySelector("[data-k=go]"), cancel = d.querySelector("[data-k=cancel]"), field = d.querySelector("#repoField");
      let busy = false, done = false, hold = 0;
      if (via) { field.hidden = true; d.dataset.repoVia = "session"; }
      const say = (text, kind) => { msg.className = "repoMsg" + (kind === "bad" ? " bad" : ""); msg.textContent = ""; if (kind === "spin") { const s = doc.createElement("span"); s.className = "repoSpin"; msg.appendChild(s); msg.appendChild(doc.createTextNode(text)); } else msg.textContent = text; };
      const wait = on => { busy = on; input.disabled = on; go.disabled = on || Date.now() < lockedUntil; cancel.disabled = on; };
      const finish = info => { if (done) return; done = true; clearTimeout(hold); input.value = ""; try { if (d.open) d.close(); } catch (_) { d.removeAttribute("open"); } d.remove(); resolve(info); };
      const lockLine = () => { say(SAY.locked, "bad"); go.disabled = true; clearTimeout(hold); hold = setTimeout(() => { if (!done && !busy) { go.disabled = false; say(""); } }, Math.max(0, lockedUntil - Date.now()) + 50); };
      /* a delete with a standing session: the confirm sentence is the dialog; no password, no attempt record (the server notes who deleted what) */
      async function submitSession() {
        const s0 = loadSession();
        if (!s0) { via = false; field.hidden = false; delete d.dataset.repoVia; say("The unlock has ended. Enter the password again.", "bad"); try { input.focus(); } catch (_) { /* gone */ } return; }
        wait(true); say("Checking…", "spin");
        const ans = await unlockCall({ sessionToken: s0.t, action, target: String(a.target || "").slice(0, 80), by: who(), tab: tabId() });
        wait(false);
        if (ans && ans.ok === true && typeof ans.token === "string" && ans.token) { finish({ action, token: ans.token, expiresAt: +ans.expiresAt > 0 ? +ans.expiresAt : Date.now() + 2 * 60 * 1000, once: true }); return; }
        if (ans && ans.error === "session") { drop({ session: true, token: s0.t }); via = false; field.hidden = false; delete d.dataset.repoVia; say("The unlock has ended. Enter the password again.", "bad"); try { input.focus(); } catch (_) { /* gone */ } return; }
        say("The check is not available right now. Please try again later.", "bad");
      }
      async function submit() {
        if (busy || done) return;
        if (via) return submitSession();
        if (Date.now() < lockedUntil) { lockLine(); return; }
        let pw = input.value; input.value = "";                              // the field is clear from here on, whatever happens
        if (!pw) { say(SAY.empty, "bad"); input.focus(); return; }
        wait(true); say("Checking the password…", "spin");
        let r; try { r = await attempt(action, pw, a); } finally { pw = ""; }
        input.value = "";
        if (r.ok) { wait(false); finish(r.info); return; }
        wait(false);
        if (r.reason === "locked") { lockLine(); go.disabled = true; }
        else if (r.reason === "wrong") { say(SAY.wrong, "bad"); input.focus(); }
        else { say(SAY.unavailable, "bad"); input.focus(); }
      }
      go.onclick = submit;
      cancel.onclick = () => { if (!busy) finish(null); };
      input.addEventListener("keydown", e => { if (e.key === "Enter") { e.preventDefault(); submit(); } });
      d.addEventListener("cancel", e => { e.preventDefault(); if (!busy) finish(null); });
      try { d.showModal(); } catch (_) { d.setAttribute("open", ""); }
      if (Date.now() < lockedUntil) lockLine();
      try { input.focus(); } catch (_) { /* a hidden tab */ }
    });
  }

  /* ── the way in ── */
  async function ensureNow(action, ask) {
    // (ask.fresh: the password is asked even when an unlock is held: Settings, every time it is opened, PERSON 26 Oct 2026)
    if (!ONCE.has(action) && !(ask && ask.fresh)) { const f = find(action); if (f) return f; }
    return openDialog(action, ask, ONCE.has(action) && !!loadSession());
  }
  /** A token for `action`, from the ones held or from the dialog; null when the person cancels. Asks one at a time: a second caller waits and then finds the token the first one got. */
  function ensure(action, ask) {
    action = norm(action);
    const run = queue.then(() => ensureNow(action, ask));
    queue = run.then(() => undefined, () => undefined);
    return run;
  }
  async function call(action, ask, fn) {
    action = norm(action); let a = ask || {}, again = false;
    for (;;) {
      const info = await ensure(action, a);
      if (!info) throw cancelledError();
      let out, err = null;
      try { out = await fn(info.token, info); } catch (e) { err = e; }
      if (info.once || ONCE.has(action)) drop(info);                          // a single-use token is spent, whether or not the call worked
      if (!err) return out;
      if (isRepoLocked(err) && !again) {
        again = true; drop(info);
        const need = err.data && ACTIONS.includes(err.data.need) ? err.data.need : action;
        action = need; a = Object.assign({}, a, { note: "The unlock has run out. Enter the password again." });
        continue;
      }
      throw err;
    }
  }

  /* ── the session: Lock now, the "Unlocked until" line, and a look at the server ── */
  /** "5:00 pm", Toronto's clock. */
  function untilText(ms) {
    try { return new Date(+ms).toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit", timeZone: "America/Toronto" }).replace(/\s*([AP])M$/i, (m, x) => " " + x.toLowerCase() + "m"); }
    catch (_) { return ""; }
  }
  /** { open, until }: a session is held now, and when it ends. */
  function status() { const s = loadSession(); return { open: !!s, until: s ? s.e : 0 }; }
  /** Lock now: this computer forgets the session first (so the lock holds even when the server cannot be reached), then the server deletes it. */
  async function lockNow() {
    const s = loadSession(); sess = null; saveSession(); mem = {}; save(); changed();
    if (!s) return { ok: true, server: true };
    const ans = await callOp("repoLock", { sessionToken: s.t });
    return { ok: true, server: !!ans && ans.ok === true };
  }
  /** Does the server still honour the held session? false: it ended (dropped here); true: it stands; null: could not be asked (the server still checks every protected call). */
  async function verify() {
    const s = loadSession(); if (!s) return false;
    const ans = await callOp("repoSession", { sessionToken: s.t });
    if (ans && ans.ok === true && ans.active === false) { drop({ session: true, token: s.t }); return false; }
    if (ans && ans.ok === true && ans.active === true) { if (+ans.expiresAt > 0 && +ans.expiresAt !== s.e) { sess = { t: s.t, e: +ans.expiresAt }; saveSession(); changed(); } return true; }
    return null;
  }

  /** A typed password for `action` from a field of the caller's own (no dialog): one try, answered { ok, reason, info?, retryAfterSec? }; a right answer is kept as the
   *  dialog keeps it, so find(action) / unlocked(action) hold a token from here on. The caller clears its field before it calls. */
  async function submitInline(action, password, ask) {
    action = norm(action);
    let pw = password == null ? "" : String(password); password = "";
    try { return await attempt(action, pw, ask); } finally { pw = ""; }
  }

  /* ── the look: a closed padlock until unlocked, an open one while it is ── */
  function mark(scope) {
    const r = scope || doc; if (!r || !r.querySelectorAll) return;
    const memo = {}; // (one look per action, not one per control)
    r.querySelectorAll("[data-repo]").forEach(el => {
      const act = el.dataset.repo, open = act in memo ? memo[act] : (memo[act] = unlockedQuiet(act));
      el.classList.toggle("repoLocked", !open); el.classList.toggle("repoOpen", open);
      if (!el.dataset.repoTitle) { const t = el.getAttribute("title") || ""; el.dataset.repoTitle = t ? "1" : "0"; if (!t) el.setAttribute("title", "Locked: asks for your password"); }
      else if (el.dataset.repoTitle === "0") el.setAttribute("title", open ? "Unlocked for a few minutes" : "Locked: asks for your password");
    });
  }
  const unlockedQuiet = action => { const f = find(action); return !!f; };
  /** A box or select that is locked asks before its first touch (a click or a key), then takes the focus. */
  function gate(el, action, ask) {
    if (!el || el.dataset.repoGate) return; el.dataset.repoGate = "1";
    const block = e => {
      if (unlockedQuiet(action)) return;
      if (e.type === "keydown" && (["Tab", "Shift", "Escape", "Control", "Alt", "Meta", "CapsLock"].includes(e.key) || e.ctrlKey || e.metaKey)) return;
      e.preventDefault(); e.stopPropagation();
      if (el._repoAsking) return; el._repoAsking = true;
      ensure(action, typeof ask === "function" ? ask() : ask).then(t => { el._repoAsking = false; if (t) { try { el.focus(); } catch (_) { /* gone */ } } }, () => { el._repoAsking = false; });
    };
    el.addEventListener("mousedown", block, true); el.addEventListener("keydown", block, true);
  }

  load(); endLegacySession();   // (PERSON: no unlock is shared between tabs or kept across a reload any more, so no storage listener either)
  if (doc) { try { if (doc.readyState === "loading") doc.addEventListener("DOMContentLoaded", () => { css(); changed(); }); else { css(); changed(); } } catch (_) { /* no page */ } }
  const api = { ensure, call, unlocked, find, forget, mark, gate, onChange, isRepoLocked, lockNow, status, verify, untilText, submitInline, personChanged, messages: SAY, cancelled: e => !!e && e.cancelled === true, configure: h => { hook = h || null; }, KEY, _held: () => { load(); return Object.keys(mem); } };
  root.CharmNestRepoLock = api;
  if (typeof module !== "undefined" && module.exports) module.exports = api;
})(typeof window !== "undefined" ? window : globalThis);
