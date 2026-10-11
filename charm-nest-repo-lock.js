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
 *    CharmNestRepoLock.forget(action?)          → drop held tokens (all when no action)
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
  const KEY = "cn.repoLock.v1", TAB = "cn.repoTab.v1", MARGIN = 5000;
  const ACTIONS = ["upload", "delete", "download", "modify", "history", "other"];
  /* which held token may serve which need (REPOAUTH-api.md: an upload token also serves masterPatch, "other" serves every
     upload / modify / download op and never a delete; delete and history tokens are single use and are never held) */
  const COVERS = { upload: ["upload", "other"], modify: ["modify", "upload", "other"], download: ["download", "other"], delete: ["delete"], history: ["history"], other: ["other"] };
  const ONCE = new Set(["delete", "history"]);
  const norm = a => (ACTIONS.includes(a) ? a : "other");
  const WORDS = {
    upload: { title: "Add to the charm repo", what: "Adding a master file to the charm repo.", go: "Unlock" },
    delete: { title: "Remove from the charm repo", what: "Removing from the charm repo.", go: "Remove" },
    download: { title: "Download a charm file", what: "Downloading a charm file from the repo.", go: "Download" },
    modify: { title: "Change a charm in the repo", what: "Changing a charm in the repo.", go: "Unlock" },
    history: { title: "Charm repo history", what: "Changing the charm repo history.", go: "Unlock" },
    other: { title: "Charm repo password", what: "This part of the charm repo is locked.", go: "Unlock" }
  };

  let hook = null;                      // tests: { api }
  let mem = {};                         // action → { t, e }: the tokens held (a single-use one is never put here)
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
  function prune() {
    const now = Date.now();
    for (const k of Object.keys(mem)) { const x = mem[k]; if (!ACTIONS.includes(k) || ONCE.has(k) || !x || typeof x.t !== "string" || !x.t || !(+x.e > now + MARGIN)) delete mem[k]; }
  }
  function find(need) {
    load();
    for (const a of COVERS[norm(need)] || []) { const x = mem[a]; if (x) return { action: a, token: x.t, expiresAt: +x.e, once: false }; }
    return null;
  }
  function remember(info) {
    if (info.once || ONCE.has(info.action)) return;
    mem[info.action] = { t: info.token, e: info.expiresAt }; save(); changed();
  }
  function drop(info) {
    if (!info) return;
    load(); if (mem[info.action] && mem[info.action].t === info.token) { delete mem[info.action]; save(); }
    changed();
  }
  function forget(action) {
    load(); if (action) delete mem[norm(action)]; else mem = {};
    save(); changed();
  }
  const unlocked = action => !!find(action);
  function onChange(fn) { listeners.add(fn); return () => listeners.delete(fn); }
  function changed() {
    clearTimeout(expiryTimer);
    const now = Date.now(); let next = Infinity; for (const k of Object.keys(mem)) { const x = mem[k]; if (x && +x.e - MARGIN > now) next = Math.min(next, +x.e - MARGIN); }
    if (next < Infinity) expiryTimer = setTimeout(() => { prune(); save(); changed(); }, Math.min(2147483000, Math.max(250, next - now + 50)));
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
  async function unlockCall(body) {
    const api = getApi();
    if (typeof api !== "function") return { ok: false, error: "unavailable" };
    try { return await api("charmNestLibrary", Object.assign({ op: "repoUnlock" }, body), { quiet: true }); }
    catch (e) { return answerOf(e) || { ok: false, error: "unavailable" }; }
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

  function openDialog(action, ask) {
    return new Promise(resolve => {
      css();
      const W = WORDS[action] || WORDS.other, a = ask || {};
      const d = doc.createElement("dialog"); d.className = "splitDlg repoDlg"; d.dataset.repoAction = action;
      d.setAttribute("aria-labelledby", "repoT"); d.setAttribute("aria-describedby", "repoP");
      d.innerHTML = `<div class="dlg"><div class="dlgHead"><h3 id="repoT">${esc(a.title || W.title)}</h3></div>
        <div class="dlgBody"><p id="repoP">${esc(a.message || a.what || W.what)}</p>${a.note ? `<div class="repoNote">${esc(a.note)}</div>` : ""}
          <div class="field"><label for="repoPw">Password</label><input id="repoPw" type="password" name="repo-unlock-field" autocomplete="off" autocapitalize="off" autocorrect="off" spellcheck="false" data-lpignore="true" data-1p-ignore data-form-type="other" aria-describedby="repoMsg"></div>
          <div class="repoMsg" id="repoMsg" role="status" aria-live="polite"></div></div>
        <div class="dlgFoot"><div class="left"><button type="button" class="btn ghost sm" data-k="cancel">Cancel</button></div><button type="button" class="btn gold sm" data-k="go">${esc(a.go || W.go)}</button></div></div>`;
      (doc.body || doc.documentElement).appendChild(d);
      const input = d.querySelector("#repoPw"), msg = d.querySelector("#repoMsg"), go = d.querySelector("[data-k=go]"), cancel = d.querySelector("[data-k=cancel]");
      let busy = false, done = false, hold = 0;
      const say = (text, kind) => { msg.className = "repoMsg" + (kind === "bad" ? " bad" : ""); msg.textContent = ""; if (kind === "spin") { const s = doc.createElement("span"); s.className = "repoSpin"; msg.appendChild(s); msg.appendChild(doc.createTextNode(text)); } else msg.textContent = text; };
      const wait = on => { busy = on; input.disabled = on; go.disabled = on || Date.now() < lockedUntil; cancel.disabled = on; };
      const finish = info => { if (done) return; done = true; clearTimeout(hold); input.value = ""; try { if (d.open) d.close(); } catch (_) { d.removeAttribute("open"); } d.remove(); resolve(info); };
      const lockLine = () => { say("Too many wrong tries. Please try again later.", "bad"); go.disabled = true; clearTimeout(hold); hold = setTimeout(() => { if (!done && !busy) { go.disabled = false; say(""); } }, Math.max(0, lockedUntil - Date.now()) + 50); };
      async function submit() {
        if (busy || done) return;
        if (Date.now() < lockedUntil) { lockLine(); return; }
        let pw = input.value; input.value = "";                              // the field is clear from here on, whatever happens
        if (!pw) { say("Type the password first.", "bad"); input.focus(); return; }
        wait(true); say("Checking the password…", "spin");
        let ans; try { ans = await unlockCall({ password: pw, action, target: String(a.target || "").slice(0, 80), by: who(), tab: tabId(), ...(a.overwrite ? { overwrite: true } : {}) }); } finally { pw = ""; }
        input.value = "";
        const ok = !!ans && ans.ok === true && typeof ans.token === "string" && ans.token.length > 0;
        const reason = ok ? "ok" : ans && (ans.error === "wrong" || ans.error === "locked") ? ans.error : "unavailable";
        try { root.dispatchEvent(new root.CustomEvent("repo-auth-attempt", { detail: { action, ok, reason } })); } catch (_) { /* the Alert is optional */ }
        if (ok) {
          const once = ans.once === true || ONCE.has(action);
          const info = { action: ACTIONS.includes(ans.action) ? ans.action : action, token: ans.token, expiresAt: +ans.expiresAt > 0 ? +ans.expiresAt : Date.now() + 5 * 60 * 1000, once };
          remember(info); wait(false); finish(info); return;
        }
        wait(false);
        if (reason === "locked") { const s = +ans.retryAfterSec; lockedUntil = Date.now() + (s > 0 ? Math.min(s, 3600) : 900) * 1000; lockLine(); go.disabled = true; }
        else if (reason === "wrong") { say("That password is not right. Try again.", "bad"); input.focus(); }
        else { say("The check is not available right now. Please try again later.", "bad"); input.focus(); }
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
    if (!ONCE.has(action)) { const f = find(action); if (f) return f; }
    return openDialog(action, ask);
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

  /* ── the look: a closed padlock until unlocked, an open one while it is ── */
  function mark(scope) {
    const r = scope || doc; if (!r || !r.querySelectorAll) return;
    r.querySelectorAll("[data-repo]").forEach(el => {
      const open = unlockedQuiet(el.dataset.repo);
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

  load();
  if (doc) { try { if (doc.readyState === "loading") doc.addEventListener("DOMContentLoaded", () => { css(); changed(); }); else { css(); changed(); } } catch (_) { /* no page */ } }
  const api = { ensure, call, unlocked, find, forget, mark, gate, onChange, isRepoLocked, cancelled: e => !!e && e.cancelled === true, configure: h => { hook = h || null; }, KEY, _held: () => { load(); return Object.keys(mem); } };
  root.CharmNestRepoLock = api;
  if (typeof module !== "undefined" && module.exports) module.exports = api;
})(typeof window !== "undefined" ? window : globalThis);
