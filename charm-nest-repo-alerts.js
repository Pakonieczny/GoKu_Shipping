/*  charm-nest-repo-alerts.js — the persistent Alert for password attempts on the Charm Repo Library (Paul, 11 Oct 12:51:
 *  "Every attempt to enter a password into any of the protected features in the Charm Repo Library must be recorded and I
 *  must be informed in the UI with a visible Alert notice that is persistent and visible only in my login until I manually
 *  close the Alert."). Contract and split of work: plans/repo-protect-1011/contract.md, REPOAUTH-api.md, REPOALERT-events.md.
 *
 *  Who sees it: ONLY the master admin (CharmNestMasterAdmin.is(), charm-nest-master-admin.js). For anyone else, and while
 *  nobody is signed in, this file draws nothing, sends nothing, keeps no timer and nothing in the browser (a missing helper
 *  counts as "not admin"). The helper's check is a name match, so the server repeats it for repoAlerts / repoAlertClose.
 *
 *  What it is: the run pill's own component (details.runMenu: a dot, two words, a count, a caret; its card is .runMenuBody)
 *  at the right end of the top bar. It exists only while there is an unclosed attempt (or the last read failed). Its card
 *  lists the attempts newest first — who, what, which charm or file, when (Toronto time), right / wrong / locked out — with
 *  ONE Close button. It never hides by itself. The state lives on the server (charmNestLibrary ops repoAlerts and
 *  repoAlertClose over CharmRepo_Alerts), so it survives a reload and shows on every device until Paul closes it; an
 *  attempt made after a close is a new alert. Nothing is kept in the browser.
 *
 *  When it reads (master admin only): page load / sign-in, the tab coming back into view or in focus, right after
 *  `repo-auth-attempt` (the Repo dialog's event, dispatched on window after every attempt), right after Close, and at most
 *  once a minute while the tab is in view (one timer, armed only while the master admin is signed in).
 *    CharmNestRepoAlerts.refresh() → Promise<count>   read now (not held by the one-minute cap)
 *    CharmNestRepoAlerts.count()   → unclosed attempts listed        .isShown() → the pill is on screen
 *    CharmNestRepoAlerts.options = { checkMs, chunk, zone, now }   (tests)                                                  */
(function (root) {
  "use strict";
  if (root.CharmNestRepoAlerts) return;
  const options = { checkMs: 60000, chunk: 100, zone: "America/Toronto", now: () => Date.now() };
  const st = { on: false, alerts: [], total: 0, more: false, loaded: false, busy: false, closing: false, err: "", closeErr: "", lastStart: -Infinity,
    seq: 0, run: null, again: false, gone: new Set(), el: null, listSig: "", shownN: 0, timer: 0, started: false };
  const now = () => { try { return +options.now() || Date.now(); } catch (_) { return Date.now(); } };
  const doc = () => root.document || null;
  const visible = () => { const d = doc(); return !!d && d.visibilityState !== "hidden"; };

  /* ── who: the helper charm-nest-master-admin.js; anything else is "not admin" ── */
  const adminNow = () => { try { const h = root.CharmNestMasterAdmin; return !!(h && typeof h.is === "function" && h.is() === true); } catch (_) { return false; } };
  const loginNow = () => { try { const h = root.CharmNestMasterAdmin; return h && typeof h.login === "function" ? String(h.login() || "").trim().slice(0, 60) : ""; } catch (_) { return ""; } };

  /* ── what the server sent, in one shape (an unknown field is dropped, a bad row is skipped) ── */
  const msOf = v => { if (typeof v === "number") return v; if (v && typeof v === "object") { if (typeof v.toMillis === "function") return v.toMillis(); const s = v._seconds != null ? v._seconds : v.seconds; if (s != null) return +s * 1000; } return +v || 0; };
  function norm(a) {
    if (!a || typeof a !== "object" || a.id == null || a.id === "") return null;
    const kind = a.ok === true || a.reason === "ok" ? "ok" : a.reason === "locked" ? "locked" : a.reason === "unavailable" ? "unavailable" : a.reason === "wrong" || a.ok === false ? "wrong" : "other";
    return { id: String(a.id), at: msOf(a.at), by: String(a.by == null ? "" : a.by).trim().slice(0, 60), action: String(a.action == null ? "" : a.action).trim().slice(0, 24),
      target: String(a.target == null ? "" : a.target).trim().slice(0, 160), kind };
  }
  function take(list) {
    const seen = new Set(), out = [];
    for (const raw of Array.isArray(list) ? list : []) { const a = norm(raw); if (a && !seen.has(a.id) && !st.gone.has(a.id)) { seen.add(a.id); out.push(a); } }
    return out.sort((x, y) => (y.at - x.at) || (x.id < y.id ? 1 : x.id > y.id ? -1 : 0));
  }

  /* ── words ── */
  const KIND = { wrong: "Wrong password", locked: "Locked out", ok: "Right password", unavailable: "Could not check", other: "Attempt" };
  const DOES = { upload: "upload", delete: "delete", download: "download", modify: "change", history: "open the upload history", other: "unlock a protected feature" };
  const fmt = {};
  const stamp = ms => {
    try {
      const z = options.zone, y = new Intl.DateTimeFormat("en-US", { timeZone: z, year: "numeric" });
      const same = y.format(new Date(ms)) === y.format(new Date(now()));
      const key = z + (same ? "" : "y");
      fmt[key] = fmt[key] || new Intl.DateTimeFormat("en-US", Object.assign({ timeZone: z, month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZoneName: "short" }, same ? {} : { year: "numeric" }));
      return fmt[key].format(new Date(ms));
    } catch (_) { return ms ? new Date(ms).toISOString().slice(0, 16).replace("T", " ") + " UTC" : "time unknown"; }
  };
  const iso = ms => { try { return new Date(ms).toISOString(); } catch (_) { return ""; } };
  function words(e) {
    const s = e && e.status, why = e && e.data && e.data.error;
    if (s === 403 || s === 401) return "the server did not accept this login for alerts";
    if (s >= 500 && why === "unavailable") return "the server could not read the alert list";
    if (e && e.transient) return "the cloud did not answer";
    const m = String((e && (e.message || why)) || "").replace(/\s+/g, " ").trim();
    return (m || "the cloud sent no answer").slice(0, 140);
  }

  /* ── the pill and its card: the run pill's own classes; a few tints and the entry rows are added here ── */
  const CSS = `
.raWrap{flex:0 0 auto;display:flex;align-items:center}.raWrap[hidden]{display:none}
.raWrap .runMenu{max-width:none}
.topbar .raWrap.bad>.runMenu>summary{background:var(--claySoft);border-color:#e3c8bf;color:#8a3a26}
.topbar .raWrap.info>.runMenu>summary{background:var(--slateSoft);border-color:#cbd9de;color:#33525d}
.raWrap.bad .rbDot{background:var(--clay)}.raWrap.info .rbDot{background:var(--slate)}
.raWrap .rbCaret{color:inherit;opacity:.6}
.raWrap>.runMenu>summary:hover,.raWrap>.runMenu[open]>summary{filter:brightness(.97)}
.raWrap.info .rbCxN{background:var(--slate)}
.raWrap .runMenuBody{width:340px}
@container workspace (max-width:1100px){.raWrap .rbText,.raWrap .rbCaret{display:none}.topbar .raWrap .runMenu>summary{padding:4px 7px;gap:5px}}
@container workspace (max-width:900px){.raWrap .rbDot{display:none}.topbar .raWrap .runMenu>summary{padding:3px 3px}}
.raList{display:grid;gap:6px;padding:0 0 10px}.raList:empty{display:none}
.raItem{display:grid;gap:2px;padding:8px 10px;border-radius:9px;border:1px solid #e3c8bf;background:var(--claySoft);color:#8a3a26;font-size:11.5px;line-height:1.45;overflow-wrap:anywhere}
.raItem.locked{border-color:var(--clay)}
.raItem.ok{border-color:#cfdcc9;background:var(--sageSoft);color:#3c5a39}
.raItem:is(.other,.unavailable){border-color:var(--line);background:var(--card2,var(--paper2));color:var(--ink70)}
.raTop{display:flex;justify-content:space-between;align-items:baseline;gap:8px}
.raTop b{font-size:11.5px}
.raWhen{font:10px var(--mono);opacity:.85;white-space:nowrap}
.raTarget{font-family:var(--mono);font-size:10.5px}
.raErr{margin:0 0 10px;padding:7px 10px;border-radius:9px;background:var(--claySoft);border:1px solid #e3c8bf;color:#8a3a26;font-size:11.5px;line-height:1.45;overflow-wrap:anywhere}
.raErr[hidden]{display:none}
.raActs{align-items:center;padding-bottom:2px}
.raNote{flex:1 1 100%;font-size:10.5px;color:var(--ink45)}
.raWait{display:inline-flex;align-items:center;gap:6px;font-size:10.5px;color:var(--ink70);animation:raIn .2s .4s both}
.raWait[hidden]{display:none}
.raWrap .spin{width:11px;height:11px;border:2px solid var(--line);border-top-color:var(--ink70);border-radius:50%;animation:spin .7s linear infinite;flex:0 0 11px;display:inline-block}
@keyframes raIn{from{opacity:0}to{opacity:1}}
@media (prefers-reduced-motion:reduce){.raWrap .spin{animation:none}.raWait{animation:none}}`;
  function mk(tag, cls, text) { const e = doc().createElement(tag); if (cls) e.className = cls; if (text != null) e.textContent = text; return e; }
  function mount() {
    const d = doc(); if (!d) return null;
    if (st.el && st.el.isConnected) return st.el;
    const host = d.querySelector(".topbar .topTools") || d.querySelector(".topbar");
    if (!host) return null;                                        // (a page without the sorter's top bar draws nothing)
    if (!d.getElementById("cnRepoAlertCss")) { const s = mk("style"); s.id = "cnRepoAlertCss"; s.textContent = CSS; (d.head || d.documentElement).appendChild(s); }
    const el = mk("div", "raWrap"); el.id = "cnRepoAlert"; el.hidden = true; el.setAttribute("role", "status"); el.setAttribute("aria-live", "polite");
    el.innerHTML = `<details class="runMenu raMenu"><summary><i class="rbDot"></i><span class="rbText">Repo alert</span><i class="rbCxN raN" hidden></i><span class="rbCaret" aria-hidden="true">⌄</span></summary>
<div class="runMenuBody raCard"><div class="runDetail"><b>Charm Repo password attempts</b><small class="raSum"></small></div>
<div class="raErr" hidden></div><div class="raList"></div>
<div class="rbActs raActs"><button type="button" class="btn sm raClose">Close alert</button><span class="raWait" hidden><span class="spin" aria-hidden="true"></span><span class="raWaitText">Checking…</span></span><span class="raNote">Newest first, times in Toronto. Closing marks the attempts listed here as seen; their records are kept.</span></div></div></details>`;
    host.appendChild(el);
    el.querySelector(".raClose").addEventListener("click", closeNow);
    st.el = el; st.listSig = ""; return el;
  }
  function unmount() {
    if (st.el) { try { st.el.remove(); } catch (_) {} }
    st.el = null; st.listSig = ""; st.shownN = 0;
  }
  function item(a) {
    const row = mk("div", "raItem " + a.kind); row.dataset.id = a.id;
    const top = mk("div", "raTop"), when = mk("time", "raWhen", a.at ? stamp(a.at) : "time unknown");
    if (a.at) when.setAttribute("datetime", iso(a.at));
    top.append(mk("b", "", KIND[a.kind]), when);
    const line = mk("div", "raLine");
    line.append(mk("b", "", a.by || "Unknown login"), doc().createTextNode(" tried to " + (DOES[a.action] || (a.action ? "use “" + a.action + "”" : "unlock a protected feature"))));
    if (a.target) { line.append(doc().createTextNode(" ")); const t = mk("span", "raTarget", a.target); t.title = a.target; line.append(t); }
    if (a.kind === "locked") line.append(doc().createTextNode(" · too many wrong tries"));
    if (a.kind === "unavailable") line.append(doc().createTextNode(" · the password could not be checked"));
    row.append(top, line); return row;
  }
  /** how many are not closed: what is listed, or the server's own total when it holds back some ("300+" is its cap) */
  function totalLabel() {
    const n = st.alerts.length;
    if (!st.more) return String(n);
    const t = Math.max(+st.total || 0, n + 1);
    return t >= 300 ? "300+" : String(t);
  }
  function paint() {
    if (!st.on) { unmount(); return; }
    const n = st.alerts.length, by = k => st.alerts.filter(a => a.kind === k).length, w = by("wrong"), l = by("locked"), k = by("ok"), u = by("unavailable");
    if (!n && !st.err) {                                            // nothing to say: no pill (it comes back with the next attempt)
      if (st.el) { st.el.hidden = true; const m = st.el.querySelector("details"); if (m) m.open = false; }
      st.shownN = 0; return;
    }
    const el = mount(); if (!el) return;
    const bad = w + l > 0 || (!n && !!st.err), label = totalLabel();
    el.hidden = false; el.classList.toggle("bad", bad); el.classList.toggle("info", !bad);
    const num = el.querySelector(".raN"), sum = el.querySelector(".raSum"), summary = el.querySelector("summary");
    el.querySelector(".rbText").textContent = n ? "Repo alert" : "Repo alert · not read";
    num.hidden = false; num.textContent = n ? label : "!";           // (the bubble stays when the words are dropped for room: a count, or "!" when it could not read)
    if (n > st.shownN && st.shownN > 0) { num.style.animation = "none"; void num.offsetWidth; num.style.animation = ""; }   // (a new attempt: the count pops in again)
    st.shownN = n;
    const head = n ? `${label} password attempt${label === "1" ? "" : "s"} on the Charm Repo Library not closed yet` : "Password attempts on the Charm Repo Library could not be read";
    summary.setAttribute("aria-label", head); summary.title = head + ". Open to read" + (n ? " and close." : ".");
    sum.textContent = n ? [`${label} not closed`, w ? `${w} wrong` : "", l ? `${l} locked out` : "", k ? `${k} right` : "", u ? `${u} not checked` : "", st.more ? `newest ${n} listed` : ""].filter(Boolean).join(" · ") : "";
    const err = el.querySelector(".raErr"), msg = st.closeErr ? `Not closed: ${st.closeErr}. The alert stays; press Close alert to try again.` : st.err ? `Not read just now: ${st.err}. Trying again within a minute.` : "";
    err.hidden = !msg; if (err.textContent !== msg) err.textContent = msg;
    const sig = st.alerts.map(a => a.id + ":" + a.kind).join(",");
    if (sig !== st.listSig) {                                       // rewritten only when the list changed, so an open card keeps its place
      const list = el.querySelector(".raList"), top = list.scrollTop;
      list.replaceChildren(...st.alerts.map(item)); list.scrollTop = top; st.listSig = sig;
    }
    const btn = el.querySelector(".raClose"), wait = el.querySelector(".raWait");
    btn.disabled = st.closing || !n;
    btn.textContent = st.closing ? "Closing…" : "Close alert";
    wait.hidden = !(st.busy || st.closing);
    el.querySelector(".raWaitText").textContent = st.closing ? "Closing…" : "Checking for new attempts…";
  }

  /* ── the two server calls (through the sorter's own door: CN.api → charmNestLibrary; the server wants `by` = the signed-in login) ── */
  const call = (body) => { const CN = root.CN; if (!CN || typeof CN.api !== "function") return null; const by = loginNow(); return CN.api("charmNestLibrary", by ? Object.assign({ by }, body) : body, { quiet: true, timeoutMs: 30000 }); };
  async function readOnce() {
    const p = call({ op: "repoAlerts" });
    if (!p) return;                                                 // the sorter is still starting: the next look tries again
    const seq = st.seq; st.lastStart = now(); st.busy = true; paint();
    let r = null, err = "";
    try { r = await p; } catch (e) { err = words(e); }
    if (!st.on || seq !== st.seq) return;                           // signed out (or another login) while it was away
    if (!err && r && r.ok !== false && Array.isArray(r.alerts)) { st.alerts = take(r.alerts); st.more = r.more === true; st.total = +r.total || 0; st.err = ""; st.loaded = true; }
    else st.err = err || words({ message: r && r.error }) || "the server sent no list";
  }
  function read() {
    if (st.run) { st.again = true; return st.run; }                 // one read at a time; a call meanwhile gets one more after it
    st.run = (async () => {
      try { do { st.again = false; await readOnce(); } while (st.again && st.on); }
      catch (e) { st.err = words(e); }
      finally { st.run = null; st.busy = false; paint(); arm(options.checkMs); }
      return st.alerts.length;
    })();
    return st.run;
  }
  async function closeNow() {
    if (st.closing || !st.on || !st.alerts.length) return;
    const ids = st.alerts.map(a => a.id);                           // exactly the attempts on the card
    st.closing = true; st.closeErr = ""; paint();
    let failed = "";
    try {
      for (let i = 0; i < ids.length; i += options.chunk) {
        const part = ids.slice(i, i + options.chunk), p = call({ op: "repoAlertClose", ids: part });
        if (!p) throw new Error("the sorter is still starting");
        const r = await p;
        if (!r || r.ok === false) throw new Error((r && r.error) || "the server did not confirm");
        const done = new Set(part); part.forEach(id => st.gone.add(id));   // (a read that left before this cannot bring them back)
        st.alerts = st.alerts.filter(a => !done.has(a.id)); paint();
      }
    } catch (e) { failed = words(e); }
    st.closing = false; st.closeErr = failed; paint();
    if (st.on) read();                                              // what arrived meanwhile (or what the server held back) is shown now
  }

  /* ── on and off with the login; the one timer exists only while the master admin is signed in ── */
  function arm(ms) { disarm(); if (st.on && typeof root.setTimeout === "function") st.timer = root.setTimeout(tick, Math.max(1000, ms)); }
  function disarm() { if (st.timer) { try { root.clearTimeout(st.timer); } catch (_) {} st.timer = 0; } }
  const onAttempt = () => { if (st.on) read(); };
  function turnOn() {
    st.on = true; st.seq++; st.alerts = []; st.total = 0; st.more = false; st.loaded = false; st.err = ""; st.closeErr = ""; st.lastStart = -Infinity;
    root.addEventListener("repo-auth-attempt", onAttempt);
    if (visible()) read();                                          // page load / sign-in: the first look
  }
  function turnOff() {
    st.on = false; st.seq++; st.alerts = []; st.total = 0; st.more = false; st.loaded = false; st.err = ""; st.closeErr = ""; st.closing = false; st.busy = false; st.again = false;
    disarm();
    try { root.removeEventListener("repo-auth-attempt", onAttempt); } catch (_) {}
    unmount();
  }
  function evaluate() { const a = adminNow(); if (a !== st.on) { if (a) turnOn(); else turnOff(); } return st.on; }
  /** The timer, a tab coming back and a focus: look at who is signed in (memory only), and for the master admin read at most once a minute while the tab is in view. */
  function tick() {
    disarm();
    if (!evaluate()) return 0;
    if (!visible()) { disarm(); return st.alerts.length; }          // nothing while hidden; coming back into view reads
    const wait = options.checkMs - (now() - st.lastStart);
    if (wait <= 0) read(); else arm(wait);
    return st.alerts.length;
  }
  function start() {
    if (st.started) return; st.started = true;
    evaluate();
    root.addEventListener("focus", tick);
    root.addEventListener("charmnest:master-admin", tick);          // the helper's own announcement: the login or the Admin answer changed
    const d = doc(); if (d) d.addEventListener("visibilitychange", tick);
  }

  root.CharmNestRepoAlerts = {
    refresh: () => (evaluate() ? read() : Promise.resolve(0)),
    count: () => (st.on ? st.alerts.length : 0),
    isShown: () => !!(st.on && st.el && st.el.isConnected && !st.el.hidden),
    options
  };
  const d0 = doc();
  if (d0 && d0.readyState === "loading") d0.addEventListener("DOMContentLoaded", start, { once: true }); else start();
})(typeof window !== "undefined" ? window : globalThis);
