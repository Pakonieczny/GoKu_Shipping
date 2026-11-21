/* charm-nest-cmd-now.js — NOWCMD (Paul, 21 Nov 2026, after a Change SKU said "Asked of the cloud: The cloud has not started it yet: it does it at its next pass (every minute or so).
   Nothing more is needed here.": "if a user makes a change like this that is important … it must be propagated immediately not on a next pass … if the cloud does not properly update this
   and there's a mistake the user will never know").

   ONE follower for every press that asks the cloud (runCmdPut: Change SKU / keepsync, Just this order, Remake line, the Options sheets grid, Custom send, hop / fill, seat, placewaiting).
   The cloud now takes a person's request up within seconds, even in the middle of a pass (_charmNestDrive.js NOWCMD); this page follows the request until the cloud ANSWERS:
     · while it waits or works, a live line: "The cloud has it — it starts within seconds…" / "The cloud is doing it… <its step>"; never "nothing more is needed" while it is not done;
     · done: the cloud's own words;
     · failed (or cancelled): a red, persistent notice in the panel or card (o.el, or the caller's own line) AND a toast, with the reason and a Try again that sends the same request again;
     · no answer after LATE_MS (5 minutes): said plainly in red ("The cloud has not done it yet — it is still queued; check again or press Try again"), never success, and the request is
       still followed in the background (watch): when the cloud answers at last, the answer is shown (toast and notice), a failure with its Try again.
   API (window.CNCmdNow):
     follow(runId, id, o) → { ok, failed?, late?, cancelled?, text, step?, runId, id, retry() }      o: { api, onStep(text, cmd), onAnswer(res), maxMs, gapMs, label, el, toast, toastOk, resend | onRetry, watch }
     put(cmd, o)          → { runId, id } | { error }       (runCmdPut on the followed run, as this computer, in the signed-in person's name)
     ask(cmd, o)          → put, then follow (its Try again puts the same request again)
     watch(runId, id, o)  → the request followed in the background until it is answered (6 hours at most); its answer shown then
     shout(res, o)        → a failure or a late answer shown (toast with Try again, notice in o.el) by a caller that follows by itself
   Mode "off": never called (the callers only use it while the cloud carries the run). No Etsy, no Firebase: only the page's own api (CN.api → charmNestLibrary). */
(function (root) {
  "use strict";
  const LATE_MS = 300000, WATCH_MS = 6 * 3600000, WATCH_GAP_MS = 15000;
  const LATE = "The cloud has not done it yet — it is still queued; check again or press Try again";
  const LATE_RUNNING = step => `The cloud has not done it yet — it is still at “${String(step || "working").replace(/[.…]+$/, "")}”; check again, or press Try again if it stays there`;
  const QUEUED = "The cloud has it — it starts within seconds…";
  const S = { watching: new Map(), shown: [] };   // (shown: every answer this page showed, for the page's tests)
  const safe = f => { try { return f(); } catch (_) { return undefined; } };
  const nap = ms => new Promise(r => setTimeout(r, ms));
  const defApi = body => (root.CN && typeof root.CN.api === "function" ? root.CN.api("charmNestLibrary", body, { quiet: true }) : Promise.reject(new Error("the page is not ready")));
  const doc = () => root.document || null;
  const clean = t => String(t == null ? "" : t).trim().replace(/\.$/, "");

  /* ── the words ── */
  function live(c, t0) {
    if (c && c.state === "running") return `The cloud is doing it…${c.step ? " " + String(c.step) : ""}`;
    const m = Math.floor((Date.now() - (+((c && c.at) || t0) || Date.now())) / 60000);
    return m >= 1 ? `The cloud has it queued — it has not started yet (asked ${m} min ago)…` : QUEUED;
  }
  function settle(c) {
    if (!c) return null;
    if (c.state === "done") return { ok: true, text: String(c.text || "Done by the cloud") };
    if (c.state === "failed") return { ok: false, failed: true, text: String(c.text || "The cloud could not do it") };
    if (c.state === "cancelled") return { ok: false, failed: true, cancelled: true, text: String(c.text || "Cancelled before it started: nothing was done") };
    return null;
  }

  /* ── the notice in a panel or card, and the toast ── */
  const CSS = ".cnNow{margin:6px 0;padding:8px 10px;border-radius:8px;font:13px/1.35 var(--sans,system-ui,sans-serif);display:flex;flex-wrap:wrap;gap:6px 10px;align-items:center}" +
    ".cnNow[data-k=work]{background:var(--paper2,#f5f1e8);color:var(--ink70,#4a4439)}.cnNow[data-k=ok]{background:#e8f3ec;color:#1f5c38}" +
    ".cnNow[data-k=bad],.cnNow[data-k=late]{background:#fbeaea;color:#8c1d18;border:1px solid #e3a9a5}.cnNow .cnNowT{flex:1 1 220px}.cnNow button{font:inherit}";
  function style() { const d = doc(); if (!d || d.getElementById("cnNowCss")) return; const s = d.createElement("style"); s.id = "cnNowCss"; s.textContent = CSS; (d.head || d.documentElement).appendChild(s); }
  /** The notice in el: kind work | ok | bad | late; acts [{ label, fn }]. Persistent: it stays until the next answer replaces it (or the card goes). */
  function notice(el, kind, text, acts) {
    const d = doc(); if (!d || !el || typeof el.appendChild !== "function") return null;
    style();
    let n = el.querySelector && el.querySelector(":scope > .cnNow");
    if (!n) { n = d.createElement("div"); n.className = "cnNow"; el.appendChild(n); }
    n.setAttribute("role", kind === "bad" || kind === "late" ? "alert" : "status"); n.dataset.k = kind; n.innerHTML = ""; const t = d.createElement("span"); t.className = "cnNowT"; t.textContent = String(text || ""); n.appendChild(t);
    for (const a of acts || []) {
      const b = d.createElement("button"); b.type = "button"; b.className = "btn xs" + (a.ghost ? " ghost" : ""); b.textContent = a.label; b.setAttribute("data-cn-now", a.label);
      b.onclick = e => { safe(() => e.stopPropagation()); b.disabled = true; Promise.resolve().then(a.fn).catch(() => {}); };
      n.appendChild(b);
    }
    return n;
  }
  /** A toast with buttons beside its words (the page's toast answers its element; "bad" toasts stay until dismissed). */
  function toast(text, tone, ms, acts) {
    const el = safe(() => (typeof root.toast === "function" ? root.toast(text, tone, ms) : null));
    const d = doc();
    safe(() => {
      if (!el || !d || typeof el.appendChild !== "function") return;
      for (const a of acts || []) {
        if (el.querySelector && el.querySelector(`[data-cn-now="${a.label}"]`)) continue;
        const b = d.createElement("button"); b.type = "button"; b.className = "btn xs"; b.setAttribute("data-cn-now", a.label); b.textContent = a.label; b.style.marginLeft = "8px";
        b.onclick = e => { safe(() => e.stopPropagation()); b.disabled = true; safe(() => el._go && el._go()); Promise.resolve().then(a.fn).catch(() => {}); };
        const x = el.querySelector && el.querySelector(".c"); if (x && typeof el.insertBefore === "function") el.insertBefore(b, x); else el.appendChild(b);
      }
    });
    return el;
  }
  /** An answer shown: the notice in o.el, the toast (a failure or a late answer always, a success when o.toastOk), o.onAnswer. */
  function show(res, o) {
    o = o || {}; const label = clean(o.label), lead = label ? label + ": " : "";
    S.shown.push({ id: res.id, ok: !!res.ok, failed: !!res.failed, late: !!res.late, text: res.text, label, at: Date.now() }); if (S.shown.length > 50) S.shown.shift();
    const acts = [];
    if (!res.ok && typeof res.retry === "function" && (!res.late || res.queued)) acts.push({ label: "Try again", fn: () => res.retry() });
    if (res.late) acts.push({ label: "Check again", ghost: true, fn: () => checkAgain(res, o) });
    if (o.el) notice(o.el, res.ok ? "ok" : res.late ? "late" : "bad", res.ok ? `${lead}${res.text}` : res.late ? `${lead}${res.text}` : `${lead}not done — ${clean(res.text)}`, acts);
    if (o.toast !== false) {
      if (res.ok) { if (o.toastOk) toast(`${lead}${res.text}`, "ok", 7000); }
      else toast(res.late ? `${lead}${res.text}` : `${lead}the cloud did not do it — ${clean(res.text)}`, "bad", 0, acts);
    }
    safe(() => typeof o.onAnswer === "function" && o.onAnswer(res));
    return res;
  }
  async function checkAgain(res, o) {
    const api = (o && o.api) || defApi;
    let c = null; try { const l = await api({ op: "runCmdList", runId: res.runId }); c = ((l && l.cmds) || []).find(x => x && x.id === res.id) || null; } catch (_) { c = null; }
    const end = settle(c);
    if (end) return show(Object.assign(withRetry(end, res.runId, res.id, o)), o);
    const again = Object.assign({}, res, { text: c && c.state === "running" ? LATE_RUNNING(c.step) : LATE, queued: !c || c.state === "waiting" });
    return show(again, o);
  }
  function withRetry(res, runId, id, o) {
    return Object.assign(res, { runId, id, retry: typeof (o && o.onRetry) === "function" ? () => o.onRetry(res) : typeof (o && o.resend) === "function" ? () => retry(o) : undefined });   // (onRetry: a caller that follows by itself sends it again its own way)
  }
  /** Try again: the same request is put again (o.resend), and followed as the first was. */
  async function retry(o) {
    safe(() => typeof o.onStep === "function" && o.onStep("Asking the cloud again…", null));
    if (o.el) notice(o.el, "work", "Asking the cloud again…", []);
    let p = null; try { p = await o.resend(); } catch (e) { p = { error: (e && e.message) || String(e) }; }
    if (!p || !p.id) return show(withRetry({ ok: false, failed: true, text: `Not sent again: ${clean((p && p.error) || "no answer")}` }, p && p.runId, null, o), o);
    return follow(p.runId, p.id, o);
  }

  /* ── following ── */
  async function follow(runId, id, o) {
    o = o || {}; const api = o.api || defApi, max = +o.maxMs > 0 ? Math.max(+o.maxMs, 1) : LATE_MS, t0 = Date.now();
    const say = t => safe(() => typeof o.onStep === "function" && o.onStep(t, last));
    let last = null;
    say(QUEUED); if (o.el) notice(o.el, "work", QUEUED, []);
    while (Date.now() - t0 < max) {
      await nap(+o.gapMs > 0 ? +o.gapMs : Date.now() - t0 < 60000 ? 2000 : 4000);
      let c = null; try { const l = await api({ op: "runCmdList", runId }); c = ((l && l.cmds) || []).find(x => x && x.id === id) || null; } catch (_) { continue; }
      if (!c) continue;
      last = c;
      const end = settle(c); if (end) { safe(() => root.CharmNestRunFollow && root.CharmNestRunFollow.now && root.CharmNestRunFollow.now("cmdnow")); return show(withRetry(end, runId, id, o), o); }
      const w = live(c, t0); say(w); if (o.el) notice(o.el, "work", w, []);
    }
    const res = withRetry({ ok: false, late: true, queued: !last || last.state === "waiting", text: last && last.state === "running" ? LATE_RUNNING(last.step) : LATE, step: last && last.step }, runId, id, o);
    if (o.watch !== false) res.later = watch(runId, id, o);   // (its answer when it comes: shown then, and given to the caller)
    show(res, o);
    return res;
  }
  /** In the background, after a late answer (or for a request nobody waits on, CNAskPlace): its answer is shown when it comes, at most 6 hours on. */
  function watch(runId, id, o) {
    o = o || {}; if (!runId || !id) return Promise.resolve(null);
    if (S.watching.has(id)) return S.watching.get(id);
    const api = o.api || defApi, gap = +o.watchGapMs > 0 ? +o.watchGapMs : WATCH_GAP_MS, upTo = Date.now() + (+o.watchMs > 0 ? +o.watchMs : WATCH_MS);
    const p = (async () => {
      while (Date.now() < upTo) {
        await nap(gap);
        let c = null; try { const l = await api({ op: "runCmdList", runId }); c = ((l && l.cmds) || []).find(x => x && x.id === id) || null; } catch (_) { continue; }
        const end = settle(c); if (!end) continue;
        if (end.ok && o.quietOk) return end;
        return show(withRetry(end, runId, id, o), Object.assign({}, o, { toastOk: o.toastOk !== false }));
      }
      return null;
    })().finally(() => S.watching.delete(id));
    S.watching.set(id, p);
    return p;
  }
  /** A caller that follows by itself shows a failure or a late answer the same way (toast with Try again, notice in o.el). */
  function shout(res, o) { o = o || {}; return show(withRetry(Object.assign({ ok: false }, res), res.runId, res.id, o), o); }

  /* ── asking ── */
  function who() { return safe(() => (typeof root.employeeName === "function" && root.employeeName()) || (root.B && root.B.employee) || "") || ""; }
  async function put(cmd, o) {
    o = o || {}; const api = o.api || defApi, F = root.CharmNestRunFollow;
    const runId = o.runId || safe(() => F.state().runId) || "", dev = o.dev || safe(() => F.dev()) || "";
    if (!runId || !dev) return { error: "No run is open in the cloud." };
    let r = null; try { r = await api({ op: "runCmdPut", runId, dev, by: o.by != null ? String(o.by) : String(who()), cmd }); } catch (e) { r = { error: (e && e.message) || String(e) }; }
    return r && r.id ? { runId, id: r.id } : { error: (r && r.error) || "The request was not sent." };
  }
  async function ask(cmd, o) {
    o = Object.assign({ label: cmd && cmd.label }, o || {});
    if (typeof o.resend !== "function") o.resend = () => put(cmd, o);
    const p = await put(cmd, o);
    if (!p.id) return show(withRetry({ ok: false, failed: true, text: `Not sent: ${clean(p.error)}` }, p.runId, null, o), o);
    return follow(p.runId, p.id, o);
  }

  root.CNCmdNow = { follow, watch, put, ask, shout, notice, LATE, LATE_MS, _state: () => S };
})(typeof window !== "undefined" ? window : globalThis);
