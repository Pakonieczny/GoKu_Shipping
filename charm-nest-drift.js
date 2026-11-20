/* charm-nest-drift.js — EVERY COMPUTER SAYS HOW FAR BEHIND THE CLOUD IT IS (DRIFT, Paul, 18 Nov 2026: "Make computers report how far behind the cloud they are. Each computer would compare every sheet's revision
   with the cloud every few minutes. If it is behind, it rebuilds itself and shows a small 'out of date' mark. You would also see in one list which computer is behind").

   Why: Michelle's Rose Gold card stayed at 7 pieces (revision 6) for days while the cloud held 27 (revision 32). Nothing said so, on her screen or anywhere else, and the changes feed only names what moves while a
   page listens: a name it missed was never looked at again. The feed is the news; this is the check that nothing was missed.

   What it does, on the visible tab of a computer, 75 s after the page is up and then every four minutes (and when a tab is shown again or the network is back, never more often than every two minutes):
     1 asks the cloud for the run's sheets with only what is needed to compare (charmNestLibrary sheetRevsGet: one read of each sheet, nothing written, nothing named on the feed);
     2 compares each sheet this page holds with it: the layout revision it holds against the cloud's (a saved picture holds its record's), a sheet the cloud has and this page lacks (missing), a sheet this page holds and the cloud
       has not (extra); sheets that are cut cannot change and are left out; a sheet with a change of its own waiting (dirty, at work) is "in hand" and is never touched;
     3 a computer that FOLLOWS the run (B.run is not its own) heals what it found, by the page's own ways and no others: a sheet behind is read again by CharmNestCloudSheets._changed (the very path the changes feed takes: rebuilt only
       when the cloud's revision is newer and nothing is in hand), a set of sheets that differs is listed again by Recall.follow({ force }). At most four sheets a check, after the person has stopped touching the page, and a sheet that
       could not be rebuilt is left alone for twenty minutes. The computer that CARRIES the run is only measured and reported, never rebuilt by this (its pages are the run's own);
     4 shows a small "Out of date" mark in the top bar while something is still behind after that, and says in the activity log what it rebuilt;
     5 tells the cloud what it found (computerReport: one small document per computer, no feed name), and Settings → Computers (#stComputers) lists every computer that reported: who, carrying or following, last seen, current or
       how many sheets behind, the longest freeze of the last ten minutes, push or polling, and its own switch.
   The switch, per computer (localStorage cn.drift): "on" (default: measure, heal, report), "report" (measure and report only), "off" (nothing). A cloud that does not know the calls (an older deploy) stops it for the page's life.
   Production only: the sandbox neither asks nor reports. Nothing here writes a sheet, a piece, a run or an order.

   window.CharmNestDrift: check(why) → report · state() · list() · mode(m?) */
(function (root) {
  "use strict";
  const doc = root.document || null;
  const safe = (f, d) => { try { const v = f(); return v === undefined ? d : v; } catch (_) { return d; } };
  const EVERY = 240000, FIRST = 75000, MIN_GAP = 120000, MAX_HEAL = 4, RETRY = 20 * 60000, STALE_S = 900, BUILD = "drift-1118d";
  const st = { checks: 0, errors: 0, healed: 0, reports: 0, unsupported: false, last: null, at: 0, started: false, pill: 0 };
  const sandbox = () => safe(() => typeof WORKSPACE_SANDBOX !== "undefined" && WORKSPACE_SANDBOX, false);
  const api = (body, o) => root.CN && typeof root.CN.api === "function" ? root.CN.api("charmNestLibrary", body, Object.assign({ quiet: true, timeoutMs: 30000 }, o || {})) : Promise.reject(new Error("no api"));
  const pages = () => safe(() => (typeof allSheets === "function" ? allSheets() : []), []) || [];
  const CS = () => root.CharmNestCloudSheets || null;
  const mode = m => { const ls = safe(() => root.localStorage, null); if (m && ls) safe(() => ls.setItem("cn.drift", ["on", "report", "off"].includes(m) ? m : "on")); const v = ls && safe(() => ls.getItem("cn.drift"), null); return ["report", "off"].includes(v) ? v : "on"; };
  const devId = () => safe(() => root.CharmNestRunFollow.dev(), "") || "";
  const who = () => String(safe(() => root.B && root.B.employee, "") || "").trim();
  const short = p => String(p.fileBase || p.sheetId || "").slice(0, 40);
  const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

  /* ── how long the page stood still (the browser's own long-task entries: the freezes a person feels) ── */
  const longs = [];
  safe(() => { new root.PerformanceObserver(l => { for (const e of l.getEntries()) longs.push([Date.now(), Math.round(e.duration)]); while (longs.length > 400) longs.shift(); }).observe({ entryTypes: ["longtask"] }); });
  const freezeNow = () => { const cut = Date.now() - 600000, a = longs.filter(x => x[0] >= cut); return { freezeMs: a.reduce((m, x) => Math.max(m, x[1]), 0), freezes: a.filter(x => x[1] >= 1000).length }; };
  let lastInput = Date.now();
  safe(() => { for (const ev of ["pointerdown", "keydown", "wheel"]) root.addEventListener(ev, () => { lastInput = Date.now(); }, { capture: true, passive: true }); });

  const ready = () => !!(root.CN && typeof root.CN.api === "function" && typeof allSheets === "function" && root.Recall && root.B && root.B.orders && CS());
  const runOf = () => { const run = safe(() => root.B && root.B.run, null); if (run && run.runId) return { runId: run.runId, carrier: true }; const id = safe(() => root.CharmNestRunFollow.state().runId, null); return { runId: id || null, carrier: false }; };
  const revOfPage = p => { if (p.recalled && !(p.charms || []).length) { const v = Number(p.recalled.layoutRev); return Number.isFinite(v) ? v : null; } return safe(() => CS().revOf(p), null); };
  const inHand = p => safe(() => ["nesting", "finishing", "queued"].includes(p.status) || !!p._operationStarting || !!p._rebuilding || !!(p.persisted && !p.persistedDone && !p.problem) || !!p.saveOwed || !!p.dirty, false);
  const cutPage = p => +p.laserDoneAt > 0 || +p.roseCutAt > 0 || !!(p.recalled && (+p.recalled.laserDoneAt > 0 || +p.recalled.roseCutAt > 0));
  const failedAt = new Map(); let followAt = 0, prevStill = new Set();
  /** (BACKFOLDER, Paul 18 Nov 2026) approvals made on this computer whose back files are not saved: Ana's and Michelle's waited hours behind "Reconnect to save the approved back files" and nothing said so. How many, and how long the oldest has waited. */
  /** What the engraving card open on this computer holds of its piece ("" when none is open): order, state, pieces named, the first piece's row, whether a charm is held, pre, fit. */
  function heldCard() {
    return safe(() => {
      const c = root.document && root.document.querySelector(".rvItem.full"), j = c && c._job; if (!j) return "";
      const pid = (j.copies || [])[0], row = pid && root.B.pool.rows.get(pid), ch = pid && (root.Pool.charmOf(pid) || root.Pool.previewCharmOf(pid)), rid = String(((j.row && (j.row.parentRow || j.row)).order || {}).receiptId || "");
      return (rid + " " + j.state + " cp" + (j.copies || []).length + " " + (pid ? (row ? row.state : "norow") : "-") + " ch" + (ch ? 1 : 0) + " pre" + (j.pre ? 1 : 0) + " fit" + (j.fit ? 1 : 0) + " " + ((root.Orders.rows().find(r => r === (j.row.parentRow || j.row)) || {}).state || "?") + (pid && !ch ? " hw=" + safe(() => root.Pool.heldWhy(pid), "") : "")).slice(0, 100);
    }, "");
  }
  /** WHY each approval counted by waiting() is not being saved: the conditions the retry (Engrave.resumeBacks) tests, the first one that holds; as "bk:<order> <why>" in the report's `still` list, for reading in the cloud (never shown). */
  function stuckWhy() {
    const items = safe(() => root.B.engrave.items, null), out = []; if (!items || typeof items.values !== "function") return out;
    const cloudOk = safe(() => root.S.cloud.ok === true, true);
    for (const j of items.values()) {
      if (!j || !j.backPending || j.editingBack) continue;
      const rid = String(((j.row && (j.row.parentRow || j.row)).order || {}).receiptId || "?"), at = +j.approvedAt || +(j.approving && j.approving.at) || 0, live = ["approved", "written"].includes(j.state) || !!(j.approving && j.state === "review");
      const why = !live ? "notLive:" + j.state + (j.approving ? "+ing" : "") + (j.approvedAt ? "+at" : "") : j.backSaving ? "saving" : j.approvalPreparing ? "preparing" : j.stamping ? "stamping" : !at ? "noAt" : !(j.fit && j.view || j.writtenFit && j.writtenFit.approvedAt === j.approvedAt) ? "noFit:" + (j.fit ? 1 : 0) + (j.view ? 1 : 0) + (j.writtenFit ? 1 : 0) : !(j.copies || []).length ? "noCopies" : (j.pre && !safe(() => (j.copies || []).some(id => root.Pool.sheetOf(id)), false)) ? "pre" + (safe(() => root.Engrave.unplacedPre(j), false) ? ":unplaced" : ":onSaved") : !cloudOk ? "cloudAway" : "retrying:" + String(typeof j.backPending === "string" ? j.backPending : j.backError || j.reason || "").slice(0, 70);
      out.push(("bk:" + rid + " " + why).slice(0, 90));
      if (out.length >= 12) break;
    }
    return out;
  }
  function waiting() {
    const items = safe(() => root.B.engrave.items, null), out = { n: 0, min: 0 }; if (!items || typeof items.values !== "function") return out;
    const now = Date.now(), held = new Map();
    for (const p of pages()) { for (const c of (p && p.charms) || []) if (c && c.poolId) held.set(String(c.poolId), p); for (const id of (p && p.recalled && p.recalled.poolIds) || []) held.set(String(id), p); }
    for (const j of items.values()) {
      if (!j || !j.backPending || j.editingBack) continue;
      if (!(["approved", "written"].includes(j.state) || !!(j.approving && j.state === "review"))) continue;   // (STALEBACK: not an approval in force: nothing waits to be saved)
      if (!safe(() => (j.copies || []).some(id => root.Pool.charmOf(id) || root.Pool.previewCharmOf(id)), true)) continue;   // (a computer that holds no charm of the piece (a sheet it has only as a saved card) cannot write its back: the computer that carries the run does, and counts it)
      const at = +j.approvedAt || +(j.approving && j.approving.at) || 0, copies = (j.copies || []).map(String).filter(id => held.has(id));
      if (!copies.length) continue;   // (none of its pieces is on a sheet any more, replaced or taken off: there is nothing to save for it; 13 of the 19 "waiting" on Michelle's computer were of this kind)
      const have = id => (held.get(id).backPool || []).some(b => b && String(b.poolId) === id && !b.invalidated && !b.pending && (+b.approvedAt || 0) >= at && (at > 0 || +b.approvedAt > 0));
      if (copies.every(have)) continue;   // (the cloud already holds this approval's back: the flag is only the page's own, not yet cleared)
      out.n++; if (at) out.min = Math.max(out.min, Math.round((now - at) / 60000));
    }
    return out;
  }

  /** What this page holds against what the cloud has: { behind: [{ p, from, to }], unverified, inHand, missing: [id], extra: [p], held, want } */
  function compare(runId, sheets) {
    const by = new Map();
    for (const x of sheets || []) { const k = (x.set || "") + "|" + (x.fb || x.id), had = by.get(k); if (!had || (+x.upd || 0) > (+had.upd || 0)) by.set(k, x); }   // (one record per sheet name, the newest: as Recall.follow lists a run)
    const gone = new Set(safe(() => (root.CharmNestRunFollow.state().retired || []).map(String), []));   // (a sheet the run has let go of is not listed here either: Recall.follow)
    const want = new Map([...by.values()].filter(x => !gone.has(String(x.id))).map(x => [x.id, x])), held = pages().filter(p => p && p.sheetId && p.runId === runId);
    const out = { behind: [], unverified: [], inHand: [], extra: [], missing: [], held: held.length, want: want.size };
    const have = new Set(held.map(p => p.sheetId));
    for (const p of held) {
      const c = want.get(p.sheetId);
      if (!c) { out.extra.push(p); continue; }
      if (c.cut || cutPage(p)) continue;                    // (cut or sent: it can no longer change)
      if (inHand(p)) { out.inHand.push(p); continue; }
      const mine = revOfPage(p);
      if (mine === null) { out.unverified.push(p); continue; }
      if ((+c.rev || 0) > mine) out.behind.push({ p, from: mine, to: +c.rev || 0 });
    }
    for (const id of want.keys()) if (!have.has(id)) out.missing.push(id);
    return out;
  }
  const idle = async () => { try { await (root.CharmNestInteraction && typeof root.CharmNestInteraction.idle === "function" ? root.CharmNestInteraction.idle() : null); } catch (_) {} const q = 5000 - (Date.now() - lastInput); if (q > 0) await new Promise(r => setTimeout(r, q)); };
  const say = text => safe(() => root.CN && root.CN.agent ? root.CN.agent({ bridge: true }, "cloud", text) : console.info("[drift]", text));

  /** Heal what a FOLLOWING computer found, by the page's own ways only (see the head of this file). */
  async function heal(runId, sheets, first) {
    const done = { rebuilt: 0, listed: false, healed: [] }, cs = CS();
    if ((first.missing.length || first.extra.length) && Date.now() - followAt > RETRY) {   // (a set of sheets that cannot be made to match is not listed again every four minutes)
      // (it builds the cards again: not while a window is open or the person has touched the page in the last half minute; the next check looks again)
      const open = !!(doc && doc.querySelector("dialog[open]"));
      if (typeof root.Recall.follow === "function" && !open && Date.now() - lastInput > 30000) { followAt = Date.now(); await idle(); try { await root.Recall.follow(runId, { force: true }); done.listed = true; } catch (e) { console.warn("[drift] follow", e && e.message); } }
    }
    let cur = compare(runId, sheets), n = 0;
    for (const b of cur.behind) {
      if (n >= MAX_HEAL) break;
      const bad = failedAt.get(b.p.sheetId); if (bad && Date.now() - bad < RETRY) continue;
      await idle(); if (inHand(b.p)) continue;
      n++;
      try { await cs._changed(["s:" + b.p.sheetId]); } catch (e) { failedAt.set(b.p.sheetId, Date.now()); console.warn("[drift] rebuild", b.p.sheetId, e && e.message); continue; }
      const now = revOfPage(b.p);
      if (now !== null && now >= b.to) { done.rebuilt++; done.healed.push(short(b.p) + " " + b.from + "→" + now); } else failedAt.set(b.p.sheetId, Date.now());
    }
    return done;
  }

  /* CLOUDWORLD: the cloud world's summary (worldGet, at most once a minute) and this computer's own lines, hashed the way the server hashes its open keys */
  let worldSeen = null, worldAt = 0;
  let worldNone = false;
  async function worldSummary() { if (worldNone) return null; if (worldSeen && Date.now() - worldAt < 60000) return worldSeen; let r = null; try { r = await api({ op: "worldGet" }); } catch (e) { if (/unknown op|not a function/i.test(String(e && e.message || ""))) worldNone = true; throw e; } if (r && r.error && /unknown op|production only/i.test(String(r.error))) worldNone = true; worldSeen = r && r.summary ? r : null; worldAt = Date.now(); return worldSeen; }
  async function sha24(text) { const subtle = safe(() => root.crypto.subtle, null); if (!subtle) return ""; const b = await subtle.digest("SHA-256", new TextEncoder().encode(text)); return [...new Uint8Array(b)].map(x => x.toString(16).padStart(2, "0")).join("").slice(0, 24); }
  async function cloudCompare(rep) {
    const O = root.Orders; if (!O || typeof O.rows !== "function") return;
    const live = O.rows().filter(r => r && r.state !== "gone").map(r => String(r.key)).sort(), mine = await sha24(live.join(","));
    rep.lines = live.length; rep.keysHash = mine;
    const w = await worldSummary().catch(() => null), s = w && w.summary; if (!s) { rep.cmp = "cloud world not built yet"; return; }
    const rc = (w.review && w.review.counts) || null, cw = rc ? ` · cloud Review open ${rc.openCount != null ? rc.openCount : rc.open}` : "";
    rep.cmp = s.keysHash === mine ? `same ${live.length} lines as the cloud${cw}` : `differs: cloud ${s.live} lines · here ${live.length}${cw}`;
  }
  let checking = false, lastDone = 0;
  async function check(why) {
    if (checking || st.unsupported || sandbox() || mode() === "off") return st.last;
    if (!ready()) return null;
    checking = true;
    try {
      const { runId, carrier } = runOf(), m = mode();
      const rep = { station: safe(() => root.localStorage.getItem("station_computer_id"), "") || "", role: !runId ? "none" : carrier ? "carrier" : "follower", runId: runId || "", build: (BUILD + "/" + (safe(() => (/[?&]v=([^&]+)/.exec((doc.querySelector('script[src*="charm-nest-bridge.js"]') || {}).src || "") || [])[1], "") || "") + "/sw:" + (root.__swBuild || "old") + "/sup:" + (safe(() => typeof root.Engrave.settleStuckBacks === "function" ? 1 : 0, 0)) + "/calm:" + (root.__calmBuild || "0")).slice(0, 80),
        checked: 0, backWaitN: 0, backWaitMin: 0, behindN: 0, behind: [], stillN: 0, still: [], healed: [], unverifiedN: 0, missingN: 0, extraN: 0, inHandN: 0, rebuilt: 0, mode: m, feed: safe(() => root.CNSync.push.status().state, ""), heapMB: safe(() => Math.round(root.performance.memory.usedJSHeapSize / 1e6), 0), idleS: Math.round((Date.now() - lastInput) / 1000), ...freezeNow() };
      try { await root.Engrave.settleStuckBacks(); } catch (_) { /* the count below is as the flags are */ }   // (STALEBACK)
      { const w = waiting(); rep.backWaitN = w.n; rep.backWaitMin = w.min; }
      { const d = heldCard(); if (d) rep.note = d; }
      // (RVSAME, Paul 19 Nov 2026: "Why does Michelle's computer not match my computer ... I requested many times to have everything updated in real time": Review read Open 40 · Custom Orders 18 · Need Resolution 18 · Held 2 on one computer and 37 · 33 · 5 · 1 on another): what Review shows on THIS computer is reported, with the orders its Open list holds, so the computers can be compared in Settings → Computers and by whoever looks into it
      safe(() => { const R = root.Review, O = root.Orders; if (!R || !O) return; const open = typeof R.openOrders === "function" ? [...R.openOrders().keys()].map(String).sort() : []; const rows = O.rows(); rep.rv = `open ${typeof R.openCount === "function" ? R.openCount() : "?"} · orders ${open.length} · held ${rows.filter(r => r && typeof O.heldNow === "function" && O.heldNow(r)).length} · lines ${rows.length}`; rep.rvOpen = open.slice(0, 40); });   // (HELDCHARM: what the open engraving card holds, for a computer whose card is not drawn whole: said in the report, 100 characters)
      let state = null;
      if (runId) {
        let res = null; try { res = await api({ op: "sheetRevsGet", runId }); } catch (e) { st.errors++; rep.error = String(e && e.message || e).slice(0, 160); }
        if (res && res.error) { if (/unknown|not a function|no such/i.test(String(res.error))) { st.unsupported = true; return null; } rep.error = String(res.error).slice(0, 160); res = null; }
        // (a set opened on purpose ("Earlier sets…", Recall.open) puts some of the run's sheets on the cards: that is not a computer that is behind; it is measured, said so, and nothing is rebuilt or marked)
        const opened = safe(() => { const q = root.Recall.state(); return !!(q && (q.runId || q.setId)); }, false);
        if (res && Array.isArray(res.sheets) && opened) { rep.note = "a set is opened on purpose on this computer"; rep.checked = pages().filter(p => p && p.sheetId && p.runId === runId).length; }
        else if (res && Array.isArray(res.sheets)) {
          const first = compare(runId, res.sheets);
          rep.checked = first.held; rep.behindN = first.behind.length; rep.behind = first.behind.map(b => short(b.p) + " " + b.from + "→" + b.to); rep.unverifiedN = first.unverified.length; rep.missingN = first.missing.length; rep.extraN = first.extra.length; rep.inHandN = first.inHand.length;
          let after = first;
          if (m === "on" && !carrier && (first.behind.length || first.missing.length || first.extra.length)) {
            const done = await heal(runId, res.sheets, first); rep.rebuilt = done.rebuilt; rep.healed = done.healed; st.healed += done.rebuilt;
            after = compare(runId, res.sheets);
            if (done.healed.length) say("Out of date: " + done.healed.length + " sheet(s) read again from the cloud — " + done.healed.join(", "));
          }
          let still = after.behind.map(b => short(b.p) + " " + b.from + "→" + b.to).concat(after.missing.map(id => id + " (not on this page)"), after.extra.map(p => short(p) + " (not in the cloud)"));
          // (the computer that carries the run saves its own sheets: one that reads behind at a single look may be the instant before its own save is noted; it is said only when it is behind at two looks in a row)
          if (carrier) { const was = prevStill; prevStill = new Set(still); still = still.filter(x => was.has(x)); } else prevStill = new Set();
          rep.stillN = still.length; rep.still = still;
          state = after;
        }
      }
      // (CLOUDWORLD, Paul 20 Nov 2026: "Only the cloud can decide"): the order lines this computer holds, compared with the cloud world's (_charmNestWorld.js): the same lines, or how they differ
      try { await cloudCompare(rep); } catch (_) { /* a report without it */ }
      // (SCREENCHECK, Paul 20 Nov 2026: "you have access to the computers ... look at it yourself"): what this computer's SCREEN shows: the three tab badges, and the Review tab's Open, Resolved and every chip of both
      /* (SCREENPAINT, Paul 20 Nov 2026: "Michelle's computer doesn't match the customer orders that needs a resolution and the held orders"): what each tab's bar PAINTS, as the person reads it: each button's words and number,
         and the one pressed (Open or Resolved, a chip's filter), for Review, Orders, Design & Engrave, and the rail; and the tab on screen. Read from the page itself, never worked out again. */
      const painted = () => safe(() => {
        const bar = sel => { const b = doc && doc.querySelector(sel); if (!b) return null; return [...b.querySelectorAll("button")].filter(x => x.offsetParent !== null || x.closest(".hidden") == null).map(x => { const t = String(x.textContent || "").replace(/\s+/g, " ").trim(); return t ? (x.classList.contains("on") || x.getAttribute("aria-pressed") === "true" ? "[" + t + "]" : t) : ""; }).filter(Boolean).slice(0, 24).join(" | ").slice(0, 600); };
        const rail = (() => { const r = doc && doc.querySelector(".ldFlow"); return r ? [...r.querySelectorAll(".ldRow")].map(x => String(x.textContent || "").replace(/\s+/g, " ").trim()).join(" | ").slice(0, 500) : null; })();
        // (and the first cards each list shows, in its order: the same cards first on every computer, or not; the Orders tab's three piles as their chips say them)
        const top = sel => { const host = doc && doc.querySelector(sel); if (!host) return null; const ks = []; for (const x of host.querySelectorAll("[data-mkey]")) { const k = x.dataset.mkey; if (k && !/^(day|ordEmpty):/.test(k) && !ks.includes(k)) ks.push(k); if (ks.length >= 8) break; } return ks.length ? ks.join(" ") : null; };
        const piles = { open: safe(() => (root.Orders && root.Orders.tabCount ? root.Orders.tabCount() : null), null), completed: safe(() => root.CNCompleted.count(), null), cancelled: safe(() => root.Cancelled.count(), null), chips: bar("#ordChips") };
        return { tab: safe(() => root.CN.S.mode, null), review: bar("#reviewView .ordBar"), orders: bar("#ordersView .ordBar"), engrave: bar("#engraveView .ordBar"), rail, piles,
          reviewTop: top("#reviewView #rvList"), ordersTop: top("#ordersView #ordBody"), engraveTop: top("#engraveView .egPlacementList") };
      }, null);
      safe(() => { const t = id => { const e = doc && doc.getElementById(id); return e ? +(String(e.textContent || "").replace(/\D/g, "")) || 0 : null; }, sc = root.Review && typeof root.Review.screen === "function" ? root.Review.screen() : null;
        // (every tab: the rail's steps as the rail draws them, the Design & Engrave tab's own numbers, the Library's totals as the rail's headline says them)
        const fl = safe(() => { const f = root.Orders && root.Orders.flowNow ? root.Orders.flowNow() : null; return f && f.counts ? Object.assign({}, f.counts) : null; }, null);
        const E = root.Engrave, eng = E && E.pendingRows ? safe(() => ({ rows: E.pendingRows(), orders: E.pendingOrderIds ? E.pendingOrderIds().size : null, decided: E.reviewedRows ? E.reviewedRows() : null, offCloud: safe(() => root.CloudWorld.engraveOff().n, null), offList: safe(() => root.CloudWorld.engraveOff().list, null) }), null) : null;
        const lt = safe(() => String((doc.getElementById("cloudText") || {}).textContent || ""), ""), lm = /Library · ([\d,]+) sheets?(?: · ([\d,]+) charms?)?/.exec(lt);
        const library = lm ? { sheets: +lm[1].replace(/,/g, ""), charms: lm[2] ? +lm[2].replace(/,/g, "") : 0 } : null;
        rep.screen = JSON.stringify({ reviewBadge: t("tabReviewN"), designBadge: t("tabEngraveN"), ordersBadge: t("tabOrdersN"), open: sc ? sc.open : null, resolved: sc ? sc.resolved : null, openChips: sc ? sc.openChips : null, doneChips: sc ? sc.doneChips : null, flow: fl, flowFrom: safe(() => (root.Orders.flowPeek() || {}).cloud ? "cloud" : "here", null), engrave: eng, library, painted: painted() }).slice(0, 3800); });
      st.checks++; st.last = rep; st.at = Date.now(); lastDone = st.at;
      mark(rep);
      try { const why = stuckWhy(); if (why.length) rep.still = (Array.isArray(rep.still) ? rep.still : []).concat(why).slice(0, 40); } catch (_) { /* a report without it */ }
      try { await api({ op: "computerReport", dev: devId(), by: who(), report: rep }); st.reports++; } catch (e) { if (/unknown|not a function/i.test(String(e && e.message || ""))) st.unsupported = true; }
      if (root.dispatchEvent && root.CustomEvent) safe(() => root.dispatchEvent(new root.CustomEvent("cn:drift", { detail: { report: rep, state } })));
      return rep;
    } catch (e) { st.errors++; console.warn("[drift]", e && e.message); return null; }
    finally { checking = false; }
  }
  /** Once per computer at a time: a Web Lock, never held longer than the check. */
  function run(why) {
    if (doc && doc.hidden) return Promise.resolve(null);
    if (Date.now() - lastDone < MIN_GAP && why !== "manual") return Promise.resolve(null);
    const L = safe(() => root.navigator.locks, null);
    if (L && typeof L.request === "function") return L.request("cn-drift:production", { ifAvailable: true }, lock => (lock ? check(why) : null)).catch(() => null);
    return check(why);
  }

  /* ── the mark in the top bar, and the list ── */
  let pill = null, pop = null;
  const css = ".cnDrift{flex:0 0 auto;margin:0 8px;padding:3px 10px;border-radius:999px;border:1px solid #c9852c;background:#fff4e0;color:#8a5200;font:600 12px/1.4 system-ui,sans-serif;cursor:pointer}.cnDrift[hidden]{display:none}.cnDriftPop{position:fixed;z-index:9000;top:52px;right:16px;width:min(720px,94vw);max-height:70vh;overflow:auto;background:#fff;color:#1c1d1d;border:1px solid #b9b2a4;border-radius:10px;box-shadow:0 10px 30px rgba(0,0,0,.25);padding:12px 14px;font:13px/1.45 system-ui,sans-serif}.cnDriftPop[hidden]{display:none}.cnComp{width:100%;border-collapse:collapse;margin-top:6px}.cnComp th,.cnComp td{text-align:left;padding:4px 8px;border-bottom:1px solid #e6e1d6;vertical-align:top}.cnComp th{font-size:11px;text-transform:uppercase;letter-spacing:.05em;color:#777}.cnComp .bad{color:#b0412e;font-weight:600}.cnComp .ok{color:#2a7a4b}.cnComp .old{color:#999}";
  function ensureUi() {
    if (!doc || pill) return;
    if (!doc.getElementById("cnDriftCss")) { const s = doc.createElement("style"); s.id = "cnDriftCss"; s.textContent = css; (doc.head || doc.documentElement).appendChild(s); }
    const bar = doc.querySelector(".topbar"); if (!bar) return;
    pill = doc.createElement("button"); pill.type = "button"; pill.className = "cnDrift"; pill.hidden = true; pill.id = "cnDriftPill"; pill.setAttribute("aria-live", "polite");
    const tools = bar.querySelector(".topTools"); if (tools) bar.insertBefore(pill, tools); else bar.appendChild(pill);
    pop = doc.createElement("div"); pop.className = "cnDriftPop"; pop.hidden = true; pop.id = "cnDriftPop"; doc.body.appendChild(pop);
    pill.addEventListener("click", () => { pop.hidden = !pop.hidden; if (!pop.hidden) paintPop(); });
    doc.addEventListener("keydown", e => { if (e.key === "Escape" && !pop.hidden) pop.hidden = true; });
    doc.addEventListener("click", e => { if (pop && !pop.hidden && !pop.contains(e.target) && e.target !== pill) pop.hidden = true; }, true);
  }
  function mark(rep) {
    if (!doc) return; ensureUi(); if (!pill) return;
    const n = rep && rep.stillN || 0, w = rep && rep.backWaitMin >= 10 ? rep.backWaitN : 0, parts = [];   // (an approval that has waited ten minutes to be saved is not "still being saved")
    if (n) parts.push("Out of date · " + n);
    if (w) parts.push("Approvals not saved · " + w);
    pill.hidden = !parts.length; st.pill = n + w;
    if (parts.length) { pill.textContent = parts.join(" · "); pill.title = (n ? (rep.role === "carrier" ? "This computer carries the run, so it does not rebuild itself: " : "Still behind the cloud after a rebuild: ") + (rep.still || []).slice(0, 6).join("; ") + ". " : "") + (w ? w + " approval(s) made here have waited " + rep.backWaitMin + " min to be saved to the cloud. " : "") + "Click for the list of computers."; }
  }
  const ago = s => s < 90 ? "just now" : s < 5400 ? Math.round(s / 60) + " min ago" : Math.round(s / 3600) + " h ago";
  /* CLOUDWORLD: the cloud's own line, first: what every computer must show */
  function cloudRow(w) {
    const s = w && w.summary; if (!s) return '<tr><td><b>Cloud</b></td><td colspan="9" class="old">the cloud world is not built yet</td></tr>';
    const rc = (w.review && w.review.counts) || {}, age = Math.max(0, Math.round((Date.now() - (+s.at || 0)) / 1000));
    const st = Object.entries(s.states || {}).map(([k, n]) => k + " " + n).join(" · ");
    const rv = w.review ? `open ${rc.openCount != null ? rc.openCount : rc.open} · custom ${rc.customOrders != null ? rc.customOrders : "?"} · need resolution ${rc.needResolution != null ? rc.needResolution : "?"} · held ${rc.held != null ? rc.held : "?"}` : "not built yet";
    return '<tr style="background:#eef6ff"><td><b>Cloud</b><div class="old">the one truth</div></td><td>reads every order</td><td>' + ago(age) + '</td><td colspan="5" class="old">' + esc(st) + (s.etsyCap ? ' · Etsy calls today ' + (s.etsyToday == null ? "?" : s.etsyToday) + ' / ' + s.etsyCap : '') + (s.errors && s.errors.length ? ' <span class="bad" title="' + esc(s.errors.join("; ")) + '">· ' + s.errors.length + ' problem(s)</span>' : '') + '</td><td>' + esc(rv) + '</td><td>' + s.live + ' lines</td></tr>';
  }
  function table(list, me, world) {
    if (!list || !list.length) return '<div class="help">No computer has reported yet.</div>';
    const rows = list.map(c => {
      const old = c.ageS > STALE_S, behind = (c.stillN || 0) > 0, sheets = old ? '<span class="old">not seen</span>' : behind ? '<span class="bad" title="' + esc((c.still || []).join("; ")) + '">' + c.stillN + " behind</span>" : '<span class="ok">current</span>' + (c.note && /^a set is opened/.test(c.note) ? ' <span class="old" title="' + esc(c.note) + '">· set opened</span>' : '')   // (the note also carries what an open engraving card holds, for reading in the cloud: not shown as a set opened)
      + '' + (c.unverifiedN ? ' <span class="old">(' + c.unverifiedN + " unchecked)</span>" : "") + (c.healed && c.healed.length ? ' <span class="old" title="' + esc(c.healed.join("; ")) + '">· rebuilt ' + c.healed.length + "</span>" : "");
      const bw = c.backWaitN && !old ? '<div class="' + (c.backWaitMin >= 10 ? "bad" : "old") + '">' + c.backWaitN + " approval" + (c.backWaitN === 1 ? "" : "s") + " waiting to save" + (c.backWaitMin ? " (oldest " + (c.backWaitMin >= 90 ? Math.round(c.backWaitMin / 60) + " h" : c.backWaitMin + " min") + ")" : "") + "</div>" : "";
      const fr = c.freezeMs >= 1000 ? '<span class="' + (c.freezeMs >= 3000 ? "bad" : "") + '">' + (c.freezeMs / 1000).toFixed(1) + " s</span>" : "—";
      return "<tr><td>" + esc(c.by || "?") + (c.dev === me ? " <b>(this one)</b>" : "") + '<div class="old">' + esc(String(c.station || c.dev).slice(-8)) + "</div></td><td>" + (c.role === "carrier" ? "carries the run" : c.role === "follower" ? "follows" : "no run") + "</td><td" + (old ? ' class="old"' : "") + ">" + ago(c.ageS) + "</td><td>" + sheets + bw + "</td><td>" + fr + "</td><td>" + esc(c.feed || "—") + "</td><td>" + esc(c.mode === "on" ? "heals" : c.mode === "report" ? "report only" : "off") + '</td><td class="old">' + esc(String(c.build || "").slice(0, 22)) + "</td><td>" + esc(c.rv || "—") + '</td><td class="' + (/^same/.test(c.cmp || "") ? "ok" : c.cmp ? "bad" : "old") + '">' + esc(c.cmp || "—") + '</td><td class="' + (c.screenSame === true ? "ok" : c.screenSame === false ? "bad" : "old") + '" title="' + esc((c.screen || "") + ((c.screensApart || []).length ? "\n\nNot the same on every computer:\n" + c.screensApart.join("\n") : "")) + '">' + esc(c.screenSame === true ? "same as the cloud" : c.screenSame === false ? "differs: " + (c.screenDiff || []).join(", ") : "—") + "</td></tr>";
    }).join("");
    return '<table class="cnComp"><thead><tr><th>Computer</th><th>Role</th><th>Seen</th><th>Sheets</th><th>Longest freeze (10 min)</th><th>Updates</th><th>Rebuilds itself</th><th>Build</th><th>Review here</th><th>Order lines vs the cloud</th><th>Screen vs the cloud</th></tr></thead><tbody>' + cloudRow(world) + rows + "</tbody></table>";
  }
  async function list() { const r = await api({ op: "computerList" }); return (r && r.computers) || []; }
  async function paintPop() {
    if (!pop) return; pop.innerHTML = '<b>Computers</b> <span class="old">— how each compares its sheets with the cloud, every four minutes</span><div data-body>reading…</div>';
    try { pop.querySelector("[data-body]").innerHTML = table(await list(), devId(), await worldSummary().catch(() => null)); } catch (e) { pop.querySelector("[data-body]").textContent = "Could not read the list: " + (e && e.message || e); }
  }
  function settingsRow() {
    if (!doc) return;
    const mount = () => {
      const host = doc.getElementById("stComputers"); if (!host || host.dataset.drift) return; host.dataset.drift = "1";
      host.innerHTML = '<label>Computers</label><div class="help">Each computer compares its sheets with the cloud every four minutes and says how far behind it is. A computer that follows the run rebuilds a sheet that is behind; the computer that carries the run only reports.</div>'
        + '<div class="fieldRow3" style="align-items:center;gap:8px"><select data-m aria-label="This computer rebuilds itself when behind"><option value="on">This computer: measure, rebuild, report</option><option value="report">This computer: measure and report only</option><option value="off">This computer: off</option></select><button class="btn ghost sm" type="button" data-r>Check now</button></div><div data-body class="help">…</div>';
      const sel = host.querySelector("[data-m]"), body = host.querySelector("[data-body]"); sel.value = mode();
      sel.addEventListener("change", () => { mode(sel.value); });
      const paint = async () => { try { worldAt = 0; body.innerHTML = table(await list(), devId(), await worldSummary().catch(() => null)); } catch (e) { body.textContent = "Could not read the list: " + (e && e.message || e); } };
      host.querySelector("[data-r]").addEventListener("click", async () => { body.textContent = "checking…"; await run("manual"); paint(); });
      const dlg = doc.getElementById("dlgSettings");
      if (dlg && typeof root.MutationObserver === "function") new root.MutationObserver(() => { if (dlg.open) paint(); }).observe(dlg, { attributes: true, attributeFilter: ["open"] });
    };
    if (doc.readyState === "loading") doc.addEventListener("DOMContentLoaded", mount, { once: true }); else mount();
  }

  function boot() {
    let tries = 0;
    const go = () => {
      tries++;
      if (!ready() && tries < 180) return setTimeout(go, 1000);
      if (!ready() || st.started) return;
      st.started = true;
      const tick = () => { run("timer").catch(() => {}); setTimeout(tick, EVERY + Math.floor(Math.random() * 20000)); };
      setTimeout(tick, FIRST + Math.floor(Math.random() * 30000));
      if (doc) doc.addEventListener("visibilitychange", () => { if (!doc.hidden && Date.now() - lastDone > MIN_GAP) setTimeout(() => run("shown").catch(() => {}), 4000 + Math.floor(Math.random() * 3000)); });
      if (root.addEventListener) root.addEventListener("online", () => setTimeout(() => run("online").catch(() => {}), 5000 + Math.floor(Math.random() * 5000)));
    };
    go();
  }
  root.CharmNestDrift = { check: why => (why === "manual" ? run("manual") : check(why || "test")), state: () => Object.assign({}, st), list, mode, compare, _paint: mark, _quiet: () => { lastInput = 0; } };
  settingsRow();
  if (doc) { if (doc.readyState === "loading") doc.addEventListener("DOMContentLoaded", boot); else boot(); }
})(typeof self !== "undefined" ? self : this);
