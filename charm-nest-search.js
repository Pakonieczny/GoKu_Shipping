/*  charm-nest-search.js — find any order, as fast as it is typed (Paul, 28 Sep 19:21: B1-B3).
 *  "An interactive and game like animated search function that searches all orders and provides a detailed popup for
 *  that specific order … focus and highlight the order number in question. This search must be ultrafast and must work
 *  with etsy order numbers."
 *
 *  One index, built once from what the page already holds and rebuilt only when that changes: the lines of the current
 *  pull (with their holds), the pool rows and the pieces on the sheets, the engravings, the Custom Orders finished by
 *  hand, the Library's sheets, the Cancelled list and the Review decisions. Every keystroke is one pass over it (a few
 *  hundred microseconds for five thousand orders). A whole Etsy number that is nowhere in it is looked up in the cloud
 *  once the typing pauses (the order's timeline, its pool rows, the sheets that name it, its design archive record):
 *  existing ops only, cancelled by the next key, and never a call to Etsy.
 *
 *  Opens with "/" or Ctrl/Cmd+K anywhere, or the field in the top bar. A result opens the order's full view
 *  (OrderWin.openOrder) growing out of its card, the number highlighted; the pull's own order window (OrderWin.open)
 *  stands in while that view does not exist.
 *
 *    OrderSearch.open(prefill) · close() · isOpen() · query(q) → { hits, ms } · rebuild() → ms · stats() */
(function () {
  "use strict";
  if (window.OrderSearch) return;
  const doc = document, W = window;
  const CODE = { gold: "GF", silver: "SS", rose: "RG", gold10k: "10K", gold14k: "14K" };
  const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const reduced = () => { try { return matchMedia("(prefers-reduced-motion: reduce)").matches; } catch (_) { return false; } };
  const E = "cubic-bezier(.2,.8,.2,1)", SPRING = "cubic-bezier(.3,1.5,.5,1)";
  const St = () => (typeof S !== "undefined" ? S : null);                       // the page's state (a global of the inline script)
  const Bs = () => W.B || null;
  const MAX = 24, CLOUD_MIN = 6, CLOUD_MAX = 13, PAUSE = 250;   // (longer than any Etsy number: a paste of several, never looked up)

  /* ── the milestone rail every card carries: the order view's own steps (OrderTimelineUI.STAGES, and stagesFor(lines):
     Welded only for a stud earring), numbered as the server's where.step (0-7). A copy stands in while that script is
     not on the page; keep it and STEP alike with STAGES and _orderTimeline.js's STEP. ── */
  const RAIL0 = [{ k: "arrived", l: "Order in" }, { k: "sheet", l: "Nested" }, { k: "engraved", l: "Engraved" }, { k: "laser", l: "Laser cut" }, { k: "sorted", l: "Sorted" },
    { k: "welded", l: "Welded", only: "stud" }, { k: "assembled", l: "Assembled" }, { k: "shipped", l: "Shipped" }];
  const STEP = { arrived: 0, placed: 1, moved: 1, renested: 1, qrLabel: 1, roseLine: 1, included: 1, merged: 1, sizeChanged: 1, setCommitted: 1, sealCompleted: 1,
    engraveApproved: 2, laserDone: 3, roseCut: 3, sorted: 4, welded: 5, assembled: 6, packed: 6, labelPrinted: 6, shipped: 7, etsyCompleted: 7 };
  const TUI = () => (W.OrderTimelineUI && Array.isArray(W.OrderTimelineUI.STAGES) && W.OrderTimelineUI.STAGES.length ? W.OrderTimelineUI : null);
  const allSteps = () => { const U = TUI(); return U ? U.STAGES : RAIL0; };
  /** The order's own steps, as its view draws them: every piece's line read for a stud (none known: every step). */
  function stepsOf(e) {
    const U = TUI(), all = allSteps();
    if (e._steps && e._steps.gen === IX.gen && e._steps.all === all) return e._steps.list;
    let list = all;
    if (U && typeof U.stagesFor === "function") {
      const rows = e.rows.filter(r => r.state !== "gone"), c = e.cloud;
      const lines = rows.length ? rows : c && c.items && c.items.length ? c.items : e.cancel && e.cancel.lines && e.cancel.lines.length ? e.cancel.lines : [];
      try { const r = U.stagesFor(lines); if (Array.isArray(r) && r.length) list = r; } catch (_) {}
    }
    e._steps = { gen: IX.gen, all, list };
    return list;
  }

  /* ═══ the index ═══ */
  const IX = { list: [], by: new Map(), gen: 0, ms: 0, at: 0, refs: [], sig: "", cloud: new Map() };
  function blank(rid) { return { rid, num: "", buyer: "", buyerL: "", skus: [], titles: [], listings: [], rows: [], pools: [], sheets: [], custom: [], engrave: [], review: 0, cancel: null, cloud: null, at: 0, hay: "", dig: "" }; }
  const push = (a, v) => { if (v && !a.includes(v)) a.push(v); };
  const ms = v => { const n = +v || 0; return n && n < 1e12 ? n * 1000 : n; };  // seconds or milliseconds → ms
  /** What the index is made from, cheaply: a list replaced or grown changes it. */
  function refsNow() {
    const b = Bs(), s = St(), C = W.Cancelled;
    return [b && b.orders.rows, b && b.orders.rows.length, b && b.pool.rows.size, b && b.engrave.items.size, b && b.maps.customDone, s && s.library && s.library.rows, s && s.library && s.library.rows && s.library.rows.length,
      b && b.review && b.review.items, b && b.review && b.review.items.length, C && C.count ? C.count() : 0, cancelList.length, IX.cloud.size];
  }
  const stale = () => { const r = refsNow(); return r.length !== IX.refs.length || r.some((v, i) => v !== IX.refs[i]); };
  function build() {
    const t0 = performance.now(), b = Bs(), s = St();
    const by = new Map(), list = [];
    const at = rid => { let e = by.get(rid); if (!e) { e = blank(rid); by.set(rid, e); list.push(e); } return e; };
    const idOf = v => String(v == null ? "" : v).replace(/\D/g, "");
    if (b) {
      // the lines of the current pull: everything about each order, holds included
      for (const r of b.orders.rows) {
        if (!r || !r.order) continue;
        const o = r.order, l = r.line || {}, e = at(String(o.receiptId));
        e.rows.push(r);
        if (!e.num && o.orderNumber) e.num = String(o.orderNumber);
        if (!e.buyer && o.buyer && o.buyer.name) e.buyer = o.buyer.name;
        push(e.skus, (r.spec && r.spec.designSku) || l.sku); push(e.titles, l.title); push(e.listings, l.listingId && String(l.listingId));
        const t = ms(r.arrivedAt) || ms(o.createTs); if (t > e.at) e.at = t;
      }
      // the pool: lines nested or waiting to be, including those of orders that have left the pull
      for (const p of b.pool.rows.values()) {
        if (!p || !p.orderId) continue;
        const e = at(String(p.orderId)); e.pools.push(p); push(e.skus, p.sku);
        const t = ms(p.arrivedAt) || ms(p.orderDate); if (t > e.at) e.at = t;
      }
      for (const j of b.engrave.items.values()) { const r = j && j.row; if (r && r.order) at(String(r.order.receiptId)).engrave.push(j); }
      for (const [k, c] of Object.entries(b.maps.customDone || {})) {
        const rid = String((c && c.receiptId) || String(k).split("_")[0]); if (!/^\d+$/.test(rid)) continue;
        const e = at(rid); e.custom.push(c); push(e.skus, c && c.sku); push(e.titles, c && c.title);
        const t = ms(c && (c.completedAt || c.lastPrintedAt)); if (t > e.at) e.at = t;
      }
      for (const it of (b.review && b.review.items) || []) for (const r of it.rows || [it.row]) if (r && r.order && !String(it.key || "").startsWith("held:")) at(String(r.order.receiptId)).review++;
    }
    // the Library's sheets in memory: each order a sheet names is on it
    for (const r of (s && s.library && s.library.rows) || []) {
      if (!r || !Array.isArray(r.orders)) continue;
      for (const id of r.orders) { const rid = idOf(id); if (!rid) continue; const e = at(rid); e.sheets.push(r); const t = ms(r.updatedAt) || ms(r.createdAt); if (!e.at && t) e.at = t; }
    }
    // the Cancelled list (its newest orders, with what was ordered)
    for (const c of cancelList) {
      const rid = idOf(c.orderId); if (!rid) continue;
      const e = at(rid); e.cancel = c; if (!e.buyer && c.buyer) e.buyer = c.buyer;
      for (const l of c.lines || []) { push(e.skus, l.sku); push(e.titles, l.title); }
      if (!e.at) e.at = ms(c.at);
    }
    // orders found in the cloud earlier this session stay found
    for (const [rid, c] of IX.cloud) { const e = at(rid); e.cloud = c; if (!e.buyer && c.buyer) e.buyer = c.buyer; for (const x of c.skus) push(e.skus, x); for (const x of c.titles) push(e.titles, x); for (const x of c.listings) push(e.listings, x); if (!e.at) e.at = c.at || 0; }
    for (const e of list) {
      e.buyerL = e.buyer.toLowerCase();
      e.hay = (e.buyer + "\u0001" + e.skus.join("\u0001") + "\u0001" + e.titles.join("\u0001")).toLowerCase();
      e.dig = (e.num && e.num !== e.rid ? e.num + "|" : "") + e.listings.join("|");
    }
    list.sort((a, b2) => b2.at - a.at);                                          // newest first: every pass keeps this order
    IX.list = list; IX.by = by; IX.gen++; IX.refs = refsNow(); IX.at = Date.now();
    IX.ms = performance.now() - t0;
    return IX.ms;
  }
  const ensure = () => { if (!IX.gen || stale()) build(); };

  /* One pass over the index. A number matches an order number by its start first, then anywhere in it, then a listing
     number; words match the buyer, a SKU or a title (every word somewhere). Newest first within each: the index is kept
     in that order, so nothing is sorted per key. The buckets are reused and hold the entries themselves: a keystroke
     makes one list, not an object per match. */
  const B0 = [], B1 = [], B2 = [];
  function query(raw) {
    const t0 = performance.now();
    const q = String(raw || "").trim().toLowerCase().replace(/\s+/g, " ");
    const d = q.replace(/^#/, "").replace(/[\s,#-]/g, ""), num = /^\d+$/.test(d) ? d : "", L = IX.list, n = L.length;
    if (!q) return { q, num, hits: L.slice(0, 6).map(e => ({ e, s: 3 })), all: L, total: n, ms: performance.now() - t0 };
    B0.length = 0; B1.length = 0; B2.length = 0;
    if (num) {
      for (let k = 0; k < n; k++) {
        const e = L[k], i = e.rid.indexOf(num);
        if (i === 0) B0.push(e); else if (i > 0) B1.push(e); else if (e.dig && e.dig.indexOf(num) >= 0) B2.push(e);
      }
    } else {
      const toks = q.split(" "), m = toks.length, t0k = toks[0];
      for (let k = 0; k < n; k++) {
        const e = L[k]; let ok = true;
        for (let j = 0; j < m; j++) if (e.hay.indexOf(toks[j]) < 0 && e.rid.indexOf(toks[j]) < 0) { ok = false; break; }
        if (!ok) continue;
        const i = e.buyerL.indexOf(t0k);
        if (i === 0) B0.push(e); else if (i > 0) B1.push(e); else B2.push(e);
      }
    }
    const n0 = B0.length, n1 = B1.length, all = B0.concat(B1, B2), hits = [];
    for (let k = 0; k < all.length && k < MAX; k++) hits.push({ e: all[k], s: k < n0 ? 0 : k < n0 + n1 ? 1 : 2 });
    return { q, num, hits, all, total: all.length, ms: performance.now() - t0 };
  }

  /* ═══ where an order is now: worked out for the cards on screen only, from the live objects ═══ */
  const TYPE_LABEL = t => (W.OrderTimeline && OrderTimeline.TYPES && OrderTimeline.TYPES[t] && OrderTimeline.TYPES[t].label) || (s => s.charAt(0).toUpperCase() + s.slice(1).toLowerCase())(String(t || "").replace(/([a-z])([A-Z])/g, "$1 $2"));
  const whenTxt = t => { if (!t) return ""; const d = new Date(t), today = new Date(); return d.toDateString() === today.toDateString() ? d.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" }) : d.toLocaleDateString([], { month: "short", day: "numeric" }); };
  function nestLabel(page) {
    try { const i = pagesOf(page.metal).indexOf(page); return `${CODE[page.metal] || ""} Sheet ${i >= 0 ? i + 1 : page.page || "?"}`.trim(); } catch (_) { return `${CODE[page.metal] || ""} Sheet ${page.page || "?"}`.trim(); }
  }
  function sheetsOf(e) {
    const out = new Map(), add = (k, label, cut) => { if (label && !out.has(k)) out.set(k, { label, cut: !!cut }); };
    const Pool = W.Pool;
    for (const r of e.rows) for (const id of r.poolIds || []) { const pg = Pool && Pool.sheetOf ? Pool.sheetOf(id) : null; if (pg) add("n:" + (pg.sheetId || nestLabel(pg)), nestLabel(pg)); }
    for (const p of e.pools) if (p.sheetId || p.sheetName) add("s:" + (p.sheetId || p.sheetName), sheetWords(p.sheetName) || null);
    for (const r of e.sheets) add("s:" + (r.id || r.sheetId), `${CODE[r.metal] || ""} Sheet ${r.sheetIndex || r.page || 1}`.trim(), +r.laserDoneAt > 0);
    const c = e.cloud; if (c) for (const x of c.sheets) add("s:" + x.id, x.label, x.cut);
    return [...out.values()];
  }
  // a pool row's sheet name is its file base (…_GF_Sheet_2 or "GF Sheet 2"): the words a person reads
  function sheetWords(n) { const m = /(GF|SS|RG|10K|14K)[ _-]*Sheet[ _-]*(\d+)/i.exec(String(n || "")); return m ? `${m[1].toUpperCase()} Sheet ${m[2]}` : n ? String(n).slice(0, 28) : ""; }
  /* An order's cancel record while it is still cancelled: one read earlier (the Cancelled list, the cloud) no longer counts
     once the order was restored, here or on another screen (it has left Cancelled's ids, or was restored since). */
  function cancelOf(e) {
    const C = W.Cancelled, c = e.cloud, live = C && C.has ? C.has(e.rid) : null;
    if (e.cancel && live !== false) return e.cancel;
    if (c && c.cancel && !(C && C.restoredAt && C.restoredAt(e.rid) > (+c.cancel.at || 0))) return c.cancel;
    return live ? {} : null;
  }
  function stateOf(e) {
    const c = e.cloud, rows = e.rows.filter(r => r.state !== "gone");
    const cancel = cancelOf(e);
    const hold = rows.find(r => r.hold);
    const sheets = sheetsOf(e);
    let reached = 0;
    const reach = i => { if (i > reached) reached = i; };
    // (the full rail's numbering, 0-7, as the server's where.step: nested, a set committed or a custom seal done is 1)
    if (sheets.length || rows.some(r => ["nested", "written", "labelled", "committed"].includes(r.state))) reach(1);
    if (e.custom.length) reach(1);
    if (e.engrave.some(j => j.approvedAt && ["approved", "written"].includes(j.state))) reach(2);
    if (sheets.some(s => s.cut)) reach(3);
    let last = null;
    if (c) {
      if (c.step >= 0) reach(Math.min(7, c.step));
      for (const ev of c.events) { if (STEP[ev.type] != null) reach(STEP[ev.type]); if (!last || ev.at >= last.at) last = ev; }
      if (c.shipped) reach(7);
    }
    const all = allSteps(), stepName = (all[Math.min(reached, all.length - 1)] || {}).l || "";
    const review = e.review || rows.some(r => (r.problems || []).length);
    const eng = e.engrave.find(j => ["words", "review", "fitting", "ready", "classify"].includes(j.state));
    let tone = "warn", pill = "In progress", now = "";
    // the pill names the step the rail has reached, as the order view does; before Nested it says where the order is
    if (cancel) { tone = "bad"; pill = "Cancelled"; now = `Cancelled${cancel.by ? " by " + cancel.by : ""}${cancel.at ? " · " + whenTxt(cancel.at) : ""}`; }
    else if (hold) { tone = "bad"; pill = "On hold"; now = `On hold · ${String(hold.hold)}`; }
    else if (review) { tone = "warn"; pill = "Decision"; now = "Needs a decision in Review"; }
    else if (reached >= 7) { tone = "ok"; pill = stepName; now = last && STEP[last.type] === 7 ? `${TYPE_LABEL(last.type)}${last.by ? " · " + last.by : ""} · ${whenTxt(last.at)}` : stepName; }
    else if (last && STEP[last.type] >= 4) { tone = "ok"; pill = stepName; now = `${TYPE_LABEL(last.type)}${last.station ? " at " + last.station[0].toUpperCase() + last.station.slice(1) : ""}${last.by ? " · " + last.by : ""}`; }
    else if (e.custom.length && !rows.some(r => r.state === "pulled")) { tone = "ok"; pill = stepName; const k = e.custom[0]; now = `Completed by hand${k.completedBy || k.printedBy ? " · " + (k.completedBy || k.printedBy) : ""}`; }
    else if (reached >= 1) {
      tone = reached >= 3 ? "ok" : "info"; pill = stepName;
      now = eng && reached < 2 ? `Back engraving · ${eng.state === "words" ? "words to read" : eng.state}` : sheets.length ? (sheets.some(s => s.cut) ? "Laser cut" : "On sheet") : last ? `${TYPE_LABEL(last.type)} · ${whenTxt(last.at)}` : stepName;
    }
    else if (eng) { tone = "warn"; pill = "Engraving"; now = `Back engraving · ${eng.state === "words" ? "words to read" : eng.state}`; }
    else if (rows.length) { const st = rows[0].state; tone = "info"; pill = "In the pull"; now = st === "pooled" ? "Ready to nest" : st === "noDesign" ? "Nothing to cut" : st === "waiting" ? "Waiting" : "In this pull"; }
    else if (c) { tone = "info"; pill = "In the cloud"; now = last ? `${TYPE_LABEL(last.type)} · ${whenTxt(last.at)}` : c.archived ? "Design completed" : "Found in the cloud"; }
    else { tone = "neutral"; pill = "Seen"; now = ""; }
    return { tone, pill, now, sheets, reached, steps: stepsOf(e), stepName, cancelled: !!cancel, hold: !!hold };
  }
  /** The card's dots: the order's own steps, done up to the step reached (a step the order skips, Welded for a piece that
      is not a stud earring, is not drawn; one it has a record of anyway is kept, as the order view keeps it). */
  function rail(st) {
    const all = allSteps(), steps = st.steps && st.steps.length ? st.steps : all;
    const own = steps.length < all.length && all[st.reached] && !steps.includes(all[st.reached]) ? all.filter(s => steps.includes(s) || s === all[st.reached]) : steps;
    // the position on its own steps of the furthest step reached (a step it passes over counts as passed)
    const at = own.filter(s => { const i = all.indexOf(s); return i >= 0 && i <= st.reached; }).length - 1;
    let h = "";
    for (let i = 0; i < own.length; i++) {
      const cls = st.cancelled ? (i <= at ? "d" : i === at + 1 ? "x" : "f") : i <= at ? "d" : i === at + 1 ? "c" : "";
      h += (i ? `<b class="${i <= at ? "d" : ""}"></b>` : "") + `<i class="${cls}" title="${esc(own[i].l)}"></i>`;
    }
    const label = st.cancelled ? "Cancelled" : (own[Math.max(0, at)] || {}).l || st.stepName;
    return `<span class="cnsRail" aria-label="${esc(label)}">${h}</span>`;
  }
  const mark = (text, q) => { const s = String(text || ""), i = q ? s.toLowerCase().indexOf(q) : -1; return i < 0 ? esc(s) : esc(s.slice(0, i)) + `<mark>${esc(s.slice(i, i + q.length))}</mark>` + esc(s.slice(i + q.length)); };
  function cardHtml(h, R, one) {
    const e = h.e, st = stateOf(e), q = R.num || (R.q.split(" ")[0] || ""), n = e.num || e.rid;
    const numHtml = R.num ? mark(n === e.rid || n.indexOf(R.num) >= 0 ? n : e.rid, R.num) : esc(n);
    let also = "";
    if (R.num && h.s === 2) { const li = e.listings.find(x => x.indexOf(R.num) >= 0); if (li) also = `<span class="cnsTag">listing ${mark(li, R.num)}</span>`; }
    const sku = !R.num && q ? e.skus.find(x => x.toLowerCase().includes(q)) || e.titles.find(x => x.toLowerCase().includes(q)) : null;
    const what = sku ? mark(sku, q) : esc(e.skus.slice(0, 2).join(" · ") + (e.skus.length > 2 ? ` +${e.skus.length - 2}` : ""));
    return `<div class="cnsTop"><span class="cnsNum mono">${numHtml}</span><span class="cnsWho">${R.num ? esc(e.buyer) : mark(e.buyer, q)}</span>${also}${e.cloud ? `<span class="cnsTag cloud" title="Not in memory here: read from the cloud">from the cloud</span>` : ""}` +
      `<span class="cnsPill ${st.tone}"><span class="d"></span>${esc(st.pill)}</span></div>` +
      `<div class="cnsMeta">${rail(st)}<span class="cnsNow${st.cancelled ? " bad" : ""}" title="Where it is now">${esc(st.now)}</span>` +
      `${st.sheets.length ? `<span class="cnsSheet">${esc(st.sheets.slice(0, 2).map(s => s.label).join(" · "))}${st.sheets.length > 2 ? ` +${st.sheets.length - 2}` : ""}</span>` : ""}<span class="cnsWhat">${what}</span></div>` +
      (one ? `<span class="cnsEnter">↵ open</span>` : "");
  }

  /* ═══ the cloud: a whole number that is nowhere here, looked up once the typing pauses ═══ */
  let cancelList = [], cancelAt = 0, cancelBusy = false;
  const fnBase = () => (typeof FN !== "undefined" ? FN : location.origin + "/.netlify/functions");
  const sandbox = () => { const s = St(); return !!(s && s.settings && s.settings.sandbox === "on"); };
  function headers() { const h = { "Content-Type": "application/json" }, s = St(); if (s && s.passcode) h["X-Edit-Passcode"] = s.passcode; return h; }
  function signalOf(ctl, t) { try { return AbortSignal.any ? AbortSignal.any([ctl.signal, AbortSignal.timeout(t)]) : ctl.signal; } catch (_) { return ctl.signal; } }
  async function lib(op, body, sig) {
    const res = await fetch(`${fnBase()}/charmNestLibrary`, { method: "POST", headers: headers(), body: JSON.stringify(Object.assign({ op }, body, sandbox() ? { sandbox: true } : {})), signal: sig });
    if (!res.ok) throw new Error(op + ": HTTP " + res.status);
    return res.json();
  }
  async function archive(id, sig) {
    const res = await fetch(`${fnBase()}/designArchive?op=get&id=${encodeURIComponent(id)}${sandbox() ? "&sandbox=1" : ""}`, { headers: headers(), signal: sig });
    if (!res.ok) throw new Error("archive: HTTP " + res.status);
    return res.json();
  }
  const cloudCache = new Map();                                                   // number → { found, at } for this session
  let cloudCtl = null, cloudTimer = 0;
  function cloudAbort() { clearTimeout(cloudTimer); cloudTimer = 0; if (cloudCtl) { try { cloudCtl.abort(); } catch (_) {} cloudCtl = null; } UI.cloud = null; }
  async function lookup(q) {
    const known = cloudCache.get(q);
    if (known && Date.now() - known.at < 120000) return known.found;
    const ctl = cloudCtl = new AbortController(), sig = signalOf(ctl, 20000);
    let bad = 0, why = "";
    const soft = p => p.catch(e => { if (ctl.signal.aborted) throw e; bad++; why = why || (e && e.message) || "no answer"; return null; });
    const [tl, pl, fs, ar] = await Promise.all([soft(lib("timelineGet", { orderId: q }, sig)), soft(lib("poolList", { orderId: q }, sig)),
      soft(lib("findSheets", { q, fallback: false, today: new Date().toISOString().slice(0, 10) }, sig)), soft(archive(q, sig))]);
    if (ctl.signal.aborted) throw new DOMException("aborted", "AbortError");
    const events = (tl && tl.events) || [], pools = (pl && pl.pools) || [], row = ar && ar.success && ar.row ? ar.row : null;
    const onSheet = ((fs && fs.sheets) || []).filter(s => (s.match || []).includes("order")), cutRows = ((fs && fs.rows) || []).filter(r => (r.match || []).includes("order"));
    const found = events.length || (tl && tl.cancelled) || pools.length || onSheet.length || cutRows.length || row;
    // nothing found where a read failed is no answer: said as such, and not kept as "not in the cloud"
    if (!found && bad) throw new Error(why);
    let c = null;
    if (found) {
      c = { rid: q, buyer: (row && row.buyer && row.buyer.name) || (tl && tl.cancelled && tl.cancelled.buyer) || "", skus: [], titles: [], listings: [], sheets: [], events, cancel: tl && tl.cancelled || null, archived: !!row,
        shipped: !!(row && Array.isArray(row.shipments) && row.shipments.length), at: 0,
        // the server's own step (its where.step, 0-7) and the pieces ordered (read for a stud: its steps)
        step: tl && tl.where && Number.isFinite(+tl.where.step) ? Math.round(+tl.where.step) : -1,
        items: ((row && row.items) || []).filter(it => it && typeof it === "object").slice(0, 20).map(it => ({ title: it.title || "", variations: Array.isArray(it.variations) ? it.variations : [], sku: it.sku || "" })) };
      for (const it of (row && row.items) || []) { push(c.skus, it.sku); push(c.titles, it.title); push(c.listings, it.listingId && String(it.listingId)); }
      for (const p of pools) { push(c.skus, p.sku); if (p.sheetId || p.sheetName) c.sheets.push({ id: p.sheetId || p.sheetName, label: sheetWords(p.sheetName) || "on a sheet", cut: false }); }
      for (const s of onSheet) c.sheets.push({ id: s.id, label: `${CODE[s.metal] || ""} Sheet ${s.sheetIndex || s.page || 1}`.trim(), cut: +s.laserDoneAt > 0 });
      for (const r of cutRows) c.sheets.push({ id: r.id, label: `${CODE[r.metal] || ""} Sheet ${r.sheetIndex || 1}`.trim(), cut: true });
      for (const l of (c.cancel && c.cancel.lines) || []) { push(c.skus, l.sku); push(c.titles, l.title); }
      c.at = Math.max(0, ...events.map(e => +e.at || 0), row ? +row.completedAtMs || 0 : 0);
      IX.cloud.set(q, c);
    }
    cloudCache.set(q, { found: !!found, at: Date.now() });
    return !!found;
  }
  /** The newest cancelled orders, with what was ordered: read once when the search first opens, then every few minutes. */
  function readCancelled() {
    const C = W.Cancelled; if (!C || !C.history || cancelBusy || Date.now() - cancelAt < 180000) return;
    cancelBusy = true;
    C.history().then(list => { cancelList = Array.isArray(list) ? list.slice() : []; cancelAt = Date.now(); if (isOpen()) refresh(); }, () => { cancelAt = Date.now() - 120000; }).finally(() => { cancelBusy = false; });
  }

  /* ═══ the window ═══ */
  const UI = { root: null, box: null, q: null, list: null, halo: null, msg: null, count: null, nodes: new Map(), R: null, sel: 0, shown: [], cloud: null, lastCount: 0, tick: 0, watch: 0, opening: false, closing: false };
  const ICON = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" aria-hidden="true"><circle cx="11" cy="11" r="7"/><path d="M20 20l-4-4"/></svg>`;
  const STYLE = `
.cnsFind{flex:0 1 232px;min-width:34px;display:flex;align-items:center;gap:8px;height:30px;padding:0 7px 0 10px;border:1px solid var(--line);border-radius:9px;background:var(--card2);color:var(--ink45);font:500 12px var(--sans);cursor:pointer;white-space:nowrap;overflow:hidden;transition:border-color .2s,box-shadow .2s,background .2s,color .2s}
.cnsFind:hover,.cnsFind:focus-visible{border-color:var(--goldLine);box-shadow:0 0 0 3px rgba(202,168,97,.14);color:var(--ink70);outline:0}
.cnsFind svg{width:14px;height:14px;flex:none;color:var(--gold)}
.cnsFind .t{overflow:hidden;text-overflow:ellipsis}
.cnsKbd{margin-left:auto;font:10px var(--mono);border:1px solid var(--line);border-bottom-width:2px;border-radius:5px;padding:0 5px;color:var(--ink45);background:var(--card);line-height:15px}
@container workspace (max-width:1180px){.cnsFind{flex-basis:152px}}
@container workspace (max-width:1000px){.cnsFind{flex:0 0 32px;padding:0;justify-content:center}.cnsFind .t,.cnsFind .cnsKbd{display:none}}
.topbar:has(>.runBanner:not(.hidden)) .cnsFind{flex:0 0 32px;padding:0;justify-content:center}
.topbar:has(>.runBanner:not(.hidden)) .cnsFind :is(.t,.cnsKbd){display:none}
@container workspace (max-width:1300px){.topbar:has(>.runBanner:not(.hidden)) .cnsFind{display:none}}
.cns{position:fixed;inset:0;z-index:250;display:flex;justify-content:center;align-items:flex-start;padding:9vh 16px 16px}
.cns[hidden]{display:none}
.cnsBd{position:absolute;inset:0;background:rgba(20,18,15,.52)}
.cnsBox{position:relative;width:min(760px,100%);max-height:82vh;display:flex;flex-direction:column;background:var(--card);border-radius:16px;box-shadow:0 30px 80px rgba(20,16,10,.28);overflow:hidden}
.cnsIn{display:flex;align-items:center;gap:12px;padding:14px 18px;border-bottom:1px solid var(--line);flex:none}
.cnsIn svg{width:18px;height:18px;color:var(--gold);flex:none}
.cnsIn input{flex:1;min-width:0;border:0;outline:0;background:transparent;font:22px var(--mono);letter-spacing:.06em;color:var(--ink);caret-color:var(--gold);padding:0}
.cnsIn input::placeholder{font:16px var(--sans);letter-spacing:0;color:var(--ink25)}
.cnsIn input::-webkit-search-cancel-button{display:none}
.cnsCount{font:11px var(--mono);color:var(--ink45);white-space:nowrap;letter-spacing:.04em}
.cnsCount b{color:var(--ink);font-size:13px}
.cnsRes{position:relative;overflow:auto;padding:10px;min-height:0;flex:1 1 auto;overscroll-behavior:contain}
.cnsList{display:grid;gap:8px;position:relative;z-index:1}
.cnsHalo{position:absolute;left:10px;right:10px;top:0;height:0;border-radius:12px;border:1px solid var(--goldLine);box-shadow:0 0 0 3px rgba(202,168,97,.2),0 10px 28px rgba(169,130,63,.14);pointer-events:none;z-index:2;opacity:0;transition:transform .26s ${SPRING},opacity .18s}
.cnsHalo.on{opacity:1}
.cnsCard{position:relative;display:grid;gap:8px;padding:12px 14px;border:1px solid var(--line);border-radius:12px;background:var(--card);cursor:pointer;transition:border-color .18s,background .18s}
.cnsCard:hover{border-color:var(--ink25)}
.cnsCard.sel{background:var(--card2)}
.cnsCard.one{animation:cnsOne 1.6s ease-in-out infinite}
.cnsCard.one .cnsMeta{padding-right:62px}
@keyframes cnsOne{50%{box-shadow:0 0 0 4px rgba(202,168,97,.14)}}
.cnsTop{display:flex;align-items:baseline;gap:10px;min-width:0}
.cnsNum{font:600 17px var(--mono);letter-spacing:.04em;color:var(--ink);flex:none}
.cnsNum mark,.cnsCard mark{background:linear-gradient(transparent 10%,var(--goldSoft) 10%,var(--goldSoft) 92%,transparent 92%);color:#6b4d16;border-radius:3px;padding:0 1px}
.cnsWho{font:15px var(--serif);color:var(--ink70);min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.cnsPill{margin-left:auto;align-self:center;font:700 9.5px var(--mono);padding:4px 9px;border-radius:30px;display:inline-flex;align-items:center;gap:6px;white-space:nowrap;letter-spacing:.08em;text-transform:uppercase;background:var(--paper2);color:var(--ink70);flex:none}
.cnsPill .d{width:7px;height:7px;border-radius:50%;background:currentColor}
.cnsPill.ok{background:var(--sageSoft);color:#3c5a39}.cnsPill.warn{background:var(--goldSoft);color:#7a5a1d}.cnsPill.bad{background:var(--claySoft);color:#8a3a26}.cnsPill.info{background:var(--slateSoft);color:#33525d}
.cnsMeta{display:flex;align-items:center;gap:14px;font:11.5px var(--sans);color:var(--ink45);min-width:0}
.cnsNow{color:var(--ink70);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;min-width:0}
.cnsNow.bad{color:var(--clay);font-weight:650}
.cnsSheet{font:10px var(--mono);letter-spacing:.06em;border:1px solid var(--line);border-radius:6px;padding:1px 6px;color:var(--ink70);white-space:nowrap;flex:none}
.cnsWhat{margin-left:auto;font:10.5px var(--mono);color:var(--ink45);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;min-width:0;max-width:34%}
.cnsTag{font:10px var(--mono);color:var(--ink45);border-radius:6px;padding:1px 6px;background:var(--paper2);white-space:nowrap;flex:none}
.cnsTag.cloud{background:var(--slateSoft);color:#33525d}
.cnsRail{display:flex;align-items:center;flex:none}
.cnsRail i{width:7px;height:7px;border-radius:50%;border:1.5px solid var(--ink25);background:var(--card);flex:none}
.cnsRail i.d{background:var(--sage);border-color:var(--sage)}
.cnsRail i.c{background:var(--gold2);border-color:var(--gold);box-shadow:0 0 0 3px rgba(202,168,97,.25)}
.cnsRail i.x{background:var(--clay);border-color:var(--clay)}
.cnsRail i.f{opacity:.45}
.cnsRail b{width:8px;height:1.5px;background:var(--ink25)}
.cnsRail b.d{background:var(--sage)}
.cnsEnter{position:absolute;right:12px;bottom:10px;font:10px var(--mono);color:#7a5a1d;background:var(--goldSoft);border-radius:6px;padding:2px 7px;animation:cnsPop .36s ${SPRING} both}
@keyframes cnsPop{from{transform:scale(.6);opacity:0}}
.cnsCard.go{animation:cnsGo .34s ${E}}
@keyframes cnsGo{0%{transform:none}30%{transform:scale(.975)}70%{transform:scale(1.012)}100%{transform:none}}
.cnsCard.go::after{content:"";position:absolute;inset:-1px;border-radius:12px;border:2px solid var(--gold2);animation:cnsRing .5s ease-out forwards;pointer-events:none}
@keyframes cnsRing{from{opacity:.9;transform:scale(1)}to{opacity:0;transform:scale(1.06,1.35)}}
.cnsCard.go .cnsNum mark{animation:cnsGlint .5s ease-out}
@keyframes cnsGlint{50%{background:var(--gold2);color:#fff}}
.cnsMsg:empty{display:none}
.cnsMsg{padding:10px 6px 4px;text-align:center;color:var(--ink45);font-size:12.5px;position:relative;z-index:1}
.cnsWait{display:inline-flex;align-items:center;gap:9px;padding:7px 12px;border-radius:30px;background:var(--card2);border:1px solid var(--line2);color:var(--ink70)}
.cnsSpin{width:12px;height:12px;border:2px solid rgba(0,0,0,.14);border-top-color:var(--gold);border-radius:50%;animation:spin .7s linear infinite;flex:none}
.cnsEmpty{padding:26px 16px 22px;display:grid;justify-items:center;gap:8px;color:var(--ink45);font-size:13px}
.cnsEmpty svg{width:44px;height:44px;color:var(--goldLine);animation:cnsBob 2.4s ease-in-out infinite}
@keyframes cnsBob{50%{transform:translateY(-3px) rotate(-6deg)}}
.cnsEmpty b{color:var(--ink);font-family:var(--mono)}
.cnsEmpty small{font-size:11.5px;color:var(--ink45)}
.cnsFoot{padding:8px 18px 11px;font:10.5px var(--mono);color:var(--ink45);display:flex;gap:16px;border-top:1px solid var(--line2);background:var(--card2);flex:none}
.cnsFoot .r{margin-left:auto;text-align:right}
.cnsHead{font:700 9.5px var(--mono);letter-spacing:.1em;text-transform:uppercase;color:var(--ink45);padding:2px 4px 0}
.cnsDetail{margin:2px 0 0;padding:10px 0 0;border-top:1px dashed var(--line);display:grid;gap:6px;font:12px var(--sans);color:var(--ink70)}
.cnsDetail>div{display:grid;grid-template-columns:86px minmax(0,1fr);gap:12px}
.cnsDetail dt{font:700 9.5px var(--mono);letter-spacing:.1em;text-transform:uppercase;color:var(--ink45);padding-top:2px}
.cnsDetail dd{margin:0;min-width:0}.cnsDetail .t{color:var(--ink45);font-size:11px}
.cnsLift{position:fixed;z-index:260;pointer-events:none;margin:0}
.cnsLift .cnsOpening{position:absolute;right:12px;bottom:10px;display:inline-flex;align-items:center;gap:7px;font:10px var(--mono);color:var(--ink70)}
#owTitle.cnsFound{position:relative}
#owTitle.cnsFound::after{content:"";position:absolute;inset:-1px -4px;border-radius:4px;background:var(--goldSoft);mix-blend-mode:multiply;transform-origin:left center;pointer-events:none;animation:cnsSweep 3.2s ease-out forwards}
@keyframes cnsSweep{0%{transform:scaleX(0);opacity:1}15.6%{transform:scaleX(1);opacity:1}78%{transform:scaleX(1);opacity:1}100%{transform:scaleX(1);opacity:0}}
@media (max-width:700px){.cnsMeta{flex-wrap:wrap;gap:6px 12px}.cnsWhat{max-width:100%;margin-left:0}.cnsFoot .r{display:none}.cnsIn input{font-size:18px}}
@media (prefers-reduced-motion:reduce){.cnsHalo{transition:none}.cnsCard.one,.cnsEmpty svg{animation:none}}`;

  function style() { if (doc.getElementById("cnsStyle")) return; const st = doc.createElement("style"); st.id = "cnsStyle"; st.textContent = STYLE; doc.head.appendChild(st); }
  function mount() {
    if (UI.root) return;
    style();
    const root = UI.root = doc.createElement("div");
    root.className = "cns"; root.id = "cnSearch"; root.hidden = true; root.setAttribute("role", "dialog"); root.setAttribute("aria-modal", "true"); root.setAttribute("aria-label", "Find an order");
    root.innerHTML = `<div class="cnsBd" data-cns-close></div><div class="cnsBox"><div class="cnsIn">${ICON}<input id="cnsQ" type="search" role="combobox" aria-expanded="true" aria-controls="cnsList" aria-autocomplete="list" autocomplete="off" spellcheck="false" placeholder="Type an Etsy order number, a buyer, a SKU or a listing"><div class="cnsCount" id="cnsCount" aria-live="polite"></div></div>` +
      `<div class="cnsRes" id="cnsRes"><div class="cnsHalo" aria-hidden="true"></div><div class="cnsList" id="cnsList" role="listbox" aria-label="Orders"></div><div class="cnsMsg" id="cnsMsg"></div></div>` +
      `<div class="cnsFoot"><span>↑ ↓ choose</span><span>↵ open</span><span>esc close</span><span class="r">this pull · sheets · cancelled · the cloud</span></div></div>`;
    doc.body.appendChild(root);
    UI.box = root.querySelector(".cnsBox"); UI.q = root.querySelector("#cnsQ"); UI.list = root.querySelector("#cnsList"); UI.halo = root.querySelector(".cnsHalo"); UI.msg = root.querySelector("#cnsMsg"); UI.count = root.querySelector("#cnsCount"); UI.res = root.querySelector("#cnsRes");
    UI.q.addEventListener("input", () => refresh(true));
    UI.q.addEventListener("keydown", onInputKey);
    root.addEventListener("click", e => {
      if (e.target.closest("[data-cns-close]")) return close();
      const card = e.target.closest(".cnsCard"); if (card) { const i = UI.shown.findIndex(h => h.e.rid === card.dataset.rid); if (i >= 0) { select(i); openHit(i); } }
    });
    root.addEventListener("pointermove", e => { const card = e.target.closest && e.target.closest(".cnsCard"); if (!card) return; const i = UI.shown.findIndex(h => h.e.rid === card.dataset.rid); if (i >= 0 && i !== UI.sel) select(i, true); }, { passive: true });
    // Tab stays in the box: the list is walked with the arrows
    root.addEventListener("keydown", e => { if (e.key === "Tab") { e.preventDefault(); UI.q.focus(); } });
    // (a click anywhere in the box leaves the focus in the field: Esc, the arrows, Enter and typing stay with it)
    root.addEventListener("mousedown", e => { if (e.target !== UI.q) e.preventDefault(); });
  }
  function trigger() {
    style();
    if (doc.getElementById("cnsFind")) return;
    const bar = doc.querySelector(".topbar"); if (!bar) return;
    const b = doc.createElement("button");
    b.type = "button"; b.className = "cnsFind"; b.id = "cnsFind"; b.title = "Find any order: an Etsy number, a buyer, a SKU (/ or Ctrl K)"; b.setAttribute("aria-label", "Find an order");
    b.innerHTML = `${ICON}<span class="t">Find an order</span><span class="cnsKbd">/</span>`;
    b.addEventListener("click", () => open());
    b.addEventListener("pointerenter", warm, { once: true });
    const tools = bar.querySelector(".topTools");
    bar.insertBefore(b, tools || null);
  }
  function warm() { try { ensure(); } catch (e) { console.warn("order search index", e); } readCancelled(); }

  const isOpen = () => !!(UI.root && !UI.root.hidden && !UI.closing);
  function open(prefill) {
    mount();
    // (still fading out after Esc: "/" or Ctrl+K pressed meanwhile opens it again, from where the fade is)
    const back = UI.closing ? +getComputedStyle(UI.root).opacity : null;
    if (back != null) { UI.closing = false; for (const a of UI.root.getAnimations({ subtree: true })) if (a.effect && a.effect.target && (a.effect.target === UI.root || a.effect.target === UI.box)) a.cancel(); UI.root.style.opacity = ""; UI.root.hidden = true; }
    if (isOpen()) { UI.q.focus(); UI.q.select(); return; }
    readCancelled();
    try { ensure(); } catch (e) { console.warn("order search index", e); }
    UI.prev = doc.activeElement && !UI.root.contains(doc.activeElement) ? doc.activeElement : null;
    doc.querySelectorAll(".cnsLift").forEach(n => n.remove());                   // a card still lifted from the last open goes
    UI.root.hidden = false; UI.opening = false;
    UI.q.value = prefill == null ? "" : String(prefill);
    UI.nodes.forEach(n => n.remove()); UI.nodes.clear(); UI.lastCount = IX.list.length;
    refresh(true);
    UI.q.focus(); UI.q.select();
    if (back != null && !reduced()) UI.root.animate([{ opacity: back }, { opacity: 1 }], { duration: 160, easing: "ease-out" });
    else if (!reduced()) {
      UI.root.querySelector(".cnsBd").animate([{ opacity: 0 }, { opacity: 1 }], { duration: 180, easing: "ease" });
      UI.box.animate([{ opacity: 0, transform: "translateY(-10px) scale(.98)" }, { opacity: 1, transform: "none" }], { duration: 260, easing: E });
    }
    // what the page holds keeps changing while the box is open (a pull, a sheet written, a cancel): the index follows
    clearInterval(UI.watch);
    UI.watch = setInterval(() => { if (!isOpen()) return clearInterval(UI.watch); if (stale()) { build(); refresh(false); } }, 1200);
  }
  function close(fast) {
    if (!isOpen()) return;
    cloudAbort(); clearInterval(UI.watch);
    // focus goes back where it was (a hidden box kept it, and "/" then read as typing), unless a window took it meanwhile
    const root = UI.root, back = () => { const a = doc.activeElement; if (a && a !== doc.body && !root.contains(a)) return; const p = UI.prev; if (p && p.isConnected && p !== doc.body && !p.closest("[hidden],dialog:not([open])")) { try { p.focus({ preventScroll: true }); } catch (_) {} } if (root.contains(doc.activeElement)) doc.activeElement.blur(); };
    UI.closing = true;
    const done = () => { if (!UI.closing) return; UI.closing = false; root.hidden = true; root.style.opacity = ""; back(); };
    back();
    if (fast || reduced()) return done();
    UI.box.animate([{ opacity: 1 }, { opacity: 0, transform: "translateY(-6px) scale(.99)" }], { duration: 160, easing: "ease-in" });
    root.animate([{ opacity: 1 }, { opacity: 0 }], { duration: 180 }).finished.then(done, done);
    root.style.opacity = "0";
  }

  /* One keystroke: the pass over the index, the cards, and the cloud if it comes to that. */
  function refresh(typed) {
    if (!isOpen()) return;
    const was = !typed && UI.shown[UI.sel] ? UI.shown[UI.sel].e.rid : null;
    const R = UI.R = query(UI.q.value);
    UI.lastMs = R.ms;
    if (typed) { cloudAbort(); UI.sel = 0; }
    else if (was) { const i = R.hits.findIndex(h => h.e.rid === was); if (i >= 0) UI.sel = i; }   // (the order chosen, not its place)
    paint(R);
    ticker(R);
    if (typed) planCloud(R);
  }
  function planCloud(R) {
    const n = R.num;
    if (!n || n.length < CLOUD_MIN || n.length > CLOUD_MAX) return message(R);
    const exact = R.all.some(e => e.rid === n || e.num === n);
    if (exact) return message(R);
    const known = cloudCache.get(n);
    if (known && Date.now() - known.at < 120000) { UI.cloud = { q: n, state: known.found ? "found" : "none" }; return message(R); }
    UI.cloud = { q: n, state: "wait" };
    message(R);
    cloudTimer = setTimeout(() => {
      cloudTimer = 0; if (!UI.cloud || UI.cloud.q !== n) return;
      UI.cloud.state = "busy"; message(UI.R);
      lookup(n).then(found => {
        if (!UI.cloud || UI.cloud.q !== n) return;
        UI.cloud.state = found ? "found" : "none";
        if (found) { build(); refresh(false); } else message(UI.R);
      }, err => {
        if (err && err.name === "AbortError") return;
        if (UI.cloud && UI.cloud.q === n) { UI.cloud.state = "error"; UI.cloud.err = err && err.message; message(UI.R); }
      });
    }, PAUSE);
  }
  const EMPTY_ICON = `<svg viewBox="0 0 48 48" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" aria-hidden="true"><circle cx="21" cy="21" r="12"/><path d="M30 30l9 9"/><path d="M16.5 19.5h.01M25.5 19.5h.01" stroke-width="3.4"/><path d="M16.5 25.5c2.5 2 6.5 2 9 0"/></svg>`;
  function message(R) {
    const c = UI.cloud && R.num && UI.cloud.q === R.num ? UI.cloud : null;
    let h = "";
    if (c && (c.state === "busy" || c.state === "wait")) h = `<span class="cnsWait"><span class="cnsSpin"></span>Looking in the cloud for <b class="mono">${esc(c.q)}</b>…</span>`;
    else if (!R.hits.length && R.q) {
      const why = c && c.state === "none" ? "Not in this pull, on any sheet in memory, under Cancelled, or in the cloud."
        : c && c.state === "error" ? `The cloud could not be asked just now (${esc(c.err || "no answer")}). Try again in a moment.`
        : R.num && R.num.length < CLOUD_MIN ? "Keep typing: Etsy order numbers have 10 digits." : R.num && R.num.length > CLOUD_MAX ? "Etsy order numbers have 10 digits: look for one at a time." : R.num ? "" : "Try an order number, a buyer's name, a SKU or a listing number.";
      h = `<div class="cnsEmpty">${EMPTY_ICON}<div>No order has <b>${esc(R.q)}</b> in it${c && c.state === "none" ? "" : " here"}.</div>${why ? `<small>${why}</small>` : ""}</div>`;
    } else if (!R.q && !R.hits.length) h = `<div class="cnsEmpty">${EMPTY_ICON}<div>Nothing loaded yet.</div><small>Type a whole Etsy order number to look it up in the cloud.</small></div>`;
    if (UI.msg.innerHTML !== h) UI.msg.innerHTML = h;
  }
  function ticker(R) {
    cancelAnimationFrame(UI.tick);
    const a = UI.lastCount, b = R.q ? R.total : IX.list.length, word = R.q ? (b === 1 ? "match" : "matches") : "orders", t = R.ms < 1 ? "&lt;1" : R.ms.toFixed(1), D = reduced() || a === b ? 0 : 240, t0 = performance.now();
    UI.lastCount = b;
    const f = now => { const k = D ? Math.min(1, (now - t0) / D) : 1, v = Math.round(a + (b - a) * (1 - Math.pow(1 - k, 3))); UI.count.innerHTML = `<b>${v.toLocaleString()}</b> ${word} · ${t} ms`; if (k < 1) UI.tick = requestAnimationFrame(f); };
    if (D) UI.tick = requestAnimationFrame(f); else f(t0);
  }
  /* The cards: one node per order, kept while it stays in the list, so what stays glides to its new place (FLIP) and
     what is new comes in one after another. */
  function paint(R) {
    const list = UI.list, shown = UI.shown = R.hits.slice(0, MAX), one = !!R.q && R.total === 1, anim = !reduced();
    const before = new Map();
    if (anim) for (const [rid, n] of UI.nodes) before.set(rid, n.getBoundingClientRect().top);
    const keep = new Set(), order = [];
    shown.forEach((h, i) => {
      let n = UI.nodes.get(h.e.rid);
      if (!n) { n = doc.createElement("div"); n.className = "cnsCard"; n.dataset.rid = h.e.rid; n.id = "cns-" + h.e.rid; n.setAttribute("role", "option"); n._fresh = true; UI.nodes.set(h.e.rid, n); }
      const html = cardHtml(h, R, one);
      if (n._html !== html) { n.innerHTML = html; n._html = html; }
      n.classList.toggle("one", one);
      keep.add(h.e.rid); order.push(n);
    });
    for (const [rid, n] of UI.nodes) if (!keep.has(rid)) { n.remove(); UI.nodes.delete(rid); }
    if (!R.q && shown.length) { if (!UI.head) { UI.head = doc.createElement("div"); UI.head.className = "cnsHead"; UI.head.textContent = "Latest orders"; } if (list.firstChild !== UI.head) list.insertBefore(UI.head, list.firstChild); }
    else if (UI.head && UI.head.isConnected) UI.head.remove();
    const off = list.firstChild === UI.head ? 1 : 0;
    order.forEach((n, i) => { if (list.children[i + off] !== n) list.insertBefore(n, list.children[i + off] || null); });
    if (anim) {
      // (a card still gliding goes on from where it is seen: its glide ends before its new place is read)
      for (const n of order) if (n._glide) { n._glide.cancel(); n._glide = null; }
      let k = 0;
      for (const n of order) {
        if (n._fresh) { n._fresh = false; n.animate([{ opacity: 0, transform: "translateY(8px) scale(.985)" }, { opacity: 1, transform: "none" }], { duration: 220, delay: Math.min(k++, 8) * 30, easing: E, fill: "backwards" }); continue; }
        const was = before.get(n.dataset.rid); if (was == null) continue;
        const dy = was - n.getBoundingClientRect().top;
        if (Math.abs(dy) > 1) n._glide = n.animate([{ transform: `translateY(${dy}px)` }, { transform: "none" }], { duration: 280, easing: E });
      }
    }
    select(Math.min(UI.sel, Math.max(0, shown.length - 1)), true);
    message(R);
  }
  function select(i, quiet) {
    UI.sel = i;
    const n = UI.shown[i] && UI.nodes.get(UI.shown[i].e.rid);
    for (const x of UI.nodes.values()) if (x !== n && x.classList.contains("sel")) { x.classList.remove("sel"); x.setAttribute("aria-selected", "false"); }
    if (!n) { UI.halo.classList.remove("on"); UI.q.removeAttribute("aria-activedescendant"); return; }
    n.classList.add("sel"); n.setAttribute("aria-selected", "true"); UI.q.setAttribute("aria-activedescendant", n.id);
    // the glow slides to the card (its place in the list, not where a card still gliding happens to be)
    UI.halo.style.transform = `translateY(${n.offsetTop + UI.list.offsetTop}px)`; UI.halo.style.height = n.offsetHeight + "px"; UI.halo.classList.add("on");
    if (!quiet) n.scrollIntoView({ block: "nearest", behavior: reduced() ? "auto" : "smooth" });
  }
  function onInputKey(e) {
    const n = UI.shown.length;
    if (e.key === "ArrowDown" || e.key === "ArrowUp") { e.preventDefault(); if (n) select((UI.sel + (e.key === "ArrowDown" ? 1 : -1) + n) % n); }
    else if (e.key === "Enter") { e.preventDefault(); if (n) openHit(UI.sel); }
    else if (e.key === "Escape") { e.preventDefault(); e.stopPropagation(); if (UI.q.value) { UI.q.value = ""; refresh(true); } else close(); }
  }

  /* ═══ opening an order: the card answers the press, the box lets go of it, and the order's view grows out of it ═══ */
  function openHit(i) {
    const h = UI.shown[i]; if (!h || UI.opening) return;
    const e = h.e, card = UI.nodes.get(e.rid); UI.opening = true;
    if (card && !reduced()) { card.classList.remove("go"); void card.offsetWidth; card.classList.add("go"); }
    const q = UI.R && UI.R.num;
    // (a real action waits on its animation for no more than a beat)
    setTimeout(() => { try { go(e, card, q); } finally { UI.opening = false; } }, reduced() ? 0 : 170);
  }
  function lift(card) {
    if (!card || !card.isConnected) return null;
    const r = card.getBoundingClientRect(); if (!r.width) return null;
    const c = card.cloneNode(true); c.removeAttribute("id"); c.classList.remove("go", "one"); c.classList.add("cnsLift", "sel");
    Object.assign(c.style, { left: r.left + "px", top: r.top + "px", width: r.width + "px", height: r.height + "px" });
    c.querySelector(".cnsEnter")?.remove();
    doc.body.appendChild(c);
    return c;
  }
  function go(e, card, q) {
    const OW = W.OrderWin, rows = e.rows.filter(r => r.state !== "gone");
    // no view can show an order outside the pull yet (OrderWin.openOrder is not in): what is known opens in its card
    if (!(OW && (typeof OW.openOrder === "function" || rows.length))) return detail(e, card);
    const from = lift(card);
    close();
    let opened = false;
    try {
      if (OW && typeof OW.openOrder === "function") { const p = OW.openOrder(e.rid, { highlight: true, from, q: q || e.rid, row: rows[0] || null }); opened = true; settle(from, p); return; }
      if (OW && rows.length) {
        OW.open(rows[0].key); opened = OW.isOpen ? OW.isOpen() : true;
        const d = doc.getElementById("orderWin");
        if (opened && d) { growFrom(d, from); const t = doc.getElementById("owTitle"); if (t) { t.classList.remove("cnsFound"); void t.offsetWidth; t.classList.add("cnsFound"); } }
      }
    } catch (err) { console.warn("order search: open", err); }
    if (!opened && typeof toast === "function") toast(`Order ${e.rid} could not be opened just now`, "bad", 5200);
    settle(from, null, opened ? 460 : 0);
  }
  function detail(e, card) {
    if (!card) return;
    const had = card.querySelector(".cnsDetail"); if (had) { had.remove(); select(UI.sel, true); return; }
    const st = stateOf(e), c = e.cloud, cx = cancelOf(e);
    const lines = e.rows.length ? e.rows.map(r => [(r.spec && r.spec.designSku) || r.line.sku, r.line.title]) : e.skus.map((x, i) => [x, e.titles[i] || ""]);
    const evs = c ? c.events.slice(-5).reverse() : [];
    const row = (k, v) => `<div><dt>${esc(k)}</dt><dd>${v}</dd></div>`;
    card.insertAdjacentHTML("beforeend", `<dl class="cnsDetail">${lines.length ? row("Ordered", lines.slice(0, 6).map(([a, b]) => `<span class="mono">${esc(a || "no SKU")}</span>${b ? " " + esc(b) : ""}`).join("<br>")) : ""}` +
      (st.sheets.length ? row("Sheets", st.sheets.map(x => `${esc(x.label)}${x.cut ? " · cut" : ""}`).join(" · ")) : "") +
      (cx ? row("Cancelled", `${esc(cx.by || "")}${cx.at ? " · " + esc(whenTxt(cx.at)) : ""}${cx.why ? " · " + esc(cx.why) : ""}`) : "") +
      (evs.length ? row("Latest", evs.map(v => `${esc(TYPE_LABEL(v.type))}${v.by ? " · " + esc(v.by) : ""} <span class="t">${esc(whenTxt(v.at))}</span>`).join("<br>")) : "") + `</dl>`);
    const d = card.querySelector(".cnsDetail");
    if (d && !reduced()) d.animate([{ opacity: 0, transform: "translateY(-4px)" }, { opacity: 1, transform: "none" }], { duration: 220, easing: E });
    select(UI.sel, true);
  }
  // the lifted card stays where it was while the view opens (with a spinner, should the view read the cloud first), then goes
  // (settled: the view was opened at once, so the card goes once the view has grown out of it)
  function settle(from, p, settled) {
    if (!from) return;
    let gone = false;
    const bye = () => { if (gone || !from.isConnected) return; gone = true; if (reduced()) return from.remove(); from.animate([{ opacity: 1 }, { opacity: 0, transform: "scale(.98)" }], { duration: 220, easing: "ease-in" }).finished.then(() => from.remove(), () => from.remove()); };
    if (settled != null) return void setTimeout(bye, settled);
    const OW = W.OrderWin, t0 = Date.now();
    const waiting = () => { if (gone || !from.isConnected) return; if ((OW && OW.isOpen && OW.isOpen()) || isOpen() || Date.now() - t0 > 4000) return bye(); if (Date.now() - t0 > 450 && !from.querySelector(".cnsOpening")) from.insertAdjacentHTML("beforeend", `<span class="cnsOpening"><span class="cnsSpin"></span>Opening the order…</span>`); setTimeout(waiting, 120); };
    setTimeout(waiting, 380);
    if (p && typeof p.then === "function") p.then(() => setTimeout(bye, 300), bye);
  }
  function growFrom(d, from) {
    const src = from && from.isConnected ? from.getBoundingClientRect() : null;
    if (!src || !src.width || reduced()) return;
    const r = d.getBoundingClientRect(), dx = src.left + src.width / 2 - (r.left + r.width / 2), dy = src.top + src.height / 2 - (r.top + r.height / 2), s = Math.max(.3, Math.min(.8, src.height / r.height));
    try { d.animate([{ opacity: 0 }, { opacity: 1 }], { duration: 380, easing: "ease-out", pseudoElement: "::backdrop" }); } catch (_) {}
    d.animate([{ transform: `translate(${dx}px,${dy}px) scale(${s})`, opacity: 0 }, { opacity: 1, offset: .35 }, { transform: "none", opacity: 1 }], { duration: 500, easing: "cubic-bezier(.2,.75,.2,1)" });
  }

  /* ═══ the keys: "/" where nothing is being typed, Ctrl/Cmd+K anywhere ═══ */
  const typing = t => !!(t && (t.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName)) && !(UI.root && UI.root.hidden && UI.root.contains(t)));
  doc.addEventListener("keydown", e => {
    const k = e.key, cmdK = (e.ctrlKey || e.metaKey) && !e.altKey && !e.shiftKey && (k === "k" || k === "K");
    if (k === "Escape" && isOpen() && e.target !== UI.q && !doc.querySelector("dialog[open]")) { e.preventDefault(); close(); return; }
    const slash = k === "/" && !e.ctrlKey && !e.metaKey && !e.altKey && !typing(e.target);
    if (!cmdK && !slash) return;
    if (isOpen()) { if (cmdK) { e.preventDefault(); UI.q.focus(); UI.q.select(); } return; }
    // one window at a time: the order window gives way to the search (it keeps its note); any other stays in front, and
    // so does the order window when a window is open over it (a photo, a conversation), and the sign-in screen
    // (the order view's header number is its own search field when it has one: design spec 7)
    const si = doc.getElementById("signin"); if (si && !si.classList.contains("hidden")) return;
    const dlgs = doc.querySelectorAll("dialog[open]"), dlg = dlgs[0];
    if (dlg) {
      if (dlgs.length > 1 || dlg.id !== "orderWin" || !W.OrderWin) return;
      if (typeof OrderWin.focusSearch === "function") { e.preventDefault(); try { OrderWin.focusSearch(); } catch (_) {} return; }
      if (!cmdK) return;
      e.preventDefault(); try { OrderWin.close(); } catch (_) {} return open();
    }
    e.preventDefault(); open();
  }, true);

  function boot() {
    trigger();
    // the index is made while the page is idle, so the first "/" finds it ready
    const idle = W.requestIdleCallback || (f => setTimeout(f, 200));
    setTimeout(() => idle(() => { try { if (!IX.gen) build(); } catch (_) {} }), 4000);
  }
  if (doc.readyState === "loading") doc.addEventListener("DOMContentLoaded", boot, { once: true }); else boot();

  /** One order as the search knows it (for the order view's own header search): its entry and where it is now. */
  function find(rid) { ensure(); const e = IX.by.get(String(rid)); return e ? { entry: e, state: stateOf(e) } : null; }
  W.OrderSearch = { open, close, isOpen, find, lookup: q => lookup(String(q)).then(f => (f ? (build(), find(q)) : null)), query: q => { ensure(); return query(q); }, rebuild: () => build(), stats: () => ({ orders: IX.list.length, buildMs: IX.ms, lastMs: UI.lastMs || 0, gen: IX.gen, cloud: IX.cloud.size }), _IX: IX };
})();
