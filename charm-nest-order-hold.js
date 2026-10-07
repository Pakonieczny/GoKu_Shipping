/* Charm Nest · Hold, the whole order (Paul, 5 Oct 2026: an orange Hold button. "When an order gets put on hold all of its pieces
   must be removed from all sheets and sets of sheets and the empty spaces must be filled in with the next order inline, even if
   that order has to be taken from a newer/incomplete sheet of the same colour just to fill the empty spot.")

   This file is the engine, with no screen of its own. It drives the machinery the sheet window already has for taking a charm
   off a sheet by hand (SheetWin.holdKit: Take off, the freed room found with the nest's own collision grid, and the verified,
   restart-safe move of an order into it), one sheet at a time, and says what it does as it does it.

     OrderHold.plan(rid, opts)             read only, no writes: what Hold would do to this order (the Plan below). opts.snapshot
                                            plans from a given snapshot (fixtures); opts.exact === false skips the real room search.
     OrderHold.run(rid, { name, note, onStep })  does it: Promise<{ ok, held, steps, error? }>. onStep(step) is called as each real
                                            step is through. Every step carries `at` (epoch ms) and `t` (ms since the run began).
     OrderHold.status(rid)                 { running, step, resumable, ... }: a page reloaded in the middle sees the run it left
     OrderHold.pending()                   the runs a reload left unfinished (run(rid) again carries on where they stopped)

   Plan = { rid, label, customer, shipBy (ms, 0 when unknown), canHold, blockedWhy, estimate,
            pieces: [{ poolId, lineKey, label, metal, state: onSheet | onCutSheet | inCommittedSet | notOnSheet | completed,
                       sheetId, sheetLabel, setId, setLabel }],
            sheets: [{ sheetId, label, setLabel, metal, removes, qrRemade }],
            fills:  [{ sheetId, sheetLabel, spots, source: waiting | newerSheet | none, fromSheetId, fromSheetLabel, orders, rids, why? }],
                    (rids: the ids of the orders that fill; the sentences count each order once, whatever number of sheets it fills)
            stays:  [{ poolId, label, sheetLabel, why }],
            effects: [plain sentences, ready to show] }

   Steps (the animation reads these; unknown types are to be ignored):
     start {rid, sheets:[id], resumed?}      sheetBegin {sheetId, label}      lift {sheetId, label, poolIds, rects, sheet}
     removed {sheetId, removed}              fillBegin {sheetId, spots}       fillFrom {toSheetId, fromSheetId, rid, poolIds, source}
     fillPlaced {sheetId, rid, poolIds}      fillSkipped {sheetId, rid?, why} qr {sheetId}
     sheetDone {sheetId, charmCount, density}   held {rid}                    done {}        error {message, rid, sheetId}
   A sheet's own steps come in this order; sheets that share a line of the order (its copies are on both) are lifted together and
   filled one after another. rects are in sheet points ({x,y,w,h} with the centre cx, cy and the angle), `sheet` is { wPt, hPt }.

   Safety (kept from the sheet window): a sheet that is cut, recalled, or whose set was sent is never filled or taken from; a piece
   on a cut sheet stays (plan.stays); a piece inside a committed set blocks Hold ("Undo the set first"); a Rose Gold sheet gives
   its pieces up as before but is never re-arranged or filled; an order alone on a sheet cannot leave it empty (blocked, plainly);
   one change at a time; nothing is deleted; a failed fill never undoes the hold. Sandbox and production keep separate journals. */
(() => {
  "use strict";
  const W = window;
  const kit = () => (W.SheetWin && W.SheetWin.holdKit) || null;
  const sandbox = () => { try { return typeof WORKSPACE_SANDBOX !== "undefined" && !!WORKSPACE_SANDBOX; } catch (_) { return false; } };
  const KEY = () => "cn.orderhold.run" + (sandbox() ? ":sandbox" : "");
  const sheetsAll = () => { try { return typeof allSheets === "function" ? allSheets() : []; } catch (_) { return []; } };
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const plural = (n, one, many) => `${n} ${n === 1 ? one : (many || one + "s")}`;
  const joinAnd = list => list.length < 2 ? list.join("") : list.slice(0, -1).join(", ") + " and " + list[list.length - 1];
  const uniq = list => [...new Set(list.filter(Boolean))];
  const whoNow = () => String((W.B && (B.employee || (B.link && B.link.state && B.link.state() && B.link.state().employee))) || (() => { try { return localStorage.getItem("cn.employee") || ""; } catch (_) { return ""; } })() || "").trim();
  const msOf = t => { t = +t || 0; return t > 0 && t < 1e11 ? t * 1000 : t; };

  /* ── what a person sees: one plain sentence per consequence ── */
  // The orders that fill the freed spots, each counted ONCE by its order id: one order that fills a spot on two sheets (its gold piece
  // moves into one sheet, its silver piece into another) is "1 order", not one for each sheet. A fill that carries no ids counts what it says.
  function moversOf(into) {
    const seen = new Set();
    const count = list => {
      let n = 0;
      for (const f of list) {
        const ids = (Array.isArray(f.rids) ? f.rids : []).map(String).filter(Boolean);
        if (!ids.length) { n += +f.orders || 0; continue; }
        for (const id of ids) if (!seen.has(id)) { seen.add(id); n++; }
      }
      return n;
    };
    const waiting = into.filter(f => f.source === "waiting"), newer = into.filter(f => f.source === "newerSheet");
    const wait = count(waiting), from = count(newer);
    return { wait, from, labels: uniq(newer.map(f => f.fromSheetLabel)) };
  }
  function effectsOf(P) {
    const out = [], on = P.pieces.filter(p => p.state === "onSheet"), loose = P.pieces.filter(p => p.state === "notOnSheet");
    const where = P.sheets.map(s => s.label + (s.setLabel ? ` (${s.setLabel})` : ""));
    if (on.length) out.push(`${plural(on.length, "piece")} of this order ${on.length === 1 ? "comes" : "come"} off ${joinAnd(where)}.`);
    if (loose.length) out.push(`${plural(loose.length, "piece")} ${loose.length === 1 ? "is" : "are"} not on a sheet yet and simply ${loose.length === 1 ? "waits" : "wait"} under On hold.`);
    const into = P.fills.filter(f => f.source !== "none"), free = P.fills.filter(f => f.source === "none");
    if (into.length) {
      const m = moversOf(into), parts = [];
      if (m.wait) parts.push(plural(m.wait, "waiting order"));
      if (m.from) parts.push(`${plural(m.from, "order")} from ${joinAnd(m.labels)}`);
      const spots = into.reduce((n, f) => n + f.spots, 0), orders = m.wait + m.from;
      if (parts.length) out.push(`${P.estimate ? "Up to " : ""}${joinAnd(parts)} ${orders === 1 ? "fills" : "fill"} the ${plural(spots, "empty spot")}.`);
    }
    const left = free.reduce((n, f) => n + f.spots, 0);
    if (left && on.length) {
      const rose = free.some(f => /Rose Gold/.test(f.why || ""));
      out.push(rose && !into.length ? `Rose Gold sheets are never re-arranged, so the room stays free.` : `${plural(left, "empty spot")} ${left === 1 ? "stays" : "stay"} free: no waiting order fits yet. New orders that fit go in as they arrive.`);
    }
    const qr = P.sheets.filter(s => s.qrRemade).length;
    if (qr) out.push(`New QR labels are made on ${plural(qr, "sheet")}.`);
    if (P.stays.length) out.push(`${plural(P.stays.length, "piece")} ${P.stays.length === 1 ? "stays" : "stay"}: ${joinAnd(uniq(P.stays.map(s => `${s.sheetLabel} (${s.why})`)))}.`);
    out.push("The order waits under On hold until someone presses Release hold.", "Nothing is deleted.");
    return out;
  }

  /* ── the plan, from a snapshot of the sorter (pure: the same snapshot gives the same plan) ──
     snapshot = { rid, label, customer, shipBy, missing?, cancelled, held, rowsLeft,
                  pieces: [{ poolId, lineKey, label, metal, status: off | none | cut | together | sent | unloaded | last | completed,
                             why, sheetId, sheetLabel, setId, setLabel, placed }],
                  sheets: { [sheetId]: { label, setLabel, metal, rose, removes, spots, fillable, why } },
                  candidates?: [{ rid, source, fromSheetId, fromSheetLabel, metal, spots }]   (fixtures: stand-ins for the room search) } */
  function planFrom(snap) {
    const P = { rid: String(snap.rid), label: snap.label || "", customer: snap.customer || "", shipBy: msOf(snap.shipBy), canHold: true, blockedWhy: null, estimate: true, pieces: [], sheets: [], fills: [], stays: [], effects: [] };
    const STATE = { off: "onSheet", none: "notOnSheet", cut: "onCutSheet", together: "onCutSheet", sent: "inCommittedSet", unloaded: "onSheet", last: "onSheet", completed: "completed" };
    for (const p of snap.pieces || []) P.pieces.push({ poolId: p.poolId, lineKey: p.lineKey || "", label: p.label || "", metal: p.metal || "", state: STATE[p.status] || "onSheet", sheetId: p.sheetId || null, sheetLabel: p.sheetLabel || "", setId: p.setId || null, setLabel: p.setLabel || "" });
    const by = st => (snap.pieces || []).filter(p => p.status === st), labels = list => joinAnd(uniq(list.map(p => p.sheetLabel)));
    const off = by("off"), none = by("none"), stays = (snap.pieces || []).filter(p => p.status === "cut" || p.status === "together");
    for (const p of stays) P.stays.push({ poolId: p.poolId, label: p.label || "", sheetLabel: p.sheetLabel || "", why: p.why || "that sheet was already cut" });
    // each sheet it comes off, with what happens to it
    const ids = uniq(off.map(p => p.sheetId));
    for (const id of ids) {
      const s = (snap.sheets || {})[id] || {}, mine = off.filter(p => p.sheetId === id);
      P.sheets.push({ sheetId: id, label: s.label || mine[0].sheetLabel || "a sheet", setLabel: s.setLabel || "", metal: s.metal || mine[0].metal || "", removes: mine.length, qrRemade: true });
    }
    const block = why => { P.canHold = false; P.blockedWhy = why; };
    if (snap.missing) block(snap.missing);
    else if (snap.cancelled) block("This order is cancelled. Restore it under Orders > Cancelled first.");
    else if (!snap.rowsLeft) block("This order is not in the sorter any more.");
    else if (snap.held) block("This order is already on hold.");
    else if (by("sent").length) block(`Undo the set first: ${labels(by("sent"))} ${uniq(by("sent").map(p => p.sheetLabel)).length === 1 ? "is" : "are"} in a set that was already sent to the station.`);
    else if (by("unloaded").length) block(`${labels(by("unloaded"))} ${uniq(by("unloaded").map(p => p.sheetLabel)).length === 1 ? "is" : "are"} not open in this sorter, and a piece of this order is there. Reload the sorter and try again.`);
    else if (by("last").length) block(`This order is the only one on ${labels(by("last"))}, and a sheet is never left empty. Let another order fill that sheet first, or delete the sheet from its menu.`);
    else if (!off.length && !none.length && stays.length) block("Every piece of this order is already cut, so there is nothing to take off.");
    // what fills the spots (an estimate here; the room search of the live page replaces it, see plan())
    // (an order is taken once for a sheet of a metal: its gold piece for a gold sheet and its silver piece for a silver sheet are two moves of ONE order, as the run does them)
    const used = new Set(), usedKey = (c, metal) => c.rid + "|" + (c.metal ? metal : ""), cands = (snap.candidates || []).slice();
    for (const s of P.sheets) {
      const info = (snap.sheets || {})[s.sheetId] || {}, spots = info.spots != null ? info.spots : s.removes;
      if (!info.fillable) { P.fills.push({ sheetId: s.sheetId, sheetLabel: s.label, spots, source: "none", fromSheetId: null, fromSheetLabel: "", orders: 0, rids: [], why: info.why || (info.rose ? "Rose Gold sheets are never re-arranged" : "this sheet cannot be filled") }); continue; }
      let left = spots;
      for (const c of cands.filter(c => !used.has(usedKey(c, s.metal)) && (!c.metal || c.metal === s.metal)).sort((a, b) => (a.source === "waiting" ? 0 : 1) - (b.source === "waiting" ? 0 : 1))) {
        if (left <= 0) break;
        const n = Math.min(left, c.spots || 1); used.add(usedKey(c, s.metal)); left -= n;
        const prev = P.fills.find(f => f.sheetId === s.sheetId && f.source === c.source && (f.fromSheetId || null) === (c.fromSheetId || null));
        if (prev) { prev.spots += n; prev.orders++; prev.rids.push(String(c.rid)); } else P.fills.push({ sheetId: s.sheetId, sheetLabel: s.label, spots: n, source: c.source, fromSheetId: c.source === "newerSheet" ? c.fromSheetId || null : null, fromSheetLabel: c.source === "newerSheet" ? c.fromSheetLabel || "" : "", orders: 1, rids: [String(c.rid)] });
      }
      if (left > 0) P.fills.push({ sheetId: s.sheetId, sheetLabel: s.label, spots: left, source: "none", fromSheetId: null, fromSheetLabel: "", orders: 0, rids: [], why: "no waiting order fits" });
    }
    P.effects = P.canHold ? effectsOf(P) : [];
    return P;
  }

  /* ── the snapshot of the live sorter ── */
  function liveSnapshot(rid) {
    const K = kit(), O = W.Orders;
    if (!O || !W.B || !K || !O.rows) return { rid, missing: "The sorter is not ready yet; try again in a moment." };
    const rows = O.rows().filter(r => String(r.order.receiptId) === rid), live = rows.filter(r => r.state !== "gone");
    const order = (rows[0] && rows[0].order) || {}, first = live[0] || rows[0] || null;
    const snap = { rid, label: String(order.orderNumber || rid), customer: String((order.buyer && order.buyer.name) || ""), shipBy: order.shipBy || 0,
      cancelled: !!(W.Cancelled && Cancelled.has(rid)), held: live.length > 0 && live.every(r => r.hold), rowsLeft: live.length, pieces: [], sheets: {} };
    const sku = first ? (first.spec && first.spec.designSku) || (first.line && first.line.sku) || "" : "";
    snap.title = sku;
    const all = sheetsAll(), holderOf = id => all.find(p => p.charms.some(c => c.poolId === id)) || null;
    const rowOf = new Map(); for (const r of rows) (r.poolIds || []).forEach((id, i) => rowOf.set(id, { r, i }));
    const setOf = sh => {
      if (!sh || !sh.setId || sh.draft) return { id: null, label: "" };
      const set = W.Sets && W.B && B.sets && B.sets.values ? [...B.sets.values()].find(z => z.setId === sh.setId) : null, m = /-(\d+)$/.exec(String(sh.setId));
      return { id: sh.setId, label: (set && set.name) || (m ? `Set ${+m[1]}` : "its set") };
    };
    const off = K.offPlan(rid), seen = new Set();
    const piece = (o, status) => {
      if (seen.has(o.id)) return; seen.add(o.id);
      const sh = o.sh || holderOf(o.id), pr = (W.B && B.pool && B.pool.rows.get(o.id)) || null, ro = rowOf.get(o.id), r = ro && ro.r;
      const qty = r && r.spec ? r.spec.quantity || (r.line && r.line.quantity) || 1 : 1, name = o.sku || (r && r.spec && r.spec.designSku) || "a piece";
      const c = sh && sh.charms.find(z => z.poolId === o.id), set = setOf(sh);
      snap.pieces.push({ poolId: o.id, lineKey: (r && r.key) || (c && c.lineKey) || "", label: qty > 1 && ro ? `${name} · ${ro.i + 1} of ${qty}` : name, metal: (sh && sh.metal) || (r && r.material) || (pr && pr.material) || "",
        status, why: o.why || "", sheetId: (sh && sh.sheetId) || (pr && pr.sheetId) || null, sheetLabel: o.where && o.where !== "not on a sheet yet" ? o.where : (sh ? K.word(sh) : ""), setId: set.id, setLabel: set.label,
        placed: !!(c && sh.placements.some(p => p.id === c.id)) });
    };
    for (const o of off.ok) piece(o, o.sh ? "off" : "none");
    for (const o of off.stay) piece(o, o.kind || "cut");
    // each sheet it comes off: what it holds besides this order, and whether the freed room can be filled
    for (const p of snap.pieces.filter(p => p.status === "off")) {
      const sh = all.find(z => z.sheetId === p.sheetId); if (!sh) continue;
      const s = snap.sheets[p.sheetId] || (snap.sheets[p.sheetId] = { label: K.word(sh), setLabel: p.setLabel, metal: sh.metal, rose: sh.metal === "rose", removes: 0, spots: 0, fillable: false, why: "" });
      s.removes++; if (p.placed) s.spots++;
    }
    for (const [id, s] of Object.entries(snap.sheets)) {
      const sh = all.find(z => z.sheetId === id), going = new Set(snap.pieces.filter(p => p.status === "off" && p.sheetId === id).map(p => p.poolId));
      const stays = sh.placements.filter(p => { const c = sh.charms.find(z => z.id === p.id); return c && !going.has(c.poolId); }).length;
      s.fillable = stays > 0 && K.fillable(sh);
      if (!s.fillable) s.why = sh.metal === "rose" ? "Rose Gold sheets are never re-arranged" : (sh.rosePlan || sh.roseProtected) ? "a sheet behind a green line is never re-arranged" : stays ? "this sheet cannot be filled" : "nothing else is on it";
    }
    return snap;
  }

  /* ── plan(rid, opts) ── */
  async function plan(rid, opts) {
    rid = String(rid); opts = opts || {};
    const snap = opts.snapshot || liveSnapshot(rid), P = planFrom(snap);
    if (!P.label) P.label = rid;
    if (opts.snapshot || opts.exact === false || !P.canHold || !P.sheets.length) return P;
    // the real room: the nest's own collision grid on each sheet as it will stand once the order is off (read only), in
    // the order the run takes them, within a few seconds (the rest stays an estimate). An order is not promised twice to sheets of one
    // metal (it has moved by then); the run only skips the held order itself, so an order with a gold piece on a newer gold sheet and a
    // silver piece on a newer silver sheet fills a spot on each: the plan says the same, and counts that order once (see moversOf)
    const K = kit(); if (!K || !K.room) return P;
    const t0 = Date.now(), budget = +opts.budgetMs || 4000, fills = [], taken = [], sheets = P.sheets.map(s => s.sheetId);
    let exact = true;
    try {
      for (const s of P.sheets) {
        const sh = sheetsAll().find(z => z.sheetId === s.sheetId), info = snap.sheets[s.sheetId] || {};
        if (!sh || !info.fillable) { fills.push(...P.fills.filter(f => f.sheetId === s.sheetId)); continue; }
        const without = new Set(snap.pieces.filter(p => p.status === "off" && p.sheetId === s.sheetId).map(p => p.poolId));
        const left = budget - (Date.now() - t0);
        if (left < 400) { exact = false; fills.push(...P.fills.filter(f => f.sheetId === s.sheetId)); continue; }
        const r = await K.room(sh, { without, skip: [rid, ...taken.filter(t => t.metal === s.metal).map(t => t.rid)], avoid: sheets.filter(id => id !== s.sheetId), budgetMs: left });
        const spots = info.spots != null ? info.spots : s.removes;
        const bySource = new Map();
        for (const it of r.list) {
          taken.push({ rid: it.k.rid, metal: s.metal });
          const from = it.source === "newerSheet" ? [...it.k.srcs][0] : null, key = it.source + "|" + (from ? from.sheetId : ""), n = new Set(it.spots.map(x => x.bi)).size || it.spots.length;
          const f = bySource.get(key) || { sheetId: s.sheetId, sheetLabel: s.label, spots: 0, source: it.source, fromSheetId: from ? from.sheetId || null : null, fromSheetLabel: from ? K.word(from) : "", orders: 0, rids: [] };
          f.spots += n; f.orders++; f.rids.push(String(it.k.rid)); bySource.set(key, f);
        }
        const filled = [...bySource.values()]; fills.push(...filled);
        const rest = Math.max(0, spots - filled.reduce((n, f) => n + f.spots, 0));
        if (rest) fills.push({ sheetId: s.sheetId, sheetLabel: s.label, spots: rest, source: "none", fromSheetId: null, fromSheetLabel: "", orders: 0, rids: [], why: "no waiting order fits" });
      }
      P.fills = fills; P.estimate = !exact; P.effects = effectsOf(P);
    } catch (e) { console.warn("[OrderHold] room search", e); }
    return P;
  }

  /* ── the run's journal: written before anything moves and at each step, so a reload sees where it stopped ── */
  const journal = {
    all() { try { const j = JSON.parse(localStorage.getItem(KEY()) || "{}"); return j && typeof j === "object" && !Array.isArray(j) ? j : {}; } catch (_) { return {}; } },
    set(rid, patch) { try { const j = journal.all(); j[rid] = Object.assign(j[rid] || {}, patch); localStorage.setItem(KEY(), JSON.stringify(j)); } catch (_) { /* a private window: the run still goes */ } },
    drop(rid) { try { const j = journal.all(); delete j[rid]; localStorage.setItem(KEY(), JSON.stringify(j)); } catch (_) {} },
  };
  const active = new Map();   // rid → { who, at, step, steps, sheetId }

  function status(rid) {
    rid = String(rid);
    const a = active.get(rid);
    if (a) return { running: true, step: a.step ? Object.assign({}, a.step) : null, rid, sheetId: a.sheetId || null, name: a.who, startedAt: a.at, stepsDone: a.steps.length, resumable: false };
    const j = journal.all()[rid];
    if (j) return { running: false, step: j.step || null, rid, sheetId: j.sheetId || null, name: j.who || "", startedAt: j.at || 0, stepsDone: j.n || 0, resumable: true };
    return { running: false, step: null, rid, sheetId: null, name: "", startedAt: 0, stepsDone: 0, resumable: false };
  }
  const pending = () => Object.entries(journal.all()).filter(([rid]) => !active.has(rid)).map(([rid, j]) => ({ rid, name: j.who || "", startedAt: j.at || 0, step: j.step || null, sheetId: j.sheetId || null }));

  /* ── is what this page shows still what the Library holds? ──
     Another page, or another computer, may have held, cancelled or moved this order, or changed a sheet it is on, since this page read them.
     A hold made from the old picture would write it back over the newer one (held pieces on a saved sheet again, a cancelled order held).
     So before anything changes, the order and the sheets it comes off are read from the Library (reads only) and compared; a difference ends
     the run plainly with nothing changed. A sheet this page is still saving, or has not saved, is not compared (its own save is the newer). */
  const libApi = () => (W.CN && W.CN.api) || W.api || null;
  const placedIdsOf = sh => (sh.placements || []).map(p => { const c = (sh.charms || []).find(z => z.id === p.id); return c && c.poolId; }).filter(Boolean);
  async function sheetDrift(sh) {
    const api = libApi();
    if (!api || !sh || !sh.sheetId || sh.dirty || sh.problem || !sh.persistedDone || ["nesting", "finishing", "queued"].includes(sh.status)) return null;
    let rec = null;
    try { rec = ((await api("charmNestLibrary", { op: "getSheet", id: sh.sheetId }, { quiet: true, timeoutMs: 12000 })) || {}).sheet; } catch (_) { return null; }   // (not readable now: the run's own read-back checks what it wrote)
    if (!rec) return null;
    const saved = new Set([...(rec.poolIds || []), ...(rec.charms || []).map(c => c && c.poolId)].filter(Boolean)), mine = new Set((sh.charms || []).map(c => c.poolId).filter(Boolean));
    const lacking = placedIdsOf(sh).filter(id => !saved.has(id)), extra = (rec.poolIds || []).filter(id => !mine.has(id));
    return lacking.length || extra.length ? `${kit().word(sh)} was changed on another page or computer after this page read it` : null;
  }
  async function drift(rid, off) {
    try { if (W.Cancelled && typeof W.Cancelled.load === "function") await W.Cancelled.load(true); } catch (_) { /* the list as it was */ }
    if (W.Cancelled && typeof W.Cancelled.has === "function" && W.Cancelled.has(rid)) return "This order was cancelled on another page or computer. Restore it under Orders > Cancelled first.";
    const api = libApi(), ids = off.ok.map(o => o.id).slice(0, 300);
    if (api && ids.length) {
      let r = null; try { r = await api("charmNestLibrary", { op: "poolGet", poolIds: ids }, { quiet: true, timeoutMs: 12000 }); } catch (_) { r = null; }
      if (r && r.pools && ids.some(id => r.pools[id] && ["abandoned", "superseded"].includes(r.pools[id].state))) return "This order was put on hold, cancelled or changed on another page or computer after this page read it. Nothing was changed here: reload this page and look again.";
    }
    for (const sh of uniq(off.ok.map(o => o.sh))) { const why = await sheetDrift(sh); if (why) return `${why}. Nothing was changed here: close the other page, reload this one and try again.`; }
    return null;
  }

  /* ── run(rid, { name, note, onStep }) ── */
  async function run(rid, opts) {
    rid = String(rid); opts = opts || {};
    const t0 = Date.now(), steps = [], K = kit();
    let resumed = false, beatTimer = null;
    // (while this page works, the journal says so every few seconds: another page of this computer that finds it silent for a while takes the run up)
    const stopBeat = () => { if (beatTimer) { clearInterval(beatTimer); beatTimer = null; } };
    const startBeat = () => { if (typeof setInterval !== "function") return; journal.set(rid, { alive: Date.now() }); beatTimer = setInterval(() => journal.set(rid, { alive: Date.now() }), 4000); };
    const A = { who: "", at: t0, step: null, steps, sheetId: null, begun: false, progress: false };
    const emit = s => {
      const now = Date.now(); s.at = now; s.t = now - t0; steps.push(s); A.step = s; if (s.sheetId) A.sheetId = s.sheetId;
      if (A.begun) journal.set(rid, { step: s, sheetId: A.sheetId, n: steps.length, who: A.who, at: t0 });
      if (typeof opts.onStep === "function") { try { opts.onStep(s); } catch (e) { console.warn("[OrderHold] onStep", e); } }
      return s;
    };
    const fail = (message, sheetId) => { stopBeat(); emit({ type: "error", message, rid, sheetId: sheetId || null }); if (A.begun && !A.progress && !resumed) journal.drop(rid); return { ok: false, held: false, steps, error: message }; };
    if (!rid || rid === "undefined") return fail("No order was given.");
    if (!K || !W.Orders || !W.Pool || !W.B) return fail("The sorter is not ready yet; try again in a moment.");
    if (active.size) return fail(`One change at a time: the hold of order ${[...active.keys()][0]} is still running.`);
    if (K.busy()) return fail(`One change at a time: ${K.flow().charAt(0).toLowerCase() + K.flow().slice(1)} is still running.`);
    // a Release of this order still putting it back on a sheet is let finish first
    if (W.OrderHold && typeof W.OrderHold.releaseStatus === "function" && (W.OrderHold.releaseStatus(rid) || {}).running) return fail(`Order ${rid} is being released right now. Try again when that has finished.`);
    const before = journal.all()[rid];
    const who = String(opts.name || (before && before.who) || whoNow()).trim(), note = String(opts.note || (before && before.note) || "").trim().slice(0, 200);
    if (!who) return fail("A name is needed for the record.");
    resumed = !!before; A.who = who; active.set(rid, A);
    try {
      const P = await plan(rid, { exact: false });
      // a run a reload cut short: the sheets it had lifted pieces from still wait for their fill and label
      const lifted = (before && before.lifted || []).filter(id => !(before.done || []).includes(id));
      if (!P.canHold && !(resumed && P.blockedWhy === "This order is already on hold." && lifted.length)) return fail(P.blockedWhy || "This order cannot be put on hold.");
      if (P.canHold) { const why = await drift(rid, K.offPlan(rid)); if (why) return fail(why); }
      // (a release a reload cut short, its lines still carrying `releasing`, is given up now: the person's hold is the newer decision,
      //  and the page's own check for unfinished releases must not lift it again)
      for (const r of W.Orders.rows()) if (String(r.order.receiptId) === rid && r.releasing) delete r.releasing;
      journal.set(rid, { rid, who, note, at: t0, lifted: (before && before.lifted) || [], done: (before && before.done) || [], names: (before && before.names) || [] });
      A.begun = true; startBeat();
      // lines already on hold keep their own reason; but lines this same hold took off before a reload are part of this hold, and get its one reason
      const earlier = h => resumed && ((before && before.lifted) || []).length > 0 && /^Taken off /.test(h) && h.endsWith(` by ${who}${note ? ": " + note : ""}`);
      const heldBefore = new Set(W.Orders.rows().filter(r => String(r.order.receiptId) === rid && r.hold && !earlier(String(r.hold))).map(r => r.key));
      const off = P.canHold ? K.offPlan(rid) : { ok: [], stay: [], ids: new Set() };
      const sheetOrder = sheetsAll(), pos = sh => sheetOrder.indexOf(sh);
      // sheets that share a line of the order (its copies are on both) come off together: the window's own rule is that a line leaves whole
      const parent = new Map(), find = x => { while (parent.get(x) !== x) { parent.set(x, parent.get(parent.get(x))); x = parent.get(x); } return x; };
      const sheetPieces = off.ok.filter(o => o.sh);
      for (const o of sheetPieces) if (!parent.has(o.sh)) parent.set(o.sh, o.sh);
      for (const r of W.Orders.rows()) {
        if (String(r.order.receiptId) !== rid) continue;
        const held = uniq(sheetPieces.filter(o => (r.poolIds || []).includes(o.id)).map(o => o.sh));
        for (let i = 1; i < held.length; i++) parent.set(find(held[i]), find(held[0]));
      }
      const clusters = [];
      for (const o of sheetPieces) { const k = find(o.sh); let c = clusters.find(z => z.key === k); if (!c) clusters.push(c = { key: k, ok: [], sheets: [] }); c.ok.push(o); if (!c.sheets.includes(o.sh)) c.sheets.push(o.sh); }
      for (const c of clusters) c.sheets.sort((a, b) => pos(a) - pos(b));
      clusters.sort((a, b) => pos(a.sheets[0]) - pos(b.sheets[0]));
      const loose = off.ok.filter(o => !o.sh);
      if (loose.length) { if (clusters.length) clusters[clusters.length - 1].ok.push(...loose); else clusters.push({ key: null, ok: loose, sheets: [] }); }
      const todoSheets = clusters.flatMap(c => c.sheets), todoIds = new Set(todoSheets.map(sh => sh.sheetId)), pendingIds = new Set(todoIds);
      const doneNames = [...(before && before.names || [])];
      emit(Object.assign({ type: "start", rid, sheets: uniq([...lifted, ...todoSheets.map(sh => sh.sheetId)]) }, resumed ? { resumed: true } : {}));

      const wordOf = sh => K.word(sh);
      const rectsOf = (sh, ids) => {
        const out = [];
        for (const c of sh.charms) {
          if (!ids.has(c.poolId)) continue;
          const p = sh.placements.find(q => q.id === c.id); if (!p) continue;
          const w = p.wPt || c.widthPt || 20, h = p.hPt || c.heightPt || 20;
          out.push({ x: p.cxPt - w / 2, y: p.cyPt - h / 2, w, h, cx: p.cxPt, cy: p.cyPt, angle: p.angle || 0, poolId: c.poolId });
        }
        return out;
      };
      const sizeOf = sh => { try { const s = typeof stockFor === "function" ? stockFor(sh.metal, sh) : null; return s ? { wPt: s.wPt, hPt: s.hPt } : null; } catch (_) { return null; } };
      const idle = async () => {   // a change of the window's, or new orders going on, finishes first (a real wait, polled; never a pause for show)
        const until = Date.now() + 90000;
        for (;;) { if (!K.busy() && !(W.B.run && B.run.arrivalBusy)) return true; if (Date.now() > until) return false; await sleep(200); }
      };
      // one sheet after its pieces are off: its freed room is filled, its QR label is made again, and it is read back
      const finish = async (sh, removed, spots) => {
        const id = sh.sheetId;
        if (removed) emit({ type: "removed", sheetId: id, removed });
        const freed = spots != null ? spots : K.freed(id);
        if (freed) emit({ type: "fillBegin", sheetId: id, spots: freed });
        if (!K.fillable(sh)) { if (freed) emit({ type: "fillSkipped", sheetId: id, why: sh.metal === "rose" ? "Rose Gold sheets are never re-arranged" : (sh.rosePlan || sh.roseProtected) ? "a sheet behind a green line is never re-arranged" : "this sheet cannot be filled" }); }
        else if (freed) {
          const r = await K.fill(sh, who, {
            skip: [rid], avoid: [...pendingIds],
            // (an order is not moved from a sheet another page changed meanwhile: its picture here is old; the spot stays free)
            from: async it => { for (const src of it.k.srcs) { const why = await sheetDrift(src); if (why) throw new Error(`${why}, so order ${it.k.rid} was not moved`); } emit({ type: "fillFrom", toSheetId: id, fromSheetId: [...it.k.srcs][0] ? [...it.k.srcs][0].sheetId || null : null, rid: it.k.rid, poolIds: uniq(it.spots.map(s => s.c.poolId)), source: it.source }); },
            placed: it => emit({ type: "fillPlaced", sheetId: id, rid: it.k.rid, poolIds: uniq(it.spots.map(s => s.c.poolId)) }),
            skipped: (it, e) => emit({ type: "fillSkipped", sheetId: id, rid: it.k.rid, why: String((e && e.message) || e || "it did not fit") }),
          });
          if (!r.planned) emit({ type: "fillSkipped", sheetId: id, why: "no waiting order fits yet" });
        }
        emit({ type: "qr", sheetId: id });
        emit({ type: "sheetDone", sheetId: id, charmCount: sh.placements.length, density: sh.density || 0 });
        const j = journal.all()[rid] || {}; journal.set(rid, { done: uniq([...(j.done || []), id]) });
      };

      // 0 · sheets a reload cut short: their pieces are off already, the fill and label are still to do
      for (const id of lifted) {
        const sh = sheetsAll().find(z => z.sheetId === id); if (!sh || todoIds.has(id)) continue;
        if (!(await idle())) return fail("Another change is still running; the hold carries on when you press it again.", id);
        emit({ type: "sheetBegin", sheetId: id, label: wordOf(sh) });
        // (its pieces are off in memory, but a save that failed, or a reload that cut it short, left the saved sheet as it was: it is written again first,
        //  so the Library never keeps listing a piece that is on hold)
        if (typeof K.rewrite === "function" && (sh.problem || sh.dirty || !sh.persistedDone)) {
          try { await K.rewrite(sh); }
          catch (e) { return fail(`${wordOf(sh)} could not be saved again: ${String((e && e.message) || e).replace(/^.*?: /, "")}. Nothing more was taken off; press Hold again to finish.`, id); }
        }
        await finish(sh, null);
      }
      // 1 · each cluster of sheets: the pieces lift, come off (verified, labels made again), and the room is filled
      for (const cl of clusters) {
        if (!(await idle())) return fail("Another change is still running; nothing more was taken off. Press Hold again in a moment.");
        const ids = new Set(cl.ok.map(o => o.id));
        for (const sh of cl.sheets) {
          emit({ type: "sheetBegin", sheetId: sh.sheetId, label: wordOf(sh) });
          const mine = new Set(cl.ok.filter(o => o.sh === sh).map(o => o.id));
          emit({ type: "lift", sheetId: sh.sheetId, label: wordOf(sh), poolIds: [...mine], rects: rectsOf(sh, mine), sheet: sizeOf(sh) });
        }
        const counts = new Map(cl.sheets.map(sh => [sh, { n: cl.ok.filter(o => o.sh === sh).length, spots: rectsOf(sh, new Set(cl.ok.filter(o => o.sh === sh).map(o => o.id))).length }]));
        const prevLifted = (journal.all()[rid] || {}).lifted || [];
        journal.set(rid, { lifted: uniq([...prevLifted, ...cl.sheets.map(sh => sh.sheetId)]) });
        try { await K.takeOff({ rid, whole: true, ids, ok: cl.ok, stay: off.stay }, who, note); }
        catch (e) {
          // (nothing came off: the sheets are not lifted, and a run that changed nothing leaves no journal)
          let moved = false; try { const now = new Set(K.offPlan(rid).ok.map(o => o.id)); moved = cl.ok.some(o => !now.has(o.id)); } catch (_) { moved = true; }
          if (moved) A.progress = true; else journal.set(rid, { lifted: prevLifted });
          return fail(String((e && e.message) || e), cl.sheets[0] && cl.sheets[0].sheetId);
        }
        A.progress = true;
        for (const sh of cl.sheets) { doneNames.push(wordOf(sh)); pendingIds.delete(sh.sheetId); }
        journal.set(rid, { names: doneNames });
        for (const sh of cl.sheets) { const c = counts.get(sh); await finish(sh, c.n, c.spots); }
      }
      // 2 · the order's lines wait under On hold with one plain reason, whichever sheets it came off
      const text = doneNames.length ? `Taken off ${uniq(doneNames).join(", ")} by ${who}${note ? ": " + note : ""}` : `Put on hold by ${who}${note ? ": " + note : ""}`;
      let marked = 0;
      for (const r of W.Orders.rows()) {
        if (String(r.order.receiptId) !== rid || r.state === "gone") continue;
        if (r.hold) { if (!heldBefore.has(r.key)) r.hold = r.reason = text; marked++; continue; }
        if (!(r.poolIds || []).length) { r.state = "held"; r.hold = r.reason = text; r.heldAt = Date.now(); r.holdSeen = false; if (W.CNListActivity && CNListActivity.touch) CNListActivity.touch(r, r.heldAt); marked++; }
      }
      if (!clusters.length && marked && W.SheetEvents && SheetEvents.order) SheetEvents.order({ type: "held", orderId: rid, id: `oh-${Date.now()}`, by: who, text: `${text}, on hold`.slice(0, 200), data: { pieces: 0, note: note || undefined } });
      if (B.run && W.RunCtl && RunCtl.save) { B.run.lines = Object.fromEntries(W.Orders.rows().map(W.Orders.lineRecord)); await RunCtl.save(B.run).catch(e => console.warn("[OrderHold] run lines", e)); }
      if (W.Review && Review.syncOrderItems) Review.syncOrderItems();
      if (W.Orders.render) W.Orders.render();
      if (W.RunCtl && RunCtl.poke) RunCtl.poke();
      emit({ type: "held", rid });
      emit({ type: "done" });
      stopBeat(); journal.drop(rid);
      return { ok: true, held: true, steps };
    } catch (e) {
      return fail(String((e && e.message) || e));
    } finally { stopBeat(); active.delete(rid); }
  }

  /* ── a hold a reload cut short is finished by the page itself (as an unfinished release is) ──
     The journal says whose hold it was and which sheets it had lifted pieces from; those sheets still wait for their save, fill and
     label, and the order already reads "On hold", so there is no Hold button to press again. Only a hold from before this page was
     loaded is taken up, only once its sheets are back, only when no other page of this computer is still at it (its journal is silent),
     and each order at most 3 times, 30 s apart. */
  const LOADED = Date.now(), TRIED = new Map();
  async function resumeHolds() {
    const out = [];
    if (!W.Orders || typeof W.Orders.rows !== "function") return out;
    for (const [rid, j] of Object.entries(journal.all())) {
      if (!j || active.has(rid) || !((j.at || 0) < LOADED) || !(j.lifted || []).length) continue;
      if (Date.now() - (j.alive || (j.step && j.step.at) || j.at || 0) < 20000) continue;
      if (!W.Orders.rows().some(r => r && r.order && String(r.order.receiptId) === rid)) continue;
      const have = sheetsAll(); if (!j.lifted.every(id => have.some(z => z.sheetId === id))) continue;
      const t = TRIED.get(rid) || { n: 0, at: 0 };
      if (t.n >= 3 || Date.now() - t.at < 30000) continue;
      TRIED.set(rid, { n: t.n + 1, at: Date.now() });
      out.push(await run(rid, { name: j.who }));
    }
    return out;
  }
  if (typeof setInterval === "function" && W.document) {
    const scan = () => { try { if (W.Session && W.Session.ready && !W.Session.ready()) return; resumeHolds().catch(() => {}); } catch (_) { /* tried again */ } };
    setInterval(scan, 5000);
  }

  W.OrderHold = Object.assign(W.OrderHold || {}, { plan, run, status, pending, planFrom, effectsOf, resumeHolds, _liveSnapshot: liveSnapshot });
})();
