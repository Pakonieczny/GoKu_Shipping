/*  netlify/functions/_charmNestSetOverride.js — OVERRIDE (Paul, 23 Nov 2026: "I need the ability to overwrite and approve for laser cutting any given set even though it may contain sheets that are
 *  not ready or have orders that are held. This is only allowed with my password. When I choose to overwrite and release a given Set of sheets to Laser cutting it should automatically release ALL
 *  Holds on ALL orders contained within the Set of sheets.")
 *
 *    setOverrideGet { setId }        (read)   what an override of this set sets aside, in a few lines: { engravings: { n, orders }, sheets: [{ id, label, why }], waits: [order], holds: [{ orderId, why }] }
 *    setOverride { setId, by, dev, repoToken }  (a press, behind the owner's password: _charmNestRepoAuth.js SETTINGS_OPS, the Settings unlock, checked before this runs)
 *      ONE transaction, all of it or nothing:
 *        · the set and every sheet of it are approved for laser cutting as an approval is: the laserReady seal by the person, processReady, a sheet's own hold lifted, and laserOverride { at, by, setId }:
 *          who set what aside (charm-nest-readiness.js reads it as it reads a sheet cut once before: the sheet is ready, its set is exempt from the set rules, "Mark set completed" takes it). A Gold or
 *          Silver sheet that was filling again is closed (releaseFull), as Make QR label closes one: nothing more is put on it. A sheet already cut is left as it is.
 *        · EVERY hold of EVERY order with a piece on those sheets is released, as Release Hold releases one: the Review mark (Hold Order) is done, the run's lines (a person's hold, an Etsy change waiting,
 *          a line a question holds) are free, the pool rows a hold took off are live again, the release is kept on the order (its RELEASE HOLD seal) and in the release journal. A piece already on one of
 *          these sheets stays there; a piece on no sheet goes to the front of the queue (Nest). Nothing is asked.
 *      Then each order's timeline says the override, and its release; a sheet with no QR label is asked for one (labelsAsk). An engraving not approved stays not approved on its order: its back is not cut.
 *  No Etsy call, no nest, nothing moved on a sheet. */
"use strict";
const { heldRow, JOBS } = require("./_charmNestHoldRelease");
const RUNS = "Charm_Nest_Runs", RUN_LIVE = "Charm_Nest_Run_Live", POOL = "Charm_Pool", OPEN = "Charm_Nest_Open", CUSTOM = "Charm_Custom_Orders", CANCELLED = "Charm_Nest_Cancelled";
const num = v => (Number.isFinite(+v) ? +v : 0);
const closedRun = r => !r || ["complete", "abandoned", "archived"].includes(String(r.status || "")) || !!r.retiredInto;
const lineOfPool = id => String(id == null ? "" : id).replace(/_\d{1,3}$/, "");
const orderOf = (k, l) => String((l && l.orderId) || String(k).split("_")[0]);
const HELD_WHY = "The order changed on Etsy";

/** d: the library's own helpers (its sandbox prefix, its feed, its timeline): { db, FV, col, prefix, str, isId, Readiness, SetRules, SETS, SHEETS, FIELDS, sheetLabel, processHistory, processEvent, processRecord, readinessRecords,
 *  withPlacements, liveParts, withLiveLines, stamp, revTouch, feedOrders, fenceTx, ops } */
module.exports = function (d) {
  const { db, FV, col, str, isId, Readiness, SETS, SHEETS } = d;
  const jobs = () => db.collection(d.prefix() + JOBS);
  const idsOfSet = s => [...new Set((s.sheetIds || []).map(String))].filter(isId).slice(0, 60);

  /** The set and its sheets as they stand: { set, sheets } or { error }. */
  async function setOf(setId) {
    if (!isId(setId)) return { error: "bad set id", status: 400 };
    const s = await col(SETS).doc(setId).get(); if (!s.exists) return { error: "There is no such set", status: 404 };
    const set = Object.assign({}, s.data(), { setId }), ids = idsOfSet(set);
    if (!ids.length) return { error: "This set has no sheets", status: 409 };
    if (num(set.laserDoneAt) > 0) return { error: "This set is already marked completed", status: 409 };
    if (/^superseded/.test(String(set.status || ""))) return { error: "This set was replaced by another", status: 409 };
    const docs = await db.getAll(...ids.map(id => col(SHEETS).doc(id)), { fieldMask: d.FIELDS }), gone = docs.filter(x => !x.exists || x.data().archived || String(x.data().setId || "") !== setId);
    if (gone.length) return { error: `A sheet of this set is no longer in it (${gone.map(x => (x.exists ? d.sheetLabel(x.data()) : x.id)).join(", ")}): open the Library again, then press Override`, status: 409 };
    return { set, sheets: docs.map(x => Object.assign({}, x.data(), { id: x.id })) };
  }
  const ordersOf = sheets => [...new Set(sheets.flatMap(s => Readiness.orderIds(s)).map(String))].filter(o => /^\d{1,30}$/.test(o));
  /** The runs that hold the set's lines: its own, and the one that is open. */
  async function runIdsOf(set) {
    const open = await col(OPEN).doc("run").get(), named = open.exists ? str((open.data() || {}).runId, 80) : "";
    return [...new Set([str(set.runId, 80), named])].filter(isId);
  }
  /** What a line's hold says, or "" (a line that left the order holds nothing). */
  function holdOf(l, rowHeld) {
    if (!l || ["gone", "skipped", "noDesign"].includes(String(l.state || ""))) return "";
    if (l.hold) return str(l.hold, 160);
    if (l.changePending) return HELD_WHY;
    if (rowHeld) return "On hold";
    return String(l.state || "") === "held" && Array.isArray(l.problems) && l.problems.length ? str(l.reason, 160) || "On hold" : "";   // (a custom listing that waits for its own designs is on no hold: nothing to release)
  }
  /** Every hold of these orders: Map(order → { why, review }) and the pool rows a hold took off their sheet. */
  async function holdsOf(set, orders) {
    const own = new Set(orders), out = new Map(), rows = [], add = (o, why, x) => { const h = out.get(o) || { why, review: false }; out.set(o, Object.assign(h, x || {})); };
    const values = orders.flatMap(o => [o].concat(Number.isSafeInteger(+o) ? [+o] : []));
    const parts = []; for (let i = 0; i < values.length; i += 30) parts.push(values.slice(i, i + 30));
    const [pools, marks, runs] = await Promise.all([
      Promise.all(parts.map(v => col(POOL).where("orderId", "in", v).limit(1000).get())),
      (async () => { const list = []; for (let i = 0; i < orders.length; i += 100) list.push(...await db.getAll(...orders.slice(i, i + 100).map(o => col(CUSTOM).doc(o + "_review")), { fieldMask: ["review", "receiptId"] })); return list; })(),
      (async () => { const list = []; for (const id of await runIdsOf(set)) { const s = await col(RUNS).doc(id).get(); if (s.exists && !closedRun(s.data())) list.push({ id, run: await d.withLiveLines(id, s.data()) }); } return list; })()]);
    for (const q of pools) for (const x of q.docs) if (heldRow(x.data())) rows.push(x.id);
    const rowLines = new Set(rows.map(lineOfPool));
    for (const s of marks) { const v = s.exists ? (s.data() || {}).review : null; if (v && v.state === "open") add(s.id.replace(/_review$/, ""), "Order held" + (v.markedBy ? " by " + str(v.markedBy, 80) : ""), { review: true }); }
    for (const { run } of runs) for (const [k, l] of Object.entries((run && run.lines) || {})) { const o = orderOf(k, l); if (!own.has(o)) continue; const why = holdOf(l, rowLines.has(k)); if (why) add(o, why); }
    for (const id of rows) { const o = String(id).split("_")[0]; if (own.has(o) && !out.has(o)) add(o, "On hold"); }
    // (a cancelled order is on no hold: its pieces come off by its cancel, never back onto a sheet by this)
    const asked = [...out.keys()], gone = asked.length ? new Set((await db.getAll(...asked.map(o => col(CANCELLED).doc(o)), { fieldMask: ["at"] })).filter(x => x.exists).map(x => x.id)) : new Set();
    for (const o of gone) out.delete(o);
    return { holds: out, rows: rows.filter(id => !gone.has(String(id).split("_")[0])), runIds: runs.map(r => r.id), gone };
  }
  /** What an override of the set sets aside, in a few plain lines. */
  async function look(setId) {
    const got = await setOf(setId); if (got.error) return got;
    await d.withPlacements(got.sheets.map(r => [r.id, r]));
    const recs = await d.readinessRecords(got.sheets), orders = ordersOf(recs), waiting = [], sheets = [], waits = new Set();
    const { holds, rows, runIds, gone } = await holdsOf(got.set, orders);
    for (const r of recs) {
      if (num(r.laserDoneAt) > 0) continue;
      const L = Readiness.laserSheet(r), st = L.stages, why = [];
      waiting.push(...Readiness.waitingPieces(r).map(id => ({ id, sheet: r.id })));
      if (d.SetRules.recordPartial(r)) why.push("not full");
      if (Readiness.held(r)) why.push("held back");
      if (!st.layout) why.push(Readiness.lineMissing(r) ? "its green line is not cut" : "its layout is not checked");
      if (!st.front) why.push("its cutting files are missing");
      if (!L.waiting && !st.backs) why.push("an engraving file is not saved yet");
      if (!st.qr) why.push("no QR label yet");
      if (why.length) sheets.push({ id: r.id, label: d.sheetLabel(r), why: why.join(", ") });
      for (const b of Readiness.orderBlockers(r)) if (!holds.has(String(b.id))) waits.add(String(b.id));
    }
    return { ok: true, setId, name: got.set.name || "", day: got.set.day || "", approved: Readiness.overridden(got.set), engravings: { n: waiting.length, orders: [...new Set(waiting.map(x => String(x.id).split("_")[0]))], pieces: waiting.map(x => x.id).slice(0, 300) },
      sheets, waits: [...waits], holds: [...holds].map(([orderId, h]) => ({ orderId, why: h.why })), orders: orders.length, _: { orders, rows, runIds, gone } };
  }
  async function setOverrideGet(b) { const r = await look(str(b.setId, 80)); if (!r.error) delete r._; return r; }

  /** A run line of a released order, free of every hold (as Release Hold leaves it: _charmNestHoldRelease.js releaseLines). */
  function freed(k, cur, o) {
    const next = Object.assign({}, cur), ids = Array.isArray(cur.poolIds) ? cur.poolIds : [], was = String(cur.state || "");
    next.hold = null; next.heldAt = null; next.holdSeen = false;
    if (cur.changePending) { next.changePending = false; next.repoolChanged = false; }   // (its piece stays as it lies on the sheet)
    if (was === "held") { next.state = !ids.length ? "pulled" : ids.every(id => o.onSheets.has(String(id))) ? "written" : "pooled"; next.reason = null; }
    else if (cur.reason != null && cur.hold != null && String(cur.reason) === String(cur.hold)) next.reason = null;
    // (a question a held line still asks is set aside, as Release Hold sets it aside: ReviewRelease.setAside)
    const kinds = was === "held" ? [...new Set((Array.isArray(cur.problems) ? cur.problems : []).map(p => str(p && typeof p === "object" ? p.kind : p, 24)).filter(Boolean))] : [];
    if (kinds.length) next.releaseAnyway = { at: o.at, by: o.by, kinds: kinds.slice(0, 8) };
    next.frontAt = o.at; next.releasedAt = o.at; next.releasedBy = o.by; next.releaseHold = { at: o.at, by: o.by, id: `releaseHold:${orderOf(k, cur)}:${o.at}` };
    delete next.releasing;
    next._lp = { at: o.at, dev: "cloud", by: o.by };
    return next;
  }

  async function setOverride(b) {
    const setId = str(b.setId, 80), by = str(b.by, 80).trim(), dev = str(b.dev, 60);
    if (!by) return { error: "A name is needed for the record: sign in, then press Override again", status: 400 };
    const L = await look(setId); if (L.error) return L;
    const { orders, rows, runIds, gone } = L._, at = Date.now();
    const mark = { id: `override-${setId}-${at}`, at, by, setId };
    const said = { engravings: L.engravings.n, pieces: L.engravings.pieces, sheets: L.sheets.map(x => `${x.label}: ${x.why}`).slice(0, 60), holds: L.holds.map(x => x.orderId).slice(0, 400), waits: L.waits.slice(0, 400) };
    const res = await db.runTransaction(async tx => {
      if (d.fenceTx) await d.fenceTx(tx);
      /* ── every read first ── */
      const sref = col(SETS).doc(setId), ss = await tx.get(sref); if (!ss.exists) return { error: "There is no such set", status: 404 };
      const set = ss.data(), ids = idsOfSet(set);
      if (num(set.laserDoneAt) > 0) return { error: "This set is already marked completed", status: 409 };
      if (!ids.length) return { error: "This set has no sheets", status: 409 };
      const docs = await tx.getAll(...ids.map(id => col(SHEETS).doc(id)), { fieldMask: d.FIELDS });
      if (docs.some(x => !x.exists || x.data().archived || String(x.data().setId || "") !== setId)) return { error: "The set's sheets changed just now: press Override again", status: 409 };
      const history = new Map([["set", await d.processHistory("set", setId, set)]]); for (const x of docs) history.set(x.id, await d.processHistory("sheet", x.id, x.data()));
      const runs = [];
      for (const id of runIds) {
        const ref = col(RUNS).doc(id), ex = await tx.get(ref); if (!ex.exists || closedRun(ex.data())) continue;
        const old = ex.data() || {}, had = old.liveLines && Array.isArray(old.liveLines.ids) ? old.liveLines.ids : []; let lines = {};
        if (had.length) { const parts = await Promise.all(had.map(x => tx.get(col(RUN_LIVE).doc(x)))); if (parts.some(p => !p.exists)) return { error: "The run was saved at the same moment: press Override again", status: 409 }; for (const p of parts) Object.assign(lines, JSON.parse(p.data().json || "{}")); }
        else lines = Object.assign({}, old.lines || {});
        runs.push({ id, ref, had, lines });
      }
      const marks = []; for (let i = 0; i < orders.length; i += 100) marks.push(...await tx.getAll(...orders.slice(i, i + 100).map(o => col(CUSTOM).doc(o + "_review"))));
      const poolSnaps = []; for (let i = 0; i < rows.length; i += 100) poolSnaps.push(...await tx.getAll(...rows.slice(i, i + 100).map(id => col(POOL).doc(id))));

      /* ── the holds ── */
      const own = new Set(orders.filter(o => !gone.has(o))), onSheets = new Set(docs.flatMap(x => (x.data().poolIds || []).map(String))), live = poolSnaps.filter(x => x.exists && heldRow(x.data())), rowLines = new Set(live.map(x => lineOfPool(x.id)));
      const released = new Map(), note = (o, f) => released.set(o, Object.assign(released.get(o) || { lines: [], pieces: 0, review: false }, f(released.get(o) || { lines: [], pieces: 0, review: false })));
      const full = {};
      for (const r of runs) {
        const changed = [], next = Object.assign({}, r.lines);
        for (const [k, l] of Object.entries(r.lines)) { const o = orderOf(k, l); if (!own.has(o) || !holdOf(l, rowLines.has(k))) continue; next[k] = freed(k, l, { at, by, onSheets }); changed.push(k); full[k] = next[k]; note(o, x => ({ lines: x.lines.concat(k) })); }
        r.changed = changed; r.lines = next;
        if (!changed.length) continue;
        r.parts = d.liveParts(r.id, next); if (r.parts.some(p => p.bytes > 1000000)) return { error: "A piece of this run is too large to save: nothing was changed", status: 500 };
      }
      for (const x of live) note(String(x.id).split("_")[0], y => ({ pieces: y.pieces + 1 }));
      for (const m of marks) { const v = m.exists ? (m.data() || {}).review : null; if (v && v.state === "open" && own.has(m.id.replace(/_review$/, ""))) note(m.id.replace(/_review$/, ""), () => ({ review: true })); }

      /* ── every write ── */
      const process = [], sealed = how => list => list.concat([d.processEvent(how, at, by, list.length)]);
      const write = (kind, id, ref, old, extra) => {
        const was = history.get(kind === "set" ? "set" : id).slice(), processSeals = old.processReady ? was : sealed("laserReady")(was);
        const patch = Object.assign({ processSeals, processReady: true, laserOverride: kind === "set" ? Object.assign({}, mark, { set: said }) : mark }, extra || {});
        tx.set(ref, Object.assign({}, patch, { updatedAt: FV.serverTimestamp() }), { merge: true });
        const rec = d.processRecord(kind, id, Object.assign({}, old, { processSeals, processReady: true }));
        Object.assign(rec.patch, { laserOverride: mark }, kind === "sheet" ? { laserHold: null, draft: false } : {});
        process.push(rec);
      };
      for (const x of docs) {
        const s = x.data(); if (num(s.laserDoneAt) > 0) continue;
        const entry = { id: `override-${at}-${x.id}`, at, by, type: "override", note: null, setId, laserDoneAt: null }, extra = { flowHistory: (Array.isArray(s.flowHistory) ? s.flowHistory : []).concat([entry]).slice(-100) };
        if (s.laserHold) extra.laserHold = FV.delete();
        if (s.draft) extra.draft = false;
        if (s.solidIncluded === false) extra.solidIncluded = true;
        if (d.SetRules.recordPartial(s)) { extra.releaseFull = true; if (s.topup && !s.topup.closedAt) extra.topup = Object.assign({}, s.topup, { closedAt: at, byHand: true }); }
        write("sheet", x.id, col(SHEETS).doc(x.id), s, extra);
      }
      write("set", setId, sref, set);
      for (const r of runs) {
        if (!r.changed.length) continue;
        const keep = new Set(r.parts.map(p => p.id)), have = new Set(r.had);
        for (const p of r.parts) if (!have.has(p.id)) tx.set(col(RUN_LIVE).doc(p.id), { runId: r.id, json: p.json, bytes: p.bytes, lines: p.lines, seq: p.seq, createdAt: FV.serverTimestamp() });
        for (const x of r.had) if (!keep.has(x)) tx.delete(col(RUN_LIVE).doc(x));
        tx.set(r.ref, { liveLines: { ids: r.parts.map(p => p.id), lines: r.parts.reduce((n, p) => n + p.lines, 0), bytes: r.parts.reduce((n, p) => n + p.bytes, 0) }, lines: FV.delete(), lineEditAt: at, updatedAt: FV.serverTimestamp() }, { merge: true });
      }
      for (const x of live) tx.set(col(POOL).doc(x.id), { repooledAt: at, updatedAt: FV.serverTimestamp() }, { merge: true });
      const byId = new Map(marks.map(m => [m.id.replace(/_review$/, ""), m]));
      for (const [o, f] of released) {
        const m = byId.get(o), cur = m && m.exists ? m.data() || {} : null, v = cur && cur.review, wasRel = cur && cur.release && typeof cur.release === "object" ? cur.release : null;
        const earlier = wasRel ? [Object.assign({}, wasRel)].concat(Array.isArray(wasRel.earlier) ? wasRel.earlier : []).map(e => { const y = Object.assign({}, e); delete y.earlier; return y; }).slice(0, 10) : [];
        const release = Object.assign({ at, by, how: "releaseHold", id: `releaseHold:${o}:${at}`, path: "override" }, earlier.length ? { earlier } : {});
        const doc = Object.assign(cur ? {} : { key: o + "_review", kind: "reviewMark", category: "Review", receiptId: o, state: "open", hasLabel: false }, { release, updatedAtMs: at, updatedAt: FV.serverTimestamp() },
          f.review && v ? { review: Object.assign({}, v, { state: "done", doneAt: at, doneBy: by, how: "other", override: setId }) } : {});
        tx.set(col(CUSTOM).doc(o + "_review"), doc, { merge: true });
        tx.set(jobs().doc(o), { rid: o, state: "done", at, frontAt: at, doneAt: at, by, dev, seal: { how: "releaseHold", id: release.id, at, by }, lines: f.lines, pieces: f.pieces, released: true, error: null, override: setId }, { merge: true });
      }
      return { ok: true, process, released: [...released.keys()], counts: Object.fromEntries([...released].map(([o, f]) => [o, f])), full, runs: runs.filter(r => r.changed.length).map(r => r.id), sheets: docs.map(x => Object.assign({}, x.data(), { id: x.id })), set: Object.assign({}, set, { setId }) };
    });
    if (res.error) return res;
    /* ── what follows the press: the orders' timelines, the labels, the readings (each best-effort: the approval and the releases stand) ── */
    const name = res.set.name || setId, what = [L.engravings.n ? `${L.engravings.n} engraving${L.engravings.n === 1 ? "" : "s"} not approved` : "", ...L.sheets.map(x => `${x.label} ${x.why}`), res.released.length ? `${res.released.length} hold${res.released.length === 1 ? "" : "s"} released` : ""].filter(Boolean).join("; ");
    const labelOfOrder = o => { const s = res.sheets.find(x => Readiness.orderIds(x).map(String).includes(o)); return s ? { sheetId: s.id, sheet: d.sheetLabel(s) } : {}; };
    await d.stamp(() => orders.map(o => Object.assign({ orderId: o, type: "note", id: `${mark.id}-${o}`, at, by, station: "laser", device: "charm-nest-1", setId, text: `Approved for laser cutting by override · ${name}${what ? " · " + what : ""}`.slice(0, 200), data: { flow: "override", signedIn: true, via: "Override" } }, labelOfOrder(o)))
      .concat(res.released.map(o => ({ orderId: o, type: "released", id: `oh-rel-${o}-${at}`, at, by, station: "sorter", device: dev.slice(0, 40) || "charm-nest-1", text: `Released from hold by ${by} · its set ${name} was approved by override`.slice(0, 200), data: { frontAt: at, cloud: true, lines: res.counts[o].lines.length, pieces: res.counts[o].pieces, seal: "releaseHold", sealAt: at, sealId: `releaseHold:${o}:${at}`, from: "override" } }))), "override");
    if (!d.prefix() && Object.keys(res.full).length) { try { await require("./_charmNestWorld").runNow({ db, lines: res.full }); } catch (e) { console.warn("[override] the reading is put right by its next build:", (e && e.message) || e); } }
    const labels = [];
    for (const s of res.sheets) {
      if (num(s.laserDoneAt) > 0 || !(L.sheets.find(x => x.id === s.id && /QR label/.test(x.why)))) continue;
      try { const r = await d.ops().labelsAsk({ op: "labelsAsk", kind: "complete", sheetId: s.id, by, dev }); if (r && r.ok) labels.push(s.id); } catch (e) { console.warn("[override] a QR label was not asked for:", (e && e.message) || e); }
    }
    d.feedOrders(b, [res.released]);
    d.revTouch(b, ["t:" + setId].concat(res.sheets.map(s => "s:" + s.id), res.runs.map(r => "r:" + r), res.released.slice(0, 200).map(o => "c:" + o + "_review"), res.released.length ? ["w:world"] : []));
    return { ok: true, setId, at, by, override: mark, process: res.process, released: res.released, lines: res.full, labels, overrode: { engravings: L.engravings.n, sheets: L.sheets, waits: L.waits } };
  }
  return { setOverride, setOverrideGet };
};
