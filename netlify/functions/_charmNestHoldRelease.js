/*  netlify/functions/_charmNestHoldRelease.js — RELEASE HOLD IN THE CLOUD, AT ONCE, FROM ANY COMPUTER (Paul, 20 Nov 2026: "a Release Hold press on ANY computer releases the hold in the
 *  cloud at once (hold marks, the run line's hold, the timeline 'released' entry, frontAt) so every computer shows it released; placing the pieces is the intake's normal work; no more waiting on
 *  the computer that carries the run"). Cloud plan v2, build step 13: ops releaseAsk and releaseJobGet, a release job journal, and the driver's release unit.
 *
 *  What there was: a computer that only FOLLOWS the run asked the computer that CARRIES it (runCmdPut release) and waited for it, up to twenty minutes (charm-nest-order-release.js relayRelease);
 *  the hold stayed in the cloud until that computer had made the order's pieces up again, and every other computer kept showing the order held. Now the press itself releases the hold in the cloud:
 *
 *    releaseAsk { orderId, by, dev, runId?, seal?, lines?: { lineKey: { releaseAnyway?, blockOverride?, skip?, skuOverride? } }, note?, resume?, queue? }
 *      queue (ITEM 8, Paul 21 Nov 2026): Release Hold pressed on a viewer for an order with NO line on hold (a blocked SKU cut anyway, a question set aside: the Review card's
 *                   "Release to nesting queue"). The lines it names (keys) that carry something set aside (lines) are taken too, held or not: what was set aside, the front of the queue,
 *                   the seal; a line the reading stopped (held, unmatched, oversize, no pieces) is "pulled" again, so the cloud's intake makes it up with what was set aside and the
 *                   placement places it. The page that pressed writes the order's timeline step (as it always did for such a press): none is written here unless a hold was lifted.
 *      1  journal   Charm_Nest_Release_Jobs/<orderId> is written first: { state: "lifting", frontAt, by, seal, ... }. A press within ten minutes of an unfinished one is that same release (its
 *                   front time and its timeline id); the driver's unit finishes one a function that died left "lifting" (resume: true).
 *      2  marks     the order's pool rows a HOLD took off their sheet (abandoned, on no sheet, heldAt/heldBy, no cancel mark, not made live again: _charmNestPlacement.takenOff for a hold) get
 *                   repooledAt: the same stamp the page's liftInCloud wrote and poolPut writes; every computer's placement answer stops calling them held. Nothing is made up and no sheet is touched.
 *      3  the run   the open run's lines of the order that are held (their hold, or pool rows held in step 2, or a line the page says it showed held in state "held"): hold, heldAt and reason go,
 *                   holdSeen false, state "held" becomes "pulled" (no pieces) or "pooled" (pieces kept), frontAt (the front of the queue: CharmNestOrders.byQueue) and releasedAt are the release's,
 *                   the RELHOLD seal (releaseHold) and what the press set aside (releaseAnyway, blockOverride: sent by the page that pressed) are kept, `releasing` goes; the line is stamped
 *                   _lp { at, dev: "cloud", by }. ONE transaction, as runLinesPatch: a computer's older copy never puts the hold back (op_runPut keeps the newer stamp), and the computer that
 *                   carries the run takes the line (charm-nest-run-follow.js applyOthers) and makes its pieces up through its intake (Review.repool).
 *      4  timeline  one 'released' event on the order's timeline, id "oh-rel-<order>-<frontAt>" (the page's own id for the same release: a carrier that goes on to place the pieces writes onto it).
 *      5  done      the journal says done (or failed, with the words).
 *      -> { ok, orderId, frontAt, at, stamp: { at, dev: "cloud", by }, runId, lines: { lineKey: the fields as the cloud holds them now }, pieces, released, already? }
 *    releaseJobGet { orderId } -> { ok, job }
 *    driveUnit({ db, call }): the cloud driver's release unit (_charmNestDrive.js pass): a release left "lifting" for a minute is finished through the cloud's own fenced call.
 *
 *  Never calls Etsy, never builds the world (the handler's worldSoon after a page's press is today's, as for poolUpdate and runLinesPatch). Every write of the cloud's own call is fenced (fenceTx). */
"use strict";
const JOBS = "Charm_Nest_Release_Jobs", RUNS = "Charm_Nest_Runs", RUN_LIVE = "Charm_Nest_Run_Live", POOL = "Charm_Pool", OPEN = "Charm_Nest_Open";
const CLOUD_DEV = "cloud";
const SAME_PRESS_MS = 10 * 60000, STUCK_MS = 60000;
const ORDER_ID = /^\d{4,20}$/, LINE_KEY = /^[\w\-]{3,80}$/;
const num = v => (Number.isFinite(+v) ? +v : 0);
const str = (v, n) => String(v == null ? "" : v).slice(0, n || 200);
const ridOf = v => String(v == null ? "" : v).replace(/\D/g, "");
/** The hold's own mark on a pool row, the same rule every reader uses (charm-nest-order-pieces.js heldRow, _charmNestPlacement.takenOff for a hold). */
const heldRow = p => !!p && p.state === "abandoned" && !p.sheetId && (num(p.heldAt) > 0 || !!p.heldBy) && !(num(p.removedAt) > 0 || p.removedBy) && !num(p.repooledAt);
const lineOfPool = id => String(id == null ? "" : id).replace(/_\d{1,3}$/, "");
const closedRun = r => !r || ["complete", "abandoned", "archived"].includes(String(r.status || "")) || !!r.retiredInto;
/** The RELHOLD seal of a Review card's Release Hold press: { how: "releaseHold", id, at, by }, or null. */
function cleanSeal(s) {
  if (!s || typeof s !== "object" || s.how !== "releaseHold" || !(num(s.at) > 1e12)) return null;
  return { how: "releaseHold", id: str(s.id, 120), at: num(s.at), by: str(s.by, 80) };
}
/** What a press set aside for a line (ReviewRelease.setAside on the page that pressed): its questions (releaseAnyway) and a blocked SKU cut anyway (blockOverride), made safe. */
function cleanAside(a) {
  const out = {};
  if (a && a.releaseAnyway && typeof a.releaseAnyway === "object" && num(a.releaseAnyway.at) > 0) out.releaseAnyway = { at: num(a.releaseAnyway.at), by: str(a.releaseAnyway.by, 80), kinds: (Array.isArray(a.releaseAnyway.kinds) ? a.releaseAnyway.kinds : []).slice(0, 8).map(x => str(x, 24)) };
  if (a && a.blockOverride && typeof a.blockOverride === "object" && a.blockOverride.sku) out.blockOverride = Object.assign({ sku: str(a.blockOverride.sku, 80), by: str(a.blockOverride.by, 80), at: num(a.blockOverride.at) }, Array.isArray(a.blockOverride.skus) && a.blockOverride.skus.length ? { skus: a.blockOverride.skus.slice(0, 20).map(x => str(x, 80)) } : {});
  // (NESTASK: Nest on a line nobody matched uses the charm it shows, for this order only: "Just this order", skuOverride, as _charmNestCmdExec.js skuonce writes it)
  if (a && a.skuOverride && typeof a.skuOverride === "object" && a.skuOverride.sku) out.skuOverride = { sku: str(a.skuOverride.sku, 80).trim().toUpperCase(), by: str(a.skuOverride.by, 80), at: num(a.skuOverride.at), was: str(a.skuOverride.was, 80) };
  return out;
}
/** NEST / SKIP (Paul, 23 Nov 2026: a Release Hold press asks whether the charm goes on a sheet or is skipped): per line, { skip: true } = its piece is never made (the line is "skipped", its hold released
 *  in the same transaction: no pass can place it in between), { skip: false } = Nest, which makes a piece skipped before again. */
const skipOf = a => (a && typeof a.skip === "boolean" ? { skip: a.skip } : {});
/** The fields of a released line every page takes (the answer's `lines`). */
const LINE_OUT = ["state", "poolIds", "hold", "heldAt", "holdSeen", "reason", "frontAt", "releasedAt", "releasedBy", "releaseHold", "releaseAnyway", "blockOverride", "skuOverride", "_lp"];
const pick = l => { const o = {}; for (const f of LINE_OUT) if (l[f] !== undefined) o[f] = l[f]; return o; };

/** d: the library's own helpers, so every write goes through its sandbox prefix, its fence and its feed:
 *  { db, FV, col(name), prefix(), marked(), fenceTx(tx), liveParts(runId, lines), stamp(events, what), revTouch(b, keys), feedOrders(b, lists) } */
function make(d) {
  const { db, FV } = d;
  const jobs = () => db.collection(d.prefix() + JOBS);   // (the sandbox keeps its own, as every family of the run: Sandbox_Charm_Nest_Release_Jobs)

  /** The open run that holds the order's lines: the one the page named, else the one the cloud names open (Charm_Nest_Open/run). */
  async function runOf(asked) {
    const id = str(asked, 80);
    if (/^[\w\-]{4,80}$/.test(id)) { const s = await d.col(RUNS).doc(id).get(); if (s.exists && !closedRun(s.data())) return id; }
    const o = await d.col(OPEN).doc("run").get(), named = o.exists ? str((o.data() || {}).runId, 80) : "";
    if (/^[\w\-]{4,80}$/.test(named)) { const s = await d.col(RUNS).doc(named).get(); if (s.exists && !closedRun(s.data())) return named; }
    return null;
  }

  /** Step 3: the run's held lines of the order released, in ONE transaction (as op_runLinesPatch). -> { runId, changed: [keys], lines: { key: fields }, at } */
  async function releaseLines(runId, rid, o) {
    const ref = d.col(RUNS).doc(runId);
    return db.runTransaction(async tx => {
      await d.fenceTx(tx);
      const ex = await tx.get(ref); if (!ex.exists) return { runId: null, changed: [], lines: {} };
      const old = ex.data() || {}; if (closedRun(old)) return { runId: null, changed: [], lines: {} };
      const had = old.liveLines && Array.isArray(old.liveLines.ids) ? old.liveLines.ids : [];
      let lines = {};
      if (had.length) { const docs = await Promise.all(had.map(x => tx.get(d.col(RUN_LIVE).doc(x)))); if (docs.some(s => !s.exists)) throw new Error("the run was saved while its lines were read: try again"); for (const s of docs) Object.assign(lines, JSON.parse(s.data().json || "{}")); }
      else lines = Object.assign({}, old.lines || {});
      const at = Date.now(), changed = [], out = {}, full = {}, skipped = [], picked = [];
      let holds = 0;
      for (const k of Object.keys(lines)) {
        if (!k.startsWith(rid + "_")) continue;
        const cur = lines[k]; if (!cur || typeof cur !== "object") continue;
        const held = !!cur.hold || o.heldKeys.has(k) || (o.askKeys.has(k) && String(cur.state || "") === "held");
        const aside = cleanAside(o.asides[k]), skip = !!(o.asides[k] && o.asides[k].skip === true) && !["gone", "committed"].includes(String(cur.state || "")) && !o.placed.has(k);   // (a piece on a sheet is never skipped behind it: it is taken off there)
        const named = !held && o.askKeys.has(k) && (skip || !!aside.skuOverride || (!!o.queue && Object.keys(aside).length > 0));   // (ITEM 8: a line the press set something aside on, not held)
        if (!held && !named) { out[k] = pick(cur); continue; }
        if (cur.hold || o.heldKeys.has(k)) holds++;
        const next = Object.assign({}, cur), was = String(cur.state || "");
        next.hold = null; next.heldAt = null; next.holdSeen = false;
        if (cur.repoolChanged === true) { next.poolIds = []; next.repoolChanged = false; if (was !== "held") next.state = "pulled"; }   // (REPOALL: its design was drawn anew while it was on hold: its pieces are made again)
        if (was === "held") { next.state = Array.isArray(next.poolIds) && next.poolIds.length ? "pooled" : "pulled"; next.reason = null; }
        else if (named && ["unmatched", "oversize"].includes(was) && !(Array.isArray(cur.poolIds) && cur.poolIds.length)) { next.state = "pulled"; next.reason = null; }
        else if (cur.reason != null && cur.hold != null && String(cur.reason) === String(cur.hold)) next.reason = null;
        if (skip) { next.state = "skipped"; next.poolIds = []; next.reason = `no charm made: skipped at Release Hold by ${o.by}`; skipped.push(k); }
        else if (was === "skipped" && o.asides[k] && o.asides[k].skip === false) { next.state = "pulled"; next.reason = null; }
        next.frontAt = o.frontAt; next.releasedAt = o.frontAt; next.releasedBy = o.by;
        if (o.seal) next.releaseHold = { at: o.seal.at, by: o.seal.by, id: o.seal.id };
        Object.assign(next, aside);
        if (aside.skuOverride) picked.push(k);
        delete next.releasing;   // (a release under way on a page is this one)
        next._lp = { at, dev: CLOUD_DEV, by: o.by };
        lines[k] = next; changed.push(k); out[k] = pick(next); full[k] = next;
      }
      if (!changed.length) return { runId, changed, lines: out, at: 0, holds };
      const parts = d.liveParts(runId, lines);
      if (parts.some(p => p.bytes > 1000000)) throw new Error("a piece of this run is too large to save");
      const keep = new Set(parts.map(p => p.id)), have = new Set(had);
      for (const p of parts) if (!have.has(p.id)) tx.set(d.col(RUN_LIVE).doc(p.id), { runId, json: p.json, bytes: p.bytes, lines: p.lines, seq: p.seq, createdAt: FV.serverTimestamp() });
      for (const x of had) if (!keep.has(x)) tx.delete(d.col(RUN_LIVE).doc(x));
      tx.set(ref, { liveLines: { ids: parts.map(p => p.id), lines: parts.reduce((n, p) => n + p.lines, 0), bytes: parts.reduce((n, p) => n + p.bytes, 0) }, lines: FV.delete(), lineEditAt: at, updatedAt: FV.serverTimestamp() }, { merge: true });
      return { runId, changed, lines: out, at, holds, full, skipped, picked };
    });
  }

  async function releaseAsk(b) {
    const rid = ridOf(b.orderId); if (!ORDER_ID.test(rid)) return { error: "orderId required", status: 400 };
    const marked = !!d.marked(), resume = b.resume === true || b.resume === 1 || b.resume === "1";
    let by = str(b.by, 80).trim(); const dev = str(b.dev, 60);
    if (!by && !marked && !resume) return { error: "A name is needed for the record: sign in, then press Release hold again", status: 400 };
    const jref = jobs().doc(rid), now = Date.now();
    // 1 · the journal, first
    const job = await db.runTransaction(async tx => {
      await d.fenceTx(tx);
      const s = await tx.get(jref), o = s.exists ? s.data() || {} : null;
      const same = !!o && ["lifting", "failed"].includes(o.state) && now - num(o.at) < SAME_PRESS_MS;   // (a press again after one that did not finish is that same release: its front time, its timeline id, what it did)
      if (resume && !(o && o.state === "lifting")) return null;   // (the driver's unit: nothing left to finish)
      const ask = resume ? {} : { note: str(b.note, 300),   // (what this press says of its lines: a press again after one that did not finish may have been answered otherwise: NESTASK)
        asides: b.lines && typeof b.lines === "object" && !Array.isArray(b.lines) ? Object.fromEntries(Object.keys(b.lines).filter(k => LINE_KEY.test(k) && k.startsWith(rid + "_")).slice(0, 40).map(k => [k, Object.assign(cleanAside(b.lines[k]), skipOf(b.lines[k]))])) : {},
        askKeys: Array.isArray(b.keys) ? b.keys.map(k => str(k, 80)).filter(k => LINE_KEY.test(k) && k.startsWith(rid + "_")).slice(0, 40) : [], queue: b.queue === true };
      const j = same || resume ? Object.assign({}, o, ask, { state: "lifting", error: null, tries: num(o.tries) + 1, beatAt: now }) : Object.assign({ rid, state: "lifting", at: now, frontAt: now, by: by || "Cloud", dev, seal: cleanSeal(b.seal), tries: 1, beatAt: now, runId: null, error: null, skips: (o && o.skips) || {} }, ask);
      tx.set(jref, j);
      return j;
    });
    if (!job) return { ok: true, orderId: rid, nothing: true };
    by = job.by || by || "Cloud";
    const frontAt = num(job.frontAt) || now, fail = async e => {
      const why = str((e && e.message) || e, 300);
      try { await jref.set({ state: "failed", error: why, failedAt: Date.now() }, { merge: true }); } catch (_) { /* the words below still say it */ }
      return { error: `The hold was not released in the cloud: ${why}. Press Release hold again.`, status: 500, orderId: rid };
    };
    try {
      // 2 · the hold's marks on the order's pool rows
      const values = [rid].concat(Number.isSafeInteger(+rid) ? [+rid] : []);
      const ps = await d.col(POOL).where("orderId", "in", values).limit(400).get();
      const placed = new Set(ps.docs.filter(x => { const p = x.data() || {}; return p.sheetId && !["abandoned", "superseded"].includes(String(p.state || "")); }).map(x => lineOfPool(x.id)));
      const held = ps.docs.filter(x => heldRow(x.data())), heldKeys = new Set(held.map(x => lineOfPool(x.id)));
      for (let i = 0; i < held.length; i += 200) {
        const part = held.slice(i, i + 200);
        await db.runTransaction(async tx => {
          await d.fenceTx(tx);
          const snaps = await Promise.all(part.map(x => tx.get(x.ref)));
          snaps.forEach(s => { if (s.exists && heldRow(s.data())) tx.set(s.ref, { repooledAt: frontAt, updatedAt: FV.serverTimestamp() }, { merge: true }); });
        });
      }
      // 3 · the run's lines
      const runId = await runOf(b.runId || job.runId);
      const lineRes = runId ? await releaseLines(runId, rid, { heldKeys, placed, askKeys: new Set(job.askKeys || []), frontAt, by, seal: job.seal || null, asides: job.asides || {}, queue: job.queue === true }) : { runId: null, changed: [], lines: {}, at: 0 };
      // (RELNOW: the reading a fresh page takes says it too, at once; it said "on hold" until the next reading of all orders)
      let worldSaid = 0; if (!d.prefix() && lineRes.full) try { const World = require("./_charmNestWorld"), picked = lineRes.picked || [], rest = Object.fromEntries(Object.entries(lineRes.full).filter(([k]) => !picked.includes(k)));
        worldSaid = await World.runNow({ db, lines: rest });
        if (picked.length && d.ops) { const all = d.ops(), ops = {}; for (const op of ["masterList", "optionMapGet", "aliasGet", "noDesignGet", "customGet", "listingSkus", "customSheetGet"]) ops[op] = q => all[op](Object.assign({ op }, q || {})); worldSaid += (await World.readNow({ db, ops, keys: picked, lines: lineRes.full })).size; }   // (a line given its charm is read again with it now, so the intake that follows makes it)
      } catch (e) { console.warn("[release] the reading is put right by its next build:", (e && e.message) || e); }
      // (what steps 2 and 3 did is kept in the journal before the timeline step: a press sent again, or the driver's unit, finds nothing held any more and still knows this release released something)
      const pieces = held.length + num(job.pieces), linesDone = [...new Set((Array.isArray(job.lines) ? job.lines : []).concat(lineRes.changed))];
      if (held.length || lineRes.changed.length) await db.runTransaction(async tx => { await d.fenceTx(tx); tx.set(jref, { pieces, lines: linesDone, runId: lineRes.runId || job.runId || null, beatAt: Date.now() }, { merge: true }); });
      const released = pieces > 0 || linesDone.length > 0;
      // 4 · the timeline (only for a release that released something: an order the cloud does not hold on hold gets no "released" step)
      if (released && !(job.queue === true && !held.length && !num(lineRes.holds))) {   // (ITEM 8: a press with no hold lifted: the page that pressed writes its own step)
        const seal = job.seal;
        await d.stamp([{ orderId: rid, type: "released", id: `oh-rel-${rid}-${frontAt}`, at: frontAt, by, station: "sorter", device: str(job.dev, 40) || "charm-nest-1",
          text: (`Released from hold by ${by} · front of the queue` + (job.note ? ` · ${job.note}` : "")).slice(0, 200),
          data: Object.assign({ frontAt, cloud: true, lines: linesDone.length, pieces }, seal ? { seal: "releaseHold", sealAt: seal.at, sealId: seal.id } : {}) }], "released");
      }
      const skipped = lineRes.skipped || [], picks = (lineRes.picked || []).map(k => [k, lineRes.full[k].skuOverride]);
      if (picks.length) await d.stamp(picks.map(([k, p]) => ({ orderId: rid, type: "decided", id: `${k}.once.${frontAt}`, at: frontAt, by, station: "sorter", lineKey: k, transactionId: k.split("_")[1] || "", text: `Decided by ${by} · Just this order: ${p.was || "no SKU"} → ${p.sku} (the listing and Etsy are not changed)`.slice(0, 200), data: { sku: p.sku, once: true, was: p.was || null, from: "releaseHold", cloud: true } })), "decided");
      if (skipped.length) await d.stamp(skipped.map(k => ({ orderId: rid, type: "skipped", id: `${k}.relskip.${frontAt}`, at: frontAt, by, station: "sorter", lineKey: k, transactionId: k.split("_")[1] || "", text: `No charm made: skipped at Release Hold by ${by}`.slice(0, 200), data: { answer: "Skip", from: "releaseHold", cloud: true } })), "skipped");
      // 5 · done
      // (NESTASK: the lines a person skipped, kept with the order for good: a run opened later takes them in as skipped (_charmNestIntake.js join), never as new pieces)
      const skips = Object.assign({}, job.skips); for (const k of lineRes.changed) if (skipped.includes(k)) skips[k] = frontAt; else delete skips[k];
      await db.runTransaction(async tx => { await d.fenceTx(tx); tx.update(jref, { state: "done", doneAt: Date.now(), runId: lineRes.runId || job.runId || null, lines: linesDone, pieces, released, error: null, skips }); });
      d.feedOrders(b, [[rid]]);
      if (lineRes.runId && lineRes.changed.length) d.revTouch(b, ["r:" + lineRes.runId].concat(worldSaid ? ["w:world"] : []));
      return Object.assign({ ok: true, orderId: rid, frontAt, at: lineRes.at || Date.now(), stamp: lineRes.at ? { at: lineRes.at, dev: CLOUD_DEV, by } : null, runId: lineRes.runId || null, lines: lineRes.lines, pieces, released }, released ? {} : { already: true });
    } catch (e) {
      if (e && e.fenced) throw e;   // (the cloud's own call: another pass holds the lease; nothing more is written)
      return fail(e);
    }
  }

  async function releaseJobGet(b) {
    const rid = ridOf(b.orderId); if (!ORDER_ID.test(rid)) return { error: "orderId required", status: 400 };
    const s = await jobs().doc(rid).get();
    return { ok: true, job: s.exists ? s.data() : null };
  }
  return { releaseAsk, releaseJobGet };
}

/** The cloud driver's release unit (cloud plan v2 §1.6 item 3, before intake): a release a function left "lifting" for a minute is finished, through the cloud's own fenced call. Production only.
 *  env: { db, call(body, unit) }. -> how many were taken up. */
async function driveUnit(env) {
  const { db, call } = env;
  const q = await db.collection(JOBS).where("state", "==", "lifting").limit(10).get(), now = Date.now();
  let n = 0;
  for (const doc of q.docs) {
    const j = doc.data() || {}; if (now - Math.max(num(j.beatAt), num(j.at)) < STUCK_MS) continue;
    await call({ op: "releaseAsk", orderId: doc.id, resume: true }, "release|" + doc.id + "|" + num(j.at) + "|" + num(j.tries));
    n++;
  }
  return n;
}

module.exports = { make, driveUnit, heldRow, cleanSeal, cleanAside, JOBS };
