'use strict';
/* The leftover sheets (GC3, Paul 7 Oct: "the remaining partial sheet that is left over after a green line is cut must be saved in
   its exact shape and real size ... a saved repository of all available remaining partial sheets").

   ONE record per cut, Charm_Nest_Remnants/{stockId}-{revision}, written by recordRemnant INSIDE the cut's own transaction
   (_charmNestRoseStock.js roseRecordCut, the one place a cut of Rose Gold, 10K or 14K is recorded, whichever surface pressed it:
   the Nest tab's Cut Sheet or the Library's drag to Laser cutting): a cut can never exist without its leftover. The app never
   deletes one (the sandbox's reset clears only Sandbox_Charm_Nest_Remnants). The record holds what a card draws and says:
     metal, code ('RG'|'10K'|'14K'), sheetId, sheetName ('RG Sheet 1'), setId, setName ('Set 1'), fileBase, stockId, revision,
     via ('nest'|'library'), cutAt, by (the signed-in person, '' = none), sheetWMm / sheetHMm (the real sheet), rings (the exact
     outline, closed rings in real mm, top left origin, even-odd; the staircase of every green line included), areaMm2, bboxMm
     {x,y,w,h} (its real width and height), status 'available' | 'used' | 'discarded' (+ statusAt, statusBy; a leftover that a later
     cut was made on is 'used' with usedBySheetId / usedBySheetName / usedAt / usedBy; one too small to reuse starts 'discarded'
     with auto:true and a reason), marked:true when a person set the status, createdAt.
   Reading is ONE op, remnantList (a list with a limit and a field mask, answered { unchanged } from one tiny document when the
   counter Charm_Nest_Rev/remnants has not moved: no polling, no per-card reads); a person's press on a card is remnantMark.
   The counter moves in the transaction of every write (production only, as the Library's own counter: the sandbox keeps none).

   PARTIAL SHEETS (PS3, Paul 7 Oct: "each sheet needs its own individual Partial Sheet repo"): a partial = one of these records. It also carries
   lastUsedAt / lastUsedBy / lastUsedSheet (= the cut until a sheet is nested on it or cut from it) and the status 'inUse' (a sheet holds it:
   inUseBySheetId / Name / At / By) beside available / used / discarded. ONE source of truth for who holds it: the Rose stock document
   (Charm_Nest_Rose_Stock/{stockId}.owner, written by roseClaim / roseRelease); the record's inUse fields are kept in step INSIDE those
   transactions (`sync`, handed to _charmNestRoseStock.js), so a partial can never be held by two sheets and the list can never disagree
   with the nester. Ops (see plans/partial-sheets/contract.md): partialList (one cheap read per metal: field mask, revision probe, no
   polling), partialPolicyGet / partialPolicySet (the setting per metal), partialClaim / partialRelease / partialUse, partialPlan.
   Nothing is ever deleted. */
const Rose = require('../../charm-nest-rose');
const Partial = require('../../charm-nest-partial');
const COLL = 'Charm_Nest_Remnants';
const MM = Rose.MM;
const CODES = { rose: 'RG', gold10k: '10K', gold14k: '14K', gold: 'GF', silver: 'SS' };
const FIELDS = ['v', 'metal', 'code', 'sheetId', 'sheetName', 'setId', 'setName', 'fileBase', 'stockId', 'revision', 'via', 'cutAt', 'by', 'sheetWMm', 'sheetHMm', 'rings', 'areaMm2', 'bboxMm',
  'status', 'statusAt', 'statusBy', 'marked', 'auto', 'reason', 'usedBySheetId', 'usedBySheetName', 'usedAt', 'usedBy', 'createdAt',
  'lastUsedAt', 'lastUsedBy', 'lastUsedSheet', 'lastUsedSheetId', 'inUseBySheetId', 'inUseBySheetName', 'inUseAt', 'inUseBy'];
const STATUSES = ['available', 'used', 'discarded'];   // what a person can mark; 'inUse' is only ever set by a claim (roseClaim / partialClaim) and cleared by a release or the next cut
const PARTIAL_METALS = ['rose', 'gold10k', 'gold14k'];
const STOCKS = 'Charm_Nest_Rose_Stock', POLICY_DEFAULT = { mode: 'auto', wMm: 100, hMm: 50 }, SIZE_MM = [5, 500];
const NOT_HELD = { inUseBySheetId: null, inUseBySheetName: null, inUseAt: null, inUseBy: null };   // what a record says when no sheet holds it
const okId = s => typeof s === 'string' && /^[\w-]{4,100}$/.test(s);
const person = s => { const t = String(s == null ? '' : s).trim().slice(0, 80); return /^operator$/i.test(t) ? '' : t; };

// as charm-nest-rose.js compact: no repeated point, no middle point of three on one axis-parallel line
function compact(points) {
  const out = [];
  for (const p of points) {
    const n = out.length;
    if (n && p[0] === out[n - 1][0] && p[1] === out[n - 1][1]) continue;
    if (n > 1) { const a = out[n - 2], b = out[n - 1]; if ((a[0] === b[0] && b[0] === p[0]) || (a[1] === b[1] && b[1] === p[1])) out.pop(); }
    out.push(p);
  }
  return out;
}
/* The sheet a cut profile leaves: the stock rectangle minus the region the green contour cut off, from the very rows the lines are
   drawn from (charm-nest-rose.js plan / profile / lines). Row i of the profile has gone from the sheet's near edge to values[i], so
   row i keeps [values[i], depth]: a leftover is one or more pieces (a piece ends where a row is cut right through), each a staircase
   on its cut side. area is exact (worked out from the rows, not read back from the rounded rings). */
function leftover(p) {
  const len = p.axis === 'x' ? p.hPt : p.wPt, depth = p.axis === 'x' ? p.wPt : p.hPt, v = p.values, n = v.length;
  const at = (d, t) => (p.axis === 'x' ? [d, t] : [t, d]), open = i => v[i] < depth - 1e-9, mm = x => +(x * MM).toFixed(3), r3 = x => +x.toFixed(3);
  const rings = [];
  let area = 0;
  for (let i = 0; i < n;) {
    if (!open(i)) { i++; continue; }
    let j = i;
    while (j + 1 < n && open(j + 1)) j++;
    const ring = [at(depth, i * p.step)];
    for (let k = i; k <= j; k++) { const a = k * p.step, b = Math.min(len, (k + 1) * p.step); ring.push(at(v[k], a), at(v[k], b)); area += (depth - v[k]) * (b - a); }
    ring.push(at(depth, Math.min(len, (j + 1) * p.step)));
    rings.push(compact(ring).map(([x, y]) => [mm(x), mm(y)]));
    i = j + 1;
  }
  let box = null;
  for (const ring of rings) for (const [x, y] of ring) box = box ? [Math.min(box[0], x), Math.min(box[1], y), Math.max(box[2], x), Math.max(box[3], y)] : [x, y, x, y];
  return { rings, areaMm2: +(area * MM * MM).toFixed(2), bboxMm: box ? { x: r3(box[0]), y: r3(box[1]), w: r3(box[2] - box[0]), h: r3(box[3] - box[1]) } : { x: 0, y: 0, w: 0, h: 0 }, sheetWMm: r3(p.wPt * MM), sheetHMm: r3(p.hPt * MM) };
}

/* deps: { col, FV, db, sheetLabel(sheetDoc), setLabel(setId), revDoc() -> the counter's ref, or null (the sandbox) }.
   Returns { recordRemnant (NOT an op: only the cut's transaction calls it), ops: { remnantList, remnantMark } }. */
module.exports = function ({ db, col, FV, sheetLabel, setLabel, revDoc, configRef, statsRef, statsWrite }) {
  /* configRef() -> the setting document (config/charmNestPartials; the sandbox has its own), statsRef() -> the small document of per-metal running sums of
     the pieces cuts have placed (Charm_Nest_Rev/partialStats; read by both workspaces), statsWrite() -> whether this workspace adds to it (production only). */
  const coll = () => col(COLL), stocks = () => col(STOCKS);
  let stockApi = null;   // the Rose stock operations (roseClaim / roseRelease), bound by the library once both factories exist (bind below)
  const bump = tx => { const ref = revDoc && revDoc(); if (ref && FV.increment) tx.set(ref, { n: FV.increment(1), at: Date.now() }, { merge: true }); };
  // the counter's value, and whether the earlier cuts' leftovers were saved (remnantBackfill); null = the sandbox, which keeps no counter
  const revState = async () => { const ref = revDoc && revDoc(); if (!ref) return null; const s = await ref.get(), d = s.exists ? s.data() || {} : {}; return { rev: String(Number(d.n) || 0), backfilled: !!d.backfilledAt, reconciled: !!d.reconciledAt }; };
  const ms = t => (t && typeof t.toMillis === 'function' ? t.toMillis() : typeof t === 'number' ? t : null);
  const clean = (id, d) => { const o = { id }; for (const k of FIELDS) if (d[k] !== undefined) o[k] = d[k]; o.createdAt = ms(d.createdAt); return o; };

  /* What a cut teaches about the metal's regular piece, from the sheet it was made on (already in the cut's transaction: no read): for every placed piece its
     footprint (silhouette, grown by half a POSITIVE clearance on every side: the page's inflatedArea) and the shorter / longer side of its box, in mm. Added
     to the metal's running sums with increments (no read), so the typical piece costs one tiny document read and never a scan. */
  function pieceStats(sheet) {
    const byId = new Map((sheet.charms || []).map(c => [c.id, c])), out = { n: 0, areaMm2: 0, minMm: 0, maxMm: 0 }, g = Math.max(0, +(sheet.params && sheet.params.clearancePt) || 0) / 2;
    for (const p of sheet.placements || []) {
      const c = byId.get(p.id); if (!c || !(+c.areaPt2 > 0) || !(+c.widthPt > 0) || !(+c.heightPt > 0)) continue;
      const sc = +p.scale > 0 ? +p.scale : 1, w = c.widthPt * sc, h = c.heightPt * sc;
      out.n++; out.areaMm2 += (c.areaPt2 * sc * sc + g * 2 * (w + h) + Math.PI * g * g) * MM * MM; out.minMm += Math.min(w, h) * MM; out.maxMm += Math.max(w, h) * MM;
    }
    return out;
  }
  const r2 = x => Math.round(x * 100) / 100;

  /* recordRemnant(tx, { stock, cut, sheet, plan, metal, device, via }): call it inside the cut's transaction, after that transaction's
     reads and BEFORE its first write (it reads the stock's previous leftover, then writes). stock = the stock as the cut writes it
     (id, wPt, hPt, revision after the cut, available), cut = the cut record (at, by, sheetId), sheet = the sheet document,
     plan = the parsed plan (plan.profile = the frontier after this cut), metal = sheet.metal, via = 'nest' | 'library'. */
  async function recordRemnant(tx, { stock, cut, sheet, plan, metal, via } = {}) {
    if (!stock || !cut || !sheet || !plan || !plan.profile || !okId(stock.id)) throw new Error('A cut needs its leftover sheet: the cut was not recorded');
    const revision = +stock.revision, at = +cut.at, by = person(cut.by), sheetId = String(cut.sheetId || sheet.id || sheet.sheetId || '');
    const id = `${stock.id}-${revision}`;
    const sheetName = String((sheetLabel && sheetLabel(sheet)) || sheet.fileBase || sheetId).slice(0, 80), setName = sheet.setId ? String((setLabel && setLabel(sheet.setId)) || (sheet.setSeq ? 'Set ' + sheet.setSeq : '')).slice(0, 40) : '';
    // reads first: the leftover this cut was made on (the stock's previous revision), if it was saved
    const prevRef = revision > 1 ? coll().doc(`${stock.id}-${revision - 1}`) : null, prev = prevRef ? await tx.get(prevRef) : null;
    const g = leftover(plan.profile), usable = !!stock.available && g.areaMm2 > 0, metalKey = String(metal || sheet.metal || 'rose').slice(0, 20);
    const rec = {
      v: 1, metal: metalKey, code: CODES[metalKey] || '', sheetId, sheetName, setId: sheet.setId || null, setName, fileBase: String(sheet.fileBase || sheet.folder || sheetId).slice(0, 120),
      stockId: stock.id, revision, via: via === 'library' ? 'library' : 'nest', cutAt: at, by,
      sheetWMm: g.sheetWMm, sheetHMm: g.sheetHMm, rings: g.rings, areaMm2: g.areaMm2, bboxMm: g.bboxMm,
      status: usable ? 'available' : 'discarded', statusAt: at, statusBy: '', createdAt: FV.serverTimestamp(),
      lastUsedAt: at, lastUsedBy: by, lastUsedSheet: sheetName, lastUsedSheetId: sheetId, ...NOT_HELD   // (the last use of a new leftover is the cut that made it)
    };
    if (!usable) Object.assign(rec, { auto: true, reason: g.areaMm2 > 0 ? 'Too small to reuse' : 'No metal left' });
    const was = prev && prev.exists ? prev.data() || {} : null;
    // the leftover this cut was made on: available or held by this very sheet -> used; used by this sheet (partialUse) keeps that and only dates the use
    if (was && (was.status === 'available' || was.status === 'inUse')) {
      tx.update(prevRef, { status: 'used', statusAt: at, statusBy: by, usedBySheetId: sheetId, usedBySheetName: sheetName, usedAt: at, usedBy: by, marked: false, ...NOT_HELD, lastUsedAt: at, lastUsedBy: by, lastUsedSheet: sheetName, lastUsedSheetId: sheetId });
    } else if (was && was.status === 'used' && was.usedBySheetId === sheetId) {
      tx.update(prevRef, { lastUsedAt: at, lastUsedBy: by, lastUsedSheet: sheetName, lastUsedSheetId: sheetId });
    }
    tx.set(coll().doc(id), rec);
    bump(tx);
    // the metal's regular piece (production only; one small merge, increments only)
    const sr = statsRef && statsWrite && statsWrite() ? statsRef() : null, ps = sr ? pieceStats(sheet) : null;
    if (ps && ps.n > 0) tx.set(sr, { [`${metalKey}_n`]: FV.increment(ps.n), [`${metalKey}_areaMm2`]: FV.increment(r2(ps.areaMm2)), [`${metalKey}_minMm`]: FV.increment(r2(ps.minMm)), [`${metalKey}_maxMm`]: FV.increment(r2(ps.maxMm)), at: Date.now() }, { merge: true });
    return { ...rec, id, createdAt: null };
  }

  /* remnantList { scope: 'available' (default) | 'all', limit (<= 150, default 100 / 60), before (scope all: the cutAt to read older than), ifRev }
     -> { items (newest first), rev, scope, more } or { unchanged: true, rev }. ONE query, with a field mask: 'available' reads every
     available leftover (an equality query: no composite index), 'all' the newest cuts (one ordered field). */
  async function remnantList(b = {}) {
    const state = await revState(), rev = state ? state.rev : null, ifRev = typeof b.ifRev === 'string' && /^[\w.-]{1,40}$/.test(b.ifRev) ? b.ifRev : null;
    if (rev !== null && ifRev !== null && ifRev === rev && b.verify !== true) return { unchanged: true, rev };
    const scope = b.scope === 'all' ? 'all' : 'available', limit = Math.min(150, Math.max(1, Math.floor(+b.limit) || (scope === 'all' ? 60 : 100)));
    let q = scope === 'all' ? coll().orderBy('cutAt', 'desc') : coll().where('status', '==', 'available');
    if (scope === 'all' && Number.isFinite(+b.before) && +b.before > 0) q = q.startAfter(+b.before);
    q = q.limit(limit + 1);
    if (typeof q.select === 'function') q = q.select(...FIELDS);
    const snap = await q.get(), docs = snap.docs.slice(0, limit);
    const items = docs.map(d => clean(d.id, d.data())).sort((a, c) => (c.cutAt || 0) - (a.cutAt || 0) || (c.revision || 0) - (a.revision || 0));
    return { items, rev, scope, more: snap.docs.length > limit, ...(state && !state.backfilled ? { needsBackfill: true } : {}) };
  }

  /* remnantMark { id, status: 'available' | 'used' | 'discarded', by } a person's press on a card (the page sends the signed-in name). A leftover
     a later cut was made on stays used; one that started too small to reuse stays discarded. A person's own mark can be taken back. It
     records the leftover only: it does not change what the nester offers (that stays as the Rose stock says). */
  async function remnantMark(b = {}) {
    const id = String(b.id || ''), status = STATUSES.includes(b.status) ? b.status : null;
    if (!okId(id)) throw new Error('Choose a leftover sheet');
    if (!status) throw new Error('Choose available, used or discarded');
    const by = person(b.by);
    return db.runTransaction(async tx => {
      const ref = coll().doc(id), d = await tx.get(ref);
      if (!d.exists) throw new Error('Leftover sheet not found');
      const r = d.data();
      if (r.status === status) return { ok: true, same: true, item: clean(id, r) };
      if (r.status === 'inUse') throw new Error(`A sheet holds this leftover now${r.inUseBySheetName ? ' (' + r.inUseBySheetName + ')' : ''}: give it back first`);
      if (r.usedBySheetId && !r.marked) throw new Error('A later sheet was cut from this leftover, so it stays used');
      if (r.auto) throw new Error('This leftover is too small to reuse, so it stays discarded');
      // the physical sheet it sits on (read before the first write): its `available` flag is what the Rose Gold nester reads, so it follows the mark
      const stockRef = okId(r.stockId) ? stocks().doc(r.stockId) : null, st = stockRef ? await tx.get(stockRef) : null, sd = st && st.exists ? st.data() : null;
      const patch = { status, statusAt: Date.now(), statusBy: by, marked: true };
      tx.update(ref, patch);
      if (sd && sd.revision === r.revision && !sd.owner) tx.update(stockRef, { available: status === 'available' });
      bump(tx);
      return { ok: true, item: clean(id, { ...r, ...patch }) };
    });
  }

  /* remnantBackfill {}: ONCE, the leftover of each physical sheet as it stands now, for the cuts recorded before leftovers were saved (the page asks
     for it when remnantList says needsBackfill, and when it is done the counter says so for good). Only the CURRENT leftover of each stock that has
     been cut: the stock document already holds its outline (profileJson), its size and its revision; the last cut's own record (not its heavy plan)
     says which sheet and who. Create-only: a leftover already saved is never touched, so it is safe to repeat or to run from two pages at once.
     Production only (the sandbox keeps no counter and no such history). */
  async function remnantBackfill() {
    const mark = revDoc && revDoc(); if (!mark) return { ok: true, skipped: true, created: 0 };
    const was = await mark.get(); if (was.exists && (was.data() || {}).backfilledAt) return { ok: true, already: true, created: 0 };
    const stocks = (await col('Charm_Nest_Rose_Stock').limit(1000).get()).docs.map(d => ({ ...d.data(), id: d.id })).filter(x => +x.revision >= 1 && okId(x.id) && x.profileJson);
    let created = 0;
    for (const x of stocks) {
      const id = `${x.id}-${x.revision}`;
      if ((await coll().doc(id).get()).exists) continue;
      let profile; try { profile = JSON.parse(x.profileJson); Rose.validate(profile, x.wPt, x.hPt); } catch (_) { continue; }
      let cutQ = col('Charm_Nest_Rose_Stock').doc(x.id).collection('cuts').where('revision', '==', x.revision).limit(1);
      if (typeof cutQ.select === 'function') cutQ = cutQ.select('sheetId', 'by', 'at', 'revision', 'fileBase');
      const cut = (await cutQ.get()).docs[0], c = cut ? cut.data() : {};
      const sheetId = String(c.sheetId || x.lastCutSheetId || ''), sheet = okId(sheetId) ? await col('Charm_Nest_Sheets').doc(sheetId).get() : null, sd = sheet && sheet.exists ? sheet.data() : {};
      const g = leftover(profile), metalKey = String(x.metal || sd.metal || 'rose').slice(0, 20), at = +c.at || +x.lastCutAt || 0, usable = x.wPt * x.hPt - Rose.area(profile) > 14 * 14;   // (the stock's own floor for reuse: roseRecordCut's available)
      const rec = {
        v: 1, backfilled: true, metal: metalKey, code: CODES[metalKey] || '', sheetId, sheetName: String((sheetLabel && sheetLabel(sd.id || sd.metal ? sd : null, c.fileBase)) || c.fileBase || x.lastCutLabel || sheetId).slice(0, 80),
        setId: sd.setId || null, setName: sd.setId ? String((setLabel && setLabel(sd.setId)) || (sd.setSeq ? 'Set ' + sd.setSeq : '')).slice(0, 40) : '', fileBase: String(c.fileBase || sd.fileBase || sheetId).slice(0, 120),
        stockId: x.id, revision: +x.revision, via: '', cutAt: at, by: person(c.by || x.lastCutBy), sheetWMm: g.sheetWMm, sheetHMm: g.sheetHMm, rings: g.rings, areaMm2: g.areaMm2, bboxMm: g.bboxMm,
        status: !usable ? 'discarded' : x.owner ? 'inUse' : 'available', statusAt: at, statusBy: '', createdAt: FV.serverTimestamp(),
        lastUsedAt: at, lastUsedBy: person(c.by || x.lastCutBy), lastUsedSheet: '', lastUsedSheetId: sheetId, ...NOT_HELD
      };
      rec.lastUsedSheet = rec.sheetName;
      if (usable && x.owner) Object.assign(rec, { inUseBySheetId: String(x.owner), inUseBySheetName: '', inUseAt: +x.updatedMs || at, inUseBy: '' });   // (a sheet holds its physical sheet right now: one source of truth is the stock's owner)
      if (!usable) Object.assign(rec, { auto: true, reason: g.areaMm2 > 0 ? 'Too small to reuse' : 'No metal left' });
      await db.runTransaction(async tx => { const ref = coll().doc(id); if ((await tx.get(ref)).exists) return; tx.set(ref, rec); created++; });
    }
    const done = { n: FV.increment ? FV.increment(1) : 1, at: Date.now(), backfilledAt: Date.now(), backfilled: created };
    await mark.set(done, { merge: true });
    return { ok: true, created };
  }

  /* ══ PARTIAL SHEETS (PS3) ══ */
  const word = m => Rose.cutWord(m);
  const asMetal = m => { const k = String(m == null ? '' : m); if (!PARTIAL_METALS.includes(k)) throw new Error('Choose Rose Gold, 10K Gold or 14K Gold'); return k; };
  const whoIsIn = r => (r && r.inUseBySheetName) || 'another sheet';

  /* Hooks the Rose stock calls INSIDE its own transactions (roseClaim / roseRelease), so the record's status can never disagree with the stock's owner:
       await sync.read(tx, stockId, revision, partialId?)   -> { ref, id, exists, data } | null    (a read: before the transaction's first write)
       sync.check(rem, { metal, sheetId, stockId, revision })                                       (throws in words: an explicit claim of a partial a sheet holds, a used one ...)
       sync.claimed(tx, rem, { sheetId, sheetName, by, at })  -> the fields it set | null           (available -> inUse, lastUsed* = now)
       sync.released(tx, rem, { sheetId, at })                -> the fields it set | null           (inUse by this sheet -> available again; lastUsedAt is kept) */
  const sync = {
    async read(tx, stockId, revision, partialId) {
      const id = partialId || (+revision >= 1 ? `${stockId}-${+revision}` : null);
      if (!id || !okId(id)) return null;
      const ref = coll().doc(id), snap = await tx.get(ref);
      return { ref, id, exists: snap.exists, data: snap.exists ? snap.data() || {} : null };
    },
    check(rem, { metal, sheetId, stockId, revision }) {
      if (!rem || !rem.exists) throw new Error('Partial sheet not found');
      const r = rem.data;
      if ((r.metal || 'rose') !== metal) throw new Error(`This partial sheet is ${word(r.metal || 'rose')}, not ${word(metal)}`);
      if (r.stockId !== stockId || +r.revision !== +revision) throw new Error('This partial sheet changed. Refresh the list');
      if (r.status === 'inUse' && r.inUseBySheetId !== sheetId) throw new Error(`This partial sheet is in use by ${whoIsIn(r)}`);
      if (r.status === 'used') throw new Error('This partial sheet was already used');
      if (r.status === 'discarded') throw new Error('This partial sheet was discarded');
    },
    claimed(tx, rem, { sheetId, sheetName, by, at }) {
      if (!rem || !rem.exists || rem.data.status !== 'available') return null;   // (held by this sheet already, or not on the list: nothing to change)
      const name = String(sheetName || sheetId).slice(0, 80), who = person(by), patch = { status: 'inUse', inUseBySheetId: sheetId, inUseBySheetName: name, inUseAt: at, inUseBy: who, lastUsedAt: at, lastUsedBy: who, lastUsedSheet: name, lastUsedSheetId: sheetId, statusAt: at };
      tx.update(rem.ref, patch); bump(tx);
      return patch;
    },
    released(tx, rem, { sheetId, at }) {
      if (!rem || !rem.exists) return null;
      const r = rem.data, own = r.status === 'inUse' && r.inUseBySheetId === sheetId, undone = r.status === 'used' && r.usedBySheetId === sheetId && !r.marked;   // (used through partialUse, never cut: giving the sheet back undoes it)
      if (!own && !undone) return null;
      const patch = { status: 'available', statusAt: at, statusBy: '', ...NOT_HELD, ...(undone ? { usedBySheetId: null, usedBySheetName: null, usedAt: null, usedBy: null } : {}) };
      tx.update(rem.ref, patch); bump(tx);
      return patch;
    }
  };

  /* ── the setting (Paul item 6): per metal, reuse partial sheets automatically OR offer a brand new sheet of a size the person sets. One small document,
     config/charmNestPartials { v, n, policies: { rose|gold10k|gold14k: { mode: 'auto'|'new', wMm, hMm, by, at } }, updatedAt }. No document = every metal on 'auto'
     (Rose Gold as it behaves today; 10K and 14K as the cut-line work built them: nest on a leftover of the sheet's own size when one fits, else a fresh sheet). ── */
  const sizeOr = (v, d) => (Number.isFinite(+v) && +v >= SIZE_MM[0] && +v <= SIZE_MM[1] ? Math.round(+v * 10) / 10 : d);
  const policiesOf = d => {
    const out = {};
    for (const m of PARTIAL_METALS) { const p = (d && d.policies && d.policies[m]) || {}; out[m] = { mode: p.mode === 'new' ? 'new' : 'auto', wMm: sizeOr(p.wMm, POLICY_DEFAULT.wMm), hMm: sizeOr(p.hMm, POLICY_DEFAULT.hMm), by: p.by ? String(p.by) : '', at: Number.isFinite(+p.at) ? +p.at : null }; }
    return out;
  };
  const readPolicies = async () => { const ref = configRef && configRef(); if (!ref) return { policies: policiesOf(null), n: 0 }; const s = await ref.get(), d = s.exists ? s.data() || {} : null; return { policies: policiesOf(d), n: d ? +d.n || 0 : 0 }; };
  async function partialPolicyGet() { const r = await readPolicies(); return { policies: r.policies, rev: String(r.n) }; }
  /* partialPolicySet { metal, mode: 'auto' | 'new', wMm, hMm, by }: mode 'new' needs a size (5 to 500 mm each way; the metal's saved one, else 100 x 50, when none is sent);
     going back to 'auto' keeps the size for next time. The page sends the signed-in name. Moves the counter, so the panel's next read finds it. */
  async function partialPolicySet(b = {}) {
    const metal = asMetal(b.metal), mode = b.mode === 'new' ? 'new' : b.mode === 'auto' ? 'auto' : null;
    if (!mode) throw new Error('Choose to reuse partial sheets automatically or to offer a brand new sheet');
    const given = b.wMm != null || b.hMm != null;
    if (given && !(Number.isFinite(+b.wMm) && Number.isFinite(+b.hMm) && +b.wMm >= SIZE_MM[0] && +b.wMm <= SIZE_MM[1] && +b.hMm >= SIZE_MM[0] && +b.hMm <= SIZE_MM[1])) throw new Error(`The new sheet's width and height can each be ${SIZE_MM[0]} to ${SIZE_MM[1]} mm`);
    const ref = configRef && configRef(); if (!ref) throw new Error('The setting cannot be saved here');
    return db.runTransaction(async tx => {
      const s = await tx.get(ref), d = s.exists ? s.data() || {} : null, cur = policiesOf(d);
      const next = { mode, wMm: given ? sizeOr(b.wMm, cur[metal].wMm) : cur[metal].wMm, hMm: given ? sizeOr(b.hMm, cur[metal].hMm) : cur[metal].hMm, by: person(b.by), at: Date.now() };
      const policies = { ...cur, [metal]: next };
      tx.set(ref, { v: 1, n: (d ? +d.n || 0 : 0) + 1, policies, updatedAt: Date.now() });
      bump(tx);
      return { ok: true, policy: next, policies };
    });
  }

  /* ── the list (Paul items 2 and 3): one query per status asked, on metal + status equality (no composite index), field mask, newest USED first ── */
  const typicalOfMetal = (stats, metal) => Partial.typicalFromSums(stats ? { n: stats[`${metal}_n`], areaMm2: stats[`${metal}_areaMm2`], minMm: stats[`${metal}_minMm`], maxMm: stats[`${metal}_maxMm`] } : null);
  const readStats = async () => { const ref = statsRef && statsRef(); if (!ref) return null; const s = await ref.get(); return s.exists ? s.data() || {} : null; };
  function card(id, d, typical) {
    const bb = d.bboxMm || { x: 0, y: 0, w: 0, h: 0 }, usedAt = d.lastUsedAt != null ? +d.lastUsedAt : (+d.cutAt || null);
    const out = {
      id, metal: d.metal || 'rose', code: d.code || '', status: d.status || 'available', outline: d.rings || [], sheetWMm: d.sheetWMm, sheetHMm: d.sheetHMm, bboxMm: bb, wMm: bb.w, hMm: bb.h, areaMm2: d.areaMm2,
      sourceSheet: d.sheetName || '', sourceSet: d.setName || '', sourceSheetId: d.sheetId || '', cutAt: +d.cutAt || null, cutBy: d.by || '',
      lastUsedAt: usedAt, lastUsedBy: d.lastUsedBy != null ? d.lastUsedBy : (d.by || ''), lastUsedSheet: d.lastUsedSheet != null ? d.lastUsedSheet : (d.sheetName || ''), lastUsedSheetId: d.lastUsedSheetId != null ? d.lastUsedSheetId : (d.sheetId || ''),
      stockId: d.stockId, revision: d.revision, wPt: Number.isFinite(+d.sheetWMm) ? +(d.sheetWMm / MM).toFixed(3) : null, hPt: Number.isFinite(+d.sheetHMm) ? +(d.sheetHMm / MM).toFixed(3) : null, estimate: null
    };
    if (d.status === 'inUse') Object.assign(out, { inUseBySheetId: d.inUseBySheetId || '', inUseBySheetName: d.inUseBySheetName || '', inUseAt: d.inUseAt != null ? +d.inUseAt : null });
    if (d.status === 'used') Object.assign(out, { usedBySheetId: d.usedBySheetId || '', usedBySheetName: d.usedBySheetName || '', usedAt: d.usedAt != null ? +d.usedAt : null });
    if (d.reason) out.reason = d.reason;
    if (typical && (out.status === 'available' || out.status === 'inUse') && out.outline.length) { const e = Partial.estimateFit(out.outline, typical, { sheetWMm: d.sheetWMm, sheetHMm: d.sheetHMm }); out.estimate = { pieces: e.pieces, low: e.low, high: e.high, packedPct: e.packedPct }; }
    return out;
  }
  async function readCards(metal, { inUse = false, used = false, limit = 60 } = {}, typical) {
    const statuses = ['available', ...(inUse ? ['inUse'] : []), ...(used ? ['used', 'discarded'] : [])];
    const per = Math.min(100, Math.max(1, Math.floor(+limit) || 60)), items = []; let more = false;
    for (const status of statuses) {
      let q = coll().where('metal', '==', metal).where('status', '==', status).limit(per + 1);
      if (typeof q.select === 'function') q = q.select(...FIELDS);
      const snap = await q.get();
      if (snap.docs.length > per) more = true;
      for (const d of snap.docs.slice(0, per)) items.push(card(d.id, d.data(), typical));
    }
    items.sort((a, c) => (c.lastUsedAt || 0) - (a.lastUsedAt || 0) || (c.cutAt || 0) - (a.cutAt || 0) || (c.revision || 0) - (a.revision || 0));
    return { items, more };
  }
  /* partialList { metal, inUse, used, limit, ifRev, verify, backfill }: the metal's available partials (newest used first); inUse / used add those. Answer
     { items:[card], rev, policies, typical, more } or { unchanged: true, rev } after ONE tiny read of the counter when nothing moved. A full answer costs one read per
     partial listed + the setting + the metal's piece sums. The first call ever also saves the earlier cuts' leftovers (once, create-only: remnantBackfill; `backfill:false` skips). */
  async function partialList(b = {}) {
    const metal = asMetal(b.metal);
    let state = await revState(), rev = state ? state.rev : null;
    const ifRev = typeof b.ifRev === 'string' && /^[\w.-]{1,40}$/.test(b.ifRev) ? b.ifRev : null;
    if (rev !== null && ifRev !== null && ifRev === rev && b.verify !== true) return { unchanged: true, rev };
    let backfilled = null, backfillError = null, reconciled = null;
    if (state && b.backfill !== false && (!state.backfilled || !state.reconciled)) {
      // the first list ever: the earlier cuts' leftovers are saved (create-only), and the records of stocks a sheet already holds say so; each once, then never again
      if (!state.backfilled) { try { const r = await remnantBackfill(); backfilled = r.created || 0; } catch (e) { backfillError = (e && e.message) || String(e); } }
      if (!backfillError) { try { const r = await remnantReconcile(); reconciled = r.changed || 0; } catch (e) { backfillError = (e && e.message) || String(e); } }
      state = await revState() || state; rev = state.rev;
    }
    const [{ items, more }, pol, stats] = await Promise.all([readCards(metal, b, null), readPolicies(), readStats()]);
    const typical = typicalOfMetal(stats, metal);
    for (const c of items) if (c.status === 'available' || c.status === 'inUse') { const e = c.outline.length ? Partial.estimateFit(c.outline, typical, { sheetWMm: c.sheetWMm, sheetHMm: c.sheetHMm }) : null; if (e) c.estimate = { pieces: e.pieces, low: e.low, high: e.high, packedPct: e.packedPct }; }
    return { items, rev, policies: pol.policies, typical, more, ...(backfilled !== null ? { backfilled } : {}), ...(reconciled ? { reconciled } : {}), ...(backfillError ? { backfillError } : {}) };
  }

  /* partialPlan { metal, pieces: [{areaMm2, wMm?, hMm?}] | {areaMm2, count}, order }: which available partials these pieces would take, filled one after the other, and
     whether they all fit. An ESTIMATE (the real fit is the nester's, partial by partial). One query + the piece sums. */
  async function partialPlan(b = {}) {
    const metal = asMetal(b.metal), stats = await readStats(), typical = typicalOfMetal(stats, metal), { items } = await readCards(metal, { limit: 100 }, null);
    const plan = Partial.planFor(items, b.pieces, { order: b.order, typical });
    return { ...plan, metal, available: items.length };
  }

  /* ── claim / release / use (Paul items 4 and 5). ONE source of truth for who holds a partial: the Rose stock's owner. Claim and release go through
     roseClaim / roseRelease (their transactions keep the record in step: `sync`); use is its own transaction on the record and the stock. ── */
  async function partialClaim(b = {}) {
    if (!stockApi) throw new Error('Partial sheets are not ready');
    const metal = asMetal(b.metal), id = String(b.id || ''), sheetId = String(b.sheetId || '');
    if (!okId(id) || !okId(sheetId)) throw new Error('Choose a partial sheet and a sheet to nest on it');
    const snap = await coll().doc(id).get();   // (one read to learn its physical sheet; the claim's own transaction checks everything again)
    sync.check({ exists: snap.exists, data: snap.exists ? snap.data() || {} : null }, { metal, sheetId, stockId: snap.exists ? (snap.data() || {}).stockId : '', revision: snap.exists ? (snap.data() || {}).revision : 0 });
    const r = snap.data();
    const out = await stockApi.roseClaim({ sheetId, metal, stockId: r.stockId, revision: r.revision, wPt: r.sheetWMm / MM, hPt: r.sheetHMm / MM, exact: true, partialId: id, nesting: b.nesting !== false, by: person(b.by), sheetName: b.sheetName, swap: b.swap === true });
    return { ...out, partial: { id, status: 'inUse', ...(out.partial || {}), inUseBySheetId: sheetId } };
  }
  async function partialRelease(b = {}) {
    if (!stockApi) throw new Error('Partial sheets are not ready');
    const sheetId = String(b.sheetId || '');
    if (!okId(sheetId)) throw new Error('Choose a sheet');
    const held = await stocks().where('owner', '==', sheetId).limit(1).get(), d = held.docs[0];
    if (!d) return { ok: true, released: false, partialId: null };
    const stock = d.data() || {}, partialId = +stock.revision >= 1 ? `${d.id}-${+stock.revision}` : null;
    await stockApi.roseRelease({ stockId: d.id, sheetId, by: person(b.by) });
    return { ok: true, released: true, stockId: d.id, partialId };
  }
  async function partialUse(b = {}) {
    const id = String(b.id || ''), sheetId = String(b.sheetId || ''), by = person(b.by);
    if (!okId(id) || !okId(sheetId)) throw new Error('Choose a partial sheet and the sheet that used it');
    return db.runTransaction(async tx => {
      const ref = coll().doc(id), d = await tx.get(ref);
      if (!d.exists) throw new Error('Partial sheet not found');
      const r = d.data() || {}, stockRef = okId(r.stockId) ? stocks().doc(r.stockId) : null, st = stockRef ? await tx.get(stockRef) : null, stock = st && st.exists ? st.data() : null;
      if (r.status === 'used' && r.usedBySheetId === sheetId) return { ok: true, same: true, partial: card(id, r, null) };
      if (r.status === 'inUse' && r.inUseBySheetId !== sheetId) throw new Error(`This partial sheet is in use by ${whoIsIn(r)}`);
      if (r.status === 'used') throw new Error('This partial sheet was already used');
      if (r.status === 'discarded') throw new Error('This partial sheet was discarded');
      if (stock && (stock.revision !== r.revision || (stock.owner && stock.owner !== sheetId))) throw new Error(stock.owner && stock.owner !== sheetId ? 'This partial sheet is in use by another sheet' : 'This partial sheet changed. Refresh the list');
      const at = Date.now(), name = String(b.sheetName || (r.status === 'inUse' && r.inUseBySheetName) || sheetId).slice(0, 80);
      const patch = { status: 'used', statusAt: at, statusBy: by, usedBySheetId: sheetId, usedBySheetName: name, usedAt: at, usedBy: by, marked: false, ...NOT_HELD, lastUsedAt: at, lastUsedBy: by, lastUsedSheet: name, lastUsedSheetId: sheetId };
      tx.update(ref, patch);
      if (stock && stock.available) tx.update(stockRef, { available: false });   // (a sheet that holds it keeps its claim until its cut or its release: the owner stays)
      bump(tx);
      return { ok: true, partial: card(id, { ...r, ...patch }, null) };
    });
  }
  /* partialStocks { ids: [partialId...] (at most 20) }: the physical sheets under those partials, for a trial pack or a claim (a card carries the outline; the solver packs against the
     stock's profile). Only the ids asked: the stock documents, once each (a revision's profile never changes, so the page keeps what it read). A partial id is {stockId}-{revision}.
     -> { stocks: { [partialId]: { stockId, revision, wPt, hPt, profileJson, metal, held, available, current } | { missing: true } } } (current: the stock is still at that revision) */
  async function partialStocks(b = {}) {
    const ids = [...new Set((Array.isArray(b.ids) ? b.ids : []).map(String))].filter(okId).slice(0, 20), out = {};
    const parts = ids.map(id => { const m = /^(.+)-(\d+)$/.exec(id); return m && okId(m[1]) ? { id, stockId: m[1], revision: +m[2] } : null; });
    const docs = await Promise.all(parts.map(x => (x ? stocks().doc(x.stockId).get() : null)));
    ids.forEach((id, i) => {
      const x = parts[i], d = docs[i];
      if (!x || !d || !d.exists) { out[id] = { missing: true }; return; }
      const s = d.data() || {};
      out[id] = { stockId: x.stockId, revision: s.revision, wPt: s.wPt, hPt: s.hPt, profileJson: s.profileJson || null, metal: s.metal || 'rose', held: !!s.owner, available: !!s.available, current: +s.revision === x.revision };
    });
    return { stocks: out };
  }
  const bind = s => { stockApi = s; };

  /* remnantReconcile {}: ONCE, for what was true before the stock's claims kept the records in step: a record that says 'available' while a sheet already holds its physical sheet
     (every Rose Gold sheet holds one from its nest on) becomes 'inUse', so the Partial Sheet list never offers a partial a sheet is nested on. Each record is changed in a transaction
     that reads the stock again (the stock's owner is the one truth); the marker on the counter document says it is done. Production only (the sandbox keeps no counter). */
  async function remnantReconcile() {
    const mark = revDoc && revDoc(); if (!mark) return { ok: true, skipped: true, changed: 0 };
    const was = await mark.get(); if (was.exists && (was.data() || {}).reconciledAt) return { ok: true, already: true, changed: 0 };
    const held = (await stocks().where('available', '==', false).limit(1000).get()).docs.map(d => ({ ...d.data(), id: d.id })).filter(x => x.owner && +x.revision >= 1 && okId(x.id));
    let changed = 0;
    for (const x of held) {
      await db.runTransaction(async tx => {
        const sref = stocks().doc(x.id), rref = coll().doc(`${x.id}-${+x.revision}`), st = await tx.get(sref), rd = await tx.get(rref);
        const stock = st.exists ? st.data() || {} : null, rec = rd.exists ? rd.data() || {} : null;
        if (!stock || !rec || !stock.owner || +stock.revision !== +x.revision || rec.status !== 'available') return;
        const sh = await tx.get(col('Charm_Nest_Sheets').doc(String(stock.owner))), name = sh.exists && sheetLabel ? String(sheetLabel(sh.data()) || '').slice(0, 80) : '';
        tx.update(rref, { status: 'inUse', inUseBySheetId: String(stock.owner), inUseBySheetName: name, inUseAt: Date.now(), inUseBy: '', statusAt: Date.now() });
        changed++;
      });
    }
    await mark.set({ n: FV.increment ? FV.increment(1) : 1, at: Date.now(), reconciledAt: Date.now(), reconciled: changed }, { merge: true });
    return { ok: true, changed };
  }

  return { recordRemnant, sync, bind, ops: { remnantList, remnantMark, remnantBackfill, partialList, partialPolicyGet, partialPolicySet, partialClaim, partialRelease, partialUse, partialPlan, partialStocks, partialBackfill: remnantBackfill } };
};
module.exports.leftover = leftover;
module.exports.FIELDS = FIELDS;
module.exports.PARTIAL_METALS = PARTIAL_METALS;
