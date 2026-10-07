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
   The counter moves in the transaction of every write (production only, as the Library's own counter: the sandbox keeps none). */
const Rose = require('../../charm-nest-rose');
const COLL = 'Charm_Nest_Remnants';
const MM = Rose.MM;
const CODES = { rose: 'RG', gold10k: '10K', gold14k: '14K', gold: 'GF', silver: 'SS' };
const FIELDS = ['v', 'metal', 'code', 'sheetId', 'sheetName', 'setId', 'setName', 'fileBase', 'stockId', 'revision', 'via', 'cutAt', 'by', 'sheetWMm', 'sheetHMm', 'rings', 'areaMm2', 'bboxMm',
  'status', 'statusAt', 'statusBy', 'marked', 'auto', 'reason', 'usedBySheetId', 'usedBySheetName', 'usedAt', 'usedBy', 'createdAt'];
const STATUSES = ['available', 'used', 'discarded'];
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
module.exports = function ({ db, col, FV, sheetLabel, setLabel, revDoc }) {
  const coll = () => col(COLL);
  const bump = tx => { const ref = revDoc && revDoc(); if (ref && FV.increment) tx.set(ref, { n: FV.increment(1), at: Date.now() }, { merge: true }); };
  const revNow = async () => { const ref = revDoc && revDoc(); if (!ref) return null; const s = await ref.get(); return String(s.exists ? Number((s.data() || {}).n) || 0 : 0); };
  const ms = t => (t && typeof t.toMillis === 'function' ? t.toMillis() : typeof t === 'number' ? t : null);
  const clean = (id, d) => { const o = { id }; for (const k of FIELDS) if (d[k] !== undefined) o[k] = d[k]; o.createdAt = ms(d.createdAt); return o; };

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
      status: usable ? 'available' : 'discarded', statusAt: at, statusBy: '', createdAt: FV.serverTimestamp()
    };
    if (!usable) Object.assign(rec, { auto: true, reason: g.areaMm2 > 0 ? 'Too small to reuse' : 'No metal left' });
    if (prev && prev.exists && (prev.data() || {}).status === 'available') {
      tx.update(prevRef, { status: 'used', statusAt: at, statusBy: by, usedBySheetId: sheetId, usedBySheetName: sheetName, usedAt: at, usedBy: by, marked: false });
    }
    tx.set(coll().doc(id), rec);
    bump(tx);
    return { ...rec, id, createdAt: null };
  }

  /* remnantList { scope: 'available' (default) | 'all', limit (<= 150, default 100 / 60), before (scope all: the cutAt to read older than), ifRev }
     -> { items (newest first), rev, scope, more } or { unchanged: true, rev }. ONE query, with a field mask: 'available' reads every
     available leftover (an equality query: no composite index), 'all' the newest cuts (one ordered field). */
  async function remnantList(b = {}) {
    const rev = await revNow(), ifRev = typeof b.ifRev === 'string' && /^[\w.-]{1,40}$/.test(b.ifRev) ? b.ifRev : null;
    if (rev !== null && ifRev !== null && ifRev === rev && b.verify !== true) return { unchanged: true, rev };
    const scope = b.scope === 'all' ? 'all' : 'available', limit = Math.min(150, Math.max(1, Math.floor(+b.limit) || (scope === 'all' ? 60 : 100)));
    let q = scope === 'all' ? coll().orderBy('cutAt', 'desc') : coll().where('status', '==', 'available');
    if (scope === 'all' && Number.isFinite(+b.before) && +b.before > 0) q = q.startAfter(+b.before);
    q = q.limit(limit + 1);
    if (typeof q.select === 'function') q = q.select(...FIELDS);
    const snap = await q.get(), docs = snap.docs.slice(0, limit);
    const items = docs.map(d => clean(d.id, d.data())).sort((a, c) => (c.cutAt || 0) - (a.cutAt || 0) || (c.revision || 0) - (a.revision || 0));
    return { items, rev, scope, more: snap.docs.length > limit };
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
      if (r.usedBySheetId && !r.marked) throw new Error('A later sheet was cut from this leftover, so it stays used');
      if (r.auto) throw new Error('This leftover is too small to reuse, so it stays discarded');
      const patch = { status, statusAt: Date.now(), statusBy: by, marked: true };
      tx.update(ref, patch);
      bump(tx);
      return { ok: true, item: clean(id, { ...r, ...patch }) };
    });
  }

  return { recordRemnant, ops: { remnantList, remnantMark } };
};
module.exports.leftover = leftover;
module.exports.FIELDS = FIELDS;
