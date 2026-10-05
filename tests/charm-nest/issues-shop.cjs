/* Synthetic shops for the Library's order-issue checks (issues-property.cjs, issues-server.cjs).
 *
 * A SPEC describes a shop in the shop's own words (sheets with their set and their own trouble, orders with lines, copies and
 * where each copy is); materialize(spec) builds the RAW records exactly as the app and the server keep them (run line
 * records, sheet records, set records, Charm_Pool records), without ever asking the code under test. The ORACLE
 * (issues-oracle.cjs) reads only those raw records, so a wrong spec->record step cannot hide a wrong answer: both sides
 * read the same records.
 *
 * Realistic staleness that real data has and that the old code stumbled on:
 *   - a line's state stays 'pooled' in the run record until its set is written, though every piece is already on a sheet
 *   - a piece listed on an ARCHIVED (superseded) sheet record as well as the live one it moved to a second ago
 *   - a cancelled ('gone') line whose piece is still on a released sheet
 *   - a line with fewer pool ids than its quantity (copies not pooled), quantity > 1 spread over sheets
 *   - a piece with a leftover problems[] while it is already on a sheet
 *   - a held / change-pending line, nested or not
 *   - a piece COMPLETED BY HAND (Review "Complete Order", or its QR label printed from Custom Orders; Paul, 5 Oct round 6): the custom order's own
 *     record (Charm_Custom_Orders, shop.customs[lineKey]) says so, while the run's copy of the line may still say 'unmatched' / 'waiting' (stale),
 *     'noDesign' with a hint the page wrote (handDone), or 'noDesign' flagged (an older save); a reopened one (state 'open'), one sent to the sheets
 *     with its own designs (how 'sheet'), a held one, one that sits on a sheet and a cancelled one are the other shapes
 */
'use strict';

function rng(seed) {
  let a = seed >>> 0;
  const r = () => { a |= 0; a = (a + 0x6D2B79F5) | 0; let t = Math.imul(a ^ (a >>> 15), 1 | a); t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t; return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
  r.int = (lo, hi) => lo + Math.floor(r() * (hi - lo + 1));
  r.pick = xs => xs[Math.floor(r() * xs.length)];
  r.chance = p => r() < p;
  r.weighted = table => { let t = 0; for (const [, w] of table) t += w; let x = r() * t; for (const [v, w] of table) { x -= w; if (x < 0) return v; } return table[table.length - 1][0]; };
  r.shuffle = xs => { const a2 = xs.slice(); for (let i = a2.length - 1; i > 0; i--) { const j = Math.floor(r() * (i + 1)); [a2[i], a2[j]] = [a2[j], a2[i]]; } return a2; };
  return r;
}

const METALS = ['gold', 'silver', 'rose', 'gold14k'];
const OWN = [['ok', 46], ['unverified', 6], ['noQr', 5], ['qrPartial', 4], ['held', 5], ['draft', 6], ['solidExcluded', 3], ['dirty', 3], ['saving', 2],
  ['done', 6], ['reopened', 4], ['backUnsaved', 6], ['noFront', 3], ['roseNoPlan', 2], ['error', 2], ['unidentified', 3]];
const KINDS = [['nested', 52], ['unnestedPooled', 9], ['unnestedPulled', 5], ['problem', 10], ['noDesign', 4], ['gone', 3], ['held', 4], ['partial', 6], ['staleProblemNested', 2], ['goneNoPiece', 1], ['lostPoolIds', 4], ['archivedOnly', 3],
  ['handDone', 6], ['handReopened', 2], ['handSheet', 2], ['handHeld', 2], ['handGone', 1]];
const HANDS = new Set(['handDone', 'handReopened', 'handSheet', 'handHeld', 'handGone']);
const PROBLEMS = [['unmatchedSku', 'sku'], ['unmatchedSku', ''], ['missingSize', 'sku'], ['blockedSku', 'sku'], ['needsMaterial', ''], ['needsMapping', 'sku']];

/** A random shop spec. o: { maxOrders, maxSheets, dual (a piece on two live sheets), sandbox } */
function makeSpec(seed, o = {}) {
  const r = rng(seed), spec = { seed, sandbox: !!o.sandbox || r.chance(0.2), sheets: [], orders: [], archivedSheets: [] };
  const metals = r.shuffle(METALS.slice(0, 3)).slice(0, r.weighted([[1, 30], [2, 45], [3, 25]]));
  if (r.chance(0.1)) metals.push('gold14k');
  const maxSheets = o.maxSheets || 7;
  for (const m of metals) {
    const n = r.weighted([[1, 55], [2, 33], [3, 12]]);
    for (let i = 1; i <= n && spec.sheets.length < maxSheets; i++) spec.sheets.push({ id: `sh-${m}-${i}`, metal: m, index: i, own: r.weighted(OWN), setId: null });
  }
  // sets: sheets are grouped at random; a draft sheet is in none
  const groups = r.int(1, Math.max(1, Math.ceil(spec.sheets.length / 2)));
  spec.sheets.forEach(s => { s.setId = s.own === 'draft' ? null : `set-${r.int(1, groups)}`; });
  const byMetal = m => spec.sheets.filter(s => s.metal === m);
  const nOrders = r.int(1, o.maxOrders || 14);
  let tx = 7000;
  for (let i = 0; i < nOrders; i++) {
    const oid = String(4170000000 + r.int(1000, 999999) * 10 + (i % 10)), order = { id: oid, buyer: `Buyer ${i}`, lines: [] };
    if (spec.orders.some(x => x.id === oid)) continue;
    const pieces = r.weighted([[1, 40], [2, 33], [3, 17], [4, 10]]), nLines = Math.min(pieces, r.weighted([[1, 45], [2, 40], [3, 15]]));
    const qs = Array(nLines).fill(1); for (let k = nLines; k < pieces; k++) qs[r.int(0, nLines - 1)]++;
    for (const q of qs) {
      const metal = r.pick(metals), kind = r.weighted(KINDS.filter(k => !(o.noLost && k[0] === 'lostPoolIds') && !(o.noHand && HANDS.has(k[0])))), line = { tx: ++tx, metal, q, kind, copies: [], state: 'pulled', problems: [], sku: 'SKU-' + tx, hold: null, change: false, engrave: r.weighted([['plain', 55], ['approved', 33], ['unapproved', 12]]), noDesign: false, stale: false };
      const sheetsOf = byMetal(metal), pickSheet = () => (sheetsOf.length ? r.pick(sheetsOf).id : null);
      const nested = () => Array.from({ length: q }, () => ({ sheet: pickSheet(), pooled: true }));
      const state = () => r.weighted([['pooled', 40], ['written', 38], ['labelled', 12], ['committed', 10]]);
      switch (kind) {
        case 'nested': line.copies = nested(); line.state = state(); line.stale = r.chance(0.25); break;
        case 'partial': line.copies = nested().map((c, i) => (i === 0 || r.chance(0.4) ? c : { sheet: null, pooled: r.chance(0.5) })); line.state = 'pooled'; break;
        case 'unnestedPooled': line.copies = Array.from({ length: q }, () => ({ sheet: null, pooled: true })); line.state = r.pick(['pooled', 'pooled', 'waiting']); break;
        case 'unnestedPulled': line.copies = Array.from({ length: q }, () => ({ sheet: null, pooled: false })); line.state = r.pick(['pulled', 'waiting', 'oversize', 'held']); break;
        case 'problem': {
          const [kindName, hasSku] = r.pick(PROBLEMS); line.problems = [kindName]; if (r.chance(0.15)) line.problems.push('needsMapping'); line.sku = hasSku ? line.sku : '';
          line.copies = Array.from({ length: q }, () => ({ sheet: null, pooled: false })); line.state = r.pick(['unmatched', 'waiting']); break;
        }
        case 'lostPoolIds': line.copies = nested(); line.lost = true; line.state = r.pick(['pulled', 'pooled', 'written']); break;
        case 'archivedOnly': line.copies = Array.from({ length: q }, (_, i) => (i === 0 || r.chance(0.5) ? { sheet: null, pooled: true, archivedOn: true } : { sheet: null, pooled: true })); line.state = 'pooled'; break;
        case 'staleProblemNested': line.problems = ['unmatchedSku']; line.copies = nested(); line.state = 'written'; break;
        case 'noDesign': line.noDesign = true; line.state = 'noDesign'; line.engrave = 'plain'; line.copies = Array.from({ length: q }, () => ({ sheet: null, pooled: false })); break;
        case 'gone': line.state = 'gone'; line.engrave = 'plain'; line.copies = nested().map(c => (r.chance(0.6) ? c : { sheet: null, pooled: false })); break;
        case 'goneNoPiece': line.state = 'gone'; line.engrave = 'plain'; line.copies = Array.from({ length: q }, () => ({ sheet: null, pooled: false })); line.problems = r.chance(0.5) ? ['unmatchedSku'] : []; break;
        case 'handDone': {
          // Paul's order: the chain-only piece, completed by hand, on no sheet. rec = how the run's copy of the line still reads
          const how = r.pick(['button', 'print']), rec = r.weighted([['stale', 50], ['hinted', 25], ['legacy', 25]]);
          line.hand = { how, state: 'completed', rec }; line.engrave = 'plain'; line.copies = Array.from({ length: q }, () => ({ sheet: null, pooled: false }));
          if (rec === 'stale') { line.state = r.pick(['unmatched', 'unmatched', 'waiting', 'held']); line.problems = ['unmatchedSku']; if (r.chance(0.7)) line.sku = ''; }
          else if (rec === 'hinted') line.state = 'noDesign';
          else { line.state = 'noDesign'; line.noDesign = true; }
          break;
        }
        case 'handReopened': {
          // completed, then reopened: a piece again (its record may still carry the hint the page wrote while it was completed)
          const rec = r.pick(['hinted', 'stale']);
          line.hand = { how: r.pick(['button', 'print']), state: 'open', rec }; line.engrave = 'plain'; line.copies = Array.from({ length: q }, () => ({ sheet: null, pooled: false }));
          if (rec === 'stale') { line.state = r.pick(['unmatched', 'waiting']); line.problems = ['unmatchedSku']; if (r.chance(0.6)) line.sku = ''; } else line.state = 'noDesign';
          break;
        }
        case 'handSheet': {
          // sent to the sheets with its own designs (how 'sheet'): cut, not completed by hand; it still waits for its place
          line.hand = { how: 'sheet', state: 'completed', rec: 'stale' }; line.engrave = 'plain'; line.state = r.pick(['pooled', 'waiting']);
          line.copies = Array.from({ length: q }, () => ({ sheet: null, pooled: r.chance(0.5) })); break;
        }
        case 'handHeld': {
          // completed by hand, but a person (or Etsy) also holds it: the stop still holds the sheets of its order
          line.hand = { how: r.pick(['button', 'print']), state: 'completed', rec: 'stale' }; line.engrave = 'plain'; line.hold = 'Customer changed the order'; line.change = r.chance(0.5);
          line.copies = Array.from({ length: q }, () => ({ sheet: null, pooled: false })); line.state = 'held'; line.problems = ['unmatchedSku']; line.sku = ''; break;
        }
        case 'handGone': {
          line.hand = { how: r.pick(['button', 'print']), state: 'completed', rec: 'stale' }; line.state = 'gone'; line.engrave = 'plain'; line.problems = ['unmatchedSku'];
          line.copies = Array.from({ length: q }, () => ({ sheet: null, pooled: false })); break;
        }
        case 'held':
          line.hold = 'Customer changed the order'; line.change = r.chance(0.5);
          line.copies = r.chance(0.5) ? nested() : Array.from({ length: q }, () => ({ sheet: null, pooled: false })); line.state = line.copies[0].sheet ? 'written' : 'held'; break;
      }
      if (o.dual && line.copies.some(c => c.sheet) && r.chance(0.3)) { const c = line.copies.find(x => x.sheet), alt = sheetsOf.filter(s => s.id !== c.sheet); if (alt.length) c.dual = r.pick(alt).id; }
      if (line.copies.some(c => c.sheet) && r.chance(0.12)) line.staleArchive = true;
      order.lines.push(line);
    }
    spec.orders.push(order);
  }
  // trouble in the sheet RECORDS themselves, as real data has it: an order listed on a sheet that holds none of its pieces any
  // more, a manual charm that belongs to no order, a pool id listed twice
  for (const sh of spec.sheets) {
    if (spec.orders.length && r.chance(0.08)) sh.staleOrder = r.pick(spec.orders).id;
    if (r.chance(o.ghost ? 0.2 : 0)) sh.ghostOrder = '4179' + String(r.int(100000, 999999));
    if (r.chance(0.04)) sh.orphan = true;
    if (r.chance(0.04)) sh.dupPool = true;
  }
  return spec;
}

const keyOf = (o, l) => `${o.id}_${l.tx}`;
const pidOf = (o, l, i) => `${keyOf(o, l)}_${i + 1}`;

/** Raw records (the shop) from a spec. */
function materialize(spec) {
  const lines = {}, placed = new Map(spec.sheets.map(s => [s.id, []])), pool = [], customs = {};
  const archivedSheets = [], archivedOnly = [];
  for (const o of spec.orders) for (const l of o.lines) {
    const key = keyOf(o, l), poolIds = [];
    // the custom order's own record, as Charm_Custom_Orders keeps it (state 'completed' | 'open', how 'button' | 'print' | 'sheet')
    if (l.hand) customs[key] = { state: l.hand.state, how: l.hand.how, completedAt: 1700000000000 + l.tx, completedBy: 'Seth' };
    l.copies.forEach((c, i) => {
      const pid = pidOf(o, l, i);
      if ((c.sheet || c.pooled) && !l.lost) poolIds.push(pid);
      if (c.sheet && placed.has(c.sheet)) placed.get(c.sheet).push(pid);
      if (c.dual && placed.has(c.dual)) placed.get(c.dual).push(pid);
      if (c.archivedOn) archivedOnly.push({ pid, o, l });
      pool.push({ poolId: pid, lineKey: key, sheetId: c.sheet || null, state: c.sheet ? 'nested' : 'pooled' });
    });
    const plain = l.engrave === 'plain';
    lines[key] = {
      orderId: o.id, transactionId: String(l.tx), state: l.state, quantity: l.q, poolIds, reason: null, hold: l.hold || null, changePending: !!l.change, sku: l.sku, material: l.metal,
      problems: l.problems.slice(), noDesign: !!l.noDesign, engraveCandidate: !plain,
      engrave: plain ? null : l.engrave === 'approved' ? { needed: true, state: 'approved', approved: true, approvedAt: 10, approvedBy: 'Paul' } : { needed: true, state: 'review', approved: false },
      snap: { title: `Charm ${l.tx}`, buyer: o.buyer, metalKey: l.metal }
    };
    // the hint the page wrote into the run's copy while the piece was completed by hand (the server only uses it to know which line to look up)
    if (l.hand && l.hand.rec === 'hinted') lines[key].handDone = { at: customs[key].completedAt, by: 'Seth', how: l.hand.how === 'button' ? 'button' : 'print' };
  }
  const sheets = [];
  const oidOfPid = pid => String(pid).split('_')[0];
  for (const s of spec.sheets) {
    const ids = placed.get(s.id).slice();
    if (s.orphan) ids.push('manual_' + s.id + '_1');
    const orders = [...new Set(ids.map(oidOfPid).filter(x => /^\d+$/.test(x)))];
    if (s.staleOrder && spec.orders.some(o => o.id === s.staleOrder) && !orders.includes(s.staleOrder)) orders.push(s.staleOrder);
    if (s.dupPool && ids.length) ids.push(ids[0]);
    if (s.ghostOrder) orders.push(s.ghostOrder);
    const backs = ids.filter(pid => { const l = lines[pid.slice(0, pid.lastIndexOf('_'))]; return l && l.engrave && l.engrave.approved && l.state !== 'gone'; })
      .map(pid => ({ poolId: pid, sheetId: s.id, approvedAt: 10, approvedBy: 'Paul', verified: { geometry: { ok: true }, file: { ok: true } }, outputs: { ai: { path: pid + '.ai', url: 'https://example.test/' + pid + '.ai' } } }));
    const rec = {
      id: s.id, metal: s.metal, metalLabel: s.metal, setId: s.setId, setSeq: 0, sheetIndex: s.index, runId: 'run-1', status: 'complete', placedCount: new Set(ids).size,
      poolIds: ids, orders, verification: { ok: true }, preview: 'https://example.test/' + s.id + '.png', outputs: { ai: { path: s.id + '.ai', url: 'https://example.test/' + s.id + '.ai' }, preview: { path: s.id + '.png', url: 'https://example.test/' + s.id + '.png' } },
      label: { files: [{ path: s.id + '-qr.png', url: 'https://example.test/' + s.id + '-qr.png', payload: 'x', orders }] }, backPool: backs, draft: false
    };
    if (s.metal === 'rose') { rec.roseStockId = 'stock-1'; rec.rosePlanHash = 'plan-1'; }
    switch (s.own) {
      case 'unverified': rec.verification = s.index % 2 ? { ok: false } : null; break;
      case 'noQr': rec.label = { files: [] }; break;
      case 'qrPartial': if (orders.length > 1) rec.label.files[0].orders = orders.slice(1); else rec.label = { files: [] }; break;
      case 'held': rec.laserHold = { at: 1000, by: 'Paul', note: '' }; break;
      case 'draft': rec.draft = true; rec.setId = null; break;
      case 'solidExcluded': rec.solidIncluded = false; break;
      case 'dirty': rec.dirty = true; break;
      case 'saving': rec.saving = true; break;
      case 'done': rec.laserDoneAt = 5000; rec.laserDoneBy = 'Seth'; break;
      case 'reopened': rec.processSeals = [{ id: 'laserDone-5000-0', how: 'laserDone', at: 5000, by: 'Seth' }]; break;
      case 'backUnsaved': if (rec.backPool.length) rec.backPool[0] = { ...rec.backPool[0], outputs: null }; break;
      case 'noFront': delete rec.outputs.ai; break;
      case 'roseNoPlan': if (s.metal === 'rose') delete rec.rosePlanHash; break;
      case 'error': rec.status = 'error'; break;
      case 'unidentified': rec.placedCount += 1; break;
    }
    sheets.push(rec);
  }
  // superseded (archived) sheet records that still list the pieces they held before a re-nest
  for (const o of spec.orders) for (const l of o.lines) if (l.staleArchive) {
    const pids = l.copies.map((c, i) => (c.sheet ? pidOf(o, l, i) : null)).filter(Boolean), id = `old-${keyOf(o, l)}`;
    sheets.push({ id, metal: l.metal, archived: true, setId: null, runId: 'run-1', sheetIndex: 9, poolIds: pids, orders: [o.id], placedCount: pids.length, status: 'complete', verification: { ok: true }, label: { files: [] } });
  }
  for (const a of archivedOnly) sheets.push({ id: `gone-${a.pid}`, metal: a.l.metal, archived: true, setId: null, runId: 'run-1', sheetIndex: 8, poolIds: [a.pid], orders: [a.o.id], placedCount: 1, status: 'complete', verification: { ok: true }, label: { files: [] } });
  const sets = [...new Set(sheets.filter(s => s.setId && !s.archived).map(s => s.setId))].map((setId, i) => ({ setId, seq: i + 1, sheetIds: sheets.filter(s => s.setId === setId && !s.archived).map(s => s.id) }));
  // each sheet says which set it is in the way the Library does (the set's number), so an order split between two sets can name both
  for (const sh of sheets) { const z = sets.find(x => x.setId === sh.setId); if (z) sh.setSeq = z.seq; }
  // part of the lines sit in the run's line archive (the server reads them from there)
  const archivedLines = {}, live = {};
  Object.keys(lines).sort().forEach((k, i) => { ((spec.seed + i) % 5 === 0 ? archivedLines : live)[k] = lines[k]; });
  return { sandbox: spec.sandbox, lines: live, archivedLines, sheets, sets, pool, customs };
}

/** The oracle's and the harness's one reading of a custom order's record: completed by hand = state not 'open' and not sent to the sheets. */
const isHandDoc = c => !!c && typeof c === 'object' && c.state !== 'open' && c.how !== 'sheet';

/** The page's rows (Orders.rows() after interpretAll) from the line records: rowFromRecord's shape plus the spec.
 *  A custom order the page read as completed (B.maps.customDone: state not 'open') is spec.customDone and, as interpretLine reads it, a line with
 *  nothing to cut (spec.noDesign, state 'noDesign' unless a person holds it); a reopened one (customKept) is read afresh as a piece again. */
function uiRowsFresh(shop) {
  const all = { ...shop.archivedLines, ...shop.lines }, customs = shop.customs || {};
  return Object.keys(all).sort().map(key => {
    const l = all[key], s2 = l.snap || {}, doc = customs[key] && customs[key].state !== 'open' ? { ...customs[key] } : null, done = !!doc && doc.how !== 'sheet';
    const order = { receiptId: String(l.orderId), orderNumber: String(l.orderId), buyer: { name: s2.buyer || '' }, lines: [] };
    const line = { transactionId: String(l.transactionId), listingId: '', sku: l.sku || '', title: s2.title || '', quantity: +l.quantity || 1, metalKey: s2.metalKey || '' };
    order.lines = [line];
    // (a leftover 'noDesign' that only a completion explained is read afresh once the completion is gone)
    let state = l.state === 'noDesign' && !l.noDesign && !done ? 'pulled' : l.state;
    if (done && state !== 'gone' && !l.hold && !(l.poolIds || []).length) state = 'noDesign';
    const noDesign = done ? true : !!l.noDesign;
    return { key, order, line, spec: { designSku: l.sku || null, quantity: +l.quantity || 1, noDesign, engraveCandidate: !!l.engraveCandidate, problems: (l.problems || []).map(k => ({ kind: k })), material: l.material, ...(doc ? { customDone: doc } : {}) },
      problems: (l.problems || []).map(k => ({ kind: k })), state, reason: l.reason, hold: l.hold || null, changePending: !!l.changePending, poolIds: (l.poolIds || []).slice(), engrave: l.engrave ? { ...l.engrave } : null, metal: l.material };
  });
}
/** The server's rows: the run's line records with their key (op_laserStatus passes these to orderReports), as productionReadiness leaves them after it
 *  looked each unplaced line's own custom order up: a line the server's record says was completed by hand carries handDone; a hint the page wrote that the
 *  record no longer backs (reopened since) is dropped, and the leftover 'noDesign' state it explained is a piece again. issues-server.cjs runs the real
 *  handler over the same records; this is the shape its answer is read from. */
function recordRowsFresh(shop) {
  const all = { ...shop.archivedLines, ...shop.lines }, customs = shop.customs || {};
  return Object.keys(all).sort().map(key => {
    const rec = { ...JSON.parse(JSON.stringify(all[key])), key, orderId: String(all[key].orderId) }, c = customs[key];
    const read = rec.state !== 'gone' && !(rec.poolIds || []).length && (rec.handDone || !(rec.noDesign || rec.state === 'noDesign'));
    if (read && isHandDoc(c)) rec.handDone = { at: +c.completedAt || 0, by: c.completedBy || '', how: c.how === 'button' ? 'button' : 'print' };
    else if (rec.handDone) { delete rec.handDone; if (read && rec.state === 'noDesign') rec.state = 'pulled'; }
    return rec;
  });
}

/** Memoised per shop: the harness asks for the same rows many times. The value is shared, so every read is also snapshotted (JSON) and
 *  untouched(shop) says whether the code under test left everything it was handed exactly as it got it (it must not write to its inputs). */
const memoStore = new WeakMap();
function memo(shop, name, make) {
  let m = memoStore.get(shop); if (!m) memoStore.set(shop, m = new Map());
  if (!m.has(name)) { const v = make(shop); m.set(name, { v, json: JSON.stringify(v) }); }
  return m.get(name).v;
}
const uiRows = shop => memo(shop, 'uiRows', uiRowsFresh);
const recordRows = shop => memo(shop, 'recordRows', recordRowsFresh);
/** Names of cached inputs the code under test changed (empty when it never wrote to them). */
function untouched(shop) { const m = memoStore.get(shop), bad = []; if (m) for (const [k, e] of m) if (JSON.stringify(e.v) !== e.json) bad.push(k); return bad; }

/** Spec edit helpers used by the shrinker. */
const cloneSpec = s => JSON.parse(JSON.stringify(s));
function shrinks(spec) {
  const out = [];
  spec.orders.forEach((o, i) => { const c = cloneSpec(spec); c.orders.splice(i, 1); out.push(c); });
  spec.orders.forEach((o, i) => o.lines.forEach((l, j) => { if (o.lines.length > 1) { const c = cloneSpec(spec); c.orders[i].lines.splice(j, 1); out.push(c); } }));
  spec.sheets.forEach((s, i) => { const c = cloneSpec(spec); const id = s.id; c.sheets.splice(i, 1); c.orders.forEach(o => o.lines.forEach(l => l.copies.forEach(cp => { if (cp.sheet === id) cp.sheet = null; if (cp.dual === id) delete cp.dual; }))); out.push(c); });
  spec.orders.forEach((o, i) => o.lines.forEach((l, j) => { if (l.staleArchive) { const c = cloneSpec(spec); delete c.orders[i].lines[j].staleArchive; out.push(c); } l.copies.forEach((cp, k) => { if (cp.dual) { const c = cloneSpec(spec); delete c.orders[i].lines[j].copies[k].dual; out.push(c); } }); }));
  spec.sheets.forEach((s, i) => { for (const k of ['staleOrder', 'orphan', 'dupPool', 'ghostOrder']) if (s[k]) { const c = cloneSpec(spec); delete c.sheets[i][k]; out.push(c); } });
  spec.orders.forEach((o, i) => o.lines.forEach((l, j) => { if (l.hand) { const c = cloneSpec(spec); delete c.orders[i].lines[j].hand; out.push(c); } }));
  spec.sheets.forEach((s, i) => { if (s.own !== 'ok') { const c = cloneSpec(spec); c.sheets[i].own = 'ok'; out.push(c); } });
  spec.orders.forEach((o, i) => o.lines.forEach((l, j) => { if (l.engrave !== 'plain') { const c = cloneSpec(spec); c.orders[i].lines[j].engrave = 'plain'; out.push(c); } if (l.stale) { const c = cloneSpec(spec); c.orders[i].lines[j].stale = false; out.push(c); } }));
  return out;
}
function shrink(spec, fails) {
  let cur = spec, changed = true, guard = 0;
  while (changed && guard++ < 200) { changed = false; for (const c of shrinks(cur)) { let f = false; try { f = fails(c); } catch (_) { f = false; } if (f) { cur = c; changed = true; break; } } }
  return cur;
}
/** A readable one-block description of a spec, for a bug report. */
function describe(spec) {
  const L = [];
  for (const s of spec.sheets) L.push(`sheet ${s.id}${s.setId ? ' in ' + s.setId : ' (no set)'}${s.own !== 'ok' ? ' own=' + s.own : ''}${s.staleOrder ? ' lists order ' + s.staleOrder + ' it does not hold' : ''}${s.orphan ? ' +manual charm' : ''}${s.dupPool ? ' +pool id twice' : ''}${s.ghostOrder ? ' lists order ' + s.ghostOrder + ' that has no lines at all' : ''}`);
  for (const o of spec.orders) for (const l of o.lines) {
    const cp = l.copies.map(c => (c.sheet ? c.sheet + (c.dual ? '+' + c.dual : '') : c.archivedOn ? 'archivedSheet' : c.pooled ? 'pooled' : '-')).join(',');
    L.push(`order ${o.id} line ${l.tx} [${l.kind}] state=${l.state} q=${l.q} copies=[${cp}]${l.problems.length ? ' problems=' + l.problems.join('/') : ''}${l.sku ? '' : ' (no sku)'}${l.hold ? ' HOLD' : ''}${l.change ? ' changePending' : ''}${l.noDesign ? ' noDesign' : ''}${l.hand ? ` HAND(${l.hand.how}, custom record ${l.hand.state}, run copy ${l.hand.rec})` : ''} engrave=${l.engrave}${l.staleArchive ? ' +archived copy' : ''}${l.lost ? ' (record lost its pool ids)' : ''}${l.copies.some(c => c.archivedOn) ? ' (some copies only on an archived sheet)' : ''}`);
  }
  return L.join('\n');
}

module.exports = { rng, makeSpec, materialize, isHandDoc, uiRows, recordRows, memo, untouched, shrink, shrinkCandidates: shrinks, describe, cloneSpec, keyOf, pidOf, METALS };
