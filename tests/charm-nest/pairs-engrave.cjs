// PAIRENGRAVE (pairs-1009, area 13): engraving and custom designs know every PIECE of a line.
//
// What it proves, offline (no browser, no network, no paid call, nothing live, no Firestore but the in-memory fake):
//   1  the slot of a piece: its stored side wins; a matching pair of earrings has a Left and a Right too (amendment 2); a mismatched design with no
//      stored side is read from the copy number; discs are D1..Dn; a single charm, a charm only and an old line with no sides have no slot at all
//   2  a line cut into slots has one job key and one side row per slot: two ears never share a record, an approval, a seal or a back; revoking one
//      leaves the other; the line's own row reads the summary (worst state first, approved only when every slot is) and each piece's own decision
//   3  amendment 2: the Right earring is the Left mirrored, so the fit is made on the piece's own geometry (CharmNestPair.pieceGeometry), the engraved
//      TEXT is never reversed (the same letters, placed in the mirrored position), the Left is the very charm it was, and a charm that is already
//      mirrored is never mirrored twice; the back file of a mirrored piece is redrawn from its mirrored cut, never copied from the unmirrored source
//   4  the page's reading: the one reading of a line is handed to each slot (per ear, the same words for both, unclear => a question, never a guess)
//   5  the seals of one ear count only that ear's approvals (CNEngravingSeals.fromEvents), the readiness decision is per piece
//   6  the paid reading: a line that is not split asks byte for byte as before; a split line asks with its slots and gets a `pieces` answer
//   7  the server: a back for the left ear and one for the right ear are two records on two docs, each says its ear on the sheet, the timeline and
//      the pieces; re-putting one never touches the other; no document holds an array inside an array
//   8  an old line (no side, no slot, no mirror) reads exactly as it always did
//   node tests/charm-nest/pairs-engrave.cjs      (needs nothing installed; the fit checks use the checked-in fonts)
'use strict';
const path = require('path'), fs = require('fs'), vm = require('vm'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const transport = require.resolve(path.join(root, 'netlify/functions/_etsyMailAnthropic.js'));
require.cache[transport] = { id: transport, filename: transport, loaded: true, exports: {} };   // (no network: the agent file only builds requests here)
const Pair = require(path.join(root, 'charm-nest-pair.js'));
const Sides = require(path.join(root, 'charm-nest-engrave-sides.js'));
const Seals = require(path.join(root, 'charm-nest-engraving-seals.js'));
const Readiness = require(path.join(root, 'charm-nest-readiness.js'));
const Agent = require(path.join(root, 'netlify/functions/_charmNestAgent.js'));
const F = require('./pairs-fixtures.cjs');
const refuseNestedArrays = require('./_noNestedArrays.cjs');

const results = []; let failed = 0;
const t = async (name, fn) => { try { await fn(); results.push([name, null]); console.log('  ok  ' + name); } catch (e) { failed++; results.push([name, e]); console.log('  FAIL ' + name + '\n      ' + String(e && e.stack || e).split('\n').slice(0, 5).join('\n      ')); } };

/* ── a world: a matching pair, a mismatched pair, a three-disc necklace, a single charm, two pairs of one design ── */
const W = F.world({ orders: [
  { rid: F.rid(1), lines: [{ n: 10, kind: 'pair', on: 'sh-gf1' }] },
  { rid: F.rid(2), lines: [{ n: 20, kind: 'mismatched', on: 'sh-gf1' }] },
  { rid: F.rid(3), lines: [{ n: 30, kind: 'discs', discs: 3, on: 'sh-gf1' }] },
  { rid: F.rid(4), lines: [{ n: 40, kind: 'single', on: 'sh-gf1' }] },
  { rid: F.rid(5), lines: [{ n: 50, kind: 'pair', qty: 2, on: 'sh-gf1' }] },
  { rid: F.rid(6), lines: [{ n: 60, kind: 'earring-single', on: 'sh-gf1' }] },
] });
const LEG = F.legacy(W);
const lineRow = (w, ord, n) => {   // the page's order row of a line: key, pool ids, order, spec (designSku, form)
  const o = w.orders.find(x => x.rid === F.rid(ord)), l = o.lines.find(x => x.n === n);
  return { key: F.lineKey(o.rid, F.tx(n)), poolIds: l.pieces.map(p => p.poolId), order: { receiptId: o.rid }, line: { transactionId: F.tx(n), sku: l.sku }, spec: { designSku: l.sku, form: l.form, quantity: l.qty, personalization: ['Anna'], engraveCandidate: true }, state: 'written' };
};
const ctxOf = w => { const rows = new Map(w.pool.map(p => [p.poolId, p])); return { poolRow: id => rows.get(id), charmOf: id => { const r = rows.get(id); return r ? F.charmOf(r.sku) : null; }, entryFor: sku => F.entryOf(sku), pair: Pair, mergeSeals: (...r) => Seals.merge(...r) }; };
const CTX = ctxOf(W), CTXL = ctxOf(LEG);
const PAIR = lineRow(W, 1, 10), MIS = lineRow(W, 2, 20), DISC = lineRow(W, 3, 30), ONE = lineRow(W, 4, 40), PAIR2 = lineRow(W, 5, 50), EAR = lineRow(W, 6, 60);
const LPAIR = lineRow(LEG, 1, 10), LMIS = lineRow(LEG, 2, 20), LDISC = lineRow(LEG, 3, 30);
const slots = (ctx, row) => row.poolIds.map(id => Sides.slotOfId(ctx, row, id));

/* ── an asymmetric charm with a cut-out, as the fit sees it (a thumb on the left, a hanging hole) ── */
const path_ = (pts, extra) => Object.assign({ kind: 'path', closed: true, stroke: true, fill: false, layer: 'CUT', strokeRGB: [1, 0, 0], lwPt: .25, bbox: [Math.min(...pts.map(p => p[0])), Math.min(...pts.map(p => p[1])), Math.max(...pts.map(p => p[0])), Math.max(...pts.map(p => p[1]))], subpaths: [pts.map((p, i) => [i ? 'l' : 'm', p]).concat([['h']])] }, extra || {});
const mitten = () => { const out = path_([[10, 0], [50, 0], [50, 60], [10, 60], [10, 36], [0, 28], [10, 20]]), hole = path_([[28, 48], [36, 48], [36, 56], [28, 56]]); return { sourceId: 'src', outline: out, members: [out, hole], bbox: out.bbox.slice(), widthPt: 50, heightPt: 60, upAngle: 90 }; };

(async () => {
  console.log('PAIRENGRAVE');

  // ═══ 1 · the slot of a piece ═══
  await t('1a a matching pair of earrings has a Left and a Right (amendment 2); two pairs of one design alternate L R L R', () => {
    assert.deepEqual(slots(CTX, PAIR), ['L', 'R']);
    assert.deepEqual(slots(CTX, PAIR2), ['L', 'R', 'L', 'R']);
    assert.deepEqual(Sides.idsOfSlot(CTX, PAIR2, 'R'), [PAIR2.poolIds[1], PAIR2.poolIds[3]]);
    const p = Sides.plan(CTX, PAIR, []); assert.equal(p.split, true); assert.deepEqual(p.slots, ['L', 'R']);
  });
  await t('1b a mismatched pair: the stored side wins; with no stored side the design says so (left body = copy 1)', () => {
    assert.deepEqual(slots(CTX, MIS), ['L', 'R']);
    assert.deepEqual(slots(CTXL, LMIS), ['L', 'R'], 'an old mismatched line has no side stored: read from the design and the copy number');
    assert.deepEqual(Sides.plan(CTXL, LMIS, []).slots, ['L', 'R']);
  });
  await t('1c discs are D1..Dn; a single charm, a charm only and a single earring have no slot', () => {
    assert.deepEqual(slots(CTX, DISC), ['D1', 'D2', 'D3']); assert.deepEqual(slots(CTXL, LDISC), ['D1', 'D2', 'D3'], 'discs have no side to store: read from the line (discsOf) and the copy number');
    assert.deepEqual(Sides.plan(CTX, DISC, []).slots, ['D1', 'D2', 'D3']);
    assert.deepEqual(slots(CTX, ONE), [null]); assert.deepEqual(slots(CTX, EAR), [null]);
    assert.equal(Sides.plan(CTX, ONE, []).split, false); assert.equal(Sides.plan(CTX, EAR, []).split, false);
  });
  await t('1d an old matching pair (no side stored anywhere) is ONE job exactly as before; a decided plain job keeps even a sided line whole', () => {
    const p = Sides.plan(CTXL, LPAIR, []); assert.equal(p.split, false); assert.deepEqual(p.slots, [null]);
    assert.equal(Sides.plan(CTX, PAIR, [{ state: 'written', slot: null }]).split, false, 'an approval already given under the plain key is never re-cut');
    assert.equal(Sides.plan(CTX, PAIR, [{ state: 'words', slot: null }]).split, true, 'a plain job still being decided is cut into its ears');
    assert.equal(Sides.plan(CTX, LPAIR, [{ state: 'words', slot: 'L' }]).split, true, 'once slot jobs exist they stay');
  });
  await t('1e keys and words', () => {
    assert.equal(Sides.jobKey('4190000001_5000000010', 'R'), '4190000001_5000000010#R'); assert.equal(Sides.jobKey('x', null), 'x');
    assert.deepEqual(Sides.parseKey('x#L'), { rowKey: 'x', slot: 'L' }); assert.deepEqual(Sides.parseKey('x'), { rowKey: 'x', slot: null });
    assert.deepEqual([Sides.earOf('L'), Sides.earOf('R'), Sides.earOf('D2'), Sides.earOf(null)], ['Left ear', 'Right ear', 'Disc 2', '']);
    assert.equal(Sides.tagOf('R'), 'RIGHT EAR'); assert.equal(Sides.tagOf('D2', 3), 'DISC 2 of 3');
  });

  // ═══ 2 · two jobs never share a record ═══
  const mk = (row, slot, state, extra) => { const job = { key: Sides.jobKey(row.key, slot), slot, copies: Sides.idsOfSlot(CTX, row, slot), state, engraveRec: Object.assign({ needed: true, state, approved: false }, extra || {}) }; job.row = Sides.sideRow(CTX, row, slot, job); return job; };
  await t('2a a side row is the line\'s row with its own key, pool ids and engrave record', () => {
    const parent = Object.assign({}, PAIR2), jl = mk(parent, 'L', 'review'), jr = mk(parent, 'R', 'words');
    assert.equal(jl.row.key, parent.key + '#L'); assert.equal(jr.row.side, 'R'); assert.equal(jl.row.parentRow, parent);
    assert.deepEqual(jl.row.poolIds, [parent.poolIds[0], parent.poolIds[2]]); assert.deepEqual(jr.row.poolIds, [parent.poolIds[1], parent.poolIds[3]]);
    assert.equal(jl.row.order, parent.order, 'everything else is the line\'s own'); parent.state = 'gone'; assert.equal(jr.row.state, 'gone', 'a cancelled line reads as gone for every ear');
    parent.state = 'written';
    jl.row.engrave = { needed: true, state: 'approved', approved: true, approvedBy: 'Paul', approvedAt: 5 };
    assert.equal(jr.row.engrave.state, 'words', 'writing the left ear\'s record never reaches the right ear\'s'); assert.equal(jr.engraveRec.approved, false);
  });
  await t('2b the line reads the summary: worst state first, approved only when every ear is, each piece its own decision', () => {
    const parent = Object.assign({}, PAIR); const jl = mk(parent, 'L', 'approved', { approved: true, approvedBy: 'Paul', approvedAt: 10, text: 'Anna' }), jr = mk(parent, 'R', 'review', { text: 'Ben' });
    Sides.linkParent(CTX, parent, [jl, jr]);
    const s = parent.engrave; assert.equal(s.state, 'review'); assert.equal(s.approved, false, 'one ear approved is not the line approved');
    assert.equal(s.text, 'Left: Anna · Right: Ben'); assert.deepEqual(s.pieces[PAIR.poolIds[0]], { needed: true, state: 'approved', approved: true }); assert.deepEqual(s.pieces[PAIR.poolIds[1]], { needed: true, state: 'review', approved: false });
    jr.engraveRec = Object.assign({}, jr.engraveRec, { state: 'approved', approved: true, approvedBy: 'Seth', approvedAt: 20 });
    assert.equal(parent.engrave.approved, true); assert.equal(parent.engrave.state, 'approved');
    // revoke ONE ear (the right): the left stays approved
    jr.engraveRec = Object.assign({}, jr.engraveRec, { state: 'words', approved: false });
    assert.equal(parent.engrave.approved, false); assert.equal(parent.engrave.pieces[PAIR.poolIds[0]].approved, true, 'the left ear\'s approval stands'); assert.equal(parent.engrave.pieces[PAIR.poolIds[1]].approved, false);
    assert.equal(jl.engraveRec.approvedBy, 'Paul', 'and the left ear keeps its own signer');
  });
  await t('2c an ear that needs no back (words say plain) does not hold the line; a line of ears all plain is plain', () => {
    const parent = Object.assign({}, PAIR); const jl = mk(parent, 'L', 'none', { needed: false, approved: true }), jr = mk(parent, 'R', 'approved', { approved: true, text: 'Ben' });
    Sides.linkParent(CTX, parent, [jl, jr]); assert.equal(parent.engrave.approved, true); assert.equal(parent.engrave.state, 'approved'); assert.equal(parent.engrave.pieces[PAIR.poolIds[0]].needed, false);
    const p2 = Object.assign({}, PAIR); const a = mk(p2, 'L', 'none', { needed: false, approved: true }), b = mk(p2, 'R', 'none', { needed: false, approved: true }); Sides.linkParent(CTX, p2, [a, b]);
    assert.equal(p2.engrave.state, 'none'); assert.equal(p2.engrave.needed, false);
  });
  await t('2d a whole new record written to the line reaches every ear; the line can be turned back into plain data', () => {
    const parent = Object.assign({}, PAIR); const jl = mk(parent, 'L', 'review'), jr = mk(parent, 'R', 'review'); Sides.linkParent(CTX, parent, [jl, jr]);
    parent.engrave = { needed: false, state: 'none', approved: true }; assert.equal(jl.engraveRec.state, 'none'); assert.equal(jr.engraveRec.state, 'none');
    jl.engraveRec = Object.assign({}, jl.engraveRec, { text: 'x' }); jr.engraveRec = Object.assign({}, jr.engraveRec, { text: 'x' });
    Sides.unlinkParent(CTX, parent); const d = Object.getOwnPropertyDescriptor(parent, 'engrave'); assert.ok('value' in d && !d.get, 'plain data again');
  });

  // ═══ 3 · amendment 2: the Right earring is the Left mirrored ═══
  const Lc = mitten(), poolRows = new Map([['L1', { poolId: 'L1', side: 'L', mirror: false, bodyIndex: 0 }], ['R1', { poolId: 'R1', side: 'R', mirror: true, bodyIndex: 0 }], ['S1', { poolId: 'S1' }], ['O1', { poolId: 'O1', side: 'R' }]]);
  const mctx = { poolRow: id => poolRows.get(id), pair: Pair };
  await t('3a the Left is the very charm; the Right is its mirror image (x -> -x about its own centre), marked mirrored; a single piece is untouched', () => {
    assert.equal(Sides.pieceCharm(mctx, 'L1', Lc), Lc, 'the Left earring is the charm it was');
    assert.equal(Sides.pieceCharm(mctx, 'S1', Lc), Lc, 'a piece with no side is untouched');
    assert.equal(Sides.pieceCharm(mctx, 'X', null), null);
    const R = Sides.pieceCharm(mctx, 'R1', Lc); assert.notEqual(R, Lc); assert.equal(R.mirrored, true);
    const cx = (Lc.bbox[0] + Lc.bbox[2]) / 2; assert.deepEqual(R.bbox, [2 * cx - Lc.bbox[2], Lc.bbox[1], 2 * cx - Lc.bbox[0], Lc.bbox[3]]);
    const thumbL = Lc.outline.subpaths[0].filter(o => o[0] === 'l').map(o => o[1]), thumbR = R.outline.subpaths[0].filter(o => o[0] === 'l').map(o => o[1]);
    assert.deepEqual(thumbR.map(p => [2 * cx - p[0], p[1]]), thumbL, 'every outline point is the Left\'s turned about the vertical axis');
    assert.equal(Sides.pieceCharm(mctx, 'R1', Lc), R, 'made once per charm: a fit kept against it still matches after a wait');
    assert.equal(Sides.pieceCharm(mctx, 'R1', R), R, 'a charm that is already the mirrored one is never mirrored twice');
    assert.deepEqual(Lc.outline.bbox, [0, 0, 50, 60], 'the original is never touched');
  });
  await t('3b with no stored mirror a stored side asks which way the drawing faces (a person\'s facing wins); no side, nothing guessed', () => {
    assert.equal(Sides.mirrorOfId(mctx, 'O1', Lc), true, 'side R of a drawing that faces nowhere in particular: the Right is the mirror');
    assert.equal(Sides.mirrorOfId(mctx, 'O1', Object.assign({}, Lc, { facing: 'R' })), false, 'the drawing faces right: as drawn IS the Right');
    assert.equal(Sides.mirrorOfId(mctx, 'S1', Lc), false); assert.equal(Sides.mirrorOfId(mctx, 'L1', Lc), false);
    assert.equal(Sides.mirrorOfId(mctx, 'Z', Lc, { mirror: true }), true, 'a saved back says so itself');
  });
  await t('3c a mismatched design: each ear is its own body (not the two glued), the Right facing right', () => {
    const glued = F.charmOf('TENNIS-MIS'); const rows = new Map([['A', { poolId: 'A', side: 'L', bodyIndex: 0, mirror: false }], ['B', { poolId: 'B', side: 'R', bodyIndex: 1, mirror: false }]]);
    const c = { poolRow: id => rows.get(id), pair: Pair };
    const a = Sides.pieceCharm(c, 'A', glued), b = Sides.pieceCharm(c, 'B', glued);
    assert.notEqual(a, glued); assert.notEqual(b, glued); assert.notDeepEqual(a.bbox, b.bbox, 'a ball and a racket');
    assert.ok(a.members.length < glued.members.length && b.members.length < glued.members.length);
    assert.equal(Sides.pieceCharm(c, 'A', glued), a, 'cached');
  });
  await t('3d the engraved TEXT is never reversed: the same letters on both ears, placed on each ear\'s own geometry (real fit, checked-in fonts)', () => {
    const G = require(path.join(root, 'charm-nest-geom.js')), Fit = require(path.join(root, 'charm-nest-engrave-fit.js')), ot = require(path.join(root, 'vendor/opentype-1.3.4.min.js')), T = require(path.join(root, 'charm-nest-text.js'));
    const bytes = p => { const b = fs.readFileSync(path.join(root, p)); return b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength); };
    const emoji = ot.parse(bytes('vendor/fonts/NotoEmoji-Regular.ttf')), emojiMap = require(path.join(root, 'vendor/fonts/emoji-sequences.json'));
    const fonts = Object.fromEntries(['Regular', 'Semibold'].map(w => [w, T.withEmoji(ot.parse(bytes(`vendor/fonts/SourceSans3-${w}.otf`)), emoji, emojiMap, ot.Path)]));
    const R = Sides.pieceCharm(mctx, 'R1', Lc);
    const run = ch => Fit.calculate({ charm: { outline: ch.outline, members: ch.members, bbox: ch.bbox, widthPt: ch.widthPt, heightPt: ch.heightPt, upAngle: ch.upAngle }, lines: ['Anna'], lineMode: 'auto', viewOptions: { res: 6, upAngle: 90 }, maskOptions: { marginMm: .8, keepOut: [] }, opts: { minCapMm: 1.6, maxHeightFrac: .4, lineGap: .216, minStrokeMm: 0, minGapMm: 0, tryRotated: false } }, fonts, G);
    const l = run(Lc), r = run(R); assert.ok(l.fit && r.fit, 'both ears fit');
    assert.equal(l.fit.size, r.fit.size, 'the same words fit the same size on the mirrored geometry'); assert.equal(l.fit.weight, r.fit.weight);
    const shape = fit => fit.glyphs.map(g => g.cmds.map(c => { const o = g.cmds[0]; return c.type === 'Z' ? ['Z'] : [c.type, +(c.x - o.x).toFixed(3), +(c.y - o.y).toFixed(3)]; }));
    assert.deepEqual(shape(r.fit), shape(l.fit), 'every letter is drawn the same way round on the Right as on the Left: text is placed, never mirrored');
    // the PLACEMENT is the mirrored one: the same words sit at the mirror position (back-frame x of the Right = minus the Left\'s, about the charm centre)
    assert.ok(Math.abs(l.fit.centre[0] + r.fit.centre[0] - 2 * l.view.cx) < 0.05 && Math.abs(l.fit.centre[0] - r.fit.centre[0]) > 1, 'the text sits at the mirrored position (the Right\'s centre is the Left\'s turned about the charm\'s centre line)');
    assert.ok(Math.abs(l.fit.centre[1] - r.fit.centre[1]) < 0.05, 'and at the same height');
  });
  await t('3e the back file of a mirrored piece is redrawn from the mirrored cut (never the unmirrored source bytes); an ordinary piece keeps the copy', () => {
    const src = fs.readFileSync(path.join(root, 'charm-nest-pdf.js'), 'utf8');
    assert.ok(/redrawn: shared \|\| cut\.some\(m => m\.synthetic\) \|\| !!spec\.mirrored/.test(src), 'buildBackFile redraws when the caller says the piece is mirrored');
    const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
    assert.equal((bridge.match(/mirrored:\s*!!charm0?\.mirrored/g) || []).length, 2, 'both back file builders pass the flag');
    assert.ok(/const pieceCharmOf = /.test(bridge) && /pieceCharmOf\(job\.copies\[0\]/.test(bridge), 'the fit reads the piece\'s own charm');
  });

  // ═══ 4 · the reading handed to each slot (the real page code, sliced) ═══
  const bridgeSrc = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  const sliceBridge = () => bridgeSrc.slice(bridgeSrc.indexOf('  const SIDES = '), bridgeSrc.indexOf('  function setNone(job'));
  function classifier(world, row, reading) {
    const jobs = new Map(), asked = [];
    const rows = new Map(world.pool.map(p => [p.poolId, p]));
    const win = { CharmNestEngraveSides: Sides, CharmNestPair: Pair };
    const ctx = { window: win, items: () => jobs, B: { pool: { rows } }, Pool: { charmOf: id => { const r = rows.get(id); return r ? F.charmOf(r.sku) : null; } }, Master: { entryFor: sku => F.entryOf(sku) }, CNEngravingSeals: Seals, DECIDED: ['approved', 'written', 'skipped'],
      S: { settings: { engraveConfidence: .65 } }, render() {}, agent() {}, Date, Promise, Map, Set, WeakMap, Array, Object, String, Number, JSON, Math, Review: { add() {}, remove() {} }, RunCtl: { poke() {} }, Session: { schedule() {} }, TL: { line() {}, hash: s => String(s).length },
      lineOf: undefined, needEvent() {}, setReady(job) { job.state = 'ready'; job.row.engrave = Object.assign({}, job.row.engrave, { needed: true, state: 'ready', approved: false }); return job; }, toWords(job, why) { job.state = 'words'; job.reason = why; job.row.engrave = Object.assign({}, job.row.engrave, { needed: true, state: 'words', approved: false }); return job; },
      agentCall: async (mode, ask) => { asked.push(ask); return reading; } };
    vm.createContext(ctx); vm.runInContext(sliceBridge(), ctx);
    return { ctx, jobs, asked, run: () => ctx.classify(row) };
  }
  const withSpec = (row, extra) => Object.assign({}, row, { spec: Object.assign({}, row.spec, extra), line: Object.assign({ title: 'Earrings' }, row.line) });
  await t('4a a pair line is cut into a Left and a Right job; each takes its own words from the one reading', async () => {
    const row = withSpec(PAIR, { personalization: ['left: Anna', 'right: Ben'] });
    const c = classifier(W, row, { engrave: true, text: 'Anna\nBen', source: 'personalization', confidence: .95, questions: [], requests: { side: 'back' }, pieces: [{ slot: 'L', text: 'Anna' }, { slot: 'R', text: 'Ben' }] });
    await c.run(); assert.deepEqual([...c.jobs.keys()].sort(), [row.key + '#L', row.key + '#R']);
    const l = c.jobs.get(row.key + '#L'), r = c.jobs.get(row.key + '#R');
    assert.equal(l.text, 'Anna'); assert.equal(r.text, 'Ben'); assert.equal(l.state, 'ready'); assert.equal(r.state, 'ready');
    assert.deepEqual(l.copies, [PAIR.poolIds[0]]); assert.deepEqual(r.copies, [PAIR.poolIds[1]]);
    assert.notEqual(l.row.engrave, r.row.engrave, 'never one shared record'); assert.equal(c.asked.length, 1, 'ONE paid reading for the line, not one per ear');
    assert.deepEqual([...c.asked[0].slots], ['L', 'R'], 'the question names the ears');
  });
  await t('4b one inscription for the whole line is the same words on both ears (still two jobs, two approvals)', async () => {
    const row = withSpec(PAIR, { personalization: ['Anna'] });
    const c = classifier(W, row, { engrave: true, text: 'Anna', source: 'personalization', confidence: .95, questions: [], requests: { side: 'back' }, pieces: [{ slot: 'L', text: 'Anna' }, { slot: 'R', text: 'Anna' }] });
    await c.run(); const l = c.jobs.get(row.key + '#L'), r = c.jobs.get(row.key + '#R');
    assert.equal(l.text, 'Anna'); assert.equal(r.text, 'Anna'); assert.notEqual(l, r); assert.notEqual(l.row.engrave, r.row.engrave);
    const reading = { engrave: true, text: 'Anna', source: 'personalization', confidence: .95, questions: [], requests: { side: 'back' } };   // (a reading with no pieces: the one text for both)
    const c2 = classifier(W, row, reading); await c2.run(); assert.equal(c2.jobs.get(row.key + '#L').text, 'Anna'); assert.equal(c2.jobs.get(row.key + '#R').text, 'Anna');
  });
  await t('4c unclear which ear gets which: both ears keep the question and neither is approved (a person decides)', async () => {
    const row = withSpec(PAIR, { personalization: ['Anna', 'Ben'] });
    const c = classifier(W, row, { engrave: true, text: 'Anna\nBen', source: 'personalization', confidence: .5, questions: ['Which ear gets Anna and which gets Ben?'], requests: { side: 'back' }, pieces: [{ slot: 'L', text: 'Anna' }, { slot: 'R', text: 'Ben' }] });
    await c.run(); for (const k of ['#L', '#R']) { const j = c.jobs.get(row.key + k); assert.ok(j.questions.some(q => /Which ear/.test(q)), k + ' keeps the question'); assert.equal(j.row.engrave.approved, false, 'a proposal never approves itself'); assert.equal(j.approvedAt, null); }
  });
  await t('4d a single charm (and an old pair with no sides) is ONE job under the line key, asked exactly as before (no slots in the question)', async () => {
    for (const [w, row] of [[W, withSpec(ONE, { personalization: ['S'] })], [LEG, withSpec(LPAIR, { personalization: ['S'] })]]) {
      const c = classifier(w, row, { engrave: true, text: 'S', source: 'personalization', confidence: .95, questions: [], requests: { side: 'back' } });
      await c.run(); assert.deepEqual([...c.jobs.keys()], [row.key]); assert.equal('slots' in c.asked[0], false, 'the same question as before'); assert.equal(c.jobs.get(row.key).text, 'S');
      assert.equal(c.jobs.get(row.key).slot == null, true);
    }
  });
  await t('4e a plain job already waiting for words is seeded into the ears (no second paid reading), and an approved plain job is never re-cut', async () => {
    const row = withSpec(PAIR, { personalization: ['Anna'] });
    const c = classifier(W, row, { skipped: 'offline' });
    c.jobs.set(row.key, { key: row.key, row, state: 'words', text: 'Anna', lines: ['Anna'], copies: row.poolIds.slice(), engraveRec: undefined });
    row.engrave = { needed: true, state: 'words', approved: false };
    const js = c.ctx.ensureJobs(row); assert.equal(js.length, 2); assert.deepEqual(js.map(j => j.slot), ['L', 'R']); assert.ok(js.every(j => j.text === 'Anna' && j.seededFrom === row.key));
    assert.equal(c.jobs.has(row.key), false, 'the plain job became the ears');
    await c.ctx.classify(row); assert.equal(c.asked.length, 0, 'the words were already paid for: no second reading'); assert.ok(js.every(j => j.state === 'words' && /left|right/i.test(j.reason || '')), 'each ear asks which words are its own: ' + js.map(j => j.reason));
    const c2 = classifier(W, row, null); const approved = { key: row.key, row, state: 'written', text: 'Anna', lines: ['Anna'], copies: row.poolIds.slice() };
    c2.jobs.set(row.key, approved); const js2 = c2.ctx.ensureJobs(row); assert.equal(js2.length, 1); assert.equal(js2[0], approved);
  });

  // ═══ 5 · seals, readiness, the back card ═══
  await t('5a the seals of one ear count only that ear\'s approvals (the shared transaction id of the other ear does not count)', () => {
    const parent = Object.assign({}, PAIR); const jl = mk(parent, 'L', 'review'), jr = mk(parent, 'R', 'review');
    const ev = [
      { id: 'a', type: 'engraveApproved', orderId: PAIR.order.receiptId, transactionId: PAIR.line.transactionId, lineKey: PAIR.key, at: 100, by: 'Paul', data: { poolId: PAIR.poolIds[0], slot: 'L' } },
      { id: 'b', type: 'engraveApproved', orderId: PAIR.order.receiptId, transactionId: PAIR.line.transactionId, lineKey: PAIR.key, at: 200, by: 'Seth', data: { poolId: PAIR.poolIds[1], slot: 'R' } }];
    const L = Seals.fromEvents(ev, jl.row), R = Seals.fromEvents(ev, jr.row);
    assert.deepEqual(L.map(s => s.by), ['Paul']); assert.deepEqual(R.map(s => s.by), ['Seth']);
    assert.deepEqual(Seals.fromEvents(ev, PAIR).map(s => s.by).sort(), ['Paul', 'Seth'], 'the line as a whole (an old reader) still sees both');
  });
  await t('5b the readiness decision is per piece: the left piece is decided while the right still waits', () => {
    const parent = Object.assign({}, PAIR); const jl = mk(parent, 'L', 'approved', { approved: true, text: 'A' }), jr = mk(parent, 'R', 'words', { text: 'B' }); Sides.linkParent(CTX, parent, [jl, jr]);
    const d = Readiness.decisions([parent]); assert.deepEqual(d[PAIR.poolIds[0]], { needed: true, state: 'approved', approved: true }); assert.deepEqual(d[PAIR.poolIds[1]], { needed: true, state: 'words', approved: false });
    const plain = Readiness.decisions([{ key: ONE.key, poolIds: ONE.poolIds, spec: ONE.spec, engrave: { needed: true, state: 'review', approved: false } }]); assert.deepEqual(plain[ONE.poolIds[0]], { needed: true, state: 'review', approved: false }, 'a line with no pieces map reads exactly as before');
  });
  await t('5c the back card names the ear', () => {
    const src = fs.readFileSync(path.join(root, 'charm-nest-backs.js'), 'utf8'); const line = /const pieceWords = [^\n]+/.exec(src)[0];
    const pieceWords = vm.runInNewContext('(' + line.replace('const pieceWords = ', '').replace(/;\s*$/, '') + ')');
    assert.deepEqual([pieceWords({ slot: 'L' }), pieceWords({ side: 'R' }), pieceWords({ slot: 'D2' }), pieceWords({}), pieceWords(null)], ['Left ear', 'Right ear', 'Disc 2', '', '']);
  });

  // ═══ 6 · the paid reading ═══
  await t('6a a line that is not split asks byte for byte as before; a split line asks for `pieces`', () => {
    const body = { order: '4190000001', sku: 'PAIR-STUD', title: 'Earrings', form: 'earrings', quantity: 1, engravable: true, personalization: ['Anna'], buyerMessage: '', staffNote: '' };
    const plain = Agent.buildRequest('engraveIntent', body), plain2 = Agent.buildRequest('engraveIntent', Object.assign({}, body, { slots: [] })), one = Agent.buildRequest('engraveIntent', Object.assign({}, body, { slots: ['L'] }));
    assert.equal(JSON.stringify(plain), JSON.stringify(plain2)); assert.equal(JSON.stringify(plain), JSON.stringify(one), 'fewer than two slots is not a split line');
    assert.equal('pieces' in plain.schema.properties, false);
    const split = Agent.buildRequest('engraveIntent', Object.assign({}, body, { slots: ['L', 'R'] }));
    assert.ok(split.schema.properties.pieces && split.schema.required.includes('pieces')); assert.ok(/LEFT earring/.test(split.system) && /never reversed/.test(split.system));
    assert.notEqual(split.system, plain.system);
  });

  // ═══ 7 · the server: two backs, two docs, each says its ear ═══
  await t('7 backPut: the left and the right ear are two records; re-putting one never touches the other; no array inside an array', async () => {
    const { start } = require('./bridge-server.cjs');
    const srv = await start({ strictArrays: true }); const st = srv.st; st.atomic = true;
    try {
      const rid = F.rid(1), tx = F.tx(10), L = F.poolId(rid, tx, 1), R = F.poolId(rid, tx, 2), NOW = Date.now();
      st.put('Charm_Nest_Sheets', 'sheet-pe', { id: 'sheet-pe', metal: 'gold', sheetIndex: 1, page: 1, day: '2026-10-09', status: 'written', folder: 'sheet-pe', fileBase: 'sheet-pe', poolIds: [L, R], orders: [rid], runId: 'run-pe', charmCount: 2, placedCount: 2 });
      for (const [id, side, copy] of [[L, 'L', 1], [R, 'R', 2]]) st.put('Charm_Pool', id, { poolId: id, orderId: rid, transactionId: tx, lineKey: `${rid}_${tx}`, sku: 'PAIR-STUD', material: 'gold', copy, quantity: 1, state: 'written', sheetId: 'sheet-pe', side, mirror: side === 'R', bodyIndex: 0, groupKey: `${rid}:${tx}`, groupSize: 2 });
      const call = async body => { const r = await fetch(`${srv.sorterOrigin}/.netlify/functions/charmNestLibrary`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }); let b = null; try { b = await r.json(); } catch (_) { /* empty */ } return { status: r.status, body: b }; };
      const back = (poolId, side, text, extra) => Object.assign({ poolId, sheetId: 'sheet-pe', order: rid, transactionId: tx, sku: 'PAIR-STUD', copy: side === 'L' ? 1 : 2, side, slot: side, text, lines: [text], approvedAt: NOW, approvedBy: 'Paul', outputs: { ai: { path: 'a/' + poolId + '.ai', url: 'https://x/' + poolId + '.ai' }, png: { path: 'a/' + poolId + '.png', url: 'https://x/' + poolId + '.png' } } }, side === 'R' ? { mirror: true } : {}, extra || {});
      const a = await call({ op: 'backPut', backs: [back(L, 'L', 'Anna'), back(R, 'R', 'Ben')] }); assert.equal(a.status, 200, JSON.stringify(a.body));
      const dl = st.doc('Charm_Pool_Back', L), dr = st.doc('Charm_Pool_Back', R);
      assert.ok(dl && dr && dl !== dr); assert.equal(dl.text, 'Anna'); assert.equal(dr.text, 'Ben'); assert.equal(dl.side, 'L'); assert.equal(dr.side, 'R'); assert.equal(dr.mirror, true); assert.notEqual(dl.mirror, true);
      const sheet = st.doc('Charm_Nest_Sheets', 'sheet-pe'); const bl = sheet.backPool.find(b => b.poolId === L), br = sheet.backPool.find(b => b.poolId === R);
      assert.equal(bl.slot, 'L'); assert.equal(br.slot, 'R'); assert.equal(br.mirror, true); assert.equal(bl.text, 'Anna'); assert.equal(br.text, 'Ben');
      const listed = (await call({ op: 'listSheets', limit: 50 })).body.sheets.find(s => s.id === 'sheet-pe'); assert.deepEqual(listed.backs.map(b => [b.poolId, b.side, b.slot, b.text]).sort(), [[L, 'L', 'L', 'Anna'], [R, 'R', 'R', 'Ben']].sort());
      // the timeline: one approval event per ear, each says its ear
      const tl = [...st.docs.entries()].filter(([k]) => k.startsWith('Order_Timeline/')).map(([, v]) => v);
      const evs = tl.flatMap(d => Array.isArray(d.events) ? d.events : [d]).filter(e => e && e.type === 'engraveApproved');
      if (evs.length) { assert.equal(evs.length, 2); const byPool = Object.fromEntries(evs.map(e => [e.data.poolId, e])); assert.match(byPool[L].text, /Left ear/); assert.match(byPool[R].text, /Right ear/); assert.equal(byPool[L].data.slot, 'L'); assert.equal(byPool[R].data.slot, 'R'); }
      // change the left ear only: the right ear's record is not touched
      const before = JSON.stringify(st.doc('Charm_Pool_Back', R));
      const b = await call({ op: 'backPut', backs: [back(L, 'L', 'Annabel', { approvedAt: NOW + 1000 })] }); assert.equal(b.status, 200, JSON.stringify(b.body));
      assert.equal(st.doc('Charm_Pool_Back', L).text, 'Annabel'); assert.equal(JSON.stringify(st.doc('Charm_Pool_Back', R)), before, 'the right ear\'s back is byte for byte what it was');
      assert.equal(st.doc('Charm_Nest_Sheets', 'sheet-pe').backPool.find(x => x.poolId === R).text, 'Ben');
      for (const [k, v] of st.docs) refuseNestedArrays(v, k);
    } finally { await srv.close?.(); }
  });

  // ═══ 8 · custom designs ═══
  await t('8 custom designs are never engraved (their designs are the customer\'s), and a mismatched pair is never read as custom', () => {
    const custom = fs.readFileSync(path.join(root, 'netlify/functions/_charmNestCustomRead.js'), 'utf8');
    assert.ok(/DIFFERENT charm on each ear/.test(custom) && /regular/.test(custom) && /never custom/.test(custom), 'the custom reading says a mismatched pair is regular, not custom');
  });

  // ═══ 9 · an earring pair cut from the customer's own designs (the page's real CustomSheet helpers, sliced) ═══
  const cutOut = (from, to) => { const a = bridgeSrc.indexOf(from), b = bridgeSrc.indexOf(to, a); assert.ok(a > 0 && b > a, 'markers: ' + from); return bridgeSrc.slice(a, b); };
  const customRows = (w, ...rows) => rows.map(r => Object.assign({}, r, { spec: Object.assign({ form: r.spec.form, quantity: r.spec.quantity || 1 }, r.spec), order: r.order, line: r.line }));
  function customCtx(rows, extra) {
    const calls = { silhouettes: 0 };
    const ctx = Object.assign({ window: { CharmNestPair: Pair }, Pair, Orders: { rows: () => rows }, Review: { cardKey: r => 'card:' + r.order.receiptId }, ver: 1, Map, Date, Math, Number, Object, Array, Promise, Error, JSON, String,
      read: async F => ({ charms: F.charms }), P: { buildSilhouettes: async () => { calls.silhouettes++; } }, S: { settings: {} }, calls }, extra || {});
    vm.createContext(ctx);
    vm.runInContext(cutOut('  /* ── an earring pair cut from the customer\'s own designs', '  /** Why the card\'s designs cannot be sent yet'), ctx);
    vm.runInContext(cutOut('  /** The ear of each piece of an earring-pair line', '  /** preparePool for a line sent from its card'), ctx);
    return ctx;
  }
  const rowOf = (ord, n, over) => { const r = lineRow(W, ord, n); return Object.assign({}, r, { poolIds: [], spec: Object.assign({}, r.spec, over && over.spec), state: 'pulled' }); };
  await t('9a an earring-pair line needs a Left and a Right per pair; any other line needs what it always needed', () => {
    const pair = rowOf(1, 10, { spec: { form: 'earrings', quantity: 1 } }), pairs2 = rowOf(5, 50, { spec: { form: 'earrings', quantity: 2 } }), one = rowOf(4, 40, { spec: { form: 'necklace', quantity: 1 } });
    const ctx = customCtx([pair, pairs2, one]);
    assert.equal(ctx.pairPlan(pair).length, 2); assert.equal(ctx.pairPlan(pairs2).length, 4); assert.equal(ctx.pairPlan(one), null);
    assert.deepEqual({ ...ctx.needOf({ ck: 'card:' + pair.order.receiptId }) }, { total: 2, pairs: 2 });
    assert.deepEqual({ ...ctx.needOf({ ck: 'card:' + one.order.receiptId }) }, { total: 1, pairs: 0 }, 'a single charm: no pair, nothing changes');
    const p3 = rowOf(1, 10, { spec: { form: 'earrings', quantity: 1, pieceCount: 1 } }); assert.equal(customCtx([p3]).pairPlan(p3), null, 'an explicit count of one piece is a single ear, not a pair');
  });
  await t('9b which ear each piece is: one body twice = a Left and its mirror; two bodies of one file by where they sit; two files by the order dropped', async () => {
    const pair = rowOf(1, 10, { spec: { form: 'earrings', quantity: 1 } }), ctx = customCtx([pair]);
    const bodyA = { bbox: [0, 0, 20, 30] }, bodyB = { bbox: [30, 0, 50, 30] };
    const e = { files: [{ id: 'f1', charms: [bodyA] }, { id: 'f2', charms: [bodyA] }, { id: 'f3', charms: [bodyA, bodyB] }, { id: 'f4', charms: [bodyB, bodyA] }] };
    const same = await ctx.earsOf(pair, e, [{ f: 'f1', i: 0 }, { f: 'f1', i: 0 }]);
    assert.deepEqual([...same].map(x => [x.side, x.mirror]), [['L', false], ['R', true]]);
    const two = await ctx.earsOf(pair, e, [{ f: 'f3', i: 0 }, { f: 'f3', i: 1 }]); assert.deepEqual([...two].map(x => [x.side, x.mirror, x.bodyIndex]), [['L', false, 0], ['R', false, 1]], 'the body on the left is the Left, both as drawn');
    const flipped = await ctx.earsOf(pair, e, [{ f: 'f4', i: 0 }, { f: 'f4', i: 1 }]); assert.deepEqual([...flipped].map(x => x.side), ['R', 'L'], 'by position, not by list order');
    const files = await ctx.earsOf(pair, e, [{ f: 'f1', i: 0 }, { f: 'f2', i: 0 }]); assert.deepEqual([...files].map(x => [x.side, x.mirror, !!x.assumed]), [['L', false, true], ['R', false, true]], 'two files: drop order is assumed, and said so');
    assert.equal(await ctx.earsOf(pair, e, [{ f: 'f1', i: 0 }]), null, 'an older send of one piece reads as it was');
    const single = rowOf(4, 40, { spec: { form: 'necklace', quantity: 1 } }); assert.equal(await ctx.earsOf(single, e, [{ f: 'f1', i: 0 }, { f: 'f1', i: 0 }]), null);
    const two2 = rowOf(5, 50, { spec: { form: 'earrings', quantity: 2 } }), ctx2 = customCtx([two2]);
    const four = await ctx2.earsOf(two2, e, [{ f: 'f1', i: 0 }, { f: 'f1', i: 0 }, { f: 'f1', i: 0 }, { f: 'f1', i: 0 }]); assert.deepEqual([...four].map(x => x.side), ['L', 'R', 'L', 'R']);
  });
  await t('9c the Right of one body is its mirror image, made once', async () => {
    const pair = rowOf(1, 10, { spec: { form: 'earrings', quantity: 1 } }), ctx = customCtx([pair]);
    const base = mitten(), src = { id: 'cust:x', charms: [base], parsed: {} };
    const a = await ctx.mirroredOf(src, 0), b = await ctx.mirroredOf(src, 0);
    assert.equal(a, b); assert.equal(ctx.calls.silhouettes, 1, 'its silhouette is traced once'); assert.equal(a.mirrored, true); assert.equal(a.mirror, true); assert.equal(a.id, 'cust:x:0m');
    assert.deepEqual(base.outline.bbox, [0, 0, 50, 60], 'the Left is untouched'); assert.notDeepEqual(a.outline.subpaths, base.outline.subpaths);
  });
  await t('9d Send to Sheet waits, in words, until the designs make a Left and a Right for every pair', () => {
    const src = cutOut('  function notReady(e) {', '  function tooBig(F)');
    const ctx = { plural: (n, w) => `${n} ${w}${n === 1 ? '' : 's'}`, tooBig: () => false, labelOf: m => m, needOf: () => ({ total: 2, pairs: 2 }) }; vm.createContext(ctx); vm.runInContext(src, ctx);
    const F = (pieces, qty) => ({ id: 'f', name: 'ears.ai', state: 'ready', metal: 'gold', pieces, qty });
    const why = ctx.notReady({ files: [F(1, 1)] }); assert.match(why, /pair of earrings/); assert.match(why, /needs 2 pieces/); assert.match(why, /designs make 1/);
    assert.equal(ctx.notReady({ files: [F(1, 2)] }), '', 'one body cut twice is a pair'); assert.equal(ctx.notReady({ files: [F(2, 1)] }), '', 'both ears drawn in one file is a pair');
    ctx.needOf = () => ({ total: 1, pairs: 0 }); assert.equal(ctx.notReady({ files: [F(1, 1)] }), '', 'a card with no earring pair on it is judged exactly as before');
  });

  const bad = results.filter(r => r[1]);
  console.log(`\npairs-engrave: ${results.length - bad.length} of ${results.length} passed`);
  if (failed) process.exitCode = 1;
})().catch(e => { console.error(e); process.exitCode = 1; });
