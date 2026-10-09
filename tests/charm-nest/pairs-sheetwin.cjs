// PAIRSHEETWIN (area 10, Paul 9 Oct 2026): pairs, mismatched pairs and discs on the sheet window, the Library issues and the placement truth.
// Offline and pure: charm-nest-piece-placement.js required in node (groups, pairWords, placeWords, resolve, roll) and the sheet window's own helpers
// read as source; nothing is written anywhere. A line that is not a pair must read exactly as before: those cases are pinned against the resolver's
// output with no `parts` at all.
// Then, in headless Chromium over the fake site (bridge-server.cjs: the real charmNestLibrary ops over an in-memory store, nothing live), the sheet window itself:
// a mismatched pair with Left on GF Sheet 1 and Right on GF Sheet 2 (the fake's pool rows carry side / groupKey, as PAIRPOOL writes them), a normal order beside it.
//   node tests/charm-nest/pairs-sheetwin.cjs [--pure]       (PW_DIR=<playwright-core's node_modules>, CHROMIUM=<chrome>; --pure skips the browser part)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const PP = require(path.join(root, 'charm-nest-piece-placement.js'));
let n = 0; const t = (name, fn) => { try { fn(); n++; console.log('ok   ' + name); } catch (e) { console.error('FAIL ' + name + '\n' + (e.stack || e)); process.exitCode = 1; } };

const RID = '4181000100', TX = '5000000011';
const piece = (copy, o) => Object.assign({ key: `${RID}_${TX}_${copy}`, poolId: `${RID}_${TX}_${copy}`, lineKey: `${RID}_${TX}`, copy, nested: false, sheetId: null, sheetLabel: null, setId: null }, o || {});
const on = (copy, sheetId, label, setId, o) => piece(copy, Object.assign({ nested: true, sheetId, sheetLabel: label, setId: setId || null }, o || {}));

t('a mismatched pair, Left on GF Sheet 1 and Right on RG Sheet 2: the group is split, and says so plainly', () => {
  const [g] = PP.groups([on(1, 's1', 'GF Sheet 1', 'set-1', { side: 'L', kind: 'mismatched' }), on(2, 's2', 'RG Sheet 2', 'set-1', { side: 'R', kind: 'mismatched' })]);
  assert.equal(g.kind, 'mismatched'); assert.equal(g.n, 2); assert.equal(g.split, true); assert.equal(g.setState, 'same'); assert.equal(g.pairs, 1);
  assert.equal(PP.placeWords(g), 'Left on GF Sheet 1, Right on RG Sheet 2');
  const w = PP.pairWords(g, 's1');
  assert.equal(w.text, 'Right piece is on RG Sheet 2, in the same set.'); assert.equal(w.set, 'same'); assert.equal(w.here.length, 1);
  assert.equal(PP.pairWords(g, 's2').text, 'Left piece is on GF Sheet 1, in the same set.');
});
t('the other piece on a sheet of ANOTHER set, or of no set, is said so (R3)', () => {
  const [a] = PP.groups([on(1, 's1', 'GF Sheet 1', 'set-1', { side: 'L' }), on(2, 's3', 'GF Sheet 3', 'set-2', { side: 'R' })]);
  assert.equal(a.setState, 'other'); assert.equal(PP.pairWords(a, 's1').text, 'Right piece is on GF Sheet 3, in another set.');
  const [b] = PP.groups([on(1, 's1', 'GF Sheet 1', null, { side: 'L' }), on(2, 's3', 'GF Sheet 3', null, { side: 'R' })]);
  assert.equal(b.setState, 'none'); assert.equal(PP.pairWords(b, 's1').text, 'Right piece is on GF Sheet 3, in no set.');
});
t('a piece on no sheet, or on hold, is named by its side', () => {
  const [a] = PP.groups([on(1, 's1', 'GF Sheet 1', 'set-1', { side: 'L' }), piece(2, { side: 'R' })]);
  assert.equal(a.split, true); assert.equal(PP.pairWords(a, 's1').text, 'Right piece is not on a sheet yet.'); assert.equal(PP.placeWords(a), 'Left on GF Sheet 1, Right not on a sheet yet');
  const [h] = PP.groups([on(1, 's1', 'GF Sheet 1', 'set-1', { side: 'L' }), piece(2, { side: 'R', hold: { reason: 'x' } })]);
  assert.equal(PP.pairWords(h, 's1').text, 'Right piece is on hold.');
});
t('a pair together on one sheet says nothing and is not split', () => {
  const [g] = PP.groups([on(1, 's1', 'GF Sheet 1', 'set-1', { side: 'L' }), on(2, 's1', 'GF Sheet 1', 'set-1', { side: 'R' })]);
  assert.equal(g.split, false); assert.equal(PP.pairWords(g, 's1').text, ''); assert.equal(g.pairs, 1);
});
t('a MATCHING pair (no side) split over two sheets is told too, without inventing a side', () => {
  const [g] = PP.groups([on(1, 's1', 'GF Sheet 1', 'set-1'), on(2, 's2', 'GF Sheet 2', 'set-1')]);
  assert.equal(g.kind, 'pair'); assert.equal(g.hasSides, false); assert.equal(g.split, true);
  assert.equal(PP.pairWords(g, 's1').text, 'The other piece is on GF Sheet 2, in the same set.');
});
t('discs: three pieces, two on another sheet, are told by place', () => {
  const [g] = PP.groups([on(1, 's1', 'SS Sheet 1', 'set-1'), on(2, 's2', 'SS Sheet 2', 'set-1'), on(3, 's2', 'SS Sheet 2', 'set-1')]);
  assert.equal(g.kind, 'multi'); assert.equal(g.pairs, 0); assert.equal(PP.pairWords(g, 's1').text, '2 of its 3 pieces are on SS Sheet 2, in the same set.');
});
t('a piece its line says is alone (groupSize 1) is no group; old records with no fields group by pool id', () => {
  const gs = PP.groups([on(1, 's1', 'GF Sheet 1', null, { groupSize: 1 }), on(2, 's2', 'GF Sheet 2', null, { groupSize: 1 })]);
  assert.equal(gs.length, 2); assert.ok(gs.every(g => g.n === 1 && !g.split));
  const old = PP.groups([{ poolId: `${RID}_${TX}_1`, nested: true, sheetId: 'a', sheetLabel: 'GF Sheet 1' }, { poolId: `${RID}_${TX}_2`, nested: true, sheetId: 'b', sheetLabel: 'GF Sheet 2' }]);
  assert.equal(old.length, 1); assert.equal(old[0].key, `${RID}:${TX}`); assert.equal(old[0].split, true);
});
t('two lines of one order are two groups; a quantity-2 mismatched line (L,R,L,R) is one group of 4 with 2 pairs', () => {
  const two = PP.groups([on(1, 's1', 'GF Sheet 1'), on(1, 's1', 'GF Sheet 1', null, { key: `${RID}_5000000012_1`, poolId: `${RID}_5000000012_1`, lineKey: `${RID}_5000000012` })]);
  assert.equal(two.length, 2);
  const q = PP.groups([on(1, 's1', 'A', 'x', { side: 'L' }), on(2, 's1', 'A', 'x', { side: 'R' }), on(3, 's1', 'A', 'x', { side: 'L' }), on(4, 's1', 'A', 'x', { side: 'R' })]);
  assert.equal(q[0].n, 4); assert.equal(q[0].pairs, 2); assert.equal(q[0].kind, 'pair'); assert.equal(q[0].lefts, 2); assert.equal(q[0].rights, 2);   // (amendment 2: an ear with no kind told is an earring pair)
});
t('amendment 2: a MATCHING pair carries Left and Right too, and is told with its ears like a mismatched one', () => {
  const [g] = PP.groups([on(1, 's1', 'GF Sheet 1', 'set-1', { side: 'L', kind: 'pair' }), on(2, 's2', 'GF Sheet 2', 'set-2', { side: 'R', kind: 'pair' })]);
  assert.equal(g.kind, 'pair'); assert.equal(g.hasSides, true); assert.equal(g.pairs, 1); assert.equal(g.halves, 0);
  assert.equal(PP.pairWords(g, 's1').text, 'Right piece is on GF Sheet 2, in another set.'); assert.equal(PP.placeWords(g), 'Left on GF Sheet 1, Right on GF Sheet 2');
  // two Left pieces and one Right (a Right still missing): one pair and one half, never two pairs
  const [h] = PP.groups([piece(1, { side: 'L' }), piece(2, { side: 'R' }), piece(3, { side: 'L' })]); assert.equal(h.pairs, 1); assert.equal(h.halves, 1);
});

// ── the resolver: what changes only where the ears are known, and nothing else ──
const facts = (copies, o) => Object.assign({ key: `${RID}_${TX}`, name: 'MISMATCHED 7134', metal: 'gold', cancelled: false, hold: null, hand: null, copies: copies.length, copiesOn: copies.filter(c => c.nested).length, sheets: [], parts: copies }, o);
const sheetsOf = cs => { const out = []; for (const c of cs) if (c.nested && !out.some(s => s.id === c.sheetId)) out.push({ id: c.sheetId, label: c.sheetLabel, setId: c.setId, pools: [] }); return out; };
t('resolve: a split mismatched pair keeps `text` and `state` and adds parts, sides, split and the plain words', () => {
  const cs = [on(1, 's1', 'GF Sheet 1', 'set-1', { side: 'L' }), on(2, 's2', 'RG Sheet 2', 'set-1', { side: 'R' })];
  const p = PP.resolve(facts(cs, { sheets: sheetsOf(cs) })), bare = PP.resolve(facts(cs, { sheets: sheetsOf(cs), parts: undefined }));
  assert.equal(p.state, 'sheet'); assert.equal(p.text, bare.text); assert.equal(p.text, 'GF Sheet 1');
  assert.equal(p.split, true); assert.equal(p.splitGroups, 1); assert.equal(p.words, 'Left on GF Sheet 1, Right on RG Sheet 2'); assert.equal(p.say, 'Left on GF Sheet 1, Right on RG Sheet 2');
  assert.ok(p.sides.L && p.sides.R && p.sides.L.key.endsWith('_1')); assert.notEqual(p.sig, bare.sig);
});
t('resolve: half of a mismatched pair on a sheet says which piece is not on one yet', () => {
  const cs = [on(1, 's1', 'GF Sheet 1', 'set-1', { side: 'L' }), piece(2, { side: 'R' })];
  const p = PP.resolve(facts(cs, { sheets: sheetsOf(cs), copiesOn: 1 }));
  assert.equal(p.partial, true); assert.equal(p.why, 'Right piece is not on a sheet yet'); assert.equal(p.say, 'On GF Sheet 1; Right piece is not on a sheet yet'); assert.equal(p.text, 'GF Sheet 1');
});
t('resolve: a line with no sides reads exactly as before (words, sig), with or without parts', () => {
  const cs = [on(1, 's1', 'GF Sheet 1', 'set-1'), on(2, 's1', 'GF Sheet 1', 'set-1')];
  const a = PP.resolve(facts(cs, { sheets: sheetsOf(cs) })), b = PP.resolve(facts(cs, { sheets: sheetsOf(cs), parts: undefined }));
  assert.equal(a.sig, b.sig); assert.equal(a.say, b.say); assert.equal(a.why, b.why); assert.equal(a.text, b.text); assert.equal(a.split, false);
  const half = [on(1, 's1', 'GF Sheet 1', 'set-1'), piece(2)];
  const c = PP.resolve(facts(half, { sheets: sheetsOf(half), copiesOn: 1 })), d = PP.resolve(facts(half, { sheets: sheetsOf(half), copiesOn: 1, parts: undefined }));
  assert.equal(c.say, d.say); assert.equal(c.why, d.why); assert.equal(c.sig, d.sig === c.sig ? c.sig : d.sig + '~' + c.pairSig);
});
t('resolve: held, cancelled, hand and waiting answers are untouched by parts', () => {
  const cs = [piece(1, { side: 'L' }), piece(2, { side: 'R' })];
  const w = PP.resolve(facts(cs)), h = PP.resolve(facts(cs, { hold: { reason: 'x' } })), c = PP.resolve(facts(cs, { cancelled: true }));
  assert.equal(w.state, 'waiting'); assert.equal(w.text, PP.WAIT_TEXT); assert.equal(h.state, 'hold'); assert.equal(c.state, 'cancelled'); assert.equal(w.split, false);
});
t('pieces whose sheets are not read yet are neither here nor away: nothing is said of them', () => {
  const [g] = PP.groups([piece(1, { side: 'L', loading: true }), piece(2, { side: 'R', loading: true })]);
  assert.equal(PP.pairWords(g, 's1').text, ''); assert.equal(g.split, false);
});
t('roll: the order counts how many of its groups are split and tells where', () => {
  const cs = [on(1, 's1', 'GF Sheet 1', 'set-1', { side: 'L' }), on(2, 's2', 'RG Sheet 2', 'set-1', { side: 'R' })];
  const split = PP.resolve(facts(cs, { sheets: sheetsOf(cs) })), single = PP.resolve({ key: 'x', copies: 1, copiesOn: 1, sheets: [{ id: 's1', label: 'GF Sheet 1' }] });
  const r = PP.roll([split, single]); assert.equal(r.splitPairs, 1); assert.equal(r.words, 'Left on GF Sheet 1, Right on RG Sheet 2');
  assert.equal(PP.roll([single]).splitPairs, undefined);
});
t('sheetCounts: a pair with both ears on the place is a pair, one ear whose other is elsewhere is a half pair, discs and singles are pieces only', () => {
  const gs = PP.groups([on(1, 's1', 'GF Sheet 1', 'set-1', { side: 'L' }), on(2, 's1', 'GF Sheet 1', 'set-1', { side: 'R' }),
    on(1, 's1', 'GF Sheet 1', 'set-1', { side: 'L', key: `${RID}_5000000012_1`, poolId: `${RID}_5000000012_1`, lineKey: `${RID}_5000000012` }), on(2, 's2', 'GF Sheet 2', 'set-1', { side: 'R', key: `${RID}_5000000012_2`, poolId: `${RID}_5000000012_2`, lineKey: `${RID}_5000000012` }),
    on(1, 's1', 'GF Sheet 1', 'set-1', { key: `${RID}_5000000013_1`, poolId: `${RID}_5000000013_1`, lineKey: `${RID}_5000000013` })]);
  const c1 = PP.sheetCounts(gs, ['s1']); assert.deepEqual(c1, { pieces: 4, pairs: 1, halves: 1 });
  assert.deepEqual(PP.sheetCounts(gs, ['s2']), { pieces: 1, pairs: 0, halves: 1 }); assert.deepEqual(PP.sheetCounts(gs, ['s1', 's2']), { pieces: 5, pairs: 2, halves: 0 });   // (a set of both sheets: the split pair is whole again)
  assert.equal(PP.countWords(c1), '4 pieces, 1 pair, 1 half pair'); assert.equal(PP.countWords({ pieces: 1, pairs: 0, halves: 0 }), '1 piece'); assert.equal(PP.countWords({ pieces: 0, pairs: 0, halves: 0 }), '');
  assert.deepEqual(PP.sheetCounts(PP.groups([on(1, 's1', 'A', null), on(2, 's1', 'A', null), on(3, 's1', 'A', null)]), ['s1']), { pieces: 3, pairs: 0, halves: 0 });   // (discs: no pairs said)
});
t('the page face exposes the same helpers', () => {
  const page = PP.makePage({}); for (const k of ['groups', 'pairWords', 'placeWords', 'sideWord']) assert.equal(typeof page[k], 'function', k);
  assert.equal(page.sideWord('L'), 'Left'); assert.equal(page.sideWord('R'), 'Right'); assert.equal(page.sideWord(null), '');
});
console.log(`\n${n} checks (pure)`);

/* ═══════════════════════════ the sheet window, in a browser ═══════════════════════════ */
if (process.argv.includes('--pure') || process.exitCode) return;
(async () => {
  const pwDir = process.env.PW_DIR || [path.join(root, 'node_modules'), '/opt/node22/lib/node_modules/playwright/node_modules'].find(d => fs.existsSync(path.join(d, 'playwright-core')));
  const { chromium } = require(path.join(pwDir, 'playwright-core'));
  const { start } = require('./bridge-server.cjs');
  const noNested = require('./_noNestedArrays.cjs');
  const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', SETS = 'Charm_Nest_Sets';
  const A = '4300000101', B = '4300000102', C = '4300000103', D = '4300000105';
  const pid = (rid, tx, copy) => `${rid}_${5000000000 + tx}_${copy}`;
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin } = srv;
  // GF Sheet 1 (set-1): the pair's Left (A tx1 copy 1), the normal order B, the filler C. GF Sheet 2 (set-1): the pair's Right (A tx1 copy 2) and a filler.
  const S1 = [[A, 1, 1], [B, 2, 1], [C, 3, 1], [D, 5, 1], [D, 5, 2]], S2 = [[A, 1, 2], ['4300000104', 4, 1]];
  const sheet = (id, n, list) => ({ id, metal: 'gold', setId: 'set-1', charms: list.map(([r, t, c], i) => ({ id: id + i, poolId: pid(r, t, c), order: r, sku: 'TEST-' + t, name: `${r} · TEST-${t}`, ...(r === A || r === D ? { side: c === 1 ? 'L' : 'R', mirror: c === 2, bodyIndex: r === A ? c - 1 : 0, groupKey: `${r}:${5000000000 + t}` } : {}) })),
    placements: list.map((x, i) => ({ id: id + i, cxPt: 30 + i * 40, cyPt: 40, angle: 0, wPt: 20, hPt: 20 })), poolIds: list.map(([r, t, c]) => pid(r, t, c)), orders: [...new Set(list.map(x => x[0]))], sheetIndex: n, fileBase: `GF_2026-10-09_Set-1_Sheet-${n}`, runId: 'run-test-1', placedCount: list.length, charmCount: list.length });
  for (const d of [sheet('gold-open-1', 1, S1), sheet('gold-open-2', 2, S2)]) { noNested(d); st.put(SHEETS, d.id, d); }
  st.put(SETS, 'set-1', { id: 'set-1', seq: 1, sheetIds: ['gold-open-1', 'gold-open-2'], runId: 'run-test-1' });
  for (const [sid, list] of [['gold-open-1', S1], ['gold-open-2', S2]]) for (const [r, t, c] of list) st.put(POOL, pid(r, t, c), { poolId: pid(r, t, c), orderId: r, sheetId: sid, setId: 'set-1', state: 'placed', material: 'gold', lineKey: `${r}_${5000000000 + t}`, sku: 'TEST-' + t,
    ...(r === A || r === D ? { side: c === 1 ? 'L' : 'R', mirror: c === 2, bodyIndex: r === A ? c - 1 : 0, groupKey: `${r}:${5000000000 + t}`, groupSize: 2, kind: r === A ? 'mismatched' : 'pair' } : {}) });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 950 } });
  await ctx.route(url => !/^https?:\/\/(127\.0\.0\.1|localhost)[:/]/.test(url.href), r => {
    const u = r.request().url();
    if (/gstatic\.com\/firebasejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(u) ? '' : "const nope = () => { throw new Error('firebase stub'); }; export const getFirestore = nope, initializeApp = nope;" });
    if (/qrcodejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) });
    return r.abort();
  });
  await ctx.addInitScript(() => { try { if (!localStorage.getItem('cn.employee')) localStorage.setItem('cn.employee', 'Tester'); const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); s.pollOrders = 'off'; s.runMode = 'manual'; localStorage.setItem('cn.settings', JSON.stringify(s)); } catch (_) {} window.confirm = () => true; window.alert = () => {}; });
  const page = await ctx.newPage(), errors = [];
  page.on('pageerror', e => errors.push('page: ' + e.message));
  page.on('console', m => { if (m.type() === 'error' && !/firebase stub|Failed to load resource|ERR_FAILED|net::/.test(m.text())) errors.push('console: ' + m.text().slice(0, 300)); });
  const until = async (fn, ms = 20000, what = '') => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await new Promise(r => setTimeout(r, 100)); } };
  try {
    await page.goto(`${sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.SheetWin && window.OrderPieces && window.PiecePlacement && window.PiecePlacement.groups, null, { timeout: 60000 });
    await page.evaluate(([a, b, c, d]) => {   // the lines the order rows carry (the pull): the pair is one line of quantity 1 that makes two pieces
      const pid = (rid, tx, copy) => `${rid}_${5000000000 + tx}_${copy}`;
      const row = (rid, tx, n) => ({ key: `${rid}_${5000000000 + tx}`, order: { receiptId: rid, orderNumber: rid, createTs: 1790000000, updateTs: 1790000000, shipBy: 1790500000, buyer: { name: 'Buyer' }, lines: [], messages: [] },
        line: { transactionId: String(5000000000 + tx), listingId: '', sku: 'TEST-' + tx, title: 'Test ' + tx, quantity: 1, variations: [], personalization: [] }, spec: { designSku: 'TEST-' + tx, quantity: 1, material: 'gold', problems: [] },
        problems: [], state: 'written', reason: null, poolIds: Array.from({ length: n }, (_, i) => pid(rid, tx, i + 1)), engrave: null, material: 'gold', arrivedAt: Date.now() - 7200000 });
      window.B.orders.rows = [row(a, 1, 2), row(b, 2, 1), row(c, 3, 1), row(d, 5, 2)]; window.B.orders.byKey = new Map(window.B.orders.rows.map(r => [r.key, r]));
    }, [A, B, C, D]);
    await page.evaluate(id => SheetWin.open('gold-open-1', { select: id }), pid(A, 1, 1));
    await until(() => page.evaluate(() => /Right piece is on/.test((document.querySelector('.swPairNote:not([hidden])') || {}).innerText || '')), 25000, 'the pair note');
    const read = () => page.evaluate(() => ({
      note: (document.querySelector('.swPairNote') || {}).innerText || '', facts: (document.querySelector('[data-r2=mm]') || {}).textContent || '',
      rows: [...document.querySelectorAll('.swTrail li')].map(li => li.innerText.replace(/\s+/g, ' ').trim()), btn: (document.querySelector('.swPairNote [data-open-sheet]') || {}).dataset ? document.querySelector('.swPairNote [data-open-sheet]').dataset.openSheet : '',
      strip: (document.querySelector('.swStrip, [data-r2=strip]') || {}).innerText || '' }));
    let v = await read();
    assert.match(v.note, /Right piece is on GF Sheet 2, in the same set\./, 'the pair note says where the Right piece is, and that the sheets share a set: ' + v.note);
    assert.match(v.facts, /^Left piece · /, 'the piece pane says Left: ' + v.facts);
    assert.equal(v.rows.length, 2, 'the trail lists both pieces: ' + JSON.stringify(v.rows));
    assert.ok(/Left/.test(v.rows[0]) && /this charm/.test(v.rows[0]), 'row 1 is this Left charm: ' + v.rows[0]);
    assert.ok(/Right/.test(v.rows[1]) && /GF Sheet 2/.test(v.rows[1]), 'row 2 is the Right piece on GF Sheet 2: ' + v.rows[1]);
    assert.equal(v.btn, 'gold-open-2', 'a link opens the other sheet');
    console.log('ok   sheet window: Left piece on Sheet 1 says "Right piece is on GF Sheet 2, in the same set" with a link, both pieces listed with their ears');
    // amendment 2: the strip counts pairs (both ears here) and half pairs (one ear here, the other on another sheet); a matching pair is told with its ears too
    const readStrip = () => page.evaluate(() => [...document.querySelectorAll('.swStrip span')].map(e => e.innerText.replace(/\s+/g, ' ').trim()));
    await until(async () => (await readStrip()).includes('1 pair'), 8000, 'the pair count').catch(() => {}); const strip = await readStrip();
    assert.ok(strip.includes('1 pair'), 'the matching pair D, both ears here, counts as a pair: ' + JSON.stringify(strip));
    assert.ok(strip.includes('1 half pair'), 'the mismatched pair A, one ear here, is a half pair: ' + JSON.stringify(strip));
    const flags = await page.evaluate(([a, d]) => Object.fromEntries(SheetWin._W.pieces.filter(x => x.rid === a || x.rid === d).map(x => [x.poolId, [x.side, x.mirror, x.bodyIndex]])), [A, D]);
    assert.deepEqual(flags[pid(D, 5, 1)], ['L', false, 0]); assert.deepEqual(flags[pid(D, 5, 2)], ['R', true, 0]); assert.deepEqual(flags[pid(A, 1, 1)], ['L', false, 0]);
    await page.evaluate(id => SheetWin.open('gold-open-1', { select: id }), pid(D, 5, 2));
    await until(() => page.evaluate(() => /^Right piece · /.test((document.querySelector('[data-r2=mm]') || {}).textContent || '')), 20000, 'the Right piece of the matching pair');
    const dn = await page.evaluate(() => { const n = document.querySelector('.swPairNote'); return { hidden: !n || n.hidden, rows: [...document.querySelectorAll('.swTrail li')].map(li => li.innerText.replace(/\s+/g, ' ').trim()) }; });
    assert.equal(dn.hidden, true, 'a pair whose ears are both on this sheet says nothing about another sheet'); assert.ok(dn.rows.length === 2 && /Left/.test(dn.rows.join('|')) && /Right/.test(dn.rows.join('|')), 'both ears listed: ' + JSON.stringify(dn.rows));
    console.log('ok   strip: "1 pair" (D, both ears here) and "1 half pair" (A); a matching pair names Left and Right, and says nothing when both are here; mirror flags read from the saved charms');
    // the drawing of a piece: an ear with a body is drawn from Pool.ensureBase (its own body, mirrored when the saved charm says mirror true); everything else from the base itself
    const pb = await page.evaluate(async () => {
      const base = { id: 'base' }, own = { id: 'own' }, g = { pool: true, base, src: { id: 'src' } }, calls = [], real = Pool.ensureBase;
      Pool.ensureBase = async (src, piece) => { calls.push([src.id, piece.side, piece.bodyIndex, piece.mirror]); if (piece.side === 'R' && piece.bodyIndex === 9) throw new Error('no body'); return own; };
      try {
        const plain = await SheetWin.pieceBase(g, {}), old = await SheetWin.pieceBase(g, { side: 'L' }), noSrc = await SheetWin.pieceBase({ pool: true, base }, { side: 'L', bodyIndex: 0 });
        const left = await SheetWin.pieceBase(g, { side: 'L', bodyIndex: 0, mirror: false }), right = await SheetWin.pieceBase(g, { side: 'R', bodyIndex: 1, mirror: true }), bad = await SheetWin.pieceBase(g, { side: 'R', bodyIndex: 9, mirror: true });
        Pool.ensureBase = undefined; const noPool = await SheetWin.pieceBase(g, { side: 'R', bodyIndex: 0, mirror: true });
        return { plain: plain === base, old: old === base, noSrc: noSrc === base, left: left === own, right: right === own, bad, noPool: noPool === base, calls };
      } finally { Pool.ensureBase = real; }
    });
    assert.ok(pb.plain && pb.old && pb.noSrc && pb.noPool, 'an unsided piece, an old piece with no body, and a page without Pool.ensureBase draw the base itself: ' + JSON.stringify(pb));
    assert.ok(pb.left && pb.right && pb.bad === null, 'an ear with a body asks Pool.ensureBase, and a body that cannot be made is an outline (null), never the wrong body: ' + JSON.stringify(pb));
    assert.deepEqual(pb.calls, [['src', 'L', 0, false], ['src', 'R', 1, true], ['src', 'R', 9, true]], 'it asks for exactly the ear, body and mirror the saved charm says');
    console.log('ok   piece drawing: an ear with a body is drawn from its own (mirrored) body by Pool.ensureBase; unsided and old pieces from the base, as before');
    // Library: the issue chip names the ear of the piece that holds an order back; a piece count with a pair on it says its pieces and pairs on hover
    const lis = await page.evaluate(([a, b, p2, p1, pb]) => {
      const feed = pieces => ({ id: 'gold-open-1', label: 'GF Sheet 1', code: 'GF', metal: 'gold', issues: pieces.map(([rid, ps, key]) => ({ step: 'orders', key: key || 'otherSheetNotReady', orderId: rid, orderLabel: 'Order ' + rid, pieces: ps, open: { type: 'order', id: rid } })) });
      const chip = (rid, ps, key) => LibraryIssues.model(feed([[rid, ps, key]])).orders[0].reason.chip;
      const r2 = { poolId: p2, sheetLabel: 'GF Sheet 2' }, r1 = { poolId: p1, sheetLabel: 'GF Sheet 2' };
      return { right: chip(a, [r2]), pair: chip(a, [r2, r1]), left: chip(a, [r1]), pooled: chip(a, [{ poolId: p2, sheetLabel: null }], 'pooled'), plain: chip(b, [{ poolId: pb, sheetLabel: 'GF Sheet 2' }]), told: chip(a, [{ poolId: 'x', side: 'R', sheetLabel: 'GF Sheet 2' }]),
        noSide: chip(a, [{ sheetLabel: 'GF Sheet 2' }]), dot: LibraryIssues.model(feed([[a, [{ poolId: p2, side: 'R', sheetLabel: 'GF Sheet 2' }]]])).orders[0].dots.find(d => d.ring).side };
    }, [A, B, pid(A, 1, 2), pid(A, 1, 1), pid(B, 2, 1)]);
    assert.equal(lis.right, 'Right piece waits on GF Sheet 2', 'one Right piece holds the order back: ' + lis.right); assert.equal(lis.left, 'Left piece waits on GF Sheet 2'); assert.equal(lis.pair, 'Pair waits on GF Sheet 2', 'both ears: ' + lis.pair);
    assert.equal(lis.pooled, 'Right piece not on a sheet yet'); assert.equal(lis.plain, 'Waits on GF Sheet 2', 'a normal order says what it always said: ' + lis.plain); assert.equal(lis.told, 'Right piece waits on GF Sheet 2', 'the issue piece\'s own side counts');
    assert.equal(lis.noSide, 'Waits on GF Sheet 2', 'a piece nothing knows the ear of says what it always said'); assert.equal(lis.dot, 'R', 'the dot carries the ear');
    // (a sheet the Library rows do not hold: the hover reads it whole once, and the words come as that lands)
    const title = await page.evaluate(async () => {
      const host = document.getElementById('libBody'), mk = (id, html) => { const e = document.createElement('span'); e.setAttribute('data-sheet-count', id); e.innerHTML = html; host.appendChild(e); return e; };
      const sp = mk('gold-open-1', '<b>5</b>/5'), none = mk('gold-open-2', '<b>2</b>/2'), lone = mk('gold-open-1', '<b>3</b>/3');
      for (const e of [sp, none, lone]) e.dispatchEvent(new MouseEvent('mouseover', { bubbles: true }));
      for (let i = 0; i < 100 && !(sp.title && none.title); i++) await new Promise(r => setTimeout(r, 100));
      const out = { s1: sp.title, s2: none.title, wrongNumber: lone.title }; sp.remove(); none.remove(); lone.remove(); return out;
    });
    assert.equal(title.s1, '5 pieces, 1 pair, 1 half pair', 'GF Sheet 1 holds the pair D and one ear of A: ' + JSON.stringify(title)); assert.equal(title.s2, '2 pieces, 1 half pair', 'GF Sheet 2 holds the other ear of A: ' + JSON.stringify(title));
    assert.equal(title.wrongNumber, '', 'a card whose printed number is not the pieces counted gets no words: ' + JSON.stringify(title));
    console.log('ok   Library: the issue chip says "Right piece waits on GF Sheet 2" / "Pair waits on ..." and a normal order is unchanged; a card\'s piece count says "5 pieces, 1 pair, 1 half pair" on hover');
    // the orders list and the strip: the pair row names its ear, the normal order reads as before
    await page.evaluate(() => SheetWin.close()); await page.waitForTimeout(700);
    await page.evaluate(() => SheetWin.open('gold-open-1'));
    await until(() => page.evaluate(() => document.querySelectorAll('.swOrd').length >= 3), 25000, 'the orders list');
    const list = await page.evaluate(() => [...document.querySelectorAll('.swOrd')].map(li => ({ rid: li.dataset.rid, what: li.querySelector('.what').textContent, tags: [...li.querySelectorAll('.swTag')].map(t => t.title) })));
    const la = list.find(x => x.rid === A), lb = list.find(x => x.rid === B);
    assert.equal(la.what, 'TEST-1 · Left', 'the pair row names its ear: ' + la.what); assert.ok(la.tags.some(t => /its Right piece/.test(t)), 'its other-sheet tag says which piece: ' + JSON.stringify(la.tags));
    assert.equal(lb.what, 'TEST-2', 'a normal order reads as before: ' + lb.what); assert.ok(!lb.tags.some(t => /^Also on/.test(t)), 'and has no other-sheet tag: ' + JSON.stringify(lb.tags));
    console.log('ok   orders list: the pair row says "TEST-1 · Left" and where its Right piece is; a normal order reads as before');
    // the link opens the other sheet and selects the Right piece, whose note now points back
    await page.evaluate(id => SheetWin.open('gold-open-1', { select: id }), pid(A, 1, 1));
    await until(() => page.evaluate(() => !!document.querySelector('.swPairNote [data-open-sheet]')), 20000, 'the link');
    await page.click('.swPairNote [data-open-sheet]');
    await until(() => page.evaluate(() => /Left piece is on GF Sheet 1/.test((document.querySelector('.swPairNote') || {}).innerText || '')), 25000, 'the Right piece selected on Sheet 2');
    v = await read();
    assert.match(v.facts, /^Right piece · /, v.facts);
    console.log('ok   the link opens GF Sheet 2 on its Right piece, whose note says "Left piece is on GF Sheet 1, in the same set"');
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
    console.log('\npairs-sheetwin: all passed');
  } catch (e) { console.error('errors:', errors); throw e; }
  finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
