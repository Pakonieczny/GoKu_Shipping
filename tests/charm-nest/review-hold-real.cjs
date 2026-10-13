/* REVLIB over the REAL CharmNestReviewHold (REVHOLD, charm-nest-review-hold.js): a fake Review mark and fake OrderPieces stand in for REVMARK and the pieces list, and
   nothing is planted in the readiness. An order is marked in Review: the sheet its piece sits on is held, the Library's Approve line names the order and the piece, the '!' list has
   the row, the note appears under a set sheet's rail; the order is resolved: all of it goes at once. Offline, jsdom, fixtures only.
     NODE_PATH=<dir with jsdom> node tests/charm-nest/review-hold-real.cjs                                                                                                         */
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
let JSDOM; try { ({ JSDOM } = require('jsdom')); } catch (_) { console.log('SKIP: jsdom is not installed (set NODE_PATH)'); process.exit(0); }
const ROOT = process.env.REVLIB_ROOT || path.join(__dirname, '../..');
if (!fs.existsSync(path.join(ROOT, 'charm-nest-review-hold.js'))) { console.log('SKIP: charm-nest-review-hold.js is not in this build yet'); process.exit(0); }
const read = f => fs.readFileSync(path.join(ROOT, f), 'utf8');
const dom = new JSDOM('<body><div class="topbar"></div><main id="libBody"></main></body>', { runScripts: 'outside-only', pretendToBeVisual: true, url: 'https://example.test/' });
const win = dom.window, document = win.document, frames = [], net = [];
win.matchMedia = () => ({ matches: true }); win.Element.prototype.getClientRects = function () { return [{}]; };
win.fetch = (...a) => { net.push(a[0]); return Promise.reject(new Error('offline')); };
win.eval(read('charm-nest-motion.js')); win.eval(read('charm-nest-readiness.js'));
let world = {};
const esc = x => String(x == null ? '' : x).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;');
const c = vm.createContext({ window: win, document, console, esc, cors: x => x, S: { mode: 'library', cloud: { ok: true } }, allSheets: () => [], Orders: { rows: () => [] }, Engrave: { items: () => new Map() }, O: { setLabel: n => 'Set ' + n }, CNListActivity: { compare: () => 0, compareBlocks: () => 0, state: () => ({ direction: 1 }) }, innerHeight: win.innerHeight, requestAnimationFrame: fn => (frames.push(fn), frames.length), setTimeout: win.setTimeout.bind(win), clearTimeout: win.clearTimeout.bind(win), setInterval() {}, api: async () => ({ sheets: Object.values(world), sets: [] }) });
vm.runInContext(read('charm-nest-bridge.js').slice(read('charm-nest-bridge.js').indexOf('const LaserReview ='), read('charm-nest-bridge.js').indexOf('const Sets =')), c);
const reviewOpens = [];
Object.assign(win, { LibraryFlow: { approve: async () => ({ ok: true, auto: [], needs: [], confirm: [], notes: [] }) }, CNEmployee: { name: () => 'Maria' }, setMode() {}, ListMedia: { peek: () => null, listing: () => Promise.resolve(null) }, openOrderFrom: () => true });
// the Review mark (REVMARK) and the pieces list, as fakes the real hold reads
const marks = new Set(), markSubs = new Set(), ORD = '4100000011', ORD_B = '4100000012';
win.CharmNestReviewMark = { markedRids: () => [...marks], isMarked: r => marks.has(String(r)), reasonOf: () => '', onChange: fn => { markSubs.add(fn); return () => markSubs.delete(fn); }, open: rid => { reviewOpens.push(String(rid)); return true; } };
const piece = (rid, n, side, sheetId) => ({ key: `${rid}_1_${n}`, poolId: `${rid}_1_${n}`, lineKey: `${rid}_1`, index: n, side, sheetId, sheetLabel: 'GF Sheet 1', state: 'nested', label: 'Charm', hand: false, problem: null, setId: null });
const PIECES = { [ORD]: [piece(ORD, 1, 'L', 'Q1'), piece(ORD, 2, 'R', 'Q1')], [ORD_B]: [piece(ORD_B, 1, null, 'S2')] };
win.OrderPieces = { of: rid => PIECES[rid] || [], subscribe: () => () => {}, load: () => Promise.resolve() };
win.eval(read('charm-nest-review-hold.js')); win.eval(read('charm-nest-library-review-hold.js')); win.eval(read('charm-nest-library-issues.js'));
const L = win.LaserReview, RH = win.LibraryReviewHold, LI = win.LibraryIssues, body = document.querySelector('#libBody');
const flush = () => { for (let i = 0; i < 8 && frames.length; i++) frames.splice(0).forEach(f => f()); };
const tick = (ms = 15) => new Promise(r => setTimeout(r, ms)), settle = async () => { await tick(); flush(); await tick(); flush(); await tick(); flush(); };
const sheetOf = (id, ids, orders) => ({ id, sheetIndex: 1, poolIds: ids, orders, placedCount: ids.length, verification: { ok: true }, preview: 'p', outputs: { ai: 'f' }, label: { files: [] }, backPool: [], engraving: Object.fromEntries(ids.map(i => [i, { needed: false, state: 'none', approved: true }])), orderReadiness: Object.fromEntries(orders.map(o => [o, { ready: true }])), updatedAt: 1 });
const flat = id => { const a = document.createElement('article'); a.className = 'librarySheet'; a.dataset.laserCard = 'sheet'; a._laserSheets = [id]; a.innerHTML = `<div class="libCard" data-id="${id}"><span data-sheet-status="${id}"></span></div>`; a.getBoundingClientRect = () => ({ left: 10, top: 80, right: 310, bottom: 430, width: 300, height: 350 }); return a; };
const setCard = (setId, ids) => { const a = document.createElement('div'); a.className = 'setCard'; a.dataset.laserCard = 'set'; a._laserSet = { setId, seq: 1, sheetIds: ids }; a._laserSheets = ids; a.innerHTML = `<div class="sh"><span class="nm" data-set-title></span></div><div class="sheetsRow">${ids.map(id => `<article class="librarySheet"><div class="libCard" data-id="${id}"><span data-sheet-status="${id}"></span></div></article>`).join('')}</div>`; a.getBoundingClientRect = () => ({ left: 10, top: 80, right: 310, bottom: 430, width: 300, height: 350 }); return a; };
const boxOf = (root, key) => root.querySelector(`.approveBox[data-approve-for="${key}"]`);
const recs = { Q1: sheetOf('Q1', [ORD + '_1_1', ORD + '_1_2'], [ORD]), S1: sheetOf('S1', ['5_1_1'], ['5']), S2: sheetOf('S2', [ORD_B + '_1_1'], [ORD_B]) };
Object.values(recs).forEach(L.record); world = { ...recs }; L.sections(body);
const q1 = flat('Q1'), set = setCard('setR', ['S1', 'S2']);
L.place(q1, L.canCut(recs.Q1), body); L.place(set, L.group(set._laserSet, [recs.S1, recs.S2]).ready, body);
const off = b => b.getAttribute('aria-disabled') === 'true';

(async () => {
  try {
    // nothing in Review (the sheets lack only their QR label, which a press makes): they are ready to approve, no word of a hold anywhere
    L.changed(); flush(); await settle();
    assert.equal(off(boxOf(q1, 'sheet:Q1').querySelector('[data-approve-btn]')), false); assert.equal(document.querySelectorAll('.rvhNote').length, 0);
    assert.equal(boxOf(set, 'set:setR').querySelector('[data-approve-why]').textContent, '');

    // 1. the pair's order is marked for Review: ONE change from the mark, and the Library says exactly which order and which pieces
    marks.add(ORD); [...markSubs].forEach(f => f({ rid: ORD, kind: 'marked' })); await settle();
    const q = boxOf(q1, 'sheet:Q1');
    assert.equal(off(q.querySelector('[data-approve-btn]')), true, 'the held sheet cannot be approved');
    assert.equal(q.querySelector('[data-approve-why]').textContent, `Held by order ${ORD} (left earring and right earring) in Review`);
    assert.equal(q.querySelector('[data-approve-review]').dataset.approveReview, ORD);
    q.querySelector('[data-approve-review]').click(); assert.equal(reviewOpens.join(), ORD, 'one tap to the Review tab (REVHOLD.openInReview -> the mark\'s open)'); reviewOpens.length = 0;
    // the '!' list has the order, said as In Review, not On hold
    const bang = q1.querySelector('.flowBox [data-issues-open]'); assert(bang, 'the rail has a \'!\''); bang.click(); await tick();
    const rows = [...document.querySelectorAll('#libIssuesPanel .lisRow')]; assert.equal(rows.length, 1); assert.equal(rows[0].querySelector('.lisWho b').textContent, ORD);
    assert.equal(rows[0].querySelector('.lisChip').textContent, 'In Review: left earring and right earring'); assert.equal(rows[0].dataset.go, 'review');
    rows[0].click(); await tick(); assert.equal(reviewOpens.join(), ORD); reviewOpens.length = 0; assert.equal(LI.isOpen(), false);

    // 2. a SET: the other order is marked, its piece is on S2: the set's one Approve says it once, S2 carries the note, S1 nothing
    marks.add(ORD_B); [...markSubs].forEach(f => f({ rid: ORD_B, kind: 'marked' })); await settle();
    assert.equal(boxOf(set, 'set:setR').querySelector('[data-approve-why]').textContent, `Held by order ${ORD_B} in Review`, 'a one-piece order is named without a piece');
    const sheetS2 = [...set.querySelectorAll('.librarySheet')].find(a => a.querySelector('.libCard').dataset.id === 'S2'), sheetS1 = [...set.querySelectorAll('.librarySheet')].find(a => a.querySelector('.libCard').dataset.id === 'S1');
    assert.equal(sheetS2.querySelector('.rvhNote').textContent, `Held by order ${ORD_B} in Review`); assert.equal(sheetS1.querySelectorAll('.rvhNote').length, 0);

    // 3. both orders are resolved in the Review tab: every word goes at once, the sheets are ready to approve again
    marks.delete(ORD); marks.delete(ORD_B); [...markSubs].forEach(f => f({ rid: ORD, kind: 'resolved' })); await settle();
    assert.equal(off(boxOf(q1, 'sheet:Q1').querySelector('[data-approve-btn]')), false, 'the held sheet can be approved again'); assert.equal(boxOf(q1, 'sheet:Q1').querySelector('[data-approve-why]').textContent, '');
    assert.equal(document.querySelectorAll('.rvhNote').length, 0); assert.equal(boxOf(set, 'set:setR').querySelector('[data-approve-why]').textContent, '');
    assert.equal(net.length, 0, 'no network call of its own');
    console.log('Review hold (real CharmNestReviewHold) OK: marked -> Approve line names the order and its pieces (one tap to Review), the \'!\' list says In Review, a set says it once and its held sheet carries the note; resolved -> all of it clears at once');
  } finally { win.close(); }
})().catch(e => { console.error(e); process.exitCode = 1; });
