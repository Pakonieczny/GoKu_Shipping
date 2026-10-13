/* A sheet held by an order in the Review tab says so, and exactly which order, wherever the app already says what holds a sheet back
   (REVLIB, Paul 12 Oct 2026: "especially in the library tab if a sheet is being held back. The user needs to be informed as to exactly what order is holding it
   back so the user can easily fix it"). Offline, jsdom, a fake CharmNestReviewHold (the real one is REVHOLD's): the real LaserReview (rails, Approve lines), the real
   '!' list (charm-nest-library-issues.js) and the real note module (charm-nest-library-review-hold.js) over records whose readiness carries the 'held' block REVHOLD folds in.
   No network, no Firebase, fixtures only ("Name One", receipt numbers that belong to nobody).
     NODE_PATH=<dir with jsdom> node tests/charm-nest/review-hold-ui.cjs                                                                                                     */
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
let JSDOM; try { ({ JSDOM } = require('jsdom')); } catch (_) { console.log('SKIP: jsdom is not installed (set NODE_PATH)'); process.exit(0); }
const ROOT = process.env.REVLIB_ROOT || path.join(__dirname, '../..');
const read = f => fs.readFileSync(path.join(ROOT, f), 'utf8');
const src = read('charm-nest-bridge.js');

const apiCalls = [];
const dom = new JSDOM('<body><div class="topbar"></div><main id="libBody"></main><div id="sw"></div></body>', { runScripts: 'outside-only', pretendToBeVisual: true, url: 'https://example.test/' });
const win = dom.window, document = win.document, frames = [], net = [], calls = [], opened = [], mode = [];
win.matchMedia = () => ({ matches: true });
win.Element.prototype.getClientRects = function () { return [{}]; };   // (jsdom has no layout: everything is visible)
win.fetch = (...a) => { net.push(a[0]); return Promise.reject(new Error('offline')); };
win.eval(read('charm-nest-motion.js'));
win.eval(read('charm-nest-readiness.js'));
let world = {};
const esc = x => String(x == null ? '' : x).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;');
const c = vm.createContext({ window: win, document, console, esc, cors: x => x, S: { mode: 'library', cloud: { ok: true } }, allSheets: () => [], Orders: { rows: () => [] }, Engrave: { items: () => new Map() }, O: { setLabel: n => 'Set ' + n }, CNListActivity: { compare: () => 0, compareBlocks: () => 0, state: () => ({ direction: 1 }) }, innerHeight: win.innerHeight, requestAnimationFrame: fn => (frames.push(fn), frames.length), setTimeout: win.setTimeout.bind(win), clearTimeout: win.clearTimeout.bind(win), setInterval() {}, api: async (name, payload) => { apiCalls.push(name); return { sheets: Object.values(world), sets: [] }; } });
vm.runInContext(src.slice(src.indexOf('const LaserReview ='), src.indexOf('const Sets =')), c);
Object.assign(win, { LibraryFlow: { approve: async o => { calls.push(o); return { ok: true, auto: [], needs: [], confirm: [], notes: [] }; } }, CNEmployee: { name: () => 'Maria' }, setMode: m => mode.push(m), ListMedia: { peek: () => null, listing: () => Promise.resolve(null) }, openOrderFrom: (el, rid) => { opened.push(['order-window', rid]); return true; } });
win.eval(read('charm-nest-library-review-hold.js'));   // (before the hold API exists: it attaches when the API is there, as the script order of the page may be either)
win.eval(read('charm-nest-library-issues.js'));
const L = win.LaserReview, RH = win.LibraryReviewHold, LI = win.LibraryIssues, body = document.querySelector('#libBody');
const flush = () => { for (let i = 0; i < 8 && frames.length; i++) frames.splice(0).forEach(f => f()); };
const tick = (ms = 15) => new Promise(r => setTimeout(r, ms));
const settle = async () => { await tick(); flush(); await tick(); flush(); };

// ── fixtures: a lone sheet and a set sheet held by orders in Review; the readiness carries the 'held' block REVHOLD folds in
const ORD1 = '4100000001', ORD2 = '4100000002', ORD3 = '4100000003', ORD4 = '4100000004';
const sheet = (id, extra = {}) => ({ id, sheetIndex: 1, poolIds: [id + '1'], placedCount: 1, verification: { ok: true }, preview: 'p', outputs: { ai: 'f' }, label: { files: [] }, backPool: [], engraving: { [id + '1']: { needed: false, state: 'none', approved: true } }, updatedAt: 1, ...extra });
const labelled = s => ({ ...s, label: { files: [{ path: 'qr', url: 'u', payload: 'o' }] }, processReady: true, updatedAt: (s.updatedAt || 0) + 1 });
const heldBlock = (rid, piece) => ({ ready: false, key: 'held', why: 'In Review' + (piece ? ': ' + piece : ''), blocks: [{ key: 'held', index: 1, label: 'Charm', poolId: rid + '_1_1', lineKey: rid + '_1', sheetId: null, sheetLabel: null, why: 'In Review' + (piece ? ': ' + piece : '') }], onSheets: [], pieceCount: 2, customer: 'Name One', listingId: '' });
const withHold = (s, list) => ({ ...s, orders: list.map(x => x[0]), label: { files: [{ path: 'qr', url: 'u', payload: 'o', orders: list.map(x => x[0]) }] }, orderReadiness: Object.fromEntries(list.map(([rid, piece]) => [rid, { ...heldBlock(rid, piece), onSheets: [s.id] }])), updatedAt: (s.updatedAt || 0) + 1 });
const rect = card => { card.getBoundingClientRect = () => ({ left: 10, top: 80, right: 310, bottom: 430, width: 300, height: 350 }); return card; };
const flat = id => { const a = document.createElement('article'); a.className = 'librarySheet'; a.dataset.laserCard = 'sheet'; a._laserSheets = [id]; a.innerHTML = `<div class="libCard" data-id="${id}"><span data-sheet-status="${id}"></span></div>`; return rect(a); };
const setCard = (setId, ids) => { const a = document.createElement('div'); a.className = 'setCard'; a.dataset.laserCard = 'set'; a._laserSet = { setId, seq: 1, sheetIds: ids }; a._laserSheets = ids; a.innerHTML = `<div class="sh"><span class="nm" data-set-title></span></div><div class="sheetsRow">${ids.map(id => `<article class="librarySheet"><div class="libCard" data-id="${id}"><span data-sheet-status="${id}"></span></div></article>`).join('')}</div>`; return rect(a); };
const boxOf = (root, key) => root.querySelector(`.approveBox[data-approve-for="${key}"]`);
const off = b => b.getAttribute('aria-disabled') === 'true';
const whyOf = box => box.querySelector('[data-approve-why]').textContent;
const articleOf = (card, id) => [...card.querySelectorAll('.librarySheet')].find(a => a.querySelector('.libCard').dataset.id === id);

const recs = {
  H1: withHold(sheet('H1'), [[ORD1, 'left earring']]),            // a lone sheet held by one order (a pair: its left piece is on this sheet)
  P1: sheet('P1'),                                               // a lone sheet, not held
  SA: sheet('SA'),                                               // a set of two: SA is fine,
  SB: withHold(sheet('SB'), [[ORD2, ''], [ORD3, 'piece 2 of 3']]), // SB is held by two orders
  K1: labelled(sheet('K1'))                                      // a sheet already approved (in Laser cutting) that a later Review also holds
};
Object.values(recs).forEach(L.record); world = { ...recs }; L.sections(body);
const cards = { H1: flat('H1'), P1: flat('P1'), K1: flat('K1'), set: setCard('setH', ['SA', 'SB']) };
for (const k of ['H1', 'P1', 'K1']) L.place(cards[k], L.canCut(recs[k]), body);
L.place(cards.set, L.group(cards.set._laserSet, ['SA', 'SB'].map(id => recs[id])).ready, body);

// the fake hold API (REVHOLD's CharmNestReviewHold): the state lives here; the page asks, it never copies
let held = {}; const subs = new Set(), reviewOpens = [];
const holdApi = () => ({ heldBy: id => (held[id] || []).map(x => ({ sheetId: id, name: 'Charm', reason: 'In Review', poolIds: [], ...x })), onChange: fn => { subs.add(fn); return () => subs.delete(fn); }, openInReview: rid => { reviewOpens.push(String(rid)); return true; } });
const fire = sheetIds => [...subs].forEach(f => f({ rids: [], sheetIds }));
const bangOf = (card, id, step) => card.querySelector(`.flowBox[data-flow-for="sheet:${id}"] [data-issues-open]${step ? `[data-issues-step="${step}"]` : ''}`) || card.querySelector(`.flowBox[data-flow-for="sheet:${id}"] [data-issues-open]`);

(async () => {
  try {
    // 1. nothing held, no hold API on the page (REVHOLD's script not loaded): the Library is as it always was, nothing drawn, nothing thrown
    L.changed(); flush();
    assert.equal(RH.isOn(), false); assert.equal(RH.rows('H1').length, 0); assert.equal(RH.line('H1'), ''); assert.equal(RH.why(['H1', 'SB']), null); assert.equal(RH.nestHtml('H1'), '');
    assert.equal(document.querySelectorAll('.rvhNote').length, 0, 'no note without the hold API');
    assert.doesNotMatch(whyOf(boxOf(cards.H1, 'sheet:H1')), /Held/, 'the Approve line says nothing of a hold it cannot know');

    // 2. the hold API arrives (a script that loaded later) and names the held sheets; the Library says so at once, on the hold's own change
    held = { H1: [{ rid: ORD1, piece: 'left earring' }], SB: [{ rid: ORD2, piece: '' }, { rid: ORD3, piece: 'piece 2 of 3' }], K1: [{ rid: ORD4, piece: '' }] };
    win.CharmNestReviewHold = holdApi();
    L.changed(); flush(); await settle();   // (the first card pass finds the API and subscribes)
    assert.equal(subs.size, 1, 'one subscription to the hold, made when the API is found');
    // 2a. a lone sheet: ITS OWN Approve button is grey and its reason line names the order and the piece; one order = one tap to Review
    const h1 = boxOf(cards.H1, 'sheet:H1');
    assert.equal(off(h1.querySelector('[data-approve-btn]')), true, 'a held sheet cannot be approved');
    assert.equal(whyOf(h1), `Held by order ${ORD1} (left earring) in Review`);
    assert.match(h1.querySelector('[data-approve-btn]').getAttribute('aria-label'), new RegExp(`not yet: Held by order ${ORD1} \\(left earring\\) in Review`));
    const link = h1.querySelector('[data-approve-review]'); assert(link, 'the reason is the one tap to the Review tab when one order holds the sheet'); assert.equal(link.dataset.approveReview, ORD1);
    assert.equal(h1.querySelector('[data-approve-reason]'), null, 'it does not open the list instead');
    h1.querySelector('[data-approve-btn]').click(); await tick(); assert.equal(calls.length, 0, 'a grey button presses nothing');
    link.click(); assert.deepEqual(reviewOpens, [ORD1], 'the Review tab opens on that order'); reviewOpens.length = 0;
    // 2b. the sheet's own Approve line says it, so no second line under the rail (it would say the same thing twice)
    assert.equal(cards.H1.querySelectorAll('.rvhNote').length, 0, 'a sheet whose own Approve line says it has no second note');
    // 2c. an unheld lone sheet is untouched
    assert.doesNotMatch(whyOf(boxOf(cards.P1, 'sheet:P1')), /Held/); assert.equal(cards.P1.querySelectorAll('.rvhNote').length, 0);
    // 2d. a set: its ONE button says it once, at set level, naming BOTH orders; the held sheet carries the note under its own rail, the other sheet nothing
    const sb = boxOf(cards.set, 'set:setH');
    assert.equal(off(sb.querySelector('[data-approve-btn]')), true);
    assert.equal(whyOf(sb), `Held by orders ${ORD2} and ${ORD3} (piece 2 of 3) in Review`, 'both orders, the piece only where the order has more than one');
    assert.equal(sb.querySelector('[data-approve-review]'), null, 'several orders: the line opens the list of what holds the sheet (every other reason does)');
    assert(sb.querySelector('[data-approve-reason]'), 'and it is a link to that sheet\'s list');
    const noteB = articleOf(cards.set, 'SB').querySelector('.rvhNote'); assert(noteB, 'the held sheet of a set says it under its rail');
    assert.equal(noteB.textContent, `Held by orders ${ORD2} and ${ORD3} (piece 2 of 3) in Review`);
    assert.deepEqual([...noteB.querySelectorAll('[data-rvh-open]')].map(b => b.dataset.rvhOpen), [ORD2, ORD3], 'each order number is a one-tap link');
    assert.equal(noteB.previousElementSibling.className.includes('flowBox'), true, 'directly under the small rail');
    assert.equal(articleOf(cards.set, 'SA').querySelectorAll('.rvhNote').length, 0, 'the sheet that is fine says nothing');
    noteB.querySelector(`[data-rvh-open="${ORD3}"]`).click(); assert.deepEqual(reviewOpens, [ORD3]); reviewOpens.length = 0;
    // 2e. a sheet already in Laser cutting (no Approve button) is held too: the note says so under its rail, the whole line is the tap
    const noteK = cards.K1.querySelector('.rvhNote'); assert(noteK, 'a sheet with no Approve button of its own carries the note'); assert.equal(noteK.textContent, `Held by order ${ORD4} in Review`);
    assert.equal(noteK.querySelectorAll('[data-rvh-open]').length, 1); noteK.querySelector('button').click(); assert.deepEqual(reviewOpens, [ORD4]); reviewOpens.length = 0;

    // 3. the '!' list names the exact order: a row of the list that exists, chip "In Review" and the piece, not "On hold"; a press opens the Review tab
    const bang = bangOf(cards.H1, 'H1'); assert(bang, 'the held sheet\'s rail carries a \'!\'');
    bang.click(); await tick();
    const panel = document.querySelector('#libIssuesPanel'); assert(panel, 'the list opens');
    const rows = [...panel.querySelectorAll('.lisRow')]; assert.equal(rows.length, 1, 'one row: the order that holds the sheet');
    assert.equal(rows[0].querySelector('.lisWho b').textContent, ORD1);
    assert.equal(rows[0].querySelector('.lisChip').textContent, 'In Review: left earring');
    assert.equal(rows[0].dataset.go, 'review'); assert.doesNotMatch(panel.textContent, /On hold/);
    assert.equal(JSON.stringify(LI.listed().map(x => [x.key, x.orderId])), JSON.stringify([['held', ORD1]]));
    rows[0].click(); await tick();
    assert.deepEqual(reviewOpens, [ORD1], 'a press opens that order in the Review tab'); reviewOpens.length = 0;
    assert.equal(LI.isOpen(), false, 'the list steps aside; no pop-up on a pop-up');
    assert.equal(opened.length, 0, 'the order window did not open on top of it'); await tick(200);

    // 3b. the safety net: an order the hold names but the readiness did not list is listed, so the list always names the exact order
    held.P1 = [{ rid: ORD2, piece: 'right earring' }]; L.changed(); flush();
    const p1bang = bangOf(cards.P1, 'P1');
    if (p1bang) { p1bang.click(); await tick(); const pr = [...document.querySelectorAll('#libIssuesPanel .lisRow')]; assert(pr.some(r => r.querySelector('.lisWho b').textContent === ORD2 && /In Review: right earring/.test(r.textContent)), 'the list names the order'); LI.close(); await tick(200); }
    delete held.P1; L.changed(); flush();

    // 4. the other places: the sheet window's line and the Nest sheet card's note say the same words, with the order number as the tap
    const host = document.getElementById('sw'); host.className = 'swSheetMsg rvhLine'; host.hidden = true;
    RH.paint(host, 'H1'); assert.equal(host.hidden, false); assert.equal(host.textContent, `Held by order ${ORD1} (left earring) in Review`);
    host.querySelector('button').click(); assert.deepEqual(reviewOpens, [ORD1]); reviewOpens.length = 0;
    RH.paint(host, 'P1'); assert.equal(host.hidden, true, 'a sheet that is not held has no line'); assert.equal(host.innerHTML, '');
    const nest = document.createElement('div'); nest.innerHTML = RH.nestHtml('SB'); assert.equal(nest.textContent, `Held by orders ${ORD2} and ${ORD3} (piece 2 of 3) in Review`); assert.equal(RH.nestHtml('SA'), '');
    // the call sites exist in the page code (the sheet window and the Nest card draw them from these files)
    assert(read('charm-nest-sheetwin.js').includes('LibraryReviewHold.paint(E.hold'), 'the sheet window draws its line from this module');
    assert(read('charm-nest-1.html').includes('LibraryReviewHold.nestHtml(sh.sheetId)'), 'the Nest sheet card draws its note from this module');
    assert(/data-r="hold"/.test(read('charm-nest-sheetwin.js')), 'and has the place for it');
    // 4b. the words for more orders
    assert.equal(RH.lineOf([{ rid: '1', piece: '' }, { rid: '2', piece: 'left earring' }, { rid: '3', piece: '' }]), 'Held by orders 1, 2 (left earring) and 3 in Review');
    assert.equal(RH.lineOf([{ rid: '1', piece: '' }, { rid: '2', piece: '' }, { rid: '3', piece: '' }, { rid: '4', piece: '' }, { rid: '5', piece: '' }]), 'Held by orders 1, 2 and 3 more in Review');

    // 5. it clears at once: the hold's change callback (no poll, no read) and the notes, lines and the list follow
    const before = net.length;
    held = { K1: [] }; delete recs.H1.orderReadiness; recs.H1 = { ...recs.H1, orders: [], orderReadiness: {}, updatedAt: 9 }; L.record(recs.H1);
    recs.SB = { ...labelled(sheet('SB')), updatedAt: 9 }; L.record(recs.SB); recs.K1 = { ...recs.K1, updatedAt: 9 }; L.record(recs.K1);
    fire(['H1', 'SB', 'K1']); await settle();
    assert.doesNotMatch(whyOf(boxOf(cards.H1, 'sheet:H1')), /Held/, 'the Approve line is rid of it'); assert.equal(off(boxOf(cards.H1, 'sheet:H1').querySelector('[data-approve-btn]')), false, 'and the button is green again');
    assert.equal(document.querySelectorAll('.rvhNote').length, 0, 'every note is gone');
    assert.doesNotMatch(whyOf(boxOf(cards.set, 'set:setH')), /Held/);
    assert.equal(host.hidden === false ? RH.line('H1') : '', '', 'nothing is held');
    RH.changed({ sheetIds: ['H1'] }); await settle(); assert.equal(host.hidden, true, 'the sheet window\'s line follows the hold too');
    assert.equal(net.length, before, 'this module made no network call of its own (the Library\'s own live read is the page\'s, not counted)'); assert.equal(net.length, 0);

    // 6. the Review mark's own event (another computer's change arrives through the live feed) redraws too, even if the hold's callback did not
    held = { H1: [{ rid: ORD1, piece: '' }] }; recs.H1 = withHold(sheet('H1'), [[ORD1, '']]); recs.H1.updatedAt = 20; L.record(recs.H1);
    document.dispatchEvent(new win.CustomEvent('cn-review-mark', { detail: { rid: ORD1, kind: 'synced' } })); await settle();
    assert.equal(whyOf(boxOf(cards.H1, 'sheet:H1')), `Held by order ${ORD1} in Review`, 'a one-piece order is named without a piece');

    console.log('Review hold UI OK: a held sheet names its order and piece on the Approve line (one tap to Review), in the \'!\' list (chip In Review, a press opens Review), under the rail of a set sheet or a sheet in Laser cutting, in the sheet window and on the Nest card; clears at once; nothing without the hold API; no network');
  } finally { win.close(); }
})().catch(e => { console.error(e); process.exitCode = 1; });
