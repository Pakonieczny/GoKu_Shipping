// PS1: the work behind Use this one in the Sheet menu of the Options window of the Rose Gold, 14K Gold and 10K Gold sheet cards (charm-nest-partial-ui.js, with the repository the
// window draws: charm-nest-options-modal.js, OptionsHistory cards), in jsdom, with the page's own Gate (charm-nest-bridge.js renderRelease) and a fake data layer (PartialSheets, PS3)
// and engine (PartialNest, PS2). Round 2: no chip strip on the sheet card, no Partial sheets card; the rule is a two-option switch; the repository is ONE searchAll.
// Needs jsdom (not installed everywhere): the test says so and passes nothing when it is missing.
//   node tests/charm-nest/partial-panel.cjs
const assert = require('node:assert/strict'), fs = require('node:fs');
let JSDOM; try { ({ JSDOM } = require('jsdom')); } catch (_) { console.log('partial-panel: SKIPPED (jsdom is not installed)'); process.exit(0); }
const R = require('../../charm-nest-rose'), O = require('../../charm-nest-orders');
const outline = { subpaths: [[['m', [0, 0]], ['l', [10, 0]], ['l', [10, 10]], ['l', [0, 10]], ['h']]] };
const wait = ms => new Promise(r => setTimeout(r, ms));
const until = async (f, what) => { for (let n = 0; n < 200; n++) { if (f()) return; await wait(5); } assert.fail('timed out waiting for ' + what); };

// ── static: the Library lost its Leftover view; the new file is built and versioned; the bridge hands the panel its node ──
const html = fs.readFileSync('charm-nest-1.html', 'utf8'), build = fs.readFileSync('scripts/build-public.cjs', 'utf8'), bridge = fs.readFileSync('charm-nest-bridge.js', 'utf8');
assert(!/charm-nest-remnants\.js/.test(html + build) && !fs.existsSync('charm-nest-remnants.js'), 'the Library Leftover sheets view (charm-nest-remnants.js) is gone');
assert(!/Leftover sheets/.test(html + fs.readFileSync('charm-nest-library-cutline.js', 'utf8')), 'no Leftover sheets choice in the Library');
assert(/charm-nest-partial-ui\.js\?v=[^"']+/.test(html), 'the new script is on the page with a ?v= token'); assert(build.includes('"charm-nest-partial-ui.js"'), 'and in the public build');
assert(/PartialSheetsUI\.paint\(sh,node\)/.test(bridge), 'renderRelease tells the work about the sheet');

const card = (metal, id, o = {}) => ({ id, stockId: 'stock-' + id, revision: 1, metal, status: 'available', outline: [[[0, 0], [60, 0], [60, 50], [30, 50], [30, 20], [0, 20]]], sheetWMm: 100, sheetHMm: 50, wMm: 60, hMm: 50, areaMm2: 2400, bboxMm: { x: 0, y: 0, w: 60, h: 50 },
  sourceSheet: 'Sheet 2', sourceSet: 'Set 4', cutAt: Date.now() - 5 * 864e5, cutBy: 'Ana', lastUsedAt: Date.now() - 5 * 864e5, lastUsedBy: '', lastUsedSheet: '', estimate: { pieces: 7, low: 5, high: 9 }, ...o });

async function run(metal, word) {
  const doc = () => dom.window.document;
  const dom = new JSDOM('<section id="sheet"><div class="shHead"></div><div class="shGate" data-r="gate"></div><div class="shPreviewWrap"></div></section>', { url: 'https://example.test', runScripts: 'outside-only' }), w = dom.window;
  w.IntersectionObserver = class { observe() { } unobserve() { } };
  w.CharmNestRose = R; w.CharmNestOrders = O; w.confirm = () => true; w.Motion = { reduced: () => true };
  const toasts = [], WPT = 100 / 25.4 * 72, HPT = 50 / 25.4 * 72;
  const charms = [0, 1, 2].map(i => ({ id: 'c' + i, outline, centerPt: [5, 5], members: [] }));
  const sh = { metal, page: 1, el: w.document.getElementById('sheet'), sheetId: metal + '-test', runId: 'run-test', charms, placements: charms.map((c, i) => ({ id: c.id, cxPt: 10 + i * 20, cyPt: 10, angle: 0, scale: 1 })), persistedDone: true, verification: { ok: true }, status: 'complete', dirty: false, draft: true, outputs: {} };
  w.CN = { S: { cloud: { ok: true }, settings: { stock: {} }, mode: 'nest', sheets: { [metal]: { cardEl: sh.el, pages: [sh] } } }, esc: s => String(s).replace(/</g, '&lt;'), stockFor: () => ({ wPt: WPT, hPt: HPT, wIn: WPT / 72, hIn: HPT / 72 }), uid: () => 'test', allSheets: () => [sh], pagesOf: () => [sh], drawPreview() { }, labelOf: () => word,
    renderCard: p => { w.Gate.renderCard(p); }, toast(m, k) { toasts.push([String(m), k]); }, sheetDirty: p => { p.dirty = true; }, flushManualIntake() { }, api: async () => ({}) };
  w.sh = sh;
  w.eval(`var C=window.CN,S=C.S,CN=C,B=window.B={run:{runId:'run-test',releasePolicy:2,status:'running',solidIncluded:{}}};
 var O=window.CharmNestOrders,allSheets=()=>[sh],pagesOf=()=>[sh],stockFor=C.stockFor,esc=C.esc,labelOf=()=> '${word}';
 var Sets={ofRun:()=>[]},RunCtl={},Session=window.Session={schedule(){}},toast=()=>{},refreshAllCards=()=>{},api=C.api;
 var sheetDirty=()=>{},saveSettings=()=>{},startNest=()=>{};`);
  const src = fs.readFileSync('charm-nest-bridge.js', 'utf8'); w.eval(src.slice(src.indexOf('const Gate ='), src.indexOf('/* ═══ 21', src.indexOf('const Gate ='))));

  // ── the data layer (PS3) and the engine (PS2), faked: every call is counted ──
  const mine = [card(metal, 'p-old', { cutAt: Date.now() - 9 * 864e5, lastUsedAt: Date.now() - 9 * 864e5 }), card(metal, 'p-new', { wMm: 40, hMm: 30, areaMm2: 1000, estimate: { pieces: 3, low: 3, high: 3 }, lastUsedAt: Date.now() - 36e5, lastUsedBy: 'Bo', lastUsedSheet: 'Sheet 3' }), card(metal, 'p-used', { status: 'used' })];
  const others = ['rose', 'gold10k', 'gold14k'].filter(m => m !== metal).map(m => card(m, 'x-' + m, { lastUsedAt: Date.now() }));   // (another metal's partial sheet that a cache might hold: it must never show)
  const calls = [], handlers = [], pol = { mode: 'auto', wMm: 100, hMm: 50 }; let release, listGate = null;
  w.PartialSheets = {
    cached: () => null, on: f => handlers.push(f), policy: () => ({ ...pol }), changed() { calls.push(['changed']); },
    async loadPolicy() { return {}; }, async list(m, o) { calls.push(['list', m, !!(o && o.force)]); return { items: [], rev: 1 }; },
    async searchAll(o) { calls.push(['searchAll', !!(o && o.force)]); if (listGate) await new Promise(r => { release = r; }); return { items: [...others, ...mine], rev: 1, more: false }; },
    async setPolicy(m, v) { calls.push(['setPolicy', m, v.mode, v.wMm, v.hMm]); Object.assign(pol, v); return { ...pol }; }, async plan() { return {}; } };
  const answers = {
    one: { ok: true, pieces: 3, fitsAll: false, links: [{ partialId: 'p-new', label: 'Sheet 2', wMm: 40, hMm: 30, placed: 2, densityPct: 61 }], continues: { n: 1, next: 'partial', nextPartialId: 'p-old', words: 'The other 1 continue on the next partial sheet.' } },
    two: { ok: true, pieces: 3, fitsAll: true, links: [{ partialId: 'p-new', wMm: 40, hMm: 30, placed: 2 }, { partialId: 'p-old', wMm: 60, hMm: 50, placed: 1 }], continues: { n: 0, next: 'none' } },
    all: { ok: true, pieces: 3, fitsAll: true, links: [{ partialId: 'p-old', wMm: 60, hMm: 50, placed: 3 }], continues: { n: 0, next: 'none' } } };
  let lock = '', previewGate = false, previewRelease, chain = [];
  w.PartialNest = {
    canSeat: () => lock ? { ok: false, why: lock } : { ok: true }, chain: () => chain, on() { },
    async preview(s, ids, o) { calls.push(['preview', ids.join('+')]); if (o && o.onStep) o.onStep({ text: 'Trying the pieces on the partial sheet…' }); if (previewGate) await new Promise(r => { previewRelease = r; }); return ids.length === 2 ? answers.two : ids[0] === 'p-new' ? answers.one : answers.all; },
    async seat(s, ids, o) { calls.push(['seat', ids.join('+'), !!o.confirmed]); o.onStep({ key: 'claim' }); o.onStep({ key: 'nest' }); return { ok: true, moved: 3, continues: 0 }; } };
  let timers = 0; const setInt = w.setInterval; w.setInterval = (...a) => { timers++; return setInt.apply(w, a); };
  w.eval(fs.readFileSync('charm-nest-partial-ui.js', 'utf8')); w.eval(fs.readFileSync('charm-nest-options-history.js', 'utf8')); w.eval(fs.readFileSync('charm-nest-options-modal.js', 'utf8')); w.Gate.renderCard(sh);
  const studioOpen = () => w.OptionsStudio.isOpen(metal), gate = sh.el.querySelector('.shGate'), opener = gate.querySelector('.sheetOptionsBtn');

  // 1. nothing is drawn on the sheet card for partial sheets and nothing is read until the window is opened; the card never shows a chip strip, even when sheets sit on partial sheets
  const menu0 = gate._optBox; assert(!menu0.querySelector('.psView, [data-card="partial"]') && !sh.el.querySelector('.psChain, .psChip'), word + ': no Partial sheets card, no chip strip');
  assert.equal(calls.length, 0, 'drawing Options reads nothing'); assert(!studioOpen());

  // 2. open: ONE list (the repository of every metal) with a labelled spinner while it reads, then only this metal's available cards by default
  listGate = true; opener.click();
  assert(studioOpen(), 'one large window holds the menu (no second pop-up, no sub view)'); const view = menu0.querySelector('.osSource'); assert(view && doc().querySelector('dialog.osDlg').contains(view), 'the Sheet source is in the window');
  assert.deepEqual(calls.filter(c => c[0] === 'searchAll'), [['searchAll', false]], 'one list call'); assert.equal(calls.filter(c => c[0] === 'list').length, 0, 'and no partial list of its own');
  assert.match(view.querySelector('[data-os="results"] .osBusy').textContent, /Reading the partial sheets/);
  release(); await until(() => view.querySelectorAll('.ohc').length);
  const cards = [...view.querySelectorAll('.ohc')];
  assert.deepEqual(cards.map(c => c.dataset.id), ['p-new', 'p-old'], 'only this metal, only available ones, newest used first: ' + cards.map(c => c.dataset.id));
  const first = cards[0];
  assert.match(first.textContent, /40 × 30 mm/); assert.match(first.textContent, /About 3 pieces/); assert.match(cards[1].textContent, /About 7 pieces/);
  assert.match(first.textContent, /Last used/); assert.match(first.textContent, /Bo/);
  const svg = first.querySelector('svg'); assert(svg && svg.innerHTML.includes('#008974'), 'the thumbnail draws the green cut line');
  assert(!/<input[^>]*type="text"|name your|your name/i.test(view.innerHTML), 'no name is ever asked for');
  const pickBtn = id => view.querySelector(`.ohc[data-id="${id}"] [data-ps="pick"]`);

  // 3. a pick shows the answer in words first: the labelled wait, then how many fit, who takes the rest, Use this one / Cancel (Cancel changes nothing)
  previewGate = true; pickBtn('p-new').click();
  await until(() => view.querySelector('.psAsk .psBusy'));
  assert.match(view.querySelector('.psAsk').textContent, /Trying the pieces on the partial sheet/); assert(!calls.some(c => c[0] === 'seat'));
  previewRelease(); await until(() => view.querySelector('[data-ps="use"]')); previewGate = false;
  const ask = view.querySelector('.psAsk').textContent;
  assert.match(ask, /2 of the 3 pieces on Sheet 1 fit on this partial sheet \(1 do not\)/); assert.match(ask, /continue on the next partial sheet/); assert.match(ask, /Next in line: Sheet 2 · Set 4/);
  assert(view.querySelector('[data-ps="add"]'), 'more partial sheets can take the rest, with no limit');
  view.querySelector('[data-ps="cancel"]').click(); assert(!view.querySelector('.psAsk') && !calls.some(c => c[0] === 'seat' || c[0] === 'policy') && studioOpen(), 'Cancel changes nothing and leaves Options as it was');

  // 4. the chain: Add another partial sheet, the second pick asks about both together, Use these 2 hands over both, in order
  pickBtn('p-new').click(); await until(() => view.querySelector('[data-ps="add"]'));
  view.querySelector('[data-ps="add"]').click(); assert(view.querySelector('[data-ps="back-ask"]'), 'while adding, the person may go back');
  pickBtn('p-old').click(); await until(() => /Use these 2/.test(view.textContent));
  assert.match(view.querySelector('.psAsk').textContent, /All 3 pieces on Sheet 1 fit on these 2 partial sheets, filled in this order/); assert.equal(view.querySelectorAll('.psChainList li').length, 2);
  assert.deepEqual(calls.filter(c => c[0] === 'preview').slice(-1)[0], ['preview', 'p-new+p-old']);
  assert.deepEqual([...view.querySelectorAll('[data-ps="pick"][aria-pressed="true"]')].map(b => b.textContent), ['Chosen · 1', 'Chosen · 2'], 'the chain is numbered 1, 2 on the cards');

  // 5. Esc: the first cancels the answer, the second closes Options as it always does
  const esc = el => el.dispatchEvent(new w.KeyboardEvent('keydown', { key: 'Escape', bubbles: true, cancelable: true }));
  esc(view.querySelector('.ohc')); assert(!view.querySelector('.psAsk') && studioOpen(), 'first Esc cancels the answer only');
  esc(view.querySelector('.ohc')); await until(() => !studioOpen()); assert(!w.Gate.state().optionsOpen[metal], 'second Esc closes the Options window');
  assert(menu0.hidden && gate.contains(menu0) && !menu0.querySelector('.osSource'), 'and the card goes home with the controls');

  // 6. reopen (one more list call per open, never a timer), pick, Use this one: handed to the engine as confirmed, the window closes, the person is told
  listGate = false; opener.click(); const view2 = () => menu0.querySelector('.osSource'); await until(() => view2() && view2().querySelectorAll('.ohc').length === 2 && calls.filter(c => c[0] === 'searchAll').length === 2);
  view2().querySelector('.ohc[data-id="p-old"] [data-ps="pick"]').click(); await until(() => view2().querySelector('[data-ps="use"]'));
  view2().querySelector('[data-ps="use"]').click(); await until(() => calls.some(c => c[0] === 'seat'));
  assert.deepEqual(calls.find(c => c[0] === 'seat'), ['seat', 'p-old', true]); await until(() => !studioOpen());
  assert(menu0.hidden, 'the window closed on the work'); await until(() => calls.some(c => c[0] === 'changed')); // the data layer is told the partial sheet was used
  await until(() => toasts.some(t => /3 pieces seated/.test(t[0]))); assert.equal(toasts.find(t => /seated/.test(t[0]))[1], 'ok');

  // 7. a sheet the engine says cannot move (already cut) says why, in words, and asks the engine nothing
  opener.click(); await until(() => view2() && view2().querySelectorAll('.ohc').length === 2);
  lock = 'Sheet 1 is already cut, so it stays on the sheet it was cut from.'; w.Gate.renderCard(sh); const before = calls.filter(c => c[0] === 'preview').length;
  view2().querySelector('[data-ps="pick"]').click(); assert.equal(calls.filter(c => c[0] === 'preview').length, before); assert.equal(toasts.slice(-1)[0][1], 'bad'); assert.match(view2().querySelector('.osLock').textContent, /already cut/); assert(view2().querySelector('[data-ps="pick"]').getAttribute('aria-disabled') === 'true'); lock = '';
  w.Gate.renderCard(sh); esc(view2().querySelector('.ohc')); await until(() => !studioOpen()); opener.click(); await until(() => view2() && view2().querySelectorAll('.ohc').length === 2);

  // 8. the rule: automatic reuse, or a brand new sheet at a size the person sets (5 to 500 mm, the Sheet dimensions conventions)
  const radios = [...view2().querySelectorAll('.psRadio input[type=radio]')]; assert.deepEqual(radios.map(r => r.value), ['auto', 'new']); assert(radios[0].checked);
  assert.match(view2().querySelector('.psPolicy').textContent, /Reuse partial sheets automatically/); assert.match(view2().querySelector('.psPolicy').textContent, /Offer a brand new sheet at 100 × 50 mm/);
  assert(view2().querySelector('[data-ps="size"]').hidden, 'no size field while partial sheets are reused');
  radios[1].checked = true; radios[1].dispatchEvent(new w.Event('change', { bubbles: true })); await until(() => calls.some(c => c[0] === 'setPolicy'));
  assert.deepEqual(calls.find(c => c[0] === 'setPolicy'), ['setPolicy', metal, 'new', 100, 50]); assert(!view2().querySelector('[data-ps="size"]').hidden, 'the size fields appear');
  const wIn = view2().querySelector('[data-ps="w"]'), hIn = view2().querySelector('[data-ps="h"]'); assert.deepEqual([wIn.min, wIn.max, hIn.min, hIn.max], ['5', '500', '5', '500']); await until(() => /5–500 mm per side|Saved/.test(view2().querySelector('[data-ps="polhelp"]').textContent));
  wIn.value = '2'; wIn.dispatchEvent(new w.Event('input', { bubbles: true })); const n0 = calls.length; view2().querySelector('[data-ps="polsave"]').click();
  assert.equal(calls.length, n0, 'a size outside 5 to 500 is refused before anything is saved'); assert.equal(toasts.slice(-1)[0][0], 'Use a width and height between 5 and 500 mm');
  wIn.value = '120.5'; wIn.dispatchEvent(new w.Event('input', { bubbles: true })); hIn.value = '60'; hIn.dispatchEvent(new w.Event('input', { bubbles: true })); await until(() => !view2().querySelector('[data-ps="polsave"]').disabled); view2().querySelector('[data-ps="polsave"]').click(); await until(() => calls.filter(c => c[0] === 'setPolicy').length === 2);
  assert.deepEqual(calls.filter(c => c[0] === 'setPolicy')[1], ['setPolicy', metal, 'new', 120.5, 60]);
  await until(() => view2().querySelector('[data-ps="polsave"]') && !view2().querySelector('[data-ps="polsave"]').disabled && +wIn.value === 120.5); assert.equal(+hIn.value, 60, 'the size is kept in the card');

  // 9. the engine says sheets sit on partial sheets: nothing is drawn for it on the card any more (Paul: the chip strips are redundant)
  chain = [1, 2, 3].map(i => ({ sheetId: 's' + i, label: 'Sheet ' + i, partialId: 'p' + i, placed: i, state: 'open', page: i, wMm: 60, hMm: 50 })); w.Gate.renderCard(sh); assert(!sh.el.querySelector('.psChain, .psChip') && !/Sheets on partial sheets/.test(sh.el.textContent), 'no chain strip on the card, however many sheets sit on partial sheets'); chain = [];

  // 10. nothing ticked on a timer, no other metal's list was ever read
  assert.equal(timers, 0, 'no setInterval: the panel never polls'); assert(calls.filter(c => c[0] === 'list').length === 0, 'the window read no partial list of any metal: one repository list per open');
  dom.window.close();
}
(async () => {
  await run('rose', 'Rose Gold');
  await run('gold14k', '14K Gold');
  await run('gold10k', '10K Gold');
  console.log('partial-panel: ok (Use this one in the Sheet menu of the Options window for RG, 14K and 10K; one repository list per open; this metal and available first; answer in words with Use this one / Cancel; chain of partial sheets; Esc; the rule as a switch with its size; no chip strip; no polling; Library Leftover view gone)');
})().catch(e => { console.error(e); process.exit(1); });
