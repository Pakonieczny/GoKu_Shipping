// PS1: the Partial Sheet button and panel in the Options menu of the Rose Gold, 14K Gold and 10K Gold sheet cards (charm-nest-partial-ui.js),
// in jsdom, with the page's own Gate (charm-nest-bridge.js renderRelease) and a fake data layer (PartialSheets, PS3) and engine (PartialNest, PS2).
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
assert(/PartialSheetsUI\.paint\(sh,node\)/.test(bridge), 'renderRelease draws the panel');

const card = (metal, id, o = {}) => ({ id, metal, status: 'available', outline: [[[0, 0], [60, 0], [60, 50], [30, 50], [30, 20], [0, 20]]], sheetWMm: 100, sheetHMm: 50, wMm: 60, hMm: 50, areaMm2: 2400, bboxMm: { x: 0, y: 0, w: 60, h: 50 },
  sourceSheet: 'Sheet 2', sourceSet: 'Set 4', cutAt: Date.now() - 5 * 864e5, cutBy: 'Ana', lastUsedAt: Date.now() - 5 * 864e5, lastUsedBy: '', lastUsedSheet: '', estimate: { pieces: 7, low: 5, high: 9 }, ...o });

async function run(metal, word) {
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
    async list(m, o) { calls.push(['list', m, !!(o && o.force)]); if (listGate) await new Promise(r => { release = r; }); return { items: [...others, ...mine], rev: 1 }; },
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
  w.eval(fs.readFileSync('charm-nest-partial-ui.js', 'utf8')); w.Gate.renderCard(sh);

  // 1. the button is in this metal's Options; nothing is read until it is pressed
  const menu = sh.el.querySelector('.solidOptions'), sec = menu.querySelector('[data-solid="partial"]');
  assert(sec && sec.querySelector('[data-ps="open"]').textContent === 'Partial Sheet', word + ': a Partial Sheet button in Options');
  assert.match(sec.querySelector('[data-ps="sum"]').textContent, /reused automatically/);
  assert.equal(calls.length, 0, 'drawing Options reads nothing');
  const details = sh.el.querySelector('.sheetOptions'), view = menu.querySelector('.psView'); details.open = true;
  assert(view.hidden, 'the panel is closed until asked for');

  // 2. open: ONE list call for this metal, a labelled spinner while it reads, then only this metal's available cards, newest used first
  listGate = true; sec.querySelector('[data-ps="open"]').click();
  assert(!view.hidden && menu.classList.contains('psOn'), 'opens inside the same Options popover (no second pop-up)');
  assert.deepEqual(calls.filter(c => c[0] === 'list'), [['list', metal, false]], 'one list call, for this metal');
  assert.match(view.querySelector('[data-ps="list"] .psBusy').textContent, new RegExp('Reading the ' + word + ' partial sheets'));
  release(); await until(() => view.querySelectorAll('.psCard').length);
  const cards = [...view.querySelectorAll('.psCard')];
  assert.deepEqual(cards.map(c => c.dataset.id), ['p-new', 'p-old'], 'only this metal, only available ones, newest used first: ' + cards.map(c => c.dataset.id));
  assert(cards.every(c => c.dataset.m === metal));
  const first = cards[0];
  assert.match(first.querySelector('.psSize').textContent, /40 × 30 mm/); assert.match(first.querySelector('.psSize').textContent, /1,?000 mm²/);
  assert.match(first.querySelector('.psFit').textContent, /About 3 pieces/); assert.match(cards[1].querySelector('.psFit').textContent, /About 7 pieces\s*roughly 5 to 9/);
  assert.match(first.querySelector('.psUse').textContent, /Last used Today, /); assert.match(first.querySelector('.psUse').textContent, /by Bo · on Sheet 3/);
  assert.match(cards[1].querySelector('.psUse').textContent, /Last used [A-Z][a-z]{2} \d/); assert.match(cards[1].querySelector('.psUse').textContent, /left over when Sheet 2, Set 4 was cut by Ana/);
  const svg = first.querySelector('svg.psSvg'); assert.equal(svg.getAttribute('viewBox'), '0 0 100 50', 'true scale in the one 100 x 50 mm frame');
  assert(svg.innerHTML.includes('stroke-dasharray') && svg.innerHTML.includes('#008974'), 'the cut edge is the green dashed line'); assert(svg.getAttribute('aria-label').includes('40 by 30') || /40 × 30 mm/.test(svg.getAttribute('aria-label')));
  assert(!/<input[^>]*type="text"|name your|your name/i.test(view.innerHTML), 'no name is ever asked for');

  // 3. a pick shows the answer in words first: the labelled wait, then how many fit, who takes the rest, Use this one / Cancel (Cancel changes nothing)
  previewGate = true; first.click();
  await until(() => view.querySelector('.psAsk .psBusy'));
  assert.match(view.querySelector('.psAsk').textContent, /Trying the pieces on the partial sheet/); assert(!calls.some(c => c[0] === 'seat'));
  previewRelease(); await until(() => view.querySelector('[data-ps="use"]')); previewGate = false;
  const ask = view.querySelector('.psAsk').textContent;
  assert.match(ask, /2 of the 3 pieces on Sheet 1 fit on this partial sheet \(1 do not\)/); assert.match(ask, /continue on the next partial sheet/); assert.match(ask, /Next in line: Sheet 2 · Set 4/);
  assert(view.querySelector('[data-ps="add"]'), 'more partial sheets can take the rest, with no limit');
  view.querySelector('[data-ps="cancel"]').click(); assert(!view.querySelector('.psAsk') && !calls.some(c => c[0] === 'seat' || c[0] === 'policy') && details.open, 'Cancel changes nothing and leaves Options as it was');

  // 4. the chain: Add another partial sheet, the second pick asks about both together, Use these 2 hands over both, in order
  view.querySelector('.psCard[data-id="p-new"]').click(); await until(() => view.querySelector('[data-ps="add"]'));
  view.querySelector('[data-ps="add"]').click(); assert(view.querySelector('[data-ps="back-ask"]'), 'while adding, the person may go back');
  view.querySelector('.psCard[data-id="p-old"]').click(); await until(() => /Use these 2/.test(view.textContent));
  assert.match(view.querySelector('.psAsk').textContent, /All 3 pieces on Sheet 1 fit on these 2 partial sheets, filled in this order/); assert.equal(view.querySelectorAll('.psChainList li').length, 2);
  assert.deepEqual(calls.filter(c => c[0] === 'preview').slice(-1)[0], ['preview', 'p-new+p-old']);
  assert.deepEqual([...view.querySelectorAll('.psBadge')].map(b => b.textContent), ['1', '2'], 'the chain is numbered 1, 2 on the cards');

  // 5. Esc: the first cancels the answer, the second closes Options as it always does
  const esc = el => el.dispatchEvent(new w.KeyboardEvent('keydown', { key: 'Escape', bubbles: true, cancelable: true }));
  esc(view.querySelector('.psCard')); assert(!view.querySelector('.psAsk') && details.open, 'first Esc cancels the answer only');
  esc(view.querySelector('.psCard')); assert(!details.open && !w.Gate.state().optionsOpen[metal], 'second Esc closes the Options panel');
  await wait(10); assert(view.hidden, 'and the panel view with it');

  // 6. reopen (one more list call per open, never a timer), pick, Use this one: handed to the engine as confirmed, the panel closes, the person is told
  listGate = false; details.open = true; sec.querySelector('[data-ps="open"]').click(); await until(() => view.querySelectorAll('.psCard').length === 2 && calls.filter(c => c[0] === 'list').length === 2);
  view.querySelector('.psCard[data-id="p-old"]').click(); await until(() => view.querySelector('[data-ps="use"]'));
  view.querySelector('[data-ps="use"]').click(); await until(() => calls.some(c => c[0] === 'seat'));
  assert.deepEqual(calls.find(c => c[0] === 'seat'), ['seat', 'p-old', true]); await until(() => !details.open);
  assert(view.hidden, 'the panel closed on the work'); await until(() => calls.some(c => c[0] === 'changed')); // the data layer is told the partial sheet was used
  await until(() => toasts.some(t => /3 pieces seated/.test(t[0]))); assert.equal(toasts.find(t => /seated/.test(t[0]))[1], 'ok');

  // 7. a sheet the engine says cannot move (already cut) says why, in words, and asks the engine nothing
  details.open = true; sec.querySelector('[data-ps="open"]').click(); await until(() => view.querySelectorAll('.psCard').length === 2);
  lock = 'Sheet 1 is already cut, so it stays on the sheet it was cut from.'; w.Gate.renderCard(sh); const before = calls.filter(c => c[0] === 'preview').length;
  view.querySelector('.psCard').click(); assert.equal(calls.filter(c => c[0] === 'preview').length, before); assert.equal(toasts.slice(-1)[0][1], 'bad'); assert.match(view.querySelector('[data-ps="list"]').textContent, /already cut/); lock = '';

  // 8. the rule: automatic reuse, or a brand new sheet at a size the person sets (5 to 500 mm, the Sheet dimensions conventions)
  const radios = [...view.querySelectorAll('.psRadio input')]; assert.deepEqual(radios.map(r => r.value), ['auto', 'new']); assert(radios[0].checked);
  assert.match(view.querySelector('.psPolicy').textContent, /Reuse partial sheets automatically/); assert.match(view.querySelector('.psPolicy').textContent, /Offer a brand new sheet and let me set its size/);
  assert(view.querySelector('[data-ps="size"]').hidden, 'no size field while partial sheets are reused');
  radios[1].checked = true; radios[1].dispatchEvent(new w.Event('change', { bubbles: true })); await until(() => calls.some(c => c[0] === 'setPolicy'));
  assert.deepEqual(calls.find(c => c[0] === 'setPolicy'), ['setPolicy', metal, 'new', 100, 50]); assert(!view.querySelector('[data-ps="size"]').hidden, 'the size fields appear');
  const wIn = view.querySelector('[data-ps="w"]'), hIn = view.querySelector('[data-ps="h"]'); assert.deepEqual([wIn.min, wIn.max, hIn.min, hIn.max], ['5', '500', '5', '500']); await until(() => /5–500 mm per side|Saved/.test(view.querySelector('[data-ps="polhelp"]').textContent));
  wIn.value = '2'; wIn.dispatchEvent(new w.Event('input', { bubbles: true })); const n0 = calls.length; view.querySelector('[data-ps="polsave"]').click();
  assert.equal(calls.length, n0, 'a size outside 5 to 500 is refused before anything is saved'); assert.equal(toasts.slice(-1)[0][0], 'Use a width and height between 5 and 500 mm');
  wIn.value = '120.5'; hIn.value = '60'; await until(() => !view.querySelector('[data-ps="polsave"]').disabled); view.querySelector('[data-ps="polsave"]').click(); await until(() => calls.filter(c => c[0] === 'setPolicy').length === 2);
  assert.deepEqual(calls.filter(c => c[0] === 'setPolicy')[1], ['setPolicy', metal, 'new', 120.5, 60]);
  await until(() => /120\.5 × 60 mm/.test(sec.querySelector('[data-ps="sum"]').textContent)); assert.match(sec.querySelector('[data-ps="sum"]').textContent, /120\.5 × 60 mm/);

  // 9. the chain strip on the sheet card: 1, 2, 3 ... with the sheets' names, no limit; hidden again when no sheet sits on a partial sheet
  chain = [1, 2, 3, 4, 5, 6].map(i => ({ sheetId: 's' + i, label: 'Sheet ' + i, partialId: 'p' + i, placed: i, state: i < 6 ? 'full' : 'open', page: i, wMm: 60, hMm: 50 }));
  w.Gate.renderCard(sh); const strip = sh.el.querySelector('.psChain'); assert(strip && !strip.hidden, 'the chain strip shows on the card');
  assert.deepEqual([...strip.querySelectorAll('.psChip .n')].map(n => n.textContent), ['1', '2', '3', '4', '5', '6'], 'six partial sheets, no limit'); assert(!strip.classList.contains('few'), 'a long chain shows 1, 2, 3 ... only');
  chain = []; w.Gate.renderCard(sh); assert(sh.el.querySelector('.psChain').hidden);

  // 10. nothing ticked on a timer, no other metal's list was ever read
  assert.equal(timers, 0, 'no setInterval: the panel never polls'); assert(calls.filter(c => c[0] === 'list').every(c => c[1] === metal), 'only this metal was read');
  dom.window.close();
}
(async () => {
  await run('rose', 'Rose Gold');
  await run('gold14k', '14K Gold');
  await run('gold10k', '10K Gold');
  console.log('partial-panel: ok (Partial Sheet button in Options for RG, 14K and 10K; one list call per open; cards: true-scale picture, size, estimate, last use, newest first, own metal only; answer in words with Use this one / Cancel; chain of partial sheets; Esc; rule and size; chain strip; no polling; Library Leftover view gone)');
})().catch(e => { console.error(e); process.exit(1); });
