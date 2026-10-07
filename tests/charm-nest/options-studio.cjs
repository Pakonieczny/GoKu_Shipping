// The Options Studio (charm-nest-options-modal.js, OptionsStudio): ONE large window for a sheet card's Options, in jsdom, with the page's own Gate
// (charm-nest-bridge.js renderRelease), Cut Sheet's controls (charm-nest-rose-ui.js), the Partial sheets card (charm-nest-partial-ui.js) and fakes for the
// data layer (PartialSheets: list, history, searchAll, policy) and for OptionsHistory (the drawing is OPT-HIST's own and has its own test).
// Proves: the Options button opens the window with the four cards; the include / size / Apply size / contour / merge controls still fire their handlers
// from inside it, and a repaint keeps the focus and a typed value; Esc closes it, puts the controls back and returns the focus; the partial sheet cards
// and the policy cards show; the history card renders from a fixture; the search filters ONE loaded list (nothing is read per keystroke).
//   node tests/charm-nest/options-studio.cjs      (needs jsdom: the test says so and passes nothing when it is missing)
const assert = require('node:assert/strict'), fs = require('node:fs');
let JSDOM; try { ({ JSDOM } = require('jsdom')); } catch (_) { console.log('options-studio: SKIPPED (jsdom is not installed)'); process.exit(0); }
const R = require('../../charm-nest-rose'), O = require('../../charm-nest-orders');
const outline = { subpaths: [[['m', [0, 0]], ['l', [10, 0]], ['l', [10, 10]], ['l', [0, 10]], ['h']]] };
const wait = ms => new Promise(r => setTimeout(r, ms));
const until = async (f, what) => { for (let n = 0; n < 300; n++) { if (f()) return; await wait(5); } assert.fail('timed out waiting for ' + what); };

// ── static: the window is built and versioned, the popover is gone ──
const html = fs.readFileSync('charm-nest-1.html', 'utf8'), build = fs.readFileSync('scripts/build-public.cjs', 'utf8'), bridge = fs.readFileSync('charm-nest-bridge.js', 'utf8');
assert(/charm-nest-options-modal\.js\?v=\d{8}-[^"']+/.test(html), 'the window is on the page with a ?v= token'); assert(build.includes('"charm-nest-options-modal.js"'), 'and in the public build');
assert(!/<details class="sheetOptions"/.test(bridge) && /class="sheetOptionsBtn"/.test(bridge), 'the Options summary is a pill button now, not a details popover');
assert(/dialog\.osDlg\{width:min\(1400px,92vw\);height:min\(90vh,1000px\)/.test(html), 'about 92vw x 90vh, 1400 px at most');
assert(/@media \(prefers-reduced-motion:reduce\)\{dialog\.osDlg\[open\]/.test(html), 'reduced motion shows a fade only');

const card = (metal, id, o = {}) => ({ id, metal, code: { rose: 'RG', gold10k: '10K', gold14k: '14K' }[metal], status: 'available', outline: [[[0, 0], [60, 0], [60, 50], [30, 50], [30, 20], [0, 20]]], sheetWMm: 100, sheetHMm: 50, wMm: 60, hMm: 50, areaMm2: 2400, bboxMm: { x: 0, y: 0, w: 60, h: 50 },
  sourceSheet: 'Sheet 2', sourceSet: 'Set 4', cutAt: Date.now() - 5 * 864e5, cutBy: 'Ana', lastUsedAt: Date.now() - 5 * 864e5, lastUsedBy: '', lastUsedSheet: '', estimate: { pieces: 7, low: 5, high: 9 }, stockId: 'stock-' + id, revision: 1, ...o });

(async () => {
  const metal = 'gold14k', word = '14K Gold';
  const dom = new JSDOM('<div id="toasts"></div><section id="sheet"><div class="shHead"></div><div class="shGate" data-r="gate"></div><div class="shPreviewWrap"></div></section>', { url: 'https://example.test', runScripts: 'outside-only' }), w = dom.window, doc = w.document;
  w.IntersectionObserver = undefined;   // (jsdom has none: the search list is read at once, as on a page without it)
  w.CharmNestRose = R; w.CharmNestOrders = O; w.confirm = () => true; w.Motion = { reduced: () => true };
  const toasts = [], WPT = 100 / 25.4 * 72, HPT = 50 / 25.4 * 72;
  const mk = n => { const charms = [0, 1].map(i => ({ id: `c${n}-${i}`, poolId: `p${n}${i}`, outline, centerPt: [5, 5], members: [] })); return { metal, page: n, runId: 'run-test', sheetId: `${metal}-test-${n}`, charms, placements: charms.map((c, i) => ({ id: c.id, cxPt: 10 + i * 20, cyPt: 10, angle: 0, scale: 1 })), persistedDone: true, verification: { ok: true }, status: 'complete', dirty: false, draft: true, outputs: { ai: 'a' } }; };
  const sh = mk(1), sh2 = mk(2); sh.el = doc.getElementById('sheet');
  const settings = { stock: {} };
  w.CN = { S: { cloud: { ok: true }, settings, mode: 'nest', sheets: { [metal]: { cardEl: sh.el, pages: [sh, sh2] } } }, METALS: [{ key: metal, label: word, color: '#d9b545' }], esc: s => String(s).replace(/</g, '&lt;'), stockFor: () => ({ wPt: WPT, hPt: HPT, wIn: WPT / 72, hIn: HPT / 72 }), uid: () => 'test',
    allSheets: () => [sh, sh2], pagesOf: () => [sh, sh2], drawPreview() { }, labelOf: () => word, renderCard: p => { w.Gate.renderCard(p); w.RoseStock && w.RoseStock.render(p); }, toast(m, k) { toasts.push([String(m), k]); }, sheetDirty: p => { p.dirty = true; }, flushManualIntake() { }, api: async () => ({}) };
  w.sh = sh; w.sh2 = sh2;
  w.eval(`var C=window.CN,S=C.S,CN=C,B=window.B={run:{runId:'run-test',releasePolicy:2,status:'review',solidIncluded:{}}};
 var O=window.CharmNestOrders,allSheets=()=>[sh,sh2],pagesOf=()=>[sh,sh2],stockFor=C.stockFor,esc=C.esc,labelOf=()=> '${word}';
 var Sets={ofRun:()=>[]},RunCtl={},Session=window.Session={schedule(){}},toast=C.toast,refreshAllCards=()=>{},api=C.api;
 var sheetDirty=()=>{},saveSettings=()=>{window.saved=(window.saved||0)+1},startNest=()=>{};`);
  const src = fs.readFileSync('charm-nest-bridge.js', 'utf8'); w.eval(src.slice(src.indexOf('const Gate ='), src.indexOf('/* ═══ 21', src.indexOf('const Gate ='))));

  // ── fakes: the data layer (every call counted) and OptionsHistory ──
  const calls = [], pol = { mode: 'auto', wMm: 100, hMm: 50 };
  const mine = [card(metal, 'p-old', { cutAt: Date.now() - 9 * 864e5, lastUsedAt: Date.now() - 9 * 864e5 }), card(metal, 'p-new', { wMm: 40, hMm: 30, areaMm2: 1000, estimate: { pieces: 3, low: 3, high: 3 }, lastUsedAt: Date.now() - 36e5, lastUsedBy: 'Bo', lastUsedSheet: 'Sheet 3' })];
  const everything = [...mine, card(metal, 'p-used', { status: 'used', usedBySheetName: '14K Sheet 5', sourceSheet: 'Sheet 9', cutBy: 'Cy', cutAt: Date.UTC(2026, 9, 5, 8, 57) }), card('rose', 'r-1', { sourceSheet: 'RG Sheet 1', cutBy: 'Dee' }), card('gold10k', 'k-1', { status: 'discarded', sourceSheet: '10K Sheet 2' })];
  const fixture = { ok: true, stock: { id: 'stock-own', metal, code: '14K', wMm: 100, hMm: 50, revision: 2, ownerSheetId: sh.sheetId, ownerSheetName: '14K Gold Sheet 1' },
    cuts: [1, 2].map(n => ({ n, revision: n, at: Date.UTC(2026, 9, 4 + n, 8, 57), by: n === 1 ? 'Paul' : 'Ana', sheetId: 's' + n, sheetName: 'Sheet ' + n, setName: 'Set ' + n, via: 'cut', rings: [[[0, 0], [100 - n * 20, 0], [100 - n * 20, 50], [0, 50]]], areaMm2: 1000, bboxMm: { x: 0, y: 0, w: 60, h: 50 }, exact: true })), rev: 'r2' };
  let listGate = null, release;
  w.PartialSheets = {
    cached: () => null, on() { }, policy: () => ({ ...pol }), changed() { calls.push(['changed']); },
    async list(m, o) { calls.push(['list', m, !!(o && o.force)]); if (listGate) await new Promise(r => { release = r; }); return { items: mine, rev: 1 }; },
    async setPolicy(m, v) { calls.push(['setPolicy', m, v.mode, v.wMm, v.hMm]); Object.assign(pol, v); return { ...pol }; }, async plan() { return {}; },
    async history(arg) { calls.push(['history', JSON.stringify(arg)]); return JSON.parse(JSON.stringify(fixture)); },
    async searchAll(o) { calls.push(['searchAll', !!(o && o.force)]); return { items: everything, rev: 1, more: false }; } };
  w.PartialNest = { canSeat: () => ({ ok: true }), chain: () => [], on() { },
    async preview(s, ids) { calls.push(['preview', ids.join('+')]); return { ok: true, pieces: 2, fitsAll: true, links: [{ partialId: ids[0], wMm: 40, hMm: 30, placed: 2 }], continues: { n: 0, next: 'none' } }; },
    async seat() { return { ok: true, moved: 2, continues: 0 }; } };
  const drawn = [];
  w.OptionsHistory = {
    svg(h, o) { drawn.push(['svg', h.cuts.length, o && o.selected]); return `<svg class="ohSvg" data-cuts="${h.cuts.length}"></svg>`; },
    timeline(h) { return `<ol class="ohList">${h.cuts.map(c => `<li data-cut="${c.n}">Cut ${c.n} · ${c.by}</li>`).join('')}</ol>`; },
    bind(root, h, o) { drawn.push(['bind', root.className]); },
    filter(items, query, o = {}) { const t = String(query || '').toLowerCase().split(/\s+/).filter(Boolean); return items.filter(c => (!o.metal || c.metal === o.metal) && (!o.status || c.status === o.status) && t.every(x => JSON.stringify(c).toLowerCase().includes(x))); } };
  w.eval(fs.readFileSync('charm-nest-rose-ui.js', 'utf8'));
  w.eval(fs.readFileSync('charm-nest-partial-ui.js', 'utf8'));
  w.eval(fs.readFileSync('charm-nest-options-modal.js', 'utf8'));
  w.CN.renderCard(sh);

  // 1. closed: a pill button, the controls hidden in their card's gate node, no window, nothing read
  const gate = sh.el.querySelector('.shGate'), btn = gate.querySelector('.sheetOptionsBtn'), box = gate._optBox;
  assert(btn && /^Options/.test(btn.textContent) && btn.tagName === 'BUTTON', 'a plain Options button'); assert(box && box.hidden && gate.contains(box), 'the controls wait in the gate node');
  assert(!doc.querySelector('dialog.osDlg') && calls.length === 0, 'closed: no window, nothing read');
  for (const k of ['include', 'w', 'h', 'size', 'status']) assert(box.querySelector(`[data-solid="${k}"]`), 'the hook stays: data-solid=' + k);
  assert(box.querySelector('[data-rose-allowance]') && box.querySelector('[data-rose-choose]'), 'the contour allowance and the stock choice are in the same box');

  // 2. open: the window, its four cards in order, the controls are the same elements, the focus rests on the window
  const width = box.querySelector('[data-solid="w"]');
  btn.focus(); btn.click();
  const dlg = doc.querySelector('dialog.osDlg'); assert(dlg && dlg.hasAttribute('open'), 'the window opens');
  assert.equal(dlg.querySelector('.osTitle h2').textContent, '14K Gold · Sheet 1', 'the title names the sheet'); assert(dlg.querySelector('.osMetal').textContent === '14K');
  assert.deepEqual([...box.children].map(c => c.dataset.card), ['sheet', 'partial', 'history', 'all'], 'four cards on one page'); assert(dlg.contains(box) && !box.hidden, 'the controls are mounted in it');
  assert.equal(width, dlg.querySelector('[data-solid="w"]'), 'the very same input'); assert(dlg.querySelector('.osCardTitle').textContent === 'This sheet');
  assert(!dlg.querySelector('details,summary'), 'no sub menu, no second pop-up');
  assert.deepEqual(box.children[1].querySelector('.osCardTitle').textContent, 'Partial sheets');
  assert.match(box.querySelector('[data-card="history"]').textContent, /not on a physical sheet yet/, 'a sheet that holds no physical sheet yet says so, and reads no history');

  // 3. the controls fire their handlers from inside it, and a repaint keeps the focus and a typed value
  const height = box.querySelector('[data-solid="h"]'), allowance = box.querySelector('[data-rose-allowance]');
  width.focus(); width.value = '120.5'; width.dispatchEvent(new w.Event('input')); height.value = '60'; height.dispatchEvent(new w.Event('input')); allowance.value = '0.35'; allowance.dispatchEvent(new w.Event('input'));
  for (let n = 0; n < 6; n++) w.CN.renderCard(sh);
  assert.equal(doc.activeElement, width, 'a repaint never steals the focus'); assert.equal(width.value, '120.5'); assert.equal(allowance.value, '0.35'); assert.equal(box.querySelector('[data-solid="w"]'), width, 'and replaces nothing');
  box.querySelector('[data-solid="size"]').click(); await until(() => w.saved === 1);
  assert.deepEqual(Array.from(settings.stock[metal], n => +(n * 25.4).toFixed(2)), [120.5, 60], 'Apply size ran its handler');
  allowance.dispatchEvent(new w.Event('change')); assert.equal(sh.roseAllowanceMm, 0.35, 'the contour allowance ran its handler');
  const include = box.querySelector('[data-solid="include"]'); assert.equal(typeof include.onchange, 'function', 'Include is wired'); assert(!include.disabled);
  const merge = box.querySelector('[data-solid="merge"]'); assert(merge && !merge.hidden, 'two sheets of 14K can be merged: the section shows');
  box.querySelector('[data-solid="merge-move"]').click(); assert(!box.querySelector('[data-solid="merge-ask"]').hidden && /go into Sheet 1's free room/.test(box.querySelector('[data-solid="merge-ask-text"]').textContent), 'Move all asks first, in words');
  box.querySelector('[data-solid="merge-cancel"]').click(); assert(box.querySelector('[data-solid="merge-ask"]').hidden, 'Cancel puts the question away');

  // 4. partial sheets: the cards of this metal, the answer in place, the policy as two large cards
  await until(() => box.querySelectorAll('.psView .psCard').length === 2);
  const pv = box.querySelector('.psView'); assert.deepEqual([...pv.querySelectorAll('.psCard')].map(c => c.dataset.id), ['p-new', 'p-old'], 'newest used first'); assert.match(pv.querySelector('.psFit').textContent, /About 3 pieces/);
  assert.deepEqual(calls.filter(c => c[0] === 'list'), [['list', metal, false]], 'one list call');
  pv.querySelector('.psCard[data-id="p-new"]').click(); await until(() => pv.querySelector('[data-ps="use"]'));
  assert.match(pv.querySelector('.psAsk').textContent, /All 2 pieces on Sheet 1 fit on this partial sheet/); assert(pv.querySelector('[data-ps="cancel"]'), 'Use this one / Cancel, in place');
  pv.querySelector('[data-ps="cancel"]').click(); assert(!pv.querySelector('.psAsk'), 'Cancel changes nothing');
  const radios = [...pv.querySelectorAll('.psRadio')]; assert.deepEqual(radios.map(r => r.dataset.v), ['auto', 'new'], 'two policy cards'); assert(radios[0].classList.contains('on'));
  assert.match(radios[0].textContent, /Reuse automatically/); assert.match(radios[1].textContent, /Offer a brand new sheet/); assert(pv.querySelector('[data-ps="size"]').hidden, 'no size while partial sheets are reused');
  radios[1].click(); await until(() => calls.some(c => c[0] === 'setPolicy'));   // (a press anywhere on the card chooses it)
  assert.deepEqual(calls.find(c => c[0] === 'setPolicy'), ['setPolicy', metal, 'new', 100, 50]); assert(!pv.querySelector('[data-ps="size"]').hidden, 'width and height appear in the new-sheet card'); assert(radios[1].contains(pv.querySelector('[data-ps="w"]')));

  // 5. the history card, from a fixture: the sheet takes its physical sheet (the next draw of the controls says so): one call for it, the drawing and the timeline from OptionsHistory
  assert.equal(calls.filter(c => c[0] === 'history').length, 0); sh.roseStock = { id: 'stock-own', wPt: WPT, hPt: HPT, revision: 2 }; w.CN.renderCard(sh);
  await until(() => dlg.querySelector('.osHist'));
  assert.deepEqual(calls.filter(c => c[0] === 'history'), [['history', '{"stockId":"stock-own"}']], 'one history call, for the physical sheet this sheet holds');
  assert(dlg.querySelector('.osHistDraw svg.ohSvg[data-cuts="2"]'), 'the drawing'); assert.equal(dlg.querySelectorAll('.osHistSide li').length, 2, 'the timeline: one line per cut'); assert(drawn.some(d => d[0] === 'bind'), 'drawing and list are bound');
  assert.match(box.querySelector('[data-card="history"] .osLead').textContent, /14K sheet, 100 × 50 mm · 2 cuts · held by 14K Gold Sheet 1/);

  // 6. the search: ONE list, read once; the browser filters it; a press on a result shows its history
  await until(() => box.querySelectorAll('[data-card="all"] .psCard').length === 5);
  assert.deepEqual(calls.filter(c => c[0] === 'searchAll'), [['searchAll', false]], 'one list call for the search');
  const all = box.querySelector('[data-card="all"]'), input = all.querySelector('input[type=search]'), ids = () => [...all.querySelectorAll('.psCard')].map(c => c.dataset.id);
  assert(all.querySelector('.psCard[data-id="p-used"] .psStat[data-s="used"]'), 'a used sheet says so on its card (the same card)');
  input.value = 'cy'; input.dispatchEvent(new w.Event('input', { bubbles: true })); await until(() => ids().length === 1); assert.deepEqual(ids(), ['p-used'], 'text search');
  input.value = ''; input.dispatchEvent(new w.Event('input', { bubbles: true })); await until(() => ids().length === 5);
  all.querySelector('[data-os="metal"][data-v="rose"]').click(); assert.deepEqual(ids(), ['r-1'], 'metal chip'); all.querySelector('[data-os="metal"][data-v=""]').click(); assert.equal(ids().length, 5);
  all.querySelector('[data-os="status"][data-v="discarded"]').click(); assert.deepEqual(ids(), ['k-1'], 'status chip'); all.querySelector('[data-os="clear"]').click(); assert.equal(ids().length, 5);
  assert.equal(calls.filter(c => c[0] === 'searchAll').length, 1, 'typing and the chips read nothing');
  all.querySelector('.psCard[data-id="p-used"]').click(); await until(() => calls.some(c => c[0] === 'history' && /stock-p-used/.test(c[1])));
  assert.deepEqual(calls.filter(c => c[0] === 'history').slice(-1)[0], ['history', '{"stockId":"stock-p-used"}']); await until(() => /sheet picked in the search/.test(box.querySelector('[data-card="history"] [data-os="hwho"]').textContent));
  assert(all.querySelector('.psCard[data-id="p-used"]').classList.contains('on'), 'the picked result is marked'); assert(box.querySelector('[data-os="own"]'), 'and the way back to this sheet is there');

  // 7. a toast sits under a modal window: what it says is said in the window's note too
  const t = doc.createElement('div'); t.className = 'toast bad'; t.dataset.msg = 'Use a width and height between 5 and 500 mm'; t.dataset.kind = 'bad'; doc.getElementById('toasts').appendChild(t);
  await until(() => /between 5 and 500 mm/.test(dlg.querySelector('.osNote').textContent)); assert(dlg.querySelector('.osNote').classList.contains('on'));

  // 8. Esc: an answer that waits goes first, then the window; the controls go home, hidden; the focus returns to the Options button
  width.value = '77'; width.dispatchEvent(new w.Event('input')); pv.querySelector('.psCard[data-id="p-old"]').click(); await until(() => pv.querySelector('[data-ps="use"]'));
  const esc = () => dlg.dispatchEvent(new w.KeyboardEvent('keydown', { key: 'Escape', bubbles: true, cancelable: true }));
  esc(); assert(!pv.querySelector('.psAsk') && doc.querySelector('dialog.osDlg'), 'the first Esc puts the answer away only');
  esc(); await until(() => !doc.querySelector('dialog.osDlg'));
  assert(box.hidden && gate.contains(box) && box.children.length === 2, 'the controls are back in their card, without the two cards of the window'); assert.equal(doc.activeElement, btn, 'the focus is back on the Options button');
  assert.equal(width.value, '77', 'what was typed is still there'); w.CN.renderCard(sh); assert.equal(width.value, '77', 'and a draw of the card keeps it'); assert(!w.Gate.state().optionsOpen[metal]);

  // 9. open again: one more list call (never a timer), the search list is the one already read; the close button closes it, the backdrop too
  btn.click(); await until(() => calls.filter(c => c[0] === 'list').length === 2 && doc.querySelector('.osHist'));
  assert.equal(calls.filter(c => c[0] === 'searchAll').length, 2, 'a second opening probes the search list once more, nothing per keystroke'); assert(doc.querySelectorAll('dialog.osDlg').length === 1);
  doc.querySelector('[data-os="close"]').click(); await until(() => !doc.querySelector('dialog.osDlg')); assert.equal(doc.activeElement, btn);
  // a sheet drawn anew for another page puts the window away at once
  btn.click(); assert(doc.querySelector('dialog.osDlg')); gate._sheetOptionsOwner = null; w.CN.renderCard(sh); assert(!doc.querySelector('dialog.osDlg'), 'controls drawn anew: the window is put away');

  console.log('options-studio: OK');
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
