// The Options Studio (charm-nest-options-modal.js, OptionsStudio), round 2: ONE large window for a sheet card's Options, in jsdom, with the page's own Gate
// (charm-nest-bridge.js renderRelease), Cut Sheet's controls (charm-nest-rose-ui.js), the work behind Use this one (charm-nest-partial-ui.js) and fakes for the
// data layer (PartialSheets: searchAll, history, make, remove, policy); the real OptionsHistory draws the cards and filters the repository.
// Proves: the Options button opens the window with ONE Sheet menu (no chip strips, no Partial sheets card, no Sheet history card) holding Include, the size row, the rule as
// a two-option switch and the Sheet source with its two options; the include / size / Apply size / contour / merge controls still fire their handlers from inside it, and a
// repaint keeps the focus and a typed value; From partial sheets is ONE list read once, filtered in the browser (this metal and Available first), as large cards with Use this one
// (the answer in place) and Delete; New sheet makes a sheet that appears as a card (any number); Delete asks why in a pop-up (refuses an empty answer, shows who, calls remove,
// shows the DELETED stamp under the Deleted chip); Esc goes pop-up, answer, enlarged card, window; the focus returns to the Options button.
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
const pui = fs.readFileSync('charm-nest-partial-ui.js', 'utf8'), modal = fs.readFileSync('charm-nest-options-modal.js', 'utf8');
assert(!/paintChain|psChip|chipsHtml|class = 'osCard psView|ensureView/.test(pui), 'the chip strips and the Partial sheets card are gone from the code');
assert(!/data-card="(partial|history|all)"|data-os="hbody"|osHistory/.test(modal + bridge), 'and from the window');
assert(/dialog\.osAskDlg\{/.test(html) && /charm-nest-options-history\.js\?v=/.test(html), 'the delete pop-up has its look, the cards their script');

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

  // ── fakes: the data layer (every call counted); OptionsHistory is the real one ──
  const calls = [], pol = { mode: 'auto', wMm: 100, hMm: 50 }, day = 864e5, now = Date.now();
  // the repository list as searchAll gives it: every record of every stock (the earlier revisions of a stock are its 'used' records), newest first
  const rec = (m, id, o = {}) => card(m, id, { stockId: 'stock-' + id, revision: 1, ...o });
  const everything = [
    rec(metal, 'p-new', { wMm: 40, hMm: 30, areaMm2: 1000, estimate: { pieces: 3, low: 3, high: 3 }, lastUsedAt: now - 36e5, lastUsedBy: 'Bo', lastUsedSheet: 'Sheet 3', cutAt: now - 2 * day }),
    rec(metal, 'p-old', { cutAt: now - 9 * day, lastUsedAt: now - 9 * day }),
    rec(metal, 'p-held', { status: 'inUse', inUseBySheetName: '14K Sheet 7', cutAt: now - 6 * day }),
    rec(metal, 'p-used', { status: 'used', usedBySheetName: '14K Sheet 5', sourceSheet: 'Sheet 9', cutBy: 'Cy', cutAt: Date.UTC(2026, 9, 5, 8, 57) }),
    rec('rose', 'r-1', { sourceSheet: 'RG Sheet 1', cutBy: 'Dee' }), rec('gold10k', 'k-1', { status: 'discarded', sourceSheet: '10K Sheet 2' }),
    { ...rec(metal, 'n-old', { revision: 0, kind: 'new', status: 'available', outline: [[[0, 0], [80, 0], [80, 40], [0, 40]]], sheetWMm: 80, sheetHMm: 40, wMm: 80, hMm: 40, areaMm2: 3200, bboxMm: { x: 0, y: 0, w: 80, h: 40 }, sourceSheet: 'New sheet 80 x 40 mm', sourceSet: '', cutBy: 'Eli', cutAt: now - 3 * day, madeAt: now - 3 * day, madeBy: 'Eli', lastUsedAt: now - 3 * day, lastUsedBy: 'Eli' }), id: 'stock-n-old-0' },
    { ...rec(metal, 'd-1', { revision: 0, kind: 'new', status: 'deleted', outline: [[[0, 0], [50, 0], [50, 50], [0, 50]]], sheetWMm: 50, sheetHMm: 50, wMm: 50, hMm: 50, areaMm2: 2500, sourceSheet: 'New sheet 50 x 50 mm', cutAt: now - 4 * day, madeAt: now - 4 * day, madeBy: 'Fay', deletedAt: now - day, deletedBy: 'Gus', deletedReason: 'made by accident' }), id: 'stock-d-1-0' }];
  const fixture = id => ({ ok: true, stock: { id, metal, code: '14K', wMm: 100, hMm: 50, revision: 1, ownerSheetId: null, ownerSheetName: '' },
    cuts: [{ n: 1, revision: 1, at: Date.UTC(2026, 9, 5, 8, 57), by: 'Paul', sheetId: 's1', sheetName: 'Sheet 1', setName: 'Set 1', via: 'cut', rings: [[[0, 0], [60, 0], [60, 50], [30, 50], [30, 20], [0, 20]]], areaMm2: 2400, bboxMm: { x: 0, y: 0, w: 60, h: 50 }, exact: true }], made: null, deleted: null, rev: 'r1' });
  let gate = null, openGate, madeN = 0; w.CNEmployee = { name: () => 'Paul' };
  w.PartialSheets = {
    cached: () => null, on() { }, policy: () => ({ ...pol }), changed() { calls.push(['changed']); }, async loadPolicy() { calls.push(['loadPolicy']); return {}; },
    async list(m) { calls.push(['list', m]); return { items: [], rev: 1 }; },
    async setPolicy(m, v) { calls.push(['setPolicy', m, v.mode, v.wMm, v.hMm]); Object.assign(pol, v); return { ...pol }; }, async plan() { return {}; },
    async history(arg) { calls.push(['history', JSON.stringify(arg)]); return fixture(arg.stockId); },
    async searchAll(o) { calls.push(['searchAll', !!(o && o.force)]); return { items: everything, rev: 1, more: false }; },
    async make(v) {
      calls.push(['make', v.metal, v.wMm, v.hMm]); if (gate === 'make') await new Promise(r => { openGate = r; });
      const id = 'nsh-' + (++madeN), W = v.wMm, H = v.hMm;
      return { ok: true, item: { id: id + '-0', stockId: id, revision: 0, kind: 'new', metal: v.metal, code: '14K', status: 'available', outline: [[[0, 0], [W, 0], [W, H], [0, H]]], sheetWMm: W, sheetHMm: H, wMm: W, hMm: H, areaMm2: W * H, bboxMm: { x: 0, y: 0, w: W, h: H }, sourceSheet: `New sheet ${W} x ${H} mm`, sourceSet: '', cutAt: Date.now(), cutBy: 'Paul', madeAt: Date.now(), madeBy: 'Paul', lastUsedAt: Date.now(), lastUsedBy: 'Paul', estimate: { pieces: 5, low: 4, high: 6 } } };
    },
    async remove(id, reason) {
      calls.push(['remove', id, reason]); if (gate === 'remove') await new Promise(r => { openGate = r; });
      const it = everything.find(x => x.id === id) || window_made.find(x => x.id === id) || { id, metal, stockId: id.replace(/-0$/, ''), revision: 0, kind: 'new', outline: [], sheetWMm: 100, sheetHMm: 50, wMm: 100, hMm: 50 };
      return { ok: true, item: { ...it, status: 'deleted', deletedAt: Date.now(), deletedBy: 'Paul', deletedReason: reason } };
    } };
  const window_made = [];
  w.PartialNest = { canSeat: () => ({ ok: true }), chain: () => [{ sheetId: 'x1', label: '14K Sheet 1', partialId: 'p-new', placed: 2, state: 'open', page: 1, wMm: 40, hMm: 30 }], on() { },
    async preview(s, ids) { calls.push(['preview', ids.join('+')]); return { ok: true, pieces: 2, fitsAll: true, links: [{ partialId: ids[0], wMm: 40, hMm: 30, placed: 2 }], continues: { n: 0, next: 'none' } }; },
    async seat(s, ids) { calls.push(['seat', ids.join('+')]); return { ok: true, moved: 2, continues: 0 }; } };
  w.eval(fs.readFileSync('charm-nest-rose-ui.js', 'utf8'));
  w.eval(fs.readFileSync('charm-nest-partial-ui.js', 'utf8'));
  w.eval(fs.readFileSync('charm-nest-options-history.js', 'utf8'));   // (OPT-HIST's own drawing, timeline and filter: the window only calls them)
  w.eval(fs.readFileSync('charm-nest-options-modal.js', 'utf8'));
  w.CN.renderCard(sh);

  const esc = el => el.dispatchEvent(new w.KeyboardEvent('keydown', { key: 'Escape', bubbles: true, cancelable: true }));
  const submit = form => form.dispatchEvent(new w.Event('submit', { bubbles: true, cancelable: true }));

  // 1. closed: a pill button, the controls hidden in their card's gate node, no window, nothing read, no chip strip on the sheet card (the engine HAS a chain: it is not drawn)
  const gateEl = sh.el.querySelector('.shGate'), btn = gateEl.querySelector('.sheetOptionsBtn'), box = gateEl._optBox;
  assert(btn && /^Options/.test(btn.textContent) && btn.tagName === 'BUTTON', 'a plain Options button'); assert(box && box.hidden && gateEl.contains(box), 'the controls wait in the gate node');
  assert(!doc.querySelector('dialog.osDlg') && calls.length === 0, 'closed: no window, nothing read');
  assert(!sh.el.querySelector('.psChain, .psChip') && !/Partial sheets/i.test(sh.el.textContent), 'no PARTIAL SHEETS chip strip on the sheet card');
  for (const k of ['include', 'status', 'retry']) assert(box.querySelector(`[data-solid="${k}"]`), 'the hook stays: data-solid=' + k);
  for (const k of ['w', 'h', 'size', 'size-help']) assert(!sh.el.querySelector(`[data-solid="${k}"]`), 'the Sheet dimensions card is gone: no data-solid=' + k);
  assert(!/Sheet dimensions|Apply size|Dimensions belong/.test(sh.el.textContent + box.textContent), 'and none of its words');
  assert(box.querySelector('[data-rose-allowance]') && box.querySelector('[data-rose-choose]'), 'the contour allowance and the stock choice are in the same box');

  // 2. open: the window, ONE Sheet card + small Settings, the controls are the same elements, the focus rests on the window
  btn.focus(); btn.click();
  const dlg = doc.querySelector('dialog.osDlg'); assert(dlg && dlg.hasAttribute('open'), 'the window opens');
  assert.equal(dlg.querySelector('.osTitle h2').textContent, '14K Gold · Sheet 1', 'the title names the sheet'); assert(dlg.querySelector('.osMetal').textContent === '14K');
  assert.deepEqual([...box.children].map(c => c.dataset.card), ['sheet', 'settings'], 'one Sheet card and the small settings'); assert(dlg.contains(box) && !box.hidden, 'the controls are mounted in it');
  assert.equal(dlg.querySelector('.osCardTitle').textContent, 'Sheet');
  const inc = dlg.querySelector('.osHead .osInclude'); assert(inc && inc.parentNode.classList.contains('osIncSlot') && inc.querySelector('input[data-solid="include"]'), 'the include switch is in the title row, top right');
  assert.deepEqual([...dlg.querySelector('.osHead').children].map(c => c.className), ['osId', 'osIncSlot', 'osX'], 'beside the close button, before it'); assert(!box.contains(inc), 'carried up out of the box while the window is open');
  assert.equal(inc.querySelector('label').textContent.trim(), 'In current set', 'with a short title'); assert(inc.querySelector('[data-solid="retry"]').hidden && inc.querySelector('[data-solid="status"]').textContent === '', 'and no other words: the retry button shows only when a save failed, the status line is read aloud only'); assert(!inc.querySelector('[title]') && !dlg.querySelector('.osHead [title]'), 'no tooltip');
  assert(!/Include Sheet|committed set|stays there|Sheet dimensions|Apply size|Dimensions belong/.test(dlg.textContent.replace(/Include \w+ sheet \d in current set/g, '')), 'the long label, the status lines and the dimensions card are gone');
  assert(!dlg.querySelector('[data-solid="w"],[data-solid="h"],[data-solid="size"],[data-solid="size-help"],.osSheetTop,.osSize'), 'no width, height or Apply size in the window');
  assert(!dlg.querySelector('details,summary,.psView,[data-card="history"],[data-card="all"],[data-card="partial"],.psChain'), 'no sub menu, no Partial sheets card, no Sheet history card, no chip strip');
  assert(!/Sheet history|Sheets on partial sheets|All partial sheets/.test(dlg.textContent), 'and none of their words');
  const sheetCard = box.querySelector('[data-card="sheet"]'), menu = sheetCard.querySelector('.osSource'); assert(menu, 'the Sheet source is inside the Sheet card');
  const sw = menu.querySelector('.psSwitch'); assert.deepEqual([...sw.querySelectorAll('input[type=radio]')].map(r => r.value), ['auto', 'new'], 'the rule is a two-option switch'); assert(sw.querySelector('input[value="auto"]').checked);
  assert.match(menu.querySelector('.psPolicyLbl').textContent, /When a sheet needs more metal/); assert.match(sw.textContent, /Reuse partial sheets automatically/); assert.match(sw.textContent, /Offer a brand new sheet at 100 × 50 mm/);
  const tabs = [...menu.querySelectorAll('[data-os="tab"]')]; assert.deepEqual(tabs.map(t => t.querySelector('b').textContent), ['From partial sheets', 'New sheet'], 'two large options'); assert.deepEqual(tabs.map(t => t.getAttribute('aria-selected')), ['true', 'false']);
  assert(box.querySelector('[data-card="settings"] [data-rose-options]'), 'Cut contour is a small settings card below'); assert(box.querySelector('[data-card="settings"] [data-solid="merge"]'), 'and so is Merge sheets');
  assert.deepEqual(calls.filter(c => c[0] === 'list'), [], 'the window reads no partial list of its own');

  // 3. the controls fire their handlers from inside it, and a repaint keeps the focus and a typed value
  const allowance = box.querySelector('[data-rose-allowance]');
  allowance.focus(); allowance.value = '0.35'; allowance.dispatchEvent(new w.Event('input'));
  for (let n = 0; n < 6; n++) w.CN.renderCard(sh);
  assert.equal(doc.activeElement, allowance, 'a repaint never steals the focus'); assert.equal(allowance.value, '0.35'); assert.equal(box.querySelector('[data-rose-allowance]'), allowance, 'and replaces nothing'); assert.equal(dlg.querySelector('[data-solid="include"]'), inc.querySelector('input'), 'the switch is the same element after a repaint');
  allowance.dispatchEvent(new w.Event('change')); assert.equal(sh.roseAllowanceMm, 0.35, 'the contour allowance ran its handler');
  const include = dlg.querySelector('[data-solid="include"]'); assert.equal(typeof include.onchange, 'function', 'Include is wired'); assert(!include.disabled);
  const merge = box.querySelector('[data-solid="merge"]'); assert(merge && !merge.hidden, 'two sheets of 14K can be merged: the section shows');
  box.querySelector('[data-solid="merge-move"]').click(); assert(!box.querySelector('[data-solid="merge-ask"]').hidden && /go into Sheet 1's free room/.test(box.querySelector('[data-solid="merge-ask-text"]').textContent), 'Move all asks first, in words');
  box.querySelector('[data-solid="merge-cancel"]').click(); assert(box.querySelector('[data-solid="merge-ask"]').hidden, 'Cancel puts the question away');

  // 4. From partial sheets: ONE list, read once; this metal and Available first, as large cards (the thumbnail is the sheet's history); no history read yet
  await until(() => menu.querySelectorAll('.ohc').length === 3);
  const res = menu.querySelector('[data-os="results"]'), ids = () => [...res.querySelectorAll('.ohc')].map(c => c.dataset.stock);
  assert.deepEqual(calls.filter(c => c[0] === 'searchAll'), [['searchAll', false]], 'ONE list for the window'); assert(calls.some(c => c[0] === 'loadPolicy'), 'and the rule, once');
  assert.deepEqual(ids(), ['stock-p-new', 'stock-n-old', 'stock-p-old'], 'this sheet\'s metal, Available only: a partial, an older partial and a made sheet, newest first');
  assert.equal(res.querySelectorAll('.ohc[data-status="deleted"], .ohc[data-status="used"], .ohc[data-status="inUse"]').length, 0, 'the used, held and deleted ones are behind their chips');
  assert(menu.querySelector('[data-os="metal"][data-v="gold14k"]').getAttribute('aria-pressed') === 'true' && menu.querySelector('[data-os="status"][data-v="available"]').getAttribute('aria-pressed') === 'true', 'the chips say what is shown');
  assert(res.querySelector('.ohcGrid') && res.querySelector('.ohc [data-oh-thumb]') && res.querySelector('.ohc svg'), 'large cards with a thumbnail drawing');
  assert.equal(calls.filter(c => c[0] === 'history').length, 0, 'no history read for a thumbnail');
  const cardOf = id => res.querySelector(`.ohc[data-stock="${id}"]`);
  for (const id of ['stock-p-new', 'stock-p-old', 'stock-n-old']) { const c = cardOf(id); assert(c.querySelector('[data-ps="pick"]') && /Use this one/.test(c.querySelector('[data-ps="pick"]').textContent), 'Use this one on ' + id); assert(c.querySelector('[data-os="del"]') && !c.querySelector('[data-os="del"]').disabled, 'Delete on ' + id); }
  assert.match(cardOf('stock-n-old').textContent, /New sheet|Made by/, 'a made sheet says who made it');

  // 5. Use this one: the answer in place (fit words, Use this one / Cancel), a chain, Cancel changes nothing
  cardOf('stock-p-new').querySelector('[data-ps="pick"]').click(); await until(() => res.querySelector('[data-ps="use"]'));
  assert.match(res.querySelector('.psAsk').textContent, /All 2 pieces on Sheet 1 fit on this partial sheet/); assert(res.querySelector('[data-ps="cancel"]'), 'Use this one / Cancel, in place');
  assert.equal(cardOf('stock-p-new').querySelector('[data-ps="pick"]').getAttribute('aria-pressed'), 'true', 'the card says it is the chosen one');
  assert.deepEqual(calls.filter(c => c[0] === 'preview'), [['preview', 'p-new']], 'the engine is asked about the card picked');
  res.querySelector('[data-ps="cancel"]').click(); assert(!res.querySelector('.psAsk') && !calls.some(c => c[0] === 'seat'), 'Cancel changes nothing');

  // 6. a card is enlarged IN PLACE: the history is read ONCE, now; Esc folds it back before the window
  cardOf('stock-p-old').querySelector('[data-oh-thumb]').click();
  await until(() => cardOf('stock-p-old').classList.contains('ohcBig') && calls.some(c => c[0] === 'history'));
  assert.deepEqual(calls.filter(c => c[0] === 'history'), [['history', '{"stockId":"stock-p-old","revision":1}']], 'one history call, on the enlarge, with the revision (the data layer caches by it)');
  assert(cardOf('stock-p-old').querySelector('.ohcClose, [data-oh-close]'), 'a close x'); assert(doc.querySelector('dialog.osDlg') && dlg.contains(cardOf('stock-p-old')), 'in place, in the window: no pop-up');
  // Esc order: an answer that waits first, then the enlarged card, then the window
  cardOf('stock-p-new').querySelector('[data-ps="pick"]').click(); await until(() => res.querySelector('[data-ps="use"]') && cardOf('stock-p-old').classList.contains('ohcBig'), 'the answer while a card is open');
  esc(res.querySelector('[data-ps="use"]')); assert(!res.querySelector('.psAsk') && cardOf('stock-p-old').classList.contains('ohcBig') && doc.querySelector('dialog.osDlg'), 'the first Esc puts the answer away only');
  esc(cardOf('stock-p-old').querySelector('[data-oh-close]')); await until(() => !cardOf('stock-p-old').classList.contains('ohcBig')); assert(doc.querySelector('dialog.osDlg'), 'the second Esc folds the card back, the window stays');

  // 7. filters: the chips and the search work on that ONE list in the browser (nothing is read)
  const all = menu, input = all.querySelector('input[type=search]');
  all.querySelector('[data-os="status"][data-v="available"]').click(); await until(() => ids().length >= 6);   // (a second press clears the status)
  assert.deepEqual(ids().sort(), ['stock-d-1', 'stock-k-1', 'stock-n-old', 'stock-p-held', 'stock-p-new', 'stock-p-old', 'stock-p-used', 'stock-r-1'].filter(i => i !== 'stock-k-1' && i !== 'stock-r-1').sort(), 'every status of this metal');
  all.querySelector('[data-os="metal"][data-v=""]').click(); assert.equal(ids().length, 8, 'every metal, every status: one card per physical sheet');
  all.querySelector('[data-os="metal"][data-v="rose"]').click(); assert.deepEqual(ids(), ['stock-r-1'], 'metal chip'); all.querySelector('[data-os="metal"][data-v=""]').click();
  all.querySelector('[data-os="status"][data-v="discarded"]').click(); assert.deepEqual(ids(), ['stock-k-1'], 'status chip'); all.querySelector('[data-os="status"][data-v="discarded"]').click();
  input.value = 'cy'; input.dispatchEvent(new w.Event('input', { bubbles: true })); await until(() => ids().length === 1); assert.deepEqual(ids(), ['stock-p-used'], 'text search');
  input.value = ''; input.dispatchEvent(new w.Event('input', { bubbles: true })); await until(() => ids().length === 8);
  all.querySelector('[data-os="status"][data-v="deleted"]').click(); assert.deepEqual(ids(), ['stock-d-1'], 'the Deleted chip');
  const dead = cardOf('stock-d-1'); assert.equal(dead.dataset.status, 'deleted'); assert(/DELETED/i.test(dead.textContent) || dead.querySelector('.ohcStamp'), 'the DELETED stamp'); assert(!dead.querySelector('[data-os="del"]') && !dead.querySelector('[data-ps="pick"]'), 'a deleted sheet has no Delete and no Use');
  assert.equal(calls.filter(c => c[0] === 'searchAll').length, 1, 'typing, the chips and the cards read nothing more');
  all.querySelector('[data-os="clear"]').click(); assert.deepEqual(ids(), ['stock-p-new', 'stock-n-old', 'stock-p-old'], 'Reset filters is the default view again');
  const held = (all.querySelector('[data-os="status"][data-v="inUse"]').click(), cardOf('stock-p-held')); assert(held.querySelector('[data-os="del"]').disabled && /cannot be deleted/.test(held.textContent) && !held.querySelector('[data-ps="pick"]'), 'a sheet another sheet holds cannot be deleted: it says so in words');
  all.querySelector('[data-os="clear"]').click();

  // 8. New sheet: width and height (5 to 500 mm), Make new sheet: a card appears in the repository; any number of them
  const tabB = menu.querySelector('[data-os="tab"][data-v="b"]'); tabB.click(); assert.equal(tabB.getAttribute('aria-selected'), 'true'); assert(menu.querySelector('[data-os="panel"][data-v="a"]').hidden && !menu.querySelector('[data-os="panel"][data-v="b"]').hidden, 'the other option shows');
  const mw = menu.querySelector('[data-os="mw"]'), mh = menu.querySelector('[data-os="mh"]'), form = menu.querySelector('[data-os="newform"]'); assert.deepEqual([mw.min, mw.max, mh.min, mh.max], ['5', '500', '5', '500']);
  mw.value = '3'; mh.value = '60'; submit(form); assert(!calls.some(c => c[0] === 'make'), 'a width outside 5 to 500 is refused before anything is written'); assert.match(menu.querySelector('[data-os="makehelp"]').textContent, /between 5 and 500 mm/);
  mw.value = '120'; mh.value = '60'; gate = 'make'; submit(form); await until(() => calls.some(c => c[0] === 'make'));
  assert.deepEqual(calls.find(c => c[0] === 'make'), ['make', metal, 120, 60]); assert.match(menu.querySelector('[data-os="makehelp"]').textContent, /Making the new sheet/, 'a labelled spinner'); assert(menu.querySelector('[data-os="make"]').disabled);
  openGate(); gate = null; await until(() => menu.querySelector('[data-os="showmade"]')); assert.match(menu.querySelector('[data-os="makehelp"]').textContent, /New sheet 120 × 60 mm is in the repository/);
  mw.value = '90'; mh.value = '45'; submit(form); await until(() => /2 made/.test(menu.querySelector('[data-os="makehelp"]').textContent)); assert.deepEqual(calls.filter(c => c[0] === 'make').map(c => c.slice(2)), [[120, 60], [90, 45]], 'any number of new sheets, each with its own size');
  menu.querySelector('[data-os="showmade"]').click(); assert.equal(tabB.getAttribute('aria-selected'), 'false');
  await until(() => ids().length === 5); assert(ids().includes('stock-nsh-1') && ids().includes('stock-nsh-2'.replace('stock-', 'nsh-').replace(/^/, 'stock-') ) || ids().includes('nsh-2'), ids().join());
  assert.equal(calls.filter(c => c[0] === 'searchAll').length, 1, 'making a sheet reads nothing: the card comes from the answer');
  assert.match(cardOf('nsh-1').textContent, /120 × 60 mm/, 'the new card'); assert(cardOf('nsh-1').querySelector('[data-ps="pick"]') && cardOf('nsh-1').querySelector('[data-os="del"]'));

  // 9. Delete: a pop-up that asks why (a required answer), shows who deletes, Cancel / Delete; soft delete through PartialSheets.remove; the DELETED stamp under the Deleted chip
  const del1 = cardOf('nsh-2').querySelector('[data-os="del"]'); del1.focus(); del1.click();
  const ask = doc.querySelector('dialog.osAskDlg'); assert(ask && ask.hasAttribute('open'), 'the pop-up'); assert(doc.querySelectorAll('dialog[open]').length === 2 && doc.body.contains(ask) && !dlg.contains(ask), 'a native dialog above the Options window');
  assert.match(ask.textContent, /Why are you deleting it/); assert.match(ask.textContent, /Deleted by Paul, the signed-in person/); assert(!ask.querySelector('input[type=text], input:not([type])'), 'no name is ever asked for');
  assert.match(ask.textContent, /New sheet 90 × 45 mm/, 'it says which sheet'); const why = ask.querySelector('textarea'); assert.equal(why.maxLength, 300);
  ask.querySelector('form').dispatchEvent(new w.Event('submit', { bubbles: true, cancelable: true })); assert(!calls.some(c => c[0] === 'remove'), 'an empty answer is refused'); assert.match(ask.querySelector('[data-ask="err"]').textContent, /answer is required/);
  why.value = ' a '; why.dispatchEvent(new w.Event('input')); ask.querySelector('form').dispatchEvent(new w.Event('submit', { bubbles: true, cancelable: true })); assert(!calls.some(c => c[0] === 'remove'), 'two characters are not an answer'); assert.match(ask.querySelector('[data-ask="err"]').textContent, /3 to 300/);
  esc(why); await until(() => !doc.querySelector('dialog.osAskDlg')); assert(doc.querySelector('dialog.osDlg') && !calls.some(c => c[0] === 'remove') && cardOf('nsh-2'), 'Esc closes the pop-up only; the sheet is untouched'); assert.equal(doc.activeElement.dataset.os, 'del', 'the focus returns to the Delete button');
  cardOf('nsh-2').querySelector('[data-os="del"]').click(); const ask2 = doc.querySelector('dialog.osAskDlg'), why2 = ask2.querySelector('textarea'); why2.value = '  made by accident, wrong size  '; why2.dispatchEvent(new w.Event('input'));
  gate = 'remove'; ask2.querySelector('form').dispatchEvent(new w.Event('submit', { bubbles: true, cancelable: true })); await until(() => calls.some(c => c[0] === 'remove'));
  assert.deepEqual(calls.find(c => c[0] === 'remove'), ['remove', 'nsh-2-0', 'made by accident, wrong size'], 'remove(id, the reason, trimmed)'); assert.match(ask2.textContent, /Deleting the sheet/, 'a labelled spinner'); assert(ask2.querySelector('[data-ask="go"]').disabled);
  openGate(); gate = null; await until(() => !doc.querySelector('dialog.osAskDlg'));
  assert(!cardOf('nsh-2') || ALL_STATUS(), 'the card leaves the Available view'); function ALL_STATUS() { return !res.querySelector('.ohc[data-stock="nsh-2"]') || res.querySelector('.ohc[data-stock="nsh-2"]').dataset.status !== 'available'; }
  await until(() => menu.querySelector('[data-os="status"][data-v="deleted"]').getAttribute('aria-pressed') === 'true' && cardOf('nsh-2')); const gone = cardOf('nsh-2'); assert.equal(gone.dataset.status, 'deleted', 'it shows under the Deleted chip with the DELETED stamp'); assert(/DELETED/i.test(gone.textContent) || gone.querySelector('.ohcStamp'));
  assert.match(gone.textContent, /Paul/, 'who deleted it'); assert(!gone.querySelector('[data-os="del"]'), 'and it cannot be deleted twice');
  assert.equal(calls.filter(c => c[0] === 'searchAll').length, 1, 'deleting reads nothing more');
  all.querySelector('[data-os="clear"]').click();

  // 10. the rule: a compact two-option switch, the same policy data
  const radios = [...menu.querySelectorAll('.psRadio input[type=radio]')]; assert(!menu.querySelector('[data-ps="size"]') || menu.querySelector('[data-ps="size"]').hidden, 'no size while partial sheets are reused');
  radios[1].checked = true; radios[1].dispatchEvent(new w.Event('change', { bubbles: true })); await until(() => calls.some(c => c[0] === 'setPolicy'));
  assert.deepEqual(calls.find(c => c[0] === 'setPolicy'), ['setPolicy', metal, 'new', 100, 50]); assert(!menu.querySelector('[data-ps="size"]').hidden, 'width and height of the new sheet appear'); assert.match(menu.querySelector('[data-ps="newlabel"]').textContent, /Offer a brand new sheet at 100 × 50 mm/);
  await until(() => !menu.querySelector('[data-ps="polsave"]').disabled && /5–500|Saved/.test(menu.querySelector('[data-ps="polhelp"]').textContent)); const wIn = menu.querySelector('[data-ps="w"]'), hIn = menu.querySelector('[data-ps="h"]'); wIn.value = '120.5'; wIn.dispatchEvent(new w.Event('input', { bubbles: true })); hIn.value = '60'; hIn.dispatchEvent(new w.Event('input', { bubbles: true })); await until(() => !menu.querySelector('[data-ps="polsave"]').disabled); menu.querySelector('[data-ps="polsave"]').click(); await until(() => calls.filter(c => c[0] === 'setPolicy').length === 2);
  assert.deepEqual(calls.filter(c => c[0] === 'setPolicy')[1], ['setPolicy', metal, 'new', 120.5, 60]); await until(() => /120.5 × 60 mm/.test(menu.querySelector('[data-ps="newlabel"]').textContent));

  // 11. a toast sits under a modal window: what it says is said in the window's note too
  const t = doc.createElement('div'); t.className = 'toast bad'; t.dataset.msg = 'Use a width and height between 5 and 500 mm'; t.dataset.kind = 'bad'; doc.getElementById('toasts').appendChild(t);
  await until(() => /between 5 and 500 mm/.test(dlg.querySelector('.osNote').textContent)); assert(dlg.querySelector('.osNote').classList.contains('on'));

  // 12. Esc: the window closes (nothing else is open); the controls go home, hidden, without the source; the focus returns to the Options button
  allowance.value = '0.41'; allowance.dispatchEvent(new w.Event('input'));
  esc(dlg); await until(() => !doc.querySelector('dialog.osDlg'));
  assert(box.hidden && gateEl.contains(box) && !box.querySelector('.osSource') && [...box.children].length === 3 && box.firstElementChild === inc && inc.querySelector('[data-solid="include"]'), 'the controls are back in their card (the switch with them), without the source area'); assert(!inc.isConnected || box.contains(inc)); assert.equal(doc.activeElement, btn, 'the focus is back on the Options button');
  assert.equal(allowance.value, '0.41', 'what was typed is still there'); w.CN.renderCard(sh); assert.equal(allowance.value, '0.41', 'and a draw of the card keeps it'); assert(!w.Gate.state().optionsOpen[metal]);

  // 13. open again: one more list (a probe, never a timer); Use this one hands the pick to the engine and the window closes on the work; the close x and the backdrop close it
  btn.click(); await until(() => calls.filter(c => c[0] === 'searchAll').length === 2 && doc.querySelector('.ohc'));
  assert.equal(calls.filter(c => c[0] === 'list').length, 0, 'the window never read a partial list'); assert(doc.querySelectorAll('dialog.osDlg').length === 1);
  const res2 = doc.querySelector('[data-os="results"]'); res2.querySelector('.ohc[data-stock="stock-p-old"] [data-ps="pick"]').click(); await until(() => res2.querySelector('[data-ps="use"]'));
  res2.querySelector('[data-ps="use"]').click(); await until(() => calls.some(c => c[0] === 'seat')); assert.deepEqual(calls.find(c => c[0] === 'seat'), ['seat', 'p-old']); await until(() => !doc.querySelector('dialog.osDlg'), 'the window closes on the work');
  btn.click(); await until(() => doc.querySelector('.ohc')); doc.querySelector('[data-os="close"]').click(); await until(() => !doc.querySelector('dialog.osDlg')); assert.equal(doc.activeElement, btn);
  // a sheet drawn anew for another page puts the window away at once
  btn.click(); assert(doc.querySelector('dialog.osDlg')); gateEl._sheetOptionsOwner = null; w.CN.renderCard(sh); assert(!doc.querySelector('dialog.osDlg'), 'controls drawn anew: the window is put away');
  assert(!/setInterval/.test(modal) && !/setInterval/.test(pui), 'no timer anywhere');

  console.log('options-studio: OK');
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
