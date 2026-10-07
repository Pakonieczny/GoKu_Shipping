// GC1: the Nest-tab Cut Sheet flow (charm-nest-rose-ui.js, window.RoseStock) for 10K and 14K gold sheets, in jsdom, with the page's own Gate.
// Same harness as rose-ui.cjs. Needs jsdom (not installed everywhere): the test says so and passes nothing when it is missing.
//   node tests/charm-nest/gold-cut-sheet-ui.cjs
const assert = require('node:assert/strict'), fs = require('node:fs');
let JSDOM; try { ({ JSDOM } = require('jsdom')); } catch (_) { console.log('gold-cut-sheet-ui: SKIPPED (jsdom is not installed)'); process.exit(0); }
const R = require('../../charm-nest-rose'), O = require('../../charm-nest-orders');
const outline = { subpaths: [[['m', [0, 0]], ['l', [10, 0]], ['l', [10, 10]], ['l', [0, 10]], ['h']]] };
const wait = ms => new Promise(r => setTimeout(r, ms));

async function run(metal, word, code) {
  const dom = new JSDOM('<section id="sheet"><div class="shHead"></div><div class="shGate" data-r="gate"></div><div class="shPreviewWrap"></div></section>', { url: 'https://example.test', runScripts: 'outside-only' }), w = dom.window;
  w.IntersectionObserver = class { observe() { } unobserve() { } };
  w.CharmNestRose = R; w.CharmNestOrders = O; w.confirm = () => true;
  const calls = [], bodies = [], toasts = [], WPT = 130, HPT = 70;   // the sheet's own size: NOT 100 x 50
  const sh = { metal, page: 1, el: w.document.getElementById('sheet'), sheetId: metal + '-test', runId: 'run-test', charms: [{ id: 'charm', outline, centerPt: [5, 5], members: [] }], placements: [{ id: 'charm', cxPt: 10, cyPt: 10, angle: 0, scale: 1 }], persistedDone: true, verification: { ok: true }, status: 'complete', dirty: false, draft: true, outputs: {} };
  let stock = { id: 'rgs-physical', metal, wPt: WPT, hPt: HPT, revision: 0, profileJson: null }, saved, leftover = null;
  w.CN = { S: { cloud: { ok: true }, settings: { stock: {} }, mode: 'nest' }, esc: s => String(s).replace(/</g, '&lt;'), stockFor: () => ({ wPt: WPT, hPt: HPT, wIn: WPT / 72, hIn: HPT / 72 }), uid: () => 'test', allSheets: () => [sh], pagesOf: () => [sh], drawPreview() { }, renderCard: p => { w.Gate.renderCard(p); w.RoseStock?.render(p); }, toast(m, k) { toasts.push([String(m), k]); }, sheetDirty: p => { p.dirty = true; }, flushManualIntake() { },
    api: async (name, b) => { calls.push(b.op); bodies.push(b);
      if (b.op === 'roseClaim') { if (b.onlyRemnant && !leftover) return { stock: null, protectedJson: null }; stock = leftover || stock; return { stock }; }
      if (b.op === 'roseGet') return { stock, cuts: [], more: false };
      if (b.op === 'roseRelease') return { ok: true };
      if (b.op === 'rosePlan') { saved = R.plan(JSON.parse(b.shapesJson), WPT, HPT, null, b.allowanceMm); return { planJson: JSON.stringify(saved), planHash: 'hash' }; }
      if (b.op === 'roseRecordCut') return { stock: { ...stock, revision: 1, profileJson: JSON.stringify(saved.profile) }, cut: { sheetId: sh.sheetId, stockId: stock.id, revision: 1, at: 1760000000000, planJson: JSON.stringify(saved), planHash: 'hash', fileBase: code + ' sheet' } };
      return {}; } };
  w.sh = sh;
  w.eval(`var C=window.CN,S=C.S,CN=C,B=window.B={run:{runId:'run-test',releasePolicy:2,status:'running',solidIncluded:{}}};
 var O=window.CharmNestOrders,allSheets=()=>[sh],pagesOf=()=>[sh],stockFor=C.stockFor,esc=C.esc,labelOf=()=> '${word}';
 var Sets={ofRun:()=>[]},RunCtl={},Session=window.Session={schedule(){}},toast=()=>{},refreshAllCards=()=>{},api=C.api;
 var sheetDirty=()=>{},saveSettings=()=>{},startNest=()=>{};`);
  const src = fs.readFileSync('charm-nest-bridge.js', 'utf8'); w.eval(src.slice(src.indexOf('const Gate ='), src.indexOf('/* ═══ 21', src.indexOf('const Gate =')))); w.Gate.renderCard(sh);
  w.eval(fs.readFileSync('charm-nest-rose-ui.js', 'utf8'));
  const RS = w.RoseStock;
  assert(RS && w.CutLine === RS, 'RoseStock, and its alias CutLine, are on the page');
  // the card: Cut Sheet is there for this metal, the line's words say the metal
  assert(sh.el.querySelector('[data-rose="cut"]'), metal + ': the sheet card has its Cut Sheet button');
  assert.equal(calls.length, 0, 'rendering never loads or claims physical stock');
  // a gold sheet nobody asked a line for holds no physical sheet and saves no contour
  await RS.plan(sh); assert(!sh.rosePlan && !sh.roseStock && calls.length === 0, 'no claim and no line without Cut Sheet');
  // nesting: no leftover of this metal and size fits, so nothing is claimed (the size stays the person's); the claim asks for the leftover only
  await RS.nestClaim(sh); assert(!sh.roseStock, 'no leftover: no physical sheet held');
  const asked = bodies.find(b => b.op === 'roseClaim'); assert.deepEqual([asked.metal, asked.wPt, asked.hPt, asked.onlyRemnant], [metal, WPT, HPT, true], 'the claim carries the metal and the sheet\'s own size');
  // a leftover of this metal and size fits: it is claimed, and a plan then follows its profile
  leftover = { id: 'rgs-left', metal, wPt: WPT, hPt: HPT, revision: 2, profileJson: JSON.stringify(R.plan([{ id: 'old', paths: [[[2, 2], [12, 2], [12, 40], [2, 40]]] }], WPT, HPT, null, .2).profile) };
  await RS.nestClaim(sh); assert.equal(sh.roseStock && sh.roseStock.id, 'rgs-left', 'a leftover that fits is taken at nest'); assert.equal(sh.roseRevision, 2);
  // let it go again for the rest of this test (the sheet is back to holding nothing)
  sh.roseStock = null; sh._roseLoaded = false; sh.roseHistory = []; leftover = null; calls.length = 0; bodies.length = 0;
  // Cut Sheet on a held sheet: includes the sheet by its own Include first (Gate.cutInclude), draws the line, records the cut
  const included = []; w.Gate.cutInclude = async s => { included.push(s.metal); throw new Error('Not cut: ' + s.sheetId + ' shares an order with Sheet 2, which is not in the set'); };
  w.CN.renderCard(sh); const press = sh.el.querySelector('[data-rose="cut"]'); assert(!press.disabled, 'a held sheet can be cut'); press.click();
  for (let n = 0; n < 50 && !sh._roseError; n++) await wait(5);
  assert.match(sh._roseError, /^Not cut: /); assert.deepEqual(included, [metal]); assert(!sh.roseCutAt && !calls.includes('roseRecordCut') && !calls.includes('roseClaim'), 'a refused Include claims nothing and cuts nothing'); sh._roseError = null;
  w.Gate.cutInclude = async s => { included.push(s.metal); s.draft = false; s.setId = 'set-test'; };
  await RS.record(sh);
  assert.deepEqual(included, [metal, metal], 'Cut Sheet includes the sheet through its own Include, not the Rose Gold metal switch');
  assert(sh.roseCutAt, 'the cut is recorded'); assert.equal(sh.roseHistory.length, 1);
  const claim = bodies.find(b => b.op === 'roseClaim'), plan = bodies.find(b => b.op === 'rosePlan'), rec = bodies.find(b => b.op === 'roseRecordCut');
  assert.deepEqual([claim.metal, claim.wPt, claim.hPt, !!claim.onlyRemnant], [metal, WPT, HPT, false], 'Cut Sheet claims the sheet\'s own size');
  assert.equal(plan.metal, metal); assert.equal(rec.metal, metal); assert.equal(rec.via, 'nest', 'the cut says it was pressed in the Nest tab');
  assert(toasts.some(([m]) => m.includes(word + ' charms nest past this green line')), 'the toast names the metal: ' + JSON.stringify(toasts));
  assert(sh.el.querySelectorAll('.roseLineTimeline time').length === 1, 'one dated line in the timeline above the sheet');
  assert(sh.el.querySelector('[data-solid="include"]').disabled, 'a cut sheet stays in its set');
  // the dash contour paints (this paint threw "Cannot access 'cuts' before initialization" once: it is checked for every metal)
  const ctx = new Proxy({ calls: [] }, { get(o, k) { if (k in o) return o[k]; return (...args) => o.calls.push([k, ...args]); }, set(o, k, v) { o[k] = v; return true; } });
  RS.paint(ctx, sh, 2, 'history'); assert(ctx.calls.some(c => c[0] === 'fillRect'), 'the removed region is shaded');
  RS.paint(ctx, sh, 2, 'lines'); assert(ctx.calls.some(c => c[0] === 'stroke'), 'the green line is drawn');
  // a sheet of a metal with no green line draws, shows and asks nothing
  const gf = { ...sh, metal: 'gold', roseCutAt: null, roseStock: null, rosePlan: null };
  const before = ctx.calls.length; RS.paint(ctx, gf, 2, 'lines'); assert.equal(ctx.calls.length, before, 'GF paints no green line');
  // waiting: a gold sheet in the set that holds a physical sheet and has charms past its last line makes its set wait, in this metal's words
  const held = { ...sh, roseCutAt: null, roseStock: { id: 'rgs-x', metal, revision: 0 }, rosePlan: null, rosePlanHash: null, roseProtected: null, draft: false, setId: 'set-test', placements: [{ id: 'a', cxPt: 20, cyPt: 20, angle: 0, scale: 1 }], charms: [{ id: 'a', outline, centerPt: [5, 5], members: [] }] };
  assert.equal(RS.waiting(held), 1); assert.equal(RS.waitWords(held), `${word} Sheet 1 has 1 charm not cut yet: press Cut Sheet`);
  assert.equal(RS.waiting({ ...held, roseStock: null }), 0, 'a sheet that holds no physical sheet does not make its set wait');
  assert.equal(RS.waiting({ ...held, metal: 'gold' }), 0, 'GF never waits');
  // letting go: out of the set before any line, a fresh physical sheet is released (the server deletes it), the size is free again
  const fresh = { ...sh, roseCutAt: null, roseHistory: [], roseStock: { id: 'rgs-fresh', metal, revision: 0, profileJson: null }, rosePlan: null, rosePlanHash: null, roseProtected: null, draft: true, setId: null };
  calls.length = 0; bodies.length = 0;
  assert.equal(await RS.letGo(fresh), true); assert(!fresh.roseStock && calls[0] === 'roseRelease' && bodies[0].metal === metal, 'let go: released with its metal');
  const lefty = { ...fresh, roseStock: { id: 'rgs-left', metal, revision: 2, profileJson: '{}' } }; calls.length = 0;
  assert.equal(await RS.letGo(lefty), false); assert.equal(calls.length, 0, 'a leftover the sheet was nested on stays reserved');
  const lined = { ...fresh, rosePlan: { lines: [[[0, 0], [1, 1]]] } }; assert.equal(await RS.letGo(lined), false, 'a planned sheet never lets go');
  dom.window.close();
}
(async () => {
  await run('gold10k', '10K Gold', '10K');
  await run('gold14k', '14K Gold', '14K');
  console.log('gold-cut-sheet-ui: ok (Cut Sheet button, lazy claim at its own size, Include by its own switch then cut, dated line, paint, waiting words, let go; 10K and 14K)');
})().catch(e => { console.error(e); process.exit(1); });
