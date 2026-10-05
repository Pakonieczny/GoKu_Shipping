// The piece dots of an order row (Paul, 5 Oct 2026, 13:21 and 13:22 UTC, round 15): "The colours of the hollow circles representing pieces that have not yet been placed on a
// sheet should be showing the same colour as the sheet they belong to. This should happen to all sheets, and the users should be able to hover over each one of those dots
// filled in and hollow, and have a small pop-up show up to show the thumbnail of the vector design for that particular piece. This needs to be instantaneous with no delay and
// very fast and ready to load on hover state." · "Also clicking on any of the dots both solid and hollow will automatically open the detailed order view with that particular
// piece pre-selected so that the user doesn't have to look for it again when the modal opens."
// Browser test on the app's own shell and CSS (charm-nest-1.html with its scripts taken out) around the REAL LaserReview rail, the real '!' panel (charm-nest-library-issues.js),
// the real dot component (charm-nest-piece-dots.js) and the real OrderPieces; the vector renderer is a stand-in with a log and a delay (offline, no network, no live data).
// Part 2 runs the whole page on the fake site (bridge-server.cjs): the REAL order window opens on the piece a dot stands for, and the card shows the page's own ListMedia rendering.
// What it holds to, at 1440 and 390 px:
//  · every hollow ring is the colour of the sheet its piece belongs to, for GF, RG, SS, 10K and 14K: a piece of a known metal in its metal's colour (also when the list is another
//    metal's), a piece on another sheet in that sheet's colour, an unknown piece (Unknown SKU) in the colour of the sheet whose list it is under (RG: rose); in a list about no sheet an
//    unknown piece keeps a calm neutral ring; never the issue red; the filled dots keep their sheet colours; the labels say how many pieces are not on a sheet yet;
//  · every dot, filled and hollow, shows its card on hover, on keyboard focus and on a touch press; the card is on screen in the very event (no timer, no animation frame, no fade),
//    well under 50 ms warm; the vector designs were made before anyone hovered (idle slices, visible rows first), so a warm hover never shows a spinner; a cold one shows the labelled
//    spinner at once and swaps to the picture the moment it is ready; a piece with no design says one calm line at once; moving dot to dot swaps with no frame without the card;
//  · the card is one element for every dot, never clickable, over the issues panel (also inside an open dialog), inside the screen; it hides at once on leave, blur, Esc and a scroll;
//    a redraw that replaces the hovered dot keeps the card; there is no listener on a dot, none grows with redraws, the cache is bounded (about 300) and 200 rows are cheap;
//  · a press on a dot (mouse, keyboard, touch) opens the order with exactly that piece (its pool id, its line's key, "pick"), from the issues panel as a row's press does (the panel
//    steps aside and is back when the window closes); a press on the row outside the dots still opens the order as before; the card is gone with the press;
//  · mutants: a red ring, a hover delay, a card that is not on the screen in the pointer's event, and a dot that opens the wrong piece are each caught.
//   node tests/charm-nest/piece-dot-hover.cjs [playwright dir]     (PW_DIR=…, CHROMIUM=…; SHOTS=<dir> saves screenshots; PD_FULL=0 skips part 2)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const F = require('./library-issues-fixture.cjs');
const root = path.join(__dirname, '../..');
let chromium;
const dirs = [process.argv[2], process.env.PW_DIR, '/opt/node22/lib/node_modules/playwright', path.join(root, 'node_modules/playwright'), path.join(root, 'node_modules/playwright-core')].filter(Boolean);
for (const d of dirs) { for (const n of [d, path.join(d, 'playwright-core'), path.join(d, 'playwright')]) { try { ({ chromium } = require(n)); if (chromium) break; } catch (_) {} } if (chromium) break; }
if (!chromium) { console.log('  - no playwright: the browser check was not run'); process.exit(0); }
const SHOTS = process.env.SHOTS || '';
const CHROME = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
const sleep = ms => new Promise(r => setTimeout(r, ms));
const read = f => F.read(f);

/* ═══ the world: five sheets (GF, RG, SS, 10K, 14K), each with the same five orders ═══ */
const SHEETS = [
  { id: 'gf1', metal: 'gold', code: 'GF', base: 4170000000, other: { code: 'SS', metal: 'silver' }, off: 'rose' },
  { id: 'rg1', metal: 'rose', code: 'RG', base: 4171000000, other: { code: 'GF', metal: 'gold' }, off: 'gold' },
  { id: 'ss1', metal: 'silver', code: 'SS', base: 4172000000, other: { code: 'RG', metal: 'rose' }, off: 'gold' },
  { id: 'k10', metal: 'gold10k', code: '10K', base: 4173000000, other: { code: 'GF', metal: 'gold' }, off: 'silver' },
  { id: 'k14', metal: 'gold14k', code: '14K', base: 4174000000, other: { code: 'SS', metal: 'silver' }, off: 'rose' }
];
const TOKEN = { gold: '--m-gold', silver: '--m-silver', rose: '--m-rose', gold10k: '--m-gold10k', gold14k: '--m-gold14k' };
const NAMES = ['Brooke Berkenpas', 'Carolyn Schmidt', 'Leslie Suhr', 'Ava Patel', 'Cora Lind'];
// order shapes (n pieces; on: the pieces on this sheet; each other piece: [kind, sku, material | 'sheet:<code>:<metal>'])
//   P  Paul's second order: six pieces, two on the sheet, four with an Unknown SKU           Q  two pieces, one Unknown SKU (his first order)
//   R  three pieces: one on the sheet, one not placed (a known SKU, ANOTHER metal), one Unknown SKU       S  two pieces, the other on a sheet of another metal that is not ready
//   T  one line of two copies: copy 1 on the sheet, copy 2 not placed
function world(sh, k = 0) {
  const b = sh.base + k * 100, rid = n => String(b + n), rows = [], blocks = {}, on = [], reps = {}, cust = {};
  const row = (r, i, o) => rows.push({ key: `${r}_${i}`, order: { receiptId: r, buyer: { name: cust[r] } }, line: { transactionId: String(i), listingId: 'L' + (+r % 97), sku: o.sku, title: o.sku + ' charm', quantity: o.qty || 1 },
    spec: { designSku: o.sku, size: '', material: o.material || null, quantity: o.qty || 1, noDesign: false }, state: o.state || 'pooled', material: o.material || null, poolIds: o.onSheet ? [`${r}_${i}_1`] : [], problems: [] });
  const add = (n, name, pieces) => {
    const r = rid(n); cust[r] = name; const bl = [];
    pieces.forEach((p, j) => {
      const i = j + 1, pool = `${r}_${i}_1`;
      if (p.onSheet) on.push(pool);
      row(r, i, p);
      if (!p.onSheet) bl.push({ key: p.kind, index: i, label: p.sku, poolId: pool, lineKey: `${r}_${i}`, sheetId: p.sheet ? 'other-' + p.sheet.code : null, sheetLabel: p.sheet ? `${p.sheet.code} Sheet 1` : null, ...(p.sheet ? { stage: 'approval' } : {}), why: p.kind });
    });
    reps[r] = { ready: false, key: bl[0].key, why: bl[0].key, blocks: bl, onSheets: [sh.id], pieceCount: pieces.length, customer: name, listingId: 'L' + (+r % 97) };
    return r;
  };
  const mine = { sku: 'AAA-' + sh.code, material: sh.metal, onSheet: true }, unk = i => ({ sku: 'ZZ-UNKNOWN-' + i, kind: 'unmatched', state: 'unmatched' });
  const P = add(1, NAMES[1], [mine, { ...mine }, unk(1), unk(2), unk(3), unk(4)]);
  const Q = add(2, NAMES[0], [mine, unk(5)]);
  const R = add(3, NAMES[2], [mine, { sku: 'BBB-' + sh.code, kind: 'pooled', material: sh.off }, unk(6)]);
  const S = add(4, NAMES[3], [mine, { sku: 'CCC-' + sh.code, kind: 'otherSheetNotReady', sheet: sh.other, material: sh.other.metal, onSheet: false }]);
  // T: one line, two copies
  const T = rid(5); cust[T] = NAMES[4]; on.push(`${T}_1_1`);
  row(T, 1, { sku: 'DDD-' + sh.code, material: sh.metal, qty: 2, onSheet: true });
  reps[T] = { ready: false, key: 'pooled', why: 'pooled', blocks: [{ key: 'pooled', index: 2, label: 'DDD', poolId: `${T}_1_2`, lineKey: `${T}_1`, sheetId: null, sheetLabel: null, why: 'pooled' }], onSheets: [sh.id], pieceCount: 2, customer: NAMES[4], listingId: 'L' + (+T % 97) };
  const rec = F.sheet(sh.id, { n: 1, metal: sh.metal, base: b + 90, seq: 1, index: 1 });
  rec.poolIds = on; rec.orders = Object.keys(reps); rec.placedCount = rec.charmCount = on.length; rec.orderReadiness = reps; rec.backPool = on.map(p => F.back(p, sh.id)); rec.label = { files: [{ path: 'qr.png', url: 'https://example.com/qr.png', payload: 'x', orders: rec.orders }] };
  rec.folder = `${sh.code}_Oct.03.26_Set-1_Sheet-1`;
  return { rec, rows, ids: { P, Q, R, S, T } };
}

/* the page: the shell, the real Library code, the real panel and dots, the real OrderPieces; a stand-in renderer for vector designs */
const VEC_STUBS = `
window.__vec = { calls: [], delay: {}, all: 0, none: new Set(), fail: new Set() };
const svg = sku => 'data:image/svg+xml;utf8,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="220" height="220" viewBox="0 0 220 220"><rect width="220" height="220" fill="#fff"/><circle cx="110" cy="40" r="14" fill="none" stroke="#222" stroke-width="3"/><path d="M110 56c-40 0-62 26-62 62 0 38 28 60 62 60s62-22 62-60c0-36-22-62-62-62z" fill="none" stroke="#2540e0" stroke-width="3"/><text x="110" y="124" text-anchor="middle" font-size="22" fill="#222">' + sku + '</text></svg>');
Object.assign(window.ListMedia, {
  vectorKey: row => { if (!row || (row.spec && row.spec.noDesign)) return ''; const sku = String((row.spec && row.spec.designSku) || (row.line && row.line.sku) || '').toUpperCase(); return sku ? 'sku:' + sku + '|' + ((row.spec && row.spec.size) || '') : ''; },
  vectorThumb: async row => { const sku = (row.spec && row.spec.designSku) || (row.line && row.line.sku) || ''; window.__vec.calls.push(sku); const d = window.__vec.delay[sku] || window.__vec.all || 0; if (d) await new Promise(r => setTimeout(r, d)); if (window.__vec.none.has(sku)) return null; if (window.__vec.fail && window.__vec.fail.has(sku)) throw new Error('No design file'); return svg(sku); }
});`;
async function openPage(browser, { width = 1440, height = 900, touch = false, errors = [], mutate = null, rows = null } = {}) {
  const { page, context } = await F.openPage(browser, { width, height, fake: false, errors, touch });
  void mutate;
  page.setDefaultTimeout(8000);
  return { page, context };
}
// (a mutant is a source file read with one change: the page loads that instead of the real one)
async function openMutant(browser, file, from, to, opts = {}) {
  const src = read(file); assert(src.includes(from), `the mutant's anchor is in ${file}: ${from.slice(0, 50)}`);
  const errors = [], { page, context } = await F.openPage(browser, { width: opts.width || 1440, height: 900, fake: false, errors, source: f => f === file ? src.replace(from, to) : read(f) });
  return { page, context, errors };
}
async function build(page, { sheets = SHEETS, extra = null } = {}) {
  const worlds = sheets.map(s => world(s));
  await page.addScriptTag({ content: VEC_STUBS });   // (ListMedia is the fixture's stub: peek and listing; the vector pair is added to it)
  await page.addScriptTag({ content: read('charm-nest-order-pieces.js') });
  await page.evaluate(({ worlds, picture }) => {
    window.CN = { S: window.S };
    window.__sheets = worlds.map(w => w.rec); window.__rows = worlds.flatMap(w => w.rows); for (const s of window.__sheets) s.preview = picture;
    window.S.library.rows = window.__sheets; window.S.library.loadedAt = Date.now();
    const L = window.LaserReview, body = document.getElementById('libBody'); L.sections(body); for (const s of window.__sheets) L.record(s);
    const st = { setId: 'set1', seq: 1, name: 'Set 1', day: '2026-10-03', sheetIds: window.__sheets.map(s => s.id), status: 'open' };
    window.__sets = [st]; const card = window.Sets.libraryCard(st, window.__sheets, window.__sheets); L.place(card, L.group(st, window.__sheets).ready, body); L.changed();
    window.__calls.length = 0;
    window.openOrderFrom = (btn, rid, o) => { window.__calls.push(['order', rid, (o && o.poolId) || null, JSON.stringify(o || {})]); return window.__openWindow ? window.__openWindow() : true; };
  }, { worlds: worlds.map(w => ({ rec: w.rec, rows: w.rows })), picture: F.sheetPicture() });
  await page.waitForSelector('.flowBox'); await sleep(450);
  return worlds;
}
const bang = (id, step = 'orders') => `[data-issues-open][data-issues-id="${id}"][data-issues-step="${step}"]`;
async function openPanel(page, id) {
  await page.evaluate(sel => document.querySelector(sel).scrollIntoView({ block: 'center' }), bang(id)); await sleep(150);
  await page.click(bang(id)); await page.waitForSelector('.lisPanel .pdot'); await sleep(350);
}
const colour = (page, token) => page.evaluate(v => { const e = document.createElement('i'); e.style.background = `var(${v})`; document.body.appendChild(e); const c = getComputedStyle(e).backgroundColor; e.remove(); return c; }, token);
// what the open panel draws: per order row, its dots (computed colours, what each stands for)
const dotsOf = page => page.evaluate(() => [...document.querySelectorAll('.lisPanel .lisRow[data-issue-order]')].map(r => ({ order: r.dataset.issueOrder, chip: (r.querySelector('.lisChip') || {}).textContent || '', group: (r.querySelector('.pdots') || {}).getAttribute && r.querySelector('.pdots').getAttribute('aria-label'),
  dots: [...r.querySelectorAll('.pdot')].map(d => { const cs = getComputedStyle(d); return { ring: d.classList.contains('ring'), m: d.dataset.m || '', bg: cs.backgroundColor, border: cs.borderTopColor, bw: cs.borderTopWidth, pool: d.dataset.pdP, line: d.dataset.pdL, n: +d.dataset.pdN, none: d.dataset.pdNone === '1', label: d.getAttribute('aria-label'), key: d.dataset.pk }; }) })));
const centreOf = async (page, sel) => { const b = await (await page.$(sel)).boundingBox(); return { x: b.x + b.width / 2, y: b.y + b.height / 2, b }; };
// the card as it is drawn
const tipNow = page => page.evaluate(() => {
  const t = window.PieceDots.tip(); if (!t) return null; const cs = getComputedStyle(t), r = t.getBoundingClientRect();
  return { on: t.hasAttribute('data-on'), visible: cs.visibility === 'visible', opacity: +cs.opacity, pe: cs.pointerEvents, l: r.left, t: r.top, r: r.right, b: r.bottom, w: r.width, h: r.height, z: +cs.zIndex || 0, parent: t.parentNode.nodeName + (t.parentNode.id ? '#' + t.parentNode.id : ''), img: !!t.querySelector('img'), src: t.querySelector('img') ? decodeURIComponent(t.querySelector('img').src) : '', imgOk: !!(t.querySelector('img') && t.querySelector('img').complete && t.querySelector('img').naturalWidth), spin: !!t.querySelector('.pdSpin'), spinText: (t.querySelector('.pdWait') || {}).textContent || '', text: t.textContent.replace(/\s+/g, ' ').trim(), below: t.classList.contains('below'), textCard: t.classList.contains('text'), vw: innerWidth, vh: innerHeight, shownFor: (window.PieceDots.current() || {}).dataset ? window.PieceDots.current().dataset.pk : '' };
});
const shot = async (page, name, dotSel, w = 380) => {
  if (!SHOTS) return; fs.mkdirSync(SHOTS, { recursive: true });
  const t0 = await tipNow(page), p = await page.evaluate(() => { const r = document.querySelector('.lisPanel').getBoundingClientRect(); return { l: r.left, t: r.top, r: r.right, b: r.bottom, vw: innerWidth, vh: innerHeight }; }), vw = p.vw, vh = p.vh;
  const t = t0 && t0.on ? t0 : { l: p.l, t: p.t, r: p.r, b: p.b };   // (the card too when it is showing)
  const top = Math.max(0, Math.min(t.t, p.t) - 10), left = Math.max(0, Math.min(t.l, p.l) - 14), right = Math.min(vw, Math.max(t.r, p.r) + 14), bottom = Math.min(vh, Math.max(t.b, p.b) + 14);
  await page.screenshot({ path: path.join(SHOTS, name + '.png'), clip: { x: left, y: top, width: Math.min(Math.max(w, right - left), vw - left), height: bottom - top } });
};
// frames: the card's state at every animation frame, from now until stopped (to prove a gap-free swap and a stay through a redraw)
const record = page => page.evaluate(() => { window.__fr = []; window.__rec = true; const tick = () => { if (!window.__rec) return; const t = window.PieceDots.tip(); window.__fr.push([t && t.hasAttribute('data-on') && getComputedStyle(t).visibility === 'visible' ? 1 : 0, (window.PieceDots.current() || { dataset: {} }).dataset.pk || '']); requestAnimationFrame(tick); }; requestAnimationFrame(tick); });
const stopRecord = page => page.evaluate(() => { window.__rec = false; return window.__fr; });
async function away(page, ms = 120) { await page.mouse.move(3, 3, { steps: 2 }); await sleep(ms); }

// the checks a mutant must break
async function colourFailures(page) {
  const out = [], red = await colour(page, '--clay'), rows = await dotsOf(page), ok = Object.values(TOKEN), toks = {};
  for (const m of Object.values(TOKEN)) toks[m] = await colour(page, m);
  for (const r of rows) for (const d of r.dots) if (d.ring && (d.border === red || !Object.values(toks).includes(d.border))) out.push(`order ${r.order}: a ring is ${d.border}, not a sheet colour`);
  void ok; return out;
}
async function instantFailures(page) {
  const out = [];
  const r = await page.evaluate(() => {
    const dot = document.querySelector('.lisPanel .pdot:not(.ring)'), seen = { timeouts: 0, intervals: 0, raf: 0 };
    const st = window.setTimeout, si = window.setInterval, rf = window.requestAnimationFrame, ri = window.requestIdleCallback;
    window.setTimeout = (...a) => { seen.timeouts++; return st(...a); }; window.setInterval = (...a) => { seen.intervals++; return si(...a); }; window.requestAnimationFrame = (...a) => { seen.raf++; return rf(...a); };
    let out;
    try {
      dot.dispatchEvent(new PointerEvent('pointerover', { bubbles: true, pointerType: 'mouse', clientX: dot.getBoundingClientRect().left + 5, clientY: dot.getBoundingClientRect().top + 5 }));
      const t = window.PieceDots.tip(), cs = t && getComputedStyle(t), b = t && t.getBoundingClientRect();
      out = { on: !!t && t.hasAttribute('data-on'), visible: !!t && cs.visibility === 'visible', opacity: t ? +cs.opacity : 0, w: b ? b.width : 0, h: b ? b.height : 0, seen };
    } finally { window.setTimeout = st; window.setInterval = si; window.requestAnimationFrame = rf; void ri; }
    return out;
  });
  if (!r.on || !r.visible) out.push('the card is not on screen in the pointer\'s own event');
  if (r.opacity < 0.99) out.push(`the card fades in (opacity ${r.opacity} in the first frame)`);
  if (r.seen.timeouts || r.seen.intervals || r.seen.raf) out.push(`the hover waits for a timer or a frame: ${JSON.stringify(r.seen)}`);
  await page.evaluate(() => window.PieceDots.hide());
  return out;
}

/* ═══ hover suites ═══ */
const RGGF = SHEETS.filter(s => s.id === 'rg1' || s.id === 'gf1'), RGONLY = SHEETS.filter(s => s.id === 'rg1');
// the design each dot of rg1's orders stands for (null: an Unknown SKU piece, no design at all)
const DESIGN = { P: ['AAA', 'AAA', null, null, null, null], Q: ['AAA', null], R: ['AAA', 'BBB', null], S: ['AAA', 'CCC'], T: ['DDD', 'DDD'] };
async function fresh(browser, width, { touch = false, sheets = RGGF, vec = null } = {}) {
  const errors = [], { page, context } = await openPage(browser, { width, height: width === 390 ? 844 : 900, errors, touch });
  if (width < 600) await page.evaluate(() => document.getElementById('app').classList.add('railOff'));
  const worlds = await build(page, { sheets });
  if (vec) await page.evaluate(v => { Object.assign(window.__vec.delay, v.delay || {}); for (const s of v.none || []) window.__vec.none.add(s); }, vec);
  return { page, context, errors, worlds, id: Object.fromEntries(sheets.map((s, i) => [s.id, worlds[i].ids])) };
}
const dotSel = (order, n) => `.lisPanel .pdots[data-pd-order="${order}"] .pdot:nth-child(${n})`;
const orderKeyOf = (id, rid) => Object.entries(id).find(([, v]) => v === rid)[0];
async function toDot(page, sel) { await page.$eval(sel, e => e.scrollIntoView({ block: 'nearest' })); const c = await centreOf(page, sel); await page.mouse.move(c.x, c.y); return c; }
const calls = page => page.evaluate(() => window.__vec.calls.slice());
const settleQueue = (page, ms = 6000) => page.waitForFunction(() => { const s = PieceDots.stats(); return !s.queued && !s.running; }, null, { timeout: ms });

async function hoverSuite(browser, width) {
  const tag = `${width}px`, { page, context, errors, id } = await fresh(browser, width), ids = id.rg1;
  assert.deepEqual(await calls(page), [], `${tag}: nothing is rendered before a list with dots is drawn`);
  await page.evaluate(() => { window.__idle = 0; const f = window.requestIdleCallback; window.requestIdleCallback = (...a) => { window.__idle++; return f.apply(window, a); }; });
  await openPanel(page, 'rg1'); await settleQueue(page);
  // ── ready before anyone hovers: the idle slices made every distinct design of the list once; Unknown SKU pieces were never rendered
  const made = await calls(page);
  assert.deepEqual([...made].sort(), ['AAA-RG', 'BBB-RG', 'CCC-RG', 'DDD-RG'], `${tag}: every design of the list was made once before any hover (${made})`);
  assert((await page.evaluate(() => window.__idle)) > 0, `${tag}: the prefetch runs in idle slices (requestIdleCallback)`);
  assert.equal(await page.evaluate(() => PieceDots.stats().ready), 4);
  // ── every dot, filled and hollow, shows its card in the pointer's own event, with its own design, never a spinner, never a second render
  await page.evaluate(() => { window.__hv = []; window.addEventListener('pointerover', () => { window.__t0 = performance.now(); }, true); document.addEventListener('pointerover', ev => { const d = ev.target; if (!d.classList || !d.classList.contains('pdot')) return; const t = PieceDots.tip(); window.__hv.push({ pk: d.dataset.pk, on: !!t && t.hasAttribute('data-on') && getComputedStyle(t).visibility === 'visible', lat: performance.now() - window.__t0, lag: performance.now() - ev.timeStamp, cur: (PieceDots.current() || { dataset: {} }).dataset.pk }); }, true); });   // (window's capture runs first, document's after the card's own listener: lat is what the card cost inside the event; lag also holds the browser's own input queue)
  const rows = await dotsOf(page); let n = 0;
  for (const r of rows) {
    const want = DESIGN[orderKeyOf(ids, r.order)];
    for (const [i, d] of r.dots.entries()) {
      const sel = dotSel(r.order, i + 1); await away(page, 15); const c = await toDot(page, sel), t = await tipNow(page); n++;
      assert(t && t.on && t.visible, `${tag}: order ${r.order} dot ${i + 1} (${d.ring ? 'hollow' : 'filled'}) shows its card`);
      assert.equal(t.shownFor, d.key, 'for that very dot'); assert.equal(t.pe, 'none', 'the card takes no press'); assert.equal(t.parent, 'BODY');
      if (want[i]) { assert(t.img && t.imgOk && !t.spin, `${tag}: ${r.order}#${i + 1} shows the picture at once (no spinner): ${t.text}`); assert(t.src.includes(want[i] + '-RG'), `${tag}: ${r.order}#${i + 1} is the ${want[i]} design (${t.src.slice(-80)})`); assert(t.w >= 100 && t.w <= 140 && t.h >= 100 && t.h <= 140, `a small card, the picture 96-120 px (${t.w}x${t.h})`); assert.equal(t.text, '', 'no caption, no icons'); }
      else { assert(!t.img && !t.spin && t.textCard, `${tag}: ${r.order}#${i + 1} is an Unknown SKU piece: one calm line, no picture, no spinner`); assert.equal(t.text, 'No vector design for this piece yet'); }
      assert(t.l >= 0 && t.r <= t.vw && t.t >= 0 && t.b <= t.vh, `${tag}: the card is inside the screen (${t.l},${t.t},${t.r},${t.b})`);
      const db = await (await page.$(sel)).boundingBox(); assert(t.b <= db.y + 1 || t.t >= db.y + db.height - 1, 'the card never covers its own dot');
      assert.deepEqual(c.b.width > 0, true);
    }
  }
  const hv = await page.evaluate(() => window.__hv);
  assert(hv.length >= n && hv.every(h => h.on && h.cur === h.pk), `${tag}: the card was on screen, for the right dot, in the pointer's own event for all ${n} dots`);
  const worst = Math.max(...hv.map(h => h.lat)), lag = Math.max(...hv.map(h => h.lag)); assert(worst < 25, `${tag}: the card is drawn in the event in well under 50 ms warm (worst ${worst.toFixed(1)} ms inside the event)`); assert(lag < 250, `${tag}: and nothing waits on a timer (event to card, with the browser's own input queue: ${lag.toFixed(0)} ms)`); console.log(`    ${tag}: ${n} dots, worst card time inside the event ${worst.toFixed(1)} ms (event to card ${lag.toFixed(0)} ms)`);
  assert.deepEqual(await calls(page), made, `${tag}: hovering rendered nothing (all ready before the hover)`);
  assert.deepEqual(await instantFailures(page), [], `${tag}: instantFailures`);
  // ── a comfortable hit area: 4 px above the dot still counts, 8 px does not
  { const sel = dotSel(ids.P, 3), r = await page.evaluate(s => { const d = document.querySelector(s), b = d.getBoundingClientRect(), x = (b.left + b.right) / 2, up = (dy) => { const e = document.elementFromPoint(x, b.top - dy); return !!e && e === d; }; return { in4: up(4), in8: up(8), side: document.elementFromPoint(b.left - 1.2, (b.top + b.bottom) / 2) === d, h: b.height }; }, sel);
    assert(r.in4 && !r.in8 && r.side, `${tag}: the hit area reaches 4 px past the dot (${JSON.stringify(r)})`); }
  // ── the card: one element, over the panel (z), pointer-events none, on screen; the page holds exactly one
  { await away(page); await toDot(page, dotSel(ids.R, 2)); const t = await tipNow(page), z = await page.evaluate(() => +getComputedStyle(document.querySelector('.lisPanel')).zIndex);
    assert(t.z > z, `${tag}: the card (${t.z}) is above the panel (${z})`);
    assert.equal(await page.evaluate(() => document.querySelectorAll('.pdTip').length), 1, 'one card for every dot');
    const top = await page.evaluate(() => { const t = PieceDots.tip(); t.style.pointerEvents = 'auto'; const r = t.getBoundingClientRect(), e = document.elementFromPoint((r.left + r.right) / 2, (r.top + r.bottom) / 2); const ok = !!e && !!e.closest('.pdTip'); t.style.pointerEvents = ''; return ok; });
    assert(top, `${tag}: nothing covers the card (it is not clipped by the panel)`); await shot(page, `card-hollow-RG-${width}`); }
  // ── dot to dot: no frame without the card; the card swaps its picture in place
  { await away(page); await toDot(page, dotSel(ids.P, 1)); await record(page); const a = await (await page.$(dotSel(ids.P, 1))).boundingBox(), b = await (await page.$(dotSel(ids.P, 6))).boundingBox();
    await page.mouse.move(a.x + a.width / 2, a.y + a.height / 2); await page.mouse.move(b.x + b.width / 2, b.y + b.height / 2, { steps: 24 }); await sleep(60);
    const fr = await stopRecord(page); assert(fr.length >= 3 && fr.every(f => f[0] === 1), `${tag}: moving along the dots never leaves a frame without the card (${fr.map(f => f[0]).join('')})`);
    assert.equal(new Set(fr.map(f => f[1])).size, 6, `${tag}: it passed over all six dots`); assert.equal(fr[fr.length - 1][1], `${ids.P}|${ids.P}_6_1`); }
  // ── hides at once: leave, blur, Esc (the key goes on to the panel), a scroll
  { await toDot(page, dotSel(ids.R, 1)); assert((await tipNow(page)).on); await page.mouse.move(3, 3); assert.equal((await tipNow(page)).on, false, `${tag}: leaving hides the card in the same event`);
    await page.evaluate(s => document.querySelector(s).focus(), dotSel(ids.R, 2)); let t = await tipNow(page); assert(t.on && t.shownFor === `${ids.R}|${ids.R}_2_1`, `${tag}: keyboard focus shows it`);
    await page.keyboard.press('ArrowRight'); t = await tipNow(page); assert(t.on && t.shownFor === `${ids.R}|${ids.R}_3_1`, `${tag}: the arrow moves to the next dot and the card with it`); await page.keyboard.press('ArrowLeft'); await page.keyboard.press('ArrowLeft'); assert.equal((await tipNow(page)).shownFor, `${ids.R}|${ids.R}_1_1`);
    assert.equal(await page.evaluate(() => [...document.querySelectorAll('.lisPanel .pdot')].every(d => d.tabIndex === -1)), true, 'dots are not Tab stops (sixty orders are not three hundred stops)');
    await page.evaluate(() => document.activeElement.blur()); assert.equal((await tipNow(page)).on, false, `${tag}: blur hides it`);
    await toDot(page, dotSel(ids.R, 2)); assert((await tipNow(page)).on);
    await page.evaluate(() => { const b = document.querySelector('.lisBody'); b.dispatchEvent(new Event('scroll')); }); assert.equal((await tipNow(page)).on, false, `${tag}: a scroll hides it`);
    await toDot(page, dotSel(ids.R, 3)); await page.mouse.move(await page.evaluate(() => { const b = document.querySelector('.lisPanel').getBoundingClientRect(); return b.right - 8; }), 40); await page.mouse.move(3, 3);
    await toDot(page, dotSel(ids.R, 2)); assert((await tipNow(page)).on); await page.keyboard.press('Escape'); assert.equal((await tipNow(page)).on, false, `${tag}: Esc hides it at once`); await sleep(450);
    assert.equal(await page.evaluate(() => document.querySelectorAll('.lisPanel').length), 0, 'and the same Esc went on to close the panel, as before'); }
  assert.deepEqual(errors, [], `${tag}: no page errors`); await context.close();
}


// the cold case: a design that is not ready when its dot is hovered
async function coldSuite(browser, width) {
  const tag = `${width}px`, { page, context, errors, id } = await fresh(browser, width, { vec: { delay: { 'AAA-RG': 2600, 'BBB-RG': 2600, 'DDD-RG': 600 }, none: ['CCC-RG'] } }), ids = id.rg1;
  await openPanel(page, 'rg1');   // (the prefetch starts: AAA and BBB take the two slots for 2.6 s; CCC and DDD wait in the queue)
  assert.equal((await calls(page)).includes('DDD-RG'), false, `${tag}: DDD is still waiting in the queue`);
  // a hovered design that is still in the queue jumps it, shows the labelled spinner at once, and swaps to the picture in the same card
  await away(page); await record(page); await toDot(page, dotSel(ids.T, 2));
  let t = await tipNow(page); assert(t.on && t.visible && t.spin && !t.img, `${tag}: a cold dot shows the card at once with a spinner`); assert.equal(t.spinText, 'Loading design', 'the spinner is labelled'); assert.equal(t.w >= 100 && t.h >= 100, true, 'the card has its final size already (the picture will not push anything)');
  await sleep(40); assert.equal((await calls(page)).includes('DDD-RG'), true, `${tag}: the hovered design was started at once (it jumped the queue: ${await calls(page)})`);
  await page.waitForFunction(() => !!PieceDots.tip().querySelector('img'), null, { timeout: 4000 }); t = await tipNow(page); assert(t.on && t.imgOk && !t.spin && t.src.includes('DDD-RG'), `${tag}: the picture replaced the spinner in the same card`);
  const all = await stopRecord(page), fr = all.slice(Math.max(0, all.findIndex(f => f[0] === 1))); assert(fr.length > 5 && fr.every(f => f[0] === 1), `${tag}: the card was there in every frame from the hover to the picture (${fr.map(f => f[0]).join('')})`);
  assert.equal(await page.evaluate(() => document.querySelectorAll('.pdTip').length), 1);
  // the second hollow dot of the same design finds it ready
  await away(page); await toDot(page, dotSel(ids.T, 1)); t = await tipNow(page); assert(t.img && !t.spin, `${tag}: the same design on another dot is ready at once`);
  // a piece of the other design that is still being made: spinner, then the picture
  await away(page); await toDot(page, dotSel(ids.R, 2)); t = await tipNow(page); assert(t.spin && t.spinText === 'Loading design', `${tag}: BBB is still being made: the spinner`);
  await page.waitForFunction(() => { const t = PieceDots.tip(); return t.hasAttribute('data-on') && !!t.querySelector('img'); }, null, { timeout: 6000 }); t = await tipNow(page); assert(t.src.includes('BBB-RG') && t.on, `${tag}: and the picture when it is there`);
  // a known SKU whose design has no master file: after the prefetch, one calm line at once
  await settleQueue(page, 8000); await away(page); await toDot(page, dotSel(ids.S, 2)); t = await tipNow(page);
  assert(t.on && t.textCard && !t.img && !t.spin && t.text === 'No vector design for this piece yet', `${tag}: a design with no file says it in one calm line (${t.text})`);
  // an Unknown SKU piece was never sent to the renderer
  assert.equal((await calls(page)).filter(x => /^ZZ/.test(x)).length, 0, `${tag}: Unknown SKU pieces are never rendered`);
  assert.deepEqual((await calls(page)).slice().sort(), ['AAA-RG', 'BBB-RG', 'CCC-RG', 'DDD-RG'], `${tag}: each design was made exactly once`);
  assert.deepEqual(errors, [], `${tag}: no page errors`); await context.close();
}

// a redraw that replaces the hovered dot; no listener on a dot; none grows with the redraws
async function redrawSuite(browser, width) {
  const tag = `${width}px`, { page, context, errors, id } = await fresh(browser, width, { sheets: RGONLY }), ids = id.rg1;
  await page.evaluate(() => {
    const add = EventTarget.prototype.addEventListener, rem = EventTarget.prototype.removeEventListener; window.__L = { dot: 0, net: new Map() };
    const cap = o => typeof o === 'object' ? !!(o && o.capture) : !!o, where = t => t === document ? 'doc' : t === window ? 'win' : 'el';
    EventTarget.prototype.addEventListener = function (type, fn, o) { if (this.classList && (this.classList.contains('pdot') || this.classList.contains('pdots'))) window.__L.dot++; const k = `${where(this)}|${type}|${cap(o)}`; if (!window.__L.net.has(k)) window.__L.net.set(k, new Set()); window.__L.net.get(k).add(fn); return add.call(this, type, fn, o); };
    EventTarget.prototype.removeEventListener = function (type, fn, o) { const s = window.__L.net.get(`${where(this)}|${type}|${cap(o)}`); if (s) s.delete(fn); return rem.call(this, type, fn, o); };
    window.__net = () => [...window.__L.net].filter(([k]) => /^(doc|win)\|/.test(k)).reduce((n, [, s]) => n + s.size, 0);
  });
  await openPanel(page, 'rg1'); await settleQueue(page);
  const rename = name => page.evaluate(({ rid, name }) => { for (const s of window.__sheets) if (s.orderReadiness[rid]) s.orderReadiness[rid].customer = name; for (const r of window.__rows) if (String(r.order.receiptId) === rid) r.order.buyer.name = name; LibraryIssues.refresh(); }, { rid: ids.R, name });
  await toDot(page, dotSel(ids.R, 2)); const pk = await page.evaluate(() => { window.__old = PieceDots.current(); return window.__old.dataset.pk; });
  await record(page); await rename('Renamed Buyer'); await sleep(120);
  let st = await page.evaluate(() => ({ gone: !window.__old.isConnected, same: PieceDots.current() !== window.__old, pk: PieceDots.current().dataset.pk, conn: PieceDots.current().isConnected, name: [...document.querySelectorAll('.lisRow i')].some(i => i.textContent === 'Renamed Buyer') }));
  assert(st.name, `${tag}: the row was redrawn with the new name`); assert(st.gone && st.same && st.conn && st.pk === pk, `${tag}: the redraw replaced the hovered dot and the card moved to its successor (${JSON.stringify(st)})`);
  assert.equal((await tipNow(page)).on, true, 'the card is still there');
  for (let i = 0; i < 3; i++) { await page.evaluate(() => window.LaserReview.changed()); await sleep(110); await rename('Renamed Buyer ' + i); await sleep(90); }   // (the Library's own redraws)
  const fr = await stopRecord(page); assert(fr.length > 20 && fr.every(f => f[0] === 1 && f[1] === pk), `${tag}: the card never left, through ${fr.length} frames of redraws`);
  // the same with the keyboard: a focused dot keeps focus and card through a redraw
  await page.mouse.move(3, 3); await page.evaluate(s => document.querySelector(s).focus(), dotSel(ids.R, 3)); assert((await tipNow(page)).on);
  await rename('Focused Buyer'); await sleep(120); st = await page.evaluate(() => ({ at: document.activeElement.dataset.pk, on: PieceDots.shown() }));
  assert.equal(st.on, true, `${tag}: a focused dot keeps its card through a redraw`); assert.equal(st.at, `${ids.R}|${ids.R}_3_1`, 'and its focus');
  // no listener on a dot, none grows
  await page.mouse.move(3, 3); await page.evaluate(() => document.activeElement.blur()); await sleep(50);
  const base = await page.evaluate(() => window.__net()); let seen = base;
  for (let i = 0; i < 20; i++) { await toDot(page, dotSel(ids.P, 1 + (i % 6))); await rename('Round ' + i); await sleep(30); await page.mouse.move(3, 3); seen = await page.evaluate(() => window.__net()); if (seen !== base) break; }
  assert.equal(seen, base, `${tag}: no listener is left behind by 20 hovers and 20 redraws (${base} -> ${seen})`);
  assert.equal(await page.evaluate(() => window.__L.dot), 0, `${tag}: no listener was ever added to a dot or a group of dots`);
  assert.deepEqual(errors, [], `${tag}: no page errors`); await context.close();
}

// the card at the edges of the screen, and in a dialog's layer
async function layerSuite(browser, width) {
  const tag = `${width}px`, { page, context, errors, id } = await fresh(browser, width, { sheets: RGONLY }), P = id.rg1.P;
  await page.evaluate(({ P }) => {
    const dots = [1, 2].map(n => ({ ring: n === 2, metal: 'rose', pool: `${P}_${n}_1`, line: `${P}_${n}`, n }));
    const mk = (nm, css) => { const d = document.createElement('div'); d.id = nm; d.setAttribute('data-pd-scope', ''); d.style.cssText = 'position:fixed;background:#fff;padding:14px;z-index:6;' + css; d.innerHTML = PieceDots.html(dots, { order: P, sheetMetal: 'rose' }); document.body.appendChild(d); };
    mk('eTop', 'left:300px;top:64px'); mk('eBot', 'left:300px;bottom:6px'); mk('eLeft', 'left:0;top:300px'); mk('eRight', 'right:0;top:300px');
  }, { P });
  await sleep(120);
  const at = async nm => { await away(page, 30); const c = await centreOf(page, `#${nm} .pdot`); await page.mouse.move(c.x, c.y); const t = await tipNow(page); return Object.assign(t, { dot: c.b }); };
  let t = await at('eTop'); assert(t.on && t.below && t.t >= t.dot.y + t.dot.height - 1, `${tag}: at the top edge the card flips below the dot (${JSON.stringify({ t: t.t, below: t.below, dot: t.dot.y })})`);
  const bar = await page.evaluate(() => { const b = document.querySelector('.topbar'); return b && b.getClientRects().length ? b.getBoundingClientRect().bottom : 0; }); assert(t.t >= bar, `${tag}: and never under the top bar (${t.t} vs ${bar})`);
  t = await at('eBot'); assert(t.on && !t.below && t.b <= t.dot.y + 1 && t.b <= t.vh - 8 + 0.5, `${tag}: at the bottom edge it stays above and inside the screen`);
  t = await at('eLeft'); assert(t.on && t.l >= 7.5, `${tag}: at the left edge it stays inside (${t.l})`);
  t = await at('eRight'); assert(t.on && t.r <= t.vw - 7.5, `${tag}: at the right edge it stays inside (${t.r} of ${t.vw})`);
  // in an open modal dialog (the shared-orders modal's kind): the card goes into the dialog's layer and shows over it
  await away(page); await page.evaluate(({ P }) => { const d = document.createElement('dialog'); d.id = 'dlg'; d.style.cssText = 'padding:26px;border:1px solid #d8d0c0;border-radius:14px;width:300px;height:160px'; d.innerHTML = '<div data-pd-scope>' + PieceDots.html([{ ring: false, metal: 'rose', pool: `${P}_1_1`, line: `${P}_1`, n: 1 }, { ring: true, metal: 'rose', pool: `${P}_2_1`, line: `${P}_2`, n: 2 }], { order: P, sheetMetal: 'rose' }) + '</div>'; document.body.appendChild(d); d.showModal(); }, { P });
  for (const n of [1, 2]) {
    await away(page, 30); const c = await centreOf(page, `#dlg .pdot:nth-child(${n})`); await page.mouse.move(c.x, c.y); t = await tipNow(page);
    assert(t.on && t.parent === 'DIALOG#dlg', `${tag}: in a dialog the card is in the dialog's layer (${t.parent})`);
    const top = await page.evaluate(() => { const tp = PieceDots.tip(); tp.style.pointerEvents = 'auto'; const r = tp.getBoundingClientRect(), e = document.elementFromPoint((r.left + r.right) / 2, (r.top + r.bottom) / 2); const ok = !!e && !!e.closest('.pdTip'); tp.style.pointerEvents = ''; return ok; });
    assert(top, `${tag}: it shows over the open dialog, not behind it`);
  }
  await shot2(page, `card-in-dialog-${width}`);
  await page.evaluate(() => { const d = document.getElementById('dlg'); d.close(); d.remove(); }); await away(page, 30);
  const c = await centreOf(page, '#eTop .pdot'); await page.mouse.move(c.x, c.y); t = await tipNow(page); assert(t.on && t.parent === 'BODY', `${tag}: after the dialog is gone the card is back on the page`);
  assert.deepEqual(errors, [], `${tag}: no page errors`); await context.close();
}
async function shot2(page, name) { if (!SHOTS) return; fs.mkdirSync(SHOTS, { recursive: true }); await page.screenshot({ path: path.join(SHOTS, name + '.png') }); }

// many rows: the cache is bounded, the first work is for what is on screen, the idle cost is small, a list that goes stops its queue
async function scaleSuite(browser) {
  const { page, context, errors } = await fresh(browser, 1440, { sheets: RGONLY });
  await page.evaluate(() => { window.__long = []; try { new PerformanceObserver(l => { for (const e of l.getEntries()) window.__long.push(e.duration); }).observe({ entryTypes: ['longtask'] }); } catch (_) {} window.__vec.fail.add('Y190b'); });
  const info = await page.evaluate(() => {
    const t0 = performance.now(), parts = [];
    for (let i = 0; i < 200; i++) {
      const o = String(4190000000 + i), dots = [];
      for (let k = 1; k <= 6; k++) { const line = `${o}_${k}`, sku = k <= 3 ? `Y${i}` : `Y${i}b`; window.__rows.push({ key: line, order: { receiptId: o, buyer: { name: 'B' } }, line: { transactionId: String(k), listingId: 'L', sku, title: sku, quantity: 1 }, spec: { designSku: sku, size: '', noDesign: false }, state: 'pooled', poolIds: [], problems: [] }); dots.push({ ring: k > 2, metal: 'rose', pool: line + '_1', line, n: k }); }
      parts.push(`<div style="height:30px">${PieceDots.html(dots, { order: o, sheetMetal: 'rose' })}</div>`);
    }
    const html = performance.now() - t0, d = document.createElement('div'); d.id = 'big'; d.setAttribute('data-pd-scope', ''); d.style.cssText = 'position:fixed;left:20px;top:80px;width:300px;height:240px;overflow:auto;background:#fff;z-index:6'; d.innerHTML = parts.join(''); document.body.appendChild(d);
    const t1 = performance.now(), q = PieceDots.warm(d, { clip: d }), warm = performance.now() - t1; return { html, warm, q, dots: d.querySelectorAll('.pdot').length };
  });
  assert.equal(info.dots, 1200, '200 rows of six dots'); assert(info.html < 150, `1200 dots drawn in ${info.html.toFixed(0)} ms`); assert(info.warm < 60, `warm() on 1200 dots took ${info.warm.toFixed(0)} ms before it hands over to idle slices`); assert.equal(info.q, 400, 'it queued each distinct design once');
  await settleQueue(page, 30000);
  const first = (await calls(page)).slice(0, 16), seeing = new Set(); for (let i = 0; i < 8; i++) { seeing.add('Y' + i); seeing.add('Y' + i + 'b'); }
  assert.deepEqual(new Set(first), seeing, `the first work was the eight visible rows (${first.join(' ')})`);
  const s = await page.evaluate(() => PieceDots.stats()); assert(s.size <= 300 && s.cap === 300 && s.ready <= 300, `the cache is bounded at about 300 (${s.size} of ${s.cap})`); assert(s.started >= 400, `all 400 designs were made (${s.started})`);
  assert.equal(await page.evaluate(() => PieceDots.has('sku:Y0|')), '', 'the oldest design was evicted (LRU)'); assert.equal(await page.evaluate(() => PieceDots.has('sku:Y199B|')), 'ready', 'and the newest is ready');
  assert.deepEqual(await page.evaluate(() => window.__long.filter(x => x >= 100)), [], 'no task of 100 ms or more while 400 designs were being prepared in the idle slices');
  // a design that failed: one calm line, tried once, never in a loop
  await page.evaluate(() => { document.getElementById('big').scrollTop = 190 * 30; }); await sleep(80);
  assert.equal(await page.evaluate(() => PieceDots.has('sku:Y190B|')), 'error', 'the failed design is marked, not retried in a loop');
  const c = await centreOf(page, '#big .pdots[data-pd-order="4190000190"] .pdot:nth-child(4)'); await page.mouse.move(c.x, c.y); let t = await tipNow(page);
  assert(t.on && t.textCard && t.text === 'Design could not be loaded', `a design that failed to render says so in one calm line (${t.text})`); await sleep(300); assert.equal((await calls(page)).filter(x => x === 'Y190b').length, 1, 'and was asked for once');
  // an evicted design is made again when its dot is hovered: spinner, then the picture
  await away(page); await page.evaluate(() => { document.getElementById('big').scrollTop = 0; }); await sleep(60);
  const c0 = await centreOf(page, '#big .pdots[data-pd-order="4190000000"] .pdot:nth-child(1)'); await page.mouse.move(c0.x, c0.y);
  await page.waitForFunction(() => { const x = PieceDots.tip(); return x.hasAttribute('data-on') && !!x.querySelector('img'); }, null, { timeout: 4000 }); await away(page);
  // a list that goes from the page stops its queue
  await page.evaluate(() => { document.getElementById('big').remove(); window.__vec.all = 120; });
  const was = await page.evaluate(() => PieceDots.stats().started);
  await page.evaluate(() => { const parts = []; for (let i = 0; i < 150; i++) { const o = String(4195000000 + i), line = o + '_1'; window.__rows.push({ key: line, order: { receiptId: o, buyer: { name: 'B' } }, line: { transactionId: '1', listingId: 'L', sku: 'W' + i, title: 'w', quantity: 1 }, spec: { designSku: 'W' + i, size: '', noDesign: false }, state: 'pooled', poolIds: [], problems: [] }); parts.push(PieceDots.html([{ ring: true, metal: 'rose', pool: line + '_1', line, n: 1 }], { order: o })); } const d = document.createElement('div'); d.id = 'gone'; d.setAttribute('data-pd-scope', ''); d.innerHTML = parts.join(''); document.body.appendChild(d); PieceDots.warm(d); });
  await sleep(140); await page.evaluate(() => document.getElementById('gone').remove()); await sleep(900);
  const now = await page.evaluate(() => PieceDots.stats()); assert(now.started - was <= 8, `the queue of a list that has gone from the page was dropped (${now.started - was} of 150 started)`); assert(!now.queued && !now.running, 'nothing is left running');
  assert.deepEqual(errors, [], 'no page errors'); await context.close();
}


/* ═══ a press on a dot opens the order on exactly that piece ═══ */
const OPEN_STUB = `window.__openWindow = () => { const w = document.createElement('dialog'); w.className = 'stubWin'; w.setAttribute('open', ''); document.body.appendChild(w); window.__win = w; return true; };`;
async function closeWin(page) {
  await page.evaluate(() => { for (const w of document.querySelectorAll('dialog.stubWin')) { w.removeAttribute('open'); w.dispatchEvent(new Event('close')); w.remove(); } window.__win = null; });
  await page.waitForFunction(() => { const p = document.querySelector('.lisPanel'); return !!p && !p.classList.contains('handed'); }, null, { timeout: 3000 }); await sleep(130);
}
// what each dot of rg1's orders must stand for, from the world (not from the component): the exact piece = its pool id (copy) and its line's key
function oracle(ids) {
  const o = {}, pcs = (rid, n) => [...Array(n)].map((_, i) => ({ pool: `${rid}_${i + 1}_1`, line: `${rid}_${i + 1}` }));
  o[ids.P] = pcs(ids.P, 6); o[ids.Q] = pcs(ids.Q, 2); o[ids.R] = pcs(ids.R, 3); o[ids.S] = pcs(ids.S, 2); o[ids.T] = [{ pool: `${ids.T}_1_1`, line: `${ids.T}_1` }, { pool: `${ids.T}_1_2`, line: `${ids.T}_1` }];
  return o;
}
// one press; what came of it
async function press(page, mode, sel) {
  await page.evaluate(() => { window.__calls.length = 0; });
  await page.$eval(sel, e => e.scrollIntoView({ block: 'nearest' }));
  const c = await centreOf(page, sel);
  if (mode === 'mouse') { await page.mouse.move(c.x, c.y); const t = await tipNow(page); await page.mouse.down(); await page.mouse.up(); return { hovered: !!(t && t.on) }; }
  if (mode === 'touch') { await page.touchscreen.tap(c.x, c.y); return {}; }
  await page.evaluate(s => document.querySelector(s).focus(), sel); const t = await tipNow(page); await page.keyboard.press(mode === 'enter' ? 'Enter' : 'Space'); return { hovered: !!(t && t.on) };
}
const outcome = page => page.evaluate(() => ({ calls: window.__calls.slice(), handed: !!document.querySelector('.lisPanel.handed'), tip: PieceDots.shown() }));
// the dots a mutant must get wrong: a filled one, a hollow one, an Unknown SKU one, the second copy of a line
async function clickFailures(page, ids) {
  const out = [], want = oracle(ids), pick = [[ids.P, 1], [ids.R, 2], [ids.R, 3], [ids.T, 2]];
  await page.evaluate(OPEN_STUB);
  for (const [rid, n] of pick) {
    await press(page, 'mouse', dotSel(rid, n)); const got = await outcome(page), w = want[rid][n - 1];
    if (got.calls.length !== 1) out.push(`${rid}#${n}: ${got.calls.length} order windows for one press`);
    else { const [, r, pool, json] = got.calls[0], o = JSON.parse(json); if (r !== rid || pool !== w.pool || o.poolId !== w.pool || !o.row || o.row.key !== w.line || o.pick !== true) out.push(`${rid}#${n}: opened ${json} instead of piece ${w.pool}`); }
    if (got.calls.length) { try { await closeWin(page); } catch (_) { out.push(`${rid}#${n}: the panel did not come back when the window closed`); return out; } }
  }
  return out;
}
async function clickSuite(browser, width) {
  const tag = `${width}px`;
  for (const touch of [false, true]) {
    const { page, context, errors, id } = await fresh(browser, width, { sheets: RGONLY, touch }), ids = id.rg1, want = oracle(ids);
    await page.emulateMedia({ reducedMotion: 'reduce' }); await page.evaluate(OPEN_STUB);
    await openPanel(page, 'rg1'); await settleQueue(page);
    const rows = await dotsOf(page); let n = 0;
    for (const mode of touch ? ['touch'] : ['mouse', 'enter', 'space']) {
      for (const r of rows) for (const [i, d] of r.dots.entries()) {
        const w = want[r.order][i];
        assert.equal(d.pool, w.pool, `${tag}: dot ${i + 1} of ${r.order} stands for piece ${w.pool}`); assert.equal(d.line, w.line);
        const how = await press(page, mode, dotSel(r.order, i + 1)), got = await outcome(page); n++;
        assert.equal(got.calls.length, 1, `${tag} ${mode} ${r.order}#${i + 1}: one press, one order window (${JSON.stringify(got.calls)})`);
        const [kind, rid, pool, json] = got.calls[0]; assert.equal(kind, 'order'); assert.equal(rid, r.order); assert.equal(pool, w.pool, `${tag} ${mode}: the pool id of exactly that piece`);
        assert.deepEqual(JSON.parse(json), { pick: true, poolId: w.pool, row: { key: w.line } }, `${tag} ${mode} ${r.order}#${i + 1} (${d.ring ? 'hollow' : 'filled'}): the order opens on that piece`);
        assert(got.handed, `${tag}: the panel steps aside exactly as for a row (no pop-up on a pop-up)`); assert.equal(got.tip, false, `${tag}: the hover card is gone with the press`);
        if (mode === 'mouse' || mode === 'enter') assert.equal(how.hovered, true, 'the card was showing before the press');
        await closeWin(page);
      }
    }
    // a press on the row outside its dots still opens the order as before: "All pieces" (no pick, no piece)
    { await page.evaluate(() => { window.__calls.length = 0; }); const sel = `.lisRow[data-issue-order="${ids.R}"] .lisWho b`; await page.$eval(sel, e => e.scrollIntoView({ block: 'nearest' })); const c = await centreOf(page, sel);
      if (touch) await page.touchscreen.tap(c.x, c.y); else await page.mouse.click(c.x, c.y);
      const got = await outcome(page); assert.deepEqual(got.calls, [['order', ids.R, null, '{}']], `${tag}: a press on the row outside the dots opens the order as before, on all pieces`); assert(got.handed); await closeWin(page); }
    // the keyboard on a row: Enter opens the order as before; ArrowRight goes into its dots
    if (!touch) { await page.evaluate(() => { window.__calls.length = 0; }); const sel = `.lisRow[data-issue-order="${ids.R}"]`; await page.focus(sel); await page.keyboard.press('ArrowRight');
      assert.equal(await page.evaluate(() => document.activeElement.dataset.pk), `${ids.R}|${ids.R}_1_1`, `${tag}: ArrowRight on a row goes to its first dot`);
      await page.keyboard.press('ArrowLeft'); assert.equal(await page.evaluate(() => document.activeElement.classList.contains('lisRow')), true, 'and ArrowLeft on the first dot goes back to the row');
      await page.keyboard.press('Enter'); const got = await outcome(page); assert.deepEqual(got.calls, [['order', ids.R, null, '{}']], `${tag}: Enter on the row opens the order as before`); await closeWin(page); }
    // a touch press shows the card while the finger is down; lifting without a tap hides it
    if (touch) { const c = await centreOf(page, dotSel(ids.R, 2)), cdp = await context.newCDPSession(page);
      await cdp.send('Input.dispatchTouchEvent', { type: 'touchStart', touchPoints: [{ x: c.x, y: c.y }] }); const t = await tipNow(page); assert(t && t.on && t.shownFor === `${ids.R}|${ids.R}_2_1`, `${tag}: a touch press shows the card for that dot`);
      await cdp.send('Input.dispatchTouchEvent', { type: 'touchCancel', touchPoints: [] }); assert.equal((await tipNow(page)).on, false, `${tag}: a cancelled touch hides it`); }
    // the window's close (Esc) returns to the panel; the panel's own Esc closes it and gives the keyboard back to the '!'
    await press(page, 'mouse', dotSel(ids.R, 3)); assert((await outcome(page)).handed); await closeWin(page);
    assert.equal(await page.evaluate(() => !!document.querySelector('.lisPanel') && !document.querySelector('.lisPanel.handed')), true, `${tag}: the panel is back when the order window closes`);
    // a dot inside a dialog (a modal pop-up): the press opens the order on that piece with the page's hand-off; the dialog is not the issues panel's business
    if (!touch) {
      await page.keyboard.press('Escape'); await sleep(450);
      await page.evaluate(({ P }) => { window.__calls.length = 0; const d = document.createElement('dialog'); d.id = 'dlg'; d.style.cssText = 'padding:26px;border:1px solid #d8d0c0;border-radius:14px;width:300px;height:160px'; d.innerHTML = '<div data-pd-scope>' + PieceDots.html([{ ring: false, metal: 'rose', pool: `${P}_1_1`, line: `${P}_1`, n: 1 }, { ring: true, metal: 'rose', pool: `${P}_2_1`, line: `${P}_2`, n: 2 }], { order: P, sheetMetal: 'rose' }) + '</div>'; document.body.appendChild(d); d.showModal(); }, { P: ids.P });
      for (const [n, via] of [[1, 'mouse'], [2, 'mouse'], [2, 'enter']]) {
        await page.evaluate(() => { window.__calls.length = 0; }); const sel = `#dlg .pdot:nth-child(${n})`;
        if (via === 'mouse') { const c = await centreOf(page, sel); await page.mouse.click(c.x, c.y); } else { await page.evaluate(s => document.querySelector(s).focus(), sel); await page.keyboard.press('Enter'); }
        const got = await outcome(page); assert.equal(got.calls.length, 1, `${tag}: a dot in a dialog opens its order (${JSON.stringify(got.calls)})`); assert.deepEqual(JSON.parse(got.calls[0][3]), { pick: true, poolId: `${ids.P}_${n}_1`, row: { key: `${ids.P}_${n}` } }); assert.equal(got.tip, false);
        await page.evaluate(() => { const w = window.__win; if (w) { w.remove(); window.__win = null; } });
      }
    }
    console.log(`    ${tag}${touch ? ' touch' : ''}: ${n} presses opened exactly their own piece`);
    assert.deepEqual(errors, [], `${tag}: no page errors`); await context.close();
  }
}

/* ═══ mutants: a broken component must fail the matching check ═══ */
async function mutantRun(browser, file, from, to, check) {
  const m = await openMutant(browser, file, from, to); m.page.setDefaultTimeout(8000);
  try {
    const worlds = await build(m.page, { sheets: RGONLY }), ids = worlds[0].ids;
    await m.page.evaluate(OPEN_STUB); await openPanel(m.page, 'rg1'); await sleep(500);
    return await check(m.page, ids);
  } finally { await m.context.close(); }
}
async function mutants(browser) {
  const PD = 'charm-nest-piece-dots.js', LI = 'charm-nest-library-issues.js';
  const cases = [
    ['a ring in the issue red', PD, 'border:2px solid var(--mc,var(--ink45,#938c80))', 'border:2px solid var(--clay,#b0563f)', async page => colourFailures(page)],
    ['an unknown piece that ignores its list\'s sheet colour', PD, 'const m = metalKey(d && d.metal) || metalKey(o.sheetMetal),', 'const m = metalKey(d && d.metal) || \'\',', async page => { const r = (await dotsOf(page))[0], rose = await colour(page, '--m-rose'); return r.dots.filter(d => d.ring && d.border !== rose).map(d => 'an unknown ring is ' + d.border); }],
    ['a hover that waits 120 ms', PD, "    show(d, 'pointer', ev);\n  }, true);", "    setTimeout(() => show(d, 'pointer', ev), 120);\n  }, true);", async page => instantFailures(page)],
    ['a hover that waits for the next frame', PD, "    show(d, 'pointer', ev);\n  }, true);", "    requestAnimationFrame(() => show(d, 'pointer', ev));\n  }, true);", async page => instantFailures(page)],
    ['no prefetch', PD, '  function warm(scope, o) {\n', '  function warm(scope, o) {\n    return 0;\n', async page => (await calls(page)).length < 4 ? ['nothing was made before the hover'] : []],
    ['a press that does not say "pick"', PD, 'Object.assign({ pick: true }, pool ?', 'Object.assign({}, pool ?', async (page, ids) => clickFailures(page, ids)],
    ['a press that opens the first copy of the line', PD, 'pool ? { poolId: pool } : {}', "pool ? { poolId: lineOf(pool) + '_1' } : {}", async (page, ids) => clickFailures(page, ids)],
    ['a panel that opens the row as well as the piece', LI, "if (t.closest('.pdot')) return;", 'if (false) return;', async (page, ids) => clickFailures(page, ids)]
  ];
  for (const [name, file, from, to, check] of cases) {
    const found = await mutantRun(browser, file, from, to, check);
    assert(found.length > 0, `the mutant "${name}" was NOT caught`);
    console.log(`    mutant caught: ${name} (${found[0]})`);
  }
  // (and the real code passes all of those checks)
  const real = await mutantRun(browser, PD, 'const CAP = 300', 'const CAP = 300', async (page, ids) => [].concat(await colourFailures(page), await instantFailures(page), (await calls(page)).length < 4 ? ['no prefetch'] : [], await clickFailures(page, ids)));
  assert.deepEqual(real, [], 'the unchanged component passes every check the mutants fail');
}


/* ═══ part 2: the whole page on the fake site: the REAL order window opens on the piece a dot stands for ═══ */
const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 12, 17) / 1000);
const O3 = { rid: '4170600003', t: ['41706000031', '41706000032', '41706000033', '41706000034', '41706000035'] };   // ROSE_A on RG Sheet 1 · ROSE_B pooled · MYSTERY_Z (a SKU in no master file) · SILV_D on SS Sheet 1 (another metal) · no SKU at all
const Q2 = { rid: '4170600004', t: ['41706000041', '41706000042'] };                                 // TWIN_X (two copies: one on RG Sheet 1, one not placed) · SOLO_Y on RG Sheet 1
const RG1 = 'sheet-pd-rg1', SS1 = 'sheet-pd-ss1';
const pid = (o, i, c = 1) => `${o.rid}_${o.t[i]}_${c}`, lkey = (o, i) => `${o.rid}_${o.t[i]}`;
const fline = (tid, sku, metalKey, metalLabel, qty = 1) => ({ transactionId: tid, listingId: '19010' + tid.slice(-5), sku, title: (sku || 'Charm') + ' earrings', quantity: qty, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: metalLabel }], metalKey, metalLabel, personalization: [], buyerMessage: '' });
const forder = (o, buyer, lines) => ({ receiptId: o.rid, orderNumber: o.rid, createTs: SHIP - 7 * DAY, updateTs: SHIP - 7 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
async function fullApp(browser, mutant = null) {   // (mutant: { from, to }: the page's order window script with one change; the run must then fail)
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] }), errors = [], seenCalls = [];
  try {
    const box = (id, cx, cy) => ({ id, cxPt: cx, cyPt: cy, angle: 0, wPt: 34, hPt: 34 }), ch = (id, o, p, sku) => ({ id, name: `${o.rid} · ${sku}`, poolId: p, order: o.rid, sku });
    srv.st.put('Charm_Nest_Sheets', RG1, { id: RG1, metal: 'rose', sheetIndex: 1, day: '2026-10-04', status: 'written', stock: { wPt: 300, hPt: 140 }, orders: [O3.rid, Q2.rid], placements: [box('r1', 60, 60), box('r2', 130, 60), box('r3', 200, 60)], charms: [ch('r1', O3, pid(O3, 0), 'ROSE_A'), ch('r2', Q2, pid(Q2, 0, 1), 'TWIN_X'), ch('r3', Q2, pid(Q2, 1), 'SOLO_Y')] });
    srv.st.put('Charm_Nest_Sheets', SS1, { id: SS1, metal: 'silver', sheetIndex: 1, day: '2026-10-04', status: 'written', stock: { wPt: 200, hPt: 140 }, orders: [O3.rid], placements: [box('s1', 60, 60)], charms: [ch('s1', O3, pid(O3, 3), 'SILV_D')] });
    const poolRow = (o, i, c, sku, material, sheetId, n, code) => srv.st.put('Charm_Pool', pid(o, i, c), { poolId: pid(o, i, c), orderId: o.rid, transactionId: o.t[i], lineKey: lkey(o, i), sku, material, copy: c, quantity: c, state: sheetId ? 'written' : 'pooled', sheetId: sheetId || null, sheetName: sheetId ? `2026-10-04_${code}_Set-1_Sheet-${n}` : null, updatedAt: Date.now() });
    poolRow(O3, 0, 1, 'ROSE_A', 'rose', RG1, 1, 'RG'); poolRow(O3, 1, 1, 'ROSE_B', 'rose', null, 0, 'RG'); poolRow(O3, 2, 1, 'MYSTERY_Z', 'rose', null, 0, 'RG'); poolRow(O3, 3, 1, 'SILV_D', 'silver', SS1, 1, 'SS');
    poolRow(Q2, 0, 1, 'TWIN_X', 'rose', RG1, 1, 'RG'); poolRow(Q2, 0, 2, 'TWIN_X', 'rose', null, 0, 'RG'); poolRow(Q2, 1, 1, 'SOLO_Y', 'rose', RG1, 1, 'RG');
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, hasTouch: true });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    if (mutant) { const src = read('charm-nest-bridge.js'); assert(src.includes(mutant.from), 'the mutant\'s anchor is in the bridge'); await context.route(/\/charm-nest-bridge\.js(\?|$)/, r => r.fulfill({ status: 200, contentType: 'text/javascript', body: src.replace(mutant.from, mutant.to) })); }
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); localStorage.setItem('cn.tour.seen', '1'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(); page.setDefaultTimeout(mutant ? 12000 : 30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    page.on('request', r => { const u = r.url(); if (/\/\.netlify\/functions\//.test(u) || !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u)) seenCalls.push(u.replace(/^https?:\/\/[^/]+/, '').split('?')[0]); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.OrderPieces && window.PieceDots && window.LibraryIssues && window.LaserReview && window.PieceMedia && PieceMedia.vectorThumb && CN.S.cloud.ok === true, null, { timeout: 60000 }).catch(async () => { const have = await page.evaluate(() => ['CN', 'Orders', 'OrderWin', 'OrderPieces', 'PieceDots', 'LibraryIssues', 'LaserReview', 'PieceMedia'].map(k => k + ':' + typeof window[k]).join(' ') + ' cloud:' + !!(window.CN && CN.S && CN.S.cloud && CN.S.cloud.ok)); throw new Error('the page did not come up: ' + have); });
    await page.emulateMedia({ reducedMotion: 'reduce' });
    const specs = [
      [forder(O3, 'Olive Three', [fline(O3.t[0], 'ROSE_A', 'rose', '14k Rose Gold Filled'), fline(O3.t[1], 'ROSE_B', 'rose', '14k Rose Gold Filled'), fline(O3.t[2], 'MYSTERY_Z', 'rose', '14k Rose Gold Filled'), fline(O3.t[3], 'SILV_D', 'silver', 'Sterling Silver'), fline(O3.t[4], '', 'rose', '14k Rose Gold Filled')]), [[pid(O3, 0)], [pid(O3, 1)], [pid(O3, 2)], [pid(O3, 3)], []]],
      [forder(Q2, 'Quinn Twin', [fline(Q2.t[0], 'TWIN_X', 'rose', '14k Rose Gold Filled', 2), fline(Q2.t[1], 'SOLO_Y', 'rose', '14k Rose Gold Filled')]), [[pid(Q2, 0, 1), pid(Q2, 0, 2)], [pid(Q2, 1)]]]];
    await page.evaluate(async ({ specs, RG1, SS1 }) => {
      await Orders.loadMaps(true);
      for (const [order, pools] of specs) order.lines.forEach((line, i) => { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: pools[i].length ? 'pooled' : 'pulled', reason: null, claimedBy: null, poolIds: pools[i], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); });
      Orders.interpretAll();
      // (the fake site has no master files: the lines that stand for a design are told they have one; MYSTERY_Z and the line with no SKU keep their "unknown SKU")
      const known = new Set(['ROSE_A', 'ROSE_B', 'SILV_D', 'TWIN_X', 'SOLO_Y']); for (const r of B.orders.rows) if (known.has(r.line.sku)) r.problems = (r.problems || []).filter(p => (p.kind || p) !== 'unmatchedSku');
      CN.setMode('orders'); Orders.render();
      // a vector design for every piece that has a SKU, drawn by the page's own ListMedia (one pooled charm: a tag with a ring)
      const sq = { kind: 'path', subpaths: [[['m', [0, 0]], ['l', [20, 0]], ['l', [20, 48]], ['l', [0, 48]], ['h']]], stroke: true, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [0, 0, 20, 48] };
      const charm = { id: 'c', name: 'TAG', metal: 'gold', centerPt: [10, 24], widthPt: 20, heightPt: 48, areaPt2: 20 * 48, outline: sq, members: [sq], bbox: [0, 0, 20, 48], thumb: '' };
      const mine = new Set(B.orders.rows.filter(r => known.has(r.line.sku)).flatMap(r => r.poolIds)), was = Pool.charmOf; Pool.charmOf = id => mine.has(id) ? Object.assign({ poolId: id }, charm) : was(id);
      await OrderPieces.load(specs.map(([o]) => o.receiptId), { force: true });
    }, { specs, RG1, SS1 });
    await page.waitForFunction(({ a, b }) => { const x = OrderPieces.of(a), y = OrderPieces.of(b); return x.length === 5 && y.length === 3 && !x.concat(y).some(p => p.loading); }, { a: O3.rid, b: Q2.rid }, { timeout: 15000 });
    const pieces = await page.evaluate(({ a, b }) => [a, b].map(r => OrderPieces.of(r).map(p => ({ key: p.key, line: p.lineKey, index: p.index, sku: p.sku, sheet: p.sheetLabel, problem: p.problem }))), { a: O3.rid, b: Q2.rid });
    assert.deepEqual(pieces[0].map(p => p.key), [pid(O3, 0), pid(O3, 1), pid(O3, 2), pid(O3, 3), pid(O3, 4)], `the order's five pieces, as OrderPieces tells them: ${JSON.stringify(pieces[0])}`);
    assert.deepEqual(pieces[1].map(p => p.key), [pid(Q2, 0, 1), pid(Q2, 0, 2), pid(Q2, 1)], `the twin order's three pieces (two copies of one line): ${JSON.stringify(pieces[1])}`);
    // the issues panel on a '!' (the feed is the readiness' own shape: the pieces that hold each order back)
    await page.evaluate(({ RG1, a, b }) => {
      const mk = (rid, customer, lid) => { const ps = OrderPieces.of(rid), bad = ps.filter(p => p.sheetId !== RG1);
        return { step: 'orders', key: bad.some(p => p.problem === 'noSku') ? 'noSku' : 'pooled', orderId: rid, orderLabel: 'Order ' + rid, customer, listingId: lid, thumb: null, pieceCount: ps.length,
          pieces: bad.map(p => ({ index: p.index, key: p.key, poolId: p.key, label: p.label, kind: p.problem || (p.sheetLabel ? 'otherSheetNotReady' : 'pooled'), sheetId: p.sheetId, sheetLabel: p.sheetLabel, stage: '', why: p.sheetLabel ? 'on ' + p.sheetLabel : p.problem || 'pooled' })), open: { type: 'order', id: rid, poolId: bad[0] && bad[0].key } }; };
      window.__feed = { id: RG1, label: 'RG Sheet 1', code: 'RG', metal: 'rose', ready: false, step: 'orders', issues: [mk(a, 'Olive Three', '1901000031'), mk(b, 'Quinn Twin', '1901000041')] };
      LaserReview.issuesOf = id => id === RG1 ? window.__feed : null;
      const bx = document.createElement('div'); bx.className = 'flowBox'; bx.style.cssText = 'position:fixed;left:60px;top:90px;z-index:40;background:#fff;padding:8px 14px;border-radius:10px'; bx.innerHTML = `<button type="button" data-issues-open data-issues-id="${RG1}" data-issues-step="orders" aria-label="Order check">!</button>`; document.body.appendChild(bx);
    }, { RG1, a: O3.rid, b: Q2.rid });
    const opener = `.flowBox [data-issues-open]`;
    const closeOrder = async () => { await page.keyboard.press('Escape'); await page.waitForFunction(() => !OrderWin.isOpen(), null, { timeout: 8000 }); await page.waitForFunction(() => { const p = document.querySelector('.lisPanel'); return !!p && !p.classList.contains('handed'); }, null, { timeout: 8000 }); await sleep(150); };
    const hoverPhase = seenCalls.length;
    await page.click(opener); await page.waitForSelector('.lisPanel .pdot'); await sleep(500);
    await settleQueue(page, 15000);
    const rows = await dotsOf(page); assert.equal(rows.length, 2, 'two orders listed');
    const by = Object.fromEntries(rows.map(r => [r.order, r]));
    // what each dot must be, from the world: O3 [filled ROSE_A, ring ROSE_B, ring MYSTERY_Z, ring SILV_D, ring no SKU]; Q2 [filled TWIN copy 1, filled SOLO, ring TWIN copy 2]
    const want = { [O3.rid]: [[pid(O3, 0), lkey(O3, 0)], [pid(O3, 1), lkey(O3, 1)], [pid(O3, 2), lkey(O3, 2)], [pid(O3, 3), lkey(O3, 3)], [pid(O3, 4), lkey(O3, 4)]], [Q2.rid]: [[pid(Q2, 0, 1), lkey(Q2, 0)], [pid(Q2, 1), lkey(Q2, 1)], [pid(Q2, 0, 2), lkey(Q2, 0)]] };
    assert.deepEqual(by[O3.rid].dots.map(d => d.ring), [false, true, true, true, true]); assert.deepEqual(by[Q2.rid].dots.map(d => d.ring), [false, false, true]);
    for (const rid of [O3.rid, Q2.rid]) assert.deepEqual(by[rid].dots.map(d => [d.pool, d.line]), want[rid], `${rid}: each dot is exactly its piece`);
    // the colours in the real page: the silver piece is silver, the pooled rose piece and the Unknown SKU piece are rose (the list's sheet)
    const rose = await colour(page, '--m-rose'), silver = await colour(page, '--m-silver');
    assert.deepEqual(by[O3.rid].dots.map(d => d.ring ? d.border : d.bg), [rose, rose, rose, silver, rose], 'rose, rose, rose (an Unknown SKU piece under an RG list), silver (a piece on SS Sheet 1), rose (no SKU at all)');
    // the real ListMedia: a design that has one shows its real picture in the card, ready before the hover; the Unknown SKU piece says its line
    const shown = [];
    for (const rid of [O3.rid, Q2.rid]) for (const [i, d] of by[rid].dots.entries()) {
      await away(page, 15); await toDot(page, dotSel(rid, i + 1)); const t = await tipNow(page); shown.push([rid, i, t.img, t.spin, t.text]);
      if (rid === O3.rid && (i === 2 || i === 4)) assert(t.textCard && t.text === 'No vector design for this piece yet' && !t.img, 'an Unknown SKU piece and a piece with no SKU: one calm line');
      else assert(t.on && t.img && t.imgOk && !t.spin && /^data:image\/png/.test(await page.evaluate(() => PieceDots.tip().querySelector('img').src)), `${rid} dot ${i + 1}: the page's own rendering of the vector design, ready before the hover ${JSON.stringify(t).slice(0, 400)} ${JSON.stringify(await page.evaluate(() => PieceDots.stats()))}`);
    }
    await shot(page, 'full-app-panel');
    await away(page);
    assert.deepEqual(seenCalls.slice(hoverPhase).filter(u => /etsy/i.test(u)), [], 'opening the list and hovering every dot made no Etsy call');
    assert.deepEqual(seenCalls.slice(hoverPhase).filter(u => !/^\/\.netlify\/functions\//.test(u)), [], 'nor any call that left this machine');
    // ── the press on each dot, by mouse, keyboard and touch: the REAL order window opens with exactly that piece selected ──
    const ui = () => page.evaluate(() => { const sc = OrderWin._scope(), on = document.querySelector('#owPieceSw button.on'); return { open: OrderWin.isOpen(), rid: OrderWin.rid(), key: OrderWin.key(), piece: sc && sc.piece, chip: on ? on.dataset.piece : null, chipText: on ? on.textContent.trim().slice(0, 40) : '', chips: [...document.querySelectorAll('#owPieceSw button')].map(b => b.dataset.piece), handed: !!document.querySelector('.lisPanel.handed') }; });
    let n = 0;
    for (const mode of ['mouse', 'enter', 'touch']) for (const rid of [O3.rid, Q2.rid]) for (const [i, [pool, line]] of want[rid].entries()) {
      await page.$eval(dotSel(rid, i + 1), e => e.scrollIntoView({ block: 'nearest' })); const c = await centreOf(page, dotSel(rid, i + 1));
      if (mode === 'mouse') { await page.mouse.move(c.x, c.y); await page.mouse.down(); await page.mouse.up(); } else if (mode === 'touch') await page.touchscreen.tap(c.x, c.y); else { await page.evaluate(s => document.querySelector(s).focus(), dotSel(rid, i + 1)); await page.keyboard.press('Enter'); }
      await page.waitForFunction(r => OrderWin.isOpen() && OrderWin.rid() === r && OrderWin._scope() && document.querySelector('#owPieceSw button.on'), rid, { timeout: 15000 });
      const u = await ui(); n++;
      assert.equal(u.rid, rid, `${mode}: the order window shows order ${rid}`); assert.equal(u.key, line, `${mode} ${rid}#${i + 1}: the window is on that piece's line`);
      assert.equal(u.piece, line, `${mode} ${rid}#${i + 1}: the piece switcher's scope is exactly that piece (${pool})`); assert.equal(u.chip, line, `${mode} ${rid}#${i + 1}: the selected chip is that piece's (${u.chipText})`);
      assert(u.handed, `${mode}: the issues panel stepped aside`);
      if (rid === O3.rid) assert.deepEqual(u.chips, ['', ...[0, 1, 2, 3, 4].map(k => lkey(O3, k))], 'all five pieces are in the switcher, and "All pieces"');
      if (n === 2 || (mode === 'mouse' && rid === Q2.rid && i === 2)) await shot2(page, `full-app-order-${mode}-${rid}-${i + 1}`);
      await closeOrder();
    }
    // a press on the row outside the dots: the order opens as before, on "All pieces"
    { const sel = `.lisRow[data-issue-order="${O3.rid}"] .lisWho b`, c = await centreOf(page, sel); await page.mouse.click(c.x, c.y);
      await page.waitForFunction(r => OrderWin.isOpen() && OrderWin.rid() === r, O3.rid, { timeout: 15000 }); const u = await ui(); assert.equal(u.piece, null, 'a press on the row opens "All pieces"'); assert.equal(u.chip, '', 'the "All pieces" chip is selected'); await closeOrder(); }
    // from a dialog (the shared-orders modal's kind): the real hand-off: the dialog gives way, the order opens on that piece, and the dialog is back when the order closes
    await page.keyboard.press('Escape'); await sleep(450);
    await page.evaluate(({ O3 }) => { const d = document.createElement('dialog'); d.id = 'srcDlg'; d.style.cssText = 'padding:26px;border:1px solid #d8d0c0;border-radius:14px;width:300px;height:160px'; d.innerHTML = '<div data-pd-scope>' + PieceDots.html([{ ring: false, metal: 'rose', pool: O3.p0, line: O3.l0, n: 1 }, { ring: true, metal: 'rose', pool: O3.p1, line: O3.l1, n: 2 }], { order: O3.rid, sheetMetal: 'rose' }) + '</div>'; document.body.appendChild(d); d.showModal(); }, { O3: { rid: O3.rid, p0: pid(O3, 0), l0: lkey(O3, 0), p1: pid(O3, 1), l1: lkey(O3, 1) } });
    { const c = await centreOf(page, '#srcDlg .pdot:nth-child(2)'); await page.mouse.click(c.x, c.y);
      await page.waitForFunction(r => OrderWin.isOpen() && OrderWin.rid() === r && OrderWin._scope() && document.querySelector('#owPieceSw button.on'), O3.rid, { timeout: 15000 }); const u = await ui(); assert.equal(u.chip, lkey(O3, 1), 'a dot in a dialog opens the order on that piece too');
      await page.keyboard.press('Escape'); await page.waitForFunction(() => !OrderWin.isOpen(), null, { timeout: 8000 }); await sleep(900);
      assert.equal(await page.evaluate(() => { const d = document.getElementById('srcDlg'); return !!d && d.open && d.style.opacity !== '0'; }), true, 'closing the order window returns to the dialog it came from'); await page.evaluate(() => { const d = document.getElementById('srcDlg'); d.close(); d.remove(); }); }
    console.log(`    full app: ${n} presses (mouse, keyboard, touch) opened the real order window on exactly their own piece`);
    assert.deepEqual(errors, [], 'no page errors'); await context.close();
  } finally { srv.close(); }
}

// screenshots (SHOTS=<dir>): the panel with the card over a filled dot, over a hollow dot of a known design, and over an Unknown SKU piece, for RG, GF and 14K
async function shotsSuite(browser, width) {
  const { page, context, errors, id } = await fresh(browser, width, { sheets: SHEETS });
  for (const sh of SHEETS.filter(x => ['rg1', 'gf1', 'k14'].includes(x.id))) {
    await openPanel(page, sh.id); await settleQueue(page); const ids = id[sh.id];
    for (const [name, rid, n] of [['filled', ids.R, 1], ['hollow', ids.R, 2], ['unknown', ids.R, 3]]) { await away(page, 40); await toDot(page, dotSel(rid, n)); await sleep(60); await shot(page, `card-${name}-${sh.code}-${width}`); }
    await away(page, 40); await page.keyboard.press('Escape'); await sleep(450);
  }
  assert.deepEqual(errors, [], 'no page errors'); await context.close();
}

(async () => {
  const browser = await chromium.launch({ executablePath: CHROME, args: ['--no-sandbox'] });
  try {
    const only = process.env.PD_ONLY || '';
    // which lists draw piece dots: one component for all of them (a list with dots of its own would have its own colours and no card)
    { const files = fs.readdirSync(root).filter(f => /^charm-nest-.*\.(js|html)$/.test(f) && f !== 'charm-nest-piece-dots.js');
      assert.deepEqual(files.filter(f => /PieceDots\.html\(/.test(read(f))), ['charm-nest-library-issues.js'], 'today the Order check panel is the one list that draws piece dots (the shared-orders modal and the search results draw none); a list that gets dots uses PieceDots and is added to this test');
      assert.deepEqual(files.filter(f => /class="pdots?\b|lisDot|\.pdot\b/.test(read(f))), ['charm-nest-library-issues.js'], 'and no file draws dots of its own (the panel only names them)'); }
    for (const width of only && only !== '1' ? [] : [1440, 390]) {
      const tag = `${width}px`, errors = [], { page, context } = await openPage(browser, { width, height: width === 390 ? 844 : 900, errors });
      if (width < 600) await page.evaluate(() => document.getElementById('app').classList.add('railOff'));
      const worlds = await build(page);
      const wid = Object.fromEntries(SHEETS.map((s, i) => [s.id, worlds[i].ids]));

      // ── 1 · the colour of every ring, for every sheet ──
      const toks = {}; for (const m of Object.values(TOKEN)) toks[m] = await colour(page, m);
      const red = await colour(page, '--clay'), neutral = await colour(page, '--ink45'), pale = await colour(page, '--ink25');
      for (const [i, sh] of SHEETS.entries()) {
        await openPanel(page, sh.id);
        const rows = await dotsOf(page), by = Object.fromEntries(rows.map(r => [r.order, r])), id = wid[sh.id], own = toks[TOKEN[sh.metal]], rowOf = k => by[id[k]];
        assert.equal(rows.length, 5, `${tag} ${sh.code}: five orders listed`);
        // P: six pieces, two on the sheet (filled, the sheet's colour), four Unknown SKU (rings in the sheet's colour: unknown metal takes the colour of the sheet whose list it is under)
        let p = rowOf('P'); assert.deepEqual(p.dots.map(d => d.ring), [false, false, true, true, true, true], `${tag} ${sh.code}: P filled first, then four rings`);
        for (const d of p.dots) assert.equal(d.ring ? d.border : d.bg, own, `${tag} ${sh.code}: P ${d.ring ? 'ring' : 'dot'} is the ${sh.code} sheet colour (${d.ring ? d.border : d.bg} vs ${own})`);
        assert.equal(p.group, '6 pieces, 4 not on a sheet yet', `${tag} ${sh.code}: P says it`);
        // Q: Paul's first order: one filled, one hollow Unknown SKU
        p = rowOf('Q'); assert.deepEqual(p.dots.map(d => d.ring), [false, true]); assert.equal(p.dots[1].border, own, `${tag} ${sh.code}: Q's Unknown SKU ring is the sheet's colour`); assert.equal(p.chip, 'Unknown SKU'); assert.equal(p.group, '2 pieces, 1 not on a sheet yet');
        // R: a piece of another metal that is not placed keeps ITS metal's colour; the Unknown SKU piece takes the sheet's
        p = rowOf('R'); assert.deepEqual(p.dots.map(d => d.ring), [false, true, true]); const off = toks[TOKEN[sh.off]];
        assert.equal(p.dots[0].bg, own); assert.equal(p.dots[1].border, off, `${tag} ${sh.code}: R's unplaced ${sh.off} piece is ${sh.off}, not ${sh.metal}`); assert.equal(p.dots[2].border, own);
        // S: the other piece is on a sheet of another metal that is not ready: that sheet's colour; the label says waiting, not "not on a sheet"
        p = rowOf('S'); assert.equal(p.dots[1].border, toks[TOKEN[sh.other.metal]], `${tag} ${sh.code}: S's piece on ${sh.other.code} Sheet 1 is ${sh.other.metal}`); assert.equal(p.group, '2 pieces, 1 waiting'); assert.match(p.dots[1].label, /on .* Sheet 1/i);
        // T: one line of two copies: copy 2 is the ring, its identity is exactly copy 2 of the line
        p = rowOf('T'); assert.deepEqual(p.dots.map(d => d.pool), [`${id.T}_1_1`, `${id.T}_1_2`], `${tag} ${sh.code}: T's two copies are told apart`); assert.deepEqual(p.dots.map(d => d.line), [`${id.T}_1`, `${id.T}_1`]);
        // never the issue red, never the pale grey of the filled unknown; every ring is a sheet token; every ring is a 2 px ring
        for (const r of rows) for (const d of r.dots) { if (d.ring) { assert.notEqual(d.border, red, `${tag} ${sh.code}: no ring is the issue red`); assert(Object.values(toks).includes(d.border), `${tag} ${sh.code}: a ring is a sheet colour (${d.border})`); assert.equal(d.bw, '2px'); assert.equal(d.bg, 'rgba(0, 0, 0, 0)', 'hollow'); } else assert(Object.values(toks).includes(d.bg), `${tag} ${sh.code}: a filled dot is a sheet colour (${d.bg})`); }
        assert(!(await page.evaluate(() => /lisDot|data-t=/.test(document.querySelector('.lisPanel').innerHTML))), `${tag}: the old dots are gone`);
        if (i === 0 || i === 1 || i === 4) await shot(page, `panel-${sh.code}-${width}`);
        await page.keyboard.press('Escape'); await sleep(400);
      }
      // a list about no one sheet: an unknown piece keeps a calm neutral ring; the same piece under a sheet's list takes that sheet's colour
      { await page.evaluate(() => {
          const d = document.createElement('div'); d.id = 'plain'; d.setAttribute('data-pd-scope', ''); d.style.cssText = 'position:fixed;left:20px;top:200px;background:#fff;padding:12px;z-index:5';
          d.innerHTML = PieceDots.html([{ ring: false, metal: 'gold' }, { ring: true, metal: '' }, { ring: true, metal: 'rose' }], { order: '4100000001' }) + '<br>' + PieceDots.html([{ ring: true, metal: '' }], { order: '4100000002', sheetMetal: 'rose' }); document.body.appendChild(d); });
        const rs = await page.evaluate(() => [...document.querySelectorAll('#plain .pdot')].map(d => { const cs = getComputedStyle(d); return [d.classList.contains('ring'), cs.backgroundColor, cs.borderTopColor]; }));
        assert.equal(rs[0][1], toks['--m-gold']); assert.equal(rs[1][2], neutral, `${tag}: an unknown piece in a list about no sheet has the calm neutral ring (${rs[1][2]} vs ${neutral})`); assert.notEqual(rs[1][2], red); assert.equal(rs[2][2], toks['--m-rose'], 'a known metal keeps its colour in any list');
        assert.equal(rs[3][2], toks['--m-rose'], `${tag}: the same unknown piece under a rose sheet's list is rose`); assert.notEqual(neutral, pale); await page.evaluate(() => document.getElementById('plain').remove()); }

      assert.deepEqual(errors, [], `${tag}: no page errors`); await context.close();
    }

    if (!only || only === '1') console.log('  ok 1 · every ring is the colour of its sheet (GF, RG, SS, 10K, 14K), unknown pieces take the list sheet\'s colour, neutral in a list about no sheet, never red');
    if (!only || only === '2') { for (const w of [1440, 390]) await hoverSuite(browser, w); console.log('  ok 2 · every dot shows its card in the pointer\'s own event, ready before the hover'); }
    if (!only || only === '3') { for (const w of [1440, 390]) await coldSuite(browser, w); console.log('  ok 3 · a cold dot shows a labelled spinner at once and swaps to the picture; no design: one calm line; Unknown SKU pieces are never rendered'); }
    if (!only || only === '4') { for (const w of [1440, 390]) { await redrawSuite(browser, w); await layerSuite(browser, w); } console.log('  ok 4 · the card survives redraws, no listener on a dot, edges and the dialog layer'); }
    if (!only || only === '5') { await scaleSuite(browser); console.log('  ok 5 · 200 rows: bounded cache, visible rows first, small idle cost, a gone list stops its queue'); }
    if (!only || only === '6') { for (const w of [1440, 390]) await clickSuite(browser, w); console.log('  ok 6 · a press on any dot (mouse, keyboard, touch) opens the order on exactly that piece; a press on the row outside the dots opens it as before'); }
    if (!only || only === '7') { await mutants(browser); console.log('  ok 7 · every mutant is caught'); }
    if (SHOTS && (!only || only === '9')) { for (const w of [1440, 390]) await shotsSuite(browser, w); console.log('  ok 9 · screenshots saved to ' + SHOTS); }
    if ((!only || only === '8') && process.env.PD_FULL !== '0') { await fullApp(browser);
      let caught = ''; try { await fullApp(browser, { from: 'if (opts.pick && r.key === key', to: 'if (false && opts.pick && r.key === key' }); } catch (e) { caught = String(e && e.message); }
      assert(/exactly that piece/.test(caught), `the real order window that ignores "pick" was caught by the full-app check (${caught.slice(0, 120)})`); console.log('    mutant caught: an order window that ignores the piece a dot asks for'); console.log('  ok 8 · the whole page: the real order window opens on the piece a dot stands for (mouse, keyboard, touch), from the panel and from a dialog'); }
  } finally { await browser.close(); }
})().catch(e => { console.error(e); process.exitCode = 1; });
