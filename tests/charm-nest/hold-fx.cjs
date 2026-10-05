// Browser test of the cinematic Hold and Release (charm-nest-hold-fx.js, window.OrderHoldFx), on a fixture page that has the
// sorter's own structure (a top bar with the tab strip, a stage with the sheet cards of two metals, each with its tabs, its
// picture and its waiting queue; the Orders view with its On hold pile) and no network. The steps it is fed are the ones the
// engine really emits (charm-nest-order-hold.js: two sheets lifted together, then each one removed, filled and made again),
// recorded as a list: 2 sheets, 4 pieces, one refill from a newer sheet and one from the waiting list; and a Release.
// Paul, 5 Oct 2026: "real-time detailed game-like cinematic step by step animation so the user can see the order moving and
// animating to the next sheet in Nest tab and returning the user to the On Hold tab to continue their work."
// What it holds to: the captions come in the order the steps came, in plain words ("pieces", never "lines"); each flight
// sets off from the element it should (the piece where it lies, the newer sheet's tab, the waiting queue, the order's card)
// and lands where the piece goes; Skip, Esc and a tab picked end it at once on the right screen; with reduced motion the
// captions still step through and nothing flies; the user ends on Orders > On hold with the order's card (or a note, for a
// release); nothing stays on the page; a click goes through the film while it plays and after; only transform, opacity and
// clip-path animate; the frames stay smooth; a stalled engine and the hard cap both end it.
//   node tests/charm-nest/hold-fx.cjs [playwright-core dir]      (PW_DIR=…, CHROMIUM=…; SHOTS=<dir> saves frames; ONLY=a,b picks scenes)
const fs = require('fs'), path = require('path'), vm = require('vm'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const FX = path.join(root, 'charm-nest-hold-fx.js'), MOTION = path.join(root, 'charm-nest-motion.js');
const SHOTS = process.env.SHOTS || '', ONLY = (process.env.ONLY || '').split(',').filter(Boolean);
const want = n => !ONLY.length || ONLY.includes(n);
const ok = [];
const pass = m => { ok.push(m); console.log('  ✓ ' + m); };

/* ── static: the file is real, wired into the page and into the public build ── */
{
  const src = fs.readFileSync(FX, 'utf8');
  new vm.Script(src, { filename: 'charm-nest-hold-fx.js' });
  const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8'), build = fs.readFileSync(path.join(root, 'scripts/build-public.cjs'), 'utf8');
  assert.match(html, /<script src="charm-nest-hold-fx\.js\?v=[^"]+"><\/script>/, 'the page loads charm-nest-hold-fx.js (with a ?v= tag)');
  assert.match(build, /"charm-nest-hold-fx\.js"/, 'scripts/build-public.cjs lists charm-nest-hold-fx.js');
  assert.ok(html.indexOf('charm-nest-hold-fx.js') > html.indexOf('charm-nest-motion.js'), 'it loads after Motion');
  assert.ok(!/sonnet|claude-/i.test(src), 'no model name in the file');
  pass('static: the file compiles, the page loads it after Motion, the public build lists it');
}

const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
let chromium;
try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser check was not run'); process.exit(0); }

/* ── the fixture page ── */
const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8').split('\n');
const i0 = html.findIndex(l => l.startsWith('#motionLayer,.motionLayer{')), i1 = html.findIndex((l, i) => i > i0 && l.startsWith('@keyframes mNoteBar'));
assert.ok(i0 > 0 && i1 > i0, 'the Motion rules are found in the page');
const MOTION_CSS = html.slice(i0, i1 + 1).join('\n');
const IMG = "data:image/svg+xml;utf8," + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="90" height="60"><rect width="90" height="60" fill="#f0e6cd"/><circle cx="45" cy="30" r="20" fill="#caa861"/></svg>');
const HEAD = `<!doctype html><meta charset="utf-8"><title>hold fx</title><style>
:root{--paper:#f3f0ea;--card:#fffefb;--ink:#1c1a17;--ink70:#5b554c;--ink45:#938c80;--line:#e4ddd0;--clay:#b0563f;--claySoft:#f4e3dc;--gold:#a9823f;--gold2:#caa861;--goldSoft:#f0e6cd;--goldLine:#e3d3a6;--sageSoft:#e5eddd;--velvet:#221f1b;--sans:system-ui,sans-serif;--serif:Georgia,serif;--mono:ui-monospace,monospace}
html,body{margin:0;height:100%}body{font:13px var(--sans);background:var(--paper);color:var(--ink)}
.topbar{position:fixed;top:0;left:0;right:0;height:56px;background:#2b2620;z-index:300;display:flex;align-items:center;padding:0 16px}
#modeSeg{display:flex;gap:6px}#modeSeg button{font:600 12px var(--sans);padding:6px 14px;border-radius:999px;border:1px solid #6b6254;background:#3a342c;color:#f3eee4}#modeSeg button.on{background:#caa861;color:#221f1b}
#stage{position:fixed;top:56px;left:0;right:0;bottom:0;overflow:auto;padding:18px;box-sizing:border-box}
.hidden{display:none!important}
#sheets{display:grid;grid-template-columns:1fr 1fr;gap:18px;align-items:start}
.sheetCard{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:10px 12px;position:relative}
.shHead{display:flex;gap:10px;align-items:center}.shHead .name{font:600 14px var(--serif)}
[data-r=tabs]{display:flex;gap:4px}[data-r=tabs] button{font:600 11px var(--sans);padding:3px 9px;border-radius:7px;border:1px solid var(--line);background:#fff}
.shPreviewWrap{position:relative;margin-top:8px}.shPreviewWrap canvas{display:block;width:100%;height:auto;border:1px solid var(--line)}
.pc{position:absolute;width:9%;aspect-ratio:1;border-radius:50%;background:#e6c97a;border:1px solid #a9823f;box-sizing:border-box}
[data-r=queue]{margin-top:8px;height:34px;border:1px dashed var(--line);border-radius:8px;display:flex;align-items:center;padding:0 10px;color:var(--ink70)}
#ordChips{display:flex;gap:8px;margin-bottom:12px}#ordChips button{font:600 12px var(--sans);padding:6px 12px;border-radius:999px;border:1px solid var(--line);background:#fff}#ordChips button.on{border-color:var(--gold2);background:var(--goldSoft)}
.ordCard{display:flex;gap:14px;align-items:center;background:var(--card);border:1px solid var(--line);border-radius:12px;padding:12px;margin-bottom:10px;max-width:760px}
.ordCard .rowMedia{width:90px;height:60px;border-radius:8px;overflow:hidden}.ordCard .rowMedia img{width:100%;height:100%;display:block}
.ordCard .meta{flex:1;display:flex;gap:10px;align-items:center}.ost{font:700 11px var(--sans);padding:3px 10px;border-radius:999px;background:var(--claySoft);color:#8a3a26}
.relHold{font:600 12px var(--sans);padding:6px 12px;border-radius:8px;border:1px solid var(--line);background:#fff}
#probeBtn{position:fixed;right:20px;bottom:20px;width:70px;height:32px;z-index:5}
${MOTION_CSS}
</style>`;
const BODY = `
<div class="topbar"><span id="modeSeg"><button data-mode="nest">Nest</button><button data-mode="orders" class="on">Orders</button><button data-mode="review">Review</button></span></div>
<div id="stage">
  <div id="sheets" class="hidden"></div>
  <div id="ordersView"><div id="ordChips"><button data-pile="new">New <b>5</b></button><button data-pile="hold">On hold <b>2</b></button></div><div id="ordItems"></div></div>
  <div id="reviewView" class="hidden">Review</div>
</div>
<button id="probeBtn" onclick="window.__clicks=(window.__clicks||0)+1">probe</button>
<script>
const IMG = ${JSON.stringify(IMG)};
const METAL_TAG = { gold: 'GF', silver: 'SS' };
const CW = 566, CH = 284;                                  // the picture's pixels: 2 per point
const charm = (id, poolId) => ({ id, poolId, widthPt: 24, heightPt: 24, outline: [[0, 0]], centerPt: [12, 12] });
const place = (id, cx, cy) => ({ id, cxPt: cx, cyPt: cy, angle: 0, wPt: 24, hPt: 24 });
const P = {
  sh1: { sheetId: 'sh1', metal: 'silver', label: 'SS Sheet 1', charms: [], placements: [], _view: { R: 0, k: 2 } },
  sh3: { sheetId: 'sh3', metal: 'silver', label: 'SS Sheet 3', charms: [], placements: [], _view: { R: 0, k: 2 } },
  sh2: { sheetId: 'sh2', metal: 'gold', label: 'GF Sheet 2', charms: [charm('c1', 'g1')], placements: [place('c1', 70, 50)], _view: { R: 0, k: 2 } }
};
function stockFor() { return { wPt: CW / 2, hPt: CH / 2 }; }
function allSheets() { return Object.values(P); }
window.SheetWin = { holdKit: { word: pg => pg.label } };
window.CharmNestPDF = {
  pathToCanvas(g, o, tx) { const c = tx(12, 12), e = tx(24, 12); g.moveTo(e[0], e[1]); g.arc(c[0], c[1], Math.hypot(e[0] - c[0], e[1] - c[1]), 0, 6.283); },
  drawCharm(g) { g.strokeStyle = '#a9823f'; g.lineWidth = 2; g.stroke(); },
  cutLinesOf() { return []; }
};
const S = { mode: 'orders', sheets: { silver: { cardEl: null, pages: [P.sh1, P.sh3], active: 0 }, gold: { cardEl: null, pages: [P.sh2], active: 0 } } };
function build(metal, title) {
  const pages = S.sheets[metal].pages, c = document.createElement('div'); c.className = 'sheetCard'; c.dataset.m = metal;
  c.innerHTML = '<div class="shHead"><span class="name">' + title + '</span><span data-r="tabs">' + pages.map((p, i) => '<button data-i="' + i + '">' + (i + 1) + '</button>').join('') + '</span></div><div class="shPreviewWrap"><canvas data-r="canvas" width="' + CW + '" height="' + CH + '"></canvas></div><div data-r="queue">Waiting: 3 orders</div>';
  const g = c.querySelector('canvas').getContext('2d'); g.fillStyle = '#faf7f0'; g.fillRect(0, 0, CW, CH); g.strokeStyle = '#e4ddd0'; for (let x = 0; x < CW; x += 40) { g.beginPath(); g.moveTo(x, 0); g.lineTo(x, CH); g.stroke(); }
  if (metal === 'gold') { g.fillStyle = '#e6c97a'; g.beginPath(); g.arc(140, 100, 24, 0, 6.283); g.fill(); }
  document.getElementById('sheets').appendChild(c); S.sheets[metal].cardEl = c; return c;
}
build('silver', 'SS sheets'); build('gold', 'GF sheets');
const wrap = S.sheets.silver.cardEl.querySelector('.shPreviewWrap');
for (const [id, l, t] of [['s1', 18, 28], ['s2', 52, 46]]) { const d = document.createElement('div'); d.className = 'pc'; d.dataset.poolId = id; d.style.left = l + '%'; d.style.top = t + '%'; wrap.appendChild(d); }
window.CN = { S, setMode(m) {
  S.mode = m; document.querySelectorAll('#modeSeg button').forEach(b => b.classList.toggle('on', b.dataset.mode === m));
  document.getElementById('sheets').classList.toggle('hidden', m !== 'nest'); document.getElementById('ordersView').classList.toggle('hidden', m !== 'orders'); document.getElementById('reviewView').classList.toggle('hidden', m !== 'review');
} };
document.querySelectorAll('#modeSeg button').forEach(b => b.addEventListener('click', () => CN.setMode(b.dataset.mode)));
/* the Orders view: the On hold pile draws a moment after it is asked for, as the real one does */
const HELD = new Set(['999001', '999002']); window.__pile = 'new';
function drawHold() {
  const box = document.getElementById('ordItems'); box.innerHTML = '';
  document.querySelectorAll('#ordChips button').forEach(b => b.classList.toggle('on', b.dataset.pile === __pile));
  document.querySelector('#ordChips [data-pile="hold"] b').textContent = String(HELD.size);
  if (__pile !== 'hold') return;
  for (const rid of [...HELD].reverse()) {
    const c = document.createElement('div'); c.className = 'ordCard'; c.dataset.rid = rid; c.dataset.key = rid + ':1'; c.dataset.mkey = 'ord:' + rid + ':1';
    c.innerHTML = '<div class="rowMedia"><img alt="" src="' + IMG + '"></div><div class="meta"><b>Order ' + rid + '</b><span class="ost">HELD</span></div><button class="relHold">Release</button>'; box.appendChild(c);
  }
}
window.Orders = { showPile(pile) { __pile = pile; document.querySelectorAll('#ordChips button').forEach(b => b.classList.toggle('on', b.dataset.pile === pile)); setTimeout(drawHold, 120); } };
window.sceneOrders = () => { CN.setMode('orders'); __pile = 'hold'; drawHold(); };
window.engine = {
  holds(rid) { HELD.add(rid); }, releases(rid) { HELD.delete(rid); },
  takeOffGold() { P.sh2.charms = []; P.sh2.placements = []; },
  takeOffSilver() { document.querySelectorAll('.pc').forEach(n => n.remove()); },
  fillsGold() { P.sh2.charms.push(charm('c2', 'w1'), charm('c3', 'w2')); P.sh2.placements.push(place('c2', 80, 50), place('c3', 172, 82)); },
  newSheet() { P.sh4 = { sheetId: 'sh4', metal: 'silver', label: 'SS Sheet 4', charms: [], placements: [], _view: { R: 0, k: 2 } }; S.sheets.silver.pages.push(P.sh4); const t = S.sheets.silver.cardEl.querySelector('[data-r=tabs]'), b = document.createElement('button'); b.dataset.i = '2'; b.textContent = '4'; t.appendChild(b); }
};
window.holdPlan = rid => ({ rid, label: 'Order ' + rid, pieces: [['s1', 'sh1', 'SS Sheet 1'], ['s2', 'sh1', 'SS Sheet 1'], ['g1', 'sh2', 'GF Sheet 2'], ['g2', 'sh2', 'GF Sheet 2']].map(a => ({ poolId: a[0], sheetId: a[1], sheetLabel: a[2], state: 'onSheet' })), sheets: [{ sheetId: 'sh1', label: 'SS Sheet 1' }, { sheetId: 'sh2', label: 'GF Sheet 2' }] });
/* NestFocus as the page has it: opens a sheet and hands back its card; records how it was asked */
window.__nf = [];
const NF = { open(id, o) { __nf.push({ id, poolIds: o && o.poolIds, noCaption: o && o.noCaption }); const pg = P[id], card = S.sheets[pg.metal].cardEl; return Promise.resolve({ card, close() {} }); }, isOpen() { return false; }, close() {}, restore() {} };
window.setNF = on => { if (on) window.NestFocus = NF; else delete window.NestFocus; };
/* the feed: pushes the steps as a recorded list, doing what the engine would have done to the page just before each one */
window.feed = (steps, gap, fx, hooks) => new Promise(resolve => {
  let i = 0;
  const next = () => {
    if (i >= steps.length) { fx.finish(); return resolve(); }
    const s = Object.assign({ at: Date.now() }, steps[i++]); if (hooks && hooks[s.type]) hooks[s.type](s);
    fx.push(s); if (gap > 0) setTimeout(next, gap); else next();
  };
  next();
});
window.HOOKS = {
  hold: { held: s => engine.holds(s.rid), fillPlaced: s => { if (s.sheetId === 'sh2') engine.fillsGold(); } },
  release: { released: s => engine.releases(s.rid) }
};
/* what the page saw */
window.__anim = new Set(); window.__ghosts = [];
{ const oa = Element.prototype.animate; Element.prototype.animate = function (f, o) { try { for (const k of (Array.isArray(f) ? f : [f])) for (const p of Object.keys(k)) __anim.add(p); } catch (_) {} return oa.call(this, f, o); }; }
new MutationObserver(ms => { for (const m of ms) for (const n of m.addedNodes) if (n.classList && n.classList.contains('mGhost')) __ghosts.push({ key: n.dataset.hfx || '', canvas: !!n.querySelector('canvas'), img: !!n.querySelector('img') }); }).observe(document.body, { childList: true, subtree: true });
window.frames = { t: [], on: false, start() { this.t = []; this.on = true; let last = 0; const f = now => { if (!this.on) return; if (last) this.t.push(now - last); last = now; requestAnimationFrame(f); }; requestAnimationFrame(f); },
  stop() { this.on = false; const a = this.t.slice().sort((x, y) => x - y), q = p => a.length ? a[Math.min(a.length - 1, Math.floor(a.length * p))] : 0; return { n: a.length, p50: q(.5), p90: q(.9), p95: q(.95), max: a[a.length - 1] || 0 }; } };
</script>`;
const PAGE = HEAD + BODY;

/* ── the recorded steps (what charm-nest-order-hold.js and the release really say, in the order they say it) ── */
const R = (poolId, cx, cy) => ({ x: cx - 12, y: cy - 12, w: 24, h: 24, cx, cy, angle: 0, poolId });
const SHEET = { wPt: 283, hPt: 142 };
const holdSteps = rid => [
  { type: 'start', rid, sheets: ['sh1', 'sh2'] },
  { type: 'sheetBegin', sheetId: 'sh1', label: 'SS Sheet 1' },
  { type: 'lift', sheetId: 'sh1', label: 'SS Sheet 1', poolIds: ['s1', 's2'], rects: [R('s2', 162, 82), R('s1', 62, 52)], sheet: SHEET },   // (the engine lists them in the sheet's own order, not the pool ids')
  { type: 'sheetBegin', sheetId: 'sh2', label: 'GF Sheet 2' },
  { type: 'lift', sheetId: 'sh2', label: 'GF Sheet 2', poolIds: ['g1', 'g2'], rects: [R('g1', 70, 50), R('g2', 162, 82)], sheet: SHEET },
  { type: 'removed', sheetId: 'sh1', removed: 2 },
  { type: 'fillBegin', sheetId: 'sh1', spots: 2 },
  { type: 'fillFrom', toSheetId: 'sh1', fromSheetId: 'sh3', rid: '111', poolIds: ['n1', 'n2'], source: 'newerSheet' },
  { type: 'fillPlaced', sheetId: 'sh1', rid: '111', poolIds: ['n1', 'n2'] },
  { type: 'qr', sheetId: 'sh1' },
  { type: 'sheetDone', sheetId: 'sh1', charmCount: 25, density: 0.742 },
  { type: 'removed', sheetId: 'sh2', removed: 2 },
  { type: 'fillBegin', sheetId: 'sh2', spots: 2 },
  { type: 'fillFrom', toSheetId: 'sh2', fromSheetId: null, rid: '222', poolIds: ['w1', 'w2'], source: 'waiting' },
  { type: 'fillPlaced', sheetId: 'sh2', rid: '222', poolIds: ['w1', 'w2'] },
  { type: 'qr', sheetId: 'sh2' },
  { type: 'sheetDone', sheetId: 'sh2', charmCount: 31, density: 0.81 },
  { type: 'held', rid },
  { type: 'done' }
];
const HOLD_CAPTIONS = rid => [
  `Putting order ${rid} on hold`, 'SS Sheet 1', 'Taking the pieces off SS Sheet 1', 'GF Sheet 2', 'Taking the pieces off GF Sheet 2',
  'Filling the empty spots', 'Filling the empty spots from SS Sheet 3', 'Placed on SS Sheet 1', 'Remaking QR labels', 'SS Sheet 1 is done',
  'Filling the empty spots', 'Filling the empty spots from the waiting list', 'Placed on GF Sheet 2', 'Remaking QR labels', 'GF Sheet 2 is done',
  `Order ${rid} is on hold`
];
const releaseSteps = (rid, newSheet) => [
  { type: 'start', rid },
  { type: 'queued', rid, front: true },
  newSheet ? { type: 'target', sheetId: 'sh4', label: 'SS Sheet 4', spot: null, newSheet: true } : { type: 'target', sheetId: 'sh1', label: 'SS Sheet 1', spot: R('', 212, 72), newSheet: false },
  { type: 'flight', toSheetId: newSheet ? 'sh4' : 'sh1', poolIds: ['r1', 'r2'] },
  { type: 'placed', sheetId: newSheet ? 'sh4' : 'sh1', poolIds: ['r1', 'r2'] },
  { type: 'qr', sheetId: newSheet ? 'sh4' : 'sh1' },
  { type: 'released', rid },
  { type: 'done' }
];
const RELEASE_CAPTIONS = (rid, label) => [`Releasing order ${rid}`, 'First in line, ahead of new orders', `Finding a spot on ${label}`, `Placing the pieces on ${label}`, `Placed on ${label}`, 'Remaking QR labels', `Order ${rid} is released`];

/* ── helpers ── */
const sleep = ms => new Promise(r => setTimeout(r, ms));
const near = (a, b, tol, msg) => assert.ok(a && b && Math.abs(a.x - b.x) <= tol && Math.abs(a.y - b.y) <= tol, `${msg}: (${a && Math.round(a.x)}, ${a && Math.round(a.y)}) is not near (${b && Math.round(b.x)}, ${b && Math.round(b.y)})`);
const ALLOWED = new Set(['transform', 'opacity', 'clipPath', 'offset', 'easing', 'composite']);

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  if (SHOTS) fs.mkdirSync(SHOTS, { recursive: true });
  let page, perr, warns;
  const open = async (o = {}) => {
    if (page) await page.close().catch(() => {});
    const ctx = await browser.newContext({ viewport: { width: 1280, height: 900 }, reducedMotion: o.reduced ? 'reduce' : 'no-preference' });
    page = await ctx.newPage(); perr = []; warns = [];
    page.on('pageerror', e => perr.push(e.message));
    page.on('console', m => { if (/hold fx/i.test(m.text())) warns.push(m.text()); });
    await page.setContent(PAGE); await page.addScriptTag({ path: MOTION }); await page.addScriptTag({ path: FX });
    await page.evaluate(nf => { setNF(nf); sceneOrders(); }, !!o.nf);
    assert.equal(await page.evaluate(() => typeof OrderHoldFx.playHold + typeof OrderHoldFx.playRelease + typeof OrderHoldFx.returnToOnHold), 'functionfunctionfunction');
  };
  const shot = async name => { if (SHOTS) await page.screenshot({ path: path.join(SHOTS, name + '.png') }); };
  /** Starts a film and feeds it; resolves with what `done` said. */
  const play = async (kind, rid, steps, gap, o = {}) => {
    await page.evaluate(([kind, rid, o, plan]) => {
      window.__done = null; const src = Object.assign({}, o); if (plan) src.plan = holdPlan(rid);
      window.__fx = kind === 'hold' ? OrderHoldFx.playHold(rid, src) : OrderHoldFx.playRelease(rid, src); __fx.done.then(r => { window.__done = r; });
    }, [kind, rid, o.source || {}, kind === 'hold']);
    if (o.before) await page.evaluate(o.before);
    if (steps) await page.evaluate(([steps, gap, kind]) => { feed(steps, gap, __fx, HOOKS[kind]); }, [steps, gap, kind]);
  };
  const finished = (ms = 120000) => page.waitForFunction(() => window.__done, null, { timeout: ms, polling: 50 }).then(h => h.jsonValue());
  const captions = () => page.evaluate(() => OrderHoldFx.events().filter(e => e.ev === 'caption'));
  const flights = () => page.evaluate(() => OrderHoldFx.events().filter(e => e.ev === 'flight'));
  const state = () => page.evaluate(() => {
    const card = [...document.querySelectorAll('#ordItems [data-rid]')], hide = [...document.querySelectorAll('[style*="visibility"]')].filter(n => n.style.visibility === 'hidden');
    return { mode: CN.S.mode, pile: __pile, ordersShown: !document.getElementById('ordersView').classList.contains('hidden'), cards: card.map(c => c.dataset.rid), active: OrderHoldFx.active(),
      layer: !!document.getElementById('hfxLayer'), ghosts: document.querySelectorAll('.mGhost').length, hfx: document.querySelectorAll('[class*="hfx"]').length, lit: document.querySelectorAll('.hfxLit,.hfxDim').length, hidden: hide.length };
  });
  const settledAt = async (rid, mode = 'orders') => { await page.waitForFunction(rid => { const c = document.querySelector('#ordItems [data-rid="' + rid + '"]'); return !!c && getComputedStyle(c).visibility !== 'hidden'; }, rid, { timeout: 8000 }).catch(() => {}); return state(); };
  const clean = (s, msg) => assert.deepEqual([s.layer, s.ghosts, s.hfx, s.lit, s.hidden, s.active], [false, 0, 0, 0, 0, false], `${msg}: nothing is left on screen: ${JSON.stringify(s)}`);
  const noWarn = msg => { assert.deepEqual(perr, [], msg + ': no page error'); assert.deepEqual(warns, [], msg + ': no step failed quietly'); };
  const plain = (caps, msg) => assert.ok(caps.every(c => !/\blines\b|line items?/i.test(c.main + ' ' + c.small)), msg + ': the words are "pieces", never "lines": ' + JSON.stringify(caps.map(c => c.main + ' ' + c.small)));
  const watch = async list => { for (const w of list) { await page.waitForFunction(re => OrderHoldFx.events().some(e => e.ev === 'caption' && new RegExp(re).test(e.main)), w.re, { timeout: 90000, polling: 30 }); await sleep(w.after); await w.do(); } };
  /** Where things are in the Nest tab, measured after a film (the layout is still). */
  const expected = () => page.evaluate(() => {
    CN.setMode('nest'); const q = s => document.querySelector(s), c = r => ({ x: r.left + r.width / 2, y: r.top + r.height / 2 });
    const cv = m => q('.sheetCard[data-m="' + m + '"] canvas').getBoundingClientRect(), pt = (m, x, y) => { const r = cv(m); return { x: r.left + x * 2 / 566 * r.width, y: r.top + y * 2 / 284 * r.height }; };
    const dom = id => { const n = q('.pc[data-pool-id="' + id + '"]'); return n ? c(n.getBoundingClientRect()) : null; };
    return { s1: dom('s1'), s2: dom('s2'), s1pt: pt('silver', 62, 52), s2pt: pt('silver', 162, 82), g1: pt('gold', 70, 50), g2: pt('gold', 162, 82), w1: pt('gold', 80, 50), w2: pt('gold', 172, 82), spot: pt('silver', 212, 72),
      tab3: c(q('.sheetCard[data-m="silver"] [data-r=tabs] [data-i="1"]').getBoundingClientRect()), queue: c(q('.sheetCard[data-m="gold"] [data-r=queue]').getBoundingClientRect()), tabs: c(q('.sheetCard[data-m="silver"] [data-r=tabs]').getBoundingClientRect()),
      spotW: (24 * 2 / 566) * cv('silver').width, canvas: cv('silver').toJSON() };
  });
  const byKey = (fl, k) => fl.find(f => f.key === k);
  const failures = [];
  const scene = async (name, f) => { if (!want(name)) return; const t0 = Date.now(); try { await f(); console.log(`    (${name}: ${((Date.now() - t0) / 1000).toFixed(1)} s)`); } catch (e) { failures.push(name); console.log('  ✗ ' + name + '\n' + (e && e.stack || e)); if (process.env.DEBUG && page) console.log('page errors:', perr, 'warnings:', warns), console.log((await page.evaluate(() => OrderHoldFx.events().filter(x => !/^(push|beat)$/.test(x.ev)).map(x => JSON.stringify(x)).join('\n')).catch(() => '')).slice(-6000)); } };

  try {
    /* 1 · Hold, steps arriving one by one (the engine's pace): the story in order, the flights from the right places */
    await scene('hold-paced', async () => {
      const RID = '4174601819';
      await open({ nf: true });
      const watched = SHOTS ? watch([
        { re: /^Taking the pieces off SS Sheet 1/, after: 1000, do: () => shot('h4-hold-1-lift') },
        { re: /^Filling the empty spots from SS Sheet 3/, after: 800, do: () => shot('h4-hold-2-fill-from-newer-sheet') },
        { re: /^Remaking QR labels/, after: 550, do: () => shot('h4-hold-3-qr-label') },
        { re: /^Filling the empty spots from the waiting list/, after: 800, do: () => shot('h4-hold-4-fill-from-waiting-list') },
        { re: /is on hold$/, after: 500, do: () => shot('h4-hold-5-held') }
      ]) : Promise.resolve();
      await play('hold', RID, holdSteps(RID), 160, { before: () => { engine.takeOffGold(); } });   // (the engine has taken the gold pieces off before the film shows them)
      // while it plays: the page is under it, a press goes through, and the first thing it did was to bring the Nest tab
      await page.waitForFunction(() => CN.S.mode === 'nest', null, { timeout: 8000 });
      await page.waitForFunction(() => OrderHoldFx.events().some(e => e.ev === 'caption' && /^Taking the pieces off SS Sheet 1/.test(e.main)), null, { timeout: 30000 });
      await sleep(350);
      const over = await page.evaluate(() => { const n = document.elementFromPoint(640, 450); return { inLayer: !!(n && n.closest('#hfxLayer')), layerPe: getComputedStyle(document.getElementById('hfxLayer')).pointerEvents }; });
      assert.deepEqual(over, { inLayer: false, layerPe: 'none' }, 'the film never takes a press');
      await page.click('#probeBtn', { timeout: 4000 });
      assert.equal(await page.evaluate(() => window.__clicks), 1, 'a click went through the film to the page');
      await watched;
      const done = await finished();
      assert.equal(done.ok, true); assert.equal(done.skipped, false); assert.equal(done.timedOut, false); assert.equal(done.steps, holdSteps(RID).length);
      const caps = await captions();
      assert.deepEqual(caps.map(c => c.main), HOLD_CAPTIONS(RID), 'the captions come in the order of the steps');
      plain(caps, 'hold');
      assert.equal(caps[2].small, '2 pieces', 'a lift says how many pieces'); assert.equal(caps[5].small, '2 spots · SS Sheet 1'); assert.equal(caps[9].small, '25 pieces · 74% full', 'a sheet says its count and density');
      const nf = await page.evaluate(() => __nf);
      assert.ok(nf.length >= 2 && nf.every(c => Array.isArray(c.poolIds) && c.poolIds.length === 0 && c.noCaption === true), 'NestFocus is only asked to take the sheet into the light (no pieces hidden): ' + JSON.stringify(nf));
      assert.ok(['sh1', 'sh2'].every(id => nf.some(c => c.id === id)));
      // the end: Orders > On hold, the order's card there, nothing left
      const s = await settledAt(RID);
      assert.equal(s.mode, 'orders'); assert.equal(s.pile, 'hold'); assert.ok(s.ordersShown); assert.ok(s.cards.includes(RID), 'the order\'s card is in On hold: ' + s.cards);
      const vis = await page.evaluate(rid => { const r = document.querySelector('#ordItems [data-rid="' + rid + '"]').getBoundingClientRect(); return r.top >= 56 && r.bottom <= innerHeight && r.width > 100; }, RID); assert.ok(vis, 'the card is in view');
      await shot('h4-hold-6-home');
      await sleep(400); clean(await state(), 'after the hold');
      await page.click('#probeBtn', { timeout: 4000 }); assert.equal(await page.evaluate(() => window.__clicks), 2, 'a click goes through after the film too');
      // the flights: each from where its piece lay (SS from the card's own elements, GF from the page's data that the engine had already emptied), to the tray; then the spots
      const fl = await flights(), exp = await expected();
      assert.deepEqual(fl.map(f => f.key), ['lift:sh1:s1', 'lift:sh1:s2', 'lift:sh2:g1', 'lift:sh2:g2', 'fill:sh1:n1', 'fill:sh1:n2', 'fill:sh2:w1', 'fill:sh2:w2', 'land:' + RID], 'the flights are the steps\' pieces, in order');
      near(byKey(fl, 'lift:sh1:s1').from, exp.s1, 4, 'SS piece 1 lifts from its element'); near(byKey(fl, 'lift:sh1:s2').from, exp.s2, 4, 'SS piece 2 lifts from its element');
      near(byKey(fl, 'lift:sh2:g1').from, exp.g1, 4, 'GF piece 1 lifts from where the page had it'); near(byKey(fl, 'lift:sh2:g2').from, exp.g2, 4, 'GF piece 2 lifts from where the step says');
      const tray = byKey(fl, 'lift:sh1:s1').to; assert.ok(tray.x > 1000 && tray.y < 130, 'the pieces fly to the small On hold marker at the top right of the stage: ' + JSON.stringify(tray));
      for (const k of ['lift:sh1:s2', 'lift:sh2:g1', 'lift:sh2:g2']) near(byKey(fl, k).to, tray, 2, k + ' flies to the same marker');
      near(byKey(fl, 'fill:sh1:n1').from, exp.tab3, 3, 'a refill from a newer sheet comes from that sheet\'s tab'); near(byKey(fl, 'fill:sh1:n2').from, exp.tab3, 3, 'the second too');
      near(byKey(fl, 'fill:sh2:w1').from, exp.queue, 3, 'a refill from the waiting list comes from the card\'s queue'); near(byKey(fl, 'fill:sh2:w2').from, exp.queue, 3, 'the second too');
      near(byKey(fl, 'fill:sh1:n1').to, exp.s1, 6, 'the first refill lands in the first emptied spot'); near(byKey(fl, 'fill:sh1:n2').to, exp.s2, 6, 'the second in the second');
      near(byKey(fl, 'fill:sh2:w1').to, exp.g1, 6, 'GF refill 1 lands in the first emptied spot'); near(byKey(fl, 'fill:sh2:w2').to, exp.g2, 6, 'GF refill 2 in the second');
      const set = await page.evaluate(() => OrderHoldFx.events().filter(e => e.ev === 'settle'));
      assert.deepEqual(set.map(s => s.pool), ['w1', 'w2'], 'chips settle where the sheet truly put the pieces (read from the page)');
      near(set[0], exp.w1, 4, 'w1 settles on its true place'); near(set[1], exp.w2, 4, 'w2 settles on its true place');
      near(byKey(fl, 'land:' + RID).from, tray, 6, 'the order\'s card comes home from the marker the pieces went to');
      const gh = await page.evaluate(() => __ghosts);
      assert.ok(gh.filter(g => g.key === 'lift:sh2:g1')[0].canvas, 'a piece the page draws flies as its own drawing'); assert.ok(gh.filter(g => g.key.startsWith('lift:sh1'))[0], 'ghosts carry the flight key');
      await page.click('#modeSeg [data-mode="review"]'); assert.equal(await page.evaluate(() => CN.S.mode), 'review', 'the tabs answer at once');
      noWarn('hold-paced');
      pass('Hold, paced: captions in order, every flight from the right element to the right spot, home on Orders > On hold, nothing left, clicks go through');
    });

    /* 2 · Hold, every step arriving at once (an engine faster than the film), frames measured, animated properties recorded */
    await scene('hold-fast', async () => {
      const RID = '4174601820';
      await open();
      await page.evaluate(() => { frames.start(); __anim.clear(); });
      await play('hold', RID, holdSteps(RID), 0, { before: () => { engine.takeOffGold(); engine.takeOffSilver(); } });   // (all four pieces are already off when the film begins)
      const done = await finished(); const f = await page.evaluate(() => frames.stop());
      assert.equal(done.ok, true); assert.equal(done.skipped, false);
      const caps = await captions(); assert.deepEqual(caps.map(c => c.main), HOLD_CAPTIONS(RID), 'a fast engine is still shown in order');
      const s = await settledAt(RID); assert.equal(s.mode, 'orders'); assert.equal(s.pile, 'hold'); assert.ok(s.cards.includes(RID));
      await sleep(400); clean(await state(), 'after the fast hold');
      const fl = await flights(), exp = await expected();
      assert.equal(fl.filter(x => x.key.startsWith('lift:')).length, 4);
      near(byKey(fl, 'lift:sh1:s1').from, exp.s1pt, 5, 'SS piece 1 lifts from its step rect (matched by pool id, though listed second)'); near(byKey(fl, 'lift:sh1:s2').from, exp.s2pt, 5, 'SS piece 2 from its step rect');
      near(byKey(fl, 'lift:sh2:g1').from, exp.g1, 5, 'GF piece 1 from what was read before the engine took it off');
      const anim = await page.evaluate(() => [...__anim]);
      const bad = anim.filter(p => !ALLOWED.has(p)); assert.deepEqual(bad, [], 'only transform, opacity and clip-path animate: ' + anim);
      assert.ok(f.n > 40, 'frames were measured: ' + JSON.stringify(f));
      console.log(`    frames: n=${f.n} p50=${f.p50.toFixed(1)} p90=${f.p90.toFixed(1)} p95=${f.p95.toFixed(1)} max=${f.max.toFixed(1)} ms; animated: ${anim.join(',')}`);
      assert.ok(f.p50 < 25 && f.p90 < 50, 'the frames stay smooth: ' + JSON.stringify(f));
      if (f.p95 > 50 || f.max > 200) console.log('    ! slow frames at the tail (a loaded machine?): ' + JSON.stringify(f));
      noWarn('hold-fast');
      pass('Hold, all steps at once: still in order, flights from the step rects matched by pool id, only transform/opacity/clip-path, smooth frames, clean');
    });

    /* 3 · Release: the order lifts out of its On hold card, flies to the next spot on the sheet, back to On hold with a note */
    await scene('release-paced', async () => {
      const RID = '555000';
      await open();
      await page.evaluate(rid => { engine.holds(rid); sceneOrders(); }, RID);
      const thumb = await page.evaluate(rid => { const r = document.querySelector('#ordItems [data-rid="' + rid + '"] .rowMedia').getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2 }; }, RID);
      const watched = SHOTS ? watch([
        { re: /^Releasing order/, after: 700, do: () => shot('h4-release-1-lifts-out') },
        { re: /^Placing the pieces on/, after: 800, do: () => shot('h4-release-2-flies-to-sheet') },
        { re: /^Placed on/, after: 500, do: () => shot('h4-release-3-settles') }
      ]) : Promise.resolve();
      await play('release', RID, releaseSteps(RID, false), 200);
      await watched; const done = await finished();
      assert.equal(done.ok, true); assert.equal(done.steps, 8);
      const caps = await captions(); assert.deepEqual(caps.map(c => c.main), RELEASE_CAPTIONS(RID, 'SS Sheet 1'), 'the release is told in the order of its steps'); plain(caps, 'release');
      await sleep(500); const s = await state(); assert.equal(s.mode, 'orders'); assert.equal(s.pile, 'hold'); assert.ok(!s.cards.includes(RID), 'a released order is no longer in On hold');
      const note = await page.evaluate(() => (document.querySelector('.mNote .mNoteT') || {}).textContent || '');
      assert.match(note, /Order 555000 is back in line/); assert.match(note, /SS Sheet 1/, 'the note says where it went: ' + note);
      await shot('h4-release-4-home');
      await page.evaluate(() => document.querySelectorAll('.mNote').forEach(n => n.remove()));
      await sleep(300); clean(await state(), 'after the release');
      const fl = await flights(), exp = await expected();
      assert.deepEqual(fl.map(f => f.key), ['coin:' + RID, 'place:sh1:r1', 'place:sh1:r2']);
      near(fl[0].from, { x: thumb.x, y: thumb.y - 42 }, 4, 'the design rises from the order\'s own card'); const tray = fl[0].to;
      assert.ok(tray.x > 1000 && tray.y < 130, 'and flies into the "Next in line" marker: ' + JSON.stringify(tray));
      near(fl[1].from, tray, 3, 'the pieces leave that marker'); near(fl[2].from, tray, 3, 'both');
      near(fl[1].to, exp.spot, 5, 'the first piece lands on the target spot the step names'); near(fl[2].to, { x: exp.spot.x + 14, y: exp.spot.y + 10 }, 5, 'the second a little beside it');
      noWarn('release-paced');
      pass('Release: the card\'s design flies out to the marker, the pieces fly onto the sheet, back to On hold with a note, nothing left');
    });

    /* 4 · Release onto a new sheet, steps all at once */
    await scene('release-new-sheet', async () => {
      const RID = '555001';
      await open();
      await page.evaluate(rid => { engine.holds(rid); sceneOrders(); engine.newSheet(); }, RID);
      await page.evaluate(() => { __anim.clear(); });
      const seenNew = page.waitForSelector('.hfxNew', { timeout: 60000 }).then(h => h.textContent());
      await play('release', RID, releaseSteps(RID, true), 0);
      const tag = await seenNew; assert.equal(tag, 'New sheet', 'a new sheet is announced');
      const done = await finished(); assert.equal(done.ok, true);
      const caps = await captions(); assert.deepEqual(caps.map(c => c.main), RELEASE_CAPTIONS(RID, 'SS Sheet 4'));
      assert.equal(caps[2].small, 'a new sheet');
      const bad = (await page.evaluate(() => [...__anim])).filter(p => !ALLOWED.has(p)); assert.deepEqual(bad, [], 'only transform, opacity and clip-path animate: ' + bad);
      await sleep(500); const s = await state(); assert.equal(s.mode, 'orders'); assert.equal(s.pile, 'hold'); assert.ok(await page.evaluate(() => !!document.querySelector('.mNote')), 'a note says where it went');
      await page.evaluate(() => document.querySelectorAll('.mNote').forEach(n => n.remove())); await sleep(300); clean(await state(), 'after the new-sheet release'); noWarn('release-new-sheet');
      pass('Release onto a new sheet: "New sheet" shown, the story in order, only transform/opacity/clip-path, back on Orders > On hold');
    });

    /* 5 · Skip (the call, the key, the small button): the final state at once, on the right screen */
    for (const how of ['call', 'esc', 'button']) await scene('skip-' + how, async () => {
      const RID = '41746018' + how.length;
      await open();
      await play('hold', RID, holdSteps(RID), 0, { before: () => { engine.takeOffGold(); } });
      await page.waitForFunction(() => OrderHoldFx.events().some(e => e.ev === 'caption' && /^Taking the pieces off/.test(e.main)), null, { timeout: 30000 });
      await sleep(300); const t0 = Date.now();
      if (how === 'call') await page.evaluate(() => __fx.skip()); else if (how === 'esc') await page.keyboard.press('Escape'); else await page.click('.hfxSkip', { timeout: 4000 });
      const done = await finished(8000); const took = Date.now() - t0;
      assert.equal(done.skipped, true); assert.equal(done.ok, true); assert.ok(took < 2500, 'skipping ends the film at once (' + took + ' ms)');
      const s = await settledAt(RID); assert.equal(s.mode, 'orders'); assert.equal(s.pile, 'hold'); assert.ok(s.cards.includes(RID), 'the order is in On hold after a skip: ' + s.cards);
      await sleep(300); clean(await state(), 'after a skip (' + how + ')'); noWarn('skip-' + how);
      pass(`Skip (${how}): ended in ${took} ms on Orders > On hold with the order's card, nothing left`);
    });

    /* 6 · reduced motion: the captions still step through, nothing flies */
    await scene('reduced', async () => {
      const RID = '4174601830';
      await open({ reduced: true, nf: true });
      assert.equal(await page.evaluate(() => Motion.reduced()), true);
      await page.evaluate(() => { __anim.clear(); });
      await play('hold', RID, holdSteps(RID), 0, { before: () => { engine.takeOffGold(); } });
      const done = await finished(); assert.equal(done.ok, true); assert.equal(done.skipped, false);
      const caps = await captions(); assert.deepEqual(caps.map(c => c.main), HOLD_CAPTIONS(RID), 'the captions still step through');
      assert.deepEqual(await flights(), [], 'no flights');
      assert.deepEqual(await page.evaluate(() => __ghosts), [], 'no flying copies were made');
      const anim = await page.evaluate(() => [...__anim]); assert.ok(!anim.includes('transform') && !anim.includes('clipPath'), 'nothing moves or wipes, it only fades: ' + anim);
      const s = await settledAt(RID); assert.equal(s.mode, 'orders'); assert.equal(s.pile, 'hold'); assert.ok(s.cards.includes(RID));
      await sleep(300); clean(await state(), 'after the reduced-motion hold'); noWarn('reduced');
      pass('Reduced motion: the captions step through, no flights, only fades, home on Orders > On hold');
    });

    /* 7 · a tab picked ends the film where the person went */
    await scene('tab-pick', async () => {
      const RID = '4174601840';
      await open();
      await play('hold', RID, holdSteps(RID), 0, { before: () => { engine.takeOffGold(); } });
      await page.waitForFunction(() => CN.S.mode === 'nest', null, { timeout: 8000 }); await sleep(900);
      await page.click('#modeSeg [data-mode="review"]');
      const done = await finished(8000); assert.equal(done.skipped, false); assert.equal(done.ok, true);
      await sleep(1800); const s = await state(); assert.equal(s.mode, 'review', 'the person stays where they went'); clean(s, 'after a tab was picked'); noWarn('tab-pick');
      pass('A tab picked during the film: it ends there, the person stays on the tab they chose, nothing left');
    });

    /* 8 · the engine stops with an error: said plainly, then home */
    await scene('error', async () => {
      const RID = '4174601850';
      await open();
      const steps = holdSteps(RID).slice(0, 3).concat([{ type: 'error', message: 'Another change is still running; nothing more was taken off.', rid: RID, sheetId: 'sh1' }]);
      await play('hold', RID, steps, 0, { before: () => { engine.takeOffGold(); } });
      const done = await finished(); assert.equal(done.ok, false); assert.match(done.error, /Another change is still running/);
      const caps = await captions(); const last = caps[caps.length - 1]; assert.equal(last.main, 'Stopped on SS Sheet 1'); assert.match(last.small, /Another change/);
      const s = await state(); assert.equal(s.mode, 'orders'); assert.equal(s.pile, 'hold'); await sleep(300); clean(await state(), 'after an error'); noWarn('error');
      pass('An error step: "Stopped on SS Sheet 1" with the reason, then back on Orders > On hold');
    });

    /* 9 · the safety timers: a run that never says anything, a run that goes quiet */
    await scene('safety', async () => {
      await open();
      const t0 = Date.now(); await play('hold', '4174601860', null, 0, { source: { safetyMs: 2500 } });
      const done = await finished(15000); assert.equal(done.timedOut, true); assert.ok(Date.now() - t0 < 9000, 'the hard cap ends the film');
      const s = await state(); assert.equal(s.mode, 'orders'); assert.equal(s.pile, 'hold'); await sleep(300); clean(await state(), 'after the hard cap');
      noWarn('safety');
      pass('The hard cap ends a film that never gets a step, and the person is put back on Orders > On hold');
    });

    /* 9b · a stalled engine: a small labelled spinner, then the film ends */
    await scene('stall', async () => {
      await open();
      await page.evaluate(() => { window.__done = null; window.__fx = OrderHoldFx.playHold('4174601862', { stallMs: 1600 }); __fx.done.then(r => { window.__done = r; }); __fx.push({ type: 'start', rid: '4174601862', sheets: ['sh1'] }); });
      const spin = await page.waitForSelector('.hfxCap.wait', { timeout: 15000 }).then(() => true).catch(() => false); assert.ok(spin, 'a wait is a small labelled spinner');
      const label = await page.evaluate(() => (document.querySelector('.hfxCap.wait small') || {}).textContent || ''); assert.ok(label.length > 0, 'the spinner is labelled: "' + label + '"');
      const done = await finished(20000); assert.equal(done.stalled, true);
      const s = await state(); assert.equal(s.mode, 'orders'); await sleep(300); clean(await state(), 'after a stall'); noWarn('stall');
      pass('A stalled engine: a labelled spinner, then the film ends and the person is back on Orders');
    });

    /* 10 · the way home alone (the glue may call it too): once, safely, from any tab */
    await scene('return', async () => {
      const RID = '777000';
      await open();
      await page.evaluate(rid => { engine.holds(rid); CN.setMode('nest'); }, RID);
      const same = await page.evaluate(rid => { const a = OrderHoldFx.returnToOnHold(rid), b = OrderHoldFx.returnToOnHold(rid); window.__ret = a; return a === b; }, RID);
      assert.equal(same, true, 'calling it twice gives the same walk home');
      assert.equal(await page.evaluate(() => window.__ret), true);
      const s = await settledAt(RID); assert.equal(s.mode, 'orders'); assert.equal(s.pile, 'hold'); assert.ok(s.cards.includes(RID));
      const t0 = Date.now(); assert.equal(await page.evaluate(rid => OrderHoldFx.returnToOnHold(rid), RID), true); assert.ok(Date.now() - t0 < 500, 'a second call right after is answered at once');
      await sleep(900); clean(await state(), 'after returnToOnHold'); noWarn('return');
      pass('returnToOnHold alone: one walk home for two calls, on Orders > On hold with the card, nothing left');
    });
  } finally { await browser.close(); }
  if (failures.length) { console.log('\n' + failures.length + ' scene(s) failed: ' + failures.join(', ')); process.exit(1); }
  console.log('\n' + ok.length + ' checks passed');
})().catch(e => { console.error(e); process.exit(1); });
