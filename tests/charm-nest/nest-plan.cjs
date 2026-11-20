/* NESTPLAN (cloud architecture v2, build step 6, 20 Nov 2026): the nest's pure helpers live in charm-nest-nest-plan.js, one copy that the page and node both load, so the cloud
 * can nest a sheet exactly as the page does. This test: the module builds the same job as the page for fixtures.
 *   1 · the module is pure: no browser global, no network, no clock, no Etsy; it loads in a bare vm and in plain node; its numbers
 *   2 · the page loads it before its own code, from the published files, and keeps a thin function of each name (no second copy of a rule)
 *   3 · the page's own code (charm-nest-1.html sliced into a vm with its own settings, stock, corner keep-out, angles and pair rules) and the module called from node with
 *       only DATA read off the page (the settings, the sheet's stock, corner and angles, each piece's group): the same job (buildJob), the same feed turn (feedTurn), the same
 *       orders kept whole (keepOrdersWhole), the same hand locks, ranks, keys and turn order, on every fixture
 *   4 · the searches side by side and KEEPSEARCH (searchCount, slotJob, keepPlan, keepAgain, keepNext, finalPick) as the page runs them
 *   5 · the real page in Chromium (the repo's fake site; any request off this computer is refused and counted): its buildJob, feedTurn, keepOrdersWhole, handLocks and
 *       orderRank equal the module in node for the same fixtures. Needs PW_DIR (and CHROMIUM); without PW_DIR part 5 says SKIP.
 * Offline: no network, no Firestore, no Etsy, no paid call.
 *   node tests/charm-nest/nest-plan.cjs            PW_DIR=... CHROMIUM=... node tests/charm-nest/nest-plan.cjs                                                          */
'use strict';
const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm'), assert = require('node:assert/strict');
const ROOT = path.join(__dirname, '../..');
const read = f => fs.readFileSync(path.join(ROOT, f), 'utf8');
const NPL = require('./_nestPlan.cjs');
const NP = require('../../charm-nest-nest-plan.js');
const Solver = require('../../charm-nest-solver.js'), Rose = require('../../charm-nest-rose.js'), Orders = require('../../charm-nest-orders.js'), NS = require('../../charm-nest-nospread.js'), Pair = require('../../charm-nest-pair.js');
const html = read('charm-nest-1.html'), SRC = NPL.source();
let fails = 0, checks = 0;
const ok = (cond, name, seen) => { checks++; if (!cond) { fails++; console.log('FAIL ' + name + (seen === undefined ? '' : ' :: ' + (typeof seen === 'string' ? seen : JSON.stringify(seen)).slice(0, 1500))); } };
const say = name => console.log('  ok ' + name);
const slice = (src, from, to) => { const a = src.indexOf(from); assert(a >= 0, 'not found: ' + from); const b = src.indexOf(to, a + from.length); assert(b > a, 'not found: ' + to); return src.slice(a, b); };
const plain = v => JSON.stringify(v, (k, x) => (ArrayBuffer.isView(x) ? Array.from(x) : x instanceof Set ? { set: [...x] } : x instanceof Map ? { map: [...x] } : x === undefined ? null : x));

/* ── fixtures: plain sheets (bits as number arrays), the same in node, in the page's vm and in Chromium ─────────────────────────────────────────────────────── */
function fixtures(count, seed0) {
  let seed = seed0; const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
  const pick = a => a[Math.floor(rnd() * a.length)], chance = p => rnd() < p, int = (a, b) => a + Math.floor(rnd() * (b - a + 1));
  const out = [];
  for (let i = 0; i < count; i++) {
    const focus = chance(.5), metal = focus ? pick(['gold', 'silver']) : pick(['gold', 'silver', 'rose', 'gold10k', 'gold14k']), n = int(0, focus ? 14 : 10), orders = Array.from({ length: focus ? int(3, 9) : int(1, 5) }, (_, k) => String(4000000000 + k * 7)), charms = [];
    for (let k = 0; k < n; k++) {
      const w = int(2, 5), h = int(2, 5), order = pick(orders), line = int(1, 2);
      const c = { id: 'c' + k, w, h, scale: pick([1, 2]), bits: Array.from({ length: w * h }, () => (chance(.8) ? 1 : 0)), areaPt2: int(10, 400), widthPt: int(5, 30), heightPt: int(5, 30), order, orderDate: chance(.9) ? int(1700000000, 1700100000) : 0 };
      if (chance(.2)) c.chunk = int(1, 3);
      if (chance(.1)) c.frontAt = int(1700000000000, 1700100000000);
      if (chance(.1)) c.excluded = true;
      if (chance(.25)) { c.groupSize = 2; c.side = pick(['L', 'R']); c.mirror = c.side === 'R'; c.bodyIndex = 0; c.poolId = `${order}_${line}_${k}`; c.orderInfo = { receiptId: order, transactionId: String(line), copy: k } }
      if (chance(.25)) { c.pinned = { cxPt: int(5, 200), cyPt: int(5, 100), angle: pick([0, 90, 33]) }; if (chance(.5)) c.arrivalPin = true; }
      if (chance(.2)) c.manual = { size: pick([1, .9, 1.1, 1.3, .85]), ...(chance(.3) ? { fit: 'inplace' } : {}) };
      if (chance(.1)) c.frozen = true;
      charms.push(c);
    }
    // (a pair: one line of two pieces, a Left and a Right, kept whole as a group, R2)
    if (chance(.5)) { const order = pick(orders), line = 9; for (const side of ['L', 'R']) { const k = charms.length; charms.push({ id: 'c' + k, w: 2, h: 3, scale: 1, bits: [1, 1, 1, 1, 0, 1], areaPt2: int(10, 200), widthPt: 8, heightPt: 12, order, orderDate: int(1700000000, 1700100000), groupSize: 2, side, mirror: side === 'R', bodyIndex: 0, poolId: `${order}_${line}_${k}`, orderInfo: { receiptId: order, transactionId: String(line), copy: k }, ...(chance(.3) ? { chunk: 1 } : {}) }); } }
    const placed = charms.filter(() => chance(.5));
    const sh = { metal, page: 1, charms, placements: placed.map(c => ({ id: c.id, cxPt: int(5, 200), cyPt: int(5, 100), angle: pick([0, 90]) })), rejects: charms.filter(c => !placed.includes(c) && chance(.4)).map(c => c.id) };
    if (focus || chance(.7)) sh.runId = 'run-1';
    if (chance(focus ? .4 : .2)) sh.topup = chance(.6) ? { tried: [] } : { tried: [], closedAt: 5 };
    if (chance(.3)) sh.appendOnly = true;
    if (chance(.4)) sh.nestInitial = sh.placements.filter(() => chance(.6)).map(p => ({ ...p }));
    else if (sh.appendOnly && chance(.7)) sh.nestInitial = sh.placements.map(p => ({ ...p }));   // (an append: every saved piece is held)
    if (chance(.15)) sh.roseProtected = { placements: sh.placements.filter(() => chance(.3)).map(p => ({ id: p.id })) };
    if (chance(.15)) sh.roseStock = { id: 'rs1', revision: 2, wPt: 180, hPt: 90, ...(chance(.5) ? { profileJson: JSON.stringify({ pts: [[0, 0], [180, 0], [180, 90]] }) } : {}) };
    if (chance(.1)) sh.roseCutAt = 5;
    if (chance(.1)) sh._keepPlaced = true;
    if (chance(.2)) sh._unjamFails = int(1, 4);
    if (chance(.3)) sh.intakePhase = pick(['fill', 'final', 'repack']);
    if (chance(.3)) sh.intakeBudgetMs = pick([12000, 180000]);
    if (chance(.5)) sh.runSeed = int(1, 99999);
    if (!focus && chance(.15)) sh.nestFlow = 'learned';
    if (chance(.2)) sh.feedWait = charms.filter(() => chance(.3)).map(c => c.id);
    if (chance(.1)) sh._mergeNext = true;
    const settings = { clearancePt: pick([-0.5, 0, 1]), insetPt: pick([1.5, 0]), budgetS: pick([180, 60]), seed: pick([1, 7]), maxFill: pick([.8, .74]), engine: pick(['solver', 'solver', 'ai']), angleStep: pick([2, 5, 10]), cornerPt: pick([undefined, 2, 0]), nospread: pick(['on', 'off']) };
    out.push({ sh, settings, usable: pick([0, 800, 1500, 4000, 100000]), room: pick([0, 1, 2, 3, 5]), pocket: chance(.3) });
  }
  return out;
}
const hydrate = sh => { const s = JSON.parse(JSON.stringify(sh)); for (const c of s.charms) c.bits = Uint8Array.from(c.bits); return s; };
const FIX = fixtures(400, 20261120);

/* ── 1 · pure ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
{
  const code = SRC.replace(/\/\*[\s\S]*?\*\//g, '').replace(/(^|[^:\\])\/\/.*$/gm, '$1');
  const banned = ['window', 'document', 'navigator', 'localStorage', 'sessionStorage', 'indexedDB', 'fetch', 'XMLHttpRequest', 'WebSocket', 'performance', 'Date', 'Math.random', 'crypto', 'setTimeout', 'setInterval', 'require', 'import', 'process', 'globalThis', 'S.settings', 'Etsy', 'etsy', 'netlify', 'firebase', 'Firestore'];
  const seen = banned.filter(w => new RegExp('(^|[^\\w$.])' + w.replace('.', '\\.') + '(?![\\w$])').test(code));
  ok(seen.length === 0, '1a the module reads no browser global, network, clock, random, page state or Etsy', seen);
  const bare = vm.createContext({ module: { exports: {} } });   // (nothing but `module`: no window, no self, no navigator)
  vm.runInContext(SRC, bare);
  const api = bare.module.exports;
  ok(typeof api.buildJob === 'function' && typeof api.feedTurn === 'function' && typeof api.keepOrdersWhole === 'function' && typeof api.finalPick === 'function', '1b it loads in a bare vm with nothing but module', Object.keys(api));
  const want = ['CHARM_SCALE', 'CAREFUL_ANGLES', 'FEED_ORDERS', 'buildJob', 'handLocks', 'withHandLocks', 'feedTurn', 'nestItems', 'orderRank', 'orderKey', 'turnOrder', 'gapFilling', 'carefulNest', 'manualSize', 'keepOrdersWhole', 'orderMatesOf', 'multiStartFor', 'searchCount', 'slotJob', 'keepPlan', 'keepResult', 'keepAgain', 'keepNext', 'keepJob', 'pickLayout', 'finalPick'];
  ok(want.every(k => k in NP), '1c node requires it with every helper of step 6', want.filter(k => !(k in NP)));
  ok(NP.CHARM_SCALE === 0.971 && NP.FEED_ORDERS === 3 && NP.KEEP_MAX === 36 && NP.CAREFUL_ANGLES.length === 180 && NP.CAREFUL_ANGLES.every((a, i) => a === 2 * i), '1d its numbers: written at 97.1 %, three orders a turn, 2° steps, at most 36 layouts kept searching');
  say('1 the module is pure and loads in node');
}

/* ── 2 · the page loads it and keeps one copy of each rule ─────────────────────────────────────────────────────────────────────────────────────────────── */
{
  const tag = html.search(/<script src="charm-nest-nest-plan\.js\?v=[^"]+"><\/script>/), app = html.indexOf('CHARM NESTING STATION 1 — application');
  ok(tag > 0 && app > tag, '2a charm-nest-1.html loads charm-nest-nest-plan.js before its own code');
  ok(read('scripts/build-public.cjs').includes('"charm-nest-nest-plan.js"'), '2b scripts/build-public.cjs publishes it (a file the page loads and the site lacks is a 404 on the live site)');
  const thin = [/const CHARM_SCALE = CharmNestNestPlan\.CHARM_SCALE;/, /const CAREFUL_ANGLES = CharmNestNestPlan\.CAREFUL_ANGLES;/, /const FEED_ORDERS = CharmNestNestPlan\.FEED_ORDERS;/, /const orderKey = CharmNestNestPlan\.orderKey;/,
    /const orderRank = c => CharmNestNestPlan\.orderRank\(c, /, /function buildJob\(sh\) \{\n  return CharmNestNestPlan\.buildJob\(sh, /, /function handLocks\(sh, items\) \{ return CharmNestNestPlan\.handLocks\(/, /function feedTurn\(sh, items\) \{\n  return CharmNestNestPlan\.feedTurn\(/,
    /function keepOrdersWhole\(sh, byId, pocket\) \{[\s\S]{0,200}CharmNestNestPlan\.keepOrdersWhole\(sh, byId, pocket, /, /CharmNestNestPlan\.searchCount\(job, items\.length, /, /CharmNestNestPlan\.keepPlan\(job, n, items\.length, /, /CharmNestNestPlan\.slotJob\(job, i, /,
    /CharmNestNestPlan\.keepResult\(K, pool\.results, idx, m\.result, /, /CharmNestNestPlan\.keepNext\(K, idx, now\)/, /let pick = CharmNestNestPlan\.finalPick\(results, /];
  ok(thin.every(r => r.test(html)), '2c the page keeps a thin function of each name and calls the module', thin.filter(r => !r.test(html)).map(String));
  const old = ['const fresh = items.filter(c => !placed.has(c.id))', 'navigator.hardwareConcurrency) || 4) - 1))', 'Math.min(36, Math.max(n, 2 * items.length))', 'const v = K.next++', 'const wholeMates = orderMatesOf(', 'results.reduce((a, b) => CharmNestSolver.betterLayout(b, a', 'const CAREFUL_ANGLES = Array.from', 'const CHARM_SCALE = 0.971', 'const ORDER_ROOM = 0.7'];
  ok(old.every(t => !html.includes(t)), '2d no second copy of a rule is left in the page', old.filter(t => html.includes(t)));
  say('2 the page loads the module and keeps one copy');
}

/* ── 3 · the page's own code in a vm against the module called from node with DATA only ────────────────────────────────────────────────────────────────── */
const pageWorld = () => {
  const w = vm.createContext({ console, CharmNestSolver: Solver, CharmNestRose: Rose, CharmNestOrders: Orders, CharmNestNoSpread: NS, CharmNestPair: Pair, navigator: { hardwareConcurrency: 8 }, window: {} });
  NPL.into(w);   // (the page's <script src="charm-nest-nest-plan.js">)
  vm.runInContext([
    slice(html, 'const PT_PER_IN', 'const S = {'), 'var S = { settings: Object.assign({}, DEFAULTS), sheets: {} };',
    slice(html, 'function stockFor(', 'function angleSet()'), slice(html, 'function angleSet()', '\n'), slice(html, 'const NO_CORNER_METALS', 'function cornerSheet('),
    slice(html, 'function activeCharms(sh)', '\n'), slice(html, 'function groupOf(c)', '/** "Left" / "Right"'), slice(html, 'function inflatedArea(c)', '\n'), slice(html, 'function nospread()', '\n'),
    // (the page's rules this one only reads, stood in for the same way on both sides: the gap fill's room and NOSPREAD's admission, and the sheet's usable area)
    'var USABLE = 0, ROOM = 0, SAID = []; const agent = (...a) => { SAID.push(String(a[2])); return null; }; const topupRoom = (sh, waiting) => ROOM; const nospreadAdmitted = (sh, byOrder) => new Set([...byOrder.keys()].filter((k, i) => i % 2 === 0)); const usableArea = sh => USABLE;',
    slice(html, 'const CHARM_SCALE', 'function packingKey('), slice(html, '/* A big batch goes onto a sheet', '/* A stopped run starts none'), slice(html, 'function keepOrdersWhole(', 'function orderSummary('),
    'this.page = { S, stockFor, cornerPtSetting, angleSet, groupOf, inflatedArea, nospread, buildJob, feedTurn, nestItems, keepOrdersWhole, handLocks, withHandLocks, orderRank, orderKey, turnOrder, set: (u, r) => { USABLE = u; ROOM = r; }, said: () => SAID.splice(0) };'
  ].join('\n'), w);
  return w.page;
};
// what a caller in node hands the module: data read off the page, and the same stand-ins for the page rules the helpers only read
const nodeEnv = (f, data, said) => ({
  settings: JSON.parse(JSON.stringify(data.settings)), stockFor: () => data.stock, cornerPt: () => data.corner, angleSet: () => data.angles.slice(), cores: data.cores,
  Solver, Rose, Orders, groupOf: c => (c ? data.groups[c.id] ?? null : null), inflatedArea: c => data.areas[c.id], nospread: () => (data.nospread ? NS : null),
  usableArea: () => f.usable, topupRoom: () => f.room, nospreadAdmitted: (sh, byOrder) => new Set([...byOrder.keys()].filter((k, i) => i % 2 === 0)), agent: (...a) => said.push(String(a[2]))
});
// the data a caller reads off the page for a sheet (in the cloud: the shared settings, the sheet record, the pair rules)
const pageData = (P, f, cores) => {
  const sh = hydrate(f.sh);
  return { settings: P.S.settings, stock: JSON.parse(JSON.stringify(P.stockFor(sh.metal, sh))), corner: P.cornerPtSetting(sh), angles: Array.from(P.angleSet()), cores, nospread: !!P.nospread(),
    groups: Object.fromEntries(sh.charms.map(c => [c.id, P.groupOf(c)])), areas: Object.fromEntries(sh.charms.map(c => [c.id, P.inflatedArea(c)])) };
};
const compareAll = (label, runPage) => {
  const tally = { buildJob: 0, feedTurn: 0, waits: 0, keepOrdersWhole: 0, lifts: 0, handLocks: 0, locks: 0, rank: 0 };
  let bad = null;
  for (const [i, f] of FIX.entries()) {
    const r = runPage(f);   // { data, job, feed, keep, locks, rank }
    const said = [], env = nodeEnv(f, r.data, said);
    const s1 = hydrate(f.sh), job = NP.buildJob(s1, env);
    if (plain(job) !== plain(r.job)) { bad = bad || ['buildJob', i, plain(r.job).slice(0, 600), plain(job).slice(0, 600)]; continue; }
    tally.buildJob++;
    const s2 = hydrate(f.sh), turn = NP.feedTurn(s2, NP.nestItems(s2, env), env).map(c => c.id);
    if (plain([turn, s2.feedWait]) !== plain(r.feed)) { bad = bad || ['feedTurn', i, plain(r.feed), plain([turn, s2.feedWait])]; continue; }
    tally.feedTurn++; if (s2.feedWait) tally.waits++;
    const s3 = hydrate(f.sh), byId = new Map(s3.charms.filter(c => !c.excluded).map(c => [c.id, c])), pocket = f.pocket ? new Set(s3.placements.slice(0, 2).map(p => p.id)) : undefined;
    NP.keepOrdersWhole(s3, byId, pocket, env);
    const kept = [s3.placements, s3.rejects, s3.nestInitial ?? null, said];
    if (plain(kept) !== plain(r.keep)) { bad = bad || ['keepOrdersWhole', i, plain(r.keep).slice(0, 600), plain(kept).slice(0, 600)]; continue; }
    tally.keepOrdersWhole++; if (said.length) tally.lifts++;
    const s4 = hydrate(f.sh), locks = [NP.handLocks(s4, s4.charms, { Solver }), NP.withHandLocks(s4, s4.charms, s4.nestInitial, { Solver })];
    if (plain(locks) !== plain(r.locks)) { bad = bad || ['handLocks', i, plain(r.locks), plain(locks)]; continue; }
    tally.handLocks++; if (locks[0]) tally.locks++;
    const s5 = hydrate(f.sh), rank = [s5.charms.map(c => NP.orderRank(c, Orders)), s5.charms.map(c => NP.orderKey(c)), s5.charms.slice().sort(NP.turnOrder(s5, s5.charms, { Orders })).map(c => c.id)];
    if (plain(rank) !== plain(r.rank)) { bad = bad || ['orderRank/orderKey/turnOrder', i, plain(r.rank), plain(rank)]; continue; }
    tally.rank++;
  }
  ok(!bad, label + ': the page and the module in node agree on every fixture', bad);
  return tally;
};
{
  const P = pageWorld(), DEF = JSON.stringify(P.S.settings);   // (the page's DEFAULTS, as a fresh computer has them)
  const runPage = f => {
    P.S.settings = Object.assign(JSON.parse(DEF), f.settings); P.set(f.usable, f.room); P.said();
    const data = pageData(P, f, 8);
    const s1 = hydrate(f.sh), job = P.buildJob(s1);
    const s2 = hydrate(f.sh), feed = [P.feedTurn(s2, P.nestItems(s2)).map(c => c.id), s2.feedWait];
    const s3 = hydrate(f.sh), byId = new Map(s3.charms.filter(c => !c.excluded).map(c => [c.id, c]));
    P.keepOrdersWhole(s3, byId, f.pocket ? new Set(s3.placements.slice(0, 2).map(p => p.id)) : undefined);
    const keep = [s3.placements, s3.rejects, s3.nestInitial ?? null, P.said()];
    const s4 = hydrate(f.sh), locks = [P.handLocks(s4, s4.charms), P.withHandLocks(s4, s4.charms, s4.nestInitial)];
    const s5 = hydrate(f.sh), rank = [s5.charms.map(c => P.orderRank(c)), s5.charms.map(c => P.orderKey(c)), s5.charms.slice().sort(P.turnOrder(s5, s5.charms)).map(c => c.id)];
    return JSON.parse(plain({ data, job, feed, keep, locks, rank }));
  };
  const t = compareAll('3 page code in a vm', runPage);
  ok(t.buildJob === FIX.length && t.waits > 10 && t.lifts > 10 && t.locks > 10, '3 the fixtures reach every path (a turn that waits, orders lifted whole, hand locks)', t);
  say(`3 the page's code and the module in node: the same job, feed turn, orders kept whole, hand locks and ranks on ${t.buildJob} sheets (${t.waits} waits, ${t.lifts} lifts, ${t.locks} hand-locked)`);
}

/* ── 3b · named sheets: what the page asks of each kind of nest ──────────────────────────────────────────────────────────────────────────────────────────── */
{
  const piece = (id, order, date, extra) => Object.assign({ id, order, orderDate: date, w: 2, h: 2, scale: 1, bits: Uint8Array.from([1, 1, 1, 1]), areaPt2: 40, widthPt: 10, heightPt: 10 }, extra);
  const env = (settings, more) => Object.assign({ settings: Object.assign({ clearancePt: -.5, insetPt: 1.5, budgetS: 180, seed: 1, maxFill: .8, engine: 'solver' }, settings), stockFor: () => ({ wPt: 283.5, hPt: 141.7 }), cornerPt: sh => (['gold10k', 'gold14k'].includes(sh.metal) ? 0 : 4.25), angleSet: () => [0, 10, 20], cores: 8, Solver, Rose, Orders }, more);
  const fresh14 = { metal: 'gold14k', charms: Array.from({ length: 8 }, (_, i) => piece('k' + i, 'o' + i, 100 + i)), placements: [] };
  const j14 = NP.buildJob(fresh14, env({}));
  ok(j14.tight === true && j14.multiStart === 6 && j14.angles.length === 180 && j14.block && !j14.nearFullContact && j14.sheet.cornerPt === 0 && j14.lockedPlacements === null, '3b-1 a fresh 14K sheet: tight, six searches side by side (eight processors), 2° steps, no corner keep-out', j14);
  ok(NP.buildJob(fresh14, env({}, { cores: 2 })).multiStart === 3 && NP.buildJob(fresh14, env({}, { cores: 0 })).multiStart === 3, '3b-2 never fewer than three searches side by side');
  const gold = { metal: 'gold', runId: 'r', charms: Array.from({ length: 6 }, (_, i) => piece('g' + i, 'o' + i, 100 + i)), placements: [] };
  const turn = NP.feedTurn(gold, gold.charms, env({}));
  ok(turn.length === 3 && gold.feedWait.length === 3 && turn.every(c => ['g0', 'g1', 'g2'].includes(c.id)), '3b-3 Gold of a run: the oldest three orders this turn, the rest wait', [turn.map(c => c.id), gold.feedWait]);
  const rose = { metal: 'rose', runId: 'r', charms: gold.charms, placements: [] };
  ok(NP.feedTurn(rose, rose.charms, env({})).length === 6 && rose.feedWait === null, '3b-4 Rose Gold places the whole batch in one search');
  const hand = { metal: 'gold10k', charms: [piece('h', 'o1', 1, { manual: { size: 1.1 }, pinned: { cxPt: 50, cyPt: 40, angle: 90 } }), piece('n', 'o2', 2)], placements: [] };
  const hj = NP.buildJob(hand, env({}));
  ok(hj.lockedPlacements && hj.lockedPlacements.length === 1 && hj.lockedPlacements[0].id === 'h' && hj.lockedPlacements[0].scale === +(0.971 * 1.1).toFixed(5) && hj.pieces[0].hold === true && Math.abs(hj.pieces[0].areaPt2 - 40 * 1.21) < 1e-9, '3b-5 a piece placed by hand is held where the person put it, at its size (ZEROGAP)', hj.lockedPlacements);
  const missed = { metal: 'gold', topup: null, charms: [piece('a', 'A', 1), piece('b', 'B', 2), piece('c', 'C', 3)], placements: [{ id: 'a' }, { id: 'c' }], rejects: ['b'] };
  const said = [];
  NP.keepOrdersWhole(missed, new Map(missed.charms.map(c => [c.id, c])), undefined, env({}, { agent: (...a) => said.push(a[2]) }));
  ok(missed.placements.map(p => p.id).join() === 'a' && missed.rejects.join() === 'b,c' && /lifted off/.test(said[0] || ''), '3b-6 an older order that missed: the younger one placed after it makes way (oldest first)', [missed, said]);
  say('3b named sheets: 14K side by side, the feed turn, Rose Gold in one search, hand locks, oldest first');
}

/* ── 4 · the searches side by side and KEEPSEARCH ─────────────────────────────────────────────────────────────────────────────────────────────────────────── */
{
  const par = a => (a >= 72 ? 1 : 2);
  let job = { multiStart: 6, tight: true, angles: NP.CAREFUL_ANGLES, lockedPlacements: null, protectedRose: null, seed: 10, timeBudgetMs: 60000 };
  ok(NP.searchCount(job, 8, par) === 6 && job.multiStart === 6, '4a a fresh tight sheet runs its six searches');
  job = { multiStart: 6, tight: true, angles: NP.CAREFUL_ANGLES, lockedPlacements: null, protectedRose: null, seed: 10 };
  ok(NP.searchCount(job, 2, par) === 2 && job.multiStart === 2, '4b SMALLSEARCH: no more searches than pieces');
  job = { multiStart: 6, tight: true, angles: NP.CAREFUL_ANGLES, lockedPlacements: [{ id: 'x' }], protectedRose: null, seed: 10 };
  ok(NP.searchCount(job, 8, par) === 1 && !('multiStart' in job), '4c GOLDLOCK: a sheet with held pieces is nested the single way');
  job = { multiStart: 4, angles: [0, 90], seed: 10 };
  ok(NP.searchCount(job, 8, par) === 4, '4d a search that is not tight runs at least the page\'s own count');
  job = { multiStart: 4, tight: true, angles: NP.CAREFUL_ANGLES, seed: 10, timeBudgetMs: 60000 };
  const K = NP.keepPlan(job, 4, 5, 1000);
  ok(K && K.next === 4 && K.max === 10 && K.started.join() === '1000,1000,1000,1000' && NP.keepPlan({ multiStart: 4 }, 4, 5, 0) === null, '4e KEEPSEARCH keeps searching a tight multi-start nest, at most two layouts a piece', K);
  const slots = [0, 1, 2, 3].map(i => NP.slotJob(job, i, { keep: true, useGPU: false }));
  ok(slots.map(s => [s.seed, s.exploreRotations, s.orderVariant, !!s.longFirstForce].join()).join('|') === '10,false,0,false|11,true,1,true|12,false,2,false|13,true,3,true', '4f each search its own seed and placing order; every other one turns more broadly and lays the bars flat', slots.map(s => [s.seed, s.exploreRotations, s.orderVariant, s.longFirstForce]));
  const results = [], better = (a, b) => (a.placements.length - b.placements.length) || (b.deadPt2 - a.deadPt2);
  const FakeSolver = { betterLayout: (a, b) => !b || better(a, b) > 0, bestResult: (a, b) => (!b || better(a, b) >= 0 ? a : b) };
  const r1 = { placements: [1, 2], deadPt2: 50, endedBy: 'complete' }, r2 = { placements: [1, 2], deadPt2: 30, endedBy: 'complete' };
  NP.keepResult(K, results, 0, r1, 'gold14k', () => ({}), FakeSolver); NP.keepResult(K, results, 0, r2, 'gold14k', () => ({}), FakeSolver);
  ok(K.done === 2 && results[0] === r2, '4g a search keeps the least wasteful of its own layouts');
  ok(NP.keepAgain(K, 0, r2, 20000, { nesting: true }) === true && NP.keepAgain(K, 0, r2, 40000, { nesting: true }) === false && NP.keepAgain(K, 0, { endedBy: 'stopped' }, 2000, { nesting: true }) === false && NP.keepAgain(K, 0, r2, 2000, { nesting: true, stopAsked: true }) === false, '4h it starts again only while the pass fits in the time left, never after a stop');
  const next = NP.keepNext(K, 0, 20000);
  ok(K.next === 5 && K.started[0] === 20000 && next.orderVariant === 4 && next.seed === 14 && next.useGPU === false && !next.longFirstForce && NP.keepJob(job, 5).longFirstForce === true, '4i the next placing order, never on the GPU');
  const pick = NP.finalPick([r1, r2, { placements: [1], deadPt2: 1 }], null, null, 'gold14k', () => ({}), FakeSolver);
  ok(pick === r2 && NP.finalPick([r1], r1, r2, 'gold14k', () => ({}), FakeSolver) === r2, '4j the pick: the best of the searches, never worse than the best already shown');
  ok(JSON.stringify(NP.pickOpts('gold10k')) === '{"reachFirst":true,"tight":true}' && JSON.stringify(NP.pickOpts('gold')) === '{"reachFirst":false,"tight":false}' && JSON.stringify(NP.keepOpts('rose')) === '{"reachFirst":false,"tight":true}', '4k solid gold: the least stock wins; Rose Gold tight');
  say('4 the searches side by side and KEEPSEARCH, as the page runs them');
}

/* ── 5 · the real page in Chromium ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
(async () => {
  if (!process.env.PW_DIR) { console.log('  SKIP 5 the real page in Chromium (no PW_DIR)'); return; }
  const { open } = require('./day-sim/harness.cjs');
  const outside = [];
  const local = u => /^https?:\/\/(127\.0\.0\.1|localhost)(:\d+)?\//.test(u);
  const stubbed = u => /gstatic\.com\/firebasejs|qrcodejs|fonts\.googleapis|fonts\.gstatic|^https:\/\/firebasestorage\.googleapis\.com\//.test(u);   // (the harness answers these itself, offline)
  const H = await open({ masters: [], receipts: [], beforePage: ctx => ctx.route(u => !local(u.href), r => { const u = r.request().url(); if (stubbed(u)) return r.fallback(); outside.push(u); return r.abort(); }) });
  try {
    const page = H.page;
    const runs = await page.evaluate(FIXJ => {
      const hyd = sh => { const s = JSON.parse(JSON.stringify(sh)); for (const c of s.charms) c.bits = Uint8Array.from(c.bits); return s; };
      const plainIn = v => JSON.parse(JSON.stringify(v, (k, x) => (ArrayBuffer.isView(x) ? Array.from(x) : x instanceof Set ? { set: [...x] } : x instanceof Map ? { map: [...x] } : x === undefined ? null : x)));
      const keepSettings = JSON.parse(JSON.stringify(S.settings)), realUsable = window.usableArea, realRoom = window.topupRoom, realAdmit = window.nospreadAdmitted, realAgent = window.agent;
      const out = []; let USABLE = 0, ROOM = 0; const SAID = [];
      // (the rules a helper only reads stand in as they do in node; buildJob and the rest are the page's own)
      window.usableArea = () => USABLE; window.topupRoom = () => ROOM; window.nospreadAdmitted = (sh, byOrder) => new Set([...byOrder.keys()].filter((k, i) => i % 2 === 0)); window.agent = (...a) => { SAID.push(String(a[2])); return null; };
      try {
        for (const f of JSON.parse(FIXJ)) {
          S.settings = Object.assign(JSON.parse(JSON.stringify(keepSettings)), f.settings); USABLE = f.usable; ROOM = f.room; SAID.length = 0;
          const sh0 = hyd(f.sh);
          const data = { settings: JSON.parse(JSON.stringify(S.settings)), stock: plainIn(stockFor(sh0.metal, sh0)), corner: cornerPtSetting(sh0), angles: Array.from(angleSet()), cores: navigator.hardwareConcurrency, nospread: !!nospread(),
            groups: Object.fromEntries(sh0.charms.map(c => [c.id, groupOf(c)])), areas: Object.fromEntries(sh0.charms.map(c => [c.id, inflatedArea(c)])) };
          const s1 = hyd(f.sh), job = buildJob(s1);
          const s2 = hyd(f.sh), feed = [feedTurn(s2, nestItems(s2)).map(c => c.id), s2.feedWait];
          const s3 = hyd(f.sh), byId = new Map(s3.charms.filter(c => !c.excluded).map(c => [c.id, c]));
          keepOrdersWhole(s3, byId, f.pocket ? new Set(s3.placements.slice(0, 2).map(p => p.id)) : undefined);
          const keep = [s3.placements, s3.rejects, s3.nestInitial ?? null, SAID.splice(0)];
          const s4 = hyd(f.sh), locks = [handLocks(s4, s4.charms), withHandLocks(s4, s4.charms, s4.nestInitial)];
          const s5 = hyd(f.sh), rank = [s5.charms.map(c => orderRank(c)), s5.charms.map(c => orderKey(c)), s5.charms.slice().sort(turnOrder(s5, s5.charms)).map(c => c.id)];
          out.push(plainIn({ data, job, feed, keep, locks, rank }));
        }
      } finally { S.settings = keepSettings; window.usableArea = realUsable; window.topupRoom = realRoom; window.nospreadAdmitted = realAdmit; window.agent = realAgent; }
      return out;
    }, JSON.stringify(FIX));
    const t = compareAll('5 the real page in Chromium', f => runs[FIX.indexOf(f)]);
    ok(t.buildJob === FIX.length, '5 every fixture compared', t);
    const errs = (H.errors || []).filter(e => !/firebase stub|favicon|HTTP 400|status of 400/.test(e));
    ok(errs.length === 0, '5 no page error', errs.slice(0, 3));
    ok(outside.length === 0, '5 nothing left this computer', outside.slice(0, 5));
    say(`5 the real page in Chromium and the module in node: the same job, feed turn, orders kept whole, hand locks and ranks on ${t.buildJob} sheets`);
  } finally { await H.close?.(); }
})().catch(e => { fails++; console.log('ERROR ' + (e && e.stack || e)); }).finally(() => {
  console.log(fails ? `FAIL nest-plan · ${fails} of ${checks} check(s)` : `PASS nest-plan · ${checks} checks`);
  process.exit(fails ? 1 : 0);
});
