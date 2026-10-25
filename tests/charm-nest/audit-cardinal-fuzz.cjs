// AUDIT (cardinal rule / shared orders / partial sheets, R3 R9 R10 R11 R16): random days through the page's REAL set assembly (audit-cardinal-lib.cjs), judged after EVERY look by
//   1 an independent oracle (R3, R1, R12, an order across two open sets)
//   2 an R11 timing oracle: a sheet that JOINS a set in this look while its group (sheets tied by orders it shares directly, never through a sheet in a set, a cut sheet or an uncut gold sheet) shares an
//     order with a partial GF/SS sheet outside every set, and no sheet of that group (nor an order mate in a set) was in that set before the look: the set was formed / enlarged around a group that must wait
//   3 the app's own checker (Gate.selfCheck) and the strict checker (R11 without 'pending')
// Scenes: plain (orders arrive, pairs sometimes split over two same-colour sheets: R16 faults), late (some lines of an order come in later), takeoff (a person takes a shared order off a sheet),
//         rose (a Rose Gold sheet keeps filling and is cut now and then), auto (sets committed when ready).
// Scenes combine with '+'. The default (plain, auto, auto+late, auto+takeoff) passes. --scenes late,takeoff,rose shows the known rule-breaking families (audit-cardinal-breaks.cjs B1-B3: a late line or a
// Rose Gold cut ties a complete unit to a formed open set (R7) or two open sets (R9); a person's take-off leaves a set bigger than the minimum (R7, conflict C6: flagged, never repaired by itself).
// Usage: node tests/charm-nest/audit-cardinal-fuzz.cjs [--seeds 1-40] [--scenes plain,late] [--verbose]
'use strict';
const L = require('./audit-cardinal-lib.cjs');
const arg = (k, d) => { const i = process.argv.indexOf('--' + k); return i > 0 ? process.argv[i + 1] : d; };
const range = s => { const m = /^(\d+)-(\d+)$/.exec(s); return m ? Array.from({ length: +m[2] - +m[1] + 1 }, (_, i) => +m[1] + i) : s.split(',').map(Number); };
const SEEDS = range(arg('seeds', '1-25')), SCENES = arg('scenes', 'plain,auto,auto+late,auto+takeoff').split(','), VERBOSE = process.argv.includes('--verbose');
const rng = seed => () => (seed = (seed * 1664525 + 1013904223) >>> 0) / 4294967296;

function groupOfJoined(w, q, was) {
  // the sheets tied to q by orders shared directly through CORE sheets (anchors: in a set BEFORE this look, laser cut, uncut gold outside every set); ends = partial GF/SS outside every set
  const pages = w.pages, inSet = p => !!was.get(p), anchor = p => inSet(p) || +p.laserDoneAt > 0 || (L.completed(p) === false && !['gold', 'silver'].includes(p.metal) && !p.solidPick);
  const shared = L.sharedOrders(pages).map(([, m]) => [...m.keys()]);
  const seen = new Set([q]), stack = [q], ends = new Set(), anchors = new Set();
  while (stack.length) {
    const x = stack.pop();
    for (const sheets of shared) if (sheets.includes(x)) for (const y of sheets) {
      if (y === x || seen.has(y)) continue;
      if (L.partial(y) && !inSet(y)) { ends.add(y); continue; }
      if (y !== q && anchor(y)) { anchors.add(y); continue; }
      seen.add(y); stack.push(y);
    }
  }
  return { cores: seen, ends, anchors };
}

async function day(scene, seed) {
  const has = f => scene === f || scene.split('+').includes(f);
  const r = rng(seed * 104729 + scene.length), w = L.world(), cur = {}, CAP = { gold: 6, silver: 6, rose: 8 };
  let oid = 5000000000 + seed * 10000, looks = 0; const fails = [], notes = [], orders = [];
  const sheetOf = m => cur[m] || (cur[m] = w.page(m, { density: .4 }));
  const room = (m, n) => { let p = sheetOf(m); if (p.placements.length + n > CAP[m]) { if (m !== 'rose') w.full(p); else { p.roseCutAt = Date.now(); } p = cur[m] = w.page(m); } return p; };
  const put = (m, id) => { const p = room(m, 1); w.put(p, id); return p; };
  const order = () => {
    const id = String(++oid), lines = 1 + (r() < .25 ? 1 : 0) + (r() < .08 ? 1 : 0); orders.push(id);
    for (let t = 1; t <= lines; t++) {
      const m = has('rose') && r() < .15 ? 'rose' : r() < .55 ? 'gold' : 'silver', pair = r() < .3;
      put(m, `${id}_${t}_1`);
      if (pair) { if (r() < .2) { const p = cur[m]; if (m !== 'rose') w.full(p); cur[m] = null; } put(m, `${id}_${t}_2`); }      // a pair split over two same-colour sheets (R16 fault)
    }
    if (has('late') && r() < .1 && orders.length > 3) { const old = orders[Math.floor(r() * orders.length)]; put(r() < .5 ? 'gold' : 'silver', `${old}_${90 + Math.floor(r() * 9)}_1`); }
  };
  const before = () => new Map(w.pages.map(p => [p, w.inSet(p) ? p.setId : null]));
  const look = async what => {
    const was = before(), wasPartialOut = new Set(w.pages.filter(p => L.partial(p) && !w.inSet(p)));
    await w.assemble(); looks++;
    for (const f of L.invariantFaults(w)) fails.push(`${what}: ${f}`);
    // R11 timing
    for (const q of w.pages) {
      if (!w.inSet(q) || was.get(q) === q.setId) continue;
      const g = groupOfJoined(w, q, was);
      const endsNow = [...g.ends].filter(e => wasPartialOut.has(e) && L.partial(e) && !w.inSet(e));
      if (!endsNow.length) continue;
      const prior = [...g.cores, ...g.anchors].some(x => was.get(x) === q.setId);
      if (!prior) fails.push(`${what}: R11: ${q.sheetId} joined Set ${w.setOf(q)} although it is tied (${[...g.cores].map(p => p.sheetId)}) to the partial ${endsNow.map(p => p.sheetId)} and no sheet of its group was in that set before`);
    }
    // sheets that left an open set this look (C6: the assembly never takes a qualifying sheet out by itself)
    for (const [p, sid] of was) if (sid && !(w.inSet(p) && p.setId === sid)) notes.push(`${what}: ${p.sheetId} left ${sid}`);
    const sc = w.selfCheck();
    const page = id => w.pages.find(p => p.sheetId === id);
    for (const v of (sc && sc.violations) || []) {
      const forced = v.rule === 'cardinal' && v.outside && /put them in one set first/.test((page(v.outside) || {}).cardinalHold || '');
      fails.push(`${what}: checker [${v.ruleNo} ${v.rule}]${forced ? ' (spread by a late line: waits "put them in one set first")' : ''} ${v.message}`);
    }
    // (Auto, in set order only: committing a later set while an earlier one is open is audit-cardinal-commit-order.cjs)
    if (has('auto')) for (const s of w.sets()) if (w.sets().filter(x => x.seq < s.seq).every(x => x.committedAt || !w.members(x).length) && !s.committedAt && w.members(s).length && !(sc && sc.violations.some(v => v.set === s.setId)) && !(sc && sc.partners.some(p => p.set === s.setId))) { const m = w.members(s); if (m.some(p => p.metal === 'gold' && L.completed(p)) && m.some(p => p.metal === 'silver' && L.completed(p))) w.commit(s); }
  };
  for (let o = 0; o < 90; o++) {
    order();
    if (has('takeoff') && r() < .08) {
      const sh = L.sharedOrders(w.pages).filter(([, m]) => [...m.keys()].every(p => !(+p.laserDoneAt > 0)));
      if (sh.length) { const [oid2, m] = sh[Math.floor(r() * sh.length)], ps = [...m.keys()], p = ps[Math.floor(r() * ps.length)]; for (const id of m.get(p)) w.off(p, id); notes.push(`took order ${oid2} off ${p.sheetId}`); }
    }
    if (has('rose') && r() < .04 && cur.rose) { cur.rose.roseCutAt = Date.now(); cur.rose = null; }
    if (o % 5 === 4) await look(`look ${looks + 1} (order ${o + 1})`);
  }
  for (const m of ['gold', 'silver']) if (cur[m] && cur[m].placements.length) w.full(cur[m]);
  await look('the stream end');
  return { scene, seed, fails, notes, sheets: w.pages.length, word: w.word() };
}

(async () => {
  let bad = 0; const kinds = new Map();
  for (const scene of SCENES) for (const seed of SEEDS) {
    let x; try { x = await day(scene, seed); } catch (e) { x = { scene, seed, fails: ['the assembly threw: ' + e.message], notes: [], word: '' }; }
    for (const f of x.fails) { const k = scene + ' ' + (/(R\d+x?)(?=:)|\[(R\d+ \w+)\]/.exec(f) || [])[0] + (/put them in one set first/.test(f) ? ' (late-line spread)' : ''); kinds.set(k, (kinds.get(k) || 0) + 1); }
    if (x.fails.length) { bad += x.fails.length; console.log(`FAIL ${scene} seed ${seed}: ${x.fails.length} reading(s); first: ${x.fails[0].slice(0, 400)}`); if (VERBOSE) { for (const f of x.fails.slice(0, 12)) console.log('     ' + f.slice(0, 500)); console.log('     final: ' + x.word); } }
  }
  for (const [k, n] of kinds) console.log(`   ${k}: ${n}`);
  if (bad) { console.log(`FAIL: audit-cardinal-fuzz: ${bad} broken rule reading(s)`); process.exit(1); }
  console.log(`PASS: audit-cardinal-fuzz: ${SCENES.join(', ')} x ${SEEDS.length} seeds: no partial sheet in a set, no order across two open sets, no set formed around a group that waits for a partial sheet, the checker agrees`);
})().catch(e => { console.error(e); process.exit(1); });
