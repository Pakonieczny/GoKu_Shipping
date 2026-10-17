/* SETSIM, sets side: a long stream of FAKE orders through a toy sheet-filling model, and the page's OWN set assembly (Gate.assemble, charm-nest-bridge.js, with the page's
 * set rules and the cardinal rule) run after every simulated 10 minutes. No solver, so it runs in seconds on thousands of orders and the exact failing case can be named.
 *
 * Paul, 13 Oct 2026: "A partial GF and SS sheet should never be part of a Set of sheets ... a set of sheets must have at least 1 GF and 1 SS sheet. Once that minimum requirement
 * is satisfied than [a set] is formed, and all other sheets must be formed into subsequent sets and also meet the same criteria."
 *
 *   node tests/charm-nest/setsim-sets.cjs [--root DIR] [--seed N] [--orders N] [--ticks N] [--json FILE] [--measure] [--legacy open|committed] [--late 0.3] [--split P] [--home 0|1]
 *
 * INVARIANTS (every update of the stream, every seed; exit 1 and print the exact case when one fails; --measure prints the numbers and always exits 0):
 *   S1  no partial GF or SS sheet is inside a set (a sheet is completed as charm-nest-set-rules.js isCompleted says: released full, or cut)
 *   S2  every set holds at least 1 completed GF and 1 completed SS sheet
 *   S3  no sheet ever leaves a set by itself, and no set loses or gains a sheet except by a sheet joining (never dissolved, never emptied)
 *   S4  nothing waits that could form a set: the completed GF/SS sheets outside every set, whose sheet-sharing group has no partial GF/SS sheet, never hold both a GF and an SS
 *   S5  an existing set that already holds a partial sheet (made before this rule) is not touched or rebuilt
 *   S6  sheets that share an order stay together: a group is wholly in one set, or wholly outside (Rose Gold waits for its own press and is named, never counted against this)
 *   S7  when every sheet is completed at the end, every completed GF and SS sheet is in a set unless it has no counterpart (nothing is lost)
 *   S8  (SETFIX, Paul 17 Oct 2026: "partial sheets being added without the green line") no Rose Gold, 10K or 14K sheet is in a set unless its green line is cut (nobody presses Include here)
 *   S9  (SETFIX: "way too many sheets in total ended up in set 2") every set is as small as the rules allow: it holds no unit (sheets tied by a shared order) it does not need
 *   S10 anything else the owner's own checker (charm-nest-set-invariants.js, docs/charm-nest/set-rules.md) finds wrong in the run's sets, rule by rule (R-number in the message)
 *   INCREMENTAL (--incremental, on by default): the sheets of a day exist from the start, filling, with Rose Gold / 10K / 14K sheets verified BEFORE their partners complete (or after, or in between),
 *   the groups complete one at a time or sheet by sheet, drafts bridging groups fill up late: after EVERY event the Gate's sets are exactly the sets the rules make of the sheets as they are
 *   (CharmNestSetInvariants.expected), and S1-S4, S6, S8-S10 hold. Both settings of SetRules.PARTIAL_BRIDGE. The runs that complete every sheet together hid the Set 2 of 17 Oct.
 * The toy model: a sheet of one metal takes a whole order-part (all the pieces of the order of that metal) when it fits, else the order goes to the next sheet; a sheet is completed
 * when the next order does not fit and it is 70% full, or at 78%. Orders with lines of two metals tie sheets of two metals into one group (the cardinal rule), which is how a
 * partial GF sheet came into Set-1 of Paul's picture. Pieces never go on two sheets unless `--split P` is given (the old behaviour, to show what splitting does to the sets).      */
'use strict';
const path = require('node:path');
const Lib = require('./setsim-lib.cjs');
const { METALS, CODE, makeStream, partOf, schedule, setWorld, L } = Lib;
const argv = process.argv.slice(2), opt = (k, d) => { const i = argv.indexOf('--' + k); return i >= 0 ? argv[i + 1] : d; };

const SHEET_PT2 = (L.PT(60) - 3) * (L.PT(40) - 3), FILL_CAP = .78, CLOSE_AT = .70;
const areaScaled = c => c.areaPt2 * 1.12;   // (a charm's silhouette grown by its clearance)

/** The toy: sheets of one metal filled whole-order by whole-order. */
function toySheets(scale = 1) {
  const areaOf = c => areaScaled(c) * scale;   // (SETRULE: `scale` shrinks every charm so a toy sheet holds as many orders as a production sheet does, about 45)
  const sheets = Object.fromEntries(METALS.map(m => [m, []]));
  const open = (m, need) => sheets[m].find(s => !s.releaseFull && s.usedPt2 + need <= SHEET_PT2 * FILL_CAP);
  return {
    sheets,
    forced: 0, lifted: 0,
    add(part, tick, splitP, r, home = true) {
      let need = part.pieces.reduce((n, c) => n + areaOf(c), 0), pieces = part.pieces;
      // a later line of an order (the intended nesting: an order is never split): it joins the sheet that holds its mates when that sheet is still open and has room; when it has no room
      // the mates move with it to the next sheet (the whole order starts the sheet) and the gap they leave is for single charms; only a sheet already released cannot take it (a forced split)
      const h = home ? sheets[part.metal].find(x => x.charms.some(c => c.order === part.id)) : null;
      if (h && !h.releaseFull) {
        if (h.usedPt2 + need <= SHEET_PT2 * FILL_CAP) { for (const c of pieces) { h.charms.push(c); h.placements.push({ id: c.id }); h.usedPt2 += areaOf(c); } if (h.usedPt2 >= SHEET_PT2 * (FILL_CAP - .03)) { h.releaseFull = true; h.releasedAt = tick; } return; }
        const mine = h.charms.filter(c => c.order === part.id), lift = mine.reduce((n, c) => n + areaOf(c), 0);
        if (mine.length && lift + need <= SHEET_PT2 * FILL_CAP) {
          h.charms = h.charms.filter(c => c.order !== part.id); h.placements = h.placements.filter(q => !mine.some(c => c.id === q.id)); h.usedPt2 -= lift;
          pieces = mine.concat(pieces); need += lift; this.lifted++;
        }
      } else if (h && h.releaseFull) this.forced++;
      let s = open(part.metal, need);
      if (!s) {   // nothing open takes it whole: sheets that are 70% full and cannot take it are completed, and a new sheet starts
        for (const x of sheets[part.metal]) if (!x.releaseFull && x.usedPt2 >= SHEET_PT2 * CLOSE_AT) { x.releaseFull = true; x.releasedAt = tick; }
        s = open(part.metal, need);
        if (!s) { s = { metal: part.metal, page: sheets[part.metal].length + 1, usedPt2: 0, charms: [], placements: [], releaseFull: false, born: tick }; sheets[part.metal].push(s); }
      }
      // the old behaviour, on request: with probability P a multi-piece order puts its last piece on the next sheet
      if (splitP && pieces.length > 1 && r() < splitP) {
        const tail = pieces.slice(-1); pieces = pieces.slice(0, -1);
        let t = sheets[part.metal].find(x => x !== s && !x.releaseFull) || (() => { const n = { metal: part.metal, page: sheets[part.metal].length + 1, usedPt2: 0, charms: [], placements: [], releaseFull: false, born: tick }; sheets[part.metal].push(n); return n; })();
        for (const c of tail) { t.charms.push(c); t.placements.push({ id: c.id }); t.usedPt2 += areaOf(c); }
      }
      for (const c of pieces) { s.charms.push(c); s.placements.push({ id: c.id }); s.usedPt2 += areaOf(c); }
      if (s.usedPt2 >= SHEET_PT2 * (FILL_CAP - .03)) { s.releaseFull = true; s.releasedAt = tick; }
    },
    all: () => METALS.flatMap(m => sheets[m]),
    // the shape the page's set assembly reads
    asNest: () => METALS.flatMap(m => sheets[m].map(s => Object.assign(s, { usablePt2: SHEET_PT2, freePt2: SHEET_PT2 - s.usedPt2, density: s.usedPt2 / SHEET_PT2 }))),
  };
}

async function run(o = {}) {
  const root = path.resolve(o.root || Lib.REPO), seed = +o.seed || 1, orders = +o.orders || 600, splitP = +o.split || 0, batches = [2, 3, 5, 4];
  const S = L.solverOf(root), stream = makeStream(S, { seed, orders, mix: o.mix }), toy = toySheets(), world = setWorld(root), SR = world.SR, r = L.rng(seed * 31 + 5);
  const info = [], fails = [], limits = [];
  // S6 and S7 are asserted when no order is ever split. With `--late`, a line that arrives after its mates' sheet was RELEASED cannot join it (a released sheet is never rebuilt): that
  // order is split by force, and the sets it ties together (S6) or leaves waiting at the end (S7) are MEASURED as forced-split consequences, not failed (the nesting side, not set assembly).
  const forcedMode = !!(o.mix && o.mix.late > 0), note = (rule, msg, tick) => { if (forcedMode && (rule === 'S6' || rule === 'S7' || rule === 'S10')) { if (limits.length < 60) limits.push({ rule: rule + '*', tick, msg }); return; } if (fails.length < 60) fails.push({ rule, tick, msg }); };
  const memberOf = new Map(), setsAt = new Map();   // sheet id -> set id (S3), set id -> sheet ids
  const waitStart = new Map(), waits = [];          // completed GF/SS sheets: ticks from completion to joining a set
  const everPartial = new Set(), blockedSheets = new Set(); let tick = 0, maxPartialIn = 0, blockedTicks = 0;
  const check = async (final) => {
    world.sync(toy.asNest()); await world.assemble();
    const pg = world.gatePages(), sets = world.sets(), byId = new Map(pg.map(p => [p.sheetId, p])), label = p => `${CODE[p.metal]} ${p.page}`;
    const inSet = p => !!p && !p.draft && !!p.setId;
    // S1 / S2
    for (const p of pg) if (inSet(p) && p.setId !== 'set-legacy' && ['gold', 'silver'].includes(p.metal) && !SR.isCompleted(p)) { everPartial.add(p.sheetId); note('S1', `${label(p)} (${Math.round(100 * p.density)}% full, still filling) is in ${setName(sets, p)} with ${(sets.find(x => x.setId === p.setId) || { ids: [] }).ids.filter(id => id !== p.sheetId).map(id => byId.get(id)).filter(Boolean).map(x => label(x) + (SR.isCompleted(x) ? ' done' : ' filling')).join(', ')}; sheets sharing its orders: ${groupOfSheet(world, pg, p).map(x => label(x) + (SR.isCompleted(x) ? ' done' : ' filling') + (inSet(x) ? ' in a set' : ' outside')).join(', ') || 'none'}`, tick); }
    maxPartialIn = Math.max(maxPartialIn, pg.filter(p => inSet(p) && p.setId !== 'set-legacy' && ['gold', 'silver'].includes(p.metal) && !SR.isCompleted(p)).length);
    for (const s of sets) { const members = s.ids.map(id => byId.get(id)).filter(Boolean), v = SR.validSet(members); if (!v.ok) note('S2', `${s.name} holds ${members.map(label).join(', ') || 'no sheet'}: no completed ${v.missing.join(' / ')}`, tick); }
    const fresh = sets.filter(s => !setsAt.has(s.setId));   // (the sets that formed in THIS update)
    // S3 membership only grows
    for (const s of sets) { const prev = setsAt.get(s.setId) || []; const lost = prev.filter(id => !s.ids.includes(id)); if (lost.length) note('S3', `${s.name} lost ${lost.map(id => byId.get(id) ? label(byId.get(id)) : id).join(', ')}`, tick); setsAt.set(s.setId, s.ids.slice()); }
    for (const [id, set] of memberOf) { const p = byId.get(id); if (!p || !inSet(p) || p.setId !== set) note('S3', `${p ? label(p) : id} left ${set}`, tick); }
    for (const p of pg) if (inSet(p)) memberOf.set(p.sheetId, p.setId);
    // S4 nothing waits that could form a set
    const norm = pg.map(p => world.SO.fromPage(p)).filter(Boolean), ends = pg.filter(p => !inSet(p) && ['gold', 'silver'].includes(p.metal) && !SR.isCompleted(p)).map(p => p.sheetId);
    const groups = world.SO.core.formation ? world.SO.core.formation(norm, ends, SR.PARTIAL_BRIDGE).groups : world.SO.core.groups(norm), grp = new Map(); for (const g of groups) for (const id of g.ids) grp.set(id, g);
    const blocked = p => { const g = grp.get(p.sheetId); return !!g && g.ids.some(id => { const q = byId.get(id); return q && ['gold', 'silver'].includes(q.metal) && !SR.isCompleted(q); }); };
    const free = pg.filter(p => !inSet(p) && ['gold', 'silver'].includes(p.metal) && SR.isCompleted(p) && !blocked(p) && !(grp.get(p.sheetId) && grp.get(p.sheetId).ids.some(id => inSet(byId.get(id)))));
    for (const p of pg) if (!inSet(p) && ['gold', 'silver'].includes(p.metal) && SR.isCompleted(p) && blocked(p)) { blockedTicks++; blockedSheets.add(p.sheetId); }
    for (const p of free) if (!waitStart.has(p.sheetId)) waitStart.set(p.sheetId, tick);
    if (free.some(p => p.metal === 'gold') && free.some(p => p.metal === 'silver')) note('S4', `completed ${free.map(label).join(', ')} wait outside every set although together they hold a completed GF and a completed SS`, tick);
    for (const p of pg) if (inSet(p) && waitStart.has(p.sheetId)) { waits.push(tick - waitStart.get(p.sheetId)); waitStart.delete(p.sheetId); }
    // S6 sheets that share an order are together
    for (const g of groups) { const ps = g.ids.map(id => byId.get(id)).filter(p => p && p.metal !== 'rose' && !['gold10k', 'gold14k'].includes(p.metal) && !(['gold', 'silver'].includes(p.metal) && !SR.isCompleted(p) && !inSet(p))), sets2 = new Set(ps.map(p => (inSet(p) ? p.setId : 'none'))); if (sets2.size > 1 && ps.some(inSet)) note('S6', `orders ${g.orders.slice(0, 3).join(', ')} are on ${ps.map(p => label(p) + (inSet(p) ? ' (' + p.setId + ')' : ' (no set)')).join(' and ')}`, tick); }
    // S8 no gold-family sheet in a set without its green line (nobody pressed Include or Cut Sheet in this run)
    for (const p of pg) if (inSet(p) && p.setId !== 'set-legacy' && ['rose', 'gold10k', 'gold14k'].includes(p.metal) && !p.roseCutAt && !p.laserDoneAt && !p.solidPick) note('S8', `${label(p)} (${Math.round(100 * p.density)}% full) is in ${setName(sets, p)} without its green line cut`, tick);
    // S9 a set is as small as the rules allow WHEN IT FORMS: it holds no unit (sheets tied by a shared order) it does not need. (Judged at its formation: an order line that reaches a Rose Gold sheet
    // later can tie two sheets of a set that had no link when it formed, and a set that exists is never dissolved: that later state is the checker's to report, Gate.violations, below)
    for (const st of fresh) { const members = st.ids.map(id => byId.get(id)).filter(Boolean), spare = st.setId === 'set-legacy' ? [] : SR.surplus(members, groups); if (spare.length) note('S9', `${st.name} formed with ${spare.map(label).join(', ')} that it does not need (${members.map(label).join(', ')}): it is valid without ${spare.length === 1 ? 'it' : 'them'}`, tick); }
    // S10 the rest of the owner's own rules, rule by rule, as the Gate itself reads them after every assembly (a checkout from before the checker has no verdict). R7 surplus is S9's, at formation.
    const verdict = legacy ? null : world.selfCheck();
    if (verdict) for (const v of verdict.violations) { if (v.rule === 'surplus') continue; note(v.rule === 'notReady' ? 'S8' : 'S10', `[${v.ruleNo} ${v.rule}] ${v.message}`, tick); }
    return { pg, sets };
  };
  // S5: a set made before the rule, holding a filling GF sheet (as Set-1 of Paul's picture), must stay exactly as it is
  let legacy = null;
  if (o.legacy) {
    const mk = (metal, page, used, full) => { const s = { metal, page, usedPt2: used * SHEET_PT2, charms: [], placements: [], releaseFull: full, born: 0 }; for (let k = 0; k < 6; k++) { const id = `42000000${page}${metal.length}${k}_1_1`; s.charms.push({ id, poolId: id, order: id.split('_')[0] }); s.placements.push({ id }); } return s; };
    const a = mk('gold', 90, .76, true), b = mk('silver', 91, .75, true), c = mk('gold', 92, .55, false);
    toy.sheets.gold.push(a, c); toy.sheets.silver.push(b);
    world.sync(toy.asNest());
    for (const s of [a, b, c]) { const g = world.gatePages().find(p => p.metal === s.metal && p.page === s.page); g.draft = false; g.setId = 'set-legacy'; g.sheetIndex = 1; }
    legacy = { setId: 'set-legacy', ids: world.gatePages().filter(p => p.setId === 'set-legacy').map(p => p.sheetId).sort() };
    vm_run(world, `B.sets.set('set-legacy',{runId:RUN,group:'dispatch',seq:1,day:today(),setId:'set-legacy',name:'Set-1',sheetIds:${JSON.stringify(legacy.ids)},labelFiles:[],materials:['gold','silver'],orders:{},sheetNos:{}${o.legacy === 'committed' ? ",committedAt:5,status:'complete'" : ''}}); seq=2;`);
    legacy.committed = o.legacy === 'committed';
  }
  for (const ev of schedule(stream, batches)) {
    if (ev.tick > (+o.ticks || 1e9)) break;
    tick = ev.tick;
    for (const a of ev.arrivals) for (const m of METALS) { const part = partOf(a.order, m, a.lines); if (part) toy.add(part, tick, splitP, r, o.home !== '0' && o.home !== 0); }
    await check(false);
    if (legacy) { const s = world.sets().find(x => x.setId === 'set-legacy'), same = s && JSON.stringify(s.ids.slice().sort()) === JSON.stringify(legacy.ids); if (!same) { if (legacy.committed) note('S5', 'a COMMITTED set made before the rule changed: ' + JSON.stringify(s && s.ids), tick); else if (!legacy.ejected) { legacy.ejected = tick; info.push(`an OPEN set made before the rule (${legacy.ids.length} sheets, one of them filling) changed at update ${tick}: it now holds ${JSON.stringify(s && s.ids)}`); } } }
  }
  // the end: every sheet is completed (the stream stops; the shop releases what is left)
  for (const s of toy.all()) if (!s.releaseFull) { s.releaseFull = true; s.releasedAt = ++tick; }
  const { pg, sets } = await check(true);
  const inSet = p => !p.draft && p.setId, left = pg.filter(p => ['gold', 'silver'].includes(p.metal) && !inSet(p));
  const hasGF = pg.filter(p => p.metal === 'gold').length, hasSS = pg.filter(p => p.metal === 'silver').length;
  // S7: leftovers are only sheets of a metal with no counterpart waiting (all done, so a GF and an SS outside would form a set), or a group tied to Rose Gold alone
  const leftGF = left.filter(p => p.metal === 'gold'), leftSS = left.filter(p => p.metal === 'silver');
  if (leftGF.length && leftSS.length) note('S7', `at the end ${leftGF.length} GF and ${leftSS.length} SS sheet(s) are completed and outside every set: ${left.map(p => CODE[p.metal] + ' ' + p.page).join(', ')}`, tick);
  const waitMean = waits.length ? waits.reduce((a, b) => a + b, 0) / waits.length : 0;
  return { root: root === path.resolve(Lib.REPO) ? 'repo' : root, seed, orders, ticks: tick, sheets: toy.all().length, gfss: toy.all().filter(s => ['gold', 'silver'].includes(s.metal)).length, sets: sets.length, partialInSetsEver: everPartial.size, maxPartialInSets: maxPartialIn,
    info, blockedSheets: blockedSheets.size, blockedSheetUpdates: blockedTicks, leftOutside: left.length, completedWaitMean: +waitMean.toFixed(2), completedWaitMax: waits.length ? Math.max(...waits) : 0, hasGF, hasSS, limits, limitRules: [...new Set(limits.map(f => f.rule))], limitReadings: limits.length, forcedSplits: toy.forced, liftedOrders: toy.lifted, fails, failRules: [...new Set(fails.map(f => f.rule))] };
}
const setName = (sets, p) => (sets.find(x => x.setId === p.setId) || { name: p.setId }).name;
function vm_run(world, code) { require('node:vm').runInContext(code, world.c); }
function groupOfSheet(world, pg, p) {
  const sheets = pg.map(x => world.SO.fromPage(x)).filter(Boolean), g = world.SO.core.groups(sheets).find(x => x.ids.includes(p.sheetId));
  return g ? g.ids.filter(id => id !== p.sheetId).map(id => pg.find(x => x.sheetId === id)).filter(Boolean) : [];
}
module.exports = { run, toySheets };

if (require.main === module) (async () => {
  const seeds = String(opt('seed', '1,2,3')).split(','), res = [];
  for (const seed of seeds) res.push(await run({ root: opt('root'), seed, orders: opt('orders', 600), ticks: opt('ticks'), mix: opt('late') ? { late: +opt('late') } : undefined, split: opt('split'), legacy: opt('legacy'), home: opt('home') }));
  let bad = 0;
  for (const r of res) {
    console.log(`seed ${r.seed}: ${r.orders} orders, ${r.sheets} sheets (${r.gfss} GF/SS), ${r.sets} sets, partial GF/SS inside sets: ${r.partialInSetsEver} sheets ever (peak ${r.maxPartialInSets}), completed sheets held out only because a sheet sharing an order is still filling: ${r.blockedSheets} (${r.blockedSheetUpdates} sheet-updates), completed sheets left outside at the end: ${r.leftOutside}, forced splits (the mates' sheet was already released) ${r.forcedSplits}, orders moved whole to the next sheet ${r.liftedOrders}, completed sheet waits ${r.completedWaitMean} (max ${r.completedWaitMax}) updates, broken rules: ${r.failRules.join(' ') || 'none'}`);
    for (const t of r.info) console.log('   note: ' + t);
    const lim = {}; for (const f of r.limits) if (!lim[f.rule]) { lim[f.rule] = f; console.log(`   ${f.rule} (a consequence of a split by force, measured not failed) first at update ${f.tick}: ${f.msg}`); }
    const first = {}; for (const f of r.fails) if (!first[f.rule]) { first[f.rule] = f; console.log(`   ${f.rule} first at update ${f.tick}: ${f.msg}`); }
    bad += r.fails.length;
  }
  if (opt('json')) require('node:fs').writeFileSync(opt('json'), JSON.stringify(res, null, 1));
  // the INCREMENTAL scenario (setsim-incremental.cjs: Rose Gold sheets verified before their partners complete, groups completing one at a time, in both settings of SetRules.PARTIAL_BRIDGE)
  if (!argv.includes('--no-incremental')) {
    const { incremental } = require('./setsim-incremental.cjs'); let inc = 0, steps = 0, skipped = 0;
    for (let seed = 1; seed <= +opt('days', 12); seed++) for (const bridge of ['hold', 'spread']) {
      const r = await incremental({ root: path.resolve(opt('root') || Lib.REPO), seed, bridge });
      if (r.skipped) { skipped++; continue; } steps += r.steps;
      for (const f of r.fails || []) { inc++; console.log(`   INCREMENTAL seed ${f.seed} (${f.bridge}) step ${f.step} after "${f.what}": ${f.msg}`); }
    }
    console.log(skipped ? '   incremental scenario skipped (this checkout has no set checker)' : `   incremental scenario: ${+opt('days', 12)} days x 2 settings, ${steps} events, ${inc ? inc + ' broken' : 'every event judged by the rules: ok'}`);
    bad += inc;
  }
  if (!argv.includes('--measure') && bad) { console.log('FAIL: ' + bad + ' broken invariant reading(s)'); process.exit(1); }
  console.log(bad ? 'MEASURED (not asserted): ' + bad + ' broken invariant reading(s)' : 'PASS: setsim-sets: every invariant holds at every update on ' + res.length + ' seed(s)');
})().catch(e => { console.error(e); process.exit(1); });
