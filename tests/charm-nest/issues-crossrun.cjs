/* Orders whose lines live in DIFFERENT runs, through the real server handler (op laserStatus over the in-memory Firestore).
 * The server reads the lines of the runs of the sheets it is asked about (and of the sheets that carry the rest of an order); this
 * measures what that does to the Issues list when an order's lines are split over two runs, against the oracle that sees every line:
 *
 *   pure    every sheet's pieces come from lines of ONE run (the sheet's own run), but an ORDER's lines are in two runs: the cleanest case
 *           (a second pull added a line to an order already pulled); nothing about a single sheet is mixed
 *   split   each sheet is in the run most of its pieces' lines are in (some sheets mix lines of both runs)
 *   stray   every sheet is in run-1 while some lines (and so their pieces) belong to run-2 (a piece pooled in an older run, placed later)
 *
 * Counts per (sheet, order): true issue shown, missed issue (false negative), invented issue (false positive), and "not checked yet"
 * (an order the server could not read: no blocks list) for an order that really has lines.
 *
 *   node tests/charm-nest/issues-crossrun.cjs [--shops 300] [--seed 1]        informational: prints the counts, exit 0
 *   node tests/charm-nest/issues-crossrun.cjs --strict                        the ACCEPTANCE: exit 1 unless zero missed, zero invented, zero false 'not checked yet'
 *   The run documents are seeded as the page writes them: run.orders = the run's open order ids (lines moved to the archive leave it), the archive
 *   parts (Charm_Nest_Run_Lines) carry their own orders / keys / json. The fake store counts the documents it reads in st.reads (reset per call).
 * Never touches a live service. */
'use strict';
const O = require('./issues-oracle.cjs');
const S = require('./issues-shop.cjs');
const { start } = require('./bridge-server.cjs');

const argv = (name, dflt) => { const i = process.argv.indexOf('--' + name); if (i < 0) return dflt; const v = process.argv[i + 1]; return v == null || v.startsWith('--') ? true : isNaN(+v) ? v : +v; };

/** Which run each line is in (a share of the lines in run-2), and which run each sheet is in. */
function assign(shop, mode, seed) {
  const r = S.rng(seed), all = { ...shop.archivedLines, ...shop.lines }, runOf = {};
  const lineOfPid = pid => String(pid).slice(0, String(pid).lastIndexOf('_'));
  if (mode === 'pure') {
    // lines that share a sheet stay together (one run per connected group of lines); an order may still be split over groups
    const up = {}, find = k => (up[k] === k ? k : (up[k] = find(up[k])));
    for (const k of Object.keys(all)) up[k] = k;
    for (const s of shop.sheets.filter(x => !x.archived)) { const ks = [...new Set((s.poolIds || []).map(lineOfPid))].filter(k => all[k]); for (const k of ks.slice(1)) up[find(k)] = find(ks[0]); }
    const runOfRoot = {};
    for (const k of Object.keys(all).sort()) { const root = find(k); if (!(root in runOfRoot)) runOfRoot[root] = r.chance(0.35) ? 'run-2' : 'run-1'; runOf[k] = runOfRoot[root]; }
    const sheetRun = {};
    for (const s of shop.sheets) { const ks = (s.poolIds || []).map(lineOfPid).filter(k => runOf[k]); sheetRun[s.id] = ks.length ? runOf[ks[0]] : 'run-1'; }
    return { runOf, sheetRun };
  }
  for (const k of Object.keys(all).sort()) runOf[k] = r.chance(0.3) ? 'run-2' : 'run-1';
  const sheetRun = {};
  for (const s of shop.sheets) {
    if (mode === 'stray') { sheetRun[s.id] = 'run-1'; continue; }
    const votes = {};
    for (const pid of s.poolIds || []) { const run = runOf[String(pid).slice(0, String(pid).lastIndexOf('_'))] || 'run-1'; votes[run] = (votes[run] || 0) + 1; }
    sheetRun[s.id] = (votes['run-2'] || 0) > (votes['run-1'] || 0) ? 'run-2' : 'run-1';
  }
  return { runOf, sheetRun };
}
function seedTwoRuns(st, shop, { runOf, sheetRun }) {
  const now = Date.now(), ts = { toMillis: () => now };
  st.docs.clear();
  for (const [n, run] of ['run-1', 'run-2'].entries()) {
    const live = {}, archived = {};
    for (const [k, l] of Object.entries(shop.lines)) if (runOf[k] === run) live[k] = l;
    for (const [k, l] of Object.entries(shop.archivedLines)) if (runOf[k] === run) archived[k] = l;
    const open = [...new Set(Object.values(live).map(l => String(l.orderId)))].sort();
    st.put('Charm_Nest_Runs', run, { runId: run, lines: live, orders: open, ...(Object.keys(archived).length ? { lineArchive: { parts: 1 } } : {}) });
    if (Object.keys(archived).length) st.put('Charm_Nest_Run_Lines', 'part-' + run, { runId: run, at: 1 + n, seq: 0, orders: [...new Set(Object.values(archived).map(l => String(l.orderId)))].sort(), keys: Object.keys(archived), json: JSON.stringify(archived) });
  }
  for (const s of shop.sheets) st.put('Charm_Nest_Sheets', s.id, { ...JSON.parse(JSON.stringify(s)), runId: sheetRun[s.id] || 'run-1', day: '2026-10-05', stock: { wIn: 6, hIn: 4.5 }, updatedAt: ts, createdAt: ts });
  for (const x of shop.sets) st.put('Charm_Nest_Sets', x.setId, { ...x, day: '2026-10-05', runId: 'run-1', updatedAt: ts, createdAt: ts });
}
const ask = async (srv, ids) => { srv.st.reads = 0; const r = await (await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ op: 'laserStatus', sheetIds: ids }) })).json(); r.reads = srv.st.reads; return r; };

async function main() {
  const shops = argv('shops', 300), seed0 = argv('seed', 1), srv = await start({ receipts: [] }), res = {};
  try {
    for (const mode of ['pure', 'split', 'stray']) for (const asked of ['all', 'one']) {
      const c = res[`${mode}/${asked}`] = { reads: 0, maxReads: 0, shops: 0, pairs: 0, real: 0, shown: 0, missed: 0, invented: 0, notChecked: 0, shopsWrong: 0, example: null };
      for (let i = 0; i < shops; i++) {
        const spec = S.makeSpec(seed0 * 4001 + i, { noLost: true, ghost: false, sandbox: false }), shop = S.materialize(spec), t = O.truth(shop), live = shop.sheets.filter(s => !s.archived);
        if (!live.length) continue;
        const a = assign(shop, mode, spec.seed + 1);
        if (!Object.values(a.runOf).includes('run-2')) continue;
        seedTwoRuns(srv.st, shop, a);
        const rnd = S.rng(spec.seed + 9), ids = asked === 'all' ? live.map(s => s.id) : [rnd.pick(live).id], ans = await ask(srv, ids);
        c.shops++; c.reads += ans.reads; c.maxReads = Math.max(c.maxReads, ans.reads); let wrong = false;
        for (const s of ans.sheets || []) {
          if (!t.sheets[s.id] || !ids.includes(s.id) || t.sheets[s.id].completedBefore) continue;
          const real = Object.keys(t.sheets[s.id].orders), rd = s.orderReadiness || {};
          const notChecked = Object.entries(rd).filter(([id, v]) => v.ready !== true && !Array.isArray(v.blocks)).map(([id]) => id);
          const shown = Object.entries(rd).filter(([id, v]) => v.ready !== true && Array.isArray(v.blocks)).map(([id]) => id);
          c.real += real.length; c.shown += shown.filter(id => real.includes(id)).length;
          for (const id of real) if (!shown.includes(id)) { c.missed++; wrong = true; if (!c.example) c.example = { seed: spec.seed, sheet: s.id, order: id, kind: 'missed', rd: rd[id] || null, runs: a }; }
          for (const id of shown) if (!real.includes(id)) { c.invented++; wrong = true; if (!c.example) c.example = { seed: spec.seed, sheet: s.id, order: id, kind: 'invented', rd: rd[id] }; }
          for (const id of notChecked) { c.notChecked++; wrong = true; if (!c.example) c.example = { seed: spec.seed, sheet: s.id, order: id, kind: 'notChecked', rd: rd[id], runs: a }; }
        }
        if (wrong) c.shopsWrong++;
      }
    }
  } finally { srv.close(); }
  let bad = 0;
  for (const [k, c] of Object.entries(res)) {
    console.log(`${k.padEnd(10)} ${String(c.shops).padStart(4)} shops with lines in two runs: real issues ${c.real}, shown right ${c.shown}, MISSED ${c.missed}, INVENTED ${c.invented}, "not checked yet" for an order that has lines ${c.notChecked}; shops with a wrong answer ${c.shopsWrong}; documents read per laserStatus: ${(c.reads / Math.max(1, c.shops)).toFixed(1)} on average, ${c.maxReads} at most (st.reads)`);
    if (c.example && argv('verbose', false)) console.log('   first:', JSON.stringify(c.example).slice(0, 400));
    bad += c.missed + c.invented + c.notChecked;
  }
  if (argv('strict', false) && bad) process.exitCode = 1;
}
if (require.main === module) main().catch(e => { console.error(e); process.exitCode = 1; });
