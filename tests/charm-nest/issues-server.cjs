/* The SAME truth through the server: the real charmNestLibrary handler (op laserStatus, a pure read) over an in-memory Firestore
 * seeded from the raw records of random shops, compared with the independent oracle:
 *   sheets[i].orderReadiness[orderId].ready === false  exactly for the orders that are an issue for THAT sheet
 *   sheets[i].laser.ready                              the oracle's laser readiness (own steps + no order issue)
 * It also runs the shop under the Sandbox_ collections (body.sandbox) and with only some sheets asked for (the server must find
 * the other sheets that carry the orders, as the Library's single-card read does).
 *
 *   node tests/charm-nest/issues-server.cjs [--shops 40] [--seed 1] [--budget-ms 20000]
 * Never touches a live service: bridge-server.cjs is an in-memory fake. */
'use strict';
const O = require('./issues-oracle.cjs');
const S = require('./issues-shop.cjs');
const { paulShop } = require('./issues-paul.cjs');
const { start } = require('./bridge-server.cjs');
const R = require('../../charm-nest-readiness.js');

const argv = (name, dflt) => { const i = process.argv.indexOf('--' + name); if (i < 0) return dflt; const v = process.argv[i + 1]; return v == null || v.startsWith('--') ? true : isNaN(+v) ? v : +v; };

function seed(st, shop, sandbox) {
  const P = sandbox ? 'Sandbox_' : '', now = Date.now(), ts = { toMillis: () => now };
  st.docs.clear();
  st.put(P + 'Charm_Nest_Runs', 'run-1', { runId: 'run-1', lines: shop.lines, ...(Object.keys(shop.archivedLines).length ? { lineArchive: { parts: 1 } } : {}) });
  if (Object.keys(shop.archivedLines).length) {
    const orders = [...new Set(Object.values(shop.archivedLines).map(l => String(l.orderId)))];
    st.put(P + 'Charm_Nest_Run_Lines', 'part1', { runId: 'run-1', at: 1, seq: 0, orders, keys: Object.keys(shop.archivedLines), json: JSON.stringify(shop.archivedLines) });
  }
  for (const s of shop.sheets) st.put(P + 'Charm_Nest_Sheets', s.id, { ...JSON.parse(JSON.stringify(s)), day: '2026-10-05', stock: { wIn: 6, hIn: 4.5 }, updatedAt: ts, createdAt: ts });
  for (const x of shop.sets) st.put(P + 'Charm_Nest_Sets', x.setId, { ...x, day: '2026-10-05', runId: 'run-1', updatedAt: ts, createdAt: ts });
  // the custom orders' own records (Review → Complete Order, QR label printed, Reopen): the server reads these itself, never the page's word
  for (const [key, c] of Object.entries(shop.customs || {})) st.put(P + 'Charm_Custom_Orders', key, { key, ...c, updatedAtMs: now });
}
async function ask(srv, shop, ids, sandbox) {
  const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ op: 'laserStatus', sheetIds: ids, ...(sandbox ? { sandbox: true } : {}) }) });
  return { status: r.status, ...(await r.json()) };
}
/** Disagreements between the server's answer and the oracle for one shop; subset = the sheet ids asked for. */
async function check(srv, shop, subset, sandbox) {
  seed(srv.st, shop, sandbox);
  const ans = await ask(srv, shop, subset, sandbox), t = O.truth(shop), out = [];
  if (ans.status !== 200 || !Array.isArray(ans.sheets)) return [{ type: 'serverError', detail: JSON.stringify(ans).slice(0, 200) }];
  const live = new Set(Object.keys(t.sheets));
  for (const s of ans.sheets) {
    if (!live.has(s.id)) { out.push({ type: 'archivedSheetReturned', sheet: s.id }); continue; }
    const real = Object.keys(t.sheets[s.id].orders).sort(), rd = s.orderReadiness || {};
    const ghosts = t.sheets[s.id].ghosts, entries = Object.entries(rd).filter(([, v]) => v.ready !== true);
    const shown = entries.filter(([id, v]) => !(ghosts.includes(id) && !Array.isArray(v.blocks))).map(([id]) => id).sort();
    for (const id of ghosts) if (rd[id] && rd[id].ready === true) out.push({ type: 'ghostReady', sheet: s.id, order: id, detail: 'an order with no line records read as ready' });
    // a sheet already cut (or cut before) is laser-ready whatever its orders say: its issues() is silent, its raw map is not compared
    const silent = t.sheets[s.id].completedBefore;
    if (!silent) for (const id of shown) if (!real.includes(id)) out.push({ type: 'falsePositive', sheet: s.id, order: id, detail: JSON.stringify(rd[id]).slice(0, 200) });
    if (!silent) for (const id of real) if (!shown.includes(id)) out.push({ type: 'falseNegative', sheet: s.id, order: id, detail: JSON.stringify(rd[id] || null) });
    // every order carried by the sheet has an entry (ready:true is an answer, an absent entry is "never verified")
    for (const id of new Set([...(s.orders || []), ...(s.poolIds || []).map(p => String(p).split('_')[0]).filter(x => /^\d+$/.test(x))])) if (!rd[id]) out.push({ type: 'unverifiedOrder', sheet: s.id, order: id, detail: 'no orderReadiness entry' });
    if (s.laser && s.laser.ready !== t.sheets[s.id].laserReady) out.push({ type: 'laserReady', sheet: s.id, detail: `server laser.ready ${s.laser.ready}, oracle ${t.sheets[s.id].laserReady}` });
    for (const id of shown) { const b = rd[id]; if (b && b.blocks && !b.blocks.length) out.push({ type: 'emptyBlocks', sheet: s.id, order: id }); }
    // round 8: the Library reads the server's answer (R.issues over the answered record, the page has no rows of its own there): an order whose one honest wait is for a piece on a
    // not-ready sheet in NO set says so, another set is the split, and the pieces are the oracle's, whichever way the page asks
    if (!silent) for (const i of R.issues(s, {}).filter(x => x.step === 'orders' && x.orderId && x.key !== 'unverified')) {
      const r = t.sheets[s.id].orders[i.orderId]; if (!r) continue;
      const wantSplit = r.offenders.some(o => o.split), wantNoSet = !wantSplit && r.offenders.some(o => o.noSet);
      if (!!i.noSet !== wantNoSet) out.push({ type: 'serverNoSet', sheet: s.id, order: i.orderId, detail: `noSet ${!!i.noSet}, oracle ${wantNoSet}` });
      if (i.key !== 'held' && !!i.split !== wantSplit) out.push({ type: 'serverSplit', sheet: s.id, order: i.orderId, detail: `split ${!!i.split}, oracle ${wantSplit}` });
      const have = (i.pieces || []).map(p => p.poolId).sort(), want = r.offenders.map(o => o.poolId).sort();
      if (JSON.stringify(have) !== JSON.stringify(want)) out.push({ type: 'serverPieces', sheet: s.id, order: i.orderId, detail: `${have} vs oracle ${want}` });
    }
  }
  // the sheets asked for (and their set mates) come back
  for (const id of subset) if (live.has(id) && !ans.sheets.some(s => s.id === id)) out.push({ type: 'sheetMissingFromAnswer', sheet: id });
  return out;
}

async function main() {
  const shops = argv('shops', 40), seed0 = argv('seed', 1), budget = argv('budget-ms', 20000), t0 = Date.now(), srv = await start({ receipts: [] });
  let ran = 0, bad = 0, calls = 0; const counts = {};
  try {
    // Paul's shop first (both sheets asked for, then only GF Sheet 1, as the Library's set card does)
    for (const subset of [['gf-sheet-1', 'ss-sheet-1'], ['gf-sheet-1'], ['ss-sheet-1']]) {
      const d = await check(srv, paulShop(), subset, false); calls++;
      if (d.length) { bad++; console.log('PAUL SHOP asked', JSON.stringify(subset), '->', d.length, 'disagreements, first:', JSON.stringify(d[0]).slice(0, 260)); for (const x of d) counts[x.type] = (counts[x.type] || 0) + 1; }
    }
    for (let i = 0; i < shops && Date.now() - t0 < budget; i++, ran++) {
      const spec = S.makeSpec(seed0 * 7919 + i, { noLost: !!argv('no-lost', false), ghost: !!argv('ghost', false) }), shop = S.materialize(spec), live = shop.sheets.filter(s => !s.archived).map(s => s.id);
      if (!live.length) continue;
      const rnd = S.rng(spec.seed + 5), subset = rnd.chance(0.4) ? live : rnd.shuffle(live).slice(0, rnd.int(1, live.length)), sandbox = spec.sandbox;
      const d = await check(srv, shop, subset, sandbox); calls++;
      if (!d.length) continue;
      bad++;
      for (const x of d) counts[x.type] = (counts[x.type] || 0) + 1;
      if (bad <= 3) {
        const fails = async sp => (await check(srv, S.materialize(sp), S.materialize(sp).sheets.filter(s => !s.archived).map(s => s.id), sandbox)).length > 0;
        let cur = spec, changed = true, guard = 0;
        while (changed && guard++ < 60) { changed = false; for (const c of S.shrinkCandidates ? S.shrinkCandidates(cur) : []) { if (await fails(c)) { cur = c; changed = true; break; } } }
        console.log(`\nSERVER DISAGREEMENT (seed ${spec.seed}, sandbox ${sandbox}, asked ${JSON.stringify(subset)}):\n${S.describe(cur)}\n  -> ${d.slice(0, 4).map(x => `${x.type} sheet ${x.sheet} order ${x.order || ''} ${x.detail || ''}`).join('\n     ')}`);
      }
    }
  } finally { srv.close(); }
  console.log(`${bad ? 'FAIL' : 'PASS'}: server laserStatus, ${ran} random shops + Paul's shop (${calls} reads) vs the oracle: ${bad} disagree ${JSON.stringify(counts)} in ${Date.now() - t0} ms`);
  process.exitCode = bad ? 1 : 0;
}
if (require.main === module) main().catch(e => { console.error(e); process.exitCode = 1; });
module.exports = { seed, check };
