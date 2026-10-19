// HELDRECHECK (Paul, 19 Oct 2026, order 4204452679): a line the SYSTEM held for its design (the broken heart BEST_4003 drawn as ONE outline with the zigzag as a line inside it: bodyPlanFor's "does not give the 2 separate
// bodies") is checked again, without a person, when the library's drawing of its SKU changes (Paul's corrected master: the Left + Right pair, two closed bodies, written over the same file charmnest/master/BEST_4003.ai).
// The REAL page over the fake site (the engraving-UI real world, _engraveRealWorld.cjs: the real intake, pool and classifier). Three orders of the listing:
//   real   4204452679 "Sisters"     held by the system for the body reason → after the library change: 2 halves pooled (bodies 0, 1), the hold and its stamp gone, a timeline line "re-checked against the new drawing"
//   person 4204452602 "Mom / Me"     held for the body reason, then ON HOLD by a person → stays held, told once (row.redrawNote, a timeline note)
//   then a second identical read does nothing; and a page RELOAD with a line held against the old drawing (stale stamp, restored from the checkpoint) and one held before the stamp existed (Paul's own line) re-checks both
//   node tests/charm-nest/redraw-held-recheck.cjs [playwright-core dir]        (PW_DIR=<dir> CHROMIUM=<chrome>; AFTER_AI=<the corrected per-SKU file>)
'use strict';
const fs = require('fs'), path = require('path');
if (process.argv[2]) process.env.PW_DIR = process.argv[2];
const R = require('./_engraveRealWorld.cjs'), Base = require('./_orderFactsWorld.cjs');
const fails = []; let nCheck = 0;
const check = (ok, msg) => { nCheck++; if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
const sleep = ms => new Promise(r => setTimeout(r, ms));
const DESIGN = 'BEST_4003', AI_PATH = 'charmnest/master/BEST_4003.ai', OLD_H = 'b3574003-one', NEW_H = 'b3574003-two';
const AFTER_AI = process.env.AFTER_AI || '/tmp/claude-0/-home-user/a2b86e36-2b5c-5271-a038-cf7fe18ee67a/scratchpad/bheart/BEST_4003_single.ai';

const f = x => (Math.round(x * 1000) / 1000).toString();
const circle = (cx, cy, r) => { const k = 0.5522847498 * r; return `${f(cx + r)} ${f(cy)} m ${f(cx + r)} ${f(cy + k)} ${f(cx + k)} ${f(cy + r)} ${f(cx)} ${f(cy + r)} c ${f(cx - k)} ${f(cy + r)} ${f(cx - r)} ${f(cy + k)} ${f(cx - r)} ${f(cy)} c ${f(cx - r)} ${f(cy - k)} ${f(cx - k)} ${f(cy - r)} ${f(cx)} ${f(cy - r)} c ${f(cx + k)} ${f(cy - r)} ${f(cx + r)} ${f(cy - k)} ${f(cx + r)} ${f(cy)} c h`; };
const pdfOf = (content, title) => {
  const objs = ['<< /Type /Pages /Kids [ 4 0 R ] /Count 1 >>', '<< /Type /Catalog /Pages 1 0 R /OCProperties << /OCGs [ 5 0 R ] /D << /Order [ 5 0 R ] /ON [ 5 0 R ] /BaseState /ON >> >> >>', `<< /Producer (HELDRECHECK test drawing) /Title (${title}) >>`,
    '<< /Type /Page /Parent 1 0 R /Resources << /Properties << /ocCut 5 0 R >> >> /MediaBox [ 0 0 60 50 ] /Contents 6 0 R >>', '<< /Type /OCG /Name (CUT) >>', `<< /Length ${Buffer.byteLength(content)} >>\nstream\n${content}endstream`];
  let out = '%PDF-1.7\n'; const at = [];
  objs.forEach((o, i) => { at.push(Buffer.byteLength(out)); out += `${i + 1} 0 obj\n${o}\nendobj\n`; });
  const x0 = Buffer.byteLength(out);
  out += `xref\n0 ${objs.length + 1}\n0000000000 65535 f \n` + at.map(n => String(n).padStart(10, '0') + ' 00000 n \n').join('') + `trailer\n<< /Size ${objs.length + 1} /Root 2 0 R /Info 3 0 R >>\nstartxref\n${x0}\n%%EOF\n`;
  return Buffer.from(out, 'latin1');
};
/** The OLD drawing: ONE heart outline with the zigzag drawn as an open line inside it (no cut between the halves), a hole in each half: one body. */
function oneOutlineHeartAi() {
  const HX = t => 30 + 1.5 * 16 * Math.pow(Math.sin(t), 3), HY = t => 26 + 1.5 * (13 * Math.cos(t) - 5 * Math.cos(2 * t) - 2 * Math.cos(3 * t) - Math.cos(4 * t));
  const heart = Array.from({ length: 97 }, (_, i) => { const t = 2 * Math.PI * i / 96; return [HX(t), HY(t)]; });
  const top = HY(0), tip = HY(Math.PI), zig = [];
  for (let i = 1; i <= 9; i++) zig.push([30 + (i % 2 ? 1.8 : -1.8), top - (top - tip) * i / 10]);
  const line = pts => pts.map((p, i) => `${f(p[0])} ${f(p[1])} ${i ? 'l' : 'm'}`).join(' ');
  const content = ['/OC /ocCut BDC', `q 0 0 0 RG 0.283 w ${line(heart)} h S Q`, `q 0 0 0 RG 0.283 w ${line(zig)} S Q`, `q 0 0 0 RG 0.283 w ${circle(17, 37, 1.3)} S Q`, `q 0 0 0 RG 0.283 w ${circle(43, 37, 1.3)} S Q`, 'EMC'].join('\n') + '\n';
  return pdfOf(content, 'BEST_4003_ONE');
}

(async () => {
  if (!fs.existsSync(AFTER_AI)) { console.log('  - no corrected heart file at ' + AFTER_AI + ' (AFTER_AI=<file>): not run'); return; }
  R.DESIGNS[DESIGN] = { file: 'BEST_4003.ai', make: oneOutlineHeartAi, entry: { sku: DESIGN, widthPt: 48, heightPt: 39, areaPt2: 1300, members: 4, holes: 2, engravable: true, engravableBy: 'index', upAngle: 90, upSource: 'drawn', charmHash: OLD_H, masterHash: 'm-best-old', masterName: 'BEST FRIENDS_2504.ai', indexedAtMs: Date.now() - 86400000, backKeepOut: [] } };
  const TITLE = 'Best Friends Necklace Set of 2, Set of 2 Necklaces, Best Friends Forever Personalized Jewelry, BFF Jewelry';
  const DROP = { name: 'Personalization', value: 'SILVER·Front', valueId: '4417', propertyId: '513' };
  const fx = (rid, buyer, words, m) => ({ rid, sku: 'Best_4003', listing: '19' + rid.slice(2), title: TITLE, buyer, placed: { day: 0, h: 11, m }, ship: 5, variations: [DROP], personalization: [words], metalKey: 'silver', metalLabel: 'Sterling Silver' });
  R.ORDERS.real = fx('4204452679', 'Sam Test', 'Sisters', 40); R.ORDERS.person = fx('4204452602', 'Jo Test', 'Mom\nMe', 30);
  R.ORDERS.stale = fx('4204452603', 'Al Test', 'Ann\nBea', 20); R.ORDERS.legacy = fx('4204452604', 'Bo Test', 'Cy\nDi', 10);
  for (const k of ['real', 'person', 'stale', 'legacy']) R.EXPECT_JOBS[k] = 0;
  const base = Base.orderOf;
  Base.orderOf = new Function('return function orderOf(fx, now) { const o = (' + base.toString() + ')(fx, now); for (const l of o.lines) if (fx.metalKey) { l.metalKey = fx.metalKey; l.metalLabel = fx.metalLabel || ""; } return o; }')();
  const w = await R.world({ viewport: { width: 1500, height: 950 } });
  if (!w) { console.log('no playwright-core (pass its node_modules dir as the first argument)'); return; }
  const st = w.srv.st, page = w.page;
  const rowOf = rid => page.evaluate(rid => { const r = B.orders.rows.find(x => x.order.receiptId === rid); if (!r) return null; const ids = r.poolIds || [];
    const charms = allSheets().flatMap(s => s.charms || []).filter(c => ids.includes(c.poolId)).sort((a, b) => ids.indexOf(a.poolId) - ids.indexOf(b.poolId));
    return { key: r.key, state: r.state, reason: r.reason, hold: r.hold || null, poolIds: ids, designHold: r.designHold || null, redrawNote: r.redrawNote || null, problems: (r.problems || []).map(p => p.kind), bodies: charms.map(c => c.oneBody), widths: charms.map(c => Math.round(c.widthPt)),
      hashes: ids.map(id => (B.pool.rows.get(id) || {}).charmHash) }; }, rid);
  const until = async (fn, ms, what) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) { check(false, 'timed out: ' + what); return null; } await sleep(200); } };
  const tl = rid => st.list('Order_Timeline').filter(e => e._id.startsWith(rid + '~')).map(e => e.text || '');
  const flushTl = () => page.evaluate(() => { try { OrderTimeline.flush && OrderTimeline.flush(); } catch (_) {} });
  const poolWrites = (from, rids) => st.calls.slice(from).filter(c => c.name === 'charmNestLibrary' && /^(poolPut|poolUpdate)$/.test(String(c.op)) && rids.some(r => JSON.stringify(c.body).includes(r)));
  try {
    /* 1 · the old drawing: one body → both lines held by the system for the body reason */
    await R.build(w, ['real', 'person']);
    const r0 = await rowOf('4204452679'), p0 = await rowOf('4204452602');
    console.log(JSON.stringify(r0));
    check(r0.state === 'held' && /does not give the 2 separate bodies/.test(r0.reason || '') && !r0.poolIds.length, `the old drawing (one outline): the line is held for the body reason (${r0.state}: ${r0.reason})`);
    check(r0.designHold && r0.designHold.charmHash === OLD_H && (r0.designHold.skus || []).includes(DESIGN), `the hold carries the entry version it was made against (${JSON.stringify(r0.designHold)})`);
    check(p0.state === 'held' && !p0.poolIds.length, `the second order is held for the same reason (${p0.state})`);
    // a person puts the second order ON HOLD; the run is open at the nest step (as Paul's page)
    await page.evaluate(() => { const r = B.orders.rows.find(x => x.order.receiptId === '4204452602'); r.hold = 'held by Paul'; B.run = { runId: 'run-real', day: new Date().toISOString().slice(0, 10), setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] }; Orders.render(); });

    /* 2 · Paul's corrected master: the same file path written over with the Left + Right pair, a new charm hash, master and index time (another computer's index run: a cloud read) */
    const w1 = st.calls.length;
    st.blobs.set(AI_PATH, { buf: fs.readFileSync(AFTER_AI), generation: 2, meta: { contentType: 'application/illustrator', metadata: { firebaseStorageDownloadTokens: 't2' } } });
    st.put('Charm_Master_Index', DESIGN, { charmHash: NEW_H, masterHash: 'm-best-new', masterName: 'BEST FRIENDS_2505.ai', widthPt: 72, heightPt: 66, areaPt2: 3000, indexedAtMs: Date.now(), pair: { v: 1, bodies: 2, mismatched: true } });
    await page.evaluate(() => Master.load({ force: true }));
    const r1 = await until(async () => { const r = await rowOf('4204452679'); return r && r.state === 'pooled' && r.poolIds.length === 2 ? r : null; }, 60000, 'the held line pooled after the library change');
    console.log(JSON.stringify(r1));
    if (r1) {
      check(JSON.stringify(r1.bodies) === '[0,1]' && r1.widths.every(x => x < 50), `without a person: 2 halves pooled, the left body and the right body (bodies ${JSON.stringify(r1.bodies)}, widths ${r1.widths})`);
      const jobs = await until(() => page.evaluate(k => { const r = B.orders.byKey.get(k), js = Engrave.jobsOf(r); return js.length === 2 ? js.map(j => [j.slot || null, j.face === 'front' ? 'front' : 'back', (j.copies || []).length]).sort() : null; }, r1.key), 60000, 'the engraving jobs of the two halves');
      check(jobs && JSON.stringify(jobs.map(j => [j[0], j[1]])) === '[["D1","front"],["D2","front"]]' && jobs.every(j => j[2] === 1), `the engraving is asked per half: one FRONT job for each (${JSON.stringify(jobs)})`);
      check(!r1.hold && !r1.designHold && !r1.problems.length && !r1.reason, `the hold is cleared (hold ${r1.hold}, stamp ${JSON.stringify(r1.designHold)}, problems ${r1.problems}, reason ${r1.reason})`);
    }
    await until(async () => { await flushTl(); return tl('4204452679').some(t => /re-checked against the new drawing/i.test(t)); }, 20000, 'the timeline line of the re-checked order');
    check(tl('4204452679').filter(t => /re-checked against the new drawing/i.test(t)).length === 1, `the order's timeline says it was re-checked against the new drawing (${JSON.stringify(tl('4204452679').filter(t => /re-checked/i.test(t)))})`);
    const p1 = await rowOf('4204452602');
    check(p1.state === 'held' && p1.hold === 'held by Paul' && !p1.poolIds.length, `the person-held line stays held (${p1.state}, ${p1.hold})`);
    check(p1.redrawNote && p1.redrawNote.how === 'held', `it is told (row.redrawNote ${JSON.stringify(p1.redrawNote)})`);
    await until(async () => { await flushTl(); return tl('4204452602').some(t => /new drawing/i.test(t) && /on hold/i.test(t)); }, 20000, 'the timeline note of the person-held order');
    check(!poolWrites(w1, ['4204452602']).length, 'nothing is written for the person-held line');

    /* 3 · a second identical read: nothing */
    await sleep(800);
    const w2 = st.calls.length, t2 = tl('4204452679').length + tl('4204452602').length;
    st.put('Charm_Master_Index', DESIGN, { indexedAtMs: Date.now() + 1 });
    await page.evaluate(() => Master.load({ force: true }));
    await page.evaluate(() => Review.heldRecheckNow && Review.heldRecheckNow());
    await sleep(2000); await flushTl();
    const r2 = await rowOf('4204452679');
    check(r2.state === 'pooled' && JSON.stringify(r2.poolIds) === JSON.stringify(r1 && r1.poolIds), `a second identical read leaves the line as it is (${r2.state}, ${r2.poolIds})`);
    check(!poolWrites(w2, ['4204452679', '4204452602']).length && tl('4204452679').length + tl('4204452602').length === t2, `nothing written, nothing told again (${poolWrites(w2, ['4204452679', '4204452602']).map(c => c.op)})`);

    /* 4 · a page RELOAD: a line held against the OLD drawing (its stamp older than the entry) and one held before the stamp existed come back from the checkpoint and are checked again once */
    await page.evaluate(async ({ fixtures, orderSrc, OLD_H }) => {
      const orderOf = eval('(' + orderSrc + ')'), now = Date.now();
      const why = "its design BEST_4003 does not give the 2 separate bodies this listing's profile cuts (the left half and the right half, each its own closed outline): a person checks the design";
      for (const [fx, stamp] of fixtures) {
        const order = orderOf(fx, now);
        for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: now, spec: null, problems: [], state: 'held', reason: why, claimedBy: null, poolIds: [], engrave: null, material: 'silver' }; if (stamp) row.designHold = { skus: ['BEST_4003'], charmHash: OLD_H, indexedAt: now - 86400000, at: now - 3600000 }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      }
      Orders.interpretAll(); await Session.flushNow();
    }, { fixtures: [[R.ORDERS.stale, true], [R.ORDERS.legacy, false]], orderSrc: Base.orderOf.toString(), OLD_H });
    const before = [await rowOf('4204452603'), await rowOf('4204452604')];
    check(before.every(r => r && r.state === 'held'), `before the reload both lines are held (${before.map(r => r && r.state)})`);
    await page.reload({ waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.Session && Session.ready() && B.master.loadedAt > 0 && B.orders.rows.length >= 4, null, { timeout: 60000 });
    const back = await until(async () => { const a = await rowOf('4204452603'), b = await rowOf('4204452604'); return a && b && a.poolIds.length === 2 && b.poolIds.length === 2 ? [a, b] : null; }, 60000, 'the stale held lines pooled after the reload');
    console.log(JSON.stringify(back));
    if (back) for (const [r, what] of [[back[0], 'held against the old drawing'], [back[1], 'held before the stamp existed']]) check(JSON.stringify(r.bodies) === '[0,1]' && !r.designHold && !r.hold, `after the reload the line ${what} is 2 halves (bodies ${JSON.stringify(r.bodies)}, state ${r.state})`);
    await until(async () => { await flushTl(); return tl('4204452603').some(t => /Re-checked against the new drawing of BEST_4003/.test(t)) && tl('4204452604').some(t => /Re-checked against the library's drawing of BEST_4003/.test(t)); }, 20000, 'the timeline lines of the lines re-checked after the reload');
    check(tl('4204452603').some(t => /Re-checked against the new drawing/.test(t)) && tl('4204452604').some(t => /Re-checked against the library's drawing/.test(t)), 'their timelines say they were re-checked at the reload');
    const p3 = await rowOf('4204452602');
    check(p3 && p3.hold === 'held by Paul' && !p3.poolIds.length, `the person-held line is still held after the reload (${p3 && p3.state}, ${p3 && p3.hold})`);
    check(!w.errors.length, 'no page error: ' + w.errors.slice(0, 3).join(' | '));
  } finally { await w.close(); }
  console.log(`redraw-held-recheck: ${nCheck - fails.length} of ${nCheck} checks passed`);
  if (fails.length) { console.log('FAILED:\n  ' + fails.join('\n  ')); process.exitCode = 1; }
})().catch(e => { console.error(e); process.exit(1); });
