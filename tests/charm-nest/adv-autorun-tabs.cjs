// A second Charm Sorter tab opened on the same computer while an Auto run is open (Paul, 15 Oct 2026: "unexpected user behaviour").
// A tab brought back from the saved workspace is a copy of the run as it was a few seconds ago; if it carries on with the run, two tabs
// nest the same sheets and write the same records, and the last write wins: the labels of one tab's layout end up on the other's files.
// The real sorter and the framed Design Station in Chromium against the repo's fake site (day-sim/harness.cjs):
//  1 · the first tab's run is at rest; a second tab is opened. It reads, and writes nothing: no sheet, run, set or piece record, for as
//      long as the first tab is open, and its run does not go on.
//  2 · new orders arrive while both tabs are open; the first tab takes them as ever, and each of the orders ends on one sheet.
//  3 · the first tab is closed: the second one, which still has the run, takes it over by itself.
//   node tests/charm-nest/adv-autorun-tabs.cjs [playwright-core dir]
const assert = require('assert');
if (process.argv[2]) process.env.PW_DIR = process.argv[2];
const { open } = require('./day-sim/harness.cjs');
const W = require('./_autorunWorld.cjs');

const WRITES = /^(putSheet|runPut|runArchive|setUpdate|setAllocate|poolPut|poolUpdate|backPut|backInvalidate|archiveEmptySheet|deleteSheet|putCharms|arrivalRecord|laserDone)$/;
(async () => {
  const receipts = W.wave(3920000001, 4);
  const H = await open({ receipts, masters: W.MASTERS, settings: { budgetS: +process.env.BUDGET || 2, runMode: 'manual', pullMode: 'all', pollOrders: 'on', pollMinutes: 10, heartbeatS: 5, heartbeatMiss: 3 } });
  const { page, ctx, st } = H;
  const t0 = Date.now(), at = () => Math.round((Date.now() - t0) / 1000) + 's', log = (...a) => console.log(at(), ...a);
  const ahead = () => H.frame().evaluate(() => { window.__realNow = window.__realNow || Date.now.bind(Date); window.__off = (window.__off || 0) + 11 * 60000; Date.now = () => window.__realNow() + window.__off; });
  try {
    await page.evaluate(() => { CN.setMode('nest'); RunCtl.setMode('auto'); });
    let v = await W.rest(page, { log, timeoutMs: 420000 }); assert(!v.timeout, 'the first batch rests: ' + JSON.stringify(v));
    const runId = v.run.id; log('first tab at rest:', v.sheets.join(' '), v.sets.join(' '));
    await page.evaluate(() => Session.flush());

    /* 1 · a second tab */
    const n0 = st.calls.length;
    const tab2 = await ctx.newPage(); const errors2 = [];
    tab2.on('pageerror', e => errors2.push(e.message));
    await tab2.goto(`${H.sorterOrigin}/charm-nest-1.html`);
    await tab2.waitForFunction(() => window.CN && window.CN.S && window.RunCtl && window.DesignLink && CN.S.cloud.ok !== null, null, { timeout: 90000 });
    const seen = [];
    for (let i = 0; i < 20; i++) { await new Promise(r => setTimeout(r, 1500)); seen.push(await tab2.evaluate(() => B.run ? `${B.run.status}/${B.run.step}${B.run.stoppedBy ? ':' + B.run.stoppedBy : ''}` : 'no run').catch(() => '?')); }
    const states = [...new Set(seen)]; log('second tab says:', states.join(' → '));
    const wrote = st.calls.slice(n0).filter(c => WRITES.test(String(c.op))).map(c => c.op);
    log('writes while the first tab was at rest and the second was open:', JSON.stringify(wrote.reduce((m, o) => (m[o] = (m[o] || 0) + 1, m), {})));
    assert(!seen.some(s => /^running/.test(s)), '1 · the second tab does not carry the run on: ' + states.join(' → '));
    assert.deepStrictEqual(wrote, [], '1 · the second tab writes no record while the first one has the run');

    /* 2 · new orders while both are open */
    receipts.push(...W.wave(3920000101, 2));
    await ahead(); await W.due(page);
    v = await W.rest(page, { log, timeoutMs: 300000, also: x => x.rowCount >= receipts.length });
    assert(!v.timeout && v.rowCount === receipts.length, '2 · the first tab takes the new orders: ' + JSON.stringify(v));
    const all = receipts.map(r => String(r.receipt_id));
    W.check(await W.outcome(H, all), all);
    const mine = await page.evaluate(() => allSheets().filter(p => p.sheetId).map(p => [p.sheetId, p.placements.length]));
    for (const [id, n] of mine) { const doc = st.doc('Charm_Nest_Sheets', id); if (doc && !doc.archived) assert.equal((doc.placements || []).length, n, `2 · sheet ${id} on the server is the layout the first tab has (${(doc.placements || []).length} vs ${n})`); }
    log(`both tabs open, ${receipts.length} orders in, every record agrees with the first tab`);

    /* 3 · the first tab is closed */
    await page.close({ runBeforeUnload: false });
    const took = await tab2.waitForFunction(() => B.run && ['running', 'processed', 'complete'].includes(B.run.status) && !/restored/.test(B.run.stoppedBy || ''), null, { timeout: +process.env.TAKEOVER_S * 1000 || 60000, polling: 500 }).then(() => true, () => false);
    log('second tab after the first closed:', await tab2.evaluate(() => B.run ? `${B.run.status}/${B.run.step}${B.run.stoppedBy ? ':' + B.run.stoppedBy : ''}` : 'no run'));
    if (process.env.STRICT3) assert(took, '3 · the second tab takes the run over when the first tab is gone');
    console.log(at(), `OK · run ${runId}`);
  } finally { await H.close(); }
})().catch(e => { console.error('FAIL', e && e.stack || e); process.exit(1); });
