// A sheet's search or check that cannot start because the network is away must not wedge the Auto run (Paul, 15 Oct 2026:
// "resilient against lost internet connections"). A sheet's nesting and its check each run on a worker made from a file the page
// fetches when it needs it; with the network away the fetch fails, and
//   - the error carried no words, so the page took it for no error at all and the sheet stayed "nesting" for good;
//   - the sheet's check library (pdf.js), asked for once and failed, kept the same failure for the rest of the session;
//   - a sheet that failed this way was held for a person while the run went on around it.
// Now each says what happened, the library is asked for again, and the run stops as a passing stop that Auto lifts.
// The real sorter in headless Chromium against the repo's fake site (day-sim/harness.cjs); nothing live.
//   node tests/charm-nest/adv-autorun-worker.cjs [playwright-core dir]
const assert = require('assert');
if (process.argv[2]) process.env.PW_DIR = process.argv[2];
const { open } = require('./day-sim/harness.cjs');

(async () => {
  const H = await open({ receipts: [], masters: [], settings: { pollOrders: 'off', runMode: 'manual' } });
  const { page, ctx } = H; let n = 0;
  try {
    // 1 · a search worker whose file cannot be fetched reports an error that says so
    const worker = /charm-nest-worker\.js/;
    await ctx.route(worker, r => r.abort('internetdisconnected'));
    const m = await page.evaluate(() => new Promise(res => { const real = window.onWorkerMessage; window.onWorkerMessage = (sh, msg) => { window.onWorkerMessage = real; res(msg); }; ensureWorkers({ jobId: 'jx', workers: [] }, 1); }));
    assert.equal(m.type, 'error'); assert.match(m.message, /is the network down/, '1 · the error has words: ' + JSON.stringify(m)); n++;
    await ctx.unroute(worker);
    // 3 · the check library is asked for again after a failure
    const pdfjs = /vendor\/pdfjs-4\.10\.38\/pdf\.min\.mjs/;
    await ctx.route(pdfjs, r => r.abort('internetdisconnected'));
    assert.equal(await page.evaluate(() => ensurePdfJs().then(() => 'loaded', () => 'failed')), 'failed', '3 · offline, the library does not load');
    await ctx.unroute(pdfjs);
    assert.equal(await page.evaluate(() => ensurePdfJs().then(() => 'loaded', e => 'failed: ' + e.message)), 'loaded', '3 · and loads once the network is back'); n++;
    // 4 · a sheet that failed for the network stops the run as a passing stop; any other failure still holds the sheet
    const stops = await page.evaluate(async () => {
      const out = {};
      for (const [name, msg] of [['network', 'the sheet search could not start: its file could not be loaded (is the network down?)'], ['other', 'Cannot read properties of undefined (reading "x")']]) {
        B.run = { runId: 'run-2026-10-15-' + name, day: '2026-10-15', status: 'running', step: 'nest', mode: 'auto', errors: [], lines: {}, sheets: {}, holds: {}, orders: [], startedAt: Date.now(), updatedAt: Date.now() };
        const sh = S.sheets.gold.pages[0]; sh.runId = B.run.runId; delete sh.runHold; sh.problem = null;
        RunCtl.onSheetDone(sh, new Error(msg));
        out[name] = { status: B.run.status, kind: RunCtl.stopKind(B.run), held: !!sh.runHold, stoppedBy: B.run.stoppedBy };
      }
      return out;
    });
    assert.equal(stops.network.status, 'stopped'); assert.equal(stops.network.kind, 'transient', '4 · Auto lifts it when the network is back'); assert.equal(stops.network.held, false, '4 · the sheet is not held for a person');
    assert.equal(stops.other.status, 'running', '4 · another failure does not stop the run'); assert.equal(stops.other.held, true, '4 · and still holds its sheet'); n++;
    // 5 · a set that cannot be numbered because the cloud had not answered is a passing stop too: Auto lifts it once the cloud is back
    const kinds = await page.evaluate(() => {
      const out = [];
      for (const why of ['Reconnect to the cloud before numbering a new set; the layout is kept locally', 'The protected Rose Gold contour belongs to an existing set. Finish that set before regrouping its sheets.']) {
        B.run = { runId: 'run-2026-10-15-k' + out.length, day: '2026-10-15', status: 'running', step: 'checkpoint', mode: 'auto', errors: [], lines: {}, sheets: {}, holds: {}, orders: [], startedAt: Date.now(), updatedAt: Date.now() };
        RunCtl.stop(why, 'x'); out.push(RunCtl.stopKind(B.run));
      }
      return out;
    });
    assert.deepEqual(kinds, ['transient', 'error'], '5 · "Reconnect to the cloud" is a passing stop; a stop that needs a person still is not: ' + JSON.stringify(kinds)); n++;
    assert.deepEqual(H.errors.filter(e => !/net::ERR_INTERNET_DISCONNECTED|Failed to load resource|firebase stub|HTTP 5|Failed to fetch/.test(e)), [], 'no other page errors');
    console.log(`Worker and check files OK: ${n} checks`);
  } finally { await H.close(); }
})().catch(e => { console.error('FAIL', e && e.stack || e); process.exit(1); });
