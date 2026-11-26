// QUIETLIVE (Paul, 19 Nov 2026: "whenever I refresh someone's computer after a while, it goes through this strange animation. It's very disturbing and it doesn't allow the user to use the system for a long time"; the video:
// "Rebuilding GF_Nov.12.26_Set-2_Sheet..." 8, 11 seconds, then the next sheet and "+1", the page frozen and the Library redrawn between them).
// After every reload a computer that follows the run builds each of the run's sheets from the master files (Recall.follow → makeLive → CharmNestCloudSheets.reload → Recall.rebuild). The page's own build of the followed sheets is now quiet:
//   1 reload(page, why, { background: true }) asks Recall.rebuild for a background build and redraws no thumbnails per sheet; a person's own reload (no option) is as before: a normal rebuild, thumbnails redrawn
//   2 Recall.rebuild(page, { background: true }) starts its progress task quiet (never drawn: no "Rebuilding ..." bar); a person's own rebuild shows the bar as before
// Real sorter in Chromium against the repo's fake site; nothing live.   PW_DIR=... CHROMIUM=... node tests/charm-nest/quiet-live-build.cjs
if (process.argv[2]) process.env.PW_DIR = process.argv[2];
const { open } = require('./day-sim/harness.cjs');
let fails = 0;
const say = (name, ok, seen) => { if (!ok) fails++; console.log(`${ok ? 'OK  ' : 'FAIL'} ${name}${seen === undefined ? '' : ' :: ' + (typeof seen === 'string' ? seen : JSON.stringify(seen))}`); };

(async () => {
  const H = await open({ masters: [], receipts: [] });
  const page = H.page;
  await page.evaluate(() => { CN.setMode('nest'); });
  await page.waitForFunction(() => window.Recall && window.CharmNestCloudSheets && window.CNProgress, null, { timeout: 30000 });
  await page.evaluate(() => { const origApi = window.api; window.__origApi = origApi; CN.api.__orig = true; window.api = (fn, body, opts) => (CN.api && CN.api.__orig ? origApi(fn, body, opts) : CN.api(fn, body, opts)); });

  // 1 ── reload
  const r1 = await page.evaluate(async () => {
    const realApi = CN.api, realRebuild = Recall.rebuild, MT = window.CharmNestMadeThumb || (window.CharmNestMadeThumb = {}), realRefresh = MT.refreshSoon, out = { opts: [], refresh: 0 };
    const rec = { id: 'qsheet1', fileBase: 'QS_1', layoutRev: 3, placements: [], charms: [], metal: 'gold14k', preview: '', outputs: {} };
    CN.api = async (fn, body) => (body.op === 'getSheet' || body.op === 'getMany' || body.op === 'listSheets' ? { sheet: rec, sheets: [rec], recs: { ['s:qsheet1']: rec } } : {});
    Recall.rebuild = async (pg, o) => { out.opts.push(o === undefined ? 'none' : JSON.stringify(o)); };
    MT.refreshSoon = () => { out.refresh++; };
    const pg = () => ({ sheetId: 'qsheet1', fileBase: 'QS_1', charms: [], placements: [], metal: 'gold14k' });
    try {
      const a = pg(); await CharmNestCloudSheets.reload(a, 'test background', { background: true }); out.afterBg = out.refresh;
      const b = pg(); await CharmNestCloudSheets.reload(b, 'test person'); out.afterPerson = out.refresh;
    } finally { CN.api = realApi; Recall.rebuild = realRebuild; MT.refreshSoon = realRefresh; }
    return out;
  });
  // (NOSTORE, Paul 23 Nov 2026, charm-nest-cloud-sheets.js reload / Recall.rebuild: the sheet is drawn from the cloud's answer it read and nothing else, so every reload hands Recall.rebuild { answer: rec } beside its own options)
  const o1 = r1.opts.map(x => { try { return x === 'none' ? null : JSON.parse(x); } catch (_) { return null; } }), only = (o, keys) => !!o && Object.keys(o).sort().join() === keys.slice().sort().join() && !!o.answer && o.answer.id === 'qsheet1';
  say('1a a background reload asks for a background rebuild (drawn from the record it read) and redraws no thumbnails', only(o1[0], ['answer', 'background']) && o1[0].background === true && r1.afterBg === 0, r1);
  say('1b a person\'s own reload is a normal rebuild (no background, drawn from the record it read) and redraws the thumbnails', only(o1[1], ['answer']) && r1.afterPerson === 1, r1);

  // 2 ── the bar
  const r2 = await page.evaluate(async () => {
    const realApi = CN.api, realStart = CNProgress.start, out = { starts: [] };
    CN.api = async (fn, body) => (body.op === 'getSheet' ? { sheet: { id: 'qsheet2', placements: [], charms: [], poolIds: [] } } : {});
    CNProgress.start = (label, o) => { out.starts.push([String(label), o ? JSON.stringify(o) : '']); return { end() {}, set() {}, note() {} }; };
    const pg = () => ({ sheetId: 'qsheet2', recalled: { id: 'qsheet2', fileBase: 'QS_2' }, charms: [], placements: [], metal: 'gold14k', status: 'complete' });
    try {
      try { await Recall.rebuild(pg(), { background: true }); } catch (e) { out.errBg = String(e && e.message || e).slice(0, 80); }
      try { await Recall.rebuild(pg()); } catch (e) { out.errFg = String(e && e.message || e).slice(0, 80); }
    } finally { CN.api = realApi; CNProgress.start = realStart; }
    return out;
  });
  const rb = r2.starts.filter(s => /^Rebuilding QS_2/.test(s[0]));
  say('2 a background rebuild starts its progress task quiet (never drawn); a person\'s own rebuild is drawn as before', rb.length === 2 && rb[0][1] === '{"quiet":true}' && rb[1][1] === '', r2);

  const errs = (H.errors || []).filter(e => !/firebase stub|favicon|HTTP 400|status of 400/.test(e));
  say('no page error', errs.length === 0, errs.slice(0, 3));
  await H.close?.();
  console.log(fails ? `FAIL quiet-live-build · ${fails} check(s)` : 'PASS quiet-live-build');
  process.exit(fails ? 1 : 0);
})().catch(e => { console.error('ERROR', e && e.stack || e); process.exit(2); });
