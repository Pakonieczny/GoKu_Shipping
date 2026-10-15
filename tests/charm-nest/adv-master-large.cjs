// ADV-MASTER (Paul, 15 Oct 2026): a production-sized master through the Master tab, in the browser route (the default: "masterServerAbove" is 0). 420 charms, 414 of them labelled and 6 not.
// Headless Chromium over the in-memory stand-in of the site; nothing leaves this machine. Asks of a big file: every label's SKU lands once with a design file of its own, the six label-less charms are named (not
// dropped, not guessed), the upload-history record counts the whole file, and the same file dropped again builds and traces nothing.
//   PW_DIR=/opt/node-tools/node_modules node tests/charm-nest/adv-master-large.cjs
'use strict';
const assert = require('assert');
const { scene } = require('./_masterScene.cjs'), R = require('./_masterRoutes.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; console.log('  ok  ' + m); };
const COLS = 21, ROWS = 20, N = COLS * ROWS, NO_LABEL = i => i % 70 === 69;
const ink = ['0.2 0.35 0.85', '0.85 0.2 0.2', '0.2 0.6 0.3'];
const name = i => 'BULK_' + String(i + 1).padStart(3, '0');
const sheet = () => scene({ page: [1000, 1000],
  bodies: Array.from({ length: N }, (_, i) => ({ shape: ['house', 'notch', 'blob'][i % 3], x: 20 + (i % COLS) * 46, y: 940 - Math.floor(i / COLS) * 46, w: 32, h: 30, ink: ink[i % 3] })),
  labels: Array.from({ length: N }, (_, i) => i).filter(i => !NO_LABEL(i)).map(i => ({ text: name(i), cx: 20 + (i % COLS) * 46 + 16, top: 939 - Math.floor(i / COLS) * 46 })) });

(async () => {
  const bytes = await sheet(), br = await R.browser(), st = br.st, page = br.page;
  try {
    const t0 = Date.now();
    const r = await br.upload(bytes, 'BULK.ai', { timeout: 210000 });
    const secs = Math.round((Date.now() - t0) / 1000), want = Array.from({ length: N }, (_, i) => i).filter(i => !NO_LABEL(i)).map(name).sort();
    console.log('  (420 charms read, traced and written in ' + secs + ' s)');
    ok(r.state === 'done' && !r.error, 'the run is done: ' + (r.error || r.state));
    const have = st.list('Charm_Master_Index').map(e => e._id).sort();
    ok(have.length === want.length && have.join() === want.join(), 'all 414 labelled SKUs are in the library, each once: ' + have.length);
    const paths = st.list('Charm_Master_Index').map(e => e.aiPath);
    ok(new Set(paths).size === 414 && paths.every(p => st.blobs.has(p)) && st.list('Charm_Master_Index').every(e => !e.blocked && e.thumbPath && st.blobs.has(e.thumbPath)), 'each has a design file and a picture of its own, none blocked');
    ok(r.unlabelled.length === 6 && r.orphans.length === 0, 'the 6 charms with no label are named, not dropped and not guessed: ' + r.unlabelled.join());
    const rec = st.list('Charm_Master_Files');
    ok(rec.length === 1 && rec[0].charms === 420 && rec[0].labelled === 414 && rec[0].skus.length === 414 && rec[0].unlabelledCount === 6, 'the upload-history record counts the whole file: ' + JSON.stringify(rec[0] && { c: rec[0].charms, l: rec[0].labelled, s: rec[0].skus.length, u: rec[0].unlabelledCount }));
    ok(await page.evaluate(() => document.querySelectorAll('#mGrid .skuTile').length) > 0, 'the grid shows them');
    // the same file again: held, traced nothing, written nothing
    await page.evaluate(() => { window.__traced = 0; const o = CharmNestPDF.buildSilhouettes; CharmNestPDF.buildSilhouettes = function (p, cs, ...rest) { window.__traced += cs.length; return o.call(this, p, cs, ...rest); }; });
    const puts = st.calls.filter(c => c.body && c.body.op === 'masterPutIndex').length;
    const r2 = await br.upload(bytes, 'BULK.ai', { timeout: 210000 });
    ok(r2.state === 'done' && r2.written === 0 && r2.held === 414 && (await page.evaluate(() => window.__traced)) === 0 && st.calls.filter(c => c.body && c.body.op === 'masterPutIndex').length === puts, 'the same file again: 414 held, 0 written, 0 traced, no index write');
    const bad = br.errors.filter(e => !/status of 500/.test(e));
    ok(bad.length === 0, 'no page error: ' + bad.join(' | '));
  } finally { await br.close(); }
  console.log(`adv-master-large: ${n} checks passed`);
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
