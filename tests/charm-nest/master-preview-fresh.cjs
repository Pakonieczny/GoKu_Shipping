// A re-index rewrites a design; the Master tab must draw it again (MASTER7, Paul 15 Oct 2026). The 26 HEARTKEY keys were rewritten with their letters, and the tiles that had been drawn
// before the re-index kept their old, letterless pictures until the page was reloaded (A–Q), while the ones never drawn (R–Z) showed the new files: Pool.masterPreview's memory cache knew the
// design file's PATH only. It now also knows the stamp the library gave the design when it was written (indexedAt).
//   · the same design, unchanged, is drawn once (the cache still does its job);
//   · the design rewritten (a new file under the same path, a new indexedAt) is drawn again, at once, with no reload, in Pool.masterPreview and in the tile on the Master tab;
//   · a file that changed under an UNCHANGED stamp is not read again (that is what the stamp is for).
//   PW_DIR=/opt/node-tools/node_modules node tests/charm-nest/master-preview-fresh.cjs
'use strict';
const assert = require('assert');
const { scene } = require('./_masterScene.cjs'), R = require('./_masterRoutes.cjs'), { Timestamp } = require('./bridge-server.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };

(async () => {
  const sheet = await scene({ page: [300, 220], bodies: [{ shape: 'house', x: 40, y: 100, w: 50, h: 50, ink: '0.2 0.35 0.85' }, { shape: 'blob', x: 170, y: 100, w: 50, h: 50, ink: '0.85 0.2 0.2' }], labels: [{ text: 'PREV_A', cx: 65, top: 96 }, { text: 'PREV_B', cx: 195, top: 96 }] });
  const br = await R.browser(), { page, st } = br;
  try {
    const r = await br.upload(sheet, 'PREV.ai'); ok(r.state === 'done' && br.index('PREV_A') && br.index('PREV_B'), 'two designs are in the library');
    const draw = sku => page.evaluate(s => Pool.masterPreview(B.master.entries.get(s), null, false), sku);
    const a1 = await draw('PREV_A'), b1 = await draw('PREV_B');
    ok(/^data:image\//.test(a1) && /^data:image\//.test(b1) && a1 !== b1, 'each design draws its own picture');
    ok((await draw('PREV_A')) === a1, 'the same design again: the same picture (drawn once)');

    // the file changes under the SAME stamp: nothing is read again
    const A = br.index('PREV_A'), B = br.index('PREV_B');
    br.blob(A.aiPath).buf = br.blob(B.aiPath).buf;
    ok((await draw('PREV_A')) === a1, 'a file that changed under an unchanged stamp is not read again');

    // a re-index: the file is rewritten and the library stamps the design anew; the page loads the library again, as it does when a run is done
    st.put('Charm_Master_Index', 'PREV_A', { indexedAt: Timestamp.fromMillis(Date.now() + 5000) });
    await page.evaluate(() => Master.load(true));
    const a2 = await draw('PREV_A');
    ok(a2 !== a1 && a2 === b1, 'rewritten and re-stamped: drawn again, with no reload, as the new file (' + (a2 === b1 ? 'the same picture as the design whose file it now is' : 'a different picture') + ')');
    ok((await draw('PREV_B')) === b1, 'the design that did not change keeps its picture');

    // the tile on the Master tab follows
    await page.fill('#mSearch', 'PREV_A'); await page.dispatchEvent('#mSearch', 'input');
    await page.waitForFunction(({ want }) => { const i = document.querySelector('#mGrid [data-preview-sku="PREV_A"] img'); return !!i && i.src === want; }, { want: a2 }, { timeout: 20000 });
    ok(true, 'and so does the tile of the Master tab');
    ok(br.errors.length === 0, 'no page error: ' + br.errors.slice(0, 2).join(' | '));
  } finally { await br.close(); }
  console.log('master-preview-fresh OK ·', n, 'checks');
})().catch(e => { console.error(e); process.exit(1); });
