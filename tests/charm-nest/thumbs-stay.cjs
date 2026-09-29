// A decided engraving row opens and closes without its two thumbnails loading again (Paul, 29 Sep: "the thumbnails
// annoyingly keep on resuming every single time the card expands"): the Vector design and Etsy listing boxes, and the
// pictures in them, are the same nodes after each open, close and a trip to Placements and back, with no spinner
// shown and no picture fetched again. Their ↺ still resets the zoom.
//   node tests/charm-nest/thumbs-stay.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser check was not run'); return; }
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    // the two listing photos are already known (the local index), so no lookup is made; the bytes come from the fake proxy
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator');
      localStorage.setItem('cn.listingPhotos.v1', JSON.stringify([['7001', '/.netlify/functions/imageProxy?url=a'], ['7002', '/.netlify/functions/imageProxy?url=b']])); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [], pics = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    page.on('request', r => { if (/imageProxy/.test(r.url())) pics.push(r.url()); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Engrave && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(() => {
      // two decided engravings, each with a drawn front (a square charm) and a listing photo
      const sq = { kind: 'path', subpaths: [[['m', [0, 0]], ['l', [34, 0]], ['l', [34, 34]], ['l', [0, 34]], ['h']]], stroke: true, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [0, 0, 34, 34] };
      const charm = { id: 'c', name: 'TAG', metal: 'gold', centerPt: [17, 17], widthPt: 34, heightPt: 34, areaPt2: 34 * 34, outline: sq, members: [sq], bbox: [0, 0, 34, 34], thumb: '' };
      const was = Pool.charmOf; Pool.charmOf = id => /^tt-/.test(id) ? Object.assign({ poolId: id }, charm) : was(id);
      for (const [n, lid] of [[1, '7001'], [2, '7002']]) {
        const row = { key: 'tt' + n, order: { receiptId: '417300000' + n }, line: { listingId: lid, sku: 'TAG_' + n, title: 'Tag', variations: [] }, spec: { designSku: 'TAG_' + n }, state: 'pooled', poolIds: ['tt-' + n], problems: [], arrivedAt: Date.now() - n };
        Engrave.items().set(row.key, { key: row.key, row, copies: row.poolIds, state: 'written', lines: ['S' + n], backs: [], approvedBy: 'Test Operator', approvedAt: Date.now() - 60000 });
      }
      CN.setMode('engrave'); Engrave.restoreView({ tab: 'done', chosen: true }); Engrave.render();
    });
    const settled = () => page.waitForFunction(() => { const rs = [...document.querySelectorAll('#egDone .decidedRow')]; return rs.length === 2 && rs.every(r => r.querySelector('[data-vector] canvas, [data-vector] img') && r.querySelector('[data-listing] img')?.complete && !r.querySelector('.comparePair .thumbLoading')); }, null, { timeout: 30000 });
    await settled();
    const picsAtStart = pics.length;
    // everything that is put into a thumbnail box from here on is recorded: a spinner, a new picture
    await page.evaluate(() => {
      window.__thumb = { added: [], anims: 0 };
      new MutationObserver(rs => { for (const r of rs) if (r.target.closest?.('.comparePair')) for (const n of r.addedNodes) window.__thumb.added.push(n.nodeName + '.' + (n.className || '')); })
        .observe(document.getElementById('engraveView'), { subtree: true, childList: true });
      // stamp the pictures that are there now
      document.querySelectorAll('#egDone .comparePair').forEach((p, i) => { p.__id = 'pair' + i; p.querySelectorAll('[data-vector],[data-listing],img,canvas').forEach(n => { n.__id = 'n' + i; }); });
    });
    const snapshot = () => page.evaluate(() => [...document.querySelectorAll('#egDone .decidedRow')].map(r => ({
      key: r.dataset.key, open: r.classList.contains('open'), pair: r.querySelector('.comparePair').__id || null,
      nodes: [...r.querySelectorAll('.comparePair [data-vector], .comparePair [data-listing], .comparePair img, .comparePair canvas')].map(n => n.__id || 'NEW'),
      anims: r.querySelector('.comparePair').getAnimations({ subtree: true }).map(a => a.animationName || a.id || 'anim'),
    })));
    const before = await snapshot();
    assert(before.every(r => r.pair && r.nodes.length >= 4 && r.nodes.every(n => n !== 'NEW')), 'two drawn thumbnails in each row: ' + JSON.stringify(before));
    const check = async (label, openKey) => {
      await page.evaluate(() => new Promise(r => setTimeout(r, 400)));
      const now = await snapshot();
      for (const r of now) {
        const b = before.find(x => x.key === r.key);
        assert.equal(r.pair, b.pair, `${label}: row ${r.key} keeps its thumbnail pair`);
        assert.deepEqual(r.nodes, b.nodes, `${label}: row ${r.key} keeps the same thumbnail boxes and pictures`);
        assert.deepEqual(r.anims, [], `${label}: no animation plays on row ${r.key}'s thumbnails`);
        assert.equal(r.open, r.key === openKey, `${label}: only ${openKey || 'no row'} is open`);
      }
      const t = await page.evaluate(() => window.__thumb);
      assert.deepEqual(t.added, [], `${label}: nothing new put into a thumbnail box`);
      assert.equal(pics.length, picsAtStart, `${label}: no listing photo fetched again`);
    };
    const rowSel = k => `#egDone .decidedRow[data-key="${k}"] .engravingIdentity`;
    for (const pass of [1, 2]) {
      await page.click(rowSel('tt2'));
      await page.waitForSelector('#egDone .decidedRow[data-key="tt2"] .doneDetail');
      await check(`open ${pass}`, 'tt2');
      await page.click(rowSel('tt2'));
      await page.waitForFunction(() => !document.querySelector('#egDone .doneDetail'));
      await check(`close ${pass}`, null);
    }
    // another row open, then Placements and back to Decided
    await page.click(rowSel('tt1'));
    await page.waitForSelector('#egDone .decidedRow[data-key="tt1"] .doneDetail');
    await check('open the other row', 'tt1');
    await page.click('.egTab[data-tab="place"]');
    await page.click('.egTab[data-tab="done"]');
    await page.waitForSelector('#egDone .decidedRow');
    await check('Placements and back', 'tt1');
    // the ↺ still resets the Etsy photo's zoom on purpose
    const reset = await page.evaluate(() => { const f = document.querySelector('#egDone .decidedRow [data-listing]').closest('figure'), im = f.querySelector('img'), b = f.querySelector('.thumbReset');
      return { hidden: b.hidden, before: +im.dataset.scale, after: (b.click(), +im.dataset.scale) }; });
    assert.equal(reset.hidden, false, 'the ↺ is shown once the photo is in');
    assert.equal(reset.after, 1, `the ↺ resets the zoom (${reset.before} → ${reset.after})`);
    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ a decided row opens and closes twice, another opens, Placements and back: the same thumbnails, nothing reloaded, no animation');
  } finally { await browser.close(); srv.close(); }
})().then(() => console.log('Thumbnails stay OK')).catch(e => { console.error(e); process.exit(1); });
