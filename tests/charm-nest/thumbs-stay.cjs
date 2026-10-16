// An approved engraving's row opens its card and comes back without its two thumbnails loading again (Paul, 29 Sep: "the thumbnails
// annoyingly keep on resuming every single time the card expands"; 16 Oct 2026: there is no Decided view any more, an approved piece is a row of the ONE list and opens the same card): the Vector
// design and Etsy listing boxes, and the pictures in them, are the same nodes after each open of the card and each return to the list, with no spinner shown and no picture fetched again.
// Their ↺ still resets the zoom.
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
      // two approved engravings (saved to their sheet), each with a drawn front (a square charm) and a listing photo
      const sq = { kind: 'path', subpaths: [[['m', [0, 0]], ['l', [34, 0]], ['l', [34, 34]], ['l', [0, 34]], ['h']]], stroke: true, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [0, 0, 34, 34] };
      const charm = { id: 'c', name: 'TAG', metal: 'gold', centerPt: [17, 17], widthPt: 34, heightPt: 34, areaPt2: 34 * 34, outline: sq, members: [sq], bbox: [0, 0, 34, 34], thumb: '' };
      const was = Pool.charmOf; Pool.charmOf = id => /^tt-/.test(id) ? Object.assign({ poolId: id }, charm) : was(id);
      for (const [n, lid] of [[1, '7001'], [2, '7002']]) {
        const row = { key: 'tt' + n, order: { receiptId: '417300000' + n }, line: { listingId: lid, sku: 'TAG_' + n, title: 'Tag', variations: [] }, spec: { designSku: 'TAG_' + n }, state: 'pooled', poolIds: ['tt-' + n], problems: [], arrivedAt: Date.now() - n };
        Engrave.items().set(row.key, { key: row.key, row, copies: row.poolIds, state: 'written', lines: ['S' + n], backs: [], approvedBy: 'Test Operator', approvedAt: Date.now() - 60000 });
      }
      CN.setMode('engrave'); Engrave.restoreView({ list: true, chosen: true }); Engrave.render();
    });
    const ROW = '#egQueue .egPlacementList .placementRow';
    const settled = () => page.waitForFunction(ROW => { const rs = [...document.querySelectorAll(ROW)]; return rs.length === 2 && rs.every(r => r.querySelector('[data-vector] canvas, [data-vector] img') && r.querySelector('[data-listing] img')?.complete && !r.querySelector('.comparePair .thumbLoading')); }, ROW, { timeout: 30000 });
    await settled();
    const picsAtStart = pics.length;
    // everything that is put into a thumbnail box from here on is recorded: a spinner, a new picture
    await page.evaluate(() => {
      window.__thumb = { added: [], anims: 0 };
      new MutationObserver(rs => { for (const r of rs) if (r.target.closest?.('.comparePair')) for (const n of r.addedNodes) window.__thumb.added.push(n.nodeName + '.' + (n.className || '')); })
        .observe(document.getElementById('engraveView'), { subtree: true, childList: true });
      // stamp the pictures that are there now
      document.querySelectorAll('#egQueue .egPlacementList .placementRow .comparePair').forEach((p, i) => { p.__id = 'pair' + i; p.querySelectorAll('[data-vector],[data-listing],img,canvas').forEach(n => { n.__id = 'n' + i; }); });
      document.querySelectorAll('#egQueue .egPlacementList .placementRow').forEach((r, i) => { r.__id = 'row' + i; });
    });
    const snapshot = () => page.evaluate(ROW => [...document.querySelectorAll(ROW)].map(r => ({
      key: r.dataset.open, row: r.__id || 'NEW', pair: r.querySelector('.comparePair').__id || null,
      nodes: [...r.querySelectorAll('.comparePair [data-vector], .comparePair [data-listing], .comparePair img, .comparePair canvas')].map(n => n.__id || 'NEW'),
      anims: r.querySelector('.comparePair').getAnimations({ subtree: true }).map(a => a.animationName || a.id || 'anim'),
    })), ROW);
    const before = await snapshot();
    assert.deepEqual(before.map(r => r.key).sort(), ['tt1', 'tt2'], 'the two approved pieces are rows of the ONE list: ' + JSON.stringify(before));
    assert(before.every(r => r.pair && r.nodes.length >= 4 && r.nodes.every(n => n !== 'NEW')), 'two drawn thumbnails in each row: ' + JSON.stringify(before));
    let picsAtCard = 0;   // (the card opened from a row draws its own pictures: what must not happen is the LIST fetching again when it comes back)
    const check = async label => {
      await page.evaluate(() => new Promise(r => setTimeout(r, 400)));
      const now = await snapshot();
      assert.equal(now.length, 2, `${label}: both rows are back in the list`);
      for (const r of now) {
        const b = before.find(x => x.key === r.key);
        assert.equal(r.row, b.row, `${label}: row ${r.key} is the same row`);
        assert.equal(r.pair, b.pair, `${label}: row ${r.key} keeps its thumbnail pair`);
        assert.deepEqual(r.nodes, b.nodes, `${label}: row ${r.key} keeps the same thumbnail boxes and pictures`);
        assert.deepEqual(r.anims, [], `${label}: no animation plays on row ${r.key}'s thumbnails`);
      }
      const t = await page.evaluate(() => window.__thumb);
      assert.deepEqual(t.added, [], `${label}: nothing new put into a thumbnail box`);
      assert.equal(pics.length, picsAtCard, `${label}: the list brought back fetches no listing photo again`);
    };
    // a click on an approved piece's row opens its card (the same card as any piece, the approved view: nothing is changed by looking), and the card's close brings the list back
    const openRow = async k => {
      await page.click(`${ROW}[data-open="${k}"] .engravingIdentity`);
      await page.waitForSelector(`#egQueue .rvItem[data-kind=placement][data-key="${k}"]`);
      const st = await page.evaluate(k => ({ card: document.querySelector(`#egQueue .rvItem[data-kind=placement][data-key="${k}"]`).dataset.state, job: Engrave.items().get(k).state, listGone: !document.querySelector('#egQueue .egPlacementList') }), k);
      assert.deepEqual([st.card, st.job], ['written', 'written'], `the card of ${k} is the approved view, nothing changed by opening it: ` + JSON.stringify(st));
      await page.evaluate(() => new Promise(r => setTimeout(r, 600))); picsAtCard = pics.length;
    };
    const toList = async () => { await page.click('#egQueue .rvItem [data-a=close]'); await page.waitForSelector(ROW); await settled(); };
    for (const pass of [1, 2]) {
      await openRow('tt2'); await toList();
      await check(`open and close ${pass}`);
    }
    await openRow('tt1'); await toList();
    await check('the other row');
    // a search that finds one, then clears
    await page.fill('#egQ', 'tt1_never_matches'); await page.waitForFunction(() => !document.querySelector('#egQueue .egPlacementList .placementRow')); await page.fill('#egQ', ''); await page.waitForSelector(ROW); await settled();
    await check('a search and back');
    assert.equal(await page.evaluate(() => Engrave.items().get('tt1').state + '/' + Engrave.items().get('tt2').state), 'written/written', 'looking never changed an approval');
    // the ↺ still resets the Etsy photo's zoom on purpose
    const reset = await page.evaluate(() => { const f = document.querySelector('#egQueue .egPlacementList .placementRow [data-listing]').closest('figure'), im = f.querySelector('img'), b = f.querySelector('.thumbReset');
      return { hidden: b.hidden, before: +im.dataset.scale, after: (b.click(), +im.dataset.scale) }; });
    assert.equal(reset.hidden, false, 'the ↺ is shown once the photo is in');
    assert.equal(reset.after, 1, `the ↺ resets the zoom (${reset.before} → ${reset.after})`);
    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ the row of an approved piece opens its card and returns to the list twice, another opens, a search and back: the same rows and thumbnails, nothing reloaded, no animation');
  } finally { await browser.close(); srv.close(); }
})().then(() => console.log('Thumbnails stay OK')).catch(e => { console.error(e); process.exit(1); });
