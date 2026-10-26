// A charm's .dxf from the charm repo, and the charm card's one row of controls (Paul, 26 Oct 2026: "add an option to download the .dxf file format version
// of every charm in the repo just like we already have for the .ai files. Please update the design for the charms cards so that these buttons are easily
// visible, tastefully positioned in one row with beautiful hover states. The .dxf should also be password gated." and "Fix the UI for the Charm cards that
// have an excessive amount of SKUs attached to them").
// The sorter runs in headless Chromium against the local fake site (bridge-server.cjs) with a mock password door in front of it (as repo-protect-ui.cjs):
//   - .dxf with no token: the password dialog opens and NOTHING is sent to masterDownload, nothing is saved (cancelled)
//   - .dxf with the password: one masterDownload, with the download token; the saved <SKU>.dxf starts with a DXF header, holds the charm's lines, in mm
//   - .ai still saves <SKU>.ai the same way; a sized charm saves <SKU>-<size>.dxf for the size picked
//   - the card of a charm sold under 31 SKUs: nothing leaves the card, the controls are ONE row at the card's foot, cards of a row line up; at 1280, 1600 and 390 px
//   - SHOTS=<dir> keeps screenshots (the grid, a hovered .dxf, a focused .dxf, the 31-SKU card opened)
//   PW_DIR=<dir with playwright-core> CHROMIUM=<chrome> node tests/charm-nest/master-dxf-download.cjs
const fs = require('fs'), path = require('path'), assert = require('assert'), os = require('os'), crypto = require('crypto');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (e) { console.log('skipped: no playwright-core (' + pwDir + ')'); process.exit(0); }
const exe = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
if (!fs.existsSync(exe)) { console.log('skipped: no browser at ' + exe); process.exit(0); }
const { start, Timestamp } = require('./bridge-server.cjs');
const { PDFDocument, rgb } = require(path.join(root, 'vendor/pdf-lib-1.17.1.min.js'));
const shots = process.env.SHOTS || '';
let n = 0; const ok = (c, m) => { assert(c, m); n++; console.log('  ok  ' + m); };
const RIGHT = 'r' + crypto.randomBytes(9).toString('hex');

(async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cn-dxf-'));
  // one charm's .ai as the repo keeps it (lines only, no label text): a 10 × 12 mm tag outline with its hanging hole, stroked near-black
  const MMPT = 72 / 25.4, one = await PDFDocument.create(), pg = one.addPage([20 * MMPT, 20 * MMPT]);
  pg.drawSvgPath(`M ${5 * MMPT} 0 L ${15 * MMPT} 0 L ${15 * MMPT} ${-9 * MMPT} L ${10 * MMPT} ${-12 * MMPT} L ${5 * MMPT} ${-9 * MMPT} Z`, { x: 0, y: 16 * MMPT, borderColor: rgb(0.05, 0.05, 0.05), borderWidth: 0.5 });
  pg.drawCircle({ x: 10 * MMPT, y: 14.5 * MMPT, size: 0.8 * MMPT, borderColor: rgb(0.05, 0.05, 0.05), borderWidth: 0.5 });
  const fx = { bytes: await one.save({ useObjectStreams: false }) };
  const srv = await start({ receipts: [], repoSeed: false });
  const { st, sorterOrigin } = srv, H = 'h'.repeat(64), at = Timestamp.fromMillis(Date.UTC(2026, 9, 26, 12));
  st.put('Charm_Master_Files', H, { masterHash: H, name: 'VIKING.ai', indexedAt: at, charms: 3, labelled: 3, skus: [], indexedBy: 'browser' });
  const put = (sku, extra) => st.put('Charm_Master_Index', sku, Object.assign({ sku, masterHash: H, widthPt: 40, heightPt: 50, areaPt2: 1500, members: 3, holes: 1, upAngle: 0, sym: 'directional', indexedAt: at }, extra));
  const file = p => ({ aiPath: p, aiUrl: srv.blobUrl(p), thumbPath: p.replace(/\.ai$/, '.png') });
  // a Viking rune sold under 31 SKUs (one file, one card), as Paul's screenshot
  const RUNE = Array.from({ length: 31 }, (_, i) => `VIKING_NECKLACE${String(i + 1).padStart(2, '0')}_CHARM-RUNE-ANSUZ-GOLD-FILLED-LONG`);
  for (const sku of RUNE) put(sku, file('charmnest/master/VIKING_RUNE.ai'));
  put('TST-PLAIN', file('charmnest/master/TST-PLAIN.ai'));
  put('TST-SIZED', { sizes: { S: Object.assign({ widthPt: 30, heightPt: 36 }, file('charmnest/master/TST-SIZED-S.ai')), L: Object.assign({ widthPt: 50, heightPt: 60 }, file('charmnest/master/TST-SIZED-L.ai')) } });
  for (const p of ['VIKING_RUNE', 'TST-PLAIN', 'TST-SIZED-S', 'TST-SIZED-L']) st.blobs.set(`charmnest/master/${p}.ai`, { buf: Buffer.from(fx.bytes), generation: 1, meta: { contentType: 'application/illustrator', metadata: { firebaseStorageDownloadTokens: 't' } } });

  // the mock door (repo-protect-ui.cjs): masterDownload needs a download token; the log keeps no password or token
  const log = [], held = new Map();
  const browser = await chromium.launch({ executablePath: exe, args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1280, height: 1000 }, timezoneId: 'UTC', locale: 'en-US' });
  await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
  await ctx.route(/\/\.netlify\/functions\/charmNestLibrary$/, async route => {
    let b = {}; try { b = route.request().postDataJSON() || {}; } catch (_) {}
    const json = (code, body) => route.fulfill({ status: code, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify(body) });
    if (b.op === 'repoUnlock') { log.push({ op: b.op, action: b.action }); if (b.password !== RIGHT) return json(200, { ok: false, error: 'wrong' }); const token = 'tk-' + crypto.randomBytes(6).toString('hex'); held.set(token, b.action); return json(200, { ok: true, token, expiresAt: Date.now() + 60000, once: false, action: b.action }); }
    if (b.op === 'masterDownload') {
      log.push({ op: b.op, sku: b.sku, size: b.size || null, token: !!b.repoToken });
      if (!held.has(b.repoToken) || !['download', 'other'].includes(held.get(b.repoToken))) return json(401, { error: 'repo-locked', need: 'download', why: b.repoToken ? 'invalid' : 'missing' });
      const p = b.sku === 'TST-SIZED' ? `charmnest/master/TST-SIZED-${b.size}.ai` : RUNE.includes(b.sku) ? 'charmnest/master/VIKING_RUNE.ai' : `charmnest/master/${b.sku}.ai`;
      return json(200, { ok: true, url: srv.blobUrl(p), path: p, name: `${b.sku}${b.size ? '-' + b.size : ''}.ai` });
    }
    return route.fallback();
  });
  await ctx.addInitScript(() => { try { localStorage.setItem('cn.employee', 'Tester'); } catch (_) {} window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {}; });
  const page = await ctx.newPage(), errors = [];
  page.on('pageerror', e => errors.push('pageerror: ' + e.message));
  try {
    await page.goto(`${sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && window.CN.S && window.Master && window.CharmNestRepoLock && CN.S.cloud.ok !== null, null, { timeout: 60000 });
    await page.evaluate(() => { const s = CN.S.settings; s.review = 'off'; s.naming = 'off'; s.notify = 'off'; s.sound = 'off'; s.runMode = 'manual'; });
    await page.evaluate(() => CN.setMode('master'));
    await page.waitForFunction(() => document.querySelectorAll('#mGrid .skuTile').length >= 3, null, { timeout: 30000 });
    // what the page saves, kept here: the file name and its bytes as text
    await page.evaluate(() => { window.__dl = []; window.download = (b, name) => window.__dl.push({ name, text: new TextDecoder().decode(b instanceof Uint8Array ? b : new Uint8Array(b)) }); });
    const tile = sku => `#mGrid .skuTile[data-sku="${sku}"]`;

    // ── 1 · the controls: up°, faces, .ai | .dxf, Etsy, Delete in ONE row, every download behind the password ──
    const row = await page.evaluate(sel => { const r = document.querySelector(sel + ' .skuActs'); const kids = [...r.children]; return { kinds: kids.map(k => k.matches('input[data-up]') ? 'up' : k.matches('select[data-faces]') ? 'faces' : k.matches('.dlPair') ? 'pair:' + [...k.children].map(c => c.textContent || c.tagName).join('|') : k.textContent.trim()), tops: [...new Set(kids.map(k => Math.round(k.getBoundingClientRect().top)))], dl: [...r.querySelectorAll('[data-dl]')].every(b => b.dataset.repo === 'download') }; }, tile('TST-PLAIN'));
    ok(row.kinds.includes('pair:.ai|.dxf') && row.kinds.includes('Delete') && !row.kinds.includes('up'), 'the buttons row holds the .ai | .dxf pair, Etsy and Delete: ' + row.kinds.join(', '));
    ok(row.tops.length === 1, 'every button is on ONE row');
    const set = await page.evaluate(sel => { const r = document.querySelector(sel + ' .skuSet'), a = document.querySelector(sel + ' .skuActs'); return r && a ? { kinds: [...r.children].map(k => k.matches('input[data-up]') ? 'up' : k.matches('select[data-faces]') ? 'faces' : k.textContent.trim()), above: r.getBoundingClientRect().bottom <= a.getBoundingClientRect().top + 1 } : null; }, tile('TST-PLAIN'));
    ok(set && set.kinds[0] === 'up' && set.kinds.includes('faces') && set.above, 'the two settings (up°, faces) are their own line, just above the buttons');
    ok(row.dl, 'both downloads are password controls (data-repo=download, the padlock marks)');

    // ── 2 · .dxf with no token: the dialog, and nothing sent ──
    await page.click(tile('TST-PLAIN') + ' [data-fmt=dxf]');
    await page.waitForSelector('dialog.repoDlg[open][data-repo-action=download]');
    ok(log.filter(x => x.op === 'masterDownload').length === 0, '.dxf with no token: the password dialog opens and nothing is asked of masterDownload');
    await page.keyboard.press('Escape'); await page.waitForTimeout(300);
    ok(await page.evaluate(() => window.__dl.length) === 0 && log.filter(x => x.op === 'masterDownload').length === 0, 'cancelled: no file saved, still nothing sent');

    // ── 3 · the password: <SKU>.dxf, a DXF in millimetres with the charm's lines ──
    await page.click(tile('TST-PLAIN') + ' [data-fmt=dxf]');
    await page.waitForSelector('dialog.repoDlg[open]'); { const f = page.locator('dialog.repoDlg[open] #repoPw'); await f.fill(RIGHT); await f.press('Enter'); }
    await page.waitForFunction(() => window.__dl.length === 1, null, { timeout: 20000 });
    const d = await page.evaluate(() => window.__dl[0]);
    const calls = log.filter(x => x.op === 'masterDownload');
    ok(calls.length === 1 && calls[0].token && calls[0].sku === 'TST-PLAIN', '.dxf with the password: one masterDownload, carrying the download token' + (calls.length === 1 ? '' : ': ' + JSON.stringify(log)));
    ok(d.name === 'TST-PLAIN.dxf', 'saved as the SKU .dxf: ' + d.name);
    ok(/^0\r\nSECTION\r\n2\r\nHEADER\r\n9\r\n\$ACADVER/.test(d.text) && /\r\nEOF\r\n?$/.test(d.text), 'it is a DXF (HEADER … EOF)');
    const ents = (d.text.match(/\r\n0\r\nLWPOLYLINE\r\n/g) || []).length + (d.text.match(/\r\n0\r\nHATCH\r\n/g) || []).length;
    ok(ents >= 2, `it holds the charm's lines (${ents} entities: the outline and its hole)`);
    // extents in mm: the fixture's charm is 10 × 12 mm (in points it would be ~28 × 34)
    const ext = k => { const m = new RegExp(`\\$${k}\\r\\n10\\r\\n([-\\d.e]+)\\r\\n20\\r\\n([-\\d.e]+)`).exec(d.text); return m ? [+m[1], +m[2]] : null; };
    const lo = ext('EXTMIN'), hi = ext('EXTMAX'), w = hi[0] - lo[0], h = hi[1] - lo[1];
    ok(w > 5 && w < 16 && h > 5 && h < 18, `in millimetres: the charm is ${w.toFixed(1)} × ${h.toFixed(1)} mm`);

    // ── 4 · .ai still saves the file; a sized charm's .dxf takes the size picked ──
    await page.click(tile('TST-PLAIN') + ' [data-fmt=ai]');
    await page.waitForFunction(() => window.__dl.length === 2, null, { timeout: 20000 });
    ok(await page.evaluate(() => window.__dl[1].name) === 'TST-PLAIN.ai' && /^%PDF/.test(await page.evaluate(() => window.__dl[1].text)), '.ai still saves <SKU>.ai, the file itself, with the same unlock');
    await page.selectOption(tile('TST-SIZED') + ' select[data-dlsize]', 'L');
    await page.click(tile('TST-SIZED') + ' [data-fmt=dxf]');
    await page.waitForFunction(() => window.__dl.length === 3, null, { timeout: 20000 });
    ok(await page.evaluate(() => window.__dl[2].name) === 'TST-SIZED-L.dxf' && log.filter(x => x.op === 'masterDownload').pop().size === 'L', 'a sized charm: the picker sets the size; saved as <SKU>-L.dxf');

    // ── 5 · the 31-SKU card: nothing leaves the card; the controls row at the foot; cards of a row line up (1280, 1600, 390 px) ──
    const look = () => page.evaluate(() => {
      const tiles = [...document.querySelectorAll('#mGrid .skuTile')], out = [];
      for (const t of tiles) { const r = t.getBoundingClientRect(); for (const el of t.querySelectorAll('*')) { const e = el.getBoundingClientRect(); if (e.width && el.checkVisibility() && (e.right > r.right + 0.5 || e.left < r.left - 0.5)) out.push(t.dataset.sku.slice(0, 20) + ' ' + (el.className || el.tagName)); } }
      const rowsOf = {}; for (const t of tiles) { const top = Math.round(t.getBoundingClientRect().top); (rowsOf[top] = rowsOf[top] || []).push(t); }
      const aligned = Object.values(rowsOf).every(ts => new Set(ts.map(t => Math.round(t.querySelector('.skuActs').getBoundingClientRect().bottom))).size === 1);
      const oneRow = tiles.every(t => { const tops = [...t.querySelector('.skuActs').children].map(k => k.getBoundingClientRect().top); return Math.max(...tops) - Math.min(...tops) <= 2; });   // (a hovered control lifts 1 px)
      const atFoot = tiles.every(t => t.getBoundingClientRect().bottom - t.querySelector('.skuActs').getBoundingClientRect().bottom < 12);
      return { out, aligned, oneRow, atFoot, scroll: document.documentElement.scrollWidth > innerWidth + 1 };
    });
    await page.mouse.move(0, 0);
    // (the dark rail folds below 900 px, as the page's own toggle does)
    const fold = w => page.evaluate(w => { const off = document.getElementById('app').classList.contains('railOff'); if ((w < 900) !== off) document.getElementById('btnRail').click(); }, w);
    for (const [w, h] of [[1280, 1000], [1600, 1000], [390, 844]]) {
      await page.setViewportSize({ width: w, height: h }); await fold(w); await page.waitForTimeout(400);
      for (const open of [false, true]) {
        await page.evaluate(o => { const m = document.querySelector('#mGrid .skuMore'); if (m) m.open = o; }, open); await page.waitForTimeout(150);
        const L = await look(), tag = `${w} px${open ? ', +29 more opened' : ''}`;
        ok(L.out.length === 0, `${tag}: nothing leaves its card` + (L.out.length ? ': ' + L.out.slice(0, 4).join('; ') : ''));
        ok(L.oneRow && L.atFoot && L.aligned, `${tag}: the controls are one row at the foot of every card, lined up across the row`);
        ok(!L.scroll, `${tag}: no sideways scroll`);
      }
    }
    const opened = await page.evaluate(() => { const m = document.querySelector('#mGrid .skuMore'); m.open = true; return { h: m.getBoundingClientRect().height, scroll: m.scrollHeight > m.clientHeight, title: m.closest('.skuTile').querySelector('.sku').title.split(', ').length }; });
    ok(opened.h <= 134 && opened.scroll, `"+N more SKUs" opens inside the card into a scrolled list (${Math.round(opened.h)} px)`);
    ok(opened.title === 31, 'the card name carries every SKU in its title');
    await page.evaluate(() => { document.querySelector('#mGrid .skuMore').open = false; });
    if (shots) {
      fs.mkdirSync(shots, { recursive: true });
      for (const [w, h] of [[1280, 900], [1600, 900], [390, 844]]) { await page.setViewportSize({ width: w, height: h }); await fold(w); await page.waitForTimeout(300); await page.locator('#mGrid').screenshot({ path: path.join(shots, `master-grid-${w}.png`) }); }
      await page.setViewportSize({ width: 1280, height: 900 }); await fold(1280); await page.waitForTimeout(300);
      await page.hover(tile('TST-PLAIN') + ' [data-fmt=dxf]'); await page.waitForTimeout(250); await page.locator(tile('TST-PLAIN')).screenshot({ path: path.join(shots, 'card-hover-dxf.png') });
      await page.mouse.move(0, 0); await page.focus(tile('TST-PLAIN') + ' [data-fmt=dxf]'); await page.keyboard.press('Shift+Tab'); await page.keyboard.press('Tab'); await page.waitForTimeout(200);
      await page.locator(tile('TST-PLAIN')).screenshot({ path: path.join(shots, 'card-focus-dxf.png') });
      await page.evaluate(() => { document.querySelector('#mGrid .skuMore').open = true; }); await page.waitForTimeout(150);
      await page.locator('#mGrid .skuMore').locator('xpath=ancestor::div[contains(@class,"skuTile")]').screenshot({ path: path.join(shots, 'card-31-skus-open.png') });
    }
    ok(errors.length === 0, 'no page errors' + (errors.length ? ': ' + errors.join(' | ') : ''));
    console.log(`master .dxf download and charm card OK (${n} checks)`);
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
