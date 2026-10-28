// CUSTOMORDERS (Paul, 28 Oct 2026: "Custom made charm, even though it's not attached to an existing order must still have all the same abilities and features that a regular order has. This means
// that I need to be able to see it in the orders tab in the list view. It needs to be searchable and the detailed order view needs to work for it just like any other order. This must work for both
// matched custom charms to a custom order and extra charms that the user decide to add the do not have a matching order." · "I noticed that the 'Open Order' button does not show up when the custom
// charm is selected"). In a real Chromium over the fake site (bridge-server.cjs: the real charmNestLibrary handlers over an in-memory store):
//   1  an order-less piece ("Spare heart") and an order's piece (order 4170000555, which this sorter never pulled) are sent to SS
//   2  the Orders tab lists both: the order-less one as its own order, CUSTOM-<yyyymmdd>-<n>, named by its name; the order's piece under order 4170000555
//   3  the search finds each by its number, its name and the word "custom", and a hit opens the order window on it
//   4  the order window opens on either (OrderWin.openOrder), names the piece, and its Sheet tab finds the SS sheet it is on
//   5  the sheet window: the custom piece names its order and has Open order, which opens the order window on it
//   6  the pull is untouched: B.orders.rows holds neither; and a page that holds no pool row of them (another computer) lists them from the cloud (customPieceList)
//   7  no page error, no warning swallowed by the order window
//   node tests/charm-nest/custom-piece-orders.cjs     (PW_DIR, CHROMIUM)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');

(async () => {
  const pwDir = process.env.PW_DIR || [path.join(root, 'node_modules'), '/opt/node22/lib/node_modules/playwright/node_modules'].find(d => fs.existsSync(path.join(d, 'playwright-core')));
  let chromium; try { ({ chromium } = require(path.join(pwDir || '', 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { start } = require('./bridge-server.cjs'), { scene } = require('./_masterScene.cjs');
  const pdf = await scene({ page: [420, 200], bodies: [], extraOps: [[20, 20], [80, 20], [140, 20]].map(([x, y]) => `q 0 0 0 RG 0.25 w ${x} ${y} 20 20 re S Q`) });
  const srv = await start({ receipts: [] }), { st, sorterOrigin } = srv;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 950 }, reducedMotion: 'reduce' });
  await ctx.route(url => !/^https?:\/\/(127\.0\.0\.1|localhost)[:/]/.test(url.href), r => {
    const u = r.request().url();
    if (/gstatic\.com\/firebasejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(u) ? '' : "const nope = () => { throw new Error('firebase stub'); }; export const getFirestore = nope, initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;" });
    if (/qrcodejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) });
    if (/fonts\.g/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
    return r.abort();
  });
  await ctx.addInitScript(() => { try { localStorage.setItem('cn.employee', 'Tester'); const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); Object.assign(s, { sandbox: 'off', sandboxStream: 'off', pollOrders: 'off', runMode: 'manual', sound: 'off', notify: 'off', dsOrigin: 'http://127.0.0.1:9' }); localStorage.setItem('cn.settings', JSON.stringify(s)); } catch (_) {} window.confirm = () => true; window.alert = () => {}; window.prompt = () => 'Tester'; });
  const page = await ctx.newPage(), errors = [];
  page.on('pageerror', e => errors.push('page: ' + e.message));
  page.on('console', m => { if (m.type() === 'error' && !/firebase stub|Failed to load resource|ERR_FAILED|net::|status of 4\d\d/.test(m.text())) errors.push('console: ' + m.text().slice(0, 300)); });
  page.setDefaultTimeout(30000);
  const until = async (fn, ms = 30000, what = '') => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await new Promise(r => setTimeout(r, 150)); } };
  try {
    await page.goto(`${sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.SheetWin && window.CNManual && window.CharmNestCustomPool && window.CharmNestDesigns && window.Pool && typeof Pool.addCustomPiece === 'function', null, { timeout: 60000 });
    // the SS sheet a design goes to: three squares dropped on Sterling Silver, nested and saved (an open sheet with pieces on it)
    await page.evaluate(async b64 => { const u = Uint8Array.from(atob(b64), c => c.charCodeAt(0)); await CN.intake([new File([u], 'squares.pdf', { type: 'application/pdf' })], 'silver'); }, pdf.toString('base64'));
    const settled = (metal, n) => page.evaluate(([metal, n]) => { const sh = CN.S.sheets[metal].pages.at(-1); if (sh.charms.length === n && sh.status === 'ready' && !sh._operationStarting) CN.startNest(sh); return sh.charms.length === n && sh.placements.length === n && ['complete', 'partial'].includes(sh.status) && sh.persistedDone && !!sh.verification && sh.verification.ok && !sh.dirty && !!sh.sheetId && !sh.problem; }, [metal, n]);
    await until(() => settled('silver', 3), 120000, 'the SS sheet of three squares nested and saved');
    const ss0 = await page.evaluate(() => { const sh = CN.S.sheets.silver.pages.at(-1); return { id: sh.sheetId, pages: CN.S.sheets.silver.pages.length }; });

    /* ── 1 · a design of the library, sent to SS with no order ── */
    // a 20 × 16 mm piece (its corners rounded with curves), a red engraved zig-zag, a hatched diamond, and words already outlined in a group the editor moved and turned
    const MMp = 72 / 25.4, W = 20 * MMp, H = 16 * MMp, r = 3 * MMp, k = 0.5523 * r;
    const outline = [[['m', [r, 0]], ['l', [W - r, 0]], ['c', [W - r + k, 0], [W, r - k], [W, r]], ['l', [W, H - r]], ['c', [W, H - r + k], [W - r + k, H], [W - r, H]], ['l', [r, H]], ['c', [r - k, H], [0, H - r + k], [0, H - r]], ['l', [0, r]], ['c', [0, r - k], [r - k, 0], [r, 0]], ['h']]];
    const zig = [[['m', [8, 10]], ['l', [14, 16]], ['l', [20, 10]], ['l', [26, 16]], ['l', [32, 10]]]];
    const diamond = [[['m', [40, 24]], ['l', [46, 30]], ['l', [40, 36]], ['l', [34, 30]], ['h']]];
    // (the matrix the Charm Editor hands back with a group: T(c + t) · R(rot) · T(−c), c the centre of its parts' box)
    const turnAbout = (deg, cx, cy, tx, ty) => { const a = deg * Math.PI / 180, cs = Math.cos(a), sn = Math.sin(a); return [cs, sn, -sn, cs, cx + tx - (cs * cx - sn * cy), cy + ty - (sn * cx + cs * cy)]; };
    const blackLevel = [[['m', [44, 6]], ['l', [52, 6]], ['l', [52, 12]], ['l', [44, 12]], ['h']]], greyLevel = [[['m', [44, 14]], ['l', [52, 14]], ['l', [52, 20]], ['l', [44, 20]], ['h']]];
    const letters = [[['m', [12, 30]], ['l', [16, 30]], ['l', [16, 38]], ['l', [12, 38]], ['h']], [['m', [18, 30]], ['l', [24, 30]], ['l', [24, 33]], ['l', [18, 33]], ['h']]];
    const design0 = { name: 'Spare heart test', source: { kind: 'new' }, widthMm: 20, heightMm: 16, faces: { front: { face: 'front', unitPt: true, frame: { x0: 0, y0: 0, wPt: W, hPt: H }, items: [
      { id: 'out', kind: 'path', role: 'cut', subpaths: outline, stroke: [0, 0, 0] },
      { id: 'zig', kind: 'path', role: 'line', subpaths: zig, stroke: [1, 0, 0] },
      { id: 'grp', kind: 'group', tx: 2, ty: -1, rot: 10, matrix: turnAbout(10, 40, 30, 2, -1), children: [{ id: 'dia', kind: 'path', role: 'hatch', subpaths: diamond, fill: [0, 0, 1] }] },
      { id: 'hidden', kind: 'path', role: 'line', hidden: true, subpaths: [[['m', [5, 5]], ['l', [9, 9]]]], stroke: [1, 0, 0] },
      { id: 'txt', kind: 'text', role: 'hatch', text: 'AB', fontKey: 'ace', sizePt: 8, subpaths: letters, fill: [0, 0, 1] },
      // (PENS, Paul 28 Oct 2026: "the image turned blue ... The Image must retain it's vector greyscale": a picture traced in the Charm Editor is a filled path per grey level, black the darkest)
      { id: 'img.g0', kind: 'path', role: 'hatch', subpaths: blackLevel, fill: [0, 0, 0], stroke: false, even: true },
      { id: 'img.g5', kind: 'path', role: 'hatch', subpaths: greyLevel, fill: [0.5, 0.5, 0.5], stroke: false, even: true }] } } };
    const saved = await page.evaluate(async d => { const s = await CharmNestDesigns.put(d); const got = await CharmNestDesigns.get(s.id, s.version); return { s: { id: s.id, version: s.version, hash: s.hash }, design: got }; }, design0);
    assert(saved.s.id && saved.s.version === 1 && /^[0-9a-f]{64}$/.test(saved.s.hash), 'the design is in the library: ' + JSON.stringify(saved.s));
    const send = (metal, extra) => page.evaluate(([d, s, metal, extra]) => Pool.addCustomPiece(Object.assign({ design: d, version: s.version, hash: s.hash, metal, orderId: null, customName: 'Spare heart', quantity: 1, by: 'Tester' }, extra || {})), [saved.design, saved.s, metal, extra || null]);

    const ids1 = await send('silver');
    const ids2 = await send('silver', { orderId: '4170000555', customName: 'Replacement charm', quantity: 1 });
    await until(() => settled('silver', 5), 180000, 'both custom pieces nested on the SS sheet and saved');
    const cid = ids1[0].split('_')[0], rid2 = '4170000555';
    const warns = []; page.on('console', m => { if (m.type() === 'warning' && /^order view:|custom orders|\[orders\]/.test(m.text())) warns.push(m.text().slice(0, 300)); });
    // ── 2 · the Orders tab ──
    await page.evaluate(() => { CN.setMode('orders'); Orders.render(); });
    await until(() => page.evaluate(([a, b]) => !!document.querySelector(`#ordItems [data-rid="${a}"]`) && !!document.querySelector(`#ordItems [data-rid="${b}"]`), [cid, rid2]), 20000, 'both custom orders in the Orders list');
    const list = await page.evaluate(([a, b]) => [a, b].map(r => { const n = document.querySelector(`#ordItems [data-rid="${r}"]`); return { rid: r, text: n.innerText.replace(/\s+/g, ' ').slice(0, 300) }; }), [cid, rid2]);
    assert(/Spare heart/.test(list[0].text) && /Custom/.test(list[0].text), 'the order-less piece is listed as its own order, by its name: ' + JSON.stringify(list[0]));
    assert(/Custom/.test(list[1].text), 'the order\'s piece is listed under its order: ' + JSON.stringify(list[1]));
    if (process.env.SHOT) { await new Promise(r => setTimeout(r, 2500)); await page.locator(`#ordItems [data-rid="${cid}"]`).first().screenshot({ path: process.env.SHOT + '-list.png' }); }
    const holdBtns = await page.evaluate(cid => document.querySelectorAll(`#ordItems [data-rid="${cid}"] .revSendBtn`).length, cid);
    assert.equal(holdBtns, 0, 'an order-less custom piece has no Hold Order (no Etsy order for the Review record to name)');
    console.log('ok   2  the Orders list: ' + list.map(x => x.text.slice(0, 90)).join(' | '));
    // ── 3 · the search ──
    const q = s => page.evaluate(s => OrderSearch.query(s).hits.map(h => h.e.rid), s);
    await page.evaluate(() => OrderSearch.rebuild && OrderSearch.rebuild());
    const byNum = await q(cid), byName = await q('spare heart'), byWord = await q('custom'), byNum2 = await q(rid2);
    assert(byNum[0] === cid && byName.includes(cid) && byWord.includes(cid) && byWord.includes(rid2) && byNum2[0] === rid2, 'the search finds them: ' + JSON.stringify({ byNum, byName, byWord, byNum2 }));
    console.log('ok   3  the search finds them by number, name and "custom"');
    // ── 4 · the order window ──
    const win = async rid => {
      await page.evaluate(() => { if (OrderWin.isOpen()) OrderWin.close(); }); await new Promise(r => setTimeout(r, 500));
      await page.evaluate(rid => OrderWin.openOrder(rid), rid);
      await until(() => page.evaluate(rid => OrderWin.isOpen() && OrderWin.rid && String(OrderWin.rid()) === rid, rid), 15000, 'the order window on ' + rid);
      await new Promise(r => setTimeout(r, 1500));
      return page.evaluate(() => ({ rid: OrderWin.rid(), key: OrderWin.key(), text: document.getElementById('orderWin').innerText.replace(/\s+/g, ' ').slice(0, 600) }));
    };
    const w1 = await win(cid);
    if (process.env.SHOT) await page.screenshot({ path: process.env.SHOT + '-win.png' });
    const w2 = await win(rid2);
    assert(w1.rid === cid && /Spare heart/.test(w1.text) && !/no piece in the sorter/.test(w1.text), 'the order window on the order-less piece: ' + JSON.stringify(w1));
    assert(w2.rid === rid2 && /Replacement charm/.test(w2.text), 'the order window on the order\'s piece: ' + JSON.stringify(w2));
    await page.evaluate(() => OrderWin.setView('sheet')); await new Promise(r => setTimeout(r, 2500));
    const sv = await page.evaluate(() => { const s = OrderWin._scope(); return s && s.all.map(p => ({ name: p.name, sheets: p.sheets.map(h => h.label) })); });
    assert(sv && sv.some(p => p.sheets.length), 'its Sheet tab finds the sheet it is on: ' + JSON.stringify(sv));
    console.log('ok   4  the order window opens on each; its Sheet tab: ' + JSON.stringify(sv));
    // ── 5 · the sheet window's Open order ──
    await page.evaluate(() => { if (OrderWin.isOpen()) OrderWin.close(); }); await new Promise(r => setTimeout(r, 500));
    const sid = await page.evaluate(id => CN.S.sheets.silver.pages.find(p => p.charms.some(c => c.poolId === id)).sheetId, ids1[0]);
    await page.evaluate(([sid, id]) => SheetWin.open(sid, { select: id }), [sid, ids1[0]]);
    await until(() => page.evaluate(() => !!document.querySelector('[data-r2=openOrd]')), 15000, 'Open order in the sheet window');
    const head = await page.evaluate(() => document.querySelector('.swOrderHead').innerText.replace(/\s+/g, ' '));
    await page.click('[data-r2=openOrd]');
    await until(() => page.evaluate(cid => OrderWin.isOpen() && String(OrderWin.rid()) === cid, cid), 15000, 'the order window from the sheet window');
    console.log('ok   5  the sheet window names its order (' + head.slice(0, 80) + ') and Open order opens it');
    // ── 6 · the pull untouched; another computer lists them from the cloud ──
    const pull = await page.evaluate(() => Orders.rows().filter(r => /CUSTOM/.test(r.line && r.line.sku || '') || r.custom).length);
    assert.equal(pull, 0, 'the pull holds no custom row');
    const other = await page.evaluate(async ([a, b]) => { for (const [id, p] of [...B.pool.rows]) if (p.custom) B.pool.rows.delete(id); await CustomOrders.load(true); return [a, b].map(r => CustomOrders.rowsOf(r).length); }, [cid, rid2]);
    assert.deepEqual(other, [1, 1], 'listed from the cloud: ' + other);
    console.log('ok   6  the pull holds no custom row; from the cloud alone both are listed');
    assert.deepEqual(errors, [], 'no page errors'); assert.deepEqual(warns, [], 'no swallowed order-view warnings');
    console.log('custom-piece-orders: all checks passed');
  } catch (e) { console.error(e); if (errors.length) console.error(errors.join('\n')); process.exitCode = 1; }
  finally { await browser.close().catch(() => {}); try { await srv.close(); } catch (_) {} }
})();
