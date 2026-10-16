// The order window's head card, "Where it is now" (#owNowCard): Paul, 12 Oct 2026, 14:43 UTC, his picture 2 (a label NEEDS A DECISION, the title "Unknown SKU: SKU MASKS
// HAPPY/SAD: not in any master file", the seal ON SHEET 12 OCT 2026 10:31 AM, two buttons "Open timeline" and "Sheet", two sheet shortcut pills on the right):
//   "this title bar is too large and too crowded with needless things. I need you to remove all buttons from this UI including the Sheet shortcuts. Shrink the
//    height of the remaining seal and title section by 33% and extend the title text all the way to the right side of this box to fill in the empty space left
//    behind from the removal of the Sheet buttons."
// Proved here, in headless Chromium over the fake site (tests/charm-nest/bridge-server.cjs: the real charmNestLibrary ops over an in-memory store; fake buyer names only,
// nothing live, no Etsy, no paid call), in every state the card has (needs a decision, a long reason, on a sheet, order in, on hold, cancelled, completed, a split
// pair, no seal) and at 1920, 1600, 1440, 1280, 1100, 1024, 900, 768, 600 and 390 px:
//  - the card holds no button, link, input or sheet shortcut at all (and the checker itself catches one put back: a mutant)
//  - ONE height for every state and every width, 79 px: 67% of the 118 px Paul's picture shows (236 px at 2x), within 2 px; and a third (or more) shorter than the card
//    measured on main (071b55e, the same fixtures, this browser: 116 px at 1920 and 1600, 123.94 px at 1440 and 1280, 146 px to 296 px narrower, 156 px for a cancelled order)
//  - the seal came down with it (50 to 44 px) and so did the title (19 to 17 px), both still in the card, the seal untouched (the same seal models as before)
//  - the title runs the whole width of the card to its right edge (no column left for the shortcuts), wraps naturally, is cut on its second line with the full text in its
//    title attribute; the label stays; one quiet line (a split pair's sentence, who did it when no seal says it, a cancelled order's steps) sits under a one-line title
//  - nothing became unreachable: the Timeline and Sheet tabs open what the two buttons opened, a piece's status chip opens its sheet, the seal opens the Timeline,
//    and the Sheet view lists every sheet of a piece that is on two
//   node tests/charm-nest/order-titlebar.cjs   (PW_DIR=<playwright-core's node_modules>, CHROMIUM=<chrome>, SHOTS=<dir>)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

/* what main drew, measured with this very file's fixtures and browser before the change (the card's height, px) */
const OLD_PICTURE = 236 / 2;                                   // Paul's own picture: the card is 236 px tall at 2x
const OLD_MAIN = { 1920: 116, 1600: 116, 1440: 123.94, 1280: 123.94 };   // the needs-a-decision card on main, by window width
const NEW_H = 79, TARGET = 0.67 * OLD_PICTURE;                 // 79.06
const WIDTHS = [1920, 1600, 1440, 1280, 1100, 1024, 900, 768, 600, 390];

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 14, 17) / 1000), T0 = Date.UTC(2026, 9, 12, 12, 0, 0), MIN = 60000;
const GOLD = ['gold', '14k Gold Filled'], SILVER = ['silver', 'Sterling Silver'];
const mk = (rid, buyer, specs) => {
  const o = { rid, t: {} };
  const lines = specs.map((s, i) => { const tid = rid + (i + 1); o.t[s.n] = tid; return { transactionId: tid, listingId: '19008' + tid.slice(-5), sku: s.sku, title: s.title || s.sku, quantity: s.qty || 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: s.m[1] }], metalKey: s.m[0], metalLabel: s.m[1], personalization: [], buyerMessage: '' }; });
  o.order = { receiptId: rid, orderNumber: rid, createTs: SHIP - 7 * DAY, updateTs: SHIP - 7 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines };
  o.specs = specs; return o;
};
const kOf = (o, n) => `${o.rid}_${o.t[n]}`, pidOf = (o, n, c) => `${o.rid}_${o.t[n]}_${c || 1}`;
const COMEDY = 'COMEDY 89578', MASK = 'MASKS HAPPY/SAD', DUCK = 'DUCK 38090', LONGSKU = 'EXTRA LONG DESIGN NAME FOR A VERY LONG TITLE';
const PAUL = mk('4195153621', 'Ina Sample', [{ n: 'c', sku: COMEDY, m: GOLD }, { n: 'm', sku: MASK, m: GOLD }]);   // Paul's picture: COMEDY on a sheet, MASKS an unknown SKU
const ONS = mk('4195153622', 'Ola Sample', [{ n: 'c', sku: COMEDY, m: GOLD }, { n: 'd', sku: DUCK, m: SILVER }]);   // both on sheets
const WAIT = mk('4195153623', 'Wen Sample', [{ n: 'c', sku: COMEDY, m: GOLD }, { n: 'd', sku: DUCK, m: SILVER }]);  // order in, nothing placed
const HELD = mk('4195153624', 'Hal Sample', [{ n: 'c', sku: COMEDY, m: GOLD }, { n: 'd', sku: DUCK, m: SILVER }]);  // on hold
const CANC = mk('4195153625', 'Cai Sample', [{ n: 'c', sku: COMEDY, m: GOLD }, { n: 'd', sku: DUCK, m: SILVER }]);  // cancelled, with its steps
const HAND = mk('4195153626', 'Han Sample', [{ n: 'c', sku: COMEDY, m: GOLD }, { n: 'd', sku: DUCK, m: SILVER }]);  // completed by hand (custom orders)
const LONG = mk('4195153627', 'Lon Sample', [{ n: 'c', sku: COMEDY, m: GOLD }, { n: 'm', sku: LONGSKU, m: GOLD }]);  // the same as Paul's, a longer SKU
const TWO = mk('4195153628', 'Tia Sample', [{ n: 'c', sku: COMEDY, m: GOLD, qty: 2 }, { n: 'd', sku: DUCK, m: SILVER }]);   // one piece on two sheets
const ALL = [PAUL, ONS, WAIT, HELD, CANC, HAND, LONG, TWO];
// the controls the card used to have, and every other thing a hand could press
const CONTROLS = 'button, a[href], input, select, textarea, summary, [role=button], [role=link], [onclick], .shs, .owShChip, .owShItem, .owShMore, [data-go], [data-pc], [data-pc-sheet]';

async function main() {
  // ── the source: the card's builder holds no button, no shortcut, no Open timeline; the dead builders are gone; the one-height rule is in the CSS ──
  const src = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8'), html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
  const a = src.indexOf('  function paintNow(r) {'), b = src.indexOf('  function splitLine(r) {');
  assert(a > 0 && b > a, 'the card\'s builder is found');
  const code = src.slice(a, b).replace(/\/\*[\s\S]*?\*\/|\n\s*\/\/[^\n]*/g, '');
  assert(!/<button|data-go|owShChip|sheetCard|Open timeline|owShItem/.test(code), 'the card\'s builder draws no button, no sheet chip and no "Open timeline": ' + (code.match(/<button|data-go|owShChip|sheetCard|Open timeline|owShItem/) || [])[0]);
  assert(!/function sheetCard\(|function handBoxSealOf\(/.test(src), 'the builders of the Sheet button, the sheet chips and the Completed-by-hand boxes are gone');
  const css = html.slice(html.indexOf('/* where it is now: the head card'), html.indexOf('.owShChip{display:flex'));
  assert(/--owHeadH:79px/.test(css) && /height:var\(--owHeadH\)/.test(css), 'one height for the card in the CSS');
  assert(!/\.owNowCard \.row|\.owNowCard \.shs|\.owNowCard \.btn/.test(html), 'no rule is left for the card\'s button row, its shortcut column or its buttons');
  assert(!/\.owShItem|\.owShMore/.test(html), 'no rule is left for the shortcut boxes');
  assert(/\.owShChip\{display:flex/.test(html) && /(\.owPcRow|:is\(\.owPcRow,\.owPcSub\)) \.pcSt\{/.test(html), 'the chip style stays: a piece\'s status chip is built on it (the row\'s and, since ONEROW, its drawer lines\')');
  assert(!/@media[^{]*\{[^}]*\.owNowCard,\.owNowCard\.sealed/.test(html), 'the card does not stack under 900 px any more (it has no column of shortcuts to drop under it)');
  const tag = (html.match(/<script src="charm-nest-bridge\.js(\?v=[^"]*)"/) || [])[1] || '';
  assert(/-sui1(?:-[a-z0-9]+)*-tb1/.test(tag), 'the bridge token keeps its earlier suffixes and appends tb1');
  console.log('  ok  order-titlebar: the source');

  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); return; }
  const shots = process.env.SHOTS || null; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] }), st = srv.st;
  const LIB = `${srv.sorterOrigin}/.netlify/functions/charmNestLibrary`;
  const post = async body => (await fetch(LIB, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })).json();
  const GF1 = 'sheet-tb-gf1', GF2 = 'sheet-tb-gf2', SS1 = 'sheet-tb-ss1';
  const sheetDocs = { [GF1]: { metal: 'gold', n: 1, charms: [] }, [GF2]: { metal: 'gold', n: 2, charms: [] }, [SS1]: { metal: 'silver', n: 1, charms: [] } };
  const onSheet = (sid, o, n, sku, copy) => sheetDocs[sid].charms.push({ id: `c${sheetDocs[sid].charms.length + 1}`, name: `${o.rid} · ${sku}`, poolId: pidOf(o, n, copy), order: o.rid, sku });
  const poolRow = (o, n, sku, material, sheetId, extra, copy) => st.put('Charm_Pool', pidOf(o, n, copy), Object.assign({ poolId: pidOf(o, n, copy), orderId: o.rid, transactionId: o.t[n], lineKey: kOf(o, n), sku, material, copy: copy || 1, quantity: 1, state: sheetId ? 'written' : 'pooled', sheetId: sheetId || null, sheetName: sheetId ? `2026-10-04_${material === 'gold' ? 'GF' : 'SS'}_Set-1_Sheet-${sheetDocs[sheetId].n}` : '', updatedAt: Date.now() }, extra || {}));
  let evN = 0;
  const ev = (o, type, at, extra) => st.put('Order_Timeline', `${o.rid}~${type}~e${++evN}`, Object.assign({ orderId: o.rid, type, at, by: 'Paul', source: 'sorter', station: '', text: '', data: {} }, extra || {}));
  const nm = sid => `${sheetDocs[sid].metal === 'gold' ? 'GF' : 'SS'} Sheet ${sheetDocs[sid].n}`;
  const placed = (o, n, sid, at, copy) => ev(o, 'placed', at, { id: `pl-${kOf(o, n)}-${at}`, sheet: nm(sid), sheetId: sid, lineKey: kOf(o, n), transactionId: o.t[n], text: `Placed on ${nm(sid)}`, data: { poolId: pidOf(o, n, copy) } });
  for (const o of ALL) ev(o, 'arrived', T0, { source: 'etsy', by: 'Etsy', id: 'ar-' + o.rid });
  const ATP = Date.UTC(2026, 9, 12, 14, 31, 0);   // 10:31 AM in Toronto
  onSheet(GF1, PAUL, 'c', COMEDY); poolRow(PAUL, 'c', COMEDY, 'gold', GF1); placed(PAUL, 'c', GF1, ATP);
  poolRow(PAUL, 'm', MASK, 'gold', null);
  ev(PAUL, 'needsDecision', ATP + MIN, { id: 'nd-' + PAUL.rid, lineKey: kOf(PAUL, 'm'), text: 'Unknown SKU: SKU MASKS HAPPY/SAD: not in any master file' });
  onSheet(GF1, LONG, 'c', COMEDY); poolRow(LONG, 'c', COMEDY, 'gold', GF1); placed(LONG, 'c', GF1, ATP);
  poolRow(LONG, 'm', LONGSKU, 'gold', null);
  ev(LONG, 'needsDecision', ATP + MIN, { id: 'nd-' + LONG.rid, lineKey: kOf(LONG, 'm'), text: 'Unknown SKU: SKU EXTRA LONG DESIGN NAME FOR A VERY LONG TITLE: not in any master file' });
  onSheet(GF1, ONS, 'c', COMEDY); onSheet(SS1, ONS, 'd', DUCK); poolRow(ONS, 'c', COMEDY, 'gold', GF1); poolRow(ONS, 'd', DUCK, 'silver', SS1);
  placed(ONS, 'c', GF1, ATP); placed(ONS, 'd', SS1, ATP + MIN);
  poolRow(WAIT, 'c', COMEDY, 'gold', null); poolRow(WAIT, 'd', DUCK, 'silver', null);
  poolRow(HELD, 'c', COMEDY, 'gold', null, { state: 'abandoned' }); poolRow(HELD, 'd', DUCK, 'silver', null, { state: 'abandoned' });
  placed(HELD, 'c', GF1, ATP); placed(HELD, 'd', SS1, ATP + MIN);
  ev(HELD, 'removed', ATP + 20 * MIN, { id: 'rm1-' + HELD.rid, sheet: 'GF Sheet 1', text: 'Taken off GF Sheet 1' });
  ev(HELD, 'held', ATP + 22 * MIN, { id: 'held-' + HELD.rid, sheet: 'GF Sheet 1', text: 'Taken off GF Sheet 1 by Paul: Add to next sheet, on hold', data: { pieces: 2, note: 'Add to next sheet' } });
  poolRow(CANC, 'c', COMEDY, 'gold', null, { state: 'abandoned' }); poolRow(CANC, 'd', DUCK, 'silver', null, { state: 'abandoned' });
  placed(CANC, 'c', GF1, ATP); placed(CANC, 'd', SS1, ATP + MIN);
  const rec = (o, n, o2) => st.put('Charm_Custom_Orders', kOf(o, n), Object.assign({ key: kOf(o, n), receiptId: o.rid, transactionId: o.t[n], sku: 'x', title: 'x', category: 'Unknown SKU', kind: '', state: 'completed', updatedAtMs: Date.now() }, o2));
  const TH = ATP + 300 * MIN;
  rec(HAND, 'c', { how: 'button', completedAt: TH, completedBy: 'Paul', stamps: [{ how: 'button', at: TH, by: 'Paul' }] });
  rec(HAND, 'd', { how: 'print', completedAt: TH + MIN, completedBy: 'Paul', printedAt: TH + MIN, printedBy: 'Paul', lastPrintedAt: TH + MIN, lastPrintedBy: 'Paul', prints: 1, hasLabel: true, stamps: [{ how: 'print', at: TH + MIN, by: 'Paul' }] });
  ev(HAND, 'sealCompleted', TH, { id: `hand-b-${HAND.rid}`, lineKey: kOf(HAND, 'c'), transactionId: HAND.t.c, text: 'Completed', data: { how: 'button', completed: true, pressedIn: 'Review · Open' } });
  ev(HAND, 'sealPrinted', TH + MIN, { id: `hand-p-${HAND.rid}`, lineKey: kOf(HAND, 'd'), transactionId: HAND.t.d, text: 'QR label printed', data: { how: 'print', prints: 1, completed: true } });
  // TWO: the COMEDY line is quantity 2, copy 1 on GF Sheet 1 and copy 2 on GF Sheet 2; the DUCK on SS Sheet 1
  onSheet(GF1, TWO, 'c', COMEDY, 1); onSheet(GF2, TWO, 'c', COMEDY, 2); onSheet(SS1, TWO, 'd', DUCK);
  poolRow(TWO, 'c', COMEDY, 'gold', GF1, null, 1); poolRow(TWO, 'c', COMEDY, 'gold', GF2, null, 2); poolRow(TWO, 'd', DUCK, 'silver', SS1);
  placed(TWO, 'c', GF1, ATP, 1); placed(TWO, 'c', GF2, ATP + 500, 2); placed(TWO, 'd', SS1, ATP + MIN);
  for (const [sid, d] of Object.entries(sheetDocs)) {
    d.charms.push({ id: 'x1', name: 'other', poolId: `9990000001_1_1_${sid}`, order: '9990000001', sku: 'OTHER' });
    st.put('Charm_Nest_Sheets', sid, { id: sid, metal: d.metal, sheetIndex: d.n, day: '2026-10-12', status: 'written', stock: { wPt: 300, hPt: 140 }, orders: [...new Set(d.charms.map(c => c.order))], placements: d.charms.map((c, i) => ({ id: c.id, cxPt: 40 + i * 40, cyPt: 60, angle: 0, wPt: 34, hPt: 34 })), charms: d.charms });
  }
  await post({ op: 'cancelPut', orderId: CANC.rid, by: 'Paul', why: 'Buyer asked to cancel', at: Date.now() - 30 * MIN, record: { buyer: { name: 'Cai Sample' }, sheets: 'GF Sheet 1, SS Sheet 1' } });

  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const outside = [], errors = [];
    const context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, timezoneId: 'America/Toronto' });
    const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      const u = r.request().url();
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/pdfmake/.test(u)) return r.fulfill(js(`window.pdfMake = { createPdf() { return { getBlob(cb) { cb(new Blob(['x'])); } }; } };`));
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/vfs_fonts/.test(u)) return r.fulfill(js(''));
      if (/qrcodejs/.test(u)) return r.fulfill(js(fs.readFileSync(path.join(root, 'lib/qrcode.min.js'))));
      if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      if (/etsy/i.test(u)) outside.push(u);
      return r.abort();
    });
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Paul'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Paul'; });
    const page = await context.newPage(); page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.Cancelled && window.OrderWin && window.OrderTimeline && CN.S.cloud.ok === true, null, { timeout: 60000 });
    const truth = Object.fromEntries(ALL.map(o => [o.rid, o.specs.map(s => ({ state: o === HELD ? 'hold' : o === HAND ? 'hand' : o === CANC ? 'gone' : 'pooled' }))]));
    await page.evaluate(async ({ orders, truth, known }) => {
      await Orders.loadMaps(true); await Cancelled.load(true);
      for (const sku of known) B.master.entries.set(sku, { sku, updatedAt: 1 });
      for (const o of orders) o.order.lines.forEach((line, li) => {
        const key = CharmNestOrders.lineKey(o.order, line), t = truth[o.order.receiptId][li].state, qty = line.quantity;
        const pools = []; for (let c = 1; c <= qty; c++) pools.push(`${key}_${c}`);
        const row = { key, order: o.order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: pools.slice(), engrave: null, material: null };
        if (t === 'hold') { row.state = 'held'; row.hold = row.reason = 'Taken off GF Sheet 1 by Paul: Add to next sheet, on hold'; row.heldAt = Date.now() - 3600e3; row.poolIds = []; }
        if (t === 'hand') { row.state = 'pulled'; row.poolIds = []; }
        if (t === 'gone') { row.state = 'gone'; row.poolIds = []; }
        B.orders.rows.push(row); B.orders.byKey.set(key, row);
      });
      Orders.interpretAll(); Review.syncOrderItems();
    }, { orders: ALL.map(o => ({ order: o.order })), truth, known: [COMEDY, DUCK] });
    await page.waitForTimeout(600);

    const idle = () => page.evaluate(() => window.Seal && Seal.whenIdle ? Seal.whenIdle() : null);
    const closeWin = async () => { await page.evaluate(() => OrderWin.isOpen() && OrderWin.close()); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 5000 }); };
    const openWin = async (o, piece) => {
      const cancelled = o === CANC, key = kOf(o, piece || 'c');
      if (cancelled) await page.evaluate(rid => OrderWin.openOrder(rid, {}), o.rid); else await page.evaluate(k => OrderWin.open(k), key);
      await page.waitForFunction(([k, cx]) => OrderWin.key() === k && OrderWin._feed() && OrderWin._feed().answer && (!cx || /Cancelled/.test(document.getElementById('owNowCard').dataset.now || '')) && !document.getElementById('owNowCard').hidden, [cancelled ? o.rid : key, cancelled], { timeout: 25000 });
      await page.mouse.move(3, 3); await page.waitForTimeout(1500); await idle();
    };
    const sized = async w => { await page.setViewportSize({ width: w, height: 1000 }); await page.mouse.move(3, 3); await page.waitForTimeout(700); await idle(); };
    // everything the card is, as the page draws it now
    const measure = () => page.evaluate(CONTROLS => {
      const c = document.getElementById('owNowCard'), r = c.getBoundingClientRect(), cs = getComputedStyle(c), t = c.querySelector('.t'), k = c.querySelector('.k'), tx = c.querySelector('.tx'), seal = c.querySelector('.tlNowSeal'), sub = c.querySelector('.sub');
      const rect = n => { if (!n) return null; const b = n.getBoundingClientRect(); return { x: b.left, y: b.top, r: b.right, b: b.bottom, w: b.width, h: b.height }; };
      const lh = t ? parseFloat(getComputedStyle(t).lineHeight) : 0;
      // the text's own line boxes (Range): where the first line ends
      let lines = []; if (t && t.firstChild) { const rg = document.createRange(); rg.selectNodeContents(t.firstChild); for (const q of rg.getClientRects()) { const at = lines.find(l => Math.abs(l.top - q.top) < 3); if (at) { at.right = Math.max(at.right, q.right); at.left = Math.min(at.left, q.left); } else lines.push({ top: q.top, left: q.left, right: q.right }); } }
      const models = [...c.querySelectorAll('svg[data-seal-model]')].map(s => { const j = JSON.parse(s.getAttribute('data-seal-model')); return `${j.action} ${j.date} ${j.time}`; });
      return { h: r.height, w: r.width, rect: rect(c), hidden: c.hidden, cls: c.className, k: k && k.textContent, kRect: rect(k), t: t && t.textContent, tTitle: t && t.getAttribute('title'), tRect: rect(t), tFont: t && parseFloat(getComputedStyle(t).fontSize), tLH: lh, tScrollH: t && t.scrollHeight, tClientH: t && t.clientHeight, tLines: lines.length, line1Right: lines[0] ? lines[0].right : null, txRect: rect(tx),
        seal: rect(seal), sealW: seal ? seal.offsetWidth : null, sealModels: models, cols: cs.gridTemplateColumns.split(' ').length, colsText: cs.gridTemplateColumns, padR: parseFloat(cs.paddingRight), padL: parseFloat(cs.paddingLeft), border: parseFloat(cs.borderTopWidth),
        sub: sub ? { text: sub.textContent.replace(/\s+/g, ' ').trim(), h: sub.getBoundingClientRect().height, scrollW: sub.scrollWidth, clientW: sub.clientWidth, kids: [...sub.children].map(x => x.className), titles: [...sub.querySelectorAll('[title]')].map(x => x.title) } : null,
        controls: [...c.querySelectorAll(CONTROLS)].map(x => x.tagName + '.' + x.className), focusable: [...c.querySelectorAll('[tabindex]')].map(x => x.className),
        scrollW: c.scrollWidth, clientW: c.clientWidth, docW: document.documentElement.scrollWidth, innerW: innerWidth, text: c.textContent.replace(/\s+/g, ' ').trim() };
    }, CONTROLS);
    const SEALS = { [PAUL.rid]: 'ON SHEET 12 OCT 2026 10:31 AM', [LONG.rid]: 'ON SHEET 12 OCT 2026 10:31 AM', [CANC.rid]: /^CANCELLED 12 OCT 2026 /, [HELD.rid]: /^ON HOLD 12 OCT 2026 /, [HAND.rid]: /^ORDER COMPLETE 12 OCT 2026 /, [WAIT.rid]: /^ORDER RECEIVED 12 OCT 2026 8:00 AM/, [ONS.rid]: /^ON SHEET 12 OCT 2026 /, [TWO.rid]: /^ON SHEET 12 OCT 2026 / };
    const problems = (o, m, w) => {
      const out = [], tag = `${o.rid} at ${w}`;
      if (m.hidden) return [`${tag}: the card is hidden`];
      // 1 · nothing to press, nothing to click through to a sheet
      if (m.controls.length) out.push(`${tag}: the card holds ${m.controls.length} control(s): ${m.controls.join(', ')}`);
      if (/Open timeline|Not on a sheet yet|Completed by hand/.test(m.text)) out.push(`${tag}: the old buttons' or shortcuts' words are still on the card: ${m.text}`);
      // 2 · one height, 67% of Paul's picture within 2 px, a third shorter than main wherever main was measured
      if (Math.abs(m.h - NEW_H) > 0.5) out.push(`${tag}: the card is ${m.h.toFixed(2)} px tall, not ${NEW_H}`);
      if (Math.abs(m.h - TARGET) > 2) out.push(`${tag}: ${m.h.toFixed(2)} px is not 67% of ${OLD_PICTURE} (${TARGET.toFixed(2)}) within 2 px`);
      if (OLD_MAIN[w] && m.h > 0.7 * OLD_MAIN[w] + 0.001) out.push(`${tag}: ${m.h} px is not a third shorter than main's ${OLD_MAIN[w]}`);
      // 3 · the seal and the title came down together (50 to 44 px, 19 to 17 px) and are still there; the label stays
      if (!(m.sealW >= 40 && m.sealW <= 46)) out.push(`${tag}: the seal is ${m.sealW} px wide (it was 50, it is 44)`);
      if (m.tFont !== 17) out.push(`${tag}: the title is ${m.tFont} px (it was 19, it is 17)`);
      if (!m.k || !m.k.trim()) out.push(`${tag}: the label is gone`);
      const want = SEALS[o.rid];
      if (want) { const got = m.sealModels.join(' | '); if (typeof want === 'string' ? got !== want : !want.test(got)) out.push(`${tag}: the seal says "${got}", wanted ${want}`); }
      if (m.sealModels.length !== 1) out.push(`${tag}: ${m.sealModels.length} seals on the card`);
      if (m.seal && (m.seal.y < m.rect.y || m.seal.b > m.rect.b)) { /* (the seal is turned a little: its corners may lean out by a pixel or two, never more) */ if (m.seal.y < m.rect.y - 3 || m.seal.b > m.rect.b + 3) out.push(`${tag}: the seal leaves the card (${m.seal.y.toFixed(1)}..${m.seal.b.toFixed(1)} in ${m.rect.y.toFixed(1)}..${m.rect.b.toFixed(1)})`); }
      // 4 · the title runs the whole width: its box ends at the card's inner right edge and begins right after the seal: no column is kept for anything else
      const innerR = m.rect.r - m.border - m.padR;
      if (Math.abs(m.tRect.r - innerR) > 1) out.push(`${tag}: the title ends at ${m.tRect.r.toFixed(1)}, the card's inner right edge is ${innerR.toFixed(1)}`);
      if (m.cols !== (m.sealModels.length ? 2 : 1)) out.push(`${tag}: ${m.cols} grid columns (${m.colsText}): a seal and the text, nothing else`);
      const textLeft = m.rect.x + m.border + m.padL + (m.sealModels.length ? m.sealW + 16 : 0);
      if (Math.abs(m.tRect.x - textLeft) > 1) out.push(`${tag}: the title starts at ${m.tRect.x.toFixed(1)}, right after the seal is ${textLeft.toFixed(1)}`);
      // 5 · wraps naturally, at most two lines, the full text in the title attribute
      // (tLines counts the lines the text makes; the box shows two of them at most and cuts the rest)
      if (m.tClientH > 2 * m.tLH + 1) out.push(`${tag}: the title shows ${(m.tClientH / m.tLH).toFixed(1)} lines (${m.tClientH} px)`);
      if (m.tLines > 2 && !(m.tScrollH > m.tClientH + 1)) out.push(`${tag}: the text makes ${m.tLines} lines and the box neither shows them nor cuts them`);
      if (m.tTitle !== m.t) out.push(`${tag}: the title attribute is not the whole title: ${JSON.stringify(m.tTitle)} vs ${JSON.stringify(m.t)}`);
      if (m.tLines >= 2 && m.line1Right < innerR - 160) out.push(`${tag}: the first of two lines stops at ${m.line1Right.toFixed(0)}, far short of the right edge ${innerR.toFixed(0)}: the text does not use the width`);
      // 6 · nothing spills: no sideways scroll, the card holds its content
      if (m.docW > m.innerW + 1) out.push(`${tag}: the page scrolls sideways (${m.docW} > ${m.innerW})`);
      if (m.scrollW > m.clientW + 1) out.push(`${tag}: the card's content is wider than the card (${m.scrollW} > ${m.clientW})`);
      return out;
    };
    const shot = async name => { if (shots) { await page.evaluate(() => document.getElementById('owNowCard').scrollIntoView({ block: 'start' })); await page.mouse.move(3, 3); await page.waitForTimeout(400); const el = await page.$('#owNowCard'); await el.screenshot({ path: path.join(shots, 'TITLEBAR-after-' + name + '.png') }); }; };
    const shotWin = async name => { if (shots) { await page.mouse.move(3, 3); await page.waitForTimeout(400); await page.screenshot({ path: path.join(shots, 'TITLEBAR-after-' + name + '.png') }); } };
    const results = {};

    // ── 1 · every state at 1440 ──
    for (const o of ALL) {
      await openWin(o);
      const m = await measure(); results[o.rid] = m;
      const p = problems(o, m, 1440); assert.deepEqual(p, [], p.join(' | '));
      if (o === PAUL) { assert.equal(m.k, 'Needs a decision'); assert.equal(m.t, 'Unknown SKU: SKU MASKS HAPPY/SAD: not in any master file'); await shot('paul-state-1440'); await shotWin('paul-window-1440'); }
      if (o === CANC) {
        // the steps of a cancelled order are one quiet line, cut at the edge, the whole story in its title attribute and on the Timeline
        assert(m.sub && /Taken out of the queue/.test(m.sub.text), 'the cancelled order\'s steps are said on the card: ' + JSON.stringify(m.sub));
        assert(m.sub.h <= 20, 'in one line: ' + m.sub.h); assert(m.sub.titles.some(t => /Taken out of the queue/.test(t) && /Cancelled by Paul/.test(t)), 'the whole story is its title: ' + JSON.stringify(m.sub.titles));
        assert.equal(m.tLines, 1, 'a one-line title above the line'); await shot('cancelled-1440');
      }
      if (o === HELD) await shot('held-1440');
      if (o === ONS) await shot('on-sheet-1440');
      if (o === HAND) await shot('completed-1440');
      if (o === WAIT) await shot('order-in-1440');
      await closeWin();
    }
    console.log('  ok  order-titlebar: all eight states at 1440: no control, 79 px, the seal and the title come down, the title runs to the right edge');

    // ── 2 · the width sweep: Paul's state, the long reason and a cancelled order at every common window width ──
    const sweep = {};
    for (const o of [PAUL, LONG, CANC]) {
      await openWin(o);
      for (const w of WIDTHS) {
        await sized(w);
        const m = await measure(); (sweep[o.rid] = sweep[o.rid] || {})[w] = m.h;
        const p = problems(o, m, w); assert.deepEqual(p, [], p.join(' | '));
        if (o === PAUL && [1920, 1280, 900, 390].includes(w)) await shot('paul-state-' + w);
        if (o === LONG && w === 390) {
          // the long reason is cut on its second line, the whole of it on hover / title
          assert.equal(Math.round(m.tClientH / m.tLH), 2, 'two lines shown at 390'); assert(m.tLines > 2, 'of three'); assert(m.tScrollH > m.tClientH + 1, 'cut on the second line: ' + m.tScrollH + ' in ' + m.tClientH); assert.equal(m.tTitle, m.t);
          assert(/^Unknown SKU: SKU EXTRA LONG DESIGN NAME FOR A VERY LONG TITLE: not in any master file$/.test(m.tTitle), 'the full text is the title: ' + m.tTitle);
          await shot('long-title-390');
        }
      }
      await sized(1440); await closeWin();
    }
    const heights = new Set(Object.values(sweep).flatMap(x => Object.values(x).map(h => Math.round(h * 100) / 100)));
    assert.deepEqual([...heights], [NEW_H], 'one height at every width in every state: ' + JSON.stringify(sweep));
    // the width ratios against main, as measured there with the same fixtures
    for (const [w, old] of Object.entries(OLD_MAIN)) assert(sweep[PAUL.rid][w] / old <= 0.7 && sweep[PAUL.rid][w] / old >= 0.6, `at ${w}: ${sweep[PAUL.rid][w]} against main's ${old}`);
    console.log('  ok  order-titlebar: ' + WIDTHS.join(', ') + ' px, three states each: always ' + NEW_H + ' px (' + (100 * NEW_H / OLD_PICTURE).toFixed(1) + '% of Paul\'s ' + OLD_PICTURE + ' px; ' + Object.entries(OLD_MAIN).map(([w, old]) => `${(100 * NEW_H / old).toFixed(1)}% of main's ${old} at ${w}`).join(', ') + ')');

    // ── 3 · a split pair's sentence, and who did it when no seal says it: one quiet line under a ONE-line title, the height the same ──
    await openWin(ONS);
    const stub = () => page.evaluate(({ key, rid }) => {
      window.__tb = { spread: OrderPieces.spread, of: OrderPieces.of, nowStamps: OrderTimelineUI.nowStamps };
      const mine = id => String(id) === rid && /splitLine/.test(new Error().stack);   // (only the card's own split line asks this way; the rows and the sheet scope read the real pieces)
      OrderPieces.spread = id => mine(id) ? { splitGroups: [{ lineKey: key, sides: ['L', 'R'] }] } : window.__tb.spread(id);
      OrderPieces.of = id => mine(id) ? [{ lineKey: key, side: 'L', nested: true, sheetLabel: 'GF Sheet 1', key: key + '#L', poolId: key + '_1' }, { lineKey: key, side: 'R', nested: false, sheetLabel: '', key: key + '#R', poolId: key + '_2' }] : window.__tb.of(id);
      OrderWin.paint();
    }, { key: kOf(ONS, 'c'), rid: ONS.rid });
    await stub(); await page.waitForTimeout(900);
    let m = await measure(); results.split = m;
    assert(m.sub && /Its pair/.test(m.sub.text) && /left earring on GF Sheet 1/.test(m.sub.text) && /right earring on no sheet|not on a sheet/i.test(m.sub.text), 'the split pair is said under the title: ' + JSON.stringify(m.sub));
    assert.equal(m.tLines, 1, 'a one-line title above it'); assert.deepEqual(problems(ONS, m, 1440), [], 'the split state keeps the rules'); assert(m.sub.titles.length === 1 && /Its pair/.test(m.sub.titles[0]), 'its words are its title: ' + JSON.stringify(m.sub.titles));
    await shot('split-pair-1440');
    await sized(390); m = await measure(); assert.deepEqual(problems(ONS, m, 390), [], 'the split state at 390'); await shot('split-pair-390'); await sized(1440);
    // no seal: who did it (the pill) takes that line
    await page.evaluate(() => { OrderTimelineUI.nowStamps = () => ({ seal: '', recent: '', blocker: null }); OrderWin.paint(); }); await page.waitForTimeout(900);
    m = await measure(); results.noSeal = m;
    assert.equal(m.sealModels.length, 0, 'no seal on the card'); assert(m.sub && m.sub.kids.some(k => /\bwho\b/.test(k)), 'who did it is said in the line: ' + JSON.stringify(m.sub));
    assert.equal(m.h, NEW_H, 'the same height without a seal: ' + m.h); assert.equal(m.cols, 1, 'one column without a seal'); assert.deepEqual(m.controls, [], 'no control'); assert(Math.abs(m.tRect.r - (m.rect.r - m.border - m.padR)) <= 1, 'the title still runs to the right edge');
    assert(Math.abs(m.tRect.x - (m.rect.x + m.border + m.padL)) <= 1, 'and starts at the left edge when there is no seal');
    await shot('no-seal-1440');
    await page.evaluate(() => { OrderPieces.spread = window.__tb.spread; OrderPieces.of = window.__tb.of; OrderTimelineUI.nowStamps = window.__tb.nowStamps; OrderWin.paint(); }); await page.waitForTimeout(900);
    m = await measure(); assert.equal(m.sealModels.length, 1, 'the seal is back'); assert.equal(m.sub, null, 'and the quiet line is gone'); assert.deepEqual(problems(ONS, m, 1440), []);
    await closeWin();
    console.log('  ok  order-titlebar: a split pair\'s sentence and a card with no seal: one quiet line, the same 79 px');

    // ── 4 · the checker itself: a button put back, a shortcut pill put back, the old height put back, the title narrowed again are all caught ──
    await openWin(PAUL);
    const base = await measure(); assert.deepEqual(problems(PAUL, base, 1440), [], 'the real card passes');
    const mutate = async fn => { await page.evaluate(fn); await page.waitForTimeout(150); const x = await measure(); await page.evaluate(() => { const c = document.getElementById('owNowCard'); c.querySelectorAll('.__mut').forEach(n => n.remove()); c.style.cssText = ''; c.querySelector('.tx').style.cssText = ''; }); return problems(PAUL, x, 1440); };
    let caught = await mutate(() => { const b = document.createElement('button'); b.className = '__mut btn ghost xs'; b.textContent = 'Open timeline'; document.querySelector('#owNowCard .tx').appendChild(b); });
    assert(caught.some(x => /control/.test(x)) && caught.some(x => /Open timeline/.test(x)), 'a put-back Open timeline button is caught: ' + caught.join(' | '));
    caught = await mutate(() => { const b = document.createElement('div'); b.className = '__mut shs'; b.innerHTML = '<button class="owShChip">GF Sheet 1 COMEDY 89578</button>'; document.getElementById('owNowCard').appendChild(b); });
    assert(caught.some(x => /control/.test(x)), 'a put-back sheet shortcut is caught: ' + caught.join(' | '));
    caught = await mutate(() => { document.getElementById('owNowCard').style.height = '118px'; });
    assert(caught.some(x => /px tall/.test(x)) && caught.some(x => /67%/.test(x)), 'the old height put back is caught: ' + caught.join(' | '));
    caught = await mutate(() => { document.querySelector('#owNowCard .tx').style.marginRight = '300px'; });
    assert(caught.some(x => /the title ends at/.test(x)), 'a title that stops short of the right edge is caught: ' + caught.join(' | '));
    caught = await mutate(() => { const c = document.getElementById('owNowCard'); c.style.gridTemplateColumns = '44px minmax(0,1fr) 310px'; });
    assert(caught.some(x => /grid columns|inner right edge/.test(x)), 'a column kept for shortcuts on the right is caught: ' + caught.join(' | '));
    await closeWin();
    console.log('  ok  order-titlebar: the checks catch a put-back button, shortcut pill, taller card and narrowed title');

    // ── 5 · nothing became unreachable ──
    // the Timeline and Sheet tabs do what the two buttons did; a piece's chip opens its sheet; the seal opens the Timeline
    await openWin(PAUL, 'm');
    await page.click('.owTabsV [data-ow-view="timeline"]'); await page.waitForFunction(() => OrderWin.view() === 'timeline');
    await page.click('.owTabsV [data-ow-view="info"]'); await page.waitForFunction(() => OrderWin.view() === 'info'); await page.waitForTimeout(700);
    await page.click('#owNowCard .tlNowSeal'); await page.waitForFunction(() => OrderWin.view() === 'timeline', null, { timeout: 10000 });
    await page.evaluate(() => OrderWin.setView('info')); await page.waitForTimeout(700);
    const sheetTab = await page.evaluate(() => { const t = document.querySelector('.owTabsV [data-ow-view="sheet"]'); return { off: t.getAttribute('aria-disabled') === 'true', why: t.dataset.why || '' }; });
    assert.equal(sheetTab.off, true, 'Paul\'s order opened on the MASKS piece (no sheet): the Sheet tab is greyed with the reason the old button gave: ' + JSON.stringify(sheetTab)); assert(/not on a sheet yet/i.test(sheetTab.why), sheetTab.why);
    await closeWin();
    await openWin(ONS);
    const chips = await page.evaluate(() => [...document.querySelectorAll('#owPcSum button[data-pc-sheet]')].map(b => ({ text: b.textContent.trim(), sheet: b.dataset.sheet, label: b.getAttribute('aria-label') })));
    assert.equal(chips.length, 2, 'each piece on a sheet has its chip, a button that opens its sheet: ' + JSON.stringify(chips)); assert(chips.every(c => /Open (GF|SS) Sheet 1/.test(c.label)), JSON.stringify(chips));
    await page.click('#owPcSum button[data-pc-sheet]'); await page.waitForFunction(() => OrderWin.view() === 'sheet' && OrderWin._sheet(), null, { timeout: 20000 });
    await page.evaluate(() => OrderWin.setView('info')); await page.waitForTimeout(500);
    await page.click('.owTabsV [data-ow-view="sheet"]'); await page.waitForFunction(() => OrderWin.view() === 'sheet' && OrderWin._sheet(), null, { timeout: 20000 });
    await closeWin();
    // a piece that is on TWO sheets: the old card gave a shortcut for each; the Sheet view lists both
    await openWin(TWO);
    await page.evaluate(k => OrderWin.selectPiece(k), kOf(TWO, 'c')); await page.waitForTimeout(700);
    await page.click('.owTabsV [data-ow-view="sheet"]'); await page.waitForFunction(() => OrderWin.view() === 'sheet' && OrderWin._sheet() && document.getElementById('owPlateWait').hidden, null, { timeout: 20000 });
    const tabs = await page.evaluate(() => [...document.querySelectorAll('#owSheetPanel .owShTabs button')].map(b => b.textContent.trim()));
    assert.deepEqual(tabs.sort(), ['GF Sheet 1', 'GF Sheet 2'], 'both sheets of the piece are in the Sheet view: ' + JSON.stringify(tabs));
    await page.evaluate(() => OrderWin.setView('info')); await page.waitForTimeout(700);
    const mt = await measure(); assert.deepEqual(problems(TWO, mt, 1440), [], 'the card of an order with a piece picked keeps the rules');
    await closeWin();
    console.log('  ok  order-titlebar: nothing is unreachable (Timeline tab, Sheet tab with its reason, the seal, the pieces\' chips, both sheets of a piece on two)');

    assert.deepEqual(outside, [], 'no Etsy call'); assert.deepEqual(errors, [], 'no page error');
    console.log('  ok  order-titlebar');
  } finally { await browser.close(); srv.close(); }
}
main().catch(e => { console.error(e); process.exit(1); });
