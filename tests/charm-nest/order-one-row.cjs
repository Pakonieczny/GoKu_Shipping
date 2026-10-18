// ONE ROW PER ORDER ITEM in the order window's "Its pieces" list (Paul, 12 Oct 2026, image 1):
//  "Each order should only take up one line and you need to amalgamate multi charm orders, including all earring orders, so that in the UI it is all
//   showing under one line and not split up ... Be clever with your UI design, most importantly stay minimalist ... hide options that are not
//   immediately necessary behind hover states or drop-down menus or other clever UI solutions."
// Fixture (order-rows-fixture.cjs, fake names, an in-memory store, no Etsy, no paid call): a single charm, a matching pair, a mismatched pair (the Right waiting),
// the screenshot's order (a necklace on a draft and a pair in Review: Print QR label / Complete Order), a 3-disc necklace, a set of four charms, a pair on hold
// and a pair completed by hand.
// Proves, in headless Chromium over the page's own code:
//  - ONE row for an earring pair (matching or mismatched), a necklace of discs and a set: the name once, a Left | Right marker on a pair, the metal dot, ONE status, the
//    six dots; a pair or a set is no taller than an ordinary row; no row says Left or Right on its own
//  - the row speaks for the SLOWEST piece (status and dots); hovering the dots or the status opens ONE card that lists each piece's own step and seal
//  - the chevron opens a drawer with one line per piece (Left, Right, Disc 2 of 3 ...): its own status (a button that opens its sheet), its own six dots on an ear;
//    it is remembered while the list is drawn again, and Right / Left on the name open and close it
//  - the collapsed row has at most ONE primary action (Print QR label, else Complete Order, else Hold); the rest (Hold, Complete Order, Print again ...) sit behind the
//    three dots, shown on hover, on focus and on a touch screen; Esc, a press elsewhere and a choice close it; nothing became unreachable: each action still works from there
//  - the rows are the piece selector: a press on the row picks the item, the Left | Right marker picks that ear (the window narrows to it), a press again shows all
//  - a single-piece order is drawn exactly as before (its buttons on its row, no chevron, no menu); widths 1440, 900 and 390
//   node tests/charm-nest/order-one-row.cjs   (PW_DIR=<playwright-core's node_modules>, CHROMIUM=<chrome>, SHOTS=<dir>)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const fx = require('./order-rows-fixture.cjs');
const Rows = require(path.join(root, 'charm-nest-order-rows.js'));
const PS = require(path.join(root, 'charm-nest-piece-switch.js'));
const { O } = fx;
let ran = 0; const ok = m => { ran++; console.log('  ok ' + m); };

// ── A · the rules that need no page ──
function partA() {
  assert.equal(Rows.kindOf({ sides: [{}, {}] }), 'pair'); assert.equal(Rows.kindOf({ counted: true }), 'counted'); assert.equal(Rows.kindOf({ sides: [{}] }), 'single'); assert.equal(Rows.kindOf({}), 'single'); assert.equal(Rows.kindOf(null), 'single');
  ok('A1 a line with a Left and a Right is a pair, a counted line is counted, every other line is single');
  assert.equal(Rows.slowest([{ step: 3 }, { step: 1 }]), 1); assert.equal(Rows.slowest([{ step: 2 }, { step: 2 }]), 0, 'a tie: the first'); assert.equal(Rows.slowest([{ step: 5, hand: true }, { step: 1 }]), 1);
  assert.equal(Rows.slowest([{ step: 0, hand: true }, { step: 4 }]), 1, 'a piece completed by hand waits on no step'); assert.equal(Rows.slowest([{ step: 0, hand: true }, { step: 4, hand: true }]), 0); assert.equal(Rows.slowest([]), -1);
  ok('A2 the row speaks for the slowest piece: the lowest step, a tie the first, a piece completed by hand never unless all are');
  assert.equal(Rows.subLabel('pair', 0, 2, { side: 'L' }), 'Left'); assert.equal(Rows.subLabel('pair', 1, 2, { side: 'R' }), 'Right'); assert.equal(Rows.subLabel('pair', 0, 2, { side: 'L', qty: 2 }), 'Left ×2');
  assert.equal(Rows.subLabel('counted', 1, 3, { noun: 'disc' }), 'Disc 2 of 3'); assert.equal(Rows.subLabel('counted', 3, 4, {}), 'Piece 4 of 4'); assert.equal(Rows.countWord(3, 'disc'), '3 discs'); assert.equal(Rows.countWord(1), '1 piece'); assert.equal(Rows.countWord(4), '4 pieces');
  ok('A3 the drawer\'s words: Left, Right, Disc 2 of 3, Piece 4 of 4; and the counts');
  // the Left | Right marker is the Engrave tab's own switch (charm-nest-piece-switch.js) drawn compact: the same classes and hooks, a tag each, no words or stage
  const pcs = PS.list([{ key: 'a#L', slot: 'L' }, { key: 'a#R', slot: 'R' }], { of: 2 }), full = PS.html(pcs, 'a#L', {}), compact = PS.html(pcs, 'a#R', { compact: true, titleOf: (p, on) => `${on ? 'Showing' : 'Show'} ${p.label}` });
  assert.match(compact, /class="egEarSwitch"/); assert.equal((compact.match(/class="egEarTab egCompact"/g) || []).length, 2); assert.equal((compact.match(/data-a="ear"/g) || []).length, 2);
  assert.match(compact, /data-ear="a#L" aria-pressed="false" title="Show Left"/); assert.match(compact, /data-ear="a#R" aria-pressed="true" title="Showing Right"/); assert.doesNotMatch(compact, /egEarWords|egEarStage/, 'no words, no stage');
  assert.match(full, /egEarWords/, 'the full switch of the Engrave tab is as it was'); assert.doesNotMatch(full, /egCompact/);
  ok('A4 the marker is the Engrave tab\'s own switch drawn compact (same classes and data hooks, a tag each); the full switch is as it was');
}

const PDFMAKE = `window.pdfMake = { createPdf(dd) { const top = window.parent; (top.__qrDocs = top.__qrDocs || []).push(JSON.parse(JSON.stringify(dd)));
  return { getBlob(cb) { cb(new Blob(['<!doctype html><title>label</title><script>window.print = () => { window.parent.parent.__printed = (window.parent.parent.__printed || 0) + 1; };<\\/script>'], { type: 'text/html' })); } }; } };`;

async function partB() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); return; }
  const shots = process.env.SHOTS || null; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] });
  fx.seed(srv);
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const errors = [], outside = [];
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      const u = r.request().url();
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/pdfmake/.test(u)) return r.fulfill(js(PDFMAKE));
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/vfs_fonts/.test(u)) return r.fulfill(js(''));
      if (/qrcodejs/.test(u)) return r.fulfill(js(fs.readFileSync(path.join(root, 'lib/qrcode.min.js'))));
      if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      outside.push(u); return r.abort();
    });
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: 'off' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    await context.addInitScript(() => { new MutationObserver(() => { const b = document.querySelector('dialog.pieceDlg[open] [data-k=go]'); if (b && !b.disabled) b.click(); }).observe(document, { childList: true, subtree: true, attributes: true, attributeFilter: ['open'] }); });   // (Paul, 14 Oct 2026: a piece of a multi-piece order asks what its sticker is for; this test is not about that question, so the offered note is accepted: tests/charm-nest/qr-piece-label.cjs)
    const page = await context.newPage(); page.setDefaultTimeout(75000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.OrderPieces && window.Review && window.CustomPrint && window.CharmNestOrderRows && window.CharmNestPieceSwitch && window.RailTip && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await fx.load(page);

    const shot = async name => { if (shots) { await page.waitForTimeout(500); await page.locator('#owPcSum').screenshot({ path: path.join(shots, name + '.png') }); } };
    const open = async (o, rows) => {
      if (await page.evaluate(() => document.getElementById('orderWin').open)) { await page.evaluate(() => OrderWin.close()); await page.waitForFunction(() => !document.getElementById('orderWin').open); }
      await page.mouse.move(2, 2);
      await page.evaluate(k => OrderWin.open(k, { view: 'info' }), fx.kOf(o));
      await page.waitForFunction(r => OrderPieces.known(r), o.rid, { timeout: 60000 }).catch(() => {});
      if (rows) await page.waitForFunction(n => document.querySelectorAll('#owPcSum > .owPcRow').length === n && !!document.querySelector('#owPcSum > .owPcMulti'), rows, { timeout: 60000 });
      await page.waitForTimeout(1100);
    };
    // each row of "Its pieces", as drawn (the drawer's lines are read apart)
    const read = () => page.evaluate(() => {
      const txt = n => n ? n.textContent.replace(/\s+/g, ' ').trim() : '';
      const vis = n => !!n && n.offsetParent !== null && getComputedStyle(n).visibility !== 'hidden';
      return [...document.querySelectorAll('#owPcSum > .owPcRow')].map(r => {
        const act = r.querySelector('.pcAct'), menu = r.querySelector('.owPcMenu');
        const prim = act ? [...act.children].filter(c => !c.matches('.owPcMenuWrap') && vis(c)) : [];
        const drawer = r.nextElementSibling && r.nextElementSibling.matches('.owPcDrawer') ? r.nextElementSibling : null;
        return { key: r.dataset.piece || r.dataset.item || '', multi: r.classList.contains('owPcMulti'), kind: r.classList.contains('owPc-pair') ? 'pair' : r.classList.contains('owPc-counted') ? 'counted' : 'single', h: Math.round(r.getBoundingClientRect().height * 10) / 10, w: r.getBoundingClientRect().width,
          name: txt(r.querySelector('.owPcName')), nameCount: (txt(r).match(new RegExp(txt(r.querySelector('.owPcName b')).replace(/[.*+?^${}()|[\]\\/]/g, '\\$&'), 'g')) || []).length,
          marker: [...r.querySelectorAll('.owPcWho .egEarTab')].map(b => ({ tag: txt(b), ear: b.dataset.ear, on: b.getAttribute('aria-pressed') })),
          status: [...r.querySelectorAll(':scope > .st .pcSt')].filter(vis).map(txt), chips: r.querySelectorAll(':scope > .st .pcSt').length, hint: txt(r.querySelector(':scope > .st .pcMore')),
          dots: r.querySelectorAll(':scope > .steps > i').length, dotsOn: r.querySelectorAll(':scope > .steps > i.on').length,
          prim: prim.map(c => txt(c)).filter(Boolean), primBtns: prim.filter(c => c.matches('button')).map(c => txt(c)), menuBtn: !!r.querySelector('.owPcMoreBtn'), menuItems: menu ? [...menu.querySelectorAll('button')].map(b => txt(b)) : [],
          chev: r.querySelector('.owPcX') ? { open: r.querySelector('.owPcX').getAttribute('aria-expanded') } : null, sel: r.classList.contains('sel'), done: r.classList.contains('isDone'), hold: r.querySelectorAll('[data-hold-btn]').length,
          drawer: drawer ? { hidden: drawer.hidden, subs: [...drawer.querySelectorAll('.owPcSub')].map(s => ({ label: txt(s.querySelector('.owPcSubName')), status: txt(s.querySelector('.pcSt')), dots: s.querySelectorAll('.steps > i').length, sel: s.classList.contains('sel'), btn: !!s.querySelector('button.pcSt') })) } : null };
      });
    });
    const state = () => page.evaluate(() => ({ side: OrderWin.selectedSide(), piece: OrderWin.selectedPiece(), head: (document.querySelector('#owPcSum .owPcState') || { textContent: '' }).textContent.replace(/\s+/g, ' ').trim() }));
    const tip = () => page.evaluate(() => { const t = document.querySelector('.railTip[data-on]'); if (!t) return null; return { name: t.querySelector('.rtHead b')?.textContent, pieces: [...t.querySelectorAll('.rtPc')].map(p => ({ label: p.querySelector('.rtPcHd b')?.textContent, state: p.querySelector('.rtPcHd .rtState')?.textContent || '', line: p.querySelector('.rtLine')?.textContent || '', seal: !!p.querySelector('.rtSeal svg, .rtSeal .seal, .rtSeal *') })), many: t.classList.contains('rtMany') }; });
    const away = async () => { await page.mouse.move(2, 2); await page.waitForTimeout(260); };

    // ── 1 · the screenshot's order: a necklace and a pair in Review ──
    await open(O.SHOT, 2);
    let R = await read();
    assert.equal(R.length, 2, 'two rows for three pieces: ' + JSON.stringify(R.map(r => r.name)));
    const [neck, pair] = R;
    assert.equal(neck.multi, false); assert.equal(pair.multi, true); assert.equal(pair.kind, 'pair'); assert.equal(pair.nameCount, 1, 'the name once: ' + pair.name); assert.match(pair.name, /^MASKS HAPPY\/SAD/);
    assert.deepEqual(pair.marker.map(m => m.tag), ['Left', 'Right'], 'a Left | Right marker, the Engrave tab\'s own switch'); assert.equal(pair.marker.every(m => m.on === 'false'), true);
    assert.equal(pair.dots, 6); assert.equal(pair.chips, 1, 'ONE status'); assert.equal(pair.status.length, 1); assert.match(pair.status[0], /In review/);
    assert.equal(await page.evaluate(() => document.querySelectorAll('#owPcSum > .owPcRow[data-side], #owPcSum > .owPcRow .owPcSide').length), 0, 'no row of its own for Left or for Right, no LEFT tag on a row');
    assert.equal(await page.evaluate(() => !!document.querySelector('#owPcSum .owPcHd .owPcState') && document.querySelector('#owPcSum .owPcState').textContent.trim()), 'Showing all 3 pieces', 'the quiet line still counts pieces');
    assert(pair.h <= neck.h + 1, `the pair is no taller than an ordinary row: ${pair.h} vs ${neck.h}`); assert(pair.h <= 40, 'and not tall: ' + pair.h);
    assert.deepEqual(pair.prim, ['Print QR label'], 'ONE primary action: ' + JSON.stringify(pair.prim)); assert.equal(pair.menuBtn, true); assert.deepEqual(pair.menuItems, ['Release Hold', 'Hold'], 'the rest is behind the three dots (RELHOLD, Paul 18 Oct 2026: a Review piece\'s Release Hold where Complete Order stood)');
    assert.equal(pair.hold, 1, 'one Hold, in the menu'); assert.deepEqual(pair.chev, { open: 'false' });
    assert.equal(pair.drawer.hidden, true); assert.deepEqual(pair.drawer.subs.map(s => s.label), ['Left', 'Right']);
    assert.equal(neck.hold, 1, 'an ordinary row keeps its own Hold, as before'); assert.equal(neck.menuBtn, false, 'and has no menu, no chevron'); assert.equal(neck.chev, null);
    ok('1 the screenshot\'s order: three pieces on TWO rows; the pair is one row (name once, Left | Right, one status, six dots, a chevron) no taller than the necklace\'s; one primary action (Print QR label), Release Hold and Hold behind the three dots');
    await shot('1-collapsed');

    // the dots and the status hover on the SLOWEST piece and open the same card that lists each piece
    await page.hover('#owPcSum > .owPcMulti .steps i:nth-child(3)'); await page.waitForFunction(() => document.querySelector('.railTip[data-on]'), null, { timeout: 15000 });
    let t = await tip();
    assert.equal(t.many, true); assert.deepEqual(t.pieces.map(p => p.label), ['Left', 'Right'], JSON.stringify(t)); assert.equal(t.name, 'Laser cut'); assert.deepEqual(t.pieces.map(p => p.state), ['', '']);
    ok('1a the dots\' card (after the rest) names each piece\'s own state: Left, Right'); await shot('1a-dots-hover'); await away();
    await page.hover('#owPcSum > .owPcMulti :is(.st [data-pcard])'); await page.waitForFunction(() => document.querySelector('.railTip[data-on]'), null, { timeout: 15000 });
    t = await tip(); assert.equal(t.many, true); assert.deepEqual(t.pieces.map(p => p.label), ['Left', 'Right']); ok('1b the status chip opens the same card'); await away();
    // the Order in dot of a pair: each ear's own seal on the card
    await page.hover('#owPcSum > .owPcMulti .steps i:nth-child(1)'); await page.waitForFunction(() => document.querySelector('.railTip[data-on]'), null, { timeout: 15000 });
    t = await tip(); assert.equal(t.name, 'Order in'); assert.deepEqual(t.pieces.map(p => [p.label, p.state, p.seal]), [['Left', 'Done', true], ['Right', 'Done', true]], 'each piece shows its own seal, none invented: ' + JSON.stringify(t)); ok('1c a done step shows each piece\'s own seal on the card'); await away();
    // by the keyboard: a dot of the row has the focus: the card is there, listing both pieces (Esc puts it away)
    await page.evaluate(() => document.querySelector('#owPcSum > .owPcMulti .steps i[data-pdot]').focus({ preventScroll: true })); await page.waitForFunction(() => document.querySelector('.railTip[data-on] .rtPc'), null, { timeout: 15000 });
    t = await tip(); assert.deepEqual(t.pieces.map(p => p.label), ['Left', 'Right'], 'the keyboard opens the same card: ' + JSON.stringify(t)); await page.keyboard.press('Escape'); await page.waitForFunction(() => !document.querySelector('.railTip[data-on]'), null, { timeout: 10000 });
    assert.equal(await page.evaluate(() => document.getElementById('orderWin').open), true, 'Esc put the card away, not the window'); await page.evaluate(() => document.activeElement.blur()); await away();
    ok('1c2 a dot of the row, reached by the keyboard, opens the card listing both pieces; Esc puts it away');
    // the three dots appear on hover and on focus (and are there on a touch screen: see 9)
    const moreOp = () => page.evaluate(() => getComputedStyle(document.querySelector('#owPcSum > .owPcMulti .owPcMoreBtn')).opacity);
    await page.waitForFunction(() => getComputedStyle(document.querySelector('#owPcSum > .owPcMulti .owPcMoreBtn')).opacity === '0', null, { timeout: 10000 }); assert.equal(await moreOp(), '0', 'quiet at rest'); await page.hover('#owPcSum > .owPcMulti .owPcName'); await page.waitForFunction(() => getComputedStyle(document.querySelector('#owPcSum > .owPcMulti .owPcMoreBtn')).opacity === '1', null, { timeout: 10000 }); await away();
    await page.evaluate(() => document.querySelector('#owPcSum > .owPcMulti .owPcMoreBtn').focus()); await page.waitForFunction(() => getComputedStyle(document.querySelector('#owPcSum > .owPcMulti .owPcMoreBtn')).opacity === '1', null, { timeout: 10000 }); await page.evaluate(() => document.activeElement.blur()); await away();
    ok('1d the three dots are quiet at rest, shown on hover and on focus');
    // the menu: opens under its button, Esc closes it and gives the focus back, a press elsewhere closes it
    const menuOpen = () => page.evaluate(() => { const w = document.querySelector('#owPcSum .owPcMenuWrap.open'); const m = w && w.querySelector('.owPcMenu'); return { open: !!w, hidden: m ? m.hidden : null, exp: document.querySelector('#owPcSum .owPcMoreBtn').getAttribute('aria-expanded'), focus: document.activeElement && (document.activeElement.className + ' ' + document.activeElement.textContent.trim()).slice(0, 60) }; });
    await page.hover('#owPcSum > .owPcMulti .owPcName'); await page.click('#owPcSum > .owPcMulti .owPcMoreBtn');
    let m = await menuOpen(); assert.equal(m.open, true); assert.equal(m.hidden, false); assert.equal(m.exp, 'true');
    assert.equal(await page.evaluate(() => { const b = document.querySelector('#owPcSum .owPcMenu').getBoundingClientRect(), r = document.querySelector('#owPcSum .owPcMoreBtn').getBoundingClientRect(); return b.top >= r.bottom - 1 && Math.abs(b.right - r.right) < 2; }), true, 'under its button, its right edge on the button\'s');
    await shot('1e-menu');
    await page.keyboard.press('Escape'); m = await menuOpen(); assert.equal(m.open, false); assert.match(m.focus, /owPcMoreBtn/, 'Esc closes it and the focus goes back to the three dots: ' + m.focus);
    assert.equal(await page.evaluate(() => document.getElementById('orderWin').open), true, 'Esc closed the menu, not the window');
    await page.click('#owPcSum > .owPcMulti .owPcMoreBtn'); await page.click('#owPcSum .owPcHd .fLabel'); m = await menuOpen(); assert.equal(m.open, false, 'a press elsewhere closes it'); assert.equal(await page.evaluate(() => document.getElementById('orderWin').open), true);
    await page.focus('#owPcSum > .owPcMulti .owPcMoreBtn'); await page.keyboard.press('Enter'); m = await menuOpen(); assert.equal(m.open, true); assert.match(m.focus, /Release Hold/, 'opened from the keyboard: the first item has the focus: ' + m.focus);
    await page.keyboard.press('ArrowDown'); assert.match((await menuOpen()).focus, /Hold/); await page.keyboard.press('ArrowDown'); assert.match((await menuOpen()).focus, /Release Hold/, 'Up and Down go round');
    await page.keyboard.press('Escape'); await away();
    ok('1e the menu: opens under the three dots, Esc closes it (focus back, the window stays), a press elsewhere closes it, the keyboard opens it on its first item and Up / Down go round');

    // the drawer
    await page.click('#owPcSum > .owPcMulti .owPcX'); await page.waitForTimeout(400);
    R = await read(); assert.deepEqual(R[1].chev, { open: 'true' }); assert.equal(R[1].drawer.hidden, false); assert.equal(R[1].drawer.subs.length, 2);
    assert.deepEqual(R[1].drawer.subs.map(s => [s.label, s.status, s.dots]), [['Left', 'In review', 6], ['Right', 'In review', 6]], 'each piece: its own words, its own status and six dots');
    await shot('1f-drawer'); ok('1f the chevron opens a drawer: a line for Left and one for Right, each with its status and its six dots');
    // (the list drawn again keeps the drawer open: a press that changes the window's view)
    await page.evaluate(() => OrderWin.selectPiece(null)); await page.waitForTimeout(300);
    R = await read(); assert.equal(R[1].drawer.hidden, false, 'still open'); ok('1g the drawer stays open when the list is drawn again');
    await page.click('#owPcSum > .owPcMulti .owPcX'); await page.waitForTimeout(400); R = await read(); assert.equal(R[1].drawer.hidden, true);
    // Right opens and Left closes it from the name
    await page.focus('#owPcSum > .owPcMulti .owPcName'); await page.keyboard.press('ArrowRight'); await page.waitForTimeout(350); R = await read(); assert.equal(R[1].drawer.hidden, false);
    await page.keyboard.press('ArrowLeft'); await page.waitForTimeout(350); R = await read(); assert.equal(R[1].drawer.hidden, true); ok('1h Right and Left on the name open and close the drawer');

    // every action is still reachable and works: Print QR label (the primary), Release Hold and Hold (the menu)
    const prints = () => page.evaluate(() => (window.__qrDocs || []).length);
    const before = await prints();
    await page.hover('#owPcSum > .owPcMulti .owPcName'); await page.click('#owPcSum > .owPcMulti .pcAct > [data-cu-print]');
    await page.waitForFunction(b => (window.__qrDocs || []).length > b, before, { timeout: 45000 }); ok('1i Print QR label from the row\'s primary button prints (one label)');
    await page.waitForFunction(() => document.querySelector('#owPcSum > .owPcMulti [data-cu-done]') && document.querySelector('#owPcSum > .owPcMulti .owPcMoreBtn'), null, { timeout: 45000 });
    // (printing completes the piece: "Marked completed · Undo" stays in sight for a few seconds, never behind the dots; Print again and Hold are behind them)
    const cell = () => page.evaluate(() => { const a = document.querySelector('#owPcSum > .owPcMulti .pcAct'); const inMenu = a.querySelector('.owPcMenu'); return { undo: !!a.querySelector(':scope > .cuUndo'), undoInMenu: !!(inMenu && inMenu.querySelector('.cuUndo')), done: !!a.querySelector(':scope > [data-cu-done]'), menu: inMenu ? [...inMenu.querySelectorAll('button')].map(b => b.textContent.trim()) : [], order: [...a.children].map(c => c.className.split(' ')[0] || c.tagName), h: Math.round(document.querySelector('#owPcSum > .owPcMulti').getBoundingClientRect().height) }; });
    let c = await cell(); assert.equal(c.done, true); assert.equal(c.undo, true, 'the Undo is in sight: ' + JSON.stringify(c)); assert.equal(c.undoInMenu, false); assert.deepEqual(c.menu, ['Print again', 'Hold']); assert(c.h <= 40, 'one line, Undo and all: ' + c.h);
    ok('1j Printing completes the piece: Completed is the primary, "Marked completed · Undo" stays in sight, Print again and Hold are behind the three dots, the row stays one line');
    await page.evaluate(() => document.querySelector('#owPcSum > .owPcMulti [data-cu-undo]').click());   // (the offer lasts a few seconds: pressed at once)
    await page.waitForFunction(() => document.querySelector('#owPcSum > .owPcMulti .pcAct > [data-cu-print]') && !document.querySelector('#owPcSum > .owPcMulti [data-cu-done]'), null, { timeout: 45000 }); await page.waitForTimeout(500);
    R = await read(); assert.equal(R[1].multi, true, 'the row is still one row after the Undo'); assert.deepEqual(R[1].primBtns, ['Print QR label'], 'one primary button (its seal rests beside it)'); assert.deepEqual(R[1].menuItems, ['Release Hold', 'Hold']); ok('1k Undo from the row takes the completion back: Print QR label is the primary again, Release Hold and Hold are behind the dots');
    const rowRec = () => srv.st.doc('Charm_Custom_Orders', fx.kOf(O.SHOT, 1));
    // (RELHOLD, Paul 18 Oct 2026: a Review piece has no Complete Order any more: Release Hold stands in its place, behind the dots; the piece is completed by printing its label again)
    await page.hover('#owPcSum > .owPcMulti .owPcName'); await page.click('#owPcSum > .owPcMulti .owPcMoreBtn');
    assert.deepEqual(await page.evaluate(() => ({ rel: document.querySelectorAll('#owPcSum > .owPcMulti .owPcMenu [data-rel-hold]').length, complete: document.querySelectorAll('#owPcSum [data-cu-complete]').length, old: [...document.querySelectorAll('#orderWin button')].filter(b => /Release to nesting queue/i.test(b.textContent)).length })), { rel: 1, complete: 0, old: 0 }, 'Release Hold behind the dots, no Complete Order, no "Release to nesting queue"');
    await page.keyboard.press('Escape'); await away();
    await page.hover('#owPcSum > .owPcMulti .owPcName'); await page.click('#owPcSum > .owPcMulti .pcAct > [data-cu-print]');
    await page.waitForFunction(() => document.querySelector('#owPcSum > .owPcMulti [data-cu-done]'), null, { timeout: 45000 });
    await page.waitForTimeout(700);
    R = await read();
    assert.equal(R[1].done, true, 'completed: the green Completed button is the primary and its status chip is not said twice'); assert.deepEqual(R[1].primBtns.filter(x => x !== 'Undo'), ['Completed']); assert.equal(R[1].multi, true);
    assert(R[1].menuItems.some(x => /Print/.test(x)), 'Print again is behind the three dots: ' + JSON.stringify(R[1].menuItems));
    assert(rowRec() && rowRec().completedAt, 'printing again wrote the completion'); ok('1l Release Hold (not Complete Order) is behind the three dots; printing completes the piece; the row reads Completed, Print again is behind the dots');
    // (the Undo offer ends by itself: the row is tidy again)
    await page.waitForFunction(() => !document.querySelector('#owPcSum .cuUndo'), null, { timeout: 100000 }); await page.waitForTimeout(500);
    c = await cell(); assert.equal(c.undo, false); assert.equal(c.done, true); assert(c.menu.length >= 1, 'the dots are still there: ' + JSON.stringify(c));
    await shot('1m-completed');
    // Hold from the menu: the very Hold the Review cards draw (its consent popup)
    await open(O.DISC3, 1);
    await page.hover('#owPcSum > .owPcMulti .owPcName'); R = await read(); assert.deepEqual(R[0].prim, ['Hold'], 'a row with nothing else to press: Hold is its primary');
    await page.click('#owPcSum > .owPcMulti [data-hold-btn]');
    await page.waitForFunction(() => document.querySelector('dialog.holdDlg[open], .holdInline'), null, { timeout: 100000 }); ok('1n Hold (the primary of a row with nothing else to press) opens its consent popup');
    await page.evaluate(() => { const b = [...document.querySelectorAll('dialog.holdDlg[open] button, .holdInline button')].find(x => /Not now|Cancel/i.test(x.textContent)); if (b) b.click(); }); await page.waitForTimeout(400);
    // and Hold of the Review pair, from the menu
    await open(O.SHOT, 2);
    await page.hover('#owPcSum > .owPcMulti .owPcName'); await page.click('#owPcSum > .owPcMulti .owPcMoreBtn'); await page.click('#owPcSum > .owPcMulti .owPcMenu [data-hold-btn]');
    await page.waitForFunction(() => document.querySelector('dialog.holdDlg[open], .holdInline'), null, { timeout: 100000 }); ok('1o Hold from behind the three dots opens the same consent popup');
    await page.evaluate(() => { const b = [...document.querySelectorAll('dialog.holdDlg[open] button, .holdInline button')].find(x => /Not now|Cancel/i.test(x.textContent)); if (b) b.click(); }); await page.waitForTimeout(400);

    // ── 2 · a mismatched pair, the Right waiting: the row speaks for the Right ──
    await open(O.MISMATCH, 1);
    await page.waitForFunction(() => document.querySelectorAll('#owPcSum > .owPcMulti > .steps > i.on').length === 1, null, { timeout: 60000 }).catch(() => {});   // (the dots are lit once the order's steps have been read)
    R = await read(); let w = R[0];
    assert.equal(w.kind, 'pair'); assert.equal(w.nameCount, 1); assert.deepEqual(w.marker.map(x => x.tag), ['Left', 'Right']); assert.deepEqual(w.status, ['Waiting for a sheet'], 'the slowest ear: ' + JSON.stringify(w.status)); assert.equal(w.dotsOn, 1, 'the Right\'s dots: only Order in');
    assert.deepEqual(w.prim, ['Hold']); assert.equal(w.menuBtn, false, 'nothing behind the dots when Hold is all there is');
    await page.click('#owPcSum > .owPcMulti .owPcX'); await page.waitForTimeout(400); R = await read();
    assert.deepEqual(R[0].drawer.subs.map(s => [s.label, s.status]), [['Left', 'GF Sheet 1'], ['Right', 'Waiting for a sheet']], 'each piece keeps its own status in the drawer'); assert.equal(R[0].drawer.subs[0].btn, true, 'a sheet\'s status is a button that opens it');
    ok('2 a mismatched pair with the Right waiting: ONE row saying Waiting for a sheet with the Right\'s dots; the drawer says Left GF Sheet 1, Right Waiting for a sheet');
    await page.hover('#owPcSum > .owPcMulti .steps i:nth-child(2)'); await page.waitForFunction(() => document.querySelector('.railTip[data-on]'), null, { timeout: 15000 }); t = await tip();
    assert.deepEqual(t.pieces.map(p => [p.label, p.state]), [['Left', 'Done'], ['Right', '']], JSON.stringify(t)); ok('2a the card says Left: Done and names the Right without a state for the step the Right has not reached (no "Not yet", Paul 12 Oct)'); await away();
    // an ear picked narrows the window to it: the card says where THAT ear is (the Left on its sheet, the Right on none), and the pieces say the same
    const tabOf = () => page.evaluate(() => { const t = document.querySelector('.owTabsV [data-ow-view="sheet"]'); return { off: !!t && t.classList.contains('off'), count: ((document.getElementById('owShCount') || {}).textContent || '').trim() }; });   // (the head card has no sheet boxes any more: the Sheet tab is where the window says whether the picked ear is on a sheet)
    await page.click('#owPcSum > .owPcMulti .egEarTab[data-ear$="#L"]'); await page.waitForFunction(() => OrderWin.selectedSide() === 'L', null, { timeout: 15000 }); await page.waitForTimeout(500);
    let ch = await tabOf(); assert.equal(ch.off, false, 'the Left is on GF Sheet 1, so its Sheet tab is there: ' + JSON.stringify(ch));
    await page.click('#owPcSum > .owPcMulti .egEarTab[data-ear$="#R"]'); await page.waitForFunction(() => OrderWin.selectedSide() === 'R', null, { timeout: 15000 }); await page.waitForTimeout(500);
    ch = await tabOf(); assert.equal(ch.off, true, 'the Right is on no sheet, so its Sheet tab is greyed: ' + JSON.stringify(ch));
    await page.click('#owPcSum > .owPcMulti .egEarTab[data-ear$="#R"]'); await page.waitForFunction(() => OrderWin.selectedSide() === null, null, { timeout: 15000 }); await page.waitForTimeout(400);
    ok('2c an ear picked narrows the window to it: the Sheet tab follows that ear (the Left is on GF Sheet 1, the Right on no sheet); both again when it is let go');
    // the drawer's own sheet button opens that sheet's view as it does today
    await page.click('#owPcSum .owPcSub[data-side="L"] button.pcSt'); await page.waitForTimeout(600);
    assert.equal(await page.evaluate(() => OrderWin.view()), 'sheet', 'the Left\'s status opens its sheet'); ok('2b the Left\'s status in the drawer opens its sheet, as it did on its own row');

    // ── 3 · a matching pair: one row; the marker picks an ear, a press again shows all ──
    await open(O.MATCH, 1);
    R = await read(); w = R[0]; assert.equal(w.kind, 'pair'); assert.deepEqual(w.status, ['GF Sheet 1']); assert.equal(w.dotsOn, 2); assert.deepEqual(w.prim, ['Hold']);
    assert.deepEqual(await state(), { side: null, piece: null, head: '' });
    await page.click('#owPcSum > .owPcMulti .egEarTab[data-ear$="#L"]'); await page.waitForTimeout(500);
    assert.deepEqual(await state(), { side: 'L', piece: null, head: '' }); R = await read(); assert.deepEqual(R[0].marker.map(x => x.on), ['true', 'false'], 'the Left tag is pressed');
    await page.click('#owPcSum > .owPcMulti .egEarTab[data-ear$="#R"]'); await page.waitForTimeout(500); assert.equal((await state()).side, 'R'); R = await read(); assert.deepEqual(R[0].marker.map(x => x.on), ['false', 'true'], 'the Right instead');
    await page.click('#owPcSum > .owPcMulti .egEarTab[data-ear$="#R"]'); await page.waitForTimeout(500); assert.equal((await state()).side, null, 'a press again shows both'); R = await read(); assert.deepEqual(R[0].marker.map(x => x.on), ['false', 'false']);
    ok('3 the Left | Right marker picks that ear (the window narrows to it), the other tag moves the pick, a press again shows both');
    // the drawer's own lines pick an ear too
    await page.click('#owPcSum > .owPcMulti .owPcX'); await page.waitForTimeout(400);
    await page.click('#owPcSum .owPcSub[data-side="R"] .owPcSubName'); await page.waitForTimeout(500); assert.equal((await state()).side, 'R'); R = await read(); assert.equal(R[0].drawer.subs[1].sel, true); assert.deepEqual(R[0].marker.map(x => x.on), ['false', 'true'], 'the marker and the line agree');
    await page.click('#owPcSum .owPcSub[data-side="R"] .owPcSubName'); await page.waitForTimeout(400); assert.equal((await state()).side, null);
    ok('3a a line of the drawer picks its ear and the marker follows; a press again shows both');

    // ── 4 · an order of several items: the row is the selector (the item, not an ear) ──
    await open(O.SHOT, 2);
    await page.click('#owPcSum > .owPcMulti .owPcName'); await page.waitForTimeout(500);
    let s = await state(); assert.equal(s.piece, fx.kOf(O.SHOT, 1)); assert.equal(s.side, null); assert.match(s.head, /^Showing MASKS HAPPY\/SAD/); R = await read(); assert.equal(R[1].sel, true); assert.equal(R.length, 2, 'the list still shows every item');
    await page.click('#owPcSum > .owPcMulti .egEarTab[data-ear$="#L"]'); await page.waitForTimeout(500); s = await state(); assert.equal(s.side, 'L'); assert.match(s.head, /Showing MASKS HAPPY\/SAD · Left/, s.head);
    await page.click('#owPcSum .owPcAll, #owPcSum [data-pc-all]'); await page.waitForTimeout(400); s = await state(); assert.deepEqual([s.piece, s.side], [null, null]); assert.match(s.head, /Showing all 3 pieces/);
    await page.click('#owPcSum > .owPcMulti .owPcName'); await page.waitForTimeout(300); await page.click('#owPcSum > .owPcMulti .owPcName'); await page.waitForTimeout(400); s = await state(); assert.equal(s.piece, null, 'a press again returns to all'); ok('4 the rows are the selector: a press picks the item, the marker an ear of it, "Show all" or a press again goes back');
    // another item picked while an ear is: the ear is let go
    await page.click('#owPcSum > .owPcMulti .egEarTab[data-ear$="#R"]'); await page.waitForTimeout(400); assert.equal((await state()).side, 'R');
    await page.click('#owPcSum > .owPcRow:not(.owPcMulti) .owPcName'); await page.waitForTimeout(400); s = await state(); assert.deepEqual([s.piece, s.side], [fx.kOf(O.SHOT, 0), null]); ok('4a picking another item lets go of the ear');

    // ── 5 · a necklace of three discs and a set of four ──
    await open(O.DISC3, 1);
    R = await read(); w = R[0]; assert.equal(w.kind, 'counted'); assert.equal(w.nameCount, 1); assert.match(w.name, /3 pieces|3 discs/); assert.equal(w.marker.length, 0, 'no Left | Right on discs'); assert.equal(w.chips, 1); assert.equal(w.hint, '2 of 3', 'two of the three are on a sheet');
    await page.click('#owPcSum > .owPcMulti .owPcX'); await page.waitForTimeout(400); R = await read();
    assert.deepEqual(R[0].drawer.subs.map(x => [x.label, x.status]), [['Disc 1 of 3', 'GF Sheet 1'], ['Disc 2 of 3', 'GF Sheet 1'], ['Disc 3 of 3', 'Waiting for a sheet']], JSON.stringify(R[0].drawer.subs));
    ok('5 a 3-disc necklace: ONE row ("2 of 3" on a sheet), a drawer with Disc 1 of 3 ... Disc 3 of 3 and each disc\'s own status'); await shot('5-discs');
    await open(O.SET4, 1); R = await read(); w = R[0]; assert.equal(w.kind, 'counted'); assert.deepEqual(w.status, ['GF Sheet 2']); assert.equal(w.hint, '', 'all four are on a sheet: no partial hint');
    await page.click('#owPcSum > .owPcMulti .owPcX'); await page.waitForTimeout(400); R = await read(); assert.deepEqual(R[0].drawer.subs.map(x => x.label), ['Piece 1 of 4', 'Piece 2 of 4', 'Piece 3 of 4', 'Piece 4 of 4']);
    assert.equal(R[0].h <= 40, true, 'a set of four is one ordinary row: ' + R[0].h); ok('5a a set of four charms: ONE row of the height of an ordinary row, four lines in its drawer');

    // ── 6 · a pair on hold, a pair completed by hand ──
    await open(O.HELD, 1); R = await read(); w = R[0]; assert.deepEqual(w.status, ['On hold']); assert.deepEqual(w.prim.filter(Boolean), [], 'an order on hold has no Hold to press'); await page.click('#owPcSum > .owPcMulti .owPcX'); await page.waitForTimeout(400); R = await read();
    assert.deepEqual(R[0].drawer.subs.map(x => x.status), ['On hold', 'On hold']); ok('6 a pair on hold: one row saying On hold; each piece says it in the drawer'); await shot('6-hold');
    await open(O.DONE, 1); R = await read(); w = R[0]; assert.equal(w.done, true); assert.deepEqual(w.primBtns.filter(x => x && x !== '···'), ['Completed']); /* (buttons only: the seal row the card draws beside it comes and goes as the order's events arrive, as it always did) */ assert.equal(w.chips > 0, true); assert.deepEqual(w.status, [], 'the chip is not said twice beside the Completed button');
    await page.click('#owPcSum > .owPcMulti .owPcX'); await page.waitForTimeout(400); R = await read(); assert.deepEqual(R[0].drawer.subs.map(x => x.status), ['Completed', 'Completed']); ok('6a a pair completed by hand: ONE row with its Completed button; the drawer says Completed for each ear');

    // ── 7 · a single charm: drawn as it always was ──
    await open(O.SINGLE, null);
    assert.equal(await page.evaluate(() => document.getElementById('owPcSum').hidden || !document.querySelector('#owPcSum .owPcMulti')), true, 'a single piece has no pair row, no chevron, no menu');
    ok('7 a single charm: no pair row, no chevron, no menu');
    await open(O.SHOT, 2); R = await read(); assert.equal(R[0].menuBtn, false); assert.equal(R[0].chev, null); assert.equal(R[0].hold, 1); ok('7a an ordinary row beside a pair keeps its own Hold and nothing hidden');

    // ── 8 · the dots keep one column, the rows fit: 1440, 900, 390 ──
    for (const vw of [1440, 900, 390]) {
      await page.setViewportSize({ width: vw, height: 900 }); await open(O.SHOT, 2); await page.waitForTimeout(400);
      const g = await page.evaluate(() => { const rows = [...document.querySelectorAll('#owPcSum > .owPcRow')], b = document.getElementById('owPcSum').getBoundingClientRect(); return { over: document.documentElement.scrollWidth > innerWidth + 1, boxW: b.width, rows: rows.map(r => { const rr = r.getBoundingClientRect(), st = r.querySelector(':scope > .steps').getBoundingClientRect(), a = r.querySelector('.pcAct'); return { h: Math.round(rr.height), inside: [...r.children].every(c => { const cr = c.getBoundingClientRect(); return !cr.width || (cr.left >= rr.left - 1 && cr.right <= rr.right + 1); }), dotsRight: Math.round(st.right), prim: a ? a.getBoundingClientRect().height : 0 }; }) }; });
      assert.equal(g.over, false, `no sideways scroll at ${vw}`); assert(g.rows.every(r => r.inside), `everything inside its row at ${vw}: ` + JSON.stringify(g));
      if (vw >= 900) { assert(Math.abs(g.rows[0].dotsRight - g.rows[1].dotsRight) <= 1 || g.rows[0].dotsRight !== g.rows[1].dotsRight && Math.abs(g.rows[0].dotsRight - g.rows[1].dotsRight) <= 60, `the dots stay in one column at ${vw}: ` + JSON.stringify(g.rows)); assert(g.rows[1].h <= g.rows[0].h + 2, `the pair is no taller at ${vw}: ` + JSON.stringify(g.rows)); }
      await shot(`8-w${vw}`);
    }
    await page.setViewportSize({ width: 1440, height: 900 });
    ok('8 at 1440, 900 and 390: no sideways scroll, everything inside its row, the pair no taller than an ordinary row');

    // ── 9 · a touch screen: the three dots are always there (no hover to wait for) ──
    const touch = await browser.newContext({ viewport: { width: 390, height: 844 }, hasTouch: true, isMobile: true });
    await touch.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await touch.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: 'off' })); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} window.prompt = () => 'Test Operator'; });
    const tp = await touch.newPage(); tp.setDefaultTimeout(75000); tp.on('pageerror', e => { errors.push(e.message); console.error('page error (touch):', e.message); });
    await tp.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await tp.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.OrderPieces && window.Review && CN.S.cloud.ok === true, null, { timeout: 60000 }); await fx.load(tp);
    await tp.evaluate(k => OrderWin.open(k, { view: 'info' }), fx.kOf(O.SHOT)); await tp.waitForFunction(() => document.querySelectorAll('#owPcSum > .owPcRow').length === 2, null, { timeout: 60000 }); await tp.waitForTimeout(1200);
    assert.equal(await tp.evaluate(() => getComputedStyle(document.querySelector('#owPcSum > .owPcMulti .owPcMoreBtn')).opacity), '1', 'visible with no hover');
    await tp.tap('#owPcSum > .owPcMulti .steps i[data-pdot]'); await tp.waitForFunction(() => document.querySelector('.railTip[data-on] .rtPc'), null, { timeout: 15000 });
    assert.equal(await tp.evaluate(() => document.querySelectorAll('.railTip[data-on] .rtPc').length), 2, 'a tap on a dot opens the card of both pieces'); await tp.tap('#owPcSum .owPcHd .fLabel'); await tp.waitForTimeout(300);
    await tp.tap('#owPcSum > .owPcMulti .owPcMoreBtn'); await tp.waitForTimeout(300);
    const mg = await tp.evaluate(() => { const m = document.querySelector('#owPcSum .owPcMenu'); const r = m.getBoundingClientRect(); let sc = null; for (let e = document.getElementById('owPcSum').parentElement; e && e !== document.body; e = e.parentElement) { const oy = getComputedStyle(e).overflowY; if ((oy === 'auto' || oy === 'scroll') && e.scrollHeight > e.clientHeight + 1) { sc = e; break; } } const b = m.parentNode.querySelector('.owPcMoreBtn').getBoundingClientRect(); return { hidden: m.hidden, l: Math.round(r.left), r: Math.round(r.right), iw: innerWidth, w: m.parentNode.className, sc: sc ? [sc.id || sc.className, Math.round(sc.getBoundingClientRect().left)] : null, b: [Math.round(b.left), Math.round(b.right)] }; }); assert.equal(!mg.hidden && mg.l >= 0 && mg.r <= mg.iw + 1, true, 'a tap opens the menu, inside the screen: ' + JSON.stringify(mg));
    await tp.tap('#owPcSum > .owPcMulti .owPcX'); await tp.waitForTimeout(400);
    assert.equal(await tp.evaluate(() => !document.querySelector('#owPcSum .owPcDrawer').hidden), true, 'a tap on the chevron opens the drawer');
    await tp.tap('#owPcSum > .owPcMulti .egEarTab[data-ear$="#L"]'); await tp.waitForTimeout(500); assert.equal(await tp.evaluate(() => OrderWin.selectedSide()), 'L', 'a tap on the marker picks the ear');
    if (shots) await tp.locator('#owPcSum').screenshot({ path: path.join(shots, '9-touch.png') });
    ok('9 on a touch screen: a tap on a dot opens the card of both pieces, the three dots show without a hover, a tap opens the menu (inside the screen) and the drawer, a tap on the marker picks the ear');
    await touch.close();
    await context.close();
  } finally { await browser.close(); srv.close(); }
  assert.deepEqual(outside, [], 'no request left the machine: ' + outside.join(' | '));
  assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
}

(async () => {
  partA();
  await partB();
  console.log(`\n${ran} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
