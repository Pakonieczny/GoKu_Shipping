// PIECES · MONOGRAMS in the real page (PM, 16 Oct 2026, order 4195059483 "Mandy Bellamy", 2 pieces: "ALL multipiece orders must always show each individual piece with respect to all Engravings and Monograms").
// A monogram order line of quantity N is N pieces: each shows ITS OWN monogram widget (the three letters the buyer wrote, the same on each unless the buyer wrote one monogram for each), each is edited and approved on its own, and approving one
// writes THAT piece's design (quantity 1). The REAL sorter (charm-nest-1.html) in headless Chromium over the fake loopback site, Paul's real 78-letter alphabet file (indexed by the local script), the real CustomSheet, no Etsy, no paid AI, nothing live.
//   1  the Engrave tab list: a monogram line of quantity 2 is TWO rows (own thumbnail of its own letters, "PIECE 1 of 2"), the second order's "ABC / XYZ" gives the two pieces their own letters; a line of one piece is one row, as before
//   2  the detail: piece 1's card says "Piece 1 of 2", holds ONE widget of its own, the strip lists both pieces, Next goes to piece 2, Back returns; typing in piece 2 never touches piece 1; approving piece 1 writes ONE design
//      "Monogram APV - piece 1.ai" of quantity 1, leaves piece 2 to approve, and the detail goes on to piece 2; approving piece 2 adds its own design (two designs, in the pieces' order)
//   3  the Order window: ONE card per piece ("Monogram · Piece 1 of 2"), each with its own widget in its own state; Approve on piece 2 alone; a letter changed after approval REPLACES only that piece's design (never two for one piece);
//      the piece picked (OrderWin.selectPiece "<line>#C2") narrows the section to its card (when the window can pick it)
//   4  MIGRATION: an order approved as ONE design of quantity 2 ("Monogram APV.ai", made before pieces) reads as pieces 1 and 2 BOTH approved, in the Engrave tab and the Order window, with no Approve asked; changing piece 2 then splits it:
//      piece 1 keeps the old file, piece 2 has its own design
//   5  a MIXED order (monogram x2, an engraved bar x3, a plain charm x2): the Order window has the two monogram cards beside the bar's own card(s), none for the plain charm; the order's pieces (strip / count) include both monogram pieces
//   6  a line of ONE piece is what it was: "Monogram APV.ai", no piece tag, one card
//   7  phone width (400 px): the two piece cards of a window and the two rows / the detail of the Engrave tab fit; screenshots at 1440 and 400 px; no console error
//   node tests/charm-nest/pieces-monogram.cjs [playwright-core dir]      run it through /tmp/cn-run.sh 1500       MONO_REAL=0 uses stand-in alphabets      Screenshots go to $SHOTS (default /tmp/pc/PM/shots)
// Offline only: a pass proves the page's own code does this on the offline fake, not a live order, a live library or a real Send to Sheet of the files.
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const R = require('./_engraveRealWorld.cjs'), Base = require('./_orderFactsWorld.cjs'), A = require('./_monogramAlphabets.cjs');
const SHOTS = process.env.SHOTS || '/tmp/pc/PM/shots';
const V = (name, value) => ({ name, value });

function orderOf(fx, now) {
  const at = (d, h, m) => { const t = new Date(now); t.setDate(t.getDate() + d); t.setHours(h, m, 0, 0); return Math.floor(t.getTime() / 1000); };
  const shipBy = at(fx.ship, 17, 0), line = (x, i) => ({ transactionId: fx.rid + (i + 1), listingId: x.listing, sku: x.sku, title: x.title, quantity: x.qty || 1, expectedShipDate: shipBy, variations: x.variations || [], metalKey: x.metalKey || 'gold', metalLabel: 'GF 14/20', personalization: x.personalization || [] });
  return { receiptId: fx.rid, orderNumber: fx.rid, createTs: at(fx.placed.day, fx.placed.h, fx.placed.m), updateTs: at(0, 12, 0), shipBy, buyer: { name: fx.buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: (fx.lines || [fx]).map(line) };
}
Base.orderOf = orderOf;   // (R.build serialises Base.orderOf into the page)
const MONO_TITLE = 'Monogram Necklace Personalized Jewelry Initials Pendant';
const MONO = (words, qty, metal) => ({ listing: '997905351', sku: 'MONOGRAM_5351', title: MONO_TITLE, qty, variations: [V('Metal choice', metal === 'silver' ? 'Silver' : 'Gold'), V('Chain length', '16 inches')], personalization: [words], metalKey: metal || 'gold' });
// (MONOSEND, Paul 18 Oct 2026: Approve also sends a monogram's card to the sheets once every piece has its design, and a piece on a sheet is final. 'two' is changed after both its pieces are approved, so its order gives no metal
//  ('moonlight' is no metal the page knows): approved, not sent, the designs window asked for (held shut here, counted). 'diff', 'legacy' and 'solo' give theirs (GF) and go to the sheets with their last press.)
const MONO_OSR = { listing: '606439317', sku: 'MONOGRAM_9317', title: 'Monogram Necklace Dainty Monogram Necklace', qty: 2, variations: [V('Metal choice', 'Silver')], personalization: ['O S R'], metalKey: 'silver' };
const BAR = { listing: '1890000101', sku: 'COMET_9427', title: 'Comet Necklace, Celestial Charm Necklace in 14k Gold Fill', qty: 3, variations: [V('Metal choice', 'Gold'), V('Font Choice', 'Stylish')], personalization: ['Name One'] };
const CAT = { listing: '1890000107', sku: 'SCHRODINGER CAT 1', title: 'Cat Necklace, Whimsical Cat Charm Gift', qty: 2, variations: [V('Metal choice', 'Gold')], personalization: [] };
const O = R.ORDERS, ADD = (name, rid, buyer, lines, n, jobs) => { O[name] = { rid, buyer, placed: { day: 0, h: 9, m: n }, ship: 5, lines }; R.EXPECT_JOBS[name] = jobs || 0; };
ADD('two', '4195059483', 'Mandy Bellamy', [MONO('APV', 2, 'moonlight')], 10);
ADD('diff', '4290000141', 'Name Two', [MONO('ABC / XYZ', 2)], 11);
ADD('mixed', '4290000142', 'Name Three', [MONO_OSR, BAR, CAT], 12, 1);
ADD('legacy', '4290000143', 'Name Four', [MONO('APV', 2)], 13);
ADD('solo', '4200683017', 'Name One', [MONO('APV', 1)], 14);

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  try { require(path.join(pwDir, 'playwright-core')); } catch (_) { console.log('pieces-monogram: no playwright-core, skipped'); return; }
  process.env.PW_DIR = pwDir; fs.mkdirSync(SHOTS, { recursive: true });
  const fails = []; let nCheck = 0;
  const check = (ok, msg) => { nCheck++; if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const w = await R.world({ viewport: { width: 1440, height: 900 }, reducedMotion: 'reduce' });
  if (!w) { console.log('pieces-monogram: no playwright-core, skipped'); return; }
  const { page, srv } = w, st = srv.st, T0 = Date.now(), consoleErrors = [];
  page.on('console', m => { if (m.type() === 'error' && !/Failed to load resource|net::ERR|favicon/i.test(m.text())) consoleErrors.push(m.text().slice(0, 200)); });
  page.on('pageerror', e => consoleErrors.push('pageerror: ' + String(e.message).slice(0, 200)));
  const shot = async name => { try { await page.evaluate(() => document.querySelectorAll('.cnNbX, .toast .x').forEach(b => { try { b.click(); } catch (_) {} })); await sleep(200); await page.screenshot({ path: path.join(SHOTS, name + '.png') }); } catch (e) { console.log('   (no screenshot ' + name + ')'); } };
  try {
    // ═══ the library: Paul's three alphabets (his file, through the local indexer), else stand-ins ═══
    const REAL = process.env.MONO_FILE || path.join(__dirname, 'fixtures', 'SKUs_MONOGRAM_2024-1015.ai'), REAL_OK = process.env.MONO_REAL !== '0' && fs.existsSync(REAL);
    if (REAL_OK) {
      const tmp = fs.mkdtempSync(path.join(require('os').tmpdir(), 'cn-mono-pc-')), file = path.join(tmp, path.basename(REAL)); fs.copyFileSync(REAL, file);
      const { main } = require('../../scripts/index-master.cjs'), { REPO_TEST } = require('./bridge-server.cjs');
      st.repoStrict = true; process.env.CHARM_REPO_PASSWORD = REPO_TEST.password;
      const rep = await main(['node', 'x', file, '--origin', srv.sorterOrigin], () => {});
      check(!!rep && rep.written === 78, `the library holds Paul's three alphabets: 78 letter SKUs (${rep && rep.written})`);
    } else { A.seed(st, await A.make()); }
    await page.evaluate(async () => { await Master.load(true); });
    // this test is about the pieces, not the engine's rules: every word is staged as meeting them, so no card is flagged "Needs a person" and nothing asks the AI (that flow is monogram-ai.cjs)
    await page.evaluate(() => {
      window.__builds = []; const eb = CharmNestMonogram.build;
      CharmNestMonogram.build = function (l) { window.__builds.push(l.left.letter + l.middle.letter + l.right.letter); const r = eb.apply(this, arguments); if (r && r.ok && r.report && r.report.metrics && Array.isArray(r.report.metrics.violations)) r.report.metrics = Object.assign({}, r.report.metrics, { violations: [], ok: true }); return r; };
    });
    const made = await R.build(w, ['two', 'diff', 'mixed', 'legacy', 'solo', 'back']);
    const rows = await page.evaluate(() => Orders.rows().map(r => ({ key: r.key, rid: r.order.receiptId, mono: !!(r.spec && r.spec.special && r.spec.special.monogram), qty: r.spec && r.spec.quantity })));
    const K = (name, i = 0) => rows.filter(r => r.rid === O[name].rid)[i].key;
    check(rows.filter(r => r.mono).length === 5 && rows.filter(r => r.mono && r.qty === 2).length === 4, 'the world: 5 monogram lines (4 of quantity 2, 1 of quantity 1) read as monograms: ' + rows.filter(r => r.mono).map(r => r.rid.slice(-3) + '×' + r.qty).join(' '));
    const [kTwo, kDiff, kMono, kBar, kCat, kLeg, kSolo] = [K('two'), K('diff'), K('mixed', 0), K('mixed', 1), K('mixed', 2), K('legacy'), K('solo')];
    await page.evaluate(() => { window.__openCalls = 0; CustomSheet.open = () => { window.__openCalls++; return Promise.resolve(); }; });      // (MONOSEND: the designs window a missing metal opens is held shut, counted; monogram-ui.cjs has the window itself)
    const decided = k => page.evaluate(k => { const d = CustomSheet.decisionOf(Orders.rows().find(r => r.key === k)); return d ? (d.lines[k] || []).filter(p => !p.removed).length : 0; }, k);
    const settle = (ms = 500) => R.settle(page, ms);
    const until = async (fn, what, ms = 60000, arg) => { const t0 = Date.now(); let v; for (;;) { v = await page.evaluate(fn, arg); if (v) return v; if (Date.now() - t0 > ms) { console.log('   (waiting for ' + what + ')'); return null; } await sleep(100); } };
    const entries = () => page.evaluate(() => Object.values(CustomSheet.entries()).map(e => ({ rid: e.rid, files: e.files.map(f => ({ id: f.id, name: f.name, qty: f.qty, metal: f.metal, slot: f.mono && f.mono.slot, line: f.mono && f.mono.line })), sent: !!e.sent })));
    const filesOf = async rid => { const e = (await entries()).find(x => x.rid === rid); return e ? e.files : []; };
    const names = async rid => (await filesOf(rid)).map(f => f.name);
    const pst = (lineKey, n) => page.evaluate(({ k, n }) => { const ui = Engrave.monogramUI(), row = Orders.rows().find(r => r.key === k); const s = ui.status(row, n), d = ui.describe(row, n); return { state: s.state, approved: s.approved, letters: s.letters, pieceKey: s.pieceKey, tag: d.piece.tag, dirty: d.dirty }; }, { k: lineKey, n });
    const untilPiece = (lineKey, n, ok, what) => until(({ k, n, ok }) => { const ui = Engrave.monogramUI(), row = Orders.rows().find(r => r.key === k); const s = ui.status(row, n); return eval('(' + ok + ')')(s) ? s.state : null; }, what, 90000, { k: lineKey, n, ok: String(ok) });
    const readyOrApproved = s => ['ready', 'check', 'approved'].includes(s.state);
    const openList = () => page.evaluate(() => { CN.setMode('engrave'); Engrave.restoreView({ tab: 'place', list: true, chosen: true, focus: null, pin: null, openDone: null }); Engrave.render(); });
    const monoRows = () => page.evaluate(() => [...document.querySelectorAll('#egQueue .doneRow[data-kind=monogram]')].map(r => ({ rid: r.dataset.rid, key: r.dataset.open || r.dataset.key, slot: r.dataset.slot || '', tag: (r.querySelector('.egPiece') || {}).textContent || '', thumb: (r.querySelector('[data-mw-thumb]') || { dataset: {} }).dataset.mwThumb || '', thumbText: ((r.querySelector('.mwThumb') || {}).textContent || '').trim(), letters: ((r.querySelector('.egMonoLetters') || {}).textContent || '').trim(), hasSvg: !!r.querySelector('.mwThumb svg') })));
    const card = () => page.evaluate(() => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); if (!c) return null; const mw = c.querySelector('.mw');
      return { mono: !!c.dataset.mono, key: c.dataset.key, mw: !!mw, mwKey: mw && mw.dataset.key, mwSlot: mw && mw.dataset.slot, mwPiece: mw && mw.dataset.piece, mwState: mw && mw.dataset.state, widgets: c.querySelectorAll('.mw').length, sub: ((c.querySelector('.rh .sub, [data-mono-sub]') || {}).textContent || ''), meta: !!c.querySelector('.rh .sub, [data-mono-sub]'),
        strip: c.querySelector('.egPcStrip') ? c.querySelector('.egPcStrip').innerText.replace(/\s+/g, ' ').trim() : '', tabs: [...c.querySelectorAll('.egPcTab')].map(b => b.dataset.piece), letters: mw ? [0, 1, 2].map(i => mw.querySelector(`[data-mw-letter="${i}"]`).value).join('') : '' }; });
    const cardKey = () => page.evaluate(() => (document.querySelector('#egQueue .rvItem[data-kind=placement]') || { dataset: {} }).dataset.key);
    const waitCard = (k, what) => page.waitForFunction(k => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); return c && c.dataset.key === k && c.querySelector('.mw'); }, k, { timeout: 60000 }).catch(() => console.log('   (waiting for ' + what + ')'));
    const noSideways = async what => { const o = await page.evaluate(() => ({ sw: document.documentElement.scrollWidth, cw: document.documentElement.clientWidth })); check(o.sw <= o.cw + 1, 'phone width, ' + what + ': no sideways overflow ' + JSON.stringify(o)); };

    // ═══ 1 · the Engrave tab: a monogram line of quantity 2 is two rows ═══
    await openList(); await settle(2500);
    await page.waitForFunction(() => Engrave.monoItems().length >= 6, null, { timeout: 60000 }).catch(() => {});
    let mr = await monoRows();
    const rowsOf = rid => mr.filter(r => r.rid === rid);
    check((await page.evaluate(() => Engrave.monoItems().length)) === 9, 'the Engrave tab carries 9 monogram pieces: 2 + 2 + 2 + 2 + 1 (' + (await page.evaluate(() => Engrave.monoItems().length)) + ')');
    check(rowsOf(O.two.rid).length === 2 && rowsOf(O.two.rid).map(r => r.key).join() === `${kTwo}#C1,${kTwo}#C2`, 'the quantity-2 order has TWO rows in Placements, one for each piece: ' + rowsOf(O.two.rid).map(r => r.key.slice(-9)).join(' '));
    check(rowsOf(O.two.rid).map(r => r.tag).join('|') === 'PIECE 1 of 2|PIECE 2 of 2', 'each row says which piece it is: ' + rowsOf(O.two.rid).map(r => r.tag).join(' | '));
    check(rowsOf(O.two.rid).every(r => /A P V/.test(r.letters)) && rowsOf(O.two.rid).map(r => r.thumb).join() === `${kTwo}#C1,${kTwo}#C2`, 'each row has the finished charm of ITS piece (own thumbnail keyed by the piece) and the buyer\'s three letters');
    check(rowsOf(O.diff.rid).map(r => r.letters).join('|') === 'A B C|X Y Z', 'the buyer\'s "ABC / XYZ": piece 1 is A B C, piece 2 is X Y Z: ' + rowsOf(O.diff.rid).map(r => r.letters).join(' | '));
    check(rowsOf(O.solo.rid).length === 1 && rowsOf(O.solo.rid)[0].key === kSolo && rowsOf(O.solo.rid)[0].tag === '', 'a line of ONE piece is one row, with its line\'s key and no piece tag (as before)');
    check(rowsOf(O.mixed.rid).length === 2 && rowsOf(O.legacy.rid).length === 2, 'the mixed order shows its monogram as two rows too');
    await page.waitForFunction(() => Engrave.monoItems().every(m => m.st.state === 'ready'), null, { timeout: 120000 }).catch(() => {}); await settle(800);
    mr = await monoRows();
    check(rowsOf(O.two.rid).every(r => r.hasSvg), 'both pieces of the order have their finished charm drawn');
    await shot('list-1440');

    // ═══ 2 · the detail of piece 1: its own widget, the strip, Next / Back ═══
    await page.click(`#egQueue .doneRow[data-kind=monogram][data-open="${kTwo}#C1"]`);
    await waitCard(`${kTwo}#C1`, 'the card of piece 1');
    let c = await card();
    check(c && c.mono && c.key === `${kTwo}#C1` && c.widgets === 1 && c.mwKey === `${kTwo}#C1` && c.mwSlot === 'C1' && c.mwPiece === '1', 'piece 1 opens ITS card: ONE widget, of piece 1 (' + JSON.stringify(c && { key: c.key.slice(-9), widgets: c.widgets, slot: c.mwSlot }) + ')');
    // HDRMETA (Paul, 18 Oct 2026: "Remove 'Thin Strokes', 'charm', '5x8.5' text from the UI."): the head has no "Monogram · Piece 1 of 2 · A P V" line under the number; the strip's tab says which piece it is
    check(c && !c.meta && c.sub === '', 'its head draws no meta line under the number (HDRMETA): "' + (c && c.sub) + '"');
    check(c && /Piece 1 of 2/i.test(c.strip) && c.tabs.join() === `${kTwo}#C1,${kTwo}#C2`, 'the strip of the order lists both pieces: "' + (c && c.strip) + '"');
    await untilPiece(kTwo, 1, readyOrApproved, 'piece 1 made');
    await settle(700); await shot('detail-piece1-1440');
    // Next -> piece 2, Back -> piece 1
    await page.click('#egQueue .rvItem [data-a=next]'); await waitCard(`${kTwo}#C2`, 'Next to piece 2'); c = await card();
    check(c && c.key === `${kTwo}#C2` && c.widgets === 1 && c.mwSlot === 'C2' && !c.meta && /Piece 2 of 2/i.test(c.strip), 'Next goes to piece 2 of the SAME order: its own card and widget ("' + (c && c.strip) + '")');
    await page.click('#egQueue .rvItem [data-a=prev]'); await waitCard(`${kTwo}#C1`, 'Back to piece 1'); c = await card();
    check(c && c.key === `${kTwo}#C1` && c.mwSlot === 'C1', 'Back returns to piece 1');
    // a strip button picks the other piece
    await page.click(`#egQueue .rvItem .egPcTab[data-piece="${kTwo}#C2"]`); await waitCard(`${kTwo}#C2`, 'the strip to piece 2'); await page.click(`#egQueue .rvItem .egPcTab[data-piece="${kTwo}#C1"]`); await waitCard(`${kTwo}#C1`, 'the strip back to piece 1');
    check((await card()).key === `${kTwo}#C1`, 'the strip\'s buttons pick either piece');
    // the letters of one piece are its own
    await page.click('#egQueue .rvItem[data-mono] [data-mw-letter="1"]'); await page.keyboard.type('Q');
    await untilPiece(kTwo, 1, s => s.letters === 'AQV' && s.state === 'ready', 'piece 1 as AQV');
    check((await pst(kTwo, 1)).letters === 'AQV' && (await pst(kTwo, 2)).letters === 'APV', 'a letter typed in piece 1 (A Q V) never touches piece 2 (A P V)');
    await page.evaluate(() => document.activeElement.blur());
    await page.click('#egQueue .rvItem[data-mono] [data-mw-letter="1"]'); await page.keyboard.type('P');
    await untilPiece(kTwo, 1, s => s.letters === 'APV' && s.state === 'ready', 'piece 1 as APV again');
    // approve piece 1: ONE design of quantity 1, for piece 1
    await page.click('#egQueue .rvItem[data-mono] [data-mw-approve]');
    await until(k => Engrave.monogramUI().status(Orders.rows().find(r => r.key === k), 1).approved, 'piece 1 approved', 60000, kTwo);
    let fl = await filesOf(O.two.rid);
    check(fl.length === 1 && fl[0].name === 'Monogram APV - piece 1.ai' && fl[0].qty === 1 && fl[0].slot === 'C1' && fl[0].line === kTwo, 'approving piece 1 puts ONE design on the order: "Monogram APV - piece 1.ai", quantity 1, tagged C1: ' + JSON.stringify(fl.map(f => [f.name, f.qty, f.slot])));
    check((await pst(kTwo, 1)).approved === true && (await pst(kTwo, 2)).approved === false, 'piece 1 is approved; piece 2 is NOT (approving one piece never approves another)');
    // the detail goes on to piece 2 (the same order's piece still to approve)
    await waitCard(`${kTwo}#C2`, 'the detail to go on to piece 2'); await settle(500); c = await card();
    check(c && c.key === `${kTwo}#C2` && c.mwState !== 'approved', 'the detail goes on to piece 2, still to approve: ' + (c && c.key.slice(-9)));
    const od = await page.evaluate(r => CharmNestEngraveOrder.countOf(r), O.two.rid);
    check(od.n === 2 && od.approved === 1 && od.left === 1, 'the order model counts two pieces, one approved: ' + JSON.stringify(od));
    // piece 2: other letters, approved on its own
    await page.click('#egQueue .rvItem[data-mono] [data-mw-letter="0"]'); await page.keyboard.type('X');
    await untilPiece(kTwo, 2, s => s.letters === 'XPV' && s.state === 'ready', 'piece 2 as XPV');
    await page.click('#egQueue .rvItem[data-mono] [data-mw-approve]');
    await until(k => Engrave.monogramUI().status(Orders.rows().find(r => r.key === k), 2).approved, 'piece 2 approved', 60000, kTwo);
    fl = await filesOf(O.two.rid);
    check(fl.map(f => f.name).join('|') === 'Monogram APV - piece 1.ai|Monogram XPV - piece 2.ai' && fl.every(f => f.qty === 1) && fl[1].slot === 'C2', 'piece 2 approved on its own: two designs, one for each piece, in the pieces\' order: ' + fl.map(f => f.name).join(' | '));
    const od2 = await page.evaluate(r => CharmNestEngraveOrder.countOf(r), O.two.rid);
    check(od2.n === 2 && od2.approved === 2 && od2.left === 0, 'both pieces approved: ' + JSON.stringify(od2));

    // ═══ 3 · the Order window: one card for each piece ═══
    const openWin = async (key, ms = 900) => { await page.evaluate(async k => { CN.setMode('orders'); await OrderWin.open(k); }, key); await page.waitForSelector('#orderWin[open]', { timeout: 15000 }).catch(() => {}); await settle(ms); };
    const closeWin = async () => { await page.evaluate(() => OrderWin.close()); await settle(300); };
    const section = () => page.evaluate(() => {
      const host = document.getElementById('owEng'), sec = document.getElementById('owEngSec');
      return { hostHidden: !host || host.hidden, kids: [...(host ? host.children : [])].map(n => ({ kind: n.matches('[data-testid="ow-mono"]') ? 'mono' : n.classList.contains('engCard') ? 'card' : 'other', key: n.dataset.key || n.dataset.piece || '', slot: n.dataset.slot || '', state: n.dataset.state || '', title: (n.querySelector('.engName') || {}).textContent || '', widgets: n.querySelectorAll('.mw').length, mwKey: (n.querySelector('.mw') || { dataset: {} }).dataset.key || '', mwState: (n.querySelector('.mw') || { dataset: {} }).dataset.state || '',
        letters: n.querySelector('.mw') ? [0, 1, 2].map(i => n.querySelector(`.mw [data-mw-letter="${i}"]`).value).join('') : '', approve: !!n.querySelector('[data-mw-approve]:not([hidden])'), queue: (n.querySelector('.mw') || { dataset: {} }).dataset.queue || '', approved: !!n.querySelector('[data-mw-approved]:not([hidden])'), change: !!n.querySelector('[data-mw-change]:not([hidden])'), box: (() => { const r = n.getBoundingClientRect(); return { l: Math.round(r.left), t: Math.round(r.top), w: Math.round(r.width), h: Math.round(r.height) }; })() })),
        selected: window.OrderWin.selectedPiece(), secH: sec ? Math.round(sec.getBoundingClientRect().height) : -1 };
    });
    const monoCard = (n, sel = '') => `#owEng [data-testid="ow-mono"][data-slot="C${n}"] .mw ${sel}`.trim();
    await openWin(kTwo, 1200);
    check(!!await until(() => document.querySelectorAll('#owEng [data-testid="ow-mono"] .mw[data-state]').length === 2, 'both piece widgets', 30000), 'the window has two monogram widgets');
    let s = await section();
    check(s.kids.length === 2 && s.kids.every(k => k.kind === 'mono' && k.widgets === 1) && s.kids.map(k => k.key).join() === `${kTwo}#C1,${kTwo}#C2`, 'the Engraving section has ONE card for each piece, in order: ' + s.kids.map(k => k.key.slice(-9) + ' ' + k.title).join(' | '));
    check(s.kids.map(k => k.title).join('|') === 'Monogram · Piece 1 of 2|Monogram · Piece 2 of 2', 'each card says which piece it is: ' + s.kids.map(k => k.title).join(' | '));
    check(s.kids.map(k => k.mwKey).join() === `${kTwo}#C1,${kTwo}#C2` && s.kids.map(k => k.letters).join() === 'APV,XPV', 'each card holds the widget of ITS piece, with its own letters (A P V, X P V)');
    check(s.kids.every(k => k.mwState === 'approved' && k.queue === 'metal' && k.change) && !await decided(kTwo) && await page.evaluate(() => window.__openCalls) === 1, 'both pieces were approved in the Engrave tab, and the window says so for each; not sent: the order gives no metal, the designs window was asked for once, Change kept (MONOSEND): ' + JSON.stringify([s.kids.map(k => [k.mwState, k.queue, k.change]), await decided(kTwo), await page.evaluate(() => window.__openCalls)]));
    check(s.kids[0].box.t === s.kids[1].box.t && s.kids[0].box.l < s.kids[1].box.l || s.kids[1].box.t > s.kids[0].box.t, 'the two cards stand side by side, or one under the other, both visible: ' + JSON.stringify(s.kids.map(k => k.box)));
    await shot('window-two-approved-1440');
    // change piece 1: it alone is replaced; piece 2's design is untouched
    const idsBefore = (await filesOf(O.two.rid)).map(f => f.id);
    await page.click(monoCard(1, '[data-mw-change]'));
    await page.click(monoCard(1, '[data-mw-letter="1"]')); await page.keyboard.type('Q');
    await untilPiece(kTwo, 1, s2 => s2.letters === 'AQV' && s2.state === 'ready', 'piece 1 as AQV in the window');
    s = await section();
    check(s.kids[0].letters === 'AQV' && s.kids[1].letters === 'XPV' && s.kids[1].mwState === 'approved', 'a letter changed in piece 1\'s card (A Q V) leaves piece 2\'s card as it was (X P V, approved)');
    check((await filesOf(O.two.rid)).map(f => f.name).join('|') === 'Monogram APV - piece 1.ai|Monogram XPV - piece 2.ai', 'and changes no design until it is approved');
    await page.click(monoCard(1, '[data-mw-approve]'));
    await until(k => { const s2 = Engrave.monogramUI().status(Orders.rows().find(r => r.key === k), 1); return s2.approved && s2.letters === 'AQV' && !s2.dirty; }, 'piece 1 replaced', 60000, kTwo);
    fl = await filesOf(O.two.rid);
    check(fl.map(f => f.name).join('|') === 'Monogram AQV - piece 1.ai|Monogram XPV - piece 2.ai' && fl.length === 2, 'the change REPLACED piece 1\'s design (never two for one piece), piece 2\'s stays: ' + fl.map(f => f.name).join(' | '));
    check(fl[1].id === idsBefore[1] && fl[0].id !== idsBefore[0] && fl[0].slot === 'C1' && fl[1].slot === 'C2' && fl.every(f => f.qty === 1), 'piece 2\'s design is the very same file as before; the new one is tagged C1, quantity 1');
    // approving the same letters again changes nothing
    // the piece picked: the section narrows to its card
    const pick = await page.evaluate(k => { try { OrderWin.selectPiece(k + '#C2'); return OrderWin.selectedPiece(); } catch (e) { return 'ERR ' + e.message; } }, kTwo);
    await settle(500); s = await section();
    if (String(pick) === kTwo + '#C2' || (pick && pick.line === kTwo)) check(s.kids.length === 1 && s.kids[0].key === `${kTwo}#C2` && s.kids[0].title === 'Monogram · Piece 2 of 2', 'piece 2 picked: only its card (' + JSON.stringify(pick) + ')');
    else console.log('   (the window of this tree cannot pick a monogram piece yet: OrderWin.selectPiece gave ' + JSON.stringify(pick) + '; not asserted)');
    await page.evaluate(() => { try { OrderWin.selectPiece(null); } catch (_) {} }); await settle(400);
    await closeWin();

    // ═══ 3b · typing in one card of the window never touches the other; Approve on piece 2 alone (the "diff" order) ═══
    await openWin(kDiff, 1200);
    await until(() => document.querySelectorAll('#owEng [data-testid="ow-mono"] .mw[data-state="ready"]').length === 2, 'both diff pieces ready', 90000);
    s = await section();
    check(s.kids.map(k => k.letters).join() === 'ABC,XYZ', 'the buyer\'s "ABC / XYZ" order: piece 1 ABC, piece 2 XYZ: ' + s.kids.map(k => k.letters).join());
    await page.click(monoCard(2, '[data-mw-letter="2"]')); await page.keyboard.type('W');
    await untilPiece(kDiff, 2, s2 => s2.letters === 'XYW' && s2.state === 'ready', 'piece 2 as XYW');
    s = await section(); check(s.kids[0].letters === 'ABC' && s.kids[1].letters === 'XYW', 'typing in piece 2\'s card changed piece 2 only (ABC / XYW)');
    await page.click(monoCard(2, '[data-mw-approve]'));
    await until(k => Engrave.monogramUI().status(Orders.rows().find(r => r.key === k), 2).approved, 'piece 2 of diff approved', 60000, kDiff);
    check((await names(O.diff.rid)).join('|') === 'Monogram XYW - piece 2.ai', 'Approve on piece 2 alone: only its design is on the order: ' + (await names(O.diff.rid)).join(' | '));
    s = await section(); check(s.kids[0].mwState !== 'approved' && s.kids[0].approve && s.kids[1].mwState === 'approved', 'piece 1 is still to approve (it has its own Approve), piece 2 reads approved');
    await page.click(monoCard(1, '[data-mw-approve]'));
    await until(k => Engrave.monogramUI().status(Orders.rows().find(r => r.key === k), 1).approved, 'piece 1 of diff approved', 60000, kDiff);
    check((await names(O.diff.rid)).join('|') === 'Monogram ABC - piece 1.ai|Monogram XYW - piece 2.ai', 'piece 1 approved after piece 2: the designs stand in the pieces\' order (1, 2): ' + (await names(O.diff.rid)).join(' | '));
    await closeWin();

    // ═══ 4 · MIGRATION: an order approved as ONE design of quantity 2 ═══
    await page.evaluate(async k => {      // the old approval, as the earlier page wrote it: one "Monogram APV.ai", added without options (quantity = the order's 2)
      const ui = Engrave.monogramUI(), row = Orders.rows().find(r => r.key === k), it = Review.customItemFor(row.key, row);
      const c = ui.cards.get(row.key + '#C1') || (ui.status(row, 1), ui.cards.get(row.key + '#C1'));
      await ui.rebuild(c); const bytes = await CharmNestMonogram.toPdf(c.result, { title: 'Monogram APV' });
      CustomSheet.add(it, [new File([bytes], 'Monogram APV.ai', { type: 'application/illustrator' })], { quiet: true });
    }, kLeg);
    await until(k => { const e = Object.values(CustomSheet.entries()).find(x => x.files.some(f => f.name === 'Monogram APV.ai') && x.rid === '4290000143'); return e && e.files[0].state === 'ready'; }, 'the old design to be read', 60000);
    let leg = await filesOf(O.legacy.rid); const oldId = leg[0] && leg[0].id;
    check(leg.length === 1 && leg[0].name === 'Monogram APV.ai' && leg[0].qty === 2 && !leg[0].slot, 'the old design is there as the earlier page wrote it: "Monogram APV.ai", quantity 2, no piece: ' + JSON.stringify(leg.map(f => [f.name, f.qty, f.slot])));
    check((await pst(kLeg, 1)).approved === true && (await pst(kLeg, 2)).approved === true && (await pst(kLeg, 1)).state === 'approved' && (await pst(kLeg, 2)).state === 'approved', 'it reads as pieces 1 AND 2 both approved, no second approval asked');
    await openWin(kLeg, 1500);
    s = await section();
    // (MONOSEND: an approval from before the rule is approved but not on a sheet: each card keeps Approve to finish it, and nothing is asked again)
    check(s.kids.length === 2 && s.kids.every(k => k.mwState === 'approved' && k.queue === 'idle' && k.approve && k.change), 'the window: both piece cards are approved, not on a sheet yet, each with Approve to finish it: ' + JSON.stringify(s.kids.map(k => [k.mwState, k.queue, k.approve])));
    check(await page.evaluate(() => [...document.querySelectorAll('#owEng [data-mw-approved]')].every(n => /Monogram APV/.test(n.textContent))), 'each badge says "Approved · Monogram APV"');
    await shot('window-legacy-1440');
    // change piece 2: it is split in place, only piece 2 gets a new design
    await page.click(monoCard(2, '[data-mw-change]'));
    await page.click(monoCard(2, '[data-mw-letter="0"]')); await page.keyboard.type('X');
    await untilPiece(kLeg, 2, s2 => s2.letters === 'XPV' && s2.state === 'ready', 'migrated piece 2 as XPV');
    await page.click(monoCard(2, '[data-mw-approve]'));
    await until(k => { const s2 = Engrave.monogramUI().status(Orders.rows().find(r => r.key === k), 2); return s2.approved && s2.letters === 'XPV'; }, 'migrated piece 2 replaced', 60000, kLeg);
    leg = await filesOf(O.legacy.rid);
    check(leg.map(f => f.name).join('|') === 'Monogram APV - piece 1.ai|Monogram XPV - piece 2.ai' && leg.every(f => f.qty === 1), 'piece 2 changed: the old design is split: piece 1 keeps APV, piece 2 has XPV, in order: ' + leg.map(f => f.name + ' x' + f.qty).join(' | '));
    check(leg[0].id === oldId, 'piece 1\'s file is the old file itself (same id), now quantity 1 and tagged C1: ' + leg[0].slot);
    check((await pst(kLeg, 1)).approved && (await pst(kLeg, 2)).approved && (await pst(kLeg, 1)).letters === 'APV', 'both still approved, piece 1 never asked again');
    await until(k => !!CustomSheet.decisionOf(Orders.rows().find(r => r.key === k)), 'the legacy order sent', 60000, kLeg);
    check(await decided(kLeg) === 2, 'and that press sent the card: the two pieces, one each (MONOSEND): ' + await decided(kLeg));
    await closeWin();

    // ═══ 5 · a MIXED order: monogram x2, an engraved bar x3, a plain charm x2 ═══
    await openWin(kMono, 1800);
    await until(() => document.querySelectorAll('#owEng [data-testid="ow-mono"] .mw[data-state]').length === 2, 'the mixed order\'s two monograms', 60000);
    s = await section();
    const monos = s.kids.filter(k => k.kind === 'mono'), cards = s.kids.filter(k => k.kind === 'card');
    check(monos.length === 2 && monos.map(k => k.title).join('|') === 'Monogram · Piece 1 of 2|Monogram · Piece 2 of 2', 'the mixed order: two monogram cards, "Piece 1 of 2" and "Piece 2 of 2"');
    check(cards.length >= 1 && cards.every(k => k.key && k.key.startsWith(kBar)), 'and the bar\'s own engraving card(s) (' + cards.length + ') beside them; no card for the plain charm');
    check(s.kids[0].kind === 'mono' && s.kids[1].kind === 'mono' && s.kids[s.kids.length - 1].kind === 'card', 'in the lines\' order: the monogram line\'s two cards, then the bar\'s');
    check(await page.evaluate(() => { const b = document.querySelector('#owEng .engCard [data-e=approve]'); return !!b; }), 'the bar keeps its own Approve');
    const mixedPieces = await page.evaluate(r => CharmNestEngraveOrder.piecesOf(r).map(p => (p.kind || 'eng') + ':' + (p.slot || '-') + ':' + p.state), O.mixed.rid);
    check(mixedPieces.filter(x => x.startsWith('monogram')).length === 2 && mixedPieces.filter(x => x.startsWith('eng')).length >= 1, 'the order\'s pieces (strip / count / guide) include both monogram pieces and the bar: ' + mixedPieces.join(' '));
    await shot('window-mixed-1440');
    // the other pieces of the order are untouched by approving one monogram piece
    await until(() => document.querySelector('#owEng [data-testid="ow-mono"][data-slot="C1"] .mw[data-state="ready"]'), 'mixed piece 1 ready', 90000);
    await page.click(monoCard(1, '[data-mw-approve]'));
    await until(k => Engrave.monogramUI().status(Orders.rows().find(r => r.key === k), 1).approved, 'mixed piece 1 approved', 60000, kMono);
    check((await names(O.mixed.rid)).join('|') === 'Monogram OSR - piece 1.ai' && !(await pst(kMono, 2)).approved, 'approving the mixed order\'s monogram piece 1: its design only; piece 2 and the bar are untouched');
    await closeWin();

    // ═══ 6 · a line of ONE piece is what it was ═══
    await openWin(kSolo, 1500);
    await until(() => document.querySelectorAll('#owEng [data-testid="ow-mono"] .mw[data-state="ready"]').length === 1, 'the solo monogram', 60000);
    s = await section();
    check(s.kids.length === 1 && s.kids[0].key === kSolo && s.kids[0].title === 'Monogram' && s.kids[0].slot === '', 'a line of one piece: ONE card, titled "Monogram", its line\'s key, no slot');
    await page.click('#owEng [data-testid="ow-mono"] [data-mw-approve]');
    await until(k => Engrave.monogramUI().status(Orders.rows().find(r => r.key === k)).approved, 'solo approved', 60000, kSolo);
    fl = await filesOf(O.solo.rid);
    check(fl.length === 1 && fl[0].name === 'Monogram APV.ai' && fl[0].qty === 1 && !fl[0].slot && fl[0].line === kSolo, 'its design is "Monogram APV.ai", quantity 1, no piece slot (tagged with its line, MONOSEND): ' + JSON.stringify(fl.map(f => [f.name, f.qty, f.slot])));
    await until(k => !!CustomSheet.decisionOf(Orders.rows().find(r => r.key === k)), 'the solo order sent', 60000, kSolo);
    check(await decided(kSolo) === 1 && fl[0].metal === 'gold', 'and the one press sent it: ONE piece on the buyer\'s metal (GF), no designs window: ' + JSON.stringify([await decided(kSolo), fl[0].metal]));
    await closeWin();

    // ═══ 7 · phone width ═══
    await page.setViewportSize({ width: 400, height: 900 }); await settle(500);
    await openWin(kDiff, 1500);
    await page.evaluate(() => { const e = document.getElementById('owEngSec'); if (e) e.scrollIntoView({ block: 'center' }); }); await settle(400);
    const ph = await page.evaluate(() => {
      const cards = [...document.querySelectorAll('#owEng [data-testid="ow-mono"]')], vw = innerWidth;
      return { n: cards.length, pageW: document.scrollingElement.scrollWidth, vw, inside: cards.every(c => { const r = c.getBoundingClientRect(); return r.left >= 0 && r.right <= vw + 1; }), stacked: cards.length === 2 && cards[1].getBoundingClientRect().top >= cards[0].getBoundingClientRect().bottom - 1,
        wide: cards.reduce((n, c) => n + [...c.querySelectorAll('.mw *')].filter(x => { const r = x.getBoundingClientRect(), cr = c.getBoundingClientRect(); return r.width > 0 && (r.right > cr.right + 1 || r.left < cr.left - 1); }).length, 0) };
    });
    check(ph.n === 2 && ph.inside && ph.pageW <= ph.vw && ph.wide === 0, `phone: the two piece cards are inside the window, nothing runs past a card, no sideways scroll (${JSON.stringify(ph)})`);
    check(ph.stacked, 'phone: the cards stand one under the other');
    await shot('window-diff-400');
    await closeWin();
    await openList(); await settle(1200);
    await noSideways('the list with two rows for a quantity-2 monogram'); await shot('list-400');
    await page.click(`#egQueue .doneRow[data-kind=monogram][data-open="${kTwo}#C2"]`).catch(() => {});
    await settle(1200);
    if (await card()) { await noSideways('the detail of piece 2'); await shot('detail-piece2-400'); }
    await page.setViewportSize({ width: 1440, height: 900 }); await settle(300);
  } catch (e) { fails.push('crash: ' + (e && e.stack || e)); console.error(e); try { await shot('crash'); } catch (_) { /* no page */ } }
  finally {
    check(consoleErrors.length === 0, 'no console error and no page error anywhere: ' + JSON.stringify(consoleErrors.slice(0, 3)));
    check(w.errors.length === 0, 'no page error' + (w.errors.length ? ': ' + w.errors.slice(0, 2).join(' | ') : ''));
    console.log(`\n${nCheck - fails.length}/${nCheck} checks passed in ${((Date.now() - T0) / 1000).toFixed(0)} s`);
    try { await w.close(); } catch (_) { /* gone */ }
    if (fails.length) { console.log('FAILED:\n - ' + fails.join('\n - ')); process.exit(1); }
  }
})().catch(e => { console.error(e); process.exit(2); });
