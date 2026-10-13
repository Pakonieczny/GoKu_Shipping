// ENGPIECES (engpieces-1013): the Engrave detail knows and shows how many ENGRAVED PIECES the order has, switches between them, and Back / Next reach every one (Paul, 13 Oct 2026):
//   "there is no easy way to tell how many pieces a given order has in the detailed engraving modal. Also, for this particular order the back and forward arrow buttons are not working, and I can't cycle through the
//    different charms, even though this order has 2 charms. You need to update this UI to be more clear as to how many pieces there are in a given order and how to toggle between them"
// and "This is the same order and should be shown as one order with 2 pieces".
//
// The case is ONE Etsy order with two LINES of one design (an SS necklace and a GF necklace, both engraved on the back): two pieces of ONE order. Before: nothing in the head told one piece from the other (same order number, same design,
// same "N of M" counter that never moved), so Back / Next "did nothing" to the eye. Expected: the head says "Piece 1 of 2" with one small button per piece (number, metal, tick when approved); a press switches; Back / Next walk every
// piece of the order in its line order and then go on to the next order; the counter counts ORDERS; the card / words / work area are always the open piece's own; a search or range that hides a piece does not make it unreachable.
//
// Offline only: the REAL page over the repo's fake site with the real drawings (_engraveRealWorld.cjs: real intake, pool, classifier, fit). Fake buyer names only ("Name One"); no Etsy, no paid call, no live write.
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/engrave-order-pieces.cjs      (CHROMIUM=<chrome>  SHOTS=<dir>  OP_KEEP=1 prints the reads)
// What a pass does NOT prove: a live order on the live page (live data, live jobs); only that the page's own code does this on the offline fake.
'use strict';
const R = require('./_engraveRealWorld.cjs'), Base = require('./_orderFactsWorld.cjs');

// the order as Etsy sends it: any number of lines, each with its own metal and words (the shared builder gives one line)
function orderOf(fx, now) {
  if (!fx.lines) fx = Object.assign({}, fx, { lines: [{ listing: fx.listing || ('18' + fx.rid.slice(-6)), sku: fx.sku, title: fx.title, qty: fx.qty, variations: fx.variations, personalization: fx.personalization }] });
  const at = (d, h, m) => { const t = new Date(now); t.setDate(t.getDate() + d); t.setHours(h, m, 0, 0); return Math.floor(t.getTime() / 1000); };
  const shipBy = at(fx.ship, 17, 0), line = (x, i) => ({ transactionId: fx.rid + (i + 1), listingId: x.listing, sku: x.sku, title: x.title, quantity: x.qty || 1, expectedShipDate: shipBy, variations: x.variations || [], metalKey: x.metalKey || 'gold', metalLabel: x.metalLabel || 'GF 14/20', personalization: x.personalization || [], buyerMessage: '' });
  return { receiptId: fx.rid, orderNumber: fx.rid, createTs: at(fx.placed.day, fx.placed.h, fx.placed.m), updateTs: at(0, 12, 0), shipBy, buyer: { name: fx.buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: fx.lines.map(line) };
}
Base.orderOf = orderOf;                                                               // (R.build serialises Base.orderOf into the page)
const V = (n, v) => ({ name: n, value: v });
const mk = (metal, words) => ({ listing: '1890000101', sku: 'COMET_9427', title: 'Comet Necklace, Celestial Charm Necklace', variations: [V('Metal choice', metal === 'silver' ? 'Silver' : 'Gold'), V('Font Choice', 'Stylish')], personalization: [words], metalKey: metal, metalLabel: metal === 'silver' ? 'SS' : 'GF 14/20' });
const BOTH = { listing: '1890000103', sku: 'SCHRODINGER CAT 2', title: 'Cat Necklace, Whimsical Cat Charm, engraved on both sides', variations: [V('Metal choice', 'Gold'), V('Engraving Side', 'Both'), V('Font Choice', 'Ace')], personalization: ['front - W\nback - Name Six'], metalKey: 'gold', metalLabel: 'GF 14/20' };
const ORD = (rid, buyer, hour, lines) => ({ rid, buyer, placed: { day: 0, h: hour, m: 10 }, ship: 5, lines });
R.ORDERS.opTwo = ORD('4290000141', 'Name Two', 11, [mk('silver', 'Name One'), mk('gold', 'Name Two')]);                       // Paul's order: an SS line and a GF line of one design, two sets of words
R.ORDERS.opTwin = ORD('4290000142', 'Name Three', 12, [mk('silver', 'Name Three'), mk('gold', 'Name Three')]);                // the same words on both (what Paul's screenshot showed: the two heads look the same)
R.ORDERS.opThree = ORD('4290000143', 'Name Four', 13, [mk('silver', 'Name Four'), mk('gold', 'Name Five'), mk('gold', 'Name Seven')]);
R.ORDERS.opFaced = ORD('4290000144', 'Name Five', 14, [mk('silver', 'Name Eight'), BOTH]);                                     // a back-only piece and a front + back piece
Object.assign(R.EXPECT_JOBS, { opTwo: 2, opTwin: 2, opThree: 3, opFaced: 3 });

const fails = [], note = (ok, what, more) => { console.log((ok ? '  PASS ' : '  FAIL ') + what + (more ? '  [' + more + ']' : '')); if (!ok) fails.push(what); };

/** What the Engrave detail shows now, read in the page. */
const READ = () => {
  const c = document.querySelector('#egQueue .rvItem'); if (!c) return null;
  const q = s => c.querySelector(s), tabs = [...c.querySelectorAll('.egPcTab')];
  return { key: c.dataset.key, rid: ((q('.reviewIdentity .ttl') || {}).textContent || '').trim(), counter: (q('.rh .kind') || {}).textContent || '', of: ((q('.egPcOf') || {}).textContent || '').trim(),
    tabs: tabs.map(b => ({ on: b.getAttribute('aria-pressed') === 'true', cur: b.getAttribute('aria-current') === 'true', piece: b.dataset.piece, state: b.dataset.state, text: b.innerText.replace(/\s+/g, ' ').trim() })),
    stripShown: !!(q('.egPcStrip') && q('.egPcStrip').getClientRects().length), earSwitch: !!q('.reviewIdentity .egEarSwitch'),
    cardPiece: (q('.engCard') || { dataset: {} }).dataset.piece || '', cardTxt: [...c.querySelectorAll('.engCard .engTxt')].map(x => x.textContent.trim()).join('|'),
    words: (q('.engp .engpWords') || {}).value || '', work: !!c.querySelector('.backHost svg, .backHost canvas, .backHost img'), prevTitle: (q('[data-a="prev"]') || {}).title || '', nextTitle: (q('[data-a="next"]') || {}).title || '' };
};

(async () => {
  const w = await R.world({ viewport: { width: 1440, height: 900 } });
  if (!w) { console.log('no playwright-core: nothing run'); return; }
  const page = w.page, rd = () => page.evaluate(`(${READ})()`), keep = process.env.OP_KEEP;
  const press = async sel => { await page.evaluate(s => document.querySelector(s).click(), sel); await R.settle(page, 450); const r = await rd(); if (keep) console.log('   ', sel, JSON.stringify({ key: r && r.key, of: r && r.of, counter: r && r.counter, words: r && r.words })); return r; };
  const open = async key => { await page.evaluate(k => { try { OrderWin.close(); } catch (_) {} CN.setMode('engrave'); Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: k, q: '' }); Engrave.render(); }, key); await page.waitForSelector('#egQueue .rvItem[data-kind=placement]', { timeout: 20000 }).catch(() => {}); await R.settle(page, 700); return rd(); };
  const NEXT = '#egQueue .rvItem [data-a="next"]', PREV = '#egQueue .rvItem [data-a="prev"]';
  try {
    const made = await R.build(w, ['opTwo', 'opTwin', 'opThree', 'opFaced', 'back']);
    const lineKeys = n => [1, 2, 3].map(i => R.ORDERS[n].rid + '_' + R.ORDERS[n].rid + i);
    note((made.opTwo.jobs || []).length === 2 && (made.opThree.jobs || []).length === 3 && (made.opFaced.jobs || []).length === 3, 'the real classifier made the jobs: two lines 2, three lines 3, a back line + a front + back line 3', ['opTwo', 'opThree', 'opFaced'].map(n => (made[n].jobs || []).length).join('/'));

    // ── the model: an order owns its pieces, whatever the line or the metal ──
    const HAS = await page.evaluate(() => typeof window.CharmNestEngraveOrder !== 'undefined' && typeof Engrave.openPiece === 'function');
    note(HAS, 'the order layer is on the page (CharmNestEngraveOrder, Engrave.openPiece)');
    const model = !HAS ? { two: { n: 0, labels: [], lines: [], states: [], count: {}, keys: [], stepFwd: [], stepBack: [] }, three: { n: 0, metals: [], idx: [] }, faced: { n: 0, faces: [] }, one: { n: 0, count: {} } } : await page.evaluate(() => {
      const E = CharmNestEngraveOrder, rowsOf = rid => Orders.rows().filter(r => String(r.order.receiptId) === rid), out = {};
      for (const [n, rid] of [['two', '4290000141'], ['three', '4290000143'], ['faced', '4290000144'], ['one', '4290000101']]) {
        const o = { rows: rowsOf(rid) }, ps = E.piecesOf(rid, o);
        out[n] = { n: ps.length, labels: ps.map(p => p.label), metals: ps.map(p => p.metal), faces: ps.map(p => p.faces.join('+')), states: ps.map(p => p.state), lines: ps.map(p => p.lineKey), count: E.countOf(rid, o),
          next0: (E.nextUnapproved(rid, ps[0] && ps[0].pieceKey, o) || {}).pieceKey || null, stepFwd: ps.map(p => (E.step(rid, p.pieceKey, 1, o) || {}).pieceKey || null), stepBack: ps.map(p => (E.step(rid, p.pieceKey, -1, o) || {}).pieceKey || null), keys: ps.map(p => p.pieceKey), idx: ps.map(p => E.indexOf(rid, p.pieceKey, o)) };
      }
      return out;
    });
    if (keep) console.log(JSON.stringify(model));
    note(model.two.n === 2 && model.two.labels.join() === 'SS,GF' && new Set(model.two.lines).size === 2, 'the order owns BOTH lines as two pieces, labelled by their metal (SS, GF), in line order', model.two.labels.join() + ' / ' + new Set(model.two.lines).size + ' lines');
    note(model.three.n === 3 && model.three.metals.join() === 'SS,GF,GF', 'a three-line order has three pieces in line order', model.three.metals.join());
    note(model.faced.n === 2 && model.faced.faces.join() === 'back,front+back', 'a front + back line is ONE piece with both faces (the other line one piece with a back)', model.faced.faces.join());
    note(model.one.n === 1 && model.one.count.n === 1, 'a one-piece order has one piece');
    note(model.two.states.every(s => s === 'to-approve') && model.two.count.left === 2 && model.two.count.approved === 0 && model.two.count.toApprove === 2, 'countOf: both pieces to approve, none approved, 2 left', JSON.stringify(model.two.count));
    note(model.two.next0 === model.two.keys[1] && model.two.stepFwd[0] === model.two.keys[1] && model.two.stepFwd[1] === null && model.two.stepBack[0] === null && model.two.stepBack[1] === model.two.keys[0], 'nextUnapproved goes to the other piece; step has no wrap (null past either end)');
    note(model.three.idx.join() === '0,1,2', 'indexOf is the place in the order');

    // ── Paul's order: the head says two pieces, and which one is open ──
    const [k1, k2] = lineKeys('opTwo');
    let r = await open(k1);
    note(r && r.key === k1 && /^piece 1 of 2$/i.test(r.of), 'the head of piece 1 says "Piece 1 of 2"', r && r.of);
    note(r && r.tabs.length === 2 && r.tabs[0].on && r.tabs[0].cur && !r.tabs[1].on && /1\s*SS/.test(r.tabs[0].text) && /2\s*GF/.test(r.tabs[1].text), 'a small button per piece: number and metal, the open one pressed', r && JSON.stringify(r.tabs.map(t => t.text + (t.on ? '*' : ''))));
    note(r && r.stripShown && !r.earSwitch, 'the strip is drawn (and no second piece switch beside it)');
    note(r && r.words === 'Name One' && /Name One/.test(r.cardTxt) && !/Name Two/.test(r.cardTxt) && r.work, 'piece 1 shows its own words in the card, the words box and a drawn work area', r && (r.words + ' | ' + r.cardTxt));
    note(r && /next piece/.test(r.nextTitle) && /previous piece/.test(r.prevTitle), 'Back / Next say they go through the pieces', r && r.nextTitle);
    const orders1 = await page.evaluate(() => new Set([...Engrave.items().values()].filter(j => ['review', 'words', 'blocked', 'classify', 'ready', 'fitting'].includes(j.state)).map(j => j.row.order.receiptId)).size);
    const lines1 = await page.evaluate(() => [...Engrave.items().values()].filter(j => ['review', 'words', 'blocked', 'classify', 'ready', 'fitting'].includes(j.state)).length);
    note(r && r.counter === `1 of ${orders1} · 0 done` && orders1 < lines1, 'the counter counts ORDERS (the two-line order is one of ' + orders1 + ', not one per piece of ' + lines1 + ' jobs)', r && r.counter + ' / orders ' + orders1 + ' / jobs ' + lines1);

    // ── Next / Back: every piece, then the next order ──
    r = await press(NEXT);
    note(r && r.key === k2 && /^piece 2 of 2$/i.test(r.of) && r.tabs[1].on && !r.tabs[0].on, 'Next goes to piece 2 of the SAME order, the strip follows', r && (r.key + ' ' + r.of));
    note(r && r.words === 'Name Two' && /Name Two/.test(r.cardTxt) && !/Name One/.test(r.cardTxt) && r.cardPiece === k2 && r.work, 'piece 2 shows ITS words (card, words box, work area), never piece 1\'s', r && (r.words + ' | ' + r.cardTxt + ' | ' + r.cardPiece));
    note(r && r.counter === `1 of ${orders1} · 0 done`, 'the counter is still ONE order', r && r.counter);
    r = await press(PREV);
    note(r && r.key === k1 && /^piece 1 of 2$/i.test(r.of) && r.words === 'Name One', 'Back goes back to piece 1 with its words', r && (r.key + ' ' + r.words));
    r = await press(NEXT); r = await press(NEXT);
    note(r && r.rid !== '4290000141', 'Next from the LAST piece goes on to the next order', r && r.rid);
    r = await press(PREV);
    note(r && r.key === k2 && r.rid === '4290000141', 'Back from that order\'s first piece lands on the last piece of the order before (piece 2 of 2)', r && (r.key + ' ' + r.rid));
    r = await press(PREV);
    note(r && r.key === k1, 'and Back again reaches piece 1: nothing was skipped', r && r.key);

    // ── the strip: one tap switches; the open piece is not pressed again ──
    await open(k1);
    const sameCard = await page.evaluate(() => { const c = document.querySelector('#egQueue .rvItem'); document.querySelector('#egQueue .egPcTab[aria-pressed=true]').click(); return new Promise(res => setTimeout(() => res(c === document.querySelector('#egQueue .rvItem')), 250)); });
    note(sameCard, 'a press on the open piece changes nothing');
    r = await press('#egQueue .egPcTab[aria-pressed=false]');
    note(r && r.key === k2 && r.words === 'Name Two' && r.tabs[1].on, 'one press on the strip switches to the other piece (its own words)', r && r.key);
    r = await press('#egQueue .egPcTab[aria-pressed=false]');
    note(r && r.key === k1 && r.words === 'Name One', 'and back', r && r.key);

    // ── the same words on both pieces (Paul's screenshot): the head still tells them apart ──
    const [t1, t2] = lineKeys('opTwin');
    r = await open(t1); const ra = r; r = await press(NEXT);
    note(ra && r && ra.key === t1 && r.key === t2 && ra.of !== r.of && JSON.stringify(ra.tabs.map(t => t.on)) !== JSON.stringify(r.tabs.map(t => t.on)) && ra.words === r.words, 'two pieces with the very same words: the head changes ("Piece 1 of 2" -> "Piece 2 of 2", the pressed button moves) so Next visibly works', ra && r && (ra.of + ' -> ' + r.of));

    // ── three pieces: Next x2 reaches all three, then the next order; Back reaches them again ──
    const [h1, h2, h3] = lineKeys('opThree');
    r = await open(h1); const seen = [r.key];
    r = await press(NEXT); seen.push(r.key); const w2 = r.words;
    r = await press(NEXT); seen.push(r.key); const w3 = r.words, of3 = r.of;
    note(seen.join() === [h1, h2, h3].join() && w2 === 'Name Five' && w3 === 'Name Seven' && /^piece 3 of 3$/i.test(of3), 'a three-piece order: Next, Next visits piece 1, 2, 3 in order with their own words', seen.map(s => s.slice(-1)).join());
    r = await press(NEXT);
    note(r && r.rid !== '4290000143', 'then the next order', r && r.rid);
    r = await press(PREV); r = await press(PREV);
    note(r && r.key === h2, 'Back twice from the next order passes piece 3 and reaches piece 2', r && r.key);

    // ── a front + back piece and a back piece in ONE order ──
    const [f1, f2] = lineKeys('opFaced');
    r = await open(f1);
    note(r && /^piece 1 of 2$/i.test(r.of) && r.tabs.length === 2 && /1\s*SS/.test(r.tabs[0].text) && /2\s*GF/.test(r.tabs[1].text), 'a front + back line is ONE piece of the strip (two buttons, not three)', r && r.tabs.map(t => t.text).join(' / '));
    r = await press(NEXT);
    note(r && /^piece 2 of 2$/i.test(r.of) && r.cardPiece === f2 && /Name Six/i.test(r.cardTxt) && r.work, 'Next reaches the front + back piece (its card shows the words, a work area is drawn)', r && (r.cardPiece + ' ' + r.cardTxt));
    note(await page.evaluate(() => document.querySelectorAll('#egQueue .rvItem .engCard .engFace').length === 2), 'its Front and Back thumbnails are still its own');
    r = await press(PREV);
    note(r && r.key === f1, 'Back returns to the back piece', r && r.key);

    // ── a search that hides a piece does not make it unreachable ──
    await open(k1);
    await page.evaluate(() => { Engrave.restoreView({ q: 'Name One' }); });
    r = await press(NEXT);
    note(r && r.key === k2 && r.words === 'Name Two' && /^piece 2 of 2$/i.test(r.of), 'with the search on "Name One" (piece 2\'s words do not match) Next still reaches piece 2, shown with its own words', r && (r.key + ' ' + r.words));
    await page.evaluate(() => { Engrave.restoreView({ q: '' }); Engrave.render(); }); await R.settle(page, 400);

    // ── openPiece: the way the guide takes a person to a piece ──
    await open(k1);
    const opened = await page.evaluate(({ rid, pk }) => (Engrave.openPiece ? Engrave.openPiece(rid, pk) : null), { rid: '4290000141', pk: k2 }); await R.settle(page, 500); r = await rd();
    note(opened === true && r && r.key === k2 && r.words === 'Name Two', 'Engrave.openPiece(rid, pieceKey) opens that piece\'s card', r && r.key);
    note(await page.evaluate(() => (Engrave.openPiece ? Engrave.openPiece('4290000141', 'no-such-piece') : null)) === false, 'an unknown piece moves nothing (false)');

    // ── a one-piece order keeps exactly the head it had ──
    r = await open(R.keyOf('back'));
    note(r && r.key === R.keyOf('back') && !r.stripShown && r.tabs.length === 0 && !/piece/i.test(r.of), 'a one-piece order has no strip and no "Piece 1 of 1"', r && r.of);
    note(r && /placement in the queue/.test(r.nextTitle), 'its Back / Next keep their old words', r && r.nextTitle);

    // ── state: the model and the strip tell which pieces are approved (the state is only read: no approval is run here) ──
    const sim = !HAS ? { before: { left: 0 }, after: {}, next: undefined } : await page.evaluate(() => {
      const rid = '4290000141', E = CharmNestEngraveOrder, o = () => ({ rows: Orders.rows().filter(r => String(r.order.receiptId) === rid) });
      const j1 = [...Engrave.items().values()].find(j => /_42900001411$/.test(j.key)), orig = j1.state;
      const before = E.countOf(rid, o()); j1.state = 'approved';
      const ps = E.piecesOf(rid, o()), after = E.countOf(rid, o()), next = E.nextUnapproved(rid, ps[1].pieceKey, o()), html = E.stripHtml(ps, ps[1].pieceKey, {});
      j1.state = orig; return { before, after, next: next && next.label, tick: /egPcTick/.test(html), tabState: /data-state="approved"/.test(html) };
    });
    note(sim.before.left === 2 && sim.after.left === 1 && sim.after.approved === 1 && sim.after.n === 2, 'one piece approved: the order has 1 piece left and is not done', JSON.stringify(sim.after));
    note(sim.tick && sim.tabState, 'an approved piece shows a tick in the strip');
    note(sim.next === null, 'nextUnapproved from the only piece left is null (nothing else to go to)', String(sim.next));
    note(!w.errors.length, 'no page errors', w.errors.join(' | ').slice(0, 200));
    if (process.env.SHOTS) { await open(k1); await R.shot(w, 'engpieces-two-piece-piece1'); await press(NEXT); await R.shot(w, 'engpieces-two-piece-piece2'); }
  } finally { await w.close(); }
  console.log(fails.length ? `\n${fails.length} FAILED:\n - ` + fails.join('\n - ') : '\nengrave-order-pieces: all checks passed');
  process.exitCode = fails.length ? 1 : 0;
})().catch(e => { console.error(e); process.exit(1); });
