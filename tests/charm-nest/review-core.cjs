// REVIEWCORE: the Review tab built ONCE from the cloud's data (charm-nest-review-core.js, CharmNestReviewCore.build) must say exactly what the page's own
// Review says, for the same world. The REAL sorter page (charm-nest-1.html) in headless Chromium over the repo's fake site (bridge-server.cjs): orders go
// through the real intake (Orders.interpretAll → Review.syncOrderItems), a person's actions are done the way the page's own buttons do them (a hold, Hold
// Order, Release Hold, Send to Sheet, a QR label printed, a skip, a cancel, an Etsy change...). Then, in the page, a snapshot of ONLY the data the core
// needs (what Firestore holds, or will hold: see the file's header) is handed to the core, and every number and every card is compared with the page:
//   counts      Open (the segment, Review.openCount()), Resolved, Review.count(), every chip of the bar in Open and in Resolved (Custom Orders, Need Resolution,
//               Held, Blocked SKU, Size...), the top nav's Review badge (the production flow's "In review / On hold" orders)
//   cards       the set of Open cards and of Resolved cards (their keys, as drawn: data-mkey), the decision queue (Review.items()), Review.openOrders()
//   the core    the same answer twice, and whatever order the lines come in
// Worlds: 1 questions (unknown SKUs shared by orders, an option shared by orders, missing material, blocked SKU, missing size, a custom order with questions, one waiting for its design),
// 2 holds / marks / changes (held orders, covered or not, cancelled; Hold Order open and resolved; Etsy changes), 3 custom orders (waiting, on their way to
// the laser, completed, sent, completed AND sent, a record whose line left the pull, a send under way, a monogram, a regular line completed by hand),
// 4 answers (Release Hold, a release under way, a decision answered, a skip, sends that narrow a shared question), 5 a busy day (a seeded mix of all of it).
// Made-up buyers; every request off the loopback refused; nothing real is read or written.
//   PW_DIR=... CHROMIUM=... node tests/charm-nest/review-core.cjs
'use strict';
if (process.argv[2]) process.env.PW_DIR = process.argv[2];
const W = require('./_reviewCoreWorld.cjs');

let fails = 0, checks = 0;
const say = (name, ok, seen) => { checks++; if (!ok) fails++; console.log(`${ok ? 'OK  ' : 'FAIL'} ${name}${ok || seen === undefined ? '' : ' :: ' + (typeof seen === 'string' ? seen : JSON.stringify(seen))}`); };
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
const sorted = xs => xs.slice().sort();
// (ONECARD, Paul 23 Nov 2026: "The system is not allowed under any circumstances to create multiple cards for a given order")
const oneCard = got => { const seen = new Map(); for (const c of got.cards.filter(x => x.state === 'Open')) for (const o of c.orders) seen.set(o, (seen.get(o) || 0) + 1); return [...seen.values()].every(n => n === 1); };
const diff = (a, b) => { const A = sorted(a), B = sorted(b), x = A.filter(k => !B.includes(k)), y = B.filter(k => !A.includes(k)); return { coreOnly: x, pageOnly: y, core: A.length, page: B.length }; };

/* ── the orders (made up) ── */
const DAY = 86400, BASE = Math.floor(Date.UTC(2026, 10, 12, 15) / 1000);
const order = (rid, lines, d = 0) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: BASE + d * 3600, updateTs: BASE + d * 3600 + 60, shipBy: BASE + 6 * DAY + d * 600, buyer: { name: 'Name ' + String(rid).slice(-3) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
const line = (tid, sku, title, vars, metal, listing, pers) => ({ transactionId: String(tid), listingId: String(listing || 1800000000 + (Number(String(tid).slice(-5)) % 100000)), sku, title, quantity: 1, expectedShipDate: BASE + 6 * DAY, variations: (vars || []).map(([name, value]) => ({ name, value })), metalKey: metal || '', metalLabel: metal === 'silver' ? 'Sterling Silver' : metal === 'gold' ? 'GF 14/20' : '', personalization: pers || [], buyerMessage: '' });
const one = (rid, sku, title, vars, metal, listing, d, pers) => order(rid, [line(rid + '1', sku, title, vars, metal, listing, pers)], d);
const KEY = rid => `${rid}_${rid}1`;
const SILVER = [['Metal', 'Sterling Silver']];

const MASTER = { OK_CHARM: {}, OK_TWO: {}, CUSTOM_SAVED_0012: {}, BLOCK_CHARM: { blocked: 'open outline', open: true }, SIZE_CHARM: { sizes: { '10mm': { sku: 'SIZE_CHARM 10mm' }, '12mm': { sku: 'SIZE_CHARM 12mm' } } } };

/** One check of a world: the page's Review, the core on the snapshot, and every number and card compared. */
async function check(w, label, expect) {
  const want = await W.truth(w);
  const snap = await W.snapshot(w, Date.now());
  const got = await W.core(w, snap);
  const c = got.counts;
  say(`${label} · the page's Open list is what its Open segment counts (harness)`, want.open.cards.length === want.open.segN[0] && want.done.cards.length === want.done.segN[1], [want.open.cards.length, want.open.segN, want.done.cards.length]);
  say(`${label} · Open (the tab's count, Review.openCount) = ${c.openCount}`, c.openCount === want.openCount && c.open === want.open.segN[0], { core: c.openCount, openCount: want.openCount, segment: want.open.segN[0] });
  say(`${label} · Resolved = ${c.resolved}`, c.resolved === want.done.segN[1], { core: c.resolved, page: want.done.segN[1] });
  say(`${label} · Review.count() (decisions, the banner) = ${c.reviewCount}`, c.reviewCount === want.count, { core: c.reviewCount, page: want.count });
  say(`${label} · Custom Orders chip = ${c.customOrders} (${c.customAsk} asking + ${c.customWait} waiting)`, c.customOrders === (want.open.chips.customOrder || 0), { core: c.customOrders, page: want.open.chips.customOrder || 0 });
  say(`${label} · Need Resolution chip = ${c.needResolution}`, c.needResolution === (want.open.chips.needResolution || 0), { core: c.needResolution, page: want.open.chips.needResolution || 0 });
  say(`${label} · Held chip = ${c.held}`, c.held === (want.open.chips.heldOrder || 0), { core: c.held, page: want.open.chips.heldOrder || 0 });
  const ORDER = await w.page.evaluate(() => CharmNestReviewCore.ORDER);
  for (const k of ORDER) if (!['customOrder', 'needResolution', 'heldOrder'].includes(k)) say(`${label} · Open chip ${k} = ${c[k]}`, c[k] === (want.open.chips[k] || 0), { core: c[k], page: want.open.chips[k] || 0 });
  const pageChips = Object.keys(want.open.chips).filter(k => !ORDER.includes(k)), pageDone = Object.keys(want.done.chips).filter(k => !ORDER.includes(k));
  say(`${label} · the bar has no chip the core does not know`, !pageChips.length && !pageDone.length, { pageChips, pageDone });
  for (const k of ORDER) say(`${label} · Resolved chip ${k} = ${c.resolvedChips[k]}`, c.resolvedChips[k] === (want.done.chips[k] || 0), { core: c.resolvedChips[k], page: want.done.chips[k] || 0 });
  const coreOpen = got.cards.filter(x => x.state === 'Open').map(x => x.mkey), coreDone = got.cards.filter(x => x.state === 'Resolved').map(x => x.mkey);
  say(`${label} · the Open cards (${coreOpen.length}) are the page's, card for card`, same(sorted(coreOpen), sorted(want.open.cards)), diff(coreOpen, want.open.cards));
  say(`${label} · the Resolved cards (${coreDone.length}) are the page's, card for card`, same(sorted(coreDone), sorted(want.done.cards)), diff(coreDone, want.done.cards));
  const pageItems = want.items.filter(k => !/^(eng|held):/.test(k));
  say(`${label} · the decision queue (${got.itemKeys.length}) is Review.items()`, same(got.itemKeys, sorted(pageItems)), diff(got.itemKeys, pageItems));
  say(`${label} · the orders in Open (${got.openOrders.length}) are Review.openOrders()`, same(got.openOrders, want.openOrders), diff(got.openOrders, want.openOrders));
  say(`${label} · the top nav's Review badge = ${c.reviewOrders} (the flow's In review / On hold orders)`, String(c.reviewOrders || '') === want.badge && c.reviewOrders === want.flowReview, { core: c.reviewOrders, badge: want.badge, flow: want.flowReview, openCount: c.openCount });
  // the core alone: the same answer again, and whatever order the store gives the lines in
  const again = await W.core(w, JSON.parse(JSON.stringify(snap)));
  const shuffled = Object.assign({}, snap, { lines: snap.lines.slice().reverse().sort((a, b) => (a.key.length % 3) - (b.key.length % 3)) });
  const mixed = await W.core(w, shuffled);
  say(`${label} · the core is deterministic (same snapshot twice, lines in another order)`, same(got, again) && same(got, mixed), { again: same(got, again), mixed: same(got, mixed) });
  if (expect) for (const [what, ok] of Object.entries(expect(got, want))) say(`${label} · the world is as built: ${what}`, ok, { counts: got.counts });
  return { got, want, snap };
}

/** A person's actions, done the way the page's own buttons and feeds do them, then the intake reads again (Orders.interpretAll → Review.syncOrderItems). */
async function act(w, list) {
  const err = await w.page.evaluate(async list => {
    const now = Date.now(), row = k => B.orders.byKey.get(k);
    try {
      for (const a of list) {
        const r = a.key ? row(a.key) : null;
        if (a.key && !r && !['customDone', 'customSent'].includes(a.do)) throw new Error('no line ' + a.key + ' for ' + a.do);
        switch (a.do) {
          case 'hold': r.state = 'held'; r.hold = r.reason = a.text || 'Order held by Name One'; r.heldAt = now - (a.ago || 0); r.holdSeen = false; CNListActivity.touch(r, r.heldAt); break;   // (charm-nest-order-hold.js 445)
          case 'release': r.hold = null; r.reason = null; r.state = 'pulled'; r.frontAt = now - (a.ago || 0); r.heldAt = null; break;                                                         // (OrderHold.release / Review.repool: frontAt)
          case 'releasing': r.releasing = { at: now - (a.ago || 0), by: a.by || 'Name Two', stage: 'place' }; break;
          case 'markOpen': B.maps.reviewMarks = Object.assign({}, B.maps.reviewMarks, { [a.rid + '_review']: { key: a.rid + '_review', kind: 'reviewMark', category: 'Review', receiptId: a.rid, state: 'open', review: { state: 'open', markedAt: now - (a.ago || 0), markedBy: a.by || 'Name Three' } } }); break;
          case 'markDone': B.maps.reviewMarks = Object.assign({}, B.maps.reviewMarks, { [a.rid + '_review']: { key: a.rid + '_review', kind: 'reviewMark', category: 'Review', receiptId: a.rid, state: 'open', review: { state: 'done', markedAt: now - 7200000, markedBy: 'Name Three', doneAt: now - (a.ago || 0), doneBy: a.by || 'Name Four', how: a.how || 'other' } } }); break;
          case 'change': r.changePending = true; r.repoolChanged = false; Review.add({ kind: 'orderChanged', key: 'chg:' + r.key, row: r, old: { text: '', spec: r.spec }, why: `Etsy updated order ${r.order.receiptId} after the pull (variations) · the customer's words changed` }); break;   // (Orders.revalidate 2282)
          case 'gone': r.state = 'gone'; r.reason = a.reason || 'cancelled on Etsy'; break;
          case 'skip': r.state = 'skipped'; r.reason = r.hold = `piece skipped by ${a.by || 'Name One'}`; r.problems = []; break;   // (the card's Skip, 23006)
          case 'cancel': { const c = await CN.api('charmNestLibrary', { op: 'cancelPut', orderId: a.rid, by: a.by || 'Name One', why: 'the buyer asked', record: { lines: [] } }); if (!c || c.error) throw new Error('cancelPut: ' + JSON.stringify(c)); await Cancelled.load(true); if (!Cancelled.has(a.rid)) throw new Error('not read back as cancelled: ' + a.rid); break; }   // (the cancel written to the cloud, Charm_Nest_Cancelled, and read back as every computer reads it)
          case 'customDone': B.maps.customDone = Object.assign({}, B.maps.customDone, { [a.key]: Object.assign({ key: a.key, receiptId: a.key.split('_')[0], transactionId: a.key.split('_')[1] || '', state: 'completed', how: a.how || 'print', completedAt: now - (a.ago || 0), completedBy: 'Name Five', category: a.category || 'Custom Orders' }, a.how === 'button' ? {} : { printedAt: now - (a.ago || 0), printedBy: 'Name Five', prints: 1, lastPrintedAt: now - (a.ago || 0), lastPrintedBy: 'Name Five' }) }); break;
          case 'customSent': B.maps.customSent = Object.assign({}, B.maps.customSent, { [a.key]: { at: now - (a.ago || 0), by: 'Name Six', id: 'designSent-' + a.key } }); break;   // (a send kept on the custom record, as the order window reads it)
          case 'sheet': { const ck = Review.cardKey(r), at = now - (a.ago || 0); if (!CustomSheet.hydrate([{ ck, rid: r.order.receiptId, at, phase: a.pending ? 'pending' : 'sent', sent: { id: `custom-sheet:${ck}:${at}`, at, by: 'Name Seven', lines: { [r.key]: [{ f: 'f1', i: 0 }] } }, files: [{ id: 'f1', name: 'own.ai', kind: 'ai', state: 'ready', metal: 'gold', qty: 1, pieces: 1, cloud: { path: 'charmnest/custom/own.ai' } }] }])) throw new Error('send not taken: ' + ck); break; }   // (a Send to Sheet receipt as the cloud gives it to every computer, Charm_Custom_Sheet: sent, or pressed and not through yet)
          case 'pool': r.poolIds = [r.key + '_1']; break;
          case 'override': r.materialOverride = a.metal || 'silver'; break;
          case 'settle': if (!Review.items().some(x => x.key === a.item)) throw new Error('no card ' + JSON.stringify(a.item)); Review.remove(a.item, a.by || 'Name One'); break;   // (a card taken away: Review.remove keeps it in Review.settled)
          case 'accept': Review.remove('chg:' + a.key, a.by || 'Name One'); r.changePending = false; break;   // (Accept the new order, acceptChange 21810: the answer is kept, the card leaves; the make-up again is the pool's)
          case 'drop': B.orders.rows = B.orders.rows.filter(x => x.key !== a.key); B.orders.byKey.delete(a.key); break;   // (the cancel flow takes the order's lines off the list)
          case 'monogram': break;
          default: throw new Error('unknown action ' + a.do);
        }
      }
      Orders.interpretAll();
      // (a skipped line keeps no question of its own until it is read again: as the Skip button leaves it)
      for (const a of list) if (a.do === 'skip') { const r = row(a.key); if (r) r.problems = []; }
      Review.syncOrderItems();
      return '';
    } catch (e) { return String(e && e.stack || e); }
  }, list);
  if (err) throw new Error('act: ' + err);
}

(async () => {
  const w = await W.open();
  if (!w) { console.log('SKIP review-core: no playwright-core (set PW_DIR)'); return; }
  try {
    await w.page.evaluate(M => { for (const [sku, e] of Object.entries(M)) B.master.entries.set(sku, Object.assign({ sku, widthPt: 20, heightPt: 20, updatedAt: 1 }, e)); }, MASTER);

    /* ═══ 1 · questions ═══ */
    // (CUSTOMWAIT, 20 Nov 2026: a custom order whose SKU has no design asks nothing, it waits on a person; the custom order WITH a question is one whose design a person saved to the
    //  charm repository, the listing's alias naming it, and whose metal is not read)
    await w.page.evaluate(() => { B.maps.aliases = Object.assign({}, B.maps.aliases, { 1811000012: { bySku: { CUSTOM_0012: 'CUSTOM_SAVED_0012' } } }); });
    await W.lines(w, [
      one('4301000001', 'NOPE_1', 'Unknown Charm', SILVER, 'silver', 1811000001, 1), one('4301000002', 'NOPE_1', 'Unknown Charm', SILVER, 'silver', 1811000002, 2), one('4301000003', 'NOPE_1', 'Unknown Charm', SILVER, 'silver', 1811000001, 3),   // one Unknown SKU, three orders
      one('4301000004', 'OK_CHARM', 'Ok Charm', [['Style', 'Funky Wobble']], 'silver', 1811000004, 4), one('4301000005', 'OK_CHARM', 'Ok Charm', [['Style', 'Funky Wobble']], 'silver', 1811000005, 5),   // one option question, two orders
      one('4301000006', 'OK_CHARM', 'Ok Charm', [['Finish', 'Mystery']], '', 1811000006, 6), one('4301000007', 'OK_CHARM', 'Ok Charm', [['Finish', 'Mystery']], '', 1811000006, 7), one('4301000008', 'OK_TWO', 'Ok Two', [], '', 1811000008, 8),   // material: one listing twice, another once
      one('4301000009', 'BLOCK_CHARM', 'Block Charm', SILVER, 'silver', 1811000009, 9), one('4301000010', 'BLOCK_CHARM', 'Block Charm', SILVER, 'silver', 1811000010, 10),   // a blocked SKU, two orders
      one('4301000011', 'SIZE_CHARM', 'Size Charm', [['Size', '15mm']], 'silver', 1811000011, 11),   // no design for the size
      one('4301000012', 'CUSTOM_0012', 'CUSTOM CHARM', [['Price', '28']], '', 1811000012, 12),       // a custom order with questions (its saved design read, its metal not)
      one('4301000016', 'CUSTOM_0016', 'CUSTOM CHARM', [['Price', '29']], '', 1811000016, 16),       // a custom order with no design yet: waits on a person, asks nothing (CUSTOMWAIT)
      one('4301000013', 'OK_CHARM', 'Ok Charm', SILVER, 'silver', 1811000013, 13),                   // nothing to ask
      order('4301000014', [line('43010000141', 'NOPE_2', 'Unknown Two', SILVER, 'silver', 1811000014), line('43010000142', 'OK_CHARM', 'Ok Charm', SILVER, 'silver', 1811000013)], 14),   // two lines, one asks
      one('4301000015', 'NOPE_9', 'Unknown Gone', SILVER, 'silver', 1811000015, 15)                  // asks, but its line left Etsy
    ]);
    await act(w, [{ do: 'gone', key: KEY('4301000015') }]);
    await check(w, 'world 1 (questions)', got => ({ 'eight decisions open (one Unknown SKU for three orders, one option for two), shown as ONE card per order (ONECARD, 23 Nov 2026): 14 cards': got.counts.reviewCount === 8 && got.counts.open === 14, 'Need Resolution holds the 9 orders of Unknown SKU + Options + metal': got.counts.needResolution === 9, 'every order is in exactly one Open card': oneCard(got), 'a blocked SKU (two orders), a size and two custom orders (one asking, one waiting)': got.counts.blockedSku === 2 && got.counts.missingSize === 1 && got.counts.customOrders === 2 && got.counts.customAsk === 1 && got.counts.customWait === 1 }));

    /* ═══ 2 · holds, Hold Order marks, Etsy changes ═══ */
    await W.lines(w, [
      one('4302000001', 'OK_CHARM', 'Ok Charm', SILVER, 'silver', 1812000001, 20),                 // held by a person
      order('4302000002', [line('43020000021', 'OK_CHARM', 'Ok Charm', SILVER, 'silver', 1812000002), line('43020000022', 'NOPE_3', 'Unknown Three', SILVER, 'silver', 1812000003)], 21),   // held, but its other line asks: covered
      one('4302000003', 'NOPE_4', 'Unknown Four', SILVER, 'silver', 1812000004, 22),               // held while it asks: parked, a held card
      one('4302000004', 'OK_CHARM', 'Ok Charm', SILVER, 'silver', 1812000005, 23),                 // held and cancelled
      one('4302000005', 'OK_CHARM', 'Ok Charm', SILVER, 'silver', 1812000006, 24),                 // Hold Order (open)
      one('4302000006', 'OK_CHARM', 'Ok Charm', SILVER, 'silver', 1812000007, 25),                 // Hold Order and held
      one('4302000007', 'OK_CHARM', 'Ok Charm', SILVER, 'silver', 1812000008, 26),                 // Hold Order resolved (Release Hold)
      one('4302000008', 'OK_CHARM', 'Ok Charm', SILVER, 'silver', 1812000009, 27),                 // Etsy changed it
      one('4302000009', 'OK_CHARM', 'Ok Charm', SILVER, 'silver', 1812000010, 28)                  // Etsy changed it, then it left Etsy
    ]);
    await act(w, [
      { do: 'hold', key: KEY('4302000001'), ago: 600000 }, { do: 'hold', key: '4302000002_43020000021', ago: 500000 }, { do: 'hold', key: KEY('4302000003'), ago: 400000, text: 'Taken off its sheet by Name One' },
      { do: 'hold', key: KEY('4302000004'), ago: 300000 }, { do: 'cancel', rid: '4302000004' },
      { do: 'markOpen', rid: '4302000005', ago: 200000 }, { do: 'markOpen', rid: '4302000006', ago: 190000 }, { do: 'hold', key: KEY('4302000006'), ago: 180000 },
      { do: 'markDone', rid: '4302000007', ago: 100000, how: 'other' }, { do: 'markOpen', rid: '4302999999' },   // (a mark for an order not in the pull: nothing)
      { do: 'markOpen', rid: '4301000001', ago: 90000 },   // (a held order that also has a question)
      { do: 'change', key: KEY('4302000008') }, { do: 'change', key: KEY('4302000009') }
    ]);
    await act(w, [{ do: 'gone', key: KEY('4302000009') }]);
    await check(w, 'world 2 (holds, marks, changes)', got => ({ 'two held orders no card covers (a held line whose order still asks, or that is held in Review, is covered; a cancelled one is not listed)': got.counts.held === 2 && got.cards.some(x => x.key === 'held:4302000003') && !got.cards.some(x => x.key === 'held:4302000004' || x.key === 'held:4302000002' || x.key === 'held:4302000006'), 'an open mark and an Etsy change are cards with no chip (a mark on an order that also asks a question is inside that order\'s one card)': got.counts.noChip.reviewMark === 2 && got.counts.noChip.orderChanged === 1, 'every order is in exactly one Open card': oneCard(got), 'a resolved mark is under Resolved': got.cards.some(x => x.state === 'Resolved' && x.kind === 'reviewMark') }));

    /* ═══ 3 · custom orders ═══ */
    await W.lines(w, [
      one('4303000001', 'CHAIN_0001', 'CHAIN REPLACEMENT', [['Metal', 'Gold'], ['Length', '17 Inches']], '', 1813000001, 30),   // waits for its label
      one('4303000002', 'CHAIN_0002', 'CHAIN REPLACEMENT', [['Metal', 'Gold'], ['Length', '18 Inches']], '', 1813000001, 31),   // on its way to the laser
      one('4303000003', 'CHAIN_0003', 'CHAIN REPLACEMENT', [['Metal', 'Gold'], ['Length', '16 Inches']], '', 1813000001, 32),   // QR label printed
      one('4303000004', 'CUSTOM_0004', 'CUSTOM CHARM', [['Price', '30']], '', 1813000004, 33),                                  // its own designs sent to a sheet
      one('4303000005', 'CHAIN_0005', 'CHAIN REPLACEMENT', [['Metal', 'Gold'], ['Length', '20 Inches']], '', 1813000001, 34),   // completed AND sent
      one('4303000007', 'CUSTOM_0007', 'CUSTOM CHARM', [['Price', '31']], '', 1813000007, 35),                                  // a send pressed, not through yet
      one('4303000008', 'OK_CHARM', 'Ok Charm', SILVER, 'silver', 1813000008, 36),                                             // a regular line completed by hand (Unknown SKU's card)
      one('4303000009', 'MONO_0009', 'Monogram Necklace Personalized Jewelry Initials Pendant', [['Metal', 'Gold']], 'gold', 997905351, 37, ['ABC']),   // a monogram: the Engraving tab's
      one('4303000010', 'ADDON_0010', 'Add a Custom Charm', [['Note', 'my dog']], 'gold', 1813000010, 38)                        // an add-on listing: waits for its own designs
    ]);
    await act(w, [
      { do: 'pool', key: KEY('4303000002') }, { do: 'customDone', key: KEY('4303000003'), ago: 50000 }, { do: 'sheet', key: KEY('4303000004'), ago: 40000 },
      { do: 'customDone', key: KEY('4303000005'), ago: 30000, how: 'button' }, { do: 'customSent', key: KEY('4303000005'), ago: 20000 },
      { do: 'customDone', key: '4303999999_43039999991', ago: 10000 },   // (its line left the pull long ago: the record is still listed)
      { do: 'sheet', key: KEY('4303000007'), pending: true }, { do: 'customDone', key: KEY('4303000008'), ago: 5000, how: 'button', category: 'Unknown SKU' }
    ]);
    await check(w, 'world 3 (custom orders)', (got, want) => ({ 'custom orders waiting on a person are in Open': got.counts.customWait >= 3, 'completed, sent, laid and orphan records are under Resolved': got.counts.resolvedChips.customOrder >= 5, 'a regular line completed by hand is under Need Resolution in Resolved': got.counts.resolvedChips.needResolution >= 1, 'the monogram is listed nowhere in Review': !got.cards.some(x => x.orders.includes('4303000009')) }));

    /* ═══ 4 · answers ═══ */
    await act(w, [
      { do: 'release', key: KEY('4302000001'), ago: 3600000 },                                // Release Hold, an hour ago
      { do: 'release', key: '4302000002_43020000021', ago: 60000 },                           // released while its other line still asks
      { do: 'releasing', key: KEY('4301000013'), ago: 30000 },                                // a release under way
      { do: 'settle', item: 'ord:mat:1811000006\u0000' }, { do: 'override', key: KEY('4301000006') }, { do: 'override', key: KEY('4301000007') }, { do: 'release', key: KEY('4301000006'), ago: 30000 },   // the metal answered; its release is said by that answer (within 2 min)
      { do: 'settle', item: 'ord:custom:4301000012:CUSTOM_SAVED_0012' },                            // taken away, but its custom card still asks: not listed under Resolved
      { do: 'accept', key: KEY('4302000008') },                                               // Accept the new order (Etsy's change)
      { do: 'skip', key: KEY('4301000011') },                                                 // Skip piece
      { do: 'customSent', key: KEY('4301000002') },                                           // one order of the shared Unknown SKU sent to a sheet: the question narrows
      { do: 'customSent', key: KEY('4301000004') }, { do: 'customSent', key: KEY('4301000005') },   // every order of the shared option sent: the question leaves Open
      { do: 'markDone', rid: '4302000005', how: 'ok', ago: 1000 }
    ]);
    await check(w, 'world 4 (answers)', got => ({ 'a release an hour ago is a Released card': got.cards.some(x => x.state === 'Resolved' && x.kind === 'released' && x.orders.includes('4302000001')), 'a release said by an answer is not listed twice': !got.cards.some(x => x.kind === 'released' && x.orders.includes('4301000006')), 'the skipped piece is a held order': got.cards.some(x => x.key === 'held:4301000011') }));

    /* ═══ 5 · a busy day (a seeded mix of all of it, on a fresh page state) ═══ */
    await W.clear(w);
    await w.page.evaluate(M => { for (const [sku, e] of Object.entries(M)) B.master.entries.set(sku, Object.assign({ sku, widthPt: 20, heightPt: 20, updatedAt: 1 }, e)); }, MASTER);
    let seed = 20261120; const rnd = () => ((seed = (seed * 1103515245 + 12345) % 2147483648) / 2147483648);
    const pick = xs => xs[Math.floor(rnd() * xs.length)];
    const KINDS = ['unknownShared', 'unknownOwn', 'option', 'material', 'blocked', 'size', 'custom', 'chain', 'chain', 'clean', 'clean', 'clean', 'addon', 'mono', 'two'];
    const orders = [], acts = [], late = [];
    for (let i = 0; i < 90; i++) {
      const rid = String(4305000000 + i), k = pick(KINDS), d = 40 + i;
      const mk = (sku, title, vars, metal, listing, pers) => one(rid, sku, title, vars, metal, listing, d, pers);
      const o = k === 'unknownShared' ? mk(pick(['SHARED_A', 'SHARED_B']), 'Shared', SILVER, 'silver', 1815000000 + (i % 7)) : k === 'unknownOwn' ? mk('OWN_' + i, 'Own', SILVER, 'silver', 1815100000 + i)
        : k === 'option' ? mk('OK_CHARM', 'Ok', [['Style', pick(['Wobble', 'Jiggle'])]], 'silver', 1815200000 + i) : k === 'material' ? mk('OK_CHARM', 'Ok', [['Finish', 'Mystery']], '', 1815300000 + (i % 3))
        : k === 'blocked' ? mk('BLOCK_CHARM', 'Block', SILVER, 'silver', 1815400000 + i) : k === 'size' ? mk('SIZE_CHARM', 'Size', [['Size', pick(['15mm', '20mm'])]], 'silver', 1815500000 + i)
        : k === 'custom' ? mk('CUSTOM_' + i, 'CUSTOM CHARM', [['Price', '25']], '', 1815600000 + i) : k === 'chain' ? mk('CHAIN_' + i, 'CHAIN REPLACEMENT', [['Metal', 'Gold'], ['Length', '17 Inches']], '', 1815700000)
        : k === 'addon' ? mk('ADDON_' + i, 'Add a Custom Charm', [['Note', 'idea ' + i]], 'gold', 1815800000 + i) : k === 'mono' ? mk('MONO_' + i, 'Monogram Necklace Personalized Jewelry Initials Pendant', [['Metal', 'Gold']], 'gold', 997905351, ['XYZ'])
        : k === 'two' ? order(rid, [line(rid + '1', 'SHARED_A', 'Shared', SILVER, 'silver', 1815000003), line(rid + '2', 'OK_CHARM', 'Ok', SILVER, 'silver', 1815900000 + i)], d)
        : mk('OK_CHARM', 'Ok', SILVER, 'silver', 1815900000 + i);
      orders.push(o);
      const key = KEY(rid), x = rnd();
      if (x < 0.10) acts.push({ do: 'hold', key, ago: Math.floor(rnd() * 9e6) });
      else if (x < 0.16) acts.push({ do: 'markOpen', rid, ago: Math.floor(rnd() * 9e6) });
      else if (x < 0.20) acts.push({ do: 'markDone', rid, ago: Math.floor(rnd() * 9e6), how: pick(['ok', 'pick', 'other']) });
      else if (x < 0.24) acts.push({ do: 'change', key });
      else if (x < 0.30) acts.push({ do: 'release', key, ago: Math.floor(rnd() * 9e6) });
      else if (x < 0.33) acts.push({ do: 'releasing', key, ago: Math.floor(rnd() * 6e5) });
      else if (x < 0.36) acts.push({ do: 'gone', key });
      else if (x < 0.39) { acts.push({ do: 'hold', key, ago: 1000 }, { do: 'cancel', rid }); }
      else if (x < 0.42) late.push({ do: 'skip', key });
      else if (x < 0.45) acts.push({ do: 'customSent', key, ago: Math.floor(rnd() * 9e6) });
      if (k === 'chain') { const y = rnd(); if (y < 0.3) acts.push({ do: 'customDone', key, ago: Math.floor(rnd() * 9e6), how: pick(['print', 'button']) }); else if (y < 0.5) acts.push({ do: 'pool', key }); else if (y < 0.6) acts.push({ do: 'customDone', key, ago: 5000 }, { do: 'customSent', key, ago: 4000 }); }
      if (k === 'custom' || k === 'addon') { const y = rnd(); if (y < 0.25) acts.push({ do: 'sheet', key, ago: Math.floor(rnd() * 9e6) }); else if (y < 0.4) acts.push({ do: 'sheet', key, pending: true }); }
      if (k === 'clean' && rnd() < 0.2) acts.push({ do: 'customDone', key, how: 'button', category: pick(['Unknown SKU', 'Options', 'Blocked SKU', 'Size']), ago: Math.floor(rnd() * 9e6) });
    }
    for (let j = 0; j < 6; j++) acts.push({ do: 'customDone', key: `43059999${String(j).padStart(2, '0')}_1`, ago: 1e5 * j });   // (records whose lines left the pull)
    await W.lines(w, orders);
    await act(w, acts);
    const accepts = await w.page.evaluate(() => Review.items().filter(x => x.key.startsWith('chg:')).slice(0, 2).map(x => ({ do: 'accept', key: x.key.slice(4) })));
    await act(w, late.concat(accepts, [{ do: 'settle', item: 'ord:sku:SHARED_B' }]));
    await check(w, 'world 5 (a busy day: 90 orders, seeded)', got => ({ 'every part of Review is there at once': got.counts.reviewCount > 5 && got.counts.held > 0 && got.counts.customOrders > 0 && got.counts.resolved > 10 && got.counts.needResolution > 0 }));

    /* ═══ 6 · the cloud's placement answer (PiecePlacement: where the pieces are, worked out from the pool and the sheets) ═══
       The fake site has no pieces, so every line reads "loading"; here the answer is given for a few lines at the page's own PiecePlacement.ofRow, the one
       place the page asks it (Orders.heldNow, the flow), and the snapshot reads it from there too. */
    await W.lines(w, [
      one('4306000001', 'OK_CHARM', 'Ok Charm', SILVER, 'silver', 1816000001, 200),     // held on another computer: only the placement says so
      one('4306000002', 'OK_CHARM', 'Ok Charm', SILVER, 'silver', 1816000002, 201),     // held here, the hold seen in the cloud, then released elsewhere: on its sheet
      one('4306000003', 'NOPE_77', 'Unknown', SILVER, 'silver', 1816000003, 202),       // asks, while its piece is on a sheet
      one('4306000004', 'OK_CHARM', 'Ok Charm', SILVER, 'silver', 1816000004, 203),     // held, its pieces cancelled
      one('4306000005', 'OK_CHARM', 'Ok Charm', SILVER, 'silver', 1816000005, 204),     // completed by hand
      one('4306000006', 'OK_CHARM', 'Ok Charm', SILVER, 'silver', 1816000006, 205)      // on a sheet, nothing to ask
    ]);
    await act(w, [{ do: 'hold', key: KEY('4306000002'), ago: 50000 }, { do: 'hold', key: KEY('4306000004'), ago: 40000 }]);
    await w.page.evaluate(stub => {
      const PP = window.PiecePlacement, orig = PP.ofRow.bind(PP);
      for (const [k, v] of Object.entries(stub)) if (v.seen) B.orders.byKey.get(k).holdSeen = true;
      PP.ofRow = r => (r && stub[r.key] ? Object.assign({ key: r.key, sheets: [], partial: false }, stub[r.key].pl) : orig(r));
      Review.syncOrderItems();
    }, { [KEY('4306000001')]: { pl: { state: 'hold', onSheet: false } }, [KEY('4306000002')]: { seen: true, pl: { state: 'sheet', onSheet: true } }, [KEY('4306000003')]: { pl: { state: 'sheet', onSheet: true } },
      [KEY('4306000004')]: { pl: { state: 'cancelled', onSheet: false } }, [KEY('4306000005')]: { pl: { state: 'hand', onSheet: false } }, [KEY('4306000006')]: { pl: { state: 'sheet', onSheet: true } } });
    await check(w, 'world 6 (the cloud\'s placement answer)', got => ({ 'a hold only the placement knows is a Held order': got.cards.some(x => x.key === 'held:4306000001'), 'a hold the cloud lifted elsewhere is not': !got.cards.some(x => x.key === 'held:4306000002'), 'a held line whose pieces were cancelled is still a Held order (the badge leaves it out)': got.cards.some(x => x.key === 'held:4306000004') }));

    say('no page errors', w.errors.length === 0, w.errors.slice(0, 5));
    say('no request left the loopback', w.outside.length === 0, w.outside.slice(0, 5));
  } catch (e) { fails++; console.log('FAIL the test stopped: ' + (e && e.stack || e)); }
  finally { await w.close(); }
  console.log(`\n${checks} checks, ${fails} failed`);
  console.log(fails ? 'FAIL review-core' : 'PASS review-core');
  process.exit(fails ? 1 : 0);
})().catch(e => { console.error(e); console.log('FAIL review-core'); process.exit(1); });
