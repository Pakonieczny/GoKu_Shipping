// The order window's facts (OrderFacts, charm-nest-order-facts.js; Paul, 12 Oct 2026: "a mush of stuff ... only the relevant data with hover states that expand more
// or drop-down menus"): the glance line of icon chips, a card on each, one Details drop-down with the whole old grid.
//   node tests/charm-nest/order-facts.cjs        (jsdom: NODE_PATH=/tmp/ps1-jsdom/node_modules; no network, nothing is written anywhere)
// A  the words: friendly purchase time, ship-by days and tone, "ago"
// B  the chips and their cards for each kind of order: a single charm, a matching pair, a mismatched pair, an order of several charms, front + back engraving,
//    a cancelled and a held order, a late ship-by, a message from the buyer (every name here is made up)
// C  the drawn thing in jsdom: the line, hover (350 ms), click, keyboard, Esc, outside press, copy with its check, Details, a repaint that changes nothing, a repaint that does
// D  nothing is lost: every cell of the old grid is in Details in its old order with its old <i> label and <span> words; the CSS adds no colour the page did not have
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const { JSDOM } = require('jsdom');
const R = path.join(__dirname, '../..') + path.sep;
const read = f => fs.readFileSync(R + f, 'utf8');
let n = 0;
const ok = (name, fn) => { try { fn(); n++; console.log('  ✓ ' + name); } catch (e) { e.message = `[${name}] ${e.message}`; throw e; } };
const later = [], okA = (name, fn) => later.push([name, fn]);
const wait = ms => new Promise(r => setTimeout(r, ms));

const dom = new JSDOM('<body><div class="owMeta" id="owMeta"></div></body>', { runScripts: 'outside-only', pretendToBeVisual: true, url: 'http://localhost/' });
const w = dom.window, d = w.document;
w.matchMedia = () => ({ matches: false });
w.eval(read('charm-nest-icons.js')); w.eval(read('charm-nest-order-facts.js'));
const OF = w.OrderFacts, host = d.getElementById('owMeta');
assert(OF && OF.paint && w.CharmNestIcons, 'the modules loaded');

// a fixed "now": Monday 12 Oct 2026, 2:30 PM in the machine's own zone (the module counts the shop's local days)
const at = (dayOffset, h, m) => new Date(2026, 9, 12 + dayOffset, h, m, 0, 0).getTime();
const NOW = at(0, 14, 30);

/* ── A · the words ── */
ok('A1 a purchase time is a friendly word for today and yesterday, a date for the rest, and the year only when it is not this one', () => {
  assert.equal(OF.whenFriendly(at(0, 11, 2), NOW).short, 'Today 11:02 AM');
  assert.equal(OF.whenFriendly(at(-1, 16, 15), NOW).short, 'Yesterday 4:15 PM');
  assert.equal(OF.whenFriendly(at(-5, 9, 40), NOW).short, 'Oct 7, 9:40 AM');
  assert.equal(OF.whenFriendly(new Date(2025, 11, 30, 8, 5).getTime(), NOW).short, 'Dec 30, 2025, 8:05 AM');
  const w1 = OF.whenFriendly(at(0, 11, 2), NOW);
  assert.equal(w1.full, 'Monday, October 12, 2026'); assert.equal(w1.time, '11:02 AM'); assert.equal(w1.ago, '3 hours ago');
  assert.equal(OF.whenFriendly(0, NOW), null); assert.equal(OF.whenFriendly(NaN, NOW), null);
});
ok('A2 ago says just now, minutes, hours, days, and nothing for a time that has not come', () => {
  assert.equal(OF.ago(NOW - 20000, NOW), 'just now'); assert.equal(OF.ago(NOW - 12 * 60000, NOW), '12 minutes ago'); assert.equal(OF.ago(NOW - 60000, NOW), '1 minute ago');
  assert.equal(OF.ago(NOW - 5 * 3600000, NOW), '5 hours ago'); assert.equal(OF.ago(NOW - 3 * 86400000, NOW), '3 days ago'); assert.equal(OF.ago(NOW + 3600000, NOW), '');
});
ok('A3 ship-by: days left with the Orders list\'s own tone (calm, warn the day before, bad when late), and a cancelled or finished order is never alarmed', () => {
  const by = days => Math.floor(at(days, 17, 0) / 1000);
  let s = OF.shipInfo(by(7), NOW); assert.equal(s.chip, 'Oct 19 · 7 days left'); assert.equal(s.tone, 'ok'); assert.equal(s.days, 7); assert.equal(s.full, 'Monday, October 19, 2026'); assert.equal(s.late, false);
  s = OF.shipInfo(by(2), NOW); assert.equal(s.chip, 'Oct 14 · 2 days left'); assert.equal(s.tone, 'ok');
  s = OF.shipInfo(by(1), NOW); assert.equal(s.chip, 'Oct 13 · tomorrow'); assert.equal(s.tone, 'warn'); assert.equal(s.words, 'Due tomorrow');
  s = OF.shipInfo(by(0), NOW); assert.equal(s.chip, 'Oct 12 · today'); assert.equal(s.tone, 'warn'); assert.equal(s.words, 'Due today');
  s = OF.shipInfo(by(-1), NOW); assert.equal(s.chip, 'Oct 11 · 1 day late'); assert.equal(s.tone, 'bad'); assert.equal(s.late, true);
  s = OF.shipInfo(by(-3), NOW); assert.equal(s.chip, 'Oct 9 · 3 days late'); assert.equal(s.tone, 'bad');
  s = OF.shipInfo(by(-3), NOW, true); assert.equal(s.tone, '', 'a cancelled or finished order: the date, no alarm'); assert.equal(s.chip, 'Oct 9 · 3 days late');
  s = OF.shipInfo(by(400), NOW); assert.equal(s.chip, 'Nov 16'); assert.equal(s.tone, 'ok');
  assert.equal(OF.shipInfo(0, NOW), null); assert.equal(OF.shipInfo(undefined, NOW), null);
  // late at 11:59 PM of the day after is still late by the calendar day, not by the clock
  assert.equal(OF.shipInfo(Math.floor(at(-1, 23, 59) / 1000), at(0, 0, 5)).tone, 'bad');
  assert.equal(OF.shipInfo(Math.floor(at(0, 0, 5) / 1000), at(0, 23, 55)).tone, 'warn', 'due today is warn all day, late only the next day');
});

/* ── B · fixtures: the facts the bridge gathers, in the shape OrderFacts takes (cells = the old grid) ── */
const cellsOf = (o, extra) => {
  const c = [{ label: 'Order', text: o.rid, copy: o.rid, mono: 1 }, { label: 'Purchased', text: 'Oct 12, 2026, 11:02 AM' }, { label: 'Buyer', text: o.buyer }, { label: 'Quantity', text: String(o.qty || 1) }, { label: 'Metal', text: 'GF 14/20' }, { label: 'State', text: o.state || 'pulled' }, { label: 'Ship by', text: 'Oct 19' }];
  return c.concat(extra || [], [{ label: 'Listing', text: '1737560072', copy: '1737560072', mono: 1 }, { label: 'Title', text: o.title, wide: 1 }]);
};
const base = (o, over) => Object.assign({ key: o.rid + '_1', loading: false, now: NOW, on: {}, order: { id: o.rid, num: '' }, buyer: { name: o.buyer, gift: false }, purchased: { ts: at(0, 11, 2), label: 'Purchased' }, ship: { by: Math.floor(at(7, 17, 0) / 1000), quiet: false },
  metal: { label: 'GF 14/20', none: false, color: '#c8a24e', choices: [{ name: 'Metal choice', value: 'Gold' }] }, qty: { n: 1, pieces: 0, pair: false, mismatched: false, counted: null, notes: [] }, custom: null, engraving: null, message: null,
  listing: { id: '1737560072', title: o.title }, state: { tone: 'info', text: 'pulled', sheet: '' }, cells: cellsOf(o) }, over || {});
const F = {};
F.single = base({ rid: '4195000011', buyer: 'Alder Fixture', title: 'Pressed leaf charm necklace' });
F.pair = base({ rid: '4195000012', buyer: 'Birch Sample', title: 'Theatre Happy Sad Drama Mask Earrings available in 14k Gold Fill, Sterling Silver, 14k Rose Gold Fill or 14K Solid Gold Earrings' }, { qty: { n: 1, pieces: 2, pair: true, mismatched: false, counted: null, notes: [] } });
F.mismatched = base({ rid: '4195000013', buyer: 'Cedar Example', title: 'Mismatched mitten earrings' }, { qty: { n: 2, pieces: 4, pair: true, mismatched: true, counted: null, notes: ['two different designs'] } });
F.multi = base({ rid: '4195000014', buyer: 'Dune Placeholder', title: 'Comedy and tragedy necklace, one charm' }, { qty: { n: 3, pieces: 0, pair: false, mismatched: false, counted: null, notes: [] } });
F.discs = base({ rid: '4195000019', buyer: 'Ivy Stub', title: 'Initial disc necklace' }, { qty: { n: 1, pieces: 0, pair: false, mismatched: false, counted: { total: 2, noun: 'disc' }, notes: [] } });
F.engraved = base({ rid: '4195000015', buyer: 'Elm Dummy', title: 'Mini tag necklace' }, { engraving: { chip: 'Front + back', approved: false, html: '<span>Front · W<br>Back · 1. 27. 24</span>' }, cells: cellsOf({ rid: '4195000015', buyer: 'Elm Dummy', title: 'Mini tag necklace' }, [{ label: 'Engraving', html: '<span>Front · W<br>Back · 1. 27. 24</span>', wide: 1, block: 1 }]) });
F.cancelled = base({ rid: '4195000016', buyer: 'Fir Testcase', title: 'Pressed leaf charm necklace', state: 'cancelled' }, { ship: { by: Math.floor(at(-3, 17, 0) / 1000), quiet: true }, state: { tone: 'bad', text: 'cancelled', sheet: '' } });
F.held = base({ rid: '4195000017', buyer: 'Gum Specimen', title: 'Pressed leaf charm necklace', state: 'held' }, { ship: { by: Math.floor(at(1, 17, 0) / 1000), quiet: false }, state: { tone: 'bad', text: 'held', sheet: 'GF Draft 1' } });
F.late = base({ rid: '4195000020', buyer: 'Hazel Standin', title: 'Pressed leaf charm necklace' }, { ship: { by: Math.floor(at(-3, 17, 0) / 1000), quiet: false } });
F.message = base({ rid: '4195000021', buyer: 'Juniper Mock', title: 'Pressed leaf charm necklace' }, { buyer: { name: 'Juniper Mock', gift: true }, message: { buyer: 'Could it arrive before the 20th? It is a surprise.', gift: 'Happy birthday, love from all of us.', unread: 1, bad: false } });

const plain = x => JSON.parse(JSON.stringify(x));   // (the module lives in jsdom's realm: its arrays are not this realm's)
const chip = (f, id) => plain(OF.chipsOf(f, NOW)).find(c => c.id === id);
const ids = f => plain(OF.chipsOf(f, NOW)).map(c => c.id);

ok('B1 a single charm: order, buyer, purchased, ship by and metal at a glance, and nothing it has no word for (no quantity, no engraving, no message chip)', () => {
  assert.deepEqual(ids(F.single), ['order', 'buyer', 'purchased', 'ship', 'metal', 'state']);
  const o = chip(F.single, 'order'); assert.equal(o.text, '4195000011'); assert.equal(o.copy, '4195000011'); assert.equal(o.mono, 1);
  assert.equal(chip(F.single, 'buyer').text, 'Alder Fixture'); assert.equal(chip(F.single, 'purchased').text, 'Today 11:02 AM');
  const s = chip(F.single, 'ship'); assert.equal(s.text, 'Oct 19 · 7 days left'); assert.equal(s.tone, 'ok');
  const m = chip(F.single, 'metal'); assert.equal(m.text, 'GF 14/20'); assert.equal(m.dot, '#c8a24e');
  assert.equal(chip(F.single, 'state').right, 1);
});
ok('B2 the order\'s card holds the number (with copy), the listing (with copy) and the full title; the metal card holds the buyer\'s metal choice; the date card the full date', () => {
  const rows = chip(F.pair, 'order').pop.rows.map(r => [r.k, r.v, !!r.copy]);
  assert.deepEqual(rows, [['Order number', '4195000012', true], ['Listing', '1737560072', true], ['Title', F.pair.listing.title, false]]);
  assert.deepEqual(chip(F.pair, 'metal').pop.rows.map(r => [r.k, r.v]), [['Metal', 'GF 14/20'], ['Metal choice', 'Gold']]);
  assert.deepEqual(chip(F.pair, 'purchased').pop.rows.map(r => [r.k, r.v]), [['Date', 'Monday, October 12, 2026'], ['Time', '11:02 AM · 3 hours ago']]);
  assert.deepEqual(chip(F.pair, 'ship').pop.rows.map(r => [r.k, r.v]), [['Date', 'Monday, October 19, 2026'], ['Time left', '7 days left']]);
  const num = chip(Object.assign({}, F.single, { order: { id: '4195000011', num: '88123' } }), 'order').pop.rows[1]; assert.deepEqual([num.k, num.v], ['Shop order number', '#88123']);
});
ok('B3 a matching pair: "1 pair" with the explanation "Matching: the Right is the mirror of the Left" and "1 pair · 2 pieces, Left and Right"', () => {
  const q = chip(F.pair, 'qty'); assert.equal(q.text, '1 pair'); assert.equal(q.icon, 'pair');
  assert.deepEqual(q.pop.rows.map(r => r.v), ['1 pair · 2 pieces, Left and Right', 'Matching: the Right is the mirror of the Left']);
});
ok('B4 a mismatched pair says so, with the intake\'s notes, and two pairs say "2 pairs · 4 pieces"', () => {
  const q = chip(F.mismatched, 'qty'); assert.equal(q.text, '2 pairs');
  assert.deepEqual(q.pop.rows.map(r => r.v), ['2 pairs · 4 pieces, Left and Right', 'Mismatched: the Left and the Right are different charms of one listing', 'two different designs']);
});
ok('B5 several of one charm is a quantity chip; counted discs say the discs', () => {
  assert.equal(chip(F.multi, 'qty').text, '×3'); assert.equal(chip(F.multi, 'qty').icon, 'quantity'); assert.deepEqual(chip(F.multi, 'qty').pop.rows.map(r => r.v), ['3']);
  assert.equal(chip(F.discs, 'qty').text, '2 discs'); assert.deepEqual(chip(F.discs, 'qty').pop.rows.map(r => r.v), ['1 · 2 discs']);
});
ok('B6 front + back engraving: a chip that says "Front + back", its card holds the engraving cell (words and seals) as the old grid drew it', () => {
  const e = chip(F.engraved, 'engraving'); assert.equal(e.text, 'Front + back'); assert.equal(e.icon, 'engrave'); assert.equal(e.tone, '');
  assert.match(OF.popHtml(e.pop), /Front · W<br>Back · 1\. 27\. 24/);
  assert.equal(chip(Object.assign({}, F.engraved, { engraving: { chip: 'Engraving', approved: true, html: '<span>approved</span>' } }), 'engraving').tone, 'ok', 'approved: the quiet green');
  assert.equal(ids(F.single).includes('engraving'), false);
});
ok('B7 a cancelled order is never alarmed by a past ship-by; a held order keeps its state pill and its sheet in the state card; a late one is red and says how late', () => {
  const s = chip(F.cancelled, 'ship'); assert.equal(s.tone, ''); assert.equal(s.text, 'Oct 9 · 3 days late'); assert.equal(s.pop.hint, '');
  assert.equal(F.cancelled.state.tone, 'bad'); assert.equal(chip(F.cancelled, 'state').pop.rows[0].v, 'cancelled');
  const st = chip(F.held, 'state').pop.rows.map(r => [r.k, r.v]); assert.deepEqual(st, [['State', 'held'], ['Sheet', 'GF Draft 1']]);
  assert.equal(chip(F.held, 'ship').tone, 'warn');
  const l = chip(F.late, 'ship'); assert.equal(l.tone, 'bad'); assert.equal(l.text, 'Oct 9 · 3 days late'); assert.equal(l.late, true); assert.deepEqual(l.pop.rows.map(r => [r.k, r.v]), [['Date', 'Friday, October 9, 2026'], ['Late by', '3 days']]); assert.match(l.pop.hint, /past its ship-by/);
});
ok('B8 a message from the buyer: a chip with the unread dot, a card with the buyer\'s words and the gift message, and a way to the Customer tab; a gift shows its mark on the buyer', () => {
  const m = chip(F.message, 'message'); assert.equal(m.text, 'Message'); assert.equal(m.dot, 'unread'); assert.equal(m.icon, 'message');
  assert.deepEqual(m.pop.secs.map(s => [s.k, s.t]), [['Message from the buyer', 'Could it arrive before the 20th? It is a surprise.'], ['Gift message', 'Happy birthday, love from all of us.']]);
  assert.match(m.pop.note, /1 new reply from the buyer in the Customer tab/); assert.deepEqual(m.pop.action, { id: 'customer', label: 'Open the Customer tab' });
  assert.equal(chip(F.message, 'buyer').gift, true); assert.match(chip(F.message, 'buyer').pop.rows.map(r => r.v).join('|'), /bought as a gift/);
  const read = chip(Object.assign({}, F.message, { message: { buyer: 'Hello', gift: '', unread: 0, bad: false } }), 'message'); assert.equal(read.dot, ''); assert.equal(read.pop.note, ''); assert.equal(read.pop.action, null);
  const bad = chip(Object.assign({}, F.message, { message: { buyer: '', gift: '', unread: 0, bad: true } }), 'message'); assert.equal(bad.dot, 'bad');
  assert.equal(ids(F.single).includes('message'), false);
});
ok('B9 no metal read: a clay "No metal" chip with no dot; a gift with no buyer name still says Gift; an order with no ship-by or purchase time has no chip for them', () => {
  const f = base({ rid: '4195000022', buyer: '', title: 'x' }, { buyer: { name: '', gift: true }, metal: { label: '', none: true, color: '', choices: [] }, ship: null, purchased: null });
  assert.deepEqual(ids(f), ['order', 'buyer', 'metal', 'state']);
  assert.equal(chip(f, 'buyer').text, 'Gift'); assert.equal(chip(f, 'metal').text, 'No metal'); assert.equal(chip(f, 'metal').tone, 'bad'); assert.equal(chip(f, 'metal').dot, '');
  assert.equal(chip(Object.assign({}, F.single, { purchased: { ts: at(0, 10, 0), label: 'Arrived' } }), 'purchased').pop.title, 'Arrived');
});

/* ── C · the drawn thing ── */
const paint = f => { OF.paint(host, f); return host; };
const bar = () => host.querySelector('.owfBar');
const chipEl = id => host.querySelector(`[data-owf-chip="${id}"]`);
const pop = () => host.querySelector('.owfPop');
const openId = () => { const p = pop(); return p && !p.hidden ? [...host.querySelectorAll('[data-owf-chip]')].find(c => c.getAttribute('aria-expanded') === 'true')?.dataset.owfChip || '' : ''; };
const ev = (el, type, init) => { const e = new w.MouseEvent(type, Object.assign({ bubbles: true, cancelable: true, view: w }, init || {})); Object.defineProperty(e, 'pointerType', { value: (init && init.pointerType) || 'mouse' }); el.dispatchEvent(e); return e; };
const kd = (el, key) => { const e = new w.KeyboardEvent('keydown', { key, bubbles: true, cancelable: true }); el.dispatchEvent(e); return e; };
let copied = [];
Object.defineProperty(w.navigator, 'clipboard', { value: { writeText: async t => { copied.push(t); } }, configurable: true });

ok('C1 the line is a toolbar of whole chips with an icon each, the state pill and the Details button at the end, and the cards are not drawn until asked for', () => {
  paint(F.pair);
  assert.equal(bar().getAttribute('role'), 'toolbar');
  const cs = [...bar().querySelectorAll('[data-owf-chip]')].map(c => c.dataset.owfChip); assert.deepEqual(cs, ['order', 'buyer', 'purchased', 'ship', 'metal', 'qty', 'state']);
  for (const id of ['order', 'buyer', 'purchased', 'ship', 'metal', 'qty']) assert(chipEl(id).querySelector('svg.ic'), id + ' has its icon');
  assert(bar().querySelector('.owfEnd [data-owf-chip=state]') && bar().querySelector('.owfEnd .owfMore'), 'state and Details at the end');
  assert.equal(pop().hidden, true); assert.equal(chipEl('order').getAttribute('aria-expanded'), 'false'); assert.equal(chipEl('order').getAttribute('aria-haspopup'), 'true');
  assert.equal(chipEl('ship').className.includes('ok'), true); assert.equal(chipEl('metal').querySelector('.owfDotM').getAttribute('style'), '--mc:#c8a24e');
  assert.equal(chipEl('order').getAttribute('aria-label'), 'Order 4195000012, press to copy the number');
  assert.equal(host.querySelector('.owfDetails').hidden, true);
  assert.equal(host.querySelectorAll('[data-owf-item][tabindex="0"]').length, 1, 'one Tab stop for the line');
});
okA('C2 a chip that rests 350 ms opens its card, leaving closes it; a card is click-through while it is only a peek; the card follows its chip in the Tab order', async () => {
  paint(F.pair);
  ev(chipEl('buyer'), 'pointerover'); await wait(200); assert.equal(openId(), '', 'not yet at 200 ms');
  await wait(220); assert.equal(openId(), 'buyer', 'open at about 350 ms');
  assert.equal(pop().previousElementSibling, chipEl('buyer'), 'the card is the very next thing after its chip');
  assert(pop().classList.contains('peek'), 'a peek is click-through'); assert.equal(chipEl('buyer').getAttribute('aria-controls'), pop().id);
  assert.match(pop().textContent, /Birch Sample/);
  ev(chipEl('buyer'), 'pointerout', { relatedTarget: d.body }); await wait(260); assert.equal(openId(), '', 'closed after the pointer left');
  // a quick pass over a chip opens nothing
  ev(chipEl('metal'), 'pointerover'); await wait(120); ev(chipEl('metal'), 'pointerout', { relatedTarget: d.body }); await wait(450); assert.equal(openId(), '');
  // with a card open, the next chip takes over after a beat, not after the full rest
  ev(chipEl('buyer'), 'pointerover'); await wait(420); assert.equal(openId(), 'buyer');
  ev(chipEl('buyer'), 'pointerout', { relatedTarget: chipEl('metal') }); ev(chipEl('metal'), 'pointerover', { relatedTarget: chipEl('buyer') }); await wait(170); assert.equal(openId(), 'metal');
  ev(chipEl('metal'), 'pointerout', { relatedTarget: d.body }); await wait(260);
});
okA('C3 a press pins the card (a tap too), a second press puts it away, Esc closes it and says it was the Esc, a press outside closes it, and touch has no hover', async () => {
  paint(F.pair);
  ev(chipEl('qty'), 'pointerover', { pointerType: 'touch' }); await wait(450); assert.equal(openId(), '', 'touch has no hover');
  chipEl('qty').click(); assert.equal(openId(), 'qty'); assert.equal(pop().classList.contains('peek'), false, 'a pinned card takes presses');
  assert.match(pop().textContent, /Matching: the Right is the mirror of the Left/); assert.match(pop().textContent, /1 pair · 2 pieces, Left and Right/);
  chipEl('qty').click(); assert.equal(openId(), '', 'a second press puts it away');
  chipEl('qty').click(); assert.equal(openId(), 'qty'); const e = kd(d.body, 'Escape'); assert.equal(e.defaultPrevented, true); assert.equal(openId(), '');
  chipEl('ship').click(); assert.equal(openId(), 'ship'); ev(d.body, 'pointerdown'); assert.equal(openId(), '', 'a press outside closes it');
  chipEl('ship').click(); ev(host.querySelector('.owfBar'), 'pointerdown'); assert.equal(openId(), '', 'a press on the empty line closes it too');
  // OrderFacts.escape: the window's Esc puts a card away first (true), and then lets the window go (false)
  chipEl('metal').click(); assert.equal(OF.escape(d.body), true); assert.equal(openId(), ''); assert.equal(OF.escape(d.body), false);
  // focus leaving closes it
  chipEl('metal').click(); chipEl('metal').focus(); chipEl('metal').blur(); assert.equal(openId(), '', 'blur closes it');
});
ok('C4 the keyboard: the arrows walk the line (one Tab stop), a focused chip opens its card, Enter pins it', () => {
  paint(F.pair); d.body.dispatchEvent(new w.Event('pointerdown'));
  const first = chipEl('order'); first.focus();
  assert.equal(openId(), 'order', 'keyboard focus opens the card'); assert.equal(host.querySelector('[data-owf-item][tabindex="0"]'), first);
  kd(first, 'ArrowRight'); assert.equal(d.activeElement, chipEl('buyer')); assert.equal(openId(), 'buyer'); assert.equal(chipEl('buyer').tabIndex, 0); assert.equal(first.tabIndex, -1);
  kd(d.activeElement, 'End'); assert.equal(d.activeElement.classList.contains('owfMore'), true); kd(d.activeElement, 'Home'); assert.equal(d.activeElement, first);
  kd(d.activeElement, 'ArrowLeft'); assert.equal(d.activeElement.classList.contains('owfMore'), true, 'it wraps round');
  chipEl('ship').focus(); chipEl('ship').click(); assert.equal(openId(), 'ship'); kd(chipEl('ship'), 'Escape'); assert.equal(openId(), ''); assert.equal(d.activeElement, chipEl('ship'), 'Esc gives the focus back to the chip');
  chipEl('ship').blur();
});
okA('C5 the order number copies on one press, the chip shows the check, the card stays up to show it, and the card\'s own copy buttons copy the listing', async () => {
  paint(F.pair); copied = [];
  chipEl('order').click(); await wait(90);
  assert.deepEqual(copied, ['4195000012']); assert(chipEl('order').classList.contains('copied'), 'the check'); assert.equal(openId(), 'order'); assert.match(host.querySelector('.owfLive').textContent, /Copied/);
  const btns = [...pop().querySelectorAll('[data-copy]')]; assert.deepEqual(btns.map(b => b.dataset.copy), ['4195000012', '1737560072']);
  btns[1].click(); await wait(90); assert.deepEqual(copied, ['4195000012', '1737560072']); assert(btns[1].classList.contains('copied'));
  await wait(1600); assert.equal(chipEl('order').classList.contains('copied'), false, 'the check goes back after a moment');
  chipEl('order').click(); assert.equal(openId(), 'order', 'pressing the number again copies again and keeps the card'); await wait(90);
  ev(d.body, 'pointerdown');
  // a clipboard that refuses says so, and no check
  Object.defineProperty(w.navigator, 'clipboard', { value: { writeText: async () => { throw new Error('no'); } }, configurable: true });
  w.document.execCommand = () => false; chipEl('order').click(); await wait(90); assert(chipEl('order').classList.contains('failed')); assert.equal(chipEl('order').classList.contains('copied'), false); assert.match(host.querySelector('.owfLive').textContent, /Could not copy/);
  Object.defineProperty(w.navigator, 'clipboard', { value: { writeText: async t => { copied.push(t); } }, configurable: true }); ev(d.body, 'pointerdown');
});
ok('C6 Details is a drop-down: closed at first, one press opens the whole grid, a second closes it; it keeps its state across repaints and collapse() shuts it', () => {
  paint(F.pair); const more = host.querySelector('.owfMore'), det = host.querySelector('.owfDetails');
  assert.equal(more.getAttribute('aria-expanded'), 'false'); assert.equal(det.hidden, true); assert.equal(more.getAttribute('aria-controls'), det.id);
  more.click(); assert.equal(more.getAttribute('aria-expanded'), 'true'); assert.equal(det.hidden, false); assert.equal(OF.details(host), true);
  paint(Object.assign({}, F.pair, { state: { tone: 'ok', text: 'nested', sheet: 'GF Sheet 1' } }));
  assert.equal(host.querySelector('.owfDetails').hidden, false, 'a repaint that changes something leaves Details open'); assert.equal(host.querySelector('.owfMore').getAttribute('aria-expanded'), 'true');
  host.querySelector('.owfMore').click(); assert.equal(host.querySelector('.owfDetails').hidden, true);
  host.querySelector('.owfMore').click(); OF.collapse(host); assert.equal(host.querySelector('.owfDetails').hidden, true); assert.equal(OF.details(host), false);
});
ok('C7 a repaint that changes nothing touches nothing (the same nodes, the card and the focus where they were); one that changes the chip a card belongs to keeps the card open on the new words', () => {
  paint(F.pair); const before = chipEl('ship'); chipEl('ship').click(); const p0 = pop(); chipEl('ship').focus();
  paint(Object.assign({}, F.pair)); assert.equal(chipEl('ship'), before, 'the same node'); assert.equal(openId(), 'ship'); assert.equal(d.activeElement, before);
  const late = Object.assign({}, F.pair, { ship: { by: Math.floor(at(-2, 17, 0) / 1000), quiet: false } });
  paint(late); assert.equal(openId(), 'ship', 'the card stays open across a repaint'); assert.match(pop().textContent, /Late by/); assert.equal(pop(), p0, 'one card for the window'); assert.equal(d.activeElement, chipEl('ship'), 'the focus is where it was');
  assert(chipEl('ship').className.includes('bad')); chipEl('ship').blur(); ev(d.body, 'pointerdown');
  // the chip a card belongs to goes away: the card goes with it
  chipEl('qty').click(); paint(F.single); assert.equal(openId(), '', 'no chip, no card'); assert.equal(pop().hidden, true);
});
ok('C8 an order still being read: its number (copyable) and what is known, and skeleton chips where the rest is coming; no Details yet', () => {
  paint({ loading: true, key: 'k', order: { id: '4195000012', num: '' }, buyer: { name: 'Birch Sample', gift: false }, cells: [], on: {} });
  assert(host.classList.contains('loading')); assert.equal(chipEl('order').dataset.copyval, '4195000012'); assert.equal(chipEl('buyer').textContent.trim(), 'Birch Sample');
  assert.equal(host.querySelectorAll('.owfSk.owSk').length, 3); assert.equal(host.querySelector('.owfMore'), null); assert.equal(host.querySelector('[data-owf-chip=ship]'), null);
  paint(F.pair); assert.equal(host.classList.contains('loading'), false); assert.equal(host.querySelectorAll('.owSk').length, 0); assert(host.querySelector('.owfMore'));
});
ok('C9 words that came from outside are text, never markup', () => {
  const evil = '<img src=x onerror="window.__pwn=1"> & "quotes"';
  paint(base({ rid: '4195000023', buyer: evil, title: evil }, { buyer: { name: evil, gift: false }, listing: { id: '1', title: evil }, message: { buyer: evil, gift: evil, unread: 0, bad: false }, cells: [{ label: 'Buyer', text: evil }, { label: 'Title', text: evil }] }));
  assert.equal(host.querySelector('img'), null); assert.equal(w.__pwn, undefined);
  assert.equal(chipEl('buyer').textContent.trim(), evil); chipEl('message').click(); assert.equal(pop().querySelector('img'), null); assert.match(pop().textContent, /onerror/);
  assert.equal(host.querySelector('.owfList .m span').textContent, evil);
  ev(d.body, 'pointerdown');
});
ok('C10 the Customer-tab button is the page\'s own action, and puts the card away', () => {
  let went = 0; paint(Object.assign({}, F.message, { on: { customer: () => { went++; } } }));
  chipEl('message').click(); const b = pop().querySelector('[data-act=customer]'); assert(b); b.click(); assert.equal(went, 1); assert.equal(openId(), '');
});

/* ── D · nothing is lost ── */
ok('D1 Details holds every cell of the old grid, in its order, as .m cells with <i> label and <span> words (the old tests\' own selectors), the engraving cell with its markup', () => {
  paint(F.engraved);
  const cells = [...host.querySelectorAll('.owfDetails .m')].map(m => [m.querySelector('i').textContent, m.querySelector('span') && m.querySelector('span').textContent]);
  assert.deepEqual(cells.map(c => c[0]), ['Order', 'Purchased', 'Buyer', 'Quantity', 'Metal', 'State', 'Ship by', 'Engraving', 'Listing', 'Title']);
  assert.equal(cells[0][1], '4195000015'); assert.equal(cells[9][1], 'Mini tag necklace');
  const eng = [...host.querySelectorAll('.owfDetails .m')].find(m => m.querySelector('i').textContent === 'Engraving'); assert.match(eng.querySelector('span').innerHTML, /Front · W<br>Back · 1\. 27\. 24/); assert(eng.classList.contains('block'));
  // copy buttons only where a number is worth copying
  assert.deepEqual([...host.querySelectorAll('.owfDetails [data-copy]')].map(b => b.dataset.copy), ['4195000015', '1737560072']);
  // a changed cell is drawn again (the sheet's records landing say a new Sheet cell)
  paint(Object.assign({}, F.engraved, { cells: F.engraved.cells.concat([{ label: 'Sheet', text: 'GF Sheet 1' }]) })); assert.equal([...host.querySelectorAll('.owfDetails .m i')].pop().textContent, 'Title' === 'x' ? '' : 'Sheet');
});
ok('D2 the stylesheet adds no colour the page did not have: every literal colour in the facts CSS is one the rest of the page already uses', () => {
  const html = read('charm-nest-1.html');
  const a = html.indexOf('/* ── the order\'s facts'), b = html.indexOf('/* an order still being read: a still block'); assert(a > 0 && b > a, 'the facts CSS block');
  const mine = html.slice(a, b), rest = html.slice(0, a) + html.slice(b) + read('charm-nest-rail-tip.js') + read('charm-nest-bridge.js');   // (the dot cards' own shadow is in charm-nest-rail-tip.js)
  const colours = s => new Set((s.match(/#[0-9a-fA-F]{3,8}\b|rgba?\([^)]*\)/g) || []).map(x => x.replace(/\s+/g, '').toLowerCase()));
  const had = colours(rest), fresh = [...colours(mine)].filter(c => !had.has(c));
  assert.deepEqual(fresh, [], 'new colours: ' + fresh.join(' '));
});
ok('D3 the page loads the module before the bridge and the build ships it; the bridge asks it for the facts, the Esc and the fresh window', () => {
  const html = read('charm-nest-1.html'), br = read('charm-nest-bridge.js');
  assert(html.indexOf('charm-nest-order-facts.js') > 0 && html.indexOf('charm-nest-order-facts.js') < html.indexOf('charm-nest-bridge.js'), 'script order');
  assert(read('scripts/build-public.cjs').includes('"charm-nest-order-facts.js"'), 'in the public manifest');
  assert(/OrderFacts\.paint\(host, f\)/.test(br) && /OrderFacts\.escape\(W\.dlg\)/.test(br) && /OrderFacts\.collapse\(/.test(br));
  assert.equal(/byId\("owMeta"\)\.innerHTML\s*=/.test(br), false, 'the grid is no longer written box by box');
});

(async () => {
  for (const [name, fn] of later) { try { await fn(); n++; console.log('  ✓ ' + name); } catch (e) { e.message = `[${name}] ${e.message}`; throw e; } }
  console.log(`order-facts: ${n} checks passed`);
  dom.window.close();
})().catch(e => { console.error(e); process.exit(1); });
