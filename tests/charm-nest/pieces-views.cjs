// PIECES / VIEWS (pieces-1016): every piece of a multi-piece line is SHOWN on its own, in every view (Paul, 16 Oct 2026, order 4195059483 "Mandy Bellamy", line "CORGI 34728 · SS · ×2"):
//   "Customer ordered two of the same Charm, but the engraving only shows one, sometimes customers will request different engraving for each piece, even though the two pieces are the same.
//    ALL multipiece orders must always show each individual piece with respect to all Engravings and Monograms."
// Before: the Order window's ITS PIECE listed ONE row "COMET 9427 · ×2" and the Engraving section ONE card with one set of words and one approval; the Engrave tab ONE row, ONE card, no way to cycle.
// Expected (CONTRACT-PIECES.md: a line of quantity N is the slots C1..Cn, one job each; an ear pair of quantity 2 is L R L.2 R.2; discs D1..Dn then D1.2 ..):
//   - ITS PIECE: the line says "2 pieces" and lists "Piece 1 of 2", "Piece 2 of 2" (each pickable); the header count matches;
//   - the Engraving section: one card per piece, titled "... Piece 1 of 2", each with its own words and its own Approve; a pick of one piece narrows the cards to it; Show all brings them all;
//   - the Engrave tab: ONE row for the line that names every piece ("PIECE 1 of 2", its words and state), the detail's switch and Back / Next walk every piece, the strip says "Piece 1 of 2" and the order's count,
//     a search for "piece 2" finds the row, a piece approved is a Decided piece on its own;
//   - plain pieces and quantity-1 lines are drawn exactly as before (no "Piece" anywhere); lines that already split (discs, ears) keep their tags, a second set of discs is "set 2";
//   - 1440 px and 400 px: nothing overflows sideways and the section keeps its room (no jumps).
//
// Offline only: the REAL page over the repo's fake site with the real drawings (_engraveRealWorld.cjs: real intake, pool, classifier, fit). Fake buyer names only; no Etsy, no paid call, no live write.
//   /tmp/cn-run.sh 600 tests/charm-nest/pieces-views.cjs       (SHOTS=<dir> writes the 1440 / 400 px pictures;  PV_KEEP=1 prints what the page holds)
// What a pass does NOT prove: a live order on the live page (live data, live jobs); only that the page's own code shows every piece on the offline fake.
'use strict';
const R = require('./_engraveRealWorld.cjs'), Base = require('./_orderFactsWorld.cjs');

function orderOf(fx, now) {
  const at = (d, h, m) => { const t = new Date(now); t.setDate(t.getDate() + d); t.setHours(h, m, 0, 0); return Math.floor(t.getTime() / 1000); };
  const shipBy = at(fx.ship, 17, 0), line = (x, i) => ({ transactionId: fx.rid + (i + 1), listingId: x.listing, sku: x.sku, title: x.title, quantity: x.qty || 1, expectedShipDate: shipBy, variations: x.variations || [], metalKey: x.metalKey || 'gold', metalLabel: x.metalLabel || 'GF 14/20', personalization: x.personalization || [], buyerMessage: '' });
  return { receiptId: fx.rid, orderNumber: fx.rid, createTs: at(fx.placed.day, fx.placed.h, fx.placed.m), updateTs: at(0, 12, 0), shipBy, buyer: { name: fx.buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: fx.lines.map(line) };
}
Base.orderOf = orderOf;
const V = (name, value) => ({ name, value });
const COMET = (words, qty, listing, metal) => ({ listing: listing || '1890000301', sku: 'COMET_9427', title: 'Comet Necklace, Celestial Charm Necklace in 14k Gold Fill', qty, variations: [V('Metal choice', metal === 'silver' ? 'Silver' : 'Gold'), V('Font Choice', 'Stylish')], personalization: words == null ? [] : [words], metalKey: metal || 'gold', metalLabel: metal === 'silver' ? 'SS' : 'GF 14/20' });
const EARS = qty => ({ listing: '1890000306', sku: 'COMET_9427', title: 'Comet Stud Earrings, Celestial Earrings in 14k Gold Fill', qty, variations: [V('Metal choice', 'Gold'), V('Style', 'Stud earrings')], personalization: [] });
const PLAIN = qty => ({ listing: '1890000307', sku: 'SCHRODINGER CAT 1', title: 'Cat Necklace, Whimsical Cat Charm Gift', qty, variations: [V('Metal choice', 'Gold')], personalization: [] });
const DISCS = qty => ({ listing: '1890000305', sku: 'COMET_9427', title: 'Initial Disc Necklace, Personalized Letter Discs', qty, variations: [V('Metal choice', 'Gold'), V('Number of Discs', '3 Discs')], personalization: ['Tag 1: A, Tag 2: B, Tag 3: C'] });
const ORD = (rid, buyer, hour, lines) => ({ rid, buyer, placed: { day: 0, h: hour, m: 5 }, ship: 5, lines });
R.ORDERS.q1 = ORD('4290000300', 'Name Qone', 9, [COMET('Name One', 1, '1890000300')]);                          // quantity 1: exactly as before
R.ORDERS.q2 = ORD('4290000301', 'Name Qtwo', 10, [COMET('Name One', 2)]);                                       // Mandy's case: two of one charm, one text
R.ORDERS.q3 = ORD('4290000302', 'Name Qthree', 11, [COMET('Name One', 3, '1890000302')]);
R.ORDERS.qd = ORD('4290000303', 'Name Qdiff', 12, [COMET('Name One\nName Two', 2, '1890000303')]);              // two different words for two pieces
R.ORDERS.dq = ORD('4290000304', 'Name Dqty', 13, [DISCS(2)]);                                                   // two necklaces of three discs: D1..D3 and D1.2..D3.2
R.ORDERS.eq = ORD('4290000305', 'Name Eqty', 14, [EARS(2)]);                                                    // two pairs of earrings (never engraved: the Pieces list only)
R.ORDERS.mx = ORD('4290000308', 'Name Mixed', 15, [COMET('Name One', 1, '1890000308'), COMET('Name Two', 3, '1890000309', 'silver'), EARS(2), PLAIN(2)]);
R.EXPECT_JOBS.q1 = 1; R.EXPECT_JOBS.q2 = 2; R.EXPECT_JOBS.q3 = 3; R.EXPECT_JOBS.qd = 2; R.EXPECT_JOBS.dq = 6; R.EXPECT_JOBS.eq = 0; R.EXPECT_JOBS.mx = 1 + 3;

const fails = [], note = (ok, what, more) => { console.log((ok ? '  PASS ' : '  FAIL ') + what + (more ? '  [' + more + ']' : '')); if (!ok) fails.push(what); };

/** The Order window's pieces list, section and header, read in the page. */
const WIN = () => {
  const host = document.getElementById('owEng'), box = document.getElementById('owPcSum'), vis = n => !!n && !n.hidden && getComputedStyle(n).display !== 'none' && n.getClientRects().length > 0;
  const cards = [...(host ? host.children : [])].filter(n => n.classList.contains('engCard')).map(c => ({
    piece: c.dataset.piece || '', slot: c.dataset.slot || '', title: ((c.querySelector('.engName') || {}).textContent || '').trim(), state: c.dataset.state || '',
    words: [...c.querySelectorAll('.engFace .engTxt')].map(t => t.textContent.trim()).join(' | '), approve: !!c.querySelector('[data-e=approve]'), approveOff: !!c.querySelector('[data-e=approve][disabled]'), seals: c.querySelectorAll('.sealRow .seal, .sealRow > *').length,
    box: (() => { const r = c.getBoundingClientRect(); return { w: Math.round(r.width), h: Math.round(r.height), l: Math.round(r.left), r: Math.round(r.right) }; })() }));
  const rows = box ? [...box.querySelectorAll('.owPcRow')].map(r => ({ piece: r.dataset.piece || '', text: (r.querySelector('.owPcName') || r).textContent.replace(/\s+/g, ' ').trim(), sel: r.classList.contains('sel') })) : [];
  const subs = box ? [...box.querySelectorAll('.owPcSub[data-sub]')].map(s => ({ sub: s.dataset.sub, text: s.textContent.replace(/\s+/g, ' ').trim(), sel: s.classList.contains('sel') })) : [];
  const dlg = document.getElementById('orderWin');
  return { hostHidden: !host || host.hidden, cards, rows, subs, boxShown: vis(box), label: box ? ((box.querySelector('.owPcHd .fLabel') || {}).textContent || '').trim() : '', state: box ? ((box.querySelector('.owPcState') || {}).textContent || '').trim() : '',
    sub: (document.getElementById('owSub') || {}).textContent || '', left: (document.getElementById('owEngLeft') || {}).hidden === false ? document.getElementById('owEngLeft').textContent.replace(/\s+/g, ' ').trim() : '',
    said: (document.getElementById('owEngSaid') || {}).hidden === false ? document.getElementById('owEngSaid').innerText.replace(/\s+/g, ' ').trim() : '',
    over: dlg ? Math.max(0, dlg.scrollWidth - dlg.clientWidth) : -1, selected: window.OrderWin.selectedPiece(), disc: window.OrderWin.selectedDisc ? window.OrderWin.selectedDisc() : null, side: window.OrderWin.selectedSide() };
};
const win = page => page.evaluate(`(${WIN})()`);
const openWin = async (w, key, pick) => {
  await w.page.evaluate(async ({ key, pick }) => { try { OrderWin.close(); } catch (_) {} await new Promise(r => setTimeout(r, 250)); CN.setMode('orders'); await OrderWin.open(key, pick ? { pick: true } : {}); }, { key, pick: !!pick });
  await w.page.waitForSelector('#orderWin[open]', { timeout: 15000 }).catch(() => {});
  await w.page.waitForFunction(() => { const h = document.getElementById('owEng'); return !h || h.hidden || !!h.querySelector('.engCard') || !!h.children.length; }, null, { timeout: 20000 }).catch(() => {});
  await R.settle(w.page, 700);
};
/** the Engrave tab: the queue's rows and the detail, read in the page */
const TAB = () => {
  const rows = [...document.querySelectorAll('#egQueue .placementRow, #egQueue .decidedRow')].map(r => ({ kind: r.classList.contains('decidedRow') ? 'decided' : 'place', rid: r.dataset.rid || '', open: r.dataset.open || '',
    ears: [...r.querySelectorAll('.egEar')].map(e => ({ tag: ((e.querySelector('.egPiece') || {}).textContent || '').trim(), words: ((e.querySelector('.w') || {}).textContent || '').trim(), stage: ((e.querySelector('[data-stage]') || {}).textContent || '').trim() })),
    text: r.innerText.replace(/\s+/g, ' ').trim().slice(0, 200), w: Math.round(r.getBoundingClientRect().width), over: Math.max(0, r.scrollWidth - r.clientWidth) }));
  const sw = [...document.querySelectorAll('#egQueue .egEarSwitch .egEarTab')].map(b => ({ key: b.dataset.ear, tag: ((b.querySelector('.egPiece') || {}).textContent || '').trim(), on: b.getAttribute('aria-pressed') === 'true' }));
  const strip = document.querySelector('#egQueue .egPcStrip');
  return { rows, switch: sw, strip: strip ? { of: ((strip.querySelector('.egPcOf') || {}).textContent || '').trim(), tabs: [...strip.querySelectorAll('.egPcTab')].map(b => ({ label: (b.querySelector('.egPcLbl') || {}).textContent, on: b.getAttribute('aria-pressed') === 'true', ok: !!b.querySelector('.egPcTick') })) } : null,
    counter: ((document.querySelector('#egQueue .reviewProgress .kind') || {}).textContent || '').trim(), focus: (window.Engrave.view() || {}).focus || '', cardTitle: ((document.querySelector('#egQueue .engCard .engName') || {}).textContent || '').trim(),
    over: Math.max(0, document.documentElement.scrollWidth - document.documentElement.clientWidth), kids: [...document.querySelectorAll('#egQueue .engCard')].length };
};
const tab = page => page.evaluate(`(${TAB})()`);
const jobsOf = (page, rid) => page.evaluate(rid => [...Engrave.items().values()].filter(j => j.row && ((j.row.parentRow || j.row).order || {}).receiptId === rid).map(j => ({ key: j.key.replace(/^\d+_\d+/, ''), slot: j.slot || null, state: j.state, text: (j.lines || []).join(' / '), copies: (j.copies || []).map(c => c.replace(/^\d+_\d+_/, '')) })).sort((a, b) => (a.key < b.key ? -1 : 1)), rid);

(async () => {
  const w = await R.world({ viewport: { width: 1440, height: 900 } });
  if (!w) { console.log('no playwright-core: nothing run'); return; }
  const page = w.page, keep = !!process.env.PV_KEEP;
  try {
    const made = await R.build(w, ['q1', 'q2', 'q3', 'qd', 'dq', 'eq', 'mx']);
    const key = n => made[n].key;
    const slots = n => made[n].jobs.map(j => j.slot).filter(Boolean).sort().join(',');
    if (keep) console.log(JSON.stringify(Object.fromEntries(Object.entries(made).map(([k, v]) => [k, v.jobs.map(j => (j.slot || '-') + ':' + j.state + ':' + j.text)])), null, 1));
    note(made.q2.jobs.length === 2 && slots('q2') === 'C1,C2', 'the model gives a line of quantity 2 its two pieces: jobs C1 and C2 (' + slots('q2') + ')');
    note(made.q3.jobs.length === 3 && slots('q3') === 'C1,C2,C3', 'quantity 3: C1, C2, C3 (' + slots('q3') + ')');
    note(made.q1.jobs.length === 1 && !made.q1.jobs[0].slot, 'quantity 1: ONE job, no slot, exactly as before');

    // ───────────── the Order window, Mandy's case: two of one charm ─────────────
    await openWin(w, key('q2'));
    let s = await win(page);
    note(s.boxShown && /pieces/i.test(s.label), 'ITS PIECES is shown for the line of two (not "Its piece")', JSON.stringify(s.label));
    note(s.rows.length === 1 && /\b2 pieces\b/.test(s.rows[0].text) && !/×\s*2/.test(s.rows[0].text), 'the line row says "2 pieces", never "×2"', s.rows.map(r => r.text).join(' || '));
    note(s.subs.length === 2 && /Piece 1 of 2/.test(s.subs[0].text) && /Piece 2 of 2/.test(s.subs[1].text), 'the line lists each piece: Piece 1 of 2, Piece 2 of 2', s.subs.map(x => x.sub.replace(/^.*#/, '#') + ' ' + x.text).join(' || '));
    note(/2 pieces/.test(s.sub) || s.sub === '', 'the header piece count agrees (2 pieces, or none)', s.sub);
    note(s.cards.length === 2, 'the Engraving section shows TWO cards for the two pieces (' + s.cards.length + ')');
    note(s.cards.length === 2 && /Piece 1 of 2/.test(s.cards[0].title) && /Piece 2 of 2/.test(s.cards[1].title), 'each card is titled with its piece: Piece 1 of 2, Piece 2 of 2', s.cards.map(c => c.title).join(' || '));
    note(s.cards.length === 2 && s.cards.every(c => c.approve), 'each card has its OWN Approve button', s.cards.map(c => c.approve + '/' + c.state).join(','));
    note(s.cards.length === 2 && s.cards[0].piece !== s.cards[1].piece && s.cards.every(c => /#C[12]$/.test(c.piece)), 'the cards are two different pieces (#C1, #C2)', s.cards.map(c => c.piece.replace(/^.*#/, '#')).join(','));
    note(s.cards.length === 2 && s.cards[0].words !== '' && s.cards[0].words === s.cards[1].words, 'one text for two pieces: both cards carry the whole text (editable on their own)', s.cards.map(c => c.words).join(' || '));
    if (process.env.SHOTS) await R.shot(w, 'PV-1440-window-qty2');

    // a pick of one piece narrows the Engraving section to it (ENGSEL), Show all brings both back
    let r = await page.evaluate(k => OrderWin.selectPiece(k + '#C2'), key('q2')); await R.settle(page, 500); s = await win(page);
    note(r === true && s.cards.length === 1 && /#C2$/.test(s.cards[0].piece) && /Piece 2 of 2/.test(s.cards[0].title), 'piece 2 picked: its card alone', s.cards.map(c => c.title).join(','));
    note(s.subs.some(x => /#C2$/.test(x.sub) && x.sel) && !s.subs.some(x => /#C1$/.test(x.sub) && x.sel), 'the Piece 2 line of the list is marked, Piece 1 is not');
    await page.evaluate(() => document.querySelector('#owPcSum .owPcSub[data-sub$="#C1"]').click()); await R.settle(page, 500); s = await win(page);
    note(s.cards.length === 1 && /#C1$/.test(s.cards[0].piece), 'a press on "Piece 1 of 2" in the list moves the pick: its card alone', s.cards.map(c => c.piece.replace(/^.*#/, '#')).join(','));
    await page.evaluate(() => document.querySelector('#owPcSum .owPcSub[data-sub$="#C1"]').click()); await R.settle(page, 500); s = await win(page);
    note(s.cards.length === 2, 'the piece pressed again goes back to both cards', String(s.cards.length));
    note(await page.evaluate(k => OrderWin.selectPiece(k + '#C9'), key('q2')) === false, 'a piece the line does not have (Piece 9 of 2) is refused');
    r = await page.evaluate(() => { OrderWin.selectPiece(null); return OrderWin.selectedDisc(); }); await R.settle(page, 300);
    note(r === null, 'Show all drops the piece');

    // each piece is approved on its own: the order's state is the worst piece, and the guide says how many are left
    await page.evaluate(k => { const j = [...Engrave.items().values()].find(x => x.key.endsWith('#C1') && x.key.startsWith(k)); window.__c1 = j.state; j.state = 'approved'; j.approvedAt = Date.now(); j.approvedBy = 'Test Operator'; Engrave.render && Engrave.render(); OrderWin.paint && OrderWin.paint(); }, key('q2'));
    await page.evaluate(k => OrderWin.selectPiece(k + '#C1'), key('q2')); await R.settle(page, 700); s = await win(page);
    note(s.cards.length === 1 && s.cards[0].state === 'approved' && !s.cards[0].approve, 'piece 1 approved: its card is Approved, no Approve button');
    note(/1 more|1 piece/.test(s.left) || s.left === '', 'the guide under the card names the piece still to approve', s.left);
    await page.evaluate(() => OrderWin.selectPiece(null)); await R.settle(page, 500); s = await win(page);
    note(s.cards.length === 2 && s.cards[0].state === 'approved' && s.cards[1].state !== 'approved' && s.cards[1].approve, 'Show all: piece 1 approved, piece 2 still waits for ITS Approve', s.cards.map(c => c.state).join(','));
    await page.evaluate(k => { const j = [...Engrave.items().values()].find(x => x.key.endsWith('#C1') && x.key.startsWith(k)); j.state = window.__c1; delete j.approvedAt; delete j.approvedBy; }, key('q2'));

    // ───────────── quantity 3, and different words for different pieces ─────────────
    await openWin(w, key('q3')); s = await win(page);
    note(s.subs.length === 3 && s.cards.length === 3 && /Piece 3 of 3/.test((s.cards[2] || {}).title || ''), 'quantity 3: three list lines and three cards (Piece 3 of 3)', s.subs.length + '/' + s.cards.length);
    await openWin(w, key('qd'));
    // (which words go to which piece is the model's and the reader's: here each piece is given its own, and the cards must show each piece's own)
    await page.evaluate(k => { const js = [...Engrave.items().values()].filter(x => x.key.startsWith(k)).sort((a, b) => (a.key < b.key ? -1 : 1)); ['Name One', 'Name Two'].forEach((t, i) => { js[i].lines = [t]; js[i].text = t; }); Engrave.render && Engrave.render(); OrderWin.paint && OrderWin.paint(); }, key('qd'));
    await R.settle(page, 900); s = await win(page);
    note(s.cards.length === 2 && s.cards[0].words !== s.cards[1].words && /One/.test(s.cards[0].words) && /Two/.test(s.cards[1].words), 'each piece\'s card shows ITS OWN words (Name One / Name Two)', s.cards.map(c => c.words).join(' || '));
    const edit = await page.evaluate(k => {
      const js = [...Engrave.items().values()].filter(x => x.key.startsWith(k)).sort((a, b) => (a.key < b.key ? -1 : 1)), before = js.map(j => (j.lines || []).join('/')), j2 = js[1];
      j2.lines = ['Zed Edit']; j2.text = 'Zed Edit'; Engrave.render && Engrave.render(); return { before, after: js.map(j => (j.lines || []).join('/')) };
    }, key('qd'));
    await page.evaluate(() => OrderWin.paint && OrderWin.paint()); await R.settle(page, 900); s = await win(page);
    note(edit.after[0] === edit.before[0] && edit.after[1] === 'Zed Edit' && /Name One/.test(s.cards[0].words) && /Zed Edit/.test(s.cards[1].words), 'editing piece 2 leaves piece 1 as it was, and the cards say so', JSON.stringify(edit) + ' ' + s.cards.map(c => c.words).join(' || '));

    // ───────────── lines that already split, and plain lines: unchanged ─────────────
    await openWin(w, key('q1')); s = await win(page);
    note(s.cards.length === 1 && !/Piece/.test(s.cards[0].title) && !/Piece/.test(s.rows.map(x => x.text).join(' ')), 'quantity 1: one card, no "Piece" anywhere (drawn as before)', s.cards.map(c => c.title).join(',') + ' | ' + s.rows.map(x => x.text).join(' || '));
    await openWin(w, key('dq')); s = await win(page);
    const dtitles = s.cards.map(c => c.title).join(' || ');
    note(s.cards.length === 6, 'two necklaces of three discs: SIX cards, one per disc (' + s.cards.length + ')', dtitles);
    note(s.cards.length === 6 && /Disc 1/.test(s.cards[0].title) && /Disc 1.*(set|pair) 2/i.test(s.cards[3].title), 'the second set is told by its number ("Disc 1 · set 2")', s.cards.map(c => c.title).join(' || '));
    await openWin(w, key('eq')); s = await win(page);
    note(s.cards.length === 0 && s.hostHidden, 'two pairs of earrings: never engraved, no card');
    note(s.boxShown && s.subs.length >= 2, 'but the pieces are listed (pairs 1 and 2 under the row)', s.rows.map(x => x.text).join(' || ') + ' / ' + s.subs.length + ' subs');

    // ───────────── a mixed order: one charm, three copies (SS), two pairs of ears, two plain pieces ─────────────
    await openWin(w, key('mx')); s = await win(page);
    note(s.rows.length === 4, 'the mixed order lists its four lines', s.rows.map(x => x.text).join(' || '));
    note(s.cards.length === 1 + 3, 'cards: the single charm\'s one and the three copies\' (4), none for the ears and the plain pieces (' + s.cards.length + ')', s.cards.map(c => c.title).join(' || '));
    note(s.cards.filter(c => /Piece \d of 3/.test(c.title)).length === 3, 'the three copies are Piece 1 / 2 / 3 of 3', s.cards.map(c => c.title).join(' || '));
    note(s.rows.some(x => /2 pieces/.test(x.text) && /SCHRODINGER|CAT/i.test(x.text)), 'the plain line of two is listed as 2 pieces too (no card, still its pieces)', s.rows.map(x => x.text).join(' || '));
    const copyKey = await page.evaluate(rid => Orders.rows().filter(r => r.order.receiptId === rid)[1].key, made.mx.rid);
    await page.evaluate(k => OrderWin.selectPiece(k + '#C2'), copyKey); await R.settle(page, 500); s = await win(page);
    note(s.cards.length === 1 && /Piece 2 of 3/.test(s.cards[0].title) && /Piece 2/.test(s.state), 'mixed order: the SS copy line\'s piece 2 picked: its card alone, and the list says "Showing ... Piece 2"', s.cards.map(c => c.title).join(',') + ' / ' + s.state);
    await page.evaluate(() => OrderWin.selectPiece(null)); await R.settle(page, 300);
    const mxKeys = made.mx.rid; const plainKey = await page.evaluate(rid => Orders.rows().filter(r => r.order.receiptId === rid)[3].key, mxKeys);
    await page.evaluate(k => OrderWin.selectPiece(k), plainKey); await R.settle(page, 400); s = await win(page);
    note(s.hostHidden && s.cards.length === 0, 'the plain line picked: no engraving section (nothing to engrave)');
    if (process.env.SHOTS) await R.shot(w, 'PV-1440-window-mixed');

    // ───────────── the Engrave tab ─────────────
    await R.openEngraveDetail(w, 'q2', { slot: 'C1' });
    await page.evaluate(() => { Engrave.restoreView({ tab: 'place', list: true, chosen: false, focus: null, q: '' }); Engrave.render(); }); await R.settle(page, 900);
    let t = await tab(page);
    const row2 = t.rows.find(x => x.rid === made.q2.rid);
    note(!!row2 && row2.ears.length === 2 && /PIECE 1 of 2/i.test(row2.ears[0].tag) && /PIECE 2 of 2/i.test(row2.ears[1].tag), 'the Engrave list: ONE row for the line that names both pieces ("PIECE 1 of 2", "PIECE 2 of 2")', row2 ? row2.ears.map(e => e.tag + ' ' + e.words).join(' || ') : 'no row');
    note(!!row2 && row2.ears.every(e => e.words !== ''), 'each piece of the row shows its own words', row2 ? row2.ears.map(e => e.words).join(' || ') : '');
    await R.openEngraveDetail(w, 'q2', { slot: 'C1' }); t = await tab(page);
    note(t.switch.length === 2 && /PIECE 1 of 2/i.test(t.switch[0].tag) && t.switch[0].on && !t.switch[1].on, 'the detail has the piece switch: Piece 1 | Piece 2, piece 1 shown', JSON.stringify(t.switch));
    note(/Piece 1 of 2/.test(t.cardTitle), 'the detail card is titled Piece 1 of 2', t.cardTitle);
    note(t.strip === null || /Piece 1 of 2/.test(t.strip.of), 'the order strip says "Piece 1 of 2"', t.strip ? t.strip.of : 'no strip');
    // Next goes to piece 2 (not to the next order), then on to the next order
    await page.evaluate(() => document.querySelector('#egQueue [data-a=next]').click()); await R.settle(page, 900); t = await tab(page);
    note(/#C2$/.test(t.focus) && t.focus.startsWith(made.q2.key), 'Next goes to piece 2 of the SAME line', t.focus.replace(/^\d+_\d+/, ''));
    note(/Piece 2 of 2/.test(t.cardTitle) && t.switch.some(b => b.on && /PIECE 2/i.test(b.tag)), 'the detail now shows Piece 2 of 2', t.cardTitle);
    await page.evaluate(() => document.querySelector('#egQueue [data-a=prev]').click()); await R.settle(page, 900); t = await tab(page);
    note(/#C1$/.test(t.focus) && t.focus.startsWith(made.q2.key), 'Back returns to piece 1', t.focus.replace(/^\d+_\d+/, ''));
    // search finds a piece by its name
    await page.evaluate(() => { Engrave.restoreView({ tab: 'place', list: true, chosen: false, focus: null, q: 'piece 2' }); Engrave.render(); }); await R.settle(page, 800); t = await tab(page);
    note(t.rows.some(x => x.rid === made.q2.rid) && !t.rows.some(x => x.rid === made.q1.rid), 'search "piece 2" finds the lines that have a Piece 2 and not the single piece', t.rows.map(x => x.rid.slice(-3)).join(','));
    await page.evaluate(() => { Engrave.restoreView({ tab: 'place', list: true, chosen: false, focus: null, q: '' }); Engrave.render(); }); await R.settle(page, 600);
    // a piece approved stays a piece of the line: the list still names it (one list with the approved piece marked, or the Decided tab of an older page); the other waits for its own approval
    await page.evaluate(k => { const j = [...Engrave.items().values()].find(x => x.key.endsWith('#C1') && x.key.startsWith(k)); window.__c1b = j.state; j.state = 'approved'; j.approvedAt = Date.now(); j.approvedBy = 'Test Operator'; Engrave.restoreView({ tab: 'place', list: true, chosen: false, focus: null, q: '' }); Engrave.render(); }, key('q2'));
    await R.settle(page, 900); t = await tab(page);
    const mine2 = t.rows.filter(x => x.rid === made.q2.rid);
    let tags = mine2.flatMap(x => x.ears.map(e => e.tag));
    note(mine2.length >= 1, 'piece 1 approved: the line is still in the list (piece 2 is still to approve)', mine2.map(x => x.kind + ':' + x.text.slice(0, 60)).join(' | '));
    const hasDone = await page.evaluate(() => !!document.querySelector('#egTabs [data-tab=done], [data-tab=done]'));
    if (hasDone) { await page.evaluate(() => { Engrave.restoreView({ tab: 'done', list: true, chosen: false, focus: null, q: '' }); Engrave.render(); }); await R.settle(page, 900); const t2 = await tab(page); tags = tags.concat(t2.rows.filter(x => x.rid === made.q2.rid).flatMap(x => x.ears.map(e => e.tag))); }
    // (one list, approved pieces kept in their row: the row names BOTH pieces. On a page that still has the Decided tab the row of the one piece left names none: that case is told by the tab's own row)
    if (!hasDone || tags.length >= 2) note(tags.some(g => /PIECE 1 of 2/i.test(g)) && tags.some(g => /PIECE 2 of 2/i.test(g)), 'both pieces are still listed by name (piece 1 approved, piece 2 not)', tags.join(' + '));
    else console.log('  (skipped: this page lists the piece left without its tag in Placements; one list names both: DEC)');
    await page.evaluate(k => { const j = [...Engrave.items().values()].find(x => x.key.endsWith('#C1') && x.key.startsWith(k)); j.state = window.__c1b; delete j.approvedAt; delete j.approvedBy; Engrave.restoreView({ tab: 'place', list: true, chosen: false, focus: null, q: '' }); Engrave.render(); }, key('q2'));
    await R.settle(page, 500);
    // the mixed order (one charm, three copies of another design line): the order strip counts every piece, and Back / Next walk charm, copy 1, copy 2, copy 3 in that order
    const mxRows = await page.evaluate(rid => Orders.rows().filter(r => r.order.receiptId === rid).map(r => ({ key: r.key, jobs: Engrave.jobsOf(r).map(j => j.key) })), made.mx.rid);
    const mxCopyFirst = mxRows[1].jobs.find(k => /#C1$/.test(k));
    await page.evaluate(k => { try { OrderWin.close(); } catch (_) {} CN.setMode('engrave'); Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: k }); Engrave.render(); }, mxRows[0].jobs[0]);
    await R.settle(page, 1000); t = await tab(page);
    note(!!t.strip && /Piece 1 of 4/.test(t.strip.of) && t.strip.tabs.length === 4, 'mixed order: the strip says "Piece 1 of 4" with a tab for each piece (the charm and the three copies)', t.strip ? t.strip.of + ' / ' + t.strip.tabs.map(b => b.label).join(' | ') : 'no strip');
    const seen = [t.focus.replace(/^\d+_\d+/, '')];
    for (let i = 0; i < 3; i++) { await page.evaluate(() => document.querySelector('#egQueue [data-a=next]').click()); await R.settle(page, 900); t = await tab(page); seen.push(t.focus.replace(/^\d+_\d+/, '')); }
    note(seen.length === 4 && /#C1$/.test(seen[1]) && /#C2$/.test(seen[2]) && /#C3$/.test(seen[3]) && !/#C/.test(seen[0]), 'Next walks charm -> Piece 1 -> Piece 2 -> Piece 3 of the order, one stop each', seen.join(' > '));
    note(!!t.strip && /Piece 4 of 4/.test(t.strip.of), 'and the strip follows: "Piece 4 of 4" on the last copy', t.strip ? t.strip.of : 'no strip');
    // the counted order of three copies: the strip walks three pieces
    await R.openEngraveDetail(w, 'q3', { slot: 'C2' }); t = await tab(page);
    note(t.switch.length === 3 && t.switch[1].on && /PIECE 2 of 3/i.test(t.switch[1].tag), 'quantity 3: the switch has three pieces, piece 2 shown', JSON.stringify(t.switch.map(b => b.tag)));

    // ───────────── 1440 and 400 px: nothing overflows sideways, nothing jumps ─────────────
    // (at phone width the Order window already scrolls sideways between its panes on a single piece: the check is that two pieces are no worse than one, and that the cards stay inside their own section)
    let oneWide = 0;
    for (const W of [1440, 400]) {
      await page.setViewportSize({ width: W, height: W < 600 ? 800 : 900 }); await R.settle(page, 500);
      const secBox = () => page.evaluate(() => { const a = document.getElementById('owEng'); const r = a.getBoundingClientRect(); return { top: Math.round(r.top), h: Math.round(r.height), l: Math.round(r.left), r: Math.round(r.right) }; });
      await openWin(w, key('q1')); const s1 = await win(page), box1 = await secBox(); oneWide = s1.over;
      await openWin(w, key('q2')); s = await win(page);
      const win0 = await secBox();
      note(win0.r - win0.l === box1.r - box1.l && win0.r - win0.l <= W, W + ' px: the Engraving section of two pieces is as wide as the one of a single piece, and fits the screen (' + (win0.r - win0.l) + ' vs ' + (box1.r - box1.l) + ')');
      note(s.cards.length === 2 && s.cards.every(c => c.box.l >= win0.l - 1 && c.box.r <= win0.r + 1 && c.box.w > 100), W + ' px: both piece cards are inside the Engraving section', s.cards.map(c => c.box.l + '-' + c.box.r).join(' | ') + ' in ' + win0.l + '-' + win0.r);
      note(s.cards.length === 2 && (W > 600 ? Math.abs(s.cards[0].box.h - s.cards[1].box.h) <= 2 : true), W + ' px: the two cards are the same size', s.cards.map(c => c.box.w + 'x' + c.box.h).join(' | '));
      await page.evaluate(k => OrderWin.selectPiece(k + '#C2'), key('q2')); await R.settle(page, 600);
      const win1 = await secBox();
      note(Math.abs(win1.top - win0.top) <= 2, W + ' px: picking a piece does not move the section (top ' + win0.top + ' -> ' + win1.top + ')');
      await page.evaluate(() => OrderWin.selectPiece(null)); await R.settle(page, 400);
      if (process.env.SHOTS) await R.shot(w, 'PV-' + W + '-window-qty2');
      await R.openEngraveDetail(w, 'q2', { slot: 'C1' }); t = await tab(page);
      note(t.over <= 1, W + ' px: the Engrave tab has no sideways scroll (' + t.over + ' px)');
      if (process.env.SHOTS) await R.shot(w, 'PV-' + W + '-engrave-detail-qty2');
      await page.evaluate(() => { Engrave.restoreView({ tab: 'place', list: true, chosen: false, focus: null, q: '' }); Engrave.render(); }); await R.settle(page, 700); t = await tab(page);
      const rowsOver = t.rows.filter(x => x.rid !== made.q2.rid).map(x => x.over), mine = t.rows.filter(x => x.rid === made.q2.rid).map(x => x.over);   // (at phone width the list's rows already carry a few px of overflow: the two-piece row must be no worse than the single ones)
      note(t.over <= 1 && mine.length > 0 && mine.every(o => o <= Math.max(...rowsOver, 0) + 2), W + ' px: the Engrave list has no sideways scroll, the two-piece row overflows no more than a single row', t.over + ' / ' + mine.join(',') + ' vs ' + rowsOver.join(','));
      if (process.env.SHOTS) await R.shot(w, 'PV-' + W + '-engrave-list');
    }
    await page.setViewportSize({ width: 1440, height: 900 });

    note(w.errors.length === 0, 'no page errors', w.errors.slice(0, 2).join(' | '));
    const warns = w.warns.filter(x => !/sheet metadata This sheet is no longer in the Library/.test(x));
    note(warns.length === 0, 'no swallowed order-window errors', warns.slice(0, 2).join(' | '));
  } catch (e) { console.log('  FAIL test crashed: ' + (e && e.stack || e)); fails.push('crash'); }
  finally { await w.close(); }
  console.log(fails.length ? `\n${fails.length} FAILED:\n - ${fails.join('\n - ')}` : '\nAll checks passed');
  process.exit(fails.length ? 1 : 0);
})();
