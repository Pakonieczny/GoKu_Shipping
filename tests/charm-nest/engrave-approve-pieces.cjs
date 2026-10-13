// ENGAPPROVE (engpieces-1013): ONE APPROVE FOR EVERY PIECE, AND NO PIECE SKIPPED (Paul, 13 Oct 2026):
//   "every engraved piece needs a dedicated Approve button so that the user only approves one charm at a time and doesn't mistakenly press approve without seeing the rest of the pieces in the multi-piece order ...
//    The user must be never allowed to skip pieces in the multipiece order so that the UI must guide the user very clearly and simply to every piece ... I wanna keep it fairly minimalist."
// and (04:27 UTC) "This is the same order and should be shown as one order with 2 pieces".
// The rule (charm-nest-engrave-guide.js): Approve approves ONE piece; an order is done only when every engraved piece of it (across its lines, whatever the metal) is approved; after a piece is approved the
// detail goes on to what is left of the SAME order (the piece's other face, then the next piece not approved), never to another order; a press that would leave an order with a piece not approved is held back
// by ONE calm line with one tap to the next piece and one to leave; in the order window, once the piece picked is approved, one quiet line says how many pieces of the order are still to approve.
//
// Offline only: the REAL page over the repo's fake site with the real drawings (_engraveRealWorld.cjs). Made-up buyer names only; no Etsy, no paid call, no live write (the approvals below are made on the fake
// site's in-memory Firestore). The two-piece order is shaped like Paul's: ONE order, two lines of one design, a silver one and a gold one, each with back engraving.
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/engrave-approve-pieces.cjs      (CHROMIUM=<chrome>  SHOTS=<dir>)
// What a pass does NOT prove: a live order on the live page; only that the page's own code does this on the offline fake.
'use strict';
const fs = require('fs'), path = require('path');
const R = require('./_engraveRealWorld.cjs'), Base = require('./_orderFactsWorld.cjs');

function orderOf(fx, now) {
  const at = (d, h, m) => { const t = new Date(now); t.setDate(t.getDate() + d); t.setHours(h, m, 0, 0); return Math.floor(t.getTime() / 1000); };
  const shipBy = at(fx.ship, 17, 0), line = (x, i) => ({ transactionId: fx.rid + (i + 1), listingId: x.listing, sku: x.sku, title: x.title, quantity: x.qty || 1, expectedShipDate: shipBy, variations: x.variations || [], metalKey: x.metalKey || 'gold', metalLabel: x.metalLabel || 'GF 14/20', personalization: x.personalization || [], buyerMessage: '' });
  return { receiptId: fx.rid, orderNumber: fx.rid, createTs: at(fx.placed.day, fx.placed.h, fx.placed.m), updateTs: at(0, 12, 0), shipBy, buyer: { name: fx.buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: (fx.lines || [fx]).map(line) };
}
const V = (name, value) => ({ name, value });
const SS = { listing: '1890000101', sku: 'COMET_9427', title: 'Comet Necklace, Celestial Charm Necklace', variations: [V('Metal choice', 'Silver'), V('Font Choice', 'Stylish')], personalization: ['Name One'], metalKey: 'silver', metalLabel: 'SS' };
const GF = { listing: '1890000101', sku: 'COMET_9427', title: 'Comet Necklace, Celestial Charm Necklace', variations: [V('Metal choice', 'Gold'), V('Font Choice', 'Stylish')], personalization: ['Name One'] };
const BOTH = { listing: '1890000103', sku: 'SCHRODINGER CAT 2', title: 'Cat Necklace, Whimsical Cat Charm, engraved on both sides', variations: [V('Metal choice', 'Gold'), V('Engraving Side', 'Both'), V('Font Choice', 'Ace')], personalization: ['front - W\nback - Name Two'] };
R.ORDERS.discsSame = Object.assign({}, R.ORDERS.discs, { rid: '4290000134', buyer: 'Name Fourteen', listing: '1890000105', placed: { day: 0, h: 12, m: 0 }, personalization: ['Tag 1: A, Tag 2: A, Tag 3: A'] }); R.EXPECT_JOBS.discsSame = 3;
R.ORDERS.discsWin = Object.assign({}, R.ORDERS.discs, { rid: '4290000136', buyer: 'Name Sixteen', listing: '1890000105', placed: { day: 0, h: 12, m: 10 }, personalization: ['Tag 1: A, Tag 2: B, Tag 3: C'] }); R.EXPECT_JOBS.discsWin = 3;   // (the order window with ONE disc picked: ENGPICK made a disc selectable)   // (the same letter on every disc: the case where the old "Approve all discs" press was offered)
Base.orderOf = orderOf;                                       // (R.build serialises Base.orderOf into the page; single-line orders of the shared world keep working: `fx.lines || [fx]`)
R.ORDERS.two = { rid: '4290000131', buyer: 'Name Eleven', placed: { day: 0, h: 11, m: 30 }, ship: 5, lines: [SS, GF] };               R.EXPECT_JOBS.two = 2;
R.ORDERS.three = { rid: '4290000132', buyer: 'Name Twelve', placed: { day: 0, h: 11, m: 40 }, ship: 5, lines: [SS, GF, BOTH] };      R.EXPECT_JOBS.three = 4;
R.ORDERS.twoB = { rid: '4290000133', buyer: 'Name Thirteen', placed: { day: 0, h: 11, m: 50 }, ship: 5, lines: [SS, GF] };               R.EXPECT_JOBS.twoB = 2;

const fails = [], note = (ok, what, more) => { console.log((ok ? '  PASS ' : '  FAIL ') + what + (more ? '  [' + more + ']' : '')); if (!ok) fails.push(what); };

(async () => {
  const w = await R.world({ viewport: { width: 1356, height: 900 } });
  if (!w) { console.log('no playwright-core: nothing run'); return; }
  const page = w.page, tails = k => String(k).replace(/^\d+_/, '');
  let writes = 0; page.on('request', rq => { if (rq.method() !== 'GET' && rq.method() !== 'OPTIONS' && /127\.0\.0\.1|localhost/.test(rq.url())) writes++; });
  try {
    // BACK first in the queue, so that "the next card" of a careless jump would be some OTHER order's
    const made = await R.build(w, ['back', 'two', 'three', 'twoB', 'discs', 'discsSame', 'discsWin']);
    // the draft of the order model, until it is on the page by its own script tag
    const haveOrder = await page.evaluate(() => !!window.CharmNestEngraveOrder);
    if (!haveOrder) { const f = path.join(__dirname, '../../charm-nest-engrave-order.js'); if (fs.existsSync(f)) await page.addScriptTag({ path: f }); }
    const ST = () => page.evaluate(() => {
      const o = {}; for (const j of Engrave.items().values()) { const rid = j.row.order.receiptId; (o[rid] ||= {})[j.key.replace(/^\d+_/, '')] = j.state; }
      const O = window.CharmNestEngraveOrder; const pcs = {}; if (O) for (const rid of Object.keys(o)) pcs[rid] = O.piecesOf(rid).map(p => p.pieceKey.replace(/^\d+_/, '') + ':' + p.state);
      return { jobs: o, pcs };
    });
    const rows = await page.evaluate(() => { const o = {}; for (const r of Orders.rows()) (o[r.order.receiptId] ||= []).push(r.key); return o; });
    const RID = { two: R.ORDERS.two.rid, three: R.ORDERS.three.rid, twoB: R.ORDERS.twoB.rid, discs: R.ORDERS.discs.rid, discsSame: R.ORDERS.discsSame.rid, discsWin: R.ORDERS.discsWin.rid, back: R.ORDERS.back.rid };
    const [kSS, kGF] = rows[RID.two];
    const hasOpenPiece = await page.evaluate(() => typeof (window.Engrave && Engrave.openPiece) === 'function');
    note(rows[RID.two].length === 2, 'the two-piece order is ONE order of two lines (SS and GF of one design)', rows[RID.two].map(tails).join(' '));
    let s = await ST();
    note(Object.values(s.jobs[RID.two]).every(x => x === 'review') && Object.keys(s.jobs[RID.two]).length === 2, 'both pieces wait for approval, each with its own job', JSON.stringify(s.jobs[RID.two]));
    note((s.pcs[RID.two] || []).length === 2, 'the order model lists its 2 pieces', (s.pcs[RID.two] || []).join(' '));

    // ═══ A · the order window: one Approve per piece, and one quiet line that points at the piece still to approve ═══
    const section = () => page.evaluate(() => {
      const host = document.getElementById('owEng'), left = document.getElementById('owEngLeft');
      const cs = [...(host ? host.children : [])].filter(n => n.classList.contains('engCard'));
      return { hostHidden: !host || host.hidden, cards: cs.map(c => ({ piece: (c.dataset.piece || '').replace(/^\d+_/, ''), state: c.dataset.state, approve: !!c.querySelector('[data-e=approve]') && !c.querySelector('[data-e=approve]').disabled, seals: +((c.querySelector('[data-seal-group]') || { dataset: {} }).dataset.sealCount || 0) })),
        left: left && !left.hidden ? left.innerText.replace(/\s+/g, ' ').trim() : '', leftBtn: left && !left.hidden && left.querySelector('button') ? left.querySelector('button').textContent.trim() : '',
        sel: OrderWin.selectedPiece() ? OrderWin.selectedPiece().replace(/^\d+_/, '') : null,
        leftBelowCards: !!(left && host && left.previousElementSibling === host) };
    });
    await page.evaluate(async k => { CN.setMode('orders'); await OrderWin.open(k); }, kSS);
    await page.waitForFunction(() => { const h = document.getElementById('owEng'); return h && !h.hidden && h.querySelector('.engCard'); }, null, { timeout: 20000 }).catch(() => {});
    await R.settle(page, 700);
    let sec = await section();
    note(sec.cards.length === 2 && sec.sel === null && sec.left === '', 'Show all: both pieces have a card, each with its own Approve, and no pointer line is needed', JSON.stringify(sec.cards.map(c => c.piece.slice(-1) + ':' + c.approve)) + ' left="' + sec.left + '"');
    await page.evaluate(k => OrderWin.selectPiece(k), kSS); await R.settle(page, 500);
    sec = await section();
    note(sec.cards.length === 1 && sec.cards[0].piece === tails(kSS) && sec.cards[0].approve && sec.left === '', 'the silver piece picked: its card alone with its Approve; the piece is still to approve, so no pointer line', JSON.stringify(sec));
    writes = 0;
    await page.click('#owEng .engCard [data-e=approve]');
    await page.waitForFunction(k => { const j = Engrave.items().get(k); return j && j.state === 'approved'; }, kSS, { timeout: 30000 });
    await R.settle(page, 2500);
    const writesPair = writes;
    s = await ST();
    note(s.jobs[RID.two][tails(kSS)] === 'approved' && s.jobs[RID.two][tails(kGF)] === 'review', 'Approve on the silver card approved the silver piece ONLY: the gold piece is still waiting', JSON.stringify(s.jobs[RID.two]));
    note((s.pcs[RID.two] || []).join(' ') === `${tails(kSS)}:approved ${tails(kGF)}:to-approve`, 'state per piece: silver approved, gold to approve; the order is not done', (s.pcs[RID.two] || []).join(' '));
    sec = await section();
    note(sec.cards.length === 1 && /1 more piece of this order to approve/.test(sec.left), 'the picked piece is approved: ONE quiet line says one more piece of the order is to approve', sec.left);
    note(/^Show /.test(sec.leftBtn) && sec.leftBelowCards, 'the line has one button, "Show ...", right under the card', sec.leftBtn);
    // (a pick repaints the whole window, which reads what it reads for any pick: the line's press must cost exactly what picking that piece by hand costs, i.e. the line adds no request of its own)
    writes = 0; await page.evaluate(k => OrderWin.selectPiece(k), kGF); await R.settle(page, 900); const pickCost = writes;
    await page.evaluate(k => OrderWin.selectPiece(k), kSS); await R.settle(page, 900);
    sec = await section();
    writes = 0;
    if (await page.waitForSelector('#owEngLeft [data-g=show]', { timeout: 2500 }).then(() => true, () => false)) await page.click('#owEngLeft [data-g=show]');
    else await page.evaluate(k => OrderWin.selectPiece(k), kGF);   // (no line on this tree: the pick is made by hand so the rest can run)
    await R.settle(page, 900);
    console.log('  NOTE the pick made by the line: ' + writes + ' requests, the same pick by hand: ' + pickCost + ' (the window repaints as it does for any pick; the guide itself makes none: see the zero below)');
    sec = await section();
    note(sec.sel === tails(kGF) && sec.cards.length === 1 && sec.cards[0].piece === tails(kGF) && sec.cards[0].approve, 'one tap picks the gold piece: its card with its own Approve', JSON.stringify(sec));
    note(sec.left === '', 'the gold piece is still to approve: the pointer line is gone', sec.left);
    await page.click('#owEng .engCard [data-e=approve]');
    await page.waitForFunction(k => { const j = Engrave.items().get(k); return j && j.state === 'approved'; }, kGF, { timeout: 30000 });
    await R.settle(page, 2500);
    s = await ST();
    note(s.jobs[RID.two][tails(kGF)] === 'approved' && (s.pcs[RID.two] || []).every(x => /:approved$/.test(x)), 'the gold piece approved on its own press: now every piece of the order is approved (the order is done)', (s.pcs[RID.two] || []).join(' '));
    sec = await section();
    note(sec.left === '', 'order done: no pointer line', sec.left);
    await page.evaluate(() => OrderWin.selectPiece(null)); await R.settle(page, 500);
    sec = await section();
    note(sec.left === '' && sec.cards.length === 2, 'Show all after both: two cards, no line', sec.left);
    await page.evaluate(() => OrderWin.close()); await R.settle(page, 400);

    // ═══ B · the Engrave detail: after Approve the next card is the SAME order's next piece ═══
    // (the three-piece order: silver, gold, and a front + back charm; BACK (another order) is first in the queue)
    const [k3a, k3b, k3c] = rows[RID.three];
    const DETAIL = () => page.evaluate(() => {
      const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); if (!c) return null; const g = c.querySelector('.egGuide');
      return { key: (c.dataset.key || '').replace(/^\d+_/, ''), rid: (c.querySelector('.reviewIdentity .ttl') || {}).textContent || '', guide: g && !g.hidden ? g.innerText.replace(/\s+/g, ' ').trim() : '', guideBtns: g && !g.hidden ? [...g.querySelectorAll('button')].map(b => b.dataset.g).join(',') : '', counter: ((c.querySelector('.reviewProgress .kind') || {}).textContent || '').trim(),
        approveBtns: [...c.querySelectorAll('[data-a=approve],[data-a=approveBoth],[data-a=approveFaces]')].map(b => (b.dataset.a + ':' + b.textContent.trim())).join(' | '), all: [...c.querySelectorAll('button')].map(b => b.textContent.trim()).filter(t => /approve/i.test(t)).join(' | ') };
    });
    const openDetail = async key => { await page.evaluate(k => { try { OrderWin.close(); } catch (_) {} CN.setMode('engrave'); Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: k }); Engrave.render(); }, key); await page.waitForSelector('#egQueue .rvItem[data-kind=placement]', { timeout: 15000 }); await R.settle(page, 900); };
    const approveInDetail = async key => {
      await page.click('#egQueue [data-a=approve]');
      await page.waitForFunction(k => { const j = Engrave.items().get(k); return j && j.state === 'approved'; }, key, { timeout: 30000 });
      await R.settle(page, 3000);
    };
    await openDetail(k3a);
    let d = await DETAIL();
    note(d && d.key === tails(k3a), 'the detail is on the first piece of the three-piece order', JSON.stringify(d));
    writes = 0; await approveInDetail(k3a); const writesSingleOfThree = writes;
    d = await DETAIL(); s = await ST();
    note(s.jobs[RID.three][tails(k3a)] === 'approved' && s.jobs[RID.three][tails(k3b)] === 'review', 'Approve in the detail approved piece 1 only', JSON.stringify(s.jobs[RID.three]));
    note(d && d.key === tails(k3b), 'after Approve the detail is on piece 2 of the SAME order (not on the other order that is first in the queue)', JSON.stringify(d));
    if (!d || d.key !== tails(k3b)) await openDetail(k3b);   // (a tree that jumped elsewhere: carry on from piece 2 so the rest is still measured)
    await approveInDetail(k3b);
    d = await DETAIL(); s = await ST();
    note(d && (d.key === tails(k3c) || d.key === tails(k3c) + '~front'), 'after piece 2 the detail is on piece 3 (the front + back charm), still the same order', JSON.stringify(d));
    // the front + back charm: its Back is approved, then its Front (the same piece) comes up, not another piece
    const fb = await page.evaluate(k => { const js = Engrave.jobsOf(B.orders.byKey.get(k)); return js.map(j => ({ key: j.key.replace(/^\d+_/, ''), face: j.face === 'front' ? 'front' : 'back', state: j.state })); }, k3c);
    note(fb.length === 2, 'piece 3 has a Front and a Back job', JSON.stringify(fb));
    const faceOn = () => page.evaluate(() => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); return c ? (c.dataset.key || '').replace(/^\d+_/, '') : null; });
    if (!d || ![tails(k3c), tails(k3c) + '~front'].includes(d.key)) await openDetail(k3c);
    const first = await faceOn(); const frontKey = fb.find(x => x.face === 'front').key, backKey = fb.find(x => x.face === 'back').key;
    const jobKeyFull = t => RID.three + '_' + t;
    note(first === frontKey || first === backKey, 'the detail shows one face of piece 3', first);
    const firstFace = first === frontKey ? 'front' : 'back', otherKey = firstFace === 'front' ? backKey : frontKey;
    await approveInDetail(jobKeyFull(first));
    d = await DETAIL(); s = await ST();
    note(d && d.key === otherKey, 'one face of piece 3 approved: the detail goes to the OTHER face of the same piece first', first + ' -> ' + (d && d.key));
    note(!/Approve all|discs left|both ears/i.test(d ? d.all : ''), 'no press that approves several pieces is offered', d && d.all);
    note((s.pcs[RID.three] || []).slice(-1)[0] && /to-approve$/.test(s.pcs[RID.three].slice(-1)[0]), 'piece 3 is not done while one face waits', (s.pcs[RID.three] || []).join(' '));
    if (!d || d.key !== otherKey) await openDetail(jobKeyFull(otherKey));
    await approveInDetail(jobKeyFull(otherKey));
    s = await ST();
    note((s.pcs[RID.three] || []).every(x => /:approved$/.test(x)), 'both faces approved: all three pieces approved, the order is done', (s.pcs[RID.three] || []).join(' '));
    d = await DETAIL();
    note(!d || d.key !== tails(k3a) && d.key !== tails(k3b) && d.key !== tails(k3c), 'with nothing left in the order the queue goes on to another order as it always did', JSON.stringify(d && { key: d.key }));
    note(writesSingleOfThree > 0 && writesPair > 0, 'an approval writes (as it always did): ' + writesPair + ' requests from the order window, ' + writesSingleOfThree + ' from the detail', '');

    // ═══ C · the discs: one Approve each, no "Approve all discs" ═══
    const dj = await page.evaluate(rid => [...Engrave.items().values()].filter(j => j.row.order.receiptId === rid).map(j => j.key), RID.discs);
    await openDetail(dj[0]);
    d = await DETAIL();
    note(d && !/all discs|discs left|both ears/i.test(d.all), 'discs: the detail offers no "Approve all discs" / "the N discs left"', d && d.all);
    note(d && /(^|\| )approve:Approve( \||$)/.test(d.approveBtns) && !/approveBoth/.test(d.approveBtns), 'discs: one Approve for the disc shown', d && d.approveBtns);
    await approveInDetail(dj[0]);
    s = await ST();
    const dst = await page.evaluate(ks => ks.map(k => Engrave.items().get(k).state), dj);
    note(dst.join(',') === 'approved,review,review', 'Approve on disc 1 approved disc 1 only', dst.join(','));
    d = await DETAIL();
    note(d && /#D2$/.test(d.key), 'then the detail is on disc 2 of the same order', d && d.key);
    if (!d || !/#D2$/.test(d.key)) await openDetail(dj[1]);
    await approveInDetail(dj[1]);
    d = await DETAIL();
    note(d && /#D3$/.test(d.key), 'then disc 3', d && d.key);

    // the same letter on every disc: the old detail offered ONE press for all of them
    const ds = await page.evaluate(rid => [...Engrave.items().values()].filter(j => j.row.order.receiptId === rid).map(j => j.key), RID.discsSame);
    await openDetail(ds[0]);
    d = await DETAIL();
    note(ds.length === 3 && d && !/all discs|discs left|both ears/i.test(d.all) && !/approveBoth/.test(d.approveBtns), 'three discs with the SAME words: still no press that approves all of them, only the disc\'s own Approve', d && d.all);
    await approveInDetail(ds[0]);
    const dst2 = await page.evaluate(ks => ks.map(k => Engrave.items().get(k).state), ds);
    note(dst2.join(',') === 'approved,review,review', 'its Approve approved that disc only', dst2.join(','));
    const hasGuide = await page.evaluate(() => !!window.CharmNestEngraveGuide);
    note(hasGuide, 'the guide (charm-nest-engrave-guide.js) is on the page');
    // ═══ D · the guide's line: a press that would leave an order with a piece not approved ═══
    // (a second two-piece order, nothing approved in it yet; the detail is on its last piece)
    if (hasGuide) {
    const [kSS2, kGF2] = rows[RID.twoB];
      await openDetail(kGF2);
      d = await DETAIL();
      note(d && d.key === tails(kGF2) && d.guide === '', 'the detail is on the gold piece (the last one); no line is showing', JSON.stringify(d));
      writes = 0;
      const out = await page.evaluate(([rid, pk]) => { let went = 0; const took = CharmNestEngraveGuide.leaving(rid, pk, () => { went++; }); const g = document.querySelector('#egQueue .egGuide'); return { took, went, text: g && !g.hidden ? g.innerText.replace(/\s+/g, ' ').trim() : '', btns: g && !g.hidden ? [...g.querySelectorAll('button')].map(b => b.dataset.g + ':' + b.textContent.trim()).join(' | ') : '' }; }, [RID.twoB, kGF2]);
      note(out.took === true && out.went === 0, 'a press that would leave the order with pieces not approved is held back (not silently let through)', JSON.stringify(out));
      note(/2 pieces of this order are not approved yet/.test(out.text), 'ONE calm line says how many pieces are left', out.text);
      note(/leave/.test(out.btns) && (!hasOpenPiece || /go:Go to piece 1/.test(out.btns)), 'with one tap to the next piece (piece 1) and one to leave', out.btns);
      note(writes === 0, 'the line makes no request', 'requests ' + writes);
      const again = await page.evaluate(([rid, pk]) => { let went = 0; const took = CharmNestEngraveGuide.leaving(rid, pk, () => { went++; }); return { took, went }; }, [RID.twoB, kGF2]);
      note(again.took === true && again.went === 0, 'pressed again while it shows: still held back (the line is what answers), nothing leaves', JSON.stringify(again));
      if (hasOpenPiece) {
        await page.click('#egQueue .egGuide [data-g=go]'); await R.settle(page, 1200);
        d = await DETAIL();
        note(d && d.key === tails(kSS2) && d.guide === '', 'one tap on "Go to piece 1" opens the silver piece and the line is gone', JSON.stringify(d));
      } else console.log('  NOTE Engrave.openPiece is not on this tree yet (ENGPIECES): "Go to piece" is not offered and not checked');
      await openDetail(kGF2);
      const lv = await page.evaluate(([rid, pk]) => { window.__went = 0; CharmNestEngraveGuide.leaving(rid, pk, () => { window.__went++; }); return !!document.querySelector('#egQueue .egGuide:not([hidden])'); }, [RID.twoB, kGF2]);
      await page.click('#egQueue .egGuide [data-g=leave]'); await R.settle(page, 300);
      const went = await page.evaluate(() => ({ went: window.__went, shown: !!document.querySelector('#egQueue .egGuide:not([hidden])') }));
      note(lv && went.went === 1 && !went.shown, '"Leave" does the press once and the line is gone', JSON.stringify(went));
      // the REAL buttons of the detail (not a call into the guide): close, Back and Next are held back at the order's edge and free inside it
      if (await page.evaluate(() => !!document.querySelector('#egQueue [data-a=close]'))) {
        await openDetail(kGF2); d = await DETAIL();
        await page.click('#egQueue [data-a=close]'); await R.settle(page, 500);
        d = await DETAIL();
        note(d && d.key === tails(kGF2) && /2 pieces/.test(d.guide), 'the real close button, with 2 pieces of the order not approved: the detail stays and says so', JSON.stringify(d && { key: d.key, guide: d.guide }));
        await page.click('#egQueue .egGuide [data-g=leave]'); await R.settle(page, 800);
        note(await page.evaluate(() => !document.querySelector('#egQueue .rvItem[data-kind=placement]')), 'Leave on that line closes the detail (the list is shown)', '');
      }
      const edge = async (which, key) => {
        await openDetail(key); const before = await DETAIL();
        if (!(await page.evaluate(w => !!document.querySelector('#egQueue [data-a=' + w + ']'), which))) return { none: true, before };
        await page.click('#egQueue [data-a=' + which + ']'); await R.settle(page, 700);
        return { before, after: await DETAIL() };
      };
      // inside the order: silver -> gold with Next, gold -> silver with Back: no line, no hold
      const e1 = await edge('next', kSS2);
      if (e1.after) note(e1.after.rid === RID.twoB && e1.after.key === tails(kGF2) && e1.after.guide === '', 'Next from piece 1 goes to piece 2 of the same order, nothing is held back', JSON.stringify({ key: e1.after.key, guide: e1.after.guide }));
      const e2 = await edge('prev', kGF2);
      if (e2.after) note(e2.after.rid === RID.twoB && e2.after.key === tails(kSS2) && e2.after.guide === '', 'Back from piece 2 goes to piece 1 of the same order, nothing is held back', JSON.stringify({ key: e2.after.key, guide: e2.after.guide }));
      // at the order's edge: Back from piece 1 (and Next from the last piece) would reach another order with 2 pieces not approved
      const e3 = await edge('prev', kSS2);
      if (e3.after && e3.after.rid !== RID.twoB) note(false, 'Back from piece 1 reached another order with 2 pieces not approved and no word said', JSON.stringify({ key: e3.after.key, guide: e3.after.guide }));
      else if (e3.after) note(/2 pieces/.test(e3.after.guide), 'Back from piece 1 to another order: held back by the line, the detail stays on this order', JSON.stringify({ key: e3.after.key, guide: e3.after.guide }));
      else console.log('  NOTE no Back button on this card');
      const e4 = await edge('next', kGF2);
      if (e4.after && e4.after.rid !== RID.twoB) note(false, 'Next from the last piece reached another order with 2 pieces not approved and no word said', JSON.stringify({ key: e4.after.key, guide: e4.after.guide }));
      else if (e4.after) note(/2 pieces/.test(e4.after.guide), 'Next from the last piece to another order: held back by the line, the detail stays on this order', JSON.stringify({ key: e4.after.key, guide: e4.after.guide }));
      const solo = await page.evaluate(([rid, k]) => { let went = 0; const t = CharmNestEngraveGuide.leaving(rid, k, () => { went++; }); return { t, went }; }, [RID.back, rows[RID.back][0]]);
      note(solo.t === false && solo.went === 0, 'an order of ONE piece: nothing is asked (the caller goes on by itself)', JSON.stringify(solo));
      // the card drawn again (a job changes) keeps the line while it is the same piece, and another piece's card does not carry it
      await openDetail(kGF2);
      await page.evaluate(([rid, pk]) => CharmNestEngraveGuide.leaving(rid, pk, () => {}), [RID.twoB, kGF2]);
      await page.evaluate(() => Engrave.render()); await R.settle(page, 600);
      d = await DETAIL();
      note(d && /2 pieces/.test(d.guide), 'the card drawn again for the same piece keeps the line', JSON.stringify(d && d.guide));
      await openDetail(kSS2); d = await DETAIL();
      note(d && d.guide === '', 'another piece\'s card does not carry it', JSON.stringify(d && d.guide));
      await approveInDetail(kSS2);
      d = await DETAIL();
      note(d && d.key === tails(kGF2), 'approving piece 1 of 2 goes straight to piece 2 of the same order', JSON.stringify(d && d.key));
      const lastLeave = await page.evaluate(([rid, pk]) => { let went = 0; const t = CharmNestEngraveGuide.leaving(rid, pk, () => { went++; }); const g = document.querySelector('#egQueue .egGuide'); return { t, text: g && !g.hidden ? g.innerText.replace(/\s+/g, ' ').trim() : '', btns: g && !g.hidden ? [...g.querySelectorAll('button')].map(b => b.dataset.g).join(',') : '' }; }, [RID.twoB, kGF2]);
      note(lastLeave.t === true && /1 piece of this order is not approved yet/.test(lastLeave.text) && !/go/.test(lastLeave.btns), 'the one piece left is the one on screen: held back with the singular line and only Leave (nothing else to go to)', JSON.stringify(lastLeave));
      await approveInDetail(kGF2);
      const done = await page.evaluate(([rid, pk]) => CharmNestEngraveGuide.leaving(rid, pk, () => {}), [RID.twoB, kGF2]);
      note(done === false, 'every piece approved: leaving is never asked anything', String(done));
    }

    // ═══ E · the order window with ONE DISC picked (ENGPICK): the line points at the next disc, one tap picks it ═══
    {
      const [dk] = rows[RID.discsWin]; const dsec = () => page.evaluate(() => {
        const host = document.getElementById('owEng'), left = document.getElementById('owEngLeft');
        return { cards: [...(host ? host.children : [])].filter(n => n.classList.contains('engCard')).map(c => (c.dataset.piece || '').replace(/^\d+_/, '') + ':' + c.dataset.state), disc: OrderWin.selectedDisc ? OrderWin.selectedDisc() : null,
          left: left && !left.hidden ? left.innerText.replace(/\s+/g, ' ').trim() : '', leftBtn: left && !left.hidden && left.querySelector('button') ? left.querySelector('button').textContent.trim() : '' };
      });
      await page.evaluate(async k => { CN.setMode('orders'); await OrderWin.open(k); }, dk);
      await page.waitForFunction(() => { const h = document.getElementById('owEng'); return h && !h.hidden && h.querySelector('.engCard'); }, null, { timeout: 20000 }).catch(() => {});
      await R.settle(page, 700);
      const picked = await page.evaluate(k => OrderWin.selectPiece(k + '#D1'), dk); await R.settle(page, 700);
      let ds = await dsec();
      if (!picked || ds.disc !== 'D1') console.log('  NOTE one disc cannot be picked on this tree (OrderWin.selectPiece "<line>#D1"): not checked');
      else {
        note(ds.cards.length === 1 && /#D1:/.test(ds.cards[0]) && ds.left === '', 'Disc 1 picked: its card alone, still to approve, no pointer line', JSON.stringify(ds));
        await page.click('#owEng .engCard [data-e=approve]');
        await page.waitForFunction(k => { const j = [...Engrave.items().values()].find(x => x.key === k + '#D1'); return j && j.state === 'approved'; }, dk, { timeout: 30000 });
        await R.settle(page, 2500);
        ds = await dsec();
        note(/2 more pieces of this order to approve/.test(ds.left) && /^Show /.test(ds.leftBtn), 'Disc 1 approved: ONE line says 2 more pieces of the order are to approve, with Show ...', JSON.stringify(ds));
        await page.click('#owEngLeft [data-g=show]'); await R.settle(page, 900);
        ds = await dsec();
        note(ds.disc === 'D2' && ds.cards.length === 1 && /#D2:approve$/.test(ds.cards[0]) && ds.left === '', 'one tap picks Disc 2 (not the whole line): its card alone, still TO APPROVE (Disc 1\'s approval is not read as Disc 2\'s), the line is gone', JSON.stringify(ds));
        const st = await ST();
        note(Object.entries(st.jobs[RID.discsWin]).filter(([k, v]) => v === 'approved').length === 1, 'only Disc 1 was approved by all this', JSON.stringify(st.jobs[RID.discsWin]));
      }
      await page.evaluate(() => OrderWin.close()); await R.settle(page, 400);
    }

    note(w.errors.length === 0, 'no page errors', w.errors.slice(0, 2).join(' | '));
    if (process.env.SHOTS) await R.shot(w, 'APPROVE-end');
  } catch (e) { console.log('  FAIL test crashed: ' + (e && e.stack || e)); fails.push('crash'); }
  finally { await w.close(); }
  console.log(fails.length ? `\n${fails.length} FAILED:\n - ${fails.join('\n - ')}` : '\nAll checks passed');
  process.exit(fails.length ? 1 : 0);
})();
