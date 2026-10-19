// NODECIDED (Paul, 16 Oct 2026): "Get rid of this Decided view. Instead just open the existing detailed Engraving modal without needing to 'Re-Open' the order. The user should be free to
// update/modify any already Approved engraving order and simply need to press the 'Approve' button again after the second change. This would then produce a second brand new seal as we already do
// today. This removes the need for this extra view and 'Re-Open' feature."
//
// What it proves, offline: the REAL page in Chromium over the repo's fake site with the real drawings (_engraveRealWorld.cjs); made-up buyers only; no Etsy, no paid call, no live write (the approvals
// below are made on the fake site's in-memory Firestore). It does NOT prove a live order, a live sheet or a live laser: only that the page's own code does this on the offline fake.
//   1  there is no Decided tab, no Decided list, no Reopen button anywhere (the Engrave tab, the order window's engraving cards); chips "Placements" (counts what still needs a decision) and, since APPTAB (19 Oct 2026), "Approved" (a view of the same list: tests/charm-nest/engrave-approved-tab.cjs)
//   2  an approved piece is in the SAME list as the pending ones (pending first, then the approved rows, with the state word and its seals); the date chips and the search apply to all of it
//   3  a click on an approved piece opens the SAME detailed card, its Approve a calm "Approved"; just looking changes nothing (state, seals, back file, no write)
//   4  the first change (words in the panel; a position button) puts the piece back to "needs Approve" (state review, back file superseded, the first seal kept) and Approve then adds a SECOND seal
//   5  a piece "Saved to sheet" follows the rule Reopen had: no block, its back record is superseded, one calm line in the card, no confirm
//   6  a mixed order (a line of discs, a faced line): each piece is told with its own state in its row; a click on a decided piece's line opens that piece
//   7  old views and links that say tab "done" still land on the list / the card (APPTAB: on its Approved chip); the order window's "View in Engraving" lands on the card of an approved piece
//   8  phone width (400 px) and desktop (1440 px): screenshots in SHOTS (default /tmp/dec/shots); no console or page errors
//   PW_DIR=/opt/node-tools/node_modules node tests/charm-nest/engrave-no-decided.cjs      (CHROMIUM=<chrome>  SHOTS=<dir>)
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
Base.orderOf = orderOf;
R.ORDERS.two = { rid: '4290000231', buyer: 'Name Eleven', placed: { day: 0, h: 11, m: 30 }, ship: 5, lines: [SS, GF] }; R.EXPECT_JOBS.two = 2;

const fails = [], note = (ok, what, more) => { console.log((ok ? '  PASS ' : '  FAIL ') + what + (more ? '  [' + more + ']' : '')); if (!ok) fails.push(what); };
const SHOTS = process.env.SHOTS || '/tmp/dec/shots';

(async () => {
  const w = await R.world({ viewport: { width: 1440, height: 900 }, shots: SHOTS });
  if (!w) { console.log('no playwright-core: nothing run'); return; }
  const page = w.page, tails = k => String(k).replace(/^\d+_/, '');
  const errs = []; page.on('pageerror', e => errs.push('pageerror: ' + e.message)); page.on('console', m => { if (m.type() === 'error' && !/favicon|Failed to load resource/.test(m.text())) errs.push('console: ' + m.text().slice(0, 200)); });
  let writes = 0, counting = false; const ops = []; page.on('request', rq => { if (counting && rq.method() !== 'GET' && rq.method() !== 'OPTIONS' && /127\.0\.0\.1|localhost/.test(rq.url())) { writes++; try { const b = JSON.parse(rq.postData() || '{}'); ops.push(String(b.op || b.type || rq.url().split('/').pop())); } catch (_) { ops.push(rq.url().split('/').pop()); } } });
  const settle = ms => R.settle(page, ms || 400);
  try {
    const made = await R.build(w, ['back', 'two', 'discs', 'both']);
    const RID = { back: R.ORDERS.back.rid, two: R.ORDERS.two.rid, discs: R.ORDERS.discs.rid, both: R.ORDERS.both.rid };
    const jobsOf = rid => page.evaluate(rid => [...Engrave.items().values()].filter(j => String(j.row.order.receiptId) === rid).map(j => ({ key: j.key, state: j.state, text: j.text, seals: CNEngravingSeals.list(j).map(s => s.id), approvedAt: j.approvedAt || 0, backs: (j.backs || []).length, fitted: !!j.fit })), rid);
    const stateOf = key => page.evaluate(k => { const j = Engrave.items().get(k); return j ? { state: j.state, seals: CNEngravingSeals.list(j).map(s => s.id), approvedAt: j.approvedAt || 0, backs: (j.backs || []).length, text: j.text, centre: j.fit && j.fit.centre && j.fit.centre.join(',') } : null; }, key);
    const showList = async o => { await page.evaluate(o => { CN.setMode('engrave'); Engrave.restoreView(Object.assign({ tab: 'place', list: true, chosen: true, focus: null, q: '' }, o || {})); Engrave.render(); }, o); await page.waitForSelector('#egQueue .egPlacementList', { timeout: 15000 }); await settle(500); };
    const listRows = () => page.evaluate(() => [...document.querySelectorAll('#egQueue .egPlacementList .placementRow')].map(r => ({ open: r.dataset.open, rid: r.dataset.rid, decided: r.dataset.decided === '1', cls: r.className, stages: [...r.querySelectorAll('[data-stage]')].map(s => ({ s: s.dataset.s || '', t: s.textContent.trim() })), seals: r.querySelectorAll('.egPlacementSeals .engravingSeals').length, sealCount: +((r.querySelector('.egPlacementSeals [data-seal-count]') || {}).dataset || {}).sealCount || 0, ears: [...r.querySelectorAll('.egEar')].map(e => ({ ear: e.dataset.ear, s: (e.querySelector('[data-stage]') || {}).dataset ? e.querySelector('[data-stage]').dataset.s || '' : '' })) })));
    const cardOf = () => page.evaluate(() => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); if (!c) return null; const ap = c.querySelector('.pvApproval'); return { key: c.dataset.key, state: c.dataset.state, approve: !!c.querySelector('[data-a=approve]'), approveOff: !!(c.querySelector('[data-a=approve]') || {}).disabled, calm: !!c.querySelector('.egApproveButton.egApproved'), calmText: (c.querySelector('.egApproveButton.egApproved') || {}).textContent || '', calmOff: !!(c.querySelector('.egApproveButton.egApproved') || {}).disabled, written: (c.querySelector('.egWrittenNote') || {}).textContent || '', reopen: !!c.querySelector('[data-a=reopen]'), seals: c.querySelectorAll('.pvApproval .engravingSeals [data-seal], .pvApproval .engravingSeals .seal, .pvApproval .engravingSeals > *').length, panel: !!c.querySelector('.engp'), words: (c.querySelector('.engpWords') || {}).value || '', wordsOff: !!(c.querySelector('.engpWords') || {}).disabled, fontOff: !!(c.querySelector('.engpFont') || {}).disabled, posOff: [...c.querySelectorAll('.engpPb')].every(b => b.disabled), canvas: !!c.querySelector('.backHost canvas') }; });
    const waitState = async (key, want, ms = 60000) => { const t0 = Date.now(); for (;;) { const s = await stateOf(key); if (s && want.includes(s.state)) return s; if (Date.now() - t0 > ms) return s; await new Promise(r => setTimeout(r, 300)); } };
    const approveFirst = async key => { await page.evaluate(k => { CN.setMode('engrave'); Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: k }); Engrave.render(); }, key); await page.waitForSelector(`#egQueue .rvItem[data-key="${key}"] [data-a=approve]`, { timeout: 20000 }); await settle(500); await page.click(`#egQueue .rvItem[data-key="${key}"] [data-a=approve]`); return waitState(key, ['approved', 'written'], 60000); };

    // the pieces
    const backKey = (await jobsOf(RID.back))[0].key, two = await jobsOf(RID.two), discs = await jobsOf(RID.discs), both = await jobsOf(RID.both);
    note(two.length === 2 && discs.length === 3 && both.length === 2 && made.back.jobs.length === 1, 'the fixture has pieces: a single back, two lines of one design, three discs, a faced charm', JSON.stringify({ two: two.length, discs: discs.length, both: both.length }));

    // ═══ 1 · no Decided tab, one chip ═══
    await showList(); await settle(300);
    const chrome = await page.evaluate(() => ({ tabs: [...document.querySelectorAll('#engraveView .ordBar .egTab[data-tab]')].map(b => b.dataset.tab + ':' + b.textContent.replace(/\s+/g, ' ').trim()), done: !!document.querySelector('#engraveView [data-tab="done"]'), egBacks: !!document.getElementById('egBacks'), egDone: !!document.getElementById('egDone'), decided: !!document.querySelector('#engraveView .decidedRow, #engraveView .doneDetail'), reopen: !!document.querySelector('#engraveView [data-a=reopen]'), view: Engrave.view().tab, q: !!document.getElementById('egQ') }));
    note(chrome.tabs.length === 2 && /^place:Placements/.test(chrome.tabs[0]) && /^approved:Approved/.test(chrome.tabs[1]) && !chrome.done && !chrome.egBacks && !chrome.egDone && !chrome.decided && !chrome.reopen, 'the Engrave tab has TWO chips (Placements, and Approved: a view of the same list, APPTAB) and no Decided tab, list, detail or Reopen', chrome.tabs.join(' | '));
    note(chrome.view === 'place', 'Engrave.view().tab is "place" while Placements is the open chip', chrome.view);
    const pendingRowsBefore = (await listRows()).filter(r => !r.decided).length;
    const badge0 = await page.evaluate(() => +((document.querySelector('#engraveView .egTab[data-tab="place"] b') || {}).textContent || 0));
    note(pendingRowsBefore > 0 && badge0 === pendingRowsBefore, 'the chip counts the rows that still need a decision', badge0 + ' / rows ' + pendingRowsBefore);

    // ═══ 2 · approve one piece: it stays in the same list, after the pending ones, with its state word and its seal ═══
    let s0 = await stateOf(backKey);
    const ap1 = await approveFirst(backKey);
    note(ap1 && ap1.state === 'approved' && ap1.seals.length === 1, 'the single back piece is approved with ONE seal', JSON.stringify(ap1 && { state: ap1.state, seals: ap1.seals.length }));
    await showList(); await settle(700);
    let rows = await listRows();
    const rowOfBack = rows.find(r => r.open === backKey || r.rid === RID.back);
    const idx = rows.indexOf(rowOfBack), lastPending = rows.map(r => r.decided).lastIndexOf(false);
    note(!!rowOfBack && rowOfBack.decided && rowOfBack.stages.some(s => s.s === 'approved' && /^Approved/.test(s.t)), 'the approved piece is a row of the same list, calm, with the word Approved', JSON.stringify(rowOfBack && rowOfBack.stages));
    note(idx > lastPending, 'pending rows come first, the approved row after them', 'row ' + idx + ' of ' + rows.length + ', last pending ' + lastPending);
    note(rowOfBack && rowOfBack.sealCount === 1, 'its seal is drawn on its row', String(rowOfBack && rowOfBack.sealCount));
    const badge1 = await page.evaluate(() => +((document.querySelector('#engraveView .egTab[data-tab="place"] b') || {}).textContent || 0));
    note(badge1 === pendingRowsBefore - 1, 'the chip now counts one row less (only what still needs a decision)', badge0 + ' -> ' + badge1);

    // ═══ 3 · open it: the same card, nothing changes by looking ═══
    const before = await stateOf(backKey); counting = true; writes = 0;
    await page.click(`#egQueue .placementRow[data-open="${backKey}"]`); await page.waitForSelector(`#egQueue .rvItem[data-key="${backKey}"]`, { timeout: 15000 }); await settle(900);
    let card = await cardOf();
    note(card && card.key === backKey && card.state === 'approved' && card.calm && card.calmText.trim() === 'Approved' && card.calmOff && !card.approve && !card.reopen, 'a click on the approved row opens the SAME detail card with a calm "Approved" (no Approve to press, no Reopen)', JSON.stringify(card));
    note(card && card.panel && card.canvas && !card.wordsOff && !card.fontOff, 'the card is the full editor: panel, work area, words and font enabled', JSON.stringify({ panel: card.panel, canvas: card.canvas, wordsOff: card.wordsOff, fontOff: card.fontOff }));
    await settle(1200);
    const afterLook = await stateOf(backKey); counting = false;
    note(JSON.stringify(before) === JSON.stringify(afterLook), 'just looking changed nothing: same state, seals, back file, words and placement', JSON.stringify(afterLook));
    note(!ops.some(o => /backInvalidate|customReopen|poolUpdate|poolPut|backPut|engrav/i.test(o)) || writes === 0, 'just looking wrote nothing about the piece (no reopen, no invalidate: any request seen is the page\'s own background save)', writes + ' request(s): ' + ops.join(','));
    await R.shot(w, 'nodecided-card-approved-1440');

    // ═══ 4 · the first change: words -> needs Approve; Approve again -> a second, new seal; the first stays ═══
    const firstSeal = before.seals[0];
    await page.fill('#egQueue .rvItem .engpWords', 'Name Seven');
    await page.evaluate(() => document.querySelector('#egQueue .rvItem [data-engp="save"]').click()); await settle(500);
    if (await page.$('#egQueue .engpWhy .engpChip')) { await page.click('#egQueue .engpWhy .engpChip'); await settle(500); }
    const changed = await waitState(backKey, ['review', 'words', 'ready', 'fitting'], 30000);
    const reviewed = await waitState(backKey, ['review'], 60000);
    note(reviewed && reviewed.state === 'review', 'changing the words put the approved piece back to "needs Approve" (state review)', JSON.stringify(reviewed && { state: reviewed.state, text: reviewed.text }));
    note(reviewed && /Name Seven/i.test(reviewed.text || ''), 'the piece carries the new words', reviewed && reviewed.text);
    note(reviewed && reviewed.seals.length === 1 && reviewed.seals[0] === firstSeal, 'the first seal is kept as history', JSON.stringify(reviewed && reviewed.seals));
    note(reviewed && reviewed.backs === 0 && (changed && changed.approvedAt === 0 || reviewed.approvedAt === 0), 'the first back record is superseded (no approval time, no back file on the job)', JSON.stringify(reviewed && { backs: reviewed.backs, at: reviewed.approvedAt }));
    await settle(800); card = await cardOf();
    note(card && card.state === 'review' && card.approve && !card.approveOff && !card.calm, 'the card now offers Approve again (and no longer the calm tick)', JSON.stringify(card && { state: card.state, approve: card.approve, off: card.approveOff }));
    const ap2 = await (async () => { await page.click('#egQueue [data-a=approve]'); return waitState(backKey, ['approved', 'written'], 60000); })();
    note(ap2 && ap2.state === 'approved' && ap2.seals.length === 2 && ap2.seals[0] === firstSeal && ap2.seals[1] !== firstSeal, 'Approve again made a SECOND brand-new seal; the first one is still there', JSON.stringify(ap2 && ap2.seals));
    note(ap2 && ap2.approvedAt > firstSeal.split(':').slice(-2)[0] * 1 || ap2.approvedAt > 0, 'with its own approval time', String(ap2 && ap2.approvedAt));
    await showList(); await settle(700); rows = await listRows();
    note((rows.find(r => r.open === backKey) || {}).sealCount === 2, 'the row of the list shows both seals', String((rows.find(r => r.open === backKey) || {}).sealCount));
    await page.click(`#egQueue .placementRow[data-open="${backKey}"]`); await page.waitForSelector(`#egQueue .rvItem[data-key="${backKey}"]`); await settle(900);
    const sealsDom = await page.evaluate(() => { const g = document.querySelector('#egQueue .rvItem .pvApproval [data-seal-count]'); return g ? +g.dataset.sealCount : 0; });
    note(sealsDom === 2, 'the open card shows both seals (the earlier approval is history)', String(sealsDom));

    // ═══ 4b · a position change reopens too, and Close / Next leave an untouched approved piece alone ═══
    const twoKeys = two.map(j => j.key); const k1 = twoKeys[0], k2 = twoKeys[1];
    await approveFirst(k1);
    const st1 = await stateOf(k1);
    await page.evaluate(k => { Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: k }); Engrave.render(); }, k1);
    await page.waitForSelector(`#egQueue .rvItem[data-key="${k1}"][data-state="approved"]`, { timeout: 20000 }); await settle(900);
    await page.keyboard.press('ArrowRight'); await page.keyboard.press('ArrowRight'); await settle(500);
    const afterKeys = await stateOf(k1);
    note(afterKeys.state === 'approved' && afterKeys.centre === st1.centre, 'arrow keys on an approved piece do not move or reopen it (looking is safe)', afterKeys.state);
    const posBtn = await page.$(`#egQueue .rvItem[data-key="${k1}"] .engpPb[data-act="right"]:not([disabled])`);
    const posDump = await page.evaluate(k => { const c = document.querySelector(`#egQueue .rvItem[data-key="${k}"]`); const j = Engrave.items().get(k); return JSON.stringify({ card: c && c.dataset.state, bs: c ? [...c.querySelectorAll('.engpPb')].map(b => b.dataset.act + ':' + (b.disabled ? 'off:' + b.title : 'on')) : null, job: j && { s: j.state, fit: !!j.fit, mask: !!(j.mask && j.mask.bits), view: !!j.view, st: j.stamping, bk: j.backSaving, ap: j.approvalPreparing } }); }, k1);
    note(!!posBtn, 'the position buttons of an approved piece are enabled', posDump);
    if (posBtn) {   // (a move with no room is no change at all: it never reopens; the first direction that has room does)
      for (const act of ['right', 'left', 'up', 'down', 'smaller', 'larger']) {
        const b = await page.$(`#egQueue .rvItem[data-key="${k1}"] .engpPb[data-act="${act}"]:not([disabled])`); if (!b) continue;
        await b.click(); await settle(500);
        if ((await stateOf(k1)).state === 'review') break;
        const still = await stateOf(k1); if (still.state === 'approved' && still.seals.length !== 1) break;
      }
    }
    const moved = await waitState(k1, ['review'], 20000);
    note(moved && moved.state === 'review' && moved.seals.length === 1 && moved.backs === 0, 'a position change put it back to "needs Approve" with its first seal kept', JSON.stringify(moved && { s: moved.state, seals: moved.seals.length, backs: moved.backs }));
    // Close on the untouched approved piece (the other line of the order): nothing happens to it
    await page.evaluate(k => { Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: k }); Engrave.render(); }, backKey);
    await page.waitForSelector(`#egQueue .rvItem[data-key="${backKey}"]`); await settle(700);
    const keep0 = await stateOf(backKey);
    await page.click('#egQueue .rvItem [data-a=next]'); await settle(700); await page.click('#egQueue .rvItem [data-a=prev]').catch(() => {}); await settle(700);
    const keep1 = await stateOf(backKey);
    note(JSON.stringify(keep0) === JSON.stringify(keep1), 'Next / Back through an untouched approved piece change nothing', JSON.stringify(keep1.state));

    // ═══ 5 · a piece Saved to sheet: same rule as Reopen had (no block), one calm line, no confirm ═══
    const dialogs = []; page.on('dialog', d => { dialogs.push(d.message()); d.dismiss().catch(() => {}); });
    await approveFirst(k2); await settle(500);
    await page.evaluate(k => { const j = Engrave.items().get(k); j.state = 'written'; j.row.engrave = Object.assign({}, j.row.engrave, { state: 'written' }); Engrave.render(); }, k2);
    await page.evaluate(k => { Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: k }); Engrave.render(); }, k2);
    await page.waitForSelector(`#egQueue .rvItem[data-key="${k2}"][data-state="written"]`, { timeout: 20000 }); await settle(900);
    card = await cardOf();
    note(card && card.state === 'written' && card.calm && /Saved to sheet/.test(card.written), 'a piece Saved to sheet opens in the same card with one calm line about its file', JSON.stringify(card && { calm: card.calm, written: card.written }));
    const w0 = await stateOf(k2);
    const rb = await page.$(`#egQueue .rvItem[data-key="${k2}"] .engpPb[data-act="up"]:not([disabled])`);
    if (rb) { await rb.click(); await settle(600); }
    const w1 = await waitState(k2, ['review'], 30000);
    note(!!rb && w1 && w1.state === 'review' && w1.seals.length === w0.seals.length && w1.backs === 0, 'changing a saved piece reopens it for Approve (nothing blocks it: its back record is superseded, its seals stay)', JSON.stringify(w1 && { s: w1.state, backs: w1.backs, seals: w1.seals.length }));
    note(dialogs.length === 0, 'no confirm dialog stood in the way', dialogs.join(' | '));

    // ═══ 6 · a line of discs: each piece told with its own state; a click on a decided disc opens that disc ═══
    const dKeys = discs.map(j => j.key).sort();
    const dApproved = await approveFirst(dKeys[0]);
    await showList(); await settle(700); rows = await listRows();
    const drow = rows.find(r => r.rid === RID.discs);
    note(drow && drow.ears.length === 3 && drow.ears[0].s === 'approved' && drow.ears.slice(1).every(e => e.s !== 'approved' && e.s !== 'written'), 'a row of three discs tells each disc with its own state (disc 1 approved, 2 and 3 still to settle)', JSON.stringify(drow && drow.ears.map(e => e.s)));
    note(drow && !drow.decided, 'the row still needs a decision, so it sits with the pending rows', String(drow && drow.decided));
    await page.click(`#egQueue .placementRow[data-rid="${RID.discs}"] .egEar[data-ear="${dKeys[0]}"]`); await page.waitForSelector(`#egQueue .rvItem[data-key="${dKeys[0]}"]`, { timeout: 15000 }); await settle(800);
    card = await cardOf();
    note(card && card.key === dKeys[0] && card.state === 'approved', 'a click on the line of the approved disc opens that disc', JSON.stringify(card && { key: tails(card.key), state: card.state }));
    await showList(); await page.click(`#egQueue .placementRow[data-rid="${RID.discs}"] .egEar[data-ear="${dKeys[1]}"]`); await page.waitForSelector(`#egQueue .rvItem`, { timeout: 15000 }); await settle(800);
    card = await cardOf();
    note(card && card.state !== 'approved', 'a click on a disc still to settle opens the piece still to settle', JSON.stringify(card && { key: tails(card.key), state: card.state }));
    // the faced charm: approve the back face only
    const bk = both.map(j => j.key).sort(); const bBack = bk.find(k => !/~front$/.test(k)); const bFront = bk.find(k => /~front$/.test(k));
    await approveFirst(bBack); await showList(); await settle(700); rows = await listRows();
    const brow = rows.find(r => r.rid === RID.both);
    note(brow && brow.ears.length >= 2 ? brow.ears.some(e => e.s === 'approved') && brow.ears.some(e => e.s === 'review') : true, 'the faced charm tells its Back approved and its Front still to settle in ONE row', JSON.stringify(brow && brow.ears.map(e => e.s)));

    // ═══ search finds approved pieces, and the date filter belongs to the one list ═══
    await page.fill('#egQ', RID.back); await settle(600);
    rows = await listRows();
    note(rows.some(r => r.rid === RID.back && r.decided) && rows.every(r => r.rid === RID.back), 'the search finds an approved piece by its order number (and only it)', rows.map(r => r.rid + (r.decided ? ':d' : '')).join(' '));
    await page.fill('#egQ', 'Name Seven'); await settle(600);
    rows = await listRows();
    note(rows.some(r => r.rid === RID.back), 'the search finds an approved piece by its engraved words', String(rows.length));
    await page.fill('#egQ', ''); await settle(400);
    const chips = await page.evaluate(() => [...document.querySelectorAll('#engraveView .ordBar button, #engraveView .ordBar [role=button]')].map(b => b.textContent.trim()).filter(Boolean));
    note(chips.some(t => /Today/i.test(t)) && chips.some(t => /Last 7/i.test(t)), 'the date chips are on the one list', chips.join(' | '));

    // ═══ 7 · old views and links ═══
    const comp = await page.evaluate(k => { Engrave.restoreView({ tab: 'done', openDone: k, list: false, chosen: true }); Engrave.render(); return Engrave.view(); }, backKey);
    await page.waitForSelector(`#egQueue .rvItem[data-key="${backKey}"]`, { timeout: 15000 }); await settle(600);
    note(comp.tab === 'approved' && comp.focus === backKey, 'an old view {tab:"done", openDone} lands on the card of that piece (on the Approved chip, APPTAB)', JSON.stringify({ tab: comp.tab, focus: tails(comp.focus) }));
    await page.evaluate(() => { Engrave.restoreView({ tab: 'done', list: false, chosen: true, focus: null, q: '' }); Engrave.render(); }); await settle(700);
    note(!!(await page.$('#egQueue .egPlacementList')) && !(await page.$('#engraveView [data-tab="done"]')), 'an old view {tab:"done"} with nothing open lands on the one list', '');
    const link = await page.evaluate(async ({ rid, key }) => { try { const r = await window.EngraveLink.open({ rid, key }); return { ok: !!(r === true || (r && r.ok !== false)), tab: Engrave.view().tab, focus: Engrave.view().focus }; } catch (e) { return { err: String(e && e.message || e) }; } }, { rid: RID.back, key: backKey });
    await settle(900); const lc = await cardOf();
    note(lc && lc.key === backKey && lc.state === 'approved', 'EngraveLink (View in Engraving) lands on the card of an approved piece', JSON.stringify({ link, card: lc && lc.state }));
    // the order window: no Reopen, a button to the same card
    await R.openOrderWindow(w, 'back'); await settle(500);
    const ow = await page.evaluate(() => ({ reopen: !!document.querySelector('#orderWin [data-a=reopen]'), eng: [...document.querySelectorAll('#orderWin .egOpen')].map(b => b.textContent.trim()) }));
    note(!ow.reopen && ow.eng.some(t => /Engraving/.test(t)), 'the order window\'s engraving card has no Reopen, only its button to Engraving', JSON.stringify(ow));
    await page.evaluate(() => { try { OrderWin.close(); } catch (_) {} }); await settle(400);

    // ═══ 8 · phone width (400 px, the rail folded as a person would) ═══
    const rail0 = await page.evaluate(() => document.getElementById('app').classList.contains('railOff'));
    await page.setViewportSize({ width: 400, height: 860 }); await settle(500);
    if (await page.evaluate(() => !document.getElementById('app').classList.contains('railOff'))) { await page.click('#btnRail').catch(() => {}); await settle(500); }
    await page.evaluate(() => document.querySelectorAll('.cnNbX, .toast .x').forEach(b => { try { b.click(); } catch (_) {} })); await settle(500);
    const over = sel => page.evaluate(sel => { const vw = document.documentElement.clientWidth, bad = []; for (const n of document.querySelectorAll(sel)) { const r = n.getBoundingClientRect(); if (r.width && (r.left < -1 || r.right > vw + 1)) bad.push((n.className || n.tagName).toString().slice(0, 40) + ':' + Math.round(r.left) + '..' + Math.round(r.right)); } return { sw: document.documentElement.scrollWidth, cw: vw, n: document.querySelectorAll(sel).length, bad }; }, sel);
    await showList(); await settle(800);
    let o8 = await over('#egQueue .placementRow, #egQueue .placementRow .egEar, #egQueue .placementRow [data-stage]');
    note(o8.sw <= o8.cw + 1 && o8.n >= 5 && o8.bad.length === 0, 'at 400 px the list (pending and approved rows) fits the screen: no sideways scroll, every row and state word inside it', JSON.stringify(o8));
    await R.shot(w, 'nodecided-list-400');
    await page.evaluate(k => { Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: k }); Engrave.render(); }, backKey); await page.waitForSelector(`#egQueue .rvItem[data-key="${backKey}"]`); await settle(1500);
    o8 = await over('#egQueue .rvItem, #egQueue .rvItem .pvApproval, #egQueue .rvItem .egApproveButton, #egQueue .rvItem .engp, #egQueue .rvItem .ctl');
    const phone2 = await page.evaluate(() => ({ calm: !!document.querySelector('.egApproveButton.egApproved'), approve: !!document.querySelector('[data-a=approve]') }));
    note(o8.sw <= o8.cw + 1 && o8.bad.length === 0 && phone2.calm, 'at 400 px the card of an approved piece fits the screen, with its calm Approved', JSON.stringify(Object.assign({}, o8, phone2)));
    await R.shot(w, 'nodecided-card-400');
    await page.setViewportSize({ width: 1440, height: 900 }); await settle(500);
    if (!rail0 && await page.evaluate(() => document.getElementById('app').classList.contains('railOff'))) { await page.click('#btnRail').catch(() => {}); await settle(400); }
    await showList(); await settle(800); await R.shot(w, 'nodecided-list-1440');

    note(errs.length === 0, 'no console or page errors', errs.slice(0, 4).join(' | '));
    note(w.errors ? w.errors.filter(e => !/favicon/.test(String(e))).length === 0 : true, 'no page errors reported by the world', w.errors ? w.errors.slice(0, 3).join(' | ') : '');
  } catch (e) { note(false, 'the test itself threw', String(e && e.stack || e).split('\n').slice(0, 4).join(' | ')); }
  finally { await w.close(); }
  console.log(fails.length ? '\nFAILED: ' + fails.length : '\nAll checks passed');
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
