// ENGSEL (engrave-prenest-1013): the order window's ENGRAVING section follows the piece that is selected (Paul, 13 Oct 2026):
//   "In a multipiece order if I select a certain piece, then the back engraving should reflect the selected piece not the one prior ... I've selected the hummingbird earrings, but the astronaut is still showing for
//    the engraving, which is incorrect because the astronaut is the other charm which I don't have currently selected so it should have disappeared since the earrings don't have any back engraving."
// The section = the title + rule + the cards (#owEng). (It used to carry the customer's words under the cards too, #owEngSaid; NOSAID, Paul 17 Oct 2026, took that block away: here it is checked ABSENT in every state, and the words
// follow the pick in the Overview's own "What the customer wrote" box, #owNotes.) Expected: ONE piece selected -> only that piece's cards; "Show all" -> every engraved piece; a selected
// piece with nothing to engrave (an earring pair, a plain piece) -> no section at all (host hidden, no title, no rule, no room); the swap is in the SAME tick as the selection (no stale card, no flash, no empty gap),
// whichever path changed the selection: a click on a row of Its pieces, the Left | Right switch of the pair, "Show all", OrderWin.selectPiece, opening the window on a piece, the Sheet tab, the keyboard.
//
// Offline only: the REAL page over the repo's fake site with the real drawings (_engraveRealWorld.cjs: real intake, pool, classifier, fit). Fake buyer names only; no Etsy, no paid call, no live write.
// The mixed order (a back-only necklace, an earring pair, a front + back necklace, a plain necklace, a necklace of three discs, ONE order) is built here; the shared world file is not edited.
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/engrave-section-follows-piece.cjs      (CHROMIUM=<chrome>  SHOTS=<dir>  SEL_KEEP=1 prints the table)
// What a pass does NOT prove: a live order on the live page (live data, live jobs); only that the page's own code follows the selection on the offline fake.
'use strict';
const path = require('path');
const R = require('./_engraveRealWorld.cjs'), Base = require('./_orderFactsWorld.cjs');

// the order as Etsy sends it, with a personalisation and a listing for every line (the shared builder gives the extra lines none)
function orderOf(fx, now) {
  const at = (d, h, m) => { const t = new Date(now); t.setDate(t.getDate() + d); t.setHours(h, m, 0, 0); return Math.floor(t.getTime() / 1000); };
  const shipBy = at(fx.ship, 17, 0), line = (x, i) => ({ transactionId: fx.rid + (i + 1), listingId: x.listing, sku: x.sku, title: x.title, quantity: x.qty || 1, expectedShipDate: shipBy, variations: x.variations || [], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: x.personalization || [], buyerMessage: '' });
  return { receiptId: fx.rid, orderNumber: fx.rid, createTs: at(fx.placed.day, fx.placed.h, fx.placed.m), updateTs: at(0, 12, 0), shipBy, buyer: { name: fx.buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: fx.lines.map(line) };
}
const V = (name, value) => ({ name, value });
const L = {
  back: { listing: '1890000101', sku: 'COMET_9427', title: 'Comet Necklace, Celestial Charm Necklace in 14k Gold Fill', variations: [V('Metal choice', 'Gold'), V('Font Choice', 'Stylish')], personalization: ['Name One'] },
  ears: { listing: '1890000106', sku: 'COMET_9427', title: 'Comet Stud Earrings, Celestial Earrings in 14k Gold Fill', variations: [V('Metal choice', 'Gold'), V('Style', 'Stud earrings')], personalization: [] },
  both: { listing: '1890000103', sku: 'SCHRODINGER CAT 2', title: 'Cat Necklace, Whimsical Cat Charm, engraved on both sides', variations: [V('Metal choice', 'Gold'), V('Engraving Side', 'Both'), V('Font Choice', 'Ace')], personalization: ['front - W\nback - Name Two'] },
  plain: { listing: '1890000107', sku: 'SCHRODINGER CAT 1', title: 'Cat Necklace, Whimsical Cat Charm Gift', variations: [V('Metal choice', 'Gold')], personalization: [] },
  discs: { listing: '1890000105', sku: 'COMET_9427', title: 'Initial Disc Necklace, Personalized Letter Discs', variations: [V('Metal choice', 'Gold'), V('Number of Discs', '3 Discs')], personalization: ['Tag 1: A, Tag 2: B, Tag 3: C'] }
};
const MIXED = { rid: '4290000121', buyer: 'Name Nine', placed: { day: 0, h: 11, m: 10 }, ship: 5, lines: [L.back, L.ears, L.both, L.plain, L.discs] };
const PAIR = { rid: '4290000122', buyer: 'Name Ten', placed: { day: 0, h: 11, m: 20 }, ship: 5, lines: [L.back, L.ears] };   // the smallest order of the picture: a charm and an earring pair
Base.orderOf = orderOf;                                                                 // (R.build serialises Base.orderOf into the page)
R.ORDERS.mixed = MIXED; R.EXPECT_JOBS.mixed = 1 + 0 + 2 + 0 + 3;
R.ORDERS.pair = PAIR; R.EXPECT_JOBS.pair = 1;

const fails = [], note = (ok, what, more) => { console.log((ok ? '  PASS ' : '  FAIL ') + what + (more ? '  [' + more + ']' : '')); if (!ok) fails.push(what); };

/** What the section is, read in the page: the host's state, the title and rule's box, the cards (line key, slot, faces) and the words block. */
const SECTION = () => {
  const host = document.getElementById('owEng'), sec = document.getElementById('owEngSec'), win = document.getElementById('orderWin');
  const title = sec && sec.querySelector('.engSecHd'), cs = [...(host ? host.children : [])].filter(n => n.classList.contains('engCard'));
  const box = n => { if (!n) return null; const r = n.getBoundingClientRect(), cs2 = getComputedStyle(n); return { h: Math.round(r.height * 10) / 10, shown: cs2.display !== 'none' && cs2.visibility !== 'hidden' && r.height > 0 }; };
  return {
    hostHidden: !host || host.hidden, kids: host ? host.children.length : 0,
    titleShown: !!(title && box(title).shown), secH: sec ? Math.round(sec.getBoundingClientRect().height) : -1, secBorder: sec ? parseFloat(getComputedStyle(sec).borderTopWidth) || 0 : 0,
    cards: cs.map(c => ({ line: c.dataset.key || '', piece: c.dataset.piece || '', faces: [...c.querySelectorAll('.engFace')].map(f => f.dataset.face).join('+'), thumbs: [...c.querySelectorAll('.engThumb')].map(t => !!(t.querySelector('svg,canvas,img') || t.style.backgroundImage || t.childElementCount)).join(','), name: (c.querySelector('.engName') || {}).textContent || '' })),
    saidBlocks: win ? win.querySelectorAll('#owEngSaid, .engSaid, .esRow, .esName, [data-said]').length : -1,   // (NOSAID: the customer's words block under the cards: none, in any state)
    selected: window.OrderWin.selectedPiece(), side: window.OrderWin.selectedSide(),
    shownLine: (document.querySelector('#orderWin .owPcRow[aria-current=true]') || {}).dataset ? ((document.querySelector('#orderWin .owPcRow[aria-current=true]') || {}).dataset.piece || '') : '',
    pcState: (document.querySelector('#owPcSum .owPcState') || {}).textContent || ''
  };
};
/** One look at the section in the SAME tick as `act` (act runs inside the page, then the section is read with no await in between: a stale card, or a hidden gap, would show here). */
const sameTick = (page, actSrc, arg) => page.evaluate(({ actSrc, arg, secSrc }) => { const act = eval('(' + actSrc + ')'), sec = eval('(' + secSrc + ')'); const before = sec(); act(arg); return { before, after: sec() }; }, { actSrc: String(actSrc), arg, secSrc: String(SECTION) });
const settledSection = async (page, ms = 700) => { await R.settle(page, ms); return page.evaluate(`(${SECTION})()`); };

(async () => {
  const w = await R.world({ viewport: { width: 1356, height: 900 } });
  if (!w) { console.log('no playwright-core: nothing run'); return; }
  const page = w.page;
  try {
    const made = await R.build(w, ['mixed', 'pair']);
    const rows = await page.evaluate(() => { const o = {}; for (const r of Orders.rows()) (o[r.order.receiptId] ||= []).push({ key: r.key, sku: r.spec && r.spec.designSku, state: r.state, jobs: Engrave.jobsOf(r).map(j => j.key + ':' + j.state) }); return o; });
    if (process.env.SEL_KEEP) console.log(JSON.stringify({ rows, made: Object.fromEntries(Object.entries(made).map(([k, v]) => [k, v.jobs.length])) }, null, 1));
    const rid = MIXED.rid, keyBy = i => rows[rid][i].key;
    const [kBack, kEars, kBoth, kPlain, kDiscs] = [0, 1, 2, 3, 4].map(keyBy);
    note(rows[rid].length === 5, 'the mixed order has its five lines in ONE order (' + rows[rid].length + ')');
    note((made.mixed.jobs || []).length === 6, 'the real classifier made the 6 jobs of the engraved lines: back 1, both 2, discs 3 (' + (made.mixed.jobs || []).length + ')');

    // ── open the window on the order (nothing picked: every piece) ──
    await page.evaluate(async k => { CN.setMode('orders'); await OrderWin.open(k); }, kBack);
    await page.waitForFunction(() => { const h = document.getElementById('owEng'); return h && !h.hidden && h.querySelector('.engCard'); }, null, { timeout: 20000 }).catch(() => {});
    let s = await settledSection(page);
    const lineOfCard = c => c.line;
    const set = cs => [...new Set(cs.map(lineOfCard))].sort().join(' ');
    const engraved = [kBack, kBoth, kDiscs].sort().join(' ');
    note(s.selected === null && set(s.cards) === engraved, 'nothing picked: the cards of every engraved piece (back necklace, both-faces necklace, three discs) and none for the earrings or the plain piece', 'cards ' + s.cards.length + ' lines ' + set(s.cards).split(' ').length);
    note(s.cards.length === 1 + 1 + 3, 'one card for each piece: 1 + 1 + 3 discs = 5 cards (' + s.cards.length + ')');
    note(!s.cards.some(c => c.line === kEars) && !s.cards.some(c => c.line === kPlain), 'the earring pair and the plain piece have no card');
    note(s.titleShown && s.secBorder > 0 && !s.hostHidden, 'the section shows its title and rule');
    note(s.saidBlocks === 0, 'NOSAID: no customer-words block under the cards (the words are in the Overview\'s own "What the customer wrote" box, which follows the pick: checked below)', 'blocks ' + s.saidBlocks);

    const pick = key => `(k) => { const ok = OrderWin.selectPiece(k); return ok; }`;
    // ── the earring row: no section at all ──
    let r = await sameTick(page, k => OrderWin.selectPiece(k), kEars);
    note(r.after.selected === kEars, 'the earring row is selected', String(r.after.selected));
    note(r.after.hostHidden && r.after.kids === 0, 'earrings selected: the engraving cards are gone in the same tick (no stale card)', 'kids ' + r.after.kids + ' hidden ' + r.after.hostHidden);
    note(!r.after.titleShown && r.after.secH <= 1 && r.after.secBorder === 0, 'earrings selected: no title, no rule, no room (section height ' + r.after.secH + ' px)', 'title ' + r.after.titleShown + ' border ' + r.after.secBorder);
    note(r.after.saidBlocks === 0, 'earrings selected: no words block either', 'blocks ' + r.after.saidBlocks);
    s = await settledSection(page);
    note(s.hostHidden && s.kids === 0 && !s.titleShown && s.saidBlocks === 0, 'earrings selected, a moment later: still no section, nothing came back');

    // ── the Left | Right switch of the pair ──
    r = await sameTick(page, () => { const b = document.querySelector('#owPcSum .egEarTab[data-ear$="#L"]'); if (!b) throw new Error('no Left tab'); b.click(); });
    note(r.after.side === 'L' && r.after.selected === kEars, 'the Left tab picks the left ear', r.after.selected + ' ' + r.after.side);
    note(r.after.hostHidden && r.after.kids === 0 && !r.after.titleShown, 'the left ear picked: no engraving section');
    r = await sameTick(page, () => { const b = document.querySelector('#owPcSum .egEarTab[data-ear$="#R"]'); if (!b) throw new Error('no Right tab'); b.click(); });
    note(r.after.side === 'R' && r.after.hostHidden && r.after.kids === 0, 'the right ear picked: still no engraving section');

    // ── the back necklace row ──
    r = await sameTick(page, k => { const row = document.querySelector('#owPcSum .owPcRow[data-piece="' + k + '"] .owPcName'); if (!row) throw new Error('no row'); row.click(); }, kBack);
    note(r.after.selected === kBack && r.after.side === null, 'a click on the necklace row picks it (the ear is dropped)', r.after.selected + ' ' + r.after.side);
    note(r.after.cards.length === 1 && r.after.cards[0].line === kBack && !r.after.hostHidden, 'necklace selected: its card alone, drawn in the same tick', JSON.stringify(r.after.cards.map(c => c.line.slice(-4) + ' ' + c.faces)));
    note(r.after.cards[0] && !/\bfront\b/.test(r.after.cards[0].faces) && /back/.test(r.after.cards[0].faces), 'its card has the Back only (a back-only piece)', r.after.cards[0] && r.after.cards[0].faces);
    note(r.after.titleShown && r.after.secBorder > 0, 'necklace selected: the title and rule are back');
    note(r.after.saidBlocks === 0, 'necklace selected: still no words block under its card (its own words are in the Overview\'s box: checked below)', 'blocks ' + r.after.saidBlocks);
    note(r.after.cards[0] && /^[,]*true/.test(r.after.cards[0].thumbs), 'the card\'s thumbnail is already painted in the same tick (no blank flash)', r.after.cards[0] && r.after.cards[0].thumbs);

    // ── Show all ──
    r = await sameTick(page, () => document.querySelector('#owPcSum [data-pc-all]').click());
    note(r.after.selected === null && set(r.after.cards) === engraved && r.after.cards.length === 5, 'Show all: every engraved piece again, in the same tick (5 cards: necklace, both-faces necklace, 3 discs)', r.after.cards.length + ' ' + r.after.selected);
    note(r.after.saidBlocks === 0, 'Show all: no words block under the cards either (NOSAID: the words of every piece are no longer said under them)', 'blocks ' + r.after.saidBlocks);
    // ── the earring row, then Show all: the cards are not lost ──
    // ── a front + back necklace ──
    r = await sameTick(page, k => OrderWin.selectPiece(k), kBoth);
    note(r.after.cards.length === 1 && r.after.cards[0].line === kBoth && /front\+back/.test(r.after.cards[0].faces), 'the front + back necklace selected: its one card with both faces', JSON.stringify(r.after.cards.map(c => c.faces)));
    note(r.after.saidBlocks === 0, 'its card has no words block under it', 'blocks ' + r.after.saidBlocks);
    // ── the plain necklace: nothing to engrave, no section ──
    r = await sameTick(page, k => OrderWin.selectPiece(k), kPlain);
    note(r.after.hostHidden && r.after.kids === 0 && !r.after.titleShown && r.after.secH <= 1 && r.after.saidBlocks === 0, 'the plain necklace selected: no engraving section at all');
    // ── the discs: the line's three disc cards, and nothing of the other pieces ──
    r = await sameTick(page, k => OrderWin.selectPiece(k), kDiscs);
    note(r.after.cards.length === 3 && r.after.cards.every(c => c.line === kDiscs), 'the three-disc necklace selected: its three disc cards and no other piece\'s', r.after.cards.map(c => c.line.slice(-5) + '/' + c.piece.slice(-3)).join(' '));
    // ENGPICK (13 Oct 2026, "make a single disc of a multi-disc line selectable too"): a press on one disc's own line in the drawer picks THAT disc: its card alone (the item stays picked, the rest of the window is the item's);
    // the same disc pressed again goes back to the item's three cards; another piece or Show all drops the disc.
    await page.evaluate(() => OrderWin.selectPiece(null)); await R.settle(page, 300);
    r = await sameTick(page, () => { const d = document.querySelector('#owPcSum .owPcSub[data-sub$="#D2"]'); if (!d) throw new Error('no disc 2 line in the drawer'); d.click(); });
    note(r.after.selected === kDiscs && r.after.cards.length === 1 && r.after.cards[0].line === kDiscs && /#D2$/.test(r.after.cards[0].piece), 'a press on "disc 2" in the drawer picks that disc: its card alone, in the same tick', r.after.selected + ' / ' + r.after.cards.map(c => c.piece.slice(-3)).join(','));
    note(await page.evaluate(() => OrderWin.selectedDisc()) === 'D2' && /Disc 2/.test(r.after.cards[0] && r.after.cards[0].name || '') && /Showing .*Disc 2/.test(await page.evaluate(() => document.querySelector('#owPcSum .owPcState').textContent)), 'the disc picked is Disc 2 (the card says Disc 2, the header says "Showing ... Disc 2")');
    r = await sameTick(page, () => document.querySelector('#owPcSum .owPcSub[data-sub$="#D3"]').click());
    note(r.after.cards.length === 1 && /#D3$/.test(r.after.cards[0].piece), 'a press on disc 3 moves the pick to disc 3: its card alone', r.after.cards.map(c => c.piece.slice(-3)).join(','));
    r = await sameTick(page, () => document.querySelector('#owPcSum .owPcSub[data-sub$="#D3"]').click());
    note(r.after.selected === kDiscs && r.after.cards.length === 3 && r.after.cards.every(c => c.line === kDiscs) && await page.evaluate(() => OrderWin.selectedDisc()) === null, 'the same disc pressed again goes back to the item: its three disc cards', r.after.cards.length + ' cards');
    await page.evaluate(() => document.querySelector('#owPcSum .owPcSub[data-sub$="#D1"]').click()); await R.settle(page, 300);
    r = await sameTick(page, k => OrderWin.selectPiece(k), kBack);
    note(r.after.cards.length === 1 && r.after.cards[0].line === kBack && await page.evaluate(() => OrderWin.selectedDisc()) === null, 'another piece picked: the disc is dropped, that piece\'s card alone', r.after.cards.map(c => c.line.slice(-4)).join(','));
    await page.evaluate(k => OrderWin.selectPiece(k + '#D2'), kDiscs); await R.settle(page, 300);
    r = await sameTick(page, () => document.querySelector('#owPcSum [data-pc-all]').click());
    note(r.after.selected === null && r.after.cards.length === 5 && await page.evaluate(() => OrderWin.selectedDisc()) === null, 'Show all drops the disc too: every card again', r.after.cards.length + ' cards');
    note(await page.evaluate(k => OrderWin.selectPiece(k + '#D9'), kDiscs) === false, 'a disc the line does not have (disc 9 of 3) is refused');
    // ── the keyboard ──
    await page.evaluate(() => OrderWin.selectPiece(null)); await R.settle(page, 300);
    r = await sameTick(page, k => { const nm = document.querySelector('#owPcSum .owPcRow[data-piece="' + k + '"] .owPcName'); nm.focus(); nm.dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowDown', bubbles: true })); }, kBack);
    note(r.after.selected && r.after.selected !== kBack, 'ArrowDown moves the pick to the row below', String(r.after.selected));
    note(set(r.after.cards) === (r.after.selected === kEars || r.after.selected === kPlain ? '' : r.after.selected) || r.after.hostHidden, 'the keyboard\'s pick shows only its own cards (or none)', set(r.after.cards).split(' ').length + ' ' + (r.after.selected || '').slice(-4));

    // ── no flash: after a selection, in every frame the screen draws, the section shows exactly the pieces picked (no old card, no empty gap) ──
    const frames = (act, arg) => page.evaluate(async ({ actSrc, arg }) => {
      const host = document.getElementById('owEng'), sec = document.getElementById('owEngSec'), look = () => (host.hidden ? 'none' : [...host.children].map(c => (c.dataset.key || '?') + (c.dataset.piece || '').replace(/^.*#/, '#')).join(',') || 'empty') + (sec.getBoundingClientRect().height > 2 || host.hidden ? '' : ' flat');
      const out = []; let on = true; const raf = () => { if (!on) return; out.push(look()); requestAnimationFrame(raf); };
      eval('(' + actSrc + ')')(arg); out.push(look()); requestAnimationFrame(raf);
      await new Promise(r => setTimeout(r, 600)); on = false; return out;
    }, { actSrc: String(act), arg });
    const uniq = a => [...new Set(a)];
    await page.evaluate(() => OrderWin.selectPiece(null)); await R.settle(page, 500);
    let fr = await frames(k => OrderWin.selectPiece(k), kBack);
    note(uniq(fr).length === 1 && uniq(fr)[0].startsWith(kBack) && !uniq(fr)[0].includes(',') && fr.length > 5, 'all pieces -> the necklace: every frame shows the necklace card only (never the other four cards)', fr.length + ' frames: ' + uniq(fr).map(f => f.replace(/4290000121_4290000121/g, '')).join(' | '));
    fr = await frames(k => OrderWin.selectPiece(k), kEars);
    note(uniq(fr).length === 1 && uniq(fr)[0] === 'none', 'the necklace -> the earrings: every frame shows no section (never the necklace card, never an empty box)', fr.length + ' frames: ' + uniq(fr).map(f => f.replace(/4290000121_4290000121/g, '')).join(' | '));
    fr = await frames(k => OrderWin.selectPiece(k), kBoth);
    note(uniq(fr).length === 1 && uniq(fr)[0].startsWith(kBoth) && !uniq(fr)[0].includes(','), 'the earrings -> the front + back necklace: every frame shows its card only', uniq(fr).map(f => f.replace(/4290000121_4290000121/g, '')).join(' | '));
    fr = await frames(k => OrderWin.selectPiece(k), kBack);
    note(uniq(fr).length === 1 && uniq(fr)[0].startsWith(kBack) && !uniq(fr)[0].includes(','), 'one necklace -> the other: every frame shows the new card only (no moment with the old one, none with both)', uniq(fr).map(f => f.replace(/4290000121_4290000121/g, '')).join(' | '));
    fr = await frames(() => OrderWin.selectPiece(null));
    note(uniq(fr).length === 1 && uniq(fr)[0].split(',').length === 5, 'the necklace -> Show all: every frame shows all five cards', uniq(fr).map(f => f.replace(/4290000121_4290000121/g, '')).join(' | ').slice(0, 200));

    // ── the other per-piece engraving surfaces of the window follow the same pick (audit): the facts' engraving chip, the Details' Engraving cell, "What the customer wrote", the SKU ──
    const FACTS = () => ({ sku: (document.getElementById('owSku') || {}).textContent || '', notes: (document.getElementById('owNotes') || {}).innerText || '', chips: [...document.querySelectorAll('#owMeta .owfChip, #owMeta [data-chip]')].map(n => n.innerText.replace(/\s+/g, ' ').trim()).join(' | '), meta: (document.getElementById('owMeta') || {}).innerText.replace(/\s+/g, ' ').trim(), sel: OrderWin.selectedPiece() });
    const factsOf = async k => { await page.evaluate(k => OrderWin.selectPiece(k), k); await R.settle(page, 500); return page.evaluate(`(${FACTS})()`); };
    const fBack = await factsOf(kBack), fEars = await factsOf(kEars), fBoth = await factsOf(kBoth), fPlain = await factsOf(kPlain);
    if (process.env.SEL_KEEP) console.log(JSON.stringify({ fBack, fEars, fBoth, fPlain }, null, 1));
    note(/Name One/.test(fBack.notes) && !/Name Two/.test(fBack.notes) && fBack.sel === kBack, 'What the customer wrote follows the pick: the necklace\'s own words', fBack.notes.replace(/\s+/g, ' ').slice(0, 80));
    note(/Name Two/.test(fBoth.notes) && !/Name One/.test(fBoth.notes), 'What the customer wrote follows the pick: the front + back necklace\'s own words', fBoth.notes.replace(/\s+/g, ' ').slice(0, 80));
    note(!/Name One|Name Two/.test(fEars.notes) && !/Name One|Name Two/.test(fPlain.notes), 'What the customer wrote: an earring pair or a plain piece shows none of the other pieces\' words');
    note(!/Name One|Name Two/.test(fEars.meta + fPlain.meta), 'the facts of an earring pair or a plain piece (chips, Details) carry none of the other pieces\' words');
    note(/COMET/i.test(fBack.sku) && /SCHRODINGER/i.test(fBoth.sku) && /SCHRODINGER/i.test(fPlain.sku), 'the SKU under the pictures follows the pick', [fBack.sku, fEars.sku, fBoth.sku].join(' / '));

    // ── a pick made while another tab is open (the Overview is hidden): back on the Overview the section is the pick's from the first frame ──
    await page.evaluate(() => OrderWin.selectPiece(null)); await R.settle(page, 400);
    await page.evaluate(() => OrderWin.setView('timeline')); await R.settle(page, 500);
    await page.evaluate(k => OrderWin.selectPiece(k), kEars); await R.settle(page, 300);
    r = await sameTick(page, () => OrderWin.setView('info'));
    note(r.after.selected === kEars && r.after.hostHidden && r.after.kids === 0 && !r.after.titleShown, 'a pick made on the Timeline tab (earrings): back on the Overview there is no engraving section, not even for a frame', 'kids ' + r.after.kids);
    await page.evaluate(() => OrderWin.setView('timeline')); await R.settle(page, 300);
    await page.evaluate(k => OrderWin.selectPiece(k), kBoth); await R.settle(page, 300);
    r = await sameTick(page, () => OrderWin.setView('info'));
    note(r.after.cards.length === 1 && r.after.cards[0].line === kBoth, 'a pick made on the Timeline tab (the front + back necklace): back on the Overview its card alone', r.after.cards.length + ' cards');

    // ── open the window ON a piece (a piece dot's press): the section is that piece's from the first draw ──
    await page.evaluate(() => OrderWin.close()); await R.settle(page, 500);
    await page.evaluate(async k => { await OrderWin.open(k, { pick: true }); }, kEars);
    await page.waitForSelector('#orderWin[open]', { timeout: 10000 }).catch(() => {});
    s = await settledSection(page, 500);
    note(s.selected === kEars && s.hostHidden && s.kids === 0 && !s.titleShown, 'opened on the earrings: no engraving section from the first draw', String(s.selected) + ' kids ' + s.kids);
    await page.evaluate(() => OrderWin.close()); await R.settle(page, 500);
    await page.evaluate(async k => { await OrderWin.open(k, { pick: true }); }, kBoth);
    await page.waitForFunction(() => { const h = document.getElementById('owEng'); return h && !h.hidden && h.querySelector('.engCard'); }, null, { timeout: 15000 }).catch(() => {});
    s = await settledSection(page, 500);
    note(s.selected === kBoth && s.cards.length === 1 && s.cards[0].line === kBoth, 'opened on the front + back necklace: its card alone from the first draw', s.cards.length + ' cards');

    // (the Sheet tab: a sheet picked there picks its piece through the same pickPiece, OrderWin's SCOPE.follow; the offline world has no sheet in a Library, so the tab cannot walk sheets here: not run, only the picks made while another tab is open are)

    // ── the smallest order of the picture: a charm and an earring pair ──
    const pr = rows[PAIR.rid], pBack = pr[0].key, pEars = pr[1].key;
    await page.evaluate(async k => { OrderWin.close(); await new Promise(r => setTimeout(r, 300)); await OrderWin.open(k); }, pBack);
    await page.waitForFunction(() => { const h = document.getElementById('owEng'); return h && !h.hidden && h.querySelector('.engCard'); }, null, { timeout: 15000 }).catch(() => {});
    s = await settledSection(page, 400);
    note(s.cards.length === 1 && s.cards[0].line === pBack, 'a charm and an earring pair: the charm\'s card is shown', s.cards.length + ' cards');
    r = await sameTick(page, k => OrderWin.selectPiece(k), pEars);
    note(r.after.hostHidden && r.after.kids === 0 && !r.after.titleShown, 'the picture\'s case: the earring row picked, the charm\'s card is gone and the section too');
    r = await sameTick(page, () => OrderWin.selectPiece(null));
    note(r.after.cards.length === 1 && r.after.cards[0].line === pBack, 'and Show all brings the charm\'s card back');
    r = await sameTick(page, k => OrderWin.selectPiece(k), pBack);
    note(r.after.cards.length === 1 && r.after.cards[0].line === pBack, 'the charm picked: its card');

    note(w.errors.length === 0, 'no page errors', w.errors.slice(0, 2).join(' | '));
    const warns = w.warns.filter(x => !/sheet metadata This sheet is no longer in the Library/.test(x));   // (the offline world's sheets are not in a Library: the Sheet tab says so, on clean main too)
    note(warns.length === 0, 'no swallowed order-window errors', warns.slice(0, 2).join(' | '));
    if (process.env.SHOTS) { await R.shot(w, 'SEL-window'); }
  } catch (e) { console.log('  FAIL test crashed: ' + (e && e.stack || e)); fails.push('crash'); }
  finally { await w.close(); }
  console.log(fails.length ? `\n${fails.length} FAILED:\n - ${fails.join('\n - ')}` : '\nAll checks passed');
  process.exit(fails.length ? 1 : 0);
})();
