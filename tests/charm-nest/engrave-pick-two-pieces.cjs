// ENGPICK (engpieces-1013, point 1): the order window's ENGRAVING section for an order of TWO PIECES of ONE design in two metals (Paul, 13 Oct 2026, with the order of his pictures: a necklace in sterling
// silver and the same necklace in gold filled, two lines of one order, both engraved on the back):
//   "I chose the gold and the silver, but the engraving card did not adapt."
// ENGSEL made the section follow the pick; but the two cards were IDENTICAL (the same design name "NEURON 370...", the same words, the same picture), and the card's leading dot was the STATE dot (amber for "to
// approve", which reads as "gold" beside the Its pieces list's metal-coloured dots), so a pick of the silver piece looked unchanged. Expected now: a card of an order of several lines names its piece the way the
// Its pieces list does (the metal's dot, and "SS" / "GF" after the name, which never gives way when the name is cut), the header says "Showing <name> · SS" when two pieces share a name, a pick shows exactly that
// piece's card (same design, own words, own Approve), "Show all" shows both cards telling apart at a glance, and an Approve approves only its own piece. A single-line order's card is exactly what it was.
//
// Offline only: the REAL page over the repo's fake site with the real drawings (_engraveRealWorld.cjs). Fake buyer names ("Name One") only; no Etsy, no paid call, no live write.
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/engrave-pick-two-pieces.cjs      (CHROMIUM=<chrome>  SHOTS=<dir>  PK_KEEP=1 prints the table)
// What a pass does NOT prove: a live order on the live page (live data, live jobs); only that the page's own code names and follows the pieces on the offline fake.
'use strict';
const R = require('./_engraveRealWorld.cjs'), Base = require('./_orderFactsWorld.cjs');

function orderOf(fx, now) {
  const at = (d, h, m) => { const t = new Date(now); t.setDate(t.getDate() + d); t.setHours(h, m, 0, 0); return Math.floor(t.getTime() / 1000); };
  const shipBy = at(fx.ship, 17, 0), line = (x, i) => ({ transactionId: fx.rid + (i + 1), listingId: x.listing, sku: x.sku, title: x.title, quantity: x.qty || 1, expectedShipDate: shipBy, variations: x.variations || [], metalKey: x.metalKey || 'gold', metalLabel: x.metalLabel || 'GF 14/20', personalization: x.personalization || [], buyerMessage: '' });
  return { receiptId: fx.rid, orderNumber: fx.rid, createTs: at(fx.placed.day, fx.placed.h, fx.placed.m), updateTs: at(0, 12, 0), shipBy, buyer: { name: fx.buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: fx.lines.map(line) };
}
const V = (name, value) => ({ name, value });
const TITLE = 'Comet Necklace, Celestial Charm Necklace in Sterling Silver or 14k Gold Fill';
const comet = (listing, metal) => ({ listing, sku: 'COMET_9427', title: TITLE, variations: [V('Metal choice', metal === 'silver' ? 'Silver' : 'Gold'), V('Font Choice', 'Stylish')], personalization: ['Name One'], metalKey: metal, metalLabel: metal === 'silver' ? 'SS' : 'GF 14/20' });
const cat = { listing: '1890000103', sku: 'SCHRODINGER CAT 2', title: 'Cat Necklace, Whimsical Cat Charm, engraved on both sides', variations: [V('Metal choice', 'Gold'), V('Engraving Side', 'Both'), V('Font Choice', 'Ace')], personalization: ['front - W\nback - Name Two'] };
Base.orderOf = orderOf;
// the order of the pictures: one design, two metals (SS first, GF second), two lines of ONE order
R.ORDERS.two = { rid: '4290000131', buyer: 'Name Eleven', placed: { day: 0, h: 11, m: 30 }, ship: 5, lines: [comet('1890000131', 'silver'), comet('1890000131', 'gold')] }; R.EXPECT_JOBS.two = 2;
// the same, ON HOLD and never nested (no job until the page makes one, ENGPRE)
R.ORDERS.twoHeld = { rid: '4290000134', buyer: 'Name Twelve', placed: { day: 0, h: 11, m: 50 }, ship: 5, lines: [comet('1890000134', 'silver'), comet('1890000134', 'gold')], held: true }; R.EXPECT_JOBS.twoHeld = 0;
// three pieces: the silver and the gold necklace of one design, and a front + back cat necklace
R.ORDERS.three = { rid: '4290000132', buyer: 'Name Thirteen', placed: { day: 0, h: 11, m: 40 }, ship: 5, lines: [comet('1890000132', 'silver'), comet('1890000132', 'gold'), cat] }; R.EXPECT_JOBS.three = 1 + 1 + 2;
// ONE line of three discs (a counted line has no list of pieces to pick from, only its drawer): a press on a disc narrows the cards to that disc
R.ORDERS.discsOne = { rid: '4290000135', buyer: 'Name Fifteen', placed: { day: 0, h: 11, m: 55 }, ship: 5, lines: [{ listing: '1890000135', sku: 'COMET_9427', title: 'Initial Disc Necklace, Personalized Letter Discs', variations: [V('Metal choice', 'Gold'), V('Number of Discs', '3 Discs')], personalization: ['Tag 1: A, Tag 2: B, Tag 3: C'] }] }; R.EXPECT_JOBS.discsOne = 3;
// one line: nothing to tell apart, the card is what it always was
R.ORDERS.one = { rid: '4290000133', buyer: 'Name Fourteen', placed: { day: 0, h: 11, m: 45 }, ship: 5, lines: [comet('1890000133', 'gold')] }; R.EXPECT_JOBS.one = 1;

const fails = [], note = (ok, what, more) => { console.log((ok ? '  PASS ' : '  FAIL ') + what + (more ? '  [' + more + ']' : '')); if (!ok) fails.push(what); };
const SILVER = 'rgb(141, 149, 160)', GOLD = 'rgb(200, 162, 78)';

/** The section as the person sees it: each card's line, title, tail (the metal word after the name, and whether it is inside the card), dot colour, Approve; the count of customer-words blocks under the cards (NOSAID, Paul 17 Oct 2026: none); the header of Its pieces. */
const SECTION = () => {
  const host = document.getElementById('owEng'), win = document.getElementById('orderWin');
  const cs = [...(host ? host.children : [])].filter(n => n.classList.contains('engCard'));
  return {
    hostHidden: !host || host.hidden,
    cards: cs.map(c => {
      const d = c.querySelector('.engDot'), nm = c.querySelector('.engName'), t = c.querySelector('.engTail'), cr = c.getBoundingClientRect(), tr = t && t.getBoundingClientRect();
      return { line: c.dataset.key || '', piece: c.dataset.piece || '', metal: c.dataset.metal || '', title: ((nm || {}).textContent || '').trim(), tail: t ? t.textContent.trim() : '', tailIn: !!t && tr.width > 0 && tr.right <= cr.right + 0.5 && tr.left >= cr.left,
        dot: d ? getComputedStyle(d).backgroundColor : '', dotMetal: d ? d.dataset.metal || '' : '', dotLabel: d ? d.getAttribute('aria-label') || '' : '', state: c.dataset.state || '', approve: !!c.querySelector('[data-e=approve]:not([disabled])'), faces: [...c.querySelectorAll('.engFace')].map(f => f.dataset.face).join('+') };
    }),
    saidBlocks: win ? win.querySelectorAll('#owEngSaid, .engSaid, .esRow, .esName, [data-said]').length : -1,
    notes: (document.getElementById('owNotes') || {}).textContent || '',   // (the Overview's own "What the customer wrote" box: where the words are now)
    showing: ((document.querySelector('#owPcSum .owPcState') || {}).textContent || '').replace(/\s+/g, ' ').trim(),
    selected: window.OrderWin.selectedPiece(),
    rows: [...document.querySelectorAll('#owPcSum .owPcRow[data-piece]')].map(r => ({ key: r.dataset.piece, text: r.querySelector('.owPcName') ? r.querySelector('.owPcName').textContent.replace(/\s+/g, ' ').trim() : '', dot: getComputedStyle(r.querySelector('.dot')).backgroundColor }))
  };
};
const sameTick = (page, actSrc, arg) => page.evaluate(({ actSrc, arg, secSrc }) => { const act = eval('(' + actSrc + ')'), sec = eval('(' + secSrc + ')'); act(arg); return sec(); }, { actSrc: String(actSrc), arg, secSrc: String(SECTION) });
const settled = async (page, ms = 700) => { await R.settle(page, ms); return page.evaluate(`(${SECTION})()`); };
const openOn = async (page, key, opts) => {
  await page.evaluate(async ([k, o]) => { try { OrderWin.close(); } catch (_) {} await new Promise(r => setTimeout(r, 250)); CN.setMode('orders'); await OrderWin.open(k, o || undefined); }, [key, opts || null]);
  await page.waitForFunction(() => { const h = document.getElementById('owEng'); return h && !h.hidden && h.querySelector('.engCard'); }, null, { timeout: 25000 }).catch(() => {});
  return settled(page, 700);
};

(async () => {
  const w = await R.world({ viewport: { width: 1356, height: 900 } });
  if (!w) { console.log('no playwright-core: nothing run'); return; }
  const page = w.page, shots = process.env.SHOTS;
  try {
    const made = await R.build(w, ['two', 'three', 'one', 'twoHeld', 'discsOne']);
    const rows = await page.evaluate(() => { const o = {}; for (const r of Orders.rows()) (o[r.order.receiptId] ||= []).push({ key: r.key, metal: r.material, jobs: Engrave.jobsOf(r).map(j => j.key + ':' + j.state) }); return o; });
    if (process.env.PK_KEEP) console.log(JSON.stringify({ rows, made: Object.fromEntries(Object.entries(made).map(([k, v]) => [k, v.jobs.length])) }, null, 1));
    const two = rows[R.ORDERS.two.rid], three = rows[R.ORDERS.three.rid], one = rows[R.ORDERS.one.rid], held = rows[R.ORDERS.twoHeld.rid];
    note(two.length === 2 && two[0].metal === 'silver' && two[1].metal === 'gold', 'the order of the pictures: ONE order, two lines, one design, the silver piece first and the gold piece second', two.map(r => r.metal).join(' '));
    note(two.every(r => r.jobs.length === 1 && /:review$/.test(r.jobs[0])), 'each line has its own engraving job, waiting for approval', two.map(r => r.jobs.join(',').slice(-14)).join(' | '));
    const [kSS, kGF] = two.map(r => r.key);

    // ── the order of the pictures, every piece shown ──
    let s = await openOn(page, kGF);
    note(s.rows.length === 2 && s.rows.every(r => /COMET 9427 · (SS|GF)/.test(r.text)), 'Its pieces lists the two lines by name and metal', s.rows.map(r => r.text).join(' | '));
    note(s.selected === null && s.cards.length === 2 && new Set(s.cards.map(c => c.line)).size === 2, 'nothing picked: two cards, one for each line', s.cards.length + ' cards');
    const cardSS = s.cards.find(c => c.line === kSS) || {}, cardGF = s.cards.find(c => c.line === kGF) || {};
    note(cardSS.title === 'COMET 9427 · SS' && cardGF.title === 'COMET 9427 · GF', 'the two cards tell themselves apart at a glance: "COMET 9427 · SS" and "COMET 9427 · GF"', cardSS.title + ' | ' + cardGF.title);
    note(cardSS.dot === SILVER && cardGF.dot === GOLD && cardSS.dotMetal === 'silver' && cardGF.dotMetal === 'gold', 'the cards\' dots are their metals\' (the Its pieces list\'s colours), not the state\'s amber', cardSS.dot + ' | ' + cardGF.dot);
    note(cardSS.dotLabel === 'To approve' && cardGF.dotLabel === 'To approve' && cardSS.state === 'approve', 'the state is still said by the dot\'s label ("To approve"), the Approve button and the card\'s state', cardSS.dotLabel + ' / ' + cardSS.state);
    note(s.rows.find(r => r.key === kSS).dot === cardSS.dot && s.rows.find(r => r.key === kGF).dot === cardGF.dot, 'the card\'s dot is the very colour of its row on the Its pieces list');
    note(cardSS.approve && cardGF.approve, 'each card has its own Approve');
    note(s.saidBlocks === 0, 'NOSAID: no customer-words block under the two cards (it used to be headed by their metals; the cards themselves tell the pieces apart: checked above)', 'blocks ' + s.saidBlocks);
    if (shots) await R.shot(w, 'pk-' + (process.env.PK_TAG || 'after') + '-1-show-all');

    // ── pick the silver piece, then the gold piece: each card alone, in the same tick ──
    let r = await sameTick(page, k => OrderWin.selectPiece(k), kSS);
    note(r.selected === kSS && r.cards.length === 1 && r.cards[0].line === kSS, 'the silver row picked: its card alone, in the same tick', r.cards.map(c => c.title).join(','));
    note(r.cards[0] && r.cards[0].title === 'COMET 9427 · SS' && r.cards[0].dot === SILVER, 'the card says SS and wears the silver dot (not the gold piece\'s card)', r.cards[0] && (r.cards[0].title + ' ' + r.cards[0].dot));
    note(/^Showing COMET 9427 · SS/.test(r.showing), 'the header says which piece is shown: "Showing COMET 9427 · SS"', r.showing);
    note(r.saidBlocks === 0, 'no customer-words block under it (NOSAID)', 'blocks ' + r.saidBlocks);
    s = await settled(page, 500);
    note(/Name One/.test(s.notes), 'the Overview\'s own "What the customer wrote" box holds this piece\'s words', s.notes.slice(0, 80));
    if (shots) { await R.settle(page, 500); await R.shot(w, 'pk-' + (process.env.PK_TAG || 'after') + '-2-silver-picked'); }
    r = await sameTick(page, k => OrderWin.selectPiece(k), kGF);
    note(r.selected === kGF && r.cards.length === 1 && r.cards[0].line === kGF && r.cards[0].title === 'COMET 9427 · GF' && r.cards[0].dot === GOLD, 'the gold row picked: the gold card alone, GF, the gold dot', r.cards.map(c => c.title + ' ' + c.dot).join(','));
    note(/^Showing COMET 9427 · GF/.test(r.showing), 'the header says "Showing COMET 9427 · GF"', r.showing);
    if (shots) { await R.settle(page, 500); await R.shot(w, 'pk-' + (process.env.PK_TAG || 'after') + '-3-gold-picked'); }
    // ── every frame after a pick shows exactly the picked piece (no old card, no gap) ──
    const frames = (act, arg) => page.evaluate(async ({ actSrc, arg }) => {
      const host = document.getElementById('owEng'), look = () => (host.hidden ? 'none' : [...host.children].map(c => (c.dataset.key || '?').slice(-1) + ':' + (c.querySelector('.engTail') || {}).textContent).join(',') || 'empty');
      const out = []; let on = true; const raf = () => { if (!on) return; out.push(look()); requestAnimationFrame(raf); };
      eval('(' + actSrc + ')')(arg); out.push(look()); requestAnimationFrame(raf);
      await new Promise(r2 => setTimeout(r2, 600)); on = false; return out;
    }, { actSrc: String(act), arg });
    const uniq = a => [...new Set(a)];
    let fr = await frames(k => OrderWin.selectPiece(k), kSS);
    note(uniq(fr).length === 1 && /^1: · SS$|^1:\s*· SS$/.test(uniq(fr)[0].replace(/\s+/g, ' ')), 'gold -> silver: every frame shows the silver card only', uniq(fr).join(' | '));
    fr = await frames(k => OrderWin.selectPiece(k), kGF);
    note(uniq(fr).length === 1 && /^2:\s*· GF$/.test(uniq(fr)[0]), 'silver -> gold: every frame shows the gold card only', uniq(fr).join(' | '));
    fr = await frames(() => OrderWin.selectPiece(null));
    note(uniq(fr).length === 1 && uniq(fr)[0].split(',').length === 2, 'gold -> Show all: every frame shows both cards', uniq(fr).join(' | '));
    // ── the opener of the window: opened on the silver line, "Its pieces" picks it (a piece dot's press) ──
    s = await openOn(page, kSS, { pick: true });
    note(s.selected === kSS && s.cards.length === 1 && s.cards[0].line === kSS && s.cards[0].title === 'COMET 9427 · SS', 'the window opened on the silver piece: its card from the first draw', s.cards.map(c => c.title).join(','));

    // ── the metal never gives way: a narrow window cuts the NAME, not the metal ──
    await page.setViewportSize({ width: 760, height: 900 }); await R.settle(page, 700);
    s = await settled(page, 500);
    await page.evaluate(() => OrderWin.selectPiece(null)); s = await settled(page, 500);
    note(s.cards.length === 2 && s.cards.every(c => c.tail.length > 0 && c.tailIn), 'in a 760 px window both cards still show their metal word inside the card', s.cards.map(c => c.tail + (c.tailIn ? '' : '!')).join(' '));
    await page.setViewportSize({ width: 1356, height: 900 }); await R.settle(page, 500);

    // ── Approve: the silver card approves the silver piece and nothing else ──
    await page.evaluate(() => { window.CNEngravingSeals.who = () => 'Test Operator'; });
    s = await openOn(page, kGF);
    await page.evaluate(k => document.querySelector('#owEng .engCard[data-key="' + k + '"] [data-e=approve]').click(), kSS);
    await page.waitForFunction(k => { const j = Engrave.jobsOf(B.orders.byKey.get(k))[0]; return j && ['approved', 'written'].includes(j.state); }, kSS, { timeout: 90000 }).catch(() => {});
    await R.settle(page, 900);
    const after = await page.evaluate(([a, b]) => ({ ss: Engrave.jobsOf(B.orders.byKey.get(a)).map(j => j.state), gf: Engrave.jobsOf(B.orders.byKey.get(b)).map(j => j.state) }), [kSS, kGF]);
    note(after.ss.every(x => ['approved', 'written'].includes(x)) && after.gf.every(x => x === 'review'), 'Approve on the silver card approves the silver piece only: the gold piece still waits', 'ss ' + after.ss + ' gf ' + after.gf);
    s = await settled(page, 500);
    const aSS = s.cards.find(c => c.line === kSS) || {}, aGF = s.cards.find(c => c.line === kGF) || {};
    note(!aSS.approve && aGF.approve && s.cards.length === 2, 'the silver card has its approval and no button; the gold card still has its Approve', 'ss ' + aSS.state + ' gf ' + aGF.state);
    await page.evaluate(k => OrderWin.selectPiece(k), kGF); await R.settle(page, 500);
    await page.evaluate(k => document.querySelector('#owEng .engCard[data-key="' + k + '"] [data-e=approve]').click(), kGF);
    await page.waitForFunction(k => { const j = Engrave.jobsOf(B.orders.byKey.get(k))[0]; return j && ['approved', 'written'].includes(j.state); }, kGF, { timeout: 90000 }).catch(() => {});
    const fin = await page.evaluate(([a, b]) => ({ ss: Engrave.jobsOf(B.orders.byKey.get(a)).map(j => j.state), gf: Engrave.jobsOf(B.orders.byKey.get(b)).map(j => j.state) }), [kSS, kGF]);
    note(fin.ss.every(x => ['approved', 'written'].includes(x)) && fin.gf.every(x => ['approved', 'written'].includes(x)), 'then the gold card\'s own Approve approves the gold piece', 'ss ' + fin.ss + ' gf ' + fin.gf);

    // ── the same order on hold, never nested (no job yet): the cards follow the pick the same way ──
    const [hSS, hGF] = held.map(x => x.key);
    s = await openOn(page, hGF);
    await page.waitForFunction(() => document.querySelectorAll('#owEng .engCard').length === 2, null, { timeout: 90000 }).catch(() => {});
    s = await settled(page, 500);
    note(held.length === 2 && held.every(x => x.jobs.length === 0), 'the held order had no job before its window was opened', held.map(x => x.jobs.length).join(','));
    note(s.cards.length === 2 && s.cards.map(c => c.title).sort().join('|') === 'COMET 9427 · GF|COMET 9427 · SS', 'on hold, never nested: both cards are there and named by their metals', s.cards.map(c => c.title).join(' | '));
    r = await sameTick(page, k => OrderWin.selectPiece(k), hSS);
    note(r.cards.length === 1 && r.cards[0].line === hSS && r.cards[0].title === 'COMET 9427 · SS', 'on hold: the silver row shows the silver card alone', r.cards.map(c => c.title).join(','));
    r = await sameTick(page, k => OrderWin.selectPiece(k), hGF);
    note(r.cards.length === 1 && r.cards[0].line === hGF && r.cards[0].title === 'COMET 9427 · GF', 'on hold: the gold row shows the gold card alone', r.cards.map(c => c.title).join(','));

    // ── three pieces: two necklaces of one design and a front + back necklace ──
    const [t1, t2, t3] = three.map(x => x.key);
    s = await openOn(page, t3);
    note(s.cards.length === 3 && new Set(s.cards.map(c => c.line)).size === 3, 'three pieces: three cards', s.cards.map(c => c.title).join(' | '));
    const c3 = s.cards.find(c => c.line === t3) || {};
    note(/GF$/.test(c3.title) && /front\+back/.test(c3.faces) && c3.dot === GOLD, 'the front + back piece\'s card says GF and has both faces', c3.title + ' ' + c3.faces);
    for (const [k, metal, n] of [[t1, 'SS', 1], [t2, 'GF', 1], [t3, 'GF', 1]]) {
      r = await sameTick(page, kk => OrderWin.selectPiece(kk), k);
      note(r.cards.length === n && r.cards[0].line === k && r.cards[0].tail === '· ' + metal, 'three pieces: picking ' + metal + ' line ...' + k.slice(-1) + ' shows its card alone', r.cards.map(c => c.title).join(','));
    }

    // ── a line of three discs on its own: a press on one disc narrows the cards to that disc ──
    const dRow = rows[R.ORDERS.discsOne.rid][0].key;
    s = await openOn(page, dRow);
    note(s.cards.length === 3 && s.cards.every(c => c.line === dRow) && s.cards.every(c => c.tail === ''), 'a line of three discs alone: three disc cards, no metal word (one line: nothing to tell apart)', s.cards.map(c => c.title).join(' | '));
    r = await sameTick(page, () => document.querySelector('#owPcSum .owPcSub[data-sub$="#D2"]').click());
    note(r.cards.length === 1 && /#D2$/.test(r.cards[0].piece) && await page.evaluate(() => OrderWin.selectedDisc()) === 'D2', 'a press on disc 2 narrows the cards to Disc 2 alone, in the same tick', r.cards.map(c => c.title).join(','));
    r = await sameTick(page, () => document.querySelector('#owPcSum .owPcSub[data-sub$="#D2"]').click());
    note(r.cards.length === 3 && await page.evaluate(() => OrderWin.selectedDisc()) === null, 'the same disc pressed again shows the three disc cards');

    // ── one line: the card is exactly what it was (no metal word, the state dot) ──
    s = await openOn(page, one[0].key);
    note(s.cards.length === 1 && s.cards[0].title === 'COMET 9427' && s.cards[0].tail === '' && s.cards[0].metal === '' && s.cards[0].dotMetal === '' && s.cards[0].dotLabel === 'To approve', 'a single-line order: the card is unchanged (name only, the state dot "To approve")', s.cards[0] && (s.cards[0].title + ' / ' + s.cards[0].dotLabel));

    note(w.errors.length === 0, 'no page errors', w.errors.slice(0, 2).join(' | '));
    const warns = w.warns.filter(x => !/sheet metadata This sheet is no longer in the Library/.test(x));
    note(warns.length === 0, 'no swallowed order-window errors', warns.slice(0, 2).join(' | '));
  } catch (e) { console.log('  FAIL test crashed: ' + (e && e.stack || e)); fails.push('crash'); }
  finally { await w.close(); }
  console.log(fails.length ? `\n${fails.length} FAILED:\n - ${fails.join('\n - ')}` : '\nAll checks passed');
  process.exit(fails.length ? 1 : 0);
})();
