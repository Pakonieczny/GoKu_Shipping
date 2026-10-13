// ENGMSG (engrave-ui-1012 round 2b, Paul point 1): "Also in the detailed order modal I don't see the customers message for the back engraving."
// The order window's Engraving section shows, directly under its cards, ONE quiet block of the customer's own words for the engraving (charm-nest-order-engraving.js, .engSaid):
// the personalisation box, the buyer's message, the staff note and, only when the piece has BOTH faces, which words are on which face. ENGOPT (Paul 13 Oct 2026: "Do not show the Options text in this view"): never an "Options" line here (the Engrave tab's box keeps it).
// This test builds the REAL page over the REAL-drawing world (tests/charm-nest/_engraveRealWorld.cjs: real .ai drawings, real intake / pool / classifier / fit, offline fake site, no Etsy, no paid AI, nothing real written)
// and opens the order window of: back only (words in the personalisation box), back only (words only in the buyer's message), front only, front + back labelled ("front - W / back - Name One").
// Then, on the same page and the same real cards: a staff note, HTML in a note (text only), a live change inside the 1 s tick, and a customer who wrote and chose nothing (the block is gone, the cards stay).
// It also measures at 1280x720: the block sits OUTSIDE the cards' grid (the grid's children are only cards), under it, and the first row of cards does not move when the block is there (the no-scroll test's rule).
// Names are made up ("Name One"). Run:  PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/engrave-order-customer-words.cjs     SHOTS=<dir> keeps MSG-*.png
// What a pass does NOT prove: a live order (the fake site has the real drawings and fonts but not the live data), another window size than the one run, a very long message in a narrow window (wraps; not scrolled).
'use strict';
const path = require('path');
const R = require('./_engraveRealWorld.cjs');
let pass = 0, fail = 0;
const ok = (c, m, extra) => { if (c) { pass++; console.log('  PASS ' + m); } else { fail++; console.log('  FAIL ' + m + (extra !== undefined ? ' :: ' + (typeof extra === 'string' ? extra : JSON.stringify(extra)) : '')); } };

// one more order for this test: back-only whose words are only in the buyer's message (the world's other orders carry them in the personalisation box)
Object.assign(R.ORDERS, {
  backMsg: Object.assign({}, R.ORDERS.back, { rid: '4290000111', listing: '1890000111', buyer: 'Name Seven', personalization: [], buyerMessage: 'Hello, could you please engrave Name One on the back of the charm. Thank you!' }),
});
const NAMES = ['back', 'backMsg', 'front', 'both'];
const SHOTS = process.env.SHOTS || '/mnt/project-files/plans/engrave-ui-1012/round2/images';

/** what the person sees of the block, and where it is (runs in the page) */
function probe() {
  const vis = e => { if (!e || !e.isConnected) return false; const r = e.getBoundingClientRect(); if (r.width < 1 || r.height < 1) return false; for (let n = e; n && n.nodeType === 1; n = n.parentElement) { const cs = getComputedStyle(n); if (cs.display === 'none' || cs.visibility === 'hidden' || n.hidden) return false; } return true; };
  const rc = e => { if (!e) return null; const b = e.getBoundingClientRect(); return { l: +b.left.toFixed(1), t: +b.top.toFixed(1), r: +b.right.toFixed(1), b: +b.bottom.toFixed(1), w: +b.width.toFixed(1), h: +b.height.toFixed(1) }; };
  const grid = document.getElementById('owEng'), said = document.getElementById('owEngSaid'), sec = document.getElementById('owEngSec');
  const rowsOf = k => said ? [...said.querySelectorAll('.esRow')].filter(r => r.dataset.said === k) : [];
  const texts = k => rowsOf(k).map(r => [...r.childNodes].map(n => n.textContent.trim()).filter(Boolean).join(' '));
  const scroller = document.querySelector('#orderWin .owVInfo .owMain');
  const dlg = document.getElementById('orderWin');
  return {
    shown: vis(said), exists: !!said, text: said ? said.innerText.replace(/\s+/g, ' ').trim() : '',
    personalisation: texts('Personalisation'), buyer: texts('Buyer message'), staff: texts('Staff note'), options: texts('Options'), faces: texts('faces'),
    nameRows: said ? [...said.querySelectorAll('.esName')].map(n => n.textContent) : [],
    html: said ? said.innerHTML : '',
    inSection: !!(said && sec && sec.contains(said)), afterGrid: !!(said && grid && (grid.compareDocumentPosition(said) & Node.DOCUMENT_POSITION_FOLLOWING)), siblingOfGrid: !!(said && grid && said.parentNode === grid.parentNode),
    gridKidsAreCards: grid ? [...grid.children].every(c => c.classList.contains('engCard')) : null, cards: grid ? grid.querySelectorAll(':scope > .engCard').length : 0, gridHidden: grid ? grid.hidden : null,
    grid: rc(grid), box: rc(said), scroller: rc(scroller), dialog: rc(dlg), vh: innerHeight,
    cardTops: grid ? [...grid.querySelectorAll(':scope > .engCard')].map(c => Math.round(c.getBoundingClientRect().top)) : [],
    cardTexts: grid ? grid.innerText.replace(/\s+/g, ' ') : ''
  };
}
const settle = (w, ms) => R.settle(w.page, ms || 500);

(async () => {
  const w = await R.world({ viewport: { width: 1280, height: 720 }, shots: SHOTS });
  if (!w) { console.log('SKIP: no playwright-core'); return; }
  try {
    // the fake site's reader is a stub (free): the plain back of the buyer-message order is read from the message, the others from the personalisation box
    w.srv.st.agentResults.engraveIntent = o => {
      const txt = String(o.messages[0].content[0].text), m = /Personalisation field: (\[.*?\])/.exec(txt); let p = []; try { p = JSON.parse(m ? m[1] : '[]'); } catch (_) {}
      const req = { side: 'back', font: null, handwriting: false, image: false };
      if (p.length) return { engrave: true, text: p.join('\n'), source: 'personalization', sourceQuote: p[0], requests: req, questions: [], confidence: 0.97 };
      if (/engrave Name One/.test(txt)) return { engrave: true, text: 'Name One', source: 'buyerMessage', sourceQuote: 'engrave Name One on the back', requests: req, questions: [], confidence: 0.9 };
      return { engrave: false, text: '', source: 'none', sourceQuote: '', requests: req, questions: [], confidence: 0.9 };
    };
    const made = await R.build(w, NAMES.concat(['earrings']));
    const open = async n => { await R.openOrderWindow(w, n); await settle(w, 400); return w.page.evaluate(probe); };
    const SAID = await w.page.evaluate(() => !!window.CharmNestEngraveSaid);   // (ENGNOTE's reader of the words and the engraving options: on the page or not yet)
    console.log('  info: charm-nest-engrave-said.js on the page: ' + SAID);
    for (const n of NAMES) ok(made[n] && made[n].jobs.length > 0, `real world: order "${n}" has an Engrave job (real drawing, real fit)`, made[n] && made[n].jobs);

    // ── 1. back only, words in the personalisation box ──
    console.log('back only (personalisation box)');
    let p = await open('back');
    ok(p.cards >= 1 && !p.gridHidden, 'the Engraving section shows its card');
    ok(p.shown && p.inSection && p.siblingOfGrid && p.afterGrid, 'the customer-words block is shown, inside the section, under the cards (a sibling after the grid)', { shown: p.shown, inSection: p.inSection, sibling: p.siblingOfGrid });
    ok(p.gridKidsAreCards, 'the grid host holds only cards (the contract: every direct child of #owEng is one piece)');
    ok(p.personalisation.length === 1 && /Name One/.test(p.personalisation[0]) && /^Personalisation/.test(p.personalisation[0]), 'the personalisation words are there under their label', p.personalisation);
    ok(!p.faces.length, 'one face: no face marks (nothing to tell apart)', p.faces);
    ok(!/\d+\s*%|sure|Read from|Words on the back|Waiting for your approval/i.test(p.text), 'no percentage, no "sure", no explanatory sentence', p.text);
    ok(!p.buyer.length && !p.staff.length, 'nothing the customer did not write (no buyer message, no staff note)');
    ok(!p.options.length && !/Options|Font Choice|Stylish/i.test(p.text) && !/data-said="Options"/.test(p.html), 'no "Options" line in this view: not drawn, nothing hidden in the DOM (the customer did choose a font)', { options: p.options, text: p.text });
    if (SAID) { const o = await w.page.evaluate(() => { const r = Orders.rows().find(x => document.getElementById('owTitle').textContent.includes(x.order.receiptId)); return CharmNestEngraveSaid.rowsOf(r).filter(x => x[0] === 'Options').map(x => x[1]); }); ok(o.length === 1 && /Font Choice: Stylish/.test(o[0]), 'the Engrave tab\'s reader still has the Options row (only this view drops it)', o); }
    await R.shot(w, 'MSG-back-personalisation');

    // ── 2. back only, the words only in the buyer's message ──
    console.log('back only (buyer message)');
    p = await open('backMsg');
    ok(p.shown && p.cards >= 1, 'block shown for an order whose words are only in the buyer message');
    ok(p.buyer.length === 1 && /could you please engrave Name One on the back of the charm\. Thank you!/.test(p.buyer[0]) && /^Buyer message/.test(p.buyer[0]), 'the buyer message is there whole, as written', p.buyer);
    ok(!p.personalisation.length, 'no personalisation row when the customer left the box empty', p.personalisation);
    ok(!p.faces.length, 'one face: no face marks');
    await R.shot(w, 'MSG-back-buyer-message');

    // ── 3. front only ──
    console.log('front only');
    p = await open('front');
    ok(p.shown && p.cards >= 1, 'block shown for a front-only order');
    ok(p.personalisation.length === 1 && /Name One/.test(p.personalisation[0]), 'the personalisation words are there', p.personalisation);
    ok(!p.faces.length, 'front only: no face marks');
    ok(!p.options.length && !/Options|Engraving Side|Font Choice|Vibur/i.test(p.text), 'the customer chose Front and the font: not said here (no Options line)', { options: p.options, text: p.text });
    await R.shot(w, 'MSG-front-only');

    // ── 4. front + back, labelled ──
    console.log('front + back, labelled');
    p = await open('both');
    ok(p.shown && p.cards >= 1, 'block shown for a front + back order');
    ok(p.personalisation.length === 1 && /front - W \/ back - Name One/i.test(p.personalisation[0]), 'the labelled message is there as the customer wrote it (lines joined by " / ")', p.personalisation);
    ok(p.faces.length === 1 && /^Front W · Back NAME( \/ | )?ONE$/i.test(p.faces[0]), 'which words belong to which face: "Front W · Back Name One" (as the card lays the lines)', p.faces);
    ok((p.html.match(/<i>Front<\/i>/g) || []).length === 1 && (p.html.match(/<i>Back<\/i>/g) || []).length === 1, 'each face named once, by the face name only');
    ok(!p.options.length && !/Options|Engraving Side|Font Choice|Ace/i.test(p.text), 'the engraving choices (both sides, the font) are not said here (no Options line)', { options: p.options, text: p.text });
    ok(!p.nameRows.length, 'one line in the order: no piece name heading');
    const both = p;
    await R.shot(w, 'MSG-front-and-back');

    // ── 5. geometry at 1280x720: the block takes nothing from the first row of cards ──
    console.log('geometry, 1280x720');
    const geo = await w.page.evaluate(() => {
      const grid = document.getElementById('owEng'), said = document.getElementById('owEngSaid'), sc = document.querySelector('#orderWin .owVInfo .owMain');
      sc.scrollTop = 0; const r = e => { const b = e.getBoundingClientRect(); return [Math.round(b.left), Math.round(b.top), Math.round(b.width), Math.round(b.height)]; };
      const withIt = r(grid), cards = [...grid.querySelectorAll(':scope > .engCard')].map(r);
      said.hidden = true; const without = r(grid); said.hidden = false;
      const sb = sc.getBoundingClientRect(), box = r(said);
      return { withIt, without, cards, box, scrollerBottom: Math.round(sb.bottom), vh: innerHeight, boxBottom: box[1] + box[3] };
    });
    ok(JSON.stringify(geo.withIt) === JSON.stringify(geo.without), 'the cards\' grid does not move or change size with the block there', geo);
    ok(geo.box[1] >= geo.withIt[1] + geo.withIt[3] - 1, 'the block starts under the grid', geo.box);
    ok(geo.cards.length >= 1 && geo.withIt[1] + geo.withIt[3] <= geo.scrollerBottom - 8, 'the first row of cards is on screen without scrolling (the no-scroll rule)', geo);
    console.log(`  info: front+back order: block ${geo.box[3]}px tall, its bottom ${geo.boxBottom}px, the scroll area ends at ${geo.scrollerBottom}px (window ${geo.vh}px)`);
    ok(geo.box[3] <= 80, 'the block is at most four short lines tall for a front + back order (<= 80px)', geo.box);

    // ── 6. a staff note, HTML as text, a live change ──
    console.log('staff note, text only, live change');
    await open('back');
    await w.page.evaluate(() => { const r = Orders.rows().find(x => document.getElementById('owTitle').textContent.includes(x.order.receiptId)); r.spec.staffNote = 'Engrave <b>bold</b> & "quotes" please'; });
    await w.page.waitForFunction(() => { const s = document.getElementById('owEngSaid'); return s && /Staff note/.test(s.textContent); }, null, { timeout: 5000 }).catch(() => {});
    p = await w.page.evaluate(probe);
    ok(p.staff.length === 1 && /Engrave <b>bold<\/b> & "quotes" please/.test(p.staff[0]) && !/<b>bold/.test(p.html), 'a staff note appears within the page\'s own 1 s tick, as plain text (markup is not markup)', { staff: p.staff });
    await w.page.evaluate(() => { const r = Orders.rows().find(x => document.getElementById('owTitle').textContent.includes(x.order.receiptId)); r.spec.staffNote = ''; });
    await w.page.waitForFunction(() => { const s = document.getElementById('owEngSaid'); return s && !/Staff note/.test(s.textContent); }, null, { timeout: 5000 }).catch(() => {});
    p = await w.page.evaluate(probe);
    ok(!p.staff.length, 'and goes again when the note goes');

    // ── 6b. the customer's only words were options (a font, a side): no block at all, not an empty box; the Engrave tab's reader still has the Options row ──
    console.log('only options');
    await w.page.evaluate(() => { const r = Orders.rows().find(x => document.getElementById('owTitle').textContent.includes(x.order.receiptId)); r.spec.personalization = []; r.spec.buyerMessage = ''; r.spec.staffNote = ''; r.line.personalization = []; });
    await w.page.waitForFunction(() => { const s = document.getElementById('owEngSaid'); return !s || s.hidden; }, null, { timeout: 5000 }).catch(() => {});
    p = await w.page.evaluate(probe);
    const optsLeft = SAID ? await w.page.evaluate(() => { const r = Orders.rows().find(x => document.getElementById('owTitle').textContent.includes(x.order.receiptId)); return CharmNestEngraveSaid.rowsOf(r).map(x => x[0]); }) : null;
    if (SAID) ok(optsLeft.join('|') === 'Options', 'the scenario is real: the only thing the reader still has for this line is its Options row', optsLeft);
    ok(!p.shown && p.text === '' && !p.html.includes('data-said="Options"'), 'a line whose only words were options: no block, no empty box, no label', { shown: p.shown, text: p.text, html: p.html });
    ok(p.cards >= 1 && !p.gridHidden, 'the cards are still there');

    // ── 7. the customer wrote and chose nothing: nothing is shown, the cards stay ──
    console.log('nothing written');
    await w.page.evaluate(() => { const r = Orders.rows().find(x => document.getElementById('owTitle').textContent.includes(x.order.receiptId)); r.spec.personalization = []; r.spec.buyerMessage = ''; r.spec.staffNote = ''; r.spec.options = []; r.line.personalization = []; r.line.variations = []; });
    await w.page.waitForFunction(() => { const s = document.getElementById('owEngSaid'); return !s || s.hidden; }, null, { timeout: 5000 }).catch(() => {});
    p = await w.page.evaluate(probe);
    ok(!p.shown && p.text === '', 'no block, no empty box, no label when the customer wrote and chose nothing', { shown: p.shown, text: p.text });
    ok(p.cards >= 1 && !p.gridHidden, 'the cards are still there');
    await R.shot(w, 'MSG-nothing-written');

    // ── 8. an order with no engraving at all (an earring pair: never engraved): no section, no block ──
    console.log('no engraving');
    p = await open('earrings');
    ok(p.gridHidden && !p.shown && p.cards === 0, 'an earring pair (never engraved) has no cards and no customer-words block', { gridHidden: p.gridHidden, shown: p.shown, cards: p.cards });

    // ── 9. clean ──
    ok(w.errors.length === 0, 'no page error', w.errors);
    ok(w.warns.length === 0, 'no "order view:" warning (OrderWin swallows errors with it)', w.warns);
  } finally { await w.close(); }
  console.log(`\n${fail ? 'FAILED' : 'PASSED'}: ${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
