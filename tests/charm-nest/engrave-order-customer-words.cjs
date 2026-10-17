// ENGMSG (engrave-ui-1012 round 2b, Paul point 1) -> ENGTEXT (13 Oct 2026) -> NOSAID (Paul, 17 Oct 2026): "Remove the personalization info from beneath the Engraving card inside the Detailed Order modal."
// History: the order window's Engraving section used to show, directly under its cards, ONE quiet block of the customer's own words (charm-nest-order-engraving.js, #owEngSaid / .engSaid: the personalisation box, the buyer's message,
// the staff note and, only for a front + back piece, which words are on which face). NOSAID took that block away: nothing is drawn under the cards any more, and a block an earlier drawing left there is removed. This test now holds
// the new contract (every check below is on the REAL page over the REAL-drawing world, tests/charm-nest/_engraveRealWorld.cjs: real .ai drawings, real intake / pool / classifier / fit, offline fake site, no Etsy, no paid AI, nothing real written):
//   1  the block is ABSENT: no #owEngSaid, no .engSaid, no .esRow / [data-said] in the order window, in the Engraving section or beside its cards; the grid holds only cards; none of the block's labels is in the section
//   2  the customer's words are still in the Overview's OWN box "What the customer wrote" (#owNotes, the bridge's paint(): the personalisation, the buyer's message, the gift message; never the engraving Options: ENGOPT, Paul 13 Oct 2026),
//      for back only (personalisation box), back only (buyer message only), front only, front + back ("front - W / back - Name One"), decoded ("There's", not "There&#39;s"; markup stays text), and gone when the customer wrote nothing
//   3  the reader, OrderEngraving.said.of / rows / html, still returns the words AS DATA for any host that wants them (decoded, no Options row, the Front / Back line for a piece with both faces); the Engrave tab's own reader is untouched
//   4  the cards are not moved by the missing block: the section ends where its cards end, the first row of cards and its pictures are on screen without scrolling at 1280x720 (the no-scroll test's rule), and a note or a change of the
//      words (a staff note, a buyer message changed while the window is open) never makes the block come back (a drawing on the page's own 1 s tick would show here)
//   5  an order with no engraving at all (an earring pair) has no section and no block
// Names are made up ("Name One"). Run:  PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/engrave-order-customer-words.cjs     SHOTS=<dir> keeps MSG-*.png
// What a pass does NOT prove: a live order (the fake site has the real drawings and fonts but not the live data), another window size than the one run. The staff note and the "Front / Back" words line were only in the removed block:
// they are not in the Overview's box (the team's messages are in the Team tab) and are said nowhere in the order window's Overview any more; the Engrave tab's box keeps the staff note and the Options row (engrave-customer-note.cjs).
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
const LABELS = /Personalisation|Buyer message|Staff note|Gift message|Options/i;   // (the labels of the removed block; the cards never print them)

/** what the person sees of the Engraving section and of the Overview's "What the customer wrote" box, and where things are (runs in the page) */
function probe() {
  const vis = e => { if (!e || !e.isConnected) return false; const r = e.getBoundingClientRect(); if (r.width < 1 || r.height < 1) return false; for (let n = e; n && n.nodeType === 1; n = n.parentElement) { const cs = getComputedStyle(n); if (cs.display === 'none' || cs.visibility === 'hidden' || n.hidden) return false; } return true; };
  const rc = e => { if (!e) return null; const b = e.getBoundingClientRect(); return { l: +b.left.toFixed(1), t: +b.top.toFixed(1), r: +b.right.toFixed(1), b: +b.bottom.toFixed(1), w: +b.width.toFixed(1), h: +b.height.toFixed(1) }; };
  const grid = document.getElementById('owEng'), sec = document.getElementById('owEngSec'), dlg = document.getElementById('orderWin'), notes = document.getElementById('owNotes');
  const scroller = document.querySelector('#orderWin .owVInfo .owMain');
  // the Overview's box: [[label, text]] (the bridge draws `<span class="lbl">Label</span>text` per part)
  const parts = notes ? [...notes.querySelectorAll('.lbl')].map(l => { let t = ''; for (let n = l.nextSibling; n && !(n.nodeType === 1 && n.classList.contains('lbl')); n = n.nextSibling) t += n.textContent; return [l.textContent.trim(), t.trim()]; }) : [];
  return {
    // (1) the block is absent: by id, by class, by its rows, anywhere in the order window; and nothing in the section is a sibling of the grid but the title and the calm lines
    byId: !!document.getElementById('owEngSaid'), inWindow: dlg ? dlg.querySelectorAll('#owEngSaid, .engSaid, .esRow, .esName, [data-said]').length : -1, inPage: document.querySelectorAll('#owEngSaid, .engSaid').length,
    secKids: sec ? [...sec.children].map(c => c.id || c.className || c.tagName) : [], gridNext: grid && grid.nextElementSibling ? (grid.nextElementSibling.id || grid.nextElementSibling.className) : '',
    secText: sec ? sec.innerText.replace(/\s+/g, ' ').trim() : '',
    gridKidsAreCards: grid ? [...grid.children].every(c => c.classList.contains('engCard')) : null, cards: grid ? grid.querySelectorAll(':scope > .engCard').length : 0, gridHidden: grid ? grid.hidden : null,
    // (2) the Overview's own box
    notesShown: vis(notes), notesNone: !!(notes && notes.classList.contains('none')), notesText: notes ? notes.innerText.replace(/\s+/g, ' ').trim() : '', notesParts: parts,
    // (4) geometry
    grid: rc(grid), sec: rc(sec), scroller: rc(scroller), dialog: rc(dlg), vh: innerHeight,
    cardTops: grid ? [...grid.querySelectorAll(':scope > .engCard')].map(c => Math.round(c.getBoundingClientRect().top)) : [],
    cardTexts: grid ? grid.innerText.replace(/\s+/g, ' ') : ''
  };
}
const settle = (w, ms) => R.settle(w.page, ms || 500);
/** the contract of every order window: no block, in any form, anywhere (the per-order checks call this) */
const noBlock = (tag, p) => ok(!p.byId && p.inWindow === 0 && p.inPage === 0 && p.gridNext === '' && !p.secKids.some(k => /Said|esRow/.test(k)), `${tag}: NOSAID: no customer-words block under the cards (no #owEngSaid, no .engSaid / .esRow / [data-said] in the order window; nothing after the grid in the section)`, { byId: p.byId, inWindow: p.inWindow, inPage: p.inPage, gridNext: p.gridNext, secKids: p.secKids });

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
    // the reader module as data (OrderEngraving.said: of / rows / html): the words of the order the window shows, the decoded text, and (with pieces handed in) the Front / Back line
    const reader = (pieces) => w.page.evaluate(pcs => { const r = Orders.rows().find(x => document.getElementById('owTitle').textContent.includes(x.order.receiptId)); const S = window.OrderEngraving && OrderEngraving.said; if (!S) return null;
      const rows = S.rows(r), entries = pcs ? S.of([r], pcs.map(p => Object.assign({ lineKey: r.key }, p))) : null; return { rows, entries, html: entries ? S.html(entries) : '' }; }, pieces || null);

    // ── 1. back only, words in the personalisation box ──
    console.log('back only (personalisation box)');
    let p = await open('back');
    ok(p.cards >= 1 && !p.gridHidden, 'the Engraving section shows its card');
    noBlock('back', p);
    ok(p.gridKidsAreCards, 'the grid host holds only cards (the contract: every direct child of #owEng is one piece)');
    ok(!LABELS.test(p.secText), 'none of the removed block\'s labels (Personalisation / Buyer message / Staff note / Gift message / Options) is in the Engraving section', p.secText);
    ok(p.notesShown && p.notesParts.length === 1 && p.notesParts[0][0] === 'Personalisation' && /Name One/.test(p.notesParts[0][1]), 'the Overview\'s own "What the customer wrote" box (#owNotes) still holds the personalisation words under their label', { parts: p.notesParts, shown: p.notesShown });
    ok(!p.notesParts.some(x => /Buyer message|Staff note/.test(x[0])), 'nothing the customer did not write (no buyer message, no staff note) in that box', p.notesParts);
    ok(!/Options|Font Choice|Stylish/i.test(p.notesText) && !/Font Choice|Stylish/i.test(p.secText), 'no "Options" line in this view (ENGOPT: the customer did choose a font; neither the box nor the section says it)', { notes: p.notesText, sec: p.secText });
    if (SAID) { const o = await w.page.evaluate(() => { const r = Orders.rows().find(x => document.getElementById('owTitle').textContent.includes(x.order.receiptId)); return CharmNestEngraveSaid.rowsOf(r).filter(x => x[0] === 'Options').map(x => x[1]); }); ok(o.length === 1 && /Font: Stylish/.test(o[0]), 'the Engrave tab\'s reader still has the Options row (only this view drops it)', o); }
    const rd = await reader();
    ok(!!rd && rd.rows.length === 1 && rd.rows[0][0] === 'Personalisation' && rd.rows[0][1] === 'Name One', 'the reader (OrderEngraving.said.rows) still returns the words as data: [["Personalisation", "Name One"]], no Options row', rd && rd.rows);
    await R.shot(w, 'MSG-back-personalisation');

    // ── 2. back only, the words only in the buyer's message ──
    console.log('back only (buyer message)');
    p = await open('backMsg');
    ok(p.cards >= 1, 'cards shown for an order whose words are only in the buyer message');
    noBlock('backMsg', p);
    ok(p.notesParts.length === 1 && p.notesParts[0][0] === 'Buyer message' && p.notesParts[0][1] === 'Hello, could you please engrave Name One on the back of the charm. Thank you!', 'the Overview\'s box has the buyer message whole, as written', p.notesParts);
    ok(!p.notesParts.some(x => x[0] === 'Personalisation'), 'no personalisation part when the customer left the box empty', p.notesParts);
    const rdm = await reader();
    ok(!!rdm && rdm.rows.length === 1 && rdm.rows[0][0] === 'Buyer message' && /could you please engrave Name One on the back of the charm\. Thank you!/.test(rdm.rows[0][1]), 'the reader returns the buyer message as data', rdm && rdm.rows);
    await R.shot(w, 'MSG-back-buyer-message');

    // ── 3. front only ──
    console.log('front only');
    p = await open('front');
    ok(p.cards >= 1, 'cards shown for a front-only order');
    noBlock('front', p);
    ok(p.notesParts.length === 1 && p.notesParts[0][0] === 'Personalisation' && /Name One/.test(p.notesParts[0][1]), 'the Overview\'s box has the personalisation words', p.notesParts);
    ok(!/Options|Engraving Side|Font Choice|Vibur/i.test(p.notesText) && !/Engraving Side|Font Choice|Vibur/i.test(p.secText), 'the customer chose Front and the font: not said here (no Options line)', { notes: p.notesText, sec: p.secText });
    await R.shot(w, 'MSG-front-only');

    // ── 4. front + back, labelled ──
    console.log('front + back, labelled');
    p = await open('both');
    ok(p.cards >= 1, 'cards shown for a front + back order');
    noBlock('both', p);
    ok(p.notesParts.length === 1 && p.notesParts[0][0] === 'Personalisation' && /^front - W\s+back - Name One$/i.test(p.notesParts[0][1]), 'the Overview\'s box has the labelled message as the customer wrote it ("front - W" / "back - Name One")', p.notesParts);
    ok(!/Options|Engraving Side|Font Choice|Ace/i.test(p.notesText) && !/Options|Engraving Side|Font Choice/i.test(p.secText), 'the engraving choices (both sides, the font) are not said here (no Options line)', { notes: p.notesText, sec: p.secText });
    const rdb = await reader([{ lineName: 'CAT', faces: { front: { text: 'W' }, back: { text: 'NAME ONE' } } }]);
    ok(!!rdb && rdb.rows.length === 1 && /front - W \/ back - Name One/i.test(rdb.rows[0][1]), 'the reader returns the labelled message as data (lines joined by " / ")', rdb && rdb.rows);
    ok(!!rdb && rdb.entries.length === 1 && JSON.stringify(rdb.entries[0].faces) === JSON.stringify([['Front', 'W'], ['Back', 'NAME ONE']]), 'the reader still says which words belong to which face, as data: [["Front","W"],["Back","NAME ONE"]]', rdb && rdb.entries);
    ok(!!rdb && (rdb.html.match(/<i>Front<\/i>/g) || []).length === 1 && (rdb.html.match(/<i>Back<\/i>/g) || []).length === 1 && /data-said="faces"/.test(rdb.html), 'its html() names each face once, by the face name only (nothing of it is on the page)', rdb && rdb.html);
    const rdOne = await reader([{ lineName: 'CAT', faces: { front: { text: 'W' } } }]);
    ok(!!rdOne && rdOne.entries.length === 1 && rdOne.entries[0].faces === null, 'one face only: no face marks in the data (nothing to tell apart)', rdOne && rdOne.entries);
    await R.shot(w, 'MSG-front-and-back');

    // ── 5. geometry at 1280x720: the section ends where its cards end, and the first row of cards is on screen ──
    console.log('geometry, 1280x720');
    const geo = await w.page.evaluate(() => {
      const grid = document.getElementById('owEng'), sec = document.getElementById('owEngSec'), sc = document.querySelector('#orderWin .owVInfo .owMain');
      sc.scrollTop = 0; const r = e => { const b = e.getBoundingClientRect(); return [Math.round(b.left), Math.round(b.top), Math.round(b.width), Math.round(b.height)]; };
      const withIt = r(grid), cards = [...grid.querySelectorAll(':scope > .engCard')].map(r);
      const sb = sc.getBoundingClientRect();
      const thumbBottom = Math.max(...[...grid.querySelectorAll('.engCard .engThumb')].map(t => Math.round(t.getBoundingClientRect().bottom)));
      sc.scrollTop = sc.scrollHeight; const reachBottom = Math.round(sec.getBoundingClientRect().bottom), reachTop = Math.round(sec.getBoundingClientRect().top), gridBottom = Math.round(grid.getBoundingClientRect().bottom); sc.scrollTop = 0;
      const secBox = r(sec);
      return { withIt, cards, sec: secBox, gridEnd: withIt[1] + withIt[3], secEnd: secBox[1] + secBox[3], scrollerBottom: Math.round(sb.bottom), vh: innerHeight, thumbBottom, reachBottom, reachTop, gridBottom };
    });
    ok(geo.secEnd - geo.gridEnd <= 4, 'the Engraving section ends where its cards end: nothing is drawn under the grid (the block used to take 8px of margin and its own lines)', { gridEnd: geo.gridEnd, secEnd: geo.secEnd });
    ok(geo.cards.length >= 1 && geo.thumbBottom <= geo.scrollerBottom - 8, 'the first row of cards starts on screen and its pictures are whole on screen without scrolling (ENGBIG: with the 160px pictures the old "whole first row" rule cannot hold; the front + back card\'s button is a few px below the fold at 1280x720)', geo);
    ok(geo.reachBottom <= geo.scrollerBottom + 1 && geo.reachTop >= 0, 'the window\'s own scrolling reaches the whole section (its bottom is inside the scroll area when scrolled to the end)', geo);
    console.log(`  info: front+back order: section ${geo.sec[3]}px tall, its bottom ${geo.secEnd}px, the scroll area ends at ${geo.scrollerBottom}px (window ${geo.vh}px)`);

    // ── 6. a staff note, HTML as text, a live change: the block never comes back ──
    console.log('staff note, text only, live change');
    await open('back');
    await w.page.evaluate(() => { const r = Orders.rows().find(x => document.getElementById('owTitle').textContent.includes(x.order.receiptId)); r.spec.staffNote = 'Engrave <b>bold</b> & "quotes" please'; r.spec.buyerMessage = 'Hello <b>bold</b> there'; OrderWin.paint(); });
    await settle(w, 1600);   // (longer than the page's own 1 s tick: a drawing on the tick would be there by now)
    p = await w.page.evaluate(probe);
    noBlock('staff note + buyer message set', p);
    ok(!/Staff note|Engrave <b>bold/.test(p.secText) && !/Staff note/.test(p.notesText), 'the staff note is not said in the section or in the Overview\'s box (the team\'s messages are in the Team tab)', { sec: p.secText, notes: p.notesText });
    ok(p.notesParts.length === 2 && p.notesParts[1][0] === 'Buyer message' && p.notesParts[1][1] === 'Hello <b>bold</b> there' && !(await w.page.evaluate(() => document.getElementById('owNotes').querySelectorAll('b').length)), 'a buyer message changed while the window is open shows in the Overview\'s box as plain text (markup is not markup)', p.notesParts);
    const rds = await reader();
    ok(!!rds && rds.rows.some(x => x[0] === 'Staff note' && x[1] === 'Engrave <b>bold</b> & "quotes" please') && rds.rows.some(x => x[0] === 'Buyer message'), 'the reader still returns the staff note and the buyer message as data (text, not markup)', rds && rds.rows);
    const html = rds ? await w.page.evaluate(() => { const r = Orders.rows().find(x => document.getElementById('owTitle').textContent.includes(x.order.receiptId)); return OrderEngraving.said.html([{ name: '', rows: OrderEngraving.said.rows(r), faces: null, bars: null }]); }) : '';
    ok(!!html && !/<b>bold/.test(html) && /&lt;b&gt;bold&lt;\/b&gt;/.test(html), 'and its html() escapes them (markup in a note is text)', html);
    await w.page.evaluate(() => { const r = Orders.rows().find(x => document.getElementById('owTitle').textContent.includes(x.order.receiptId)); r.spec.staffNote = ''; r.spec.buyerMessage = ''; OrderWin.paint(); });
    await settle(w, 600);
    p = await w.page.evaluate(probe);
    noBlock('note cleared', p);
    ok(p.notesParts.length === 1 && p.notesParts[0][0] === 'Personalisation', 'the note goes again and the box is the personalisation alone', p.notesParts);

    // ── 6b. the customer's only words were options (a font, a side): the Overview's box says nothing was written; the Engrave tab's reader still has the Options row ──
    console.log('only options');
    await w.page.evaluate(() => { const r = Orders.rows().find(x => document.getElementById('owTitle').textContent.includes(x.order.receiptId)); r.spec.personalization = []; r.spec.buyerMessage = ''; r.spec.staffNote = ''; r.line.personalization = []; OrderWin.paint(); });
    await settle(w, 800);
    p = await w.page.evaluate(probe);
    const optsLeft = SAID ? await w.page.evaluate(() => { const r = Orders.rows().find(x => document.getElementById('owTitle').textContent.includes(x.order.receiptId)); return CharmNestEngraveSaid.rowsOf(r).map(x => x[0]); }) : null;
    if (SAID) ok(optsLeft.join('|') === 'Options', 'the scenario is real: the only thing the reader still has for this line is its Options row', optsLeft);
    noBlock('only options', p);
    ok(p.notesNone && p.notesParts.length === 0 && !/Options|Stylish/i.test(p.notesText), 'a line whose only words were options: the Overview\'s box says the customer wrote nothing and prints no Options', { none: p.notesNone, text: p.notesText });
    ok(p.cards >= 1 && !p.gridHidden, 'the cards are still there');
    const rdo = await reader();
    ok(!!rdo && rdo.rows.length === 0, 'the reader returns no rows for it (the Options row is never in this view\'s data)', rdo && rdo.rows);

    // ── 7. the customer wrote and chose nothing: nothing is shown, the cards stay ──
    console.log('nothing written');
    await w.page.evaluate(() => { const r = Orders.rows().find(x => document.getElementById('owTitle').textContent.includes(x.order.receiptId)); r.spec.personalization = []; r.spec.buyerMessage = ''; r.spec.staffNote = ''; r.spec.options = []; r.line.personalization = []; r.line.variations = []; OrderWin.paint(); });
    await settle(w, 800);
    p = await w.page.evaluate(probe);
    noBlock('nothing written', p);
    ok(p.notesNone && p.notesParts.length === 0, 'the Overview\'s box has no words and no label when the customer wrote and chose nothing', { none: p.notesNone, parts: p.notesParts, text: p.notesText });
    ok(p.cards >= 1 && !p.gridHidden, 'the cards are still there');
    await R.shot(w, 'MSG-nothing-written');

    // ── 8. an order with no engraving at all (an earring pair: never engraved): no section, no block ──
    console.log('no engraving');
    p = await open('earrings');
    ok(p.gridHidden && p.cards === 0, 'an earring pair (never engraved) has no cards', { gridHidden: p.gridHidden, cards: p.cards });
    noBlock('earrings', p);

    // ── 8b. ENGTEXT: the words read decoded ("There's", not "There&#39;s") in the Overview's box: personalisation, buyer message; markup stays text; the order's own text is not changed ──
    console.log('entities in the customer\'s words');
    await open('back');
    const RAW = { pers: 'There&#39;s a made-up note for Name One &amp; Date One', buyer: 'It&#x27;s &quot;Name One&quot; &lt;3', staff: 'Name One&amp;#39;s font' };
    await w.page.evaluate(raw => { const r = Orders.rows().find(x => document.getElementById('owTitle').textContent.includes(x.order.receiptId)); r.spec.personalization = [raw.pers]; r.spec.buyerMessage = raw.buyer; r.spec.staffNote = raw.staff; OrderWin.paint(); }, RAW);
    await settle(w, 800);
    p = await w.page.evaluate(probe);
    noBlock('entities', p);
    ok(p.notesParts.length === 2 && p.notesParts[0][0] === 'Personalisation' && p.notesParts[0][1] === 'There\'s a made-up note for Name One & Date One', 'the Overview\'s personalisation reads "There\'s ... Name One & Date One"', p.notesParts);
    ok(p.notesParts.length === 2 && p.notesParts[1][0] === 'Buyer message' && p.notesParts[1][1] === 'It\'s "Name One" <3', 'the Overview\'s buyer message reads decoded (&#x27; &quot; &lt;)', p.notesParts);
    ok(!/&(?:#\d+|#x[0-9a-f]+|amp|quot|lt|gt|apos);/i.test(p.notesText) && !/&(?:#\d+|#x[0-9a-f]+|amp|quot|lt|gt|apos);/i.test(p.cardTexts), 'no entity is printed raw in the Overview\'s box or in the cards', { notes: p.notesText, cards: p.cardTexts });
    const rde = await reader();
    ok(!!rde && rde.rows.find(x => x[0] === 'Personalisation')[1] === 'There\'s a made-up note for Name One & Date One' && rde.rows.find(x => x[0] === 'Staff note')[1] === 'Name One\'s font', 'the reader\'s data is decoded too (the personalisation, and the double-escaped &amp;#39; of the staff note)', rde && rde.rows);
    const kept = await w.page.evaluate(() => { const r = Orders.rows().find(x => document.getElementById('owTitle').textContent.includes(x.order.receiptId)); return [r.spec.personalization[0], r.spec.buyerMessage, r.spec.staffNote]; });
    ok(kept[0] === RAW.pers && kept[1] === RAW.buyer && kept[2] === RAW.staff, 'what is stored is untouched: the order still holds the text as Etsy gave it (display only)', kept);
    await R.shot(w, 'MSG-entities');

    // ── 9. clean ──
    ok(w.errors.length === 0, 'no page error', w.errors);
    ok(w.warns.length === 0, 'no "order view:" warning (OrderWin swallows errors with it)', w.warns);
  } finally { await w.close(); }
  console.log(`\n${fail ? 'FAILED' : 'PASSED'}: ${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
