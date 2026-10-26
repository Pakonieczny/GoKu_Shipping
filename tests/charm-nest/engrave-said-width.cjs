// ENGTEXT (Paul, 13 Oct 2026 04:14 UTC, over a screenshot of the order window's ENGRAVING section after ENGBIG's 2x pictures): "Ensure this text stays contained underneath each engraving card."
// -> NOSAID (Paul, 17 Oct 2026): "Remove the personalization info from beneath the Engraving card inside the Detailed Order modal."
// History: the customer-words block (#owEngSaid: a left rule, the small PERSONALISATION label, the buyer's sentence) sat under the cards; ENGTEXT made it exactly as wide as the card above it and decoded the apostrophe it
// printed as the raw text "&#39;". NOSAID took the block away: nothing is drawn under the cards any more (charm-nest-order-engraving.js paintSaid only removes what an earlier drawing left). So what this test measures now is that,
// at every width, with every kind of words, there is NO block, the cards are laid out as they were (tracks, spans, no sideways scroll), the Engraving section ends where its last row of cards ends, and the customer's words are
// where they stay: the Overview's own "What the customer wrote" box (#owNotes), decoded, whole and wrapped inside its box. This test builds the REAL page over the REAL-drawing world (tests/charm-nest/_engraveRealWorld.cjs: real .ai drawings,
// real intake / pool / classifier / fit, offline fake site, no Etsy, no paid AI, nothing real written, every name made up) and measures at 1024, 1280, 1356, 1440 and 1920 px wide, for
//   one       a charm engraved on one face: ONE card, one track wide
//   both      a charm engraved front + back: ONE card spanning two tracks
//   long      a very long buyer message (with an address that has no spaces): the Engraving section is EXACTLY as tall as with a short message (the words never push the cards), and every word can be read in the Overview's box
//   multiA    one order of three lines, first card single (the order of cards: one track, a two-track front + back card, then three discs on a second row)
//   multiB    the same lines with the front + back card FIRST
//   tracks    the section forced to 340, 361, 362, 547, 548, 713, 714, 839, 840, 1059, 1060 and 1196px (every grid rule's edge: 1 to 5 tracks), one card and front + back
// and checks: no words block anywhere in the order window (not under the cards, not in a card, not after the grid), the section ends where its cards end, no sideways scroll anywhere (the section, the window's scroll area, the page),
// the words are the customer's with their HTML entities decoded (&#39; &amp; &quot; &lt; and a double-escaped &amp;#39;) in the Overview's box (the personalisation and the buyer message; the staff note was only ever in the removed block),
// markup in a note stays text, nothing in the section prints an entity raw, and what is stored (the order's own text) is not changed.
// NOT this test's: the engraving JOB's own words (what a card prints under its picture) are the job's text as the intake made it; a personalisation that carries an entity gives a job that carries it too (found by ENGTEXT with a made-up
// "Name One&#39;s": the card, its picture and the job read it raw, and a front + back job upper-cases "&amp;" to "&AMP;"). That is the engraving intake's, not display: this test does not decode it (it would hide it).
// The section's scrollWidth is its clientWidth + 20px at every width: that is the grid's seal room (charm-nest-1.html, #owEng.engGrid padding-right / margin-right), allowed by the check, nothing scrolls (26 Oct 2026).
// Run:  PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/engrave-said-width.cjs      SHOTS=<dir> keeps SAIDW-<scenario>-<width>.png (the section as the person sees it)
// What a pass does NOT prove: a live order (the fake site has the real drawings and fonts but not the live data), a browser other than headless Chromium, widths other than the five run.
'use strict';
const R = require('./_engraveRealWorld.cjs');
const Base = require('./_orderFactsWorld.cjs');
let pass = 0, fail = 0;
const ok = (c, m, extra) => { if (c) { pass++; console.log('  PASS ' + m); } else { fail++; console.log('  FAIL ' + m + (extra !== undefined ? ' :: ' + (typeof extra === 'string' ? extra : JSON.stringify(extra)) : '')); } };

// an order of several lines, each its own listing (so each line's saved answers apply), each its own words (the world's own orderOf gives lines after the first none; this one is the same, plus a `listing` and `personalization` for them)
Base.orderOf = function (fx, now) {
  const at = (d, h, m) => { const t = new Date(now); t.setDate(t.getDate() + d); t.setHours(h, m, 0, 0); return Math.floor(t.getTime() / 1000); };
  const shipBy = at(fx.ship, 17, 0), tid = fx.rid + '1';
  return { receiptId: fx.rid, orderNumber: fx.rid, createTs: at(fx.placed.day, fx.placed.h, fx.placed.m), updateTs: at(0, 12, 0), shipBy, buyer: { name: fx.buyer }, buyerMessage: fx.buyerMessage || '', isGift: !!fx.gift, giftMessage: fx.giftMessage || '', staffNote: '', messages: [],
    lines: [{ transactionId: tid, listingId: fx.listing || ('18' + fx.rid.slice(-6)), sku: fx.sku, title: fx.title, quantity: fx.qty || 1, expectedShipDate: shipBy, variations: fx.variations || [], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: fx.personalization || [], buyerMessage: fx.buyerMessage || '' }]
      .concat((fx.more || []).map((m, i) => ({ transactionId: fx.rid + (i + 2), listingId: m.listing || ('19' + fx.rid.slice(-6) + i), sku: m.sku, title: m.title, quantity: m.qty || 1, expectedShipDate: shipBy, variations: m.variations || [], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: m.personalization || [], buyerMessage: '' }))) };
};
const V = (name, value) => ({ name, value });
const LINE = {
  back: R.ORDERS.back, both: R.ORDERS.both, discs: R.ORDERS.discs,
};
const more = n => ({ sku: LINE[n].sku, title: LINE[n].title, listing: LINE[n].listing, variations: LINE[n].variations, personalization: LINE[n].personalization });
Object.assign(R.ORDERS, {
  multiA: Object.assign({}, R.ORDERS.back, { rid: '4290000231', listing: '1890000101', buyer: 'Name Twelve', more: [more('both'), more('discs')] }),   // one, then front + back (two tracks), then three discs
  multiB: Object.assign({}, R.ORDERS.both, { rid: '4290000232', listing: '1890000103', buyer: 'Name Thirteen', more: [more('back')] }),            // front + back first, then one
});
Object.assign(R.EXPECT_JOBS, { multiA: 6, multiB: 3 });

const WIDTHS = [[1024, 768], [1280, 720], [1356, 768], [1440, 900], [1920, 1080]];
const SHOTS = process.env.SHOTS || '';
// the made-up words (entities as Etsy hands them over); EXPECTED is what the person must read
const RAW = {
  pers: 'There&#39;s a made-up gift note for Name One &amp; Name Two: please engrave &quot;Date One&quot; and &lt;3 on the back, and keep the sentence going so that it must wrap inside the card.',
  buyer: 'Hello &lt;b&gt;bold&lt;/b&gt;, it&#x27;s Name Two &amp;amp; Name One, please say &quot;Date One&quot;.',
  staff: 'Mind Name One&amp;#39;s font',
};
const EXPECTED = {
  pers: 'There\'s a made-up gift note for Name One & Name Two: please engrave "Date One" and <3 on the back, and keep the sentence going so that it must wrap inside the card.',
  buyer: 'Hello <b>bold</b>, it\'s Name Two & Name One, please say "Date One".',
  staff: 'Mind Name One\'s font',
};
const URL = 'https://example.test/' + 'x'.repeat(150);
const LONG = Array.from({ length: 14 }, (_, i) => `Sentence ${i + 1} is made up for Name One and Date One and goes on for a while.`).join(' ') + ' Address ' + URL + ' END-OF-MESSAGE';

/** what the person sees of the Engraving section and of the Overview's words box (runs in the page) */
function probe() {
  const rc = e => { if (!e) return null; const b = e.getBoundingClientRect(); return { l: b.left, t: b.top, r: b.right, b: b.bottom, w: b.width, h: b.height }; };
  const host = document.getElementById('owEng'), sec = document.getElementById('owEngSec'), notes = document.getElementById('owNotes'), dlg = document.getElementById('orderWin');
  const scroller = document.querySelector('#orderWin .owVInfo .owMain');
  const cards = host ? [...host.querySelectorAll(':scope > .engCard')] : [];
  const grid = host ? getComputedStyle(host) : null;
  return {
    blocks: dlg ? dlg.querySelectorAll('#owEngSaid, .engSaid, .esRow, .esName, [data-said]').length : -1, byId: !!document.getElementById('owEngSaid'), gridNext: host && host.nextElementSibling ? (host.nextElementSibling.id || host.nextElementSibling.className) : '',
    cards: cards.length, first: rc(cards[0]), cardRects: cards.map(rc), grid: rc(host), sec: rc(sec),
    tracks: grid ? grid.gridTemplateColumns.split(' ').length : 0,
    firstFaces: cards[0] ? (cards[0].querySelector('.engFaces') || { dataset: {} }).dataset.n || '' : '',
    gutter: host ? Math.max(0, -parseFloat(getComputedStyle(host).marginRight) || 0) : 0, secScrollW: sec ? sec.scrollWidth : 0, secClientW: sec ? sec.clientWidth : 0, hostScrollW: host ? host.scrollWidth : 0, hostClientW: host ? host.clientWidth : 0,
    docScrollW: document.documentElement.scrollWidth, innerW: innerWidth, scrollerScrollW: scroller ? scroller.scrollWidth : 0, scrollerClientW: scroller ? scroller.clientWidth : 0,
    notesScrollW: notes ? notes.scrollWidth : 0, notesClientW: notes ? notes.clientWidth : 0, notesBox: rc(notes),
    notesText: notes ? notes.innerText.replace(/\s+/g, ' ').trim() : '', notesContent: notes ? notes.textContent : '', notesHtml: notes ? notes.innerHTML : '',
    notesParts: notes ? [...notes.querySelectorAll('.lbl')].map(l => { let t = ''; for (let n = l.nextSibling; n && !(n.nodeType === 1 && n.classList.contains('lbl')); n = n.nextSibling) t += n.textContent; return [l.textContent.trim(), t.trim()]; }) : [],
    markup: notes ? notes.querySelectorAll('b').length : 0,
    inCards: cards.reduce((n, c) => n + c.querySelectorAll('.esRow, .engSaid').length, 0), cardsText: host ? host.innerText.replace(/\s+/g, ' ') : '', secText: sec ? sec.innerText.replace(/\s+/g, ' ').trim() : '',
    lastRowBottom: cards.length ? Math.max(...cards.map(c => c.getBoundingClientRect().bottom)) : 0,
    firstRowBottom: cards.length ? Math.max(...cards.filter(c => Math.abs(c.getBoundingClientRect().top - cards[0].getBoundingClientRect().top) < 3).map(c => c.getBoundingClientRect().bottom)) : 0
  };
}
/** put made-up words on every line of the order the window shows (display fields only: the engraving jobs are untouched), and repaint the window so the Overview's box reads them */
const words = (w, words) => w.page.evaluate(({ words }) => {
  const rid = document.getElementById('owTitle').textContent.match(/\d{8,}/); const rows = Orders.rows().filter(r => !rid || r.order.receiptId === rid[0]);
  rows.forEach((r, i) => { const o = words[Math.min(i, words.length - 1)]; if (!o) return; for (const k of Object.keys(o)) r.spec[k] = o[k]; });
  OrderWin.paint();
  return rows.length;
}, { words });
const stored = w => w.page.evaluate(() => { const rid = document.getElementById('owTitle').textContent.match(/\d{8,}/); return Orders.rows().filter(r => !rid || r.order.receiptId === rid[0]).map(r => ({ p: r.spec.personalization, b: r.spec.buyerMessage, s: r.spec.staffNote })); });
const waitNotes = (w, re) => w.page.waitForFunction(src => new RegExp(src).test((document.getElementById('owNotes') || {}).textContent || ''), re.source, { timeout: 8000 }).catch(() => {});
const measure = async (w, width, height) => { await w.page.setViewportSize({ width, height }); await R.settle(w.page, 350); return w.page.evaluate(probe); };

/** the layout checks every case shares: the cards as they were, no block, no sideways scroll */
function laidOut(tag, p) {
  const f = p.first, E = 0.6;
  ok(p.cards >= 1 && !!f && !!p.sec, `${tag}: the cards are on the page`, { cards: p.cards });
  ok(p.blocks === 0 && !p.byId && p.gridNext === '' && p.inCards === 0, `${tag}: NOSAID: no customer-words block (none under the cards, none after the grid, none inside a card)`, { blocks: p.blocks, byId: p.byId, gridNext: p.gridNext, inCards: p.inCards });
  if (!f || !p.sec) return;
  ok(p.sec.b - p.lastRowBottom <= 4, `${tag}: the Engraving section ends where its last row of cards ends (${(p.sec.b - p.lastRowBottom).toFixed(1)}px below it)`, { sec: p.sec, lastRowBottom: p.lastRowBottom });
  ok(f.r <= p.sec.r + E && f.l >= p.sec.l - E, `${tag}: the first card is inside the section`, { card: f, sec: p.sec });
  // (the grid's seal room: #owEng padding-right 20px taken back by margin-right -20px, so a seal hanging off a card's corner is not clipped; the section does not clip (overflow visible), so that room is
  // in its scrollWidth without anything scrolling: the window's own scroll area is checked just below. More than that room is a real overflow)
  ok(p.secScrollW <= p.secClientW + p.gutter + 1 && p.hostScrollW <= p.hostClientW + 1, `${tag}: the section does not scroll sideways (beyond the grid's ${p.gutter}px seal room)`, { sec: [p.secScrollW, p.secClientW], gutter: p.gutter, host: [p.hostScrollW, p.hostClientW] });
  ok(p.scrollerScrollW <= p.scrollerClientW + 1 && p.docScrollW <= p.innerW + 1, `${tag}: the window's scroll area and the page do not scroll sideways`, { scroller: [p.scrollerScrollW, p.scrollerClientW], doc: [p.docScrollW, p.innerW] });
}
const noRawEntity = (tag, p) => ok(!/&(?:#\d+|#x[0-9a-f]+|amp|quot|lt|gt|apos);/i.test(p.notesContent) && !/&(?:#\d+|#x[0-9a-f]+|amp|quot|lt|gt|apos);/i.test(p.cardsText), `${tag}: no entity printed raw in the Overview's box or in the cards`, { notes: p.notesContent.slice(0, 160), cards: p.cardsText.slice(0, 160) });
const part = (p, k) => (p.notesParts.find(r => r[0] === k) || [])[1];

(async () => {
  const w = await R.world({ viewport: { width: 1280, height: 720 }, shots: SHOTS || '/tmp/saidw-shots' });
  if (!w) { console.log('SKIP: no playwright-core'); return; }
  try {
    w.srv.st.agentResults.engraveIntent = o => {
      const txt = String(o.messages[0].content[0].text), m = /Personalisation field: (\[.*?\])/.exec(txt); let p = []; try { p = JSON.parse(m ? m[1] : '[]'); } catch (_) {}
      const req = { side: 'back', font: null, handwriting: false, image: false };
      if (p.length) return { engrave: true, text: p.join('\n'), source: 'personalization', sourceQuote: p[0], requests: req, questions: [], confidence: 0.97 };
      return { engrave: false, text: '', source: 'none', sourceQuote: '', requests: req, questions: [], confidence: 0.9 };
    };
    const NAMES = ['back', 'both', 'discs', 'multiA', 'multiB'];
    const made = await R.build(w, NAMES);
    for (const n of NAMES) ok(made[n] && made[n].jobs.length === R.EXPECT_JOBS[n], `real world: order "${n}" has its ${R.EXPECT_JOBS[n]} Engrave job(s) (real drawing, real fit)`, made[n] && made[n].jobs.map(j => j.key));
    const open = async n => { await R.openOrderWindow(w, n); await R.settle(w.page, 300); };
    const shot = async (name) => { if (!SHOTS) return; await w.page.evaluate(() => { const s = document.getElementById('owEngSec'); const sc = document.querySelector('#orderWin .owVInfo .owMain'); if (s && sc) sc.scrollTop = Math.max(0, s.offsetTop - 40); }); await R.settle(w.page, 150); const el = await w.page.$('#owEngSec'); if (el) { const b = await el.boundingBox(); await w.page.screenshot({ path: `${SHOTS}/${name}.png`, clip: { x: Math.max(0, b.x - 12), y: Math.max(0, b.y - 8), width: Math.min(b.width + 24, w.page.viewportSize().width - Math.max(0, b.x - 12)), height: Math.min(b.height + 16, w.page.viewportSize().height - Math.max(0, b.y - 8)) } }); } };
    if (SHOTS) require('fs').mkdirSync(SHOTS, { recursive: true });
    const secH = {};   // the Engraving section's height at each width with a short message (the long message must not change it)

    // ── one card ──
    console.log('one card (a charm engraved on one face)');
    await open('back');
    await words(w, [{ personalization: [RAW.pers], buyerMessage: '', staffNote: '' }]);
    await waitNotes(w, /gift note/);
    for (const [W, H] of WIDTHS) {
      const p = await measure(w, W, H); const tag = `one @${W}`;
      laidOut(tag, p);
      secH[W] = p.sec && p.sec.h;
      ok(p.cards === 1, `${tag}: one card`, p.cards);
      ok(p.notesParts.length === 1 && p.notesParts[0][0] === 'Personalisation' && p.notesParts[0][1] === EXPECTED.pers, `${tag}: the Overview's box reads the personalisation as the customer wrote it ("There's", "&", quotes, "<3")`, p.notesParts);
      noRawEntity(tag, p);
      if (W === 1280 || W === 1440) await shot(`SAIDW-one-${W}`);
    }
    const st1 = await stored(w);
    ok(st1[0].p[0] === RAW.pers, 'one: what is stored is untouched (the order still holds the text as Etsy gave it)', st1);

    // ── one card, all three kinds of words (personalisation, buyer message, staff note), markup stays text ──
    console.log('one card, three kinds of words');
    await words(w, [{ personalization: [RAW.pers], buyerMessage: RAW.buyer, staffNote: RAW.staff }]);
    await waitNotes(w, /Buyer message/);
    for (const [W, H] of WIDTHS) {
      const p = await measure(w, W, H); const tag = `one, three kinds @${W}`;
      laidOut(tag, p);
      ok(part(p, 'Personalisation') === EXPECTED.pers && part(p, 'Buyer message') === EXPECTED.buyer, `${tag}: the Overview's box reads the personalisation and the buyer message decoded (&#39; &#x27; &amp; &quot; &lt; &gt; and the double-escaped forms)`, p.notesParts);
      ok(!/Staff note/.test(p.notesText) && !/Mind Name One/.test(p.notesText + ' ' + p.secText), `${tag}: the staff note (only ever in the removed block) is said neither under the cards nor in the Overview's box`, { notes: p.notesText, sec: p.secText });
      ok(p.markup === 0, `${tag}: "<b>bold</b>" from the message is text, not markup`, p.notesHtml.slice(0, 200));
      ok(p.sec && Math.abs(p.sec.h - secH[W]) <= 1, `${tag}: a buyer message and a staff note do not change the Engraving section (${p.sec && p.sec.h.toFixed(1)}px, as with the personalisation alone: ${secH[W] && secH[W].toFixed(1)}px)`, { h: p.sec && p.sec.h, was: secH[W] });
      noRawEntity(tag, p);
      if (W === 1280) await shot('SAIDW-three-1280');
    }

    // ── front + back ──
    console.log('front + back (one card of two tracks)');
    await open('both');
    await words(w, [{ personalization: [RAW.pers], buyerMessage: '', staffNote: '' }]);
    await waitNotes(w, /gift note/);
    for (const [W, H] of WIDTHS) {
      const p = await measure(w, W, H); const tag = `front+back @${W}`;
      laidOut(tag, p);
      ok(p.cards === 1 && p.firstFaces === '2', `${tag}: one card with both faces`, { cards: p.cards, faces: p.firstFaces });
      ok(p.notesParts.length === 1 && part(p, 'Personalisation') === EXPECTED.pers, `${tag}: the Overview's box has the personalisation (and only that: the "Front / Back" words line was only ever in the removed block)`, p.notesParts);
      noRawEntity(tag, p);
      if (W === 1280 || W === 1440) await shot(`SAIDW-front-back-${W}`);
    }
    {
      const p = await measure(w, 1280, 720);
      ok(p.first && p.first.w > 380, 'front + back @1280: the wide card is two tracks (more than 380px), not a single track', { card: p.first });
    }

    // ── a very long message ──
    console.log('a very long buyer message with an address that has no spaces');
    await open('back');
    await words(w, [{ personalization: [RAW.pers], buyerMessage: LONG, staffNote: '' }]);
    await waitNotes(w, /END-OF-MESSAGE/);
    for (const [W, H] of WIDTHS) {
      const p = await measure(w, W, H); const tag = `long @${W}`;
      laidOut(tag, p);
      ok(p.sec && Math.abs(p.sec.h - secH[W]) <= 1, `${tag}: the long message does not make the Engraving section taller (${p.sec && p.sec.h.toFixed(1)}px, as with the short message: ${secH[W] && secH[W].toFixed(1)}px): the cards are not pushed`, { h: p.sec && p.sec.h, was: secH[W] });
      ok(!/END-OF-MESSAGE|Sentence 1 /.test(p.secText), `${tag}: no word of it is in the Engraving section`, p.secText.slice(0, 200));
      ok(p.notesContent.includes('END-OF-MESSAGE') && p.notesContent.includes(URL), `${tag}: nothing is cut: the whole message and the whole address are in the Overview's box`);
      ok(p.notesScrollW <= p.notesClientW + 1, `${tag}: the Overview's box wraps the address inside itself (${p.notesScrollW} <= ${p.notesClientW}): no sideways scroll`, { scrollW: p.notesScrollW, clientW: p.notesClientW });
      if (W === 1280 || W === 1440) await shot(`SAIDW-long-${W}`);
    }

    // ── several cards ──
    for (const [name, firstKind] of [['multiA', 'a single card'], ['multiB', 'a front + back card']]) {
      console.log(`several cards, ${name}: the first card is ${firstKind}`);
      await open(name);
      await words(w, [{ personalization: [RAW.pers], buyerMessage: '', staffNote: '' }, { personalization: ['Name One - second line of the order, made up'], buyerMessage: '' }, { personalization: ['Name One - third line of the order, made up'], buyerMessage: '' }]);
      await waitNotes(w, /Personalisation/);
      for (const [W, H] of WIDTHS) {
        const p = await measure(w, W, H); const tag = `${name} @${W}`;
        laidOut(tag, p);
        ok(p.cards === (name === 'multiA' ? 5 : 2), `${tag}: several cards (${p.cards}: ${name === 'multiA' ? 'one, one front + back, three discs' : 'one front + back, one single'})`, p.cards);
        ok(p.blocks === 0 && p.inCards === 0, `${tag}: no words block under the cards and none inside any card (it was said once, under the first card; now it is not said there at all)`, { blocks: p.blocks, inCards: p.inCards });
        ok(p.notesParts.length === 1 && p.notesParts[0][0] === 'Personalisation' && p.notesParts[0][1] === EXPECTED.pers, `${tag}: the Overview's box holds the words of the line the window shows (the first line's, decoded), not the other lines'`, p.notesParts);
        noRawEntity(tag, p);
        if (W === 1280 || W === 1440) await shot(`SAIDW-${name}-${W}`);
      }
      const p = await measure(w, 1280, 720);
      ok(p.firstFaces === (name === 'multiB' ? '2' : '1'), `${name} @1280: the first card is ${firstKind}`, p.firstFaces);
      if (name === 'multiA') ok(p.cardRects.some(c => c.t > p.first.t + 3), `${name} @1280: the cards run to a second row (and the section still ends at the last row)`, p.cardRects);
      if (name === 'multiA') ok(p.first.w < 230 && p.first.h > 0, 'multiA @1280: a single first card is one track wide (about 207px), not the width of the section', p.first);
      if (name === 'multiB') ok(p.first.w > 380, 'multiB @1280: a front + back first card is two tracks', p.first);
    }

    // ── every track count, at the container query's own edges: the section is set to a width (in a 1920px window, where the column has room), so 1 track (under 362px, a phone), 2, 3, 4 and 5 are all reached, one card and front + back ──
    console.log('every track count (the section forced to 340 ... 1196px), one card and front + back');
    await w.page.setViewportSize({ width: 1920, height: 1080 });
    for (const n of ['back', 'both']) {
      await open(n);
      await words(w, [{ personalization: [RAW.pers], buyerMessage: '', staffNote: '' }]);
      await waitNotes(w, /gift note/);
      for (const px of [340, 361, 362, 547, 548, 713, 714, 839, 840, 1059, 1060, 1196]) {
        await w.page.evaluate(px => { document.getElementById('owEngSec').style.width = px + 'px'; }, px); await R.settle(w.page, 200);
        const p = await w.page.evaluate(probe); const tag = `${n === 'both' ? 'front+back' : 'one'} in a ${px}px section`;
        laidOut(tag, p);
        const want = px <= 361 ? 1 : px <= 547 ? 2 : px < 840 ? 3 : px < 1060 ? 4 : 5;
        ok(p.tracks === want, `${tag}: ${want} track(s)`, p.tracks);
        if (n === 'both') ok(p.first && (px <= 361 ? p.first.w < px + 1 : p.first.w > (px - 10 * (want - 1)) / want * 1.9), `${tag}: the front + back card is ${px <= 361 ? 'the one track' : 'two tracks'} (${p.first && p.first.w.toFixed(1)})`, { card: p.first });
      }
      await w.page.evaluate(() => { document.getElementById('owEngSec').style.width = ''; });
    }

    // ── clean ──
    ok(w.errors.length === 0, 'no page error', w.errors);
    ok(w.warns.length === 0, 'no "order view:" warning (OrderWin swallows errors with it)', w.warns);
  } finally { await w.close(); }
  console.log(`\n${fail ? 'FAILED' : 'PASSED'}: ${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
