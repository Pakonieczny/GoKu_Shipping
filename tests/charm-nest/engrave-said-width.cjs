// ENGTEXT (Paul, 13 Oct 2026 04:14 UTC, over a screenshot of the order window's ENGRAVING section after ENGBIG's 2x pictures): "Ensure this text stays contained underneath each engraving card."
// The customer-words block (#owEngSaid: a left rule, the small PERSONALISATION label, the buyer's sentence) sat under the cards and ran the FULL width of the section, far wider than the card above it, wrapping only at
// the window's edge; and it printed the apostrophe as the raw text "&#39;". This test builds the REAL page over the REAL-drawing world (tests/charm-nest/_engraveRealWorld.cjs: real .ai drawings, real intake / pool / classifier /
// fit, offline fake site, no Etsy, no paid AI, nothing real written, every name made up) and measures the block at 1024, 1280, 1356, 1440 and 1920 px wide, for
//   one       a charm engraved on one face: ONE card, one track wide. The block is exactly that card's width, directly under it
//   both      a charm engraved front + back: ONE card spanning two tracks. The block is that wider card's width
//   long      a very long buyer message (with an address that has no spaces): the block wraps inside the card's width, never grows sideways, scrolls inside its own max-height, and every word can be read
//   multiA    one order of three lines, first card single (the order of cards: one track, a two-track front + back card, then three discs on a second row): the words are said ONCE, under the FIRST card, its width
//   multiB    the same lines with the front + back card FIRST: the block is that card's width (two tracks)
// and checks: the block's right edge never passes the right edge of the card it sits under (and its left edge is the card's), its width is the card's, nothing inside it sticks out, no sideways scroll anywhere
// (the block, the section, the window's scroll area, the page), the words are the customer's with their HTML entities decoded (&#39; &amp; &quot; &lt; and a double-escaped &amp;#39;) in the personalisation, the buyer message and the staff
// note, markup in a note stays text, nothing in the section prints an entity raw, the sentence is not repeated under every card, and what is stored (the order's own text) is not changed.
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

/** what the person sees of the block and of the card it sits under (runs in the page) */
function probe() {
  const rc = e => { if (!e) return null; const b = e.getBoundingClientRect(); return { l: b.left, t: b.top, r: b.right, b: b.bottom, w: b.width, h: b.height }; };
  const host = document.getElementById('owEng'), said = document.getElementById('owEngSaid'), sec = document.getElementById('owEngSec');
  const scroller = document.querySelector('#orderWin .owVInfo .owMain');
  const cards = host ? [...host.querySelectorAll(':scope > .engCard')] : [];
  const shown = !!(said && !said.hidden && said.getBoundingClientRect().width > 0);
  const kids = shown ? [...said.querySelectorAll('*')].filter(n => { const b = n.getBoundingClientRect(), s = said.getBoundingClientRect(); return b.width > 0 && b.right > s.right + 0.5; }).map(n => n.className || n.tagName) : [];
  const grid = host ? getComputedStyle(host) : null;
  return {
    shown, cards: cards.length, first: rc(cards[0]), cardRects: cards.map(rc), said: shown ? rc(said) : null, grid: rc(host), sec: rc(sec),
    tracks: grid ? grid.gridTemplateColumns.split(' ').length : 0,
    firstFaces: cards[0] ? (cards[0].querySelector('.engFaces') || { dataset: {} }).dataset.n || '' : '',
    saidScrollW: said ? said.scrollWidth : 0, saidClientW: said ? said.clientWidth : 0, saidScrollH: said ? said.scrollHeight : 0, saidClientH: said ? said.clientHeight : 0,
    saidMaxH: said ? getComputedStyle(said).maxHeight : '', saidOverflowY: said ? getComputedStyle(said).overflowY : '',
    secScrollW: sec ? sec.scrollWidth : 0, secClientW: sec ? sec.clientWidth : 0, hostScrollW: host ? host.scrollWidth : 0, hostClientW: host ? host.clientWidth : 0,
    docScrollW: document.documentElement.scrollWidth, innerW: innerWidth, scrollerScrollW: scroller ? scroller.scrollWidth : 0, scrollerClientW: scroller ? scroller.clientWidth : 0,
    kids, text: said ? said.innerText.replace(/\s+/g, ' ').trim() : '', textContent: said ? said.textContent : '', html: said ? said.innerHTML : '',
    rows: said ? [...said.querySelectorAll('.esRow')].map(r => [r.dataset.said, [...r.childNodes].filter(n => !(n.nodeType === 1 && n.tagName === 'I')).map(n => n.textContent).join('').trim()]) : [],
    markup: said ? said.querySelectorAll('b').length : 0,
    inCards: cards.reduce((n, c) => n + c.querySelectorAll('.esRow, .engSaid').length, 0), cardsText: host ? host.innerText.replace(/\s+/g, ' ') : '',
    lastRowBottom: cards.length ? Math.max(...cards.map(c => c.getBoundingClientRect().bottom)) : 0,
    firstRowBottom: cards.length ? Math.max(...cards.filter(c => Math.abs(c.getBoundingClientRect().top - cards[0].getBoundingClientRect().top) < 3).map(c => c.getBoundingClientRect().bottom)) : 0
  };
}
/** put made-up words on every line of the order the window shows (display fields only: the engraving jobs are untouched) */
const words = (w, words) => w.page.evaluate(({ words }) => {
  const rid = document.getElementById('owTitle').textContent.match(/\d{8,}/); const rows = Orders.rows().filter(r => !rid || r.order.receiptId === rid[0]);
  rows.forEach((r, i) => { const o = words[Math.min(i, words.length - 1)]; if (!o) return; for (const k of Object.keys(o)) r.spec[k] = o[k]; });
  return rows.length;
}, { words });
const stored = w => w.page.evaluate(() => { const rid = document.getElementById('owTitle').textContent.match(/\d{8,}/); return Orders.rows().filter(r => !rid || r.order.receiptId === rid[0]).map(r => ({ p: r.spec.personalization, b: r.spec.buyerMessage, s: r.spec.staffNote })); });
const waitSaid = (w, f) => w.page.waitForFunction(f, null, { timeout: 8000 }).catch(() => {});
const measure = async (w, width, height) => { await w.page.setViewportSize({ width, height }); await R.settle(w.page, 350); return w.page.evaluate(probe); };

/** the width checks every case shares */
function contained(tag, p) {
  const f = p.first, s = p.said, E = 0.6;
  ok(p.shown && p.cards >= 1 && !!f && !!s, `${tag}: the card and the words block are both on the page`, { shown: p.shown, cards: p.cards });
  if (!f || !s) return;
  ok(s.r <= f.r + E, `${tag}: the block's right edge (${s.r.toFixed(1)}) does not pass the card's right edge (${f.r.toFixed(1)})`, { block: s, card: f });
  ok(Math.abs(s.l - f.l) <= E, `${tag}: the block starts at the card's left edge (${s.l.toFixed(1)} vs ${f.l.toFixed(1)})`, { block: s, card: f });
  ok(Math.abs(s.w - f.w) <= 1, `${tag}: the block is exactly as wide as the card (${s.w.toFixed(1)} vs ${f.w.toFixed(1)}; ${p.firstFaces === '2' ? 'front + back, two tracks' : 'one track'}, ${p.tracks} tracks)`, { block: s, card: f });
  ok(s.t >= p.lastRowBottom - 1, `${tag}: the block is under the cards (its top ${s.t.toFixed(0)} is not above the last row's bottom ${p.lastRowBottom.toFixed(0)})`, { block: s, lastRowBottom: p.lastRowBottom });
  if (p.cards === 1 || p.cardRects.every(c => Math.abs(c.t - f.t) < 3)) ok(s.t - p.firstRowBottom <= 14, `${tag}: one row of cards: the block is directly beneath it (${(s.t - p.firstRowBottom).toFixed(1)}px below)`, { block: s, rowBottom: p.firstRowBottom });
  ok(p.saidScrollW <= p.saidClientW + 1, `${tag}: nothing inside the block makes it scroll sideways (${p.saidScrollW} <= ${p.saidClientW})`);
  ok(!p.kids.length, `${tag}: nothing inside the block sticks out of its right edge`, p.kids);
  ok(p.secScrollW <= p.secClientW + 1 && p.hostScrollW <= p.hostClientW + 1, `${tag}: the section does not scroll sideways`, { sec: [p.secScrollW, p.secClientW], host: [p.hostScrollW, p.hostClientW] });
  ok(p.scrollerScrollW <= p.scrollerClientW + 1 && p.docScrollW <= p.innerW + 1, `${tag}: the window's scroll area and the page do not scroll sideways`, { scroller: [p.scrollerScrollW, p.scrollerClientW], doc: [p.docScrollW, p.innerW] });
  ok(s.r <= p.sec.r + 0.6, `${tag}: and it is inside the section`, { block: s, sec: p.sec });
}
const noRawEntity = (tag, p) => ok(!/&(?:#\d+|#x[0-9a-f]+|amp|quot|lt|gt|apos);/i.test(p.textContent) && !/&(?:#\d+|#x[0-9a-f]+|amp|quot|lt|gt|apos);/i.test(p.cardsText), `${tag}: no entity printed raw in the block or in the cards`, { block: p.textContent.slice(0, 160), cards: p.cardsText.slice(0, 160) });

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

    // ── one card ──
    console.log('one card (a charm engraved on one face)');
    await open('back');
    await words(w, [{ personalization: [RAW.pers], buyerMessage: '', staffNote: '' }]);
    await waitSaid(w, () => /gift note/.test((document.getElementById('owEngSaid') || {}).textContent || ''));
    for (const [W, H] of WIDTHS) {
      const p = await measure(w, W, H); const tag = `one @${W}`;
      contained(tag, p);
      ok(p.cards === 1, `${tag}: one card`, p.cards);
      ok(p.rows.length === 1 && p.rows[0][0] === 'Personalisation' && p.rows[0][1] === EXPECTED.pers, `${tag}: the personalisation reads as the customer wrote it ("There's", "&", quotes, "<3")`, p.rows);
      noRawEntity(tag, p);
      if (W === 1280 || W === 1440) await shot(`SAIDW-one-${W}`);
    }
    const st1 = await stored(w);
    ok(st1[0].p[0] === RAW.pers, 'one: what is stored is untouched (the order still holds the text as Etsy gave it)', st1);

    // ── one card, all three kinds of words (personalisation, buyer message, staff note), markup stays text ──
    console.log('one card, three kinds of words');
    await words(w, [{ personalization: [RAW.pers], buyerMessage: RAW.buyer, staffNote: RAW.staff }]);
    await waitSaid(w, () => /Staff note/.test((document.getElementById('owEngSaid') || {}).textContent || ''));
    for (const [W, H] of WIDTHS) {
      const p = await measure(w, W, H); const tag = `one, three kinds @${W}`;
      contained(tag, p);
      const by = k => (p.rows.find(r => r[0] === k) || [])[1];
      ok(by('Personalisation') === EXPECTED.pers && by('Buyer message') === EXPECTED.buyer && by('Staff note') === EXPECTED.staff, `${tag}: personalisation, buyer message and staff note all read decoded (&#39; &#x27; &amp; &quot; &lt; &gt; and the double-escaped forms)`, p.rows);
      ok(p.markup === 0, `${tag}: "<b>bold</b>" from the message is text, not markup`, p.html.slice(0, 200));
      noRawEntity(tag, p);
      if (W === 1280) await shot('SAIDW-three-1280');
    }

    // ── front + back ──
    console.log('front + back (one card of two tracks)');
    await open('both');
    await words(w, [{ personalization: [RAW.pers], buyerMessage: '', staffNote: '' }]);
    await waitSaid(w, () => /gift note/.test((document.getElementById('owEngSaid') || {}).textContent || ''));
    for (const [W, H] of WIDTHS) {
      const p = await measure(w, W, H); const tag = `front+back @${W}`;
      contained(tag, p);
      ok(p.cards === 1 && p.firstFaces === '2', `${tag}: one card with both faces`, { cards: p.cards, faces: p.firstFaces });
      ok(p.rows.some(r => r[0] === 'faces') && p.rows.find(r => r[0] === 'Personalisation')[1] === EXPECTED.pers, `${tag}: the personalisation and the "Front / Back" words line are both there`, p.rows);
      noRawEntity(tag, p);
      if (W === 1280 || W === 1440) await shot(`SAIDW-front-back-${W}`);
    }
    {
      const p = await measure(w, 1280, 720);
      ok(p.said && p.first && p.said.w > 380, 'front + back @1280: the block is the wide card\'s width (two tracks, more than 380px), not a single track', { block: p.said, card: p.first });
    }

    // ── a very long message ──
    console.log('a very long buyer message with an address that has no spaces');
    await open('back');
    await words(w, [{ personalization: [RAW.pers], buyerMessage: LONG, staffNote: '' }]);
    await waitSaid(w, () => /END-OF-MESSAGE/.test((document.getElementById('owEngSaid') || {}).textContent || ''));
    for (const [W, H] of WIDTHS) {
      const p = await measure(w, W, H); const tag = `long @${W}`;
      contained(tag, p);
      ok(p.saidScrollH > p.saidClientH + 4 && p.saidOverflowY === 'auto', `${tag}: the long message scrolls inside the block (${p.saidScrollH}px of words in ${p.saidClientH}px), it does not grow`, { scrollH: p.saidScrollH, clientH: p.saidClientH, overflowY: p.saidOverflowY });
      ok(p.saidClientH <= 152 && p.said.h <= 156, `${tag}: the block is at most ${p.saidMaxH} tall (${p.said.h.toFixed(0)}px): the section is not pushed off the screen`, { h: p.said.h, max: p.saidMaxH });
      ok(p.textContent.includes('END-OF-MESSAGE') && p.textContent.includes(URL), `${tag}: nothing is cut: the whole message and the whole address are in the block`);
      const reach = await w.page.evaluate(() => { const s = document.getElementById('owEngSaid'); s.scrollTop = s.scrollHeight; const last = [...s.querySelectorAll('.esRow')].pop(), a = last.getBoundingClientRect(), b = s.getBoundingClientRect(); const r = { lastBottom: a.bottom, boxBottom: b.bottom, atEnd: s.scrollTop + s.clientHeight >= s.scrollHeight - 1 }; s.scrollTop = 0; return r; });
      ok(reach.atEnd && reach.lastBottom <= reach.boxBottom + 1, `${tag}: scrolled to the end, the last words are readable inside the block`, reach);
      if (W === 1280 || W === 1440) await shot(`SAIDW-long-${W}`);
    }

    // ── several cards ──
    for (const [name, firstKind] of [['multiA', 'a single card'], ['multiB', 'a front + back card']]) {
      console.log(`several cards, ${name}: the first card is ${firstKind}`);
      await open(name);
      await words(w, [{ personalization: [RAW.pers], buyerMessage: '', staffNote: '' }, { personalization: ['Name One - second line of the order, made up'], buyerMessage: '' }, { personalization: ['Name One - third line of the order, made up'], buyerMessage: '' }]);
      await waitSaid(w, () => /second line/.test((document.getElementById('owEngSaid') || {}).textContent || ''));
      for (const [W, H] of WIDTHS) {
        const p = await measure(w, W, H); const tag = `${name} @${W}`;
        contained(tag, p);
        ok(p.cards === (name === 'multiA' ? 5 : 2), `${tag}: several cards (${p.cards}: ${name === 'multiA' ? 'one, one front + back, three discs' : 'one front + back, one single'})`, p.cards);
        const said1 = (p.textContent.match(/gift note/g) || []).length;
        ok(said1 === 1, `${tag}: the first line's sentence is said once, not under every card`, said1);
        ok(p.inCards === 0, `${tag}: no words block inside any card (they are said once, under the first card)`, p.inCards);
        ok(p.rows.filter(r => r[0] === 'Personalisation').length >= 2, `${tag}: each line's own words are there (one entry per line, named)`, p.rows);
        noRawEntity(tag, p);
        if (W === 1280 || W === 1440) await shot(`SAIDW-${name}-${W}`);
      }
      const p = await measure(w, 1280, 720);
      ok(p.firstFaces === (name === 'multiB' ? '2' : '1'), `${name} @1280: the first card is ${firstKind}`, p.firstFaces);
      if (name === 'multiA') ok(p.cardRects.some(c => c.t > p.first.t + 3), `${name} @1280: the cards run to a second row (the block sits under the last row, not between rows)`, p.cardRects);
      if (name === 'multiA') ok(p.said.w < 230 && p.said.h > 0, 'multiA @1280: under a single first card the block is one track wide (about 207px), not the width of the section', p.said);
      if (name === 'multiB') ok(p.said.w > 380, 'multiB @1280: under a front + back first card the block is that card\'s two tracks', p.said);
    }

    // ── a window too narrow for tracks: the phone-width section is one track, the block is the whole of it and no more ──
    console.log('a narrow window (section under 362px: one track)');
    await open('both');
    await words(w, [{ personalization: [RAW.pers] }]);
    await waitSaid(w, () => /gift note/.test((document.getElementById('owEngSaid') || {}).textContent || ''));
    for (const [W, H] of [[700, 800], [520, 800]]) {
      const p = await measure(w, W, H); const tag = `front+back narrow @${W}`;
      if (!p.shown) { console.log(`  info: ${tag}: the order window is not laid out at ${W}px in this build (block not shown)`); continue; }
      contained(tag, p);
    }

    // ── clean ──
    ok(w.errors.length === 0, 'no page error', w.errors);
    ok(w.warns.length === 0, 'no "order view:" warning (OrderWin swallows errors with it)', w.warns);
  } finally { await w.close(); }
  console.log(`\n${fail ? 'FAILED' : 'PASSED'}: ${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
