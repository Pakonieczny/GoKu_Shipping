// NO SCROLL to see the engraving cards (Paul, 12 Oct 2026, point 2 of his engraving message):
//   "make sure you can fit at least three cards horizontally in a row before having to go to the next row so that we maximize the chances of the user seeing all of the
//    needed engraving requirements without scrolling. I want that to be tested and ensure that there is no scroll in requirements to see the first row of three engraving
//    cards, and the centre of this page."
// The spec is plans/engrave-ui-1012/CONTRACT.md section B (layout, arithmetic) and G (this test); the section's markup is plans/engrave-ui-1012/INTERFACES.md (ENGLAYOUT).
// This is the TEST only (ENGNOSCROLL). It mounts the REAL order window (charm-nest-1.html, headless Chromium over the offline fake site, bridge-server.cjs through
// _orderFactsWorld.cjs: no Etsy, no paid call, no live write, fake buyer names only) with an order that needs engraving on THREE pieces (three different charms: front + back,
// back only, front only), an order with SIX (the row wraps), one with TWO, one with ONE and one with a charm and an earring pair, at 1280x720, 1440x900, 1024x768 and
// 1920x1080, the order window scrolled to its top, and checks:
//   (a) the engraving section sits directly below the head chip strip (same column, after it in the DOM, not in the left pictures column)
//   (b) a thin divider and an "Engraving" title at the section's top-left
//   (c) the first row holds at least THREE cards side by side (same top within 2 px, every width at least the card minimum)
//   (d) the bottom edge of the first row is inside what the person sees (the scroll area, minus a small margin), nothing covers a card: all three cards, NO scrolling
//   (e) the page has no horizontal scrollbar (nothing wider than the window, the scroll area or the section)
//   (f) with SIX pieces the next row wraps correctly: same columns as the first row, below it, nothing overlapping
//   (g) no text of the deleted list in the cards (TO APPROVE, Waiting for your approval, WORDS ON THE BACK, Read from the personalisation box, "NN% sure", Fix in Engraving,
//       "nothing in the listing or the note says otherwise", Undo my edit, The system read)
//   (h) a card is at most 212 px tall (CONTRACT B: the height the 1280x720 arithmetic leaves)
//   (x) one or two pieces: the card is as wide as in the 3-piece order and at most a third of the grid (never stretched)
//   (i) an earring pair gets no card and no Back slot (CONTRACT: no earring is ever engraved), printed only
//   (j) nothing inside a card spills sideways out of it, printed only
// WHICH checks fail the run is the table GATE below (CONTRACT G: hard at 1280x720 and 1440x900; at 1024x768 the 3-in-a-row and the widths are hard, the below-the-fold
// result for orders of several lines is PRINTED, since only 171 px are free there). The checker itself is proved first on a small synthetic page (a good layout passes; one
// mutant per check fails), so a pass on the real page is not "the check cannot fail".
//
//   node tests/charm-nest/engrave-section-noscroll.cjs
//     PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules  (the dir holding playwright-core; found by itself when unset)   CHROMIUM=<chrome>
//     SHOTS=<dir>  keeps NOSCROLL-<WxH>.png (the section as the person sees it) and NOSCROLL-<WxH>-window.png (the whole window)
//     NOSCROLL_ONLY=1280x720[,..]  only these sizes     NOSCROLL_DEBUG=1  dumps the measurements as JSON     CARD_MIN=<px>  SAFE_BOTTOM=<px>  override the thresholds
//   exit 1 when a gating check fails at any size.
//   What a pass here does NOT prove: the fake site has no master drawings, no real fonts and no real Engrave jobs (thumbnails are stand-ins or empty), the page is headless
//   Chromium only, and the sizes are fixed windows: a live order must be looked at once on the live page.
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');

/* ═══════════════════════════ everything that may need adjusting is HERE ═══════════════════════════ */
// where things are in the page. Each entry is a list of CSS selectors, tried in order; the first match that is on screen wins.
const SEL = {
  dialog: ['#orderWin'],
  scroller: ['#orderWin .owVInfo .owMain', '#orderWin .owMain'],          // the scroll area of the Overview (it scrolls: the test sets it to the top)
  column: ['#orderWin .owInfoMain'],                                      // the centre column of the Overview
  strip: ['#owMeta'],                                                     // the head chip strip: order number, buyer, time, ship by, metal, Details (OrderFacts)
  section: ['#owEngSec', '.engSec', '[data-engraving-section]', '#owEngraving', '#owEng'],   // the section = rule + title + cards (ENGLAYOUT: <section class="engSec" id="owEngSec">); #owEng is the cards' grid host, and main's old left-column host
  grid: ['#owEng.owEngGrid', '.engGrid', '[data-eng-grid]', '#owEng'],    // the cards' grid host (children = cells); else found by its CSS
  card: ['.engCard', '[data-eng-card]', '.owEngCard'],                    // one card of one piece (ENGCARD: article.engCard); when none match, the grid's children are the cards
  titleText: /^engraving:?$/i,                                            // the title's words
};
const SIZES = [[1280, 720], [1440, 900], [1024, 768], [1920, 1080]];     // [width, height]
const T = {
  CARD_MIN: n => +process.env.CARD_MIN || (n < 1200 ? 118 : 200),   // px, CONTRACT G: 118 at 1024 wide, 200 from 1280 (ENGLAYOUT's hard minimum is 112)
  CARD_MAX_H: 212,  // px, CONTRACT B: the card height the 1280x720 arithmetic leaves (not a CSS cap; this test asserts it)
  SAFE_BOTTOM: +process.env.SAFE_BOTTOM || 8,   // px kept free at the foot of what is seen (the scroll area has 28px padding of its own; nothing else sits there)
  TOP_TOL: 2,       // px: "the same top"
  GAP_MAX: 24,      // px, CONTRACT G: the most air between the chip strip and the section
  TITLE_LEFT_MAX: 32, TITLE_TOP_MAX: 56,   // px: how far from the section's top-left corner the title may start
  LINE_MAX: 2.5,    // px: how thick a "thin" divider may be
};
// the words that must appear in no card (Paul's list; the card text is read with innerText and textContent, plus title / aria-label / alt attributes)
const FORBIDDEN = [
  ['TO APPROVE', /to approve/i], ['Waiting for your approval', /waiting for your approval/i], ['WORDS ON THE BACK', /words on the back/i],
  ['Read from the personalisation box', /read from the personali[sz]ation box/i], ['NN% sure', /\d+\s*%\s*sure|\bsure\b[^.]{0,12}\d+\s*%/i], ['Fix in Engraving', /fix in engraving/i],
  ['nothing in the listing or the note says otherwise', /nothing in the listing or the note says otherwise/i], ['Undo my edit', /undo my edit/i], ['The system read', /the system read/i],
];
// which checks FAIL THE RUN, by window size (CONTRACT G). A check not listed is printed only. d = the 3-piece order's first row on screen, d2 = the same for the 2-line order,
// d6 = the 6-piece order's first row on screen (six rows of "Its pieces" push the strip down by themselves: printed where the window is too short for that).
const GATE = {
  '1280x720': 'a b c d d2 e f g h x', '1440x900': 'a b c d d2 e f g x', '1920x1080': 'a b c d d2 e f g x d6',   // (h: the 212px height only binds where the window is short; a wider card has bigger square thumbnails and a taller card, printed)
  '1024x768': 'a b c e f g h x',   // d, d2, d6 printed: two-line orders have only 171 px free at 1024x768 (CONTRACT B)
};
const gated = (size, k) => (GATE[size] || 'a b c d d2 e f g h x').split(' ').includes(k);
const LABEL = { a: 'section directly below the chip strip', b: 'thin divider and "Engraving" title at the top-left', c: 'first row holds 3+ cards side by side, wide enough', d: 'first row fully on screen, no scrolling (3 pieces)', d2: 'first row fully on screen, no scrolling (2 lines)', d6: 'first row fully on screen, no scrolling (6 pieces)', e: 'no horizontal scrollbar', f: '6 pieces: the second row wraps correctly', g: 'no text of the deleted list', h: `card at most ${T.CARD_MAX_H}px tall`, x: '1 or 2 pieces: card not stretched', i: 'earring pair: no card, no Back slot', j: 'nothing inside a card spills out of it' };

/* ═══════════════════════════ the measurement (runs in the page) ═══════════════════════════ */
// Pure reading of the page: no judgement. `cfg` = { SEL (selectors), titleSrc, titleFlags }. Returns plain JSON. Must stay self-contained (it is serialised into the page).
function measureInPage(cfg) {
  const vis = e => { if (!e || !e.isConnected) return false; const r = e.getBoundingClientRect(); if (r.width < 1 || r.height < 1) return false; for (let n = e; n && n.nodeType === 1; n = n.parentElement) { const cs = getComputedStyle(n); if (cs.display === 'none' || cs.visibility === 'hidden' || n.hidden) return false; } return true; };
  const pick = (sels, scope) => { for (const s of sels) { let list; try { list = (scope || document).querySelectorAll(s); } catch (_) { continue; } for (const e of list) if (vis(e)) return e; } return null; };
  const R = e => { if (!e) return null; const r = e.getBoundingClientRect(); return { l: +r.left.toFixed(2), t: +r.top.toFixed(2), r: +r.right.toFixed(2), b: +r.bottom.toFixed(2), w: +r.width.toFixed(2), h: +r.height.toFixed(2) }; };
  const nameOf = e => (e.id ? '#' + e.id : e.className && typeof e.className === 'string' ? '.' + e.className.split(' ')[0] : e.tagName.toLowerCase());
  const out = { vw: innerWidth, vh: innerHeight };
  const dialog = pick(cfg.SEL.dialog), scroller = pick(cfg.SEL.scroller), column = pick(cfg.SEL.column), strip = pick(cfg.SEL.strip);
  // "the window scrolled to its top": every scroll area from the section up to the page is put at 0
  const section = pick(cfg.SEL.section);
  const scrollables = []; for (let n = section || scroller; n; n = n.parentElement) { const cs = getComputedStyle(n); if (/(auto|scroll)/.test(cs.overflowY) && n.scrollHeight > n.clientHeight) scrollables.push(n); }
  const tops = [...new Set([...scrollables, scroller].filter(Boolean))]; for (const n of tops) n.scrollTop = 0; document.scrollingElement.scrollTop = 0;
  out.scrollTops = tops.map(n => n.scrollTop);
  out.dialog = R(dialog); out.scroller = R(scroller); out.column = R(column); out.strip = R(strip); out.section = R(section);
  out.scrollerScrollable = scroller ? scroller.scrollHeight - scroller.clientHeight : null;
  // what the person sees: the scroll area's box, inside the dialog, inside the window
  const sc = scroller ? scroller.getBoundingClientRect() : null, dl = dialog ? dialog.getBoundingClientRect() : null;
  out.visibleBottom = Math.min(innerHeight, dl ? dl.bottom : innerHeight, sc ? sc.bottom : innerHeight);
  out.visibleTop = Math.max(0, dl ? dl.top : 0, sc ? sc.top : 0);
  out.sectionId = section ? nameOf(section) : null;
  out.noSection = !section;
  const eng = document.getElementById('owEng'); out.eng = vis(eng) ? R(eng) : null;
  out.engInPics = !!document.querySelector('#orderWin .owPics #owEng');
  if (!section) return out;
  out.inColumn = !!column && column.contains(section) && !section.closest('.owPics');
  out.afterStrip = !!strip && !!(strip.compareDocumentPosition(section) & Node.DOCUMENT_POSITION_FOLLOWING);
  // ── between the strip and the section: anything of height in the column ──
  if (column && strip) {
    const sr = section.getBoundingClientRect(), br = strip.getBoundingClientRect();
    out.between = [...column.querySelectorAll('*')].filter(e => vis(e) && !e.contains(section) && !section.contains(e) && !e.contains(strip) && !strip.contains(e)).map(e => ({ e, r: e.getBoundingClientRect() })).filter(x => x.r.top >= br.bottom - 1 && x.r.bottom <= sr.top + 1 && x.r.height > 3 && x.r.width > 3).map(x => nameOf(x.e) + ' ' + Math.round(x.r.height) + 'px').slice(0, 6);
  }
  // ── the title: an element of the section whose own text is "Engraving" ──
  const re = new RegExp(cfg.titleSrc, cfg.titleFlags);
  let title = null; { const w = document.createTreeWalker(section, NodeFilter.SHOW_TEXT); for (let n = w.nextNode(); n; n = w.nextNode()) { if (re.test(n.nodeValue.trim()) && n.parentElement && vis(n.parentElement)) { title = n.parentElement; break; } } }
  if (title) { const range = document.createRange(); range.selectNodeContents(title); const tr = range.getBoundingClientRect(); out.title = { l: +tr.left.toFixed(2), t: +tr.top.toFixed(2), r: +tr.right.toFixed(2), b: +tr.bottom.toFixed(2), tag: title.tagName.toLowerCase(), fontPx: parseFloat(getComputedStyle(title).fontSize) }; }
  // ── the divider: a thin full-width line at the top of the section ──
  const sr0 = section.getBoundingClientRect(); const lines = [];
  const clear = c => /rgba\(\s*\d+,\s*\d+,\s*\d+,\s*0\s*\)|transparent/.test(c);
  const edge = (el, label) => { if (!el) return; const cs = getComputedStyle(el), r = el.getBoundingClientRect();
    for (const side of ['Top', 'Bottom']) { const w = parseFloat(cs['border' + side + 'Width']), st = cs['border' + side + 'Style'], c = cs['border' + side + 'Color']; if (w > 0 && st !== 'none' && st !== 'hidden' && !clear(c)) lines.push({ how: label + ' border-' + side.toLowerCase(), y: side === 'Top' ? r.top : r.bottom - w, x: r.left, w: r.width, thick: w }); }
    for (const ps of ['::before', '::after']) { const p = getComputedStyle(el, ps); if (p.content && p.content !== 'none' && p.content !== 'normal' && p.display !== 'none') { const ph = parseFloat(p.height), pw = parseFloat(p.width), bt = parseFloat(p.borderTopWidth) || 0, bg = p.backgroundColor && !clear(p.backgroundColor);
        const top = parseFloat(p.top), left = parseFloat(p.left);
        if (ph > 0 && ph <= 4 && (bg || bt) && pw > 0) lines.push({ how: label + ' ' + ps + ' (' + ph + 'px)', y: p.position === 'absolute' && Number.isFinite(top) ? r.top + top : r.top, x: r.left + (Number.isFinite(left) ? left : 0), w: pw, thick: ph || bt }); } } };
  edge(section, 'section'); edge(section.previousElementSibling, 'previous element');
  for (const k of [...section.children].slice(0, 4)) { edge(k, nameOf(k)); }
  for (const k of section.querySelectorAll('hr,[role=separator]')) if (vis(k)) { const r = k.getBoundingClientRect(), cs = getComputedStyle(k); lines.push({ how: 'hr/separator', y: r.top, x: r.left, w: r.width, thick: Math.max(r.height, parseFloat(cs.borderTopWidth) || 0) }); }
  out.lines = lines.map(l => ({ how: l.how, y: +(l.y - sr0.top).toFixed(2), x: +(l.x - sr0.left).toFixed(2), w: +l.w.toFixed(2), thick: +l.thick.toFixed(2) }));
  // ── the grid and the cards ──
  let grid = pick(cfg.SEL.grid, section) || (section.matches(cfg.SEL.grid.join(',')) ? section : null);
  if (!grid) {
    const kids = e => [...e.children].filter(vis);
    const cand = [section, ...section.querySelectorAll('*')].filter(e => vis(e) && /grid|flex/.test(getComputedStyle(e).display) && kids(e).length >= 1);
    grid = cand.sort((a, b) => kids(b).length - kids(a).length)[0] || null;
  }
  let cards = []; for (const s of cfg.SEL.card) { try { cards = [...section.querySelectorAll(s)].filter(vis); } catch (_) {} if (cards.length) break; }
  out.cardsBy = cards.length ? 'selector' : 'grid children';
  if (!cards.length && grid) cards = [...grid.children].filter(vis);
  if (grid) { const cs = getComputedStyle(grid); out.grid = { sel: nameOf(grid), display: cs.display, cols: cs.gridTemplateColumns, colGap: parseFloat(cs.columnGap) || 0, rowGap: parseFloat(cs.rowGap) || 0, inner: +(grid.clientWidth - parseFloat(cs.paddingLeft) - parseFloat(cs.paddingRight)).toFixed(2), r: R(grid) };
  }
  out.cards = cards.map(c => { const r = c.getBoundingClientRect(); const cx = r.left + r.width / 2, cy = r.top + r.height / 2;
    const inBox = (x, y) => { if (x < 0 || y < 0 || x >= innerWidth || y >= innerHeight) return null; const t = document.elementFromPoint(x, y); return !!t && (c === t || c.contains(t)); };
    const attrs = [c, ...c.querySelectorAll('*')].map(e => ['title', 'aria-label', 'alt', 'placeholder'].map(a => e.getAttribute && e.getAttribute(a) || '').join(' ')).join(' ');
    return { l: +r.left.toFixed(2), t: +r.top.toFixed(2), r: +r.right.toFixed(2), b: +r.bottom.toFixed(2), w: +r.width.toFixed(2), h: +r.height.toFixed(2), centreHit: inBox(cx, cy), footHit: inBox(cx, Math.min(r.bottom - 3, innerHeight - 1)),
      spill: [...c.querySelectorAll('*')].filter(e => vis(e) && (e.getBoundingClientRect().right > r.right + 1.5 || e.getBoundingClientRect().left < r.left - 1.5)).slice(0, 3).map(e => nameOf(e) + ' ' + Math.round(Math.max(e.getBoundingClientRect().right - r.right, r.left - e.getBoundingClientRect().left)) + 'px'),
      text: (c.innerText || '').replace(/\s+/g, ' ').trim(), attrs: attrs.replace(/\s+/g, ' ').trim(), thumbs: c.querySelectorAll('.engThumb, [data-face]').length }; });
  // ── the sideways scroll: anything wider than the window, the scroll area, the section ──
  const de = document.documentElement;
  out.hscroll = { page: de.scrollWidth - de.clientWidth, body: document.body.scrollWidth - de.clientWidth, dialog: dialog ? dialog.scrollWidth - dialog.clientWidth : 0, scroller: scroller ? scroller.scrollWidth - scroller.clientWidth : 0, section: section.scrollWidth - section.clientWidth, column: column ? column.scrollWidth - column.clientWidth : 0,
    scrollerOverflowX: scroller ? getComputedStyle(scroller).overflowX : '' };
  out.wideOnes = [...section.querySelectorAll('*')].filter(e => vis(e) && e.getBoundingClientRect().right > Math.min(innerWidth, dl ? dl.right : innerWidth) + 1).slice(0, 4).map(e => nameOf(e) + ' right=' + Math.round(e.getBoundingClientRect().right));
  out.sectionText = (section.innerText || '').replace(/\s+/g, ' ').trim();
  return out;
}

/* ═══════════════════════════ the judgement (node) ═══════════════════════════ */
const near = (a, b, tol) => Math.abs(a - b) <= tol;
const px = n => (Math.round(n * 10) / 10) + 'px';
/** rows of cards: cards whose tops are within `tol` of each other, top to bottom */
function rowsOf(cards, tol) {
  const rows = []; for (const c of [...cards].sort((x, y) => x.t - y.t || x.l - y.l)) { const row = rows.find(r => near(r[0].t, c.t, tol)); if (row) row.push(c); else rows.push([c]); }
  rows.forEach(r => r.sort((x, y) => x.l - y.l)); return rows;
}
const firstRow = m => rowsOf(m.cards, T.TOP_TOL)[0] || [];
const noSec = { ok: false, say: 'no engraving section in the order window (looked for ' + SEL.section.join(', ') + ')' };
/** every check, from one measurement `m` (+ `x`: { cardMin }). Each -> { ok, say } (say = the numbers). */
const judge = {
  a(m) {
    if (m.noSection) return noSec;
    const s = m.section, strip = m.strip;
    if (!strip) return { ok: false, say: 'the chip strip #owMeta is not on screen' };
    const gap = s.t - strip.b, colOk = s.l <= strip.l + T.TOP_TOL && s.r >= strip.r - T.TOP_TOL && (!m.scroller || s.r <= m.scroller.r + 1);
    const between = m.between || [], bad = [];
    if (!m.inColumn) bad.push('not in the centre column (.owInfoMain)'); if (!m.afterStrip) bad.push('not after #owMeta in the page'); if (m.engInPics) bad.push('#owEng is still in the left pictures column (.owPics)');
    if (!colOk) bad.push(`not under the strip's width (section ${px(s.l)}..${px(s.r)}, strip ${px(strip.l)}..${px(strip.r)})`);
    if (gap < -1 || gap > T.GAP_MAX) bad.push(`gap ${px(gap)} is more than ${T.GAP_MAX}px`);
    if (between.length) bad.push('between them: ' + between.join(', '));
    return { ok: bad.length === 0, say: `section ${m.sectionId} top ${px(s.t)}, chip strip bottom ${px(strip.b)}, gap ${px(gap)} (max ${T.GAP_MAX})${m.eng ? ', cards host top ' + px(m.eng.t) : ''}${bad.length ? ' => ' + bad.join('; ') : ''}` };
  },
  b(m) {
    if (m.noSection) return noSec;
    const s = m.section, t = m.title, first = firstRow(m);
    if (!t) return { ok: false, say: 'no "Engraving" title inside the section' };
    const dl = t.l - s.l, dt = t.t - s.t, above = !first.length || t.b <= first[0].t + 1;
    const line = (m.lines || []).filter(l => l.w >= 0.9 * s.w && l.thick <= T.LINE_MAX && l.y <= (t.t - s.t) + 1).sort((x, y) => x.y - y.y)[0];
    const place = dl >= -1 && dl <= T.TITLE_LEFT_MAX && dt >= -T.LINE_MAX - 24 && dt <= T.TITLE_TOP_MAX && above;
    const bad = []; if (!line) bad.push('no thin full-width line at the top (candidates: ' + ((m.lines || []).map(l => `${l.how} w${Math.round(l.w)} t${l.thick} y${Math.round(l.y)}`).join('; ') || 'none') + ')');
    if (!place) bad.push(`title is not at the top-left (${px(dl)} from the left, ${px(dt)} from the top${above ? '' : ', and not above the cards'})`);
    return { ok: bad.length === 0, say: `title "Engraving" ${px(dl)} from the section's left, ${px(dt)} from its top; divider ${line ? line.how + ', ' + px(line.w) + ' wide, ' + px(line.thick) + ' thick, ' + px(line.y) + ' from the top' : 'NOT FOUND'}${bad.length ? ' => ' + bad.join('; ') : ''}` };
  },
  c(m, x) {
    if (m.noSection) return noSec;
    const rows = rowsOf(m.cards, T.TOP_TOL), first = rows[0] || [], widths = first.map(c => c.w), n = first.length, g = m.grid, min = x.cardMin;
    const minW = widths.length ? Math.min(...widths) : 0, why = [];
    if (n < 3) { const gap = g ? g.colGap : 0; why.push(`only ${n} card(s) in the first row; 3 cards need ${px(3 * min + 2 * gap)} at the ${min}px minimum, the grid is ${g ? px(g.inner) : px(m.section.w)} wide${m.column ? ', the centre column ' + px(m.column.w) : ''}`); }
    if (n >= 3 && minW < min) why.push(`narrowest card ${px(minW)} < minimum ${min}px`);
    return { ok: why.length === 0, say: `${n} card(s) in the first row of ${m.cards.length}, widths [${widths.map(w => Math.round(w)).join(', ')}] (minimum ${min}px)${g ? `, grid ${px(g.inner)} wide, gap ${g.colGap}px, columns "${g.cols}"` : ', no grid found'}${why.length ? ' => ' + why.join('; ') : ''}` };
  },
  d(m) {
    if (m.noSection) return noSec;
    const first = firstRow(m);
    if (!first.length) return { ok: false, say: 'no cards' };
    const bottom = Math.max(...first.map(c => c.b)), top = Math.min(...first.map(c => c.t)), limit = m.visibleBottom - T.SAFE_BOTTOM, need = bottom - limit;
    const covered = first.filter(c => c.centreHit === false || c.footHit === false).length, off = first.filter(c => c.centreHit === null || c.footHit === null).length;
    const want = Math.min(3, m.cards.length), ok = bottom <= limit && top >= m.visibleTop - 1 && covered === 0 && off === 0 && first.length >= want;
    return { ok, say: `first row of ${first.length}: top ${px(top)}, bottom ${px(bottom)}; the window shows down to ${px(m.visibleBottom)} (viewport ${m.vh}, scroll area bottom ${m.scroller ? px(m.scroller.b) : '?'}), minus ${T.SAFE_BOTTOM}px margin = ${px(limit)} => ${need > 0 ? px(need) + ' BELOW the fold (scroll needed)' : px(-need) + ' to spare'}; room for a card ${px(limit - top)} (${m.strip ? px(top - m.strip.b) + ' of rule, title and gaps sit between the chip strip and the cards, ' : ''}tallest card ${px(Math.max(...first.map(c => c.h)))})${first.length < want ? '; fewer than ' + want + ' cards in the first row' : ''}${covered ? '; ' + covered + ' card(s) covered by something else' : ''}${off ? '; ' + off + ' card(s) cut off by the window' : ''}; scroll tops ${JSON.stringify(m.scrollTops)}, the scroll area could scroll ${m.scrollerScrollable}px` };
  },
  e(m) {
    if (m.noSection) return noSec;
    const h = m.hscroll, s = m.section, over = m.cards.filter(c => c.r > s.r + 1 || c.l < s.l - 1).length;
    const ok = h.page <= 0 && h.body <= 0 && h.dialog <= 1 && h.scroller <= 1 && h.section <= 1 && h.column <= 1 && over === 0 && !(m.wideOnes || []).length;
    return { ok, say: `overflow-x px: page ${h.page}, dialog ${h.dialog}, scroll area ${h.scroller} (overflow-x ${h.scrollerOverflowX}), column ${h.column}, section ${h.section}; cards outside the section ${over}${(m.wideOnes || []).length ? '; past the window: ' + m.wideOnes.join(', ') : ''}` };
  },
  f(m6) {
    if (m6.noSection) return noSec;
    const rows = rowsOf(m6.cards, T.TOP_TOL), n = m6.cards.length, cols = rows[0] ? rows[0].length : 0, why = [];
    if (n !== 6) why.push(`${n} cards for 6 pieces`);
    if (cols < 3) why.push(`first row holds ${cols}`);
    if (cols < 6 && rows.length < 2) why.push('no second row although the first holds only ' + cols);
    const lefts = (rows[0] || []).map(c => c.l);
    rows.slice(1).forEach((row, i) => {
      const prevBottom = Math.max(...rows[i].map(c => c.b));
      if (row[0].t < prevBottom - 1) why.push(`row ${i + 2} starts at ${px(row[0].t)}, above row ${i + 1}'s bottom ${px(prevBottom)}`);
      for (const c of row) if (!lefts.some(l => near(l, c.l, T.TOP_TOL))) why.push(`row ${i + 2} card at left ${px(c.l)} is under no column of row 1 [${lefts.map(Math.round).join(', ')}]`);
      for (const c of row) if (rows[0].every(r1 => !near(r1.w, c.w, T.TOP_TOL + 1))) why.push(`row ${i + 2} card is ${px(c.w)} wide, row 1's are [${rows[0].map(r1 => Math.round(r1.w)).join(', ')}]`);
    });
    for (let i = 0; i < m6.cards.length; i++) for (let j = i + 1; j < m6.cards.length; j++) { const p = m6.cards[i], q = m6.cards[j]; if (Math.min(p.r, q.r) - Math.max(p.l, q.l) > 1 && Math.min(p.b, q.b) - Math.max(p.t, q.t) > 1) why.push(`cards ${i + 1} and ${j + 1} overlap`); }
    return { ok: why.length === 0, say: `${n} cards in ${rows.length} row(s) of [${rows.map(r => r.length).join(', ')}]; row tops [${rows.map(r => Math.round(r[0].t)).join(', ')}], first-row widths [${(rows[0] || []).map(c => Math.round(c.w)).join(', ')}]${why.length ? ' => ' + [...new Set(why)].join('; ') : ''}` };
  },
  g(m) {
    if (m.noSection) return noSec;
    if (!m.cards.length) return { ok: false, say: 'no cards to read, so nothing can be said to be absent' };
    // what a person SEES (innerText) must hold none of the phrases. A phrase only in a title / aria-label is a tooltip: CONTRACT C lets the state dot carry its one state word there
    // ("To approve", "Approved", "Words to confirm"...), so "TO APPROVE" as the dot's tooltip is noted, never failed; any other phrase in an attribute fails.
    const hits = [], notes = [];
    for (const [name, re] of FORBIDDEN) {
      if (m.cards.some(c => re.test(c.text))) hits.push(name);
      else if (re.test(m.sectionText)) hits.push(name + ' (in the section, outside the cards)');
      else if (m.cards.some(c => re.test(c.attrs))) (name === 'TO APPROVE' ? notes : hits).push(name === 'TO APPROVE' ? 'the state dot\'s tooltip says "To approve" (allowed by CONTRACT C: a title, not a pill)' : name + ' (in a title / aria-label)');
    }
    return { ok: hits.length === 0, say: (hits.length ? 'still printed: ' + hits.map(h => '"' + h + '"').join(', ') : `none of the ${FORBIDDEN.length} phrases is visible in ${m.cards.length} cards (${m.cards.reduce((n, c) => n + c.text.length, 0)} characters read)`) + (notes.length ? '; note: ' + notes.join('; ') : '') };
  },
  h(m) {
    if (m.noSection) return noSec;
    if (!m.cards.length) return { ok: false, say: 'no cards' };
    const hs = m.cards.map(c => c.h), mx = Math.max(...hs);
    return { ok: mx <= T.CARD_MAX_H + 0.5, say: `card heights [${hs.map(Math.round).join(', ')}], limit ${T.CARD_MAX_H}px` };
  },
  /** nothing inside a card spills sideways out of it (a 122px card at 1024 is cramped, not broken) */
  j(m) {
    if (m.noSection) return noSec;
    const bad = m.cards.map((c, i) => c.spill.length ? `card ${i + 1}: ${c.spill.join(', ')}` : '').filter(Boolean);
    return { ok: bad.length === 0, say: bad.length ? 'spills out of its card: ' + bad.join('; ') : `nothing spills out of ${m.cards.length} card(s)` };
  },
  /** one and two pieces (m1, m2) against the three-piece order (m3): the same card width, at most a third of the grid */
  x(m1, m2, m3) {
    if (m1.noSection || m2.noSection || m3.noSection) return noSec;
    const w3 = m3.cards.length ? Math.max(...m3.cards.map(c => c.w)) : 0, bad = [];
    const say = [[m1, 1], [m2, 2]].map(([m, n]) => { const w = m.cards.length ? Math.max(...m.cards.map(c => c.w)) : 0, third = m.grid ? (m.grid.inner - 2 * m.grid.colGap) / 3 : 0;
      if (m.cards.length !== n) bad.push(`${m.cards.length} cards for ${n} piece(s)`); if (!near(w, w3, 2)) bad.push(`${n} piece(s): card ${px(w)} wide, the 3-piece order's ${px(w3)}`); if (m.grid && w > third + 1) bad.push(`${n} piece(s): card ${px(w)} is wider than a third of the grid (${px(third)})`);
      return `${n} piece(s): card ${px(w)}`; });
    return { ok: bad.length === 0, say: `${say.join(', ')}; 3 pieces: ${px(w3)}${bad.length ? ' => ' + bad.join('; ') : ''}` };
  },
  /** the earring pair (m, with `m.earringNever` from the seed): no card for it, no Back slot anywhere for it */
  i(m, earring) {
    if (m.noSection) return { ok: true, say: 'no section (the order has one engraved charm and no card could be drawn)' };
    const ear = m.cards.filter(c => /mask|stud|earring|\bleft\b|\bright\b/i.test(c.text));
    return { ok: ear.length === 0, say: `${m.cards.length} card(s) for a charm and an earring pair${earring && earring.never ? '' : ' (the intake did NOT mark the pair as never-engraved)'}; cards that look like the pair's: ${ear.length}` };
  },
};
module.exports = { SEL, T, FORBIDDEN, GATE, measureInPage, judge, rowsOf };

/* ═══════════════════════════ the synthetic page: proves the checker can pass and can fail ═══════════════════════════ */
// A tiny stand-in of the order window with the SAME ids and classes the page uses (#orderWin .owMain .owInfoMain #owMeta #owEngSec #owEng .engCard). `o` switches one fault on.
function synthetic(o) {
  o = o || {};
  const card = i => `<div><article class="engCard"><b>Piece ${i + 1}</b><div class="th"><i></i><i></i></div><span>${o.badText && i === 0 ? 'Waiting for your approval · 97% sure' : 'Name ' + (i + 1)}</span></article></div>`;
  const cards = Array.from({ length: o.count || 3 }, (_, i) => card(i)).join('');
  const sec = `<section id="owEngSec">${o.noTitle ? '' : '<h4 class="fLabel owEngTitle">Engraving</h4>'}<div id="owEng" class="owEng owEngGrid">${cards}</div></section>`;
  return `<!doctype html><html><head><meta charset="utf-8"><style>
    *{box-sizing:border-box}html,body{margin:0;height:100%}body{font:14px sans-serif}
    #orderWin{position:fixed;inset:0;display:grid;grid-template-rows:60px 1fr;background:#fff;border:0;padding:0;margin:0;width:100vw;height:100vh;max-width:none;max-height:none}
    .owHead{height:60px;border-bottom:1px solid #ddd}
    .owVInfo{position:relative;overflow:hidden}.owMain{position:absolute;inset:0;overflow:auto;padding:18px 22px 28px 18px;display:grid;grid-template-columns:196px minmax(0,1fr);gap:22px;align-content:start}
    .owInfoMain{min-width:0}
    #owMeta{height:96px;display:block}.owfBar{height:50px;border:1px solid #bbb;border-radius:20px}
    .pad{height:${o.padAbove || 0}px}
    #owEngSec{min-width:0;margin-top:${o.gapTop != null ? o.gapTop : 12}px;padding-top:8px;${o.noDivider ? '' : 'border-top:1px solid #c9c2b3;'}${o.wideSection ? 'width:2200px;' : ''}}
    #owEngSec h4{margin:0 0 8px;font:700 10px sans-serif;text-transform:uppercase;${o.titleRight ? 'text-align:right;' : ''}}
    #owEng{display:grid;gap:10px;grid-template-columns:repeat(${o.narrow ? 6 : 3},minmax(0,1fr));${o.hscroll ? 'width:' + o.hscroll + 'px;' : ''}}
    .engCard{border:1px solid #ccc;border-radius:12px;padding:10px;height:${o.tall || 150}px}
    ${o.overlap ? '#owEng>div:nth-child(n+4) .engCard{margin-top:-60px}' : ''}
    .engCard .th{display:flex;gap:8px;margin:6px 0}.engCard .th i{flex:1;height:60px;background:#eee;border-radius:6px}
    ${o.cover ? '.cov{position:fixed;left:0;right:0;bottom:0;height:520px;background:#fffd;z-index:5}' : ''}
  </style></head><body>
  <dialog id="orderWin" open><div class="owHead"></div><section class="owVInfo"><div class="owMain">
    <div class="owPics" style="height:300px">pictures${o.inPics ? sec : ''}</div>
    <div class="owInfoMain"><div class="pad"></div><div id="owMeta"><div class="owfBar"></div></div>${o.inPics ? '' : sec}</div></div></section>${o.cover ? '<div class="cov"></div>' : ''}</dialog></body></html>`;
}
const MUTANTS = [
  { name: 'GOOD layout', o: {}, expect: 'all pass' },
  { name: 'section 200px below the chip strip', o: { gapTop: 200 }, expect: 'a' },
  { name: 'section still in the left pictures column', o: { inPics: 1 }, expect: 'a' },
  { name: 'no "Engraving" title', o: { noTitle: 1 }, expect: 'b' },
  { name: 'title on the right', o: { titleRight: 1 }, expect: 'b' },
  { name: 'no divider line', o: { noDivider: 1 }, expect: 'b' },
  { name: 'cards too narrow (6 across at 1280 is 100px each)', o: { narrow: 1 }, expect: 'c' },
  { name: 'first row below the fold', o: { padAbove: 400 }, expect: 'd' },
  { name: 'a bar covers the cards', o: { cover: 1 }, expect: 'd' },
  { name: 'a sideways scrollbar', o: { hscroll: 2400 }, expect: 'e' },
  { name: 'a second row overlaps the first', o: { overlap: 1 }, expect: 'f' },
  { name: 'a deleted phrase in a card', o: { badText: 1 }, expect: 'g' },
  { name: 'a card 260px tall', o: { tall: 260 }, expect: 'h' },
];
async function selfTest(browser) {
  const ctx = await browser.newContext({ viewport: { width: 1280, height: 720 } }), page = await ctx.newPage(), results = [];
  const run = async o => { await page.setContent(synthetic(o)); return page.evaluate(measureInPage, cfgOf()); };
  for (const mu of MUTANTS) {
    const m = await run(mu.o), m6 = await run(Object.assign({}, mu.o, { count: 6 })), x = { cardMin: T.CARD_MIN(1280) };
    const res = { a: judge.a(m), b: judge.b(m), c: judge.c(m, x), d: judge.d(m), e: judge.e(m), f: judge.f(m6), g: judge.g(m), h: judge.h(m) };
    const failing = Object.keys(res).filter(k => !res[k].ok);
    results.push({ name: mu.name, expect: mu.expect, failing, ok: mu.expect === 'all pass' ? failing.length === 0 : failing.includes(mu.expect), say: failing.map(k => k + ': ' + res[k].say.slice(0, 120)).join(' | ') });
  }
  await ctx.close(); return results;
}

/* ═══════════════════════════ the fixtures: orders that need engraving ═══════════════════════════ */
// Neutral names only. The order ships in 5 days, metal gold. `mode`: both | back | front. Words: [front, back].
const NOW = Math.floor(Date.now() / 1000), SHIP = NOW + 5 * 86400;
const line = (rid, n, sku, title, extra) => Object.assign({ transactionId: rid + n, listingId: '18' + rid.slice(-5) + n, sku, title, quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal choice', value: 'Gold' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' }, extra || {});
const order = (rid, buyer, lines) => ({ receiptId: rid, orderNumber: rid, createTs: NOW - 86400, updateTs: NOW - 3600, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
const CHARMS = [   // six different charms (SKU, listing title)
  ['LEAF_CHARM', 'Pressed leaf charm necklace in 14k gold fill'], ['ASTER_FLOWER', 'Aster flower charm necklace, birth month flower'], ['MINI_0445', 'Mini tag initial charm necklace, personalized'],
  ['PLAIN_TAG', 'Plain tag charm necklace in 14k gold fill'], ['COMEDY 89578', 'Comedy and tragedy theatre mask charm necklace'], ['DUCK 38090', 'Little duck charm necklace in 14k gold fill'],
];
const MODES = ['both', 'back', 'front', 'both', 'back', 'front'];
const WORDS = [['Name One', 'Name Two'], ['', 'Name Three'], ['Name Four', ''], ['Name Five', 'Name Six'], ['', 'Name Seven'], ['Name Eight', '']];
function fixture() {
  const said = k => [WORDS[k][0] && 'front - ' + WORDS[k][0], WORDS[k][1] && 'back - ' + WORDS[k][1]].filter(Boolean).join(' / ');   // (what the buyer typed in the personalisation box: one line, as in the real orders; a longer text pushes the chip strip down by about 20px a line)
  const mk = (rid, buyer, idx) => { const o = order(rid, buyer, idx.map((k, i) => line(rid, i + 1, CHARMS[k][0], CHARMS[k][1], { personalization: [said(k)] }))); o.setup = idx.map(k => ({ mode: MODES[k], front: WORDS[k][0], back: WORDS[k][1] })); return o; };
  const ear = order('4200000105', 'Name Five', [line('4200000105', 1, CHARMS[0][0], CHARMS[0][1]), line('4200000105', 2, 'MASKS HAPPY/SAD', 'Theatre Happy Sad Drama Mask Earrings in 14k Gold Fill', { variations: [{ name: 'Metal choice', value: 'Gold' }, { name: 'Style', value: 'Stud earrings' }] })]);
  ear.setup = [{ mode: 'both', front: 'Name One', back: 'Name Two' }, null];   // (the earring line is left to the real intake: it marks it never-engraved)
  return {
    three: mk('4200000101', 'Name One', [0, 1, 2]),             // three different charms: front + back, back only, front only
    six: mk('4200000102', 'Name Two', [0, 1, 2, 3, 4, 5]),      // six pieces: the row wraps
    one: mk('4200000103', 'Name Three', [0]),                   // one piece
    two: mk('4200000104', 'Name Four', [0, 2]),                 // two lines, two pieces
    ear,                                                        // one charm and an earring pair
  };
}

/* ═══════════════════════════ the run ═══════════════════════════ */
const cfgOf = () => { const { titleText, ...sels } = SEL; return { SEL: sels, titleSrc: titleText.source, titleFlags: titleText.flags }; };

function findPlaywrightDir() {
  const cands = [process.env.PW_DIR, path.join(root, 'node_modules'), '/opt/node22/lib/node_modules/playwright/node_modules', '/opt/node22/lib/node_modules', '/usr/lib/node_modules', '/usr/local/lib/node_modules'].filter(Boolean);
  return cands.find(d => { try { return fs.existsSync(path.join(d, 'playwright-core')); } catch (_) { return false; } }) || null;
}
function findChrome() {
  const cands = [process.env.CHROMIUM, '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', '/opt/pw-browsers/chromium'].filter(Boolean);
  return cands.find(c => { try { return fs.existsSync(c); } catch (_) { return false; } }) || cands[0];
}

/** put an order that needs engraving into the page as pulled rows (the page's own intake reads each line), give each charm line the engraving the buyer asked for
 *  (spec.engraving, as the intake writes it) and its engraving jobs as Engrave holds them (words, face, in review) */
async function seedOrder(page, o) {
  return page.evaluate(async ({ order, setup }) => {
    const T0 = Date.now() - 3 * 3600e3, rows = [];
    await Orders.loadMaps(true);
    order.lines.forEach(line => { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: T0, spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [key + '_1'], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); rows.push(row); });
    for (const l of order.lines) B.master.entries.set(l.sku, { sku: l.sku, updatedAt: 1 });   // (the charms are in the master: the order is an ordinary one, not an unknown SKU)
    Orders.interpretAll(); await Orders.loadMaps(true); Orders.interpretAll();
    const art = text => { const c = document.createElement('canvas'); c.width = 300; c.height = 300; const x = c.getContext('2d'); x.fillStyle = '#fff'; x.fillRect(0, 0, 300, 300); x.fillStyle = '#111'; x.font = '36px sans-serif'; x.textAlign = 'center'; String(text || '').split('\n').forEach((t, i) => x.fillText(t, 150, 140 + i * 44)); return c; };
    if (Engrave.renderBack) Engrave.renderBack = job => art(job.text || '');
    if (Engrave.renderFront) Engrave.renderFront = job => art(job.text || '');
    Engrave.render = () => {};   // (the Engrave tab itself is not under test)
    const done = [];
    rows.forEach((row, i) => {
      const s = setup[i]; const never = !!(row.spec && row.spec.engraveNever);
      if (!s) { done.push({ key: row.key, never, form: row.spec && row.spec.form || '' }); return; }
      const face = t => (t ? { text: t, lines: [t] } : undefined);
      row.spec.engraveCandidate = true;
      row.spec.engraving = { v: 1, mode: s.mode, front: face(s.front), back: face(s.back), source: 'note', textSource: 'labelled', confidence: 1, needsPerson: false };
      const jobs = Engrave.ensureJobs(row) || [];
      for (const j of jobs) { const f = Engrave.faceOf ? Engrave.faceOf(j) : (j.face || 'back'), words = (f === 'front' ? s.front : s.back) || ''; Object.assign(j, { state: 'review', text: words, lines: words.split('\n'), fit: { size: 10, capMm: 2, centre: [0, 0], angle: 0, weight: 'Regular' }, view: {}, verify: { geometry: { ok: true } }, source: 'note', confidence: 1 }); }
      row.engrave = Object.assign({ needed: true, approved: false }, row.engrave || {}, { state: 'review', text: (jobs[0] && jobs[0].text) || '' });
      done.push({ key: row.key, mode: s.mode, jobs: jobs.map(j => j.key.replace(/^.*?_\d+/, '')), never });
    });
    return done;
  }, { order: o, setup: o.setup });
}

async function openOrder(page, rid) {
  await page.evaluate(() => { const d = document.getElementById('orderWin'); if (d && d.open) d.dispatchEvent(new Event('cancel', { cancelable: true })); }).catch(() => {});
  await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 8000 }).catch(() => {});
  await page.evaluate(r => OrderWin.openOrder(r), rid);
  const settled = () => page.waitForFunction(() => { const d = document.getElementById('orderWin'); return d.open && document.querySelector('#owMeta .owfBar') && !d.getAnimations({ subtree: true }).some(a => a.playState === 'running' && a.effect && a.effect.getTiming().iterations !== Infinity); }, null, { timeout: 20000 });
  await settled();
  await page.waitForTimeout(700);   // (the engraving cards follow Engrave's jobs and the timeline feed a moment later)
  await settled().catch(() => {});
  await page.evaluate(() => { document.querySelectorAll('#orderWin *').forEach(n => { if (n.scrollTop) n.scrollTop = 0; }); });
}

async function main() {
  const pwDir = findPlaywrightDir(); let chromium;
  if (pwDir) try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) {}
  if (!chromium) { console.log('  - no playwright-core: the browser checks were not run (set PW_DIR=<dir holding playwright-core>)'); return 0; }
  process.env.PW_DIR = pwDir; process.env.CHROMIUM = findChrome();
  const only = process.env.NOSCROLL_ONLY ? process.env.NOSCROLL_ONLY.split(',').map(s => s.split('x').map(Number)) : null;
  const sizes = only || SIZES, shots = process.env.SHOTS || '', debug = !!process.env.NOSCROLL_DEBUG;
  if (shots) fs.mkdirSync(shots, { recursive: true });
  let failed = 0;
  const say = (ok, line) => console.log((ok === null ? '  info ' : ok ? '  PASS ' : '  FAIL ') + line);

  // ── 0 · the checker on a synthetic page ──
  const b0 = await chromium.launch({ executablePath: process.env.CHROMIUM, args: ['--no-sandbox'] });
  try {
    for (const r of await selfTest(b0)) {
      say(r.ok, `checker self-test, ${r.name}: ${r.expect === 'all pass' ? (r.ok ? 'every check passes' : 'FAILS: ' + r.say) : (r.ok ? `check (${r.expect}) fails as it must` + (r.failing.length > 1 ? ` (also ${r.failing.filter(k => k !== r.expect).join(',')})` : '') : `check (${r.expect}) did NOT fail; failing: ${r.failing.join(',') || 'none'}`)}`);
      if (!r.ok) failed++;
    }
  } finally { await b0.close(); }

  // ── 1 · the real order window ──
  const W = require('./_orderFactsWorld.cjs');
  const fx = fixture();
  const w = await W.world({ facts: true, reducedMotion: 'reduce', viewport: { width: sizes[0][0], height: sizes[0][1] } });
  if (!w) { console.log('  - the order world could not start'); return 1; }
  const { page } = w; const table = {}, numbers = []; let earring = null;
  try {
    await page.waitForFunction(() => window.Engrave && window.OrderEngraving && window.CNEngravingSeals && window.OrderFacts, null, { timeout: 60000 });
    const seeded = {};
    for (const k of ['three', 'six', 'one', 'two', 'ear']) seeded[k] = await seedOrder(page, fx[k]);
    earring = seeded.ear.find(x => !x.mode) || null;
    if (debug) console.log('  seeded: ' + JSON.stringify(seeded));
    for (const [vw, vh] of sizes) {
      const size = `${vw}x${vh}`; await page.setViewportSize({ width: vw, height: vh }); const t = table[size] = {}, x = { cardMin: T.CARD_MIN(vw) };
      const get = async o => { await openOrder(page, o.receiptId); return page.evaluate(measureInPage, cfgOf()); };
      const m3 = await get(fx.three);
      if (debug) console.log(JSON.stringify(m3, null, 1));
      if (shots) { const s = m3.section; const clip = s ? { x: Math.max(0, s.l - 8), y: Math.max(0, s.t - 8), width: Math.min(vw, s.r + 8) - Math.max(0, s.l - 8), height: Math.min(vh, s.b + 8) - Math.max(0, s.t - 8) } : null;
        await page.screenshot({ path: path.join(shots, `NOSCROLL-${size}.png`), ...(clip && clip.width > 20 && clip.height > 20 ? { clip } : {}) }); await page.screenshot({ path: path.join(shots, `NOSCROLL-${size}-window.png`) }); }
      const m2 = await get(fx.two), m6 = await get(fx.six), m1 = await get(fx.one), me = await get(fx.ear);
      if (debug) console.log(JSON.stringify(m6.cards.map(c => [c.l, c.t, c.w, c.h])));
      const r = { a: judge.a(m3), b: judge.b(m3), c: judge.c(m3, x), d: judge.d(m3), d2: judge.d(m2), e: judge.e(m3), f: judge.f(m6), g: judge.g(m3), h: judge.h(m3), d6: judge.d(m6), x: judge.x(m1, m2, m3), i: judge.i(me, earring), j: judge.j(m3) };
      r.c6 = judge.c(m6, x); r.f.ok = r.f.ok && r.c6.ok; if (!r.c6.ok) r.f.say += ' | first row of 6: ' + r.c6.say;
      for (const k of Object.keys(r)) t[k] = r[k].ok;
      const fb = m3.cards.length ? Math.max(...firstRow(m3).map(c => c.b)) : null, fb6 = m6.cards.length ? Math.max(...firstRow(m6).map(c => c.b)) : null;
      const free = m => m.strip ? px(m.visibleBottom - m.strip.b) : '?', fo = m3.strip ? 45 : 0;
      numbers.push(`${size}: free space between the chip strip and the window's foot: 1 piece ${free(m1)}, 2 lines ${free(m2)}, 3 lines ${free(m3)}, 6 lines ${free(m6)} (the section's own overhead is about 45px)`);
      numbers.push(`${size}: centre column ${m3.column ? px(m3.column.w) : '?'} wide; section ${m3.section ? px(m3.section.w) + ' at top ' + px(m3.section.t) : 'none'}; chip strip bottom ${m3.strip ? px(m3.strip.b) : '?'}; first row bottom: 3 pieces ${fb == null ? '-' : px(fb)}, 2 lines ${m2.cards.length ? px(Math.max(...firstRow(m2).map(c => c.b))) : '-'}, 6 pieces ${fb6 == null ? '-' : px(fb6)}; the window shows down to ${px(m3.visibleBottom)}; card widths (3 pieces) [${m3.cards.map(c => Math.round(c.w)).join(', ')}], heights [${m3.cards.map(c => Math.round(c.h)).join(', ')}]`);
      console.log(`\n  ── ${size} ──`);
      for (const k of ['a', 'b', 'c', 'd', 'd2', 'e', 'f', 'g', 'h', 'd6', 'x', 'i', 'j']) { if (k === 'i' || k === 'j') { say(null, `${size} (${k}) ${LABEL[k]}: ${r[k].say}`); continue; } const hard = gated(size, k); say(hard ? r[k].ok : null, `${size} (${k}) ${LABEL[k]}${hard ? '' : ` [printed only: ${r[k].ok ? 'would pass' : 'would FAIL'}]`}: ${r[k].say}`); }
    }
    if (!w.errors.length) say(true, 'no script error on the page throughout'); else { say(false, 'script errors: ' + w.errors.slice(0, 3).join(' | ')); failed++; }
  } finally { await w.close(); }

  // ── the table ──
  const sizesDone = Object.keys(table);
  console.log('\n  CHECK TABLE   (PASS / FAIL gating; pass / fail printed only)');
  console.log('  ' + 'check'.padEnd(66) + sizesDone.map(s => s.padEnd(11)).join(''));
  for (const k of ['a', 'b', 'c', 'd', 'd2', 'd6', 'e', 'f', 'g', 'h', 'x']) {
    const cells = sizesDone.map(s => { const hard = gated(s, k), ok = table[s][k]; if (hard && !ok) failed++; return (hard ? (ok ? 'PASS' : 'FAIL') : (ok ? 'pass' : 'fail')).padEnd(11); });
    console.log('  ' + `(${k}) ${LABEL[k]}`.padEnd(66) + cells.join(''));
  }
  console.log('\n  MEASURED');
  for (const n of numbers) console.log('  ' + n);
  console.log(`\n  card minimum ${sizesDone.map(s => s + ': ' + T.CARD_MIN(+s.split('x')[0]) + 'px').join(', ')}; safe bottom margin ${T.SAFE_BOTTOM}px; card height limit ${T.CARD_MAX_H}px`);
  console.log('  not proved by this run: real drawings, real fonts and real Engrave jobs (the fake site has none: thumbnails are stand-ins or empty), a live order, any browser but headless Chromium');
  console.log(failed ? `\nengrave-section-noscroll: ${failed} gating failure(s)` : '\nengrave-section-noscroll: every gating check passes');
  return failed ? 1 : 0;
}
if (require.main === module) main().then(c => process.exit(c)).catch(e => { console.error(e); process.exit(1); });
