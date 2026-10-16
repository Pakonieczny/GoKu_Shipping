// EGROW (Paul, 15 Oct 2026, a screenshot of two Engrave rows of the set-of-2-bars listing 532401548): "This UI is broken. Please upgrade the position and placement to always fit within the bounds of each
//   card. Also be clever in how you display and arrange this information. It already looks too crowded and squished. Take a more minimalist approach."
// The second bar's line of a set hung out of the bottom of its card. This test pins the answer: in the Engrave tab's one list, with every piece still to approve and again with every piece approved (NODECIDED, 16 Oct 2026: no Decided list; an approved piece is a row of the same list), at 1440, 900 and 390 px wide, for a plain necklace (1 job),
// a set of 2 bars (listing 532401548, Angelina, 16" and 18"), an earring pair (Left + Right), 3 discs, 6 discs, a front + back line, a 120-character word and an unbroken 60-character word:
//   1  every element in a row lies inside the row's box (getBoundingClientRect of each descendant against the row), nothing is cut off or hangs out of it
//   2  no row overlaps the next, and no row, its list or the page scrolls sideways
//   3  the row's height follows its content (no empty room under the last line beyond the row's own padding; a min-height is only ever a floor)
//   4  one state per job, on that job's own line; a long word is ellipsised on ONE line with its whole text in the title
// The REAL page over the repo's offline fake site with the real drawings (tests/charm-nest/_engraveRealWorld.cjs): nothing live, no Etsy, no paid AI, made-up names.
//   /tmp/cn-run.sh 240 tests/charm-nest/engrave-row-fit.cjs      (SHOTS=<dir> saves a PNG of each list at each width)
// What a pass does NOT prove: the live page, the live fonts and drawings (the bars are drawn here, BAR_1548 / SKINNY_0191 are not in the repo), a browser other than headless Chromium. The earring pair is made
//   from a two-disc order whose two jobs are given the slots L and R (the shop never engraves earrings now, ZODNOBACK, but the row for the Left and the Right is still drawn for ears decided before).
'use strict';
const R = require('./_engraveRealWorld.cjs');
const MMPT = 72 / 25.4;
/** A PDF-compatible .ai of a rounded bar with a hole at each end on the CUT layer (as discAi() makes a disc): wMm x hMm. Nothing in it is a customer's. */
function barAi(wMm, hMm, title) {
  const f = n => (Math.round(n * 1000) / 1000).toString();
  const W = wMm * MMPT, H = hMm * MMPT, X = 10, Y = 10, x0 = X, x1 = X + W, y0 = Y, y1 = Y + H, cy = Y + H / 2, rr = H * 0.32, c = 0.5522847498 * rr;
  const circle = (cx, cy2, r) => { const k = 0.5522847498 * r; return `${f(cx + r)} ${f(cy2)} m ${f(cx + r)} ${f(cy2 + k)} ${f(cx + k)} ${f(cy2 + r)} ${f(cx)} ${f(cy2 + r)} c ${f(cx - k)} ${f(cy2 + r)} ${f(cx - r)} ${f(cy2 + k)} ${f(cx - r)} ${f(cy2)} c ${f(cx - r)} ${f(cy2 - k)} ${f(cx - k)} ${f(cy2 - r)} ${f(cx)} ${f(cy2 - r)} c ${f(cx + k)} ${f(cy2 - r)} ${f(cx + r)} ${f(cy2 - k)} ${f(cx + r)} ${f(cy2)} c h`; };
  const bar = `${f(x0 + rr)} ${f(y0)} m ${f(x1 - rr)} ${f(y0)} l ${f(x1 - rr + c)} ${f(y0)} ${f(x1)} ${f(y0 + rr - c)} ${f(x1)} ${f(y0 + rr)} c ${f(x1)} ${f(y1 - rr)} l ${f(x1)} ${f(y1 - rr + c)} ${f(x1 - rr + c)} ${f(y1)} ${f(x1 - rr)} ${f(y1)} c ${f(x0 + rr)} ${f(y1)} l ${f(x0 + rr - c)} ${f(y1)} ${f(x0)} ${f(y1 - rr + c)} ${f(x0)} ${f(y1 - rr)} c ${f(x0)} ${f(y0 + rr)} l ${f(x0)} ${f(y0 + rr - c)} ${f(x0 + rr - c)} ${f(y0)} ${f(x0 + rr)} ${f(y0)} c h`;
  const hole = H * 0.13, hx = H * 0.42;
  const content = ['/OC /ocCut BDC', `q 0 0 0 RG 0.283 w ${bar} S Q`, `q 0 0 0 RG 0.283 w ${circle(x0 + hx, cy, hole)} S Q`, `q 0 0 0 RG 0.283 w ${circle(x1 - hx, cy, hole)} S Q`, 'EMC'].join('\n') + '\n';
  const objs = ['<< /Type /Pages /Kids [ 4 0 R ] /Count 1 >>',
    '<< /Type /Catalog /Pages 1 0 R /OCProperties << /OCGs [ 5 0 R 6 0 R 7 0 R ] /D << /Order [ 5 0 R 6 0 R 7 0 R ] /ON [ 5 0 R 6 0 R 7 0 R ] /BaseState /ON >> >> >>',
    `<< /Producer (SETBAR-B test drawing) /Title (${title}) >>`,
    `<< /Type /Page /Parent 1 0 R /Resources << /Properties << /ocEng 5 0 R /ocHatch 6 0 R /ocCut 7 0 R >> >> /MediaBox [ 0 0 ${Math.ceil(W + 20)} ${Math.ceil(H + 20)} ] /Contents 8 0 R >>`,
    '<< /Type /OCG /Name (ENGRAVE) >>', '<< /Type /OCG /Name (HATCH) >>', '<< /Type /OCG /Name (CUT) >>', `<< /Length ${Buffer.byteLength(content)} >>\nstream\n${content}endstream`];
  let out = '%PDF-1.7\n'; const at = [];
  objs.forEach((o, i) => { at.push(Buffer.byteLength(out)); out += `${i + 1} 0 obj\n${o}\nendobj\n`; });
  const xs = Buffer.byteLength(out);
  out += `xref\n0 ${objs.length + 1}\n0000000000 65535 f \n` + at.map(n => String(n).padStart(10, '0') + ' 00000 n \n').join('') + `trailer\n<< /Size ${objs.length + 1} /Root 2 0 R /Info 3 0 R >>\nstartxref\n${xs}\n%%EOF\n`;
  return Buffer.from(out, 'latin1');
}
const BARS = { BAR_1548: { w: 25.9, h: 4.9 }, SKINNY_0191: { w: 25.9, h: 3.9 }, BAR_WIDE_9999: { w: 50, h: 7.2 } };
function seedBars(st) {
  for (const [sku, d] of Object.entries(BARS)) {
    const wPt = d.w * MMPT, hPt = d.h * MMPT, aiPath = 'charmnest/master/' + sku + '.ai';
    st.put('Charm_Master_Index', sku, { sku, widthPt: wPt, heightPt: hPt, areaPt2: wPt * hPt * 0.95, members: 3, holes: 2, engravable: true, engravableBy: 'index', upAngle: 90, upSource: 'drawn', charmHash: sku.slice(0, 4).toLowerCase() + '1234', backKeepOut: [], aiPath, thumbPath: null, blocked: null, sizes: null, conflict: null });
    st.blobs.set(aiPath, { buf: barAi(d.w, d.h, sku), generation: 1, meta: { contentType: 'application/illustrator', metadata: { firebaseStorageDownloadTokens: 't' } } });
  }
}

const V = (name, value) => ({ name, value });
const COORDS = `33°0'45" N 96°45'53" W`;
const LONG120 = ('Ginger and Catalina together forever ' + 'with all my love and every kind word I can think of ').slice(0, 120).trim().padEnd(120, 'x').slice(0, 120);
const UNBROKEN60 = 'Abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWX'.slice(0, 60);
const SET_TITLE = 'Bar Necklace SET of 2, Custom Gold Bar Set, Engraved Necklace';
const SETV = [V('Metal · #Characters?', 'GOLD·Set·ENGRAVED'), V('Necklace Length·FONT', '16"&18"·Set·ANGELINA')];
const mk = (rid, listing, base, extra) => Object.assign({}, base, { rid, listing, placed: { day: 0, h: 9, m: 10 }, ship: 5 }, extra || {});
const ORD = {
  rowBars: { rid: '4290000201', sku: 'BAR_1548', listing: '532401548', title: SET_TITLE, buyer: 'Name One', placed: { day: 0, h: 9, m: 10 }, ship: 5, variations: SETV, personalization: [`16"= 33*0'45" N 96*45'53" W 18"= Ginger`] },
  rowBars2: { rid: '4290000202', sku: 'BAR_1548', listing: '532401548', title: SET_TITLE, buyer: 'Name Two', placed: { day: 0, h: 9, m: 20 }, ship: 5, variations: SETV, personalization: [`16"= Myneve 18"= Catalina`] },
  rowOne: mk('4290000203', '1890000203', R.ORDERS.back, { buyer: 'Name Three' }),
  rowFaced: mk('4290000204', '1890000103', R.ORDERS.both, { buyer: 'Name Four' }),
  rowEars: mk('4290000205', '1890000205', R.ORDERS.discs, { buyer: 'Name Five', variations: [V('Metal choice', 'Gold'), V('Number of Discs', '2 Discs')], personalization: ['Tag 1: Ginger, Tag 2: Catalina'] }),
  rowDiscs3: mk('4290000206', '1890000206', R.ORDERS.discs, { buyer: 'Name Six', personalization: ['Tag 1: Ginger, Tag 2: Catalina, Tag 3: Anna'] }),
  rowDiscs6: mk('4290000207', '1890000207', R.ORDERS.discs, { buyer: 'Name Seven', variations: [V('Metal choice', 'Gold'), V('Number of Discs', '6 Discs'), V('Font Choice', 'Ace')], personalization: ['Tag 1: A, Tag 2: B, Tag 3: C, Tag 4: D, Tag 5: E, Tag 6: F'] }),
  rowLong: mk('4290000208', '1890000208', R.ORDERS.back, { buyer: 'Name Eight' }),
  rowUnbroken: mk('4290000209', '1890000209', R.ORDERS.back, { buyer: 'Name Nine' })
};
Object.assign(R.ORDERS, ORD);
Object.assign(R.EXPECT_JOBS, { rowBars: 2, rowBars2: 2, rowOne: 1, rowFaced: 2, rowEars: 2, rowDiscs3: 3, rowDiscs6: 6, rowLong: 1, rowUnbroken: 1 });
const NAMES = Object.keys(ORD);
const WIDTHS = [1440, 1100, 900, 390];   // (390: the rail is folded, as a person on a phone has it; the others keep it open, so the list is narrower than the window)

const fails = [], note = (ok, what, more) => { console.log((ok ? '  PASS ' : '  FAIL ') + what + (more ? '  [' + more + ']' : '')); if (!ok) fails.push(what); };

/** In the page: what every row of a list measures. rowSel: the rows ('#egQueue .placementRow'). */
const MEASURE = rowSel => {
  const rows = [...document.querySelectorAll(rowSel)].filter(r => r.offsetParent !== null);
  const vis = e => { const cs = getComputedStyle(e); if (cs.display === 'none' || cs.visibility === 'hidden') return false; const r = e.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
  // (what is not measured: a hidden thing, a picture's own zoomed content: it is clipped by its picture box by design)
  const skip = (e, row) => { for (let p = e; p && p !== row; p = p.parentElement) { if (p.hidden || p.classList.contains('srOnly') || p.classList.contains('thumbLoading')) return true; if (p !== e && p.classList.contains('placementThumb')) return true; } return false; };
  const name = e => (e.className && e.className.baseVal === undefined ? '.' + String(e.className).trim().split(/\s+/).join('.') : e.tagName.toLowerCase());
  const out = rows.map(row => {
    const rr = row.getBoundingClientRect(), cs = getComputedStyle(row), pad = parseFloat(cs.paddingBottom) || 0, bw = parseFloat(cs.borderBottomWidth) || 0;
    const outside = []; let lastBottom = rr.top, lastEl = '';
    for (const e of row.querySelectorAll('*')) {
      if (!vis(e) || skip(e, row)) continue;
      const r = e.getBoundingClientRect();
      if (r.bottom > lastBottom && !e.closest('.seal')) { lastBottom = r.bottom; lastEl = name(e); }   // (a stamp is turned a little: its box is wider than its place, which is not what sets the row's height)
      if (r.left < rr.left - .6 || r.right > rr.right + .6 || r.top < rr.top - .6 || r.bottom > rr.bottom + .6) outside.push(name(e) + ' ' + [r.left - rr.left, r.top - rr.top, r.right - rr.right, r.bottom - rr.bottom].map(n => Math.round(n)).join('/'));
    }
    const STATE = '[data-stage]';
    const ears = [...row.querySelectorAll('.egEar')].filter(vis).map(ear => {
      const er = ear.getBoundingClientRect(), chip = ear.querySelector('.egPiece'), w = ear.querySelector('.w'), st = ear.querySelector(STATE);
      const cr = chip && chip.getBoundingClientRect(), wr = w && w.getBoundingClientRect(), sr = st && vis(st) ? st.getBoundingClientRect() : null;
      const mid = r => (r.top + r.bottom) / 2, near = (a, b) => !!a && !!b && Math.abs(mid(a) - mid(b)) < 12;
      return { top: er.top, bottom: er.bottom, chip: chip ? chip.textContent : '', words: w ? w.textContent : '', wTitle: w ? w.getAttribute('title') : null, wH: wr ? wr.height : 0, wLine: w ? parseFloat(getComputedStyle(w).lineHeight) : 0, cut: w ? w.scrollWidth > w.clientWidth + 1 : false,
        stage: st ? st.textContent : null, stageIn: !!sr && sr.top >= er.top - .6 && sr.bottom <= er.bottom + .6, stageOnLine: near(sr, cr) || near(sr, wr), stageS: st ? st.dataset.s || '' : '' };
    });
    const sum = row.querySelector('.purchaseSummary'), sr2 = sum && sum.getBoundingClientRect();
    const single = !row.querySelector('.egEars') ? row.querySelector('.engravingIdentity > .w') : null;
    return { rid: row.dataset.rid, top: rr.top, bottom: rr.bottom, h: rr.height, outside, lastEl, empty: Math.round(rr.bottom - bw - pad - lastBottom), sw: row.scrollWidth - row.clientWidth, ears, stages: row.querySelectorAll(STATE).length,
      minH: parseFloat(cs.minHeight) || 0, fixedH: row.style.height || '', sumLines: sr2 ? Math.round(sr2.height / (parseFloat(getComputedStyle(sum).lineHeight) || 18)) : 0, sumTitle: sum ? sum.getAttribute('title') : null, sumCut: sum ? sum.scrollWidth > sum.clientWidth + 1 : false, sumText: sum ? sum.textContent.replace(/\s+/g, ' ').trim() : '',
      single: single ? { text: single.textContent, title: single.getAttribute('title'), cut: single.scrollWidth > single.clientWidth + 1, h: single.getBoundingClientRect().height, line: parseFloat(getComputedStyle(single).lineHeight) } : null,
      thumbReset: [...row.querySelectorAll('.thumbReset')].map(b => ({ op: getComputedStyle(b).opacity, pos: getComputedStyle(b).position, h: b.getBoundingClientRect().height })), mode: !!row.querySelector('.egMode'), reopen: [...row.querySelectorAll('[data-a=reopen]')].map(b => b.textContent), seals: row.querySelectorAll('.seal').length };
  });
  const list = rows[0] && rows[0].parentElement, pane = list && list.closest('.egPane');
  return { rows: out, listSW: list ? list.scrollWidth - list.clientWidth : 0, pageSW: document.documentElement.scrollWidth - innerWidth, listTaller: list ? list.scrollHeight > list.clientHeight + 40 : false, listH: list ? list.clientHeight : 0, listSH: list ? list.scrollHeight : 0, paneOverflow: pane ? getComputedStyle(pane).overflowY : '' };
};

const gotoList = async (page, tab, q) => {
  await page.evaluate(({ q }) => { CN.setMode('engrave'); Engrave.restoreView({ list: true, chosen: true, focus: null, pin: null, q: q || '' }); Engrave.render(); }, { q });   // (the one list: `tab` only says which state the pieces are in)
  await R.settle(page, 900);
};

/** What each order's row must tell: the pieces, in order (chips), the words each carries. */
const WANT = {
  '4290000201': { jobs: 2, chips: ['LARGE BAR · 18″', 'SMALL BAR · 16″'], words: ['Ginger', COORDS], what: 'a set of 2 bars (the listing of Paul\'s screenshot)' },
  '4290000202': { jobs: 2, chips: ['LARGE BAR · 18″', 'SMALL BAR · 16″'], words: ['Catalina', 'Myneve'], what: 'a second set of 2 bars' },
  '4290000203': { jobs: 1, what: 'a plain necklace, one job' },
  '4290000204': { jobs: 2, chips: ['FRONT', 'BACK'], what: 'a line with a front and a back' },
  '4290000205': { jobs: 2, chips: ['LEFT EAR', 'RIGHT EAR'], words: ['Ginger', 'Catalina'], what: 'an earring pair (Left + Right)' },
  '4290000206': { jobs: 3, chips: ['DISC 1 of 3', 'DISC 2 of 3', 'DISC 3 of 3'], words: ['Ginger', 'Catalina', 'Anna'], what: '3 discs' },
  '4290000207': { jobs: 6, chips: [1, 2, 3, 4, 5, 6].map(n => `DISC ${n} of 6`), words: ['A', 'B', 'C', 'D', 'E', 'F'], what: '6 discs' },
  '4290000208': { jobs: 1, long: LONG120, what: 'a 120-character word' },
  '4290000209': { jobs: 1, long: UNBROKEN60, what: 'an unbroken 60-character word' }
};
const chipsOf = r => r.ears.map(e => e.chip.replace(/\s+/g, ' ').trim());

/** Every pin on one measured list. tab: 'place' (pieces to approve) | 'done' (every piece approved: the same rows, calm). */
function pin(m, tab, width, label) {
  const L = `${tab} @${width}${label ? ' ' + label : ''}`;
  note(m.rows.length >= 1, `${L}: the rows are on the page`, m.rows.length + ' rows');
  for (const r of m.rows) {
    const w = WANT[r.rid] || {}, id = `${L} · ${w.what || r.rid}`;
    note(r.outside.length === 0, `${id}: everything in the row lies inside its card`, r.outside.slice(0, 4).join(' | '));
    note(r.sw <= 0, `${id}: the row does not scroll sideways`, 'scrollWidth - clientWidth = ' + r.sw);
    note(r.fixedH === '' && r.minH <= 0, `${id}: the row has no fixed height or min-height`, `style.height "${r.fixedH}" min-height ${r.minH}`);
    note(r.empty >= -1 && r.empty <= 4, `${id}: the row's height follows its content (nothing past it, no room left under it)`, `h=${Math.round(r.h)}, ${r.empty}px between the last thing (${r.lastEl}) and the padding`);
    if ((w.jobs || 0) > 1) {
      note(r.ears.length === w.jobs, `${id}: one line for each of its ${w.jobs} pieces`, r.ears.length + ' lines');
      if (w.chips) note(JSON.stringify(chipsOf(r)) === JSON.stringify(w.chips), `${id}: each line names its piece`, JSON.stringify(chipsOf(r)));
      if (w.words) note(JSON.stringify(r.ears.map(e => e.words)) === JSON.stringify(w.words), `${id}: each line carries its own words`, JSON.stringify(r.ears.map(e => e.words)));
      note(r.stages === w.jobs && r.ears.every(e => e.stage && e.stage.length && e.stageIn && e.stageOnLine), `${id}: ONE state for each piece, on that piece's own line (never between the pieces)`, JSON.stringify(r.ears.map(e => [e.stage, e.stageIn, e.stageOnLine])) + ' of ' + r.stages);
      const gaps = r.ears.slice(1).map((e, i) => Math.round(e.top - r.ears[i].bottom));
      note(gaps.every(g => g >= 4 && Math.abs(g - gaps[0]) <= 1), `${id}: the pieces are stacked with even spacing`, JSON.stringify(gaps));
      note(r.ears.every(e => e.wH <= e.wLine * 1.2 + 1), `${id}: each piece's words are one line`, JSON.stringify(r.ears.map(e => [Math.round(e.wH), e.wLine])));
      if (tab === 'done') note(r.reopen.length === 0 && r.ears.every(e => e.stageS === 'approved' && e.stage === 'Approved'), `${id}: each piece says Approved on its own line, and there is no Reopen`, JSON.stringify(r.ears.map(e => [e.stageS, e.stage])) + ' reopen ' + r.reopen.length);
    }
    if (w.long) {
      const one = r.single;
      note(!!one && one.h <= one.line * 1.2 + 1, `${id}: the words stay on ONE line`, one && `${Math.round(one.h)} high, line ${one.line}`);
      note(!!one && (!one.cut || one.title === w.long), `${id}: when the words are cut with … their whole text is the title`, one && JSON.stringify([one.cut, one.title && one.title.length]));
      if (width <= 390) note(!!one && one.cut && one.title === w.long, `${id}: on a phone it is cut and says all of itself`, one && JSON.stringify([one.cut, one.title && one.title.length]));
    }
    note(r.sumLines <= 1 && r.sumText.length > 0 && (!r.sumCut || (r.sumTitle && r.sumTitle.length >= r.sumText.length - 8)), `${id}: what was bought is ONE muted line (cut with … only with its whole text as the title)`, JSON.stringify([r.sumLines, r.sumText, r.sumCut, r.sumTitle]));
    note(r.thumbReset.every(b => b.op === '0' || b.op === '1') && r.thumbReset.every(b => b.pos === 'absolute'), `${id}: the reload marks take no room (on the picture itself, shown on hover or focus)`, JSON.stringify(r.thumbReset));
  }
  const sorted = m.rows.slice().sort((a, b) => a.top - b.top);
  note(sorted.every((r, i) => !i || r.top >= sorted[i - 1].bottom - .6), `${L}: no row overlaps the next`, sorted.map((r, i) => i ? Math.round(r.top - sorted[i - 1].bottom) : '-').join(','));
  note(m.listSW <= 0 && m.pageSW <= 0, `${L}: neither the list nor the page scrolls sideways`, `list ${m.listSW}, page ${m.pageSW}`);
}

(async () => {
  const w = await R.world({ viewport: { width: 1440, height: 1000 } });
  if (!w) { console.log('no playwright-core: nothing run'); return; }
  const page = w.page, shots = process.env.SHOTS;
  const foldRail = async narrow => page.evaluate(narrow => { const app = document.getElementById('app'); if (app.classList.contains('railOff') !== narrow) document.getElementById('btnRail').click(); }, narrow);
  const shotRows = async (tab, width, sel, tag) => {
    if (!shots) return;
    await R.shot(w, `egrow-${tab}-${width}${tag || ''}`);
    for (const [n, rid] of [['bars', '4290000201'], ['bars2', '4290000202'], ['ears', '4290000205'], ['faced', '4290000204'], ['one', '4290000203'], ['long', '4290000208'], ['discs6', '4290000207']]) {
      const loc = page.locator(`${sel}[data-rid="${rid}"]`).first();
      if (await loc.count()) await loc.screenshot({ path: require('path').join(shots, `egrow-${tab}-${width}${tag || ''}-${n}.png`) }).catch(() => {});
    }
  };
  try {
    seedBars(w.srv.st);
    await page.evaluate(async () => { await Master.load(true); });
    const made = await R.build(w, NAMES);
    for (const n of NAMES) note(made[n] && made[n].jobs.length === R.EXPECT_JOBS[n], `${n}: ${R.EXPECT_JOBS[n]} Engrave job(s) in the real world`, made[n] && made[n].jobs.map(j => j.slot + ':' + j.state).join(' '));
    // the shapes the real build cannot make by itself: the two jobs of one order are an earring pair (slots L and R), and two long words
    await page.evaluate(({ long, unbroken }) => {
      const jobs = rid => [...Engrave.items().values()].filter(j => j.row.order.receiptId === rid || (j.row.parentRow && j.row.parentRow.order.receiptId === rid));
      const ears = jobs('4290000205').sort((a, b) => String(a.key).localeCompare(String(b.key))); ears[0].slot = 'L'; if (ears[1]) ears[1].slot = 'R';
      jobs('4290000208').forEach(j => { j.lines = [long]; });
      jobs('4290000209').forEach(j => { j.lines = [unbroken]; });
    }, { long: LONG120, unbroken: UNBROKEN60 });

    for (const [tab, sel] of [['place', '#egQueue .egPlacementList .placementRow'], ['done', '#egQueue .egPlacementList .placementRow']]) {
      if (tab === 'done') await page.evaluate(() => { for (const j of Engrave.items().values()) { j.state = 'approved'; j.approvedAt = Date.now(); j.approvedBy = 'Test'; } });
      for (const width of WIDTHS) {
        console.log(`\n── the list at ${width}px, ${tab === 'place' ? 'every piece to approve' : 'every piece approved'}`);
        await page.setViewportSize({ width, height: 1000 });
        await foldRail(width <= 480);
        await gotoList(page, tab);
        const m = await page.evaluate(`(${MEASURE})(${JSON.stringify(sel)})`);
        note(m.rows.length === 9, `${tab} @${width}: all nine orders are rows`, m.rows.length);
        note(await page.evaluate(() => document.querySelectorAll('#egDone, #egBacks, .decidedRow, .doneDetail, .egTab[data-tab="done"]').length === 0), `${tab} @${width}: no Decided list, row or tab`);
        if (tab === 'done') note(m.rows.every(r => r.seals > 0), `${tab} @${width}: every row carries its seals`, JSON.stringify(m.rows.map(r => r.seals)));
        if (tab === 'place') note(m.listTaller, `${tab} @${width}: the pane is shorter than its rows (the case that used to cut every row down to a fixed height)`, `${m.listH} < ${m.listSH}`);
        pin(m, tab, width);
        await shotRows(tab, width, sel);
      }
    }

    // Paul's own case: two rows of the bars set in a pane shorter than they are (the rows were cut back to 138px and the second bar hung out of its card)
    console.log('\n── the two bars rows alone, in a short window');
    await page.evaluate(() => { for (const j of Engrave.items().values()) { j.state = 'review'; delete j.approvedAt; delete j.approvedBy; } });
    for (const [width, height] of [[1440, 380], [1100, 380], [900, 440], [390, 520]]) {
      await page.setViewportSize({ width, height });
      await foldRail(width <= 480);
      await gotoList(page, 'place', 'BAR_1548');
      const m = await page.evaluate(`(${MEASURE})("#egQueue .placementRow")`);
      note(m.rows.length === 2 && m.rows.every(r => /^42900002(01|02)$/.test(r.rid)), `short window ${width}x${height}: the two bars orders are the rows`, m.rows.map(r => r.rid).join());
      note(m.listSH > m.listH + 4, `short window ${width}x${height}: the pane is shorter than the two rows (the case of Paul's screenshot)`, `${m.listH} / ${m.listSH}`);
      pin(m, 'place', width, `(${height} high)`);
      if (shots) { await R.shot(w, `egrow-short-${width}x${height}`); }
    }

    // seals in a Placements row (a piece reopened after it was approved keeps its seal): they sit in the row too, a little smaller than the 50px of the other lists
    console.log('\n── Placements rows that still carry their seals');
    await page.evaluate(() => { for (const j of Engrave.items().values()) if (['4290000201', '4290000204', '4290000205', '4290000203'].includes(j.row.order.receiptId)) CNEngravingSeals.add(j, 'engraveApproved', 'Test', Date.now()); });
    for (const width of [1440, 900, 390]) {
      await page.setViewportSize({ width, height: 1000 });
      await foldRail(width <= 480);
      await gotoList(page, 'place');
      const m = await page.evaluate(`(${MEASURE})("#egQueue .placementRow")`);
      const withSeals = m.rows.filter(r => r.seals > 0).map(r => r.rid).sort().join();
      note(withSeals === '4290000201,4290000203,4290000204,4290000205', `seals @${width}: the four rows with approvals show their seals`, withSeals);
      const sz = await page.evaluate(() => [...document.querySelectorAll('#egQueue .placementRow .egPlacementSeals .seal')].map(n => Math.round(n.getBoundingClientRect().width)));
      note(sz.length >= 5 && sz.every(n => n >= 24 && n <= 44), `seals @${width}: the seals are smaller than the 50px of the other lists, never squeezed to nothing`, JSON.stringify(sz));
      pin(m, 'place', width, '(with seals)');
      await shotRows('place', width, '#egQueue .placementRow', '-seals');
    }

    // an approved piece opened: its row opens the same card, which stays inside its bounds (no sideways scroll, nothing hangs out of it)
    console.log('\n── an approved piece opened into its card');
    await page.evaluate(() => { for (const j of Engrave.items().values()) { j.state = 'approved'; j.approvedAt = Date.now(); j.approvedBy = 'Test'; } });
    for (const width of [1440, 390]) {
      await page.setViewportSize({ width, height: 1000 });
      await foldRail(width <= 480);
      await page.evaluate(() => { const j = [...Engrave.items().values()].find(x => x.row.order.receiptId === '4290000201'); CN.setMode('engrave'); Engrave.restoreView({ list: false, chosen: true, focus: j.key, pin: null, q: 'BAR_1548' }); Engrave.render(); });
      await R.settle(page, 1200);
      const o = await page.evaluate(() => {
        const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); if (!c) return null; const cr = c.getBoundingClientRect(), outside = [];
        for (const e of c.querySelectorAll('*')) { const r = e.getBoundingClientRect(); if (!r.width || !r.height || e.closest('[hidden]') || getComputedStyle(e).visibility === 'hidden' || e.closest('.seal')) continue; if (r.left < cr.left - .6 || r.right > cr.right + .6) outside.push((e.className && e.className.baseVal === undefined ? '.' + String(e.className).trim().split(/\s+/).join('.') : e.tagName) + ' ' + Math.round(r.left - cr.left) + '/' + Math.round(r.right - cr.right)); }
        return { state: c.dataset.state, tick: !!c.querySelector('.egApproved[data-a=approved][disabled]'), reopen: c.querySelectorAll('[data-a=reopen]').length, cardSW: c.scrollWidth - c.clientWidth, pageSW: document.documentElement.scrollWidth - innerWidth, outside: outside.slice(0, 4) };
      });
      note(!!o && o.state === 'approved' && o.tick && o.reopen === 0, `approved card @${width}: it opened as the same card, approved (a calm tick), no Reopen`, JSON.stringify(o));
      note(!!o && o.cardSW <= 0 && o.pageSW <= 0 && o.outside.length === 0, `approved card @${width}: nothing hangs out of the card, nothing scrolls sideways`, JSON.stringify(o));
      if (shots) await R.shot(w, `egrow-open-${width}`);
    }
    note(w.errors.length === 0, 'no page error', JSON.stringify(w.errors.slice(0, 2)));
  } finally { await w.close(); }
  console.log(fails.length ? `\n${fails.length} FAILED:\n  - ` + fails.slice(0, 40).join('\n  - ') : '\nall passed');
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
