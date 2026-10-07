// Options on the title line, and a controls row that shows everything (Paul, 5 Oct: "Move the Options button to be to the far right of
// the date text 'Oct 2'. This should allow ample room and spacing to show the remainder of the options without crowding everything").
// A Nest card's title line reads "RG 14/20 · Filling · Oct 2"; under it the controls row held the sheet pill, Cut Sheet, Options, the
// back-engraving pill and Front | Back · engraving, and ran out of room: the sheet pill showed "Sh…" and Front | Back was cut off at the
// card's edge. Options now stands at the right end of the TITLE line (the same button, the same panel), and the controls row shows each
// control whole: when they cannot all stand in one row (a narrow card) the rest goes to a second row, left aligned, never clipped.
//  · at 1440 / 900 / 390 / 320 px (the rail folded below 900, as a person does), on five cards (GF with back engraving, SS plain, RG with Cut
//    Sheet and back engraving, 10K and 14K with back engraving, the 14K one with its sheet ticked and a crowded title):
//    Options is on the title row, to the right of the date text, at the card's right end, centred with the title, never under the title
//    text, never wrapped below; the title text gives way with an ellipsis before Options does; no Options button in the controls row;
//  · every control of the row is whole: the sheet pill ("Sheet N", never "Sh…"), its count, Cut Sheet, the engraving pill, Front and
//    "Back · engraving": boxes inside the card, every label unclipped, a hit test at both ends lands on the control, the row and the card
//    and the page do not scroll sideways; a second row only when needed, and left aligned;
//  · the five cards line up: one title height, one controls height (one row) and the picture at the same depth at 1440 and 900;
//  · the panel opens under the title line, inside the card, at the card's right edge, covering neither the title nor the card's edge;
//    its open state is kept across a redraw (the same node), Esc and × close it;
//  · "Options ✓" for a ticked sheet; the old waiting line of a run of the earlier release rule still stands in the controls row;
//  · a mutant: with Options put back in the controls row the same measurements fail.
//   node tests/charm-nest/options-on-title-line.cjs   (PW_DIR=<dir holding playwright-core>, CHROMIUM=<chrome>; OPT_SHOTS=<dir> writes pictures)
const path = require('path'), fs = require('fs'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const tick = (n = 30) => new Promise(r => setTimeout(r, n));
const PNG_1 = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==', 'base64');
const pick = () => {
  for (const d of [process.env.PW_DIR, '/opt/node22/lib/node_modules/playwright/node_modules', '/opt/node22/lib/node_modules', path.join(root, 'node_modules')].filter(Boolean)) {
    for (const n of ['playwright-core', 'playwright']) { try { return require(path.join(d, n)).chromium; } catch (_) {} }
  }
  return null;
};

async function main() {
  const chromium = pick(); if (!chromium) { console.log('  – no playwright: the browser checks were not run'); return; }
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const shots = process.env.OPT_SHOTS || '', errors = [];
  if (shots) fs.mkdirSync(shots, { recursive: true });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => /\/eng-back-\d+\.png/.test(u.href), r => r.fulfill({ status: 200, contentType: 'image/png', headers: { 'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-store' }, body: PNG_1 }));
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} });
    const pg = await context.newPage(); pg.setDefaultTimeout(20000);
    pg.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await pg.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await pg.waitForFunction(() => window.CN && window.Engrave && window.CharmNestBacks && window.Gate && window.RoseStock && CN.S.cloud.ok === true, null, { timeout: 60000 });

    // Five cards. Sets of this page's own, never a real one: GF (two sheets, the first with back engraving), SS (one sheet, none),
    // RG (Sheet 1 saved and checked, so Cut Sheet shows; back engraving), 10K (two sheets, back engraving), 14K (three, back engraving).
    await pg.evaluate(origin => {
      const MM = 72 / 25.4, sq = { kind: 'path', subpaths: [[['m', [0, 0]], ['l', [6 * MM, 0]], ['l', [6 * MM, 6 * MM]], ['l', [0, 6 * MM]], ['h']]], stroke: true, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [0, 0, 6 * MM, 6 * MM] };
      let seq = 0, nb = 0;
      const fill = (pgx, sheetId, n, status, backs, extra) => {
        const ids = Array.from({ length: n }, (_, i) => `${sheetId}c${i}`);
        pgx.sheetId = sheetId; pgx.status = status; pgx.dirty = false; pgx.poolIds = ids.map(i => `${i}_1_1`);
        pgx.charms = ids.map((id, i) => ({ id, name: `41737${++seq} · TAG`, poolId: `${id}_1_1`, order: `41737${seq}`, sku: 'TAG', centerPt: [3 * MM, 3 * MM], widthPt: 6 * MM, heightPt: 6 * MM, areaPt2: 36 * MM * MM, outline: sq, members: [sq] }));
        pgx.placements = ids.map((id, i) => ({ id, cxPt: (10 + (i % 8) * 9) * MM, cyPt: (12 + Math.floor(i / 8) * 9) * MM, angle: 0, wPt: 6 * MM, hPt: 6 * MM }));
        if (backs) pgx.backPool = ids.slice(0, backs).map((id, i) => ({ poolId: `${id}_1_1`, order: pgx.charms[i].order, sku: 'TAG', copy: 1, text: 'For ' + id, approvedAt: Date.now() - 1000 - i, previewWPt: 12 * MM, previewHPt: 12 * MM, preview: `${origin}/eng-back-${nb++}.png` }));
        Object.assign(pgx, extra || {});
      };
      const add = (metal, specs) => { const prim = S.sheets[metal]; specs.forEach((sp, i) => { const pgx = i === 0 ? prim : makeSheet(metal, i + 1); fill(pgx, `opt-${metal}-${i + 1}`, ...sp); if (i) prim.pages.push(pgx); }); };
      add('gold', [[4, 'complete', 2], [3, 'complete', 0]]);
      add('silver', [[12, 'complete', 0]]);
      add('rose', [[3, 'complete', 3, { persistedDone: true, verification: { ok: true } }], [2, 'ready']]);
      add('gold10k', [[4, 'complete', 2], [3, 'complete', 0]]);
      add('gold14k', [[3, 'complete', 3], [4, 'complete', 0], [5, 'ready']]);
      CN.setMode('nest');
      for (const m of ['gold14k', 'gold', 'silver', 'gold10k', 'rose']) { for (const p of S.sheets[m].pages) p.el = null; S.sheets[m].active = 0; S.sheets[m].pages[0].el = S.sheets[m].cardEl; }
      refreshAllCards();
    }, srv.sorterOrigin);
    await tick(700);

    const M = ['gold', 'silver', 'rose', 'gold10k', 'gold14k'], card = m => `.sheetCard[data-m="${m}"]`;
    const fit = async (w, h = 900) => { await pg.setViewportSize({ width: w, height: h }); await pg.evaluate(w => { const off = document.getElementById('app').classList.contains('railOff'); if ((w < 900) !== off) document.getElementById('btnRail').click(); }, w); await tick(380); };

    // what a card shows: the title line, the controls row, every control's box, and what each label does with its room
    const look = m => pg.evaluate(m => {
      const c = document.querySelector(`.sheetCard[data-m="${m}"]`); c.scrollIntoView({ block: 'center' });
      const head = c.querySelector('.shHead'), row = c.querySelector('.shControls'), name = head.querySelector('.name'), prov = head.querySelector('.prov'), pillS = head.querySelector('.pill');
      const opt = c.querySelector('.sheetOptions > .sheetOptionsBtn'), box = e => { const r = e.getBoundingClientRect(); return { l: r.left, t: r.top, r: r.right, b: r.bottom, w: r.width, h: r.height, cy: r.top + r.height / 2 }; };
      const shown = e => e && !e.hidden && e.getClientRects().length > 0 && !e.classList.contains('hidden') && getComputedStyle(e).visibility !== 'hidden';
      const hits = e => { const r = e.getBoundingClientRect(), y = r.top + r.height / 2; return [r.left + 2, r.right - 2].every(x => { const at = document.elementFromPoint(x, y); return !!at && (e === at || e.contains(at)); }); };
      const kids = [...row.children].filter(shown), cardB = box(c), rowB = box(row);
      // each control of the row, each label of it, against the card and its own room
      const parts = [];
      const add = (what, e) => { if (!shown(e)) return; const b = box(e); parts.push({ what, ...b, inCard: b.l >= cardB.l + 1 - .5 && b.r <= cardB.r - 1 + .5, whole: e.scrollWidth <= e.clientWidth + 1, hits: hits(e), text: e.textContent.trim().replace(/\s+/g, ' ') }); };
      add('pill', row.querySelector('.shMenu .shmBtn')); add('pillName', row.querySelector('.shmName')); add('pillMark', row.querySelector('.shmMark, .shmSpin')); add('pillChev', row.querySelector('.shmChev'));
      add('cut', row.querySelector('.roseCut button')); add('eng', row.querySelector('.engTog button')); add('face', row.querySelector('.shFace'));
      add('front', row.querySelector('.shFace [data-face="front"]')); add('back', row.querySelector('.shFace [data-face="back"]'));
      const ys = kids.map(k => box(k).cy).sort((a, b) => a - b), nRows = ys.reduce((n, v, i) => n + (i && v - ys[i - 1] > 10 ? 1 : 0), ys.length ? 1 : 0);
      const lefts = kids.map(k => ({ cy: box(k).cy, l: box(k).l })), firstRowY = ys[0], secondRowLeft = lefts.filter(k => k.cy - firstRowY > 10).map(k => k.l), firstRowLeft = Math.min(...lefts.filter(k => k.cy - firstRowY <= 10).map(k => k.l));
      const range = document.createRange(); range.selectNodeContents(prov); const provText = range.getBoundingClientRect();
      const cs = opt && getComputedStyle(opt), sample = document.createElement('i'); sample.style.color = 'var(--ink70)'; document.body.append(sample); const ink70 = getComputedStyle(sample).color; sample.remove();
      return {
        card: cardB, head: box(head), row: rowB, canvas: box(c.querySelector('.shPreviewWrap')), name: box(name), prov: box(prov), provTextR: provText.right, provCut: prov.scrollWidth > prov.clientWidth + 1, provText: prov.textContent.trim(),
        pill: shown(pillS) ? box(pillS) : null, opt: opt ? { ...box(opt), text: opt.textContent.trim(), inHead: head.contains(opt), inRow: row.contains(opt), hits: hits(opt), whole: opt.scrollWidth <= opt.clientWidth + 1, font: cs.fontSize, radius: cs.borderTopLeftRadius, color: cs.color, ink70, border: cs.borderTopWidth + ' ' + cs.borderTopStyle } : null,
        optInRow: !!row.querySelector('.sheetOptions'), kids: kids.map(k => (k.dataset.r || k.className)), parts, nRows, secondRowLeft, firstRowLeft, rowScroll: [row.scrollWidth, row.clientWidth], headScroll: [head.scrollWidth, head.clientWidth], cardScroll: [c.scrollWidth, c.clientWidth],
        page: document.documentElement.scrollWidth - document.documentElement.clientWidth, vw: innerWidth,
      };
    }, m);

    // the title line: Options at its far right, after the date text, centred with the title, nothing on top of anything
    const onTitleLine = (x, tag) => {
      assert(x.opt, `${tag}: it has an Options button`);
      assert.equal(x.opt.inHead, true, `${tag}: Options is in the title line`); assert.equal(x.opt.inRow || x.optInRow, false, `${tag}: and no longer in the controls row`);
      assert(x.opt.t >= x.head.t - .5 && x.opt.b <= x.head.b + .5, `${tag}: Options is on the title row (${x.opt.t}-${x.opt.b} in ${x.head.t}-${x.head.b})`);
      assert(Math.abs(x.opt.cy - x.name.cy) <= 2.5, `${tag}: centred with the title (${x.opt.cy.toFixed(1)} / ${x.name.cy.toFixed(1)})`);
      assert(x.opt.l >= x.name.r + 4, `${tag}: right of the title text (${x.opt.l} >= ${x.name.r})`);
      const textEnd = Math.min(x.provTextR, x.prov.r);   // (a status text shortened with an ellipsis ends where its box does)
      assert(x.opt.l >= textEnd - .5, `${tag}: right of the date text (${x.opt.l} >= ${textEnd})`);
      assert(x.prov.r <= x.opt.l + .5 && x.name.r <= x.prov.l + .5, `${tag}: the title, the status text and Options do not overlap (${x.name.r} | ${x.prov.l}-${x.prov.r} | ${x.opt.l})`);
      if (x.pill) assert(x.pill.r <= x.opt.l + .5 && x.pill.l >= x.prov.l - .5, `${tag}: a status pill stands before Options, not on it`);
      assert(Math.abs((x.card.r - 1 - 14) - x.opt.r) <= 1.5, `${tag}: at the card's right end, one gutter in (${x.card.r - 15} / ${x.opt.r})`);
      assert.equal(x.opt.whole && x.opt.hits, true, `${tag}: Options is whole and not covered`);
      assert(x.opt.h <= x.head.h && Math.abs(x.opt.t - x.head.t) >= 0, `${tag}: Options does not make the title line taller (${x.opt.h} in ${x.head.h})`);
      assert.deepEqual([x.opt.font, x.opt.radius.startsWith('9') || parseFloat(x.opt.radius) > 20, x.opt.border, x.opt.color === x.opt.ink70], ['11px', true, '1px solid', true], `${tag}: the same look: 11px text, a round 1px outline, the same ink`);
      assert.equal(x.head.h <= 30, true, `${tag}: the title line is its one height (${x.head.h})`);
    };
    // the controls row: every control whole, inside the card, nothing clipped, nothing scrolling sideways
    const everythingShown = (x, tag, o = {}) => {
      assert(x.parts.length >= 1, `${tag}: the row has its controls`);
      for (const p of x.parts) {
        assert.equal(p.inCard, true, `${tag}: ${p.what} is inside the card (${p.l.toFixed(1)}-${p.r.toFixed(1)} in ${x.card.l}-${x.card.r})`);
        assert.equal(p.whole, true, `${tag}: ${p.what} "${p.text}" is not clipped`);
        assert.equal(p.hits, true, `${tag}: ${p.what} is seen at both its ends (nothing covers or cuts it)`);
      }
      assert(x.rowScroll[0] <= x.rowScroll[1] + 1, `${tag}: the row does not scroll sideways (${x.rowScroll})`); assert(x.cardScroll[0] <= x.cardScroll[1] + 1, `${tag}: nor the card (${x.cardScroll})`);
      assert(x.headScroll[0] <= x.headScroll[1] + 1, `${tag}: nor the title line (${x.headScroll})`); assert(x.page <= 0, `${tag}: nor the page (${x.page})`);
      const part = w => x.parts.find(p => p.what === w), nm = part('pillName');
      if (nm) assert.match(nm.text, /^Sheet \d+$/, `${tag}: the sheet pill says "Sheet N" whole, never "Sh…" (${nm.text})`);
      if (part('back')) { assert.equal(part('back').text, 'Back · engraving', `${tag}: the whole "Back · engraving" label`); assert.equal(part('front').text, 'Front'); }
      if (o.cut) assert(part('cut'), `${tag}: Cut Sheet shows`);
      // a second row only when needed, and left aligned: the second row's first control starts where the first row's does
      assert(x.nRows <= 2, `${tag}: at most two rows (${x.nRows})`);
      if (x.nRows === 2) assert(Math.abs(Math.min(...x.secondRowLeft) - x.firstRowLeft) <= 1 && Math.abs(x.firstRowLeft - (x.row.l + 14)) <= 1.5, `${tag}: the second row is left aligned with the first (${x.secondRowLeft} / ${x.firstRowLeft})`);
    };

    // ── 1 · four widths: Options on the title line, every control whole
    const seen = {};
    for (const w of [1440, 900, 390, 320]) {
      await fit(w); seen[w] = {};
      for (const m of M) {
        const x = seen[w][m] = await look(m), tag = `${m} @${w}px`;
        if (m === 'silver' || m === 'gold') { assert.equal(x.opt, null, `${tag}: a Gold or Silver card has no Options, as before`); assert.equal(x.optInRow, false); }
        else onTitleLine(x, tag);
        everythingShown(x, tag, { cut: m === 'rose' });
      }
      const wrapped = M.filter(m => seen[w][m].nRows === 2);
      console.log(`  ✓ @${w}px: Options at the right end of the title line (${M.filter(m => seen[w][m].opt).map(m => `${m} ${Math.round(seen[w][m].opt.l)}-${Math.round(seen[w][m].opt.r)}`).join(', ')}); every control whole${wrapped.length ? '; a second row, left aligned, on ' + wrapped.join(', ') : '; each row one row'}`);
    }
    // the card Paul showed (a RG card with Cut Sheet and back engraving) at the width where it was cut off: whole in one row now
    {
      await fit(1280); const x = await look('rose'); everythingShown(x, 'rose @1280px', { cut: true }); onTitleLine(x, 'rose @1280px');
      assert.equal(x.nRows, 1, 'the RG card of the picture (Cut Sheet, the engraving pill, Front | Back) stands in one row at 1280px, which was cut off before');
      console.log('  ✓ @1280px (the width of the picture): the RG card holds the sheet pill, Cut Sheet, the engraving pill and Front | Back · engraving whole, in one row');
    }

    // ── 2 · the cards line up: one title height, one controls row, the picture at one depth (a row of cards; with or without Cut Sheet)
    for (const w of [1440, 900]) {
      const g = seen[w], heads = new Set(M.map(m => Math.round(g[m].head.h))), rows = new Set(M.map(m => Math.round(g[m].row.h))), depth = new Set(M.map(m => Math.round(g[m].canvas.t - g[m].card.t)));
      assert.equal(heads.size, 1, `@${w}px: one title height on every card (${[...heads]}), with Options and without`);
      assert.equal(rows.size, 1, `@${w}px: one controls row height on every card (${[...rows]}), with Cut Sheet and without`);
      assert.equal(depth.size, 1, `@${w}px: the picture at one depth on every card (${[...depth]})`);
      for (const m of M) assert.equal(g[m].nRows, 1, `${m} @${w}px: one row`);
      // sheets side by side in the grid (1440: two to a row) stand level
      const byTop = {}; for (const m of M) (byTop[Math.round(g[m].card.t)] ||= []).push(m);
      for (const ms of Object.values(byTop)) if (ms.length > 1) assert.equal(new Set(ms.map(m => Math.round(g[m].canvas.t))).size, 1, `@${w}px: ${ms.join(' and ')} side by side: their pictures start at one height`);
    }
    // at 390 the controls of the RG card (the most of any) go to a second row; the card is that much taller and the others are unchanged
    { const g = seen[390]; assert.equal(g.rose.nRows, 2, 'RG @390px: Cut Sheet, the engraving pill and Front | Back · engraving take two rows'); assert.equal(g.gold14k.nRows, 1, '14K @390px: the sheet pill, the engraving pill and Front | Back fit one row'); assert.equal(g.silver.nRows, 1);
      assert(g.rose.row.h > g.gold14k.row.h && g.rose.row.h <= 2 * g.gold14k.row.h + 6, `RG @390px: a row and a half-gutter taller, no more (${g.rose.row.h} / ${g.gold14k.row.h})`);
      assert.equal(new Set(['gold', 'silver', 'gold10k', 'gold14k'].map(m => Math.round(g[m].row.h))).size, 1, 'the cards that fit one row keep the one height'); }
    console.log('  ✓ the five cards line up at 1440 and 900px (title height, controls row, picture depth, pairs side by side); at 390px only the card that needs a second row has one');

    // ── 3 · a crowded title: a long status text and a status pill: the text gives way with an ellipsis, Options stays whole at the right end
    await fit(390);
    await pg.evaluate(() => {
      const sh = S.sheets.gold14k.pages[0]; sh.seq = 3; sh.draft = true; sh.topup = { tried: [1, 2, 3], closedAt: null }; sh.status = 'queued'; sh.solidPick = true;
      sh.charms.forEach((c, i) => { if (i < 2) c.custom = true; }); renderCard(sh);
    });
    await tick(250);
    { const x = await look('gold14k'), tag = 'a crowded title @390px';
      onTitleLine(x, tag); assert(x.pill, `${tag}: the status pill shows`); assert.equal(x.provCut, true, `${tag}: the status text is shortened with an ellipsis (${x.provText})`);
      assert.equal(x.opt.text, 'Options ✓', `${tag}: a ticked sheet says "Options ✓"`); assert(x.opt.r <= x.card.r - 14, `${tag}: Options is whole at the card's right end`);
      everythingShown(x, tag);
      await fit(320); const y = await look('gold14k'); onTitleLine(y, 'a crowded title @320px'); everythingShown(y, 'a crowded title @320px');
      assert.equal(y.opt.text, 'Options ✓');
      console.log(`  ✓ a crowded title (Set 3, filling gaps, custom, a Queued pill) at 390 and 320px: the status text is shortened with an ellipsis, "Options ✓" stays whole at the right end`); }
    await pg.evaluate(() => { const sh = S.sheets.gold14k.pages[0]; delete sh.seq; delete sh.topup; sh.status = 'complete'; sh.charms.forEach(c => { delete c.custom; }); delete sh.solidPick; renderCard(sh); });
    await tick(200);

    // ── 4 · the window: Options opens ONE large window (OptionsStudio: about 92vw x 90vh, 1400px at most, a full screen on a phone) holding the card's own
    //        controls and its Sheet menu; kept across a redraw (the same window, the same controls); Esc and × close it and the focus goes back to Options
    const win = () => pg.evaluate(() => {
      const d = document.querySelector('dialog.osDlg'); if (!d) return null; const r = d.getBoundingClientRect(), box = d.querySelector('.solidOptions');
      return { open: d.open, shown: r.width > 0 && getComputedStyle(d).display !== 'none', w: r.width, h: r.height, vw: innerWidth, vh: innerHeight, cards: box ? [...box.children].map(c => c.dataset.card) : [], same: window.__optDlg ? window.__optDlg === d : (window.__optDlg = d, true), inside: d.contains(document.activeElement), onBtn: false };
    });
    const onButton = m => pg.evaluate(m => document.activeElement === document.querySelector(`.sheetCard[data-m="${m}"] .sheetOptionsBtn`), m);
    for (const w of [1440, 390]) {
      await fit(w);
      for (const m of ['rose', 'gold10k', 'gold14k']) {
        const tag = `${m} @${w}px`; await pg.evaluate(m => { window.__optDlg = null; document.querySelector(`.sheetCard[data-m="${m}"]`).scrollIntoView({ block: 'start' }); }, m); await tick(120);
        await pg.click(`${card(m)} .sheetOptionsBtn`); await tick(500);
        let p = await win();
        assert(p && p.open && p.shown, `${tag}: a press on Options opens the window`);
        if (w >= 1000) { assert(Math.abs(p.w - Math.min(1400, p.vw * .92)) <= 2 && Math.abs(p.h - Math.min(1000, p.vh * .9)) <= 2, `${tag}: about 92vw x 90vh, 1400px at most (${p.w} x ${p.h} in ${p.vw} x ${p.vh})`); }
        else assert(p.w >= p.vw - 2, `${tag}: a full screen on a phone (${p.w} in ${p.vw})`);
        assert.deepEqual(p.cards.slice(0, 1), ['sheet'], `${tag}: the Sheet menu leads`); assert(!p.cards.some(c => ['history', 'all', 'partial'].includes(c)), `${tag}: one Sheet menu, no Partial sheets / Sheet history / All partial sheets cards (${p.cards})`);
        assert.equal(p.inside, true, `${tag}: the focus is inside the window`);
        // a redraw of the card and of every card keeps the same window open
        await pg.evaluate(m => { const sh = S.sheets[m].pages[0]; renderCard(sh); refreshAllCards(); Gate.refreshMembership(); Gate.renderCard(sh); }, m); await tick(120);
        p = await win(); assert(p && p.open && p.same, `${tag}: still open, the same window, after a redraw`);
        await pg.keyboard.press('Escape'); await tick(400); assert.equal(await win(), null, `${tag}: Esc closes it`); assert.equal(await onButton(m), true, `${tag}: and the focus is back on Options`);
        await pg.click(`${card(m)} .sheetOptionsBtn`); await tick(400); assert((await win()).open);
        await pg.click('dialog.osDlg [data-os="close"]'); await tick(400); assert.equal(await win(), null, `${tag}: × closes it`);
        // closed it stays closed across a redraw
        await pg.evaluate(m => { renderCard(S.sheets[m].pages[0]); refreshAllCards(); }, m); await tick(80); assert.equal(await win(), null);
      }
    }
    console.log('  ✓ the Options window: about 92vw x 90vh, kept across a redraw (the same window); Esc and × close it, the focus goes back to Options; at 1440 and 390px');

    // ── 5 · the old waiting line of a run of the earlier release rule is wider than a title line: it stands in the controls row, and Options returns to the title line after
    await fit(1440);
    {
      const both = await pg.evaluate(() => {
        const sh = S.sheets.gold10k.pages[0], c = sh.cardEl, g = c.querySelector('[data-r="gate"]'), where = () => ({ inRow: g.parentElement === c.querySelector('.shControls'), inHead: g.parentElement === c.querySelector('.shHead'), hidden: g.classList.contains('hidden'), text: g.textContent.trim().slice(0, 60), opt: !!c.querySelector('.sheetOptions') });
        const saved = B.run; B.run = { runId: 'run-old', status: 'running', releasePolicy: 1, step: 'nest' }; Gate.state().lastReleased.gold10k = new Date().toISOString().slice(0, 10);
        let legacy, modern; try { Gate.renderCard(sh); legacy = where(); } finally { B.run = saved; delete Gate.state().lastReleased.gold10k; }
        Gate.renderCard(sh); modern = where(); modern.summary = !!c.querySelector('.shHead .sheetOptions > .sheetOptionsBtn');
        return { legacy, modern };
      });
      assert.deepEqual([both.legacy.inRow, both.legacy.inHead, both.legacy.hidden, both.legacy.opt], [true, false, false, false], `the earlier rule's waiting line stands in the controls row (${both.legacy.text})`);
      assert.deepEqual([both.modern.inHead, both.modern.summary, both.modern.hidden], [true, true, false], 'with the current release rule again, Options is back on the title line');
      console.log('  ✓ a run of the earlier release rule keeps its waiting line in the controls row; the current rule puts Options back on the title line');
    }

    // ── 6 · pictures
    if (shots) {
      for (const w of [1440, 390]) {
        await fit(w);
        for (const [m, n] of [['rose', 'rg'], ['gold14k', '14k'], ['gold10k', '10k'], ['gold', 'gf']]) { await pg.evaluate(m => document.querySelector(`.sheetCard[data-m="${m}"]`).scrollIntoView({ block: 'center' }), m); await tick(120); await (await pg.$(card(m))).screenshot({ path: path.join(shots, `options-title-${n}-${w}.png`) }); }
        // the Options window open
        await pg.evaluate(() => document.querySelector('.sheetCard[data-m="rose"]').scrollIntoView({ block: 'start' })); await tick(120);
        await pg.click(`${card('rose')} .sheetOptions > .sheetOptionsBtn`); await tick(250);
        const r = await pg.evaluate(() => { const b = document.querySelector('.sheetCard[data-m="rose"]').getBoundingClientRect(); return { x: Math.max(0, b.left - 6), y: Math.max(0, b.top - 6), w: Math.min(innerWidth, b.width + 12), h: Math.min(innerHeight - Math.max(0, b.top - 6), 640) }; });
        await pg.screenshot({ path: path.join(shots, `options-title-rg-panel-${w}.png`), clip: { x: r.x, y: r.y, width: r.w, height: r.h } });
        await pg.keyboard.press('Escape'); await tick(250);
      }
    }

    // ── 7 · the mutant: Options put back into the controls row: the same measurements fail
    await fit(1440);
    await pg.evaluate(() => { const c = S.sheets.rose.cardEl, g = c.querySelector('[data-r="gate"]'), row = c.querySelector('.shControls'); row.insertBefore(g, row.querySelector('.engTogHost')); });
    { const x = await look('rose'); let caught = false; try { onTitleLine(x, 'mutant'); } catch (_) { caught = true; } assert.equal(caught, true, 'a card with Options back in the controls row is caught by the title-line check'); console.log('  ✓ mutant: Options back in the controls row is caught'); }
    await pg.evaluate(() => { Gate.renderCard(S.sheets.rose.pages[0]); });
    { const x = await look('rose'); onTitleLine(x, 'rose after the mutant'); }
    // the second mutant: the old no-wrap row at 390px clips Front | Back, and the checks see it
    await fit(390);
    await pg.addStyleTag({ content: '.shControls{flex-wrap:nowrap!important}.shControls .shTabs.shMenuHost{flex:1 1 0!important}' }); await tick(200);
    { const x = await look('rose'); let caught = false; try { everythingShown(x, 'mutant', { cut: true }); } catch (_) { caught = true; } assert.equal(caught, true, 'a row that cannot wrap clips, and the checks catch it'); console.log('  ✓ mutant: a controls row that cannot wrap clips at 390px, and the checks catch it'); }

    assert.deepEqual(errors, [], 'no page errors');
    console.log('PASS: options-on-title-line');
  } finally { await browser.close(); srv.close && srv.close(); }
}
main().then(() => process.exit(0), e => { console.error(e && e.stack || e); process.exit(1); });
