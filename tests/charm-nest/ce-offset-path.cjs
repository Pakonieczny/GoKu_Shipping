/* OFS1 · OFFSET PATH, AS ILLUSTRATOR'S, AND THE CHARM MADE THICKER OR THINNER (charm-nest-ce-ops.js; Paul, 9 Nov 2026: "some designs are just too thin, and they need to be
 * offset on the inside and outside perimeter and the user needs to be able to choose whether to make it thinner or thicker. Illustrator already has such an option, so please
 * replicate the functionality"). No network, no live service.
 *
 *   node tests/charm-nest/ce-offset-path.cjs                                        the geometry and the operations on items (node only)
 *   PW_DIR=<dir holding playwright-core> node tests/charm-nest/ce-offset-path.cjs   ... and the browser pass (the editor's harness page served from 127.0.0.1)
 *
 *  O1  a 10 mm ring with a 0.6 mm wall, +0.2 mm Replace on the charm: wall 1.0 mm, outer 10.4 mm, hole 8.4 mm; still role cut, closed, the same id, its lock kept
 *  O2  −0.2 mm: wall 0.2 mm, warned under the laser's minimum (0.3 mm, the laser check's)
 *  O3  Joins on a 10 mm square: Miter 144, Round 100 + 40 + π, Bevel 142 mm²; Miter limit 1 falls back to bevel (Illustrator's)
 *  O4  Add new path keeps the original (Illustrator's standard), on a shape and on the charm
 *  O5  jump rings and ring holes keep their size unless "Include rings / holes"; engraved art is not touched; a cut line the person locked is left alone
 *  O6  a thinning that makes a part vanish says so; a hole closed by thickening is counted
 *  O7  the live effect re-edits from its original (not the offset of the offset); Expand Appearance lets it go; the charm's live offset re-edits too
 *  B*  the browser: the menus (Object ▸ Path, Effect ▸ Path) with the icon, Quick Actions, the dialog's fields, Preview changes nothing until OK, Cancel / Esc, Enter = OK,
 *      one undo step, redo, the last values remembered, the thinnest wall in the dialog
 */
'use strict';
const assert = require('node:assert/strict'), path = require('node:path'), fs = require('node:fs'), http = require('node:http');
const ROOT = path.join(__dirname, '..', '..');
const CE = require(path.join(ROOT, 'charm-nest-charm-editor.js'));
const OPS = require(path.join(ROOT, 'charm-nest-ce-ops.js'));
const K = OPS.kernel, O = OPS.ops, M = CE.model, PT = 72 / 25.4, MM = 25.4 / 72;
const geo = M.createGeo();
let n = 0;
const ok = (c, label) => { assert.ok(c, label); n++; console.log('  ok   ' + label); };
const mm2 = sps => Math.abs(K.signedArea(sps)) * MM * MM;
const near = (a, b, t) => Math.abs(a - b) <= t;
const boxMm = sub => { const b = M.bboxOf([sub]); return [(b[2] - b[0]) * MM, (b[3] - b[1]) * MM]; };
const closedAll = it => (it.subpaths || []).every(s => s[s.length - 1][0] === 'h');
const ringItem = (id, cx, cy, rOut, rIn, extra) => Object.assign({ id, kind: 'path', role: 'cut', source: 'master', name: 'Outline', stroke: true, fill: false, lwPt: 0.5, evenOdd: true, locked: true, _autoLock: true,
  subpaths: [M.ellipseSP(cx * PT, cy * PT, rOut * PT, rOut * PT)[0], M.ellipseSP(cx * PT, cy * PT, rIn * PT, rIn * PT)[0]] }, extra || {});
const sq = (id, x0, y0, x1, y1, extra) => Object.assign({ id, kind: 'path', role: 'hatch', source: 'master', name: id, stroke: false, fill: true, subpaths: M.rectSP(x0 * PT, y0 * PT, x1 * PT, y1 * PT) }, extra || {});
const find = (items, id) => { let f = null; M.walk(items, it => { if (it.id === id) f = it; }); return f; };

/* ── O1 / O2 · the 10 mm ring, thicker and thinner ── */
{
  const ring = ringItem('cut1', 10, 10, 5, 4.4);
  const r = O.offsetItems([ring], [], geo, { target: 'charm', mm: 0.2, join: 'round', miter: 4 });
  const it = find(r.items, 'cut1'), subs = it.subpaths.map(boxMm).sort((a, b) => b[0] - a[0]);
  ok(near(r.report.wallMm, 1.0, 0.01), `O1 +0.2 mm on a 0.6 mm wall: the wall is ${r.report.wallMm.toFixed(3)} mm (1.0)`);
  ok(near(subs[0][0], 10.4, 0.005) && near(subs[1][0], 8.4, 0.005), `O1 the outer diameter grows to ${subs[0][0].toFixed(4)} mm (10.4), the hole shrinks to ${subs[1][0].toFixed(4)} mm (8.4)`);
  ok(it.role === 'cut' && closedAll(it) && it.subpaths.length === 2 && it.locked && it._autoLock && r.items.length === 1, 'O1 it stays the charm\'s cut line: role cut, both rings closed, the same id, the editor\'s lock kept, nothing added');
  ok(!O.charmReport(r.report).warn, `O1 nothing to warn of: "${O.charmReport(r.report).text}"`);
  const t = O.offsetItems([ring], [], geo, { target: 'charm', mm: -0.2, join: 'round', miter: 4 }), rep = O.charmReport(t.report);
  ok(near(t.report.wallMm, 0.2, 0.01) && rep.warn && /under the laser's 0\.3 mm minimum/.test(rep.text), `O2 −0.2 mm: the wall is ${t.report.wallMm.toFixed(3)} mm and the dialog warns: "${rep.text}"`);
  const tt = find(t.items, 'cut1').subpaths.map(boxMm).sort((a, b) => b[0] - a[0]);
  ok(near(tt[0][0], 9.6, 0.005) && near(tt[1][0], 9.2, 0.005), `O2 the outer edge moves in (${tt[0][0].toFixed(4)} mm) and the hole out (${tt[1][0].toFixed(4)} mm, 9.2)`);
  ok(O.offsetItems([ring], [], geo, { target: 'charm', mm: -0.4, join: 'round', miter: 4 }).error === 'Thinned to nothing: try a smaller inset', 'O2 a thinning past the wall is refused, nothing changed');
}

/* ── O3 · Joins and the Miter limit, on a filled 10 mm square (Replace) ── */
{
  const s = sq('s', 0, 0, 10, 10);
  const area = (join, miter) => mm2(find(O.offsetItems([s], ['s'], geo, { mm: 1, join, miter, mode: 'replace' }).items, 's').subpaths);
  const a = { miter: area('miter', 4), round: area('round', 4), bevel: area('bevel', 4), limit1: area('miter', 1) };
  ok(near(a.miter, 144, 0.144), `O3 Miter: ${a.miter.toFixed(4)} mm² (144: sharp corners)`);
  ok(near(a.round, 140 + Math.PI, 0.141), `O3 Round: ${a.round.toFixed(4)} mm² (100 + 40 + π = ${(140 + Math.PI).toFixed(4)})`);
  ok(near(a.bevel, 142, 0.142), `O3 Bevel: ${a.bevel.toFixed(4)} mm² (142: each corner cut straight across)`);
  ok(near(a.limit1, a.bevel, 1e-6), `O3 Miter limit 1: a square's mitre (√2 × the offset) is past it, so it is bevelled as Illustrator does: ${a.limit1.toFixed(4)} mm²`);
  const kept = find(O.offsetItems([s], ['s'], geo, { mm: 1, join: 'miter', miter: 4, mode: 'replace' }).items, 's');
  ok(kept.role === 'hatch' && kept.fill === true && kept.id === 's', 'O3 Replace original keeps its id and paint (a hatch fill stays a hatch fill)');
}

/* ── O4 · Add new path (Illustrator's standard) ── */
{
  const s = sq('s', 0, 0, 10, 10), r = O.offsetItems([s], ['s'], geo, { mm: 0.5, join: 'miter', miter: 4 });
  ok(r.items.length === 2 && r.items[1] === s && r.sel.length === 1 && r.sel[0] !== 's' && near(mm2(find(r.items, r.sel[0]).subpaths), 121, 0.121), 'O4 Add new path: the original stays as it was, a new 11 mm square just behind it, selected');
  const ring = ringItem('cut1', 10, 10, 5, 4.4), c = O.offsetItems([ring], [], geo, { target: 'charm', mode: 'add', mm: 0.2, join: 'round', miter: 4 });
  ok(c.items.length === 2 && c.items[1] === ring && find(c.items, c.sel[0]).role === 'cut', 'O4 on the charm, Add new path leaves the charm untouched and adds a cut line beside it');
}

/* ── O5 · rings and ring holes kept; engraved art and the person's locks left alone ── */
{
  const outline = { id: 'out', kind: 'path', role: 'cut', source: 'master', name: 'Cut outline', stroke: true, fill: false, locked: true, _autoLock: true, subpaths: M.rectSP(0, 0, 20 * PT, 30 * PT) };
  const bail = { id: 'bail', kind: 'path', role: 'cut', source: 'master', name: 'Bail hole', stroke: true, fill: false, locked: true, _autoLock: true, subpaths: M.ellipseSP(10 * PT, 3 * PT, 1.05 * PT, 1.05 * PT) };
  const big = { id: 'win', kind: 'path', role: 'cut', source: 'master', name: 'Window', stroke: true, fill: false, locked: true, _autoLock: true, subpaths: M.rectSP(4 * PT, 10 * PT, 16 * PT, 25 * PT) };
  const art = sq('art', 2, 26, 6, 28);
  const items = [outline, bail, big, art];
  const r = O.offsetItems(items, [], geo, { target: 'charm', mm: 0.3, join: 'miter', miter: 4 });
  const o = find(r.items, 'out');
  ok(find(r.items, 'bail') === bail && find(r.items, 'art') === art, 'O5 the bail hole (a ring-sized round hole) and the engraved art are the very same items, untouched');
  ok(!find(r.items, 'win') && o.subpaths.length === 2, 'O5 the window (a hole too big to be a ring hole) is offset with the outline: one cut line with its hole');
  const bx = o.subpaths.map(boxMm).sort((a, b) => b[0] - a[0]);
  ok(near(bx[0][0], 20.6, 0.005) && near(bx[1][0], 11.4, 0.005) && r.report.kept === 1, `O5 the outline grew to ${bx[0][0].toFixed(3)} mm, the window shrank to ${bx[1][0].toFixed(3)} mm; ${r.report.kept} ring hole kept`);
  const all = O.offsetItems(items, [], geo, { target: 'charm', mm: 0.3, join: 'miter', miter: 4, rings: true });
  const hole = find(all.items, 'out').subpaths.map(boxMm).sort((a, b) => a[0] - b[0])[0];
  ok(!find(all.items, 'bail') && near(hole[0], 1.5, 0.005), `O5 with "Include rings / holes" the bail hole is offset too: ${hole[0].toFixed(3)} mm (2.1 − 0.6)`);
  const mine = Object.assign({}, big, { _autoLock: false }), lk = O.offsetItems([outline, bail, mine], [], geo, { target: 'charm', mm: 0.3, join: 'miter', miter: 4 });
  ok(find(lk.items, 'win') === mine && lk.report.personLocked === 1 && /1 locked cut line left alone/.test(O.charmReport(lk.report).text), 'O5 a cut line the person locked is left alone, and the report says so');
  // the welded jump ring: a ring item of the shop's own (shape bail) keeps its size
  const jr = { id: 'jr', kind: 'path', role: 'cut', source: 'added', name: 'Jump ring', stroke: true, fill: false, evenOdd: true, shape: { kind: 'bail' }, subpaths: [M.ellipseSP(10 * PT, -2 * PT, 3.25 * MM * PT * PT, 3.25 * MM * PT * PT)[0]] };
  const jrr = O.offsetItems([outline, jr], [], geo, { target: 'charm', mm: 0.3, join: 'miter', miter: 4 });
  ok(find(jrr.items, 'jr') === jr, 'O5 the jump ring item keeps its size');
}

/* ── O6 · parts that vanish, holes that close ── */
{
  const two = { id: 'two', kind: 'path', role: 'cut', source: 'master', name: 'Outline', stroke: true, fill: false, subpaths: [M.rectSP(0, 0, 10 * PT, 10 * PT)[0], M.rectSP(15 * PT, 0, 15.3 * PT, 10 * PT)[0]] };
  const r = O.offsetItems([two], [], geo, { target: 'charm', mm: -0.2, join: 'miter', miter: 4 }), rep = O.charmReport(r.report);
  ok(r.report.vanished === 1 && rep.warn && /1 part disappeared/.test(rep.text), `O6 a 0.3 mm strip thinned by 0.2 mm vanishes and it is said: "${rep.text}"`);
  const pin = Object.assign(ringItem('pin', 10, 10, 5, 0.5), { subpaths: [M.ellipseSP(10 * PT, 10 * PT, 5 * PT, 5 * PT)[0], M.ellipseSP(10 * PT, 10 * PT, 0.6 * PT, 0.2 * PT)[0]] });   // (a slot, not a round ring hole)
  const c = O.offsetItems([pin], [], geo, { target: 'charm', mm: 0.6, join: 'round', miter: 4 });
  ok(c.report.holes1 === 0 && /1 hole closed/.test(O.charmReport(c.report).text) && find(c.items, 'pin').subpaths.length === 1, 'O6 a hole smaller than the thickening closes, and it is said');
}

/* ── O7 · the live effect ── */
{
  const s = sq('s', 0, 0, 10, 10);
  const a = O.offsetItems([s], ['s'], geo, { mm: 0.5, join: 'miter', miter: 4, mode: 'replace', live: true }), la = find(a.items, 's');
  ok(la.offsetFx && la.offsetFx.mm === 0.5 && near(mm2(la.subpaths), 121, 0.121), 'O7 Effect ▸ Offset Path: the square is 11 mm and keeps what it was made from');
  const b = O.offsetItems(a.items, ['s'], geo, { mm: 0.2, join: 'miter', miter: 4, mode: 'replace', live: true }), lb = find(b.items, 's');
  ok(near(mm2(lb.subpaths), 108.16, 0.11) && lb.offsetFx.mm === 0.2 && near(mm2(lb.offsetFx.src), 100, 1e-6), `O7 opened again at 0.2 mm it is 10.4 mm from the original (${mm2(lb.subpaths).toFixed(3)} mm²), not 11.4`);
  const moved = Object.assign({}, lb, { tx: 5 * PT }), c = O.offsetItems([moved], ['s'], geo, { mm: 1, join: 'miter', miter: 4, mode: 'replace', live: true });
  const bc = M.bboxOf(find(c.items, 's').subpaths).map(v => v * MM);
  ok(near(bc[0], 4, 1e-4) && near(bc[2], 16, 1e-4), 'O7 moved after, then opened again: the offset is of the original where it is now');
  const x = O.expandOffsetItems(b.items, ['s'], geo), lx = find(x.items, 's');
  ok(!lx.offsetFx && lx.subpaths === lb.subpaths, 'O7 Expand Appearance: the path as drawn, the live original let go');
  const ring = ringItem('cut1', 10, 10, 5, 4.4);
  const r1 = O.offsetItems([ring], [], geo, { target: 'charm', mm: 0.2, join: 'round', miter: 4, live: true });
  const r2 = O.offsetItems(r1.items, [], geo, { target: 'charm', mm: -0.1, join: 'round', miter: 4, live: true });
  ok(near(r2.report.wallMm, 0.4, 0.01), `O7 the charm's live offset, opened again at −0.1 mm, works from the 0.6 mm original: wall ${r2.report.wallMm.toFixed(3)} mm`);
}

console.log(`ce-offset-path node: ${n} checks passed`);

/* ── B · the browser ── */
(async () => {
  let chromium; try { ({ chromium } = require(path.join(process.env.PW_DIR || path.join(ROOT, 'node_modules'), 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser pass was not run'); return; }
  const exe = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1243/chrome-linux64/chrome';
  if (!fs.existsSync(exe)) { console.log('  - no Chromium: the browser pass was not run'); return; }
  const types = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css' };
  const server = http.createServer((req, res) => {
    const p = path.normalize(path.join(ROOT, decodeURIComponent(req.url.split('?')[0])));
    if (!p.startsWith(ROOT) || !fs.existsSync(p) || fs.statSync(p).isDirectory()) { res.writeHead(404); res.end(); return; }
    res.writeHead(200, { 'content-type': types[path.extname(p)] || 'application/octet-stream' }); fs.createReadStream(p).pipe(res);
  });
  await new Promise(r => server.listen(0, '127.0.0.1', r));
  const base = `http://127.0.0.1:${server.address().port}/`;
  const browser = await chromium.launch({ executablePath: exe, args: ['--no-sandbox'] });
  const failures = [];
  const step = async (name, fn) => { try { await fn(); n++; console.log('  ok   ' + name); } catch (e) { failures.push(name); console.log('  FAIL ' + name + '\n         ' + String(e && e.stack || e).split('\n').slice(0, 8).join('\n         ')); } };
  try {
    const ctxB = await browser.newContext({ viewport: { width: 1440, height: 900 }, reducedMotion: 'reduce' });
    await ctxB.route(u => !u.href.startsWith(base), r => r.abort());
    const page = await ctxB.newPage();
    const errs = []; page.on('pageerror', e => errs.push(e.message)); page.on('console', m => { if (m.type() === 'error') errs.push(m.text()); });
    await page.goto(base + 'tests/charm-nest/fixtures/charm-editor-harness.html');
    for (const f of ['charm-nest-vector.js', 'charm-nest-ce-extras.js', 'charm-nest-ce-ops.js', 'charm-nest-ce-ai-dialogs.js']) await page.addScriptTag({ url: base + f });
    const settle = () => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
    await page.evaluate(() => { window.openEditor(); }); await page.waitForSelector('.ce-root .ce-items path'); await settle(); await settle();
    const C = (fn, arg) => page.evaluate(([src, a]) => (new Function('C', 'a', 'return (' + src + ')(C, a)'))(window.CharmEditor.current.ctx, a), [fn.toString(), arg]);
    const hist = () => page.evaluate(() => window.CharmEditor.current.history().undo);
    const outlineW = () => C(c => { const e = c.entry('cut-outline'); const b = c.geo.drawn(e.it, e.P); return (b[2] - b[0]) * 25.4 / 72; });
    const holeW = () => C(c => { const e = c.entry('cut-hole'); if (!e) return null; const b = c.geo.drawn(e.it, e.P); return (b[2] - b[0]) * 25.4 / 72; });
    const closeM = () => page.evaluate(() => window.CharmEditor.current.ai && window.CharmEditor.current.ai.closeMenus());
    const openMenu = async (top, sub) => {
      await closeM(); await page.click(`.ce-menubar .ce-mb__t:text-is("${top}")`); await settle();
      if (sub) { await page.hover(`.ce-aimenu:not([hidden]) .ce-aimi--sub:has(.ce-aimi__l:text-is("${sub}"))`); await page.waitForTimeout(400); await settle(); }
    };

    await step('B1 Object ▸ Path ▸ Offset Path… and Effect ▸ Path ▸ Offset Path… are where Illustrator has them, each with the Offset Path icon; Object ▸ Expand Appearance is there', async () => {
      await C(c => c.select([])); await settle();
      await openMenu('Object', 'Path');
      const obj = await page.$$eval('.ce-root .ce-aimenu--sub:not([hidden]) [data-xcmd="ops.offset"]', els => els.map(e => ({ t: e.textContent, ic: !!e.querySelector('.ce-aimi__ic svg'), off: e.classList.contains('is-off') })));
      assert.equal(obj.length, 1, 'Object ▸ Path ▸ Offset Path…'); assert.ok(obj[0].ic, 'its icon'); assert.ok(!obj[0].off, 'on with nothing selected: it works on the charm'); assert.match(obj[0].t, /Offset Path…/);
      if (process.env.SHOTS) await page.screenshot({ path: path.join(process.env.SHOTS, 'ofs-menu-object.png') });
      await openMenu('Object');
      const ex = await page.$$eval('.ce-aimenu:not([hidden]) > [data-xcmd="ops.expandFx"]', els => els.map(e => e.classList.contains('is-off')));
      assert.deepEqual(ex, [true], 'Object ▸ Expand Appearance, off until something has a live offset');
      await openMenu('Effect', 'Path');
      const fx = await page.$$eval('.ce-root .ce-aimenu--sub:not([hidden]) [data-xcmd="ops.offsetFx"]', els => els.map(e => ({ t: e.textContent, ic: !!e.querySelector('.ce-aimi__ic svg') })));
      assert.equal(fx.length, 1, 'Effect ▸ Path ▸ Offset Path…'); assert.ok(fx[0].ic, 'its icon');
      await closeM(); await settle();
    });
    await step('B2 the Properties panel\'s Quick Actions: Offset Path… with its icon, with nothing selected (the charm) and with a shape selected', async () => {
      await C(c => c.select([])); await settle();
      const doc = await page.$$eval('.ce-root [data-aip-sec="docqa"] [data-aim="x:ops.offset"]', els => els.map(e => ({ ic: !!e.querySelector('.cops-ic svg'), hid: e.hidden || !e.getClientRects().length, dis: e.disabled })));
      assert.equal(doc.length, 1); assert.ok(doc[0].ic && !doc[0].dis, 'shown and on: ' + JSON.stringify(doc));
      await C(c => c.select(['heart-fill'])); await settle();
      const q = await page.$$eval('.ce-root [data-aip-qa] [data-aim="x:ops.offset"]', els => els.map(e => !!e.querySelector('.cops-ic svg')));
      assert.deepEqual(q, [true]);
    });
    await step('B3 nothing selected, Offset Path…: Illustrator\'s fields (Offset with its spinner, Joins Miter / Round / Bevel, Miter limit, Preview ticked), Replace original for the charm', async () => {
      await C(c => c.select([])); await settle();
      await page.click('.ce-root [data-aip-sec="docqa"] [data-aim="x:ops.offset"]'); await page.waitForSelector('.ce-root .cops-card [data-dnum="mm"]'); await settle();
      const f = await page.evaluate(() => { const c = document.querySelector('.ce-root .cops-card'); return { title: c.querySelector('h3').textContent, mm: !!c.querySelector('[data-dnum="mm"]'), spin: c.querySelectorAll('[data-dspin="mm"]').length, joins: [...c.querySelectorAll('[data-dseg="join"]')].map(b => b.textContent), miter: !!c.querySelector('[data-dnum="miter"]'), prev: c.querySelector('[data-dprev]').checked, mode: (c.querySelector('[data-dseg="mode"].is-on') || {}).textContent, rings: !!c.querySelector('[data-dchk="rings"]'), aid: c.closest('.aid') ? 'dressed' : 'plain' }; });
      assert.equal(f.title, 'Offset Path'); assert.ok(f.mm && f.miter && f.spin === 2, JSON.stringify(f)); assert.deepEqual(f.joins, ['Miter', 'Round', 'Bevel']);
      assert.ok(f.prev, 'Preview ticked at first'); assert.equal(f.mode, 'Replace original'); assert.ok(f.rings, 'the bail hole: Include rings / holes is offered'); assert.equal(f.aid, 'dressed');
    });
    let w0, h0, hist0;
    await step('B4 Preview draws the thicker charm over the work and changes nothing; the line under it says the thinnest wall', async () => {
      w0 = await outlineW(); h0 = await holeW(); hist0 = await hist();
      await page.fill('.ce-root .cops-card [data-dnum="mm"]', '0.3'); await settle(); await settle();
      await page.waitForFunction(() => /wall/i.test((document.querySelector('.ce-root .cops-card [data-dstat]') || {}).textContent || ''), null, { timeout: 5000 });
      const drawn = await page.evaluate(() => (document.querySelector('.ce-root .cops-preview path') || { getAttribute: () => '' }).getAttribute('d').length);
      assert.ok(drawn > 50, 'the preview is drawn'); assert.equal(await outlineW(), w0, 'the document is unchanged'); assert.equal(await hist(), hist0);
      console.log('  ..   the dialog\'s line: ' + await page.textContent('.ce-root .cops-card [data-dstat]'));
      if (process.env.SHOTS) await page.screenshot({ path: path.join(process.env.SHOTS, 'ofs-dialog-charm.png') });
    });
    await step('B5 the spinner steps the Offset (0.05 mm), ↑ too; Esc is Cancel: the charm as it was, no step', async () => {
      await page.click('.ce-root .cops-card [data-dspin="mm"][data-dir="1"]'); assert.equal(await page.inputValue('.ce-root .cops-card [data-dnum="mm"]'), '0.35');
      await page.focus('.ce-root .cops-card [data-dnum="mm"]'); await page.keyboard.press('ArrowDown'); assert.equal(await page.inputValue('.ce-root .cops-card [data-dnum="mm"]'), '0.3');
      await page.keyboard.press('Escape'); await settle();
      assert.equal(await page.$('.ce-root .cops-card'), null); assert.equal(await outlineW(), w0); assert.equal(await hist(), hist0);
      assert.equal(await page.$('.ce-root .cops-preview'), null, 'the preview is gone');
    });
    await step('B6 OK (Enter) at +0.3 mm: the charm\'s outline 0.6 mm wider, the bail hole the same size, ONE undo step; ⌘Z gives it back, ⇧⌘Z does it again; the values are remembered', async () => {
      await page.click('.ce-root [data-aip-sec="docqa"] [data-aim="x:ops.offset"]'); await page.waitForSelector('.ce-root .cops-card [data-dnum="mm"]');
      assert.equal(await page.inputValue('.ce-root .cops-card [data-dnum="mm"]'), '0.3', 'the last value typed');
      await page.focus('.ce-root .cops-card [data-dnum="mm"]'); await page.keyboard.press('Enter'); await settle();
      assert.equal(await page.$('.ce-root .cops-card'), null);
      const w1 = await outlineW(); assert.ok(Math.abs(w1 - w0 - 0.6) < 0.005, `outline ${w0} → ${w1}`); assert.ok(Math.abs(await holeW() - h0) < 1e-9, 'the bail hole kept its size');
      assert.equal(await hist(), hist0 + 1, 'one step');
      assert.ok(await C(c => c.entry('cut-outline').it.role === 'cut' && c.entry('cut-outline').it.locked), 'still the locked cut line');
      await page.keyboard.press('Control+z'); await settle(); assert.equal(await outlineW(), w0);
      await page.keyboard.press('Control+Shift+z'); await settle(); assert.ok(Math.abs(await outlineW() - w0 - 0.6) < 0.005);
      const saved = await page.evaluate(() => JSON.parse(localStorage.getItem('ce.offsetPath')));
      assert.equal(saved.mm, '0.3');
      await page.keyboard.press('Control+z'); await settle();
    });
    await step('B7 a filled shape selected: Add new path by default (Illustrator\'s), Replace original on choice; Cancel button leaves it as it was', async () => {
      await C(c => c.select(['heart-fill'])); await settle();
      const a0 = await C(c => c.items().length);
      await C(c => { window.CharmEditor.current.ctx; });
      await page.click('.ce-root [data-aip-qa] [data-aim="x:ops.offset"]'); await page.waitForSelector('.ce-root .cops-card [data-dnum="mm"]');
      assert.equal(await page.textContent('.ce-root .cops-card [data-dseg="mode"].is-on'), 'Add new path');
      await page.click('.ce-root .cops-card [data-db="cancel"]'); await settle(); assert.equal(await C(c => c.items().length), a0);
      await page.click('.ce-root [data-aip-qa] [data-aim="x:ops.offset"]'); await page.waitForSelector('.ce-root .cops-card [data-dnum="mm"]');
      await page.click('.ce-root .cops-card [data-dseg="mode"][data-v="replace"]'); await page.fill('.ce-root .cops-card [data-dnum="mm"]', '0.2');
      await page.click('.ce-root .cops-card [data-db="ok"]'); await settle();
      assert.equal(await C(c => c.items().length), a0, 'in place'); assert.ok(await C(c => !!c.entry('heart-fill') && c.selection()[0] === 'heart-fill'));
      await page.keyboard.press('Control+z'); await settle();
    });
    await step('B8 Effect ▸ Path ▸ Offset Path… is live: opened again on the shape it shows its values and works from the original; Expand Appearance lets it go', async () => {
      await C(c => c.select(['heart-fill'])); await settle();
      const area = () => C(c => { const e = c.entry('heart-fill'); return Math.abs(window.CharmEditorOps.kernel.signedArea(c.model.outlinesOf(e.it, c.geo, e.P))) * Math.pow(25.4 / 72, 2); });
      const a0 = await area();
      await openMenu('Effect', 'Path'); await page.click('.ce-aimenu--sub:not([hidden]) [data-xcmd="ops.offsetFx"]');
      await page.waitForSelector('.ce-root .cops-card [data-dnum="mm"]');
      assert.equal(await page.$('.ce-root .cops-card [data-dseg="mode"]'), null, 'the effect is always in place');
      await page.fill('.ce-root .cops-card [data-dnum="mm"]', '0.25'); await page.keyboard.press('Enter'); await settle();
      const a1 = await area(); assert.ok(a1 > a0, 'bigger');
      await openMenu('Effect', 'Path'); await page.click('.ce-aimenu--sub:not([hidden]) [data-xcmd="ops.offsetFx"]'); await page.waitForSelector('.ce-root .cops-card [data-dnum="mm"]');
      assert.equal(await page.inputValue('.ce-root .cops-card [data-dnum="mm"]'), '0.25', 'its own value');
      await page.fill('.ce-root .cops-card [data-dnum="mm"]', '0'); await page.keyboard.press('Enter'); await settle();
      assert.ok(Math.abs(await area() - a0) < a0 * 1e-6, 'back to the original at 0 mm: ' + (await area()) + ' / ' + a0);
      assert.ok(await C(c => !!c.entry('heart-fill').it.offsetFx));
      await openMenu('Object'); await page.click('.ce-aimenu:not([hidden]) > [data-xcmd="ops.expandFx"]'); await settle();
      assert.ok(await C(c => !c.entry('heart-fill').it.offsetFx), 'expanded');
    });
    await step('B9 no errors on the page', async () => { assert.deepEqual(errs, []); });
    await ctxB.close();
  } finally { await browser.close(); server.close(); }
  console.log(`ce-offset-path: ${n} checks passed${failures.length ? `, ${failures.length} FAILED: ${failures.join(' · ')}` : ''}`);
  if (failures.length) process.exitCode = 1;
})().catch(e => { console.error(e); process.exitCode = 1; });
