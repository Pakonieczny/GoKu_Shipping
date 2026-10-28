// REFIT (Paul, 28 Oct 2026, order 4213955608, a 12 x 14 mm CALENDAR_86188 with a hole: "When I clicked save button, my changes made in the Charm Editor were not saved and did not show up in
// the UI after pressing the Save button. I expect all of this to function like a native application. Everything must be immediate and responsive."). The edit WAS saved: the save sent the front to
// be fitted again, the work area stood empty for the 2.7 s the fit of "SEPTEMBER" took, and came back with the "21" he had written drawn at half strength among the master's own lines.
//   A  (node) the fit itself, on the real calendar: the very input the page sent its worker after the "21" was saved (fixtures/calendar-front-fit-input.json, captured from the page) and the
//      calendar master (fixtures/CALENDAR_86188.ai: its front "SEPTEMBER", its back "12:11 P.M.", in Ace with the shop's margins and stroke limits): the search answers exactly what it answered
//      before (sha-256 of the whole fit, recorded with the search as it was, 4c453be), and the time one fit takes is printed (CPU ms of this process, the least of 5: the work, not the wait)
//   B  the real sorter page over the repo's fake site (_engraveRealWorld.cjs: offline, made-up names): the calendar ordered "front - September / back - 12:11 p.m.", both faces approved, a "21"
//      written into a day square in the charm editor, Save. Every frame from the press until the new fit is drawn has a drawing in the work area: the drawing it had, dimmed, with "Fitting…" and
//      no handles, already showing the 21; then the new placement in one frame. The 21 is drawn at full strength, the master's art stays muted. The fit's time in the page's worker is printed.
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/charm-edit-refit.cjs      (REFIT_ONLY=A or B; SHOTS=<dir> for the screenshots)
'use strict';
const fs = require('node:fs'), path = require('node:path'), crypto = require('node:crypto');
const ROOT = path.join(__dirname, '../..'), FIX = path.join(__dirname, 'fixtures');
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const ONLY = process.env.REFIT_ONLY || '';
const sha = x => crypto.createHash('sha256').update(JSON.stringify(x)).digest('hex').slice(0, 20);

/* ═══ A · the fit: the same answer, in a fraction of the time ═══ */
// (recorded with the search as it was, 4c453be, on these inputs: Fit.calculate's fit, JSON, sha-256)
const BEFORE = {
  saved21: { sha: '03264da644052de9e996', size: 5.495845947265627, angle: -15, weight: 'Semibold' },   // the page's own input after Paul's "21" (2.7 s in the page's worker before)
  front: '7c34eaf640ff7aadab15',                                                                        // the master's front, "SEPTEMBER"
  back: 'a17b608f60e9d720d881'                                                                          // the master's back, "12:11 P.M."
};
async function partA() {
  global.window = global; global.self = global;
  const G = global.CharmNestGeom = require(path.join(ROOT, 'charm-nest-geom.js'));
  const Fit = require(path.join(ROOT, 'charm-nest-engrave-fit.js')), T = require(path.join(ROOT, 'charm-nest-text.js')), ot = require(path.join(ROOT, 'vendor/opentype-1.3.4.min.js'));
  global.PDFLib = require(path.join(ROOT, 'vendor/pdf-lib-1.17.1.min.js')); require(path.join(ROOT, 'charm-nest-pdf.js'));
  const fb = p => { const b = fs.readFileSync(path.join(ROOT, p)); return b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength); };
  // the fonts as the page's worker makes them: Ace (Jost Slashed Zero) with the emoji and Source Sans 3 behind it
  const emoji = ot.parse(fb('vendor/fonts/NotoEmoji-Regular.ttf')), map = require(path.join(ROOT, 'vendor/fonts/emoji-sequences.json')), sans = ot.parse(fb('vendor/fonts/SourceSans3-Regular.otf'));
  const ace = { Regular: T.withEmoji(ot.parse(fb('vendor/fonts/JostSlashedZero-Regular.ttf')), emoji, map, ot.Path, [sans]), Semibold: T.withEmoji(ot.parse(fb('vendor/fonts/JostSlashedZero-SemiBold.ttf')), emoji, map, ot.Path, [sans]) };
  // A1 · the page's own input, as its worker got it
  const inp = JSON.parse(fs.readFileSync(path.join(FIX, 'calendar-front-fit-input.json'), 'utf8')); inp.charm.outline = inp.charm.members[inp.charm.outlineAt]; delete inp.charm.outlineAt;
  const runs = []; let r = null;
  for (let i = 0; i < 7; i++) { const c0 = process.cpuUsage(), t0 = performance.now(); r = Fit.calculate(inp, ace, G); const c = process.cpuUsage(c0); runs.push({ cpu: (c.user + c.system) / 1000, wall: performance.now() - t0 }); }
  const warm = runs.slice(2), cpu = Math.min(...warm.map(x => x.cpu)), wall = Math.min(...warm.map(x => x.wall)), f = r.fit, b = BEFORE.saved21;
  check(`A1 Paul's calendar after the "21": "SEPTEMBER" is fitted exactly as before (${f.size.toFixed(4)} pt, ${f.angle}°, ${f.weight}, the whole fit byte for byte) in ${cpu.toFixed(0)} ms of work (${wall.toFixed(0)} ms wall; first ${runs[0].cpu.toFixed(0)} ms cold; it took 2.7 s in the page's worker, target ~300 ms)`,
    sha(f) === b.sha && f.size === b.size && f.angle === b.angle && f.weight === b.weight && cpu < 600, JSON.stringify({ sha: sha(f), size: f.size, angle: f.angle, weight: f.weight, runs: runs.map(x => Math.round(x.cpu)) }));
  // A2 · the master itself, both faces, as the shop sets them (margin 0.3 mm, strokes 0.15 / gaps 0.12 mm, turned candidates on)
  const P = global.CharmNestPDF, parsed = await P.parseSource(fs.readFileSync(path.join(FIX, 'CALENDAR_86188.ai')), 'CALENDAR_86188.ai'), c = P.groupCharms(parsed).charms[0];
  const charm = { outline: c.outline, members: c.members, bbox: c.bbox, widthPt: c.bbox[2] - c.bbox[0], heightPt: c.bbox[3] - c.bbox[1] };
  const opts = { minCapMm: 1.6, maxHeightFrac: .4, lineGap: .216, minStrokeMm: .15, minGapMm: .12, tryRotated: true };
  const faces = {};
  for (const [face, lines] of [['front', ['SEPTEMBER']], ['back', ['12:11 P.M.']]]) {
    const c0 = process.cpuUsage(); const out = Fit.calculate({ charm, lines, lineMode: 'auto', viewOptions: G.viewOptionsFor({ face }), maskOptions: { marginMm: .3, keepOut: [] }, opts }, ace, G); const u = process.cpuUsage(c0);
    faces[face] = { sha: sha(out.fit), size: +out.fit.size.toFixed(4), angle: out.fit.angle, ms: Math.round((u.user + u.system) / 1000) };
  }
  check(`A2 the calendar master: its front "SEPTEMBER" (${faces.front.size} pt, ${faces.front.ms} ms) and its back "12:11 P.M." (${faces.back.size} pt, ${faces.back.ms} ms) are fitted exactly as before`,
    faces.front.sha === BEFORE.front && faces.back.sha === BEFORE.back, JSON.stringify(faces));
  delete global.window; delete global.self;
}

/* ═══ B · the page: never blank, the person's own art drawn clearly ═══ */
const settleP = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
async function partB() {
  const R = require('./_engraveRealWorld.cjs');
  R.DESIGNS.CALENDAR_86188 = { file: 'CALENDAR_86188.ai', entry: { sku: 'CALENDAR_86188', widthPt: 33.68, heightPt: 39.3081, areaPt2: 1036.17, members: 15, holes: 1, engravable: true, engravableBy: 'index', upAngle: 89.9977, upSource: 'hole', charmHash: '60dde504', backKeepOut: [] } };
  R.ORDERS.cal = Object.assign({}, R.ORDERS.both, { rid: '4290000131', sku: 'CALENDAR_86188', title: 'Calendar Necklace, Personalized Month Calendar Charm', buyer: 'Name Eleven', personalization: ['front - September\nback - 12:11 p.m.'] });
  R.EXPECT_JOBS.cal = 2;
  const w = await R.world({ shots: process.env.SHOTS || '/tmp/charm-edit-refit-shots' });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page, st = w.srv.st, Q = '#egQueue .rvItem[data-kind=placement]';
  try {
    const made = await R.build(w, ['cal']), PID = made.cal.poolIds[0];
    const sheets = await page.evaluate(() => { const out = []; for (const s of allSheets()) if (s.charms && s.charms.length) { if (!s.folderPath) s.folderPath = 'charmnest/sheets/' + s.sheetId; out.push({ sheetId: s.sheetId, poolIds: s.charms.map(x => x.poolId) }); } return out; });
    for (const s of sheets) st.put('Charm_Nest_Sheets', s.sheetId, { sheetId: s.sheetId, poolIds: s.poolIds, backPool: [] });
    for (const face of ['front', 'back']) {   // (both faces approved first, as Paul's were)
      await R.openEngraveDetail(w, 'cal', { face }); await R.settle(page, 700);
      await page.click(Q + ' [data-a="approve"]');
      for (let t0 = Date.now(); Date.now() - t0 < 120000 && !st.doc('Charm_Pool_Back', PID + '~' + face); ) await new Promise(r => setTimeout(r, 300));
    }
    await R.openEngraveDetail(w, 'cal', { face: 'front' }); await R.settle(page, 900);
    const b0 = await page.evaluate(() => { const card = document.querySelector('#egQueue .rvItem[data-kind=placement]'), j = Engrave.items().get(card.dataset.key), cv = card.querySelector('.backHost canvas'); return { state: j.state, text: j.text, art: cv && cv._frontArt ? cv._frontArt.length : -1, own: cv && cv._frontArt ? cv._frontArt.filter(s => s.artAdded || s.artChanged).length : -1 }; });
    check('B0 the calendar\'s front "SEPTEMBER" is approved and its work area draws the master\'s day grid and digits, none of it a person\'s own', ['approved', 'written'].includes(b0.state) && b0.text === 'SEPTEMBER' && b0.art > 0 && b0.own === 0, JSON.stringify(b0));
    // the charm editor: "21" written into a day square (the third column of the fourth row of days), 3 pt
    await page.click(Q + ' .backHost .egCharmEdit');
    await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settleP(page); await settleP(page);
    const lines = await page.evaluate(() => CharmEditor.current.items().filter(i => i.role === 'line').map(i => CharmEditor.current.box(i.id)));
    const u = lines.reduce((a, b) => [Math.min(a[0], b[0]), Math.min(a[1], b[1]), Math.max(a[2], b[2]), Math.max(a[3], b[3])]);
    const at = await page.evaluate(([x, y]) => CharmEditor.current.clientOf(x, y), [u[0] + (u[2] - u[0]) * (1.3 / 7), u[1] + (u[3] - u[1]) * 0.75]);
    await page.keyboard.press('v'); await settleP(page); await page.keyboard.press('t');
    await page.mouse.click(at.x, at.y); await settleP(page);
    await page.keyboard.type('21'); await page.keyboard.press('Enter'); await settleP(page);
    if (await page.locator('.ce-root [data-f="pt"]').count()) { const fld = page.locator('.ce-root [data-f="pt"]'); await fld.click(); await page.keyboard.press('Control+a'); await page.keyboard.type('3'); await page.keyboard.press('Enter'); await settleP(page); }
    const typed = await page.evaluate(() => CharmEditor.current.items().filter(i => i.kind === 'text').map(i => i.text));
    await R.shot(w, 'REFIT-1-editor');
    // what every frame shows, read after the frame's own work and before it is painted (a ResizeObserver on a probe nudged every frame), the worker's fits, the long tasks
    await page.evaluate(() => {
      const L = window.__refit = { frames: [], fits: [], long: [] }, now = () => performance.now();
      const wp = Worker.prototype.postMessage;
      Worker.prototype.postMessage = function (m) { if (m && m.type === 'fit') { L.fits.push({ sent: now(), id: m.id }); if (!this.__refit) { this.__refit = 1; this.addEventListener('message', e => { const f = L.fits.find(x => x.id === (e.data && e.data.id)); if (f && !f.back) f.back = now(); }); } } return wp.apply(this, arguments); };
      try { new PerformanceObserver(l => { for (const e of l.getEntries()) L.long.push([e.startTime, e.duration]); }).observe({ entryTypes: ['longtask'] }); } catch (_) { /* not measured */ }
      const probe = document.createElement('div'); probe.style.cssText = 'position:fixed;left:0;top:0;width:1px;height:1px;opacity:0;pointer-events:none'; document.body.appendChild(probe);
      new ResizeObserver(() => {
        const card = document.querySelector('#egQueue .rvItem[data-kind=placement]'), host = card && card.querySelector('.backHost'), cv = host && host.querySelector('canvas'), j = card && Engrave.items().get(card.dataset.key);
        const own = cv && cv._frontArt ? cv._frontArt.filter(s => s.artAdded).length : 0;
        L.frames.push({ t: now(), state: j ? j.state : '-', drawn: cv ? (cv.classList.contains('egHeld') ? 'held' : 'real') : 'none', cue: !!(host && host.querySelector('.egRefitCue')), box: !!(cv && cv._box), own, fit: !!(j && j.fit), dim: cv ? getComputedStyle(cv).opacity : '' });
      }).observe(probe);
      let flip = false; const tick = () => { flip = !flip; probe.style.width = flip ? '2px' : '1px'; if (!L.stop) requestAnimationFrame(tick); }; requestAnimationFrame(tick);
      L.t0 = now();
    });
    await page.click('.ce-root [data-act="save"]');
    // until the new fit is drawn (the real drawing with the 21 in it), then a few frames more
    await page.waitForFunction(() => { const f = __refit.frames, i = f.findIndex(x => x.drawn === 'real' && x.state === 'review' && x.own > 0); return i >= 0 && f.length > i + 6; }, null, { timeout: 60000, polling: 100 }).catch(() => {});
    const L = await page.evaluate(() => { __refit.stop = true; return __refit; });
    const saved = st.doc('Charm_Pool_Art', PID + '~front'), fr = L.frames;
    const firstNew = fr.findIndex(x => x.drawn === 'real' && x.state === 'review' && x.own > 0), held = fr.filter(x => x.drawn === 'held'), none = fr.slice(0, firstNew < 0 ? fr.length : firstNew + 1).filter(x => x.drawn === 'none');
    const runs = []; let last = null; for (const x of fr) { const k = x.drawn + (x.drawn === 'real' ? (x.own ? '+21' : '') : ''); if (k !== last) { runs.push([Math.round(x.t - L.t0), k, x.state]); last = k; } }
    const fit = L.fits.filter(x => x.back).map(x => x.back - x.sent), fitMs = fit.length ? Math.round(Math.max(...fit)) : -1, drawnMs = firstNew >= 0 ? Math.round(fr[firstNew].t - L.t0) : -1;
    const longest = L.long.filter(x => x[0] >= L.t0).reduce((a, x) => Math.max(a, x[1]), 0);
    console.log(`   frames from the press: ${JSON.stringify(runs)}; ${fr.length} frames, ${held.length} held; the worker's fit ${fitMs} ms (${fit.length} fit${fit.length === 1 ? '' : 's'}); the longest main-thread task ${Math.round(longest)} ms`);
    check(`B1 the "21" is saved (${saved && saved.added ? saved.added.map(a => a.text).join() : '-'}) and from the press of Save until the new fit is drawn (${drawnMs} ms) every one of the ${firstNew + 1} frames has a drawing in the work area: none is blank`,
      typed.includes('21') && !!saved && (saved.added || []).some(a => a.text === '21') && firstNew > 0 && none.length === 0, JSON.stringify({ typed, saved: !!saved, firstNew, none: none.length, runs }));
    check(`B2 while it is fitted again (${held.length} frames) the work area holds the drawing it had, dimmed (opacity ${held.length ? held[0].dim : '-'}), with "Fitting…" and no handles to drag, and already shows the 21`,
      (held.length > 0 || fitMs < 34) && held.every(x => x.cue && !x.box && x.own > 0 && +x.dim < 1 && ['ready', 'fitting'].includes(x.state)), JSON.stringify(held.slice(0, 3)));
    const after = firstNew >= 0 ? fr[firstNew - 1] : null;
    check('B3 the new placement takes the held drawing\'s place in one frame: the frame before it shows the held drawing (or the approved one), never an empty work area', !!after && after.drawn !== 'none', JSON.stringify(after));
    // the 21 at full strength: its own box on the drawing holds the hatch blue itself; the master's red grid and blue digits are drawn, muted, and nowhere at full strength
    const ink = await page.evaluate(() => {
      const cv = document.querySelector('#egQueue .rvItem[data-kind=placement] .backHost canvas'), g = cv.getContext('2d'), d = g.getImageData(0, 0, cv.width, cv.height).data, M = cv._map;
      const mine = cv._frontArt.filter(s => s.artAdded), box = mine.reduce((a, s) => { const p = M.tx(s.bbox[0], s.bbox[1]), q = M.tx(s.bbox[2], s.bbox[3]); return [Math.min(a[0], p[0], q[0]), Math.min(a[1], p[1], q[1]), Math.max(a[2], p[0], q[0]), Math.max(a[3], p[1], q[1])]; }, [Infinity, Infinity, -Infinity, -Infinity]);
      const near = (i, c, tol) => Math.abs(d[i] - c[0]) <= tol && Math.abs(d[i + 1] - c[1]) <= tol && Math.abs(d[i + 2] - c[2]) <= tol;
      const BLUE = [30, 80, 220], RED = [214, 48, 40], TINT = [231, 237, 223], half = (c, under) => c.map((v, k) => Math.round((v + under[k]) / 2));   // (half strength: over the white, or over the tint of the metal the words may use)
      let inBlue = 0, outBlue = 0, fullRed = 0, halfRed = 0, halfBlue = 0;
      for (let y = 0; y < cv.height; y++) for (let x = 0; x < cv.width; x++) {
        const i = (y * cv.width + x) * 4, inside = x >= box[0] - 1 && x <= box[2] + 1 && y >= box[1] - 1 && y <= box[3] + 1;
        if (near(i, BLUE, 24)) { if (inside) inBlue++; else outBlue++; }
        if (near(i, RED, 24)) fullRed++;
        if (near(i, half(RED, [255, 255, 255]), 12) || near(i, half(RED, TINT), 12)) halfRed++;
        if (near(i, half(BLUE, [255, 255, 255]), 12) || near(i, half(BLUE, TINT), 12)) halfBlue++;
      }
      return { mine: mine.length, text: mine.map(s => s.artText).join(), box: box.map(Math.round), inBlue, outBlue, fullRed, halfRed, halfBlue, opacity: getComputedStyle(cv).opacity, held: cv.classList.contains('egHeld') };
    });
    await R.shot(w, 'REFIT-2-after-save');
    check(`B4 the 21 is drawn at full strength (${ink.inBlue} px of the hatch blue itself in its box), the master's art muted (${ink.halfRed} px of its red grid and ${ink.halfBlue} of its blue at half strength; ${ink.fullRed} red and ${ink.outBlue} blue px at full strength elsewhere)`,
      ink.mine === 1 && ink.text === '21' && ink.inBlue >= 12 && ink.outBlue === 0 && ink.fullRed === 0 && ink.halfRed > 0 && !ink.held && ink.opacity === '1', JSON.stringify(ink));
    check(`B5 the page's worker fits "SEPTEMBER" around the new art in ${fitMs} ms (2.7 s before; target ~300 ms) and the new drawing is on screen ${drawnMs} ms after the press`, fitMs > 0 && fitMs < 1500, JSON.stringify({ fits: L.fits }));
    console.log('   page errors:', JSON.stringify(w.errors || []).slice(0, 600));
  } finally { await w.close(); }
}

(async () => {
  if (!ONLY || ONLY === 'A') await partA();
  if (!ONLY || ONLY === 'B') await partB();
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\ncharm-edit-refit: ${RESULTS.length - bad.length} passed, ${bad.length} failed`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
