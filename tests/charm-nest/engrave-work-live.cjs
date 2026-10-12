// ENGWORK (engrave-ui-1012, round 2, Paul's points 1 and 2): "nothing seems to happen in real-time" and "The text in the work area does not show the preview of the selected text font style".
// The WORK AREA is the big canvas in the Engrave tab's detail (the charm on a 1 mm grid with the engraving and its selection box). Picking a font, typing, changing the mode or nudging the position must redraw the
// engraving there at once, in the chosen font, BEFORE Save, as a DRAFT that is never saved, approved or sent anywhere (charm-nest-bridge.js section 7.5b: Engrave.draftFit; the panel's one call site is `draftOut` in paint).
//
// REAL, not a stand-in: ENGLIVE's real-drawing world (tests/charm-nest/_engraveRealWorld.cjs): the real sorter page in headless Chromium over the repo's fake site, the real charm drawings of the .ai fixtures through the
// real intake, pool, classifier and fit worker, so each job has a real view, mask and fit and the work area draws a real charm. Nothing live, no Etsy, no paid reading. Names are made up.
//
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/engrave-work-live.cjs        (WORK_SHOTS=<dir> saves screenshots; default /mnt/project-files/plans/engrave-ui-1012/round2/images when it exists)
//
//  A  the committed state: the work area draws the real fit; the laser file (built from fit.glyphs exactly as the approval writes it) is the reference
//  B  FONT: font A then font B in the panel's drop-down: in the SAME tick the work area's drawn glyph path changes, differs between the two fonts and from the committed one, is that installed font's own outline,
//     and the canvas shows exactly that path
//  C  TYPING: new words change the drawn path in the same tick, in the staged font
//  D  MODE: a mode that does not engrave this face draws no engraving at once; the mode back to this face draws the staged words again
//  G  nothing leaves the page during the draft and nothing of it is on the job (no request, no cloud call, no paid look, job and laser file untouched)
//  E  POSITION: a position button nudges the job at once and the staged words move with it
//  F  CANCEL: the committed fit is drawn again, byte for byte (canvas, fit, the whole job, the laser file)
//  H  a font that is still loading keeps the previous drawing and shows the small "Loading font" spinner; the draft is made as soon as it has loaded
//  J  SAVE: the staged words in the staged font are what Save fits (same glyph shapes); the draft is gone after it
//  I  a FRONT job drafts too, and the staged mode decides whether it is engraved
//  K  the API never throws
'use strict';
const fs = require('fs'), path = require('path');
let R; try { R = require('./_engraveRealWorld.cjs'); } catch (e) { console.log('engrave-work-live: tests/charm-nest/_engraveRealWorld.cjs (ENGLIVE) is not here, skipped'); process.exit(0); }
const SHOT_DIR = process.env.WORK_SHOTS || (fs.existsSync('/mnt/project-files/plans/engrave-ui-1012/round2/images') ? '/mnt/project-files/plans/engrave-ui-1012/round2/images' : '');

(async () => {
  const w = await R.world({ viewport: { width: 1440, height: 900 }, shots: SHOT_DIR || undefined });
  if (!w) { console.log('engrave-work-live: playwright-core not found, skipped'); return; }
  let fails = 0; const check = (ok, msg) => { if (!ok) fails++; console.log((ok ? '  ok   ' : '  FAIL ') + msg); };
  const shot = async name => { if (SHOT_DIR) { try { await R.shot(w, name); } catch (e) { console.log('  (no screenshot ' + name + ': ' + e.message + ')'); } } };
  const page = w.page;
  try {
    const made = await R.build(w, ['back', 'front']);
    const backJob = made.back.jobs.find(j => j.face === 'back'), frontJob = made.front.jobs.find(j => j.face === 'front');
    check(backJob && backJob.state === 'review' && backJob.fitted && backJob.hasView && backJob.hasMask, 'the world has a REAL back job: view, mask and a fitted engraving (' + (backJob && backJob.fontKey) + ', "' + (backJob && backJob.text) + '")');
    check(frontJob && frontJob.state === 'review' && frontJob.fitted, 'and a REAL front job (' + (frontJob && frontJob.fontKey) + ', "' + (frontJob && frontJob.text) + '")');
    if (!backJob || !frontJob) throw new Error('no real jobs to test with');
    const JK = backJob.key, FK = frontJob.key;

    /* helpers that live in the page: the laser file as the approval writes it, a stable view of a job, the work area's canvas and what it shows */
    await page.evaluate(() => {
      const hex = async b => [...new Uint8Array(await crypto.subtle.digest('SHA-256', b))].map(x => x.toString(16).padStart(2, '0')).join('');
      window.__laser = async key => {
        const job = Engrave.items().get(key), view = job.view, fit = job.fit, charm = Pool.charmOf(job.copies[0]), src = sourceOf(charm.sourceId), PTm = 72 / 25.4, front = job.face === 'front';
        const rel = fit.glyphs.map(g => ({ cmds: g.cmds.map(c => { const o = { type: c.type }; if (c.type !== 'Z') { o.x = c.x - view.cx; o.y = c.y - view.cy; } if (c.type === 'C' || c.type === 'Q') { o.x1 = c.x1 - view.cx; o.y1 = c.y1 - view.cy; } if (c.type === 'C') { o.x2 = c.x2 - view.cx; o.y2 = c.y2 - view.cy; } return o; }) }));
        const RD = Date, FIX = RD.UTC(2026, 9, 12, 12), RR = Math.random; let seed = 20261012;   // (pdf-lib stamps dates and random names: nothing else varies a file)
        window.Date = class extends RD { constructor(...a) { if (a.length) super(...a); else super(FIX); } static now() { return FIX; } };
        Math.random = () => { seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed / 4294967296; };
        try { const built = await CharmNestPDF.buildBackFile({ charm, mirrored: !!charm.mirrored, parsed: src.parsed, cutMembers: view.cutMembers, cx: view.cx, cy: view.cy, angleDeg: view.angleDeg, padPt: 5 * PTm, glyphs: rel, ...(front ? { face: 'front' } : {}), view: front ? 'asSeenFromFront' : 'asSeenFromBack', title: job.key, meta: { poolId: job.copies[0] } }); return await hex(built.bytes); }
        finally { window.Date = RD; Math.random = RR; }
      };
      const r3 = v => Math.round(v * 1000) / 1000;
      window.__path = glyphs => JSON.stringify(glyphs.map(g => g.cmds.map(c => [c.type, c.x, c.y, c.x1, c.y1, c.x2, c.y2].map(v => typeof v === 'number' ? r3(v) : v))));
      // the job as everything that could reach a file, the checkpoint or the cloud sees it (the verify stamp's clock aside)
      window.__job = key => { const j = Engrave.items().get(key); return JSON.stringify({ state: j.state, text: j.text, lines: j.lines, lineInput: j.lineInput, lineMode: j.lineMode, wantSize: j.wantSize, nudged: j.nudged, fontKey: j.fontKey, faceFontWord: j.faceFontWord || null, fontAsked: j.fontAsked || null, claude: j.claude || null, verify: j.verify && j.verify.geometry, fit: j.fit && { centre: j.fit.centre, angle: j.fit.angle, size: j.fit.size, weight: j.fit.weight, capMm: j.fit.capMm, lines: j.fit.lines, path: __path(j.fit.glyphs) } }); };
      window.__cv = () => document.querySelector('#egQueue .backHost canvas');
      // what the work area shows, read in the same tick as whatever the caller just did
      window.__drawn = key => { const d = Engrave.draftOf(key), bc = __cv(), now = bc.toDataURL(); let sameAsPath = null; if (d) { bc._paint({ glyphs: d.glyphs, centre: d.centre, angle: d.angle }); sameAsPath = bc.toDataURL() === now; bc._paint(); } return { draft: d && { fontId: d.fontId, lines: d.lines, empty: d.empty, path: __path(d.glyphs), centre: d.centre, angle: d.angle, size: d.size }, canvas: now, sameAsPath, note: (document.querySelector('.engwNote') || {}).textContent || '' }; };
      // everything that could leave the page: fetch, XHR, and the page's own cloud entry points (every paid read, Firebase call and save goes through one)
      window.__sink = { req: 0, calls: [] };
      const f0 = window.fetch; window.fetch = function () { __sink.req++; return f0.apply(this, arguments); };
      const x0 = XMLHttpRequest.prototype.open; XMLHttpRequest.prototype.open = function () { __sink.req++; return x0.apply(this, arguments); };
      if (window.DesignLink && DesignLink.call) { const c0 = DesignLink.call; DesignLink.call = function (n) { __sink.calls.push(String(n)); return c0.apply(this, arguments); }; }
      if (window.CN && CN.api) { const a0 = CN.api; CN.api = function (n) { __sink.calls.push('api:' + String(n)); return a0.apply(this, arguments); }; }
    });
    const requests = []; let watching = false;
    page.on('request', r => { if (watching) requests.push(r.method() + ' ' + r.url().replace(/^https?:\/\/[^/]+/, '')); });
    const sink = () => page.evaluate(() => ({ req: __sink.req, calls: __sink.calls.slice() }));

    /* ═══ A · the committed state ═══ */
    const idOf = word => page.evaluate(wd => CharmNestOrders.fontOfPhrase(wd), word);
    const FA = 'Pristina', FB = 'Angelina', idA = await idOf(FA), idB = await idOf(FB);
    await R.openEngraveDetail(w, 'back', { face: 'back' });
    await page.waitForSelector('#egQueue .backHost canvas', { timeout: 30000 });
    await page.evaluate(async ids => { for (const id of ids) await Engrave.loadFontSet(id); }, [idA, idB]);   // (the fonts parts B..E pick are loaded; the "still loading" case is H)
    const base = await page.evaluate(async key => ({ job: __job(key), canvas: __cv().toDataURL(), laser: await __laser(key), drawn: __drawn(key), path: __path(Engrave.items().get(key).fit.glyphs), font: Engrave.items().get(key).fontKey, text: Engrave.items().get(key).text }), JK);
    check(base.drawn.draft === null && /^[0-9a-f]{64}$/.test(base.laser), 'A1 nothing staged: the work area draws the fitted engraving (no draft); laser file ' + base.laser.slice(0, 10));
    check(await page.evaluate(() => !!document.querySelector('#egQueue .pvSetup .engp .engpFont') && !!document.querySelector('#egQueue .pvSetup .engp .engpWords')), 'A2 the Engrave detail has the real panel (words box, font drop-down) beside the work area');
    await shot('WORK-1-committed');

    /* ═══ B · FONT: A then B ═══ */
    watching = true; const sink0 = await sink();
    const pick = word => page.evaluate(({ key, word }) => {   // ONE tick: the pick and what the work area shows right after it
      const sel = document.querySelector('#egQueue .pvSetup .engp .engpFont'), t0 = performance.now(); sel.value = word; sel.dispatchEvent(new Event('change', { bubbles: true }));
      const out = __drawn(key); out.ms = performance.now() - t0; return out;
    }, { key: JK, word });
    const a = await pick(FA);
    check(a.draft && !a.draft.empty && a.draft.fontId === idA, `B1 picking ${FA}: the same tick the work area has a draft in ${idA}: ${a.draft && a.draft.fontId} (${Math.round(a.ms)} ms)`);
    check(a.canvas !== base.canvas && a.draft.path !== base.path, `B2 the drawn path and the canvas are no longer the committed engraving's (${base.font})`);
    check(a.sameAsPath === true, 'B3 the canvas shows exactly the draft glyph path at the draft place (a provisional repaint of draftOf() is pixel-identical)');
    await shot('WORK-2-font-' + FA.toLowerCase());
    const b = await pick(FB);
    check(b.draft && b.draft.fontId === idB && b.draft.path !== a.draft.path && b.canvas !== a.canvas && b.sameAsPath === true, `B4 picking ${FB} next: path and canvas change at once and differ from ${FA}'s (${idB} vs ${idA})`);
    await shot('WORK-3-font-' + FB.toLowerCase());
    // the shapes ARE the installed font's outlines: the ink's width (turned back level) is opentype's own path of that font file at that size
    const shape = key => page.evaluate(key => {
      const d = Engrave.draftOf(key), job = Engrave.items().get(key), set = Engrave.fontSetFor(d.fontId), font = (job.fit.weight === 'Semibold' && set.Semibold) || set.Regular;
      const a = -(d.angle || 0) * Math.PI / 180, ca = Math.cos(a), sa = Math.sin(a); let x0 = 1e9, x1 = -1e9;
      for (const g of d.glyphs) for (const c of g.cmds) { if (c.type === 'Z') continue; const lx = (c.x - d.centre[0]) * ca - (c.y - d.centre[1]) * sa; if (lx < x0) x0 = lx; if (lx > x1) x1 = lx; }
      const own = font.getPath(d.lines[0], 0, 0, d.size, { kerning: true }).getBoundingBox();
      return { drawn: x1 - x0, own: own.x2 - own.x1, lines: d.lines };
    }, key);
    const sb = await shape(JK);
    check(sb.lines.length === 1 && Math.abs(sb.drawn - sb.own) < 0.02, `B5 the drawn ink is the font's own outline: ${sb.drawn.toFixed(3)} pt wide, opentype's own path of that font file ${sb.own.toFixed(3)} pt`);

    /* ═══ C · TYPING ═══ */
    const typeIn = text => page.evaluate(({ key, text }) => {
      const ta = document.querySelector('#egQueue .pvSetup .engp .engpWords'), t0 = performance.now(); ta.value = text; ta.dispatchEvent(new Event('input', { bubbles: true }));
      const out = __drawn(key); out.ms = performance.now() - t0; return out;
    }, { key: JK, text });
    const c1 = await typeIn('Name Two');
    check(c1.draft && c1.draft.lines.join(' ') === 'Name Two' && c1.draft.fontId === idB && c1.draft.path !== b.draft.path && c1.canvas !== b.canvas && c1.sameAsPath === true, `C1 typing "Name Two": the same tick the work area draws those words in ${idB} (${Math.round(c1.ms)} ms)`);
    const c2 = await typeIn('Name Two Three');
    check(c2.draft && c2.draft.lines.join(' ') === 'Name Two Three' && c2.draft.path !== c1.draft.path && c2.canvas !== c1.canvas, `C2 typing on: the drawing follows each change (${c2.draft.lines.length} line(s), ${c2.draft.size.toFixed(2)} pt)`);
    const c3 = await typeIn('X');
    check(c3.draft && c3.draft.lines.join('') === 'X' && c3.draft.path !== c2.draft.path, 'C3 deleting back to one letter redraws at once');
    const c4 = await typeIn('Name Two');
    check(c4.canvas === c1.canvas, 'C4 the same words give the same drawing again (it is a pure function of the staged edit)');
    await shot('WORK-4-typing');

    /* ═══ D · MODE ═══ */
    const modeTo = (key, m) => page.evaluate(({ key, m }) => {
      const t0 = performance.now(); document.querySelector(`#egQueue .pvSetup .engp [data-engp=mode][data-mode=${m}]`).click();
      const out = __drawn(key); out.ms = performance.now() - t0; return out;
    }, { key, m });
    const ink = url => page.evaluate(async u => { const im = new Image(); im.src = u; await im.decode(); const cv = document.createElement('canvas'); cv.width = im.width; cv.height = im.height; const c = cv.getContext('2d'); c.drawImage(im, 0, 0); const d = c.getImageData(0, 0, im.width, im.height).data; let n = 0; for (let i = 0; i < d.length; i += 4) if (d[i] < 60 && d[i + 1] < 60 && d[i + 2] < 60) n++; return n; }, url);
    const d1 = await modeTo(JK, 'front');
    check(d1.draft && d1.draft.empty === true && d1.canvas !== c4.canvas, 'D1 mode "Front" while this is the back: the work area shows no engraving at once (the back would be cut plain)');
    const ink4 = await ink(c4.canvas), ink1 = await ink(d1.canvas);
    check(ink1 < ink4, `D2 fewer dark pixels without the engraving: ${ink1} < ${ink4}`);
    await shot('WORK-5-mode-front-only');
    const d2 = await modeTo(JK, 'none');
    check(d2.draft && d2.draft.empty === true, 'D3 mode "None" draws no engraving either');
    const d3 = await modeTo(JK, 'both');
    check(d3.draft && !d3.draft.empty && d3.draft.lines.join(' ') === 'Name Two' && d3.draft.fontId === idB && d3.sameAsPath === true, 'D4 mode "Front + back" engraves the back again: the staged words in the staged font');
    const d4 = await modeTo(JK, 'back');
    check(d4.draft && !d4.draft.empty && d4.canvas === c4.canvas, 'D5 mode "Back" draws exactly what it drew before the mode was touched');

    /* ═══ G · nothing left the page, nothing of the draft is on the job ═══ */
    const sink1 = await sink(); watching = false;
    check(sink1.req === sink0.req && sink1.calls.length === sink0.calls.length && requests.length === 0, `G1 through two font picks, typing, four mode changes: no fetch, no XHR, no cloud call, no request at all: ${JSON.stringify({ fetch: sink1.req - sink0.req, calls: sink1.calls.slice(sink0.calls.length), requests: requests.slice(0, 5) })}`);
    const jobDuring = await page.evaluate(key => __job(key), JK);
    check(jobDuring === base.job, 'G2 the job is byte-identical while a draft is staged: words, font, size, nudged flag, engraving path, verify, no paid look (job.claude empty)');
    check(await page.evaluate(async () => { let leak = false; for (const j of Engrave.items().values()) { try { if (JSON.stringify(j, (k, v) => (['row', 'view', 'mask', 'editCharm', 'editSheet', 'charm'].includes(k) ? undefined : v)).includes('"Name Two"')) leak = true; } catch (_) { /* a cycle in a heavy key */ } } return !leak; }), 'G3 the staged words are on no job (the draft lives only in the bridge\'s DRAFTS: the session checkpoint copies jobs, so it cannot carry a draft)');
    check(await page.evaluate(async key => (await __laser(key)) === arguments[0] || true, JK) && (await page.evaluate(key => __laser(key), JK)) === base.laser, 'G4 the laser file built from the job is byte-identical while the draft is on screen');

    /* ═══ E · POSITION ═══ */
    const MM = 72 / 25.4;
    const move = act => page.evaluate(({ key, act }) => {
      const j = Engrave.items().get(key), before = j.fit.centre.slice(), d0 = Engrave.draftOf(key), t0 = performance.now();
      document.querySelector(`#egQueue .pvSetup .engp [data-act=${act}]`).click();
      const out = __drawn(key); out.ms = performance.now() - t0; out.job = j.fit.centre.slice(); out.jobBefore = before; out.draftBefore = d0 && d0.centre; return out;
    }, { key: JK, act });
    const e1 = await move('right');
    const jdx = e1.job[0] - e1.jobBefore[0], ddx = e1.draft.centre[0] - e1.draftBefore[0];
    check(Math.abs(jdx - 0.5 * MM) < 1e-6 && Math.abs(ddx - jdx) < 1e-6 && Math.abs(e1.draft.centre[1] - e1.draftBefore[1]) < 1e-6, `E1 "right": the job moved 0.5 mm (${jdx.toFixed(4)} pt) and the staged words moved with it in the same tick (${ddx.toFixed(4)} pt)`);
    check(e1.draft.path !== c4.draft.path && e1.canvas !== c4.canvas && e1.sameAsPath === true && e1.draft.lines.join(' ') === 'Name Two' && e1.draft.fontId === idB, 'E2 the work area redrew at once, still the staged words in the staged font');
    const e2 = await move('up');
    check(Math.abs((e2.draft.centre[1] - e1.draft.centre[1]) - 0.5 * MM) < 1e-6 && Math.abs(e2.job[1] - e1.job[1] - 0.5 * MM) < 1e-6, 'E3 "up" moves the job and the staged words 0.5 mm up together');
    check((await sink()).calls.every(n => !/engraveReview|claude|agent/i.test(n)), 'E4 a position button asks for no paid look: cloud calls so far ' + JSON.stringify((await sink()).calls.slice(sink0.calls.length)));
    await shot('WORK-6-position');

    /* ═══ F · CANCEL ═══ */
    const cancel = await page.evaluate(async key => {
      const t0 = performance.now(); document.querySelector('#egQueue .pvSetup .engp [data-engp=cancel]').click();
      const out = __drawn(key); out.ms = performance.now() - t0; out.job = __job(key); out.laser = await __laser(key); out.path = __path(Engrave.items().get(key).fit.glyphs);
      out.words = document.querySelector('#egQueue .pvSetup .engp .engpWords').value; out.font = document.querySelector('#egQueue .pvSetup .engp .engpFont').value; return out;
    }, JK);
    check(cancel.draft === null && cancel.words === base.text, `F1 Cancel: no draft any more, the panel is back to the saved words "${cancel.words}"`);
    check(cancel.canvas === base.canvas, 'F2 Cancel: the work area is the committed drawing again, pixel for pixel (the canvas before the first pick equals the canvas after Cancel)');
    check(cancel.path === base.path && cancel.job === base.job, 'F3 Cancel: the fit\'s glyph path and the whole job (position put back too) are byte-identical to before the first pick');
    check(cancel.laser === base.laser, `F4 Cancel: the laser file built from the fit is byte-identical to before (${cancel.laser.slice(0, 10)})`);
    await shot('WORK-7-cancel');

    /* ═══ H · a font that is still loading ═══ */
    const idT = await idOf('Typewriter'), fontFile = await page.evaluate(id => Object.values(CharmNestOrders.fontById(id).files)[0], idT);
    check(idT !== 'source-sans-3' && await page.evaluate(id => !Engrave.fontSetFor(id) || Engrave.fontSetFor(id) === Engrave.fonts, idT), `H0 ${idT} (Typewriter) is not loaded yet (${fontFile})`);
    let release; const gate = new Promise(r => { release = r; });
    const heldFont = u => u.pathname.endsWith('/' + fontFile.split('/').pop());
    await page.route(heldFont, async route => { await gate; await route.continue(); });   // (the font file is held until the check has looked at the page)
    const h1 = await page.evaluate(({ key }) => {
      const sel = document.querySelector('#egQueue .pvSetup .engp .engpFont'), before = __cv().toDataURL(); sel.value = 'Typewriter'; sel.dispatchEvent(new Event('change', { bubbles: true }));
      const out = __drawn(key); out.before = before; const n = document.querySelector('.engwNote'); out.spinner = !!(n && n.querySelector('.owSpin')); out.role = n && n.getAttribute('role'); return out;
    }, { key: JK });
    check(h1.canvas === h1.before && h1.draft === null, 'H1 the font is still being fetched: the previous drawing stays (nothing blocks, nothing goes blank)');
    check(h1.note === 'Loading font' && h1.spinner && h1.role === 'status', `H2 the small labelled spinner is there: "${h1.note}"`);
    await shot('WORK-8-loading-font');
    check(await page.evaluate(() => { const ta = document.querySelector('#egQueue .pvSetup .engp .engpWords'); ta.value = 'Name Four'; ta.dispatchEvent(new Event('input', { bubbles: true })); return !!document.querySelector('.engwNote'); }), 'H3 typing while the font loads works and the spinner stays');
    release();
    await page.waitForFunction(key => { const d = Engrave.draftOf(key); return d && !d.empty && d.lines.join(' ') === 'Name Four' && !document.querySelector('.engwNote'); }, JK, { timeout: 90000 });
    const h2 = await page.evaluate(key => __drawn(key), JK);
    check(h2.draft.fontId === idT && h2.draft.lines.join(' ') === 'Name Four' && h2.sameAsPath === true && h2.note === '' && h2.canvas !== base.canvas, `H4 once ${idT} has loaded the staged words are drawn in it by themselves and the spinner is gone`);
    await page.unroute(heldFont).catch(() => {});
    await page.evaluate(() => document.querySelector('#egQueue .pvSetup .engp [data-engp=cancel]').click());
    const afterH = await page.evaluate(async key => ({ draft: Engrave.draftOf(key), canvas: __cv().toDataURL(), laser: await __laser(key), job: __job(key) }), JK);
    check(afterH.draft === null && afterH.canvas === base.canvas && afterH.laser === base.laser && afterH.job === base.job, 'H5 Cancel after the loading case: the committed drawing, the job and the laser file are as at the start');

    /* ═══ J · SAVE: what is drawn is what Save fits ═══ */
    const staging = await page.evaluate(({ key }) => {
      const sel = document.querySelector('#egQueue .pvSetup .engp .engpFont'), ta = document.querySelector('#egQueue .pvSetup .engp .engpWords');
      ta.value = 'Name'; ta.dispatchEvent(new Event('input', { bubbles: true })); sel.value = 'Angelina'; sel.dispatchEvent(new Event('change', { bubbles: true }));
      const d = Engrave.draftOf(key); return { lines: d.lines, fontId: d.fontId, size: d.size, angle: d.angle, centre: d.centre, glyphs: JSON.parse(JSON.stringify(d.glyphs)) };
    }, { key: JK });
    check(staging.lines.join(' ') === 'Name' && staging.fontId === idB, `J1 staged: "Name" in ${idB}`);
    await page.evaluate(() => { document.querySelector('#egQueue .pvSetup .engp [data-engp=save]').click(); });
    await page.waitForFunction(({ key, id }) => { const j = Engrave.items().get(key); return j && j.state === 'review' && j.fit && j.fontKey === id && j.lines.join(' ') === 'Name'; }, { key: JK, id: idB }, { timeout: 150000 });
    await page.waitForSelector('#egQueue .backHost canvas', { timeout: 30000 });
    await page.waitForFunction(() => { const s = document.querySelector('#egQueue .pvSetup .engp [data-engp=save]'); return !s || s.disabled; }, null, { timeout: 30000 }).catch(() => {});
    const levelShape = (glyphs, centre, angle, size) => { const a = -(angle || 0) * Math.PI / 180, ca = Math.cos(a), sa = Math.sin(a), out = []; for (const g of glyphs) for (const c of g.cmds) { if (c.type === 'Z') { out.push('Z'); continue; } for (const [x, y] of [[c.x, c.y], c.x1 != null ? [c.x1, c.y1] : null, c.x2 != null ? [c.x2, c.y2] : null].filter(Boolean)) { const dx = x - centre[0], dy = y - centre[1]; out.push([(dx * ca - dy * sa) / size, (dx * sa + dy * ca) / size]); } } return out; };
    const saved = await page.evaluate(key => { const j = Engrave.items().get(key); return { fontKey: j.fontKey, faceFontWord: j.faceFontWord, lines: j.fit.lines, draft: Engrave.draftOf(key), glyphs: JSON.parse(JSON.stringify(j.fit.glyphs)), centre: j.fit.centre, angle: j.fit.angle, size: j.fit.size, canvas: __cv().toDataURL() }; }, JK);
    check(saved.fontKey === idB && saved.faceFontWord === 'Angelina' && saved.draft === null, `J2 after Save the job is fitted in ${saved.fontKey} (the person's pick) and no draft is left`);
    const p = levelShape(staging.glyphs, staging.centre, staging.angle, staging.size), q = levelShape(saved.glyphs, saved.centre, saved.angle, saved.size);
    let same = p.length === q.length && p.length > 4 && saved.lines.join('|') === staging.lines.join('|');
    if (same) for (let i = 0; i < p.length; i++) if ((p[i] === 'Z') !== (q[i] === 'Z') || (p[i] !== 'Z' && (Math.abs(p[i][0] - q[i][0]) > 2e-3 || Math.abs(p[i][1] - q[i][1]) > 2e-3))) { same = false; break; }
    check(same, `J3 the glyph SHAPES drawn before Save are exactly the ones Save fitted and would cut (${p.length} points, size-normalised, turned level)`);
    await shot('WORK-9-saved');

    /* ═══ I · a FRONT job ═══ */
    await R.openEngraveDetail(w, 'front', { face: 'front' });
    await page.waitForSelector('#egQueue .backHost canvas', { timeout: 30000 });
    const fb = await page.evaluate(async key => ({ canvas: __cv().toDataURL(), laser: await __laser(key), job: __job(key) }), FK);
    const front1 = await page.evaluate(({ key }) => { const ta = document.querySelector('#egQueue .pvSetup .engp .engpWords'); ta.value = 'Name Five'; ta.dispatchEvent(new Event('input', { bubbles: true })); return __drawn(key); }, { key: FK });
    check(front1.draft && front1.draft.lines.join(' ') === 'Name Five' && front1.canvas !== fb.canvas && front1.sameAsPath === true, 'I1 a FRONT job drafts too: typing redraws the front\'s work area at once');
    const front2 = await modeTo(FK, 'back');
    check(front2.draft && front2.draft.empty === true, 'I2 mode "Back" while this is the front: no front engraving is drawn');
    const front3 = await modeTo(FK, 'both');
    check(front3.draft && !front3.draft.empty && front3.draft.lines.join(' ') === 'Name Five', 'I3 mode "Front + back" draws the front\'s staged words');
    const fcancel = await page.evaluate(async key => { document.querySelector('#egQueue .pvSetup .engp [data-engp=cancel]').click(); return { canvas: __cv().toDataURL(), laser: await __laser(key), job: __job(key), draft: Engrave.draftOf(key) }; }, FK);
    check(fcancel.draft === null && fcancel.canvas === fb.canvas && fcancel.laser === fb.laser && fcancel.job === fb.job, 'I4 Cancel on the front: the committed front, its job and its laser file are byte-identical to before');

    /* ═══ K · the API's edges ═══ */
    const edge = await page.evaluate(key => {
      const r = {}; try { r.none = Engrave.draftFit('no-such-job', { text: 'x' }); r.nullStaged = Engrave.draftFit(key, null); r.garbage = Engrave.draftFit(key, 'x'); r.same = Engrave.draftFit(key, { text: Engrave.items().get(key).lines.join('\n'), fontWord: '', mode: '' }); r.draftOfNone = Engrave.draftOf('no-such-job'); } catch (e) { r.threw = e.message; }
      return r;
    }, FK);
    check(!edge.threw && edge.none.ok === false && edge.nullStaged.ok === true && edge.garbage.ok === true && edge.same.same === true && edge.draftOfNone === null, 'K1 the API never throws: an unknown job, null, junk, and the words the face already has (no draft): ' + JSON.stringify(edge));
    check(w.errors.length === 0, 'no page error: ' + JSON.stringify(w.errors.slice(0, 3)));
  } catch (e) { fails++; console.log('  FAIL the test stopped: ' + (e && e.stack || e)); }
  finally { await w.close(); }
  console.log(fails ? `\nengrave-work-live: ${fails} FAILED` : '\nengrave-work-live: all passed');
  process.exit(fails ? 1 : 0);
})();
