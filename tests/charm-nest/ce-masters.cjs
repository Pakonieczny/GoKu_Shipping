// DESIGNING A CHARM (charm-nest-ce-masters.js; DESIGN & ENGRAVE, Paul, 28 Oct 2026, items 16 and 10: "Users also need the ability to search and open any charm existing in our charm
// repository and have full control over manipulating that charm" ... "designing custom charms").
//   A  (node) the starting outlines are the size asked for (their TIGHT box, to a thousandth of a mm), the jump ring hole keeps its wall to the edge, a hole too big for a small charm is
//      refused, the design made from the editor's result leaves the back's guide out and measures the cut, and the search over a 7,000-charm library ranks the SKU first and is quick
//   B  the real sorter page over the repo's fake site (_engraveRealWorld.cjs: the master library holding the fixture drawings; offline, nothing real written, made-up names), with stand-ins
//      for the design library (CharmNestDesigns), the Send to sheet pop-up (CharmNestCustomPiece) and the employee log (CNDesignLog), which other workstreams build:
//      the search is answered from what the page holds (no call per keystroke, typing in the picker paints within a frame over a 7,000-charm library); a master opened from the picker
//      comes up in the Charm Editor with every part of its drawing a layer, none locked; a petal deleted and a part nudged, Save makes version 1 of a design (source master, its SKU),
//      the master and every piece's artwork record untouched; Keep editing, another change, Save makes version 2 of the SAME design; Send to sheet hands that version on; a blank
//      heart 20 × 18 mm with a jump ring hole opens as its outline and hole (the cut), saved as a new design of that size; and a master opened from inside a changed editor asks first
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/ce-masters.cjs      (CEM_ONLY=A or B; SHOTS=<dir> keeps screenshots)
'use strict';
const path = require('node:path'), fs = require('node:fs');
const ROOT = path.join(__dirname, '../..');
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}${evidence ? '  ' + evidence : ''}`); };
const ONLY = process.env.CEM_ONLY || '';
const MM = 25.4 / 72, PT = 72 / 25.4;

/* ═══ A · the model ═══ */
function partA() {
  const C = require(path.join(ROOT, 'charm-nest-ce-masters.js')), M = require(path.join(ROOT, 'charm-nest-charm-editor.js')).model, X = C.model;
  // A1 every outline's tight box is the size asked for
  let worst = 0; const seen = [];
  for (const shape of ['circle', 'rounded', 'heart', 'tag']) for (const [w, h] of [[10, 10], [20, 18], [32.5, 41]]) {
    const f = X.startFace({ shape, wMm: w, hMm: h }), o = f.items.find(i => i.id === 'outline'), b = M.bboxOf(o.subpaths);
    const want = shape === 'circle' ? [w, w] : [w, h];
    worst = Math.max(worst, Math.abs((b[2] - b[0]) * MM - want[0]), Math.abs((b[3] - b[1]) * MM - want[1]), Math.abs(b[0]), Math.abs(b[1]));
    seen.push(o.role === 'cut' && f.frame.x0 === 0 && Math.abs(f.frame.wPt * MM - want[0]) < 1e-9);
  }
  const free = X.startFace({ shape: 'free', wMm: 22, hMm: 30, hole: true });
  check('A1 a new design\'s outline (circle, rounded rectangle, heart, tag) is the cut and its tight box IS the size asked for; "your own outline" is the frame alone',
    worst < 1e-3 && seen.every(Boolean) && free.items.length === 0 && Math.abs(free.frame.wPt * MM - 22) < 1e-9 && Math.abs(free.frame.hPt * MM - 30) < 1e-9, `worst ${worst.toExponential(2)} mm`);
  // A2 the hole: inside the outline, a wall of at least 1 mm round it (the outline flattened finely; the distance from the hole's edge to the nearest point of the outline)
  const dist = (px, py, pts) => { let d = Infinity; for (let i = 0; i + 3 < pts.length; i += 2) { const ax = pts[i], ay = pts[i + 1], bx = pts[i + 2], by = pts[i + 3], dx = bx - ax, dy = by - ay, L = dx * dx + dy * dy; let t = L ? ((px - ax) * dx + (py - ay) * dy) / L : 0; t = Math.max(0, Math.min(1, t)); d = Math.min(d, Math.hypot(px - ax - t * dx, py - ay - t * dy)); } return d; };
  const inside = (px, py, pts) => { let c = false; for (let i = 0, j = pts.length - 2; i < pts.length; j = i, i += 2) { const xi = pts[i], yi = pts[i + 1], xj = pts[j], yj = pts[j + 1]; if ((yi > py) !== (yj > py) && px < (xj - xi) * (py - yi) / (yj - yi) + xi) c = !c; } return c; };
  const walls = [];
  for (const shape of ['circle', 'rounded', 'heart', 'tag']) for (const [w, h] of [[10, 10], [20, 18], [25, 30]]) {
    const f = X.startFace({ shape, wMm: w, hMm: h, hole: true, holeMm: 1.5 }), o = f.items.find(i => i.id === 'outline'), hl = f.items.find(i => i.id === 'hole');
    const pts = M.flatten(o.subpaths)[0].pts, hb = M.bboxOf(hl.subpaths), cx = (hb[0] + hb[2]) / 2, cy = (hb[1] + hb[3]) / 2, r = (hb[2] - hb[0]) / 2;
    walls.push({ shape, w, h, inside: inside(cx, cy, pts), wallMm: +((dist(cx, cy, pts) - r) * MM).toFixed(3), diaMm: +(2 * r * MM).toFixed(3), cut: hl.role === 'cut' });
  }
  const minWall = Math.min(...walls.map(x => x.wallMm));
  check('A2 the jump ring hole is a cut inside the outline, Ø 1.5 mm, with at least 1 mm of metal to the edge (every outline, three sizes)',
    walls.every(x => x.inside && x.cut && Math.abs(x.diaMm - 1.5) < 1e-3 && x.wallMm >= 0.99), `min wall ${minWall} mm; ${walls.map(x => x.shape[0] + x.w + ':' + x.wallMm).join(' ')}`);
  check('A3 a hole the charm has no room for is refused in words (a 5 mm circle with a 2 mm hole), a 10 mm one is fine',
    /bigger charm/.test(X.startWhy({ shape: 'circle', wMm: 5, hole: true, holeMm: 2 })) && X.startWhy({ shape: 'circle', wMm: 10, hole: true }) === '' && X.startWhy({ shape: 'circle', wMm: 5, hole: false }) === '', '');
  // A4 the design from the editor's result: the back's guide never saved; words added on the back are; the size is the cut's as drawn (an outline scaled 1.5 in x)
  const st = X.newStart({ shape: 'rounded', wMm: 20, hMm: 10, hole: true }), geo = M.createGeo(null);
  const front = { face: 'front', unitPt: true, frame: st.faces.front.frame, items: M.exportItems(st.faces.front.items.map(i => i.id === 'outline' ? Object.assign({}, i, { sx: 1.5 }) : i), geo) };
  const backOnly = { face: 'back', unitPt: true, frame: st.faces.back.frame, items: M.exportItems(st.faces.back.items, geo) };
  const backWords = { face: 'back', unitPt: true, frame: st.faces.back.frame, items: backOnly.items.concat([{ id: 'w', kind: 'text', role: 'hatch', text: 'A', subpaths: [[['m', [1, 1]], ['l', [3, 1]], ['l', [3, 3]], ['h']]] }]) };
  const d1 = X.designOf({ faces: { front, back: backOnly } }, st, 'Test', M), d2 = X.designOf({ faces: { front, back: backWords } }, st, 'Test', M);
  const free15 = X.newStart({ shape: 'free', wMm: 15, hMm: 12 }), d3 = X.designOf({ faces: { front: { face: 'front', frame: free15.faces.front.frame, items: [] } } }, free15, 'Test', M);
  check('A4 the design keeps the front as edited, the back only when something was put on it (the guide never), the cut\'s size as drawn (30 × 10 mm), and says when there is no cut yet',
    !d1.faces.back && d2.faces.back.items.length === 1 && d2.faces.back.items[0].id === 'w' && Math.abs(d1.widthMm - 30) < 1e-3 && Math.abs(d1.heightMm - 10) < 1e-3 && d1.source.kind === 'new' && d1.by === 'Test'
      && d3.noCut === true && Math.abs(d3.widthMm - 15) < 1e-3, JSON.stringify({ w: d1.widthMm, h: d1.heightMm, back2: d2.faces.back && d2.faces.back.items.length, noCut: d3.noCut }));
  // A5 the search: 7,000 charms, the SKU first; the time of one search
  const lib = new Map(); for (let i = 0; i < 7000; i++) { const sku = ['SUNFLOWER', 'COMET', 'CAT', 'HEART', 'MOON', 'STAR', 'TAG'][i % 7] + ' ' + (1000 + i); lib.set(sku, { sku, aiPath: 'x/' + i, masterName: 'FILE' + (i % 50) + '.ai', widthPt: 40, heightPt: 40 }); }
  lib.set('CAT', { sku: 'CAT', aiPath: 'x/cat' }); lib.set('CATALOG 1', { sku: 'CATALOG 1', aiPath: 'x/c1' });
  const t0 = process.hrtime.bigint(), rows = X.indexOf(lib), tIx = Number(process.hrtime.bigint() - t0) / 1e6;
  const qs = ['c', 'ca', 'cat', 'cat 10', 'sun', 'sunflower 1', 'moon 5', 'file3', 'zzz', 'star 69'], times = [];
  let first = null;
  for (let k = 0; k < 3; k++) for (const q of qs) { const a = process.hrtime.bigint(), r = X.rank(rows, q, 60); times.push(Number(process.hrtime.bigint() - a) / 1e6); if (q === 'cat' && !first) first = r.slice(0, 3).map(x => x.e.sku); }
  times.sort((a, b) => a - b);
  const med = times[times.length >> 1], max = times[times.length - 1];
  check('A5 the search over 7,000 charms: the SKU itself first, then SKUs that start with it; a search costs ' + med.toFixed(2) + ' ms (median), ' + max.toFixed(2) + ' ms at most; the index ' + tIx.toFixed(1) + ' ms once',
    first && first[0] === 'CAT' && first[1] === 'CAT 1002' && med < 8 && max < 40, JSON.stringify(first));
}

/* ═══ B · the page ═══ */
const settleP = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
async function waitFor(page, fn, arg, ms) { return page.waitForFunction(fn, arg, { timeout: ms || 60000 }); }

async function partB() {
  const R = require('./_engraveRealWorld.cjs');
  const shots = process.env.SHOTS || '';
  const w = await R.world({ shots: shots || path.join(require('node:os').tmpdir(), 'ce-masters-shots') });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page, st = w.srv.st;
  const shot = async name => { if (shots) { fs.mkdirSync(shots, { recursive: true }); await page.screenshot({ path: path.join(shots, name + '.png') }); } };
  const libCalls = () => st.calls.filter(c => c.name === 'charmNestLibrary').length;
  const artCalls = () => st.calls.filter(c => c.name === 'charmNestLibrary' && /^art/.test(String(c.op || ''))).length;
  try {
    // the stand-ins for what other workstreams build: the design library, the Send to sheet pop-up, the employee log
    await page.evaluate(() => {
      const ver = {}; let n = 0;
      window.__puts = []; window.__sends = []; window.__log = [];
      window.CharmNestDesigns = { put: async d => { await new Promise(r => setTimeout(r, 30)); window.__puts.push(JSON.parse(JSON.stringify(d))); const id = d.id || 'DSG-' + (++n); ver[id] = (ver[id] || 0) + 1; return { id, version: ver[id], at: Date.now(), hash: 'h' + ver[id] }; } };
      window.CharmNestCustomPiece = { open: o => { window.__sends.push(o); } };
      window.CNDesignLog = { event: (kind, data) => window.__log.push({ kind, data: JSON.parse(JSON.stringify(data || {})) }) };
    });
    const b0 = await page.evaluate(() => ({ mod: !!window.CharmNestCeMasters, host: !!(window.CharmEditDesign && CharmEditDesign.fonts && CharmEditDesign.textToPaths && CharmEditDesign.open), n: B.master.entries.size, entry: JSON.stringify(Master.entryFor('BFLOWER_9901')) }));
    check('B0 the page has the module and the bridge\'s design-mode open (fonts, outlines), and the fixture library (' + b0.n + ' masters)', b0.mod && b0.host && b0.n >= 5, JSON.stringify({ mod: b0.mod, host: b0.host }));

    /* ── B1 the search: what the page holds, no call ── */
    const c0 = libCalls();
    const s1 = await page.evaluate(() => { const out = {}; for (const q of ['flower', 'BFLOWER_9901', 'cat', 'comet 9427', 'nothing here', '']) out[q] = CharmNestCeMasters.search(q); return out; });
    const fl = s1.flower[0] || {};
    check('B1 search answers from the library the page holds: "flower" finds BFLOWER_9901 with its size (the drawing\'s own, ' + fl.widthMm + ' × ' + fl.heightMm + ' mm), "cat" both cats, nothing for nonsense, A to Z for nothing typed; no call to the cloud',
      fl.sku === 'BFLOWER_9901' && Math.abs(fl.widthMm - (34 - 2.6) * MM) < 0.06 && s1.BFLOWER_9901[0].sku === 'BFLOWER_9901' && s1.cat.map(r => r.sku).join() === 'SCHRODINGER CAT 1,SCHRODINGER CAT 2'
        && s1['comet 9427'][0].sku === 'COMET_9427' && s1['nothing here'].length === 0 && s1[''].length === b0.n && libCalls() === c0, JSON.stringify({ cat: s1.cat.map(r => r.sku), calls: libCalls() - c0 }));

    /* ── B2 the picker: typing over a 7,000-charm library, within a frame, no call ── */
    await page.evaluate(() => { window.__pick = CharmNestCeMasters.openPicker(); });
    await waitFor(page, () => document.querySelectorAll('.cem-root .cem-tile').length > 0);
    await settleP(page);
    const p0 = await page.evaluate(() => ({ tiles: document.querySelectorAll('.cem-tile').length, count: document.querySelector('[data-count]').textContent, focus: document.activeElement && document.activeElement.dataset.cf, title: document.querySelector('.cem-id h2').textContent }));
    await shot('B2-picker');
    const t2 = await page.evaluate(async () => {
      const real = B.master.entries, big = new Map(real), pic = 'data:image/svg+xml,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 10 10"><circle cx="5" cy="5" r="4"/></svg>');
      for (let i = 0; i < 7000; i++) { const sku = ['SUNFLOWER', 'COMET', 'CAT', 'HEART', 'MOON', 'STAR', 'TAG'][i % 7] + ' ' + (1000 + i); big.set(sku, { sku, aiPath: 'charmnest/master/none.ai', thumbUrl: pic, widthPt: 40, heightPt: 36, masterName: 'TEST' + (i % 40) + '.ai' }); }
      B.master.entries = big;
      const q = document.querySelector('.cem-root [data-cf="q"]'), times = [];
      const a0 = performance.now(); CharmNestCeMasters.search('x'); const tIndex = performance.now() - a0;   // (the index, once per library read)
      for (const v of ['s', 'su', 'sun', 'sunf', 'sunfl', 'sunflo', 'sunflower', 'sunflower 1', 'sunflower 10', 'c', 'ca', 'cat', 'cat 2', 'm', 'mo', 'moo', 'moon']) {
        const t = performance.now(); q.value = v; q.dispatchEvent(new Event('input', { bubbles: true })); times.push(performance.now() - t);
        await new Promise(r => requestAnimationFrame(r));
      }
      const tiles = document.querySelectorAll('.cem-tile').length, count = document.querySelector('[data-count]').textContent;
      B.master.entries = real;
      q.value = ''; q.dispatchEvent(new Event('input', { bubbles: true }));
      times.sort((a, b) => a - b);
      return { med: times[times.length >> 1], max: times[times.length - 1], tIndex, tiles, count };
    });
    check(`B2 the picker opens on the library (${p0.tiles} charms, "${p0.count}", the search field ready); typing over 7,000 charms paints each keystroke in ${t2.med.toFixed(1)} ms (median), ${t2.max.toFixed(1)} ms at most (60 tiles; the index ${t2.tIndex.toFixed(1)} ms once); no call to the cloud`,
      p0.tiles === b0.n && p0.focus === 'q' && t2.med < 16 && t2.max < 50 && t2.tiles === 60 && /^60\+ of 7,0\d\d$/.test(t2.count) && libCalls() === c0, JSON.stringify({ count: t2.count, calls: libCalls() - c0 }));

    /* ── B3 a master opened from the picker: every part a layer, none locked ── */
    await page.fill('.cem-root [data-cf="q"]', 'bflower');
    await settleP(page);
    await page.click('.cem-tile[data-sku="BFLOWER_9901"]');
    await waitFor(page, () => !!(window.CharmEditor && CharmEditor.current) && !document.querySelector('.cem-root:not(.cem-root--card)'), null, 90000);
    await settleP(page);
    const b3 = await page.evaluate(async () => {
      const e = Master.entryFor('BFLOWER_9901'), src = await Pool.masterCharm(e, null), ch = src.charms[0], AE = CharmNestArtEdit;
      const paths = AE.itemsOf(ch, { M: [1, 0, 0, 1, 0, 0], R: [1, 0, 0, 1, 0, 0] }, 'front').filter(i => i.subpaths && i.subpaths.length);
      const C = CharmEditor.current, items = C.items('front'), back = C.items('back'), outId = AE.memberId(ch, ch.members.indexOf(ch.outline)), out = items.find(i => i.id === outId);
      const ob = C.box(outId), fr = C.frame();
      return { members: ch.members.length, paths: paths.length, ids: items.map(i => i.id).join() === paths.map(i => i.id).join(), n: items.length, locked: items.filter(i => i.locked).length, master: items.every(i => i.source === 'master'),
        out: out && out.role === 'cut' && out.name === 'Outline', box: ob && [ob[0], ob[1], ob[2] - ob[0], ob[3] - ob[1]].map(v => +v.toFixed(3)), frame: [fr.x0, fr.y0, +fr.wPt.toFixed(3), +fr.hPt.toFixed(3)],
        title: document.querySelector('.ce-root .ce-id h2').textContent, sub: (document.querySelector('.ce-root .ce-eyebrow') || {}).textContent || '', back: back.length, backLocked: back.every(i => i.locked && i.guide), cuts: items.filter(i => i.role === 'cut').length,
        roles: [...new Set(items.map(i => i.role))].sort().join() };
    });
    check(`B3 BFLOWER_9901 opens in the Charm Editor as a new design: all ${b3.n} parts of its drawing are layers (the master's ${b3.members} members, the same ids, roles ${b3.roles}), none locked, its outline the cut framing it; the back starts with its ${b3.back} cut lines seen from behind, locked as a guide`,
      b3.ids && b3.n === b3.paths && b3.n === b3.members && b3.locked === 0 && b3.master && b3.out && b3.box && b3.box[0] === 0 && b3.box[1] === 0 && Math.abs(b3.box[2] - b3.frame[2]) < 1e-6 && b3.title === 'BFLOWER_9901'
        && /charm library/.test(b3.sub) && b3.back === b3.cuts && b3.backLocked, JSON.stringify(b3));
    await shot('B3-master-open');

    /* ── B4 changed as a person changes it: a petal deleted from the Layers, the hatched centre nudged 0.2 mm, Save ── */
    const ids = await page.evaluate(() => { const its = CharmEditor.current.items('front'); return { petal: its.find(i => i.role === 'line').id, centre: (its.find(i => i.role === 'hatch') || its.find(i => i.role !== 'cut' && i.role !== 'line')).id }; });
    const before = await page.evaluate(id => CharmEditor.current.box(id), ids.centre);
    await page.click(`.ce-root .ce-layer[data-id="${ids.petal}"]`); await settleP(page);
    await page.keyboard.press('Delete'); await settleP(page);
    await page.click(`.ce-root .ce-layer[data-id="${ids.centre}"]`); await settleP(page);
    for (let i = 0; i < 4; i++) await page.keyboard.press('ArrowRight');
    await settleP(page);
    const after = await page.evaluate(id => ({ box: CharmEditor.current.box(id), n: CharmEditor.current.items('front').length, dirty: CharmEditor.current.dirty() }), ids.centre);
    const a0 = artCalls();
    await page.click('.ce-root [data-act="save"]');
    await waitFor(page, () => document.querySelector('.cem-card h3') && /Saved · version 1/.test(document.querySelector('.cem-card h3').textContent));
    await shot('B4-saved-card');
    const b4 = await page.evaluate(() => ({ puts: window.__puts.length, d: window.__puts[0], card: document.querySelector('.cem-card').textContent, send: !document.querySelector('.cem-card [data-c="send"]').disabled, log: window.__log.map(x => x.kind + ':' + (x.data.source && (x.data.source.kind || x.data.source) || '')), entry: JSON.stringify(Master.entryFor('BFLOWER_9901')), open: !!CharmEditor.current }));
    const d1 = b4.d || {}, f1 = (d1.faces && d1.faces.front) || {}, moved = after.box[0] - before[0];
    check(`B4 a petal deleted and the centre nudged ${(moved * MM).toFixed(3)} mm; Save makes version 1 of a design: "${d1.name}", source master ${d1.source && d1.source.sku}, ${f1.items && f1.items.length} layers, ${d1.widthMm} × ${d1.heightMm} mm, by ${d1.by}, no back (the guide is not kept); the card offers Send to sheet; the master record and every piece's artwork untouched (${artCalls() - a0} art calls)`,
      after.n === b3.n - 1 && after.dirty && Math.abs(moved * MM - 0.2) < 1e-6 && b4.puts === 1 && d1.name === 'BFLOWER_9901' && d1.source.kind === 'master' && d1.source.sku === 'BFLOWER_9901' && !d1.id
        && f1.items.length === b3.n - 1 && !f1.items.some(i => i.id === ids.petal) && !d1.faces.back && Math.abs(d1.widthMm - b3.box[2] * MM) < 1e-3 && Math.abs(d1.heightMm - b3.box[3] * MM) < 1e-3 && d1.by === 'Test Operator'
        && b4.send && /The master BFLOWER_9901 in the charm library is unchanged/.test(b4.card) && b4.entry === b0.entry && artCalls() === a0 && artCalls() === 0 && !b4.open
        && b4.log.join() === 'designOpen:master,designSave:master', JSON.stringify({ n: after.n, moved, log: b4.log, art: artCalls() }));

    /* ── B5 Keep editing: the same design, another change, version 2; then Send to sheet hands version 2 on ── */
    await page.click('.cem-card [data-c="again"]');
    await waitFor(page, () => !!(window.CharmEditor && CharmEditor.current));
    await settleP(page);
    const r5 = await page.evaluate(() => ({ n: CharmEditor.current.items('front').length, sub: document.querySelector('.ce-root .ce-eyebrow').textContent, locked: CharmEditor.current.items('front').filter(i => i.locked).length }));
    const another = await page.evaluate(() => CharmEditor.current.items('front').filter(i => i.role === 'line')[1].id);
    await page.click(`.ce-root .ce-layer[data-id="${another}"]`); await settleP(page);
    await page.keyboard.press('Delete'); await settleP(page);
    await page.click('.ce-root [data-act="save"]');
    await waitFor(page, () => document.querySelector('.cem-card h3') && /Saved · version 2/.test(document.querySelector('.cem-card h3').textContent));
    await page.click('.cem-card [data-c="send"]');
    await settleP(page);
    const b5 = await page.evaluate(() => ({ puts: window.__puts.length, d: window.__puts[1], sends: window.__sends, card: !!document.querySelector('.cem-card'), result: null }));
    const pick = await page.evaluate(() => window.__pick);
    check(`B5 Keep editing opens version 1 again (${r5.n} layers, none locked: "${r5.sub.slice(0, 40)}…"); a second petal deleted and Save makes version 2 of the SAME design; Send to sheet hands it on (${JSON.stringify(b5.sends[0])}); the picker's promise says what was made`,
      r5.n === b3.n - 1 && r5.locked === 0 && /version 1/.test(r5.sub) && b5.puts === 2 && b5.d.id === 'DSG-1' && b5.d.faces.front.items.length === b3.n - 2 && b5.sends.length === 1 && b5.sends[0].designId === 'DSG-1' && b5.sends[0].version === 2
        && !b5.card && pick && pick.designId === 'DSG-1' && pick.version === 2, JSON.stringify({ puts: b5.puts, id: b5.d && b5.d.id, pick }));

    /* ── B6 a blank heart, 20 × 18 mm, with a jump ring hole ── */
    await page.evaluate(() => { window.__new = CharmNestCeMasters.newDesign(); });
    await waitFor(page, () => !!document.querySelector('.cem-root [data-panel="new"]:not([hidden])'));
    await page.click('.cem-shape[data-shape="heart"]');
    await page.fill('.cem-root [data-cf="w"]', '20'); await page.fill('.cem-root [data-cf="h"]', '18');
    await page.locator('.cem-root [data-cf="h"]').press('Tab');
    if (!(await page.isChecked('.cem-root [data-cf="hole"]'))) await page.check('.cem-root [data-cf="hole"]');
    await settleP(page);
    const prev = await page.evaluate(() => ({ dims: document.querySelector('[data-dims]').textContent, paths: document.querySelectorAll('[data-prev] path').length, start: !document.querySelector('[data-act="start"]').disabled, name: document.querySelector('[data-cf="name"]').placeholder }));
    await shot('B6-new-heart');
    await page.click('.cem-root [data-act="start"]');
    await waitFor(page, () => !!(window.CharmEditor && CharmEditor.current) && !document.querySelector('.cem-root:not(.cem-root--card)'));
    await settleP(page);
    const b6 = await page.evaluate(() => {
      const C = CharmEditor.current, its = C.items('front'), o = C.box('outline'), h = C.box('hole'), MM = 25.4 / 72;
      return { ids: its.map(i => i.id + ':' + i.role).join(), locked: its.filter(i => i.locked).length, o: o && [(o[2] - o[0]) * MM, (o[3] - o[1]) * MM], hole: h && { dia: (h[2] - h[0]) * MM, top: h[1] * MM, cx: (h[0] + h[2]) / 2 * MM }, title: document.querySelector('.ce-root .ce-id h2').textContent, back: C.items('back').length };
    });
    check(`B6 a new heart: the preview says "${prev.dims}"; the editor opens on its outline and its jump ring hole (both the cut, nothing locked), the outline ${b6.o && b6.o.map(v => v.toFixed(3)).join(' × ')} mm, the hole Ø ${b6.hole && b6.hole.dia.toFixed(3)} mm on its middle line, its top ${b6.hole && (b6.hole.top - 0.2 * 18).toFixed(3)} mm below the dip between the lobes`,
      prev.paths === 2 && prev.start && /20 × 18 mm · hole Ø 1.5 mm/.test(prev.dims) && b6.ids === 'outline:cut,hole:cut' && b6.locked === 0 && Math.abs(b6.o[0] - 20) < 1e-3 && Math.abs(b6.o[1] - 18) < 1e-3
        && Math.abs(b6.hole.dia - 1.5) < 1e-3 && Math.abs(b6.hole.cx - 10) < 1e-3 && Math.abs(b6.hole.top - 0.2 * 18 - 1) < 1e-3 && b6.title === 'Heart 20 × 18 mm' && b6.back === 2, JSON.stringify(b6));
    await shot('B6-heart-editor');
    // the hole moved down 1 mm (Shift + arrow), Save: a new design of that size
    await page.click('.ce-root .ce-layer[data-id="hole"]'); await settleP(page);
    await page.keyboard.press('Shift+ArrowDown'); await settleP(page);
    await page.click('.ce-root [data-act="save"]');
    await waitFor(page, () => document.querySelector('.cem-card h3') && /Saved · version 1/.test(document.querySelector('.cem-card h3').textContent));
    const b6s = await page.evaluate(() => { const d = window.__puts[2]; const h = d.faces.front.items.find(i => i.id === 'hole'); return { name: d.name, source: d.source, w: d.widthMm, h: d.heightMm, n: d.faces.front.items.length, id: d.id || null, holeTy: h && h.ty * 25.4 / 72, log: window.__log.slice(-2).map(x => x.kind + ':' + JSON.stringify(x.data.source || '')) }; });
    await page.click('.cem-card [data-c="done"]');
    const nw = await page.evaluate(() => window.__new);
    check(`B6b saved as a new design: "${b6s.name}", ${b6s.w} × ${b6s.h} mm, source ${JSON.stringify(b6s.source)}, its hole ${b6s.holeTy && b6s.holeTy.toFixed(3)} mm lower; Done puts the card away`,
      b6s.name === 'Heart 20 × 18 mm' && b6s.source.kind === 'new' && b6s.source.shape === 'heart' && Math.abs(b6s.w - 20) < 1e-3 && Math.abs(b6s.h - 18) < 1e-3 && b6s.n === 2 && !b6s.id && Math.abs(b6s.holeTy - 1) < 1e-6
        && nw && nw.designId === 'DSG-2' && nw.version === 1 && !(await page.$('.cem-card')), JSON.stringify(b6s));

    /* ── B7 the library panel's way in: a master opened while the editor holds changes asks first (Discard), and the editor is then the master's ── */
    await page.evaluate(() => { window.__h = CharmNestCeMasters.newDesign(); });
    await waitFor(page, () => !!document.querySelector('.cem-root [data-panel="new"]:not([hidden])'));
    await page.click('.cem-root [data-act="start"]');
    await waitFor(page, () => !!(window.CharmEditor && CharmEditor.current));
    await page.click('.ce-root .ce-layer[data-id="outline"]'); await settleP(page);
    await page.keyboard.press('Shift+ArrowRight'); await settleP(page);
    await page.evaluate(() => { window.__m = CharmNestCeMasters.open('COMET_9427'); });
    await waitFor(page, () => !!document.querySelector('.ce-root .ce-ask'), null, 90000);
    const asked = await page.evaluate(() => document.querySelector('.ce-root .ce-ask h3').textContent);
    await page.click('.ce-root .ce-ask [data-ask="yes"]');
    await waitFor(page, () => !!(window.CharmEditor && CharmEditor.current) && document.querySelector('.ce-root .ce-id h2').textContent === 'COMET_9427', null, 90000);
    const b7 = await page.evaluate(async () => ({ h: await window.__h, n: CharmEditor.current.items('front').length, locked: CharmEditor.current.items('front').filter(i => i.locked).length, cur: CharmNestCeMasters.current() }));
    await page.click('.ce-root [data-act="cancel"]');
    await waitFor(page, () => !(window.CharmEditor && CharmEditor.current));
    const b7b = await page.evaluate(async () => ({ m: await window.__m, puts: window.__puts.length, cur: CharmNestCeMasters.current() }));
    check(`B7 from inside a changed editor, a master asks first ("${asked}"); Discard, and COMET_9427 is in the editor (${b7.n} layers, none locked) while the heart's session ended unsaved; Cancel on the untouched master closes it, nothing saved`,
      asked === 'Leave without saving?' && b7.h === null && b7.n > 0 && b7.locked === 0 && b7.cur && b7.cur.source.sku === 'COMET_9427' && b7b.m === null && b7b.puts === 3 && b7b.cur === null, JSON.stringify({ b7, b7b }));

    check('B8 no page error, no call that writes a piece\'s artwork (' + artCalls() + ')', w.errors.length === 0 && artCalls() === 0, w.errors.slice(0, 3).join(' | '));
  } finally { await w.close(); }
}

(async () => {
  if (!ONLY || ONLY === 'A') partA();
  if (!ONLY || ONLY === 'B') await partB();
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\n${RESULTS.length - bad.length}/${RESULTS.length} passed${bad.length ? ' · FAILED: ' + bad.map(r => r.id.split(' ')[0]).join(', ') : ''}`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
