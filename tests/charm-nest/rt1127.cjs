/* Paul, 27 Nov 2026, Node only: run with `node tests/charm-nest/rt1127.cjs`. No server, no network.
   U · UNGROUPALL: one Ungroup takes the whole selection apart (every group at every depth; a path of lines released ring by ring, so a hoop that crosses its outline comes away; a filled path keeps its holes; a locked part stays)
   R · REPOONLY / DRAWN: a Master files card opens its charm in the Charm Editor for the repository alone (the blue Save off, Update repository saves), the master read as its .ai draws it (the hoop not welded)
   S · SAMEFILE: an update of one SKU updates every SKU that is the very same drawing, in the same transaction; one that no longer draws it is left as it is
   M · MIDPAN: in Manual Control the wheel button pans from anywhere (over a charm too), Option + wheel zooms
   O · OWNROW: a custom piece of a file of several charms is drawn as its own charm (the 3 % size rule was removed 28 Nov 2026: not Paul's rule)
   E · SHEETTRUTH: the engraving panel asks the cloud before saying an order has no piece on a sheet */
'use strict';
const fs = require('fs'), path = require('path');
const ROOT = path.join(__dirname, '..', '..');
const read = f => fs.readFileSync(path.join(ROOT, f), 'utf8');
let passed = 0, failed = 0;
const ok = (name, cond, detail) => { console.log((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + JSON.stringify(detail).slice(0, 700))); if (cond) passed++; else failed++; };

/* ── U · ungroup all ── */
const CE = require(path.join(ROOT, 'charm-nest-charm-editor.js')), M = CE.model, geo = M.createGeo();
const ring = (cx, cy, r) => { const k = 0.5523 * r; return [['m', [cx + r, cy]], ['c', [cx + r, cy + k], [cx + k, cy + r], [cx, cy + r]], ['c', [cx - k, cy + r], [cx - r, cy + k], [cx - r, cy]], ['c', [cx - r, cy - k], [cx - k, cy - r], [cx, cy - r]], ['c', [cx + k, cy - r], [cx + r, cy - k], [cx + r, cy]], ['h']]; };
const box = (x0, y0, x1, y1) => [['m', [x0, y0]], ['l', [x1, y0]], ['l', [x1, y1]], ['l', [x0, y1]], ['h']];
const P = (id, sps, o) => Object.assign({ id, kind: 'path', role: 'cut', subpaths: sps, stroke: '#000', fill: null, tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 }, o || {});
const G = (id, kids, o) => Object.assign({ id, kind: 'group', role: 'cut', name: 'Group', children: kids, tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 }, o || {});
const leaves = items => { const out = []; const w = l => l.forEach(it => it.kind === 'group' ? w(it.children || []) : out.push(it)); w(items); return out; };
const boxOf = (it) => M.bboxOf(M.transformSubpaths(it.subpaths, geo.matrix(it)));
{
  // a charm placed from the library: a group holding a group (outline + hoop rings) and an art path, moved as a whole
  const items = [P('bg', [box(-50, -50, -40, -40)]), G('charm', [G('cut', [P('outline', [box(0, 0, 100, 80)]), P('hoop', [ring(50, -6, 10), ring(50, -6, 5)])]), P('art', [box(20, 20, 30, 30)], { role: 'art', fill: '#00f', stroke: null })], { tx: 7, ty: 3 }), P('top', [box(200, 0, 210, 10)])];
  const before = leaves(items).map(it => [it.id, boxOf(it).map(v => Math.round(v * 100) / 100)]);
  const r = M.ungroupAll(items, ['charm'], geo);
  ok('U1 one press lifts every group at every depth: no group is left of the charm', r.items.every(it => it.kind !== 'group'), r.items.map(it => it.kind + ':' + it.id));
  const got = r.items.map(it => it.id);
  ok('U2 every part stays in its place in the stack, between the parts round it', got[0] === 'bg' && got[got.length - 1] === 'top' && got.indexOf('outline') > 0 && got.indexOf('art') > got.indexOf('outline'), got);
  ok('U3 the hoop (two rings of lines, crossing nothing) is released ring by ring: each ring its own path, the selection is every part', r.ids.length === 4 && r.items.filter(it => /^hoop/.test(String(it.name || '')) || (it.subpaths || []).length === 1).length >= 4 && r.split === 2, { ids: r.ids, split: r.split });
  const moved = r.items.find(it => it.id === 'outline'), b = boxOf(moved).map(v => Math.round(v * 100) / 100);
  ok('U4 each part stands exactly where it stood (the groups\' move, 7 and 3, pushed into it)', JSON.stringify(b) === JSON.stringify([7, 3, 107, 83]), { now: b });
  // a cut line drawn as ONE path: the outline and a hoop ring that crosses it, and the hoop's hole (lines only)
  const one = [P('cutline', [box(0, 0, 100, 80), ring(50, 0, 12), ring(50, 0, 6)])];
  const r2 = M.ungroupAll(one, ['cutline'], geo);
  ok('U5 a path of lines whose hoop crosses the outline: released into its three rings (Illustrator\'s Release), so the hoop comes away from the charm', r2.items.length === 3 && r2.items.every(it => it.subpaths.length === 1), r2.items.map(it => it.subpaths.length));
  // a filled letter O: its hole stays with it (a hole released alone would fill in)
  const O = [P('o', [ring(50, 50, 20), ring(50, 50, 12)], { fill: '#00f', stroke: null, role: 'art' })];
  const r3 = M.ungroupAll(O, ['o'], geo);
  ok('U6 a filled O keeps its hole: nothing to take apart', r3.items.length === 1 && r3.items[0].subpaths.length === 2 && r3.split === 0);
  const locked = [G('lk', [P('a', [box(0, 0, 1, 1)]), P('b', [box(2, 2, 3, 3)])], { locked: true })];
  const r4 = M.ungroupAll(locked, ['lk'], geo);
  ok('U7 a locked group stays as it is', r4.items.length === 1 && r4.items[0].kind === 'group');
  const src = read('charm-nest-charm-editor.js');
  ok('U8 the editor\'s Ungroup (Shift+Cmd+G, the buttons, the menus) uses ungroupAll', /function ungroupSel\(\) \{[\s\S]{0,400}ungroupAll\(next, selEntries\(\)\.map\(e => e\.it\.id\), geo\)/.test(src));
}

/* ── R · the repository edit ── */
{
  const br = read('charm-nest-bridge.js'), ms = read('charm-nest-ce-masters.js'), ce = read('charm-nest-charm-editor.js'), pm = read('charm-nest-pool-make.js');
  ok('R1 a Master files card has an Edit button that opens its charm for the repository alone', /data-ceopen="\$\{esc\(e\.sku\)\}"/.test(br) && /M\.open\(b\.dataset\.ceopen, \{ repoOnly: true \}\)/.test(br));
  ok('R2 repoOnly: the start carries saveOff and the editor is opened with it', /if \(o && o\.repoOnly\) \{ st\.saveOff = /.test(ms) && /start\.saveOff \? \{ saveOff: start\.saveOff \} : \{\}/.test(ms));
  ok('R3 the blue Save is drawn disabled with saveOff, and Save / Mod+S only say to press Update repository', /data-act="save" \$\{opts\.saveOff \? `disabled aria-disabled="true"/.test(ce) && /if \(opts\.saveOff\) \{ say\(/.test(ce));
  ok('R4 closing after the drawing is in the repository asks nothing; before, it says Update repository keeps it', /rp\.dataset\.done === '1' && rp\.dataset\.dirty !== '1'/.test(ce) && /Nothing is kept until you press Update repository/.test(ce));
  ok('R5 DRAWN: the master reader can skip the hoop weld, the page exposes it, the editor opens the master as drawn', /if \(!\(o && o\.weld === false\)\) \{ const r = P\.integrateRings\(charm\)/.test(pm) && /drawnMaster: \(entry, size\) => readMasterCharm\(entry, size, \{ weld: false \}\)/.test(br) && /drawn = await Pl\.drawnMaster\(entry, sz\)/.test(ms));
  // the weld itself, on a real drawing: the drawn charm keeps its hoop as parts of its own, the pool's reading welds it
  const W = { window: {} }; global.window = global.window || global; require(path.join(ROOT, 'netlify/functions/_charmNestPdf.js'));
}

/* ── S · the same drawing under several SKUs ── */
(async () => {
  const Master = require(path.join(ROOT, 'netlify/functions/_charmNestMaster.js'));
  const DEL = { del: 1 }, docs = new Map(), FV = { serverTimestamp: () => 'TS', delete: () => DEL };
  const ref = (c, id) => ({ id: id || 'auto' + Math.random().toString(36).slice(2), path: c + '/' + (id || ''), c });
  const snap = r => { const k = r.c + '/' + r.id, d = docs.get(k); return { id: r.id, exists: !!d, data: () => (d ? JSON.parse(JSON.stringify(d)) : undefined) }; };
  const put = (r, patch) => { const k = r.c + '/' + r.id, d = Object.assign({}, docs.get(k) || {}); for (const [f, v] of Object.entries(patch)) { if (v === DEL) delete d[f]; else d[f] = v; } docs.set(k, d); };
  const db = { collection: c => ({ doc: id => ref(c, id) }), getAll: async (...rs) => rs.map(snap), runTransaction: async fn => fn({ get: async r => snap(r), set: (r, p) => put(r, p) }) };
  const A = { sku: 'OCTO A', aiPath: 'charmnest/master/OCTO.ai', charmHash: 'old1', widthPt: 20, heightPt: 20, members: 3 };
  docs.set('Charm_Master_Index/OCTO A', A);
  docs.set('Charm_Master_Index/OCTO B', Object.assign({}, A, { sku: 'OCTO B', upAngle: 30, upSource: 'operator' }));
  docs.set('Charm_Master_Index/OCTO C', Object.assign({}, A, { sku: 'OCTO C', aiPath: 'charmnest/master/OTHER.ai' }));
  const body = { sku: 'OCTO A', by: 'Paul_K', expect: { charmHash: 'old1' }, siblings: ['OCTO B', 'OCTO C', 'OCTO A'], entry: { charmHash: 'new2', widthPt: 30, heightPt: 31, areaPt2: 500, members: 5, holes: 2, aiPath: 'charmnest/master/edited/OCTO_A-e1.ai', thumbPath: 'charmnest/master/edited/OCTO_A-e1.png', upAngle: 0, upSource: 'drawn' } };
  const r = await Master.editorPut({ db, FV }, body);
  const a = docs.get('Charm_Master_Index/OCTO A'), b = docs.get('Charm_Master_Index/OCTO B'), c = docs.get('Charm_Master_Index/OCTO C');
  ok('S1 the SKU asked is updated', r.ok && a.charmHash === 'new2' && a.aiPath === body.entry.aiPath, r);
  ok('S2 a sibling that is the very same drawing is updated with it in the same transaction, its own angle kept (a person set it)', b.charmHash === 'new2' && b.aiPath === body.entry.aiPath && b.widthPt === 30 && b.upAngle === 30 && b.edited && b.edited.with === 'OCTO A' && JSON.stringify(r.siblings) === '["OCTO B"]', { b, r });
  ok('S3 a SKU that draws another file is left as it is, and said', c.charmHash === 'old1' && c.aiPath === 'charmnest/master/OTHER.ai' && JSON.stringify(r.siblingsLeft) === '["OCTO C"]', { c, r });
  ok('S4 each SKU changed is kept in the edits log', [...docs.keys()].filter(k => k.startsWith('Charm_Master_Edits/')).length === 2);
  ok('S5 the answer carries the siblings\' new entries for the page', Array.isArray(r.siblingEntries) && r.siblingEntries.length === 1 && r.siblingEntries[0].sku === 'OCTO B');
  // EVERYOLD: a second update keeps the first drawing among those replaced
  const r2 = await Master.editorPut({ db, FV }, Object.assign({}, body, { siblings: [], expect: { charmHash: 'new2' }, entry: Object.assign({}, body.entry, { charmHash: 'new3', aiPath: 'charmnest/master/edited/OCTO_A-e2.ai', thumbPath: 'charmnest/master/edited/OCTO_A-e2.png' }) }));
  const a2 = docs.get('Charm_Master_Index/OCTO A');
  ok('S7 EVERYOLD: after a second update the record still names the first drawing as replaced (was), so pieces cut from it are made again too', r2.ok && a2.edited.from === 'new2' && JSON.stringify(a2.edited.was) === '["old1","new2"]', a2.edited);
  { const ex = read('netlify/functions/_charmNestCmdExec.js'), st = ex.indexOf('function redrawn(e, r) {'), en = ex.indexOf('\n}', st) + 2, redrawn = new Function(ex.slice(st, en) + '; return redrawn;')();
    ok('S8 EVERYOLD: the cloud\'s remake finds a piece cut from the first drawing after two updates, and one cut from the last replaced, never one cut from the drawing now', redrawn(a2, { charmHash: 'old1' }) && redrawn(a2, { charmHash: 'new2~10MM' }) && !redrawn(a2, { charmHash: 'new3' }) && !redrawn({ charmHash: 'x' }, { charmHash: 'old1' })); }
  const br = read('charm-nest-bridge.js');
  ok('S6 the page names the siblings (same file, same charm, one size, no pair), takes their entries and remakes their open orders too', /const sibs = size \? \[\] : \[\.\.\.B\.master\.entries\.values\(\)\]/.test(br) && /ids: \[\.\.\.news\.keys\(\)\]/.test(br) && /Review\.redrawNoted\(olds, news\)/.test(br));

  /* ── M · Manual Control's wheel and wheel button ── */
  const sw = read('charm-nest-sheetwin.js');
  ok('M1 the wheel button pressed over the plate, a charm, a handle or the stage pans, taken before the charm or tool sees it, no autoscroll', /E\.plateBox\.addEventListener\("pointerdown", e => \{ if \(e\.button !== 1 \|\| !MC\.on\) return; e\.preventDefault\(\); e\.stopPropagation\(\); panStart\(e\); \}, true\)/.test(sw) && /\["mousedown", "auxclick"\]/.test(sw));
  ok('M2 panStart takes the wheel button anywhere (over a charm too); a still wheel-button press lets go of nothing', /const mid = e\.button === 1;/.test(sw) && /if \(!mid && !W\.space && \(mcHandleAt\(e\) \|\| hitAt\(e\)\)\) return false;/.test(sw) && /!W\.space && !P\.mid\) tryDo\(\(\) => mcSelect\(null\)\)/.test(sw));
  ok('M3 Option + wheel (or a pinch) zooms; the plain wheel moves a zoomed view and is the window\'s at full view', /function mcWheel\(e\) \{\s*if \(e\.altKey \|\| e\.ctrlKey\) return zoomWheel\(e\);\s*if \(!zoomed\(\) \|\| !W\.el\) return;/.test(sw) && /if \(MC\.on\) mcWheel\(e\)/.test(sw));
  // M4: the wheel handler, run on a stand-in of the window's view: Option zooms about the pointer, plain wheel pans, at 100 % plain wheel does nothing
  {
    const start = sw.indexOf('  function mcWheel(e) {'), end = sw.indexOf('  /** Pan, while zoomed in Manual Control', start), code = sw.slice(start, end);
    const calls = []; const f = new Function('W', 'zoomed', 'zoomWheel', 'panBy', code + '; return mcWheel;');
    const Wst = { el: { fx: { width: 200, getBoundingClientRect: () => ({ width: 100 }) } } };
    let z = true; const wheel = f(Wst, () => z, e => calls.push(['zoom', e.deltaY]), (x, y) => calls.push(['pan', x, y]));
    const ev = o => Object.assign({ deltaX: 0, deltaY: 10, deltaMode: 0, altKey: false, ctrlKey: false, shiftKey: false, prevented: false, preventDefault() { this.prevented = true; } }, o);
    wheel(ev({ altKey: true })); wheel(ev({})); wheel(ev({ shiftKey: true })); z = false; const e4 = ev({}); wheel(e4);
    ok('M4 Option + wheel zooms, the wheel pans (×2 for the canvas\' pixels), Shift + wheel pans sideways, unzoomed the wheel is left to the window', JSON.stringify(calls) === JSON.stringify([['zoom', 10], ['pan', -0, -20], ['pan', -20, -0]]) && !e4.prevented, calls);
  }

  /* ── O · the seat's neighbours ── */
  const seat = read('netlify/functions/_charmNestSeatNow.js'), np = read('netlify/functions/_charmNestNestPieces.js');
  ok('O1 every piece is drawn with its own row (its customIndex, its body), as rebuild draws it', /rows\.set\(String\(rc\.poolId\), pool\);/.test(np));
  ok('O2 no size rule of its own decides whether a neighbour counts (NO3PCT, 28 Nov 2026: Paul: "No rules are allowed to be made by you")', !/sizeOk/.test(seat));

  /* ── E · the engraving panel and the cloud ── */
  ok('E1 the cloud\'s pieces of a line are read (once in 30 s, in memory) and given to the panel', /async function linePieces\(row\)/.test(br) && /linePieces, linePiecesKnown, heldWhy:/.test(br));
  ok('E2 "no piece on a sheet" is said only after the cloud is read, never while a piece of the line is on a sheet there', /if \(!last\) return "";\s*if \(last\.some\(p => p\.sheetId && !PIECE_GONE\.has\(String\(p\.state \|\| ""\)\)\)\) \{ heldCopiesFor\(job\); return ""; \}/.test(br));
  ok('E3 a job that names no piece takes its line\'s pieces (its run line\'s, else the cloud\'s), so its charm and grid are drawn', /const own = \(row\.poolIds \|\| \[\]\)\.map\(String\)\.filter\(Boolean\);/.test(br) && /Pool\.linePieces\(row\)\.then\(ps => \{ const live =/.test(br));

  /* ── F · the refit banner and the set's revision ── */
  {
    const rf = read('charm-nest-refit.js'), st = rf.indexOf('  function see(rec) {'), en = rf.indexOf('  /** What a sheet is going through now', st);
    const KNOWN = new Map(); let soons = 0;
    const see = new Function('KNOWN', 'num', 'idOf', 'pick', 'soon', rf.slice(st, en) + '; return see;')(KNOWN, v => (Number.isFinite(+v) ? +v : 0), r => (r && (r.id || r.sheetId)) || '', r => r, () => soons++);
    see({ id: 's1', updatedAt: 100, refit: { at: 90, jobId: 'j1', pieces: [1] }, nestJob: { id: 'j1', status: 'running' }, previewAt: 50 });
    see({ id: 's1', updatedAt: 120, refit: true, nestJob: { status: 'done', at: 118 }, previewAt: 50 });
    const k = KNOWN.get('s1');
    ok('F1 a list row (refit true, a job with no id) keeps the refit read from the whole record and brings its job\'s status up to date', k.refit && k.refit.jobId === 'j1' && k.nestJob.id === 'j1' && k.nestJob.status === 'done', k);
    see({ id: 's1', updatedAt: 130, refit: false });
    ok('F2 a list row saying the refit is over ends it', KNOWN.get('s1').refit === null);
    const tk = read('netlify/functions/_charmNestTakeOffOps.js'), cd = read('netlify/functions/_charmNestCustomDone.js'), gl = read('netlify/functions/_charmNestGreenLine.js'), sw2 = read('netlify/functions/_charmNestSweeps.js');
    ok('F3 a take-off and a custom piece done move the set\'s revision with its order list', /orders: setOrders, setRev: setRevNow \+ 1/.test(tk) && /orders: e\.setOrders, setRev: num\(e\.setRevNow\) \+ 1/.test(cd) && /setRev: num\(x\.setNow\.setRev\) \+ 1/.test(cd));
    ok('F4 the green-line undo and the name sweep write the set with the revision they read, and read it again when refused', /setBase: num\(s\.data\(\)\.setRev\)/.test(gl) && /setBase: num\(cur\.setRev\)/.test(sw2));
    const html = read('charm-nest-1.html');
    ok('F5 every browser file changed here has a new cache token', ['charm-nest-bridge.js', 'charm-nest-ce-masters.js', 'charm-nest-charm-editor.js', 'charm-nest-cloud-sheets.js', 'charm-nest-pool-make.js', 'charm-nest-refit.js', 'charm-nest-run-follow.js', 'charm-nest-sheetwin.js', 'charm-nest-store.js', 'charm-nest-sync-worker.js'].every(f => new RegExp(f.replace(/\./g, '\\.') + '\\?v=[^"]*-rt1"').test(html)));
  }
  console.log(`rt1127: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
