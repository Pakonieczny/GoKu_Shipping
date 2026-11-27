/* RGHAND (Paul, 27 Nov 2026, RG_Nov.23.26_Set-1_Sheet-2: Manual Control refused "This sheet has been cut on the laser" though only its green line cut was recorded). "There should be nothing
   preventing the user from moving the pieces around on a given RG sheets even if the green cut line has already been marked" ... "the pieces must use the green cut line as a boundary a barrier
   of the sheet so that pieces must stay to the left of the green cut line for that particular RG sheet". Offline, Node only (the real geometry, the page's own functions read from
   charm-nest-sheetwin.js, the real library handler over _cloudFakes.cjs: no network, no Firebase):
     G  the barrier's geometry: a plan's shape moved with its charm is the shape drawn at the new pose; a charm on its own metal passes, one past its line (moved, turned, made bigger) does not;
        one on metal an earlier cut took is said apart (into); where the line reaches the far edge there is no barrier
     P  the page: a Rose Gold sheet with its line drawn or cut has its band (the line, the outline before it: a set-aside cut never counts), read once when its physical sheet is not read yet;
        a pose past the line is refused as "line" (no free placement, no snap past it), one inside passes; 10K / 14K / GF and a laser-cut sheet keep their rules
     S  the server: a recorded Rose Gold cut is claimed (the laser's mark alone refuses); a move on its own metal is saved with the cut, the line, its fingerprint and the physical sheet untouched;
        a move past the line, a charm the line was not drawn for, and a line drawn on a physical sheet that changed since are refused, nothing written; a 10K / 14K cut keeps its rule
   node tests/charm-nest/rt1127-rghand.cjs */
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path');
const ROOT = path.join(__dirname, '..', '..'), R = require(path.join(ROOT, 'charm-nest-rose.js'));
const log0 = console.log.bind(console);
let n = 0, failed = 0; const ok = (m, c, d) => { n++; log0((c ? 'OK   ' : 'FAIL ') + m + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 700))); if (!c) failed++; };
const part = async (name, fn) => { try { await fn(); } catch (e) { ok(name + ' ran to its end', false, String((e && e.stack) || e).slice(0, 900)); } };

// ── a 100 x 50 mm Rose Gold sheet; charms drawn as outlines (y up, as the PDF reads them) ──
const WPT = 100 / R.MM, HPT = 50 / R.MM, SC = 0.971;
const rect = (id, w, h) => ({ id, hash: 'h-' + id, outline: { subpaths: [[['m', [0, 0]], ['l', [w, 0]], ['l', [w, h]], ['l', [0, h]], ['h']]] }, centerPt: [w / 2, h / 2], members: [] });
const at = (id, cxPt, cyPt, angle = 0, scale = SC) => ({ id, cxPt, cyPt, angle, scale, wPt: 20, hPt: 20 });
const fingerprint = s => JSON.stringify((s.placements || []).map(p => [p.id, +p.cxPt.toFixed(3), +p.cyPt.toFixed(3), p.angle, p.scale || 1, p.hash || s.charms?.find(c => c.id === p.id)?.hash || null]));
// an earlier sheet's cut along the left edge, then this sheet's charms right of it: four squares and a long bar alone in its rows
const prevCharms = [rect('p1', 20, 20), rect('p2', 20, 20), rect('p3', 20, 20), rect('p4', 20, 20), rect('p5', 20, 20)];
const prevPl = [at('p1', 12, 14), at('p2', 12, 40), at('p3', 12, 66), at('p4', 12, 92), at('p5', 12, 118)];
const prevPlan = R.plan(R.shapes(prevCharms, prevPl), WPT, HPT, null, 0.2);
const ownCharms = [rect('a', 20, 20), rect('b', 20, 20), rect('c', 20, 20), rect('d', 20, 20), rect('e', 30, 8)];
const ownPl = [at('a', 50, 14), at('b', 50, 40), at('c', 76, 14), at('d', 76, 40), at('e', 55, 100)];
const ownPlan = R.plan(R.shapes(ownCharms, ownPl), WPT, HPT, prevPlan.profile, 0.2);
const shapeOf = id => ownPlan.shapes.find(s => s.id === id), poseOf = id => ownPl.find(p => p.id === id);
const inBand = (id, to) => R.inside([R.moved(shapeOf(id), poseOf(id), to)], ownPlan.profile, prevPlan.profile, 0.2);

(async () => {
  await part('G', async () => {
    ok('G0 the earlier line runs along the left edge and this sheet\'s line right of its charms, on the same axis', prevPlan.profile.axis === 'x' && ownPlan.profile.axis === 'x' && Math.max(...ownPlan.profile.values) > Math.max(...prevPlan.profile.values), { prev: Math.max(...prevPlan.profile.values), own: Math.max(...ownPlan.profile.values) });
    // a shape moved with its charm is the shape shapes() draws at the new pose (the same turn, the same size)
    let worst = 0;
    for (const to of [at('c', 70, 20, 30), at('c', 60, 30, 90, SC * 1.1), at('c', 80, 18, 245, SC * 0.85)]) {
      const a = R.moved(shapeOf('c'), poseOf('c'), to), b = R.shapes(ownCharms, [Object.assign({}, to, { id: 'c' })])[0];
      const pa = a.paths.flat(), pb = b.paths.flat();
      for (const q of pb) { const d = Math.min(...pa.map(p => Math.hypot(p[0] - q[0], p[1] - q[1]))); worst = Math.max(worst, d); }
    }
    ok('G1 a plan\'s shape moved with its charm (turned, resized) is the outline drawn at the new pose (within 0.002 pt)', worst < 0.002, { worst });
    const none = r => !r.past.length && !r.into.length;
    ok('G2 every charm where the line was drawn for it is inside', ownPl.every(p => none(inBand(p.id, p))));
    ok('G3 moved left on its own metal, and turned where it has room: inside', none(inBand('a', at('a', 45, 14))) && none(inBand('d', at('d', 74, 40, 0))) && none(inBand('a', at('a', 50, 14, 45, SC * 0.8))));
    const r1 = inBand('c', at('c', 80, 14)), r2 = inBand('e', at('e', 55, 100, 90)), r3 = inBand('c', at('c', 76, 14, 0, SC * 1.2)), r4 = inBand('a', at('a', 50, 70));
    ok('G4 past its line: moved right 4 pt, the bar turned upright into rows the line did not reach, made 20% bigger, moved down into an empty band', [r1, r2, r3, r4].every(r => r.past.length === 1), [r1, r2, r3, r4]);
    const r5 = inBand('a', at('a', 25, 14));
    ok('G5 onto metal the earlier cut took: said apart (into), not past the line', r5.into.length === 1 && !r5.past.length, r5);
    const full = Object.assign({}, ownPlan.profile, { values: ownPlan.profile.values.map(() => WPT) });
    ok('G6 a line that reaches the far edge (a full sheet) is no barrier: the sheet\'s own edge is its check', !R.inside([R.moved(shapeOf('c'), poseOf('c'), at('c', 200, 14))], full, prevPlan.profile, 0.2).past.length);
    // the other axis: a line from the top
    const yPlan = R.plan(R.shapes([rect('t', 20, 20)], [at('t', 60, 14)]), WPT, HPT, Object.assign({}, prevPlan.profile, { axis: 'y', values: Array(Math.ceil(WPT / 0.5)).fill(0) }), 0.2);
    const ty = to => R.inside([R.moved(yPlan.shapes[0], at('t', 60, 14), to)], yPlan.profile, null, 0.2).past.length;
    ok('G7 a line drawn from the top: moving up passes, moving down past it does not', yPlan.profile.axis === 'y' && !ty(at('t', 60, 12)) && ty(at('t', 60, 20)) === 1, { axis: yPlan.profile.axis });
  });

  await part('P', async () => {
    const src = fs.readFileSync(path.join(ROOT, 'charm-nest-sheetwin.js'), 'utf8');
    const a = src.indexOf('  const RGB = new Map();'), m = src.indexOf('  function mcLine(G, c, q) {'), b = src.indexOf('\n  }\n', m) + 4;
    const c0 = src.indexOf('  function mcCheck(G, c, pose, snapPt, full) {'), c1 = src.indexOf('\n  }\n', c0) + 4, fr = src.match(/  const mcFree = r => [^\n]+\n/);
    ok('P0 the page has the band, its check and THE check that asks it', a > 0 && m > a && c0 > 0 && !!fr);
    const restored = [];
    const env = { W: { rec: null }, window: { CharmNestRose: R, RoseStock: { restore: async (sh, rec) => { restored.push(sh.sheetId); } } }, laserCut: sh => +sh.laserDoneAt > 0, sheetWord: () => 'RG Sheet 2',
      sizeOf: c => (c && c.manual && +c.manual.size) || 1, scaleOf: size => +(SC * size).toFixed(5), frozenWhy: () => '', tryDo: f => { try { return f(); } catch (_) { return undefined; } }, mcButton: () => {},
      normDeg: x => ((x % 360) + 360) % 360, clampSize: x => x, MANUAL_METALS: ['gold10k', 'gold14k'] };
    const names = Object.keys(env);
    const mk = new Function(...names, 'CharmNestRose', src.slice(a, b) + src.slice(c0, c1) + fr[0] + '\nreturn { rgBand, rgCut, mcLine, mcCheck, mcFree };');
    const mcVerify = () => ({ ok: true }), SVs = () => ({ tryPlace: () => ({ ok: false }) });
    const P = new Function('mk', 'env', 'R', 'mcVerify', 'SV', 'return mk(...Object.values(env), R);')(mk, env, R, mcVerify, SVs);
    // THE check's own helpers it reads by name: given in its scope through the same Function
    const P2 = new Function(...names, 'CharmNestRose', 'mcVariant', 'mcVerify', 'SV', 'mcInCorner', 'mcCornerWords', 'mcNear', src.slice(a, b) + src.slice(c0, c1) + fr[0] + '\nreturn { rgBand, rgCut, mcLine, mcCheck, mcFree };')(
      ...Object.values(env), R, () => ({ solid: { cx: 0, cy: 0, w: 20, h: 20 }, fine: { pm: {} } }), mcVerify, SVs, () => false, () => '', () => 'a charm');
    const hist = [{ revision: 2, sheetId: 'rg-cut', plan: ownPlan }, { revision: 1, sheetId: 'rg-prev', plan: prevPlan }];
    const rec = { id: 'rg-cut', metal: 'rose', roseCutAt: 1, roseCutRevision: 2, roseRevision: 1, rosePlanJson: JSON.stringify(ownPlan), rosePlanHash: 'ph', roseFingerprint: fingerprint({ placements: ownPl, charms: ownCharms }), roseStockId: 'rgs-t1' };
    const sheet = o => Object.assign({ sheetId: 'rg-cut', metal: 'rose', roseCutAt: 1, _standalone: true, _rec: rec, _roseLoaded: true, roseHistory: hist, roseStock: { revision: 2 }, placements: ownPl.map(p => Object.assign({}, p)), charms: ownCharms.map(c => Object.assign({ bits: new Uint8Array(4), w: 2, h: 2 }, c)) }, o || {});   // (a drawing THE check can read)
    const B = P2.rgBand(sheet());
    ok('P1 a Rose Gold sheet with its cut recorded: its band is its line and the outline the earlier cut left', !!(B && B.cut && B.prior) && JSON.stringify(B.cut.values) === JSON.stringify(ownPlan.profile.values) && JSON.stringify(B.prior.values) === JSON.stringify(prevPlan.profile.values) && B.shapes.size === 5 && B.from.size === 5, B && B.why);
    const B2 = P2.rgBand(sheet({ sheetId: 'rg-cut2', roseHistory: [hist[0], Object.assign({}, hist[1], { setAsideAt: 5 })] }));
    ok('P2 an earlier cut set aside never counts: the band starts at the sheet\'s edge', !!(B2 && B2.cut) && B2.prior === null, B2);
    const B3 = P2.rgBand(sheet({ sheetId: 'rg-cut3', _roseLoaded: false }));
    await new Promise(r => setTimeout(r, 5));
    ok('P3 its physical sheet not read yet: Manual Control waits in words, and it is read once (RoseStock.restore)', !!(B3 && B3.why) && /being read/.test(B3.why) && restored.join() === 'rg-cut3', { B3, restored });
    const drawn = Object.assign({}, rec, { id: 'rg-line', roseCutAt: null, roseCutRevision: null, roseRevision: 4 });
    const B4 = P2.rgBand(sheet({ sheetId: 'rg-line', roseCutAt: null, _rec: drawn, roseStock: { revision: 4, profileJson: JSON.stringify(prevPlan.profile) } }));
    const B5 = P2.rgBand(sheet({ sheetId: 'rg-line2', roseCutAt: null, _rec: Object.assign({}, drawn, { id: 'rg-line2' }), roseStock: { revision: 5, profileJson: JSON.stringify(prevPlan.profile) } }));
    ok('P4 a line drawn only: its band starts at its physical sheet as the line found it; a physical sheet that changed since asks for the line again', !!(B4 && B4.cut && B4.prior) && !!(B5 && B5.why) && /changed since its green line was drawn/.test(B5.why), { B5 });
    ok('P5 10K, 14K, Gold Filled, a laser-cut sheet and a Rose Gold sheet with no line have no band (their own rules)', [{ metal: 'gold14k' }, { metal: 'gold10k' }, { metal: 'gold' }, { laserDoneAt: 9 }, { roseCutAt: null, _rec: Object.assign({}, rec, { roseCutAt: null, rosePlanJson: null }), rosePlanHash: null }].every(o => P2.rgBand(sheet(Object.assign({ sheetId: 'x' + Math.random() }, o))) === null));
    const sh = sheet(), G = { band: P2.rgBand(sh), grid: { fits: () => true }, edge: { fits: () => true }, sheet: {} }, c = id => sh.charms.find(x => x.id === id);
    ok('P6 in place and moved left on its own metal: no words', P2.mcLine(G, c('a'), { cxPt: 50, cyPt: 14, angle: 0 }) === '' && P2.mcLine(G, c('a'), { cxPt: 45, cyPt: 14, angle: 0, size: 1 }) === '');
    const w = P2.mcLine(G, c('c'), { cxPt: 80, cyPt: 14, angle: 0 }), w2 = P2.mcLine(G, c('c'), { cxPt: 76, cyPt: 14, angle: 0, size: 1.2 });
    ok('P7 past the line, moved or made bigger: refused in words that say it stays to the left of the line', /would cross this sheet's green line.*to the left of it/.test(w) && /green line/.test(w2), { w, w2 });
    const r1 = P2.mcCheck(G, c('c'), { cxPt: 80, cyPt: 14, angle: 0, size: 1 }, 1, true), r2 = P2.mcCheck(G, c('a'), { cxPt: 45, cyPt: 14, angle: 0, size: 1 }, 1, true);
    ok('P8 THE check: past the line is "line", never a free (red) placement, so the charm springs back; on its own metal it passes', r1.ok === false && r1.why === 'line' && !P2.mcFree(r1) && r2.ok === true, { r1: { ok: r1.ok, why: r1.why }, r2: { ok: r2.ok, why: r2.why } });
    ok('P9 a sheet with no band: THE check is as before', P2.mcCheck({ band: null, grid: { fits: () => true } }, c('c'), { cxPt: 300, cyPt: 14, angle: 0, size: 1 }, 0, false).ok === true);
    // the wiring around it
    ok('P10 Manual Control waits for the band, the outline before the line replaces the stock\'s, a charm in a saved line moves, the save refuses a pose past the line, a released set hears of the change',
      /const band = rgBand\(sh\); if \(band && band\.why\) return band\.why;/.test(src) && /const band = rgCut\(sh\); if \(band\) \{ if \(band\.prior\) out\.remnant = band\.prior; else delete out\.remnant; return out; \}/.test(src)
      && /sh\.roseProtected && !rgCut\(sh\) &&/.test(src) && /res\.why === "line"\) throw new Error/.test(src) && /MANUAL_METALS\.includes\(sh\.metal\) \|\| sh\.metal === "rose"\) \|\| !allSheets\(\)\.includes\(sh\)\) return;/.test(src)
      && /band: rgCut\(sh\) \};/.test(src) && /band: E\.band \|\| null \};/.test(src));
  });

  await part('S', async () => {
    const T0 = Date.UTC(2026, 10, 27, 18, 0, 0), F = require('./_cloudFakes.cjs')({ now: T0 }), lib = F.load('charmNestLibrary.js');
    const quiet = [console.log, console.warn, console.error]; console.log = () => {}; console.warn = () => {}; console.error = () => {};
    const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
    const doc = p => F.doc(p) || {};
    const charmsRec = ownCharms.map(c => ({ id: c.id, hash: c.hash, poolId: c.id + '_p', order: '4250000001', widthPt: 20, heightPt: 20 }));
    const base = { metal: 'rose', runId: 'run-rg', fileBase: 'RG_Nov.23.26_Set-1_Sheet-2', archived: false, draft: false, setId: 'set-rg', charms: charmsRec, placements: ownPl.map(p => Object.assign({}, p)), stock: { wPt: WPT, hPt: HPT }, verification: { ok: true } };
    const seed = () => {
      F.reset(); F.clock.set(T0);
      F.seed('Charm_Nest_Rose_Stock/rgs-t1', { id: 'rgs-t1', metal: 'rose', revision: 2, owner: 'rg-hold', available: false, lastCutSheetId: 'rg-cut', profileJson: JSON.stringify(ownPlan.profile), wPt: WPT, hPt: HPT });
      F.seed('Charm_Nest_Rose_Stock/rgs-t1/cuts/k1', { revision: 1, sheetId: 'rg-prev', planJson: JSON.stringify(prevPlan) });
      F.seed('Charm_Nest_Rose_Stock/rgs-t1/cuts/k2', { revision: 2, sheetId: 'rg-cut', planJson: JSON.stringify(ownPlan) });
      F.seed('Charm_Nest_Sheets/rg-cut', Object.assign({ id: 'rg-cut', roseStockId: 'rgs-t1', roseCutAt: T0 - 86400e3, roseCutRevision: 2, roseRevision: 1, rosePlanJson: JSON.stringify(ownPlan), rosePlanHash: 'ph', roseFingerprint: fingerprint({ placements: ownPl, charms: charmsRec }), processReady: true, laserSetPending: true }, base));
      F.seed('Charm_Nest_Rose_Stock/rgs-t2', { id: 'rgs-t2', metal: 'rose', revision: 4, owner: 'rg-line', available: false, profileJson: JSON.stringify(prevPlan.profile), wPt: WPT, hPt: HPT });
      F.seed('Charm_Nest_Sheets/rg-line', Object.assign({ id: 'rg-line', roseStockId: 'rgs-t2', roseRevision: 4, rosePlanJson: JSON.stringify(ownPlan), rosePlanHash: 'ph2', roseFingerprint: fingerprint({ placements: ownPl, charms: charmsRec }) }, base));
      F.seed('Charm_Nest_Sheets/rg-laser', Object.assign({ id: 'rg-laser', roseStockId: 'rgs-t1', roseCutAt: T0 - 86400e3, laserDoneAt: T0 - 3600e3 }, base));
      F.seed('Charm_Nest_Sheets/k14-cut', Object.assign({}, base, { id: 'k14-cut', metal: 'gold14k', roseStockId: 'rgs-t3', roseCutAt: T0 - 86400e3, roseCutRevision: 1, rosePlanJson: JSON.stringify(ownPlan) }));
    };
    const patch = (id, poses, extra) => P(Object.assign({ op: 'sheetLayoutPatch', id, tab: 'tab-rg-1', by: 'Tester', poses, pins: {}, mark: { at: T0, by: 'Tester', n: Object.keys(poses).length } }, extra || {}));
    const pose = (id, cxPt, cyPt, angle = 0, scale = SC) => ({ [id]: { cxPt, cyPt, angle, scale } });
    seed();
    const cl = await P({ op: 'manualClaim', id: 'rg-cut', tab: 'tab-rg-1', by: 'Tester' }), cl2 = await P({ op: 'manualClaim', id: 'rg-laser', tab: 'tab-rg-1', by: 'Tester' });
    ok('S1 a Rose Gold sheet whose green line cut is recorded is claimed for Manual Control; one cut on the laser is not', cl.ok === true && !!cl.held && cl2.ok === false && cl2.refused === 'cut', { cl, cl2 });
    const stock0 = JSON.stringify(doc('Charm_Nest_Rose_Stock/rgs-t1')), rec0 = doc('Charm_Nest_Sheets/rg-cut');
    const s2 = await patch('rg-cut', pose('a', 45, 14)), rec1 = doc('Charm_Nest_Sheets/rg-cut');
    ok('S2 a charm moved left on its own metal is saved; the cut, the line, its fingerprint and its revision stay; the files are owed again',
      s2.ok === true && s2.moved === 1 && rec1.placements.find(p => p.id === 'a').cxPt === 45 && rec1.roseCutAt === rec0.roseCutAt && rec1.rosePlanJson === rec0.rosePlanJson && rec1.roseFingerprint === rec0.roseFingerprint && rec1.roseCutRevision === 2 && rec1.rosePlanHash === 'ph' && rec1.filesOwed > 0, s2);
    ok('S3 its physical sheet and the leftover another sheet holds are not touched', JSON.stringify(doc('Charm_Nest_Rose_Stock/rgs-t1')) === stock0);
    const before = JSON.stringify(doc('Charm_Nest_Sheets/rg-cut').placements);
    const s4 = await patch('rg-cut', pose('c', 80, 14)), s5 = await patch('rg-cut', pose('c', 76, 14, 0, SC * 1.2)), s6 = await patch('rg-cut', pose('e', 55, 100, 90));
    ok('S4 moved past the line, made bigger past it, turned past it: refused as "line", in words, nothing written', [s4, s5, s6].every(r => r.http === 409 && r.refused === 'line' && /would cross its green line, which stays where it was drawn, so its charms stay to the left of it/.test(r.error || ''))
      && JSON.stringify(doc('Charm_Nest_Sheets/rg-cut').placements) === before, [s4, s5, s6].map(r => [r.http, r.refused, r.error]));
    const s7 = await patch('rg-cut', Object.assign(pose('c', 75, 14, 0, SC * 0.8), pose('a', 47, 14)));
    ok('S5 made smaller and moved within its metal, two charms at once: saved', s7.ok === true && s7.moved === 2, s7);
    const s8 = await patch('rg-cut', pose('a', 25, 14));
    ok('S6 onto metal the earlier cut took: not this refusal (the page keeps it red and out of every set, as metal cut away on any sheet)', s8.ok === true, s8);
    // a charm the line was not drawn for
    { const r = doc('Charm_Nest_Sheets/rg-cut'); F.seed('Charm_Nest_Sheets/rg-cut', Object.assign({}, r, { placements: r.placements.concat([at('z', 60, 120)]), charms: r.charms.concat([{ id: 'z', hash: 'h-z', poolId: 'z_p', order: '4250000009' }]) })); }
    const s9 = await patch('rg-cut', pose('z', 61, 120));
    ok('S7 a charm its line was not drawn for is refused, nothing written', s9.http === 409 && s9.refused === 'line', s9);
    // a line drawn only (no cut): the same barrier, the physical sheet as the line found it
    const s10 = await patch('rg-line', pose('a', 46, 14)), s11 = await patch('rg-line', pose('d', 85, 40)), lr = doc('Charm_Nest_Sheets/rg-line');
    ok('S8 a line drawn only: a move on its own metal is saved with the line kept; past it refused', s10.ok === true && lr.rosePlanJson === JSON.stringify(ownPlan) && lr.rosePlanHash === 'ph2' && s11.refused === 'line', { s10, s11 });
    F.seed('Charm_Nest_Rose_Stock/rgs-t2', Object.assign({}, doc('Charm_Nest_Rose_Stock/rgs-t2'), { revision: 5 }));
    const s12 = await patch('rg-line', pose('a', 47, 14));
    ok('S9 its physical sheet changed since the line was drawn: refused in words, nothing written', s12.refused === 'line' && /changed since its green line was drawn/.test(s12.error || '') && doc('Charm_Nest_Sheets/rg-line').placements.find(p => p.id === 'a').cxPt === 46, s12);
    // an earlier cut set aside: the band starts at the sheet's edge
    seed(); F.seed('Charm_Nest_Rose_Stock/rgs-t1/cuts/k1', { revision: 1, sheetId: 'rg-prev', planJson: JSON.stringify(prevPlan), setAsideAt: T0 - 1000 });
    const s13 = await patch('rg-cut', pose('a', 25, 14));
    ok('S10 a set-aside earlier cut never counts (the band starts at the sheet\'s edge): saved', s13.ok === true, s13);
    const s14 = await patch('k14-cut', pose('a', 45, 14));
    ok('S11 a 10K / 14K sheet keeps its rule: its recorded cut is set aside first (refused here, the drive off)', s14.http === 409 && s14.refused === 'line' && /set aside before its charms change/.test(s14.error || ''), s14);
    const s15 = await patch('rg-laser', pose('a', 45, 14));
    ok('S12 a Rose Gold sheet cut on the laser: refused "cut"', s15.http === 409 && s15.refused === 'cut', s15);
    [console.log, console.warn, console.error] = quiet;
  });

  log0(`rt1127-rghand: ${n - failed} passed, ${failed} failed`); process.exit(failed ? 1 : 0);
})();
