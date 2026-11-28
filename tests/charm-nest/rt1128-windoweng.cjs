/* WINDOWENG (Paul, 28 Nov 2026): the sheet window must draw exactly what the downloaded .ai shows. On production RG_Nov.26.26_Set-1_Sheet-1 drew "Ada Love" on piece
   4246616737_5310628394_1 (TINY_7557) and GF draft d-34970fa918a0793f drew "IOWA" on piece 4251664563_5317870826_1 (CURB-BEADY-2): neither piece had a saved engraving
   record, the words came from the engraving job (charm-nest-sheetwin.js engGeoOf fell back to the job's face record and backOfJob), and the .ai never holds them.
   The download composes ONLY the sheet's saved engraving records (charm-nest-export-ui.js build / missingBacks, through CharmNestBacks.forSheet): right face, not invalidated,
   not pending, with its file (outputs.ai or ai).
   Offline, Node only: the sheet window's own functions (engOf, backsOf, engGeoOf and every place that draws words on a plate: paintBase front and back, markBack (the marks layer,
   Manual Control's mates), the order view / Library plate paintOrderBase with its "not drawable yet" label, the Nest card's backPiece / frontWords, the piece panel's picture drawThumb)
   run from charm-nest-sheetwin.js as they stand, over a recording canvas; the download's own build runs from charm-nest-export-ui.js with the real CharmNestBacks.forSheet, up to its compose step.
     A  a piece with only an engraving job's words (in Review, the cloud's place of one, approved but not written) draws no words anywhere, and gets no label
     B  a saved, approved, not invalidated record with its file (front and back) is drawn, its own words, on the plate's matching face
     C  an invalidated record, a pending record, a record without its file: nothing drawn
     D  what the window draws is exactly what the download composes, piece by piece and face by face
     E  the page loads the new file (cache token -we1)
     T  the piece panel's picture (drawThumb): the charm with its saved words only, front and back, never CharmNestMadeThumb's picture "as made" (an Engrave job's words), and drawn
        again when a record is saved while the panel is open
   node tests/charm-nest/rt1128-windoweng.cjs */
'use strict';
const fs = require('fs'), path = require('path');
const ROOT = path.join(__dirname, '../..');
let n = 0, failed = 0; const ok = (m, c, d) => { n++; console.log((c ? 'OK   ' : 'FAIL ') + m + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 700))); if (!c) failed++; };
const read = f => fs.readFileSync(path.join(ROOT, f), 'utf8');
function slice(src, file, from, to) {
  const a = src.indexOf(from), b = src.indexOf(to, a + from.length);
  if (a < 0 || b < 0) throw new Error(`${file}: "${from.trim().slice(0, 60)}" .. "${to.trim().slice(0, 60)}" not found`);
  return src.slice(a, b);
}
const Backs = require(path.join(ROOT, 'charm-nest-backs.js'));
const Faces = require(path.join(ROOT, 'charm-nest-engrave-faces.js'));

/* ── the pieces of the sheet ── */
const T = Date.UTC(2026, 10, 27, 15, 0, 0), SHEET = 'rg-nov26-s1-sheet1';
const P = {
  ada: '4246616737_5310628394_1',    // TINY_7557: a FRONT job in Review, "Ada Love", fitted here; no record
  iowa: '4251664563_5317870826_1',   // CURB-BEADY-2: a back job in Review taken from the cloud, "IOWA", only the cloud's place (keepPlace); no record
  grace: '4250000001_5310000001_1',  // a back job approved here, fitted, not written yet: no record
  maya: '4250000002_5310000002_1',   // a saved front record "Maya" and a saved back record "Love you", both with their file
  old: '4250000003_5310000003_1',    // an invalidated front record "Old words" (with its file) and a front job in Review "New words"
  pend: '4250000004_5310000004_1',   // a back record still being saved (pending, no file)
  nofile: '4250000005_5310000005_1', // a back record with no file
  label: '4250000006_5310000006_1',  // a saved back record "Label me" on a piece whose drawing is not read yet (the plate's label)
  hidden: '4250000007_5310000007_1'  // a back job in Review "Hidden" on a piece whose drawing is not read yet
};
const file = (id, f) => ({ ai: { url: `https://storage.example/charmnest/backs/${id}${f === 'front' ? '~front' : ''}.ai` }, png: { url: `https://storage.example/charmnest/backs/${id}.png` } });
const recOf = (poolId, words, o = {}) => Object.assign({ poolId, sheetId: SHEET, text: words, lines: words.split('\n'), sizePt: 8, weight: 'Regular', centre: [0, 0], angle: 0, lineGap: 1, upAngle: 90,
  approvedAt: T, approvedBy: 'Paul_K', font: 'Source Sans 3', fontKey: 'source-sans-3' }, o);
const RECORDS = [
  recOf(P.maya, 'Maya', { face: 'front', outputs: file(P.maya, 'front') }),
  recOf(P.maya, 'Love you', { outputs: file(P.maya) }),
  recOf(P.old, 'Old words', { face: 'front', outputs: file(P.old, 'front'), invalidated: true }),
  recOf(P.pend, 'Pending', { pending: true }),
  recOf(P.nofile, 'No file'),
  recOf(P.label, 'Label me', { outputs: file(P.label) })
];
const fit = () => ({ size: 9, capMm: 1.2, weight: 'Regular', centre: [0, 0], angle: 0, layout: { lineGap: 1 } });
const job = (key, poolId, face, words, o = {}) => Object.assign({ key: key + (face === 'front' ? '~front' : ''), rowKey: key, face, copies: [poolId], state: 'review', text: words, lines: words.split('\n'),
  fit: fit(), view: { upAngle: 90 }, approvedAt: 0, fontKey: 'source-sans-3', fontName: 'Source Sans 3', backs: [] }, o);
const JOBS = [
  job('4246616737_5310628394', P.ada, 'front', 'Ada Love'),
  job('4251664563_5317870826', P.iowa, 'back', 'IOWA', { fit: null, view: null, lineInput: ['IOWA'], keepPlace: { centre: [0, 0], size: 8, lines: ['IOWA'], weight: 'Regular', angle: 0, lineGap: 1, upAngle: 90, fontKey: 'source-sans-3' } }),
  job('4250000001_5310000001', P.grace, 'back', 'Grace', { state: 'approved', approvedAt: T, approvedBy: 'Paul_K' }),
  job('4250000002_5310000002', P.maya, 'front', 'Maya', { state: 'written', approvedAt: T, approvedBy: 'Paul_K' }),
  job('4250000002_5310000002', P.maya, 'back', 'Love you', { state: 'written', approvedAt: T, approvedBy: 'Paul_K' }),
  job('4250000003_5310000003', P.old, 'front', 'New words'),
  job('4250000004_5310000004', P.pend, 'back', 'Pending', { state: 'approved', approvedAt: T, approvedBy: 'Paul_K' }),
  job('4250000006_5310000006', P.label, 'back', 'Label me', { state: 'written', approvedAt: T, approvedBy: 'Paul_K' }),
  job('4250000007_5310000007', P.hidden, 'back', 'Hidden')
];
const IDS = Object.values(P), NO_DRAWING = new Set([P.label, P.hidden]);
const charmOf = id => ({ id: 'c:' + id, poolId: id, centerPt: [10, 10], widthPt: 20, heightPt: 20, bbox: [0, 0, 20, 20], outline: { bbox: [0, 0, 20, 20] } });
const sheetRec = backPool => ({ id: SHEET, fileBase: 'RG_Nov.26.26_Set-1_Sheet-1', metal: 'rose', poolIds: IDS.slice(), outputs: { ai: { url: 'https://storage.example/sheet.ai' } },
  charms: IDS.map((id, i) => ({ id: 'c:' + id, poolId: id, index: i })), placements: IDS.map((id, i) => ({ id: 'c:' + id, x: i * 30, y: 0 })), backPool: backPool.map(r => Object.assign({}, r)),
  engraving: Object.fromEntries(IDS.map(id => [id, { needed: true, faces: { back: { needed: true }, ...([P.ada, P.maya, P.old].includes(id) ? { front: { needed: true } } : {}) } }])) });

/* ── a canvas that records ── */
function canvas() {
  const c = { width: 200, height: 200, isConnected: true, texts: [] };
  const ctx = new Proxy({ measureText: s => ({ width: String(s).length * 6 }), fillText: (s) => c.texts.push(String(s)) }, { get: (t, k) => (k in t ? t[k] : () => {}), set: (t, k, v) => { t[k] = v; return true; } });
  c.getContext = () => ctx; c.ctx = ctx; return c;
}

/* ── the sheet window's own code, from its file ── */
function sheetWindow(env) {
  const F = 'charm-nest-sheetwin.js', src = read(F), s = (a, b) => slice(src, F, a, b);
  const code = [
    s('  const FACES = () =>', '  /* ── the sheet pane: its orders ── */'),                                  // FACES, isFrontRec, recKeyOf, jobsOfPiece, engFace, combineEng, engOf, faceTextOf
    s('  function backsOf(rec, live, extra) {', '  /** The back side asked for:'),                         // backsOf, the fonts, and what the plate reads a record from
    s('  function engGeoOf(x, backs, face) {', '  /* SHEETBACK (Paul, 28 Oct 2026'),                       // engGeoOf, dropGeo
    s('  const artPaint = s =>', '  /** The words typed among them'),                                      // faceArtList, backArtOf
    s('  function paintBase() {', '  /* SHIPPEDSTAYS'),                                                     // the window's plate, front and back
    s('  function markBack(ctx, x) {', '  // a point clicked on a picture'),                                // the marks layer (a lit charm, Manual Control's mates)
    s('  const orderHighlights = G =>', '  /** The plate on screen: its still layer'),                     // the order view's / Library's plate, with its label
    s('  function backPiece(ctx, x, backs, k, o = {}) {', '  /** Does a sheet (a page of the Nest tab'),    // a Nest card's back and front
    s('  function drawThumb(cv, x) {', '  /** MSSTALE:')                                                    // the piece panel's picture (and, on this branch, what it reads)
  ].join('\n') + '\nreturn { engOf, backsOf, engGeoOf, dropGeo, paintBase, markBack, paintOrderBase, backPiece, frontWords, drawThumb, thumbSig: typeof thumbSig === "function" ? thumbSig : null, thumbBacks: typeof thumbBacks === "function" ? thumbBacks : null };';
  const names = Object.keys(env);
  return new Function(...names, code)(...names.map(k => env[k]));
}
function world() {
  const drawn = [], state = { back: false, where: '' };
  const Engrave = { items: () => new Map(JOBS.map(j => [j.key, j])), fonts: { ok: true, Regular: { f: 'R' }, Semibold: { f: 'S' } }, fontSetFor: () => null, isWorking: () => false, canFit: () => true };
  const CNB = Object.assign({}, Backs, {
    engraveOn: (rec, c) => (rec && !rec.invalidated ? { glyphs: [[0, 0]], text: (rec.lines || []).join('\n') || rec.text, poolId: rec.poolId, face: rec.face === 'front' ? 'front' : 'back' } : null),
    drawEngrave: (ctx, geo) => { drawn.push({ where: state.where, poolId: geo.poolId, face: geo.face, text: geo.text }); return true; }
  });
  // (a stand-in for the page's picture "as made" (charm-nest-made-thumb.js): it paints a job's fitted words, Review or not, as the real one does; the window must never ask it)
  const made = [], CharmNestMadeThumb = { paintInto: (cv, poolId) => { made.push(poolId); for (const j of JOBS) if (j.copies.includes(poolId) && j.fit && j.view) drawn.push({ where: state.where, poolId, face: j.face, text: j.text, made: true }); return Promise.resolve(true); } };
  const win = { CharmNestBacks: CNB, CharmNestEngraveFaces: Faces, Engrave, CharmNestOrders: { fontOfRecord: r => r.fontKey || 'source-sans-3' }, CharmNestMadeThumb };
  const W = { rec: null, el: {}, st: { wPt: 400, hPt: 200 }, R: 0, k: 1, geom: true, pre: null, pieces: [], backs: null, dpr: 1 };
  const env = {
    window: win, Engrave, W, CharmNestBacks: CNB, CharmNestOrders: win.CharmNestOrders, CharmNestPDF: { drawCharm: () => {}, pathToCanvas: () => {} },
    tryDo: f => { try { return f(); } catch (e) { console.log('     (threw: ' + (e && e.message) + ')'); return undefined; } },
    zoomed: () => false, lineK: () => 1, OX: () => 0, OY: () => 0, roseUnder: () => {}, sheetEdge: () => {}, MC: { lift: null }, offView: () => false,
    withPiece: (ctx, x, fn) => fn(), outlinePath: () => {}, backFace: () => state.back, drawBackArt: () => false, tx0: () => (x, y) => [x, y], backInk: () => '#2f6fd0', markShipped: () => {},
    layoutOrder: () => true, document: { createElement: () => canvas() }, drawRulers: () => {}, orderPiece: (ctx, G, x, fn) => fn(), orderOutline: () => {}, drawOf: () => {}, cutLinesOf: () => [],
    liveOf: () => null, cors: u => u, Image: function () {}
  };
  const SW = sheetWindow(env);
  return { SW, W, drawn, state, made };
}
/** Everything one plate draws of the sheet `rec`, from the window (paintBase front / back, markBack front / back), the order view (front / back, its labels) and a Nest card (back / front). */
function drawAll(rec, live) {
  const { SW, W, drawn, state, made } = world();
  const pieces = IDS.map((id, i) => ({ id: 'c:' + id, poolId: id, rid: id.split('_')[0], c: NO_DRAWING.has(id) ? null : charmOf(id), p: { cxPt: 20 + i * 30, cyPt: 20, wPt: 20, hPt: 20, scale: 1 } }));
  for (const x of pieces) x.eng = SW.engOf(x, rec);
  W.rec = rec; W.id = rec.id; W.live = live || null; W.backsList = null; W.pieces = pieces; W.backs = SW.backsOf(rec, live || null); W.el.base = canvas();
  for (const back of [false, true]) {
    state.back = back; state.where = back ? 'window back' : 'window front'; for (const x of pieces) SW.dropGeo(x); SW.paintBase();
    state.where = back ? 'marks back' : 'marks front'; for (const x of pieces) SW.markBack(W.el.base.ctx, x);
  }
  const labels = {};
  for (const back of [false, true]) {
    const cv = canvas(), G = { cv, base: canvas(), rec, live: live || null, pieces, mine: [], search: null, focus: null, img: null, backSide: back, thumb: true, bare: false, backs: SW.backsOf(rec, live || null), R: 0, dpr: 1, k: 1, Wp: 400, Hp: 200 };
    state.where = back ? 'order back' : 'order front'; for (const x of pieces) SW.dropGeo(x); SW.paintOrderBase(G); labels[back ? 'back' : 'front'] = G.base.texts.slice();
  }
  const card = canvas(), cb = SW.backsOf({ id: rec.id, backPool: rec.backPool }, null);
  state.where = 'card back'; for (const x of pieces) { SW.dropGeo(x); SW.backPiece(card.ctx, x, cb, 1); }
  state.where = 'card front'; for (const x of pieces) { SW.dropGeo(x); SW.frontWords(card.ctx, x, cb, 1); }
  state.where = 'thumb'; const thumbs = {}; for (const x of pieces) { SW.dropGeo(x); const cv = canvas(); SW.drawThumb(cv, x); thumbs[x.poolId] = cv; }
  return { drawn, labels, made, thumbs, SW, W, pieces, state };
}

/* ── the download's own build (charm-nest-export-ui.js), up to its compose step ── */
async function download(sheet) {
  const F = 'charm-nest-export-ui.js', src = read(F), s = (a, b) => slice(src, F, a, b);
  const STOP = new Error('STOP'); let got = null;
  const env = {
    CharmNestBacks: Backs, CharmNestPDF: {}, TextEncoder,
    bytes: async o => { const u = typeof o === 'string' ? o : o && o.url; if (!u) throw new Error('no file to read'); return new Uint8Array([1]); },
    CharmNestExport: { compose: async (sheetBytes, sh, backs, fronts) => { got = { backs: backs || [], fronts: fronts || [] }; throw STOP; } }
  };
  const code = s('  const isFront=r=>', '  /** report(stage') + s('  async function build(sheet,format,report) {', '  /** IMPORT.txt') + '\nreturn { build, missingBacks };';
  const names = Object.keys(env), D = new Function(...names, code)(...names.map(k => env[k]));
  let error = null; try { await D.build(sheet, 'ai'); } catch (e) { if (e !== STOP) error = e.message; }
  const composed = got && !error ? [...got.backs.map(b => ({ poolId: b.poolId, face: 'back', text: b.text })), ...got.fronts.map(b => ({ poolId: b.poolId, face: 'front', text: b.text }))] : [];
  return { error, composed, missing: D.missingBacks(sheet) };
}

const on = (drawn, where) => drawn.filter(d => d.where === where);
const names = list => list.map(d => `${d.poolId}:${d.face}:${d.text}`).sort();

(async () => {
  const rec = sheetRec(RECORDS), all = drawAll(rec), { drawn, labels } = all;
  const words = id => drawn.filter(d => d.poolId === id);
  const allLabels = [...labels.front, ...labels.back];

  // A — a job's words, never saved, are drawn nowhere
  ok('A1 TINY_7557 (4246616737_5310628394_1): its front job in Review, "Ada Love", draws no words on any plate (window, marks, order view, Nest card) nor in the piece panel\'s picture', !words(P.ada).length && !drawn.some(d => d.text === 'Ada Love'), words(P.ada));
  ok('A2 CURB-BEADY-2 (4251664563_5317870826_1): its back job taken from the cloud, "IOWA" (only the cloud\'s place), draws no words', !words(P.iowa).length && !drawn.some(d => d.text === 'IOWA'), words(P.iowa));
  ok('A3 a back approved here but not written yet ("Grace", no record): no words', !words(P.grace).length, words(P.grace));
  ok('A4 a piece not drawn yet whose only words are a job\'s ("Hidden"): no label on the plate either', !allLabels.includes('Hidden'), labels);
  ok('A5 no plate says "Ada Love", "IOWA", "Grace" or "New words" in any way', !drawn.some(d => ['Ada Love', 'IOWA', 'Grace', 'New words'].includes(d.text)) && !allLabels.some(t => ['Ada Love', 'IOWA', 'Grace', 'New words'].includes(t)), { drawn: names(drawn), labels });

  // B — a saved record with its file is drawn, its own words, on its own face
  const maya = words(P.maya);
  ok('B1 a saved, approved front record with its file ("Maya") is drawn on the window\'s front, the marks layer\'s front, the order view\'s front and a Nest card\'s front', ['window front', 'marks front', 'order front', 'card front'].every(w => on(maya, w).some(d => d.face === 'front' && d.text === 'Maya')), names(maya));
  ok('B2 its saved back record ("Love you") is drawn on the window\'s back, the marks layer\'s back, the order view\'s back and a Nest card\'s back', ['window back', 'marks back', 'order back', 'card back'].every(w => on(maya, w).some(d => d.face === 'back' && d.text === 'Love you')), names(maya));
  ok('B3 and never on the other face (no back words on a front, no front words on a back)', !maya.some(d => (/front/.test(d.where) && d.face !== 'front') || (/back/.test(d.where) && d.face !== 'back')), names(maya));
  ok('B4 a saved back record on a piece whose drawing is not read yet is said by the plate\'s label, in the record\'s own words', labels.back.includes('Label me'), labels);

  // C — invalidated, pending, no file: nothing
  ok('C1 an invalidated front record ("Old words"), with its file, beside a job in Review ("New words"): nothing drawn', !words(P.old).length, words(P.old));
  ok('C2 a record still being saved (pending, "Pending"): nothing drawn', !words(P.pend).length, words(P.pend));
  ok('C3 a record without its file ("No file"): nothing drawn', !words(P.nofile).length, words(P.nofile));
  { const live = { sheetId: SHEET, backPool: [recOf(P.ada, 'Ada Love', { face: 'front', pending: true })] }, r2 = drawAll(sheetRec(RECORDS), live);
    ok('C4 a pending front record the page itself holds (the live sheet) for TINY_7557 draws nothing either', !r2.drawn.some(d => d.poolId === P.ada), r2.drawn.filter(d => d.poolId === P.ada)); }

  // D — exactly what the download composes
  const clean = sheetRec(RECORDS.filter(r => ![P.pend, P.nofile].includes(r.poolId))), dl = await download(clean), shown = drawAll(clean).drawn;
  const want = names(dl.composed.filter(d => !NO_DRAWING.has(d.poolId)));
  for (const [where, face] of [['window front', 'front'], ['window back', 'back'], ['order front', 'front'], ['order back', 'back'], ['card front', 'front'], ['card back', 'back'], ['thumb', 'both']])
    ok(`D1 ${where}: the words drawn are exactly the download's ${face === 'both' ? 'front and back' : face} engravings`, !dl.error && JSON.stringify(names(on(shown, where))) === JSON.stringify(want.filter(k => face === 'both' || k.split(':')[1] === face)), { error: dl.error, drawn: names(on(shown, where)), download: want });
  ok('D2 (the download composes Maya\'s front and back and Label me\'s back, nothing of a job)', JSON.stringify(names(dl.composed)) === JSON.stringify([`${P.label}:back:Label me`, `${P.maya}:back:Love you`, `${P.maya}:front:Maya`].sort()), names(dl.composed));
  const dp = await download(sheetRec(RECORDS.filter(r => r.poolId !== P.nofile))), dn = await download(sheetRec(RECORDS.filter(r => r.poolId !== P.pend)));
  ok('D3 the download holds no pending record ("still saving") and no record without a file; the window draws neither', !!dp.error && !!dn.error && !shown.some(d => [P.pend, P.nofile].includes(d.poolId)) && !words(P.pend).length && !words(P.nofile).length, { pending: dp.error, nofile: dn.error });
  const miss = dl.missing.map(m => `${m.poolId}:${m.face}`).sort();
  ok('D4 every face the download names as missing (no saved record) is drawn nowhere in the window', miss.length > 0 && !shown.some(d => miss.includes(`${d.poolId}:${d.face}`)), { missing: miss, drawn: names(shown) });

  // T — the piece panel's picture
  const th = on(drawn, 'thumb');
  ok('T1 the piece panel\'s picture never lays CharmNestMadeThumb\'s picture "as made" (an Engrave job\'s words) over the charm', all.made.length === 0, all.made);
  ok('T2 the picture of a piece with only a job\'s words (Ada Love, IOWA, Grace, New words) or a pending / no-file / invalidated record shows no words', !th.some(d => [P.ada, P.iowa, P.grace, P.old, P.pend, P.nofile, P.hidden].includes(d.poolId)), names(th));
  ok('T3 the picture of a piece with saved front and back records shows both, each its own words ("Maya" front, "Love you" back)', JSON.stringify(names(th.filter(d => d.poolId === P.maya))) === JSON.stringify([`${P.maya}:back:Love you`, `${P.maya}:front:Maya`]), names(th));
  { const SW = all.SW, x = all.pieces.find(p => p.poolId === P.ada), cv = all.thumbs[P.ada];
    const before = SW.thumbSig ? SW.thumbSig(x, SW.thumbBacks()) : null;
    all.W.rec.backPool.push(recOf(P.ada, 'Ada Love', { face: 'front', outputs: file(P.ada, 'front') }));   // (approved and saved while the panel is open)
    const after = SW.thumbSig ? SW.thumbSig(x, SW.thumbBacks()) : null, n0 = all.drawn.length; all.state.where = 'thumb again'; SW.dropGeo(x); SW.drawThumb(cv, x);
    const again = all.drawn.slice(n0);
    ok('T4 once TINY_7557\'s front is approved and saved, what its picture was drawn from has changed (the engraving poll draws it again) and the picture shows "Ada Love"',
      before !== null && cv._weSig === after && before !== after && again.some(d => d.poolId === P.ada && d.face === 'front' && d.text === 'Ada Love' && !d.made), { before, after, again }); }
  ok('T5 the engraving poll draws the selected piece\'s picture again when what it was drawn from changed', /if \(th && th\._weSig !== undefined && thumbSig\(W\.sel, thumbBacks\(\)\) !== th\._weSig\) tryDo\(\(\) => drawThumb\(th, W\.sel\)\)/.test(read('charm-nest-sheetwin.js')));

  // E — the page loads the new file
  ok('E1 charm-nest-1.html loads the new charm-nest-sheetwin.js (token -we1)', /charm-nest-sheetwin\.js\?v=[^"]*-we1(?:-\w+)*"/.test(read('charm-nest-1.html')));

  console.log(`\nrt1128-windoweng: ${n - failed} passed, ${failed} failed`); process.exit(failed ? 1 : 0);
})().catch(e => { console.log('FAIL the test ran to its end :: ' + ((e && e.stack) || e)); process.exit(1); });
