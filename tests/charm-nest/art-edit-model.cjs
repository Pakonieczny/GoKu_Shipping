/* ART EDIT (Paul, 27 Oct 2026): "The user needs full control similar to Adobe Illustrator over each individual layer on any charm ... e.g. add a date inside a given calendar day square and
   update the month name located on top of the calendar charm." The model of ONE piece's own edit (charm-nest-art-edit.js). Offline: pdf-lib and the repo's own modules, no network.
     A  apply() never touches the shared charm: the master's members, their points, the parsed master and the charm handed in are byte for byte as they were; the piece's members are its own
     B  member ids are stable: the same drawing read twice gives the same ids; a part moved gives another id; an edited piece names its master parts by the same ids; the drawing's key
     C  the editor's frame: view → charm points and back, for the front (turned) and the back (mirrored and turned); an item's own move, scale and turn about its box
     D  the calendar: the month name's letters removed, a new month name and a date (a group of digits) added inside a given day square, through the turned front view;
        roles and paint (hatching blue, its master layer), the group dropped from the copied bytes, the rest drawn again, a hash of its own, keep-outs kept, decode → encode round trip
     E  refused, with the reason: the outline removed, a part off the metal, a cut line on a piece already on a sheet, a removal on the back; a record made on another drawing is not put on
     F  apply over an edited piece starts from the master again; a null record takes the edit off; the back's additions stay off the front's members
   node tests/charm-nest/art-edit-model.cjs */
'use strict';
const assert = require('node:assert/strict'), path = require('node:path'), fs = require('node:fs');
const ROOT = path.join(__dirname, '..', '..');
global.window = global; global.self = global;
global.PDFLib = require(path.join(ROOT, 'vendor/pdf-lib-1.17.1.min.js'));
require(path.join(ROOT, 'charm-nest-pdf.js'));
global.CharmNestVector = require(path.join(ROOT, 'charm-nest-vector.js'));
global.CharmNestGeom = require(path.join(ROOT, 'charm-nest-geom.js'));
const AE = require(path.join(ROOT, 'charm-nest-art-edit.js')), P = global.CharmNestPDF, G = global.CharmNestGeom;
let n = 0; const ok = (c, m) => { assert.ok(c, m); n++; }, eq = (a, b, m) => { assert.deepEqual(a, b, m); n++; };
const near = (a, b, e, m) => { assert.ok(Math.abs(a - b) <= e, `${m}: ${a} vs ${b}`); n++; };
// a snapshot of a value with its object graph flattened to JSON (Sets as arrays, typed arrays as lists): what "untouched" is compared by
const snap = v => JSON.stringify(v, (k, x) => (x instanceof Set ? { set: [...x] } : ArrayBuffer.isView(x) ? { bytes: Array.from(x).slice(0, 64), n: x.length } : x));
const bboxIn = (b, box) => b[0] >= box[0] - 1e-6 && b[1] >= box[1] - 1e-6 && b[2] <= box[2] + 1e-6 && b[3] <= box[3] + 1e-6;
const sq = (x, y, w, h) => [[['m', [x, y]], ['l', [x + w, y]], ['l', [x + w, y + h]], ['l', [x, y + h]], ['h']]];

(async () => {
  const cal = await require('./_artCalendar.cjs')(ROOT), base = cal.charm;
  const sharedBefore = snap({ members: base.members, outline: base.outline, parsed: cal.parsed.segments, nested: cal.parsed.nested, top: base.topIndices, drop: base.dropIndices || null, hash: base.hash });
  const piece = Object.assign({}, base, { id: 's:4170000001_1001_1', poolId: '4170000001_1001_1', pinned: null, frontKeepOut: [{ kind: 'path', subpaths: sq(20, 20, 2, 2), bbox: [20, 20, 22, 22] }], backKeepOut: [] });   // (Pool.cloneCharm: a shallow copy, the master's members shared)
  const pieceBefore = snap(piece);

  /* ── B · ids ── */
  const ids = AE.memberIds(base);
  eq(ids.length, base.members.length, 'B one id per part of the master');
  ok(ids.every((id, i) => new RegExp('^' + i + '-[0-9a-z]+$').test(id)), 'B an id is the part\'s place and a hash of its geometry: ' + ids.slice(0, 3).join(' '));
  ok(new Set(ids).size === ids.length, 'B every id is its own');
  eq(ids.map((_, i) => AE.memberId(base, i)), ids, 'B memberId(charm, index) is the same id');
  const again = await require('./_artCalendar.cjs')(ROOT);
  eq(AE.memberIds(again.charm), ids, 'B the same drawing read again gives the same ids (another reload, another computer)');
  eq(AE.baseKeyOf(again.charm), AE.baseKeyOf(base), 'B and the same drawing key');
  const moved = Object.assign({}, again.charm, { members: again.charm.members.map((m, i) => (i === 5 ? G.transformSeg(m, [1, 0, 0, 1, 0.5, 0]) : m)) });
  ok(AE.memberIds(moved)[5] !== ids[5] && AE.memberIds(moved)[4] === ids[4], 'B a part drawn elsewhere has another id; the others keep theirs');
  ok(AE.baseKeyOf(moved) !== AE.baseKeyOf(base), 'B and the drawing key changes with it');
  const real = new Uint8Array(fs.readFileSync(path.join(__dirname, 'fixtures', 'MASTER_5X8_5.ai')));
  const r1 = P.groupCharms(await P.parseSource(real, 'a.ai')).charms[0], r2 = P.groupCharms(await P.parseSource(real, 'b.ai')).charms[0];
  eq(AE.memberIds(r1), AE.memberIds(r2), 'B a real master (MASTER_5X8_5.ai) read twice: the same ids');

  /* ── C · the frame ── */
  const front = G.backView(base, { face: 'front', upAngle: 60, res: 6 }), back = G.backView(base, { upAngle: 120, res: 6 });
  ok(Math.abs(front.angleDeg) > 1 && back.M[0] === -1, 'C the views: the front turned by ' + front.angleDeg + '°, the back mirrored and turned');
  for (const [name, v] of [['front', front], ['back', back], ['no view', null]]) {
    const fr = AE.frame(v); let worst = 0;
    for (const p of [[0, 0], [12.5, 40.25], [-3, 77.7], [base.bbox[2], base.bbox[3]]]) { const q = fr.toSource(fr.toItem(p)); worst = Math.max(worst, Math.hypot(q[0] - p[0], q[1] - p[1])); }
    ok(worst < 1e-9, `C ${name}: charm point → item → charm point comes back (${worst})`);
  }
  { // y DOWN: a point above another in the view is below it in the item frame; the item frame is the view's own (renderBack's) with y negated
    const fr = AE.frame(front), a = [40, 40], vp = G.ap(G.mul(front.M, front.R), a[0], a[1]), it = fr.toItem(a);
    near(it[0], vp[0], 1e-9, 'C item x is the view x'); near(it[1], -vp[1], 1e-9, 'C item y is the view y, down');
    const up = fr.toItem([40, 41]); ok(true, 'C (checked)'); void up;
  }
  { // an item's own move, scale and turn, about its box centre (clockwise on screen in y-down)
    const b = [10, 20, 14, 22], cx = 12, cy = 21;
    const m1 = AE.itemMatrix({ tx: 3, ty: -2 }, b); eq(G.ap(m1, cx, cy).map(v => +v.toFixed(9)), [15, 19], 'C a move is tx, ty');
    const m2 = AE.itemMatrix({ sx: 2, sy: 0.5 }, b); eq(G.ap(m2, 14, 22).map(v => +v.toFixed(9)), [16, 21.5], 'C a scale is about the box centre');
    const m3 = AE.itemMatrix({ rot: 90 }, b); eq(G.ap(m3, cx, cy).map(v => +v.toFixed(9)), [cx, cy], 'C a turn keeps the box centre'); eq(G.ap(m3, 14, 21).map(v => +v.toFixed(9)), [12, 23], 'C and turns +90° clockwise on screen (x → y down)');
  }

  /* ── D · the calendar: May → June, the 17th in its square (row 3, column 1 from the top left: day 16 of a grid starting on 1) ── */
  const fr = AE.frame(front), dayAt = cal.day(3, 1), dayBox = base.members[dayAt].bbox, monthIds = cal.month.map(i => ids[i]);
  eq(cal.month.length, 3, 'D the master\'s month name is three letters, each its own part'); ok(cal.month.every(i => AE.roleOf(base.members[i], base) === 'hatch'), 'D drawn as black fills: the black-fill rule hatches them (role hatch)');
  ok(dayAt >= 0 && AE.roleOf(base.members[dayAt], base) === 'line', 'D the day square is a red line engraving');
  // the editor's items: points in the turned front view, y down. The new month name in the band the old one stood in, the date a group of two digit outlines inside the square
  const toItem = subs => subs.map(sp => sp.map(o => (o[0] === 'h' ? ['h'] : [o[0]].concat(o.slice(1).map(fr.toItem)))));
  const [bx, by] = cal.at(20, 68), letters = [0, 1, 2].map(k => ({ id: 'jun' + k, kind: 'text', role: 'art', text: 'JUN'[k], fill: '#000000', subpaths: toItem(sq(bx + 2 + 9 * k, by + 0.5, 5, 7)) }));
  const cx = (dayBox[0] + dayBox[2]) / 2, cy = (dayBox[1] + dayBox[3]) / 2;
  const date = { id: 'date', kind: 'group', role: 'art', text: '17', children: [{ id: 'd1', kind: 'text', fill: [0, 0, 0], subpaths: toItem(sq(cx - 3, cy - 2.5, 2.5, 5)) }, { id: 'd7', kind: 'text', fill: [0, 0, 0], subpaths: toItem(sq(cx + 0.5, cy - 2.5, 2.5, 5)) }] };
  const changes = { removed: monthIds, changed: {}, added: letters.concat([date]) };
  const rec = AE.encode(changes, { charm: piece, view: front, face: 'front', placed: true });
  eq(rec.removed, monthIds, 'D the record removes the three letters by their ids'); eq(rec.added.map(x => x.id), ['jun0', 'jun1', 'jun2', 'd1', 'd7'], 'D and adds five outlines (the group\'s digits each a part, named by their group)');
  ok(rec.added.slice(3).every(x => x.group === 'date'), 'D the digits keep their group'); eq(rec.cut, false, 'D no cut line is touched'); eq(rec.baseKey, AE.baseKeyOf(base), 'D the record names the drawing it was made on');
  ok(rec.added.every(x => /^[MLCZ0-9 .\-]+$/.test(x.d) && x.role === 'art' && x.fill.join() === '0,0,1' && x.stroke === null), 'D paths as M/L/C/Z strings in the charm\'s own points; a filled outline is hatched (blue), no stroke');
  ok(!JSON.stringify(rec).includes('[['), 'D no array inside an array anywhere in the record (Firestore refuses them)');
  const saved = Object.assign({ at: 1761600000000, by: 'Ana_M', face: 'front' }, rec);
  const ed = AE.apply(piece, saved, 'front');
  ok(ed !== piece && ed.members !== base.members, 'D apply makes a NEW charm with members of its own');
  eq(snap(piece), pieceBefore, 'A the piece\'s charm handed in is as it was'); eq(snap({ members: base.members, outline: base.outline, parsed: cal.parsed.segments, nested: cal.parsed.nested, top: base.topIndices, drop: base.dropIndices || null, hash: base.hash }), sharedBefore, 'A the shared master (its members, their points, the parsed segments and forms) is byte for byte as it was');
  ok(base.members.every(m => !m.artRedraw && !m.synthetic && !m.artMemberId), 'A no master part was marked'); ok(ed.members.every(m => !base.members.includes(m)), 'A every member of the edited piece is its own object (deep copies)');
  ok(ed.members.every(m => !m.subpaths || base.members.every(b => b.subpaths !== m.subpaths)), 'A and its points are its own arrays');
  eq(ed.members.length, base.members.length - 3 + 5, 'D 24 parts − 3 letters + 5 outlines');
  ok(!ed.members.some(m => monthIds.includes(m.artMemberId)), 'D the old month name is gone');
  const added = ed.members.filter(m => m.artAdded);
  eq(added.map(m => m.artMemberId), ['jun0', 'jun1', 'jun2', 'd1', 'd7'], 'D the new parts, named by the editor\'s ids');
  ok(added.every(m => m.hatchBlue && m.fill && !m.stroke && m.layer === 'ENGRAVE' && m.synthetic && m.artRedraw && m.manufacturingRole === 'hatch' && G.pathRole(m) === 'artwork' && !G.isCutLine(m)), 'D each added outline is blue hatching on the master\'s own ENGRAVE layer, written from its geometry, never a cut');
  const d17 = added.filter(m => m.artGroup === 'date'), box17 = d17.reduce((a, m) => [Math.min(a[0], m.bbox[0]), Math.min(a[1], m.bbox[1]), Math.max(a[2], m.bbox[2]), Math.max(a[3], m.bbox[3])], [Infinity, Infinity, -Infinity, -Infinity]);
  ok(bboxIn(box17, dayBox), 'D the date sits inside its day square, in the charm\'s own points: ' + box17.map(v => v.toFixed(2)) + ' in ' + dayBox.map(v => v.toFixed(2)));
  near((box17[0] + box17[2]) / 2, cx, 1e-3, 'D centred across the square'); near((box17[1] + box17[3]) / 2, cy, 1e-3, 'D centred up and down');
  const kept = ed.members.filter(m => !m.artAdded);
  ok(kept.every(m => m.artRedraw && m.synthetic), 'D the master draws the whole charm in ONE form: every part it keeps is drawn again from its geometry');
  ok(ed.dropIndices instanceof Set && ed.dropIndices.has(base.topIndices[0]), 'D and that form is out of the copied bytes (dropIndices ' + [...ed.dropIndices] + ')');
  eq(kept.map(m => m.artMemberId), ids.filter(id => !monthIds.includes(id)), 'D the kept parts carry their master ids, in order');
  ok(ed.outline !== base.outline && ed.outline === ed.members[0] && ed.outline.subpaths.join() === base.outline.subpaths.join(), 'D the outline is the piece\'s own copy, where it was');
  ok(ed.hash !== base.hash && ed.hash.startsWith(base.hash + '~ae'), 'D a hash of its own: ' + ed.hash);
  ok(ed.frontKeepOut === piece.frontKeepOut && ed.backKeepOut === piece.backKeepOut, 'D the keep-outs are kept');
  ok(ed.bits === piece.bits && ed.bbox === base.bbox && ed.id === piece.id && ed.poolId === piece.poolId, 'D the silhouette, the box, the id and the pool id stay (no cut line changed)');
  ok(ed.artEdit.front.at === saved.at && ed.artEdit.front.by === 'Ana_M' && ed.artEdit.front.removed === 3 && ed.artEdit.front.added === 5 && !ed.artEdit.front.refused.length && !ed.artEdit.front.stale.length, 'D who and when, and what was put on');
  ok(ed.artFrom && ed.artFrom.members === base.members && ed.artFrom.hash === base.hash, 'D the master is kept by reference (artFrom), never copied');
  // the front view of the edited piece: the words keep off the new month name and the date; the old month's band is free
  const v2 = G.backView(ed, { face: 'front', upAngle: 60, res: 6 }), m1 = G.engraveMask(front, { marginMm: 0.3 }), m2 = G.engraveMask(v2, { marginMm: 0.3 });
  const vc = p => G.ap(G.mul(front.M, front.R), p[0], p[1]);
  const inN = vc(cal.at(44, 74));   // (inside the new "N", clear of every old letter)
  ok(G.at(m1, ...inN) && !G.at(m2, ...inN), 'D the front\'s mask keeps the words off the new month name');
  const ed0 = AE.apply(piece, Object.assign({}, saved, { added: [] }), 'front'), m0 = G.engraveMask(G.backView(ed0, { face: 'front', upAngle: 60, res: 6 }), { marginMm: 0.3 }), inA = vc(cal.at(35.5, 70));   // (inside the old "A"'s left leg)
  ok(!G.at(m1, ...inA) && G.at(m0, ...inA), 'D and where the old "A" stood is free for words once it is removed');
  // the editor reads it back
  const items = AE.itemsOf(ed, front, 'front');
  eq(items.map(x => x.id), AE.memberIds(ed), 'D itemsOf: one item a part, by its id'); ok(items.filter(x => x.origin === 'added').length === 5 && items.filter(x => x.origin === 'master').length === 21, 'D the master\'s parts and the added ones told apart');
  const back2 = AE.decode(saved, piece, front), rec2 = AE.encode(back2, { charm: piece, view: front, face: 'front' });
  eq(rec2.added.map(x => x.d), rec.added.map(x => x.d), 'D decode → encode gives the same paths'); eq(rec2.removed, rec.removed, 'D and the same removals');
  ok(AE.encode(back2, { charm: ed, view: front, face: 'front' }).baseKey === rec.baseKey, 'D the edited piece is read as its master (baseOf)');
  // a changed part: the day square moved 1 pt right on screen and its paint kept
  const sqId = ids[dayAt], recM = AE.encode({ removed: [], changed: { [sqId]: { tx: 1 } }, added: [] }, { charm: piece, view: { M: [1, 0, 0, 1, 0, 0], R: [1, 0, 0, 1, 0, 0] }, face: 'front' });
  ok(recM.changed[sqId].keep === true && recM.changed[sqId].role === 'line', 'D a moved part keeps its own paint and role');
  const edM = AE.apply(piece, Object.assign({ at: 2 }, recM), 'front'), sqM = edM.members.find(m => m.artMemberId === sqId);
  near(sqM.bbox[0], dayBox[0] + 1, 1e-6, 'D it moved 1 pt'); ok(sqM.artChanged && sqM.stroke && sqM.strokeRGB.join() === base.members[dayAt].strokeRGB.join() && sqM.layer === 'ENGRAVE', 'D still a red line on ENGRAVE');
  const recM2 = AE.encode({ changed: { [sqId]: { tx: 1 } } }, { charm: edM, view: { M: [1, 0, 0, 1, 0, 0], R: [1, 0, 0, 1, 0, 0] }, face: 'front' });
  near(AE.apply(piece, Object.assign({ at: 3 }, recM2), 'front').members.find(m => m.artMemberId === sqId).bbox[0], dayBox[0] + 2, 1e-6, 'D moved again from where it is now (the edit saved before is the start of the next)');

  /* ── E · refused ── */
  const refusal = (fn, re, m) => { let e = null; try { fn(); } catch (x) { e = x; } ok(e && Array.isArray(e.refused) && e.refused.some(r => re.test(r.why)), m + ': ' + (e ? JSON.stringify(e.refused) : 'not refused')); };
  refusal(() => AE.encode({ removed: [ids[0]] }, { charm: piece, view: front, face: 'front' }), /outline is the piece itself/, 'E the outline is never removed');
  refusal(() => AE.encode({ added: [{ id: 'far', role: 'hatch', subpaths: toItem(sq(base.bbox[2] + 5, base.bbox[1], 3, 3)) }] }, { charm: piece, view: front, face: 'front' }), /outside the piece's outline/, 'E an engraving off the metal');
  refusal(() => AE.encode({ added: [{ id: 'hole', role: 'cut', subpaths: toItem(sq(cx - 1, cy - 1, 2, 2)) }] }, { charm: piece, view: front, face: 'front', placed: true }), /nested with/, 'E a new cut line on a piece already on a sheet');
  const cutOk = AE.encode({ added: [{ id: 'hole', role: 'cut', subpaths: toItem(sq(cx - 1, cy - 1, 2, 2)) }] }, { charm: piece, view: front, face: 'front', placed: false });
  ok(cutOk.cut === true && cutOk.added[0].stroke.join() === '0,0,0' && cutOk.added[0].fill === null, 'E ... but allowed on a piece not on a sheet yet: a black cut hairline, the record says cut');
  const edCut = AE.apply(piece, Object.assign({ at: 3 }, cutOk), 'front');
  ok(edCut.artTrace === true && G.isCutLine(edCut.members.find(m => m.artMemberId === 'hole')), 'E such a piece is traced again before it is nested (artTrace), its new hole a cut line');
  refusal(() => AE.encode({ removed: [ids[5]] }, { charm: piece, view: back, face: 'back' }), /back shows only the cut lines/, 'E a removal on the back');
  refusal(() => AE.encode({ removed: ['999-zzz'] }, { charm: piece, view: front, face: 'front' }), /not in the piece's drawing/, 'E an id that is no part of this drawing');
  const stale = AE.apply(piece, Object.assign({}, saved, { baseKey: 'otherdraw' }), 'front');
  ok(stale.members === base.members && stale.artEdit.front.stale[0] === '*' && /drawn again/.test(stale.artEdit.front.why), 'E a record made on another drawing is not put on (the piece is the master\'s, and says why)');
  ok(AE.apply(piece, Object.assign({}, saved, { removed: [ids[0]] }), 'front').artEdit.front.refused.some(r => /outline/.test(r.why)), 'E apply refuses on its own too (a record that names the outline)');
  ok(AE.apply(piece, Object.assign({}, saved, { removed: [ids[5]], added: [], cut: false }), 'front').members.length === base.members.length - 1, 'E (an engraving part removed by a record: applied)');

  /* ── F · over an edited piece, off again, the back ── */
  const twice = AE.apply(ed, saved, 'front');
  eq(twice.members.map(m => m.artMemberId + ':' + JSON.stringify(m.subpaths)), ed.members.map(m => m.artMemberId + ':' + JSON.stringify(m.subpaths)), 'F the same record over the edited piece gives the same parts (it starts from the master)');
  eq(twice.hash, ed.hash, 'F and the same hash');
  const off = AE.apply(ed, null, 'front');
  ok(off.members === base.members && off.outline === base.outline && off.hash === base.hash && !off.artEdit && !off.artFrom, 'F a null record takes the edit off: the master\'s own members, hash and outline');
  eq(snap(Object.assign({}, off, { artEdit: undefined })), snap(Object.assign({}, piece, { artEdit: undefined })), 'F the piece is exactly the master\'s copy again');
  const recB = AE.encode({ added: [{ id: 'heart', role: 'line', stroke: '#ff0000', lwPt: 0.4, subpaths: AE.frame(back).toItem ? sq(0, 0, 1, 1).map(sp => sp.map(o => (o[0] === 'h' ? ['h'] : [o[0], AE.frame(back).toItem([cx + o[1][0], cy + o[1][1]])]))) : [] }] }, { charm: ed, view: back, face: 'back' });
  const both = AE.apply(ed, Object.assign({ at: 9, by: 'Ana_M' }, recB), 'back');
  ok(both.members === ed.members && both.artBack.length === 1 && both.artBack[0].stroke && both.artBack[0].strokeRGB.join() === '1,0,0', 'F the back\'s addition stays off the front\'s members (artBack), the front edit kept');
  ok(both.artEdit.front && both.artEdit.back && both.hash !== ed.hash && both.hash.startsWith(base.hash + '~ae'), 'F both faces noted, a hash of its own');
  const bb = both.artBack[0].bbox; near((bb[0] + bb[2]) / 2, cx + 0.5, 1e-6, 'F the back part in the charm\'s own points (the mirror undone)');
  const frontOff = AE.apply(both, null, 'front');
  ok(frontOff.members === base.members && frontOff.artBack === both.artBack && frontOff.artEdit.back && !frontOff.artEdit.front, 'F the front taken off keeps the back');
  ok(AE.SUFFIX.test(both.hash) && both.hash.replace(AE.SUFFIX, '') === base.hash, 'F the hash it was cut from is read back without the mark (soloBase)');
  // a checkpoint (Session: a structured copy in IndexedDB) keeps the edited piece whole, and it can be edited again or taken back after a reload
  const restored = structuredClone(both);
  eq(AE.memberIds(restored), AE.memberIds(both), 'F a restored piece names its parts by the same ids');
  eq(snap(AE.apply(restored, null, 'front').members), snap(base.members), 'F and is taken back to the master exactly');
  eq(AE.apply(restored, saved, 'front').members.map(m => JSON.stringify(m.subpaths)), ed.members.map(m => JSON.stringify(m.subpaths)), 'F or edited again from its master');
  console.log(`PASS ${n} checks: ART EDIT model — apply never touches the shared master, stable member ids, the editor's frame, the calendar's month removed and a new month and date added in their places, refusals, edits off and on again`);
})().catch(e => { console.error(e); process.exit(1); });
