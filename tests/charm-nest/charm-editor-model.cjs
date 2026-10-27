/* THE CHARM EDITOR'S MODEL (charm-nest-charm-editor.js, Paul 27 Oct 2026: "full control similar to Adobe Illustrator ... over each individual layer contained on any
 * charm"). Pure node: no page, no browser, no network. The workspace is driven in tests/charm-nest/charm-editor-browser.cjs; this file checks the maths under it.
 *
 *   node tests/charm-nest/charm-editor-model.cjs
 *
 *  A  matrices and boxes: the transform is T(c+t)·R(rot)·S(sx,sy)·T(-c) about the TIGHT box of the geometry (a circle's box is its radius, not its control points)
 *  B  transforms land on the leaves: a move, a turn and an even scale are exact; a turned path stretched along a slant is redrawn and still exact; a text keeps its turn
 *  C  groups pass their own transform down (outlines unchanged), a mirrored item stays mirrored, words changed keep their anchor, an even stretch of words becomes their size
 *  D  hit tests: strokes by their line, fills by their inside, words by their box, the topmost first, hidden and locked never, a group whole unless reached into
 *  E  the marquee takes what it touches (an edge, an inside), not what it merely surrounds the empty middle of
 *  F  the history: 60 and more steps back and forward, a new step clears the way forward, the limit holds
 *  G  the clipboard: new ids, source 'added', 0.5 mm away, in face space even from inside a turned group
 *  H  group and ungroup keep the stacking and the drawing; arrange moves within each parent
 *  I  in and out: the caller's objects are never touched, the editor's own locks are not changes, text leaves as outlines in face space, the diff names what changed
 *  J  the stand-in font: centred / right alignment, linear in size, several lines
 */
'use strict';
const assert = require('node:assert/strict'), path = require('node:path');
const CE = require(path.join(__dirname, '..', '..', 'charm-nest-charm-editor.js'));
const M = CE.model, PT = 72 / 25.4;
let n = 0;
const ok = (c, label) => { assert.ok(c, label); n++; };
const near = (a, b, eps) => Math.abs(a - b) <= (eps == null ? 1e-6 : eps);
const nearBox = (a, b, eps) => !!a && !!b && a.every((v, i) => near(v, b[i], eps));
const geo = M.createGeo();
const rectItem = (id, x0, y0, x1, y1, extra) => Object.assign({ id, kind: 'path', role: 'art', source: 'master', name: id, stroke: true, fill: false, lwPt: 0.3, subpaths: M.rectSP(x0, y0, x1, y1) }, extra || {});
const pts = (it, P) => { const out = []; for (const sp of M.outlinesOf(it, geo, P)) for (const op of sp) if (op[0] !== 'h') for (const p of op.slice(1)) out.push(p); return out; };
const samePts = (a, b, eps) => a.length === b.length && a.every((p, i) => near(p[0], b[i][0], eps) && near(p[1], b[i][1], eps));
const worldPts = (sps, W) => { const out = []; for (const sp of sps) for (const op of sp) if (op[0] !== 'h') for (const p of op.slice(1)) out.push(M.ap(W, p[0], p[1])); return out; };

/* ── A ── */
{
  const m = [2, 0.5, -0.3, 1.5, 10, -4], mi = M.inv(m);
  ok(M.isI(M.mul(m, mi)) && M.isI(M.mul(mi, m)), 'A1 a matrix times its inverse is the identity');
  const p = M.ap(m, 3, 7); ok(near(p[0], 2 * 3 - 0.3 * 7 + 10) && near(p[1], 0.5 * 3 + 1.5 * 7 - 4), 'A2 ap maps x, y the SVG way (a·x + c·y + e, b·x + d·y + f)');
  const circ = M.ellipseSP(50, 40, 10, 10);
  ok(nearBox(M.bboxOf(circ), [40, 30, 60, 50], 1e-9), 'A3 the tight box of a circle drawn with cubics is its radius, not its control points');
  const bar = rectItem('bar', 0, 0, 10, 4);
  ok(nearBox(geo.drawn(bar), [0, 0, 10, 4]), 'A4 an untransformed item is drawn where its geometry is');
  const turned = Object.assign({}, bar, { rot: 90 });
  ok(nearBox(geo.drawn(turned), [3, -3, 7, 7], 1e-9), 'A5 rot turns about the centre of the geometry\'s own box (10 × 4 at 5,2 becomes 4 × 10 at 5,2)');
  const moved = Object.assign({}, bar, { tx: 2, ty: -1, sx: 2, sy: 0.5 });
  ok(nearBox(geo.drawn(moved), [-5 + 2, 1 - 1, 15 + 2, 3 - 1], 1e-9), 'A6 sx, sy scale about the centre and tx, ty move it');
  const rot30 = Object.assign({}, bar, { rot: 30 }), W = geo.matrix(rot30);
  const c = M.ap(W, 5, 2); ok(near(c[0], 5) && near(c[1], 2), 'A7 the centre of the box stays put under a turn');
  ok(near(Math.atan2(W[1], W[0]) * 180 / Math.PI, 30), 'A8 positive rot is clockwise on a y-down page (the SVG / CSS convention)');
}

/* ── B ── */
{
  const base = rectItem('r', 10, 10, 30, 20, { rot: 25, sx: 1.5, sy: 0.8, tx: 3, ty: 4 });
  const before = pts(base);
  const T = M.tr(7, -2), R = M.about(M.rotM(40), 12, 9), S = M.about(M.scM(1.7, 1.7), 0, 0);
  for (const [name, X] of [['a move', T], ['a turn about another point', R], ['an even scale', S]]) {
    const out = M.composeInto(base, X, geo);
    ok(out.subpaths === base.subpaths && samePts(pts(out), before.map(p => M.ap(X, p[0], p[1])), 1e-6), `B1 ${name}: exact, and the geometry itself is not touched (only tx, ty, sx, sy, rot)`);
  }
  const slant = M.about(M.scM(2, 1), 0, 0);   // a turned item stretched across its turn
  const baked = M.composeInto(base, slant, geo);
  ok(baked.subpaths !== base.subpaths && baked.rot === 0 && baked.sx === 1 && baked.sy === 1 && baked.tx === 0, 'B2 a turned path stretched along a slant is redrawn: new geometry, no transform');
  ok(samePts(pts(baked), before.map(p => M.ap(slant, p[0], p[1])), 1e-6), 'B3 ... and the drawing is still exactly the slanted one');
  const unturned = rectItem('u', 0, 0, 10, 10);
  const stretched = M.composeInto(unturned, M.about(M.scM(3, 0.5), 0, 0), geo);
  ok(stretched.subpaths === unturned.subpaths && near(stretched.sx, 3) && near(stretched.sy, 0.5), 'B4 an unturned path stretched unevenly keeps its geometry (sx 3, sy 0.5)');
  const words = { id: 't', kind: 'text', role: 'hatch', source: 'added', text: 'AB', fontKey: 'sans', sizePt: 10, align: 'left', tx: 20, ty: 30, rot: 30 };
  const wSlant = M.composeInto(words, slant, geo);
  ok(wSlant.kind === 'text' && wSlant.text === 'AB' && Number.isFinite(wSlant.rot) && wSlant.rot !== 0, 'B5 a text is never redrawn as a path: it keeps its words and a turn');
  const there = [M.tr(0.1, 0), M.about(M.rotM(3), 5, 5), M.about(M.scM(1.25, 1.25), 2, 3), M.tr(-0.4, 0.2)];
  const back = there.map(X => M.inv(X)).reverse();
  let it = base; for (const X of there.concat(back)) it = M.composeInto(it, X, geo);
  ok(near(it.tx, base.tx, 1e-6) && near(it.ty, base.ty, 1e-6) && near(it.rot, base.rot, 1e-9) && near(it.sx, base.sx, 1e-9), 'B6 moves and turns that cancel out leave no drift');
}

/* ── C ── */
{
  const a = rectItem('a', 0, 0, 10, 10), b = rectItem('b', 20, 0, 30, 10, { rot: 15 });
  const g = { id: 'g', kind: 'group', role: 'art', source: 'master', name: 'G', children: [a, b], rot: 30, tx: 5, sx: 1.2, sy: 1.2 };
  const before = pts(g);
  const flat = M.pushDown(g, null, geo, M.I, true);
  ok(flat.rot === 0 && flat.sx === 1 && flat.tx === 0 && flat.children.every(ch => ch.subpaths === (ch.id === 'a' ? a.subpaths : b.subpaths)), 'C1 a group pushes its own transform down to its leaves and keeps none');
  ok(samePts(pts(flat), before, 1e-6), 'C2 ... and nothing moves on the charm');
  const locked = Object.assign({}, g, { rot: 0, tx: 0, sx: 1, sy: 1, children: [a, Object.assign({}, b, { locked: true })] });
  const movedL = M.pushDown(locked, M.tr(5, 0), geo);
  ok(movedL.children[0].tx === 5 && movedL.children[1].tx == null, 'C3 a move given to a group passes its locked leaves by');
  const mirrored = rectItem('m', 0, 0, 10, 4, { sx: -1 });
  const mm = M.composeInto(mirrored, M.tr(3, 3), geo);
  ok(mm.sx === -1 && mm.sy === 1 && mm.rot === 0, 'C4 a mirrored item moved stays mirrored the same way (sx -1, not "turned 180° and mirrored the other way")');
  const words = { id: 't', kind: 'text', role: 'hatch', source: 'added', text: 'MAY', fontKey: 'sans', sizePt: 9, align: 'left', tx: 40, ty: 60, rot: 35, sx: 1.3, sy: 1.3 };
  const anchor = w => M.ap(geo.matrix(w), 0, 0);
  const longer = M.retext(words, { text: 'SEPTEMBER' }, geo);
  ok(near(anchor(longer)[0], anchor(words)[0], 1e-6) && near(anchor(longer)[1], anchor(words)[1], 1e-6), 'C5 new words keep the text\'s anchor where it was on the charm, turned and scaled');
  const bigger = M.retext(words, { sizePt: 14 }, geo);
  ok(near(anchor(bigger)[0], anchor(words)[0], 1e-6), 'C6 ... and so does a new size');
  const folded = M.foldTextScale(words, geo);
  ok(near(folded.sizePt, 9 * 1.3, 1e-9) && folded.sx === 1 && folded.sy === 1, 'C7 an even stretch of words becomes their size (9 pt × 1.3)');
  ok(nearBox(geo.drawn(folded), geo.drawn(words), 1e-6), 'C8 ... drawn exactly where the stretched words were');
  const uneven = M.foldTextScale(Object.assign({}, words, { sy: 1.1 }), geo);
  ok(uneven.sizePt === 9 && uneven.sy === 1.1, 'C9 an uneven stretch stays a stretch');
}

/* ── D ── */
{
  const sq = rectItem('sq', 0, 0, 10, 10);
  const fillHeart = { id: 'fh', kind: 'path', role: 'hatch', source: 'master', subpaths: M.ellipseSP(50, 50, 5, 5), fill: true };
  const word = { id: 'w', kind: 'text', role: 'hatch', source: 'added', text: 'HI', fontKey: 'sans', sizePt: 10, align: 'left', tx: 100, ty: 100 };
  const g = { id: 'g', kind: 'group', role: 'art', source: 'master', children: [rectItem('g1', 200, 0, 210, 10), rectItem('g2', 220, 0, 230, 10)] };
  const items = [sq, fillHeart, word, g];
  const hit = (x, y, o) => { const h = M.hitTest(items, x, y, 0.5, geo, o); return h && h.id; };
  ok(hit(10.2, 5) === 'sq' && hit(5, 0.1) === 'sq', 'D1 a stroked square is hit on its line');
  ok(hit(5, 5) === null, 'D2 ... and not in its empty middle (a day square is picked by its edge, as in Illustrator)');
  ok(hit(50, 50) === 'fh' && hit(52, 51) === 'fh' && hit(57, 50) === null, 'D3 a filled shape is hit anywhere inside it');
  const wb = geo.drawn(word);
  ok(hit((wb[0] + wb[2]) / 2, (wb[1] + wb[3]) / 2) === 'w', 'D4 words are hit anywhere in their box');
  ok(hit(205, 0) === 'g' && hit(225, 10, { deep: true }) === 'g2', 'D5 a group is hit whole; reaching in (Alt) gives the leaf');
  const over = rectItem('over', 0, 0, 10, 10, { name: 'over' });
  ok(M.hitTest(items.concat(over), 10, 5, 0.5, geo).id === 'over', 'D6 the topmost item wins');
  ok(M.hitTest([Object.assign({}, sq, { locked: true })], 10, 5, 0.5, geo) === null && M.hitTest([Object.assign({}, sq, { hidden: true })], 10, 5, 0.5, geo) === null, 'D7 locked and hidden items are never hit');
  const turned = rectItem('t', 0, 0, 20, 2, { rot: 90 });   // a 20 × 2 bar stood up about its centre (10, 1)
  ok(M.hitTest([turned], 9, 7, 0.3, geo) && M.hitTest([turned], 10, -8.95, 0.3, geo) && M.hitTest([turned], 18, 1, 0.3, geo) === null, 'D8 hit tests follow the turn (its long edge now runs down x = 9)');
  ok(M.hitTest([sq], 11.2, 5, 1.5, geo) && !M.hitTest([sq], 11.2, 5, 0.5, geo), 'D9 the tolerance is the hand\'s (pt): 1.2 pt away is a hit at 1.5 and not at 0.5');
}

/* ── E ── */
{
  const sq = rectItem('sq', 0, 0, 10, 10), letter = { id: 'L', kind: 'path', role: 'art', fill: true, stroke: false, subpaths: M.rectSP(20, 0, 30, 10) };
  const lockedCut = rectItem('cut', -5, -5, 40, 40, { role: 'cut', locked: true });
  const items = [lockedCut, sq, letter];
  const ids = r => M.touching(items, r, geo).map(i => i.id).sort().join(',');
  ok(ids([8, 4, 12, 6]) === 'sq', 'E1 a box across an edge takes the square');
  ok(ids([3, 3, 7, 7]) === '', 'E2 a box inside the empty middle of an outline touches nothing');
  ok(ids([24, 4, 26, 6]) === 'L', 'E3 a box inside a filled letter touches it');
  ok(ids([-1, -1, 31, 11]) === 'L,sq', 'E4 a sweep over both takes both, and never the locked cut line');
  ok(ids([12, 6, 8, 4]) === 'sq' && ids([26, 6, 24, 4]) === 'L', 'E5 corners in any order');
}

/* ── F ── */
{
  const H = new M.History(120);
  let state = 0;
  for (let i = 0; i < 75; i++) { H.push(state); state++; }
  for (let i = 0; i < 75; i++) state = H.undo(state);
  ok(state === 0 && !H.canUndo, 'F1 75 steps back to the start');
  for (let i = 0; i < 75; i++) state = H.redo(state);
  ok(state === 75 && !H.canRedo, 'F2 ... and 75 forward again');
  state = H.undo(state); state = H.undo(state); H.push(state); state = 'new';
  ok(!H.canRedo && H.back.length === 74, 'F3 a new step after undoing clears the way forward');
  const L = new M.History(60); for (let i = 0; i < 100; i++) L.push(i);
  ok(L.back.length === 60 && L.back[0] === 40, 'F4 the limit holds: the oldest steps go first');
}

/* ── G ── */
{
  const a = rectItem('a', 0, 0, 10, 10), b = rectItem('b', 20, 0, 30, 10);
  const g = { id: 'g', kind: 'group', role: 'art', source: 'master', children: [a, b], rot: 90 };
  const items = [g, rectItem('c', 50, 50, 60, 60)];
  const clip = M.copyItems(items, ['b', 'c'], geo);
  ok(clip.length === 2 && nearBox(geo.drawn(clip[0]), geo.drawn(b, geo.matrix(g)), 1e-6), 'G1 a copy from inside a turned group is placed in face space, where it is drawn');
  let k = 0; const pasted = M.pasteItems(clip, 0.5 * PT, 0.5 * PT, geo, () => 'p' + (++k));
  ok(pasted.map(p => p.id).join() === 'p1,p2' && pasted.every(p => p.source === 'added'), 'G2 pasted items get new ids and are "added"');
  const d0 = geo.drawn(clip[1]), d1 = geo.drawn(pasted[1]);
  ok(near(d1[0] - d0[0], 0.5 * PT, 1e-9) && near(d1[1] - d0[1], 0.5 * PT, 1e-9), 'G3 ... 0.5 mm right and down');
  const inPlace = M.pasteItems(clip, 0, 0, geo);
  ok(nearBox(geo.drawn(inPlace[1]), d0, 1e-12) && inPlace[1].id !== 'c', 'G4 paste in place: same spot, new id');
  ok(items[0].children[1] === b && b.id === 'b', 'G5 copying touches nothing');
  const grp = { id: 'gg', kind: 'group', role: 'line', children: [rectItem('x', 0, 0, 1, 1, { role: 'line' })] };
  const pg = M.pasteItems([grp], 1, 1, geo, () => 'n' + (++k));
  ok(pg[0].id !== 'gg' && pg[0].children[0].id !== 'x', 'G6 a pasted group gets new ids all the way down');
}

/* ── H ── */
{
  const L = id => rectItem(id, 0, 0, 5, 5, { tx: id.charCodeAt(0) });
  let items = [L('a'), L('b'), L('c'), L('d'), L('e')];
  const before = items.map(i => pts(i));
  const r = M.groupItems(items, ['b', 'd'], geo, 'G');
  ok(r.items.map(i => i.id).join() === 'a,c,G,e' && r.items[2].children.map(c => c.id).join() === 'b,d', 'H1 grouping puts the group where the topmost of them was, its members in their stacking order');
  const u = M.ungroupItem(r.items, 'G', geo);
  ok(u.items.map(i => i.id).join() === 'a,c,b,d,e' && u.ids.join() === 'b,d', 'H2 ungrouping puts the members where the group was');
  ok(['a', 'b', 'c', 'd', 'e'].every((id, i) => samePts(pts(u.items.find(x => x.id === id)), before[i], 1e-9)), 'H3 nothing moved on the charm');
  const tg = { id: 'T', kind: 'group', role: 'art', children: [L('p'), L('q')], rot: 45, tx: 3 };
  const tp = pts(tg);
  const ut = M.ungroupItem([tg], 'T', geo);
  ok(samePts(pts(ut.items[0]).concat(pts(ut.items[1])), tp, 1e-6), 'H4 a turned group\'s members keep their places when it is taken apart');
  const all = M.groupItems([tg], ['p', 'q'], geo, 'N');
  ok(all.items.length === 1 && all.items[0].id === 'T' && all.items[0].children.length === 1 && all.items[0].children[0].id === 'N' && samePts(pts(all.items[0]), tp, 1e-6), 'H5 grouping everything inside a group nests the new group in it (Illustrator does the same); nothing moves');
  const out = M.groupItems([tg, L('z')], ['p', 'z'], geo, 'Q');
  ok(out.items.map(i => i.id).join() === 'T,Q' && out.items[0].children.map(c => c.id).join() === 'q' && out.items[1].children.map(c => c.id).join() === 'p,z', 'H5b grouping across groups takes the members out of theirs');
  const emptied = M.groupItems([{ id: 'E', kind: 'group', role: 'art', children: [L('k')] }, L('j')], ['k', 'j'], geo, 'R');
  ok(emptied.items.map(i => i.id).join() === 'R' && emptied.items[0].children.map(c => c.id).join() === 'k,j', 'H5c a group left empty by it goes');
  ok(M.arrange(items, ['b'], 'forward', geo).map(i => i.id).join() === 'a,c,b,d,e', 'H6 bring forward: one place up');
  ok(M.arrange(items, ['d'], 'backward', geo).map(i => i.id).join() === 'a,b,d,c,e', 'H7 send backward: one place down');
  ok(M.arrange(items, ['a', 'c'], 'front', geo).map(i => i.id).join() === 'b,d,e,a,c', 'H8 to the front keeps their own order');
  ok(M.arrange(items, ['e'], 'back', geo).map(i => i.id).join() === 'e,a,b,c,d', 'H9 to the back');
  ok(M.arrange(items, ['e'], 'forward', geo) === items || M.arrange(items, ['e'], 'forward', geo).map(i => i.id).join() === 'a,b,c,d,e', 'H10 the topmost going forward stays put');
}

/* ── I ── */
{
  const input = [
    rectItem('cut', 0, 0, 50, 60, { role: 'cut' }),
    rectItem('day', 10, 10, 20, 20),
    { id: 'L1', kind: 'path', role: 'art', source: 'master', fill: true, subpaths: M.rectSP(5, 2, 7, 6) },
    { id: 'grp', kind: 'group', role: 'line', source: 'master', children: [rectItem('s1', 30, 40, 32, 42, { role: 'line' }), rectItem('s2', 34, 40, 36, 42, { role: 'line' })] },
    { kind: 'text', role: 'hatch', source: 'engrave', text: 'HOH', fontKey: 'sans', sizePt: 8, align: 'center', tx: 25, ty: 30 }
  ];
  const frozen = JSON.stringify(input);
  const prep = M.prepIn(input, ['cut']);
  ok(JSON.stringify(input) === frozen, 'I1 the caller\'s items are not touched');
  ok(prep[0].locked === true && prep[0]._autoLock && prep[4].id && prep[1].subpaths === input[1].subpaths, 'I2 cut lines open locked, a missing id is filled in, geometry is shared not copied');
  const base = M.exportItems(prep, geo);
  ok(base[0].locked === false && !('_autoLock' in base[0]), 'I3 the editor\'s own lock goes back off on the way out');
  ok(Object.keys(M.diffFace(base, M.exportItems(prep, geo))).every(k => (Array.isArray(M.diffFace(base, base)[k]) ? !M.diffFace(base, base)[k].length : !Object.keys(M.diffFace(base, base)[k]).length)), 'I4 nothing done: nothing changed');
  // the owner's scenario in miniature: the letter goes, new words come, a day is moved, the cut line is unlocked and locked again, a group is taken apart
  let items = M.removeIds(prep, ['L1']);
  const sept = { id: 'new-1', kind: 'text', role: 'hatch', source: 'added', name: 'SEPTEMBER', text: 'SEPTEMBER', fontKey: 'sans', sizePt: 6, align: 'left', tx: 5, ty: 6, rot: 10 };
  items = items.concat(sept);
  items = M.replaceIn(items, new Map([['day', M.composeInto(items.find(i => i.id === 'day'), M.tr(1, 0), geo)]]));
  const relocked = Object.assign({}, items[0], { locked: true }); delete relocked._autoLock; delete relocked._lockedIn;
  items = M.replaceIn(items, new Map([['cut', relocked]]));
  items = M.ungroupItem(items, 'grp', geo).items;
  const out = M.exportItems(items, geo);
  const d = M.diffFace(base, out);
  ok(d.removed.sort().join() === 'L1,grp', 'I5 removed: the letter and the group taken apart');
  ok(d.added.length === 1 && d.added[0].id === 'new-1', 'I6 added: the new words');
  ok(Object.keys(d.changed).sort().join() === 'cut,day,s1,s2', 'I7 changed: the moved day, the cut line now locked by hand, and the group\'s members (their group changed)');
  const tx = d.added[0];
  ok(Array.isArray(tx.subpaths) && tx.subpaths.length > 0 && nearBox(tx.bbox, geo.drawn(sept), 1e-6), 'I8 the new words leave as outlines already in face space (their box is where they are drawn)');
  const raw = geo.raw(sept).subpaths[0][0][1], W = geo.matrix(sept), want = M.ap(W, raw[0], raw[1]);
  ok(near(tx.subpaths[0][0][1][0], want[0], 1e-6) && near(tx.subpaths[0][0][1][1], want[1], 1e-6), 'I9 ... point for point (turn included): pure vector, nothing to apply again');
  const day = d.changed.day;
  ok(day.tx === 1 && day.subpaths === input[1].subpaths && day.matrix[4] === 1 && near(day.pivot[0], 15), 'I10 a moved path keeps its geometry and says its matrix and pivot');
  ok(!out.some(o => Object.keys(o).some(k => k[0] === '_')), 'I11 no internal marks leave the editor');
  const words = out.find(o => o.kind === 'text' && o.text === 'HOH');
  ok(words && words.bbox && near((words.bbox[0] + words.bbox[2]) / 2, 25, 1e-6), 'I12 a text that came in leaves outlined too (centred words, centred box)');
  const same = M.diffFace(base, M.exportItems(M.prepIn(JSON.parse(JSON.stringify(input)).map((x, i) => Object.assign(x, { id: prep[i].id })), ['cut']), geo));
  ok(!same.removed.length && !same.added.length && !Object.keys(same.changed).length, 'I13 the same charm read in again (a fresh copy) is no change');
}

/* ── J ── */
{
  const T = CE.builtinTextToPaths;
  const l = T('ABC', 'sans', 10, { align: 'left' }), c = T('ABC', 'sans', 10, { align: 'center' }), r = T('ABC', 'sans', 10, { align: 'right' });
  const hw = 10 * 0.7 / 6 * 0.46;   // half a stroke: the ink reaches that far past the glyph's own box (a square cap on a slant, √2 of it)
  ok(near(c.bbox[0], -c.bbox[2], 1e-9) && r.bbox[2] >= hw && r.bbox[2] <= hw * 1.5 && near(l.bbox[0], -r.bbox[2], 1e-9) && near(r.bbox[2] - r.bbox[0], l.bbox[2] - l.bbox[0], 1e-9), 'J1 left words start at the anchor, centred ones straddle it, right-aligned ones end at it');
  const big = T('ABC', 'sans', 20, {});
  ok(big.bbox.every((v, i) => near(v, l.bbox[i] * 2, 1e-9)), 'J2 twice the size is twice the outline');
  const two = T('AB\nCD', 'sans', 10, {});
  ok(two.bbox[3] > 10 && two.bbox[1] < 0, 'J3 a second line goes below the first');
  ok(T('', 'sans', 10, {}).subpaths.length === 0 && T('Ab9.', 'sans', 10, {}).subpaths.length > 0, 'J4 nothing for no words; lower case, figures and marks draw');
  ok(l.bbox[3] > 0 && l.bbox[3] < 1 && l.bbox[1] <= -7 - hw && l.bbox[1] >= -7 - hw * 1.5, 'J5 the anchor is the baseline: capitals stand on it (cap height 0.7 em)');
}

console.log('PASS ' + n + ' checks: charm editor model: matrices and tight boxes; transforms exact on the leaves (slants redrawn); groups pass their transform down; mirrored stays mirrored; words keep their anchor and fold an even stretch into their size; hit tests and the marquee; 75-step history; clipboard in face space; group / ungroup / arrange; in and out (no mutation, own locks are not changes, text as outlines, the diff)');
