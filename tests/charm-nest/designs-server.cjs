/* DESIGNS (Paul, 28 Oct 2026: "this charm editor must have a repository of all opened, modified designed pieces, and this repository must be searchable ... this repository must grow
   with every new update or change to any charm using the charm editor"; "track which order each design was attached to"; "ensure the sanctity of the designed charm. It's exact
   dimensions and user designed features"): the design library — charmNestLibrary.js designPut / designList / designGet run by the REAL handler over the in-memory Firestore and
   Storage of tests/charm-nest/bridge-server.cjs, and the page module charm-nest-designs.js against the same handler. No live service, no credential, no network.
     A  refused whole, nothing written: who, the design, its id, faces, parts (kind, role, path vocabulary and numbers, paint, widths, a text without outlines, a group moved without
        its matrix), its size (missing, or not its cut line's), its source, its order, too many operators, too many bytes, a picture that is no PNG
     B  saved: the record (latest fields, no faces) and version 1 (faces as path strings: no array inside an array), the count, the picture in Storage named by the geometry hash;
        a second save is version 2 (version 1 untouched), with the order and the people added; the same drawing renamed keeps its picture
     C  the hash (D6): the server's is canonical()'s; ids and names do not change it, a point moved by 0.01 pt does; a part hidden is not in it, a cut line hidden is; a turned curve
        without its matrix is placed exactly as the Charm Editor places it (its model's matrix, and its exported result hashes the same)
     D  designList: newest first with upTo; since = upTo is `unchanged` for ONE read; a save since sends only it; parts by cursor; a revision the count does not know is `reset`;
        the cloud's own search by order, person and word
     E  designGet: the latest, a version, the history; the faces back exactly as cleaned (verify), unknown → null; a very big design (a traced photograph) keeps its faces in
        Storage beside its record (gzip) and reads back whole
     F  the sandbox: its own records, count and Storage; production untouched and unseen. No op raises the placement counter
     G  the page module: put (checked first: a refused design sends nothing) → the index row at once and the employee log; search instant on 3,000 rows; a reload reads the index
        from this computer and asks the cloud ONE question; another computer's save comes alone; get keeps a version in memory
   node tests/charm-nest/designs-server.cjs */
'use strict';
const assert = require('node:assert/strict'), path = require('node:path'), crypto = require('node:crypto'), zlib = require('node:zlib');
const { start } = require('./bridge-server.cjs');
const ROOT = path.join(__dirname, '..', '..');
const D = require(path.join(ROOT, 'charm-nest-designs.js')), CE = require(path.join(ROOT, 'charm-nest-charm-editor.js'));
let n = 0; const ok = (c, m) => { assert.ok(c, m); n++; }, eq = (a, b, m) => { assert.deepEqual(a, b, m); n++; };
const nested = v => Array.isArray(v) ? v.some(x => Array.isArray(x) || nested(x)) : v && typeof v === 'object' ? Object.values(v).some(nested) : false;
const PT = 72 / 25.4, W = 20 * PT, H = 15 * PT;
const rect = (x0, y0, x1, y1) => [['m', [x0, y0]], ['l', [x1, y0]], ['l', [x1, y1]], ['l', [x0, y1]], ['h']];
const K = 0.5522847498, circle = (cx, cy, r) => [['m', [cx + r, cy]], ['c', [cx + r, cy + r * K], [cx + r * K, cy + r], [cx, cy + r]], ['c', [cx - r * K, cy + r], [cx - r, cy + r * K], [cx - r, cy]], ['c', [cx - r, cy - r * K], [cx - r * K, cy - r], [cx, cy - r]], ['c', [cx + r * K, cy - r], [cx + r, cy - r * K], [cx + r, cy]], ['h']];
const PNG = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAC0lEQVR4nGP4DwQACfsD/fteaysAAAAASUVORK5CYII=';
/** A tag 20 × 15 mm: an outline and a hole (cut), a turned curve hatched (no matrix: tx…rot), a red line in a moved group, words as outlines; on the back the outline and a hidden part. */
function design(extra) {
  return Object.assign({ name: 'Heart tag', widthMm: 20, heightMm: 15, source: { kind: 'new' }, faces: {
    front: { face: 'front', unitPt: true, frame: { x0: 0, y0: 0, wPt: W, hPt: H }, items: [
      { id: 'outline', kind: 'path', role: 'cut', stroke: [0, 0, 0], subpaths: [rect(0, 0, W, H)] },
      { id: 'hole', kind: 'path', role: 'cut', stroke: [0, 0, 0], subpaths: [circle(6, 6, 2.5)] },
      { id: 'heart', kind: 'path', role: 'hatch', fill: [0, 0, 1], subpaths: [[['m', [20, 14]], ['c', [22, 6], [30, 6], [30, 14]], ['l', [25, 22]], ['h']]], tx: 3, ty: -1, rot: 30, sx: 1.2, sy: 0.9 },
      { id: 'g1', kind: 'group', role: 'art', name: 'Lines', tx: 5, ty: 2, matrix: [1, 0, 0, 1, 5, 2], children: [{ id: 'ln', kind: 'path', role: 'line', stroke: [1, 0, 0], lwPt: 0.4, subpaths: [[['m', [2, 30]], ['l', [30, 30]]]] }] },
      { id: 't1', kind: 'text', role: 'hatch', text: 'AB', fontKey: 'jost-ace', sizePt: 8, subpaths: [rect(35, 20, 40, 26), rect(41, 20, 46, 26)], layer: 'words', origin: 'added', _cache: { no: 1 } }
    ] },
    back: { face: 'back', unitPt: true, frame: { x0: 0, y0: 0, wPt: W, hPt: H }, items: [
      { id: 'outline', kind: 'path', role: 'cut', subpaths: [rect(0, 0, W, H)] },
      { id: 'hid', kind: 'path', role: 'hatch', hidden: true, fill: [0, 0, 1], subpaths: [rect(5, 5, 8, 8)] }
    ] }
  } }, extra || {});
}
const clone = v => JSON.parse(JSON.stringify(v));
const sha = t => crypto.createHash('sha256').update(t).digest('hex');
const hashOf = d => sha(D.canonical(D.clean(d).design));

(async () => {
  const srv = await start({ receipts: [], strictArrays: true }), st = srv.st;
  try {
    const call = async body => { const out = await st.handlers.charmNestLibrary.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body), queryStringParameters: {} }); return { status: out.statusCode, body: JSON.parse(out.body || '{}') }; };
    const docs = () => [...st.docs.keys()].filter(k => /^(Sandbox_)?Charm_Designs\//.test(k)).length;
    const put = (d, extra) => call(Object.assign({ op: 'designPut', by: 'Ana_M', design: d }, extra || {}));
    const reads = async fn => { const r0 = st.reads || 0; const out = await fn(); return [out, (st.reads || 0) - r0]; };

    /* ── A · refused whole ── */
    const bad = async (body, re, m) => { const before = docs(), blobs = st.blobs.size, r = await call(Object.assign({ op: 'designPut', by: 'Ana_M', design: design() }, body)); ok(r.status === 400 && re.test(r.body.error || '') && docs() === before && st.blobs.size === blobs, `A ${m}: ${r.status} ${r.body.error}`); };
    const part = (face, i, patch) => { const d = design(); Object.assign(d.faces[face].items[i], patch); return d; };
    await bad({ by: ' ' }, /signed-in person/, 'no one named');
    await bad({ design: null }, /missing/, 'no design');
    await bad({ design: design({ id: 'a b' }) }, /id is not a short name/, 'an id with a space');
    await bad({ design: design({ faces: { side: design().faces.front } }) }, /front or the back/, 'a face that is neither');
    await bad({ design: design({ faces: {} }) }, /no faces/, 'no faces');
    await bad({ design: part('front', 2, { role: 'engrave' }) }, /role must be cut, line, hatch or art/, 'a role that is none');
    await bad({ design: part('front', 2, { kind: 'image' }) }, /kind must be path, text or group/, 'a picture (traced to paths first)');
    await bad({ design: part('front', 2, { subpaths: [[['m', [1, 1]], ['q', [2, 2], [3, 3]]]] }) }, /m \/ l \/ c \/ h/, 'a quadratic');
    await bad({ design: part('front', 2, { subpaths: [[['l', [1, 1]]]] }) }, /m \/ l \/ c \/ h/, 'no m first');
    await bad({ design: part('front', 2, { subpaths: [[['m', [1, 99999]], ['l', [2, 2]]]] }) }, /±20000/, 'a point out of range');
    await bad({ design: part('front', 2, { subpaths: [[['m', [1, NaN]]]] }) }, /m \/ l \/ c \/ h/, 'not a number (sent as null)');
    await bad({ design: part('front', 2, { subpaths: [] }) }, /path is empty/, 'an empty path');
    await bad({ design: part('front', 4, { subpaths: undefined }) }, /outlines/, 'a text without its outlines');
    await bad({ design: part('front', 3, { matrix: undefined, tx: 4 }) }, /group that is moved/, 'a group moved without its matrix');
    await bad({ design: part('front', 3, { matrix: [1, 0, 0] }) }, /matrix is six numbers/, 'a matrix of three numbers');
    await bad({ design: part('front', 2, { fill: [0, 0, 2] }) }, /colour/, 'a colour out of 0..1');
    await bad({ design: part('front', 2, { fill: 'url(#x)' }) }, /colour/, 'a colour that is a link');
    await bad({ design: part('front', 3, { children: [{ id: 'ln', kind: 'path', role: 'line', lwPt: 50, subpaths: [[['m', [0, 0]], ['l', [1, 1]]]] }] }) }, /line width/, 'a line 50 pt wide');
    await bad({ design: part('front', 2, { weightMm: 9 }) }, /weight/, 'a weight of 9 mm');
    await bad({ design: part('front', 2, { id: 'outline' }) }, /used twice/, 'an id twice on a face');
    await bad({ design: design({ widthMm: null }) }, /size/, 'no size');
    await bad({ design: design({ widthMm: 21 }) }, /cut line measures 20 × 15 mm/, 'a size that is not its cut line\'s (D6)');
    await bad({ design: design({ source: { kind: 'stolen' } }) }, /source/, 'a source that is none');
    await bad({ design: design({ orderId: 'abc' }) }, /order number or CUSTOM/, 'an order that is none');
    await bad({ design: design({ faces: { front: { face: 'front', items: [{ id: 'outline', kind: 'path', role: 'cut', subpaths: [rect(0, 0, W, H)] }, { id: 'many', kind: 'path', role: 'line', subpaths: [[['m', [0, 0]]].concat(Array.from({ length: 300001 }, (_, i) => ['l', [i % 50, 1]]))] }] } } }) }, /path operators/, '300 002 operators');
    const huge = Array.from({ length: 72000 }, (_, i) => ['c', [12345.678 - i % 7, -12345.671], [12345.672, -12345.673], [12345.674, -12345.675]]);
    await bad({ design: design({ faces: { front: { face: 'front', items: [{ id: 'outline', kind: 'path', role: 'cut', subpaths: [rect(0, 0, W, H)] }, { id: 'big', kind: 'path', role: 'line', subpaths: [[['m', [0, 0]]].concat(huge)] }] } } }) }, /KB; one version is at most 4000 KB/, 'a version of over 4 MB');
    await bad({ thumb: Buffer.from('GIF89a not a png').toString('base64') }, /PNG/, 'a picture that is no PNG');
    eq(docs(), 0, 'A nothing was written by any refusal');

    /* ── B · saved ── */
    let r = await put(design(), { thumb: PNG });
    ok(r.status === 200 && r.body.ok && /^d[0-9a-z]+$/.test(r.body.id) && r.body.version === 1 && /^[0-9a-f]{64}$/.test(r.body.hash) && r.body.at > 0, 'B saved: an id, version 1, a hash: ' + JSON.stringify(r.body).slice(0, 140));
    const id = r.body.id, first = r.body, head = st.doc('Charm_Designs', id), v1 = st.doc('Charm_Designs/' + id + '/versions', '1');
    ok(head && head.version === 1 && head.name === 'Heart tag' && head.by === 'Ana_M' && head.createdBy === 'Ana_M' && head.widthMm === 20 && head.heightMm === 15 && head.hash === first.hash && !('faces' in head) && head.seq === 1, 'B the record: the latest fields, no faces');
    ok(v1 && v1.version === 1 && v1.faces && v1.faces.front.items.length === 5 && typeof v1.faces.front.items[0].d === 'string' && !v1.faces.front.items[0].subpaths && v1.facesPath === null && v1.hash === first.hash, 'B version 1: its faces as path strings');
    ok(!nested(v1) && !nested(head), 'B no array inside an array (the fake refuses them as Firestore does: strictArrays)');
    const t1 = v1.faces.front.items[4];
    ok(t1.text === 'AB' && t1.fontKey === 'jost-ace' && t1.layer === 'words' && t1.origin === 'added' && !('_cache' in t1), 'B a text keeps its words, font and what else it carries (short plain values; the editor\'s own _ fields are not kept)');
    const pic = 'charmnest/designs/thumbs/' + first.hash + '.png';
    ok(st.blobs.has(pic) && head.thumbPath === pic && /firebasestorage\.googleapis\.com\/v0\/b\/.+charmnest%2Fdesigns%2Fthumbs%2F/.test(head.thumbUrl) && first.row.thumbUrl === head.thumbUrl, 'B the picture is in Storage, named by the geometry hash, and its link is on the record and the row');
    eq((st.doc('Charm_Nest_Rev', 'designs') || {}).n, 1, 'B the count is 1');
    ok(first.rev === st.doc('Charm_Nest_Rev', 'designs').epoch + '.1' && first.prev === null, 'B the answer says the revision after the save (rev) and before it (prev: none, the first)');
    r = await put(design({ id, orderId: '4180000001', name: 'Heart tag v2', faces: Object.assign(design().faces, { back: undefined }) }), { by: 'Bea' });
    const head2 = st.doc('Charm_Designs', id);
    ok(r.status === 200 && r.body.version === 2 && head2.version === 2 && head2.by === 'Bea' && head2.createdBy === 'Ana_M' && head2.orderId === '4180000001' && head2.seq === 2 && r.body.prev === first.rev, 'B a second save is version 2 (who made the first is kept), the order on it');
    eq([head2.orders, head2.people], [['4180000001'], ['Ana_M', 'Bea']], 'B every order and person of its versions');
    ok(JSON.stringify(st.doc('Charm_Designs/' + id + '/versions', '1')) === JSON.stringify(v1) && !st.doc('Charm_Designs/' + id + '/versions', '2').faces.back, 'B version 1 is untouched; version 2 is as sent (the front only)');
    ok(r.body.hash !== first.hash && head2.thumbUrl === null, 'B another drawing (no back): another hash, and no picture of the old drawing');
    r = await put(design({ id, name: 'Heart tag v3' }), { by: 'Bea' });
    ok(r.status === 200 && r.body.version === 3 && r.body.hash === first.hash && st.doc('Charm_Designs', id).thumbUrl === head.thumbUrl, 'B the first drawing saved again under another name: its hash again, and it keeps its picture');

    /* ── C · the hash (D6) ── */
    ok(first.hash === hashOf(design()), 'C the cloud\'s hash is canonical()\'s (the one rule, charm-nest-designs.js)');
    const renamed = design(); renamed.faces.front.items.forEach((it, i) => { it.id = 'x' + i; it.name = 'Part ' + i; }); renamed.name = 'Other';
    ok(hashOf(renamed) === first.hash, 'C ids and names do not change it');
    ok(hashOf(part('front', 4, { subpaths: [rect(35, 20, 40, 26.01), rect(41, 20, 46, 26)] })) !== first.hash, 'C a point moved by 0.01 pt does');
    const noHid = design(); noHid.faces.back.items.pop();
    ok(hashOf(noHid) === first.hash && hashOf(part('back', 1, { hidden: false })) !== first.hash, 'C a hidden part is not in it (not engraved); shown, it is');
    ok(hashOf(part('front', 0, { hidden: true })) === first.hash, 'C a cut line hidden is still in it (still cut)');
    ok(hashOf(design({ widthMm: 20.04 })) !== first.hash, 'C the size is in it');
    const geo = CE.model.createGeo(), heart = design().faces.front.items[2];
    ok(D.matrixOf(heart).every((v, i) => Math.abs(v - geo.matrix(heart)[i]) < 1e-9), 'C a turned, scaled curve without its matrix is placed by the editor\'s own rule (about its TIGHT box): ' + D.matrixOf(heart).map(v => v.toFixed(4)).join(' '));
    const noWords = design(); noWords.faces.front.items.pop();   // (the editor makes words' outlines again from its font engine: here its stand-in)
    const result = clone(noWords); for (const f of ['front', 'back']) result.faces[f].items = CE.model.exportItems(noWords.faces[f].items, geo);
    ok(result.faces.front.items[2].matrix && hashOf(result) === hashOf(noWords), 'C the Charm Editor\'s result (matrices, its own fields) hashes the same as what it was opened with');
    const solo = design(); solo.faces.front.items[2] = { id: 'heart', kind: 'path', role: 'hatch', fill: [0, 0, 1], matrix: D.matrixOf(heart), subpaths: heart.subpaths };   // (an importer's part: a matrix alone)
    const soloC = D.clean(solo).design.faces.front.items[2];
    ok(!soloC.matrix && Math.abs(soloC.subpaths[0][0][1][0] - D.leaves(D.clean(design()).design.faces).front[2].subpaths[0][0][1][0]) < 1e-3 && hashOf(solo) === first.hash, 'C a part placed by a matrix alone: the matrix put into its geometry (the editor opens it where it is hashed), the same hash');
    const g2 = design(); g2.faces.front.items[3] = { id: 'g1', kind: 'group', role: 'art', matrix: [1, 0, 0, 1, 5, 2], children: [{ id: 'ln', kind: 'path', role: 'line', stroke: [1, 0, 0], lwPt: 0.4, subpaths: [[['m', [2, 30]], ['l', [30, 30]]]] }] };
    ok(hashOf(g2) === first.hash && !D.clean(g2).design.faces.front.items[3].matrix && D.clean(g2).design.faces.front.items[3].children[0].subpaths[0][0][1].join() === '7,32', 'C a group placed by a matrix alone: put into its parts, the same hash');
    const disagree = part('front', 2, { matrix: [1, 0, 0, 1, 0, 0] }), wordsIn = design(); wordsIn.faces.front.items[3] = { id: 'g1', kind: 'group', matrix: [1, 0, 0, 1, 5, 2], children: [design().faces.front.items[4]] }; wordsIn.faces.front.items.pop();
    ok(/not what its move, turn and scale/.test(D.clean(disagree).error) && /words in a group placed by a matrix alone/.test(D.clean(wordsIn).error), 'C a matrix that is not what tx … rot make, and words in a group placed by a matrix alone, are refused');
    const L = D.leaves(D.clean(design()).design.faces);
    eq([L.front.map(l => l.id), L.back.map(l => l.id)], [['outline', 'hole', 'heart', 'ln', 't1'], ['outline']], 'C leaves(): every part as made, in z-order, the hidden one left out');
    ok(Math.abs(L.front[3].subpaths[0][0][1][0] - 7) < 1e-9 && Math.abs(L.front[3].subpaths[0][0][1][1] - 32) < 1e-9, 'C a part in a moved group is placed by the group\'s matrix');

    /* ── D · designList ── */
    const id2 = (await put(design({ name: 'Moon charm', customName: 'Lost moon', orderId: 'CUSTOM-20261028-1', source: { kind: 'import', file: 'moon.ai' } }), { by: 'Cara' })).body.id;
    let [lr, cost] = await reads(() => call({ op: 'designList' }));
    ok(lr.status === 200 && lr.body.designs.map(x => x.id).join() === [id2, id].join() && lr.body.upTo === st.doc('Charm_Nest_Rev', 'designs').epoch + '.4' && lr.body.next === null, 'D the list, newest first, and upTo (' + lr.body.upTo + ', ' + cost + ' reads)');
    const row = lr.body.designs[1];
    eq(Object.keys(row).sort(), ['at', 'by', 'createdAt', 'createdBy', 'customName', 'hash', 'heightMm', 'id', 'name', 'orderId', 'orders', 'source', 'thumbUrl', 'version', 'widthMm'], 'D a row: what a list shows (no faces, no words, no count)');
    const upTo = lr.body.upTo;
    [lr, cost] = await reads(() => call({ op: 'designList', since: upTo }));
    ok(lr.body.unchanged === true && lr.body.designs.length === 0 && lr.body.upTo === upTo && cost === 1, `D since = upTo: unchanged, for ${cost} read`);
    [lr, cost] = await reads(() => call({ op: 'designList', ifRev: upTo })); ok(lr.body.unchanged === true && cost === 1, 'D ifRev says the same');
    await put(design({ id: id2, name: 'Moon charm', customName: 'Lost moon', orderId: 'CUSTOM-20261028-1', source: { kind: 'import', file: 'moon.ai' } }), { by: 'Cara' });
    [lr, cost] = await reads(() => call({ op: 'designList', since: upTo }));
    ok(!lr.body.unchanged && lr.body.designs.length === 1 && lr.body.designs[0].id === id2 && lr.body.designs[0].version === 2 && cost === 2, `D a save since: only that design is sent (${cost} reads: the count and it)`);
    const p1 = await call({ op: 'designList', limit: 1 }), p2 = await call({ op: 'designList', limit: 1, cursor: p1.body.next }), p3 = await call({ op: 'designList', limit: 1, cursor: p2.body.next });
    ok(p1.body.designs[0].id === id2 && p2.body.designs[0].id === id && p2.body.next != null && p3.body.designs.length === 0 && p3.body.next === null, 'D parts by cursor, newest first');
    lr = await call({ op: 'designList', since: 'zzzz.3' }); ok(lr.body.reset === true && lr.body.designs.length === 2, 'D a revision the count does not know (it began again: a sandbox reset) is answered whole, reset');
    lr = await call({ op: 'designList', orderId: '4180000001' }); ok(lr.body.designs.map(x => x.id).join() === id, 'D by order: every design any version of which was on it (item 21)');
    lr = await call({ op: 'designList', orderId: 'CUSTOM-20261028-1' }); ok(lr.body.designs.map(x => x.id).join() === id2, 'D by an orderless piece\'s CUSTOM id');
    lr = await call({ op: 'designList', by: 'Bea' }); ok(lr.body.designs.map(x => x.id).join() === id, 'D by person (anyone who saved a version)');
    lr = await call({ op: 'designList', q: 'lost moon' }); ok(lr.body.designs.map(x => x.id).join() === id2, 'D by words (the custom name)');
    lr = await call({ op: 'designList', q: 'moon.ai' }); ok(lr.body.designs.map(x => x.id).join() === id2, 'D by the imported file');
    lr = await call({ op: 'designList', orderId: 'x' }); ok(lr.status === 400, 'D an order that is none is refused');

    /* ── E · designGet ── */
    let g = await call({ op: 'designGet', id });
    ok(g.status === 200 && g.body.design.version === 3 && g.body.design.name === 'Heart tag v3' && g.body.design.faces.front.items.length === 5, 'E the latest version');
    eq(g.body.design.faces, D.clean(design()).design.faces, 'E its faces come back exactly as they were cleaned (subpaths, the extra fields in place)');
    ok(await D.verify(g.body.design), 'E verify(): the design is still what was hashed');
    g = await call({ op: 'designGet', id, version: 2, history: true });
    ok(g.body.design.version === 2 && g.body.design.by === 'Bea' && g.body.history.map(h => h.version).join() === '3,2,1' && g.body.history[2].by === 'Ana_M' && !('faces' in g.body.history[0]), 'E a version by number, and the history (newest first, no faces)');
    ok((await call({ op: 'designGet', id: 'nosuchid' })).body.design === null && (await call({ op: 'designGet', id, version: 9 })).body.design === null && (await call({ op: 'designGet', id: '../x' })).status === 400, 'E none: null; a bad id: refused');
    // a traced photograph: more than a record holds
    const trace = Array.from({ length: 2600 }, (_, i) => { const x = 2 + (i % 50) * 1.1, y = 2 + Math.floor(i / 50) * 0.75; return [['m', [x, y]], ['c', [x + 0.3, y], [x + 0.6, y + 0.2], [x + 0.6, y + 0.5]], ['c', [x + 0.6, y + 0.7], [x + 0.2, y + 0.7], [x, y + 0.5]], ['c', [x - 0.1, y + 0.3], [x - 0.1, y + 0.1], [x, y]], ['h']]; });
    const big = design({ name: 'Photo', faces: { front: { face: 'front', frame: { x0: 0, y0: 0, wPt: W, hPt: H }, items: [{ id: 'outline', kind: 'path', role: 'cut', subpaths: [rect(0, 0, W, H)] }].concat([0.2, 0.45, 0.7].map((gr, k) => ({ id: 'lv' + k, kind: 'path', role: 'art', fill: [gr, gr, gr], subpaths: trace.map(sp => sp.map(o => o[0] === 'h' ? o : [o[0], ...o.slice(1).map(p => [p[0] + k * 0.013, p[1]])])) }))) } } });
    const bigC = D.clean(big); ok(bigC.bytes > D.LIMITS.inline, 'E (the photograph is ' + Math.round(bigC.bytes / 1000) + ' KB as kept)');
    r = await put(big); const bv = st.doc('Charm_Designs/' + r.body.id + '/versions', '1');
    ok(r.status === 200 && bv.faces === null && /^charmnest\/designs\/faces\/.+\.json\.gz$/.test(bv.facesPath) && st.blobs.has(bv.facesPath) && st.blobs.get(bv.facesPath).buf.length < bigC.bytes / 2, 'E a very big design keeps its faces in Storage beside its record, gzip (' + Math.round(st.blobs.get(bv.facesPath).buf.length / 1000) + ' KB)');
    g = await call({ op: 'designGet', id: r.body.id });
    ok(JSON.stringify(g.body.design.faces) === JSON.stringify(bigC.design.faces) && await D.verify(g.body.design), 'E and reads back whole, its hash verified');
    ok(JSON.parse(zlib.gunzipSync(st.blobs.get(bv.facesPath).buf)).front.items.length === 4, 'E (the file is the kept faces)');

    /* ── F · the sandbox, the counters ── */
    const prod = JSON.stringify([...st.docs.entries()].filter(([k]) => /^Charm_Designs|^Charm_Nest_Rev\/designs/.test(k)));
    r = await put(design(), { sandbox: true, thumb: PNG });
    const sb = st.doc('Sandbox_Charm_Designs', r.body.id);
    ok(r.status === 200 && sb && st.doc('Sandbox_Charm_Designs/' + r.body.id + '/versions', '1') && st.doc('Sandbox_Order_Rev', '_designs').n === 1 && st.blobs.has('charmnest/sandbox/designs/thumbs/' + r.body.hash + '.png'), 'F the sandbox: its own record, version, count (Sandbox_Order_Rev/_designs) and picture (charmnest/sandbox/)');
    eq(JSON.stringify([...st.docs.entries()].filter(([k]) => /^Charm_Designs|^Charm_Nest_Rev\/designs/.test(k))), prod, 'F production untouched');
    ok((await call({ op: 'designGet', id: r.body.id })).body.design === null && (await call({ op: 'designList', sandbox: true })).body.designs.map(x => x.id).join() === r.body.id, 'F production does not see it; the sandbox sees only its own');
    const gen = () => (st.doc('Charm_Nest_Rev', 'placement') || {}).n || 0, g0 = gen();
    await put(design()); await call({ op: 'designList' }); await call({ op: 'designGet', id });
    eq(gen(), g0, 'F no design op raises the placement counter (nothing a placement answer is made from)');

    /* ── G · the page module ── */
    const ls = new Map(); global.localStorage = { getItem: k => (ls.has(k) ? ls.get(k) : null), setItem: (k, v) => ls.set(k, String(v)), removeItem: k => ls.delete(k) };
    const sent = [], logged = []; global.CNDesignLog = { event: (kind, data) => logged.push([kind, data]) };
    D.setHost({ api: async (fn, body) => { sent.push(body.op); const x = await call(body); if (x.status >= 400) throw Object.assign(new Error(x.body.error || 'HTTP ' + x.status), { status: x.status }); return x.body; }, who: () => 'Dana', sandbox: false });
    D._reset();
    const flush = () => new Promise(res => setTimeout(res, 5));
    let rows = await D.list(); await flush();
    ok(sent.join() === 'designList' && rows.length === 4 && rows[0].name === 'Heart tag' && ls.has('cn.designs.v1'), 'G the first list: one call, the whole library, kept on this computer (' + rows.length + ' designs)');
    sent.length = 0; rows = await D.list(); ok(sent.length === 0 && rows.length === 4, 'G within 20 s: from memory, no call');
    let refused = null; try { await D.put(design({ widthMm: 30 })); } catch (e) { refused = e; } ok(refused && refused.refused && /cut line measures/.test(refused.message) && sent.length === 0, 'G a design the cloud would refuse is refused here, before anything is sent');
    const onc = []; const off = D.onChange(ev => onc.push(ev.kind));
    const fresh = design({ name: 'Star charm', orderId: '4180000777', source: { kind: 'piece', poolId: '4180000777_1_1', sku: 'STAR-1' }, widthMm: undefined, heightMm: undefined });
    const pr = await D.put(fresh); await flush();
    ok(pr.version === 1 && sent.join() === 'designPut' && D.rows()[0].id === pr.id && D.rows()[0].by === 'Dana' && onc.includes('put'), 'G put: one call, the row at the top of the index at once, who from the page; onChange told');
    ok(st.doc('Charm_Designs', pr.id).widthMm === 20 && st.doc('Charm_Designs', pr.id).heightMm === 15, 'G a size not given is the cut line\'s');
    const lg = logged.find(x => x[0] === 'designSave');
    ok(lg && lg[1].designId === pr.id && lg[1].version === 1 && lg[1].orderId === '4180000777' && lg[1].sku === 'STAR-1' && lg[1].name === 'Star charm' && lg[1].faces.join() === 'front,back', 'G the employee log hears designSave (designId, version, name, sku, order, faces)');
    const t0 = performance.now(), hits = D.search('star'), ms0 = performance.now() - t0;
    ok(hits[0].id === pr.id && D.search('4180000777')[0].id === pr.id && D.search('dana').length === 1 && D.search('lost moon')[0].id === id2 && D.search('heart tag v3')[0].id === id && D.search('xyzzy').length === 0, 'G search at once: by name, order, person, custom name');
    sent.length = 0; D._reset(); rows = D.rows();
    ok(rows.length === 5 && sent.length === 0 && rows[0].id === pr.id, 'G a reload: the index read from this computer, no call');
    [rows, cost] = await reads(() => D.list());
    ok(sent.join() === 'designList' && cost === 1 && rows.length === 5, `G ... and the cloud asked one question (since): unchanged, ${cost} read`);
    await call({ op: 'designPut', by: 'Eve', design: design({ name: 'Elsewhere' }) });   // another computer
    sent.length = 0; let told = 0; const off2 = D.onChange(() => told++);
    rows = await D.list({ force: true });
    ok(sent.join() === 'designList' && rows.length === 6 && rows[0].name === 'Elsewhere' && told === 1, 'G another computer\'s save: it alone comes in, onChange told');
    off(); off2();
    sent.length = 0; const gd = await D.get(pr.id, 1), gd2 = await D.get(pr.id, 1);
    ok(gd && gd === gd2 && sent.join() === 'designGet' && gd.faces.front.items.length === 5, 'G get: a version read once, then kept in memory');
    ok((await D.get(pr.id, 1, { open: true })) === gd && logged.some(x => x[0] === 'designOpen' && x[1].designId === pr.id), 'G get with open: the employee log hears designOpen');
    // instant search on 3,000 designs
    const many = Array.from({ length: 3000 }, (_, i) => ({ id: 'dm' + i, name: (i % 3 ? 'Heart ' : 'Initial ') + i, version: 1, at: 1e12 + i, by: ['Ana_M', 'Bea', 'Cara'][i % 3], orderId: String(4180000000 + i), source: { kind: 'master', sku: 'SKU-' + i }, hash: 'x' }));
    D.useStore({ load: () => ({ rows: many, upTo: null }), save() {} });
    D.search('warm'); const ts = performance.now(); let found = 0; for (let i = 0; i < 20; i++) found += D.search(['heart 12', 'bea 41800002', 'sku-29', 'initial'][i % 4]).length; const each = (performance.now() - ts) / 20;
    ok(each < 16 && found > 0, `G search over 3,000 designs: ${each.toFixed(2)} ms a query (within a frame); the first, 5 designs: ${ms0.toFixed(2)} ms`);
    D.useStore(null); D._reset();
    // the sandbox's index is its own key (a sandbox reset clears every key with ":sandbox")
    D.setHost({ sandbox: true, api: async (fn, body) => (await call(Object.assign({}, body, { sandbox: true }))).body }); await D.list(); await flush();   // (the page's api() tags every call of a sandbox page)
    ok(ls.has('cn.designs.v1:sandbox') && D.rows().length === 1 && D.rows()[0].id === sb.id, 'G the sandbox keeps its own index (cn.designs.v1:sandbox)');
    for (const k of [...st.docs.keys()]) if (/^Sandbox_(Charm_Designs|Order_Rev)\//.test(k)) st.docs.delete(k);   // (a sandbox reset, on another computer)
    const after = (await call({ op: 'designPut', by: 'Eve', sandbox: true, design: design({ name: 'After the reset' }) })).body;
    let asked = 0; D.setHost({ api: async (fn, body) => { asked++; return (await call(Object.assign({}, body, { sandbox: true }))).body; } });
    rows = await D.list({ force: true });
    ok(asked === 1 && rows.length === 1 && rows[0].id === after.id, 'G after a sandbox reset the count began again: the page drops what it held and takes the whole list from that one answer');
    console.log(`PASS ${n} checks: DESIGNS — designPut / designList / designGet checked strictly, every save a version with who, when, its order and a geometry hash (D6), pictures and big faces in Storage, since/ifRev unchanged for 1 read, the sandbox apart, no placement counter; the page module's instant search (${each.toFixed(2)} ms on 3,000) and one-question refresh`);
  } finally { srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
