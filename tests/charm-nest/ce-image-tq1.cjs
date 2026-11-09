/* TQ1 (Paul, 9 Nov 2026: "When I vectorize an image, it looks really good in a preview, but then when it gets applied to the actual charm, it looks like crap and it loses all its fidelity and
   quality. Please fix it."). His case: a cat photo, Trace = Greys, Levels 16, Detail 100, Smoothness 100, Despeckle 0, placed 7.47 × 9.96 mm. The preview was traced from 280 px with no limit;
   Place traced at the placed size within what ONE piece's record holds (450 KB of path), and the full trace is about 4.2 MB, so it was smoothed three steps into posterised blobs.
   Now the picture's big paths are kept in a file beside the record (charm-nest-art-edit.js pack / unpack, charmNestLibrary.js ART_BLOB), the Image step's budget is 200 000 points a
   picture, and the preview is the trace at the placed size. No live service, no credential, no network: the real handler over the in-memory Firestore and Storage of bridge-server.cjs.
     A  quality: a cat-like picture (fine fur strokes, eyes, whiskers 2 px wide) at Paul's settings and size: the quick look the page used to show (280 px, no budget) and what Place
        now puts on the charm (IM.placeSync: the same as the page's preview at the placed size), against the picture: blurred grey error and fine detail kept (the correlation of
        the detail finer than 3 px). Place is not simplified, and is as good as the preview or better. (The old Place, measured on 9 Nov 2026 with ce-trace-quality.cjs (d)
        QUALITY_BEFORE: simplified 3 steps, 6 990 operators, blurred error 4.47, fine detail 0.77; the quick look 3.94, 0.90.)
     B  the server: a save with its picture's paths beside it (blob) is checked whole and refused whole: a bad gzip, a part naming a path not sent, a cut line in the file, a path in
        the file no part names, two parts naming one path, too many operators; a good one keeps ONE file by its hash (sent twice: one file) and the record names it
     C  round trip: Place → the piece's record saved through the real artPut (the record small, the file in Storage) → this page forgets it → read again as another computer reads it
        (artList, then the file through the asset door, charmNestAsset) → every path whole: the piece's charm, the editor opened on it (decode), and the laser's files written from it
        (.pdf / .ai of the sheet, CharmNestPDF.buildSheet, its production .ai and .dxf, charm-nest-export.js): each holds the picture's full-detail paths; their sizes and times said
     D  the design library: the same picture saved as a design is sent gzipped (designGz) and read back by its link (facesUrl, through the asset door): the same paths
   node tests/charm-nest/ce-image-tq1.cjs */
'use strict';
const assert = require('node:assert/strict'), path = require('node:path'), zlib = require('node:zlib');
const ROOT = path.join(__dirname, '..', '..');
global.window = global; global.self = global;
global.PDFLib = require(path.join(ROOT, 'vendor/pdf-lib-1.17.1.min.js')); global.JSZip = require(path.join(ROOT, 'vendor/jszip-3.10.1.min.js'));
require(path.join(ROOT, 'charm-nest-pdf.js'));
global.CharmNestVector = require(path.join(ROOT, 'charm-nest-vector.js'));
global.CharmNestGeom = require(path.join(ROOT, 'charm-nest-geom.js'));
const P = global.CharmNestPDF, E = global.CharmNestExport = require(path.join(ROOT, 'charm-nest-export.js'));
const AE = require(path.join(ROOT, 'charm-nest-art-edit.js')), IM = require(path.join(ROOT, 'charm-nest-ce-image.js')), T = require(path.join(ROOT, 'charm-nest-ce-trace.js'));
const D = require(path.join(ROOT, 'charm-nest-designs.js'));
const Q = require('./ce-trace-quality.cjs'), assets = require(path.join(ROOT, 'netlify/functions/_charmNestAsset.js'));
const { start } = require('./bridge-server.cjs');
let n = 0; const ok = (c, m) => { assert.ok(c, m); n++; console.log('  ok   ' + m); };
const say = s => console.log('       ' + s);
const now = () => Number(process.hrtime.bigint() / 1000n) / 1000;
const PT = 72 / 25.4, S = { levels: 16, detail: 1, smooth: 1, despeckle: 0 }, W_MM = 7.47, H_MM = 9.96;
const opsOfD = d => (String(d).match(/[MLCZ]/g) || []).length, opsOfSubs = sps => sps.reduce((a, sp) => a + sp.length, 0);

(async () => {
  /* ── A · quality, at Paul's settings and size ── */
  const pic = Q.cat(1800, 2400), EW = 300, EH = 400, ref = Q.reference(pic, EW, EH), fineRef = Q.fine(ref, EW, EH);
  const quick = Q.quickLook(pic, S, H_MM), qq = Q.photoQuality(quick, ref, EW, EH);
  let t = now(); const placed = IM.placeSync(pic, S, H_MM), placeMs = now() - t, qp = Q.photoQuality(placed.res, ref, EW, EH);
  const fq = Q.corr(fineRef, Q.fine(qq.grey, EW, EH)), fp = Q.corr(fineRef, Q.fine(qp.grey, EW, EH)), w = placed.res.work;
  say(`the quick look (280 px, no budget): ${IM.sizeOf(quick.layers).ops} operators · blurred grey error ${qq.maeBlur.toFixed(2)} / 255 · fine detail kept ${fq.toFixed(3)}`);
  say(`Place: traced at ${w.width} × ${w.height} (${w.pxPerMm.toFixed(1)} px a mm), simplified ${w.simplified}, made smaller ${w.shrunk + placed.tries}, ${placed.size.ops} operators (${(placed.size.bytes / 1e6).toFixed(2)} MB as path text), ${placeMs.toFixed(0)} ms · blurred grey error ${qp.maeBlur.toFixed(2)} · fine detail kept ${fp.toFixed(3)}`);
  ok(placed.fits && !w.simplified && !w.shrunk && !placed.tries, 'A Paul\'s case is placed at its full detail: not simplified, not made smaller');
  ok(qp.maeBlur <= qq.maeBlur + 0.2 && fp >= fq - 0.02 && fp > 0.88, `A what is placed is as good as the preview or better (blurred error ${qp.maeBlur.toFixed(2)} against ${qq.maeBlur.toFixed(2)}; fine detail ${fp.toFixed(3)} against ${fq.toFixed(3)})`);

  const srv = await start({ receipts: [], strictArrays: true }), st = srv.st;
  try {
    const call = async body => { const out = await st.handlers.charmNestLibrary.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body), queryStringParameters: {} }); return { status: out.statusCode, body: JSON.parse(out.body || '{}') }; };
    // the asset door, as the page reads a file by its link (charmNestAsset)
    const door = async url => { const r = await assets.serve(new Request('http://sorter.test/.netlify/functions/charmNestAsset?url=' + encodeURIComponent(url)), st.bucket); if (r.status !== 200) throw new Error('asset ' + r.status); return new Uint8Array(await r.arrayBuffer()); };
    global.CharmNestAssets = { bytes: door };

    /* ── B · the server's checks ── */
    const ID = '4170000101_10010_1';
    st.put('Charm_Pool', ID, { poolId: ID, orderId: '4170000101', sku: 'CALENDAR', state: 'pooled', sheetId: null });
    const gz = obj => zlib.gzipSync(Buffer.from(typeof obj === 'string' ? obj : JSON.stringify(obj))).toString('base64');
    const part = (extra) => Object.assign({ id: 'g1', kind: 'path', role: 'hatch', fill: [0.5, 0.5, 0.5], artGrey: true, even: true }, extra);
    const base = { baseKey: 'k1a2b3', removed: [], changed: {}, added: [] };
    const files = () => [...st.blobs.keys()].filter(k => k.includes('art-blob/')).length, docs = () => st.list('Charm_Pool_Art').length;
    const bad = async (body, re, m) => { const f0 = files(), d0 = docs(), r = await call(Object.assign({ op: 'artPut', poolId: ID, face: 'front', by: 'Ana_M' }, body)); ok(r.status === 400 && re.test(r.body.error || '') && files() === f0 && docs() === d0, `B refused, nothing kept: ${m} ("${r.body.error}")`); };
    const big = 'M1 1' + 'L2.25 3.5L4.5 1.75'.repeat(5000) + 'Z';
    await bad({ art: Object.assign({}, base, { added: [part({ bi: 0 })] }), blob: 'bm90IGd6aXA=' }, /not a gzip/, 'paths that are not a gzip');
    await bad({ art: Object.assign({}, base, { added: [part({ bi: 0 })] }) }, /no such path/, 'a part naming a path in a file the save did not send');
    await bad({ art: Object.assign({}, base, { added: [part({ bi: 1 })] }), blob: gz({ v: 1, ds: [big] }) }, /no such path/, 'a part naming a path the file does not hold');
    await bad({ art: Object.assign({}, base, { added: [part({ bi: 0, role: 'cut', fill: null, artGrey: undefined, stroke: [0, 0, 0] })] }), blob: gz({ v: 1, ds: [big] }) }, /cut line is kept in the record/, 'a cut line in the file');
    await bad({ art: Object.assign({}, base, { added: [part({ bi: 0 })] }), blob: gz({ v: 1, ds: [big, big] }) }, /no part of the edit names/, 'a path in the file no part names');
    await bad({ art: Object.assign({}, base, { added: [part({ bi: 0 }), part({ id: 'g2', bi: 0 })] }), blob: gz({ v: 1, ds: [big] }) }, /two parts name one path/, 'two parts naming one path');
    await bad({ art: Object.assign({}, base, { added: [part({ bi: 0 })] }), blob: gz({ v: 1, ds: ['M0 0L1 1Q2 2'] }) }, /its path is not M/, 'a path in the file that is not M L C Z');
    const many = 'M0 0' + 'L1 2'.repeat(130000);
    await bad({ art: Object.assign({}, base, { added: [part({ bi: 0 }), part({ id: 'g2', bi: 1 })] }), blob: gz({ v: 1, ds: [many, many] }) }, /at most 250000/, '260 002 operators in the file');
    const good = { art: Object.assign({}, base, { added: [part({ bi: 0 })] }), blob: gz({ v: 1, ds: [big] }) };
    const r1 = await call(Object.assign({ op: 'artPut', poolId: ID, face: 'front', by: 'Ana_M' }, good));
    const rec1 = st.doc('Charm_Pool_Art', ID + '~front');
    ok(r1.status === 200 && rec1.blob && /^charmnest\/art-blob\/[0-9a-f]{32}\.json\.gz$/.test(rec1.blob.path) && rec1.added[0].bi === 0 && rec1.added[0].d == null && rec1.blob.ops === opsOfD(big) && files() === 1, 'B a good one: ONE file by its hash, the record names it and its part its path (no d in the record)');
    const r2 = await call(Object.assign({ op: 'artPut', poolId: ID, face: 'front', by: 'Ana_M', expectedAt: rec1.at }, good));
    ok(r2.status === 200 && files() === 1 && st.doc('Charm_Pool_Art', ID + '~front').blob.key === rec1.blob.key, 'B the same paths sent again: the same file, not another');

    /* ── C · round trip: Place → save → read again → the editor and the laser's files ── */
    const cal = await require('./_artCalendar.cjs')(ROOT), cb = cal.charm, PID = '4170000103_10030_1';
    st.put('Charm_Pool', PID, { poolId: PID, orderId: '4170000103', sku: 'CALENDAR', state: 'pooled', sheetId: null });
    const view = global.CharmNestGeom.backView(cb, { face: 'front', res: 6 }), fr = AE.frame(view);
    const ob = cb.outline.bbox, c0 = fr.toItem([(ob[0] + ob[2]) / 2, (ob[1] + ob[3]) / 2]), bw = W_MM * PT, bh = H_MM * PT;
    const g = IM.buildGroup(placed.res, [c0[0] - bw / 2, c0[1] - bh / 2, c0[0] + bw / 2, c0[1] + bh / 2], { id: 'scat0000000000000000p00', src: 'scat0000000000000000p00', settings: S, name: 'cat.jpg' });
    const groupOps = g.children.reduce((a, k) => a + opsOfSubs(k.subpaths), 0);
    const page = { rows: new Map(), sheets: [{ sheetId: 'sheet-p', charms: [Object.assign({}, cb, { id: 's:' + PID, poolId: PID, pinned: null })] }], reads: [], sent: 0 };
    AE.setHost({
      api: async (fn, body) => { page.reads.push(body.op); page.sent = Math.max(page.sent, JSON.stringify(body).length); const r = await call(body); if (r.status >= 400) throw Object.assign(new Error(r.body.error || 'HTTP ' + r.status), { status: r.status, data: r.body }); return r.body; },
      who: () => 'Ana_M', rowOf: id => page.rows.get(id) || null, charmOf: id => page.sheets[0].charms.find(c => c.poolId === id) || null, placed: () => false,
      flag: (id, face, at) => { const r = page.rows.get(id) || {}; r.artEdit = Object.assign({}, r.artEdit, { [face]: at || null }); page.rows.set(id, r); },
      swap: (id, next) => { page.sheets[0].charms = page.sheets[0].charms.map(c => (c.poolId === id ? next : c)); }, changed: async () => {}, liveCharms: () => page.sheets[0].charms
    });
    t = now(); const saved = await AE.save(PID, 'front', { added: [g] }, { view }); const saveMs = now() - t;
    const rec = st.doc('Charm_Pool_Art', PID + '~front'), recBytes = Buffer.byteLength(JSON.stringify(rec)), file = st.blobs.get(rec.blob.path);
    const inFile = rec.added.filter(e => e.bi != null), inline = rec.added.filter(e => e.d != null);
    say(`saved in ${saveMs.toFixed(0)} ms: the record ${(recBytes / 1000).toFixed(0)} KB (${inline.length} small greys in it), the picture's file ${(file.buf.length / 1e6).toFixed(2)} MB gzipped (${inFile.length} greys, ${rec.blob.ops} operators, ${(rec.blob.n / 1e6).toFixed(2)} MB of path); the biggest call sent ${(page.sent / 1e6).toFixed(2)} MB`);
    ok(recBytes < 600000 && inFile.length >= 1 && rec.blob.ops + inline.reduce((a, e) => a + opsOfD(e.d), 0) === groupOps && page.sent < 6e6, `C saved: the record within what one record holds (${(recBytes / 1000).toFixed(0)} KB), the paths beside it, every operator kept (${groupOps}), each call under a function's 6 MB`);
    ok(saved.charm.members.filter(m => m.artGrey).reduce((a, m) => a + opsOfSubs(m.subpaths), 0) === groupOps, 'C this page\'s piece carries every path at once (its own paths put back, nothing read)');
    // another computer: nothing held
    AE.forget(); page.reads.length = 0;
    const fresh = Object.assign({}, cb, { id: 's:' + PID, poolId: PID, pinned: null });
    t = now(); const made = await AE.piece(fresh, PID, { poolId: PID, artEdit: { front: rec.at } }); const readMs = now() - t;
    const madeOps = made.members.filter(m => m.artGrey).reduce((a, m) => a + opsOfSubs(m.subpaths), 0);
    ok(madeOps === groupOps && page.reads.join() === 'artList', `C read again (artList and the file through the asset door, ${readMs.toFixed(0)} ms): the piece is made with every path (${madeOps} operators)`);
    const dec = AE.decode(AE.cached(PID, 'front'), made, view), decOps = dec.added.filter(i => i.artGrey).reduce((a, i) => a + opsOfSubs(i.subpaths), 0);
    ok(decOps === groupOps && dec.added.filter(i => i.artGrey).length === g.children.length, `C the editor opened on it again (decode): its ${g.children.length} greys, every path whole`);
    // the laser's files
    const SHEET = { wPt: 120, hPt: 130 };
    t = now(); const pl = [{ charm: made, angle: 0, cxPt: 60, cyPt: 65, scale: 1 }], sheetBytes = await P.buildSheet({ sheet: SHEET, placements: pl, sources: new Map([[made.sourceId, cal.parsed]]), title: 'cat' }); const pdfMs = now() - t;
    const parsed = await P.parseSource(new Uint8Array(sheetBytes), 'sheet.ai'), forms = parsed.segments.filter(s => s.kind === 'xobj');
    const greyKids = forms[0].children.filter(k => k.kind === 'path' && k.fill && k.fillRGB[0] === k.fillRGB[1] && k.fillRGB[1] === k.fillRGB[2] && !(k.fillRGB[0] === 0 && k.fillRGB[2] === 1));
    const sheetOps = greyKids.reduce((a, k) => a + opsOfSubs(k.subpaths), 0);
    say(`the sheet's .pdf / .ai (buildSheet): ${(sheetBytes.length / 1e6).toFixed(2)} MB, written in ${pdfMs.toFixed(0)} ms, ${greyKids.length} grey fills holding ${sheetOps} operators`);
    ok(sheetOps >= groupOps * 0.99 && greyKids.length === g.children.length, 'C the sheet\'s laser file holds the picture at its full detail, each grey its own fill');
    const recOf = list => ({ id: 'sheet-cat', fileBase: 'GF_cat', metal: 'gold', charms: list.map(c => ({ id: c.id, poolId: c.poolId })), poolIds: list.map(c => c.poolId), placements: pl.map((p, i) => ({ id: list[i].id, n: i + 1, layer: p.layerName, scale: p.scale })), backPool: [], outputs: {} });
    t = now(); const prod = await E.compose(sheetBytes, recOf([made]), []); const aiMs = now() - t;
    const lay = await P.parseSource(prod.ai, 'prod.ai'); t = now(); const dxf = E.dxf(E.productionPaths(lay), E.layerNames(lay)).text; const dxfMs = now() - t;
    const prodOps = E.leaves(lay).filter(p => p.fill && p.fillRGB && p.fillRGB[0] === p.fillRGB[1] && p.fillRGB[1] === p.fillRGB[2] && p.fillRGB[0] < 0.999).reduce((a, p) => a + opsOfSubs(p.subpaths || []), 0);
    const hatches = (dxf.match(/\r\n0\r\nHATCH\r\n/g) || []).length;
    say(`the production .ai (compose): ${(prod.ai.length / 1e6).toFixed(2)} MB in ${aiMs.toFixed(0)} ms, ${prodOps} operators in its greys · the .dxf: ${(dxf.length / 1e6).toFixed(2)} MB in ${dxfMs.toFixed(0)} ms, ${hatches} HATCH entities`);
    ok(prodOps >= groupOps * 0.99 && hatches >= g.children.length, 'C the production .ai and the .dxf hold it in full too');

    /* ── D · the design library ── */
    const design = { name: 'Cat test', faces: { front: { face: 'front', unitPt: true, frame: { x0: 0, y0: 0, wPt: 80, hPt: 90 }, items: [Object.assign({}, g)] } }, widthMm: 28.2, heightMm: 31.7, source: { kind: 'new' } };
    let sentD = 0;
    D.setHost({ api: async (fn, body) => { sentD = Math.max(sentD, JSON.stringify(body).length); const r = await call(body); if (r.status >= 400) throw Object.assign(new Error(r.body.error || 'HTTP ' + r.status), { data: r.body }); return r.body; }, who: () => 'Ana_M', sandbox: false });
    const put = await D.put(design, { thumb: false });
    const ver = st.list('Charm_Designs').length ? (st.doc('Charm_Designs/' + put.id + '/versions', String(put.version)) || null) : null;
    const back = await call({ op: 'designGet', id: put.id, version: put.version });
    ok(!!put.id && sentD < 6e6 && back.status === 200 && back.body.design.faces === null && /firebasestorage/.test(back.body.design.facesUrl), `D the design is sent gzipped (${(sentD / 1e6).toFixed(2)} MB the call) and a version this big is answered by its link`);
    const got = await D.get(put.id, put.version), gotOps = (got.faces.front.items[0].children || []).reduce((a, k) => a + opsOfSubs(k.subpaths), 0);
    ok(gotOps === groupOps, `D read back through the asset door: the same ${gotOps} operators`);
    void ver;
  } finally { await srv.close(); }
  console.log(`\nPASS ${n} checks: TQ1 — the placed image keeps the preview's detail (its paths beside the piece's record), round trip to the laser's files, the design library`);
})().catch(e => { console.error(e); process.exit(1); });
