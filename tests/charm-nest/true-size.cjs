// TRUESIZE (Paul, 22 Nov 2026: "I had to set the size to 8.1 mm in order for the system to recognize it as 9 mm ... They must be considered the same size"): the size the app shows a person is the
// charm AS DRAWN (Illustrator's and the Charm Editor's W and H), never the reader's frame (the shape plus half its line and 1 pt on every side, which stays the silhouette's and the nest's).
//   1  a 9 mm disc with a 1.3 mm hole, drawn in a PDF with a 0.159 pt line: drawnPt reads 9.00 × 9.00 mm (its frame, widthPt, 9.88 mm, is unchanged)
//   2  every save of a repository entry sends drawnPt (the indexer, the Charm Editor's update, the custom design's save), and the server keeps it and gives it back (slimEntry)
//   3  the Charm Editor's repository card and the Library card show the size as drawn
//   4  TRUESIZE 3: the same, read as the shop computers read it, in the REAL background worker (charm-nest-compute-worker.js, through charm-nest-background.js's copy of a charm): a
//      9.0 mm circle drawn by the Charm Editor comes back 9.00 mm, never 9.2 (the frame without its 1 pt: the shape plus its line)
//   NODE_PATH=<deps> node tests/charm-nest/true-size.cjs
'use strict';
const fs = require('fs'), path = require('path'), root = path.join(__dirname, '../..');
let failed = 0; const ok = (n, c, d) => { console.log((c ? 'OK   ' : 'FAIL ') + n + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d))); if (!c) failed++; };
const MM = 25.4 / 72;
/** A one-page PDF: a circle of diameter d (pt) and a hole of diameter h (pt), stroked lw pt, on a 100 × 100 pt page. */
function discPdf(d, h, lw, turn) {
  const k = 0.5523, circ = (cx, cy, r) => turn ? turned(cx, cy, r) : `${cx + r} ${cy} m ${cx + r} ${cy + k * r} ${cx + k * r} ${cy + r} ${cx} ${cy + r} c ${cx - k * r} ${cy + r} ${cx - r} ${cy + k * r} ${cx - r} ${cy} c ${cx - r} ${cy - k * r} ${cx - k * r} ${cy - r} ${cx} ${cy - r} c ${cx + k * r} ${cy - r} ${cx + r} ${cy - k * r} ${cx + r} ${cy} c h S\n`;
  // (a circle as the Charm Editor may write it: its four arcs start at 45°, so their control points stand outside the circle)
  function turned(cx, cy, r) { const P = a => [cx + r * Math.cos(a), cy + r * Math.sin(a)], T = a => [-Math.sin(a), Math.cos(a)], f = v => v.toFixed(4); let out = '';
    for (let i = 0; i < 4; i++) { const a0 = Math.PI / 4 + i * Math.PI / 2, a1 = a0 + Math.PI / 2, p0 = P(a0), p1 = P(a1), t0 = T(a0), t1 = T(a1);
      if (!i) out += `${f(p0[0])} ${f(p0[1])} m `; out += `${f(p0[0] + k * r * t0[0])} ${f(p0[1] + k * r * t0[1])} ${f(p1[0] - k * r * t1[0])} ${f(p1[1] - k * r * t1[1])} ${f(p1[0])} ${f(p1[1])} c `; }
    return out + 'h S\n'; }
  const content = `0 0 0 RG ${lw} w 1 j 1 J\n` + circ(50, 50, d / 2) + circ(50, 50 + d / 2 - h, h / 2);
  const objs = ['<</Type/Catalog/Pages 2 0 R>>', '<</Type/Pages/Kids[3 0 R]/Count 1>>', '<</Type/Page/Parent 2 0 R/MediaBox[0 0 100 100]/Contents 4 0 R>>', `<</Length ${content.length}>>stream\n${content}endstream`];
  let out = '%PDF-1.4\n', offs = [];
  objs.forEach((o, i) => { offs.push(out.length); out += `${i + 1} 0 obj\n${o}\nendobj\n`; });
  const xref = out.length; out += `xref\n0 ${objs.length + 1}\n0000000000 65535 f \n` + offs.map(o => String(o).padStart(10, '0') + ' 00000 n \n').join('') + `trailer\n<</Size ${objs.length + 1}/Root 1 0 R>>\nstartxref\n${xref}\n%%EOF\n`;
  return new Uint8Array(Buffer.from(out, 'latin1'));
}
(async () => {
  const P = require(path.join(root, 'netlify/functions/_charmNestPdf')).withCanvas().CharmNestPDF;
  const parsed = await P.parseSource(discPdf(9 / MM, 1.3 / MM, 0.159), '9mm disc.ai');
  const g = await (P.groupCharmsAsync || P.groupCharms)(parsed, { minPt: 6 });
  await P.buildSilhouettes(parsed, g.charms, 6);
  const c = g.charms[0], dw = c && c.drawnPt ? c.drawnPt.map(v => +(v * MM).toFixed(2)) : null;
  ok('1 · a 9 mm disc reads 9.00 × 9.00 mm as drawn (the size Illustrator and the Charm Editor give)', g.charms.length === 1 && !!dw && Math.abs(dw[0] - 9) < 0.02 && Math.abs(dw[1] - 9) < 0.02, dw);
  ok('1 · its frame (widthPt, the silhouette\'s and the nest\'s) is unchanged: the shape plus half its line and 1 pt on every side', !!c && c.widthPt - 9 / MM >= 2 && c.widthPt - 9 / MM <= 3, c && c.widthPt);   // (the reader's line is at least 0.5 pt: 2.5 pt here)
  { const p2 = await P.parseSource(discPdf(9 / MM, 1.3 / MM, 0.2, true), 'editor disc.pdf'), g2 = await (P.groupCharmsAsync || P.groupCharms)(p2, { minPt: 6 }); await P.buildSilhouettes(p2, g2.charms, 6);
    const c2 = g2.charms[0], d2 = c2 && c2.drawnPt ? c2.drawnPt.map(v => +(v * MM).toFixed(2)) : null, cp = c2 ? +((c2.bbox[2] - c2.bbox[0]) * MM).toFixed(2) : null;
    ok('1 · TRUESIZE 2: a 9 mm circle whose arcs start at 45° (control points outside it, ' + cp + ' mm by them) reads 9.00 mm, measured on the curve as the Charm Editor measures W', !!d2 && Math.abs(d2[0] - 9) < 0.02 && Math.abs(d2[1] - 9) < 0.02 && cp > 9.5, { drawn: d2, byControlPoints: cp }); }
  const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8'), repo = fs.readFileSync(path.join(root, 'charm-nest-ce-repo.js'), 'utf8');
  const master = fs.readFileSync(path.join(root, 'netlify/functions/_charmNestMaster.js'), 'utf8'), save = fs.readFileSync(path.join(root, 'netlify/functions/_charmNestCustomSave.js'), 'utf8');
  ok('2 · the indexer, the Charm Editor\'s update and the custom design\'s save send drawnPt', (bridge.match(/drawnPt: c\.drawnPt|drawnPt: back\.drawnPt/g) || []).length >= 3);
  ok('2 · the server keeps it (the index, the editor\'s update, the custom save) and gives it back', /drawnOf\(e\)\);/.test(master) && /drawnPt: cleanBody\(e\.drawnPt\) \|\| FV\.delete\(\)/.test(master) && /\.\.\.\(drawn \? \{ drawnPt: drawn \} : \{\}\)/.test(master) && /drawnPt: drawn/.test(save));
  const Master = require(path.join(root, 'netlify/functions/_charmNestMaster.js'));
  const slim = Master.slimEntry({ sku: 'X', widthPt: 28, heightPt: 28, drawnPt: [25.512, 25.512] });
  ok('2 · slimEntry gives drawnPt back', slim && Array.isArray(slim.drawnPt) && slim.drawnPt[0] === 25.512, slim && slim.drawnPt);
  ok('3 · the repository card shows both drawings as drawn (drawnOf), never the frame', /In the repository\$\{sizeWords\(geom\)\}/.test(repo) && /Your drawing\$\{st\.made \? sizeWords\(st\.made\)/.test(repo) && /drawnPt: c\.drawnPt \|\| null/.test(repo));
  ok('3 · the Library card shows the size as drawn', /function drawnSize\(e\)/.test(bridge) && /\(drawnSize\(e\)\[0\] \* MM\)\.toFixed\(1\)/.test(bridge));
  { // 4 · the real worker, as the page runs it: parse, group, silhouettes all in charm-nest-compute-worker.js, the charm crossing as charm-nest-background.js copies it
    const vm = require('vm');   // (the canvas: the one _charmNestPdf.withCanvas() put in place above, the cloud's own, as the worker's OffscreenCanvas)
    const box = vm.createContext({ console: { log() {}, warn() {}, error() {}, info() {} }, Intl, navigator: { userAgent: 'node-worker' }, TextEncoder, TextDecoder, setTimeout, clearTimeout, queueMicrotask, Uint8Array, Uint16Array, Uint32Array, Int32Array, Float32Array, Float64Array, ArrayBuffer, Map, Set, OffscreenCanvas: globalThis.OffscreenCanvas, FileReader: globalThis.FileReader, FileReaderSync: globalThis.FileReaderSync, Blob: globalThis.Blob, btoa: globalThis.btoa, atob: globalThis.atob });
    const waiting = new Map(); let nextId = 0;
    box.self = box; box.postMessage = m => { const w = waiting.get(m.id); if (!w || m.progress) return; waiting.delete(m.id); m.error ? w.reject(new Error(m.error)) : w.resolve(m.result); };
    box.importScripts = (...files) => files.forEach(f => vm.runInContext(fs.readFileSync(path.join(root, f.split('?')[0]), 'utf8'), box, { filename: f }));
    box.importScripts('charm-nest-compute-worker.js');
    const run = (type, input) => new Promise((resolve, reject) => { const id = ++nextId; waiting.set(id, { resolve, reject }); box.onmessage({ data: structuredClone({ id, type, input }) }); }).then(r => structuredClone(r));
    const bg = fs.readFileSync(path.join(root, 'charm-nest-background.js'), 'utf8'), keys = JSON.parse(bg.match(/const charm=c=>Object\.fromEntries\((\[[^\]]+\])/)[1].replace(/'/g, '"'));
    const copy = c => Object.fromEntries(keys.filter(k => c[k] !== undefined).map(k => [k, c[k]]));
    const bytes = discPdf(9 / MM, 1.3 / MM, 0.2, true), pw = await run('parse', { bytes, name: 'editor disc.pdf', key: 'k1' }), gw = await run('group', { parsed: pw, opts: { minPt: 6 } });
    const charms = gw.charms.filter(c => !c.open), back = await run('silhouettes', { charms: charms.map(copy), scale: 6 });
    back.forEach((f, i) => Object.assign(charms[i], f));   // (exactly as charm-nest-background.js's buildSilhouettes takes the worker's answer)
    const cw = charms[0], dw4 = cw && cw.drawnPt ? cw.drawnPt.map(v => +(v * MM).toFixed(2)) : null;
    const drawnOf = g => { const d = Array.isArray(g.drawnPt) && g.drawnPt.length === 2 ? g.drawnPt : null; if (d) return d; const w = +g.widthPt || 0, h = +g.heightPt || 0; return [w > 2 ? w - 2 : w, h > 2 ? h - 2 : h]; };   // (charm-nest-ce-repo.js's)
    const card = cw ? (drawnOf(cw)[0] * MM).toFixed(1) : null;
    ok('4 · TRUESIZE 3: in the real background worker a 9.0 mm circle drawn by the Charm Editor comes back with its size as drawn: ' + JSON.stringify(dw4) + ' mm', charms.length === 1 && !!dw4 && Math.abs(dw4[0] - 9) < 0.02 && Math.abs(dw4[1] - 9) < 0.02, { drawn: dw4, frame: cw && cw.widthPt });
    ok('4 · and the repository card reads it as ' + card + ' mm (never 9.2)', card === '9.0', card);
    ok('4 · the page loads the new worker (its version tags moved)', /charm-nest-background\.js\?v=1117-17148c77-ts3"/.test(fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8')) && /-nat1-ts3'\)/.test(bg));
  }
  console.log(failed ? `FAIL true-size (${failed})` : 'PASS true-size'); process.exit(failed ? 1 : 0);
})().catch(e => { console.log('FAIL true-size: ' + (e && e.stack || e)); process.exit(1); });
