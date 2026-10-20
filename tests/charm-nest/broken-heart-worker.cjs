// BROKENHEART2 (Paul, 20 Oct 2026, order 4204452679): Paul's corrected master SKUs_CORRECTED_Broken_Heart.ai (one heart split into a LEFT and a RIGHT half, two closed outlines sharing the
// zigzag edge, each with its hole, labelled "Best Friends_2505" / "Best_4003") added in the Library tab of a real browser. The Library upload folds the row of two halves into ONE design (charm-nest-pair.js
// pairLayer) and writes its per-SKU file with P.buildSingleCharm(pm.charm, parsed, { bodies: pm.bodies }): each body a form of its own, so the pool can cut one half alone (charm-nest-bridge.js traceSolo).
// In a browser that call goes through the page's background copy (charm-nest-background.js) to the compute worker, and both dropped the third argument: the file drew the whole pair as ONE form, both halves
// came back in one drawing group and every order of the listing was held ("does not give the 2 separate bodies … its bodies are drawn in one group of the library's file").
// Here: the page's background copy over the real compute worker (a node worker thread, no canvas), the real file, the real pair layer; the per-SKU file is read back the way the pool reads it
// (readMasterCharm folds the pieces into the biggest, traceSolo asks that no drawing group holds members of both bodies).
//   node tests/charm-nest/broken-heart-worker.cjs
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm'), { Worker } = require('node:worker_threads');
const root = path.join(__dirname, '../..');
const workers = [];
class Adapter {
  constructor() {
    this.w = new Worker(`
 const {parentPort}=require('node:worker_threads'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
 const c=vm.createContext({console,Intl,navigator:{userAgent:"node-worker"},TextEncoder,TextDecoder,setTimeout,clearTimeout,queueMicrotask,Uint8Array,Uint16Array,Uint32Array,Int32Array,Float32Array,Float64Array,ArrayBuffer,Map,Set,OffscreenCanvas:function(){throw new Error('no canvas in this test');}});
 c.self=c;c.postMessage=data=>parentPort.postMessage(data);c.importScripts=(...files)=>files.forEach(file=>vm.runInContext(fs.readFileSync(path.join(${JSON.stringify(root)},file.split('?')[0]),'utf8'),c,{filename:file}));c.importScripts('charm-nest-compute-worker.js');parentPort.on('message',data=>c.onmessage({data}));
 `, { eval: true });
    workers.push(this); this.w.on('message', data => this.onmessage?.({ data })); this.w.on('error', e => this.onerror?.(e));
  }
  postMessage(d) { this.w.postMessage(d); } terminate() { this.w.terminate(); }
}
global.self = global; global.PDFLib = require(path.join(root, 'vendor/pdf-lib-1.17.1.min.js')); require(path.join(root, 'charm-nest-pdf.js'));
const native = Object.assign({}, global.CharmNestPDF), E = require(path.join(root, 'charm-nest-export.js')), G = require(path.join(root, 'charm-nest-geom.js')), Pair = require(path.join(root, 'charm-nest-pair.js'));
// the page: just what charm-nest-background.js touches (a window with a Worker and an OffscreenCanvas, a document to listen on)
const win = vm.createContext({ Worker: Adapter, OffscreenCanvas: function () {}, CharmNestPDF: Object.assign({}, native), CharmNestExport: Object.assign({}, E), addEventListener() {}, document: { addEventListener() {} }, performance, setTimeout, clearTimeout, console });
win.window = win;
vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-background.js'), 'utf8'), win);
const P = win.CharmNestPDF;
const MM = 25.4 / 72, FILE = path.join(__dirname, 'fixtures', 'SKUs_CORRECTED_Broken_Heart.ai');
let n = 0; const ok = (c, m) => { assert(c, m); n++; console.log('  ✓ ' + m); };

/** The per-SKU file read back as the pool reads it: { bodies, tops: the drawing groups of each body, shared: a group holding members of both } */
async function readBack(bytes) {
  const parsed = await native.parseSource(bytes, 'BEST_FRIENDS_2505.ai'), g = native.groupCharms(parsed, { minPt: 6 });
  const charm = g.charms.reduce((a, b) => (b.bbox[2] - b.bbox[0]) * (b.bbox[3] - b.bbox[1]) > (a.bbox[2] - a.bbox[0]) * (a.bbox[3] - a.bbox[1]) ? b : a);
  for (const c of g.charms) if (c !== charm) for (const m of c.members) if (!charm.members.includes(m)) charm.members.push(m);   // (readMasterCharm)
  const bodies = Pair.bodiesOf(charm), tops = bodies.map(b => new Set(b.members.map(m => (m.parent != null ? m.parent : m.index)).filter(t => t != null)));   // (traceSolo)
  return { charm, bodies, tops, shared: tops.some((t, i) => tops.some((u, j) => j > i && [...t].some(x => u.has(x)))) };
}

(async () => {
  const buf = fs.readFileSync(FILE), bytes = new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
  // the Library upload in a browser: parsed and grouped in the worker, labels and the pair layer on the page
  const parsed = await P.parseSource(bytes, 'SKUs_CORRECTED_Broken_Heart.ai');
  ok(!parsed.doc && parsed.computeKey, 'the master is parsed in the compute worker, as in a browser');
  const g = await P.groupCharmsAsync(parsed, { minPt: 6 });
  const lab = P.labelCharms(parsed, g.charms, { pattern: P.SKU_PATTERN_DEFAULT, gapPt: 6.4 / MM, widen: 0.25 });
  const owner = [...lab.labels].find(([, l]) => /BEST FRIENDS_2505/i.test(l.sku));
  ok(owner && (owner[1].extra || []).some(x => /BEST_4003/i.test(x.sku)), `one label of two SKU lines: ${owner && [owner[1].sku].concat((owner[1].extra || []).map(x => x.sku)).join(' / ')}`);
  const items = [...lab.labels].map(([index, l]) => ({ index, l, c: g.charms.find(c => c.index === index) }));
  const pairs = Pair.pairLayer(P, G, { charms: g.charms }, lab, items, { engraveMarginMm: 0.8 }, () => {});
  const pm = pairs.fold.get(owner[0]);
  ok(pm && Array.isArray(pm.bodies) && pm.bodies.length === 2, 'the row of two halves is folded into ONE design of two bodies');
  // the per-SKU file the browser writes (charm-nest-bridge.js indexing: P.buildSingleCharm(pm.charm, parsed, { bodies: pm.bodies })), through the page's background copy and the worker
  const file = await P.buildSingleCharm(pm.charm, parsed, { bodies: pm.bodies });
  const back = await readBack(file);
  ok(back.bodies.length === 2 && Pair.isMismatched(back.charm), `the file read back gives the left half and the right half (${back.bodies.length} bodies)`);
  ok(!back.shared, `each half is a drawing group of its own, so one half can be cut alone (groups ${back.tops.map(t => [...t].join('+')).join(' | ')})`);
  ok(back.bodies.every(b => b.members.length === 2), `each half keeps its own hole (${back.bodies.map(b => b.members.length).join(', ')} members)`);
  // the same file as the server route and Node write it (charmMaster-background.js, no worker): the page's copy now writes the same drawing groups
  const nodeBack = await readBack(await native.buildSingleCharm(pm.charm, await native.parseSource(bytes, 'm.ai'), { bodies: pm.bodies }));
  ok(!nodeBack.shared && nodeBack.tops.length === back.tops.length, 'the server and Node write the same split');
  // a design that is no pair is written exactly as before (no third argument)
  const one = g.charms.find(c => c.mergedInto == null && c.index !== owner[0] && !(pm.partners || []).includes(c.index)) || pm.charm;
  ok((await P.buildSingleCharm(one, parsed)).length > 100, 'a lone charm is written as before');
  console.log(`broken-heart-worker: ${n} checks passed`);
})().catch(e => { console.error(e); process.exitCode = 1; }).finally(() => workers.forEach(x => x.terminate()));
