// SKU labels through the background worker: the page parses and groups a master in charm-nest-compute-worker.js (here a
// node worker thread, as in background.cjs, without the canvas the thumbnails need), gets its own segments back, and
// labelCharms takes every label off its charm. The worker's copies never matched the page's labels, so from Sep 22 every
// master indexed in the browser kept its SKU labels on its charms and wrote them into the charms' design files.
//   node tests/charm-nest/worker-labels.cjs
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), { Worker } = require('node:worker_threads'), { JSDOM } = require('jsdom');
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
const E = require(path.join(root, 'charm-nest-export.js'));
const dom = new JSDOM('<span></span>', { url: 'https://example.test', runScripts: 'outside-only' }), w = dom.window;
w.Worker = Adapter; w.OffscreenCanvas = function () {}; w.CharmNestPDF = CharmNestPDF; w.CharmNestExport = E;
w.eval(fs.readFileSync(path.join(root, 'charm-nest-background.js'), 'utf8'));
const P = w.CharmNestPDF; const { buildMaster } = require('./fixture-master.cjs');
(async () => {
  const master = await buildMaster(null, { count: 8, edge: true });
  const parsed = await P.parseSource(new Uint8Array(master.bytes), 'master.ai');
  assert(!parsed.doc, 'the master was parsed in the worker');
  const g = await P.groupCharmsAsync(parsed, { minPt: 6 });
  const own = new Set(parsed.segments.concat(parsed.nested));
  for (const c of g.charms) assert(own.has(c.outline) || c.outline.synthetic, 'each outline is the page\'s own segment');
  for (const c of g.charms) for (const m of c.members) assert(own.has(m) || m.synthetic, 'each member is the page\'s own segment');
  assert(Array.isArray(parsed._frames), 'the frames the grouping set aside reach the page');
  const lab = P.labelCharms(parsed, g.charms, { gapPt: 6.4 * 72 / 25.4 });
  assert(lab.labels.size >= 5, 'the labels are read');
  for (const c of g.charms) assert(!c.members.some(m => m.kind === 'text'), 'no label stays on a charm');
  console.log(`Worker labels OK: ${lab.labels.size} labels read and taken off ${g.charms.length} charms grouped in the worker`);
})().catch(e => { console.error(e); process.exitCode = 1; }).finally(() => workers.forEach(x => x.terminate()));
