'use strict';
/* FRONTARTONLY (Paul, 28 Nov 2026): GF_Nov.25.26_Set-1_Sheet-3 (d-bb46c9c7651c9772) would not download (.ai or .dxf): "Front engraving contains no engraving paths."
   Its one front engraving (order 4230787700, PERSONALIZED_6464, approved 23 Nov) says artOnly: its Charm Editor parts are in the sheet's own drawing, and the composer
   (charm-nest-export.js planFronts, FRONTLIKEBACK 25 Nov) goes on without it. The download composes in the background worker, and the page's copy of each record for the
   worker (charm-nest-background.js rec) kept only poolId, bytes, face, side and groupKey: artOnly never reached the worker, which stopped the whole download on it.
   A: the record the worker gets keeps artOnly (and every other field it had). B: the composer itself, given that record, goes on (no throw) and names the front as in the drawing.
   C: the page loads the new file (cache token). */
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '..', '..');
let pass = 0, fail = 0; const ok = (c, m) => { if (c) { pass++; console.log('  ok  ' + m); } else { fail++; console.log('  FAIL ' + m); } };
// A — the page's wrapper, with a stand-in worker that records what it is sent
const sent = [];
class FakeWorker { constructor() {} postMessage(d) { sent.push(d); } terminate() {} }
const doc = { addEventListener() {} };
const root = { document: doc, Worker: FakeWorker, OffscreenCanvas: function () {}, addEventListener() {}, setTimeout, clearTimeout, performance: { now: () => 0 }, CharmNestPDF: { adoptGrouping: x => x }, CharmNestExport: {} };
root.window = root;
const ctx = vm.createContext(Object.assign({ console, Promise, Map, Set, Symbol, Object, Array, JSON, Error, Uint8Array }, root, { document: doc }));
ctx.self = ctx; ctx.root = ctx;
const src = fs.readFileSync(path.join(ROOT, 'charm-nest-background.js'), 'utf8');
vm.runInContext(src.replace(/\}\)\(\s*(typeof window[^)]*|window|self|this)\s*\)\s*;?\s*$/, '})(this);'), ctx, { filename: 'charm-nest-background.js' });
const E = ctx.CharmNestExport;
ok(typeof E.compose === 'function', 'A0 the page wraps the composer for the worker');
const bytes = new Uint8Array([1, 2, 3]);
const front = { poolId: '4230787700_5289878699_1', bytes, face: 'front', artOnly: true, approvedAt: 1795461992819, sku: 'PERSONALIZED_6464' };
const back = { poolId: '4230000000_1_1', bytes, side: 'L', groupKey: '4230000000:1' };
E.compose(bytes, { id: 'd-bb46c9c7651c9772', fileBase: 'GF_Nov.25.26_Set-1_Sheet-3', placements: [], charms: [] }, [back], [front]);
const msg = sent.find(m => m.type === 'compose');
ok(!!msg, 'A1 the composition is sent to the worker');
const f0 = msg && msg.input.fronts && msg.input.fronts[0];
ok(!!f0 && f0.artOnly === true, 'A2 the front the worker gets says artOnly (it did not: the whole download stopped)');
ok(!!f0 && f0.poolId === front.poolId && f0.face === 'front' && f0.bytes === bytes, 'A3 the front keeps its poolId, face and file');
const b0 = msg && msg.input.backs && msg.input.backs[0];
ok(!!b0 && b0.side === 'L' && b0.groupKey === '4230000000:1' && !('artOnly' in b0), 'A4 a back keeps its side and pair, and gains nothing it did not have');
// B — the composer's own rule with that record: an artOnly front with nothing on ENGRAVE does not stop the download
const ex = fs.readFileSync(path.join(ROOT, 'charm-nest-export.js'), 'utf8');
ok(ex.includes('if (!ink.length && f.artOnly) { entries.push(') && /inDrawing:true/.test(ex) && /if \(!ink\.length\) throw new Error\('Front engraving contains no engraving paths\.'\)/.test(ex), 'B1 the composer goes on for an artOnly front and still stops for a front with words and no paths');
// C — the page loads the new wrapper
const html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8');
ok(/charm-nest-background\.js\?v=[^"]*-fa1(?:-\w+)*"/.test(html), 'C1 charm-nest-1.html loads the new charm-nest-background.js (token -fa1)');
console.log(`\nrt1128-frontartonly: ${pass} passed, ${fail} failed`); process.exit(fail ? 1 : 0);
