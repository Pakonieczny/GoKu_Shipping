// Adversarial: a sheet whose laser file cannot be made (ADV-LASER, Paul, 15 Oct 2026: "a sheet that fails export must not mark the set cut").
// The real charm-nest-export-ui.js on a stand-in page: a set of two sheets is downloaded as .dxf, the second sheet's conversion throws (and, in a
// second pass, the connection drops while the sheets are read). Nothing is downloaded, no completion is ever sent to the cloud (only reads),
// the busy flag is let go, and the next download works.
//   node tests/charm-nest/adv-laser-export.cjs
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const src = fs.readFileSync(path.join(__dirname, '../../charm-nest-export-ui.js'), 'utf8');
(async () => {
  const ops = [], downloads = [], toasts = [];
  let failDxfOn = 's2', offline = false;
  const buttons = [];   // (the page's export buttons: only their disabled flag is read here)
  const document = { readyState: 'complete', getElementById: () => null, querySelectorAll: () => buttons, addEventListener() {} };
  const ctx = vm.createContext({ console, document, Uint8Array, TextEncoder, JSON, Promise, Set, Error, Array, Object, Date, requestAnimationFrame: fn => fn(), MutationObserver: class { observe() {} } });
  ctx.window = ctx;
  ctx.CN = {
    api: async (name, body) => { ops.push(body.op); if (offline) throw new Error('Failed to fetch'); return { sheet: { id: body.id, fileBase: 'Set-1_' + body.id, outputs: { ai: { url: 'u/' + body.id } }, backPool: [], placements: [], charms: [], updatedAt: 1 } }; },
    toast: (t, kind) => toasts.push([t, kind]), download: (data, name) => downloads.push(name)
  };
  ctx.CharmNestBacks = { forSheet: () => [] };
  ctx.CharmNestAssets = { bytes: async () => new Uint8Array([1]) };
  ctx.CharmNestExport = { compose: async (bytes, sheet) => ({ ai: bytes, layout: [], sheetId: sheet.id }), FRONT_LAYER: /^FRONT/, productionDxf: async () => { if (failDxfOn) throw new Error('DXF could not be built'); return { text: '0\nEOF\n', layers: [], colors: [], entityCount: 0 }; } };
  ctx.JSZip = function () { this.file = () => {}; this.generateAsync = async () => new Uint8Array([2]); };
  vm.runInContext(src, ctx);
  const X = ctx.ProductionExports;
  assert(X && typeof X.run === 'function', 'the export module loaded');
  // one sheet's file cannot be made: the whole download stops, nothing is saved, nothing is marked
  await X.run(['s1', 's2'], 'dxf');
  assert.equal(downloads.length, 0, 'no partial download');
  assert(toasts.some(([t, k]) => /Export stopped: DXF could not be built/.test(t) && k === 'bad'), 'the person is told why: ' + JSON.stringify(toasts));
  assert(ops.length > 0 && ops.every(o => o === 'getSheet'), 'only reads reached the cloud, never a completion: ' + ops.join(','));
  // the connection drops while the sheets are read; the module is not stuck busy from the failure before
  offline = true; const before = ops.length; await X.run(['s1', 's2'], 'dxf');
  assert.equal(downloads.length, 0); assert(toasts.some(([t]) => /Export stopped: Failed to fetch/.test(t)), 'the dropped connection is said'); assert(ops.length > before && ops.slice(before).every(o => o === 'getSheet'));
  // back online and the file can be made: the same download now works (one zip for the two sheets), and still only reads were sent
  offline = false; failDxfOn = null; await X.run(['s1', 's2'], 'dxf');
  assert.deepEqual(downloads, ['charm-sheets-dxf.zip']); assert(ops.every(o => o === 'getSheet'));
  console.log('Laser export OK: a failed or offline .dxf downloads nothing, marks nothing, is not left busy, and works again afterwards');
})().catch(e => { console.error(e); process.exitCode = 1; });
