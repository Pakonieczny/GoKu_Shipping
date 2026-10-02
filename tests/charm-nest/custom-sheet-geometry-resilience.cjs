// Run the real saved-sheet source reader and geometry attachment against controlled PDF parse fixtures.
// Custom pool sources must read their own uploaded artwork; a CUSTOM SKU is never a master lookup.
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../../charm-nest-sheetwin.js'), 'utf8');
const between = (start, end) => {
  const a = source.indexOf(start), b = source.indexOf(end, a);
  assert(a >= 0 && b > a, 'production boundaries exist: ' + start);
  return source.slice(a, b);
};
const deferred = () => { let resolve, reject; const promise = new Promise((a, b) => { resolve = a; reject = b; }); return { promise, resolve, reject }; };
const tick = () => new Promise(setImmediate);
const fixture = hash => ({ hash, index: 0, centerPt: [21, 15], widthPt: 42, heightPt: 30,
  outline: { bbox: [0, 0, 42, 30], subpaths: [{ closed: true, ops: [['M', 0, 0], ['L', 42, 0], ['L', 42, 30], ['Z']] }] },
  members: [{ cut: true, subpaths: [{ closed: true, ops: [['M', 18, 21], ['L', 24, 21], ['L', 24, 27], ['Z']] }] }] });

function harness() {
  const h = { calls: [], masterCalls: [], parses: [], groups: [], builds: [], bytes: [], shapes: [fixture('custom-hash')], load: async () => new Uint8Array([37, 80, 68, 70]), failParse: false };
  const context = vm.createContext({
    Map, Promise, Object, String, decodeURIComponent,
    S: { settings: { minPt: 7, silhouetteRes: 8 } },
    Master: {
      entryFor: sku => { h.masterCalls.push(['entry', sku]); return h.entry || null; },
      fetchEntry: async sku => { h.masterCalls.push(['fetch', sku]); return h.entry || null; },
    },
    Pool: {
      masterCharm: async (entry, size) => { h.masterCalls.push(['charm', entry.sku, size]); return { charms: [h.masterShape || fixture('master-hash')] }; },
      cloneCharm: (c, id) => Object.assign({}, c, { id }),
    },
    api: async (name, body) => { h.calls.push([name, body]); return { url: 'https://fixture/' + encodeURIComponent(body.path) }; },
    CharmNestAssets: { bytes: async url => { h.bytes.push(url); return h.load(url); } },
    CharmNestPDF: {
      parseSource: async (bytes, name) => { h.parses.push([bytes, name]); if (h.failParse) throw new Error('PDF read interrupted'); return { marker: 'parsed', bytes }; },
      groupCharmsAsync: async (parsed, opts) => { h.groups.push([parsed, opts]); return { charms: h.shapes }; },
      buildSilhouettes: async (parsed, charms, res) => { h.builds.push([parsed, charms, res]); },
      pathToCanvas: (ctx, outline, transform) => { ctx.paths.push({ outline, transformed: transform(42, 30) }); },
    },
    cutLinesOf: c => c.members,
    engGeoOf: () => null,
    tryDo: fn => fn(),
  });
  vm.runInContext('const fileGeoms = new Map();\n' + between('  async function sourceGeom(s) {', '  function geometryReady(tok)'), context);
  vm.runInContext(between('  function attachGeom(x, g, rec) {', '  /* ── the back of a sheet'), context);
  vm.runInContext(between('  function backPiece(ctx, x, backs, k, o = {}) {', '  /** The engraving font'), context);
  h.context = context;
  h.read = s => context.sourceGeom(s);
  return h;
}
const customSource = extra => Object.assign({ id: 'cust:legacy123', pool: true, sku: 'CUSTOM', path: 'charmnest/custom/4174476673/tag.ai', name: 'tag.ai' }, extra);

async function main() {
  let cases = 0;
  for (const descriptor of [
    customSource({ path: 'saved/legacy-tag.ai' }),
    customSource({ id: 'old-file', path: 'charmnest/custom/4174476673/tag.ai' }),
    customSource({ id: 'old-sandbox-file', path: 'charmnest/sandbox/custom/4174476673/tag.ai' }),
    customSource({ id: 'encoded-file', path: 'charmnest%2Fsandbox%2Fcustom%2F4174476673%2Ftag.ai' }),
    customSource({ id: 'url-file', path: null, url: 'https://fixture/objects/charmnest%2Fcustom%2F4174476673%2Ftag.ai' }),
    customSource({ id: 'url-and-path-file', path: 'legacy/file.ai', url: 'https://fixture/charmnest/custom/4174476673/tag.ai' }),
    customSource({ id: 'explicit-file', custom: true, path: 'legacy/file.ai', sku: 'GECKO' }),
  ]) {
    const h = harness(), geom = await h.read(descriptor);
    assert.equal(h.masterCalls.length, 0, 'custom artwork never queries a master SKU: ' + descriptor.id);
    assert.equal(h.bytes.length, 1); assert.equal(h.parses.length, 1);
    assert.equal(h.parses[0][1], 'tag.ai', 'its uploaded filename reaches the parser');
    assert.equal(h.groups[0][1].minPt, 7); assert.equal(h.builds[0][2], 8);
    assert.equal(geom.pool, false); assert.equal(geom.charms[0].custom, true, 'restored artwork keeps its custom identity');
    assert.strictEqual(geom.charms[0].outline, h.shapes[0].outline, 'the original shape and holes remain shared');
    assert.strictEqual(geom.charms[0].members, h.shapes[0].members);
    assert.equal(geom.charms[0].widthPt, 42); assert.equal(geom.charms[0].heightPt, 30);
    const x = { id: 'saved-piece', sourceId: descriptor.id, index: 0, hash: 'custom-hash', name: '4174476673 · Custom · tag', p: { wPt: 42, hPt: 30, scale: 1 } };
    h.context.attachGeom(x, geom, { metal: 'gold' });
    assert.equal(x.c.id, 'saved-piece'); assert.equal(x.c.name, x.name); assert.equal(x.c.custom, true);
    const ctx = { paths: [], fills: [], strokes: [], beginPath() {}, fill() { this.fills.push(this.fillStyle); }, stroke() { this.strokes.push(this.strokeStyle); }, setLineDash() {} };
    h.context.backPiece(ctx, x, new Map(), 2);
    assert.equal(ctx.fills[0], 'rgba(125,86,168,.20)', 'the actual saved-sheet painter restores the plum custom fill');
    assert(ctx.strokes.includes('rgba(125,86,168,.9)'), 'the custom border is present');
    assert.deepEqual(Array.from(ctx.paths[0].transformed), [42, -30], 'the original geometry scale is preserved');
    cases++;
  }
  {
    const h = harness(); h.masterShape = fixture('master-hash'); h.entry = { sku: 'GECKO', sizes: { Small: { aiPath: 'master/gecko-small.ai' }, Large: { aiPath: 'master/gecko-large.ai' } } };
    const geom = await h.read({ id: 'master:gecko', pool: true, sku: 'GECKO', path: 'master/gecko-large.ai' });
    assert.equal(geom.pool, true); assert.strictEqual(geom.base.outline, h.masterShape.outline);
    assert.equal(geom.base.custom, undefined); assert.equal(h.bytes.length, 0); assert.equal(h.parses.length, 0);
    assert.deepEqual(h.masterCalls, [['entry', 'GECKO'], ['charm', 'GECKO', 'Large']], 'ordinary master lookup and exact saved size remain unchanged');
    const x = { id: 'master-piece', name: 'GECKO', rid: '4176277010', poolId: '4176277010_line_1' };
    h.context.attachGeom(x, geom, { metal: 'silver' }); assert.equal(x.c.id, x.id); assert.equal(x.c.order, x.rid); assert.equal(x.c.metal, 'silver');
    await assert.rejects(harness().read({ id: 'master:missing', pool: true, sku: 'MISSING', path: 'master/missing.ai' }), /no longer in the master library/);
    cases++;
  }
  {
    const h = harness(), gate = deferred(); h.load = () => gate.promise;
    const a = h.read(customSource()), b = h.read(customSource({ id: 'cust:another-copy' })); await tick();
    assert.equal(h.bytes.length, 1, 'concurrent copies share their uploaded source read');
    gate.resolve(new Uint8Array([37, 80, 68, 70])); const [ga, gb] = await Promise.all([a, b]);
    assert.equal(h.parses.length, 1); assert.equal(h.builds.length, 1);
    assert.strictEqual(ga.charms[0].outline, gb.charms[0].outline, 'copies reuse the exact source geometry');
    await h.read(customSource()); assert.equal(h.bytes.length, 1, 'warm views reuse the parsed file');
    cases++;
  }
  {
    const h = harness(); h.failParse = true;
    await assert.rejects(h.read(customSource()), /interrupted/); assert.equal(h.bytes.length, 1);
    h.failParse = false; const geom = await h.read(customSource());
    assert.equal(h.bytes.length, 2); assert.equal(geom.charms[0].custom, true, 'a failed parse is evicted and a reopened view retries');
    cases++;
  }
  {
    const h = harness(), path = 'shared/unclassified.ai';
    const regular = await h.read({ id: 'file:regular', path, name: 'shared.ai' });
    const custom = await h.read({ id: 'cust:same-file', path, name: 'shared.ai' });
    const regularAgain = await h.read({ id: 'file:regular', path, name: 'shared.ai' });
    assert.equal(custom.charms[0].custom, true); assert.equal(regular.charms[0].custom, undefined); assert.equal(regularAgain.charms[0].custom, undefined);
    assert.equal(h.shapes[0].custom, undefined, 'the shared PDF result is not mutated by descriptor identity');
    assert.equal(h.bytes.length, 1); assert.strictEqual(regular.charms[0].outline, custom.charms[0].outline);
    cases++;
  }
  console.log(`PASS: custom saved-sheet geometry — ${cases} cases: legacy/custom/sandbox/encoded descriptors, real attachment and plum painting, unchanged master sizing, shared reads, retry and cache identity`);
}
main().catch(e => { console.error(e); process.exitCode = 1; });
