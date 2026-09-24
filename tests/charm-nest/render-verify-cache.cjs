// Browser test of the render check's kept layers (CharmNestPDF.verifyRendered with layerSigs and a cache, as the sorter
// runs it after every update): a layer is drawn again only when its charm is new, moved, turned or changed, and what the
// check finds is the same as a check that draws every layer, charm by charm, as a sheet grows, loses a charm (the layer
// names shift), gains one that overlaps another, and changes size.
//   node tests/charm-nest/render-verify-cache.cjs [playwright-core dir]
const path = require('path'), assert = require('assert'), fs = require('fs'), http = require('http');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));

const page = `<!doctype html><meta charset="utf-8"><body>
<script src="/vendor/pdf-lib-1.17.1.min.js"></script>
<script src="/charm-nest-pdf.js"></script>
<script type="module">
import * as pdfjs from '/vendor/pdfjs-4.10.38/pdf.min.mjs';
pdfjs.GlobalWorkerOptions.workerSrc = new URL('/vendor/pdfjs-4.10.38/pdf.worker.min.mjs', location.href).href;
window.pdfjsLib = pdfjs;
const L = PDFLib;
// a master with one charm drawn on its own Illustrator-style layer, as the per-SKU files are
async function vector(name, w, h, ops) { const d = await L.PDFDocument.create(), p = d.addPage([w, h]); p.node.normalize(); const ref = d.context.register(d.context.obj({ Type: 'OCG', Name: L.PDFString.of(name) })); p.node.Resources().set(L.PDFName.of('Properties'), d.context.obj({ art: ref })); p.node.addContentStream(d.context.register(d.context.flateStream('/OC /art BDC ' + ops + ' EMC'))); d.catalog.set(L.PDFName.of('OCProperties'), d.context.obj({ OCGs: [ref], D: { Order: [ref], ON: [ref] } })); return d.save({ useObjectStreams: false }); }
const sources = new Map(), charms = {};
async function master(id, w, h, ops) { const parsed = await CharmNestPDF.parseSource(await vector('Artwork', w, h, ops), id); sources.set(id, parsed); charms[id] = { id, sourceId: id, name: 'Heart', bbox: [0, 0, w, h], centerPt: [w / 2, h / 2], strokePt: .5, topIndices: parsed.segments.map((_, i) => i), members: parsed.segments }; }
await master('a', 30, 24, '1 0 0 RG 0.5 w 2 2 m 28 2 l 28 22 l 2 22 l h S');
await master('b', 20, 20, '1 0 0 RG 0.5 w 10 1 m 19 10 l 10 19 l 1 10 l h S');
await master('c', 16, 30, '1 0 0 RG 0.5 w 1 1 m 15 1 l 8 29 l h S');
const sheet = (list, wPt = 300, hPt = 200) => ({ sheet: { wPt, hPt, strokeRGB: [1, 0, 0], strokePt: .5 }, placements: list.map(([k, cxPt, cyPt, angle]) => ({ charm: charms[k], cxPt, cyPt, angle, scale: .975 })), sources, title: 'test' });
const spec = (wPt, hPt, extra) => Object.assign({ wPt, hPt, insetPt: 1.5, res: 6, erodePt: .5 }, extra || {});
const brief = r => ({ ok: r.ok, overlapPx: r.overlapPx, outsidePx: r.outsidePx, empty: r.emptyLayers, pairs: r.overlappingPairs.slice().sort(), detail: r.overlapDetail, layers: r.layers });
window.check = async (list, cache, size) => {
  const s = sheet(list, ...(size || [])), bytes = await CharmNestPDF.buildSheet(s);
  const layerSigs = new Map(s.placements.map(p => [p.layerName, p.layerSig]));
  const kept = await CharmNestPDF.verifyRendered(bytes, spec(s.sheet.wPt, s.sheet.hPt, { layerSigs, cache }));
  const fresh = new Map(), full = await CharmNestPDF.verifyRendered(bytes, spec(s.sheet.wPt, s.sheet.hPt, { layerSigs, cache: fresh }));
  const plain = await CharmNestPDF.verifyRendered(bytes, spec(s.sheet.wPt, s.sheet.hPt));
  // every kept layer area is exactly what drawing the layer again finds
  let sameAreas = fresh.size === cache.size;
  for (const [k, a] of fresh) { const b = cache.get(k); if (!b || !!a.empty !== !!b.empty || !a.empty && (a.X0 !== b.X0 || a.Y0 !== b.Y0 || a.bw !== b.bw || a.bh !== b.bh || a.core.some((v, i) => v !== b.core[i]))) sameAreas = false; }
  return { kept: brief(kept), full: brief(full), plain: brief(plain), rendered: kept.rendered, freshRendered: full.rendered, cacheSize: cache.size, sameAreas, names: s.placements.map(p => p.layerName) };
};
window.ready = true;
</script>`;

(async () => {
  const srv = http.createServer((req, rsp) => {
    const u = decodeURIComponent(req.url.split('?')[0]);
    if (u === '/' || u === '/test.html') { rsp.writeHead(200, { 'Content-Type': 'text/html' }); return rsp.end(page); }
    const f = path.join(root, u);
    if (!f.startsWith(root) || !fs.existsSync(f)) { rsp.writeHead(404); return rsp.end(); }
    rsp.writeHead(200, { 'Content-Type': /\.m?js$/.test(f) ? 'text/javascript' : 'application/octet-stream' }); rsp.end(fs.readFileSync(f));
  });
  await new Promise(r => srv.listen(0, '127.0.0.1', r));
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const pg = await (await browser.newContext()).newPage();
    const errors = []; pg.on('pageerror', e => errors.push(e.message));
    await pg.goto(`http://127.0.0.1:${srv.address().port}/test.html`);
    await pg.waitForFunction(() => window.ready === true, null, { timeout: 30000 });
    const run = (list, size) => pg.evaluate(([list, size]) => window.check(list, window.cache || (window.cache = new Map()), size), [list, size]);
    const same = (r, label) => {
      assert.deepStrictEqual(r.kept, r.full, `${label}: the check with kept layers finds what drawing every layer finds`);
      assert.deepStrictEqual(r.kept, r.plain, `${label}: and what the check without fingerprints finds`);
      assert(r.sameAreas, `${label}: every kept layer area equals the layer drawn again`);
    };

    // 1 · a new sheet: every layer is drawn
    const four = [['a', 40, 40, 0], ['b', 100, 40, 30], ['c', 160, 50, 90], ['a', 220, 60, 144]];
    let r = await run(four);
    same(r, 'four charms'); assert.strictEqual(r.kept.ok, true); assert.strictEqual(r.rendered, 4, 'all four drawn'); assert.strictEqual(r.freshRendered, 4);

    // 2 · two more charms: only they are drawn
    const six = four.concat([['b', 40, 120, 0], ['c', 100, 130, 12]]);
    r = await run(six);
    same(r, 'six charms'); assert.strictEqual(r.kept.ok, true); assert.strictEqual(r.rendered, 2, 'only the two new charms drawn: ' + r.rendered); assert.strictEqual(r.cacheSize, 6);

    // 3 · a cancelled charm comes off: the rest keep their places, their layer names shift ("Heart-3" becomes "Heart-2"),
    //     nothing is drawn again and the cache holds only the charms on the sheet
    const five = six.filter((_, i) => i !== 1);
    r = await run(five);
    same(r, 'one removed'); assert.strictEqual(r.kept.ok, true); assert.strictEqual(r.rendered, 0, 'nothing drawn again: ' + r.rendered); assert.strictEqual(r.cacheSize, 5, 'the removed charm left the cache');
    assert.deepStrictEqual(r.names, ['Heart', 'Heart-2', 'Heart-3', 'Heart-4', 'Heart-5']);

    // 4 · a charm placed over another: found, the pair named, the same as drawing every layer
    const clash = five.concat([['a', 44, 44, 0]]);
    r = await run(clash);
    same(r, 'overlap'); assert.strictEqual(r.kept.ok, false, 'the overlap is found'); assert(r.kept.overlapPx > 0); assert.deepStrictEqual(r.kept.pairs, ['Heart ↔ Heart-6']); assert.strictEqual(r.rendered, 1);

    // 5 · a charm moved or turned is drawn again; a charm reaching into the sheet's edge band is found outside
    const moved = five.map((p, i) => i === 2 ? ['c', 165, 55, 90] : i === 3 ? ['a', 220, 60, 146] : p).concat([['a', 286.8, 100, 0]]);
    r = await run(moved);
    same(r, 'moved'); assert.strictEqual(r.rendered, 3, 'the moved, the turned and the new charm drawn: ' + r.rendered); assert(r.kept.outsidePx > 0, 'the charm past the edge is found'); assert.strictEqual(r.kept.ok, false);

    // 6 · another sheet size: every layer is drawn again
    r = await run(five, [320, 200]);
    same(r, 'new size'); assert.strictEqual(r.rendered, 5, 'all drawn again at the new size'); assert.strictEqual(r.kept.ok, true);

    assert.deepStrictEqual(errors, [], 'no page errors');
    console.log('render-verify-cache: ok');
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
