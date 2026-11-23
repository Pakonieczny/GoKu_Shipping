// REPOLISTING (Paul, 22 Nov 2026: "This type of update must also be the case for all listings/orders updated in the Charm Editor when the charm repo is being updated in the Charm Editor";
// order 4239999491 of listing 234758391 kept INITIAL_8391 after 9MM_DISC_8391 was updated from the listing's other order 4231992262). The bridge's real listingFollow (and editSkuSame,
// savedLink) run in a vm with the page's other parts stood in; the Charm Editor's Update repository calls it. No network, no Etsy.
//   1  updated from order 4231992262's piece: the listing's link is saved (aliasPut: listing 234758391 · INITIAL_8391 is cut as 9MM_DISC_8391) and the listing's OTHER open order, whose pieces
//      are INITIAL_8391, is asked of the cloud to be made again (remakeline); the order already of 9MM_DISC_8391 and another listing's order are not
//   2  "Leave pieces already made as they are": the link is saved (later orders), nothing is made again
//   3  a listing whose Etsy SKU IS the charm: no link is written
//   4  Etsy is never asked (no Etsy op), and a link the cloud did not read back is said, never counted as saved
//   5  the Charm Editor's Update repository calls it for an order's piece, with the order, the piece, the SKU and the keep choice, and says the listing in its answer
//   node tests/charm-nest/repo-listing.cjs
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '../..');
let failed = 0; const ok = (n, c, d) => { console.log((c ? 'OK   ' : 'FAIL ') + n + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 900))); if (!c) failed++; };
const src = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8');
const cut = (from, to) => { const i = src.indexOf(from), j = src.indexOf(to, i); if (i < 0 || j < 0) throw new Error('not found: ' + from); return src.slice(i, j); };
const code = [cut('  const savedLink = r =>', '\n'), cut('  function editSkuSame(off) {', '  window.CNListingFollow = listingFollow;'), '  window.CNListingFollow = listingFollow;'].join('\n');

function page(o) {
  const calls = [], asks = [];
  const line = (rid, tx, lid, etsy, design, pieceSku, extra) => Object.assign({ key: `${rid}_${tx}`, state: 'pooled', order: { receiptId: rid }, line: { listingId: lid, sku: etsy, title: 'Disc necklace' }, spec: { designSku: design }, poolIds: [`${rid}_${tx}_1`, `${rid}_${tx}_2`], _pieceSku: pieceSku }, extra || {});
  const rows = o.rows || [
    line('4231992262', '5290446926', '234758391', 'INITIAL_8391', '9MM_DISC_8391', '9MM_DISC_8391', { skuOverride: { sku: '9MM_DISC_8391' } }),
    line('4239999491', '5301489346', '234758391', 'INITIAL_8391', 'INITIAL_8391', 'INITIAL_8391'),
    line('4250000001', '5310000001', '999999999', 'INITIAL_8391', 'INITIAL_8391', 'INITIAL_8391')];
  const pool = new Map(); for (const r of rows) for (const id of r.poolIds) pool.set(id, { sku: r._pieceSku });
  const w = { console, Promise, Date, JSON, Map, Set, String, Object, Array, setTimeout };
  w.window = w; w.B = { run: null, pool: { rows: pool } };
  w.employeeName = () => 'Paul_K';
  w.Master = { entryFor: s => (/^(9MM_DISC_8391|INITIAL_8391)$/.test(s) ? { sku: s } : null) };
  w.Orders = { rows: () => rows, loadMaps: async () => {}, interpretAll: () => {}, render: () => {} };
  w.syncOrderItems = () => {}; w.saveLinesNow = async () => {}; w.repool = async () => {}; w.ensureBought = async () => true;
  w.editSkuOffer = row => ({ ok: true, lid: String(row.line.listingId), from: String(row.line.sku).toUpperCase(), cur: String(row.spec.designSku).toUpperCase(), viaLink: false });
  w.CharmNestReviewLink = { fromOf: l => String(l.sku || '').toUpperCase() };
  w.CNViewer = { on: () => true };
  w.api = async (fn, body) => { calls.push(body); if (body.op === 'aliasPut') return o.unread ? { ok: true } : { ok: true, verified: true, kind: 'bySku', listingId: body.listingId, key: body.fromSku, sku: body.sku }; return { ok: true }; };
  w.CNRelayAsk = async (cmd, onStep) => { asks.push(cmd); if (onStep) onStep('Making the pieces again…'); return { ok: true, cloud: true, text: `${cmd.ids.length} order line(s) made again: ${cmd.ids.length * 2} piece(s) on GF Sheet 2, being nested` }; };
  vm.runInContext(code, vm.createContext(w));
  return { w, calls, asks };
}

(async () => {
  { const P = page({}), says = [];
    const r = await P.w.CNListingFollow({ order: '4231992262', piece: '4231992262_5290446926_1', sku: '9MM_DISC_8391', by: 'Paul_K', say: t => says.push(t) });
    const put = P.calls.find(c => c.op === 'aliasPut');
    ok('1 · the listing\'s link is saved: listing 234758391 · INITIAL_8391 is cut as 9MM_DISC_8391, in the person\'s name', !!put && put.listingId === '234758391' && put.fromSku === 'INITIAL_8391' && put.sku === '9MM_DISC_8391' && put.by === 'Paul_K', put);
    ok('1 · the listing\'s OTHER open order (pieces INITIAL_8391) is asked of the cloud to be made again; the order already of the charm and another listing\'s order are not', P.asks.length === 1 && P.asks[0].type === 'remakeline' && JSON.stringify(P.asks[0].ids) === JSON.stringify(['4239999491_5301489346']), P.asks);
    ok('1 · it answers with the listing and the cloud\'s words, and said what it did on the way', r.ok && r.lid === '234758391' && r.n === 1 && /made again/.test(r.text) && says.some(t => /234758391/.test(t)), { r, says });
  }
  { const P = page({});
    const r = await P.w.CNListingFollow({ order: '4231992262', piece: '4231992262_5290446926_1', sku: '9MM_DISC_8391', by: 'Paul_K', remake: false });
    ok('2 · "Leave pieces as they are": the link is saved for later orders, nothing is made again', r.ok && P.calls.some(c => c.op === 'aliasPut') && !P.asks.length, { r, asks: P.asks });
  }
  { const rows = [{ key: '4260000001_5320000001', state: 'pooled', order: { receiptId: '4260000001' }, line: { listingId: '111', sku: '9MM_DISC_8391', title: 'x' }, spec: { designSku: '9MM_DISC_8391' }, poolIds: ['p1'], _pieceSku: '9MM_DISC_8391' }];
    const P = page({ rows }); const r = await P.w.CNListingFollow({ order: '4260000001', piece: 'p1', sku: '9MM_DISC_8391', by: 'Paul_K' });
    ok('3 · a listing whose Etsy SKU IS the charm: no link written, nothing to make again', r.ok && !P.calls.some(c => c.op === 'aliasPut') && !P.asks.length, { r, calls: P.calls });
  }
  { const P = page({ unread: true }); const r = await P.w.CNListingFollow({ order: '4231992262', piece: '4231992262_5290446926_1', sku: '9MM_DISC_8391', by: 'Paul_K' });
    ok('4 · a link the cloud did not read back is said, never counted as saved, and nothing is made again', !r.ok && /not saved/.test(r.why) && !P.asks.length, r);
    ok('4 · no Etsy op is ever asked by it', !/etsy/i.test(cut('  async function listingFollow(o) {', '  window.CNListingFollow = listingFollow;').replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/.*$/gm, '').replace(/Etsy is NOT changed/g, '')), 'an Etsy call in listingFollow');
  }
  const repo = fs.readFileSync(path.join(ROOT, 'charm-nest-ce-repo.js'), 'utf8');
  ok('5 · the Charm Editor\'s Update repository calls it for an order\'s piece (order, piece, SKU, the keep choice) and says the listing in its answer',
    /st\.id\.kind === 'order' && st\.id\.order && typeof root\.CNListingFollow === 'function'/.test(repo) && /CNListingFollow\(\{ order: st\.id\.order, piece: st\.id\.piece, sku: st\.id\.sku, by: whoNow\(\), remake: !st\.keep, say \}\)/.test(repo) && /The listing was not changed: \$\{lf\.why\}/.test(repo));
  ok('5 · the page loads both changed files under new tokens', /charm-nest-ce-repo\.js\?v=[^"]*-rl1(?:-\w+)*"/.test(fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8')) && /charm-nest-bridge\.js\?v=[^"]*-rl1(?:-\w+)*"/.test(fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8')));
  console.log(failed ? `FAIL repo-listing (${failed})` : 'PASS repo-listing'); process.exit(failed ? 1 : 0);
})().catch(e => { console.log('FAIL repo-listing: ' + (e && e.stack || e)); process.exit(1); });
