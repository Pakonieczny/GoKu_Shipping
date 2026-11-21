// NESTBODY (Paul, 21 Nov 2026: since the cloud took every run, every cloud nest was refused: "N placed piece(s) could not be made again in the cloud (its design has no such body)").
// A piece a computer made says no body, and node's drawing of a design differs from Chrome's by an edge pixel, so its hash never matches the cloud's. charm-nest-pool-make.js soloBase, in the
// cloud: a design of ONE body is the design whole (nothing to tell); a design of several bodies is told by the piece's own size (within 3 %, the whole design a candidate too); two that fit, or
// no size, is still refused in words. The page's rule (hash against the page's own bodies) is unchanged.
//   NODE_PATH=<deps> node tests/charm-nest/cloud-nest-body.cjs
'use strict';
const path = require('path');
const PoolMake = require(path.join(__dirname, '../../charm-nest-pool-make.js'));
let failed = 0; const ok = (n, c, d) => { console.log((c ? 'OK   ' : 'FAIL ') + n + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d))); if (!c) failed++; };
const make = cloud => PoolMake.create({ window: {}, S: { settings: {} }, B: { pool: { rows: new Map(), sources: new Map() }, master: { entries: new Map() } }, O: {}, P: null, MM: 25.4 / 72, Master: {}, PoolPieces: { cleanFields: () => ({}), fieldsOf: () => ({}) },
  agent: () => {}, api: async () => ({}), stockFor: () => null, labelOf: m => m, now: () => Date.now(), CharmNestAssets: () => ({}), Orders: () => null, Review: () => null, CustomPrint: () => null, CustomSheet: () => null, ArtEdit: () => null,
  keptMaster: async () => null, keepMaster: () => {}, pinPooled: () => {}, charmOf: () => null, greenHeld: () => false, cancelled: () => false, cloud, pace: async () => {} });
const whole = { hash: 'cloudwhole', widthPt: 40, heightPt: 30 };
const src = solo => ({ sku: 'TEST_1', charms: [whole], soloChecked: true, solo });
(async () => {
  const M = make(true);
  ok('0 · the maker exposes soloBase', typeof M.soloBase === 'function');
  const rec = { poolId: '4240569715_5303191633_1', hash: 'pagehash1', widthPt: 40.2, heightPt: 29.9 };
  ok('1 · a ONE-body design (no solo bodies): a page-made piece whose hash differs is the design whole (no refusal)', (await M.soloBase(src(null), rec)) === null);
  ok('1b · ... one solo body only: the design whole too', (await M.soloBase(src([{ hash: 'b0', widthPt: 40, heightPt: 30 }]), rec)) === null);
  const b0 = { hash: 'b0', widthPt: 18, heightPt: 30 }, b1 = { hash: 'b1', widthPt: 20, heightPt: 22 };
  ok('2 · several bodies: the piece\'s own size names its body', (await M.soloBase(src([b0, b1]), { poolId: 'x', hash: 'p', widthPt: 20.3, heightPt: 21.9 })) === b1);
  ok('2b · several bodies, the piece is the whole design by its size: whole', (await M.soloBase(src([b0, b1]), { poolId: 'x', hash: 'p', widthPt: 40, heightPt: 30 })) === null);
  let threw = null; try { await M.soloBase(src([b0, { hash: 'b2', widthPt: 18.1, heightPt: 30 }]), { poolId: 'x', hash: 'p', widthPt: 18, heightPt: 30 }); } catch (e) { threw = e; }
  ok('3 · two bodies fit the same size: still refused in words (a person checks it)', !!threw && threw.bodyUnknown === true && /a person checks it/.test(threw.message), threw && threw.message);
  threw = null; try { await M.soloBase(src([b0, b1]), { poolId: 'x', hash: 'p' }); } catch (e) { threw = e; }
  ok('3b · several bodies and no size on the record: refused in words', !!threw && threw.bodyUnknown === true);
  ok('4 · a record that SAYS its body is read as before (oneBody + bodyIndex)', (await M.soloBase(src([b0, b1]), { oneBody: true, bodyIndex: 0 })) === b0);
  ok('4b · the same hash as the cloud\'s whole charm: the design whole', (await M.soloBase(src([b0, b1]), { hash: 'cloudwhole' })) === null);
  const Page = make(false);
  ok('5 · on a page the hash rule is unchanged (matched against the page\'s own bodies)', (await Page.soloBase(src([b0, b1]), { hash: 'b1' })) === b1);
  console.log(failed ? `FAIL cloud-nest-body (${failed})` : 'PASS cloud-nest-body'); process.exit(failed ? 1 : 0);
})().catch(e => { console.log('FAIL cloud-nest-body: ' + (e && e.stack || e)); process.exit(1); });
