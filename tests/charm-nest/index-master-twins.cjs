// A SKU the sheet labels under two charms ("twins") is kept on ONE of them, the same one on every run (scripts/index-master.cjs):
// the first charm of the page unless a --pin file names the other. The twin that repeats its label text on two lines used to keep the
// SKU as well (labelCharms' drop removes one line only), so both charms were built under one file name and the library got whichever
// finished last. Offline: the stage mode touches no server.
//   node tests/charm-nest/index-master-twins.cjs
const fs = require('fs'), path = require('path'), assert = require('assert'), os = require('os');
const { buildMaster } = require('./fixture-master.cjs');
const { main, fileKeys } = require('../../scripts/index-master.cjs');

(async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cn-twins-'));
  const file = path.join(tmp, 'BRITES-twins.ai');
  const fx = await buildMaster(file, { count: 8, edge: false, twins: true });
  const log = l => { if (process.env.CN_VERBOSE) console.log(l); };
  const stage = async (name, extra = []) => {
    const dir = path.join(tmp, name);
    const rep = await main(['node', 'x', file, '--out-dir', dir, '--concurrency', '4'].concat(extra), log);
    return { rep, rec: JSON.parse(fs.readFileSync(path.join(dir, 'records.json'), 'utf8')), dir };
  };
  const rows = (rec, sku) => rec.entries.filter(e => e.sku === sku);

  // 1. default: charm 1 (the first on the page) keeps BR-TST-01, charm 7 keeps only its other SKU, the SKU is in the records ONCE
  const a = await stage('a');
  assert.strictEqual(rows(a.rec, 'BR-TST-01').length, 1, 'the twin SKU is recorded once: ' + rows(a.rec, 'BR-TST-01').length);
  assert.strictEqual(rows(a.rec, 'BR-TST-01')[0].aiPath, 'charmnest/master/BR-TST-01.ai');
  assert.strictEqual(rows(a.rec, 'BR-TWN-07').length, 1, 'the second twin keeps its other SKU');
  assert.strictEqual(rows(a.rec, 'BR-TWN-07')[0].aiPath, 'charmnest/master/BR-TWN-07.ai', 'and builds it under its own file name');
  assert(a.rec.entries.every(e => e.sku !== 'BR-TST-07'), 'charm 7 carries no SKU of its own in this master');
  assert.strictEqual(a.rep.twins.length, 1, 'the report names the twin'); assert.strictEqual(a.rep.twins[0].rule, 'first on the page');
  const wA = rows(a.rec, 'BR-TST-01')[0].widthPt;
  // 2. the same on every run, in the same order
  for (let k = 0; k < 3; k++) { const b = await stage('b' + k); assert.deepStrictEqual(b.rec.entries, a.rec.entries, 'run ' + k + ' writes the same records in the same order'); }
  // 3. a pin hands the SKU to the other twin: its file, its size; the first charm (its only label gone) is not written
  const c7 = fx.charms[6];
  fs.writeFileSync(path.join(tmp, 'pin.json'), JSON.stringify({ 'BR-TST-01': { at: [c7.cx, c7.cy] } }));
  const p = await stage('p', ['--pin', path.join(tmp, 'pin.json')]);
  assert.strictEqual(rows(p.rec, 'BR-TST-01').length, 1, 'pinned: still recorded once');
  const wP = rows(p.rec, 'BR-TST-01')[0].widthPt;
  assert(Math.abs(wP - wA) > 1, 'pinned: the record is the other charm\'s (' + wA.toFixed(1) + ' pt vs ' + wP.toFixed(1) + ' pt)');
  assert.strictEqual(p.rep.twins[0].rule, 'pinned');
  assert.strictEqual(rows(p.rec, 'BR-TWN-07')[0].aiPath, rows(p.rec, 'BR-TST-01')[0].aiPath, 'pinned: the owner charm carries both SKUs on one file');
  assert.strictEqual(p.rec.entries.length, a.rec.entries.length, 'same number of records');
  // 4. a pin by index does the same; a pin that names no claimant stops the run
  fs.writeFileSync(path.join(tmp, 'pin2.json'), JSON.stringify({ pins: { 'br-tst-01': p.rep.twins[0].owner } }));
  const q = await stage('q', ['--pin', path.join(tmp, 'pin2.json')]);
  assert.strictEqual(rows(q.rec, 'BR-TST-01')[0].widthPt, wP, 'a pin by charm index');
  fs.writeFileSync(path.join(tmp, 'pin3.json'), JSON.stringify({ 'BR-TST-01': { at: [1, 1] } }));
  await assert.rejects(() => stage('r', ['--pin', path.join(tmp, 'pin3.json')]), /no charm carrying it/, 'a pin that points at nothing is an error, not a guess');
  // 5. "elsewhere": another master owns the SKU, so no charm of this sheet carries it; the charm's other SKU stays
  fs.writeFileSync(path.join(tmp, 'pin4.json'), JSON.stringify({ 'BR-TST-01': 'elsewhere', 'BR-TST-02': 'elsewhere' }));
  const e = await stage('e', ['--pin', path.join(tmp, 'pin4.json')]);
  assert.strictEqual(rows(e.rec, 'BR-TST-01').length, 0, 'elsewhere: the twin SKU is in no record of this sheet');
  assert.strictEqual(rows(e.rec, 'BR-TST-02').length, 0, 'elsewhere: and a SKU with a single charm is left to the other master too');
  assert.strictEqual(rows(e.rec, 'BR-TWN-07').length, 1, 'elsewhere: the second twin keeps its other SKU');
  assert(e.rep.twins.some(t => t.rule === 'elsewhere' && t.owner === null));
  // 6. two SKUs the server would store under one file name get two names (the first charm keeps the plain one)
  const ks = fileKeys([{ index: 864, l: { sku: 'BOWLING_PIN+BALL' } }, { index: 2227, l: { sku: 'BOWLING PIN + BALL' } }, { index: 5, l: { sku: 'RING', size: 'S' } }, { index: 6, l: { sku: 'RING', size: 'M' } }]);
  assert.strictEqual(ks.get(864), 'BOWLING_PIN+BALL'); assert.strictEqual(ks.get(2227), 'BOWLING PIN + BALL__2227'); assert.strictEqual(ks.get(5), 'RING__S'); assert.strictEqual(ks.get(6), 'RING__M');
  // 7. a pin may name the charm whose line labelCharms dropped (the SKU is written ONCE under it, and the other charm was first on the page):
  //    that charm gets the line back and becomes the owner, the SKU is not lost
  const file2 = path.join(tmp, 'BRITES-twins-single.ai');
  const fx2 = await buildMaster(file2, { count: 8, edge: false, twins: 'single' });
  const stage2 = async (name, extra = []) => { const dir = path.join(tmp, name); const rep = await main(['node', 'x', file2, '--out-dir', dir, '--concurrency', '4'].concat(extra), log); return { rep, rec: JSON.parse(fs.readFileSync(path.join(dir, 'records.json'), 'utf8')) }; };
  const d0 = await stage2('s0');
  assert.strictEqual(rows(d0.rec, 'BR-TST-01').length, 1, 'single: default keeps the SKU once');
  const c7s = fx2.charms[6];
  fs.writeFileSync(path.join(tmp, 'pin5.json'), JSON.stringify({ 'BR-TST-01': { at: [c7s.cx, c7s.cy] } }));
  const d1 = await stage2('s1', ['--pin', path.join(tmp, 'pin5.json')]);
  assert.strictEqual(rows(d1.rec, 'BR-TST-01').length, 1, 'single: pinned to the charm that lost the line, the SKU is still recorded once (not lost)');
  assert(Math.abs(rows(d1.rec, 'BR-TST-01')[0].widthPt - rows(d0.rec, 'BR-TST-01')[0].widthPt) > 1, 'single: and it is that charm\'s record');
  assert.strictEqual(rows(d1.rec, 'BR-TWN-07')[0].aiPath, rows(d1.rec, 'BR-TST-01')[0].aiPath, 'single: both SKUs on the owner\'s file');
  assert.strictEqual(d1.rep.twins[0].rule, 'pinned'); assert.strictEqual(d1.rec.entries.length, d0.rec.entries.length, 'single: same number of records');
  console.log('index-master twins OK · default owner, determinism, pin by position and by index (also to a charm that lost the line), elsewhere, file names');
})().catch(e => { console.error(e); process.exit(1); });
