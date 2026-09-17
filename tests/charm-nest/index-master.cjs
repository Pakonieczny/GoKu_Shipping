// The local indexer (scripts/index-master.cjs) against the stand-in server: a dry run reports without writing, a real
// run writes the per-SKU files, the index and the file record exactly as the Master tab and the server route do, and
// a resumed run skips what was already written.
//   node tests/charm-nest/index-master.cjs
const fs = require('fs'), path = require('path'), assert = require('assert'), os = require('os');
const { start } = require('./bridge-server.cjs');
const { buildMaster } = require('./fixture-master.cjs');
const { main } = require('../../scripts/index-master.cjs');

(async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cn-index-'));
  const file = path.join(tmp, 'BRITES-master.ai');
  const fx = await buildMaster(file, { count: 8, edge: true });
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin } = srv;
  const lines = []; const log = l => { lines.push(String(l)); if (process.env.CN_VERBOSE) console.log(l); };
  // dry run: nothing written
  const dry = await main(['node', 'x', file, '--dry'], log);
  assert(dry.dry && dry.charms === 8 && dry.labelled === 6 && dry.written === 0, 'dry run reports without writing: ' + JSON.stringify({ charms: dry.charms, labelled: dry.labelled }));
  assert(st.list('Charm_Master_Index').length === 0 && st.blobs.size === 0, 'a dry run writes nothing');
  // real run
  const rep = await main(['node', 'x', file, '--origin', sorterOrigin, '--upload-master'], log);
  console.log(lines.filter(l => !l.startsWith('  ')).join('\n'));
  assert.strictEqual(rep.written, 7, 'seven SKU lines written: six charms, one of them under two lines');
  const ix = st.list('Charm_Master_Index');
  const fileOf = e => e.aiPath || (e.sizes && Object.values(e.sizes)[0] && Object.values(e.sizes)[0].aiPath);
  assert(ix.length === 7 && ix.every(e => fileOf(e) && st.blobs.has(fileOf(e))), 'index entries with their per-SKU files');
  assert(ix.some(e => e.sizes && e.sizes.S), 'the size suffix became a sized entry');
  assert(ix.some(e => e.sku === 'BR-DUP-05') && ix.find(e => e.sku === 'BR-DUP-05').aiPath === ix.find(e => e.sku === 'BR-TST-05').aiPath, 'the second line under a charm is its own SKU on the same file');
  assert(rep.orphans.length === 1 && rep.unlabelled.length === 2 && rep.duplicates.length === 0, 'edge cases reported: ' + JSON.stringify({ o: rep.orphans.length, u: rep.unlabelled.length, d: rep.duplicates.length }));
  const files = st.list('Charm_Master_Files'); assert(files.length === 1 && files[0].indexedBy === 'local-indexer' && files[0].charms === 8, 'the file record');
  assert([...st.blobs.keys()].some(k => k.startsWith('charmnest/master/files/')), 'the master itself was stored');
  assert(fs.existsSync(file + '.index-progress.json') && fs.existsSync(file + '.index-report.json'), 'progress and report files written');
  // the same sheet again: every SKU is already held, so nothing is built and nothing is uploaded
  const blobsBefore = st.blobs.size; const callsBefore = st.calls.length;
  const again = await main(['node', 'x', file, '--origin', sorterOrigin], log);
  assert(again.written === 0 && again.held === 6, 'a sheet whose SKUs are all held indexes nothing: ' + JSON.stringify({ written: again.written, held: again.held }));
  assert(st.blobs.size === blobsBefore && !st.calls.slice(callsBefore).some(c => c.name === 'charmNestOutput'), 'and uploads nothing');
  // one new SKU on the sheet: only that charm is built, and it carries its charm's other SKUs with it
  const holdBack = st.docs.get('Charm_Master_Index/BR-TST-06'); st.docs.delete('Charm_Master_Index/BR-TST-06');
  const partial = await main(['node', 'x', file, '--origin', sorterOrigin], log);
  assert(partial.written === 1 && partial.held === 5, 'only the charm with the new SKU is indexed: ' + JSON.stringify({ written: partial.written, held: partial.held }));
  assert(st.docs.has('Charm_Master_Index/BR-TST-06'), 'the new SKU is in the index');
  void holdBack;
  // --all rebuilds the lot
  const forced = await main(['node', 'x', file, '--origin', sorterOrigin, '--all'], log);
  assert(forced.written === 7, '--all rewrites every SKU: ' + forced.written);
  void fx;
  console.log('index-master OK ·', ix.length, 'SKUs ·', st.blobs.size, 'files');
  srv.close();
})().catch(e => { console.error(e); process.exit(1); });
