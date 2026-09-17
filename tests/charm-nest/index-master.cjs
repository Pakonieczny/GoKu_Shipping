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
  assert.strictEqual(rep.written, 6, 'six labelled SKUs written (one sized, one blocked for two labels)');
  const ix = st.list('Charm_Master_Index');
  const fileOf = e => e.aiPath || (e.sizes && Object.values(e.sizes)[0] && Object.values(e.sizes)[0].aiPath);
  assert(ix.length === 6 && ix.every(e => fileOf(e) && st.blobs.has(fileOf(e))), 'index entries with their per-SKU files');
  assert(ix.some(e => e.sizes && e.sizes.S), 'the size suffix became a sized entry');
  assert(ix.some(e => /two labels/.test(e.blocked || '')), 'the double-labelled charm is stored as blocked');
  assert(rep.orphans.length === 1 && rep.unlabelled.length === 2 && rep.duplicates.length === 1, 'edge cases reported: ' + JSON.stringify({ o: rep.orphans.length, u: rep.unlabelled.length, d: rep.duplicates.length }));
  const files = st.list('Charm_Master_Files'); assert(files.length === 1 && files[0].indexedBy === 'local-indexer' && files[0].charms === 8, 'the file record');
  assert([...st.blobs.keys()].some(k => k.startsWith('charmnest/master/files/')), 'the master itself was stored');
  assert(fs.existsSync(file + '.index-progress.json') && fs.existsSync(file + '.index-report.json'), 'progress and report files written');
  // resume: everything comes from the progress file, nothing is re-uploaded
  const blobsBefore = st.blobs.size; const callsBefore = st.calls.length;
  const again = await main(['node', 'x', file, '--origin', sorterOrigin, '--resume'], log);
  assert(again.written === 6 && st.blobs.size === blobsBefore, 'a resumed run re-writes the index but re-uploads nothing');
  assert(!st.calls.slice(callsBefore).some(c => c.name === 'charmNestOutput'), 'no upload calls on resume');
  void fx;
  console.log('index-master OK ·', ix.length, 'SKUs ·', st.blobs.size, 'files');
  srv.close();
})().catch(e => { console.error(e); process.exit(1); });
