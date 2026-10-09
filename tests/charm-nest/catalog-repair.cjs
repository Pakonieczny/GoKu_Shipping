// The catalogue repair kit (scripts/index-master.cjs --only / --out-dir, scripts/catalog-repair.cjs, scripts/audit-catalog*.cjs)
// against the stand-in server, which runs the REAL charmNestLibrary / charmNestOutput handlers over an in-memory Firestore and
// Storage. It rehearses the whole live procedure on a fixture master and counts what each step costs:
//   1. a stale per-SKU file is in the library (one design holds another design's drawing);
//   2. backup reads the index and downloads the files, and WRITES nothing;
//   3. a staged run makes no server call at all;
//   4. diff finds the one stale design and leaves the others out;
//   5. index-master --only rewrites exactly that SKU: 1 pre-read, 1 file list, 1 put, 1 index write, no masterList, and the
//      thumbnail it held is kept;
//   6. verify passes, and fails when a SKU that was not repaired moves;
//   7. restore puts the backed-up files and records back.
//   node tests/charm-nest/catalog-repair.cjs
const fs = require('fs'), path = require('path'), assert = require('assert'), os = require('os'), crypto = require('crypto');
const { start } = require('./bridge-server.cjs');
const { buildMaster } = require('./fixture-master.cjs');
const IM = require('../../scripts/index-master.cjs');
const CR = require('../../scripts/catalog-repair.cjs');
const AC = require('../../scripts/audit-catalog.cjs');
const ACR = require('../../scripts/audit-catalog-report.cjs');

const sha = b => crypto.createHash('sha256').update(b).digest('hex');
(async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cn-repair-'));
  const file = path.join(tmp, 'BRITES-master.ai');
  await buildMaster(file, { count: 8, edge: true });
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin } = srv;
  const lines = []; const log = l => { lines.push(String(l)); if (process.env.CN_VERBOSE) console.log(l); };
  // a stored link points at Google's host: answer it from the in-memory bucket, and count it (a Storage GET, not a Netlify call)
  let storageGets = 0; const realFetch = global.fetch;
  global.fetch = async (url, init) => {
    const u = String(url); const m = /^https:\/\/firebasestorage\.googleapis\.com\/v0\/b\/[^/]+\/o\/([^?]+)/.exec(u);
    if (!m) return realFetch(url, init);
    storageGets++; const b = st.blobs.get(decodeURIComponent(m[1]));
    return b ? new Response(b.buf, { status: 200 }) : new Response('', { status: 404 });
  };
  const calls = (from, name, op) => st.calls.slice(from).filter(c => (!name || c.name === name) && (!op || c.op === op)).length;
  const idx = sku => st.docs.get('Charm_Master_Index/' + sku);
  const urlOf = p => `https://firebasestorage.googleapis.com/v0/b/test-bucket/o/${encodeURIComponent(p)}?alt=media&token=t`;
  const run = (mod, args) => mod.main(['node', 'x', ...args], log);

  // ── the library as it stands: the whole fixture indexed, then one design made stale ──
  await run(IM, [file, '--origin', sorterOrigin]);
  const all = st.list('Charm_Master_Index').map(e => e._id).sort();
  assert(all.length === 7, 'seven SKU lines indexed: ' + all.join());
  const stalePath = idx('BR-TST-01').aiPath, otherPath = idx('BR-TST-06').aiPath;
  assert(stalePath && otherPath && stalePath !== otherPath);
  const good = Buffer.from(st.blobs.get(stalePath).buf);
  st.blobs.get(stalePath).buf = Buffer.from(st.blobs.get(otherPath).buf);          // BR-TST-01 now holds BR-TST-06's drawing
  // it has a thumbnail the local indexer cannot redraw (no resvg here): the repair must leave it
  const thumbP = 'charmnest/master/BR-TST-01.png', png = Buffer.from('89504e470d0a1a0a0000000d49484452', 'hex');
  st.blobs.set(thumbP, { buf: png, generation: 1, meta: { contentType: 'image/png', metadata: { firebaseStorageDownloadTokens: 't' } } });
  st.put('Charm_Master_Index', 'BR-TST-01', { thumbPath: thumbP, thumbUrl: urlOf(thumbP) });

  // ── 2. backup: reads only ──
  const bk = path.join(tmp, 'backup'), stage = path.join(tmp, 'stage');
  const docsBefore = JSON.stringify([...st.docs.entries()]), blobsBefore = st.blobs.size;
  let c0 = st.calls.length; storageGets = 0;
  const bkRes = await run(CR, ['backup', '--origin', sorterOrigin, '--out', bk, '--skus', 'BR-TST-01,BR-TST-05']);
  assert.strictEqual(process.exitCode || 0, 0, 'the backup ended cleanly');
  const ops = st.calls.slice(c0).map(c => c.name + ':' + c.op);
  assert(ops.every(o => o === 'charmNestLibrary:masterList' || o === 'charmNestLibrary:masterListFiles'), 'backup only lists: ' + ops.join());
  assert.strictEqual(calls(c0, 'charmNestLibrary', 'masterList'), 1, 'one masterList part for a library this size');
  assert.strictEqual(JSON.stringify([...st.docs.entries()]), docsBefore, 'a backup changes no document');
  assert.strictEqual(st.blobs.size, blobsBefore, 'a backup changes no file');
  const man = JSON.parse(fs.readFileSync(path.join(bk, 'manifest.json'), 'utf8'));
  assert.deepStrictEqual(man.skus, ['BR-DUP-05', 'BR-TST-01', 'BR-TST-05'], 'the whole design of a named SKU is backed up (BR-DUP-05 shares BR-TST-05\'s file): ' + man.skus);
  assert(man.files.some(f => f.path === stalePath && f.sha256 === sha(st.blobs.get(stalePath).buf)), 'the stale file is in the backup as it was');
  assert(man.files.some(f => f.path === thumbP), 'the thumbnail is in the backup too');
  assert.strictEqual(storageGets, man.files.length, 'one Storage GET per file, no Netlify call for them');
  assert(!/token=(?!REDACTED)/.test(fs.readFileSync(path.join(bk, 'index-all.json'), 'utf8')), 'download tokens are redacted in the backup');
  assert(bkRes.entries === 7 && JSON.parse(fs.readFileSync(path.join(bk, 'index-all.json'), 'utf8')).count === 7);

  // ── 3. staging touches no server ──
  c0 = st.calls.length;
  await run(IM, [file, '--only', 'BR-TST-01,BR-TST-05', '--out-dir', stage]);
  assert.strictEqual(st.calls.length, c0, 'a staged run makes no call to the site');
  const rec = JSON.parse(fs.readFileSync(path.join(stage, 'records.json'), 'utf8'));
  assert.deepStrictEqual(rec.entries.map(e => e.sku).sort(), ['BR-DUP-05', 'BR-TST-01', 'BR-TST-05'], 'the staged records are the named charms, with the SKU that shares one');
  assert(fs.existsSync(path.join(stage, 'files', stalePath)), 'the staged per-SKU file is where the library keeps it');

  // ── 4. diff: only the stale design differs ──
  c0 = st.calls.length;
  const df = await run(CR, ['diff', '--origin', sorterOrigin, '--stage', stage]);
  assert(df.designs === 2 && df.changed === 1 && df.same === 1 && df.failed === 0, 'diff finds the one stale design: ' + JSON.stringify(df));
  assert.deepStrictEqual(JSON.parse(fs.readFileSync(path.join(stage, 'changed-skus.json'), 'utf8')), ['BR-TST-01']);
  assert.strictEqual(calls(c0, 'charmNestLibrary', 'masterList'), 1, 'diff reads the index once');
  assert.strictEqual(calls(c0) - 1, 0, 'and nothing else');

  // ── 5. the repair: --only, from the diff ──
  c0 = st.calls.length; const reads0 = st.reads || 0; const touched = []; st.tick = async what => { touched.push(what); };   // (what each Firestore read and commit touched)
  const rep = await run(IM, [file, '--only', path.join(stage, 'changed-skus.json'), '--origin', sorterOrigin]); st.tick = null;
  assert.strictEqual(rep.written, 1, 'one SKU written: ' + rep.written);
  const by = {}; for (const c of st.calls.slice(c0)) { const k = c.name + ':' + c.op; by[k] = (by[k] || 0) + 1; }
  assert.deepStrictEqual(by, { 'charmNestLibrary:masterGetMany': 2, 'charmNestLibrary:masterListFiles': 1, 'charmNestOutput:put': 1, 'charmNestLibrary:masterPutIndex': 1 },
    'the calls of a one-SKU repair: ' + JSON.stringify(by));   // (masterGetMany: the pre-read and the read-back)
  assert(!by['charmNestLibrary:masterList'] && !by['charmNestLibrary:masterPutFile'], 'no full index read, no file-record write');
  console.log('  Firestore touched:', touched.join(' | '));
  console.log('one-SKU repair: Netlify calls', st.calls.length - c0, '· Firestore document reads', (st.reads || 0) - reads0, JSON.stringify(by));
  assert(sha(st.blobs.get(stalePath).buf) === sha(good) || AC.printDiff(await AC.filePrint(st.blobs.get(stalePath).buf), await AC.filePrint(good)).length === 0, 'BR-TST-01 draws as it should again');
  assert.strictEqual(idx('BR-TST-01').thumbPath, thumbP, 'the thumbnail it held is kept (no resvg here to redraw it)');
  assert.strictEqual(sha(st.blobs.get(thumbP).buf), sha(png), 'and so is the thumbnail file');
  assert(sha(st.blobs.get(otherPath).buf) !== sha(good), 'the other design was not touched');
  // nobody else moved: every other document is as it was
  const before = JSON.parse(fs.readFileSync(path.join(bk, 'index-all.json'), 'utf8')).entries;
  for (const b of before) if (b.sku !== 'BR-TST-01') { const n = idx(b.sku); const pick = e => JSON.stringify([e.charmHash || null, e.aiPath || null, Object.entries(e.sizes || {}).map(([k, g]) => [k, g.charmHash, g.aiPath])]); assert(n && pick(n) === pick(b), 'untouched: ' + b.sku); }

  // ── 6. verify: passes; fails when something else moved ──
  process.exitCode = 0; c0 = st.calls.length;
  let v = await run(CR, ['verify', '--origin', sorterOrigin, '--stage', stage, '--backup', bk, '--changed', path.join(stage, 'changed-skus.json')]);
  assert(v.bad.length === 0 && v.checked === 1, 'the repair verifies: ' + JSON.stringify(v.bad));
  assert.strictEqual(calls(c0, 'charmNestLibrary', 'masterList'), 1, 'verify reads the index once');
  assert.strictEqual(calls(c0, 'charmNestOutput'), 0, 'and writes nothing');
  st.put('Charm_Master_Index', 'BR-TST-07', { charmHash: 'moved' });
  v = await run(CR, ['verify', '--origin', sorterOrigin, '--stage', stage, '--backup', bk, '--changed', path.join(stage, 'changed-skus.json')]);
  assert(v.bad.some(b => b.sku === 'BR-TST-07' && /though it was not repaired/.test(b.why)), 'a SKU that moved without being repaired is reported: ' + JSON.stringify(v.bad));
  assert.strictEqual(process.exitCode, 1, 'and the exit code says so'); process.exitCode = 0;
  st.put('Charm_Master_Index', 'BR-TST-07', { charmHash: before.find(b => b.sku === 'BR-TST-07').charmHash });

  // ── 7. restore: the backed-up state comes back ──
  c0 = st.calls.length;
  const dry = await run(CR, ['restore', '--origin', sorterOrigin, '--from', bk, '--skus', 'BR-TST-01', '--dry']);
  assert(dry.skus === 1 && calls(c0) === 0, 'a dry restore calls nothing');
  const rs = await run(CR, ['restore', '--origin', sorterOrigin, '--from', bk, '--skus', 'BR-TST-01']);
  assert(rs.wrote === 1 && rs.bad === 0, 'restore wrote the record and read it back: ' + JSON.stringify(rs));
  assert.strictEqual(sha(st.blobs.get(stalePath).buf), man.files.find(f => f.path === stalePath).sha256, 'the file is back byte for byte');
  assert.strictEqual(idx('BR-TST-01').charmHash, before.find(b => b.sku === 'BR-TST-01').charmHash, 'the record is back');
  assert.strictEqual(idx('BR-TST-01').thumbPath, thumbP);
  v = await run(CR, ['verify', '--origin', sorterOrigin, '--stage', stage, '--changed', path.join(stage, 'changed-skus.json')]);
  assert(v.bad.length > 0, 'after the rollback verify says the repair is not live (the files differ from the staged ones)'); process.exitCode = 0;

  // ── the audit tool runs on a master and its report names a defect ──
  const dumpFile = path.join(tmp, 'dump.json'), outDir = path.join(tmp, 'audit');
  await AC.dump(file, dumpFile, {});
  const d = JSON.parse(fs.readFileSync(dumpFile, 'utf8'));
  assert(d.charms.length === 8 && d.charms.filter(c => c.sku).length === 6, 'the dump holds every outline and its SKU');
  const origLog = console.log; console.log = () => {};
  try { ACR.main([dumpFile, '--out', outDir]); } finally { console.log = origLog; }
  const off = JSON.parse(fs.readFileSync(path.join(outDir, 'CATALOG-offending.json'), 'utf8'));
  assert(Array.isArray(off.offending) || Array.isArray(off.rows) || Array.isArray(off), 'CATALOG-offending.json is written');
  assert(/Defects per category/.test(fs.readFileSync(path.join(outDir, 'CATALOG-report.md'), 'utf8')), 'and so is the report');

  global.fetch = realFetch;
  console.log('catalog-repair OK ·', all.length, 'SKUs rehearsed');
  srv.close();
})().catch(e => { console.error(e); process.exit(1); });
