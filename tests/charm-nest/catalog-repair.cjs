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
  const by2 = from => { const o = {}; for (const c of st.calls.slice(from)) { const k = c.name + ':' + c.op; o[k] = (o[k] || 0) + 1; } return o; };
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
  st.put('Charm_Master_Index', 'BR-TST-01', { thumbPath: thumbP, thumbUrl: urlOf(thumbP), charmHash: 'oldhash' });   // (a hash from before: the geometry has not changed, so the repair keeps it)

  // two junk records, as an older reader left them: a callout that shares a real design's file, and one with a file of its own
  const mate = idx('BR-TST-05'), frontPath = 'charmnest/master/FRONT.ai';
  st.blobs.set(frontPath, { buf: Buffer.from(st.blobs.get(otherPath).buf), generation: 1, meta: { contentType: 'application/illustrator', metadata: { firebaseStorageDownloadTokens: 't' } } });
  st.put('Charm_Master_Index', '11.4 MM', { sku: '11.4 MM', masterHash: mate.masterHash, masterName: mate.masterName, aiPath: mate.aiPath, aiUrl: mate.aiUrl, charmHash: mate.charmHash, widthPt: mate.widthPt, heightPt: mate.heightPt });
  st.put('Charm_Master_Index', 'FRONT', { sku: 'FRONT', masterHash: mate.masterHash, masterName: mate.masterName, aiPath: frontPath, aiUrl: urlOf(frontPath), charmHash: 'x', widthPt: 10, heightPt: 10 });

  // ── 2. backup: reads only ──
  const bk = path.join(tmp, 'backup'), stage = path.join(tmp, 'stage');
  const docsBefore = JSON.stringify([...st.docs.entries()]), blobsBefore = st.blobs.size;
  let c0 = st.calls.length; storageGets = 0;
  const bkRes = await run(CR, ['backup', '--origin', sorterOrigin, '--out', bk, '--skus', 'BR-TST-01,BR-TST-05,FRONT']);
  assert.strictEqual(process.exitCode || 0, 0, 'the backup ended cleanly');
  const ops = st.calls.slice(c0).map(c => c.name + ':' + c.op);
  assert(ops.every(o => o === 'charmNestLibrary:masterList' || o === 'charmNestLibrary:masterListFiles'), 'backup only lists: ' + ops.join());
  assert.strictEqual(calls(c0, 'charmNestLibrary', 'masterList'), 1, 'one masterList part for a library this size');
  assert.strictEqual(JSON.stringify([...st.docs.entries()]), docsBefore, 'a backup changes no document');
  assert.strictEqual(st.blobs.size, blobsBefore, 'a backup changes no file');
  const man = JSON.parse(fs.readFileSync(path.join(bk, 'manifest.json'), 'utf8'));
  assert.deepStrictEqual(man.skus, ['11.4 MM', 'BR-DUP-05', 'BR-TST-01', 'BR-TST-05', 'FRONT'], 'the whole design of a named SKU is backed up (BR-DUP-05 shares BR-TST-05\'s file): ' + man.skus);
  assert(man.files.some(f => f.path === stalePath && f.sha256 === sha(st.blobs.get(stalePath).buf)), 'the stale file is in the backup as it was');
  assert(man.files.some(f => f.path === thumbP), 'the thumbnail is in the backup too');
  assert.strictEqual(storageGets, man.files.length, 'one Storage GET per file, no Netlify call for them');
  assert(!/token=(?!REDACTED)/.test(fs.readFileSync(path.join(bk, 'index-all.json'), 'utf8')), 'download tokens are redacted in the backup');
  assert(bkRes.entries === 9 && JSON.parse(fs.readFileSync(path.join(bk, 'index-all.json'), 'utf8')).count === 9);

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
  assert.strictEqual(idx('BR-TST-01').charmHash, 'oldhash', 'the charm hash stands while size, area and holes are the same (a cut Rose sheet compares it)');
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

  // ── junk records: listed, then removed (only with --write), then brought back by restore ──
  const jl = await run(CR, ['junk', '--from', bk]);
  assert.deepStrictEqual(jl.map(j => j.sku).sort(), ['11.4 MM', 'FRONT'], 'the callouts are junk, the SKUs are not: ' + jl.map(j => j.sku));
  assert(jl.find(j => j.sku === '11.4 MM').shareGood.includes('BR-TST-05'), 'a callout that shares a real design\'s file says so (the file stays)');
  c0 = st.calls.length;
  const dr = await run(CR, ['prune', '--origin', sorterOrigin, '--from', bk]);
  assert(dr.would === 2 && calls(c0) === 0 && idx('FRONT') && idx('11.4 MM'), 'prune without --write only lists');
  fs.writeFileSync(path.join(tmp, 'busy.json'), '["FRONT"]');                       // FRONT is on a sheet
  const pr = await run(CR, ['prune', '--origin', sorterOrigin, '--from', bk, '--write', '--exclude', path.join(tmp, 'busy.json')]);
  assert(pr.removed === 1 && !idx('11.4 MM') && idx('FRONT'), 'a junk SKU that a sheet uses is left (the exclude list), the other is removed: ' + JSON.stringify(pr));
  assert.strictEqual(calls(c0, 'charmNestLibrary', 'masterRemoveSku'), 1, 'one call per record removed');
  assert(st.blobs.has(mate.aiPath) && idx('BR-TST-05'), 'the real design and its file are untouched');
  const rj = await run(CR, ['restore', '--origin', sorterOrigin, '--from', bk, '--skus', '11.4 MM']);
  assert(rj.bad === 0 && idx('11.4 MM'), 'restore brings a pruned record back');

  // ── busy: the SKUs on open sheets, read the way the Library reads them ──
  st.put('Charm_Nest_Sheets', 'sheetAAAAAAAAAA', { id: 'sheetAAAAAAAAAA', day: '2026-10-08', metal: 'gold', status: 'open', fileBase: 'GF Sheet 1', sources: [{ name: 'BR-TST-03 (master)', hash: 'h1' }, { name: 'BR-TST-02 · S (master)', hash: 'h2' }, { name: 'custom-upload.ai', hash: 'h3' }] });
  st.put('Charm_Nest_Sheets', 'sheetBBBBBBBBBB', { id: 'sheetBBBBBBBBBB', day: '2026-10-07', metal: 'rose', status: 'open', roseStockId: 'stock1', sources: [{ name: 'BR-TST-06 (master)', hash: 'h4' }] });
  st.put('Charm_Nest_Sheets', 'sheetCCCCCCCCCC', { id: 'sheetCCCCCCCCCC', day: '2026-10-06', metal: 'gold', archived: true, sources: [{ name: 'BR-TST-07 (master)', hash: 'h5' }] });
  c0 = st.calls.length;
  const bz = await run(CR, ['busy', '--origin', sorterOrigin, '--out', path.join(tmp, 'busy-skus.json')]);
  const bj = JSON.parse(fs.readFileSync(path.join(tmp, 'busy-skus.json'), 'utf8'));
  assert.deepStrictEqual(bj.skus, ['BR-TST-02', 'BR-TST-03', 'BR-TST-06'], 'the SKUs of the open sheets, not the archived one, no custom upload: ' + bj.skus);
  assert(bj.sheets.find(x => x.id === 'sheetBBBBBBBBBB').rose && bz.sheets === 2, 'a Rose sheet is marked');
  assert.deepStrictEqual(by2(c0), { 'charmNestLibrary:listSheets': 1 }, 'one call');

  // ── a whole-master stage: diff finds every design that differs, and names the live records the stage does not carry ──
  const stageAll = path.join(tmp, 'stage-all'); await run(IM, [file, '--out-dir', stageAll]);
  const dfa = await run(CR, ['diff', '--origin', sorterOrigin, '--stage', stageAll]);
  assert(dfa.changed === 1 && dfa.same === 5 && dfa.failed === 0, 'whole sheet: only the stale design differs: ' + JSON.stringify(dfa));
  const dja = JSON.parse(fs.readFileSync(path.join(stageAll, 'diff.json'), 'utf8'));
  assert.deepStrictEqual(dja.liveOnly.map(x => x.sku).sort(), ['11.4 MM', 'FRONT'], 'the records the sheet no longer carries are listed');
  // diff --save-to: the backup of what it found, from the one read of the index it made
  c0 = st.calls.length; const bk2 = path.join(tmp, 'backup2');
  await run(CR, ['diff', '--origin', sorterOrigin, '--stage', stageAll, '--save-to', bk2]);
  assert.deepStrictEqual(by2(c0), { 'charmNestLibrary:masterList': 1, 'charmNestLibrary:masterListFiles': 1 }, 'one index read and one file-record read: ' + JSON.stringify(by2(c0)));
  const man2 = JSON.parse(fs.readFileSync(path.join(bk2, 'manifest.json'), 'utf8'));
  assert(man2.skus.includes('BR-TST-01') && man2.files.some(f => f.path === stalePath) && fs.existsSync(path.join(bk2, 'files', stalePath)), 'the changed design is in the backup');

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
  console.log('catalog-repair OK ·', all.length + 2, 'SKU records rehearsed');
  srv.close();
})().catch(e => { console.error(e); process.exit(1); });
