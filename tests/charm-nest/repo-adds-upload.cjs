// LETTERS-HOOPS (Paul, 9 Nov 2026): the ONE press that puts committed new charms into the Charm Repo (charm-nest-repo-adds.js + charm-repo-adds/queue.json), OFFLINE.
// The page's files are served from this checkout (the committed folder, as the site serves it); the library is the REAL write code (netlify/functions/_charmNestMaster.js putIndex / putFile,
// the code behind masterPutIndex / masterPutFile) on the in-memory Firestore of tests/charm-nest/_sandboxFakes.cjs; Storage is a map; the password lock is a stand-in that says yes or no (no password exists here).
// It proves:
//   · the queue names the letters add, its records are complete, every file it names is committed;
//   · nothing is sent before the lock says yes; a lock left shut writes nothing;
//   · the first press stores each SKU's .ai and picture under charmnest/master/, the master sheet under charmnest/master/files/, writes the 52 index entries and the upload-history record, and reads them back;
//   · the second press says "Already there" and writes nothing (no file, no index write);
//   · a SKU another master file already holds stops the press before anything is written (nothing is ever replaced);
//   · a press interrupted after part of the work finishes the rest on the next press;
//   · no request leaves this process (no Etsy, no network).
//   node tests/charm-nest/repo-adds-upload.cjs
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const Fk = require('./_sandboxFakes.cjs');
const Master = require(path.join(root, 'netlify/functions/_charmNestMaster.js'));
const RA = require(path.join(root, 'charm-nest-repo-adds.js'));
const FV = Fk.admin.firestore.FieldValue, db = Fk.db;
let n = 0; const ok = (c, m) => { assert(c, m); n++; console.log('  ok  ' + m); };
global.fetch = () => { throw new Error('a network request in an offline test'); };

const queue = RA.normalize(JSON.parse(fs.readFileSync(path.join(root, 'charm-repo-adds/queue.json'), 'utf8')));
const add = queue.find(a => a.id === 'letters-hoops-2026-11-09');
ok(queue.length >= 1 && add && add.folder === 'charm-repo-adds/letters-hoops-2026-11-09' && add.sheet && add.contact, 'the queue names the letters add');
const rec = RA.checkRecords(JSON.parse(fs.readFileSync(path.join(root, add.folder, add.records), 'utf8')));
ok(rec.entries.length === 52, 'its records hold 52 charms');
for (const e of rec.entries) assert(fs.existsSync(path.join(root, add.folder, 'files', e.aiPath)) && fs.existsSync(path.join(root, add.folder, 'files', e.thumbPath)), e.sku + ' files');
ok(fs.existsSync(path.join(root, add.folder, add.sheet)) && fs.existsSync(path.join(root, add.folder, add.contact)), 'every file it names is committed (52 .ai, 52 pictures, the sheet, the contact sheet)');

/* ── the page's world ── */
const served = [];   // every address the page fetched
const siteFetch = async (url) => {
  served.push(url);
  const rel = decodeURIComponent(String(url));
  if (/^[a-z]+:/i.test(rel) || rel.includes('..')) throw new Error('not this site: ' + url);
  const f = path.join(root, rel);
  if (!fs.existsSync(f)) return { ok: false, status: 404 };
  const buf = fs.readFileSync(f);
  return { ok: true, status: 200, json: async () => JSON.parse(buf.toString('utf8')), arrayBuffer: async () => buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength) };
};
const blobs = new Map(); let lockSays = true, asked = 0, failAfter = Infinity, uploads = 0;
const safePath = p => String(p).replace(/[^\w.\-\/]+/g, '_');                 // (charmNestOutput's own rule, _charmNestAuth.js safePath)
const lock = { ensure: async () => { asked++; return lockSays ? { token: 't', action: 'upload' } : null; } };
const upBytes = async (p, bytes, type) => { if (!lockSays) { const e = new Error('locked'); e.cancelled = true; throw e; } if (++uploads > failAfter) throw new Error('the connection dropped'); const k = safePath(p); blobs.set(k, { bytes: Buffer.from(bytes), type }); return { path: k, url: 'https://storage.example/' + encodeURIComponent(k) }; };
const calls = [];
const repoApi = async (action, ask, body) => {
  calls.push(body.op);
  if (!lockSays) { const e = new Error('locked'); e.cancelled = true; throw e; }
  if (action !== 'upload') throw new Error('wrong unlock ' + action);
  if (body.op === 'masterPutIndex') return Object.assign({ ok: true }, await Master.putIndex(db, FV, body));
  if (body.op === 'masterPutFile') return Master.putFile(db, FV, body);
  throw new Error('unexpected op ' + body.op);
};
const api = async (fn, body) => {
  assert.strictEqual(fn, 'charmNestLibrary'); assert.strictEqual(body.op, 'masterGetMany'); calls.push('masterGetMany');
  const out = {}; for (const s of body.skus) { const sn = await db.collection(Master.INDEX).doc(String(s).toUpperCase()).get(); if (sn.exists) out[sn.id] = Master.slimEntry(sn.data()); }
  return { entries: out };
};
const o = () => ({ repoApi, upBytes, api, fetch: siteFetch, who: () => 'Paul', lock, say: () => {} });
const indexed = () => Fk.store.size ? [...Fk.store.keys()].filter(k => k.startsWith(Master.INDEX + '/')) : [];
const reset = () => { Fk.store.clear(); blobs.clear(); calls.length = 0; served.length = 0; uploads = 0; failAfter = Infinity; lockSays = true; asked = 0; };

(async () => {
  /* 1 · the lock stays shut: nothing is read or written */
  reset(); lockSays = false;
  let r = await RA.run(o(), add);
  ok(r.state === 'stop' && /Nothing was added: the repo stayed locked/.test(r.text) && asked === 1, 'the lock left shut: "Nothing was added: the repo stayed locked."');
  ok(!blobs.size && !indexed().length && !calls.length, 'and nothing was stored, written or even read from the library');

  /* 2 · the first press */
  reset();
  r = await RA.run(o(), add);
  ok(r.state === 'done' && r.added === 52 && /^Done: 52 charms added to the charm repo\. All 52 of Letter charms with hoops/.test(r.text), 'the first press: ' + r.text);
  ok(asked === 1, 'the password was asked once');
  const ix = indexed().map(k => Fk.store.get(k));
  ok(ix.length === 52 && ix.every(e => e.masterHash === rec.masterHash && e.masterName === 'LETTER-HOOPS_2026-1109.ai' && e.hashSource === 'local' && !e.blocked), '52 index entries, all from this master sheet, none blocked');
  const want = rec.entries.map(e => e.sku.toUpperCase()).sort();
  ok(JSON.stringify(ix.map(e => e.sku).sort()) === JSON.stringify(want), 'exactly the 52 SKUs LETTER_NECKLACE-0 … -25 and LETTER_NECKLACE-0 (HUGGIE) … -25 (HUGGIE)');
  for (const e of ix) {
    const src = rec.entries.find(x => x.sku.toUpperCase() === e.sku);
    assert(blobs.has(e.aiPath) && blobs.has(e.thumbPath), e.sku + ' stored');
    assert(Buffer.compare(blobs.get(e.aiPath).bytes, fs.readFileSync(path.join(root, add.folder, 'files', src.aiPath))) === 0, e.sku + ' .ai is the committed file');
    assert(e.aiUrl && e.thumbUrl && e.widthPt === src.widthPt && e.holes === src.holes && e.charmHash === src.charmHash, e.sku + ' entry');
  }
  ok(true, 'each entry points at its stored .ai and picture (the committed bytes), with the staged size, holes and hash');
  ok([...blobs.keys()].some(k => k.startsWith('charmnest/master/files/' + rec.masterHash.slice(0, 12))), 'the master sheet is stored under charmnest/master/files/');
  const file = Fk.store.get(Master.FILES + '/' + rec.masterHash);
  ok(file && file.charms === 52 && file.skus.length === 52 && file.name === 'LETTER-HOOPS_2026-1109.ai' && file.by === 'Paul', 'the upload history has the file: 52 charms, by Paul');
  ok(!new Set(rec.entries.map(e => e.aiPath)).has('charmnest/master/LETTER_EARRING-0.ai') && [...blobs.keys()].every(k => k.startsWith('charmnest/master/')), 'only charmnest/master/ was written');
  ok(served.every(u => u.startsWith('charm-repo-adds/letters-hoops-2026-11-09/')), 'the page read only the committed folder');

  /* 3 · the second press: Already there, nothing written */
  const before = { blobs: blobs.size, writes: Fk.writes.length }; calls.length = 0; served.length = 0;
  r = await RA.run(o(), add);
  ok(r.state === 'already' && r.text === 'Already there: all 52 charms of Letter charms with hoops (26 necklace + 26 huggie, a–z) are in the charm repo. Nothing was written.', 'the second press: ' + r.text);
  ok(blobs.size === before.blobs && Fk.writes.length === before.writes && JSON.stringify(calls) === '["masterGetMany"]', 'and nothing was stored or written (one free read)');

  /* 4 · a SKU another master file holds: stop before anything is written */
  reset();
  await Master.putIndex(db, FV, { entries: [{ sku: 'LETTER_NECKLACE-3', widthPt: 10, heightPt: 10, aiPath: 'charmnest/master/OTHER.ai' }], masterHash: 'f'.repeat(64), masterName: 'SOMEONE ELSE.ai', hashSource: 'browser' });
  const w0 = Fk.writes.length; calls.length = 0;
  r = await RA.run(o(), add);
  ok(r.state === 'stop' && /^Nothing was added: LETTER_NECKLACE-3 is already in the repo from another file, and this press never replaces a charm\.$/.test(r.text), 'a SKU held from another file: ' + r.text);
  ok(!blobs.size && Fk.writes.length === w0 && Fk.store.get(Master.INDEX + '/LETTER_NECKLACE-3').masterName === 'SOMEONE ELSE.ai', 'nothing stored, written or replaced');

  /* 5 · interrupted while storing the files: no index written; the next press finishes */
  reset(); failAfter = 30;
  r = await RA.run(o(), add);
  ok(r.state === 'stop' && /Stopped while storing the files/.test(r.text) && !indexed().length, 'a dropped connection while storing: no charm in the index, said plainly');
  failAfter = Infinity;
  r = await RA.run(o(), add);
  ok(r.state === 'done' && indexed().length === 52, 'pressing again finishes: ' + r.text);

  /* 6 · interrupted after half the index: the next press adds only the rest */
  reset();
  await Master.putIndex(db, FV, { entries: rec.entries.slice(0, 20).map(e => Object.assign({}, e, { aiUrl: 'u', thumbUrl: 'u' })), masterHash: rec.masterHash, masterName: rec.masterName, hashSource: 'local' });
  r = await RA.run(o(), add);
  ok(r.state === 'done' && r.added === 32 && /^Done: 32 charms added to the charm repo \(20 were already there\)\./.test(r.text) && uploads === 65, 'twenty already there: only the other 32 are stored and written, 64 files and the sheet (' + r.text + ')');

  console.log(`repo-adds-upload: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
