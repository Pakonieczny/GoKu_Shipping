// FC11 cost check: the evidence poll that investorEvents-background runs every 10 minutes (08:30 to 20:00 ET) for each watched symbol.
// Run: node tests/cost/fc11-evidence-cost.cjs        (no network, no secrets, in-memory Firestore from tests/cost/meter.cjs)
//
// For each polled symbol the loop calls documentsForCompany twice, and only looks at the accession numbers and at when each version became known.
// That function reads EVERY version of the symbol (each carries the whole canonical text, up to 120 KB) to find the newest one known at the time.
// A `light` caller now asks Firestore for the four small fields it needs. This proves the answer a light caller reads is unchanged and prints the bytes.
// VERSION_KB (default 20) is the assumed size of one version's text; the real one was not measurable from here.
'use strict';
const assert = require('node:assert/strict');
const Module = require('module');
const meter = require('./meter.cjs');

const realLoad = Module._load;
Module._load = function (req, ...rest) {
  if (req === '@google-cloud/firestore') {
    try { return realLoad.call(this, req, ...rest); } catch (e) {
      class X {}
      return { Firestore: X, FieldValue: meter.FieldValue, Timestamp: meter.Timestamp, DocumentReference: X, Transaction: X, WriteBatch: X };
    }
  }
  return realLoad.call(this, req, ...rest);
};
const A = require('../../netlify/functions/_investorAdmin');
const E = require('../../netlify/functions/_investorEvidence');

const DOCS = Number(process.env.DOCS || 60), VERSION_KB = Number(process.env.VERSION_KB || 20);
const NOW = Date.UTC(2026, 9, 7, 15, 0, 0);
const filler = kb => 'x'.repeat(Math.round(kb * 1024));

function seed() {
  const d = {};
  for (let i = 0; i < DOCS; i++) {
    const id = 'doc' + i, published = new Date(NOW - i * 86400000 * 0.2).toISOString();
    d[`${A.COL.documents}/${id}`] = { documentId: id, symbol: 'AAA', accession: 'acc' + i, source_published_at: published, firstSeenAtMs: NOW - i * 86400000 * 0.2, summary: 'summary ' + i };
    for (let v = 0; v < 2; v++) d[`${A.COL.versions}/${id}_v${v}`] = { versionId: `${id}_v${v}`, documentId: id, symbol: 'AAA', fetchedAtMs: NOW - i * 86400000 * 0.2 + v * 1000, canonical_content_sha256: 'sha' + i + v, canonicalText: filler(VERSION_KB) };
  }
  return d;
}
async function run(light, mk) {
  const m = meter.create(); m.db.seed(seed());
  const admin = mk(m);
  const before = m.snapshot();
  const out = await m.op('documentsForCompany', () => E.documentsForCompany('AAA', NOW, 10, 120, { admin, light }));
  return { d: m.since(before), out };
}
const full = m => ({ COL: A.COL, col: n => m.db.collection(n) });
const noSelect = m => ({ COL: A.COL, col: n => { const c = m.db.collection(n); return { where: (...a) => { const q = c.where(...a); return new Proxy(q, { get: (t, p) => (p === 'select' ? undefined : typeof t[p] === 'function' ? t[p].bind(t) : t[p]) }); } }; } });

(async () => {
  const kb = n => (n / 1024).toFixed(0) + ' KB';
  const whole = await run(false, full), lightR = await run(true, full), lightNoSelect = await run(true, noSelect);
  const pick = r => r.out.map(x => [x.documentId, x.accession, x.versionId, x.decisionKnownAtMs]);
  assert.deepEqual(pick(lightR), pick(whole), 'a light read returns the same documents, accessions, versions and known-at times');
  assert.deepEqual(pick(lightNoSelect), pick(whole), 'and so does a backend without field masks');
  assert.deepEqual(lightNoSelect.out, whole.out, 'a backend without field masks returns exactly the old documents');
  assert(whole.out.length > 0 && lightR.out.length === whole.out.length);
  console.log(`one symbol, ${DOCS} documents x 2 versions of ${VERSION_KB} KB: full read ${whole.d.reads} reads / ${kb(whole.d.bytes)}, light read ${lightR.d.reads} reads / ${kb(lightR.d.bytes)} (${Math.round(whole.d.bytes / lightR.d.bytes)}x fewer bytes)`);
  console.log(`per 10-minute slot, 40 polled symbols x 2 calls: ${kb(whole.d.bytes * 80)} -> ${kb(lightR.d.bytes * 80)}; per day (69 slots): ${(whole.d.bytes * 80 * 69 / 1e9).toFixed(2)} GB -> ${(lightR.d.bytes * 80 * 69 / 1e9).toFixed(3)} GB`);
  console.log('investor evidence cost: answers unchanged, bytes measured');
})().catch(e => { console.error(e); process.exitCode = 1; });
