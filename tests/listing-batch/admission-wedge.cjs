'use strict';
// The shared submission gate (lib/listingBatchAdmission.cjs) after a create that never answered.
// beforeCreate moves the gate to phase 'creating' just before the paid OpenAI create call; if that call times
// out or the worker is frozen, the gate stays 'creating' and only reconcile() can release it. reconcile()
// used to release it only when OpenAI's job list showed the job: a create that never reached OpenAI held
// every queued set for good, until someone edited the database.
// Checked, with a gate stuck in 'creating' for 20 minutes: when the provider list reaches back past the
// create without the job ({ match: null, covered: true }) the gate is released and the set queued again with
// no strike against it; when the list did not reach back (covered: false) the gate stays (no second, paid
// create of the same set); when the job is found, complete() records it. And the sweep's own list reader
// (geminiImageProxy-background.js) answers covered only when the list really reaches back past the create.
// Usage: node tests/listing-batch/admission-wedge.cjs   (from the repository root)
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { admissionControl } = require(path.resolve('netlify/functions/lib/listingBatchAdmission.cjs'));

const clone = value => value == null ? value : structuredClone(value);
function database() {
  const data = new Map();
  const snapshot = p => ({ exists: data.has(p), id: p.split('/').pop(), data: () => clone(data.get(p)) });
  let serial = Promise.resolve();
  return {
    data,
    collection(name) {
      const query = (filters = []) => ({
        where: (key, op, val) => query([...filters, [key, op, val]]),
        limit: () => query(filters),
        get: async () => ({ docs: [...data].filter(([k, v]) => k.startsWith(name + '/') &&
          filters.every(([field, op, w]) => op === 'in' ? w.includes(v[field]) : v[field] === w)).map(([p]) => snapshot(p)) }),
      });
      return { ...query(), doc(id) { const p = name + '/' + id; return { path: p, get: async () => snapshot(p), set: async value => data.set(p, { ...data.get(p), ...clone(value) }) }; } };
    },
    runTransaction(fn) {
      const result = serial.then(() => fn({ get: ref => ref.get(), set: (ref, val) => { data.set(ref.path, { ...data.get(ref.path), ...clone(val) }); } }));
      serial = result.catch(() => {}); return result;
    },
  };
}
const SOURCE = 'batch_local_91ce2041f6847cbd38cdaa9061b830222587dd0e', OTHER = 'batch_local_91ce2042f6847cbd38cdaa9061b830222587dd0e';
const INPUT = 'file-Ab12Cd34Ef56Gh78';
const prepared = { inputFileName: INPUT, displayName: 'lg1-Regular_Necklace-630sets-part500of630', sets: [{ outputBasePath: 'x/Set_1800', tasks: ['paid request'] }] };
// A gate left in 'creating' `ageMin` minutes ago by a worker that never came back.
function stuck(ageMin = 20) {
  let now = Date.parse('2026-10-24T23:50:00Z');
  const db = database();
  const startedAt = now - ageMin * 60000;
  db.data.set('LG1_Config/batchAdmission', { owner: 'token-of-the-frozen-worker', sourceName: SOURCE, phase: 'creating',
    startedAt, inputFileName: INPUT, preparationStage: 'uploading the batch input file', probeName: null, lastError: null });
  for (const id of [SOURCE, OTHER]) db.data.set('batches/' + id, { batchName: id, state: 'JOB_STATE_QUEUED', locallyQueued: true,
    collected: false, retryRequested: true, retryStatus: id === SOURCE ? 'submitting' : undefined, preparationFailures: 0,
    ...(id === SOURCE ? { preparedSubmission: clone(prepared) } : {}) });
  const gate = admissionControl(db, 'batches', () => now, () => now);
  return { db, gate, startedAt, gateDoc: () => db.data.get('LG1_Config/batchAdmission'), source: () => db.data.get('batches/' + SOURCE) };
}

(async () => {
  // (1) the provider list reaches back past the create and the job is not there: it was never made.
  {
    const w = stuck(20), calls = [];
    await w.gate.reconcile(async (file, startedAt) => { calls.push([file, startedAt]); return { match: null, covered: true }; });
    assert.deepEqual(calls, [[INPUT, w.startedAt]], 'the provider list is asked for the input file, with the time the create began');
    assert.equal(w.gateDoc().phase, 'idle', 'the gate is released');
    assert.equal(w.gateDoc().owner, null);
    assert.equal(w.gateDoc().sourceName, null);
    assert.equal(w.source().retryRequested, true, 'the set is queued again');
    assert.equal(w.source().retryStatus, 'queued');
    assert.equal(w.source().preparedSubmission, null, 'its unconfirmed submission is dropped');
    assert.equal(w.source().retryBatchName, undefined, 'no job is recorded for it');
    assert.equal(w.source().preparationFailures, 0, 'no strike: nothing about the set failed');
    assert.equal(w.source().preparationInterruptions, undefined);
    assert(!(Number(w.source().retryNotBefore || 0) > 0), 'and it is not held back');
    assert.equal([...w.db.data.keys()].filter(k => k.startsWith('batches/')).length, 2, 'no job record was made up');
    const next = await w.gate.reserve(OTHER);
    assert(next.token, `admission goes on: the next queued set can be sent (got ${JSON.stringify(next)})`);
  }
  // ... but not before the create is CREATE_CONFIRM_MS old (OpenAI lists a new job within seconds; give it time)
  {
    const w = stuck(10);
    await w.gate.reconcile(async () => ({ match: null, covered: true }));
    assert.equal(w.gateDoc().phase, 'creating', 'a create ten minutes old is not given up yet');
    assert.equal(w.gateDoc().owner, 'token-of-the-frozen-worker');
  }
  // (2) the list did not reach back far enough: the job may exist, so nothing is sent again.
  {
    const w = stuck(20);
    await w.gate.reconcile(async () => ({ match: null, covered: false }));
    assert.equal(w.gateDoc().phase, 'creating', 'the gate stays: sending the set again could pay for it twice');
    assert.equal(w.gateDoc().owner, 'token-of-the-frozen-worker');
    assert.match(String(w.gateDoc().lastError), /unconfirmed/i, 'and says why admission is paused');
    assert.deepEqual(w.source().preparedSubmission, prepared, 'the submission is kept for the next check');
    assert.equal(w.source().retryStatus, 'submitting');
    assert.equal((await w.gate.reserve(OTHER)).queued, true, 'no other set is admitted meanwhile');
  }
  // (3) the job is there: it is recorded as the set's job and the gate moves on.
  {
    const w = stuck(20);
    const job = { id: 'batch_6b00a1b2c3d4e5f60718293a4b5c6d7e', input_file_id: INPUT, status: 'validating', created_at: 1792886400 };
    await w.gate.reconcile(async () => ({ match: job, covered: true }));
    const rec = w.db.data.get('batches/' + job.id);
    assert(rec, 'complete() wrote the job record');
    assert.equal(rec.state, 'JOB_STATE_PENDING');
    assert.deepEqual(rec.sets, prepared.sets, 'with the prepared submission');
    assert.deepEqual(rec.rawCreate, job);
    assert.equal(w.source().retryBatchName, job.id, 'the set points at its job');
    assert.equal(w.source().retryStatus, 'submitted');
    assert.equal(w.gateDoc().phase, 'idle');
    assert.equal(w.gateDoc().probeName, job.id, 'and the job is waited for while it validates');
  }

  // (4) the sweep's reader of OpenAI's job list (newest first, 100 a page, created_at in seconds)
  const src = fs.readFileSync(path.resolve('netlify/functions/geminiImageProxy-background.js'), 'utf8');
  const from = src.indexOf('await admission.reconcile(async (');
  const to = src.indexOf('}, stalledSubmission ?', from);
  assert(from > 0 && to > from, 'the sweep reconciles admission');
  const reader = src.slice(from + 'await admission.reconcile('.length, to + 1);
  const listWith = (pages) => {
    const asked = [];
    const fn = vm.runInNewContext(`(${reader})`, {
      batchApiKey: () => 'sk-test', encodeURIComponent,
      openAIBatchRequest: async (_key, url) => { asked.push(url); const after = /after=([^&]+)/.exec(url)?.[1];
        const i = after ? pages.findIndex(p => p.data.length && p.data[p.data.length - 1].id === decodeURIComponent(after)) + 1 : 0;
        return pages[i] || { data: [], has_more: false }; },
    });
    return { fn, asked };
  };
  const startedAt = Date.parse('2026-10-24T23:30:00Z');
  const sec = ms => Math.floor(ms / 1000);
  const page = (fromMs, n, extra = {}) => ({ has_more: true, ...extra,
    data: Array.from({ length: n }, (_, i) => ({ id: `batch_${fromMs}_${i}`, input_file_id: `file-other-${fromMs}-${i}`, created_at: sec(fromMs - i * 1000) })) });
  const plain = x => JSON.parse(JSON.stringify(x));
  {
    const pages = [page(startedAt + 15 * 60000, 100), page(startedAt + 5 * 60000, 100)];
    pages[1].data[40].input_file_id = INPUT;
    const { fn } = listWith(pages);
    const got = plain(await fn(INPUT, startedAt));
    assert.equal(got.match?.id, pages[1].data[40].id, 'the job is found on a later page');
  }
  {
    // newer jobs, then a page that reaches back to before the create (minus ten minutes): covered
    const pages = [page(startedAt + 15 * 60000, 100), page(startedAt - 5 * 60000, 100), page(startedAt - 20 * 60000, 100)];
    const { fn, asked } = listWith(pages);
    const got = plain(await fn(INPUT, startedAt));
    assert.deepEqual(got, { match: null, covered: true }, 'a list that reaches back past the create without the job covers it');
    assert.equal(asked.length, 3, 'it reads on until it reaches back far enough');
    const w = stuck(20);
    await w.gate.reconcile(fn);
    assert.equal(w.gateDoc().phase, 'idle', 'and with that answer the stuck gate is released');
  }
  {
    const { fn } = listWith([page(startedAt + 15 * 60000, 30, { has_more: false })]);
    assert.deepEqual(plain(await fn(INPUT, startedAt)), { match: null, covered: true }, 'the end of the list covers it too');
  }
  {
    // ten pages, every job newer than the create: the list never reached back, so nothing is concluded
    const pages = Array.from({ length: 12 }, (_, k) => page(startedAt + (60 - k) * 60000, 100));
    const { fn, asked } = listWith(pages);
    assert.deepEqual(plain(await fn(INPUT, startedAt)), { match: null, covered: false }, 'a list that does not reach back is not covered');
    assert(asked.length <= 10, 'and it stops after ten pages');
    const w = stuck(20);
    await w.gate.reconcile(fn);
    assert.equal(w.gateDoc().phase, 'creating', 'so the gate stays');
  }
  console.log('Listing batch admission wedge: a create that never reached OpenAI is released and re-queued with no strike, an unconfirmed one stays, a found one is recorded, the sweep\'s list reader covers only what it read passed');
})().catch(err => { console.error(err); process.exitCode = 1; });
