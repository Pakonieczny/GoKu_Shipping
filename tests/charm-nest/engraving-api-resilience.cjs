// Exercise the real engraving/sheet handlers with atomic in-memory Firestore and controlled Storage faults.
// No network, browser or shop data. Run: node tests/charm-nest/engraving-api-resilience.cjs
"use strict";
const assert = require("node:assert/strict"), path = require("node:path"), Module = require("node:module");
const store = new Map(), writes = new Map(), metadataCalls = new Map();
const SERVER_TS = { __serverTimestamp: true }, DELETED = { __deleted: true };
const timestamp = at => ({ toMillis: () => at });
const plain = v => v && typeof v === "object" && !Array.isArray(v) && !v.toMillis && v !== SERVER_TS && v !== DELETED;
const clone = v => Array.isArray(v) ? v.map(clone) : plain(v) ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, clone(x)])) : v;
function merge(into, patch, deep = true) {
  for (const [k, v] of Object.entries(patch)) {
    if (v === DELETED) delete into[k];
    else if (v === SERVER_TS) into[k] = timestamp(Date.now());
    else if (deep && plain(v) && Object.keys(v).length && plain(into[k])) into[k] = merge(clone(into[k]), v);
    else into[k] = clone(v);
  }
  return into;
}
let timelineDown = false, failTransaction = 0;
const isTimeline = key => /^(Sandbox_)?Order_Timeline\//.test(key);
function commit(ops) {
  if (timelineDown && ops.some(([key]) => isTimeline(key))) throw Error("DEADLINE_EXCEEDED: timeline unavailable");
  const next = new Map(store);
  for (const [key, data, options] of ops) {
    if (data === DELETED) next.delete(key);
    else next.set(key, merge(options?.merge ? clone(next.get(key) || {}) : {}, data, !!options?.merge));
  }
  for (const [key] of ops) writes.set(key, (writes.get(key) || 0) + 1);
  store.clear(); for (const [key, value] of next) store.set(key, value);
}
function ref(collection, id) {
  const key = collection + "/" + id;
  return {
    id, path: key, parent: { id: collection }, collection: name => query(key + "/" + name),
    async get(mask) { const d = store.get(key); return { exists: !!d, id, ref: this, data: () => d ? clone(mask ? Object.fromEntries(mask.filter(k => d[k] !== undefined).map(k => [k, d[k]])) : d) : undefined }; },
    async set(data, options) { commit([[key, data, options]]); },
    async delete() { commit([[key, DELETED]]); }
  };
}
function query(collection, filters = [], limit = 0, mask = null, after = null) {
  return {
    doc: id => ref(collection, id), where: (field, op, value) => query(collection, filters.concat([[field, op, value]]), limit, mask, after),
    select: (...fields) => query(collection, filters, limit, fields, after), orderBy() { return this; },
    startAfter: id => query(collection, filters, limit, mask, id), limit: n => query(collection, filters, n, mask, after),
    async get() {
      let keys = [...store.keys()].filter(k => k.startsWith(collection + "/") && !k.slice(collection.length + 1).includes("/")).sort();
      if (after) keys = keys.filter(k => k.slice(collection.length + 1) > after);
      keys = keys.filter(k => filters.every(([field, op, value]) => {
        const saved = store.get(k)[field];
        return op === "==" ? saved === value : op === "in" ? value.includes(saved) : op === "array-contains-any" ? (saved || []).some(x => value.includes(x)) : op === "array-contains" ? (saved || []).includes(value) : false;
      }));
      if (limit) keys = keys.slice(0, limit);
      const docs = await Promise.all(keys.map(k => ref(collection, k.slice(collection.length + 1)).get(mask)));
      return { docs, size: docs.length, empty: !docs.length };
    }
  };
}
const db = {
  collection: name => query(name),
  async getAll(...refs) { const options = refs.at(-1)?.fieldMask ? refs.pop() : null; return Promise.all(refs.map(r => r.get(options?.fieldMask))); },
  async runTransaction(fn) {
    const ops = [], result = await fn({ get: r => r.get(), getAll: (...refs) => db.getAll(...refs), set: (r, d, o) => ops.push([r.path, d, o]), delete: r => ops.push([r.path, DELETED]) });
    if (failTransaction) { failTransaction--; throw Error("UNAVAILABLE: transaction commit failed"); }
    commit(ops); return result;
  },
  batch() { const ops = []; return { set: (r, d, o) => ops.push([r.path, d, o]), delete: r => ops.push([r.path, DELETED]), async commit() { commit(ops); } }; }
};
let metadata = async () => ({ metadata: { firebaseStorageDownloadTokens: "fresh-token,another-token" } });
const movedFiles = [];
const bucket = { name: "controlled-bucket", file(filePath) { return {
  async getMetadata() { metadataCalls.set(filePath, (metadataCalls.get(filePath) || 0) + 1); return [await metadata(filePath)]; },
  async move(to) { movedFiles.push([filePath, to]); }
}; } };
const admin = {
  firestore: Object.assign(() => db, { FieldValue: { serverTimestamp: () => SERVER_TS, delete: () => DELETED, increment: n => ({ __inc: n }) }, Timestamp: { fromMillis: timestamp }, FieldPath: { documentId: () => "__name__" } }),
  storage: () => ({ bucket: () => bucket })
};
const originalLoad = Module._load;
Module._load = function (name, ...args) {
  if (name === "./firebaseAdmin" || name === "firebase-admin" || /\/firebaseAdmin(?:\.js)?$/.test(name)) return admin;
  if (name === "node-fetch") return async () => { throw Error("No network in this fixture"); };
  return originalLoad.call(this, name, ...args);
};
const lib = require(path.resolve(__dirname, "../../netlify/functions/charmNestLibrary.js"));
const originalPasscode = process.env.EDIT_PASSCODE; delete process.env.EDIT_PASSCODE;
const post = async body => { const response = await lib.handler({ httpMethod: "POST", headers: {}, body: JSON.stringify(body) }); return { status: response.statusCode, body: JSON.parse(response.body) }; };
const ok = async body => { const out = await post(body); assert.equal(out.status, 200, JSON.stringify(out)); return out.body; };
const events = () => [...store.entries()].filter(([key]) => isTimeline(key)).map(([, value]) => value);
const waitTick = () => new Promise(resolve => setImmediate(resolve));
const order = "4170000700", line = order + "_60001", poolId = line + "_1", runId = "run-api-resilience", sheetId = "sheet-api-resilience";
const T = Date.now() - 10000;
const makeBack = (at, by, text) => ({
  poolId, sheetId, runId, order, transactionId: "60001", copy: 1, text, lines: [text], approvedAt: at, approvedBy: by,
  verified: { geometry: { ok: true }, file: { ok: true } }, metrics: { minimumGap: 0.25 },
  outputs: { ai: { path: `charmnest/backs/${at}.ai`, url: "https://saved.example/" + at + ".ai" }, png: { path: `charmnest/backs/${at}.png`, url: "https://saved.example/" + at + ".png" } }
});

(async () => {
  store.set("Charm_Nest_Sheets/" + sheetId, {
    id: sheetId, runId, metal: "silver", poolIds: [poolId], orders: [order], placedCount: 1, verification: { ok: true },
    status: "complete", backPool: [], outputs: { ai: { url: "https://saved.example/front.ai" }, preview: { url: "https://saved.example/front.png" } },
    label: { files: [{ path: "charmnest/labels/front.png", url: "https://saved.example/label.png", payload: "real-label-payload", orders: [order] }] }
  });
  store.set("Charm_Nest_Runs/" + runId, { runId, lines: { [line]: { key: line, orderId: order, poolIds: [poolId], state: "written", engrave: { needed: true, state: "placement", approved: false } } } });
  const first = makeBack(T, "Paul", "Love");
  let saved = await ok({ op: "backPut", back: first });
  assert.equal(saved.written, 1); assert.equal(store.get("Charm_Nest_Sheets/" + sheetId).backPool[0].approvedAt, T);
  assert.equal(store.get("Charm_Pool_Back/" + poolId).approvedBy, "Paul", "approval and exact sheet membership commit together");
  const firstWrites = writes.get("Charm_Pool_Back/" + poolId);
  saved = await ok({ op: "backPut", back: first });
  assert.equal(saved.skipped, 1); assert.equal(writes.get("Charm_Pool_Back/" + poolId), firstWrites);
  assert.equal(events().length, 1, "a lost-response retry preserves one timeline event and one seal");

  // Simulate a page closing before its separate run save: authoritative back/sheet records still recover the approval.
  const recovered = (await ok({ op: "getSheet", id: sheetId })).sheet;
  assert.equal(recovered.backPool[0].metrics.minimumGap, 0.25, "full edit details are restored from the back record");
  assert.equal(recovered.backPool[0].approvedBy, "Paul"); assert.equal(recovered.laser.approved, 1); assert.equal(recovered.laser.waiting, 0);
  assert.equal(recovered.laser.ready, true, "a stale pending run line cannot erase a saved verified back approval");
  assert.equal((await ok({ op: "backList", runId })).backs[0].approvedAt, T);
  assert.equal(store.get("Charm_Nest_Runs/" + runId).lines[line].engrave.approved, false, "read-only recovery does not fake a run write");

  const edit = makeBack(T + 1000, "Seth", "Love forever");
  await ok({ op: "backPut", back: edit, expectedApprovedAt: T }); // The original answer is deliberately discarded.
  const beforeRetry = writes.get("Charm_Pool_Back/" + poolId);
  saved = await ok({ op: "backPut", back: edit, expectedApprovedAt: T });
  assert.equal(saved.skipped, 1, "the exact committed edit retries successfully with its original prior expectation");
  assert.equal(writes.get("Charm_Pool_Back/" + poolId), beforeRetry);
  assert.equal(events().length, 2, "retry does not stamp the edited approval twice");
  let conflict = await post({ op: "backPut", back: { ...edit, text: "a genuinely different edit" }, expectedApprovedAt: T });
  assert.equal(conflict.status, 500); assert.match(conflict.body.error, /edited elsewhere/);
  conflict = await post({ op: "backPut", back: makeBack(T + 2000, "Paul", "stale editor"), expectedApprovedAt: T });
  assert.equal(conflict.status, 500); assert.equal(store.get("Charm_Pool_Back/" + poolId).text, "Love forever");
  conflict = await post({ op: "backPut", back: first }); assert.equal(conflict.status, 500); assert.match(conflict.body.error, /superseded/);
  const history = store.get("Charm_Pool_Back/" + poolId).engravingSeals;
  assert.deepEqual(history.map(s => [s.at, s.by]), [[T, "Paul"], [T + 1000, "Seth"]], "every original signer and timestamp survives edits");
  assert.equal(movedFiles.length, 2, "prior immutable file outputs are archived once");

  const sheetBeforeFailure = JSON.stringify(store.get("Charm_Nest_Sheets/" + sheetId)), backBeforeFailure = JSON.stringify(store.get("Charm_Pool_Back/" + poolId));
  failTransaction = 1;
  const retryable = makeBack(T + 3000, "Paul", "transaction retry");
  let failed = await post({ op: "backPut", back: retryable, expectedApprovedAt: edit.approvedAt });
  assert.equal(failed.status, 500); assert.equal(JSON.stringify(store.get("Charm_Nest_Sheets/" + sheetId)), sheetBeforeFailure);
  assert.equal(JSON.stringify(store.get("Charm_Pool_Back/" + poolId)), backBeforeFailure, "a failed commit cannot save only one side");
  await ok({ op: "backPut", back: retryable, expectedApprovedAt: edit.approvedAt });
  assert.equal(store.get("Charm_Pool_Back/" + poolId).approvedAt, retryable.approvedAt);

  timelineDown = true;
  const originalWarn = console.warn; console.warn = () => {};
  const afterTimelineFault = makeBack(T + 4000, "Seth", "timeline retry");
  try { await ok({ op: "backPut", back: afterTimelineFault, expectedApprovedAt: retryable.approvedAt }); }
  finally { timelineDown = false; console.warn = originalWarn; }
  assert.equal(store.get("Charm_Pool_Back/" + poolId).approvedAt, afterTimelineFault.approvedAt, "optional timeline failure cannot erase the actual approval");
  await ok({ op: "backPut", back: afterTimelineFault, expectedApprovedAt: retryable.approvedAt });
  assert.equal(events().filter(e => e.at === afterTimelineFault.approvedAt).length, 1, "idempotent retry repairs a missed timeline write");

  const missing = await post({ op: "backPut", back: { ...afterTimelineFault, sheetId: "wrong-sheet", approvedAt: T + 5000 } });
  assert.equal(missing.status, 500); assert.match(missing.body.error, /exact charm copy/);
  const preservedHistory = store.get("Charm_Pool_Back/" + poolId).engravingSeals.length;
  await ok({ op: "backInvalidate", poolIds: [poolId] });
  assert.equal(store.get("Charm_Nest_Sheets/" + sheetId).backPool.length, 0);
  assert.equal(store.get("Charm_Pool_Back/" + poolId).engravingSeals.length, preservedHistory, "reopening retains all historical seals");
  failed = await post({ op: "backPut", back: afterTimelineFault }); assert.equal(failed.status, 500, "a retry cannot revive an invalidated approval");

  const urlSheetId = "sheet-url-resilience", sharedHash = "abcdef0123456789";
  store.set("Charm_Nest_Sheets/" + urlSheetId, {
    id: urlSheetId, charms: Array.from({ length: 85 }, (_, i) => ({ id: "charm-" + i, hash: sharedHash, thumbUrl: "https://old.example/shared.png", aiUrl: "https://old.example/shared.ai" })),
    outputs: { preview: { path: "charmnest/charms/" + sharedHash + ".png", url: "https://old.example/preview.png" } }, poolIds: [], backPool: []
  });
  metadataCalls.clear();
  let refreshed = (await ok({ op: "getSheet", id: urlSheetId })).sheet;
  assert.equal(metadataCalls.size, 2); assert([...metadataCalls.values()].every(n => n === 1), "85 equal charms and the output share one metadata read per unique path");
  assert(refreshed.charms.every(c => c.thumbUrl === refreshed.outputs.preview.url));
  assert.match(refreshed.charms[0].thumbUrl, /token=fresh-token$/); assert(!refreshed.charms[0].thumbUrl.includes("another-token"));
  metadata = async () => { throw Object.assign(Error("UNAVAILABLE: optional metadata"), { code: 503 }); };
  refreshed = (await ok({ op: "getSheet", id: urlSheetId })).sheet;
  assert.equal(refreshed.charms[0].thumbUrl, "https://old.example/shared.png"); assert.equal(refreshed.outputs.preview.url, "https://old.example/preview.png", "lookup failure preserves existing file URLs");

  metadata = async () => ({ metadata: {} });
  refreshed = (await ok({ op: "getSheet", id: urlSheetId })).sheet;
  assert.equal(refreshed.charms[0].aiUrl, "https://old.example/shared.ai", "a missing token never replaces a usable URL with null");

  const hangSheetId = "sheet-slow-metadata";
  store.set("Charm_Nest_Sheets/" + hangSheetId, {
    id: hangSheetId, outputs: { ai: { path: "charmnest/hangs.ai", url: "https://old.example/hangs.ai" } },
    stock: { wPt: 300, hPt: 150 }, placements: [{ id: "real-placement", xPt: 20, yPt: 30 }], poolIds: [], backPool: [],
    charms: Array.from({ length: 80 }, (_, i) => ({ id: "late-" + i, hash: "hash" + i, thumbUrl: "https://old.example/" + i + ".png" }))
  });
  const releases = []; let active = 0, peak = 0;
  metadataCalls.clear();
  metadata = async () => {
    active++; peak = Math.max(peak, active);
    await new Promise(resolve => { releases.push(resolve); });
    active--; return { metadata: { firebaseStorageDownloadTokens: "late-token" } };
  };
  const began = Date.now();
  refreshed = (await lib.ops.getSheet({ id: hangSheetId })).sheet;
  const elapsed = Date.now() - began;
  assert(elapsed >= 1300 && elapsed < 3000, "optional links have a 1500 ms total budget instead of waiting indefinitely: " + elapsed);
  assert(peak <= 12, "metadata enrichment has bounded concurrency: " + peak);
  assert.equal(metadataCalls.size, 12, "unstarted metadata work remains queued when the shared deadline expires");
  assert.deepEqual(refreshed.placements, [{ id: "real-placement", xPt: 20, yPt: 30 }]);
  assert.equal(refreshed.outputs.ai.url, "https://old.example/hangs.ai", "the deadline never removes mandatory file evidence");
  const beforeLateResponse = JSON.stringify(refreshed), callsAtDeadline = [...metadataCalls.values()].reduce((n, x) => n + x, 0);
  releases.forEach(release => release()); await waitTick(); await waitTick();
  assert.equal(JSON.stringify(refreshed), beforeLateResponse, "a late metadata result cannot mutate the finished answer");
  assert.equal([...metadataCalls.values()].reduce((n, x) => n + x, 0), callsAtDeadline, "the expired refresh never starts another lookup");
  metadata = async () => ({ metadata: { firebaseStorageDownloadTokens: "recovered-token" } });
  refreshed = (await ok({ op: "getSheet", id: hangSheetId })).sheet;
  assert.match(refreshed.outputs.ai.url, /token=recovered-token$/, "the next read can refresh successfully after a transient timeout");
  assert.equal(store.get("Charm_Nest_Sheets/" + hangSheetId).outputs.ai.url, "https://old.example/hangs.ai", "reads never persist token substitutions or alter geometry");
  console.log("Engraving API resilience OK: atomic records, lost-response edit retry, strict conflicts, immutable history, optional timeline retry, exact membership, invalidation, deduplicated links, bounded metadata and recovery");
})().catch(error => { console.error(error); process.exitCode = 1; }).finally(() => {
  Module._load = originalLoad;
  if (originalPasscode === undefined) delete process.env.EDIT_PASSCODE; else process.env.EDIT_PASSCODE = originalPasscode;
});
