// Exercise durable Send to Sheet decisions against the real handler and atomic Firestore failures.
// No network, browser or shop data. Run: node tests/charm-nest/custom-send-api-resilience.cjs
"use strict";
const assert = require("node:assert/strict"), path = require("node:path"), Module = require("node:module");
const store = new Map(), writes = new Map(), metadataCalls = new Map(), reads = new Map();
let broadReads = 0;
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
    async get(mask) { reads.set(key, (reads.get(key) || 0) + 1); const d = store.get(key); return { exists: !!d, id, ref: this, data: () => d ? clone(mask ? Object.fromEntries(mask.filter(k => d[k] !== undefined).map(k => [k, d[k]])) : d) : undefined }; },
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
      broadReads++;
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
const order = "4170000990", line = order + "_60001", line2 = order + "_60002", ck = "custom:" + order + ":CUSTOM-N-001", at = Date.now() - 10000;
const original = {
  ck, rid: order, at: at - 1000, phase: "pending",
  files: [{ id: "file-custom-original", name: "Customer design.ai", kind: "ai", size: 2400, hash: "abcde01234567890123456789", cloud: { path: "charmnest/custom/" + order + "/original.pdf", url: "https://saved.example/original.pdf" }, metal: "gold", qty: 2, pieces: 1,
    wMm: 14, hMm: 18, maxPt: 52, minPt: 40, maxAreaPt2: 2000, state: "ready", bytes: [1, 2, 3, 4], thumb: "data:image/png;base64,AAAA" }],
  sent: { id: "custom-sheet:" + ck + ":" + at, at, by: "Paul", lines: { [line]: [{ f: "file-custom-original", i: 0 }], [line2]: [{ f: "file-custom-original", i: 0 }] } }
};
const copy = v => JSON.parse(JSON.stringify(v));
const cards = prefix => [...store.entries()].filter(([key]) => key.startsWith((prefix || "") + "Charm_Custom_Sheet/card-")).map(([key, value]) => ({ key, value }));
(async () => {
  const warn = console.warn, error = console.error; console.warn = () => {}; console.error = () => {};
  try {
    let result = await ok({ op: "customSheetPut", record: original });
    assert.equal(result.record.phase, "pending"); assert.equal(events().length, 0, "a durable intent cannot pretend the designs were sent before pooling acknowledged");
    assert.equal(cards().length, 1); assert.equal(cards()[0].value.files[0].bytes, undefined, "raw artwork bytes never enter Firestore");
    assert.equal(store.has("Charm_Custom_Orders/" + line), false, "Send to Sheet does not complete a custom order by hand");
    const pendingWrites = writes.get(cards()[0].key);
    await ok({ op: "customSheetPut", phase: "pending", record: original });
    assert.equal(writes.get(cards()[0].key), pendingWrites, "a lost pending reply retries the same immutable intent");

    let all = await ok({ op: "customSheetGet", keys: [ck, "custom:unknown"], lineKeys: [line] });
    assert.deepEqual(all.records[ck].sent, original.sent); assert.equal(all.records[ck].files[0].cloud.path, original.files[0].cloud.path);
    assert.equal(all.records[ck].phase, "pending"); assert.equal(broadReads, 0, "known keys and line indexes never scan the custom decision collection");
    const writesBeforeRead = [...writes];
    all = await ok({ op: "customSheetGet", lineKeys: [line2] });
    assert.equal(all.records[ck].ck, ck, "another device can recover the original card even if its current grouping key changed");
    assert.deepEqual([...writes], writesBeforeRead, "recovery lookup is read-only");

    const final = copy(original); final.phase = "sent";
    failTransaction = 1;
    let failed = await post({ op: "customSheetPut", record: final });
    assert.equal(failed.status, 500); assert.equal(cards()[0].value.phase, "pending"); assert.equal(events().length, 0, "a failed final transaction cannot save a sent decision without its history");
    timelineDown = true;
    failed = await post({ op: "customSheetPut", record: final });
    assert.equal(failed.status, 500); assert.equal(cards()[0].value.phase, "pending", "timeline failure keeps the durable send retryable");
    timelineDown = false;
    result = await ok({ op: "customSheetPut", record: final });
    assert.equal(result.record.phase, "sent"); assert.equal(events().length, 2);
    assert(events().every(e => e.type === "designSent" && e.by === "Paul" && e.at === at && e.data.decisionId === original.sent.id));
    assert.deepEqual(events().map(e => e.lineKey).sort(), [line, line2]);
    assert.deepEqual(result.record.history, [{ phase: "sent", id: original.sent.id, at, by: "Paul" }]);
    const sentWrites = writes.get(cards()[0].key), savedFacts = JSON.stringify(cards()[0].value);
    await ok({ op: "customSheetPut", record: final }); // Intentionally discard the original send reply, then repeat it.
    assert.equal(events().length, 2); assert.equal(writes.get(cards()[0].key), sentWrites); assert.equal(JSON.stringify(cards()[0].value), savedFacts);

    result = await ok({ op: "customSheetPut", record: original });
    assert.equal(result.record.phase, "sent", "a delayed pending request cannot reverse an acknowledged send");
    assert.equal(events().length, 2); assert.equal(writes.get(cards()[0].key), sentWrites);
    // Repair a missed historical event from the immutable saved facts instead of the retry caller's clock.
    const missingKey = [...store.keys()].find(key => isTimeline(key) && store.get(key).lineKey === line);
    store.delete(missingKey);
    await ok({ op: "customSheetPut", record: original });
    assert.equal(events().length, 2); assert.equal(store.get(missingKey).by, "Paul"); assert.equal(store.get(missingKey).at, at);

    for (const mutate of [r => { r.sent.by = "Seth"; }, r => { r.sent.at++; }, r => { r.sent.id += "-second"; }, r => { r.files[0].hash += "new"; }, r => { r.sent.lines[line] = []; }]) {
      const changed = copy(final); mutate(changed);
      const conflict = await post({ op: "customSheetPut", record: changed });
      assert.equal(conflict.status, 409, JSON.stringify(conflict)); assert.equal(JSON.stringify(cards()[0].value), savedFacts, "conflicts cannot replace the original signer, files or copy mapping");
    }
    const cleaned = copy(final); cleaned.sent.lines[line][0].removed = "cancelled by Paul";
    result = await ok({ op: "customSheetPut", record: cleaned });
    assert.equal(result.record.sent.lines[line][0].removed, "cancelled by Paul", "cleanup metadata can sync after a lost send reply");
    assert.deepEqual(result.record.sent.id, original.sent.id); assert.equal(result.record.sent.by, "Paul"); assert.equal(result.record.sent.at, at);
    assert.equal(result.record.history.length, 1); assert.equal(events().length, 2, "a cleanup neither restamps the send nor appends a new decision");
    const cleanupWrites = writes.get(cards()[0].key);
    result = await ok({ op: "customSheetPut", record: original });
    assert.equal(result.record.sent.lines[line][0].removed, "cancelled by Paul", "a stale retry cannot resurrect a removed copy");
    assert.equal(writes.get(cards()[0].key), cleanupWrites, "an old retry does not rewrite the preserved cleanup markers");
    const unremove = copy(final); unremove.sent.lines[line][0].removed = false;
    result = await ok({ op: "customSheetPut", record: unremove });
    assert.equal(result.record.sent.lines[line][0].removed, "cancelled by Paul", "an explicit false marker cannot remove historical cleanup facts");
    await ok({ op: "customSheetPut", record: cleaned });
    assert.equal(writes.get(cards()[0].key), cleanupWrites, "the exact cleanup reply can be retried without another write");

    const competing = copy(final); competing.ck += ":another-card"; competing.sent.id += ":another-card";
    failed = await post({ op: "customSheetPut", record: competing });
    assert.equal(failed.status, 409); assert.equal(cards().length, 1, "a different card cannot claim the same order line twice");

    const malformed = copy(original); malformed.sent.by = "";
    assert.equal((await post({ op: "customSheetPut", record: malformed })).status, 400);
    const noSource = copy(original); noSource.files[0].cloud = null;
    assert.equal((await post({ op: "customSheetPut", record: noSource })).status, 400, "a byte-free remote intent must have an artwork source it can recover");
    const wrongLine = copy(original); wrongLine.sent.lines = { "4170001000_60001": [{ f: original.files[0].id, i: 0 }] };
    assert.equal((await post({ op: "customSheetPut", record: wrongLine })).status, 400);
    const wrongCopy = copy(original); wrongCopy.sent.lines[line][0].i = 100;
    assert.equal((await post({ op: "customSheetPut", record: wrongCopy })).status, 400);
    assert.equal((await post({ op: "customSheetGet" })).status, 400, "there is no broad-list fallback");

    result = await ok({ op: "customSheetPut", record: final, sandbox: true });
    assert.equal(cards("Sandbox_").length, 1); assert.equal(cards().length, 1);
    assert.equal([...store.keys()].filter(key => key.startsWith("Sandbox_Order_Timeline/")).length, 2);
    assert.equal(result.record.sent.by, "Paul");
    await ok({ op: "customSheetGet", keys: [ck], sandbox: true });
    assert.equal(broadReads, 0); assert.equal([...store.keys()].some(key => key.includes("Charm_Pool/") || key.includes("Charm_Nest_Sheets/") || key.includes("Charm_Custom_Orders/")), false, "send persistence never changes nested copies, sheet stages, QR labels, engraving or completion");
    // No modern receipt or local customDesigns: the real old ledger is the only decision authority.
    const legacyOrder = "4174476673", legacyLine = legacyOrder + "_5218516944", legacyAt = at - 86400000, legacySheet = "sheet-custom-legacy", legacyPath = "charmnest/custom/" + legacyOrder + "/hash-Old_design.pdf";
    const legacyEventKey = "Sandbox_Order_Timeline/" + legacyOrder + "~designSent~" + legacyLine + "." + legacyAt;
    store.set(legacyEventKey, { orderId: legacyOrder, lineKey: legacyLine, type: "designSent", at: legacyAt, by: "Seth", source: "sorter", station: "sorter", data: { files: [{ name: "Old design.ai", qty: 2, pieces: 2, metal: "gold" }], pieces: 4, placed: true } });
    for (let i = 1; i <= 4; i++) store.set("Sandbox_Charm_Pool/" + legacyLine + "_" + i, { poolId: legacyLine + "_" + i, orderId: legacyOrder, lineKey: legacyLine, copy: i, quantity: 4, custom: true, customFile: "Old design.ai", aiPath: legacyPath, material: "gold", sheetId: legacySheet, state: "written" });
    store.set("Sandbox_Charm_Nest_Sheets/" + legacySheet, { id: legacySheet, sources: [{ id: "source-legacy", custom: true, hash: "original-file-hash", path: legacyPath, url: "https://saved.example/legacy.pdf?Expires=1", bytes: 3456 }], charms: Array.from({ length: 4 }, (_, i) => ({ poolId: legacyLine + "_" + (i + 1), custom: true, sourceId: "source-legacy", aiPath: legacyPath, index: i % 2, widthPt: 30, heightPt: 20, areaPt2: 450 })) });
    const beforeLegacyRead = [...writes];
    all = await ok({ op: "customSheetGet", legacyLineKeys: [legacyLine], sandbox: true });
    let legacy = Object.values(all.records)[0]; assert(legacy);
    assert.equal(legacy.legacy, true); assert.equal(legacy.phase, "sent"); assert.equal(legacy.sent.id, legacyEventKey.split("/")[1]);
    assert.equal(legacy.sent.at, legacyAt); assert.equal(legacy.sent.by, "Seth", "restore uses the original recorded decision, never the current reviewer or today's time");
    assert.deepEqual(legacy.sent.lines[legacyLine].map(pc => pc.i), [0, 1, 0, 1]); assert.equal(legacy.files[0].cloud.path, legacyPath); assert.equal(legacy.files[0].cloud.url, "", "legacy preview resolves the authoritative cloud path instead of reusing an expired signed URL");
    assert.equal(legacy.files[0].hash, "original-file-hash"); assert.deepEqual([...writes], beforeLegacyRead, "legacy lookup creates no receipt, timeline, pool, sheet, approval or completion writes");
    const removedPool = store.get("Sandbox_Charm_Pool/" + legacyLine + "_2"); removedPool.removedAt = at; removedPool.removedReason = "custom cleanup";
    legacy = Object.values((await ok({ op: "customSheetGet", legacyLineKeys: [legacyLine], sandbox: true })).records)[0];
    assert.equal(legacy.sent.lines[legacyLine][1].removed, "custom cleanup", "historical cleanup survives browser-independent reconstruction");
    const savedEvent = store.get(legacyEventKey); store.delete(legacyEventKey);
    store.set("Sandbox_Order_Timeline/" + legacyOrder + "~placed~old", { orderId: legacyOrder, lineKey: legacyLine, type: "placed", at, by: "Paul", sheetId: legacySheet });
    all = await ok({ op: "customSheetGet", legacyLineKeys: [legacyLine], sandbox: true });
    assert.equal(Object.keys(all.records).length, 0, "a normal custom placement with no actual Send to Sheet event cannot fabricate a send decision");
    store.set(legacyEventKey, savedEvent);
    store.get("Sandbox_Charm_Nest_Sheets/" + legacySheet).charms[0].index = 1;
    assert.equal(Object.keys((await ok({ op: "customSheetGet", legacyLineKeys: [legacyLine], sandbox: true })).records).length, 0, "inconsistent multi-piece geometry is not silently assigned a made-up index");
    store.get("Sandbox_Charm_Nest_Sheets/" + legacySheet).charms[0].index = 0;
    const missingPoolKey = "Sandbox_Charm_Pool/" + legacyLine + "_4", savedPool = store.get(missingPoolKey); store.delete(missingPoolKey);
    assert.equal(Object.keys((await ok({ op: "customSheetGet", legacyLineKeys: [legacyLine], sandbox: true })).records).length, 0, "a partial old pool cannot fabricate missing sent copies");
    store.set(missingPoolKey, savedPool);
    store.get(missingPoolKey).aiPath = null;
    assert.equal(Object.keys((await ok({ op: "customSheetGet", legacyLineKeys: [legacyLine], sandbox: true })).records).length, 0, "a historic decision alone is insufficient without recoverable uploaded artwork evidence");
    store.get(missingPoolKey).aiPath = legacyPath;
    savedEvent.by = "";
    assert.equal(Object.keys((await ok({ op: "customSheetGet", legacyLineKeys: [legacyLine], sandbox: true })).records).length, 0, "no historical signer is guessed from the later placement"); savedEvent.by = "Seth";
    assert.equal(Object.keys((await ok({ op: "customSheetGet", keys: [], sandbox: true })).records).length, 0, "legacy scans require explicit candidate line keys");
    assert.deepEqual([...writes], beforeLegacyRead);
    reads.clear();
    const keys = Array.from({ length: 1010 }, (_, i) => "custom:bounded:" + i);
    all = await ok({ op: "customSheetGet", keys });
    assert.equal(all.keys, 1000); assert.equal(all.truncated, true); assert.equal(reads.size, 1000, "known-key reads are bounded and report omitted keys");
    console.log("Custom Send API resilience OK: durable intent, atomic sent history, exact retry, original signer/time/copy mapping, line lookup, monotonic cleanup metadata, conflict protection, sandbox isolation, evidence-backed legacy recovery and bounded read-only recovery");
  } finally { console.warn = warn; console.error = error; }
})().catch(error => { console.error(error); process.exitCode = 1; }).finally(() => {
  Module._load = originalLoad;
  if (originalPasscode === undefined) delete process.env.EDIT_PASSCODE; else process.env.EDIT_PASSCODE = originalPasscode;
});
