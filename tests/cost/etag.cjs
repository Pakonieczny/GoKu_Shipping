// FC15: netlify/functions/_etag.js — revision ETag, body-hash ETag, 304 with an empty body, weak compare, conditional() reads the
// revision first and builds only when stale. Pure functions, no network, no Firestore.   node tests/cost/etag.cjs
"use strict";
const assert = require("node:assert/strict"), path = require("node:path");
const E = require(path.join(__dirname, "../../netlify/functions/_etag.js"));
const ev = (h) => ({ headers: h || {} });
let n = 0; const ok = (name) => { n++; console.log("  ok  " + name); };
(async () => {
  // stable text: key order does not matter, undefined/functions skipped, arrays keep order
  assert.equal(E.stableStringify({ b: 1, a: [2, { d: 1, c: undefined }] }), E.stableStringify({ a: [2, { c: undefined, d: 1 }], b: 1 }));
  assert.notEqual(E.stableStringify([1, 2]), E.stableStringify([2, 1]));
  assert.equal(E.stableStringify(undefined), "null"); ok("stableStringify");

  // value tags: same value any order = same tag, a change = another tag, omit leaves timestamps out
  const a = E.etagOfValue({ x: 1, y: [1, 2], now: 5 }, { omit: ["now"] }), b = E.etagOfValue({ now: 9, y: [1, 2], x: 1 }, { omit: ["now"] });
  assert.equal(a, b); assert.notEqual(a, E.etagOfValue({ x: 2, y: [1, 2] })); assert.match(a, /^"h[\w-]{22}"$/); ok("etagOfValue");
  assert.equal(E.etagOfRevision(7), '"r7"'); assert.equal(E.etagOfRevision(7, "lib"), '"lib.r7"'); assert.equal(E.etagOfRevision(null), '"r0"');
  assert.notEqual(E.etagOfRevision(7, "a"), E.etagOfRevision(7, "b")); ok("etagOfRevision");

  // matches: exact, weak, list, star, none
  const t = E.etagOfRevision(3);
  assert.equal(E.matches(ev({ "If-None-Match": t }), t), true);
  assert.equal(E.matches(ev({ "if-none-match": "W/" + t }), t), true);
  assert.equal(E.matches(ev({ "IF-NONE-MATCH": '"zz", W/' + t + ' , "yy"' }), t), true);
  assert.equal(E.matches(ev({ "if-none-match": "*" }), t), true);
  assert.equal(E.matches(ev({ "if-none-match": '"r4"' }), t), false);
  assert.equal(E.matches(ev({}), t), false); assert.equal(E.matches({}, t), false); assert.equal(E.matches(ev({ "if-none-match": t }), ""), false); ok("matches");

  // 304: empty body, ETag, the caller's headers kept, content-type/length dropped
  const nm = E.notModified(t, { "Access-Control-Allow-Origin": "*", "Cache-Control": "no-store", "Content-Type": "application/json", "content-length": "5" });
  assert.equal(nm.statusCode, 304); assert.equal(nm.body, ""); assert.equal(nm.headers.ETag, t);
  assert.equal(nm.headers["Cache-Control"], "no-store"); assert.equal(nm.headers["Access-Control-Allow-Origin"], "*");
  assert.ok(!("Content-Type" in nm.headers) && !("content-length" in nm.headers)); ok("notModified");

  // conditional: revision first (one call), build only when stale, never when fresh
  let revReads = 0, builds = 0;
  const opts = (rev) => ({ revision: async () => { revReads++; return rev; }, build: async () => { builds++; return { sheets: [1, 2, 3], rev }; }, scope: "library", headers: { "Cache-Control": "no-store" } });
  let r = await E.conditional(ev({}), opts(5));
  assert.equal(r.statusCode, 200); assert.equal(builds, 1); assert.equal(revReads, 1); assert.deepEqual(JSON.parse(r.body), { sheets: [1, 2, 3], rev: 5 });
  assert.equal(r.headers.ETag, '"library.r5"'); assert.equal(r.headers["Content-Type"], "application/json");
  r = await E.conditional(ev({ "If-None-Match": '"library.r5"' }), opts(5));
  assert.equal(r.statusCode, 304); assert.equal(r.body, ""); assert.equal(builds, 1, "a fresh revision must not build"); assert.equal(revReads, 2);
  r = await E.conditional(ev({ "If-None-Match": '"library.r5"' }), opts(6));
  assert.equal(r.statusCode, 200); assert.equal(builds, 2); assert.equal(r.headers.ETag, '"library.r6"');
  await assert.rejects(() => E.conditional(ev({}), { revision: async () => { throw new Error("DEADLINE_EXCEEDED"); }, build: async () => ({}) }), /DEADLINE/);
  await assert.rejects(() => E.conditional(ev({}), {}), /needs revision/); ok("conditional");

  // revisionAnswer: tiny body, 304 when the caller has it
  r = E.revisionAnswer(ev({}), 9, { count: 3 });
  assert.equal(r.statusCode, 200); assert.deepEqual(JSON.parse(r.body), { rev: 9, count: 3 }); assert.ok(r.body.length < 40);
  r = E.revisionAnswer(ev({ "if-none-match": E.etagOfRevision(9) }), 9, { count: 3 });
  assert.equal(r.statusCode, 304); assert.equal(r.body, ""); ok("revisionAnswer");

  // withEtag: body hash; omit keeps a moving timestamp out; only 200 answers are tagged; never mutates the input
  const resp = { statusCode: 200, headers: { "Content-Type": "application/json" }, body: JSON.stringify({ a: 1, serverTime: 1 }) };
  const first = E.withEtag(ev({}), resp, { omit: ["serverTime"] });
  assert.equal(first.statusCode, 200); assert.ok(first.headers.ETag); assert.ok(!resp.headers.ETag, "input untouched"); assert.equal(first.body, resp.body);
  const again = E.withEtag(ev({ "if-none-match": first.headers.ETag }), { ...resp, body: JSON.stringify({ a: 1, serverTime: 999 }) }, { omit: ["serverTime"] });
  assert.equal(again.statusCode, 304); assert.equal(again.body, "");
  const moved = E.withEtag(ev({ "if-none-match": first.headers.ETag }), { ...resp, body: JSON.stringify({ a: 2, serverTime: 999 }) }, { omit: ["serverTime"] });
  assert.equal(moved.statusCode, 200);
  const err = { statusCode: 500, headers: {}, body: "x" }; assert.equal(E.withEtag(ev({}), err), err);
  const plain = E.withEtag(ev({}), resp), plain2 = E.withEtag(ev({ "if-none-match": plain.headers.ETag }), resp);
  assert.equal(plain2.statusCode, 304); ok("withEtag");

  // corsFor
  const c = E.corsFor({ "Access-Control-Allow-Headers": "Content-Type, X-Edit-Passcode" });
  assert.match(c["Access-Control-Allow-Headers"], /If-None-Match/); assert.match(c["Access-Control-Expose-Headers"], /ETag/);
  assert.equal(E.corsFor(c)["Access-Control-Allow-Headers"], c["Access-Control-Allow-Headers"]); ok("corsFor");
  console.log("etag.cjs: " + n + " groups passed");
})().catch((e) => { console.error(e); process.exit(1); });
