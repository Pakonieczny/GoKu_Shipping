/* ETSYSTOP (Paul, 20 Nov 2026: "Once a 429 is reached the application must stop all work related to Etsy" … "a manual reset … all the other
   computers should comply through the cloud"). Real function handlers, a fake Etsy and one shared fake cloud switch:
   A  the first 429 from Etsy turns the shop's switch on; after it NOTHING reaches Etsy, from that function or any other (another computer's
      function instance reads the same cloud switch); other addresses still pass
   B  the shared limiter never asks Etsy again after a 429
   C  Resume Etsy turns every function back on; a 429 after it stops everything again
   D  every function file that calls Etsy goes through the switch */
"use strict";
const assert = require("assert"), path = require("path"), fs = require("fs"), Module = require("module");
const fnDir = path.join(__dirname, "../../netlify/functions");
const realFetch = require("node-fetch");

// ── the cloud switch, one for every "computer" (a Firestore document)
let cloudDoc = null;
const cloud = { async get() { return cloudDoc ? JSON.parse(JSON.stringify(cloudDoc)) : null; }, async set(p) { cloudDoc = Object.assign(cloudDoc || {}, JSON.parse(JSON.stringify(p))); } };

// ── a fake Etsy: counts what reaches it; answers 429 while `limit` is on
const etsy = { calls: 0, other: 0, limit: false };
const fakeFetch = Object.assign(async (url, init) => {
  const u = String(url);
  if (/etsy\.com/.test(u)) { etsy.calls++; if (etsy.limit) return new realFetch.Response('{"error":"Exceeded daily rate limit"}', { status: 429, headers: { "content-type": "application/json" } }); return new realFetch.Response(JSON.stringify({ receipt_id: 1, results: [], count: 0 }), { status: 200, headers: { "content-type": "application/json" } }); }
  etsy.other++; return new realFetch.Response("{}", { status: 200 });
}, { Response: realFetch.Response, Headers: realFetch.Headers });
const realLoad = Module._load;
Module._load = function (req, ...rest) { if (req === "node-fetch") return fakeFetch; return realLoad.call(this, req, ...rest); };
global.fetch = fakeFetch;
process.env.SHOP_ID = "1"; process.env.CLIENT_ID = "k"; process.env.CLIENT_SECRET = "s";

// two "computers": each loads the functions fresh (its own module copies and its own few-second cache), both read the same cloud
function computer() {
  for (const k of Object.keys(require.cache)) if (k.startsWith(fnDir)) delete require.cache[k];
  const stop = require(path.join(fnDir, "_etsyStop.js")); stop._setStore(cloud);
  return { stop, order: require(path.join(fnDir, "etsyOrderProxy.js")), limiter: require(path.join(fnDir, "etsyRateLimiter.js")), open: require(path.join(fnDir, "listOpenOrders.js")) };
}
const orderEvent = id => ({ httpMethod: "GET", queryStringParameters: { orderId: String(id) }, headers: { "access-token": "t" } });
const results = []; const check = (ok, what, data) => { results.push([!!ok, what]); console.log(`${ok ? "PASS" : "FAIL"}  ${what}${ok ? "" : "  " + JSON.stringify(data)}`); };

(async () => {
  const A = computer(), B = computer();
  // A — running, then Etsy's first 429
  let r = await A.order.handler(orderEvent(11));
  check(r.statusCode === 200 && etsy.calls === 1, "A0 Etsy runs: an order is read (1 call)", { r: r.statusCode, calls: etsy.calls });
  etsy.limit = true;
  r = await A.order.handler(orderEvent(12));
  check(r.statusCode === 429 && etsy.calls === 2, "A1 Etsy answers 429 (the call that reached it)", { r: r.statusCode, calls: etsy.calls });
  check(cloudDoc && cloudDoc.stopped === true && cloudDoc.fn === "etsyOrderProxy" && /receipts\/12$/.test(cloudDoc.path) && !/\?/.test(cloudDoc.path) && /daily/.test(cloudDoc.detail), "A2 the shop's switch is on in the cloud, with the function, the path (no query) and Etsy's words", cloudDoc);
  etsy.limit = false;   // (even when Etsy would answer again, nothing goes until a person resumes)
  const before = etsy.calls;
  for (let i = 0; i < 20; i++) r = await A.order.handler(orderEvent(100 + i));
  check(etsy.calls === before && r.statusCode === 429 && /etsy_stopped/.test(r.body), "A3 the same function: 20 more orders, none reaches Etsy, each answered 429 etsy_stopped", { calls: etsy.calls - before, r });
  r = await B.order.handler(orderEvent(200));
  const lim = await B.limiter.etsyFetch("https://openapi.etsy.com/v3/application/listings/5", {});
  check(etsy.calls === before && r.statusCode === 429 && lim.status === 429 && B.stop.isStopAnswer(lim), "A4 another computer's functions (the order proxy, the shared limiter) obey the same cloud switch", { calls: etsy.calls - before, r: r.statusCode, lim: lim.status });
  const oo = await B.open.handler({ httpMethod: "GET", queryStringParameters: {}, headers: { "access-token": "t" } });
  check(etsy.calls === before && oo && oo.statusCode >= 400, "A5 the open-orders list (the built-in fetch) does not reach Etsy either", { calls: etsy.calls - before, status: oo && oo.statusCode });
  const other = etsy.other; const g = A.stop.guard(fakeFetch, "test"); await g("https://brites-charm-sorter.goldenspike.app/.netlify/functions/x");
  check(etsy.other === other + 1, "A6 an address that is not Etsy still passes", { other: etsy.other - other });

  // B — the limiter never asks Etsy again after a 429
  cloudDoc = null; const C = computer(); etsy.limit = true; const c0 = etsy.calls;
  const t0 = Date.now(), res = await C.limiter.etsyFetch("https://openapi.etsy.com/v3/application/listings/9", {});
  check(res.status === 429 && etsy.calls === c0 + 1 && Date.now() - t0 < 1500 && cloudDoc.stopped === true, "B1 the shared limiter: one call, Etsy's 429 returned at once (no retries, no waiting), the switch on", { calls: etsy.calls - c0, ms: Date.now() - t0, cloudDoc });

  // C — Resume Etsy (a press in Settings on one computer) reaches every computer
  etsy.limit = false;
  const st = await C.stop.resume("Paul");
  check(st.stopped === false && cloudDoc.stopped === false && cloudDoc.resumedBy === "Paul", "C1 Resume Etsy turns the cloud switch off", cloudDoc);
  A.stop._clearCache(); B.stop._clearCache();   // (each computer reads the switch again within a few seconds; the test does not wait for it)
  const c1 = etsy.calls; r = await A.order.handler(orderEvent(300)); const r2 = await B.order.handler(orderEvent(301));
  check(r.statusCode === 200 && r2.statusCode === 200 && etsy.calls === c1 + 2, "C2 both computers call Etsy again after the press", { a: r.statusCode, b: r2.statusCode, calls: etsy.calls - c1 });
  etsy.limit = true; r = await B.order.handler(orderEvent(302)); etsy.limit = false; const c2 = etsy.calls; A.stop._clearCache(); r = await A.order.handler(orderEvent(303));
  check(cloudDoc.stopped === true && etsy.calls === c2 && r.statusCode === 429, "C3 a new 429 after the resume stops everything again", { cloudDoc, calls: etsy.calls - c2 });
  const sinceA = cloudDoc.since; etsy.limit = true; await new Promise(r => setTimeout(r, 5)); await A.stop.stop({ fn: "later" });
  check(cloudDoc.since === sinceA && cloudDoc.fn !== "later", "C4 a later 429 keeps the first stop's time and cause", cloudDoc);

  // D — every function file that calls Etsy goes through the switch
  const files = fs.readdirSync(fnDir).filter(f => f.endsWith(".js") && /openapi\.etsy\.com|api\.etsy\.com/.test(fs.readFileSync(path.join(fnDir, f), "utf8")));
  const bad = files.filter(f => { const s = fs.readFileSync(path.join(fnDir, f), "utf8"); const via = /_etsyStop"\)\.guard\(|require\("\.\/etsyRateLimiter"\)/.test(s), own = /fetchFn = require\("\.\/_etsyStop"\)\.guard\(fetchFn/.test(s), bare = /require\(['"]node-fetch['"]\)/.test(s.replace(/_etsyStop"\)\.guard\(require\("node-fetch"\)/g, "")); return !via || (bare && !own); });
  check(files.length >= 27 && !bad.length, `D1 all ${files.length} function files that call Etsy go through the switch (none with a fetch of its own)`, bad);
  const lim2 = fs.readFileSync(path.join(fnDir, "etsyRateLimiter.js"), "utf8");
  check(/_etsyStop"\)\.guard\(require\("node-fetch"\), "etsyRateLimiter"\)/.test(lim2), "D2 the shared limiter's own fetch is the guarded one", null);

  const failed = results.filter(x => !x[0]).length;
  console.log(failed ? `FAILED: ${failed}` : `etsy-stop OK · ${results.length} checks`);
  process.exit(failed ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
