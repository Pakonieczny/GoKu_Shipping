// FC15 adoption 2: design.html's completion check (the Design page asks the completion ledger about the open orders on screen) now rides
// cn-poll.js with the SAME wait schedule it always had (0.75 s ... 30 s), and a tab nobody can see asks nothing. The REAL code is cut out of
// design.html and run against the fake world of tests/cost/_pollWorld.cjs (virtual clock, fake tab, fake fetch). No browser, no network.
//   node tests/cost/design-completion-poll.cjs
"use strict";
const assert = require("node:assert/strict"), fs = require("node:fs"), path = require("node:path"), vm = require("node:vm");
const { makeWorld, makeTab, CnPoll } = require("./_pollWorld.cjs");
const src = fs.readFileSync(path.join(__dirname, "../../design.html"), "utf8");
const a = src.indexOf("const COMPLETE_STEPS"), b = src.indexOf("/* ═══ 10 · STAFF NOTES");
assert(a > 0 && b > a, "the completion block is where the test expects it");
const block = src.slice(a, b);
assert(/CnPoll\.create\(/.test(block) && /steps: COMPLETE_STEPS/.test(block), "design.html uses the shared poller with its own schedule");

function page(w, tab, { helper = true } = {}) {
  const rows = ["1001", "1002", "1003"].map(id => ({ dataset: { receipt: id }, offsetTop: 0, offsetHeight: 10, remove() { this.gone = true; rows.splice(rows.indexOf(this), 1); } }));
  const ctx = {
    console, Set, Map, Promise, Date, encodeURIComponent, FN: "/fn", completedOrders: new Set(), selectedOrders: new Set(), orderCache: {}, allOpenReceipts: [], currentReceipts: [],
    $: () => ({ scrollTop: 0, clientHeight: 100 }), $$: () => rows, rtSet() {}, removePreviewBoxesForOrder() {}, updateCounters() {}, applyMetalFilter() {}, refreshChrome() {},
    document: tab.env.document, setTimeout: tab.env.setTimeout, clearTimeout: tab.env.clearTimeout,
    served: new Set(),
    fetch: async url => {
      w.calls.push({ t: w.t, url, tab: tab.name });
      if (ctx.failFetch) throw new TypeError("network down");
      const ids = new URL(url, "http://x").searchParams.get("dcFor").split(",");
      return { ok: true, json: async () => ({ success: true, orderNumbers: ids.filter(i => ctx.served.has(i)) }) };
    }
  };
  ctx.window = ctx;
  if (helper) ctx.CnPoll = { create: o => CnPoll.create(Object.assign({}, o, { env: tab.env })) };
  vm.createContext(ctx);
  vm.runInContext(block + "\n;this.__start = startCompletionPolling; this.__wake = wakeCompletionPolling; this.__poll = COMPLETE_POLL;", ctx);
  return { ctx, rows };
}
const gaps = w => w.calls.slice(1).map((c, i) => c.t - w.calls[i].t);
(async () => {
  let groups = 0; const ok = n => { groups++; console.log("  ok  " + n); };

  // 1. the schedule is the old one: first answer unchanged -> 1.5 s, 3, 6, 12, 30, 30 (the old loop's COMPLETE_STEPS[idle])
  {
    const w = makeWorld(), tab = makeTab(w, "d"), { ctx } = page(w, tab); ctx.__start(); await w.advance(200000);
    assert.deepEqual(gaps(w).slice(0, 8), [1500, 3000, 6000, 12000, 30000, 30000, 30000, 30000], "gaps " + gaps(w));
    assert.ok(w.calls.every(c => /dcFor=1001%2C1002%2C1003/.test(c.url)), "it asks about the open orders on screen, as before");
    ctx.__poll.p.stop(); ok("same wait schedule as the old loop (1.5, 3, 6, 12, 30 s)");
  }
  // 2. an order completed elsewhere: the row goes, and the next look is 0.75 s later; then it backs off again
  {
    const w = makeWorld(), tab = makeTab(w, "d"), { ctx, rows } = page(w, tab); ctx.__start(); await w.advance(40000);
    const n = w.calls.length; ctx.served.add("1002"); await w.advance(30500);
    assert.ok(rows.every(r => r.dataset.receipt !== "1002") && ctx.completedOrders.has("1002"), "the completed order's row is removed");
    const g = w.calls.slice(n).map(c => c.t), after = g.slice(g.findIndex((t, i) => i > 0 && t - g[i - 1] === 750) - 1);
    assert.ok(g.some((t, i) => i && t - g[i - 1] === 750), "0.75 s after a change: " + g.map((t, i) => (i ? t - g[i - 1] : 0)).join(","));
    ctx.__poll.p.stop(); ok("a completion removes the row and speeds the next look to 0.75 s");
  }
  // 3. hidden: nothing is asked, however long; shown: asked at once
  {
    const w = makeWorld(), tab = makeTab(w, "d"), { ctx } = page(w, tab); ctx.__start(); await w.advance(70000);
    tab.setHidden(true); await w.advance(500); const n = w.calls.length; await w.advance(600000);
    assert.equal(w.calls.length, n, "hidden for ten minutes: no request (the old loop made 20 requests of 12 queries)");
    tab.setHidden(false); await w.advance(300); assert.equal(w.calls.length, n + 1, "shown again: checked at once");
    ctx.__poll.p.stop(); ok("a hidden Design tab asks the ledger nothing; shown, it checks at once");
  }
  // 4. a click or key wakes it: back to 0.75 s without a request of its own
  {
    const w = makeWorld(), tab = makeTab(w, "d"), { ctx } = page(w, tab); ctx.__start(); await w.advance(120000);
    assert.ok(ctx.__poll.p.state().nextInMs > 5000); const n = w.calls.length; ctx.__wake(); assert.equal(w.calls.length, n, "waking makes no request by itself");
    assert.ok(ctx.__poll.p.state().nextInMs <= 830, "the next look is within 0.75 s: " + ctx.__poll.p.state().nextInMs);
    await w.advance(900); assert.equal(w.calls.length, n + 1);
    // the helper's own activity listener is off here (activity: false): a wheel or pointer move alone does not wake it
    await w.advance(120000); const m = w.calls.length, nx = ctx.__poll.p.state().nextInMs; tab.fire("doc", "pointerdown"); tab.fire("doc", "wheel");
    assert.ok(ctx.__poll.p.state().nextInMs > nx - 100, "only the page's own wake (click, key) speeds it"); void m;
    ctx.__poll.p.stop(); ok("click / key wake the check; the helper's own activity listener is off");
  }
  // 5. a failed read is an idle one (never faster, never an error storm)
  {
    const w = makeWorld(), tab = makeTab(w, "d"), { ctx } = page(w, tab); ctx.failFetch = true; ctx.__start(); await w.advance(100000);
    assert.deepEqual(gaps(w).slice(0, 5), [1500, 3000, 6000, 12000, 30000], "failures follow the same schedule: " + gaps(w));
    ctx.__poll.p.stop(); ok("a failing ledger read keeps the idle schedule");
  }
  // 6. without the helper the old loop still runs (cn-poll.js blocked or missing)
  {
    const w = makeWorld(), tab = makeTab(w, "d"), { ctx } = page(w, tab, { helper: false }); ctx.__start(); await w.advance(100000);
    assert.deepEqual(gaps(w).slice(0, 5), [1500, 3000, 6000, 12000, 30000], "fallback loop " + gaps(w)); ok("no helper -> the old loop, same schedule");
  }
  console.log("design-completion-poll.cjs: " + groups + " groups passed");
})().catch(e => { console.error(e); process.exit(1); });
