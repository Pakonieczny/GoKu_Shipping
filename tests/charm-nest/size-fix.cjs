// SIZEFIX (Paul, 14 Nov 2026: "correct the charm repository ... don't change the already ones"): an entry re-indexed at the size its master draws it says which drawings it replaced
// (sizeFix.from); a sorter then does NOT make again the pieces already cut from those, but still forgets the old drawing so the next piece made up takes the new one.
// The repository's entry shape (slimEntry / cleanSizeFix) keeps the field, and the standard size is off by default (SIZEOFF).
const assert = require("assert"), fs = require("fs"), path = require("path"), vm = require("vm");
const root = path.join(__dirname, "..", "..");
const src = fs.readFileSync(path.join(root, "charm-nest-bridge.js"), "utf8");
const a = src.indexOf("const rdUp = s =>"), b = src.indexOf("/** Master.load replaced the library");
assert(a > 0 && b > a, "the redraw helpers are found");
const ctx = vm.createContext({}); vm.runInContext(src.slice(a, b) + "\nthis.redrawChanged = redrawChanged; this.rdFixed = rdFixed;", ctx);
const { redrawChanged, rdFixed } = ctx;
let n = 0; const ok = (c, m) => { assert(c, m); n++; console.log("  ok  " + m); };
const e = (h, extra) => Object.assign({ sku: "X_1", charmHash: h }, extra || {});
ok(redrawChanged(e("aaa"), e("bbb")) === true, "a new drawing (new hash) remakes the open pieces, as before");
ok(redrawChanged(e("aaa"), e("aaa")) === false, "the same drawing changes nothing");
ok(redrawChanged(e("aaa"), e("bbb", { sizeFix: { from: ["aaa"], at: 1 } })) === false && rdFixed(e("aaa"), e("bbb", { sizeFix: { from: ["aaa"], at: 1 } })) === true, "a size fix that names the old hash leaves placed pieces alone");
ok(redrawChanged(e("ccc"), e("bbb", { sizeFix: { from: ["aaa"], at: 1 } })) === true, "a size fix that names another hash does not cover this change");
ok(redrawChanged(e("bbb"), e("ddd", { sizeFix: { from: ["aaa"], at: 1 } })) === true, "a later, real change of the drawing is not hidden by an old size fix");
ok(rdFixed(e("aaa"), e("aaa", { sizeFix: { from: ["aaa"] } })) === false, "nothing changed: not a fix");
ok(rdFixed(null, e("bbb", { sizeFix: { from: ["aaa"] } })) === false && rdFixed(e("aaa"), null) === false, "no entry on one side: not a fix");
ok(rdFixed(e("aaa", { sizes: { "10MM": { charmHash: "s1" } } }), e("bbb", { sizes: { "10MM": { charmHash: "s2" } }, sizeFix: { from: ["aaa", "s1"] } })) === true, "a sized design: every hash it had is covered");
ok(rdFixed(e("aaa", { sizes: { "10MM": { charmHash: "s1" } } }), e("bbb", { sizes: { "10MM": { charmHash: "s2" } }, sizeFix: { from: ["aaa"] } })) === false, "a sized design: one hash not covered is a change");

// the whole of redrawNoted, with the page's pieces stubbed: what a sorter does when the library is read again
{
  const c = src.indexOf("function redrawNoted(prev, next) {"), d = src.indexOf("function redrawLater(");
  assert(c > 0 && d > c, "redrawNoted is found");
  const calls = { forget: [], agent: [], held: [], later: 0, redrawLater: 0 };
  const ctx2 = vm.createContext({ redrawWait: new Map(), REDRAW_MAX: 500, heldWait: new Set(), console,
    Pool: { forgetDrawing: (was, now) => { calls.forget.push(now.sku); return new Set(["old"]); } },
    agent: (o, k, m) => calls.agent.push(m), heldLater: () => { calls.later++; }, redrawLater: () => { calls.redrawLater++; } });
  vm.runInContext(src.slice(a, b) + "\n" + src.slice(c, d) + "\nthis.redrawNoted = redrawNoted;", ctx2);
  const prev = new Map([["FIXED_1", e("aaa", { sku: "FIXED_1" })], ["CHANGED_1", e("ccc", { sku: "CHANGED_1" })], ["SAME_1", e("sss", { sku: "SAME_1" })]]);
  const next = new Map([["FIXED_1", e("bbb", { sku: "FIXED_1", sizeFix: { from: ["aaa"], at: 1 } })], ["CHANGED_1", e("ddd", { sku: "CHANGED_1" })], ["SAME_1", e("sss", { sku: "SAME_1" })]]);
  const n2 = ctx2.redrawNoted(prev, next);
  ok(n2 === 1 && ctx2.redrawWait.size === 1 && ctx2.redrawWait.has("CHANGED_1") && !ctx2.redrawWait.has("FIXED_1"), "the sweep is queued for the changed SKU only: a size-fixed SKU's placed pieces are not made again");
  ok(calls.forget.includes("FIXED_1") && calls.forget.includes("CHANGED_1"), "the old drawing is forgotten for both, so the next piece made up reads the new file");
  ok(ctx2.heldWait.has("FIXED_1") && ctx2.heldWait.has("CHANGED_1") && calls.later >= 1, "held lines of both are looked at again");
  ok(calls.agent.some(m => /FIXED_1/.test(m) && /keep the drawing they were cut from/.test(m)) && calls.agent.some(m => /CHANGED_1/.test(m) && /made up again/.test(m)), "the log says plainly which pieces keep their drawing and which are made up again");
}
const M = require(path.join(root, "netlify", "functions", "_charmNestMaster.js"));
ok(JSON.stringify(M.cleanSizeFix({ from: ["a", "", "b"], at: 5 })) === JSON.stringify({ from: ["a", "b"], at: 5 }) && M.cleanSizeFix({ from: [] }) === null && M.cleanSizeFix("x") === null, "cleanSizeFix keeps the hashes and drops what is not one");
const slim = M.slimEntry({ sku: "X_1", charmHash: "bbb", widthPt: 10, heightPt: 10, sizeFix: { from: ["aaa"], at: 7 } });
ok(slim.sizeFix && slim.sizeFix.from[0] === "aaa", "the entry the sorter reads keeps sizeFix");
ok(!("sizeFix" in M.slimEntry({ sku: "X_1", charmHash: "bbb", widthPt: 10, heightPt: 10 })), "an entry without one carries none");
global.self = global.self || global;
const PDF = require(path.join(root, "charm-nest-pdf.js")); const SR = (global.self.CharmNestPDF || PDF || {}).SIZE_RULE;
ok(SR && SR.maxMm === 0, "the standard size is off by default: a charm is indexed at the size its master draws it");
console.log("size-fix: " + n + " checks passed");
