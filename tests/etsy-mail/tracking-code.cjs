// Tests for netlify/functions/_etsyMailTrackingCode.js: customers get the
// USPS tracking number, never the "420" + ZIP label barcode.
//
//   node tests/etsy-mail/tracking-code.cjs
"use strict";
const assert = require("node:assert/strict");
const path = require("node:path");
const T = require(path.resolve(__dirname, "../../netlify/functions/_etsyMailTrackingCode.js"));

let passed = 0;
function test(name, fn) { fn(); passed++; console.log("ok -", name); }

test("a barcode with a 5-digit ZIP gives the 22-digit number", () => {
  assert.equal(T.cleanTrackingCode("420721139214490362894362155863"), "9214490362894362155863");
});
test("a barcode with a ZIP+4 gives the 22-digit number", () => {
  assert.equal(T.cleanTrackingCode("4204308196259214490362894362884787"), "9214490362894362884787");
  assert.equal(T.cleanTrackingCode("420 78163 2615 9214 4903 6289 4361 4404 89"), "9214490362894361440489");
});
test("other codes come back unchanged", () => {
  for (const c of ["9214490362894361440489", "LX094516277NL", "420123", "", "4201234567890"]) assert.equal(T.cleanTrackingCode(c), c);
});
test("barcodes inside a reply are replaced", () => {
  assert.equal(T.cleanTrackingCodesInText("Your tracking number is 420721139214490362894362155863."),
    "Your tracking number is 9214490362894362155863.");
});
console.log(passed + " tracking code tests passed");
