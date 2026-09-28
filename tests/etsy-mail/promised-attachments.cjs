// Tests for the promised-attachment guard in
// netlify/functions/etsyMailCollateral.js: a reply that says a file is
// attached goes out with exactly that file.
//
//   node tests/etsy-mail/promised-attachments.cjs
//
// Pure functions and a fixed sheet list; nothing touches the network or
// Firestore (the Firebase module is replaced by a stub that throws).
"use strict";
const assert = require("node:assert/strict");
const path = require("node:path");
const Module = require("node:module");

const FN = path.resolve(__dirname, "../../netlify/functions");
const realLoad = Module._load;
Module._load = function (req, parent, ...rest) {
  if (/(^|\/)firebaseAdmin(\.js)?$/.test(req)) {
    const no = () => { throw new Error("Firestore is not available in this test"); };
    return { firestore: () => ({ collection: no }), storage: () => ({ bucket: () => ({ file: no }) }) };
  }
  return realLoad.call(this, req, parent, ...rest);
};
const C = require(path.join(FN, "etsyMailCollateral.js"));
Module._load = realLoad;

// The owner's uploaded sheets, as stored (every doc is kind "line_sheet").
const POOL = [
  { id: "neck", name: "Necklace Charm + Chain Line Sheet", category: "Custom Necklace Charm", kind: "line_sheet",
    storagePath: "etsymail-collateral/n-Necklace_Charm_Chain_Line_Sheet.png", uploadedContentType: "image/png", uploadedFilename: "Necklace_Charm_Chain_Line_Sheet.png" },
  { id: "hug", name: "Huggie Hoop Line Sheet", category: "Custom Huggie Hoop Charm Earrings", kind: "line_sheet",
    storagePath: "etsymail-collateral/h-Huggie_Hoop_Line_Sheet.png", uploadedContentType: "image/png", uploadedFilename: "Huggie_Hoop_Line_Sheet.png" },
  { id: "stud", name: "Stud Earrings Line Sheet", category: "Custom Stud Earrings", kind: "line_sheet",
    storagePath: "etsymail-collateral/s-Stud_Earrings_Line_Sheet.png", uploadedContentType: "image/png", uploadedFilename: "Stud_Earrings_Line_Sheet.png" },
  { id: "care", name: "Jewellery care instructions", category: "care_instructions", kind: "line_sheet",
    storagePath: "etsymail-collateral/c-Care.jpg", uploadedContentType: "image/jpeg", uploadedFilename: "Care.jpg" },
  { id: "metal", name: "Gold metals comparison (filled vs plated vs solid)", category: "metal_comparison", kind: "line_sheet",
    storagePath: "etsymail-collateral/m-Metals.jpg", uploadedContentType: "image/jpeg", uploadedFilename: "Metals.jpg" },
  { id: "fit", name: "Necklace fit reference (chain length on body)", category: "fit_reference", kind: "line_sheet",
    storagePath: "etsymail-collateral/f-Necklace_length_guide.jpg", uploadedContentType: "image/jpeg", uploadedFilename: "Necklace_length_guide.jpg" },
  { id: "brace", name: "Bracelet sizing reference (wrist sizing chart)", category: "bracelet_sizing", kind: "line_sheet",
    storagePath: "etsymail-collateral/b-Bracelet_Sizing.jpg", uploadedContentType: "image/jpeg", uploadedFilename: "Bracelet_Sizing.jpg" }
];
// A chip as the inbox adds it (AI record or the Sheets picker).
const chip = (id) => {
  const c = POOL.find(x => x.id === id);
  return { attachmentId: "att_collateral_" + id, type: "image", storagePath: c.storagePath, proxyUrl: "/x?path=" + c.storagePath,
           filename: c.uploadedFilename, collateralId: id, collateralName: c.name, collateralKind: "line_sheet" };
};
const kinds = (text) => C.attachmentClaims(text).map(c => c.kind + (c.family ? ":" + c.family : "")).sort();

let passed = 0;
async function test(name, fn) {
  await fn();
  passed++;
  console.log("ok -", name);
}

(async () => {
  await test("real promises are read, with the product when named", () => {
    assert.deepEqual(kinds("I've attached our necklace line sheet so you can pick a chain."), ["line_sheet:necklace"]);
    assert.deepEqual(kinds("Before you order, I've attached the line sheet with every option."), ["line_sheet"]);
    assert.deepEqual(kinds("I've attached our care guide so they keep their shine."), ["care_instructions"]);
    assert.deepEqual(kinds("Your tracking timeline is attached below."), ["tracking"]);
  });

  await test("links, past sends and plain mentions are not promises", () => {
    assert.deepEqual(kinds("Here's the tracking information: https://tools.usps.com/go/TrackConfirmAction?tLabels=9400"), []);
    assert.deepEqual(kinds("The line sheet I sent before shows every size."), []);
    assert.deepEqual(kinds("Our line sheet lists 16, 18 and 20 inch chains."), []);
  });

  await test("only the exact promised file covers a promise", () => {
    const care = { kind: "care_instructions" };
    assert.equal(C.missingAttachmentClaims("I've attached our care guide.", [chip("metal")]).length, 1);
    assert.equal(C.missingAttachmentClaims("I've attached our care guide.", [chip("care")]).length, 0);
    assert.equal(C.missingAttachmentClaims("I've attached our necklace line sheet.", [chip("hug")]).length, 1);
    assert.equal(C.missingAttachmentClaims("I've attached our necklace line sheet.", [chip("neck")]).length, 0);
    assert.equal(C.describeClaims([care, { kind: "line_sheet", family: "huggie" }]), "care guide, huggie line sheet");
  });

  await test("a missing promised sheet is added from the uploads, same product only", async () => {
    const r = await C.attachClaimedCollateral("I've attached our huggie line sheet with all the hoop options.", [], { pool: POOL, askModel: false });
    assert.equal(r.missing.length, 0);
    assert.equal(r.add.length, 1);
    assert.equal(r.add[0].collateralId, "hug");
    assert.equal(r.add[0].type, "image");
  });

  await test("a sheet with no product named follows the draft's own file, then the hint", async () => {
    const own = await C.attachClaimedCollateral("I've attached the line sheet.", [], { pool: POOL, prefer: [chip("stud")], askModel: false });
    assert.deepEqual(own.add.map(a => a.collateralId), ["stud"]);
    const hint = await C.attachClaimedCollateral("I've attached the line sheet.", [], { pool: POOL, family: "necklace", askModel: false });
    assert.deepEqual(hint.add.map(a => a.collateralId), ["neck"]);
  });

  await test("a guide is never swapped for another guide", async () => {
    const r = await C.attachClaimedCollateral("I've attached our bracelet sizing chart.", [], { pool: POOL.filter(x => x.id !== "brace"), askModel: false });
    assert.equal(r.add.length, 0);
    assert.deepEqual(r.missing.map(c => c.kind), ["bracelet_sizing"]);
  });

  await test("a promised photo nobody attached comes back to the sender", async () => {
    const r = await C.attachClaimedCollateral("I've attached a photo of your finished charm.", [], { pool: POOL, askModel: false });
    assert.equal(r.add.length, 0);
    assert.deepEqual(r.missing.map(c => c.kind), ["photo"]);
  });

  console.log(passed + " promised-attachment tests passed");
})().catch(e => { console.error(e); process.exit(1); });
