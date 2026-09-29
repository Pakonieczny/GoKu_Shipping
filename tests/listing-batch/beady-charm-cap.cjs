"use strict";

// Beady Necklace charm size after generation (Paul, 2026-09-29: "still too large
// for all 5 slots, the 15% shrink did not help").
// Measured on finished sets, the prompt wording does not move the charm: the image
// model copies the placeholder charm of the reference photo. So the charm the model
// drew is shrunk by _beadyCharmCap.js after generation. This file checks the
// plumbing without sharp or a network:
//   - which pictures are passed through (Beady slots 1 to 5 only) and with what;
//   - that a failure of any kind leaves the model's picture as it was;
//   - that at most two pictures are processed at once;
//   - that the three places a generated picture is saved call it, only for a
//     request that places a charm, and before the design text is embedded.
// The picture processing itself is checked by beady-charm-cap-images.cjs.
// Usage: node tests/listing-batch/beady-charm-cap.cjs [background.js]

const assert = require("node:assert/strict");
const fs = require("node:fs");
const Module = require("node:module");
const path = require("node:path");

const server = fs.readFileSync(process.argv[2] || "netlify/functions/geminiImageProxy-background.js", "utf8");

// ---- the orchestrator, with stand-ins for the two picture modules ---------
const calls = [];
let live = 0, maxLive = 0;
const stub = (name, result) => ({
  [name]: async (buf, template, opts) => {
    calls.push({ name, buf, template, opts });
    live++; maxLive = Math.max(maxLive, live);
    await new Promise((r) => setTimeout(r, 15));
    live--;
    return typeof result === "function" ? result(buf) : result;
  },
});
let flatResult = { changed: true, buf: Buffer.from("flat-out") };
let photoResult = { changed: true, buf: Buffer.from("photo-out") };
const origLoad = Module._load;
Module._load = function (request, parent, ...rest) {
  if (request === "./_beadyCharmCapFlat") return stub("capFlatSlot", () => flatResult);
  if (request === "./_beadyCharmCapPhoto") return stub("capPhotoSlot", () => photoResult);
  return origLoad.call(this, request, parent, ...rest);
};
const cap = require(path.resolve("netlify/functions/_beadyCharmCap.js"));
// (the picture modules are required lazily, so the stand-ins stay in place until the end)
const quiet = { log() {}, warn() {} };
const IN = Buffer.from("model-out");
const template = Buffer.from("template");
let templateLoads = 0;
const loadTemplate = async () => { templateLoads++; return template; };
const run = (o) => cap.capBeadyCharmSize({ buf: IN, loadTemplate, log: quiet, ...o });

(async () => {
  assert.equal(cap.CAP_FACTOR, 0.87, "13% smaller in width and height (Paul, 2026-09-29: 0.75 was too much)");

  // flat slots 3, 4 are shrunk (and photo slots 1, 2, 5 once the photo module ships)
  const shrunk = [[2, "capFlatSlot", "flat-out"], [3, "capFlatSlot", "flat-out"]];
  if (cap.PHOTO_SLOTS.size) for (const slot of cap.PHOTO_SLOTS) shrunk.push([slot, "capPhotoSlot", "photo-out"]);
  for (const [slot, name, out] of shrunk) {
    calls.length = 0;
    const got = await run({ category: "Beady_Necklace", slotIndex: slot });
    assert.equal(got.toString(), out, `slot ${slot + 1} uses ${name}`);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].name, name);
    assert.equal(calls[0].buf, IN);
    assert.equal(calls[0].template, template, "the reference photo is handed over");
    assert.equal(calls[0].opts.slotIndex, slot);
    assert.equal(calls[0].opts.factor, 0.87);
  }

  // everything else passes through untouched, without loading the template
  templateLoads = 0; calls.length = 0;
  for (const [category, slot] of [["Regular_Necklace", 0], ["Regular_Necklace", 2], ["Stud_Earrings", 1], ["Charms", 3], ["Bracelets", 0],
                                   ["Beady_Necklace", 5], ["Beady_Necklace", 6], ["Beady_Necklace", 7], ["Beady_Necklace", -1], ["Beady_Necklace", NaN],
                                   ...[0, 1, 4].filter((n) => !cap.PHOTO_SLOTS.has(n)).map((n) => ["Beady_Necklace", n])]) {
    assert.equal(await run({ category, slotIndex: slot }), IN, `${category} slot ${slot + 1} is not resized`);
  }
  assert.equal(templateLoads, 0);
  assert.equal(calls.length, 0);
  assert.equal(await cap.capBeadyCharmSize({ category: "Beady_Necklace", slotIndex: 0, buf: null, loadTemplate, log: quiet }), null);

  // the module could not find the charm: the model's picture is kept
  flatResult = { changed: false, reason: "charm not found", buf: IN };
  assert.equal(await run({ category: "Beady_Necklace", slotIndex: 2 }), IN);
  flatResult = { changed: true, buf: "not a buffer" };
  assert.equal(await run({ category: "Beady_Necklace", slotIndex: 2 }), IN, "a bad result is ignored");
  flatResult = null;
  assert.equal(await run({ category: "Beady_Necklace", slotIndex: 2 }), IN);

  // anything that throws: the model's picture is kept and nothing propagates
  assert.equal(await run({ category: "Beady_Necklace", slotIndex: 0, loadTemplate: async () => { throw new Error("gone"); } }), IN);

  // at most two pictures at a time (each is decoded to raw 2048 x 2048 pixels)
  flatResult = { changed: true, buf: Buffer.from("flat-out") };
  maxLive = 0;
  const all = await Promise.all(Array.from({ length: 9 }, (_, i) => run({ category: "Beady_Necklace", slotIndex: i % 2 ? 2 : 3 })));
  assert.equal(all.length, 9);
  assert(maxLive === 2, `two at a time, saw ${maxLive}`);

  // ---- the save points ----------------------------------------------------
  const count = [...server.matchAll(/await capBeadyCharmSize\(/g)].length;
  assert.equal(count, 3, "batch collection, direct edits and job edits each call it");
  assert(/const \{ capBeadyCharmSize \} = require\("\.\/_beadyCharmCap"\);/.test(server));

  // direct edits: only when a charm is sent, not in the Charm Maker pipeline; before the design text is embedded
  const direct = server.slice(server.indexOf("if (kind === \"edits\") {\n      const cat = normalizeCategory(activeCategory);"),
                              server.indexOf("if (kind === \"write_manifest\") {"));
  assert(/if \(basePath1 && !isCharmPipeline\) \{\s*outBuf = await capBeadyCharmSize\(/.test(direct));
  assert(direct.indexOf("capBeadyCharmSize(") < direct.indexOf("embedPngTextMetadata(outBuf"), "shrunk before the metadata chunk is written");
  assert(direct.indexOf("capBeadyCharmSize(") > direct.indexOf("callImageModelEdits"), "after the model answered");
  assert(direct.indexOf("capBeadyCharmSize(") < direct.indexOf("uploadPngBufferToSetPath"), "before the save");

  // job edits (Redo): only kind=edits with a charm; an adjustment redo (no charm) is not shrunk again
  const job = server.slice(server.indexOf("outBuf = await applyFinalFrameZoomIfNeeded(outBuf, postprocess);\n\n    // Beady Necklace charm size"),
                           server.indexOf("await uploadPngBufferToSetPath(outBuf, output_base_path, slotIndex, jobId, runId);"));
  assert(job.length > 100, "found the job handler save point");
  assert(/if \(kind === "edits" && input_charm_storage_path && input_storage_path\) \{\s*outBuf = await capBeadyCharmSize\(/.test(job));

  // batch collection: only tasks that place a charm, skipped when the file already exists, before the metadata chunk
  const collect = server.slice(server.indexOf("const capByKey = new Map();"), server.indexOf("// Drive the stream."));
  assert(/String\(t\?\.type\) === "copy"\) continue;/.test(collect), "copy slots are not resized");
  assert(/t\?\.input_charm_storage_path && t\?\.input_storage_path/.test(collect));
  assert(/if \(!already\) \{\s*buffer = await capBeadyCharmSize\(/.test(collect), "a slot another collector already saved is not processed again");
  assert(collect.indexOf("capBeadyCharmSize(") < collect.indexOf("embedPngTextMetadata(buffer, embed)"));
  assert(collect.indexOf("capBeadyCharmSize(") < collect.indexOf("file.save("));
})().then(() => {
  Module._load = origLoad;
  console.log("beady charm cap: which slots, fallbacks, concurrency and the three save points check out");
}).catch((e) => { console.error(e); process.exit(1); });
