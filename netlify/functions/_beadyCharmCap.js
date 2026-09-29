"use strict";

// Beady Necklace listing images: the charm is made smaller AFTER generation.
//
// Paul asked for smaller charms twice (10% on 2026-09-28, another 15% on
// 2026-09-29) and changed the prompt wording each time. Measured on the
// finished sets, the wording moved nothing: the image model copies the size of
// the placeholder charm in the reference photo and answers "make it X% smaller"
// with roughly the same charm every time (slot 3, charm height against the
// template's: 0.83 with the original wording, 0.78 with -10%, 0.75 with -25%;
// slot 4 came out bigger than its placeholder although the prompt asked for
// 55%). So the size is now set here, deterministically: the charm the model drew
// is shrunk by CAP_FACTOR (a linear factor: 0.87 = 13% smaller in width and
// height) and the vacated area is repainted from the picture around it.
//
//   slots 3 and 4 (flat graphics)  -> _beadyCharmCapFlat.js
//   slots 1, 2 and 5 (photographs) -> _beadyCharmCapPhoto.js (not shipped yet:
//     PHOTO_SLOTS is empty until that module passes its chain-junction check)
//
// Every step is best effort: when the charm cannot be found with confidence, or
// anything throws, the picture the model produced is saved untouched. A finished
// image is never lost or delayed by this step.
//
// It runs once per generated picture, at the point the picture is saved (batch
// collection and single generations / Redo). Adjustment-mode Redo edits an
// existing slot image without a charm and is not passed through here.

const CAP_FACTOR = 0.87;

// Slots (0-based) that get the shrink. Slot 6 is a product shot, 7 and 8 are
// copied graphics; they are left as generated.
const FLAT_SLOTS = new Set([2, 3]);
const PHOTO_SLOTS = new Set([]);

// Two at a time: each picture is decoded to raw 2048 x 2048 pixels.
const MAX_CONCURRENT = 2;
let running = 0;
const waiting = [];
async function withSlot(fn) {
  if (running >= MAX_CONCURRENT) await new Promise((resolve) => waiting.push(resolve));
  running++;
  try { return await fn(); }
  finally {
    running--;
    const next = waiting.shift();
    if (next) next();
  }
}

function isCapped(category, slotIndex) {
  return category === "Beady_Necklace" && (FLAT_SLOTS.has(slotIndex) || PHOTO_SLOTS.has(slotIndex));
}

// buf: the PNG the model produced. loadTemplate: async () => Buffer of the
// reference photo that was image 1 of the request. Returns the buffer to save.
async function capBeadyCharmSize({ category, slotIndex, buf, loadTemplate, log = console, factor = CAP_FACTOR }) {
  if (!isCapped(category, slotIndex) || !Buffer.isBuffer(buf)) return buf;
  try {
    return await withSlot(async () => {
      const t0 = Date.now();
      const template = await loadTemplate();
      const run = require("./_beadyCharmCapFlat").capFlatSlot;
      const r = await run(buf, template, { slotIndex, factor });
      const ms = Date.now() - t0;
      if (r && r.changed && Buffer.isBuffer(r.buf)) {
        log.log(`[beady-cap] slot ${slotIndex + 1}: charm shrunk to ${factor} in ${ms} ms`);
        return r.buf;
      }
      log.warn(`[beady-cap] slot ${slotIndex + 1}: left as generated (${(r && r.reason) || "no reason"}) in ${ms} ms`);
      return buf;
    });
  } catch (e) {
    log.warn(`[beady-cap] slot ${slotIndex + 1}: left as generated (${(e && e.message) || e})`);
    return buf;
  }
}

module.exports = { capBeadyCharmSize, isCapped, CAP_FACTOR, FLAT_SLOTS, PHOTO_SLOTS };
