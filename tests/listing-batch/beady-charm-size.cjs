"use strict";

// Beady Necklace charm size (Paul, 2026-09-28 and 2026-09-29).
// 2026-09-28: 10% smaller on slots 1, 3 and 5. 2026-09-29: another 15% smaller
// on slots 1 to 5 (models 0.675 -> 0.574, sizing guide 0.9 -> 0.765, background
// scene 0.65 -> 0.5525, back-engraving guide 65% -> 55.25%). Regular necklaces
// share slots 2 and 4 and the older model/guide wording, and keep every size.
// Sets queued earlier and tabs opened earlier still send an older wording;
// until 2026-10-05 the server swaps it, so every generation, batch or Redo comes
// out at the current size no matter which wording it was sent with.
// No network: the page constants and the server swap are cut out of the files.
// Usage: node tests/listing-batch/beady-charm-size.cjs [background.js] [page.html]

const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");

const server = fs.readFileSync(process.argv[2] || "netlify/functions/geminiImageProxy-background.js", "utf8");
const page = fs.readFileSync(process.argv[3] || "Listing_Generator_1.html", "utf8");
const cut = (src, from, to) => {
  const start = src.indexOf(from);
  const end = src.indexOf(to, start);
  assert(start > 0 && end > start, `found ${from.trim().split("\n")[0]}`);
  return src.slice(start, end);
};

const names = ["NECKLACE_MODEL_CHARM_SIZE", "BEADY_MODEL_CHARM_SIZE", "SIZING_GUIDE_CHARM_SIZE",
  "SIZING_GUIDE_CHARM_SIZE_FAIL", "BEADY_SIZING_GUIDE_CHARM_SIZE", "BEADY_SIZING_GUIDE_CHARM_SIZE_FAIL",
  "SLOT2_CHARM_SIZE", "BEADY_SLOT2_CHARM_SIZE", "SLOT4_CHARM_SIZE", "SLOT4_CHARM_SIZE_PCT",
  "BEADY_SLOT4_CHARM_SIZE", "BEADY_SLOT4_CHARM_SIZE_PCT"];
const P = vm.runInNewContext(`${cut(page, "    const NECKLACE_MODEL_CHARM_SIZE = `", "    const slot2DuplicatePrompt")};
  ({ ${names.join(", ")} })`, {});

// The prompt builders around those blocks, so the whole prompt can be compared.
const B = vm.runInNewContext(`${cut(page, "    const CHARM_GRAVITY_RULES = `", "    // ====")};
  ${cut(page, "    const slot2DuplicatePrompt = ", "    const SLOT2_DUPLICATE_PROMPT =")}
  ${cut(page, "    const necklaceSizingGuidePrompt = ", "    const SLOT3_SIZING_GUIDE_PROMPT =")}
  ${cut(page, "    const necklaceModelPrompt = ", "    const DEFAULT_PROMPT =")}
  ${cut(page, "    const slot4Prompt = ", "    const SLOT4_PROMPT =")}
  ${cut(page, "    const necklaceSecondModelPrompt = ", "    const SLOT5_PROMPT =")}
  ({ slot2DuplicatePrompt, necklaceSizingGuidePrompt, necklaceModelPrompt, slot4Prompt, necklaceSecondModelPrompt })`, {});

// The server: the swap tables and withCurrentBeadyCharmSize, with a clock.
const swapSource = cut(server, "const BEADY_CHARM_SIZE_SWAPS_UNTIL", "function buildOpenAIBatchJsonlLine");
const withClock = (nowIso) => {
  class Clock extends Date { static now() { return Date.parse(nowIso); } }
  return vm.runInNewContext(`${swapSource}; ({ withCurrentBeadyCharmSize })`, { Date: Clock }).withCurrentBeadyCharmSize;
};
const swap = withClock("2026-09-29T17:00:00Z");
const beady = { category: "Beady_Necklace" };
const regular = { category: "Regular_Necklace" };
const inPrompt = (block) => `LEAD-IN\n\n      ${block}\n\nTAIL`;

// ---- older wording that was live and may still be sent --------------------
// 2026-09-28 (49b323f / bfe515e): what the server produced from the first
// wording, and what the page then sent.
const modelStep1 = [
  ["REQUIRED 25% REDUCTION", "REQUIRED 32.5% REDUCTION"],
  ["visibly 25% smaller", "visibly 32.5% smaller"],
  ["× 0.75 / 7 (the previous distance / 7 baseline multiplied by 0.75)", "× 0.675 / 7 (the previous distance / 7 baseline multiplied by 0.675)"],
  ["use 0.75 times the template placeholder", "use 0.675 times the template placeholder"],
  ["charm 100 pixels tall must now be 75 pixels tall", "charm 200 pixels tall must now be 135 pixels tall"],
];
const modelTenPercent = modelStep1.reduce((t, [a, b]) => t.split(a).join(b), P.NECKLACE_MODEL_CHARM_SIZE);
const guideTenPercent = `CHARM POSITION + SIZE — REQUIRED 10% REDUCTION (NON-NEGOTIABLE)
    • The new charm must sit in the same position as the original charm in the reference image, centred on the same point, with the pointer line still pointing to it.
    • The new charm must be exactly 10% smaller than the original charm’s on-image size in BOTH width and height: scale it to fit 0.9 × the original charm’s height and 0.9 × its width, keeping the new charm’s own proportions. An original charm 400 pixels tall becomes a new charm 360 pixels tall.
    • Apply this reduction exactly ONCE. Never match or exceed the original charm’s size. Resize nothing else: the text, pencil, pointer line, layout and background stay exactly as they are.`;
const guideFailTenPercent = "- Any charm that is not 10% smaller than the original charm in the reference image.";

// Whole prompts, as the page builds them for each wording.
const model = (size) => B.necklaceModelPrompt(size);
const second = (size) => B.necklaceSecondModelPrompt(size);
const guide = (size, fail) => B.necklaceSizingGuidePrompt(size, fail);
const nowModel = model(P.BEADY_MODEL_CHARM_SIZE);
const nowSecond = second(P.BEADY_MODEL_CHARM_SIZE);
const nowGuide = guide(P.BEADY_SIZING_GUIDE_CHARM_SIZE, P.BEADY_SIZING_GUIDE_CHARM_SIZE_FAIL);
const nowSlot2 = B.slot2DuplicatePrompt(P.BEADY_SLOT2_CHARM_SIZE);
const nowSlot4 = B.slot4Prompt(P.BEADY_SLOT4_CHARM_SIZE, P.BEADY_SLOT4_CHARM_SIZE_PCT);

// Slot index -> [wording sent, prompt that must come out].
const cases = [
  ["slot 1, first wording", 0, model(P.NECKLACE_MODEL_CHARM_SIZE), nowModel],
  ["slot 1, 10% wording", 0, model(modelTenPercent), nowModel],
  ["slot 1, current wording", 0, nowModel, nowModel],
  ["slot 2, previous wording", 1, B.slot2DuplicatePrompt(P.SLOT2_CHARM_SIZE), nowSlot2],
  ["slot 2, current wording", 1, nowSlot2, nowSlot2],
  ["slot 3, first wording", 2, guide(P.SIZING_GUIDE_CHARM_SIZE, P.SIZING_GUIDE_CHARM_SIZE_FAIL), nowGuide],
  ["slot 3, 10% wording", 2, guide(guideTenPercent, guideFailTenPercent), nowGuide],
  ["slot 3, current wording", 2, nowGuide, nowGuide],
  ["slot 4, previous wording", 3, B.slot4Prompt(P.SLOT4_CHARM_SIZE, P.SLOT4_CHARM_SIZE_PCT), nowSlot4],
  ["slot 4, current wording", 3, nowSlot4, nowSlot4],
  ["slot 5, first wording", 4, second(P.NECKLACE_MODEL_CHARM_SIZE), nowSecond],
  ["slot 5, 10% wording", 4, second(modelTenPercent), nowSecond],
  ["slot 5, current wording", 4, nowSecond, nowSecond],
];
for (const [label, slotIndex, sent, want] of cases) {
  assert.equal(swap(beady, slotIndex, sent), want, `${label}: comes out at the current size`);
  // the batch path knows the set only by its folder
  assert.equal(swap({ outputBasePath: "listing-generator-1/Beady_Necklace/Ready_To_List/Set_9" }, slotIndex, sent),
    want, `${label}: recognised by folder`);
  // sent twice, still the same result
  assert.equal(swap(beady, slotIndex, swap(beady, slotIndex, sent)), want, `${label}: a second pass changes nothing`);
}

// ---- the current wording says what was asked ------------------------------
const need = (text, parts, label) => parts.forEach((part) => assert(text.includes(part), `${label} says ${part}`));
need(P.BEADY_MODEL_CHARM_SIZE, ["REQUIRED 42.6% REDUCTION", "visibly 42.6% smaller", "× 0.574 / 7", "multiplied by 0.574",
  "use 0.574 times", "500 pixels tall must now be 287 pixels tall"], "slots 1 and 5");
need(P.BEADY_SIZING_GUIDE_CHARM_SIZE, ["REQUIRED 23.5% REDUCTION", "exactly 23.5% smaller", "0.765 × the original charm’s height and 0.765 × its width",
  "400 pixels tall becomes a new charm 306 pixels tall"], "slot 3");
need(P.BEADY_SLOT2_CHARM_SIZE, ["REQUIRED 44.75% REDUCTION", "visibly 44.75% smaller", "× 0.5525 / 7", "multiplied by 0.5525",
  "use 0.5525 times", "400 pixels tall must now be 221 pixels tall"], "slot 2");
need(P.BEADY_SLOT4_CHARM_SIZE, ["Render both charms at 55.25% of", "65% × 0.85 = 55.25%", "Apply the final 55.25% scaling once"], "slot 4");
assert.equal(P.BEADY_SLOT4_CHARM_SIZE_PCT, "55.25%");
// 0.675 x 0.85, 0.9 x 0.85, 0.65 x 0.85: each is 15% below the previous size
assert.equal(+(0.675 * 0.85).toFixed(3), 0.574);
assert.equal(+(0.9 * 0.85).toFixed(3), 0.765);
assert.equal(+(0.65 * 0.85).toFixed(4), 0.5525);
assert.equal(+(0.65 * 0.85 * 100).toFixed(2), 55.25);
// no earlier number is left in the current wording
for (const [label, text, stale] of [
  ["slots 1 and 5", P.BEADY_MODEL_CHARM_SIZE, ["32.5", "0.675", "135 pixels", "25%", "0.75"]],
  ["slot 3", `${P.BEADY_SIZING_GUIDE_CHARM_SIZE}${P.BEADY_SIZING_GUIDE_CHARM_SIZE_FAIL}`, ["10%", "0.9 ", "360 pixels"]],
  ["slot 2", P.BEADY_SLOT2_CHARM_SIZE, ["35%", "0.65 ", "65 pixels"]],
  ["slot 4", P.BEADY_SLOT4_CHARM_SIZE, ["30% increase", "1.30"]],
]) stale.forEach((part) => assert(!text.includes(part), `${label} no longer says ${part}`));

// ---- Regular necklaces and the other slots are left alone -----------------
for (const [slotIndex, sent] of [[0, P.NECKLACE_MODEL_CHARM_SIZE], [1, P.SLOT2_CHARM_SIZE], [2, P.SIZING_GUIDE_CHARM_SIZE],
  [3, P.SLOT4_CHARM_SIZE], [4, P.NECKLACE_MODEL_CHARM_SIZE]]) {
  assert.equal(swap(regular, slotIndex, inPrompt(sent)), inPrompt(sent), `Regular slot ${slotIndex + 1} keeps its size`);
  assert.equal(swap({ category: "Stud_Earrings" }, slotIndex, inPrompt(sent)), inPrompt(sent), "another category keeps its size");
}
for (const slotIndex of [5, 6, 7]) {
  const text = inPrompt(`${P.NECKLACE_MODEL_CHARM_SIZE} ${P.SLOT2_CHARM_SIZE}`);
  assert.equal(swap(beady, slotIndex, text), text, `Beady slot ${slotIndex + 1} is not touched`);
}
// after 2026-10-05 the swap retires itself
const late = withClock("2026-10-05T00:00:01Z");
assert.equal(late(beady, 1, inPrompt(P.SLOT2_CHARM_SIZE)), inPrompt(P.SLOT2_CHARM_SIZE));
// not a string: passed through
assert.equal(swap(beady, 0, undefined), undefined);

// ---- the page: shared blocks are unchanged, Beady uses its own ------------
assert.match(page, /1: BEADY_DEFAULT_PROMPT,\s+2: BEADY_SLOT2_DUPLICATE_PROMPT,\s+3: BEADY_SLOT3_SIZING_GUIDE_PROMPT,\s+4: BEADY_SLOT4_PROMPT,\s+5: BEADY_SLOT5_PROMPT,\s+7: SLOT7_PROMPT/);
assert.match(page, /const REGULAR_SLOT2_DUPLICATE_PROMPT = SLOT2_DUPLICATE_PROMPT;/);
assert.match(page, /const REGULAR_SLOT4_PROMPT = SLOT4_PROMPT;/);
assert.match(page, /const SLOT4_PROMPT = slot4Prompt\(SLOT4_CHARM_SIZE, SLOT4_CHARM_SIZE_PCT\);/);
assert.match(page, /const BEADY_SLOT4_PROMPT = slot4Prompt\(BEADY_SLOT4_CHARM_SIZE, BEADY_SLOT4_CHARM_SIZE_PCT\);/);

console.log("Beady charm size: slots 1 to 5 come out 15% smaller from every wording still in flight; Regular and other slots untouched");
