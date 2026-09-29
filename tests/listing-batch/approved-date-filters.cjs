"use strict";

// Approved → Listings has the same date filters as Approved → Charms (Paul,
// 2026-09-29). Both sub-tabs share one resolve → filter → readout path; a
// listing set is dated by its manifest `timestamp` (its folder name carries no
// time), falling back to a file time. Checked here with no network:
//   markup  - the Listings pane has the same quick filters and custom range
//   window  - custom range beats a preset, "Latest N" is the newest by date,
//             undated sets drop out of a date window, the readout says so
//   dates   - manifest timestamp; no/corrupt/implausible timestamp -> manifest
//             file time; no manifest -> first file's time; a network error
//             throws (read again later) instead of marking the set undated
// Usage: node tests/listing-batch/approved-date-filters.cjs [page.html]

const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");

const page = fs.readFileSync(process.argv[2] || "Listing_Generator_1.html", "utf8");
const cut = (src, from, to) => {
  const start = src.indexOf(from);
  const end = src.indexOf(to, start);
  assert(start > 0 && end > start, `found ${from.trim().split("\n")[0]}`);
  return src.slice(start, end);
};

// ---- markup ------------------------------------------------------------------
const pane = (id) => cut(page, `<div id="${id}"`, "<div class=\"selection-bar\">");
const presets = (html) => [...html.matchAll(/<button class="chip[^"]*" data-preset="([^"]+)">([^<]+)<\/button>/g)].map((m) => `${m[1]}=${m[2]}`);
const listPane = pane("approvedSubListings");
const charmPane = pane("approvedSubCharms");
assert.equal(presets(listPane).length, 9, "Listings has nine quick date filters");
assert.deepEqual(presets(listPane), presets(charmPane), "same quick date filters as Charms");
for (const id of ["approvedListDateFrom", "approvedListDateTo", "approvedListDateClear", "approvedListWindowInfo"]) {
  assert(listPane.includes(`id="${id}"`), `Listings has #${id}`);
}
assert(listPane.indexOf("approvedListPresets") < listPane.indexOf("approvedListCategories"), "date filters sit above the category chips, where Charms has them");

// ---- window helpers ------------------------------------------------------------
const helpers = cut(page, "    const _startOfDayMs = (d) =>", "    function _parseListingId(folderId) {");
const ctx = vm.createContext({ Date, Number, Math, String, JSON });
vm.runInContext(`${helpers}; this.h = { _resolveDateWindow, _applyDateWindow, _dateWindowReadout, _presetRange, _startOfDayMs };`, ctx);
const h = ctx.h;

const DAY = 86400000;
const now = Date.now();
const rows = [
  { id: "a", ms: now - 1000 }, { id: "b", ms: now - 2 * DAY }, { id: "c", ms: now - 40 * DAY },
  { id: "undated", ms: 0 }, { id: "d", ms: now - 5 * DAY },
];
const msOf = (r) => r.ms;
const ids = (list) => list.map((r) => r.id);

let w = h._resolveDateWindow({ preset: "all", dateFrom: null, dateTo: null });
assert.equal(w.source, "all");
assert.deepEqual(ids(h._applyDateWindow(rows, w, msOf)), ids(rows), "All time keeps every set, undated too");

w = h._resolveDateWindow({ preset: "today", dateFrom: null, dateTo: null });
assert.equal(w.source, "preset");
assert.deepEqual(ids(h._applyDateWindow(rows, w, msOf)), ["a"], "Today");

w = h._resolveDateWindow({ preset: "today", dateFrom: now - 6 * DAY, dateTo: now });
assert.equal(w.source, "custom", "a custom range beats the preset");
assert.deepEqual(ids(h._applyDateWindow(rows, w, msOf)), ["a", "b", "d"]);

w = h._resolveDateWindow({ preset: "latest10", dateFrom: null, dateTo: null });
assert.equal(w.latestN, 10);
w = h._resolveDateWindow({ preset: "latest25", dateFrom: null, dateTo: null });
assert.equal(w.latestN, 25);
assert.deepEqual(ids(h._applyDateWindow(rows, { ...w, latestN: 2 }, msOf)), ["a", "b"], "Latest N = newest by date, whatever the input order");
assert.deepEqual(ids(rows), ["a", "b", "c", "undated", "d"], "the caller's list is not reordered");

w = h._resolveDateWindow({ preset: "30d", dateFrom: null, dateTo: null });
assert.equal(
  h._dateWindowReadout(w, 3, 5, 1).replace(/^.*? · 3/, "… · 3"),
  "… · 3 of 5 sets · dates use set creation time · 1 undated set excluded",
  "readout counts and names the undated sets a date window drops");
assert.equal(h._dateWindowReadout(h._resolveDateWindow({ preset: "all" }), 5, 5, 1),
  "no date limit · showing everything · 5 of 5 sets · dates use set creation time", "Charms wording kept for All time");
assert.equal(h._dateWindowReadout(h._resolveDateWindow({ preset: "all" }), 2, 5, 0, "no date limit"),
  "no date limit · 2 of 5 sets · dates use set creation time", "a category filter does not claim everything is shown");
assert.match(h._dateWindowReadout({ from: now, to: now - DAY, source: "custom", latestN: 0 }, 0, 5, 0), /^Invalid range/);

// ---- listing set dates ---------------------------------------------------------
const reader = cut(page, "    // Creation time in ms, or 0 when the value is missing or implausible.", "    async function _loadListingDatesLocal() {");
const SET = "listing-generator-1/Generated_Listing_Sets/Completed_Listing_Sets/Beady_Necklace_Set_7";
const world = (files, failWith = null) => {
  const calls = [];
  const notFound = () => Object.assign(new Error("not found"), { code: "storage/object-not-found" });
  const c = vm.createContext({
    Date, Number, JSON, TextDecoder, SyntaxError, calls,
    storage: {},
    ref: (_s, p) => ({ fullPath: p, name: p.split("/").pop() }),
    getBytes: async (r) => {
      calls.push(`getBytes ${r.name}`);
      if (failWith) throw failWith;
      if (!(r.fullPath in files)) throw notFound();
      return new TextEncoder().encode(files[r.fullPath].text).buffer;
    },
    getMetadata: async (r) => {
      calls.push(`getMetadata ${r.name}`);
      if (!(r.fullPath in files)) throw notFound();
      return { timeCreated: new Date(files[r.fullPath].created).toISOString() };
    },
    listAll: async (r) => {
      calls.push(`listAll ${r.name}`);
      const items = Object.keys(files).filter((p) => p.startsWith(r.fullPath + "/")).map((p) => ({ fullPath: p, name: p.split("/").pop() }));
      return { items, prefixes: [] };
    },
  });
  vm.runInContext(`${reader}; this.read = _readListingSetDate;`, c);
  return { read: () => c.read({ id: "Beady_Necklace_Set_7", fullPath: SET }), calls };
};
const made = Date.UTC(2026, 8, 20, 14, 0, 0);
const approved = Date.UTC(2026, 8, 22, 9, 30, 0);
const slot = { [`${SET}/Slot_1.png`]: { created: approved - 1000 } };

(async () => {
  let t = world({ ...slot, [`${SET}/manifest.json`]: { text: JSON.stringify({ timestamp: new Date(made).toISOString() }), created: approved } });
  assert.equal(await t.read(), made, "manifest timestamp is the creation time");
  assert.deepEqual(t.calls, ["getBytes manifest.json"], "one read when the manifest is dated");

  t = world({ ...slot, [`${SET}/manifest.json`]: { text: JSON.stringify({ slots: [] }), created: approved } });
  assert.equal(await t.read(), approved, "manifest without timestamp: the manifest file's time");

  t = world({ ...slot, [`${SET}/manifest.json`]: { text: "{not json", created: approved } });
  assert.equal(await t.read(), approved, "unreadable manifest: the manifest file's time");

  t = world({ ...slot, [`${SET}/manifest.json`]: { text: JSON.stringify({ timestamp: "1970-01-01T00:00:00.000Z" }), created: approved } });
  assert.equal(await t.read(), approved, "an implausible timestamp is not used");

  t = world({ ...slot });
  assert.equal(await t.read(), approved - 1000, "no manifest: the first file's time");
  assert.deepEqual(t.calls, ["getBytes manifest.json", "listAll Beady_Necklace_Set_7", "getMetadata Slot_1.png"]);

  t = world({ ...slot }, Object.assign(new Error("network down"), { code: "storage/retry-limit-exceeded" }));
  await assert.rejects(() => t.read(), /network down/, "a network error is not taken for 'undated'");

  console.log("approved-date-filters: all checks passed");
})().catch((e) => { console.error(e); process.exit(1); });
