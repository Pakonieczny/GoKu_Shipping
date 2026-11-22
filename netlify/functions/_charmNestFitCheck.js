/*  netlify/functions/_charmNestFitCheck.js — FITFIRST (Paul, 21 Nov 2026: "a piece goes on a sheet only after the cloud's nester confirms it fits there; if it doesn't fit, it goes
 *  straight to the next draft"; "You just try to place it in an empty spot"). Before the placement unit puts an order's Gold Filled or Sterling Silver pieces on a sheet that already has
 *  a layout, those pieces (only them: the pieces on the sheet are never drawn again) are tried in the empty spots of the sheet's saved map (_charmNestFreeMap.js), one after another,
 *  the whole order or nothing.
 *  create({ db, bucket, log }) → { fits(rec, cands, candRows) → { fits: true | false | null, why } }
 *    null: no answer (the sheet has no map for its current layout, a new piece cannot be drawn): the caller decides.                                                                */
"use strict";
const Nest = require("./_charmNestNest");
const NestPlan = require("../../charm-nest-nest-plan.js");
const FreeMap = require("./_charmNestFreeMap");

function create(o = {}) {
  const { db, bucket } = o, log = o.log || (() => {});
  let S = null, policy = null, Pieces = null;
  const maps = new Map();   // sheet id → its map, for this run of the unit
  async function ready() {
    if (Pieces) return;
    [S, policy] = await Promise.all([Nest.readSettings(db), Nest.readPolicy(db)]);
    Pieces = require("./_charmNestNestPieces").create({ db, bucket, S, log, budgetMs: 60000 });
  }
  async function fits(rec, cands, candRows) {
    try {
      const saved = (rec.placements || []).filter(p => p && [p.cxPt, p.cyPt].every(v => Number.isFinite(+v)));
      if (!saved.length) return { fits: null, why: "no layout yet" };
      if (!(cands || []).length) return { fits: true, why: "" };
      if (!maps.has(rec.id)) maps.set(rec.id, await FreeMap.load(bucket, rec.id));
      const map = maps.get(rec.id);
      if (!map) return { fits: null, why: "the sheet has no map yet" };
      if (map.sig !== FreeMap.sig(saved)) return { fits: null, why: "the sheet's map is of an older layout" };
      await ready();
      const rows = new Map(candRows || []);
      const built = await Pieces.rebuild(Object.assign({}, rec, { charms: cands }), rows, async () => {});
      if (built.missing.length) return { fits: null, why: `a new piece cannot be drawn (${built.missing[0].why})` };
      const plate = Nest.plateOf(rec, S, await Nest.roseStockOf(db, rec), policy);
      const view = Object.assign({ metal: rec.metal, runId: rec.runId || null, sheetId: rec.id, charms: built.charms.map(c => Object.assign({}, c, { pinned: null })), placements: [], rejects: [], topup: null,
        roseCutAt: null, roseProtected: null, nestFlow: "standard", runSeed: 1 }, plate);
      const job = NestPlan.buildJob(view, Nest.planEnv(S, { agent: () => {} }));
      const r = FreeMap.fits(job, map);
      // (the map follows the sheet: pieces just found a spot are stamped on it for the next order of this run of the unit, as they will lie once nested)
      return r;
    } catch (e) {
      return { fits: null, why: String((e && e.message) || e).slice(0, 160) };
    }
  }
  return { fits };
}

module.exports = { create };
