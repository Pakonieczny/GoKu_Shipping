/*  netlify/functions/charmNestKeepRepairCron.js
 *
 *  Scheduled repair of the permanent Charm repository's saved custom designs (Paul, 16 Oct 2026: a vector file dropped on a Review order with SAVE on must end linked
 *  with nobody present: "super robust and completely self contained"). The page repairs and finishes the Etsy step while it is open; this does the same every 10 minutes
 *  with NO page open: orphan files, saves whose records, entry, alias or files disagree, a stuck Etsy note, intent litter, and the Etsy steps that are not done yet.
 *
 *  It only calls the library's customSaveRepair { etsy: true } (rules and bounds: _charmNestCustomRepair.js): no repository token (default age 24 h, so it never removes a
 *  young file), throttled to one full pass per 5 minutes, leased (one pass at a time), idempotent, never throws, a 7 second budget inside Netlify's scheduled limit, nothing in the
 *  sandbox. The Etsy step reads Etsy first and writes once per listing, so a page and this job running together cannot write twice.
 *
 *  The schedule is declared in this file and in netlify.toml (as with every cron: the file's own CommonJS `exports.config` was not registered by itself).
 *  A scheduled function answers 403 to a direct call; tests/charm-nest/keep-repair-cron.cjs runs the handler on a fake library.  */
"use strict";

exports.handler = async function () {
  try {
    const Lib = require("./charmNestLibrary");
    const out = await Lib.ops.customSaveRepair({ etsy: true, maxEtsy: 3 });
    console.log("[charmNestKeepRepairCron]", JSON.stringify(out).slice(0, 600));
    return { statusCode: 200, body: JSON.stringify({ ok: true, repair: out }) };
  } catch (e) {
    console.error("[charmNestKeepRepairCron] failed:", e && e.message);
    return { statusCode: 500, body: JSON.stringify({ ok: false, error: String((e && e.message) || e).slice(0, 160) }) };
  }
};

exports.config = {
  schedule: "*/10 * * * *"
};
