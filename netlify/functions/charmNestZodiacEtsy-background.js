/*  netlify/functions/charmNestZodiacEtsy-background.js
 *  ═══════════════════════════════════════════════════════════════════════
 *  The ONE PUT of the zodiac drop-down write (_charmNestEtsyZodiac.js, stage "write"). A -background function lives up to 15 minutes, so a slow 72-product PUT
 *  is not cut off half way. It is kicked only by the gated op zodiacListingWrite (after every check, the typed words and the lease), with {listingId, writeId}:
 *  writeId is 128 random bits stored on the listing's record by that op and never returned to a page, and this function sends NOTHING unless its own
 *  transaction (writing -> putting, for exactly that writeId, once) wins. So a stray call, a second kick or a replay sends nothing.
 *  Whatever Etsy answers is written to the record (config/zodiacEtsy_{listingId}); nothing is retried and nothing is undone here.
 *  ═══════════════════════════════════════════════════════════════════════ */
"use strict";
const { parseBody } = require("./_charmNestAuth");

exports.handler = async (event) => {
  const body = parseBody(event);
  try {
    const out = await require("./_charmNestEtsyZodiac").real().execute({ listingId: body.listingId, writeId: body.writeId });
    console.log("[charmNestZodiacEtsy]", JSON.stringify({ ok: out.ok, state: out.state, skipped: out.skipped, reason: out.reason }));
    return { statusCode: 200, body: JSON.stringify({ ok: !!out.ok, state: out.state || null, skipped: out.skipped || null }) };
  } catch (e) {
    console.error("[charmNestZodiacEtsy] failed:", e && e.message);
    return { statusCode: 500, body: JSON.stringify({ ok: false }) };
  }
};
