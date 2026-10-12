/*  netlify/functions/_etsyMailHelperSeen.js
 *
 *  The inbox's Etsy helper (the Chrome extension) leaves one mark every time it is heard from:
 *      EtsyMail_OrderLinkMeta/helper   { seenAtMs, jobsAtMs, pollAtMs, peekAtMs }
 *  The link monitor (_etsyMailLinkHealth.js) and the send queue (_etsyMailSendQueue.js) read seenAtMs as "the helper is
 *  alive". It used to be written only when the helper asked for a scrape job (etsyMailJobs). That poll is thinned on
 *  purpose by the extension itself: each 30-second tick it skips at random (3 % of ticks before 6 am on its computer,
 *  30 % before 8, 10 % after 11 pm, and a third fewer at weekends, see getActivityProfile in its background.js), so at
 *  night the mark was 13, 18, 24 minutes old while the helper was perfectly well, and the line was called "down".
 *
 *  The extension also makes two calls that are NOT thinned, and that only the extension makes:
 *    · every 30 s, the check for messages waiting to be sent:  firestoreProxy GET op=list coll=EtsyMail_Drafts
 *      where=status,==,queued  (the inbox itself never asks for exactly that);
 *    · every 6 s from each open Etsy conversation tab:  etsyMailDraftSend op=peek.
 *  Each of them now leaves the same mark (its own field says which one, for whoever looks next). Nothing changes in the
 *  extension, so nothing has to be reinstalled.
 *
 *  Cost: one small write a minute at most per source per function instance (the same as before for the scrape poll).
 *  Never throws and never waits for long: a failed write only means a slightly older mark.
 */
"use strict";

const admin = require("./firebaseAdmin");

const EVERY_MS = 60 * 1000;
const FIELDS = { jobs: "jobsAtMs", poll: "pollAtMs", peek: "peekAtMs" };
const last = Object.create(null);   // when this instance last wrote for a source

/** The extension's own check for messages waiting to be sent, as it reaches firestoreProxy. */
function isQueuedDraftsPoll(method, qs) {
  qs = qs || {};
  return String(method || "").toUpperCase() === "GET" && qs.op === "list" && qs.coll === "EtsyMail_Drafts" && String(qs.where || "") === "status,==,queued";
}

/**
 * Leave the mark for one source ("jobs", "poll" or "peek"). Returns true when a write was made.
 * opts.now and opts.db are for tests.
 */
async function noteHelper(source, opts) {
  opts = opts || {};
  const field = FIELDS[source];
  if (!field) return false;
  const now = opts.now != null ? opts.now : Date.now();
  if (last[source] && now - last[source] < EVERY_MS && now >= last[source]) return false;
  last[source] = now;
  try {
    const db = opts.db || admin.firestore();
    await db.collection("EtsyMail_OrderLinkMeta").doc("helper").set({ seenAtMs: now, [field]: now }, { merge: true });
    return true;
  } catch (e) {
    console.warn("helper check-in not recorded (" + source + "):", e && e.message);
    return false;
  }
}

/** For tests: forget what this instance wrote. */
function reset() { for (const k of Object.keys(last)) delete last[k]; }

module.exports = { noteHelper, isQueuedDraftsPoll, reset, EVERY_MS };
