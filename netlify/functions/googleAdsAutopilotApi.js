// netlify/functions/googleAdsAutopilotApi.js
// ─────────────────────────────────────────────────────────────────────────────
// THE HTTP FACE OF THE AD AUTOPILOT: the console page (GET) and every console
// action (POST). Nothing else.
//
// WHY THIS FILE EXISTS
//   googleAdsAutopilotKick is a SCHEDULED function, and Netlify does not allow
//   a scheduled function to be reached over HTTP — their docs say so outright
//   ("You can't invoke scheduled functions directly with a URL"), and the
//   platform answers any direct request with a bare 403 and no body. That is
//   why unlocking the console showed "HTTP 403" with nothing to explain it:
//   the request never reached the function at all, so the passcode was never
//   the problem (a wrong passcode returns 401).
//
//   Splitting the HTTP surface into its own, UNSCHEDULED function is the fix.
//
// ⚠  DO NOT ADD A SCHEDULE ENTRY FOR THIS FUNCTION IN netlify.toml.
//    Doing so recreates the exact bug. The cron entry stays on
//    googleAdsAutopilotKick, which is now scheduled-only.
//
// There is no duplicated logic here: the implementation is imported from
// googleAdsAutopilotKick, so the console, the auth check and every action
// behave identically to before.
// ─────────────────────────────────────────────────────────────────────────────

const { httpHandler } = require("./googleAdsAutopilotKick");

exports.handler = async (event) => httpHandler(event);
