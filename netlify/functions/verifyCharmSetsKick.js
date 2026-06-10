// netlify/functions/verifyCharmSetsKick.js
// Thin scheduled "kicker": @daily it triggers the 15-minute background
// verifier (verifyCharmSets-background) until all 389 families are verified,
// then goes quiet. Uses Netlify's own URL env to find the site.
const fetch = require("node-fetch");
let _fb = null;
function fb() {
  if (_fb !== null) return _fb;
  try { const admin = require("./firebaseAdmin"); _fb = { db: admin.firestore() }; }
  catch (e) { _fb = false; }
  return _fb;
}
exports.handler = async () => {
  const f = fb();
  if (f) {
    try {
      const s = await f.db.collection("Brites_Editor_Meta").doc("charmVerifyState").get();
      if (s.exists && s.data().summary && s.data().summary.complete) {
        return { statusCode: 200, body: JSON.stringify({ status: "verification complete - dormant" }) };
      }
    } catch (e) {}
  }
  const base = process.env.URL || ("https://" + (process.env.SITE_NAME || "goldenspike") + ".netlify.app");
  const res = await fetch(base + "/.netlify/functions/verifyCharmSets-background", {
    method: "POST"
  });
  return { statusCode: 200, body: JSON.stringify({ status: "kicked background verifier", upstream: res.status }) };
};
