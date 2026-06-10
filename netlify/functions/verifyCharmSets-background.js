// netlify/functions/verifyCharmSets-background.js
// ---------------------------------------------------------------------------
// VISUAL SET VERIFICATION — the custom matching program, piggybacking your
// existing OpenAI setup (same OPENAI_API_KEY env your openaiProxy.js and
// openaiImageProxy.js already use; same direct-call pattern, same error
// hygiene with timeouts and non-JSON body handling).
//
// WHY "-background": Netlify background functions run up to 15 MINUTES per
// invocation (vs ~10s for normal functions). One to three invocations cover
// all 389 charm families end-to-end.
//
// HOW IT'S SMART (minimal scanning, as designed):
//   • It never compares all 7,000+ images against each other. The offline
//     title-signature engine already grouped the catalog into 389 candidate
//     families (943 products, duplicates excluded) — vision only CONFIRMS
//     those candidates: one API call per family, all members' featured
//     photos in a single request, image[0] as the reference.
//   • detail:"low" keeps vision token cost minimal (charm-shape judgment
//     doesn't need full-res). ~389 low-detail calls on gpt-5.4-mini ≈ a dollar or two total.
//   • Families containing best sellers are verified FIRST (rank priority),
//     so your highest-traffic PDPs are confirmed within the first minutes.
//   • Verified families are checkpointed in Firestore — re-runs skip them.
//
// WHAT IT DOES WITH A MISMATCH (auto-pruning):
//   If the model judges a member's photo to show a DIFFERENT charm than the
//   family reference, that handle is pruned: every other member's brites.set
//   metafield is rewritten without it, and the offender's own set is emptied.
//   The PDP bundle module reads the metafield, so pruning takes effect on
//   the storefront immediately. Full verdicts + reasons land in Firestore
//   (Brites_Editor_Meta/charmVerifyAudits) as your audit trail.
//
// TRIGGERING:
//   • Manually: curl -X POST https://goldenspike.app/.netlify/functions/verifyCharmSets-background \
//       (returns 202 immediately, runs in background)
//   • Automatically: verifyCharmSetsKick.js is scheduled @daily and triggers
//     this until all families are verified, then both go dormant.
//
// ENV (all already present except optional model override):
//   OPENAI_API_KEY (exists — your proxies use it), SHOPIFY_STORE,
//   SHOPIFY_CLIENT_ID, SHOPIFY_CLIENT_SECRET, EDIT_PASSCODE,
//   OPENAI_VISION_MODEL (optional, default "gpt-4o-mini")
// ---------------------------------------------------------------------------

const fetch = require("node-fetch");
const { CHARM_SETS, RANKS } = require("./charmSetsData");

let _fb = null;
function fb() {
  if (_fb !== null) return _fb;
  try { const admin = require("./firebaseAdmin"); _fb = { db: admin.firestore() }; }
  catch (e) { _fb = false; }
  return _fb;
}

const API_VERSION = process.env.SHOPIFY_API_VERSION || "2025-10";
let _token = null, _tokenExp = 0;
async function getToken() {
  if (_token && Date.now() < _tokenExp - 60000) return _token;
  const res = await fetch(`https://${process.env.SHOPIFY_STORE}/admin/oauth/access_token`, {
    method: "POST", headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ grant_type: "client_credentials",
      client_id: process.env.SHOPIFY_CLIENT_ID, client_secret: process.env.SHOPIFY_CLIENT_SECRET })
  });
  const t = await res.text();
  if (!res.ok) throw new Error("token " + res.status + ": " + t);
  const d = JSON.parse(t); _token = d.access_token; _tokenExp = Date.now() + (d.expires_in || 86399) * 1000;
  return _token;
}
async function gql(q, v) {
  const res = await fetch(`https://${process.env.SHOPIFY_STORE}/admin/api/${API_VERSION}/graphql.json`, {
    method: "POST", headers: { "X-Shopify-Access-Token": await getToken(), "Content-Type": "application/json" },
    body: JSON.stringify({ query: q, variables: v || {} })
  });
  const d = await res.json();
  if (d.errors && d.errors.length) throw new Error(JSON.stringify(d.errors));
  return d.data;
}

/* ---- same upstream hygiene as your openaiImageProxy.js ---- */
async function fetchWithTimeout(url, options, timeoutMs) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  try { return await fetch(url, { ...options, signal: controller.signal }); }
  finally { clearTimeout(t); }
}
async function readBodySafe(resp) {
  const text = await resp.text().catch(() => "");
  try { return { text, json: JSON.parse(text) }; } catch { return { text, json: null }; }
}

/* ---- build families once: closure of handle + partners, best sellers first ---- */
function buildFamilies() {
  const visited = new Set(), fams = [];
  const handles = Object.keys(CHARM_SETS).sort((a, b) => (RANKS[a] || 9999) - (RANKS[b] || 9999));
  for (const h of handles) {
    if (visited.has(h)) continue;
    const members = [h];
    for (const p of CHARM_SETS[h]) if (!visited.has(p.h) && CHARM_SETS[p.h]) members.push(p.h);
    members.forEach(m => visited.add(m));
    if (members.length >= 2) fams.push({ key: members.slice().sort().join("|").slice(0, 480), members });
  }
  return fams;
}

async function judgeFamily(imgs) {
  // CRITERIA: identical to the Smart Match aiVision prompt in brites-editor.liquid —
  // same per-style distinctions (HOOP/HUGGIE/STUD/DROP earrings; necklace structure +
  // REQUIRED chain style incl. the Beady/Satellite flag; charm-only = no chain, not a
  // necklace), the same charm-MEANING layer (literal depiction -> symbolic meaning:
  // zodiac/constellation/birth flower/Norse Mjolnir-Valknut-Vegvisir-runes/etc.), and
  // the same settings: gpt-5.4-mini, image detail "low", reasoning_effort "low".
  const model = process.env.OPENAI_VISION_MODEL || "gpt-5.4-mini";
  const content = [{ type: "text", text:
    "You are an expert jeweler's assistant examining product photos from ONE fine-jewelry store. Report ONLY what is actually visible — do not guess from context.\n\n" +
    "Apply these distinctions when reading each photo:\n" +
    "- Earrings: HOOP (ring/loop through the ear), HUGGIE (small hoop hugging the lobe), STUD (sits flat on the lobe, no dangle), DROP/DANGLE (hangs below the lobe). Earrings usually appear as a matching PAIR.\n" +
    "- Necklaces: structure = CHAIN / PENDANT / BEADED / CHARM-ONLY; note the chain style — especially BEADY/SATELLITE (fine chain with tiny round beads spaced at regular intervals; do not mistake for plain cable or a fully beaded strand).\n" +
    "- CHARM-ONLY: a single charm shown by itself with NO chain attached — this is NOT a necklace.\n" +
    "- Bracelet: worn on the wrist. Ring: worn on the finger.\n\n" +
    "CRITICAL — identify each photo's CHARM/motif at BOTH levels, as in our Smart Match process: the literal depiction (heart, letter, cross, star, moon, sun, alien/UFO, evil eye, butterfly, flower, animal, hammer, runes, constellation dots, coin, crown, bee, snake, etc.) AND its symbolic MEANING when one clearly applies (a ZODIAC sign or its constellation, a BIRTH FLOWER, a NORSE symbol such as Mjolnir/Valknut/Vegvisir/Yggdrasil/runes, etc.).\n\n" +
    "TASK: Photo 1 is the REFERENCE. " + (imgs[0].form ? "Expected formats in order: " + imgs.map(i => i.form || "?").join(", ") + ". " : "") +
    "For EACH subsequent photo decide same_charm: TRUE only if it shows the SAME charm — same literal depiction (same subject, same pose/orientation/silhouette) AND same symbolic meaning — merely mounted as a different jewelry format. Different pose, different animal/symbol, or different meaning (e.g. a different zodiac) = FALSE. Metal color differences are fine.\n" +
    'Reply with ONLY a JSON array, one entry per photo starting from photo 2: [{"photo":2,"same_charm":true,"charm":"<meaning, e.g. Leo (zodiac)>","charm_detail":"<literal depiction>","confidence":"high|medium|low","reason":"short"}]' }];
  for (const im of imgs) content.push({ type: "image_url", image_url: { url: im.url, detail: "low" } });

  const payload = { model, messages: [{ role: "user", content }] };
  if (/^(gpt-5|o\d)/.test(model)) {
    // GPT-5 / o-series reject "max_tokens" on chat completions — they require
    // max_completion_tokens (which must also cover reasoning tokens).
    payload.max_completion_tokens = 1500;
    payload.reasoning_effort = "low"; // mirror Smart Match SMART_EFFORT
  } else {
    payload.max_tokens = 700;
  }
  const upstream = await fetchWithTimeout("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${process.env.OPENAI_API_KEY}` },
    body: JSON.stringify(payload)
  }, 60000).catch((err) => {
    throw new Error(err && err.name === "AbortError" ? "OpenAI timeout after 60s" : (err && err.message) || String(err));
  });
  const { text, json } = await readBodySafe(upstream);
  if (!upstream.ok) throw new Error((json && json.error && json.error.message) || text || ("OpenAI HTTP " + upstream.status));
  const raw = ((json.choices || [])[0] || {}).message;
  const out = (raw && raw.content ? raw.content : "").replace(/```json|```/g, "").trim();
  try { return JSON.parse(out); } catch { return null; }
}

async function pruneFamily(family, byHandle, offenders) {
  const good = family.members.filter(m => !offenders.includes(m));
  const metafields = [];
  for (const m of family.members) {
    if (!byHandle[m] || !byHandle[m].id) continue;
    let value;
    if (offenders.includes(m)) value = "[]";
    else value = JSON.stringify((CHARM_SETS[m] || []).filter(p => good.includes(p.h)));
    metafields.push({ ownerId: byHandle[m].id, namespace: "brites", key: "set", type: "json", value });
  }
  if (!metafields.length) return;
  const r = await gql(`mutation($m: [MetafieldsSetInput!]!) {
    metafieldsSet(metafields: $m) { userErrors { field message } } }`, { m: metafields });
  const ue = r.metafieldsSet.userErrors || [];
  if (ue.length) throw new Error("prune metafieldsSet: " + ue[0].message);
}

exports.handler = async (event) => {
  // Background function: Netlify returns 202 to the caller and lets this run up to 15 min.
  // Passcode removed at owner request (single-user, one-time tool).

  const started = Date.now();
  const BUDGET_MS = 13 * 60 * 1000;
  const f = fb();
  let state = { verified: {}, pruned: 0 };
  if (f) {
    try { const s = await f.db.collection("Brites_Editor_Meta").doc("charmVerifyState").get();
      if (s.exists) state = Object.assign(state, s.data()); } catch (e) {}
  }

  const fams = buildFamilies();
  let processed = 0, mismatches = 0, errors = 0;

  for (const fam of fams) {
    const prev = state.verified[fam.key];
    if (prev && !(typeof prev.verdict === "string" && prev.verdict.indexOf("error") === 0)) continue; // retry errored families
    if (Date.now() - started > BUDGET_MS) break;
    try {
      // one batched Shopify lookup per family
      const q = fam.members.map((h, j) => `p${j}: productByHandle(handle: ${JSON.stringify(h)}) { id title featuredImage { url } }`).join("\n");
      const d = await gql(`query { ${q} }`);
      const byHandle = {};
      const imgs = [];
      fam.members.forEach((h, j) => {
        byHandle[h] = d["p" + j];
        if (d["p" + j] && d["p" + j].featuredImage) {
          const partner = (CHARM_SETS[fam.members[0]] || []).find(p => p.h === h);
          imgs.push({ handle: h, url: d["p" + j].featuredImage.url,
            form: j === 0 ? "Reference" : (partner ? partner.f : "") });
        }
      });
      if (imgs.length < 2) {
        state.verified[fam.key] = { at: new Date().toISOString(), verdict: "insufficient-images" };
        processed++; continue;
      }

      const verdicts = await judgeFamily(imgs);
      const offenders = [];
      const detail = [];
      if (Array.isArray(verdicts)) {
        verdicts.forEach((v, i) => {
          const im = imgs[i + 1];
          if (!im) return;
          detail.push({ handle: im.handle, same_charm: !!v.same_charm, charm: v.charm || "", charm_detail: v.charm_detail || "", confidence: v.confidence || "", reason: v.reason || "" });
          if (v.same_charm === false) offenders.push(im.handle);
        });
      }
      if (offenders.length) {
        await pruneFamily(fam, byHandle, offenders);
        mismatches += offenders.length;
        state.pruned = (state.pruned || 0) + offenders.length;
      }
      state.verified[fam.key] = { at: new Date().toISOString(),
        verdict: offenders.length ? "pruned:" + offenders.join(",") : "confirmed", n: imgs.length };
      if (f) {
        try { await f.db.collection("Brites_Editor_Meta").doc("charmVerifyAudits")
          .set({ [fam.members[0].replace(/[.\/]/g, "_")]: { at: new Date().toISOString(), members: fam.members, detail } }, { merge: true }); } catch (e) {}
      }
      processed++;
    } catch (e) {
      errors++;
      state.verified[fam.key] = { at: new Date().toISOString(), verdict: "error: " + String(e.message || e).slice(0, 140) };
      if (errors > 15) break; // bail on systemic failure (bad key, quota) rather than burn the run
    }
    // periodic checkpoint so a crash never loses progress
    if (f && processed % 10 === 0) {
      try { await f.db.collection("Brites_Editor_Meta").doc("charmVerifyState").set(state); } catch (e) {}
    }
  }

  const totalDone = Object.keys(state.verified).length;
  state.summary = { lastRun: new Date().toISOString(), processedThisRun: processed,
    mismatchesThisRun: mismatches, errorsThisRun: errors,
    familiesVerified: totalDone, familiesTotal: fams.length,
    complete: totalDone >= fams.length };
  if (f) { try { await f.db.collection("Brites_Editor_Meta").doc("charmVerifyState").set(state); } catch (e) {} }

  console.log("verifyCharmSets summary:", JSON.stringify(state.summary));
  return { statusCode: 200, body: JSON.stringify(state.summary) };
};
