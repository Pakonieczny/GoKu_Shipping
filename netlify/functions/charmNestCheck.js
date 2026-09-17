/*  netlify/functions/charmNestCheck.js
 *  ═══════════════════════════════════════════════════════════════════════
 *  The server half of the Charm Sorter ⇄ Design Station connections check
 *  (charm-nest-check.html). Every row is the smallest real request that
 *  proves one thing: a credential has the right shape, a collection answers,
 *  a query needs no missing index, the bucket accepts a write, the station
 *  page carries the headers that let it be framed, an asset is served, a
 *  function is deployed, the model answers.
 *
 *  A green row means the thing answered. A grey row was never attempted and
 *  is never counted as a pass. Nothing here changes data unless write=1, and
 *  then only a probe object under charmnest/diag/ that is deleted again.
 *
 *  POST { op:"run", write?:1, ai?:1, sorterOrigin, stationOrigin, functionsBase }
 *  → { ok, at, rows:[{ group, name, status:"ok"|"warn"|"fail"|"skip", value, note, ms }], summary }
 *  ═══════════════════════════════════════════════════════════════════════ */
"use strict";
const admin = require("./firebaseAdmin");
const { json, gate, parseBody } = require("./_charmNestAuth");

const COLLECTIONS = [
  ["Charm_Nest_Library", "charm names and thumbnails from every nested source"], ["Charm_Nest_Sheets", "one record per written sheet, with set and label"],
  ["Charm_Nest_Calibration", "nesting calibration"], ["Charm_Nest_Jobs", "server solver and master-index jobs"], ["Charm_Nest_Agent", "model jobs (grouping, engraving, labels)"],
  ["Charm_Master_Index", "one entry per SKU from the master files"], ["Charm_Master_Files", "one record per indexed master file"],
  ["Charm_Pool", "one row per order line and copy"], ["Charm_Pool_Back", "engraved backs, one per piece"],
  ["Charm_Nest_Sets", "sets numbered per date"], ["Charm_Nest_Counters", "the per-date set counter (transaction)"], ["Charm_Nest_Runs", "run records, resumable"],
  ["Design_Bridge", "bridge sessions and their command log"], ["Charm_Sku_Aliases", "listing → SKU aliases"], ["Charm_Sku_NoDesign", "SKUs and patterns with no design"], ["Charm_Option_Map", "Etsy option → form/size maps"],
  ["Design_RealTime_Selected_Orders", "station locks and sorter claims"], ["Design_Completed Orders", "the station's design-complete ledger"], ["Design_Order_Archive", "completed orders with labels, set and run"], ["Brites_Orders", "order metadata and chat"]
];
const FUNCTIONS = ["charmNestLibrary", "charmNestOutput", "charmNestCheck", "charmNestAgent-background", "charmEngrave-background", "charmMaster-background", "charmNestSolve-background", "charmNestName", "charmNestReview", "firebaseOrders", "designArchive", "listOpenOrders", "etsyOrderProxy", "etsyImages", "refreshEtsyToken", "etsyApiProbe", "authGate"];
const ASSETS = ["charm-nest-1.html", "charm-nest-bridge.js", "charm-nest-geom.js", "charm-nest-orders.js", "charm-nest-pdf.js", "charm-nest-solver.js", "charm-nest-worker.js", "brites-progress.js", "vendor/opentype-1.3.4.min.js", "vendor/pdf-lib-1.17.1.min.js", "vendor/jszip-3.10.1.min.js", "lib/qrcode.min.js", "vendor/fonts/SourceSans3-Regular.otf", "vendor/fonts/SourceSans3-Semibold.otf", "charm-nest-check.html"];

const present = (v, n) => v ? `present (${String(v).length} chars${n ? ", " + n : ""})` : "";
async function timed(fn) { const t0 = Date.now(); try { const r = await fn(); return { r, ms: Date.now() - t0 }; } catch (e) { return { e, ms: Date.now() - t0 }; } }
const errText = e => String(e && e.message || e).slice(0, 400);
/** A Firestore "needs an index" error names the console link; surface it as the note so the fix is one click. */
function indexNote(e) { const m = /(https:\/\/console\.firebase\.google\.com\S+)/.exec(String(e && e.message || "")); return m ? `needs an index — create it here: ${m[1]}` : errText(e); }

async function fetchHead(url, opts = {}) {
  const ctl = new AbortController(); const t = setTimeout(() => ctl.abort(), 12000);
  try { const r = await fetch(url, Object.assign({ redirect: "manual", signal: ctl.signal, headers: { "user-agent": "charm-nest-check" } }, opts)); const h = {}; r.headers.forEach((v, k) => { h[k] = v; }); let len = Number(r.headers.get("content-length")) || 0; if (!len && opts.method !== "HEAD") { const b = await r.arrayBuffer(); len = b.byteLength; } return { status: r.status, headers: h, length: len }; }
  finally { clearTimeout(t); }
}

exports.handler = async function (event) {
  if (event.httpMethod === "OPTIONS") return json(204, {});
  if (event.httpMethod !== "POST") return json(405, { error: "POST only" });
  const body = parseBody(event);
  const locked = gate(event, body); if (locked) return locked;
  if (body.op !== "run") return json(400, { error: "unknown op" });
  const write = body.write === 1 || body.write === "1" || body.write === true, ai = body.ai === 1 || body.ai === "1" || body.ai === true;
  const sorterOrigin = String(body.sorterOrigin || "").replace(/\/+$/, ""), stationOrigin = String(body.stationOrigin || "").replace(/\/+$/, ""), fnBase = String(body.functionsBase || (sorterOrigin ? sorterOrigin + "/.netlify/functions" : "")).replace(/\/+$/, "");
  const rows = []; const row = (group, name, status, value, note, ms) => { rows.push({ group, name, status, value: value == null ? "" : String(value), note: note || "", ms: ms == null ? null : ms }); };

  /* ── credentials: shape only; the rows below prove whether they are accepted ── */
  const env = process.env;
  row("Credentials", "FIREBASE_PROJECT_ID", env.FIREBASE_PROJECT_ID ? "ok" : "fail", env.FIREBASE_PROJECT_ID || "missing");
  row("Credentials", "FIREBASE_CLIENT_EMAIL", env.FIREBASE_CLIENT_EMAIL ? "ok" : "fail", env.FIREBASE_CLIENT_EMAIL || "missing");
  row("Credentials", "FIREBASE_PRIVATE_KEY", env.FIREBASE_PRIVATE_KEY ? "ok" : "fail", present(env.FIREBASE_PRIVATE_KEY, /BEGIN PRIVATE KEY/.test(env.FIREBASE_PRIVATE_KEY || "") ? "PEM" : "not PEM") || "missing");
  row("Credentials", "FIREBASE_STORAGE_BUCKET", "ok", env.FIREBASE_STORAGE_BUCKET || `not set — default ${admin.DEFAULT_BUCKET || "?"}`);
  row("Credentials", "EDIT_PASSCODE", env.EDIT_PASSCODE ? "ok" : "warn", env.EDIT_PASSCODE ? "set — the sorter's cloud calls need it" : "not set — every charmNest* function is open to anyone with the URL", env.EDIT_PASSCODE ? "" : "Set EDIT_PASSCODE in Netlify to lock the sorter's functions.");
  row("Credentials", "ANTHROPIC_API_KEY", env.ANTHROPIC_API_KEY ? "ok" : "fail", env.ANTHROPIC_API_KEY ? `present (${env.ANTHROPIC_API_KEY.slice(0, 7)}…)` : "missing — engraving intent, label reads and reviews all need it");
  row("Credentials", "CHARM_NEST_NAME_MODEL", "ok", env.CHARM_NEST_NAME_MODEL || "not set — default claude-opus-5");
  row("Credentials", "Etsy CLIENT_ID", env.CLIENT_ID || env.ETSY_CLIENT_ID ? "ok" : "fail", present(env.CLIENT_ID || env.ETSY_CLIENT_ID) || "missing — listOpenOrders and etsyOrderProxy cannot call Etsy", "The token itself lives in the Design Station's browser; the station rows say whether it is signed in.");
  row("Credentials", "ETSY_SHARED_SECRET", env.ETSY_SHARED_SECRET || env.CLIENT_SECRET ? "ok" : "warn", present(env.ETSY_SHARED_SECRET || env.CLIENT_SECRET) || "not set");
  row("Credentials", "SHOP_ID", env.SHOP_ID ? "ok" : "warn", env.SHOP_ID || "not set");

  /* ── Firestore: every collection the bridge touches, one cheap read each ── */
  let db = null;
  try { db = admin.firestore(); } catch (e) { row("Firestore", "admin SDK", "fail", errText(e)); }
  if (db) {
    for (const [name, what] of COLLECTIONS) {
      const { r, e, ms } = await timed(() => db.collection(name).limit(500).select().get());
      if (e) row("Firestore", name, "fail", errText(e), what, ms);
      else row("Firestore", name, "ok", `${r.size >= 500 ? "500+" : r.size} document(s)`, what, ms);
    }
    /* the queries the app runs, through the library itself: a missing composite index shows here with its link */
    let lib = null; try { lib = require("./charmNestLibrary"); } catch (e) { row("Firestore queries", "charmNestLibrary", "fail", errText(e)); }
    if (lib && lib.ops) {
      const day = new Date().toISOString().slice(0, 10);
      const QUERIES = [["ping", {}, "sheets and charms counted"], ["setList", { from: day, to: day }, "sets by date"], ["runList", { limit: 5 }, "runs, newest first"], ["listSheets", { limit: 5 }, "sheets, newest first"], ["listSheets", { limit: 5, setId: "diag-none" }, "sheets of one set"], ["masterList", { limit: 5 }, "master index"], ["masterListFiles", {}, "master files"], ["poolList", { runId: "diag-none" }, "pool rows of a run"], ["backList", { sheetId: "diag-none" }, "backs of a sheet"], ["aliasGet", {}, "SKU aliases"], ["noDesignGet", {}, "no-design list"], ["optionMapGet", {}, "option maps"], ["getCalibration", {}, "calibration"]];
      for (const [op, args, what] of QUERIES) {
        const fn = lib.ops[op]; if (!fn) { row("Firestore queries", op, "skip", "not an op in this build", what); continue; }
        const { r, e, ms } = await timed(() => fn(Object.assign({ op }, args)));
        if (e) row("Firestore queries", op, "fail", indexNote(e), what, ms);
        else if (r && r.error) row("Firestore queries", op, "fail", String(r.error), what, ms);
        else { const n = r && (Array.isArray(r.sets) ? r.sets.length : Array.isArray(r.runs) ? r.runs.length : Array.isArray(r.sheets) ? r.sheets.length : Array.isArray(r.entries) ? r.entries.length : Array.isArray(r.files) ? r.files.length : Array.isArray(r.pools) ? r.pools.length : Array.isArray(r.backs) ? r.backs.length : null); row("Firestore queries", op, "ok", n == null ? "answered" : `${n} row(s)`, what, ms); }
      }
    }
    row("Firestore", "security rules", "skip", "not checkable from the server (the Admin SDK bypasses rules)", "Confirm in the console that every Charm_* and Design_Bridge collection denies client reads and writes.");
  }

  /* ── Storage ── */
  try {
    const bucket = admin.storage().bucket();
    row("Storage", "bucket", "ok", bucket.name);
    const { r, e, ms } = await timed(() => bucket.getMetadata());
    if (e) row("Storage", "bucket metadata", "fail", errText(e), "", ms);
    else {
      const meta = r[0] || {}; row("Storage", "bucket metadata", "ok", `${meta.location || "?"} · ${meta.storageClass || "?"}`, "", ms);
      const cors = Array.isArray(meta.cors) ? meta.cors : []; const origins = new Set(cors.flatMap(c => c.origin || []));
      const need = [sorterOrigin, stationOrigin].filter(Boolean); const missing = need.filter(o => !origins.has(o) && !origins.has("*"));
      row("Storage", "CORS origins", missing.length ? "warn" : "ok", origins.size ? [...origins].join(", ") : "no CORS rule", missing.length ? `missing: ${missing.join(", ")} — the browser cannot fetch per-SKU files or labels from these pages until added` : "The sorter fetches per-SKU files and labels straight from the bucket.");
      const methods = new Set(cors.flatMap(c => c.method || [])); row("Storage", "CORS methods", methods.has("PUT") && methods.has("GET") ? "ok" : "warn", [...methods].join(", ") || "none", methods.has("PUT") ? "" : "PUT is needed for the signed-URL upload route.");
    }
    if (write) {
      const p = `charmnest/diag/probe-${Date.now()}.txt`; const f = bucket.file(p);
      const w = await timed(async () => { await f.save(Buffer.from("charm-nest-check " + new Date().toISOString()), { resumable: false, contentType: "text/plain" }); const [buf] = await f.download(); const [url] = await f.getSignedUrl({ version: "v4", action: "write", expires: Date.now() + 60000, contentType: "text/plain" }); await f.delete(); return { len: buf.length, signed: !!url }; });
      if (w.e) row("Storage", "write · read · signed URL · delete", "fail", errText(w.e), "", w.ms); else row("Storage", "write · read · signed URL · delete", "ok", `probe of ${w.r.len} bytes round-tripped, V4 signed URL minted`, `probe object ${p} written and deleted`, w.ms);
    } else row("Storage", "write · read · signed URL · delete", "skip", "not run. Add ?write=1 to include it; the probe object is deleted again.");
  } catch (e) { row("Storage", "bucket", "fail", errText(e)); }

  /* ── the station page: the headers that make framing legal ── */
  if (stationOrigin) {
    const { r, e, ms } = await timed(() => fetchHead(stationOrigin + "/design-1.html?bridge=1"));
    if (e) row("Station page", "reachable", "fail", errText(e), "", ms);
    else {
      row("Station page", "reachable", r.status === 200 ? "ok" : "fail", `HTTP ${r.status} · ${r.length} bytes`, "", ms);
      const csp = r.headers["content-security-policy"] || ""; const fa = /frame-ancestors([^;]*)/i.exec(csp);
      // a source may carry a wildcard (https://*.goldenspike.app, http://127.0.0.1:*); match the sorter origin the way a browser would
      const allows = fa && sorterOrigin && fa[1].trim().split(/\s+/).some(src => { if (!/^https?:\/\//i.test(src)) return false; const re = new RegExp("^" + src.replace(/[.+?^${}()|[\]\\]/g, "\\$&").replace(/\*/g, "[^/\\s]*") + "$", "i"); return re.test(sorterOrigin); });
      row("Station page", "frame-ancestors", allows ? "ok" : "fail", fa ? fa[0].trim() : (csp ? "CSP without frame-ancestors" : "no Content-Security-Policy header"), allows ? "" : `must name ${sorterOrigin || "the sorter origin"} — see the /design-1.html header block in netlify.toml`);
      const corp = r.headers["cross-origin-resource-policy"] || ""; row("Station page", "Cross-Origin-Resource-Policy", /same-site|cross-origin/.test(corp) ? "ok" : "fail", corp || "missing", /same-site|cross-origin/.test(corp) ? "" : "the site-wide COEP require-corp blocks a cross-origin frame without it");
      const cc = r.headers["cache-control"] || ""; row("Station page", "Cache-Control", /no-store|no-cache/.test(cc) ? "ok" : "warn", cc || "missing", /no-store/.test(cc) ? "" : "no-store keeps a stale station out of the frame after a deploy");
      const coep = r.headers["cross-origin-embedder-policy"] || ""; row("Station page", "Cross-Origin-Embedder-Policy", "ok", coep || "none", "");
    }
  } else row("Station page", "reachable", "skip", "no station origin given");

  /* ── the sorter's own assets and the functions, from the outside ── */
  if (sorterOrigin) {
    for (const a of ASSETS) {
      const { r, e, ms } = await timed(() => fetchHead(sorterOrigin + "/" + a));
      const font = /\.otf$/.test(a);
      if (e) row("Sorter assets", a, "fail", errText(e), "", ms);
      else row("Sorter assets", a, r.status === 200 && (!font || r.length > 50000) ? "ok" : "fail", `HTTP ${r.status} · ${r.length} bytes`, font && r.length <= 50000 ? "font file missing or truncated — engraving cannot be set" : "", ms);
    }
  }
  if (fnBase) {
    for (const name of FUNCTIONS) {
      const { r, e, ms } = await timed(() => fetchHead(fnBase + "/" + name, { method: "OPTIONS" }));
      if (e) row("Functions deployed", name, "fail", errText(e), "", ms);
      else row("Functions deployed", name, r.status === 404 ? "fail" : "ok", r.status === 404 ? "404 — not deployed" : `HTTP ${r.status}`, r.status === 404 ? "check scripts/netlify-function-entries.json and the deploy log" : "", ms);
    }
  }

  /* ── the model ── */
  if (!env.ANTHROPIC_API_KEY) row("Claude", "model answers", "fail", "no ANTHROPIC_API_KEY");
  else if (ai) {
    let anthro = null; try { anthro = require("./_etsyMailAnthropic"); } catch (e) { row("Claude", "model answers", "fail", errText(e)); }
    if (anthro) {
      const model = env.CHARM_NEST_NAME_MODEL || "claude-opus-5";
      const { r, e, ms } = await timed(() => anthro.callClaudeRaw({ model, maxTokens: 16, system: [{ type: "text", text: "Reply with the single word OK." }], messages: [{ role: "user", content: [{ type: "text", text: "ping" }] }] }));
      if (e) row("Claude", "model answers", "fail", errText(e), model, ms);
      else { const txt = (r && r.content || []).map(c => c.text || "").join("").trim(); row("Claude", "model answers", /ok/i.test(txt) ? "ok" : "warn", `${model} · "${txt.slice(0, 40)}" · ${r && r.usage ? `${r.usage.input_tokens} in / ${r.usage.output_tokens} out` : ""}`, "", ms); }
    }
  } else row("Claude", "model answers", "skip", "not run. Add ?ai=1 to spend one tiny call.");

  const summary = { ok: rows.filter(r => r.status === "ok").length, fail: rows.filter(r => r.status === "fail").length, warn: rows.filter(r => r.status === "warn").length, skip: rows.filter(r => r.status === "skip").length };
  return json(200, { ok: true, at: new Date().toISOString(), rows, summary, write, ai });
};
