/*  netlify/functions/_charmNestCustomRepair.js
 *  The REPAIR PATH of "drop a vector file on a Review order, SAVE on -> the permanent Charm repository under the listing's SKU" (Paul, 16 Oct 2026: "super robust and completely self contained ...
 *  All eventualities must be fully robust with no failures"). Rules: /tmp/kp/CONTRACT-KEEP.md, section KR (R1-R9); the code here is the authority.
 *
 *  WHY IT EXISTS. A save is several things in several places: the two uploaded files (Storage), the index entry (Charm_Master_Index), the file record (Charm_Master_Files), the listing's alias
 *  (Charm_Sku_Aliases), the save record (Charm_Custom_Saves, with the Etsy step's state) and the server's copy of the intent (Charm_Custom_Intents). The commit writes the records in ONE transaction, so a
 *  torn commit is not expected on Firestore; but a person's Library delete, a master re-index, a function killed in the Etsy step, a page that died after its uploads, or a store that is not atomic all leave
 *  one part without the others. The NEXT RUN of the same save completes what it can (customSaveCommit converges); this module is the other half: it needs no person and no run of the save.
 *
 *  customSaveRepair { olderThanMs?, rid?, listingId?, limit?, etsy?, maxEtsy?, force? }   (no repository token; olderThanMs under one day needs the UPLOAD token: _charmNestRepoAuth.requirement)
 *    1 · ORPHAN FILES  charmnest/master/custom/{SKU}.ai|png whose SKU has NO index entry and that nobody touched for olderThanMs (default 24 h: a save waiting for the password keeps its files for a day;
 *        after that its page uploads again, customSaveCommit answers not-uploaded and the run goes on). The index is read again right before each delete. An entry's own files are never touched.
 *    2 · RECORDS       every save record, a page of them per call (a cursor in Charm_Custom_Repair/state, so a shop with years of saves is covered over time), against its entry, alias and files:
 *          entry-gone    the entry was removed (a person's Library delete): the listing's answer that THIS save wrote is cut (restoring what it replaced when that still exists); the record says so
 *                        (repair.state). Nothing is re-created: the bytes are in a browser; the next run of the save (the file dropped again) re-creates the entry and the answer (commit converges).
 *          overtaken     the entry now holds ANOTHER drawing (a master re-index wrote over the SKU): same cut, same flag. The saved design is never silently replaced by a different one.
 *          files-missing the entry (a saved design's own) has no .ai or .png in Storage: the record says so; the next run of the save uploads the bytes again.
 *          alias gone    the record promised an answer and NOTHING answers now (the alias document lost its key): put back, only when the alias is this save's own (keep.rid) or missing altogether;
 *                        an answer that is someone else's (a person's link, another design) is never touched (first link wins).
 *          etsy writing  a run that died after its note "writing" and whose lease ran out: made "unverified" (reason interrupted): never "working" for ever; its next try reads Etsy first.
 *        A record that is whole has its repair flag removed.
 *    3 · INTENTS       done / cancelled copies older than 7 days and wanted copies older than 30 days with no save record: removed (the browser's own copy is the actor, this is only the server's note).
 *    4 · ETSY STEPS    only with etsy:true and an injected step: the saves whose Etsy step is not done and not final (oldest first, at most maxEtsy, one after the other) are run by the server itself:
 *        the step takes only {rid} and is idempotent (reads Etsy first, one write per listing, leases), so any number of callers may do this at any time.
 *  COST (Oct 2026): steps 1 and 3 work on a scale of days (a 24 h grace, 7 and 30 day ages), yet read every saved SKU's index entry and up to 200 intents
 *  on every pass (the cron every 10 minutes, every sorter tab every 5): they now run at most once every DAILY_MS (stamp `dailyAt` in Charm_Custom_Repair/state),
 *  unless the call is forced or names its own olderThanMs (a person's act). Step 4 no longer lists the failed saves that can never be retried (they stayed in its
 *  query for ever): a failed save is listed only when its record says retry:true (every failed record the Etsy step writes carries retry).
 *  Throttled (a full pass at most every 5 minutes unless forced), leased (one pass at a time), bounded (limit records, 2,500 files), idempotent (a second pass finds nothing to do), never throws.
 *  The sandbox leaves no trace. Dependencies are passed in ({ db, FV, bucket, now?, etsySku? }), so a test runs on fakes.                                                                             */
"use strict";
const { str, num } = require("./_charmNestAuth");
const Master = require("./_charmNestMaster");

const COLL = "Charm_Custom_Saves", INTENTS = "Charm_Custom_Intents", ALIASES = "Charm_Sku_Aliases", INDEX = Master.INDEX, STATE = "Charm_Custom_Repair";
const DIR = "charmnest/master/custom/";
const DAY = 24 * 3600000, MIN = 60000;
const GRACE_MS = DAY, SHORT_MS = 3600000;            // the default age of an orphan; below SHORT_MS the call is a person's (upload token)
const THROTTLE_MS = 5 * MIN, PASS_LEASE_MS = 2 * MIN;
const RECORDS_PER_PASS = 40, FILE_PAGES = 5, FILE_PAGE = 500;
const INTENT_DONE_MS = 7 * DAY, INTENT_WANTED_MS = 30 * DAY;
const DAILY_MS = DAY;                                 // COST (Oct 2026): how often the orphan and intent steps run on an unforced pass
const COOLDOWN_MS = 2 * MIN, SCOPE_COOLDOWN_MS = 15 * MIN, ETSY_BUDGET_MS = 3000, PASS_BUDGET_MS = 7000;
const up = s => String(s == null ? "" : s).trim().toUpperCase();
const nowOf = deps => (typeof deps.now === "function" ? deps.now : Date.now)();
const warn = (what, e) => { try { console.warn("[customRepair] " + what + ":", String((e && (e.message || e.code)) || e).slice(0, 160)); } catch (_) {} };
const isSandbox = b => b && (b.sandbox === true || b.sandbox === 1 || b.sandbox === "1" || b.sandbox === "true");
const dataOf = s => (s && s.exists ? s.data() || {} : null);
const tsMs = v => { if (!v) return 0; if (typeof v === "number") return v; if (typeof v.toMillis === "function") { try { return v.toMillis(); } catch (_) { return 0; } } return 0; };
const EtsySku = () => require("./_charmNestEtsySku");

/* ═══ 1 · orphan files ═══ */
/** Files under charmnest/master/custom/ whose SKU has no index entry and that are older than olderThanMs are removed. -> { listed, orphans:[sku], deleted, kept } */
async function orphans(deps, o) {
  o = o || {};
  const out = { listed: 0, orphans: [], deleted: 0, kept: 0 };
  const { db, bucket } = deps, now = nowOf(deps), age = Number.isFinite(+o.olderThanMs) && o.olderThanMs !== "" && o.olderThanMs != null ? Math.max(0, +o.olderThanMs) : GRACE_MS;
  if (!bucket || typeof bucket.getFiles !== "function") return out;
  const bySku = new Map();
  let q = { prefix: DIR, autoPaginate: false, maxResults: FILE_PAGE };
  for (let page = 0; q && page < FILE_PAGES; page++) {
    const res = await bucket.getFiles(q), files = (res && res[0]) || [];
    out.listed += files.length;
    for (const f of files) { const m = /^charmnest\/master\/custom\/([^\/]+)\.(?:ai|png)$/.exec(String(f && f.name)); if (m) { if (!bySku.has(m[1])) bySku.set(m[1], []); bySku.get(m[1]).push(f); } }   // (KEEPOVERRIDE: a stem {SKU}__k{tag} is kept while an entry draws from it, below)
    q = res && res[1] ? res[1] : null;
  }
  const skus = [...bySku.keys()];
  for (let i = 0; i < skus.length; i += 100) {
    const part = skus.slice(i, i + 100), docs = await db.getAll(...part.map(s => db.collection(INDEX).doc(s)), { fieldMask: ["sku"] });
    for (let j = 0; j < part.length; j++) {
      if (docs[j] && docs[j].exists) continue;                       // an entry's files are never touched
      if (/__k[0-9a-z]{6,16}$/.test(part[j])) { try { const q = await db.collection(INDEX).where("aiPath", "==", DIR + part[j] + ".ai").limit(1).get(); if (!q.empty) continue; } catch (e) { warn("tagged file not looked up", e); continue; } }   // (KEEPOVERRIDE, Paul 20 Oct 2026: an override's or an Etsy SKU's own files)
      let old = true;
      for (const f of bySku.get(part[j])) {
        let t = NaN; try { const [m] = await f.getMetadata(); t = Date.parse((m && (m.updated || m.timeCreated)) || ""); } catch (_) { t = NaN; }
        if (!Number.isFinite(t) || now - t < age) { old = false; break; }      // young, or its age is unknown: kept
      }
      if (!old) { out.kept++; continue; }
      const again = await db.collection(INDEX).doc(part[j]).get();   // (a commit may have landed since the list was read)
      if (again.exists) continue;
      out.orphans.push(part[j]);
      for (const f of bySku.get(part[j])) { try { await f.delete({ ignoreNotFound: true }); out.deleted++; } catch (e) { warn("orphan not removed " + f.name, e); } }
    }
  }
  return out;
}

/* ═══ 2 · records ═══ */
/** What is wrong with one save record, from what a transaction READ. Pure. `files`: { ai:bool, thumb:bool } (is each there), or null when not looked at.
    -> { state: null | "entry-gone" | "overtaken" | "files-missing", which?, alias: null | { kind:"cut", restore?:sku } | { kind:"put" }, etsy: bool } */
function plan(d, entry, alias, files, now) {
  const out = { state: null, alias: null, etsy: false };
  if (d.superseded) return out;   // (KEEPOVERRIDE, Paul 20 Oct 2026: its SKU was moved to another design by the designs window's switch: nothing to put right, nothing to cut)
  const e = d.etsy && typeof d.etsy === "object" ? d.etsy : {};
  if (e.state === "writing" && !(+e.runningUntil > now)) out.etsy = true;
  const sku = up(d.sku), mode = d.alias === "bySku" && d.oldSku ? "bySku" : d.alias === "whole" ? "whole" : "none";
  if (!entry) out.state = "entry-gone";
  else if (str(d.charmHash, 80) && str(entry.charmHash, 80) && str(d.charmHash, 80) !== str(entry.charmHash, 80)) out.state = "overtaken";
  else if (entry.hashSource === "custom-save" && files && (!files.ai || !files.thumb)) { out.state = "files-missing"; out.which = [!files.ai ? "ai" : "", !files.thumb ? "thumb" : ""].filter(Boolean); }
  if (mode === "none") return out;
  const a = alias || null, keep = a && a.keep && typeof a.keep === "object" ? a.keep : null;
  const stamped = !!keep && up(keep.sku) === sku, ours = stamped && String(keep.rid || "") === String(d.rid || "");   // (a person's own link carries no stamp: it is never cut)
  const answer = !a ? "" : mode === "bySku" ? up(a.bySku && a.bySku[up(d.oldSku)]) : up(a.sku);
  if ((out.state === "entry-gone" || out.state === "overtaken") && answer === sku && stamped) out.alias = { kind: "cut" };
  else if (!out.state && !answer && (!a || ours)) out.alias = { kind: "put" };
  return out;
}

/** Applies plan() to one record in ONE transaction that reads everything again, so a save, a person's edit or another repair that landed meanwhile decides again from the truth. */
async function fixRecord(deps, id, files) {
  const { db, FV } = deps, now = nowOf(deps), ref = db.collection(COLL).doc(id);
  return db.runTransaction(async tx => {
    const rs = await tx.get(ref), d = dataOf(rs);
    if (!d || d.phase !== "repo" || d.rehearsal === true) return { skipped: true };
    const sku = up(d.sku), listingId = str(d.listingId, 30);
    if (!/^\d{3,20}$/.test(listingId) || !sku) return { skipped: true };
    const aref = db.collection(ALIASES).doc(listingId);
    const [es, as] = await tx.getAll(db.collection(INDEX).doc(sku), aref);
    const entry = dataOf(es), alias = dataOf(as), p = plan(d, entry, alias, files, now), did = [];
    if (p.alias && p.alias.kind === "cut") {
      // what the save replaced is put back when that design still exists, else the answer is taken away
      const was = up(alias.keep && alias.keep.was);
      let back = "";
      if (was && was !== sku) { const ws = dataOf(await tx.get(db.collection(INDEX).doc(was))); if (ws && ws.sku && !ws.blocked) back = was; }
      const keep = Object.assign({}, alias.keep, { cut: { at: now, why: p.state, rid: d.rid } });
      if (d.alias === "bySku") await tx.set(aref, { bySku: { [up(d.oldSku)]: back || FV.delete() }, keep, updatedAt: FV.serverTimestamp() }, { merge: true });
      else await tx.set(aref, back ? { sku: back, keep, updatedAt: FV.serverTimestamp() } : { sku: FV.delete(), v: FV.delete(), keep, updatedAt: FV.serverTimestamp() }, { merge: true });
      did.push("alias-cut");
    } else if (p.alias && p.alias.kind === "put") {
      const keep = { rid: d.rid, sku, oldSku: str(d.oldSku, 120), was: "", at: now, by: str(d.by, 80) || "operator", repaired: true };
      if (d.alias === "bySku") await tx.set(aref, { listingId, bySku: { [up(d.oldSku)]: sku }, keep, updatedAt: FV.serverTimestamp() }, { merge: true });
      else await tx.set(aref, { listingId, sku, v: 2, keep, updatedAt: FV.serverTimestamp() }, { merge: true });
      did.push("alias-put");
    }
    const patch = {}, cur = d.repair && typeof d.repair === "object" ? d.repair : null;
    if (p.etsy) {
      const e = Object.assign({}, d.etsy); delete e.runningUntil;
      const r = EtsySku().REASONS.interrupted;
      patch.etsy = Object.assign(e, { state: "unverified", reason: "interrupted", text: r.text, retry: true, at: now });
      did.push("etsy-final");
    }
    if (p.state && (!cur || cur.state !== p.state || JSON.stringify(cur.which || []) !== JSON.stringify(p.which || []))) { patch.repair = Object.assign({ state: p.state, at: now }, p.which ? { which: p.which } : {}); did.push("flag:" + p.state); }
    else if (!p.state && cur) { patch.repair = FV.delete(); did.push("flag-cleared"); }
    if (Object.keys(patch).length) await tx.update(ref, patch);
    return { state: p.state, did };
  });
}

async function filesOf(deps, entry) {
  if (!entry || entry.hashSource !== "custom-save" || !deps.bucket) return null;
  try {
    const [ai, thumb] = await Promise.all([entry.aiPath ? deps.bucket.file(entry.aiPath).exists() : [false], entry.thumbPath ? deps.bucket.file(entry.thumbPath).exists() : [false]]);
    return { ai: !!ai[0], thumb: !!thumb[0] };
  } catch (e) { warn("files not looked at", e); return null; }   // (unknown is not missing)
}

/** One page of save records (or the record(s) of one order): -> { checked, fixed:[{id, did}], flagged:[{id, state}], cursor, wrapped } */
async function records(deps, o) {
  o = o || {};
  const { db } = deps, out = { checked: 0, fixed: [], flagged: [], cursor: 0, wrapped: false };
  let docs;
  if (o.rid) {
    const ids = [String(o.rid)].concat(/^\d{3,20}$/.test(String(o.listingId || "")) ? [o.rid + "-" + o.listingId] : []);
    docs = (await db.getAll(...ids.map(i => db.collection(COLL).doc(i)))).filter(s => s.exists);
  } else {
    const limit = Math.min(200, Math.max(1, Math.floor(+o.limit) || RECORDS_PER_PASS)), cursor = Math.max(0, +o.cursor || 0);
    const snap = await db.collection(COLL).orderBy("at").startAfter(cursor > 0 ? cursor - 1 : 0).limit(limit).get();
    docs = snap.docs; out.wrapped = docs.length < limit;
    out.cursor = out.wrapped ? 0 : num(dataOf(docs[docs.length - 1]).at);
  }
  const one = async s => {
    const d = dataOf(s);
    if (!d || d.phase !== "repo" || d.rehearsal === true) return;
    out.checked++;
    try {
      const [es, as] = await db.getAll(db.collection(INDEX).doc(up(d.sku)), db.collection(ALIASES).doc(str(d.listingId, 30) || "0")), entry = dataOf(es), files = await filesOf(deps, entry);
      const p0 = plan(d, entry, dataOf(as), files, nowOf(deps));
      if (!p0.state && !p0.alias && !p0.etsy && !d.repair) return;   // whole: nothing to read again, nothing to write
      const r = await fixRecord(deps, s.id, files);
      if (r && r.did && r.did.length) out.fixed.push({ id: s.id, did: r.did });
      if (r && r.state) out.flagged.push({ id: s.id, state: r.state });
    } catch (e) { warn("record " + s.id, e); out.error = String((e && e.message) || e).slice(0, 120); }
  };
  for (let i = 0; i < docs.length; i += 8) await Promise.all(docs.slice(i, i + 8).map(one));   // (a function has seconds, not minutes: eight at a time)
  return out;
}

/* ═══ 3 · intents ═══ */
async function intents(deps, o) {
  const { db } = deps, now = nowOf(deps), out = { deleted: 0 };
  const rows = [];
  for (const [states, limit] of [[["done", "cancelled"], 100], [["wanted"], 100]]) {
    const snap = await db.collection(INTENTS).where("state", "in", states).limit(limit).get();
    for (const s of snap.docs) rows.push({ id: s.id, d: dataOf(s) || {} });
  }
  for (const r of rows) {
    const t = num(r.d.updatedAtMs) || tsMs(r.d.updatedAt) || num(r.d.at);
    if (!t) continue;
    if (r.d.state === "wanted") {
      if (now - t < INTENT_WANTED_MS) continue;
      const rid = str(r.d.rid, 30) || r.id, lid = str(r.d.listingId, 30), ids = [rid].concat(lid ? [rid + "-" + lid] : []);
      if ((await db.getAll(...ids.map(i => db.collection(COLL).doc(i)))).some(x => x.exists)) continue;   // saved after all: the commit marks it done
    } else if (now - t < INTENT_DONE_MS) continue;
    try { await db.collection(INTENTS).doc(r.id).delete(); out.deleted++; } catch (e) { warn("intent not removed " + r.id, e); }
  }
  return out;
}

/* ═══ 4 · the Etsy steps, driven by the server ═══ */
/** The saves whose Etsy step is not done and not final, oldest first. */
async function pendingEtsy(deps, max) {
  const { db } = deps, now = nowOf(deps), R = EtsySku().REASONS;
  // COST (Oct 2026): two queries instead of one "in" over all four states: a failed save that can never be retried (retry:false) stayed in that
  // query and was read on every pass for ever. Equality filters only, so no composite index is needed.
  const snaps = await Promise.all([
    db.collection(COLL).where("etsy.state", "in", ["pending", "unverified", "writing"]).limit(200).get(),
    db.collection(COLL).where("etsy.state", "==", "failed").where("etsy.retry", "==", true).limit(200).get()
  ]);
  const rows = [];
  for (const s of snaps[0].docs.concat(snaps[1].docs)) {
    const d = dataOf(s) || {}, e = d.etsy && typeof d.etsy === "object" ? d.etsy : {};
    if (d.phase !== "repo" || d.rehearsal === true || e.state === "linked") continue;
    if (e.state === "failed" && (e.retry === false || (R[e.reason] && R[e.reason].retry === false))) continue;   // a person's matter, or nothing to retry
    if (+e.runningUntil > now) continue;                                                                          // somebody is running it
    const cool = e.reason === "scope" || e.reason === "not-connected" ? SCOPE_COOLDOWN_MS : COOLDOWN_MS;
    if (e.state !== "writing" && +e.at && now - +e.at < cool) continue;                                           // tried a moment ago
    rows.push({ id: s.id, rid: str(d.rid, 30), listingId: str(d.listingId, 30), at: num(d.at) || 0 });
  }
  rows.sort((a, b) => a.at - b.at);
  return rows.slice(0, max);
}
async function etsySteps(deps, o) {
  const out = { ran: [] };
  if (typeof deps.etsySku !== "function") return out;
  const max = Math.min(5, Math.max(1, Math.floor(+o.maxEtsy) || 3));
  const t0 = Date.now();
  for (const r of await pendingEtsy(deps, max)) {
    if (out.ran.length && Date.now() - t0 > ETSY_BUDGET_MS) break;   // (a function has 10 seconds by default; the rest waits for the next pass)
    try {
      const a = await deps.etsySku({ rid: r.rid, listingId: r.listingId });
      out.ran.push({ id: r.id, state: a && a.state, reason: a && a.reason });
      if (a && a.reason === "scope") break;   // Etsy needs Paul: the other saves would only ask the same
    } catch (e) { warn("etsy step " + r.id, e); out.ran.push({ id: r.id, state: "error" }); }
  }
  return out;
}

/* ═══ the op ═══ */
async function repair(deps, body) {
  body = body && typeof body === "object" ? body : {};
  if (isSandbox(body)) return { ok: true, sandbox: true, skipped: "sandbox" };
  const { db } = deps, now = nowOf(deps), targeted = !!body.rid, timed = Number.isFinite(+body.olderThanMs) && body.olderThanMs !== "" && body.olderThanMs != null;
  const sref = db.collection(STATE).doc("state"), report = { ok: true, at: now };
  let st = {};
  if (!targeted) {
    // one pass at a time, and not more than every few minutes unless a person (or a test) forces it
    const gate = await db.runTransaction(async tx => {
      const s = dataOf(await tx.get(sref)) || {};
      if (+s.runningUntil > now) return { skip: "running", s };
      if (!body.force && !timed && +s.lastRunAt && now - +s.lastRunAt < THROTTLE_MS) return { skip: "recent", s };
      await tx.set(sref, Object.assign({}, s, { runningUntil: now + PASS_LEASE_MS, startedAt: now }));
      return { s };
    });
    if (gate.skip) return { ok: true, skipped: gate.skip, lastRunAt: num(gate.s.lastRunAt) || null };
    st = gate.s;
  }
  const t0 = Date.now(), step = async (name, fn) => {
    if (Date.now() - t0 > PASS_BUDGET_MS) { report[name] = { skipped: "time" }; return; }   // (what does not fit waits for the next pass: the cursor and the age rules make every step resumable)
    try { report[name] = await fn(); } catch (e) { warn(name, e); report[name] = { error: String((e && e.message) || e).slice(0, 120) }; }
  };
  try {
    if (!targeted && body.etsy === true) await step("etsy", () => etsySteps(deps, body));   // (first: it is what the shop is waiting for)
    await step("records", () => records(deps, { rid: body.rid, listingId: body.listingId, limit: body.limit, cursor: st.cursor }));
    // COST (Oct 2026): the day-scale steps, once a day on an unforced pass (see the header)
    const daily = !targeted && (body.force || timed || !(now - (+st.dailyAt || 0) < DAILY_MS));
    if (daily) await step("orphans", () => orphans(deps, { olderThanMs: timed ? +body.olderThanMs : GRACE_MS }));
    if (daily) await step("intents", () => intents(deps, {}));
    if (!targeted && !daily) report.orphans = report.intents = { skipped: "daily", dailyAt: +st.dailyAt || null };
    if (daily && report.orphans && !report.orphans.error && !report.orphans.skipped && report.intents && !report.intents.error && !report.intents.skipped) report.dailyDone = true;
  } finally {
    if (!targeted) {
      const cur = report.records && !report.records.error && !report.records.skipped ? report.records.cursor : st.cursor || 0;
      const daily = report.dailyDone ? { dailyAt: now } : {}; delete report.dailyDone;
      try { await sref.set(Object.assign({ cursor: cur || 0, lastRunAt: now, runningUntil: 0, last: { orphans: report.orphans && report.orphans.deleted || 0, checked: report.records && report.records.checked || 0 } }, daily), { merge: true }); }
      catch (e) { warn("state not written", e); }
    }
  }
  return report;
}

module.exports = { repair, orphans, records, intents, etsySteps, pendingEtsy, plan, fixRecord, GRACE_MS, SHORT_MS, THROTTLE_MS, DAILY_MS, STATE, DIR };
