/*  netlify/functions/_charmEngraveOps.js — the engraving presses for the cloud (cloud plan v2, build step 11): the page's Approve, sent to the cloud, and what the cloud's back-file
 *  tasks say of themselves. One line in charmNestLibrary.js puts them in OPS (hard-parts §0.2: an op lives in its own module, a factory of the library's own helpers).
 *
 *    engraveApprove   (PRESS)  { runId, lineKey, jobKey, at, by, dev, text, place, fields?, artAtBy? }
 *        A person's Approve of a placed piece, while the cloud carries the run and "engrave" is one of its lanes (drive.cloudSteps). In ONE transaction:
 *          · the run's carrier is the cloud (otherwise 409 with words and `local: true`: the page approves as today, on its own);
 *          · the line's engraving record (Charm_Nest_Engrave/<line>) takes the words, the place and the fields the person approved, and the job waits in `approving`
 *            { at, by, text, task } (SEALAFTERSAVE: no approved state, no seal, no event until its file is saved; every computer reads that from the record);
 *          · an older approval of the same job still being written is superseded (its task ends "superseded");
 *          · the task Charm_Engrave_Files/<jobKey>@<at> is created (create(): the same press sent again finds it and answers it, nothing twice).
 *        The cloud's driver is kicked (DRIVE_TRIGGERS has engraveApprove; never while the drive is off). The page that sent it shows the task and, when the record says
 *        "written", the seal. Refused in the sandbox, without a signed-in name, for an unplaced or undecided face, and while the drive is off (`local: true`).
 *    engraveFilesGet  (READ)   { runId } | { lineKeys: [...] } | { jobKeys: [...] }  → { tasks: [{ id, lineKey, jobKey, face, at, by, state, stage, note, why, done, total, endedAt }] }
 *        What the cloud's back-file tasks are doing (the page's "Saving the back file…" and its reason when it stopped). No staged record and no token is answered.
 *  Nothing here calls Etsy; nothing writes a sheet, a pool row or a file (the cloud's task does that, fenced, in its pass).                                              */
"use strict";
const Faces = require("../../charm-nest-engrave-faces.js");

const TASKS = "Charm_Engrave_Files", CARRIER = "Charm_Nest_Run_Carrier", ACTIVE = ["waiting", "working"];
const FIELD_KEYS = ["lines", "lineInput", "lineMode", "lineGap", "fontKey", "fontName", "fontAsked", "wantSize", "nudged", "turnUp", "upSystem", "orientVersion", "decision", "decidedAt", "decidedBy"];

module.exports = function engraveOps(D) {
  const { db, col, FV, str, num, isId, ENGR, ENGR_LINE, ENGR_MAX } = D;
  const fin = v => typeof v === "number" && Number.isFinite(v);
  const taskIdOf = (jobKey, at) => `${String(jobKey).replace(/\//g, "_")}@${Math.round(at)}`;
  /** The place a person approved: the words, their size, turn and centre in the face's view, the view's own centre and up angle, the font. null when it is not a placement. */
  function cleanPlace(p) {
    if (!p || typeof p !== "object") return null;
    if (p.artOnly === true) return { artOnly: true, upAngle: fin(+p.upAngle) ? +p.upAngle : null, ...(fin(+p.cx) && fin(+p.cy) ? { cx: +p.cx, cy: +p.cy } : {}) };
    const centre = Array.isArray(p.centre) && p.centre.length === 2 && p.centre.every(v => fin(+v)) ? p.centre.map(Number) : null;
    const lines = Array.isArray(p.lines) ? p.lines.slice(0, 12).map(l => String(l == null ? "" : l).slice(0, 200)) : null;
    if (!centre || !lines || !lines.length || !(+p.size > 0) || !(+p.size < 1000)) return null;
    const out = { centre, angle: fin(+p.angle) ? +p.angle : 0, size: +p.size, lines, weight: p.weight === "Semibold" ? "Semibold" : "Regular", fittedMax: +p.fittedMax > 0 ? +p.fittedMax : +p.size,
      upAngle: p.upAngle == null || !fin(+p.upAngle) ? null : +p.upAngle, fontKey: str(p.fontKey, 60) || null };
    if (p.lineGap != null && fin(+p.lineGap)) out.lineGap = +p.lineGap;
    if (fin(+p.cx) && fin(+p.cy)) { out.cx = +p.cx; out.cy = +p.cy; }
    return out;
  }
  const plain = v => { try { return v === undefined ? undefined : JSON.parse(JSON.stringify(v)); } catch (_) { return undefined; } };
  /** JSON with its keys in order (a map read back from Firestore need not keep the order it was written in). */
  const stable = v => (v === null || typeof v !== "object" ? JSON.stringify(v === undefined ? null : v) : Array.isArray(v) ? "[" + v.map(stable).join(",") + "]" : "{" + Object.keys(v).filter(k => v[k] !== undefined).sort().map(k => JSON.stringify(k) + ":" + stable(v[k])).join(",") + "}");
  const isPoolId = D.isPoolId || (s => /^(?:\d{5,20}|CUSTOM-\d{8}-\d{1,6})_\d{5,20}_\d{1,3}$/.test(String(s || "")));

  async function op_engraveApprove(b) {
    if (D.prefix()) return { error: "The sandbox approves engravings on this computer: the cloud writes the shop's own runs only", status: 409, local: true };
    const runId = str(b.runId, 80), lineKey = str(b.lineKey, 120), jobKey = str(b.jobKey, 160), by = str(b.by, 80).trim(), dev = str(b.dev, 60) || null;
    if (!isId(runId)) return { error: "bad run id", status: 400 };
    if (!ENGR_LINE.test(lineKey)) return { error: "bad line key", status: 400 };
    if (!jobKey || Faces.parseKey(jobKey).lineKey !== lineKey) return { error: "this engraving is not of that order line", status: 400 };
    if (!by) return { error: "Not approved: sign in first (an approval is kept with the name of the person who made it)", status: 400 };
    const place = cleanPlace(b.place); if (!place) return { error: "Not approved: the words are not placed on the charm yet", status: 400 };
    const text = typeof b.text === "string" ? b.text.slice(0, 2000) : "";
    // the lane (P9): the cloud writes a run's engraving files only while it carries the run and "engrave" is one of its lanes; otherwise the page does, as today
    const drive = await D.driveDoc(15000), mode = D.driveModeOf(drive), steps = drive && Array.isArray(drive.cloudSteps) ? drive.cloudSteps.map(String) : [];
    if (mode === "off" || !steps.includes("engrave")) return { error: "The cloud does not write engraving files now: this computer saves the approval", status: 409, local: true };
    const now = Date.now(), at0 = Math.round(num(b.at)), at = at0 > now - 10 * 60000 && at0 < now + 60000 ? at0 : now;
    const fields = {}; const f = b.fields && typeof b.fields === "object" ? b.fields : {};
    for (const k of FIELD_KEYS) if (Object.prototype.hasOwnProperty.call(f, k)) { const v = plain(f[k]); if (v !== undefined) fields[k] = v; }
    const artAtBy = {}; if (b.artAtBy && typeof b.artAtBy === "object") for (const [id, v] of Object.entries(b.artAtBy).slice(0, 60)) if (isPoolId(id) && fin(+v)) artAtBy[id] = +v;
    const ref = col(ENGR).doc(lineKey), cref = db.collection(CARRIER).doc(runId), taskId = taskIdOf(jobKey, at), tref = db.collection(TASKS).doc(taskId);
    const out = await db.runTransaction(async tx => {
      const [cs, es, ts] = await tx.getAll(cref, ref, tref);
      const c = cs.exists ? (cs.data() || {}).carrier : null;
      if (!c || c.dev !== "cloud") return { error: `${(c && c.by) || "This computer"} carries this run: the approval is saved on the computer that carries it`, status: 409, local: true };
      if (ts.exists) { const t = ts.data() || {}; return { ok: true, taskId, at: num(t.at) || at, state: t.state || null, replayed: true }; }
      const active = await tx.get(db.collection(TASKS).where("jobKey", "==", jobKey));
      const cur = es.exists ? es.data() || {} : null;
      let rec = null; try { rec = cur && cur.rec ? JSON.parse(cur.rec) : null; } catch (_) { rec = null; }
      if (!rec || typeof rec !== "object") rec = { v: 1, override: null, engrave: null, jobs: [] };
      if (!Array.isArray(rec.jobs)) rec.jobs = [];
      let job = rec.jobs.find(j => j && j.key === jobKey);
      if (!job) { const k = Faces.parseKey(jobKey); job = Object.assign({ key: jobKey }, k.face === "front" ? { face: "front" } : {}, k.slot ? { slot: k.slot, rowKey: lineKey } : {}); rec.jobs.push(job); }
      if (["approved", "written"].includes(job.state) && num(job.approvedAt) >= at) return { ok: true, already: true, at: num(job.approvedAt), state: job.state };
      if (["approved", "written"].includes(job.state)) return { error: "Not approved: it is approved already. Change the words or the place first, then approve again", status: 409 };
      if (["none", "skipped"].includes(job.state)) return { error: "Not approved: this face is set to no engraving", status: 409 };
      // the same approval pressed again while its file is being written: that one stands
      const live = active.docs.map(d => Object.assign({ id: d.id }, d.data() || {})).filter(t => ACTIVE.includes(t.state));
      const same = live.find(t => t.text === text && stable(t.place) === stable(place));
      if (same) return { ok: true, taskId: same.id, at: num(same.at), state: same.state, same: true };
      Object.assign(job, fields, { text, lines: place.lines ? place.lines.slice() : job.lines, place, state: "review", reason: null, approving: { at, by, text, task: taskId } });
      if (place.fontKey) job.fontKey = place.fontKey;
      if (Array.isArray(fields.lineInput)) job.lineInput = fields.lineInput;
      const json = JSON.stringify(rec);
      if (json.length > ENGR_MAX) return { error: `a line's engraving is plain JSON of at most ${ENGR_MAX / 1000} KB`, status: 400 };
      const rev = (cur && Number.isFinite(+cur.rev) ? +cur.rev : 0) + 1;
      for (const t of live) tx.set(db.collection(TASKS).doc(t.id), { state: "superseded", why: "a newer approval of this engraving was pressed", supersededBy: taskId, endedAt: now, updatedAt: now }, { merge: true });
      tx.set(ref, { key: lineKey, order: lineKey.split("_")[0], rec: json, at: now, by, dev, sig: null, rev, updatedAt: FV.serverTimestamp() });
      const k = Faces.parseKey(jobKey);
      tx.create(tref, { id: taskId, runId, lineKey, jobKey, face: k.face === "front" ? "front" : "back", slot: k.slot || null, at, by, dev, text, place, fields, artAtBy: Object.keys(artAtBy).length ? artAtBy : null,
        origin: "press", state: "waiting", stage: "check", note: "waiting for the cloud's next pass", copies: null, done: {}, staged: {}, files: {}, sheets: {}, errors: 0, createdAt: now, updatedAt: now,
        expireAt: new Date(now + 30 * 86400000) });
      return { ok: true, taskId, at, rev, superseded: live.map(t => t.id) };
    });
    if (out && out.ok && !out.replayed && !out.same && !out.already) D.feedOrders(b, [[lineKey]]);   // (FEED: every computer reads the line again: its face waits on the cloud)
    return out;
  }

  async function op_engraveFilesGet(b) {
    let docs = [];
    if (Array.isArray(b.jobKeys) && b.jobKeys.length) {
      const keys = [...new Set(b.jobKeys.map(k => str(k, 160)).filter(Boolean))].slice(0, 60);
      for (let i = 0; i < keys.length; i += 30) { const qs = await db.collection(TASKS).where("jobKey", "in", keys.slice(i, i + 30)).get(); docs.push(...qs.docs); }
    } else if (Array.isArray(b.lineKeys) && b.lineKeys.length) {
      const keys = [...new Set(b.lineKeys.map(k => str(k, 120)).filter(k => ENGR_LINE.test(k)))].slice(0, 60);
      for (let i = 0; i < keys.length; i += 30) { const qs = await db.collection(TASKS).where("lineKey", "in", keys.slice(i, i + 30)).get(); docs.push(...qs.docs); }
    } else if (isId(str(b.runId, 80))) {
      const qs = await db.collection(TASKS).where("runId", "==", str(b.runId, 80)).limit(500).get(); docs = qs.docs;
    } else return { error: "runId, lineKeys or jobKeys required", status: 400 };
    const tasks = docs.map(d => { const t = d.data() || {}; const copies = Array.isArray(t.copies) ? t.copies : []; return { id: d.id, runId: t.runId || null, lineKey: t.lineKey, jobKey: t.jobKey, face: t.face, at: num(t.at), by: t.by || "", state: t.state, stage: t.stage || null, note: t.note || null, why: t.why || null, origin: t.origin || null, done: Object.keys(t.done || {}).length, total: copies.length, updatedAt: num(t.updatedAt), endedAt: num(t.endedAt) || null }; });
    tasks.sort((x, y) => x.at - y.at);
    return { tasks, now: Date.now() };
  }

  return { engraveApprove: op_engraveApprove, engraveFilesGet: op_engraveFilesGet };
};
module.exports.TASKS = TASKS;
module.exports.FIELD_KEYS = FIELD_KEYS;
