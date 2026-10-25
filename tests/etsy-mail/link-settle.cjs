// "Messages still missing" that never cleared (Paul, 25 Oct 2026: "short for 6 days after 25 tries", "the system is unable to recover and also to reset the error
// message"), offline. The real _etsyMailLinkConsistency.js and _etsyMailLinkHealth.js over the in-memory Firestore of tests/charm-nest/_sandboxFakes.cjs; the test
// plays the Chrome extension by finishing jobs (and, where it says so, storing messages or marking a read incomplete). Nothing reads live data or calls Etsy.
//   · a conversation whose newest message the record claims but no read ever finds (every read finishes, nothing is stored, no read is "complete") settles BY
//     ITSELF after three such reads, stays settled while it stays the same, and is reported again the moment it changes;
//   · a read that stores something starts the count again; a long outage of the helper (jobs waiting for days) never settles anything by itself;
//   · why a read was no proof (readIncomplete) is said in the notice;
//   · a person's "Matches Etsy" (resolve) closes every gap of the notice they saw, for everybody: the light goes green, the re-reads stop, and the gap comes back
//     only if the conversation changes; a notice that changed meanwhile is not cleared blind;
//   · a gap of COUNTS (older messages missing) is never settled by reads that were not complete: it waits for proof or a person.
//   node tests/etsy-mail/link-settle.cjs
"use strict";
const path = require("path");
const Fk = require("../charm-nest/_sandboxFakes.cjs"); Fk.install();
const { store, TS, writes, canon } = Fk;
const fnDir = path.join(__dirname, "../../netlify/functions");
const warn = [], realWarn = console.warn; console.warn = (...a) => warn.push(a.map(String).join(" "));
const fetchCalls = []; const okFetch = global.fetch; global.fetch = async (...a) => { fetchCalls.push(String(a[0])); return okFetch(...a); };

const CON = require(path.join(fnDir, "_etsyMailLinkConsistency.js"));
const LH = require(path.join(fnDir, "_etsyMailLinkHealth.js"));
const db = require(path.join(fnDir, "firebaseAdmin")).firestore();

const fails = [];
const check = (ok, what) => { if (!ok) fails.push(what); console.log((ok ? "  ok   " : "  FAIL ") + what); };
const MIN = 60e3, HOUR = 3600e3;
const T0 = Date.UTC(2026, 9, 12, 8, 0, 0);          // a night (08:00 UTC)
const T = ms => new TS(ms);
const put = (k, v) => store.set(k, v);
const msg = (tid, id, dir, ms) => put(`EtsyMail_Threads/${tid}/messages/${id}`, { direction: dir, text: id, timestamp: T(ms) });
const thread = (tid, over) => put(`EtsyMail_Threads/${tid}`, Object.assign({ status: "etsy_scraped", customerName: "Ada Byrne", etsyUsername: "adab", etsyOrderId: "4174973193", etsyConversationUrl: "https://e/" + tid, messageCount: 0, lastInboundAt: T(T0 - 30 * HOUR), updatedAt: T(T0 - 30 * HOUR) }, over || {}));
const jobsOf = tid => [...store.keys()].filter(k => k.startsWith("EtsyMail_Jobs/consistency_" + tid + "_")).sort();
const jobSet = (id, status, atMs) => { const j = store.get("EtsyMail_Jobs/" + id); store.set("EtsyMail_Jobs/" + id, Object.assign({}, j, { status, updatedAt: T(atMs) })); };
const cfg = () => store.get("EtsyMail_Config/linkConsistency");
const healthy = at => ({ global: {}, watcher: { enabled: true }, gmail: { lastSyncCompletedAt: T(at - MIN) }, helper: { seenAtMs: at - 20e3 }, scrapeHealth: { consecutiveBad: 0 }, mirror: { enabled: true, lastSyncCompletedAt: T(at - 2 * MIN) }, bell: { reconcileAtMs: at - MIN }, monitor: { atMs: at - MIN }, queuedDrafts: [], scrapeJobs: [], readOk: {}, queue: null });
const judged = (at, doc) => LH.evaluate(Object.assign(healthy(at), { consistency: doc || cfg() }), at);
const row = res => res.checks.find(c => c.id === "consistency") || {};
const others = () => [...store].filter(([k]) => /^EtsyMail_Threads\//.test(k)).map(([k, v]) => { const c = JSON.parse(canon(v)); delete c.fullReread; delete c.readComplete; return k + JSON.stringify(c); }).join("|");

put("EtsyMail_OrderLinkMeta/bell", { n: 1, reconcileAtMs: T0 - MIN });
put("EtsyMail_OrderLinkMeta/helper", { seenAtMs: T0 - 20e3 });
put("EtsyMail_Config/global", {});

(async () => {
  const clean = () => { for (const k of [...store.keys()]) if (/^EtsyMail_(Jobs|Threads)\//.test(k)) store.delete(k); store.delete("EtsyMail_Config/linkConsistency"); };
  const finish = (tid, at, status) => { const id = jobsOf(tid).pop().replace("EtsyMail_Jobs/", ""); jobSet(id, status || "succeeded", at); return id; };
  const incomplete = (tid, at, why) => store.set("EtsyMail_Threads/" + tid, Object.assign({}, store.get("EtsyMail_Threads/" + tid), { readIncomplete: { atMs: at, why, mode: "full", pageMessages: 6 } }));
  // one read cycle: the next due try is queued by the look, the helper finishes it (nothing stored), the next look sees it finished
  const cycle = async (tid, at) => { await CON.tick(db, { now: at, prev: cfg() }); const before = jobsOf(tid).length; finish(tid, at + 2 * MIN); await CON.tick(db, { now: at + 5 * MIN, prev: cfg() }); return before; };

  // ── 1 · Paul's conversation: the newest message the record claims is on no read ──
  console.log("The newest message is never found");
  thread("na", { etsyOrderId: "", messageCount: 6, lastInboundAt: T(T0 - 2 * HOUR) });   // no order number known
  for (let i = 1; i <= 6; i++) msg("na", "a" + i, i % 2 ? "inbound" : "outbound", T0 - (60 - i) * HOUR);   // the newest stored is 54 h old, the record says the customer wrote 2 h ago
  let r = await CON.maybeRun(db, { now: T0, force: true });
  check(r.doc.gaps.na && r.doc.gaps.na.k === "inbound_missing" && r.doc.gaps.na.tries === 1 && r.doc.gaps.na.sn === T0 - 54 * HOUR, "one gap (newest message missing), its newest stored time kept, one re-read asked");
  finish("na", T0 + 3 * MIN); incomplete("na", T0 + 3 * MIN, "Scraped messages matched none of the stored ones");
  await CON.tick(db, { now: T0 + 6 * MIN, prev: cfg() });
  let g = cfg().gaps.na;
  check(g && g.reads === 1 && g.same === 1 && g.why === "Scraped messages matched none of the stored ones", "the first read finished, stored nothing: counted once, and why it was no proof is kept");
  let c = row(judged(T0 + 7 * MIN));
  check(/The helper's last read was not a complete one: Scraped messages matched none of the stored ones\./.test(c.text), "the notice says why the read was no proof: " + c.text);
  // a read that stores something (an older message turns up) starts the count again
  let at = T0 + 30 * MIN;
  await CON.tick(db, { now: at, prev: cfg() }); msg("na", "a0", "inbound", T0 - 70 * HOUR); finish("na", at + 2 * MIN); await CON.tick(db, { now: at + 5 * MIN, prev: cfg() });
  g = cfg().gaps.na;
  check(g && g.reads === 2 && g.same === 0 && g.s === 7, "a read that stored a message starts the count again (7 stored now, still short of the newest)");
  at = T0 + 2 * HOUR; await cycle("na", at);
  check(cfg().gaps.na && cfg().gaps.na.same === 1, "one more read storing nothing: 1");
  at += 4 * HOUR; await cycle("na", at);
  check(cfg().gaps.na && cfg().gaps.na.same === 2, "two");
  at += 7 * HOUR; const before = writes.length; await cycle("na", at);
  let d = cfg();
  check(!d.gaps.na && d.healed[0].how === "settled" && d.proven.na && d.proven.na.how === "settled", "the third read in a row that stores nothing settles it BY ITSELF (nobody pressed anything)");
  check(judged(at + 6 * MIN).level === "ok", "the light is green, the notice and the banner are gone");
  check(!writes.slice(before).some(w => w.kind === "delete" || /\/messages\//.test(w.path)), "nothing was deleted and no message written");
  const jobsBefore = jobsOf("na").length;
  r = await CON.maybeRun(db, { now: T0 + 24 * HOUR + 10 * MIN });
  check(r.ran && !r.doc.gaps.na && jobsOf("na").length === jobsBefore, "the next night it is not reported again and no re-read is asked");
  // the conversation changes: the customer writes again and that message is not stored either
  store.set("EtsyMail_Threads/na", Object.assign({}, store.get("EtsyMail_Threads/na"), { lastInboundAt: T(T0 + 30 * HOUR), updatedAt: T(T0 + 30 * HOUR) }));
  r = await CON.maybeRun(db, { now: T0 + 48 * HOUR + 10 * MIN });
  check(r.doc.gaps.na && r.doc.gaps.na.k === "inbound_missing" && r.doc.gaps.na.tries === 1, "a NEW missing message is reported (and re-read) again");
  clean();

  // ── 2 · a long outage of the helper never settles anything ──
  console.log("The helper is away for five days");
  thread("nb", { messageCount: 4, lastInboundAt: T(T0 - 2 * HOUR) });
  for (let i = 1; i <= 4; i++) msg("nb", "b" + i, i % 2 ? "inbound" : "outbound", T0 - (60 - i) * HOUR);
  await CON.maybeRun(db, { now: T0, force: true });
  for (let h = 1; h <= 5 * 24; h += 6) await CON.tick(db, { now: T0 + h * HOUR, prev: cfg() });
  g = cfg().gaps.nb;
  check(g && g.pend === true && g.tries === 1 && (g.same || 0) === 0 && jobsOf("nb").length === 1, "five days with the read waiting: one job, nothing counted, nothing settled, no pile of jobs");
  // the helper comes back: three reads that find nothing settle it
  at = T0 + 5 * 24 * HOUR; finish("nb", at); await CON.tick(db, { now: at + 5 * MIN, prev: cfg() });
  for (let k = 0; k < 2; k++) { at += 7 * HOUR; await cycle("nb", at); }
  check(!cfg().gaps.nb && cfg().healed[0].how === "settled", "when it is back, three finished reads that find nothing settle it");
  clean();

  // ── 3 · "Matches Etsy": a person's reset ──
  console.log("Matches Etsy");
  thread("nc", { messageCount: 12, lastInboundAt: T(T0 - 40 * HOUR) });
  for (let i = 1; i <= 7; i++) msg("nc", "c" + i, i % 2 ? "inbound" : "outbound", T0 - (50 - i) * HOUR);
  await CON.maybeRun(db, { now: T0, force: true });
  for (let k = 0; k < 4; k++) { finish("nc", T0 + (k * 7 + 1) * HOUR); await CON.tick(db, { now: T0 + (k * 7 + 7) * HOUR, prev: cfg() }); }
  check(cfg().gaps.nc && cfg().gaps.nc.k === "fewer_than_counted" && cfg().gaps.nc.reads >= 3, "a gap of COUNTS (5 older messages short) is not settled by reads that were not complete: " + JSON.stringify({ reads: cfg().gaps.nc && cfg().gaps.nc.reads }));
  const now3 = T0 + 80 * HOUR; await CON.tick(db, { now: now3, prev: cfg() });
  let res = judged(now3 + MIN); c = row(res);
  check(res.gap && res.gap.esc && /press Matches Etsy if nothing is missing/.test(c.text), "red, and the notice says what to do: " + c.text);
  const sig = res.gap.sig;
  let rv = await CON.resolve(db, { sig: "notthesig", by: "Paul", now: now3 + 2 * MIN });
  check(rv.changed && cfg().gaps.nc, "a press on a notice that is not the one shown clears nothing");
  rv = await CON.resolve(db, { sig, by: "  Paul   K ", now: now3 + 2 * MIN });
  d = cfg();
  check(rv.ok && rv.n === 1 && !Object.keys(d.gaps).length && d.healed[0].how === "checked" && d.healed[0].by === "Paul K" && d.proven.nc && d.proven.nc.d === 5, "Matches Etsy closes it for everybody, says who, and remembers the drift");
  check(judged(now3 + 3 * MIN).level === "ok", "the light is green at once");
  const nJobs = jobsOf("nc").length;
  for (let h = 1; h <= 30; h += 5) await CON.tick(db, { now: now3 + h * HOUR, prev: cfg() });
  r = await CON.maybeRun(db, { now: T0 + 4 * 24 * HOUR + 10 * MIN });
  check(jobsOf("nc").length === nJobs && !r.doc.gaps.nc, "the re-reads stop, and the next night does not bring it back");
  store.set("EtsyMail_Threads/nc", Object.assign({}, store.get("EtsyMail_Threads/nc"), { messageCount: 15, updatedAt: T(T0 + 4 * 24 * HOUR - 5 * HOUR) }));
  r = await CON.maybeRun(db, { now: T0 + 5 * 24 * HOUR + 10 * MIN });
  check(r.doc.gaps.nc && r.doc.gaps.nc.c === 15, "but when that conversation changes (3 more counted, none stored) it is reported again");
  rv = await CON.resolve(db, { sig: "x", now: T0 + 5 * 24 * HOUR + 11 * MIN }); clean();
  rv = await CON.resolve(db, { sig: "x", now: T0 + 6 * 24 * HOUR });
  check(rv.nothing, "with nothing open there is nothing to clear");
  check(!writes.some(w => /^EtsyMail_(Drafts|OrderLinks|SendQueue)/.test(w.path)), "no reply box, question or send queue was touched");

  console.warn = realWarn;
  console.log(fails.length ? `\nFAILED: ${fails.length}\n- ` + fails.join("\n- ") : "\nlink-settle OK");
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.warn = realWarn; console.error(e); console.log(warn.slice(-5).join("\n")); process.exit(1); });
