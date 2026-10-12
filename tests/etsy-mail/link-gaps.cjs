// "Messages missing": the life of a gap (MAILMISS, 2026-10-12), offline. The real _etsyMailLinkConsistency.js and _etsyMailLinkHealth.js
// over the in-memory Firestore of tests/charm-nest/_sandboxFakes.cjs; Etsy, the network and the extension are stand-ins (the test plays the
// extension by finishing a job and storing messages). Nothing here reads live data, calls Etsy or sends anything.
//   · what the notice says: the order number (never a name), how many short, what the inbox is doing, last try and next try;
//   · it clears by itself, with nobody pressing anything: when the stored messages match again, and when a COMPLETE read of Etsy's
//     conversation found nothing missing (the counter was off) - and it does not come back for the same drift, but does for a worse one;
//   · the self-heal: one re-read at a time, back-off 20 min / 1 h / 3 h / 6 h, a day's cap, Check now adds a try only after 10 minutes,
//     red only after 72 h, four tries and two finished re-reads;
//   · Dismiss hides THIS gap (and only this gap) for everybody, while the gap, the checking and the re-reads go on;
//   · a conversation that cannot be read is not "matched"; nothing is deleted; no message or conversation field changes except the mark.
//   node tests/etsy-mail/link-gaps.cjs
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
  // ── 1 · a real gap: the sentence, the self-heal, and the notice clearing by itself ──
  console.log("A conversation 3 messages short");
  thread("tx", { messageCount: 8, lastInboundAt: T(T0 - 29 * HOUR) });
  for (let i = 1; i <= 5; i++) msg("tx", "m" + i, i % 2 ? "inbound" : "outbound", T0 - (40 - i) * HOUR);
  const SNAP = others();
  let r = await CON.maybeRun(db, { now: T0, force: true });
  let d = r.doc, g = d.gaps.tx;
  check(others() === SNAP, "the scan changed no conversation field and no message (the mark apart)");
  check(Object.keys(d.gaps).length === 1 && g && g.o === "4174973193" && g.k === "fewer_than_counted" && g.c === 8 && g.s === 5 && g.tries === 1, "one open gap, with the order number, 8 expected and 5 stored, one re-read asked");
  check(jobsOf("tx").length === 1 && jobsOf("tx")[0] === "EtsyMail_Jobs/consistency_tx_20261012" && store.get(jobsOf("tx")[0]).payload.fullRead === true, "a complete re-read is queued as a scrape job on a deterministic id");
  check(store.get("EtsyMail_Threads/tx").fullReread && store.get("EtsyMail_Threads/tx").fullReread.atMs === T0, "the conversation carries the mark that makes the extension read it in full");
  let res = judged(T0 + 2 * MIN), c = row(res);
  console.log("   sentence:", c.text);
  check(res.level === "warn" && res.short === "Messages missing" && res.kind === "consistency", "amber, 'Messages missing'");
  check(/Order 4174973193 shows 3 fewer messages in the inbox than expected \(8 expected, 5 stored\)\./.test(c.text), "it says which conversation (the order number) and the counts in plain words");
  check(!/Ada|Byrne|adab/i.test(JSON.stringify([c, res.gap, d])), "no buyer name or username anywhere in the notice or the record");
  check(/asked the Etsy helper to re-read it 2 min ago and is waiting for the helper's turn/.test(c.text), "it says what it is doing and when it last tried");
  check(res.gap.n === 1 && res.gap.items[0].st === "wait" && res.gap.items[0].lt === T0 && res.gap.sig.length === 12, "the sorter gets the structure: count, state, last try, a signature");

  // while the read waits, nothing is queued again, however often it is looked at
  let t = await CON.tick(db, { now: T0 + 5 * MIN, prev: cfg() });
  check(t.ran && jobsOf("tx").length === 1 && cfg().gaps.tx.tries === 1 && cfg().gaps.tx.pend === true, "the 5-minute look finds the read still waiting: no second job");
  t = await CON.recheck(db, { now: T0 + 30 * MIN, manual: true });
  check(jobsOf("tx").length === 1 && t.results[0].outcome === "waiting", "Check now while it waits: no second job either, and it says 'waiting'");

  // the extension reads Etsy in full: stores the 3 missing messages (the snapshot adds 3 to the counter that already counted them) and marks the read complete
  jobSet("consistency_tx_20261012", "claimed", T0 + 10 * MIN);
  for (let i = 6; i <= 8; i++) msg("tx", "m" + i, i % 2 ? "inbound" : "outbound", T0 - (40 - i) * HOUR);
  store.set("EtsyMail_Threads/tx", Object.assign({}, store.get("EtsyMail_Threads/tx"), { messageCount: 11, updatedAt: T(T0 + 40 * MIN), readComplete: { atMs: T0 + 40 * MIN, reqAtMs: T0, pageMessages: 8, inserted: 3 } }));
  delete store.get("EtsyMail_Threads/tx").fullReread;
  jobSet("consistency_tx_20261012", "succeeded", T0 + 41 * MIN);
  const before = writes.length;
  t = await CON.tick(db, { now: T0 + 45 * MIN, prev: cfg() });
  d = cfg();
  check(Object.keys(d.gaps).length === 0 && d.healed[0].how === "proven" && d.healed[0].o === "4174973193", "the next 5-minute look closes it BY ITSELF: a complete read of Etsy found every message stored (nothing pressed)");
  check(d.proven.tx && d.proven.tx.d === 3, "how far the counter was off is remembered");
  res = judged(T0 + 46 * MIN);
  check(res.level === "ok" && row(res).level === "ok" && res.problem === "", "the light is green again");
  check(!writes.slice(before).some(w => w.kind === "delete" || /\/messages\//.test(w.path) || /^EtsyMail_Threads\//.test(w.path)), "the look itself deleted nothing and wrote no message and no conversation (only its own record)");
  // the same drift is not reported again; a worse one is
  const night2 = T0 + 24 * HOUR + 30 * MIN;
  r = await CON.maybeRun(db, { now: night2 });
  check(r.ran && Object.keys(r.doc.gaps).length === 0 && r.doc.mismatched === 0, "the next night the same drift (3) is not reported again");
  store.set("EtsyMail_Threads/tx", Object.assign({}, store.get("EtsyMail_Threads/tx"), { messageCount: 13, updatedAt: T(night2 - 5 * HOUR) }));   // two new messages counted, only one stored: a message went missing
  msg("tx", "m9", "inbound", night2 - 6 * HOUR);
  r = await CON.maybeRun(db, { now: night2 + 24 * HOUR + 10 * MIN });
  check(r.doc.mismatched === 1 && r.doc.gaps.tx && r.doc.gaps.tx.tries === 1 && r.doc.gaps.tx.c === 13 && r.doc.gaps.tx.s === 9, "but a drift that GROWS (a message really missing) is reported and re-read again: " + JSON.stringify(r.doc.gaps.tx && { c: r.doc.gaps.tx.c, s: r.doc.gaps.tx.s }));
  // (clean up for the next scenarios)
  for (const k of [...store.keys()]) if (k.startsWith("EtsyMail_Jobs/")) store.delete(k);
  store.delete("EtsyMail_Config/linkConsistency");
  for (const k of [...store.keys()]) if (k.startsWith("EtsyMail_Threads/tx")) store.delete(k);

  // ── 2 · matched: the stored messages agree with the record again ──
  console.log("Counts agree again");
  thread("ty", { messageCount: 4, etsyOrderId: "", lastInboundAt: T(T0 - 40 * HOUR) });
  r = await CON.maybeRun(db, { now: T0, force: true });
  check(r.doc.gaps.ty && r.doc.gaps.ty.k === "claimed_but_none" && r.doc.gaps.ty.o === "", "claimed 4, none stored: one gap, no order number known");
  c = row(judged(T0 + MIN));
  check(/^A conversation \(no order number known\) shows no messages in the inbox, but its record counts 4\./.test(c.text), "no order number: it says so, and still names no buyer: " + c.text);
  for (let i = 1; i <= 4; i++) msg("ty", "p" + i, i % 2 ? "inbound" : "outbound", T0 - (44 - i) * HOUR);
  t = await CON.tick(db, { now: T0 + 6 * MIN, prev: cfg() });
  check(!Object.keys(cfg().gaps).length && cfg().healed[0].how === "matched" && t.results[0].outcome === "matched" && t.results[0].s === 4, "the next look finds 4 stored of 4 expected: closed as 'matched' (result says 4)");
  check(judged(T0 + 7 * MIN).level === "ok", "green without a person");

  // ── 3 · back-off, the day's cap, Check now ──
  console.log("Retry with back-off");
  for (const k of [...store.keys()]) if (/^EtsyMail_(Jobs|Threads\/ty)/.test(k)) store.delete(k);
  store.delete("EtsyMail_Config/linkConsistency");
  thread("tz", { messageCount: 9, lastInboundAt: T(T0 - 40 * HOUR) });
  msg("tz", "q1", "inbound", T0 - 50 * HOUR);
  await CON.maybeRun(db, { now: T0, force: true });
  const finish = (jobId, at, status) => jobSet(jobId, status || "succeeded", at);
  finish("consistency_tz_20261012", T0 + 3 * MIN);
  t = await CON.tick(db, { now: T0 + 10 * MIN, prev: cfg() });
  check(cfg().gaps.tz.reads === 1 && cfg().gaps.tz.pend === false && jobsOf("tz").length === 1, "the read finished and the gap is still there: it waits (20 minutes after the read ended)");
  check(/last re-read it 10 min ago and it is still short; next try in about 13 min/.test(row(judged(T0 + 10 * MIN)).text), "the sentence gives the last try and when the next one is: " + row(judged(T0 + 10 * MIN)).text);
  t = await CON.recheck(db, { now: T0 + 8 * MIN, manual: true });
  check(jobsOf("tz").length === 1 && t.results[0].outcome === "short", "Check now 8 minutes after the last try: looks again (still short), does not queue another yet [" + t.results[0].outcome + "]");
  t = await CON.tick(db, { now: T0 + 24 * MIN, prev: cfg() });
  check(jobsOf("tz").length === 2 && jobsOf("tz")[1].endsWith("_20261012_r1") && cfg().gaps.tz.tries === 2, "after the wait the second try is queued (a new job id, _r1)");
  finish("consistency_tz_20261012_r1", T0 + 30 * MIN);
  t = await CON.tick(db, { now: T0 + 60 * MIN, prev: cfg() });
  check(cfg().gaps.tz.reads === 2 && jobsOf("tz").length === 2, "second read over: the next wait is an hour");
  t = await CON.tick(db, { now: T0 + 30 * MIN + 61 * MIN, prev: cfg() });
  check(jobsOf("tz").length === 3 && cfg().gaps.tz.tries === 3, "an hour later the third try is queued");
  // a person's press: not before 10 minutes after the last try, at most 3 extra a day
  finish("consistency_tz_20261012_r2", T0 + 95 * MIN);
  t = await CON.tick(db, { now: T0 + 96 * MIN, prev: cfg() });
  const lastTry = cfg().gaps.tz.lastTryAtMs;
  t = await CON.recheck(db, { now: lastTry + 4 * MIN, manual: true });
  check(jobsOf("tz").length === 3 && t.results[0].outcome === "short", "Check now 4 minutes after the last try: no new read");
  t = await CON.recheck(db, { now: lastTry + 11 * MIN, manual: true });
  check(jobsOf("tz").length === 4 && t.results[0].queuedNow === true && cfg().gaps.tz.manualN === 1, "Check now 11 minutes after it adds one try (the wait of three hours is for the automatic ones)");
  let presses = 0;
  for (let k = 1; k <= 6; k++) {
    const g2 = cfg().gaps.tz; if (g2.pend) finish(g2.jobId, g2.lastTryAtMs + 2 * MIN);
    const before2 = jobsOf("tz").length;
    await CON.recheck(db, { now: cfg().gaps.tz.lastTryAtMs + 11 * MIN, manual: true });
    if (jobsOf("tz").length > before2) presses++;
  }
  check(cfg().gaps.tz.dayTries <= 6 && jobsOf("tz").length <= 6, "never more than 6 tries a day, whoever asks: " + jobsOf("tz").length);
  check(cfg().gaps.tz.manualN <= 3, "and never more than 3 extra tries by presses: " + cfg().gaps.tz.manualN);

  // ── 4 · red only after a long time ──
  console.log("Red only when it stays");
  const tzNow = cfg().gaps.tz;
  const old = Object.assign({}, cfg(), { gaps: { tz: Object.assign({}, tzNow, { firstAtMs: T0 - 80 * HOUR, tries: 4, reads: 2, pend: false, lastTryAtMs: T0 - HOUR, nextTryAtMs: T0 + 5 * HOUR }) }, dismissed: null });
  res = judged(T0 + 1000, old);
  check(res.level === "down" && res.short === "Messages still missing" && /stayed short for 3 days after 4 tries/.test(row(res).text), "after 80 hours, 4 tries and 2 finished reads it turns red and says what to do: " + row(res).text);
  const young = Object.assign({}, old, { gaps: { tz: Object.assign({}, old.gaps.tz, { firstAtMs: T0 - 20 * HOUR }) } });
  check(judged(T0 + 1000, young).level === "warn", "the same gap after 20 hours is amber");
  const fewReads = Object.assign({}, old, { gaps: { tz: Object.assign({}, old.gaps.tz, { reads: 1 }) } });
  check(judged(T0 + 1000, fewReads).level === "warn", "and with only one finished read it stays amber (the extension may simply not have been there)");

  // ── 5 · Dismiss: this gap, for everybody, and only this gap ──
  console.log("Dismiss");
  for (const k of [...store.keys()]) if (/^EtsyMail_(Jobs|Threads\/tz)/.test(k)) store.delete(k);
  store.delete("EtsyMail_Config/linkConsistency");
  thread("t1", { messageCount: 6, etsyOrderId: "4170000001", lastInboundAt: T(T0 - 40 * HOUR) });
  await CON.maybeRun(db, { now: T0, force: true });
  const sig1 = LH.gapSummary(cfg(), T0 + MIN).sig;
  const jobsBefore = jobsOf("t1").length, gapBefore = JSON.stringify(cfg().gaps.t1);
  let dr = await CON.dismiss(db, { sig: "wrongsig", by: "Paul", now: T0 + MIN });
  check(dr.changed === true && !cfg().dismissed, "a dismissal of a gap that is not the one on screen is refused ('changed'): nothing is hidden");
  dr = await CON.dismiss(db, { sig: sig1, by: "Paul Smith  ", now: T0 + MIN });
  check(dr.ok === true && cfg().dismissed.sig === sig1 && cfg().dismissed.by === "Paul Smith", "Dismiss with the signature on screen is accepted (by the signed-in name)");
  res = judged(T0 + 2 * MIN);
  check(res.level === "ok" && res.problem === "" && row(res).level === "ok" && row(res).data.dismissed === true && /Dismissed \d+ (s|min) ago by Paul Smith; the inbox keeps re-reading it/.test(row(res).text), "the notice is gone from the light and the pill, and the details box still lists it, quietly: " + row(res).text);
  check(JSON.stringify(cfg().gaps.t1) === gapBefore && jobsOf("t1").length === jobsBefore, "the gap itself and its re-read are untouched");
  t = await CON.tick(db, { now: T0 + 10 * MIN, prev: cfg() });
  check(cfg().dismissed && cfg().dismissed.sig === sig1 && cfg().gaps.t1, "the checking goes on and the dismissal holds while the gap is the same");
  jobSet("consistency_t1_20261012", "succeeded", T0 + 12 * MIN);
  await CON.tick(db, { now: T0 + 40 * MIN, prev: cfg() });
  check(cfg().gaps.t1.tries === 2 && jobsOf("t1").length === 2, "the self-heal keeps retrying while it is dismissed");
  check(judged(T0 + 41 * MIN).level === "ok", "...and the notice stays hidden");
  // a new conversation goes missing: the notice is back, for the new gap
  thread("t2", { messageCount: 5, etsyOrderId: "4170000002", lastInboundAt: T(T0 - 41 * HOUR) });
  await CON.maybeRun(db, { now: T0 + 24 * HOUR, force: true });
  res = judged(T0 + 24 * HOUR + MIN);
  check(res.level === "warn" && res.gap.n === 2 && /^2 conversations show fewer messages/.test(row(res).text) && cfg().dismissed, "a second conversation falling short brings the notice back (the old dismissal does not cover it)");
  // the same conversation gets worse
  const sig2 = LH.gapSummary(cfg(), T0 + 24 * HOUR).sig;
  await CON.dismiss(db, { sig: sig2, by: "Paul", now: T0 + 24 * HOUR + 2 * MIN });
  check(judged(T0 + 24 * HOUR + 3 * MIN).level === "ok", "dismissed again (both)");
  store.set("EtsyMail_Threads/t2", Object.assign({}, store.get("EtsyMail_Threads/t2"), { messageCount: 9 }));
  await CON.recheck(db, { now: T0 + 25 * HOUR, manual: true });
  check(judged(T0 + 25 * HOUR + MIN).level === "warn", "when the gap changes (5 short becomes 9) the notice returns");
  // everything settles: the dismissal goes with the gap
  for (const tid of ["t1", "t2"]) { const th = store.get("EtsyMail_Threads/" + tid); store.set("EtsyMail_Threads/" + tid, Object.assign({}, th, { messageCount: 1 })); msg(tid, "z", "inbound", T0 - 39 * HOUR); }
  await CON.recheck(db, { now: T0 + 26 * HOUR, manual: true });
  check(!Object.keys(cfg().gaps).length && cfg().dismissed === null && judged(T0 + 26 * HOUR + MIN).level === "ok", "when the counts agree the gaps close and the dismissal is forgotten");
  dr = await CON.dismiss(db, { sig: "x", by: "Paul", now: T0 + 27 * HOUR });
  check(dr.nothing === true, "Dismiss with nothing open does nothing");

  // ── 6 · an unreadable conversation is not 'matched'; publishing keeps the monitor's stamp ──
  console.log("Honest failures, publishing");
  for (const k of [...store.keys()]) if (/^EtsyMail_(Jobs|Threads\/t[12])/.test(k)) store.delete(k);
  thread("t3", { messageCount: 7, etsyOrderId: "4170000003", lastInboundAt: T(T0 - 40 * HOUR) });
  store.delete("EtsyMail_Config/linkConsistency");
  await CON.maybeRun(db, { now: T0 + 30 * HOUR, force: true });
  const realGetAll = db.getAll; db.getAll = async () => { throw new Error("Firestore unavailable"); };
  t = await CON.recheck(db, { now: T0 + 30 * HOUR + 10 * MIN, manual: true });
  db.getAll = realGetAll;
  check(t.results[0].outcome === "error" && cfg().gaps.t3 && judged(T0 + 31 * HOUR).level === "warn", "a look that fails leaves the gap open and says 'error', never 'matched'");
  put("EtsyMail_Config/linkHealth", { atMs: T0 + 29 * HOUR, level: "ok", short: "", problem: "", checks: [], downSinceMs: 0, incidentId: "", auth: { secretSet: true, context: "production" } });
  const pub = await LH.publish(db, T0 + 31 * HOUR, { keepStamp: true, ev: Object.assign(healthy(T0 + 31 * HOUR), { consistency: cfg(), self: true }) });
  const lhd = store.get("EtsyMail_Config/linkHealth"), bell = store.get("EtsyMail_OrderLinkMeta/bell");
  check(lhd.level === "warn" && lhd.atMs === T0 + 29 * HOUR && lhd.auth && lhd.auth.secretSet === true, "publishing writes the new judgement but keeps the monitor's own time (a stopped monitor still shows as stopped) and the auth note");
  check(bell.link && bell.link.level === "warn" && bell.link.kind === "consistency" && bell.link.gap && bell.link.gap.n === 1 && bell.link.gap.sig === pub.res.gap.sig && bell.reconcileAtMs, "every sorter's summary carries the gap (order, counts, last/next try, signature) and the bell's other fields are kept");

  // ── 6b · more short conversations than the ledger holds: counted, never hidden ──
  console.log("A great many at once");
  for (const k of [...store.keys()]) if (/^EtsyMail_(Jobs|Threads\/)/.test(k)) store.delete(k);
  store.delete("EtsyMail_Config/linkConsistency");
  for (let i = 1; i <= 55; i++) thread("o" + i, { messageCount: 5, etsyOrderId: String(4180000000 + i), lastInboundAt: T(T0 - 60 * HOUR), updatedAt: T(T0 - 60 * HOUR - i * MIN) });
  r = await CON.maybeRun(db, { now: T0 + 40 * HOUR, force: true });
  check(Object.keys(r.doc.gaps).length === 50 && r.doc.overflow === 5 && r.doc.mismatched === 55 && r.doc.repairQueued === 5, "55 short conversations: 50 are tracked, 5 are counted as overflow, and no more than 5 re-reads are asked in one pass");
  res = judged(T0 + 40 * HOUR + MIN, r.doc);
  check(res.level === "warn" && res.gap.n === 55 && /^55 conversations show fewer messages/.test(row(res).text), "the notice says 55, not 50: " + row(res).text.slice(0, 80));
  for (const k of [...store.keys()]) if (/^EtsyMail_Threads\/o\d+$/.test(k)) store.delete(k);
  t = await CON.recheck(db, { now: T0 + 41 * HOUR, manual: true });
  check(Object.keys(cfg().gaps).length === 0 && cfg().overflow === 0 && judged(T0 + 41 * HOUR + MIN).level === "ok", "when the conversations are gone the ledger drains, the overflow is forgotten, and nothing throws");

  // ── 7 · what was and was not touched, and no Etsy ──
  check(fetchCalls.length === 0, "no network call at all in any of this (no Etsy, no API)");
  check(!writes.some(w => w.kind === "delete" && /^EtsyMail_Threads\//.test(w.path)), "no conversation or message was ever deleted");
  check(!writes.some(w => /^EtsyMail_(Drafts|OrderLinks|SendQueue)/.test(w.path)), "no reply box, question or send queue was touched");
  console.warn = realWarn;
  console.log(fails.length ? `\nFAILED: ${fails.length}\n- ` + fails.join("\n- ") : "\nlink-gaps OK");
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.warn = realWarn; console.error(e); console.log(warn.slice(-5).join("\n")); process.exit(1); });
