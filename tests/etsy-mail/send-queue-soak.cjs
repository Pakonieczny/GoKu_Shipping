// MAILQUEUE (10 Oct 2026): a randomised soak. Many senders, double clicks, several browsers whose helpers die, fail,
// hang and come back, the 3-minute upkeep, pages asking for the queue: every seed must keep the promises below at EVERY
// commit and at the end. The generator is seeded, so a failure replays with the same number.
//   - never more than one message active (claimed or sending), never more than one slot offered to the helpers
//   - a message is clicked on Etsy at most once, ever
//   - 'confirmed' / 'sent' means it was clicked; 'failed' means it was NOT clicked (a failed message is safe to resend)
//   - nothing is lost: when the dust settles every message is confirmed, sent, failed or needs-attention; none is left queued
//
//   node tests/etsy-mail/send-queue-soak.cjs [seeds]
"use strict";
const { boot, reporter } = require("./_queueHarness.cjs");
const R = reporter(); const check = R.check;
const SEEDS = Number(process.argv[2]) || 14;
const ONLY = process.env.SOAK_ONLY ? Number(process.env.SOAK_ONLY) : 0;

(async () => {
  console.log = () => {};
  for (let seed = ONLY || 1; seed <= (ONLY || SEEDS); seed++) {
    const h = boot({ seed: 1000 + seed, jitterMs: 3 });
    const rnd = h.fake.rnd;
    const pick = a => a[Math.floor(rnd() * a.length)];
    const threads = [1, 2, 3, 4, 5, 6, 7, 8].map(n => h.thread(700 + n, rnd() < 0.3 ? { status: "queued_for_auto_send" } : {}));
    const exts = [new h.Ext("A"), new h.Ext("B"), new h.Ext("C")];
    const revive = new Map();
    const made = [];                    // { key, text, thread }
    let violation = null, maxActive = 0, maxSlots = 0, n = 0;
    h.fake.hooks.beforeCommit = () => {
      const active = h.fake.list("EtsyMail_SendQueue").filter(x => x.data.state === "claimed" || x.data.state === "sending").length;
      const slots = h.fake.list("EtsyMail_Drafts").filter(x => x.data.status === "queued" || x.data.status === "sending").length;
      maxActive = Math.max(maxActive, active); maxSlots = Math.max(maxSlots, slots);
      if (active > 1 && !violation) violation = "two messages active at once";
      if (slots > 1 && !violation) violation = "two slots offered at once";
    };
    const scriptFor = () => {
      const x = rnd();
      if (x < 0.55) return {};
      if (x < 0.66) return { dieAt: "typing" };
      if (x < 0.72) return { dieAt: "after_mark" };
      if (x < 0.80) return { dieAt: "after_click" };
      if (x < 0.90) return { failBeforeClick: { code: pick(["DOM_TEXTAREA_MISS", "EXECUTE_THREW", "IMAGE_INJECT_FAILED"]), retry: true } };
      if (x < 0.94) return { failBeforeClick: { code: "DOM_SEND_BUTTON_MISS", retry: false } };
      if (x < 0.97) return { unverified: true };
      return { sendMs: 70000 };                            // a very slow send, past the 60 s lease without a heartbeat
    };
    const step = async () => {
      h.clock.advance(2000 + Math.floor(rnd() * 4000));
      // people and robots press Send
      if (rnd() < 0.35 && n < 28) {
        const tid = pick(threads), auto = rnd() < 0.3, dup = made.some(m => m.key) && rnd() < 0.15;
        if (dup) { const m = pick(made.filter(x => x.key)); await h.send(m.thread, m.text, { key: m.key, origin: m.auto ? "auto" : "manual" }); }
        else {
          n++; const text = "Message " + n + " about your order.", key = rnd() < 0.8 ? "k" + n : undefined;
          const m = { key, text, thread: tid, auto, answers: [] }; made.push(m);
          const calls = [h.send(tid, text, { key, origin: auto ? "auto" : "manual", aiMeta: auto ? { generatedByAI: true, model: "m" } : undefined })];
          if (rnd() < 0.2) calls.push(h.send(tid, text, { key, origin: auto ? "auto" : "manual", aiMeta: auto ? { generatedByAI: true, model: "m" } : undefined }));   // a double click
          for (const a of await Promise.all(calls)) m.answers.push(a.status + (a.body.errorCode ? ":" + a.body.errorCode : ""));
        }
      }
      for (const e of exts) {
        if (e.dead && revive.has(e.name) && h.clock.now() >= revive.get(e.name)) { e.dead = false; revive.delete(e.name); }
        if (e.dead && !revive.has(e.name)) revive.set(e.name, h.clock.now() + 60000 + Math.floor(rnd() * 240000));
      }
      await Promise.all(exts.map(e => rnd() < 0.5 ? e.poll(2, scriptFor) : null));
      if (rnd() < 0.15) await h.get("queue_state", { n: String(h.rev()), open: "1" });
      if (rnd() < 0.05) await h.Cron.handler();
    };
    for (let i = 0; i < 260; i++) await step();
    // drain: no new messages, helpers behave, time passes
    n = 999;
    for (const e of exts) { e.dead = false; }
    for (let i = 0; i < 400 && h.items().some(x => x.state === "queued" || x.state === "claimed" || x.state === "sending"); i++) {
      h.clock.advance(9000);
      await Promise.all(exts.map(e => e.poll(2)));
      await h.Cron.handler();
    }
    h.fake.hooks.beforeCommit = null;
    const items = h.items();
    const clicksBy = {};
    for (const s of h.sentToEtsy || []) clicksBy[s.text] = (clicksBy[s.text] || 0) + 1;
    const keysDistinct = new Set(made.map(m => m.key || ("d:" + m.thread + ":" + m.text))).size;
    const label = "seed " + seed + " (" + made.length + " messages, " + items.length + " queue entries, " + (h.sentToEtsy || []).length + " clicks)";
    const twice = Object.entries(clicksBy).filter(([, c]) => c > 1);
    const lied = items.filter(i => (i.state === "confirmed" || i.state === "sent") && !clicksBy[i.text]);
    const falseFail = items.filter(i => i.state === "failed" && clicksBy[i.text]);
    const lost = items.filter(i => ["queued", "claimed", "sending"].includes(i.state));
    // a message the server refused (an automated reply while a person's reply waits) was told so; it is not lost
    const missing = made.filter(m => !items.some(i => i.text === m.text) && m.answers.some(a => a.startsWith("200")));
    const refused = made.filter(m => !m.answers.some(a => a.startsWith("200")));
    const ok = !violation && !twice.length && !lied.length && !falseFail.length && !lost.length && !missing.length && maxActive <= 1 && maxSlots <= 1;
    check(ok, label + ": at most one active (" + maxActive + "), one slot (" + maxSlots + "), none clicked twice, none lost, none mislabelled" +
      (ok ? "" : " | " + [violation, twice.length && "clicked twice: " + twice.map(x => x[0]).join(","), lied.length && "said sent but never clicked: " + lied.map(i => i.text).join(","), falseFail.length && "said failed but was clicked: " + falseFail.map(i => i.text).join(","), lost.length && "left unfinished: " + lost.map(i => i.text + "=" + i.state).join(","), missing.length && "missing: " + missing.map(m => m.text).join(",")].filter(Boolean).join("; ")));
    if (seed === 1) {
      const by = {}; for (const i of items) by[i.state] = (by[i.state] || 0) + 1;
      process.stdout.write("       seed 1 outcomes: " + JSON.stringify(by) + "; " + keysDistinct + " distinct messages; warnings: " + h.warn.length + "\n");
    }
    if (!ok) {
      for (const bad of [...twice.map(x => x[0]), ...falseFail.map(i => i.text)]) {
        const it = items.find(i => i.text === bad);
        process.stdout.write("       --- " + bad + ": state " + (it && it.state) + ", sendId " + (it && it.sendId) + ", attempts " + (it && it.attempts) + "\n");
        for (const e of (it && it.events) || []) process.stdout.write("         " + new Date(e.t).toISOString().slice(11, 19) + " " + e.s + " " + e.n + "\n");
        for (const c of (h.sentToEtsy || []).filter(x => x.text === bad)) process.stdout.write("         CLICK " + new Date(c.at).toISOString().slice(11, 19) + " " + c.session + "\n");
      }
    }
    if (process.env.SOAK_WARN) process.stdout.write("       warnings seed " + seed + ": " + [...new Set(h.warn.map(w => w.slice(0, 160)))].join(" || ") + "\n");
    h.done();
  }
  R.finish("send-queue-soak");
})().catch(e => { process.stdout.write("CRASH " + (e.stack || e) + "\n"); process.exit(1); });
