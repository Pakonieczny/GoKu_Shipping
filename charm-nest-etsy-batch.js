/*  charm-nest-etsy-batch.js
 *  ═══════════════════════════════════════════════════════════════════════
 *  "Put all the queued SKUs on Etsy" in ONE press (ETSYBATCH, 13 Oct 2026).
 *  Paul, 02:51 UTC: "I need you to do all of that automatically to all etsy listings I have already asked you to update."
 *
 *  One extra line in the Etsy chip's panel (charm-nest-etsy-link.js, Library > Master files). It reads the committed queue
 *  (charm-nest-etsy-queue.json: each listing Paul asked for, with the exact repo SKU), shows ONE plain confirm line listing every
 *  listing and its SKU, asks for the repo password ONCE (the lock's own dialog; the password is never kept here, only the lock's
 *  token is used, and it is never in a URL), and then runs the listings one after another. Each listing is ONE request to the
 *  single press's own op, listingEtsySkuWrite (netlify/functions/_charmNestEtsyLink.js): the server reads the listing first,
 *  keeps a copy of it BEFORE anything is written, changes only the SKUs with one PUT, compares Etsy's answer, and answers
 *  "already" with no Etsy request when the listing is done. This file decides nothing about Etsy; it asks, waits, and says each
 *  answer in the same words the single press uses (CharmNestEtsyLink.wording).
 *
 *  Why the page drives the loop: the server has a time limit and one listing has 816 products; one request per listing keeps each
 *  inside it. A closed tab loses nothing: every listing is complete or untouched, and pressing again resumes (the server skips what
 *  is done, with no Etsy request).
 *
 *  Failure handling (a listing that stops never blocks the others, and nothing is retried):
 *    · a listing the server stops (different answer, Etsy refused it, ...) is reported in its row and the run goes on with the next;
 *    · "needs listings_w", "Etsy is busy" and "could not reach Etsy" stop the WHOLE run (every later listing would fail the same way);
 *    · the password dialog cancelled, the unlock ended, or two answers in a row that never arrived stop the run, nothing else is tried;
 *    · a listing that comes "after" another (the chameleon after the swan, as Paul asked) is not tried unless that one is done.
 *
 *    CharmNestEtsyBatch.attach(panel, { repoApi, toast, who, hold })   adds the line to one Etsy panel (called by CharmNestEtsyLink.mount)
 *    CharmNestEtsyBatch.normalize(json)                                the queue's listings: [{ id, key, after }]
 *    CharmNestEtsyBatch.summary(rows)                                  { text, kind } for a finished run
 *  Everything is drawn with textContent.
 *  ═══════════════════════════════════════════════════════════════════════ */
(function (root) {
  "use strict";
  const doc = root.document;
  const QUEUE = "charm-nest-etsy-queue.json", LISTING = /^\d{3,20}$/;
  const STOP_ALL = new Set(["scope", "busy", "offline"]);   // the server's reasons that would repeat for every later listing
  const plural = (n, w) => n + " " + w + (n === 1 ? "" : "s");

  /** The queue file → [{ id, key, after }] in file order (a listing twice, a bad number or an empty SKU is left out). */
  function normalize(json) {
    const arr = Array.isArray(json) ? json : json && Array.isArray(json.listings) ? json.listings : [];
    const seen = new Set(), items = [];
    for (const x of arr) {
      if (!x || typeof x !== "object") continue;
      const id = String(x.listingId == null ? "" : x.listingId).trim(), key = typeof x.key === "string" ? x.key : "";
      if (!LISTING.test(id) || !key || seen.has(id)) continue;
      seen.add(id);
      const after = String(x.after == null ? "" : x.after).trim();
      items.push({ id, key, after: LISTING.test(after) ? after : "" });
    }
    return items;
  }

  /** The words for a finished run, from its rows (state: done | already | stop | skip). */
  function summary(rows) {
    const c = { done: 0, already: 0, stop: 0, skip: 0 };
    for (const r of rows) if (r.state in c) c[r.state]++;
    const parts = [];
    if (c.done) parts.push(c.done + " done");
    if (c.already) parts.push(c.already + " already there");
    if (c.stop) parts.push(c.stop + " stopped");
    if (c.skip) parts.push(c.skip + " not tried");
    return { text: plural(rows.length, "listing") + ": " + (parts.join(", ") || "nothing done") + ".", kind: c.stop || c.skip ? "bad" : "ok" };
  }

  /* ── the queue: read once from the committed file; a failed read is not remembered (the next panel tries again) ── */
  let queue = null, loading = null;
  async function readQueue() {
    try {
      if (typeof root.fetch !== "function") return [];
      const res = await root.fetch(QUEUE, { cache: "no-store" });
      if (!res.ok) return [];
      queue = normalize(await res.json());
      return queue;
    } catch (_) { return []; }
  }
  function loadQueue() {
    if (queue) return Promise.resolve(queue);
    if (!loading) { const p = loading = readQueue(); p.then(() => { if (loading === p) loading = null; }); }
    return loading;
  }

  /* ── the run: ONE per page, shared by every open panel (a panel opened during a run shows it) ── */
  const R = { running: false, rows: [], say: "", kind: "", views: new Set() };
  const changed = () => { for (const v of [...R.views]) { if (v.box.isConnected) { try { v.draw(); } catch (_) { /* a view never breaks the run */ } } else R.views.delete(v); } };

  async function run(o, items) {
    if (R.running || !items.length) return;
    const L = root.CharmNestRepoLock, E = root.CharmNestEtsyLink;
    R.running = true; R.say = ""; R.kind = "";
    R.rows = items.map(i => ({ id: i.id, key: i.key, after: i.after, state: "wait", text: "", kind: "" }));
    const rows = R.rows;
    let note = "";   // why the run stopped early, when it did
    const stopFrom = (from, text) => { for (let j = from; j < rows.length; j++) if (rows[j].state === "wait") { rows[j].state = "skip"; rows[j].text = "Not tried."; } note = text; };
    const finish = (override) => {
      const s = summary(rows); R.running = false;
      if (override) { R.say = override.text; R.kind = override.kind || "bad"; } else { R.say = note ? note + " " + s.text : s.text; R.kind = s.kind; }
      changed();
      try { if (!override && typeof o.toast === "function") o.toast(s.text, s.kind === "ok" ? "ok" : "bad", 7000); } catch (_) { /* a toast is only a hint */ }
    };
    changed();
    if (!L || typeof L.ensure !== "function" || typeof L.unlocked !== "function" || typeof o.repoApi !== "function" || !E || typeof E.wording !== "function") {
      stopFrom(0, ""); return finish({ text: "The charm repo lock did not load: reload the page.", kind: "bad" });
    }
    /* the password is asked ONCE, here, before anything is sent; the lock keeps its token (never the password) for the whole run */
    R.say = "Asking for the repo password once; then each listing in turn."; changed();
    let info = null;
    try { info = await L.ensure("modify", { what: `Putting the SKUs of ${plural(rows.length, "queued Etsy listing")} on Etsy, one after another.`, target: plural(rows.length, "Etsy listing"), go: "Put SKUs on Etsy" }); } catch (_) { info = null; }
    if (!info) { stopFrom(0, ""); return finish({ text: "Nothing was sent: the repo stayed locked.", kind: "bad" }); }
    R.say = "Putting the SKUs on Etsy, one listing at a time. Closing this page loses nothing: press again to carry on."; changed();
    const by = (() => { try { return typeof o.who === "function" ? String(o.who() || "").trim() : ""; } catch (_) { return ""; } })();
    let lost = 0;   // answers in a row that never arrived
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      if (row.after) {   // "after the swan": only when that one is done
        const dep = rows.find(x => x.id === row.after);
        if (dep && dep.state !== "done" && dep.state !== "already") { row.state = "skip"; row.text = `Not tried: listing ${dep.id} has to be done first.`; changed(); continue; }
      }
      if (i > 0 && !L.unlocked("modify")) { stopFrom(i, "The repo unlock ended, so nothing more was sent. Press again to unlock and carry on: what is done is skipped."); break; }
      row.state = "work"; changed();
      let r = null, err = null;
      try { r = await o.repoApi("modify", { what: `Putting the SKU ${row.key} on Etsy listing ${row.id}.`, target: `${row.key} → Etsy ${row.id}`, go: "Put SKU on Etsy" }, { op: "listingEtsySkuWrite", listingId: row.id, sku: row.key, by }); }
      catch (e) { err = e || new Error("no answer"); }
      if (err) {
        if (err.cancelled) { row.state = "skip"; row.text = "Not sent."; stopFrom(i + 1, "Nothing more was sent: the repo stayed locked."); break; }
        row.state = "stop"; row.text = "Etsy may not have been changed: " + String(err.message || err) + ". Look at the listing before pressing again.";
        lost++;
        if (lost >= 2) { stopFrom(i + 1, "The run stopped after two answers in a row did not arrive."); break; }
        changed(); continue;
      }
      lost = 0;
      /* an ok answer that made no Etsy request (calls 0) is the record of an earlier press: the listing is already done */
      const again = !!(r && r.ok && !r.already && r.calls === 0);
      const w = E.wording(again ? Object.assign({}, r, { already: true }) : r, row.id, row.key);
      row.text = w.text; row.kind = w.kind;
      row.state = w.kind === "ok" ? (again || (r && r.already) ? "already" : "done") : "stop";
      if (r && !r.ok && (STOP_ALL.has(r.reason) || r.needs)) { stopFrom(i + 1, "The run stopped here: the listings after it would fail the same way, so they were not tried."); break; }
      changed();
    }
    finish();
  }

  /* ── the line in one panel ── */
  const CSS = `
.elPanel .elBatch{display:grid;gap:5px;min-width:0;margin-top:4px;padding-top:7px;border-top:1px solid var(--line)}
.elPanel .elBatch[hidden]{display:none}
.elBatch .elBtSay{font-size:11px;line-height:1.45;color:var(--ink70);overflow-wrap:anywhere}
.elBatch .elBtSay:empty{display:none}
.elBatch .elBtSay.ok{color:var(--sage)}
.elBatch .elBtSay.bad{color:#8a3a26}
.elBatch .elBtList{display:grid;gap:4px;position:relative;max-height:min(55vh,340px);overflow-y:auto;overscroll-behavior:contain;padding-right:2px}
.elBatch .elBtList:empty{display:none}
.elBatch .elBtRow{display:grid;gap:1px;font-size:11px;line-height:1.4;overflow-wrap:anywhere}
.elBatch .elBtRow b{font:700 11px var(--mono);color:var(--ink)}
.elBatch .elBtRow .st{display:flex;align-items:flex-start;gap:6px;color:var(--ink45)}
.elBatch .elBtRow.work .st{color:var(--ink70)}
.elBatch .elBtRow.done .st,.elBatch .elBtRow.already .st{color:var(--sage)}
.elBatch .elBtRow.stop .st,.elBatch .elBtRow.skip .st{color:#8a3a26}
.elBatch .elBtRow .spin{width:10px;height:10px;margin-top:2px;border:2px solid rgba(0,0,0,.15);border-top-color:currentColor;border-radius:50%;animation:elBtSpin .7s linear infinite;flex:0 0 auto}
@keyframes elBtSpin{to{transform:rotate(360deg)}}
.elBatch .elBtActs{display:flex;gap:4px;align-items:center;flex-wrap:wrap}
.elBatch .elBtActs .btn[hidden]{display:none}
.elBatch .elBtActs .btn{white-space:normal;text-align:left;line-height:1.3;max-width:100%}
.elBatch .elBtRow>div,.elBatch .elBtRow .st>span{min-width:0}
.elBatch .elBtGo,.elBatch .elBtNo{background:transparent;color:var(--ink);border-color:var(--line)}
.elBatch .elBtGo:hover,.elBatch .elBtNo:hover{background:var(--card2)}
.elBatch .elBtYes{background:var(--gold);border-color:var(--gold);color:#2a2013}
.elPanel.elBtRun>:not(.elBatch){opacity:.5}
`;
  function css() { try { if (!doc || doc.getElementById("cnEtsyBatchCss")) return; const s = doc.createElement("style"); s.id = "cnEtsyBatchCss"; s.textContent = CSS; doc.head.appendChild(s); } catch (_) { /* unstyled, still works */ } }
  const el = (tag, cls, text) => { const e = doc.createElement(tag); if (cls) e.className = cls; if (text != null) e.textContent = text; return e; };

  /** Adds the batch line to one Etsy panel. o = { repoApi, toast, who, hold(on) }: hold keeps the panel open while a run is in progress. */
  function attach(panel, o) {
    if (!panel || !doc) return;
    css(); o = o || {};
    const box = el("div", "elBatch"); box.hidden = true;
    const say = el("div", "elBtSay"); say.setAttribute("aria-live", "polite");
    const list = el("div", "elBtList"), acts = el("div", "elBtActs");
    const go = el("button", "btn xs elBtGo"), yes = el("button", "btn xs elBtYes"), no = el("button", "btn xs elBtNo", "Cancel");   // (own classes: the single press's own selectors, .btn.gold and .btn.ghost, never meet these)
    for (const b of [go, yes, no]) b.type = "button";
    go.setAttribute("data-repo", "modify"); yes.setAttribute("data-repo", "modify");
    acts.append(go, yes, no); box.append(say, list, acts); panel.appendChild(box);
    let asking = false, held = false;
    const row = (head, key, st, cls, spin) => {
      const r = el("div", "elBtRow" + (cls ? " " + cls : "")), s = el("div", "st");
      r.append(el("div", "", ""), s); r.firstChild.append(el("b", "", head), "  " + key);
      if (spin) s.append(el("span", "spin"));
      if (st) s.append(el("span", "", st));
      if (!st && !spin) s.hidden = true;
      return r;
    };
    function draw() {
      const items = queue || [], n = items.length;
      box.hidden = !n;
      const running = R.running, kept = list.scrollTop;
      list.textContent = "";
      if (asking && !running) {   // the one plain confirm line, then every listing and its SKU
        say.className = "elBtSay"; say.textContent = `Every product of these ${plural(n, "Etsy listing")} will carry the SKU beside it, one listing after another. Prices, stock and options stay as they are; each listing as it is now is kept first, and one already done is skipped.`;
        for (const i of items) list.appendChild(row(i.id, i.key, "", ""));
      } else if (running || R.rows.length) {
        say.className = "elBtSay" + (R.kind ? " " + R.kind : ""); say.textContent = R.say;
        for (const r of R.rows) list.appendChild(row(r.id, r.key, r.state === "wait" ? "Waiting" : r.state === "work" ? "Putting the SKU on Etsy…" : r.text, r.state, r.state === "work"));
      } else { say.className = "elBtSay"; say.textContent = ""; }
      list.scrollTop = kept;
      const w = list.querySelector(".work");   // the listing in progress stays in view inside the list
      if (w && (w.offsetTop < list.scrollTop || w.offsetTop + w.offsetHeight > list.scrollTop + list.clientHeight)) list.scrollTop = Math.max(0, w.offsetTop - 8);
      go.textContent = "Put all " + plural(n, "queued SKU") + " on Etsy"; yes.textContent = "Put all " + plural(n, "SKU") + " on Etsy";
      go.hidden = running || asking; yes.hidden = running || !asking; no.hidden = running || !asking;
      const on = running; panel.classList.toggle("elBtRun", on);
      for (const c of panel.children) if (c !== box) c.inert = on;
      if (on !== held) { held = on; if (typeof o.hold === "function") { try { o.hold(on); } catch (_) { /* optional */ } } }
      try { if (root.CharmNestRepoLock) root.CharmNestRepoLock.mark(box); } catch (_) { /* the padlock is only a hint */ }
    }
    go.onclick = () => { if (R.running || panel.querySelector(".elActs .spin")) return; asking = true; draw(); try { yes.focus({ preventScroll: true }); } catch (_) { /* a convenience */ } };
    no.onclick = () => { asking = false; draw(); };
    yes.onclick = () => { if (R.running || !queue || !queue.length) return; asking = false; run(o, queue.slice()); };
    const view = { box, draw };
    R.views.add(view);
    loadQueue().then(draw, draw);
    draw();
  }

  root.CharmNestEtsyBatch = { attach, normalize, summary, _run: R };
  if (typeof module !== "undefined" && module.exports) module.exports = root.CharmNestEtsyBatch;
})(typeof window !== "undefined" ? window : globalThis);
