/*  charm-nest-keep.js — the engine that saves a custom design to the charm repository, gives it a SKU and links it on Etsy.
 *  ═══════════════════════════════════════════════════════════════════════
 *  Paul, 11 Oct 2026: the Designs window should "save the uploaded vector file to the master charm repository", "generate a unique SKU
 *  to go along with" it, and save that "to Etsy for that particular listing AND into our charm repository so that next time that
 *  particular listing is purchased, it's already linked". The window (CustomSheet) shows it; THIS file does it and is the only way the
 *  window talks to the save. No DOM, no clock of its own, no network of its own: everything is handed in, so a test runs it on fakes.
 *  Contract: plans/save-to-repo-1011/CONTRACT.md sections 6 to 8.
 *
 *    const k = CharmNestKeep.make(ctx);
 *    ctx = { rid, listingId, title, line:{ sku, productId, variations, listingId }, listingKind, F, charm, parsed, by, sandbox,
 *            S (the page's state: S.cloud.ok), O (charm-nest-orders.js), Sku (charm-nest-sku.js), Master (the bridge's Master: entryFor,
 *            looseFor, sameDesign, saveCustom), aliases (the saved aliases, or a function that returns them), tableFor(listingId) (the
 *            listing's cached SKU table, or null), api (the page's api()), lock (CharmNestRepoLock), now, designs (how many designs the
 *            window holds, default 1), noDesign, setTimeout / clearTimeout (tests) }
 *    await k.open()    -> { offered, why, sku, group:"all"|"some"|"unknown", aliasWorks, saved:null|{sku,phase,etsy}, locked, rehearsal, reuse:null|"SKU" }
 *    k.on(fn)          fn({ row:"sku"|"repo"|"etsy", state:"idle|work|done|failed|locked|skipped|rehearsed", text, reason?, retry? })
 *                      and fn({ done:"linked|saved|held|rehearsed", sku, text, detail?, note? });  returns a function that stops listening
 *    await k.run()     never throws; sku -> repository -> Etsy; resolves { ok, done }; continues from the first step not yet done
 *    await k.retry(row)   the same, after a failed row (a step already done is never done twice: nothing is uploaded or written again)
 *    await k.unlock(password) -> { ok, reason?:"wrong|locked|unavailable", retryAfterSec? }   (the inline password row; the password is used once and never kept)
 *    k.settled(ms)     -> Promise: true when the run has ended, false when `ms` passed first (the window waits for it, up to 20 s, then closes as before)
 *    k.snapshot()      -> { rows, done, sku, info }  what a repaint of the strip needs
 *
 *  It writes nothing until run() or retry() is called (a person's press): not on open, not on a reload, not on its own. In the sandbox
 *  (ctx.sandbox) open() and the SKU check are real reads, the file is built to prove it can be, and nothing is uploaded, saved or sent to Etsy:
 *  the rows end "rehearsed". The Etsy step only ever follows a saved repository step and sends the order's number alone: the server reads
 *  everything else from the record the repository step wrote under the upload password.                                                     */
(function (root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api; else root.CharmNestKeep = api;
})(typeof self !== "undefined" ? self : globalThis, function () {
  "use strict";

  /* ── the words (plain, no emoji; the window shows them as they are) ── */
  const T = {
    one: "Saving works for one design at a time.",
    own: "This listing takes the buyer's own design every time, so it is not linked to one charm.",
    linked: sku => "This listing is already linked to " + sku + ".",
    offline: "Saving needs a connection.",
    noListing: "This order has no Etsy listing to link.",
    notRead: "Saving works once the design has been read.",
    noSku: "No free SKU for this design",
    choosing: "Choosing a SKU…",
    chosen: sku => "Chosen: " + sku,
    saving: sku => "Saving " + sku + ".ai…",
    saved: "Saved to the repository",
    already: sku => "Already in the repository as " + sku,
    needPw: "Not saved: needs the repository password",
    notSaved: why => "Not saved: " + why,
    waits: "Waits for the repository",
    etsyWork: "Setting the SKU on Etsy…",
    etsyDone: "Linked on Etsy",
    etsyNo: why => "Etsy not updated: " + why,
    rehearsedSave: sku => "Rehearsed: would save " + sku + ".ai",
    rehearsedHeld: sku => "Rehearsed: already in the repository as " + sku,
    rehearsedEtsy: tail => "Rehearsed: would set the SKU on listing …" + tail,
    rehearsedAll: "Rehearsal complete. Nothing was saved.",
    savedOnly: "Saved to the repository · Etsy not updated",
    offlineWhy: "you are offline",
    aliasNote: "Until Etsy has the new SKU, the next order of this listing will still ask."
  };
  /* what the cloud's reasons say in plain words when it sent none (the Etsy step sends its own `text`) */
  const ETSY_WHY = { scope: "Etsy refused the change. Paul needs to re-authorise Etsy with listings_w", busy: "Etsy is busy, try again in a minute", "etsy-down": "Etsy did not answer", offline: "could not reach Etsy",
    "listing-state": "the listing is not active on Etsy", "listing-gone": "Etsy no longer has this listing", changed: "the SKU on Etsy changed since the order was read", "product-missing": "the product bought is no longer on the listing",
    "sku-in-use": "that SKU is already on another product of the listing", verify: "Etsy did not keep the new SKU", "backup-failed": "the safety copy could not be saved" };

  const digits = s => String(s == null ? "" : s).replace(/\D/g, "");
  const idOf = x => (x == null ? "" : String(x));
  const toMap = v => (typeof v === "function" ? v() : v) || {};
  const SAVED_SKU = /^[A-Z0-9][A-Z0-9_-]{1,31}$/;
  const isNet = e => !!e && (e.transient === true || (typeof navigator !== "undefined" && navigator.onLine === false) || /cloud offline|timed out|no answer|failed to fetch|network/i.test(String(e.message || "")));

  function make(ctx) {
    ctx = ctx || {};
    const O = ctx.O, Sku = ctx.Sku, Master = ctx.Master || {}, F = ctx.F || null, line = ctx.line || {};
    const listingId = digits(ctx.listingId != null && ctx.listingId !== "" ? ctx.listingId : line.listingId);
    const sandbox = !!ctx.sandbox;
    const decode = s => { const t = s == null ? "" : String(s); return O && O.decodeEntities ? O.decodeEntities(t) : t; };
    const upSku = s => decode(s).trim().toUpperCase();
    const listeners = new Set();
    const st = {
      info: null, plan: null, sku: "", reuse: "", exclude: new Set(), skuDone: false, repoDone: false, etsyDone: false, committed: null, saved: null,
      built: null, uploaded: {}, running: null, done: null,
      rows: { sku: { row: "sku", state: "idle", text: "" }, repo: { row: "repo", state: "idle", text: "" }, etsy: { row: "etsy", state: "idle", text: T.waits } }
    };

    /* ── events ── */
    function emit(ev) {
      for (const fn of [...listeners]) { try { fn(Object.assign({}, ev)); } catch (_) { /* a listener never breaks the save */ } }
    }
    function row(name, state, text, extra) {
      const ev = Object.assign({ row: name, state, text: text == null ? "" : String(text) }, extra || {});
      st.rows[name] = ev; emit(ev); return ev;
    }
    function finish(kind, sku, text, extra) {
      const ev = Object.assign({ done: kind, sku: sku || st.sku || "", text }, extra || {});
      st.done = ev; emit(ev); return ev;
    }
    const lockHeld = () => !!(ctx.lock && typeof ctx.lock.find === "function" && ctx.lock.find("upload"));
    const tail4 = () => listingId.slice(-4);
    const call = (body, opts) => ctx.api("charmNestLibrary", sandbox ? Object.assign({ sandbox: true }, body) : body, Object.assign({ quiet: true }, opts || {}));

    /* ── open: is the strip offered, and what does it say? Cheap checks first; one small read (the order's save record); no write ── */
    async function open() {
      if (st.running && st.info) return st.info;
      const info = { offered: false, why: "", sku: "", group: "unknown", aliasWorks: true, saved: null, locked: !lockHeld(), rehearsal: sandbox, reuse: null };
      const no = why => { info.offered = false; info.why = why; st.info = info; return info; };
      if (!F || (Number(ctx.designs) || 1) > 1 || (Number(F.pieces) || 1) > 1) return no(T.one);
      if (F.state && F.state !== "ready") return no(T.notRead);
      if (!ctx.charm || !ctx.parsed || !(F.bytes && F.bytes.length)) return no(T.notRead);
      if (!listingId) return no(T.noListing);
      // a listing that takes the buyer's own design every time (an add-on) is never linked to one charm
      if (O && typeof O.specialOf === "function") {
        const lk = ctx.listingKind && typeof ctx.listingKind === "object" ? ctx.listingKind : ctx.listingKind ? { kind: String(ctx.listingKind) } : undefined;
        let sp = null; try { sp = O.specialOf({ listingId, sku: line.sku, title: ctx.title || "", variations: line.variations || [] }, { listingKind: lk }); } catch (_) { sp = null; }
        if (sp && sp.own) return no(T.own);
      }
      if (!(ctx.S && ctx.S.cloud && ctx.S.cloud.ok === true)) return no(T.offline);
      // a record of this order's save (a reload): the strip shows where it stands, whatever the line says now
      let rec = null;
      try { const r = await call({ op: "customSaveGet", rid: String(ctx.rid) }); rec = r && r.save ? r.save : null; } catch (e) { if (isNet(e)) return no(T.offline); rec = null; }
      const aliases = toMap(ctx.aliases), entryFor = s => (Master.entryFor ? Master.entryFor(s) : null), looseFor = s => (Master.looseFor ? Master.looseFor(s) : "");
      const table = typeof ctx.tableFor === "function" ? ctx.tableFor(listingId) : null;
      const plan = Sku && typeof Sku.plan === "function" ? Sku.plan({ file: F, title: ctx.title || "", listingId, line: Object.assign({}, line, { listingId }), table, hasMaster: s => !!entryFor(s), hasLoose: s => looseFor(s), aliases }) : null;
      st.plan = plan;
      if (plan) { info.group = (plan.group && plan.group.mode) || "unknown"; info.aliasWorks = plan.aliasWorks !== false; }
      if (rec && rec.sku) {
        info.saved = { sku: rec.sku, phase: rec.phase || "repo", etsy: rec.etsy || { state: "pending" } };
        info.sku = rec.sku; info.offered = true; st.saved = rec; st.sku = rec.sku; st.skuDone = true;
        st.repoDone = (rec.phase || "repo") === "repo"; st.etsyDone = !!(rec.etsy && rec.etsy.state === "linked");
        paintSaved(rec);
        st.info = info; return info;
      }
      // a SKU that already names a master design is not changed: the listing is linked already (a raw SKU that keeps the line a custom order does not count)
      const raw = upSku(line.sku);
      if (raw && O && typeof O.resolveSku === "function") {
        let custom = false; try { custom = !!O.specialOf({ listingId: "", sku: raw, title: "", variations: [] }, {}); } catch (_) { custom = false; }
        if (!custom) {
          let rs = null; try { rs = O.resolveSku(Object.assign({}, line, { listingId, sku: raw }), aliases, entryFor, ctx.noDesign, looseFor); } catch (_) { rs = null; }
          if (rs && rs.sku && entryFor(rs.sku)) return no(T.linked(rs.sku));
        }
      }
      // the very same drawing may be in the repository already: its SKU is used, nothing new is stored (D8)
      let same = ""; try { same = typeof Master.sameDesign === "function" && ctx.charm ? Master.sameDesign(ctx.charm.hash) : ""; } catch (_) { same = ""; }
      const reusable = same && SAVED_SKU.test(same) && !(Sku && typeof Sku.reserved === "function" && Sku.reserved(same));
      if (reusable) { st.reuse = same; info.reuse = same; info.sku = same; info.offered = true; st.sku = same; st.info = info; return info; }
      if (!plan || !plan.ok || !plan.sku) return no((plan && plan.why) || T.noSku);
      if (!st.skuDone) st.sku = plan.sku;   // (a SKU already chosen for this order stays chosen when the strip is opened again)
      info.sku = st.sku; info.offered = true; st.info = info;
      return info;
    }
    /* the rows a saved record shows (no event is a write; they only say what is there) */
    function paintSaved(rec) {
      const e = rec.etsy || {};
      row("sku", "done", rec.sku);
      row("repo", "done", rec.reused ? T.already(rec.sku) : T.saved);
      if (e.state === "linked") row("etsy", "done", T.etsyDone);
      else if (e.state === "failed" || e.state === "unverified") row("etsy", "failed", T.etsyNo(e.text || ETSY_WHY[e.reason] || "Etsy did not confirm the change"), { reason: e.reason || e.state, retry: true });
      else row("etsy", "idle", T.waits);
    }

    /* ── the work ── */
    function failure(name, e, prefix) {
      const net = isNet(e), why = net ? T.offlineWhy : String((e && e.message) || e || "something went wrong").slice(0, 140);
      return row(name, "failed", prefix(why), { reason: net ? "offline" : "error", retry: true });
    }
    async function skuStep() {
      if (st.skuDone) { row("sku", "done", st.sku); return { ok: true }; }
      if (st.reuse) { st.sku = st.reuse; st.skuDone = true; row("sku", "done", st.sku); return { ok: true }; }
      // locked: nothing can be saved yet, so nothing is asked of the cloud; the SKU shown is the page's own choice
      if (!sandbox && !lockHeld()) { row("sku", "idle", T.chosen(st.sku)); return { ok: false, held: "locked" }; }
      const candidates = ((st.plan && st.plan.candidates) || []).filter(s => !st.exclude.has(s));
      if (!candidates.length) { row("sku", "failed", T.noSku, { reason: "none", retry: false }); return { ok: false, held: "sku" }; }
      row("sku", "work", T.choosing);
      let r; try { r = await call({ op: "customSkuCheck", listingId, candidates }); } catch (e) { failure("sku", e, w => "Could not choose a SKU: " + w); return { ok: false, held: "sku" }; }
      if (!r || r.ok === false || !r.free) { row("sku", "failed", T.noSku, { reason: "none", retry: false }); return { ok: false, held: "sku" }; }
      st.sku = String(r.free).toUpperCase(); st.skuDone = true; row("sku", "done", st.sku);
      return { ok: true };
    }
    const specFor = () => {
      const g = (st.plan && st.plan.group) || {};
      return { rid: String(ctx.rid), listingId, sku: st.sku, oldSku: upSku(line.sku), productId: idOf(g.boughtId || line.productId), group: { mode: g.mode || "unknown", productIds: g.productIds || [] },
        optionPairs: (line.variations || []).map(v => [idOf(v && (v.propertyId != null ? v.propertyId : v.property_id)), idOf(v && (v.valueId != null ? v.valueId : v.value_id))]).filter(p => p[0] && p[1] && p[1] !== "0"),
        title: decode(ctx.title || ""), by: ctx.by || "", F, charm: ctx.charm, parsed: ctx.parsed, reuse: st.reuse || false, rehearsal: sandbox, built: st.built, uploaded: st.uploaded };
    };
    const hooks = {
      needPassword: async () => { row("repo", "locked", T.needPw, { reason: "locked" }); return null; },   // (no dialog: the person types the password into the strip and presses Retry)
      say: text => { if (st.rows.repo.state === "work") row("repo", "work", text); },
      built: b => { st.built = b; },
      uploaded: (which, info) => { st.uploaded[which] = info || true; }
    };
    async function repoStep() {
      if (st.repoDone && !sandbox) { row("repo", "done", st.reuse || (st.saved && st.saved.reused) ? T.already(st.sku) : T.saved); return { ok: true }; }
      if (!sandbox && !lockHeld()) { row("repo", "locked", T.needPw, { reason: "locked" }); return { ok: false, held: "locked" }; }
      row("repo", "work", T.saving(st.sku));
      let r;
      try { r = await ctx.Master.saveCustom(specFor(), hooks); } catch (e) { r = { ok: false, reason: isNet(e) ? "offline" : "error", text: isNet(e) ? T.offlineWhy : String((e && e.message) || e).slice(0, 140) }; }
      if (r && r.ok) {
        if (sandbox) { row("repo", "rehearsed", st.reuse ? T.rehearsedHeld(st.sku) : T.rehearsedSave(st.sku)); return { ok: true }; }
        st.repoDone = true; st.committed = r; st.built = null; st.sku = r.sku || st.sku;
        row("repo", "done", st.reuse || r.created === false ? T.already(st.sku) : T.saved);
        return { ok: true };
      }
      const reason = (r && r.reason) || "error";
      if (reason === "locked") { row("repo", "locked", T.needPw, { reason: "locked" }); return { ok: false, held: "locked" }; }
      if (reason === "sku-taken") {   // someone made that SKU a moment ago: the next free candidate is asked for, once
        st.exclude.add(st.sku); st.skuDone = false; st.uploaded = {}; st.built = null;
        row("repo", "failed", T.notSaved((r && r.text) || "that SKU was just taken"), { reason, retry: true });
        return { ok: false, held: "taken" };
      }
      if (reason === "already-saved") {   // another tab saved this order: its record is the truth
        try { const g = await call({ op: "customSaveGet", rid: String(ctx.rid) }); if (g && g.save && g.save.sku) { st.saved = g.save; st.sku = g.save.sku; st.skuDone = true; st.repoDone = true; paintSaved(g.save); return { ok: true }; } } catch (_) { /* below */ }
      }
      row("repo", "failed", T.notSaved((r && r.text) || "something went wrong"), { reason, retry: reason !== "reserved" });
      return { ok: false, held: reason };
    }
    async function etsyStep() {
      if (sandbox) { row("etsy", "rehearsed", T.rehearsedEtsy(tail4())); return { ok: true }; }
      if (st.etsyDone) { row("etsy", "done", T.etsyDone); return { ok: true }; }
      row("etsy", "work", T.etsyWork);
      let r; try { r = await call({ op: "customEtsySkuWrite", rid: String(ctx.rid) }, { timeoutMs: 60000 }); }
      catch (e) { failure("etsy", e, T.etsyNo); return { ok: false, held: "etsy" }; }
      if (r && r.ok === true && (r.state === "linked" || r.linked === true)) { st.etsyDone = true; row("etsy", "done", T.etsyDone); return { ok: true }; }
      const why = (r && (r.text || ETSY_WHY[r.reason])) || (r && r.error) || "Etsy did not confirm the change";
      row("etsy", "failed", T.etsyNo(why), Object.assign({ reason: (r && (r.reason || r.state)) || "error", retry: !r || r.retry !== false }, r && r.needs ? { needs: r.needs } : {}));
      return { ok: false, held: "etsy" };
    }

    /* the end of a run says one of: linked | saved (repository done, Etsy not) | held (repository not done) | rehearsed */
    function conclude(stopped) {
      if (sandbox) { finish("rehearsed", st.sku, T.rehearsedAll); return { ok: true, done: "rehearsed" }; }
      if (!st.repoDone) {
        const a = st.rows.sku, b = st.rows.repo;
        const text = stopped === "locked" ? T.needPw : a.state === "failed" ? a.text : b.state === "failed" || b.state === "locked" ? b.text : T.notSaved("something went wrong");
        finish("held", st.sku, text, stopped === "locked" ? { reason: "locked" } : {});
        return { ok: false, done: "held" };
      }
      if (st.etsyDone) { finish("linked", st.sku, "Linked · " + st.sku, { detail: "Saved to the repository and set on Etsy listing …" + tail4() }); return { ok: true, done: "linked" }; }
      finish("saved", st.sku, T.savedOnly, st.info && st.info.aliasWorks === false ? { note: T.aliasNote } : {});
      return { ok: false, done: "saved" };
    }
    async function go() {
      const info = st.info && st.info.offered ? st.info : await open();
      if (!info.offered) return { ok: false, done: null, why: info.why };
      st.done = null;
      let stop = "";
      for (let again = 0; again < 3; again++) {   // (a SKU taken a moment ago sends the loop back to the SKU step, at most twice)
        const a = await skuStep(); if (!a.ok) { stop = a.held; break; }
        const b = await repoStep(); if (b.ok) { stop = ""; break; }
        stop = b.held; if (b.held !== "taken") break;
      }
      if (stop === "" ) {
        // the repository is saved (or rehearsed): Etsy is last, and only now
        if (!sandbox && !st.repoDone) stop = "repo";
        else await etsyStep();
      } else {
        if (stop === "locked" && st.rows.repo.state !== "locked") row("repo", "locked", T.needPw, { reason: "locked" });   // (locked before the SKU step: the Repository row says why nothing is saved)
        if (st.rows.etsy.state !== "failed" && st.rows.etsy.state !== "done") row("etsy", "idle", T.waits);
      }
      return conclude(stop);
    }
    function run() {
      if (st.running) return st.running;
      const p = (async () => { try { return await go(); } catch (e) { try { row(st.repoDone ? "etsy" : "repo", "failed", (st.repoDone ? T.etsyNo : T.notSaved)(String((e && e.message) || e).slice(0, 120)), { reason: "error", retry: true }); finish(st.repoDone ? "saved" : "held", st.sku, st.repoDone ? T.savedOnly : "Not saved"); } catch (_) { /* nothing more to say */ } return { ok: false, done: st.repoDone ? "saved" : "held" }; } })();
      st.running = p; p.then(() => { if (st.running === p) st.running = null; }, () => { if (st.running === p) st.running = null; });
      return p;
    }
    function retry(which) {
      if (st.running) return st.running;
      if (which === "sku") st.skuDone = false;
      return run();
    }
    async function unlock(password) {
      const L = ctx.lock;
      if (!L || typeof L.submitInline !== "function") return { ok: false, reason: "unavailable" };
      let r = null;
      try { r = await L.submitInline("upload", String(password == null ? "" : password), { what: "Adding " + (st.sku || "a custom design") + " to the charm repo.", target: st.sku || "" }); } catch (e) { r = (e && e.data) || null; }
      password = "";
      const ok = !!r && (r.ok === true || (typeof r.token === "string" && r.token.length > 0));
      if (ok) { if (st.info) st.info.locked = false; return { ok: true }; }
      const reason = r && (r.reason || r.error), known = ["wrong", "locked", "unavailable"];
      return Object.assign({ ok: false, reason: known.includes(reason) ? reason : "unavailable" }, r && +r.retryAfterSec > 0 ? { retryAfterSec: +r.retryAfterSec } : {});
    }
    function settled(ms) {
      const p = st.running; if (!p) return Promise.resolve(true);
      const setT = ctx.setTimeout || setTimeout, clearT = ctx.clearTimeout || clearTimeout;
      return new Promise(res => {
        let over = false, timer = null;
        const end = v => { if (over) return; over = true; try { clearT(timer); } catch (_) { /* gone */ } res(v); };
        timer = setT(() => end(false), Math.max(0, +ms || 0));
        p.then(() => end(true), () => end(true));
      });
    }
    const snapshot = () => ({ locked: !lockHeld(), rows: { sku: Object.assign({}, st.rows.sku), repo: Object.assign({}, st.rows.repo), etsy: Object.assign({}, st.rows.etsy) }, done: st.done ? Object.assign({}, st.done) : null, sku: st.sku, info: st.info ? Object.assign({}, st.info) : null });
    return { open, on(fn) { if (typeof fn === "function") listeners.add(fn); return () => listeners.delete(fn); }, run, retry, unlock, settled, snapshot };
  }

  return { make, TEXT: T };
});
