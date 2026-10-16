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
 *  It writes nothing until run() or retry() is called: not on open, not on a reload, not on its own. WHO calls run() is the keeper below (KEEPSAVE, 16 Oct
 *  2026): the person's switch ON records a save INTENT and the keeper runs this engine, again and again, until the save ends. In the sandbox
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
    aliasNote: "Until Etsy has the new SKU, the next order of this listing will still ask.",
    cancelled: "Not saved: the switch was turned off"
  };
  /* what the cloud's reasons say in plain words when it sent none (the Etsy step sends its own `text`) */
  const ETSY_WHY = { scope: "Etsy refused the change. Paul needs to re-authorise Etsy with listings_w", busy: "Etsy is busy, try again in a minute", "etsy-down": "Etsy did not answer", offline: "could not reach Etsy",
    running: "Etsy is being updated by another run", interrupted: "the last change to Etsy was cut short", "not-connected": "Etsy is not connected: Paul needs to connect it", "listing-state": "the listing is not active on Etsy", "listing-gone": "Etsy no longer has this listing", changed: "the SKU on Etsy changed since the order was read", "product-missing": "the product bought is no longer on the listing",
    "sku-in-use": "that SKU is already on another product of the listing", rejected: "Etsy rejected the change", "bad-save": "the saved record could not be used", "no-save": "there is no saved record for this order", "not-saved": "the design is not saved yet", verify: "Etsy did not keep the new SKU", "backup-failed": "the safety copy could not be saved" };

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
      built: null, uploaded: {}, running: null, done: null, last: null,
      rows: { sku: { row: "sku", state: "idle", text: "" }, repo: { row: "repo", state: "idle", text: "" }, etsy: { row: "etsy", state: "idle", text: T.waits } }
    };

    /* ── events ── */
    function emit(ev) {
      for (const fn of [...listeners]) { try { fn(Object.assign({}, ev)); } catch (_) { /* a listener never breaks the save */ } }
    }
    function row(name, state, text, extra) {
      const ev = Object.assign({ row: name, state, text: text == null ? "" : String(text) }, extra || {});
      if (state === "failed" || state === "locked") st.last = { row: name, reason: (extra && extra.reason) || state, retry: !(extra && extra.retry === false), text: ev.text, needs: extra && extra.needs };
      st.rows[name] = ev; emit(ev); return ev;
    }
    function finish(kind, sku, text, extra) {
      const ev = Object.assign({ done: kind, sku: sku || st.sku || "", text }, extra || {});
      st.done = ev; emit(ev); return ev;
    }
    const lockHeld = () => !!(ctx.lock && typeof ctx.lock.find === "function" && ctx.lock.find("upload"));
    const tail4 = () => listingId.slice(-4);
    const call = (body, opts) => ctx.api("charmNestLibrary", sandbox ? Object.assign({ sandbox: true }, body) : body, Object.assign({ quiet: true }, opts || {}));

    /* the SKU plan, made from what the page knows NOW (the listing's SKU table may arrive after the first look: it is read again before the SKU is chosen) */
    function makePlan() {
      const aliases = toMap(ctx.aliases), entryFor = s => (Master.entryFor ? Master.entryFor(s) : null), looseFor = s => (Master.looseFor ? Master.looseFor(s) : "");
      const table = typeof ctx.tableFor === "function" ? ctx.tableFor(listingId) : null;
      return Sku && typeof Sku.plan === "function" ? Sku.plan({ file: F, title: ctx.title || "", listingId, line: Object.assign({}, line, { listingId }), table, hasMaster: s => !!entryFor(s), hasLoose: s => looseFor(s), aliases }) : null;
    }
    /* ── open: is the strip offered, and what does it say? Cheap checks first; one small read (the order's save record); no write ── */
    async function open() {
      if (st.running && st.info) return st.info;
      const info = { offered: false, why: "", sku: "", group: "unknown", aliasWorks: true, saved: null, locked: !lockHeld(), rehearsal: sandbox, reuse: null };
      const no = (why, later) => { info.offered = false; info.why = why; info.later = later || ""; st.info = info; return info; };   // (later: "data" | "connection" = not yet, the keeper waits; "" = never, for this design)
      if (!F || (Number(ctx.designs) || 1) > 1 || (Number(F.pieces) || 1) > 1) return no(T.one);
      if (F.state && F.state !== "ready") return no(T.notRead, "data");
      if (!ctx.charm || !ctx.parsed || !(F.bytes && F.bytes.length) || !F.hash) return no(T.notRead, "data");
      if (!listingId) return no(T.noListing);
      // a listing that takes the buyer's own design every time (an add-on) is never linked to one charm
      if (O && typeof O.specialOf === "function") {
        const lk = ctx.listingKind && typeof ctx.listingKind === "object" ? ctx.listingKind : ctx.listingKind ? { kind: String(ctx.listingKind) } : undefined;
        let sp = null; try { sp = O.specialOf({ listingId, sku: line.sku, title: ctx.title || "", variations: line.variations || [] }, { listingKind: lk }); } catch (_) { sp = null; }
        if (sp && sp.own) return no(T.own);
      }
      // a record of this order's save (a reload): the strip shows where it stands, whatever the line says now.
      // Offline the switch is still offered (KEEPSAVE: a save asked for while the connection is down waits for it): the record is read when the connection is back
      let rec = null, offline = !(ctx.S && ctx.S.cloud && ctx.S.cloud.ok === true);
      if (!offline) { try { const r = await call({ op: "customSaveGet", rid: String(ctx.rid), listingId }); rec = r && r.save ? r.save : null; } catch (e) { if (isNet(e)) offline = true; rec = null; } }
      info.offline = offline;
      const aliases = toMap(ctx.aliases), entryFor = s => (Master.entryFor ? Master.entryFor(s) : null), looseFor = s => (Master.looseFor ? Master.looseFor(s) : "");
      const plan = makePlan();
      st.plan = plan;
      if (plan) { info.group = (plan.group && plan.group.mode) || "unknown"; info.aliasWorks = plan.aliasWorks !== false; }
      if (rec && rec.sku && rec.repair) {   // the listing's answer was cut or the files vanished (the cloud's repair says so): the SAME save runs again under the same SKU; the commit re-creates / re-attaches
        st.sku = String(rec.sku).toUpperCase(); st.skuDone = true; st.repoDone = false; st.etsyDone = false; info.sku = st.sku; info.offered = true; st.info = info; return info;
      }
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
          if (rs && rs.sku && entryFor(rs.sku)) { const lk = no(T.linked(rs.sku)); lk.linked = rs.sku; return lk; }
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
      // (the cloud always finds a free SKU for any list it is given, extending it itself: when ours is used up, the ones already refused are sent again rather than ending with "No free SKU")
      let candidates = ((st.plan && st.plan.candidates) || []).filter(s => !st.exclude.has(s));
      if (!candidates.length && st.exclude.size) candidates = [...st.exclude];
      if (!candidates.length) { row("sku", "failed", T.noSku, { reason: "none", retry: false }); return { ok: false, held: "sku" }; }
      row("sku", "work", T.choosing);
      let r; try { r = await call({ op: "customSkuCheck", listingId, candidates }); } catch (e) { failure("sku", e, w => "Could not choose a SKU: " + w); return { ok: false, held: "sku" }; }
      if (!r || r.ok === false || !r.free) { row("sku", "failed", T.noSku, { reason: "none", retry: false }); return { ok: false, held: "sku" }; }
      st.sku = String(r.free).toUpperCase(); st.skuDone = true; row("sku", "done", st.sku);
      return { ok: true };
    }
    const specFor = () => {
      const g = (st.plan && st.plan.group) || {};
      return { rid: String(ctx.rid), listingId, sku: st.sku, oldSku: upSku(line.sku).replace(/[\u0000-\u001f\u007f]+/g, " ").slice(0, 120), productId: idOf(g.boughtId || line.productId), group: { mode: g.mode || "unknown", productIds: g.productIds || [] },
        optionPairs: (line.variations || []).map(v => [idOf(v && (v.propertyId != null ? v.propertyId : v.property_id)), idOf(v && (v.valueId != null ? v.valueId : v.value_id))]).filter(p => p[0] && p[1] && p[1] !== "0"),
        title: decode(ctx.title || ""), by: ctx.by || "", F, charm: ctx.charm, parsed: ctx.parsed, reuse: st.reuse || false, rehearsal: sandbox, built: st.built, uploaded: st.uploaded };
    };
    /* is the save still wanted? (the switch). One already in the repository is finished whatever the switch says: it is never left half linked. */
    const wanted = () => st.repoDone || typeof ctx.alive !== "function" || ctx.alive() !== false;
    const hooks = {
      alive: () => wanted(),   // (saveCustom looks before each upload and before the commit: a switch turned off stops it there, with nothing written)
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
      if (reason === "cancelled") { row("repo", "idle", T.waits); return { ok: false, held: "cancelled" }; }
      if (reason === "listing-linked") {   // the listing's answer is ANOTHER design's (a race lost, or a person's link): first link wins, final and calm, no Etsy step
        st.linkedTo = String((r && r.sku) || "").toUpperCase(); row("repo", "skipped", T.linked(st.linkedTo || "a charm")); row("etsy", "skipped", "");
        return { ok: false, held: "already" };
      }
      if (reason === "not-uploaded" || reason === "bad-file") st.uploaded = {};   // (the cloud does not hold what we uploaded: the next try uploads again, it never trusts a memory of it)
      if (reason === "locked") { row("repo", "locked", T.needPw, { reason: "locked" }); return { ok: false, held: "locked" }; }
      if (reason === "sku-taken") {   // someone made that SKU a moment ago: first, is THIS order saved already (another tab got there)? else the next free candidate is asked for
        try { const g = await call({ op: "customSaveGet", rid: String(ctx.rid), listingId }); if (g && g.save && g.save.sku) { st.saved = g.save; st.sku = g.save.sku; st.skuDone = true; st.repoDone = true; paintSaved(g.save); return { ok: true }; } } catch (_) { /* the next candidate */ }
        st.exclude.add(st.sku); st.skuDone = false; st.uploaded = {}; st.built = null;
        row("repo", "failed", T.notSaved((r && r.text) || "that SKU was just taken"), { reason, retry: true });
        return { ok: false, held: "taken" };
      }
      if (reason === "already-saved") {   // another tab saved this order: its record is the truth
        try { const g = await call({ op: "customSaveGet", rid: String(ctx.rid), listingId }); if (g && g.save && g.save.sku) { st.saved = g.save; st.sku = g.save.sku; st.skuDone = true; st.repoDone = true; paintSaved(g.save); return { ok: true }; } } catch (_) { /* below */ }
      }
      row("repo", "failed", T.notSaved((r && r.text) || "something went wrong"), { reason, retry: reason !== "reserved" });
      return { ok: false, held: reason };
    }
    async function etsyStep() {
      if (sandbox) { row("etsy", "rehearsed", T.rehearsedEtsy(tail4())); return { ok: true }; }
      if (st.etsyDone) { row("etsy", "done", T.etsyDone); return { ok: true }; }
      row("etsy", "work", T.etsyWork);
      let r; try { r = await call({ op: "customEtsySkuWrite", rid: String(ctx.rid), listingId }, { timeoutMs: 60000 }); }
      catch (e) { failure("etsy", e, T.etsyNo); return { ok: false, held: "etsy" }; }
      if (r && r.ok === true && (r.state === "linked" || r.linked === true)) { st.etsyDone = true; row("etsy", "done", T.etsyDone); return { ok: true }; }
      const why = (r && (r.text || ETSY_WHY[r.reason])) || (r && r.error) || "Etsy did not confirm the change";
      row("etsy", "failed", T.etsyNo(why), Object.assign({ reason: (r && (r.reason || r.state)) || "error", retry: !r || r.retry !== false }, r && r.needs ? { needs: r.needs } : {}));
      return { ok: false, held: "etsy" };
    }

    /* the end of a run says one of: linked | saved (repository done, Etsy not) | held (repository not done) | rehearsed */
    function conclude(stopped) {
      // (what the keeper needs to decide wait / try again / needs attention is in outcome(), not in the resolved value, which keeps its shape { ok, done })
      const why = st.last || {}, out = (done, o) => { st.outcome = Object.assign({ done, held: stopped || "", reason: why.reason || "", retry: why.retry !== false, text: why.text || "", needs: why.needs || "", sku: st.sku, repo: st.repoDone, etsy: st.etsyDone }, o || {}); };
      if (sandbox) { out("rehearsed"); finish("rehearsed", st.sku, T.rehearsedAll); return { ok: true, done: "rehearsed" }; }
      if (stopped === "already") { out("already", { sku: st.linkedTo || st.sku }); finish("already", st.linkedTo || st.sku, T.linked(st.linkedTo || "a charm")); return { ok: true, done: "already" }; }
      if (!st.repoDone) {
        const a = st.rows.sku, b = st.rows.repo;
        if (stopped === "cancelled") { out("held", { reason: "cancelled" }); finish("held", st.sku, T.cancelled, { reason: "cancelled" }); return { ok: false, done: "held" }; }
        const text = stopped === "locked" ? T.needPw : a.state === "failed" ? a.text : b.state === "failed" || b.state === "locked" ? b.text : T.notSaved("something went wrong");
        out("held", stopped === "locked" ? { reason: "locked" } : {});
        finish("held", st.sku, text, stopped === "locked" ? { reason: "locked" } : {});
        return { ok: false, done: "held" };
      }
      if (st.etsyDone) { out("linked"); finish("linked", st.sku, "Linked · " + st.sku, { detail: "Saved to the repository and set on Etsy listing …" + tail4() }); return { ok: true, done: "linked" }; }
      out("saved");
      finish("saved", st.sku, T.savedOnly, st.info && st.info.aliasWorks === false ? { note: T.aliasNote } : {});
      return { ok: false, done: "saved" };
    }
    async function go() {
      const info = st.info && st.info.offered && !st.info.offline ? st.info : await open();   // (a look taken offline is taken again: the order's record may exist)
      if (!info.offered) return { ok: false, done: null, why: info.why, later: info.later || "", linked: info.linked || "" };
      st.done = null; st.last = null; st.outcome = null;
      let stop = "";
      // the SKU table of the listing may have come since the first look: the plan is made again before a SKU is chosen
      if (!st.skuDone && !st.reuse && !st.saved) { try { const pl = makePlan(); if (pl && pl.ok && pl.sku) { st.plan = pl; if (!st.exclude.size) st.sku = pl.sku; } } catch (_) { /* the first plan stands */ } }
      for (let again = 0; again < 3; again++) {   // (a SKU taken a moment ago sends the loop back to the SKU step, at most twice)
        if (!wanted()) { stop = "cancelled"; break; }
        const a = await skuStep(); if (!a.ok) { stop = a.held; break; }
        if (!wanted()) { stop = "cancelled"; break; }
        const b = await repoStep(); if (b.ok) { stop = ""; break; }
        stop = b.held; if (b.held !== "taken") break;
      }
      if (stop === "already") return conclude("already");
      if (stop === "" ) {
        // the repository is saved (or rehearsed): Etsy is last, and only now
        if (!sandbox && !st.repoDone) stop = "repo";
        else await etsyStep();
      } else {
        if (stop === "cancelled") { if (st.rows.repo.state === "work") row("repo", "idle", T.waits); }
        else if (stop === "locked" && st.rows.repo.state !== "locked") row("repo", "locked", T.needPw, { reason: "locked" });   // (locked before the SKU step: the Repository row says why nothing is saved)
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
    /** What the last run ended with, for the keeper: { done, held, reason, retry, text, needs, sku, repo, etsy } (null while nothing has ended). */
    const outcome = () => (st.outcome ? Object.assign({}, st.outcome) : null);
    const snapshot = () => ({ locked: !lockHeld(), rows: { sku: Object.assign({}, st.rows.sku), repo: Object.assign({}, st.rows.repo), etsy: Object.assign({}, st.rows.etsy) }, done: st.done ? Object.assign({}, st.done) : null, sku: st.sku, info: st.info ? Object.assign({}, st.info) : null });
    return { open, outcome, on(fn) { if (typeof fn === "function") listeners.add(fn); return () => listeners.delete(fn); }, run, retry, unlock, settled, snapshot };
  }

  /* ═══════════════════════════════════════════════════════════════════════
   *  THE KEEPER (KEEPSAVE, Paul, 16 Oct 2026: "super robust and completely self contained ... resilient to pauses, interruptions ... all eventualities")
   *
   *  The person's switch ON is an INTENT, not a press that has to be followed by another. The keeper records it BEFORE any step (browser storage, keyed by
   *  order + listing + file hash), then runs the engine above, again and again, until the save ends:
   *
   *    const keeper = CharmNestKeep.durable({ store, now, setTimeout, clearTimeout, tab, online(), lock, locks, resolve(rec), make, remote, storageKey })
   *    keeper.begin({ sandbox, rid, listingId, hash, name, kind, by, ck, line, title, engine? }) -> { key, created, conflict? }   (recorded first; the first tick is a moment later)
   *    keeper.cancel(key, why)  -> { ok, committed? }    the switch OFF: stops a save that is not in the repository yet; one that is, is completed (never left half linked)
   *    keeper.resume(reason)    every wake-up (page load, online, tab visible, lock opened/closed, window closed, order or SKU data ready, a timer): continues every
   *                             intent from the first step not done; nothing runs twice (one flight per intent here, one save per listing across tabs through
   *                             deps.locks = navigator.locks, the server's create-only commit for the rest)
   *    keeper.retry(key)        a person's "Try again" on a needs-attention line (the only thing a person is ever asked to press)
   *    keeper.get(key) / list() / forOrder(rid) / describe(rec) -> { phase, line, sku, wait, attention }   phase: off | saving | password | connection | data | retry | linked | attention
   *    keeper.on(fn)  fn({ type:"state", key, rec, view }) | fn({ type:"engine", key, ev })   ·  keeper.idle() -> Promise (tests)  ·  keeper.external()  (another tab wrote the record)
   *
   *  deps.resolve(rec) -> { state:"ready", ctx } | { state:"wait", wait:"data", why? } | { state:"gone", why }   (the page hands over the design as it is NOW; a design
   *  not read yet, an order or SKU table not loaded yet is a wait, never an error). The engine's own ctx.alive() is wired here to the record (switch OFF).
   *  Waiting for the password is event driven (the lock's onChange, and a slow safety beat); waiting for the connection likewise; a transient failure backs off
   *  2 s, 4 s ... 5 min (8 tries, then ONE needs-attention line with "Try again"); a step done is never done again.
   * ═══════════════════════════════════════════════════════════════════════ */
  const INTENTS_KEY = "cn.keepIntents.v1";
  const PHASE_LINE = {
    saving: "Saving to the repository…", password: "Waiting for the repository password. It saves by itself once it is entered.",
    connection: "Waiting for the connection. It saves by itself when you are back online.", data: "Getting this listing ready. It saves by itself.",
    retry: "Could not finish just now. Trying again by itself.", off: "", rehearsed: "Rehearsal complete. Nothing was saved."
  };
  /* the reasons for which trying again can never help; everything else is tried again, with a pause */
  const STOP_REPO = new Set(["geometry", "bad-sku", "bad-file", "reserved", "reuse-missing", "reuse-blocked", "reuse-mismatch", "sandbox", "none"]);
  /* Etsy: the server marks the ones that trying again cannot mend (retry:false); these never become a loop. scope and listing-state wait for a person or the listing: one attention line, tried again once per page load */
  const STOP_ETSY = new Set(["listing-gone", "product-missing", "changed", "sku-in-use", "verify", "rejected", "bad-save", "no-save", "not-saved", "rehearsal", "sandbox"]);
  const LATER_ETSY = new Set(["scope", "listing-state", "not-connected"]);
  const MAX_FAILS = 8, BEAT = 30000, WATCHDOG = 90000, KEEP_DONE_MS = 14 * 86400000;
  const backoffMs = n => Math.min(300000, 2000 * Math.pow(2, Math.max(0, n - 1)));

  function durable(deps) {
    deps = deps || {};
    const now = typeof deps.now === "function" ? deps.now : Date.now;
    const setT = deps.setTimeout || setTimeout, clearT = deps.clearTimeout || clearTimeout;
    const KEY = deps.storageKey || INTENTS_KEY, tab = deps.tab || ("t" + Math.random().toString(36).slice(2, 9));
    const autoTried = new Set(), listeners = new Set(), flights = new Map(), redo = new Set(), engines = new Map(), wired = new WeakSet(), polls = new Map();
    let memo = {}, timer = null, stopLock = null;
    const online = () => { try { return typeof deps.online === "function" ? deps.online() !== false : true; } catch (_) { return true; } };
    const lockHeld = () => { try { return !!(deps.lock && typeof deps.lock.find === "function" && deps.lock.find("upload")); } catch (_) { return false; } };

    /* ── the record: read and written whole every time (another tab may have written another intent meanwhile) ── */
    function readAll() {
      try {
        const t = deps.store ? deps.store.get(KEY) : null; const o = t ? JSON.parse(t) : null;
        if (o && o.intents && typeof o.intents === "object") memo = o.intents; else if (deps.store && !t) memo = {};
      } catch (_) { /* the page's own memory stands in */ }
      return memo;
    }
    function writeAll(all) { memo = all; try { if (deps.store) deps.store.set(KEY, JSON.stringify({ v: 1, intents: all })); } catch (_) { /* memory only: this tab still finishes the save */ } }
    const read = key => { const r = readAll()[key]; return r ? Object.assign({}, r) : null; };
    function patch(key, fn) {
      const all = Object.assign({}, readAll()), cur = all[key]; if (!cur) return null;
      const next = Object.assign({}, cur, typeof fn === "function" ? fn(cur) : fn, { upd: now() });
      all[key] = next; writeAll(all); announce(next); return Object.assign({}, next);
    }
    const keyOf = o => [o.sandbox ? "sbx" : "live", String(o.rid), digits(o.listingId), String(o.hash || "")].join("|");
    const live = r => !!r && (r.state === "wanted" || r.state === "running" || r.state === "waiting");
    const wantedNow = key => { const r = read(key); return !!r && r.state !== "off"; };

    /* ── what the person reads: ONE calm line ── */
    function describe(rec) {
      if (!rec) return { phase: "off", line: "", sku: "", wait: "", attention: false };
      const sku = rec.sku || "", V = (phase, line, extra) => Object.assign({ phase, line, sku, wait: rec.wait || "", attention: phase === "attention", repo: !!rec.repo }, extra || {});
      if (rec.state === "off") return V("off", "");
      if (rec.state === "done") return rec.outcome === "rehearsed" ? V("rehearsed", PHASE_LINE.rehearsed) : rec.outcome === "already" ? V("linked", "Already linked to " + sku, { linked: true }) : V("linked", "Saved and linked · " + sku, { linked: true });
      if (rec.state === "attention") return V("attention", "Needs attention: " + (rec.why || "something went wrong"), { auto: rec.auto || "" });
      if (rec.state === "waiting") {
        if (rec.wait === "password" || rec.wait === "connection" || rec.wait === "data" || rec.wait === "retry") return V(rec.wait, PHASE_LINE[rec.wait] + (rec.repo && rec.wait !== "retry" ? "" : ""));
      }
      return V("saving", rec.repo ? "Saved to the repository" + (sku ? " as " + sku : "") + ". Linking on Etsy…" : PHASE_LINE.saving);
    }
    function announce(rec) { emit({ type: "state", key: rec.key, rec: Object.assign({}, rec), view: describe(rec) }); }
    function emit(ev) { for (const fn of [...listeners]) { try { fn(ev); } catch (_) { /* a listener never breaks a save */ } } }

    /* ── begin / cancel / retry ── */
    function begin(spec) {
      spec = spec || {};
      const sandbox = !!spec.sandbox, hash = String(spec.hash || ""), rid = String(spec.rid == null ? "" : spec.rid), listing = digits(spec.listingId);
      if (!rid || !listing || !hash) return { key: "", created: false, error: "no-file" };
      const key = keyOf({ sandbox, rid, listingId: listing, hash }), all = readAll(), ex = all[key];
      // one design per save: the file the switch was turned on for is the one that is saved; another file of the same order and listing waits for it to end
      const other = Object.values(all).find(r => r && r.key !== key && r.sandbox === sandbox && r.rid === rid && r.listingId === listing && live(r));
      if (other) return { key: other.key, created: false, conflict: true, rec: Object.assign({}, other) };
      const first = () => { Promise.resolve(spec.ready).catch(() => {}).then(() => setT(() => tick(key, "begin"), 0)); };   // (spec.ready: the design's own copy is stored first, so a reload finds it)
      if (ex && (live(ex) || ex.state === "done")) { if (live(ex)) first(); return { key, created: false, rec: Object.assign({}, ex) }; }
      const rec = { v: 1, key, sandbox, rid, listingId: listing, hash, name: String(spec.name || ""), kind: spec.kind === "dxf" ? "dxf" : "ai", by: String(spec.by || ""), ck: String(spec.ck || ""), title: String(spec.title || ""),
        line: spec.line || null, at: now(), upd: now(), state: "wanted", wait: "", why: "", sku: String((ex && ex.sku) || ""), repo: !!(ex && ex.repo), etsy: false, fails: 0, nextAt: 0, auto: "", outcome: "", tab };
      const next = Object.assign({}, all); next[key] = rec; writeAll(next);      // ← recorded BEFORE any step: nothing below has run yet
      engines.delete(key);
      announce(rec); remote(rec); first();
      return { key, created: true, rec: Object.assign({}, rec) };
    }
    function remote(rec) { try { if (deps.remote && typeof deps.remote.put === "function") Promise.resolve(deps.remote.put(Object.assign({}, rec))).catch(() => {}); } catch (_) { /* best effort: the browser's record is the truth */ } }
    function cancel(key, why) {
      const rec = read(key); if (!rec) return { ok: false, why: "unknown" };
      if (rec.state === "off") return { ok: true, already: true };
      if (rec.state === "done") return { ok: false, done: true };
      if (rec.repo) return { ok: false, committed: true };       // in the repository already: it cannot be un-saved, so the link is completed
      const r = patch(key, { state: "off", wait: "", why: String(why || "switched off"), nextAt: 0 }); remote(r); arm();
      return { ok: true };
    }
    function retry(key) {
      const rec = read(key); if (!rec || rec.state === "off" || rec.state === "done") return false;
      patch(key, { state: "wanted", wait: "", why: "", fails: 0, nextAt: 0, auto: "" }); tick(key, "retry"); return true;
    }

    /* ── the loop ── */
    function resume(reason) {
      const all = readAll(), t = now();
      for (const rec of Object.values(all)) {
        if (!rec) continue;
        if ((rec.state === "done" || rec.state === "off") && t - (rec.upd || 0) > KEEP_DONE_MS) { const n = Object.assign({}, readAll()); delete n[rec.key]; writeAll(n); continue; }
        // (a needs-attention save that Etsy may take later, e.g. after Paul re-authorised it, is tried once more per page load; the person is never asked for it)
        if (rec.state === "attention" && rec.auto && reason === "load" && !autoTried.has(rec.key)) { autoTried.add(rec.key); patch(rec.key, { state: "wanted", wait: "", fails: 0, nextAt: 0 }); }
        if (live(read(rec.key))) tick(rec.key, reason || "resume");
      }
      arm();
    }
    function tick(key, reason) {
      if (flights.has(key)) { redo.add(key); return flights.get(key); }
      const p = (async () => {
        try { do { redo.delete(key); await exclusive(key, () => oneTick(key, reason)); } while (redo.has(key)); }
        catch (e) { try { const r = read(key); if (live(r)) fail(key, String((e && e.message) || e)); } catch (_) { /* nothing more to say */ } }
        finally { flights.delete(key); arm(); }
      })();
      flights.set(key, p); return p;
    }
    /* one save per listing at a time (this tab: the queue below; other tabs: navigator.locks) */
    const queues = new Map();
    function exclusive(key, fn) {
      const rec = read(key); if (!rec) return Promise.resolve();
      const name = "cn-keep:" + (rec.sandbox ? "sbx:" : "") + rec.listingId, prev = queues.get(name) || Promise.resolve();
      const run = () => (deps.locks && typeof deps.locks.request === "function" ? deps.locks.request(name, () => fn()) : fn());
      const mine = prev.catch(() => {}).then(run);
      queues.set(name, mine); mine.then(() => { if (queues.get(name) === mine) queues.delete(name); }, () => { if (queues.get(name) === mine) queues.delete(name); });
      return mine;
    }
    function park(key, wait, why, extra) {
      const r = patch(key, Object.assign({ state: "waiting", wait, why: why || "" }, extra || {}));
      if (wait === "data") polls.set(key, (polls.get(key) || 0) + 1); else polls.delete(key);
      return r;
    }
    function fail(key, text, hard) {
      const rec = read(key); if (!rec) return;
      const n = (rec.fails || 0) + 1;
      if (hard || n > MAX_FAILS) { const r = patch(key, { state: "attention", wait: "", why: cleanWhy(text), fails: n, nextAt: 0, auto: hard === "etsy" ? "load" : "" }); remote(r); return; }
      patch(key, { state: "waiting", wait: "retry", why: cleanWhy(text), fails: n, nextAt: now() + backoffMs(n) });
    }
    const cleanWhy = t => String(t || "something went wrong").replace(/^(Not saved|Etsy not updated):\s*/i, "").replace(/\s+/g, " ").trim().slice(0, 160);

    async function oneTick(key) {
      let rec = read(key); if (!live(rec)) return;
      if (rec.nextAt && rec.nextAt > now()) return;
      if (!online()) { park(key, "connection"); return; }
      if (rec.etsyOnly) return etsyOnlyTick(key, rec);
      let r; try { r = await deps.resolve(rec); } catch (e) { r = { state: "wait", wait: "data", why: String((e && e.message) || e) }; }
      rec = read(key); if (!live(rec)) return;
      if (!r || r.state === "wait") { park(key, "data", r && r.why); return; }
      if (r.state === "skip") return;                      // not this workspace's (a sandbox page does not run a real save, nor the other way round)
      if (r.state === "lost") { patch(key, { state: "attention", wait: "", why: r.why || "the design is no longer on this browser: drop the file again", nextAt: 0 }); return; }
      if (r.state === "gone") {
        if (rec.repo) { patch(key, { state: "attention", wait: "", why: "the design is no longer on this order: open the order to finish linking it on Etsy" }); return; }
        const n = patch(key, { state: "off", wait: "", why: r.why || "the design is no longer on this order" }); remote(n); return;
      }
      if (!rec.sandbox && !lockHeld()) { park(key, "password"); return; }
      let eng = engines.get(key);
      if (!eng) {
        const ctx = Object.assign({}, r.ctx, { alive: () => wantedNow(key), lock: r.ctx && r.ctx.lock ? r.ctx.lock : deps.lock, sandbox: !!rec.sandbox });
        eng = (deps.make || make)(ctx); engines.set(key, eng);
      }
      if (!wired.has(eng)) { wired.add(eng); try { eng.on(ev => emit({ type: "engine", key, ev })); } catch (_) { /* no events */ } }
      patch(key, { state: "running", wait: "", why: "" }); polls.delete(key);
      let res = null, timedOut = false, wd = null;
      try { res = await Promise.race([eng.run(), new Promise(ok => { wd = setT(() => { timedOut = true; ok(null); }, WATCHDOG); })]); }
      catch (e) { res = { ok: false, done: "held", error: String((e && e.message) || e) }; }
      finally { try { clearT(wd); } catch (_) { /* gone */ } }
      if (timedOut) { patch(key, { state: "waiting", wait: "retry", why: "the cloud is slow", nextAt: now() + 15000 }); return; }
      settle(key, res || {}, (typeof eng.outcome === "function" && eng.outcome()) || {});
    }
    /* a save that reached the repository (maybe on another computer) whose Etsy step is not done: any page that loads may do that step; the cloud is the memory, this page the actor */
    function adoptPending(list, sandbox) {
      let k = 0;
      for (const p of Array.isArray(list) ? list : []) {
        const rid = String(p && p.rid || ""), listing = digits(p && p.listingId); if (!rid || !listing) continue;
        const key = [sandbox ? "sbx" : "live", rid, listing, "etsy"].join("|"), all = readAll(), mine = Object.values(all).find(r => r && r.rid === rid && r.listingId === listing && r.sandbox === !!sandbox && r.key !== key && (live(r) || r.state === "done"));
        if (mine || (all[key] && (live(all[key]) || all[key].state === "done" || all[key].state === "attention"))) continue;
        const rec = { v: 1, key, sandbox: !!sandbox, rid, listingId: listing, hash: "etsy", name: "", kind: "ai", by: "", ck: "", title: "", line: null, at: now(), upd: now(), state: "wanted", wait: "", why: "", sku: String(p.sku || ""), repo: true, etsy: false, fails: 0, nextAt: 0, auto: "", outcome: "", tab, etsyOnly: true };
        const next = Object.assign({}, all); next[key] = rec; writeAll(next); announce(rec); setT(() => tick(key, "pending"), 0); k++;
      }
      return k;
    }
    async function etsyOnlyTick(key, rec) {
      if (typeof deps.etsy !== "function") { patch(key, { state: "off", why: "no Etsy step here" }); return; }
      patch(key, { state: "running", wait: "", why: "" });
      let r; try { r = await deps.etsy(rec); } catch (e) { fail(key, (e && e.transient) ? "you could not reach the cloud" : String((e && e.message) || e)); return; }
      if (r && r.ok === true && (r.state === "linked" || r.linked === true)) { patch(key, { state: "done", wait: "", why: "", etsy: true, outcome: "linked", nextAt: 0, fails: 0 }); return; }
      const reason = (r && (r.reason || r.state || r.error)) || "error", text = (r && (r.text || ETSY_WHY[reason])) || (r && r.error) || "Etsy did not confirm the change", retry = !r || r.retry !== false;
      if (reason === "offline") { fail(key, text); return; }
      settle(key, { done: "saved" }, { done: "saved", reason, retry, text, sku: rec.sku });
    }
    function settle(key, res, out) {
      const rec = read(key); if (!rec) return;
      const sku = out.sku || rec.sku || "", done = out.done || res.done;
      if (done === "linked" || done === "rehearsed") { const n = patch(key, { state: "done", wait: "", why: "", sku, repo: true, etsy: done === "linked", outcome: done, fails: 0, nextAt: 0 }); remote(n); return; }
      if (done === "already") { const n = patch(key, { state: "done", wait: "", why: "", sku: out.sku || sku, outcome: "already", nextAt: 0 }); remote(n); return; }
      if (res.done === null || res.done === undefined) {        // the engine did not offer the save
        if (res.linked) { const n = patch(key, { state: "done", wait: "", why: "", sku: res.linked, outcome: "already", nextAt: 0 }); remote(n); return; }
        if (res.later === "connection") { park(key, "connection"); return; }
        if (res.later === "data") { park(key, "data", res.why); return; }
        if (res.error) { fail(key, res.error); return; }
        const n = patch(key, { state: "attention", wait: "", why: cleanWhy(res.why || "this design cannot be saved"), nextAt: 0 }); remote(n); return;
      }
      if (done === "saved") {                                   // in the repository; Etsy has not taken the SKU
        patch(key, { sku, repo: true });
        if (out.reason === "offline") { park(key, "connection", "", { sku, repo: true }); return; }
        if (out.reason === "running") { patch(key, { state: "waiting", wait: "retry", why: "Etsy is being updated by another run", nextAt: now() + 45000 }); return; }   // (another run holds the lease: wait, it is not a failure)
        if (LATER_ETSY.has(out.reason)) { fail(key, out.text || "Etsy did not take the SKU", "etsy"); return; }
        if (STOP_ETSY.has(out.reason) || out.retry === false) { fail(key, out.text || "Etsy did not take the SKU", "etsy-final"); return; }
        fail(key, out.text || "Etsy did not answer"); return;
      }
      // held: the repository is not saved
      const reason = out.reason || (res.held === "locked" ? "locked" : ""), held = out.held || res.held || "";
      if (reason === "cancelled") { if (read(key).state !== "off") patch(key, { state: "off", wait: "", why: "switched off" }); return; }
      if (held === "locked" || reason === "locked") { park(key, "password"); return; }
      if (reason === "offline") { patch(key, { state: "waiting", wait: online() ? "retry" : "connection", why: "", nextAt: online() ? now() + backoffMs((rec.fails || 0) + 1) : 0, fails: online() ? (rec.fails || 0) + 1 : rec.fails }); if ((rec.fails || 0) + 1 > MAX_FAILS * 2) fail(key, out.text || "the cloud does not answer"); return; }
      if (held === "taken" || reason === "sku-taken") { fail(key, "that SKU was just taken; choosing another"); return; }
      if (STOP_REPO.has(reason) || out.retry === false || held === "sku" && reason === "none") { fail(key, out.text || "this design cannot be saved", true); return; }
      fail(key, out.text || res.error || "something went wrong");
    }

    /* ── timers: only what is due, plus a slow beat while something waits (a missed event never strands a save) ── */
    function arm() {
      try { clearT(timer); } catch (_) { /* none */ } timer = null;
      let at = Infinity; const t = now();
      for (const rec of Object.values(readAll())) {
        if (!live(rec)) continue;
        const when = rec.nextAt > t ? rec.nextAt : rec.wait === "data" ? t + Math.min(15000, 1000 * Math.pow(2, Math.min(4, polls.get(rec.key) || 0))) : rec.state === "waiting" || rec.state === "running" || rec.state === "wanted" ? t + BEAT : Infinity;
        at = Math.min(at, when);
      }
      if (at < Infinity) timer = setT(() => { timer = null; resume("timer"); }, Math.max(20, at - t));
    }
    if (deps.lock && typeof deps.lock.onChange === "function") { try { stopLock = deps.lock.onChange(() => resume("lock")); } catch (_) { stopLock = null; } }

    return {
      begin, cancel, retry, resume, arm, describe, tab, adoptPending,
      get: key => read(key), list: () => Object.values(readAll()).map(r => Object.assign({}, r)),
      forOrder: rid => Object.values(readAll()).filter(r => r && r.rid === String(rid)).map(r => Object.assign({}, r)),
      wanted: wantedNow, engineOf: key => engines.get(key) || null,
      on(fn) { if (typeof fn === "function") listeners.add(fn); return () => listeners.delete(fn); },
      idle: async () => { for (let i = 0; i < 50 && flights.size; i++) await Promise.all([...flights.values()]); },
      external() { for (const r of Object.values(readAll())) announce(r); resume("storage"); },
      close() { try { clearT(timer); } catch (_) { /* none */ } if (stopLock) try { stopLock(); } catch (_) { /* none */ } listeners.clear(); }
    };
  }

  return { make, durable, TEXT: T, INTENTS_KEY };
});
