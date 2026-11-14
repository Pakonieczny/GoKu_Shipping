/* charm-nest-cloud-share.js — WHAT EVERY COMPUTER KEEPS THE SAME, KEPT IN THE CLOUD (CLOUDSHARE, Paul, 14 Nov 2026: "Do all of these they all must be in the cloud",
   and "Make Firebase and Etsy cheaper without losing any real-time function").

   The Sorter kept some of its work in this browser only: its Settings, the stock preset, the set-aside notices, the drafts of unsent messages (customer mail and
   Team), the lists it learned of finished and shipped orders and of the steps after the laser, and the moves of Manual Control not saved yet. Each is now mirrored,
   field by field, into one shared store in the cloud (charmNestLibrary sharePut / shareGet, collection Charm_Nest_Shared):
     · a change made here is written to the cloud a moment after it is made (one write for a burst), only the fields that changed;
     · the cloud names the store in the changes feed (g:<store>), so every other computer reads it at once (about a second): nothing is asked on a timer;
     · each field keeps the time it was written: the later write wins, field by field, so two computers changing two settings never undo each other;
     · a field removed on one computer is removed on every computer.
   What belongs to ONE computer stays on it: whether it runs the sandbox, its graphics/processor settings, the Design Station it frames, its sounds and alerts, its
   Auto run mode and its own Etsy order-check timing, its layout choices, and every secret (passcodes, keys). A setting that arrives from another computer changes what
   the next nest and the next screen use; it never starts a nest again here (the computer where the person saved it does what it always did).

   window.CharmNestCloudShare: state() · pull(names) · push(name) */
(function (root) {
  "use strict";
  if (!root.document || root.CharmNestCloudShare) return;
  const safe = (f, d) => { try { const v = f(); return v === undefined ? d : v; } catch (_) { return d; } };
  const LS = root.localStorage, SS = root.sessionStorage;
  const sandbox = () => safe(() => typeof WORKSPACE_SANDBOX !== "undefined" && !!WORKSPACE_SANDBOX, false);
  const world = () => (sandbox() ? "sandbox" : "production");
  const api = body => root.CN && typeof root.CN.api === "function" ? root.CN.api("charmNestLibrary", Object.assign({ sandbox: sandbox() || undefined }, body), { quiet: true, timeoutMs: 20000 }) : Promise.reject(new Error("no api"));
  const dev = () => safe(() => root.CharmNestRunFollow.dev(), "") || "";
  const who = () => String(safe(() => (typeof B !== "undefined" ? B : root.B).employee, "") || "").trim();
  const SO = () => safe(() => root.CN.S, null);   // (the Sorter's state: a constant of the page, reached as CN.S)
  const st = { started: false, pulls: 0, puts: 0, applied: 0, fails: 0, last: null, errors: [] };
  const J = v => { try { return JSON.stringify(v); } catch (_) { return ""; } };
  const parse = (t, d) => { try { const v = JSON.parse(t); return v == null ? d : v; } catch (_) { return d; } };
  const hash = s => { let h = 2166136261; for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 16777619); } return h >>> 0; };

  /* ── the stores ── each: name (the cloud's), shards (a big list is kept in several documents), key() (this browser's storage key, or match(k) for a family of
     keys), fields(raw) → { field: value } (what is shared), build(fields, raw) → raw (this browser's value made of the shared fields and what stays here), apply(changed) */
  const SETTINGS_HERE = new Set(["v", "sandbox", "sandboxSeed", "sandboxStream", "sandboxSpeed", "computePipeline", "parallel", "serverAbove", "masterServerAbove", "rail", "orderView", "notify", "sound", "dsOrigin",
    "heartbeatS", "heartbeatMiss", "runMode", "pollOrders", "pollMinutes", "autoEvery", "etsyHourlyCap", "nestFlow", "learnedBudgetS", "learnedStallS"]);
  const STORES = [
    { name: "sorter-settings", key: () => "cn.settings", noSandbox: true,
      fields: raw => { const o = parse(raw, {}), f = {}; for (const [k, v] of Object.entries(o || {})) if (!SETTINGS_HERE.has(k)) f[k] = v; return f; },
      build: (f, raw) => { const o = parse(raw, {}); for (const k of Object.keys(o)) if (!SETTINGS_HERE.has(k) && !(k in f)) delete o[k]; return J(Object.assign(o, f)); },
      apply: changed => { const s = safe(() => SO().settings, null); if (!s) return; for (const [k, v] of Object.entries(changed)) { if (SETTINGS_HERE.has(k)) continue; if (v === undefined) delete s[k]; else s[k] = v; }
        safe(() => root.renderNestFlow && root.renderNestFlow()); safe(() => root.refreshAllCards && root.refreshAllCards()); } },
    { name: "sorter-stock", key: () => "cn.stock", noSandbox: true,
      fields: raw => (raw ? { preset: raw } : {}), build: f => (f.preset == null ? null : String(f.preset)),
      apply: changed => { if (!("preset" in changed) || !changed.preset) return; safe(() => { SO().stockPreset = changed.preset; }); safe(() => { const sel = root.document.getElementById("stockSel"); if (sel) sel.value = changed.preset; }); safe(() => root.refreshAllCards && root.refreshAllCards()); } },
    { name: () => "autocancel-" + world(), key: () => "cn.autoCancel.v1:" + world(),
      // the set-aside notices and the orders already dealt with are shared; this computer's own work in hand (jobs, the cursor of its read, what it read) stays here
      fields: raw => { const o = parse(raw, {}), f = {}; for (const n of Array.isArray(o.notices) ? o.notices : []) if (n && n.rid) f["n:" + n.rid + ":" + (+n.at || 0)] = n; for (const [rid, d] of Object.entries(o.done || {})) f["d:" + rid] = d; return f; },
      build: (f, raw) => { const o = parse(raw, {}); o.notices = Object.entries(f).filter(([k]) => k.startsWith("n:")).map(([, v]) => v).sort((a, b) => (+a.at || 0) - (+b.at || 0)); o.done = {}; for (const [k, v] of Object.entries(f)) if (k.startsWith("d:")) o.done[k.slice(2)] = v; return J(o); },
      apply: () => { safe(() => root.AutoCancel && root.AutoCancel.reload && root.AutoCancel.reload()); safe(() => root.RunCtl && root.RunCtl.renderBanner()); } },
    { name: () => "maildrafts-" + world(), key: () => "cn.mail.drafts" + (sandbox() ? ":sandbox" : ""),
      fields: raw => Object.assign({}, parse(raw, {})), build: f => J(f),
      apply: () => safe(() => root.CustomerMail && root.CustomerMail.reloadDrafts && root.CustomerMail.reloadDrafts()) },
    { name: () => "teamdrafts-" + world(), key: () => "cn.team.drafts" + (sandbox() ? ":sandbox" : ""),
      fields: raw => Object.assign({}, parse(raw, {})), build: f => J(f),
      apply: changed => safe(() => root.TeamMail && root.TeamMail.draftsArrived && root.TeamMail.draftsArrived(Object.keys(changed))) },
    { name: () => "completed-" + world(), shards: 16, key: () => "cn.completed.v1:" + world(),
      fields: raw => { const f = {}; for (const e of parse(raw, [])) if (e && e.rid) f[String(e.rid)] = e; return f; }, build: f => J(Object.values(f)),
      apply: changed => safe(() => root.CNCompleted && root.CNCompleted.reload && root.CNCompleted.reload(Object.keys(changed))) },
    { name: () => "flowstages-" + world(), shards: 4, key: () => "cn.flow.v1:" + world(),
      fields: raw => { const f = {}; for (const e of parse(raw, [])) if (Array.isArray(e) && e[0]) f[String(e[0])] = [e[1], e[2]]; return f; }, build: f => J(Object.entries(f).map(([rid, v]) => [rid, v[0], v[1]])),
      apply: changed => safe(() => root.CNFlow && root.CNFlow.reload && root.CNFlow.reload(Object.keys(changed))) },
    // Manual Control's moves not saved yet, by sheet: kept here under this tab's own key (cn.mc.draft.<sheet>.<tab>), so Manual Control turned on for that sheet on ANY
    // computer puts them back (the claim still lets only one person move a sheet's charms at a time); a save drops them everywhere
    { name: () => "mcdrafts-" + world(), shards: 4, family: true,
      match: k => /^cn\.mc\.draft\.[\w\-]+\.[\w\-]+$/.test(k),
      sheetOf: k => k.split(".")[3],
      keyFor: sheet => `cn.mc.draft.${sheet}.${safe(() => SS.getItem("cn.sheetwin.tab"), "") || "t0000"}` }
  ];
  const nameOf = s => (typeof s.name === "function" ? s.name() : s.name);
  const docsOf = s => (s.shards ? Array.from({ length: s.shards }, (_, i) => `${nameOf(s)}-${i}`) : [nameOf(s)]);
  const docOf = (s, field) => (s.shards ? `${nameOf(s)}-${hash(field) % s.shards}` : nameOf(s));

  /* ── what this browser last agreed with the cloud: per store document, field → [time written, the value's text] ── */
  const SEEN_KEY = () => "cn.share.seen." + world();
  let seen = parse(safe(() => LS.getItem(SEEN_KEY()), "{}"), {});
  let seenT = 0; const keepSeen = () => { clearTimeout(seenT); seenT = setTimeout(() => { try { realSet.call(LS, SEEN_KEY(), J(seen)); } catch (_) {} }, 500); };
  const seenOf = doc => seen[doc] || (seen[doc] = {});

  /* ── this browser's value of a store, as fields ── */
  function localFields(s) {
    if (s.family) { const f = {}; for (let i = 0; i < LS.length; i++) { const k = LS.key(i); if (!k || !s.match(k)) continue; const v = parse(LS.getItem(k), null); if (!v) continue; const id = s.sheetOf(k); if (!f[id] || (+v.at || 0) > (+f[id].at || 0)) f[id] = v; } return f; }
    return s.fields(safe(() => LS.getItem(s.key()), null));
  }
  let quiet = false;
  const realSet = Storage.prototype.setItem, realRemove = Storage.prototype.removeItem;
  function writeLocal(s, fields, removed) {
    quiet = true;
    try {
      if (s.family) {
        for (const id of removed) for (let i = LS.length - 1; i >= 0; i--) { const k = LS.key(i); if (k && s.match(k) && s.sheetOf(k) === id) realRemove.call(LS, k); }
        for (const [id, v] of Object.entries(fields)) { let mine = false; for (let i = 0; i < LS.length; i++) { const k = LS.key(i); if (k && s.match(k) && s.sheetOf(k) === id) { mine = true; break; } } if (!mine) realSet.call(LS, s.keyFor(id), J(v)); }
        return;
      }
      const raw = s.build(fields, safe(() => LS.getItem(s.key()), null));
      if (raw == null) realRemove.call(LS, s.key()); else realSet.call(LS, s.key(), raw);
    } catch (e) { st.errors.push(String(e && e.message || e)); } finally { quiet = false; }
  }

  /* ── to the cloud: the fields that changed here since the last agreement ── */
  const pending = new Map();   // store → timer
  function changedHere(s) { clearTimeout(pending.get(s)); pending.set(s, setTimeout(() => { pending.delete(s); push(s).catch(() => {}); }, 800)); }
  async function push(s, opts) {
    if (!st.started) return;
    const now = localFields(s), byDoc = new Map();
    for (const doc of docsOf(s)) byDoc.set(doc, { set: {}, del: [] });
    for (const [k, v] of Object.entries(now)) { const doc = docOf(s, k), was = seenOf(doc)[k], t = J(v); if (!was || was[1] !== t) byDoc.get(doc).set[k] = v; }
    for (const doc of docsOf(s)) for (const k of Object.keys(seenOf(doc))) if (!(k in now) && seenOf(doc)[k][1] !== null) byDoc.get(doc).del.push(k);
    for (const [doc, w] of byDoc) {
      if (!Object.keys(w.set).length && !w.del.length) continue;
      try {
        const r = await api({ op: "sharePut", name: doc, set: w.set, del: w.del, ifAbsent: !!(opts && opts.ifAbsent), dev: dev(), by: who() });
        if (!r || r.error) throw new Error((r && r.error) || "no answer");
        const at = +r.at || Date.now(), mine = seenOf(doc), wrote = new Set(r.wrote || []);
        for (const [k, v] of Object.entries(w.set)) if (wrote.has(k)) mine[k] = [at, J(v)];
        for (const k of w.del) if (wrote.has(k) || !mine[k]) mine[k] = [at, null];
        st.puts++; keepSeen();
        if (opts && opts.ifAbsent && wrote.size < Object.keys(w.set).length) await pull([doc]);   // (another computer was first: its values are taken here)
      } catch (e) { st.fails++; st.last = { at: Date.now(), error: String(e && e.message || e) }; changedHere(s); return; }   // (tried again a moment later)
    }
  }

  /* ── from the cloud: the fields written since the last agreement ── */
  async function pull(docs) {
    const want = [...new Set(docs)].filter(Boolean); if (!want.length) return;
    let r = null; try { r = await api({ op: "shareGet", names: want }); } catch (e) { st.fails++; return; }
    st.pulls++;
    const items = (r && r.items) || {};
    const byStore = new Map();
    for (const s of STORES) for (const doc of docsOf(s)) if (want.includes(doc)) { if (!byStore.has(s)) byStore.set(s, []); byStore.get(s).push(doc); }
    for (const [s, list] of byStore) {
      const fields = localFields(s), changed = {}, removed = [];
      for (const doc of list) {
        const it = items[doc], mine = seenOf(doc); if (!it || !it.f) continue;
        for (const [k, e] of Object.entries(it.f)) {
          const at = +e.at || 0, was = mine[k]; if (was && was[0] >= at) continue;
          if (e.x) { mine[k] = [at, null]; if (k in fields) { delete fields[k]; changed[k] = undefined; removed.push(k); } continue; }
          const v = parse(e.j, null), t = J(v); mine[k] = [at, t];
          if (J(fields[k]) !== t) { fields[k] = v; changed[k] = v; }
        }
      }
      keepSeen();
      if (!Object.keys(changed).length) continue;
      writeLocal(s, fields, removed); st.applied += Object.keys(changed).length;
      if (s.apply) safe(() => s.apply(changed));
    }
  }

  /* ── this browser's writes, heard: a store's key set or removed here is sent a moment later (another tab of this computer hears it by its storage event, as before) ── */
  function storeOfKey(k) { for (const s of STORES) { if (s.family ? s.match(k) : s.key() === k) return s; } return null; }
  Storage.prototype.setItem = function (k, v) { const r = realSet.apply(this, arguments); if (!quiet && this === LS && st.started) { const s = storeOfKey(String(k)); if (s) changedHere(s); } return r; };
  Storage.prototype.removeItem = function (k) { const r = realRemove.apply(this, arguments); if (!quiet && this === LS && st.started) { const s = storeOfKey(String(k)); if (s) changedHere(s); } return r; };

  /* ── start: read every store once, take what is newer there, send what is here and not there yet; then follow the changes feed ── */
  async function start() {
    if (st.started) return; st.started = true;
    const all = STORES.flatMap(docsOf);
    for (let i = 0; i < all.length; i += 50) await pull(all.slice(i, i + 50));
    for (const s of STORES) await push(s, { ifAbsent: true });   // (a field this computer has and the cloud has never had: given; one the cloud already has: taken from it above)
    for (const s of STORES) await push(s);                       // (what changed here while this page was closed or offline)
    const S = root.CNSync;
    if (S && typeof S.follow === "function") {
      S.follow({ name: "cloud-share", kinds: "g", wild: "g", active: () => true,
        wants: k => all.includes(String(k).slice(2)),
        read: (why, keys) => { const docs = why === "keys" || why === "wrote" || why === "dirty" ? (keys || []).map(k => String(k).slice(2)).filter(d => all.includes(d)) : all; pull(docs.length ? docs : all).catch(() => {}); } });
    }
    // the net: everything once every ten minutes while the page is shown (one read per store document, only when the feed is not live)
    setInterval(() => { if (!root.document.hidden && !(S && S.live && S.live())) pull(all).catch(() => {}); }, 600000);
  }
  function boot() {
    let tries = 0;
    const go = () => { tries++; if (!(root.CN && typeof root.CN.api === "function" && SO() && SO().settings && root.CharmNestRunFollow) && tries < 180) return setTimeout(go, 1000); setTimeout(() => start().catch(e => { st.fails++; st.last = { at: Date.now(), error: String(e && e.message || e) }; }), 2000); };
    go();
  }
  root.CharmNestCloudShare = { state: () => Object.assign({}, st, { stores: STORES.map(nameOf) }), pull: names => pull(names || STORES.flatMap(docsOf)), push: name => { const s = STORES.find(x => nameOf(x) === name); return s ? push(s) : Promise.resolve(); } };
  if (root.document.readyState === "loading") root.document.addEventListener("DOMContentLoaded", boot); else boot();
})(typeof self !== "undefined" ? self : this);
