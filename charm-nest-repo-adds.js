/*  charm-nest-repo-adds.js
 *  ═══════════════════════════════════════════════════════════════════════
 *  "New charms ready for the repo" in ONE press (LETTERS-HOOPS, Paul 9 Nov 2026).
 *  Paul: "Extract all of these letters from my charm repo (a-z) and make 2 additional complete new sets of alphabet letters.
 *  Set A) Add a Necklace charm style hoop to the top of each letter (a-z). Set B) Add a Huggie Charm style hoop to each letter (a-z)."
 *
 *  The designs are made and checked in the repository (scripts/make-letter-hoops.cjs: the app's own indexer staged every per-SKU .ai,
 *  its picture and records.json, the exact entries masterPutIndex takes), committed under charm-repo-adds/<folder>/ with a contact sheet.
 *  Writing them into the Charm Repo is Paul's: this file adds ONE line to Workspace > Master files, under "Add a master file". Pressing it shows
 *  one plain confirm; "Yes" asks the repo password ONCE (the lock's own dialog: the password is never seen here, only the lock's token is
 *  used and never put in a URL), then the page does what the Master tab does for a master file, with these committed files:
 *    1. reads the library for the add's SKUs (masterGetMany: free). A SKU another master file already holds stops the press before anything is
 *       written (nothing is ever replaced here). All of them already there from this add: "Already there", nothing is sent.
 *    2. stores each missing SKU's .ai and picture under charmnest/master/ (charmNestOutput: the upload unlock), the master sheet under
 *       charmnest/master/files/, then masterPutIndex (the missing entries only, newest: false) and masterPutFile (the upload history);
 *    3. reads the library back and says how many landed. A closed tab loses nothing: press again, what is there is skipped.
 *  The committed list of adds is charm-repo-adds/queue.json. Everything is drawn with textContent.
 *
 *    CharmNestRepoAdds.attach(box, { repoApi, upBytes, api, toast, who })   the line, in the Master files tab (charm-nest-bridge.js)
 *    CharmNestRepoAdds.normalize(json)                                      the queue's adds: [{ id, title, folder, records, sheet, contact }]
 *    CharmNestRepoAdds.run(o, add)                                          one press after the password: { state: "done" | "already" | "stop", text, added }
 *  ═══════════════════════════════════════════════════════════════════════ */
(function (root) {
  "use strict";
  const doc = root.document;
  const QUEUE = "charm-repo-adds/queue.json";
  const FOLDER = /^charm-repo-adds\/[A-Za-z0-9._-]+$/, FILE = /^[A-Za-z0-9._-]+\.(?:json|ai|html)$/, HASH = /^[0-9a-f]{64}$/;
  const plural = (n, w) => n + " " + w + (n === 1 ? "" : "s");
  const up = s => String(s || "").trim().toUpperCase();

  /** The queue file → [{ id, title, folder, records, sheet, contact }] (an entry with a folder or file name outside charm-repo-adds/ is left out). */
  function normalize(json) {
    const arr = json && Array.isArray(json.adds) ? json.adds : [];
    const out = [], seen = new Set();
    for (const x of arr) {
      if (!x || typeof x !== "object") continue;
      const id = String(x.id || "").trim(), folder = String(x.folder || "").trim().replace(/\/+$/, "");
      const records = String(x.records || "records.json"), sheet = String(x.sheet || ""), contact = String(x.contact || "");
      if (!id || seen.has(id) || !FOLDER.test(folder) || !FILE.test(records) || (sheet && !FILE.test(sheet)) || (contact && !FILE.test(contact))) continue;
      seen.add(id);
      out.push({ id, title: String(x.title || id).slice(0, 120), folder, records, sheet, contact });
    }
    return out;
  }

  /** A committed file's address on this site (each path segment encoded: the per-SKU names hold spaces and brackets). */
  const siteUrl = (add, rel) => add.folder + "/" + String(rel).split("/").map(encodeURIComponent).join("/");

  /** The records of one add, checked: { masterHash, masterName, sheet: { pageW, pageH }, entries }. Throws with plain words. */
  function checkRecords(rec) {
    if (!rec || !HASH.test(String(rec.masterHash || "")) || !rec.masterName || !Array.isArray(rec.entries) || !rec.entries.length) throw new Error("the committed records of this add are not complete");
    const seen = new Set();
    for (const e of rec.entries) {
      const sku = up(e && e.sku);
      if (!sku || seen.has(sku)) throw new Error("the committed records name a SKU twice or none");
      if (!/^charmnest\/master\/[^/]+\.ai$/.test(String(e.aiPath || "")) || !/^charmnest\/master\/[^/]+\.png$/.test(String(e.thumbPath || ""))) throw new Error(`the committed record of ${sku} has no file`);
      seen.add(sku);
    }
    return rec;
  }

  async function bytesOf(o, url) {
    const res = await o.fetch(url, { cache: "no-store" });
    if (!res || !res.ok) throw new Error(`could not read ${decodeURIComponent(url.split("/").pop())} from this site (${res ? res.status : "no answer"})`);
    return new Uint8Array(await res.arrayBuffer());
  }

  /** What the library holds of these SKUs: Map(SKU → entry) (masterGetMany, a free read, 300 at a time). */
  async function held(o, skus) {
    const out = new Map();
    for (let i = 0; i < skus.length; i += 300) {
      const r = await o.api("charmNestLibrary", { op: "masterGetMany", bare: true, skus: skus.slice(i, i + 300) });
      for (const [k, e] of Object.entries((r && r.entries) || {})) if (e) out.set(up(k), e);
    }
    return out;
  }

  /** One press, after the confirm. o = { repoApi, upBytes, api, fetch, who, lock, say(text) }. */
  async function run(o, add) {
    const say = t => { try { if (typeof o.say === "function") o.say(t); } catch (_) { /* a hint */ } };
    let rec;
    try { const res = await o.fetch(siteUrl(add, add.records), { cache: "no-store" }); if (!res || !res.ok) throw new Error("the committed records could not be read (" + (res ? res.status : "no answer") + ")"); rec = checkRecords(await res.json()); }
    catch (e) { return { state: "stop", text: "Nothing was added: " + e.message + ". Reload the page and press again.", added: 0 }; }
    const entries = rec.entries, skus = entries.map(e => up(e.sku)), n = skus.length;
    const ask = { what: `Adding ${plural(n, "charm")} (${add.title}) to the charm repo.`, target: add.title, go: "Add" };
    const L = o.lock;
    if (!L || typeof L.ensure !== "function") return { state: "stop", text: "The charm repo lock did not load: reload the page. Nothing was added.", added: 0 };
    let info = null; try { info = await L.ensure("upload", ask); } catch (_) { info = null; }
    if (!info) return { state: "stop", text: "Nothing was added: the repo stayed locked.", added: 0 };
    let have;
    try { say("Looking in the library for these SKUs…"); have = await held(o, skus); }
    catch (e) { return { state: "stop", text: "Nothing was added: the library could not be read (" + (e.message || e) + "). Press again.", added: 0 }; }
    const other = skus.filter(s => have.has(s) && have.get(s).masterHash !== rec.masterHash);
    if (other.length) return { state: "stop", text: `Nothing was added: ${other.slice(0, 6).join(", ")}${other.length > 6 ? ` and ${other.length - 6} more` : ""} ${other.length === 1 ? "is" : "are"} already in the repo from another file, and this press never replaces a charm.`, added: 0, other };
    const missing = entries.filter(e => !have.has(up(e.sku)));
    if (!missing.length) return { state: "already", text: `Already there: all ${plural(n, "charm")} of ${add.title} are in the charm repo. Nothing was written.`, added: 0 };
    const by = (() => { try { return typeof o.who === "function" ? String(o.who() || "").trim() || null : null; } catch (_) { return null; } })();
    // the files: each missing SKU's .ai and picture, four at a time (the server names the stored file; its answer is what the entry keeps)
    const sent = []; let done = 0;
    try {
      const queue = missing.slice();
      await Promise.all(Array.from({ length: 4 }, async () => {
        while (queue.length) {
          const e = queue.shift();
          const [ai, png] = await Promise.all([bytesOf(o, siteUrl(add, "files/" + e.aiPath)), bytesOf(o, siteUrl(add, "files/" + e.thumbPath))]);
          const a = await o.upBytes(e.aiPath, ai, "application/illustrator", `Saving ${e.sku}`, add.title);
          const p = await o.upBytes(e.thumbPath, png, "image/png", undefined, add.title);
          sent.push(Object.assign({}, e, { aiPath: a.path, aiUrl: a.url || "", thumbPath: p.path, thumbUrl: p.url || "" }));
          say(`Storing the charm files: ${++done} of ${missing.length}…`);
        }
      }));
    } catch (e) {
      if (e && e.cancelled) return { state: "stop", text: "Nothing was added: the repo stayed locked.", added: 0 };
      return { state: "stop", text: `Stopped while storing the files (${e.message || e}): no charm was added to the index. Press again: it starts over and skips nothing it should not.`, added: 0 };
    }
    let sheet = null;
    if (add.sheet) { try { const b = await bytesOf(o, siteUrl(add, add.sheet)); sheet = await o.upBytes(`charmnest/master/files/${rec.masterHash.slice(0, 12)}-${add.sheet}`, b, "application/pdf", "Saving the master sheet", add.title); } catch (_) { sheet = null; /* the sheet is a record for "download"; the charms do not need it */ } }
    sent.sort((a, b) => skus.indexOf(up(a.sku)) - skus.indexOf(up(b.sku)));
    let r;
    try {
      say("Writing the charm library index…");
      r = await o.repoApi("upload", ask, { op: "masterPutIndex", entries: sent, masterHash: rec.masterHash, masterPath: sheet ? sheet.path : null, masterName: rec.masterName, hashSource: "local", replaces: [], newest: false, by });
      const s = rec.sheet || {};
      await o.repoApi("upload", ask, { op: "masterPutFile", file: { masterHash: rec.masterHash, path: sheet ? sheet.path : null, url: sheet ? sheet.url || null : null, name: rec.masterName, charms: n, labelled: n, held: n - missing.length, unlabelled: [], orphans: [], duplicates: [], undecodable: 0, blocked: [], skus, indexedBy: "local-indexer", by, pageW: s.pageW || null, pageH: s.pageH || null } });
    } catch (e) {
      if (e && e.cancelled) return { state: "stop", text: "The files are stored but the index was not written: the repo stayed locked. Press again to finish.", added: 0 };
      return { state: "stop", text: `The files are stored but the index may not be written (${e.message || e}). Press again to finish: what is there is skipped.`, added: 0 };
    }
    // read back: what the library holds now, from this add
    let after = new Map(); try { after = await held(o, skus); } catch (_) { /* said below */ }
    const landed = skus.filter(s => after.has(s) && after.get(s).masterHash === rec.masterHash).length;
    const refused = [].concat((r && r.rejected) || [], (r && r.blocked) || [], (r && r.pairKept) || []).map(x => x && x.sku).filter(Boolean);
    if (landed === n) return { state: "done", text: `Done: ${plural(missing.length, "charm")} added to the charm repo${missing.length < n ? ` (${n - missing.length} were already there)` : ""}. All ${n} of ${add.title} are in the Library now.`, added: missing.length };
    return { state: "stop", text: `${landed} of ${n} are in the repo now${refused.length ? `; the library refused ${refused.slice(0, 6).join(", ")}` : ""}. Press again to add the rest.`, added: Math.max(0, landed - (n - missing.length)) };
  }

  /* ── the line in the Master files tab ── */
  const CSS = `
.raBox{display:grid;gap:6px;margin:8px 0 2px;padding:9px 11px;border:1px solid var(--line);border-radius:10px;background:var(--card2,transparent)}
.raBox[hidden]{display:none}
.raBox .raHead{font-size:12px;line-height:1.45;color:var(--ink)}
.raBox .raHead b{font-weight:700}
.raBox .raRow{display:flex;flex-wrap:wrap;gap:6px;align-items:center}
.raBox .raRow .btn[hidden]{display:none}
.raBox .raSay{font-size:11px;line-height:1.45;color:var(--ink70);overflow-wrap:anywhere}
.raBox .raSay:empty{display:none}
.raBox .raSay.ok{color:var(--sage)}
.raBox .raSay.bad{color:#8a3a26}
.raBox a{font-size:11px;color:var(--ink70)}
.raBox .spin{display:inline-block;width:10px;height:10px;margin-right:6px;vertical-align:-1px;border:2px solid rgba(0,0,0,.15);border-top-color:currentColor;border-radius:50%;animation:raSpin .7s linear infinite}
@keyframes raSpin{to{transform:rotate(360deg)}}
`;
  function css() { try { if (!doc || doc.getElementById("cnRepoAddsCss")) return; const s = doc.createElement("style"); s.id = "cnRepoAddsCss"; s.textContent = CSS; doc.head.appendChild(s); } catch (_) { /* unstyled, still works */ } }
  const el = (tag, cls, text) => { const e = doc.createElement(tag); if (cls) e.className = cls; if (text != null) e.textContent = text; return e; };

  let queue = null, loading = null;
  async function readQueue(f) { try { const res = await f(QUEUE, { cache: "no-store" }); if (!res.ok) return []; return (queue = normalize(await res.json())); } catch (_) { return []; } }
  const loadQueue = f => queue ? Promise.resolve(queue) : (loading = loading || readQueue(f).then(q => { loading = null; return q; }));

  /** One line per committed add, in `box`. o = { repoApi, upBytes, api, toast, who }. */
  function attach(box, o) {
    if (!box || !doc) return;
    o = o || {}; css();
    const f = typeof o.fetch === "function" ? o.fetch : (...a) => root.fetch(...a);
    loadQueue(f).then(list => {
      box.textContent = "";
      for (const add of list) {
        const card = el("div", "raBox"), head = el("div", "raHead"), row = el("div", "raRow"), sayEl = el("div", "raSay");
        sayEl.setAttribute("aria-live", "polite");
        const go = el("button", "btn gold sm", "Add these charms to the repo"), yes = el("button", "btn gold sm", "Yes, add them"), no = el("button", "btn ghost sm", "Cancel");
        for (const b of [go, yes, no]) b.type = "button";
        go.setAttribute("data-repo", "upload"); yes.setAttribute("data-repo", "upload");
        yes.hidden = no.hidden = true;
        head.append(el("b", "", "New charms ready: "), add.title + ".");
        row.append(go, yes, no);
        if (add.contact) { const a = el("a", "", "See them first (contact sheet)"); a.href = siteUrl(add, add.contact); a.target = "_blank"; a.rel = "noopener"; row.append(a); }
        card.append(head, row, sayEl); box.appendChild(card);
        let busy = false;
        const show = (text, kind, spin) => { sayEl.className = "raSay" + (kind ? " " + kind : ""); sayEl.textContent = ""; if (spin) sayEl.append(el("span", "spin")); sayEl.append(text || ""); };
        go.onclick = () => { if (busy) return; go.hidden = true; yes.hidden = no.hidden = false; show("These charms will be added to the charm repo, each with its picture. A charm already in the repo is never replaced; one already added is skipped. You will be asked for the repo password once.", ""); try { yes.focus({ preventScroll: true }); } catch (_) { /* a convenience */ } };
        no.onclick = () => { if (busy) return; go.hidden = false; yes.hidden = no.hidden = true; show("", ""); };
        yes.onclick = async () => {
          if (busy) return; busy = true; yes.hidden = no.hidden = true; go.hidden = false; go.disabled = true; show("Asking for the repo password once…", "", true);
          let out;
          try { out = await run({ repoApi: o.repoApi, upBytes: o.upBytes, api: o.api, fetch: f, who: o.who, lock: root.CharmNestRepoLock, say: t => show(t, "", true) }, add); }
          catch (e) { out = { state: "stop", text: "Stopped: " + (e && e.message || e) + ". Press again: what is there is skipped." }; }
          busy = false; go.disabled = false;
          show(out.text, out.state === "stop" ? "bad" : "ok", false);
          try { if (typeof o.toast === "function") o.toast(out.text, out.state === "stop" ? "bad" : "ok", 9000); } catch (_) { /* a hint */ }
          try { if (out.added && typeof o.reload === "function") o.reload(); } catch (_) { /* the grid refreshes on the next look */ }
        };
      }
      try { if (root.CharmNestRepoLock) root.CharmNestRepoLock.mark(box); } catch (_) { /* the padlock is only a hint */ }
    });
  }

  root.CharmNestRepoAdds = { attach, normalize, run, checkRecords, siteUrl };
  if (typeof module !== "undefined" && module.exports) module.exports = root.CharmNestRepoAdds;
})(typeof window !== "undefined" ? window : globalThis);
