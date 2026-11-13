/* charm-nest-cloud-sheets.js — EVERY SHEET IS ONE CLOUD DOCUMENT, CHANGED FROM ANY COMPUTER (CLOUDALL, Paul, 13 Nov 2026: "Everything must be on the cloud
   nothing can be on one computer", "It doesn't matter which computer is doing that… That's why we're connecting it to the web").

   What there was: a sheet lived in the memory of the page that nested it. Another computer showed its saved record and refused every change to it
   ("its sheet is not open in this sorter"), and a page that still held an older copy of a sheet could save it over a newer one.

   What this does, on every computer alike:
     · the layout's revision (layoutRev, kept by the server with every save that carries the placements: charmNestLibrary.js op_putSheet, CLOUDALL S1)
       is known for every sheet this page holds. persistSheet sends it (layoutBase); a save made from an older copy is refused by the server and
       nothing is overwritten; this page then reads the sheet again (reload) and the change is made on what is there now.
     · a page learns a sheet's revision only when its own copy IS the cloud's (same pieces, same places): a page whose copy differs (an unsaved change,
       or a cloud record that is the stale one) is never refreshed from the cloud by itself; it is named in the log and saves as before.
     · a sheet changed elsewhere (the change feed names it, "s:<id>") is read again here: a saved record shown here is refreshed; a sheet this page
       holds, not at work and with no change of its own waiting, is rebuilt from the cloud (Recall.rebuild: its pieces from the master files, its places
       from the record), so every computer shows the same sheet within seconds.
     · ensureLive(sheetId): a sheet this page does not hold, or holds only as a saved record, becomes a sheet this page can change (Take off, Hold,
       Cancel, fill, Manual Control): read from the cloud and rebuilt. Its save goes to the cloud like any other.
   Nothing here changes what a sheet holds: it only reads, rebuilds what the cloud holds, and says which revision a save was made from.

   window.CharmNestCloudSheets: state() · reload(page) · ensureLive(sheetId) → page · learn() · revOf(page) */
(function (root) {
  "use strict";
  const safe = (f, d) => { try { const v = f(); return v === undefined ? d : v; } catch (_) { return d; } };
  const api = body => root.CN && typeof root.CN.api === "function" ? root.CN.api("charmNestLibrary", body, { quiet: true, timeoutMs: 30000 }) : Promise.reject(new Error("no api"));
  const st = { learned: 0, differs: [], reloads: 0, refreshed: 0, live: 0, conflicts: 0, skipped: 0, last: null, started: false };
  const say = (kind, text) => safe(() => root.CN && root.CN.agent ? root.CN.agent({ bridge: true }, kind, text) : console.info("[cloud-sheets]", text));
  const pages = () => safe(() => (typeof allSheets === "function" ? allSheets() : []), []) || [];
  const pageOf = id => pages().find(p => p && p.sheetId === id) || null;
  const BUSY = ["nesting", "finishing", "queued"];
  const atWork = p => !!p && (BUSY.includes(p.status) || !!p._operationStarting || !!p._rebuilding || !!(p.persisted && !p.persistedDone && !p.problem) || !!p.saveOwed);
  const r2 = v => Math.round((+v || 0) * 100) / 100;
  const sig = list => (list || []).map(p => [p.id, r2(p.cxPt), r2(p.cyPt), r2(p.angle)].join("@")).sort().join(",");
  const near = (a, b) => { if (a === b) return true; const A = a.split(","), Bv = b.split(","); if (A.length !== Bv.length) return false; for (let i = 0; i < A.length; i++) { const x = A[i].split("@"), y = Bv[i].split("@"); if (x[0] !== y[0]) return false; for (let k = 1; k < 4; k++) if (Math.abs(+x[k] - +y[k]) > 0.05) return false; } return true; };
  const revOf = p => (p && Number.isFinite(p.layoutRev) ? p.layoutRev : null);
  const getRec = async id => { const r = await api({ op: "getSheet", id }); return (r && r.sheet) || null; };

  /** The revision of every sheet this page holds as a live sheet whose copy is the cloud's. Run once a page is up; a page that differs keeps saving as before. */
  async function learn() {
    for (const p of pages()) {
      if (!p || !p.sheetId || p.recalled || revOf(p) !== null || atWork(p) || p.dirty) continue;
      let rec = null; try { rec = await getRec(p.sheetId); } catch (_) { continue; }
      if (!rec) continue;
      if (near(sig(p.placements), sig(rec.placements))) { p.layoutRev = +rec.layoutRev || 0; st.learned++; }
      else if (!st.differs.includes(p.sheetId)) { st.differs.push(p.sheetId); say("cloud", `${p.fileBase || p.sheetId}: this computer's copy differs from the cloud's (${(p.placements || []).length} pieces here, ${(rec.placements || []).length} in the cloud); it is left as it is and saves as before`); }
    }
    return st;
  }

  /** Rebuild a sheet this page holds from the cloud, as it is there now (its pieces from the master files, its places from the record). */
  async function reload(p, why) {
    if (!p || !p.sheetId || !root.Recall || typeof root.Recall.rebuild !== "function") return p;
    const rec = await getRec(p.sheetId); if (!rec) return p;
    p.charms = (p.charms || []).filter(c => !c.poolId); p.placements = [];
    p.recalled = rec;
    p.outputs = null; p.verification = rec.verification || null; p.cloud = Object.assign({ preview: rec.preview }, rec.outputs || {});
    p.backPool = Array.isArray(rec.backPool) ? rec.backPool : p.backPool;
    await root.Recall.rebuild(p);
    p.layoutRev = +rec.layoutRev || 0; p.persistedDone = true; p.persisted = Promise.resolve(); p.dirty = false; p.problem = null; delete p.saveOwed;
    st.reloads++; st.last = { at: Date.now(), id: p.sheetId, why: why || "" };
    say("cloud", `${rec.fileBase || p.sheetId}: read again from the cloud (${why || "changed on another computer"}) · ${(p.placements || []).length} pieces`);
    safe(() => root.CN.renderCard(p));
    return p;
  }

  /** A sheet this page can change, from the cloud: the page holding it (rebuilt when it holds only its saved record), else a new page made from the record. */
  async function ensureLive(id) {
    let p = pageOf(id);
    if (p && !p.recalled) return p;
    const rec = await getRec(id); if (!rec) throw new Error("that sheet is no longer in the cloud");
    if (!p) {
      if (typeof addPage !== "function") throw new Error("the page is not ready");
      p = addPage(rec.metal, { fresh: true });
      Object.assign(p, { status: "complete", sheetId: rec.id, runId: rec.runId || null, setId: rec.setId || null, seq: rec.setSeq || rec.seq || null, setDay: rec.day || null, sheetIndex: rec.sheetIndex == null ? null : rec.sheetIndex,
        fileBase: rec.fileBase || null, cardStartedAt: rec.cardStartedAt || rec.createdAt || null, draft: !!rec.draft, releaseFull: !!rec.releaseFull, label: rec.label || null, roseCutAt: rec.roseCutAt || null, laserDoneAt: rec.laserDoneAt || null,
        charms: [], placements: [], rejects: [], dirty: false, problem: null });
    }
    await reload(p, "opened here to be changed");
    st.live++;
    return p;
  }

  /** The change feed named sheets: each one this page shows is read again (a saved record refreshed; a live sheet rebuilt when it is not at work and newer). */
  const PENDING = new Set(); let pendT = 0;
  async function changed(keys) {
    for (const k of keys || []) {
      const id = String(k).slice(2); if (!id || id === "*") continue;
      const p = pageOf(id); if (!p) continue;
      if (atWork(p) || p.dirty) { PENDING.add(id); continue; }
      let rec = null; try { rec = await getRec(id); } catch (_) { PENDING.add(id); continue; }
      if (!rec) continue;
      if (p.recalled) { p.recalled = rec; st.refreshed++; safe(() => root.CN.renderCard(p)); continue; }
      const mine = revOf(p);
      if (mine === null) { st.skipped++; continue; }   // (a sheet whose copy here was not the cloud's is never refreshed by itself)
      // (only a newer revision, saved by another computer, rebuilds a sheet held here: a sheet this page is changing itself is never rebuilt under it)
      if ((+rec.layoutRev || 0) > mine) { try { await reload(p, "changed on another computer"); } catch (e) { PENDING.add(id); console.warn("[cloud-sheets] reload", id, e); } }
    }
    if (PENDING.size && !pendT) pendT = setTimeout(() => { pendT = 0; const ks = [...PENDING].map(id => "s:" + id); PENDING.clear(); changed(ks); }, 5000);
  }

  /** persistSheet's refusal from the server (409 conflict): the sheet is read again; the run goes on (its next look nests what is still waiting). */
  async function conflict(p) { st.conflicts++; try { await reload(p, "it changed on another computer while this one was saving it"); } catch (e) { console.warn("[cloud-sheets] conflict reload", e); } }

  function boot() {
    let tries = 0;
    const go = () => {
      tries++;
      const ready = !!(root.CN && typeof root.CN.api === "function" && typeof allSheets === "function" && root.Recall && root.B && root.B.orders);
      if (!ready && tries < 180) return setTimeout(go, 1000);
      if (!ready) return;
      st.started = true;
      const Y = root.CNSync;
      if (Y && typeof Y.follow === "function") Y.follow({ name: "cloud-sheets", kinds: "s", wild: "", wants: k => !!pageOf(String(k).slice(2)), read: (why, keys) => changed(keys) });
      setTimeout(() => { learn().catch(e => console.warn("[cloud-sheets] learn", e)); }, 15000);
      setInterval(() => { if (!(root.document && root.document.hidden)) learn().catch(() => {}); }, 300000);
    };
    go();
  }
  root.CharmNestCloudSheets = { state: () => Object.assign({}, st, { differs: st.differs.slice() }), reload, ensureLive, learn, revOf, conflict, _changed: changed };
  if (root.document) { if (root.document.readyState === "loading") root.document.addEventListener("DOMContentLoaded", boot); else boot(); }
})(typeof self !== "undefined" ? self : this);
