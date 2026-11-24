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
  /* (FOLLOWFEED, Paul 17 Nov 2026: "one cloud truth, every computer shows the same sheets in real time"; designs-window-e2e.cjs, scenario A, proved it by the feed's own answers): REVS keeps the revision (updateTime) of the cloud record this page last read for each sheet, so a look at a sheet
     that has not moved is one field read (getSheet ifRev / getMany ifRevs) and not the whole record. getRec(id, probe) → the record, SAME (probe: it has not moved since this page read it) or null. */
  const REVS = new Map(), SAME = Object.freeze({ same: true });
  const getRec = async (id, probe) => {
    const ask = { op: "getSheet", id, pieces: true }; if (probe && REVS.has(id)) ask.ifRev = REVS.get(id);
    const r = await api(ask); if (r && r.unchanged === true && ask.ifRev) return SAME;
    if (r && r.sheet) r.sheet.pools = r.pools || await root.Recall.piecesOf(r.sheet);   // (NOSTORE: the pieces' rows come with the sheet, in one answer)
    if (r && r.sheet && r.rev) REVS.set(id, String(r.rev));
    return (r && r.sheet) || null;
  };
  /* (CLOUDALL C6, Paul 13 Nov 2026: "Sheets that haven't been saved yet ... exist only on the computer doing it"): a sheet this page is nesting, or holds changed and not nested yet, is marked on its cloud record (working: { status, dev, by, at }), cleared when it is saved; every
     other computer sees it and does not change that sheet meanwhile (ensureLive refuses it, in words, for 15 minutes at most: a mark left by a page that closed does not lock it for good) */
  const devId = () => safe(() => root.CharmNestRunFollow.dev(), "") || "";
  const whoAmI = () => String(safe(() => root.B && root.B.employee, "") || "").trim();
  const WORK = new Map(), workOf = p => (!p || !p.sheetId || p.recalled || !(p.layoutRev != null || p.persistedDone === true || (p.cloud && Object.keys(p.cloud).length)) ? "" : BUSY.includes(p.status) ? "nesting" : p.dirty ? "waiting" : "");
  let announcing = false;
  async function announce() {
    if (announcing) return; announcing = true;
    try { for (const p of pages()) { const k = workOf(p), had = WORK.has(p.sheetId) ? WORK.get(p.sheetId) : null; if (had === null && !k) { WORK.set(p.sheetId, ""); continue; } if (k === had) continue; WORK.set(p.sheetId, k);
      try { await api({ op: "putSheet", sheet: { id: p.sheetId, working: k ? { status: k, dev: devId(), by: whoAmI(), at: Date.now() } : null } }); st.announced = (st.announced || 0) + 1; } catch (_) { WORK.set(p.sheetId, had); } } }
    finally { announcing = false; }
  }
  /* (CLOUDMARKS, Paul 14 Nov 2026: "You must put all this in the cloud"): a sheet's set marks, when it joined its set (joinedAt), whether a person put it there (joinedBy) and the set it was taken out of (leftSet), were kept only in the
     workspace of the computer carrying the run. They are written to the sheet's cloud record whenever they change, by that computer only (a following computer's copy may be older),
     and every computer that builds the sheet from the cloud takes them from it (ensureLive below, the run's restore in charm-nest-bridge.js). A sheet with nothing on it is never written. */
  const MARKS = new Map(); let marking = false;
  async function marks() {
    const run = safe(() => root.B && root.B.run, null); if (marking || !run || !run.runId) return; marking = true;
    try { for (const p of pages()) { if (!p || !p.sheetId || p.recalled || p.runId !== run.runId || !(p.charms || []).length || !(p.persistedDone === true || p.layoutRev != null)) continue;
        const m = { joinedAt: +p.joinedAt || null, joinedBy: p.joinedBy || null, leftSet: p.leftSet || null }, sig = JSON.stringify(m); if (MARKS.get(p.sheetId) === sig) continue;
        if (p.restored && !MARKS.has(p.sheetId)) { MARKS.set(p.sheetId, sig); continue; }   // (built from its cloud record: these ARE the cloud's marks)
        try { await api({ op: "putSheet", sheet: Object.assign({ id: p.sheetId }, m) }); MARKS.set(p.sheetId, sig); st.marks = (st.marks || 0) + 1; } catch (_) { /* tried again at the next look */ } } }
    finally { marking = false; }
  }
  const lockOf = rec => { const w = rec && rec.working; return w && w.status && w.dev && w.dev !== devId() && Date.now() - (+w.at || 0) < 15 * 60000 ? w : null; };

  /** The revision of every sheet this page holds as a live sheet whose copy is the cloud's. Run once a page is up; a page that differs keeps saving as before. */
  async function learn() {
    for (const p of pages()) {
      if (!p || !p.sheetId || p.recalled || atWork(p) || p.dirty) continue;
      /* (RESTOREDSTALE, Paul 18 Nov 2026: "why her nesting is not updating"): a followed sheet that came back from this computer's saved workspace keeps the revision it was saved with and nothing had read its record, so the sweep (which looks only at
         sheets this page has read) never looked at it again: Michelle's Rose Gold draft stayed at 7 pieces, revision 6, while the cloud held 27, revision 32. It is read once here; a newer one is rebuilt, the same one is now known to the sweep. */
      if (revOf(p) !== null) {
        if (!p.followed || REVS.has(p.sheetId)) continue;
        let cur = null; try { cur = await getRec(p.sheetId); } catch (_) { continue; }
        if (cur && (+cur.layoutRev || 0) > revOf(p) && !atWork(p) && !p.dirty) { try { await reload(p, "this computer's saved copy was older than the cloud's"); } catch (e) { console.warn("[cloud-sheets] restored sheet", p.sheetId, e && e.message); } }
        continue;
      }
      let rec = null; try { rec = await getRec(p.sheetId); } catch (_) { continue; }
      if (!rec) continue;
      if (near(sig(p.placements), sig(rec.placements))) { p.layoutRev = +rec.layoutRev || 0; st.learned++; }
      else if (!st.differs.includes(p.sheetId)) { st.differs.push(p.sheetId); say("cloud", `${p.fileBase || p.sheetId}: this computer's copy differs from the cloud's (${(p.placements || []).length} pieces here, ${(rec.placements || []).length} in the cloud); it is left as it is and saves as before`); }
    }
    return st;
  }

  /** Rebuild a sheet this page holds from the cloud, as it is there now (its pieces from the master files, its places from the record). */
  async function reload(p, why, o) {   // (QUIETLIVE, Paul 19 Nov 2026: o.background = the page's own building of the followed sheets after a reload: no progress pill, the page is let answer, no thumbnail redraw per sheet)
    if (!p || !p.sheetId || !root.Recall || typeof root.Recall.rebuild !== "function") return p;
    const rec = await getRec(p.sheetId); if (!rec) return p;
    safe(() => root.CNViewer && typeof root.CNViewer.sawSheet === "function" && root.CNViewer.sawSheet(p, rec));   // (S4: the cloud's nest of this sheet, as its record says)
    p.recalled = rec; p.answer = rec; p.recStockId = rec.roseStockId || "";   // (MOVESHOW, Paul 15 Nov 2026: the physical sheet the record names, kept for the tile that says what a sheet lies on; a rebuilt sheet no longer carries the record)
    p.outputs = null; p.verification = rec.verification || null; p.cloud = Object.assign({ preview: rec.preview }, rec.outputs || {});
    p.backPool = Array.isArray(rec.backPool) ? rec.backPool : p.backPool;
    // (a sheet is drawn whole from this answer or not at all: until then, and when a design cannot be read, the cloud's own saved picture stands)
    try { await root.Recall.rebuild(p, Object.assign({ answer: rec }, o && o.background ? { background: true } : {})); }
    catch (e) { p.charms = (p.charms || []).filter(c => !c.poolId); p.placements = []; safe(() => root.CN.renderCard(p)); throw e; }
    p.layoutRev = +rec.layoutRev || 0; p.persistedDone = true; p.persisted = Promise.resolve(); p.dirty = false; p.problem = null; delete p.saveOwed;
    st.reloads++; st.last = { at: Date.now(), id: p.sheetId, why: why || "" };
    say("cloud", `${rec.fileBase || p.sheetId}: read again from the cloud (${why || "changed on another computer"}) · ${(p.placements || []).length} pieces`);
    safe(() => root.CN.renderCard(p));
    // (FOLLOWFEED: the pieces of this sheet are other charms now, so every thumbnail on this page that shows one looks at its piece again: its picture key names the piece this sheet holds (charm-nest-bridge.js ListMedia pieceSig))
    if (!(o && o.background)) safe(() => root.OrderWin && typeof root.OrderWin.remade === "function" && root.OrderWin.remade([p.sheetId]));   // (an order window open on this sheet draws it again at once: order 4231992262, 23 Nov 2026, showed the old discs until its tab was opened again)
    if (!(o && o.background)) safe(() => root.CharmNestMadeThumb && typeof root.CharmNestMadeThumb.refreshSoon === "function" && root.CharmNestMadeThumb.refreshSoon());   // (a background build asks once, when the last sheet is built)
    return p;
  }

  /** A sheet this page can change, from the cloud: the page holding it (rebuilt when it holds only its saved record), else a new page made from the record. */
  async function ensureLive(id) {
    let p = pageOf(id);
    if (p && !p.recalled) return p;
    const rec = await getRec(id); if (!rec) throw new Error("that sheet is no longer in the cloud");
    { const w = lockOf(rec); if (w) throw new Error(`${rec.fileBase || "This sheet"} is being ${w.status === "nesting" ? "nested" : "changed"} on another computer${w.by ? " (" + w.by + ")" : ""} right now; it can be changed here once it is saved there`); }   // (CLOUDALL C6, Paul 13 Nov 2026: "Sheets that haven't been saved yet ... exist only on the computer doing it")
    if (!p) {
      if (typeof addPage !== "function") throw new Error("the page is not ready");
      p = addPage(rec.metal, { fresh: true });
      Object.assign(p, { status: "complete", sheetId: rec.id, runId: rec.runId || null, setId: rec.setId || null, seq: rec.setSeq || rec.seq || null, setDay: rec.day || null, sheetIndex: rec.sheetIndex == null ? null : rec.sheetIndex,
        fileBase: rec.fileBase || null, cardStartedAt: rec.cardStartedAt || rec.createdAt || null, draft: !!rec.draft, releaseFull: !!rec.releaseFull, label: rec.label || null, roseCutAt: rec.roseCutAt || null, laserDoneAt: rec.laserDoneAt || null,
        ...(+rec.joinedAt ? { joinedAt: +rec.joinedAt } : {}), ...(rec.joinedBy ? { joinedBy: rec.joinedBy } : {}), ...(rec.leftSet ? { leftSet: rec.leftSet } : {}),   // (CLOUDMARKS, Paul 14 Nov 2026: "You must put all this in the cloud")
        charms: [], placements: [], rejects: [], dirty: false, problem: null });
    }
    await reload(p, "opened here to be changed");
    st.live++;
    return p;
  }

  /** The change feed named sheets: each one this page shows is read again (a saved record refreshed; a live sheet rebuilt when it is not at work and newer). */
  /* (TAKEOFFSOLID G1, Paul, 15 Nov 2026: "make sure it's absolutely rock solid ... between the cloud and all the computers"): a take-off writes the piece's row and the sheet's piece list first (poolUpdate: the record is flagged dirty and no longer lists the piece)
     and the pressing page rewrites the sheet (layout, files) after. If that page closed or failed between the two, the record keeps the piece in its layout and files and nothing wrote it again. The computer that CARRIES
     the run (B.run is its own; a follower never does this) completes such a sheet of its run: when the record is dirty, lists fewer pieces than its layout holds, and has been left alone for st.grace ms (no other computer's
     working mark newer than two minutes), it reads the sheet again (the pieces no longer listed are not put back: Recall.rebuild) and writes it as it stands (SheetWin.hopKit.writeOwed), once, with a log line. */
  const DONE = new Set(), LATER = new Set(); st.grace = 90000;
  async function completeTakeOff(p, rec) {
    const run = safe(() => root.B && root.B.run, null); if (!run || !run.runId || p.runId !== run.runId) return false;
    if (rec.dirty !== true || !Array.isArray(rec.poolIds) || rec.archived || +rec.laserDoneAt > 0 || +rec.roseCutAt > 0) return false;
    const listed = new Set(rec.poolIds.map(String)), stale = (rec.charms || []).filter(c => c && c.poolId && !listed.has(String(c.poolId)));
    if (!stale.length) return false;
    const key = rec.id + ":" + (+rec.layoutRev || 0) + ":" + rec.poolIds.length; if (DONE.has(key)) return false;
    const w = rec.working, now = Date.now(), quiet = Math.max(st.grace - (now - (+rec.updatedAt || 0)), w && w.dev && w.dev !== devId() ? 120000 - (now - (+w.at || 0)) : 0);
    if (quiet > 0) { if (!LATER.has(rec.id)) { LATER.add(rec.id); setTimeout(() => { LATER.delete(rec.id); changed(["s:" + rec.id]); }, quiet + 1000); } return true; }
    DONE.add(key);
    try {
      say("cloud", `${rec.fileBase || rec.id}: a take-off of ${stale.length} piece${stale.length === 1 ? "" : "s"} was not finished on the computer that made it; this computer (it carries the run) writes the sheet as it now stands`);
      await reload(p, "a take-off made on another computer was not finished");
      const K = root.SheetWin && root.SheetWin.hopKit;
      if (K && typeof K.writeOwed === "function") await K.writeOwed([p]);
      st.completed = (st.completed || 0) + 1;
    } catch (e) { DONE.delete(key); console.warn("[cloud-sheets] complete take-off", rec.id, e); }
    return true;
  }
  const PENDING = new Set(); let pendT = 0; let orderT = 0;
  /* (FOLLOWFEED, Paul 17 Nov 2026) a record read while a take-off or an exchange in place is half written (dirty: the piece list already cut, the layout still to be written by the computer that carries the run) is read again 5 s later, up to two minutes:
     the write that finishes it names the sheet on the feed as well (below), this is the net under it */
  const DIRTYN = new Map(), again = (id, dirty) => { if (!dirty) { DIRTYN.delete(id); return; } const n = (DIRTYN.get(id) || 0) + 1; DIRTYN.set(id, n); if (n <= 24) PENDING.add(id); };
  async function changed(keys, probe) {
    for (const k of keys || []) {
      const id = String(k).slice(2); if (!id || id === "*") continue;
      const p = pageOf(id); if (!p) continue;
      if (atWork(p) || p.dirty) { PENDING.add(id); continue; }
      let rec = null; try { rec = await getRec(id, probe); } catch (_) { PENDING.add(id); continue; }
      if (rec === SAME) { if (DIRTYN.has(id)) again(id, true); continue; }   // (not moved since this page read it; read half written, it is looked at again)
      if (!rec) continue;
      safe(() => root.CNViewer && typeof root.CNViewer.sawSheet === "function" && root.CNViewer.sawSheet(p, rec));   // (S4: a viewer learns the cloud's nest of this sheet, nestJob, for its card's Stop)
      again(id, rec.dirty === true);
      // (ORDERCLOUD 2, Paul 15 Nov 2026: the carrier's new draft order never reached the computer that follows): a followed sheet whose order changed in the cloud has this computer list the run's sheets again
      if (p.followed && rec.draftOrder != null && Number.isFinite(+rec.draftOrder) && +rec.draftOrder !== +p.draftOrder) { clearTimeout(orderT); orderT = setTimeout(() => safe(() => { const R = root.Recall, F = root.CharmNestRunFollow, id = (F && F.state().runId) || p.runId; if (R && R.follow && id) Promise.resolve(R.follow(id, { force: true })).catch(() => {}); }), 1500); }
      if (p.recalled) { p.recalled = rec; p.answer = rec; st.refreshed++; safe(() => root.CN.renderCard(p)); if (!p.followed || revOf(p) === null) continue; }
      if (await completeTakeOff(p, rec)) continue;
      const mine = revOf(p);
      if (mine === null) { st.skipped++; continue; }   // (a sheet whose copy here was not the cloud's is never refreshed by itself)
      // (only a newer revision, saved by another computer, rebuilds a sheet held here: a sheet this page is changing itself is never rebuilt under it)
      if ((+rec.layoutRev || 0) > mine) { try { await reload(p, "changed on another computer", { background: true }); } catch (e) { PENDING.add(id); console.warn("[cloud-sheets] reload", id, e); } }
    }
    if (PENDING.size && !pendT) pendT = setTimeout(() => { pendT = 0; const ks = [...PENDING].map(id => "s:" + id); PENDING.clear(); changed(ks, true); }, 5000);
  }
  /* (FOLLOWFEED, Paul 17 Nov 2026: "one cloud truth, every computer shows the same sheets in real time"; the cause of "the follower's own picture of a sheet does not take the new charm" (designs-window-e2e.cjs, scenario A, traced answer by answer): the changes feed hands a surface the NAMES that
     concern it only when nothing else made the answer concern it (charm-nest-sync-worker.js take); an answer that also carried '*' (a write that named nothing, here the exchange's own piece writes), a reset, a verify or a lost feed hands it NO names, though the same answer named the sheets. changed([])
     read nothing, so the last write of an exchange in place (rev 6 to 8 on the sheet) never reached the following computer: it had read the sheet half written (7 of 8 pieces listed, layout 6) and kept it. A read with no names now looks at every sheet this page shows
     that can still change, all in ONE call by revision (getMany ifRevs: a sheet that has not moved costs one field read), and the ones that moved are read as if the feed had named them; at most every 8 s, the last one never lost. A write of this computer itself is not looked at (its page is the newest). */
  let sweepT = 0, sweepAt = 0, sweeping = false;
  async function sweep() {
    const wait = 8000 - (Date.now() - sweepAt);
    if (sweeping || wait > 0) { if (!sweepT) sweepT = setTimeout(() => { sweepT = 0; sweep(); }, Math.max(1000, wait) + 100); return; }
    sweeping = true; sweepAt = Date.now();
    try {
      const mine = pages().filter(p => p && p.sheetId && REVS.has(p.sheetId) && !(+p.laserDoneAt > 0) && !(+p.roseCutAt > 0) && !(p.recalled && +p.recalled.laserDoneAt > 0)), ifRevs = {};
      for (const p of mine) ifRevs["s:" + p.sheetId] = REVS.get(p.sheetId);
      const keys = Object.keys(ifRevs); if (!keys.length) return;
      const moved = []; for (let i = 0; i < keys.length; i += 300) { const part = keys.slice(i, i + 300), r = await api({ op: "getMany", keys: part, ifRevs: Object.fromEntries(part.map(k => [k, ifRevs[k]])) }); moved.push(...Object.keys((r && r.recs) || {})); }
      st.swept = (st.swept || 0) + 1; if (moved.length) { st.sweptMoved = (st.sweptMoved || 0) + moved.length; await changed(moved); }
    } catch (e) { console.warn("[cloud-sheets] sweep", e && e.message); }
    finally { sweeping = false; }
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
      if (Y && typeof Y.follow === "function") Y.follow({ name: "cloud-sheets", kinds: "s", wild: "", wants: k => !!pageOf(String(k).slice(2)), read: (why, keys, at, names) => { if (keys && keys.length) return changed(keys); if (names && names.length && why !== "wrote") { changed(names); } if (why !== "wrote") return sweep(); } });   // (ENGLIVE: the names of an answer that also carried a wildcard / reset / verify are read at once, then the sweep)   // (FOLLOWFEED: no names = '*', a reset, a verify or a lost feed: every sheet shown here is looked at, by revision)
      setTimeout(() => { learn().catch(e => console.warn("[cloud-sheets] learn", e)); }, 15000);
      setInterval(() => { if (!(root.document && root.document.hidden)) learn().catch(() => {}); }, 300000);
      setInterval(() => { announce().catch(() => {}); }, 4000);
      setInterval(() => { marks().catch(() => {}); }, 15000);   // (CLOUDMARKS, Paul 14 Nov 2026: "You must put all this in the cloud")   // (CLOUDALL C6, Paul 13 Nov 2026: "Sheets that haven't been saved yet ... exist only on the computer doing it")
    };
    go();
  }
  root.CharmNestCloudSheets = { state: () => Object.assign({}, st, { differs: st.differs.slice() }), reload, ensureLive, learn, revOf, conflict, announce, lockOf, marks, setGrace: ms => { st.grace = +ms || 0; }, _changed: changed };
  if (root.document) { if (root.document.readyState === "loading") root.document.addEventListener("DOMContentLoaded", boot); else boot(); }
})(typeof self !== "undefined" ? self : this);
