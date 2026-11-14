/* charm-nest-cloud-tray.js — THE NEST RAIL'S DROPPED FILES ARE SHARED BY EVERY COMPUTER (CLOUDTRAY, Paul, 14 Nov 2026: "You must put all this in the cloud" -> "Yes, build it").

   A file dropped on the Nest rail is read on the computer it was dropped on and its bytes are uploaded to the cloud as before (persistSource: charmnest/sources/...). What was missing
   was the tray itself: which files wait there, unassigned, was known only to that computer. Now:
     · a file read here is listed in the cloud tray (charmNestLibrary trayPut, keyed by the file's own hash): "tray" while none of its charms has a metal, "assigned" as soon as one of
       them is given a metal here (this computer claims the file: its charms nest here only, never on two computers), "removed" when it is taken off the rail here;
     · every other computer reads the tray (trayList: every 20 s while the page is shown) and puts a waiting file on its own rail from its cloud copy (read like a dropped file);
       a file claimed or removed on another computer leaves this rail, when none of its charms is on a sheet here.
   Nothing here touches an order, a run or a sheet.

   window.CharmNestCloudTray: state() · look() · announce() */
(function (root) {
  "use strict";
  const safe = (f, d) => { try { const v = f(); return v === undefined ? d : v; } catch (_) { return d; } };
  const api = body => root.CN && typeof root.CN.api === "function" ? root.CN.api("charmNestLibrary", body, { quiet: true, timeoutMs: 20000 }) : Promise.reject(new Error("no api"));
  const dev = () => safe(() => root.CharmNestRunFollow.dev(), "") || "";
  const who = () => String(safe(() => root.B && root.B.employee, "") || "").trim();
  const S = () => safe(() => root.CN.S, null);
  const st = { put: 0, taken: 0, dropped: 0, fails: 0, last: null };
  const said = new Map();   // hash -> state this computer last wrote
  const fetching = new Set();
  const sources = () => (S() && S().sources) || [];
  const unassigned = () => (S() && S().unassigned) || [];
  const onSheets = src => safe(() => (typeof allSheets === "function" ? allSheets() : []).some(p => (p.charms || []).some(c => c.sourceId === src.id)), false);
  // the tray state of a file this page holds: removed once it left the rail; assigned once any charm of it has a metal; else tray
  function stateOf(src) {
    if (!sources().includes(src)) return "removed";
    if (src.metal || onSheets(src) || (src.charms || []).some(c => c.pendingMetal)) return "assigned";
    return "tray";
  }
  const held = new Map();   // hash -> the source object last seen here (to tell that it was removed)
  async function announce() {
    for (const src of sources()) {
      if (!src || !src.hash || src.state !== "ready" || !src.cloud || !src.cloud.path) continue;
      held.set(src.hash, src);
      if (src.fromTray && stateOf(src) === "tray") continue;   // (a file read from the tray is the other computer's to list: only what this computer does to it is written)
      const s = stateOf(src); if (said.get(src.hash) === s) continue;
      try { await api({ op: "trayPut", id: src.hash, name: src.name, path: src.cloud.path, url: src.cloud.url || "", state: s, metal: src.metal || null, dev: dev(), by: who() }); said.set(src.hash, s); st.put++; }
      catch (_) { st.fails++; }
    }
    for (const [hash, src] of held) {
      if (sources().includes(src)) continue;
      held.delete(hash);
      if (said.get(hash) === "removed" || src.trayGone) continue;
      try { await api({ op: "trayPut", id: hash, name: src.name, path: (src.cloud && src.cloud.path) || "", url: (src.cloud && src.cloud.url) || "", state: "removed", metal: null, dev: dev(), by: who() }); said.set(hash, "removed"); st.put++; }
      catch (_) { st.fails++; }
    }
  }
  async function look() {
    let r = null; try { r = await api({ op: "trayList" }); } catch (_) { st.fails++; return; }
    const me = dev(), list = (r && r.items) || []; st.last = { at: Date.now(), n: list.length };
    const byHash = new Map(sources().filter(s => s && s.hash).map(s => [s.hash, s]));
    for (const it of list) {
      const here = byHash.get(it.id);
      if (it.state === "tray") {
        if (here || it.dev === me || fetching.has(it.id) || !it.url) continue;
        fetching.add(it.id);
        try {
          const res = await fetch(it.url); if (!res.ok) throw new Error("HTTP " + res.status);
          const bytes = new Uint8Array(await res.arrayBuffer());
          const before = new Set(sources());
          await root.CN.intake([new File([bytes], it.name || "file.pdf", { type: "application/pdf" })], null);
          for (const s of sources()) if (!before.has(s)) { s.fromTray = it.id; s.trayBy = it.by || ""; }
          st.taken++;
          try { root.LiveStrip && root.LiveStrip.push({ t: Date.now(), kind: "info", text: `${it.name} is in the tray from ${it.by || "another computer"}` , __remote: true }); } catch (_) {}
        } catch (e) { st.fails++; console.warn("[cloud-tray] take", it.name, e && e.message); }
        finally { fetching.delete(it.id); }
      } else if (here && it.dev !== me && (it.state === "assigned" || it.state === "removed")) {
        // claimed or removed on another computer: it leaves this rail, when nothing of it is on a sheet here (then it stays, and is said)
        if (onSheets(here) || (here.charms || []).some(c => c.pendingMetal) || here.metal) continue;
        here.trayGone = true; said.set(it.id, "removed");
        try { if (typeof root.removeSource === "function") root.removeSource(here); else { S().sources = sources().filter(s => s !== here); S().unassigned = unassigned().filter(c => c.sourceId !== here.id); } } catch (_) {}
        try { if (typeof root.renderRail === "function") root.renderRail(); } catch (_) {}
        st.dropped++;
        try { root.LiveStrip && root.LiveStrip.push({ t: Date.now(), kind: "info", text: `${it.name} left the tray: ${it.state === "assigned" ? "taken" : "removed"} on ${it.by || "another computer"}`, __remote: true }); } catch (_) {}
      }
    }
  }
  function boot() {
    let tries = 0;
    const go = () => { tries++; if (!(root.CN && typeof root.CN.api === "function" && typeof root.CN.intake === "function" && S()) && tries < 180) return setTimeout(go, 1000);
      setInterval(() => { announce().catch(() => {}); }, 5000);
      setInterval(() => { if (!(root.document && root.document.hidden)) look().catch(() => {}); }, 20000);
      setTimeout(() => look().catch(() => {}), 6000); };
    go();
  }
  root.CharmNestCloudTray = { state: () => Object.assign({}, st), look, announce };
  if (root.document) { if (root.document.readyState === "loading") root.document.addEventListener("DOMContentLoaded", boot); else boot(); }
})(typeof self !== "undefined" ? self : this);
