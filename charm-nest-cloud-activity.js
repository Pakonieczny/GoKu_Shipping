/* charm-nest-cloud-activity.js — THE ACTIVITY LOG IS SHARED BY EVERY COMPUTER (CLOUDALL S4, Paul, 13 Nov 2026: "Small lists ... the activity log").

   Every line this page writes to its Recent activity (LiveStrip.push) is also sent to the cloud (charmNestLibrary activityPut, a batch every 10 s), and the lines the other computers
   wrote are read back (activityGet: every 20 s while Recent activity is open, every 3 minutes otherwise) and shown here with who wrote them. Lines from before this page opened are not
   read. Nothing here changes an order, a piece or a sheet.

   window.CharmNestCloudActivity: state() · flush() · poll(force) */
(function (root) {
  "use strict";
  const safe = (f, d) => { try { const v = f(); return v === undefined ? d : v; } catch (_) { return d; } };
  const api = body => root.CN && typeof root.CN.api === "function" ? root.CN.api("charmNestLibrary", body, { quiet: true, timeoutMs: 20000 }) : Promise.reject(new Error("no api"));
  const dev = () => safe(() => root.CharmNestRunFollow.dev(), "") || "";
  const who = () => String(safe(() => root.B && root.B.employee, "") || "").trim();
  const st = { sent: 0, got: 0, fails: 0, last: null, hooked: false };
  const queue = [], seen = new Set();
  let since = Date.now(), lastPoll = 0, busy = false;
  function hook() {
    const L = root.LiveStrip; if (!L || typeof L.push !== "function") return false; if (L.__cloud) return true;
    const orig = L.push;
    L.push = function (ev) { orig.call(L, ev); if (ev && !ev.__remote) queue.push({ t: ev.t || Date.now(), kind: String(ev.kind || ""), text: String(ev.text || (ev.html ? String(ev.html).replace(/<[^>]+>/g, "") : "")).slice(0, 300) }); };
    L.__cloud = true; st.hooked = true; return true;
  }
  async function flush() {
    if (!queue.length) return; const rows = queue.splice(0, 60);
    try { await api({ op: "activityPut", rows, dev: dev(), by: who() }); st.sent += rows.length; }
    catch (_) { st.fails++; if (queue.length < 200) queue.unshift(...rows); }
  }
  const open = () => safe(() => { const b = root.document.getElementById("railRecentBody"); return !!(b && b.parentElement && b.parentElement.open); }, false);
  async function poll(force) {
    const now = Date.now(); if (busy || (!force && now - lastPoll < (open() ? 20000 : 180000))) return; busy = true; lastPoll = now;
    try {
      const r = await api({ op: "activityGet", since }), me = dev();
      for (const bt of (r && r.batches) || []) {
        since = Math.max(since, +bt.at || 0); const key = bt.at + "|" + bt.dev; if (seen.has(key)) continue; seen.add(key);
        if (bt.dev === me) continue;
        for (const row of bt.rows || []) { safe(() => root.LiveStrip.push({ t: row.t, kind: row.kind, text: `(${bt.by || "another computer"}) ${row.text}`, __remote: true })); st.got++; }
      }
      st.last = { at: now };
    } catch (_) { st.fails++; } finally { busy = false; }
  }
  function boot() {
    let tries = 0;
    const go = () => { tries++; if (!(hook() && root.CN && typeof root.CN.api === "function") && tries < 180) return setTimeout(go, 1000); setInterval(flush, 10000); setInterval(() => { if (!(root.document && root.document.hidden)) poll(false); }, 5000); };
    go();
  }
  root.CharmNestCloudActivity = { state: () => Object.assign({ queued: queue.length }, st), flush, poll };
  if (root.document) { if (root.document.readyState === "loading") root.document.addEventListener("DOMContentLoaded", boot); else boot(); }
})(typeof self !== "undefined" ? self : this);
