/* charm-nest-cloud-world.js — THE CLOUD WORLD ON THIS PAGE (CLOUDWORLD, Paul 20 Nov 2026: "the computers are never allowed to store their own information.
   Unless the cloud gives it to them ... Only the cloud can decide").

   The server reads every open order line once (netlify/functions/_charmNestWorld.js) and keeps the full line, the reading, the run's facts and every
   decision in Firestore, with the Review built from them. This file holds THAT on this page, and nothing else:
     · in memory only: nothing is written to localStorage, IndexedDB or any cache. A reload starts empty and reads the cloud again;
     · read whole once (worldGet { lines }, paged), then only what the cloud wrote after the world this page holds (sinceAt), each time the changes feed
       names w:world (the server bumps it after every build), when the tab is shown again, when the network comes back, and once a minute as a net;
     · what it holds is the cloud's, as the cloud has it: nothing here reads an order, decides a state or counts a chip.

   window.CloudWorld: start() · ready() → Promise · summary() · review() · lines() → Map(key → line) · line(key) · at() · on(fn) → off · refresh(why) · state() */
(function (root) {
  "use strict";
  if (!root.document || root.CloudWorld) return;
  const safe = (f, d) => { try { const v = f(); return v === undefined ? d : v; } catch (_) { return d; } };
  const sandbox = () => safe(() => typeof WORKSPACE_SANDBOX !== "undefined" && !!WORKSPACE_SANDBOX, false);
  const api = body => root.CN && typeof root.CN.api === "function" ? root.CN.api("charmNestLibrary", body, { quiet: true, timeoutMs: 60000 }) : Promise.reject(new Error("no api"));
  const NET_MS = 60000;
  const W = { lines: new Map(), summary: null, review: null, lease: null, at: 0, loadedAt: 0, reads: 0, fails: 0, error: null, started: false, busy: null, again: false, fns: new Set() };
  let readyRes; const readyP = new Promise(r => { readyRes = r; });

  function tell(why) { for (const fn of [...W.fns]) safe(() => fn({ why, summary: W.summary, review: W.review, size: W.lines.size })); safe(() => root.dispatchEvent(new root.CustomEvent("cn:world", { detail: { why } }))); }
  function take(lines) { for (const l of lines || []) { if (!l || !l.key) continue; if (l.gone) W.lines.delete(l.key); else W.lines.set(l.key, l); } }

  /** Reads the cloud world: whole the first time (and after a failed read), then what was written after the world held. */
  async function read(why) {
    if (sandbox()) return null;
    const whole = !W.loadedAt || why === "whole";
    let cursor = null, pages = 0, got = null;
    const since = whole ? 0 : W.at;
    const fresh = whole ? new Map() : null;
    do {
      got = await api(Object.assign({ op: "worldGet", lines: true }, since ? { sinceAt: since } : {}, cursor ? { cursor } : {}));
      if (!got || got.error) throw new Error((got && got.error) || "no answer");
      if (whole) { for (const l of got.lines || []) if (l && l.key && !l.gone) fresh.set(l.key, l); } else take(got.lines);
      cursor = got.next || null; pages++;
    } while (cursor && pages < 20);
    if (whole) W.lines = fresh;
    W.summary = got.summary || null; W.review = got.review || null; W.lease = got.lease || null;
    W.at = (W.summary && +W.summary.at) || W.at; W.loadedAt = Date.now(); W.reads++; W.error = null;
    return got;
  }
  async function refresh(why) {
    if (W.busy) { W.again = true; return W.busy; }
    W.busy = (async () => {
      try { await read(why); tell(why); readyRes && readyRes(true); }
      catch (e) { W.fails++; W.error = String((e && e.message) || e).slice(0, 200); if (/unknown op|production only/i.test(W.error)) W.dead = true; }
      finally { W.busy = null; if (W.again && !W.dead) { W.again = false; setTimeout(() => refresh("again"), 200); } }
    })();
    return W.busy;
  }
  function start() {
    if (W.started || sandbox()) return; W.started = true;
    refresh("whole");
    try { if (root.CNSync && typeof root.CNSync.follow === "function") root.CNSync.follow({ name: "world", kinds: "w", wild: "w", active: () => !W.dead, read: () => refresh("feed") }); } catch (_) { /* the net below */ }
    setInterval(() => { if (!W.dead && !root.document.hidden) refresh("net"); }, NET_MS);
    root.document.addEventListener("visibilitychange", () => { if (!root.document.hidden && !W.dead) refresh("shown"); });
    root.addEventListener("online", () => { if (!W.dead) refresh("online"); });
  }
  function boot() { let n = 0; const go = () => { if (root.CN && typeof root.CN.api === "function" && safe(() => root.CN.S.cloud.ok, null) === true) return start(); if (++n < 240) setTimeout(go, 1000); }; go(); }

  root.CloudWorld = {
    start, refresh, ready: () => readyP,
    summary: () => W.summary, review: () => W.review, lease: () => W.lease, lines: () => W.lines, line: key => W.lines.get(String(key)) || null, at: () => W.at,
    on: fn => { W.fns.add(fn); return () => W.fns.delete(fn); },
    state: () => ({ size: W.lines.size, at: W.at, loadedAt: W.loadedAt, reads: W.reads, fails: W.fails, error: W.error, dead: !!W.dead, live: W.summary ? W.summary.live : null })
  };
  if (root.document.readyState === "loading") root.document.addEventListener("DOMContentLoaded", boot); else boot();
})(typeof window !== "undefined" ? window : globalThis);
