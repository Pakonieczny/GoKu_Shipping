/* charm-nest-set-override.js — OVERRIDE, on a set's card (Paul, 23 Nov 2026: "I need the ability to overwrite and approve for laser cutting any given set even though it may contain sheets that are not
   ready or have orders that are held. This is only allowed with my password. When I choose to overwrite and release a given Set of sheets to Laser cutting it should automatically release ALL Holds on
   ALL orders contained within the Set of sheets.")
     CharmNestSetOverride.press({ setId, name })   the "Override" button beside a grey "Approve for laser cutting", and beside "Mark set completed" when the cloud refused it
       1  the password, in the one dialog that asks for it everywhere (CharmNestRepoLock, the Settings unlock; the server checks it again for this very op). What the override sets aside is read meanwhile.
       2  one short list of what is set aside, and one button: "Approve for laser cutting anyway".
       3  the press: the set is in Laser cutting and its orders are off hold on the screen at once; the cloud does it behind it (op setOverride: all of it or nothing). Refused, everything is as it was,
          and why is said in red.
   Nothing is stored on the computer; the unlock ends with the press (or with Cancel), here and on the server. */
(function (root) {
  "use strict";
  const doc = root.document;
  const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const safe = (f, dflt) => { try { const v = f(); return v === undefined ? dflt : v; } catch (e) { console.warn("[override]", e); return dflt; } };
  const api = (...a) => ((root.CN && root.CN.api) || root.api)(...a);
  const plural = (n, w) => `${n} ${w}${n === 1 ? "" : "s"}`;
  const copy = o => (o && typeof o === "object" ? JSON.parse(JSON.stringify(o)) : o);
  const CSS = `
.btn.ovBtn{flex:0 0 auto;border-color:#b8902f;color:#6b4f0f;background:#fbf3df;font-weight:650}
.btn.ovBtn:hover{background:#f6e8c6}
.ovSet{margin-left:8px}.ovSet .btn.ovBtn{height:24px;padding:0 11px;border-radius:999px;font-size:11px}
dialog.ovDlg{width:min(620px,95vw)}
.ovDlg .dlgHead h3{font-size:24px;white-space:normal}
.ovDlg .dlgBody{font-size:16px;line-height:1.5}
.ovDlg .ovLead{margin:0 0 14px;color:var(--ink70)}
.ovDlg .ovList{list-style:none;margin:0;padding:0;display:grid;gap:10px;max-height:46vh;overflow:auto}
.ovDlg .ovList li{padding:13px 16px;border:1px solid var(--line);border-left:4px solid #b8902f;border-radius:10px;background:var(--card2)}
.ovDlg .ovList b{display:block;font:500 18px/1.3 var(--serif)}
.ovDlg .ovList span{display:block;margin-top:3px;color:var(--ink70);font-variant-numeric:tabular-nums;overflow-wrap:anywhere}
.ovDlg .dlgFoot{padding:14px 18px}
.ovDlg .dlgFoot .btn{font-size:15px;padding:11px 18px}`;
  function css() { if (doc.getElementById("cnOverrideCss")) return; const s = doc.createElement("style"); s.id = "cnOverrideCss"; s.textContent = CSS; doc.head.appendChild(s); }
  const button = () => { css(); return '<button type="button" class="btn ghost sm ovBtn" data-override>Override</button>'; };

  /** The list, from what the cloud answered ([] = nothing is set aside but the press itself). */
  function items(L) {
    const out = [], nums = (list, n) => list.slice(0, n).join(", ") + (list.length > n ? ` and ${list.length - n} more` : "");
    if (!L) return out;
    if (L.engravings && L.engravings.n) out.push([`${plural(L.engravings.n, "engraving")} not approved`, `${nums(L.engravings.orders, 12)} · ${L.engravings.n === 1 ? "it is" : "they are"} not cut`]);
    for (const s of L.sheets || []) out.push([s.label, s.why]);
    if ((L.waits || []).length) out.push([`${plural(L.waits.length, "order")} with a piece not ready`, nums(L.waits, 12)]);
    if ((L.holds || []).length) out.push([`${plural(L.holds.length, "order")} on hold, released`, nums(L.holds.map(h => h.orderId), 24)]);
    return out;
  }
  function listHtml(L, err) {
    if (err) return `<li><b>What it sets aside could not be read</b><span>${esc(err)}</span></li>`;
    if (!L) return `<li><b>Reading what it sets aside</b><span>The set, its sheets and its orders</span></li>`;
    const rows = items(L);
    return rows.length ? rows.map(([a, b]) => `<li><b>${esc(a)}</b><span>${esc(b)}</span></li>`).join("") : `<li><b>Nothing waits</b><span>Every sheet is ready and no order is on hold</span></li>`;
  }
  /** → true when the person pressed "Approve for laser cutting anyway". `state`: { look, err, changed } filled in by the read that began at the first press. */
  function ask(name, state) {
    return new Promise(resolve => {
      css();
      const d = doc.createElement("dialog"); d.className = "splitDlg ovDlg"; d.setAttribute("aria-labelledby", "ovT");
      d.innerHTML = `<div class="dlg"><div class="dlgHead"><h3 id="ovT">${esc(name)}</h3></div><div class="dlgBody"><p class="ovLead">Approving it now sets this aside:</p><ul class="ovList" aria-live="polite">${listHtml(state.look, state.err)}</ul></div>
        <div class="dlgFoot"><div class="left"><button type="button" class="btn ghost" data-k="no">Cancel</button></div><button type="button" class="btn gold" data-k="yes">Approve for laser cutting anyway</button></div></div>`;
      (doc.body || doc.documentElement).appendChild(d);
      let over = false;
      const end = yes => { if (over) return; over = true; state.changed = null; try { if (d.open) d.close(); } catch (_) { d.removeAttribute("open"); } d.remove(); resolve(yes); };
      state.changed = () => { const ul = d.querySelector(".ovList"); if (ul) ul.innerHTML = listHtml(state.look, state.err); };
      d.querySelector("[data-k=yes]").onclick = () => end(true);
      d.querySelector("[data-k=no]").onclick = () => end(false);
      d.addEventListener("cancel", e => { e.preventDefault(); end(false); });
      try { d.showModal(); } catch (_) { d.setAttribute("open", ""); }
      try { d.querySelector("[data-k=yes]").focus(); } catch (_) { /* a hidden tab */ }
    });
  }

  /* ── on the screen at once ── */
  const cardsOf = setId => [...doc.querySelectorAll(".setCard")].filter(c => c._laserSet && c._laserSet.setId === setId);
  const sheetsOf = setId => [...new Set(cardsOf(setId).flatMap(c => c._laserSheets || []))];
  const LR = () => root.LaserReview, R = () => root.CharmNestReadiness;
  const HOLD_FIELDS = ["state", "hold", "heldAt", "holdSeen", "reason", "changePending", "repoolChanged", "frontAt", "releaseHold", "releaseAnyway", "holdFromCloud", "_lp"];
  function redraw() {
    safe(() => LR() && LR().changed()); safe(() => root.CharmNestReviewMark && root.CharmNestReviewMark.sync && root.CharmNestReviewMark.sync());
    safe(() => root.Review && root.Review.syncOrderItems && root.Review.syncOrderItems()); safe(() => root.Orders && root.Orders.render && root.Orders.render());
    safe(() => root.LibraryDone && root.LibraryDone.refreshCards && root.LibraryDone.refreshCards(doc.getElementById("libBody")));
  }
  /** The set approved and its orders off hold, on this page; → what puts it all back. */
  function show(setId, who) {
    const at = Date.now(), mark = { at, by: who, setId }, ids = sheetsOf(setId), undo = [];
    const base = id => safe(() => LR().projected({ id }), null) || { id };
    const before = ids.map(id => { const p = base(id); return { id, kind: "sheet", patch: { laserOverride: p.laserOverride || null, laserHold: p.laserHold || null, processReady: !!p.processReady, draft: p.draft, releaseFull: p.releaseFull }, orders: R() ? R().orderIds(p) : [] }; });
    const sets = cardsOf(setId).map(c => [c, c._laserSet]);
    safe(() => LR().acceptProcess(before.map(b => ({ kind: "sheet", id: b.id, patch: { laserOverride: mark, laserHold: null, processReady: true, draft: false } }))));
    for (const [c, st] of sets) c._laserSet = Object.assign({}, st, { laserOverride: mark, processReady: true });
    undo.push(() => { safe(() => LR().acceptProcess(before.map(b => ({ kind: "sheet", id: b.id, patch: b.patch })))); for (const [c, st] of sets) c._laserSet = Object.assign({}, c._laserSet, { laserOverride: st.laserOverride || null, processReady: !!st.processReady }); });
    const own = new Set(before.flatMap(b => b.orders).map(String)), onSheets = new Set(ids.flatMap(id => base(id).poolIds || []).map(String));
    const held = (safe(() => root.Orders.rows(), []) || []).filter(r => r && r.order && own.has(String(r.order.receiptId)) && r.state !== "gone" && (r.hold || r.changePending));
    for (const r of held) {
      const was = {}; for (const f of HOLD_FIELDS) was[f] = copy(r[f]);
      if (r.state === "held") { const p = r.poolIds || []; r.state = !p.length ? "pulled" : p.every(x => onSheets.has(String(x))) ? "written" : "pooled"; r.reason = null; }
      r.hold = null; r.heldAt = null; r.holdSeen = false; r.changePending = false; r.frontAt = at; r.holdFromCloud = false;
      undo.push(() => { for (const f of HOLD_FIELDS) if (was[f] === undefined) delete r[f]; else r[f] = was[f]; });
    }
    const B = root.B, marks = (B && B.maps && B.maps.reviewMarks) || null;
    if (marks) {
      const next = Object.assign({}, marks); let n = 0;
      for (const o of own) { const m = marks[o + "_review"]; if (m && m.review && m.review.state === "open") { next[o + "_review"] = Object.assign({}, m, { review: Object.assign({}, m.review, { state: "done", doneAt: at, doneBy: who, how: "other" }) }); n++; } }
      if (n) { B.maps.reviewMarks = next; undo.push(() => { const cur = Object.assign({}, B.maps.reviewMarks); for (const o of own) { const m = marks[o + "_review"]; if (m && m.review && m.review.state === "open") cur[o + "_review"] = m; } B.maps.reviewMarks = cur; }); }
    }
    redraw();
    return () => { for (const f of undo.reverse()) safe(f); redraw(); };
  }
  /** What the cloud kept, taken onto this page. */
  function took(a) {
    safe(() => LR().acceptProcess((a.process || []).filter(p => p.kind === "sheet")));
    for (const c of cardsOf(a.setId)) c._laserSet = Object.assign({}, c._laserSet, { laserOverride: a.override, processReady: true });
    const lines = a.lines || {};
    for (const r of safe(() => root.Orders.rows(), []) || []) { const l = r && lines[r.key]; if (!l) continue; for (const f of HOLD_FIELDS) if (l[f] !== undefined) r[f] = copy(l[f]); r.holdFromCloud = false; }
    redraw(); safe(() => LR().nudge && LR().nudge()); safe(() => root.PlacementFeed && root.PlacementFeed.nudge && root.PlacementFeed.nudge()); safe(() => root.CharmNestRunFollow && root.CharmNestRunFollow.now && root.CharmNestRunFollow.now("override"));
  }
  const busy = new Set();
  async function press(o) {
    const setId = String((o && o.setId) || ""), name = (o && o.name) || "This set", L = root.CharmNestRepoLock, E = root.CNEmployee || {};
    if (!setId || busy.has(setId)) return false;
    if (!L) { safe(() => root.toast("The password check did not load: reload the page", "bad", 8000)); return false; }
    busy.add(setId);
    try {
      const who = (E.name && E.name()) || (E.need ? await E.need("Kept with this override.") : E.ask ? E.ask() : ""); if (!who) return false;
      const state = { look: null, err: "", changed: null };
      api("charmNestLibrary", { op: "setOverrideGet", setId }, { quiet: true }).then(r => { if (!r || r.error) throw new Error((r && r.error) || "no answer"); state.look = r; }).catch(e => { state.err = String((e && e.data && e.data.error) || (e && e.message) || e); }).then(() => { if (state.changed) safe(state.changed); });
      const info = await L.ensure("settings", { fresh: true, title: "Override · " + name, message: "Approve this set for laser cutting as it stands. Enter your password.", target: "override " + setId, go: "Continue" });
      if (!info) return false;
      const unlockEnds = () => safe(() => { L.lockNow(); });   // (the unlock is this press's alone: it ends here and on the server)
      if (!(await ask(name, state))) { unlockEnds(); return false; }
      const res = await root.CNInstant.press({ key: "override:" + setId, what: `${name} was not approved`, again: () => press(o),
        show: () => show(setId, who),
        send: () => api("charmNestLibrary", { op: "setOverride", setId, by: who, repoToken: info.token }, { quiet: true, timeoutMs: 60000 }),
        done: a => { unlockEnds(); took(a); const n = (a.released || []).length; safe(() => root.toast(`${name} approved for laser cutting${n ? ` · ${plural(n, "order")} released from hold` : ""}`, "ok", 8000)); },
        back: put => { unlockEnds(); if (put) put(); } }).done;
      return !!res.ok;
    } finally { busy.delete(setId); }
  }
  root.CharmNestSetOverride = { press, button, items };
})(typeof window !== "undefined" ? window : globalThis);
