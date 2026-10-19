/* Charm Nest · a charm dragged onto the next or previous sheet (10K and 14K solid gold only).
   Paul, 18 Oct 2026: "The user also needs the ability to manually move charms between sheets of the same kind. This needs to be a
   seamless and visually/animation rich experience where the user is allowed to drag and drop a given charm on the next or previous
   sheet. If there is not enough room on the given sheet that the user is trying to drop a charm onto, there must be a small pop-up
   where the user is informed that there's not enough space and that the charm was unable to be relocated to that particular sheet."

   Where it lives: the sheet window (charm-nest-sheetwin.js), while its Manual Control is on for a 10K or 14K sheet this sorter holds
   (window.CNManual.isOn(page); without that module, a small "Move between sheets" switch on the plate is the flag). Two docks stand at
   the plate's edges, the previous and the next sheet of the same metal and run (by page), each a small drawing of that sheet as it is
   now. A click on a dock opens that sheet. A charm picked up from the plate lifts off it (its spot left as a dashed outline), the docks
   open, and a dock held under the hand for a moment opens into a larger view of its sheet where the charm is drawn where it would go:
   sage where it fits, clay where it does not. Let go over the dock:
     · the room is found with the window's own collision grid (the solver's sheet grid: real outlines, the run's clearance), the drop
       point first, then near it, then anywhere free on the sheet; each pose is then confirmed by ONE fit test, fitOne() (CNManual.fits
       when it is on the page);
     · found: the charm flies on an arc into its spot on that sheet and lands with a ring; the move is the window's own verified,
       restart-safe moveIn (off this sheet, saved without it, onto the other pinned where it was put, saved, piece records, QR labels
       and sets, both sheets read back; the pool row says who moved it, which the order's timeline shows as "Moved to ...");
       MANUALHOP (Paul, 19 Oct 2026): with the sheet window's moveNow on the page (always, beside its Manual Control) the move is made at once on
       the live sheets and saved in the background (records, piece records, read-back; "Saving..." -> "Saved" by the plate); the two sheets'
       files are written as they stand when Manual Control is turned off (hopNow below);
     · not found: the charm flies back to its spot and a small pop-up says "Not enough space on 10K Draft 3 — the charm stayed on
       10K Draft 2". Nothing is written.
   Guards (refused with a plain sentence, the charm flies back, nothing is written): another metal is never offered (10K to 10K, 14K to
   14K only); neither sheet may be nesting, finishing, queued or saving, cut by the laser, carrying a green line (cut, or drawn and not
   cut: a move would make the line wrong, the same rule as Merge sheets), in a set (committed, sent or open: a sheet in a set keeps its
   charms), taken back from the Library, or of a run that is finishing or committing; the source is never emptied (Merge sheets is
   for that); the order must not be on hold, cancelled or in Review.
   Orders with several pieces (the owner's set rules, docs/charm-nest/set-rules.md R9 and R16): the unit that moves is every piece of the
   charm's order on this sheet, so an order never spreads over two sheets by a drag, and a pair is never split. Where the other sheet
   already holds that order, only the charm's own line (its pair) goes, which brings the order together. A move that would tie a sheet
   to one it shares no order with yet (the order also has pieces on a third sheet) is refused: sheets that share an order go into one set.
   The keyboard way: Alt + Shift + ← / → takes the selected charm the same way, aiming at the place it has now.
   Two computers: only the screen that holds the run (the sheets are live here) offers it; just before the move both saved records and
   the pieces' records are read from the cloud, and a piece another computer took off, held or moved meanwhile stops the move.

   Manual Control (window.CNManual, another module), read feature-detected and never required:
     isOn(page)                 page = the live sheet page object (allSheets(): metal, page, sheetId, charms, placements) the window shows
     fits(page, charm, pose)    page = the TARGET sheet's live page, charm = the live charm object (still on its own sheet), pose = { cxPt, cyPt,
                                angle } in sheet points (y down, as placements are) -> { ok, reason }; asked once per piece in fitOne()
     onToggle(fn)               fn() is called when it is switched; the docks are drawn again
     cancelDrag()  (optional)   told when its own drag of a charm inside the sheet is handed over to this one (the pointer left the plate or
                                reached a dock); it should put the charm back where it was and write nothing. Without it a synthetic
                                pointercancel is sent to the plate's canvas.
   Without CNManual, the switch on the plate is the flag and the window's collision grid is the verifier.

   window.SheetHop: { refresh(), state(), fitOne(target, charm, pose), hopSelected(side), hopPiece(page, charm, side, at), _H } */
(function () {
  "use strict";
  if (window.SheetHop) return;
  const doc = document;
  const METALS_HOP = ["gold10k", "gold14k"];
  const EASE = "cubic-bezier(.2,.8,.2,1)";
  const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const reduced = () => { try { return !!(window.Motion && Motion.reduced ? Motion.reduced() : matchMedia("(prefers-reduced-motion: reduce)").matches); } catch (_) { return false; } };
  const wait = ms => new Promise(r => setTimeout(r, ms));
  const tryDo = f => { try { return f(); } catch (_) { return null; } };
  const kit = () => (window.SheetWin && SheetWin.hopKit) || null;
  const Sv = () => window.CharmNestSolver || null;
  const pages = () => (typeof allSheets === "function" ? allSheets() : []);
  const actives = sh => (typeof activeCharms === "function" ? activeCharms(sh) : (sh.charms || []).filter(c => !c.excluded));
  const colorOf = m => ((typeof METALS !== "undefined" ? METALS : []).find(x => x.key === m) || {}).color || "#b08d2a";
  const lineOf = c => String(c.poolId || "").replace(/_\d+$/, "");
  // (Manual Control on: the charm in hand is its selection, and a charm it moved, not saved yet, is drawn where it was put: the hand lifts it from there)
  const selOf = K => (K.selected ? K.selected() : K.W.sel) || null;
  const shown = (c, p) => { const K = kit(), q = p && K && K.manualPose ? tryDo(() => K.manualPose(c.id)) : null; return q ? Object.assign({}, p, { cxPt: q.cxPt, cyPt: q.cyPt, angle: q.angle }) : p; };
  const SVG = {
    bad: '<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="7" fill="currentColor" opacity=".14"/><path d="M5.6 5.6l4.8 4.8M10.4 5.6l-4.8 4.8" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/></svg>',
    room: '<svg viewBox="0 0 16 16" aria-hidden="true"><rect x="2" y="3" width="12" height="10" rx="2" fill="none" stroke="currentColor" stroke-width="1.4"/><circle cx="6" cy="7" r="1.6" fill="currentColor"/><circle cx="10" cy="9" r="1.6" fill="currentColor"/><path d="M4 13 12 3" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"/></svg>',
    prev: '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M10 3 5 8l5 5" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></svg>',
    next: '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="m6 3 5 5-5 5" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></svg>',
    hop: '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M2.5 11.5c2-5 9-5 11 0" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/><path d="m11 9.3 2.6 2.3-.4-3.4" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/><circle cx="4.5" cy="5" r="1.7" fill="currentColor"/></svg>',
    x: '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M4 4l8 8M12 4l-8 8" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round"/></svg>'
  };

  // (MANUALHOP, 19 Oct 2026: a dock lets presses through to the plate, only its tab takes them: its card, still folding away after a drop, never covers the next charm)
  const CSS = `
.shpDock{position:absolute;top:50%;z-index:4;display:flex;align-items:center;gap:0;transform:translateY(-50%);transition:transform .32s ${EASE},opacity .22s ease;-webkit-user-select:none;user-select:none;touch-action:none;pointer-events:none}
.shpDock[data-side=prev]{left:6px;flex-direction:row}
.shpDock[data-side=next]{right:6px;flex-direction:row-reverse}
.shpDock.in{animation:shpIn .34s ${EASE} backwards}
.shpDock[data-side=next].in{animation-name:shpInR}
@keyframes shpIn{from{opacity:0;transform:translate(-18px,-50%)}}
@keyframes shpInR{from{opacity:0;transform:translate(18px,-50%)}}
.shpTab{pointer-events:auto;display:grid;justify-items:center;gap:4px;width:30px;padding:9px 0;border:1px solid var(--goldLine,#e2cf9f);border-radius:11px;background:color-mix(in srgb,var(--card) 92%,transparent);color:var(--ink70);box-shadow:0 6px 18px rgba(30,24,16,.12);cursor:pointer;-webkit-backdrop-filter:blur(8px);backdrop-filter:blur(8px);transition:background-color .15s,border-color .15s,box-shadow .2s,width .3s ${EASE}}
.shpTab svg{width:14px;height:14px}
.shpTab b{writing-mode:vertical-rl;transform:rotate(180deg);font:700 10px var(--mono);letter-spacing:.06em;color:var(--ink);white-space:nowrap}
.shpTab i{width:8px;height:8px;border-radius:50%;background:var(--c)}
.shpTab:hover,.shpTab:focus-visible{background:var(--card);border-color:var(--gold2,#caa861);outline:none;box-shadow:0 8px 22px rgba(169,130,63,.22)}
.shpCard{display:grid;gap:5px;width:0;opacity:0;overflow:hidden;margin:0 6px;padding:0;border:1.5px dashed var(--goldLine,#e2cf9f);border-radius:13px;background:color-mix(in srgb,var(--card) 95%,transparent);box-shadow:0 18px 44px rgba(30,24,16,.2),0 2px 6px rgba(30,24,16,.08);pointer-events:none;transition:width .34s ${EASE},opacity .2s ease,padding .34s ${EASE},border-color .18s,background-color .18s,box-shadow .2s,transform .2s ${EASE};-webkit-backdrop-filter:blur(12px);backdrop-filter:blur(12px)}
.shpDock.open .shpCard{width:var(--w,170px);opacity:1;padding:9px 10px 10px}
.shpDock.peek .shpCard{--w:var(--pw,420px)}
.shpCard canvas{display:block;width:100%;border-radius:6px;background:#fffefb;box-shadow:inset 0 0 0 1px rgba(176,86,63,.5)}
.shpName{display:flex;align-items:center;gap:6px;min-width:0;font:650 12px var(--sans);color:var(--ink);white-space:nowrap}
.shpName i{flex:0 0 8px;width:8px;height:8px;border-radius:50%;background:var(--c)}
.shpName b{overflow:hidden;text-overflow:ellipsis;font-weight:650}
.shpName span{margin-left:auto;font:10.5px var(--mono);color:var(--ink45)}
.shpSub{font:11px var(--sans);color:var(--ink45);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;min-height:14px}
.shpDock[data-state=armed] .shpCard{border-style:solid;border-color:var(--gold,#a9823f);background:var(--goldSoft,#f6eedb);transform:scale(1.03);box-shadow:0 14px 34px rgba(169,130,63,.32)}
.shpDock[data-state=armed] .shpSub{color:var(--gold,#a9823f)}
.shpDock[data-state=dim] .shpCard{opacity:.5;border-color:var(--line)}
.shpDock[data-state=dim] .shpCard canvas{filter:grayscale(.6)}
.shpDock[data-state=dim][data-hot] .shpCard{opacity:1;border-style:solid;border-color:var(--clay,#b0563f);background:var(--claySoft,#f6e3dc)}
.shpDock[data-state=dim][data-hot] .shpSub{color:#8a3a26;white-space:normal}
.shpDock.got .shpCard{animation:shpGot 1.2s ease-out 1}
@keyframes shpGot{0%{box-shadow:0 0 0 0 rgba(95,122,91,.55),0 18px 44px rgba(30,24,16,.2)}60%{box-shadow:0 0 0 9px rgba(95,122,91,0),0 18px 44px rgba(30,24,16,.2)}}
.shpDock.nope .shpCard,.shpDock.nope .shpTab{animation:shpNope .42s ease 1}
@keyframes shpNope{20%{transform:translateX(-5px)}45%{transform:translateX(4px)}70%{transform:translateX(-2px)}}
.shpSwitch{position:absolute;top:10px;right:14px;z-index:4;display:inline-flex;align-items:center;gap:6px;border:1px solid var(--line);background:color-mix(in srgb,var(--card) 92%,transparent);border-radius:999px;padding:4px 11px 4px 8px;font:600 11px var(--sans);color:var(--ink70);cursor:pointer;box-shadow:0 4px 14px rgba(30,24,16,.08);-webkit-backdrop-filter:blur(8px);backdrop-filter:blur(8px)}
.shpSwitch svg{width:15px;height:15px}
.shpSwitch[aria-pressed=true]{background:var(--goldSoft,#f6eedb);border-color:var(--gold2,#caa861);color:#6b4f1d}
.shpSwitch:focus-visible{outline:2px solid var(--gold2,#caa861);outline-offset:2px}
.shpFx{position:absolute;inset:0;width:100%;height:100%;pointer-events:none;z-index:2}
.shpLift{position:fixed;left:0;top:0;pointer-events:none;will-change:transform;z-index:3;filter:drop-shadow(0 14px 18px rgba(30,24,16,.28)) drop-shadow(0 2px 4px rgba(30,24,16,.16))}
.shpLift b{position:absolute;right:-8px;top:-8px;min-width:18px;height:18px;padding:0 5px;border-radius:9px;background:var(--gold2,#caa861);color:#2a2013;font:700 10px/18px var(--mono);text-align:center;box-shadow:0 0 0 2px var(--card)}
.shpLift.think::after{content:"";position:absolute;inset:-6px;border-radius:50%;border:2px solid var(--gold2,#caa861);border-top-color:transparent;animation:shpSpin .8s linear infinite}
@keyframes shpSpin{to{transform:rotate(360deg)}}
.shpPop{position:fixed;z-index:306;display:flex;align-items:flex-start;gap:9px;max-width:min(340px,calc(100vw - 24px));padding:10px 8px 11px 12px;border:1px solid #e7b9aa;border-left:3px solid var(--clay,#b0563f);border-radius:12px;background:var(--card);box-shadow:0 18px 44px rgba(30,24,16,.22),0 2px 6px rgba(30,24,16,.08);color:#8a3a26;font:12.5px/1.4 var(--sans);transform-origin:var(--ox,50%) 0;animation:shpPop .34s ${EASE} both}
.shpPop>svg{flex:0 0 16px;width:16px;height:16px;margin-top:1px;color:var(--clay,#b0563f)}
.shpPop .t{flex:1 1 auto;min-width:0}.shpPop .t b{display:block;font-weight:650}.shpPop .t small{display:block;color:var(--ink45);font-size:11.5px;margin-top:2px}
.shpPop button{flex:0 0 auto;width:22px;height:22px;display:grid;place-items:center;border:0;border-radius:50%;background:transparent;color:var(--ink45);cursor:pointer}
.shpPop button svg{width:12px;height:12px}.shpPop button:hover{background:var(--paper2);color:var(--ink)}
.shpPop.out{animation:shpPopOut .26s ease-in both}
@keyframes shpPop{from{opacity:0;transform:translateY(-6px) scale(.94)}}
@keyframes shpPopOut{to{opacity:0;transform:translateY(-4px) scale(.97)}}
@media (max-width:980px){.shpDock.peek .shpCard{--w:min(62vw,var(--pw,420px))}}
@media (prefers-reduced-motion:reduce){.shpDock,.shpCard,.shpTab{transition:none!important}.shpDock.in,.shpDock.got .shpCard,.shpDock.nope .shpCard,.shpDock.nope .shpTab,.shpPop,.shpPop.out{animation:none!important}}
`;
  function style() { if (doc.getElementById("shpStyle")) return; const s = doc.createElement("style"); s.id = "shpStyle"; s.textContent = CSS; doc.head.appendChild(s); }

  /* ── state ── */
  const H = { local: false, docks: { prev: null, next: null }, sig: "", fx: null, sw: null, drag: null, pending: null, busy: null, pop: null, landing: new Map(), noClick: 0, hooked: false, toggleHooked: false, plate: null };

  /* ── Manual Control (the other module's), feature-detected; this file's own small flag stands in for it ── */
  const CNM = () => { const m = window.CNManual; return m && typeof m.isOn === "function" ? m : null; };
  function manualOn(pg) {
    const m = CNM(); if (m) { try { return !!m.isOn(pg); } catch (_) { return false; } }
    return H.local;
  }
  /** The ONE fit test of a pose on a sheet, {ok, reason}: Manual Control's verifier when it is on the page (CNManual.fits), else the window's
   *  collision grid (the solver's sheet grid with the sheet's charms stamped at the run's clearance, the charm's own outline bitmap). */
  function fitOne(target, c, pose, G) {
    const m = window.CNManual;
    if (m && typeof m.fits === "function") {
      try { const r = m.fits(target, c, pose); return r && typeof r === "object" ? { ok: !!r.ok, reason: r.reason || "" } : { ok: !!r, reason: "" }; }
      catch (e) { return { ok: false, reason: e.message || "the fit test failed" }; }
    }
    const K = kit(); if (!K) return { ok: false, reason: "the sheet window is not ready" };
    try {
      G = G || K.plateGrid(target);
      const v = K.variantOf(c, ((+pose.angle || 0) % 360 + 360) % 360, G.clearancePt); if (!v) return { ok: false, reason: "its outline is not known here" };
      const x = Math.round(pose.cxPt * 2 - v.solid.cx), y = Math.round(pose.cyPt * 2 - v.solid.cy);
      return G.grid.fits(v.fine.pm, x, y) ? { ok: true, reason: "" } : { ok: false, reason: "it would touch a charm or the sheet's edge" };
    } catch (e) { return { ok: false, reason: e.message || "the fit test failed" }; }
  }

  /* ── which sheets ── */
  /** The sheet the window shows, when it is a 10K or 14K sheet this sorter holds (live), else null. */
  function here() {
    const K = kit(); if (!K || !window.SheetWin || !SheetWin.isOpen()) return null;
    const W = K.W, pg = pages().find(p => p.sheetId && p.sheetId === W.id) || null;
    return pg && METALS_HOP.includes(pg.metal) ? pg : null;
  }
  /** The previous and next sheet of the same metal and run, by page (only sheets with something on them, saved). */
  function neighbours(pg) {
    const list = pages().filter(p => p.metal === pg.metal && p.sheetId && (p.runId || null) === (pg.runId || null) && (p.placements || []).length).sort((a, b) => (+a.page || 0) - (+b.page || 0));
    const i = list.indexOf(pg);
    return { prev: i > 0 ? list[i - 1] : null, next: i >= 0 && i < list.length - 1 ? list[i + 1] : null };
  }
  const word = sh => { const K = kit(); return sh ? (K ? K.word(sh) : window.CharmNestSheetName ? CharmNestSheetName.name(sh) : `Sheet ${sh.page}`) : ""; };
  const setWord = sh => { try { const s = window.Sets && Sets.ofRun(sh.runId).find(x => x.setId === sh.setId); return s && s.name ? s.name.replace(/^Set-/, "Set ") : "a set"; } catch (_) { return "a set"; } };
  /** Why a sheet may not give or take a charm now ("" to go). */
  function sheetRefusal(sh) {
    const K = kit(), n = word(sh);
    if (!sh || !sh.sheetId || !pages().includes(sh)) return `${n || "That sheet"} is not held by this sorter now`;
    if (!METALS_HOP.includes(sh.metal)) return `${n} is not a 10K or 14K sheet`;
    if (+sh.laserDoneAt > 0) return `${n} has been cut by the laser`;
    if (sh.roseCutAt) return `${n} has its green line cut: its charms stay where they are`;
    if (sh.rosePlan || sh.roseProtected) return `${n} has its green dash line drawn: moving a charm would make the line wrong`;
    if (sh.recalled) return `${n} was taken back from the Library: it keeps its charms`;
    if (K && K.sentToStation(sh)) return `${n} is in a set sent to laser cutting`;
    if (window.Gate && tryDo(() => (Gate.committedSheet && Gate.committedSheet(sh)) || (Gate.fixedSet && Gate.fixedSet(sh)))) return `${n} is in a committed set`;
    if (sh.setId && !sh.draft) return `${n} is in ${setWord(sh)}: a sheet in a set keeps its charms where they are`;
    if (K && K.sheetWorking(sh)) return `${n} is being nested or saved right now; try again once it has saved`;
    const run = window.B && B.run;
    if (run && sh.runId === run.runId && ["complete", "abandoned"].includes(run.status)) return `The run of ${n} is finished`;
    return "";
  }
  /** Every sheet that holds a piece of the order (this sorter's sheets, and the pieces' records for the rest), by sheet id. */
  function holdersOf(rid, skip) {
    const K = kit(), out = new Map();
    for (const sh of pages()) if (sh.sheetId && (sh.charms || []).some(c => !skip.has(c.poolId) && K.ridOf(c) === rid)) out.set(sh.sheetId, word(sh));
    try { for (const p of (window.B && B.pool && B.pool.rows ? B.pool.rows.values() : [])) if (String(p.orderId) === rid && p.sheetId && !skip.has(p.poolId) && !["abandoned", "superseded"].includes(p.state) && !out.has(p.sheetId)) out.set(p.sheetId, p.sheetName || "another sheet"); } catch (_) {}
    return out;
  }
  /** What moves when charm c of sheet src goes to target: { pieces: [{c, src, placed, p}], rid } or { why }. */
  function unitOf(src, c, target) {
    const K = kit(), rid = K.ridOf(c);
    if (!c || !c.poolId || !/^\d+$/.test(rid)) return { why: "This charm is not a piece of an order, so it stays where it is" };
    const mine = actives(src).filter(z => z.poolId && K.ridOf(z) === rid);
    const onTarget = target ? (target.charms || []).some(z => K.ridOf(z) === rid) : false;
    // an order already on the other sheet: only this charm's line (a pair moves whole) goes, which brings the order together
    const list = onTarget ? mine.filter(z => lineOf(z) === lineOf(c)) : mine;
    const byId = new Map((src.placements || []).map(p => [p.id, p]));
    const pieces = [c, ...list.filter(z => z !== c)].map(z => ({ c: z, src, placed: byId.has(z.id), p: byId.has(z.id) ? shown(z, byId.get(z.id)) : null }));
    return { rid, pieces, whole: !onTarget && pieces.length > 1, lineOnly: onTarget && pieces.length < mine.length };
  }
  /** Why this unit may not go from src to target ("" to go): the order's own state, the source never emptied, and the set rule. */
  function unitRefusal(u, src, target) {
    if (u.why) return u.why;
    const K = kit(), rid = u.rid, n = word(src), t = word(target);
    if (K.cancelled(rid)) return `Order ${rid} is cancelled`;
    if (K.rowsOfOrder(rid).some(r => r.hold || ["held", "skipped", "gone"].includes(r.state))) return `Order ${rid} is on hold`;
    if (tryDo(() => window.CharmNestReviewHold && CharmNestReviewHold.inReview(rid))) return `Order ${rid} is paused in Review`;
    if (actives(src).length <= u.pieces.length) return `${n} would be left empty: a sheet is never emptied by a move (Merge sheets, in its Options, moves everything)`;
    // the cardinal rule: a move never ties a sheet to one it shares no order with yet
    const ids = new Set(u.pieces.map(z => z.c.poolId)), others = holdersOf(rid, ids);
    const before = new Set([...others.keys(), src.sheetId]), after = new Set([...others.keys(), target.sheetId]);
    if (after.size > 1 && [...after].some(id => !before.has(id))) {
      const far = [...others].filter(([id]) => id !== src.sheetId && id !== target.sheetId).map(([, nm]) => nm);
      if (far.length) return `Order ${rid} also has pieces on ${far.join(", ")}: moving it to ${t} would tie ${t} to ${far.length === 1 ? "that sheet" : "those sheets"} (sheets that share an order go into one set). Bring the order together on one sheet first`;
    }
    return "";
  }

  /* ── the room: the drop point first, then near it, then anywhere free ── */
  const norm = a => ((Math.round(+a || 0) % 360) + 360) % 360;
  const angDist = (a, b) => { const d = Math.abs(norm(a) - norm(b)) % 360; return Math.min(d, 360 - d); };
  async function findRoom(target, u, drop, breathe) {
    const K = kit(), S2 = Sv(); if (!K || !S2) return null;
    const G = K.plateGrid(target), grid = G.grid.clone(); grid.fineRes = 2; if (grid.buildSAT) grid.buildSAT();
    const spots = []; let anchor = drop;
    for (let i = 0; i < u.pieces.length; i++) {
      const z = u.pieces[i], a0 = norm(z.p ? z.p.angle : 0);
      const angles = [...new Set([a0, ...G.angles.map(norm)])].sort((a, b) => angDist(a, a0) - angDist(b, a0) || a - b);
      let best = null;
      // 1 · at the point it was let go (or beside the order's first piece), its own angle first, then the nearest angles; 2 · a little further
      if (anchor) {
        for (const [R, n] of [[3, angles.length], [10, 12], [26, 6]]) {
          for (const ang of angles.slice(0, n)) {
            const v = K.variantOf(z.c, ang, G.clearancePt); if (!v) continue;
            const r = S2.tryPlaceTight(grid, v, anchor.x, anchor.y, R);
            if (r && r.ok) { const cx = (r.x + v.solid.cx) / 2, cy = (r.y + v.solid.cy) / 2, d = Math.hypot(cx - anchor.x, cy - anchor.y) + angDist(ang, a0) * 0.02; if (!best || d < best.d) best = { v, x: r.x, y: r.y, cxPt: cx, cyPt: cy, d, how: R === 3 ? "drop" : "near" }; }
            await breathe();   // (MANUALHOP: a breath between angles too: a charm's first turns are each made here, and 36 of them in one go held the page ~90 ms)
          }
          await breathe();
          if (best) break;
        }
      }
      // 3 · anywhere free on the sheet, the tightest spot the nest's own measurement finds
      if (!best) {
        for (const ang of angles) {
          const v = K.variantOf(z.c, ang, G.clearancePt); if (!v) continue;
          const r = S2.bestSpots(grid, [v], 1)[0];
          if (r && (!best || (r.contact || 0) > (best.contact || 0) + 1e-6)) best = { v, x: r.x, y: r.y, cxPt: r.cxPt, cyPt: r.cyPt, contact: r.contact || 0, how: "free" };
          await breathe();
        }
      }
      if (!best) return null;
      S2.stampVariant(grid, best.v, best.x, best.y);
      spots.push({ c: z.c, src: z.src, placed: z.placed, cxPt: best.cxPt, cyPt: best.cyPt, angle: best.v.angle, how: best.how });
      if (i === 0) anchor = { x: best.cxPt, y: best.cyPt };
    }
    // every pose confirmed by the one fit test, against the sheet as it stands (CNManual's verifier when it is here)
    for (const s of spots) { const f = fitOne(target, s.c, { cxPt: s.cxPt, cyPt: s.cyPt, angle: s.angle }, G); if (!f.ok) return null; await breathe(); }
    return spots;
  }

  /* ── drawing ── */
  function charmPath(ctx, c, k, fallbackR) {
    ctx.beginPath();
    if (c && c.outline && window.CharmNestPDF && CharmNestPDF.pathToCanvas) { const cx = c.centerPt[0], cy = c.centerPt[1]; CharmNestPDF.pathToCanvas(ctx, c.outline, (x, y) => [(x - cx) * k, (cy - y) * k]); return; }
    ctx.arc(0, 0, fallbackR * k, 0, Math.PI * 2);
  }
  const radiusOf = (c, p) => Math.max(+(c && c.widthPt) || +(p && p.wPt) || 14, +(c && c.heightPt) || +(p && p.hPt) || 14) / 2;
  /** A small drawing of a live sheet: the stock, its charms in their metal, the ones landing in sage, a ghost where the held charm would go. */
  function paintThumb(dk) {
    const cv = dk.cv, sh = dk.sh; if (!cv || !sh) return;
    const st = typeof stockFor === "function" ? stockFor(sh.metal, sh) : { wPt: 283, hPt: 142 };
    const cssW = Math.max(60, Math.round(dk.cardW - 20)), cssH = Math.round(cssW * st.hPt / st.wPt), dpr = Math.min(2.5, devicePixelRatio || 1);
    if (cv.width !== Math.round(cssW * dpr) || cv.height !== Math.round(cssH * dpr)) { cv.width = Math.round(cssW * dpr); cv.height = Math.round(cssH * dpr); }
    cv.style.aspectRatio = `${st.wPt} / ${st.hPt}`; dk.st = st;
    const ctx = cv.getContext("2d"), k = cv.width / st.wPt;
    ctx.setTransform(1, 0, 0, 1, 0, 0); ctx.clearRect(0, 0, cv.width, cv.height); ctx.fillStyle = "#fffefb"; ctx.fillRect(0, 0, cv.width, cv.height);
    const col = colorOf(sh.metal), byId = new Map((sh.charms || []).map(c => [c.id, c])), landing = H.landing.get(sh.sheetId);
    const one = (c, p, fill, stroke, lw, dash) => { ctx.save(); ctx.translate(p.cxPt * k, p.cyPt * k); ctx.rotate((+p.angle || 0) * Math.PI / 180); charmPath(ctx, c, k, radiusOf(c, p)); ctx.fillStyle = fill; ctx.fill("evenodd"); if (dash) ctx.setLineDash(dash); ctx.strokeStyle = stroke; ctx.lineWidth = lw; ctx.stroke(); ctx.restore(); };
    for (const p of sh.placements || []) { const c = byId.get(p.id); if (!c || (landing && landing.ids.has(c.poolId))) continue; one(c, p, col + "9a", "rgba(70,52,20,.55)", Math.max(.8, .6 * dpr)); }
    if (landing) for (const s of landing.spots) one(s.c, s, "rgba(95,122,91,.5)", "#5f7a5b", 1.4 * dpr, landing.saving ? [3 * dpr, 2.5 * dpr] : null);
    const g = dk.ghost; if (g) for (const s of g.spots) one(s.c, s, g.ok ? "rgba(95,122,91,.28)" : "rgba(176,86,63,.26)", g.ok ? "#5f7a5b" : "#b0563f", 1.6 * dpr, [4 * dpr, 3 * dpr]);
  }
  /** The plate's overlay: the spots the held charms left (paper over them, a dashed outline breathing), drawn above the window's own layers. */
  function paintPlateFx(now) {
    const K = kit(), cv = H.fx; if (!K || !cv) return;
    const W = K.W, fxCv = W.el.fx; if (!fxCv) return;
    if (cv.width !== fxCv.width || cv.height !== fxCv.height) { cv.width = fxCv.width; cv.height = fxCv.height; }
    const ctx = cv.getContext("2d"); ctx.setTransform(1, 0, 0, 1, 0, 0); ctx.clearRect(0, 0, cv.width, cv.height);
    const d = H.drag || H.busy; if (!d || !W.st || d.sheetId !== W.id) return;
    const back = K.back(), k = W.k, a = .55 + .35 * Math.sin((now || performance.now()) / 260);
    for (const z of d.unit.pieces) {
      const p = z.p; if (!p) continue;
      ctx.save(); ctx.translate(W.R + (back ? W.st.wPt - p.cxPt : p.cxPt) * k, W.R + p.cyPt * k); if (back) ctx.scale(-1, 1); ctx.rotate((+p.angle || 0) * Math.PI / 180);
      charmPath(ctx, z.c, k, radiusOf(z.c, p)); ctx.fillStyle = "#fffefb"; ctx.lineWidth = 3 * W.dpr; ctx.strokeStyle = "#fffefb"; ctx.fill("evenodd"); ctx.stroke();
      ctx.setLineDash([4 * W.dpr, 3 * W.dpr]); ctx.lineWidth = 1.3 * W.dpr; ctx.strokeStyle = d.nope ? `rgba(176,86,63,${a})` : `rgba(169,130,63,${a})`; ctx.stroke();
      ctx.restore();
    }
  }
  function fxLoop() {
    if (H.fxRaf) return;
    const step = now => { H.fxRaf = 0; paintPlateFx(now); if (H.drag || H.busy) H.fxRaf = requestAnimationFrame(step); };
    H.fxRaf = requestAnimationFrame(step);
  }

  /* ── the docks, the switch, the overlay ── */
  function ensureMounts() {
    const K = kit(); if (!K || !K.W.el || !K.W.el.plateBox) return false;
    style();
    const E = K.W.el;
    if (!H.fx || !H.fx.isConnected) { H.fx = doc.createElement("canvas"); H.fx.className = "shpFx"; H.fx.setAttribute("aria-hidden", "true"); E.plate.appendChild(H.fx); }
    if (!H.hooked) hookPointer(E);
    return true;
  }
  function renderSwitch(pg) {
    const K = kit(), E = K.W.el, want = !!pg && !CNM();
    if (!want) { if (H.sw) { H.sw.remove(); H.sw = null; } return; }
    if (!H.sw || !H.sw.isConnected) {
      H.sw = doc.createElement("button"); H.sw.type = "button"; H.sw.className = "shpSwitch"; H.sw.dataset.hop = "switch";
      H.sw.innerHTML = `${SVG.hop}<span>Move between sheets</span>`;
      H.sw.title = "Drag a charm onto the previous or next sheet of this metal";
      H.sw.onclick = () => { H.local = !H.local; refresh(true); };
      E.plateBox.appendChild(H.sw);
    }
    H.sw.setAttribute("aria-pressed", String(!!H.local));
  }
  function dockSig(sh) { return sh ? [sh.sheetId, sh.page, (sh.placements || []).length, (sh.charms || []).length, sh.status, sh.setId || "", sh.draft ? 1 : 0, sh.roseCutAt ? 1 : 0, sh.rosePlan ? 1 : 0, (sh.placements || []).map(p => p.id).join(",").length].join(":") : "-"; }
  function makeDock(side, sh) {
    const K = kit(), E = K.W.el, el = doc.createElement("div");
    el.className = "shpDock in"; el.dataset.side = side; el.dataset.sheet = sh.sheetId; el.dataset.state = "idle";
    el.style.setProperty("--c", colorOf(sh.metal));
    const name = word(sh), code = (name.match(/^(\S+)/) || [])[1] || "", no = (name.match(/(\d+)\s*$/) || [])[1] || "";
    el.innerHTML = `<button type="button" class="shpTab" data-hop="tab" aria-label="${side === "prev" ? "Previous" : "Next"} sheet: ${esc(name)}" title="${esc(name)} · open it, or drop a charm here to move it there">${side === "prev" ? SVG.prev : SVG.next}<i></i><b>${esc(code + " " + no)}</b></button>` +
      `<div class="shpCard" aria-hidden="true"><div class="shpName"><i></i><b></b><span></span></div><canvas></canvas><div class="shpSub"></div></div>`;
    const dk = { side, sh, el, cv: el.querySelector("canvas"), name, cardW: 170, k: 1, st: null, ghost: null, why: "" };
    el.querySelector("[data-hop=tab]").onclick = () => { if (H.drag || H.busy) return; K.switchSheet(sh.sheetId); };
    el.addEventListener("animationend", e => { if (e.animationName === "shpIn" || e.animationName === "shpInR") el.classList.remove("in"); if (e.animationName === "shpGot") el.classList.remove("got"); if (e.animationName === "shpNope") el.classList.remove("nope"); });
    E.plateBox.appendChild(el);
    fillDock(dk);
    return dk;
  }
  function fillDock(dk) {
    const sh = dk.sh, q = s => dk.el.querySelector(s);
    q(".shpName b").textContent = dk.name;
    const n = (sh.placements || []).length;
    q(".shpName span").textContent = `${n} charm${n === 1 ? "" : "s"}`;
    q(".shpSub").textContent = dk.why || (dk.ghost ? (dk.ghost.ok ? "Fits here · let go to move it" : "No room just here · let go to look for room") : (sh.density ? `${Math.round(sh.density * 100)}% full` : "Drop a charm here"));
    paintThumb(dk);
  }
  /** Draw the docks and the switch for the sheet the window shows (only when something they show changed). */
  function refresh(force) {
    const K = kit(); if (!K || !ensureMounts()) return;
    const pg = here(), on = !!pg && manualOn(pg), nb = on ? neighbours(pg) : { prev: null, next: null };
    renderSwitch(pg);
    const sig = [K.W.id, on ? 1 : 0, dockSig(nb.prev), dockSig(nb.next)].join("|");
    if (!force && sig === H.sig) return;
    H.sig = sig;
    for (const side of ["prev", "next"]) {
      const sh = nb[side], dk = H.docks[side];
      if (dk && (!sh || dk.sh !== sh)) { dk.el.remove(); H.docks[side] = null; }
      if (sh && !H.docks[side]) H.docks[side] = makeDock(side, sh);
      else if (sh && H.docks[side]) { H.docks[side].name = word(sh); fillDock(H.docks[side]); }
    }
    if (!on && H.drag) abort(H.drag);
  }
  function clearDocks() { for (const side of ["prev", "next"]) { const dk = H.docks[side]; if (dk) dk.el.remove(); H.docks[side] = null; } H.sig = ""; }

  /* ── the hand: press on a charm, past 5 px it lifts; with Manual Control's own drag on the page, once the pointer leaves the plate ── */
  function hookPointer(E) {
    H.hooked = true; H.plate = E.plate;
    E.plate.addEventListener("pointerdown", onDown, true);
    addEventListener("pointermove", onMove, true);
    addEventListener("pointerup", onUp, true);
    addEventListener("pointercancel", onCancel, true);
    // the click that ends a drag is eaten (the hand was captured by the window, so the click lands on the dialog itself, which would read it as a
    // click on its backdrop and close): the next click after a drag, not a time, so a busy computer cannot let it through
    addEventListener("pointerdown", () => { H.eat = false; }, true);
    addEventListener("click", e => { if (H.eat && performance.now() < H.noClick && e.target instanceof Element && e.target.closest("dialog.sheetWin")) { H.eat = false; e.stopImmediatePropagation(); e.preventDefault(); } }, true);
    addEventListener("keydown", onKey, true);
  }
  function onDown(e) {
    const K = kit(); if (!K || H.drag || H.busy || !e.isPrimary || (e.pointerType === "mouse" && e.button !== 0)) return;
    const pg = here(); if (!pg || !manualOn(pg) || K.W.hand || K.W.add) return;
    if (!(e.target instanceof Element) || !e.target.closest("canvas")) return;
    const pt = K.toPlate(e), x = K.hitPt(pt.x, pt.y); if (!x || !x.poolId) return;
    const c = actives(pg).find(z => z.poolId === x.poolId); if (!c) return;
    H.pending = { pid: e.pointerId, x0: e.clientX, y0: e.clientY, pg, c, x, type: e.pointerType };
  }
  function outsidePlate(e) { const r = H.plate.getBoundingClientRect(); return e.clientX < r.left - 8 || e.clientX > r.right + 8 || e.clientY < r.top - 8 || e.clientY > r.bottom + 8; }
  function onMove(e) {
    const p = H.pending;
    if (p && e.pointerId === p.pid) {
      const far = Math.hypot(e.clientX - p.x0, e.clientY - p.y0) > (p.type === "touch" ? 8 : 5);
      // (Manual Control moves a charm inside its sheet itself: the hand is taken over only once it leaves the plate, or reaches a dock)
      const take = CNM() ? (outsidePlate(e) || !!dockAt(e.clientX, e.clientY)) : far;
      if (take) { H.pending = null; begin(p, e.clientX, e.clientY); }
      return;
    }
    const d = H.drag; if (d && e.pointerId === d.pid) { d.x = e.clientX; d.y = e.clientY; e.stopImmediatePropagation(); if (e.cancelable) e.preventDefault(); }
  }
  function onUp(e) {
    if (H.pending && e.pointerId === H.pending.pid) { H.pending = null; return; }
    const d = H.drag; if (d && e.pointerId === d.pid) { d.x = e.clientX; d.y = e.clientY; e.stopImmediatePropagation(); drop(d); }
  }
  function onCancel(e) {
    if (H.pending && e.pointerId === H.pending.pid) { H.pending = null; return; }
    const d = H.drag; if (d && e.pointerId === d.pid && !d.handing) abort(d);
  }
  function onKey(e) {
    if (e.key === "Escape" && H.drag) { e.preventDefault(); e.stopImmediatePropagation(); abort(H.drag); return; }
    // the keyboard way: Alt + Shift + ← / → moves the selected charm to the previous / next sheet, at the same place (Alt + ← alone is the browser's Back)
    if (e.altKey && e.shiftKey && (e.key === "ArrowLeft" || e.key === "ArrowRight") && !(e.target instanceof Element && e.target.closest("input,textarea,[contenteditable]"))) {
      const K = kit(), pg = here(); if (!K || !pg || !manualOn(pg) || !selOf(K) || H.drag || H.busy) return;
      e.preventDefault(); e.stopImmediatePropagation(); hopSelected(e.key === "ArrowLeft" ? "prev" : "next");
    }
  }
  /** The copy of a charm that the hand carries: drawn at the plate's scale, angle 0 (the element turns, so a flight can turn it). */
  function makeLift(K, z, extra) {
    const W = K.W, kc = W.k / W.dpr, dpr = W.dpr, r = radiusOf(z.c, z.p) * 1.2, size = Math.ceil(2 * r * kc + 12);
    const el = doc.createElement("div"); el.className = "shpLift"; el.setAttribute("aria-hidden", "true");
    Object.assign(el.style, { width: size + "px", height: size + "px" });
    const cv = doc.createElement("canvas"); cv.width = Math.round(size * dpr); cv.height = Math.round(size * dpr); Object.assign(cv.style, { width: size + "px", height: size + "px", display: "block" });
    const ctx = cv.getContext("2d"), k = kc * dpr; ctx.translate(cv.width / 2, cv.height / 2);
    charmPath(ctx, z.c, k, radiusOf(z.c, z.p)); ctx.fillStyle = colorOf(z.src.metal) + "e6"; ctx.fill("evenodd"); ctx.strokeStyle = "rgba(70,52,20,.75)"; ctx.lineWidth = 1.2 * dpr; ctx.stroke();
    try { if (z.c.outline && window.CharmNestPDF && CharmNestPDF.drawCharm) { const cx = z.c.centerPt[0], cy = z.c.centerPt[1]; CharmNestPDF.drawCharm(ctx, z.c, (x, y) => [(x - cx) * k, (cy - y) * k], k); } } catch (_) {}
    el.appendChild(cv);
    if (extra) { const b = doc.createElement("b"); b.textContent = "+" + extra; el.appendChild(b); }
    const layer = window.Motion && Motion.layer ? Motion.layer(W.el.plate) : W.dlg;
    layer.appendChild(el);
    const L = { el, size, x: 0, y: 0, rot: 0, s: 1, mirror: K.back() ? -1 : 1 };
    L.place = (x, y, rot, s, m) => { L.x = x; L.y = y; L.rot = rot; L.s = s; if (m != null) L.mirror = m; el.style.transform = tf(L, x, y, rot, s, L.mirror); };
    return L;
  }
  const tf = (L, x, y, rot, s, m) => `translate3d(${x - L.size / 2}px,${y - L.size / 2}px,0) rotate(${rot}deg) scale(${s * (m || 1)},${s})`;
  /** Where a pose of the window's sheet is on the screen (its centre, css px), and the plate's scale. */
  function plateAt(K, p) {
    const W = K.W, r = W.el.fx.getBoundingClientRect(), kc = W.k / W.dpr, R = W.R / W.dpr, back = K.back();
    return { x: r.left + R + (back ? W.st.wPt - p.cxPt : p.cxPt) * kc, y: r.top + R + p.cyPt * kc, kc };
  }
  /** Where a pose of a dock's sheet is on the screen, and the dock's scale. */
  const dkScale = dk => { const r = dk.cv.getBoundingClientRect(); return dk.st && r.width ? r.width / dk.st.wPt : 0; };
  function dockAtPose(dk, p) { const r = dk.cv.getBoundingClientRect(), k = dkScale(dk); return { x: r.left + p.cxPt * k, y: r.top + p.cyPt * k, kc: k }; }
  function dockAt(x, y) {
    for (const side of ["prev", "next"]) {
      const dk = H.docks[side]; if (!dk) continue;
      const r = dk.el.getBoundingClientRect(), pad = 14;
      if (x >= r.left - pad && x <= r.right + pad && y >= r.top - pad && y <= r.bottom + pad) return dk;
    }
    return null;
  }
  function dropPoint(dk, x, y) {
    if (!dk.st) return null; const r = dk.cv.getBoundingClientRect(); if (!r.width) return null;
    const px = (x - r.left) / r.width * dk.st.wPt, py = (y - r.top) / r.height * dk.st.hPt;
    return { x: Math.max(0, Math.min(dk.st.wPt, px)), y: Math.max(0, Math.min(dk.st.hPt, py)) };
  }
  function setOpen(on) { for (const side of ["prev", "next"]) { const dk = H.docks[side]; if (!dk) continue; dk.el.classList.toggle("open", on); if (!on) { dk.el.classList.remove("peek"); dk.el.dataset.state = "idle"; dk.el.removeAttribute("data-hot"); dk.ghost = null; dk.why = ""; dk.cardW = 170; dk.el.style.removeProperty("--pw"); fillDock(dk); } } }
  function begin(p, x, y) {
    const K = kit(); if (!K || H.drag) return;
    const pg = p.pg, live = (pg.placements || []).find(q => q.id === p.c.id);
    const z0 = { c: p.c, src: pg, placed: !!live, p: live ? shown(p.c, live) : p.x.p };
    const unit = unitOf(pg, p.c, null); if (unit.why) { popup(K.W.el.plate, unit.why + ".", "Nothing was changed."); return; }
    for (const z of unit.pieces) if (!z.p && z.c === p.c) z.p = z0.p;
    const d = H.drag = { pid: p.pid, x, y, px: x, tilt: 0, pg, c: p.c, sheetId: pg.sheetId, unit, hot: null, peekT: 0, raf: 0, nope: false };
    K.W.hop = true; if (K.W.moveFrame) { cancelAnimationFrame(K.W.moveFrame); K.W.moveFrame = 0; } K.W.hover = null; tryDo(() => K.tip(null)); K.paintFx();
    if (CNM()) { d.handing = true; const m = window.CNManual; if (m && typeof m.cancelDrag === "function") tryDo(() => m.cancelDrag()); else tryDo(() => K.W.el.fx.dispatchEvent(new PointerEvent("pointercancel", { pointerId: p.pid, bubbles: true }))); d.handing = false; }
    try { K.W.dlg.setPointerCapture(p.pid); } catch (_) {}
    const at = plateAt(K, z0.p);
    d.lift = makeLift(K, z0, unit.pieces.length > 1 ? unit.pieces.length - 1 : 0);
    d.lift.place(at.x, at.y, +z0.p.angle || 0, 1);
    if (!reduced()) d.lift.el.animate([{ transform: tf(d.lift, at.x, at.y, +z0.p.angle || 0, 1, d.lift.mirror) }, { transform: tf(d.lift, x, y, +z0.p.angle || 0, 1.08, d.lift.mirror) }], { duration: 160, easing: EASE });
    // each target is judged now, before the hand gets there: a dim dock says why when the hand is over it
    for (const side of ["prev", "next"]) { const dk = H.docks[side]; if (!dk) continue; const u = unitOf(pg, p.c, dk.sh); dk.why = sheetRefusal(pg) || sheetRefusal(dk.sh) || unitRefusal(u, pg, dk.sh); dk.unit = u; dk.el.dataset.state = dk.why ? "dim" : "idle"; fillDock(dk); }
    setOpen(true); fxLoop();
    const frame = () => {
      if (H.drag !== d) return; d.raf = requestAnimationFrame(frame);
      if (K.W.hover) { K.W.hover = null; K.paintFx(); } if (K.W.el.tip) K.W.el.tip.classList.remove("on");   // (the window's own hover and name tip stay down while a charm is held)
      const dx = d.x - d.px; d.px = d.x; d.tilt += ((reduced() ? 0 : Math.max(-6, Math.min(6, dx * .6))) - d.tilt) * .2;
      const dk = dockAt(d.x, d.y); if (dk !== d.hot) setHot(d, dk);
      // over an open view of the other sheet, the charm takes that sheet's scale: what is seen is what lands
      const peek = d.hot && d.hot.el.classList.contains("peek") && !d.hot.why, s = peek ? Math.max(.25, dkScale(d.hot) / (K.W.k / K.W.dpr)) : 1.08;
      d.lift.place(d.x, d.y, (+z0.p.angle || 0) + d.tilt, d.lift.s + (s - d.lift.s) * .25);
      if (peek) aim(d, d.hot);
    };
    d.raf = requestAnimationFrame(frame);
  }
  function setHot(d, dk) {
    const was = d.hot; d.hot = dk; clearTimeout(d.peekT);
    if (was) { was.el.removeAttribute("data-hot"); was.el.classList.remove("peek"); if (!was.why) was.el.dataset.state = "idle"; was.ghost = null; was.cardW = 170; was.el.style.removeProperty("--pw"); fillDock(was); }
    if (!dk) return;
    dk.el.setAttribute("data-hot", ""); if (!dk.why) dk.el.dataset.state = "armed"; fillDock(dk);
    if (dk.why) return;
    // held there a moment: the dock opens into a larger view of its sheet (the hand aims inside it)
    d.peekT = setTimeout(() => {
      if (d.hot !== dk || H.drag !== d) return;
      const K = kit(), box = K.W.el.plateBox.getBoundingClientRect(), pw = Math.round(Math.max(240, Math.min(560, box.width * .46)));
      dk.el.style.setProperty("--pw", pw + "px"); dk.cardW = pw; dk.el.classList.add("peek"); fillDock(dk);
    }, reduced() ? 0 : 380);
  }
  /** The ghost in the open view: where the charm (and its order's pieces) would sit, sage where it fits, clay where it does not. */
  function aim(d, dk) {
    const K = kit(), pt = dropPoint(dk, d.x, d.y); if (!pt) return;
    const key = Math.round(pt.x) + ":" + Math.round(pt.y); if (dk.aimKey === key) return; dk.aimKey = key;
    let G; try { G = dk.G && dk.G.key === dockSig(dk.sh) ? dk.G : (dk.G = Object.assign(K.plateGrid(dk.sh), { key: dockSig(dk.sh) })); } catch (_) { return; }
    const z = d.unit.pieces[0], a0 = norm(z.p ? z.p.angle : 0), v = K.variantOf(z.c, a0, G.clearancePt); if (!v) return;
    const grid = G.grid.clone(); grid.fineRes = 2;
    const r = Sv().tryPlaceTight(grid, v, pt.x, pt.y, 3), ok = !!(r && r.ok);
    const spot = ok ? { c: z.c, cxPt: (r.x + v.solid.cx) / 2, cyPt: (r.y + v.solid.cy) / 2, angle: a0 } : { c: z.c, cxPt: pt.x, cyPt: pt.y, angle: a0 };
    dk.ghost = { ok, spots: [spot] }; fillDock(dk);
    warm(d, dk, G, spot);
  }
  /** MANUALHOP (Paul, 19 Oct 2026): while the hand waits over the open view, what the drop will need is made in idle slices (the held charm's turns,
   *  and the fit test's grid of that sheet, kept by its layout), so letting go does not make them all at once. */
  function warm(d, dk, G, spot) {
    if (d.warmed === dk) return; d.warmed = dk;
    const K = kit(), z = d.unit.pieces[0], todo = [...new Set(G.angles.map(norm))];
    const idle = f => (window.requestIdleCallback ? requestIdleCallback(f, { timeout: 120 }) : setTimeout(() => f({ timeRemaining: () => 8 }), 16));
    let fitted = false;
    const step = dl => {
      if (H.drag !== d || d.hot !== dk) return;
      if (!fitted) { fitted = true; tryDo(() => fitOne(dk.sh, z.c, { cxPt: spot.cxPt, cyPt: spot.cyPt, angle: spot.angle })); return idle(step); }
      while (todo.length && dl.timeRemaining() > 3) tryDo(() => K.variantOf(z.c, todo.shift(), G.clearancePt));
      if (todo.length) idle(step);
    };
    idle(step);
  }
  function stopDrag(d) {
    const K = kit(); cancelAnimationFrame(d.raf); clearTimeout(d.peekT);
    if (H.drag === d) H.drag = null;
    try { K.W.dlg.releasePointerCapture(d.pid); } catch (_) {}
    H.noClick = performance.now() + 3000; H.eat = true;
  }
  function endHold() { const K = kit(); if (K && !H.drag && !H.busy) { K.W.hop = false; paintPlateFx(); K.paintFx(); } }
  function abort(d) { stopDrag(d); setOpen(false); flyBack(d).then(endHold); }
  /** The copy goes home to its spot on the plate (a soft spring), then the spot is the charm again. */
  async function flyBack(d, from) {
    const K = kit(), L = d.lift; if (!L) return;
    const z = d.unit.pieces[0], home = plateAt(K, z.p), rot = +z.p.angle || 0;
    if (!reduced() && L.el.animate && K.W.id === d.sheetId) {
      const f = from || { x: L.x, y: L.y, rot: L.rot, s: L.s };
      const dx = home.x - f.x, dy = home.y - f.y, bend = Math.min(80, 20 + Math.hypot(dx, dy) * .12);
      await L.el.animate([
        { transform: tf(L, f.x, f.y, f.rot, f.s, L.mirror) },
        { transform: tf(L, f.x + dx * .45, f.y + dy * .45 - bend, (f.rot + rot) / 2, (f.s + 1.1) / 2, L.mirror), offset: .45 },
        { transform: tf(L, home.x, home.y, rot, 1.04, L.mirror), offset: .85 },
        { transform: tf(L, home.x, home.y, rot, 1, L.mirror) }
      ], { duration: 620, easing: "cubic-bezier(.3,1.15,.4,1)", fill: "forwards" }).finished.catch(() => {});
    }
    L.el.remove(); d.lift = null;
  }
  /** The copy flies on an arc into its spot on the other sheet's view, turning to the angle it was given, and lands. */
  async function flyInto(L, from, to, rotTo, delay, ms) {
    if (!L) return;
    if (reduced() || !L.el.animate) { L.el.remove(); return; }
    const dx = to.x - from.x, dy = to.y - from.y, bend = Math.min(120, 36 + Math.hypot(dx, dy) * .16), s1 = to.kc / from.kc;
    await L.el.animate([
      { transform: tf(L, from.x, from.y, from.rot, from.s, L.mirror), opacity: 1 },
      { transform: tf(L, from.x + dx * .42, from.y + dy * .42 - bend, (from.rot + rotTo) / 2, (from.s + s1) / 1.7, 1), opacity: 1, offset: .42 },
      { transform: tf(L, to.x, to.y - 2, rotTo, s1 * 1.14, 1), opacity: 1, offset: .86 },
      { transform: tf(L, to.x, to.y, rotTo, s1, 1), opacity: 0 }
    ], { duration: ms || 760, delay: delay || 0, easing: "cubic-bezier(.5,.05,.3,1)", fill: "forwards" }).finished.catch(() => {});
    L.el.remove();
  }
  async function drop(d) {
    const dk = d.hot; stopDrag(d);
    if (!dk) { setOpen(false); await flyBack(d); endHold(); return; }
    const pt = dropPoint(dk, d.x, d.y);
    return hop(d, dk, pt);
  }

  /* ── the move ── */
  function popup(anchor, title, sub) {
    const K = kit(); if (!K || !K.W.dlg || !K.W.dlg.open) return null;
    if (H.pop) closePop(H.pop, true);
    const n = doc.createElement("div"); n.className = "shpPop"; n.setAttribute("role", "alert"); n.dataset.hop = "pop";
    n.innerHTML = `${SVG.room}<span class="t"><b></b><small></small></span><button type="button" aria-label="Dismiss">${SVG.x}</button>`;
    n.querySelector("b").textContent = title; n.querySelector("small").textContent = sub || "Nothing was changed.";
    K.W.dlg.appendChild(n);
    const r = anchor && anchor.isConnected ? anchor.getBoundingClientRect() : K.W.el.plateBox.getBoundingClientRect(), d = K.W.dlg.getBoundingClientRect(), w = n.offsetWidth;
    const left = Math.max(d.left + 10, Math.min(d.right - w - 10, r.left + r.width / 2 - w / 2)), top = Math.min(innerHeight - n.offsetHeight - 10, r.bottom + 10);
    Object.assign(n.style, { left: left + "px", top: top + "px" }); n.style.setProperty("--ox", Math.round(r.left + r.width / 2 - left) + "px");
    let hold = false; n.addEventListener("pointerenter", () => { hold = true; }); n.addEventListener("pointerleave", () => { hold = false; });
    n.querySelector("button").onclick = () => closePop(n);
    const t0 = Date.now(); n._t = setInterval(() => { if (!hold && Date.now() - t0 > 5200) closePop(n); }, 250);
    H.pop = n; return n;
  }
  function closePop(n, now) {
    if (!n || n._closing) return; n._closing = true; clearInterval(n._t); if (H.pop === n) H.pop = null;
    if (now || reduced()) { n.remove(); return; }
    n.classList.add("out"); setTimeout(() => n.remove(), 280);
  }
  const charmWords = u => (u.pieces.length === 1 ? "the charm" : `the ${u.pieces.length} pieces of order ${u.rid}`);
  async function hop(d, dk, pt) {
    const K = kit(), src = d.pg, target = dk.sh, sName = word(src), tName = word(target);
    const L = d.lift, from = L ? { x: L.x, y: L.y, rot: L.rot, s: L.s, kc: K.W.k / K.W.dpr } : null;
    const refuse = async (title, sub) => { dk.el.classList.remove("nope"); void dk.el.offsetWidth; dk.el.classList.add("nope"); d.nope = true; popup(dk.el, title, sub); setOpen(false); await flyBack(d, d.lift && d.lift !== L ? null : from); H.busy = null; endHold(); return false; };
    const u = unitOf(src, d.c, target);
    const why = sheetRefusal(src) || sheetRefusal(target) || unitRefusal(u, src, target) || (K.flow() ? `One change at a time: ${K.flow().charAt(0).toLowerCase() + K.flow().slice(1)} is still running` : "");
    if (why) return refuse(`${why}.`, `${u.pieces && u.pieces.length > 1 ? `The ${u.pieces.length} pieces` : "The charm"} stayed on ${sName}. Nothing was changed.`);
    const who = K.needName(() => hopPiece(src, d.c, dk.side, pt)); if (!who) { setOpen(false); await flyBack(d, from); H.busy = null; endHold(); return false; }
    // the hand let go: the copy waits over the dock while the room is looked for
    H.busy = d; d.unit = u; fxLoop();
    if (L) L.el.classList.add("think");
    let spots = null, last = performance.now();
    const breathe = async () => { if (performance.now() - last > 12) { await wait(0); last = performance.now(); } };
    try { spots = await findRoom(target, u, pt, breathe); } catch (e) { console.warn("sheet hop: room", e); spots = null; }
    if (L) L.el.classList.remove("think");
    if (!spots) {
      const title = u.pieces.length === 1 ? `Not enough space on ${tName} — the charm stayed on ${sName}` : `Not enough space on ${tName} — the ${u.pieces.length} pieces of order ${u.rid} stayed on ${sName}`;
      return refuse(title, u.pieces.length === 1 ? "The charm could not be moved to that sheet. Nothing was changed." : "An order's pieces on a sheet move together. Nothing was changed.");
    }
    if (K.moveNow) return hopNow(d, dk, u, spots, who, from, L, refuse);
    // found: it flies into its spot, its order's other pieces after it from where they lie; the view keeps them drawn there while the sheets save.
    // Meanwhile (not after: the flight never waits on the network) the cloud is asked whether another computer changed these pieces or sheets
    const checking = cloudCheck(u, src, target);
    H.landing.set(target.sheetId, { ids: new Set(spots.map(s => s.c.poolId)), spots, saving: true });
    dk.ghost = null; fillDock(dk);
    const flights = [];
    if (L) { const to = dockAtPose(dk, spots[0]); flights.push(flyInto(L, from, to, spots[0].angle, 0)); d.lift = null; }
    spots.slice(1).forEach((s, i) => {
      const z = u.pieces.find(q => q.c === s.c); if (!z || !z.p || K.W.id !== d.sheetId) return;
      const Lm = makeLift(K, z, 0), a = plateAt(K, z.p); Lm.place(a.x, a.y, +z.p.angle || 0, 1);
      flights.push(flyInto(Lm, { x: a.x, y: a.y, rot: +z.p.angle || 0, s: 1, kc: a.kc }, dockAtPose(dk, s), s.angle, 110 + i * 90));
    });
    await Promise.all(flights);
    const stale = await checking;
    if (stale) {
      // another computer got there first: the charm comes back out of that sheet to its own spot, and nothing is written
      H.landing.delete(target.sheetId); fillDock(dk);
      const z = u.pieces[0], a = dockAtPose(dk, spots[0]), kc = K.W.k / K.W.dpr;
      if (z.p && K.W.id === d.sheetId) { d.lift = makeLift(K, z, 0); d.lift.place(a.x, a.y, spots[0].angle, a.kc / kc, 1); }
      return refuse(stale, `${u.pieces.length === 1 ? "The charm" : "Its pieces"} stayed on ${sName}. Nothing was changed.`);
    }
    dk.el.classList.remove("got"); void dk.el.offsetWidth; dk.el.classList.add("got");
    if (window.Motion && Motion.arrive && !reduced()) tryDo(() => Motion.arrive(dk.el.querySelector(".shpCard"), { plus: "+" + spots.length, tone: "ok" }));
    const k = { rid: u.rid, pieces: u.pieces.map(z => ({ c: z.c, src, placed: z.placed })), srcs: new Set([src]) };
    let ok = false, err = null;
    try { ok = await K.moveIn(k, spots.map(s => ({ c: s.c, src, placed: s.placed, cxPt: s.cxPt, cyPt: s.cyPt, angle: s.angle })), target, who, { guard: sheetRefusal }); }
    catch (e) { err = e; }
    H.landing.delete(target.sheetId); H.busy = null; setOpen(false); endHold(); refresh(true);
    if (ok) {
      const anchor = (H.docks[dk.side] && H.docks[dk.side].el) || K.W.el.plateBox;
      K.note(anchor, { text: `Order ${u.rid}: ${u.pieces.length === 1 ? "the charm" : u.pieces.length + " pieces"} moved to ${tName}`, tone: "ok", actions: [{ label: "Show it", fn: () => K.switchSheet(target.sheetId, { select: spots[0].c.poolId }) }] });
      return true;
    }
    popup((H.docks[dk.side] || dk).el, err ? `Not moved: ${err.message}` : "Not moved: another change is still running", `${u.pieces.length === 1 ? "The charm" : "Its pieces"} stayed on ${sName}.`);
    return false;
  }
  /* MANUALHOP (Paul, 19 Oct 2026: "When I moved the charm from one sheet to another, it took a very long time, and it froze ... This needs to be a near
     instantaneous process"). Root cause of the slowness: the drop awaited the cloud check, then moveIn, which nested, wrote, labelled and read back both sheets
     one after the other before the hand was let go (10 to 20 s, the window held by its steps panel). Now the sheet window's moveNow makes the move on the live
     sheets at once (the guards first: a refusal flies the charm back and says why, nothing changed), the hand is free in the same frame, and the copies finish
     a short flight into the open view while the save runs in the background (the cloud check included; a save that fails puts the pieces back and says so). */
  const FLY_NOW = 420;
  function hopNow(d, dk, u, spots, who, from, L, refuse) {
    const K = kit(), src = d.pg, target = dk.sh, sName = word(src);
    const k = { rid: u.rid, pieces: u.pieces.map(z => ({ c: z.c, src, placed: z.placed })), srcs: new Set([src]) };
    // where each copy flies, read before the sheets change (the open view keeps its scale), and where the order's other pieces lie on this plate
    const tos = spots.map(s => dockAtPose(dk, s));
    const lying = spots.map(s => { const z = u.pieces.find(q => q.c === s.c); return z && z.p && K.W.id === d.sheetId ? { z, a: plateAt(K, z.p) } : null; });
    const failed = why => { const dk2 = H.docks[dk.side]; popup(dk2 && dk2.sh === target ? dk2.el : K.W.el.plateBox, `Not moved: ${String(why || "it could not be saved").replace(/\.$/, "")}.`, `${u.pieces.length === 1 ? "The charm went" : "Its pieces went"} back to ${sName}.`); };
    let r; try { r = K.moveNow(k, spots.map(s => ({ c: s.c, src, placed: s.placed, cxPt: s.cxPt, cyPt: s.cyPt, angle: s.angle })), target, who, { guard: sheetRefusal, check: () => cloudCheck(u, src, target), failed }); }
    catch (e) { console.warn("sheet hop: move", e); r = { ok: false, why: e.message || "it could not be moved" }; }
    if (!r || !r.ok) return refuse(`Not moved: ${String((r && r.why) || "it could not be moved").replace(/\.$/, "")}.`, `${u.pieces.length === 1 ? "The charm" : "Its pieces"} stayed on ${sName}. Nothing was changed.`);
    // on B now: the hand is free, the dock answers, the copies land
    H.busy = null; dk.ghost = null; fillDock(dk);
    dk.el.classList.remove("got"); void dk.el.offsetWidth; dk.el.classList.add("got");
    if (window.Motion && Motion.arrive && !reduced()) tryDo(() => Motion.arrive(dk.el.querySelector(".shpCard"), { plus: "+" + spots.length, tone: "ok" }));
    const flights = [];
    if (L) { flights.push(flyInto(L, from, tos[0], spots[0].angle, 0, FLY_NOW)); if (d.lift === L) d.lift = null; }
    spots.slice(1).forEach((s, i) => { const q = lying[i + 1]; if (!q) return; const Lm = makeLift(K, q.z, 0); Lm.place(q.a.x, q.a.y, +q.z.p.angle || 0, 1); flights.push(flyInto(Lm, { x: q.a.x, y: q.a.y, rot: +q.z.p.angle || 0, s: 1, kc: q.a.kc }, tos[i + 1], s.angle, 60 + i * 60, FLY_NOW)); });
    endHold(); paintPlateFx();
    Promise.all(flights).then(() => { if (!H.drag && !H.busy) { setOpen(false); refresh(true); } });
    const anchor = (H.docks[dk.side] && H.docks[dk.side].el) || K.W.el.plateBox;
    K.note(anchor, { text: `Order ${u.rid}: ${u.pieces.length === 1 ? "the charm" : u.pieces.length + " pieces"} moved to ${word(target)}`, tone: "ok", actions: [{ label: "Show it", fn: () => K.switchSheet(target.sheetId, { select: spots[0].c.poolId }) }] });
    return true;
  }
  /** The two saved records and the pieces' records, read now: "" when they still say what this screen holds, else what changed. */
  async function cloudCheck(u, src, target) {
    if (typeof api !== "function") return "";
    const ids = u.pieces.map(z => z.c.poolId);
    try {
      const [a, b, pr] = await Promise.all([
        api("charmNestLibrary", { op: "getSheet", id: src.sheetId }, { quiet: true, timeoutMs: 12000 }),
        api("charmNestLibrary", { op: "getSheet", id: target.sheetId }, { quiet: true, timeoutMs: 12000 }),
        api("charmNestLibrary", { op: "poolGet", poolIds: ids }, { quiet: true, timeoutMs: 12000 })
      ]);
      const listed = r => new Set(((r && r.sheet && r.sheet.poolIds) || []).map(String));
      const sa = listed(a), sb = listed(b);
      if (!(a && a.sheet) || !(b && b.sheet)) return "One of the two sheets is no longer in the Library";
      if (a.sheet.archived || b.sheet.archived) return "One of the two sheets was removed meanwhile";
      if (+a.sheet.laserDoneAt > 0 || +b.sheet.laserDoneAt > 0 || a.sheet.roseCutAt || b.sheet.roseCutAt) return "One of the two sheets was cut on another computer meanwhile";
      if (ids.some(id => sb.has(id))) return `${word(target)} already lists this piece (another computer moved it there): the sheet is read again`;
      // (a piece the run placed but has not saved yet is on no record: only a record that lists other pieces and not this one says it was taken off)
      if (sa.size && ids.some(id => !sa.has(id))) return `This piece was taken off ${word(src)} on another computer meanwhile`;
      for (const p of Object.values((pr && pr.pools) || {})) {
        // (the cloud's own reading of a take-off, _charmNestPlacement.js takenOff: a hold or a cancel's mark on a row with no sheet, not made up again since)
        if (["abandoned", "superseded"].includes(p.state) && !p.sheetId && (p.state === "superseded" || (!p.repooledAt && (p.heldAt || p.removedAt || p.heldBy || p.removedBy)))) return `Order ${u.rid} was put on hold or taken off on another computer meanwhile`;
        if (p.sheetId && p.sheetId !== src.sheetId) return `Order ${u.rid}'s piece is recorded on another sheet now`;
      }
      return "";
    } catch (e) { return `The sheets could not be read from the cloud (${e.message}); nothing was moved`; }
  }
  /** The selected charm (the window's piece view) to the previous or next sheet, at the same place: the keyboard's way, and a name given late. */
  function hopSelected(side, pt) {
    const K = kit(), pg = here(), x = K && selOf(K); if (!pg || !x || !x.poolId) return Promise.resolve(false);
    const c = actives(pg).find(z => z.poolId === x.poolId); if (!c) return Promise.resolve(false);
    return hopPiece(pg, c, side, pt, x.p);
  }
  async function hopPiece(pg, c, side, pt, p1) {
    const K = kit(); if (!K || !pg || here() !== pg || !manualOn(pg) || H.drag || H.busy) return false;
    refresh(true);
    const dk = H.docks[side]; if (!dk) return false;
    const live = (pg.placements || []).find(q => q.id === c.id), p0 = live ? shown(c, live) : p1; if (!p0) return false;
    const unit = unitOf(pg, c, null); if (unit.why) { popup(dk.el, unit.why + ".", "Nothing was changed."); return false; }
    for (const z of unit.pieces) if (!z.p && z.c === c) z.p = p0;
    const d = { pid: -1, pg, c, sheetId: pg.sheetId, unit, x: 0, y: 0, nope: false };
    K.W.hop = true; setOpen(true);
    await wait(reduced() ? 0 : 340);   // (the docks open first)
    const at = plateAt(K, p0); d.lift = makeLift(K, { c, src: pg, p: p0 }, unit.pieces.length > 1 ? unit.pieces.length - 1 : 0); d.lift.place(at.x, at.y, +p0.angle || 0, 1.08);
    fxLoop();
    return hop(d, dk, pt || { x: p0.cxPt, y: p0.cyPt });
  }

  /* ── following the window ── */
  setInterval(() => {
    const K = kit();
    if (!K || !window.SheetWin || !SheetWin.isOpen()) { if (H.drag) abort(H.drag); if (H.docks.prev || H.docks.next) clearDocks(); if (H.sw) { H.sw.remove(); H.sw = null; } H.local = false; return; }
    if (!H.toggleHooked) { const m = CNM(); if (m && typeof m.onToggle === "function") { H.toggleHooked = true; tryDo(() => m.onToggle(() => refresh(true))); } }
    if (!H.drag && !H.busy) refresh(false);
  }, 300);

  window.SheetHop = { refresh: () => refresh(true), state: () => ({ on: !!here() && manualOn(here()), docks: Object.fromEntries(["prev", "next"].map(s => [s, H.docks[s] ? { sheetId: H.docks[s].sh.sheetId, name: H.docks[s].name, why: H.docks[s].why } : null])), dragging: !!H.drag, busy: !!H.busy }), fitOne, findRoom, unitOf, unitRefusal, sheetRefusal, hopSelected, hopPiece, _H: H };
})();
