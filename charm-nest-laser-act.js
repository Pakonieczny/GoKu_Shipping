/* charm-nest-laser-act.js — what a person did at the Laser station in the Sorter app, for the Employee efficiency console
   (Paul, 6 Oct 2026: "all applications ... fully wired into the employee efficiency portal"; stations round 2, SA5).

   ONE place says what a laser-side press writes, so the Library, the flow (Approve for laser cutting, drags), the sheet window
   and Rose Gold's Cut Sheet all write the same way:
     CNLaserAct.cut(done, { name, ids, rec })   a laser cut marked completed (done) or taken back (an undo)
     CNLaserAct.flow(steps)                     the flow's kept steps (charm-nest-flow.js): approved, hold lifted, put back
     CNLaserAct.rose(sh)                        Rose Gold's Cut Sheet, recorded
   Each is one call to CNAct (charm-nest-bridge.js: station-activity.js, only with a signed-in name, held back when nobody is
   named, a Laser or Design role routing it to its own station) and never throws, waits or touches the network. A sheet holds
   several orders and the person's page lists ORDERS, so each order of the sheet gets its own event (`each`), carrying the sheet's
   id in `line`, its label in `detail`, and its charms as `parts`. A sheet whose orders are not known here is one event with the
   sheet id. A cut is `complete` / `undo` with orders 0 (it is not an order completion: the order is still on its way); an
   approval, a lifted hold and a put-back are `note` (a decision, no pieces). Never keystrokes, never a hover.
   The phrases are fixed words (netlify/functions/_activityKinds.js reads `detail`): none is a held/skipped/sent-back/cancel
   phrase, so none counts as an issue. Documented in plans/stations-round2/api.md (SA5). */
(function (root) {
  "use strict";
  const CAP = 200, MEMO_MAX = 400;
  const digits = v => String(v == null ? "" : v).replace(/\D/g, "").slice(0, 30);
  const text = (v, n) => String(v == null ? "" : v).replace(/\s+/g, " ").trim().slice(0, n);
  /** the order of a piece's pool id ("rid_tid_copy") */
  const ridOf = id => { const m = /^(\d{1,30})_/.exec(String(id == null ? "" : id)); return m ? m[1] : ""; };
  const LD = () => root.LibraryDone || {};

  /* What each sheet held, remembered from the moment it was known (a Library row), so a later Undo from the Completed tab, whose rows
     only count their orders, still names them. Bounded; in this tab only. */
  const memo = new Map();
  const remember = (id, f) => { if (!id || !f.by.size) return; memo.delete(id); memo.set(id, f); while (memo.size > MEMO_MAX) memo.delete(memo.keys().next().value); };

  /** What a record says of its orders: { by: Map(orderId -> charms), parts }. A Library row (orders, poolIds, placedCount), a
      Completed row (counts only) or a Nest-tab sheet (charms with poolId, placements). */
  function factsOf(id, rec) {
    const by = new Map();
    let parts = 0;
    try {
      if (rec && typeof rec === "object") {
        const pools = Array.isArray(rec.poolIds) && rec.poolIds.length ? rec.poolIds : Array.isArray(rec.charms) ? rec.charms.map(c => c && (c.poolId || c.id)) : [];
        for (const p of pools) { const r = ridOf(p); if (r) by.set(r, (by.get(r) || 0) + 1); }
        const placed = Math.max(0, Math.floor(+rec.placedCount || +rec.pieces || +rec.charmCount || (Array.isArray(rec.placements) ? rec.placements.length : 0) || 0));
        parts = placed || [...by.values()].reduce((a, b) => a + b, 0);
        if (!by.size && Array.isArray(rec.orders)) { for (const o of rec.orders) { const r = digits(o); if (r) by.set(r, 0); } if (by.size === 1) by.set([...by.keys()][0], parts); }
      }
    } catch (_) { /* what is known is used */ }
    const f = { by, parts };
    if (by.size) remember(id, f);
    else if (memo.has(id)) return memo.get(id);
    return f;
  }

  /** one event per order of the sheets (the page lists orders), or one for the sheet when no order is known: the CNAct options */
  function optsOf(sheets, withParts) {
    const each = [];
    let total = 0;
    for (const s of sheets) {
      const f = factsOf(s.id, s.rec); total += f.parts;
      for (const [orderId, n] of f.by) each.push(withParts ? { orderId, line: s.id, parts: n } : { orderId, line: s.id });
    }
    if (each.length) return { each: each.slice(0, CAP) };
    const o = { line: sheets[0] ? sheets[0].id : "" };
    if (withParts) o.parts = total;
    return o;
  }

  function send(action, sheets, detail, withParts) {
    try {
      const A = root.CNAct;
      if (typeof A !== "function") return false;
      const clean = (sheets || []).filter(s => s && s.id).map(s => ({ id: text(s.id, 40), rec: s.rec }));
      if (!clean.length) return false;
      return A(action, Object.assign({ station: "laser", orders: 0, detail: text(detail, 120) }, optsOf(clean, withParts)));
    } catch (_) { return false; }
  }

  /** a laser cut marked completed, or taken back (Library check, the sheet window, a drag to Completed, Undo). o: { name, ids, rec } */
  function cut(done, o) {
    try {
      o = o || {};
      const rec = typeof o.rec === "function" ? o.rec : () => null, name = text(o.name, 60) || "Sheet";
      return send(done ? "complete" : "undo", (o.ids || []).map(id => ({ id, rec: rec(id) })), `${name} ${done ? "marked completed (laser)" : "returned to Laser cutting"}`, true);
    } catch (_) { return false; }
  }

  /** Cut Sheet, recorded (charm-nest-rose-ui.js: sh is the Nest-tab sheet; Rose Gold, 10K and 14K: "RG Sheet 1 cut", "10K Sheet 1 cut") */
  function rose(sh) {
    try {
      if (!sh) return false;
      const id = text(sh.sheetId || sh.id, 40) || "rose";
      return send("complete", [{ id, rec: sh }], `${(root.CharmNestRose && root.CharmNestRose.cutCode && root.CharmNestRose.cutCode(sh.metal)) || "RG"} Sheet ${Math.max(1, Math.floor(+sh.page) || 1)} cut (Cut Sheet)`, true);
    } catch (_) { return false; }
  }

  /** What the flow kept of one press (charm-nest-flow.js: steps { type: "seal" | "release" | "hold", kind?, id?, sheetIds? }):
      the person's ready seal is "approved for laser cutting"; a lifted hold alone, "hold lifted"; a put-back, "put back in progress". */
  function flow(steps) {
    try {
      const ld = LD(), rec = id => (typeof ld.recordOf === "function" ? ld.recordOf(id) : null);
      const nameOf = (kind, id) => { try { return typeof ld.nameOf === "function" ? ld.nameOf(kind, id) : ""; } catch (_) { return ""; } };
      const setOf = id => { try { return typeof ld.setSheets === "function" ? ld.setSheets(id) : []; } catch (_) { return []; } };
      const group = { seal: [], release: [], hold: [], setMember: [] };
      for (const s of steps || []) {
        if (s && s.type === "setMember") { for (const m of s.moves || []) group.setMember.push({ out: !m.to, id: m.sheetId, name: nameOf("sheet", m.sheetId) }); continue; }   // (a sheet taken out of, or put in, a committed set: Paul, 7 Oct)
        if (!s || !group[s.type]) continue;
        if (s.type === "seal") {
          const ids = s.kind === "set" ? setOf(s.id) : [s.id];
          group.seal.push({ names: [nameOf(s.kind === "set" ? "set" : "sheet", s.id)], ids: ids.length ? ids : [s.id] });
        } else group[s.type].push({ names: (s.sheetIds || []).map(i => nameOf("sheet", i)), ids: (s.sheetIds || []).slice() });
      }
      const merge = list => {
        const ids = [...new Set(list.flatMap(g => g.ids).filter(Boolean))], names = [...new Set(list.flatMap(g => g.names).filter(Boolean))];
        return { ids, label: names.length > 3 ? names.slice(0, 3).join(", ") + ` +${names.length - 3}` : names.join(", ") };
      };
      let did = false;
      const seal = merge(group.seal), sealed = new Set(seal.ids);
      if (seal.ids.length) did = send("note", seal.ids.map(id => ({ id, rec: rec(id) })), `approved for laser cutting${seal.label ? " · " + seal.label : ""}`, false) || did;
      const rel = merge(group.release.map(g => ({ names: g.names, ids: g.ids.filter(i => !sealed.has(i)) })));
      if (rel.ids.length) did = send("note", rel.ids.map(id => ({ id, rec: rec(id) })), `hold lifted${rel.label ? " · " + rel.label : ""}`, false) || did;
      const hold = merge(group.hold);
      if (hold.ids.length) did = send("note", hold.ids.map(id => ({ id, rec: rec(id) })), `put back in progress${hold.label ? " · " + hold.label : ""}`, false) || did;
      for (const out of [true, false]) { const g = group.setMember.filter(x => x.out === out); if (g.length) did = send("note", g.map(x => ({ id: x.id, rec: rec(x.id) })), `${out ? "taken out of its set" : "put in a committed set"} · ${g.map(x => x.name).filter(Boolean).join(", ")}`, false) || did; }
      return did;
    } catch (_) { return false; }
  }

  root.CNLaserAct = { cut, flow, rose, factsOf: (id, rec) => factsOf(id, rec) };
})(typeof window !== "undefined" ? window : globalThis);
