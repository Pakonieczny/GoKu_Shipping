/* LibraryFlow: the engine behind dragging a sheet or a set between In progress, Laser cutting and Completed in the
 * Library, and behind the manual "Approve for laser cutting" (Paul, 3 Oct 2026). No screen of its own: the drag, the
 * flight and the bars are charm-nest-library-dnd.js and its neighbours, and they call this.
 *
 *   LibraryFlow.targets(item)            where a sheet or set may be dropped: [{area}|{set}|{newSet:true}]  (sync, from the page)
 *   LibraryFlow.explainTargets(item)     the same, every zone with {name, ok, reason}: a dimmed zone says why on hover
 *   LibraryFlow.sheetZone(item, otherId, {self, name})   the place a draft sheet makes when another draft sheet is dropped on it: {sheet, name, sub, ok, reason}
 *                                        (Paul, 7 Oct: "I can not drag/drop these to combine them"); plan({kind:'sheet', id, to:{sheet:otherId}}) puts both
 *                                        into the run's open set, or into a new set when none is open (the other sheet is a companion `include` step)
 *   LibraryFlow.plan({kind,id,to,by})    Promise<Plan>: reads, never writes
 *   LibraryFlow.commit(plan,{confirmed,by,onStep})   Promise<{ok,applied:[{key,label}],error}>
 *   LibraryFlow.approve({kind,id,by,confirmed})      Promise<Plan>: runs every safe automatic step now, lists what is still missing
 *   LibraryFlow.cutLine({kind,id,sheetIds?,by,onStep}) Promise<{ok,error,lines,cut,sheets}>: the person's yes in the green dash line window: the Cut Sheet press (line drawn and dated, cut recorded) for each partial sheet of the item; never moves it
 *
 *   Plan = { ok, from:{area,setId}, to:{area,setId}, auto:[{key,label,detail}], needs:[{key,label,detail,items}],
 *            confirm:[{key,label,detail}], notes:[string], kind, id, move:{kind,id,to}, noop?:true }
 *   (an auto line with check:true is a check that already passed, shown so the person sees what was verified; every other
 *   auto line is something the move does by itself, and commit reports it back in `applied`.)
 *
 * Where a sheet or set stands is READ, never stored (charm-nest-readiness.js): Completed = marked cut (laserDoneAt), Laser
 * cutting = every check passed, In progress = the rest. A move therefore does the things that make the destination true,
 * each with the code that already owns it, and nothing else:
 *   to Laser cutting     the checks, the QR label (made again when missing), a person's hold lifted, the readiness seal by
 *                        the person (op flowApply "seal" = laserStatus' recordProcessReadiness)
 *   to Completed         the above, then LibraryDone.mark (op laserDone: the same stage gate, seals and timeline)
 *   Completed to Laser   LibraryDone.mark(…, false) (Undo: every seal and cut record stays)
 *   back to In progress  a HOLD (laserHold on the sheets, op flowApply): approvals, seals and cut records are all kept, the
 *                        sheet just is not offered to the laser; moving it to Laser cutting again lifts it
 *   into a set           the sheet window's own Include / Make QR label paths (Gate.changeMembership, release): a set of a
 *                        run still open is made by that run's release rules; a COMMITTED set takes and gives sheets by op flowApply
 *                        (step setMember, Paul 7 Oct: only while no multi-piece order is shared with another sheet of the set and the
 *                        sheet is not completed; see committedMembership / planLeave below and charm-nest-set-edit.js)
 *   out of a committed set  a sheet dropped on In progress that is in a committed set that is not completed: taken out of the set (held back)
 * A set advances as ONE (Paul, 5 Oct, round 7: "you cannot have a green approved button on a single sheet that is part of a set
 * where the other sheets are not ready yet"). Going forward (Approve, or a drop on Laser cutting), a sheet of a set of several is
 * planned WITH its set: every sheet's own steps (hold lifted, QR label) and one seal for the set, in one action. First the set's gate
 * (CharmNestReadiness.setGate, the lone button's hard test for every sheet): while any sheet of it is not ready to be approved the
 * plan carries the need `setGate` (label = the plain reason, items = the blocking sheets), no step and no auto line, so approve and
 * commit write nothing at all, for a sheet and for the set alike. A set of one sheet and a loose sheet are planned as before. The
 * server twin refuses the same (op flowApply, 409). A step that only restores a failed move carries restore:true and is never refused.
 * Cardinal rule of a Set of Sheets (Paul, 5 Oct; charm-nest-shared-orders.js): every sheet that shares a multi-piece order with
 * another is in the SAME set. A move into or out of a set that would separate such sheets is not a plan that can be committed:
 * plan.needs gets key `sharedOrders` ("Orders shared with another sheet", items = SharedOrders.between's
 * shape, also plan.shared; plan.group the labels of the sheets that must travel together) and nothing is written. Moving the
 * whole group together is allowed: when every sheet of the group can join, the plan asks for one yes (confirm key `together`,
 * commit includes them all); `plan({..., together:true})` plans that explicitly. The orders come from the records (op flowState
 * with `move`, the server's twin) and from the page (SharedOrders.between); either one finding a split blocks.
 * Rose Gold: a move NEVER adds a green dash line by itself. A Rose Gold sheet without one that the move needs gets the
 * confirm key `roseLine`; commit calculates it (LibraryFlowRose.calculate, the Cut Sheet path) only when `confirmed` holds
 * that key, and does not move anything without it.
 * Restart-safe: commit reads the records again and plans again before it writes; every step is idempotent; a hold or a
 * release that a later step makes pointless is undone; a failed commit changes nothing. No Etsy calls anywhere here. */
(function (root, factory) { const api = factory(root); if (typeof module === 'object' && module.exports) module.exports = api; else root.LibraryFlow = api; })(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const RD = () => root.CharmNestReadiness || (typeof require === 'function' ? require('./charm-nest-readiness.js') : null);
  const SE = () => root.SetEdit || (typeof require === 'function' ? require('./charm-nest-set-edit.js') : null);
  const AREAS = ['progress', 'laser', 'completed'], AREA = { progress: 'In progress', laser: 'Laser cutting', completed: 'Completed' };
  const CODE = { gold: 'GF', silver: 'SS', rose: 'RG', gold10k: '10K', gold14k: '14K' };
  const LISTED = 30;
  const held = s => !!(s && s.laserHold && +s.laserHold.at > 0);
  const sid = s => s.id || s.sheetId;
  // a sheet is included unless it is a draft or left out of its set (Readiness.sheet reads the same fields); it is in a set when it also has one
  const joined = s => !!s && !s.draft && s.solidIncluded !== false;
  const inSet = s => !!(s && s.setId && joined(s));
  const sheetName = s => `${CODE[s.metal] || s.metalLabel || ''} Sheet ${s.sheetIndex || s.page || 1}`.trim();
  const setName = d => d && (d.seq || d.setSeq) ? `Set ${d.seq || d.setSeq}` : (d && d.name) || 'This set';
  const count = (n, one, more) => `${n} ${n === 1 ? one : (more || one + 's')}`;
  const laserOf = s => (s.laser && typeof s.laser === 'object' && s.laser.stages) ? s.laser : RD().laserSheet(s);
  const committedSet = d => !!(d && (+d.committedAt > 0 || /^complete/.test(String(d.status || ''))));
  /* The ONE test for "this metal gets a green dash line": charm-nest-rose.js CharmNestRose.cuts (Rose Gold, 10K, 14K once that
     file says so; Rose Gold alone before it). A partial sheet of such a metal that is not cut and has no line yet needs one. */
  const cuts = m => { try { if (hooks.cuts) return !!hooks.cuts(m); const R = root.CharmNestRose; return R && typeof R.cuts === 'function' ? !!R.cuts(m) : m === 'rose'; } catch (_) { return m === 'rose'; } };
  /* The ONE test for "this sheet still owes its line" and "readiness holds it for its line" is charm-nest-rose.js owesLine / holdsLine, the same two
     CharmNestReadiness asks (GF1, Paul 7 Oct); the written-out copy is only for a page or a test where that file is not loaded (or `cuts` is set by hand). */
  const shared = name => { try { if (hooks.cuts) return null; const R = root.CharmNestRose; return R && typeof R[name] === 'function' ? R[name] : null; } catch (_) { return null; } };
  const needsCutLine = s => { const f = shared('owesLine'); return f ? !!f(s) : !!s && cuts(s.metal) && !s.roseCutAt && !s.rosePlanHash; };
  const lineHeld = s => { const f = shared('holdsLine'); return f ? !!f(s) : needsCutLine(s) && !!s.roseStockId; };
  const needsRoseLine = needsCutLine;       // (the old name: the rest of this file and its tests use it)

  /* ── reading the records ────────────────────────────────────────────────────────────────────────────────────────
   * state = { sheets:{id:record}, sets:{id:setRecord}, runs:{runId:{open}}, live:{sheetId:{…}} }: what op flowState
   * answers (laserStatus' sheets with their `laser` report, each set's own record, whether each run is open) and what
   * the page knows of the sheets the open run holds. */
  function view(state, kind, id) {
    if (kind === 'set') {
      const set = state.sets[id];
      if (!set) return { error: 'There is no such set' };
      const ids = [...new Set(set.sheetIds || [])], got = ids.map(i => state.sheets[i]);
      return { kind, id, set, members: got.filter(s => s && !s.archived), missing: ids.filter((i, n) => !got[n] || got[n].archived), label: setName(set) };
    }
    const sheet = state.sheets[id];
    if (!sheet || sheet.archived) return { error: 'There is no such sheet' };
    if (!inSet(sheet)) return { kind, id, sheet, set: null, members: [sheet], missing: [], label: sheetName(sheet) };
    const set = state.sets[sheet.setId] || null;
    if (!set) return { kind, id, sheet, set: null, members: [sheet], missing: [], setMissing: true, label: sheetName(sheet) };
    const ids = [...new Set(set.sheetIds || [])], got = ids.map(i => state.sheets[i]);
    return { kind, id, sheet, set, members: got.filter(s => s && !s.archived), missing: ids.filter((i, n) => !got[n] || got[n].archived), label: sheetName(sheet) };
  }
  const memberReady = m => laserOf(m).ready;
  const groupReady = v => !v.setMissing && v.missing.length === 0 && v.members.length > 0 && v.members.every(memberReady);
  const readyNow = v => v.kind === 'set' || v.set ? groupReady(v) : memberReady(v.sheet);
  function areaOf(v) {
    if (v.kind === 'set') return +v.set.laserDoneAt > 0 ? 'completed' : groupReady(v) ? 'laser' : 'progress';
    if (+v.sheet.laserDoneAt > 0 && !v.sheet.laserSetPending) return 'completed';
    return readyNow(v) ? 'laser' : 'progress';
  }
  const normTo = to => typeof to === 'string' ? (AREAS.includes(to) ? { area: to } : { set: to }) : (to || {});
  // the group a seal belongs to is the one laserStatus records for the active view: a set for a sheet in a set
  const sealTarget = v => v.kind === 'set' ? { kind: 'set', id: v.id } : v.set ? { kind: 'set', id: v.set.setId } : { kind: 'sheet', id: v.id };
  const sealed = (v, t) => t.kind === 'set' ? !!(v.set && v.set.processReady) : !!v.sheet.processReady;

  /* ── what a sheet still lacks before Laser cutting, in the words of CharmNestReadiness.explain ──────────────────── */
  function explainSteps(rec, env) {
    const R = RD();
    if (!R || typeof R.explain !== 'function') return {};
    try { return Object.fromEntries(R.explain(rec, { kind: 'sheet', rows: env.rows || [] }).steps.map(t => [t.key, t])); } catch (_) { return {}; }
  }
  /** One sheet's own gaps: needs (hard), steps and auto (the move does it), checks (already passed). */
  function sheetGaps(rec, env) {
    const L = laserOf(rec), st = L.stages || {}, id = sid(rec), label = sheetName(rec), g = { needs: [], auto: [], steps: [], checks: {} };
    const ex = explainSteps(rec, env);
    const need = (key, lab, detail, items, n, of) => g.needs.push({ key, lab, n: n || 1, of: of || 0, detail, items: (items || []).slice(0, LISTED), sheet: label });
    if (held(rec)) {
      g.auto.push({ key: 'release:' + id, label: `${label} released from its hold`, detail: `Held back${rec.laserHold.by ? ' by ' + rec.laserHold.by : ''}; every approval and seal it had is kept.` });
      g.steps.push({ type: 'release', sheetIds: [id], key: 'release:' + id });
    }
    if (!joined(rec)) need('membership', () => `${label} is not in a set yet`, rec.draft ? 'It joins a set when it is full, or when you drop it on a set.' : 'It is not included in its set. Include it in a set first.');
    // layout: a verified layout the page is not changing, and (Rose Gold) a calculated green dash line
    if (!st.layout) {
      const lineOnly = lineHeld(rec) && rec.verification?.ok === true && L.total > 0;     // (held for its line and nothing else: roseConfirm asks for it, or says why it cannot)
      if (!lineOnly) {
        const working = !!(rec.saving || ['nesting', 'finishing', 'queued'].includes(rec.status));
        const why = L.total === 0 ? 'No charms are placed on it.' : rec.status === 'error' ? 'Nesting stopped with an error: nest this sheet again.' : working ? 'The sheet is still being nested or saved. Try again when it is done.' : rec.verification?.ok === false ? 'The layout check found a problem: open the sheet and nest it again.' : rec.verification?.ok !== true ? 'The layout has not been checked yet.' : 'The sheet is being changed.';
        need('nesting', () => 'Layout not ready', ex.nesting && ex.nesting.state !== 'done' && ex.nesting.detail ? ex.nesting.detail : why, ex.nesting && ex.nesting.items);
      }
    } else g.checks.layout = { ok: true };
    if (st.layout && !st.front) need('files', () => 'Cutting files missing', 'The cutting file (.ai) or the sheet picture is not saved yet.');
    else if (!st.layout && !st.front) need('files', () => 'Cutting files missing', 'The cutting file (.ai) or the sheet picture is not saved yet.');
    // Engraving is the approved backs AND their saved files (round 13: the Back files step is part of it; the gate is the same). The files are
    // saved right after an approval and it normally takes seconds, so the gap says so in plain words.
    const engFiles = ex.engraving && ex.engraving.items ? ex.engraving.items.filter(i => i.part === 'files') : [];
    if (!st.approval) need('engraving', n => `${count(n, 'back engraving')} not approved`, ex.engraving && ex.engraving.detail || `${count(L.waiting, 'back engraving')} still need approval.`, ex.engraving && ex.engraving.items, Math.max(1, L.waiting));
    else if (!st.backs) {
      const failed = !!(ex.engraving && ex.engraving.state === 'blocked');
      need('engravingFiles', (n, of) => failed ? 'A saved back file failed its check' : `Saving back files: ${Math.max(0, of - n)} of ${of}`,
        failed && ex.engraving.detail || 'Each approved back is saved as a file before the sheet can be cut. This normally takes a few seconds.', engFiles, Math.max(1, L.required - L.saved), L.required);
    } else if (L.required) g.checks.engraving = { ok: true, a: L.approved, r: L.required };
    // the QR label belongs to Order check (it covers every order on the sheet): made by the press when it can be, else a gap with its one shortcut
    if (!st.qr) {
      const orders = (rec.orders || (rec.label && rec.label.orders) || []).length, rose = rec.metal === 'rose' && !rec.roseCutAt, can = !rose && orders > 0 && rec.verification?.ok === true && !!(rec.outputs && (rec.outputs.ai || rec.outputs.preview)) && env.canRelabel !== false;
      if (can) {
        g.auto.push({ key: 'qrLabel:' + id, label: `${label}: QR label made`, detail: `A QR label for ${count(orders, 'order')} is made and saved with the sheet.`, stamp: true });
        g.steps.push({ type: 'qrLabel', sheetId: id, key: 'qrLabel:' + id });
      } else need('qrLabel', () => 'QR label not made yet', rose ? 'A Rose Gold sheet gets its QR label when Cut Sheet is pressed: open it on the Nest tab and press Cut Sheet there.' : orders ? 'The QR label cannot be made from here: open the sheet and press Make QR label.' : 'There is no order on this sheet to put on a label.', ex.orders && ex.orders.items ? ex.orders.items.filter(i => i.part === 'qr') : []);
    }
    // an order waits for every piece of it. A piece on this sheet, or on a sheet of the same move, is no wait of its own:
    // that sheet's own gaps are listed (or done) above, and the order passes when they are
    const together = id => !!(env.own && env.own.has(id));
    const blockers = Object.entries(rec.orderReadiness || {}).filter(([, v]) => v && v.ready !== true && !together(v.sheetId));
    if (!st.orders && blockers.length) {
      need('orders', n => `${count(n, 'order')} waiting for other pieces`, st.qr && ex.orders && ex.orders.detail || 'Every piece of an order must be ready before any of it is cut.',
        ex.orders && ex.orders.items && ex.orders.items.filter(i => i.part !== 'qr').length ? ex.orders.items.filter(i => i.part !== 'qr') : blockers.map(([oid, v]) => ({ kind: 'order', id: oid, label: `Order ${oid}`, why: v.why || 'Another piece of this order is not ready' })), Math.max(1, blockers.length));
    } else if (Object.keys(rec.orderReadiness || {}).length && !blockers.length) g.checks.orders = { ok: true };
    return g;
  }
  // the needs of several sheets as one line each: counts add up, the sheets are named in the words when there are several
  function foldNeeds(list, several) {
    const out = new Map();
    for (const n of list) {
      const e = out.get(n.key) || { key: n.key, lab: n.lab, n: 0, of: 0, detail: [], items: [], sheets: [] };
      e.n += n.n; e.of += n.of; e.detail.push(several ? `${n.sheet}: ${n.detail}` : n.detail); e.items.push(...n.items); e.sheets.push(n.sheet);
      out.set(n.key, e);
    }
    return [...out.values()].map(e => ({ key: e.key, label: e.lab(e.n, e.of) + (several && e.sheets.length > 1 && !/\d/.test(e.lab(e.n, e.of)) ? ` (${e.sheets.length} sheets)` : ''), detail: e.detail.join(' '), items: e.items.slice(0, LISTED) }));
  }
  // the checks that passed on every sheet, said once ("Layout verified", not once a sheet)
  function checkLines(gaps, mine) {
    const out = [], every = t => gaps.every(g => g.checks[t] && g.checks[t].ok), n = mine.length, tail = n > 1 ? ` · ${count(n, 'sheet')}` : '';
    const sum = (t, k) => gaps.reduce((a, g) => a + ((g.checks[t] && g.checks[t][k]) || 0), 0);
    if (every('layout')) out.push({ key: 'check:layout', label: 'Layout verified' + tail, detail: 'The saved layout passed its check.', check: true });
    if (every('engraving')) out.push({ key: 'check:engraving', label: `Back engravings approved · ${sum('engraving', 'a')} of ${sum('engraving', 'r')}`, detail: 'Every back engraving on it is approved, and its back file is saved.', check: true });
    if (every('orders')) out.push({ key: 'check:orders', label: "Every order's other pieces ready" + tail, detail: 'Nothing on it waits for a piece on another sheet.', check: true });
    return out;
  }

  /* ── a set advances as ONE: its gate ───────────────────────────────────────────────────────────────────────────
   * CharmNestReadiness.setGate over the records of the set (the sheets and the set as flowState answers them): every sheet of it
   * ready to be approved, or none is. The button, the page's grey reason and the server's refusal read the same function. */
  function setGateOf(v) {
    const R = RD();
    if (!v.set || !R || typeof R.setGate !== 'function') return null;
    return R.setGate(v.set, v.members);
  }
  function gateNeed(v, gate) {
    return { key: 'setGate', label: gate.reason, detail: `${setName(v.set)} is approved together, so every sheet of it must be ready first.`,
      items: gate.blockers.slice(0, LISTED).map(b => ({ kind: 'sheet', id: b.sheetId, label: b.sheetLabel, why: b.why })) };
  }

  /* ── planning ───────────────────────────────────────────────────────────────────────────────────────────────── */
  const blank = (kind, id, move) => Object.defineProperty({ ok: false, kind, id, move, from: { area: null, setId: null }, to: { area: null, setId: null }, auto: [], needs: [], confirm: [], notes: [] }, 'steps', { value: [], writable: true, enumerable: false });
  const nameOf = env => env.by ? env.by : 'you';
  const finish = plan => { plan.ok = plan.needs.length === 0; return plan; };
  /**
   * The plan of one move over what the records say. Pure: state in, plan out; plan.steps (not enumerable) is what commit runs.
   * env: by · rows (the page's order rows) · rose (LibraryFlowRose.check's answer, when that module is there) ·
   *      canRelabel · canJoin (the sheet window's own paths are on this page)
   */
  function planMove(state, move, env = {}) {
    const kind = move.kind === 'set' ? 'set' : 'sheet', id = String(move.id || ''), to = normTo(move.to), plan = blank(kind, id, { kind, id, to, ...(move.together ? { together: true } : {}) });
    const v = view(state, kind, id);
    if (v.error) { plan.needs.push({ key: 'missing', label: v.error, detail: 'It may have been removed. Refresh the Library.', items: [] }); return finish(plan); }
    const from = areaOf(v);
    plan.from = { area: from, setId: v.set ? v.set.setId : null, label: AREA[from] };
    if (to.set || to.newSet) return planMembership(state, v, to, plan, env);
    // dropped on another sheet (Paul, 7 Oct: "I can not drag/drop these to combine them"): both go into the open set, or into a new one when none is open
    if (to.sheet) return planMembership(state, v, env.dest || combineDest(state, v), plan, { ...env, also: [String(to.sheet)] });
    if (!AREAS.includes(to.area)) { plan.needs.push({ key: 'target', label: 'That is not a place a sheet can go', detail: 'Drop it on In progress, Laser cutting, Completed or a set.', items: [] }); return finish(plan); }
    plan.to = { area: to.area, setId: plan.from.setId, label: AREA[to.area] };
    // a sheet of a committed set that is not completed, dropped on In progress, is TAKEN OUT of the set (Paul, 7 Oct)
    if (to.area === 'progress' && v.kind === 'sheet' && v.set && inSet(v.sheet) && committedSet(v.set) && !(+v.set.laserDoneAt > 0)) return planLeave(state, v, plan, env);
    if (from === to.area) { plan.noop = true; plan.notes.push(`${v.label} is already in ${AREA[from]}.`); return finish(plan); }
    const grouped = v.kind === 'sheet' && !!v.set && v.set.sheetIds.length > 1;
    // A set advances as ONE (Paul, 5 Oct, round 7): going forward, a sheet of a set of several is approved and moved WITH its set, so
    // one press or one drop covers every sheet (never half a set), and the set's own gate (CharmNestReadiness.setGate: every sheet
    // ready to be approved, the lone button's hard test) is read first. Going back is a hold on the sheet moved, as before.
    const withSet = grouped && from === 'progress', mine = v.kind === 'set' || withSet ? v.members : [v.sheet];
    if (grouped) plan.notes.push(withSet ? `${v.label} is approved with ${setName(v.set)}: its sheets move together.` : `${v.label} travels with ${setName(v.set)}: its sheets are cut together.`);
    if (from === 'progress') {               // forwards: Laser cutting, or Completed through it
      const several = !!v.set && (v.set.sheetIds || []).length > 1, gate = several ? setGateOf(v) : null;
      const gaps = mine.map(m => sheetGaps(m, { ...env, own: new Set(v.members.map(sid)) }));
      plan.needs.push(...foldNeeds(gaps.flatMap(g => g.needs), (v.kind === 'set' || withSet) && mine.length > 1));
      for (const g of gaps) { plan.steps.push(...g.steps); plan.auto.push(...g.auto); }
      plan.auto.push(...checkLines(gaps, mine));
      if (v.setMissing) plan.needs.push({ key: 'set', label: `The set of ${v.label} could not be read`, detail: 'Refresh the Library and try again.', items: [] });
      if (v.kind === 'set' && !mine.length) plan.needs.push({ key: 'members', label: 'This set has no sheets', detail: 'A set without sheets cannot be cut.', items: [] });
      for (const i of v.missing) plan.needs.push({ key: 'members', label: 'A sheet of this set cannot be found', detail: `Sheet ${i} is listed in ${setName(v.set)} but is missing or removed.`, items: [] });
      if (gate && !gate.ready) {   // one sheet of the set cannot be approved yet: NO sheet is, and nothing at all is done (no hold lifted, no QR label)
        plan.needs = plan.needs.filter(n => n.key !== 'members');
        plan.needs.unshift(gateNeed(v, gate));
        plan.steps.length = 0; plan.auto = []; plan.confirm = [];
        plan.gate = gate;
        return finish(plan);
      }
      // (a sheet of a set of several is planned WITH its set, above: the other sheets' own gaps are already in `needs`, and what the move does for them is in `auto`)
      roseConfirm(plan, mine, env);                // (adds the roseLine confirm and its step: only an explicit press runs it)
      const t = sealTarget(v);
      if (!sealed(v, t)) plan.auto.push({ key: 'seal', label: `Ready seal recorded by ${nameOf(env)}`, detail: 'The blue seal that says it is ready for Laser cutting, with your name and the time.', stamp: true, seal: { how: 'laserReady' }, ...(env.by ? { by: env.by } : {}) });
      plan.steps.push({ type: 'seal', ...t, key: 'seal' });
      if (to.area === 'completed') addMark(plan, v, env);
      return finish(plan);
    }
    if (from === 'laser' && to.area === 'completed') { addMark(plan, v, env); return finish(plan); }
    if (from === 'completed' && to.area === 'laser') { addReopen(plan, v); return finish(plan); }
    // back to In progress (from Laser cutting, or from Completed through it): held first, so a failed reopen leaves it as it was
    const holdIds = mine.filter(m => !held(m)).map(sid);
    if (holdIds.length) plan.steps.push({ type: 'hold', sheetIds: holdIds, key: 'hold', note: `Moved back to In progress from ${AREA[from]}` });
    if (from === 'completed') addReopen(plan, v);
    if (holdIds.length) plan.auto.push({ key: 'hold', label: `${v.kind === 'set' || mine.length === 1 ? v.label : 'The sheets'} held back from Laser cutting`, detail: 'Every approval, seal and cut record is kept. Move it to Laser cutting again when it is ready.' });
    if (grouped) plan.notes.push(`${setName(v.set)} goes back to In progress with it.`);
    plan.notes.push('Seals and cut records stay on record; this move adds its own entry to the history.');
    return finish(plan);
  }
  /* Rose Gold: a move never adds a green dash line. The line is a yes the person gives (`roseLine`), worked out by
     LibraryFlowRose.calculate only after that yes. A sheet that calculation could not be given a line (not open on this page,
     its layout still saving) is a hard need instead, with the one place it can be done: Cut Sheet on the Nest tab. */
  const roseAnswer = env => env.rose && typeof env.rose === 'object' && !env.rose.error ? env.rose : null;
  // true when a Rose Gold sheet that needs a line cannot be given one from here (a need is added: no yes is asked for)
  function roseBlocked(plan, want, env) {
    const R = roseAnswer(env); if (!R) return false;
    const sheets = (R.sheets || []).filter(x => x && x.needsLine);
    const away = sheets.filter(x => x.source !== 'live'), busy = sheets.filter(x => x.source === 'live' && x.blocked);
    const names = l => l.length === 1 ? l[0].label + ' is' : l.map(x => x.label).join(', ') + ' are';
    if (away.length) plan.needs.push({ key: 'roseOpen', label: `${names(away)} not open on this page`, detail: 'It needs a green dash line before it can be cut, and a move never adds one for a sheet it cannot see. Open it on the Nest tab and press Cut Sheet there.',
      items: away.slice(0, LISTED).map(x => ({ kind: 'sheet', id: x.sheetId, label: x.label, why: x.why || 'Open it on the Nest tab' })) });
    if (busy.length) plan.needs.push({ key: 'roseBusy', label: `${names(busy)} still being nested or saved`, detail: 'Wait for that to finish, or open it on the Nest tab and press Cut Sheet there.',
      items: busy.slice(0, LISTED).map(x => ({ kind: 'sheet', id: x.sheetId, label: x.label, why: x.blocked })) });
    return away.length > 0 || busy.length > 0;
  }
  // the yes for the green dash line: the Rose Gold module's own words when it answered, else ours
  function roseLineConfirm(want, env) {
    const R = roseAnswer(env), c = R && R.confirm && R.confirm.key === 'roseLine' ? R.confirm : null, names = want.map(sheetName);
    // which sheets, and how many charms each has to be lined (the Library's green line window says it; a screen that does not need it ignores it)
    const sheets = ((R && R.sheets) || []).filter(x => x && x.needsLine).map(x => ({ sheetId: x.sheetId, label: x.label, metal: x.metal, charms: x.needs || x.charms || 0 }));
    const who = { count: want.length, sheetIds: want.map(sid), ...(sheets.length ? { sheets } : {}) };
    return c ? { key: 'roseLine', label: c.label, detail: c.detail, ...who } : { key: 'roseLine', label: `Add the green dash line to ${names.length === 1 ? names[0] : count(names.length, 'sheet')}?`, detail: `This calculates the cut contour for ${names.length === 1 ? 'these charms' : 'the charms on ' + names.join(', ')}. Nothing is added until you press the button.`, ...who };
  }
  /* The safety net (GF1, Paul 7 Oct: a 14K sheet moved to Laser cutting with no question about its green line, then blocked on it). A sheet the move
     carries that still owes its line must never go on silently. When the line check lists it (a line can be made) the one yes is asked (below).
     When the check says there is nothing to add, or cannot say (the sheet could not be read, it is not in the answer), the sheet goes on only if
     readiness itself does not hold it for its line (lineHeld, the very test readiness asks: it holds no physical sheet, so it is a full sheet that
     takes the rest of the metal whole). Otherwise the move is refused with ONE plain reason that names the sheet and the one place to fix it, and
     writes nothing (a need, so commit and approve stop before any step). Returns true when it added that need. */
  function lineStuck(plan, want, R) {
    const said = new Map(((R && R.sheets) || []).filter(x => x && x.sheetId).map(x => [x.sheetId, x]));
    const stuck = want.filter(s => { const e = said.get(sid(s)); return !(e && e.needsLine) && (!e || !!e.unknown || lineHeld(s)); });
    if (!stuck.length) return false;
    const one = stuck.length === 1, names = stuck.map(sheetName), why = s => { const e = said.get(sid(s)); return (e && e.why) || 'Open it on the Nest tab'; };
    const full = stuck.every(s => { const e = said.get(sid(s)); return !!(e && !e.unknown && e.full); });
    plan.needs.push({ key: 'roseLineStuck', label: `${joinNames(names)} still ${one ? 'needs' : 'need'} the green dash line`,
      detail: full ? `${one ? 'It looks' : 'They look'} full, so no line is to be added, but the plan that says so is not saved yet, and a sheet waiting for its line is not ready. Open ${one ? 'it' : 'them'} on the Nest tab: ${one ? 'it is' : 'they are'} saved there by itself. Nothing was changed.`
        : `The line check could not make ${one ? 'it' : 'them'} from here, so nothing was moved or written. Open ${one ? 'it' : 'them'} on the Nest tab and press Cut Sheet there.`,
      items: stuck.slice(0, LISTED).map(s => ({ kind: 'sheet', id: sid(s), label: sheetName(s), why: why(s) })) });
    return true;
  }
  const joinNames = l => l.length <= 1 ? l[0] || '' : l.slice(0, -1).join(', ') + ' and ' + l[l.length - 1];
  function roseConfirm(plan, mine, env) {
    const want = mine.filter(needsRoseLine);
    if (!want.length) return;
    const R = roseAnswer(env);
    let ask = want;
    if (R) {
      if (lineStuck(plan, want, R)) return;                // (a sheet readiness holds for its line, or one that cannot be read: refused, no yes)
      const listed = new Set((R.sheets || []).filter(x => x && x.needsLine).map(x => x.sheetId));
      ask = want.filter(s => listed.has(sid(s)));          // (the sheets a line can be made for: a full or lined one in the same move is left out)
      if (!ask.length) return;
    }
    if (roseBlocked(plan, ask, env)) return;               // (pressing could only be refused: no yes is asked for)
    plan.confirm.push(roseLineConfirm(ask, env));
    plan.steps.push({ type: 'roseLine', item: plan.move ? { kind: plan.move.kind, id: plan.move.id } : null, sheetIds: ask.map(sid), key: 'roseLine', label: 'Green dash line calculated' });
  }
  /* A Rose Gold sheet into a set (Paul: "dragging and dropping a rose gold sheet between sets"). Joining a set IS the Cut Sheet
     press for it (RoseStock.record: the sheet joins the set, its green dash line is drawn and dated when it has none, and the
     cut is recorded, which is permanent), so it asks for its own yes (`roseSet`) and, when a line is still to be added, the
     line's yes (`roseLine`). Neither is ever assumed: commit runs it only when both were given. */
  function roseJoin(plan, v, s, T, to, live, env) {
    const R = roseAnswer(env), full = !!(live.rose && live.rose.full), line = !full && needsRoseLine(s) && !(R && R.needsLine === false);
    if (line && roseBlocked(plan, [s], env)) return;
    const name = sheetName(s), target = to.newSet ? 'a new set' : setName(T);
    plan.auto.push({ key: 'membership', label: `${name} added to ${target}`, detail: full ? 'This sheet is full: it takes the rest of the metal whole, so it gets no green dash line and no cut is recorded.' : `Joining a set is the Cut Sheet press: ${line ? 'its green dash line is drawn and dated, and ' : ''}the cut is recorded, and a recorded cut is permanent.` });
    plan.auto.push({ key: 'qrLabel', label: `QR label made for ${target}`, detail: 'The label names the set and the sheet, and covers every order on it.', stamp: true });
    plan.notes.push('Rose Gold joins a set by metal, as Cut Sheet does: every Rose Gold sheet of this run joins the same set (their own green lines are not added).');
    plan.confirm.push({ key: 'roseSet', label: `Add ${name} to ${target}?`, detail: full ? 'Rose Gold joins a set the way Cut Sheet does. This sheet is full, so no green dash line is added and no cut is recorded.' : 'Rose Gold joins a set the way Cut Sheet does, with its green dash line. The cut is recorded with it, and a recorded cut is permanent.' });
    if (line) plan.confirm.push(roseLineConfirm([s], env));
    plan.steps.push({ type: 'roseJoin', sheetId: v.id, setId: T ? T.setId : null, newSet: !!to.newSet, line, full, key: 'membership' });
  }
  function addMark(plan, v, env) {
    plan.auto.push({ key: 'mark', label: `Marked completed by ${nameOf(env)}`, detail: 'The completed seal is stamped with your name and the time, and its orders get their milestone.', stamp: true, seal: { how: 'laserDone' }, ...(env.by ? { by: env.by } : {}) });
    plan.steps.push({ type: 'mark', kind: v.kind, id: v.id, done: true, key: 'mark' });
    if (v.kind === 'sheet' && v.set && v.set.sheetIds.length > 1) {
      const rest = v.members.filter(m => sid(m) !== v.id && !(+m.laserDoneAt > 0));
      if (rest.length) plan.notes.push(`It stays with ${setName(v.set)} until the other ${count(rest.length, 'sheet')} ${rest.length === 1 ? 'is' : 'are'} completed too.`);
    }
  }
  function addReopen(plan, v) {
    plan.auto.push({ key: 'reopen', label: 'Completion taken back', detail: 'Its completed seal and cut record stay on record; it returns to Laser cutting with its approvals.' });
    plan.steps.push({ type: 'mark', kind: v.kind, id: v.id, done: false, key: 'reopen' });
    if (v.kind === 'sheet' && v.set && v.set.sheetIds.length > 1) plan.notes.push(`${setName(v.set)} returns to Laser cutting with it.`);
  }

  /* A sheet into a set, or into a new one. A set is made by its run (a sheet joins the open one by the release rules, a full
     sheet always does), so a drop is the sheet window's own Include / Make QR label for a sheet the open run holds here, and
     is refused, with the reason, for every set that is fixed. */
  function planMembership(state, v, to, plan, env) { return withCardinal(state, v, to, planMembershipBase(state, v, to, plan, env), env); }
  function planMembershipBase(state, v, to, plan, env) {
    const need = (key, label, detail, items) => plan.needs.push({ key, label, detail, items: items || [] });
    plan.to = { area: null, setId: to.set || null, label: to.newSet ? 'A new set' : 'The set' };
    if (v.kind !== 'sheet') { need('wholeSet', 'A set cannot go into a set', 'Move its sheets one at a time.'); return finish(plan); }
    const s = v.sheet, T = to.set ? state.sets[to.set] : null, live = (state.live || {})[v.id] || null, run = (state.runs || {})[s.runId] || null;
    if (to.set && !T) { need('noSet', 'That set could not be found', 'Refresh the Library and try again.'); return finish(plan); }
    if (T) plan.to = { area: null, setId: T.setId, label: setName(T) };
    if (T && inSet(s) && T.setId === s.setId) { plan.noop = true; plan.notes.push(`${v.label} is already in ${setName(T)}.`); return finish(plan); }
    if (s.roseCutAt) { need('sheetCut', `${v.label} was already cut`, 'A recorded cut is permanent: a cut sheet stays in the set it was cut in.'); return finish(plan); }
    if (+s.laserDoneAt > 0) { need('sheetCompleted', `${v.label} is completed`, 'It keeps its cut record and set until it is moved back to Laser cutting.'); return finish(plan); }
    // a committed set takes and gives sheets (Paul, 7 Oct): its own rules, below
    if ((T && committedSet(T)) || (inSet(s) && committedSet(state.sets[s.setId]))) return committedMembership(state, v, to, plan, env);
    if (T && +T.laserDoneAt > 0) need('setCompleted', `${setName(T)} is completed`, 'A completed set takes no more sheets.');
    else if (T && committedSet(T)) need('setCommitted', `${setName(T)} is already committed`, 'The next set takes new sheets.');
    else if (T && /superseded/.test(String(T.status || ''))) need('setClosed', `${setName(T)} was replaced`, 'Drop it on a current set.');
    if (plan.needs.length) return finish(plan);
    const from = inSet(s) ? state.sets[s.setId] || null : null, fromName = from ? setName(from) : 'its set';
    if (from && committedSet(from)) {
      need('leaveCommitted', `${fromName} is committed to the station`, `Moving it out would undo that commit: press Undo set on ${fromName}'s card first.`);
      return finish(plan);
    }
    if (run && run.open && !(live && live.runHere)) { need('runElsewhere', 'Its run is open on another screen', 'Move it from that screen, so the two do not undo each other.'); return finish(plan); }
    if (!live) { need('fixedSet', `${from ? fromName : v.label} is closed`, from ? 'Its run is finished, so its sheets stay together.' : 'Its run is finished, so it stays as it is.'); return finish(plan); }
    const open = live.dispatchSetId || null, openName = open && state.sets[open] ? setName(state.sets[open]) : 'the open set';
    if (inSet(s)) { need('runOwned', `Already in ${state.sets[s.setId] ? setName(state.sets[s.setId]) : 'a set'}`, 'A sheet in a set cannot be moved to another one.'); return finish(plan); }
    if (to.set && T.setId !== open) need('notOpenSet', `${setName(T)} is not the open set`, open ? `Drop it on ${openName}, the set that is open now.` : 'There is no open set; drop it on New set.');
    else if (to.newSet && open) need('openSetExists', `${openName} is still open`, `Drop the sheet on ${openName}.`);
    else if (live.can && live.can.ok === false) need('include', 'Not ready to join a set', live.can.reason || 'Nest and verify it first.');
    else if (s.metal === 'rose' ? env.canRose === false : env.canJoin === false) need('join', 'Joining a set is not available here', s.metal === 'rose' ? 'Open the sheet on the Nest tab and press Cut Sheet there.' : 'Open the sheet and use Include or Make QR label.');
    if (plan.needs.length) return finish(plan);
    // a sheet dropped on another sheet (env.also): the other one must be able to join the same set, or nothing is done at all
    if ((env.also || []).length && s.metal === 'rose') { need('roseCombine', `${v.label} is a Rose Gold sheet`, 'Rose Gold joins a set by its own Cut Sheet press and its own yes: drop it on the set, then add the other sheet.'); return finish(plan); }
    const mates = alsoCheck(state, v, T, plan, env, need);
    if (plan.needs.length) return finish(plan);
    if (s.metal === 'rose') { roseJoin(plan, v, s, T, to, live, env); return finish(plan); }
    const target = to.newSet ? 'a new set' : setName(T);
    plan.auto.push({ key: 'membership', label: `${v.label} added to ${target}`, detail: live.can && live.can.byHand ? 'Released as it stands, with the QR label of its orders.' : 'Included in the set by its own Include switch.' });
    plan.auto.push({ key: 'qrLabel', label: `QR label made for ${target}`, detail: 'The label names the set and the sheet, and covers every order on it.', stamp: true });
    roseConfirm(plan, [s], env);
    plan.steps.push({ type: 'include', sheetId: v.id, setId: T ? T.setId : null, newSet: !!to.newSet, key: 'membership' });
    // (the sheets that share an order with this one come with it: withCardinal, below, says so and asks for the one yes; a page
    //  that cannot say who they are still never offers "only this sheet")
    const dropped = new Set(mates.map(c => c.label));       // (a sheet dropped on is named by the drop itself: no yes is asked about it)
    for (const sp of live.split || []) if (!dropped.has(sp.label)) plan.confirm.push({ key: 'splitOrders', label: `${count(sp.orders.length, 'order')} also on ${sp.label}`, detail: `${sp.orders.slice(0, 4).map(o => 'Order ' + o).join(', ')}${sp.orders.length > 4 ? ' and more' : ''} ${sp.orders.length === 1 ? 'is' : 'are'} on both sheets, so ${sp.label} joins ${target} with ${v.label}.` });
    for (const c of mates) {          // (the sheets dropped on: each joins the same set right after this one, by its own Include / release)
      plan.auto.push({ key: 'membership:' + c.sheetId, label: `${c.label} added to ${target}`, detail: c.live.can && c.live.can.byHand ? 'Released as it stands, with the QR label of its orders.' : 'Included in the set by its own Include switch.' });
      plan.steps.push({ type: 'include', sheetId: c.sheetId, setId: T ? T.setId : null, newSet: false, also: true, key: 'membership:' + c.sheetId });
      for (const sp of c.live.split || []) if (sp.label !== v.label && !dropped.has(sp.label) && !plan.confirm.some(x => x.key === 'splitOrders')) plan.confirm.push({ key: 'splitOrders', label: `${count(sp.orders.length, 'order')} also on ${sp.label}`, detail: `${sp.orders.slice(0, 4).map(o => 'Order ' + o).join(', ')}${sp.orders.length > 4 ? ' and more' : ''} ${sp.orders.length === 1 ? 'is' : 'are'} on both sheets, so ${sp.label} joins ${target} with ${c.label}.` });
    }
    return finish(plan);
  }
  /* ── the sheets of a COMMITTED set (Paul, 7 Oct 2026: "The User should be able to add/remove individual sheets from a set of sheets even in
     the Laser cutting process as long as: A. there are NO shared pieces from a multi-piece order on that particular sheet shared with other
     sheets in the same Set. B. The sheet has NOT been marked by the user as completed (Laser cut)").
     charm-nest-set-edit.js holds the one rule and the words; the server (op flowApply, step setMember) checks it again inside its
     transaction. A sheet dropped on In progress is TAKEN OUT of its committed set; a sheet dropped on a committed set is PUT IN (with
     the sheets it shares an order with, in one yes); a sheet of one committed set dropped on another goes straight over, both ways
     checked, in one step. The rest of the set stays committed with its seals, approvals and place; the sheet put in joins as it is
     (its own approvals and seals are kept, none are added), so the set goes back to In progress until every sheet of it is ready. ── */
  function planLeave(state, v, plan, env) {
    const need = (key, label, detail, items) => plan.needs.push({ key, label, detail, items: items || [] });
    const s = v.sheet, T = v.set, name = v.label, tn = setName(T), items = env.sharedItems || [], SEd = SE();
    plan.to = { area: 'progress', setId: null, label: AREA.progress }; plan.leave = true; plan.committed = true;
    const cut = SEd && SEd.cutReason(s);
    if (cut) { need(cut.key, cut.label, cut.detail); return finish(plan); }
    const rest = v.members.filter(m => sid(m) !== v.id);
    if (!rest.length) { need('lastSheet', `${name} is all that is in ${tn}`, `A set keeps at least one sheet: press Undo set on ${tn}'s card to take it apart.`); return finish(plan); }
    if (items.length) {
      plan.shared = items; plan.group = [name].concat(items.flatMap(i => i.there || []));
      need('sharedOrders', 'Orders shared with another sheet', SEd ? SEd.sharedWords(items, 'they stay in one set') : 'Sheets that share an order stay in one set.', items.map(i => ({ ...i, why: `on ${[i.here].concat(i.there || []).filter(Boolean).join(' and ')}` })));
      plan.notes.push(`To hold ${tn} back as a whole instead, drop the set on In progress.`);
      return finish(plan);
    }
    const left = rest.every(m => +m.laserDoneAt > 0);
    plan.auto.push({ key: 'membership', label: `${name} taken out of ${tn}`, detail: `${tn} stays committed with its other ${count(rest.length, 'sheet')}, keeping its seals, its approvals and its place. This sheet keeps every seal and cut record it has.` });
    plan.auto.push({ key: 'hold', label: `${name} held back from Laser cutting`, detail: 'It goes back to In progress so the open run does not put it in another set by itself. Drop it on Laser cutting to release it.' });
    plan.auto.push({ key: 'setFiles', label: `${tn}'s labels PDF and manifest made again without it`, detail: `The QR label of ${name} comes out of the set's files; the other sheets keep theirs.`, stamp: true });
    plan.confirm.push({ key: 'leaveSet', label: `Take ${name} out of ${tn}?`, detail: `${tn} was committed to the Design Station and stays so. Its orders on ${name} stay marked design-complete there.` });
    plan.notes.push(left ? `Only completed sheets are left in ${tn}, so it is completed with this one gone.` : `${tn} goes on as one set with its other ${count(rest.length, 'sheet')}.`);
    plan.notes.push('Seals and cut records stay on record; this move adds its own entry to the history.');
    plan.steps.push({ type: 'setMember', moves: [{ sheetId: v.id, to: null }], expect: { [v.id]: { setId: T.setId } }, key: 'membership' });
    plan.steps.push({ type: 'setFiles', setId: T.setId, key: 'setFiles' });
    return finish(plan);
  }
  function committedMembership(state, v, to, plan, env) {
    const need = (key, label, detail, items) => plan.needs.push({ key, label, detail, items: items || [] });
    const s = v.sheet, T = to.set ? state.sets[to.set] : null, from = inSet(s) ? state.sets[s.setId] || null : null, name = v.label, SEd = SE();
    const fromName = from ? setName(from) : 'its set', toName = T ? setName(T) : 'a new set';
    plan.committed = true;
    if (from && !committedSet(from)) { need('runOwned', `${fromName} is still being made`, `${name} is in ${fromName}, which its run is making: take it out of ${fromName} first, then drop it on ${toName}.`); return finish(plan); }
    if (!T || !committedSet(T)) { need('leaveCommitted', `${fromName} is committed`, `Take ${name} out of ${fromName} first (drop it on In progress), then drop it on ${T ? toName : 'the open set'}.`); return finish(plan); }
    if (+T.laserDoneAt > 0) need('setCompleted', `${toName} is completed`, 'A completed set takes no more sheets.');
    else if (/superseded/.test(String(T.status || ''))) need('setClosed', `${toName} was replaced`, 'Drop it on a current set.');
    else if (s.metal === 'rose' && !from) need('roseSet', `${name} is a Rose Gold sheet`, 'Rose Gold joins a set by its own Cut Sheet press, so it cannot be added to a committed set from here.');
    if (plan.needs.length) return finish(plan);
    const run = (state.runs || {})[s.runId] || null, live = (state.live || {})[v.id] || null;
    if (!from && run && run.open && !(live && live.runHere)) { need('runElsewhere', 'Its run is open on another screen', 'Move it from that screen, so the two do not undo each other.'); return finish(plan); }
    // the other end: a sheet that leaves a committed set keeps no mate behind (A), the sheets that share an order with it come along (the cardinal rule)
    const items = env.sharedItems || [], group = env.group || null, members = group && Array.isArray(group.members) ? group.members : [], by = new Map(members.map(m => [m.id, m]));
    const tied = new Map();
    for (const i of items) (i.thereIds || []).forEach((id, k) => { if (id !== v.id && !tied.has(id)) tied.set(id, { id, label: (i.there || [])[k] || (by.get(id) || {}).label || id, setId: (by.get(id) || {}).setId || null }); });
    for (const m of members) if (m.id !== v.id && !tied.has(m.id) && (m.setId || null) !== T.setId && items.length) tied.set(m.id, { id: m.id, label: m.label, setId: m.setId || null });
    const mates = [...tied.values()].filter(m => (m.setId || null) !== T.setId);
    if (from && items.some(i => (i.thereIds || []).some(id => (by.get(id) || {}).setId === from.setId || (state.sheets[id] && inSet(state.sheets[id]) && state.sheets[id].setId === from.setId)))) {
      plan.shared = items; plan.group = [name].concat(mates.map(m => m.label));
      need('sharedOrders', 'Orders shared with another sheet', SEd ? SEd.sharedWords(items, 'they stay in one set') : 'Sheets that share an order stay in one set.', items.map(i => ({ ...i, why: `on ${[i.here].concat(i.there || []).filter(Boolean).join(' and ')}` })));
      return finish(plan);
    }
    const fixedOf = new Map(((group && group.fixed) || []).map(f => [f.sheetId, f.why]));
    const stuck = mates.filter(m => fixedOf.has(m.id) || (m.setId && m.setId !== T.setId));
    if (stuck.length) {
      plan.shared = items; plan.group = [name].concat(mates.map(m => m.label));
      need('sharedOrders', 'Orders shared with another sheet', `${SEd ? SEd.sharedWords(items, 'they go into one set together') : 'Sheets that share an order go into one set.'} ${stuck[0].label} cannot come: ${fixedOf.get(stuck[0].id) || 'it is in another set'}.`, items.map(i => ({ ...i, why: `on ${[i.here].concat(i.there || []).filter(Boolean).join(' and ')}` })));
      return finish(plan);
    }
    // what the set does with it: the sheet joins as it is; the set waits in In progress until every sheet of it is ready (it advances as one)
    const tv = view(state, 'set', T.setId), tArea = tv.error ? null : areaOf(tv), gaps = sheetGaps(s, { ...env, own: new Set([v.id, ...members.map(m => m.id)]) });
    const waits = gaps.needs.filter(n => n.key !== 'membership' && n.key !== 'qrLabel'), personHold = held(s) && !/^Taken out of /.test(String(s.laserHold.note || ''));
    const why = waits.length ? waits[0].lab(waits[0].n, waits[0].of) : personHold ? 'it is held back from Laser cutting' : '';
    const label = to.set ? toName : 'the set';
    plan.to = { area: null, setId: T.setId, label: toName };
    plan.auto.push({ key: 'membership', label: from ? `${name} moved from ${fromName} to ${toName}` : `${name} added to ${toName}`, detail: `${toName} stays committed, keeping its seals and its place. ${name} joins as it is: its own approvals and seals are kept, none are added.` });
    for (const m of mates) plan.auto.push({ key: 'membership:' + m.id, label: `${m.label} added to ${toName}`, detail: `It shares ${items.length === 1 ? 'an order' : 'orders'} with ${name}, so it joins with it.` });
    plan.auto.push({ key: 'qrLabel', label: `QR label made for ${toName}`, detail: 'The label names the set and the sheet, and covers every order on it.', stamp: true });
    plan.auto.push({ key: 'setFiles', label: `${toName}'s labels PDF and manifest made again with ${mates.length ? 'them' : 'it'}`, detail: `${from ? `${fromName}'s files are made again without it. ` : ''}The station's copy of the labels follows the set.`, stamp: true });
    if (mates.length) {
      plan.shared = items; plan.group = [name].concat(mates.map(m => m.label));
      const ml = mates.map(m => m.label), together = name;
      plan.confirm.push({ key: 'together', label: ml.length === 1 ? `${ml[0]} joins ${toName} with ${together}` : ml.length <= 3 ? `${ml.slice(0, -1).join(', ')} and ${ml[ml.length - 1]} join ${toName} with ${together}` : `${count(ml.length, 'sheet')} join ${toName} with ${together}`, detail: `${items.length ? SEd.sharedWords(items, 'they go in together') : 'They share an order'}.` });
    }
    if (why && tArea === 'laser') plan.confirm.push({ key: 'setWaits', label: `${toName} will wait for ${name}`, detail: `${name} is not ready yet (${why}). ${label} goes back to In progress until every sheet of it is ready, because a set advances as one. Nothing already approved is lost.` });
    else if (why) plan.notes.push(`${name} is not ready yet (${why}): ${label} waits in In progress until it is.`);
    if (from) plan.notes.push(`${fromName} stays committed with its other sheets.`);
    plan.notes.push(`The orders on ${name} are not marked design-complete at the Design Station: ${toName} was committed before this sheet joined it.`);
    const moves = [{ sheetId: v.id, to: T.setId }].concat(mates.map(m => ({ sheetId: m.id, to: T.setId, pulled: true })));
    plan.steps.push({ type: 'setMember', moves, expect: Object.fromEntries(moves.map(m => [m.sheetId, { setId: m.sheetId === v.id ? (from ? from.setId : null) : null }])), key: 'membership' });
    plan.steps.push({ type: 'relabel', sheetIds: moves.map(m => m.sheetId), setId: T.setId, key: 'qrLabel' });
    plan.steps.push({ type: 'setFiles', setId: T.setId, key: 'setFiles' });
    if (from) plan.steps.push({ type: 'setFiles', setId: from.setId, key: 'setFiles:' + from.setId });
    return finish(plan);
  }
  /* The sheets dropped on (Paul, 7 Oct: combine two sheets that are not in Laser cutting). Each must be a sheet of the open run, not cut,
     not completed, not in a set, not Rose Gold (it joins by its own Cut Sheet press) and ready to join, exactly as the sheet moved: a need
     here means the whole move is refused and nothing is written. Answers [{sheetId, label, live}] for the ones that still have to join. */
  function alsoCheck(state, v, T, plan, env, need) {
    const out = [];
    for (const bid of env.also || []) {
      const bv = view(state, 'sheet', String(bid));
      if (bv.error) { need('noSheet', 'That sheet could not be found', 'Refresh the Library and try again.'); continue; }
      if (bv.id === v.id) continue;
      const b = bv.sheet, B = bv.label, bl = (state.live || {})[bv.id] || null, brun = (state.runs || {})[b.runId] || null;
      if (b.roseCutAt) { need('sheetCut', `${B} was already cut`, 'A recorded cut is permanent: a cut sheet stays in the set it was cut in.'); continue; }
      if (+b.laserDoneAt > 0) { need('sheetCompleted', `${B} is completed`, 'It keeps its cut record and set until it is moved back to Laser cutting.'); continue; }
      if (inSet(b)) {
        if (T && b.setId === T.setId) continue;               // (already in the set: nothing to add)
        const bs = state.sets[b.setId] ? setName(state.sets[b.setId]) : 'a set';
        need('alsoInSet', `${B} is already in ${bs}`, `A sheet in a set cannot be moved to another one: drop ${v.label} on ${bs} instead.`); continue;
      }
      if (b.metal === 'rose' || (bl && bl.rose)) { need('roseCombine', `${B} is a Rose Gold sheet`, 'Rose Gold joins a set by its own Cut Sheet press and its own yes: drop it on the set, not the other way round.'); continue; }
      if (brun && brun.open && !(bl && bl.runHere)) { need('runElsewhere', `${B}: its run is open on another screen`, 'Move it from that screen, so the two do not undo each other.'); continue; }
      if (!bl) { need('fixedSet', `${B} is closed`, 'Its run is finished, so it stays as it is.'); continue; }
      if (bl.can && bl.can.ok === false) { need('include', `${B} is not ready to join a set`, bl.can.reason || 'Nest and verify it first.'); continue; }
      if (env.canJoin === false) { need('join', 'Joining a set is not available here', 'Open the sheet and use Include or Make QR label.'); continue; }
      out.push({ sheetId: bv.id, label: B, live: bl });
    }
    return out;
  }
  /* where a drop on another sheet puts both: the open set when the run has one, else a new set (the one open set of the run is how it works) */
  function combineDest(state, v) { return destOf((state.live || {})[v.id] || null); }

  /* ── the cardinal rule of a Set of Sheets ───────────────────────────────────────────────────────────────────────
     What the records say (state.shared / state.group, op flowState with `move`) and what the page holds (SharedOrders.between /
     groupOf) are both read; an order either one finds split is split. */
  const byOrderId = (a, b) => (a.orderId < b.orderId ? -1 : a.orderId > b.orderId ? 1 : 0);
  function sharedEnv(item, to, state, together) {
    const SO = hooks.shared || root.SharedOrders, safe = (f, d) => { try { return f(); } catch (_) { return d; } };
    const page = SO && typeof SO.between === 'function' ? safe(() => SO.between(item.id, to.set || (to.leave ? null : 'new'), { kind: item.kind, together }), []) || [] : [];
    const by = new Map();
    for (const it of state.shared || []) by.set(it.orderId, it);
    for (const it of page) { const sv = by.get(it.orderId); by.set(it.orderId, sv ? { ...sv, customer: it.customer || sv.customer || '', thumb: it.thumb || sv.thumb || null, pieces: it.pieces && it.pieces.length ? it.pieces : sv.pieces, locked: sv.locked && sv.locked.length ? sv.locked : it.locked || [], kind: it.kind || sv.kind || '', words: it.words || sv.words || '', groups: it.groups || sv.groups } : it); }   // (what the pieces ARE, left/right earrings or discs: the page knows it better than the records)
    const items = [...by.values()].sort(byOrderId);
    if (SO && typeof SO.enrich === 'function') safe(() => SO.enrich(items.filter(i => !i.customer && !i.thumb)), null);
    let group = state.group || null;
    if (!group && SO && typeof SO.groupOf === 'function') group = safe(() => SO.groupOf(item.id), null);
    return { sharedItems: items, group };
  }
  const joinableNow = id => { let l = null; try { l = hooks.live ? hooks.live(id) : null; } catch (_) { /* not live */ } return l; };
  const destOf = lv => lv && lv.dispatchSetId ? { set: lv.dispatchSetId } : { newSet: true };
  /* The sheets dropped on bring their own shared orders (what the page holds: the records twin answers for the sheet moved): both lists are
     merged, so a sheet that shares an order with the one dropped on is asked for too, with the one yes. */
  function alsoEnv(env, also, dest, together) {
    const by = new Map((env.sharedItems || []).map(i => [i.orderId, i])), mem = new Map();
    const take = g => { for (const m of g && Array.isArray(g.members) ? g.members : (g && Array.isArray(g.ids) ? g.ids.map((x, i) => ({ id: x, label: (g.labels || [])[i] || x, setId: null })) : [])) if (!mem.has(m.id)) mem.set(m.id, m); };
    take(env.group);
    for (const b of also) {
      const e = sharedEnv({ kind: 'sheet', id: b }, dest, { shared: [], group: null }, together);
      for (const i of e.sharedItems) if (!by.has(i.orderId)) by.set(i.orderId, i);
      take(e.group);
    }
    return { ...env, sharedItems: [...by.values()].sort(byOrderId), group: mem.size ? { members: [...mem.values()] } : env.group };
  }
  function withCardinal(state, v, to, plan, env) {
    if (v.kind !== 'sheet' || plan.noop || plan.committed) return plan;       // (a committed set's edit carries its own rule: planLeave, committedMembership)
    const items = env.sharedItems || [], group = env.group || null, id = v.id, dest = to.set ? String(to.set) : 'new', T = to.set ? state.sets[to.set] : null;
    const members = group && Array.isArray(group.members) ? group.members : (group && Array.isArray(group.ids) ? group.ids.map((x, i) => ({ id: x, label: (group.labels || [])[i] || x, setId: null })) : []);
    const explicit = new Set((env.also || []).map(String));     // (the sheets dropped on: they travel by the drop itself, they are not "mates" to ask a yes about)
    const partners = members.filter(m => m.id !== id && !explicit.has(m.id) && (m.setId || null) !== dest);
    if (!items.length && !(env.together && partners.length)) return plan;
    // the sheets that would stay on the other side, from the items when the group is not known
    const from = items.length ? [...new Map(items.flatMap(i => (i.thereIds || []).map((x, k) => [x, { id: x, label: (i.there || [])[k] || x, setId: null }]))).values()].filter(m => !explicit.has(m.id)) : [];
    const mates = partners.length ? partners : from;
    if (explicit.size && !mates.length) return plan;             // (the orders only tie the sheet to the one it was dropped on: both go in, no yes)
    const together = [v.label].concat((env.also || []).map(i => state.sheets[i] ? sheetName(state.sheets[i]) : '').filter(Boolean)).join(' and ');
    const target = to.newSet ? 'a new set' : setName(T);
    const orderWords = items.length ? `${items.slice(0, 4).map(i => 'Order ' + i.orderId).join(', ')}${items.length > 4 ? ' and more' : ''}` : 'Orders they share';
    const leaving = inSet(v.sheet), names = [v.label].concat(mates.map(m => m.label));
    const live = mates.map(m => ({ m, l: joinableNow(m.id) }));
    const stuck = live.filter(x => !(x.l && x.l.runHere && x.l.draft && !x.l.rose && (!x.l.can || x.l.can.ok !== false)));
    const joining = !leaving && plan.steps.some(x => x.type === 'include') && plan.needs.length === 0;
    plan.shared = items; plan.group = names;
    if (joining && !stuck.length) {
      plan.confirm = plan.confirm.filter(c => c.key !== 'splitOrders');
      plan.confirm.push({ key: 'together', label: mates.length === 1 ? `${mates[0].label} joins ${target} with ${together}` : mates.length <= 3 ? `${mates.slice(0, -1).map(m => m.label).join(', ')} and ${mates[mates.length - 1].label} join ${target} with ${together}` : `${count(mates.length, 'sheet')} join ${target} with ${together}`,
        detail: items.length === 1 && items[0].words ? `Order ${items[0].orderId} ${items[0].words}, so they go in together.` : `${orderWords} ${items.length === 1 ? 'has' : 'have'} pieces on ${mates.length === 1 ? 'both sheets' : 'these sheets'}, so they go in together.` });
      for (const m of mates) plan.auto.push({ key: 'membership:' + m.id, label: `${m.label} added to ${target}`, detail: `It shares ${items.length === 1 ? 'an order' : 'orders'} with ${v.label}, so it joins with it, with its QR label.` });
      const st = plan.steps.find(x => x.type === 'include'); if (st) st.with = mates.map(m => m.id);
      return finish(plan);
    }
    const lockedWhy = id => { for (const i of items) { const l = (i.locked || []).find(z => z.sheetId === id); if (l) return l.why; } return ''; };
    const why = stuck.map(x => `${x.m.label}: ${lockedWhy(x.m.id) || (x.l ? (x.l.rose ? 'a Rose Gold sheet joins a set only by its own Cut Sheet press' : x.l.can && x.l.can.reason ? x.l.can.reason.replace(/\.$/, '') : !x.l.runHere ? 'it is not on a page of the open run' : 'it cannot join a set yet') : 'it is not open on this page')}`);
    plan.needs.unshift({ key: 'sharedOrders', label: 'Orders shared with another sheet', items: items.map(i => ({ ...i, why: `on ${[i.here].concat(i.there || []).filter(Boolean).join(' and ')}` })),
      // (one short sentence: the shared-orders window shows the orders and sheets; when a sheet cannot come, its own reason)
      detail: leaving ? 'Sheets that share an order stay in one set.' : why.length ? `${why[0]}${why.length > 1 ? ` (and ${why.length - 1} more)` : ''}.` : 'Sheets that share an order go into one set.' });
    return finish(plan);
  }

  /* ── where it may be dropped (sync, from the page) ──────────────────────────────────────────────────────────── */
  function zones(item, page) {
    const it = { kind: item && item.kind === 'set' ? 'set' : 'sheet', id: String(item && item.id || '') };
    const cur = (item && (item.area || item.from)) || (page.area ? page.area(it) : null);
    const own = (item && item.setId) || (page.setOf ? page.setOf(it) : null);
    const out = AREAS.map(a => ({ area: a, name: AREA[a], ok: a !== cur, reason: a === cur ? `It is already in ${AREA[a]}.` : '' }));
    if (it.kind === 'sheet') {
      const done = cur === 'completed';
      // a sheet of a committed set that is not completed can be taken out of it: In progress says so (and is open even for a set that waits there)
      const mine = own && cur !== 'completed' ? (page.sets ? page.sets() : []).find(x => x && x.setId === own) : null;
      if (mine && committedSet(mine) && !(+mine.laserDoneAt > 0)) { const z = out.find(x => x.area === 'progress'); z.leaveSet = true; z.ok = true; z.reason = ''; z.sub = `out of ${setName(mine)}`; }
      for (const st of page.sets ? page.sets() : []) {
        if (!st || !st.setId || st.setId === own) continue;
        const why = done ? 'Move it back to Laser cutting first.' : +st.laserDoneAt > 0 ? `${setName(st)} is completed.` : /superseded/.test(String(st.status || '')) ? `${setName(st)} was replaced.` : '';
        out.push({ set: st.setId, name: setName(st), ok: !why, reason: why });
      }
      const live = page.live ? page.live(it.id) : null;
      // a draft sheet joins the run's ONE open set (or starts it): a set that is open but not that one is dimmed here, in plain words, before any plan is asked for
      if (live && live.draft && !done) {
        const all = page.sets ? page.sets() : [], open = live.dispatchSetId ? all.find(x => x && x.setId === live.dispatchSetId) : null;
        for (const z of out) {
          const st = z.set && z.ok ? all.find(x => x && x.setId === z.set) : null;
          if (st && !committedSet(st) && !(+st.laserDoneAt > 0) && !/superseded/.test(String(st.status || '')) && st.setId !== live.dispatchSetId) { z.ok = false; z.reason = `${setName(st)} is not the open set. ${live.dispatchSetId ? `Drop it on ${open ? setName(open) : 'the open set'}.` : 'Drop it on New set.'}`; }
        }
      }
      const blocked = !!(live && live.can && live.can.ok === false);
      out.push({ newSet: true, name: 'New set', ok: !done && !!(live && live.draft && !live.dispatchSetId) && !blocked, reason: done ? 'Move it back to Laser cutting first.' : !live ? 'Only a sheet of the open run can start a new set.' : !live.draft ? 'It is already in a set.' : live.dispatchSetId ? 'The open set takes it: drop it there.' : blocked ? `Not ready to join a set. ${live.can.reason || 'Nest and verify it first.'}` : '' });
    }
    return out;
  }
  /* A draft sheet dropped on another draft sheet (Paul, 7 Oct: "I can not drag/drop these to combine them. Neither of them are in Laser
     cutting so there should be nothing preventing them from being combined"): both join the open set, or a new one when none is open.
     Answers the one place the pointer is over: { sheet, name, sub, ok, reason }. o: { self, name } the two sheets' names for the words. */
  function sheetZone(item, otherId, page, o = {}) {
    const it = { kind: item && item.kind === 'set' ? 'set' : 'sheet', id: String(item && item.id || '') }, oid = String(otherId || '');
    if (it.kind !== 'sheet' || !it.id || !oid || it.id === oid) return null;
    const a = o.self || 'This sheet', b = o.name || 'That sheet', no = reason => ({ sheet: oid, name: `Combine with ${b}`, ok: false, reason });
    const cur = (item && (item.area || item.from)) || (page.area ? page.area(it) : null);
    if (cur === 'completed') return no('Move it back to Laser cutting first.');
    const la = page.live ? page.live(it.id) : null, lb = page.live ? page.live(oid) : null;
    if (!la) return no(`Only sheets of the open run can be combined: ${a} is not on this page.`);
    if (!la.draft) return no(`${a} is already in a set: drop ${b} on that set to add it.`);
    if (!lb) return no(`Only sheets of the open run can be combined: ${b} is not on this page.`);
    if (!lb.draft) return no(`${b} is already in a set: drop ${a} on that set to add it.`);
    if (la.rose) return no(`${a} is Rose Gold: it joins a set by its own Cut Sheet press, so drop it on the set.`);
    if (lb.rose) return no(`${b} is Rose Gold: it joins a set by its own Cut Sheet press, so drop it on the set.`);
    if (la.runHere === false) return no(`${a} is not on a page of the open run.`);
    if (lb.runHere === false) return no(`${b} is not on a page of the open run.`);
    if (la.can && la.can.ok === false) return no(`${a} is not ready to join a set. ${la.can.reason || 'Nest and verify it first.'}`);
    if (lb.can && lb.can.ok === false) return no(`${b} is not ready to join a set. ${lb.can.reason || 'Nest and verify it first.'}`);
    const open = la.dispatchSetId ? (page.sets ? page.sets() : []).find(x => x && x.setId === la.dispatchSetId) : null;
    return { sheet: oid, name: `Combine with ${b}`, sub: la.dispatchSetId ? `both join ${open ? setName(open) : 'the open set'}` : 'both start a new set', ok: true, reason: '' };
  }
  const keyOfZone = z => z.area ? { area: z.area } : z.set ? { set: z.set } : { newSet: true };

  /* ── the browser's side: reading the cloud, running the steps with the code that owns each ─────────────────────── */
  const mine = {};             // hooks a caller configured: the page never wires over them
  const hooks = {
    api: null,                 // (body) -> Promise<answer>: op of charmNestLibrary
    employee: () => { try { return (root.CNEmployee && root.CNEmployee.name && root.CNEmployee.name()) || ''; } catch (_) { return ''; } },
    ask: () => { try { return (root.CNEmployee && root.CNEmployee.ask && root.CNEmployee.ask()) || ''; } catch (_) { return ''; } },
    rows: () => { try { return (root.Orders && root.Orders.rows && root.Orders.rows()) || []; } catch (_) { return []; } },
    mark: null,                // (kind, id, done, {by}) -> Promise: LibraryDone.mark on the page
    remakeLabel: null,         // (sheetId) -> Promise: the sheet's QR label made again (the sheet window's own path)
    relabelSet: null,          // ({sheetIds, setId}, {by}) -> Promise: the QR labels of sheets put in a committed set (charm-nest-set-edit.js)
    setFiles: null,            // ({setId}, {by}) -> Promise: a committed set's labels PDF, manifest and set.json made again, then written (op flowApply setFiles)
    applyMembership: null,     // (membership) -> Promise: the page's own copies follow what the server wrote (a sheet in or out of a committed set)
    include: null,             // (sheetId, {setId, newSet, split}) -> Promise: the sheet window's own Include / release
    roseJoin: null,            // (sheetId, {by, line, full, onStep}) -> Promise<{ok, sheets, warnings}>: a Rose Gold sheet's Cut Sheet press
    shared: null,              // a SharedOrders-shaped object ({between, groupOf, enrich}); the page's window.SharedOrders when null
    live: null,                // (sheetId) -> null | {runHere, draft, dispatchSetId, can:{ok,reason,byHand}, split:[{label,orders}], rose?:{full}}
    areaOf: null, sets: null, setOf: null,
    sync: null,                // (process) -> void: the page's own records and cards follow what the cloud now says
    rose: () => root.LibraryFlowRose || null,
    cuts: null,                // (metal) -> boolean: which metals get a green dash line (default CharmNestRose.cuts; tests set it)
    wait: ms => new Promise(r => setTimeout(r, ms))
  };
  const cloud = body => {
    if (hooks.api) return hooks.api(body);
    if (root.CN && typeof root.CN.api === 'function') return root.CN.api('charmNestLibrary', body, { quiet: true });
    return Promise.reject(new Error('The Library is not connected to the cloud'));
  };
  const answer = r => { if (r && r.error) throw Object.assign(new Error(r.error), { status: r.status }); return r; };
  async function readState(item, extraSets, move, extraSheets) {
    const r = answer(await cloud({ op: 'flowState', sheetIds: item.kind === 'sheet' ? [item.id, ...(extraSheets || [])] : [], setIds: [...(item.kind === 'set' ? [item.id] : []), ...(extraSets || [])], ...(move ? { move } : {}) }));
    const state = { sheets: {}, sets: {}, runs: r.runs || {}, live: {}, shared: Array.isArray(r.shared) ? r.shared : null, group: r.group || null };
    for (const s of r.sheets || []) state.sheets[sid(s)] = s;
    for (const x of r.sets || []) state.sets[x.setId] = x;
    for (const d of r.setDocs || []) state.sets[d.setId] = { ...(state.sets[d.setId] || {}), ...d };
    if (hooks.live) for (const id of Object.keys(state.sheets)) { let l = null; try { l = hooks.live(id); } catch (_) { /* not live */ } if (l) state.live[id] = l; }
    return state;
  }
  const itemOf = req => ({ kind: req.kind === 'set' ? 'set' : 'sheet', id: String(req.id || '') });
  async function plan(req) {
    req = req || {};
    if (typeof document !== 'undefined' && !hooks.remakeLabel) { try { wirePage(); } catch (_) { /* as it was */ } }   // (the sheet window loads after this file)
    const item = itemOf(req), to = normTo(req.to), together = !!req.together;
    // dropped on another sheet: both go where the open run takes a sheet (its open set, else a new one); the other sheet is read with it
    const also = item.kind === 'sheet' && to.sheet && String(to.sheet) !== item.id ? [String(to.sheet)] : [];
    const dest = also.length ? destOf(joinableNow(item.id)) : to, joining = item.kind === 'sheet' && !!(dest.set || dest.newSet);
    const state = await readState(item, dest.set ? [dest.set] : [], joining ? { kind: item.kind, id: item.id, to: dest.set ? { set: dest.set } : { newSet: true }, together } : null, also);
    // a sheet of a committed set dropped on In progress leaves it (Paul, 7 Oct): the orders it shares with the set's other sheets are read from the records too
    let leaving = false;
    if (item.kind === 'sheet' && to.area === 'progress') { const lv = view(state, 'sheet', item.id); if (!lv.error && lv.set && inSet(lv.sheet) && committedSet(lv.set) && !(+lv.set.laserDoneAt > 0)) { const r = answer(await cloud({ op: 'sharedOrders', kind: 'sheet', id: item.id, to: { leave: true } })); state.shared = r.shared || []; state.group = r.group || null; leaving = true; } }
    let env = { by: req.by || hooks.employee(), rows: hooks.rows(), canRelabel: typeof hooks.remakeLabel === 'function', canJoin: typeof hooks.include === 'function', canRose: typeof hooks.roseJoin === 'function', together, ...(also.length ? { dest } : {}) };
    if (joining || leaving) env = { ...env, ...sharedEnv(item, leaving ? { leave: true } : dest, state, together) };
    if (also.length) env = alsoEnv(env, also, dest, together);
    let p = planMove(state, { ...item, to, ...(together ? { together: true } : {}) }, env);
    const M = hooks.rose();
    if (p.steps.some(x => x.type === 'roseLine' || x.type === 'roseJoin') && M && typeof M.check === 'function') {
      // The check reads EVERY sheet the move carries that owes a line, not only the one dragged: a sheet of a set moves with its set (GF1).
      // (one sheet and nothing else: the item itself, as before)
      const ids = [...new Set(p.steps.filter(x => x.type === 'roseLine').flatMap(x => x.sheetIds || []))];
      const ask = item.kind === 'set' ? (ids.length ? [item, ...ids.map(id => ({ kind: 'sheet', id }))] : item)
        : ids.length === 1 && ids[0] === item.id || !ids.length ? item : ids.map(id => ({ kind: 'sheet', id }));
      try { const c = await M.check(ask); if (c && typeof c === 'object') { env = { ...env, rose: c }; p = planMove(state, { ...item, to, ...(together ? { together: true } : {}) }, env); } } catch (_) { /* the fallback confirm stands */ }
    }
    return p;
  }
  const bySure = by => by || hooks.employee() || hooks.ask();
  async function sync(process) { if (hooks.sync && process && process.length) { try { hooks.sync(process); } catch (_) { /* the next refresh draws it */ } } }
  async function flow(steps, by, extra) { const r = answer(await cloud({ op: 'flowApply', steps, by, device: 'charm-nest-1', via: 'Library move', ...extra })); await sync(r.process); return r; }
  async function markIt(s, by) {
    if (hooks.mark) return hooks.mark(s.kind, s.id, s.done, { by });
    const r = answer(await cloud({ op: 'laserDone', kind: s.kind, id: s.id, done: s.done, by: by || undefined, stage: s.done ? 'laser' : undefined, device: 'charm-nest-1', via: 'Library move' }));
    await sync(r.process); return r;
  }
  /* What the person's press kept, for the Employee efficiency console (charm-nest-laser-act.js, only on a page that has it: the
     Sorter app): one record of the press, never of a step that was taken back, never a reason to stop or wait. */
  function acted(list) { try { const A = root.CNLaserAct; if (A && typeof A.flow === 'function' && list && list.length) A.flow(list); } catch (_) { /* a record of the work never stops the work */ } }
  /**
   * Runs a plan's steps in order. `only` (a Set of step types) limits it to those (approve). A hold or a release is undone
   * when a later step fails, so a failed commit leaves what it found.
   */
  async function run(p, o) {
    const applied = [], undo = [], by = bySure(o.by);
    const tell = (s, label, state) => { try { o.onStep && o.onStep({ key: s.key, label, state }); } catch (_) { /* a listener never stops the move */ } };
    const lineOf = s => s.label || (p.auto.find(a => a.key === s.key) || {}).label || s.key;
    // an applied line carries what its plan line said (detail, stamp), so a screen draws both alike
    const doneLine = (key, label) => { const a = p.auto.find(x => x.key === key) || {}; return { key, label, ...(a.detail ? { detail: a.detail } : {}), ...(a.stamp ? { stamp: true } : {}), ...(a.seal ? { seal: a.seal } : {}), ...(a.by ? { by: a.by } : {}) }; };
    const todo = p.steps.filter(s => !o.only || o.only.has(s.type));
    if (todo.some(s => s.type !== 'qrLabel' && s.type !== 'roseLine' && s.type !== 'include') && !by) throw new Error('Say who is making this change');
    // the green dash line and a Rose Gold sheet's join are checked first: if they cannot be done here, or were not asked for
    // with a yes, nothing else of the move has been done yet
    const joinStep = todo.find(s => s.type === 'roseJoin'), yes = k => (o.confirmed || []).includes(k);
    if (todo.some(s => s.type === 'roseLine') || (joinStep && joinStep.line)) {
      if (!yes('roseLine')) throw new Error('The green dash line is only added when you press its button');
      const M = hooks.rose();
      if (!M || typeof M.calculate !== 'function') throw new Error('The green dash line cannot be calculated from here: open the sheet on the Nest tab and press Cut Sheet there');
    }
    if (joinStep) {
      if (!yes('roseSet')) throw new Error('A Rose Gold sheet is only added to a set when you press its button');
      if (typeof hooks.roseJoin !== 'function') throw new Error('A Rose Gold sheet cannot be added to a set from here: open it on the Nest tab and press Cut Sheet there');
    }
    const warnings = [], kept = [];     // (kept: the steps this run did that stay; o.acts, when given, receives them for one record of the press)
    for (const s of todo) {
      let label = lineOf(s); const extra = [];
      tell(s, label, 'start');
      try {
        if (s.type === 'release') {
          await flow([{ type: 'release', sheetIds: s.sheetIds }], by, { expect: Object.fromEntries(s.sheetIds.map(i => [i, { held: true }])) });
          undo.push({ key: s.key, fn: () => flow([{ type: 'hold', sheetIds: s.sheetIds, note: 'Hold restored: the move was not completed' }], by) });
        } else if (s.type === 'hold') {
          await flow([{ type: 'hold', sheetIds: s.sheetIds, note: s.note }], by, { expect: Object.fromEntries(s.sheetIds.map(i => [i, { held: false }])) });
          undo.push({ key: s.key, fn: () => flow([{ type: 'release', sheetIds: s.sheetIds, restore: true }], by) });
        } else if (s.type === 'qrLabel') {
          if (typeof hooks.remakeLabel !== 'function') throw new Error('The QR label cannot be made from here: open the sheet and press Make QR label');
          await hooks.remakeLabel(s.sheetId);
        } else if (s.type === 'roseLine') {
          // LibraryFlowRose.calculate on the item the move is about (never recordCut: the Cut Sheet button alone records a cut).
          // It re-checks every sheet right before acting and refuses before writing when one cannot be given a line.
          // (every sheet the plan listed: a sheet of a set moves with its set. A set item is calculated as the set, one sheet as that sheet)
          const ids = s.sheetIds || [], what = s.item && s.item.kind === 'set' ? s.item : ids.length === 1 && (!s.item || s.item.id === ids[0]) ? (s.item || { kind: 'sheet', id: ids[0] }) : ids.length ? ids.map(id => ({ kind: 'sheet', id })) : s.item;
          const r = await hooks.rose().calculate(what, { by, onStep: x => { try { o.onStep && o.onStep({ ...x, key: x.key || 'roseLine' }); } catch (_) { /* never stops it */ } } });
          if (!r || r.ok === false) throw new Error((r && r.error) || 'The green dash line could not be calculated: open the sheet on the Nest tab and press Cut Sheet there');
          const got = (r.sheets || []).filter(x => x && x.lineAdded && !x.skipped).map(x => x.label).filter(Boolean);
          label = got.length ? `Green dash line added to ${got.join(', ')}` : 'Green dash line already there';
        } else if (s.type === 'roseJoin') {
          // the Cut Sheet press for this sheet (never anything else): joins the set, draws the dated line when `line`, records the cut
          const r = await hooks.roseJoin(s.sheetId, { by, line: s.line, full: s.full, onStep: x => { try { o.onStep && o.onStep({ ...x, key: x.key || 'roseJoin' }); } catch (_) { /* never stops it */ } } });
          if (!r || r.ok === false) throw new Error((r && r.error) || 'The Rose Gold sheet could not be added to the set: open it on the Nest tab and press Cut Sheet there');
          for (const x of r.sheets || []) { if (x && x.lineAdded && !x.skipped) extra.push({ key: 'roseLine', label: `Green dash line added to ${x.label}`, detail: 'Dated, and saved with the sheet.' }); if (x && x.cut) extra.push({ key: 'roseCut', label: `Cut recorded for ${x.label}`, detail: 'A recorded cut is permanent.' }); }
          for (const w of r.warnings || []) warnings.push(w);
        } else if (s.type === 'seal') {
          if (o.verify) await o.verify();          // every check holds now: only then a ready seal
          await flow([{ type: 'seal', kind: s.kind, id: s.id }], by);
        } else if (s.type === 'mark') {
          if (s.done && o.verify) await o.verify();
          const r = await markIt(s, by);
          if (r && r.error) throw new Error(r.error);
        } else if (s.type === 'setMember') {
          // the server checks both rules again inside its transaction and answers with what it wrote (a refusal says why in plain words)
          const r = await flow([{ type: 'setMember', moves: s.moves, expect: s.expect }], by);
          if (r && r.membership && typeof hooks.applyMembership === 'function') { try { await hooks.applyMembership(r.membership); } catch (_) { /* the next read draws it */ } }
          for (const a of p.auto.filter(x => x.key === 'hold' || /^membership:/.test(x.key))) extra.push(doneLine(a.key, a.label));
        } else if (s.type === 'relabel' || s.type === 'setFiles') {
          // after the membership is saved: a failure here is a warning, never an undo of the saved membership
          const fn = s.type === 'relabel' ? hooks.relabelSet : hooks.setFiles;
          try {
            if (typeof fn !== 'function') throw new Error(s.type === 'relabel' ? 'the QR label cannot be made from here' : 'the set files cannot be made from here');
            await fn(s, { by });
          } catch (e) {
            warnings.push(s.type === 'relabel' ? `The QR label could not be made (${e.message || e}). Open the sheet and press Make QR label.` : `The labels PDF and manifest of the set could not be made again (${e.message || e}).`);
            tell(s, label, 'error'); continue;
          }
        } else if (s.type === 'include') {
          if (typeof hooks.include !== 'function') throw new Error('Joining a set is not available here: open the sheet and use Include');
          const all = s.with && s.with.length ? s.with : null;
          // a sheet dropped on: it may already have come along with the first one (the sheets that share an order join together); then there is nothing to do
          const there = s.also ? joinableNow(s.sheetId) : null;
          if (!(there && there.draft === false)) await hooks.include(s.sheetId, { setId: s.setId, newSet: s.newSet, split: all || (o.confirmed || []).includes('splitOrders') || (s.also && (o.confirmed || []).includes('together')) ? 'all' : null, ...(all ? { with: all } : {}) });
        }
        applied.push(doneLine(s.key, label)); kept.push(s);
        if (s.type === 'include' && s.with && s.with.length) for (const a of p.auto.filter(x => /^membership:/.test(x.key))) applied.push(doneLine(a.key, a.label));
        if ((s.type === 'include' || s.type === 'roseJoin') && p.auto.some(a => a.key === 'qrLabel')) applied.push(doneLine('qrLabel', (p.auto.find(a => a.key === 'qrLabel') || {}).label || 'QR label made'));
        for (const x of extra.splice(0)) applied.push(x);
        tell(s, label, 'done');
      } catch (e) {
        tell(s, label, 'error');
        const left = [], undone = new Set();
        for (const u of undo.reverse()) { try { await u.fn(); undone.add(u.key); } catch (x) { left.push(x.message); } }
        if (o.acts) o.acts.push(...kept.filter(k => !undone.has(k.key)));
        // (a sheet dropped on that could not join after the first one did: the first is in the set, and it is said)
        const first = s.also ? kept.find(k => k.type === 'include' && !k.also) : null, said = first ? `${(p.auto.find(a => a.key === first.key) || {}).label || 'The first sheet was added'}, but ${((p.auto.find(a => a.key === s.key) || {}).label || 'the other sheet').replace(/ added to .*$/, '')} could not join: ${e.message || e}` : null;
        throw Object.assign(new Error(said || e.message || String(e)), { quiet: e.quiet, applied: applied.filter(a => !undone.has(a.key)), left });
      }
    }
    if (o.acts) o.acts.push(...kept);
    return Object.defineProperty(applied, 'warnings', { value: warnings });
  }
  const inflight = new Map();
  /**
   * commit(plan, {confirmed, by, onStep}): the move, planned again from the records right now. Refuses (and changes nothing)
   * when something is still missing or a confirm has not been given. A move already done answers ok with nothing applied.
   */
  function commit(planned, o = {}) {
    const m = (planned && planned.move) || { kind: planned && planned.kind, id: planned && planned.id, to: planned && planned.to };
    const key = JSON.stringify([m.kind, m.id, m.to, !!m.together]);
    if (inflight.has(key)) return inflight.get(key);
    const job = (async () => {
      try {
        const confirmed = [].concat(o.confirmed || []), item = itemOf(m);
        const fresh = await plan({ kind: item.kind, id: item.id, to: m.to, by: o.by, together: !!m.together });
        if (fresh.needs.length) return { ok: false, applied: [], error: `${fresh.needs[0].label}. ${fresh.needs[0].detail}`.trim(), plan: fresh };
        const open = fresh.confirm.filter(c => !confirmed.includes(c.key));
        if (open.length) return { ok: false, applied: [], error: `Needs your yes first: ${open[0].label}`, plan: fresh };
        if (fresh.noop || !fresh.steps.length) return { ok: true, applied: [], noop: true, plan: fresh };
        // after the label, the hold and the green line, every check is read again: nothing is sealed or marked on a guess
        const verify = async () => {
          const v = view(await readState(item, m.to && m.to.set ? [m.to.set] : []), item.kind, item.id);
          if (v.error) throw new Error(v.error);
          if (!readyNow(v)) throw new Error(`${v.label} is still not ready for Laser cutting. Refresh the Library to see what is left`);
        };
        const acts = [];
        let applied;
        try { applied = await run(fresh, { by: o.by, confirmed, onStep: o.onStep, verify, acts }); } finally { acted(acts); }
        return { ok: true, applied: applied.concat(fresh.auto.filter(a => a.check).map(a => ({ key: a.key, label: a.label }))), ...(applied.warnings.length ? { warnings: applied.warnings } : {}) };
      } catch (e) {
        return { ok: false, applied: e.applied || [], error: e.message || String(e), ...(e.left && e.left.length ? { notUndone: e.left } : {}) };
      }
    })().finally(() => inflight.delete(key));
    inflight.set(key, job);
    return job;
  }
  const SAFE = ['release', 'qrLabel', 'seal'];
  /**
   * approve({kind,id,by,confirmed}): "Approve for laser cutting". Runs every safe automatic step now (the hold lifted, the
   * QR label made, the readiness seal by the person once everything is in place) and says what is still missing. It never
   * marks anything completed and never adds a green dash line unless `confirmed` holds roseLine (the person pressed it).
   * The Plan it returns can be handed to commit(plan, {confirmed}) for what still needs a yes.
   */
  async function approve(req) {
    req = req || {};
    const item = itemOf(req), confirmed = [].concat(req.confirmed || []), by = bySure(req.by);
    const p = await plan({ ...item, to: { area: 'laser' }, by });
    p.applied = []; p.approved = false; p.mode = 'approve';
    if (p.noop) {                                    // already in Laser cutting: only the person's seal can still be missing
      const state = await readState(item), v = view(state, item.kind, item.id);
      if (!v.error && by) { const t = sealTarget(v); if (!sealed(v, t)) { try { await flow([{ type: 'seal', ...t }], by); acted([{ type: 'seal', ...t }]); const line = { key: 'seal', label: `Ready seal recorded by ${by}`, detail: 'The blue seal that says it is ready for Laser cutting.', stamp: true, seal: { how: 'laserReady' }, by }; p.auto.push({ ...line, done: true }); p.applied.push(line); } catch (_) { /* the next refresh records it */ } } }
      p.approved = true;
      return p;
    }
    if (p.from.area === 'completed') { p.ok = false; p.auto = []; p.notes.push(`${p.move.kind === 'set' ? 'This set' : 'This sheet'} is already completed.`); return p; }
    const only = new Set(SAFE);
    if (confirmed.includes('roseLine') && p.confirm.some(c => c.key === 'roseLine')) only.add('roseLine');
    const doneKeys = new Set();
    const noSeal = { ...p, steps: p.steps.filter(s => s.type !== 'seal') }, sealStep = p.steps.find(s => s.type === 'seal');
    const acts = [];       // (what this press kept: one record of the press, below)
    try {
      for (const g of await run(noSeal, { by, confirmed, only, onStep: req.onStep, acts })) doneKeys.add(g.key);
      if (sealStep) {
        try {
          const v = view(await readState(item), item.kind, item.id);
          if (!v.error && readyNow(v)) for (const g of await run({ ...p, steps: [sealStep] }, { by, confirmed, only, onStep: req.onStep, acts })) doneKeys.add(g.key);
        } catch (e) { if (!/not ready/i.test(e.message)) throw e; }
      }
    } catch (e) {
      acted(acts);
      p.error = e.message || String(e); p.ok = false; p.applied = e.applied || [];
      const did = new Set(p.applied.map(a => a.key));
      p.auto = p.auto.filter(a => a.check || did.has(a.key)).map(a => ({ ...a, done: true }));       // (only what was really done is drawn as done)
      return p;
    }
    acted(acts);
    // what is true now, in the same shape: the lines that were done turn into `done`, what is left stays to do
    const after = await plan({ ...item, to: { area: 'laser' }, by });
    // (an approval plan lists only what is now done: a screen draws every line of it as done, what is left is in needs and confirm)
    const doneLines = p.auto.filter(a => a.check || doneKeys.has(a.key) || [...doneKeys].some(k => a.key.startsWith(k + ':') || k.startsWith(a.key + ':'))).map(a => ({ ...a, done: true }));
    after.auto = doneLines;
    after.mode = 'approve';
    after.applied = doneLines.filter(a => !a.check).map(a => { const { done, ...line } = a; return line; });
    after.approved = after.needs.length === 0 && after.confirm.length === 0 && (after.noop === true || !after.steps.length);
    if (after.noop) { after.notes = [...new Set(p.notes.concat(after.notes))]; }
    return after;
  }

  /**
   * cutLine({kind, id, by, onStep}): the person said yes in the Library's green dash line window (charm-nest-library-cutline.js; Paul,
   * 7 Oct: a partial sheet dragged to Laser cutting). Runs the Cut Sheet press for every sheet of the item that is partial and has no
   * line (LibraryFlowRose.calculate with recordCut:true: the line is drawn and dated, then the cut is recorded, which is permanent)
   * and nothing else: it never moves the item and never seals it (the move that follows does). The yes is the press, so it is the
   * caller's to ask for; this never asks. A sheet it cannot give a line (not open on the Nest tab, still saving) is refused before
   * anything is written, in plain words. Resolves { ok, error, lines, cut, sheets, warnings }; never throws.
   */
  async function cutLine(req) {
    req = req || {};
    const item = itemOf(req), by = String(req.by || hooks.employee() || '').trim(), M = hooks.rose();
    if (!item.id) return { ok: false, error: 'There is nothing to make the green dash line for', sheets: [] };
    if (!by) return { ok: false, error: 'Sign in first: the cut is recorded with your name', sheets: [] };
    if (!M || typeof M.calculate !== 'function') return { ok: false, error: 'The green dash line cannot be calculated from here: open the sheet on the Nest tab and press Cut Sheet there', sheets: [] };
    // (the sheets the window listed, when the caller says them: a sheet of a set moves with its set, so every one that needs a line gets it; else the item)
    const ids = [...new Set((Array.isArray(req.sheetIds) ? req.sheetIds : []).map(String).filter(Boolean))], what = item.kind === 'sheet' && ids.length && !(ids.length === 1 && ids[0] === item.id) ? ids.map(id => ({ kind: 'sheet', id })) : item;
    try {
      const r = await M.calculate(what, { by, recordCut: true, onStep: x => { try { req.onStep && req.onStep(x); } catch (_) { /* a listener never stops it */ } } });
      if (!r || typeof r !== 'object') return { ok: false, error: 'No answer came back, nothing was changed', sheets: [] };
      // a line that is saved but whose cut could not be recorded is not the whole press: the move waits, and the sheet says why
      if (r.ok !== false && (r.warnings || []).length) return { ...r, ok: false, error: `${r.warnings.join('. ')}. Press Cut Sheet on the Nest tab to record it` };
      return r;
    } catch (e) { return { ok: false, error: (e && e.message) || String(e), sheets: [] }; }
  }

  function targets(item) { return zones(item, pageAdapter()).filter(z => z.ok).map(keyOfZone); }
  function explainTargets(item) { return zones(item, pageAdapter()); }
  function onSheet(item, otherId, o) { return sheetZone(item, otherId, pageAdapter(), o); }
  function pageAdapter() { return { area: hooks.areaOf, sets: hooks.sets, setOf: hooks.setOf, live: hooks.live }; }

  /* ── the page: how the code that owns each thing is reached. Each falls back to nothing, and a move that needs one
     then says so in `needs` or in its error instead of working around it. ──────────────────────────────────────────── */
  /* A Rose Gold sheet into a set is its Cut Sheet press (charm-nest-rose-ui.js RoseStock.record): the sheet joins the set,
     the dated green line is drawn when it has none, the cut is recorded. With a line to add it goes through
     LibraryFlowRose.calculate with recordCut:true (the one place a move records a cut: joining a set is that press); a sheet
     that already has its line is pressed the way its button does. A full sheet has no Cut Sheet button: it only joins.
     g() gives the page's parts { RoseStock, Gate, CN, LibraryFlowRose }; commit calls this only after the person's yes keys. */
  function makeRoseJoin(g) {
    return async (id, o) => {
      const { RoseStock: RS, Gate: G, CN: C, LibraryFlowRose: M } = g();
      const sh = ((C && C.allSheets && C.allSheets()) || []).find(p => p.sheetId === id && !p.recalled), name = sh ? `RG Sheet ${sh.page || 1}` : 'This sheet';
      if (!sh || !RS) return { ok: false, error: `${name} is not open on this page, so it cannot be added to the set from here. Open it on the Nest tab and press Cut Sheet there` };
      const inSetNow = () => !!sh.setId && !sh.draft;
      if (o.line && M && typeof M.calculate === 'function') {
        const r = await M.calculate({ kind: 'sheet', id }, { by: o.by, recordCut: true, onStep: o.onStep });
        if (!r || r.ok === false) return r || { ok: false, error: 'The green dash line could not be calculated' };
        if (inSetNow()) return r;          // (a line that had meanwhile been drawn is skipped by calculate: the plain press follows)
      }
      if (sh._roseAction) return { ok: false, error: `${name} is busy with another action: try again in a moment` };
      sh._roseAction = true; sh._roseError = null;
      try {
        if (o.full) {
          if (!G || !G.changeMembership) throw new Error('Sets are not loaded yet · try again in a moment');
          await G.changeMembership('rose', true);
          if (!inSetNow()) throw new Error('Not added: ' + ((G.policy && G.policy(sh) && G.policy(sh).reason) || 'this sheet is not in a set yet'));
        } else await RS.record(sh, { by: o.by });
      } catch (e) { sh._roseError = e.message; return { ok: false, error: `${name}: ${e.message}` }; }
      finally { sh._roseAction = false; try { RS.render(sh); } catch (_) { /* drawn at its next refresh */ } }
      return { ok: true, sheets: [{ sheetId: id, label: name, lineAdded: false, lines: 0, cut: !!sh.roseCutAt }], warnings: [] };
    };
  }
  function wirePage() {
    if (typeof document === 'undefined') return;
    const LR = () => root.LaserReview, LD = () => root.LibraryDone;
    const cards = () => [...document.querySelectorAll('.setCard[data-laser-card]')];
    const cardOf = it => it.kind === 'set' ? cards().find(c => c._laserSet && c._laserSet.setId === it.id) : cards().find(c => (c._laserSheets || []).includes(it.id));
    hooks.setOf = it => { if (it.kind !== 'sheet') return null; const c = cardOf(it); return c && c._laserSet && !c._laserSet.standalone && !c._laserSet.working ? c._laserSet.setId : null; };
    hooks.areaOf = it => {
      const c = cardOf(it), a = c && c.closest('[data-laser-area]');
      return a ? (a.dataset.laserArea === 'ready' ? 'laser' : 'progress') : null;
    };
    hooks.sets = () => cards().map(c => c._laserSet).filter(st => st && st.setId && !st.standalone && !st.working);
    hooks.sync = process => {
      const lr = LR(); if (!lr) return;
      lr.acceptProcess(process);
      for (const p of process) {
        if (p.kind !== 'sheet') continue;
        for (const c of cards()) for (const s of c._sheets || []) if (s.id === p.id) Object.assign(s, p.patch);
        try { const rows = (typeof S !== 'undefined' && S.library && S.library.rows) || []; for (const r of rows) if (r.id === p.id) Object.assign(r, p.patch); } catch (_) { /* not shown */ }
      }
      lr.changed();
    };
    hooks.mark = async (kind, id, done, o) => {
      const ld = LD(), lr = LR();
      // a sheet that has just become ready is placed in Laser cutting by the page's next refresh: LibraryDone.mark waits for that
      let shown = !done || (ld && ld.canComplete(kind, id));
      for (let i = 0; ld && !shown && i < 24; i++) { if (i === 0 && lr) { lr.changed(); if (lr.poll) lr.poll(true); } await hooks.wait(150); shown = ld.canComplete(kind, id); }
      if (ld && shown) return (await ld.mark(kind, id, done, { by: o && o.by, via: 'Library move' })) || { ok: true };
      const r = answer(await cloud({ op: 'laserDone', kind, id, done, by: (o && o.by) || undefined, stage: done ? 'laser' : undefined, device: 'charm-nest-1', via: 'Library move' }));
      await sync(r.process);
      try { if (root.CNLaserAct) root.CNLaserAct.cut(done, { name: ld && ld.nameOf ? ld.nameOf(kind, id) : '', ids: r.sheetIds && r.sheetIds.length ? r.sheetIds : [id], rec: ld && ld.recordOf }); } catch (_) { /* a record of the work never stops the work */ }
      if (ld && ld.addedSeals) ld.addedSeals(r.added);
      try { if (root.CN && root.CN.loadLibrary) root.CN.loadLibrary(); } catch (_) { /* refreshed at its next read */ }
      return r;
    };
    hooks.roseJoin = makeRoseJoin(() => root);
    // a committed set's edit (charm-nest-set-edit.js, page side): the QR labels, the set's files, and the page's own copies
    const se = () => root.SetEdit || {};
    hooks.relabelSet = se().relabel ? (s, o) => se().relabel(s, o) : null;
    hooks.setFiles = se().remakeFiles ? (s, o) => se().remakeFiles(s, o) : null;
    hooks.applyMembership = se().applyMembership ? m => se().applyMembership(m) : null;
    // the sheet window's own paths (charm-nest-sheetwin.js): present once that file offers them
    const sw = () => root.SheetWin || {};
    hooks.remakeLabel = sw().remakeLabel ? id => sw().remakeLabel(id) : null;
    hooks.include = sw().joinSet ? (id, o) => sw().joinSet(id, o) : null;
    hooks.live = id => (sw().joinInfo ? sw().joinInfo(id) : null);
    Object.assign(hooks, mine);
  }
  if (typeof document !== 'undefined') { try { wirePage(); document.addEventListener('DOMContentLoaded', () => { try { wirePage(); } catch (_) { /* hooks stay as they were */ } }, { once: true }); } catch (_) { /* a page without these parts */ } }
  const configure = o => { Object.assign(hooks, o || {}); Object.assign(mine, o || {}); return api; };
  const api = { targets, explainTargets, sheetZone: onSheet, plan, commit, approve, cutLine, configure, hooks, core: { planMove, view, areaOf, groupReady, sheetGaps, zones, sheetZone, sheetName, setName, makeRoseJoin, needsCutLine, cuts }, AREAS, AREA_NAMES: AREA };
  return api;
});
