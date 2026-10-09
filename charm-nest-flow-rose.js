/* LibraryFlowRose (alias LibraryFlowCutLine): the green-line guard of the Library's process flow (contract v1.1, section 1).
   It covers every metal that has a green line (charm-nest-rose.js cuts(): Rose Gold, 10K Gold, 14K Gold). The words and names keep
   saying "Rose Gold" for RG; a 10K or 14K sheet is named by its own code ("10K Sheet 1") and nothing else about the guard differs.

   Paul, 3 Oct: Rose Gold sheets that have no green dash line yet "will automatically require a dash line to be
   calculated, this will all have to be visually shown to the user ... and the last verification step will have to be
   added so that the user doesn't mistakenly add a green dash line to a rose gold sheet without intentionally actually
   doing that simply by dragging and dropping a rose gold sheet between sets or process checkpoints."

   The standing rules this keeps (charm-nest-rose-ui.js, _charmNestRoseStock.js):
   - Only a press adds a green line. A drag, a drop, a move between sets, a process step, Include, Save, Nest, a changed
     allowance: none of them ever adds one. Nothing in this file adds a line except calculate(), and calculate() is only
     called after the explicit press on confirmBar's "Add the green dash line" (or the same press, wired by the caller).
   - The line is the existing one: RoseStock.plan(sheet, {cut:true}) is the call the Cut Sheet button makes; the server
     (rosePlan) still refuses a new line that is not asked for with cut:true. Each line is dated by the server.
   - A full or nearly full sheet takes the rest of the metal whole and has no line to add (RoseStock.addsLine says so).
   - Partial sheets pack as one tight block from the left, as before: this file never touches placements.

   window.LibraryFlowRose
     check(item)                 -> Promise<{needsLine, sheets:[{sheetId,label,needsLine,why,...}], confirm:null|{key:'roseLine',label,detail}}>
                                    READ ONLY. At most it reads saved sheet records (listSheets / getSheet); it never writes, and
                                    never rejects (a sheet it cannot read is flagged unknown, not reported as needing a line).
     calculate(item, {by, onStep, recordCut}) -> Promise<{ok, lines, error, sheets, cut, warnings}>
                                    Runs the EXISTING path for each Rose Gold sheet of the item that still has no line:
                                    RoseStock.plan(sheet,{cut:true}) (the Cut Sheet button's own contour call) and, only with
                                    recordCut:true, RoseStock.record(sheet,{by}) = the whole Cut Sheet press (it also puts a held
                                    sheet in the current set and records the cut, which is permanent). Without recordCut the cut is
                                    left for the Cut Sheet button, so the line alone is added. Re-checks each sheet right before
                                    acting, so a repeated or stale call never adds a second line. onStep({key:'calculating'|
                                    'drawn'|'saved', label, sheetId, sheetLabel, index, count}).
     confirmBar(host, {sheetLabel, onConfirm, onCancel}) -> controller {el, step, finish, fail, cancel, destroy, onStep}
                                    The inline last verification bar. onConfirm fires only from an explicit press on "Add the
                                    green dash line" (never from a drop, a scripted click or a ghost click); onCancel only from
                                    "Not now" or Esc; mounting writes nothing. After the press: a small labelled spinner, then the
                                    drawn green dash line appears on the sheet preview (CN.paintPreview, which paints it with
                                    RoseStock.paint) and a tick says it is saved. Optional: item / sheet / count / by / armMs.

   Item: {kind:'sheet'|'set', id} (plus optional sheet / sheets records as hints), a live sheet object, a set record with
   sheetIds, a sheet id string, or an array of those. A set means every Rose Gold sheet in it.
   Callers feature-detect it (window.LibraryFlowRose?.check); every dependency here is feature-detected too. */
(function () {
  'use strict';
  if (window.LibraryFlowRose) return;
  const W = window, doc = document, KEY = 'roseLine';
  const cn = () => W.CN || null, rs = () => W.RoseStock || null;
  const CL = () => W.CharmNestRose || null;
  const hasLine = m => { const c = CL(); return !!(c && c.cuts(m)); };           // a metal with a green line (the one list: CharmNestRose.cuts)
  const codeOf = m => { const c = CL(); return (c && c.cutCode(m)) || 'RG'; };
  const plural = (n, one, many) => `${n} ${n === 1 ? one : many || one + 's'}`;
  const parse = s => { try { return s ? JSON.parse(s) : null; } catch (_) { return null; } };
  const liveSheets = () => { try { return cn()?.allSheets?.() || []; } catch (_) { return []; } };
  const idOf = x => (x && (x.sheetId || x.id)) || null;
  const reduced = () => { try { return !!(W.matchMedia && W.matchMedia('(prefers-reduced-motion: reduce)').matches); } catch (_) { return false; } };
  const when = at => Number.isFinite(+at) && +at > 0 ? new Date(+at).toLocaleString(undefined, { year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) : '';

  /* ── names ──────────────────────────────────────────────────────────────────────────────────────────────────── */
  function sheetNo(x) {
    const n = +x.sheetIndex || +x.page; if (n > 0) return n;
    const m = /_Sheet-(\d+)/.exec(String(x.fileBase || x.folder || '')); return m ? +m[1] : 0;
  }
  const labelOf = x => codeOf(x && x.metal) + ' Sheet' + (sheetNo(x || {}) ? ' ' + sheetNo(x) : '');
  // whether a name ("RG Sheet 1") is one of the names in a text (not "RG Sheet 10")
  const mentions = (text, name) => !!name && new RegExp('(^|\\W)' + String(name).replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '(?!\\d)', 'i').test(String(text || ''));
  // "A Rose Gold sheet" / "A 10K Gold sheet" / "A sheet" (a label naming sheets of several metals)
  const sheetWord = label => { const codes = [...new Set((String(label || '').match(/\b(10K|14K|RG)\b/gi) || []).map(c => c.toUpperCase()))]; return codes.length === 1 ? (codes[0] === 'RG' ? 'A Rose Gold sheet' : `A ${codes[0]} Gold sheet`) : codes.length ? 'A sheet' : 'A Rose Gold sheet'; };
  const joinLabels = ls => ls.length <= 1 ? ls[0] || '' : ls.slice(0, -1).join(', ') + ' and ' + ls[ls.length - 1];

  /* ── what a sheet has ───────────────────────────────────────────────────────────────────────────────────────── */
  const planStages = p => !p ? 0 : p.stages ? p.stages.length : p.lines && p.lines.length ? 1 : 0;
  const lineCount = sh => planStages(sh.rosePlan);
  const lastLineAt = sh => { const st = sh.rosePlan && sh.rosePlan.stages; const l = st && st[st.length - 1]; return l && Number.isFinite(+l.at) ? +l.at : null; };
  function cutOf(x) {
    if (x.roseCutAt || +x.laserDoneAt > 0) return true;
    try { return !!(W.CharmNestReadiness && W.CharmNestReadiness.completedBefore && W.CharmNestReadiness.completedBefore(x)); } catch (_) { return false; }
  }
  // charms of a live sheet outside every green line it has (the rule RoseStock.unlined applies)
  function unlinedLocal(sh) {
    const lined = new Set([...((sh.roseProtected && sh.roseProtected.placements) || []).map(p => p.id), ...((sh.rosePlan && !sh.dirty ? sh.rosePlan.shapes : null) || []).map(s => s.id)]);
    return (sh.placements || []).filter(p => !lined.has(p.id));
  }
  const noPlan = (base, why) => ({ ...base, needsLine: false, why });

  function verdictLive(sh) {
    const placed = (sh.placements || []).length;
    const base = { sheetId: idOf(sh), label: labelOf(sh), metal: sh.metal, cutLine: hasLine(sh.metal), rose: sh.metal === 'rose', needsLine: false, why: '', charms: placed, needs: 0, held: !!(sh.draft || !sh.setId), source: 'live' };
    if (!hasLine(sh.metal)) return noPlan(base, 'Not a Rose Gold sheet · this metal has no green dash line');
    if (cutOf(sh)) return noPlan(base, 'Already cut: its green dash line is recorded');
    if (!placed) return noPlan(base, 'No charms are on this sheet yet');
    const RS = rs();
    let open, adds;
    try { open = RS && RS.unlined ? RS.unlined(sh) : unlinedLocal(sh); adds = RS && RS.addsLine ? !!RS.addsLine(sh) : open.length > 0; } catch (_) { open = unlinedLocal(sh); adds = open.length > 0; }
    const lines = (sh.rosePlan && sh.rosePlan.lines && sh.rosePlan.lines.length) || (sh.roseProtected && sh.roseProtected.lines && sh.roseProtected.lines.length) || 0;
    if (!adds) {
      if (open.length) return noPlan({ ...base, needs: 0, full: true }, 'The sheet is full: it takes the rest of the metal whole, so there is no green dash line to add');   // (full: LibraryFlow says so when readiness still holds the sheet for its line)
      const at = lastLineAt(sh);
      return noPlan(base, lines ? 'Already has its green dash line' + (at ? ' (' + when(at) + ')' : '') : 'It is planned whole, so there is no green dash line to add');
    }
    const blocked = !sh.persistedDone || !sh.verification || !sh.verification.ok || sh.dirty || ['nesting', 'finishing', 'queued'].includes(sh.status) ? 'Its layout is still being nested or saved: wait for that to finish first' : '';
    const cut = straddling(sh);   // (an order with a piece inside a line and a piece outside it)
    return { ...base, needsLine: true, needs: open.length, blocked, ...(cut.length ? { straddle: cut } : {}),
      why: (lines ? `${plural(open.length, 'charm')} sit past its last green dash line` : `No green dash line yet: ${plural(open.length, 'charm')} not inside a cut line`) + (cut.length ? `. ${straddleWords(cut)}` : '') };
  }
  // pairs (PAIRPARTIAL): the groups (order lines) of a live sheet with a piece inside a green line and a piece outside every line: [{ order, inside, outside }]
  function straddling(sh) {
    try {
      const lined = new Set([...((sh.roseProtected && sh.roseProtected.placements) || []).map(p => p.id), ...((sh.rosePlan && !sh.dirty ? sh.rosePlan.shapes : null) || []).map(s => s.id)]);
      if (!lined.size) return [];
      const P = W.CharmNestPartial, key = P && P.groupKeyOf ? P.groupKeyOf : c => c.order || c.id, byId = new Map((sh.charms || []).map(c => [c.id, c])), by = new Map();
      for (const p of sh.placements || []) { const c = byId.get(p.id); if (!c) continue; const k = key(c), g = by.get(k) || { order: String(c.order != null ? c.order : k).split('/')[0], inside: 0, outside: 0 }; if (lined.has(p.id)) g.inside++; else g.outside++; by.set(k, g); }
      return [...by.values()].filter(g => g.inside > 0 && g.outside > 0);
    } catch (_) { return []; }
  }
  const straddleWords = list => list.length === 1 ? `Order ${list[0].order} has ${plural(list[0].inside, 'piece')} inside a green line and ${plural(list[0].outside, 'piece')} outside it: its pieces would be cut in two lines` : `${list.length} orders have pieces inside a green line and pieces outside it (for example order ${list[0].order}): their pieces would be cut in two lines`;
  // a saved record (the Library's own copy): it has no geometry, so it can say whether a line exists, not draw one
  function verdictRecord(r, how) {
    const placed = +r.placedCount || (r.placements || []).length || (r.poolIds || []).length || 0;
    const base = { sheetId: idOf(r), label: labelOf(r), metal: r.metal, cutLine: hasLine(r.metal), rose: r.metal === 'rose', needsLine: false, why: '', charms: placed, needs: 0, held: !!(r.draft || !r.setId), source: how || 'record' };
    if (!hasLine(r.metal)) return noPlan(base, 'Not a Rose Gold sheet · this metal has no green dash line');
    if (cutOf(r)) return noPlan(base, 'Already cut: its green dash line is recorded');
    if (!placed && !+r.charmCount) return noPlan(base, 'No charms are on this sheet yet');
    const plan = parse(r.rosePlanJson), guard = parse(r.roseProtectedJson);
    if (r.placements && (plan || guard)) {
      const lined = new Set([...(guard && guard.placements || []).map(p => p.id), ...(plan && plan.shapes || []).map(s => s.id)]);
      const open = r.placements.filter(p => !lined.has(p.id));
      return open.length ? { ...base, needsLine: true, needs: open.length, why: `${plural(open.length, 'charm')} sit past its last green dash line` } : noPlan(base, 'Already has its green dash line');
    }
    // the saved line is the plan hash (what readiness reads): a protected-line copy or a plan with no hash is no saved line (GF1: it said "already has its line" and let the move go on)
    if (r.rosePlanHash) return noPlan(base, 'Already has its green dash line');
    return { ...base, needsLine: true, needs: placed, why: 'No green dash line yet' + (placed ? `: ${plural(placed, 'charm')} not inside a cut line` : '') };
  }
  const geometry = sh => !!sh && Array.isArray(sh.placements) && !(sh.recalled && !(sh.charms || []).length);
  function verdict(e) {
    if (e.live && geometry(e.live)) return verdictLive(e.live);
    const sh = e.live, r = e.rec || (sh && sh.recalled ? { ...sh.recalled, roseCutAt: sh.roseCutAt || sh.recalled.roseCutAt } : null);
    if (r) return verdictRecord(r, sh ? 'saved' : 'record');
    return { sheetId: e.id || null, label: 'A sheet', needsLine: false, unknown: true, why: 'This sheet could not be read, so its green dash line was not checked' };
  }

  /* ── finding the sheets of an item (reads only) ─────────────────────────────────────────────────────────────── */
  const isSheetObj = o => o && typeof o === 'object' && !o.kind && o.metal !== undefined && !!(o.placements || o.charms || o.placedCount !== undefined || o.sheetId || o.id);
  const liveById = id => { const all = liveSheets().filter(sh => id && sh.sheetId === id); return all.find(sh => !sh.recalled) || all[0] || null; };
  function setRecordOf(setId) {
    try { const m = W.B && W.B.sets; if (m && m.values) for (const s of m.values()) if (s && s.setId === setId) return s; } catch (_) {}
    return null;
  }
  async function readRecords(body) {
    const C = cn(); if (!C || !C.api) throw new Error('not connected');
    return C.api('charmNestLibrary', body, { quiet: true });
  }
  async function gather(item) {
    const out = { entries: [], unknown: [] }, seen = new Set(), list = Array.isArray(item) ? item : [item];
    const add = (id, live, rec, from) => { const k = id || live || rec; if (!k || seen.has(k)) return; seen.add(k); out.entries.push({ id, live: live || null, rec: rec || null, from }); };
    for (const it of list) {
      if (it == null) continue;
      const o = typeof it === 'string' ? { id: it } : it;
      if (isSheetObj(o)) { const lv = liveSheets().includes(o) ? o : liveById(idOf(o)); add(idOf(o), lv, lv ? null : o, 'sheet'); continue; }
      let kind = o.kind || (o.sheetIds || o.setId ? 'set' : 'sheet');
      if (typeof it === 'string' && !liveById(it) && (setRecordOf(it) || /^set-/i.test(it))) kind = 'set';
      if (kind === 'sheet') {
        const id = o.id || o.sheetId, lv = liveById(id), hint = o.sheet || o.record || null;
        if (lv) { add(id, lv, null, 'sheet'); continue; }
        if (hint) { add(id, null, hint, 'sheet'); continue; }
        try { const r = await readRecords({ op: 'getSheet', id }); if (r && r.sheet) add(id, null, r.sheet, 'sheet'); else out.unknown.push({ id }); } catch (_) { out.unknown.push({ id }); }
        continue;
      }
      const setId = o.id || o.setId, rec = setRecordOf(setId);
      const ids = [...new Set([...(o.sheetIds || []), ...((rec && rec.sheetIds) || [])])];
      const hints = new Map([...(o.sheets || o.records || [])].filter(Boolean).map(r => [idOf(r), r]));
      for (const sh of liveSheets()) if (sh.setId === setId && !sh.draft && (sh.sheetId ? !seen.has(sh.sheetId) : true)) add(idOf(sh), sh, null, 'set');
      for (const [id, r] of hints) if (!liveById(id)) add(id, null, r, 'set');
      const missing = ids.filter(id => !seen.has(id));
      if (missing.length || (!out.entries.some(e => e.from === 'set') && !ids.length)) {
        try {
          const r = await readRecords({ op: 'listSheets', setId, limit: 200 });
          for (const d of (r && r.sheets) || []) if (d && d.setId === setId && !d.archived) add(idOf(d), liveById(idOf(d)), d, 'set');
          for (const id of missing) if (!seen.has(id)) out.unknown.push({ id });
        } catch (_) { out.unknown.push({ id: 'set:' + setId, label: 'A sheet of this set' }); }
      }
    }
    return out;
  }
  function confirmOf(need) {
    const labels = need.map(s => s.label), n = need.reduce((t, s) => t + (s.needs || 0), 0);
    return { key: KEY, label: `Add the green dash line to ${joinLabels(labels)}?`,
      detail: `This calculates the cut contour for ${n ? plural(n, 'charm') : 'the charms'} and dates the line. Nothing is added until you press the button.`,
      sheetIds: need.map(s => s.sheetId).filter(Boolean), count: need.length };
  }

  /* ── check: read only ───────────────────────────────────────────────────────────────────────────────────────── */
  async function check(item) {
    const out = { needsLine: false, sheets: [], confirm: null, unknown: false };
    try {
      const g = await gather(item);
      const single = (Array.isArray(item) ? item : [item]).every(x => x && (typeof x === 'string' || isSheetObj(x) || x.kind === 'sheet'));
      // a set lists its sheets that have a green line (Rose Gold, 10K, 14K); a sheet asked about by name is always answered, line or not
      for (const e of g.entries) { const v = verdict(e); if (single || v.cutLine || v.unknown) out.sheets.push(v); }
      for (const u of g.unknown) out.sheets.push({ sheetId: u.id || null, label: u.label || 'A sheet', needsLine: false, unknown: true, why: 'This sheet could not be read, so its green dash line was not checked' });
      out.unknown = g.unknown.length > 0;
      const need = out.sheets.filter(s => s.needsLine);
      out.needsLine = need.length > 0; out.confirm = need.length ? confirmOf(need) : null;
    } catch (e) { out.unknown = true; out.error = (e && e.message) || String(e); }
    return out;
  }

  /* ── calculate: only after an explicit press ────────────────────────────────────────────────────────────────── */
  const bars = new Set(), inflight = new WeakMap();
  // the one bar that is waiting on a calculation takes every step of it; with several, each takes the steps that name its sheets
const broadcast = (kind, payload) => { const all = [...bars].filter(b => b.working()); for (const b of all) { try { b.hear(kind, payload, all.length === 1); } catch (_) {} } };
  function refresh(sh) {
    try { W.Session && W.Session.schedule && W.Session.schedule(); if (sh.el) { const C = cn(); C.renderCard(sh); C.drawPreview(sh); } } catch (_) {}
  }
  async function calculate(item, o) {
    o = o || {};
    const by = String(o.by || '').trim(), recordCut = o.recordCut === true;
    const say = (key, label, v, extra) => {
      const ev = { key, label, sheetId: v && v.sheetId || null, sheetLabel: v && v.label || '', ...(extra || {}) };
      try { if (o.onStep) o.onStep(ev); } catch (_) {}
      broadcast('step', ev);
    };
    const end = res => { try { broadcast('finish', res); } catch (_) {} return res; };
    const refuse = (error, extra) => end({ ok: false, lines: 0, error, sheets: [], cut: false, warnings: [], ...(extra || {}) });
    try {
      const RS = rs(), C = cn();
      if (!RS || !RS.plan || (recordCut && !RS.record)) return refuse('Cut Sheet is not loaded on this page yet. Reload the page and try again');
      const g = await gather(item);
      const need = g.entries.map(e => ({ e, v: verdict(e) })).filter(x => x.v.needsLine);
      // nothing is touched until every sheet that needs a line can really be given one
      const stuck = need.filter(x => !(x.e.live && geometry(x.e.live)));
      if (stuck.length) return refuse(`${joinLabels(stuck.map(x => x.v.label))} ${stuck.length === 1 ? 'is' : 'are'} not open on this page, so its green dash line cannot be worked out here. Open it on the Nest tab and press Cut Sheet there`);
      const waiting = need.filter(x => x.v.blocked);
      if (waiting.length) return refuse(`${waiting[0].v.label}: ${waiting[0].v.blocked}`);
      if (need.length && C && C.S && C.S.cloud && C.S.cloud.ok === false) return refuse('Reconnect to the cloud before working out a green dash line');
      const sheets = []; let lines = 0, error = null; const warnings = [];
      for (let i = 0; i < need.length; i++) {
        const sh = need[i].e.live;
        if (inflight.has(sh)) { try { await inflight.get(sh); } catch (_) {} }
        // checked again, right before acting: a repeated or stale press adds nothing
        const v = verdictLive(sh);
        if (!v.needsLine) { sheets.push({ sheetId: v.sheetId, label: v.label, lineAdded: false, lines: 0, skipped: true, cut: !!sh.roseCutAt, error: null }); continue; }
        if (sh._roseAction) { error = `${v.label} is busy with another action: try again in a moment`; sheets.push({ sheetId: v.sheetId, label: v.label, lineAdded: false, lines: 0, cut: false, error }); break; }
        const meta = { index: i + 1, count: need.length };
        const run = addLine(sh, v, { by, recordCut, say: (k, l, x) => say(k, l, x, meta) });
        inflight.set(sh, run);
        let r; try { r = await run; } finally { if (inflight.get(sh) === run) inflight.delete(sh); }
        sheets.push(r); lines += r.lines;
        if (r.note) warnings.push(`${r.label}: ${r.note}`);
        if (r.error) { error = `${r.label}: ${r.error}`; break; }
      }
      return end({ ok: !error, lines, error, sheets, cut: sheets.length > 0 && sheets.every(s => s.cut), warnings });
    } catch (e) { return refuse((e && e.message) || String(e)); }
  }
  async function addLine(sh, v, x) {
    const RS = rs(), C = cn(), before = lineCount(sh);
    x.say('calculating', `Working out the green dash line for ${v.label}…`, v);
    sh._roseAction = true; sh._roseError = null; refresh(sh);
    let failure = null, said = false;
    const sayDrawn = () => { if (!said) { said = true; x.say('drawn', `Green dash line drawn on ${v.label}`, v); } };
    const watch = x.recordCut ? setInterval(() => { if (lineCount(sh) > before && sh.rosePlanHash) sayDrawn(); }, 60) : 0;
    try {
      if (x.recordCut) await RS.record(sh, { by: x.by });
      else {
        await RS.plan(sh, { cut: true });
        // a plan already under way (not asked for a new line) is joined by plan(): if it added none, ask once more
        if (lineCount(sh) <= before && verdictLive(sh).needsLine) await RS.plan(sh, { cut: true });
      }
    } catch (e) { failure = e; }
    finally {
      if (watch) clearInterval(watch);
      sh._roseAction = false; if (failure) sh._roseError = failure.message || String(failure);
      refresh(sh); try { C.flushManualIntake && C.flushManualIntake(sh.metal); } catch (_) {}
    }
    const added = lineCount(sh) - before, drawn = added > 0 || !verdictLive(sh).needsLine;
    const res = { sheetId: v.sheetId, label: v.label, lineAdded: drawn, lines: Math.max(0, added), cut: !!sh.roseCutAt, at: lastLineAt(sh), error: null };
    if (!drawn) { res.error = (failure && failure.message) || 'The green dash line could not be added'; return res; }
    sayDrawn();
    const n = lineCount(sh), stamp = when(res.at);
    if (failure) res.note = `the green dash line is saved, but the cut is not recorded yet (${failure.message || failure})`;
    x.say('saved', `Saved${n ? ' as green line ' + n : ''}${stamp ? ' · ' + stamp : ''}${failure ? ' · the cut is not recorded yet' : ''}`, v);
    return res;
  }

  /* ── confirmBar: the last verification step ─────────────────────────────────────────────────────────────────── */
  const CSS = `
.lfrBar{--lfr-line:#008974;display:grid;grid-template-columns:auto minmax(0,1fr);gap:14px;align-items:start;margin:8px 0;padding:12px 14px;border:1px solid var(--goldLine,#e3d3a6);border-left:3px solid var(--gold,#a9823f);border-radius:var(--rsm,9px);background:#fbf5e6;color:var(--ink,#1c1a17);font:13px/1.45 var(--sans,system-ui,sans-serif);max-width:640px;box-sizing:border-box}
.lfrBar.lfrNoPv{grid-template-columns:minmax(0,1fr)}
.lfrBar p{margin:0}
.lfrBar .lfrTitle{font-weight:650;font-size:13px}
.lfrBar .lfrText{margin-top:3px;color:var(--ink70,#5b554c);font-size:12px}
.lfrStatus{display:flex;align-items:center;gap:8px;margin-top:8px;min-height:20px;font-size:12px;color:var(--ink70,#5b554c)}
.lfrStatus:empty{display:none}
.lfrStatus.lfrErr{color:var(--clay,#b0563f)}
.lfrSpin{width:12px;height:12px;border:2px solid rgba(0,0,0,.14);border-top-color:var(--lfr-line);border-radius:50%;animation:lfrSpin .7s linear infinite;flex:0 0 auto}
@keyframes lfrSpin{to{transform:rotate(360deg)}}
.lfrTick{width:14px;height:14px;color:var(--lfr-line);flex:0 0 auto}
.lfrNote{margin-top:4px;font-size:11px;color:var(--ink45,#938c80)}
.lfrActions{display:flex;flex-wrap:wrap;gap:8px;margin-top:10px}
.lfrActions[hidden]{display:none}
.lfrGo{position:relative;overflow:hidden}
.lfrGo[aria-disabled=true]{opacity:.6;cursor:default}
.lfrGo[aria-disabled=true]::after{content:"";position:absolute;left:0;bottom:0;height:2px;width:100%;background:currentColor;opacity:.45;transform-origin:left;animation:lfrArm var(--lfr-arm,700ms) linear forwards}
@keyframes lfrArm{from{transform:scaleX(0)}to{transform:scaleX(1)}}
.lfrPv{position:relative;width:190px;border:1px solid var(--line,#e4ddd0);border-radius:7px;overflow:hidden;background:#fffefb}
.lfrPv[hidden]{display:none}
.lfrPv canvas{display:block;width:100%;height:auto}
.lfrPv canvas.after{position:absolute;left:0;top:0;clip-path:inset(0 100% 0 0);transition:clip-path .9s cubic-bezier(.25,.7,.2,1)}
.lfrPv.drawn canvas.after{clip-path:inset(0 0 0 0)}
.lfrPv.still canvas.after{transition:none}
@media (max-width:520px){.lfrBar{grid-template-columns:minmax(0,1fr)}.lfrPv{width:100%;max-width:260px}}
@media (prefers-reduced-motion:reduce){.lfrSpin{animation-duration:2s}.lfrPv canvas.after{transition:none}.lfrGo[aria-disabled=true]::after{animation:none}}`;
  const TICK = '<svg class="lfrTick" viewBox="0 0 16 16" aria-hidden="true"><path d="M3.4 8.5l3 3 6.2-6.4" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>';
  const escape = s => String(s == null ? '' : s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
  function css() { if (doc.getElementById('lfrCss') || !doc.head) return; const s = doc.createElement('style'); s.id = 'lfrCss'; s.textContent = CSS; doc.head.appendChild(s); }
  // the sheet a bar shows: the one given, the first live sheet (Rose Gold, 10K, 14K) of the item that has no line, or the one its label names
  function previewSheet(o, label) {
    const direct = o.sheet && geometry(o.sheet) ? o.sheet : null; if (direct) return direct;
    const live = liveSheets().filter(sh => hasLine(sh.metal) && geometry(sh) && !cutOf(sh));
    if (o.item) {
      const it = o.item, ids = new Set(), sets = new Set();
      for (const x of Array.isArray(it) ? it : [it]) { if (!x) continue; if (typeof x === 'string') { ids.add(x); sets.add(x); } else if (isSheetObj(x)) ids.add(idOf(x)); else if (x.kind === 'set') sets.add(x.id || x.setId); else ids.add(x.id || x.sheetId); }
      const mine = live.filter(sh => ids.has(sh.sheetId) || (sh.setId && sets.has(sh.setId) && !sh.draft));
      return mine.find(sh => verdictLive(sh).needsLine) || mine[0] || null;
    }
    return live.find(sh => label && mentions(label, labelOf(sh)) && verdictLive(sh).needsLine) || null;
  }
  function paintInto(cv, sh) {
    const C = cn(); if (!C || !C.paintPreview || !C.stockFor) return false;
    try {
      const st = C.stockFor(sh.metal, sh), w = Math.round(190 * Math.min(2, W.devicePixelRatio || 1));
      cv.width = w; cv.height = Math.max(20, Math.round(w * st.hPt / st.wPt));
      C.paintPreview(cv, sh, true, 0);   // the sheet's own picture: RoseStock.paint draws its green lines in it
      return true;
    } catch (_) { return false; }
  }

  function confirmBar(host, o) {
    o = o || {};
    if (!host || typeof host.appendChild !== 'function') return null;
    css();
    const label = String(o.sheetLabel || 'This sheet').replace(/^\s*add the green dash line to\s+/i, '').replace(/[?\s]+$/, '') || 'This sheet', count = Math.max(1, +o.count || (label.split(/,| and /).length));
    const armMs = o.armMs == null ? 700 : Math.max(0, +o.armMs || 0);
    for (const old of [...host.querySelectorAll(':scope > .lfrBar')]) { if (old._lfr) old._lfr.destroy(); else old.remove(); }
    const bar = doc.createElement('div');
    bar.className = 'lfrBar lfrNoPv'; bar.setAttribute('role', 'group'); bar.setAttribute('aria-label', 'Green dash line check'); bar.dataset.lfr = 'ask';
    bar.innerHTML = `<div class="lfrPv" hidden><canvas class="before" aria-hidden="true"></canvas><canvas class="after" aria-hidden="true"></canvas></div>
      <div class="lfrBody"><p class="lfrTitle">${escape(label)} ${count > 1 ? 'have' : 'has'} no green dash line yet</p>
        <p class="lfrText">${escape(o.reason || `${sheetWord(label)} needs one to go to Laser cutting.`)} Moving or dropping ${count > 1 ? 'them' : 'it'} never adds one: only the button below does.</p>
        <div class="lfrStatus" role="status" aria-live="polite"></div>
        <div class="lfrActions"><button type="button" class="btn gold sm lfrGo" aria-disabled="true">Add the green dash line</button><button type="button" class="btn ghost sm lfrNo">Not now</button></div></div>`;
    const pv = bar.querySelector('.lfrPv'), before = pv.querySelector('.before'), after = pv.querySelector('.after');
    const status = bar.querySelector('.lfrStatus'), actions = bar.querySelector('.lfrActions'), go = bar.querySelector('.lfrGo'), no = bar.querySelector('.lfrNo');
    let seen = false, state = 'ask', fired = false, armed = armMs === 0, destroyed = false, cancelled = false, pressed = false, pressT = 0, armT = 0, slowT = 0, drawnAt = 0, lastSaved = '';
    const shown = previewSheet(o, label), ids = new Set(shown && shown.sheetId ? [shown.sheetId] : []);
    if (!armed) { bar.style.setProperty('--lfr-arm', armMs + 'ms'); armT = setTimeout(() => { armed = true; go.removeAttribute('aria-disabled'); }, armMs); } else go.removeAttribute('aria-disabled');
    if (shown && paintInto(before, shown)) { pv.hidden = false; bar.classList.remove('lfrNoPv'); }
    const set = (next, html, cls) => { state = next; bar.dataset.lfr = next; status.className = 'lfrStatus' + (cls ? ' ' + cls : ''); status.innerHTML = html || ''; actions.hidden = !(next === 'ask' || next === 'failed'); };
    const spin = text => `<i class="lfrSpin" aria-hidden="true"></i><span>${escape(text)}</span>`;
    const matches = ev => !ev || label === 'This sheet' || (ev.sheetId && ids.has(ev.sheetId)) || mentions(label, ev.sheetLabel);
    function draw(sh, wipe) {
      if (!sh || !paintInto(after, sh)) return;
      if (pv.hidden) { paintInto(before, sh); pv.hidden = false; bar.classList.remove('lfrNoPv'); }
      if (!wipe || reduced()) pv.classList.add('still');
      pv.classList.add('drawn'); if (wipe && !reduced()) drawnAt = Date.now();
    }
    const sheetOf = ev => (ev && ev.sheetId && liveById(ev.sheetId)) || shown;
    function step(ev) {
      if (destroyed) return; ev = ev || {};
      if (state === 'ask' || state === 'failed') set('working', '');
      if (ev.sheetId) ids.add(ev.sheetId);
      clearTimeout(slowT);
      if (ev.key === 'drawn') { draw(sheetOf(ev), true); set('working', spin(ev.label || 'Saving the green dash line…')); }
      else if (ev.key === 'saved') {
        const sh = sheetOf(ev), wait = drawnAt && !reduced() ? Math.max(0, 900 - (Date.now() - drawnAt)) : 0;
        lastSaved = ev.label || 'Saved';
        const show = () => { if (destroyed || state === 'done' || state === 'failed') return; draw(sh, false); set('working', TICK + `<span>${escape(lastSaved)}</span>`); };
        if (wait) setTimeout(show, wait); else show();
      } else set('working', spin(ev.label || 'Working out the green dash line…'));
      slowT = setTimeout(() => { if (state === 'working' && status.querySelector('.lfrSpin')) status.innerHTML = spin('Still working: this can take a moment'); }, 25000);
    }
    function finish(res) {
      if (destroyed) return; clearTimeout(slowT);
      if (!res || res.ok === false) return fail((res && res.error) || 'The green dash line could not be added');
      const text = res.lines === 0 && !lastSaved ? 'Nothing to add: it already has its green dash line' : lastSaved || `Green dash line saved${res.lines > 1 ? ` (${res.lines} lines)` : ''}`;
      const done = () => {
        if (destroyed) return;
        // the question is answered: the bar says what is now true, and drops the explanation
        bar.querySelector('.lfrTitle').textContent = `${label} ${res.lines === 0 && !lastSaved ? 'already ' : 'now '}${count > 1 ? 'have their' : 'has its'} green dash line`; bar.querySelector('.lfrText').hidden = true;
        set('done', TICK + `<span>${escape(text)}</span>`); (res.warnings || []).forEach(w => { const p = doc.createElement('p'); p.className = 'lfrNote'; p.textContent = w; status.after(p); }); };
      const wait = drawnAt && !reduced() ? Math.max(0, 900 - (Date.now() - drawnAt)) : 0; if (wait) setTimeout(done, wait); else done();
    }
    function fail(err) {
      if (destroyed) return; clearTimeout(slowT); fired = false;
      set('failed', `<span role="alert">${escape((err && err.message) || err || 'Something went wrong')}. Nothing more was changed.</span>`, 'lfrErr');
      go.textContent = 'Try again';
    }
    function fire() {
      if (fired || destroyed) return; fired = true;
      set('working', spin(`Working out the green dash line for ${label}…`));
      let r; try { r = o.onConfirm && o.onConfirm(ctl); } catch (e) { return fail(e); }
      Promise.resolve(r).then(v => { if (v && typeof v === 'object' && 'ok' in v && state === 'working') finish(v); }, e => fail(e));
    }
    function cancel() {
      if (destroyed || cancelled || !(state === 'ask' || state === 'failed')) return;
      cancelled = true; destroy();
      try { if (o.onCancel) o.onCancel(); } catch (_) {}
    }
    function destroy() {
      if (destroyed) return; destroyed = true; clearTimeout(armT); clearTimeout(slowT); clearTimeout(pressT);
      doc.removeEventListener('keydown', onKey); bars.delete(entry); bar.remove(); bar._lfr = null;
    }
    // A press is a pointer down (or Enter / Space) on the button and the click that follows it, after the bar has been
    // up for a moment. A click nobody pressed (a script, a ghost click after a touch drag) is ignored; a trusted click with no
    // pointer (a screen reader's activate) is a real press.
    const hold = () => { pressed = true; clearTimeout(pressT); };
    const release = () => { clearTimeout(pressT); pressT = setTimeout(() => { pressed = false; }, 600); };
    go.addEventListener('pointerdown', e => { if (e.button === 0 || e.button === undefined) hold(); });
    go.addEventListener('pointerup', () => { if (pressed) release(); });
    go.addEventListener('pointerleave', e => { if (!e.pointerType || e.pointerType === 'mouse') { clearTimeout(pressT); pressed = false; } });   // (a touch or pen leaves after it lifts, and its click comes after that)
    go.addEventListener('pointercancel', () => { clearTimeout(pressT); pressed = false; });
    go.addEventListener('keydown', e => { if (e.key === 'Enter' || e.key === ' ') hold(); });
    go.addEventListener('keyup', () => { if (pressed) release(); });
    go.addEventListener('click', e => {
      const was = pressed; pressed = false; clearTimeout(pressT);
      if (e.preventDefault) e.preventDefault();
      if (!(state === 'ask' || state === 'failed') || !armed) return;
      if (!was && !(e.isTrusted && e.detail === 0)) return;
      fire();
    });
    no.addEventListener('click', e => { if (e.preventDefault) e.preventDefault(); cancel(); });
    // a bar its owner took out of the page (hide, a re-render) lets go of everything the next time it is asked
    const gone = () => { if (bar.isConnected) { seen = true; return false; } return seen; };
    function onKey(e) {
      if (gone()) { if (!destroyed) destroy(); return; }
      if (e.key !== 'Escape' || !(state === 'ask' || state === 'failed')) return;
      const a = doc.activeElement; if (a && a !== doc.body && !bar.contains(a)) return;
      cancel();
    }
    doc.addEventListener('keydown', onKey);
    const entry = { working: () => { if (!destroyed && gone()) destroy(); return !destroyed && state === 'working'; }, hear(kind, payload, only) {
      if (destroyed || state !== 'working') return;
      if (gone()) { destroy(); return; }
      if (kind === 'step' && (only || matches(payload))) step(payload);
      else if (kind === 'finish') { const hit = only || !payload || !(payload.sheets || []).length || (payload.sheets || []).some(s => s && (ids.has(s.sheetId) || mentions(label, s.label))); if (hit) finish(payload); }
    } };
    bars.add(entry);
    const ctl = { el: bar, step, finish, fail, cancel, destroy, onStep: step, get state() { return state; } };
    bar._lfr = ctl;
    host.appendChild(bar); seen = bar.isConnected;
    try { no.focus({ preventScroll: true }); } catch (_) {}   // the safe button takes the focus: an Enter right after a drop cancels
    return ctl;
  }

  W.LibraryFlowRose = W.LibraryFlowCutLine = { check, calculate, confirmBar, key: KEY, hasLine };   // (hasLine(metal): rose, gold10k, gold14k)
})();
