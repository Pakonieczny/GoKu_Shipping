// Paul, 26 Oct 2026, of a 14K Sheet 1 the automatic cut had locked ("This sheet's green line has been cut: its layout can no longer change"): "In this specific case where the system did a very poor
// job filling in the remainder of the 14K sheet with charms. I need you to adjust the logic for the 10K and 14 K gold sheets so that they don't get automatically approved and the green cut lines do not
// get automatically engaged for the 10K or 14K Gold sheet when an order is shared with one of the other sheets (GF, SS, RG, 10K) in a given Set of Sheets. What should happen instead in such a case is
// that a given 10 K or 14 K gold sheet should be added to a given Set of Sheets where it shares charms with from a given multi-charm order. However the 10k and 14k sheets should stay greyed-out inside
// the Set of Sheets until a user approves and adds a Green Cut line for either/both of the 10k / 14k gold sheets in question. While the 14K or 10K sheet is waiting for user approval and green cut line,
// the users should be able to move charms (Manual toggle) in and out of that 10k/14k sheet without any restrictions until the manual Green cut line is engaged by the user at which point that particular
// 10k/14k gold sheet would be locked and the Set of Sheets where it resides would be released from that particular Hold and the previously greyed-out 10k/14k gold sheet would become visible and released
// fully inside that particular set of sheets."
// Every part is the page's REAL code (the Gate's assembly, autoCut, cutInclude and undoGoldJoin; the Library's libraryGroups and partnersOf; SetRules.gate; CharmNestReadiness.setGate; SetRepair; the sheet
// hop; Manual Control's guard; the set download's ids) and the server's real handler over the in-memory shop (bridge-server.cjs):
//   1  NO AUTOMATIC CUT for 10K / 14K, full or not: Gate.autoCut presses Cut Sheet for the Rose Gold sheet of a complete set (Decision 2, unchanged), never for the 14K sheet; the 14K sheet stays out of
//      the set's membership, uncut, reserved for its ONE set (cardinalHome, Gate.goldWaitHome) and says so on its own card. RoseStock.record refuses via 'auto' for it (gold-cut-sheet-ui.cjs: the page;
//      gold-cut-sheet.cjs: the server's roseRecordCut).
//   2  THE SET IS HELD, no rule break: the checker names the 14K sheet the set's partner to add (R10, canApprove false), no violation; SetRules.gate / CharmNestReadiness.setGate: "Waiting for 14K Draft 1:
//      approve it and cut its green line" (never "joins by itself"); no red line and no Rebuild; the server refuses to approve the set, in the same words.
//   3  THE LIBRARY draws it greyed INSIDE its set's card (goldPending), out of the "14K / 10K Solid" group, never one of the set's sheets (not in its download, no Approve or rail of its own), with its
//      press "Approve & cut green line" (SetRepair.cutPending: the window first, Cancel changes nothing, the Cut Sheet press after the yes).
//   4  MANUAL MOVES are free while it waits: the sheet hop gives and takes its charms with no "tie" refusal (the physical checks stand), Manual Control is open on it; after the cut both lock it.
//   5  THE PERSON'S PRESS: it joins its set ("either/both": a second 10K sheet waiting in the same set does not stop it), the cut keeps it there, the set's hold for it lifts and the Library draws it as any
//      sheet of the set. A press whose cut is not recorded takes its include back (Gate.undoGoldJoin): the sheet waits greyed again.
//   node tests/charm-nest/gold-karat-manual-cut.cjs
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm'), path = require('node:path');
const root = path.join(__dirname, '../..');
const { world, SO, SR, SN, I, O } = require('./audit-cardinal-lib.cjs');
const RD = require('../../charm-nest-readiness.js'), SRp = require('../../charm-nest-set-repair.js');
const code = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
const ok = (name, cond, detail) => { assert(cond, name + (detail ? ' :: ' + detail : '')); console.log('  ok  ' + name); };
const settle = async (n = 40) => { for (let i = 0; i < n; i++) await new Promise(r => setTimeout(r, 5)); };
let JSDOM = null; try { ({ JSDOM } = require('jsdom')); } catch (_) { JSDOM = null; }

/** The page's run: GF Sheet 1 + SS Sheet 1 (order 7001) form Set 1; the 14K sheet shares order 7002 with GF Sheet 1, the Rose Gold sheet order 7003; o.full: the 14K sheet is full (no green line to add). */
function karatWorld(o = {}) {
  const w = world(), calls = [];
  // RoseStock.record as the page has it, in short: the server refuses an automatic cut of a 10K / 14K sheet; a recorded cut keeps the sheet in the set its press put it in
  w.c.__rec = async (sh, opts) => { calls.push([sh.sheetId, opts && opts.via]); if (opts && opts.via === 'auto' && sh.metal !== 'rose') throw new Error('Not cut: a 14K Gold sheet is never cut automatically'); sh.roseCutAt = Date.now(); await vm.runInContext('Gate.assemble(B.run)', w.c); };
  vm.runInContext('window.RoseStock = { record: (sh, o) => __rec(sh, o) }; var stampWho = () => ({ by: "Test" }); var Session = { schedule() {} }; RunCtl.poke = () => {};', w.c);
  const base = { verification: { ok: true }, persistedDone: true, status: 'complete', dirty: false };
  const gf = w.full(w.page('gold', base)), ss = w.full(w.page('silver', base));
  w.put(gf, '7001_1_1', '7002_1_1', '7003_1_1'); w.put(ss, '7001_2_1');
  const k14 = w.page('gold14k', Object.assign({ releaseFull: !!o.full, density: o.full ? .92 : .4 }, base)); w.put(k14, '7002_2_1', '7004_1_1', '7005_1_1');
  const rg = w.page('rose', Object.assign({ releaseFull: false, density: .3 }, base)); w.put(rg, '7003_2_1');
  const out = { w, calls, gf, ss, k14, rg, base, G: name => vm.runInContext('Gate.' + name, w.c) };
  if (o.k10) { out.k10 = w.page('gold10k', Object.assign({ releaseFull: false, density: .3 }, base)); w.put(gf, '7006_1_1'); w.put(out.k10, '7006_2_1', '7007_1_1'); }
  return out;
}
const setRow = (w, set) => w.selfCheck().sets.find(s => s.id === set.setId);

(async () => {
  /* ── 1 · no automatic cut for 10K / 14K; Rose Gold as before ── */
  for (const full of [false, true]) {
    const t = karatWorld({ full }), { w, calls, gf, ss, k14, rg } = t, what = full ? 'a FULL 14K sheet' : 'a 14K sheet';
    await w.assemble(); await settle();
    const set = w.sets().find(s => s.setId === gf.setId);
    ok(`${what}: Set 1 formed from GF Sheet 1 + SS Sheet 1`, !!set && w.inSet(gf) && w.inSet(ss), w.word());
    ok(`${what}: the Rose Gold sheet of the complete set is still cut automatically (Decision 2) and is in the set`, calls.some(c => c[0] === rg.sheetId && c[1] === 'auto') && w.inSet(rg) && rg.setId === set.setId && !!rg.roseCutAt, JSON.stringify(calls) + ' ' + w.word());
    ok(`${what}: never cut, approved or put in the set by itself`, !calls.some(c => c[0] === k14.sheetId) && !k14.roseCutAt && !w.inSet(k14) && k14.solidPick !== true, JSON.stringify(calls) + ' ' + w.word());
    vm.runInContext('Gate.autoCut(B.run)', w.c); await settle(); await w.assemble(); await settle();
    ok(`${what}: nor at a later look`, !calls.some(c => c[0] === k14.sheetId) && !w.inSet(k14) && !k14.roseCutAt, JSON.stringify(calls));
    ok(`${what}: it is reserved for its ONE set (cardinalHome, Gate.goldWaitHome)`, k14.cardinalHome === set.setId && (t.G('goldWaitHome')(k14) || {}).setId === set.setId, JSON.stringify({ home: k14.cardinalHome }));
    ok(`${what}: its own card says it waits for your approval and its green cut line, and that its charms move by hand meanwhile`, /^Waits in .+ for your approval and its green cut line: press Approve & cut green line on .+ in the Library and it joins .+\. Until then its charms can be moved by hand$/.test(k14.cardinalNote || ''), k14.cardinalNote);
    const res = w.selfCheck(), row = setRow(w, set);
    ok(`${what}: no rule is broken (no red line, no Rebuild)`, res.violations.length === 0, JSON.stringify(res.violations.map(v => v.message)));
    ok(`${what}: it is the set's partner to add (R10): the set cannot be approved`, res.partners.some(p => p.set === set.setId && p.sheet === k14.sheetId && p.kind === 'notReady') && row && row.canApprove === false, JSON.stringify(res.partners));
  }

  /* ── 2 · the words of the hold, page and server ── */
  {
    const gfR = { id: 'gf-1', metal: 'gold', setId: 'set-1', draft: false, releaseFull: true, placedCount: 3 }, ssR = { id: 'ss-1', metal: 'silver', setId: 'set-1', draft: false, releaseFull: true, placedCount: 1 };
    const g = SR.gate({ setId: 'set-1', cardinalWait: [{ id: 'k14-1', label: '14K Draft 1', manual: true }] }, [gfR, ssR]);
    ok('SetRules.gate: the set is held with ONE plain line naming the sheet and the press', g.applies && !g.ok && g.line === 'Waiting for 14K Draft 1: approve it and cut its green line.' && g.short === 'waiting for 14K Draft 1: approve it and cut its green line', JSON.stringify(g));
    ok('both waiting sheets are named together', SR.goldWaitWords(['14K Draft 1', '10K Draft 1']) === 'waiting for 14K Draft 1 and 10K Draft 1: approve them and cut their green lines');
    const mixed = SR.gate({ setId: 'set-1', cardinalWait: [{ id: 'k14-1', label: '14K Draft 1', manual: true }, { id: 'rg-1', label: 'RG Draft 1' }] }, [gfR, ssR]);
    ok('a Rose Gold partner beside it keeps its own words (it is still cut automatically)', /^waiting for 14K Draft 1: approve it and cut its green line; orders shared with RG Draft 1: it joins this set by itself \(a green line is cut automatically\) once it is ready$/.test(mixed.short), mixed.short);
    const rg = SR.gate({ setId: 'set-1', cardinalWait: [{ id: 'rg-1', label: 'RG Draft 1' }] }, [gfR, ssR]);
    ok('a Rose Gold partner alone reads exactly as before', rg.short === 'orders shared with RG Draft 1: it joins this set by itself (a green line is cut automatically) once it is ready');
    const gate = RD.setGate({ setId: 'set-1', name: 'Set-1', seq: 1, sheetIds: ['gf-1', 'ss-1'], cardinalWait: [{ id: 'k14-1', label: '14K Draft 1', manual: true }] }, [gfR, ssR]);
    ok('Approve for laser cutting stays grey with that line, a hold (no "breaks your rules")', !gate.ready && gate.blockers[0].rule === 'setPrinciple' && /waiting for 14K Draft 1: approve it and cut its green line/.test(gate.reason) && !/breaks your rules/.test(gate.reason), gate.reason);
  }

  /* ── 3 · the Library: greyed inside its set, never one of its sheets ── */
  const LG = code.slice(code.indexOf('function libraryGroups(sets, sheets)'), code.indexOf('let _cache = null, libraryRequest'));
  assert(LG.startsWith('function libraryGroups') && /function goldPendingOf\(groups\)/.test(LG) && /function goldPendingTile\(st, r\)/.test(LG), 'the page code to test was found');
  const row = (id, metal, o = {}) => Object.assign({ id, metal, runId: 'run-k', day: '2026-10-26', page: 1, draft: true, setId: null, releaseFull: true, placedCount: 3, charmCount: 3, density: .8, poolIds: [], orders: [] }, o);
  const pool = (r, ...ids) => Object.assign(r, { poolIds: ids, orders: [...new Set(ids.map(i => i.split('_')[0]))] });
  const libRows = cut => [
    pool(row('gf-1', 'gold', { setId: 'set-1', draft: false, sheetIndex: 1, solidIncluded: null }), '7001_1_1', '7002_1_1', '7006_1_1'),
    pool(row('ss-1', 'silver', { setId: 'set-1', draft: false, sheetIndex: 1, solidIncluded: null }), '7001_2_1'),
    pool(row('k14-1', 'gold14k', cut ? { setId: 'set-1', draft: false, sheetIndex: 1, roseCutAt: 1761480000000, solidIncluded: true } : { releaseFull: false, density: .4, solidIncluded: false }), '7002_2_1', '7004_1_1'),
    pool(row('k10-1', 'gold10k', { releaseFull: false, density: .3, solidIncluded: false }), '7006_2_1', '7007_1_1'),
    pool(row('k10-9', 'gold10k', { releaseFull: false, density: .2, solidIncluded: false }), '7999_1_1')      // (a 10K sheet that shares nothing: it stays in the "14K / 10K Solid" group)
  ];
  const groupsOf = rows => {
    const lc = vm.createContext({ SO, O, console, SetRepair: SRp, _cache: { sheets: rows }, esc: s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])), Engrave: { backsMarkup: () => '' }, cors: x => x,
      window: { CharmNestSetRules: SR, SetRepair: SRp, CharmNestSheetName: SN, SharedOrders: { core: SO.core, sheets: () => rows.map(r => SO.core.sheetOf(r, { label: SN.name(r) })).filter(Boolean) } }, allSheets: () => [] });
    vm.runInContext(LG, lc);
    const sheetIds = rows.filter(r => r.setId === 'set-1' && !r.draft).map(r => r.id);
    lc.__s = [{ setId: 'set-1', seq: 1, name: 'Set-1', day: '2026-10-26', runId: 'run-k', status: 'awaiting review', sheetIds }]; lc.__r = rows;
    return { lc, groups: vm.runInContext('libraryGroups(__s, __r)', lc) };
  };
  {
    const rows = libRows(false), { lc, groups } = groupsOf(rows), g = groups.find(x => x.setId === 'set-1'), solid = groups.find(x => x.key === 'solid-waiting');
    const k14 = rows.find(r => r.id === 'k14-1'), k10 = rows.find(r => r.id === 'k10-1'), n14 = SN.name(k14), n10 = SN.name(k10);
    ok('the set card draws BOTH waiting sheets inside it, greyed (goldPending), never as sheets of the set', g && (g.goldPending || []).map(r => r.id).sort().join() === 'k10-1,k14-1' && g.sheets.map(r => r.id).sort().join() === 'gf-1,ss-1', JSON.stringify({ pending: (g.goldPending || []).map(r => r.id), sheets: g.sheets.map(r => r.id) }));
    ok('they left the "14K / 10K Solid" group; a 10K sheet that shares nothing stays there', solid && solid.sheets.map(r => r.id).join() === 'k10-9', JSON.stringify(solid && solid.sheets.map(r => r.id)));
    ok('the set waits for them by a person\'s press (cardinalWait manual)', (g.cardinalWait || []).filter(x => x.manual).map(x => x.id).sort().join() === 'k10-1,k14-1', JSON.stringify(g.cardinalWait));
    ok('no red line and no Rebuild for it', !g.setBreaks, JSON.stringify(g.setBreaks));
    const gate = RD.setGate(g, rows.filter(r => r.setId === 'set-1'));
    ok('its Approve for laser cutting is grey with the waiting line', !gate.ready && gate.reason.includes(`waiting for ${n14} and ${n10}: approve them and cut their green lines`) || gate.reason.includes(`waiting for ${n10} and ${n14}: approve them and cut their green lines`), gate.reason);
    const html = vm.runInContext('goldPendingTile', lc)(g, k14);
    ok('its tile: greyed, its plain line, its one press, no QR label row and no Approve of its own', /class="librarySheet goldPending" data-gold-pending="k14-1"/.test(html) && /<button type="button" class="btn sage sm" data-gold-cut="k14-1">Approve &amp; cut green line<\/button>/.test(html) && html.includes(`Not in Set-1 yet: ${n14} waits for you to approve it and cut its green line. Until then its charms can be moved by hand.`) && !/data-laser-sheet|approveBox|data-label/.test(html), html);
    ok('the card draws the tiles and wires the press; no rail, no Approve and no set download for them', code.includes('${(st.goldPending || []).map(r => goldPendingTile(st, r)).join("")}') && code.includes('SetRepair.wirePending(card, st)') && code.includes(".librarySheet:not([data-gold-pending])')];") && code.includes("'.sheetsRow > .librarySheet:not([data-gold-pending])'") && fs.readFileSync(path.join(root, 'charm-nest-export-ui.js'), 'utf8').includes(".filter(c=>!c.closest('[data-gold-pending]'))"));
  }
  {
    const rows = libRows(true); rows.find(r => r.id === 'k10-1').roseCutAt = 1761480000001; Object.assign(rows.find(r => r.id === 'k10-1'), { setId: 'set-1', draft: false, sheetIndex: 1, solidIncluded: true });
    const { groups } = groupsOf(rows), g = groups.find(x => x.setId === 'set-1');
    ok('once cut, each is a sheet of the set, drawn as any (no grey tile) and the hold lifts', g.sheets.map(r => r.id).sort().join() === 'gf-1,k10-1,k14-1,ss-1' && !(g.goldPending || []).length && !(g.cardinalWait || []).length && !g.setBreaks, JSON.stringify({ sheets: g.sheets.map(r => r.id), pending: g.goldPending, wait: g.cardinalWait }));
    const rules = RD.setRules(g, rows.filter(r => r.setId === 'set-1'));
    ok('the set rules say nothing more against it', rules.applies && rules.ok, JSON.stringify(rules));
  }
  if (JSDOM) {
    // the set download of a card that holds a waiting tile: the set's own sheets only (charm-nest-export-ui.js, the real module in jsdom)
    const dom = new JSDOM('<div id="libBody"><div class="setCard"><div class="sh"></div><div class="sheetsRow"><article class="librarySheet"><div class="libCard" data-id="gf-1"><div class="h"></div></div></article><article class="librarySheet"><div class="libCard" data-id="ss-1"><div class="h"></div></div></article><article class="librarySheet goldPending" data-gold-pending="k14-1"><div class="libCard" data-id="k14-1"><div class="h"></div></div></article></div></div></div>', { runScripts: 'outside-only', pretendToBeVisual: true });
    dom.window.eval(fs.readFileSync(path.join(root, 'charm-nest-export-ui.js'), 'utf8')); await settle(10);
    const b = dom.window.document.querySelector('[data-export-set]');
    ok('the set\'s .ai / .dxf download leaves the waiting sheet out (its own sheet card keeps its own download)', !!b && JSON.parse(b.dataset.exportSet).join() === 'gf-1,ss-1' && !!dom.window.document.querySelector('[data-gold-pending] [data-export-one="k14-1"]'), b && b.dataset.exportSet);
  } else console.log('  -   jsdom is not installed: the download check in a page was not run');

  /* ── 3b · the press: the window first, Cancel changes nothing, the Cut Sheet press after the yes ── */
  {
    const page = { sheetId: 'k14-1', metal: 'gold14k', draft: true, setId: null }, said = [], cutCalls = [], asked = [];
    const st = { setId: 'set-1', seq: 1, name: 'Set-1', goldPending: [row('k14-1', 'gold14k', { releaseFull: false })] };
    const env = yes => ({ by: () => 'Pat Lee', cloud: () => true, pages: () => [page], holding: () => false, say: (t, k) => said.push([t, k || 'bad']), refresh: () => {}, confirmCut: async words => { asked.push(words); return yes; },
      cut: async (ids, by) => { cutCalls.push([ids.slice(), by]); page.roseCutAt = 1761480000000; page.setId = 'set-1'; page.draft = false; return ids.map(id => ({ id, cut: true })); } });
    let r = await SRp.cutPending(st, 'k14-1', { env: env(false) });
    ok('Cancel in its window changes nothing', r.cancelled === true && cutCalls.length === 0 && !page.roseCutAt, JSON.stringify(r));
    const w0 = asked[0];
    ok('the window says what the press does: approve, draw and record the cut, join the set; a recorded cut is permanent', /^Approve and cut 14K Draft 1\?$/.test(w0.title) && w0.confirm === 'Approve & cut' && w0.cancel === 'Cancel' && w0.lines.some(l => /green dash line is drawn and dated, and the cut is recorded with your name/.test(l.text)) && w0.lines.some(l => /joins Set 1, and Set 1 no longer waits for it/.test(l.text)) && /A recorded cut is permanent/.test(w0.what), JSON.stringify(w0));
    r = await SRp.cutPending(st, 'k14-1', { env: Object.assign(env(true), { by: () => '' }) });
    ok('nobody signed in: refused, nothing cut', r.ok === false && r.refused.key === 'who' && cutCalls.length === 0);
    r = await SRp.cutPending(st, 'k14-1', { env: Object.assign(env(true), { pages: () => [Object.assign({}, page, { dirty: true })] }) });
    ok('a sheet being saved: refused, nothing cut', r.ok === false && r.refused.key === 'busy' && cutCalls.length === 0, JSON.stringify(r));
    r = await SRp.cutPending(st, 'k14-1', { env: env(true) });
    ok('after the yes: the Cut Sheet press, with the person\'s name; it is in its set', r.ok === true && cutCalls.length === 1 && cutCalls[0][0].join() === 'k14-1' && cutCalls[0][1] === 'Pat Lee' && /Approved and cut: 14K Draft 1 is in Set 1\./.test(r.words), JSON.stringify(r));
    r = await SRp.cutPending(st, 'k14-1', { env: Object.assign(env(true), { cut: async () => { throw new Error('Complete every item in the sheet’s orders and its production checks before recording its cut'); } }) });
    ok('a refused cut says why, in the server\'s words', r.ok === false && /production checks/.test(r.error), JSON.stringify(r));
    if (JSDOM) {
      const dom = new JSDOM(`<div class="setCard"><div data-gold-pending-note><button data-gold-cut="k14-1">Approve &amp; cut green line</button><span data-gold-say hidden></span></div></div>`), card = dom.window.document.querySelector('.setCard');
      page.roseCutAt = null; const n = cutCalls.length;
      SRp.wirePending(card, st, { env: env(true) }); card.querySelector('[data-gold-cut]').click(); await settle(5);
      const say = card.querySelector('[data-gold-say]');
      ok('its button runs that press and says what came of it', cutCalls.length === n + 1 && !say.hidden && /Approved and cut/.test(say.textContent), say.textContent);
    }
  }

  /* ── 4 · manual moves while it waits ── */
  {
    const t = karatWorld(), { w, gf, k14 } = t;
    await w.assemble(); await settle();
    const k14b = w.page('gold14k', Object.assign({ releaseFull: false, density: .2 }, t.base)); w.put(k14b, '7008_1_1', '7009_1_1');
    await w.assemble(); await settle();
    ok('a second 14K sheet that shares nothing waits for no set', !t.G('goldWaitHome')(k14b), JSON.stringify(k14b.cardinalHome));
    if (JSDOM) {
      const dom = new JSDOM('<!doctype html><body></body>', { runScripts: 'outside-only' }), hw = dom.window;
      hw.setInterval = () => 0; hw.allSheets = () => w.pages;
      hw.SheetWin = { isOpen: () => false, hopKit: { W: {}, ridOf: c => String(c.order), rowsOfOrder: () => [], cancelled: () => false, word: sh => SN.name(sh), sentToStation: () => false, sheetWorking: () => false, flow: () => '' } };
      hw.eval(fs.readFileSync(path.join(root, 'charm-nest-sheet-hop.js'), 'utf8'));
      const H = hw.SheetHop, piece = k14.charms.find(c => c.poolId === '7002_2_1'), u = H.unitOf(k14, piece, k14b);
      hw.Gate = { goldWaitHome: sh => t.G('goldWaitHome')(sh) };      // (the run's real Gate)
      ok('the sheet hop: the waiting 14K sheet gives a charm of a shared order to another 14K sheet, no "tie" refusal', H.sheetRefusal(k14) === '' && H.sheetRefusal(k14b) === '' && H.unitRefusal(u, k14, k14b) === '', H.unitRefusal(u, k14, k14b));
      const back = H.unitOf(k14b, k14b.charms[0], k14);
      ok('and takes one in', H.unitRefusal(back, k14b, k14) === '', H.unitRefusal(back, k14b, k14));
      hw.Gate = { goldWaitHome: () => null };
      ok('(the same move between two sheets that wait for no set keeps the rule of orders that span sheets, as before)', /would tie/.test(H.unitRefusal(u, k14, k14b)), H.unitRefusal(u, k14, k14b));
      hw.Gate = { goldWaitHome: sh => t.G('goldWaitHome')(sh) };
      const lone = { ...H.unitOf(k14, piece, k14b) }; lone.pieces = k14.charms.filter(c => !c.excluded).map(c => ({ c, src: k14, placed: true }));
      ok('the physical checks stand: the sheet is never emptied by a move', /would be left empty/.test(H.unitRefusal(lone, k14, k14b)), H.unitRefusal(lone, k14, k14b));
      t.k14Cut = () => { k14.roseCutAt = Date.now(); };
      t.hop = H;
    } else console.log('  -   jsdom is not installed: the sheet hop checks were not run');
    // Manual Control's guard (charm-nest-sheetwin.js mcBlock, the real function)
    const sw = fs.readFileSync(path.join(root, 'charm-nest-sheetwin.js'), 'utf8'), a = sw.indexOf('  const MANUAL_METALS = '), b = sw.indexOf('  /** Why this one charm stays where it is');
    assert(a > 0 && b > a, 'mcBlock was found');
    const mc = vm.createContext({ console, W: {}, HOP: { writing: new Set() }, busy: () => false, sentToStation: () => false, SV: () => ({ makeSheetGrid: 1, prepareVariant: 1, verify: 1 }), MC_OWED: 'owed', mcFlagOnly: () => false, Sets: { ofRun: () => [] }, window: { Sets: { ofRun: () => [] } }, allSheets: () => w.pages, Date });
    vm.runInContext(sw.slice(a, b) + '\nthis.mcBlock = mcBlock;', mc);
    ok('Manual Control is open on the waiting 14K sheet', mc.mcBlock(k14, null, { api: true }) === '', mc.mcBlock(k14, null, { api: true }));
    k14.roseCutAt = Date.now();
    ok('once its green line is cut, its layout is locked (Manual Control and the hop)', /green line has been cut: its layout can no longer change/.test(mc.mcBlock(k14, null, { api: true })) && (!t.hop || /green line cut/.test(t.hop.sheetRefusal(k14))), mc.mcBlock(k14, null, { api: true }));
  }

  /* ── 5 · the person's press: in its set, the hold lifts; a refused cut takes the include back ── */
  {
    const t = karatWorld({ k10: true }), { w, gf, k14, k10 } = t;
    await w.assemble(); await settle();
    const set = w.sets().find(s => s.setId === gf.setId);
    ok('both the 14K and the 10K sheet wait greyed in Set 1', !w.inSet(k14) && !w.inSet(k10) && (t.G('goldWaitHome')(k14) || {}).setId === set.setId && (t.G('goldWaitHome')(k10) || {}).setId === set.setId, w.word());
    // a press whose cut the server refuses: the include it made is taken back, the sheet waits greyed again, its own tick as it was
    await t.G('cutInclude')(k14);
    ok('the press puts the 14K sheet in its set ("either": the 10K sheet waiting in the same set does not stop it)', w.inSet(k14) && k14.setId === set.setId, w.word());
    const undone = await t.G('undoGoldJoin')(k14, { inSet: false });
    ok('a press whose cut is not recorded takes its include back: the sheet waits greyed again', undone === true && !w.inSet(k14) && k14.solidPick === undefined && (t.G('goldWaitHome')(k14) || {}).setId === set.setId, w.word() + ' pick=' + k14.solidPick);
    ok('the set is held for it again, still no rule broken', setRow(w, set).canApprove === false && w.selfCheck().violations.length === 0 && w.selfCheck().partners.some(p => p.sheet === k14.sheetId));
    // the press that goes through: included, the cut recorded (the server's answer), and it stays
    await t.G('cutInclude')(k14); k14.roseCutAt = Date.now(); await w.assemble(); await settle();
    ok('cut: the 14K sheet is a sheet of Set 1 and stays there', w.inSet(k14) && k14.setId === set.setId && !t.G('goldWaitHome')(k14), w.word());
    let res = w.selfCheck();
    ok('the set no longer waits for it; it still waits for the 10K sheet', !res.partners.some(p => p.sheet === k14.sheetId) && res.partners.some(p => p.sheet === k10.sheetId && p.set === set.setId) && res.violations.length === 0 && setRow(w, set).canApprove === false, JSON.stringify(res.partners));
    await t.G('cutInclude')(k10); k10.roseCutAt = Date.now(); await w.assemble(); await settle();
    res = w.selfCheck();
    ok('"both": the 10K sheet\'s own press puts it in too; the hold lifts: nothing left to wait for, nothing broken', w.inSet(k10) && k10.setId === set.setId && res.violations.length === 0 && !res.partners.some(p => p.set === set.setId) && setRow(w, set).canApprove === true, JSON.stringify({ word: w.word(), partners: res.partners, v: res.violations }));
    vm.runInContext('Gate.autoCut(B.run)', w.c); await settle(); await w.assemble(); await settle();
    ok('and nothing automatic moves them afterwards', w.inSet(k14) && w.inSet(k10) && k14.setId === set.setId && k10.setId === set.setId, w.word());
  }

  /* ── 2b · the server: the set's approval is refused while it waits, in the page's words (bridge-server.cjs: the real charmNestLibrary handler, in memory) ── */
  {
    const { start } = require('./bridge-server.cjs');
    const S = 'Charm_Nest_Sheets', SET = 'Charm_Nest_Sets', srv = await start({ receipts: [] }), { st } = srv, now = Date.now(), ts = { toMillis: () => now }, day = '2026-10-26';
    const post = async body => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }); return { status: r.status, ...await r.json() }; };
    const image = 'data:image/svg+xml,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="30" height="20"><rect width="30" height="20"/></svg>');
    const mk = (id, pools, o = {}) => st.put(S, id, { id, runId: 'run-k', metal: 'gold', day, status: 'complete', placedCount: pools.length, charmCount: pools.length, density: .8, releaseFull: true, stock: { wIn: 6, hIn: 4.5 }, poolIds: pools, orders: [...new Set(pools.map(p => p.split('_')[0]))], verification: { ok: true },
      outputs: { ai: { path: id + '.ai', url: srv.sorterOrigin + '/' + id + '.ai' }, preview: { path: id + '.png', url: image } }, label: { files: [{ path: id + '-qr.png', url: image, payload: 'x', orders: [] }], orders: [] }, sheetIndex: 1, updatedAt: ts, createdAt: ts, ...o });
    try {
      st.put(SET, 'set-k', { setId: 'set-k', seq: 1, day, runId: 'run-k', sheetIds: ['gf-k', 'ss-k'], materials: [], orders: {}, status: 'nesting', updatedAt: ts, createdAt: ts });
      mk('gf-k', ['4210178745_1_1', '4210000001_1_1'], { setId: 'set-k', setSeq: 1 }); mk('ss-k', ['4210000001_2_1'], { setId: 'set-k', setSeq: 1, metal: 'silver' });
      mk('k14-k', ['4210178745_2_1', '4210178745_3_1'], { metal: 'gold14k', draft: true, setId: null, releaseFull: false, density: .5, solidIncluded: false });
      const r = await post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'seal', kind: 'set', id: 'set-k' }] });
      ok('the server refuses to approve the set while the 14K sheet waits, naming the press', r.status === 409 && /approve it and cut its green line \(Approve & cut green line, on the set's card in the Library\)/.test(r.error || ''), JSON.stringify([r.status, r.error]));
    } finally { srv.close(); }
  }

  console.log('PASS: gold-karat-manual-cut: 10K / 14K never cut or approved by themselves (Rose Gold still is), the set held with one plain line and no rule break, the sheet drawn greyed inside its set, its charms free to move until its press, then a full member and the hold lifted');
})().catch(e => { console.error(e); process.exit(1); });
