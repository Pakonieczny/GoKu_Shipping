// The "In current set" switch of the Options window works for a sheet of a COMMITTED set (Paul, 7 Oct 2026: "This toggle is not operational").
// Cause it covers: the switch was a checkbox DISABLED for every sheet of a committed set (Gate.renderRelease: include.disabled = !membershipEditable(sh)), so a press did
// nothing and said nothing, and its handler only knew the open run's own Include. Now it is never disabled; a sheet of a committed set goes out of its set and back
// in through LibraryFlow (the Library's own plan / commit, op flowApply step setMember), the pill shows the press and a labelled spinner while it saves and follows the
// truth after, and a press the rule refuses shakes the switch and says why in one short line beside it (never a tooltip), for a few seconds.
// The page's own Gate and Options window (jsdom), the REAL LibraryFlow and SetEdit, and the real charmNestLibrary handler over the in-memory shop of bridge-server.cjs.
// A fixture only: nothing here touches the live site, Etsy or a paid service.
//   node tests/charm-nest/options-include-switch.cjs      (needs jsdom: the test says so and passes nothing when it is missing)
const assert = require('node:assert/strict'), fs = require('node:fs');
let JSDOM; try { ({ JSDOM } = require('jsdom')); } catch (_) { console.log('options-include-switch: SKIPPED (jsdom is not installed)'); process.exit(0); }
const { start } = require('./bridge-server.cjs');
const LF = require('../../charm-nest-flow.js');
const O = require('../../charm-nest-orders'), R = require('../../charm-nest-rose');
const S = 'Charm_Nest_Sheets', SET = 'Charm_Nest_Sets', RUN = 'Charm_Nest_Runs';
const wait = ms => new Promise(r => setTimeout(r, ms));
const until = async (f, what) => { for (let n = 0; n < 600; n++) { if (f()) return; await wait(5); } assert.fail('timed out waiting for ' + what); };

(async () => {
  const srv = await start({ receipts: [] }), { st } = srv, now = Date.now(), ts = { toMillis: () => now }, day = '2026-10-03';
  try {
    // ── the shop: Set 1 committed with two sheets (Paul's RG Sheet 1 and a 14K sheet); Set 5 has ONE sheet; Set 6 holds two sheets that share an order; Set 7 has a completed sheet; Set 8 is still open ──
    let gateWrite = null;   // a test may hold the write of a press to look at the switch while it saves
    const post = async body => { if (gateWrite && body.op === 'flowApply') await gateWrite; const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }); return { status: r.status, ...await r.json() }; };
    const image = 'data:image/svg+xml,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="30" height="20"><rect width="30" height="20"/></svg>');
    const mk = (id, metal, pieces, extra = {}) => st.put(S, id, { id, runId: 'run-x', metal, day, status: 'complete', draft: !extra.setId, placedCount: pieces.length, charmCount: pieces.length, density: .36, stock: { wIn: 6, hIn: 4.5 },
      poolIds: pieces, orders: [...new Set(pieces.map(p => p.split('_')[0]))], verification: { ok: true }, outputs: { ai: { path: id + '.ai', url: srv.sorterOrigin + '/' + id + '.ai' }, preview: { path: id + '.png', url: image } },
      page: 1, sheetIndex: extra.setId ? 1 : null, updatedAt: ts, createdAt: ts, ...extra });
    const file = (sheetId, setId) => ({ sheetId, sheet: sheetId, path: `${setId}/${sheetId}-qr.png`, url: image, payload: 'p-' + sheetId, orders: [] });
    const mkSet = (setId, sheetIds, extra = {}) => st.put(SET, setId, { setId, seq: +setId.replace(/\D/g, ''), day, runId: 'run-x', sheetIds, materials: ['gold'], orders: {}, status: 'complete', committedAt: now - 9000, committed: [], labelFiles: sheetIds.map(i => file(i, setId)),
      labels: { pdf: { path: 'x.pdf', url: 'u' } }, processSeals: [{ how: 'approved', by: 'Ann', at: now - 5000 }], updatedAt: ts, createdAt: ts, ...extra });
    const seal = { processSeals: [{ how: 'approved', by: 'Ann', at: now - 6000 }], processReady: true };
    mkSet('set-1', ['rg-1', 'gk-2'], { materials: ['rose', 'gold'] });
    mk('rg-1', 'rose', ['5100000004_1_1'], { setId: 'set-1', setSeq: 1, sheetIndex: 1, ...seal, roseStockId: 'stock-1' });   // (re-seated: the cut mark was set aside, the sheet owes a new cut)
    mk('gk-2', 'gold14k', ['5100000002_1_1'], { setId: 'set-1', setSeq: 1, sheetIndex: 2, ...seal });
    mkSet('set-5', ['only5']); mk('only5', 'gold14k', ['5100000050_1_1'], { setId: 'set-5', setSeq: 5, ...seal });
    mkSet('set-6', ['sh-a', 'sh-b']); mk('sh-a', 'gold14k', ['4171450075_1_1', '5100000061_1_1'], { setId: 'set-6', setSeq: 6, ...seal }); mk('sh-b', 'gold14k', ['4171450075_2_1'], { setId: 'set-6', setSeq: 6, sheetIndex: 2, ...seal });
    mkSet('set-7', ['c7-a', 'c7-b']); mk('c7-a', 'gold14k', ['5100000070_1_1'], { setId: 'set-7', setSeq: 7, ...seal, laserDoneAt: now - 3000, laserDoneBy: 'Ben' }); mk('c7-b', 'gold14k', ['5100000071_1_1'], { setId: 'set-7', setSeq: 7, sheetIndex: 2, ...seal });
    mkSet('set-9', ['c9-a', 'c9-b']); mk('c9-a', 'rose', ['5100000090_1_1'], { setId: 'set-9', setSeq: 9, ...seal, roseCutAt: now - 2000, roseStockId: 'stock-9' }); mk('c9-b', 'gold14k', ['5100000091_1_1'], { setId: 'set-9', setSeq: 9, sheetIndex: 2, ...seal });
    mkSet('set-10', ['k10-a', 'k10-b']); mk('k10-a', 'gold14k', ['5100000100_1_1'], { setId: 'set-10', setSeq: 10, ...seal }); mk('k10-b', 'gold14k', ['5100000101_1_1'], { setId: 'set-10', setSeq: 10, sheetIndex: 2, ...seal });
    mkSet('set-8', ['o8-s', 'o8-t'], { status: 'open', committedAt: null, committed: undefined, labelFiles: [] }); mk('o8-s', 'gold14k', ['5100000080_1_1'], { setId: 'set-8', setSeq: 8 }); mk('o8-t', 'gold14k', ['5100000081_1_1'], { setId: 'set-8', setSeq: 8, sheetIndex: 2 });
    st.put(RUN, 'run-x', { runId: 'run-x', status: 'review', lines: {} });
    const PERMANENT = ['processSeals', 'processReady', 'laserDoneAt', 'laserDoneBy', 'roseCutAt', 'roseStockId', 'committedAt', 'status'];
    const permanent = () => JSON.stringify([...st.list(S), ...st.list(SET)].map(d => [d._id, ...PERMANENT.map(k => d[k] === undefined ? null : d[k])]));
    const everything = () => JSON.stringify([...st.docs.entries()].filter(([k]) => !k.startsWith('Charm_Nest_Rev/')));
    const sheet = id => st.doc(S, id), set = id => st.doc(SET, id), held = id => !!(sheet(id).laserHold && sheet(id).laserHold.at);

    // ── the page: its Gate (renderRelease and the switch), the Options window, SetEdit (the pages follow what the server wrote), LibraryFlow (the Library's own plan and commit) ──
    const dom = new JSDOM('<div id="toasts"></div><div id="cards"></div>', { url: 'https://example.test', runScripts: 'outside-only' }), w = dom.window, doc = w.document;
    w.IntersectionObserver = undefined; w.CharmNestOrders = O; w.CharmNestRose = R; w.confirm = () => true;
    const toasts = [], WPT = 100 / 25.4 * 72, HPT = 50 / 25.4 * 72, pages = [], opsBusy = { commit: false };
    const sayTimers = []; const realST = w.setTimeout.bind(w); w.setTimeout = (f, ms, ...a) => { if (ms === 5200) { sayTimers.push(f); return sayTimers.length; } return realST(f, ms, ...a); };   // (the line's few seconds: fired by hand)
    w.CharmNestOperations = { running: k => opsBusy.commit && k === 'commit:run-x', run: async (o, f) => f({}) };
    const openSet = { setId: 'set-8', runId: 'run-x', seq: 8, group: 'dispatch', committedAt: null, sheetIds: ['o8-s', 'o8-t'] };
    const committed1 = { setId: 'set-1', runId: 'run-x', seq: 1, group: 'dispatch', committedAt: now - 9000, sheetIds: ['rg-1', 'gk-2'] };
    const committed5 = { setId: 'set-5', runId: 'run-x', seq: 5, group: 'dispatch', committedAt: now - 9000, sheetIds: ['only5'] };
    const committed6 = { setId: 'set-6', runId: 'run-x', seq: 6, group: 'dispatch', committedAt: now - 9000, sheetIds: ['sh-a', 'sh-b'] };
    const committed7 = { setId: 'set-7', runId: 'run-x', seq: 7, group: 'dispatch', committedAt: now - 9000, sheetIds: ['c7-a', 'c7-b'] };
    const committed10 = { setId: 'set-10', runId: 'run-x', seq: 10, group: 'dispatch', committedAt: now - 9000, sheetIds: ['k10-a', 'k10-b'] };
    const committed9 = { setId: 'set-9', runId: 'run-x', seq: 9, group: 'dispatch', committedAt: now - 9000, sheetIds: ['c9-a', 'c9-b'] };
    w.CN = { S: { cloud: { ok: true }, settings: { stock: {} }, mode: 'nest', sheets: {}, library: { rows: [] } }, METALS: [{ key: 'rose', label: 'RG 14/20', color: '#c08578' }, { key: 'gold14k', label: '14K Gold', color: '#d9b545' }], esc: s => String(s).replace(/</g, '&lt;'), stockFor: () => ({ wPt: WPT, hPt: HPT, wIn: WPT / 72, hIn: HPT / 72 }),
      uid: () => 'test', allSheets: () => pages, pagesOf: m => pages.filter(p => p.metal === m), drawPreview() { }, labelOf: m => (m === 'rose' ? 'RG 14/20' : '14K Gold'), renderCard() { }, toast(m, k) { toasts.push([String(m), k]); }, sheetDirty() { }, api: async () => ({}) };
    w.eval(`var C=window.CN,S=C.S,CN=C,B=window.B={run:{runId:'run-x',releasePolicy:2,status:'review',solidIncluded:{}},sets:new Map()};
 var O=window.CharmNestOrders,allSheets=()=>C.allSheets(),pagesOf=m=>C.pagesOf(m),stockFor=C.stockFor,esc=C.esc,labelOf=C.labelOf;
 var Sets={ofRun:()=>[...B.sets.values()]},RunCtl={save:async()=>{},poke(){},membershipUpdated(){}},Session=window.Session={schedule(){}},toast=C.toast,refreshAllCards=()=>{},api=C.api;
 var sheetDirty=()=>{},saveSettings=()=>{},startNest=()=>{},employeeName=()=>'Paul';`);
    const src = fs.readFileSync('charm-nest-bridge.js', 'utf8'); w.eval(src.slice(src.indexOf('const Gate ='), src.indexOf('/* ═══ 21', src.indexOf('const Gate ='))));
    w.eval(fs.readFileSync('charm-nest-set-edit.js', 'utf8')); w.eval(fs.readFileSync('charm-nest-options-modal.js', 'utf8'));
    for (const s of [committed1, committed5, committed6, committed7, committed9, committed10, openSet]) w.B.sets.set(s.setId, s);
    LF.configure({ api: post, employee: () => 'Paul', rows: () => [], live: id => { const p = pages.find(x => x.sheetId === id); return p ? { runHere: true, draft: !!p.draft, dispatchSetId: null, can: { ok: true }, split: [] } : null; },   // (the sheet window's joinInfo: a sheet on a page of the open run)
       remakeLabel: async () => { }, include: async () => { }, relabelSet: async () => { }, setFiles: async () => { }, applyMembership: async m => w.SetEdit.applyMembership(m) });
    w.LibraryFlow = LF;

    // a page copy of a saved sheet, with its own card
    const page = (id, metal, o = {}) => {
      const el = doc.createElement('section'); el.innerHTML = '<div class="shHead"></div><div class="shGate" data-r="gate"></div>'; doc.getElementById('cards').appendChild(el);
      const rec = sheet(id), charms = [{ id: 'c-' + id, poolId: rec.poolIds[0], order: rec.poolIds[0].split('_')[0] }];
      const p = Object.assign({ metal, page: 1, el, runId: 'run-x', sheetId: id, charms, placements: [{ id: 'c-' + id, cxPt: 10, cyPt: 10, angle: 0, scale: 1 }], persistedDone: true, verification: { ok: true }, status: 'complete', dirty: false, outputs: { ai: 'a' },
        draft: !rec.setId, setId: rec.setId || null, seq: rec.setSeq || null, sheetIndex: rec.sheetIndex || null, group: 'dispatch' }, o);
      pages.push(p); const gate = el.querySelector('.shGate'); w.Gate.renderRelease(p, gate); return { p, gate, el, input: gate._optInclude.querySelector('[data-solid="include"]'), inc: gate._optInclude };
    };
    const line = c => c.inc.querySelector('[data-solid="say"]'), spin = c => c.inc.querySelector('[data-solid="busy"]'), label = c => c.inc.querySelector('.osSwitch');
    const press = async (c, f) => { c.input.click(); await until(f || (() => !c.p._incBusy && !w.Gate.state().membershipPending), 'the press to finish'); };
    const refuses = async (c, words, what) => {
      const before = everything(), was = c.input.checked, shown = line(c);
      c.input.click(); await until(() => !shown.hidden, what + ': the line');
      assert.equal(shown.textContent, words, what + ': the reason, in everyday words'); assert.equal(c.input.checked, was, what + ': the switch stays in its true position'); assert(!c.input.disabled, what + ': never disabled');
      assert(label(c).classList.contains('nope'), what + ': the switch shakes'); assert(!c.inc.querySelector('[title]') && !shown.getAttribute('title'), what + ': no tooltip');
      assert.equal(everything(), before, what + ': nothing was written'); assert(!c.p._incBusy && spin(c).hidden, what + ': not busy');
      assert(sayTimers.length > 0, what + ': the line has its few seconds'); sayTimers.splice(0).pop()();   // (its time is up)
      assert(shown.hidden && shown.textContent === '' && !label(c).classList.contains('nope'), what + ': then it disappears');
    };

    // ═══ 1. Paul's sheet: RG Sheet 1 of the committed Set 1 (re-seated: no cut mark on it now), in the Options window ═══
    const rg = page('rg-1', 'rose', { seq: 1, setDay: day });
    assert.equal(w.Gate.committedSheet(rg.p), true); assert.equal(w.Gate.fixedSet(rg.p), true, 'a sheet of a committed set is one the Library edits');
    assert.equal(rg.input.checked, true, 'it is in Set 1: the switch is on'); assert.equal(rg.input.disabled, false, 'and NOT disabled (the dark grey pill that took no press)');
    const opener = rg.gate.querySelector('.sheetOptionsBtn'); opener.click();
    const dlg = doc.querySelector('dialog.osDlg'); assert(dlg && dlg.querySelector('.osHead .osIncSlot [data-solid="include"]') === rg.input, 'the switch rides in the window\'s title row, top right: the same element');
    assert.equal(rg.inc.querySelector('label').textContent.trim(), 'In current set', 'its short label is all it says'); assert(line(rg).hidden && spin(rg).hidden);
    const permanentBefore = permanent();
    // the press: the pill shows the pressed position and a labelled spinner while it saves; a second press waits and is told so
    let release; gateWrite = new Promise(r => { release = r; });
    rg.input.click(); await until(() => rg.p._incBusy, 'the press to start saving');
    assert(label(rg).classList.contains('busy') && rg.input.getAttribute('aria-busy') === 'true', 'the switch is visibly busy'); assert(!spin(rg).hidden && /Taking out/.test(spin(rg).textContent), 'with the app\'s small labelled spinner');
    assert.equal(rg.input.checked, false, 'it shows the position pressed while it saves'); w.CN.renderCard(rg.p); w.Gate.refreshMembership(); assert.equal(rg.input.checked, false, 'a repaint does not flip it back mid-save');
    rg.input.click(); await until(() => !line(rg).hidden, 'the second press to be answered'); assert.equal(line(rg).textContent, 'Still saving. One moment.'); assert.equal(rg.input.checked, false, 'and it stays at the pressed position'); sayTimers.splice(0).pop()();
    release(); gateWrite = null; await until(() => !rg.p._incBusy, 'the save'); 
    assert.equal(sheet('rg-1').draft, true); assert.equal(sheet('rg-1').setId, null, 'the sheet is out of Set 1 on the server'); assert(held('rg-1') && /^Taken out of Set 1/.test(sheet('rg-1').laserHold.note), 'held back, not pulled into another set by the run');
    assert.deepEqual(set('set-1').sheetIds, ['gk-2'], 'Set 1 keeps its other sheet'); assert.equal(set('set-1').committedAt, now - 9000, 'and stays committed'); assert.equal(permanent(), permanentBefore, 'no seal, approval, cut record or completion was touched');
    assert.equal(rg.input.checked, false, 'the pill follows the real membership: off'); assert(!label(rg).classList.contains('busy') && spin(rg).hidden && line(rg).hidden, 'no longer busy, nothing said'); assert.equal(rg.p.draft, true); assert.equal(rg.p.leftSet, 'set-1');
    assert.equal(w.Gate.committedSheet(rg.p), false, 'the page\'s set follows'); assert.deepEqual(committed1.sheetIds, ['gk-2']);
    w.B.run.solidIncluded.rose = true; w.Gate.refreshMembership(); assert.equal(rg.input.checked, false, 'a sheet a person took out stays out, even when the run includes Rose Gold'); assert.equal(w.Gate.policy(rg.p, 3).include, false); w.B.run.solidIncluded.rose = false;
    assert(toasts.length === 0, 'no pop-up repeats it');
    // and back in: Rose Gold joins a committed set only by its own Cut Sheet press (the Library says so), so the switch is the run's own Include for it (the first step of the Cut Sheet press)
    // (the run's own set-making, faked at its edges only: which open set it fills, and the files it writes for a sheet that joins; the Library and the server are not involved)
    w.B.run.solidIncluded.rose = false; const memberships = [], saved8 = w.B.sets.get('set-8'); w.B.sets.delete('set-8'); const joined = [];
    w.eval(`Object.assign(Sets,{ensure:async()=>{const s={setId:'set-8',runId:'run-x',seq:8,day:'${day}',group:'dispatch',committedAt:null,sheetIds:[],labelFiles:[],materials:[],orders:{}};B.sets.set(s.setId,s);return s;},save:async()=>{},labelsReady:()=>true,
      onSheetSaved:async(sh,c,u,o)=>{const s=o.setOverride;if(!s.sheetIds.includes(sh.sheetId))s.sheetIds.push(sh.sheetId);window.__joined.push(sh.sheetId);}});
     var Pool={update:async()=>{}},Orders={rows:()=>[]},Engrave={saveSheetBacks:async()=>{}};CN.sheetFileBase=sh=>'Set-'+sh.seq+'-Sheet-'+sh.sheetIndex;RunCtl.onSheetDone=()=>{};`);
    w.__joined = joined; w.SheetEvents = { membership: (list, on) => memberships.push([list.map(p => p.sheetId), on]) };
    await press(rg); assert(line(rg).hidden, 'a press that works says nothing'); assert.equal(rg.p.leftSet, undefined, 'a person\'s own Include lets the run take the sheet again'); assert.equal(w.B.run.solidIncluded.rose, true, 'Rose Gold is included for the run');
    assert.equal(rg.input.checked, true, 'the switch is on again'); assert.equal(JSON.stringify(memberships), JSON.stringify([[['rg-1'], true]]), 'the sheet\'s timeline records the Include');
    assert.equal(permanent(), permanentBefore, 'putting it back changes no seal either'); assert.equal(sheet('rg-1').flowHistory.map(h => h.type).join(), 'setLeave', 'the history keeps the move out');
    assert.equal(rg.p.setId, 'set-8', 'the run puts it in its current set, as it does any Rose Gold sheet'); assert.equal(rg.p.draft, false); assert.deepEqual(joined, ['rg-1']); assert.equal(toasts.length, 0, 'saved without a word of complaint');
    w.B.sets.set('set-8', saved8); pages.splice(pages.indexOf(rg.p), 1);   // (the open set goes back to the shop as it was; this page copy is done with)

    // ═══ 2. what the rule refuses: the switch stays where it is true, shakes, and says why in one short line that goes away ═══
    const only = page('only5', 'gold14k'); await refuses(only, 'Last sheet of the set. Use Undo set in the Library.', 'the last sheet of a set');
    const shared = page('sh-a', 'gold14k'); await refuses(shared, 'Shares an order with another sheet.', 'a multi-piece order shared with another sheet of the set');
    const done = page('c7-a', 'gold14k'); await refuses(done, 'Already laser cut.', 'a sheet marked Completed (laser cut)');
    const cut = page('c9-a', 'rose', { roseCutAt: now - 2000 }); await refuses(cut, 'Already cut.', 'a recorded cut');
    const free = page('k10-a', 'gold14k');
    w.CN.S.cloud.ok = false; await refuses(free, 'Offline. Try again when connected.', 'offline'); w.CN.S.cloud.ok = true;
    opsBusy.commit = true; await refuses(free, 'The set is being committed. Try again soon.', 'a set being committed'); opsBusy.commit = false;
    const saved = page('o8-s', 'gold14k', { recalled: { roseStockId: 'x' } }); await refuses(saved, 'Saved sheet. Its set is fixed.', 'a saved sheet of a set the Library does not edit');
    assert.equal(permanent(), permanentBefore, 'and no refusal touched any seal, approval or cut record');

    // ═══ 3. a gold sheet in a committed set, the 14K one: out and back in (a gold sheet is per sheet: its own pick follows) ═══
    await press(free); assert.equal(free.input.checked, false); assert.equal(sheet('k10-a').setId, null); assert.equal(free.p.solidPick, false, 'its own pick follows the truth'); assert.equal(free.p.leftSet, 'set-10');
    await press(free); assert.equal(free.input.checked, true); assert.equal(sheet('k10-a').setId, 'set-10'); assert.equal(permanent(), permanentBefore);
    // the sheet was marked Completed on another computer (this page's copy does not know): the Library's plan reads the records, so the press is refused with the reason
    st.put(S, 'k10-a', { laserDoneAt: now - 10, laserDoneBy: 'Cy' }); const beforeLate = JSON.stringify(sheet('k10-a'));
    free.p.laserDoneAt = null; free.input.click(); await until(() => !line(free).hidden, 'the late refusal'); assert.equal(line(free).textContent, 'Already laser cut.'); assert.equal(free.input.checked, true); assert.equal(JSON.stringify(sheet('k10-a')), beforeLate, 'nothing was changed'); sayTimers.splice(0).pop()();
    st.put(S, 'k10-a', { laserDoneAt: undefined, laserDoneBy: undefined });

    // ═══ 4. the open run's own Include is still the switch of every other sheet (nothing was taken from it) ═══
    const draftSheet = page('o8-t', 'gold14k', { draft: true, setId: null, seq: null }); assert.equal(w.Gate.fixedSet(draftSheet.p), false);
    draftSheet.input.click(); assert.equal(draftSheet.p.solidPick, true, 'a draft sheet is picked for the current set by the run\'s own Include'); assert(line(draftSheet).hidden, 'no refusal');
    await wait(30);

    w.document.querySelector('dialog.osDlg [data-os="close"]').click(); await until(() => !doc.querySelector('dialog.osDlg')); assert(rg.gate._optBox.contains(rg.inc) || rg.gate.contains(rg.inc), 'the switch goes home with the controls');
    console.log('options-include-switch: OK (a committed-set sheet goes out and back in by the Library\'s path with a busy spinner; each refusal shakes the switch and says why; seals, history and cut records untouched)');
  } finally { await srv.close(); }
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
