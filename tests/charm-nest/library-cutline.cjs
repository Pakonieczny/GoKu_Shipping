// The Library's green dash line window (Paul, 7 Oct 2026): a partial 10K / 14K / Rose Gold sheet dragged (or moved with "Move to...") to
// Laser cutting first asks, in the shared-orders window's own look, and only the person's yes makes the line and records the cut. Offline.
//   A  engine (LibraryFlow over the fake bridge-server shop): a partial 10K sheet's plan asks for the one `roseLine` yes with the sheet named, the plan
//      writes nothing; a plain gold sheet asks nothing; cutLine() is the Cut Sheet press (calculate with recordCut:true and the person's name), needs a
//      name, and a refusal, a warning (cut not recorded) or a throw is a plain failure that never throws;
//   B  SharedOrdersModal.ask: the same window markup, yes only from a real press after it is armed (not a script's click, not early), every other way
//      out (Cancel, X, the window closed as Esc does, a click outside) is no, a second window while one is open answers no at once;
//   C  LibraryCutLine: guess() names only partial sheets without a line moving out of In progress, the window says what it will do, make() shows the
//      spinner, the line and the tag on the card before it returns (and keeps them), a failure says why in words and leaves the card as it was.
//   node tests/charm-nest/library-cutline.cjs   (jsdom: NODE_PATH=<node_modules with jsdom> when it is not installed here)
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), { JSDOM } = require('jsdom');
const { start } = require('./bridge-server.cjs');
const root = path.join(__dirname, '../..');
const LF = require('../../charm-nest-flow.js');
const S = 'Charm_Nest_Sheets', RUN = 'Charm_Nest_Runs';
const wait = ms => new Promise(r => setTimeout(r, ms));

(async () => {
  const srv = await start({ receipts: [] }), { st } = srv, now = Date.now(), ts = { toMillis: () => now }, day = '2026-10-07';
  try {
    const post = async body => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }); return { status: r.status, ...await r.json() }; };
    const image = 'data:image/svg+xml,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="30" height="20"><rect width="30" height="20"/></svg>');
    const lines = {};
    // a sheet of one order held In progress (laserHold), so a move to Laser cutting has to lift the hold
    const mk = (id, metal, order, extra = {}) => {
      const pool = order + '_1_1'; lines[order + '_1'] = { orderId: order, state: 'written', quantity: 1, poolIds: [pool], engraveCandidate: false };
      st.put(S, id, { id, runId: 'run-x', metal, day, status: 'complete', placedCount: 1, charmCount: 1, density: .4, stock: { wIn: 6, hIn: 4.5 }, poolIds: [pool], orders: [order], verification: { ok: true },
        laserHold: { at: now - 1000, by: 'Paul' }, outputs: { ai: { path: id + '.ai', url: srv.sorterOrigin + '/' + id + '.ai' }, preview: { path: id + '.png', url: image } },
        label: { files: [{ path: id + '-qr.png', url: image, payload: order, orders: [order] }], orders: [order] }, sheetIndex: 1, updatedAt: ts, createdAt: ts, ...extra });
    };
    mk('g10-new', 'gold10k', '3900000001'); mk('g10-has-line', 'gold10k', '3900000002', { rosePlanHash: 'h-1' }); mk('gf-plain', 'gold', '3900000003');
    st.put(RUN, 'run-x', { runId: 'run-x', status: 'complete', lines });

    const calc = [], writes = () => st.calls.filter(c => c.op === 'laserStatus' || c.op === 'flowApply' || c.op === 'laserDone' || c.op === 'roseRecordCut').length;
    let mode = 'ok', by = 'Paul';
    LF.configure({
      api: post, employee: () => by, rows: () => [], live: () => null,
      cuts: m => ['rose', 'gold10k', 'gold14k'].includes(m),
      rose: () => ({
        check: async item => ({ needsLine: true, sheets: [{ sheetId: item.id, label: '10K Sheet 1', metal: 'gold10k', needsLine: true, needs: 3, source: 'live', blocked: '' }],
          confirm: { key: 'roseLine', label: 'Add the green dash line to 10K Sheet 1?', detail: 'This calculates the cut contour for these charms' } }),
        calculate: async (item, o) => {
          calc.push({ id: item.id, by: o.by, recordCut: o.recordCut });
          if (mode === 'throw') throw new Error('the cloud went away');
          if (mode === 'refuse') return { ok: false, error: '10K Sheet 1 is not open on this page', sheets: [] };
          o.onStep({ key: 'calculating', sheetId: item.id }); o.onStep({ key: 'drawn', sheetId: item.id });
          return { ok: true, lines: 3, cut: true, sheets: [{ sheetId: item.id, label: '10K Sheet 1', lineAdded: true }], warnings: mode === 'warn' ? ['The cut could not be recorded'] : [] };
        }
      })
    });

    // ── A. the engine ──
    let p = await LF.plan({ kind: 'sheet', id: 'g10-new', to: { area: 'laser' } });
    assert.equal(p.ok && p.from.area, 'progress', JSON.stringify(p));
    assert.equal(p.ok, true, JSON.stringify(p.needs));
    assert.deepEqual(p.confirm.map(c => c.key), ['roseLine'], 'a partial 10K sheet asks the one yes');
    assert.deepEqual(p.confirm[0].sheetIds, ['g10-new']); assert.equal(p.confirm[0].count, 1); assert.equal(p.confirm[0].sheets[0].label, '10K Sheet 1'); assert.equal(p.confirm[0].sheets[0].charms, 3);
    assert(p.steps.some(x => x.type === 'roseLine' && x.sheetIds[0] === 'g10-new'));
    assert.equal(writes(), 0, 'a plan writes nothing'); assert.equal(calc.length, 0, 'and calculates nothing');
    for (const id of ['g10-has-line', 'gf-plain']) { p = await LF.plan({ kind: 'sheet', id, to: { area: 'laser' } }); assert.deepEqual(p.confirm.map(c => c.key), [], id + ' asks nothing: it has its line, or it is not a cut metal'); }
    assert.equal(LF.core.needsCutLine({ metal: 'gold14k' }), true); assert.equal(LF.core.needsCutLine({ metal: 'gold14k', roseCutAt: 5 }), false); assert.equal(LF.core.needsCutLine({ metal: 'silver' }), false);

    let r = await LF.cutLine({ kind: 'sheet', id: 'g10-new', by: 'Paul', onStep: x => calc.push(x.key) });
    assert.equal(r.ok, true); assert.deepEqual(calc.slice(0, 1), [{ id: 'g10-new', by: 'Paul', recordCut: true }], 'the yes is the whole Cut Sheet press, with the signed-in name'); assert(calc.includes('drawn'), 'the steps are passed on');
    calc.length = 0; by = ''; r = await LF.cutLine({ kind: 'sheet', id: 'g10-new' }); assert.equal(r.ok, false); assert(/Sign in/.test(r.error)); assert.equal(calc.length, 0, 'no name: nothing is pressed'); by = 'Paul';
    mode = 'refuse'; r = await LF.cutLine({ kind: 'sheet', id: 'g10-new' }); assert.equal(r.ok, false); assert(/not open/.test(r.error));
    mode = 'warn'; r = await LF.cutLine({ kind: 'sheet', id: 'g10-new' }); assert.equal(r.ok, false, 'a line without its recorded cut is not done'); assert(/could not be recorded/.test(r.error) && /Cut Sheet/.test(r.error));
    mode = 'throw'; r = await LF.cutLine({ kind: 'sheet', id: 'g10-new' }); assert.equal(r.ok, false); assert(/went away/.test(r.error)); mode = 'ok';
    assert.equal(writes(), 0, 'the engine itself wrote nothing in all of it (the page does, through calculate)');

    // ── B + C. the window and the card, in a page ──
    const dom = new JSDOM('<!doctype html><html><head></head><body><div id="libBody"><div class="libCard" data-id="g10-new"><img class="pv" data-sheet-preview src="' + image + '"></div></div></body></html>', { url: 'https://example.test', runScripts: 'outside-only', pretendToBeVisual: true });
    const w = dom.window, doc = w.document;
    w.HTMLDialogElement.prototype.showModal = function () { this.setAttribute('open', ''); };
    w.HTMLDialogElement.prototype.close = function () { if (!this.hasAttribute('open')) return; this.removeAttribute('open'); this.dispatchEvent(new w.Event('close')); };
    w.HTMLCanvasElement.prototype.toDataURL = () => 'data:image/png;base64,AAAA';
    w.Motion = { reduced: () => true };
    const toasts = []; let live = { sheetId: 'g10-new', metal: 'gold10k', page: 1, placements: [1, 2, 3] };
    w.CN = { S: { library: { rows: [{ id: 'g10-new', metal: 'gold10k', sheetIndex: 1, placedCount: 3, density: .4 }, { id: 'g10-has-line', metal: 'gold10k', rosePlanHash: 'h', sheetIndex: 2 }, { id: 'gf-plain', metal: 'gold', sheetIndex: 1 }] } },
      allSheets: () => [live], stockFor: () => ({ wPt: 100, hPt: 50 }), paintPreview() {}, toast: (m, k) => toasts.push([m, k]) };
    w.LibraryFlow = LF;
    w.eval(fs.readFileSync(path.join(root, 'charm-nest-shared-orders-modal.js'), 'utf8'));
    w.eval(fs.readFileSync(path.join(root, 'charm-nest-library-cutline.js'), 'utf8'));
    const SOM = w.SharedOrdersModal, CL = w.LibraryCutLine;
    assert(typeof SOM.ask === 'function' && CL && typeof CL.make === 'function');
    const ev = (el, type, init) => el.dispatchEvent(new w.MouseEvent(type, { bubbles: true, cancelable: true, button: 0, ...init }));
    const press = el => { ev(el, 'pointerdown'); ev(el, 'pointerup'); ev(el, 'click'); };
    const dlg = () => doc.querySelector('dialog.soDlg[open]');
    const opts = (extra = {}) => ({ title: [{ b: '10K Sheet 1' }, ' needs its green dash line'], sub: 'Sub', chips: [{ label: '10K Sheet 1', here: true }, { label: 'Laser cutting' }], count: '1 sheet', note: 'Cancel changes nothing.', yes: 'Generate', no: 'Cancel', armMs: 40,
      cards: [{ key: 'g10-new', label: '10K Sheet 1', tag: 'partial sheet', facts: [['3 charms', '40% of the sheet']], lines: [{ icon: 'dash', text: 'The system generates the green dash line.' }, { icon: 'lock', text: 'A recorded cut is permanent.' }] }], ...extra });
    // B1: not before it is armed, not a script's click, then a real press is the yes
    let got = null, a = SOM.ask(opts()).then(v => { got = v; });
    await wait(5); let d = dlg(); assert(d && d.classList.contains('soDlg') && d.querySelector('.soHead .soTitle b').textContent === '10K Sheet 1' && d.querySelectorAll('.soSheets .soChip').length === 2 && d.querySelector('.soCount').textContent === '1 sheet' && d.querySelectorAll('.soGrid .soCard').length === 1 && d.querySelector('.soCutDash') && d.querySelector('[data-no]') && d.querySelector('[data-yes]'), 'the same window markup');
    assert.equal(doc.activeElement, d.querySelector('[data-no]'), 'the safe button has the focus');
    press(d.querySelector('[data-yes]')); await wait(5); assert.equal(got, null, 'a press before it is armed is no yes');
    await wait(60); ev(d.querySelector('[data-yes]'), 'click'); await wait(5); assert.equal(got, null, 'a click nobody pressed is no yes');
    press(d.querySelector('[data-yes]')); await a; assert.equal(got, true, 'a real press after it is armed is the yes'); assert(!dlg(), 'and the window closes');
    // B2: every other way out is no
    for (const how of ['no', 'x', 'esc', 'outside']) {
      got = null; a = SOM.ask(opts({ armMs: 0 })).then(v => { got = v; }); await wait(5); d = dlg(); assert(d, how);
      if (how === 'no') press(d.querySelector('[data-no]')); else if (how === 'x') ev(d.querySelector('[data-x]'), 'click'); else if (how === 'esc') d.close(); else ev(d, 'click');
      await a; assert.equal(got, false, how + ' is no'); await wait(5);
    }
    // B3: one window at a time
    got = null; a = SOM.ask(opts({ armMs: 0 })).then(v => { got = v; }); await wait(5);
    assert.equal(await SOM.ask(opts()), false, 'a second window answers no at once'); assert(dlg(), 'the first stays'); press(dlg().querySelector('[data-no]')); await a;

    // C1: guess
    const item = { kind: 'sheet', id: 'g10-new' };
    assert.equal(JSON.stringify(CL.guess(item, { area: 'laser' }, 'progress').hits), '["g10-new"]');
    assert.equal(CL.guess(item, { area: 'laser' }, 'laser'), null, 'only out of In progress'); assert.equal(CL.guess(item, { area: 'nowhere' }, 'progress'), null);
    assert.equal(CL.guess({ kind: 'sheet', id: 'g10-has-line' }, { area: 'laser' }, 'progress'), null, 'it has its line'); assert.equal(CL.guess({ kind: 'sheet', id: 'gf-plain' }, { area: 'laser' }, 'progress'), null, 'not a cut metal');
    // C2: the window says what it will do
    calc.length = 0; p = await LF.plan({ kind: 'sheet', id: 'g10-new', to: { area: 'laser' } });
    let ans = null; const asked = CL.ask(item, p.confirm[0], { dest: 'Laser cutting', fromName: 'In progress' }).then(v => { ans = v; });
    await wait(5); d = dlg(); const text = d.textContent;
    assert(/10K Sheet 1/.test(d.querySelector('.soTitle').textContent) && /needs its green dash line/.test(d.querySelector('.soTitle').textContent), text);
    assert(/system will generate the green dash line/.test(d.querySelector('.soSub').textContent) && /3 charms/.test(text) && /40% of the sheet/.test(text) && /permanent/.test(text) && /stays in In progress/.test(text) && d.querySelector('img'), text);
    assert.equal(d.querySelector('[data-no]').textContent, 'Cancel'); assert.equal(writes(), 0, 'nothing is drawn or recorded before the yes');
    press(d.querySelector('[data-no]')); await asked; assert.equal(ans, false); assert.equal(calc.length, 0, 'Cancel calls nothing');
    // C3: make(): spinner, then the line and its tag on the card, kept
    const card = doc.querySelector('.libCard[data-id=g10-new]'); let sawSpin = false, sawLine = false;
    const made = CL.make({ ids: ['g10-new'], el: card, run: async onStep => {
      sawSpin = !!card.querySelector('.clBusy .clSpin') && /Working out the green dash line/.test(card.querySelector('.clBusy').textContent);
      live = { ...live, rosePlan: { lines: [1, 2], stages: [{ n: 1, at: now }] } }; onStep({ key: 'drawn', sheetId: 'g10-new' }); sawLine = !!card.querySelector('.clLine img') && /Green line 1/.test(card.querySelector('.clTag').textContent);
      return { ok: true, lines: 2 };
    } });
    const t0 = Date.now(); r = await made;
    assert.equal(r.ok, true); assert(sawSpin, 'a labelled spinner while the line is worked out'); assert(sawLine, 'the line and its tag are on the card as soon as it is drawn');
    assert(card.querySelector('.clLine') && !card.querySelector('.clBusy') && /Green line 1/.test(card.querySelector('.clTag').textContent), 'the line stays, the spinner is gone');
    assert(Date.now() - t0 >= 700, 'the line is held in view before anything moves');
    card.querySelector('.clLine').remove(); CL.restore(doc.getElementById('libBody')); assert(card.querySelector('.clLine'), 'a redrawn card gets the line back');
    // C4: a failure says why and leaves the card as it was
    card.querySelector('.clLine').remove(); live = { sheetId: 'g10-new', metal: 'gold10k', page: 1, placements: [1] };
    r = await CL.make({ ids: ['g10-new'], el: card, run: async () => ({ ok: false, error: '10K Sheet 1 is not open on this page' }) });
    assert.equal(r.ok, false); assert(toasts.some(([m, k]) => /not open on this page\. Nothing was moved\./.test(m) && k === 'bad'), JSON.stringify(toasts)); assert(card.querySelector('.clErr') && /Nothing was moved/.test(card.querySelector('.clErr').textContent) && !card.querySelector('.clBusy'));
    assert.equal(calc.length, 0, 'no calculate call came from the window or the card'); assert.equal(writes(), 0, 'and nothing at all was written');

    console.log('PASS: library cut line window: a partial 10K sheet asks one yes (nothing before it), the window is the shared-orders look and only a real press says yes, the yes is the Cut Sheet press with the name, the line is seen on the card before the sheet moves, a failure says why and moves nothing');
  } finally { srv.close(); }
})().catch(e => { console.error(e); process.exitCode = 1; });
