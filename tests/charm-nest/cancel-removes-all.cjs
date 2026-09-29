// A cancelled order comes off every sheet it sits on (Paul, 29 Sep: "I cancelled the 2 orders that make up the 3 lion
// heads but none of them disappears from the sheet after being cancelled"; RG 14/20 Sheet 1 held a custom lion outside
// its green lines and a lion of Etsy quantity 2 inside line 2, and both stayed for good).
// In a real Chromium on the fake site (bridge-server.cjs: the real charmNestLibrary and Rose stock handlers over an
// in-memory store; no Etsy, no model). The sheets are built through the page (a custom design sent to a metal, Nest,
// Cut Sheet), never seeded. Cancel records are written as the Etsy mirror or another station writes them, and the
// sorter's cancel check (AutoCancel) is asked to look:
//   gold    · a GF sheet with three custom designs, one of Etsy quantity 2 (two copies): cancelled, both copies leave, the
//             others stay, the pieces are recorded, the order's timeline and its cancel record say so;
//   rose    · an RG sheet with line 1 around a design, line 2 around a lion of quantity 2 (Cut Sheet twice) and a design
//             outside every line: the one outside leaves, lines untouched; then the lion leaves and line 2 goes with it
//             (nothing is left inside it), line 1 byte for byte as saved;
//   stale   · the same sheet, the cancels written while this browser holds its own saved copy with every piece: the page
//             loads, both orders come off the sheet on their own, in the page's copy and in the cloud, and stay off after
//             one more reload;
//   window  · the same sheet, the lion cancelled from the sheet window ("Take off the sheet…", then Cancel the order).
// Each removal is on the piece records (who, why, when), on the order's timeline and on its cancel record (kept).
//   node tests/charm-nest/cancel-removes-all.cjs [gold|rose|stale|window]   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), fs = require('fs');
const here = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(here, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start, Timestamp } = require(path.join(here, 'tests/charm-nest/bridge-server.cjs'));
const SHOTS = process.env.SHOTS || path.join(process.env.TMPDIR || '/tmp', 'cancel-removes-all');
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
const DXF = w => DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES',
  0, 'LWPOLYLINE', 8, 'CUT', 90, 4, 70, 1, 10, 0, 20, 0, 10, w, 20, 0, 42, 0.4, 10, w, 20, 20, 10, 0, 20, 20,
  0, 'CIRCLE', 8, 'CUT', 10, w / 2, 20, 16, 40, 1.2, 0, 'ENDSEC', 0, 'EOF');
const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const KEY = { gold: 'Gold Filled', rose: 'Rose Gold Filled' }, LABEL = { gold: 'GF 14/20', rose: 'RG 14/20' };
const order = (metal, rid, n, qty = 1) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - n * DAY, updateTs: SHIP - n * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + rid.slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: rid + '1', listingId: '18000' + rid.slice(-5), sku: 'CUSTOM-N-001-' + rid.slice(-6), title: 'Custom Name Necklace, Personalized Charm Necklace', quantity: qty, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: KEY[metal] }], metalKey: metal, metalLabel: LABEL[metal], personalization: [], buyerMessage: '' }] });
// A: the design line 1 is cut around; L: the lion, Etsy quantity 2; X: a design outside every line (Paul's 4175892473)
const A = '4175423829', L = '4176537942', X = '4175892473';
const poolsOf = (rid, n) => Array.from({ length: n }, (_, i) => `${rid}_${rid}1_${i + 1}`);
const sorted = a => JSON.stringify([...(a || [])].sort());
// (the plan a nest leaves beside the protected lines, when nothing is left to cut, only mirrors them: no line of its own)
const mirrors = r => !r.rosePlanJson || JSON.stringify(stagesOf(r.rosePlanJson)) === JSON.stringify(stagesOf(r.roseProtectedJson));
const stagesOf = json => (json ? (JSON.parse(json).stages || []).map(s => s.ids.length) : null);
const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', CANCELLED = 'Charm_Nest_Cancelled', TL = 'Order_Timeline';

const fails = [];
async function run(kind, browser) {
  const metal = kind === 'gold' ? 'gold' : 'rose', rose = metal === 'rose';
  const srv = await start({ receipts: [] }), { st } = srv;
  const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
  const check = (ok, what) => { if (!ok) fails.push(`${kind}: ${what}`); console.log((ok ? '  ok   ' : '  FAIL ') + `${kind} · ${what}`); };
  const said = [];
  try {
    fs.mkdirSync(SHOTS, { recursive: true });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    // a question or a pop-up would be recorded here: taking a cancelled order off asks none
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', pollOrders: 'off' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; window.__asked = []; window.confirm = m => { window.__asked.push(String(m)); return true; }; window.alert = m => { window.__asked.push(String(m)); }; });
    const page = await context.newPage(), errors = []; page.setDefaultTimeout(30000);
    page.on('pageerror', e => errors.push(String(e)));
    page.on('console', m => said.push(`[${m.type()}] ${m.text()}`.slice(0, 600)));
    const ready = () => page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomSheet && window.RoseStock && window.Gate && window.Cleanups && window.Session && window.AutoCancel && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await ready();
    const orders = [order(metal, A, 5), order(metal, L, 4, 2), order(metal, X, 3)];
    const seed = () => page.evaluate(async orders => {
      await Orders.loadMaps(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
      const day = new Date().toISOString().slice(0, 10); B.run = { runId: `run-${day}-cx`, day, setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] };
    }, orders);
    await seed();
    let sheetId = null;
    const info = () => page.evaluate(([id, m]) => {
      const sh = id ? CN.S.sheets[m].pages.find(p => p.sheetId === id) : CN.S.sheets[m].pages.at(-1); if (!sh) return null;
      const byId = new Map(sh.charms.map(c => [c.id, c]));
      return { sheetId: sh.sheetId, status: sh.status, dirty: !!sh.dirty, saved: !!sh.persistedDone, charms: sh.charms.map(c => c.poolId), placed: sh.placements.map(p => (byId.get(p.id) || {}).poolId), at: Object.fromEntries(sh.placements.map(p => [(byId.get(p.id) || {}).poolId, [p.cxPt, p.cyPt, p.angle]])),
        plan: (sh.rosePlan?.stages || []).map(s => ({ n: s.n, ids: s.ids.map(i => (byId.get(i) || {}).poolId || i) })), guard: (sh.roseProtected?.stages || []).map(s => ({ n: s.n, ids: s.ids.map(i => (byId.get(i) || {}).poolId || i) })),
        guardJson: sh.roseProtected ? JSON.stringify(sh.roseProtected) : null, cutButton: !!sh.el?.querySelector('.roseCut:not([hidden]) [data-rose="cut"]'), error: sh._roseError || null, problem: sh.problem || null, labelOrders: sh.label ? sh.label.orders || [] : null,
        asked: window.__asked, dialogs: document.querySelectorAll('dialog[open]').length, noteText: [...document.querySelectorAll('.mNote')].map(n => n.textContent) };
    }, [sheetId, metal]);
    const record = id => st.doc(SHEETS, id) || st.doc('Sandbox_' + SHEETS, id);
    const until = async (f, ms, what) => { const t = Date.now() + ms; for (;;) { const v = await f(); if (v) return v; if (Date.now() > t) throw new Error('timed out: ' + what); await page.waitForTimeout(400); } };
    async function send(rid, name, w) {
      await page.evaluate(() => CN.setMode('review'));
      await page.click('#reviewView .egTab[data-k="customOrder"]');
      const card = `#rvList .reviewListRow[data-rid="${rid}"]`;
      await page.waitForSelector(card);
      await page.evaluate(({ sel, text, name }) => { const dt = new DataTransfer(); dt.items.add(new File([text], name)); const n = document.querySelector(sel); for (const t of ['dragenter', 'dragover', 'drop']) n.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt, clientX: 700, clientY: 400 })); }, { sel: card, text: DXF(w), name });
      await page.waitForFunction(() => document.querySelectorAll('#cuDlg[open] .cuFile .cuThumb img').length === 1, null, { timeout: 30000 });
      await page.click(`#cuDlg .cuFile .cuM[data-m="${metal}"]`);
      await page.click('#cuDlg [data-send]');
      await page.waitForFunction(() => !document.querySelector('#cuDlg').open && !document.querySelector('#tourLayer > *'), null, { timeout: 30000 });
      await page.waitForFunction(m => { const sh = CN.S.sheets[m].pages.at(-1); return sh.charms.length && !['nesting', 'finishing', 'queued'].includes(sh.status); }, metal, { timeout: 30000 });
      await page.evaluate(m => { const sh = CN.S.sheets[m].pages.at(-1); if (sh.status !== 'complete' || sh.dirty) CN.startNest(sh); }, metal);
      await page.waitForFunction(m => { const sh = CN.S.sheets[m].pages.at(-1); return sh.status === 'complete' && sh.persistedDone && sh.verification?.ok && !sh.dirty; }, metal, { timeout: 120000 });
      await page.evaluate(() => CN.setMode('nest'));
      await page.waitForTimeout(1500);
    }
    const assemble = async () => {
      await page.evaluate(async m => { const sh = CN.S.sheets[m].pages.at(-1); sh._roseError = null; await Gate.assemble(B.run); CN.renderCard(sh); }, metal);
      await page.waitForTimeout(1500);
      await page.evaluate(async m => { await Gate.flush(B.run).catch(() => {}); await Gate.changeMembership(m, true).catch(() => {}); }, metal);
      await page.waitForTimeout(2500);
    };
    const cutSheet = async () => {
      await page.click('.sheetCard[data-m="rose"] [data-rose="cut"]');
      await page.waitForFunction(() => { const sh = CN.S.sheets.rose.pages.at(-1); return !sh._roseAction && !sh._rosePlanning && !sh._roseStep; }, null, { timeout: 60000 });
      await page.waitForTimeout(800);
    };

    // 1 · the sheet as Paul's was
    let guard1 = null, s;
    await send(A, 'a.dxf', 18);
    await page.evaluate(m => Gate.changeMembership(m, true), metal);
    await page.waitForTimeout(2000);
    if (rose) { await cutSheet(); s = await info(); sheetId = s.sheetId; check(s.plan.length === 1 && s.placed.length === 1, `1 · line 1 around the first design (${JSON.stringify(s.plan.map(p => p.ids.length))})`); }
    await send(L, 'lion.dxf', 24);
    await assemble();
    s = await info(); sheetId = s.sheetId;
    if (rose) {
      check(s.placed.length === 3 && s.guard.length === 1 && !s.plan.length, `1 · the lion twice beside line 1, uncut (${JSON.stringify({ placed: s.placed.length, guard: s.guard.length, plan: s.plan.length })})`);
      guard1 = record(sheetId).roseProtectedJson;
      await cutSheet(); s = await info();
      const r = record(sheetId);
      check(s.plan.length === 2 && sorted(s.plan[1].ids) === sorted(poolsOf(L, 2)) && JSON.stringify(stagesOf(r.rosePlanJson)) === '[1,2]' && r.roseProtectedJson === guard1, `1 · line 2 around both lions (${JSON.stringify({ plan: s.plan.map(p => p.ids.length), error: s.error })})`);
    }
    await send(X, 'x.dxf', 16);
    await assemble();
    s = await info();
    const want0 = [...poolsOf(A, 1), ...poolsOf(L, 2), ...poolsOf(X, 1)];
    check(sorted(s.placed) === sorted(want0) && s.saved && !s.dirty && !s.problem, `1 · the sheet holds the design, the lion twice and the third design (${JSON.stringify({ placed: s.placed, dirty: s.dirty, saved: s.saved, problem: s.problem })})`);
    if (rose) {
      const g = JSON.parse(record(sheetId).roseProtectedJson || 'null');
      check(g && g.stages.length === 2 && s.guard.length === 2 && !s.plan.length && !s.guard.some(t => t.ids.some(id => id.startsWith(X))), `1 · lines 1 and 2 saved as protected, the third design outside both (${JSON.stringify({ guard: s.guard.map(t => t.ids.length) })})`);
    }
    // (the piece records as a set's save leaves them in production: written on their sheet, with its name and set)
    for (const id of [...poolsOf(A, 1), ...poolsOf(L, 2), ...poolsOf(X, 1)]) Object.assign(st.doc(POOL, id) || {}, { state: 'written', sheetId, sheetName: `${rose ? 'RG' : 'GF'}_Sep.29.26_Set-1_Sheet-1`, setId: record(sheetId).setId || '' });
    const before = { at: s.at, guardJson: record(sheetId).roseProtectedJson || null };
    const at0 = await page.evaluate(() => ({ sent: Object.values(B.customDesigns || {}).map(e => Object.keys((e.sent && e.sent.lines) || {})) }));
    check(true, `1 · ready (custom designs known: ${JSON.stringify(at0.sent)})`);

    // (the cancel record as the Etsy mirror writes it; createdAt is the server's time: cancelList reads what was written since)
    const cancel = (rid, when) => st.put(CANCELLED, rid, { orderId: rid, by: 'Etsy', source: 'etsy', why: '', at: when, sheets: [], lines: [], createdAt: Timestamp.now() }, false);
    const settle = () => page.evaluate(async () => { const due = await AutoCancel.poll(); await AutoCancel.idle(); return due; });
    /** What one cancelled order left behind, everywhere it is recorded. */
    async function gone(rid, n, when, sheetWords, what) {
      const ids = poolsOf(rid, n), pools = ids.map(id => st.doc(POOL, id));
      check(pools.every(p => p && p.state === 'abandoned' && !p.sheetId && p.removedBy === 'Etsy' && p.removedReason === 'cancelled' && p.removedAt >= when && p.removedVerifiedAt > 0), `${what} · its ${n} piece record${n > 1 ? 's' : ''}: abandoned, removed by Etsy (cancelled), with the time, verified (${JSON.stringify(pools.map(p => p && [p.state, p.removedBy, p.removedAt, p.removedVerifiedAt]))})`);
      const at = pools[0] && pools[0].removedAt;
      await until(() => st.list(TL).some(e => e._id.startsWith(`${rid}~removed~`) && e.sheetId === sheetId && e.data && e.data.reason === 'cancelled on Etsy'), 10000, `the removed event of ${rid}`);
      const evs = st.list(TL).filter(e => e._id.startsWith(`${rid}~removed~`)), ev = evs[0];
      check(evs.length === 1 && ev.sheetId === sheetId && ev.by === 'Etsy' && ev.at === at && /^Removed from /.test(ev.text || '') && ev.data && ev.data.reason === 'cancelled on Etsy', `${what} · the timeline has one "removed" with its sheet, its who and why, at the same time as the piece records (${JSON.stringify(evs.map(e => [e.text, e.by, e.at, e.sheetId]))})`);
      await until(() => { const c = st.doc(CANCELLED, rid); return c && (c.removals || []).some(r => r.kind === 'sheet' && r.outcome === 'removed' && r.at > 0); }, 10000, `the cancel record of ${rid}`);
      const c = st.doc(CANCELLED, rid), rem = (c.removals || []).filter(r => r.kind === 'sheet');
      check(rem.length === 1 && rem[0].outcome === 'removed' && rem[0].by === 'Etsy' && rem[0].at === at && (c.fates || []).some(f => f.fate === 'removed') && c.at === when, `${what} · its cancel record keeps the removal with its exact time, the fate and its own cancel time (${JSON.stringify({ rem: rem.map(r => [r.where, r.outcome, r.at, r.by]), at, cancelAt: c.at, when })})`);
      const lines = await page.evaluate(rid => Orders.rows().filter(r => String(r.order.receiptId) === rid).length, rid);
      check(lines === 0, `${what} · its lines have left Orders (${lines})`);
    }
    const sentOf = (rid) => page.evaluate(([rid]) => Object.values(B.customDesigns || {}).flatMap(e => Object.entries((e.sent && e.sent.lines) || {}).filter(([k]) => k.includes(rid)).map(([k, l]) => ({ k, removed: l.map(pc => !!pc.removed) }))), [rid]);
    // the workspace this browser keeps (IndexedDB, Session): every store as text, to look for a piece in it
    const kept = () => page.evaluate(() => new Promise(res => { const r = indexedDB.open('charm-nest-workspace'); r.onerror = () => res(null); r.onsuccess = () => { const db = r.result, names = [...db.objectStoreNames]; if (!names.length) { db.close(); return res(''); } const tx = db.transaction(names, 'readonly'); let out = '', n = names.length; for (const nm of names) { const q = tx.objectStore(nm).getAll(); q.onsuccess = () => { try { out += JSON.stringify(q.result); } catch (_) {} if (!--n) { db.close(); res(out); } }; q.onerror = () => { if (!--n) { db.close(); res(out); } }; } }; }));
    const quiet = async what => { const z = await info(); check(!z.asked.length && !z.dialogs, `${what} · no question and no pop-up (${JSON.stringify(z.asked)})`); };

    if (kind === 'gold' || kind === 'rose') {
      /* 2 · the one outside every line (rose) / the last design (gold) */
      const whenX = Date.now(); cancel(X, whenX);
      let due = await settle();
      check(JSON.stringify(due) === JSON.stringify([X]), `2 · the cancel check finds ${X} (${JSON.stringify(due)})`);
      s = await until(async () => { const z = await info(); return z && !z.charms.includes(poolsOf(X, 1)[0]) && z.saved && !z.dirty && z; }, 60000, 'the sheet without the third design');
      check(sorted(s.placed) === sorted([...poolsOf(A, 1), ...poolsOf(L, 2)]), `2 · ${X} is off the sheet, the design and the lion stay (${JSON.stringify(s.placed)})`);
      const r2 = record(sheetId);
      check(sorted((r2.charms || []).map(c => c.poolId)) === sorted([...poolsOf(A, 1), ...poolsOf(L, 2)]) && sorted(r2.poolIds) === sorted([...poolsOf(A, 1), ...poolsOf(L, 2)]), `2 · the saved sheet no longer lists it (${JSON.stringify(r2.poolIds)})`);
      if (rose) check(r2.roseProtectedJson === before.guardJson && mirrors(r2) && s.guard.length === 2, `2 · both green lines untouched, byte for byte (${JSON.stringify({ guard: stagesOf(r2.roseProtectedJson), plan: stagesOf(r2.rosePlanJson), same: r2.roseProtectedJson === before.guardJson })})`);
      await gone(X, 1, whenX, [], '2');
      check(!(st.doc(CANCELLED, L)), `2 · the lion's order has no cancel record yet`);
      await quiet('2');

      /* 3 · the lion, quantity 2 (inside line 2 on the RG sheet) */
      const whenL = Date.now(); cancel(L, whenL);
      due = await settle();
      check(JSON.stringify(due) === JSON.stringify([L]), `3 · the cancel check finds ${L} (${JSON.stringify(due)})`);
      s = await until(async () => { const z = await info(); return z && !z.charms.some(id => id.startsWith(L)) && z.saved && !z.dirty && z; }, 60000, 'the sheet without the lion');
      check(sorted(s.placed) === sorted(poolsOf(A, 1)), `3 · both lions are off the sheet, the first design stays (${JSON.stringify(s.placed)})`);
      const r3 = record(sheetId);
      check(sorted((r3.charms || []).map(c => c.poolId)) === sorted(poolsOf(A, 1)) && sorted(r3.poolIds) === sorted(poolsOf(A, 1)), `3 · the saved sheet lists the first design alone (${JSON.stringify(r3.poolIds)})`);
      if (rose) {
        check(r3.roseProtectedJson === guard1 && mirrors(r3), `3 · line 2 went with the lion and line 1 is byte for byte as saved (${JSON.stringify({ guard: stagesOf(r3.roseProtectedJson), plan: stagesOf(r3.rosePlanJson), same: r3.roseProtectedJson === guard1 })})`);
        check(s.guard.length === 1 && s.plan.length <= 1 && s.at[poolsOf(A, 1)[0]].join() === before.at[poolsOf(A, 1)[0]].join(), `3 · the design in line 1 has not moved (${JSON.stringify([s.at[poolsOf(A, 1)[0]], before.at[poolsOf(A, 1)[0]]])})`);
        const off = st.list(TL).filter(e => e._id.startsWith(`${L}~`) && /Green line 2/.test(e.text || ''));
        check(off.length === 1 && off[0].type === 'note', `3 · the lion's timeline says line 2 was taken off, once (${JSON.stringify(off.map(e => [e.type, e.text]))})`);
      }
      await gone(L, 2, whenL, [], '3');
      const rowsA = await page.evaluate(a => Orders.rows().filter(r => String(r.order.receiptId) === a).length, A);
      check(rowsA === 1, `3 · the first design's line is still in Orders (${rowsA})`);
      check(!s.labelOrders || !s.labelOrders.includes(L), `3 · the sheet's label does not name the lion's order (${JSON.stringify(s.labelOrders)})`);
      await quiet('3');
      await page.locator(`.sheetCard[data-m="${metal}"]`).screenshot({ path: path.join(SHOTS, `cancel-${kind}-after.png`) });

      /* 4 · a reload: nothing comes back, and nothing is taken off twice */
      const n0 = st.list(TL).filter(e => e._id.includes('~removed~')).length;
      await page.evaluate(() => Session.flush(true)); await page.waitForTimeout(1000);
      await page.reload({ waitUntil: 'load' });
      await ready(); await page.waitForTimeout(4000);
      await settle();
      s = await info();
      const r4 = record(sheetId);
      check(s && sorted(s.placed) === sorted(poolsOf(A, 1)) && sorted((r4.charms || []).map(c => c.poolId)) === sorted(poolsOf(A, 1)) && (!rose || r4.roseProtectedJson === guard1 && mirrors(r4)), `4 · after a reload the sheet still holds the first design alone${rose ? ', line 1 as saved' : ''} (${JSON.stringify(s && s.placed)})`);
      check(st.list(TL).filter(e => e._id.includes('~removed~')).length === n0, `4 · nothing is recorded a second time`);
      await quiet('4');
    }

    if (kind === 'stale') {
      /* 2 · the browser's own copy holds every piece; both orders are cancelled meanwhile (as another station or Etsy does) */
      await page.evaluate(() => Session.flush(true)); await page.waitForTimeout(1500);
      const localText = await kept(), every = [...poolsOf(A, 1), ...poolsOf(L, 2), ...poolsOf(X, 1)];
      check(localText && every.every(id => localText.includes(id)), `2 · this browser's own saved copy holds all four pieces (${localText === null ? 'unreadable' : every.map(id => localText.includes(id)).join()})`);
      const whenX = Date.now(), whenL = whenX + 1;
      cancel(X, whenX); cancel(L, whenL);
      check(true, '2 · both orders are cancelled in the cloud while the page is closed');
      await page.reload({ waitUntil: 'load' });
      await ready();
      await page.waitForFunction(([x, l]) => { const d = AutoCancel.state().done; return !!(d[x] && d[l]); }, [X, L], { timeout: 120000 });
      await page.evaluate(() => AutoCancel.idle());
      s = await until(async () => { const z = await info(); return z && !z.charms.some(id => id.startsWith(L) || id.startsWith(X)) && z.saved && !z.dirty && z; }, 60000, 'the page without either order');
      check(sorted(s.placed) === sorted(poolsOf(A, 1)) && sorted(s.charms) === sorted(poolsOf(A, 1)), `3 · the page's own copy gives up all three pieces on load (${JSON.stringify(s.charms)})`);
      const r3 = record(sheetId);
      check(sorted((r3.charms || []).map(c => c.poolId)) === sorted(poolsOf(A, 1)) && sorted(r3.poolIds) === sorted(poolsOf(A, 1)) && r3.roseProtectedJson === guard1 && mirrors(r3), `3 · the cloud's sheet: the first design alone, line 2 gone, line 1 byte for byte (${JSON.stringify({ guard: stagesOf(r3.roseProtectedJson), same: r3.roseProtectedJson === guard1 })})`);
      await gone(X, 1, whenX, [], '3'); await gone(L, 2, whenL, [], '3');
      await quiet('3');
      // and off for good: the copy kept here is written again without them, and another reload brings nothing back
      await page.evaluate(() => Session.flush(true)); await page.waitForTimeout(1500);
      const after = await kept();
      check(after && after.includes(poolsOf(A, 1)[0]) && !poolsOf(L, 2).some(id => after.includes(id)) && !after.includes(poolsOf(X, 1)[0]), `3 · this browser's saved copy is written again without them (${after === null ? 'unreadable' : [...poolsOf(A, 1), ...poolsOf(L, 2), ...poolsOf(X, 1)].map(id => after.includes(id)).join()})`);
      await page.reload({ waitUntil: 'load' }); await ready(); await page.waitForTimeout(4000); await settle();
      s = await info();
      const r4 = record(sheetId);
      check(s && sorted(s.charms) === sorted(poolsOf(A, 1)) && sorted((r4.charms || []).map(c => c.poolId)) === sorted(poolsOf(A, 1)) && r4.roseProtectedJson === guard1, `4 · one more reload: still the first design alone, in the page's copy and in the cloud (${JSON.stringify(s && s.charms)})`);
      await quiet('4');
    }

    if (kind === 'window') {
      /* 2 · the lion cancelled from the sheet window */
      const whenL = Date.now();
      await page.evaluate(([id, sel]) => SheetWin.open(id, { select: sel }), [sheetId, poolsOf(L, 2)[0]]);
      await until(() => page.evaluate(() => !!document.querySelector('[data-r2=off] .swOffBtn')), 30000, 'the Take off button');
      const cur = await page.evaluate(() => ({ open: SheetWin.isOpen(), id: SheetWin.current() }));
      check(cur.open && cur.id === sheetId, `2 · the sheet window is open on the sheet, on a piece of the lion (${JSON.stringify(cur)})`);
      await page.click('[data-r2=off] .swOffBtn');
      await page.waitForSelector('.swOff [data-then="cancel"]');
      const allOpt = await page.$('.swOff input[name=swOffWho][value=all]'); if (allOpt) await allOpt.check();
      await page.click('.swOff [data-then="cancel"]');
      await page.click('.swOff [data-o="go"]');
      await until(async () => { const z = await info(); return z && !z.charms.some(id => id.startsWith(L)) && z.saved && !z.dirty && z; }, 90000, 'the sheet without the lion (window)');
      s = await info();
      const r2 = record(sheetId);
      check(sorted(s.placed) === sorted([...poolsOf(A, 1), ...poolsOf(X, 1)]) && sorted((r2.charms || []).map(c => c.poolId)) === sorted([...poolsOf(A, 1), ...poolsOf(X, 1)]), `2 · both lions leave the sheet from the window; the other two designs stay (${JSON.stringify(s.placed)})`);
      check(r2.roseProtectedJson !== null && stagesOf(r2.roseProtectedJson).join() === '1' && r2.roseProtectedJson === guard1 && mirrors(r2), `2 · line 2 goes with the lion, line 1 is byte for byte as saved (${JSON.stringify({ guard: stagesOf(r2.roseProtectedJson), same: r2.roseProtectedJson === guard1 })})`);
      const pools = poolsOf(L, 2).map(id => st.doc(POOL, id));
      check(pools.every(p => p && p.state === 'abandoned' && p.removedAt >= whenL && p.removedBy && /cancel/i.test(p.removedReason || '')), `2 · its piece records say abandoned, who, why and when (${JSON.stringify(pools.map(p => p && [p.state, p.removedBy, p.removedReason, p.removedAt]))})`);
      await until(() => st.list(TL).some(e => e._id.startsWith(`${L}~removed~`)), 10000, 'the removed event');
      const c = await until(() => { const c = st.doc(CANCELLED, L); return c && (c.removals || []).some(r => r.kind === 'sheet' && r.outcome === 'removed' && r.at > 0) && c; }, 15000, 'the cancel record with its removal');
      check(!!c && (c.removals || []).filter(r => r.kind === 'sheet').length >= 1, `2 · its cancel record keeps the removal with its time (${JSON.stringify((c.removals || []).map(r => [r.where, r.outcome, r.at]))})`);
      { const z = await info(); check(!z.asked.length && z.dialogs <= 1, `2 · no question, and no pop-up on the sheet window (${JSON.stringify({ asked: z.asked, dialogs: z.dialogs })})`); }
    }
    check(!errors.length, 'no page errors ' + errors.join(' | '));
  } catch (e) { fails.push(`${kind}: ${e.message}`); console.log(`  FAIL ${kind} · ${e.stack || e.message}`); }
  finally { try { fs.writeFileSync(path.join(SHOTS, `cancel-${kind}-console.log`), said.join('\n')); } catch (_) {} await context.close(); srv.close(); }
}

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try { for (const kind of process.argv[2] ? [process.argv[2]] : ['gold', 'rose', 'stale', 'window']) await run(kind, browser); }
  finally { await browser.close(); }
  if (fails.length) { console.log(`\n${fails.length} failed`); process.exit(1); }
  console.log('\ncancel-removes-all OK: a cancelled order comes off every sheet it sits on, GF and RG, inside a green line or outside one; each removal is recorded; a stale page loses them too');
})().catch(e => { console.error(e); process.exit(1); });
