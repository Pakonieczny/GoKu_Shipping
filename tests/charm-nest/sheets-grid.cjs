// SHEETS GRID (Paul, 15 Nov 2026, SHEETSLIST / SHEETSANYWHERE / ONEGRID / ORDERCLOUD): the 14K solid-gold draft sheets of one run, arranged on the computer that carries it and from a computer that
// only follows it (the relay: runCmdPut / runCmdList / runCmdDone / runCmdCancel on the cloud, runCommands / execCmd on the carrier). Real sorter in Chromium against the repo's fake site; nothing live.
//  1 carrier: sheetsOf, ensureOrder (draftOrder on every saved record), reorder (cloud follows)   2 follower lists the same sheets, same order, same names, before and after
//  3 follower relays a reorder, the carrier does it   4 consolidate keeps the emptied sheet, deleteEmpty removes it   5 runCmdCancel of a waiting command   6 seatOn preview on a physical sheet
//   PW_DIR=... CHROMIUM=... node tests/charm-nest/sheets-grid.cjs
const fs = require('fs'), path = require('path');
if (process.argv[2]) process.env.PW_DIR = process.argv[2];
const { open, repo } = require('./day-sim/harness.cjs');
const W = require('./_autorunWorld.cjs');
const M14 = 'gold14k';
let fails = 0;
// (the solver's result varies from run to run: when it leaves fewer than two SAVED sheets, the whole test starts again, up to three times)
if (!process.env.SG_CHILD) { const cp = require('child_process'); let code = 3; for (let t = 1; t <= 3 && code === 3; t++) { if (t > 1) console.log('-- the run left fewer than two saved gold sheets; trying again (' + t + '/3)'); code = cp.spawnSync(process.execPath, [__filename, ...process.argv.slice(2)], { stdio: 'inherit', env: Object.assign({}, process.env, { SG_CHILD: '1' }) }).status; } process.exit(code); }
const say = (name, ok, seen) => { if (!ok) fails++; console.log(`${ok ? 'OK  ' : 'FAIL'} ${name}${seen === undefined ? '' : ' :: ' + (typeof seen === 'string' ? seen : JSON.stringify(seen))}`); };
const sleep = ms => new Promise(r => setTimeout(r, ms));
async function until(fn, ms = 20000, every = 500) { const t0 = Date.now(); let v; while (Date.now() - t0 < ms) { v = await fn(); if (v) return v; await sleep(every); } return v; }

async function otherComputer(H) {
  const st = H.st, ctx = await H.browser.newContext({ viewport: { width: 1500, height: 1000 }, timezoneId: process.env.SIM_TZ || 'America/Toronto' });
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
  await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(repo, 'lib/qrcode.min.js')) }));
  await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
  await ctx.route(/^https:\/\/firebasestorage\.googleapis\.com\//, r => { const u = new URL(r.request().url()); const m = /\/o\/(.+)$/.exec(u.pathname); const key = m ? decodeURIComponent(m[1]) : ''; const b = st.blobs.get(key); if (!b) return r.fulfill({ status: 404, body: 'no blob ' + key }); return r.fulfill({ status: 200, headers: { 'Content-Type': b.meta.contentType || 'application/octet-stream', 'Access-Control-Allow-Origin': '*', 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: b.buf }); });
  await ctx.addInitScript(({ station, sorter }) => {
    if (location.origin === station) { localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200 * 24 * 30)); localStorage.setItem('employee_name', 'Second'); }
    if (location.origin === sorter) { localStorage.setItem('cn.employee', 'Second'); }
    window.confirm = () => true; window.prompt = () => 'Second'; window.alert = () => {};
    if (window.Notification) { try { Object.defineProperty(window, 'Notification', { value: undefined }); } catch (_) {} }
  }, { station: H.stationOrigin, sorter: H.sorterOrigin });
  const page = await ctx.newPage(), errors = [];
  page.on('pageerror', e => errors.push(e.message));
  await page.goto(`${H.sorterOrigin}/charm-nest-1.html`);
  await page.waitForFunction(() => window.CN && window.CN.S && window.RunCtl && window.DesignLink && CN.S.cloud.ok !== null, null, { timeout: 90000 });
  await page.evaluate(({ station }) => { const s = CN.S.settings; s.dsOrigin = station; s.engine = 'solver'; s.budgetS = 2; s.review = 'off'; s.naming = 'off'; s.notify = 'off'; s.sound = 'off'; s.autoCommit = 'on'; s.pullMode = 'all'; CN.saveSettings && CN.saveSettings(); }, { station: H.stationOrigin });
  return { ctx, page, errors };
}

const rowsJS = () => Gate.sheetsOf('gold14k').map(r => ({ id: r.id, name: r.name, pieces: r.pieces, page: r.page, relay: !!r.relay, canDelete: r.canDelete, whyDelete: r.whyDelete }));
const recs = st => st.list('Charm_Nest_Sheets').filter(s => s.metal === M14);

(async () => {
  const SK = ['BR-TST-01', 'BR-TST-02', 'BR-TST-03', 'BR-TST-06', 'BR-TST-07', 'BR-TST-08'];
  const receipts = []; for (let i = 0; i < 8; i++) receipts.push(W.receipt(3940000001 + i, SK[i % SK.length], '14k Solid Gold'));
  const H = await open({ receipts, masters: W.MASTERS, settings: { budgetS: 2, runMode: 'manual', pullMode: 'all', pollOrders: 'on', pollMinutes: 10, heartbeatS: 5, heartbeatMiss: 3 } });
  const { page, st } = H; let two = null;
  const t0 = Date.now(), log = (...a) => console.log(Math.round((Date.now() - t0) / 1000) + 's', ...a);
  try {
    await page.evaluate(() => { CN.setMode('nest'); RunCtl.setMode('auto'); });
    const v = await W.rest(page, { log, timeoutMs: 420000 });
    if (v.timeout) throw new Error('the run never rested: ' + JSON.stringify(v));
    const runId = v.run.id; await page.evaluate(() => Session.flush());
    log('run at rest:', v.sheets.join(' '));

    /* 1 */
    let rows = await page.evaluate(rowsJS);
    const saved0 = recs(st).filter(s => !s.archived).length;
    if (saved0 < 2 || rows.length < 2 || rows.some(r => !r.pieces)) { console.log('precondition not met:', JSON.stringify({ saved0, rows: rows.length, pages: await page.evaluate(() => CN.pagesOf('gold14k').map(p => [p.page, p.status, (p.placements || []).length, (p.charms || []).length, p.runHold || '', p.problem || '', p.fileBase || '', p.endedBy || ''])).catch(e => String(e)) })); await H.close().catch(() => {}); process.exit(3); }
    say('1a carrier sheetsOf lists >=2 draft sheets with pieces', rows.length >= 2 && rows.every(r => r.pieces > 0), rows);
    if (rows.length < 2) throw new Error('not enough sheets');
    const ens = await page.evaluate(() => Gate.ensureOrder('gold14k'));
    const withDO = await until(() => { const r = recs(st).filter(s => !s.archived); return r.length && r.every(s => Number.isFinite(+s.draftOrder)) ? r : null; }, 15000);
    const recsNow = recs(st).filter(s => !s.archived);
    say('1b after ensureOrder every saved sheet record has draftOrder', !!withDO && recsNow.length >= 2, { ensureOrder: ens, records: recsNow.map(s => [s.id, s.draftOrder, s.page]), pageRows: rows.length });
    const ids0 = rows.map(r => r.id), names0 = rows.map(r => r.name);
    const doOf = id => { const s = st.doc('Charm_Nest_Sheets', id); return s && [s.draftOrder, s.page]; };
    say('1c carrier list order matches the cloud draftOrder', ids0.every((id, i) => !st.doc('Charm_Nest_Sheets', id) || +doOf(id)[0] === i + 1), ids0.map(doOf));

    /* the follower, before the reorder */
    two = await otherComputer(H); const p2 = two.page;
    await p2.evaluate(() => { CN.setMode('nest'); RunCtl.setMode('auto'); });
    await p2.waitForFunction(() => allSheets().some(p => p.followed), null, { timeout: 60000 });
    // (FASTOPEN, 23 Nov 2026, charm-nest-bridge.js makeLive: a computer that follows the run builds only the sheet a person looks at, the Nest card's active sheet of each metal; every other followed sheet stays its
    //  saved record (p.recalled) until it is opened to be changed (CharmNestCloudSheets.ensureLive). The follower is ready when the gold sheet on screen is built live and every other followed sheet is a saved record or live)
    const fState = () => p2.evaluate(() => { const f = allSheets().filter(p => p.followed), prim = CN.S.sheets.gold14k, shown = prim && prim.pages[prim.active || 0];
      return { n: f.length, shown: !!shown && f.includes(shown), shownLive: !!shown && !shown.recalled && (shown.charms || []).length > 0, others: f.filter(p => p !== shown).map(p => [(p.sheetId || '').slice(-6), p.recalled ? 'record' : (p.charms || []).length ? 'live' : 'empty']) }; });
    await p2.waitForFunction(() => { const f = allSheets().filter(p => p.followed), prim = CN.S.sheets.gold14k, shown = prim && prim.pages[prim.active || 0];
      return f.length > 0 && !!shown && f.includes(shown) && !shown.recalled && (shown.charms || []).length > 0 && f.every(p => p === shown || !!p.recalled || (p.charms || []).length > 0); }, null, { timeout: 90000, polling: 500 }).catch(() => null);
    const fs0 = await fState();
    say('2-pre follower builds the gold sheet on screen live; the other followed sheets stay saved records (FASTOPEN)', fs0.n >= 2 && fs0.shown && fs0.shownLive && fs0.others.every(o => o[1] === 'record'), fs0);
    const fol = () => p2.evaluate(rowsJS);
    const same = (a, b) => JSON.stringify(a.map(r => [r.id, r.name])) === JSON.stringify(b.map(r => [r.id, r.name]));
    let f0 = await until(async () => { const f = await fol(); return f.length === ids0.length ? f : null; }, 30000) || await fol();
    say('2a follower sheetsOf = carrier (same ids, same order, same names) before the reorder', same(f0, rows), { carrier: rows.map(r => [r.id.slice(-6), r.name]), follower: f0.map(r => [r.id.slice(-6), r.name, r.relay]) });

    /* reorder on the carrier: swap Draft 1 and 2 */
    const want = [ids0[1], ids0[0], ...ids0.slice(2)];
    const rr = await page.evaluate(ids => { const r = Gate.reorder('gold14k', ids); return { ok: r.ok, changed: r.changed, why: r.why, names: r.names }; }, want);
    rows = await page.evaluate(rowsJS);
    say('1d reorder swaps Draft 1 and 2 on the carrier', rr.ok && rr.changed && rows[0].id === ids0[1] && rows[1].id === ids0[0], { rr, order: rows.map(r => [r.id.slice(-6), r.name]) });
    const cloudOk = await until(() => want.every((id, i) => { const d = doOf(id); return !d || (+d[0] === i + 1 && +d[1] === i + 1); }), 15000);
    say('1e cloud records draftOrder/page follow the reorder', !!cloudOk, want.map(doOf));
    const names1 = rows.map(r => r.name);
    say('1f names follow positions (Draft 1 stays "Draft 1")', JSON.stringify(names1) === JSON.stringify(names0), { before: names0, after: names1 });
    const fAfter = await until(async () => { const f = await fol(); return same(f, rows) ? f : null; }, 30000) || await fol();
    if (process.env.DIAG) {
      const d1 = await p2.evaluate(() => ({ vis: document.visibilityState, cs: CharmNestCloudSheets.state(), follow: CharmNestRunFollow.state(), sync: (window.CNSync && CNSync.state) ? JSON.stringify(CNSync.state()).slice(0, 900) : null, pages: CN.pagesOf('gold14k').map(p => [(p.sheetId || '').slice(-6), p.draftOrder, p.followed, !!p.recalled, p.dirty, p.status, !!p._rebuilding, !!p.saveOwed, !!(p.persisted && !p.persistedDone)]) }));
      console.log('DIAG natural', same(fAfter, rows), JSON.stringify(d1).slice(0, 2400));
      await p2.evaluate(ids => CharmNestCloudSheets._changed(ids.map(id => 's:' + id)), want);
      const f2 = await until(async () => { const f = await fol(); return same(f, rows) ? f : null; }, 15000) || await fol();
      const d2 = await p2.evaluate(() => ({ cs: CharmNestCloudSheets.state(), pages: CN.pagesOf('gold14k').map(p => [(p.sheetId || '').slice(-6), p.draftOrder, p.followed]) }));
      console.log('DIAG after manual _changed', same(f2, rows), JSON.stringify(d2).slice(0, 1200));
      const r3 = await p2.evaluate(async () => { const id = CharmNestRunFollow.state().runId; let r; try { r = await Recall.follow(id, { force: true }); } catch (e) { r = 'ERR ' + e.message; } return r; });
      const f3 = await until(async () => { const f = await fol(); return same(f, rows) ? f : null; }, 15000) || await fol();
      console.log('DIAG after manual follow(force)', r3, same(f3, rows), JSON.stringify(f3.map(r => [r.id.slice(-6), r.name])));
      process.exit(0);
    }
    say('2b follower sheetsOf = carrier after the carrier reorder', same(fAfter, rows), { carrier: rows.map(r => [r.id.slice(-6), r.name]), follower: fAfter.map(r => [r.id.slice(-6), r.name]), followerPages: await p2.evaluate(() => CN.pagesOf('gold14k').map(p => [(p.sheetId || '').slice(-6), p.draftOrder, p.page, p.layoutRev])), cloud: rows.map(r => doOf(r.id)) });

    /* 3 · follower relays a reorder (what OptionsSheets.send does: runCarrier check, runCmdPut, then runCmdList until done) */
    const relay = (cmd, label) => p2.evaluate(async ({ runId, cmd, label }) => {
      const RFn = window.CharmNestRunFollow, dev = RFn.dev(), api = b => CN.api('charmNestLibrary', b, { quiet: true });
      const c = await api({ op: 'runCarrier', runId, dev, check: true });
      if (!c || !c.alive || c.mine) return { sent: false, carrier: c };
      const put = await api({ op: 'runCmdPut', runId, cmd: Object.assign({ metal: 'gold14k', label }, cmd), by: 'Second', dev });
      return { sent: !!put.id, id: put.id, dev };
    }, { runId, cmd, label });
    const waitCmd = (id, ms = 40000) => until(async () => { const r = await p2.evaluate(async ({ runId, id }) => (await CN.api('charmNestLibrary', { op: 'runCmdList', runId }, { quiet: true })).cmds.find(c => c.id === id), { runId, id }); return r && r.state !== 'waiting' ? r : null; }, ms, 1000);
    const back = rows.map(r => r.id).reverse();   // a distinct order: reversed
    const n3 = st.calls.length;
    const sent = await relay({ type: 'reorder', ids: back }, 'Change the order of the sheets');
    const done = sent.sent ? await waitCmd(sent.id) : null;
    rows = await page.evaluate(rowsJS);
    const fA = await until(async () => { const f = await fol(); return same(f, rows) ? f : null; }, 30000) || await fol();
    say('3a follower relay: command accepted and executed by the carrier', !!done && done.state === 'done', { sent, done: done && { state: done.state, text: done.text, doneBy: done.doneBy } });
    say('3b carrier now has the requested order', JSON.stringify(rows.map(r => r.id)) === JSON.stringify(back), { got: rows.map(r => r.id.slice(-6)), want: back.map(i => i.slice(-6)) });
    say('3c both computers agree afterwards (ids, order, names) and the cloud follows', same(fA, rows) && rows.every((r, i) => { const d = doOf(r.id); return d && +d[0] === i + 1; }), { carrier: rows.map(r => [r.id.slice(-6), r.name]), follower: fA.map(r => [r.id.slice(-6), r.name]), cloud: rows.map(r => doOf(r.id)) });

    /* 4 · consolidate: the smallest sheet onto the previous one that has room */
    const bySize = [...rows].sort((a, b) => a.pieces - b.pieces);
    const piecesBefore = rows.reduce((n, r) => n + r.pieces, 0);
    let src = null, tgt = null, cr = null, tried = [];
    // (GRIDGAPS, Paul 15 Nov 2026, charm-nest-bridge.js mergeSheets / consolidate: what the target has no room for goes to the next sheets of the order, else onto a NEW sheet, so the source empties
    //  even when a full target took nothing: the targets with the most room are tried first, so the pair found is one where the target takes them)
    const fillOf = await page.evaluate(() => Object.fromEntries(Gate.sheetsOf('gold14k').map(r => [r.id, r.pct == null ? 100 : +r.pct])));
    const byRoom = [...rows].sort((a, b) => ((fillOf[a.id] ?? 100) - (fillOf[b.id] ?? 100)) || (a.pieces - b.pieces));
    for (const s0 of bySize) { for (const t0 of byRoom) { if (t0.id === s0.id || s0.pieces === 0) continue;
      const r0 = await page.evaluate(([s, t]) => Gate.consolidate(s, t), [s0.id, t0.id]); await until(() => page.evaluate(() => !Gate.mergeFx().live), 20000);
      const now = await page.evaluate(rowsJS), moved = (now.find(r => r.id === s0.id) || {}).pieces; tried.push([s0.name, s0.pieces, '->', t0.name, t0.pieces, fillOf[t0.id], r0 && r0.text, moved, (now.find(r => r.id === t0.id) || {}).pieces, now.length]);
      if (moved === 0) { src = s0; tgt = t0; cr = r0; break; } }
      if (src) break; }
    if (!src) { src = bySize[0]; tgt = rows.find(r => r.id !== src.id); say('4-setup could find two sheets where one fits onto the other', false, tried); }
    await page.evaluate(() => Session.flush()).catch(() => {});
    rows = await page.evaluate(rowsJS);
    const sRow = rows.find(r => r.id === src.id), tRow = rows.find(r => r.id === tgt.id);
    say('4a consolidate moved pieces onto target', cr && cr.ok !== false && tRow && tRow.pieces === tgt.pieces + src.pieces, { source: [src.name, src.pieces], target: [tgt.name, tgt.pieces], result: cr, tried, nowTarget: tRow && tRow.pieces, nowSource: sRow && sRow.pieces });
    say('4b source sheet is KEPT, empty', !!sRow && sRow.pieces === 0, { sourceRow: sRow, sheetsNow: rows.length });
    const rec = st.doc('Charm_Nest_Sheets', src.id);
    say('4c source record still in the cloud and not archived', !!rec && !rec.archived, rec && { archived: rec.archived, placements: (rec.placements || []).length, poolIds: (rec.poolIds || []).length });
    say('4d no piece lost (total pieces unchanged)', rows.reduce((n, r) => n + r.pieces, 0) === piecesBefore, { before: piecesBefore, after: rows.map(r => r.pieces) });
    if (sRow) {
      const dr = await page.evaluate(id => Gate.deleteEmpty(id), src.id);
      rows = await page.evaluate(rowsJS); await sleep(1500);
      const rec2 = st.doc('Charm_Nest_Sheets', src.id);
      say('4e deleteEmpty removes the emptied sheet', dr.ok && !rows.some(r => r.id === src.id), { dr, left: rows.map(r => [r.name, r.pieces]), record: rec2 ? { archived: rec2.archived } : 'gone' });
      const retired = ((st.doc('Charm_Nest_Runs', runId) || {}).intakeRecovery || {}).retire || [];   // (dropEmpty retires the page in the run's record; the sheet's own record is never deleted)
      say('4f the cloud no longer counts it as a live sheet (archived, or retired in the run record)', !rec2 || !!rec2.archived || retired.includes(src.id), { archived: rec2 && rec2.archived, retired });
      say('4h no blank placeholder page left with a name that repeats a real sheet', new Set(rows.map(r => r.name)).size === rows.length, rows.map(r => [r.id.slice(-6), r.name, r.pieces]));
      const fB = await until(async () => { const f = await fol(); return same(f, rows) ? f : null; }, 30000) || await fol();
      say('4g follower agrees after delete', same(fB, rows), { carrier: rows.map(r => [r.id.slice(-6), r.name]), follower: fB.map(r => [r.id.slice(-6), r.name]),
        missing: same(fB, rows) ? [] : rows.filter(r => !fB.some(f => f.id === r.id)).map(r => { const d = st.doc('Charm_Nest_Sheets', r.id); return [r.id.slice(-6), d ? { sameRun: d.runId === runId, archived: !!d.archived, placements: (d.placements || []).length, draftOrder: d.draftOrder } : 'no cloud record']; }),
        followerPages: same(fB, rows) ? [] : await p2.evaluate(() => CN.pagesOf('gold14k').map(p => [(p.sheetId || '').slice(-6), !!p.followed, !!p.recalled, (p.charms || []).length])) });
    }

    /* 5 · runCmdCancel of a command that waits: the carrier cannot read its list while we hold its runCmdList calls back */
    let hold = true; await page.route(/charmNestLibrary/, async r => { let op = ''; try { op = JSON.parse(r.request().postData() || '{}').op; } catch (_) {} if (hold && op === 'runCmdList') return r.abort(); return r.continue(); });
    const before5 = (await page.evaluate(rowsJS)).map(r => r.id);
    const want5 = [...before5].reverse();
    const s5 = await relay({ type: 'reorder', ids: want5 }, 'Change the order of the sheets');
    await sleep(4000);
    const state1 = await p2.evaluate(async ({ runId, id }) => (await CN.api('charmNestLibrary', { op: 'runCmdList', runId }, { quiet: true })).cmds.find(c => c.id === id), { runId, id: s5.id });
    const cres = await p2.evaluate(async ({ runId, id, dev }) => CN.api('charmNestLibrary', { op: 'runCmdCancel', runId, id, dev }, { quiet: true }), { runId, id: s5.id, dev: s5.dev });
    say('5a a waiting command is cancelled by runCmdCancel', state1 && state1.state === 'waiting' && cres && cres.cancelled === true && cres.state === 'cancelled', { waitingBefore: state1 && state1.state, cancel: cres });
    hold = false; await sleep(12000);
    const after5 = (await page.evaluate(rowsJS)).map(r => r.id);
    const final5 = await p2.evaluate(async ({ runId, id }) => (await CN.api('charmNestLibrary', { op: 'runCmdList', runId }, { quiet: true })).cmds.find(c => c.id === id), { runId, id: s5.id });
    say('5b the carrier does not run it afterwards (order unchanged, still cancelled)', JSON.stringify(after5) === JSON.stringify(before5) && final5.state === 'cancelled', { order: after5.map(i => i.slice(-6)), wasOrder: before5.map(i => i.slice(-6)), state: final5.state, text: final5.text });
    const again = await p2.evaluate(async ({ runId, id, dev }) => CN.api('charmNestLibrary', { op: 'runCmdCancel', runId, id, dev }, { quiet: true }), { runId, id: sent.id, dev: s5.dev });
    say('5c cancelling a command that already ran says it could not', again && again.cancelled === false && again.state === 'done', again);

    /* 6 · seatOn preview onto an Available physical sheet */
    const parts = await page.evaluate(async () => { try { const r = await PartialSheets.list(); return { n: (r.items || []).length, items: (r.items || []).filter(i => i.metal === 'gold14k' || /14/.test(String(i.metal || i.stockMetal || ''))).slice(0, 2).map(i => ({ id: i.id || i.stockId, metal: i.metal })) }; } catch (e) { return { err: String(e.message || e) }; } });
    if (!parts.n || !parts.items.length) say('6 seatOn preview', true, 'no physical sheet in fixtures (PartialSheets.list: ' + JSON.stringify(parts) + ')');
    else { const sh = (await page.evaluate(rowsJS))[0]; const r = await page.evaluate(([s, p]) => Gate.seatOn(s, p, { preview: true }), [sh.id, parts.items[0].id]); say('6 seatOn {preview:true}', !!(r && r.ok), r); }

    /* 7 · MOVESHOW (Paul, 15 Nov 2026): the move plan, numbered sheets, progress overlay, result banner; carrier by REAL mouse drag in the Options window, follower the same through the relay.
         Physical sheets are made the way the window's "New sheet" tile does (PartialSheets.make). The page's own module (OptionsSheets) does the drawing; nothing here fakes its output. */
    await page.setViewportSize({ width: 1500, height: 1700 }); await p2.setViewportSize({ width: 1500, height: 1700 });   // (tall: a tile and the cards under it are on the screen together for the mouse drag)
    const SHOTS = '/private/tmp/claude-501/move1-shots'; fs.mkdirSync(SHOTS, { recursive: true });
    const mk = (pg, w, h) => pg.evaluate(async ([w, h]) => { const r = await PartialSheets.make({ metal: 'gold14k', wMm: w, hMm: h }); return r && r.item ? { id: r.item.id, stockId: r.item.stockId, kind: r.item.kind, status: r.item.status } : { err: JSON.stringify(r) }; }, [w, h]);
    const NA = await mk(page, 55, 38), NB = await mk(page, 50, 50), NC = await mk(page, 60, 60), ND = await mk(page, 62, 62);
    say('7a four new 14K physical sheets made (PartialSheets.make)', [NA, NB, NC, ND].every(x => x.id && x.kind === 'new'), [NA, NB, NC, ND]);
    let rowsM = (await page.evaluate(rowsJS)).filter(r => r.pieces > 0).sort((a, b) => a.pieces - b.pieces);
    if (!rowsM.length) throw new Error('no sheet with pieces left for the move test');
    const mover = rowsM[0];
    const pv1 = await page.evaluate(([s, a]) => Gate.seatOn(s, a, { preview: true }), [mover.id, NA.id]);
    say('7b seatOn preview with ONE id (a string)', !!(pv1 && pv1.ok && pv1.text), pv1 && { ok: pv1.ok, text: pv1.text, why: pv1.why });
    const pv2 = await page.evaluate(([s, a, b]) => Gate.seatOn(s, [a, b], { preview: true }), [mover.id, NA.id, NB.id]);
    say('7c seatOn preview with an ORDERED LIST of two ids', !!(pv2 && pv2.ok && pv2.text) && (!pv2.preview.links.length || pv2.preview.links[0].partialId === NA.id), pv2 && { ok: pv2.ok, text: pv2.text, why: pv2.why, links: pv2.preview && pv2.preview.links.map(l => [l.partialId.slice(-6), l.placed]) });
    const pv3 = await page.evaluate(([s, a, b]) => Gate.seatOn(s, [b, a], { preview: true }), [mover.id, NA.id, NB.id]);
    say('7d the list order is the order used (B first: first link is B)', !!(pv3 && pv3.ok) && (!pv3.preview.links.length || pv3.preview.links[0].partialId === NB.id), pv3 && { text: pv3.text, links: pv3.preview && pv3.preview.links.map(l => [l.partialId.slice(-6), l.placed]) });

    // the Options window of a computer (the sheet card's Options button), then a REAL mouse drag of a tile onto an Available card
    const openWin = pg => pg.evaluate(() => { const sh = CN.pagesOf('gold14k').find(p => p.el && p.el.querySelector('.sheetOptionsBtn')); if (!sh) return false; sh.el.querySelector('.sheetOptionsBtn').click(); return true; });
    const sampler = pg => pg.evaluate(() => { window.__seen = { ghost: false, tileOv: false, cardOv: 0, steps: [], badges: [] }; window.__iv = setInterval(() => { const q = s => document.querySelector(s), z = window.__seen; if (q('.osGhost')) z.ghost = true; if (q('.osSh .osOv')) z.tileOv = true; z.cardOv = Math.max(z.cardOv, document.querySelectorAll('article.ohc .osOv').length); for (const e of document.querySelectorAll('.osOv [data-ov-s]')) { const t = e.textContent; if (t && !z.steps.includes(t)) z.steps.push(t); } }, 80); });
    const dragMove = async (pg, srcId, cardId) => {
      await pg.evaluate(id => { const t = document.querySelector(`.osSh[data-id="${id}"]`); t && t.scrollIntoView({ block: 'start' }); }, srcId);   // (the tile at the top; the viewport is tall enough for the cards below it)
      const a = await pg.evaluate(id => { const r = document.querySelector(`.osSh[data-id="${id}"] .osShPic`).getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2 }; }, srcId);
      const b = await pg.evaluate(id => { const r = [...document.querySelectorAll('article.ohc')].find(x => x.dataset.id === id).getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + Math.min(r.height / 2, 90) }; }, cardId);
      await pg.mouse.move(a.x, a.y); await pg.mouse.down(); await pg.mouse.move((a.x + b.x) / 2, (a.y + b.y) / 2, { steps: 8 }); await pg.mouse.move(b.x, b.y, { steps: 8 }); await sleep(150); await pg.mouse.up();
    };
    const clickCard = async (pg, cardId) => { const b = await pg.evaluate(id => { const r = [...document.querySelectorAll('article.ohc')].find(x => x.dataset.id === id).getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + 14 }; }, cardId); await pg.mouse.click(b.x, b.y); };
    const planState = pg => pg.evaluate(() => { const pl = document.querySelector('[data-plan]'); return { open: !!pl, head: pl ? pl.querySelector('.osPlanHead').textContent : '', items: pl ? [...pl.querySelectorAll('li')].map(li => li.textContent.replace(/\s+/g, ' ').trim()) : [], words: pl ? (pl.querySelector('[data-plan-words]') || {}).textContent : '', help: pl ? (pl.querySelector('.osPlanHelp') || {}).textContent : '', rest: pl && pl.querySelector('[data-plan-rest]') ? pl.querySelector('[data-plan-rest]').textContent : '', go: pl ? !pl.querySelector('[data-plan-go]').disabled : false, badges: [...document.querySelectorAll('article.ohc .osBadge')].map(b => [b.parentNode.dataset.id, b.textContent]) }; });
    const waitPreview = pg => until(async () => { const z = await planState(pg); return z.open && z.go ? z : null; }, 90000, 700);
    const MOVE = async (pg, label, ids0, srcId, isFollower) => {
      const [first, second] = ids0;
      await sampler(pg);
      await pg.evaluate(() => { if (!window.__steps) { window.__steps = []; const g = Gate.seatOn; Gate.seatOn = function (a, b, o) { o = o || {}; if (!o.preview) { const f = o.onStep; o = Object.assign({}, o, { onStep: st => { window.__steps.push(st && st.text); f && f(st); } }); } return g.call(this, a, b, o); }; } });
      const opened = await openWin(pg); say(`7${label}1 Options window opens`, opened);
      const tiles = await until(() => pg.evaluate(id => !!document.querySelector(`.osSh[data-id="${id}"]`) && document.querySelectorAll('article.ohc').length > 0, srcId), 60000, 700);
      say(`7${label}2 tile and repository cards are drawn`, !!tiles);
      const cardsOk = await until(() => pg.evaluate(ids => ids.every(id => [...document.querySelectorAll('article.ohc')].some(x => x.dataset.id === id)), ids0), 30000, 700);
      say(`7${label}3 the new sheets are cards in the grid`, !!cardsOk, await pg.evaluate(() => [...document.querySelectorAll('article.ohc')].map(x => x.dataset.id.slice(-6) + ':' + x.dataset.status)));
      await dragMove(pg, srcId, first);
      const z1 = await until(async () => { const z = await planState(pg); return z.open ? z : null; }, 20000, 300);
      if (!z1) { await pg.screenshot({ path: path.join(SHOTS, `drop-failed-${isFollower ? 'follower' : 'carrier'}.png`) }); console.log('DIAG after drop', JSON.stringify(await pg.evaluate(() => ({ notes: [...document.querySelectorAll('.osShNote')].map(n => n.textContent), said: (document.querySelector('[data-sh="said"]') || {}).textContent, ask: (document.querySelector('[data-sh="ask"]') || {}).textContent, drag: document.documentElement.className, tiles: [...document.querySelectorAll('.osSh')].map(t => [t.dataset.id.slice(-6), t.className, t.querySelector('.osShPic').getBoundingClientRect().top|0, t.querySelector('.osShPic').getBoundingClientRect().left|0]), cards: [...document.querySelectorAll('article.ohc')].map(c => [c.dataset.id.slice(-6), c.getBoundingClientRect().top|0, c.getBoundingClientRect().left|0, c.getBoundingClientRect().height|0]), rows: Gate.sheetsOf('gold14k').map(r => [r.id.slice(-6), r.movable, r.canSource, r.whySource, r.pieces]) })))); throw new Error('the drop did not open the plan'); }
      say(`7${label}4 dropping the tile on an Available card opens the move plan (no yes/no ask)`, !!z1 && /1st:/.test(z1.items[0] || '') && /Move plan/.test(z1.head), z1);
      say(`7${label}5 the plan carries the instruction words and a "1" badge on the chosen card`, !!z1 && /Click another Available sheet to add it as 2nd, 3rd/.test(z1.help) && /click a chosen sheet to take it out/.test(z1.help) && z1.badges.some(b => b[0] === first && b[1] === '1'), z1 && { help: z1.help, badges: z1.badges });
      await clickCard(pg, second);
      const z2 = await until(async () => { const z = await planState(pg); return z.items.length === 2 ? z : null; }, 10000, 300);
      say(`7${label}6 clicking a second Available sheet adds it as 2nd with badge 2`, !!z2 && /2nd:/.test(z2.items[1] || '') && z2.badges.some(b => b[0] === second && b[1] === '2'), z2 && { items: z2.items, badges: z2.badges });
      const z3 = await waitPreview(pg);
      say(`7${label}7 the engine's preview words for the whole list arrive ("working it out…" before)`, !!z3 && !/working it out/.test(z3.words) && z3.words.length > 10, z3 && { words: z3.words, rest: z3.rest });
      await pg.screenshot({ path: path.join(SHOTS, `plan-${isFollower ? 'follower' : 'carrier'}.png`) });
      // reorder with the arrows: first goes down (second becomes 1st), then back
      await pg.click('[data-plan-down]'); const zr = await until(async () => { const z = await planState(pg); return z.badges.some(b => b[0] === second && b[1] === '1') ? z : null; }, 8000, 300);
      say(`7${label}8 ▼ swaps the order (the numbered badges follow)`, !!zr, zr && zr.badges);
      await pg.click('[data-plan-down]'); await until(async () => { const z = await planState(pg); return z.badges.some(b => b[0] === first && b[1] === '1') ? z : null; }, 8000, 300);
      // take the 2nd out, then add it again
      await clickCard(pg, second); const zt = await until(async () => { const z = await planState(pg); return z.items.length === 1 ? z : null; }, 8000, 300);
      say(`7${label}9 clicking a chosen sheet takes it out`, !!zt, zt && zt.items);
      await clickCard(pg, second); await until(async () => { const z = await planState(pg); return z.items.length === 2 ? z : null; }, 8000, 300); await waitPreview(pg);
      // Cancel leaves everything as it was, then drop again
      await pg.click('[data-plan-cancel]'); const zc = await planState(pg);
      say(`7${label}10 Cancel closes the plan and removes the badges`, !zc.open && !zc.badges.length, zc.badges);
      await dragMove(pg, srcId, first); await until(async () => (await planState(pg)).open, 20000, 300); await clickCard(pg, second); await until(async () => (await planState(pg)).items.length === 2, 10000, 300); const zp = await waitPreview(pg);
      say(`7${label}11 the plan is open again with two sheets and a preview`, !!zp && zp.items.length === 2, zp && zp.words);
      const t0m = Date.now();
      await pg.click('[data-plan-go]');
      const ov = await until(() => pg.evaluate(() => !!document.querySelector('.osOv')), 15000, 100);
      if (ov) await pg.screenshot({ path: path.join(SHOTS, `overlay-${isFollower ? 'follower' : 'carrier'}.png`) });
      say(`7${label}12 Move them: overlay (progress bar, step words, seconds) appears at once`, !!ov, await pg.evaluate(() => [...document.querySelectorAll('.osOv')].map(o => o.textContent.replace(/\s+/g, ' ').trim()).slice(0, 4)));
      const ban = await until(() => pg.evaluate(() => { const b = document.querySelector('[data-banner]'); return b ? b.textContent.replace(/\s+/g, ' ').trim() : null; }), 300000, 500);
      const seen = await pg.evaluate(() => { clearInterval(window.__iv); return window.__seen; });
      say(`7${label}13 the ghost picture flew (a .osGhost existed)`, !!seen.ghost, { ghost: seen.ghost });
      say(`7${label}14 overlay on the tile and on each chosen card`, seen.tileOv && seen.cardOv >= 2, { tileOv: seen.tileOv, cardOv: seen.cardOv });
      say(`7${label}15 at least two different step texts were shown in the overlay`, seen.steps.length >= 2, seen.steps);
      say(`7${label}16 result banner stays until dismissed, in plain words from real values`, !!ban && /now lies? on/.test(ban) && !/NaN|undefined/.test(ban), { ban, secs: Math.round((Date.now() - t0m) / 1000) });
      const onSel = id => pg.evaluate(id => (document.querySelector(`.osSh[data-id="${id}"] [data-sh-on]`) || {}).textContent || '', id);
      await until(() => onSel(srcId), isFollower ? 40000 : 3000, 800);   // (a following computer learns of the new stock when the feed brings the sheet's record)
      const after = await pg.evaluate(id => ({ plan: !!document.querySelector('[data-plan]'), overlays: document.querySelectorAll('.osOv').length, banner: !!document.querySelector('[data-banner]'), on: (document.querySelector(`.osSh[data-id="${id}"] [data-sh-on]`) || {}).textContent || '', freed: [...document.querySelectorAll('.osFreedTag')].map(x => x.textContent) }), srcId);
      say(`7${label}17 afterwards: plan and overlays gone, banner still there, tile names its physical sheet`, !after.plan && !after.overlays && after.banner && /^on (New sheet|Partial sheet)/.test(after.on), Object.assign({ rowStock: await pg.evaluate(id => (Gate.sheetsOf('gold14k').find(r => r.id === id) || {}).stockId, srcId) }, after));
      return { ban, seen, after };
    };
    const rec7 = id => st.doc('Charm_Nest_Sheets', id);
    const m1 = await MOVE(page, 'c', [NA.id, NB.id], mover.id, false);
    const steps = await page.evaluate(() => window.__steps);
    say('7c18 the real seat reported >= 2 distinct onStep lines (collected from Gate.seatOn)', new Set(steps).size >= 2, steps.slice(0, 12));
    const named = await until(() => { const r = rec7(mover.id); return r && (r.roseStockId === NA.stockId) ? r : null; }, 25000, 800);
    say('7c19 the sheet\'s record names the physical sheet afterwards', !!named, { want: NA.stockId, got: (rec7(mover.id) || {}).roseStockId });
    await page.click('[data-sh-dismiss]'); await sleep(300);
    say('7c20 Dismiss removes the banner', await page.evaluate(() => !document.querySelector('[data-banner]')));
    await page.evaluate(() => OptionsStudio.close());
    await sleep(1500);

    // ── the follower: the same panel, overlay and banner; progress comes through runCmdStep
    const mover2 = (await page.evaluate(rowsJS)).filter(r => r.pieces > 0).sort((a, b) => a.pieces - b.pieces)[0];
    await p2.evaluate(() => { window.__relay = { steps: [], states: [], cancel: null }; window.__riv = setInterval(async () => { try { const rid = (CN.pagesOf('gold14k')[0] || {}).runId; const r = await CN.api('charmNestLibrary', { op: 'runCmdList', runId: rid }, { quiet: true }); const c = (r.cmds || []).find(x => x.type === 'seat' && x.dev === CharmNestRunFollow.dev()); if (!c) return; const z = window.__relay; if (!z.states.includes(c.state)) z.states.push(c.state); if (c.step && !z.steps.includes(c.step)) z.steps.push(c.step); if (c.state === 'running' && !z.cancel) z.cancel = await CN.api('charmNestLibrary', { op: 'runCmdCancel', runId: rid, id: c.id, dev: CharmNestRunFollow.dev() }, { quiet: true }); } catch (_) {} }, 700); });
    await until(async () => (await p2.evaluate(rowsJS)).some(r => r.id === mover2.id && r.pieces > 0), 30000, 700);
    const m2 = await MOVE(p2, 'f', [NC.id, ND.id], mover2.id, true);
    const rl = await p2.evaluate(() => { clearInterval(window.__riv); return window.__relay; });
    say('7f18 the follower saw runCmdStep texts while the command ran (before done)', rl.steps.length >= 1 && rl.states.includes('running') && rl.states.includes('done'), rl);
    say('7f19 runCmdCancel refused once the command was running', !!rl.cancel && rl.cancel.cancelled === false && rl.cancel.state === 'running', rl.cancel);
    say('7f20 the follower\'s overlay showed the carrier\'s words', m2.seen.steps.some(t => /computer: /.test(t)), m2.seen.steps);
    const named2 = await until(() => { const r = rec7(mover2.id); return r && (r.roseStockId === NC.stockId) ? r : null; }, 25000, 800);
    say('7f21 the sheet\'s record names the physical sheet chosen first on the follower', !!named2, { want: NC.stockId, got: (rec7(mover2.id) || {}).roseStockId });

    const errs = [...H.errors.filter(e => !/firebase|HTTP 404|\.pdf not made|Failed to load resource/.test(e)), ...(two ? two.errors : [])];
    say('page errors', errs.length === 0, errs.slice(0, 5));
  } finally { await H.close(); }
  console.log(fails ? `FAILED ${fails} check(s)` : 'ALL OK'); process.exit(fails ? 1 : 0);
})().catch(e => { console.error('ABORT', e && e.stack || e); process.exit(2); });
