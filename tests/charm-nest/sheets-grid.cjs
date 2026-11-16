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
    if (saved0 < 2 || rows.length < 2 || rows.some(r => !r.pieces)) { console.log('precondition not met:', JSON.stringify({ saved0, rows: rows.length })); await H.close().catch(() => {}); process.exit(3); }
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
    await p2.waitForFunction(() => { const f = allSheets().filter(p => p.followed); return f.length > 0 && f.every(p => !p.recalled && (p.charms || []).length > 0); }, null, { timeout: 90000, polling: 500 });
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
    for (const s0 of bySize) { for (const t0 of rows) { if (t0.id === s0.id || s0.pieces === 0) continue;
      const r0 = await page.evaluate(([s, t]) => Gate.consolidate(s, t), [s0.id, t0.id]); await until(() => page.evaluate(() => !Gate.mergeFx().live), 20000);
      const now = await page.evaluate(rowsJS), moved = (now.find(r => r.id === s0.id) || {}).pieces; tried.push([s0.name, s0.pieces, '->', t0.name, t0.pieces, r0 && r0.text, moved]);
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
      say('4g follower agrees after delete', same(fB, rows), { carrier: rows.map(r => [r.id.slice(-6), r.name]), follower: fB.map(r => [r.id.slice(-6), r.name]) });
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

    const errs = [...H.errors.filter(e => !/firebase|HTTP 404|\.pdf not made|Failed to load resource/.test(e)), ...(two ? two.errors : [])];
    say('page errors', errs.length === 0, errs.slice(0, 5));
  } finally { await H.close(); }
  console.log(fails ? `FAILED ${fails} check(s)` : 'ALL OK'); process.exit(fails ? 1 : 0);
})().catch(e => { console.error('ABORT', e && e.stack || e); process.exit(2); });
