// LEAN (Paul, 18 Nov 2026: "make the browser checkpoint small enough that no page freezes while it saves"; Michelle's computer: a 191 MB checkpoint, about every 3 minutes, 4 to 7 s of frozen page each time).
// Her computer FOLLOWS the run: every sheet of the run is built there from the cloud's records (~72 KB a piece) and every decided engraving keeps its drawn placement (1 to 2 MB a face), and the whole of it was copied and
// structured-cloned on the page's thread at each checkpoint. Two computers (the first carries the run, the second follows it) and the real Session:
//   1 the followed, saved sheets go into the checkpoint as their record (no pieces, no places); the checkpoint of the follower is a small part of what the same pages and faces held whole
//   2 the decided faces keep the numbers of their place (keepPlace), not the drawn view / mask / fit
//   3 anything of this computer's own stays whole: a changed sheet (dirty), a sheet at work (nesting), a piece with no pool row; the computer that carries the run writes all its sheets whole
//   4 the masters only the followed sheets use are left out
//   5 a reload with the cloud out of reach brings each sheet back as the record it is shown from (followed, no pieces), the faces back without a drawn placement; the next reload with the cloud brings the live
//     sheets back with the very pieces and places, and every face fitted again at its place (within 0.05 mm), states unchanged
// Real sorter in Chromium against the repo's fake site; nothing live.   PW_DIR=... CHROMIUM=... node tests/charm-nest/checkpoint-lean.cjs
const assert = require('assert'), fs = require('fs'), path = require('path');
if (process.argv[2]) process.env.PW_DIR = process.argv[2];
const { open, repo } = require('./day-sim/harness.cjs');
const W = require('./_autorunWorld.cjs');

const SIZER = `(() => {
  const sz = (v, seen = new Set()) => {
    if (v == null) return 0; const t = typeof v;
    if (t === 'string') return v.length; if (t === 'number') return 8; if (t === 'boolean') return 4; if (t !== 'object') return 0;
    if (v instanceof Blob) return v.size;
    if (ArrayBuffer.isView(v) || v instanceof ArrayBuffer) return v.byteLength;
    if (seen.has(v)) return 0; seen.add(v);
    let n = 0;
    if (v instanceof Map) { for (const [k, x] of v) n += sz(k, seen) + sz(x, seen); return n; }
    if (v instanceof Set) { for (const x of v) n += sz(x, seen); return n; }
    if (Array.isArray(v)) { for (const x of v) n += sz(x, seen); return n; }
    for (const [k, x] of Object.entries(v)) n += k.length + sz(x, seen);
    return n;
  };
  window.__sz = sz;
  /** what the follower holds of its pages and faces, whole (the old checkpoint wrote exactly this), and what the checkpoint writes now */
  window.__sizes = () => {
    const snap = Session.capture();
    const whole = allSheets().reduce((n, p) => n + sz(Session.copy(p)), 0) + [...Engrave.items().values()].reduce((n, j) => n + sz(Session.copy(j)), 0);
    const now = sz(snap.sheets) + sz(snap.jobs);
    const pages = snap.sheets.flatMap(g => g.pages);
    return { whole, now, total: sz(snap), poolSources: sz(snap.poolSources), nSources: Object.keys(snap.poolSources || {}).length,
      lean: pages.filter(p => p.lean).length, leanWithPieces: pages.filter(p => p.lean && ((p.charms || []).length || (p.placements || []).length)).length,
      wholeFollowed: pages.filter(p => p.followed && !p.lean && (p.charms || []).length).length,
      jobs: snap.jobs.map(j => ({ state: j.state, fit: !!j.fit, view: !!j.view, mask: !!j.mask, keep: !!j.keepPlace, size: sz(j) })),
      pageSizes: pages.map(p => ({ id: p.sheetId, lean: !!p.lean, n: (p.charms || []).length, size: sz(p) })) };
  };
})()`;

/** A browser context that is another computer: the harness's own routes and start-up script, and storage of its own. */
async function otherComputer(H) {
  const st = H.st, ctx = await H.browser.newContext({ viewport: { width: 1500, height: 1000 }, timezoneId: process.env.SIM_TZ || 'America/Toronto' });
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
  await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(repo, 'lib/qrcode.min.js')) }));
  await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
  await ctx.route(/^https:\/\/firebasestorage\.googleapis\.com\//, r => { const u = new URL(r.request().url()); const m = /\/o\/(.+)$/.exec(u.pathname); const key = m ? decodeURIComponent(m[1]) : ''; const b = st.blobs.get(key); if (!b) return r.fulfill({ status: 404, body: 'no blob ' + key }); return r.fulfill({ status: 200, headers: { 'Content-Type': b.meta.contentType || 'application/octet-stream', 'Access-Control-Allow-Origin': '*', 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: b.body }); });
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

let fails = 0;
const say = (name, ok, seen) => { if (!ok) fails++; console.log(`${ok ? 'OK  ' : 'FAIL'} ${name}${seen === undefined ? '' : ' :: ' + (typeof seen === 'string' ? seen : JSON.stringify(seen))}`); };
const mb = n => (n / 1048576).toFixed(2) + ' MB';

(async () => {
  const receipts = W.wave(3930000301, 3);
  receipts.forEach((r, i) => { if (i % 2 === 0) { const t = r.transactions[0]; t.is_personalized = true; t.variations.push({ formatted_name: 'Personalization', formatted_value: ['Anna', 'Bea', 'Chloe'][i % 3] }); } });
  const H = await open({ receipts, masters: W.MASTERS, settings: { budgetS: +process.env.BUDGET || 2, runMode: 'manual', pullMode: 'all', pollOrders: 'on', pollMinutes: 10, heartbeatS: 5, heartbeatMiss: 3 } });
  const { page } = H;
  const t0 = Date.now(), log = (...a) => console.log(Math.round((Date.now() - t0) / 1000) + 's', ...a);
  try {
    await page.evaluate(SIZER);
    await page.evaluate(() => { CN.setMode('nest'); RunCtl.setMode('auto'); });
    const v = await W.rest(page, { log: () => {}, timeoutMs: 600000 }); assert(!v.timeout, 'the first computer rests: ' + JSON.stringify(v));
    // every face decided on the first computer (approved, then written)
    await page.evaluate(async () => { for (const j of [...Engrave.items().values()]) if (j.state === 'review') await Engrave.approve(j, 'Tester'); });
    await page.waitForFunction(() => { const js = [...Engrave.items().values()]; return js.length > 0 && js.every(j => ['written', 'skipped', 'none'].includes(j.state)); }, null, { timeout: 300000, polling: 1000 });
    await page.evaluate(() => Session.flush(true));
    const runId = v.run.id;
    log('carrier done', runId);

    const two = await otherComputer(H), p2 = two.page;
    await p2.evaluate(SIZER);
    await p2.evaluate(() => { CN.setMode('nest'); RunCtl.setMode('auto'); });
    await p2.waitForFunction(() => { const f = allSheets().filter(p => p.followed); return f.length > 0 && f.every(p => !p.recalled && (p.charms || []).length > 0); }, null, { timeout: 180000, polling: 500 });
    await p2.waitForFunction(() => { const js = [...Engrave.items().values()]; return js.length > 0 && js.every(j => j.fit); }, null, { timeout: 180000, polling: 500 }).catch(() => {});
    await p2.waitForTimeout(3000);
    const before = await p2.evaluate(() => ({ pages: allSheets().filter(p => (p.charms || []).length).map(p => ({ id: p.sheetId, ids: p.placements.map(x => x.id).sort(), pl: p.placements.map(x => [x.id, +x.cxPt.toFixed(2), +x.cyPt.toFixed(2), +(x.angle || 0).toFixed(2)]).sort((a, b) => String(a[0]).localeCompare(String(b[0]))) })),
      jobs: [...Engrave.items().values()].map(j => ({ key: j.key, state: j.state, approvedAt: j.approvedAt, centre: j.fit && j.fit.centre, size: j.fit && j.fit.size, angle: j.fit && j.fit.angle })) }));
    log('follower live: pages', before.pages.length, 'faces', before.jobs.length, JSON.stringify(before.jobs.map(j => j.state)));

    // 1, 2, 4 ─ what the follower's checkpoint holds
    const z = await p2.evaluate(() => window.__sizes());
    log('follower checkpoint: whole', mb(z.whole), '→ now', mb(z.now), '· total', mb(z.total), '· masters', z.nSources, mb(z.poolSources));
    say('1 the followed, saved sheets are written as their record, without pieces or places', z.lean > 0 && z.lean === before.pages.length && z.leanWithPieces === 0 && z.wholeFollowed === 0, { lean: z.lean, pages: before.pages.length, leanWithPieces: z.leanWithPieces, wholeFollowed: z.wholeFollowed });
    say('2 the decided faces keep the numbers of their place, not the drawn view, mask and fit', z.jobs.length > 0 && z.jobs.every(j => !j.fit && !j.view && !j.mask && j.keep), z.jobs);
    say('1/2 the sheets and faces take under a tenth of what they took whole', z.whole > 0 && z.now < z.whole / 10, { whole: z.whole, now: z.now });
    const carrierSz = await page.evaluate(() => window.__sizes());
    say('4 the masters only the followed sheets use are left out (the carrier keeps its masters)', carrierSz.nSources > 0 && z.nSources < carrierSz.nSources, { follower: z.nSources, carrier: carrierSz.nSources });

    // 3 ─ anything of this computer's own is written whole
    const own = await p2.evaluate(() => {
      const p = allSheets().find(x => x.followed && (x.charms || []).length), out = {}, held = id => Session.capture().sheets.flatMap(g => g.pages).find(x => x.sheetId === id);
      const check = (name, set, undo) => { set(); const c = held(p.sheetId); out[name] = c && !c.lean && (c.charms || []).length > 0; undo(); };
      check('dirty', () => { p.dirty = true; }, () => { p.dirty = false; });
      check('saveOwed', () => { p.saveOwed = true; }, () => { delete p.saveOwed; });
      check('writeOwed', () => { p._writeOwed = true; }, () => { delete p._writeOwed; });
      check('nesting', () => { p.status = 'nesting'; }, () => { p.status = 'complete'; });
      check('noRevision', () => { const r = p.layoutRev; p.layoutRev = null; p._r = r; }, () => { p.layoutRev = p._r; delete p._r; });
      check('ownPiece', () => { p._poolId = p.charms[0].poolId; delete p.charms[0].poolId; }, () => { p.charms[0].poolId = p._poolId; delete p._poolId; });
      check('notFollowed', () => { p.followed = false; }, () => { p.followed = true; });
      out.back = !!held(p.sheetId).lean;
      return out;
    });
    say('3 a changed, waiting, nesting, unconfirmed or own-piece sheet is written whole', ['dirty', 'saveOwed', 'writeOwed', 'nesting', 'noRevision', 'ownPiece', 'notFollowed'].every(k => own[k]) && own.back, own);
    const cz = await page.evaluate(() => { const snap = Session.capture(); return { lean: snap.sheets.flatMap(g => g.pages).filter(p => p.lean).length, whole: snap.sheets.flatMap(g => g.pages).filter(p => (p.charms || []).length).length, leanJobs: snap.jobs.filter(j => j.keepPlace && !j.fit).length }; });
    say('3 the computer that carries the run writes every sheet whole', cz.lean === 0 && cz.whole > 0, cz);

    // 5 ─ a reload with the cloud out of reach, then with it
    await p2.evaluate(() => Session.flush(true));
    const rec = await p2.evaluate(() => new Promise((res, rej) => { const q = indexedDB.open('charm-nest-workspace'); q.onsuccess = () => { const g = q.result.transaction('workspaces').objectStore('workspaces').get('production'); g.onsuccess = () => res({ size: window.__sz(g.result), lean: g.result.sheets.flatMap(x => x.pages).filter(p => p.lean).length }); g.onerror = () => rej(g.error); }; q.onerror = () => rej(q.error); }));
    say('5 the stored record holds the lean sheets', rec.lean === before.pages.length, rec);
    await two.ctx.route(/\/\.netlify\/functions\//, r => r.abort());
    await p2.reload();
    await p2.waitForFunction(() => window.Session && Session.ready && Session.ready() && allSheets().some(p => p.followed), null, { timeout: 90000, polling: 500 });
    await p2.waitForTimeout(1500);
    const off = await p2.evaluate(() => ({ pages: allSheets().filter(p => p.followed).map(p => ({ id: p.sheetId, recalled: !!p.recalled, rid: p.recalled && p.recalled.id, placed: p.recalled && p.recalled.placedCount, charms: (p.charms || []).length, status: p.status })),
      jobs: [...Engrave.items().values()].map(j => ({ key: j.key, state: j.state, fit: !!j.fit, keep: !!j.keepPlace })) }));
    say('5 offline: each sheet is back as the record it is shown from (followed, no pieces)', off.pages.length === before.pages.length && off.pages.every(p => p.recalled && p.rid === p.id && p.charms === 0 && p.placed > 0), off.pages);
    say('5 offline: every face is back in its state, with the numbers of its place', off.jobs.length === before.jobs.length && off.jobs.every(j => ['written', 'approved'].includes(j.state)), off.jobs);
    await two.ctx.unroute(/\/\.netlify\/functions\//);
    await p2.waitForFunction(() => { const f = allSheets().filter(p => p.followed); return f.length > 0 && f.every(p => !p.recalled && (p.charms || []).length > 0); }, null, { timeout: 180000, polling: 500 }).catch(() => {});
    // the cloud is back for the next reload
    await p2.reload();
    await p2.waitForFunction(() => { const f = allSheets().filter(p => p.followed); return f.length > 0 && f.every(p => !p.recalled && (p.charms || []).length > 0); }, null, { timeout: 180000, polling: 500 });
    await p2.waitForFunction(() => { const js = [...Engrave.items().values()]; return js.length > 0 && js.every(j => j.fit); }, null, { timeout: 180000, polling: 500 }).catch(() => {});
    const after = await p2.evaluate(() => ({ pages: allSheets().filter(p => (p.charms || []).length).map(p => ({ id: p.sheetId, ids: p.placements.map(x => x.id).sort(), pl: p.placements.map(x => [x.id, +x.cxPt.toFixed(2), +x.cyPt.toFixed(2), +(x.angle || 0).toFixed(2)]).sort((a, b) => String(a[0]).localeCompare(String(b[0]))) })),
      jobs: [...Engrave.items().values()].map(j => ({ key: j.key, state: j.state, approvedAt: j.approvedAt, centre: j.fit && j.fit.centre, size: j.fit && j.fit.size })) }));
    const same = JSON.stringify(after.pages.map(p => [p.id, p.pl]).sort()) === JSON.stringify(before.pages.map(p => [p.id, p.pl]).sort());
    say('5 with the cloud: the live sheets are back with the very pieces and places', same && after.pages.length === before.pages.length, { before: before.pages.length, after: after.pages.length });
    const byKey = new Map(after.jobs.map(j => [j.key, j]));
    const faces = before.jobs.every(b => { const a = byKey.get(b.key); return a && a.state === b.state && a.approvedAt === b.approvedAt && a.centre && b.centre && Math.abs(a.centre[0] - b.centre[0]) < 0.05 && Math.abs(a.centre[1] - b.centre[1]) < 0.05 && Math.abs(a.size - b.size) < 0.02; });
    say('5 with the cloud: every face is fitted again at its place (0.05 mm), states and approvals unchanged', faces && after.jobs.length === before.jobs.length, { before: before.jobs.map(j => [j.state, j.centre && j.centre.map(n => +n.toFixed(2)), j.size && +j.size.toFixed(2)]), after: after.jobs.map(j => [j.state, j.centre && j.centre.map(n => +n.toFixed(2)), j.size && +j.size.toFixed(2)]) });
    // 3b ─ the computer that carries the run: its reload brings every sheet back whole, as before
    const sigOf = pg => pg.evaluate(() => allSheets().filter(p => (p.charms || []).length).map(p => [p.sheetId, p.placements.map(x => [x.id, +x.cxPt.toFixed(2), +x.cyPt.toFixed(2), +(x.angle || 0).toFixed(2)]).sort((a, b) => String(a[0]).localeCompare(String(b[0])))]).sort((a, b) => String(a[0]).localeCompare(String(b[0]))));
    const carrierBefore = await sigOf(page);
    await page.evaluate(() => Session.flush(true));
    await page.reload();
    await page.waitForFunction(() => window.Session && Session.ready && Session.ready() && allSheets().some(p => (p.charms || []).length), null, { timeout: 120000, polling: 500 });
    await page.waitForTimeout(2000);
    const carrierAfter = await sigOf(page);
    say('3b the computer that carries the run: a reload brings every sheet back whole, with the very pieces and places', carrierBefore.length > 0 && JSON.stringify(carrierBefore) === JSON.stringify(carrierAfter), { before: carrierBefore.length, after: carrierAfter.length });
    say('no page error on the second computer', two.errors.length === 0, two.errors);
  } finally { await H.close(); }
  console.log(fails ? `\n${fails} FAILED` : '\nall passed');
  if (fails) process.exit(1);
})().catch(e => { console.error('FAIL', e && e.stack || e); process.exit(1); });
