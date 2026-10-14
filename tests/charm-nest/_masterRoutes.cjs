// The three ways a master file gets indexed, over the same synthetic bytes and the same in-memory library (MASTER2), so a test can ask whether they agree:
//   script   scripts/index-master.cjs  (staged: nothing is sent anywhere)
//   server   netlify/functions/charmMaster-background.js
//   browser  the Master tab of charm-nest-1.html in headless Chromium  (the route Paul's drops go through)
// Offline only: the stand-in site of bridge-server.cjs (an in-memory Firestore and Storage). PW_DIR=<playwright node_modules>.
'use strict';
const fs = require('fs'), os = require('os'), path = require('path');
const root = path.join(__dirname, '../..');
const { start, REPO_TEST } = require('./bridge-server.cjs');
const IM = require('../../scripts/index-master.cjs');

async function viaScript(bytes, name, extra) {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cn-route-')), file = path.join(tmp, name); fs.writeFileSync(file, bytes);
  const lines = [], report = await IM.main(['node', 'x', file, '--out-dir', path.join(tmp, 'stage')].concat(extra || []), l => lines.push(String(l)));
  const rec = JSON.parse(fs.readFileSync(path.join(tmp, 'stage', 'records.json'), 'utf8'));
  return { report, entries: rec.entries, lines, tmp };
}
/** The server route over a fresh or an existing stand-in (srv = { st }): one job. */
async function viaServer(bytes, name, srv, id) {
  const s = srv || await start({ receipts: [] }), st = s.st; id = id || 'job-' + Math.random().toString(36).slice(2, 7);
  st.blobs.set('charmnest/uploads/' + id + '.ai', { buf: bytes, generation: 1, meta: { contentType: 'application/pdf', metadata: {} } });
  st.put('Charm_Nest_Jobs', id, { id, kind: 'master', status: 'pending', path: 'charmnest/uploads/' + id + '.ai', name, opts: {} });
  const out = await st.handlers['charmMaster-background'].handler({ body: JSON.stringify({ id }) });
  return { out, job: st.doc('Charm_Nest_Jobs', id), srv: s, st };
}
/** The Master tab: one page over one stand-in library; upload() drops a file and waits for the job. */
async function browser(opts) {
  opts = opts || {};
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  const { chromium } = require(path.join(pwDir, 'playwright-core'));
  const srv = opts.srv || await start({ receipts: [] }), { st, sorterOrigin, stationOrigin } = srv;
  const br = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await br.newContext({ viewport: { width: 1500, height: 1000 } });
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
  await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
  await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
  await ctx.route(/^https:\/\/firebasestorage\.googleapis\.com\//, r => { const u = new URL(r.request().url()); const m = /\/o\/(.+)$/.exec(u.pathname); const key = m ? decodeURIComponent(m[1]) : ''; const b = st.blobs.get(key); if (!b) return r.fulfill({ status: 404, body: 'no blob' }); r.fulfill({ status: 200, contentType: (b.meta && b.meta.contentType) || 'application/octet-stream', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body: b.buf }); });
  await ctx.addInitScript(({ station, sorter }) => {
    if (location.origin === station) { localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200)); localStorage.setItem('employee_name', 'Tester'); }
    if (location.origin === sorter) { localStorage.setItem('cn.employee', 'Tester'); }
    window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {};
    if (window.Notification) { try { Object.defineProperty(window, 'Notification', { value: undefined }); } catch (_) {} }
  }, { station: stationOrigin, sorter: sorterOrigin });
  const page = await ctx.newPage(), errors = [];
  page.on('pageerror', e => errors.push('pageerror: ' + e.message));
  page.on('console', m => { if (m.type() === 'error') errors.push('console: ' + m.text().slice(0, 300)); if (process.env.CN_VERBOSE) console.log('  [page]', m.type(), m.text().slice(0, 200)); });
  await page.goto(`${sorterOrigin}/charm-nest-1.html`);
  await page.waitForFunction(() => window.CN && window.CN.S && window.RunCtl && window.DesignLink && CN.S.cloud.ok !== null);
  await page.evaluate(({ station, review }) => { const s = CN.S.settings; s.dsOrigin = station; s.review = review || 'off'; s.naming = 'off'; s.notify = 'off'; s.sound = 'off'; }, { station: stationOrigin, review: opts.review || 'off' });
  await page.evaluate(() => CN.setMode('master'));
  await page.waitForSelector('#mFile', { state: 'attached' });
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cn-brw-'));
  const api = {
    page, st, srv, errors,
    /** drop a file in the Master tab; resolves with what the tab shows when its job settles */
    async upload(bytes, name, o) {
      o = o || {};
      const file = path.join(tmp, name); fs.writeFileSync(file, bytes);
      await page.evaluate(({ all }) => { const cb = document.querySelector('#mAllSkus'); if (cb) { cb.checked = !!all; cb.onchange && cb.onchange(); } B.master.jobs.clear(); }, { all: !!o.reindexAll });
      await page.setInputFiles('#mFile', file);
      await page.waitForFunction(() => { const j = [...B.master.jobs.values()].pop(); return j && ['done', 'error'].includes(j.state) && !(j.vision || []).some(v => !v.confirmed) || (j && j.state === 'done'); }, null, { timeout: o.timeout || 120000 });
      return page.evaluate(() => {
        const j = [...B.master.jobs.values()].pop(), l = j.lab || {};
        return { state: j.state, error: j.error, written: j.written, held: j.held, log: j.log, blocked: j.blocked, entries: (j.entries || []).map(e => e.sku),
          unlabelled: l.unlabelled || [], orphans: (l.orphans || []).map(x => x.sku), pairs: (j.pairs || []).map(x => ({ sku: x.sku, kind: x.kind, folded: x.folded, bodies: x.bodies })), kept: j.kept || [],
          card: (document.querySelector('#mJobs') || {}).innerText || '' };
      });
    },
    index: sku => st.doc('Charm_Master_Index', sku), blob: p => st.blobs.get(p),
    async close() { await br.close(); if (!opts.srv) srv.close(); }
  };
  return api;
}
module.exports = { viaScript, viaServer, browser, start, REPO_TEST, IM };
