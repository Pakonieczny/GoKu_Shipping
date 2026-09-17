// Browser test of the Charm Sorter ⇄ Design Station bridge (design document §15): both pages on two loopback origins,
// the station framed by the sorter, a master file indexed through the sorter, a whole Auto run (pull → claim → pool →
// nest → engrave review → labels → commit), then heartbeat recovery after a frame reload and an undo of the set.
//   node tests/charm-nest/bridge.cjs [playwright-core dir]
const fs = require('fs'), path = require('path'), assert = require('assert'), os = require('os');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('./bridge-server.cjs');
const { buildMaster } = require('./fixture-master.cjs');

const day = Math.floor(Date.now() / 1000);
const tx = (rid, i, sku, extra = {}) => Object.assign({ transaction_id: Number(`${rid}${i}`), listing_id: 1718000 + i, receipt_id: rid, sku, title: `${sku} charm`, quantity: 1, expected_ship_date: day + 86400 * (2 + i), variations: [{ formatted_name: 'Metal', formatted_value: '14k Gold Filled' }], is_personalized: false }, extra);
const receipt = (rid, txs, extra = {}) => Object.assign({ receipt_id: rid, order_number: rid, name: 'Buyer ' + rid, country_iso: 'US', city: 'Austin', message_from_buyer: '', update_timestamp: day, create_timestamp: day - 3600, status: 'Paid', is_shipped: false, transactions: txs }, extra);
const receipts = [
  receipt(3521000001, [tx(3521000001, 1, 'BR-TST-01')]),
  receipt(3521000002, [tx(3521000002, 1, 'BR-TST-02', { is_personalized: true, variations: [{ formatted_name: 'Metal', formatted_value: 'Sterling Silver' }, { formatted_name: 'Personalization', formatted_value: 'ANNA' }] })]),
  receipt(3521000003, [tx(3521000003, 1, 'BR-TST-03', { quantity: 2 })]),
  receipt(3521000004, [tx(3521000004, 1, 'BR-NOPE-99')]),                       // not in any master file → held, never committed
  receipt(3521000005, [tx(3521000005, 1, 'BR-TST-04', { expected_ship_date: day + 86400 * 40 })]) // far out: left out by the "due by" pull rule
];

(async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cn-bridge-'));
  const masterPath = path.join(tmp, 'BRITES-master.ai');
  const fx = await buildMaster(masterPath, { count: 4, edge: false });
  const srv = await start({ receipts });
  const { st, sorterOrigin, stationOrigin } = srv;
  console.log('sorter', sorterOrigin, '· station', stationOrigin);
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 1000 } });
  // the station's CDN scripts: Firebase is not needed (direct writes are optional), the QR library is served from the repo
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
  await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
  await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
  // tokenised Storage URLs (the real handlers build them for the test bucket) are answered from the in-memory blob store
  await ctx.route(/firebasestorage\.googleapis\.com/, r => { const u = new URL(r.request().url()); const m = /\/o\/(.+)$/.exec(u.pathname); const key = m ? decodeURIComponent(m[1]) : ''; const b = st.blobs.get(key); if (!b) return r.fulfill({ status: 404, body: 'no blob ' + key }); return r.fulfill({ status: 200, headers: { 'Content-Type': b.meta.contentType || 'application/octet-stream', 'Access-Control-Allow-Origin': '*', 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: b.buf }); });
  // the station is "signed in": a token in its origin's storage (Etsy is the in-memory server); the sorter's settings point at the station
  await ctx.addInitScript(({ station, sorter }) => {
    if (location.origin === station) { localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200)); localStorage.setItem('employee_name', 'Tester'); }
    if (location.origin === sorter) { localStorage.setItem('cn.employee', 'Tester'); }
    window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {};
    if (window.Notification) { try { Object.defineProperty(window, 'Notification', { value: undefined }); } catch (_) {} }
  }, { station: stationOrigin, sorter: sorterOrigin });
  const page = await ctx.newPage();
  const errors = [];
  page.on('pageerror', e => errors.push('sorter pageerror: ' + e.message));
  page.on('console', m => { if (m.type() === 'error') errors.push('console: ' + m.text().slice(0, 300)); if (process.env.CN_VERBOSE) console.log('  [page]', m.type(), m.text().slice(0, 200)); });
  ctx.on('page', p => p.on('pageerror', e => errors.push('popup pageerror: ' + e.message)));
  page.on('frameattached', f => f.on && f.on('pageerror', e => errors.push('frame pageerror: ' + e.message)));

  await page.goto(`${sorterOrigin}/charm-nest-1.html`);
  await page.waitForFunction(() => window.CN && window.CN.S && window.RunCtl && window.DesignLink && CN.S.cloud.ok !== null);
  await page.evaluate((station) => { const s = CN.S.settings; s.dsOrigin = station; s.engine = 'solver'; s.budgetS = 12; s.review = 'off'; s.naming = 'off'; s.notify = 'off'; s.sound = 'off'; s.autoCommit = 'on'; s.runMode = 'manual'; s.pullMode = 'dueBy'; const d = new Date(Date.now() + 86400 * 1000 * 20); s.pullDueBy = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`; s.heartbeatS = 2; s.heartbeatMiss = 2; s.engraveConfidence = 0.9; CN.saveSettings && CN.saveSettings(); }, stationOrigin);
  const cloud = await page.evaluate(() => CN.S.cloud);
  assert(cloud.ok, 'sorter sees the cloud: ' + JSON.stringify(cloud));

  // ── 1 · master indexing through the sorter (§6.3) ──
  await page.evaluate(() => CN.setMode('master'));
  await page.waitForSelector('#mFile', { state: 'attached' });
  await page.setInputFiles('#mFile', masterPath);
  await page.waitForFunction(() => { const j = [...B.master.jobs.values()][0]; return j && ['done', 'error'].includes(j.state); }, null, { timeout: 120000 });
  const mj = await page.evaluate(() => { const j = [...B.master.jobs.values()][0]; return { state: j.state, error: j.error, written: j.written, log: j.log, entries: [...B.master.entries.keys()], blocked: j.blocked }; });
  console.log('master', JSON.stringify(mj));
  assert.strictEqual(mj.state, 'done', mj.error);
  assert.strictEqual(mj.written, fx.charms.length, 'every labelled charm indexed');
  assert.deepStrictEqual(mj.entries.sort(), fx.charms.map(c => c.sku).sort());
  const ix = st.list('Charm_Master_Index');
  assert(ix.length >= fx.charms.length && ix.every(e => e.aiPath && st.blobs.has(e.aiPath)), 'per-SKU .ai files uploaded: ' + ix.map(e => e.aiPath).join(','));

  // the same sheet again: the SKUs are read, matched against the library and skipped, so nothing is rebuilt (§6.3)
  const blobsBefore = st.blobs.size;
  await page.setInputFiles('#mFile', masterPath);
  await page.waitForFunction(() => [...B.master.jobs.values()].every(j => ['done', 'error'].includes(j.state)) && [...B.master.jobs.values()].some(j => j.held), null, { timeout: 60000 });
  const second = await page.evaluate(() => { const j = [...B.master.jobs.values()].pop(); return { state: j.state, held: j.held, written: j.written, log: j.log }; });
  console.log('second pass', JSON.stringify(second));
  assert.strictEqual(second.state, 'done', 'a sheet with nothing new finishes');
  assert(second.held === fx.charms.length && !second.written, 'every charm was already held, so none was rebuilt');
  assert.strictEqual(st.blobs.size, blobsBefore, 'and nothing was uploaded');
  // drop one SKU from the library and the same sheet rebuilds only that charm
  await page.evaluate(() => { B.master.entries.delete('BR-TST-03'); });
  st.docs.delete('Charm_Master_Index/BR-TST-03');
  await page.evaluate(() => B.master.jobs.clear());
  await page.setInputFiles('#mFile', masterPath);
  await page.waitForFunction(() => [...B.master.jobs.values()].some(j => ['done', 'error'].includes(j.state)), null, { timeout: 60000 });
  const third = await page.evaluate(() => { const j = [...B.master.jobs.values()].pop(); return { state: j.state, held: j.held, written: j.written }; });
  console.log('one new SKU', JSON.stringify(third));
  assert(third.state === 'done' && third.written === 1 && third.held === fx.charms.length - 1, 'only the charm with the missing SKU is rebuilt: ' + JSON.stringify(third));
  assert(st.docs.has('Charm_Master_Index/BR-TST-03'), 'the missing SKU is back in the index');
  await page.evaluate(() => B.master.jobs.clear());

  // the grid is one tile per charm, listing every SKU that charm is sold under, and any of them finds it (§6.3)
  const gridOf = async q => page.evaluate((qq) => {
    document.querySelector('#mSearch').value = qq; Master.render();
    return { tiles: [...document.querySelectorAll('#mGrid .skuTile')].map(t => [...t.querySelectorAll('.sku, .meta div')].map(x => x.textContent.trim()).filter(Boolean)), count: document.querySelector('#mCount').textContent };
  }, q);
  const gAll = await gridOf('');
  console.log('grid', JSON.stringify(gAll).slice(0, 300));
  assert.strictEqual(gAll.tiles.length, fx.charms.length, 'one tile per charm');
  // a second SKU on the same design file: one charm sold as two things
  await page.evaluate(() => { const e = Master.entryFor('BR-TST-01'); B.master.entries.set('ZZ-SAME-01', Object.assign({}, e, { sku: 'ZZ-SAME-01' })); });
  const gTwo = await gridOf('');
  assert.strictEqual(gTwo.tiles.length, fx.charms.length, 'a second SKU on one design does not add a tile');
  const both = gTwo.tiles.find(t => t.includes('BR-TST-01') && t.includes('ZZ-SAME-01'));
  assert(both, 'both SKUs are listed on the one tile: ' + JSON.stringify(gTwo.tiles));
  for (const q of ['ZZ-SAME-01', 'BR-TST-01']) {
    const g = await gridOf(q);
    assert(g.tiles.length === 1 && g.tiles[0].includes('BR-TST-01') && g.tiles[0].includes('ZZ-SAME-01'), `searching ${q} finds that one charm with both SKUs: ` + JSON.stringify(g.tiles));
  }
  await page.evaluate(() => { B.master.entries.delete('ZZ-SAME-01'); document.querySelector('#mSearch').value = ''; Master.render(); });

  // every wait shows a bar with a label and, when the work can be counted, a percentage (no silent screens)
  assert.strictEqual(await page.evaluate(() => typeof window.CNProgress), 'object', 'the progress module is loaded');
  const seen = await page.evaluate(async () => {
    const shots = [];
    const snap = () => {
      const rows = [...document.querySelectorAll('.cnp .cnpRow')].map(r => ({
        label: r.querySelector('.cnpLabel').textContent, pct: r.querySelector('.cnpPct').textContent,
        meta: r.querySelector('.cnpMeta').textContent, width: r.querySelector('.cnpFill').style.width
      }));
      if (rows.length) shots.push(rows);
    };
    const watch = setInterval(snap, 15);
    B.master.loadedAt = 0;
    const p = Master.load(true);
    snap();                                   // the bar is up before the first await returns, not after a delay
    await p;
    clearInterval(watch);
    return shots.flat();
  });
  console.log('progress rows seen', seen.length, JSON.stringify(seen[0] || null));
  assert(seen.length, 'a bar is shown while the library loads');
  assert(seen.some(r => /charm library/i.test(r.label)), 'and it names what is loading: ' + JSON.stringify(seen.slice(0, 3)));
  assert(seen.every(r => r.meta), 'and always shows the time spent');
  const counted = await page.evaluate(() => {
    const t = CNProgress.start('Writing the charm library', { total: 200 });
    t.set(50, 200);
    const r = document.querySelector('.cnp .cnpRow');
    const out = { pct: r.querySelector('.cnpPct').textContent, meta: r.querySelector('.cnpMeta').textContent, width: r.querySelector('.cnpFill').style.width };
    t.end();
    return { out, left: document.querySelectorAll('.cnp .cnpRow').length };
  });
  console.log('counted bar', JSON.stringify(counted));
  assert(counted.out.pct === '25%' && /^25(\.0)?%$/.test(counted.out.width) && /50 \/ 200/.test(counted.out.meta), 'countable work shows a real percentage: ' + JSON.stringify(counted.out));
  assert.strictEqual(counted.left, 0, 'the bar goes away when the work ends');

  // a master dropped where sheets are nested is offered to the library, not queued as 4 charms to cut (§6.3)
  await page.evaluate(() => CN.setMode('nest'));
  await page.setInputFiles('#fileInput', masterPath);
  await page.waitForFunction(() => S.sources.some(s => s.state === 'master' || s.state === 'error'), null, { timeout: 60000 });
  const asSrc = await page.evaluate(() => { const s = S.sources[S.sources.length - 1]; return { state: s.state, lines: s.masterLines, queued: S.unassigned.length, buttons: [...document.querySelectorAll('[data-tolib],[data-nestanyway]')].map(b => b.textContent.trim()) }; });
  console.log('dropped master', JSON.stringify(asSrc));
  assert.strictEqual(asSrc.state, 'master', 'a master file is recognised where sheets are dropped');
  assert(asSrc.queued === 0, 'nothing was queued for nesting');
  assert(asSrc.buttons.length === 2, 'the card offers the library and nesting: ' + JSON.stringify(asSrc.buttons));
  await page.evaluate(() => document.querySelector('[data-tolib]').click());
  await page.waitForFunction(() => [...B.master.jobs.values()].some(j => ['done', 'error'].includes(j.state)), null, { timeout: 120000 });
  const viaDrop = await page.evaluate(() => { const j = [...B.master.jobs.values()].pop(); return { state: j.state, error: j.error, written: j.written, sources: S.sources.length }; });
  assert.strictEqual(viaDrop.state, 'done', 'indexing from the drop card: ' + viaDrop.error);
  assert(viaDrop.sources === 0, 'the source card is gone once it went to the library');
  await page.evaluate(() => CN.setMode('design'));

  // ── 2 · the link: hello, snapshot shape, dropped messages, refusal without a preview (§5, §15) ──
  await page.evaluate(() => CN.setMode('design'));
  await page.evaluate(() => DesignLink.ensure());
  const hello = await page.evaluate(() => DesignLink.state());
  console.log('hello', JSON.stringify({ bench: hello.bench, counts: hello.counts, etsy: hello.etsy, employee: hello.employee }));
  assert(hello.counts && typeof hello.counts.open === 'number' && Array.isArray(hello.selection), 'hello carries the counts and the selection');
  assert(hello.etsy.signedIn, 'station reports its Etsy token');
  const frame = page.frames().find(f => f.url().startsWith(stationOrigin));
  assert(frame, 'station frame present');
  frame.on && frame.on('pageerror', e => errors.push('frame: ' + e.message));
  const snap = await page.evaluate(() => DesignLink.call('orders.snapshot', { hydrate: true, refresh: true }));
  assert.strictEqual(snap.total, receipts.length); assert.strictEqual(snap.hydrated, receipts.length, 'every order hydrated');
  const o2 = snap.orders.find(o => o.receiptId === '3521000002');
  assert(o2 && o2.lines[0].metalKey === 'silver' && o2.lines[0].personalization[0] === 'ANNA' && o2.shipBy > 0, 'order shape: ' + JSON.stringify(o2));
  // a message with the wrong nonce is dropped and counted at the station; one from an origin not in the list never reaches a command
  await page.evaluate(() => DesignLink._S.frame.contentWindow.postMessage({ source: 'brites-sorter', nonce: 'bogus', id: 999, type: 'claim', args: { receiptIds: ['3521000001'] } }, '*'));
  await page.waitForTimeout(300);
  const dropped = await frame.evaluate(() => DesignStation.bridge.state().dropped);
  assert(dropped >= 1, 'wrong nonce dropped and counted');
  await assert.rejects(page.evaluate(() => DesignLink.call('complete.commit', { receiptIds: ['3521000001'], labels: { files: [{ path: 'x' }] } })), /preview/, 'commit without a preview is refused');
  await assert.rejects(page.evaluate(() => DesignLink.call('complete.preview', { receiptIds: ['3521000001'] })), /select/, 'preview needs the selection');
  // a claim shows at the station and is written through the functions
  await page.evaluate(() => DesignLink.call('claim', { receiptIds: ['3521000001'] }));
  assert(st.doc('Design_RealTime_Selected_Orders', '3521000001').claimed === true, 'claim recorded');
  const claimedRow = await frame.evaluate(() => { const el = document.querySelector(".orderRow[data-receipt='3521000001']"); return el ? { claimed: el.classList.contains('claimed'), title: el.dataset.claim || '' } : null; });
  assert(claimedRow && claimedRow.claimed && /Charm Sorter/.test(claimedRow.title), 'the station row shows the claim: ' + JSON.stringify(claimedRow));
  await page.evaluate(() => DesignLink.call('unclaim', { receiptIds: ['3521000001'] }));

  // ── 2b · Connect Etsy from the sorter: Etsy cannot be framed, so the station opens in its own popup on its origin, signs
  //         in, reports back to its opener and closes; the framed station then reads as signed in ──
  await frame.evaluate(() => { localStorage.removeItem('access_token'); localStorage.removeItem('refresh_token'); });
  await page.evaluate(() => DesignLink.open());
  assert.strictEqual((await page.evaluate(() => DesignLink.state().etsy.signedIn)), false, 'the station reads as signed out');
  const [popup] = await Promise.all([ctx.waitForEvent('page', { timeout: 15000 }), page.click('#dsConnectEtsy')]);
  assert(/\/design-1\.html\?connect=1$/.test(popup.url()) && popup.url().startsWith(stationOrigin), 'the popup opens the station on its own origin with connect=1: ' + popup.url());
  await page.waitForFunction(() => DesignLink.state() && DesignLink.state().etsy.signedIn === true && CN.AG.events.some(e => /Etsy connected at the station/.test(e.text || '')), null, { timeout: 20000 });
  await page.waitForTimeout(2000);
  assert(popup.isClosed(), 'the popup closed itself after reporting');
  assert(await frame.evaluate(() => DesignStation.bridge.cursor.log.some(l => l.role === 'connect')), 'the cursor showed the Connect Etsy button on the framed station');
  console.log('connect etsy: popup reported back, station signed in again');
  // ── 3 · the whole run in Auto (§9) — it pauses for a person only at the engraving review ──
  await page.evaluate(() => CN.setMode('orders'));
  await page.evaluate(() => RunCtl.setMode('auto'));
  await page.waitForFunction(() => B.run && B.run.status !== undefined, null, { timeout: 10000 });
  // §5.7 · the live view: on any other tab the station frame is a picture-in-picture panel, never re-parented (one hello so far)
  await page.waitForFunction(() => Dock.mode() === 'pip', null, { timeout: 5000 });
  const hellosAtStart = await page.evaluate(() => DesignLink.log.filter(r => r.dir === 'cmd' && r.type === 'hello').length);
  const dock = await page.evaluate(() => ({ mode: Dock.mode(), visible: !document.getElementById('dsDock').classList.contains('hidden'), scaled: /matrix\(0\./.test(getComputedStyle(document.getElementById('dsFrame')).transform), hellos: DesignLink.log.filter(r => r.dir === 'cmd' && r.type === 'hello').length }));
  assert(dock.mode === 'pip' && dock.visible && dock.scaled && dock.hellos === hellosAtStart, 'live panel shown while running on another tab: ' + JSON.stringify(dock));
  const approvals = [];
  let t0 = Date.now(), lastStep = '', lastJobs = '', lastDone = '';
  while (Date.now() - t0 < 600000) {
    const r = await page.evaluate(() => B.run && { status: B.run.status, step: B.run.step, stoppedBy: B.run.stoppedBy, fix: B.run.fix, setId: B.run.setId });
    if (r && (r.step !== lastStep || r.status === 'stopped')) { lastStep = r.step; console.log('  run', r.status, r.step, r.stoppedBy || ''); }
    if (!r) { await page.waitForTimeout(300); continue; }
    if (r.status === 'complete') break;
    if (r.status === 'stopped') { const ag = await page.evaluate(() => CN.AG.events.slice(-12).map(e => e.text)); throw new Error(`run stopped: ${r.stoppedBy} — ${r.fix}\n${ag.join('\n')}\n${errors.join('\n')}`); }
    if (r.status === 'review') {
      const jobs = await page.evaluate(() => [...Engrave.items().values()].map(j => ({ key: j.key, state: j.state, reason: j.reason, text: j.text, conf: j.confidence })));
      const jk = JSON.stringify(jobs); if (jk !== lastJobs) { lastJobs = jk; console.log('  engrave jobs', jk); console.log('  agent', JSON.stringify(await page.evaluate(() => CN.AG.events.filter(e => /engrav|ENGRAVE|Claude|font|Myriad/i.test(e.text || '')).slice(-8).map(e => e.text)))); }
      if (jobs.some(j => j.state === 'blocked')) throw new Error('engraving blocked: ' + jk);
      if (jobs.some(j => j.state === 'ready') && Date.now() - t0 > 90000) throw new Error('engraving never fitted: ' + JSON.stringify(await page.evaluate(() => [...Engrave.items().values()].map(j => { const c = Pool.charmOf(j.copies[0]); const sh = Pool.sheetOf(j.copies[0]); return { copies: j.copies, charm: c && c.id, sheet: sh && { fileBase: sh.fileBase, status: sh.status, ids: sh.placements.map(p => p.id), charmIds: sh.charms.map(c => c.id + ':' + c.poolId) } }; }))));
      const done = await page.evaluate(async () => {
        const out = [];
        for (const j of Engrave.items().values()) {
          if (j.state === 'words') { await Engrave.decideWords(j, { text: j.text, by: 'Tester' }); out.push(['words', j.key]); }
          else if (j.state === 'review') { await Engrave.approve(j, 'Tester'); out.push(['approve', j.key, j.fit && +j.fit.size.toFixed(2), j.state]); }
        }
        for (const it of Review.items()) if (it.kind === 'heldOrder' || it.kind === 'unmatchedSku') out.push(['left', it.kind, it.key]);
        return out;
      });
      if (done.length) { approvals.push(...done); const dk = JSON.stringify(done); if (dk !== lastDone) { lastDone = dk; console.log('  review actions', dk); } }
    }
    await page.waitForTimeout(700);
  }
  const run = await page.evaluate(() => Object.assign({}, B.run, { lines: Object.fromEntries(Object.entries(B.run.lines).map(([k, v]) => [k, v.state])) }));
  console.log('run', JSON.stringify({ status: run.status, step: run.step, committed: run.committed, refused: run.refused, holds: Object.keys(run.holds || {}), lines: run.lines, setId: run.setId, sheets: run.sheets }));
  assert.strictEqual(run.status, 'complete', 'run completed');
  assert(approvals.some(a => a[0] === 'approve'), 'the engraving placement was approved by a person');
  assert.deepStrictEqual((run.committed || []).sort(), ['3521000001', '3521000002', '3521000003'], 'the three ready orders were committed');
  assert(Object.keys(run.holds || {}).includes('3521000004'), 'the unmatched SKU held its order');
  assert(!Object.keys(run.lines).some(k => k.startsWith('3521000005_')), 'the far-out order was left out by the pull rule');

  // ── 4 · the set, its sheets, labels and back files (§7, §8) ──
  const set = await page.evaluate(() => { const s = [...B.sets.values()].find(x => x.setId === B.run.setId); return s && { setId: s.setId, name: s.name, folder: s.folder, seq: s.seq, sheetIds: s.sheetIds, materials: s.materials, labels: s.labels, status: s.status, backCount: s.backCount, orders: Object.keys(s.orders) }; });
  console.log('set', JSON.stringify(set));
  assert(set && set.seq === 1 && set.name === 'Set-1' && /\/Set-1$/.test(set.folder), 'first set of the day');
  assert(set.materials.includes('gold') && set.materials.includes('silver'), 'gold and silver sheets in the set');
  assert(set.labels && set.labels.files.length === set.sheetIds.length && set.labels.pdf, 'one label per sheet and a labels PDF');
  assert.strictEqual(set.backCount, 1, 'one engraved back');
  for (const f of set.labels.files) assert(st.blobs.has(f.path), 'label PNG saved: ' + f.path);
  assert(st.blobs.has(`${set.folder}/set.json`) && st.blobs.has(`${set.folder}/${set.name}_manifest.pdf`), 'set.json and manifest saved');
  const sheets = st.list('Charm_Nest_Sheets').filter(s => s.setId === set.setId);
  assert.strictEqual(sheets.length, set.sheetIds.length, 'sheet records carry the set');
  for (const sh of sheets) { assert(sh.label && sh.label.files.length && sh.label.files.every(f => /^B36\|(gold|silver|rose|gold10k|gold14k)\|/.test(f.payload) && f.ecc === 'M'), 'sheet label payload: ' + JSON.stringify(sh.label)); }
  const backs = st.list('Charm_Pool_Back');
  assert(backs.length === 1 && backs[0].text === 'ANNA' && st.blobs.has(backs[0].outputs && backs[0].outputs.ai && backs[0].outputs.ai.path || ''), 'the back file record and .ai: ' + JSON.stringify(backs[0]));
  const silverSheet = sheets.find(s => s.metal === 'silver');
  assert(silverSheet && silverSheet.backOutputs && st.blobs.has(silverSheet.backOutputs.index.path), 'the silver sheet has a back-index.pdf');
  // the label payload decodes to the sheet's orders
  const O = require(path.join(root, 'charm-nest-orders.js'));
  for (const sh of sheets) for (const f of sh.label.files) { const parts = f.payload.split('|'); assert.strictEqual(parts[0], 'B36'); const ids = parts[2].split('.').map(s => String(parseInt(s, 36))); assert.deepStrictEqual(ids.sort(), f.orders.slice().sort(), 'label decodes to the sheet orders'); }
  void O;

  // ── 5 · the station side of the commit (§8.4): rows gone, ledger, archive with the labels ──
  const stationState = await frame.evaluate(() => Object.assign(DesignStation.bridge.snapshot(), { rows: document.querySelectorAll('.orderRow, [data-rid]').length }));
  assert.strictEqual(stationState.counts.open, 2, 'the two uncommitted orders stay open at the station: ' + JSON.stringify(stationState.counts));
  // §5.7 · the remote cursor: the station's own record of every motion the sorter made, and the sorter's feed on its banner
  const motions = await frame.evaluate(() => DesignStation.bridge.cursor.log.map(l => ({ role: l.role, caption: l.caption, click: l.click, fallback: l.fallback })));
  const clicked = roles => roles.every(r => motions.some(m => m.role === r && m.click));
  assert(clicked(['row', 'complete', 'print']), 'the cursor pressed the rows, Generate QR and the print/complete button: ' + JSON.stringify(motions.slice(-12)));
  assert(motions.some(m => m.role === 'row' && /^Claim /.test(m.caption)) && motions.some(m => /^Select /.test(m.caption)), 'claims and selections were shown as clicks');
  assert(!motions.some(m => m.fallback && m.role !== 'banner'), 'no motion fell back to the banner: ' + JSON.stringify(motions.filter(m => m.fallback)));
  const feed = await frame.evaluate(() => ({ rows: DesignStation.bridge.state().feed.length, kinds: [...new Set(DesignStation.bridge.state().feed.map(r => r.kind))], shown: !document.getElementById('bridgeFeed').classList.contains('hidden'), now: document.getElementById('bridgeFeedNow').textContent }));
  console.log('station feed', JSON.stringify(feed));
  assert(feed.rows >= 8 && feed.shown && feed.kinds.includes('POOL') && feed.kinds.includes('ENGRAVE') && (feed.kinds.includes('GF') || feed.kinds.includes('SS')), 'the sorter narrated pooling, engraving and nesting on the station banner: ' + JSON.stringify(feed));
  // Etsy thrift: the whole run costs one list sweep at the pull, one detail read per order for the first hydration, one list
  // sweep per re-validation (or none within the cooldown) and the station's own archive reads — never a detail read per
  // order at re-validation, and the sorter's meter agrees with the server's count
  const etsyCalls = st.calls.filter(c => c.name === 'listOpenOrders' || c.name === 'etsyOrderProxy');
  const proxies = etsyCalls.filter(c => c.name === 'etsyOrderProxy').length, lists = etsyCalls.filter(c => c.name === 'listOpenOrders').length;
  const meterLine = await page.evaluate(() => ({ etsy: DesignLink.etsy(), lines: CN.AG.events.filter(e => /^Etsy: /.test(e.text || '')).map(e => e.text) }));
  console.log('etsy calls', { lists, proxies, meter: meterLine.etsy }, meterLine.lines);
  console.log('etsy sequence', st.calls.filter(c => c.name === 'listOpenOrders' || c.name === 'etsyOrderProxy').map(c => c.name === 'listOpenOrders' ? 'LIST' : c.q.orderId).join(' '));
  console.log('revalidation lines', JSON.stringify(await page.evaluate(() => CN.AG.events.filter(e => /Re-validat|open-list|fresh read/.test(e.text || '')).map(e => e.text))));
  console.log('station hydration', await frame.evaluate(() => ({ cached: DesignStation.cache.detailCache.size, hydrated: DesignStation.state().allOpenReceipts.map(r => [String(r.receipt_id), !!r._hydrated, r._stamp]) })));
  assert(lists <= 4, 'at most one list sweep per pull and re-validation: ' + lists);
  assert(proxies <= receipts.length + run.committed.length, `detail reads: first hydration (${receipts.length}) plus the archive (${run.committed.length}) at most, got ${proxies}`);
  assert(meterLine.lines.some(l => /open-list check/.test(l)), 'the re-validation was metered as an open-list check');

  // the archive is written in the background after the commit (the operator is never made to wait for it): allow it a moment
  for (let i = 0; i < 60 && !['3521000001', '3521000002', '3521000003'].every(id => st.doc('Design_Order_Archive', id)); i++) await page.waitForTimeout(250);
  // both readouts show the station's ledger and it matches what the server actually received (read at one instant,
  // after the background archive reads have settled and one heartbeat has carried the ledger to the sorter)
  await page.waitForTimeout(2600);
  const serverEtsy = st.calls.filter(c => /^(etsy|etsyImages|etsyOrderProxy|listOpenOrders|refreshEtsyToken|exchangeToken)$/.test(c.name)).length;
  const hud = await frame.evaluate(() => ({ total: +document.getElementById('etsyTotal').textContent, tenMin: +document.getElementById('etsy10m').textContent, today: +document.getElementById('etsyDay').textContent, alarm: document.getElementById('etsyHud').classList.contains('alarm') }));
  const pill = await page.evaluate(() => ({ text: document.getElementById('etsyPillN').textContent, cls: document.getElementById('etsyPill').className, meter: DesignLink.etsy().meter && { total: DesignLink.etsy().meter.total, last10Min: DesignLink.etsy().meter.last10Min, today: DesignLink.etsy().meter.today } }));
  console.log('readouts', { server: serverEtsy, hud, pill });
  assert.strictEqual(hud.total, serverEtsy, 'the station HUD counts exactly the calls the server received');
  assert(hud.today >= hud.total && hud.tenMin <= hud.total, 'today and 10-minute figures are consistent');
  assert(pill.meter && pill.meter.total === serverEtsy && pill.meter.today === serverEtsy && pill.text.startsWith('today ' + serverEtsy + ' ·'), 'the sorter pill shows the station ledger: ' + JSON.stringify(pill));
  assert(!hud.alarm && !/alarm/.test(pill.cls), 'no watchdog alarm on a normal run');
  for (const id of ['3521000001', '3521000002', '3521000003']) {
    assert(st.doc('Design_Completed Orders', id), 'ledger has ' + id);
    const ar = st.doc('Design_Order_Archive', id); assert(ar && ar.labels && ar.setId === set.setId && ar.runId === run.runId, 'archive row carries labels/set/run: ' + JSON.stringify(ar && { labels: !!ar.labels, setId: ar.setId }));
    const rt = st.doc('Design_RealTime_Selected_Orders', id); assert(!rt || !rt.claimed, 'claim released after the run');
  }
  assert(!st.doc('Design_Completed Orders', '3521000004') && !st.doc('Design_Completed Orders', '3521000005'), 'held and unpulled orders are not complete');
  const pools = st.list('Charm_Pool');
  assert(pools.length === 4 && pools.filter(p => p.state === 'committed').length === 4, 'four pool rows (3 lines, one ×2) committed: ' + pools.map(p => p.poolId + ':' + p.state).join(' '));
  const runRec = st.doc('Charm_Nest_Runs', run.runId); assert(runRec && runRec.status === 'complete', 'run record complete');
  const log = st.list('Design_Bridge'); assert(log.length >= 1, 'bridge session recorded');
  // the commit is idempotent per run: the same request again returns the same result without touching the ledger
  const again = await page.evaluate((ids) => DesignLink.call('complete.commit', { receiptIds: ids, labels: { files: [{ path: 'x' }] }, runId: B.run.runId }), run.committed);
  assert(again.idempotent && again.completed.length === 3, 'second commit for the run is a no-op: ' + JSON.stringify(again));

  await page.evaluate(() => CN.setMode('design')); await page.waitForTimeout(250);
  const full = await page.evaluate(() => { const d = document.getElementById('dsDock').getBoundingClientRect(), h = document.querySelector('.dsFrameHost').getBoundingClientRect(); return { mode: Dock.mode(), fits: Math.abs(d.left - h.left) < 2 && Math.abs(d.width - h.width) < 2 && d.height > 300, hellos: DesignLink.log.filter(r => r.dir === 'cmd' && r.type === 'hello').length }; });
  assert(full.mode === 'full' && full.fits && full.hellos === hellosAtStart, 'the Design Station tab shows the same frame full size (no reload across tabs): ' + JSON.stringify(full));
  await page.screenshot({ path: path.join(tmp, 'bridge-design-tab.png') });
  // ── 6 · heartbeat loss and recovery: the frame reloads, the sorter says hello again by itself (§5.6) ──
  const helloBefore = await page.evaluate(() => DesignLink._S.lastHello);
  await frame.evaluate(() => location.reload());
  await page.waitForFunction((before) => DesignLink.up() && DesignLink.state() && DesignLink._S.lastHello > before, helloBefore, { timeout: 30000 });
  const snap2 = await page.evaluate(() => DesignLink.call('orders.snapshot', { hydrate: false, refresh: true, withNotes: false }));
  assert.strictEqual(snap2.total, 2, 'after the reload the station still lists the two open orders: ' + snap2.total);
  const hellos = await page.evaluate(() => DesignLink.log.filter(r => r.dir === 'cmd' && r.type === 'hello').length);
  assert(hellos >= 2, 'a second hello after the reload');
  const frame2 = page.frames().find(f => f.url().startsWith(stationOrigin));
  assert.strictEqual(await frame2.evaluate(() => DesignStation.bridge.active()), true, 'station active again');
  // the station's own banner shows the sorter in control and hides the print button
  const banner = await frame2.evaluate(() => ({ shown: !document.getElementById('bridgeBanner').classList.contains('hidden'), text: document.getElementById('bridgeBanner').textContent.slice(0, 80) }));
  assert(banner.shown, 'banner shown: ' + JSON.stringify(banner));

  // ── 7 · undo the set: orders return to the open list, ledger cleared, files kept (§8.5) ──
  await page.evaluate(() => Sets.undo([...B.sets.values()].find(x => x.setId === B.run.setId)));
  await page.waitForTimeout(500);
  for (const id of ['3521000001', '3521000002', '3521000003']) assert(!st.doc('Design_Completed Orders', id), 'ledger cleared for ' + id);
  assert(await frame2.evaluate(() => DesignStation.bridge.cursor.log.some(l => l.role === 'undo' && l.click)), 'the undo was pressed by the cursor');
  const after = await frame2.evaluate(() => DesignStation.bridge.snapshot().counts);
  assert.strictEqual(after.open, 5, 'all five orders open again: ' + JSON.stringify(after));
  assert(st.blobs.has(`${set.folder}/set.json`), 'files kept after the undo');
  const setRec = st.doc('Charm_Nest_Sets', set.setId); assert(setRec && setRec.status === 'awaiting review', 'set back to awaiting review');

  // ── 7b · the Etsy watchdog: a station that sees one order re-read over and over, or a burst, raises the alarm, brakes
  //         automatic Etsy work, and tells the sorter — whose pill turns red and whose run stops ──
  await page.evaluate(() => RunCtl.start({ mode: 'manual' }).catch(() => {}));
  await page.waitForFunction(() => B.run && B.run.status !== 'complete', null, { timeout: 5000 });
  const before = await frame2.evaluate(() => ({ total: DesignStation.etsyMeter ? DesignStation.etsyMeter.total : null }));
  const alarm = await frame2.evaluate(async () => { for (let i = 0; i < 5; i++) await DesignStation.pullEtsyOrderDetails('3521000004'); return { alarm: document.getElementById('etsyHud').classList.contains('alarm'), why: DesignStation.etsyMeterSnapshot().alarm && DesignStation.etsyMeterSnapshot().alarm.why, braked: DesignStation.etsyMeterSnapshot().braked }; });
  console.log('watchdog', JSON.stringify(alarm), before);
  assert(alarm.alarm && alarm.braked && /read 5 times/.test(alarm.why), 'the repeated read raised the alarm and the brake: ' + JSON.stringify(alarm));
  await assert.rejects(page.evaluate(() => DesignLink.call('orders.detail', { receiptId: '3521000004', fresh: true })), /watchdog brake/, 'a fresh read over the bridge is refused during the brake');
  const chkBrake = await page.evaluate(() => DesignLink.call('orders.check', { receiptIds: ['3521000004'] }).then(r => ({ ok: true, swept: r.swept, calls: r.etsyCalls }), e => ({ ok: false, error: e.message })));
  assert((!chkBrake.ok && /watchdog brake/.test(chkBrake.error)) || (chkBrake.ok && chkBrake.swept === false && chkBrake.calls === 0), 'during the brake a check is refused or answered from memory at no cost: ' + JSON.stringify(chkBrake));
  await page.waitForFunction(() => document.getElementById('etsyPill').classList.contains('alarm') && B.run && B.run.status === 'stopped', null, { timeout: 8000 });
  const stopped = await page.evaluate(() => ({ status: B.run.status, why: B.run.stoppedBy, pill: document.getElementById('etsyPill').className, warned: CN.AG.events.some(e => /Etsy watchdog at the station/.test(e.text || '')) }));
  assert(/Etsy watchdog/.test(stopped.why) && /alarm/.test(stopped.pill) && stopped.warned, 'the sorter stopped its run on the alarm and shows it: ' + JSON.stringify(stopped));
  // a person's own Refresh at the station still works during the brake (the brake is for automatic work only)
  const manual = await frame2.evaluate(async () => { const t0 = DesignStation.etsyMeter.total; await DesignStation.refreshOrders(); return DesignStation.etsyMeter.total - t0; });
  assert(manual >= 1, 'a manual refresh at the station is not blocked by the brake');
  await page.evaluate(() => { RunCtl.stop('test over'); RunCtl.clearRunState(); });
  await page.screenshot({ path: path.join(tmp, 'bridge-watchdog.png') });
  // ── 8 · a second run the same day numbers Set-2 and the released orders can be pulled again ──
  const seq2 = await page.evaluate(async () => { const r = await CN.api('charmNestLibrary', { op: 'setAllocate', day: CharmNestOrders.localDay(), runId: 'run-probe' }); return r.seq; });
  assert.strictEqual(seq2, 2, 'the next set of the day is Set-2');

  await page.screenshot({ path: path.join(tmp, 'bridge-final.png') });
  await page.evaluate(() => CN.setMode('library'));
  // a File in a browser that implements Blob.bytes() must still be read as a file, not as that method (§6.3)
  {
    const r = await page.evaluate(async (b64) => {
      const bin = atob(b64), u8 = new Uint8Array(bin.length); for (let i = 0; i < bin.length; i++) u8[i] = bin.charCodeAt(i);
      const f = new File([u8], 'BRITES-bytes-method.ai', { type: 'application/pdf' });
      if (!('bytes' in f)) Object.defineProperty(f, 'bytes', { value: async () => u8 });   // some browsers have it, some do not
      try { const job = await Master.indexFile(f); return { ok: true, state: job.state, written: job.written, held: job.held }; }
      catch (e) { return { ok: false, error: e.message }; }
    }, require('fs').readFileSync(masterPath).toString('base64'));
    console.log('File with a bytes() method', JSON.stringify(r));
    assert(r.ok && r.state === 'done', 'a File is read through arrayBuffer, never through its bytes() method: ' + JSON.stringify(r));
  }

  // the station opened in a tab is a mirror: it follows the pointer of the one being driven and runs nothing itself
  {
    const url = await page.evaluate(() => { CN.setMode('design'); Views.designHost(); return document.querySelector('#dsOpenTab').href; });
    console.log('mirror url', url);
    assert(/mirror=1/.test(url), 'the tab link opens a mirror: ' + url);
    const mirror = await ctx.newPage();
    await mirror.goto(url);
    await mirror.waitForFunction(() => document.body && document.body.textContent.includes('Mirror'), null, { timeout: 30000 });
    // a real command from the sorter moves the driven station's pointer; the mirror must move the same way
    await page.evaluate(() => DesignLink.call('ui.closeModal', {}).catch(() => {}));
    await mirror.waitForFunction(() => { const c = document.getElementById('rcCursor'); return c && c.getClientRects().length; }, null, { timeout: 20000 })
      .catch(async () => { throw new Error('the mirror did not follow the pointer · ' + await mirror.evaluate(() => { const c = document.getElementById('rcCursor'); return c ? c.outerHTML.slice(0, 200) : 'no cursor element'; })); });
    console.log('mirror followed the pointer');
    await mirror.close();
  }


  const fatal = errors.filter(e => !/favicon|net::ERR|Notification|AudioContext|ResizeObserver|The play\(\)|Failed to load resource/.test(e));
  if (fatal.length) console.log('page errors:\n  ' + fatal.join('\n  '));
  assert.strictEqual(fatal.length, 0, 'no page errors');
  console.log('bridge e2e OK ·', st.docs.size, 'docs ·', st.blobs.size, 'blobs · screenshots in', tmp);
  await browser.close(); srv.close();
})().catch(e => { console.error(e); process.exit(1); });
