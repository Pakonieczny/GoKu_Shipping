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
  // §5.7 · the live view: a status pill while the run works, the panel when a person asks for it, never re-parented
  await page.waitForFunction(() => Dock.mode() === 'pip', null, { timeout: 5000 });
  const hellosAtStart = await page.evaluate(() => DesignLink.log.filter(r => r.dir === 'cmd' && r.type === 'hello').length);
  const dock = await page.evaluate(() => {
    const d = document.getElementById('dsDock'), r2 = d.getBoundingClientRect();
    return { mode: Dock.mode(), visible: !d.classList.contains('hidden'), h: Math.round(r2.height), w: Math.round(r2.width),
      bar: !!d.querySelector('.dockBar'), state: (document.getElementById('dockState') || {}).textContent || '',
      framed: !!document.getElementById('dsFrame'), hellos: DesignLink.log.filter(r => r.dir === 'cmd' && r.type === 'hello').length };
  });
  assert(dock.mode === 'pip' && dock.visible && dock.hellos === hellosAtStart, 'the live link is shown while running on another tab: ' + JSON.stringify(dock));
  // off its own tab it is a status strip: it says what the station is doing without taking a fifth of the screen
  assert(dock.h <= 64, 'the dock is a strip, not a mirror: ' + dock.h + 'px tall');
  assert(dock.bar && dock.state, 'and it says what the station is doing: ' + JSON.stringify(dock.state));
  assert(dock.framed, 'the frame stays laid out behind it, so the station keeps hydrating');
  // and the screen keeps room for it, so it never sits on top of the work
  const clear = await page.evaluate(() => {
    const d = document.getElementById('dsDock').getBoundingClientRect();
    const st = document.querySelector('.stage');
    return { pad: Math.round(parseFloat(getComputedStyle(st).paddingBottom)), dockH: Math.round(d.height) };
  });
  assert(clear.pad >= clear.dockH, 'the stage reserves the live view\'s height: ' + JSON.stringify(clear));
  const approvals = [];
  let engChecked = false;                      // the placement-card checks below must actually have run
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
      // the engraving screen: three tabs carrying the counts, one pane open, and the queue reachable from the rail (§7)
      const eg = await page.evaluate(async () => {
        CN.setMode('engrave'); Engrave.render();
        await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)));   // the preview is drawn to the box it was given
        const v = document.getElementById('engraveView');
        const tabs = [...v.querySelectorAll('.egTab[data-tab]')].map(b => ({ id: b.dataset.tab, on: b.classList.contains('on'), n: +((b.querySelector('b') || {}).textContent || 0) }));
        const panes = [...v.querySelectorAll('.egPane')].map(p2 => !p2.hasAttribute('hidden'));
        const card = v.querySelector('.rvItem[data-kind=placement]');
        const head = card ? { kind: card.querySelector('.rh .kind').textContent, ttl: card.querySelector('.rh .ttl').textContent, words: (card.querySelector('.pvWords') || {}).textContent || '', confs: card.querySelectorAll('.rh .conf').length } : null;
        const canvas = card ? card.querySelector('.backHost canvas') : null;
        const box = canvas ? canvas.getBoundingClientRect() : null;
        const stage = document.querySelector('.stage');
        const zeroBadges = [...v.querySelectorAll('.egTab[data-tab] b')].filter(x => +x.textContent === 0).length;
        return { tabs, zeroBadges, openPanes: panes.filter(Boolean).length, head, canvasW: box && Math.round(box.width), canvasH: box && Math.round(box.height),
          overflow: stage ? stage.scrollHeight - stage.clientHeight : 0, cardBottom: card ? Math.round(card.getBoundingClientRect().bottom) : null,
          inner: { w: window.innerWidth, h: window.innerHeight } };
      });
      console.log('engraving screen', JSON.stringify(eg));
      assert.strictEqual(eg.tabs.length, 3, 'three tabs');
      assert(eg.zeroBadges === 0, 'an empty queue is not dressed as work: ' + eg.zeroBadges + ' zero badge(s)');
      assert.strictEqual(eg.tabs.filter(t => t.on).length, 1, 'exactly one tab is open');
      assert.strictEqual(eg.openPanes, 1, 'and exactly one pane is shown');
      if (eg.head) {
        assert(/^\d+ of \d+$/.test(eg.head.kind.trim()), 'the card says where you are in the queue: ' + eg.head.kind);
        assert(/^\d{6,}$/.test(eg.head.ttl.trim()), 'the order number is the heading: ' + eg.head.ttl);
        assert(/ANNA/.test(eg.head.words), 'and the words that will be cut are beside the preview: ' + eg.head.words);
        assert.strictEqual(eg.head.confs, 1, 'one confidence score, not several');
        assert(eg.canvasW <= Math.round(eg.inner.w * 0.6) && eg.canvasH <= Math.round(eg.inner.h * 0.7),
          `the back preview fits the window: ${eg.canvasW}x${eg.canvasH} in ${eg.inner.w}x${eg.inner.h}`);
        assert(eg.canvasW >= 180 && eg.canvasH >= 180, `and is drawn large enough to judge: ${eg.canvasW}x${eg.canvasH}`);
        assert(eg.overflow <= 2, `the placement screen is one screen, not a scroll: ${eg.overflow}px over`);
        assert(eg.cardBottom <= eg.inner.h, `and the card ends inside the window: bottom ${eg.cardBottom} of ${eg.inner.h}`);
      }
      // …and it survives a small screen: the preview redraws to the box it is given and nothing runs off the side (§7)
      if (eg.head) {
        for (const [w, h] of [[1180, 720], [900, 640]]) {
          await page.setViewportSize({ width: w, height: h });
          const small = await page.evaluate(async () => {
            Engrave.render();
            await new Promise(r => setTimeout(r, 120));
            await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)));
            const v = document.getElementById('engraveView'), card = v.querySelector('.rvItem[data-kind=placement]');
            const cv = card && card.querySelector('.backHost canvas'), b = cv && cv.getBoundingClientRect();
            const st = document.querySelector('.stage');
            const wide = [...v.querySelectorAll('*')].filter(n2 => n2.getBoundingClientRect().right > window.innerWidth + 1).map(n2 => n2.className);
            return { cw: b && Math.round(b.width), ch: b && Math.round(b.height), sideScroll: st.scrollWidth - st.clientWidth, wide: wide.slice(0, 4) };
          });
          console.log('narrow ' + w + 'x' + h, JSON.stringify(small));
          assert(small.sideScroll <= 2, `nothing runs off the side at ${w}x${h}: ${small.sideScroll}px`);
          assert.deepStrictEqual(small.wide, [], `and nothing is cut off at ${w}x${h}`);
          assert(small.cw >= 150 && small.cw <= w, `the preview redrew to fit ${w}x${h}: ${small.cw}x${small.ch}`);
        }
        await page.setViewportSize({ width: 1500, height: 1000 });
        await page.evaluate(() => Engrave.render());
      }
      // engraving belongs to the Engraving tab: Review neither lists it nor counts it (§11)
      const rv = await page.evaluate(() => {
        CN.setMode('review'); Review.render();
        const v = document.getElementById('reviewView');
        const cards = [...v.querySelectorAll('.rvItem')].map(c2 => c2.dataset.kind);
        return { cards, count: Review.count(), held: Review.items().filter(i2 => String(i2.key).startsWith('eng:')).length,
          chips: [...v.querySelectorAll('.egTab')].map(b2 => b2.textContent.trim()) };
      });
      console.log('review screen', JSON.stringify(rv));
      // ── the ladder: where the work is, on every screen, with the evidence for each step (§10) ──
      const lad = await page.evaluate(() => {
        const out = {};
        for (const m of ['orders', 'engrave', 'review', 'master', 'nest', 'library', 'charms', 'design']) {
          CN.setMode(m);
          const h = document.getElementById('ladder');
          out[m] = { rows: h.querySelectorAll('.ldRow').length, band: (h.querySelector('.ldBand b') || {}).textContent || '' };
        }
        CN.setMode('orders');
        const h = document.getElementById('ladder');
        return { perTab: out,
          steps: [...h.querySelectorAll('.ldRow')].map(r2 => ({ name: r2.querySelector('.n').textContent, count: r2.querySelector('.c').textContent, glyph: r2.querySelector('.g').className.replace('g ', ''), wait: r2.classList.contains('wait'), tab: r2.dataset.tab })),
          band: (h.querySelector('.ldBand b') || {}).textContent || '',
          chips: [...h.querySelectorAll('.ldChip')].map(c2 => c2.textContent.trim()),
          strip: !!document.getElementById('liveStrip'),
          recent: !!document.getElementById('railRecent'),
        };
      });
      console.log('ladder', JSON.stringify(lad));
      assert(Object.values(lad.perTab).every(t => t.rows >= 7), 'the ladder is on every tab: ' + JSON.stringify(lad.perTab));
      assert(Object.values(lad.perTab).every(t => t.band === lad.band), 'and says the same thing on all of them');
      assert.deepStrictEqual(lad.steps.map(s2 => s2.name), ['Pull', 'Pool', 'Nest', 'Check', 'Engrave', 'Labels', 'Commit'], 'seven steps a person would name: ' + lad.steps.map(s2 => s2.name));
      assert(lad.steps.some(s2 => s2.count), 'with the evidence for them: ' + JSON.stringify(lad.steps.map(s2 => s2.count)));
      assert(lad.steps.filter(s2 => s2.glyph === 'now' || s2.wait).length >= 1, 'and the step the run is standing on is marked: ' + JSON.stringify(lad.steps));
      assert(lad.steps.every(s2 => s2.tab), 'every step goes to the screen that settles it');
      assert(!lad.strip, 'the scrolling ticker is gone');
      assert(lad.recent, 'and what happened lately is one click away in the rail');
      assert(lad.band && !/^run [0-9a-z]+$/i.test(lad.band), 'the band says a state word, not a run id: ' + lad.band);
      // ── the run banner's buttons go where they say, and the bench cannot be pulled out from under a run (§10) ──
      const flow = await page.evaluate(() => {
        CN.setMode('orders'); RunCtl.renderBanner();
        const before = { mode: CN.S.settings.runMode, tab: CN.S.mode };
        const rb = document.getElementById('rbReview'); if (rb) rb.click();
        const afterReview = { mode: CN.S.settings.runMode, tab: CN.S.mode };
        CN.setMode('orders');
        const eb = document.getElementById('rbEngrave'); if (eb) eb.click();
        const afterEngrave = { mode: CN.S.settings.runMode, tab: CN.S.mode };
        CN.setMode('orders');
        const bench = ['btnNestAll', 'btnClearAll', 'btnPick'].map(id => { const b = document.getElementById(id); return { id, off: !!(b && b.disabled) }; });
        return { before, afterReview, afterEngrave, bench, hadReview: !!rb, hadEngrave: !!eb };
      });
      console.log('run banner buttons', JSON.stringify(flow));
      if (flow.hadReview) {
        assert.strictEqual(flow.afterReview.tab, 'review', 'Review (N) opens the Review tab');
        assert.strictEqual(flow.afterReview.mode, flow.before.mode, 'and does not change Auto/Manual: ' + flow.afterReview.mode);
      }
      if (flow.hadEngrave) {
        assert.strictEqual(flow.afterEngrave.tab, 'engrave', 'Engraving (N) opens the Engraving tab');
        assert.strictEqual(flow.afterEngrave.mode, flow.before.mode, 'and does not change Auto/Manual either');
      }
      assert(flow.bench.every(b => b.off), 'the bench buttons cannot take the cards out from under a run: ' + JSON.stringify(flow.bench));
      await page.evaluate(() => CN.setMode('engrave'));
      // the step that needs a person is a button that goes there
      const jumped = await page.evaluate(() => {
        const w = document.querySelector('#ladder .ldRow.wait') || document.querySelector('#ladder .ldRow[data-tab=engrave]');
        if (!w) return null;
        const want = w.dataset.tab; w.click();
        return { want, got: CN.S.mode };
      });
      if (jumped) { console.log('ladder jump', JSON.stringify(jumped)); assert.strictEqual(jumped.got, jumped.want, 'the ladder row goes to its own screen'); }
      await page.evaluate(() => CN.setMode('engrave'));
      // the Master tab says, once, what the orders want that the library has never heard of (§6)
      const miss = await page.evaluate(() => {
        CN.setMode('master'); Master.render();
        const b = document.getElementById('mMissing');
        if (!b || b.classList.contains('hidden')) return { shown: false };
        const shut = { head: b.querySelector('.t b').textContent, skus: [...b.querySelectorAll('.s')].length, acts: [...b.querySelectorAll('[data-a]')].map(x => x.dataset.a) };
        b.querySelector('[data-a=see]').click();                              // it opens only when asked
        const b2 = document.getElementById('mMissing');
        const open = { skus: [...b2.querySelectorAll('.s')].map(x => x.textContent), acts: [...b2.querySelectorAll('[data-a]')].map(x => x.dataset.a) };
        b2.querySelector('[data-a=shut]').click();                            // and it closes, and stays closed
        const gone = document.getElementById('mMissing').classList.contains('hidden');
        B.missShut = false; B.missOpen = false; Master.render();
        return { shown: true, shut, open, gone };
      });
      console.log('missing skus', JSON.stringify(miss));
      assert(miss.shown, 'an unmatched SKU is named on the Master tab, not only in Review');
      assert(/1 SKU the orders want/.test(miss.shut.head), 'counted once per SKU: ' + miss.shut.head);
      assert.strictEqual(miss.shut.skus, 0, 'it opens as one line, not a wall of chips');
      assert.deepStrictEqual(miss.shut.acts, ['see', 'shut'], 'and that line can be opened or closed: ' + miss.shut.acts.join(','));
      assert(miss.open.skus.some(x => /BR-NOPE-99/.test(x)), 'opened, it names them: ' + miss.open.skus.join(','));
      assert.deepStrictEqual(miss.open.acts, ['see', 'shut', 'copy', 'save', 'add'], 'with a way to take the list to the master files');
      assert(miss.gone, 'the close button closes it');
      // and the tab always offers the thing it exists for, whether or not anything is missing
      assert(await page.evaluate(() => !!document.getElementById('mAdd')), 'the Master tab has a visible way to add a master file');
      /* Every run that ever ran, and the way back into one. Until this existed the only run reachable was the one in
         front of you: yesterday's set and the order that shipped on Tuesday had no door at all. */
      const hist = await page.evaluate(async () => {
        RunHistory.show(); await new Promise(r => setTimeout(r, 900));
        const d = document.getElementById('histDlg');
        const runs = [...d.querySelectorAll('.hRun')].map(n => ({ id: n.dataset.run, text: n.querySelector('.hRow').textContent.replace(/\s+/g, ' ').trim(), acts: [...n.querySelectorAll('.hRow [data-a]')].map(b => b.dataset.a) }));
        const foot = d.querySelector('#hFoot').textContent;
        // and it is searchable: by order number, by SKU, by the words that were engraved
        const q = d.querySelector('#hQ'); q.value = '3521000002'; q.dispatchEvent(new Event('input'));
        await new Promise(r => setTimeout(r, 900));
        const found = [...d.querySelectorAll('.hRun')].map(n => n.dataset.run);
        const hits = [...d.querySelectorAll('.hHits')].map(n => n.textContent.trim());
        q.value = 'no-such-order-anywhere'; q.dispatchEvent(new Event('input'));
        await new Promise(r => setTimeout(r, 900));
        const none = d.querySelector('.hEmpty') ? d.querySelector('.hEmpty').textContent.trim() : '';
        d.close();
        // a closed dialog must actually be gone: CSS that lays one out unconditionally leaves it on screen forever
        const stillVisible = d.getBoundingClientRect().height > 0 || getComputedStyle(d).display !== 'none';
        return { open: !!runs.length, runs, foot, found, hits, none, stillVisible };
      });
      console.log('history', JSON.stringify(hist));
      assert(hist.open, 'the history lists the runs on record');
      assert(hist.runs.some(r => /line/.test(r.text) && /Set|no set/.test(r.text)), 'each run says which set, which day and how big: ' + JSON.stringify(hist.runs[0]));
      assert(hist.runs.every(r => r.acts.includes('lines')), 'and opens its orders');
      assert(/looked at the \d+ most recent runs/.test(hist.foot), 'it says how far it looked: ' + hist.foot);
      assert(hist.found.length >= 1 && hist.hits.some(h => /3521000002/.test(h)), 'searching by order number finds the run that carried it: ' + JSON.stringify(hist));
      assert(/nothing matches/.test(hist.none), 'and a search with no answer says so: ' + hist.none);
      assert(!hist.stillVisible, 'and closing it puts it away');
      await page.evaluate(() => { CN.setMode('review'); Review.render(); });
      // CN_SHOTS=<dir> captures every screen at two widths and the order window — the evidence a design review runs on
      if (process.env.CN_SHOTS) {
        const SH = process.env.CN_SHOTS;
        await page.evaluate(() => { document.querySelectorAll('dialog[open]').forEach(d => d.close()); });
        const grab = async (name, w, h) => { await page.setViewportSize({ width: w, height: h }); await page.waitForTimeout(450); await page.screenshot({ path: SH + '/' + name + '.png' }); };
        for (const [mode, fn] of [['orders', 'Orders'], ['engrave', 'Engrave'], ['review', 'Review'], ['master', 'Master'], ['design', null], ['nest', null], ['library', null], ['charms', null]]) {
          await page.evaluate(m => { CN.setMode(m); }, mode);
          if (fn) await page.evaluate(f => { try { window[f].render(); } catch (_) {} }, fn);
          await grab('tab-' + mode + '-1500', 1500, 1000);
          await grab('tab-' + mode + '-1180', 1180, 760);
        }
        await page.evaluate(() => RunHistory.show('')); await page.waitForTimeout(900);
        await grab('win-history-1500', 1500, 1000);
        await page.evaluate(() => { const d = document.getElementById('histDlg'); if (d) d.close(); });
        await page.setViewportSize({ width: 1500, height: 1000 });
        await page.evaluate(() => { CN.setMode('orders'); Orders.render(); });
        await page.waitForTimeout(400);
        await page.evaluate(() => { const b = document.querySelector('#ordItems .ocard'); if (b) b.click(); });
        await page.waitForTimeout(800);
        await page.screenshot({ path: SH + '/win-order-1500.png' });
        await grab('win-order-1180', 1180, 760);
        await page.evaluate(() => { const d = document.getElementById('orderWin'); if (d && d.open) d.close(); });
        await page.setViewportSize({ width: 1500, height: 1000 });
        await page.evaluate(() => { CN.setMode('orders'); Orders.render(); const lb = document.querySelector('[data-view=list]'); if (lb) lb.click(); });
        await page.waitForTimeout(400);
        await page.screenshot({ path: SH + '/orders-list-1500.png' });
        await page.evaluate(() => { const cb = document.querySelector('[data-view=cards]'); if (cb) cb.click(); CN.setMode('engrave'); Engrave.render(); });
        await page.waitForTimeout(400);
      }
      // ── the Orders tab: a card for every line, the same line as a row, the filters, and the order window (§5) ──
      const ord = await page.evaluate(async () => {
        CN.setMode('orders'); Orders.render();
        await new Promise(r => setTimeout(r, 250));
        const v = document.getElementById('ordersView');
        const chips = [...v.querySelectorAll('[data-pile]')].map(b => ({ id: b.dataset.pile, n: +b.querySelector('b').textContent, on: b.classList.contains('on') }));
        const cards = [...v.querySelectorAll('.ocard')].map(c2 => ({
          num: c2.querySelector('.onum').textContent, sku: c2.querySelector('.osku b').textContent,
          state: c2.querySelector('.ost').textContent, metal: c2.querySelector('.ometal span').textContent,
          words: (c2.querySelector('.opers') || {}).textContent || '', why: (c2.querySelector('.owhy') || {}).textContent || '',
          qty: (c2.querySelector('.oimg .qty') || {}).textContent || '', flag: !!c2.querySelector('.oimg .flag'),
        }));
        const stage = document.querySelector('.stage');
        return { chips, cards, sideScroll: stage.scrollWidth - stage.clientWidth, view: 'cards' };
      });
      console.log('orders cards', JSON.stringify({ chips: ord.chips, n: ord.cards.length, first: ord.cards[0], sideScroll: ord.sideScroll }));
      assert(ord.cards.length >= 3, 'every pulled line is a card: ' + ord.cards.length);
      assert(ord.cards.every(c2 => /^\d{6,}$/.test(c2.num.trim()) && c2.sku && c2.state && c2.metal), 'each card carries the order, the SKU, the state and the metal: ' + JSON.stringify(ord.cards[0]));
      assert(ord.cards.some(c2 => c2.flag && c2.why), 'a line that needs a person says so on its own card');
      assert(ord.cards.some(c2 => c2.words), 'and the words the customer typed are on the card');
      assert(ord.chips.some(c2 => c2.id === 'attn' && c2.n >= 1), 'the piles are filter chips with counts: ' + JSON.stringify(ord.chips));
      assert(ord.sideScroll <= 2, 'the cards do not run off the side: ' + ord.sideScroll);
      // the filter shows one pile and nothing else
      const filtered = await page.evaluate(() => {
        document.querySelector('[data-pile=attn]').click();
        const v = document.getElementById('ordersView');
        return { n: v.querySelectorAll('.ocard').length, allFlagged: [...v.querySelectorAll('.ocard')].every(c2 => c2.classList.contains('attn')) };
      });
      assert(filtered.n >= 1 && filtered.allFlagged, 'the filter leaves only that pile: ' + JSON.stringify(filtered));
      // the list carries the same fields, with a smaller picture
      const asList = await page.evaluate(() => {
        document.querySelector('[data-pile=""]').click();
        document.querySelector('[data-view=list]').click();
        const v = document.getElementById('ordersView');
        const hdr = [...v.querySelectorAll('.olist.hdr span, .olist.hdr button')].map(x => x.textContent.replace(/[\u25b4\u25be]/g, '').trim()).filter(Boolean);
        const rows = [...v.querySelectorAll('.olist:not(.hdr)')].map(r2 => ({
          num: r2.querySelector('.onum').textContent, sku: r2.querySelector('.osku b').textContent,
          state: r2.querySelector('.ost').textContent, qty: r2.querySelector('.qtyc').textContent, thumb: !!r2.querySelector('img.th'),
        }));
        const stage = document.querySelector('.stage');
        return { rows, hdr, cards: v.querySelectorAll('.ocard').length, sideScroll: stage.scrollWidth - stage.clientWidth };
      });
      console.log('orders list', JSON.stringify({ n: asList.rows.length, first: asList.rows[0], sideScroll: asList.sideScroll }));
      // 411 lines is a real pull: the tab must draw them quickly and must not ask Etsy for 411 photographs (§5)
      const scale = await page.evaluate(async () => {
        const seed = Orders.rows().slice();
        const many = [];
        for (let i = 0; i < 120; i++) for (const r of seed) {
          many.push(Object.assign(Object.create(Object.getPrototypeOf(r)), r, {
            key: r.key + '_x' + i,
            order: Object.assign({}, r.order, { receiptId: String(+r.order.receiptId + i * 10) }),
            line: Object.assign({}, r.line, { listingId: String(1000000 + i * 7) }),
          }));
        }
        const real = B.orders.rows;
        B.orders.rows = many;
        const cb = document.querySelector('[data-view=cards]'); if (cb) cb.click();
        const t0 = performance.now();
        Orders.render();
        const drawn = performance.now() - t0;
        await new Promise(r => setTimeout(r, 350));
        const cards = document.querySelectorAll('#ordBody .ocard').length;
        const asked = window.__imgAsked ? window.__imgAsked() : null;
        const stage = document.querySelector('.stage');
        const out = { cards, drawn: Math.round(drawn), sideScroll: stage.scrollWidth - stage.clientWidth, listings: new Set(many.map(r => r.line.listingId)).size };
        B.orders.rows = real; Orders.render();
        return out;
      });
      console.log('orders at scale', JSON.stringify(scale));
      assert.strictEqual(scale.cards, 480, 'every line of a big pull is drawn: ' + scale.cards);
      assert(scale.drawn < 2500, 'and drawn without a stall: ' + scale.drawn + ' ms');
      assert(scale.sideScroll <= 2, 'with nothing running off the side at scale: ' + scale.sideScroll);
      // a run writing to the tab must not take the scroller, the caret or the search text with it (§5)
      const steady = await page.evaluate(async () => {
        const seed = Orders.rows().slice(); const many = [];
        for (let i = 0; i < 60; i++) for (const r of seed) many.push(Object.assign({}, r, { key: r.key + '_s' + i, order: Object.assign({}, r.order, { receiptId: String(+r.order.receiptId + i * 10) }) }));
        const real = B.orders.rows; B.orders.rows = many;
        Orders.render();
        await new Promise(r => setTimeout(r, 200));
        const body = document.getElementById('ordBody'), q = document.getElementById('ordQ');
        body.scrollTop = 320; q.focus(); q.value = '35210'; q.setSelectionRange(3, 3);
        const before = { body: body.scrollTop, node: body, qNode: q, caret: q.selectionStart, val: q.value };
        // the kind of repaint a run fires every few hundred milliseconds
        for (let i = 0; i < 6; i++) { RunCtl.renderBanner(); Orders.render(); }
        await new Promise(r => setTimeout(r, 150));
        const body2 = document.getElementById('ordBody'), q2 = document.getElementById('ordQ');
        const out = { sameBody: body2 === before.node, sameInput: q2 === before.qNode, scroll: body2.scrollTop, wanted: before.body,
          focused: document.activeElement === q2, caret: q2.selectionStart, val: q2.value };
        B.orders.rows = real; Orders.render();
        return out;
      });
      console.log('orders steady under a run', JSON.stringify(steady));
      assert(steady.sameBody, 'the scroller is not rebuilt by a run repaint');
      assert(steady.sameInput, 'and neither is the search box');
      assert(Math.abs(steady.scroll - steady.wanted) <= 2, `the scroll position survives: ${steady.scroll} of ${steady.wanted}`);
      assert(steady.focused && steady.caret === 3, 'and so do the focus and the caret: ' + JSON.stringify(steady));
      assert.strictEqual(steady.val, '35210', 'and what was typed');
      assert.strictEqual(asList.cards, 0, 'the list view replaces the cards');
      assert.strictEqual(asList.rows.length, ord.cards.length, 'and holds exactly the same lines');
      assert(asList.rows.every(r2 => r2.num && r2.sku && r2.state && r2.qty && r2.thumb), 'each row carries the same fields and a thumbnail: ' + JSON.stringify(asList.rows[0]));
      assert(asList.sideScroll <= 2, 'and does not run off the side: ' + asList.sideScroll);
      assert.deepStrictEqual(asList.hdr, ['Order', 'SKU', 'Item', 'Qty', 'Metal', 'Ship by', 'State'], 'the columns say what they are: ' + asList.hdr.join(','));
      // the order window: everything about one line, and the way to settle it
      const win = await page.evaluate(async () => {
        document.querySelector('[data-view=cards]').click();
        const held = [...document.querySelectorAll('.ocard')].find(c2 => c2.classList.contains('attn')) || document.querySelector('.ocard');
        held.click();
        await new Promise(r => setTimeout(r, 400));
        const d = document.getElementById('orderWin');
        const meta = [...d.querySelectorAll('#owMeta .m i')].map(i => i.textContent);
        return { open: d.open, title: document.getElementById('owTitle').textContent, sku: document.getElementById('owSku').textContent,
          metal: document.getElementById('owMetal').textContent, meta, fix: !!d.querySelector('#owFix .rvItem'),
          note: !!document.getElementById('owNote'), thread: !!document.getElementById('owThread'),
          composer: !!document.getElementById('owInput'), attach: !!document.getElementById('owAttach'),
          skip: document.getElementById('owSkip').getAttribute('aria-checked'), photo: !!d.querySelector('#owPhoto'),
          box: (() => { const r2 = d.getBoundingClientRect(); return { w: Math.round(r2.width), h: Math.round(r2.height), inW: window.innerWidth, inH: window.innerHeight }; })() };
      });
      console.log('order window', JSON.stringify(win));
      assert(win.open, 'clicking a card opens the order window');
      assert(/^Order \d{6,}$/.test(win.title.trim()), 'headed by the order: ' + win.title);
      assert(/^SKU: /.test(win.sku) && win.metal, 'with the SKU and the metal');
      assert(['Quantity', 'Metal', 'State', 'Ship by', 'Listing', 'Title'].every(k => win.meta.includes(k)), 'and the details: ' + win.meta.join(','));
      assert(win.note && win.thread && win.composer && win.attach && win.photo, 'the staff note, the thread, the composer and the picture are all there');
      assert(win.fix, 'and the decision the line is waiting on is answered from inside the window');
      assert(win.box.w <= win.box.inW && win.box.h <= win.box.inH, `the window fits the screen: ${win.box.w}x${win.box.h} in ${win.box.inW}x${win.box.inH}`);
      // a staff note typed here reaches the station
      const noted = await page.evaluate(async () => {
        const n = document.getElementById('owNote'); n.value = 'checked by the sorter'; n.dispatchEvent(new Event('input'));
        n.dispatchEvent(new Event('blur'));
        await new Promise(r => setTimeout(r, 700));
        return true;
      });
      void noted;
      const noteSeen = await frame.evaluate(() => DesignStation.bridge.cursor.log.filter(l => /Staff note on/.test(l.caption)).length);
      assert(noteSeen >= 1, 'the staff note was written through the station');
      // a message typed here reaches the shared thread
      await page.evaluate(async () => {
        const i = document.getElementById('owInput'); i.value = 'sorter says hello'; i.dispatchEvent(new Event('input'));
        document.getElementById('owSend').click();
        await new Promise(r => setTimeout(r, 900));
      });
      const chatSeen = await frame.evaluate(() => DesignStation.bridge.cursor.log.filter(l => /Post a chat message/.test(l.caption)).length);
      assert(chatSeen >= 1, 'and the message was posted through the station');
      await page.evaluate(() => { const d = document.getElementById('orderWin'); if (d && d.open) d.close(); });
      const ENG = ['engraveWords', 'placement', 'flipFailed', 'notRepresentable', 'fontMissing'];
      assert(!rv.cards.some(k => ENG.includes(k)), 'no engraving card is listed in Review: ' + rv.cards);
      assert.strictEqual(rv.count, rv.cards.length, 'and the count is what the tab shows');
      assert(rv.chips.length >= 2, 'the kinds present are filter chips: ' + JSON.stringify(rv.chips));
      await page.evaluate(() => { CN.setMode('engrave'); Engrave.render(); });
      const switched = await page.evaluate(() => {
        const v = document.getElementById('engraveView');
        v.querySelector('.egTab[data-tab=words]').click();   // an empty tab still opens when it is asked for
        const v2 = document.getElementById('engraveView');
        return { on: [...v2.querySelectorAll('.egTab')].filter(b => b.classList.contains('on')).map(b => b.dataset.tab), panes: [...v2.querySelectorAll('.egPane')].map(p2 => !p2.hasAttribute('hidden')).filter(Boolean).length };
      });
      assert.deepStrictEqual(switched.on, ['words'], 'a tab switches the screen');
      assert.strictEqual(switched.panes, 1, 'and still one pane at a time');
      await page.evaluate(() => { const v = document.getElementById('engraveView'); const b = v.querySelector('.egTab[data-tab=place]'); if (b) b.click(); });

      // ── moving the lettering must not resize it, and the slider must survive being dragged (§7.4) ──
      const hasCard = !engChecked && await page.evaluate(async () => {
        const j = [...Engrave.items().values()].find(x => x.state === 'review'); if (!j) return false;
        EG_FORCE: { CN.setMode('engrave'); const v = document.getElementById('engraveView'); Engrave.render();
          const b = v.querySelector('.egTab[data-tab=place]'); if (b) b.click(); }
        await new Promise(r => setTimeout(r, 250));
        return !!document.querySelector('#egQueue .rvItem[data-kind=placement]');
      });
      if (hasCard) {
        engChecked = true;
        const moved = await page.evaluate(async () => {
          const j = [...Engrave.items().values()].find(x => x.state === 'review'); if (!j) return null;
          const before = { size: j.fit.size, max: j.fit.fittedMax, centre: j.fit.centre.slice() };
          // set a size well under the ceiling, the way a person would with the slider
          Engrave.resize(j, before.max * 0.62);
          const chosen = j.fit.size;
          Engrave.nudge(j, -0.25, 0);
          const afterNudge = { size: j.fit.size, max: j.fit.fittedMax, centre: j.fit.centre.slice() };
          Engrave.nudge(j, 0, 0.25);
          const afterTwo = { size: j.fit.size, max: j.fit.fittedMax };
          return { before, chosen, afterNudge, afterTwo };
        });
        if (moved) {
          console.log('nudge keeps the size', JSON.stringify(moved));
          assert(moved.chosen < moved.before.max * 0.7, 'the size was brought down first: ' + moved.chosen);
          assert(Math.abs(moved.afterNudge.size - moved.chosen) < 0.06, `a nudge keeps the size a person chose: ${moved.chosen} → ${moved.afterNudge.size}`);
          assert(Math.abs(moved.afterTwo.size - moved.chosen) < 0.06, `and so does the next one: ${moved.afterTwo.size}`);
          assert(moved.afterNudge.max > moved.chosen + 0.1, 'the slider keeps room to grow back: max ' + moved.afterNudge.max);
          assert(moved.afterNudge.centre[0] !== moved.before.centre[0], 'and the text actually moved');
        }
        // the slider is still in the DOM after an input, so one grab is not one step
        const slider = await page.evaluate(async () => {
          const v = document.getElementById('engraveView');
          const s2 = v.querySelector('input[data-a=resize]'); if (!s2) return null;
          s2.focus();
          const started = s2.value, alive = [];
          for (let i = 0; i < 4; i++) {
            s2.value = String(+s2.value - 0.1);
            s2.dispatchEvent(new Event('input', { bubbles: true }));
            await new Promise(r => setTimeout(r, 30));
            alive.push(document.contains(s2) && document.activeElement === s2);
          }
          return { started, ended: s2.value, alive };
        });
        if (slider) {
          console.log('slider survives', JSON.stringify(slider));
          assert(slider.alive.every(Boolean), 'the slider is not destroyed under the pointer: ' + JSON.stringify(slider.alive));
          assert(+slider.ended < +slider.started, 'and every step of the drag lands');
        }
        // a held key does not run the queue
        const held = await page.evaluate(() => {
          const card = document.querySelector('#egQueue .rvItem[data-kind=placement]'); if (!card) return null;
          const n0 = [...Engrave.items().values()].filter(x => x.state === 'review').length;
          card.dispatchEvent(new KeyboardEvent('keydown', { key: 'a', repeat: true, bubbles: true }));
          return { n0, n1: [...Engrave.items().values()].filter(x => x.state === 'review').length };
        });
        if (held) { console.log('key repeat', JSON.stringify(held)); assert.strictEqual(held.n1, held.n0, 'a repeated key press is ignored'); }
      }
      // the run banner names engraving as well as review when it is waiting on a person
      const banner = await page.evaluate(() => {
        RunCtl.renderBanner();
        const h = document.getElementById('runBanner');
        return { text: h.textContent.replace(/\s+/g, ' ').trim(), btns: [...h.querySelectorAll('button')].map(b => b.id) };
      });
      console.log('banner', JSON.stringify(banner));
      if (/Waiting for a person/.test(banner.text)) {
        assert(!/\b0 item/.test(banner.text), 'the banner never says it is waiting for nothing: ' + banner.text);
        assert(/in Review|in Engraving/.test(banner.text), 'it says what it is waiting for: ' + banner.text);
        assert(banner.btns.some(b => b === 'rbReview' || b === 'rbEngrave'), 'and offers the way there: ' + banner.btns.join(','));
      }
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
  assert(engChecked, 'the placement card was on screen and its checks ran');
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

  // a bar that says what the app is doing never stands over the way out, and the way out always works (§5)
  {
    const clear = await page.evaluate(() => {
      const t = CNProgress.start('Preparing 411 order line(s)', { total: 411 }); t.set(229, 411, 'BLUE_94532');
      const row = document.querySelector('.cnp .cnpRow').getBoundingClientRect();
      const hits = ['#modeSeg', '#runBanner', '#topBar', '.topBar'].map(sel => { const el = document.querySelector(sel); if (!el) return null; const r = el.getBoundingClientRect(); if (!r.width) return null; return { sel, over: r.left < row.right && r.right > row.left && r.top < row.bottom && r.bottom > row.top }; }).filter(Boolean);
      const unreachable = [...document.querySelectorAll('#modeSeg button')].filter(seg => {
        seg.scrollIntoView({ inline: 'nearest', block: 'nearest' });
        const r2 = seg.getBoundingClientRect();
        const atPoint = document.elementFromPoint(r2.left + r2.width / 2, r2.top + r2.height / 2);
        return !(atPoint && seg.contains(atPoint));
      }).map(b => b.dataset.mode);
      t.end();
      return { hits, reachable: !unreachable.length, unreachable };
    });
    console.log('progress bar clearance', JSON.stringify(clear));
    assert(clear.hits.every(h => !h.over), 'the bar covers no part of the chrome: ' + JSON.stringify(clear.hits));
    assert(clear.reachable, 'every tab is where the pointer can reach it: ' + JSON.stringify(clear.unreachable));
  }

  // leaving a tab while work is running neither stops the work nor reloads the station
  {
    const before = await page.evaluate(() => ({ hellos: B.link ? B.link.hellos : null, mode: CN.S.mode }));
    const kept = await page.evaluate(async () => {
      const t = CNProgress.start('long job', { total: 10 }); t.set(3, 10);
      const seen = [];
      for (const m of ['orders', 'nest', 'master', 'review', 'design']) { CN.setMode(m); seen.push(CN.S.mode); await new Promise(r => setTimeout(r, 60)); }
      const stillThere = !!document.querySelector('.cnp .cnpRow');
      t.end();
      return { seen, stillThere, frames: document.querySelectorAll('#dsFrame').length };
    });
    console.log('moving about while busy', JSON.stringify(kept));
    assert.deepStrictEqual(kept.seen, ['orders', 'nest', 'master', 'review', 'design'], 'every tab opens while work is running');
    assert(kept.stillThere, 'and the work carries on with its bar still up');
    assert.strictEqual(kept.frames, 1, 'the station is the same frame throughout, never reloaded');
    void before;
  }

  // a manual run says why it is waiting and offers to carry on by itself (§7)
  {
    const paused = await page.evaluate(() => {
      const r = { runId: 'run-test', day: '2026-09-18', setId: null, step: 'claim', status: 'paused', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [], committed: [] };
      B.run = r; RunCtl.renderBanner();
      const el = document.querySelector('#runBanner') || document.body;
      return { text: el.textContent.replace(/\s+/g, ' ').trim().slice(0, 200), next: !!document.querySelector('#rbNext'), auto: !!document.querySelector('#rbAuto') };
    });
    console.log('paused banner', JSON.stringify(paused));
    assert(paused.next && paused.auto, 'a paused run offers both the next step and running on: ' + JSON.stringify(paused));
    assert(/Manual/i.test(paused.text), 'and says why it is waiting: ' + paused.text);
    const after = await page.evaluate(() => {                      // the button's own effect, without driving a stub run through the loop
      const real = RunCtl.next; RunCtl.next = () => {};
      document.querySelector('#rbAuto').click();
      return new Promise(r => setTimeout(() => { RunCtl.next = real; r(B.run && B.run.mode); }, 300));
    });
    assert.strictEqual(after, 'auto', 'pressing it sets the run to carry on by itself');
    await page.evaluate(() => { B.run = null; RunCtl.renderBanner(); });
  }

  // the panel folds away and the way back is always on screen, never under the station's own buttons
  {
    const fold = await page.evaluate(() => {
      CN.setMode('design'); Views.designHost();
      const v = document.getElementById('designView'), b = v.querySelector('.dsFold');
      const box = () => { const r = b.getBoundingClientRect(); const f = document.querySelector('.dsFrameHost').getBoundingClientRect(); return { w: r.width, h: r.height, overFrame: r.left < f.right - 1 && r.right > f.left + 1 && r.top < f.bottom - 1 && r.bottom > f.top + 1 }; };
      const open0 = !v.classList.contains('dsWide'); if (!open0) b.click();
      const asOpen = box();
      b.click(); const wide = v.classList.contains('dsWide'); const asFolded = box();
      b.click(); const backOpen = !v.classList.contains('dsWide');
      return { asOpen, wide, asFolded, backOpen, label: b.textContent };
    });
    console.log('fold', JSON.stringify(fold));
    assert(fold.wide && fold.backOpen, 'the panel folds and comes back');
    assert(fold.asFolded.w > 0 && fold.asFolded.h > 0, 'the way back is still on screen when folded: ' + JSON.stringify(fold.asFolded));
    assert(!fold.asOpen.overFrame && !fold.asFolded.overFrame, 'and it never sits on top of the station: ' + JSON.stringify(fold));
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
