// The back engraving card's Approve engraving, driven for real (Paul, 5 Oct 2026, 17:47 UTC: "quick link buttons to the engraving panel to fix it and
// also to approve engraving"). order-engraving-states.cjs (EN2) draws every state of the card but STUBS Engrave.approve; an approval is permanent (a BACK
// ENGRAVING seal is written), so this test does not: it makes the fake site (bridge-server.cjs) run a whole Auto set for a handful of personalised orders
// (the real master file, nesting, the real fitting, the real review) and then presses the card's buttons, with nothing replaced:
//   the name asked for in the small bar inside the window (never a browser pop-up on the order window) · exactly one approval and one server write (double
//   click, Enter then click, a repaint during the write) · the permanent seal on the card, the Timeline, the Engraving panel's Decided row, the Sheet tab and
//   the Sheet window · the counts agree · a reload keeps it · the data written is what the Engraving panel's own Approve writes (field by field) ·
//   Approve disabled and impossible while the words are unconfirmed or the back is being prepared · one piece of an order never approves another ·
//   a failed write: a plain message IN the card (a toast would sit under the modal order window), no seal before the checkpoint, no second seal on retry ·
//   Fix / View in Engraving land on the right order and piece from the Overview, the Sheet tab and the Sheet window · the sandbox's card approves only
//   the sandbox's records · seals are never removed or replaced · the card at 280, 390 and 700 px · Paul's words.
// Headless Chromium against the local fake site; nothing live is touched, no paid model is called (the fake model answers), no Etsy call.
//   SHOTS=<dir> node tests/charm-nest/order-engraving-approve-real.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, ONLY=<phase,phase>, SKIP_SANDBOX=1)
const fs = require('fs'), path = require('path'), os = require('os');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const { buildMaster } = require('./fixture-master.cjs');

const day = Math.floor(Date.now() / 1000);
const tx = (rid, i, sku, extra = {}) => Object.assign({ transaction_id: Number(`${rid}${i}`), listing_id: 1718000 + i, receipt_id: rid, sku, title: `${sku} charm`, quantity: 1, expected_ship_date: day + 86400, variations: [{ formatted_name: 'Metal', formatted_value: '14k Gold Filled' }], is_personalized: false }, extra);
const pers = text => ({ is_personalized: true, variations: [{ formatted_name: 'Metal', formatted_value: 'Sterling Silver' }, { formatted_name: 'Personalization', formatted_value: text }] });
const receipt = (rid, txs) => ({ receipt_id: rid, order_number: rid, name: 'Buyer ' + rid, country_iso: 'US', city: 'Austin', message_from_buyer: '', update_timestamp: day, create_timestamp: day - 3600, status: 'Paid', is_shipped: false, transactions: txs });
// O1 three pieces (two to engrave, one plain) · O2 one piece, the card's happy path · O3 words that need a decision · O4 the Engraving panel's own Approve
// (the reference: the same charm and the same words as O2, so what it writes can be compared field by field) · O5 approved from the Sheet window ·
// O6 a checkpoint that fails · O7 the cloud refusing the back file (500) · O8 no network at all · O9 the Sheet tab · O10 the stale card and the words
// being prepared · O11 a 409 from the cloud · O12 the Sandbox's own card
const R = { O1: '3521100001', O2: '3521100002', O3: '3521100003', O4: '3521100004', O5: '3521100005', O6: '3521100006', O7: '3521100007', O8: '3521100008', O9: '3521100009', O10: '3521100010', O11: '3521100011', O12: '3521100012' };
const receipts = [
  receipt(R.O1, [tx(R.O1, 1, 'BR-TST-01', pers('ANNA')), tx(R.O1, 2, 'BR-TST-02', pers('BELLA')), tx(R.O1, 3, 'BR-TST-03')]),
  receipt(R.O2, [tx(R.O2, 1, 'BR-TST-04', pers('CARLA'))]),
  receipt(R.O3, [tx(R.O3, 1, 'BR-TST-01', pers('WHICH ONE?'))]),
  receipt(R.O4, [tx(R.O4, 1, 'BR-TST-04', pers('CARLA'))]),
  receipt(R.O5, [tx(R.O5, 1, 'BR-TST-03', pers('EVA'))]),
  receipt(R.O6, [tx(R.O6, 1, 'BR-TST-04', pers('FIONA'))]),
  receipt(R.O7, [tx(R.O7, 1, 'BR-TST-01', pers('GRETA'))]),
  receipt(R.O8, [tx(R.O8, 1, 'BR-TST-02', pers('HOPE'))]),
  receipt(R.O9, [tx(R.O9, 1, 'BR-TST-03', pers('IRIS'))]),
  receipt(R.O10, [tx(R.O10, 1, 'BR-TST-04', pers('JUNE'))]),
  receipt(R.O11, [tx(R.O11, 1, 'BR-TST-01', pers('KATE'))]),
  receipt(R.O12, [tx(R.O12, 1, 'BR-TST-03', pers('LILY'))])
];
const NJOBS = Object.keys(R).length + 1, NROWS = receipts.reduce((n, r) => n + r.transactions.length, 0);   // (a job for each piece with words: the first order has two)
const K = { A: R.O1 + '_' + R.O1 + '1', B: R.O1 + '_' + R.O1 + '2', P: R.O1 + '_' + R.O1 + '3' };
for (const [n, rid] of Object.entries(R)) if (n !== 'O1') K[n] = rid + '_' + rid + '1';
const poolOf = k => k + '_1';
const WORDS = { [K.A]: 'ANNA', [K.B]: 'BELLA', [K.O2]: 'CARLA', [K.O4]: 'CARLA', [K.O5]: 'EVA', [K.O6]: 'FIONA', [K.O7]: 'GRETA', [K.O8]: 'HOPE', [K.O9]: 'IRIS', [K.O10]: 'JUNE', [K.O11]: 'KATE', [K.O12]: 'LILY' };
const agentResults = { engraveIntent: o => {
  const txt = String(o.messages[0].content[0].text), m = /Personalisation field: (\[.*?\])/.exec(txt); let p = []; try { p = JSON.parse(m ? m[1] : '[]'); } catch (_) {}
  const req = { side: 'back', font: null, handwriting: false, image: false };
  if (p.some(x => /WHICH ONE/.test(x))) return { engrave: false, text: '', source: 'none', sourceQuote: '', requests: req, questions: ['Which of the two names is wanted?'], confidence: 0.3 };
  const e = p.length > 0; return { engrave: e, text: p.join('\n'), source: e ? 'personalization' : 'none', sourceQuote: p[0] || '', requests: req, questions: [], confidence: e ? 0.97 : 0.95 };
} };
const sleep = ms => new Promise(r => setTimeout(r, ms));
const SERIAL = { on: false }; let libChain = Promise.resolve();
const serial = fn => { const run = libChain.then(fn, fn); libChain = run.catch(() => {}); return run; };
const WRITE_OP = /^(sign|finalize|put|backPut|poolUpdate|poolPut|putSheet|runPut|timelineAdd)$/;
// the writes an approval makes (the back file, its record, the piece's update, the approval on the timeline): background writes of the page are not these
// (for one piece: the back record, the back files and the approval on the timeline; not another piece's, not the sheet's own files that follow some seconds later,
//  not the piece's plain updates that sending it back makes)
const engW = key => x => { const s = JSON.stringify(x.body || {}), pool = poolOf(key); return (x.op === 'backPut' && s.includes(pool)) || (x.op === 'timelineAdd' && s.includes('engraveApproved') && s.includes(pool)) || (['sign', 'finalize'].includes(x.op) && s.includes('_back_' + pool)); };

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { try { ({ chromium } = require('/opt/node22/lib/node_modules/playwright/node_modules/playwright-core')); } catch (__) { console.log('  – no playwright-core: not run'); return; } }
  const shots = process.env.SHOTS || ''; if (shots) fs.mkdirSync(shots, { recursive: true });
  const only = (process.env.ONLY || '').split(',').filter(Boolean), want = n => !only.length || only.includes(n);
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cn-eapr-')), masterPath = path.join(tmp, 'BRITES-master.ai');
  await buildMaster(masterPath, { count: 4, edge: false });
  const srv = await start({ receipts, agentResults });
  const { st, sorterOrigin, stationOrigin } = srv;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
  const shot = async (page, name, sel) => { if (!shots) return; try { if (sel) await page.locator(sel).first().screenshot({ path: path.join(shots, name + '.png') }); else await page.screenshot({ path: path.join(shots, name + '.png') }); } catch (_) {} };
  const errors = [];

  /** One browser window on the fake site: a real master file, a real Auto set, up to the engraving review. */
  async function boot(label, { sandbox = false } = {}) {
    const ctx = await browser.newContext({ viewport: { width: 1500, height: 1000 } });
    const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
    await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
    await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
    await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
    await ctx.route(/^https:\/\/firebasestorage\.googleapis\.com\//, r => { const u = new URL(r.request().url()), m = /\/o\/(.+)$/.exec(u.pathname), key = m ? decodeURIComponent(m[1]) : '', b = st.blobs.get(key); if (!b) return r.fulfill({ status: 404, body: 'no blob ' + key }); return r.fulfill({ status: 200, headers: { 'Content-Type': b.meta.contentType || 'application/octet-stream', 'Access-Control-Allow-Origin': '*', 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: b.buf }); });
    // what the cloud does for the writes of an approval: held up, or refused, when a phase says so (every call is logged)
    const net = { log: [], delayMs: 0, rules: [] };
    await ctx.route(/\/\.netlify\/functions\//, async route => {
      const req = route.request();
      if (req.method() !== 'POST') { if (SERIAL.on && /\/charmNestLibrary$/.test(new URL(req.url()).pathname)) return serial(async () => { try { const resp = await route.fetch(); await route.fulfill({ response: resp }); } catch (e) { await route.abort().catch(() => {}); } }); return route.continue(); }
      let b = {}; try { b = JSON.parse(req.postData() || '{}'); } catch (_) {}
      const fn = new URL(req.url()).pathname.split('/').pop(), op = b.op || (Array.isArray(b.timeline) ? 'timeline' : '');
      net.log.push({ t: Date.now(), fn, op, body: b });
      const rule = net.rules.find(r => r.times !== 0 && r.match(fn, op, b));
      if (rule) { if (rule.times > 0) rule.times--; if (rule.action === 'abort') return route.abort('internetdisconnected'); return route.fulfill({ status: rule.status || 500, contentType: 'application/json', body: JSON.stringify({ error: rule.error || 'forced failure' }) }); }
      if (net.delayMs && (WRITE_OP.test(op) || fn === 'charmNestOutput')) await sleep(net.delayMs);
      // (two pages share this one process: the library function keeps its Sandbox_ prefix in a variable "set per request; an instance handles one request at a time",
      //  so with two pages at once its calls are answered one at a time, as one deployed instance does)
      if (SERIAL.on && fn === 'charmNestLibrary') return serial(async () => { try { const resp = await route.fetch(); await route.fulfill({ response: resp }); } catch (e) { await route.abort().catch(() => {}); } });
      return route.continue();
    });
    await ctx.addInitScript(({ station, sorter }) => {
      if (location.origin === station) { localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200)); localStorage.setItem('employee_name', 'Tester'); }
      if (location.origin === sorter) { if (!sessionStorage.getItem('__named')) { localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__named', '1'); } localStorage.setItem('cn.tour.seen', '1'); }
      window.confirm = () => true; window.alert = () => {};
    }, { station: stationOrigin, sorter: sorterOrigin });
    const page = await ctx.newPage(), dialogs = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(label + ': ' + e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    // (a browser pop-up is recorded, never answered; the page's own "leave this page?" when it reloads itself is let through, or the reload into the Sandbox would never happen)
    page.on('dialog', d => { dialogs.push(d.type() + ': ' + d.message()); (d.type() === 'beforeunload' ? d.accept() : d.dismiss()).catch(() => {}); });
    await page.goto(`${sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && window.CN.S && window.RunCtl && window.DesignLink && CN.S.cloud.ok !== null, null, { timeout: 60000 });
    await page.evaluate(station => { const s = CN.S.settings; s.dsOrigin = station; s.engine = 'solver'; s.budgetS = 8; s.review = 'off'; s.naming = 'off'; s.notify = 'off'; s.sound = 'off'; s.autoCommit = 'on'; s.runMode = 'manual'; s.pullMode = 'all'; s.heartbeatS = 2; s.heartbeatMiss = 2; s.sandboxStream = 'off'; CN.saveSettings(); }, stationOrigin);
    if (!sandbox) {
      await page.evaluate(() => CN.setMode('master')); await page.waitForSelector('#mFile', { state: 'attached' }); await page.setInputFiles('#mFile', masterPath);
      await page.waitForFunction(() => { const j = [...B.master.jobs.values()][0]; return j && ['done', 'error'].includes(j.state); }, null, { timeout: 120000 });
    }
    await page.evaluate(() => CN.setMode('design')); await page.evaluate(() => DesignLink.ensure());
    if (sandbox) {
      // one press does the chain: snapshot of the open orders, switch on, reload, pull from the copy
      await page.evaluate(() => CN.setMode('orders')); await page.evaluate(() => openSettings()); await page.click('#stSbOn');
      await page.waitForFunction(() => !!(window.CN && window.B) && CN.S.settings.sandbox === 'on' && B.orders.rows.length > 0, null, { timeout: 120000 });
      await page.waitForFunction(() => window.CN && window.Sandbox && CN.S.cloud.ok !== null && CN.S.settings.sandbox === 'on');
      await page.waitForFunction(n => !!(window.CN && window.B) && B.orders.rows.length >= n && CN.S.mode === 'orders', NROWS, { timeout: 90000 }).catch(() => {});
      await page.evaluate(station => { const s = CN.S.settings; s.dsOrigin = station; s.engine = 'solver'; s.budgetS = 8; s.review = 'off'; s.naming = 'off'; s.notify = 'off'; s.sound = 'off'; s.autoCommit = 'on'; s.runMode = 'manual'; s.pullMode = 'all'; s.heartbeatS = 2; s.heartbeatMiss = 2; s.sandboxStream = 'off'; CN.saveSettings(); }, stationOrigin);
    }
    await page.evaluate(() => CN.setMode('orders')); await page.evaluate(() => RunCtl.setMode('auto'));
    const t0 = Date.now();
    for (;;) {
      const r = await page.evaluate(() => B.run && { status: B.run.status, step: B.run.step }), jobs = await page.evaluate(() => [...Engrave.items().values()].map(j => j.state));
      if (r && r.status === 'stopped') throw new Error('the run stopped: ' + JSON.stringify(await page.evaluate(() => ({ by: B.run.stoppedBy, fix: B.run.fix }))));
      if (r && r.status === 'processed' && jobs.length >= NJOBS && !jobs.some(s => ['ready', 'classify', 'fitting', 'reclassify'].includes(s))) break;
      if (Date.now() - t0 > 420000) throw new Error('the run never reached the engraving review: ' + JSON.stringify([r, jobs]));
      await page.waitForTimeout(500);
    }
    // the engraving jobs of the nine orders: the ones still to approve, and the one whose words need a decision
    const states = await page.evaluate(() => Object.fromEntries([...Engrave.items().values()].map(j => [j.key, j.state])));
    return { ctx, page, net, dialogs, states };
  }

  // ───────────────────────────── what a person reads and presses ─────────────────────────────
  const H = page => ({
    // the card in a host: what it says, its buttons, its seals, and whether anything lies outside its own box
    read: sel => page.evaluate(sel => {
      const card = document.querySelector(sel); if (!card) return null;
      const r = card.getBoundingClientRect(), q = s => card.querySelector(s), out = [];
      for (const n of card.querySelectorAll('*')) { if (n.closest('.seal, .sealTool, .sealRing') || !n.getClientRects().length) continue; const b = n.getBoundingClientRect(); if (b.width && b.height && (b.right > r.right + .5 || b.left < r.left - .5)) out.push(n.tagName.toLowerCase() + '.' + String(n.className).split(' ')[0]); }
      const seals = [...card.querySelectorAll('.seal svg[data-seal-model]')].map(s => { try { const m = JSON.parse(s.getAttribute('data-seal-model')); return `${m.action}|${m.by}|${m.at}`; } catch (_) { return '?'; } });
      const ab = q('.egApproveButton'), op = q('[data-e=engrave]');
      return { state: card.dataset.state, w: Math.round(r.width), out, scrollOver: card.scrollWidth - card.clientWidth, pill: (q('.egPill') || {}).textContent || '', why: (q('.egWhy b') || q('.top b') || {}).textContent || '', sub: ((q('.egSub') || {}).textContent || '').trim(), words: (q('.words') || {}).textContent || '',
        approve: ab ? { text: ab.textContent.trim(), disabled: ab.disabled, live: !!q('[data-e=approve]'), spin: !!ab.querySelector('.spin'), busy: ab.getAttribute('aria-busy') } : null, offWhy: (q('.egOffWhy') || {}).textContent || '', fail: ((q('.egFail') || {}).textContent || '').trim(), failRole: (q('.egFail') || {}).getAttribute && q('.egFail').getAttribute('role'),
        open: op ? op.textContent.trim() : '', seals, sealCount: card.querySelectorAll('.seal').length, buttons: [...card.querySelectorAll('button')].filter(b => !b.closest('.pv')).map(b => b.textContent.trim()), text: card.innerText.replace(/\s+/g, ' ') };
    }, sel),
    waitState: (sel, state, ms = 20000) => page.waitForFunction(({ sel, state }) => { const e = document.querySelector(sel); return e && (e.offsetParent || e.getClientRects().length) && (!state || e.dataset.state === state); }, { sel, state }, { timeout: ms }).then(() => true, () => false),
    openWin: async (key, opts) => { await page.evaluate(({ k, o }) => OrderWin.open(k, o || undefined), { k: key, o: opts || null }); await page.waitForFunction(k => OrderWin.isOpen() && OrderWin.key() === k, key, { timeout: 15000 }); await page.waitForFunction(() => !document.querySelector('#orderWin').getAnimations({ subtree: true }).some(a => a.playState === 'running' && a.effect && a.effect.getTiming().iterations !== Infinity), null, { timeout: 15000 }).catch(() => {}); },
    closeWin: async () => { await page.evaluate(() => { const o = document.getElementById('orderWin'); if (o && o.open) document.getElementById('owClose').click(); }); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 6000 }).catch(() => {}); },
    job: key => page.evaluate(k => { const j = Engrave.items().get(k); return j ? { state: j.state, by: j.approvedBy || '', at: j.approvedAt || 0, seals: (j.engravingSeals || []).map(s => `${s.how}|${s.by}|${s.at}`), backs: (j.backs || []).length, backPending: j.backPending || '', backSaving: !!j.backSaving, stamping: !!j.stamping, preparing: !!j.approvalPreparing, text: j.text } : null; }, key),
    counts: () => page.evaluate(() => {
      const t = id => ((document.getElementById(id) || {}).textContent || '').trim();
      const lib = [...document.querySelectorAll('#libView .libCard')].map(c => c.innerText.replace(/\s+/g, ' ')).find(x => /^SS Sheet/.test(x)) || '';
      return { engraveTab: t('tabEngraveN'), ordersTab: t('tabOrdersN'), reviewTab: t('tabReviewN'), pending: Engrave.pendingCount(), reviewed: Engrave.reviewedCount(), review: Review.count(), lib: (/(\d+) \/ (\d+)/.exec(lib) || [])[0] || '' };
    })
  });
  // the writes of the page and of the fake cloud
  const docsNow = () => new Map(JSON.parse(JSON.stringify([...st.docs.entries()])));
  const diffDocs = (a, b) => { const out = []; for (const [k, v] of b) { if (!a.has(k)) out.push(['new', k]); else if (JSON.stringify(a.get(k)) !== JSON.stringify(v)) out.push(['changed', k]); } for (const k of a.keys()) if (!b.has(k)) out.push(['gone', k]); return out; };
  const engDocs = diff => diff.filter(([, k]) => /^(Sandbox_)?(Charm_Pool|Charm_Pool_Back|Order_Timeline|Charm_Nest_Sheets)\//.test(k));
  const timelineSeals = (rid, pool) => st.list('Order_Timeline').filter(d => String(d.orderId) === String(rid) && d.type === 'engraveApproved' && (!pool || (d.data && d.data.poolId === pool)));

  try {
    if (!want('boot')) { /* every phase needs the run */ }
    console.log('· booting the fake site, the master file and a whole Auto set (production)');
    const A = await boot('production'), page = A.page, h = H(page), net = A.net;
    const watch = { approve: [], press: [] };
    // (pass-through counters on the page's own functions: they call straight through to the real ones)
    const wrap = p => p.evaluate(() => { window.__calls = { approve: [], press: [], open: [] }; const ap = Engrave.approve; Engrave.approve = function (job, by, btn) { __calls.approve.push({ key: job.key, by: by || '', t: Date.now(), state: job.state }); return ap.apply(this, arguments); }; const pr = CNEngravingSeals.press; CNEngravingSeals.press = function (b, s) { __calls.press.push({ how: s && s.how, by: s && s.by, at: s && s.at }); return pr.apply(this, arguments); }; const op = EngraveLink.open; EngraveLink.open = function (t) { __calls.open.push({ rid: t && t.rid, key: t && t.key, poolId: t && t.poolId, piece: t && t.piece }); return op.apply(this, arguments); }; });
    await wrap(page);
    const calls = () => page.evaluate(() => JSON.parse(JSON.stringify(window.__calls)));
    const states = A.states;
    check(Object.keys(states).length >= NJOBS && states[K.A] === 'review' && states[K.B] === 'review' && states[K.O2] === 'review' && states[K.O3] === 'words', 'the real run reached the engraving review: ' + JSON.stringify(Object.fromEntries(Object.entries(states).map(([k, v]) => [k.slice(-4), v]))));
    const OV = '#owEng .swEng', OVA = '#owEng > .owEng:nth-of-type(1) .swEng', OVB = '#owEng > .owEng:nth-of-type(2) .swEng', TAB = '#owSheetPanel [data-engraving-panel] .swEng', WIN = '[data-r2=eng] .swEng';

    // (a phase that stops on an error says so and the next one goes on: the window is put away, a picture is kept)
    const phase = async (name, fn) => {
      if (!want(name)) return;
      try { await fn(); }
      catch (e) {
        check(false, name + ': stopped by an error: ' + String(e && e.message || e).split('\n').slice(0, 3).join(' | '));
        await shot(page, 'real-error-' + name);
        try { await page.evaluate(() => { const o = document.getElementById('orderWin'); if (o && o.open) document.getElementById('owClose').click(); try { SheetWin.close(); } catch (_) {} }); } catch (_) {}
        try { await A.ctx.setOffline(false); net.rules.length = 0; net.delayMs = 0; await page.setViewportSize({ width: 1500, height: 1000 }); } catch (_) {}
      }
    };
    const asNamed = n => page.evaluate(n => { B.employee = n; try { localStorage.setItem('cn.employee', n); } catch (_) {} }, n);
    const noName = () => page.evaluate(() => { B.employee = ''; try { localStorage.removeItem('cn.employee'); } catch (_) {} });
    const written = (key, ms = 90000) => page.waitForFunction(k => { const j = Engrave.items().get(k); return j && j.state === 'written' && !j.backSaving && !j.backPending; }, key, { timeout: ms }).then(() => true, () => false);
    const stood = (key, ms = 60000) => page.waitForFunction(k => { const j = Engrave.items().get(k); return j && ['approved', 'written'].includes(j.state) && !j.stamping && !j.approvalPreparing; }, key, { timeout: ms }).then(() => true, () => false);
    // every state a card passes through (what it says, its button, the note and the failure line), so a short wait is not missed
    const watchCard = cardSel => page.evaluate(sel => {
      window.__w = [];
      const snap = () => {
        const c = document.querySelector(sel); if (!c) return;
        const b = c.querySelector('.egApproveButton'), sub = c.querySelector('.egSub'), f = c.querySelector('.egFail'), ow = c.querySelector('.egOffWhy');
        const s = [c.dataset.state, b ? b.textContent.trim() + (b.disabled ? ' (off' : ' (on') + (b.hasAttribute('data-e') ? ',live' : ',inert') + (b.querySelector('.spin') ? ',spin' : '') + ')' : '-', sub ? 'note: ' + sub.textContent.trim() + (sub.querySelector('.owSpin') ? ' +spin' : '') : '', ow ? 'why: ' + ow.textContent.trim() : '', f ? 'FAIL: ' + f.textContent.trim() : ''].filter(Boolean).join(' | ');
        if (window.__w[window.__w.length - 1] !== s) window.__w.push(s);
        // (while the back is being prepared: the off button is clicked and Enter pressed on it, in the very turn it is seen)
        if (c.dataset.state === 'preparing' && window.__poke && b) { window.__poked = (window.__poked || 0) + 1; b.click(); b.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true })); b.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true })); }
      };
      if (window.__wo) window.__wo.disconnect();
      window.__wo = new MutationObserver(snap); window.__wo.observe(document.body, { subtree: true, childList: true, attributes: true, characterData: true }); snap();
    }, cardSel);
    const watched = () => page.evaluate(() => window.__w.slice());
    const srv0 = () => st.calls.length;
    const srvOps = from => st.calls.slice(from).map(c => c.name + ':' + (c.op || ''));
    const openLanding = () => page.evaluate(() => { const v = Engrave.view(), c = document.querySelector('#engraveView .rvItem[data-key]'), d = document.querySelector('#engraveView .doneRow.open[data-key]'); return { mode: CN.S.mode, tab: v.tab, q: v.q, key: (c || d || {}).dataset ? (c || d).dataset.key : null, dialogs: document.querySelectorAll('dialog[open]').length, orderWin: document.getElementById('orderWin').open }; });
    const ids = (rid, key) => ({ rid, key, pool: poolOf(key) });
    const normStr = (s, i) => s.split(i.pool).join('<pool>').split(i.key).join('<key>').split(i.rid).join('<rid>').replace(/(?<!\d)1[5-9]\d{11}(?!\d)/g, '<t>');
    const norm = (v, i) => { const w = x => { if (typeof x === 'number') return x > 1.5e12 && x < 2e12 ? '<t>' : x; if (typeof x === 'string') return normStr(x, i); if (Array.isArray(x)) return x.map(w); if (x && typeof x === 'object') return Object.fromEntries(Object.entries(x).map(([k, y]) => [normStr(k, i), k === '_seconds' || k === '_nanoseconds' ? '<ts>' : w(y)])); return x; }; return w(JSON.parse(JSON.stringify(v === undefined ? null : v))); };
    const flat = (v, p = '', o = {}) => { if (v && typeof v === 'object') for (const k of Object.keys(v)) flat(v[k], p + '/' + k, o); else o[p] = v; return o; };
    const diffFlat = (a, b) => { const fa = flat(a), fb = flat(b), out = []; for (const k of new Set([...Object.keys(fa), ...Object.keys(fb)])) if (JSON.stringify(fa[k]) !== JSON.stringify(fb[k])) out.push(k + ': ' + JSON.stringify(fa[k]) + ' vs ' + JSON.stringify(fb[k])); return out; };

    // ═══ 1 · Approve is off, with the plain reason, while the words are unconfirmed (and cannot be pressed any way) ═══
    await phase('disabled', async () => {
      console.log('· disabled while the words are unconfirmed');
      await h.openWin(K.O3); await h.waitState(OV, 'words');
      let c = await h.read(OV), w0 = (await calls()).approve.length, n0 = net.log.length;
      check(c.state === 'words' && c.pill === 'Words to confirm' && c.approve && c.approve.disabled && !c.approve.live && /Confirm the words in Engraving first/.test(c.offWhy), 'words to confirm: Approve engraving is disabled, one plain line says why: ' + JSON.stringify([c.pill, c.approve, c.offWhy]));
      await shot(page, 'real-words-disabled', '#owEng');
      const tried = await page.evaluate(async () => {
        const b = document.querySelector('#owEng .egApproveButton'), seen = []; const L = ev => seen.push(ev.type); b.addEventListener('click', L, true);
        b.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true })); b.click(); b.focus();
        const focused = document.activeElement === b;
        await new Promise(r => setTimeout(r, 300)); return { focused, seen };
      });
      await page.keyboard.press('Tab'); await page.keyboard.press('Enter'); await page.keyboard.press('Space'); await page.waitForTimeout(400);
      const w1 = (await calls()).approve.length, writes = net.log.slice(n0).filter(engW(K.O3));
      check(!tried.focused, 'it takes no focus (a disabled button is skipped by the keyboard): ' + JSON.stringify(tried));
      check(w1 === w0 && writes.length === 0 && (await h.job(K.O3)).state === 'words' && (await h.job(K.O3)).seals.length === 0, 'a click, Enter and Space do nothing: no approval call, no write, no seal (' + (w1 - w0) + ' approvals, ' + writes.length + ' writes)');
      check(A.dialogs.length === 0, 'and nothing was asked (no browser pop-up): ' + JSON.stringify(A.dialogs));
      await h.closeWin();
    });

    // ═══ 2 · the happy path from the card: O2 (one piece), the real Engrave.approve, nothing stubbed ═══
    let ref2 = null;
    await phase('approve', async () => {
      console.log('· Approve engraving on the card, for real (O2)');
      net.delayMs = 700;                                  // (the cloud answers slowly: the waits and the repaints can be seen)
      await h.openWin(K.O2); await h.waitState(OV, 'approve');
      let c = await h.read(OV);
      check(c.state === 'approve' && c.approve && !c.approve.disabled && c.approve.live && c.words === 'CARLA' && c.sealCount === 0, 'To approve: Approve engraving is live, the words are CARLA, no seal yet: ' + JSON.stringify([c.state, c.approve, c.words]));
      const before = { counts: await (async () => { await page.evaluate(() => CN.setMode('library')); await page.waitForTimeout(3500); const x = await h.counts(); await page.evaluate(() => CN.setMode('orders')); return x; })(), docs: docsNow(), blobs: new Set(st.blobs.keys()), n: net.log.length, calls: await calls(), revCard: await page.evaluate(k => Review.items().some(i => i.key === 'eng:' + k), K.O2) };
      await h.openWin(K.O2); await h.waitState(OV, 'approve');
      // no name saved on this computer: a small bar in the window asks (never a browser pop-up on the window)
      await page.evaluate(() => { B.employee = ''; try { localStorage.removeItem('cn.employee'); } catch (_) {} });
      const nameNow = await page.evaluate(() => CNEmployee.name());
      await page.click(OV + ' [data-e=approve]');
      const bar = await page.waitForSelector('.cnNameBar', { timeout: 4000 }).then(() => true, () => false);
      const inWin = bar && await page.evaluate(() => !!document.querySelector('#orderWin .cnNameBar'));
      check(nameNow === '' && bar && inWin && A.dialogs.length === 0, 'no name saved: the small name bar opens inside the order window and no browser pop-up is raised: ' + JSON.stringify({ nameNow, bar, inWin, dialogs: A.dialogs }));
      await shot(page, 'real-name-bar', '#orderWin');
      // put away: nothing was approved, nothing written, the button is as it was
      await page.click('.cnNameBar .cnNbX'); await page.waitForTimeout(500);
      c = await h.read(OV);
      check(c.state === 'approve' && c.approve && !c.approve.disabled && c.approve.text === 'Approve engraving' && c.sealCount === 0 && (await calls()).approve.length === before.calls.approve.length && !net.log.slice(before.n).some(engW(K.O2)), 'the name bar put away: no approval, no write, no seal, the button is as it was: ' + JSON.stringify([c.state, c.approve]));
      // again, with a name
      await page.click(OV + ' [data-e=approve]'); await page.waitForSelector('.cnNameBar', { timeout: 4000 });
      await page.fill('.cnNameBar input', 'Test Operator');
      // (every state the button passes through is recorded, so a short wait is not missed)
      await page.evaluate(() => { window.__btn = []; const host = document.getElementById('owEng'); const snap = () => { const b = host.querySelector('.egApproveButton'); if (!b) return; const s = (b.textContent || '').trim() + '|' + (b.disabled ? 'off' : 'on') + '|' + (b.querySelector('.spin') ? 'spin' : '') + '|' + (b.getAttribute('aria-busy') || ''); if (window.__btn[window.__btn.length - 1] !== s) window.__btn.push(s); }; new MutationObserver(snap).observe(host, { subtree: true, childList: true, attributes: true, characterData: true }); snap(); });
      await page.click('.cnNameBar button[type=submit]');
      const t0 = Date.now();
      // the wait is said on the button with a small spinner; a second press, Enter then a click, and a repaint all find the approval already running
      await page.waitForFunction(() => window.__btn.some(s => /^Approved\|off/.test(s)) || window.__btn.some(s => /^Approving/.test(s)), null, { timeout: 8000 }).catch(() => {});
      c = await h.read(OV);
      const log0 = await page.evaluate(() => window.__btn.slice());
      check(log0.some(s => /^Approving…\|off\|spin\|true$/.test(s)), 'while it is prepared the button says so, with a small spinner, and is off: ' + JSON.stringify(log0));
      await page.evaluate(() => { const b = document.querySelector('#owEng .egApproveButton'); b.focus(); });
      await page.keyboard.press('Enter');
      await page.click(OV + ' .egApproveButton', { force: true, noWaitAfter: true }).catch(() => {});
      await page.dblclick(OV + ' .egApproveButton', { force: true, noWaitAfter: true }).catch(() => {});
      await page.evaluate(() => { try { Orders.render(); } catch (_) {} try { OrderWin.paint(); } catch (_) {} try { document.getElementById('owEng')._orderEngraving.refresh(true); } catch (_) {} try { OrderWin.nudge(); } catch (_) {} });
      // (the stamp lands; the card must turn green Approved within a second of it, not after the back file is saved)
      await page.waitForFunction(() => window.__calls.press.length >= 1, null, { timeout: 30000 });
      const stampAt = Date.now();
      await page.waitForFunction(() => document.querySelector('#owEng .swEng[data-state=approved]'), null, { timeout: 30000 });
      const approvedAfter = Date.now() - stampAt, jobAt = await h.job(K.O2);
      check(approvedAfter < 2600 || jobAt.state === 'written', 'the card turned Approved when the stamp landed, not when the back file finished saving (' + approvedAfter + ' ms after the press; the job: ' + jobAt.state + ')');
      await page.waitForFunction(k => Engrave.items().get(k).state === 'written', K.O2, { timeout: 60000 });
      await page.waitForTimeout(1500); net.delayMs = 0;
      c = await h.read(OV); const cl = await calls(), job = await h.job(K.O2);
      const mine = cl.approve.slice(before.calls.approve.length), presses = cl.press.slice(before.calls.press.length);
      check(mine.length === 1 && mine[0].key === K.O2 && mine[0].by === 'Test Operator', 'Engrave.approve was called once for this piece, by the name typed (' + mine.length + ' calls)');
      check(presses.length === 1 && presses[0].how === 'engraveApproved' && presses[0].by === 'Test Operator', 'one stamp was pressed: ' + JSON.stringify(presses));
      const writes = net.log.slice(before.n).filter(x => WRITE_OP.test(x.op)), count = op => writes.filter(x => x.op === op && (op !== 'backPut' && op !== 'poolUpdate' || JSON.stringify(x.body).includes(poolOf(K.O2)))).length;
      check(count('backPut') === 1 && count('poolUpdate') === 1 && count('timelineAdd') >= 1 && timelineSeals(R.O2, poolOf(K.O2)).length === 1 && !!st.doc('Charm_Pool_Back', poolOf(K.O2)), 'the server wrote it once: backPut ' + count('backPut') + ', poolUpdate ' + count('poolUpdate') + ', one engraveApproved on the timeline (' + timelineSeals(R.O2, poolOf(K.O2)).length + '), the back record is there');
      check(c.state === 'approved' && c.pill === 'Approved' && c.approve && c.approve.disabled && c.approve.text === 'Approved' && /^View in Engraving/.test(c.open) && c.seals.length === 1 && /^BACK ENGRAVING\|Test Operator\|/.test(c.seals[0]) && c.out.length === 0, 'the card is green Approved with one BACK ENGRAVING seal on the button and View in Engraving: ' + JSON.stringify([c.state, c.approve, c.open, c.seals]));
      check(job.state === 'written' && job.seals.length === 1 && job.backs === 1 && !job.backPending && !job.preparing && !job.stamping, 'the piece is written, one seal, its back file saved: ' + JSON.stringify(job));
      await shot(page, 'real-approved-card', '#owEng');
      const dAfter = docsNow(), dDiff = engDocs(diffDocs(before.docs, dAfter)).filter(([, k]) => k.includes(poolOf(K.O2)) || String((dAfter.get(k) || {}).orderId) === R.O2);
      ref2 = { key: K.O2, rid: R.O2, docs: Object.fromEntries(dDiff.map(([how, k]) => [k, dAfter.get(k)])), blobs: [...st.blobs.keys()].filter(k => !before.blobs.has(k)), reqs: writes.map(x => ({ fn: x.fn, op: x.op, body: x.body })), seal: job.seals[0] };
      // on the Timeline (the one seal, drawn by the timeline itself)
      await page.click('.owTabsV [data-ow-view="timeline"]'); await page.waitForTimeout(2200);
      const tl = await page.evaluate(() => { const set = new Set(); for (const s of document.querySelectorAll('#orderWin .seal svg[data-seal-model]')) { try { const m = JSON.parse(s.getAttribute('data-seal-model')); if (m.action === 'BACK ENGRAVING') set.add(`${m.by}|${m.at}`); } catch (_) {} } return [...set]; });
      check(tl.length === 1 && /^Test Operator\|/.test(tl[0]), 'the Timeline shows the BACK ENGRAVING seal, once, under the same name: ' + JSON.stringify(tl));
      await shot(page, 'real-timeline', '#orderWin');
      // on the Sheet tab
      await page.click('.owTabsV [data-ow-view="sheet"]'); await page.waitForFunction(() => OrderWin.view() === 'sheet' && OrderWin._sheet() && document.querySelector('#owSheetPanel .owPieces'), null, { timeout: 25000 });
      await h.waitState(TAB, 'approved', 15000); const ct = await h.read(TAB);
      check(ct && ct.state === 'approved' && ct.seals.length === 1 && ct.seals[0] === c.seals[0], 'the Sheet tab shows the same approval, the same single seal: ' + JSON.stringify(ct && ct.seals));
      await h.closeWin();
      // in the Sheet window
      const sh = await page.evaluate(k => { const s = Pool.sheetOf(k + '_1'); return s && s.sheetId; }, K.O2);
      await page.evaluate(({ sh, p }) => SheetWin.open(sh, { select: p }), { sh, p: poolOf(K.O2) });
      await page.waitForFunction(() => SheetWin.isOpen && SheetWin.isOpen(), null, { timeout: 20000 }); await h.waitState(WIN, 'approved', 20000);
      const cw = await h.read(WIN);
      check(cw && cw.state === 'approved' && cw.seals.length === 1 && cw.seals[0] === c.seals[0], 'the Sheet window shows it too, the same single seal: ' + JSON.stringify(cw && cw.seals));
      await page.evaluate(() => { try { SheetWin.close(); } catch (_) {} });
      await page.waitForFunction(() => !(SheetWin.isOpen && SheetWin.isOpen()), null, { timeout: 8000 }).catch(() => {});
      // View in Engraving: the Engraving panel opens on this piece's Decided row, which carries the same seal, once
      await h.openWin(K.O2); await h.waitState(OV, 'approved');
      await page.click(OV + ' [data-e=engrave]');
      await page.waitForFunction(() => CN.S.mode === 'engrave' && document.querySelector('#engraveView .doneRow.open[data-key]'), null, { timeout: 20000 }).catch(() => {});
      await page.waitForTimeout(1200);
      const dec = await page.evaluate(k => { const row = document.querySelector('#engraveView .doneRow.open[data-key]'), v = Engrave.view(); const seals = row ? [...row.querySelectorAll('.seal svg[data-seal-model]')].map(s => { try { const m = JSON.parse(s.getAttribute('data-seal-model')); return `${m.action}|${m.by}|${m.at}`; } catch (_) { return '?'; } }) : []; return { key: row && row.dataset.key, seals, tab: v.tab, text: row ? row.innerText.replace(/\s+/g, ' ') : '', dialogs: document.querySelectorAll('dialog[open]').length, orderWin: document.getElementById('orderWin').open }; }, K.O2);
      check(dec.key === K.O2 && /CARLA/.test(dec.text) && dec.seals.includes(c.seals[0]) && dec.seals.filter(x => x === c.seals[0]).length === 1 && dec.dialogs === 0 && !dec.orderWin, 'View in Engraving opens the Engraving panel on this piece\'s Decided row, with the same seal, once: ' + JSON.stringify(dec));
      // the counts agree
      await page.evaluate(() => CN.setMode('library')); await page.waitForTimeout(3500);
      const after = await h.counts(); const p0 = before.counts, num = x => +(/^(\d+)/.exec(x.lib) || [])[1] || 0;
      check(+after.pending === +p0.pending - 1 && +after.reviewed === +p0.reviewed + 1 && +after.engraveTab === +p0.engraveTab - 1 && after.ordersTab === p0.ordersTab && after.review === p0.review && num(after) === num(p0) + 1, 'the counts agree: Engraving ' + p0.engraveTab + ' → ' + after.engraveTab + ', decided ' + p0.reviewed + ' → ' + after.reviewed + ', Orders ' + p0.ordersTab + ' → ' + after.ordersTab + ', Review ' + p0.review + ' → ' + after.review + ', the Library sheet ' + p0.lib + ' → ' + after.lib);
      await page.evaluate(() => CN.setMode('orders'));
      const meta = await (async () => { await h.openWin(K.O2); const m = await page.evaluate(() => [...document.querySelectorAll('#owMeta .m')].map(x => x.innerText.replace(/\s+/g, ' ')).find(t => /^Engraving/i.test(t)) || ''); await h.closeWin(); return m; })();
      check(/written|approved/i.test(meta) && /CARLA/.test(meta), 'the order window\'s Engraving cell reads the same: ' + meta);
      const rv = await page.evaluate(k => ({ item: Review.items().some(i => i.key === 'eng:' + k), mode: CN.S.mode }), K.O2);
      check(rv.item === false && before.revCard === true, 'the piece\'s Review card (the placement waiting for approval) was there before and is gone now: ' + JSON.stringify([before.revCard, rv.item]));
      check(A.dialogs.length === 0, 'no browser pop-up was raised at any point: ' + JSON.stringify(A.dialogs));
    });

    // ═══ 3 · the Engraving panel's own Approve is the reference: O4 has the same charm and words as O2, so what the two write can be compared ═══
    await phase('panel', async () => {
      console.log('· the Engraving panel\'s own Approve (O4) against the card\'s (O2)');
      await asNamed('Test Operator'); net.delayMs = 0;
      const n0 = net.log.length, d0 = docsNow();
      await page.evaluate(({ k, rid }) => { CN.setMode('engrave'); Engrave.restoreView(Object.assign({}, Engrave.view(), { tab: 'place', focus: k, list: false, chosen: true, q: rid, openDone: null })); Engrave.render(); }, { k: K.O4, rid: R.O4 });
      const btn = `#egQueue .rvItem[data-key="${K.O4}"] [data-a=approve]`;
      await page.waitForFunction(s => { const b = document.querySelector(s); return b && !b.disabled; }, btn, { timeout: 40000 });
      await page.click(btn);
      const ok4 = await written(K.O4), job4 = await h.job(K.O4);
      check(ok4 && job4.seals.length === 1 && job4.by === 'Test Operator', 'the Engraving panel\'s own Approve wrote the piece, one seal: ' + JSON.stringify(job4));
      await page.waitForTimeout(600);
      const dAfter = docsNow(), keep = k => /^(Charm_Pool|Charm_Pool_Back|Order_Timeline)\//.test(k);
      const docs4 = Object.fromEntries(engDocs(diffDocs(d0, dAfter)).filter(([, k]) => keep(k) && (k.includes(poolOf(K.O4)) || String((dAfter.get(k) || {}).orderId) === R.O4)).map(([, k]) => [k, dAfter.get(k)]));
      const writes4 = net.log.slice(n0).filter(x => WRITE_OP.test(x.op)).map(x => ({ fn: x.fn, op: x.op, body: x.body }));
      const i2 = ids(R.O2, K.O2), i4 = ids(R.O4, K.O4), seq = a => a.map(x => x.fn + ':' + x.op).join(' ');
      // (the approval's own writes: up to the last record of the run; the sheet's own files that follow some seconds later are not part of it)
      const core = a => { let n = 0; a.forEach((x, i) => { if (x.op === 'runPut' || x.op === 'poolUpdate') n = i + 1; }); return a.slice(0, n); };
      if (!ref2) check(false, 'the card\'s approval (phase approve) has to run in the same run to be compared');
      else {
        const r2 = core(ref2.reqs), r4 = core(writes4);
        check(seq(r2) === seq(r4), 'the card and the panel make the same requests in the same order: ' + seq(r2) + (seq(r2) === seq(r4) ? '' : '  |  ' + seq(r4)));
        const diffs = [];
        r2.forEach((x, i) => { const y = r4[i]; if (y && x.op === y.op && ['backPut', 'poolUpdate', 'timelineAdd'].includes(x.op)) diffs.push(...diffFlat(norm(x.body, i2), norm(y.body, i4)).map(d => x.op + ' ' + d)); });
        const nd2 = Object.fromEntries(Object.entries(ref2.docs).filter(([k]) => keep(k)).map(([k, v]) => [normStr(k, i2), norm(v, i2)])), nd4 = Object.fromEntries(Object.entries(docs4).map(([k, v]) => [normStr(k, i4), norm(v, i4)]));
        check(JSON.stringify(Object.keys(nd2).sort()) === JSON.stringify(Object.keys(nd4).sort()) && Object.keys(nd2).length >= 3, 'the same records were written: ' + Object.keys(nd2).sort().join(', ') + '  |  ' + Object.keys(nd4).sort().join(', '));
        for (const k of Object.keys(nd2)) if (nd4[k]) diffs.push(...diffFlat(nd2[k], nd4[k]).map(d => k + ' ' + d));
        check(diffs.length === 0, 'field by field, what the card writes is what the Engraving panel writes (' + diffs.length + ' differences): ' + diffs.slice(0, 14).join(' · '));
        const b2 = ref2.blobs.map(k => normStr(k, i2)).sort(), b4 = [...st.blobs.keys()].filter(k => k.includes(poolOf(K.O4))).map(k => normStr(k, i4)).sort();
        check(b2.length >= 2 && JSON.stringify(b2) === JSON.stringify(b4), 'the same files were saved: ' + b2.join(', ') + '  |  ' + b4.join(', '));
      }
      // the seal of the Engraving panel's own Approve is the one the card shows
      await h.openWin(K.O4); await h.waitState(OV, 'approved');
      const c4 = await h.read(OV);
      check(c4.seals.length === 1 && c4.seals[0] === 'BACK ENGRAVING|Test Operator|' + job4.at, 'the card shows the panel\'s approval, the same single seal: ' + JSON.stringify(c4.seals));
      await h.closeWin();
    });

    // ═══ 4 · seals are permanent: the approved card offers nothing that takes one off or puts another in its place ═══
    await phase('seals', async () => {
      console.log('· seals are permanent (O2)');
      await h.openWin(K.O2); await h.waitState(OV, 'approved');
      const s0 = await h.read(OV), n0 = net.log.length, j0 = await h.job(K.O2);
      const r = await page.evaluate(async k => {
        const j = Engrave.items().get(k), b = document.querySelector('#owEng .egApproveButton');
        b.click(); b.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
        await Engrave.approve(j, 'Someone Else', b); await Engrave.approve(j, 'Someone Else');
        const owner = document.getElementById('owEng'); try { OrderWin.paint(); } catch (_) {} try { Orders.render(); } catch (_) {} try { owner._orderEngraving.refresh(true); } catch (_) {}
        return { state: j.state, by: j.approvedBy, seals: (j.engravingSeals || []).length, tools: document.querySelectorAll('#owEng .seal button, #owEng .seal [role=button], #owEng .seal a, #owEng .sealTool').length, ctl: [...document.querySelectorAll('#owEng [data-e]')].map(n => n.dataset.e) };
      }, K.O2);
      await page.waitForTimeout(1500);
      const s1 = await h.read(OV), j1 = await h.job(K.O2);
      check(JSON.stringify(s1.seals) === JSON.stringify(s0.seals) && s1.seals.length === 1, 'drawn again, the card holds the same one seal: ' + JSON.stringify(s1.seals));
      check(r.state === 'written' && r.by === j0.by && j1.seals.join() === j0.seals.join() && !net.log.slice(n0).some(engW(K.O2)), 'pressing the Approved button, and asking for the approval again as another person: nothing changes, nothing is written, the seal and its name stand: ' + JSON.stringify([r, j1.seals]));
      check(s1.buttons.join('|') === 'Approved|View in Engraving →' && r.tools === 0 && r.ctl.join() === 'engrave', 'the approved card has no way to remove or replace a seal (buttons ' + JSON.stringify(s1.buttons) + ', controls on the seals ' + r.tools + ', actions ' + JSON.stringify(r.ctl) + ')');
      await h.closeWin();
    });

    // ═══ 5 · one piece never approves another: "All pieces" shows a card for each, each approves only its own (O1) ═══
    await phase('multi', async () => {
      console.log('· an order of several pieces: each card approves only its own piece (O1)');
      await asNamed('Test Operator'); net.delayMs = 0;
      const pieces = () => page.evaluate(() => [...document.querySelectorAll('#owEng .swEng')].map(c => ({ state: c.dataset.state, words: (c.querySelector('.words') || {}).textContent || '', live: !!c.querySelector('[data-e=approve]'), seals: c.querySelectorAll('.seal').length })));
      await h.openWin(K.A);
      await page.waitForFunction(() => { const c = [...document.querySelectorAll('#owEng .swEng')]; return c.length >= 2 && c.every(x => x.dataset.state === 'approve'); }, null, { timeout: 40000 });
      let cards = await pieces();
      check(cards.length === 2 && cards.map(x => x.words).join() === 'ANNA,BELLA' && cards.every(x => x.live), '"All pieces": ANNA and BELLA each have their own card with a live Approve engraving; the plain piece has none: ' + JSON.stringify(cards));
      // Fix in Engraving on BELLA's card opens BELLA's own piece (EngraveLink.open with the order and that piece), with no pop-up on the window
      const o0 = (await calls()).open.length;
      await page.click(OVB + ' [data-e=engrave]');
      await page.waitForFunction(() => CN.S.mode === 'engrave' && document.querySelector('#engraveView .rvItem[data-key]'), null, { timeout: 20000 }).catch(() => {});
      await page.waitForTimeout(500);
      let lnd = await openLanding(), op = (await calls()).open.slice(o0);
      check(op.length === 1 && op[0].rid === R.O1 && op[0].piece === K.B && lnd.mode === 'engrave' && lnd.key === K.B && lnd.q === R.O1 && lnd.dialogs === 0 && !lnd.orderWin && A.dialogs.length === 0, 'Fix in Engraving on BELLA\'s card opens the Engraving panel on BELLA\'s piece of order ' + R.O1 + ': ' + JSON.stringify([op, lnd]));
      await page.evaluate(() => CN.setMode('orders'));
      // approve BELLA only
      await h.openWin(K.A);
      await page.waitForFunction(() => document.querySelectorAll('#owEng .swEng').length >= 2, null, { timeout: 30000 });
      const n0 = net.log.length, c0 = await calls(), pend0 = await page.evaluate(() => Engrave.pendingCount());
      await page.click(OVB + ' [data-e=approve]');
      const okB = await written(K.B);
      const a1 = await h.job(K.A), b1 = await h.job(K.B), cl = (await calls()).approve.slice(c0.approve.length);
      check(okB && cl.length === 1 && cl[0].key === K.B, 'only BELLA\'s piece was sent to approval: ' + JSON.stringify(cl));
      check(a1.state === 'review' && a1.seals.length === 0 && b1.state === 'written' && b1.seals.length === 1, 'ANNA\'s piece is untouched (' + a1.state + ', ' + a1.seals.length + ' seals), BELLA\'s is written with one seal (' + b1.state + ', ' + b1.seals.length + ')');
      const wr = net.log.slice(n0).filter(x => ['backPut', 'poolUpdate'].includes(x.op)), s = JSON.stringify(wr.map(x => x.body));
      check(wr.length === 2 && s.includes(poolOf(K.B)) && !s.includes(poolOf(K.A)) && !s.includes(poolOf(K.P)), 'the cloud was written for BELLA\'s piece only (' + wr.length + ' writes, ANNA\'s piece not in them)');
      cards = await pieces();
      check(cards.length === 2 && cards[0].state === 'approve' && cards[0].live && cards[0].seals === 0 && cards[1].state === 'approved' && cards[1].seals === 1, 'on the window, ANNA\'s card still waits for approval and BELLA\'s is Approved with its seal: ' + JSON.stringify(cards));
      const pend1 = await page.evaluate(() => Engrave.pendingCount());
      check(pend1 === pend0 - 1, 'the Engraving count went down by one: ' + pend0 + ' → ' + pend1);
      const sealB = (await h.read(OVB)).seals;
      // ANNA picked on her own (the piece list's own switch): her card approves her piece, and BELLA's seal is not touched
      await page.evaluate(k => OrderWin.selectPiece(k), K.A);
      await page.waitForFunction(() => { const c = document.querySelectorAll('#owEng .swEng'); return c.length === 1 && c[0].dataset.state === 'approve' && !c[0].classList.contains('egCompact'); }, null, { timeout: 20000 });
      const c1 = await calls(), n1 = net.log.length;
      await page.click(OV + ' [data-e=approve]');
      const okA = await written(K.A), a2 = await h.job(K.A), b2 = await h.job(K.B), cl2 = (await calls()).approve.slice(c1.approve.length);
      check(okA && cl2.length === 1 && cl2[0].key === K.A && a2.seals.length === 1 && b2.seals.join() === b1.seals.join() && b2.seals.length === 1, 'ANNA picked on her own approves ANNA only (' + JSON.stringify(cl2.map(x => x.key.slice(-2))) + '); BELLA still has her one seal ' + JSON.stringify(b2.seals));
      const wr2 = net.log.slice(n1).filter(x => ['backPut', 'poolUpdate'].includes(x.op)), s2 = JSON.stringify(wr2.map(x => x.body));
      check(wr2.length === 2 && s2.includes(poolOf(K.A)) && !s2.includes(poolOf(K.B)), 'and the cloud was written for ANNA\'s piece only');
      await page.evaluate(() => OrderWin.selectPiece(null));
      await page.waitForFunction(() => document.querySelectorAll('#owEng .swEng').length >= 2, null, { timeout: 20000 });
      await page.waitForTimeout(800);
      cards = await pieces();
      check(cards.length === 2 && cards.every(x => x.state === 'approved' && x.seals === 1 && !x.live) && (await h.read(OVB)).seals.join() === sealB.join(), 'back on "All pieces": two Approved cards, one seal each, BELLA\'s unchanged: ' + JSON.stringify(cards));
      check((await page.evaluate(k => { const j = Engrave.items().get(k); return !j || ['none', 'skipped'].includes(j.state) || !(j.engravingSeals || []).length; }, K.P)), 'the plain third piece got no approval and no seal');
      await shot(page, 'real-all-pieces-approved', '#owEng');
      await h.closeWin();
    });

    // ═══ 6 · the Sheet window (O5): its own name bar, Fix / View in Engraving, one approval ═══
    await phase('sheetwin', async () => {
      console.log('· the Sheet window (O5)');
      net.delayMs = 700; await noName();
      const sh = await page.evaluate(k => { const s = Pool.sheetOf(k + '_1'); return s && s.sheetId; }, K.O5);
      const open = () => page.evaluate(({ sh, p }) => SheetWin.open(sh, { select: p }), { sh, p: poolOf(K.O5) }).then(() => page.waitForFunction(() => SheetWin.isOpen && SheetWin.isOpen(), null, { timeout: 20000 }));
      await open(); await h.waitState(WIN, 'approve', 25000);
      // Fix in Engraving from the Sheet window: the window closes, the Engraving panel is on this piece
      const o0 = (await calls()).open.length;
      await page.click(WIN + ' [data-e=engrave]');
      await page.waitForFunction(() => CN.S.mode === 'engrave' && document.querySelector('#engraveView .rvItem[data-key]'), null, { timeout: 20000 }).catch(() => {});
      await page.waitForTimeout(500);
      const lnd = await openLanding(), op = (await calls()).open.slice(o0);
      check(op.length === 1 && op[0].rid === R.O5 && op[0].poolId === poolOf(K.O5) && lnd.key === K.O5 && lnd.q === R.O5 && lnd.dialogs === 0 && A.dialogs.length === 0, 'Fix in Engraving from the Sheet window opens the Engraving panel on this piece, the window gone, no pop-up on a pop-up: ' + JSON.stringify([op, lnd]));
      await page.evaluate(() => CN.setMode('orders'));
      await open(); await h.waitState(WIN, 'approve', 25000);
      // no name on this computer: the window's own small bar asks, and nothing is approved before it is answered
      const n0 = net.log.length, c0 = await calls();
      await watchCard(WIN);
      await page.click(WIN + ' [data-e=approve]');
      const bar = await page.waitForSelector('input[aria-label^="Your name"]', { timeout: 6000 }).then(() => true, () => false);
      const dbg = await page.evaluate(() => ({ employee: B.employee, ls: (() => { try { return localStorage.getItem('cn.employee'); } catch (_) { return null; } })(), name: CNEmployee.name(), btn: (document.querySelector('[data-r2=eng] .egApproveButton') || {}).outerHTML, nm: !!document.querySelector('[data-nm]'), cnBar: !!document.querySelector('.cnNameBar') }));
      const wrote = net.log.slice(n0).filter(engW(K.O5)).map(x => x.fn + ':' + x.op);
      check(bar && A.dialogs.length === 0 && (await calls()).approve.length === c0.approve.length && !wrote.length, 'no name saved: the Sheet window\'s own bar asks (no browser pop-up), and nothing is approved or written until it is answered: ' + JSON.stringify([bar, A.dialogs, (await calls()).approve.length - c0.approve.length, wrote, dbg]));
      await shot(page, 'real-sheetwin-name', '[data-r2=eng]');
      await page.fill('input[aria-label^="Your name"]', 'Sheet Operator'); await page.click('button[data-nm=go]');
      await page.waitForFunction(() => window.__w.some(s => /Approving… \(off,live,spin\)/.test(s)) || window.__w.some(s => /^approved/.test(s)), null, { timeout: 15000 }).catch(() => {});
      await page.dblclick(WIN + ' .egApproveButton', { force: true, noWaitAfter: true }).catch(() => {});
      await page.evaluate(() => { try { document.querySelector('[data-r2=eng] .egApproveButton').dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true })); } catch (_) {} });
      const okW = await written(K.O5);
      await page.waitForTimeout(800);
      const log = await watched(), cl = (await calls()).approve.slice(c0.approve.length), pr = (await calls()).press.slice(c0.press.length), job = await h.job(K.O5), cw = await h.read(WIN);
      check(okW && cl.length === 1 && cl[0].key === K.O5 && cl[0].by === 'Sheet Operator' && pr.length === 1, 'approved once, under the name typed in the window\'s bar: ' + JSON.stringify([cl, pr]));
      check(log.some(s => /^approve \| Approving… \(off,live,spin\)/.test(s)) && log.some(s => /^approved \| Approved \(off,inert\) \| note: Saving the back file… \+spin/.test(s)), 'the waits are said with a small spinner (preparing, then the back file being saved): ' + JSON.stringify(log));
      check(cw.state === 'approved' && cw.seals.length === 1 && /^BACK ENGRAVING\|Sheet Operator\|/.test(cw.seals[0]) && job.state === 'written' && job.seals.length === 1 && cw.out.length === 0, 'the window shows one BACK ENGRAVING seal under that name and the piece is written: ' + JSON.stringify([cw.state, cw.seals, job.state]));
      const wdWin = await page.evaluate(() => { const d = document.querySelector('dialog.sheetWin[open]') || document.body; return d.innerText + ' ' + [...d.querySelectorAll('[title],[aria-label]')].map(n => (n.getAttribute('title') || '') + ' ' + (n.getAttribute('aria-label') || '')).join(' '); });
      check(!/\blines?\b/i.test(cw.text) && !/still to be settled/i.test(wdWin), 'the Sheet window\'s card and window: Pieces, never "lines"; no "still to be settled" anywhere');
      const srvWr = st.calls.filter(x => x.op === 'backPut' && JSON.stringify(x.body).includes(poolOf(K.O5))).length;
      check(srvWr === 1 && timelineSeals(R.O5, poolOf(K.O5)).length === 1, 'the cloud has one back record and one engraveApproved on the timeline (' + srvWr + ', ' + timelineSeals(R.O5, poolOf(K.O5)).length + ')');
      // the View in Engraving of the same card, from the window
      const o1 = (await calls()).open.length;
      await page.click(WIN + ' [data-e=engrave]');
      await page.waitForFunction(() => CN.S.mode === 'engrave' && document.querySelector('#engraveView .doneRow.open[data-key]'), null, { timeout: 20000 }).catch(() => {});
      const lnd2 = await openLanding();
      check(lnd2.key === K.O5 && lnd2.tab === 'done' && lnd2.dialogs === 0 && (await calls()).open.length === o1 + 1, 'View in Engraving from the Sheet window opens the Decided row of this piece: ' + JSON.stringify(lnd2));
      await page.evaluate(() => CN.setMode('orders'));
      await h.openWin(K.O5); await h.waitState(OV, 'approved');
      const co = await h.read(OV);
      check(co.seals.length === 1 && co.seals[0] === cw.seals[0], 'the order window\'s card has the same seal: ' + JSON.stringify(co.seals));
      await h.closeWin(); net.delayMs = 0;
    });

    // ═══ 7 · the Sheet tab (O9): a repaint during the write, a double click ═══
    await phase('sheettab', async () => {
      console.log('· the Sheet tab of the order window (O9)');
      await asNamed('Test Operator'); net.delayMs = 700;
      await h.openWin(K.O9);
      const toSheet = async () => { await page.click('.owTabsV [data-ow-view="sheet"]'); await page.waitForFunction(() => OrderWin.view() === 'sheet' && OrderWin._sheet() && document.querySelector('#owSheetPanel .owPieces'), null, { timeout: 25000 }); await h.waitState(TAB, 'approve', 25000); };
      await toSheet();
      const o0 = (await calls()).open.length;
      await page.click(TAB + ' [data-e=engrave]');
      await page.waitForFunction(() => CN.S.mode === 'engrave' && document.querySelector('#engraveView .rvItem[data-key]'), null, { timeout: 20000 }).catch(() => {});
      await page.waitForTimeout(500);
      const lnd = await openLanding(), op = (await calls()).open.slice(o0);
      check(op.length === 1 && op[0].rid === R.O9 && op[0].poolId === poolOf(K.O9) && lnd.key === K.O9 && lnd.q === R.O9 && lnd.dialogs === 0 && !lnd.orderWin && A.dialogs.length === 0, 'Fix in Engraving from the Sheet tab opens the Engraving panel on this piece, the order window gone: ' + JSON.stringify([op, lnd]));
      await page.evaluate(() => CN.setMode('orders'));
      await h.openWin(K.O9); await toSheet();
      const n0 = net.log.length, c0 = await calls();
      await watchCard(TAB);
      await page.click(TAB + ' [data-e=approve]');
      // a repaint of the tab and of the window while the approval runs, then Enter and a double click on the very button
      for (let i = 0; i < 3; i++) { await page.evaluate(() => { try { OrderWin.paint(); } catch (_) {} try { Orders.render(); } catch (_) {} try { Engrave.render(); } catch (_) {} try { OrderWin.nudge(); } catch (_) {} }); await page.waitForTimeout(250); }
      await page.evaluate(() => { const b = document.querySelector('#owSheetPanel .egApproveButton'); if (b) b.focus(); });
      await page.keyboard.press('Enter');
      await page.dblclick(TAB + ' .egApproveButton', { force: true, noWaitAfter: true }).catch(() => {});
      const ok9 = await written(K.O9);
      await page.waitForTimeout(1200);
      const log = await watched(), cl = (await calls()).approve.slice(c0.approve.length), pr = (await calls()).press.slice(c0.press.length), job = await h.job(K.O9);
      await h.waitState(TAB, 'approved', 20000); const ct = await h.read(TAB);
      check(ok9 && cl.length === 1 && cl[0].key === K.O9 && pr.length === 1, 'a repaint, Enter and a double click during the write: one approval, one stamp: ' + JSON.stringify([cl.length, pr.length]));
      check(log.some(s => /^approve \| Approving… \(off,live,spin\)/.test(s)) && log.some(s => /note: Saving the back file… \+spin/.test(s)), 'the Sheet tab says the waits with a small spinner: ' + JSON.stringify(log));
      check(ct.state === 'approved' && ct.seals.length === 1 && /^BACK ENGRAVING\|Test Operator\|/.test(ct.seals[0]) && job.seals.length === 1 && job.state === 'written', 'the Sheet tab shows one BACK ENGRAVING seal and the piece is written: ' + JSON.stringify([ct.state, ct.seals, job.state]));
      const wdTab = await page.evaluate(() => { const d = document.getElementById('orderWin'); return d.innerText + ' ' + [...d.querySelectorAll('[title],[aria-label]')].map(n => (n.getAttribute('title') || '') + ' ' + (n.getAttribute('aria-label') || '')).join(' '); });
      check(!/\blines?\b/i.test(ct.text) && !/still to be settled/i.test(wdTab), 'the Sheet tab\'s card and the window around it: Pieces, never "lines"; no "still to be settled" anywhere');
      const srvWr = st.calls.filter(x => x.op === 'backPut' && JSON.stringify(x.body).includes(poolOf(K.O9))).length, wr = net.log.slice(n0).filter(x => x.op === 'backPut').length;
      check(srvWr === 1 && wr === 1 && timelineSeals(R.O9, poolOf(K.O9)).length === 1, 'one back record and one engraveApproved reached the cloud (' + srvWr + ', requests ' + wr + ', ' + timelineSeals(R.O9, poolOf(K.O9)).length + ')');
      await page.click('.owTabsV [data-ow-view="info"]').catch(() => {});
      await h.waitState(OV, 'approved', 20000);
      await page.waitForFunction(s => document.querySelectorAll(s + ' .seal svg[data-seal-model]').length >= 1, OV, { timeout: 15000 }).catch(() => {});
      const co = await h.read(OV);
      check(co.seals.length === 1 && co.seals[0] === ct.seals[0], 'the Overview card of the order window has the same seal: ' + JSON.stringify(co.seals));
      // View in Engraving from the Sheet tab
      await toSheetDone();
      async function toSheetDone() { await page.click('.owTabsV [data-ow-view="sheet"]'); await page.waitForFunction(() => OrderWin.view() === 'sheet' && document.querySelector('#owSheetPanel .owPieces'), null, { timeout: 25000 }); await h.waitState(TAB, 'approved', 20000); }
      await page.click(TAB + ' [data-e=engrave]');
      await page.waitForFunction(() => CN.S.mode === 'engrave' && document.querySelector('#engraveView .doneRow.open[data-key]'), null, { timeout: 20000 }).catch(() => {});
      const lnd2 = await openLanding();
      check(lnd2.key === K.O9 && lnd2.tab === 'done' && lnd2.dialogs === 0, 'View in Engraving from the Sheet tab opens the Decided row of this piece: ' + JSON.stringify(lnd2));
      await page.evaluate(() => CN.setMode('orders')); net.delayMs = 0;
    });

    // ═══ 8 · a card out of date, and the back being prepared (O10) ═══
    await phase('stale', async () => {
      console.log('· an out-of-date card, and the words being prepared (O10)');
      await asNamed('Test Operator'); net.delayMs = 0;
      await h.openWin(K.O10); await h.waitState(OV, 'approve');
      const n0 = net.log.length, c0 = await calls();
      await watchCard(OV);
      await page.evaluate(() => { window.__poke = true; window.__poked = 0; });
      // the piece is sent back for a new placement from another place (the engine then fits the words again by itself: the card passes Being prepared);
      // the card's Approve button is pressed in the same breath, and the off button is pressed while it is prepared
      const r = await page.evaluate(k => { const j = Engrave.items().get(k), b = document.querySelector('#owEng [data-e=approve]'); Engrave.sendBack(j, 'sent back from the Engraving panel'); const s0 = j.state; b.click(); b.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true })); return { state: s0 }; }, K.O10);
      await page.waitForFunction(() => window.__w.some(s => /^(preparing|words)/.test(s)), null, { timeout: 30000 }).catch(() => {});
      await h.waitState(OV, 'approve', 120000);
      await page.waitForTimeout(500);
      const log = await watched(), poked = await page.evaluate(() => { window.__poke = false; return window.__poked; });
      const prep = log.filter(s => /^preparing/.test(s));
      check(r.state === 'words' && prep.length >= 1 && prep.every(s => /Approve engraving \(off,inert\)/.test(s)) && prep.some(s => /why: Not ready to approve: the back is still being prepared\./.test(s)), 'sent back elsewhere: the card says Being prepared, Approve is off and cannot be pressed, and one plain line says why (' + prep.length + ' looks, the off button and the old button pressed ' + poked + ' times): ' + JSON.stringify(prep));
      const cl = (await calls()).approve.slice(c0.approve.length), jn = await h.job(K.O10);
      check(cl.length === 0 && jn.seals.length === 0 && jn.state === 'review' && !net.log.slice(n0).some(engW(K.O10)), 'the button pressed as the piece was sent back (and the off button pressed while it was prepared) reached no approval (' + cl.length + ' calls), sealed nothing, wrote nothing; the card then offers Approve again: ' + JSON.stringify({ calls: cl.length, state: jn.state, seals: jn.seals, wrote: net.log.slice(n0).filter(engW(K.O10)).map(x => x.fn + ':' + x.op) }));
      // a card that is out of date while its button is still there: the placement changed since the card was drawn; pressing it approves nothing and the card is drawn again
      const c1 = await calls(), n2 = net.log.length;
      const st1 = await page.evaluate(k => { const j = Engrave.items().get(k), b = document.querySelector('#owEng [data-e=approve]'); const live = !!b && b.isConnected && !b.disabled; j.fit = Object.assign({}, j.fit); b.click(); return { live, state: j.state }; }, K.O10);
      await page.waitForTimeout(1000);
      const cl2 = (await calls()).approve.slice(c1.approve.length), j2 = await h.job(K.O10), c2 = await h.read(OV);
      check(st1.live && st1.state === 'review' && cl2.length === 0 && j2.seals.length === 0 && !net.log.slice(n2).some(engW(K.O10)) && c2.state === 'approve' && c2.approve && c2.approve.live && !c2.approve.disabled && c2.approve.text === 'Approve engraving', 'a card out of date with its button still there (the placement changed since it was drawn): pressing it approves nothing, the card is drawn again and Approve is offered again: ' + JSON.stringify([st1, cl2.length, j2.seals.length, c2.state, c2.approve]));
      await h.closeWin();
      // a piece whose words still need a decision: the approval called directly does nothing either
      const n1 = net.log.length;
      const d3 = await page.evaluate(async k => { const j = Engrave.items().get(k); await Engrave.approve(j, 'Someone'); return { state: j.state, seals: (j.engravingSeals || []).length }; }, K.O3);
      check(d3.state === 'words' && d3.seals === 0 && !net.log.slice(n1).some(engW(K.O3)), 'and Engrave.approve called directly for the piece whose words are unconfirmed does nothing: ' + JSON.stringify(d3));
    });

    // ═══ 9 · an approval that fails: the checkpoint, the cloud (500), no network, a 409 ═══
    await phase('fail', async () => {
      console.log('· an approval that fails (O6, O7, O8, O11)');
      await asNamed('Test Operator'); net.delayMs = 0;
      const plain = t => !/undefined|\[object|Error:|<|\{"|HTTP \d|forced|ECONN|TypeError|NetworkError|Failed to fetch/i.test(t);
      // (a) the checkpoint on this computer fails: nothing is approved, nothing sealed, nothing written; the same press works once it can be saved
      await h.openWin(K.O6); await h.waitState(OV, 'approve');
      await page.evaluate(() => { window.__idbFail = true; const p = IDBObjectStore.prototype.put; IDBObjectStore.prototype.put = function () { if (window.__idbFail) throw new DOMException('no room left', 'QuotaExceededError'); return p.apply(this, arguments); }; });
      let n0 = net.log.length, c0 = await calls();
      await watchCard(OV);
      await page.click(OV + ' [data-e=approve]');
      await page.waitForFunction(() => document.querySelector('#owEng .egFail'), null, { timeout: 40000 }).catch(() => {});
      await page.waitForTimeout(600);
      let c = await h.read(OV), j = await h.job(K.O6), log = await watched();
      check(c.state === 'approve' && /could not be saved on this browser/.test(c.fail) && plain(c.fail) && c.failRole === 'alert' && c.approve && !c.approve.disabled && c.approve.live && c.approve.text === 'Approve engraving' && !c.approve.spin && c.sealCount === 0, 'checkpoint fails: the card is still To approve, says so in plain words where the button is, and gives the button back: ' + JSON.stringify([c.state, c.fail, c.approve]));
      check(log.some(s => /^approve \| Approving… \(off,live,spin\)/.test(s)) && !log.some(s => /^approved/.test(s)), 'the wait was said with a small spinner, and the card never said Approved: ' + JSON.stringify(log));
      check(j.state === 'review' && j.seals.length === 0 && !j.stamping && !j.preparing && (await calls()).press.length === c0.press.length && !net.log.slice(n0).some(engW(K.O6)), 'no seal was pressed and nothing was written: ' + JSON.stringify(j));
      await shot(page, 'real-failed-checkpoint', '#owEng');
      const fitAt = async (what) => { for (const w of [390, 280]) { await page.setViewportSize({ width: w, height: 900 }); await page.waitForTimeout(500); const g = await h.read(OV); check(g && g.out.length === 0 && g.scrollOver <= 0, `${what} at ${w} px: nothing outside the card (${g && g.w} px wide, outside ${JSON.stringify(g && g.out)}, overflow ${g && g.scrollOver})`); if (w === 280) await shot(page, 'real-' + what.replace(/\W+/g, '-') + '-280', '#orderWin'); } await page.setViewportSize({ width: 1500, height: 1000 }); await page.waitForTimeout(300); };
      await fitAt('the failure line');
      await page.evaluate(() => { window.__idbFail = false; });
      await page.click(OV + ' [data-e=approve]');
      let ok = await written(K.O6); await page.waitForTimeout(600);
      c = await h.read(OV); j = await h.job(K.O6);
      const ap6 = (await calls()).approve.slice(c0.approve.length), pr6 = (await calls()).press.slice(c0.press.length);
      check(ok && c.state === 'approved' && c.fail === '' && c.seals.length === 1 && j.seals.length === 1 && pr6.length === 1 && timelineSeals(R.O6, poolOf(K.O6)).length === 1 && st.calls.filter(x => x.op === 'backPut' && JSON.stringify(x.body).includes(poolOf(K.O6))).length === 1, 'retried once it can be saved: approved, one seal, one stamp, one record in the cloud, the failure line gone (' + ap6.length + ' approve calls)');
      await h.closeWin();

      // (b) the cloud refuses the back file (500): the approval stands on this computer, the card says the file waits and is saved by itself; no second seal
      net.rules.push({ match: (fn, op) => op === 'backPut', status: 500, times: -1, error: 'forced backPut failure' });
      await h.openWin(K.O7); await h.waitState(OV, 'approve');
      n0 = net.log.length; c0 = await calls(); const s7 = srv0();
      await watchCard(OV);
      await page.click(OV + ' [data-e=approve]');
      await page.waitForFunction(k => { const x = Engrave.items().get(k); return x && x.backPending; }, K.O7, { timeout: 60000 }).catch(() => {});
      await page.waitForTimeout(1500);
      c = await h.read(OV); j = await h.job(K.O7); log = await watched();
      check(c.state === 'approved' && c.seals.length === 1 && /waits for the cloud/.test(c.sub) && plain(c.text) && c.fail === '' && c.out.length === 0, 'the cloud refuses (500): the card is Approved with its one seal and says in plain words that the back file waits for the cloud: ' + JSON.stringify([c.state, c.sub, c.seals]));
      check(j.state === 'approved' && j.backPending && j.seals.length === 1 && !st.doc('Charm_Pool_Back', poolOf(K.O7)) && timelineSeals(R.O7, poolOf(K.O7)).length === 1 && (await calls()).press.length - c0.press.length === 1, 'nothing half-written: no back record yet, one approval on the timeline, one stamp: ' + JSON.stringify([j.state, j.seals]));
      await shot(page, 'real-cloud-waits', '#owEng');
      await fitAt('the back file waits note');
      net.rules.length = 0;
      await page.evaluate(() => window.dispatchEvent(new Event('online')));
      ok = await written(K.O7, 90000); await page.waitForTimeout(800);
      c = await h.read(OV); j = await h.job(K.O7);
      check(ok && c.state === 'approved' && c.sub === '' && c.seals.length === 1 && j.seals.length === 1 && !!st.doc('Charm_Pool_Back', poolOf(K.O7)) && st.calls.filter(x => x.op === 'backPut' && JSON.stringify(x.body).includes(poolOf(K.O7))).length === 1 && timelineSeals(R.O7, poolOf(K.O7)).length === 1 && (await calls()).approve.slice(c0.approve.length).length === 1, 'the cloud back: the file is saved by itself, still one seal, one back record, one timeline entry, the approval was called once (' + JSON.stringify(c.sub) + ')');
      await h.closeWin();

      // (c) no network at all
      await h.openWin(K.O8); await h.waitState(OV, 'approve');
      c0 = await calls();
      await A.ctx.setOffline(true);
      await watchCard(OV);
      await page.click(OV + ' [data-e=approve]');
      await page.waitForFunction(k => { const x = Engrave.items().get(k); return x && x.backPending; }, K.O8, { timeout: 60000 }).catch(() => {});
      await page.waitForTimeout(1500);
      c = await h.read(OV); j = await h.job(K.O8);
      check(c.state === 'approved' && c.seals.length === 1 && /waits for the cloud/.test(c.sub) && plain(c.text) && j.seals.length === 1 && (await calls()).press.length - c0.press.length === 1, 'no network: the approval stands on this computer, one seal, and the card says the back file waits for the cloud: ' + JSON.stringify([c.state, c.sub, c.seals]));
      await A.ctx.setOffline(false);
      await page.evaluate(() => window.dispatchEvent(new Event('online')));
      ok = await written(K.O8, 120000); await page.waitForTimeout(800);
      c = await h.read(OV); j = await h.job(K.O8);
      check(ok && c.sub === '' && c.seals.length === 1 && j.seals.length === 1 && st.calls.filter(x => x.op === 'backPut' && JSON.stringify(x.body).includes(poolOf(K.O8))).length === 1 && timelineSeals(R.O8, poolOf(K.O8)).length === 1, 'the network is back: saved by itself, still one seal, one back record, one timeline entry');
      // (at 280 px, with the card saying a wait or a failure: nothing pokes out)
      await h.closeWin();

      // (d) a 409 (the record changed in the cloud): once on the back record, once on the piece's update
      net.rules.push({ match: (fn, op) => op === 'backPut', status: 409, times: 1, error: 'conflict' });
      net.rules.push({ match: (fn, op) => op === 'poolUpdate', status: 409, times: 1, error: 'conflict' });
      await h.openWin(K.O11); await h.waitState(OV, 'approve'); const s11 = srv0();
      c0 = await calls();
      await page.click(OV + ' [data-e=approve]');
      await page.waitForFunction(k => { const x = Engrave.items().get(k); return x && x.backPending; }, K.O11, { timeout: 60000 }).catch(() => {});
      await page.waitForTimeout(1200);
      c = await h.read(OV); j = await h.job(K.O11);
      check(c.state === 'approved' && c.seals.length === 1 && /waits for the cloud/.test(c.sub) && plain(c.text) && j.seals.length === 1, '409 on the back record: the approval stands, one seal, the card says the file waits for the cloud: ' + JSON.stringify([c.state, c.sub, c.seals]));
      for (let i = 0; i < 2; i++) {
        await page.evaluate(() => window.dispatchEvent(new Event('online')));
        if (await written(K.O11, i ? 90000 : 15000)) break;
        c = await h.read(OV); j = await h.job(K.O11);
      }
      ok = await written(K.O11, 90000); await page.waitForTimeout(800);
      c = await h.read(OV); j = await h.job(K.O11);
      const bp = st.calls.slice(s11).filter(x => x.op === 'backPut' && JSON.stringify(x.body).includes(poolOf(K.O11))).length, pu = st.calls.slice(s11).filter(x => x.op === 'poolUpdate' && JSON.stringify(x.body).includes(poolOf(K.O11))).length, tl = timelineSeals(R.O11, poolOf(K.O11)).length, ac = (await calls()).approve.slice(c0.approve.length).length;
      check(ok && c.sub === '' && c.seals.length === 1 && j.seals.length === 1 && bp === 1 && pu === 1 && tl === 1 && ac === 1, 'after the 409s the retry finished it: written, one seal, one back record, one update, one timeline entry, the approval called once: ' + JSON.stringify({ ok, sub: c.sub, seals: c.seals.length, jobSeals: j.seals.length, backPut: bp, poolUpdate: pu, timeline: tl, approveCalls: ac, state: j.state, pending: j.backPending, rules: net.rules.map(r => r.times) }));
      net.rules.length = 0;
      await h.closeWin();
    });

    // ═══ 10 · the card at 700, 390 and 280 px, and Paul's words ═══
    await phase('widths', async () => {
      console.log('· widths and words');
      const states = [[K.O10, OV, 'approve'], [K.O3, OV, 'words'], [K.O2, OV, 'approved']];
      for (const w of [700, 390, 280]) {
        await page.setViewportSize({ width: w, height: 900 });
        for (const [key, sel, state] of states) {
          await h.openWin(key); if (!(await h.waitState(sel, state, 25000))) { check(false, `${state} card at ${w} px did not draw`); await h.closeWin(); continue; }
          await page.waitForTimeout(500);
          const c = await h.read(sel);
          const text = await page.evaluate(() => { const win = document.getElementById('orderWin'), t = [win.innerText]; for (const n of win.querySelectorAll('[title],[aria-label],[alt],[placeholder]')) for (const a of ['title', 'aria-label', 'alt', 'placeholder']) if (n.getAttribute(a)) t.push(n.getAttribute(a)); const card = [...document.querySelectorAll('#owEng .swEng')].map(e => e.innerText + ' ' + [...e.querySelectorAll('[title],[aria-label]')].map(n => (n.getAttribute('title') || '') + ' ' + (n.getAttribute('aria-label') || '')).join(' ')).join('\n'); return { win: t.join('\n'), card, over: win.scrollWidth - win.clientWidth }; });
          check(c.out.length === 0 && c.scrollOver <= 0 && c.w > 0, `${state} card at ${w} px: nothing outside its box, no sideways scroll (card ${c.w} px wide, outside the card: ${JSON.stringify(c.out)}, the card's own overflow ${c.scrollOver})`);
          if (text.over > 1) console.log(`  – (the order window itself is ${text.over} px wider than its box at ${w} px with the ${state} card: not the card's)`);
          check(!/\blines?\b/i.test(text.card) && !/still to be settled/i.test(text.win), `${state} card at ${w} px: Pieces, never "lines"; no "still to be settled" anywhere in the order window`);
          if (w === 280 || w === 390) await shot(page, `real-${state}-${w}`, '#orderWin');
          await h.closeWin();
        }
      }
      await page.setViewportSize({ width: 1500, height: 1000 });
    });

    // ═══ 10b · the words confirmed in Engraving (the panel's own Confirm: no paid model), then approved from the card (O3) ═══
    await phase('confirmed', async () => {
      console.log('· the words confirmed in Engraving, then Approve engraving on the card (O3)');
      await asNamed('Test Operator'); net.delayMs = 0;
      await h.openWin(K.O3); await h.waitState(OV, 'words');
      await watchCard(OV);
      const c0 = await calls(), n0 = net.log.length;
      await page.evaluate(k => { Engrave.decideWords(Engrave.items().get(k), { text: 'WHICH ONE', note: 'confirmed' }); }, K.O3);
      await h.waitState(OV, 'approve', 120000);
      await page.waitForTimeout(500);
      const log = await watched(), meta = await page.evaluate(() => (document.querySelector('#owEng .egMeta') || {}).textContent || ''), c = await h.read(OV);
      check(log[0].startsWith('words') && c.state === 'approve' && c.approve && c.approve.live && !c.approve.disabled && c.words.replace(/\s+/g, ' ') === 'WHICH ONE' && /Words confirmed by Test Operator/.test(meta), 'confirmed in Engraving: the card went from Words to confirm to To approve with Approve engraving live, and says who confirmed the words: ' + JSON.stringify([log.map(s => s.split(' | ')[0]), c.state, c.words, meta]));
      await page.click(OV + ' [data-e=approve]');
      const ok = await written(K.O3); await page.waitForTimeout(600);
      const job = await h.job(K.O3), cc = await h.read(OV), cl = (await calls()).approve.slice(c0.approve.length);
      check(ok && cl.length === 1 && cl[0].key === K.O3 && job.seals.length === 1 && job.text.replace(/\s+/g, ' ') === 'WHICH ONE' && cc.state === 'approved' && cc.seals.length === 1 && cc.words.replace(/\s+/g, ' ') === 'WHICH ONE' && st.calls.filter(x => x.op === 'backPut' && JSON.stringify(x.body).includes(poolOf(K.O3))).length === 1 && timelineSeals(R.O3, poolOf(K.O3)).length === 1, 'Approve engraving on the confirmed words: one approval, one seal, the words written are the confirmed ones, one back record, one timeline entry: ' + JSON.stringify([cl.length, job.seals, cc.state, cc.words]));
      await h.closeWin();
    });

    // ═══ 11 · a reload keeps an approval, and writes nothing again ═══
    await phase('reload', async () => {
      console.log('· a reload keeps the approval (O2)');
      const n0 = net.log.length, ref = await h.job(K.O2);
      await page.reload();
      await page.waitForFunction(() => window.CN && CN.S && window.RunCtl && window.Engrave && CN.S.cloud.ok !== null && window.B && B.orders && B.orders.rows.length > 0, null, { timeout: 90000 });
      await page.evaluate(() => CN.setMode('orders'));
      const back = await page.waitForFunction(k => { const j = Engrave.items().get(k); return j && ['approved', 'written'].includes(j.state); }, K.O2, { timeout: 90000 }).then(() => true, () => false);
      await page.waitForTimeout(4000);
      const job = await h.job(K.O2);
      check(back && job && job.seals.join() === ref.seals.join() && job.seals.length === 1, 'after a reload the piece is still approved, with the same single seal: ' + JSON.stringify(job));
      await h.openWin(K.O2); const ok = await h.waitState(OV, 'approved', 40000); const c = await h.read(OV);
      check(ok && c.seals.length === 1 && c.seals[0].replace(/^BACK ENGRAVING\|/, 'engraveApproved|') === ref.seals[0], 'its card is green Approved with the same seal: ' + JSON.stringify([c.state, c.seals]));
      const again = net.log.slice(n0).filter(x => ['backPut', 'poolUpdate'].includes(x.op) && JSON.stringify(x.body).includes(poolOf(K.O2)));
      check(again.length === 0 && timelineSeals(R.O2, poolOf(K.O2)).length === 1, 'the reload wrote the approval nowhere again (' + again.length + ' writes; one engraveApproved on the timeline)');
      await h.closeWin();
      await wrap(page);
    });

    // ═══ 12 · production and the Sandbox never meet: a Sandbox card approves only the Sandbox's records, and a production card only production's (O12) ═══
    await phase('sandbox', async () => {
      if (process.env.SKIP_SANDBOX) return console.log('  – the Sandbox phase is skipped (SKIP_SANDBOX)');
      console.log('· the Sandbox card approves only the Sandbox\'s records (O12)');
      let S = null; SERIAL.on = true;
      try { S = await boot('sandbox', { sandbox: true }); } catch (e) { check(false, 'the Sandbox page did not reach the engraving review: ' + (e && e.message)); }
      if (S) {
        const sp = S.page, sh = H(sp), pn = SBOX => (SBOX ? 'Sandbox_' : '') + 'Order_Timeline';
        const on = await sp.evaluate(() => CN.S.settings.sandbox === 'on' && !!document.body.innerText);
        check(on, 'the second page is in Sandbox mode');
        await sp.evaluate(() => { B.employee = 'Sandbox Operator'; try { localStorage.setItem('cn.employee', 'Sandbox Operator'); } catch (_) {} });
        await sh.openWin(K.O12); await sh.waitState(OV, 'approve', 40000);
        const cardIn = await sh.read(OV);
        check(cardIn && cardIn.state === 'approve' && cardIn.words === 'LILY', 'the Sandbox page has its own card for the order, To approve: ' + JSON.stringify(cardIn && [cardIn.state, cardIn.words]));
        const d0 = docsNow(), b0 = new Set(st.blobs.keys()), s0 = st.calls.length, nSb0 = S.net.log.length;
        await sp.click(OV + ' [data-e=approve]');
        const okS = await sp.waitForFunction(k => { const j = Engrave.items().get(k); return j && j.state === 'written' && !j.backSaving && !j.backPending; }, K.O12, { timeout: 120000 }).then(() => true, () => false);
        await sp.waitForTimeout(1200);
        const d1 = docsNow(), dd = diffDocs(d0, d1), c = await sh.read(OV), j = await sh.job(K.O12);
        // (a production record counts as touched when it is this order's, or a sheet or run that now speaks of this piece's back: the idle production page's own upkeep is not that)
        const mentions = k => k.includes(poolOf(K.O12)) || k.includes(R.O12) || (/^(Charm_Nest_Sheets|Charm_Nest_Runs)\//.test(k) && JSON.stringify(d1.get(k) || {}).includes(poolOf(K.O12)));
        const prodTouched = dd.filter(([, k]) => !/^Sandbox_/.test(k) && /^(Charm_Pool|Charm_Pool_Back|Order_Timeline|Charm_Nest_Sheets|Charm_Nest_Runs)\//.test(k) && mentions(k));
        const sandTouched = dd.filter(([, k]) => /^Sandbox_(Charm_Pool|Charm_Pool_Back|Order_Timeline|Charm_Nest_Sheets)\//.test(k));
        const newBlobs = [...st.blobs.keys()].filter(k => !b0.has(k));
        check(okS && c.state === 'approved' && c.seals.length === 1 && /^BACK ENGRAVING\|Sandbox Operator\|/.test(c.seals[0]) && j.seals.length === 1, 'the Sandbox card approved: Approved, one BACK ENGRAVING seal, written: ' + JSON.stringify([c.state, c.seals, j.state]));
        const isSb = x => [true, 1, '1'].includes(x.body && x.body.sandbox), libW = x => x.fn === 'charmNestLibrary' && ['backPut', 'poolUpdate', 'timelineAdd', 'runPut', 'putSheet'].includes(x.op);
        const sbW = S.net.log.slice(nSb0).filter(libW);
        check(sbW.length >= 4 && sbW.every(isSb), 'every write the Sandbox page made for the approval says it is the Sandbox\'s (' + sbW.length + ' writes: ' + sbW.map(x => x.op + (isSb(x) ? ':sandbox' : ':PRODUCTION')).join(' ') + ')');
        check(prodTouched.length === 0 && sandTouched.length >= 3, 'only Sandbox records changed (' + sandTouched.length + ' Sandbox records: ' + sandTouched.map(([h2, k]) => k.replace(/\/.*/, '')).join(', ') + '); no production pool, back, timeline or sheet record was touched' + (prodTouched.length ? ': ' + JSON.stringify(prodTouched) : ''));
        check(newBlobs.length >= 2 && newBlobs.every(k => /charmnest\/sandbox\//.test(k) || /sandbox/i.test(k)), 'every new file is under the Sandbox\'s own path: ' + JSON.stringify(newBlobs.slice(0, 4)));
        check(!st.doc('Charm_Pool_Back', poolOf(K.O12)) && !!st.doc('Sandbox_Charm_Pool_Back', poolOf(K.O12)) && st.list('Order_Timeline').filter(d => String(d.orderId) === R.O12 && d.type === 'engraveApproved').length === 0 && st.list('Sandbox_Order_Timeline').filter(d => String(d.orderId) === R.O12 && d.type === 'engraveApproved').length === 1, 'the real order\'s records have no approval; the Sandbox copy has exactly one');
        const prodJob = await page.evaluate(k => { const x = Engrave.items().get(k); return x && { state: x.state, seals: (x.engravingSeals || []).length }; }, K.O12);
        check(prodJob && prodJob.state === 'review' && prodJob.seals === 0, 'the production page still shows that piece To approve, with no seal: ' + JSON.stringify(prodJob));
        // and the other way: the same piece approved on the production page writes production's records only, and leaves the Sandbox's seal as it is
        const sSeal = (await sh.read(OV)).seals;
        await h.openWin(K.O12); await h.waitState(OV, 'approve', 25000);
        await asNamed('Test Operator');
        const e0 = docsNow(), nP0 = net.log.length;
        await page.click(OV + ' [data-e=approve]');
        const okP = await written(K.O12, 120000); await page.waitForTimeout(1000);
        const pW = net.log.slice(nP0).filter(libW);
        check(pW.length >= 4 && pW.every(x => !isSb(x)), 'every write the production page made for its approval says it is production\'s (' + pW.length + ' writes: ' + pW.map(x => x.op + (isSb(x) ? ':SANDBOX' : '')).join(' ') + ')');
        const dd2 = diffDocs(e0, docsNow()), sandTouched2 = dd2.filter(([, k]) => /^Sandbox_/.test(k) && /(Charm_Pool|Charm_Pool_Back|Order_Timeline|Charm_Nest_Sheets)\//.test(k));
        check(okP && sandTouched2.length === 0 && !!st.doc('Charm_Pool_Back', poolOf(K.O12)) && st.list('Order_Timeline').filter(d => String(d.orderId) === R.O12 && d.type === 'engraveApproved').length === 1 && st.list('Sandbox_Order_Timeline').filter(d => String(d.orderId) === R.O12 && d.type === 'engraveApproved').length === 1, 'the production card approved production\'s record only: no Sandbox record changed (' + sandTouched2.length + '), one approval on each timeline');
        await h.closeWin();
        const sSeal2 = await sh.read(OV);
        check(JSON.stringify(sSeal2.seals) === JSON.stringify(sSeal), 'the Sandbox card still has its own single seal: ' + JSON.stringify(sSeal2.seals));
        const pops = S.dialogs.filter(d => !/^beforeunload/.test(d));   // (the page's own "leave this page?" when it reloads itself into the Sandbox is not a pop-up on a window)
        check(pops.length === 0, 'no browser pop-up on the Sandbox page: ' + JSON.stringify(S.dialogs));
        await S.ctx.close().catch(() => {});
      }
      SERIAL.on = false;
    });
  } finally { await browser.close(); srv.close(); }
  check(errors.length === 0, 'no page errors: ' + errors.join(' | '));
  if (fails.length) { console.error('\n' + fails.length + ' failed:\n - ' + fails.join('\n - ')); process.exit(1); }
  console.log('order-engraving-approve-real: ok');
})().catch(e => { console.error(e); process.exit(1); });
