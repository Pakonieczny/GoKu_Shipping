// Who is working is taken from the sign-in, automatically; the Sorter never asks for a name in a browser box (Paul, 6 Oct 2026, 23:28 UTC:
// "Why is it asking me to record my name here, it should all be automatic based on my login credentials").
// Real charm-nest-1.html (bridge, name bar, Review, Engraving, order window, Hold, Cancel), the real station-session.js / charm-nest-role.js,
// over the fake site (bridge-server.cjs); the stations' door (firebaseOrders: sessions, events, the Admin question) is this test's own recorder.
// Nothing leaves the machine, no PIN is typed, no paid model and no Etsy call. EVERY page has a `dialog` listener: any browser pop-up fails the test.
//   A · the Engraving work, on a whole Auto set (the real run, like order-engraving-approve-real.cjs)
//       signed in (the Admin): Engrave these words and Approve engraving go straight through, under the name, no bar, no pop-up
//       nobody signed in: the inline name bar (kind ask) opens with the last name ready and "Continue as <name>"; one tap, and Engrave these
//       words / Approve engraving go on by themselves (no second press); Esc puts it away and the words typed stay as typed
//       after an IDLE sign-out of a non-Admin (cause of the empty name: the name is cleared by the idle rule): the same, with "Continue as"
//       CAUSE (reproduced): a typed name that is not the Admin's is cleared after 10 minutes without input; cn.lastEmployee keeps it for the chip
//   B · Review's Complete Order, Hold, Cancel Order and a message in the order window (the Review / Hold fixture of hold-ui.cjs)
//       signed in: no field, no bar, no pop-up, the name is on the record; nobody signed in: the card's own field or the bar, the last name ready, the press goes on
//   C · when the Sorter opens with nobody signed in the calm name hint is already there (not first at the first approval), with the chip
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/auto-name.cjs      (ONLY=A,B,C)
const fs = require('fs'), path = require('path'), os = require('os');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const { buildMaster } = require('./fixture-master.cjs');
const sleep = ms => new Promise(r => setTimeout(r, ms));
const only = (process.env.ONLY || '').split(',').filter(Boolean), want = n => !only.length || only.includes(n);
const fails = [], check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
const errors = [], dialogsAll = [];
const ADMIN = 'Paul K', STAFF = 'Tess Welder';

// ───────────────────────────── the stations' door, as the test's own recorder ─────────────────────────────
const door = { admins: ['paul k', 'paul'] };
async function routeDoor(context, rec) {
  const ok = body => ({ status: 200, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify(body) });
  await context.route(u => /\/\.netlify\/functions\/firebaseOrders/.test(u.pathname), async r => {
    let j = null; try { j = JSON.parse(r.request().postData() || 'null'); } catch (_) {}
    if (r.request().method() === 'POST' && j && typeof j.stationAdmin === 'string') return r.fulfill(ok({ ok: true, admin: door.admins.includes(String(j.stationAdmin).replace(/[._]/g, ' ').replace(/\s+/g, ' ').trim().toLowerCase()) }));
    if (r.request().method() === 'POST' && j && (Array.isArray(j.activity) || j.session || j.live)) {
      if (Array.isArray(j.activity)) for (const e of j.activity) rec.events.push(e);
      if (j.session) rec.sessions.push(j.session);
      return r.fulfill(ok({ success: true, written: 1 }));
    }
    return r.fallback();
  });
}
const watchDialogs = (page, label) => page.on('dialog', d => { if (d.type() === 'beforeunload') { d.accept().catch(() => {}); return; } dialogsAll.push(label + ' ' + d.type() + ': ' + d.message().slice(0, 80)); d.dismiss().catch(() => {}); });
const skewInit = () => { const real = Date.now.bind(Date); window.__skew = 0; Date.now = () => real() + (window.__skew || 0); };

// ═════════════════════════════════ A · the Engraving work, on a whole Auto set ═════════════════════════════════
const day = Math.floor(Date.now() / 1000);
const tx = (rid, i, sku, extra = {}) => Object.assign({ transaction_id: Number(`${rid}${i}`), listing_id: 1718000 + i, receipt_id: rid, sku, title: `${sku} charm`, quantity: 1, expected_ship_date: day + 86400, variations: [{ formatted_name: 'Metal', formatted_value: '14k Gold Filled' }] }, extra);
const pers = text => ({ is_personalized: true, variations: [{ formatted_name: 'Metal', formatted_value: 'Sterling Silver' }, { formatted_name: 'Personalization', formatted_value: text }] });
const receipt = (rid, txs) => ({ receipt_id: rid, order_number: rid, name: 'Buyer ' + rid, country_iso: 'US', city: 'Austin', message_from_buyer: '', update_timestamp: day, create_timestamp: day - 3600, status: 'Paid', is_shipped: false, transactions: txs });
// W1..W3: words that need a decision (signed in / nobody signed in / after an idle sign-out) · C1..C3: words to approve · M: the order window's message
const R = { W1: '3522100001', W2: '3522100002', W3: '3522100003', C1: '3522100004', C2: '3522100005', C3: '3522100006', M: '3522100007' };
const receipts = [
  receipt(R.W1, [tx(R.W1, 1, 'BR-TST-01', pers('WHICH ONE?'))]), receipt(R.W2, [tx(R.W2, 1, 'BR-TST-02', pers('WHICH ONE?'))]), receipt(R.W3, [tx(R.W3, 1, 'BR-TST-03', pers('WHICH ONE?'))]),
  receipt(R.C1, [tx(R.C1, 1, 'BR-TST-04', pers('CARLA'))]), receipt(R.C2, [tx(R.C2, 1, 'BR-TST-01', pers('DORA'))]), receipt(R.C3, [tx(R.C3, 1, 'BR-TST-02', pers('EDNA'))]),
  receipt(R.M, [tx(R.M, 1, 'BR-TST-03', pers('FAYE'))])
];
const K = Object.fromEntries(Object.entries(R).map(([n, rid]) => [n, rid + '_' + rid + '1']));
const agentResults = { engraveIntent: o => {
  const txt = String(o.messages[0].content[0].text), m = /Personalisation field: (\[.*?\])/.exec(txt); let p = []; try { p = JSON.parse(m ? m[1] : '[]'); } catch (_) {}
  const req = { side: 'back', font: null, handwriting: false, image: false };
  if (p.some(x => /WHICH ONE/.test(x))) return { engrave: false, text: '', source: 'none', sourceQuote: '', requests: req, questions: ['Which of the two names is wanted?'], confidence: 0.3 };
  const e = p.length > 0; return { engrave: e, text: p.join('\n'), source: e ? 'personalization' : 'none', sourceQuote: p[0] || '', requests: req, questions: [], confidence: e ? 0.97 : 0.95 };
} };

async function partA(browser) {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cn-autoname-')), masterPath = path.join(tmp, 'BRITES-master.ai');
  await buildMaster(masterPath, { count: 4, edge: false });
  const srv = await start({ receipts, agentResults });
  const { st, sorterOrigin, stationOrigin } = srv;
  const rec = { events: [], sessions: [] };
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 1000 } });
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
  await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
  await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
  await ctx.route(/^https:\/\/firebasestorage\.googleapis\.com\//, r => { const u = new URL(r.request().url()), m = /\/o\/(.+)$/.exec(u.pathname), key = m ? decodeURIComponent(m[1]) : '', b = st.blobs.get(key); if (!b) return r.fulfill({ status: 404, body: 'no blob ' + key }); return r.fulfill({ status: 200, contentType: 'application/octet-stream', headers: { 'Access-Control-Allow-Origin': '*', 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: Buffer.from(b) }); });
  await routeDoor(ctx, rec);
  const posts = [];     // every call to a function with a body (the messages are found here)
  await ctx.route(/\/\.netlify\/functions\//, r => { const q = r.request(); if (q.method() === 'POST') { let b = null; try { b = JSON.parse(q.postData() || 'null'); } catch (_) {} posts.push({ fn: new URL(q.url()).pathname.split('/').pop(), body: b }); } return r.fallback(); });
  await ctx.addInitScript(({ station, sorter }) => {
    if (location.origin === station) { localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200)); localStorage.setItem('employee_name', 'Tester'); }
    if (location.origin === sorter) { if (!sessionStorage.getItem('__named')) { localStorage.setItem('cn.employee', 'Paul K'); sessionStorage.setItem('__named', '1'); } localStorage.setItem('cn.tour.seen', '1'); }
    window.confirm = () => true; window.alert = () => {};
    const real = Date.now.bind(Date); window.__skew = 0; Date.now = () => real() + (window.__skew || 0);
  }, { station: stationOrigin, sorter: sorterOrigin });
  const page = await ctx.newPage(); page.setDefaultTimeout(30000); watchDialogs(page, 'A');
  page.on('pageerror', e => { errors.push('A: ' + e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 3).join(' | ')); });
  await page.goto(`${sorterOrigin}/charm-nest-1.html`);
  await page.waitForFunction(() => window.CN && window.CN.S && window.RunCtl && window.DesignLink && window.CNRole && window.StationSession && CN.S.cloud.ok !== null, null, { timeout: 60000 });
  await page.evaluate(station => { const s = CN.S.settings; s.dsOrigin = station; s.engine = 'solver'; s.budgetS = 8; s.review = 'off'; s.naming = 'off'; s.notify = 'off'; s.sound = 'off'; s.autoCommit = 'on'; s.runMode = 'manual'; s.pullMode = 'all'; s.heartbeatS = 2; CN.saveSettings && CN.saveSettings(); }, stationOrigin);
  await page.evaluate(() => CN.setMode('master')); await page.waitForSelector('#mFile', { state: 'attached' }); await page.setInputFiles('#mFile', masterPath);
  await page.waitForFunction(() => { const j = [...B.master.jobs.values()][0]; return j && ['done', 'error'].includes(j.state); }, null, { timeout: 120000 });
  await page.evaluate(() => CN.setMode('design')); await page.evaluate(() => DesignLink.ensure());
  await page.evaluate(() => CN.setMode('orders')); await page.evaluate(() => RunCtl.setMode('auto'));
  const t0 = Date.now();
  for (;;) {
    const r = await page.evaluate(() => B.run && { status: B.run.status, step: B.run.step }), jobs = await page.evaluate(() => [...Engrave.items().values()].map(j => j.state));
    if (r && r.status === 'stopped') throw new Error('the run stopped: ' + JSON.stringify(await page.evaluate(() => ({ by: B.run.stoppedBy, fix: B.run.fix }))));
    if (r && r.status === 'processed' && jobs.length >= Object.keys(R).length && !jobs.some(s => ['ready', 'classify', 'fitting', 'reclassify'].includes(s))) break;
    if (Date.now() - t0 > 420000) throw new Error('the run never reached the engraving review: ' + JSON.stringify([r, jobs]));
    await page.waitForTimeout(500);
  }
  const jobOf = k => page.evaluate(k => { const j = Engrave.items().get(k); return j ? { state: j.state, by: j.approvedBy || (j.decision && j.decision.by) || '', text: j.text || '', seals: (j.engravingSeals || []).map(s => `${s.how}|${s.by}`) } : null; }, k);
  const bar = () => page.evaluate(() => { const b = document.querySelector('.cnNameBar[data-kind="ask"], .cnNameBar[data-kind="edit"], .cnNameBar[data-kind="hint"]'); return b ? { kind: b.dataset.kind, chip: (b.querySelector('.cnNbCont:not([hidden])') || {}).textContent || '', input: b.querySelector('input') ? b.querySelector('input').value : null, inWin: !!b.closest('dialog') } : null; });
  const asName = n => page.evaluate(n => { B.employee = n; }, n);
  const signOutName = () => page.evaluate(() => { try { localStorage.removeItem('cn.employee'); } catch (_) {} B.employee = ''; });
  const focusJob = async (k, rid) => { await page.evaluate(({ k, rid }) => { CN.setMode('engrave'); Engrave.restoreView(Object.assign({}, Engrave.view(), { tab: 'place', focus: k, list: false, chosen: true, q: rid, openDone: null })); Engrave.render(); }, { k, rid }); };
  const useBtn = k => `#egQueue .rvItem[data-key="${k}"] [data-a=usewords]`;
  const apprBtn = k => `#egQueue .rvItem[data-key="${k}"] [data-a=approve]`;
  const pressUse = async (k, rid, words) => {
    await focusJob(k, rid);
    await page.waitForSelector(useBtn(k), { timeout: 20000 });
    await page.fill(`#egQueue .rvItem[data-key="${k}"] [data-f=words]`, words);
    await page.click(useBtn(k));
  };
  const states = await page.evaluate(() => Object.fromEntries([...Engrave.items().values()].map(j => [j.key, j.state])));
  check(['W1', 'W2', 'W3'].every(n => states[K[n]] === 'words') && ['C1', 'C2', 'C3', 'M'].every(n => states[K[n]] === 'review'), 'the real run reached the engraving review (3 words to decide, 4 to approve): ' + JSON.stringify(Object.fromEntries(Object.entries(states).map(([k, v]) => [k.slice(-4), v]))));

  if (want('A')) {
    console.log('· A1: signed in (the Admin): Engrave these words, Approve engraving, no bar, no pop-up');
    await asName(ADMIN); await page.waitForTimeout(600);
    check(await page.evaluate(() => CNRole.admin()), 'the Admin answer came from the door: ' + ADMIN + ' is the Admin (no Laser or Design question)');
    await pressUse(K.W1, R.W1, 'ROSA');
    await page.waitForFunction(k => { const j = Engrave.items().get(k); return j && j.state !== 'words'; }, K.W1, { timeout: 40000 });
    check(!(await bar()) && (await jobOf(K.W1)).by === ADMIN + '.', `Engrave these words: no bar, the decision names the person: ${JSON.stringify(await jobOf(K.W1))}`);
    await focusJob(K.C1, R.C1); await page.waitForFunction(s => { const b = document.querySelector(s); return b && !b.disabled; }, apprBtn(K.C1), { timeout: 40000 });
    await page.click(apprBtn(K.C1));
    await page.waitForFunction(k => { const j = Engrave.items().get(k); return j && ['approved', 'written'].includes(j.state); }, K.C1, { timeout: 60000 });
    const c1 = await jobOf(K.C1);
    check(!(await bar()) && c1.by === ADMIN + '.' && c1.seals.length >= 1 && c1.seals.every(s => s.includes(ADMIN + '.')), 'Approve engraving: no bar, the seal names the person: ' + JSON.stringify(c1));
    check(await page.evaluate(() => localStorage.getItem('cn.lastEmployee')) === ADMIN + '.', 'the last name is kept for next time (cn.lastEmployee)');

    console.log('· A2: nobody signed in: the inline bar, last name ready, Continue as <name>, the press goes on by itself');
    await signOutName();
    check(await page.evaluate(() => CNEmployee.name()) === '' && await page.evaluate(() => localStorage.getItem('cn.employee')) === null, 'no name saved');
    await pressUse(K.W2, R.W2, 'SARA');
    await page.waitForSelector('.cnNameBar[data-kind="ask"]', { timeout: 8000 });
    let b = await bar();
    check(b && b.kind === 'ask' && b.chip === 'Continue as ' + ADMIN + '.' && b.input === ADMIN + '.', 'Engrave these words with no name: the inline bar (kind ask) opens with the last name ready and the chip: ' + JSON.stringify(b));
    check((await jobOf(K.W2)).state === 'words', 'nothing was decided before a name');
    await page.click('.cnNameBar .cnNbCont');
    await page.waitForFunction(k => { const j = Engrave.items().get(k); return j && j.state !== 'words'; }, K.W2, { timeout: 40000 });
    check(!(await bar()) && (await jobOf(K.W2)).by === ADMIN + '.', 'one tap on Continue as: the decision went through by itself, no second press, under the name: ' + JSON.stringify(await jobOf(K.W2)));
    await signOutName();
    await focusJob(K.C2, R.C2); await page.waitForFunction(s => { const x = document.querySelector(s); return x && !x.disabled; }, apprBtn(K.C2), { timeout: 40000 });
    await page.click(apprBtn(K.C2));
    await page.waitForSelector('.cnNameBar', { timeout: 8000 }); b = await bar();
    check(b && /^Continue as /.test(b.chip), 'Approve engraving with no name: the bar with Continue as: ' + JSON.stringify(b));
    check(!['approved', 'written'].includes((await jobOf(K.C2)).state), 'not approved before a name');
    await page.click('.cnNameBar .cnNbCont');
    await page.waitForFunction(k => { const j = Engrave.items().get(k); return j && ['approved', 'written'].includes(j.state); }, K.C2, { timeout: 60000 });
    const c2 = await jobOf(K.C2);
    check(c2.by === ADMIN + '.' && c2.seals.length >= 1 && c2.seals.every(s => s.includes(ADMIN + '.')), 'the approval went through by itself after one tap, the seal names the person: ' + JSON.stringify(c2));

    console.log('· A3: Esc puts the bar away calmly: nothing decided, the words typed stay as typed');
    await signOutName();
    await focusJob(K.W3, R.W3); await page.waitForSelector(useBtn(K.W3), { timeout: 20000 });
    await page.fill(`#egQueue .rvItem[data-key="${K.W3}"] [data-f=words]`, 'TYPED WORDS');
    await page.click(useBtn(K.W3)); await page.waitForSelector('.cnNameBar[data-kind="ask"]', { timeout: 8000 });
    await page.keyboard.press('Escape'); await page.waitForFunction(() => !document.querySelector('.cnNameBar'), null, { timeout: 4000 });
    await page.waitForTimeout(500);
    const kept = await page.evaluate(k => { const t = document.querySelector(`#egQueue .rvItem[data-key="${k}"] [data-f=words]`); return t ? t.value : null; }, K.W3);
    check(kept === 'TYPED WORDS' && (await jobOf(K.W3)).state === 'words' && !(await page.evaluate(() => CNEmployee.name())), 'Esc: no decision, no name, the typed words are still in the box: ' + JSON.stringify([kept, await jobOf(K.W3)]));

    console.log('· A4: CAUSE of the empty name: a non-Admin\'s name is cleared by the idle sign-out; the chip brings it back');
    await asName(STAFF); await page.waitForSelector('.cnNameBar[data-kind="role"]', { timeout: 8000 });
    await page.click('.cnNameBar[data-kind="role"] [data-role="design"]');
    await page.waitForFunction(() => StationSession.current() && StationSession.current().person === 'Tess Welder', null, { timeout: 8000 });
    check(await page.evaluate(() => CNEmployee.name()) === STAFF && !(await page.evaluate(() => CNRole.admin())), STAFF + ' is signed in and is not the Admin (Design role, the 10 minute rule)');
    await page.evaluate(() => { window.__skew = 11 * 60 * 1000; });        // eleven minutes without input pass (the clock of the page)
    await page.waitForFunction(() => !CNEmployee.name(), null, { timeout: 30000 }).catch(() => {});
    check(await page.evaluate(() => CNEmployee.name()) === '' && await page.evaluate(() => localStorage.getItem('cn.employee')) === null, 'after 10 minutes without input the idle sign-out cleared the name (the cause: a name that is not the Admin\'s)');
    check(await page.evaluate(() => localStorage.getItem('cn.lastEmployee')) === STAFF, 'cn.lastEmployee still has the name for the chip');
    await focusJob(K.W3, R.W3); await page.waitForSelector(useBtn(K.W3), { timeout: 20000 });
    await page.fill(`#egQueue .rvItem[data-key="${K.W3}"] [data-f=words]`, 'UNA');
    await page.click(useBtn(K.W3)); await page.waitForSelector('.cnNameBar[data-kind="ask"]', { timeout: 8000 });
    b = await bar();
    check(b && b.chip === 'Continue as ' + STAFF, 'after the idle sign-out the next press shows the inline bar with Continue as: ' + JSON.stringify(b));
    await page.click('.cnNameBar .cnNbCont');
    await page.waitForFunction(k => { const j = Engrave.items().get(k); return j && j.state !== 'words'; }, K.W3, { timeout: 40000 });
    check((await jobOf(K.W3)).by === STAFF, 'one tap and the decision was made under the name: ' + JSON.stringify(await jobOf(K.W3)));
    // the Admin is never cleared by the idle rule (only midnight does): same eleven minutes
    await signOutName(); await asName(ADMIN); await page.waitForTimeout(800);
    await page.evaluate(() => { window.__skew = 22 * 60 * 1000; }); await page.waitForTimeout(13000);
    check(await page.evaluate(() => CNEmployee.name()) === ADMIN + '.', 'the Admin stays signed in through a long quiet time (not cleared by the idle rule)');
    await page.evaluate(() => { window.__skew = 0; });
  }

  if (want('A')) {
    console.log('· A5: a message in the order window');
    await asName(ADMIN); await page.evaluate(() => { CN.setMode('orders'); });
    const sendMsg = async (text) => {
      await page.evaluate(k => OrderWin.open(k), K.M); await page.waitForFunction(k => OrderWin.isOpen() && OrderWin.key() === k, K.M, { timeout: 15000 });
      await page.waitForSelector('#owInput', { state: 'visible', timeout: 15000 });
      await page.fill('#owInput', text); await page.waitForFunction(() => !document.getElementById('owSend').disabled, null, { timeout: 5000 });
      await page.click('#owSend');
    };
    const sent = text => posts.filter(p => p.body && JSON.stringify(p.body).includes(text));
    await sendMsg('hello signed in'); await page.waitForTimeout(1500);
    check(!(await bar()) && sent('hello signed in').length >= 1 && JSON.stringify(sent('hello signed in')).includes(ADMIN + '.'), 'signed in: the message went out under the name, no bar: ' + JSON.stringify(sent('hello signed in').map(p => p.fn)));
    await signOutName();
    await page.fill('#owInput', 'hello no name'); await page.waitForFunction(() => !document.getElementById('owSend').disabled, null, { timeout: 5000 });
    await page.click('#owSend'); await page.waitForSelector('#orderWin .cnNameBar', { timeout: 8000 });
    b = await bar();
    check(b && b.inWin && /^Continue as /.test(b.chip) && sent('hello no name').length === 0, 'no name: the bar opens inside the order window (no pop-up on a pop-up) with Continue as; nothing sent yet: ' + JSON.stringify(b));
    await page.click('.cnNameBar .cnNbCont'); await page.waitForTimeout(1800);
    check(sent('hello no name').length >= 1 && JSON.stringify(sent('hello no name')).includes(ADMIN + '.'), 'one tap: the message was sent by itself under the name: ' + JSON.stringify(sent('hello no name').map(p => p.fn)));
    await page.evaluate(() => { const o = document.getElementById('orderWin'); if (o && o.open) document.getElementById('owClose').click(); });
  }
  await ctx.close(); try { await srv.close && srv.close(); } catch (_) {}
}

// ═════════════════════════════ B · Complete Order, Hold, Cancel Order (the Review / Hold fixture) ═════════════════════════════
const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 10, 17) / 1000);
const O = n => ({ rid: '41714090' + String(70 + n), solo: '41714090' + String(70 + n) + '1' });
const ONES = [1, 2, 3, 4, 5, 6].map(O);          // 1 Complete (signed in) · 2 Hold (signed in) · 3 Complete (no name) · 4 Hold (no name) · 5 message (no name) · 6 spare
const line = (tid, sku) => ({ transactionId: tid, listingId: '19008' + tid.slice(-5), sku, title: sku + ' necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Charm', value: 'Chain only' }], metalKey: null, metalLabel: '' });
const order = (rid, buyer, lines) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 7 * DAY, updateTs: SHIP - 7 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
const PDFMAKE = `window.pdfMake = { createPdf(dd) { return { getBlob(cb) { cb(new Blob(['<!doctype html><title>label</title>'], { type: 'text/html' })); } }; } };`;
async function partB(browser) {
  const srv = await start({ receipts: [] });
  const rec = { events: [], sessions: [] }, posts = [];
  ONES.forEach((o, i) => srv.st.put('Order_Timeline', `${o.rid}~arrived~e${i}`, { orderId: o.rid, type: 'arrived', at: Date.now() - 4000 * 60000, by: 'Etsy', source: 'etsy', station: '', text: '', data: {} }));
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
  await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
    const u = r.request().url();
    if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/pdfmake/.test(u)) return r.fulfill(js(PDFMAKE));
    if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/vfs_fonts/.test(u)) return r.fulfill(js(''));
    if (/qrcodejs/.test(u)) return r.fulfill(js(fs.readFileSync(path.join(root, 'lib/qrcode.min.js'))));
    if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
    return r.abort();
  });
  await routeDoor(context, rec);
  await context.route(/\/\.netlify\/functions\//, r => { const q = r.request(); if (q.method() === 'POST') { let b = null; try { b = JSON.parse(q.postData() || 'null'); } catch (_) {} posts.push({ fn: new URL(q.url()).pathname.split('/').pop(), body: b }); } return r.fallback(); });
  await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Paul K'); sessionStorage.setItem('__seeded', '1'); } localStorage.setItem('cn.tour.seen', '1'); } catch (_) {} window.alert = () => {}; });
  const page = await context.newPage(); page.setDefaultTimeout(30000); watchDialogs(page, 'B');
  page.on('pageerror', e => { errors.push('B: ' + e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 3).join(' | ')); });
  await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
  await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomPrint && window.Seal && window.OrderWin && window.OrderTimeline && window.HoldUI && window.CancelUI && window.CNRole && CN.S.cloud.ok === true, null, { timeout: 60000 });
  const orders = ONES.map((o, i) => [order(o.rid, 'Buyer ' + (i + 1), [line(o.solo, 'CHAIN ONLY 56' + String(10 + i))]), [null]]);
  await page.evaluate(async ({ orders }) => {
    await Orders.loadMaps(true);
    for (const [order, pools] of orders) order.lines.forEach((line, i) => { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: pools[i] ? 'pooled' : 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, hold: null, notes: [] }; B.orders.rows.push(row); B.orders.byKey.set(key, row); });
    Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
  }, { orders });
  await page.waitForFunction(n => document.querySelectorAll('#rvList .reviewListRow').length >= n, ONES.length, { timeout: 20000 });
  // the fake engine and film (the real ones are other workers' modules: only the glue is proved), and the cancel's step recorded
  await page.evaluate(() => {
    window.__t = { runs: [], cancels: [] };
    const rowsOf = rid => Orders.rows().filter(r => String(r.order.receiptId) === String(rid));
    window.OrderHold = {
      plan: async rid => { __t.plans = (__t.plans || []).concat(rid); return ({ rid, label: 'CHAIN', customer: 'Buyer', canHold: true, blockedWhy: null, pieces: [], sheets: [], fills: [], stays: [], effects: [] }); },
      run: async (rid, o) => { __t.runs.push({ rid, name: o && o.name }); for (const r of rowsOf(rid)) { r.hold = 'Taken off by ' + o.name; r.state = 'held'; } o.onStep && o.onStep({ type: 'done', at: Date.now() }); return { ok: true, held: true, steps: [] }; },
      status: () => ({ running: false })
    };
    window.OrderHoldFx = { playHold: () => ({ push() {}, finish() {}, skip() {}, done: Promise.resolve() }), playRelease: () => ({ push() {}, finish() {}, skip() {}, done: Promise.resolve() }), returnToOnHold: () => { CN.setMode('orders'); Orders.showPile('hold', ''); } };
    SheetWin.takeOffOrder = async o => { __t.cancels.push({ rid: o.orderId, by: o.by, mode: o.mode }); for (const r of rowsOf(o.orderId)) { r.state = 'gone'; } return { ok: true }; };
    Review.syncOrderItems(); Review.render();
  });
  const T = () => page.evaluate(() => JSON.parse(JSON.stringify(window.__t)));
  const idle = async () => { await page.evaluate(() => Seal.whenIdle()); await page.waitForTimeout(800); };      // (the cards finish their motion first: a press in the middle of it is lost)
  const card = o => `#rvList .reviewListRow[data-row="${o.rid}_${o.solo}"]`;
  const bar = () => page.evaluate(() => { const b = document.querySelector('.cnNameBar'); return b ? { kind: b.dataset.kind, chip: (b.querySelector('.cnNbCont:not([hidden])') || {}).textContent || '', input: b.querySelector('input') ? b.querySelector('input').value : null, inWin: !!b.closest('dialog') } : null; });
  const named = (txt) => JSON.stringify(posts.filter(p => p.body).map(p => p.body)).includes(txt);
  await page.evaluate(() => { B.employee = 'Paul K'; }); await page.waitForTimeout(700);
  check(await page.evaluate(() => CNRole.admin()), 'B: ' + ADMIN + ' is the Admin (no Laser or Design question)');
  const [o1, o2, o3, o4, o5] = ONES;

  console.log('· B1: Complete Order, signed in');
  await page.click(`${card(o1)} [data-cu-complete]`); await page.waitForTimeout(1500);
  check(!(await bar()) && !(await page.evaluate(() => document.querySelector('[data-cu-name]'))), 'no field, no bar');
  check(named('Paul K'), 'the completion was written under the name: ' + JSON.stringify(posts.filter(p => p.body && JSON.stringify(p.body).includes('Paul K')).map(p => p.fn).slice(0, 4)));

  console.log('· B2: Hold, signed in');
  await idle();
  await page.click(`${card(o2)} [data-hold-btn]`);
  await page.waitForFunction(() => document.querySelector('dialog.holdDlg[open]'), null, { timeout: 15000 });
  await page.click('dialog.holdDlg [data-k=go]'); await page.waitForFunction(() => window.__t.runs.length >= 1, null, { timeout: 15000 });
  let t = await T();
  check(!(await bar()) && t.runs[0].rid === o2.rid && t.runs[0].name === ADMIN + '.', 'the hold ran at once under the name, no bar: ' + JSON.stringify(t.runs));

  console.log('· B3: Cancel Order, signed in (the held order, in On hold)');
  await page.evaluate(() => { CN.setMode('orders'); Orders.showPile('hold', ''); });
  await page.waitForSelector(`#ordItems [data-rid="${o2.rid}"] .cnCancelBtn`, { timeout: 15000 });
  await idle(); await page.click(`#ordItems [data-rid="${o2.rid}"] .cnCancelBtn`); await page.waitForFunction(() => window.__t.cancels.length >= 1, null, { timeout: 15000 });
  t = await T();
  check(!(await bar()) && t.cancels[0].rid === o2.rid && t.cancels[0].by === ADMIN + '.' && t.cancels[0].mode === 'cancel', 'the cancel went out under the name, no bar: ' + JSON.stringify(t.cancels));

  console.log('· B4: nobody signed in: Complete Order (the card\'s own field with the last name ready), Hold and Cancel (the bar with Continue as), a message');
  await page.evaluate(() => { CN.setMode('review'); Review.render(); try { localStorage.removeItem('cn.employee'); } catch (_) {} B.employee = ''; });
  check(await page.evaluate(() => CNEmployee.name()) === '' && await page.evaluate(() => CNEmployee.last()) === ADMIN + '.', 'no name; the last name is ' + ADMIN + '.');
  await page.waitForSelector(card(o3), { timeout: 10000 });
  await page.click(`${card(o3)} [data-cu-complete]`);
  await page.waitForSelector('[data-cu-name]', { timeout: 8000 });
  const field = await page.evaluate(() => document.querySelector('[data-cu-name]').value);
  check(field === ADMIN + '.' && !(await bar()), 'Complete Order with no name: the card\'s own field, last name ready, no pop-up: ' + JSON.stringify(field));
  await page.press('[data-cu-name]', 'Enter'); await page.waitForTimeout(1500);
  check(!(await page.evaluate(() => document.querySelector('[data-cu-name]'))) && (await page.evaluate(() => CNEmployee.name())) === ADMIN + '.', 'Enter kept it: signed in again and the order was completed by itself');
  await page.evaluate(() => { try { localStorage.removeItem('cn.employee'); } catch (_) {} B.employee = ''; });
  await idle(); await page.click(`${card(o4)} [data-hold-btn]`); await page.waitForFunction(() => document.querySelector('dialog.holdDlg[open]'), null, { timeout: 15000 });
  await page.click('dialog.holdDlg [data-k=go]'); await page.waitForSelector('.cnNameBar', { timeout: 8000 });
  let b = await bar();
  check(b && /^Continue as /.test(b.chip) && (await T()).runs.length === 1, 'Hold with no name: the inline bar with Continue as; nothing runs before it: ' + JSON.stringify(b));
  await page.click('.cnNameBar .cnNbCont'); await page.waitForFunction(() => window.__t.runs.length >= 2, null, { timeout: 15000 });
  t = await T();
  check(t.runs[1].rid === o4.rid && t.runs[1].name === ADMIN + '.', 'one tap and the hold ran by itself under the name: ' + JSON.stringify(t.runs[1]));
  await page.evaluate(() => { try { localStorage.removeItem('cn.employee'); } catch (_) {} B.employee = ''; CN.setMode('orders'); Orders.showPile('hold', ''); });
  await page.waitForSelector(`#ordItems [data-rid="${o4.rid}"] .cnCancelBtn`, { timeout: 15000 });
  await idle(); await page.click(`#ordItems [data-rid="${o4.rid}"] .cnCancelBtn`); await page.waitForSelector('.cnNameBar', { timeout: 8000 });
  b = await bar();
  check(b && /^Continue as /.test(b.chip) && (await T()).cancels.length === 1, 'Cancel Order with no name: the inline bar with Continue as; nothing cancelled before it: ' + JSON.stringify(b));
  await page.click('.cnNameBar .cnNbCont'); await page.waitForFunction(() => window.__t.cancels.length >= 2, null, { timeout: 15000 });
  t = await T();
  check(t.cancels[1].rid === o4.rid && t.cancels[1].by === ADMIN + '.', 'one tap and the cancel went out by itself under the name: ' + JSON.stringify(t.cancels[1]));
  // a message from the order window
  await page.evaluate(() => { try { localStorage.removeItem('cn.employee'); } catch (_) {} B.employee = ''; CN.setMode('review'); });
  await page.evaluate(k => OrderWin.open(k), `${o5.rid}_${o5.solo}`); await page.waitForFunction(k => OrderWin.isOpen() && OrderWin.key() === k, `${o5.rid}_${o5.solo}`, { timeout: 15000 });
  await page.fill('#owInput', 'hello from the window'); await page.waitForFunction(() => !document.getElementById('owSend').disabled, null, { timeout: 5000 });
  await page.click('#owSend'); await page.waitForSelector('#orderWin .cnNameBar', { timeout: 8000 });
  b = await bar();
  check(b && b.inWin && /^Continue as /.test(b.chip) && !named('hello from the window'), 'Message with no name: the bar opens inside the order window; nothing sent yet: ' + JSON.stringify(b));
  await page.click('.cnNameBar .cnNbCont'); await page.waitForTimeout(1800);
  check(named('hello from the window') && JSON.stringify(posts.filter(p => p.body && JSON.stringify(p.body).includes('hello from the window'))).includes('Paul K'), 'one tap and the message was sent by itself under the name');
  await context.close(); try { await srv.close && srv.close(); } catch (_) {}
}

// ═════════════════════════════ C · the hint is there when the Sorter opens ═════════════════════════════
async function partC(browser) {
  const srv = await start({ receipts: [] }), rec = { events: [], sessions: [] };
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => { const u = r.request().url(); if (/qrcodejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }); if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' }); return r.abort(); });
  await routeDoor(context, rec);
  await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.tour.seen', '1'); if (!sessionStorage.getItem('__c')) { sessionStorage.setItem('__c', '1'); localStorage.setItem('cn.lastEmployee', 'Paul K.'); } } catch (_) {} window.alert = () => {}; });
  const page = await context.newPage(); page.setDefaultTimeout(30000); watchDialogs(page, 'C');
  page.on('pageerror', e => { errors.push('C: ' + e.message); });
  await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
  await page.waitForSelector('.cnNameBar[data-kind="hint"]', { timeout: 15000 }).catch(() => {});
  const hint = await page.evaluate(() => { const b = document.querySelector('.cnNameBar[data-kind="hint"]'); return b ? { chip: (b.querySelector('.cnNbCont:not([hidden])') || {}).textContent || '', focused: b.contains(document.activeElement) } : null; });
  check(hint && hint.chip === 'Continue as Paul K.' && !hint.focused, 'C: nobody signed in at open: the calm hint is already there (no first press needed), takes no focus, and offers Continue as: ' + JSON.stringify(hint));
  await page.click('.cnNameBar .cnNbCont'); await page.waitForFunction(() => CNEmployee.name() === 'Paul K.', null, { timeout: 8000 }).catch(() => {});
  check(await page.evaluate(() => CNEmployee.name()) === 'Paul K.' && !(await page.evaluate(() => document.querySelector('.cnNameBar[data-kind="hint"]'))), 'C: one tap signs the person in and the hint goes away');
  // a name set meanwhile by another tab takes the hint away and is this tab's name
  const page2 = await context.newPage(); await page2.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
  await context.close(); try { await srv.close && srv.close(); } catch (_) {}
}

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { ({ chromium } = require('/opt/node22/lib/node_modules/playwright/node_modules/playwright-core')); }
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    if (want('C')) await partC(browser).catch(e => check(false, 'C stopped by an error: ' + String(e && e.stack || e).split('\n').slice(0, 4).join(' | ')));
    if (want('B')) await partB(browser).catch(e => check(false, 'B stopped by an error: ' + String(e && e.stack || e).split('\n').slice(0, 4).join(' | ')));
    if (want('A')) await partA(browser).catch(e => check(false, 'A stopped by an error: ' + String(e && e.stack || e).split('\n').slice(0, 4).join(' | ')));
  } finally { await browser.close().catch(() => {}); }
  check(dialogsAll.length === 0, 'not one browser pop-up (dialog event) in any page: ' + JSON.stringify(dialogsAll));
  check(errors.length === 0, 'no page error: ' + JSON.stringify(errors.slice(0, 3)));
  console.log(fails.length ? `\n${fails.length} FAILED` : '\nall passed');
  process.exit(fails.length ? 1 : 0);
})();
