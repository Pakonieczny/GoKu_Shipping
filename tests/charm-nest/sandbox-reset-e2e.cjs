// End to end: press the real Reset in the real page and look at every tab (Paul, 3 Oct 2026: "It still didn't purge the system
// fully, and kept the relationships of the custom orders": after the earlier fixes the clean-up was right, but the sandbox
// replayed the 17 Sep orders by itself the moment the page reloaded, so pool rows, sheets, a set, arrivals and runs were
// back within seconds and nothing looked purged).
// A dirty sandbox is built through the REAL ops of the real functions (charmNestLibrary: a custom order with its QR-label
// and complete stamps, a custom design sent to the sheets, pool rows (one nested), a set, a sheet, a run with its line
// archive, a back record, arrivals, a cancelled order, timeline events; the stations' door: sign-in sessions, activity with
// its rollup, finished orders, messages, a staff note) over the in-memory Firestore of bridge-server.cjs, plus the sandbox
// side of the browser (saved workspace, drafts, queues, journals) and a production twin of every record under the same
// numbers. The real station runs in its frame and holds events it has not sent. Then, in the page:
//   · before: every tab shows the old records (a control: the test would otherwise pass on an empty page),
//   · Reset is pressed (the real button of Settings; the page's own confirm), and the reloaded page is checked tab by tab:
//     Orders, Review (Open and Completed), Library (Sets, Sheets), Nest (the rail and the sheets), Engraving, Sign-ins and
//     Employee efficiency show nothing of before; the Orders tab says the sandbox waits and offers Start,
//   · the sandbox stays quiet until started: no stream, no check of the orders, no Auto run, nothing written, in Manual and
//     with Auto on; saving Settings is not Start; a second tab of the browser is quiet too,
//   · Settings says the page build and what the last reset did, inside the dialog (no pop-up on a pop-up),
//   · Start (the Orders tab's button) pulls the 250 newest Etsy orders (here: the same four) once and plays the day from step 0 with the seed in Settings: the same orders come back as NEW
//     orders, with none of the old stamps, pool rows, sheets or decisions on them,
//   · production's records and keys are byte-identical, and nothing of the old sandbox was written to the cloud again.
//   node tests/charm-nest/sandbox-reset-e2e.cjs [playwright-core dir]
'use strict';
const path = require('path'), assert = require('assert'), fs = require('fs');
const root = path.join(__dirname, '../..'), fnDir = path.join(root, 'netlify/functions');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('./bridge-server.cjs');
const PEEK = !!process.env.PEEK;

/* ── the orders the sandbox plays under their real numbers (the 17 Sep snapshot) ── */
const A = '4173162973', B = '4170408845', C = '4170000555', D = '4170000777';   // A chain only (QR label printed), B a custom design sent to a sheet, C cancelled, D plain
const TX = { [A]: '10010', [B]: '20020', [C]: '30030', [D]: '40040' };
const DAY = '2026-09-29', RUN = 'run-20260929-1', SET = `set-${DAY}-1`, SHEET = 'sheet-20260929-gf-1';
const PASS = 'e2e-passcode', SEED = 777, SPEED = 1000, STEP = 600000;
const NOW = Date.now(), day = Math.floor(NOW / 1000);
const GF = '14k Gold Filled';
const line = (rid, sku, title, i) => { const created = day - 500 * 3600 + i * 60; return { transaction_id: Number(TX[rid] || `${rid}${i}`), listing_id: 1718000 + i, receipt_id: Number(rid), sku, title, quantity: 1, create_timestamp: created, created_timestamp: created, paid_timestamp: created, expected_ship_date: created + 5 * 86400, shipped_timestamp: null, variations: [{ formatted_name: 'Metal', formatted_value: GF }], is_personalized: false }; };
const receipt = (rid, i, sku, title) => { const created = day - (500 - i * 4) * 3600; return { receipt_id: Number(rid), order_number: String(rid), name: 'Buyer ' + rid, country_iso: 'US', city: 'Austin', message_from_buyer: '', create_timestamp: created, created_timestamp: created, update_timestamp: created + 60, updated_timestamp: created + 60, status: 'Paid', is_paid: true, is_shipped: false, transactions: [Object.assign(line(rid, sku, title, i), { create_timestamp: created, created_timestamp: created, paid_timestamp: created, expected_ship_date: created + 5 * 86400 })] }; };
const snapshot = [receipt(A, 0, 'CHAIN_8941', 'CHAIN REPLACEMENT'), receipt(B, 1, 'CUSTOM-N-001', 'CUTE TRICERATOPS W/ HEARTS'), receipt(C, 2, 'BR-TST-01', 'Charm C'), receipt(D, 3, 'BR-TST-02', 'Charm D')]
  .concat(Array.from({ length: 8 }, (_, i) => receipt(String(4171000001 + i), 4 + i, 'BR-TST-0' + (3 + (i % 3)), 'Charm ' + i)));
const OLD = 'OLDREC';

(async () => {
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin, stationOrigin } = srv;
  // the sandbox's orders are the 250 newest Etsy orders, pulled at every Start (op sandboxPullOrders): here the fake Etsy's newest
  // orders are these same four, so "the same orders come back as new ones" after the reset still reads the same
  process.env.SHOP_ID = '987654'; process.env.CLIENT_ID = 'test-key'; process.env.CLIENT_SECRET = 'test-secret';
  const Pull = require(path.join(fnDir, '_charmNestSandboxPull.js')), realPull = Pull.pull, pullReqs = [];
  const fakeEtsy = async url => { const u = new URL(url), off = +u.searchParams.get('offset'), lim = +u.searchParams.get('limit'); return { ok: true, status: 200, headers: { get: () => null }, text: async () => '', json: async () => ({ results: JSON.parse(JSON.stringify(snapshot.slice(off, off + lim))) }) }; };
  Pull.pull = (b, ctx) => { pullReqs.push(b.startId); return realPull(b, ctx, { fetch: fakeEtsy, token: async () => 'test-token', meter: { bump: () => ({ fromHttp() {}, failNet() {} }), flushNow: async () => {} } }); };
  const fdb = st.admin.firestore(); if (!fdb.doc) fdb.doc = p => { const [c, ...rest] = p.split('/'); return fdb.collection(c).doc(rest.join('/')); };
  st.put('config', 'etsyOauth', { access_token: 'a', refresh_token: 'r', expires_at_ms: Date.now() + 3600e3 });
  // the stations' door and the efficiency console's reader are the real handlers, over the same in-memory Firestore
  const door = require(path.join(fnDir, 'firebaseOrders.js')), effFn = require(path.join(fnDir, 'employeeEfficiency.js'));
  st.fn.firebaseOrders = door.handler; st.fn.employeeEfficiency = effFn.handler;
  st.put('config', 'editPasscode', { passcode: PASS });
  const lib = b => st.handlers.charmNestLibrary.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(b) }).then(r => ({ status: r.statusCode, body: JSON.parse(r.body) }));
  const must = async b => { const r = await lib(b); assert.strictEqual(r.status, 200, JSON.stringify(b).slice(0, 120) + ' -> ' + JSON.stringify(r.body).slice(0, 300)); return r.body; };
  const station = (sb, body) => door.handler({ httpMethod: 'POST', headers: {}, queryStringParameters: sb ? { sandbox: '1' } : {}, body: JSON.stringify(body) }).then(r => { assert.strictEqual(r.statusCode, 200, JSON.stringify(body).slice(0, 100) + ' -> ' + r.body.slice(0, 300)); return JSON.parse(r.body); });

  /** Everything one side writes (sb: a sandbox page; else a production page), the same numbers on both sides. */
  async function seed(sb) {
    const L = b => must(Object.assign({}, b, sb ? { sandbox: true } : {})), P = sb ? 'Sandbox_' : '', F = b => station(sb, b);
    const label = { w: 40, h: 20, qr: 'x'.repeat(40), lines: [A, 'CHAIN_8941'] };
    await L({ op: 'customPut', key: `${A}_${TX[A]}`, by: 'paul', label, receiptId: A, transactionId: TX[A], sku: 'CHAIN_8941', title: 'CHAIN REPLACEMENT', category: 'Chain only', kind: 'chainOnly' });
    await L({ op: 'customPut', key: `${A}_${TX[A]}`, by: 'paul', label, receiptId: A });
    await L({ op: 'customPut', key: `${B}_${TX[B]}`, by: 'ann', how: 'button', receiptId: B, transactionId: TX[B], sku: 'CUSTOM-N-001', title: 'CUTE TRICERATOPS W/ HEARTS', kind: 'custom' });
    await L({ op: 'customReopen', key: `${B}_${TX[B]}`, by: 'ann', how: 'reopen' });
    const at = NOW - 3 * 86400000, ck = `custom:${B}:CUSTOM-N-001`;
    await L({ op: 'customSheetPut', record: { ck, rid: B, at: at - 1000, phase: 'sent',
      files: [{ id: 'file-custom-1', name: 'Customer design.ai', kind: 'ai', size: 2400, hash: 'abcde01234567890123456789', cloud: { path: `charmnest/custom/${B}/original.pdf`, url: 'https://saved.example/original.pdf' }, metal: 'silver', qty: 1, pieces: 1, wMm: 14, hMm: 18, maxPt: 52, minPt: 40, maxAreaPt2: 2000, state: 'ready' }],
      sent: { id: `custom-sheet:${ck}:${at}`, at, by: 'paul', lines: { [`${B}_${TX[B]}`]: [{ f: 'file-custom-1', i: 0 }] } } } });
    const poolId = rid => `${rid}_${TX[rid]}_1`;
    await L({ op: 'poolPut', pools: [A, B, D].map((rid, i) => ({ poolId: poolId(rid), orderId: rid, transactionId: TX[rid], lineKey: `${rid}_${TX[rid]}`, runId: RUN, state: i === 0 ? 'written' : 'pooled', sheetId: i === 0 ? SHEET : null, setId: i === 0 ? SET : null, material: ['gold', 'silver', 'gold'][i], copy: 1, quantity: 1, custom: i === 1 })) });
    await L({ op: 'setAllocate', day: DAY, runId: RUN });
    await L({ op: 'putSheet', sheet: { id: SHEET, metal: 'gold', label: 'GF Sheet 1', fileBase: 'GF_Sheet-1', runId: RUN, setId: SET, status: 'complete', orders: [A, B], poolIds: [poolId(A), poolId(B)], charms: [A, B].map(rid => ({ id: poolId(rid), poolId: poolId(rid), order: rid })), placements: [A, B].map((rid, i) => ({ id: poolId(rid), x: 10 + 30 * i, y: 10, rot: 0 })), placedCount: 2 } });
    const runLine = (rid, sku, title, state) => ({ state, poolIds: [poolId(rid)], reason: null, hold: null, wait: null, sku, material: 'gold', quantity: 1, engrave: null, engraveCandidate: false, noDesign: false, activityAt: 0, changePending: false, repoolChanged: false, arrivedAt: NOW - 86400000, createTs: day - 3 * 86400, materialOverride: null, sizeOverride: null, problems: [], updateTs: day - 3 * 86400, orderId: rid, transactionId: TX[rid],
      snap: { title, listingId: '1718000', metalKey: 'gf', metalLabel: GF, orderNumber: rid, buyer: 'Buyer ' + rid, shipBy: day + 2 * 86400, isGift: false, vars: [], pers: [] } });
    await L({ op: 'runPut', run: { runId: RUN, status: 'complete', step: 'complete', day: DAY, orders: [A, B, D], lines: { [`${A}_${TX[A]}`]: runLine(A, 'CHAIN_8941', 'CHAIN REPLACEMENT', 'written'), [`${B}_${TX[B]}`]: runLine(B, 'CUSTOM-N-001', 'CUTE TRICERATOPS W/ HEARTS', 'written'), [`${D}_${TX[D]}`]: runLine(D, 'BR-TST-02', 'Charm D', 'pooled') } } });
    await L({ op: 'runArchive', runId: RUN, parts: [{ json: JSON.stringify({ [`${D}_${TX[D]}`]: { orderId: D, key: `${D}_${TX[D]}` } }) }] });
    await L({ op: 'backPut', back: { poolId: poolId(A), sheetId: SHEET, setId: SET, approvedAt: NOW, approvedBy: 'paul', text: 'ANNA' } }).catch(() => null);
    await L({ op: 'releasePut', released: { gf: DAY }, lastReleased: { gf: DAY } });
    await L({ op: 'bridgeLog', session: 'sorter-session-1', meta: { by: 'paul' }, rows: [{ t: Date.now(), dir: 'cmd', type: 'ping' }, { t: Date.now(), dir: 'evt', type: 'pong' }] });
    await L({ op: 'arrivalRecord', orders: [{ id: A, createTs: 1 }, { id: C, createTs: 1 }] });
    await L({ op: 'cancelPut', orderId: C, by: 'Etsy', why: 'buyer cancelled', record: { buyer: 'Buyer C', lines: [{ transactionId: TX[C], title: 'Charm C' }] } });
    await L({ op: 'timelineAdd', events: [{ orderId: A, type: 'qrLabel', id: 'q1', text: 'QR label for GF Sheet 1', by: 'paul' }, { orderId: B, type: 'designSent', id: 'ds1', text: 'sent', by: 'paul' }] });
    // the stations' door: finished orders, a message and a note, a sign-in with what was done (and its daily rollup)
    await F({ completedIds: [A] });
    await F({ newMessage: 'hello team', orderNumber: A, employeeName: 'Paul' });
    await F({ orderNumber: B, staffNote: 'engrave the back' });
    // (a person of each side: the Sign-ins window reads the shop's real sign-ins, the Employee efficiency console the sandbox's own)
    const who = sb ? { person: 'Sandy', station: 'welding', computer: 'computer-bbb222', label: 'Bench 2' } : { person: 'Paul', station: 'sorting', computer: 'computer-aaa111', label: 'Bench 1' };
    await F({ session: { id: 'session-aaaa-0001', event: 'start', station: who.station, computerId: who.computer, person: who.person, computerLabel: who.label } });
    await F({ activity: [{ id: 'activity-evt-0001', station: who.station, action: 'scan', person: who.person, orderId: A, parts: 2, at: NOW - 60000, computer: who.computer, session: 'session-aaaa-0001' }, { id: 'activity-evt-0002', station: who.station, action: 'complete', person: who.person, orderId: A, parts: 2, orders: 1, at: NOW - 30000, computer: who.computer }] });
    st.put(P + 'Brites_Orders', 'unused', { touched: 1 });   // (a parent only a message hangs under)
    st.put('EtsyMail_OrderLinks', `${sb ? 'olsb_' : 'ol_'}${A}_o_k1abc`, { id: `${sb ? 'olsb_' : 'ol_'}${A}_o_k1abc`, receiptId: A, scope: 'order', sandbox: sb, status: 'open', outbox: [{ id: 'x1', text: 'which chain length?', status: sb ? 'sent' : 'queued' }], sim: [], createdAtMs: NOW });
  }
  // the sandbox's snapshot (the emulated Etsy serves it) and the production side
  const snapPath = 'charmnest/sandbox/orders-e2e.json';
  st.blobs.set(snapPath, { buf: Buffer.from(JSON.stringify({ at: NOW, count: snapshot.length, receipts: snapshot })), generation: 1, meta: { contentType: 'application/json', metadata: {} } });
  st.put('Charm_Sandbox', 'current', { path: snapPath, count: snapshot.length, open: snapshot.length, at: NOW, pulledAt: NOW, takenBy: 'test', source: 'etsy-pull', startId: 'sbx-before-the-reset' });
  await seed(false); await seed(true);
  const ownKey = k => k.startsWith('Sandbox_') || k === 'Charm_Sandbox/stream' || k.startsWith('EtsyMail_OrderLinks/olsb_');
  const sandboxDocs = () => [...st.docs.keys()].filter(ownKey);
  const prodSnap = () => JSON.stringify([...st.docs.entries()].filter(([k]) => !ownKey(k)).sort(([a], [b]) => (a < b ? -1 : 1)));
  const prodBlobs = () => JSON.stringify([...st.blobs.entries()].filter(([k]) => !/^charmnest\/sandbox\//.test(k)).map(([k, v]) => [k, v.buf.toString('base64')]).sort(([a], [b]) => (a < b ? -1 : 1)));
  const seeded = sandboxDocs().length;
  assert(seeded > 40, 'the sandbox holds many records: ' + seeded);
  assert(st.docs.has(`Sandbox_Charm_Custom_Orders/${A}_${TX[A]}`) && st.doc('Sandbox_Charm_Custom_Orders', `${A}_${TX[A]}`).stamps.length === 2, 'the seeded custom order has its QR label stamps');
  console.log(`seeded: ${seeded} sandbox records and ${st.docs.size - seeded - 0} others (production, with the same numbers)`);

  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 1000 } });
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
  await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
  await ctx.route(/^https:\/\/firebasestorage\.googleapis\.com\//, r => { const u = new URL(r.request().url()); const m = /\/o\/(.+)$/.exec(u.pathname); const key = m ? decodeURIComponent(m[1]) : ''; const b = st.blobs.get(key); if (!b) return r.fulfill({ status: 404, body: 'no blob ' + key }); return r.fulfill({ status: 200, headers: { 'Content-Type': b.meta.contentType || 'application/octet-stream', 'Access-Control-Allow-Origin': '*', 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: b.buf }); });
  await ctx.addInitScript(({ station, sorter, pass }) => {
    if (location.origin === station) { localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200)); localStorage.setItem('employee_name', 'Tester'); }
    if (location.origin === sorter) { localStorage.setItem('cn.employee', 'Tester'); try { sessionStorage.setItem('cn.eff.key', pass); } catch (_) {} }
    window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {};
  }, { station: stationOrigin, sorter: sorterOrigin, pass: PASS });
  const errors = [];
  const open = async () => { const p = await ctx.newPage(); p.on('pageerror', e => errors.push('page: ' + e.message)); p.on('console', m => { if (m.type() === 'error') errors.push('console: ' + m.text().slice(0, 200)); }); await p.goto(`${sorterOrigin}/charm-nest-1.html`); return p; };
  const booted = p => p.waitForFunction(() => window.CN && window.Sandbox && window.Arrivals && window.Session && CN.S.cloud.ok !== null, null, { timeout: 60000 });
  const settle = (p, extra) => p.evaluate(([station, extra]) => { const s = CN.S.settings; Object.assign(s, { dsOrigin: station, engine: 'solver', budgetS: 12, review: 'off', naming: 'off', packingAI: 'off', notify: 'off', sound: 'off', autoCommit: 'on', runMode: 'manual', pullMode: 'all', heartbeatS: 2, heartbeatMiss: 2 }, extra || {}); CN.saveSettings(); }, [stationOrigin, extra]);
  const pause = ms => new Promise(r => setTimeout(r, ms));
  const rows = p => p.evaluate(() => B.orders.rows.map(r => ({ rid: String(r.order.receiptId), tx: String(r.line.transactionId), state: r.state, pool: r.poolIds.length })));
  const peek = (label, text) => { if (PEEK) console.log(`--- ${label}\n${String(text).replace(/\n{2,}/g, '\n').slice(0, 1800)}`); };

  /* ── boot: the sandbox, streaming (fast), Manual; the orders of the day arrive over the dirty records ── */
  const page = await open(); await booted(page);
  await settle(page, { sandbox: 'on', sandboxStream: 'on', sandboxSpeed: SPEED, sandboxSeed: SEED });
  await page.reload(); await booted(page);
  await page.waitForFunction(() => Sandbox.stream(), null, { timeout: 20000 });
  await page.evaluate(() => CN.setMode('orders'));
  assert.strictEqual(await page.evaluate(() => WORKSPACE_SANDBOX), true, 'the page is in the sandbox');
  assert.strictEqual(await page.evaluate(() => Sandbox.held()), false, 'a sandbox that was never reset plays at once (the stream starts by itself, as it always did)');
  // the page opens on the last run, as it always does (Recall: its orders, its sheets, read from the record)
  await page.waitForFunction(ids => window.Recall && Recall.on() && ids.every(id => B.orders.rows.some(r => String(r.order.receiptId) === id)), [A, B, D], { timeout: 60000 });
  // the real station, in its frame: signed in as the sandbox's, holding an event it has not sent, and a finished order
  const frame = () => page.frames().find(f => /design-1\.html/.test(f.url()));
  await page.waitForFunction(() => { const s = DesignLink.state && DesignLink.state(); return !!s; }, null, { timeout: 30000 });
  await page.evaluate(() => DesignLink.ensure());
  await page.waitForFunction(() => /design-1\.html/.test((document.getElementById('dsFrame') || {}).src || ''), null, { timeout: 30000 });
  await pause(1500);

  /* ── seed the sandbox side of this browser, and the production side beside it ── */
  const sandboxKeys = {
    'cn.team.outbox:sandbox': [{ id: 'q-old', rid: A, text: OLD + ' message', who: 'paul', at: Date.now(), tries: 0, error: 'held' }],
    'cn.team.drafts:sandbox': { [A]: { t: OLD + ' draft', at: Date.now() } },
    'cn.mail.drafts:sandbox': { [A]: { t: OLD + ' mail draft', at: Date.now() } },
    'cn.tl.sheets:sandbox': { [OLD]: Date.now() },
    'cn.sheetwin.moves:sandbox': { [A]: { rid: A, by: OLD } },
    'cn.roseRehearsal.sandbox.v1': { [OLD]: 1 }
  };
  const prodKeys = {
    'cn.team.outbox': [{ id: 'q-prod', rid: '4100000001', text: 'production message', who: 'paul', at: Date.now(), tries: 0, error: 'held' }],
    'cn.team.drafts': { 4100000001: { t: 'production draft', at: Date.now() } },
    'cn.mail.drafts': { 4100000001: { t: 'production mail draft', at: Date.now() } },
    'cn.autoCancel.v1:production': { known: [], done: { 4100000001: { at: 1, t: Date.now() } }, jobs: {}, notices: [], pend: {} },
    'cn.tl.sheets': { prod: Date.now() }, 'cn.sheetwin.moves': { 4100000001: { rid: '4100000001' } },
    'cn.customRead.production': { '4100000001_1': { hash: 'h', at: Date.now() } }
  };
  const sharedLists = { 'orderTimeline.outbox.v1': [{ orderId: A, type: 'qrLabel', id: OLD + '-ev', sandbox: true, mode: 'sorter', at: Date.now() }, { orderId: '4100000001', type: 'scan', id: 'prod-ev', sandbox: false, mode: 'station', at: Date.now() }] };
  await page.evaluate(({ sandboxKeys, prodKeys, sharedLists, A, B }) => {
    for (const o of [sandboxKeys, prodKeys, sharedLists]) for (const [k, v] of Object.entries(o)) localStorage.setItem(k, JSON.stringify(v));
    Review.settled().unshift({ key: `ord:custom:${A}:CHAIN_8941`, kind: 'customOrder', why: 'QR label printed by paul · Sep 30 11:48 PM', lines: 1, orders: [A], by: 'paul', t: Date.now() - 3600000 });
    Session.schedule();
  }, { sandboxKeys, prodKeys, sharedLists, A, B });
  // the station, in its own origin: a finished order in its browser ledger (sandbox only) and an event it has not sent
  const fr = frame(); assert(fr, 'the station frame is mounted');
  await fr.waitForFunction(() => window.StationActivity && window.StationSession, null, { timeout: 30000 });
  await fr.evaluate(ids => { ids.forEach(id => completedOrders.add(id)); return persistCompleted(ids); }, [D]);
  const unsent = await fr.evaluate(() => { const ok = StationActivity.log('scan', { orderId: '4170000777', line: '40040', parts: 1, detail: 'OLDREC scan before the reset' }); return { ok, pending: StationActivity.pending(), who: StationActivity.who() }; });
  console.log('station:', JSON.stringify(unsent));
  assert.strictEqual(await page.evaluate(() => Session.flushNow()), true, 'the workspace was saved');

  // production's own side of the browser: a workspace checkpoint (IndexedDB) and a keyed list, to be found unchanged
  const idb = (p, fn, arg) => p.evaluate(async ([src, arg]) => { const f = eval(src); return f(arg); }, [fn.toString(), arg]);
  const idbPutRaw = (p, key, value) => idb(p, ([key, value]) => new Promise((res, rej) => { const r = indexedDB.open('charm-nest-workspace', 1); r.onsuccess = () => { const tx = r.result.transaction('workspaces', 'readwrite'); tx.objectStore('workspaces').put(value, key); tx.oncomplete = () => res(true); tx.onerror = () => rej(tx.error); }; r.onerror = () => rej(r.error); }), [key, value]);
  const idbDump = p => idb(p, () => new Promise((res, rej) => { const r = indexedDB.open('charm-nest-workspace', 1); r.onsuccess = () => { const tx = r.result.transaction('workspaces', 'readonly'), s = tx.objectStore('workspaces'), keys = s.getAllKeys(), out = {}; keys.onsuccess = () => { let n = keys.result.length; if (!n) return res(out); for (const k of keys.result) { const g = s.get(k); g.onsuccess = () => { out[k] = g.result; if (!--n) res(out); }; } }; }; r.onerror = () => rej(r.error); }));
  const PROD_CHECKPOINT = { v: 1, at: 1, epoch: '', marker: 'production-untouched', settled: [{ key: 'prod' }] };
  await idbPutRaw(page, 'production', PROD_CHECKPOINT);

  /* ── before: every tab shows the old records (the control) ── */
  const view = async (p, mode, id, prep) => p.evaluate(([mode, id, prep]) => { CN.setMode(mode); if (prep) eval(prep); return document.getElementById(id).innerText; }, [mode, id, prep || '']);
  const tabs = async p => ({
    orders: await view(p, 'orders', 'ordersView'),
    reviewOpen: await view(p, 'review', 'reviewView', "{ const v = Review.view(); v.cseg = 'open'; v.filter = null; Review.render(); }"),
    reviewDone: await view(p, 'review', 'reviewView', "{ const v = Review.view(); v.cseg = 'done'; v.filter = null; Review.render(); }"),
    library: await (async () => { await view(p, 'library', 'libView'); await p.waitForFunction(() => !/Loading sets/.test(document.getElementById('libView').innerText), null, { timeout: 15000 }).catch(() => {}); return p.evaluate(() => document.getElementById('libView').innerText); })(),
    nest: await p.evaluate(() => { CN.setMode('nest'); return [document.getElementById('sheets').innerText, document.getElementById('railScroll').innerText, document.getElementById('railRecentBody').innerText].join('\n'); }),
    engrave: await view(p, 'engrave', 'engraveView')
  });
  const signins = async p => { await p.evaluate(() => SignIns.open()); await p.waitForFunction(() => { const d = document.querySelector('dialog[open]'); return d && !/Reading/i.test(d.innerText) && d.innerText.length > 20; }, null, { timeout: 15000 }); const t = await p.evaluate(() => [...document.querySelectorAll('dialog[open]')].map(d => d.innerText).join('\n')); await p.evaluate(() => SignIns.close()); return t; };
  const efficiency = async p => { await p.evaluate(() => CN.setMode('efficiency')); await p.waitForFunction(() => { const h = document.getElementById('efficiencyView'); return h && !h.querySelector('[data-lock]') && !/Reading employee activity/.test(h.innerText) && h.innerText.length > 40; }, null, { timeout: 20000 }).catch(() => {}); const t = await p.evaluate(() => document.getElementById('efficiencyView').innerText); return t; };
  const before = await tabs(page); before.signins = await signins(page); before.efficiency = await efficiency(page);
  for (const [k, v] of Object.entries(before)) peek('BEFORE ' + k, v);
  const effAt = Date.now();
  if (PEEK) for (const c of ['Station_Sessions', 'Sandbox_Station_Sessions', 'Station_Activity', 'Sandbox_Station_Activity']) console.log('  before:', c, JSON.stringify(st.list(c).map(d => [d._id, d.person, d.station])));
  // the control: the old records are on screen (so a clean page below proves something)
  assert(before.orders.includes(A) && before.orders.includes(B), 'before: the Orders tab lists the orders');
  assert(/NESTED|nested|on the cards|written/i.test(before.orders), 'before: the Orders tab shows pool/nested state: ' + before.orders.slice(0, 400));
  assert(/QR label printed/i.test(before.reviewDone) || before.reviewDone.includes(A), 'before: Review > Completed lists the old record');
  assert(/Paul/.test(before.signins) && !/Sandy/.test(before.signins), "before: Sign-ins lists the shop's real sign-in, not the sandbox's: " + before.signins.slice(0, 300));
  assert(/Sandy/.test(before.efficiency) && /Paul/.test(before.efficiency), "before: Employee efficiency lists the sandbox's own people (a sign-in, and the seals Paul's QR label left): " + before.efficiency.slice(0, 300));
  const preReset = { sandbox: sandboxDocs().length };
  assert(preReset.sandbox > seeded, 'before: the replay added its own records on top: ' + JSON.stringify(preReset));
  const prodBefore = prodSnap(), blobsBefore = prodBlobs();
  const idbBefore = await idbDump(page);
  const lsBefore = await page.evaluate(() => Object.fromEntries(Object.keys(localStorage).map(k => [k, localStorage.getItem(k)])));
  const stationLsBefore = await fr.evaluate(() => Object.fromEntries(Object.keys(localStorage).map(k => [k, localStorage.getItem(k)])));

  /* ── the press: Settings, the real button (the page's own confirm answers yes) ── */
  await page.evaluate(() => openSettings());
  await page.waitForFunction(() => /Page build/.test(document.getElementById('stBuildNote').innerText), null, { timeout: 15000 });
  const note0 = await page.evaluate(() => document.getElementById('stBuildNote').innerText);
  assert(/Page build 2\d{7}-[\w-]+/.test(note0) && /No reset or purge has been made from this browser yet/.test(note0), 'Settings says the page build and that no reset was made yet: ' + note0);
  const pressedAt = Date.now(), mark = st.calls.length;
  await page.click('#stSandboxReset');
  await page.waitForNavigation({ waitUntil: 'load', timeout: 90000 });
  await booted(page);
  await page.waitForFunction(() => /Sandbox cleaned/.test(document.getElementById('toasts').innerText), null, { timeout: 10000 }).catch(() => {});
  const wipeAt = st.calls.reduce((n, c, i) => (i >= mark && c.op === 'sandboxReset' ? i : n), -1);
  assert(wipeAt >= 0, 'the page asked the cloud to reset the sandbox');
  const toast1 = await page.evaluate(() => [...document.querySelectorAll('#toasts .toast .m')].map(x => x.textContent));
  assert(toast1.some(t => /Sandbox cleaned — \d+ record\(s\) and \d+ file\(s\) removed; nothing is left\. The sandbox now waits, empty, until you press Start/.test(t)), 'the clean page says what was removed and that the sandbox waits: ' + JSON.stringify(toast1));
  assert.strictEqual(await page.evaluate(() => Sandbox.held()), true, 'the sandbox waits');

  /* ── quiet: nothing replays by itself (Manual here; Auto below) ── */
  await pause(6000);   // (at 1000x a check is due every 0.6 s: six seconds is thousands of simulated minutes)
  const since = st.calls.slice(wipeAt + 1);
  const WRITES = /^(poolPut|poolUpdate|setAllocate|setUpdate|putSheet|runPut|runArchive|backPut|backInvalidate|releasePut|arrivalRecord|customPut|customReopen|customSheetPut|customDecide|cancelPut|cancelRestore|timelineAdd|sandboxStream|putCharms|startAgent|startJob|aliasPut|optionMapPut|noDesignPut)$/;
  const wrote = since.filter(c => WRITES.test(c.op || ''));
  assert.deepStrictEqual(wrote.map(c => c.op), [], 'after the reset the page wrote nothing of the sandbox: ' + wrote.map(c => c.op).join(','));
  if (PEEK) console.log('calls since the wipe:', since.map(c => c.name + (c.op ? ':' + c.op : '') + (c.q && c.q.fn ? '/' + c.q.fn : '')).join(' '));
  // (two reads of the emulator that the old page's last check had out when the records went finish after the final delete: reads only, and dropped)
  assert(!since.some(c => c.name === 'etsySandbox' && c.q.fn === 'listOpenOrders'), 'and swept no order list')
  assert.strictEqual(await page.evaluate(() => !!Sandbox.stream() || SimClock.on()), false, 'no stream, no simulated clock');
  assert.strictEqual(st.doc('Charm_Sandbox', 'stream'), undefined, 'the cloud holds no stream');
  const quiet = sandboxDocs();
  const surprise = quiet.filter(k => !/^Sandbox_(Station_Sessions|Station_Activity|Efficiency_Daily)\//.test(k));
  if (PEEK) for (const k of quiet) console.log('  ', k, JSON.stringify(st.docs.get(k)).slice(0, 400));
  console.log('in the cloud, after the quiet window:', JSON.stringify(quiet.map(k => k.split('/')[0]).reduce((m, k) => (m[k] = (m[k] || 0) + 1, m), {})));
  assert.deepStrictEqual(surprise, [], 'the sandbox holds no record but the sign-in of whoever is working now: ' + surprise.join(', '));
  for (const k of quiet) { const d = st.docs.get(k); const t = d.startAt || d.at || d.lastSeenAt; assert(!t || +t >= pressedAt - 1000, `no old sign-in or activity came back: ${k} ${JSON.stringify(d).slice(0, 160)}`); assert(!JSON.stringify(d).includes(OLD) && !/Sandy|Bench 2/.test(JSON.stringify(d)), `nothing of the old sign-ins: ${k}`); }

  /* ── tab by tab: nothing from before ── */
  const after = await tabs(page);
  await page.waitForFunction(() => !/Loading sets/.test(document.getElementById('libView').innerText), null, { timeout: 15000 }).catch(() => {});
  after.library = await view(page, 'library', 'libView');
  after.signins = await signins(page);
  while (Date.now() - effAt < 6500) await pause(200);   // (the reader keeps an answer 5 s)
  after.efficiency = await efficiency(page);
  for (const [k, v] of Object.entries(after)) peek('AFTER ' + k, v);
  const OLDWORDS = new RegExp([A, B, D, 'CHAIN', 'TRICERATOPS', 'WRITTEN', 'POOLED', 'NESTED', 'QR LABEL', 'Sent to Sheet', 'ON THE SHEETS', 'GF_Sheet-1', 'GF Sheet 1', 'Sheet 1(?!\\d)', 'recalled', 'ANNA', 'Sandy', 'Bench 2', 'ENGRAVING · DECIDED', 'OLDREC'].join('|'), 'i');
  after.efficiency += '';   // (here Paul is the person whose seals the sandbox's timeline held)
  for (const [k, v] of Object.entries(after)) assert(!OLDWORDS.test(v) && !(k === 'efficiency' && /Paul/.test(v)), `after: the ${k} tab shows nothing from before — it shows "${(OLDWORDS.exec(v) || [])[0]}": ${v.slice(0, 300).replace(/\n+/g, ' | ')}`);
  assert(/Sandbox is empty\. Press Start \(or turn Auto on\) to replay the \d+ \w{3} orders\./.test(after.orders) && /\nStart\b/.test(after.orders), 'the Orders tab says the sandbox is empty and offers Start: ' + after.orders.slice(-200));
  assert(!/Nothing pulled yet/.test(after.orders), 'and not the old "Nothing pulled yet"');
  assert.strictEqual(await page.evaluate(() => [Review.settled().length, Object.keys(B.customDesigns).length, B.orders.rows.length, CN.allSheets().filter(p => p.charms.length).length, Engrave.items().size, Recall.on() ? 1 : 0].join()), '0,0,0,0,0,0', 'the page holds none of the old records in memory');
  assert(/Paul/.test(after.signins), "Sign-ins still lists the shop's real sign-in (a sandbox reset never touches production's): " + after.signins.slice(0, 300));
  assert(/Tester/.test(after.efficiency), 'Employee efficiency lists the person working now (signed in after the reset): ' + after.efficiency.slice(0, 300));

  /* ── Settings: the build, the last reset, what the sandbox holds now (inside the dialog; no pop-up on a pop-up) ── */
  await page.evaluate(() => openSettings());
  await page.waitForFunction(() => /Last reset/.test(document.getElementById('stBuildNote').innerText) && /In the sandbox now/.test(document.getElementById('stBuildNote').innerText), null, { timeout: 15000 });
  const st1 = await page.evaluate(() => { const n = document.getElementById('stBuildNote'), d = document.getElementById('dlgSettings'), field = e => e.closest('.field'); n.scrollIntoView({ block: 'center' }); const r = n.getBoundingClientRect(); return { text: n.innerText, inside: d.contains(n), open: document.querySelectorAll('dialog[open]').length, dlgOpen: d.open, build: Sandbox.build(), page: Sandbox.pageBuild(), adjacent: field(document.getElementById('stSandboxReset')).nextElementSibling === field(document.getElementById('stPurge')) && field(document.getElementById('stPurge')).nextElementSibling === field(n), within: r.top >= 0 && r.bottom <= innerHeight, h: Math.round(r.height) }; });
  console.log('settings note:\n' + st1.text);
  if (process.env.SHOT) { await pause(900); await page.screenshot({ path: process.env.SHOT }); }
  assert(st1.inside && st1.dlgOpen && st1.open === 1, 'the line is inside the one open Settings dialog');
  assert(st1.build === st1.page && /^2\d{7}-[\w-]+$/.test(st1.build), 'the build in the script is the ?v= of its tag: ' + JSON.stringify([st1.build, st1.page]));
  assert(st1.text.includes(`Page build ${st1.build}`), 'it names the page build');
  assert(/Last reset .+ — \d+ cloud record\(s\) and \d+ file\(s\) removed · this browser's saved copy cleared \(\d+ saved workspace part\(s\), \d+ stored key\(s\)\) · nothing left in the cloud/.test(st1.text), 'it says the time, the cloud records removed, the browser copy and that nothing is left: ' + st1.text);
  const nowLine = st1.text.split('\n').pop();
  assert(/^In the sandbox now: /.test(nowLine) && !/pool|sheets|sets|runs|arrivals|custom|counters/i.test(nowLine), 'and what the sandbox holds now, none of the old kinds: ' + nowLine);
  assert(st1.adjacent && st1.within && st1.h < 160, 'it sits right under the two buttons, inside the dialog, and is a few calm lines: ' + JSON.stringify(st1));
  await page.evaluate(() => closeDlg(document.getElementById('dlgSettings')));

  /* ── saving Settings is not Start; a second tab of the browser is quiet too ── */
  const callsMark = st.calls.length;
  await page.evaluate(() => openSettings()); await page.click('#btnSaveSettings'); await pause(1500);
  assert.strictEqual(await page.evaluate(() => Sandbox.held() && !Sandbox.stream()), true, 'saving Settings does not start the sandbox');
  const tab2 = await open(); await booted(tab2); await pause(3000);
  assert.strictEqual(await tab2.evaluate(() => Sandbox.held() && !Sandbox.stream() && !B.orders.rows.length), true, 'a second tab of this browser waits too');
  const callsLater = st.calls.slice(callsMark);
  assert.deepStrictEqual(callsLater.filter(c => WRITES.test(c.op || '') || c.name === 'etsySandbox').map(c => c.op || c.name), [], 'nothing was written or swept meanwhile');
  await tab2.close();

  /* ── Auto on: the run does not begin by itself either ── */
  await page.evaluate(() => { CN.S.settings.runMode = 'auto'; CN.saveSettings(); });
  const autoMark = st.calls.length;
  await page.reload(); await booted(page); await pause(5000);   // (a fresh Auto load starts its run after 1.5 s)
  const auto = await page.evaluate(() => ({ held: Sandbox.held(), run: !!B.run, stream: !!Sandbox.stream(), rows: B.orders.rows.length, mode: CN.S.settings.runMode }));
  assert(auto.held && !auto.run && !auto.stream && auto.rows === 0 && auto.mode === 'auto', 'Auto is on and the page still waits: ' + JSON.stringify(auto));
  assert(/Auto is on, so the run then starts by itself/.test(await view(page, 'orders', 'ordersView')), 'the Orders tab says Auto starts the run once Start is pressed');
  const autoCalls = st.calls.slice(autoMark).filter(c => WRITES.test(c.op || '') || c.name === 'etsySandbox');
  assert.deepStrictEqual(autoCalls.map(c => c.op || c.name), [], 'with Auto on, no run, no stream, no order swept');
  assert.strictEqual(sandboxDocs().filter(k => !/^Sandbox_(Station_Sessions|Station_Activity|Efficiency_Daily)\//.test(k)).length, 0, 'and the cloud still holds none of the old records');
  await page.evaluate(() => { CN.S.settings.runMode = 'manual'; CN.saveSettings(); });
  await page.reload(); await booted(page);

  /* ── Start: the day plays again from step 0, with the seed in Settings; the same orders come back as NEW ones ── */
  await page.evaluate(() => CN.setMode('orders'));
  await page.waitForSelector('[data-sb-start]', { timeout: 10000 });
  const startAt = Date.now();
  await page.click('[data-sb-start]');
  await page.waitForFunction(() => !Sandbox.held() && Sandbox.stream(), null, { timeout: 20000 });
  await page.waitForFunction(ids => ids.every(id => B.orders.rows.some(r => String(r.order.receiptId) === id)), [A, B, C, D], { timeout: 60000 });
  const s1 = st.doc('Charm_Sandbox', 'stream');
  assert(s1 && s1.seed === SEED && s1.startedAt >= startAt - 1000, 'a new stream, with the seed in Settings, begun at Start: ' + JSON.stringify(s1));
  assert(pullReqs.length === 1 && /^charmnest\/sandbox\/orders-pull\//.test(s1.snapshotPath), 'Start pulled the newest orders once (a fresh set, not the one before the reset): ' + pullReqs.length + ' ' + s1.snapshotPath);
  const replay = await rows(page);
  const replayed = new Set(replay.map(r => r.rid));
  assert([A, B, C, D].every(id => replayed.has(id)), 'the same orders come back');
  // (a chain-only line reads "no design" by its SKU; what it must not read is "completed by hand": that stamp went with the records)
  assert(replay.every(r => ['pulled', 'noDesign'].includes(r.state) && r.pool === 0), 'as new orders: none is nested, pooled or written: ' + JSON.stringify(replay));
  assert.strictEqual(await page.evaluate(() => Object.keys(B.maps.customDone || {}).length + B.orders.rows.filter(r => r.spec && r.spec.customDone).length), 0, 'and no line is completed by hand any more');
  const replayTabs = await tabs(page);
  for (const [k, v] of Object.entries(replayTabs)) peek('REPLAY ' + k, v);
  assert(replayTabs.orders.includes(B) && replayTabs.orders.includes('CUTE TRICERATOPS'), 'the replayed Orders tab lists the custom order');
  assert(!/WRITTEN|POOLED|NESTED|QR LABEL|Sent to Sheet/i.test(replayTabs.orders), 'with none of the old state on it: ' + replayTabs.orders.slice(0, 200));
  assert(!/QR label printed|CUSTOM ORDER · COMPLETED|Sent to Sheet|ON THE SHEETS/i.test(replayTabs.reviewDone + replayTabs.reviewOpen), 'Review shows none of the old stamps on the replayed orders');
  assert(!/GF_Sheet-1|recalled|Sheet 1(?!\d)/.test(replayTabs.nest) && !/ANNA|ENGRAVING · DECIDED/.test(replayTabs.engrave), 'no old sheet and no old engraving decision');
  assert.strictEqual(await page.evaluate(c => !!(window.Cancelled && Cancelled.has(c)), C), false, 'the order cancelled before the reset is not cancelled in the replay');
  assert.strictEqual(await page.evaluate(([b, tx]) => !!CustomSheet.decisionOf({ key: `${b}_${tx}` }), [B, TX[B]]), false, 'the custom design is not "sent" in the replay');
  const ledger = st.list('Sandbox_Charm_Nest_Arrivals').map(d => d._id);
  assert(ledger.length && ledger.every(id => snapshot.some(r => String(r.receipt_id) === id)), "the arrivals ledger holds the replay's own orders only");
  for (const f of ['Sandbox_Charm_Pool', 'Sandbox_Charm_Nest_Sheets', 'Sandbox_Charm_Nest_Sets', 'Sandbox_Charm_Nest_Runs', 'Sandbox_Charm_Custom_Orders', 'Sandbox_Charm_Custom_Sheet', 'Sandbox_Charm_Nest_Cancelled', 'Sandbox_Charm_Pool_Back']) assert.strictEqual(st.list(f).length, 0, `the replay is in Manual: no ${f} yet`);
  assert(!sandboxDocs().some(k => JSON.stringify(st.docs.get(k)).includes(OLD)), 'no old marker anywhere in the sandbox');

  /* ── production: byte for byte ── */
  assert.strictEqual(prodSnap(), prodBefore, "production's records are byte-identical");
  assert.strictEqual(prodBlobs(), blobsBefore, "production's files are byte-identical");
  const ls = await page.evaluate(() => Object.fromEntries(Object.keys(localStorage).map(k => [k, localStorage.getItem(k)])));
  for (const [k, v] of Object.entries(prodKeys)) assert.strictEqual(ls[k], JSON.stringify(v), `the production key ${k} is unchanged`);
  const outbox = JSON.parse(ls['orderTimeline.outbox.v1'] || '[]');   // (the replay's own events — pulled, unmatched SKU — wait here too: new play, sandbox's)
  assert(outbox.some(e => e.id === 'prod-ev' && e.sandbox === false) && !outbox.some(e => e.id === OLD + '-ev') && outbox.filter(e => e.id !== 'prod-ev').every(e => e.sandbox === true && e.at >= startAt - 1000), "the shared timeline outbox keeps production's event and none of the old sandbox's: " + JSON.stringify(outbox.map(e => [e.id, e.sandbox, e.at >= startAt])));
  for (const k of Object.keys(sandboxKeys)) assert(ls[k] == null || !ls[k].includes(OLD), `the sandbox key ${k} holds nothing old`);
  const idbAfter = await idbDump(page);
  assert.deepStrictEqual(idbAfter.production, PROD_CHECKPOINT, 'the production workspace checkpoint is unchanged');
  const stationLs = await frame().evaluate(() => Object.fromEntries(Object.keys(localStorage).map(k => [k, localStorage.getItem(k)])));
  assert(!JSON.stringify(Object.entries(stationLs).filter(([k]) => /sandbox|station_activity_q|station_session_unsent/.test(k))).includes(OLD), 'the station kept nothing old in its own browser store');
  assert(!JSON.parse(stationLs['designCompletedLedger.v1:sandbox'] || '{}')[D], 'the order finished at the station before the reset is not finished after it');
  const errs = errors.filter(e => !/firebase stub|favicon|net::ERR|Failed to load resource|404/.test(e));
  assert.deepStrictEqual(errs, [], 'no page errors');
  console.log(`sandbox reset e2e OK: ${seeded} sandbox records and the browser's copy were cleaned, every tab was empty, the sandbox waited (Manual and Auto) until Start, the replay brought the same orders as new ones, production untouched`);
  await browser.close(); srv.close(); process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
