'use strict';
/* SAL1 (Paul, 9 Nov 2026): the Orders head's Today / Yesterday / Last 7 days, where "1221 charms" stood.
   Offline: Toronto day boundaries (DST days included), 23:59 vs 00:01, distinct receipts (not pieces), cancelled orders, the webhook's signature and
   idempotence, the counter document's transaction, the page's counter following the changes feed with no polling, and the charm total gone. */
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm'), crypto = require('node:crypto'), path = require('node:path');
const root = path.join(__dirname, '..', '..');
const D = require(path.join(root, 'charm-nest-order-days.js'));
const Hook = require(path.join(root, 'netlify/functions/_etsyOrderWebhook.js'));
let n = 0; const ok = (c, m) => { n++; assert.ok(c, m); };
const at = iso => Date.parse(iso), s = iso => Math.floor(at(iso) / 1000);

(async () => {
  // ── 1 · Toronto days, never a fixed offset ──
  ok(D.dateOf(at('2026-11-09T04:59:00Z')) === '2026-11-08' && D.dateOf(at('2026-11-09T05:00:00Z')) === '2026-11-09', 'EST: midnight Toronto is 05:00 UTC');
  ok(D.dateOf(at('2026-07-15T03:59:00Z')) === '2026-07-14' && D.dateOf(at('2026-07-15T04:00:00Z')) === '2026-07-15', 'EDT: midnight Toronto is 04:00 UTC');
  // 2026-11-01: clocks fall back at 02:00 EDT (06:00 UTC); the day is 25 hours, 04:00 UTC Nov 1 → 05:00 UTC Nov 2
  ok(D.dateOf(at('2026-11-01T03:59:00Z')) === '2026-10-31' && D.dateOf(at('2026-11-01T04:00:00Z')) === '2026-11-01', 'DST end day starts at 04:00 UTC (EDT)');
  ok(D.dateOf(at('2026-11-02T04:59:00Z')) === '2026-11-01' && D.dateOf(at('2026-11-02T05:00:00Z')) === '2026-11-02', 'and ends at 05:00 UTC (EST): 25 hours');
  ok(D.secOfDay(at('2026-11-01T05:30:00Z')) === 5400 && D.secOfDay(at('2026-11-01T06:30:00Z')) === 5400, 'the repeated 01:30 reads 01:30 both times (wall clock)');
  // 2026-03-08: clocks spring forward; 05:00 UTC Mar 8 → 04:00 UTC Mar 9 (23 hours)
  ok(D.dateOf(at('2026-03-08T04:59:00Z')) === '2026-03-07' && D.dateOf(at('2026-03-08T05:00:00Z')) === '2026-03-08' && D.dateOf(at('2026-03-09T03:59:00Z')) === '2026-03-08' && D.dateOf(at('2026-03-09T04:00:00Z')) === '2026-03-09', 'DST start day: 23 hours');
  ok(D.shift('2026-11-01', -1) === '2026-10-31' && D.shift('2026-03-01', -1) === '2026-02-28' && D.shift('2026-12-31', 1) === '2027-01-01', 'calendar arithmetic, no clock');

  // ── 2 · 23:59 vs 00:01 Toronto ──
  {
    const now = at('2026-11-02T15:00:00Z');   // 10:00 EST, Nov 2
    const doc = D.apply({}, [{ receipt_id: 101, create_timestamp: s('2026-11-02T04:59:00Z') }, { receipt_id: 102, create_timestamp: s('2026-11-02T05:01:00Z') }], now).doc;
    const sum = D.summarize(doc, now);
    ok(sum.today.n === 1 && sum.today.ids[0] === '102', '00:01 Toronto is today: ' + JSON.stringify(sum.today));
    ok(sum.yesterday.n === 1 && sum.yesterday.date === '2026-11-01', '23:59 Toronto (on the 25-hour day) is yesterday');
    ok(sum.week.n === 2 && sum.week.from === '2026-10-27' && sum.week.to === '2026-11-02', 'the week is the 7 Toronto days ending today, today included');
    ok(sum.yesterday.sameTime === 0, 'by 10:00 yesterday, the 23:59 order had not been placed (the delta compares like with like)');
    // the moment the clock passes Toronto midnight, Today turns over with no read
    const later = D.summarize(doc, at('2026-11-03T05:00:30Z'));
    ok(later.today.n === 0 && later.yesterday.n === 1 && later.yesterday.date === '2026-11-02', 'midnight Toronto: today resets, yesterday is the day that ended');
  }

  // ── 3 · distinct receipts, not pieces; cancelled count as placed; the webhook's entry counted once ──
  {
    const now = at('2026-11-09T20:00:00Z'), t = s('2026-11-09T14:00:00Z');
    let doc = D.apply({}, [
      { receipt_id: 201, create_timestamp: t, status: 'Paid' },
      { receipt_id: 201, create_timestamp: t, status: 'Paid' },                     // the same receipt twice in one page
      { receipt_id: 202, create_timestamp: t + 60, status: 'Canceled' },            // Etsy's spelling
      { receiptId: '203', createTs: (t + 120) * 1000 }                               // the sorter's order shape, in ms
    ], now).doc;
    // three pieces of one order the page holds: one receipt
    const held = [{ receiptId: '204', createTs: t + 200 }, { receiptId: '204', createTs: t + 200 }, { receiptId: '204', createTs: t + 200 }].map(o => ({ id: o.receiptId, createTs: o.createTs }));
    let sum = D.summarize(doc, now, held);
    ok(sum.today.n === 4, 'four distinct receipts (one in the page only, one of three pieces): ' + sum.today.n);
    ok(sum.today.cx === 1, 'the cancelled one counts as placed, and is said: ' + sum.today.cx);
    // a webhook names 205 (paid now) and 201 (already mirrored): 205 counts once; 201 is not counted again
    doc = D.apply(doc, [{ id: '205', createTs: s('2026-11-09T19:00:00Z'), hook: true }, { id: '201', createTs: s('2026-11-09T19:00:00Z'), hook: true }], now).doc;
    sum = D.summarize(doc, now, held);
    ok(sum.today.n === 5, 'the webhook adds the new receipt only: ' + sum.today.n);
    // the mirror later reads 205 with its real creation time, the day before (paid after midnight): it moves, never counted twice
    doc = D.apply(doc, [{ receipt_id: 205, create_timestamp: s('2026-11-09T04:50:00Z') }], now).doc;
    sum = D.summarize(doc, now, held);
    ok(sum.today.n === 4 && sum.yesterday.n === 1 && sum.week.n === 5, 'a receipt seen by the webhook and then the mirror is counted once, on its real day: ' + JSON.stringify([sum.today.n, sum.yesterday.n, sum.week.n]));
    // a cancel taken back only by a receipt whose status was read
    const again = D.apply(doc, [{ receiptId: '202', createTs: t + 60 }], now);
    ok(!again.changed, 'an order without a status (the page\'s) never takes a cancel back');
    // older days go; the document stays small
    const old = D.apply({ days: { '2026-10-01': { ids: { 1: 1 }, cx: {}, hook: {} } } }, [{ receipt_id: 9, create_timestamp: s('2026-10-02T12:00:00Z') }], now);
    ok(old.changed && !old.doc.days['2026-10-01'] && !old.doc.days['2026-10-02'], 'days past the last ' + D.KEEP_DAYS + ' are dropped and not added');
  }

  // ── 4 · the counter document: one transaction, idempotent ──
  const fakeDb = () => {
    const store = new Map(), st = { reads: 0, writes: 0, tx: 0 };
    const ref = p => ({ path: p, get: async () => { st.reads++; return snap(p); } });
    const snap = p => ({ exists: store.has(p), data: () => JSON.parse(JSON.stringify(store.get(p))) });
    return { st, store, collection: c => ({ doc: d => ref(c + '/' + d) }),
      runTransaction: async fn => { st.tx++; const w = []; const out = await fn({ get: async r => { st.reads++; return snap(r.path); }, set: (r, v) => w.push([r.path, v]) }); for (const [p, v] of w) { st.writes++; store.set(p, JSON.parse(JSON.stringify(v))); } return out; } };
  };
  {
    const db = fakeDb(), now = Date.now(), list = [{ receipt_id: 301, create_timestamp: Math.floor(now / 1000) - 60, status: 'Paid' }];
    let m = await D.merge(db, '', list, now);
    ok(m.ok && m.changed && db.st.writes === 1 && db.store.has('Charm_Nest_Order_Days/recent'), 'the first sight of a receipt writes the one document');
    m = await D.merge(db, '', list, now);
    ok(m.ok && !m.changed && db.st.writes === 1, 'the same receipt again: no write');
    m = await D.merge(db, 'Sandbox_', list, now);
    ok(db.store.has('Sandbox_Charm_Nest_Order_Days/recent'), 'the sandbox keeps its own document');
    m = await D.merge({ runTransaction: async () => { throw new Error('offline'); }, collection: () => ({ doc: () => ({}) }) }, '', list, now);
    ok(m && m.ok === false && /offline/.test(m.error), 'a failure is an answer, never a throw (the read that carries it goes on)');
  }

  // ── 5 · the webhook: Standard Webhooks signature, idempotent ──
  {
    const key = crypto.randomBytes(24), secret = 'whsec_' + key.toString('base64');
    const sign = (id, ts, body, k = key) => 'v1,' + crypto.createHmac('sha256', k).update(`${id}.${ts}.${body}`).digest('base64');
    const nowMs = at('2026-11-09T18:00:00Z'), ts = String(Math.floor(nowMs / 1000));
    const body = JSON.stringify({ event_type: 'order.paid', resource_url: 'https://openapi.etsy.com/v3/application/shops/123/receipts/4199990001', shop_id: 123 });
    const ev = (o = {}) => ({ httpMethod: 'POST', body: o.body || body, headers: Object.assign({ 'Webhook-Id': 'msg_1', 'Webhook-Timestamp': o.ts || ts, 'Webhook-Signature': o.sig || sign('msg_1', o.ts || ts, o.body || body) }, o.headers) });
    const db = fakeDb(); let bumps = 0;
    const deps = { db, OrderDays: D, secret, now: () => nowMs, bump: async () => { bumps++; } };
    let r = await Hook.handle(ev(), deps);
    ok(r.statusCode === 200 && JSON.parse(r.body).counted === true && bumps === 1, 'a signed order.paid counts its receipt and nudges the feed once: ' + r.body);
    r = await Hook.handle(ev(), deps);
    ok(r.statusCode === 200 && JSON.parse(r.body).counted === false && bumps === 1 && db.st.writes === 1, 'the same delivery again (Etsy retries): counted once, nothing written, no nudge');
    ok(D.summarize(db.store.get('Charm_Nest_Order_Days/recent'), nowMs).today.n === 1, 'and Today says 1');
    r = await Hook.handle(ev({ sig: sign('msg_1', ts, body, crypto.randomBytes(24)) }), deps);
    ok(r.statusCode === 401, 'a signature under another key is refused');
    r = await Hook.handle(ev({ body: body.replace('4199990001', '4199990002'), sig: sign('msg_1', ts, body) }), deps);
    ok(r.statusCode === 401, 'a body changed after signing is refused');
    const old = String(Number(ts) - 3600);
    r = await Hook.handle(ev({ ts: old, sig: sign('msg_1', old, body) }), deps);
    ok(r.statusCode === 401, 'a replay an hour old is refused');
    r = await Hook.handle(ev(), Object.assign({}, deps, { secret: '' }));
    const was = process.env.ETSY_WEBHOOK_SECRET; delete process.env.ETSY_WEBHOOK_SECRET;
    r = await Hook.handle(ev(), Object.assign({}, deps, { secret: undefined }));
    if (was != null) process.env.ETSY_WEBHOOK_SECRET = was;
    ok(r.statusCode === 503 && db.st.writes === 1, 'with no signing secret set, nothing is accepted');
    // the secret from Firestore (EtsyApi_Config/webhook.signingSecret, admin SDK), the env only as a fallback, never in a response or a log line
    {
      const logged = [], keep = {};
      for (const k of ['log', 'info', 'warn', 'error', 'debug']) { keep[k] = console[k]; console[k] = (...a) => logged.push(a.map(String).join(' ')); }
      const wasEnv = process.env.ETSY_WEBHOOK_SECRET; delete process.env.ETSY_WEBHOOK_SECRET;
      const seen = [];
      try {
        const b2 = JSON.stringify({ event_type: 'order.paid', resource_url: 'https://openapi.etsy.com/v3/application/shops/123/receipts/4199990101', shop_id: 123 });
        const db2 = fakeDb(); db2.store.set('EtsyApi_Config/webhook', { signingSecret: ' ' + secret + ' ' });
        const d2 = { db: db2, OrderDays: D, now: () => nowMs };
        Hook._forgetSecret();
        r = await Hook.handle(ev({ body: b2, sig: sign('msg_1', ts, b2) }), d2); seen.push(r);
        ok(r.statusCode === 200 && JSON.parse(r.body).counted === true, 'a delivery signed with the secret kept in Firestore is accepted: ' + r.body);
        ok(Hook.SECRET_PATH === 'EtsyApi_Config/webhook', 'the secret lives in EtsyApi_Config/webhook');
        const reads = db2.st.reads;
        r = await Hook.handle(ev({ body: b2, sig: sign('msg_1', ts, b2) }), d2); seen.push(r);
        ok(r.statusCode === 200 && db2.st.reads === reads + 1, 'a warm function reads the secret once (the second delivery reads only the day document)');
        const ts11 = String(Number(ts) + 11 * 60), r1 = db2.st.reads;
        r = await Hook.handle(ev({ body: b2, ts: ts11, sig: sign('msg_1', ts11, b2) }), Object.assign({}, d2, { now: () => nowMs + 11 * 60 * 1000 })); seen.push(r);
        ok(r.statusCode === 200 && db2.st.reads === r1 + 2, 'after ten minutes the document is read again (secret + day document)');
        r = await Hook.handle(ev({ body: b2, sig: sign('msg_1', ts, b2, crypto.randomBytes(24)) }), d2); seen.push(r);
        ok(r.statusCode === 401, 'with the Firestore secret, another key is still refused');
        // the env fallback: no document, the env set
        const db3 = fakeDb(); process.env.ETSY_WEBHOOK_SECRET = secret; Hook._forgetSecret();
        r = await Hook.handle(ev(), { db: db3, OrderDays: D, now: () => nowMs }); seen.push(r);
        ok(r.statusCode === 200 && JSON.parse(r.body).ok === true, 'with no Firestore secret, ETSY_WEBHOOK_SECRET still works as a fallback');
        // the document wins over the env
        const db4 = fakeDb(); db4.store.set('EtsyApi_Config/webhook', { signingSecret: 'whsec_' + crypto.randomBytes(24).toString('base64') }); Hook._forgetSecret();
        r = await Hook.handle(ev(), { db: db4, OrderDays: D, now: () => nowMs }); seen.push(r);
        ok(r.statusCode === 401, 'the Firestore secret takes precedence over the env');
        // Firestore unreachable: the env fallback, and the failure is not cached
        const dbErr = { collection: () => ({ doc: () => ({ get: async () => { throw new Error('offline'); } }) }), runTransaction: db3.runTransaction };
        Hook._forgetSecret();
        r = await Hook.handle(ev(), { db: dbErr, OrderDays: D, now: () => nowMs }); seen.push(r);
        ok(r.statusCode === 200, 'a failed Firestore read falls back to the env: ' + r.body);
        // nowhere: refused
        delete process.env.ETSY_WEBHOOK_SECRET; Hook._forgetSecret();
        const db5 = fakeDb();
        r = await Hook.handle(ev(), { db: db5, OrderDays: D, now: () => nowMs }); seen.push(r);
        ok(r.statusCode === 503 && db5.st.writes === 0 && /EtsyApi_Config\/webhook\.signingSecret/.test(r.body), 'no secret in Firestore or the env: refused (503), nothing written: ' + r.body);
        db5.store.set('EtsyApi_Config/webhook', { signingSecret: secret });
        r = await Hook.handle(ev(), { db: db5, OrderDays: D, now: () => nowMs + 61 * 1000 }); seen.push(r);
        ok(r.statusCode === 200, 'a secret pasted after a refusal is picked up within a minute');
        r = await Hook.handle({ httpMethod: 'POST', body: 'x', headers: {} }, { db: db5, OrderDays: D, now: () => nowMs + 62 * 1000 }); seen.push(r);
      } finally {
        for (const k of Object.keys(keep)) console[k] = keep[k];
        if (wasEnv != null) process.env.ETSY_WEBHOOK_SECRET = wasEnv; else delete process.env.ETSY_WEBHOOK_SECRET;
        Hook._forgetSecret();
      }
      const raw = secret.replace(/^whsec_/, '');
      const leaks = seen.concat(logged).map(x => typeof x === 'string' ? x : JSON.stringify(x)).filter(t => t.includes(raw) || t.includes(key.toString('hex')));
      ok(leaks.length === 0, 'the secret is in no response and no log line');
    }
    r = await Hook.handle(Object.assign(ev(), { httpMethod: 'GET' }), deps);
    ok(r.statusCode === 405, 'POST only');
    const other = JSON.stringify({ event_type: 'listing.updated', resource_url: 'x', shop_id: 123 });
    r = await Hook.handle(ev({ body: other, sig: sign('msg_1', ts, other) }), deps);
    ok(r.statusCode === 200 && /ignored/.test(r.body) && db.st.writes === 1, 'another event is answered and ignored');
    ok(Hook.receiptOf({ resource_url: 'https://openapi.etsy.com/v3/application/shops/1/receipts/77?x=1' }) === '77' && Hook.receiptOf({ data: { receipt_id: 78 } }) === '78' && Hook.receiptOf({ resource_url: '/listings/5' }) === '', 'the receipt id is read from the payload, with no Etsy call');
    const lib = fs.readFileSync(path.join(root, 'netlify/functions/charmNestLibrary.js'), 'utf8');
    const door = lib.indexOf('hook === "etsy-order"'), gate = lib.indexOf('const denied = gate(event, body)');
    ok(door > 0 && door < gate, 'the webhook has its own door before the passcode (Etsy signs instead)');
  }

  // ── 6 · the page: the counter follows the order check's answer and the changes feed, with no polling ──
  {
    const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
    const start = bridge.indexOf('const SalesDays = window.SalesDays'), end = bridge.indexOf('/* Import cadence is independent of run completion.');
    ok(start > 0 && end > start, 'SalesDays is in the bridge');
    const calls = [], timers = [], intervals = []; let follow = null;
    const el = () => ({ textContent: '', hidden: true, dataset: {}, title: '', classList: { remove() {}, add() {}, contains: () => false }, setAttribute() {}, querySelector: () => el(), isConnected: true });
    const box = { title: '', isConnected: true, secs: {}, querySelector(sel) { const k = /data-k="(\w+)"/.exec(sel); if (!k) return null; return this.secs[k[1]] || (this.secs[k[1]] = Object.assign(el(), { querySelector: q => { const o = this.secs[k[1]]; o._q = o._q || {}; return o._q[q] || (o._q[q] = el()); } })); } };
    const view = { classList: { contains: () => false } };
    const ctx = { window: {}, WORKSPACE_SANDBOX: false, S: { cloud: { ok: true } }, Date, JSON, Math, Map, Set, Object, String, Number, Promise, console,
      localStorage: { getItem: () => null, setItem() {} },
      document: { hidden: false, getElementById: id => (id === 'ordDays' ? box : id === 'ordersView' ? view : null) },
      setTimeout: (f, ms) => { timers.push({ f, ms }); return timers.length; }, clearTimeout() {}, setInterval: (f, ms) => { intervals.push({ f, ms }); return 1; },
      api: async (fn, body) => { calls.push(body.op); return { ok: true, days: { v: 1, at: Date.now(), days: { [D.dateOf(Date.now())]: { ids: { 9001: Math.floor(Date.now() / 1000) - 5, 9002: Math.floor(Date.now() / 1000) - 9 }, cx: {}, hook: {} } } } }; } };
    ctx.window.CharmNestOrderDays = D;
    ctx.window.CNSync = { follow: spec => { follow = spec; return { stop() {} }; } };
    ctx.CNSync = ctx.window.CNSync; ctx.SimClock = null;
    ctx.window.Orders = { rows: () => [{ order: { receiptId: '9003', createTs: Math.floor(Date.now() / 1000) - 30 } }, { order: { receiptId: '9003', createTs: Math.floor(Date.now() / 1000) - 30 } }] };
    vm.createContext(ctx);
    vm.runInContext(bridge.slice(start, end) + ';this.SalesDays=SalesDays;', ctx);
    const SD = ctx.SalesDays;
    const html = SD.html();
    ok((html.match(/class="odSec"/g) || []).length === 3 && /data-k="today"/.test(html) && /data-k="yday"/.test(html) && /data-k="week"/.test(html) && (html.match(/<svg class="odIc"/g) || []).length === 3, 'three sections, each with its own line icon');
    SD.paint(); await new Promise(r => setImmediate(r));
    ok(calls.length === 1 && calls[0] === 'getOrderDays', 'first drawing with no copy: one read (getOrderDays): ' + calls.join());
    ok(box.secs.today.querySelector('.odN').textContent === '3', 'Today = the document\'s 2 + the page\'s own arrival (2 pieces, 1 receipt): ' + box.secs.today.querySelector('.odN').textContent);
    ok(follow && follow.kinds === 'd' && intervals.length === 1, 'it follows the changes feed (d:) and keeps one drawing tick');
    for (let i = 0; i < 30; i++) intervals[0].f();   // half an hour of the minute tick
    await new Promise(r => setImmediate(r));
    ok(calls.length === 1, 'the minute tick only draws: no read, no Etsy call (' + calls.length + ' reads)');
    // the order check's answer carries the counts: no read
    SD.take({ v: 1, at: Date.now(), days: { [D.dateOf(Date.now())]: { ids: { 9001: 1e9, 9002: 1e9, 9004: Math.floor(Date.now() / 1000) - 1 }, cx: {}, hook: {} } } });
    await new Promise(r => setImmediate(r));
    ok(calls.length === 1, 'the order check\'s answer updates them with no read');
    // the feed names d:orderdays (another computer's check, or the webhook): one read, and a burst does not read again at once
    follow.read('keys', ['d:orderdays']); await new Promise(r => setImmediate(r));
    follow.read('keys', ['d:orderdays']); follow.read('keys', ['d:orderdays']); await new Promise(r => setImmediate(r));
    ok(calls.length <= 2, 'the feed asks once, then at most every 20 s: ' + calls.length);
    follow.read('verify', []); await new Promise(r => setImmediate(r));
    ok(calls.length <= 2, 'the feed\'s ten-minute net reads nothing more (the order check carries the counts)');
    ok(timers.some(t => t.ms > 0 && t.ms <= 3600000), 'a timer turns the day over at Toronto midnight (re-armed hourly at most)');
  }

  // ── 7 · the bar: "1221 charms" is gone; the server carries the counts ──
  {
    const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
    ok(!/ordCharmTotal/.test(bridge) && !/<b>\$\{t(otals)?\.charms\}<\/b> charms/.test(bridge) && !/\$\{ordBase\.charms/.test(bridge), 'the charm total is gone from the Orders bar');
    const head = bridge.slice(bridge.indexOf('function buildHead(v)'), bridge.indexOf('<span class="ordTools" id="ordTools">'));
    ok(/SalesDays\.html\(\)/.test(head) && !/charmTotal/.test(head), 'the sales days stand where it stood');
    const css = fs.readFileSync(path.join(root, 'charm-nest-activity.css'), 'utf8');
    ok(!/#ordCharmTotal/.test(css) && /\.ordDays/.test(css), 'and its style went with it');
    const lib = fs.readFileSync(path.join(root, 'netlify/functions/charmNestLibrary.js'), 'utf8');
    const ar = lib.slice(lib.indexOf('async function op_arrivalRecord'), lib.indexOf('// ── runs'));
    ok(/OrderDays\.merge\(/.test(ar) && /revTouch\(b, \["d:orderdays"\]\)/.test(ar) && /days\}/.test(ar), 'the order check writes the day counts and answers with them');
    ok(/getOrderDays: op_getOrderDays/.test(lib) && /NO_GEN_BUMP\.add\("getOrderDays"\)/.test(lib) && /\[strcpxomad\]/.test(lib), 'one read op, and d: is a feed name');
    const cron = fs.readFileSync(path.join(root, 'netlify/functions/etsyMailReceiptsMirrorCron.js'), 'utf8');
    const page = cron.indexOf('await writeReceiptBatch(receipts)'), merge = cron.indexOf('OrderDays.merge(db, "", receipts)');
    ok(page > 0 && merge > page, 'the receipt mirror counts every receipt of each page it reads already (no Etsy call of its own)');
    const fam = require(path.join(root, 'charm-nest-sandbox-families.js'));
    ok(fam.server().some(f => f.key === 'Charm_Nest_Order_Days'), 'the sandbox\'s counter is a family its reset wipes');
    const worker = fs.readFileSync(path.join(root, 'charm-nest-sync-worker.js'), 'utf8');
    ok(/KIND = \/\^\[strcpxomad\*\]\$\//.test(worker) && /\[strcpxomad\]:/.test(worker), 'the changes feed carries d: names to the page');
  }
  console.log(`sales-days: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
