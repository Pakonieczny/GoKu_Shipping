/* PUSH, the server's half (Paul, 28 Oct 2026: "a change made on one computer to appear on the others at once, with no idle polling"; plan: the local-first design,
   5.5 option a): netlify/functions/_charmNestPush.js and its two hooks (charmNestLibrary.js bumpRev, _orderTimeline.js touchNow) over the repo's in-memory Firestore
   (bridge-server.cjs). No network, no live record, no credential, no browser.
     A · the module alone, over a fake document that takes its time: a burst of nudges is written as a few writes, never one per nudge; each nudge is answered only
         by a write that BEGAN after it (the last nudge of a burst is never lost, and nothing is left to a timer after the answer: an instance freezes then); a stream
         of nudges one after another is held to the gap; a failed write is tried again; a hung one is given up at once for the nudges behind it; two worlds never meet
     B · through the real handler: a write that moves the feed moves Charm_Nest_Push/counter, {n, at} and nothing else (no order, piece or sheet id), the sandbox's
         Sandbox_Charm_Nest_Push/counter apart; a read moves nothing; a Team message's Order_Rev raise does; the person's write never fails (the counter refusing
         every write, or hanging for seconds); 30 writes at once: every answer comes after a counter write that began after its own shards (the last nudge is not lost)
     C · the registry: the sandbox's counter is a family (a reset wipes it), production's is kept
   node tests/charm-nest/push-server.cjs */
'use strict';
const path = require('path'), assert = require('node:assert/strict');
const root = path.join(__dirname, '../..'), fnDir = path.join(root, 'netlify/functions');
const { start } = require('./bridge-server.cjs'), { seed } = require('./laser-workflow.cjs');
const sleep = ms => new Promise(r => setTimeout(r, ms));
const say = s => console.log('  ' + s);

(async () => {
  const srv = await start({ receipts: [] }); seed(srv); const { st } = srv;
  try {
    const Push = require(path.join(fnDir, '_charmNestPush.js')), handler = st.handlers.charmNestLibrary, db = st.db;
    const FV = { increment: n => ({ __inc: n }), serverTimestamp: () => ({ __ts: true }) };
    const call = async b => { const t0 = Date.now(), r = await handler.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(b), queryStringParameters: {} }); return { status: r.statusCode, body: JSON.parse(r.body), headers: r.headers || {}, ms: Date.now() - t0 }; };

    /* ── A · the module alone ── */
    // a fake database whose counter takes `lat` ms to write and can be told to fail or hang; every write logs when it began and when it ended
    const fake = (o = {}) => {
      const log = { writes: [], docs: new Map() };
      return { log, collection: c => ({ doc: id => ({ async set(data, opt) {
        const w = { path: c + '/' + id, begin: Date.now(), end: 0, data, opt }; log.writes.push(w);
        if (o.fail && o.fail(log.writes.length)) { w.end = Date.now(); throw new Error('UNAVAILABLE: refused (test)'); }
        if (o.hang && o.hang(log.writes.length)) return new Promise(() => {});
        await sleep(o.lat == null ? 50 : o.lat); w.end = Date.now(); log.docs.set(w.path, data);
      } }) }) };
    };
    {
      // A1 · a burst: 100 nudges in 600 ms, the counter takes 50 ms
      const f = fake({ lat: 50 }), issued = [], t0 = Date.now(); const done = [];
      for (let i = 0; i < 100; i++) { await sleep(6); const t = Date.now(); issued.push(t); done.push(Push.nudge(f, FV, '').then(ok => ({ ok, at: Date.now() }))); }
      const res = await Promise.all(done), span = Date.now() - t0, w = f.log.writes;
      assert(res.every(r => r.ok === true), 'every nudge of the burst is answered true');
      assert(w.length < 100 / 3, `100 nudges are a few writes, not 100: ${w.length}`);
      assert(w.length <= Math.ceil(span / Push.GAP_MS) + 1, `no more than one write each gap (${Push.GAP_MS} ms): ${w.length} in ${span} ms`);
      for (let i = 1; i < w.length; i++) assert(w[i].begin >= w[i - 1].end && w[i].begin - w[i - 1].begin >= Push.GAP_MS - 5, 'one write in the air at a time, a gap between their beginnings');
      res.forEach((r, i) => assert(w.some(x => x.begin >= issued[i] && x.end <= r.at), `nudge ${i} was answered by a write that began after it (the last nudge is never lost, nothing is left for a later request)`));
      assert(w.every(x => x.path === 'Charm_Nest_Push/counter' && x.opt.merge === true && Object.keys(x.data).sort().join() === 'at,n' && x.data.n.__inc === 1 && x.data.at.__ts), 'each write is { n: increment(1), at: server time } on the real world\'s counter, merged, and nothing else');
      say(`A · 100 nudges in ${span} ms → ${w.length} writes (${(w.length / (span / 1000)).toFixed(1)}/s), each answered by a write that began after it`);
    }
    {
      // A2 · one after another (an instance's sequential requests): each waits out the gap, and nobody waits for nothing
      const f = fake({ lat: 20 }), t0 = Date.now(); const first = Push.nudge(f, FV, ''); await first; const tFirst = Date.now() - t0;
      for (let i = 0; i < 6; i++) await Push.nudge(f, FV, '');
      const span = Date.now() - t0, w = f.log.writes;
      assert(tFirst < 150, 'the first nudge of a quiet spell is written at once: ' + tFirst + ' ms');
      assert(w.length === 7 && span >= 6 * (Push.GAP_MS - 25), `a stream one after another is held to the gap: ${w.length} writes in ${span} ms`);
      say(`A · 7 nudges one after another → ${w.length} writes in ${span} ms (the first at once, ${tFirst} ms; then one each ${Push.GAP_MS} ms)`);
    }
    {
      // A3 · a refused write is tried again; two refusals are given up, never thrown; a hung one does not hold the nudges behind it
      let f = fake({ fail: n => n === 1 }); const s0 = JSON.parse(JSON.stringify(Push.stats));
      assert.equal(await Push.nudge(f, FV, ''), true, 'a write refused once is tried again');
      assert.equal(f.log.writes.length, 2, 'two tries'); assert.equal(Push.stats.retried - s0.retried, 1);
      f = fake({ fail: () => true });
      const t0 = Date.now(); let threw = false, r; try { r = await Push.nudge(f, FV, ''); } catch (_) { threw = true; }
      assert(!threw && r === false, 'refused twice: false, never a throw'); assert(Date.now() - t0 < 1000, 'and quickly');
      assert(Push.stats.failed - s0.failed === 1, 'counted as failed');
      f = fake({ hang: n => n === 1 }); const t1 = Date.now();
      const a = await Push.nudge(f, FV, ''), tA = Date.now() - t1;
      assert(a === true || a === false, 'a hung write is answered'); assert(tA <= Push.WAIT_MS + 150, `a hung write holds nobody longer than the cap: ${tA} ms`);
      const t2 = Date.now(), b = await Push.nudge(f, FV, ''), tB = Date.now() - t2;
      assert(b === true && tB < Push.WAIT_MS, `the nudge after a hung write is written (the hung try is abandoned after its own limit): ${tB} ms, ${f.log.writes.length} tries`);
      f = fake({ hang: () => true }); const t3 = Date.now(), h1 = Push.nudge(f, FV, ''), h2 = Push.nudge(f, FV, ''); const hs = await Promise.all([h1, h2]), tH = Date.now() - t3;
      assert(hs.every(x => x === false) && tH <= Push.WAIT_MS + 150, `every write hanging: every nudge answered false at the cap (${tH} ms), none left waiting`);
      assert.equal(await Push.nudge({ collection: () => { throw new Error('no such thing'); } }, FV, ''), false, 'a database that throws at once: false');
      assert.equal(await Push.nudge(null, FV, ''), false, 'no database: false');
      say(`A · refused once → tried again and written; refused twice → false (no throw); hung → answered in ${tA} ms (cap ${Push.WAIT_MS}), the next nudge written in ${tB} ms`);
    }
    {
      // A4 · two worlds never meet: the sandbox's counter, apart, and one in the air does not hold the other
      const f = fake({ lat: 120 }); const p = Push.nudge(f, FV, ''), s = Push.nudge(f, FV, 'Sandbox_'); const t0 = Date.now(); await Promise.all([p, s]);
      assert.deepEqual(f.log.writes.map(w => w.path).sort(), ['Charm_Nest_Push/counter', 'Sandbox_Charm_Nest_Push/counter'], 'each world writes its own document');
      assert(Date.now() - t0 < 250, 'the two worlds write side by side, not one after the other');
      assert(Math.abs(f.log.writes[0].begin - f.log.writes[1].begin) < 60, 'at the same time');
      say('A · two worlds: Charm_Nest_Push/counter and Sandbox_Charm_Nest_Push/counter, side by side');
    }

    /* ── B · through the real handler ── */
    const counter = prefix => st.doc(prefix + 'Charm_Nest_Push', 'counter');
    await sleep(Push.GAP_MS + 20);
    assert(!counter('') && !counter('Sandbox_'), 'nothing written before a write');
    let r = await call({ op: 'putSheet', sheet: { id: 'pending-sheet', note: 'w1', orders: ['3700000102'] } });
    assert.equal(r.status, 200); let c = counter('');
    assert(c && c.n === 1 && c.at, 'a write that moves the feed raises the counter: ' + JSON.stringify(c));
    assert.deepEqual(Object.keys(c).sort(), ['at', 'n'], 'the counter holds { n, at } and nothing else');
    assert(!JSON.stringify(c).match(/3700000102|pending-sheet|[spotxcmar]:/), 'no order id, sheet id or key in it');
    assert(!counter('Sandbox_'), 'the sandbox\'s counter is not touched by a production write');
    await sleep(Push.GAP_MS + 20);
    r = await call({ op: 'poolUpdate', poolIds: ['3700000101_12345_1'], patch: { note: 'y' } }); assert.equal(r.status, 200);
    assert.equal(counter('').n, 2, 'a second write: 2 (a count that only goes up)');
    await sleep(Push.GAP_MS + 20);
    const n0 = counter('').n, w0 = Push.stats.writes;
    r = await call({ op: 'listSheets', excludeDone: true }); assert.equal(r.status, 200);
    r = await call({ op: 'laserStatus', sheetIds: ['ready-sheet'] }); assert.equal(r.status, 200);
    assert.equal(counter('').n, n0, 'a read moves nothing'); assert.equal(Push.stats.writes, w0, 'and writes nothing');
    st.put('Sandbox_Charm_Nest_Sheets', 'sb-sheet', { id: 'sb-sheet', metal: 'gold', day: '2026-10-28' });
    r = await call({ op: 'putSheet', sandbox: true, sheet: { id: 'sb-sheet', note: 'sandbox' } }); assert.equal(r.status, 200);
    c = counter('Sandbox_'); assert(c && c.n === 1 && Object.keys(c).sort().join() === 'at,n', 'a sandbox write raises the sandbox\'s counter: ' + JSON.stringify(c));
    assert.equal(counter('').n, n0, 'and not the real one');
    await sleep(Push.GAP_MS + 20);
    r = await call({ op: 'timelineAdd', events: [{ orderId: '3700000100', type: 'note', at: Date.now(), by: 'Bea', text: 'a', id: 'push-1' }] });
    assert.equal(r.status, 200); assert.equal(counter('').n, n0 + 1, 'a timeline event (feed shard o) raises it');
    // a Team message raises an order's Order_Rev through _orderTimeline.touchNow (firebaseOrders): the counter follows it
    await sleep(Push.GAP_MS + 20);
    const Timeline = require(path.join(fnDir, '_orderTimeline.js')), FVr = st.admin.firestore.FieldValue, n1 = counter('').n;
    await Timeline.touchNow(db, FVr, ['3700000100'], '');
    assert(st.doc('Order_Rev', '3700000100').n > 0 && counter('').n === n1 + 1, 'a Team message\'s Order_Rev raise moves the counter');
    await Timeline.touchNow(db, FVr, ['3700000100'], 'Sandbox_');
    assert(counter('Sandbox_').n === 2, 'and in the sandbox the sandbox\'s');
    say(`B · a write raises Charm_Nest_Push/counter ({n, at} only, no ids), the sandbox's its own; a read, nothing; a timeline event and a Team message's Order_Rev raise it too (now ${counter('').n} / sandbox ${counter('Sandbox_').n})`);

    const col0 = db.collection, push = /Charm_Nest_Push/;
    // B3 · 30 writes at once, the counter taking 40 ms: each answer comes after a counter write that began after ITS shards were written
    await sleep(Push.GAP_MS + 20);
    db.collection = c => { if (push.test(c)) { const o = col0(c); return { doc: id => { const d = o.doc(id); return { set: async (data, opt) => { events.push({ k: 'cbegin', seq: ++seq }); await sleep(40); const x = await d.set(data, opt); events.push({ k: 'cend', seq: ++seq }); return x; } }; } }; } return col0(c); };
    let seq = 0; const events = [], shardSeq = new Map(), set0 = st.docs.set;
    st.docs.set = (k, v) => { const x = set0.call(st.docs, k, v); if (k === 'Charm_Nest_Rev/feed-o' && v && v.changed) for (const [name, n] of Object.entries(v.changed)) if (n === v.n && !shardSeq.has(name)) shardSeq.set(name, ++seq); return x; };
    const ids = Array.from({ length: 30 }, (_, i) => String(3800000000 + i)), answered = new Map(), writes0 = Push.stats.writes, nBefore = counter('').n, t0 = Date.now();
    await Promise.all(ids.map(async (id, i) => { await sleep((i % 6) * 25); const x = await call({ op: 'timelineAdd', events: [{ orderId: id, type: 'note', at: Date.now() + i, by: 'Bea', text: 'burst ' + i, id: 'burst-' + i }] }); assert.equal(x.status, 200); answered.set(id, ++seq); }));
    const span = Date.now() - t0; st.docs.set = set0; db.collection = col0;
    for (const id of ids) {
      const s = shardSeq.get('o:' + id), a = answered.get(id); assert(s && a, 'the shard commit and the answer of ' + id + ' were seen');
      const begun = events.filter(e => e.k === 'cbegin' && e.seq > s), ended = events.filter(e => e.k === 'cend' && e.seq < a);
      assert(begun.length && ended.some(e => begun.some(b => b.seq < e.seq && e.seq > s)), `the answer of ${id} came after a counter write that began after its shards (the last nudge is not lost)`);
    }
    const writes = Push.stats.writes - writes0, bumps = counter('').n - nBefore;
    assert(writes < 30 && bumps === writes, `30 writes at once → ${writes} counter writes, each one landed (n moved ${bumps})`);
    say(`B · 30 writes at once over ${span} ms → ${writes} counter writes (${(writes / (span / 1000)).toFixed(1)}/s), every answer after a counter write that began after its own shards`);

    // B2 · the person's write never fails: the counter refuses every write, or hangs
    await sleep(Push.GAP_MS + 20);
    db.collection = c => { if (push.test(c)) return { doc: () => ({ set: async () => { throw new Error('UNAVAILABLE: the counter refuses (test)'); } }) }; return col0(c); };
    const f0 = Push.stats.failed, shards0 = st.doc('Charm_Nest_Rev', 'feed-p') ? st.doc('Charm_Nest_Rev', 'feed-p').n : 0;
    r = await call({ op: 'poolUpdate', poolIds: ['3700000101_12345_1'], patch: { note: 'refused' } });
    assert.equal(r.status, 200, 'the person\'s write answers 200 with the counter refusing'); assert(/p:3700000101/.test(r.headers['X-CN-Feed'] || ''), 'and still names what it wrote');
    assert(st.doc('Charm_Nest_Rev', 'feed-p').n > shards0, 'its shards are written'); assert(Push.stats.failed - f0 === 1, 'the counter failure is counted');
    db.collection = c => { if (push.test(c)) return { doc: () => ({ set: () => new Promise(() => {}) }) }; return col0(c); };
    await sleep(Push.GAP_MS + 20);
    r = await call({ op: 'poolUpdate', poolIds: ['3700000101_12345_1'], patch: { note: 'hung' } });
    assert.equal(r.status, 200, 'the counter hanging: still 200'); assert(r.ms <= 2500 + 400, `and within the existing cap (2.5 s): ${r.ms} ms`);
    db.collection = col0;
    say(`B · the counter refusing every write: the write answers 200 with its names; the counter hanging: 200 in ${r.ms} ms (cap 2.5 s)`);

    /* ── C · the registry ── */
    const Fam = require(path.join(root, 'charm-nest-sandbox-families.js'));
    assert.deepEqual(Fam.ownerOf('firestore', 'Sandbox_Charm_Nest_Push/counter'), { kind: 'family', key: 'Charm_Nest_Push' }, 'the sandbox\'s counter is a family: the wipe clears it');
    assert.equal(Fam.ownerOf('firestore', 'Charm_Nest_Push/counter').kind, 'protected', 'the real one is kept');
    assert(Fam.firestoreCollections().includes('Sandbox_Charm_Nest_Push'));
    say('C · the registry: Sandbox_Charm_Nest_Push is a family (the wipe guard drives it: sandbox-wipe-guard.cjs), Charm_Nest_Push is kept');
    console.log('push-server: OK');
  } catch (e) { console.error(e); process.exitCode = 1; }
  finally { srv.close(); }
})();
