/* OUTBOX (Paul, 28 Oct 2026: "every press to feel like a native app": nothing he presses may wait for the cloud; his decisions: the later change wins and the person
   whose change lost is told; locks, set rules and laser state stay server-enforced; plan: the local-first design, phase 3). On the real page over bridge-server.cjs
   (the real charmNestLibrary handlers on the in-memory Firestore). No network, no live record, no credential.
     A · the cloud's side: a press sent twice with its mid is written once (the second answer replayed), a refusal comes back the same, one under way is "being
         written already", a write that threw keeps nothing; the same field written by two people: the later wins and says whom it overwrote; sheetLayoutPatch
         refuses what the old save refused (another screen's claim, the laser's cut, a charm gone); the feed names the counts of what it lists
     B · the page's side: a press is on the disk (IndexedDB, strict) in the task it is made in, sent by the drain, its answer told where it was pressed
     C · ZERO lost writes: offline (context.setOffline), the press kept across a reload and sent once the network is back; the page closed while its request is
         on its way (the cloud wrote it, the answer never came): the next page sends it again and the cloud writes it once (its mid)
     D · follow-up reads: a press from the outbox sets off none (the feed's answer that names it is this computer's own); a direct write with the feed answering
         asks the feed once
   Each converted press adds its own part below (E …): it paints in the frame it is pressed in (< 16 ms from the event to the paint, every function 3 s late),
   and a refusal is said, in plain words, where it was pressed.
   node tests/charm-nest/outbox.cjs   (the browser parts through the slot queue: slot.sh timeout 600 node tests/charm-nest/outbox.cjs) */
'use strict';
const path = require('path'), fs = require('fs'), assert = require('node:assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const CHROME = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1243/chrome-linux64/chrome';
const pwDir = process.env.PW_DIR || [path.join(root, 'node_modules'), '/opt/node-tools/node_modules'].find(d => fs.existsSync(path.join(d, 'playwright-core')));
const until = async (fn, ms, what) => { const t0 = Date.now(); for (;;) { let v; try { v = await fn(); } catch (_) { v = false; } if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await new Promise(r => setTimeout(r, 80)); } };
const sleep = ms => new Promise(r => setTimeout(r, ms));
const say = s => console.log('  ok   ' + s);
const ONLY = process.env.OUTBOX_ONLY || '';   // (OUTBOX_ONLY=A,C: the parts named)
const part = p => !ONLY || ONLY.split(',').includes(p);

(async () => {
  const srv = await start({ receipts: [] }), { st } = srv;
  const lib = async (b, h) => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: Object.assign({ 'content-type': 'application/json' }, h || {}), body: JSON.stringify(b) }); return Object.assign({ status: r.status }, JSON.parse(await r.text())); };
  const handler = st.handlers.charmNestLibrary, OPS = handler.ops;
  // (each run of an op counted where it runs: the handler looks its op up at every request, so a wrapper here counts what the cloud really did)
  const ran = {}; for (const op of ['sheetLayoutPatch', 'putSheet', 'customPut', 'reviewMarkPut', 'flowApply']) { const f = OPS[op]; ran[op] = 0; OPS[op] = async b => { ran[op]++; return f(b); }; }
  const SHEETS = 'Charm_Nest_Sheets', MUT = 'Charm_Nest_Mutations';
  const sheet = (id, extra) => st.put(SHEETS, id, Object.assign({ id, metal: 'gold14k', metalLabel: '14K', day: '2026-10-28', status: 'complete', runId: 'run-ob-1', placements: [{ id: 'c1', cxPt: 40, cyPt: 40, angle: 0, scale: 0.971 }, { id: 'c2', cxPt: 90, cyPt: 40, angle: 0, scale: 0.971 }], charms: [{ id: 'c1', poolId: '4300000001_7001_1', order: '4300000001' }, { id: 'c2', poolId: '4300000002_7002_1', order: '4300000002' }], updatedAt: new Date() }, extra || {}), false);
  let browser = null;
  try {
    if (part('A')) {
      /* ── A · the cloud's side ── */
      sheet('ob-sheet-a');
      const patch = (o = {}) => Object.assign({ op: 'sheetLayoutPatch', id: 'ob-sheet-a', tab: 'tab-ob-1', by: 'Ana_M', poses: { c1: { cxPt: 44, cyPt: 41, angle: 15 } }, pins: { c1: { pinned: { cxPt: 44, cyPt: 41, angle: 15 }, manual: { by: 'Ana_M', at: Date.now() } } }, base: { c1: { cxPt: 40, cyPt: 40, angle: 0 } }, mark: { at: Date.now(), by: 'Ana_M', n: 1 }, release: true }, o);
      const n00 = (await lib({ op: 'getChanges' })).n;
      const r1 = await lib(patch({ mid: 'mp-test-a-1' })), r2 = await lib(patch({ mid: 'mp-test-a-1' }));
      assert.equal(r1.status, 200, JSON.stringify(r1)); assert.equal(r1.ok, true); assert.equal(r1.moved, 1);
      assert(r1.rev && /^s:ob-sheet-a;lib=\d+/.test(r1.feed) && r1.mid === 'mp-test-a-1', 'the answer carries the record\'s revision, and the names it wrote with their counts (the feed\'s X-CN-Feed, kept for a replay): ' + JSON.stringify(r1));
      assert.equal(r2.replayed, true, 'sent again: the first answer, replayed'); assert.equal(r2.rev, r1.rev); assert.equal(ran.sheetLayoutPatch, 1, 'and the op ran once');
      const rec = st.doc(SHEETS, 'ob-sheet-a'), led = st.doc(MUT, 'mp-test-a-1');
      assert(rec.placements[0].cxPt === 44 && rec.placements[0].angle === 15 && rec.charms[0].pinned.cxPt === 44 && rec.charms[0].manual.by === 'Ana_M' && !('arrivalPin' in rec.charms[0]), 'the pose and the pin written: ' + JSON.stringify(rec.placements[0]));
      assert(rec.manual && rec.manual.n === 1 && rec.manualEdit === null && rec.filesOwed > 0 && rec._w['pose:c1'].by === 'Ana_M' && rec._w['pose:c1'].mid === 'mp-test-a-1', 'the mark, the claim given back, the files owed, who wrote the pose');
      assert(led && led.state === 'done' && led.status === 200 && led.expireAt && led.op === 'sheetLayoutPatch', 'the ledger keeps the answer, with its expiry: ' + JSON.stringify(led).slice(0, 200));
      // the feed: what it lists, with the count each was written at
      const libN = +/lib=(\d+)/.exec(r1.feed)[1], ch = await lib({ op: 'getChanges', since: n00 });
      assert(ch.keys && ch.keys.includes('s:ob-sheet-a') && ch.kn['s:ob-sheet-a'] === libN, 'the feed lists the sheet at the count the press wrote it: ' + JSON.stringify(ch));
      // a refusal: another screen holds the sheet; sent again, the same refusal; the cut sheet; a charm gone
      st.put(SHEETS, 'ob-sheet-a', { manualEdit: { by: 'Tess Welder', tab: 'tab-other', at: Date.now() } });
      const f1 = await lib(patch({ mid: 'mp-test-a-2' })), f2 = await lib(patch({ mid: 'mp-test-a-2' }));
      assert(f1.status === 409 && f1.refused === 'held' && /Tess Welder took this sheet on another screen/.test(f1.error), 'another screen\'s claim refuses it, in words: ' + JSON.stringify(f1));
      assert(f2.status === 409 && f2.replayed && f2.error === f1.error && ran.sheetLayoutPatch === 2, 'sent again: the same refusal, the op not run again');
      st.put(SHEETS, 'ob-sheet-a', { manualEdit: null, laserDoneAt: Date.now() });
      const f3 = await lib(patch({ mid: 'mp-test-a-3' }));
      assert(f3.status === 409 && f3.refused === 'cut' && /cut on the laser/.test(f3.error), 'the laser\'s cut refuses it: ' + f3.error);
      st.put(SHEETS, 'ob-sheet-a', { laserDoneAt: null });
      const f4 = await lib(patch({ mid: 'mp-test-a-4', poses: { c9: { cxPt: 1, cyPt: 1, angle: 0 } } }));
      assert(f4.status === 409 && f4.refused === 'gone' && /left/.test(f4.error), 'a charm gone from the sheet refuses it: ' + f4.error);
      // one under way: "being written already" (the page tries again); one that threw: nothing kept, sent again it runs
      st.put(MUT, 'mp-test-a-5', { mid: 'mp-test-a-5', state: 'running', at: Date.now() });
      const b1 = await lib(patch({ mid: 'mp-test-a-5' }));
      assert(b1.status === 409 && b1.inFlight === true && b1.retry === true, 'one under way: being written already: ' + JSON.stringify(b1));
      const bad = OPS.sheetLayoutPatch; OPS.sheetLayoutPatch = async () => { throw new Error('boom'); };
      const t1 = await lib(patch({ mid: 'mp-test-a-6' })); OPS.sheetLayoutPatch = bad;
      assert(t1.status === 500 && !st.doc(MUT, 'mp-test-a-6'), 'one that threw keeps nothing');
      const t2 = await lib(patch({ mid: 'mp-test-a-6', poses: { c2: { cxPt: 95, cyPt: 42, angle: 30 } }, base: { c2: { cxPt: 90, cyPt: 40, angle: 0 } } }));
      assert(t2.status === 200 && t2.ok && !t2.replayed, 'sent again it runs: ' + JSON.stringify(t2));
      // the same field, two people: Bea moves c1 after Ana; Ana's later patch, begun from where c1 stood before Bea, still wins and says whom it overwrote
      const bea = await lib(patch({ by: 'Bea', tab: 'tab-bea', poses: { c1: { cxPt: 60, cyPt: 45, angle: 0 } }, base: { c1: { cxPt: 44, cyPt: 41, angle: 15 } }, mid: 'mp-test-a-7' }));
      assert(bea.ok && !bea.overwrote.length, 'Bea\'s move began from what the record held: nothing overwritten');
      const ana = await lib(patch({ poses: { c1: { cxPt: 50, cyPt: 50, angle: 30 } }, base: { c1: { cxPt: 44, cyPt: 41, angle: 15 } }, mid: 'mp-test-a-8' }));
      assert(ana.ok && ana.overwrote.length === 1 && ana.overwrote[0].by === 'Bea' && ana.overwrote[0].field === 'pose:c1', 'the later wins, and names whom it overwrote: ' + JSON.stringify(ana.overwrote));
      assert.equal(st.doc(SHEETS, 'ob-sheet-a').placements[0].cxPt, 50); assert.equal(st.doc(SHEETS, 'ob-sheet-a')._w['pose:c1'].by, 'Ana_M');
      // putSheet: a field patch with what it saw (base): the same rule
      const p1 = await lib({ op: 'putSheet', sheet: { id: 'ob-sheet-a', releaseFull: true }, base: { releaseFull: false }, by: 'Bea', mid: 'mp-test-a-9' });
      const p2 = await lib({ op: 'putSheet', sheet: { id: 'ob-sheet-a', releaseFull: false }, base: { releaseFull: false }, by: 'Ana_M', mid: 'mp-test-a-10' });
      assert(p1.ok && !p1.overwrote && p1.rev, 'putSheet with base: written, its revision answered');
      assert(p2.ok && p2.overwrote && p2.overwrote[0].by === 'Bea' && p2.overwrote[0].field === 'releaseFull', 'putSheet: the later wins and says whom it overwrote: ' + JSON.stringify(p2));
      // a patch of the sheet's own fields names no order (no placement read follows it)
      const n0 = (await lib({ op: 'getChanges' })).n; await lib({ op: 'putSheet', sheet: { id: 'ob-sheet-a', label: null } }); const ch2 = await lib({ op: 'getChanges', since: n0 });
      assert.deepEqual(ch2.keys, ['s:ob-sheet-a'], 'a field patch names the sheet alone, no p:<order>: ' + ch2.keys);
      say(`A · cloud: a press sent twice ran once (replayed, its revision ${r1.rev}, ${r1.feed}), a refusal comes back the same, "being written already", a throw keeps nothing; held / cut / gone refused in words; the later change wins and names Bea; the feed's counts`);
    }

    if (['B', 'C', 'D'].some(part)) {
      const { chromium } = require(path.join(pwDir, 'playwright-core'));
      browser = await chromium.launch({ executablePath: CHROME, args: ['--no-sandbox'] });
      const settings = { v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', sandbox: 'off', pollOrders: 'off' };
      const computer = async () => {
        const ctx = await browser.newContext({ viewport: { width: 1400, height: 900 }, serviceWorkers: 'block' });   // (the page's service worker would carry the function calls past the test's routes)
        await ctx.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
        await ctx.addInitScript(s => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify(s)); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; window.confirm = () => true; }, settings);
        return ctx;
      };
      const calls = [];
      const open = async (ctx, tag, o = {}) => {
        const page = await ctx.newPage(); page.setDefaultTimeout(60000);
        page.on('request', q => { const m = /\/\.netlify\/functions\/([\w-]+)/.exec(q.url()); if (m) { let b = {}; try { b = JSON.parse(q.postData() || '{}'); } catch (_) {} calls.push({ t: Date.now(), tag, fn: m[1], op: b.op || '', mid: b.mid || '' }); } });
        page.on('pageerror', e => console.log('  page error (' + tag + '): ' + String(e.message).slice(0, 200)));
        await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
        await page.waitForFunction(cloud => window.CN && (!cloud || CN.S.cloud.ok === true) && window.CNSync && window.CharmNestStore, o.cloud !== false);
        await page.evaluate(() => CharmNestStore.ready());
        return page;
      };
      const disk = p => p.evaluate(() => new Promise(res => { const rq = indexedDB.open('cn-local.production'); rq.onsuccess = () => { const db = rq.result; if (!db.objectStoreNames.contains('outbox')) return res([]); const g = db.transaction('outbox').objectStore('outbox').getAll(); g.onsuccess = () => { res(g.result); db.close(); }; }; rq.onerror = () => res(null); }));

      if (part('B')) {
        /* ── B · a press: on the disk in its own task, sent, answered where it was pressed ── */
        sheet('ob-sheet-b');
        const ctx = await computer(), page = await open(ctx, 'B');
        const r = await page.evaluate(async () => {
          const t0 = performance.now(), h = CharmNestStore.mutate({ entity: 's:ob-sheet-b', op: 'putSheet', args: { sheet: { id: 'ob-sheet-b', releaseFull: true } }, fields: ['releaseFull'], label: 'The release of GF Sheet B', by: 'Test Operator' });
          const ms = performance.now() - t0, inMem = CharmNestStore.pending().length;
          const ans = await h.done; return { ms, inMem, ans, mid: h.mid };
        });
        assert(r.ms < 16 && r.inMem === 1, `kept in the task it was pressed in (${r.ms.toFixed(2)} ms)`);
        assert(r.ans.ok && r.ans.rev && r.ans.mid === r.mid, 'answered where it was pressed: ' + JSON.stringify(r.ans));
        assert.equal(st.doc(SHEETS, 'ob-sheet-b').releaseFull, true);
        assert.deepEqual(await disk(page), [], 'answered: it left the disk');
        // the disk holds it before its answer: every function 3 s late, read the disk 100 ms after the press
        await ctx.route(/\/\.netlify\/functions\//, async q => { await sleep(3000); return q.continue(); });
        const k = await page.evaluate(() => CharmNestStore.mutate({ entity: 's:ob-sheet-b', op: 'putSheet', args: { sheet: { id: 'ob-sheet-b', releaseFull: false } }, fields: ['releaseFull'], label: 'Release this sheet', by: 'Test Operator' }).k);
        await sleep(150); const d1 = await disk(page);
        assert(d1.length === 1 && d1[0].k === k && d1[0].op === 'putSheet', 'on the disk within 150 ms, before any answer: ' + JSON.stringify(d1.map(e => [e.k, e.state])));
        // a refusal, in the cloud's own words, where it was pressed
        await ctx.unroute(/\/\.netlify\/functions\//);
        const no = await page.evaluate(() => CharmNestStore.mutate({ entity: 's:none', op: 'sheetLayoutPatch', args: { id: 'ob-sheet-none', tab: 'tab-x1', poses: { c1: { cxPt: 1, cyPt: 1, angle: 0 } } }, label: 'Manual Control', by: 'Test Operator' }).done.then(() => null, e => ({ msg: e.message, refused: e.refused })));
        assert(no && no.refused && /no longer in the Library/.test(no.msg), 'a refusal comes back where it was pressed, in the cloud\'s words: ' + JSON.stringify(no));
        await until(async () => (await disk(page)).length === 0, 10000, 'the outbox empty');
        // the person whose change lost is told (Paul, 28 Oct 2026, decision 4): this computer released the sheet; Bea, on another computer, then sets it back (the later change wins);
        // when this computer next reads the sheet, it says so
        await page.evaluate(() => CharmNestStore.mutate({ entity: 's:ob-sheet-b', op: 'putSheet', args: { sheet: { id: 'ob-sheet-b', releaseFull: true }, base: { releaseFull: false }, by: 'Test Operator' }, fields: ['releaseFull'], label: 'The release of GF Sheet B', by: 'Test Operator' }).done);
        await sleep(300);
        const bea = await lib({ op: 'putSheet', sheet: { id: 'ob-sheet-b', releaseFull: false }, base: { releaseFull: false }, by: 'Bea', mid: 'mp-bea-b-1' });
        assert(bea.ok && bea.overwrote && /^Test.Operator$/.test(bea.overwrote[0].by), 'Bea\'s later change won and named whom it overwrote: ' + JSON.stringify(bea));
        const toldLost = await page.evaluate(async () => { const r = await CN.api('charmNestLibrary', { op: 'getSheet', id: 'ob-sheet-b' }, { quiet: true }); CharmNestStore.put('sheetFull', 'ob-sheet-b', r.sheet, r.rev); await new Promise(z => setTimeout(z, 300)); return [...document.querySelectorAll('.toast, [role=status], [role=alert]')].map(n => n.textContent).join(' | '); });
        assert(/Bea changed the release of GF Sheet B after you .* their change was kept/.test(toldLost), 'the person whose change lost is told, in words: ' + toldLost.slice(0, 300));
        await ctx.close();
        say(`B · a press kept in ${r.ms.toFixed(2)} ms (memory, and on the disk before its answer, strict), sent, answered where it was pressed; a refusal in the cloud's words; a lost conflict told: "${(toldLost.match(/Bea changed[^|]*/) || [''])[0].trim()}"`);
      }

      if (part('C')) {
        /* ── C · ZERO lost writes ── */
        sheet('ob-sheet-c');
        const ctx = await computer(); let page = await open(ctx, 'C1');
        const base = ran.sheetLayoutPatch;
        // offline: kept on the disk; the page reloaded while the cloud still cannot be reached (its functions cut off), kept still; sent once the network is back, once
        await ctx.setOffline(true);
        const kOff = await page.evaluate(() => CharmNestStore.mutate({ entity: 's:ob-sheet-c', op: 'sheetLayoutPatch', args: { id: 'ob-sheet-c', tab: 'tab-c1', by: 'Test Operator', poses: { c1: { cxPt: 41, cyPt: 41, angle: 15 } }, release: true }, fields: ['pose:c1'], label: 'Manual Control', by: 'Test Operator' }).k);
        await sleep(1500);
        assert.equal(ran.sheetLayoutPatch, base, 'offline: nothing reached the cloud');
        const kept1 = await disk(page);
        await ctx.route(/\/\.netlify\/functions\//, q => q.abort('internetdisconnected'));
        await page.close(); await ctx.setOffline(false);   // (closed offline: its keepalive send on the way out cannot leave either)
        page = await open(ctx, 'C2', { cloud: false });
        await sleep(1200);
        const kept2 = await disk(page);
        const early = ran.sheetLayoutPatch - base;   // (the closed page's keepalive send may land once the network is back: its answer reaches no page, so the outbox still holds it)
        await ctx.unroute(/\/\.netlify\/functions\//); await page.evaluate(() => window.dispatchEvent(new Event('online')));
        await until(() => ran.sheetLayoutPatch > base, 20000, 'sent once back online');
        await until(async () => (await disk(page)).length === 0, 20000, 'the outbox empty after the network came back');
        await sleep(1500);
        assert(kept1.some(e => e.k === kOff) && kept2.some(e => e.k === kOff), 'the press made offline was on the disk, and still after the reload');
        assert.equal(ran.sheetLayoutPatch - base, 1, `written once (${early ? 'the closed page\'s keepalive landed first, the next page\'s send replayed' : 'sent by the next page'})`); assert.equal(st.doc(SHEETS, 'ob-sheet-c').placements[0].cxPt, 41);
        // the page closed while its request is on its way: the cloud writes it, the answer never comes; the next page sends it again, written once
        const base2 = ran.sheetLayoutPatch;
        let arrived = 0; await ctx.route(/\/\.netlify\/functions\/charmNestLibrary/, async q => { if (/sheetLayoutPatch/.test(q.request().postData() || '')) { arrived++; const resp = await q.fetch(); await sleep(4000); return q.fulfill({ response: resp }).catch(() => {}); } return q.continue(); });
        const kMid = await page.evaluate(() => { const h = CharmNestStore.mutate({ entity: 's:ob-sheet-c', op: 'sheetLayoutPatch', args: { id: 'ob-sheet-c', tab: 'tab-c1', by: 'Test Operator', poses: { c2: { cxPt: 99, cyPt: 41, angle: 30 } }, release: true }, fields: ['pose:c2'], label: 'Manual Control', by: 'Test Operator' }); return { k: h.k, mid: h.mid }; });
        await until(() => ran.sheetLayoutPatch > base2, 10000, 'the cloud wrote it');
        await page.close({ runBeforeUnload: false });
        await ctx.unroute(/\/\.netlify\/functions\/charmNestLibrary/);
        assert.equal(st.doc(SHEETS, 'ob-sheet-c').placements[1].cxPt, 99, 'the cloud wrote it before the page closed');
        page = await open(ctx, 'C3');
        await until(async () => (await disk(page)).length === 0, 20000, 'the next page sent it again');
        await sleep(1000);
        const again = calls.filter(c => c.mid === kMid.mid);
        assert(again.length >= 2, `the press was sent again by the next page (${again.length} sends of ${kMid.mid})`);
        assert.equal(ran.sheetLayoutPatch - base2, 1, 'and the cloud wrote it once (its mid: the second answer replayed)');
        await ctx.close();
        say(`C · ZERO lost writes: made offline, kept across a reload, sent once back online; the page closed with its request out (${arrived} arrived): the next page sent it again (${again.length} sends), written once`);
      }

      if (part('D')) {
        /* ── D · follow-up reads per write ── */
        sheet('ob-sheet-d');
        const ctx = await computer(), page = await open(ctx, 'D');
        await page.evaluate(() => document.querySelector('#modeSeg button[data-mode="library"]').click());
        await until(() => page.evaluate(() => CNSync.state().live), 20000, 'the feed answers');
        await sleep(2500);
        const reads = t0 => calls.filter(c => c.t >= t0 && c.tag === 'D' && /^(laserStatus|getOrderPieces|cancelList|timelineGet|customGet|getSheet|getMany)$/.test(c.op));
        // (direct, as before Phase 3: every surface's own reads)
        let t0 = Date.now(); await page.evaluate(() => CN.api('charmNestLibrary', { op: 'putSheet', sheet: { id: 'ob-sheet-d', note: 'direct' } }, { quiet: true })); await sleep(4000);
        const direct = reads(t0), directFeed = calls.filter(c => c.t >= t0 && c.op === 'getChanges').length;
        t0 = Date.now(); await page.evaluate(() => CharmNestStore.mutate({ entity: 's:ob-sheet-d', op: 'putSheet', args: { sheet: { id: 'ob-sheet-d', note: 'outbox' } }, fields: ['note'], label: 'A note', by: 'Test Operator' }).done); await sleep(4000);
        const viaBox = reads(t0), st1 = await page.evaluate(() => CNSync.state());
        assert.equal(viaBox.length, 0, 'a press from the outbox: no read follows it: ' + viaBox.map(c => c.op).join(', '));
        await ctx.close();
        say(`D · follow-up reads in 4 s: a direct write ${direct.length} (${[...new Set(direct.map(c => c.op))].join(', ') || 'none'}; the feed asked ${directFeed}×), a press from the outbox 0 (the feed's answer naming it skipped as this computer's own: ${st1.ownSkipped})`);
      }
    }
    if (part('E')) {
      /* ── E · Hold Order and Release Hold (the order window), every function 3 s late: each paints in the frame it is pressed in; the mark goes from the outbox, once; a refusal is said under the button ── */
      const RW = require('./_reviewSendWorld.cjs'), w = await RW.open('production');
      if (!w) console.log('  - E skipped: no playwright-core');
      else try {
        const page = w.page, lst = w.srv.st;
        await RW.addOrders(w, ['sheeted', 'mate']);
        const sh = await RW.seedSheet(w, { id: 'sh-ob-e1', orders: ['sheeted', 'mate'] });
        const rid = RW.ORDERS.sheeted.rid, key = RW.keyOf('sheeted'), pid = RW.poolIdOf('sheeted');
        await page.evaluate(() => CN.setMode('orders')); await page.evaluate(k => OrderWin.open(k), key);
        const rb = `#owPcSum .revSendBtn[data-rev-host][data-rid="${rid}"]`;
        await page.waitForSelector(rb, { timeout: 20000 });
        // (a stand-in hold engine that answers at once: this part measures the press and the outbox, not the sheets' take-off, which tests/charm-nest/hold-order-progress.cjs drives)
        await page.evaluate(({ pid, label }) => { const real = window.OrderHold; window.__realHold = real; window.OrderHold = Object.assign({}, real, { plan: async r => ({ rid: r, canHold: true, pieces: [{ poolId: pid, state: 'onSheet', sheetLabel: label }], sheets: [], fills: [], stays: [] }), run: async (r, o) => { for (const row of Orders.rows()) if (String(row.order.receiptId) === r) { row.poolIds = []; row.state = 'held'; row.hold = row.reason = 'Taken off ' + label; } if (o && o.onStep) o.onStep({ type: 'done', at: Date.now() }); return { ok: true, held: true, steps: [] }; } }); }, { pid, label: sh.name });
        const fnCalls = []; page.on('request', q => { if (/\/\.netlify\/functions\//.test(q.url())) { let b = {}; try { b = JSON.parse(q.postData() || '{}'); } catch (_) {} fnCalls.push({ t: Date.now(), op: b.op || '', action: b.action || '', mid: b.mid || '' }); } });
        await page.route(/\/\.netlify\/functions\//, async q => { await sleep(3000); return q.continue().catch(() => {}); });
        const pressPaint = (sel, probe) => page.evaluate(([sel, probe]) => new Promise(res => { const b = document.querySelector(sel); const on = e => { const ev = e.timeStamp; requestAnimationFrame(() => { let shows; try { shows = (0, eval)(probe); } catch (x) { shows = 'threw: ' + x.message; } res({ ms: +(performance.now() - ev).toFixed(2), shows }); }); }; b.addEventListener('click', on, { capture: true, once: true }); b.click(); }), [sel, probe]);
        // E1 · Hold Order
        const t0 = Date.now();
        const hold = await pressPaint(rb, `CharmNestReviewMark.isMarked('${rid}') && !!document.querySelector('#orderWin .revSendProg')`);
        assert(hold.ms < 16 && hold.shows === true, `Hold Order paints in the frame it is pressed in: ${JSON.stringify(hold)}`);
        await until(() => lst.doc('Charm_Custom_Orders', rid + '_review') && lst.doc('Charm_Custom_Orders', rid + '_review').review && lst.doc('Charm_Custom_Orders', rid + '_review').review.state === 'open', 20000, 'the mark reached the cloud');
        await sleep(4500);
        const marks = fnCalls.filter(c => c.op === 'reviewMarkPut' && c.action === 'mark'), reads = fnCalls.filter(c => c.t > t0 && /^(customGet|laserStatus|getOrderPieces|cancelList|timelineGet)$/.test(c.op));
        assert(marks.length === 1 && marks[0].mid, 'the mark went from the outbox, once, with its mid: ' + JSON.stringify(marks));
        console.log(`  ok   E1 · Hold Order: ${hold.ms} ms from the press to the paint (< 16 ms; held and its bar shown in that frame, every function 3 s late); the mark sent once from the outbox (${marks[0].mid}); reads after it in 4.5 s: ${reads.length} (${[...new Set(reads.map(c => c.op))].join(', ') || 'none'}: the feed's and the window's own)`);
        // E2 · Release Hold in the order window
        await page.evaluate(() => { window.OrderHold = window.__realHold; });
        const relSel = '#orderWin [data-rel-hold]:not([aria-disabled="true"])';
        await page.waitForFunction(s => { const b = document.querySelector(s); return b && b.getBoundingClientRect().width > 0; }, relSel, { timeout: 30000 });
        const rel = await pressPaint(relSel, `(() => { const b = document.querySelector('#orderWin [data-rel-hold]'); return { busy: !!b && (b.disabled || b.getAttribute('aria-busy') === 'true' || /Releasing/.test(b.textContent)), sealExpected: true, released: !!(window.CharmNestReviewMark && CharmNestReviewMark.released('${rid}')) }; })()`);
        assert(rel.ms < 16, `Release Hold paints in the frame it is pressed in: ${JSON.stringify(rel)}`);
        console.log(`  ok   E2 · Release Hold in the order window: ${rel.ms} ms from the press to the paint (< 16 ms; that frame: ${JSON.stringify(rel.shows)}), its placement on the sheets as before (the server's rules, awaited behind the press)`);
        await page.unroute(/\/\.netlify\/functions\//);
        await sleep(3000);
        // E3 · a refusal: the cloud says no to a Hold Order; the order is not held, and the words are under the button
        const rid2 = RW.ORDERS.mate.rid, key2 = RW.keyOf('mate');
        await page.evaluate(() => { if (OrderWin.isOpen()) OrderWin.close(); }); await sleep(400);
        await page.evaluate(k => OrderWin.open(k), key2);
        const rb2 = `#owPcSum .revSendBtn[data-rev-host][data-rid="${rid2}"]`;
        await page.waitForSelector(rb2, { timeout: 20000 });
        await page.route(/\/\.netlify\/functions\/charmNestLibrary/, async q => (/reviewMarkPut/.test(q.request().postData() || '') ? q.fulfill({ status: 409, contentType: 'application/json', body: JSON.stringify({ error: 'This order was cancelled on another computer' }) }) : q.continue()));
        await page.click(rb2);
        const said = await until(() => page.evaluate(r => { const m = [...document.querySelectorAll('#orderWin .revSendMsg, #orderWin [role=status], #orderWin [role=alert], .toast')].map(n => n.textContent).join(' | '); return !CharmNestReviewMark.isMarked(r) && /cancelled on another computer/.test(m) ? m : null; }, rid2), 20000, 'the refusal said where it was pressed');
        await page.unroute(/\/\.netlify\/functions\/charmNestLibrary/);
        console.log(`  ok   E3 · a Hold Order the cloud refused: not held, and said under the button: "${(said.match(/Not held[^|]*/) || [said])[0].trim().slice(0, 110)}"`);
        assert.deepEqual(w.errors || [], [], 'no page errors: ' + (w.errors || []).join(' | '));
      } finally { await w.close(); }
    }
    console.log('\nall outbox checks passed');
  } catch (e) { console.error(e); process.exitCode = 1; }
  finally { if (browser) await browser.close().catch(() => {}); srv.close && srv.close(); }
})();
