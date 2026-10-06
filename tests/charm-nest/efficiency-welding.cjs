// The Welding card of the live stations board (charm-nest-efficiency-stations.js) and the Welding rows of the Overview (charm-nest-efficiency.js),
// stations round 2, worker WS2. Fakes only: the harness answers the gated read function (op "live") from a hand-built answer in the shape the real
// live layer sends for the Welding station (people with a task, noThroughput, today, matched; plans/stations-round2/api.md, WS2 section C4); every
// request off the loopback is aborted, so nothing reaches the internet, Etsy or Firestore. Invented people and orders, a fake passcode.
//   1 · the view model: task and last input of a person, the matched rows, `noThroughput`; a station without it is untouched
//   2 · two groups (Welding | Matching): who is in each, a person in both is in both, an old sign-in with no task is in Welding and says so;
//       time on task and time since last input (ticking, "seen" when no input has been reported); no pieces, no orders, anywhere on the card
//   3 · today's matched orders with thumbnails, an order scanned twice once with ×2, "N scans · M orders", "Scanned with nobody in Matching" for
//       the unattributed scan, a press opens the order, Show more / Show fewer, a person's name opens their page
//   4 · hover cards: the station (matched, welding and matching time, never pieces or orders), a person (task, time on task, last input)
//   5 · live: a new matched scan arrives at the top, a person leaves a group, the summary counts a person once; the other stations are as before
//   6 · 1440, 900 and 390 px: no sideways scroll, the groups stack on a narrow board; reduced motion; no console errors
//   node tests/charm-nest/efficiency-welding.cjs     (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, SHOTS=<dir for the screenshots>)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const F = require('./efficiency-stations-fixture.cjs');
const sleep = ms => new Promise(r => setTimeout(r, ms));

/** The Welding station as the live layer sends it today: two tasks, two people at once, one person in both, an old sign-in with no task, 9 matched scans. */
function welding(now, o = {}) {
  const H = 3600000, M = 60000, pic = F.pic;
  const dev = { device: 'weld-1', deviceLabel: 'Welding' };
  const person = (name, task, since, lastInputAt, todayMs, lastSeenAt) => Object.assign({ name, since: now - since, lastSeenAt: now - (lastSeenAt == null ? 4000 : lastSeenAt), lastInputAt: lastInputAt == null ? null : now - lastInputAt, todayMs }, task ? { task } : {}, dev);
  const m = (rid, ago, who, extra) => Object.assign({ rid, orderNumber: rid, at: now - ago, person: who, task: 'matching', unattributed: !who, note: who ? '' : 'Scanned with nobody in Matching', customer: who ? 'Fixture Customer ' + rid.slice(-3) : '', thumbUrl: pic(+rid.slice(-3), 'photo'), vectorUrl: '', photoUrl: '', pieceCount: 0, pieces: [] }, extra || {});
  const people = o.people || [
    person('Wanda W.', 'welding', 3 * H, 95000, 3 * H + 10 * M),
    person('Max M.', 'welding', 2 * H, 40 * M, 90 * M),
    person('Max M.', 'matching', 4 * H, 12000, 4 * H),
    person('Ray R.', 'matching', 1 * H, null, 55 * M, 30000),
    person('Giovanna C.', '', 6 * H, null, 5400000, 61000)
  ];
  const matched = o.matched || [m('3521000901', 20000, 'Max M.'), m('3521000902', 150000, 'Max M.'), m('3521000903', 300000, ''), m('3521000901', 620000, 'Ray R.'), m('3521000904', 900000, 'Max M.'), m('3521000905', 1500000, 'Ray R.'), m('3521000906', 2400000, 'Max M.'), m('3521000907', 3000000, 'Ray R.'), m('3521000908', 3600000, 'Max M.')];
  const un = matched.filter(x => x.unattributed).length;
  const st = { key: 'welding', label: 'Welding', state: o.state || 'idle', noThroughput: true, people, names: [...new Set(people.map(p => p.name))], current: [],
    devices: [{ device: 'weld-1', label: 'Welding', state: 'idle', person: 'Wanda W., Max M., Ray R., Giovanna C.', since: now - 6 * H }], lastEventAt: now - 20000,
    counts: { partsToday: null, ordersToday: null, scansToday: matched.length },
    today: { day: '2026-10-06', matched: o.matchedCount != null ? o.matchedCount : matched.length, unattributed: un, taskMs: { welding: 4 * H + 40 * M, matching: 4 * H + 5 * M, unknown: 90 * M } },
    matched };
  return st;
}

(async () => {
  const pwDir = process.env.PW_DIR || (fs.existsSync(path.join(root, 'node_modules/playwright-core')) ? path.join(root, 'node_modules') : '/opt/node22/lib/node_modules/playwright/node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fx = F.make(), seen = { urls: [], aborted: 0 };
  let W = null, SIGNED = null;   // the Welding station of the answer (rebuilt by each test step), and the answer's sign-in rows
  const shots = process.env.SHOTS || '';
  const wire = async ctx => {
    await ctx.route(() => true, async route => {
      const u = new URL(route.request().url()); seen.urls.push(u.href);
      if (u.hostname !== '127.0.0.1' && u.hostname !== 'localhost') { seen.aborted++; return route.abort(); }
      if (u.pathname.endsWith('/employeeEfficiency')) {
        let b = {}; try { b = JSON.parse(route.request().postData() || '{}'); } catch (_) {}
        const r = fx.answer(b);
        return route.fulfill({ status: r.status, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify(r.json) });
      }
      return route.continue();
    });
    await ctx.addInitScript(() => { window.confirm = () => true; window.alert = () => {}; });
  };
  const track = page => { const errs = []; page.on('pageerror', e => errs.push(e.message)); page.on('console', m => { if (m.type() === 'error' && !/Failed to load resource/.test(m.text())) errs.push('console: ' + m.text()); }); return errs; };
  const load = async ctx => {
    const page = await ctx.newPage(), errs = track(page);
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.EfficiencyStations && window.QRCode && window.Motion && window.OrderWin && document.readyState === 'complete', null, { timeout: 60000 });
    await page.evaluate(k => sessionStorage.setItem('cn.eff.key', k), F.KEY);
    return { page, errs };
  };
  const mount = (page, o = {}) => page.evaluate(o => {
    document.querySelector('.app').classList.toggle('railOff', innerWidth < 700);
    for (const c of document.getElementById('stage').children) if (c.id !== 'esHost') c.classList.add('hidden');
    let d = document.getElementById('esHost'); if (!d) { d = document.createElement('div'); d.className = 'lib'; d.id = 'esHost'; document.getElementById('stage').appendChild(d); }
    if (window.__b) { window.__b.unmount(); window.__b = null; }
    d.classList.remove('hidden'); d.textContent = '';
    const E = EfficiencyStations; E.options.pollMs = o.pollMs || 600000; E.options.zoomDelay = 250; E.options.doneMs = 700; E.options.flyMs = 700;
    window.__opened = []; window.__people = []; window.openOrderFrom = (btn, rid) => { window.__opened.push(rid); return true; };
    window.__b = E.mount(d, { own: true, mode: () => 'real', onPerson: n => window.__people.push(n) });
    return true;
  }, o);
  const V = '#esHost', WS = `${V} .esSt[data-key=welding]`;
  const text = (page, sel) => page.$eval(sel, e => e.innerText.replace(/\s+/g, ' ').trim());
  const texts = (page, sel) => page.$$eval(sel, es => es.map(e => e.innerText.replace(/\s+/g, ' ').trim()));
  const noSideways = (page, what) => page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1 && document.getElementById('esHost').scrollWidth <= document.getElementById('esHost').clientWidth + 1).then(ok => assert(ok, what + ': no sideways scroll'));
  const refresh = page => page.evaluate(() => __b.refresh());
  // a hover card: bring the thing into view first (a scroll hides a card), move off, rest on it
  const tipOn = async (page, sel) => { for (let i = 0; i < 3; i++) { await (await page.$(sel)).scrollIntoViewIfNeeded(); await page.mouse.move(2, 2); await sleep(250); await page.hover(sel); try { await page.waitForSelector('.esTip[data-on]', { timeout: 2500 }); return page.$eval('.esTip', e => e.innerText.replace(/\s+/g, ' ')); } catch (_) {} } throw new Error('no hover card on ' + sel); };
  const shot = async (page, name, sel) => { if (!shots) return; fs.mkdirSync(shots, { recursive: true }); await (sel ? (await page.$(sel)).screenshot({ path: path.join(shots, name) }) : page.screenshot({ path: path.join(shots, name) })); };
  try {
    fx.setHook(live => { live.stations = live.stations.map(s => (s.key === 'welding' ? W : s)); if (SIGNED) live.signedIn = SIGNED; return live; });
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await wire(ctx);
    const { page, errs } = await load(ctx);

    /* ── 1 · the view model ── */
    {
      W = welding(Date.now());
      const r = await page.evaluate(w => {
        const n = EfficiencyStations.norm({ at: 1e12 + 5, stations: [w, { key: 'assembly', label: 'Assembly', people: ['Ann A.'], counts: { partsToday: 5, ordersToday: 2 } }], signedIn: [] });
        const s = n.stations[0], a = n.stations[1];
        return { noT: s.noThroughput, counts: s.counts, today: s.today, people: s.people.map(p => [p.name, p.task, p.todayMs != null, !!p.lastInputAt]), matched: s.matched.map(m => [m.rid, m.unattributed, m.person, m.note]), a: [a.noThroughput, a.today, a.matched.length, a.counts.parts, a.counts.orders] };
      }, W);
      assert.equal(r.noT, true); assert.deepEqual(r.counts, { parts: null, orders: null, scans: 9 }, 'no pieces, no orders: the matched scans stand in the scan count');
      assert.deepEqual(r.today, { matched: 9, unattributed: 1, taskMs: { welding: 4 * 3600000 + 40 * 60000, matching: 4 * 3600000 + 5 * 60000, unknown: 90 * 60000 } });
      assert.deepEqual(r.people.map(p => p.slice(0, 2)), [['Wanda W.', 'welding'], ['Max M.', 'welding'], ['Max M.', 'matching'], ['Ray R.', 'matching'], ['Giovanna C.', '']], 'a person in two tasks is two entries; an old sign-in has no task');
      assert.deepEqual(r.people.map(p => p[3]), [true, true, true, false, false], 'last input only where one was reported');
      assert.deepEqual(r.matched[2], ['3521000903', true, '', 'Scanned with nobody in Matching'], 'the unattributed scan has no person and says so');
      assert.deepEqual(r.a, [false, null, 0, 5, 2], 'a station without noThroughput is read as before');
      const bad = await page.evaluate(() => { const n = EfficiencyStations.norm({ stations: [{ key: 'welding', label: 'Welding', noThroughput: true, people: [{ name: 'X', task: 'sabotage', todayMs: 'nope' }], matched: [null, {}, { rid: '12', unattributed: true }], today: { matched: 'x' } }] }); const s = n.stations[0]; return { p: s.people[0], m: s.matched.map(x => [x.rid, x.note]), t: s.today }; });
      assert.equal(bad.p.task, '', 'an unknown task is no task'); assert.equal(bad.p.todayMs, null); assert.deepEqual(bad.m, [['12', 'Scanned with nobody in Matching']], 'rows without an order are dropped; a missing note is filled in'); assert.deepEqual(bad.t, { matched: null, unattributed: null, taskMs: null }, 'junk is empty, never a guess');
      console.log('  ✓ view model: tasks, last input, time on task and the matched rows are read; junk is empty; a station without noThroughput is untouched');
    }

    /* ── 2 · the card: two groups, who is in each, time on task, last input ── */
    W = welding(Date.now()); SIGNED = W.people.map(p => ({ name: p.name, stationKey: 'welding', device: 'weld-1', since: p.since, lastSeenAt: p.lastSeenAt, task: p.task, lastInputAt: p.lastInputAt }));
    await mount(page); await page.waitForSelector(`${WS} .esGrp`); await page.waitForSelector(`${WS} .esMr`);
    {
      assert.equal(await page.$eval(WS, r => r.dataset.weld), '1');
      const g = await page.$$eval(`${WS} .esGrp`, gs => gs.map(x => ({ task: x.dataset.task, title: x.querySelector('.esGrpH b').textContent, n: x.querySelector('.esGrpN').textContent, t: x.querySelector('.esGrpT').textContent, who: [...x.querySelectorAll('.esWp')].map(p => ({ name: p.querySelector('.esPn').textContent, tm: p.querySelector('.esWm').textContent, inp: p.querySelector('.esWi').textContent, tag: !p.querySelector('.esTag').hidden })) })));
      assert.deepEqual(g.map(x => [x.task, x.title]), [['welding', 'Welding'], ['matching', 'Matching']], 'two groups, Welding then Matching');
      assert.deepEqual(g[0].who.map(p => p.name), ['Wanda W.', 'Max M.', 'Giovanna C.'], 'Welding: the two welders and the old sign-in with no task');
      assert.deepEqual(g[1].who.map(p => p.name), ['Max M.', 'Ray R.'], 'Matching: Max (who is in both) and Ray');
      assert.deepEqual(g[0].who.map(p => p.tag), [false, false, true], 'only the old sign-in says its task was not recorded');
      assert.equal(g[0].n, '3 in'); assert.equal(g[1].n, '2 in');
      assert.equal(g[0].t, '6 h 10 m today', 'Welding time = welding 4 h 40 m + the 1 h 30 m with no task (read as Welding)'); assert.equal(g[1].t, '4 h 5 m today');
      assert.deepEqual(g[0].who.map(p => p.tm), ['3 h 10 m on task', '1 h 30 m on task', '1 h 30 m on task']);
      assert.deepEqual(g[1].who.map(p => p.tm), ['4 h on task', '55 m on task']);
      assert.match(g[0].who[0].inp, /^last input (9[0-9]|1\d\d)s ago$|^last input 1 m ago$/, 'time since last input'); assert.match(g[0].who[1].inp, /^last input 40 m ago$/); assert.match(g[1].who[0].inp, /^last input \d+s ago$/);
      assert.match(g[1].who[1].inp, /^seen \d+s ago$/, 'no input reported yet: the last sign of life, labelled as seen'); assert.match(g[0].who[2].inp, /^seen 1 m ago$/);
      assert.equal(await page.$eval(`${WS} .esWp[data-name="Max M."][data-task=welding] .esWi`, e => e.dataset.cold), '1', 'a long quiet time is marked'); assert.equal(await page.$eval(`${WS} .esWp[data-name="Wanda W."] .esWi`, e => e.dataset.cold), '');
      // no pieces, no orders, anywhere on this card
      const head = await page.$eval(`${WS} .esStHead`, e => e.innerText.replace(/\s+/g, ' '));
      assert(!/pieces|orders/i.test(head), 'no "pieces" or "orders" in the head: ' + head); assert(/9 matched/.test(head), 'the matched count stands in: ' + head);
      assert.equal(await page.$eval(`${WS} .esCnt`, e => e.hidden), true, 'the pieces / orders block is hidden');
      assert.equal(await page.$eval(`${WS} .esCntW b`, e => e.dataset.v), '9');
      const all = await page.$eval(WS, e => e.innerText.replace(/\s+/g, ' ')); assert(!/\b\d+ pieces\b|\b\d+ orders today\b/i.test(all.replace(/\d+ scans? · \d+ orders?/g, '')), 'no pieces / orders total on the card');
      // tick: the last-input time follows the clock with no request
      const c0 = fx.state.calls.length, t0 = await text(page, `${WS} .esWp[data-name="Max M."][data-task=matching] .esWi`); await sleep(2300);
      const t1 = await text(page, `${WS} .esWp[data-name="Max M."][data-task=matching] .esWi`); assert.notEqual(t0, t1, 'the time since last input ticks'); assert.equal(fx.state.calls.length, c0, '… with no request');
      // the station row keeps its light and name; the summary counts a person once (Max is in two tasks)
      const on = await page.evaluate(() => { const d = __b.data, s = new Set(); for (const x of d.signedIn) s.add(x.name.toLowerCase()); for (const st of d.stations) for (const p of st.people) s.add(p.name.toLowerCase()); return { n: s.size, stations: d.stations.length, welding: d.signedIn.filter(x => x.stationKey === 'welding').length, wNames: new Set(d.signedIn.filter(x => x.stationKey === 'welding').map(x => x.name.toLowerCase())).size }; });
      assert.equal(on.welding, 5, 'five sign-in rows at Welding'); assert.equal(on.wNames, 4, '… of four people: Max is in two tasks');
      assert.equal(await text(page, `${V} .esSum`), `3 of ${on.stations} stations working · ${on.n} people on`, 'a person is counted once, however many tasks or pages');
      console.log('  ✓ two groups: who is in each (a person in both is in both, an old sign-in is Welding and says so), time on task, time since last input ticking; no pieces or orders on the card');
    }

    /* ── 3 · the matched list ── */
    {
      const rows = await page.$$eval(`${WS} .esMr`, rs => rs.map(r => ({ rid: r.querySelector('.esMrId').textContent, x: r.querySelector('.esMrX').hidden ? '' : r.querySelector('.esMrX').textContent, who: r.querySelector('.esMrW').textContent, un: r.dataset.un, time: r.querySelector('.esMrT').textContent, img: !!r.querySelector('.esMrTh img'), ph: !!r.querySelector('.esMrTh .esPh') })));
      assert.deepEqual(rows.map(r => r.rid), ['3521000901', '3521000902', '3521000903', '3521000904', '3521000905', '3521000906'], 'newest first; an order scanned twice is ONE row; six are shown');
      assert.deepEqual(rows.map(r => r.x), ['×2', '', '', '', '', ''], 'the repeated order says ×2');
      assert.deepEqual(rows.map(r => r.who), ['Max M.', 'Max M.', 'Scanned with nobody in Matching', 'Max M.', 'Ray R.', 'Max M.'], 'who matched it (the newest scan), or "Scanned with nobody in Matching"');
      assert.deepEqual(rows.map(r => r.un), ['', '', '1', '', '', ''], 'only the unattributed one is marked');
      assert(rows.every(r => /^\d{1,2}:\d\d [AP]M$/.test(r.time)), 'each has its time');
      await page.waitForFunction(() => document.querySelectorAll('#esHost .esMrTh img').length >= 6, null, { timeout: 8000 });
      assert.equal(await page.$$eval(`${WS} .esMrTh img`, is => is.filter(i => i.naturalWidth > 0).length), 6, 'an order thumbnail on each row');
      assert.equal(await text(page, `${WS} .esMtN`), '9 scans · 8 orders', 'N scans · M orders');
      assert.equal(await text(page, `${WS} .esMtU`), '1 scanned with nobody in Matching', 'the unattributed scans are counted, credited to nobody');
      assert.equal(await text(page, `${WS} .esMtMore`), 'Show 2 more'); await page.click(`${WS} .esMtMore`);
      assert.equal(await page.$$eval(`${WS} .esMr`, r => r.length), 8, 'all of them'); assert.equal(await text(page, `${WS} .esMtMore`), 'Show fewer');
      await page.click(`${WS} .esMtMore`); assert.equal(await page.$$eval(`${WS} .esMr`, r => r.length), 6);
      // a press opens the order; a name opens the person
      await page.click(`${WS} .esMr[data-un="1"] .esMrId`); await page.click(`${WS} .esMr >> nth=1`, { position: { x: 120, y: 6 } });
      await page.click(`${WS} .esMr >> nth=0 >> .esMrW`);
      const opened = await page.evaluate(() => [window.__opened, window.__people]);
      assert.deepEqual(opened[0].slice(0, 2), ['3521000903', '3521000902'], 'a press opens the order (the app\'s own way, once each)'); assert.deepEqual(opened[1], ['Max M.'], 'a person\'s name opens their page, not the order');
      assert.equal(opened[0].length, 2);
      await shot(page, 'board-welding-1440.png', WS);
      console.log('  ✓ matched list: thumbnails, ×2 for a repeated order, "9 scans · 8 orders", "Scanned with nobody in Matching", Show more / fewer, a press opens the order');
    }

    /* ── 4 · hover cards ── */
    {
      const tip = await tipOn(page, `${WS} .esStId`);
      assert(/Matched today\s*9/i.test(tip) && /Welding time today\s*6 h 10 m/i.test(tip) && /Matching time today\s*4 h 5 m/i.test(tip), tip); assert(/Scanned with nobody in Matching\s*1/i.test(tip), tip);
      assert(!/Pieces today|Orders today/i.test(tip), 'no pieces or orders in the station card: ' + tip); assert(/not counted in pieces or orders/.test(tip), tip);
      const pt = await tipOn(page, `${WS} .esWp[data-name="Max M."][data-task=matching] .esPer`);
      assert(/Task\s*Matching/i.test(pt) && /Time on task today\s*4 h/i.test(pt) && /Last input/i.test(pt) && /Welding · Matching/.test(pt), pt);
      assert(!/Pieces today|Orders today|Median per order/i.test(pt), 'a person at Welding has no pieces or orders: ' + pt);
      const gt = await tipOn(page, `${WS} .esWp[data-name="Giovanna C."] .esPer`); assert(/Welding \(task not recorded\)/.test(gt), gt);
      await page.mouse.move(5, 5);
      console.log('  ✓ hover cards: the station (matched, welding and matching time) and a person (task, time on task, last input); never pieces or orders');
    }

    /* ── 5 · live ── */
    {
      const now = Date.now();
      W = welding(now, { people: welding(now).people.filter(p => !(p.name === 'Ray R.')), matched: [{ rid: '3521000950', orderNumber: '3521000950', at: now - 2000, person: 'Max M.', task: 'matching', unattributed: false, note: '', customer: 'Fresh Fixture', thumbUrl: F.pic(950, 'photo'), vectorUrl: '', photoUrl: '', pieceCount: 0, pieces: [] }].concat(welding(now).matched) });
      await refresh(page); await page.waitForFunction(() => document.querySelector('#esHost .esMr .esMrId').textContent === '3521000950', null, { timeout: 5000 });
      assert.equal(await text(page, `${WS} .esMtN`), '10 scans · 9 orders', 'the new scan is at the top and counted');
      await page.waitForFunction(() => ![...document.querySelectorAll('#esHost .esWp')].some(p => p.dataset.name === 'Ray R.'), null, { timeout: 4000 });
      assert.equal(await page.$$eval(`${WS} .esGrp[data-task=matching] .esWp`, r => r.length), 1, 'a person who left Matching is gone from the group');
      assert.equal(await text(page, `${WS} .esCntW b`).then(v => v.split(' ')[0]), '10', 'the head count follows');
      // nobody in a group; no scans yet today
      W = welding(now, { people: [welding(now).people[0]], matched: [], matchedCount: 0 });
      await refresh(page); await page.waitForFunction(() => document.querySelectorAll('#esHost .esMr').length === 0, null, { timeout: 5000 });
      assert.equal(await text(page, `${WS} .esGrp[data-task=matching] .esGrpE`), 'Nobody is signed in to Matching'); assert.equal(await text(page, `${WS} .esMtE`), 'No orders matched yet today'); assert.equal(await page.$eval(`${WS} .esMtN`, e => e.textContent), '0 scans · 0 orders');
      // a capped list (more scans than rows listed)
      W = welding(now, { matchedCount: 41 }); await refresh(page); await page.waitForFunction(() => /41 scans/.test(document.querySelector('#esHost .esMtN').textContent), null, { timeout: 5000 });
      assert.equal(await text(page, `${WS} .esMtN`), '41 scans · the latest 9 are listed', 'when the list is shorter than the count it says so');
      // the other stations are as before: pieces and orders on Assembly, none on Welding
      assert(/pieces/.test(await text(page, `${V} .esSt[data-key=assembly] .esCnt`)), 'Assembly still shows pieces and orders'); assert.equal(await page.$$eval(`${V} .esSt[data-key=assembly] .esGrp`, r => r.length), 0, 'and has no task groups');
      // an old-shape Welding row (no noThroughput) is drawn the old way: nothing is invented
      const keep = W; W = Object.assign({}, welding(now), { noThroughput: false, counts: { partsToday: 64, ordersToday: 21 } }); W.people = W.people.map(p => ({ name: p.name }));
      await refresh(page); await page.waitForFunction(() => document.querySelector('#esHost .esSt[data-key=welding]').dataset.weld === '', null, { timeout: 5000 });
      assert.equal(await page.$eval(`${WS} .esWeld`, e => e.hidden), true); assert.equal(await page.$eval(`${WS} .esCnt`, e => e.hidden), false);
      W = keep;
      console.log('  ✓ live: a new matched scan lands on top, a person leaves a group, empty and capped lists say so, other stations unchanged, an old-shape row is drawn the old way');
    }

    /* ── 6 · widths, reduced motion, errors ── */
    {
      W = welding(Date.now()); await refresh(page); await page.waitForSelector(`${WS} .esMr`);
      await noSideways(page, '1440');
      for (const [w, hh] of [[900, 900], [390, 844]]) {
        await page.setViewportSize({ width: w, height: hh }); await mount(page); await page.waitForSelector(`${WS} .esMr`); await sleep(250);
        await noSideways(page, String(w));
        const cols = await page.$eval(`${WS} .esWeld`, e => [getComputedStyle(e).gridTemplateColumns.split(' ').length, e.closest('.es').getBoundingClientRect().width]);
        assert.equal(cols[0], cols[1] > 720 ? 2 : 1, `the groups stack on a board narrower than 720 px (the board is ${Math.round(cols[1])} px wide at ${w})`);
        const r = await page.$eval(`${WS} .esWeld`, e => { const b = e.getBoundingClientRect(); return [b.left >= 0, b.right <= innerWidth + 1]; }); assert.deepEqual(r, [true, true], 'the card stays inside the screen at ' + w);
        await shot(page, `board-welding-${w}.png`, WS);
      }
      await page.setViewportSize({ width: 1440, height: 900 });
      await page.emulateMedia({ reducedMotion: 'reduce' }); await mount(page); await page.waitForSelector(`${WS} .esMr`);
      assert.equal(await page.$$eval(`${WS} .esMr`, r => r.length), 6, 'reduced motion: the same card'); await page.emulateMedia({ reducedMotion: 'no-preference' });
      assert.deepEqual(errs, [], 'no console errors: ' + errs.join(' | '));
      assert(seen.urls.every(u => !u.includes(F.KEY) && !/[?&]key=/.test(u)), 'the passcode is never in a URL'); assert(seen.aborted >= 0);
      console.log('  ✓ 1440, 900 and 390 px: no sideways scroll, the groups stack when narrow; reduced motion; no console errors');
    }
    await ctx.close();
  } finally { await browser.close(); await srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
