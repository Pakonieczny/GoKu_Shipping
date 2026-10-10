/* The Library follows the cloud in real time (Paul, 3 Oct 04:47: "real-time", and "what is still remaining for a sheet or set of
   sheets"). Offline: no production record, service or Etsy call.
   A. the cloud's side (netlify/functions/charmNestLibrary.js, the real handler over an in-memory Firestore): laserStatus is a
      pure read unless recordSeals is true; ifRevs answers "unchanged" from the documents' update times alone; what that costs.
   B. the page's side (charm-nest-bridge.js, LaserReview; the real code, fake clock and fake backend): a local change shows
      within 400 ms, a change made elsewhere within 3.5 s, back-off after failures, nothing read while the tab is hidden, no
      recordSeals:true on the fast path, one read in flight, an unchanged answer redraws nothing, the open checklist and the
      Moving bar survive, and a card that changes place glides. */
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const root = path.join(__dirname, '../..');
const S = 'Charm_Nest_Sheets', SET = 'Charm_Nest_Sets', RUN = 'Charm_Nest_Runs';

/* ─────────────────────────── A. the cloud's side ─────────────────────────── */
async function cloud() {
  const { start } = require('./bridge-server.cjs'), { seed } = require('./laser-workflow.cjs');
  const srv = await start({ receipts: [] }); seed(srv); const { st } = srv;
  const call = async b => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(b) }); return { status: r.status, ...await r.json() }; };
  const frozen = () => new Map(st.docs);                                   // (every document object as it stands: a write replaces its object)
  const untouched = before => { for (const [k, v] of st.docs) if (before.get(k) !== v) return false; return st.docs.size === before.size; };
  try {
    const ids = ['cut-sheet', 'ready-sheet', 'pending-sheet'], ask = (extra = {}) => call({ op: 'laserStatus', sheetIds: ids, setIds: ['set-fixture'], ...extra });
    // 1. no recordSeals, recordSeals:false and a probe never write; the answer is what it always was (no revs, no unchanged)
    let before = frozen();
    let plain = await ask(); assert.equal(plain.status, 200); assert(!('revs' in plain) && !('unchanged' in plain), 'the default answer keeps its shape');
    const off = await ask({ recordSeals: false, wantRevs: true }); assert(untouched(before), 'recordSeals:false writes nothing');
    assert.deepEqual(Object.keys(off.revs).sort(), ['r:run-fixture','s:cut-sheet', 's:pending-sheet', 's:ready-sheet', 't:set-fixture'], 'the answer names the documents it was made from');
    const same = await ask({ recordSeals: false, wantRevs: true, ifRevs: off.revs });
    assert.equal(same.unchanged, true); assert.equal(same.probed, 5); assert(!('sheets' in same), 'nothing is worked out again'); assert(untouched(before), 'a probe writes nothing');
    // 2. what a probe reads: the five documents and nothing else; a full answer reads more (and whole records)
    st.reads = 0; await ask({ recordSeals: false, wantRevs: true, ifRevs: off.revs }); const probeReads = st.reads;
    st.reads = 0; await ask({ recordSeals: false, wantRevs: true }); const fullReads = st.reads;
    assert.equal(probeReads, 5); assert(fullReads >= probeReads);
    // 3. a change in any watched document, by anyone, makes the next probe answer in full, with new revisions
    for (const [what, edit] of [['a sheet', () => st.put(S, 'ready-sheet', { verification: { ok: false } })], ['its set', () => st.put(SET, 'set-fixture', { status: 'again' })], ['its run', () => st.put(RUN, 'run-fixture', { touched: 1 })]]) {
      const was = await ask({ recordSeals: false, wantRevs: true }); edit();
      const now = await ask({ recordSeals: false, wantRevs: true, ifRevs: was.revs });
      assert(!now.unchanged && now.sheets.length === 3 && now.revs, `${what} changed: answered in full`); assert.notDeepEqual(now.revs, was.revs);
    }
    st.put(S, 'ready-sheet', { verification: { ok: true } });
    // 4. a card not read before, a bad key list, or recordSeals:true never take the cheap answer
    const base = await ask({ wantRevs: true });
    assert(!(await call({ op: 'laserStatus', sheetIds: [...ids, 'other-sheet'], setIds: ['set-fixture'], wantRevs: true, ifRevs: base.revs })).unchanged, 'an unread card is read in full');
    assert(!(await ask({ wantRevs: true, ifRevs: { 'x:bad': '1' } })).unchanged);
    const sealing = await ask({ recordSeals: true, by: 'Test', wantRevs: true, ifRevs: (await ask({ wantRevs: true })).revs });
    assert(!sealing.unchanged && Array.isArray(sealing.added) && !('revs' in sealing), 'the slow, seal-recording check is the full answer it was, with no revs');
    // 5. the price of one visible Library: a probe reads one document per sheet, set and run on screen
    const many = (sets, per, runs) => {
      const at = Date.now(), ts = { toMillis: () => at }, rows = [];
      for (let k = 0; k < sets; k++) {
        const setId = 'scale-set-' + k, runId = 'scale-run-' + (k % runs), sheetIds = [];
        for (let i = 0; i < per; i++) {
          const id = `scale-${k}-${i}`, orders = Array.from({ length: 25 }, (_, n) => String(4100000000 + k * 1000 + i * 30 + n)), pool = orders.map(o => o + '_1_1');
          const run = st.doc(RUN, runId) || { runId, lines: {} };
          orders.forEach((o, n) => { run.lines[o + '_1'] = { orderId: o, state: 'written', quantity: 1, poolIds: [pool[n]], engraveCandidate: false }; });
          st.put(RUN, runId, run, false);
          st.put(S, id, { id, setId, setSeq: k + 1, sheetIndex: i + 1, runId, metal: 'gold', day: '2026-10-03', status: 'complete', placedCount: 25, charmCount: 25, density: .7, stock: { wIn: 6, hIn: 4.5 }, poolIds: pool, orders, verification: { ok: true }, outputs: { ai: { path: id + '.ai', url: 'u' }, preview: { path: id + '.png', url: 'u' } }, label: { files: [{ path: id + '-qr.png', url: 'u', payload: 'p', orders }] }, updatedAt: ts, createdAt: ts });
          sheetIds.push(id); rows.push(id);
        }
        st.put(SET, setId, { setId, seq: k + 1, day: '2026-10-03', runId, sheetIds, materials: ['gold'], orders: {}, status: 'labelled', updatedAt: ts, createdAt: ts });
      }
      return rows;
    };
    const cost = [];
    for (const [sets, per, runs] of [[2, 3, 1], [6, 3, 2], [12, 3, 3]]) {
      const rows = many(sets, per, runs), setIds = [...new Set(rows.map(r => r.replace(/-\d+$/, '').replace('scale-', 'scale-set-')))];
      const full = await call({ op: 'laserStatus', sheetIds: rows, setIds, recordSeals: false, wantRevs: true });
      st.reads = 0; await call({ op: 'laserStatus', sheetIds: rows, setIds, recordSeals: false, wantRevs: true });
      const fullDocs = st.reads;
      const idle = await call({ op: 'laserStatus', sheetIds: rows, setIds, recordSeals: false, wantRevs: true, ifRevs: full.revs });
      assert.equal(idle.unchanged, true);
      st.reads = 0; await call({ op: 'laserStatus', sheetIds: rows, setIds, recordSeals: false, wantRevs: true, ifRevs: full.revs });
      cost.push({ sets, sheets: rows.length, runs, fullDocs, probeDocs: st.reads, perHour: st.reads * 1200 });
    }
    for (const c of cost) assert(c.probeDocs <= c.sheets + c.sets + c.runs, 'a probe reads no more than one document per sheet, set and run');
    return cost;
  } finally { srv.close(); }
}

/* ─────────────────────────── B. the page's side ─────────────────────────── */
async function page() {
  const { JSDOM } = require('jsdom');
  const src = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  const dom = new JSDOM('<body><main id="libBody"></main></body>', { runScripts: 'outside-only', pretendToBeVisual: true }), win = dom.window, document = win.document;
  win.matchMedia = () => ({ matches: true });
  win.eval(fs.readFileSync(path.join(root, 'charm-nest-motion.js'), 'utf8'));
  win.eval(fs.readFileSync(path.join(root, 'charm-nest-readiness.js'), 'utf8'));
  // the fake clock: time moves only when the test says; timers, frames and the backend's answers all run on it
  const clock = { now: 1790000000000 }, timers = [], frames = []; let tid = 0;
  const setT = (fn, ms) => { const id = ++tid; timers.push({ id, at: clock.now + Math.max(0, +ms || 0), fn }); return id; };
  const clearT = id => { const i = timers.findIndex(t => t.id === id); if (i >= 0) timers.splice(i, 1); };
  class FakeDate extends Date { constructor(...a) { if (a.length) super(...a); else super(clock.now); } static now() { return clock.now; } }
  const micro = async () => { for (let i = 0; i < 30; i++) await Promise.resolve(); };
  const flush = () => { for (let i = 0; i < 8 && frames.length; i++) frames.splice(0).forEach(f => f()); };
  async function advance(ms) {
    const end = clock.now + ms;
    for (;;) {
      await micro(); flush();
      timers.sort((a, b) => a.at - b.at || a.id - b.id);
      const t = timers[0]; if (!t || t.at > end) break;
      timers.shift(); clock.now = Math.max(clock.now, t.at); t.fn();
    }
    clock.now = end; await micro(); flush();
  }
  // the fake backend: records with a revision each, answering as the real one does (ifRevs, wantRevs, recordSeals)
  const world = { v: {}, sheets: {}, sets: {}, latency: 120, fail: false, legacy: false, down: 0 }, requests = [];
  let inFast = 0, maxFast = 0;
  const watch = payload => { const k = {}; for (const id of payload.sheetIds) { k['s:' + id] = world.v['s:' + id] || 1; const set = world.sheets[id] && world.sheets[id].setId; if (set) k['t:' + set] = world.v['t:' + set] || 1; } for (const id of payload.setIds || []) k['t:' + id] = world.v['t:' + id] || 1; return k; };
  const bump = key => { world.v[key] = (world.v[key] || 1) + 1; };
  const put = s => { world.sheets[s.id] = s; bump('s:' + s.id); };
  const answer = payload => {
    const now = watch(payload);
    if (!world.legacy && payload.ifRevs && payload.recordSeals !== true && Object.keys(now).every(k => payload.ifRevs[k] === now[k]) && Object.keys(payload.ifRevs).length === Object.keys(now).length) return { unchanged: true, probed: Object.keys(now).length, checkedAt: clock.now };
    const sheets = payload.sheetIds.map(id => world.sheets[id]).filter(Boolean), setIds = [...new Set(sheets.map(s => s.setId).filter(Boolean).concat(payload.setIds || []))];
    return { sheets: JSON.parse(JSON.stringify(sheets)), sets: setIds.map(id => JSON.parse(JSON.stringify(world.sets[id] || { setId: id, sheetIds: [] }))), added: [], checkedAt: clock.now, ...(!world.legacy && payload.wantRevs && payload.recordSeals !== true ? { revs: now } : {}) };
  };
  const api = async (name, payload) => {
    requests.push({ at: clock.now, name, payload: JSON.parse(JSON.stringify(payload)) });
    const quickRead = payload.op === 'laserStatus' && payload.recordSeals !== true;
    if (quickRead) { inFast++; maxFast = Math.max(maxFast, inFast); }
    await new Promise(r => setT(r, world.latency)); if (quickRead) inFast--;
    if (world.fail) throw Object.assign(new Error('network down'), { transient: true });
    return answer(payload);
  };
  const glides = [], pulses = [], reloads = []; let hidden = false;
  Object.defineProperty(document, 'hidden', { get: () => hidden, configurable: true });
  const esc = x => String(x == null ? '' : x).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;');
  const ctx = vm.createContext({ window: win, document, console: { ...console, warn() {} }, esc, cors: x => x, S: { mode: 'library', cloud: { ok: true }, library: { loadedAt: 0 } }, allSheets: () => [], Orders: { rows: () => [] }, Engrave: { items: () => new Map() }, O: { setLabel: n => 'Set ' + n }, CNListActivity: { compare: () => 0, compareBlocks: () => 0, state: () => ({ direction: 1 }) }, innerHeight: 800, requestAnimationFrame: fn => (frames.push(fn), frames.length), setTimeout: setT, clearTimeout: clearT, setInterval() {}, api, Date: FakeDate });
  vm.runInContext(src.slice(src.indexOf('const LaserReview ='), src.indexOf('const Sets =')), ctx);
  const L = win.LaserReview, body = document.querySelector('#libBody');
  // (the Library's own glide helpers, as charm-nest-library.js exports them; refresh() calls its bare name, so it is in both places)
  ctx.LibraryDone = win.LibraryDone = { isDone: r => !!(r && r.laserDoneAt), addedSeals() {}, refreshCards() {}, snapshot: () => new Map([['x', 1]]), glideFrom: (b, before) => glides.push(before) };
  win.LibraryFx = { pulse: el => pulses.push(el), active: () => 0 };
  win.CN = { loadLibrary: () => { reloads.push(clock.now); return Promise.resolve(); } };

  const sheet = (id, extra = {}) => ({ id, sheetIndex: 1, poolIds: [id + '1'], placedCount: 1, verification: { ok: true }, preview: 'p', outputs: { ai: 'f' }, label: { files: [] }, backPool: [], engraving: { [id + '1']: { needed: false, state: 'none', approved: true } }, updatedAt: 1, ...extra });
  const labelled = s => ({ ...s, label: { files: [{ path: 'qr', url: 'u', payload: 'o' }] }, processReady: true, updatedAt: (s.updatedAt || 0) + 1 });
  const rect = (card, top = 80) => { card.getBoundingClientRect = () => ({ left: 10, top, right: 310, bottom: top + 350, width: 300, height: 350 }); return card; };
  const flat = (id, top) => { const a = document.createElement('article'); a.className = 'librarySheet'; a.dataset.laserCard = 'sheet'; a._laserSheets = [id]; a.innerHTML = `<div class="libCard" data-id="${id}"><span data-sheet-status="${id}"></span></div>`; return rect(a, top); };
  const area = card => card.closest('[data-laser-area]').dataset.laserArea;
  const fast = () => requests.filter(r => r.payload.op === 'laserStatus' && r.payload.recordSeals !== true);
  const gaps = list => list.slice(1).map((r, i) => r.at - list[i].at);

  // three cards on screen: A waits only for its QR label (an automatic step), B is ready, C is far below the screen
  const A = sheet('shA'), B = labelled(sheet('shB')), C = sheet('shC');
  [A, B, C].forEach(s => { put(s); L.record(s); });
  L.sections(body);
  const cards = { A: flat('shA', 80), B: flat('shB', 440), C: flat('shC', 5000) };
  for (const [k, id] of [['A', 'shA'], ['B', 'shB'], ['C', 'shC']]) L.place(cards[k], L.canCut(world.sheets[id]), body);
  assert.equal(area(cards.A), 'pending'); assert.equal(area(cards.B), 'ready');
  const t0 = clock.now;

  // 1. opening: one read at once, for the cards on screen only, never recordSeals:true
  L.changed(); await advance(0);
  assert.equal(fast().length, 1, 'the Library opens with one read'); const first = fast()[0].payload;
  assert.deepEqual(first.sheetIds.sort(), ['shA', 'shB'], 'only the sheets of the cards on screen'); assert.equal(first.recordSeals, false); assert.equal(first.wantRevs, true); assert(!('ifRevs' in first), 'the first read is in full');
  await advance(200);
  assert.equal(L.liveState().revs, 2, 'the answer\'s revisions are held for the next read');

  // 2. start to start about 3 s; an unchanged answer redraws nothing (no frame, no change to the page)
  const seen = []; const mo = new win.MutationObserver(m => seen.push(...m)); mo.observe(body, { subtree: true, childList: true, attributes: true, characterData: true });
  await advance(3000 * 4); seen.push(...mo.takeRecords());
  assert.equal(fast().length, 5, 'one read every 3 s'); assert.deepEqual(gaps(fast()).slice(0, 4), [3000, 3000, 3000, 3000]);
  assert(fast().slice(1).every(r => r.payload.ifRevs && Object.keys(r.payload.ifRevs).length === 2), 'each later read sends back what the last answer was made from');
  assert.equal(seen.length, 0, 'an unchanged answer redraws nothing'); assert.equal(frames.length, 0);
  assert.equal(glides.length, 0);

  // 3. a change made on another computer: on the card within 3.5 s
  await advance(500);
  const elsewhere = clock.now; world.sets.none = null; put(labelled(sheet('shA')));
  let shown = null; for (let t = 0; t < 4000 && shown === null; t += 50) { await advance(50); if (area(cards.A) === 'ready') shown = clock.now - elsewhere; }
  assert(shown !== null && shown <= 3500, `a change made elsewhere shows within 3.5 s (took ${shown} ms)`); const slow = shown;
  assert.equal(glides.length, 1, 'the card glided to Laser cutting (the Library\'s own glide)'); assert.deepEqual([...glides[0].keys()], ['x']);
  await advance(900); assert.equal(pulses.length, 1, 'and left a soft ring when it had settled'); assert.equal(pulses[0], cards.A);

  // 4. what a person does here: the read comes at once (within 400 ms), and again 1.5 s after the last write; a burst of writes
  //    makes one of each. (`put` is the cloud changing; the nudge is what api() does after the person's own write)
  await advance(1200);
  const n0 = fast().length, local = clock.now; world.latency = 100; put(sheet('shA', { updatedAt: 9, label: { files: [] }, processReady: false, laserHold: { at: 1, by: 'P', note: 'x' } }));
  L.nudge(); L.nudge(); L.nudge();   // (three writes in a row)
  let quick = null; for (let t = 0; t < 1000 && quick === null; t += 20) { await advance(20); if (area(cards.A) === 'pending') quick = clock.now - local; }
  assert(quick !== null && quick <= 400, `a local change shows within 400 ms (took ${quick} ms)`);
  await advance(2000);
  const mine = fast().slice(n0).filter(r => r.at - local < 2200); assert.equal(mine.length, 2, 'one read at once and one 1.5 s after the last write'); assert.equal(mine[0].at - local, 0); assert.equal(mine[1].at - local, 1500);
  // the same a moment after the loop's own read began (the 600 ms gap does not hold a person's change back)
  { const before = fast().length; while (fast().length === before) await advance(10); await advance(150);
    const at = clock.now; put(labelled(sheet('shA'))); L.nudge(); let t2 = null; for (let t = 0; t < 1000 && t2 === null; t += 10) { await advance(10); if (area(cards.A) === 'ready') t2 = clock.now - at; }
    assert(t2 !== null && t2 <= 400, `also when the loop's read had just begun (took ${t2} ms)`); await advance(2000); }
  // and by the Approve button's own call, LaserReview.poll(true, true), which used to be a seal-recording read
  const n1 = fast().length, press = clock.now; put(sheet('shA', { updatedAt: 30, label: { files: [] }, processReady: false })); win.LaserReview.poll(true, true);
  await advance(300); assert.equal(area(cards.A), 'pending', 'poll(true, true) is the live read'); assert.equal(fast()[n1].at, press); assert.equal(requests.filter(r => r.payload.recordSeals === true).length, 0, 'no seal-recording read for it');
  put(labelled(sheet('shA'))); await advance(3500); assert.equal(area(cards.A), 'ready');

  // 5. the slow check is what it was: poll(true) still records seals (recordSeals:true) and applies its answer
  L.poll(true); await advance(500);
  const sealing = requests.filter(r => r.payload.recordSeals === true); assert.equal(sealing.length, 1); assert.equal(sealing[0].payload.op, 'laserStatus'); assert(!('ifRevs' in sealing[0].payload) && !('wantRevs' in sealing[0].payload), 'its request is unchanged');
  assert(fast().every(r => r.payload.recordSeals === false), 'no fast read ever asks to record seals'); assert(requests.every(r => r.payload.recordSeals === false || r === sealing[0]), 'and nothing else asks for it either');

  // 6. the loop never reads over its own read, even when the answer is slower than the loop; a person's write while one is out starts ONE read beside it at once
  //    (the one out may have been asked before the write: waiting for it would show the write seconds late), and the writes that follow within 600 ms ask for one more after both are back
  await advance(4000); world.latency = 5000; const n2 = fast().length;
  while (fast().length === n2) await advance(10); await advance(1000);
  const during = fast().length; assert.equal(maxFast, 1, 'until then, one fast read in flight at a time'); L.nudge(); await advance(100); assert.equal(fast().length, during + 1, 'a write while a read is out starts one beside it, at once'); assert.equal(maxFast, 2);
  L.nudge(); L.nudge(); await advance(300); assert.equal(fast().length, during + 1, 'the writes right after it start no more beside them');
  await advance(30000); const phase = fast().slice(n2);
  assert(phase.length >= 4 && gaps(phase.slice(2)).every(g => g >= 5000), 'the loop starts each read after the last one is back'); assert.equal(maxFast, 2, 'never more than the loop\'s read and the write\'s in flight'); world.latency = 120;
  assert.equal(phase[1].at - phase[0].at, 1000, 'the write\'s read left the moment it was announced'); assert.equal(phase[2].at - phase[0].at, 6000, 'and the one for the writes after it follows when both are back');

  // 7. failures: 6, 12, 24, 30, 30 s apart, then 3 s again at the first answer; online reads at once and starts over
  await advance(12000); world.fail = true;
  await advance(3100); const n3 = fast().length; await advance(130000);
  const sequence = gaps(fast().slice(n3 - 1)); assert.deepEqual(sequence.slice(0, 5), [6000, 12000, 24000, 30000, 30000], 'back-off doubles to 30 s and stays there');
  world.fail = false; win.dispatchEvent(new win.Event('online')); const back = clock.now; await advance(1000);
  assert(fast().slice(-1)[0].at - back <= 600, 'the network back: read at once (600 ms after the last read started at the latest)'); assert.equal(L.liveState().fails, 0);
  await advance(10000); assert(gaps(fast().slice(-4)).every(g => g === 3000), 'and every 3 s again');

  // 8. hidden tab: nothing is read, nothing is scheduled; back in view: one read at once
  const n4 = requests.length; hidden = true; document.dispatchEvent(new win.Event('visibilitychange'));
  await advance(120000); assert.equal(requests.length, n4, 'nothing is read while the tab is hidden'); assert.equal(L.liveState().armed, false);
  L.nudge(); await advance(5000); assert.equal(requests.length, n4, 'not even for a change');
  hidden = false; const shownAt = clock.now; document.dispatchEvent(new win.Event('visibilitychange')); await advance(1000);
  const returned = fast().filter(r => r.at >= shownAt); assert(returned.length >= 1 && returned[0].at - shownAt <= 600, 'one read at once on return');
  await advance(6000); assert(gaps(fast().slice(-3)).every(g => g >= 3000), 'and the loop goes on');

  // 9. only while the Library is the page in view
  const n5 = requests.length; ctx.S.mode = 'orders'; await advance(20000); assert.equal(requests.length, n5, 'not in another view'); ctx.S.mode = 'library'; L.changed(); await advance(700); assert(requests.length > n5, 'back in the Library it reads again');
  assert.equal(reloads.length, 0, 'nothing here has moved a sheet to another set: the list was never read again');

  // 10. the sheet's rail (with its '!') and the Moving bar survive a redraw a change causes (A, waiting on one back engraving, then on two)
  world.latency = 120; put(sheet('shA', { updatedAt: 40, poolIds: ['shA1', 'shA2'], placedCount: 2, engraving: { shA1: { needed: false, state: 'none', approved: true }, shA2: { needed: true, state: 'words', approved: false } } }));
  await advance(3500); assert.equal(area(cards.A), 'pending');
  let bangs = 0; document.addEventListener('click', e => { if (e.target.closest && e.target.closest('[data-issues-open]')) bangs++; });
  assert.equal(L.openChecklist(cards.A, { kind: 'sheet', id: 'shA' }), true, "the sheet's '!' is pressed (the issues panel opens from it)"); assert.equal(bangs, 1, 'once');
  const boxBefore = cards.A.querySelector('.flowBox'), textBefore = boxBefore.querySelector('.flowStep.current .flowDot').getAttribute('aria-label'), glidesBefore = glides.length;
  const bar = document.createElement('div'); bar.dataset.libraryApproval = ''; bar.textContent = 'Moving'; cards.A.appendChild(bar);
  put(sheet('shA', { updatedAt: 41, poolIds: ['shA1', 'shA2', 'shA3'], placedCount: 3, engraving: { shA1: { needed: false, state: 'none', approved: true }, shA2: { needed: true, state: 'words', approved: false }, shA3: { needed: true, state: 'words', approved: false } } }));
  await advance(3500);
  assert.notEqual(boxBefore.querySelector('.flowStep.current .flowDot').getAttribute('aria-label'), textBefore, 'the rail says what is left now'); assert(boxBefore.isConnected && cards.A.querySelector('.flowBox') === boxBefore, 'in the same box');
  assert.equal(boxBefore.querySelectorAll('[data-issues-open]').length, 1, "and still carries its '!'"); assert(bar.isConnected && bar.parentElement === cards.A, 'the Moving bar is still on its card');
  assert.equal(glides.length, glidesBefore, 'a card that did not change place does not glide');
  bar.remove();

  // 11. a sheet that moved to another set on another computer: the Library's list is read again once, a moment after, and not
  //     while a Moving bar is up
  world.sets.setX = { setId: 'setX', sheetIds: ['shA'] }; world.sheets.shA = { ...world.sheets.shA, setId: 'setX' }; bump('s:shA'); bump('t:setX');
  const bar2 = document.createElement('div'); bar2.dataset.libraryApproval = ''; cards.A.appendChild(bar2);
  await advance(15000); assert.equal(reloads.length, 0, 'not over a Moving bar');
  bar2.remove(); await advance(3000); assert.equal(reloads.length, 1, 'then read once'); await advance(30000); assert.equal(reloads.length, 1, 'and not again for the same change');
  // a list read since the change says it already
  reloads.length = 0; world.sheets.shA = { ...world.sheets.shA, setId: 'setY' }; bump('s:shA'); ctx.S.library.loadedAt = clock.now + 1500; await advance(12000); assert.equal(reloads.length, 0, 'a list read since says it already');

  // 12. a cloud that does not know ifRevs (the page is newer than the function): answered in full each time, so asked 20 s apart
  world.legacy = true; await advance(30000); const nl = fast().length; await advance(100000);
  const legacyGaps = gaps(fast().slice(nl - 1)); assert(legacyGaps.length >= 4 && legacyGaps.every(g => g >= 20000), 'a cloud that cannot say "unchanged" is asked every 20 s, not every 3 s');
  mo.disconnect(); win.close();
  return { quick, slow };
}

(async () => {
  const cost = await cloud();
  const live = await page();
  console.log('Library live OK: laserStatus is a pure read unless recordSeals is true, ifRevs answers "unchanged" from one read per watched document, a local change shows in ' + live.quick + ' ms and one made elsewhere in ' + live.slow + ' ms, back-off to 30 s, silent while hidden, one loop read in flight (a person write starts one beside it), no recordSeals:true on the fast path, unchanged answers redraw nothing, rail and Moving bar survive, cards glide between In progress and Laser cutting');
  console.log('Cost of one probe (documents read), against a full read:'); console.table(cost);
})().catch(e => { console.error(e); process.exitCode = 1; });
