// A sheet released at the end of the sandbox stream (settleTopups) must still get its QR label when a step fails on the
// way (sandbox check, 28 Sep). settleTopups marked the sheet's gap fill closed, then saved the release (putSheet) and
// assembled the run's set (Gate.assemble, which makes the label). When either failed, the sheet stayed marked closed but
// never joined its set: no QR label, and nothing tried again (only "Make QR label" in the sheet window fixed it).
// Now a sheet the stream released stays owed its label until it has one: a failed step is tried again a few times with
// a growing wait, then the card says "No QR yet: retrying" (never a pop-up), and a reload tries again. Orders stay on it.
// The real settleTopups runs in node's vm; its cloud calls go over loopback to the fake server (bridge-server.cjs), whose
// failure injection (st.fail) turns charmNestLibrary down. No network beyond 127.0.0.1, no Etsy, no paid AI.
//   node tests/charm-nest/adv-topup-retry.cjs
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const root = path.join(__dirname, '../..');
const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
const part = (src, a, b) => { const i = src.indexOf(a); assert(i >= 0, 'not found: ' + a); const j = src.indexOf(b, i); assert(j > i, 'not found: ' + b); return src.slice(i, j); };
const { start } = require('./bridge-server.cjs');

(async () => {
  const srv = await start();
  try {
    const st = srv.st;
    // the sorter's api() over loopback, sandbox records only, as the page sends them
    const api = async (fn, body) => {
      const r = await fetch(`${srv.sorterOrigin}/.netlify/functions/${fn}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(Object.assign({ sandbox: true }, body)) });
      const out = await r.json().catch(() => ({})); if (!r.ok || out.error) throw new Error(out.error || 'HTTP ' + r.status); return out;
    };
    const logs = [], notes = [], toasts = [], cards = [];
    let sheets = [], labelFail = null, assembleCalls = 0;
    // Gate.assemble as the bridge does it for a released sheet: the set membership saved on the sheet record, then the
    // QR label made and saved; either can fail (the cloud down, a label upload refused)
    const Gate = {
      modern: id => id === 'run-1',
      async assemble(run) {
        assembleCalls++;
        for (const sh of sheets.filter(p => p.runId === run.runId && p.releaseFull && p.topup?.closedAt && p.draft)) {
          await api('charmNestLibrary', { op: 'putSheet', sheet: { id: sh.sheetId, draft: false, setId: 'set-1', setSeq: 1 } });
          if (labelFail) { sh.problem = 'Set membership was not saved: ' + labelFail; throw new Error(labelFail); }
          Object.assign(sh, { draft: false, setId: 'set-1', seq: 1, sheetIndex: 1, label: { files: [{ path: 'labels/' + sh.sheetId + '.png', url: 'u', part: 1 }] }, problem: null });
          await api('charmNestLibrary', { op: 'putSheet', sheet: { id: sh.sheetId, label: sh.label } });
        }
      }
    };
    const c = { console, Set, Map, Math, JSON, Object, Array, String, Number, Promise, Error, api, logs, notes, toasts, cards, Gate }; vm.createContext(c);
    vm.runInContext(`
      let __clock = 1790550000000; Date.now = () => __clock;
      const TOPUP = { orders: 35, target: .75 };
      const fmt = { pct: v => Math.round(v * 100) + '%' }, topupNow = () => 1790550000000;
      const esc = s => String(s).replace(/[&<>"]/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[ch]);
      const sheetName = sh => sh.metal + ' · sheet ' + sh.page, activeCharms = sh => sh.charms.filter(c => !c.excluded);
      const log = (sh, m) => logs.push(sh.metal + sh.page + ': ' + m), agent = (_, kind, text) => notes.push(kind + ': ' + text), renderCard = sh => cards.push(sh.sheetId), toast = (m, k) => toasts.push(m);
      let sheets = [];
      const allSheets = () => sheets;
      const window = { B: { run: { runId: 'run-1', status: 'processed', step: 'complete' } }, Sandbox: { done: () => true }, Arrivals: { held: () => '', state: () => ({ pending: false }) }, Gate, Session: { schedule() {} } };
    `, c);
    vm.runInContext(part(html, '/* Gap fill also ends when no more orders will come', 'function usableArea('), c);
    const run = code => vm.runInContext(code, c), settle = () => run('settleTopups()'), later = ms => run(`__clock += ${ms}`);
    const placedOn = ids => ids.map((id, i) => ({ id, cxPt: 10 + 10 * i, cyPt: 10, angle: 0 }));
    const sheet = over => Object.assign({ metal: 'gold', page: 1, sheetId: 'gold-1', runId: 'run-1', status: 'complete', dirty: false, draft: true, persistedDone: true, feedWait: null, verification: { ok: true }, density: .7306, log: [],
      charms: [{ id: 'a', order: '4001' }, { id: 'b', order: '4002' }], placements: placedOn(['a', 'b']), rejects: [], topup: { at: 1, base: .7306, placed: 2, tried: Array.from({ length: 30 }, (_, i) => 'o' + i) } }, over);
    const setSheets = list => { sheets = list; c.__list = list; run('sheets = __list'); };
    const saved = id => st.doc('Sandbox_Charm_Nest_Sheets', id) || st.list('Sandbox_Charm_Nest_Sheets').find(d => d._id === id) || null;
    const labelled = sh => !!sh.setId && !sh.draft && !!(sh.label?.files || []).length;
    const libraryCalls = () => st.calls.filter(x => x.name === 'charmNestLibrary').length;
    const note = sh => run('qrRetryNote')(sh);

    /* ── 1 · the release save fails (the cloud down): the sheet is not dropped; it is tried again and gets its label ── */
    {
      const gold = sheet(); setSheets([gold]);
      st.fail.charmNestLibrary = true;
      await settle();
      assert.equal(gold.charms.length, 2, 'no order is lost'); assert(!labelled(gold), 'no label while the cloud is down');
      assert.equal(toasts.length, 0, 'never a pop-up');
      // tried again on its own, a little later (not at every tick: the tick comes every 250 ms)
      const callsAfterFirst = libraryCalls();
      for (let i = 0; i < 8; i++) { later(250); await settle(); }
      assert.equal(libraryCalls(), callsAfterFirst, 'no retry storm: the next try waits');
      st.fail.charmNestLibrary = false;
      later(60000); await settle();
      assert(labelled(gold), 'the save failed once, and the sheet still joins its set and gets its QR label once the cloud answers');
      const rec = saved('gold-1'); assert(rec, 'the release is saved on the sheet record');
      assert.equal(rec.releaseFull, true); assert(rec.topup && rec.topup.closedAt, 'its gap fill saved as closed'); assert.equal(rec.setId, 'set-1');
      assert.equal(note(gold), '', 'labelled: no note on its card');
      const n = libraryCalls(); later(600000); await settle(); assert.equal(libraryCalls(), n, 'once labelled it is left alone');
    }

    /* ── 2 · the label step fails (Gate.assemble): a few tries with a growing wait, then a calm note; a reload tries again ── */
    {
      st.docs.clear(); st.calls.length = 0; assembleCalls = 0;
      const gold = sheet({ sheetId: 'gold-2', page: 2 }); setSheets([gold]);
      labelFail = 'QR label upload refused';
      await settle();
      assert.equal(assembleCalls, 1); assert(!labelled(gold));
      assert.equal(note(gold), '', 'one failed try: no note yet');
      for (let i = 0; i < 40; i++) { later(5000); await settle(); }   // over three minutes of ticks
      assert(assembleCalls >= 3, `tried again a few times (${assembleCalls})`);
      assert(assembleCalls <= 4, `bounded: ${assembleCalls} tries, not one per tick`);
      assert.match(note(gold), /No QR yet: retrying/, 'still failing: the card says so, calmly');
      assert.doesNotMatch(note(gold), /class="bad"/, 'a calm note, not an error line');
      assert.equal(gold.problem || null, null, 'the red set-membership error is not left on the card: the note says it');
      assert.equal(toasts.length, 0, 'never a pop-up');
      assert.equal(gold.charms.length, 2, 'no order is lost');
      const spent = assembleCalls; for (let i = 0; i < 20; i++) { later(60000); await settle(); }
      assert.equal(assembleCalls, spent, 'the tries are bounded in this session');
      // a reload: the workspace checkpoint keeps every key but those starting with "_"
      const reloaded = JSON.parse(JSON.stringify(gold, (k, v) => (k.startsWith('_') ? undefined : v)));
      assert(reloaded.topup.closedAt && reloaded.releaseFull, 'the release itself survives the reload');
      setSheets([reloaded]); labelFail = null;
      await settle();
      assert(labelled(reloaded), 'after a reload the sheet is tried again and gets its QR label');
      assert.equal(note(reloaded), '');
    }

    /* ── 3 · released and saved, the label failed, and the page closed: restored from the cloud record, it is finished ── */
    {
      st.docs.clear(); st.calls.length = 0;
      const first = sheet({ sheetId: 'gold-3', page: 3 }); setSheets([first]);
      labelFail = 'QR label upload refused'; await settle(); labelFail = null;
      const rec = saved('gold-3'); assert(rec && rec.releaseFull && rec.topup.closedAt, 'the release was saved before the label failed');
      // the page opened again: the sheet as restoreRunSheets builds it from its record (releaseFull, topup, draft)
      const gold = sheet({ sheetId: 'gold-3', page: 3, releaseFull: !!rec.releaseFull, topup: rec.topup, draft: true }); setSheets([gold]);
      await settle();
      assert(labelled(gold), 'a stream release left without its label is finished when the page runs again');
    }

    /* ── 4 · zero Etsy calls, sandbox records only ── */
    const names = new Set(st.calls.map(x => x.name));
    assert.deepEqual([...names].filter(n => /etsy|listOpenOrders|imageProxy/i.test(n)), [], 'no Etsy call');
    assert(st.list('Charm_Nest_Sheets').length === 0, 'nothing written to the production sheet records');
    console.log('adv-topup-retry OK: a stream release whose save or label step fails is tried again with a growing wait (bounded), then shows "No QR yet: retrying" on its card (no pop-up), is tried again after a reload, and gets its QR label once the cloud answers; no order lost, no Etsy call');
  } finally { srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
