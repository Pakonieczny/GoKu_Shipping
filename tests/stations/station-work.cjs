// station-work.js + the rollup's `work` map (Paul, 9 Nov 2026: "If a user scans a QR code, it should just open that order and as long as the user
// doesn't press complete that order never gets completed"):
//   - a scan only opens the order: no seal, no `complete`, no `matched`, nothing on screen added
//   - Complete Order runs the station's completion once, credits the person and logs `complete` with the time since the order was opened here
//   - pressed again: a note, never counted twice; a cancelled order the person backs out of: nothing; a failed completion: nothing
//   - Welding / Assembly never call the Etsy tracking endpoint, and show the page's own Complete Order button
//   - the efficiency aggregation (_stationWork.js through _stationActivity.js's rollup) counts completions, pieces, times and orders opened, never completed
// Fakes only: jsdom, a fake StationSession / StationTimeline; nothing touches the network.
//   JSDOM_DIR=<…/node_modules> node tests/stations/station-work.cjs
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
let JSDOM;
for (const d of [process.env.JSDOM_DIR, process.argv[2], path.join(root, 'node_modules')].filter(Boolean)) { try { ({ JSDOM } = require(path.join(d, 'jsdom'))); break; } catch (_) {} }
if (!JSDOM) { try { ({ JSDOM } = require('jsdom')); } catch (_) { console.error('jsdom not found: set JSDOM_DIR'); process.exit(2); } }
const ACT = fs.readFileSync(path.join(root, 'station-activity.js'), 'utf8');
const WORK = fs.readFileSync(path.join(root, 'station-work.js'), 'utf8');
const T0 = Date.parse('2026-11-09T15:00:00Z');
let passed = 0; const ok = (c, m) => { assert.ok(c, m); passed++; };

function page(o = {}) {
  const dom = new JSDOM('<!doctype html><html><body></body></html>', { url: `http://station.test/${o.device || 'assembly-1'}.html`, pretendToBeVisual: true, runScripts: 'outside-only' });
  const w = dom.window;
  const h = { w, now: o.now || T0, did: [], completes: [], toasts: [], guard: true, fail: false, station: o.station || 'assembly', device: o.device || 'assembly-1', signedIn: o.signedIn !== false };
  w.Date.now = () => h.now;
  w.fetch = () => Promise.resolve({ status: 200, json: () => Promise.resolve({}) });
  w.M = { toast: x => h.toasts.push(String(x.html)) };
  w.StationSession = { who: () => (h.signedIn ? { person: 'Ivy', station: h.station, device: h.device, computer: 'pc-ABCDEFGHJKMN', session: 'sess-ABCDEFGH-1', startAt: T0 - 60000 } : null), page: () => ({ station: h.station, device: h.device, computer: 'pc-ABCDEFGHJKMN' }) };
  w.StationTimeline = { guard: async () => h.guard, did: (t, id) => { h.did.push([t, id]); return {}; } };
  w.eval(ACT); w.eval(WORK);
  h.W = w.StationWork;
  h.W.init({ station: h.station, device: h.device, label: 'Assembly 1', guarded: !!o.guarded,
    complete: async id => { h.completes.push(id); if (h.fail) return { ok: false, message: 'not now' }; w.StationTimeline.did('assembled', id); return { ok: true, parts: 3 }; } });
  h.q = () => JSON.parse(w.localStorage.getItem(`station_activity_q.${h.device}`) || '[]');
  h.acts = a => h.q().filter(e => e.action === a);
  return h;
}

(async () => {
  /* 1 · a scan only opens the order: nothing is completed, nothing is drawn */
  {
    const h = page();
    h.W.viewed('3500000001', { parts: 3 });
    ok(h.acts('complete').length === 0 && h.acts('matched').length === 0 && h.did.length === 0, 'opening an order completes nothing and seals nothing');
    ok(h.w.document.body.children.length === 0, 'no panel, no pop-up: nothing is added to the page');
  }

  /* 2 · Complete Order: the station's completion once, the person credited, the time since the order was opened */
  {
    const h = page();
    h.W.viewed('3500000010', { parts: 3 });
    h.now += 7 * 60000;
    ok(await h.W.complete('3500000010'), 'Complete Order works');
    ok(h.completes.length === 1 && h.did.length === 1 && h.did[0][0] === 'assembled', 'the seal is stamped once');
    const c = h.acts('complete');
    ok(c.length === 1 && c[0].person === 'Ivy' && c[0].orders === 1 && c[0].parts === 3 && c[0].orderId === '3500000010', 'one complete, credited to the person, with its pieces');
    ok(c[0].workMs === 7 * 60000 && /^Complete Order/.test(c[0].detail), 'with the time since the order was opened');
    ok(await h.W.complete('3500000010') && h.q().length === 1, 'a double-click (a moment later) is the same press: nothing more');
    h.now += 5000;
    ok(await h.W.complete('3500000010'), 'pressed again');
    ok(h.acts('complete').length === 1 && /already completed/.test(h.acts('note')[0].detail), 'a note, never a second completion');
    ok(h.W.completedHere('3500000010'), 'this station remembers it');
    const r = page({ now: h.now + 60000 }); r.w.localStorage.setItem('station_work_done.assembly-1', h.w.localStorage.getItem('station_work_done.assembly-1'));
    ok(r.W.completedHere('3500000010'), 'also after a reload');
  }

  /* 3 · an order completed without having been opened here (typed straight in): no time is made up */
  {
    const h = page();
    ok(await h.W.complete('3500000020'), 'completed');
    ok(h.acts('complete')[0].workMs === undefined, 'no time when it was never opened here');
    ok(!(await h.W.complete('')), 'no order on screen: nothing');
  }

  /* 4 · a cancelled order the person backs out of, and a completion that fails: nothing recorded */
  {
    const h = page();
    h.guard = false;
    ok(!(await h.W.complete('3500000030')) && h.completes.length === 0 && h.acts('complete').length === 0, 'a cancelled order (Stop in the alert): not completed');
    h.guard = true; h.fail = true;
    ok(!(await h.W.complete('3500000030')) && h.acts('complete').length === 0 && /not now/.test(h.toasts.join()), 'a failed completion: nothing counted, the reason shown');
    h.fail = false;
    ok(await h.W.complete('3500000030') && h.acts('complete').length === 1, 'the next press goes through');
  }

  /* 5 · a station that guards itself (Shipping): no second guard; with nobody signed in the completion still runs (as before), nothing is credited */
  {
    const h = page({ station: 'shipping', device: 'shipping-1', guarded: true });
    h.guard = false;                                                 // (the module never asks: the station's own completion does)
    ok(await h.W.complete('3500000040') && h.completes.length === 1 && h.acts('complete').length === 1, 'the station\'s own guard decides');
    const g = page({ station: 'shipping', device: 'shipping-1', guarded: true, signedIn: false });
    ok(await g.W.complete('3500000041') && g.completes.length === 1 && g.acts('complete').length === 0, 'signed out: the Etsy completion still runs, no one is credited');
  }

  /* 6 · the pages */
  {
    const read = f => fs.readFileSync(path.join(root, f), 'utf8');
    for (const f of ['weld-1.html', 'assembly-1.html', 'assembly-2.html', 'assembly-3.html', 'assembly-4.html']) {
      const s = read(f);
      ok(!/trackOrderProxy/.test(s), `${f}: no Etsy tracking endpoint`);
      ok(/StationWork\.init\(/.test(s) && /station-work\.js\?v=/.test(s) && /StationWork\.complete\(document\.getElementById\("etsyOrderNumber"\)/.test(s), `${f}: the Complete Order button completes the order on screen`);
      ok(!/#completeOrderBtn\s*\{\s*display:\s*none/.test(s), `${f}: the Complete Order button is shown`);
    }
    for (const f of ['shipping-1.html', 'shipping-2.html', 'shipping-3.html']) {
      const s = read(f);
      ok(/trackOrderProxy/.test(s) && /StationWork\.init\(/.test(s) && /guarded: true/.test(s), `${f}: the Etsy completion is kept, through station-work.js`);
    }
    const weld = read('weld-1.html');
    ok(!/\.then\(r => \{ if \(r && !r\.cancelled\) weldDone/.test(weld), 'weld-1: the scan no longer welds');
    ok(!/weldAct\("complete"/.test(weld), 'weld-1: the scan logs no complete');
    ok(/matched: false/.test(weld), 'weld-1: the phone scan is no matched credit');
    ok(/weldDone\(id, d\)/.test(weld), 'weld-1: Complete Order welds by the stud-line rule');
    for (const f of ['assembly-1.html', 'assembly-4.html']) {
      const s = read(f);
      ok(!/did\("assembled", orderId, "Assembled · " \+ msgTxt/.test(s), `${f}: the Team stamp no longer seals assembled`);
      ok(!/actLog\("complete"/.test(s), `${f}: the Team stamp is no completion`);
    }
  }

  /* 7 · the efficiency aggregation */
  {
    const Work = require(path.join(root, 'netlify/functions/_stationWork.js'));
    const A = require(path.join(root, 'netlify/functions/_stationActivity.js'));
    const ev = (action, orderId, extra) => Object.assign({ station: 'assembly', action, orderId, parts: 0, orders: 0, detail: '', at: T0 }, extra);
    const day1 = [
      ev('scan', '1001'), ev('scan', '1002'), ev('scan', '1003'),
      ev('complete', '1001', { parts: 3, orders: 1, detail: 'Complete Order', workMs: 600000, at: T0 + 600000 }),
      ev('complete', '1002', { parts: 1, orders: 1, detail: 'Complete Order', workMs: 120000, at: T0 + 900000 }),
      ev('complete', '1009', { parts: 5, orders: 1, detail: 'Team stamp QA1' })];                 // an old completion: not a Complete Order press
    const p = Work.patch(null, day1);
    ok(p.assembly.done === 2 && p.assembly.parts === 4 && p.assembly.timed === 2 && p.assembly.ms === 720000, 'patch counts presses, pieces and times');
    const r = Work.rows(Work.merge(null, p))[0];
    ok(r.station === 'assembly' && r.completed === 2 && r.pieces === 4, 'completed orders and pieces');
    ok(r.avgMin === 6 && r.medianMin === 6, 'average and median opened → Complete (10 and 2 minutes)');
    ok(r.viewedOnly === 1 && r.viewed === 3, 'one order opened and never completed (1003)');
    const day2 = Work.patch(null, [ev('complete', '1003', { parts: 2, orders: 1, detail: 'Complete Order', workMs: 300000 })]);
    const r2 = Work.rows(Work.merge(Work.merge(null, p), day2))[0];
    ok(r2.completed === 3 && r2.medianMin === 5 && r2.viewedOnly === 0, 'two days add up; the median of 10, 2 and 5 minutes is 5; 1003 is completed the next day');
    ok(Work.rows(Work.merge(null, Work.patch(null, [Object.assign(ev('scan', '1'), { station: 'sorting' })]))).length === 0, 'other stations are not read');
    const c2 = A.clean({ id: 'assembly-1_ABCD_2_x', station: 'assembly', device: 'assembly-1', person: 'Ivy', action: 'complete', orderId: '3500000099', parts: 3, orders: 1, detail: 'Complete Order', workMs: 420000, at: T0 }, T0 + 1000, '');
    ok(c2.doc.workMs === 420000, 'workMs is kept by the door');
    const FV = { increment: n => ({ inc: n }) };
    const roll = A.rollupPatch(FV, null, '2026-11-09', 'Ivy', [c2.doc], '');
    ok(roll.work && roll.work.assembly.done.inc === 1 && roll.work.assembly.ms.inc === 420000 && roll.work.assembly.w['3500000099'] === 1, 'the rollup gets the work counters as increments');
  }

  console.log(`station-work: ${passed} checks passed`);
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
