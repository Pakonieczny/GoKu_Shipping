// NOWCMD (Paul, 21 Nov 2026): a Change SKU on a staff computer said "Asked of the cloud: The cloud has not started it yet: it does it at its next pass (every minute or so). Nothing more
// is needed here." — "if a user makes a change like this that is important … it must be propagated immediately not on a next pass … if the cloud does not properly update this and
// there's a mistake the user will never know". Offline (_cloudFakes.cjs, the real library handler and driver; a jsdom page with the real follower and the real CNRelayAsk):
//   C1  a person's request put while a pass is mid-step (the lease running: driveSoon only queues) is done by THAT pass, between two units, before it ends
//   C2  two requests of one computer put during the pass's last unit are done, in the order asked, before the pass ends; a refusal is answered in words
//   C3  the next pass does none of them again (idempotent: nothing moves, the answers stay as they were)
//   C4  no pass running: the press starts one at once (driveSoon), which does it
//   C5  mode "off": nothing is started and nothing is done (as before)
//   R1  the page follows a request to DONE: the live line ("The cloud has it…", "The cloud is doing it… <step>"), the cloud's words; never "nothing more is needed"
//   R2  the page follows a request to FAILED: a red persistent notice in the panel AND a toast, with the reason and Try again; Try again sends the same request and follows it to done
//   R3  no answer after 5 minutes: said plainly in red (still queued; Check again / Try again), still followed: the late answer is shown when it comes
//   R4  the callers: no "Nothing more is needed" left for the cloud; Change SKU, keepsync, skuonce, Remake, the grid, Custom send, hop/fill, seat, placewaiting use the follower; tokens bumped
//   NODE_PATH=<deps> node tests/charm-nest/cloud-cmd-now.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path');
const ROOT = path.join(__dirname, '../..');
const T0 = Date.UTC(2026, 10, 21, 18, 0, 0);
const PART_W = process.env.CMDNOW_PART === 'commit';   // (part W runs in its own process, with its own in-memory cloud)
const F = PART_W ? null : require('./_cloudFakes.cjs')({ now: T0 });
const log0 = console.log.bind(console); console.log = () => {}; console.warn = () => {};
let failed = 0;
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + JSON.stringify(detail).slice(0, 1400))); if (!cond) failed++; };
const lib = F && F.load('charmNestLibrary.js'), Drive = F && F.load('_charmNestDrive.js'), Lease = F && F.load('_charmNestLease.js'), Unit = F && F.load('_charmNestCmdUnit.js');
const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
const CV = 'cloud14-20261120', DEV = 'd0staff0001', R = 'run-2026-11-21-cmdnow', A = 'g10-a-cmdnow', B = 'g10-b-cmdnow', C = 'g10-c-cmdnow', LEASE = 'Charm_Nest_Drive/lease';
const pid = (o, l, n) => `${o}_${o}${l}_${n}`;
const charm = (id, poolId) => ({ id, poolId, order: poolId.split('_')[0], name: id, areaPt2: 300, widthPt: 20, heightPt: 20 });
function seed(mode, status) {
  F.reset(); F.clock.set(T0);
  F.seed('Charm_Nest_Open/drive', { mode, runMode: 'manual', cloudSteps: ['nest', 'cmd:hop'], flippedAt: T0 - 3600e3 }); lib._driveForget();
  F.seed('Charm_Nest_Runs/' + R, { runId: R, status: status || 'running', step: 'nest', mode: 'auto', day: '2026-11-21', sheets: {}, holds: {}, errors: [], orders: [] });
  F.seed('Charm_Nest_Open/run', { runId: R, at: T0 - 3600e3 });
  F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
  const sheet = (id, page, list, o) => {
    const charms = list.map(([cid, p]) => charm(cid, p));
    F.seed('Charm_Nest_Sheets/' + id, Object.assign({ id, runId: R, metal: 'gold10k', page, draft: true, status: 'complete', layoutRev: 4, contentRev: 4, fileBase: id, charms, poolIds: charms.map(c => c.poolId),
      placements: charms.map((c, i) => ({ id: c.id, cxPt: 20 + i * 30, cyPt: 30, angle: 0, wPt: 20, hPt: 20, scale: 1 })), orders: [...new Set(charms.map(c => c.order))] }, o || {}));
    for (const c of charms) F.seed('Charm_Pool/' + c.poolId, { poolId: c.poolId, runId: R, orderId: c.order, state: 'nested', sheetId: id });
  };
  sheet(A, 1, [['a1', pid('111', '1', 1)], ['a2', pid('111', '1', 2)], ['a3', pid('222', '1', 1)]]);
  sheet(B, 2, [['b1', pid('333', '1', 1)]]);
  sheet(C, 3, [['c1', pid('444', '1', 1)]], { draft: false, setId: 'set-cmdnow' });
  F.seed('Charm_Nest_Sets/set-cmdnow', { setId: 'set-cmdnow', runId: R, status: 'awaiting review', sheetIds: [C], orders: {} });
}
const hop = o => Object.assign({ type: 'hop', sourceId: A, targetId: B, ids: [pid('111', '1', 1), pid('111', '1', 2)], poses: [{ poolId: pid('111', '1', 1), cxPt: 120, cyPt: 60, angle: 90 }, { poolId: pid('111', '1', 2), cxPt: 150, cyPt: 60, angle: 90 }], layoutBase: 4, label: 'Move order 111' }, o || {});
const cmds = () => (F.doc('Charm_Nest_Run_Cmds/' + R) || {}).cmds || {};
const cmdOf = id => cmds()[id] || {};
const ask = async cmd => { const r = await P({ op: 'runCmdPut', runId: R, dev: DEV, by: 'Michelle', cmd, cv: CV }); return r.id || null; };
const passWith = async (nonce, steps) => Drive.pass({ db: F.db, Lib: lib, nonce, steps: Object.assign(Drive.defaultSteps(), steps || {}), passItems: Unit.units.passItems, sleep: async () => {}, renewMs: 0, budgetMs: 600000 });
const dispatch = async () => (await Lease.dispatch(F.db, F.db.collection('Charm_Nest_Drive').doc('lease'), 'test', { heartbeat: true })).nonce;
const bgKicks = () => F.net.kicks.filter(k => k.name === 'charmNestDrive-background');

async function cloud() {
  /* C1-C3 */
  seed('on');
  const nonce = await dispatch(), seen = {}, ids = {};
  const steps = { nest: [
    async () => { F.net.kicks.length = 0; ids.first = await ask(hop()); seen.lease = Object.assign({}, F.doc(LEASE)); seen.kicksAtPut = bgKicks().length; F.clock.advance(20000); },
    async () => { seen.atUnit1 = Object.assign({}, cmdOf(ids.first)); seen.leaseAtUnit1 = (F.doc(LEASE) || {}).nonce; F.clock.advance(5000); },
    async () => { ids.second = await ask(hop({ sourceId: B, targetId: A, ids: [pid('333', '1', 1)], poses: [{ poolId: pid('333', '1', 1), cxPt: 120, cyPt: 60, angle: 0 }], layoutBase: undefined, label: 'Move order 333' })); F.clock.advance(1000);
      ids.third = await ask(hop({ sourceId: A, targetId: C, ids: [pid('222', '1', 1)], poses: [{ poolId: pid('222', '1', 1), cxPt: 60, cyPt: 60, angle: 0 }], layoutBase: undefined, label: 'Move order 222 into the set' })); }
  ] };
  const out = await passWith(nonce, steps).catch(e => ({ error: String(e && e.stack || e) }));
  const c1 = cmdOf(ids.first), b = F.doc('Charm_Nest_Sheets/' + B) || {};
  ok('C1 · the request was put while the pass ran (lease running: the press only queued the next pass, no new pass was started)', seen.lease.state === 'running' && seen.lease.nonce === nonce && seen.lease.again === true && seen.kicksAtPut === 0, { lease: seen.lease, kicks: seen.kicksAtPut });
  ok('C1 · THAT pass did it between two units: answered done before the next unit began, under the same lease', seen.atUnit1 && seen.atUnit1.state === 'done' && /Order 111: 2 pieces moved/.test(seen.atUnit1.text || '') && seen.leaseAtUnit1 === nonce && c1.claim && c1.claim.dev === 'cloud', { atUnit1: seen.atUnit1, out });
  ok('C1 · the pieces are where the person put them (on B, pinned), B owes its files', [1, 2].every(n => (F.doc('Charm_Pool/' + pid('111', '1', n)) || {}).sheetId === B) && (b.placements || []).some(q => q.id === 'a1' && q.cxPt === 120) && b.filesOwed > 0, b.placements);
  const c2 = cmdOf(ids.second), c3 = cmdOf(ids.third);
  ok('C2 · both requests put during the last unit were answered before the pass ended (the same pass)', ['done', 'failed'].includes(c2.state) && ['done', 'failed'].includes(c3.state) && out && out.ok && Array.isArray(out.cmdNow) && out.cmdNow.length >= 2, { c2, c3, out });
  ok('C2 · in the order asked (one computer\'s presses in order)', +c1.doneAt <= +c2.doneAt && +c2.doneAt <= +c3.doneAt, [c1.doneAt, c2.doneAt, c3.doneAt]);
  ok('C2 · the second is done; the third (into a set) is refused in words, never left waiting', c2.state === 'done' && c3.state === 'failed' && String(c3.text || '').length > 10, { c2: [c2.state, c2.text], c3: [c3.state, c3.text] });
  const snap = JSON.stringify([F.doc('Charm_Nest_Sheets/' + A), F.doc('Charm_Nest_Sheets/' + B), F.doc('Charm_Nest_Sheets/' + C), cmds()]);
  F.clock.advance(120000);
  const n2 = (F.doc(LEASE) || {}).state === 'dispatched' ? F.doc(LEASE).nonce : await dispatch();
  const out2 = await passWith(n2, { nest: [async () => {}] }).catch(e => ({ error: String(e) }));
  ok('C3 · the next pass did none of them again (sheets and answers unchanged)', out2 && out2.ok && JSON.stringify([F.doc('Charm_Nest_Sheets/' + A), F.doc('Charm_Nest_Sheets/' + B), F.doc('Charm_Nest_Sheets/' + C), cmds()]) === snap, out2);

  /* C4: no pass running: the press starts one at once, and it does the request */
  seed('on', 'processed');
  F.net.kicks.length = 0;
  const id4 = await ask(hop());
  const k = bgKicks();
  ok('C4 · no pass running: the press started a pass at once (driveSoon kicked the driver)', k.length === 1 && (F.doc(LEASE) || {}).state === 'dispatched', { kicks: F.net.kicks.map(x => x.name), lease: F.doc(LEASE) });
  const out4 = await passWith(JSON.parse(k[0].body).nonce).catch(e => ({ error: String(e) }));
  ok('C4 · that pass did it: answered done', cmdOf(id4).state === 'done' && out4 && out4.ok, { c: cmdOf(id4), out4 });

  /* C5: mode off */
  seed('off');
  F.net.kicks.length = 0;
  const id5 = await ask(hop());
  const n5 = await dispatch(), out5 = await passWith(n5).catch(e => ({ error: String(e) }));
  ok('C5 · mode off: nothing is started, the pass refuses, the request is not done by the cloud', !bgKicks().length && out5 && out5.ok === false && !!out5.refused && cmdOf(id5).state === 'waiting', { kicks: F.net.kicks, out5, c: cmdOf(id5) });
  ok('C · no network beyond the fakes, no Etsy', !F.net.other.length && !F.net.tripwire.length, { other: F.net.other, trip: F.net.tripwire });
}

/* ═══ R · the page ═══ */
async function page() {
  const { JSDOM } = require('jsdom');
  const bridge = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8');
  const i = bridge.indexOf('window.CNRelayAsk = async function'), j = bridge.indexOf('\n};\n', i), relay = bridge.slice(i, j + 3);
  function world(script) {
    const dom = new JSDOM('<div id="toasts"></div><section id="panel"></section>', { url: 'https://test.invalid', runScripts: 'outside-only' }), w = dom.window;
    let clock = T0; const nap = [];
    w.Date.now = () => clock;
    w.setTimeout = (f, ms) => { clock += +ms || 0; setImmediate(f); return 0; };
    const toasts = [];
    w.toast = (msg, kind, ms) => { const el = w.document.createElement('div'); el.className = 'toast ' + (kind || ''); el.innerHTML = '<span class="m"></span><button class="c">x</button>'; el.querySelector('.m').textContent = msg; el._go = () => el.remove(); w.document.getElementById('toasts').appendChild(el); toasts.push({ msg, kind, el }); return el; };
    const S = { list: {}, puts: [], script, n: 0 };
    w.api = async (fn, body) => {
      if (body.op === 'runCarrier') return { ok: true, alive: true, mine: false, carrier: { dev: 'cloud', by: 'Cloud', at: clock } };
      if (body.op === 'runCmdPut') { const id = 'c' + (++S.n); S.puts.push({ id, cmd: body.cmd }); S.list[id] = { id, type: body.cmd.type, state: 'waiting', at: clock, looks: 0 }; return { ok: true, id }; }
      if (body.op === 'runCmdList') { for (const c of Object.values(S.list)) { c.looks++; const nx = S.script(c, clock - c.at, S.puts.findIndex(p => p.id === c.id)); if (nx) Object.assign(c, nx); } return { ok: true, cmds: Object.values(S.list).map(c => Object.assign({}, c)) }; }
      return { ok: true };
    };
    w.CN = { api: (fn, body) => w.api(fn, body) };
    w.B = { run: null }; w.employeeName = () => 'Michelle';
    w.CharmNestRunFollow = { state: () => ({ runId: 'run-r' }), dev: () => DEV, now: () => {} };
    w.CNViewer = { on: () => true };
    w.eval(fs.readFileSync(path.join(ROOT, 'charm-nest-cmd-now.js'), 'utf8'));
    w.eval(relay);
    return { w, S, toasts, panel: w.document.getElementById('panel'), clock: () => clock };
  }
  const NOTHING = /Nothing more is needed/;

  /* R1 done */
  {
    const P1 = world((c, age) => age < 4000 ? null : age < 9000 ? { state: 'running', step: 'Making order 111 again from CELESTIAL3-COMET' } : { state: 'done', text: '1 order line(s) made again: 1 piece(s) on GF Draft 1' });
    const steps = [], ans = await P1.w.CNRelayAsk({ type: 'remakeline', ids: ['111_1111'], label: 'Change SKU: A → B' }, t => steps.push(t), 300000, { always: true, el: P1.panel });
    ok('R1 · followed to done: the cloud\'s own words', ans.ok === true && /made again/.test(ans.text) && ans.cloud === true, ans);
    ok('R1 · the live line: "The cloud has it…" then "The cloud is doing it… <its step>"', steps.some(t => /The cloud has it — it starts within seconds/.test(t)) && steps.some(t => /^The cloud is doing it… Making order 111 again/.test(t)) && !steps.some(t => NOTHING.test(t)), steps);
    ok('R1 · the panel shows it done (green), no red toast', /made again/.test(P1.panel.textContent) && P1.panel.querySelector('.cnNow').dataset.k === 'ok' && !P1.toasts.some(t => t.kind === 'bad'), { panel: P1.panel.innerHTML, toasts: P1.toasts.map(t => t.msg) });
  }
  /* R2 failed, then Try again */
  {
    const P2 = world((c, age, n) => n === 0 ? (age < 3000 ? null : { state: 'failed', text: 'Order 111: its sheet GF Draft 1 is cut on the laser: nothing was changed.' }) : (age < 3000 ? { state: 'running', step: 'Making order 111 again' } : { state: 'done', text: '1 order line(s) made again' }));
    const steps = [], ans = await P2.w.CNRelayAsk({ type: 'remakeline', ids: ['111_1111'], label: 'Change SKU: A → B' }, t => steps.push(t), 300000, { always: true, el: P2.panel });
    const n = P2.panel.querySelector('.cnNow'), tb = P2.toasts.find(t => t.kind === 'bad');
    ok('R2 · followed to failed: the reason, never success', ans.ok === false && ans.failed === true && /cut on the laser/.test(ans.text), ans);
    ok('R2 · a red persistent notice in the panel with the reason and Try again', n && n.dataset.k === 'bad' && n.getAttribute('role') === 'alert' && /not done — .*cut on the laser/.test(n.textContent) && !!n.querySelector('[data-cn-now="Try again"]'), n && n.outerHTML);
    ok('R2 · AND a red toast with the reason and Try again', !!tb && /did not do it — .*cut on the laser/.test(tb.msg) && !!tb.el.querySelector('[data-cn-now="Try again"]'), P2.toasts.map(t => [t.msg, t.kind]));
    n.querySelector('[data-cn-now="Try again"]').click();
    for (let k = 0; k < 200 && !(P2.panel.querySelector('.cnNow') || {}).dataset || (P2.panel.querySelector('.cnNow').dataset.k !== 'ok' && k < 200); k++) await new Promise(r => setImmediate(r));
    ok('R2 · Try again sent the SAME request again and followed it to done (the panel says so)', P2.S.puts.length === 2 && JSON.stringify(P2.S.puts[0].cmd) === JSON.stringify(P2.S.puts[1].cmd) && P2.panel.querySelector('.cnNow').dataset.k === 'ok' && /made again/.test(P2.panel.textContent), { puts: P2.S.puts, panel: P2.panel.innerHTML });
  }
  /* R3 late: 5 minutes with no answer, then the answer comes */
  {
    const P3 = world((c, age) => age < 7 * 60000 ? null : { state: 'done', text: '1 order line(s) made again' });
    const t0 = P3.clock(), ans = await P3.w.CNRelayAsk({ type: 'skuonce', ids: ['111_1111'], sku: 'X', label: 'Just this order: X' }, () => {}, 240000, { always: true, el: P3.panel });
    const n = P3.panel.querySelector('.cnNow'), tb = P3.toasts.find(t => t.kind === 'bad');
    ok('R3 · after 5 minutes with no answer: late, never success, said plainly', ans.ok === false && ans.late === true && /has not done it yet — it is still queued; check again or press Try again/.test(ans.text) && P3.clock() - t0 >= 300000 && P3.clock() - t0 < 320000, { ans, waited: P3.clock() - t0 });
    ok('R3 · red in the panel and in a toast, with Check again and Try again', n && n.dataset.k === 'late' && !!n.querySelector('[data-cn-now="Try again"]') && !!n.querySelector('[data-cn-now="Check again"]') && !!tb && /has not done it yet/.test(tb.msg), { panel: n && n.outerHTML, toasts: P3.toasts.map(t => t.msg) });
    const later = await ans.later;
    ok('R3 · still followed: the late answer is shown when it comes (the panel and a toast)', later && later.ok === true && P3.panel.querySelector('.cnNow').dataset.k === 'ok' && P3.toasts.some(t => t.kind === 'ok' && /made again/.test(t.msg)), { later, toasts: P3.toasts.map(t => [t.msg, t.kind]) });
  }
  /* R3b the relink panel's Try again button (charm-nest-relink-flow.js act) */
  {
    const dom = new JSDOM('<body></body>', { url: 'https://test.invalid', runScripts: 'outside-only' }), w = dom.window;
    w.eval(fs.readFileSync(path.join(ROOT, 'charm-nest-relink-flow.js'), 'utf8'));
    const f = w.CNRelinkFlow.open({ title: 'Edit SKU' }); let pressed = 0;
    f.line('carrier', 'The cloud did not make it again: cut', 'failed'); f.act('carrier', 'Try again', () => { pressed++; });
    const btn = f.el.querySelector('[data-rl-act="Try again"]'); btn.click(); await new Promise(r => setImmediate(r));
    ok('R3b · the Change SKU panel\'s red line carries Try again, which asks again', !!btn && pressed === 1 && f.lines().find(x => x.id === 'carrier').state === 'work', f.lines());
  }
  /* R4 the callers */
  {
    const read = f => fs.readFileSync(path.join(ROOT, f), 'utf8');
    const relaySrc = relay.slice(0, relay.indexOf('for (const t0 = Date.now(); Date.now() - t0 < (maxMs || 600000);)', relay.indexOf('if (cloudOn) {')));
    ok('R4 · CNRelayAsk in cloud mode: no "Nothing more is needed", no 25-second give-up, the follower used, Try again = the same request put again', !NOTHING.test(relaySrc.slice(relaySrc.indexOf('if (cloudOn) {'))) && /N\.follow\(runId, put\.id/.test(relay) && /resend/.test(relay) && !/has not started it yet: it does it at its next pass/.test(bridge));
    const sku = bridge.slice(bridge.indexOf('async function editSku(row, sku, opt)'), bridge.indexOf('async function editSkuOnce('));
    ok('R4 · Change SKU: the panel line follows the answer, red on failure or late, with Try again on the panel', /flow\.panel\.act\("carrier", "Try again"/.test(sku) && /ans\.later/.test(sku) && !/Asked of the cloud: \$\{ans\.text\}/.test(sku));
    const once = bridge.slice(bridge.indexOf('async function editSkuOnce('), bridge.indexOf('async function editSkuOnce(') + 6000);
    ok('R4 · Just this order: a late or failed answer is not "ok" (said in red in the window)', /ans && ans\.late\) return \{ ok: false, late: true/.test(once) && /onStep: t => say\("", esc/.test(bridge));
    ok('R4 · keepsync (relink) and Remake (Review) and Merge sheets treat late / failed in red', /fl\.act\("carrier", "Try again"/.test(bridge) && /NOWCMD: CNRelayAsk said it already, in red, with Try again/.test(bridge) && /r && r\.late\) \{ bad = true;/.test(bridge));
    ok('R4 · Custom send and CNAskPlace (placewaiting) follow through the follower', /CNCmdNow\.follow\(runId,put\.id/.test(bridge) && /CNCmdNow\.watch\(runId, put\.id/.test(bridge));
    const os = read('charm-nest-options-sheets.js');
    ok('R4 · the Options sheets grid: no "Nothing more is needed", the cloud\'s step, a kept red failure with Try again, 5-minute late in red', !NOTHING.test(os) && /the cloud is doing it… ' \+ r\.step/.test(os) && /keep: true, retry: rel/.test(os) && /data-sh-retry/.test(os) && /LATE_MS/.test(os));
    ok('R4 · hop, fill, seat, custom piece and the viewer\'s follow use the follower', /N\.follow\(runId, r\.id/.test(read('charm-nest-sheet-hop.js')) && /N\.shout\(/.test(read('charm-nest-sheetwin.js')) && /N\.follow\(runId, put\.id/.test(read('charm-nest-partial-ui.js')) && /N\.follow\(runId, put\.id/.test(read('charm-nest-custom-piece.js')) && /N\.follow\(runId, id/.test(read('charm-nest-viewer.js')));
    const html = read('charm-nest-1.html');
    ok('R4 · charm-nest-1.html loads the follower and every changed page file under a new token', /<script src="charm-nest-cmd-now\.js\?v=[^"]*now"/.test(html) && ['bridge', 'viewer', 'options-sheets', 'relink-flow', 'sheet-hop', 'sheetwin', 'partial-ui', 'custom-piece'].every(f => new RegExp(`charm-nest-${f}\\.js\\?v=[^"]*-now(?:-\\w+)*"`).test(html)) && /"charm-nest-cmd-now\.js"/.test(read('scripts/build-public.cjs')));
    const drive = read('netlify/functions/_charmNestDrive.js');
    ok('R4 · the driver reads the requests between units, after pass items and before the pass ends (NOWCMD)', (drive.match(/await cmdNow\(/g) || []).length >= 6 && /NOWCMD/.test(drive));
  }
}

const CV_W = 'cloud14-20261120';
const LIVE_STEPS = ['pull', 'claim', 'pool', 'plan', 'nest', 'checkpoint', 'engrave', 'revalidate', 'labels', 'commit', 'complete', 'files', 'takeoff', 'rosecut', 'sweeps',
  'cmd:reorder', 'cmd:consolidate', 'cmd:empty', 'cmd:delete', 'cmd:repicture', 'cmd:seat', 'cmd:renest', 'cmd:remake', 'cmd:unkeep', 'cmd:placewaiting', 'cmd:customsend', 'cmd:setrebuild',
  'cmd:pairnow', 'cmd:leaveset', 'cmd:sendhome', 'cmd:remakeline', 'cmd:keepsync', 'cmd:release', 'cmd:skuonce', 'cmd:hop'];

/* ═══ W · (NOWCMD, the coordinator's scope addition) the run WAITS at commit (the Etsy freshness gate: the shop's daily limit) — the real crons, driver, intake, placement, nest and sheet
   writer over _cloudFakes.cjs, mode "on", no computer, as viewer-item16 C:
     W1  Change SKU's remakeline reaches "on a sheet": the request makes its own lines' pieces (no waiting for the intake's gate), puts them back, nests the sheet, answers done
     W2  a new order arriving while the run waits at commit is made up, placed on a draft and nested (the intake's gate is open while the run merely waits at labels / commit) ═══ */
async function partW() {
  process.env.EDIT_PASSCODE = 'test-pass';
  const { performance } = require('perf_hooks');
  const T0 = Date.UTC(2026, 10, 21, 15, 0, 0);
  const F = require('./_cloudFakes.cjs')({ now: T0 });   // (a fresh in-memory cloud, in its own process)
  const p0 = performance.now();
  Date.now = () => F.clock.t + Math.floor(performance.now() - p0);
  const said = [], VERBOSE = !!process.env.CN_VERBOSE;
  console.log = (...a) => { said.push(a.join(' ')); if (VERBOSE) log0(...a); };
  console.warn = (...a) => { said.push('WARN ' + a.join(' ')); if (VERBOSE) log0('WARN', ...a); };
  console.error = (...a) => { said.push('ERROR ' + a.join(' ')); if (VERBOSE) log0('ERROR', ...a); };
  const FIX = path.join(__dirname, 'fixtures');
  const lib = F.load('charmNestLibrary.js');
  const EtsyStop = F.load('_etsyStop.js'), worldCron = F.load('charmNestWorldCron.js'), driveCron = F.load('charmNestDriveCron.js');
  const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
  const OLD = 'COMET_9427', NEW = 'CELESTIAL3-COMET', GF = '14k Gold Filled', SS = 'Sterling Silver';
  const entries = JSON.parse(fs.readFileSync(path.join(FIX, 'comet-master-entries.json'), 'utf8')).entries;
  for (const sku of [OLD, NEW]) {
    const ai = 'charmnest/master/' + sku + '.ai';
    await F.bucket.file(ai).save(fs.readFileSync(path.join(FIX, sku + '.ai')), { contentType: 'application/postscript' });
    F.seed('Charm_Master_Index/' + sku, Object.assign({}, entries.find(e => e.sku === sku), { aiPath: ai, thumbPath: null, masterPath: null }));
  }
  const hashOf = sku => entries.find(e => e.sku === sku).charmHash;
  const shared = (name, fields) => F.seed('Charm_Nest_Shared/' + name, { name, f: Object.fromEntries(Object.entries(fields).map(([k, v]) => [k, { j: JSON.stringify(v), at: T0 - 3600e3, dev: 'd1aaaa', by: 'Ann' }])), at: T0 - 3600e3, n: 1 });
  shared('sorter-settings', { insetPt: 1.5, clearancePt: -0.5, maxFill: 0.8, budgetS: 3, pullMode: 'all', autoCommit: 'off' });
  shared('sorter-stock', { preset: 'A' });
  F.seed(EtsyStop.DOC, { stopped: false });
  const setDrive = runMode => { F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode, cloudSteps: LIVE_STEPS.slice(), at: T0 - 3600e3, flippedAt: T0 - 3600e3 }); lib._driveForget(); };
  setDrive('auto');
  let seq = 0;
  function receipt(rid, items) {
    const now = Math.floor(Date.now() / 1000) - 3600 + (seq++);
    return { receipt_id: rid, order_number: rid, name: 'Buyer ' + rid, country_iso: 'US', city: 'Austin', message_from_buyer: '', status: 'Paid', is_paid: true, is_shipped: false, is_gift: false, gift_message: '',
      create_timestamp: now, created_timestamp: now, update_timestamp: now, updated_timestamp: now,
      transactions: items.map(([lid, sku, metal], i) => ({ transaction_id: Number(`${rid}${i + 1}`), listing_id: lid, receipt_id: rid, sku, title: `${sku} charm`, quantity: 1, expected_ship_date: now + 3 * 86400, is_personalized: false,
        variations: [{ formatted_name: 'Metal', formatted_value: metal, property_id: 200, value_id: 9000 + i }] })) };
  }
  function arrive(r) {
    F.seed('EtsyMail_Receipts/' + r.receipt_id, { receipt_id: String(r.receipt_id), status: r.status, is_paid: true, is_shipped: false, updated_timestamp: r.updated_timestamp, raw: r });
    F.seed('EtsyMail_Config/receiptsMirrorState', { lastSyncOutcome: 'ok', lastSyncCompletedAt: F.Timestamp.fromMillis(Date.now()) });
  }
  let ticks = 0;
  async function tick() {
    ticks++;
    if (ticks % 5 === 0) F.seed('EtsyMail_Config/receiptsMirrorState', { lastSyncOutcome: 'ok', lastSyncCompletedAt: F.Timestamp.fromMillis(Date.now()) });
    await worldCron.handler(); await driveCron.handler();
    for (let i = 0; i < 20 && F.net.kicks.length; i++) await F.drain(100);
    F.clock.advance(60000);
  }
  const runIdNow = () => (F.doc('Charm_Nest_Open/run') || {}).runId || null;
  const live = p => !['abandoned', 'superseded'].includes(p.state);
  const poolOf = rid => F.list('Charm_Pool').filter(p => String(p.poolId || p._id).startsWith(rid + '_') && live(p));
  const sheetsNow = () => F.list('Charm_Nest_Sheets').filter(s => !s.archived);
  const sheetOf = id => sheetsNow().find(s => s._id === id) || null;
  const savedSheet = s => !!(s && s.outputs && s.outputs.ai && s.outputs.ai.path && s.verification && s.verification.ok && !s.filesOwed && !s.saving && !s.dirty);
  /** Where a piece is: its sheet lists it, holds its charm and seats it once. */
  const seat = id => { const s = sheetsNow().find(x => (x.poolIds || []).includes(id)); if (!s) return null; const c = (s.charms || []).find(z => z && String(z.poolId) === id), n = c ? (s.placements || []).filter(p => p && p.id === c.id).length : 0; return { s, c, n }; };
  const settled = rids => { const rows = rids.flatMap(poolOf); return rows.length >= rids.length && rows.every(p => { const w = seat(p.poolId || p._id); return w && w.n === 1 && p.sheetId === w.s._id && savedSheet(w.s); }); };
  const cmdOf = id => { const d = F.doc('Charm_Nest_Run_Cmds/' + runIdNow()) || {}; return (d.cmds || {})[id] || null; };
  const view = () => { const id = runIdNow(), r = id ? F.doc('Charm_Nest_Runs/' + id) || {} : {}; return `${id ? `${r.status}/${r.step}` : 'no run'} | pool ${F.list('Charm_Pool').filter(live).map(p => `${String(p._id).slice(6, 12)}:${p.sku}:${p.state}`).join(' ')} | sheets ${sheetsNow().map(s => `${s.metal}:${(s.poolIds || []).length}/${(s.placements || []).length}${s.outputs && s.outputs.ai ? ':files' : ''}${s.filesOwed ? ':owed' : ''}${s.nestOwed ? ':nestOwed' : ''}${s.nestJob ? ':nest-' + (s.nestJob.status || s.nestJob.state) : ''}`).join(' ')}`; };
  async function until(what, done, max) {
    let last = '';
    for (let k = 0; k < max; k++) { await tick(); const v = view(); if (v !== last) { if (VERBOSE) log0(`  tick ${ticks}: ${v}`); last = v; } if (done()) return true; }
    log0(`  ${what}: not settled after ${max} ticks: ${view()}\n    ` + said.slice(-30).join('\n    '));
    return false;
  }
  const ask = async (cmd, by) => { const r = await P({ op: 'runCmdPut', runId: runIdNow(), dev: PAGE, by: by || 'Michelle', cmd, cv: CV }); return r.id || (r.cmd && r.cmd.id) || null; };
  const ended = id => () => { const c = cmdOf(id); return !!c && ['done', 'failed', 'cancelled'].includes(c.state); };
  const linesNow = async () => { const g = await lib.call({ op: 'runGet', runId: runIdNow() }, {}); return (g && g.run && g.run.lines) || {}; };
  const keysOf = async rid => Object.keys(await linesNow()).filter(k => k.startsWith(rid + '_')).sort();
  /** Every live piece of these orders: its SKU, the charm its sheet holds for it (that charm's SKU or drawing), seated once on a saved sheet. */
  const pieces = rids => rids.flatMap(poolOf).map(p => { const id = p.poolId || p._id, w = seat(id); return { id, sku: p.sku, hash: p.charmHash || null, sheet: w && w.s._id, onSheetSku: w && w.c && (w.c.sku || (w.c.orderInfo && w.c.orderInfo.sku)), onSheetHash: w && w.c && (w.c.charmHash || w.c.hash || null), seated: w ? w.n : 0, saved: !!(w && savedSheet(w.s)), layoutRev: w ? +w.s.layoutRev || 0 : 0 }; });
  const CHG = '9000000001';   // (made-up)
  const LA = 1718099, A = '4310000001', B = '4310000002';
  const PAGE = 'd0michelle1', LIVE = LIVE_STEPS;
  arrive(receipt(A, [[LA, OLD, GF], [LA, OLD, SS]]));
  const firstOk = await until('A', () => settled([A]), 45);
  ok('W0 order A is made, placed, nested and written by the cloud alone', firstOk && pieces([A]).every(p => p.sku === OLD), { view: view() });
  if (!firstOk) return;
  const runId = runIdNow();
  // the run waits behind the Etsy freshness gate (the shop's daily limit is reached): it stands at revalidate, the first gated step, as Paul's run stood at commit
  F.seed('EtsyMail_Config/syncState', { etsyDailyLimitResetAt: F.Timestamp.fromMillis(Date.now() + 7 * 86400e3) });
  await F.db.collection('Charm_Nest_Runs').doc(runId).set({ status: 'running', step: 'revalidate', processingComplete: false }, { merge: true });
  for (let k = 0; k < 3; k++) await tick();
  const atCommit = () => { const r = F.doc('Charm_Nest_Runs/' + runId) || {}, st = F.doc('Charm_Nest_Drive/state') || {}; return r.status === 'running' && ['revalidate', 'labels', 'commit'].includes(r.step) && !!(st.wait && /daily limit/.test(String(st.wait.why || ''))); };
  ok('W0 the run waits behind the freshness gate ("Etsy\'s daily limit for the shop is reached")', atCommit(), { view: view(), wait: (F.doc('Charm_Nest_Drive/state') || {}).wait });
  const Intake = F.load('_charmNestIntake.js'), auto = { runMode: 'auto' };
  ok('W0 the intake\'s gate is open while the run merely waits at revalidate, labels or commit; open at the nest step and while a sheet is nested (FLOWNOW); closed for an operator\'s stop, the other mid-steps and manual mode',
    ['revalidate', 'labels', 'commit'].every(step => Intake.poolGate({ status: 'running', step }, auto, null) === '') && /stopped by the operator/.test(Intake.poolGate({ status: 'stopped', stopKind: 'operator', step: 'commit' }, auto, null))
    && Intake.poolGate({ status: 'running', step: 'commit' }, auto, new Map([['s1', { id: 's1', status: 'nesting' }]])) === '' /* FLOWNOW: a sheet being nested no longer holds new orders */ && Intake.poolGate({ status: 'running', step: 'nest' }, auto, null) === '' /* FLOWNOW: made at the nest step too */ && /next checkpoint/.test(Intake.poolGate({ status: 'running', step: 'plan' }, auto, null)) && /manual/.test(Intake.poolGate({ status: 'running', step: 'commit' }, { runMode: 'manual' }, null)));
  /* W1 */
  {
    const before = pieces([A]);
    const al = await P({ op: 'aliasPut', listingId: String(LA), sku: NEW, fromSku: OLD, by: 'Michelle', dev: PAGE, cv: CV });
    const kA = await keysOf(A), id = await ask({ type: 'remakeline', ids: kA, label: `Change SKU: ${OLD} → ${NEW}` });
    let steps = new Set();
    const done = await until('W1', () => { const c = cmdOf(id); if (c && c.step) steps.add(c.step); return ended(id)() && settled([A]); }, 30);
    const c = cmdOf(id) || {}, after = pieces([A]);
    ok('W1 the request answered done, in words, while the run still waits behind the gate', al.http === 200 && done && c.state === 'done' && /made again/.test(c.text || '') && atCommit(), { c, view: view() });
    ok('W1 the new pieces (the NEW design) are on a sheet, seated once, the sheet written again', after.length === before.length && after.every(p => p.sku === NEW && p.seated === 1 && p.saved), after);
    ok('W1 it never sat at "0 of N made" waiting for the intake\'s gate', ![...steps].some(s => /0 of \d+ made/.test(s)), [...steps]);
  }
  /* W2 */
  {
    arrive(receipt(B, [[LA, NEW, GF]]));
    const okB = await until('W2', () => settled([B]), 30);
    ok('W2 a new order arriving while the run waits behind the gate is made up, placed on a draft, nested and written', okB && atCommit() && pieces([B]).every(p => p.seated === 1 && p.saved), { view: view(), pieces: pieces([B]) });
    const sets = F.list('Charm_Nest_Sets').filter(s => s.runId === runId);
    ok('W2 no set changed hands: the new pieces went on sheets of no set (SETSHUT)', pieces([B]).every(p => { const s = F.doc('Charm_Nest_Sheets/' + p.sheet) || {}; return !s.setId || !sets.some(x => x.setId === s.setId && /commit/.test(String(x.status || ''))); }), { sets: sets.map(s => [s.setId, s.status]) });
  }
  void CHG; void hashOf; void LIVE;
}

(async () => {
  if (process.env.CMDNOW_PART === 'commit') { await partW(); log0(failed ? `FAIL part (${failed})` : 'PASS part'); process.exit(failed ? 1 : 0); }
  await cloud();
  await page();
  const r = require('child_process').spawnSync(process.execPath, [__filename], { env: Object.assign({}, process.env, { CMDNOW_PART: 'commit' }), encoding: 'utf8', timeout: 600000 });
  for (const l of (String(r.stdout || '') + String(r.stderr || '')).split('\n')) if (/^(OK|FAIL) |^  /.test(l)) log0(l);
  if (r.status !== 0) failed++;
  log0(failed ? `FAIL cloud-cmd-now (${failed})` : 'PASS cloud-cmd-now');
  process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL cloud-cmd-now: ' + (e && e.stack || e)); process.exit(1); });
