// FOLLOWRELEASE (Paul, 18 Nov 2026: "this order was released by a user and then it disappeared for several minutes and then it reappeared again under the review tab ... you must fix this"; order 4235861617, released by Michelle_R on a
// computer that only FOLLOWS the run: the timeline said "released", the run's line in the cloud stayed "held", and the next read put it back under Review).
// A release pressed on a following computer is now done by the computer that carries the run: asked through the run's command list (runCmdPut release), done there by OrderHold.release in the person's name, and answered.
//   1 the cloud takes a release command (its order id kept) and still refuses an unknown one
//   2 a FOLLOWING computer asks the carrier and changes nothing itself: no local release, no timeline step, the line stays as the cloud has it; it answers when the carrier has
//   3 a refusal by the carrier is told in the carrier's words;  no carrier answering (ANYWHERE, 19 Nov 2026): the request is put in the cloud all the same and kept there, answered "queued" (never a refusal)
//   4 the CARRYING computer picks the command up, releases the order in the asker's name, and answers done
// Real sorter in Chromium against the repo's fake site; nothing live.   PW_DIR=... CHROMIUM=... node tests/charm-nest/follow-release.cjs
const path = require('path');
if (process.argv[2]) process.env.PW_DIR = process.argv[2];
const { open } = require('./day-sim/harness.cjs');
let fails = 0;
const say = (name, ok, seen) => { if (!ok) fails++; console.log(`${ok ? 'OK  ' : 'FAIL'} ${name}${seen === undefined ? '' : ' :: ' + (typeof seen === 'string' ? seen : JSON.stringify(seen))}`); };
const RUN = 'run-rel-t', RID = '4999000001';

(async () => {
  const H = await open({ masters: [], receipts: [] });
  const page = H.page;
  await page.evaluate(() => { CN.setMode('nest'); });
  await page.waitForFunction(() => window.OrderHold && window.CharmNestRunFollow && window.Orders, null, { timeout: 30000 });

  // 1 ── the cloud
  const put = await page.evaluate(([RUN, RID]) => CN.api('charmNestLibrary', { op: 'runCmdPut', runId: RUN, dev: 'dtestfoll01', by: 'Michelle_R', cmd: { type: 'release', ids: [RID], label: 'x' } }).then(r => r, e => ({ error: String(e.message || e) })), [RUN, RID]);
  const bad = await page.evaluate(RUN => CN.api('charmNestLibrary', { op: 'runCmdPut', runId: RUN, dev: 'dtestfoll01', by: 'x', cmd: { type: 'bogus', ids: ['1'] } }).then(r => r, e => ({ error: String(e.message || e) })), RUN);
  say('1a the cloud takes a release command with its order id and the person\'s name', put && put.ok && put.cmd && put.cmd.type === 'release' && JSON.stringify(put.cmd.args.ids) === JSON.stringify([RID]) && put.cmd.by === 'Michelle_R' && put.cmd.state === 'waiting', put);
  say('1b an unknown command is still refused', !!(bad && bad.error), bad);

  // 2, 3 ── a following computer
  const follower = await page.evaluate(async ([RUN, RID]) => {
    const realApi = CN.api, realRows = Orders.rows, realState = CharmNestRunFollow.state, realDev = CharmNestRunFollow.dev, realEv = window.SheetEvents && SheetEvents.order, out = {};
    const row = { key: RID + '_1', order: { receiptId: RID }, state: 'held', hold: 'on hold: option "Initial: S" is not mapped', problems: [], releasing: { at: 1, by: 'old', stage: 'queued' } };
    Orders.rows = () => [row]; B.run = null; CharmNestRunFollow.state = () => ({ runId: RUN }); CharmNestRunFollow.dev = () => 'dtestfoll01';
    let events = 0; if (window.SheetEvents) SheetEvents.order = () => { events++; };
    let calls = [], listN = 0, mode = 'ok';
    CN.api = async (fn, body) => { if (/^(runCarrier|runCmdPut|runCmdList)$/.test(body.op)) calls.push(body.op === 'runCmdPut' ? ['runCmdPut', body.cmd.type, body.cmd.ids.join(), body.by] : [body.op]);   // (the page's other upkeep runs on its own beat and is not this release's)
      if (body.op === 'runCarrier') return mode === 'nocarrier' ? { ok: true, alive: false, mine: false, carrier: null } : { ok: true, alive: true, mine: false, carrier: { by: 'Paul_K', dev: 'dcarrier01' } };
      if (body.op === 'runCmdPut') return { ok: true, id: 'cX1' };
      if (body.op === 'runCmdList') { listN++; return { cmds: [listN < 2 ? { id: 'cX1', state: 'running' } : { id: 'cX1', state: mode === 'refused' ? 'failed' : 'done', text: mode === 'refused' ? 'a piece has no design yet' : 'Order released from hold and placed on GF 14/20 · sheet 2' }] }; }
      return {}; };
    try {
      const t0 = Date.now(); const ok = await OrderHold.release(RID, { name: 'Michelle_R' });
      Object.assign(out, { ok: { ok: ok.ok, released: ok.released, via: ok.via, text: ok.text, ms: Date.now() - t0 }, calls: calls.slice(), events, hold: row.hold, releasing: !!row.releasing, state: row.state });
      calls = []; listN = 0; mode = 'refused'; const no = await OrderHold.release(RID, { name: 'Michelle_R' }); out.refused = { ok: no.ok, released: no.released, error: no.error };
      calls = []; mode = 'nocarrier'; const nc = await OrderHold.release(RID, { name: 'Michelle_R' }); out.nocarrier = { ok: nc.ok, queued: nc.queued, released: nc.released, error: nc.error, calls: calls.slice() };
    } finally { CN.api = realApi; Orders.rows = realRows; CharmNestRunFollow.state = realState; CharmNestRunFollow.dev = realDev; if (window.SheetEvents) SheetEvents.order = realEv; }
    return out;
  }, [RUN, RID]);
  say('2a it asks the carrier (and only that): a carrier check, then one release command for this order in the person\'s name', JSON.stringify(follower.calls.slice(0, 2)) === JSON.stringify([['runCarrier'], ['runCmdPut', 'release', RID, 'Michelle_R']]) && follower.calls.slice(2).every(c => c[0] === 'runCmdList'), follower.calls);
  say('2b it answers released through the carrier, once the carrier has', follower.ok.ok === true && follower.ok.released === true && follower.ok.via === 'carrier' && /released/.test(follower.ok.text) && follower.ok.ms >= 2400, follower.ok);
  say('2c nothing was done here: the line is as the cloud has it (still held), no timeline step written, a stale local flag cleared', follower.hold && follower.events === 0 && follower.releasing === false && follower.state === 'held', [follower.hold, follower.events, follower.releasing, follower.state]);
  say('3a a refusal is told in the carrier\'s words, not released', follower.refused.ok === false && follower.refused.released !== true && /a piece has no design yet/.test(follower.refused.error || ''), follower.refused);
  say('3b no carrier answering: the request is still put in the cloud (never refused), and the answer is "queued", not a failure', follower.nocarrier.ok === false && follower.nocarrier.queued === true && !/was not released|cannot be released/.test(follower.nocarrier.error || '') && /asked of the run/.test(follower.nocarrier.error || '') && JSON.stringify(follower.nocarrier.calls) === JSON.stringify([['runCarrier'], ['runCmdPut', 'release', RID, 'Michelle_R']]), follower.nocarrier);

  // 4 ── the computer that carries the run
  const carrier = await page.evaluate(async ([RUN, RID]) => {
    const realApi = CN.api, realRelease = OrderHold.release, realState = CharmNestRunFollow.state, realDev = CharmNestRunFollow.dev, out = { done: null, release: null };
    B.run = { runId: RUN, status: 'running', step: 'nest' }; CharmNestRunFollow.dev = () => 'dcarrier01';
    OrderHold.release = async (rid, o) => { out.release = [rid, o && o.name]; return { ok: true, released: true, placed: true, sheets: [{ label: 'GF 14/20 · sheet 2' }], steps: [] }; };
    let given = false;
    CN.api = async (fn, body) => { if (body.op === 'runCmdList') { if (given) return { cmds: [] }; given = true; return { cmds: [{ id: 'cY1', type: 'release', args: { ids: [RID] }, by: 'Michelle_R', dev: 'dtestfoll01', at: Date.now(), state: 'waiting' }] }; } if (body.op === 'runCmdDone') out.done = { id: body.id, ok: body.ok, text: body.text }; return {}; };
    try { await CharmNestRunFollow.now('test'); for (let i = 0; i < 40 && !out.done; i++) await new Promise(r => setTimeout(r, 250)); } finally { CN.api = realApi; OrderHold.release = realRelease; CharmNestRunFollow.state = realState; CharmNestRunFollow.dev = realDev; B.run = null; }
    return out;
  }, [RUN, RID]);
  say('4a the carrying computer releases the order in the asker\'s name', JSON.stringify(carrier.release) === JSON.stringify([RID, 'Michelle_R']), carrier.release);
  say('4b and answers the command done, with where it went', carrier.done && carrier.done.id === 'cY1' && carrier.done.ok === true && /released from hold/.test(carrier.done.text) && /GF 14\/20/.test(carrier.done.text), carrier.done);

  const errs = (H.errors || []).filter(e => !/firebase stub|favicon|HTTP 400|status of 400/.test(e));
  say('no page error', errs.length === 0, errs.slice(0, 3));
  await H.close?.();
  console.log(fails ? `FAIL follow-release · ${fails} check(s)` : 'PASS follow-release');
  process.exit(fails ? 1 : 0);
})().catch(e => { console.error('ERROR', e && e.stack || e); process.exit(2); });
