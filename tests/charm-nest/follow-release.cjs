// FOLLOWRELEASE (Paul, 18 Nov 2026: "this order was released by a user and then it disappeared for several minutes and then it reappeared again under the review tab ... you must fix this"; order 4235861617, released by Michelle_R on a
// computer that only FOLLOWS the run: the timeline said "released", the run's line in the cloud stayed "held", and the next read put it back under Review).
// (FOLLOWRELEASE, 18 Nov) A release pressed on a following computer was done by the computer that carries the run (runCmdPut release); RELEASEHOLD (20 Nov) does it in the cloud at once instead. The carrying computer still takes up a release command left in the run's list (4).
//   1 the cloud takes a release command (its order id kept) and still refuses an unknown one
//   2 a FOLLOWING computer releases IN THE CLOUD at once (RELEASEHOLD, Paul 20 Nov 2026, charm-nest-order-release.js askCloud/doRelease: op releaseAsk; the relay to the carrier and its wait are gone):
//     it asks the cloud only (no carrier check, no run command), answers released via the cloud at once, places nothing itself and writes no timeline step (the cloud does), the line is as the cloud has it
//   3 a refusal by the cloud is told in the cloud's words, not released;  no carrier answering: nothing waits on it, the release is done in the cloud all the same (never "queued", never refused)
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

  // 2, 3 ── a following computer (RELEASEHOLD: the release is made in the cloud at once, op releaseAsk; never relayed to the carrier)
  const follower = await page.evaluate(async ([RUN, RID]) => {
    const realApi = CN.api, realRows = Orders.rows, realState = CharmNestRunFollow.state, realDev = CharmNestRunFollow.dev, realEv = window.SheetEvents && SheetEvents.order, out = {};
    const row = { key: RID + '_1', order: { receiptId: RID }, state: 'held', hold: 'on hold: option "Initial: S" is not mapped', problems: [], releasing: { at: 1, by: 'old', stage: 'queued' } };
    const fresh = () => Object.assign(row, { state: 'held', hold: 'on hold: option "Initial: S" is not mapped', frontAt: undefined, releasing: { at: 1, by: 'old', stage: 'queued' } });
    Orders.rows = () => [row]; B.run = null; CharmNestRunFollow.state = () => ({ runId: RUN }); CharmNestRunFollow.dev = () => 'dtestfoll01';
    let events = 0; if (window.SheetEvents) SheetEvents.order = () => { events++; };
    let calls = [], mode = 'ok'; const F = Date.now();
    CN.api = async (fn, body) => { if (/^(runCarrier|runCmdPut|runCmdList|releaseAsk)$/.test(body.op)) calls.push(body.op === 'releaseAsk' ? ['releaseAsk', body.orderId, body.by, body.runId, (body.keys || []).join()] : [body.op]);   // (the page's other upkeep runs on its own beat and is not this release's)
      if (body.op === 'runCarrier') return mode === 'nocarrier' ? { ok: true, alive: false, mine: false, carrier: null } : { ok: true, alive: true, mine: false, carrier: { by: 'Paul_K', dev: 'dcarrier01' } };
      if (body.op === 'runCmdPut') return { ok: true, id: 'cX1' };
      if (body.op === 'runCmdList') return { cmds: [] };
      if (body.op === 'releaseAsk') {
        if (mode === 'refused') throw Object.assign(new Error('The hold was not released in the cloud: a piece has no design yet. Press Release hold again.'), { status: 500 });
        return { ok: true, orderId: RID, frontAt: F, at: F, stamp: { at: F, dev: 'cloud', by: body.by }, runId: RUN, lines: { [RID + '_1']: { state: 'pulled', hold: null, heldAt: null, reason: null, frontAt: F, releasedAt: F, _lp: { at: F, dev: 'cloud' } } }, released: 1 };
      }
      return {}; };
    try {
      const t0 = Date.now(); const ok = await OrderHold.release(RID, { name: 'Michelle_R' });
      Object.assign(out, { ok: { ok: ok.ok, released: ok.released, via: ok.via, placed: ok.placed, text: ok.text, ms: Date.now() - t0 }, calls: calls.slice(), events, hold: row.hold, releasing: !!row.releasing, state: row.state, frontAt: row.frontAt, F });
      calls = []; mode = 'refused'; fresh(); const no = await OrderHold.release(RID, { name: 'Michelle_R' }); out.refused = { ok: no.ok, released: no.released, error: no.error, hold: row.hold };
      calls = []; mode = 'nocarrier'; fresh(); const t1 = Date.now(); const nc = await OrderHold.release(RID, { name: 'Michelle_R' }); out.nocarrier = { ok: nc.ok, queued: nc.queued, released: nc.released, via: nc.via, error: nc.error, calls: calls.slice(), ms: Date.now() - t1 };
    } finally { CN.api = realApi; Orders.rows = realRows; CharmNestRunFollow.state = realState; CharmNestRunFollow.dev = realDev; if (window.SheetEvents) SheetEvents.order = realEv; }
    return out;
  }, [RUN, RID]);
  say('2a it asks the cloud (and only that): one releaseAsk for this order in the person\'s name and the run it follows; no carrier check, no run command', JSON.stringify(follower.calls) === JSON.stringify([['releaseAsk', RID, 'Michelle_R', RUN, RID + '_1']]), follower.calls);
  say('2b it answers released through the cloud at once (no wait on another computer), placing nothing itself', follower.ok.ok === true && follower.ok.released === true && follower.ok.via === 'cloud' && follower.ok.placed === false && /released in the cloud/.test(follower.ok.text || '') && follower.ok.ms < 2400, follower.ok);
  say('2c nothing was placed here: the line is as the cloud has it (released, at the cloud\'s front time), no timeline step written by this page, a stale local flag cleared', !follower.hold && follower.state === 'pulled' && follower.frontAt === follower.F && follower.events === 0 && follower.releasing === false, [follower.hold, follower.state, follower.frontAt === follower.F, follower.events, follower.releasing]);
  say('3a a refusal is told in the cloud\'s words, not released (the line stays held)', follower.refused.ok === false && follower.refused.released !== true && /a piece has no design yet/.test(follower.refused.error || '') && !!follower.refused.hold, follower.refused);
  say('3b no carrier answering: nothing waits on it; the release is made in the cloud at once (not "queued", never refused)', follower.nocarrier.ok === true && follower.nocarrier.released === true && follower.nocarrier.via === 'cloud' && !follower.nocarrier.queued && !follower.nocarrier.error && follower.nocarrier.ms < 2400 && JSON.stringify(follower.nocarrier.calls) === JSON.stringify([['releaseAsk', RID, 'Michelle_R', RUN, RID + '_1']]), follower.nocarrier);

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
