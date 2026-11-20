// ANYWHERE (Paul, 19 Nov 2026: "SKU charm was updated ... system refused to place newly updated charm ... This must be allowed to happen from any computer. It doesn't matter. The computers are just nodes that are sending and
// receiving information. The cloud controls everything so nothing belongs to any computer" — the Iguana order 4239219834 left unmatched for hours; Release Hold and "Just this order" refused on Michelle's computer).
//   1 a line's record carries the SKU Etsy sold it under (snap.bought) and a computer that follows the run reads it back (line.etsySku): Change SKU there saves the link under THAT SKU, not "no SKU"
//   2 a record from before has none: Etsy's stored table of the listing (one SKU for every product) says it; a listing with several SKUs is not guessed and not asked again for a minute
//   3 "Just this order" on a following computer is asked of the cloud (runCmdPut skuonce, the row and the charm) and answered; with the carrying computer not answering it is queued, never refused
//   4 the carrying computer does it in the asker's name, and keeps what a person asked for hours (a release, a one-order charm) while any other request is "too old" after ten minutes
//   5 the cloud takes a skuonce command with its charm
//   6 Release Hold asked while the carrier answers but never starts: it is queued after 30 s, not a failure
// Real sorter in Chromium against the repo's fake site; nothing live.   PW_DIR=... CHROMIUM=... node tests/charm-nest/anywhere-requests.cjs
if (process.argv[2]) process.env.PW_DIR = process.argv[2];
const { open } = require('./day-sim/harness.cjs');
let fails = 0;
const say = (name, ok, seen) => { if (!ok) fails++; console.log(`${ok ? 'OK  ' : 'FAIL'} ${name}${seen === undefined ? '' : ' :: ' + (typeof seen === 'string' ? seen : JSON.stringify(seen))}`); };
const RUN = 'run-any-t', LID = '1697972740';

(async () => {
  const H = await open({ masters: [], receipts: [] });
  const page = H.page;
  await page.evaluate(() => { CN.setMode('nest'); });
  await page.waitForFunction(() => window.OrderHold && window.CharmNestRunFollow && window.Orders && window.Review && window.CharmNestReviewLink, null, { timeout: 30000 });

  // (the bridge asks the cloud through the page's own api(): while a test stubs CN.api, api() follows it; the real one is __origApi)
  await page.evaluate(() => { const origApi = window.api; window.__origApi = origApi; CN.api.__orig = true; window.api = (fn, body, opts) => (CN.api && CN.api.__orig ? origApi(fn, body, opts) : CN.api(fn, body, opts)); });

  // 1 ── the record
  const rec = await page.evaluate(([LID]) => {
    const row = { key: '4239219834_5300628403', state: 'unmatched', reason: 'x', poolIds: [], hold: null, wait: null, problems: [{ kind: 'unmatchedSku' }], material: 'silver',
      order: { receiptId: '4239219834', orderNumber: '4239219834', shipBy: 1, createTs: 1, updateTs: 1, buyer: { name: 'K' }, isGift: false, lines: [] },
      line: { transactionId: '5300628403', listingId: LID, sku: '', title: 'Iguana Studs', quantity: 1, metalKey: 'silver', metalLabel: 'Sterling Silver', variations: [], personalization: [] },
      spec: { boughtSku: 'REPTILE 2-IGUANA', designSku: null, quantity: 1, pieceCount: 2 } };
    const r = Orders.lineRecord(row)[1], back = Orders.rowFromRecord(row.key, r), RL = CharmNestReviewLink;
    const old = Object.assign({}, r, { snap: Object.assign({}, r.snap) }); delete old.snap.bought; const backOld = Orders.rowFromRecord(row.key, old);
    return { bought: r.snap.bought, etsySku: back.line.etsySku, from: RL.fromOf(back.line, { boughtSku: '' }), fromOld: RL.fromOf(backOld.line, { boughtSku: '' }), oldHas: typeof backOld.line.etsySku, carrierFrom: RL.fromOf(row.line, row.spec) };
  }, [LID]);
  say('1a the run line\'s record carries the SKU Etsy sold the line under', rec.bought === 'REPTILE 2-IGUANA', rec.bought);
  say('1b a following computer reads it back as the line\'s Etsy SKU, and Change SKU there answers for THAT SKU (not "no SKU")', rec.etsySku === 'REPTILE 2-IGUANA' && rec.from === 'REPTILE 2-IGUANA', [rec.etsySku, rec.from]);
  say('1c a record from before has none (the old answer: the line\'s own SKU), and the carrying computer\'s own reading is unchanged', rec.oldHas === 'undefined' && rec.fromOld === '' && rec.carrierFrom === 'REPTILE 2-IGUANA', [rec.oldHas, rec.fromOld, rec.carrierFrom]);

  // 2 ── Etsy's stored table, for a record from before
  const tbl = await page.evaluate(async ([LID]) => {
    const realApi = CN.api, calls = []; let table = { uni: 'Reptile 2-Iguana' };
    CN.api = async (fn, body) => { if (body.op === 'listingSkus') { calls.push(body); return { tables: { [LID]: table } }; } return __origApi(fn, body); };
    const mk = () => ({ key: 'k' + Math.random(), fromRecord: true, line: { listingId: LID, sku: '' }, order: { receiptId: '1' }, spec: null });
    const out = {};
    try {
      const a = mk(); out.okUni = await Review.ensureBought(a); out.uni = a.line.etsySku; out.cached = await Review.ensureBought(a); out.callsUni = calls.length;
      table = { products: [{ id: 1, sku: 'A' }, { id: 2, sku: 'B' }] }; calls.length = 0;
      const b = mk(); out.okMany = await Review.ensureBought(b); out.many = b.line.etsySku; out.manyAgain = await Review.ensureBought(b); out.callsMany = calls.length;
      const c = { key: 'c', fromRecord: false, line: { listingId: LID, sku: 'X' }, order: {}, spec: { boughtSku: 'X' } }; calls.length = 0; out.carrierRow = await Review.ensureBought(c); out.callsCarrier = calls.length;
    } finally { CN.api = realApi; }
    return out;
  }, [LID]);
  say('2a a following computer reads the one SKU of the listing from Etsy\'s stored table (read only), once', tbl.okUni === true && tbl.uni === 'REPTILE 2-IGUANA' && tbl.cached === true && tbl.callsUni === 1, tbl);
  say('2b a listing with several SKUs is not guessed (no answer) and not asked again within a minute; the carrying computer\'s own rows never ask', tbl.okMany === false && tbl.many === undefined && tbl.manyAgain === false && tbl.callsMany === 1 && tbl.carrierRow === true && tbl.callsCarrier === 0, tbl);

  // 3 ── "Just this order" on a following computer
  const once = await page.evaluate(async ([LID, RUN]) => {
    const realApi = CN.api, realState = CharmNestRunFollow.state, realDev = CharmNestRunFollow.dev, out = {}, calls = [];
    const SKU = 'ANYWHERE_TEST_1';
    B.master.entries.set(SKU, { sku: SKU, widthPt: 20, heightPt: 20 });
    B.run = null; CharmNestRunFollow.state = () => ({ runId: RUN }); CharmNestRunFollow.dev = () => 'dtestfoll02'; const emp = B.employee; B.employee = 'Michelle_R';
    const row = { key: '4237444149_5301', state: 'held', reason: 'x', poolIds: [], hold: null, wait: null, problems: [{ kind: 'unmatchedSku', reason: 'x' }], fromRecord: true, material: 'gold',
      order: { receiptId: '4237444149', orderNumber: '4237444149', shipBy: 1, createTs: 1, updateTs: 1, buyer: { name: 'K' }, isGift: false, lines: [] },
      line: { transactionId: '5301', listingId: LID, sku: '', etsySku: 'CUSTOM-H-060-849928', title: 'Custom', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', variations: [], personalization: [] },
      spec: { boughtSku: '', designSku: '', quantity: 1, pieceCount: 1, problems: [{ kind: 'unmatchedSku' }] } };
    let mode = 'done';
    CN.api = async (fn, body) => {
      if (/^(runCarrier|runCmdPut|runCmdList)$/.test(body.op)) calls.push(body.op === 'runCmdPut' ? ['runCmdPut', body.cmd.type, (body.cmd.ids || []).join(), body.cmd.sku, body.by] : [body.op]);
      if (body.op === 'runCarrier') return mode === 'gone' ? { ok: true, alive: false, mine: false, carrier: { by: '', dev: 'dcarrier01' } } : { ok: true, alive: true, mine: false, carrier: { by: 'Paul_K', dev: 'dcarrier01' } };
      if (body.op === 'runCmdPut') return { ok: true, id: 'cS1' };
      if (body.op === 'runCmdList') return { cmds: [{ id: 'cS1', state: mode === 'gone' ? 'waiting' : 'done', text: SKU + ' for order 4237444149 only' }] };
      return __origApi(fn, body);
    };
    try {
      const a = await Review.editSkuOnce(row, SKU); out.done = { ok: a && a.ok, queued: a && a.queued, why: a && a.why, via: a && a.via, calls: calls.slice() };
      calls.length = 0; mode = 'gone'; const b = await Review.editSkuOnce(row, SKU); out.gone = { ok: b && b.ok, queued: b && b.queued, text: b && b.text, why: b && b.why, calls: calls.slice(0, 2) };
    } finally { CN.api = realApi; CharmNestRunFollow.state = realState; CharmNestRunFollow.dev = realDev; B.employee = emp; B.master.entries.delete(SKU); }
    return out;
  }, [LID, RUN]);
  say('3a "Just this order" on a following computer is put in the cloud (the row and the charm, in the person\'s name) and answered done by the carrying computer', once.done.ok === true && !once.done.why && once.done.calls[0][0] === 'runCarrier' && JSON.stringify(once.done.calls[1].slice(0, 4)) === JSON.stringify(['runCmdPut', 'skuonce', '4237444149_5301', 'ANYWHERE_TEST_1']) && !!once.done.calls[1][4], once.done);
  say('3b with the carrying computer not answering it is still put in the cloud and is "queued" (ok), never refused', once.gone.ok === true && once.gone.queued === true && !once.gone.why && JSON.stringify(once.gone.calls[1].slice(0, 4)) === JSON.stringify(['runCmdPut', 'skuonce', '4237444149_5301', 'ANYWHERE_TEST_1']) && /queued/.test(once.gone.text || ''), once.gone);

  // 4 ── the carrying computer
  const carrier = await page.evaluate(async ([RUN]) => {
    const realApi = CN.api, realDev = CharmNestRunFollow.dev, realEdit = Review.editSkuOnce, realRows = Orders.rows, out = { once: null, done: {} };
    B.run = { runId: RUN, status: 'running', step: 'nest' }; CharmNestRunFollow.dev = () => 'dcarrier01';
    const row = { key: '4237444149_5301', order: { receiptId: '4237444149' } };
    Orders.rows = () => [row];
    Review.editSkuOnce = async (r, sku, o) => { out.once = [r.key, sku, o && o.by]; return { ok: true, sku }; };
    const old = Date.now() - 30 * 60000;
    const cmds = [
      { id: 'cA', type: 'skuonce', args: { ids: ['4237444149_5301'], sku: 'ANYWHERE_TEST_1' }, by: 'Michelle_R', dev: 'dfoll', at: old, state: 'waiting' },
      { id: 'cB', type: 'keepsync', args: { ids: ['X'] }, by: 'Michelle_R', dev: 'dfoll', at: old, state: 'waiting' }
    ];
    CN.api = async (fn, body) => { if (body.op === 'runCmdList') return { cmds: cmds.filter(c => c.state === 'waiting') }; if (body.op === 'runCmdDone') { out.done[body.id] = { ok: body.ok, text: body.text }; const c = cmds.find(x => x.id === body.id); if (c) c.state = body.ok ? 'done' : 'failed'; return { ok: true }; } if (body.op === 'runCarrier') return { ok: true, mine: true, alive: true, carrier: { dev: 'dcarrier01', by: 'Paul_K' } }; if (/^runCmd/.test(body.op)) return { ok: true }; return __origApi(fn, body); };
    try { await CharmNestRunFollow.now('test'); for (let i = 0; i < 60 && Object.keys(out.done).length < 2; i++) await new Promise(r => setTimeout(r, 250)); } finally { CN.api = realApi; CharmNestRunFollow.dev = realDev; Review.editSkuOnce = realEdit; Orders.rows = realRows; B.run = null; }
    return out;
  }, [RUN]);
  say('4a the carrying computer does "Just this order" in the asker\'s name, half an hour after it was asked', JSON.stringify(carrier.once) === JSON.stringify(['4237444149_5301', 'ANYWHERE_TEST_1', 'Michelle_R']) && carrier.done.cA && carrier.done.cA.ok === true, [carrier.once, carrier.done.cA]);
  say('4b any other request that old is still "too old"', carrier.done.cB && carrier.done.cB.ok === false && /Not done: asked \d+ min ago/.test(carrier.done.cB.text), carrier.done.cB);

  // 4c ── a line of the open run that was taken in from the record (a following computer added it) is the carrier's own line
  const offer = await page.evaluate(([RUN]) => {
    const mk = key => ({ key, state: 'unmatched', fromRecord: true, problems: [{ kind: 'unmatchedSku' }], order: { receiptId: '4237444149' }, line: { listingId: '4620198805', sku: 'CUSTOM-H-060-849928', etsySku: 'CUSTOM-H-060-849928', title: 'Custom' }, spec: { boughtSku: 'CUSTOM-H-060-849928', designSku: 'CUSTOM-H-060-849928', quantity: 1 } });
    const inRun = mk('4237444149_5298095720'), other = mk('4237444149_ELSEWHERE');
    const realState = CharmNestRunFollow.state; CharmNestRunFollow.state = () => ({ runId: null });
    B.run = { runId: RUN, status: 'running', lines: { '4237444149_5298095720': {} } };
    const out = {};
    try { out.inRun = Review.editSkuOffer(inRun); out.other = Review.editSkuOffer(other); } finally { B.run = null; CharmNestRunFollow.state = realState; }
    return { inRun: { ok: out.inRun.ok, why: out.inRun.why }, other: { ok: out.other.ok, why: out.other.why } };
  }, [RUN]);
  say('4c on the carrying computer a line of the open run read from the record is offered "Change SKU / Just this order"; an order of another run read from the records is still not', offer.inRun.ok === true && offer.other.ok === false && /read from the records/.test(offer.other.why || ''), offer);

  // 5 ── the cloud
  const put = await page.evaluate(([RUN]) => CN.api('charmNestLibrary', { op: 'runCmdPut', runId: RUN, dev: 'dtestfoll02', by: 'Michelle_R', cmd: { type: 'skuonce', ids: ['4237444149_5301'], sku: 'basketball (huggie)' } }).then(r => r, e => ({ error: String(e.message || e) })), [RUN]);
  say('5 the cloud takes a skuonce command with the row and the charm (upper case)', put && put.ok && put.cmd.type === 'skuonce' && put.cmd.args.sku === 'BASKETBALL (HUGGIE)' && JSON.stringify(put.cmd.args.ids) === JSON.stringify(['4237444149_5301']), put);

  // 6 ── a carrier that answers but never starts
  const stuck = await page.evaluate(async ([RUN]) => {
    const realApi = CN.api, realState = CharmNestRunFollow.state, realDev = CharmNestRunFollow.dev, out = {}; let checks = 0;
    B.run = null; CharmNestRunFollow.state = () => ({ runId: RUN }); CharmNestRunFollow.dev = () => 'dtestfoll02';
    const row = { key: '4999000002_1', order: { receiptId: '4999000002' }, state: 'held', hold: 'x', problems: [] };
    const realRows = Orders.rows; Orders.rows = () => [row];
    CN.api = async (fn, body) => {
      if (body.op === 'runCarrier') { checks++; return checks <= 1 ? { ok: true, alive: true, mine: false, carrier: { by: 'Paul_K', dev: 'dcarrier01' } } : { ok: true, alive: false, mine: false, carrier: { by: 'Paul_K', dev: 'dcarrier01' } }; }
      if (body.op === 'runCmdPut') return { ok: true, id: 'cW1' };
      if (body.op === 'runCmdList') return { cmds: [{ id: 'cW1', state: 'waiting' }] };
      return __origApi(fn, body);
    };
    try { const t0 = Date.now(); const r = await OrderHold.release('4999000002', { name: 'Michelle_R' }); out.r = { ok: r.ok, queued: r.queued, released: r.released, error: r.error, ms: Date.now() - t0 }; }
    finally { CN.api = realApi; CharmNestRunFollow.state = realState; CharmNestRunFollow.dev = realDev; Orders.rows = realRows; }
    return out;
  }, [RUN]);
  say('6 asked while the carrier answers, then it stops answering before it starts: after about 30 s it is "queued" (kept in the cloud), not a failure', stuck.r.queued === true && stuck.r.released !== true && !/was not released/.test(stuck.r.error || '') && /stopped answering/.test(stuck.r.error || '') && stuck.r.ms >= 29000 && stuck.r.ms < 60000, stuck.r);

  // 7 ── a release that takes longer than this page used to wait (RELWAIT: order 4237522021, 158 s against a wait of 150 s)
  const long = await page.evaluate(async ([RUN]) => {
    const realApi = CN.api, realState = CharmNestRunFollow.state, realDev = CharmNestRunFollow.dev, realRows = Orders.rows, out = { steps: [] }; let n = 0;
    B.run = null; CharmNestRunFollow.state = () => ({ runId: RUN }); CharmNestRunFollow.dev = () => 'dtestfoll02';
    const row = { key: '4999000003_1', order: { receiptId: '4999000003' }, state: 'held', hold: 'x', problems: [] }; Orders.rows = () => [row];
    OrderHold.relayPollMs = 200; OrderHold.relayNoticeMs = 1500; OrderHold.relayLimitMs = 20000;
    CN.api = async (fn, body) => {
      if (body.op === 'runCarrier') return { ok: true, alive: true, mine: false, carrier: { by: '', dev: 'dcarrier01' } };
      if (body.op === 'runCmdPut') return { ok: true, id: 'cL1' };
      if (body.op === 'runCmdList') { n++; const at = n * 200; return { cmds: [{ id: 'cL1', state: at < 800 ? 'waiting' : at < 3600 ? 'running' : 'done', step: at < 2000 ? 'Its place at the front is saved · 1 s' : 'Placing on GF Draft 2… · 4 s', text: 'Order 4999000003 released from hold and placed on GF Draft 2' }] }; }
      return __origApi(fn, body);
    };
    try { const t0 = Date.now(); const r = await OrderHold.release('4999000003', { name: 'Michelle_R', onStep: s => out.steps.push(s.text || s.type) }); out.r = { ok: r.ok, released: r.released, queued: r.queued, error: r.error, ms: Date.now() - t0 }; }
    finally { CN.api = realApi; CharmNestRunFollow.state = realState; CharmNestRunFollow.dev = realDev; Orders.rows = realRows; delete OrderHold.relayPollMs; delete OrderHold.relayNoticeMs; delete OrderHold.relayLimitMs; }
    return out;
  }, [RUN]);
  say('7 a release that takes a long time is waited for to its end: released (not queued, not a failure), with what the run\'s computer is doing said on the way', long.r.ok === true && long.r.released === true && !long.r.queued && long.r.ms >= 3000 && long.steps.some(s => /Its place at the front is saved/.test(s)) && long.steps.some(s => /Placing on GF Draft 2/.test(s)) && long.steps.some(s => /busy/.test(s)), long);

  // 8, 9 ── RVSAME: the run's decisions reach a following computer; an unmatched line is read under its Etsy SKU
  const same = await page.evaluate(async ([RUN]) => {
    const realApi = CN.api, realRows = Orders.rows, realInterp = Orders.interpretAll, realState = CharmNestRunFollow.state, out = {}; let reads = 0;
    const rec = { state: 'pooled', poolIds: ['4237444149_5298095720_1'], reason: null, hold: null, wait: null, sku: 'BASKETBALL (HUGGIE)', material: 'gold', quantity: 1, orderId: '4237444149', transactionId: '5298095720', problems: [],
      skuOverride: { sku: 'BASKETBALL (HUGGIE)', by: 'Claude', at: 1, was: 'CUSTOM-H-060-849928' }, snap: { title: 'Custom huggie', listingId: '4620198805', bought: 'CUSTOM-H-060-849928', vars: [], pers: [] } };
    const row = Orders.rowFromRecord('4237444149_5298095720', Object.assign({}, rec, { skuOverride: undefined, state: 'unmatched', poolIds: [] }));
    row.spec = { problems: [{ kind: 'unmatchedSku' }] };
    Orders.rows = () => [row]; B.run = null; Orders.interpretAll = () => { reads++; };
    CN.api = async (fn, body) => { if (body.op === 'runList') return { runs: [{ runId: RUN, status: 'running' }] }; if (body.op === 'runGet') return { run: { runId: RUN, status: 'running', lines: { '4237444149_5298095720': rec }, orders: ['4237444149'] } }; return __origApi(fn, body); };
    try { for (let i = 0; i < 40 && !(row.skuOverride && row.skuOverride.sku); i++) { await CharmNestRunFollow.now('test'); if (!(row.skuOverride && row.skuOverride.sku)) await new Promise(r => setTimeout(r, 250)); } out.ov = row.skuOverride && row.skuOverride.sku; out.state = row.state; out.specReset = row.spec === null; out.reads = reads; }   // (the follower's own read may be under way: asked again until it has run)
    finally { CN.api = realApi; Orders.rows = realRows; Orders.interpretAll = realInterp; }
    const un = Orders.rowFromRecord('4239217080_5300625029', { state: 'unmatched', sku: null, orderId: '4239217080', transactionId: '5300625029', problems: ['unmatchedSku'], snap: { title: 'Capybara', listingId: '1', bought: 'CAPYBARA1', vars: [], pers: [] } });
    out.unSku = un.line.sku; out.unFrom = CharmNestReviewLink.fromOf(un.line, null);
    const matched = Orders.rowFromRecord('x_1', { state: 'pooled', sku: 'MOON_3288', orderId: 'x', transactionId: '1', snap: { title: 'Moon', listingId: '2', bought: 'MOON', vars: [], pers: [] } });
    out.matchedSku = matched.line.sku;
    return out;
  }, [RUN]);
  say('8 a decision made on the computer that carries the run ("Just this order") reaches a following computer: the line takes it and is read again', same.ov === 'BASKETBALL (HUGGIE)' && same.state === 'pooled' && same.specReset === true && same.reads >= 1, same);
  say('9 a line the run has not matched is read on a following computer under the SKU Etsy sold it under (so it asks what the run asks); a matched line keeps its design', same.unSku === 'CAPYBARA1' && same.unFrom === 'CAPYBARA1' && same.matchedSku === 'MOON_3288', same);

  // 10 ── RVSAME: a following computer uses the run's reading of a line (Custom Orders or not), never one of its own, and never asks Claude
  const rd = await page.evaluate(async () => {
    const realApi = CN.api, realRows = Orders.rows, out = { asked: [], agent: 0 };
    const mk = (key, crh) => { const r = Orders.rowFromRecord(key, { state: 'unmatched', sku: null, orderId: key.split('_')[0], transactionId: key.split('_')[1], problems: ['unmatchedSku'], crh, snap: { title: 'x', listingId: '1', bought: 'X', vars: [], pers: [] } }); r.spec = { readable: true, problems: [{ kind: 'unmatchedSku' }], special: null }; r.problems = [{ kind: 'unmatchedSku' }]; return r; };
    const a = mk('4999000010_1', 'h-run-a'), b = mk('4999000011_1', null);
    B.maps.customRead['4999000010_1'] = { hash: 'h-mine-a', kind: 'regular', at: Date.now() };   // (this computer's own older reading)
    B.maps.customRead['4999000011_1'] = { hash: 'h-mine-b', kind: 'custom', at: Date.now() };   // (a reading the run does not have)
    B.run = null; Orders.rows = () => [a, b];
    CN.api = async (fn, body) => { if (body.op === 'customReadGet') { out.asked.push(...body.items.map(i => i.key + '|' + i.hash)); return { reads: { '4999000010_1': { hash: 'h-run-a', kind: 'custom', confidence: 0.9, at: 1 } }, decided: {} }; } if (body.op === 'startAgent' || body.op === 'getAgent') { out.agent++; return {}; } return __origApi(fn, body); };
    try { await CustomRead.sync(); out.a = (B.maps.customRead['4999000010_1'] || {}).hash; out.b = B.maps.customRead['4999000011_1'] || null; }
    finally { CN.api = realApi; Orders.rows = realRows; delete B.maps.customRead['4999000010_1']; delete B.maps.customRead['4999000011_1']; }
    return out;
  });
  say('10 a following computer takes the run\'s own reading of each line (its own older one replaced, one the run does not have dropped) and never asks Claude itself', JSON.stringify(rd.asked) === JSON.stringify(['4999000010_1|h-run-a']) && rd.a === 'h-run-a' && rd.b === null && rd.agent === 0, rd);

  const errs = (H.errors || []).filter(e => !/firebase stub|favicon|HTTP 400|status of 400/.test(e));
  say('no page error', errs.length === 0, errs.slice(0, 3));
  await H.close?.();
  console.log(fails ? `FAIL anywhere-requests · ${fails} check(s)` : 'PASS anywhere-requests');
  process.exit(fails ? 1 : 0);
})().catch(e => { console.error('ERROR', e && e.stack || e); process.exit(2); });
