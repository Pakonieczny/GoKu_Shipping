// THE CLOUD READS AN ORDER'S ENGRAVING WORDS (cloud plan v2, build step 11, C3 / E5), offline: the classify unit (netlify/functions/_charmEngraveClassify.js, lane "engrave", order 80)
// runs the page's own reading (charm-nest-engrave-classify.js) over the line's records, against the in-memory cloud of _cloudFakes.cjs, with the paid reader stubbed
// (_charmNestAgent.run gives a fixed engraveIntent answer and counts its calls). Specs are made by the real intake (charm-nest-orders.js interpretLine); every name is made up.
//   A  a name the order's option puts on the FRONT ("Engraving: Front", "Ginger"): read by the rules (the faces' setup), ready for its fit, 0 paid readings
//   B  coordinates with asterisks and a note to the shop: the rules clean them (° for *, the note out), the words wait for a person, 0 paid readings
//   C  a name with a buyer message beside it (the rules stand aside, as on the page): exactly ONE engraveIntentStart reading; the answer is taken up at the next pass by its id
//   D  a pooled line with nothing written: plain on the run line, no job
//   E  BARFRONT: a bar necklace whose words were read on its BACK before the bar rule (one undecided back, "Ginger"): moved to its front once (the page's own
//      charm-nest-engrave-bar-front.js), the back gone, one timeline line, no paid reading
//   F  BARFRONT stays: a bar necklace whose back a person approved keeps that back alone: no front is made, nothing is written
//   G  pieces changed: a line read as ONE job, now two pieces (quantity 2): the job is handed to each piece (#C1, #C2) with its words, no paid reading (migrateToPieces)
//   H  ENGPRE: a line whose pieces are not made yet (held): its job is made on the pool id its piece WILL get (pre), its clear words read for free, NOTHING written to the
//      order's timeline and nothing to the pool; once pooled, the same job, nothing read or paid for again
//   I  EDITOR WORDS: a line whose words would wait for a person, but whose back the Charm Editor drew (its artwork edit adds parts): it goes to Review with no words
//   ENGNOTE (part 3, Paul 20 Nov 2026 "Yes, use ENG: notes"; the shape of order 4235295893, made-up words and names): a staff message "ENG: Happy Birthday Pretty" on an order whose
//      calendar charm says "CHARM + Engraving" and waits for a person with the buyer's note as its words: the calendar takes the note's words, ready, 0 paid readings, its reason names the
//      writer; the decorative charm keeps waiting; a plain line and a line a person cut plain are untouched; an order with two such lines gets the words only as a suggestion on each (and a
//      BLANK line nothing); a newer note ("eng : Happy Birthday Mom") replaces the older one; another pass writes nothing; and a note never jumps: on an order whose calendar a person APPROVED with
//      "Happy Birthday Pretty", the newer "Mom" note leaves the calendar as it is (its card's notice says the newer note differs) and is NOT given to the decorative charm
//   DRIVER (part 2, a run of its own): the real driver's tick and pass (charmNestDrive-background) run the unit: the world moving starts the pass that starts the reading; a reading
//      waiting 30 s starts no pass; waiting a minute starts one ("an engraving reading waits"), which takes the answer; then nothing waits
//   and: each record is written once (passes again, and a pass that forgot its cursor, change nothing and pay for nothing); the payload the reader got carries only the words' fields;
//   engraveIntentStart is refused on a page's request and startAgent on the cloud's; Etsy is called 0 times, no other address is fetched, the Etsy switch is never written.
//   NODE_PATH=<deps> node tests/charm-nest/cloud-engrave-classify.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const path = require('path');
const T0 = Date.UTC(2026, 10, 20, 15, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
const root = path.join(__dirname, '../..');
const out = (...a) => console.log(...a);
let failed = 0;
const ok = (name, cond, detail) => { out((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 1200))); if (!cond) failed++; };
process.on('unhandledRejection', e => { out('FAIL an unhandled rejection: ' + ((e && e.stack) || e)); failed++; });

/* the paid reader, stubbed and counted (charmEngrave-background calls agent.run at call time) */
const Agent = F.load('_charmNestAgent.js'), asked = [];
Agent.run = async (mode, payload) => { asked.push({ mode, payload: JSON.parse(JSON.stringify(payload)) }); return { engrave: true, text: 'Ginger', source: 'personalization', sourceQuote: 'Ginger', requests: { side: 'back', font: null, handwriting: false, image: false }, questions: [], confidence: 0.96, reasoning: 'the name; the message asks for a heart, not words', ignored: [] }; };
const lib = F.load('charmNestLibrary.js'), spy = F.spy(lib);
const Standby = F.load('_charmNestStandby.js');
const Unit = F.load('_charmEngraveClassify.js');
const Steps = F.load('_charmNestSteps.js');
const O = require(path.join(root, 'charm-nest-orders.js'));

/* ── the shop: one order, four lines ── */
const V = (name, value) => ({ name, value });
const SKU = 'HOCKEY 1- STICKS', RID = '4300000001', runId = 'cls-20261120-1';
const entry = { sku: SKU, members: 2, holes: 1, backKeepOut: [], widthPt: 30, heightPt: 34 };
const ictx = { optionMaps: {}, aliases: {}, noDesign: {}, listingSkus: {}, masterEntry: () => entry, masterLoose: () => '' };
const order = msg => ({ receiptId: RID, updateTs: 0, createTs: 0, buyerMessage: msg || '', staffNote: '', messages: [] });
const BAR = 'TINY BAR 7427', BART = 'Tiny Bar Necklace, Personalized Gold Bar Necklace';
const COORDS = `33*0'45" N 96*45'53" W (I don't have the degree sign on my keyboard)`;
const LINES = {
  A: { tid: '43000000011', pers: ['Ginger'], vars: [V('Metal Choice', 'Gold Filled'), V('Engraving', 'Front')] },
  B: { tid: '43000000012', pers: [COORDS], vars: [V('Metal Choice', 'Gold Filled')] },
  C: { tid: '43000000013', pers: ['Ginger'], vars: [V('Metal Choice', 'Gold Filled')], msg: 'Also could you add a little heart after the name if possible?' },
  D: { tid: '43000000014', pers: [], vars: [V('Metal Choice', 'Gold Filled')] },
  E: { tid: '43000000015', sku: BAR, title: BART, pers: ['Ginger'], vars: [V('Metal Choice', 'Gold Filled')] },
  F: { tid: '43000000016', sku: BAR, title: BART, pers: ['Max'], vars: [V('Metal Choice', 'Gold Filled')] },
  G: { tid: '43000000017', qty: 2, pers: ['Ginger'], vars: [V('Metal Choice', 'Gold Filled'), V('Engraving', 'Back')] },
  H: { tid: '43000000018', state: 'held', pers: ['Mila'], vars: [V('Metal Choice', 'Gold Filled')] },
  I: { tid: '43000000019', pers: [COORDS], vars: [V('Metal Choice', 'Gold Filled')] }
};
const keyOf = n => RID + '_' + LINES[n].tid, poolOf = (n, c) => keyOf(n) + '_' + (c || 1);
const runLines = {}, specs = {};
for (const [n, d] of Object.entries(LINES)) {
  const o = order(d.msg), l = { transactionId: d.tid, listingId: '1718099', sku: d.sku || SKU, title: d.title || 'Hockey stick necklace', quantity: d.qty || 1, metalKey: 'gold', metalLabel: 'Gold Filled', personalization: d.pers, variations: d.vars };
  const spec = specs[n] = O.interpretLine(o, l, ictx), key = keyOf(n), pooled = (d.state || 'pooled') === 'pooled', ids = pooled ? Array.from({ length: d.qty || 1 }, (_, i) => poolOf(n, i + 1)) : [];
  runLines[key] = { state: d.state || 'pooled', poolIds: ids, sku: d.sku || SKU, quantity: d.qty || 1, engraveCandidate: !!spec.engraveCandidate, orderId: RID, transactionId: d.tid };
  F.seed('Charm_World_Lines/' + key, { v: 1, key, receiptId: RID, state: runLines[key].state, runId, sig: 's-' + n, at: T0 - 60000, gone: false,
    data: JSON.stringify({ v: 1, key, receiptId: RID, order: o, line: l, spec, problems: [], state: runLines[key].state, run: runLines[key], runId }) });
  ids.forEach((id, i) => F.seed('Charm_Pool/' + id, { poolId: id, lineKey: key, orderId: RID, transactionId: d.tid, sku: d.sku || SKU, state: 'pooled', runId, material: 'gold', copy: i + 1 }));
}
for (const sku of [SKU, BAR]) F.seed('Charm_Master_Index/' + sku, { sku, aiPath: 'charmnest/master/' + sku + '.ai', widthPt: 30, heightPt: 34, engravable: true });
/* the records a computer saved before this run went to the cloud (charm-nest-engrave-sync.js lineRec shape) */
const rec0 = (n, jobs, engrave) => F.seed('Charm_Nest_Engrave/' + keyOf(n), { key: keyOf(n), order: RID, rec: JSON.stringify({ v: 1, override: null, engrave, raised: true, jobs }), at: T0 - 600000, rev: 3, by: 'Ann', dev: 'd1aaaa' });
rec0('E', [{ key: keyOf('E'), state: 'words', reason: 'Check the requested inscription', text: 'Ginger', lines: ['Ginger'], source: 'personalization', quote: 'Ginger', confidence: 0, questions: ['Check the requested inscription'], requests: { side: 'back', font: null, handwriting: false, image: false }, lineGap: .216 }],
  { needed: true, state: 'words', text: 'Ginger', approved: false });
rec0('F', [{ key: keyOf('F'), state: 'approved', text: 'Max', lines: ['Max'], source: 'personalization', approvedBy: 'Ann', approvedAt: T0 - 300000, engravingSeals: [{ id: 'engraveApproved:' + (T0 - 300000) + ':Ann', how: 'engraveApproved', at: T0 - 300000, by: 'Ann' }], lineGap: .216 }],
  { needed: true, state: 'approved', text: 'Max', approved: true, approvedBy: 'Ann', approvedAt: T0 - 300000 });
rec0('G', [{ key: keyOf('G'), state: 'ready', text: 'Ginger', lines: ['Ginger'], source: 'personalization', quote: 'Ginger', confidence: 1, questions: [], requests: { side: 'back', font: null, handwriting: false, image: false }, wordsSource: 'spec.engraving', lineGap: .216 }],
  { needed: true, state: 'ready', text: 'Ginger', approved: false });
for (const n of ['E', 'F', 'G']) runLines[keyOf(n)].engrave = JSON.parse(F.doc('Charm_Nest_Engrave/' + keyOf(n)).rec).engrave;   // (the run line's copy of the summary, as the page saved it)
/* I: the Charm Editor added a part to its back (Charm_Pool_Art, op artPut's record) */
F.seed('Charm_Pool_Art/' + poolOf('I'), { poolId: poolOf('I'), face: 'back', added: [{ id: 'a1', kind: 'path', role: 'line', d: 'M2 2 L8 2' }], removed: [], changed: {}, by: 'Ann', at: T0 - 120000 });
/* every timeline event the cloud sends */
const tlSent = []; { const t0 = lib.ops.timelineAdd; lib.ops.timelineAdd = b => { for (const e of [].concat(b.events || [])) tlSent.push(JSON.parse(JSON.stringify(e))); return t0(b); }; }
F.seed('Charm_Nest_Open/drive', { mode: 'standby', runMode: 'auto', cloudSteps: Standby.STANDBY_STEPS.concat(['engrave']), at: T0 - 3600e3 });
F.seed('Charm_Nest_Runs/' + runId, { runId, status: 'running', step: 'engrave', day: '2026-11-20', lines: JSON.parse(JSON.stringify(runLines)) });
F.seed('Charm_Nest_Drive/lease', { nonce: 'pass-1', state: 'running', at: T0 });
F.seed('Charm_Nest_Run_Carrier/' + runId, { runId, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 } });
lib._driveForget();
const fence = { path: 'Charm_Nest_Drive/lease', nonce: 'pass-1', runId };
const said = [];
/** A pass's ctx, as the driver gives it (_charmNestDrive.js pass items): the run as runGet reads it (its lines merged), the cursor the last pass returned, the fenced cloud call. */
const ctxOf = async cursor => { const g = await lib.call({ op: 'runGet', runId }, {}); return { db: F.db, run: g.run, runId, step: 'pass:engrave-classify', cursor: cursor || {},
  call: (b, u) => lib.call(b, { unit: u, fence }), read: b => lib.call(b, {}), say: s => said.push(s), live: () => {}, budgetEnd: Date.now() + 40000, pass: true }; };
const recOf = n => { const d = F.doc('Charm_Nest_Engrave/' + keyOf(n)); return d ? Object.assign({ rev: d.rev }, JSON.parse(d.rec)) : null; };
const jobOf = n => { const r = recOf(n); return r && r.jobs && r.jobs[0]; };
const jobsOfRec = n => ((recOf(n) || {}).jobs || []);
const intentCalls = () => spy.ops.filter(o => o.op === 'engraveIntentStart').length;
const runLine = async n => { const g = await lib.call({ op: 'runGet', runId }, {}); return ((g && g.run && g.run.lines) || {})[keyOf(n)] || {}; };   // (the run's lines as every reader reads them: runGet merges Run_Live)
const engraveSnap = () => JSON.stringify(F.list('Charm_Nest_Engrave').map(d => [d._id, d.rev, d.rec]).sort());

(async () => {
  ok('wiring · the unit is a pass item of the lane "engrave", order 80, registered in _charmNestSteps (before the fit at 81)', Steps.passItems.some(p => p.name === 'engrave-classify' && p.lane === 'engrave' && p.order === 80)
    && Steps.passItems.findIndex(p => p.name === 'engrave-classify') < Steps.passItems.findIndex(p => p.name === 'engrave-fit'), Steps.passItems.map(p => [p.name, p.lane, p.order]));

  /* ═══ pass 1: a run the cloud takes up: every line looked at once ═══ */
  const r1 = await Unit.classifyUnit(await ctxOf({}));
  const a = jobOf('A'), b = jobOf('B'), c = jobOf('C');
  ok('A · the FRONT name from the order\'s option is read by the rules: one front job, "Ginger", ready for its fit', !!a && a.face === 'front' && a.state === 'ready' && a.text === 'Ginger' && a.wordsSource === 'spec.engraving', a);
  ok('B · the coordinates are cleaned by the rules (° for *, the note to the shop left out) and wait for a person to confirm', !!b && b.state === 'words' && b.text === `33°0'45" N 96°45'53" W` && /confirm the words/.test(b.reason || ''), b);
  ok('A, B · no paid reading for either (0 agent calls, 0 engraveIntentStart before C\'s)', asked.length === 0 && intentCalls() === 1, { asked: asked.length, intent: intentCalls() });
  ok('C · the unclear line starts exactly ONE paid reading (engraveIntentStart), its id kept on the job, the job still "classify"', intentCalls() === 1 && !!c && c.state === 'classify' && c.claudeJob && /^agent-/.test(c.claudeJob.id), c);
  ok('C · the reading is Claude\'s alone: the kick is charmEngrave-background, nothing else was asked of the network', F.net.kicks.length === 1 && F.net.kicks[0].name === 'charmEngrave-background' && !F.net.other.length, F.net.kicks);
  const lD = await runLine('D'), lA = await runLine('A');
  ok('D · nothing written: no job, and the run line is marked plain (engrave none, approved), as the page\'s classifyAll did', !recOf('D') && !!lD.engrave && lD.engrave.state === 'none' && lD.engrave.approved === true, lD);
  ok('A · the run line carries the summary (ready) and the piece is flagged engraveFront, as the page\'s setReady', (lA.engrave || {}).state === 'ready' && (F.doc('Charm_Pool/' + poolOf('A')) || {}).engraveFront === true,
    { line: lA.engrave, pool: F.doc('Charm_Pool/' + poolOf('A')) });
  /* 1 · BARFRONT */
  const eJobs = jobsOfRec('E'), tlE = tlSent.filter(e => e.lineKey === keyOf('E'));
  ok('E · BARFRONT: the bar line\'s undecided back moved to its FRONT once: one front job "Ginger", ready; the back is gone', eJobs.length === 1 && eJobs[0].key === keyOf('E') + '~front' && eJobs[0].face === 'front' && eJobs[0].state === 'ready' && eJobs[0].text === 'Ginger', eJobs);
  ok('E · BARFRONT: one timeline line says so ("moved to the front: bar necklaces engrave on the front by default", id <line>.barfront), and the front\'s piece is flagged engraveFront',
    tlE.filter(e => e.type === 'engraveChanged' && e.id === keyOf('E') + '.barfront' && /moved to the front/.test(e.text)).length === 1 && (F.doc('Charm_Pool/' + poolOf('E')) || {}).engraveFront === true, { tlE, pool: F.doc('Charm_Pool/' + poolOf('E')) });
  const fJobs = jobsOfRec('F');
  ok('F · BARFRONT stays: the bar line whose back Ann approved keeps that back alone: no front job, its record not written (rev 3)', fJobs.length === 1 && fJobs[0].key === keyOf('F') && fJobs[0].state === 'approved' && recOf('F').rev === 3, { rev: recOf('F').rev, fJobs });
  /* 2 · the pieces changed */
  const gJobs = jobsOfRec('G');
  ok('G · pieces changed: the line read as one job is now two pieces: the job is handed to each (#C1, #C2), its words and state kept, the whole-line job gone',
    gJobs.map(j => j.key).join() === [keyOf('G') + '#C1', keyOf('G') + '#C2'].join() && gJobs.every(j => j.state === 'ready' && j.text === 'Ginger' && j.slot === 'C' + j.key.slice(-1)), gJobs);
  /* 3 · ENGPRE */
  const h = jobOf('H'), tlH = tlSent.filter(e => e.lineKey === keyOf('H'));
  ok('H · ENGPRE: the held line\'s job is made on the pool id its piece will get (pre) and its clear words read for free: "Mila", ready', !!h && h.key === keyOf('H') && h.pre === true && h.state === 'ready' && h.text === 'Mila' && h.wordsSource === 'personalization:clear', h);
  ok('H · ENGPRE: nothing written to the order\'s timeline for it, and nothing to the pool (its piece is not made)', !tlH.length && !F.doc('Charm_Pool/' + poolOf('H')), { tlH });
  /* 4 · EDITOR WORDS */
  const i = jobOf('I'), bLike = jobOf('B');
  ok('I · EDITOR WORDS: the words would wait for a person (as B\'s do), but the Charm Editor drew its back: it goes to Review with no words, never to a words card', !!i && i.state === 'review' && i.text === '' && (i.lines || []).length === 0 && bLike.state === 'words', { i, b: bLike && bLike.state });
  ok('A-I · still ONE paid reading in all (C\'s), none for the rules\', BARFRONT\'s, the pieces\', ENGPRE\'s or the editor\'s lines', intentCalls() === 1 && asked.length === 0, { intent: intentCalls(), asked: asked.length });
  ok('pass 1 · the reading under way is remembered in the cursor (C pending, readingAt set)', !!(r1 && r1.cursor && (r1.cursor.pending || []).includes(keyOf('C')) && r1.cursor.readingAt > 0), r1 && r1.cursor);
  const revA1 = recOf('A').rev, revB1 = recOf('B').rev;

  /* a pass before the reading is answered: nothing is paid for again, nothing is written */
  const before2 = engraveSnap(), r2 = await Unit.classifyUnit(await ctxOf(r1.cursor));
  ok('pass 2 (reading not answered yet) · no second engraveIntentStart, no record written, C still waits', intentCalls() === 1 && engraveSnap() === before2 && jobOf('C').state === 'classify' && F.net.kicks.length === 1, { intent: intentCalls(), c: jobOf('C') });

  /* the background function runs (the reader answers once) */
  await F.drain(5);
  ok('the reader · charmEngrave-background ran the stubbed agent exactly once, mode engraveIntent', asked.length === 1 && asked[0].mode === 'engraveIntent', asked.map(x => x.mode));
  const INTENT = ['order', 'sku', 'title', 'form', 'quantity', 'engravable', 'personalization', 'buyerMessage', 'staffNote', 'messages', 'slots', 'pieceLengths'];
  ok('the reader · its question carries only the words\' fields from the records (no picture, no address)', asked.length === 1 && Object.keys(asked[0].payload).every(k => INTENT.includes(k)) && asked[0].payload.order === RID && !/https?:|etsystatic|data:image/i.test(JSON.stringify(asked[0].payload)), asked[0] && asked[0].payload);

  /* pass 3: the answer is taken up by its id */
  const r3 = await Unit.classifyUnit(await ctxOf(r2.cursor));
  const c3 = jobOf('C');
  ok('pass 3 · C takes the reader\'s answer by its id: "Ginger", ready for its fit; still ONE paid reading', c3 && c3.state === 'ready' && c3.text === 'Ginger' && intentCalls() === 1 && asked.length === 1, c3);
  ok('pass 3 · the run line of C carries the summary (ready); A and B were not written again', ((await runLine('C')).engrave || {}).state === 'ready' && recOf('A').rev === revA1 && recOf('B').rev === revB1, (await runLine('C')).engrave);

  /* H is pooled now, on the very id it was read on: the same job, nothing read or paid for again */
  F.seed('Charm_Pool/' + poolOf('H'), { poolId: poolOf('H'), lineKey: keyOf('H'), orderId: RID, transactionId: LINES.H.tid, sku: SKU, state: 'pooled', runId, material: 'gold', copy: 1 });
  const pooledH = await lib.call({ op: 'runLinesPatch', runId, lines: { [keyOf('H')]: { state: 'pooled', poolIds: [poolOf('H')] } } }, { unit: 'test|pool-H', fence });
  const hRev = recOf('H').rev, r3b = await Unit.classifyUnit(await ctxOf(r3.cursor));
  ok('H · once pooled (taken from the lines that waited): the same job, its words as read, no record written, no reading, no timeline line', pooledH.status === 200 && recOf('H').rev === hRev && jobOf('H').key === keyOf('H') && jobOf('H').state === 'ready' && jobOf('H').text === 'Mila'
    && intentCalls() === 1 && asked.length === 1 && !tlSent.some(e => e.lineKey === keyOf('H')) && !(r3b.cursor.waiting || []).includes(keyOf('H')), { status: pooledH.status, h: jobOf('H'), waiting: r3b.cursor.waiting });

  /* written once: more passes, and a pass that lost its cursor (the run looked at whole again), change nothing */
  const snap = engraveSnap(), w0 = F.count().writes, ops0 = spy.ops.length;
  const r4 = await Unit.classifyUnit(await ctxOf(r3b.cursor));
  const r5 = await Unit.classifyUnit(await ctxOf({}));
  await F.drain(5);
  const OpClass = F.load('_charmNestOpClass.js'), wrote = spy.ops.slice(ops0).filter(o => !OpClass.READ.has(o.op)).map(o => o.op), writes = F.count().writes - w0;
  ok('again · two more passes (one with no cursor) send no write, write nothing to Firestore, start no reading and pay for nothing', engraveSnap() === snap && intentCalls() === 1 && asked.length === 1 && !wrote.length && writes === 0, { wrote, writes, intent: intentCalls(), asked: asked.length });
  /* KEEPLIST (Paul, 22 Nov 2026: order 4242358457 never reached Design & Engrave): the driver saves a pass item's cursor merged ({ merge: true }); a map merged key by key kept every line
     ever pending, so every pass read the same first 300 lines again. Saved as the driver saves it, over a cursor that still held 1,200 done lines as a map, the list is the unit's own. */
  {
    const ref = F.db.collection('Charm_Nest_Drive').doc('keeplist-test'), stale = Object.fromEntries(Array.from({ length: 1200 }, (_, i) => ['4200000000_' + (5200000000 + i), true]));
    await ref.set({ items: { 'engrave-classify': { runId, since: 1, pending: stale, reading: { [keyOf('C')]: 'old-reading' } } } });
    const back = (await ref.get()).data().items['engrave-classify'], k1 = await Unit.classifyUnit(await ctxOf(back));
    await ref.set({ items: { 'engrave-classify': k1.cursor } }, { merge: true });
    const saved = (await ref.get()).data().items['engrave-classify'];
    ok('KEEPLIST · a cursor saved as a map (1,200 lines not in the run) is still read; the saved list is the new one: an array, none of the old lines kept, no old reading kept', Array.isArray(saved.pending) && Array.isArray(saved.reading)
      && !saved.pending.some(k => stale[k]) && !saved.reading.includes(keyOf('C')) && JSON.stringify(saved.pending) === JSON.stringify(k1.cursor.pending), { pending: (saved.pending || []).length, reading: saved.reading });
    const many = Array.from({ length: 900 }, (_, i) => 'x' + i);
    await ref.set({ items: { 'engrave-classify': { pending: many } } }, { merge: true }); await ref.set({ items: { 'engrave-classify': { pending: many.slice(600) } } }, { merge: true });
    ok('KEEPLIST · saved merged, a shorter list replaces the longer one whole (an array is one value): lines taken off stay off', (await ref.get()).data().items['engrave-classify'].pending.length === 300);
  }
  ok('again · the cursor has no reading waiting', !!(r4 && r4.cursor && !r4.cursor.readingAt && r5 && r5.cursor && !r5.cursor.readingAt), [r4 && r4.cursor, r5 && r5.cursor]);

  /* the doors */
  const page = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify({ op: 'engraveIntentStart', payload: { order: RID, personalization: ['x'] } }) });
  ok('doors · a page\'s engraveIntentStart is refused (CLOUD_ONLY, 403)', page.statusCode === 403 && /only the cloud/.test(page.body), page);
  const sa = await lib.call({ op: 'startAgent', mode: 'engraveIntent', payload: {} }, { unit: 'x' });
  ok('doors · the cloud\'s startAgent is refused ("not a cloud op"): startAgent stays denied to the cloud', sa.status === 403 && /not a cloud op/.test(sa.error || ''), sa);

  /* ═══ part 2 · 5 · THE REAL DRIVER: its tick (charmNestDriveCron's) and its pass (charmNestDrive-background) run the unit ═══ */
  F.reset(); lib._driveForget();
  const Drive = F.load('_charmNestDrive.js'), RID2 = '4300000002', TID2 = '43000000021', R2 = 'cls-20261120-2', K2 = RID2 + '_' + TID2, P2 = K2 + '_1';
  { const o = Object.assign(order('Also could you add a little heart after the name if possible?'), { receiptId: RID2 }), l = { transactionId: TID2, listingId: '1718099', sku: SKU, title: 'Hockey stick necklace', quantity: 1, metalKey: 'gold', metalLabel: 'Gold Filled', personalization: ['Ginger'], variations: [V('Metal Choice', 'Gold Filled')] };
    const spec = O.interpretLine(o, l, ictx), rl = { state: 'pooled', poolIds: [P2], sku: SKU, quantity: 1, engraveCandidate: !!spec.engraveCandidate, orderId: RID2, transactionId: TID2 };
    F.seed('Charm_World_Lines/' + K2, { v: 1, key: K2, receiptId: RID2, state: 'pooled', runId: R2, sig: 's2', at: Date.now() - 60000, gone: false, data: JSON.stringify({ v: 1, key: K2, receiptId: RID2, order: o, line: l, spec, problems: [], state: 'pooled', run: rl, runId: R2 }) });
    F.seed('Charm_Pool/' + P2, { poolId: P2, lineKey: K2, orderId: RID2, transactionId: TID2, sku: SKU, state: 'pooled', runId: R2, material: 'gold', copy: 1 });
    F.seed('Charm_Nest_Runs/' + R2, { runId: R2, status: 'review', step: 'engrave', day: '2026-11-20', lines: { [K2]: rl } });   // (a run waiting for its engraving approvals: no step runs, only the pass items)
    F.seed('Charm_Nest_Open/run', { runId: R2 });
    F.seed('Charm_Nest_Open/drive', { mode: 'standby', runMode: 'auto', cloudSteps: ['engrave'], at: Date.now() - 3600e3 });
    F.seed('Charm_Nest_Run_Carrier/' + R2, { runId: R2, carrier: { dev: 'cloud', by: 'Cloud', at: Date.now(), since: Date.now() - 600000 } });
    F.seed('Charm_World/summary', { at: Date.now(), live: 1 }); }
  const job2 = () => { const d = F.doc('Charm_Nest_Engrave/' + K2); return d ? JSON.parse(d.rec).jobs[0] : null; };
  const item2 = () => ((F.doc('Charm_Nest_Drive/state') || {}).items || {})['engrave-classify'] || {};
  const tick = () => Drive.tick({ db: F.db, kick: Drive.kick });
  const asked1 = asked.length, t1 = await tick();
  ok('driver · tick 1: the world moved, a pass is dispatched', !!t1.dispatched && (t1.why || []).includes('the world moved'), t1);
  const k1 = F.net.kicks.map(k => k.name); await F.drain(1);   // (the pass, alone)
  ok('driver · the real pass ran the classify unit: ONE reading started for the unclear line, its job "classify" with the reading\'s id, the reading remembered in the drive state (readingAt)',
    k1.join() === 'charmNestDrive-background' && (job2() || {}).state === 'classify' && /^agent-/.test(((job2() || {}).claudeJob || {}).id || '') && item2().runId === R2 && item2().readingAt > 0 && F.net.kicks.some(k => k.name === 'charmEngrave-background'),
    { kicks: F.net.kicks.map(k => k.name), job: job2(), item: item2() });
  while (F.net.kicks.length && F.net.kicks[0].name === 'charmEngrave-background') await F.drain(1);   // (the reader answers)
  ok('driver · the reader answered once', asked.length === asked1 + 1, asked.length - asked1);
  F.clock.advance(30000);
  const t2 = await tick();
  ok('driver · 30 s later the reading has waited less than a minute: no pass for it', !(t2.why || []).includes('an engraving reading waits') && !t2.dispatched, t2);
  F.clock.advance(31000);
  const t3 = await tick();
  ok('driver · a minute later: the drive check starts a pass ("an engraving reading waits")', !!t3.dispatched && (t3.why || []).includes('an engraving reading waits'), t3);
  await F.drain(1);
  ok('driver · that pass takes the answer by its id: "Ginger", ready for its fit; nothing waits any more (readingAt 0); still ONE reading', (job2() || {}).state === 'ready' && (job2() || {}).text === 'Ginger' && !item2().readingAt && asked.length === asked1 + 1,
    { job: job2(), item: item2(), asked: asked.length - asked1 });
  F.clock.advance(61000);
  const t4 = await tick();
  ok('driver · afterwards no reading waits: the drive check starts no pass for it', !(t4.why || []).includes('an engraving reading waits'), t4);

  /* ═══ part 3 · ENGNOTE: a staff note "ENG: <words>" in the cloud's reading ═══ */
  F.reset(); lib._driveForget();
  const R3 = 'cls-20261120-3', RID3 = '4399000101', RID4 = '4399000102', fence3 = { path: 'Charm_Nest_Drive/lease', nonce: 'pass-3', runId: R3 };
  const NOTE_CAL = 'only the month of March and the 9th marked as in the photo please', NOTE_DEC = 'please make this the center piece and tell me if it should be larger than 12mm';
  const M3 = [{ senderName: 'Gina S.', text: 'MAKE CHAIN 16"', at: T0 - 9000 }, { senderName: 'Gina S.', text: 'DATE FOR CALENDAR: March 9th, 2004', at: T0 - 8000 }, { senderName: 'Gina S.', text: 'ENG: Happy Birthday Pretty', at: T0 - 7000 }];
  const M4 = [{ senderName: 'Gina S.', text: 'ENG:  Love you\nAlways ', at: T0 - 7000 }];
  const lines3 = {}, docs3 = {};
  const seed3 = (rid, tid, sku, opts, pers, msgs) => {
    const key = rid + '_' + tid, o = Object.assign(order(''), { receiptId: rid, messages: msgs }), l = { transactionId: tid, listingId: '1718099', sku, title: sku, quantity: 1, metalKey: 'gold', metalLabel: 'Gold Filled', personalization: pers, variations: [V('Metal Choice', 'Gold Filled')].concat(opts) };
    const ent = { sku, members: 2, holes: 1, backKeepOut: [], widthPt: 30, heightPt: 30 }, spec = O.interpretLine(o, l, Object.assign({}, ictx, { masterEntry: () => ent }));
    lines3[key] = { state: 'pooled', poolIds: [key + '_1'], sku, quantity: 1, engraveCandidate: !!spec.engraveCandidate, orderId: rid, transactionId: tid };
    docs3[key] = { o, l, spec };
    F.seed('Charm_World_Lines/' + key, { v: 1, key, receiptId: rid, state: 'pooled', runId: R3, sig: 's3', at: Date.now() - 60000, gone: false, data: JSON.stringify({ v: 1, key, receiptId: rid, order: o, line: l, spec, problems: [], state: 'pooled', run: lines3[key], runId: R3 }) });
    F.seed('Charm_Pool/' + key + '_1', { poolId: key + '_1', lineKey: key, orderId: rid, transactionId: tid, sku, state: 'pooled', runId: R3, material: 'gold', copy: 1 });
    F.seed('Charm_Master_Index/' + sku, { sku, aiPath: 'charmnest/master/' + sku + '.ai', widthPt: 30, heightPt: 30, engravable: true });
    return key;
  };
  const kCal = seed3(RID3, '43990001011', 'PERSONALIZED_23973', [V('Charm Type', 'CHARM + Engraving')], [NOTE_CAL], M3);
  const kDec = seed3(RID3, '43990001012', 'DECORATIVE_2519', [V('Charm Size', '12mm')], [NOTE_DEC], M3);
  const kPlain = seed3(RID3, '43990001013', 'GIRAFFE', [V('Charm Type', 'Necklace CHARM')], [], M3);
  const kAna = seed3(RID3, '43990001014', 'DONUT 1', [V('Charm Type', 'Necklace CHARM')], [], M3);
  const kA = seed3(RID4, '43990001021', 'PERSONALIZED_23973', [V('Charm Type', 'CHARM + Engraving')], [NOTE_CAL], M4);
  const kB = seed3(RID4, '43990001022', 'PERSONALIZED_23974', [V('Charm Type', 'CHARM + Engraving')], ['the same as the other please'], M4);
  const kBlank = seed3(RID4, '43990001023', 'BAR_1548', [V('Metal · #Characters?', 'GOLD·Set·BLANK')], [], M4);
  const RID5 = '4399000103', M5 = M3.slice();   // (an order whose calendar a person approved with the note's words: a newer note must not jump to another line)
  const kCal5 = seed3(RID5, '43990001031', 'PERSONALIZED_23973', [V('Charm Type', 'CHARM + Engraving')], [NOTE_CAL], M5);
  const kDec5 = seed3(RID5, '43990001032', 'DECORATIVE_2519', [V('Charm Size', '12mm')], [NOTE_DEC], M5);
  // (what the page shows today: the calendar and the decorative charm wait for a person with the buyer's note as their words; the giraffe read as nothing to engrave; the donut cut plain by Ana)
  const words3 = (k, t) => ({ key: k, state: 'words', reason: 'Check the requested inscription', text: t, lines: [t], source: 'personalization', quote: t, confidence: 0, questions: ['Check the requested inscription'], requests: { side: 'back', font: null, handwriting: false, image: false }, lineGap: .216 });
  const rec3 = (k, jobs, engrave) => { F.seed('Charm_Nest_Engrave/' + k, { key: k, order: k.split('_')[0], rec: JSON.stringify({ v: 1, override: null, engrave, raised: true, jobs }), at: T0 - 600000, rev: 2, by: 'Ann', dev: 'd1aaaa' }); lines3[k].engrave = engrave; };
  rec3(kCal, [words3(kCal, NOTE_CAL)], { needed: true, state: 'words', text: NOTE_CAL, approved: false });
  rec3(kDec, [words3(kDec, NOTE_DEC)], { needed: true, state: 'words', text: NOTE_DEC, approved: false });
  rec3(kPlain, [{ key: kPlain, state: 'none', reason: 'No engraving requested', text: '', lines: [], lineGap: .216 }], { needed: false, state: 'none', approved: true, seals: [] });
  rec3(kAna, [{ key: kAna, state: 'none', reason: 'no engraving — decided by Ana', decision: { by: 'Ana', at: T0 - 500000, none: true }, lineGap: .216 }], { needed: false, state: 'none', approved: true, seals: [{ id: 'engravePlain:' + (T0 - 500000) + ':Ana', how: 'engravePlain', at: T0 - 500000, by: 'Ana' }] });
  rec3(kCal5, [{ key: kCal5, state: 'approved', text: 'Happy Birthday Pretty', lines: ['Happy Birthday Pretty'], source: 'staffNote', quote: 'ENG: Happy Birthday Pretty', wordsSource: 'staffNote:eng', confidence: 1, approvedBy: 'Ana', approvedAt: T0 - 6500, engravingSeals: [{ id: 'engraveApproved:' + (T0 - 6500) + ':Ana', how: 'engraveApproved', at: T0 - 6500, by: 'Ana' }], lineGap: .216 }],
    { needed: true, state: 'approved', text: 'Happy Birthday Pretty', approved: true, approvedBy: 'Ana', approvedAt: T0 - 6500 });
  rec3(kDec5, [words3(kDec5, NOTE_DEC)], { needed: true, state: 'words', text: NOTE_DEC, approved: false });
  F.seed('Charm_Nest_Open/drive', { mode: 'standby', runMode: 'auto', cloudSteps: ['engrave'], at: T0 - 3600e3 });
  F.seed('Charm_Nest_Runs/' + R3, { runId: R3, status: 'review', step: 'engrave', day: '2026-11-20', lines: JSON.parse(JSON.stringify(lines3)) });
  F.seed('Charm_Nest_Drive/lease', { nonce: 'pass-3', state: 'running', at: Date.now() });
  F.seed('Charm_Nest_Run_Carrier/' + R3, { runId: R3, carrier: { dev: 'cloud', by: 'Cloud', at: Date.now(), since: Date.now() } });
  lib._driveForget();
  const ctx3 = async cursor => { const g = await lib.call({ op: 'runGet', runId: R3 }, {}); return { db: F.db, run: g.run, runId: R3, step: 'pass:engrave-classify', cursor: cursor || {}, call: (b, u) => lib.call(b, { unit: u, fence: fence3 }), read: b => lib.call(b, {}), say: x => said.push(x), live: () => {}, budgetEnd: Date.now() + 40000, pass: true }; };
  const rec3Of = k => { const d = F.doc('Charm_Nest_Engrave/' + k); return d ? Object.assign({ rev: d.rev }, JSON.parse(d.rec)) : null; }, job3 = k => ((rec3Of(k) || {}).jobs || [])[0] || null;
  const asked3 = asked.length, intent3 = intentCalls();
  const e1 = await Unit.classifyUnit(await ctx3({}));
  const cj = job3(kCal);
  ok('ENG · the calendar ("CHARM + Engraving") takes the staff note\'s words "Happy Birthday Pretty": ready, its reason names the note\'s writer, the buyer\'s note no longer its words',
    !!cj && cj.state === 'ready' && cj.text === 'Happy Birthday Pretty' && cj.wordsSource === 'staffNote:eng' && /staff note by Gina S\./.test(cj.reason || '') && rec3Of(kCal).engrave.text === 'Happy Birthday Pretty' && !/March/.test(cj.text), cj);
  ok('ENG · NO paid reading for any line of either order (0 agent calls, 0 engraveIntentStart)', asked.length === asked3 && intentCalls() === intent3, { asked: asked.length - asked3, intent: intentCalls() - intent3 });
  ok('ENG · the decorative charm keeps waiting for a person, untouched (rev 2, words, its own note)', rec3Of(kDec).rev === 2 && job3(kDec).state === 'words' && job3(kDec).text === NOTE_DEC, job3(kDec));
  ok('ENG · the line read as nothing to engrave and the line Ana cut plain are untouched (rev 2)', rec3Of(kPlain).rev === 2 && rec3Of(kAna).rev === 2 && job3(kAna).state === 'none' && job3(kAna).decision.by === 'Ana');
  const ja = job3(kA), jb = job3(kB), jbl = job3(kBlank);
  ok('ENG · two lines that could take the words: neither is guessed: each shows "Love you / Always" to confirm (words, staffNote:suggest), the writer\'s line break kept',
    [ja, jb].every(j => j && j.state === 'words' && j.text === 'Love you\nAlways' && j.wordsSource === 'staffNote:suggest' && /does not say which line/.test(j.reason || '')), [ja, jb]);
  ok('ENG · the BLANK line takes no words from the note', !jbl || !/Love you/.test(jbl.text || ''), jbl);
  // a newer note, a while later: every line of the order is read again by the world (its messages changed)
  F.clock.advance(10000);
  const M3b = M3.concat([{ senderName: 'Gina S.', text: 'eng : Happy Birthday Mom', at: Date.now() - 1000 }]);
  for (const k of [kCal, kDec, kPlain, kAna, kCal5, kDec5]) { const d = docs3[k], spec = Object.assign({}, d.spec, { messages: M3b }), o = Object.assign({}, d.o, { messages: M3b });
    F.seed('Charm_World_Lines/' + k, { v: 1, key: k, receiptId: k.split('_')[0], state: 'pooled', runId: R3, sig: 's3b', at: Date.now(), gone: false, data: JSON.stringify({ v: 1, key: k, receiptId: RID3, order: o, line: d.l, spec, problems: [], state: 'pooled', run: lines3[k], runId: R3 }) }); }
  const e2 = await Unit.classifyUnit(await ctx3(e1.cursor));
  ok('ENG · a newer note ("eng : Happy Birthday Mom") replaces the older one on the calendar: ready with the new words; the decorative and the plain lines still untouched; no paid reading',
    job3(kCal).state === 'ready' && job3(kCal).text === 'Happy Birthday Mom' && rec3Of(kDec).rev === 2 && rec3Of(kPlain).rev === 2 && rec3Of(kAna).rev === 2 && asked.length === asked3 && intentCalls() === intent3, job3(kCal));
  ok('ENG · a note never jumps: the calendar a person APPROVED with "Happy Birthday Pretty" is left as it is (rev 2) under the newer "Mom" note, and the decorative charm is NOT given the words (rev 2, still waiting)',
    rec3Of(kCal5).rev === 2 && job3(kCal5).state === 'approved' && job3(kCal5).text === 'Happy Birthday Pretty' && rec3Of(kDec5).rev === 2 && job3(kDec5).state === 'words' && job3(kDec5).text === NOTE_DEC, { cal: job3(kCal5), dec: job3(kDec5) });
  { const Cls = require(path.join(root, 'charm-nest-engrave-classify.js')), Faces3 = require(path.join(root, 'charm-nest-engrave-faces.js')), R3d = require(path.join(root, 'charm-nest-engrave-read.js'));
    const rowOf3 = k => { const d = docs3[k]; return { key: k, order: Object.assign({}, d.o, { messages: M3b }), line: d.l, spec: Object.assign({}, d.spec, { messages: M3b }), state: 'pooled', engrave: rec3Of(k).engrave }; };
    const rows5 = [rowOf3(kCal5), rowOf3(kDec5)], h = { rows: rows5, jobsOf: r => rec3Of(r.key).jobs, READER: () => R3d, neverEngraves: r => Faces3.neverOf(r), finished: r => O.FINISHED_LINE.has(r.state) };
    const n = Cls.engNotice(rows5[0], h);
    ok('ENG · the approved calendar\'s card says it: "Newer staff note by Gina S.: “Happy Birthday Mom” — the approved words were kept" (from the records, as every page reads them); the decorative charm\'s says nothing',
      n === 'Newer staff note by Gina S.: “Happy Birthday Mom” — the approved words were kept' && Cls.engNotice(rows5[1], h) === '', n); }
  const snap3 = JSON.stringify(F.list('Charm_Nest_Engrave').map(d => [d._id, d.rev]).sort()), wr3 = F.count().writes;
  await Unit.classifyUnit(await ctx3(e2.cursor)); await Unit.classifyUnit(await ctx3({}));
  ok('ENG · two more passes (one with no cursor) write nothing and pay for nothing', JSON.stringify(F.list('Charm_Nest_Engrave').map(d => [d._id, d.rev]).sort()) === snap3 && F.count().writes === wr3 && asked.length === asked3 && intentCalls() === intent3, { writes: F.count().writes - wr3 });

  /* the owner's rule */
  ok('rule · Etsy was never called (its stub counted 0 calls)', F.etsy.calls === 0, F.etsy.log);
  ok('rule · no Etsy address and no other address was fetched', !F.net.tripwire.length && !F.net.other.length, { tripwire: F.net.tripwire, other: F.net.other });
  ok('rule · the Etsy switch was never written', !F.stopWrites.length, F.stopWrites);
  if (failed) out('  the unit said:\n    ' + said.slice(-20).join('\n    '));
  out(failed ? `FAIL cloud-engrave-classify (${failed})` : 'PASS cloud-engrave-classify');
  process.exit(failed ? 1 : 0);
})().catch(e => { out('FAIL the test crashed: ' + ((e && e.stack) || e)); out('    ' + said.slice(-20).join('\n    ')); process.exit(1); });
