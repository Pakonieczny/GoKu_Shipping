// BARFRONT, the bar lines already waiting (Paul, 28 Oct 2026, order 4212020155, TINY BAR 7427 waiting for Approve with "VIII XVII XXVI" on its back: "Also make sure that all orders that contain a bar necklace should
// have the engraving default to the front of the bar. Not just this listing, but any listing that has a bar necklace."). Offline: the real reader (charm-nest-orders.js), the real faces model, and the page's own
// engraving code (charm-nest-bridge.js, sliced as engrave-faces-bridge.cjs slices it) in a vm over stand-ins; no browser, no network, no paid call, nothing live.
//
//   1  a RESTORED bar line (read before the rule: back only) whose back waits for Approve, a person's moves on it: read again, it moves to the front, once: the same words, line split and font, the back gone with
//      its Review card, the front fitted again, ONE timeline line and ONE agent line, no Review card; read again, nothing more happens
//   2  a bar line nobody may move: its back approved, a person's own setup (engravingOverride), an approval under way, a piece the laser has cut: every job exactly as it was
//   3  a line that is no bar (another listing's disc necklace): exactly as it was
//   4  a back with no settled words yet: its front is read from the setup (free), as a new line's front is
//   5  afterRestore: reads the lines again only when a restored bar line waits on its back and the maps and library are in
//   6  the hooks: Orders.interpretAll asks every line, the boot asks after a restore, the page loads the file
//   node tests/charm-nest/bar-front-waiting.cjs
'use strict';
const path = require('path'), fs = require('fs'), vm = require('vm'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const transport = require.resolve(path.join(root, 'netlify/functions/_etsyMailAnthropic.js'));
require.cache[transport] = { id: transport, filename: transport, loaded: true, exports: {} };
const Pair = require(path.join(root, 'charm-nest-pair.js'));
const Sides = require(path.join(root, 'charm-nest-engrave-sides.js'));
const Seals = require(path.join(root, 'charm-nest-engraving-seals.js'));
const Faces = require(path.join(root, 'charm-nest-engrave-faces.js'));
const O = require(path.join(root, 'charm-nest-orders.js'));
const BarFront = require(path.join(root, 'charm-nest-engrave-bar-front.js'));

let checks = 0; const results = [];
const ok = (c, m) => { assert.ok(c, m); checks++; }, eq = (a, b, m) => { assert.deepEqual(J(a), J(b), m); checks++; };
const J = x => (x === undefined ? undefined : JSON.parse(JSON.stringify(x)));
const t = async (name, fn) => { try { await fn(); results.push([name, null]); console.log('  ok  ' + name); } catch (e) { results.push([name, e]); console.log('  FAIL ' + name + '\n      ' + String(e && e.stack || e).split('\n').slice(0, 6).join('\n      ')); } };
const tick = () => new Promise(r => setTimeout(r, 5));

/* ── the order of the screenshot, as the reader reads it ── */
const RID = '4212020155', SKU = 'TINY BAR_7427', LISTING = '1111111111';
const TITLE = 'Tiny Bar Necklace Personalized Roman Numeral Necklace Custom Date Gift for Her';
const entryFor = s => (/^(TINY BAR_7427|INITIAL_DISC_1)$/i.test(String(s || '').trim()) ? { sku: String(s).toUpperCase(), widthPt: 73.4, heightPt: 14 } : null);
const readCtx = { optionMaps: {}, aliases: {}, noDesign: {}, listingSkus: {}, masterEntry: entryFor, masterLoose: () => '' };
const VARS = [{ name: 'Metal', value: 'Gold Filled' }, { name: 'LENGTH / FONT / ALIGNMENT', value: '16"·PRISTINA·Center' }];
const barLine = (n, o) => Object.assign({ transactionId: String(5212020150 + n), listingId: LISTING, sku: SKU, title: TITLE, quantity: 1, metalKey: 'gold', metalLabel: 'Gold Filled', personalization: ['VIII XVII XXVI'], variations: VARS }, o || {});
const order = { receiptId: RID, updateTs: 0 };
const readNow = line => O.interpretLine(order, line, readCtx);
/** A line as a checkpoint from before the bar rule kept it: the same reading with no face (the shop's usual back). */
function restoredRow(line, extra) {
  const before = readNow(line); delete before.engraving;
  return Object.assign({ key: `${RID}_${line.transactionId}`, order, line, spec: before, state: 'written', poolIds: [`${RID}_${line.transactionId}_1`] }, extra || {});
}

/* ── the page's own code (SIDES … setNone, setupOf … applyEngraving, wantsFront), run over stand-ins ── */
const bridgeSrc = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
const cut = (from, to) => { const a = bridgeSrc.indexOf(from), b = bridgeSrc.indexOf(to, a); assert.ok(a > 0 && b > a, 'slice ' + from.trim().slice(0, 30)); return bridgeSrc.slice(a, b); };
const SLICE1 = cut('  const SIDES = ', '  function setNone(job'), SLICE2 = cut("  /** The line's effective engraving setup", '  let previewRecovery'), SLICE3 = cut("  /** A line whose setup asks for a front that has no front job yet", '  let classifyPass');
function harness(rows, o = {}) {
  const jobs = new Map(), events = [], agents = [], reviewAdds = [], reviewRemoves = [], fits = [], readies = [], poolUpdates = [];
  const win = { CharmNestEngraveSides: Sides, CharmNestPair: Pair, CharmNestEngraveFaces: Faces };
  const byKey = new Map(rows.map(r => [r.key, r]));
  const ctx = {
    window: win, items: () => jobs, B: { pool: { rows: new Map() }, orders: { byKey }, run: null },
    Pool: { charmOf: () => null, previewIds: () => [], update: async (ids, patch) => { poolUpdates.push({ ids, patch }); }, sheetOf: id => (o.sheets || {})[id] || null }, Master: { entryFor }, CNEngravingSeals: Seals, DECIDED: ['approved', 'written', 'skipped'],
    O: { FINISHED_LINE: O.FINISHED_LINE, engravingNote: null, rawWords: O.rawWords, decodeEntities: O.decodeEntities }, S: { settings: { engraveConfidence: .65 } },
    render() {}, agent: (scope, kind, text) => agents.push({ kind, text }), Date, Promise, Map, Set, WeakMap, Array, Object, String, Number, JSON, Math, RegExp, Error,
    Review: { add: it => reviewAdds.push(it.key), remove: k => reviewRemoves.push(k), render() {} }, RunCtl: { poke() {}, save: async () => {} }, Session: { schedule() {} }, Orders: { render() {}, rows: () => rows, lineRecord: r => [r.key, { key: r.key }] },
    TL: { line: (row, type, d) => { if (d && d.once && events.some(e => e.type === type && e.id === d.id)) return; events.push({ row, type, id: d && d.id, text: d && d.text, data: d && d.data }); }, hash: s => String(s).length },
    sheets: [], allSheets: () => [], toast() {}, api: async () => ({}), EG: {}, employeeName: () => 'Paul', needEmployee: async () => 'Paul', charmFor: () => null, unplacedPre: () => false, virtualJob: () => false,
    setReady(job) { readies.push(job.key); job.state = 'ready'; job.row.engrave = Object.assign({}, job.row.engrave, { needed: true, state: 'ready', text: job.text, approved: false }); return job; },
    toWords(job, why) { job.state = 'words'; job.reason = why; job.row.engrave = Object.assign({}, job.row.engrave, { needed: true, state: 'words', text: job.text, approved: false, reason: why }); return job; },
    setNone(job, why) { job.state = 'none'; job.reason = why; job.row.engrave = Object.assign({}, job.row.engrave, { needed: false, state: 'none', reason: why, approved: true }); return job; },
    agentCall: async () => { throw new Error('a paid reading was asked for'); },
  };
  vm.createContext(ctx); vm.runInContext(SLICE1, ctx); vm.runInContext(SLICE2, ctx); vm.runInContext(SLICE3, ctx); vm.runInContext('this.wantsFront = wantsFront;', ctx);
  // the Engrave object as the page exports it (the names charm-nest-engrave-bar-front.js reads), over the sliced code; a fit is free and offline: here it only records that it was asked
  const Engrave = vm.runInContext('({ items, jobsOf, isFront, faceOf, ensureJobs, neverEngraves, classify, setupOf, render })', ctx);
  Object.assign(Engrave, { setReady: ctx.setReady, canFit: () => true, isWorking: j => !!(o.working && o.working.has(j.key)), fitJob: async j => { fits.push({ key: j.key, text: j.text, fontKey: j.fontKey }); j.state = 'review'; j.row.engrave = Object.assign({}, j.row.engrave, { state: 'review' }); return j; }, draftOf: () => null, view: () => ({ drafts: {} }) });
  const interpreted = [];
  const env = { Engrave, CharmNestEngraveFaces: Faces, CharmNestOrders: O, Review: ctx.Review, Pool: ctx.Pool, Session: ctx.Session, CNTimeline: ctx.TL, agent: ctx.agent, replaceCutWhy: sh => (sh && sh.laserDoneAt ? 'laser cut' : ''),
    Orders: { rows: () => rows, render() {}, interpretAll() { interpreted.push(Date.now()); for (const r of rows) { r.spec = readNow(r.line); env.CNBarFront.move(r); } } }, B: o.B || { master: { loadedAt: 1, loading: null }, maps: { loadedAt: 1 } } };
  env.CNBarFront = win.CNBarFront = BarFront.make(env);   // (the page's plan asks it too: faceWant, wantsFront)
  return { ctx, env, jobs, events, agents, reviewAdds, reviewRemoves, fits, readies, poolUpdates, interpreted, Engrave, bf: env.CNBarFront, keys: () => [...jobs.keys()] };
}
/** The back the page had made for a restored line before the rule: read, fitted, waiting for Approve, moved by a person (Paul: "after I made adjustments"). */
function waitingBack(h, row, extra) {
  const [b] = h.Engrave.ensureJobs(row);
  const font = O.pieceFont(row.spec, 0);
  Object.assign(b, { state: 'review', text: 'VIII XVII XXVI', lines: ['VIII', 'XVII', 'XXVI'], lineInput: ['VIII', 'XVII', 'XXVI'], lineMode: 'manual', source: 'personalization', quote: 'VIII XVII XXVI', confidence: .95, questions: [], requests: { side: 'back', font: null }, fontKey: font.id, fontAsked: font.asked, fit: { size: 9, capMm: 2.1 }, verify: { geometry: { ok: true } }, view: { cx: 1 }, nudged: true, wantSize: 8.5 }, extra || {});
  row.engrave = { needed: true, state: 'review', text: b.text, approved: false };
  return b;
}

(async () => {
  console.log('BARFRONT — bar lines already waiting on their back');

  await t('1 a restored bar line whose back waits for Approve moves to the front once: same words and font, the back gone, the front fitted again, one timeline line, no Review card', async () => {
    const row = restoredRow(barLine(5)), h = harness([row]), back = waitingBack(h, row);
    eq([Faces.setupOf(row).mode, h.bf.move(row)], ['back', false], 'as restored (the reading from before the rule): the back, nothing moves');
    eq(h.keys(), [row.key], 'one back job');
    // the page reads the line again (Orders.interpretAll: the first order check after the reload, a Resume, this file's afterRestore)
    row.spec = readNow(row.line);
    eq([row.spec.engraving.mode, row.spec.engraving.source, /bar rule/.test(row.spec.engraving.why)], ['front', 'profile', true], 'read again: the bar rule says front');
    const t0 = process.hrtime.bigint(), moved = h.bf.move(row), ms = Number(process.hrtime.bigint() - t0) / 1e6;
    ok(moved, 'it moves');
    eq(h.keys(), [row.key + '~front'], 'one FRONT job, no back job');
    const f = h.jobs.get(row.key + '~front');
    eq([f.face, f.text, f.lines, f.lineInput, f.lineMode, f.quote], ['front', 'VIII XVII XXVI', ['VIII', 'XVII', 'XXVI'], ['VIII', 'XVII', 'XXVI'], 'manual', 'VIII XVII XXVI'], 'the back\'s words, line split and quote, exactly');
    eq([f.fontKey, f.fontAsked, f.requests.side], [back.fontKey, back.fontAsked, 'front'], 'the same font');
    eq([O.pieceFont(row.spec, 0, 'front').id, back.fontKey], [back.fontKey, back.fontKey], 'and the font the front reads for itself is that font (Pristina, the line\'s own), so a refit keeps it');
    ok(back.fontKey && back.fontKey !== O.DEFAULT_FONT, 'a font of its own (' + back.fontKey + '), not the default');
    eq([f.fit, f.nudged, f.wantSize, f.approvedAt], [null, undefined, null, null], 'the back\'s placement is not carried: the front is fitted again');
    eq([f.row.parentRow === row, Faces.setupOf(row).faces], [true, ['front']], 'the front sits on its own row of the line; the line is Front only');
    ok(h.reviewRemoves.includes('eng:' + row.key), 'the back\'s Review card goes with it');
    eq(h.reviewAdds, [], 'no Review card is raised');
    await tick(); await tick();
    eq(h.readies, [row.key + '~front'], 'made ready once');
    eq(h.fits.map(x => [x.key, x.text, x.fontKey]), [[row.key + '~front', 'VIII XVII XXVI', back.fontKey]], 'fitted again on the front, with its words and font');
    eq([row.engrave.state, row.engrave.approved, row.engrave.text], ['review', false, 'Front: VIII XVII XXVI'], 'the line now waits for the FRONT\'s Approve: ' + row.engrave.text);
    const ev = h.events.filter(e => e.type === 'engraveChanged');
    eq(ev.length, 1, 'one timeline line'); ok(ev[0].text.includes(BarFront.MOVED) && ev[0].text.includes('VIII XVII XXVI'), 'it says so: ' + ev[0].text); eq([ev[0].data.how, ev[0].data.face], ['barFront', 'front'], 'its data');
    const al = h.agents.filter(a => a.text.includes(BarFront.MOVED)); eq(al.length, 1, 'one agent line: ' + (al[0] && al[0].text));
    // read again (every pass of interpretAll asks): nothing more happens
    row.spec = readNow(row.line);
    eq([h.bf.move(row), h.bf.move(row)], [false, false], 'read again: nothing more moves');
    eq([h.keys(), h.events.filter(e => e.type === 'engraveChanged').length, h.readies.length, h.fits.length], [[row.key + '~front'], 1, 1, 1], 'the same front, still one line, no second fit');
    console.log(`      moved in ${ms.toFixed(2)} ms of main-thread work`);
  });

  await t('1b a bar line bought twice: each piece\'s back hands its own words to its own front', async () => {
    const line = barLine(7, { quantity: 2 }), row = restoredRow(line, { poolIds: [`${RID}_${barLine(7).transactionId}_1`, `${RID}_${barLine(7).transactionId}_2`] }), h = harness([row]);
    const backs = h.Engrave.ensureJobs(row); console.log('      pieces: ' + backs.map(b => b.key.slice(row.key.length) || '(line)').join(', '));
    ok(backs.length === 2 && backs.every(b => b.slot), 'two backs, one per piece');
    backs.forEach((b, i) => Object.assign(b, { state: 'review', text: i ? 'TWO' : 'ONE', lines: [i ? 'TWO' : 'ONE'], fit: { size: 9 } }));
    row.spec = readNow(row.line); ok(h.bf.move(row), 'it moves');
    eq(h.keys().sort(), backs.map(b => b.key + '~front').sort(), 'a front for each piece, no back');
    eq(backs.map(b => h.jobs.get(b.key + '~front').text), ['ONE', 'TWO'], 'each with its own back\'s words');
    await tick(); await tick(); eq(h.fits.length, 2, 'both fitted again'); eq(h.events.filter(e => e.type === 'engraveChanged').length, 1, 'one timeline line for the line');
  });

  await t('2 a bar line nobody may move stays exactly as it is: an approved back, a person\'s own setup, an approval under way, a piece the laser has cut', async () => {
    const cases = [
      ['approved back', row => ({}), (h, row) => waitingBack(h, row, { state: 'approved', approvedAt: 5, approvedBy: 'Paul' })],
      ['a person\'s override (back)', row => ({ engravingOverride: { mode: 'back', by: 'Rita', at: 3 } }), (h, row) => waitingBack(h, row)],
      ['a person\'s words only', row => ({ engravingOverride: { back: { text: 'VIII XVII XXVI' }, by: 'Rita', at: 3 } }), (h, row) => waitingBack(h, row)],
      ['an approval under way', row => ({}), (h, row) => waitingBack(h, row, { approvalIntent: { phase: 'verified', at: 9, by: 'Paul' } })],
      ['skipped (cut plain)', row => ({}), (h, row) => waitingBack(h, row, { state: 'skipped', decidedAt: 4 })],
      ['no engraving', row => ({}), (h, row) => waitingBack(h, row, { state: 'none' })],
      ['a back file written', row => ({}), (h, row) => waitingBack(h, row, { backs: [{ poolId: row.poolIds[0] }] })],
      ['a reading or fit running', row => ({}), (h, row) => waitingBack(h, row), { working: true }],
      ['the laser has cut it', row => ({}), (h, row) => waitingBack(h, row), { cut: true }],
      ['the order is committed', row => ({ state: 'committed' }), (h, row) => waitingBack(h, row)],
    ];
    let n = 0;
    for (const [name, extra, make, opt] of cases) {
      const line = barLine(10 + n++), row = restoredRow(line), o2 = {};
      if (opt && opt.working) o2.working = new Set([row.key]);
      if (opt && opt.cut) o2.sheets = { [row.poolIds[0]]: { sheetId: 's1', laserDoneAt: 7 } };
      const h = harness([row], o2), back = make(h, row);
      Object.assign(row, extra(row)); row.spec = readNow(row.line);   // (read again: the bar rule says front)
      const before = JSON.stringify(J(Object.assign({}, back, { row: undefined }))), keys = h.keys();
      eq(h.bf.move(row), false, name + ': nothing moves');
      eq([h.keys(), JSON.stringify(J(Object.assign({}, back, { row: undefined })))], [keys, before], name + ': the back job is exactly as it was');
      eq([h.events.length, h.agents.length, h.reviewRemoves.length, h.fits.length], [0, 0, 0, 0], name + ': no line, no card taken down, no fit');
    }
  });

  await t('2b a decided bar back gains no front from the page\'s own plan either (Engrave.ensureJobs: a sheet\'s backs, a record, a reading; the run\'s wantsFront): the person\'s decision stands, back only', async () => {
    for (const [name, extra] of [['approved', { state: 'approved', approvedAt: 5, approvedBy: 'Paul' }], ['written', { state: 'written', approvedAt: 5, approvedBy: 'Paul', backs: [{ poolId: 'x' }] }], ['skipped', { state: 'skipped', decidedAt: 4 }]]) {
      const row = restoredRow(barLine(60)), h = harness([row]), back = waitingBack(h, row, extra);
      row.spec = readNow(row.line); row.engrave = { needed: true, state: back.state, approved: true, text: back.text };
      const js = h.Engrave.ensureJobs(row);
      eq([js.map(j => j.key), h.keys()], [[row.key], [row.key]], name + ': ensureJobs keeps the back alone, no front is made');
      eq(h.ctx.wantsFront(row), false, name + ': the run does not look at it for a front');
      eq([back.state, js[0] === back], [extra.state, true], name + ': the same job, as decided');
    }
    // an undecided back is still the plan's to follow (and BARFRONT moves it first, at its reading)
    const row = restoredRow(barLine(61)), h = harness([row]); waitingBack(h, row); row.spec = readNow(row.line);
    eq(h.ctx.wantsFront(row), true, 'an undecided back: the run looks at it for its front, as before');
  });

  await t('3 a line that is no bar keeps its back, exactly as it was', async () => {
    const line = barLine(30, { sku: 'INITIAL_DISC_1', title: 'Initial Disc Necklace Custom Engraved Jewelry' }), row = restoredRow(line), h = harness([row]), back = waitingBack(h, row);
    row.spec = readNow(row.line);
    eq([row.spec.engraving || null, Faces.setupOf(row).mode], [null, 'back'], 'read again: no face, the shop\'s usual back');
    const before = JSON.stringify(J(Object.assign({}, back, { row: undefined })));
    eq([h.bf.move(row), h.keys(), JSON.stringify(J(Object.assign({}, back, { row: undefined }))), h.events.length], [false, [row.key], before, 0], 'nothing moves, nothing is said');
  });

  await t('4 a back with no settled words yet: its front is read from the setup (the buyer\'s words, free, no paid reading)', async () => {
    const row = restoredRow(barLine(40)), h = harness([row]); const [b] = h.Engrave.ensureJobs(row); b.state = 'classify';
    row.spec = readNow(row.line); ok(h.bf.move(row), 'it moves');
    await tick(); await tick();
    const f = h.jobs.get(row.key + '~front');
    eq([h.keys(), f.text, f.wordsSource, f.requests.side], [[row.key + '~front'], 'VIII XVII XXVI', 'spec.engraving', 'front'], 'the front reads the buyer\'s words from the bar rule');
    eq(h.fits.map(x => x.key), [row.key + '~front'], 'and is fitted');
  });

  await t('5 afterRestore reads the lines again only when a restored bar line waits on its back and the maps and library are in', async () => {
    const row = restoredRow(barLine(50)), h = harness([row]); waitingBack(h, row);
    eq(await h.bf.afterRestore(), true, 'a bar line waits: the lines are read again'); eq(h.interpreted.length, 1, 'once');
    eq(h.keys(), [row.key + '~front'], 'and it moved');
    eq(await h.bf.afterRestore(), false, 'nothing waits any more: no reading'); eq(h.interpreted.length, 1, 'still once');
    const r2 = restoredRow(barLine(51)), h2 = harness([r2], { B: { master: { loadedAt: 0, loading: null }, maps: { loadedAt: 1 } } }); waitingBack(h2, r2);
    eq([await h2.bf.afterRestore(), h2.interpreted.length, h2.keys()], [false, 0, [r2.key]], 'the library not in yet: the next order check reads it');
    const r3 = restoredRow(barLine(52, { sku: 'INITIAL_DISC_1', title: 'Initial Disc Necklace' })), h3 = harness([r3]); waitingBack(h3, r3);
    eq([await h3.bf.afterRestore(), h3.interpreted.length], [false, 0], 'no bar line waits: nothing is read');
  });

  await t('6 the hooks: Orders.interpretAll asks every line it reads, the boot asks after a restore, the page loads the file', () => {
    const ia = cut('  function interpretAll() {', '    // what a line was read as is kept for the lines on the list');
    ok(/row\.spec = readLine\(row\)[\s\S]*window\.Engrave\?\.raiseCandidate\?\.\(row\);[^\n]*\n\s*window\.CNBarFront\?\.move\?\.\(row\);/.test(ia), 'interpretAll: after the line is read (and raised), the bar rule\'s move');
    ok(/if \(recovered\) setTimeout\(\(\) => \{ Promise\.resolve\(window\.CNBarFront\?\.afterRestore\?\.\(\)\)/.test(bridgeSrc), 'the boot asks after a restore');
    const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8'), build = fs.readFileSync(path.join(root, 'scripts/build-public.cjs'), 'utf8');
    ok(/<script src="charm-nest-engrave-bar-front\.js\?v=[^"]+"><\/script>/.test(html), 'charm-nest-1.html loads it'); ok(build.includes('"charm-nest-engrave-bar-front.js"'), 'the build ships it');
    eq(typeof O.barNecklace, 'function', 'the reader tells a bar line');
  });

  const failed = results.filter(r => r[1]);
  console.log(`${results.length - failed.length}/${results.length} passed, ${checks} checks`);
  if (failed.length) process.exit(1);
})();
