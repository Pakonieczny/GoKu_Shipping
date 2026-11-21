// ENGNOTE on the PAGE (Paul, 20 Nov 2026: "Yes, use ENG: notes"): a staff message "ENG: <words>" is the engraving words of the order's one line that takes them. The page's own code, offline:
// the shared reading (charm-nest-engrave-classify.js, loaded as the page loads it: a global) under the bridge's real doors (charm-nest-bridge.js, the classify slice, run in a vm),
// with the page's Orders.rows and Engrave.jobsOf stood in by the test, the paid reader stubbed and counted. The shape of order 4235295893 (made-up words and names):
//   1  the order's lines: a calendar charm whose option says "CHARM + Engraving" and whose personalisation is a note to the shop (waiting for a person, the note drawn as its words),
//      a decorative charm with a note about its size (waiting for a person), a necklace charm with nothing to engrave, a necklace charm a person cut plain; the staff messages
//      "MAKE CHAIN 16"", "DATE FOR CALENDAR: …", "ENG: Happy Birthday Pretty"
//   2  a read of each line (Engrave.raiseCandidate, as Orders.interpretAll asks it) and the classify pass: the calendar takes "Happy Birthday Pretty", ready, the reason names the note's
//      writer, no paid reading, the buyer's note no longer its words; the decorative charm keeps waiting; the plain and the person's lines are untouched
//   3  a newer note ("eng : Happy Birthday Mom") replaces the older one on the calendar
//   4  an order with two lines that could take the words (both "CHARM + Engraving"): neither is guessed: each shows the note's words to confirm; a BLANK line takes nothing
//   3b a note never jumps: the calendar APPROVED by a person with "Happy Birthday Pretty", then a newer "ENG: Happy Birthday Mom": the calendar is left exactly as it is and its card says, in words, that
//      a newer staff note differs ("Newer staff note by Gina S.: … — the approved words were kept"); the decorative charm is NOT given the words (still waiting for a person)
//   3c after the fit clears the reason line, the words panel still says where the words came from: "From the staff note by Gina S." (CharmNestEngraveSetup.read, the panel's html, Faces.panelRows)
//   5  the page loads the module before the bridge, with a new cache token; the bridge asks raiseCandidate of every line it reads
//   node tests/charm-nest/engrave-eng-note.cjs
'use strict';
const assert = require('assert/strict'), fs = require('fs'), path = require('path'), vm = require('vm');
const root = path.join(__dirname, '../..');
let pass = 0, fail = 0;
const ta = async (name, fn) => { try { await fn(); pass++; console.log('  ok   ' + name); } catch (e) { fail++; console.log('  FAIL ' + name + '\n       ' + String(e && e.stack || e).split('\n').slice(0, 6).join('\n       ')); } };
const V = (name, value) => ({ name, value });
const O = require(path.join(root, 'charm-nest-orders.js'));
const bridgeSrc = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8'), slice = bridgeSrc.slice(bridgeSrc.indexOf('  const classifyTasks ='), bridgeSrc.indexOf('  function setNone(job'));
const moduleSrc = fs.readFileSync(path.join(root, 'charm-nest-engrave-classify.js'), 'utf8');

/** The page's world for one test: its rows, its job map, the reader stubbed and counted. */
function world(rows) {
  const calls = { agent: 0 }, map = new Map();
  const ctx = { O, CharmNestEngraveRead: require(path.join(root, 'charm-nest-engrave-read.js')), CharmNestEngraveAsks: require(path.join(root, 'charm-nest-engrave-asks.js')), FACES: () => null, SIDES: () => require(path.join(root, 'charm-nest-engrave-sides.js')),
    neverEngraves: () => null, NEVER_OPEN: () => true, NEVER_WHY: 'never', CNEngravingSeals: require(path.join(root, 'charm-nest-engraving-seals.js')), render() {}, S: { settings: { engraveConfidence: 0.65 } },
    items: () => map, faceOf: () => 'back', agent() {}, console, window: {},
    Orders: { rows: () => rows }, Engrave: { jobsOf: r => (r.jobs || []).filter(j => map.get(j.key) === j) },
    ensureJobs: row => { if (!row.jobs) { row.jobs = [{ key: row.key, row, state: 'classify', text: null, lines: [], copies: [] }]; } for (const j of row.jobs) map.set(j.key, j); return row.jobs; },
    ensureJob: row => ctx.ensureJobs(row)[0],
    agentCall: async () => { calls.agent++; return { skipped: 'offline' }; },
    setReady: async j => { j.state = 'ready'; j.reason = null; j.row.engrave = Object.assign({}, j.row.engrave, { needed: true, state: 'ready', text: j.text, approved: false }); return j; },
    toWords: (j, why) => { j.state = 'words'; j.reason = why; j.row.engrave = Object.assign({}, j.row.engrave, { needed: true, state: 'words', text: j.text, approved: false, reason: why }); return j; },
    setNone: (j, why) => { j.state = 'none'; j.reason = why; j.row.engrave = Object.assign({}, j.row.engrave, { needed: false, state: 'none', approved: true }); return j; } };
  vm.createContext(ctx); vm.runInContext(moduleSrc, ctx); vm.runInContext(slice, ctx);
  for (const r of rows) for (const j of r.jobs || []) { j.row = r; map.set(j.key, j); }
  return { ctx, calls, map };
}
const msgs = extra => [{ senderName: 'Gina S.', text: 'MAKE CHAIN 16"', at: 1000 }, { senderName: 'Gina S.', text: 'DATE FOR CALENDAR: March 9th, 2004', at: 2000 }, { senderName: 'Gina S.', text: 'ENG: Happy Birthday Pretty', at: 3000 }].concat(extra || []);
const NOTE_CAL = 'only the month of March and the 9th marked as in the photo please', NOTE_DEC = 'please make this the center piece and tell me if it should be larger than 12mm';
function line(rid, tid, sku, opts, pers, messages, extra) {
  const o = { receiptId: rid, updateTs: 0, buyerMessage: '', staffNote: '', messages }, l = { transactionId: tid, listingId: '1', sku, title: sku, quantity: 1, metalKey: 'gold', metalLabel: 'Gold Filled', personalization: pers, variations: [V('Metal Choice', 'Gold Filled')].concat(opts) };
  const ent = { sku, members: 2, holes: 1, backKeepOut: [], widthPt: 30, heightPt: 30 };
  const spec = O.interpretLine(o, l, { optionMaps: {}, aliases: {}, noDesign: {}, listingSkus: {}, masterEntry: () => ent, masterLoose: () => '' });
  assert.deepEqual(JSON.parse(JSON.stringify(spec.messages)), JSON.parse(JSON.stringify(messages)), 'the intake keeps the order\'s staff messages on the line');
  spec.messages = messages;   // (one array for the order's lines, so a message the test adds is on every line, as a new reading of the order gives it)
  return Object.assign({ key: rid + '_' + tid, order: o, line: l, spec, state: 'pooled' }, extra || {});
}
/** a job read before the note, waiting for a person with the buyer's note as its words (what the page shows today) */
const waiting = (r, text) => { r.jobs = [{ key: r.key, state: 'words', text, lines: [text], source: 'personalization', quote: text, confidence: 0, questions: ['Check the requested inscription'], reason: 'Check the requested inscription', copies: [] }]; r.engrave = { needed: true, state: 'words', text, approved: false }; return r; };

(async () => {
  console.log('1-2 · the order of 4235295893\'s shape');
  const M = msgs(), RID = '4399000001';
  const cal = waiting(line(RID, '43990000011', 'PERSONALIZED_23973', [V('Charm Type', 'CHARM + Engraving')], [NOTE_CAL], M), NOTE_CAL);
  const dec = waiting(line(RID, '43990000012', 'DECORATIVE_2519', [V('Charm Size', '12mm')], [NOTE_DEC], M), NOTE_DEC);
  const plainL = line(RID, '43990000013', 'GIRAFFE', [V('Charm Type', 'Necklace CHARM')], [], M);
  plainL.jobs = [{ key: plainL.key, state: 'none', reason: 'No engraving requested', text: '', lines: [], copies: [] }]; plainL.engrave = { needed: false, state: 'none', approved: true, seals: [] };
  const person = line(RID, '43990000014', 'DONUT 1', [V('Charm Type', 'Necklace CHARM')], [], M);
  person.jobs = [{ key: person.key, state: 'none', reason: 'no engraving — decided by Ana', decision: { by: 'Ana', at: 500, none: true }, copies: [] }]; person.engrave = { needed: false, state: 'none', approved: true, seals: [{ id: 'engravePlain:500:Ana', how: 'engravePlain', at: 500, by: 'Ana' }] };
  const rows = [cal, dec, plainL, person], w = world(rows), snap = r => JSON.stringify(r.jobs.map(j => Object.assign({}, j, { row: undefined })));
  const decWas = snap(dec), plainWas = snap(plainL), personWas = snap(person);
  await ta('a read of each line (raiseCandidate, as interpretAll asks it): the calendar waiting for a person is set to be read again; the others are not', async () => {
    for (const r of rows) w.ctx.raiseCandidate(r);
    assert.equal(cal.jobs[0].state, 'reclassify'); assert.equal(snap(dec), decWas); assert.equal(snap(plainL), plainWas); assert.equal(snap(person), personWas);
  });
  await ta('the classify pass: the calendar takes the ENG words "Happy Birthday Pretty", ready, NO paid reading; its reason names the note and its writer; the buyer\'s note is no longer its words', async () => {
    await w.ctx.classify(cal);
    const j = cal.jobs[0];
    assert.equal(w.calls.agent, 0); assert.equal(j.state, 'ready'); assert.equal(j.text, 'Happy Birthday Pretty'); assert.deepEqual(Array.from(j.lines), ['Happy Birthday Pretty']);
    assert.equal(j.wordsSource, 'staffNote:eng'); assert.equal(j.source, 'staffNote'); assert.match(j.reason, /staff note by Gina S\./); assert.match(cal.engrave.reason, /staff note by Gina S\./);
    assert.ok(!/March|photo/.test(j.text) && cal.engrave.text === 'Happy Birthday Pretty', 'the preview draws the staff\'s words, not the buyer\'s note');
  });
  await ta('the decorative charm keeps waiting for a person (its own words card), the plain line and the line Ana cut plain are untouched; another read changes nothing', async () => {
    for (const r of rows) w.ctx.raiseCandidate(r);
    assert.equal(snap(dec), decWas); assert.equal(dec.jobs[0].state, 'words'); assert.equal(snap(plainL), plainWas); assert.equal(snap(person), personWas); assert.equal(cal.jobs[0].state, 'ready');
    assert.equal(w.calls.agent, 0);
  });
  console.log('3 · a newer note');
  await ta('a newer note ("eng : Happy Birthday Mom", later) replaces the older one on the calendar at the next read; still no paid reading', async () => {
    M.push({ senderName: 'Gina S.', text: 'eng : Happy Birthday Mom', at: 4000 });
    for (const r of rows) w.ctx.raiseCandidate(r);
    assert.equal(cal.jobs[0].state, 'reclassify'); assert.equal(dec.jobs[0].state, 'words');
    await w.ctx.classify(cal);
    assert.equal(cal.jobs[0].state, 'ready'); assert.equal(cal.jobs[0].text, 'Happy Birthday Mom'); assert.equal(w.calls.agent, 0);
  });
  await ta('a person\'s decision on the calendar is never overridden by a note', async () => {
    cal.jobs[0].state = 'approved'; cal.jobs[0].approvedAt = 9000; cal.jobs[0].approvedBy = 'Ana'; const was = snap(cal);
    M.push({ senderName: 'Gina S.', text: 'ENG: Something Else', at: 5000 });
    for (const r of rows) w.ctx.raiseCandidate(r);
    assert.equal(snap(cal), was);
  });
  await ta('3c · after the fit clears the reason line, the words panel still says the words came from the staff note by Gina S. (setup, panel, Faces.panelRows)', async () => {
    const Setup = require(path.join(root, 'charm-nest-engrave-setup.js')), Panel = require(path.join(root, 'charm-nest-engrave-panel.js')), Faces = require(path.join(root, 'charm-nest-engrave-faces.js'));
    const R3 = '4399000003', M3 = msgs(), c3 = line(R3, '43990000031', 'PERSONALIZED_23973', [V('Charm Type', 'CHARM + Engraving')], [NOTE_CAL], M3);
    c3.jobs = [{ key: c3.key, row: c3, state: 'review', text: 'Happy Birthday Pretty', lines: ['Happy Birthday Pretty'], source: 'staffNote', quote: 'ENG: Happy Birthday Pretty', wordsSource: 'staffNote:eng', confidence: 1, reason: null, copies: [] }];   // (fitted: the fit set reason to null)
    c3.engrave = { needed: true, state: 'review', text: 'Happy Birthday Pretty', approved: false };
    const vm = Setup.read(c3, { jobs: c3.jobs, orderRows: () => [c3] });
    assert.equal(vm.rows[0].staffBy, 'Gina S.'); assert.equal(vm.from, 'from the staff note by Gina S.'); assert.equal(vm.staffNotice, '');
    const html = Panel.html(vm, { key: c3.key, choice: vm.mode, mode: vm.mode, active: 'back', faces: { back: { text: 'Happy Birthday Pretty', font: '' }, front: { text: '', font: '' } } }, {});
    assert.match(html, /From the staff note by Gina S\./); assert.doesNotMatch(html, /Newer staff note/);
    assert.equal(Faces.panelRows(Faces.setupOf(c3), c3.jobs)[0].from, 'staffNote', 'Faces.panelRows says "staffNote", not "reader"');
    assert.equal(Setup.autoChoice(c3, null, { jobs: c3.jobs }).back.from, 'staffNote'); assert.equal(Setup.autoChoice(c3, null, { jobs: c3.jobs }).back.by, 'Gina S.');
  });
  await ta('3b · a note never jumps: the calendar APPROVED with "Happy Birthday Pretty", then a newer "ENG: Happy Birthday Mom": the calendar unchanged and its card says the newer note differs; the decorative charm NOT given the words', async () => {
    const Setup = require(path.join(root, 'charm-nest-engrave-setup.js')), Panel = require(path.join(root, 'charm-nest-engrave-panel.js'));
    const R4 = '4399000004', M4 = msgs();
    const c4 = line(R4, '43990000041', 'PERSONALIZED_23973', [V('Charm Type', 'CHARM + Engraving')], [NOTE_CAL], M4), d4 = waiting(line(R4, '43990000042', 'DECORATIVE_2519', [V('Charm Size', '12mm')], [NOTE_DEC], M4), NOTE_DEC);
    const g4 = line(R4, '43990000043', 'GIRAFFE', [V('Charm Type', 'Necklace CHARM')], [], M4); g4.jobs = [{ key: g4.key, state: 'none', reason: 'No engraving requested', text: '', lines: [], copies: [] }]; g4.engrave = { needed: false, state: 'none', approved: true, seals: [] };
    c4.jobs = [{ key: c4.key, state: 'approved', text: 'Happy Birthday Pretty', lines: ['Happy Birthday Pretty'], source: 'staffNote', quote: 'ENG: Happy Birthday Pretty', wordsSource: 'staffNote:eng', confidence: 1, approvedAt: 3500, approvedBy: 'Ana', copies: [] }];
    c4.engrave = { needed: true, state: 'approved', text: 'Happy Birthday Pretty', approved: true, approvedBy: 'Ana', approvedAt: 3500 };
    const rows4 = [c4, d4, g4], w4 = world(rows4), was = [c4, d4, g4].map(snap);
    for (const r of rows4) w4.ctx.raiseCandidate(r);
    assert.deepEqual([c4, d4, g4].map(snap), was, 'the older note: nothing to do');
    M4.push({ senderName: 'Gina S.', text: 'ENG: Happy Birthday Mom', at: 4000 });
    for (const r of rows4) w4.ctx.raiseCandidate(r);
    assert.deepEqual([c4, d4, g4].map(snap), was, 'the calendar, the decorative charm and the giraffe are all exactly as they were');
    assert.equal(d4.engrave.state, 'words', 'the decorative charm is not set to be read again: the classify pass (classify / reclassify only) does not take it up');
    assert.equal(d4.jobs[0].text, NOTE_DEC); assert.equal(d4.jobs[0].state, 'words'); assert.equal(w4.calls.agent, 0);
    const vm = Setup.read(c4, { jobs: c4.jobs, orderRows: () => rows4.map(r => Object.assign(r, {})) });
    assert.equal(vm.staffNotice, 'Newer staff note by Gina S.: “Happy Birthday Mom” — the approved words were kept');
    const html = Panel.html(vm, { key: c4.key, choice: vm.mode, mode: vm.mode, active: 'back', faces: { back: { text: 'Happy Birthday Pretty', font: '' }, front: { text: '', font: '' } } }, {});
    assert.match(html, /Newer staff note by Gina S\.: “Happy Birthday Mom” — the approved words were kept/, 'the card\'s panel says it');
    assert.equal(Setup.read(d4, { jobs: d4.jobs, orderRows: () => rows4 }).staffNotice, '', 'the decorative charm\'s card says nothing of the note');
  });
  console.log('4 · two lines that could take the words');
  await ta('both "CHARM + Engraving": neither is guessed: each shows the note\'s words to confirm (no paid reading); a BLANK line takes nothing', async () => {
    const M2 = [{ senderName: 'Gina S.', text: 'ENG:  Love you\nAlways ', at: 100 }], R2 = '4399000002';
    const a = waiting(line(R2, '43990000021', 'PERSONALIZED_23973', [V('Charm Type', 'CHARM + Engraving')], [NOTE_CAL], M2), NOTE_CAL);
    const b = waiting(line(R2, '43990000022', 'PERSONALIZED_23974', [V('Charm Type', 'CHARM + Engraving')], ['the same as the other please'], M2), 'the same as the other please');
    const blank = line(R2, '43990000023', 'BAR_1548', [V('Metal · #Characters?', 'GOLD·Set·BLANK')], [], M2);
    const w2 = world([a, b, blank]);
    for (const r of [a, b, blank]) w2.ctx.raiseCandidate(r);
    await w2.ctx.classify(a); await w2.ctx.classify(b);
    for (const r of [a, b]) { const j = r.jobs[0]; assert.equal(j.state, 'words'); assert.equal(j.text, 'Love you\nAlways', 'the writer\'s line break kept, the spaces trimmed'); assert.equal(j.wordsSource, 'staffNote:suggest'); assert.match(j.reason, /does not say which line/); }
    assert.equal(w2.calls.agent, 0); assert.ok(!blank.jobs, 'the BLANK line got no job and no words');
    const was = snap(a); for (const r of [a, b, blank]) w2.ctx.raiseCandidate(r); assert.equal(snap(a), was, 'shown once: another read changes nothing');
  });
  console.log('5 · the page');
  await ta('charm-nest-1.html loads the module before the bridge with a new cache token (and the setup, faces and panel modules with theirs); the bridge asks raiseCandidate of every line it reads (Orders.interpretAll)', async () => {
    const h0 = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
    for (const f of ['charm-nest-engrave-setup.js', 'charm-nest-engrave-faces.js', 'charm-nest-engrave-panel.js']) assert.match(h0, new RegExp(f.replace(/\./g, '\\.') + '\\?v=[^"]*-eng2"'), f);
    const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
    assert.ok(html.indexOf('charm-nest-engrave-classify.js?v=1120-classify1-eng1-eng2"') > 0 && html.indexOf('charm-nest-engrave-classify.js?v=') < html.indexOf('<script src="charm-nest-bridge.js?v='));
    assert.ok(/window\.Engrave\?\.raiseCandidate\?\.\(row\);/.test(bridgeSrc));
  });
  console.log(fail ? `\nFAILED: ${pass} passed, ${fail} failed` : `\nPASSED: ${pass} passed, 0 failed`);
  process.exitCode = fail ? 1 : 0;
})();
