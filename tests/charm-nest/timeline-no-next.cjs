// The step cards say nothing of what is NEXT (Paul, 12 Oct 2026: "I don't need any of the information about what is next since that is what the timeline
// bar is for. Please remove that from all state menus in the timeline"). Pure: charm-nest-timeline-ui.js in a vm, no page, no network.
//   1 · requirementsOf, for each of the 8 steps of an order (arrived, sheet, engraved, laser, sorted, welded, assembled, shipped), at every place the order can
//       be (nothing done, then each step done), held, cancelled, completed by hand, with and without the host's sheets: no row of kind "next" or "after", and no
//       row that says what will happen ("its set is committed once every sheet in it is ready", "the laser cuts the sheet and marks it cut", "<sheet> is ready: commit
//       its set and cut it", "scan its QR label at the Sorting station", "weld the studs", ...); every row that is left is one of wait, person, stop, label or was
//   2 · the card's own markup (the one explainOn draws over the "Where it is now" seal): no "Next" or "To come" chip, no "Step n of m" footer, no "click to open" text
//   3 · what still shows: a held order says so (Needs a person), a piece with no charm says so, the order's own engravings to approve say so, "Not on a sheet now"
//       and a done step keep their words, and a cancelled order's steps say "Stopped" / "Won't happen"
//   4 · no source keeps the words: the HOW strings, the "next"/"after" kinds and the chip words are gone from charm-nest-timeline-ui.js
//   node tests/charm-nest/timeline-no-next.cjs
const fs = require('fs'), path = require('path'), vm = require('vm'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const src = fs.readFileSync(path.join(root, 'charm-nest-timeline-ui.js'), 'utf8');

function load() {
  const win = { document: { getElementById: () => null, createElement: () => ({}), head: { appendChild() {} } } };
  win.window = win; vm.createContext(win); vm.runInContext(src, win);
  return win.OrderTimelineUI;
}
const UI = load();

const HOW_WORDS = /committed once every sheet|laser cuts the sheet|commit its set|is ready:|scan its QR label|weld the studs|assemble it, then scan|pack it, print|nests it on a sheet at its next pass|back engraving is read, fitted and approved|at its station|Etsy marks the order complete/i;
const KINDS_OK = new Set(['wait', 'person', 'stop', 'label', 'was']);

const T0 = 1.7e12;
const ev = (type, i, extra) => Object.assign({ id: `4201047416~${type}~e${i}`, orderId: '4201047416', type, at: T0 + i * 60000, by: 'Test Operator', source: 'sorter', station: '', device: '', text: '' }, extra || {});
const STEPS = [
  ev('arrived', 1, { source: 'etsy', by: 'Etsy' }),
  ev('placed', 2, { sheet: 'GF Draft 3', sheetId: 'sh-g3' }),
  ev('engraveApproved', 3, { by: 'Ana P.' }),
  ev('laserDone', 4, { sheet: 'GF Draft 3', sheetId: 'sh-g3' }),
  ev('sorted', 5, { station: 'sorting' }),
  ev('welded', 6, { station: 'welding' }),
  ev('assembled', 7, { station: 'assembly' }),
  ev('shipped', 8, { station: 'shipping' })
];
// what the host knows of the order: a sheet that is not ready (engravings to approve, QR labels to print), a sheet that is ready, a held piece, a piece with no charm
const SHEETS = [
  { name: 'GF Draft 3', placed: 24, pct: 61, stages: { layout: true, front: true, approval: false, backs: false, qr: false }, waiting: 19, saved: 0, required: 24, ready: false },
  { name: 'GF Sheet 1', placed: 12, pct: 80, stages: { layout: true, front: true, approval: true, backs: true, qr: true }, ready: true }
];
const LINES = [
  { sku: 'LEAPING 40171', form: 'necklace', onSheet: true, engrave: { needed: true, approved: false, text: 'Mia' } },
  { sku: 'POLICE 10814', form: 'necklace', onSheet: false, hold: true, reason: 'Buyer asked to wait' },
  { sku: 'CHAIN 6190', form: 'bracelet', onSheet: false, state: 'unmatched', reason: 'no charm for this SKU' }
];
const CONTEXTS = [{ lines: [], sheets: [] }, { lines: LINES, sheets: SHEETS }, { lines: LINES.slice(0, 1), sheets: SHEETS.slice(0, 1) }, { lines: [], sheets: SHEETS }];
const KEYS = ['arrived', 'sheet', 'engraved', 'laser', 'sorted', 'welded', 'assembled', 'shipped'];

let cards = 0;
for (let upTo = 0; upTo <= STEPS.length; upTo++) {                 // the order has done the first `upTo` steps
  for (const mode of ['plain', 'held', 'cancelled', 'hand']) {
    const events = STEPS.slice(0, upTo).concat(mode === 'held' ? [ev('held', 20, { text: 'On hold: buyer asked to wait' })] : mode === 'hand' ? [ev('sealCompleted', 21, { data: { how: 'button' } })] : []);
    for (const context of CONTEXTS) {
      for (const k of KEYS) {
        const q = UI.requirementsOf(k, { events, context, cancelled: mode === 'cancelled' ? { at: T0 + 30 * 60000, by: 'Paul' } : null });
        if (!q) continue; cards++;
        for (const n of q.need) {
          assert(KINDS_OK.has(n.kind), `${k} @${upTo} ${mode}: row kind "${n.kind}" (${n.t}) is not one of wait / person / stop / label / was`);
          assert.doesNotMatch(n.t, HOW_WORDS, `${k} @${upTo} ${mode}: a row says what will happen: ${n.t}`);
        }
        for (const f of q.facts) assert.doesNotMatch(f, HOW_WORDS, `${k} @${upTo} ${mode}: a fact says what will happen: ${f}`);
      }
    }
  }
}
console.log(`  ✓ 1 · requirementsOf: ${cards} cards (8 steps × every place × plain / held / cancelled / completed by hand × 4 host contexts) have no "next" or "after" row and no row about what will happen`);

// 2 · the card's markup is built in one place (reqCard) from the chip word and the rows: read from the source (the browser suites hover every step)
assert.doesNotMatch(src, /Step \$\{[^}]*\} of \$\{/, 'the card has no "Step n of m" footer');
assert.doesNotMatch(src, /click to open it on the Timeline|click seal to open on the Timeline/, 'the card has no "click to open" text');
assert.match(src, /const STATE_WORD = \{ done: "Done", stopped: "Stopped here", gone: "Won't happen", none: "Not needed", skipped: "Skipped" \};/, 'the chip words: no "Next", no "To come"');
console.log('  ✓ 2 · the card has no "Next" / "To come" chip, no "Step n of m" footer and no "click to open" text');

// 3 · what still shows
{
  const ev3 = STEPS.slice(0, 2);
  const eng = UI.requirementsOf('engraved', { events: ev3, context: { lines: LINES, sheets: [] } });
  assert(eng.need.some(n => n.kind === 'person' && /Approve the back engraving .*Mia.* of SKU LEAPING 40171/.test(n.t)), 'the order\'s own engraving to approve: ' + JSON.stringify(eng.need));
  const nested = UI.requirementsOf('sheet', { events: STEPS.slice(0, 1), context: { lines: LINES, sheets: [] } });
  assert(nested.need.some(n => n.kind === 'person' && /POLICE 10814.* is held/.test(n.t)) && nested.need.some(n => n.kind === 'person' && /no charm/.test(n.t)), 'a held piece and a piece with no charm stay: ' + JSON.stringify(nested.need));
  const held = UI.requirementsOf('laser', { events: STEPS.slice(0, 2).concat(ev('held', 20, { text: 'On hold: waiting for the buyer' })), context: { lines: [], sheets: [] } });
  assert(held.need.some(n => n.kind === 'person' && /^On hold/.test(n.t)), 'a hold stays: ' + JSON.stringify(held.need));
  const done = UI.requirementsOf('arrived', { events: STEPS.slice(0, 2), context: { lines: [], sheets: [] } });
  assert.equal(done.state, 'done'); assert(done.done.length && !done.need.length, 'a done step with nothing wrong keeps its who and when and says nothing else: ' + JSON.stringify([done.done, done.need]));
  const cx = UI.requirementsOf('sorted', { events: STEPS.slice(0, 4), cancelled: { at: T0 + 30 * 60000, by: 'Paul' } });
  assert(cx.need.some(n => n.kind === 'stop'), 'a cancelled order\'s step says it will not happen: ' + JSON.stringify(cx.need));
  const later = UI.requirementsOf('shipped', { events: STEPS.slice(0, 2), context: { lines: [], sheets: [] } });
  assert.equal(later.state, 'later'); assert.equal(later.need.filter(n => n.kind !== 'label').length, 0, 'a step further on, with nothing wrong, has no row at all (no "After Laser cut"): ' + JSON.stringify(later.need));
}
console.log('  ✓ 3 · a hold, a held piece, a piece with no charm, the order\'s own engraving to approve, Stopped and a done step\'s who/when still show; a step with nothing wrong has no row');

// 4 · the words are gone from the source
for (const [re, why] of [[/add\("next"/, 'no add("next", ...) call'], [/add\("after"/, 'no add("after", ...) call'], [/HOW\.(sheet|engraved|laser|sorted|welded|assembled|shipped|approved|completed)\b/, 'no HOW.<step> string'], [/REQ_WORD = \{[^}]*\b(next|after)\b/, 'no "next"/"after" word for a row'], [/\.tlCnt|tlCntIn/, 'no count pill CSS or markup']]) assert.doesNotMatch(src, re, why);
console.log('  ✓ 4 · charm-nest-timeline-ui.js keeps no "next" / "after" row, HOW string or count pill');
console.log('Timeline no next OK');
