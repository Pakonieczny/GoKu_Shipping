// NOWORTH (Paul, 18 Oct 2026, of the engraving card's "WORTH A LOOK" note: 'Staff message gives the engraving as "diabetic" with no customer text on
// the order — confirm wording and capitalisation?'): "Remove this info/note from all UI in the app". Offline: no browser, no Etsy, no paid call.
//   node tests/charm-nest/engraving-no-worth-note.cjs
// What it proves:
//   · the engraving card (charm-nest-engraving-seals.js panel(): the order window's Overview and Engraving section, the Sheet tab, the sheet window) draws no
//     "Worth a look" / "What is unclear" list in any state, nor the reader's questions, nor what it says the buyer asked for (side, font, handwriting, image)
//   · the words still wait for a person: a "words" card still offers no approval (the button is there, off, with its one line of why)
//   · the Engrave tab's detail and the Review tab's placement card no longer list Claude's questions (nor their "Ask" button), the Review feed's words card
//     has no "Claude asks" row, and the message box's placeholder is never the reader's question; the dead CSS is gone
//   · the reader still stores its questions and they still send the words to a person (nothing about the decisions changed)
const assert = require('assert');
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const SEALS = require('../../charm-nest-engraving-seals.js');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const src = f => fs.readFileSync(path.join(root, f), 'utf8');

const Q = 'Staff message gives the engraving as "diabetic" with no customer text on the order — confirm wording and capitalisation?';
const NOTE = /Worth a look|What is unclear|egUnclear|Staff message gives|Which side|Asked for (the|an|handwriting)|Requested font/i;
const job = (state, extra) => Object.assign({ key: 'k1', state, questions: [Q, 'Which side?'], reason: 'Check the requested inscription',
  requests: { side: 'front', font: 'Comic Sans', handwriting: true, image: true }, source: 'staffNote', confidence: .5,
  row: { spec: { font: { asked: 'Script', id: '' } } } }, extra || {});

// ── 1 · the engraving card, every state that used to show the list ──
for (const [name, e] of [
  ['words', { kind: 'words', job: job('words'), text: 'diabetic', reason: 'Check the requested inscription' }],
  ['words, no text', { kind: 'words', job: job('words'), text: '' }],
  ['approve', { kind: 'approve', job: job('review', { fit: {}, view: {} }), text: 'Diabetic' }],
  ['approve, compact', { kind: 'approve', job: job('review', { fit: {}, view: {} }), text: 'Diabetic', compact: true }],
  ['preparing', { kind: 'preparing', job: job('ready'), text: 'diabetic' }],
  ['approved', { kind: 'approved', job: job('approved'), text: 'Diabetic' }],
  ['skipped', { kind: 'skipped', job: job('skipped') }]]) {
  const h = SEALS.panel(e);
  ok(!NOTE.test(h), `${name}: no "Worth a look" / "What is unclear" note, no reader's question: ${(h.match(NOTE) || [])[0]}`);
  ok(!/<ul>/.test(h), `${name}: no note list at all`);
}
{ const h = SEALS.panel({ kind: 'words', job: job('words'), text: 'diabetic' });
  ok(/egApproveButton egAsk egOff" disabled/.test(h) && /Confirm the words in Design &(?:amp;)? Engrave first/.test(h), 'words to confirm are still never approved on the card (the button is off, with its why)');
  ok(/data-e="engrave"/.test(h) && /Fix in Design &(?:amp;)? Engrave/.test(h), 'and Fix in Design & Engrave still takes the person to the decision');
  ok(/class="words">diabetic</.test(h), 'the words as read are still shown'); }

// ── 2 · the other surfaces (charm-nest-bridge.js: Engrave tab detail, Review placement card, Review feed card; charm-nest-mail.js: the message box) ──
const bridge = src('charm-nest-bridge.js'), mail = src('charm-nest-mail.js');
ok(!/class="pvAsk"/.test(bridge) && !/data-ask=/.test(bridge) && !/CustomerMail\?\.cardAsk/.test(bridge), 'the placement card lists no Claude question and has no "Ask" button');
ok(/\$\{wants\.length \? `<section class="pvSec pvCheck">/.test(bridge), 'its "To check with the buyer" list keeps only what the buyer asked for');
ok(!/evRow\("Claude asks"/.test(bridge), 'the Review feed\'s words card has no "Claude asks" row');
ok(!/"e\.g\. " \+ qs\[0\]/.test(mail) && /function suggestion\(\) \{ return "Ask about this engraving…"; \}/.test(mail), 'the message box placeholder is never the reader\'s question');
ok(!/^[^/\n]*\.egUnclear/m.test(src('charm-nest-activity.css')) && !/\.pvAsk\b/.test(src('charm-nest-1.html')), 'the dead CSS is gone');
// (display only: the reader still stores its questions, and a question still sends the words to a person)
ok(/job\.questions = r\.questions/.test(bridge) && /\|\| job\.questions\.length\)\s*\n\s*return toWords\(job, "Check the requested inscription"\)/.test(bridge), 'the reader still stores its questions and they still send the words to a person');

console.log(`engraving-no-worth-note: ${n} checks passed`);
