// ENGUNDO (engrave-ui-1012, Paul's point 8: "Remove the 'Undo my edit...' button."): the Engrave tab's options panel has NO Undo / revert control and no "Undo my edit" string exists
// anywhere in the shipped browser files. Offline and pure: the panel markup is built from neutral fixtures with CharmNestEngraveSetup.html (a string builder, no browser, no network, no
// paid reading, no Firestore).
//   1  every state of the panel (not edited / edited by a person / open for editing / waiting to be saved / a face to cut plain / cut / words to confirm / a wait) holds no Undo or
//      revert control and none of the old words ("Undo my edit", "back to the system's reading", "Undo and cut it plain")
//   2  what stays: Edit on the quiet line, Save and Cancel in the open panel, the mode choices, the words box and the font drop-down; the stored override (row.engravingOverride) is
//      still read, still wins over the system's reading, and a Save still stores one (buildOverride) - nothing about stored data changed
//   3  the shipped files: no "Undo my edit" / "back to the system's reading" / data-su="undo" in any browser file, no "undo" at all in charm-nest-engrave-setup.js, no undoSetup in the bridge,
//      and the save path has no undo option (an old { undo: true } call stores an override like any save, it never clears one)
//   node tests/charm-nest/engrave-no-undo.cjs
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const F = require(path.join(root, 'charm-nest-engrave-faces.js'));
const S = require(path.join(root, 'charm-nest-engrave-setup.js'));
let n = 0;
const ok = (msg, cond) => { assert.ok(cond, msg); n++; };
const eq = (msg, a, b) => { assert.deepEqual(a, b, msg); n++; };

// the words no panel may carry any more (and any button that reverts a person's edit)
const OLD_WORDS = /undo|revert|back to the system|my edit|take (my|the) edit back|system's (own )?reading/i;
const OLD_ATTRS = /data-su="?undo/i;
const textOf = html => html.replace(/<[^>]*>/g, ' ').replace(/&#39;/g, "'").replace(/&amp;/g, '&').replace(/\s+/g, ' ').trim();
const buttonsOf = html => [...html.matchAll(/<button\b[^>]*>[\s\S]*?<\/button>/gi)].map(m => m[0]);
const noUndo = (what, html) => {
  ok(`${what}: markup is not empty`, html.length > 50);
  ok(`${what}: no data-su="undo" / "undoYes" control`, !OLD_ATTRS.test(html) && !/data-su="undoYes"/.test(html));
  const bad = buttonsOf(html).filter(b => OLD_WORDS.test(textOf(b)) || OLD_WORDS.test(b.replace(/>[\s\S]*$/, '')));   // (a button's own words, its title and its attributes)
  eq(`${what}: no button says undo / revert / back to the system's reading`, bad, []);
  ok(`${what}: no "Undo my edit" anywhere in the panel`, !/Undo my edit/i.test(html) && !/back to the system/i.test(html) && !/Undo and cut it plain/i.test(html));
};

// neutral fixtures (no buyer names, no personal text)
const LK = '4200000001_5000000000001';
const reading = (x = {}) => Object.assign({ v: 1, mode: 'both', source: 'option', front: { text: 'Name One', lines: ['Name One'], fontWord: null }, back: { text: 'Name Two', lines: ['Name Two'], fontWord: null }, textSource: 'labelled', needsPerson: false, confidence: 0.95, why: 'The listing asks for both faces', option: { name: 'Charm Type', value: 'Tag1 (front+back)' } }, x);
const mkLine = (o = {}) => ({ key: LK, state: 'pooled', order: { receiptId: '4200000001' }, line: { transactionId: '5000000000001' }, poolIds: [LK + '_1'],
  spec: Object.assign({ designSku: '5X8.5', engraveCandidate: true, personalization: ['front - Name One', 'back - Name Two'], font: { asked: 'Ace', id: 'jost', name: 'Jost', source: 'option' },
    tagOption: { name: 'Charm Type', value: 'Tag1 (front+back)', tag: 1, mode: 'both', shape: 'plain rounded rectangle', sku: '5X8.5', how: 'rule:tag-shape' }, engraving: reading() }, o.spec || {}), ...(o.row || {}) });
const edited = (o = {}) => mkLine({ row: Object.assign({ engravingOverride: { mode: 'both', front: { text: 'Name Three', lines: ['Name Three'], fontWord: 'Vibur' }, by: 'Test Operator', at: 1760190000000 } }, o) });
const reset = () => { S.DRAFTS.clear(); S.NOTES.clear(); };
const job = (face, state, lines, extra = {}) => Object.assign({ key: F.jobKey(LK, null, face), face, slot: null, state, lines: lines.slice(), text: lines.join('\n') }, extra);

/* 1 + 2 ── every state of the panel, from fixtures */
{ reset();
  const states = {
    'not edited, quiet line': () => S.html(mkLine(), { jobs: [] }),
    'not edited, open for editing': () => S.html(mkLine(), { jobs: [], open: true }),
    'edited by a person, quiet line': () => S.html(edited(), { jobs: [] }),
    'edited by a person, open for editing': () => S.html(edited(), { jobs: [], open: true }),
    'edited by a person, compact': () => S.html(edited(), { jobs: [], compact: true }),
    'edited, a face already approved (cut plain asked first)': () => { const line = edited(), jobs = [job('front', 'review', ['Name Three']), job('back', 'approved', ['Name Two'], { approvedBy: 'Test Operator' })];
      S.html(line, { jobs, open: true }); const d = S.DRAFTS.get(LK); d.mode = 'front'; d.dirty = true; d.confirm = 'save'; d.confirmLines = ['The back is already approved by Test Operator: saving cuts it plain.']; return S.html(line, { jobs, open: true }); },
    'edited, waiting on a save': () => { const line = edited(); S.html(line, { jobs: [], open: true }); S.DRAFTS.get(LK).busy = 'Saving…'; return S.html(line, { jobs: [], open: true }); },
    'edited, a failed save': () => { const line = edited(); S.html(line, { jobs: [], open: true }); S.DRAFTS.get(LK).error = 'Not saved. Press Save to try again.'; return S.html(line, { jobs: [], open: true }); },
    'edited, line already cut': () => S.html(edited({ state: 'written' }), { jobs: [job('front', 'written', ['Name Three']), job('back', 'written', ['Name Two'])] }),
    'words to confirm': () => S.html(mkLine({ spec: { engraving: reading({ needsPerson: true, textSource: 'unlabelled', front: null, back: null }) } }), { jobs: [] }),
    'back only': () => S.html(edited({ engravingOverride: { mode: 'back', back: { text: 'Name Two', lines: ['Name Two'], fontWord: null }, by: 'Test Operator', at: 1 } }), { jobs: [], open: true }),
  };
  for (const [name, build] of Object.entries(states)) { reset(); noUndo(name, build()); }

  // what stays in the panel: Edit on the quiet line; Save, Cancel, the mode choices, the words box and the font drop-down when open
  reset(); const quiet = S.html(edited(), { jobs: [] });
  ok('the quiet line still offers Edit', buttonsOf(quiet).some(b => /^Edit$/.test(textOf(b))));
  reset(); const open = S.html(edited(), { jobs: [], open: true }), bs = buttonsOf(open).map(textOf);
  ok('Save is still there', bs.includes('Save')); ok('Cancel is still there', bs.includes('Cancel'));
  ok('the four mode choices are still there', ['None', 'Front only', 'Back only', 'Front + back'].every(t => bs.includes(t)));
  ok('the words box and the font drop-down are still there', /<textarea\b/.test(open) && /<select\b/.test(open));
  ok('a person\'s edit is still told (who set it)', /Set by Test Operator/.test(textOf(open)) || /Set by Test Operator/.test(textOf(quiet)));

  // the stored override still works: it is read, it wins over the system's reading, a Save still builds one
  reset(); const line = edited(), vm = S.read(line, { jobs: [] });
  ok('the stored override is still read', vm.edited === true && vm.mode === 'both');
  eq('and wins over the system\'s reading (front words, font)', [vm.rows[0].text, vm.rows[0].fontWord], ['Name Three', 'Vibur']);
  eq('buildOverride still stores a person\'s edit', S.buildOverride(mkLine(), { mode: 'front', front: { text: 'Name Four' } }, 'Test Operator', 5), { mode: 'front', front: { text: 'Name Four', lines: ['Name Four'], fontWord: null }, by: 'Test Operator', at: 5 });
  ok('the module offers no undo function', !Object.keys(S).some(k => /undo|revert/i.test(k)));
}

/* 3 ── the save path has no way back to the system's reading */
(async () => {
  reset();
  const line = edited(), stored = [], jobs = [job('front', 'review', ['Name Three'])];
  const env = { who: () => 'Test Operator', now: () => 7, store: (l, o) => { stored.push(o); l.engravingOverride = o; }, jobsOf: () => jobs, ensureJobs: () => jobs, slots: () => [null], remove: () => {}, skip: async () => {}, setNone: () => {}, reopen: () => {}, seed: async () => {}, changed: () => {} };
  const res = await S.apply(line, { mode: 'front', front: { text: 'Name Four' } }, env, { undo: true });   // (an old caller that still passes { undo: true })
  ok('an old { undo: true } call is an ordinary save', res.ok && stored.length === 1 && stored[0] && stored[0].front.text === 'Name Four');
  ok('and never clears the stored override', !!line.engravingOverride && line.engravingOverride.by === 'Test Operator');

  // every shipped browser file (and the server functions): none says "Undo my edit" / "back to the system's reading" or carries the old control
  const SKIP = new Set(['node_modules', '.git', '.claude', 'tests', 'plans', 'docs', 'backups', 'dist', 'build']);
  const files = [];
  (function walk(dir) { for (const e of fs.readdirSync(dir, { withFileTypes: true })) { if (e.isDirectory()) { if (!SKIP.has(e.name)) walk(path.join(dir, e.name)); } else if (/\.(html|js|cjs|mjs|css|json)$/i.test(e.name)) files.push(path.join(dir, e.name)); } })(root);
  ok('the scan saw the browser files', files.some(f => path.basename(f) === 'charm-nest-1.html') && files.some(f => path.basename(f) === 'charm-nest-engrave-setup.js') && files.length > 200);
  const hits = [];
  for (const f of files) {
    let t; try { t = fs.readFileSync(f, 'utf8'); } catch (_) { continue; }
    if (/Undo my edit/i.test(t) || /back to the system'?s? (own )?read/i.test(t) || /data-su=["']?undo/i.test(t) || /Undo and cut it plain/i.test(t)) hits.push(path.relative(root, f));
  }
  eq('no shipped file carries "Undo my edit" / "back to the system\'s reading" / the old control', hits, []);
  const setupSrc = fs.readFileSync(path.join(root, 'charm-nest-engrave-setup.js'), 'utf8'), bridgeSrc = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  ok('charm-nest-engrave-setup.js has no undo code or copy at all', !/undo/i.test(setupSrc));
  ok('the bridge has no undoSetup', !/undoSetup/.test(bridgeSrc));
  const html1 = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
  ok('the page still loads the setup file', /<script src="charm-nest-engrave-setup\.js\?v=[^"]*"/.test(html1));

  console.log(`engrave-no-undo: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
