// ENGCARD (Paul, 12 Oct 2026: "the cards are currently full of needless text ... front and back side-by-side in a larger card ... keep it very simple and minimalist"):
// the engraving card of ONE piece, charm-nest-engrave-card.js (CharmNestEngraveCard), offline and pure: the real faces model and the real card file run over stand-ins for Engrave's jobs, the seals and
// the preview (no page, no network, no paid call, nothing live; names are neutral fixtures).
//   1  a back-only necklace: ONE card with ONE face (the Back, with its words); there is no Front slot at all (ENGSMART: no dashed empty box), one dot, one Approve button
//   2  a tag with front and back: ONE card with two thumbnails (Front, Back) side by side, the words under each, ONE Approve button
//   3  an earring: no card; a legacy decided front job: a card with the Front only, never a Back slot (even for a legacy approved back job)
//   4  a counted line: one card for each disc, each with its own faces
//   5  Approve is there only when a face waits for approval (and off, with a title, until every placement is ready), in the header row beside the name; an approved card has its green dot and its seals there, no button; words to confirm has no button
//   6  none of the deleted words is on any card
//   node tests/charm-nest-engrave-card.cjs
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');

function world(jobsByLine) {
  const sb = { console, setTimeout, clearTimeout, JSON, Math, Date, Promise, Map, Set, WeakMap, Array, Object, String, Number, RegExp };
  sb.self = sb; sb.window = sb; vm.createContext(sb);
  for (const f of ['charm-nest-engrave-faces.js', 'charm-nest-engraving-seals.js', 'charm-nest-engrave-card.js']) vm.runInContext(fs.readFileSync(path.join(root, f), 'utf8'), sb, { filename: f });
  sb.Seal = { BASE_SIZE: 50, html: s => `<span class="seal" data-by="${s.by}"></span>` };
  sb.Engrave = { jobsOf: row => jobsByLine[row.key] || [], isWorking: () => false };
  return sb;
}
const line = (key, extra) => Object.assign({ key, order: { receiptId: '1001' }, line: { sku: 'NAME_ONE', transactionId: '1' }, spec: { designSku: 'NAME_ONE' }, state: 'pooled', poolIds: [key + '_1'] }, extra || {});
const job = (row, key, state, text, extra) => Object.assign({ key, row, state, text, lines: text ? text.split('\n') : [], copies: [row.key + '_1'], fit: state === 'review' ? { glyphs: [] } : null, view: state === 'review' ? { members: [] } : null, verify: { geometry: { ok: true } }, backs: [] }, extra || {});
const setup = faces => ({ v: 1, mode: faces.length === 2 ? 'both' : faces[0] || 'none', faces, front: null, back: null, source: 'labelled', edited: false });
const DELETED = ['TO APPROVE', 'To approve', 'Waiting for your approval', 'WORDS ON THE BACK', 'Words on the back', 'Words as read', 'Read from', 'sure', '%', 'Fix in Engraving', 'View in Engraving', 'ENGRAVES', 'Edit', 'nothing in the listing', 'The system read', 'Undo', 'Approve both', 'unclear', 'Worth a look', 'Words confirmed by', 'Set by'];
const noDeleted = (htmlText, what) => { const txt = htmlText.replace(/<[^>]*>/g, ' ').replace(/(?:title|aria-label)="[^"]*"/g, ''); for (const w of DELETED) assert.ok(!txt.includes(w), `${what}: the deleted text "${w}" is on the card`); };
const count = (s, re) => (s.match(re) || []).length;
let n = 0; const ok = m => console.log('  ✓ ' + m);

// 1 ── a back-only necklace
{
  const row = line('K1'), jobs = { K1: [job(row, 'K1', 'review', 'Name One')] }, W = world(jobs), C = W.CharmNestEngraveCard;
  const ps = C.piecesOf([row], { nameOf: () => 'NAME ONE' });
  assert.equal(ps.length, 1, 'one piece'); const p = ps[0], h = C.html(p);
  assert.equal(count(h, /class="engCard"/g), 1); assert.equal(count(h, /class="engThumb"/g), 1, 'ONE face: the Back only (no Front slot)');
  assert.ok(!/data-face="front"/.test(h) && !/>Front</.test(h), 'no Front box and no "Front" title over nothing'); assert.match(h, /data-face="back"/); assert.ok(!/data-empty/.test(h), 'no empty-slot markup at all');
  assert.equal(count(h, /class="engDot"/g), 1, 'ONE state mark'); assert.equal(count(h, /data-e="approve"/g), 1, 'ONE Approve button');
  assert.ok(h.includes('>Back<') && h.includes('>Name One<'));
  assert.equal(p.faces.front, null); assert.equal(p.state, 'approve');
  noDeleted(h, 'back-only'); ok('back-only necklace: one card, only the Back (with its words), no Front slot, one dot, one Approve'); n++;
}
// 2 ── a tag with front and back
{
  const row = line('K2'), f = job(row, 'K2~front', 'review', 'Name Two', { face: 'front' }), b = job(row, 'K2', 'review', 'Name Three'), W = world({ K2: [f, b] }), C = W.CharmNestEngraveCard;
  const ps = C.piecesOf([row], { nameOf: () => 'TAG' }); assert.equal(ps.length, 1, 'ONE card for the piece, not two'); const h = C.html(ps[0]);
  assert.equal(count(h, /class="engCard"/g), 1); assert.equal(count(h, /class="engThumb"/g), 2);
  assert.ok(h.indexOf('data-face="front"') < h.indexOf('data-face="back"'), 'Front first'); assert.ok(h.includes('>Name Two<') && h.includes('>Name Three<'));
  assert.equal(count(h, /data-e="approve"/g), 1, 'ONE button for both faces'); assert.ok(!/egOff/.test(h) && !/disabled/.test(h), 'both placements ready: enabled');
  assert.ok(ps[0].approve && ps[0].approve.jobs.length === 2);
  const o2 = world({ K2: [Object.assign({}, f, { verify: { geometry: { ok: false } } }), b] }).CharmNestEngraveCard.piecesOf([row], { nameOf: () => 'TAG' })[0];
  assert.equal(o2.approve.ok, false, 'a placement not ready: Approve is off'); assert.match(world({}).CharmNestEngraveCard.html(o2), /egOff[^>]*disabled title="[^"]+"/);
  const sel = C.html(ps[0], { current: 'front', approve: false }); assert.match(sel, /data-face="front"[^>]*aria-current="true"/); assert.ok(!/data-e="approve"/.test(sel), 'the Engrave detail card has no Approve');
  noDeleted(h, 'front+back'); ok('front + back: ONE card, two thumbnails side by side, words under each, one Approve (off until both are ready), the detail card has none'); n++;
}
// 3 ── earrings
{
  const row = line('K3', { spec: { designSku: 'HUGGIE', engraveNever: { rule: 'earring', why: 'Earrings are never engraved' } } }), W = world({ K3: [] }), C = W.CharmNestEngraveCard;
  assert.equal(C.piecesOf([row], {}).length, 0, 'an earring has no card');
  // a legacy decided front job: the Front only, no Back slot
  const f = job(row, 'K3~front', 'approved', 'Name One', { face: 'front', approvedAt: 5, approvedBy: 'Name Two' }), b = job(row, 'K3', 'approved', 'Name Three', { approvedAt: 4, approvedBy: 'Name Two' });
  const W2 = world({ K3: [f, b] }), ps = W2.CharmNestEngraveCard.piecesOf([row], {}); assert.equal(ps.length, 1); const h = W2.CharmNestEngraveCard.html(ps[0]);
  assert.equal(count(h, /class="engThumb"/g), 1, 'the Front only'); assert.ok(!/data-face="back"/.test(h), 'an earring never has a Back slot'); assert.equal(ps[0].faces.back, null);
  assert.equal(world({ K3: [b] }).CharmNestEngraveCard.piecesOf([row], {}).length, 0, 'a legacy back job alone gives an earring no card');
  noDeleted(h, 'earring'); ok('earring: no card; a decided legacy front: Front only, never a Back slot'); n++;
}
// 4 ── a counted line: a card for each disc
{
  const row = line('K4'), d1 = job(row, 'K4#D1', 'review', 'Name One', { slot: 'D1' }), d2 = job(row, 'K4#D2', 'approved', 'Name Two', { slot: 'D2', approvedAt: 9, approvedBy: 'Name Three' });
  const W = world({ K4: [d1, d2] }), C = W.CharmNestEngraveCard, ps = C.piecesOf([row], { nameOf: () => 'DISC' });
  assert.equal(ps.length, 2); assert.equal(ps.map(p => p.title).join('|'), 'DISC · Disc 1|DISC · Disc 2'); assert.equal(ps.map(p => p.state).join('|'), 'approve|approved');
  const h1 = C.html(ps[0]), h2 = C.html(ps[1]);
  assert.equal(count(h1, /data-e="approve"/g), 1); assert.ok(/class="engDot" data-s="approved"/.test(h2) && !/data-e="approve"/.test(h2) && /egButtonSeal/.test(h2) && !/<button[^>]*egApproveButton/.test(h2), 'the approved disc has its green dot and its seal in the Approve button\'s place, and no button');
  assert.ok(h1.indexOf('class="engDot"') < h1.indexOf('class="engName"') && h1.indexOf('class="engName"') < h1.indexOf('data-e="approve"') && h1.indexOf('data-e="approve"') < h1.indexOf('class="engFaces"'), 'one header row: dot, name, Approve; the faces after it');
  ok('counted line: one card for each disc, its own state and button'); n++;
}
// 5 ── Approve only when needed
{
  const row = line('K5'), mk = (state, text) => world({ K5: [job(row, 'K5', state, text)] }).CharmNestEngraveCard;
  for (const [state, want, dot] of [['review', 1, 'approve'], ['words', 0, 'words'], ['fitting', 0, 'busy'], ['approved', 0, 'approved']]) {
    const C = mk(state, state === 'words' ? '' : 'Name One'), p = C.piecesOf([row], {})[0], h = C.html(p);
    assert.equal(count(h, /data-e="approve"/g), want, `${state}: Approve ${want ? 'present' : 'absent'}`); assert.equal(p.state, dot); assert.match(h, new RegExp(`class="engDot" data-s="${dot}"`));
    if (state === 'words') assert.ok(h.includes('Words to confirm')); if (state === 'fitting') assert.ok(h.includes('Preparing')); if (state === 'approved') assert.ok(!h.includes('>Approved<') && !/<button/.test(h.replace(/<button[^>]*engThumb[^>]*>[^<]*<\/button>/g, '')), 'approved: no button at all');
    noDeleted(h, state);
  }
  ok('Approve only when a face waits for approval; words to confirm / preparing / approved have none'); n++;
}
// 6 ── a line that engraves nothing, and a decided seal
{
  const row = line('K6'), W = world({ K6: [job(row, 'K6', 'none', '')] });
  assert.equal(W.CharmNestEngraveCard.piecesOf([row], {}).length, 0, 'nothing to engrave: no card'); ok('a line that engraves nothing has no card'); n++;
}
// 7 ── the words under a picture are the words as GIVEN, not the fit's layout (ENGAUTO: Ace capitals, one word a line)
{
  const row = line('K7'), W = world({}), C = W.CharmNestEngraveCard, f = (state, extra) => C.facesOf('K7', null, [job(row, 'K7', state, 'NAME\nONE', Object.assign({ lines: ['NAME', 'ONE'], lineInput: ['Name One'] }, extra || {}))], { faces: ['back'] }, { row }).back.text;
  assert.equal(f('review'), 'Name One', 'an undecided job shows what went in');
  assert.equal(f('approved'), 'NAME\nONE', 'a decided job shows its lines as they are'); assert.equal(f('review', { lineMode: 'preserve' }), 'NAME\nONE', 'a hand layout shows its lines');
  assert.equal(f('review', { lineInput: ['Other words'] }), 'NAME\nONE', 'other words than the fit holds: the fit\'s lines');
  ok('the words under a picture are the words as given'); n++;
}
console.log(`engrave-card: ${n} groups passed`);
