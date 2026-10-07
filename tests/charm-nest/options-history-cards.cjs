'use strict';
// Options Studio round 2: the history IN each card (charm-nest-options-history.js: OptionsHistory.groups / card / bindCards).
//   node tests/charm-nest/options-history-cards.cjs
// groups() is built from the repository list alone; card() carries the large thumbnail (dates on every green line, DELETED stamp, NEW SHEET mark,
// the window's actions slot); when a scratch jsdom exists (/tmp/ps1-jsdom/node_modules/jsdom, or JSDOM_DIR) the enlarge / collapse / Esc wiring is run too.
const path = require('path'), fs = require('fs'), assert = require('assert/strict');
const FILE = path.join(__dirname, '../../charm-nest-options-history.js');
const OH = require(FILE);

const at = (mo, d, h, mi, y = 2026) => new Date(y, mo - 1, d, h, mi).getTime();
const NOW = at(10, 7, 17, 10);
// one 100 x 50 mm RG sheet cut four times (rings = what is left after each cut, mm, top left origin)
const L1 = [[[30, 0], [100, 0], [100, 50], [0, 50], [0, 20], [30, 20]]];
const L2 = [[[60, 0], [100, 0], [100, 50], [0, 50], [0, 30], [60, 30]]];
const L3 = [[[60, 0], [100, 0], [100, 50], [30, 50], [30, 40], [60, 40]]];
const L4 = [[[60, 10], [100, 10], [100, 50], [30, 50], [30, 40], [60, 40]]];
const rec = (o) => Object.assign({ metal: 'rose', code: 'RG', status: 'used', sheetWMm: 100, sheetHMm: 50, stockId: 'rose-1', estimate: null }, o);
const RECS = [L1, L2, L3, L4].map((rings, i) => rec({
  id: `rose-1-${i + 1}`, revision: i + 1, outline: rings, wMm: 100 - 30 * (i > 1 ? 1 : 0), hMm: 50, areaMm2: 3000 + i,
  cutAt: [at(10, 5, 8, 57), at(10, 6, 14, 5), at(10, 7, 9, 30), at(10, 7, 11, 45)][i], cutBy: ['Paul', 'Maria', 'Jon', 'Maria'][i],
  sourceSheet: `RG Sheet ${i + 1}`, sourceSet: i < 2 ? 'Set 1' : 'Set 2', sourceSheetId: `sh${i + 1}`,
  lastUsedAt: [at(10, 6, 14, 5), at(10, 7, 9, 30), at(10, 7, 11, 45), at(10, 7, 11, 45)][i],
  ...(i < 3 ? { usedBySheetName: `RG Sheet ${i + 2}`, usedAt: [at(10, 6, 14, 5), at(10, 7, 9, 30), at(10, 7, 11, 45)][i], usedBy: ['Maria', 'Jon', 'Maria'][i] } : {}),
}));
RECS[3].status = 'available'; RECS[3].estimate = { pieces: 6, low: 5, high: 7, packedPct: 61 }; RECS[3].lastUsedAt = at(10, 7, 15, 0); RECS[3].lastUsedBy = 'Paul'; RECS[3].lastUsedSheet = 'RG Sheet 9';
RECS[3].wMm = 70; RECS[3].hMm = 40; RECS[3].areaMm2 = 2310;
const FULL = (w, h) => [[[0, 0], [w, 0], [w, h], [0, h]]];
const NEW1 = { id: 'nsh-aaa-0', stockId: 'nsh-aaa', revision: 0, kind: 'new', metal: 'rose', code: 'RG', status: 'available', outline: FULL(120, 60), wMm: 120, hMm: 60, sheetWMm: 120, sheetHMm: 60, areaMm2: 7200,
  sourceSheet: 'New sheet 120 x 60 mm', sourceSet: '', cutAt: at(10, 6, 11, 15), cutBy: 'Maria', madeAt: at(10, 6, 11, 15), madeBy: 'Maria', lastUsedAt: at(10, 6, 11, 15), lastUsedBy: 'Maria', estimate: { pieces: 12, low: 10, high: 14 } };
const DEL1 = { id: 'nsh-bbb-0', stockId: 'nsh-bbb', revision: 0, kind: 'new', metal: 'gold14k', code: '14K', status: 'deleted', outline: FULL(80, 40), wMm: 80, hMm: 40, sheetWMm: 80, sheetHMm: 40, areaMm2: 3200,
  sourceSheet: 'New sheet 80 x 40 mm', cutAt: at(10, 7, 9, 50), cutBy: 'Paul', madeAt: at(10, 7, 9, 50), madeBy: 'Paul',
  deletedAt: at(10, 7, 10, 2), deletedBy: 'Paul', deletedReason: 'Made by accident <b>wrong</b> size' };
// a stock whose earlier revisions are not in the list (the list only holds its newest record)
const PART = rec({ id: 'rose-9-4', stockId: 'rose-9', revision: 4, status: 'available', outline: [[[0, 0], [60, 0], [60, 50], [0, 50]]], wMm: 60, hMm: 50, areaMm2: 3000, cutAt: at(9, 30, 10, 0), cutBy: 'Jon',
  sourceSheet: 'RG Sheet 20', sourceSet: 'Set 7', lastUsedAt: at(9, 30, 10, 0), lastUsedBy: 'Jon', lastUsedSheet: 'RG Sheet 20', estimate: { pieces: 4, low: 4, high: 4 } });
const ITEMS = [RECS[3], DEL1, NEW1, RECS[2], RECS[1], RECS[0], PART];   // newest first, as searchAll answers (the earlier revisions of rose-1 are its 'used' records)
if (require.main !== module) { module.exports = { ITEMS, NOW, at }; return; }

const count = (s, re) => (s.match(re) || []).length;

// ── groups(): from the list alone ──
const before = JSON.stringify(ITEMS);
const G = OH.groups(ITEMS);
assert.equal(JSON.stringify(ITEMS), before, 'the list is not changed');
assert.deepEqual(G.map(g => g.stockId), ['rose-1', 'nsh-bbb', 'nsh-aaa', 'rose-9'], 'one group per physical sheet, in the order each first appears');
const g1 = G[0], gDel = G[1], gNew = G[2], gPart = G[3];
assert.equal(g1.latest.id, 'rose-1-4', 'latest = the newest revision');
assert.deepEqual(g1.history.cuts.map(c => c.revision), [1, 2, 3, 4], 'cuts oldest first');
assert.deepEqual(g1.history.cuts.map(c => c.by), ['Paul', 'Maria', 'Jon', 'Maria']);
assert.deepEqual(g1.history.cuts.map(c => c.sheetName), ['RG Sheet 1', 'RG Sheet 2', 'RG Sheet 3', 'RG Sheet 4']);
assert.deepEqual(g1.history.cuts[2].rings, L3, 'rings = what is left after that cut');
assert.equal(g1.history.cuts[0].at, at(10, 5, 8, 57));
assert.deepEqual(g1.history.stock, { id: 'rose-1', metal: 'rose', code: 'RG', wMm: 100, hMm: 50, revision: 4, ownerSheetId: null, ownerSheetName: null, kind: '' });
assert.equal(g1.history.partial, false, 'revisions 1 to 4 are all there');
assert.equal(g1.history.made, null);
assert.equal(g1.history.deleted, null);
// a made sheet: its revision-0 record is the making, not a cut
assert.deepEqual(gNew.history.cuts, []);
assert.deepEqual(gNew.history.made, { at: at(10, 6, 11, 15), by: 'Maria' });
assert.equal(gNew.history.stock.kind, 'new');
assert.equal(gNew.history.stock.wMm, 120);
assert.equal(gNew.history.partial, false);
assert.deepEqual(gDel.history.deleted, { at: at(10, 7, 10, 2), by: 'Paul', reason: 'Made by accident <b>wrong</b> size' });
assert.deepEqual(gDel.history.cuts, []);
// earlier revisions missing: it says so and keeps what it has
assert.equal(gPart.history.partial, true);
assert.equal(gPart.history.missing, 3);
assert.equal(gPart.history.cuts.length, 1);
assert.equal(gPart.history.cuts[0].revision, 4);
// a made sheet that was cut once: its revision-0 record ('used') gives made, the revision-1 leftover is the cut
const cutNew = OH.groups([rec({ id: 'nsh-aaa-1', stockId: 'nsh-aaa', revision: 1, status: 'available', outline: L1, sheetWMm: 120, sheetHMm: 60, cutAt: at(10, 7, 12, 0), cutBy: 'Jon', sourceSheet: 'RG Sheet 5', sourceSet: 'Set 1' }), Object.assign({}, NEW1, { status: 'used' })])[0];
assert.equal(cutNew.latest.id, 'nsh-aaa-1');
assert.equal(cutNew.history.cuts.length, 1);
assert.deepEqual(cutNew.history.made, { at: at(10, 6, 11, 15), by: 'Maria' });
assert.equal(cutNew.history.partial, false);
// tolerant of whatever the list holds
assert.doesNotThrow(() => { OH.groups(null); OH.groups([null, 5, {}, { id: 'x' }, { stockId: 's', revision: 'z', outline: 'not json' }]); });
assert.deepEqual(OH.groups(undefined), []);
assert.equal(OH.groups([{ id: 'A-1', status: 'available' }, { id: 'B-1', status: 'available' }]).length, 2, 'records without a stock stand alone');
assert.equal(OH.card(null), '');

// ── card(): the large thumbnail is the history ──
const ACT = '<button type="button" class="btn xs" data-act="use">Use this one</button><button type="button" class="btn ghost xs" data-act="del">Delete</button>';
const html1 = OH.card(g1, { actions: ACT, now: NOW });
assert.match(html1, /^<article class="ohc ohcS-available"[^>]*data-stock="rose-1"/);
assert.match(html1, /<div class="ohcThumb" role="button" tabindex="0" aria-expanded="false" aria-label="RG partial sheet, 70 by 40 mm, 4 cuts, press to enlarge"/);
for (const d of ['Oct 5, 8:57 AM', 'Oct 6, 2:05 PM', 'Oct 7, 9:30 AM', 'Oct 7, 11:45 AM']) assert.ok(html1.includes(d), 'the line says ' + d);
assert.equal(count(html1, /class="ohLine[ "]/g), 4, 'a green line per cut');
assert.equal(count(html1, /stroke="#008974"[^>]*stroke-dasharray/g), 4, 'dashed green');
assert.equal(count(html1, /class="ohBadgeN"/g), 4, 'numbered badges');
assert.equal(new Set(html1.match(/class="ohL[^"]*"[^>]*fill="(#[0-9a-f]{6})"/g).map(x => x.match(/fill="(#[0-9a-f]{6})"/)[1]).filter(c => c !== '#fffefb')).size, 4, 'one grey per cut');
assert.match(html1, /viewBox="0 0 100 50"/, 'the whole original sheet at true scale');
assert.equal(count(html1, /role="button"/g), 1, 'the thumbnail is the only control inside the picture');
assert.ok(!/ohReveal/.test(html1), 'no replay of the reveal on every repaint');
assert.ok(!/DELETED|NEW SHEET/.test(html1));
assert.ok(html1.includes('About 6 pieces') && html1.includes('roughly 5 to 7'), 'the fit words');
assert.ok(html1.includes('70 × 40 mm') && html1.includes('2,310 mm²'), 'size and area');
assert.ok(html1.includes('Last used') && html1.includes('Oct 7, 3:00 PM') && html1.includes('by Paul · on RG Sheet 9'), 'last used: when, by whom, on which sheet');
assert.ok(html1.includes('Cut by') && html1.includes('from RG Sheet 4, Set 2'), 'who cut it, from which sheet');
assert.ok(html1.includes('click to enlarge') && html1.includes('4 cuts'));
assert.ok(html1.includes('class="ohcActions"') && html1.includes('data-act="use"') && html1.includes('data-act="del"'), 'the window\'s actions slot');
assert.ok(html1.indexOf('ohcActions') > html1.indexOf('ohcFacts'), 'actions at the bottom');
assert.ok(!OH.card(g1, {}).includes('ohcActions'), 'no slot without actions');
assert.match(OH.card(g1, { selected: true }), /class="ohc ohcS-available ohcSel"/);
// a deleted sheet: the red stamp, the reason with the person and date (escaped)
const htmlD = OH.card(gDel, { now: NOW });
assert.match(htmlD, /class="ohcStamp"[^>]*>.*>DELETED</s);
assert.match(htmlD, /class="ohcChip" data-s="deleted">Deleted</);
assert.ok(htmlD.includes('Deleted by') && htmlD.includes('Paul') && htmlD.includes('Oct 7, 10:02 AM'));
assert.ok(htmlD.includes('Made by accident &lt;b&gt;wrong&lt;/b&gt; size') && !htmlD.includes('<b>wrong</b>'), 'the reason is shown as text');
assert.match(htmlD, /aria-label="14K new sheet, 80 by 40 mm, never cut, deleted, press to enlarge"/);
// a new sheet that was never cut: the NEW SHEET mark and no cut line
const htmlN = OH.card(gNew, { now: NOW });
assert.match(htmlN, /class="ohcNew"[^>]*>.*>NEW SHEET</s);
assert.equal(count(htmlN, /class="ohLine[ "]/g), 0, 'no cut lines');
assert.equal(count(htmlN, /class="ohTag[ "]/g), 0);
assert.ok(htmlN.includes('New sheet 120 × 60 mm') && htmlN.includes('Made by') && htmlN.includes('Maria') && htmlN.includes('Oct 6, 11:15 AM'));
assert.ok(htmlN.includes('Not used yet') && htmlN.includes('About 12 pieces'));
assert.match(htmlN, /aria-label="RG new sheet, 120 by 60 mm, never cut, press to enlarge"/);
// earlier revisions missing: a quiet note, the real cut number on its badge
const htmlP = OH.card(gPart, { now: NOW });
assert.ok(htmlP.includes('Earlier cuts not shown'));
assert.match(htmlP, /class="ohBadgeN"[^>]*>4</, 'the badge keeps the real cut number');
assert.match(htmlP, /aria-label="RG partial sheet, 60 by 50 mm, 4 cuts,/, 'the cut count is the sheet\'s revision');
assert.ok(htmlP.includes('Oct 5, 8:57 AM') === false && htmlP.includes('Sep 30, 10:00 AM'));
// enlarged markup: the detail (drawing + timeline), the close x, no thumbnail
const htmlE = OH.card(g1, { enlarged: true, actions: ACT, now: NOW });
assert.match(htmlE, /class="ohc ohcS-available ohcBig"[^>]*data-enlarged="true"/);
assert.ok(htmlE.includes('data-oh-close') && htmlE.includes('data-oh-detail') && !htmlE.includes('data-oh-thumb'));
assert.equal(count(htmlE, /<button type="button" class="ohItem/g), 4, 'every cut in the timeline');
for (const w of ['Paul', 'Maria', 'Jon', 'RG Sheet 1, Set 1', 'RG Sheet 3, Set 2', 'Oct 5, 8:57 AM', 'Original sheet 100 × 50 mm', 'Left on the sheet now']) assert.ok(htmlE.includes(w), w);
assert.equal(count(htmlE, /class="ohT2/g), 4, 'the person on every line of the detail');
assert.ok(htmlE.includes('data-act="use"'));
// the timeline of a made / deleted sheet: made-by, deleted-by with the reason
const tlN = OH.timeline(gNew.history, { now: NOW, dates: 'short' });
assert.ok(tlN.includes('New sheet 120 × 60 mm') && tlN.includes('Made by') && tlN.includes('Maria') && tlN.includes('Oct 6, 11:15 AM'));
const tlD = OH.timeline(gDel.history, { now: NOW, dates: 'short' });
assert.ok(tlD.includes('Deleted') && tlD.includes('By') && tlD.includes('Paul') && tlD.includes('Oct 7, 10:02 AM') && tlD.includes('Reason: Made by accident &lt;b&gt;'), 'deleted-by and why');
assert.ok(OH.timeline(gPart.history, { now: NOW }).includes('Earlier cuts of this sheet are not shown here'));
assert.match(OH.timeline(g1.history, { now: NOW, dates: 'short' }), /Original sheet 100 × 50 mm/);
assert.equal(OH.shortDate(at(10, 5, 8, 57), NOW), 'Oct 5, 8:57 AM');
assert.equal(OH.shortDate(at(10, 5, 8, 57, 2025), NOW), 'Oct 5, 2025, 8:57 AM');

// ── the wiring, when jsdom is at hand ──
let JSDOM = null;
for (const p of [process.env.JSDOM_DIR, '/tmp/ps1-jsdom/node_modules/jsdom', 'jsdom'].filter(Boolean)) { try { ({ JSDOM } = require(p)); break; } catch (_) { /* next */ } }
(async () => {
  if (!JSDOM) { console.log('  – no jsdom: the enlarge / collapse wiring was not run'); console.log('options-history-cards: ok'); return; }
  const tick = (n = 5) => new Promise(r => setTimeout(r, n));
  const dom = new JSDOM('<!doctype html><html><head></head><body><dialog open id="dlg"><input id="q"><div id="host"></div></dialog></body></html>', { runScripts: 'outside-only', pretendToBeVisual: true });
  const w = dom.window, d = w.document;
  w.eval(fs.readFileSync(FILE, 'utf8'));
  const W = w.OptionsHistory, host = d.getElementById('host'), dlg = d.getElementById('dlg');
  const groups = W.groups(ITEMS);
  const draw = (enlarged) => { host.innerHTML = `<div class="ohcGrid">${groups.map(g => W.card(g, { actions: ACT, now: NOW, enlarged: enlarged === g.stockId })).join('')}</div>`; };
  draw();
  assert.equal(host.querySelectorAll('.ohc').length, 4);
  // the server's exact history for the partial sheet: all four cuts
  const serverCuts = [L1, L2, L3, [[[0, 0], [60, 0], [60, 50], [0, 50]]]].map((rings, i) => ({ n: i + 1, revision: i + 1, at: at(9, 20 + i, 10, 0), by: ['Ann', 'Bo', 'Cy', 'Jon'][i], sheetName: `RG Sheet ${16 + i}`, setName: 'Set 7', rings }));
  const asked = []; let release;
  const calls = { enlarge: [], collapse: [], escHeld: false };
  const api = W.bindCards(host, { now: NOW, onEnlarge: g => calls.enlarge.push(g.stockId), onCollapse: g => calls.collapse.push(g.stockId), holdEscape: () => calls.escHeld,
    getHistory: g => { asked.push(g.stockId); return new Promise(r => { release = () => r({ ok: true, stock: { id: g.stockId, wMm: 100, hMm: 50, revision: 4 }, cuts: serverCuts, rev: '4' }); }); } });
  const cardOf = id => host.querySelector(`.ohc[data-stock="${id}"]`);
  const thumbOf = id => cardOf(id).querySelector('[data-oh-thumb]');
  const click = el => el.dispatchEvent(new w.MouseEvent('click', { bubbles: true }));
  const key = (el, k) => { const e = new w.KeyboardEvent('keydown', { key: k, bubbles: true, cancelable: true }); el.dispatchEvent(e); return e; };
  let docSaw = 0; d.addEventListener('keydown', e => { if (e.key === 'Escape') docSaw++; });
  const actionsNode = cardOf('rose-9').querySelector('.ohcActions'); let pressed = 0; actionsNode.addEventListener('click', () => { pressed++; });

  // a press on the window's own buttons does not enlarge anything
  click(actionsNode.querySelector('[data-act="use"]')); assert.equal(pressed, 1); assert.equal(host.querySelectorAll('.ohcBig').length, 0, 'the buttons are not the thumbnail');
  // click: enlarged IN PLACE, the detail drawn at once, the spinner while the exact history loads
  click(thumbOf('rose-9'));
  let c9 = cardOf('rose-9');
  assert.ok(c9.classList.contains('ohcBig') && c9.getAttribute('data-enlarged') === 'true', 'the card grows (it spans the window\'s width)');
  assert.deepEqual(calls.enlarge, ['rose-9']); assert.deepEqual(asked, ['rose-9'], 'getHistory(group) asked once');
  assert.ok(c9.querySelector('.ohcLoad') && /Loading the full history/.test(c9.querySelector('.ohcLive').textContent), 'a labelled spinner while it loads');
  assert.equal(c9.querySelectorAll('.ohItem[data-cut]').length, 1, 'the thumbnail\'s data is shown meanwhile');
  assert.equal(c9.querySelector('.ohcActions'), actionsNode, 'the window\'s buttons are the very same nodes (nothing rebuilt)');
  assert.equal(host.querySelectorAll('.ohc:not(.ohcBig) [data-oh-thumb]').length, 3, 'the others stay thumbnails');
  assert.equal(d.activeElement, c9.querySelector('.ohcClose'), 'focus moves into the open card');
  click(actionsNode.querySelector('[data-act="del"]')); assert.equal(pressed, 2); assert.ok(cardOf('rose-9').classList.contains('ohcBig'), 'and a press on a button leaves it open');
  // the exact history arrives: the detail is drawn again with every cut, the spinner is gone
  release(); await tick(20);
  c9 = cardOf('rose-9');
  assert.equal(c9.querySelectorAll('.ohItem[data-cut]').length, 4, 'the server\'s four cuts');
  assert.ok(!c9.querySelector('.ohcLoad') && !c9.querySelector('.ohcNote'), 'no spinner, no note once it is here');
  assert.ok(c9.textContent.includes('Ann') && c9.textContent.includes('Sep 20, 10:00 AM'), 'names and dates from the server');
  assert.equal(c9.querySelector('.ohcActions'), actionsNode);
  assert.ok(!c9.querySelector('.ohcStage') && c9.querySelectorAll('.ohTag[role="button"]').length === 4, 'the labels of the open drawing are real buttons');
  // hover / click between the timeline row and its green line
  const row2 = c9.querySelector('.ohItem[data-cut="2"]');
  row2.dispatchEvent(new w.MouseEvent('mouseover', { bubbles: true }));
  assert.ok(row2.classList.contains('ohHot') && c9.querySelector('.ohLine[data-cut="2"]').classList.contains('ohHot'), 'hover lights the row and its line');
  click(row2);
  assert.ok(c9.querySelector('.ohLine[data-cut="2"]').classList.contains('ohOn') && row2.getAttribute('aria-pressed') === 'true');
  click(c9.querySelector('.ohTag[data-cut="3"]')); assert.ok(cardOf('rose-9').classList.contains('ohcBig'), 'a click on a label picks its cut, it does not close the card');
  assert.equal(c9.querySelector('.ohItem[data-cut="3"]').getAttribute('aria-pressed'), 'true');
  // another card: only one is open at a time
  click(thumbOf('rose-1'));
  assert.equal(host.querySelectorAll('.ohcBig').length, 1);
  assert.ok(cardOf('rose-1').classList.contains('ohcBig') && !cardOf('rose-9').classList.contains('ohcBig'));
  assert.deepEqual(calls.collapse, ['rose-9'], 'the first one was collapsed for it');
  assert.deepEqual(asked, ['rose-9', 'rose-1']);
  release(); await tick(20);
  // Esc: pending answer first (the window says so), then the enlarged card, then the window
  calls.escHeld = true; docSaw = 0;
  let e = key(d.getElementById('q'), 'Escape');
  assert.ok(!e.defaultPrevented && docSaw === 1 && cardOf('rose-1').classList.contains('ohcBig'), 'held: the window\'s pending answer gets the Esc, the card stays open');
  calls.escHeld = false; docSaw = 0;
  e = key(d.getElementById('q'), 'Escape');   // from the search box: still inside the window
  assert.ok(e.defaultPrevented && docSaw === 0, 'enlarged: Esc is taken (stopPropagation) so the window stays open');
  const c1 = cardOf('rose-1');
  assert.ok(!c1.classList.contains('ohcBig') && c1.querySelector('[data-oh-thumb]'), 'and the card is a thumbnail again');
  assert.deepEqual(calls.collapse, ['rose-9', 'rose-1']);
  assert.equal(d.activeElement, c1.querySelector('[data-oh-thumb]'), 'focus goes back to the thumbnail');
  e = key(d.getElementById('q'), 'Escape');
  assert.ok(!e.defaultPrevented && docSaw === 1, 'nothing enlarged: the window\'s own Esc runs');
  // Enter and Space on the thumbnail, the close x, a click on the open drawing's background
  key(thumbOf('nsh-aaa'), 'Enter'); assert.ok(cardOf('nsh-aaa').classList.contains('ohcBig'), 'Enter enlarges');
  assert.ok(cardOf('nsh-aaa').textContent.includes('Made by') && cardOf('nsh-aaa').querySelector('.ohcNew, .ohcNewT'), 'the made sheet\'s own line');
  click(cardOf('nsh-aaa').querySelector('[data-oh-close]')); assert.ok(!cardOf('nsh-aaa').classList.contains('ohcBig'), 'the close x collapses');
  key(thumbOf('nsh-bbb'), ' '); assert.ok(cardOf('nsh-bbb').classList.contains('ohcBig'), 'Space enlarges');
  assert.ok(cardOf('nsh-bbb').querySelector('.ohcStamp') && cardOf('nsh-bbb').textContent.includes('Reason: Made by accident <b>wrong</b> size'), 'the stamp and the reason (as text) in the open card');
  click(cardOf('nsh-bbb').querySelector('.ohTray')); assert.ok(!cardOf('nsh-bbb').classList.contains('ohcBig'), 'clicking the open drawing again collapses it');
  assert.equal(calls.enlarge.length, 4);
  // a repaint of the window's list keeps an open card open, wired and (when it never arrived) asked for again
  draw('rose-1'); const again = W.bindCards(host, { now: NOW, getHistory: () => Promise.resolve(null) });
  assert.ok(cardOf('rose-1').classList.contains('ohcBig'));
  cardOf('rose-1').querySelector('.ohItem[data-cut="2"]').dispatchEvent(new w.MouseEvent('click', { bubbles: true }));
  assert.equal(cardOf('rose-1').querySelector('.ohItem[data-cut="2"]').getAttribute('aria-pressed'), 'true', 'rebound after a repaint');
  assert.equal(W.collapse(host), true, 'collapse(root) for a window running its own Esc order');
  assert.equal(W.collapse(host), false);
  assert.ok(!cardOf('rose-1').classList.contains('ohcBig'));
  // a history that cannot be loaded: the thumbnail's data stays and says so
  again.destroy();
  const dom2 = new JSDOM('<!doctype html><html><head></head><body><div id="host"></div></body></html>', { runScripts: 'outside-only', pretendToBeVisual: true });
  dom2.window.eval(fs.readFileSync(FILE, 'utf8'));
  const W2 = dom2.window.OptionsHistory, h2 = dom2.window.document.getElementById('host'), g2 = W2.groups(ITEMS);
  h2.innerHTML = g2.map(g => W2.card(g, { now: NOW })).join('');
  W2.bindCards(h2, { now: NOW, getHistory: () => Promise.reject(new Error('offline')) });
  h2.querySelector('.ohc[data-stock="rose-9"] [data-oh-thumb]').dispatchEvent(new dom2.window.MouseEvent('click', { bubbles: true })); await tick(20);
  const c92 = h2.querySelector('.ohc[data-stock="rose-9"]');
  assert.ok(/could not be loaded/.test(c92.querySelector('.ohcLive').textContent) && c92.querySelectorAll('.ohItem[data-cut]').length === 1, 'the list\'s data stays and the note says why');
  console.log('options-history-cards: ok');
})().catch(err => { console.error(err); process.exit(1); });
