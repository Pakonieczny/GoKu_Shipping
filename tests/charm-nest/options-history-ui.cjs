'use strict';
// Options Studio: charm-nest-options-history.js (window.OptionsHistory), pure rendering and filtering, checked on its string output.
//   node tests/charm-nest/options-history-ui.cjs
// (when a scratch jsdom exists, e.g. /tmp/ps1-jsdom/node_modules/jsdom, the wiring is checked too; otherwise that part is skipped)
const path = require('path'), fs = require('fs'), assert = require('assert/strict');
const FILE = path.join(__dirname, '../../charm-nest-options-history.js');
const OH = require(FILE);

const at = (mo, d, h, mi, y = 2026) => new Date(y, mo - 1, d, h, mi).getTime();
const NOW = at(1, 15, 12, 0, 2027);   // a later year, so dates show their year: "Oct 5, 2026, 8:57 AM"
// a 100 x 50 mm sheet cut three times, the leftover shrinking each time (rings in mm, top left origin)
const L1 = [[[30, 0], [100, 0], [100, 50], [0, 50], [0, 20], [30, 20]]];
const L2 = [[[60, 0], [100, 0], [100, 50], [0, 50], [0, 30], [60, 30]]];
const L3 = [[[60, 0], [100, 0], [100, 50], [30, 50], [30, 40], [60, 40]]];
const history = {
  ok: true, rev: 'r3',
  stock: { id: 'rose-1', metal: 'rose', code: 'RG', wMm: 100, hMm: 50, revision: 3, ownerSheetId: 's9', ownerSheetName: 'Sheet 9' },
  cuts: [
    { n: 1, revision: 1, at: at(10, 5, 8, 57), by: 'Paul', sheetName: 'RG Sheet 1', setName: 'Set 1', rings: L1, exact: true },
    { n: 2, revision: 2, at: at(10, 6, 14, 5), by: '', sheetName: 'RG Sheet 2', setName: 'Set 1', rings: L2, exact: false },
    { n: 3, revision: 3, at: at(10, 7, 9, 30), by: 'Maria', sheetName: 'RG Sheet 3', setName: 'Set 2', rings: L3, exact: true },
  ],
};
const count = (s, re) => (s.match(re) || []).length;

// ── the drawing ──
const svg = OH.svg(history, { now: NOW, width: 700 });
assert.match(svg, /^<svg /);
assert.equal(count(svg, /class="ohTag[ "]/g), 3, '3 labelled cut lines');
assert.equal(count(svg, /class="ohLine[ "]/g), 3, '3 green lines');
for (const k of [1, 2, 3]) assert.match(svg, new RegExp(`class="ohBadgeN"[^>]*>${k}<`), `badge ${k}`);
assert.equal(count(svg, /stroke="#008974"/g), 3, 'each cut line is GREEN');
assert.equal(count(svg, /stroke-dasharray/g), 3, 'and dashed');
const greys = new Set(svg.match(/class="ohL[^"]*"[^>]*fill="(#[0-9a-f]{6})"/g).map(x => x.match(/fill="(#[0-9a-f]{6})"/)[1]).filter(c => c !== '#fffefb'));
assert.equal(greys.size, 3, '3 distinct greys');
assert.ok(greys.has('#efece7') && greys.has('#d9d5ce'), 'oldest and newest grey');
assert.match(svg, /fill="#fffefb"/, 'the live sheet colour');
assert.match(svg, /fill="#efe9de"/, 'the tray');
assert.match(svg, /viewBox="0 0 100 50"/, 'true scale in the 100 x 50 mm frame');
assert.match(svg, /Oct 5, 2026, 8:57 AM/);
assert.match(svg, /Oct 6, 2026, 2:05 PM/);
assert.match(svg, /Oct 7, 2026, 9:30 AM/);
assert.ok(svg.includes('>Paul<') && svg.includes('>Maria<'), 'persons on the lines');
assert.equal(count(svg, />Person not recorded</g), 1, 'a missing person says so on its line');
assert.equal(count(svg, /person not recorded/g), 2, '... and in its title and aria-label');
assert.equal(count(svg, /aria-pressed="false"/g), 3);
assert.match(OH.svg(history, { now: NOW, selected: 2 }), /id="oh-tag-2"[^>]*aria-pressed="true"/, 'selected');
assert.equal(count(OH.svg(history, { now: NOW, reveal: false }), /ohReveal/g), 0, 'reveal can be switched off');
assert.match(svg, /ohReveal/);
// the line of cut k is the cut edge of its leftover that an earlier cut did not already draw: cut 2's line is not cut 1's line again
const lineD = k => svg.match(new RegExp(`id="oh-line-${k}"[^>]*>\\s*<path class="ohHalo" d="([^"]*)"`))[1];
assert.equal(lineD(1), 'M0 20L30 20L30 0', 'cut 1: the whole new edge');
assert.equal(lineD(2), 'M0 30L60 30L60 0', 'cut 2: only its own edge (cut 1\'s edge is not drawn again)');
assert.equal(lineD(3), 'M30 50L30 40L60 40L60 30', 'cut 3: the stretch of x=60 that cut 2 already drew is left out');
// a never-cut sheet, a history with nothing in it
const none = OH.svg({ stock: { wMm: 100, hMm: 50 }, cuts: [] });
assert.equal(count(none, /class="ohTag[ "]/g), 0);
assert.match(none, /Sheet history at true scale: 100 by 50 millimetres, never cut/);
assert.doesNotThrow(() => { OH.svg(null); OH.svg({}); OH.timeline(undefined); OH.svg({ stock: { wMm: 60, hMm: 40 }, cuts: [{ at: 'x', by: 5 }, null, { rings: 'not json' }] }); });
assert.match(OH.svg({ stock: { wMm: 60, hMm: 40 }, cuts: [] }), /viewBox="0 0 100 50"/, 'a smaller sheet sits top left in the same frame');

// ── the timeline ──
const tl = OH.timeline(history, { now: NOW, selected: 3 });
assert.ok(tl.indexOf('Original sheet 100 × 50 mm') > 0 && tl.indexOf('Original sheet') < tl.indexOf('Cut 1'), 'the original sheet comes first');
assert.equal(count(tl, /<button type="button" class="ohItem/g), 3, 'a button per cut');
assert.match(tl, /data-cut="3"[^>]*aria-pressed="true"/);
assert.match(tl, /data-cut="1"[^>]*aria-pressed="false"/);
for (const w of ['Oct 5, 2026, 8:57 AM', 'Paul', 'RG Sheet 1, Set 1', 'RG Sheet 3, Set 2', 'Maria']) assert.ok(tl.includes(w), w);
assert.match(tl, /Person not recorded/);
assert.match(tl, /\d[\d,]* mm² cut away[^<]*<[^>]*>·<\/span>[\d.]+% of the sheet/);
assert.match(tl, /Left on the sheet now/);
assert.match(OH.timeline({ stock: {}, cuts: [{ rings: L1 }] }), /Date not recorded/);
assert.match(OH.timeline({ stock: {}, cuts: [] }), /No cut has been made/);
assert.match(OH.view(history, { now: NOW }), /^<div class="ohView">.*<svg .*<ol class="ohTl"/s);

// ── the search ──
const items = [
  { id: 'A', metal: 'rose', code: 'RG', status: 'available', sourceSheet: 'RG Sheet 1', sourceSet: 'Set 1', cutBy: 'Paul', cutAt: at(10, 5, 8, 57), lastUsedAt: at(10, 5, 8, 57), wMm: 94.7, hMm: 50, sheetWMm: 100, sheetHMm: 50 },
  { id: 'B', metal: 'gold14k', code: '14K', status: 'available', sourceSheet: 'Sheet 3', sourceSet: 'Set 2', cutBy: 'Maria', cutAt: at(10, 6, 9, 0), lastUsedAt: at(10, 6, 9, 0), wMm: 60, hMm: 50, sheetWMm: 100, sheetHMm: 50 },
  { id: 'C', metal: 'gold10k', code: '10K', status: 'used', sourceSheet: 'Sheet 4', sourceSet: 'Set 1', cutBy: 'Paul', cutAt: at(9, 30, 10, 0), lastUsedAt: at(10, 1, 11, 0), lastUsedBy: 'Jon', usedBySheetName: 'Sheet 9', wMm: 40, hMm: 25 },
  { id: 'D', metal: 'rose', code: 'RG', status: 'inUse', sourceSheet: 'RG Sheet 7', sourceSet: 'Set 5', cutBy: 'Jon', cutAt: at(10, 3, 16, 0), lastUsedAt: at(10, 4, 16, 0), inUseBySheetName: 'RG Sheet 8', wMm: 70, hMm: 30 },
  { id: 'E', metal: 'gold14k', code: '14K', status: 'discarded', sourceSheet: 'Sheet 6', sourceSet: 'Set 3', cutBy: 'Maria', cutAt: at(10, 2, 12, 0), lastUsedAt: at(10, 2, 12, 0), wMm: 12, hMm: 9 },
];
const ids = (q, o) => OH.filter(items, q, Object.assign({ now: NOW }, o)).map(c => c.id).join('');
assert.equal(ids('oct 5'), 'A', 'date words: oct 5 (not Oct 6 with a 50 mm size)');
assert.equal(ids('October 5, 2026'), 'A');
assert.equal(ids('2026-10-05'), 'A');
assert.equal(ids('5 oct'), 'A');
assert.equal(ids('10/6'), 'B');
assert.equal(ids('oct'), 'ABCDE', 'a month alone is a word');
assert.equal(ids('sept'), 'C');
assert.equal(ids('paul'), 'AC', 'person');
assert.equal(ids('JON'), 'CD', 'the last user counts, case does not');
assert.equal(ids('14k'), 'BE', 'metal');
assert.equal(ids('rose gold'), 'AD');
assert.equal(ids('94.7'), 'A', 'size');
assert.equal(ids('94.7 x 50'), 'A');
assert.equal(ids('sheet 9'), 'C', 'the sheet that used it');
assert.equal(ids('in use'), 'D', 'status word');
assert.equal(ids('discarded'), 'E');
assert.equal(ids('paul oct 5'), 'A', 'tokens AND together');
assert.equal(ids('paul sept'), 'C');
assert.equal(ids('paul maria'), '');
assert.equal(ids(''), 'ABCDE', 'empty query: everything, in order');
assert.equal(ids('   '), 'ABCDE');
assert.equal(ids('', { status: 'used' }), 'C', 'the Used chip');
assert.equal(ids('', { status: 'inUse' }), 'D');
assert.equal(ids('', { status: 'all' }), 'ABCDE');
assert.equal(ids('', { metal: '14K' }), 'BE');
assert.equal(ids('', { metal: 'rose', status: 'available' }), 'A');
assert.equal(ids('paul', { metal: 'gold10k', status: 'used' }), 'C', 'chips and text together');
assert.equal(ids('paul', { status: 'discarded' }), '');
assert.deepEqual(OH.filter(null, 'x'), []);

// ── the wiring and the style, when jsdom is at hand ──
let JSDOM = null;
for (const p of [process.env.JSDOM_DIR, '/tmp/ps1-jsdom/node_modules/jsdom', 'jsdom'].filter(Boolean)) { try { ({ JSDOM } = require(p)); break; } catch (_) { /* next */ } }
if (JSDOM) {
  const dom = new JSDOM('<!doctype html><html><head></head><body><div id="host"></div></body></html>', { runScripts: 'outside-only', pretendToBeVisual: true });
  const w = dom.window;
  w.eval(fs.readFileSync(FILE, 'utf8')); w.eval(fs.readFileSync(FILE, 'utf8'));
  assert.equal(w.document.querySelectorAll('#optionsHistoryCss').length, 1, 'the style is injected once');
  const host = w.document.getElementById('host');
  host.innerHTML = w.OptionsHistory.view(history, { now: NOW });
  const picked = [];
  const h = w.OptionsHistory.bind(host, history, { onSelect: (k, cut) => picked.push([k, cut && cut.sheetName]) });
  const list2 = host.querySelector('.ohItem[data-cut="2"]'), tag3 = host.querySelector('.ohTag[data-cut="3"]');
  list2.dispatchEvent(new w.MouseEvent('mouseover', { bubbles: true }));
  assert.ok(host.querySelector('.ohPiece[data-cut="2"]').classList.contains('ohHot') && list2.classList.contains('ohHot'), 'hover lights both');
  list2.dispatchEvent(new w.MouseEvent('click', { bubbles: true }));
  assert.deepEqual(picked[0], [2, 'RG Sheet 2']);
  assert.equal(list2.getAttribute('aria-pressed'), 'true');
  assert.ok(host.querySelector('.ohTag[data-cut="2"]').classList.contains('ohOn') && host.querySelector('.ohLine[data-cut="2"]').classList.contains('ohOn'), 'the drawing follows the list');
  tag3.dispatchEvent(new w.KeyboardEvent('keydown', { key: 'Enter', bubbles: true }));
  assert.equal(h.selected(), 3);
  assert.equal(host.querySelector('.ohItem[data-cut="3"]').getAttribute('aria-pressed'), 'true', 'the list follows the drawing');
  assert.equal(host.querySelector('.ohItem[data-cut="2"]').getAttribute('aria-pressed'), 'false');
  host.querySelector('.ohPiece[data-cut="3"]').dispatchEvent(new w.MouseEvent('click', { bubbles: true }));
  assert.equal(h.selected(), null, 'a second click clears');
  assert.deepEqual(picked.map(x => x[0]), [2, 3, null]);
  w.OptionsHistory.bind(host, history, {});   // bound again: the first wiring is dropped, a click is handled once
  list2.dispatchEvent(new w.MouseEvent('click', { bubbles: true }));
  assert.equal(picked.length, 3);
} else console.log('  – no jsdom: the wiring was not run');
console.log('options-history-ui: ok');
