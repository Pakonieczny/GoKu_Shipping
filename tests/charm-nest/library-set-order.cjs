/* SETORDER (Paul, 11 Nov 2026): "why does the order of the sets as they're displayed get reversed on the date changes the set should always be in the
 * same order for every date. Maybe you can actually add a separation layer that's visible from date to date and then in each date there should be sets
 * that are ordered the same way" (In progress showed Set-2 · 2026-11-11, then Set-3, Set-2, Set-1 · 2026-11-10: the last-activity order, so a set moved
 * up each time someone worked on it).
 *
 * The Library's Laser cutting and In progress areas: actual sets by day (newest first; oldest first when the list is flipped), Set-1, Set-2, Set-3 inside
 * a day whatever was touched last, the loose sheets after them in last-activity order; each day opens with a rule naming it and its count of sets.
 * Offline: charm-nest-activity.js as is, and LaserReview's arrange() sliced out of charm-nest-bridge.js over a small stand-in DOM (no jsdom needed).
 *   node tests/charm-nest/library-set-order.cjs                                                                                                      */
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const ROOT = path.join(__dirname, '../..'), A = require(path.join(ROOT, 'charm-nest-activity.js'));
const set = (id, day, seq, activityAt) => ({ setId: id, day, seq, activityAt });
const name = x => x.setId ? `${x.day.slice(5)}#${x.seq}` : x.key;

// 1. The order: the 11 Nov screenshot's list, Set-3 touched most recently.
const shot = [set('a', '2026-11-11', 2, 50), set('b', '2026-11-10', 3, 40), set('c', '2026-11-10', 2, 30), set('d', '2026-11-10', 1, 20), { key: 'loose-new', working: true, activityAt: 60 }, { key: 'loose-old', working: true, activityAt: 10 }];
assert.deepEqual(shot.slice().sort((x, y) => A.compareDays(x, y, 'desc')).map(name), ['11-11#2', '11-10#1', '11-10#2', '11-10#3', 'loose-new', 'loose-old'], 'newest day first, Set-1 up inside a day, loose sheets last by activity');
assert.deepEqual(shot.slice().sort((x, y) => A.compareDays(x, y, 'asc')).map(name), ['11-10#1', '11-10#2', '11-10#3', '11-11#2', 'loose-old', 'loose-new'], 'flipped: oldest day first, still Set-1 up inside a day');
// whatever is touched last, a day keeps its order
for (const t of [[1, 2, 3], [3, 2, 1], [2, 3, 1]]) {
  const day = [set('x1', '2026-11-10', 1, t[0]), set('x2', '2026-11-10', 2, t[1]), set('x3', '2026-11-10', 3, t[2])];
  assert.deepEqual(day.sort((x, y) => A.compareDays(x, y, 'desc')).map(x => x.seq), [1, 2, 3], 'the same order for every activity');
}
assert.deepEqual([set('n', '', 1, 9), set('m', '2026-11-09', 4, 1)].sort((x, y) => A.compareDays(x, y, 'desc')).map(x => x.setId), ['m', 'n'], 'a set with no day comes after the dated ones');
assert.deepEqual(A.selectDays('library', shot.slice(), Date.parse('2026-11-11T15:00:00Z')).map(name), ['11-11#2', '11-10#1', '11-10#2', '11-10#3', 'loose-new', 'loose-old'], 'selectDays is the same order');
// the Completed list keeps the last-activity order (compareBlocks unchanged)
assert.deepEqual(shot.filter(A.isSet).sort((x, y) => A.compareBlocks(x, y, 'desc')).map(x => x.setId), ['a', 'b', 'c', 'd']);

// 2. The page: both places that order the Library's sets use it.
const bridge = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8');
assert(/sets=CNListActivity\.selectDays\("library",sets\)/.test(bridge), 'renderLibrary orders by day');
assert(/CNListActivity\.compareDays\(value\(a\),value\(b\)/.test(bridge), 'arrange orders by day');
assert(/\.laserDayRule \{/.test(fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8')), 'the rule is styled');

// 3. arrange() on a stand-in DOM: the rules are laid, an empty area still says it is empty, and a second pass moves nothing.
class Node {
  constructor(cls) { this.className = cls || ''; this.dataset = {}; this.children = []; this.parent = null; this.attrs = {}; this.innerHTML = ''; this.hidden = false; }
  get classList() { const n = this; return { contains: c => n.className.split(/\s+/).includes(c) }; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  insertBefore(n, ref) { if (n.parent) n.parent.children.splice(n.parent.children.indexOf(n), 1); const i = ref ? this.children.indexOf(ref) : -1; this.children.splice(i < 0 ? this.children.length : i, 0, n); n.parent = this; moves++; return n; }
  appendChild(n) { return this.insertBefore(n, null); }
  remove() { if (this.parent) { this.parent.children.splice(this.parent.children.indexOf(this), 1); this.parent = null; moves++; } }
  querySelectorAll(sel) { return sel === ':scope > .laserDayRule' ? this.children.filter(c => c.classList.contains('laserDayRule')) : []; }
}
let moves = 0;
const pending = new Node('laserAreaItems'), ready = new Node('laserAreaItems');
const area = (key, list) => { const a = new Node('laserSection'); a.dataset.laserArea = key; a.empty = new Node('laserEmpty'); a.querySelector = s => s === '.laserEmpty' ? a.empty : s === '.laserAreaItems > :not(.laserDayRule)' ? list.children.find(c => !c.classList.contains('laserDayRule')) || null : null; return a; };
const areas = [area('ready', ready), area('pending', pending)];
const document = { querySelectorAll: s => s === '#libBody .laserAreaItems' ? [ready, pending] : s === '[data-laser-area]' ? areas : [], createElement: () => new Node() };
const card = st => { const c = new Node('setCard'); c._laserSet = st; c._laserSheets = []; return c; };
for (const st of shot) pending.appendChild(card(st));
const a0 = bridge.indexOf('  const DAY_WORDS'), a1 = bridge.indexOf('  function paint(only){');
assert(a0 > 0 && a1 > a0, 'arrange found in the bridge');
const ctx = { document, CNListActivity: A, records: new Map(), esc: s => String(s), Intl, Date };
vm.createContext(ctx);
vm.runInContext(bridge.slice(a0, a1) + '\nthis.arrange = arrange;', ctx);
ctx.arrange(); ctx.arrange(true);
const shown = pending.children.map(n => n.classList.contains('laserDayRule') ? `[${n.dataset.day} ${n.dataset.n}]` : name(n._laserSet));
assert.deepEqual(shown, ['[2026-11-11 1]', '11-11#2', '[2026-11-10 3]', '11-10#1', '11-10#2', '11-10#3', '[loose 2]', 'loose-new', 'loose-old'], 'each day opens with its rule; the loose sheets with theirs');
assert.equal(pending.children[0].attrs['aria-label'], 'Wed, Nov 11, 2026, 1 set', 'the rule names the day and its sets');
assert.equal(ready.children.length, 0, 'an empty area gets no rule');
assert.equal(areas[0].empty.hidden, false, 'and still says it is empty');
assert.equal(areas[1].hidden, false, 'In progress shows');
moves = 0; ctx.arrange(); assert.equal(moves, 0, 'a second pass moves nothing');
// a set that is touched or moves keeps the order; one more day's set opens its own rule
pending.children.find(n => n._laserSet && n._laserSet.setId === 'd')._laserSet.activityAt = 999;
pending.appendChild(card(set('e', '2026-11-12', 1, 1)));
ctx.arrange();
assert.deepEqual(pending.children.map(n => n.classList.contains('laserDayRule') ? `[${n.dataset.day}]` : name(n._laserSet)).slice(0, 8), ['[2026-11-12]', '11-12#1', '[2026-11-11]', '11-11#2', '[2026-11-10]', '11-10#1', '11-10#2', '11-10#3']);
// only loose sheets: nothing to separate
const solo = new Node('laserAreaItems'); solo.appendChild(card({ key: 'w', working: true, activityAt: 1 }));
document.querySelectorAll = s => s === '#libBody .laserAreaItems' ? [solo] : [];
ctx.arrange(); assert.equal(solo.children.length, 1, 'a list of loose sheets alone has no rule');

console.log('Library set order OK: sets by day, Set-1 up inside a day whatever was touched last, loose sheets after; a rule opens each day; a second pass moves nothing');
