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

// 3. arrange() on a stand-in DOM (ONELIST, Paul 19 Nov 2026: "all the sets in order either descending or ascending based on date and then all the items, all the drafts, all the stuff in progress right at the bottom"):
//    the sets of BOTH areas are drawn in one list by day (the cards stay in the area that says where they stand), a rule opens each day (one for the day, not one for each area), the loose sheets and drafts come last.
class Node {
  constructor(cls) { this.className = cls || ''; this.dataset = {}; this.children = []; this.parent = null; this.attrs = {}; this.innerHTML = ''; this.hidden = false; this.style = {}; }
  get parentElement() { return this.parent; }
  get classList() { const n = this; return { contains: c => n.className.split(/\s+/).includes(c), add: c => { if (!n.className.split(/\s+/).includes(c)) n.className = (n.className + ' ' + c).trim(); } }; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  insertBefore(n, ref) { if (n.parent) n.parent.children.splice(n.parent.children.indexOf(n), 1); const i = ref ? this.children.indexOf(ref) : -1; this.children.splice(i < 0 ? this.children.length : i, 0, n); n.parent = this; moves++; return n; }
  appendChild(n) { return this.insertBefore(n, null); }
  remove() { if (this.parent) { this.parent.children.splice(this.parent.children.indexOf(this), 1); this.parent = null; moves++; } }
  querySelectorAll() { return []; }
}
let moves = 0;
const body = new Node('libBody'), pending = new Node('laserAreaItems'), ready = new Node('laserAreaItems');
const area = (key, list) => { const a = new Node('laserSection'); a.dataset.laserArea = key; a.empty = new Node('laserEmpty'); a.querySelector = s => s === '.laserEmpty' ? a.empty : s === '.laserAreaItems > :not(.laserDayRule)' ? list.children.find(c => !c.classList.contains('laserDayRule')) || null : null; return a; };
const areas = [area('ready', ready), area('pending', pending)];
body.children.push(...areas); areas.forEach(a => { a.parent = body; });
const styles = []; const document = { getElementById: id => id === 'libBody' ? body : null, head: { appendChild: n => styles.push(n) }, querySelectorAll: s => s === '#libBody [data-laser-area="pending"] .laserAreaItems' ? [pending] : s === '[data-laser-area]' ? areas : [], querySelector: s => s === '#libBody [data-laser-area="ready"] .laserAreaItems' ? ready : null, createElement: () => new Node() };
const card = st => { const c = new Node('setCard'); c._laserSet = st; c._laserSheets = []; return c; };
// the 11 Nov list, with one of its sets (10 Nov, Set-3) READY: it stands in the other area
for (const st of shot) (st.setId === 'b' ? ready : pending).appendChild(card(st));
const a0 = bridge.indexOf('  const DAY_WORDS'), a1 = bridge.indexOf('  function paint(only){');
assert(a0 > 0 && a1 > a0, 'arrange found in the bridge');
const ctx = { document, CNListActivity: A, records: new Map(), esc: s => String(s), Intl, Date };
vm.createContext(ctx);
vm.runInContext(bridge.slice(a0, a1) + '\nthis.arrange = arrange;', ctx);
// what is DRAWN, top to bottom: the Library's row of cards (the rules, then each area's cards) by `order`
// (LASERBOX, Paul 22 Nov 2026: Laser cutting is its own box at the top; the one list by day below it is everything else)
const drawn = () => [...body.children.filter(n => n.classList.contains('laserDayRule')), ...pending.children].sort((x, y) => +x.style.order - +y.style.order).map(n => n.classList.contains('laserDayRule') ? `[${n.dataset.day} ${n.dataset.n}]` : name(n._laserSet));
const boxed = () => ready.children.map(n => name(n._laserSet));
A.set('library', { direction: 'desc' });
ctx.arrange(); ctx.arrange(true);
assert.deepEqual(boxed(), ['11-10#3'], 'LASERBOX: the ready set is in the Laser cutting box, not in the list below');
assert.deepEqual(drawn(), ['[2026-11-11 1]', '11-11#2', '[2026-11-10 2]', '11-10#1', '11-10#2', '[loose 2]', 'loose-new', 'loose-old'], 'newest first: every other set by day, each day opens with ONE rule, the drafts last');
assert.equal(body.classList.contains('laserOne'), true, 'the Library is one row of cards'); assert.equal(styles.length, 1, 'with its one style');
assert.equal(body.children.find(n => n.classList.contains('laserDayRule')).attrs['aria-label'], 'Wed, Nov 11, 2026, 1 set', 'the rule names the day and its sets');
assert(ready.children.every(n => !n.classList.contains('laserDayRule')) && pending.children.every(n => !n.classList.contains('laserDayRule')), 'no rule is kept inside an area');
assert.equal(ready.children.length, 1, 'the ready set stays in its area (Approve, the drops and the step rail read it)');
assert.equal(areas[1].hidden, false, 'In progress shows');
moves = 0; ctx.arrange(); assert.equal(moves, 0, 'a second pass moves nothing');
// flipped: oldest day first, Set-1 up inside a day, the drafts still last
A.set('library', { direction: 'asc' }); ctx.arrange();
assert.deepEqual(drawn(), ['[2026-11-10 2]', '11-10#1', '11-10#2', '[2026-11-11 1]', '11-11#2', '[loose 2]', 'loose-old', 'loose-new'], 'oldest first: the same, the other way, the drafts last');
A.set('library', { direction: 'desc' });
// a set that is touched or moves keeps the order; one more day's set opens its own rule
pending.children.find(n => n._laserSet && n._laserSet.setId === 'd')._laserSet.activityAt = 999;
pending.appendChild(card(set('e', '2026-11-12', 1, 1)));
ctx.arrange();
assert.deepEqual(drawn().slice(0, 7), ['[2026-11-12 1]', '11-12#1', '[2026-11-11 1]', '11-11#2', '[2026-11-10 2]', '11-10#1', '11-10#2']);
// only loose sheets: nothing to separate
const solo = new Node('laserAreaItems'); solo.appendChild(card({ key: 'w', working: true, activityAt: 1 }));
document.querySelectorAll = s => s === '#libBody [data-laser-area="pending"] .laserAreaItems' ? [solo] : []; document.querySelector = () => null;
for (const r of body.children.filter(n => n.classList.contains('laserDayRule'))) r.remove();
ctx.arrange(); assert.equal(body.children.filter(n => n.classList.contains('laserDayRule')).length, 0, 'a list of loose sheets alone has no rule');

console.log('Library set order OK: Laser cutting in its own box; every other set in one list by day (both ways), Set-1 up inside a day, one rule a day, the drafts and loose sheets last; a second pass moves nothing');
