/* STILLBAR (Paul, 27 Nov 2026, screen recording of the Design & Engrave bar: "I don't want to see this happen in the engraving menu. It interrupts the engraving workflow"). Node only:
   node tests/charm-nest/rt1127-stillbar.cjs. The Working chip came and went between the tabs and the search box, moving every control after it.
   S1 the chip stands last in the bar   S2 idle it is hidden but its room is kept by the bar's rule   S3 working and idle again: the same node, never rebuilt, nothing inserted or removed */
'use strict';
const fs = require('fs'), path = require('path'), ROOT = path.join(__dirname, '..', '..'), br = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8'), html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8');
let passed = 0, failed = 0; const ok = (n, c, d) => { console.log((c ? 'OK   ' : 'FAIL ') + n + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d))); c ? passed++ : failed++; };
const a = br.indexOf('  function renderWorking(v, jobs) {'), b = br.indexOf('\n  }\n', a) + 4;
// a small element model: children, appendChild moves, innerHTML builds the chip once
let built = 0, moves = 0;
const el = (tag, cls) => { const e = { tag, cls, children: [], parentElement: null, hidden: false, _html: '', attrs: {},
  get lastElementChild() { return this.children[this.children.length - 1] || null; }, get firstElementChild() { return this._chip || null; },
  set innerHTML(v) { built++; this._html = v; this._chip = { b: { textContent: '1' } }; }, querySelector(q) { return q === 'b' ? this._chip && this._chip.b : null; },
  appendChild(c) { if (c.parentElement) { c.parentElement.children = c.parentElement.children.filter(x => x !== c); moves++; } c.parentElement = this; this.children.push(c); return c; },
  closest(sel) { let p = this.parentElement; while (p) { if (sel === '.egBar' && p.cls === 'egBar') return p; p = p.parentElement; } return null; } }; return e; };
const bar = el('div', 'egBar'), tabs = el('span', 'controlGroup'), host = el('span', 'working'), search = el('span', 'search'), tools = el('span', 'tools'), ce = el('button', 'ce');
bar.appendChild(tabs); tabs.appendChild(host); bar.appendChild(search); bar.appendChild(tools); bar.appendChild(ce); moves = 0;
const v = { querySelector: q => (q === '[data-eg-working]' ? host : null) };
let working = new Set();
const renderWorking = new Function('isWorking', br.slice(a, b) + '; return renderWorking;')(j => working.has(j));
renderWorking(v, ['j1', 'j2']);
ok('S1 the chip stands last in the bar, after the Charm Editor button', bar.lastElementChild === host && host.parentElement === bar);
ok('S2 idle: hidden, and the bar\'s rule keeps its room (no display:none)', host.hidden === true && /\.egBar>\[data-eg-working\]\[hidden\]\{display:inline-flex;visibility:hidden\}/.test(html));
const builtBefore = built, movesBefore = moves; working = new Set(['j1']); renderWorking(v, ['j1', 'j2']); const shown = !host.hidden, cnt = host.querySelector('b').textContent; working = new Set(); renderWorking(v, ['j1', 'j2']);
ok('S3 working then idle: the same chip shown and hidden in place (built once, not moved again), its count said', shown && cnt === '1' && host.hidden === true && built === builtBefore && moves === movesBefore, { built, builtBefore, moves, movesBefore, cnt });
console.log(`rt1127-stillbar: ${passed} passed, ${failed} failed`); process.exit(failed ? 1 : 0);
