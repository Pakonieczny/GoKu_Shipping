/* MCREBIND (Paul, 27 Nov 2026, GF_Nov.24.26_Set-2_Sheet-1: Manual Control said "The window showed an older copy of this sheet; it is read again" on a sheet released long before). Node only:
   node tests/charm-nest/rt1127-mc.cjs. The page's copy of the sheet was built again in the background (same charms, same places, other objects); the window's pieces kept the old objects.
   M1 the same pieces in the same places: the window takes the page's charms and Manual Control may start
   M2 a piece moved, one more or less, or a charm the copy cannot draw: nothing taken, the sheet is read again as before
   M3 the press uses it before its "older copy" check */
'use strict';
const fs = require('fs'), path = require('path'), src = fs.readFileSync(path.join(__dirname, '..', '..', 'charm-nest-sheetwin.js'), 'utf8');
let passed = 0, failed = 0; const ok = (n, c, d) => { console.log((c ? 'OK   ' : 'FAIL ') + n + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 400))); c ? passed++ : failed++; };
const a = src.indexOf('  function mcRebind(shown, sh) {'); if (a < 0) { ok('mcRebind exists', false); console.log(`rt1127-mc: ${passed} passed, ${failed} failed`); process.exit(1); }
const b = src.indexOf('\n  }\n', a) + 4, mcRebind = new Function(src.slice(a, b) + '; return mcRebind;')();
const charm = id => ({ id, bits: new Uint8Array(4), w: 2, h: 2 }), pose = (cx, cy, angle) => ({ cxPt: cx, cyPt: cy, angle, scale: 0.971 });
const page = () => ({ placements: [Object.assign({ id: 'a' }, pose(10, 20, 90)), Object.assign({ id: 'b' }, pose(40, 25, 360))], charms: [charm('a'), charm('b')] });
const shownOf = () => [{ id: 'a', p: pose(10, 20, 90), c: charm('a') }, { id: 'b', p: pose(40, 25, 0), c: charm('b') }];
{ const sh = page(), shown = shownOf(), r = mcRebind(shown, sh); ok('M1 same places (360° is 0°), other objects: taken, every piece now draws the page\'s own charm', r === true && shown.every(x => x.c === sh.charms.find(c => c.id === x.id))); }
{ const sh = page(); sh.placements[1].cxPt = 41; const shown = shownOf(), was = shown.map(x => x.c), r = mcRebind(shown, sh); ok('M2a a piece moved by a point: not taken, nothing changed', r === false && shown.every((x, i) => x.c === was[i])); }
{ const sh = page(); sh.placements.pop(); sh.charms.pop(); ok('M2b one piece fewer on the page: not taken', mcRebind(shownOf(), sh) === false); }
{ const sh = page(); delete sh.charms[0].bits; ok('M2c a charm the page cannot draw: not taken', mcRebind(shownOf(), sh) === false); }
const t = src.indexOf('if (W.geom && mcRebind(shown, sh))'), u = src.indexOf('The window showed an older copy of this sheet');
ok('M3 the press takes the page\'s charms before its "older copy" check', t > 0 && u > t && u - t < 600);
console.log(`rt1127-mc: ${passed} passed, ${failed} failed`); process.exit(failed ? 1 : 0);
