// ORDERART (Paul, 22 Nov 2026, order 4239655218: "Disorder that fixed correctly however, the thumbnails in the order did not get updated with the engraving"): both discs carry their words as the Charm
// Editor's parts on the FRONT and nothing on the back; the order window's engraving cards drew only the back job's face, empty. They now show a face the Charm Editor drew on, as Engrave's card does.
//   1  the card module: a piece whose back job has no words and whose FRONT carries the Charm Editor's parts is drawn with its Front (the art face) beside the Back, when asked with artFaces
//   2  the order window asks it (artFaces from the page's own test, CNPieceArtOn), and the page shares that test; both files load under new tokens
//   node tests/charm-nest/order-art-face.cjs
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '../..');
let failed = 0; const ok = (n, c, d) => { console.log((c ? 'OK   ' : 'FAIL ') + n + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 600))); if (!c) failed++; };
const sb = { console, Math, JSON, Object, Array, String, Number, Set, Map, WeakMap, Promise, setTimeout, clearTimeout };
sb.self = sb; sb.window = sb; vm.createContext(sb);
vm.runInContext(fs.readFileSync(path.join(ROOT, 'charm-nest-engrave-card.js'), 'utf8'), sb);
const C = sb.CharmNestEngraveCard;
const LINE = '4239655218_5300163468', job = { key: LINE + '#D2', slot: 'D2', state: 'review', text: '', lines: [], copies: [LINE + '_2'], decision: { artOnly: true, why: 'Words removed' } };
const art = (jobs, face) => (face === 'front' ? 'h-front-art' : '');
const without = C.facesOf(LINE, 'D2', [job], null, {});
const withArt = C.facesOf(LINE, 'D2', [job], null, { artFaces: art });
ok('1 · asked with artFaces, the Front the Charm Editor drew on is drawn as the art face beside the back job', !!withArt.front && withArt.front.art === true && withArt.front.face === 'front' && !!withArt.back, { front: withArt.front, back: !!withArt.back });
ok('1 · without it (as the order window was) only the empty back was drawn', !without.front && !!without.back, { front: without.front });
const oe = fs.readFileSync(path.join(ROOT, 'charm-nest-order-engraving.js'), 'utf8'), br = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8'), html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8');
ok('2 · the order window passes artFaces from the page\'s own test (CNPieceArtOn); the page shares it', /const popt = \{[^\n]*artFaces: \(jobs, face\) => \(typeof root\.CNPieceArtOn === 'function'/.test(oe) && /window\.CNPieceArtOn = pieceArtOn;/.test(br));
ok('2 · both files load under new tokens', /charm-nest-order-engraving\.js\?v=[^"]*-oa1"/.test(html) && /charm-nest-bridge\.js\?v=[^"]*-oa1"/.test(html));
console.log(failed ? `FAIL order-art-face (${failed})` : 'PASS order-art-face'); process.exit(failed ? 1 : 0);
