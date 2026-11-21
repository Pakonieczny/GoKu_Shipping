// CLOUDDRIVE build step 1b (C1, E5): every op of charmNestLibrary is in exactly one class (READ, PRESS, PRODUCTION; CLOUD_ONLY for the cloud's own ops), the way
// scripts/build-netlify.cjs fails an unclassified function; the cloud's list of ops holds only ops that exist, and none of the denied ones. Offline.
//   node tests/charm-nest/op-class.cjs
'use strict';
const F = require('./_cloudFakes.cjs')({ clock: false });
const lib = F.load('charmNestLibrary.js'), C = F.load('_charmNestOpClass.js');
let failed = 0;
const ok = (name, cond, detail) => { console.log((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + JSON.stringify(detail).slice(0, 900))); if (!cond) failed++; };
const ops = Object.keys(lib.ops), classes = { READ: C.READ, PRESS: C.PRESS, PRODUCTION: C.PRODUCTION, CLOUD_ONLY: C.CLOUD_ONLY };
const none = ops.filter(op => !Object.values(classes).some(s => s.has(op)));
const twice = ops.filter(op => Object.values(classes).filter(s => s.has(op)).length > 1);
ok(`every one of the library's ${ops.length} ops is classified (a new op must be put in a class)`, !none.length, none);
ok('no op is in two classes', !twice.length, twice);
const ghosts = Object.entries(classes).flatMap(([k, s]) => [...s].filter(op => k !== 'CLOUD_ONLY' && !ops.includes(op) && op !== 'runLinesRetire'));
ok('every classified op exists (a class names no op that is gone; CLOUD_ONLY and runLinesRetire name ops their later build steps add)', !ghosts.length, ghosts);
ok('a CLOUD_ONLY op that is built is in no other class (the handler refuses it to a page in every mode); the rest are added by their later build steps', [...C.CLOUD_ONLY].filter(op => ops.includes(op)).every(op => C.classOf(op) === 'CLOUD_ONLY' && C.CLOUD_OPS.has(op)));
const missing = [...C.CLOUD_OPS].filter(op => !ops.includes(op));
ok('every op on the cloud\'s list exists', !missing.length, missing);
ok('the cloud\'s list holds none of the denied ops', [...C.DENIED].every(op => !C.CLOUD_OPS.has(op)));
ok('every denied op exists (each has its own refusal test in cloud-foundations.cjs)', [...C.DENIED].every(op => ops.includes(op)), [...C.DENIED].filter(op => !ops.includes(op)));
ok('the cloud\'s list holds no Etsy write, no Etsy switch press, no world build, no agent', ![...C.CLOUD_OPS].some(op => /etsy|zodiac|worldBuild|startAgent|sandboxPull|customSave/i.test(op) && op !== 'etsyStopGet'), [...C.CLOUD_OPS].filter(op => /etsy|zodiac|worldBuild|startAgent/i.test(op)));
ok('the two ops the cloud reads that could reach Etsy are made safe before they run (listingSkus cache only, listingPhotos without prepare)', (() => { const a = C.CLOUD_BODY.listingSkus({ nameIds: [1], refreshIds: [1] }), b = C.CLOUD_BODY.listingPhotos({ prepare: true }); return a.cacheOnly === true && !a.nameIds && !a.refreshIds && !('prepare' in b); })());
ok('runPut, poolPut, putSheet, setAllocate and archiveEmptySheet are PRODUCTION; runOpen, runControl and sheetLayoutPatch are presses; worldGet is a read', ['runPut', 'poolPut', 'putSheet', 'setAllocate', 'archiveEmptySheet'].every(op => C.classOf(op) === 'PRODUCTION') && ['runOpen', 'runControl', 'sheetLayoutPatch'].every(op => C.classOf(op) === 'PRESS') && C.classOf('worldGet') === 'READ');
ok('S2: reviewSettledPut is a person\'s press (PRESS) and reviewSettledGet a read (READ); neither is a cloud call', C.classOf('reviewSettledPut') === 'PRESS' && C.classOf('reviewSettledGet') === 'READ' && !C.CLOUD_OPS.has('reviewSettledPut'));
console.log(failed ? `FAIL op-class · ${failed} check(s)` : 'PASS op-class');
process.exit(failed ? 1 : 0);
