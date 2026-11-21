// CLOUDDRIVE build step 1b (E4, E5, E1): a static check of every file the cloud driver is made of (and of every file a later build step ports into it: add it to CLOUD_FILES).
// None of them reads the library's ops table (only Lib.call and Lib.cloudOps), asks Etsy for a listing's SKU table, requires a module that calls Etsy, names an Etsy address,
// builds the world or takes the world's lease, or passes force / sweep. Reads files only.
//   node tests/charm-nest/cloud-static.cjs
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..'), fn = f => path.join('netlify/functions', f);
const CLOUD_FILES = [fn('_charmNestDrive.js'), fn('_charmNestLease.js'), fn('_charmNestOpClass.js'), fn('charmNestDrive-background.js'), fn('charmNestDriveCron.js'),
  fn('_charmNestIntake.js'), fn('_charmNestPieceHost.js'), 'charm-nest-pool-make.js',   // (build steps 3 and 4: the intake unit, the piece maker's host and the piece maker it runs)
  fn('_charmNestPlaceUnit.js'), fn('_charmNestPlaceOp.js'), 'charm-nest-place.js',   // (build step 5: the placement unit, placeOnSheet, the shared sheet choice)
  fn('_charmNestTakeOff.js'), fn('_charmNestTakeOffOps.js')];   // (build step 9: the take-off unit and its ops)
CLOUD_FILES.push(fn('_charmNestSteps.js'), fn('_charmEngraveFiles.js'), fn('_charmEngraveFonts.js'), fn('_charmEngravePiece.js'), fn('_charmEngraveOps.js'), 'charm-nest-back-file.js');   // (build step 11: engraving in the cloud)
CLOUD_FILES.push(fn('_charmNestCommit.js'), fn('_charmNestCommitOps.js'), fn('_charmNestArchiveRecord.js'));   // (build step 12: sending finished sets to the Design Station)
CLOUD_FILES.push(fn('_charmNestCmdOps.js'), fn('_charmNestCmdExec.js'), fn('_charmNestCmdUnit.js'), fn('_charmNestSweeps.js'));   // (build step 13: a person's requests and the repair sweeps; _charmNestSteps.js is listed above)
const RULES = [
  [/\bLib\.ops\b|\.ops\[|\bops\s*=\s*Lib\b/, 'reads the library\'s ops table (the cloud gets Lib.call and Lib.cloudOps only)'],
  [/\b(wantTables|askTables|askTable|tablesLoad|getListingInventory)\b/, 'asks Etsy for a listing\'s SKU table'],
  [/require\(\s*["'][^"']*(_etsyMailEtsy|_etsyImageCache|_charmNestEtsySku|_charmNestEtsyLink|_charmNestEtsyZodiac|_charmNestSandboxPull|_charmNestWorldLease|etsyRateLimiter)["']\s*\)/, 'requires a module that calls Etsy, or the world\'s lease'],
  [/openapi\.etsy\.com|api\.etsy\.com|etsystatic\.com/, 'names an Etsy address'],
  [/["']worldBuild["']|charmNestWorld-background/, 'starts a world build'],
  [/\b(force|sweep)\s*:\s*true\b/, 'passes force or sweep']
];
let failed = 0; const ok = (name, cond, detail) => { console.log((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + JSON.stringify(detail))); if (!cond) failed++; };
for (const f of CLOUD_FILES) {
  const text = fs.readFileSync(path.join(root, f), 'utf8'), code = text.split('\n').filter(l => !/^\s*(\/\/|\/\*|\*)/.test(l)).join('\n');
  const hits = RULES.filter(([re]) => re.test(f === fn('_charmNestOpClass.js') ? code.replace(/words\(`[\s\S]*?`\)/g, '') : code)).map(([, why]) => why);
  ok(`${f}: ${hits.length ? hits.join('; ') : 'none of the forbidden things'}`, !hits.length, hits);
}
// the driver writes only through Lib.call: the background function hands it call and cloudOps, never the module
const bg = fs.readFileSync(path.join(root, fn('charmNestDrive-background.js')), 'utf8');
ok('charmNestDrive-background gives the driver { call, cloudOps } only', /Lib:\s*\{\s*call:\s*Lib\.call,\s*cloudOps:\s*Lib\.cloudOps\s*\}/.test(bg));
// firebaseOrders has no Etsy call (only comments), so the cloud may call it in-process later (claim / complete)
const fo = fs.readFileSync(path.join(root, 'firebaseOrders.js'), 'utf8').split('\n').filter(l => !/^\s*(\/\/|\/\*|\*)/.test(l)).join('\n');
ok('firebaseOrders.js makes no Etsy call', !/openapi\.etsy\.com|api\.etsy\.com|_etsyMailEtsy|etsyFetch/.test(fo));
// the world's lease is not edited (E3) and only the world build uses it
const users = fs.readdirSync(path.join(root, 'netlify/functions')).filter(f => f.endsWith('.js') && /require\(["']\.\/_charmNestWorldLease["']\)/.test(fs.readFileSync(path.join(root, 'netlify/functions', f), 'utf8')));
ok('only the world build and its doors take the world\'s lease (charmNestWorld-background, charmNestWorldCron, charmNestLibrary\'s worldBuild / worldSoon)', users.sort().join(',') === ['charmNestLibrary.js', 'charmNestWorld-background.js', 'charmNestWorldCron.js'].join(','), users);
console.log(failed ? `FAIL cloud-static · ${failed} check(s)` : 'PASS cloud-static');
process.exit(failed ? 1 : 0);
