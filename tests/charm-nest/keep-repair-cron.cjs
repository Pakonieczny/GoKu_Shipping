// The scheduled repair of saved custom designs (charmNestKeepRepairCron): it calls the library's customSaveRepair { etsy: true } with no page,
// never throws, and is registered everywhere the build and Netlify need it (the function's own config, netlify.toml, the function manifest).
// Node only, on a fake library.   node tests/charm-nest/keep-repair-cron.cjs
'use strict';
const assert = require('assert'), fs = require('fs'), path = require('path'), Module = require('module');
const root = path.resolve(__dirname, '../..'), fn = path.join(root, 'netlify/functions/charmNestKeepRepairCron.js'), libPath = path.join(root, 'netlify/functions/charmNestLibrary.js');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };

function load(ops) {
  delete require.cache[fn];
  require.cache[libPath] = Object.assign(new Module(libPath), { id: libPath, filename: libPath, loaded: true, exports: { ops } });
  return require(fn);
}
(async () => {
  const calls = [];
  let cron = load({ customSaveRepair: async b => { calls.push(b); return { ok: true, pass: 'done', etsy: { ran: 1 } }; } });
  let r = await cron.handler({});
  ok(r.statusCode === 200 && JSON.parse(r.body).ok === true, 'a pass answers 200 ok: ' + r.body);
  ok(calls.length === 1 && calls[0].etsy === true && calls[0].maxEtsy === 3, 'it asks for the Etsy steps too, at most 3 a pass: ' + JSON.stringify(calls));
  ok(!('olderThanMs' in calls[0]) && !('force' in calls[0]) && !('repoToken' in calls[0]), 'it never asks for the short age, a forced pass or a token (nothing a person must give)');

  cron = load({ customSaveRepair: async () => { throw new Error('firestore is down'); } });
  r = await cron.handler({});
  ok(r.statusCode === 500 && JSON.parse(r.body).ok === false && /firestore is down/.test(r.body), 'a failing pass answers 500 and does not throw: ' + r.body);

  cron = load({ customSaveRepair: () => { throw new Error('sync throw'); } });
  r = await cron.handler({});
  ok(r.statusCode === 500, 'a synchronous throw is caught too');

  // registered everywhere
  const cfg = cron.config && cron.config.schedule;
  ok(cfg === '*/10 * * * *', 'the function declares its schedule: ' + cfg);
  const toml = fs.readFileSync(path.join(root, 'netlify.toml'), 'utf8');
  const m = /\[functions\."charmNestKeepRepairCron"\]\s*\n\s*schedule\s*=\s*"([^"]+)"/.exec(toml);
  ok(m && m[1] === cfg, 'netlify.toml carries the same schedule: ' + (m && m[1]));
  const man = require(path.join(root, 'scripts/netlify-function-entries.json'));
  ok(man.endpoints.includes('charmNestKeepRepairCron.js') && !man.modules.includes('charmNestKeepRepairCron.js'), 'the build manifest lists it as an endpoint');
  ok(man.modules.includes('_charmNestCustomRepair.js'), 'and its helper as a module');
  console.log('keep-repair-cron OK: ' + n + ' checks');
})().catch(e => { console.error(e); process.exit(1); });
