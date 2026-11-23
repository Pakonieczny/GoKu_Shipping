// LINKLATE (Paul, 23 Nov 2026, orders 4236768670 and 4238823369: a listing linked LEMON → LEMON_7271 and the sheets kept the old charm; "I need this properly fixed once and for all"). The cloud's request that makes a charm's
// open orders again (keepsync) is asked seconds after a link is saved; it looked at once, with the orders as they were read BEFORE the link, found nothing and answered "Nothing to make again" for good. The real
// _charmNestCmdExec.js runs over the in-memory cloud (_cloudFakes.cjs):
//   1  Ana's press: the link saved at 16:53:53, asked at 16:53:57, the orders last read before: it waits for the next reading, never answers "nothing"
//   2  the orders read again: both lemon orders are picked, and their pieces are taken off to be made again; an order of another listing is not
//   3  a save of a drawing with no link (KEEPNOW): answered at once, as before; a link to ANOTHER charm, or an old one, changes nothing
//   4  an option of a listing linked to the charm, a pair, a huggie: wait too; links that cannot be read: wait
//   5  the pages that save a link and asked nobody (a charm picked in Review, an option answer) ask the cloud
//   LINK_ROOT=<another checkout> NODE_PATH=<deps> node tests/charm-nest/link-late.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path'), root = process.env.LINK_ROOT ? path.resolve(process.env.LINK_ROOT) : path.join(__dirname, '../..');
const T0 = Date.UTC(2026, 10, 23, 16, 53, 57), F = require(path.join(root, 'tests/charm-nest/_cloudFakes.cjs'))({ now: T0 });
const log0 = console.log.bind(console); console.log = () => {}; console.warn = () => {}; let failed = 0, n = 0; const ok = (m, c, d) => { n++; log0((c ? 'OK   ' : 'FAIL ') + m + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 400))); if (!c) failed++; };
(async () => {
  const { EXEC } = F.load('_charmNestCmdExec.js'), TO = 'LEMON_7271', A = '4236768670_5297762073', B = '4238823369_5300964535', C = '4200000001_5300000001';
  const lines = {}, read = (k, sku) => F.seed('Charm_World_Lines/' + k, { at: T0, data: JSON.stringify({ spec: { designSku: sku } }) });
  for (const k of [A, B]) { lines[k] = { state: 'written', sku: 'LEMON', poolIds: [k + '_1'] }; read(k, 'LEMON'); F.seed('Charm_Pool/' + k + '_1', { poolId: k + '_1', sku: 'LEMON', masterHash: 'm1', state: 'written' }); }
  lines[C] = { state: 'written', sku: 'MOON', poolIds: [C + '_1'] }; read(C, 'MOON'); F.seed('Charm_Pool/' + C + '_1', { poolId: C + '_1', sku: 'MOON', masterHash: 'm2', state: 'written' });
  for (const [s, h] of [['LEMON', 'm1'], [TO, 'm3'], ['MOON', 'm2']]) F.seed('Charm_Master_Index/' + s, { sku: s, masterHash: h });
  F.seed('Charm_World/summary', { at: T0 - 60000 });
  const stamp = ms => new Date(ms), alias = (id, d, ms) => F.seed('Charm_Sku_Aliases/' + id, Object.assign({ listingId: id, updatedAt: stamp(ms) }, d));
  const x = (ids, work) => ({ ctx: { db: F.db, run: { runId: 'R', lines }, runId: 'R', cloudSteps: ['nest', 'pool'], call: async () => ({ ok: true }) }, a: { ids }, cmd: { id: 'c', at: T0, by: 'Ana_M' }, work: work || {} });
  const ask = async (ids, work) => { const r = await EXEC.keepsync(x(ids, work)); return r && r.done ? 'done: ' + r.done.text : r && r.wait ? 'wait: ' + (r.wait.work.stage || '?') : JSON.stringify(r); };

  alias('1816160635', { bySku: { LEMON: TO }, by: 'Ana_M' }, T0 - 4000);
  let r = await ask([TO]); ok('1 the link saved 4 s before: it waits for the next reading of the orders (' + r + ')', r === 'wait: world', r);
  let s = await EXEC.keepsync(x([TO], { stage: 'world', asked: T0, at: T0 })); ok('1 still waiting while the orders have not been read again', !!(s.wait && s.wait.work.stage === 'world'), s);
  { const w = await EXEC.keepsync(x([TO])); ok('1 it waits from the link\'s own time (the reading the save starts is the one, not the one after it)', !!(w.wait && w.wait.work.asked === T0 - 4000), w); F.seed('Charm_World/summary', { at: T0 - 1000 }); const z = await EXEC.keepsync(x([TO], w.wait ? w.wait.work : {})); ok('1 a reading begun after the link, 3 s before the request, is enough', !!(z.wait && z.wait.work.stage === 'pick'), z); F.seed('Charm_World/summary', { at: T0 - 60000 }); }
  read(A, TO); read(B, TO); F.seed('Charm_World/summary', { at: T0 + 45000 });
  s = await EXEC.keepsync(x([TO], { stage: 'world', asked: T0, at: T0 })); ok('2 the orders were read again: it looks now', !!(s.wait && s.wait.work.stage === 'pick'), s);
  s = await EXEC.keepsync(x([TO], s.wait ? s.wait.work : {})); ok('2 both lemon orders are made again, the other listing\'s is not', !!(s.wait && s.wait.work.stage === 'vacate' && JSON.stringify((s.wait.work.keys || []).sort()) === JSON.stringify([A, B])), s);
  for (const k of [A, B]) { F.seed('Charm_Pool/' + k + '_1', { poolId: k + '_1', sku: TO, masterHash: 'm3', state: 'written' }); }
  F.seed('Charm_World/summary', { at: T0 - 60000 });
  r = await ask(['MOON']); ok('3 a drawing saved with no link: answered at once (' + r + ')', /^done: Nothing to make again/.test(r), r);
  alias('1700000001', { bySku: { X: 'OTHER_1' } }, T0 - 2000); r = await ask(['MOON']); ok('3 a link to another charm changes nothing', /^done: Nothing/.test(r), r);
  alias('1700000002', { sku: 'MOON', v: 2 }, T0 - 3600000); r = await ask(['MOON']); ok('3 an old link changes nothing', /^done: Nothing/.test(r), r);
  alias('1700000003', { pairBySku: { P: { L: 'PAIR_L', R: 'PAIR_R' } } }, T0 - 1000); ok('4 a pair: waits', (await ask(['PAIR_R'])) === 'wait: world'); alias('1700000004', { huggie: 'HUG_1' }, T0 - 1000); ok('4 a huggie: waits', (await ask(['HUG_1'])) === 'wait: world');
  const om = /LINKS = \["[^"]+", "([^"]+)"\]/.exec(fs.readFileSync(path.join(root, 'netlify/functions/_charmNestCmdExec.js'), 'utf8')); F.seed((om ? om[1] : 'Charm_Option_Map') + '/1700000005', { listingId: '1700000005', map: { charm: { maple: { field: 'design', value: 'OPT_9' } } }, updatedAt: stamp(T0 - 1000) }); ok('4 an option of a listing: waits', (await ask(['OPT_9'])) === 'wait: world');
  { const y = x(['MOON']); y.ctx.db = new Proxy(F.db, { get(t, k) { if (k === 'collection') return name => { if (/Alias/.test(name)) throw new Error('not read'); return t.collection(name); }; const v = t[k]; return typeof v === 'function' ? v.bind(t) : v; } }); const q = await EXEC.keepsync(y); ok('4 links that cannot be read: waits', !!(q.wait && q.wait.work.stage === 'world'), q); }
  { const lib = fs.readFileSync(path.join(root, 'netlify/functions/charmNestLibrary.js'), 'utf8'); ok('4 the two places the links are kept are the ones the cloud writes', !!om && lib.includes(`ALIASES = "Charm_Sku_Aliases"`) && lib.includes(`OPTMAP = "${om[1]}"`), om && om[1]); ok('4 a saved link starts a new reading of the orders', /WORLD_TRIGGERS = new Set\(\[[^\]]*"aliasPut"/.test(lib) && /WORLD_TRIGGERS = new Set\(\[[^\]]*"op(tionMapPut)/.test(lib)); }
  const br = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8'); ok('5 a charm picked in Review and an option answer ask the cloud', (br.match(/relinkNow\(\)[^\n]*askRelink\(\[sku\]\)/g) || []).length === 2 && /function askRelink\(skus\)/.test(br));
  log0(`${failed ? 'FAIL' : 'PASS'} link-late (${n - failed} of ${n})`); process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL link-late: ' + (e && e.stack || e)); process.exit(1); });
