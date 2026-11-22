// FASTPICK (Paul, 22 Nov 2026): a save to the charm repository on Michelle's computer stayed at "Reading the lines again…" and failed, and the request after it waited behind it. The cloud's
// search for the open lines of a design (staleLines: keepsync / remake) read every open line one by one, two reads a line: two minutes for a run of 1,400 lines, past the unit's one-minute
// limit, so its result was never saved and every pass searched again. Offline, over _cloudFakes.cjs:
//   1  a run of 1,400 open lines: the two lines of the design whose pieces hold an older drawing are found, and nothing else
//   2  in a few batched reads (getAll), never one read a line
//   3  the requests unit saves a request's progress through ctx.callNow (no time limit) when the driver gives it; the driver gives it to every pass item
//   NODE_PATH=<deps> node tests/charm-nest/fast-pick.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path');
const T0 = Date.UTC(2026, 10, 22, 22, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
const log0 = console.log.bind(console); console.log = () => {}; console.warn = () => {};
let failed = 0; const ok = (n, c, d) => { log0((c ? 'OK   ' : 'FAIL ') + n + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 500))); if (!c) failed++; };
(async () => {
  const Exec = F.load('_charmNestCmdExec.js');
  const SKU = '9MM_DISC_8391', OTHER = 'MOON_1234', lines = {};
  F.seed('Charm_Master_Index/' + SKU, { sku: SKU, masterHash: 'new-hash' });
  F.seed('Charm_Master_Index/' + OTHER, { sku: OTHER, masterHash: 'm-hash' });
  for (let i = 0; i < 1400; i++) {
    const key = `${4200000000 + i}_${5300000000 + i}`, pid = key + '_1', mine = i % 700 === 0, sku = mine || i === 5 ? SKU : OTHER;
    lines[key] = { state: 'pooled', sku, poolIds: [pid] };
    F.seed('Charm_World_Lines/' + key, { at: T0, data: JSON.stringify({ spec: { designSku: sku } }) });
    F.seed('Charm_Pool/' + pid, { poolId: pid, sku, masterHash: mine ? 'old-hash' : sku === SKU ? 'new-hash' : 'm-hash', state: 'ready' });
  }
  let gets = 0, getAlls = 0;
  const db = new Proxy(F.db, { get(t, k) {
    if (k === 'getAll') return (...refs) => { getAlls++; return t.getAll(...refs); };
    if (k === 'collection') return name => { const c = t.collection(name); return new Proxy(c, { get(cc, kk) { if (kk === 'doc') return id => { const d = cc.doc(id); return new Proxy(d, { get(dd, kd) { if (kd === 'get') return () => { gets++; return dd.get(); }; const v = dd[kd]; return typeof v === 'function' ? v.bind(dd) : v; } }); }; const v = cc[kk]; return typeof v === 'function' ? v.bind(cc) : v; } }); };
    const v = t[k]; return typeof v === 'function' ? v.bind(t) : v; } });
  const ctx = { db, run: { runId: 'R', lines }, runId: 'R' };
  const t0 = Date.now(), found = await Exec.staleLines(ctx, [SKU]), ms = Date.now() - t0;
  ok('1 · of 1,400 open lines, the two lines of the design whose pieces hold an older drawing are found, nothing else', JSON.stringify(found.sort()) === JSON.stringify(['4200000000_5300000000', '4200000700_5300000700']), found);
  ok('2 · in a few batched reads, never one read a line (' + getAlls + ' batched, ' + gets + ' single, ' + ms + ' ms)', getAlls <= 12 && gets <= 3, { getAlls, gets, ms });
  const unit = fs.readFileSync(path.join(__dirname, '../../netlify/functions/_charmNestCmdUnit.js'), 'utf8'), drive = fs.readFileSync(path.join(__dirname, '../../netlify/functions/_charmNestDrive.js'), 'utf8');
  ok('3 · the requests unit saves progress and answers through ctx.callNow, and the driver gives it to every pass item', /const put = \(b, u\) => \(typeof ctx\.callNow === "function"/.test(unit) && /await put\(\{ op: "cmdWorkPut"/.test(unit) && /await put\(\{ op: "runCmdDone"/.test(unit) && /callNow: call/.test(drive));
  log0(failed ? `FAIL fast-pick (${failed})` : 'PASS fast-pick'); process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL fast-pick: ' + (e && e.stack || e)); process.exit(1); });
