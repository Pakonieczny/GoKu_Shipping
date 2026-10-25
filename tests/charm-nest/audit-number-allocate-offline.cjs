// AUDIT (R14 numbering, persistence): the page's real Sets.ensure / allocate (charm-nest-bridge.js) when the cloud goes away
// while a new set is being numbered. Required: whatever happens, the run's set map holds only real sets, and once the cloud
// is back the next set is numbered normally (no sheet or set stuck for good). The real code over fakes: no cloud, no browser.
//   node tests/charm-nest/audit-number-allocate-offline.cjs
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm'), path = require('node:path');
const O = require('../../charm-nest-orders.js');
const code = fs.readFileSync(path.join(__dirname, '../../charm-nest-bridge.js'), 'utf8');
const at = code.indexOf('const Sets = window.Sets = (() => {'), upto = code.indexOf('const ofRun = runId =>', at), end = code.indexOf('\n', upto);
assert(at > 0 && upto > at, 'the Sets code to test was found');
function world() {
  const calls = []; let counter = 0;
  const ctx = vm.createContext({ O, calls, B: { sets: new Map(), run: null }, S: { cloud: { ok: true } }, window: {}, today: () => '2026-10-12', labelOf: m => m, agent: () => {}, RunCtl: { renderBanner() {} },
    onSave: null,
    api: async (name, b) => { calls.push(b); if (b.op === 'setAllocate') { const seq = ++counter; return { ok: true, setId: 'set-' + seq, seq, day: b.day }; } return {}; } });
  vm.runInContext(code.slice(at, end) + `
    async function save(s) { if (typeof onSave === 'function') await onSave(s); }
    globalThis.T = { ensure, ofRun, byRun, keyOf };
  })();`, ctx);
  return ctx;
}
const RUN = 'run-x';
(async () => {
  const fails = [], latent = [];
  // latent: a fault of the code that no caller reaches today (reported, does not fail the file unless AUDIT_STRICT=1)
  const check = async (name, fn, isLatent) => { try { await fn(); console.log('ok   ' + name); } catch (e) { (isLatent && !process.env.AUDIT_STRICT ? latent : fails).push(name); console.log((isLatent ? 'LATENT ' : 'FAIL ') + name + '\n     ' + (e && e.message).split('\n')[0]); } };

  // A · offline at the moment of numbering: refused in words, and recovers when the cloud is back
  await check('A · offline before numbering: refused, nothing half made, the next set is numbered once the cloud is back', async () => {
    const c = world(), T = c.T;
    const s1 = await T.ensure(RUN, 'dispatch', {}); s1.sheetIds.push('gf-1', 'ss-1');
    c.S.cloud.ok = false;
    await assert.rejects(T.ensure(RUN, 'dispatch', { follow: s1 }), /Reconnect to the cloud/);
    for (const [k, v] of T.byRun()) assert(v && v.setId, `the set map holds a real set at ${k}`);
    c.S.cloud.ok = true;
    const s2 = await T.ensure(RUN, 'dispatch', { follow: s1 });
    assert.equal(s2.setId, 'set-2'); assert.deepEqual(Array.from(T.ofRun(RUN), x => x.setId), ['set-1', 'set-2']);
  });

  // B · the cloud drops WHILE the formed set it follows is being saved (Sets.ensure awaits save(followed) between its cloud check and the setAllocate call;
  //     S.cloud.ok is flipped by any failing cloudProbe meanwhile). Required: refused (or numbered), the map never holds a hole, and the next ask works once the cloud is back.
  await check('B · cloud drops during save(followed): the set map is not poisoned and the next set is numbered when the cloud is back', async () => {
    const c = world(), T = c.T;
    const s1 = await T.ensure(RUN, 'dispatch', {}); s1.sheetIds.push('gf-1', 'ss-1');
    c.onSave = async () => { c.S.cloud.ok = false; };            // a probe failed while the save was in flight
    let first = null; try { await T.ensure(RUN, 'dispatch', { follow: s1 }); } catch (e) { first = e; }
    const holes = [...T.byRun()].filter(([, v]) => !v).map(([k]) => k);
    c.onSave = null; c.S.cloud.ok = true;
    let again = null, s2 = null; try { s2 = await T.ensure(RUN, 'dispatch', { follow: s1 }); } catch (e) { again = e; }
    let order = null, orderErr = null; try { order = Array.from(T.ofRun(RUN), x => x.setId); } catch (e) { orderErr = e; }
    assert.deepEqual(holes, [], `the set map holds undefined at ${holes.join(', ')} (first ask threw: ${first && first.message}; asked again with the cloud back: ${again ? again.message : 'ok'}; Sets.ofRun: ${orderErr ? orderErr.message : 'ok'})`);
    assert.equal(again, null, 'asked again with the cloud back: ' + (again && again.message));
    assert.equal(orderErr, null, 'Sets.ofRun throws: ' + (orderErr && orderErr.message));
    assert.equal(s2 && s2.setId, 'set-2'); assert.deepEqual(order, ['set-1', 'set-2']);
  }, true);   // (latent: in charm-nest-1.html S.cloud.ok goes from true to false nowhere after the first answer, cloudProbe only runs again while it is false)

  // C · a formed set is followed while the cloud is off, then a page asks WITHOUT naming the set it follows (a reload of the Library, a second path):
  //     the server's per-run key hands back Set-1 itself. Required: never two different objects for one set in the run's map (two copies that save over each other's sheet list).
  await check('C · one set, one object: no second copy of the formed set after an offline follow', async () => {
    const c = world(), T = c.T;
    const s1 = await T.ensure(RUN, 'dispatch', {}); s1.sheetIds.push('gf-1', 'ss-1');
    c.S.cloud.ok = false; await T.ensure(RUN, 'dispatch', { follow: s1 }).catch(() => {});
    c.S.cloud.ok = true;
    // the fake server answers as op_setAllocate does for the run's plain dispatch key: the set already allocated for it (Set-1)
    c.api = async (name, b) => { c.calls.push(b); if (b.op === 'setAllocate') return b.after ? { ok: true, setId: 'set-2', seq: 2, day: b.day } : { ok: true, setId: 'set-1', seq: 1, day: b.day, existing: true }; return {}; };
    vm.runInContext('api = globalThis.api', c);
    const back = await T.ensure(RUN, 'dispatch', {});
    const copies = [...T.byRun().values()].filter(v => v && v.setId === 'set-1');
    assert.equal(copies.length, 1, `Set-1 is held ${copies.length} times in the run's map (the new copy has sheetIds ${JSON.stringify(back.sheetIds)}, the formed one ${JSON.stringify(s1.sheetIds)})`);
  }, true);   // (latent: the run's assembly always asks with {follow: last}; only legacy callers ask without it)

  if (fails.length) { console.log(`FAIL: audit-number-allocate-offline: ${fails.length} case(s): ${fails.join(' | ')}`); process.exit(1); }
  console.log('PASS: audit-number-allocate-offline' + (latent.length ? ` (${latent.length} latent: ${latent.join(' | ')})` : ''));
})().catch(e => { console.error(e); process.exit(1); });
