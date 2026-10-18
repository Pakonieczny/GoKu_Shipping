// MASTERNEW (Paul, 18 Oct 2026, on "SKUs_CORRECTED_2026-1018.ai": "This is the updated file. Any existing/duplicated SKUs must be removed in favour of the new file. Please update the system.")
// The newest master file wins. Headless Chromium over the in-memory stand-in of the site (tests/charm-nest/bridge-server.cjs), the Master tab Paul drops files on; nothing leaves this machine.
//   1 · an older file holds A, B and a pair P; a newer file brings B (drawn larger), C, and P drawn as ONE body:
//       B is re-indexed from the newer file (its drawing, size and file reference), with what a person set on it kept (angle, engravable, facing, a confirmation, a field the index never writes);
//       C is added; A is untouched (still the older file's, its design file as it was); P stays the pair the library holds (pairKept: one body never overwrites a pair);
//       one entry per SKU, nothing blocked as "in another master too"; the card says "2 SKU line(s) replaced with this file's drawings (1 at a different size)" with old file → new file and
//       old size → new size; the entry records what it replaced and who did it; the older file's upload-history record no longer lists B;
//   2 · the newer file added again changes nothing (nothing written, nothing uploaded, the entries as they were);
//   3 · a design saved from an order (KEEP R4) carried by a newer master is never replaced;
//   4 · "keep the library's drawings" leaves a SKU of an older file as it is (the old rule) and says how to replace it;
//   5 · the server rule alone (putIndex): `newest` replaces instead of blocking, without it a SKU in two masters is blocked as before.
//   PW_DIR=/opt/node-tools/node_modules node tests/charm-nest/master-newest.cjs
'use strict';
const assert = require('assert');
const { scene } = require('./_masterScene.cjs'), R = require('./_masterRoutes.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; console.log('  ok  ' + m); };
const IA = '0.2 0.35 0.85', IB = '0.85 0.2 0.2', IC = '0.2 0.6 0.3';
const MM = 25.4 / 72;
const libSkus = st => st.list('Charm_Master_Index').map(e => e._id).sort();

(async () => {
  const br = await R.browser(), st = br.st, page = br.page;
  const job = () => page.evaluate(() => { const j = [...B.master.jobs.values()].pop(); return { state: j.state, error: j.error || null, written: j.written, held: j.held, replaced: (j.replaced || []).map(x => ({ sku: x.sku, from: x.from, fromMm: x.fromMm, toMm: x.toMm, differs: x.differs, by: x.by })), heldList: (j.heldList || []).map(x => x.sku), conflicts: (j.conflicts || []).map(x => x.sku), pairKept: (j.pairKept || []).concat(j.kept || []).map(x => x.sku), log: j.log }; });
  const card = text => page.evaluate(t => { const d = [...document.querySelectorAll('#mJobs details.noteBox')].find(x => x.textContent.includes(t)); return d ? d.textContent : ''; }, text);
  const wipe = async () => { for (const k of [...st.docs.keys()]) if (/^Charm_Master_(Index|Files)\//.test(k)) st.docs.delete(k); await page.evaluate(() => { B.master.jobs.clear(); return Master.load(true); }); };
  try {
    // 1 · the older file: A, B, and P drawn as a pair (two touching bodies under one label)
    const older = await scene({ page: [420, 300], bodies: [{ shape: 'house', x: 30, y: 150, w: 40, h: 40, ink: IA }, { shape: 'blob', x: 120, y: 150, w: 40, h: 40, ink: IC }, { shape: 'house', x: 220, y: 150, w: 45, h: 45, ink: IA }, { shape: 'notch', x: 267, y: 150, w: 45, h: 45, ink: IB }],
      labels: [{ text: 'ALPHA_A_1', cx: 50, top: 149 }, { text: 'BRAVO_B_2', cx: 140, top: 149 }, { text: 'PAIR_P_3', cx: 266, top: 149 }] });
    let r = await br.upload(older, 'OLD MASTER.ai'); let j = await job();
    ok(r.state === 'done' && libSkus(st).join() === 'ALPHA_A_1,BRAVO_B_2,PAIR_P_3' && st.doc('Charm_Master_Index', 'PAIR_P_3').pair && st.doc('Charm_Master_Index', 'PAIR_P_3').pair.bodies === 2, 'the older file holds A, B and the pair P: ' + libSkus(st));
    const oldHash = st.doc('Charm_Master_Index', 'ALPHA_A_1').masterHash, aBefore = JSON.parse(JSON.stringify(st.doc('Charm_Master_Index', 'ALPHA_A_1'))), aBytes = Buffer.from(st.blobs.get(aBefore.aiPath).buf);
    const pBefore = JSON.parse(JSON.stringify(st.doc('Charm_Master_Index', 'PAIR_P_3'))), bOld = JSON.parse(JSON.stringify(st.doc('Charm_Master_Index', 'BRAVO_B_2')));
    // what a person set on B, which the drawing does not carry (masterPatch's fields, and one the index never writes)
    Object.assign(st.doc('Charm_Master_Index', 'BRAVO_B_2'), { upAngle: 33, upSource: 'operator', engravable: false, engravableBy: 'operator', facing: 'L', facingBy: 'operator', confirmedBy: 'Paul', etsyNote: 'listing 123 links here' });
    st.put('Charm_Sku_Aliases', 'BRAVO OLD NAME', { sku: 'BRAVO_B_2' }, false);
    await page.evaluate(() => Master.load(true));

    // the newer file: B drawn larger, C new, P drawn as ONE body
    const newer = await scene({ page: [420, 300], bodies: [{ shape: 'blob', x: 30, y: 150, w: 70, h: 66, ink: IC }, { shape: 'notch', x: 140, y: 150, w: 40, h: 40, ink: IB }, { shape: 'house', x: 240, y: 150, w: 45, h: 45, ink: IA }],
      labels: [{ text: 'BRAVO_B_2', cx: 65, top: 149 }, { text: 'CHARLIE_C_4', cx: 160, top: 149 }, { text: 'PAIR_P_3', cx: 262.5, top: 149 }] });
    const putsBefore = () => st.calls.filter(c => c.body && c.body.op === 'masterPutIndex').length;
    r = await br.upload(newer, 'SKUs_CORRECTED.ai'); j = await job();
    const b = st.doc('Charm_Master_Index', 'BRAVO_B_2'), c = st.doc('Charm_Master_Index', 'CHARLIE_C_4'), a = st.doc('Charm_Master_Index', 'ALPHA_A_1'), p = st.doc('Charm_Master_Index', 'PAIR_P_3');
    ok(j.state === 'done', 'the newer file is indexed: ' + j.error + ' ' + j.log.join(' | '));
    ok(b.masterName === 'SKUs_CORRECTED.ai' && b.masterHash !== oldHash && b.widthPt > bOld.widthPt + 20 && b.aiPath && st.blobs.has(b.aiPath), 'B is re-indexed from the newer file: its drawing, size and file reference: ' + JSON.stringify({ m: b.masterName, w: b.widthPt, was: bOld.widthPt, ai: b.aiPath }));
    ok(b.upAngle === 33 && b.upSource === 'operator' && b.engravable === false && b.engravableBy === 'operator' && b.facing === 'L' && b.facingBy === 'operator' && b.confirmedBy === 'Paul' && b.etsyNote === 'listing 123 links here', 'what a person set on B is kept: ' + JSON.stringify({ up: b.upAngle, src: b.upSource, eng: b.engravable, f: b.facing, c: b.confirmedBy, n: b.etsyNote }));
    ok(st.doc('Charm_Sku_Aliases', 'BRAVO OLD NAME').sku === 'BRAVO_B_2', 'and the alias that points to B still does');
    ok(b.replaced && b.replaced.from === oldHash && b.replaced.fromName === 'OLD MASTER.ai' && b.replaced.by === 'Tester' && Math.abs(b.replaced.fromSize[0] - bOld.widthPt) < 0.01 && Math.abs(b.replaced.toSize[0] - b.widthPt) < 0.01, 'the entry records what it replaced, from which file, the old size beside the new, and who: ' + JSON.stringify(b.replaced));
    ok(!b.blocked && !b.conflict && !c.blocked && st.list('Charm_Master_Index').every(e => !e.blocked && !e.conflict) && j.conflicts.length === 0, 'nothing is blocked as "in another master too"');
    ok(c && c.masterName === 'SKUs_CORRECTED.ai', 'C is added');
    ok(JSON.stringify(a) === JSON.stringify(aBefore) && Buffer.compare(Buffer.from(st.blobs.get(a.aiPath).buf), aBytes) === 0, 'A, which the newer file does not carry, is untouched (its entry and its design file)');
    ok(p.masterHash === oldHash && p.pair && p.pair.bodies === 2 && p.aiPath === pBefore.aiPath && p.widthPt === pBefore.widthPt && j.heldList.includes('PAIR_P_3'), 'P, held as a pair, is not overwritten by a drawing of one body: ' + JSON.stringify({ m: p.masterName, pair: p.pair, held: j.heldList }));
    { const hc = await card('left as they are'); ok(/PAIR_P_3 — from OLD MASTER\.ai.*held as a pair, and this sheet draws one body: kept/.test(hc) && /never overwritten by a drawing of one body/.test(hc) && j.log.some(l => /holds as a pair are drawn with one body on this sheet.*PAIR_P_3/.test(l)), 'and the card and the run name it: ' + hc); }
    ok(libSkus(st).join() === 'ALPHA_A_1,BRAVO_B_2,CHARLIE_C_4,PAIR_P_3', 'one entry per SKU, nothing duplicated: ' + libSkus(st));
    ok(j.replaced.length === 1 && j.replaced[0].sku === 'BRAVO_B_2' && j.replaced[0].from === 'OLD MASTER.ai' && j.replaced[0].differs, 'the run lists B as replaced, at a different size: ' + JSON.stringify(j.replaced));
    const rc = await card('replaced with this file');
    ok(/1 SKU line\(s\) replaced with this file's drawings \(1 at a different size\)/.test(rc) && /BRAVO_B_2 — OLD MASTER\.ai.* → SKUs_CORRECTED\.ai — 1\d\.\d × 1\d\.\d mm → 2\d\.\d × 2\d\.\d mm/.test(rc) && /Added by Tester/.test(rc), 'the card says it, old file → new file, old size → new size: ' + rc);
    ok(!/left as they are/.test(r.card) || !/BRAVO_B_2/.test(await card('left as they are')), 'and B is not listed as left as it is');
    { const fo = st.list('Charm_Master_Files').find(f => f.name === 'OLD MASTER.ai'), fn = st.list('Charm_Master_Files').find(f => f.name === 'SKUs_CORRECTED.ai');
      ok(fo.skus.slice().sort().join() === 'ALPHA_A_1,PAIR_P_3' && fo.movedOut && fo.movedOut.skus.join() === 'BRAVO_B_2' && fo.movedOut.by === 'Tester', 'the older file\'s upload-history record no longer lists B, and says where it went: ' + JSON.stringify({ skus: fo.skus, moved: fo.movedOut }));
      ok(fn.skus.slice().sort().join() === 'BRAVO_B_2,CHARLIE_C_4' && fn.by === 'Tester', 'the newer file\'s record lists what it holds and who added it: ' + JSON.stringify({ skus: fn.skus, by: fn.by })); }

    // 2 · the newer file again changes nothing
    const snap = JSON.stringify(st.list('Charm_Master_Index')), blobs = st.blobs.size, puts = putsBefore();
    r = await br.upload(newer, 'SKUs_CORRECTED.ai'); j = await job();
    ok(j.state === 'done' && j.written === 0 && j.replaced.length === 0 && st.blobs.size === blobs && putsBefore() === puts, 'adding the newer file again writes and uploads nothing: ' + JSON.stringify({ w: j.written, rep: j.replaced, puts: putsBefore() - puts }));
    ok(JSON.stringify(st.list('Charm_Master_Index')) === snap, 'and every entry is as it was');
    ok(j.heldList.slice().sort().join() === 'BRAVO_B_2,CHARLIE_C_4,PAIR_P_3' && /3 SKU line\(s\) already in the library, left as they are/.test(await card('left as they are')), 'its SKUs (and the pair it leaves) are listed as left as they are: ' + JSON.stringify(j.heldList));

    // 3 · a design saved from an order is never replaced by a master file
    await wipe();
    st.put('Charm_Master_Index', 'SAVED_S_5', { sku: 'SAVED_S_5', masterHash: 'c0ffee00c0ffee00', masterName: 'custom design', hashSource: 'custom-save', widthPt: 30, heightPt: 30, aiPath: 'charmnest/master/custom/SAVED_S_5.ai', thumbPath: '', readerV: 99 }, false);
    st.blobs.set('charmnest/master/custom/SAVED_S_5.ai', { buf: Buffer.from(older), generation: 1, meta: { contentType: 'application/illustrator', metadata: {} } });
    await page.evaluate(() => Master.load(true));
    const savedSheet = await scene({ page: [420, 300], bodies: [{ shape: 'house', x: 60, y: 150, w: 50, h: 50, ink: IA }], labels: [{ text: 'SAVED_S_5', cx: 85, top: 149 }] });
    r = await br.upload(savedSheet, 'MASTER WITH SAVED.ai'); j = await job();
    const sv = st.doc('Charm_Master_Index', 'SAVED_S_5');
    ok(j.state === 'done' && sv.hashSource === 'custom-save' && sv.masterHash === 'c0ffee00c0ffee00' && sv.widthPt === 30 && !sv.replaced && j.replaced.length === 0, 'a design saved from an order stays as it is: ' + JSON.stringify({ src: sv.hashSource, m: sv.masterName, rep: j.replaced, w: j.written }));

    // 4 · "keep the library's drawings": the old rule
    await wipe();
    await br.upload(older, 'OLD MASTER.ai');
    const bKeep = JSON.stringify(st.doc('Charm_Master_Index', 'BRAVO_B_2'));
    r = await br.upload(newer, 'SKUs_CORRECTED.ai', { held: 'keep' }); j = await job();
    ok(j.state === 'done' && JSON.stringify(st.doc('Charm_Master_Index', 'BRAVO_B_2')) === bKeep && st.doc('Charm_Master_Index', 'CHARLIE_C_4') && j.replaced.length === 0 && j.heldList.includes('BRAVO_B_2'), 'with "keep the library\'s drawings" B is left as it is and C is added: ' + JSON.stringify({ rep: j.replaced, held: j.heldList }));
    ok(/To replace them with this file's drawings, choose “replace with this file's drawings” under SKUs already held/.test(await card('left as they are')), 'and the card says how to replace it');
    await page.evaluate(() => { const sel = document.querySelector('#mHeld'); sel.value = 'newest'; sel.onchange(); });
    r = await br.upload(newer, 'SKUs_CORRECTED.ai'); j = await job();
    ok(j.state === 'done' && st.doc('Charm_Master_Index', 'BRAVO_B_2').masterName === 'SKUs_CORRECTED.ai' && j.replaced.map(x => x.sku).join() === 'BRAVO_B_2', 'the same file added again with the default replaces it: ' + JSON.stringify(j.replaced));

    // 5 · the server rule alone
    { const Master = require('../../netlify/functions/_charmNestMaster.js');
      const docs = new Map(), FV = { serverTimestamp: () => 1, delete: () => ({ __del: true }) };
      const ref = (coll, id) => ({ id, get: async () => ({ exists: docs.has(coll + '/' + id), data: () => docs.get(coll + '/' + id) }), set: async (d, o) => { const cur = (o && o.merge && docs.get(coll + '/' + id)) || {}; const nx = Object.assign({}, cur, d); for (const k of Object.keys(nx)) if (nx[k] && nx[k].__del) delete nx[k]; docs.set(coll + '/' + id, nx); } });
      const db = { collection: coll => ({ doc: id => ref(coll, id) }), getAll: async (...refs) => { if (refs.length && typeof refs[refs.length - 1].get !== 'function') refs.pop(); return Promise.all(refs.map(x => x.get())); }, batch() { const ops = []; return { set: (r0, d, o) => ops.push(() => r0.set(d, o)), commit: async () => { for (const o of ops) await o(); } }; } };
      const ent = (sku, w) => ({ sku, widthPt: w, heightPt: w, areaPt2: w * w, members: 1, holes: 0, aiPath: 'charmnest/master/' + sku + '.ai', charmHash: 'h' + w });
      await Master.putIndex(db, FV, { entries: [ent('X_1', 40)], masterHash: 'aaaa1111', masterName: 'one.ai' });
      let out = await Master.putIndex(db, FV, { entries: [ent('X_1', 60)], masterHash: 'bbbb2222', masterName: 'two.ai' });
      ok(out.blocked.length === 1 && /two master files/.test(docs.get('Charm_Master_Index/X_1').blocked), 'without `newest` a SKU in two masters is blocked, as before');
      out = await Master.putIndex(db, FV, { entries: [ent('X_1', 60)], masterHash: 'bbbb2222', masterName: 'two.ai', newest: true, by: 'Paul' });
      const x = docs.get('Charm_Master_Index/X_1');
      ok(out.blocked.length === 0 && !x.blocked && !x.conflict && x.masterName === 'two.ai' && out.replaced.length === 0, 'with `newest` the second write clears the block (the entry is two.ai\'s already, so nothing more to replace): ' + JSON.stringify({ b: x.blocked, r: out.replaced }));
      docs.set('Charm_Master_Files/aaaa1111', { masterHash: 'aaaa1111', name: 'one.ai', skus: ['X_1', 'Y_1'] });
      await Master.putIndex(db, FV, { entries: [ent('X_1', 40)], masterHash: 'aaaa1111', masterName: 'one.ai', newest: true });
      out = await Master.putIndex(db, FV, { entries: [ent('X_1', 80)], masterHash: 'cccc3333', masterName: 'three.ai', newest: true, by: 'Paul' });
      ok(out.blocked.length === 0 && out.replaced.length === 1 && out.replaced[0].fromName === 'one.ai' && out.replaced[0].by === 'Paul' && docs.get('Charm_Master_Index/X_1').replaced.toSize[0] === 80, 'with `newest` an entry of another master is replaced and the answer says so: ' + JSON.stringify(out.replaced));
      ok(docs.get('Charm_Master_Files/aaaa1111').skus.join() === 'Y_1' && docs.get('Charm_Master_Files/aaaa1111').movedOut.to === 'cccc3333', 'and the older file\'s record lets go of it');
      out = await Master.putIndex(db, FV, { entries: [ent('X_1', 80)], masterHash: 'cccc3333', masterName: 'three.ai', newest: true, by: 'Paul' });
      ok(out.replaced.length === 0 && docs.get('Charm_Master_Index/X_1').replaced.fromName === 'one.ai', 'the same write again replaces nothing and keeps the record of the last replacement'); }

    const bad = br.errors.filter(e => !/status of 500/.test(e));
    ok(bad.length === 0, 'no page error: ' + bad.join(' | '));
  } finally { await br.close(); }
  console.log(`master-newest: ${n} checks passed`);
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
