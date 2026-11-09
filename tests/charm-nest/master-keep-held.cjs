// KEEPHELD / KEEPSIZE (Paul, 9 Nov 2026, on "SKUs_BEADY_2026-1023 (1).ai", added again: "I uploaded this file, but I got all these errors because the sizes don't match. I like to keep my original
// sizes of the charms if they do exist. If the charm already exists, then it doesn't need to be updated."). The card said "1900 SKU line(s) already in the library from this file, left as they are ·
// 611 drawn a different size on this sheet" over 1,900 lines such as "VIKING_NECKLACE_CHARM-0 — this sheet draws it 10.9 × 11 mm, the library holds 11.8 × 13.8 mm".
//   THE RULE: adding a master file never changes a SKU the library holds (its record, drawing and size stay); only the SKUs it does not hold are added. A held SKU is drawn again only when a person
//   chooses it for the file under "SKUs already held" ("replace with this file's drawings", "rebuild every charm"). "keep" is the default.
//   THE SIZE: the library records the traced silhouette's box (every member, the jump ring included, plus half the stroke and 1 pt each side, after the 11.5 mm standard size); the card compared it
//   with the bare outline's box (no ring, no margin, not resized). The comparison now measures this sheet the way the indexer does, so a design held exactly as drawn compares equal.
// Headless Chromium over the in-memory stand-in of the site (bridge-server.cjs): nothing leaves this machine.
//   1 · the page starts at "keep the library's drawings";
//   2 · a file whose SKUs are all held changes no record, no design file, writes nothing, and the card is ONE calm line with the list folded away, no size and no warning colour; a charm with a
//       jump ring, one without, and one over the standard size compare equal to what the library holds;
//   3 · a newer file with held and new SKUs adds only the new ones (a new SKU line under a held charm too); a held SKU this sheet draws at another size keeps the library's size, raises no warning,
//       and is one quiet line inside the fold;
//   4 · a line of this same file written by an older reader stays as it is under "keep";
//   5 · the explicit path still draws a held SKU again: "replace with this file's drawings" re-indexes it from the file.
//   PW_DIR=/opt/node-tools/node_modules node tests/charm-nest/master-keep-held.cjs
'use strict';
const assert = require('assert');
const { scene } = require('./_masterScene.cjs'), R = require('./_masterRoutes.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; console.log('  ok  ' + m); };
const libSkus = st => st.list('Charm_Master_Index').map(e => e._id).sort();
const snap = (st, skus) => JSON.stringify(skus.map(k => st.doc('Charm_Master_Index', k)));
const blobOf = (st, sku) => { const e = st.doc('Charm_Master_Index', sku), b = e && st.blobs.get(e.aiPath); return b ? Buffer.from(b.buf).toString('base64') : null; };

(async () => {
  const br = await R.browser(), st = br.st, page = br.page;
  const job = () => page.evaluate(() => { const j = [...B.master.jobs.values()].pop(); return { state: j.state, error: j.error || null, written: j.written, held: j.held, log: j.log, replaced: (j.replaced || []).map(x => x.sku), heldList: (j.heldList || []).map(x => ({ sku: x.sku, differs: x.differs, sheetMm: x.sheetMm, libraryMm: x.libraryMm })) }; });
  const keptBox = () => page.evaluate(() => { const d = [...document.querySelectorAll('#mJobs .noteBox')].find(x => /already in the library/.test(x.textContent)); if (!d) return null; const f = d.querySelector('details'); return { text: d.textContent, html: d.innerHTML, folded: !!f && !f.open, line: d.firstElementChild ? d.firstElementChild.textContent : '', fold: f ? f.textContent : '' }; });
  const puts = () => st.calls.filter(c => c.body && /^master(PutIndex|PutFile)$/.test(c.body.op || '')).length;
  try {
    // 1 · the default
    ok(await page.evaluate(() => document.querySelector('#mHeld').value) === 'keep', 'the page starts at "keep the library\'s drawings"');

    // the first file: a charm with a jump ring above it, one over the standard size with a ring, one with no ring, and one charm with TWO SKU lines under it
    const first = await scene({ page: [520, 300], bodies: [{ shape: 'house', x: 30, y: 120, w: 28, h: 30 }, { shape: 'blob', x: 120, y: 120, w: 48, h: 42 }, { shape: 'house', x: 240, y: 120, w: 28, h: 30 }, { shape: 'notch', x: 360, y: 120, w: 30, h: 30 }],
      rings: [{ cx: 44, cy: 150 + 2.75, r: 3.25, inner: 1.85 }, { cx: 144, cy: 120 + 42 * .94 + 2.75, r: 3.25, inner: 1.85 }],
      labels: [{ text: 'SMALL_R_1', cx: 44, top: 119 }, { text: 'BIG_R_2', cx: 144, top: 119 }, { text: 'NORING_3', cx: 254, top: 119 }, { text: 'DUO_A_4', cx: 375, top: 119 }] });
    let r = await br.upload(first, 'BEADY.ai', { held: 'keep' }); let j = await job();
    const ALL = ['BIG_R_2', 'DUO_A_4', 'NORING_3', 'SMALL_R_1'];
    ok(j.state === 'done' && libSkus(st).join() === ALL.join(), 'the first file is indexed: ' + libSkus(st) + ' ' + j.error);

    // 2 · the same file again: nothing changes, and the card is calm
    const before = snap(st, ALL), blobs = new Map(ALL.map(k => [k, blobOf(st, k)])), p0 = puts(), b0 = st.blobs.size;
    r = await br.upload(first, 'BEADY.ai', { held: 'keep' }); j = await job();
    ok(j.state === 'done' && j.written === 0 && puts() === p0 && st.blobs.size === b0, 'a file whose SKUs are all held writes nothing: ' + JSON.stringify({ w: j.written, puts: puts() - p0 }));
    ok(snap(st, ALL) === before && ALL.every(k => blobOf(st, k) === blobs.get(k)), 'and every record and design file is as it was');
    ok(j.heldList.length === 4 && j.heldList.every(x => !x.differs), 'a charm with a ring, one without and one over the standard size compare equal to the library: ' + JSON.stringify(j.heldList));
    ok(j.heldList.every(x => x.sheetMm && x.libraryMm && Math.abs(x.sheetMm[0] - x.libraryMm[0]) <= 0.2 && Math.abs(x.sheetMm[1] - x.libraryMm[1]) <= 0.2), 'measured the way the library measures (ring, margin, standard size), within 0.2 mm');
    let kb = await keptBox();
    ok(kb && /^4 already in the library: kept as they are · 0 new added$/.test(kb.line.trim()) && kb.folded && /Show the 4 kept/.test(kb.fold), 'the card is one calm line, the list folded behind "Show the 4 kept": ' + (kb && kb.line));
    ok(!/drawn a different size|this sheet draws it|the library holds|Sizes differ/.test(kb.text) && !/8a3a26|<b>/.test(kb.html), 'with no size and no warning colour: ' + kb.text);
    ok(!j.log.some(l => /different size/.test(l)), 'and the run log raises nothing about sizes');

    // 3 · a newer file: SMALL_R_1 drawn smaller, a new SKU, and a new SKU line under the held DUO_A_4
    const second = await scene({ page: [520, 300], bodies: [{ shape: 'house', x: 30, y: 120, w: 16, h: 16 }, { shape: 'blob', x: 240, y: 120, w: 30, h: 30 }, { shape: 'notch', x: 360, y: 120, w: 30, h: 30 }],
      rings: [{ cx: 38, cy: 136 + 2.75, r: 3.25, inner: 1.85 }],
      labels: [{ text: 'SMALL_R_1', cx: 38, top: 119 }, { text: 'NEW_N_6', cx: 255, top: 119 }, { text: 'DUO_A_4', cx: 375, top: 119 }, { text: 'DUO_C_7', cx: 375, top: 111 }] });
    const s1 = snap(st, ['SMALL_R_1', 'DUO_A_4']), sBlob = blobOf(st, 'SMALL_R_1'), dBlob = blobOf(st, 'DUO_A_4');
    r = await br.upload(second, 'BEADY v2.ai', { held: 'keep' }); j = await job();
    ok(j.state === 'done' && libSkus(st).join() === 'BIG_R_2,DUO_A_4,DUO_C_7,NEW_N_6,NORING_3,SMALL_R_1', 'only the new SKUs are added: ' + libSkus(st) + ' ' + j.error);
    ok(st.doc('Charm_Master_Index', 'NEW_N_6').masterName === 'BEADY v2.ai' && st.doc('Charm_Master_Index', 'DUO_C_7').masterName === 'BEADY v2.ai' && j.written === 2 && j.replaced.length === 0, 'NEW_N_6 and DUO_C_7 come from the newer file, nothing is replaced: ' + JSON.stringify({ w: j.written, rep: j.replaced }));
    ok(snap(st, ['SMALL_R_1', 'DUO_A_4']) === s1 && blobOf(st, 'SMALL_R_1') === sBlob && blobOf(st, 'DUO_A_4') === dBlob, 'SMALL_R_1 (drawn smaller) and DUO_A_4 (a new line beside it) keep their record, size and design file');
    ok(st.list('Charm_Master_Index').every(e => !e.blocked && !e.conflict), 'nothing is blocked as "in another master too"');
    const sm = j.heldList.find(x => x.sku === 'SMALL_R_1');
    ok(sm && sm.differs && j.heldList.some(x => x.sku === 'DUO_A_4'), 'the run knows SMALL_R_1 is drawn at another size here, and lists DUO_A_4 as kept: ' + JSON.stringify(j.heldList));
    kb = await keptBox();
    ok(kb && /^2 already in the library: kept as they are · 2 new added$/.test(kb.line.trim()) && kb.folded, 'the card: "2 already in the library: kept as they are · 2 new added": ' + (kb && kb.line));
    ok(/Sizes differ on this sheet for 1 of them \(kept the library's\)/.test(kb.fold) && !/this sheet draws it|the library holds|drawn a different size/.test(kb.text) && !/8a3a26|<b>/.test(kb.html), 'the size difference is one quiet line inside the fold, no warning: ' + kb.fold);
    ok(/SMALL_R_1 — from BEADY\.ai/.test(kb.fold) && /To draw them again from this file, choose “replace with this file's drawings”/.test(kb.fold), 'the fold names whose each kept SKU is, and how to draw them again');

    // 4 · a line of the first file written by an older reader stays as it is under "keep"
    st.doc('Charm_Master_Index', 'NORING_3').readerV = 1; await page.evaluate(() => Master.load(true));
    const nr = snap(st, ['NORING_3']), p1 = puts();
    r = await br.upload(first, 'BEADY.ai', { held: 'keep' }); j = await job();
    ok(j.state === 'done' && j.written === 0 && puts() === p1 && snap(st, ['NORING_3']) === nr, 'an older reader\'s line of the same file is kept, not built again: ' + JSON.stringify({ w: j.written }));

    // 5 · the explicit path: "replace with this file's drawings"
    const wasW = st.doc('Charm_Master_Index', 'SMALL_R_1').widthPt;
    r = await br.upload(second, 'BEADY v2.ai', { held: 'newest' }); j = await job();
    const s = st.doc('Charm_Master_Index', 'SMALL_R_1');
    ok(j.state === 'done' && s.masterName === 'BEADY v2.ai' && s.widthPt < wasW - 5 && j.replaced.includes('SMALL_R_1'), 'chosen, "replace with this file\'s drawings" re-indexes the held SKU from the file: ' + JSON.stringify({ m: s.masterName, w: s.widthPt, was: wasW, rep: j.replaced }));
    ok(await page.evaluate(() => document.querySelector('#mHeld').value) === 'newest', 'and the choice is the person\'s, on the page');

    const bad = br.errors.filter(e => !/status of 500/.test(e));
    ok(bad.length === 0, 'no page error: ' + bad.join(' | '));
  } finally { await br.close(); }
  console.log(`master-keep-held: ${n} checks passed`);
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
