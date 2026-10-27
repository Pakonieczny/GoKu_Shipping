// DUPID (Paul, 27 Oct 2026, order 4208640203, SANDBOX): "this order is correctly added to the sheet for both pieces. However, the system does not recognize their presence on the sheet ... these
// pieces on the sheet are not recognized as being part of the same order." The REAL page (charm-nest-1.html in headless Chromium over the repo's fake site, the real Mini Tag drawing as the master
// design 5X8.5: _engraveRealWorld.cjs), the REAL intake and pool (Orders.interpretAll, Pool.addAll). Nothing leaves the machine.
//   A  the root cause: the first line of a design took the master's OWN charm ("<design>:0"); the design read again (a reload: the page keeps its sheets, not the designs it read) gave the next
//      line of it a fresh master charm, "<design>:0" again. Two pieces of one order (two Etsy lines) went onto one sheet under ONE id. Now every piece is a copy named by its pool id.
//   B  what was made before: a sheet holding two pieces of two orders under one id, placed once (the sandbox's SS Sheet 1): the page's index now tells them apart (the piece the sheet was written for
//      keeps the spot, the other waits to be placed, as the order check says), and a restore marks the sheet to take the waiting piece at its next nest, append only.
//   node tests/charm-nest/order-on-sheet-browser.cjs      (PW_DIR=<dir holding playwright-core>, CHROMIUM=<chrome>)
'use strict';
const assert = require('assert/strict');
const R = require('./_engraveRealWorld.cjs');
let n = 0; const ok = m => { n++; console.log('  ✓ ' + m); };
const TAG_TITLE = 'Mini Tag Initial Charm Add On Charm Personalized Letter Charm Minimalist Tiny Pendant Gift for Her Custom Initial Charm';
const RID = '4208640203', OTHER = '4211837232';

(async () => {
  const w = await R.world({ viewport: { width: 1440, height: 900 } });
  if (!w) { console.log('order-on-sheet-browser: no playwright-core here, skipped'); return; }
  const page = w.page;
  try {
    const out = await page.evaluate(async ({ RID, OTHER, TAG_TITLE }) => {
      const now = Math.floor(Date.now() / 1000), V = (name, value) => ({ name, value });
      const line = (tx, pers) => ({ transactionId: tx, listingId: '4458930445', sku: 'Mini_0445', title: TAG_TITLE, quantity: 1, expectedShipDate: now + 5 * 86400, metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [pers], buyerMessage: '',
        variations: [V('Metal Choice', 'Gold Filled'), V('Charm Type', 'Tag1 (front+back)'), V('Personalization', pers), V('Font Choice', 'Ace')] });
      const order = (rid, buyer, lines) => ({ receiptId: rid, orderNumber: rid, createTs: now - 3600, updateTs: now - 60, shipBy: now + 5 * 86400, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
      const add = (o, ln) => { const key = CharmNestOrders.lineKey(o, ln); const row = { key, order: o, line: ln, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); return row; };
      const L1 = line('5262269683', 'Front - CMG    Back - 3.22.19'), L2 = line('5262269685', 'Front - LRG    Back- 9.2.16'), M = order(RID, 'Name Megan', [L1, L2]);
      const LO = line('5264597300', 'Front - AB    Back - 1.2.20'), X = order(OTHER, 'Name Other', [LO]);
      await Orders.loadMaps(true);
      const r1 = add(M, L1); Orders.interpretAll(); await Pool.addAll({ runId: 'run-dupid', lines: {} });
      // a reload: the page keeps its sheets and pool rows (the workspace), never the designs it read (B.pool.sources): the next line of the design reads it again
      const paths = []; for (const [k, s] of [...B.pool.sources]) if (s && s.sku === '5X8.5') { paths.push(k); B.pool.sources.delete(k); }
      const r2 = add(M, L2), r3 = add(X, LO); Orders.interpretAll(); await Pool.addAll({ runId: 'run-dupid', lines: {} });
      const masters = new Set(Object.values(S.poolSources || {}).map(s => s && s.charms && s.charms[0]).filter(Boolean));
      const piece = row => (row.poolIds || []).map(id => { const c = Pool.charmOf(id), sh = c && allSheets().find(p => p.charms.includes(c)); return { poolId: id, id: c && c.id, sourceId: c && c.sourceId, master: !!(c && masters.has(c)), sheet: sh ? sh.metal + ':' + sh.page : null }; });
      const res = { reread: paths.length, states: [r1, r2, r3].map(r => [r.key, r.state, r.spec && r.spec.designSku, r.spec && r.spec.engraving && [r.spec.engraving.front && r.spec.engraving.front.text, r.spec.engraving.back && r.spec.engraving.back.text]]), pieces: [r1, r2, r3].map(piece) };
      // B · a sheet as the sandbox's SS Sheet 1 is: the CMG piece and the other order's piece under ONE id ("<design>:0"), the CMG one first, one placement of that id (written for the other order)
      const c1 = Pool.charmOf(r1.poolIds[0]), c2 = Pool.charmOf(r2.poolIds[0]), cx = Pool.charmOf(r3.poolIds[0]), sh = allSheets().find(p => p.charms.includes(c1));
      for (const c of [c1, c2, cx]) for (const p of allSheets()) if (p !== sh && p.charms.includes(c)) { p.charms = p.charms.filter(z => z !== c); p.placements = p.placements.filter(z => z.id !== c.id); }
      const shared = c1.sourceId + ':0', order3 = [c1, c2, cx];
      sh.charms = sh.charms.filter(c => !order3.includes(c)).concat(order3); c1.id = shared; cx.id = shared;
      sh.placements = sh.charms.filter(c => c !== c1).map((c, i) => ({ id: c.id, cxPt: 40 + 40 * i, cyPt: 30, angle: 0, wPt: 20, hPt: 20, scale: 0.971 }));
      Object.assign(sh, { status: 'complete', dirty: false, appendOnly: false, intakeAppend: false, sheetId: sh.sheetId || 'dupid-ss-1', fileBase: sh.fileBase || 'SS_Oct.28.26_Set-2_Sheet-1', persistedDone: true, persisted: Promise.resolve(), verification: { ok: true } });
      sh.charms = sh.charms.slice();   // (a new list: the page's index is made again, as after a restore)
      const where = id => { const s = Pool.sheetOf(id); return s === sh ? 'sheet' : s ? 'other' : null; };
      res.healed = { m1: where(r1.poolIds[0]), m2: where(r2.poolIds[0]), x: where(r3.poolIds[0]), ids: sh.charms.filter(c => order3.includes(c)).map(c => [c.poolId, c.id]), unique: new Set(sh.charms.map(c => c.id)).size === sh.charms.length,
        placements: sh.placements.length, sharedOwner: (sh.charms.find(c => c.id === shared) || {}).poolId || null };
      // the restore's step: the sheet that was told apart keeps every placed charm and takes its waiting piece at its next nest (append only)
      res.settled = typeof Pool.uniqueIds === 'function' ? (Pool.uniqueIds(null, { settle: true }), { appendOnly: !!sh.appendOnly, dirty: !!sh.dirty, status: sh.status, placed: sh.placements.length }) : null;
      return res;
    }, { RID, OTHER, TAG_TITLE });

    // A · the root cause
    assert.ok(out.reread >= 1, 'the design was read again between the two updates: ' + JSON.stringify(out));
    assert.deepEqual(out.states.map(s => s[1]), ['pooled', 'pooled', 'pooled'], 'all three lines are pooled: ' + JSON.stringify(out.states));
    assert.deepEqual(out.states[0][3], ['CMG', '3.22.19'], 'the CMG line reads its back: ' + JSON.stringify(out.states[0]));
    assert.deepEqual(out.states[1][3], ['LRG', '9.2.16'], 'the LRG line reads its back: ' + JSON.stringify(out.states[1]));
    const all = out.pieces.flat();
    assert.equal(all.length, 3, 'one piece per line: ' + JSON.stringify(out.pieces));
    for (const p of all) {
      assert.ok(p.id, 'every piece has a charm: ' + JSON.stringify(p));
      assert.equal(p.master, false, 'no piece IS the master\'s own charm: ' + JSON.stringify(p));
      assert.equal(p.id, `${p.sourceId}:${p.poolId}`, 'every piece is named by its pool id: ' + JSON.stringify(p));
    }
    assert.equal(new Set(all.map(p => p.id)).size, 3, 'three pieces, three ids (the two pieces of order ' + RID + ' had ONE): ' + JSON.stringify(all));
    ok('A · the first line of a design and the next one after the design was read again each get an id of their own: the two pieces of one order are two pieces on the sheet');

    // B · what was made before
    assert.deepEqual([out.healed.m1, out.healed.m2, out.healed.x], [null, 'sheet', 'sheet'], 'the page reads the CMG piece as waiting, the LRG piece and the other order\'s piece on the sheet: ' + JSON.stringify(out.healed));
    assert.ok(out.healed.unique, 'every charm of the sheet has an id of its own: ' + JSON.stringify(out.healed.ids));
    assert.equal(out.healed.sharedOwner, all[2].poolId, 'the shared id stays with the piece the sheet was written for (the other order\'s)');
    assert.ok(out.settled && out.settled.appendOnly && out.settled.dirty && out.settled.status === 'ready', 'the sheet takes its waiting piece at its next nest, append only: ' + JSON.stringify(out.settled));
    assert.equal(out.settled.placed, out.healed.placements, 'no placed charm is moved or dropped');
    ok('B · a sheet with two pieces of two orders under one id: the page agrees with the order check (the piece never placed waits), the shared spot stays with the piece written there, and the sheet is set to place the waiting one');
    assert.ok(!w.errors.length, 'no page error: ' + w.errors.join(' | '));
  } finally { await w.close(); }
  console.log(`order-on-sheet-browser: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
