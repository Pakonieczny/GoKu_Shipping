// AUDIT (cardinal rule / shared orders / partial sheets): the boundary cases of R3, R9, R10, R11 (SETFIX "a partial sheet is an END", PARTIAL_BRIDGE 'hold'), the R9/R10 anchors and R16, each through
// the page's REAL set assembly (audit-cardinal-lib.cjs) and judged by an independent oracle, the live self-check AND the strict checker (R11 without 'pending'). Every case here is expected to PASS:
// it is the proof that the rule holds in that circumstance. (The circumstances in which a rule IS broken are tests/charm-nest/audit-cardinal-breaks.cjs.)
'use strict';
const assert = require('node:assert/strict');
const L = require('./audit-cardinal-lib.cjs');
const ok = (w, what, o = {}) => {
  const f = L.invariantFaults(w); assert.deepEqual(f, [], `${what}: ${f.join('; ')}\n  ${w.word()}`);
  const sc = w.selfCheck(); assert.deepEqual(sc.violations.map(v => v.message), [], `${what}: the live self-check\n  ${w.word()}`);
  if (!o.pending) { const st = w.strict(); assert.deepEqual(st.violations.map(v => v.message), [], `${what}: the strict checker\n  ${w.word()}`); }
  return sc;
};
const ids = (w, seq) => { const s = w.sets().find(x => x.seq === seq); return s ? w.members(s).map(p => p.sheetId).sort() : null; };
const done = [];
(async () => {
  /* 1 · R3 + R11: a partial GF sheet that shares an order with a complete GF+SS unit holds it whole; full, it joins the set with it */
  { const w = L.world();
    const g1 = w.full(w.put(w.page('gold'), '1100_1_1', '1101_1_1')), s1 = w.full(w.put(w.page('silver'), '1101_2_1')), p = w.put(w.page('gold'), '1100_1_2');
    await w.assemble(); ok(w, '1a');
    assert.equal(w.sets().filter(s => w.members(s).length).length, 0, '1a: the unit waits whole for the partial GF Draft 2'); assert(/not full yet/.test(g1.cardinalHold) && /not full yet/.test(s1.cardinalHold), '1a: both say why');
    w.full(p); await w.assemble(); ok(w, '1b');
    assert.deepEqual(ids(w, 1), ['GF1', 'GF2', 'SS1'], '1b: full, it is one set with its unit'); done.push('partial holds its unit whole, then joins with it');
  }
  /* 2 · PARTIAL_BRIDGE 'hold' (default): a partial sheet with orders on TWO complete units holds both; full, they are ONE unit and one set; it never chains them before */
  { const w = L.world(); assert.equal(L.SR.PARTIAL_BRIDGE, 'hold', 'the default is hold');
    w.full(w.put(w.page('gold'), '1200_1_1', '1201_1_1')); w.full(w.put(w.page('silver'), '1201_2_1'));
    w.full(w.put(w.page('gold'), '1210_1_1', '1211_1_1')); w.full(w.put(w.page('silver'), '1211_2_1'));
    const p = w.put(w.page('silver'), '1200_2_1', '1210_2_1');
    await w.assemble(); ok(w, '2a'); assert.equal(w.pages.filter(w.inSet).length, 0, '2a: both units wait');
    w.full(p); await w.assemble(); ok(w, '2b'); assert.deepEqual(ids(w, 1), ['GF1', 'GF2', 'SS1', 'SS2', 'SS3'], '2b: one unit, one set'); done.push("PARTIAL_BRIDGE 'hold'");
  }
  /* 3 · a partial sheet that appears AFTER its group formed a set: the set is right, the partial is its pending partner; the group's next completed sheet joins that set (closed or not); full, the partial joins */
  { const w = L.world();
    const g1 = w.full(w.put(w.page('gold'), '1300_1_1', '1301_1_1')), s1 = w.full(w.put(w.page('silver'), '1301_2_1'));
    await w.assemble(); ok(w, '3a'); assert.deepEqual(ids(w, 1), ['GF1', 'SS1']);
    const p = w.put(w.page('silver'), '1300_9_1', '1302_1_1'); const g2 = w.full(w.put(w.page('gold'), '1302_2_1'));    // a late line of 1300 on a filling SS sheet, which also shares 1302 with a full GF sheet
    await w.assemble(); const sc = ok(w, '3b', { pending: true });
    assert(!w.inSet(g2), '3b: GF2 waits (its only mate is the partial SS Draft 2)'); assert(sc.partners.some(x => x.kind === 'partial' && x.sheet === 'SS2'), '3b: SS Draft 2 is the pending partner of Set 1');
    w.full(p); await w.assemble(); ok(w, '3c'); assert.deepEqual(ids(w, 1), ['GF1', 'GF2', 'SS1', 'SS2'], '3c: full, it and GF2 join Set 1 (the cardinal rule outranks the closing)'); done.push('partial after formation');
  }
  /* 4 · one order on THREE sheets across metals, the Rose Gold one uncut: the set forms without it, it is the named partner, Approve grey; cut, it joins THAT set */
  { const w = L.world();
    w.full(w.put(w.page('gold'), '1400_1_1')); w.full(w.put(w.page('silver'), '1400_2_1')); const r = w.put(w.page('rose'), '1400_3_1');
    await w.assemble(); let sc = ok(w, '4a'); assert.deepEqual(ids(w, 1), ['GF1', 'SS1']); assert(sc.partners.some(x => x.sheet === 'RG1' && x.kind === 'notReady')); assert.equal(sc.sets.find(s => s.name).canApprove, false, '4a: Approve grey');
    r.roseCutAt = Date.now(); await w.assemble(); sc = ok(w, '4b'); assert.deepEqual(ids(w, 1), ['GF1', 'RG1', 'SS1']); assert.equal(sc.sets[0].canApprove, true); done.push('3 metals, RG partner');
  }
  /* 5 · the same with an uncut 14K sheet; a sheet in no set that shares only a SINGLE-piece order... is not shared: (a) 14K partner; (b) one piece listed on two sheets ties nothing */
  { const w = L.world();
    w.full(w.put(w.page('gold'), '1500_1_1')); w.full(w.put(w.page('silver'), '1500_2_1')); w.put(w.page('gold14k'), '1500_3_1');
    await w.assemble(); const sc = ok(w, '5a'); assert.deepEqual(ids(w, 1), ['GF1', 'SS1']); assert(sc.partners.some(x => x.sheet === '14K1')); assert.equal(sc.sets[0].canApprove, false);
    const w2 = L.world();
    w2.full(w2.put(w2.page('gold'), '1510_1_1')); w2.full(w2.put(w2.page('silver'), '1511_1_1')); w2.full(w2.put(w2.page('gold'), '1510_1_1'));    // the same piece listed on GF1 and GF2 (one piece): not shared
    await w2.assemble(); ok(w2, '5b'); assert.deepEqual(ids(w2, 1), ['GF1', 'SS1'], '5b: GF2 is not tied to GF1 by a one-piece order, it waits for its own set'); done.push('14K partner; one piece on two sheets');
  }
  /* 6 · a mismatched pair whose Left and Right carry ONE pool id on two GF sheets IS shared (two bodies) */
  { const w = L.world();
    const g1 = w.full(w.put(w.page('gold'), '1600_1_1', '1601_1_1')), s1 = w.full(w.put(w.page('silver'), '1601_2_1')), g2 = w.full(w.put(w.page('gold'), '1600_1_1'));
    g1.charms.find(c => c.poolId === '1600_1_1').side = 'L'; g2.charms.find(c => c.poolId === '1600_1_1').side = 'R';
    await w.assemble(); ok(w, '6'); assert.deepEqual(ids(w, 1), ['GF1', 'GF2', 'SS1'], '6: the Left and Right of one pair tie GF1 and GF2'); done.push('mismatched pair, one id L/R');
  }
  /* 7 · the R9/R10 anchors: two units that each share one order with a sheet of the committed Set 1, and two that each share one with an uncut Rose Gold sheet, form FOUR sets, not one */
  { const w = L.world();
    const a = w.full(w.put(w.page('gold'), '1700_1_1', '1701_1_1')), b = w.full(w.put(w.page('silver'), '1701_2_1', '1702_1_1'));
    await w.assemble(); w.commit(w.sets()[0]);
    const r = w.put(w.page('rose'), '1730_1_1', '1740_1_1');
    for (const [o1, o2, o3] of [['1700', '1710', '1711'], ['1702', '1720', '1721'], ['1730', '1731', '1732'], ['1740', '1741', '1742']]) {
      w.full(w.put(w.page('gold'), `${o1}_8_1`, `${o2}_1_1`)); w.full(w.put(w.page('silver'), `${o2}_2_1`));
    }
    await w.assemble(); const sc = ok(w, '7'); assert.deepEqual([...w.sets().map(s => w.members(s).length)], [2, 2, 2, 2, 2], '7: one set per unit: ' + w.word());
    assert.equal(sc.partners.filter(x => x.sheet === 'RG1').length, 2, '7: the uncut RG is the partner to add of both of its units'); done.push('anchors: committed Set 1, uncut RG');
  }
  /* 8 · R3 by a person's drop: a partial sheet dropped on a formed set does not go in (handSet) */
  { const w = L.world();
    w.full(w.put(w.page('gold'), '1800_1_1')); w.full(w.put(w.page('silver'), '1800_2_1')); await w.assemble();
    const p = w.put(w.page('gold'), '1801_1_1'); p.handSet = w.sets()[0].setId; await w.assemble(); ok(w, '8'); assert(!w.inSet(p), '8: a partial sheet is never dropped into a set'); done.push('partial drop refused');
  }
  /* 9 · R16: an order split over two same-colour sheets is a warning of the checker (never a violation), and it does tie them */
  { const w = L.world();
    w.full(w.put(w.page('gold'), '1900_1_1')); w.full(w.put(w.page('gold'), '1900_1_2')); w.full(w.put(w.page('silver'), '1901_1_1'));
    await w.assemble(); const sc = ok(w, '9'); assert(sc.warnings.some(x => x.rule === 'orderSplit')); assert.deepEqual(ids(w, 1), ['GF1', 'GF2', 'SS1']); done.push('R16 warning');
  }
  console.log(`PASS: audit-cardinal-scenarios: ${done.join(' · ')}`);
})().catch(e => { console.error(e); process.exit(1); });
