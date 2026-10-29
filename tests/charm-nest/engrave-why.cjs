// ENGWHY (engrave-ui-1012 round 2, Paul point 7, first half): "If a user tries to override the auto selection engraving options then there should be a quick prompt asking the user for a reason. There should be
// 3-4 quick clickable options for the user to choose from and a small input text box if the 3-4 shortcut buttons aren't sufficient. This needs to be recorded in firebase, be mindful of how much API usage we
// are using for firebase." The REAL page (charm-nest-1.html, headless Chromium, offline fake site over the real charmNestLibrary handlers and an in-memory Firestore that refuses nested arrays; no Etsy, no paid
// call, nothing live). The run is a finished one, so RunCtl.poke does not start more run steps (their saves are the same with or without a reason; they would only blur the count). Order: Mini Tag "Tag1 (front+back)", note "front - W / back - 1. 27. 24", Ace: the system chooses Front + back, W, 1. 27. 24.
//   LIVEWORDS (Paul, 29 Oct 2026): the reason prompt is REMOVED ("Remove that entire process"). This test keeps the pure half (old records with a reason are still read) and checks, on the real page,
//   that a kept change asks nothing, stores no reason, and still leaves the order timeline's "Engraving edited by ..." line (who changed the words).
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/engrave-why.cjs
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const Why = require(path.join(root, 'charm-nest-engrave-why.js'));
const SHIP = Math.floor(Date.UTC(2026, 9, 14, 17) / 1000), RID = '4195966039', TX = '5009123456789', KEY = `${RID}_${TX}`;
const ORDER = { receiptId: RID, orderNumber: RID, createTs: SHIP - 5 * 86400, updateTs: SHIP - 5 * 86400 + 60, shipBy: SHIP, buyer: { name: 'Name One' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: TX, listingId: '4458930445', sku: 'MINI_0445', title: 'Mini Tag Initial Charm Add On Charm Personalized Letter Charm', quantity: 1, expectedShipDate: SHIP, metalKey: 'gold', metalLabel: 'GF 14/20',
    variations: [{ name: 'Charm Type', value: 'Tag1 (front+back)' }, { name: 'Font choice', value: 'Ace' }, { name: 'Metal Choice', value: 'Gold Filled' }], personalization: ['front - W\nback - 1. 27. 24'] }] };

(async () => {
  const fails = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };

  // ── the pure half, in node ──
  check(Why.REASONS.length >= 3 && Why.REASONS.length <= 4 && Why.REASONS.every(r => r.code && r.label.length <= 24), 'the quick buttons are 3-4 short plain labels: ' + Why.REASONS.map(r => r.label).join(' | '));
  { const out = { by: 'Name One', at: 5 }, prev = { reason: { code: 'typo', text: 'Spelling fix', by: 'Old', at: 1 }, was: { mode: 'back' } };
    Why.fold(out, prev, { reason: { code: 'asked', text: 'Customer asked', of: 'back' }, was: { mode: 'both', front: { text: 'W', font: 'a' }, back: { text: '1', font: 'a' } }, slot: 'D2' });
    check(out.reason.code === 'asked' && out.reason.by === 'Name One' && out.reason.at === 5 && out.reason.slot === 'D2' && out.was.mode === 'both' && out.was.back.font === 'a', 'fold: a patch with a reason sets reason (by, at of the save) and was: ' + JSON.stringify(out));
    const keep = Why.fold({ by: 'x', at: 6 }, prev, { mode: 'front' }); check(keep.reason === prev.reason && keep.was === prev.was, 'fold: a patch that says nothing about it (any other caller) keeps what the line had');
    const gone = Why.fold({ by: 'x', at: 6 }, prev, { mode: 'front', reason: null, was: null }); check(!('reason' in gone) && !('was' in gone), 'fold: reason: null drops both (the save is equal to the system\'s choice)');
    const long = Why.fold({ by: 'x', at: 6 }, null, { reason: { code: 'other', text: 'y'.repeat(500) } }); check(long.reason.text.length === Why.MAX_TEXT, 'a custom reason is cut at ' + Why.MAX_TEXT + ' characters');
    const bad = Why.fold({ by: 'x', at: 6 }, null, { reason: { code: 'nonsense', text: 'z' } }); check(!bad.reason, 'an unknown code is not stored'); }
  check(Why.reasonOf({ engravingOverride: { mode: 'back' } }) === null && Why.reasonOf({}) === null, 'no reason in the record: reasonOf is null (old records keep working)');
  { const r = Why.reasonOf({ engravingOverride: { reason: { code: 'asked', text: 'Customer asked', by: 'Name One', at: 1791847578705 }, was: { mode: 'both' } } });
    check(r && r.line === 'Changed: Customer asked · Name One' && /Customer asked/.test(r.title) && r.was.mode === 'both', 'reasonOf gives the quiet line and a title: ' + JSON.stringify({ line: r && r.line, title: r && r.title })); }
  check(Why.reasonOf({ engravingOverride: { reason: { code: 'asked', text: 'x', by: 'a', at: 1, slot: 'D1' } } }, 'D2') === null && !!Why.reasonOf({ engravingOverride: { reason: { code: 'asked', text: 'x', by: 'a', at: 1, slot: 'D1' } } }, 'D1'), 'a reason written for one piece is not another piece\'s');


  // ── the page (LIVEWORDS, Paul 29 Oct 2026: "Remove the process that ask the user why the made text engraving changes including asking the user to click save and filling in the reason as to why. Remove
  //    that entire process."): the prompt is gone. What stays: a kept change asks nothing, stores no reason, and the order timeline's existing "Engraving edited by ..." line still says who changed the
  //    words (the audit, silently). The pure half above stays for the old records that carry a reason (reasonOf / fold keep reading them). ──
  let R; try { R = require('./_engraveRealWorld.cjs'); } catch (_) { R = null; }
  const w = R ? await R.world({ viewport: { width: 1440, height: 900 } }) : null;
  if (!w) console.log('engrave-why: no playwright-core / real world: the page checks were skipped');
  else {
    const page = w.page, Q = '#egQueue .rvItem[data-kind=placement]';
    try {
      const made = await R.build(w, ['both']);
      await R.openEngraveDetail(w, 'both', { face: 'back' }); await R.settle(page, 600);
      await page.evaluate(() => { window.__tl = []; if (window.OrderTimeline && OrderTimeline.record) { const rec = OrderTimeline.record; OrderTimeline.record = function (e) { __tl.push(JSON.parse(JSON.stringify(e))); return rec.apply(this, arguments); }; } });
      await page.fill(Q + ' .engpWords', 'Name Two');
      const ui = await page.evaluate(q => { const c = document.querySelector(q); return { save: !!c.querySelector('[data-engp=save],[data-engp=cancel]'), why: !!c.querySelector('.engpWhy,.engpWhyLine,[data-engp=why]') }; }, Q);
      check(!ui.save && !ui.why, 'typing other words: no Save, no Cancel, no "Why change it?": ' + JSON.stringify(ui));
      const res = await page.evaluate(q => document.querySelector(q)._pieceMount.panel.commit(), Q);   // (what Approve does first)
      await R.settle(page, 2500);
      const after = await page.evaluate(({ q, key }) => { const row = B.orders.byKey.get(key), ov = row.engravingOverride || null; return { why: !!document.querySelector(q + ' .engpWhy,' + q + ' [data-engp=why]'), ov, tl: (window.__tl || []).filter(e => e.type === 'engraveChanged').map(e => e.text) }; }, { q: Q, key: made.both.key });
      check(res && res.ok && !after.why && after.ov && after.ov.back && after.ov.back.text === 'Name Two' && !after.ov.reason, 'kept with no question: the override holds the new words and no reason: ' + JSON.stringify({ res, ov: after.ov }));
      check(after.tl.some(t => /edited/.test(t) && /Test Operator/.test(t) && /Name Two/i.test(t)) && !after.tl.some(t => /\(Customer asked/.test(t)), 'the order timeline\'s own "edited by" line still records who changed the words, with no reason: ' + JSON.stringify(after.tl));
      check(w.errors.length === 0, 'no page errors: ' + w.errors.join(' | ').slice(0, 300));
    } catch (e) { check(false, 'the page checks threw: ' + (e && e.stack || e)); }
    finally { await w.close(); }
  }
  if (fails.length) { console.log('\nengrave-why: ' + fails.length + ' failed'); fails.forEach(f => console.log('  - ' + f)); process.exit(1); }
  console.log('\nengrave-why: ok');
})().catch(e => { console.error(e); process.exit(1); });
