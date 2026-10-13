// APPRFIX-B (Paul, 13 Oct 2026, order 4194538165: "in Engraving, after I change the auto-selected face the card's Approve button does nothing when pressed").
//
// A card that was drawn while its placement waited for approval stayed on screen when the placement left that state (renderView's "nothing about what this pane holds has changed" shortcut compared the
// card's state as it was DRAWN, `card.dataset.state === "review"`, never the job's state now, nor which job object the card belongs to). Its Approve button stayed live, and a press was dropped without a word
// by cardAction / approveOnce ("a card that is not the job in the list, or a job that is not in review, approves nothing"): the person pressed, nothing happened, nothing said why, and no render ever drew
// the card again.  Two ways a placement leaves review under a card that is in front, both with no face switch at all:
//   1  a Save in the panel whose words (or the font picked for the face) the font cannot engrave: seedWords -> setReady leaves the job in "words" (Engrave.items), fit null, card untouched
//   2  an order that changed on Etsy: Engrave.invalidate(row) puts every undecided job of the line back to "classify" (charm-nest-bridge.js ~1802)
// and 3: the job object behind the card is replaced by another under the same key (a restored / recalled / re-made job): the card's click handler holds the OLD object.
// The rule this test holds: a card in front is always the card of the job that is in the list NOW; a placement that is not waiting for approval has no live Approve; a press on a live Approve always does something.
//
// Offline only: the REAL page over the repo's fake site with real drawings (_engraveRealWorld.cjs). Made-up names only; no Etsy, no paid call, no live write.
//   PW_DIR=/opt/node-tools/node_modules node tests/charm-nest/engrave-stale-approve.cjs        (CHROMIUM=<chrome> if the default path is missing)
//   Exit code 1 while any check is red.
const R = require('./_engraveRealWorld.cjs');
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${evidence}`); };
const Q = '#egQueue .rvItem[data-kind=placement]';

(async () => {
  const rid = R.ORDERS.both.rid, BACK = rid + '1';   // (the back job's key without the order prefix; the front's ends in ~front)
  /** a fresh page for each case (an approval or a restore is permanent in a page) with the two-face necklace built and its back card open */
  async function fresh(run) {
    const w = await R.world({ viewport: { width: 1440, height: 900 } });
    if (!w) { console.log('no playwright-core: nothing run'); process.exit(0); }
    const page = w.page;
    const facts = () => page.evaluate(({ Q, rid }) => {
      const c = document.querySelector(Q), ap = c && c.querySelector('[data-a="approve"]');
      return { card: c ? c.dataset.key.replace(/^\d+_/, '') : null, approve: ap ? (ap.disabled ? 'dimmed' : 'live') : 'none', toasts: [...document.querySelectorAll('#toasts .toast')].map(t => t.dataset.msg),
        jobs: Object.fromEntries([...Engrave.items().values()].filter(j => j.key.startsWith(rid)).map(j => [j.key.replace(/^\d+_/, ''), j.state])) };
    }, { Q, rid });
    /** a press on whatever Approve the card offers: the DOM's own click (a card with no live Approve is reported as such) */
    const press = () => page.evaluate(Q => { const b = document.querySelector(Q + ' [data-a="approve"]'); if (!b) return 'none'; if (b.disabled) return 'dimmed'; b.click(); return 'pressed'; }, Q);
    const until = async (fn, ms) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) return null; await new Promise(r => setTimeout(r, 200)); } };
    try {
      await R.build(w, ['both']);
      await R.openEngraveDetail(w, 'both', { face: 'back' }); await R.settle(page, 600);
      await run({ w, page, facts, press, until });
    } catch (e) { check('run', false, String(e && e.stack || e).split('\n').slice(0, 3).join(' | ')); }
    check('no page error', w.errors.length === 0, w.errors.slice(0, 2).join(' | '));
    await w.close();
  }
  /** a live Approve on a placement that is not waiting for approval is a button that cannot work */
  const deadButton = f => f.approve === 'live' && f.jobs[f.card] !== 'review';

  // ── 1 · a Save whose words the font cannot engrave: the job is "words", the card must not go on offering Approve ──
  await fresh(async ({ page, facts, press, until }) => {
    const f0 = await facts();
    check('S0 the back card opens with a live Approve on a placement waiting for approval', f0.approve === 'live' && f0.card === BACK && f0.jobs[BACK] === 'review', JSON.stringify(f0));
    await page.evaluate(({ Q }) => { const t = document.querySelector(Q + ' .engpWords'), set = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value').set; t.focus(); set.call(t, '漢字'); t.dispatchEvent(new InputEvent('input', { bubbles: true, inputType: 'insertText', data: 'x' })); }, { Q });
    await R.settle(page, 400);
    await page.click(`${Q} [data-engp=save]`); await R.settle(page, 400);
    const chip = await page.$(`${Q} [data-engp=why]`); if (chip) await chip.click();
    const left = await until(async () => { const f = await facts(); return f.jobs[BACK] !== 'review' ? f : null; }, 15000);
    await R.settle(page, 1200);
    const f1 = await facts();
    check('S1 the Save left the back placement out of review (its words cannot be engraved)', !!left && f1.jobs[BACK] !== 'review', JSON.stringify(f1.jobs));
    check('S2 the card in front does not go on offering a live Approve for a placement that is not waiting for approval', !deadButton(f1), JSON.stringify(f1));
    const p1 = await press(); await R.settle(page, 800);
    const f2 = await facts();
    check('S3 a press on what the card offers either finds no live button or does something the person can see', p1 !== 'pressed' || f2.toasts.length > 0 || JSON.stringify(f2.jobs) !== JSON.stringify(f1.jobs), JSON.stringify({ p1, f2 }));
  });

  // ── 2 · the order changed on Etsy: every undecided job of the line goes back to be read; the card must follow ──
  await fresh(async ({ page, facts, press, until }) => {
    const g0 = await facts();
    check('I0 the back card opens with a live Approve', g0.approve === 'live' && g0.jobs[BACK] === 'review', JSON.stringify(g0));
    const g1 = await page.evaluate(({ Q, rid }) => {   // (read in the very tick of the change: the placement is being read again, and what the card in front offers then)
      const row = Orders.rows().find(r => r.order.receiptId === rid); Engrave.invalidate(row, 'order changed'); Engrave.render();
      const c = document.querySelector(Q), ap = c && c.querySelector('[data-a="approve"]');
      return { card: c ? c.dataset.key.replace(/^\d+_/, '') : null, approve: ap ? (ap.disabled ? 'dimmed' : 'live') : 'none', state: [...Engrave.items().values()].find(j => j.key.startsWith(rid) && !/~front$/.test(j.key)).state };
    }, { Q, rid });
    check('I1 the order changed: its back placement went back to be read', g1.state === 'classify', JSON.stringify(g1));
    check('I2 the card in front offers no live Approve for a placement that is being read again', g1.approve !== 'live', JSON.stringify(g1));
    const g2 = await until(async () => { const f = await facts(); return f.jobs[BACK] === 'review' && f.approve === 'live' ? f : null; }, 30000);
    check('I3 read and fitted again, the card in front offers a live Approve for it again', !!g2, JSON.stringify(g2 || await facts()));
    if (g2) { const p2 = await press(); const g3 = await until(async () => { const f = await facts(); return /approved|written/.test(f.jobs[BACK]) ? f : null; }, 30000); check('I4 and that Approve approves it', p2 === 'pressed' && !!g3, JSON.stringify({ p2, g3 })); }
  });

  // ── 3 · the job object behind the card is replaced under the same key: the card belongs to the job that is in the list ──
  await fresh(async ({ page, facts, press, until }) => {
    const h0 = await facts();
    await page.evaluate(rid => { const m = Engrave.items(); for (const [k, j] of [...m]) if (k.startsWith(rid) && !/~front$/.test(k)) m.set(k, Object.assign({}, j)); Engrave.render(); }, rid);   // (a restored job: another object, the same key and the same state)
    await R.settle(page, 1000);
    const p3 = await press();
    const h1 = await until(async () => { const f = await facts(); return /approved|written/.test(f.jobs[BACK]) ? f : null; }, 30000);
    check('J1 a press on the Approve of a card whose job object was replaced still approves the placement in the list', p3 === 'pressed' && !!h1, JSON.stringify({ h0: h0.jobs, p3, h1: h1 && h1.jobs }));
  });

  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\n${RESULTS.length - bad.length}/${RESULTS.length} pass`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
