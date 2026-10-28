// APPROVEFIX (Paul, 28 Oct 2026, order 4212020155: "The 'Approve' button is not working after I made adjustments to this charm's engraving.")
//
// Paul's card: words typed and put back in the panel (its Save greyed, its Cancel live), the words moved in the work area, a green Approve that did nothing, and nothing written (no
// engraveApproved, no engraveChanged). An approval waits on several things (the fit of new words, a sign-in, the back file, the workspace save, the stamp); one that never came back held
// job._approvalTask and approvalPreparing for good: that press never ended, the card kept no word of why, and every later press was dropped without a word by cardAction / approve().
// The rule this test holds, on a BACK and on a FRONT: a press on Approve either approves or says plainly IN THE CARD why not, within the time limit of the wait that hangs; the next press
// works; the moves made before it are kept (and are what gets approved). Staged words dim Approve with the reason written in the card, never only a tooltip.
//
// Offline only: the REAL page over the repo's fake site with real drawings (_engraveRealWorld.cjs). Made-up names only; no Etsy, no paid call, no live write.
//   /tmp/.../int/slot.sh timeout 600 node tests/charm-nest/engrave-approve-never-silent.cjs      Exit code 1 while any check is red.
'use strict';
const R = require('./_engraveRealWorld.cjs');
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${evidence}`); };
const Q = '#egQueue .rvItem[data-kind=placement]';
const SCALE = 0.02;   // (CN_SPIN_SCALE while the wait hangs: the 30 s limit of the workspace save is 0.6 s here)

(async () => {
  const w = await R.world({ viewport: { width: 1440, height: 900 } });
  if (!w) { console.log('no playwright-core: nothing run'); process.exit(0); }
  const page = w.page;
  const facts = key => page.evaluate(({ Q, key }) => {
    const j = Engrave.items().get(key), c = document.querySelector(Q), mine = !!c && c.dataset.key === key, ap = mine ? c.querySelector('[data-a="approve"]') : null, why = mine ? c.querySelector('.egApproveWhy') : null;
    const sv = mine ? c.querySelector('[data-engp="save"]') : null, cn = mine ? c.querySelector('[data-engp="cancel"]') : null;
    return { mine, state: j && j.state, approvedAt: j && j.approvedAt || null, centre: j && j.fit ? j.fit.centre.map(v => Math.round(v * 1000) / 1000) : null, nudged: !!(j && j.nudged),
      busy: !!(j && (j._approvalTask || j.approvalPreparing || j.stamping)), approve: ap ? { text: ap.textContent.trim(), live: !ap.disabled } : null,
      why: why && !why.hidden ? why.textContent.trim() : '', save: sv ? !sv.disabled : null, cancel: cn ? !cn.disabled : null };
  }, { Q, key });
  const until = async (fn, ms) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) return null; await new Promise(r => setTimeout(r, 100)); } };
  const press = () => page.evaluate(Q => { const b = document.querySelector(Q + ' [data-a="approve"]'); if (!b) return 'none'; if (b.disabled) return 'dimmed'; b.click(); return 'pressed'; }, Q);
  /** Paul's adjustments: a letter typed in the panel's words and taken out again (Save greyed, Cancel live), then the words dragged in the work area. */
  async function adjust(key, tag) {
    const words = `${Q} .engpWords`;
    await page.focus(words); await page.keyboard.press('End'); await page.keyboard.type('x'); await R.settle(page, 150); await page.keyboard.press('Backspace'); await R.settle(page, 250);
    const c0 = (await facts(key)).centre;
    for (const [dx, dy] of [[5, 2], [-5, 2], [5, -2], [-5, -2]]) {
      const b = await page.evaluate(Q => { const c = document.querySelector(Q + ' .backHost canvas'); if (!c || !c._box) return null; const r = c.getBoundingClientRect(), k = r.width / c.width; return [r.left + c._box.centrePx[0] * k, r.top + c._box.centrePx[1] * k]; }, Q);
      if (!b) break;
      await page.mouse.move(b[0], b[1]); await page.mouse.down(); await page.mouse.move(b[0] + dx, b[1] + dy, { steps: 4 }); await page.mouse.up(); await R.settle(page, 300);
      if (JSON.stringify((await facts(key)).centre) !== JSON.stringify(c0)) break;
    }
    const f = await facts(key);
    check(`${tag}0 Paul's adjustments: the words moved in the work area, the panel's Save greyed and its Cancel live, a live Approve`, f.mine && f.state === 'review' && f.nudged && JSON.stringify(f.centre) !== JSON.stringify(c0) && f.save === false && f.cancel === true && f.approve && f.approve.live, JSON.stringify({ c0, f }));
    return f;
  }
  /** A wait of the approval that never comes back (the workspace save), a press, the reason in the card, then the same press approves once the wait is back. */
  async function hangThenApprove(name, face, tag) {
    const key = await R.openEngraveDetail(w, name, { face }); await R.settle(page, 600);
    const f0 = await adjust(key, tag);
    await page.evaluate(s => { window.__flush = Session.flushNow; Session.flushNow = () => new Promise(() => {}); window.CN_SPIN_SCALE = s; }, SCALE);
    const t0 = Date.now(), p1 = await press();
    const f1 = await until(async () => { const f = await facts(key); return f.why ? f : null; }, 8000), ms = Date.now() - t0;
    check(`${tag}1 the press while the workspace save hangs is not silent: the card says why it was not approved`, p1 === 'pressed' && !!f1 && /^Not approved/.test(f1.why), JSON.stringify({ p1, ms, f1 }));
    const f2 = await until(async () => { const f = await facts(key); return !f.busy && f.approve && f.approve.live ? f : null; }, 4000) || await facts(key);
    check(`${tag}2 it does not stay stuck: the approval let go (no task, nothing preparing), Approve is live and reads "Approve" again`, !f2.busy && f2.state === 'review' && f2.approve && f2.approve.live && f2.approve.text === 'Approve', JSON.stringify(f2));
    check(`${tag}3 the adjustments are kept: the words stay where they were moved`, JSON.stringify(f2.centre) === JSON.stringify(f0.centre) && f2.nudged, JSON.stringify({ moved: f0.centre, now: f2.centre }));
    await page.evaluate(() => { Session.flushNow = window.__flush; window.CN_SPIN_SCALE = 1; });
    const p2 = await press();
    const f3 = await until(async () => { const f = await facts(key); return f.state === 'approved' || f.state === 'written' ? f : null; }, 30000) || await facts(key);
    check(`${tag}4 the next press approves it, at the place the words were moved to`, p2 === 'pressed' && /approved|written/.test(f3.state) && !!f3.approvedAt && JSON.stringify(f3.centre) === JSON.stringify(f0.centre), JSON.stringify({ p2, f3 }));
  }
  try {
    await R.build(w, ['back', 'both']);
    await hangThenApprove('back', 'back', 'B');
    await hangThenApprove('both', 'front', 'F');   // (Paul, 28 Oct 2026: bar necklaces engrave on the FRONT by default: the same press on a front)
    // the panel's gate: staged words dim Approve, and the card says why in words (not only in a tooltip); taken out again, Approve is live and the line is gone
    const key = await R.openEngraveDetail(w, 'both', { face: 'back' }); await R.settle(page, 600);
    await page.focus(`${Q} .engpWords`); await page.keyboard.press('End'); await page.keyboard.type('x'); await R.settle(page, 250);
    const g1 = await facts(key);
    check('G1 words staged and not saved: Approve is dimmed and the card says why', g1.approve && !g1.approve.live && /Save or Cancel/.test(g1.why), JSON.stringify(g1));
    await page.keyboard.press('Backspace'); await R.settle(page, 250);
    const g2 = await facts(key);
    check('G2 the words put back: Approve is live and the line is gone', g2.approve && g2.approve.live && !g2.why, JSON.stringify(g2));
  } catch (e) { check('run', false, String(e && e.stack || e).split('\n').slice(0, 3).join(' | ')); }
  check('no page error', w.errors.length === 0, w.errors.slice(0, 2).join(' | '));
  await w.close();
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\n${RESULTS.length - bad.length}/${RESULTS.length} pass`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
