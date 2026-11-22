// APPROVEFIX (Paul, 28 Oct 2026, order 4212020155: "The 'Approve' button is not working after I made adjustments to this charm's engraving.")
//
// Paul's card: words typed and put back in the panel (its Save greyed, its Cancel live), the words moved in the work area, a green Approve that did nothing, and nothing written (no
// engraveApproved, no engraveChanged). An approval waits on several things (the fit of new words, a sign-in, the back file, the workspace save, the stamp); one that never came back held
// job._approvalTask and approvalPreparing for good: that press never ended, the card kept no word of why, and every later press was dropped without a word by cardAction / approve().
// The rule this test holds, on a BACK and on a FRONT: a press on Approve either approves or says plainly IN THE CARD why not, within the time limit of the wait that hangs; the next press
// works; the moves made before it are kept (and are what gets approved). Staged words dim Approve with the reason written in the card, never only a tooltip.
// APPROVECLOUD (Paul, 20 Nov 2026, commit a13213e, after "Not approved: it took too long" on an approval still working): while a press works the card says "Approving…" and the step it is
// at, in words, under Approve; it fails only on a real error (a wait past its own limit, in minutes) or after 4 minutes with nothing moving, says why, and Approve is pressable again.
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
    const j = Engrave.items().get(key), c = document.querySelector(Q), mine = !!c && c.dataset.key === key, ap = mine ? (c.querySelector('[data-engp-approve]') || c.querySelector('[data-a="approve"]')) : null, why = mine ? c.querySelector('.egApproveWhy') : null;
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
    check(`${tag}0 Paul's adjustments: the words moved in the work area, no Save / Cancel in the panel, a live Approve`, f.mine && f.state === 'review' && f.nudged && JSON.stringify(f.centre) !== JSON.stringify(c0) && f.save === null && f.cancel === null && f.approve && f.approve.live, JSON.stringify({ c0, f }));   // (LIVEWORDS: the panel has no Save and no Cancel)
    return f;
  }
  /** A wait of the approval that never comes back (the workspace save), a press, the reason in the card, then the same press approves once the wait is back. */
  async function hangThenApprove(name, face, tag) {
    const key = await R.openEngraveDetail(w, name, { face }); await R.settle(page, 600);
    const f0 = await adjust(key, tag);
    await page.evaluate(s => { window.__flush = Session.flushNow; Session.flushNow = () => new Promise(() => {}); window.CN_SPIN_SCALE = s; }, SCALE);
    const t0 = Date.now(), p1 = await press();
    // (APPROVECLOUD: never silent. At once, and at every look while it works, "Approving…" (off) with the step it is at in the card's line, never "Not approved" while the hung wait is within its
    //  own limit (the workspace save's 2 minutes, APPROVE_MS.save in charm-nest-bridge.js; CN_SPIN_SCALE makes it 2.4 s here); once past it, "Not approved:" with why (the workspace save named),
    //  never the old "it took too long")
    const looks = [], SAVE_LIMIT_MS = 2 * 60000 * SCALE;
    const f1 = await until(async () => { const f = await facts(key); looks.push(f); return /^Not approved/.test(f.why) ? f : null; }, 8000), ms = Date.now() - t0;
    const working = f1 ? looks.slice(0, -1) : looks;
    // (INSTANTAPPROVE, Paul 22 Nov 2026: "The approval process needs to be instantaneous": "Approved ✓" at once and while the save works behind it, never "Approving…"; the save that never answered
    //  takes it back: Approve lit again, "Not approved:" with why)
    check(`${tag}1 the press while the workspace save hangs: "Approved ✓" at once and while it works, then taken back with why it was not approved once the save's limit passed`,
      p1 === 'pressed' && working.length > 0 && working.every(f => f.approve && /^Approved/.test(f.approve.text) && !f.approve.live && !/^Not approved/.test(f.why))
      && !!f1 && /^Not approved/.test(f1.why) && /workspace save/.test(f1.why) && !/took too long/.test(f1.why) && ms >= SAVE_LIMIT_MS && !!f1.approve && f1.approve.live && f1.approve.text === 'Approve',
      JSON.stringify({ p1, ms, looks: looks.length, steps: [...new Set(working.map(f => f.why))], first: working[0] || null, f1 }));
    const f2 = await until(async () => { const f = await facts(key); return !f.busy && f.approve && f.approve.live ? f : null; }, 4000) || await facts(key);
    check(`${tag}2 it does not stay stuck: the approval let go (no task, nothing preparing), Approve is live and reads "Approve" again`, !f2.busy && f2.state === 'review' && f2.approve && f2.approve.live && f2.approve.text === 'Approve', JSON.stringify(f2));
    check(`${tag}3 the adjustments are kept: the words stay where they were moved`, JSON.stringify(f2.centre) === JSON.stringify(f0.centre) && f2.nudged, JSON.stringify({ moved: f0.centre, now: f2.centre }));
    await page.evaluate(() => { Session.flushNow = window.__flush; window.CN_SPIN_SCALE = 1; });
    await page.evaluate(Q => { window.__card0 = document.querySelector(Q); window.__rebuilt = 0; }, Q);
    const p2 = await press();
    // (INSTANTAPPROVE, Paul 22 Nov 2026: the approval and its seal show at once; NOFLICKER: the card is not drawn again while its approval lands)
    const now0 = await page.evaluate(Q => { const c = document.querySelector(Q), b = c && c.querySelector('[data-engp-approve]'); return { text: b ? b.textContent.trim() : '', seals: c ? c.querySelectorAll('.egApproveWrap .seal').length : 0, prov: c ? c.querySelectorAll('.egApproveWrap .seal[data-provisional]').length : 0 }; }, Q);
    check(`${tag}4a the press shows "Approved ✓" and its seal at once, before anything is saved`, p2 === 'pressed' && /^Approved/.test(now0.text) && now0.text.includes('✓') && now0.prov >= 1, JSON.stringify(now0));
    const f3 = await until(async () => { const f = await facts(key); const r = await page.evaluate(({ Q, key }) => { const c = document.querySelector(Q); if (c && c.dataset.key === key && c !== window.__card0) window.__rebuilt++; return true; }, { Q, key }); return f.state === 'approved' || f.state === 'written' ? f : null; }, 30000) || await facts(key);
    await R.settle(page, 400);
    const after = await page.evaluate(({ Q, key }) => { const c = document.querySelector(Q), mine = !!c && c.dataset.key === key; return { mine, same: c === window.__card0, rebuilt: window.__rebuilt, seals: mine ? c.querySelectorAll('.egApproveWrap .seal').length : -1, prov: mine ? c.querySelectorAll('.egApproveWrap .seal[data-provisional]').length : -1 }; }, { Q, key });
    check(`${tag}4b when it is saved: one seal (the press's seal taken over, never stamped twice) and the card was never drawn again (no flicker)`, after.rebuilt === 0 && (!after.mine || (after.same && after.seals === now0.seals && after.prov === 0)), JSON.stringify({ now0, after }));
    check(`${tag}4 the next press approves it, at the place the words were moved to`, p2 === 'pressed' && /approved|written/.test(f3.state) && !!f3.approvedAt && JSON.stringify(f3.centre) === JSON.stringify(f0.centre), JSON.stringify({ p2, f3 }));
  }
  try {
    await R.build(w, ['back', 'both']);
    await hangThenApprove('back', 'back', 'B');
    await hangThenApprove('both', 'front', 'F');   // (Paul, 28 Oct 2026: bar necklaces engrave on the FRONT by default: the same press on a front)
    // (ENGSTAGE, Paul 30 Oct 2026: the one Approve keeps the whole piece: its back is approved too; that approval's tail (its file, the tab moving on) is let finish before the piece is opened again)
    await until(() => page.evaluate(() => !(window.CharmNestEngravePanel && CharmNestEngravePanel.COMMITS.size) && ![...Engrave.items().values()].some(j => j._approvalTask || j._backTask || j.approvalPreparing || j.stamping || j.backSaving)), 60000); await R.settle(page, 600);
    // LIVEWORDS: staged words never dim Approve (a press keeps them first); taken out again, Approve is live and no line is there
    const key = await R.openEngraveDetail(w, 'both', { face: 'back' }); await R.settle(page, 600);
    await page.focus(`${Q} .engpWords`); await page.keyboard.press('End'); await page.keyboard.type('x'); await R.settle(page, 250);
    const g1 = await facts(key);
    check('G1 words staged: Approve stays live, no "Save or Cancel" line', g1.approve && g1.approve.live && !/Save or Cancel/.test(g1.why), JSON.stringify(g1));
    await page.keyboard.press('Backspace'); await R.settle(page, 250);
    const g2 = await facts(key);
    // (ENGSTAGE: the words put back on a piece the one press approved: nothing is staged, so Approve is the calm "Approved" again, or live on a piece still to approve)
    check('G2 the words put back: Approve is as before the letter (calm on an approved piece, live otherwise) and the line is gone', (g2.approve ? (g2.approve.live || (g2.state !== 'review' && /^Approved/.test(g2.approve.text))) : !!(await page.evaluate(Q => document.querySelector(Q + ' [data-a="approved"]'), Q))) && !g2.why, JSON.stringify(g2));
  } catch (e) { check('run', false, String(e && e.stack || e).split('\n').slice(0, 3).join(' | ')); }
  check('no page error', w.errors.length === 0, w.errors.slice(0, 2).join(' | '));
  await w.close();
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\n${RESULTS.length - bad.length}/${RESULTS.length} pass`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
