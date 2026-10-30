// APPRFIX-A: the panel's staged-edit gate (charm-nest-engrave-panel.js applyGate: "Save or Cancel the edit first") must give a card's Approve buttons back exactly as the CARD last left them.
//
// The bug (Paul, order 4194538165: "after I change the face the Approve button does nothing"): an edit is staged (words, font or mode), the person presses the other face's Front / Back picture. The Engrave tab draws the card
// again for that face (the staged edit lives on in the panel's draft), the panel gates the new card AT ONCE, when it is built, and keeps each button's pre-gate state (dataset.engpWas). A frame later the card brings
// "Approve both faces" up to date (syncBoth: both placements have now been on screen -> enabled). Cancel (or typing the words back) lifted the gate by writing the OLD state back: "Look at the front placement first",
// disabled, for good. The button looked dead and a press did nothing.
//
// LIVEWORDS (Paul, 29 Oct 2026: "Remove the process that ask the user ... to click save"): there is no gate any more: Approve keeps staged words first, so the panel never dims or retitles a card's Approve
// buttons. What this test still guards: with words staged and the face switched, the card's Approve and "Approve both faces" are exactly what the card says with nothing staged.
// ENGSTAGE (Paul, 30 Oct 2026: one Approve keeps the whole piece, its faces approved Front before Back): there is no "Approve both faces" any more; the guard is the ONE Approve: live with the edit staged, live
// after the face is switched, live when the draft is let go, and never covered.
//
// Offline only: the REAL page over the repo's fake site with the real drawings (tests/charm-nest/_engraveRealWorld.cjs), made-up words, no Etsy, no paid call, no live write.
//   PW_DIR=/opt/node-tools/node_modules node tests/charm-nest/engrave-gate-stale.cjs
'use strict';
let R; try { R = require('./_engraveRealWorld.cjs'); } catch (e) { console.log('engrave-gate-stale: tests/charm-nest/_engraveRealWorld.cjs is not here, skipped'); process.exit(0); }

(async () => {
  const w = await R.world({ viewport: { width: 1440, height: 900 } });
  if (!w) { console.log('engrave-gate-stale: playwright-core not found, skipped'); return; }
  let fails = 0; const check = (ok, msg, more) => { if (!ok) fails++; console.log((ok ? '  ok   ' : '  FAIL ') + msg + (more !== undefined ? '  [' + more + ']' : '')); };
  const page = w.page;
  try {
    await R.build(w, ['both']);   // a necklace with a Front AND a Back (two jobs, both at review): the piece with "Approve both faces"
    const btns = () => page.evaluate(() => {
      const card = document.querySelector('#egQueue .rvItem'), p = card && card.querySelector('.engp'), o = { face: p && p.dataset.face };
      for (const b of card.querySelectorAll('[data-a="approve"],[data-a="approveBoth"]')) o[b.dataset.a] = { dis: b.disabled, title: b.title.slice(0, 44), was: !!b.dataset.engpWas };
      return o;
    });
    const GATE = /Save or Cancel the edit first/;

    // (fresh page: the front has not been on screen yet)
    await R.openEngraveDetail(w, 'both', { face: 'back' });
    await page.evaluate(() => CharmNestEngravePanel.DRAFTS.clear());
    const b0 = await btns();
    check(!b0.approveBoth && b0.approve && b0.approve.dis === false, 'the back card, nothing staged: one Approve (it keeps the whole piece), no "Approve both faces"', JSON.stringify(b0));
    // the case: words staged on the back, the front picture pressed, then the draft let go
    await R.openEngraveDetail(w, 'both', { face: 'back' });
    await page.evaluate(() => CharmNestEngravePanel.DRAFTS.clear());
    await page.evaluate(() => { const ta = document.querySelector('.engp .engpWords'); ta.value = ta.value + ' z'; ta.dispatchEvent(new Event('input', { bubbles: true })); });
    const g0 = await btns();
    check(g0.approve.dis === false && !GATE.test(g0.approve.title) && !g0.approve.was, 'words staged: Approve is NOT dimmed (it keeps the words first), no "Save or Cancel"', JSON.stringify(g0.approve));
    await page.evaluate(() => { const t = document.querySelector('.pvSetup [data-eng-thumb=front]') || document.querySelector('.pvSetup .engFace[data-face=front] .engThumb'); if (t) t.click(); }); await R.settle(page, 1500);
    const g1 = await btns();
    check(g1.face === 'front' && g1.approve.dis === false && !GATE.test(g1.approve.title) && !g1.approve.was, 'the front card is drawn with the edit still staged: its Approve is as the card has it', JSON.stringify(g1.approve));
    await page.evaluate(() => CharmNestEngravePanel.DRAFTS.clear()); await R.settle(page, 500);
    const g2 = await btns();
    check(g2.approve.dis === false && !g2.approve.was && /approve/i.test(g2.approve.title), 'Approve is as the card had it', JSON.stringify(g2.approve));
    // and nothing covers it: a press at its centre lands on the button itself (a press would approve both faces of the fake order: not made here)
    const hit = await page.evaluate(() => { const b = document.querySelector('#egQueue .rvItem [data-a="approve"]'); b.scrollIntoView({ block: 'center' }); const r = b.getBoundingClientRect(), t = document.elementFromPoint(r.x + r.width / 2, r.y + r.height / 2); return !!(t && t.closest('[data-a="approve"]')); });
    check(hit, 'a press at the centre of Approve lands on the button');
    // the reference: the same front card visited with NOTHING staged (the back was on screen, now the front is)
    await R.openEngraveDetail(w, 'both', { face: 'back' });
    await page.evaluate(() => CharmNestEngravePanel.DRAFTS.clear());
    await page.evaluate(() => { const t = document.querySelector('.pvSetup [data-eng-thumb=front]') || document.querySelector('.pvSetup .engFace[data-face=front] .engThumb'); if (t) t.click(); }); await R.settle(page, 1500);
    const ref = await btns();
    check(ref.face === 'front' && !ref.approveBoth && ref.approve.dis === false, 'the front shown, nothing staged: the one Approve is on', JSON.stringify(ref));
    check(g2.approve.dis === ref.approve.dis && g2.approve.title === ref.approve.title && !g2.approve.was, 'Approve is exactly what the same card says with nothing staged', JSON.stringify({ after: g2.approve, reference: ref.approve }));
    check(w.errors.length === 0, 'no page errors', JSON.stringify(w.errors).slice(0, 300));
  } catch (e) { fails++; console.log('  FAIL threw: ' + (e && e.stack || e)); }
  finally { await w.close(); }
  console.log(fails ? 'engrave-gate-stale: ' + fails + ' check(s) FAILED' : 'engrave-gate-stale: all checks passed');
  process.exit(fails ? 1 : 0);
})();
