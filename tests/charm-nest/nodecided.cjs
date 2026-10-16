// NODECIDED (Paul, 16 Oct 2026): "Get rid of this Decided view. Instead just open the existing detailed Engraving modal without needing to Re-Open the order. The user should be free to
// update/modify any already Approved engraving order and simply need to press the Approve button again after the second change. This would then produce a second brand new seal as we already
// do today. This removes the need for this extra view and Re-Open feature."
//
// The REAL page (charm-nest-1.html) over the repo's offline fake site with REAL drawings (tests/charm-nest/_engraveRealWorld.cjs): real intake, pool, classifier, fit, a real installed font; the
// approvals below are made by pressing the card's own Approve (the fake site's in-memory Firestore); made-up buyer names only; no Etsy, no paid call, nothing live.
//   1  there is no Decided tab, list, row, detail or Reopen anywhere (`.egTab[data-tab=done]`, `#egDone`, `#egBacks`, `.decidedRow`, `.doneDetail`, `[data-a=reopen]`, `[data-a=monoChange]`, no "Reopen" word)
//   2  every piece, approved ones too, is a row of the ONE list (`#egQueue .egPlacementList .placementRow`), each piece with its own state word and its own seals; rows that still need a decision come first
//   3  a click on an approved piece's row opens the SAME detailed card as any piece (`.rvItem[data-kind=placement]`), with a calm disabled "Approved" tick where the Approve was; just looking
//      changes nothing (no state change, no seal, no request that writes)
//   4  the FIRST change of an approved piece (a nudge, the words) flips ONLY that piece to "needs Approve" at once: the Approve button is back, the old seal stays, the first back file is
//      superseded (backInvalidate); Approve again adds a SECOND, brand new seal, the first one stays (newest first); one seal each on the pieces nobody changed
//   5  the other pieces of the same order (the other face, the other discs) keep their approval and their seal; a piece "Saved to sheet" (written) is changed the same way, with no confirm dialog
//   6  Back / Next walk EVERY piece of the list, approved ones too, and wrap
//   7  the old doors still land on the piece: restoreView({tab:'done', openDone}) opens that piece's card, restoreView({tab:'done'}) shows the list, view().tab is always 'place'
//   8  phone width (400): the list and the card do not overflow sideways; no page errors; no call outside the loopback
//   PW_DIR=/opt/node-tools/node_modules node tests/charm-nest/nodecided.cjs      (SHOTS=<dir> keeps screenshots)
'use strict';
const R = require('./_engraveRealWorld.cjs');
const fails = [];
const note = (ok, what, more) => { console.log((ok ? '  PASS ' : '  FAIL ') + what + (more ? '  [' + more + ']' : '')); if (!ok) fails.push(what); };
const Q = '#egQueue .rvItem[data-kind=placement]';
const NUDGE = `(() => { for (const a of ['right', 'left', 'up', 'down']) { const b = document.querySelector('#egQueue .rvItem .engpPb[data-act="' + a + '"]'); if (b && !b.disabled && !/no room/i.test(b.title || '')) { b.click(); return a; } } return null; })()`;   // (a press that has room: a piece against the edge of its safe area has no room that way)

(async () => {
  const w = await R.world({ viewport: { width: 1440, height: 900 }, shots: process.env.SHOTS || '/tmp/dt/shots' });
  if (!w) { console.log('no playwright-core: nothing run'); return; }
  const page = w.page, settle = (ms = 500) => R.settle(page, ms);
  const dialogs = [], ops = [], outside = [];
  page.on('dialog', d => { dialogs.push(d.message()); d.dismiss().catch(() => {}); });
  page.on('request', rq => {
    const u = rq.url();
    if (!/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u)) { outside.push(u); return; }
    if (rq.method() === 'POST') { let op = ''; try { op = (JSON.parse(rq.postData() || '{}') || {}).op || ''; } catch (_) {} if (!/^(get|list|read|cancelList|backList|poolList|jobList|sandboxStatus)/i.test(op)) ops.push(op || 'POST'); }   // (reads and the page's own polls are not writes)
  });
  try {
    const made = await R.build(w, ['back', 'both', 'discs']);
    const K = { back: made.back.key, both: made.both.key, discs: made.discs.key };
    const jobsOf = lineKey => page.evaluate(k => Engrave.jobsOf(B.orders.byKey.get(k)).map(j => ({ key: j.key, face: j.face === 'front' ? 'front' : 'back', slot: j.slot || null, state: j.state, seals: CNEngravingSeals.list(j).length, by: j.approvedBy || null, at: j.approvedAt || 0 })), lineKey);
    const jobOf = async (lineKey, face, slot) => (await jobsOf(lineKey)).find(j => (!face || j.face === face) && (!slot || j.slot === slot));
    const until = async (fn, ms = 60000) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) return null; await new Promise(r => setTimeout(r, 150)); } };
    const openCard = async key => { await page.evaluate(k => { try { OrderWin.close(); } catch (_) {} CN.setMode('engrave'); Engrave.restoreView({ list: false, chosen: true, focus: k, q: '' }); Engrave.render(); }, key); await page.waitForSelector(`${Q}[data-key="${key}"]`, { timeout: 20000 }); await settle(700); };
    const showList = async q => { await page.evaluate(q => { try { OrderWin.close(); } catch (_) {} CN.setMode('engrave'); Engrave.restoreView({ list: true, chosen: true, focus: null, q: q || '' }); Engrave.render(); }, q || ''); await page.waitForSelector('#egQueue .egPlacementList .placementRow', { timeout: 20000 }); await settle(700); };
    const cardFacts = () => page.evaluate(Q => {
      const c = document.querySelector(Q); if (!c) return null;
      const t = c.querySelector('.pvApproval button.egApproveButton.egApproved[data-a=approved]');
      return { key: c.dataset.key, state: c.dataset.state, approve: !!c.querySelector('[data-a=approve]:not([disabled])'), tick: !!t, tickOff: !!(t && t.disabled), tickText: t ? t.textContent.trim() : '', seals: c.querySelectorAll('.pvApproval .seal').length,
        written: (c.querySelector('.egWrittenNote') || {}).textContent || '', reopen: [...c.querySelectorAll('button')].filter(b => /re-?open/i.test(b.textContent) || b.dataset.a === 'reopen').length, scrollW: document.documentElement.scrollWidth, innerW: innerWidth };
    }, Q);
    const approveCard = async (key, ms = 90000) => {
      await openCard(key);
      await page.click(`${Q}[data-key="${key}"] [data-a=approve]`);
      const j = await until(async () => { const x = (await page.evaluate(k => { const j = Engrave.items().get(k); return j && { state: j.state, seals: CNEngravingSeals.list(j).length }; }, key)); return x && ['approved', 'written'].includes(x.state) ? x : null; }, ms);
      await until(async () => page.evaluate(k => { const j = Engrave.items().get(k); return !(j.backSaving || j.approvalPreparing || j.stamping || j._backTask || j._approvalTask); }, key), 60000);
      await settle(800); return j;
    };
    const flags = key => page.evaluate(k => { const j = Engrave.items().get(k), b = document.querySelector('#egQueue .rvItem .engpPb[data-act="right"]'); return { state: j.state, backSaving: !!j.backSaving, prep: !!j.approvalPreparing, task: !!(j._backTask || j._approvalTask), pending: j.backPending || null, btn: b ? !b.disabled : null, title: b ? b.title : null }; }, key);
    const nudge = async key => { await openCard(key); await page.waitForFunction(() => ['right', 'left', 'up', 'down'].some(a => { const b = document.querySelector('#egQueue .rvItem .engpPb[data-act="' + a + '"]'); return b && !b.disabled && !/no room/i.test(b.title || ''); }), null, { timeout: 20000 }).catch(() => {}); await page.evaluate(NUDGE); };
    const stateOf = key => page.evaluate(k => { const j = Engrave.items().get(k); return j && { state: j.state, seals: CNEngravingSeals.list(j).length, backs: (j.backs || []).length }; }, key);

    // ═══ 1 · nothing of the Decided view is left, with nothing approved yet ═══
    await showList(); await settle(800);
    const GONE = '#engraveView .egTab[data-tab="done"], #egDone, #egBacks, .decidedRow, .doneDetail, [data-a=reopen], [data-a=monoChange]';
    const chrome0 = await page.evaluate(GONE => ({ tabs: [...document.querySelectorAll('#engraveView .egTab[data-tab]')].map(b => b.dataset.tab + (b.classList.contains('on') ? '*' : '')), gone: document.querySelectorAll(GONE).length, reopenWord: /re-?open/i.test(document.getElementById('engraveView').innerText), tab: Engrave.view().tab }), GONE);
    note(chrome0.tabs.join() === 'place*' && chrome0.gone === 0 && !chrome0.reopenWord && chrome0.tab === 'place', '1a one chip (Placements), no Decided tab / list / detail / Reopen on the page', JSON.stringify(chrome0));

    // ═══ 2 · approve a back-only piece, both faces of a faced piece and Disc 1 of 3, each by its own press ═══
    const aBack = await approveCard(K.back);
    note(aBack && aBack.state === 'approved' && aBack.seals === 1, '2a the back-only piece is approved with ONE seal', JSON.stringify(aBack));
    const bj = await jobsOf(K.both); const bBack = bj.find(j => j.face === 'back'), bFront = bj.find(j => j.face === 'front');
    note(!!bBack && !!bFront, '2b the faced piece has a Back and a Front job', JSON.stringify(bj));
    await approveCard(bBack.key); await approveCard(bFront.key);
    const bj2 = await jobsOf(K.both);
    note(bj2.every(j => j.state === 'approved' && j.seals === 1), '2c both faces approved, one seal each', JSON.stringify(bj2));
    const d1 = (await jobsOf(K.discs)).find(j => j.slot === 'D1');
    await approveCard(d1.key);
    const dj = await jobsOf(K.discs);
    note(dj.find(j => j.slot === 'D1').state === 'approved' && dj.filter(j => j.slot !== 'D1').every(j => j.state === 'review' && j.seals === 0), '2d Disc 1 approved alone; Disc 2 and Disc 3 still wait, no seal', JSON.stringify(dj));

    // ═══ 3 · the ONE list: approved pieces are rows beside the pending ones ═══
    await showList(); await settle(800);
    const list = await page.evaluate(() => {
      const rows = [...document.querySelectorAll('#egQueue .egPlacementList .placementRow')];
      return { rows: rows.map(r => ({ rid: r.dataset.rid, open: r.dataset.open, decided: r.classList.contains('isDecided') || r.dataset.decided === '1', seals: r.querySelectorAll('.egPlacementSeals .seal').length,
        s: [...r.querySelectorAll('[data-s]')].map(n => n.dataset.s + ':' + n.textContent.trim()), reopen: r.querySelectorAll('[data-a=reopen]').length })),
        badge: (document.querySelector('.egTab[data-tab="place"] b') || {}).textContent || '', tabs: [...document.querySelectorAll('#engraveView .egTab[data-tab]')].length, gone: document.querySelectorAll('#egDone, #egBacks, .decidedRow, .doneDetail, [data-a=reopen]').length };
    }, null);
    const rowOf = rid => list.rows.find(r => r.rid === rid);
    const rBack = rowOf(made.back.rid), rBoth = rowOf(made.both.rid), rDiscs = rowOf(made.discs.rid);
    note(!!rBack && rBack.decided && rBack.s.length === 1 && /^approved:Approved/.test(rBack.s[0]) && rBack.seals === 1, '3a the approved back-only piece is a row of the one list: "Approved", its seal on the row', JSON.stringify(rBack));
    note(!!rBoth && rBoth.decided && rBoth.s.length === 2 && rBoth.s.every(x => /^approved:Approved/.test(x)) && rBoth.seals === 2, '3b the faced piece is ONE row: Front and Back each "Approved", a seal each', JSON.stringify(rBoth));
    note(!!rDiscs && !rDiscs.decided && rDiscs.s.filter(x => /^approved:Approved/.test(x)).length === 1 && rDiscs.s.length === 3, '3c the necklace of discs is ONE row: Disc 1 "Approved", the others not (the row still needs a decision)', JSON.stringify(rDiscs));
    note(list.rows.findIndex(r => r.rid === made.discs.rid) < list.rows.findIndex(r => r.rid === made.back.rid) && list.rows.findIndex(r => r.rid === made.discs.rid) < list.rows.findIndex(r => r.rid === made.both.rid), '3d rows that still need a decision come before the rows that are all decided', list.rows.map(r => r.rid.slice(-4) + (r.decided ? 'D' : 'P')).join(' '));
    note(list.badge === '1' && list.tabs === 1 && list.gone === 0 && list.rows.every(r => r.reopen === 0), '3e the one chip counts the rows that still need a decision (1); no Decided tab, no Reopen on any row', JSON.stringify({ badge: list.badge, tabs: list.tabs, gone: list.gone }));
    await R.shot(w, 'NODECIDED-1-list');

    // ═══ 4 · a click on an approved row opens the SAME card; looking changes nothing ═══
    const opsBefore = ops.length, s0 = await stateOf(K.back);
    await page.click(`#egQueue .placementRow[data-rid="${made.back.rid}"]`);
    await page.waitForSelector(`${Q}[data-key="${K.back}"]`, { timeout: 15000 }); await settle(1200);
    let c = await cardFacts();
    note(c && c.key === K.back && c.state === 'approved' && c.tick && c.tickOff && /^Approved/.test(c.tickText) && !c.approve && c.seals === 1 && c.reopen === 0, '4a the approved piece opens in the same detailed card: a calm disabled "Approved" tick where Approve was, its seal, no Reopen', JSON.stringify(c));
    await settle(1500);
    const s1 = await stateOf(K.back), opsAfter = ops.length;
    note(JSON.stringify(s0) === JSON.stringify(s1) && opsAfter === opsBefore, '4b just looking changes nothing: same state, same seals, same files, no request that writes', JSON.stringify({ s0, s1, opsBefore, opsAfter, new: ops.slice(opsBefore) }));
    await R.shot(w, 'NODECIDED-2-approved-card');

    // ═══ 5 · the FIRST change flips it to "needs Approve"; Approve again makes a SECOND seal and keeps the first ═══
    const at0 = (await jobOf(K.back, 'back')).at;
    const opsMark = ops.length;
    const fl0 = await flags(K.back); await page.waitForFunction(() => ['right', 'left', 'up', 'down'].some(a => { const b = document.querySelector('#egQueue .rvItem .engpPb[data-act="' + a + '"]'); return b && !b.disabled && !/no room/i.test(b.title || ''); }), null, { timeout: 20000 }).catch(() => {});
    const pressed = await page.evaluate(NUDGE);
    const flipped = await until(async () => { const x = await stateOf(K.back); return x && x.state === 'review' ? x : null; }, 15000);
    await settle(800);
    c = await cardFacts();
    note(!!flipped && flipped.seals === 1, '5a the first change (a nudge) puts the piece back to "needs Approve" at once; its first seal stays', JSON.stringify({ pressed, flipped, before: fl0, after: await flags(K.back) }));
    note(c && c.state === 'review' && c.approve && !c.tick, '5b the card shows the Approve button again (no tick)', JSON.stringify(c));
    note(ops.slice(opsMark).includes('backInvalidate'), '5c the first back file is superseded (the backInvalidate request goes out)', ops.slice(opsMark).join(','));
    note(dialogs.length === 0, '5d no confirm dialog was asked', dialogs.join(' | '));
    await showList();
    const listAfter = await page.evaluate(rid => { const r = document.querySelector(`#egQueue .placementRow[data-rid="${rid}"]`); return { decided: r.classList.contains('isDecided') || r.dataset.decided === '1', s: [...r.querySelectorAll('[data-s]')].map(n => n.dataset.s), badge: document.querySelector('.egTab[data-tab="place"] b').textContent }; }, made.back.rid);
    note(!listAfter.decided && !listAfter.s.includes('approved') && listAfter.badge === '2', '5e its row is a row to decide again (the chip counts it)', JSON.stringify(listAfter));
    const a2 = await approveCard(K.back);
    note(a2 && a2.state === 'approved' && a2.seals === 2, '5f Approve again: approved with a SECOND brand new seal, the first stays', JSON.stringify(a2));
    note(ops.slice(opsMark).includes('backPut'), '5g the new back file is written (backPut)', ops.slice(opsMark).join(','));
    c = await cardFacts();
    note(c && c.tick && c.seals === 2, '5h the card shows both seals and the tick again', JSON.stringify(c));
    const at1 = (await jobOf(K.back, 'back')).at;
    note(at1 > at0, '5i approvedAt is the second approval\'s', at0 + ' -> ' + at1);
    await R.shot(w, 'NODECIDED-3-second-seal');

    // ═══ 6 · changing one piece never touches the others of its order ═══
    await nudge(bFront.key);
    const fl = await until(async () => { const x = await stateOf(bFront.key); return x && x.state === 'review' ? x : null; }, 15000);
    const bk = await stateOf(bBack.key);
    note(!!fl && fl.seals === 1 && bk.state === 'approved' && bk.seals === 1, '6a changing the Front of the faced piece flips only the Front; the Back keeps its approval and its one seal', JSON.stringify({ front: fl, back: bk }));
    await approveCard(bFront.key);
    const bj3 = await jobsOf(K.both);
    note(bj3.find(j => j.face === 'front').seals === 2 && bj3.find(j => j.face === 'back').seals === 1 && bj3.every(j => j.state === 'approved'), '6b Approve on the Front: its second seal; the Back still has one', JSON.stringify(bj3));
    const d1k = (await jobsOf(K.discs)).find(j => j.slot === 'D1').key;
    await nudge(d1k);
    await until(async () => { const x = await stateOf(d1k); return x && x.state === 'review' ? x : null; }, 15000);
    const dj2 = await jobsOf(K.discs);
    note(dj2.find(j => j.slot === 'D1').state === 'review' && dj2.find(j => j.slot === 'D1').seals === 1 && dj2.filter(j => j.slot !== 'D1').every(j => j.state === 'review' && j.seals === 0), '6c changing Disc 1 touches no other disc (Disc 2 / 3 as they were)', JSON.stringify(dj2));

    // ═══ 7 · a piece "Saved to sheet" is changed the same way, no confirm ═══
    await page.evaluate(k => { const j = Engrave.items().get(k); j.state = 'written'; Engrave.render(); }, K.back);
    await openCard(K.back);
    c = await cardFacts();
    note(c && c.state === 'written' && /Saved to sheet/.test(c.written) && c.tick, '7a a piece saved to a sheet shows the calm line "Saved to sheet · approving again replaces its file"', JSON.stringify(c));
    await page.evaluate(NUDGE);
    const wf = await until(async () => { const x = await stateOf(K.back); return x && x.state === 'review' ? x : null; }, 15000);
    note(!!wf && wf.seals === 2 && dialogs.length === 0, '7b the first change of a written piece also flips it to "needs Approve" (seals kept), without any confirm', JSON.stringify({ wf, dialogs }));

    // ═══ 8 · Back / Next walk every piece, approved ones too ═══
    await showList(); await settle(600);
    const order = await page.evaluate(() => [...document.querySelectorAll('#egQueue .egPlacementList .placementRow')].map(r => r.dataset.rid));
    const startKey = (await jobsOf(K.discs)).find(j => j.slot === 'D1').key;
    await openCard(startKey);
    const walk = [startKey];
    for (let i = 0; i < 8; i++) {
      await page.click(`${Q} [data-a=next]`); await settle(700);
      walk.push(await page.evaluate(Q => (document.querySelector(Q) || {}).dataset ? document.querySelector(Q).dataset.key : null, Q));
    }
    const all = new Set([...(await jobsOf(K.back)), ...(await jobsOf(K.both)), ...(await jobsOf(K.discs))].map(j => j.key));
    note([...all].every(k => walk.includes(k)) && walk[walk.length - 1] !== null, '8a Next reaches every piece of the list, approved ones included (6 jobs in 3 orders)', walk.map(k => String(k).replace(/^\d+_/, '')).join(' '));
    const uniq = [...new Set(walk)];
    note(walk.filter(k => k === startKey).length >= 2 || uniq.length === all.size, '8b and wraps round', walk.length + ' steps, ' + uniq.length + ' distinct of ' + all.size);

    // ═══ 9 · the old doors ═══
    const compat = await page.evaluate(k => { Engrave.restoreView({ tab: 'done', chosen: true, list: false, openDone: k }); Engrave.render(); const v = Engrave.view(); return { tab: v.tab, focus: v.focus, list: v.list }; }, bBack.key);
    await settle(700);
    note(compat.tab === 'place' && compat.focus === bBack.key && compat.list === false && (await cardFacts()).key === bBack.key, '9a restoreView({tab:"done", openDone}) opens that piece\'s card; view().tab is "place"', JSON.stringify(compat));
    const compat2 = await page.evaluate(() => { Engrave.restoreView({ tab: 'done', chosen: true }); Engrave.render(); const v = Engrave.view(); return { tab: v.tab, list: v.list, rows: document.querySelectorAll('#egQueue .egPlacementList .placementRow').length }; });
    note(compat2.tab === 'place' && compat2.list === true && compat2.rows >= 3, '9b restoreView({tab:"done"}) shows the one list', JSON.stringify(compat2));

    // ═══ 10 · phone width ═══
    await page.setViewportSize({ width: 400, height: 800 }); await settle(900);
    await showList(); await settle(500);
    const phoneList = await page.evaluate(() => ({ sw: document.documentElement.scrollWidth - innerWidth, listSW: (document.querySelector('#egQueue .egPlacementList') || { scrollWidth: 0, clientWidth: 0 }).scrollWidth - (document.querySelector('#egQueue .egPlacementList') || { clientWidth: 0 }).clientWidth }));
    await openCard(bBack.key); await settle(500);
    const phoneCard = await cardFacts();
    note(phoneList.sw <= 1 && phoneList.listSW <= 1 && phoneCard && phoneCard.scrollW <= phoneCard.innerW + 1, '10 at 400 px the list and an approved piece\'s card do not scroll sideways', JSON.stringify({ phoneList, scrollW: phoneCard && phoneCard.scrollW }));
    await R.shot(w, 'NODECIDED-4-phone');

    note(w.errors.length === 0, '11 no page errors', w.errors.join(' | ').slice(0, 300));
    note(outside.length === 0, '12 nothing left the loopback (no Etsy, no AI)', outside.slice(0, 3).join(' '));
  } catch (e) { fails.push('crashed: ' + (e && e.stack || e)); console.log('  FAIL crashed: ' + (e && e.stack || e)); }
  finally { await w.close(); }
  if (fails.length) { console.log('\nnodecided: ' + fails.length + ' failed: ' + fails.join(' | ')); process.exit(1); }
  console.log('\nnodecided OK');
})().catch(e => { console.error(e); process.exit(1); });
