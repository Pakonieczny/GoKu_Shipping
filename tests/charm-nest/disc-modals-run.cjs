module.exports = async function run({ page, check, probe, srv, D, E, S1, Q3, pool, keyOf, dKeys, eKeys, sKeys, qKeys, errors }) {
  const { shot, OV, TAB, WIN, waitCard, closeWin, area } = probe;
  const tabText = (a, i) => (a.tabs[i] || {}).text || '';
  // ── 1 · order window, Overview ──
  await page.evaluate(k => OrderWin.open(k), keyOf(D));
  await page.waitForFunction(() => OrderWin.isOpen());
  check(await waitCard(OV, 'approve'), 'Overview of the 2-disc order: a card (To approve)');
  await page.waitForTimeout(500);
  let a = await area(OV);
  check(a.cards === 1 && a.switchShown && a.tabs.length === 2 && a.tabs[0].on && !a.tabs[1].on, 'ONE card with the Disc 1 | Disc 2 switch, Disc 1 shown: ' + JSON.stringify(a.tabs));
  check(/DISC 1 of 2/i.test(tabText(a, 0)) && /DISC 2 of 2/i.test(tabText(a, 1)) && /J/.test(tabText(a, 0)) && /Q/.test(tabText(a, 1)) && /Typewriter/.test(tabText(a, 0)) && /Typewriter/.test(tabText(a, 1)), 'each disc button carries its tag, its words and its font by name: ' + tabText(a, 0) + ' | ' + tabText(a, 1));
  check(a.words === 'J' && a.state === 'approve' && a.nav.length === 2, 'the card is Disc 1\'s: words J, Back and Next present: ' + JSON.stringify([a.words, a.state, a.nav]));
  await shot('disc-overview-disc1.png', '#orderWin');
  // Disc 2 by the switch
  await page.click(`${OV} .egEarTab[data-ear="${dKeys[1]}"]`);
  await page.waitForFunction(sel => { const e = document.querySelector(sel + ' .words'); return e && e.textContent.trim() === 'Q'; }, OV, { timeout: 5000 }).catch(() => {});
  a = await area(OV);
  check(a.words === 'Q' && !a.tabs[0].on && a.tabs[1].on && a.cards === 1, 'a press on Disc 2: its own card, words Q: ' + JSON.stringify([a.words, a.tabs.map(t => t.on)]));
  // Next wraps to Disc 1, Back goes to Disc 1 as well (two discs)
  await page.click(`${OV} [data-a=discNext]`);
  await page.waitForFunction(sel => (document.querySelector(sel + ' .words') || {}).textContent === 'J', OV, { timeout: 5000 }).catch(() => {});
  a = await area(OV); check(a.words === 'J' && a.tabs[0].on, 'Next from Disc 2 goes round to Disc 1');
  await page.click(`${OV} [data-a=discPrev]`);
  await page.waitForFunction(sel => (document.querySelector(sel + ' .words') || {}).textContent === 'Q', OV, { timeout: 5000 }).catch(() => {});
  a = await area(OV); check(a.words === 'Q' && a.tabs[1].on, 'Back from Disc 1 goes round to Disc 2');
  // Fix in Engraving asks for the disc shown
  await page.click(OV + ' [data-e=engrave]'); await page.waitForFunction(() => window.__links.length >= 1);
  let l = await page.evaluate(() => __links[__links.length - 1]);
  check(l && String(l.rid) === D.rid && l.key === dKeys[1], 'Fix in Engraving asks EngraveLink for Disc 2\'s job: ' + JSON.stringify(l));
  // approve Disc 2 only: one approval, one seal on its button; Disc 1 is still to approve
  await page.click(OV + ' [data-e=approve]');
  check(await waitCard(OV, 'approved'), 'Disc 2 approved here reads approved');
  await page.waitForTimeout(500);
  a = await area(OV);
  check(a.seals === 1 && a.button && a.button.disabled && (await page.evaluate(() => __approves.map(x => x.key))).join() === dKeys[1], 'one approval (Disc 2), one seal on its disabled button: ' + JSON.stringify([a.seals, a.button]));
  check(/Approved/.test(tabText(a, 1)) && /Placement to check/.test(tabText(a, 0)), 'the switch says where each disc stands: ' + tabText(a, 0) + ' | ' + tabText(a, 1));
  await page.click(`${OV} .egEarTab[data-ear="${dKeys[0]}"]`);
  await page.waitForFunction(sel => { const e = document.querySelector(sel + ' .swEng'); return e && e.dataset.state === 'approve'; }, OV, { timeout: 5000 }).catch(() => {});
  a = await area(OV);
  check(a.state === 'approve' && a.words === 'J' && a.seals === 0 && a.button && !a.button.disabled, 'Disc 1 is still to approve, no seal, enabled button (Disc 2\'s approval did not reach it): ' + JSON.stringify([a.state, a.seals, a.button]));
  // the Engraving cell: every disc's seal, disc by disc
  const cell = await page.evaluate(() => { const m = [...document.querySelectorAll('#owMeta .m')].find(x => x.querySelector('i') && x.querySelector('i').textContent === 'Engraving'); return m ? { text: m.querySelector('span').textContent.replace(/\s+/g, ' ').trim(), seals: m.querySelectorAll('.seal').length } : null; });
  check(cell && /Disc 1 · J/.test(cell.text) && /Disc 2 · Q/.test(cell.text) && cell.seals >= 1, 'the Engraving cell says it disc by disc, with the approved disc\'s seal: ' + JSON.stringify(cell));
  await shot('disc-overview-disc1-after.png', '#orderWin');
  await closeWin();

  // ── 2 · order window, Sheet tab ──
  const sheetTab = async (o, sel) => { if (process.env.DM_NOCYCLE) await page.evaluate(() => { OrderEngraving.cycleOf = () => null; });
    await page.evaluate(k => OrderWin.open(k, { view: 'sheet' }), keyOf(o)); await page.waitForFunction(() => OrderWin.view() === 'sheet' && OrderWin._sheet() && document.querySelector('#owSheetPanel .owPieces'), null, { timeout: 20000 }); };
  const wordsIs = (sel, w) => page.waitForFunction(({ sel, w }) => { const e = document.querySelector(sel + ' .swEng .words'); return e && e.textContent.trim() === w; }, { sel, w }, { timeout: 8000 }).then(() => true, () => false);
  await sheetTab(D);
  check(await waitCard(TAB, 'approve', 12000), 'Sheet tab: a card for the disc in focus');
  a = await area(TAB);
  check(a.cards === 1 && a.switchShown && a.tabs.length === 2 && a.nav.length === 2, 'Sheet tab: ONE card with the Disc 1 | Disc 2 switch and Back / Next: ' + JSON.stringify(a.tabs));
  const first = a.words;
  check(/^(J|Q)$/.test(first), 'Sheet tab: it shows one disc\'s words (' + first + ')');
  await shot('disc-sheettab-disc-a.png', '#orderWin');
  const other = first === 'J' ? 'Q' : 'J', otherKey = first === 'J' ? dKeys[1] : dKeys[0];
  await page.click(`${TAB} .egEarTab[data-ear="${otherKey}"]`);
  check(await wordsIs(TAB, other), 'Sheet tab: a press on the other disc shows its words (' + other + ')');
  a = await area(TAB); check(a.cards === 1 && a.tabs.filter(t => t.on).length === 1, 'Sheet tab: one card, one disc on at a time: ' + JSON.stringify(a.tabs.map(t => t.on)));
  await page.click(`${TAB} [data-a=discNext]`);
  check(await wordsIs(TAB, first), 'Sheet tab: Next goes round to the first disc again');
  await page.click(`${TAB} [data-a=discPrev]`);
  check(await wordsIs(TAB, other), 'Sheet tab: Back goes round again');
  await shot('disc-sheettab-disc-b.png', '#orderWin');
  await closeWin();
  // three discs, the third on another sheet
  await sheetTab(E);
  check(await waitCard(TAB, null, 12000), 'Sheet tab of the three-disc order: a card');
  a = await area(TAB);
  check(a.tabs.length === 3 && /DISC 1 of 3/i.test(tabText(a, 0)) && /DISC 3 of 3/i.test(tabText(a, 2)), 'Sheet tab: Disc 1 | Disc 2 | Disc 3: ' + a.tabs.map(t => t.text).join(' | '));
  await page.click(`${TAB} .egEarTab[data-ear="${eKeys[2]}"]`);
  check(await wordsIs(TAB, 'B'), 'Sheet tab: Disc 3 (on the other sheet) shows its own words (B)');
  a = await area(TAB);
  check(a.state === 'words' && a.tabs[2].on && a.seals === 0, 'Sheet tab: Disc 3 reads "words to settle", its button on, and no seal (Disc 2\'s approval is not Disc 3\'s): ' + JSON.stringify([a.state, a.seals, a.tabs.map(t => t.on)]));
  await shot('disc-sheettab-disc3.png', '#orderWin');
  await page.click(`${TAB} .egEarTab[data-ear="${eKeys[1]}"]`);
  check(await wordsIs(TAB, 'A'), 'Sheet tab: Disc 2 (approved) shows its words (A)');
  a = await area(TAB);
  check(a.state === 'approved' && a.seals === 1, 'Sheet tab: Disc 2 reads approved with its one seal: ' + JSON.stringify([a.state, a.seals]));
  await closeWin();

  // ── 3 · sheet window, piece view ──
  const openWin = async (sh, pid) => {
    await page.evaluate(({ sh, ps }) => { SheetWin.open(sh, { select: ps }); }, { sh, ps: pid }).catch(() => {});
    await page.waitForFunction(() => SheetWin.isOpen && SheetWin.isOpen(), null, { timeout: 15000 }).catch(() => {});
  };
  await openWin('sheet-dm-1', pool(D, 1));
  check(await waitCard(WIN, null, 15000), 'Sheet window: a card for the piece selected');
  a = await area(WIN);
  check(a.cards === 1 && a.switchShown && a.tabs.length === 2 && a.nav.length === 2 && a.words === 'J', 'Sheet window: ONE card with the Disc 1 | Disc 2 switch, Disc 1 (J) shown: ' + JSON.stringify([a.tabs.length, a.words, a.nav.length]));
  check(a.seals === 0 && a.state === 'approve', 'Sheet window: Disc 1 has none of Disc 2\'s seals: ' + JSON.stringify([a.state, a.seals]));
  check(/DISC 1 of 2/i.test(tabText(a, 0)) && /Typewriter/.test(tabText(a, 1)), 'Sheet window: each disc button carries its tag, words and font: ' + tabText(a, 0) + ' | ' + tabText(a, 1));
  await shot('disc-sheetwin-disc1.png', 'dialog.sheetWin[open]');
  await page.click(`${WIN} .egEarTab[data-ear="${dKeys[1]}"]`);
  check(await wordsIs(WIN, 'Q'), 'Sheet window: a press on Disc 2 shows its words (Q) and selects its charm');
  const sel = await page.evaluate(() => { const x = SheetWin._W.sel; return x && (x.poolId || x.id); });
  check(sel === pool(D, 2), 'Sheet window: the charm selected is Disc 2\'s: ' + sel);
  await page.click(`${WIN} [data-a=discNext]`);
  check(await wordsIs(WIN, 'J'), 'Sheet window: Next goes round to Disc 1');
  await page.click(`${WIN} [data-a=discPrev]`);
  check(await wordsIs(WIN, 'Q'), 'Sheet window: Back goes round to Disc 2');
  await shot('disc-sheetwin-disc2.png', 'dialog.sheetWin[open]');
  await page.evaluate(() => { try { SheetWin.close && SheetWin.close(); } catch (_) {} const d = document.querySelector('dialog.sheetWin[open]'); if (d) d.close(); });

  // a single charm keeps its card, with no switch
  await page.evaluate(k => OrderWin.open(k), keyOf(S1));
  await page.waitForFunction(() => OrderWin.isOpen());
  check(await waitCard(OV, 'review') || await waitCard(OV, 'approve'), 'a single charm: its card');
  a = await area(OV);
  check(a.cards === 1 && !a.switchShown && a.nav.length === 0 && a.words === 'GOOD // LUCK', 'no switch, no Back/Next on a single: ' + JSON.stringify([a.switchShown, a.nav, a.words]));
  await closeWin();
  // a plain quantity-3 line (three of one charm): one card as ever, no disc switch
  await page.evaluate(k => OrderWin.open(k), keyOf(Q3));
  await page.waitForFunction(() => OrderWin.isOpen());
  check(await waitCard(OV, 'review') || await waitCard(OV, 'approve'), 'a plain quantity-3 line: its card');
  a = await area(OV);
  check(!a.switchShown && a.nav.length === 0 && a.words === 'LOVE' && qKeys.length === 1, 'no disc switch, no Back/Next on a plain quantity-3 line: ' + JSON.stringify([a.cards, a.switchShown, a.nav, a.words, qKeys]));
  const qcell = await page.evaluate(() => { const m = [...document.querySelectorAll('#owMeta .m')].find(x => x.querySelector('i') && x.querySelector('i').textContent === 'Engraving'); return m ? m.querySelector('span').textContent.replace(/\s+/g, ' ').trim() : null; });
  check(!/Disc/.test(qcell || ''), 'its Engraving cell (if shown) says no disc: ' + qcell);
  await closeWin();
  // ── 5 · text: the global search, the order's timeline lines, the set manifest name the disc ──
  // (the search says "Needs a decision" for an order with a Review item; the engraving line is for the order whose discs are only waiting to be fitted: both discs are being fitted here, no decision open, none approved)
  await page.evaluate(k => {
    const r = B.orders.byKey.get(k); r.problems = [];
    const items = B.review.items; for (let i = items.length - 1; i >= 0; i--) { const it = items[i]; if ((it.rows || [it.row]).some(x => x && x.order && String(x.order.receiptId) === String(r.order.receiptId))) items.splice(i, 1); }
    for (const j of Engrave.jobsOf(r)) { j.state = 'fitting'; j.approvedAt = null; } OrderSearch.rebuild();
  }, keyOf(D));
  await page.keyboard.press('/');
  await page.waitForFunction(() => OrderSearch.isOpen() && document.activeElement && document.activeElement.id === 'cnsQ', null, { timeout: 8000 });
  await page.keyboard.type(D.rid);
  await page.waitForSelector(`#cnsList .cnsCard[data-rid="${D.rid}"]`, { timeout: 10000 });
  const card = await page.evaluate(rid => document.querySelector(`#cnsList .cnsCard[data-rid="${rid}"]`).textContent.replace(/\s+/g, ' '), D.rid);
  check(/Back engraving · Disc 1 of 2 · fitting/.test(card), 'search: the result says which disc is waiting: ' + (card.match(/Back engraving[^·]*·[^·]*·[^·]*/) || [card.slice(0, 120)])[0]);
  const card2 = await page.evaluate(() => { document.getElementById('cnsQ').value = ''; document.getElementById('cnsQ').dispatchEvent(new Event('input', { bubbles: true })); return 1; }); await page.keyboard.press('Escape');
  await page.waitForFunction(() => !OrderSearch.isOpen(), null, { timeout: 5000 }).catch(() => {});
  await page.evaluate(() => { window.__tl = []; const o = CNTimeline.line; CNTimeline.line = function (row, type, a) { __tl.push({ type, text: a && a.text }); return o.apply(this, arguments); }; });
  await page.evaluate(k => Engrave.decideWords(Engrave.jobsOf(B.orders.byKey.get(k)).find(j => j.slot === 'D3'), { none: true, by: 'Test Operator' }), keyOf(E));
  await page.evaluate(k => Engrave.decideWords(Engrave.jobOf(B.orders.byKey.get(k)), { none: true, by: 'Test Operator' }), keyOf(S1));
  const tl = await page.evaluate(() => __tl.filter(x => x.type === 'engraveChanged').map(x => x.text));
  check(tl.includes('No engraving · Disc 3 of 3 — decided by Test Operator'), 'timeline: the line names the disc ("Disc 3 of 3"): ' + JSON.stringify(tl));
  check(tl.includes('No engraving — decided by Test Operator'), 'timeline: a single charm\'s line reads exactly as before: ' + JSON.stringify(tl));
  const dw = await page.evaluate(() => ['D1', 'D2', 'L', 'R', '', undefined].map(sl => CharmNestPairLabels.discWord({ slot: sl })).concat([CharmNestPairLabels.backWord({ slot: 'D2' }), CharmNestPairLabels.backWord({ side: 'L', slot: 'L' })]));
  check(JSON.stringify(dw) === JSON.stringify([' · Disc 1', ' · Disc 2', '', '', '', '', '', ' · Left']), 'manifest: the back of a disc says " · Disc 2", an ear or a single says what it said: ' + JSON.stringify(dw));
  check(errors.length === 0, 'no page errors: ' + errors.join(' | '));
};
