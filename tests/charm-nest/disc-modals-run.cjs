module.exports = async function run({ page, check, probe, srv, D, E, S1, Q3, pool, keyOf, dKeys, eKeys, sKeys, qKeys, errors }) {
  const { shot, OV, TAB, WIN, waitCard, waitEngCard, cards, closeWin, area } = probe;
  const tabText = (a, i) => (a.tabs[i] || {}).text || '';
  let a;
  // ── 1 · order window, Overview (ENGCARD, 12 Oct 2026: a card for EACH disc, side by side; no Disc switch, no Back / Next) ──
  await page.evaluate(k => OrderWin.open(k), keyOf(D));
  await page.waitForFunction(() => OrderWin.isOpen());
  check(await waitEngCard(dKeys[0], 'approve') && await waitEngCard(dKeys[1], 'approve'), 'Overview of the 2-disc order: a card for each disc (To approve)');
  await page.waitForTimeout(500);
  let cs = await cards(OV);
  check(cs.length === 2 && cs[0].key === dKeys[0] && cs[1].key === dKeys[1] && !cs[0].switch, 'TWO cards side by side, Disc 1 then Disc 2, and no Disc switch or Back / Next in the Overview: ' + JSON.stringify(cs.map(c => [c.key.slice(-3), c.name])));
  check(/Disc 1/.test(cs[0].name) && /Disc 2/.test(cs[1].name) && cs[0].words === 'J' && cs[1].words === 'Q' && cs.every(c => c.state === 'approve'), 'each card names its disc and has its own words (J, Q): ' + JSON.stringify(cs.map(c => [c.name, c.words, c.state])));
  await shot('disc-overview-disc1.png', '#orderWin');
  // a press on Disc 2's Back picture asks Engrave for Disc 2 (the card is not left)
  await page.click(`${OV} .engCard[data-piece="${dKeys[1]}"] .engFace[data-face=back] .engThumb`); await page.waitForFunction(() => window.__links.length >= 1);
  let l = await page.evaluate(() => __links[__links.length - 1]);
  check(l && String(l.rid) === D.rid && l.key === keyOf(D) && l.piece === dKeys[1] && l.poolId === pool(D, 2) && l.face === 'back', 'the Back picture of Disc 2 asks EngraveLink for the line, Disc 2\'s piece, its charm and the back: ' + JSON.stringify(l));
  // approve Disc 2 only: one approval, one seal where its button was; Disc 1 is still to approve
  await page.click(`${OV} .engCard[data-piece="${dKeys[1]}"] [data-e=approve]`);
  check(await waitEngCard(dKeys[1], 'approved', 15000), 'Disc 2 approved here reads approved');
  await page.waitForTimeout(500);
  cs = await cards(OV);
  check(cs[1].seals === 1 && !cs[1].button && (await page.evaluate(() => __approves.map(x => x.key))).join() === dKeys[1], 'one approval (Disc 2), one seal where its button was, no button: ' + JSON.stringify([cs[1].seals, cs[1].button]));
  check(cs[0].state === 'approve' && cs[0].words === 'J' && cs[0].seals === 0 && cs[0].button && !cs[0].button.disabled, 'Disc 1 is still to approve, no seal, enabled button (Disc 2\'s approval did not reach it): ' + JSON.stringify([cs[0].state, cs[0].seals, cs[0].button]));
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
  check(await waitEngCard(sKeys[0], 'review') || await waitEngCard(sKeys[0], 'approve'), 'a single charm: its card');
  cs = await cards(OV);
  check(cs.length === 1 && !cs[0].switch && cs[0].words.replace(/\n/g, ' // ') === 'GOOD // LUCK', 'one card, no switch, no Back/Next on a single: ' + JSON.stringify([cs.length, cs[0] && cs[0].switch, cs[0] && cs[0].words]));
  await closeWin();
  // a plain quantity-3 line (three of one charm): one card as ever, no disc switch
  await page.evaluate(k => OrderWin.open(k), keyOf(Q3));
  await page.waitForFunction(() => OrderWin.isOpen());
  check(await waitEngCard(qKeys[0], 'review') || await waitEngCard(qKeys[0], 'approve'), 'a plain quantity-3 line: its card');
  cs = await cards(OV);
  check(cs.length === 1 && !cs[0].switch && cs[0].words === 'LOVE' && qKeys.length === 1, 'one card, no disc switch, no Back/Next on a plain quantity-3 line: ' + JSON.stringify([cs.length, cs[0] && cs[0].switch, cs[0] && cs[0].words, qKeys]));
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
  // ── 6 · the count people read (DISCREAD 3): a counted line says 2 pieces / 3 pieces in the header, the Quantity cell, the "Its pieces" list and the order list row; a single and a plain quantity-3 line keep their words ──
  const says = async o => { await page.evaluate(k => OrderWin.open(k), keyOf(o)); await page.waitForFunction(() => OrderWin.isOpen());
    await page.waitForFunction(k => { const t = document.getElementById('owPcSum'); return document.getElementById('owSub') && /piece/.test(document.getElementById('owSub').textContent) && t && !t.hidden && /Its piece/.test(t.textContent); }, keyOf(o), { timeout: 8000 }).catch(() => {});
    const r = await page.evaluate(() => { const t = e => e ? e.textContent.replace(/\s+/g, ' ').trim() : '', q = [...document.querySelectorAll('#owMeta .m')].find(m => m.querySelector('i') && m.querySelector('i').textContent === 'Quantity');
      return { head: t(document.getElementById('owSub')), qty: q ? t(q.querySelector('span')) : null, pieces: t(document.getElementById('owPcSum')) }; }); await closeWin(); return r; };
  const sd = await says(D), se = await says(E), ss = await says(S1), sq = await says(Q3);
  check(/ · 2 pieces · /.test(sd.head) && sd.qty === '1 · 2 discs' && /^Its pieces/.test(sd.pieces) && /· 2 pieces/.test(sd.pieces), 'the 2-disc order: header "2 pieces", "Quantity 1 · 2 discs", "Its pieces" with "2 pieces": ' + JSON.stringify(sd));
  check(/ · 3 pieces · /.test(se.head) && se.qty === '1 · 3 discs' && /^Its pieces/.test(se.pieces) && /· 3 pieces/.test(se.pieces), 'the 3-disc order: header "3 pieces", "Quantity 1 · 3 discs", "Its pieces" with "3 pieces": ' + JSON.stringify(se));
  check(/ · 1 piece · /.test(ss.head) && ss.qty === '1' && /^Its piece(?!s)/.test(ss.pieces) && !/pieces|×/.test(ss.pieces), 'a single charm keeps "1 piece", "Quantity 1", "Its piece": ' + JSON.stringify(ss));
  check(/ · 3 pieces · /.test(sq.head) && sq.qty === '3' && /^Its piece(?!s)/.test(sq.pieces) && /· ×3/.test(sq.pieces), 'a plain quantity-3 line keeps "3 pieces", "Quantity 3", "Its piece" with "×3": ' + JSON.stringify(sq));
  const facts = await page.evaluate(ks => ks.map(k => { const e = document.querySelector(`#ordItems [data-key="${k}"] .rowFacts`); return e ? e.textContent.replace(/\s+/g, ' ').trim().replace(/ · Ship by.*$/, '') : null; }), [keyOf(D), keyOf(E), keyOf(S1), keyOf(Q3)]);
  check(JSON.stringify(facts) === JSON.stringify(['Qty 1 · 2 discs', 'Qty 1 · 3 discs', 'Qty 1', 'Qty 3']), 'the order list rows: counted lines say their discs, a single and a plain quantity-3 line say what they said: ' + JSON.stringify(facts));
  check(errors.length === 0, 'no page errors: ' + errors.join(' | '));
};
