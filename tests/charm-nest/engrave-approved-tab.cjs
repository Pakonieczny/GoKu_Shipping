// APPTAB (Paul, 19 Oct 2026: "I'm not seeing the Tab for Approved engravings/Monograms inside the Engraving tab"): the Engraving tab has a second chip, "Approved", beside "Placements".
// It is a VIEW of the ONE list (NODECIDED: there is no second list, no second pane): the same rows from the same renderer, kept to the rows that hold an APPROVED piece (an approved or "Saved to sheet"
// engraving, an approved monogram). A piece cut plain ("No engraving") is decided but not approved, so a row of only those is in Placements alone.
//
// What it proves, offline: the REAL page in Chromium over the repo's fake site (the _engraveRealWorld.cjs world, the monogram alphabets); made-up buyers only; no Etsy, no paid call.
//   1  two chips, Placements and Approved; Placements is on, its number is what it always was (the rows still to settle); Approved is empty and says so (with a way back)
//   2  an approval moves a piece into the Approved number at once (an engraving, a monogram), and out of the Placements number; a piece cut plain moves out of Placements and is NOT counted in Approved
//   3  the Approved tab lists exactly the approved engravings and monograms: the rows on the screen are the number on the chip, nothing pending, nothing cut plain; Placements still lists every row, unchanged
//   4  the same controls work on it: newest approval first by default (and the sort button flips it), the date menu, the search; the search kept in the box follows the chip both ways and the chips count what they show
//   5  the detail card, Back / Next, Close and Esc work from the Approved tab and stay on it; approving inside it (a monogram, then the bar of its order) raises the Approved number live
//   6  saved views: "done" and "approved" land on the Approved tab (a card with openDone); "mono", "words" and anything else on Placements
//   7  no new traffic: switching chips and typing in the search make no fetch / XHR / socket request; no console or page errors; the phone width has no sideways overflow
//   PW_DIR=/opt/node-tools/node_modules node tests/charm-nest/engrave-approved-tab.cjs [playwright-core dir]      (SHOTS=<dir>)
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const SHOTS = process.env.SHOTS || '/tmp/apptab/shots';
const fails = []; let nCheck = 0;
const check = (ok, msg) => { nCheck++; if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  try { require(path.join(pwDir, 'playwright-core')); } catch (_) { console.log('engrave-approved-tab: no playwright-core, nothing run'); finish(); return; }
  process.env.PW_DIR = pwDir;
  fs.mkdirSync(SHOTS, { recursive: true });
  const R = require('./_engraveRealWorld.cjs'), Base = require('./_orderFactsWorld.cjs'), A = require('./_monogramAlphabets.cjs');
  const V = (name, value) => ({ name, value });
  function orderOf(fx, now) {
    const at = (d, h, m) => { const t = new Date(now); t.setDate(t.getDate() + d); t.setHours(h, m, 0, 0); return Math.floor(t.getTime() / 1000); };
    const shipBy = at(fx.ship, 17, 0), line = (x, i) => ({ transactionId: fx.rid + (i + 1), listingId: x.listing, sku: x.sku, title: x.title, quantity: x.qty || 1, expectedShipDate: shipBy, variations: x.variations || [], metalKey: x.metalKey || 'gold', metalLabel: x.metalLabel, personalization: x.personalization || [] });
    return { receiptId: fx.rid, orderNumber: fx.rid, createTs: at(fx.placed.day, fx.placed.h, fx.placed.m), updateTs: at(0, 12, 0), shipBy, buyer: { name: fx.buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: (fx.lines || [fx]).map(line) };
  }
  Base.orderOf = orderOf;
  const MONO = { listing: '997905351', sku: 'MONOGRAM_5351', title: 'Monogram Necklace Personalized Jewelry Initials Pendant', variations: [V('Metal choice', 'Gold'), V('Chain length', '16 inches')], personalization: ['APV'], metalKey: 'gold' };
  const MONO2 = { listing: '606439317', sku: 'MONOGRAM_9317', title: 'Monogram Necklace Dainty Monogram Necklace', variations: [V('Metal choice', 'Silver')], personalization: ['O S R'], metalKey: 'silver' };
  const MONO3 = { listing: '997905351', sku: 'MONOGRAM_5351', title: 'Monogram Necklace Personalized Jewelry Initials Pendant', variations: [V('Metal choice', 'Gold')], personalization: ['J.Q.X'], metalKey: 'gold' };
  const BAR = { listing: '1890000101', sku: 'COMET_9427', title: 'Comet Necklace, Celestial Charm Necklace in 14k Gold Fill', variations: [V('Metal choice', 'Gold'), V('Font Choice', 'Stylish')], personalization: ['Name One'] };
  R.ORDERS.mono = { rid: '4200683017', buyer: 'Name One', placed: { day: 0, h: 9, m: 10 }, ship: 5, lines: [MONO] }; R.EXPECT_JOBS.mono = 0;          // (stays to approve)
  R.ORDERS.mono2 = { rid: '4202783520', buyer: 'Name Two', placed: { day: 0, h: 9, m: 15 }, ship: 5, lines: [MONO2] }; R.EXPECT_JOBS.mono2 = 0;      // (approved: the monogram)
  R.ORDERS.mixed = { rid: '4290000131', buyer: 'Name Three', placed: { day: 0, h: 9, m: 20 }, ship: 5, lines: [MONO3, BAR] }; R.EXPECT_JOBS.mixed = 1;   // (a monogram and a bar: approved inside the Approved tab)
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const w = await R.world({ viewport: { width: 1440, height: 900 }, shots: SHOTS });
  if (!w) { console.log('engrave-approved-tab: no playwright-core, nothing run'); finish(); return; }
  const page = w.page;
  let counting = false; const traffic = [];
  page.on('request', rq => { if (counting && ['fetch', 'xhr', 'websocket', 'eventsource'].includes(rq.resourceType())) { let op = ''; try { op = String((JSON.parse(rq.postData() || '{}') || {}).op || ''); } catch (_) { /* not JSON */ } if (!/^(getOrderPieces|cancelList)$/.test(op)) traffic.push(rq.method() + ' ' + rq.url().split('/').pop() + (op ? ' op=' + op : '')); } });   // (the PlacementFeed's own tick, getOrderPieces + cancelList every few seconds while rows are on screen, runs with or without a press: not counted)
  const settle = ms => R.settle(page, ms || 400);
  const shot = async name => { try { await page.evaluate(() => document.querySelectorAll('.cnNbX, .toast .x').forEach(b => { try { b.click(); } catch (_) {} })); await sleep(350); await page.screenshot({ path: path.join(SHOTS, name + '.png') }); } catch (e) { console.log('   (no screenshot ' + name + ')'); } };
  try {
    // ═══ the world: the alphabets (the monograms), the orders ═══
    A.seed(w.srv.st, await A.make());
    for (let i = 0; i < 8; i++) { const want = [...w.srv.st.docs.keys()].filter(k => k.startsWith('Charm_Master_Index/')).length, have = await page.evaluate(async () => { await Master.load(true); return B.master.entries.size; }); if (have >= want) break; await sleep(400); }
    // (this test is about the Engrave tab, not the monogram engine's rules: every word is staged as meeting them, so no card is "Needs a person" and nothing asks the AI)
    await page.evaluate(() => { if (window.__wrapped) return; window.__wrapped = true; const eb = CharmNestMonogram.build; CharmNestMonogram.build = function () { const r = eb.apply(this, arguments); if (r && r.ok && r.report && r.report.metrics && Array.isArray(r.report.metrics.violations)) r.report.metrics = Object.assign({}, r.report.metrics, { violations: [], ok: true }); return r; }; });
    await R.build(w, ['mono', 'mono2', 'mixed', 'back', 'refAnna', 'refMarck']);
    const rid = n => R.ORDERS[n].rid;
    const lines = await page.evaluate(() => Orders.rows().map(r => ({ key: r.key, rid: r.order.receiptId, mono: !!(r.spec && r.spec.special && r.spec.special.monogram) })));
    const monoKey = n => lines.find(l => l.rid === rid(n) && l.mono).key;
    const jobKey = n => page.evaluate(r => [...Engrave.items().values()].filter(j => String(j.row.order.receiptId) === r).map(j => j.key)[0], rid(n));
    const K = { mono: monoKey('mono'), mono2: monoKey('mono2'), monoMixed: monoKey('mixed'), barMixed: await jobKey('mixed'), back: await jobKey('back'), anna: await jobKey('refAnna'), marck: await jobKey('refMarck') };
    check(Object.values(K).every(Boolean) && new Set(Object.values(K)).size === 7, 'the world: 3 monogram lines (APV, OSR, JQX) and 4 engraved lines (a bar of the mixed order, back, refAnna, refMarck): 7 pieces');

    // ── helpers on the page ──
    const chips = () => page.evaluate(() => [...document.querySelectorAll('#engraveView .ordBar .egTab[data-tab]')].map(b => ({ tab: b.dataset.tab, label: b.firstChild.textContent.trim(), on: b.classList.contains('on'), pressed: b.getAttribute('aria-pressed'), n: +((b.querySelector('b') || {}).textContent || 0) })));
    const chip = async t => (await chips()).find(c => c.tab === t) || { n: -1, on: false };
    const rows = () => page.evaluate(() => [...document.querySelectorAll('#egQueue .egPlacementList .placementRow')].map(r => ({ open: r.dataset.open, rid: r.dataset.rid, kind: r.dataset.kind || 'engraved', decided: r.dataset.decided === '1', text: r.innerText.replace(/\s+/g, ' ').trim() })));
    const keysOf = async () => (await rows()).map(r => r.open);
    const stateOfKey = key => page.evaluate(k => { const j = Engrave.items().get(k) || Engrave.monoItems().find(m => m.key === k); return j ? j.state : null; }, key);
    const view = () => page.evaluate(() => { const v = Engrave.view(); return { tab: v.tab, list: v.list, focus: v.focus, q: v.q }; });
    const cardKey = () => page.evaluate(() => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); return c ? { key: c.dataset.key, state: c.dataset.state, mono: !!c.dataset.mono } : null; });
    const none = () => page.evaluate(() => { const n = document.querySelector('#egQueue .egNone, #engraveView .egNone'); return n ? n.innerText.replace(/\s+/g, ' ').trim() : ''; });
    const until = async (fn, what, ms = 60000) => { const t0 = Date.now(); let v; for (;;) { v = await fn(); if (v) return v; if (Date.now() - t0 > ms) { console.log('   (waited for ' + what + ')'); return v; } await sleep(150); } };
    const pressChip = async t => { await page.click(`#engraveView .ordBar .egTab[data-tab="${t}"]`); await until(async () => (await chip(t)).on, 'chip ' + t + ' on', 8000); await settle(500); };
    const openTab = tab => page.evaluate(tab => { CN.setMode('engrave'); Engrave.restoreView({ tab, list: true, chosen: true, focus: null, pin: null, openDone: null, q: '' }); Engrave.render(); }, tab);
    const itemOf = k => `(Engrave.items().get(${JSON.stringify(k)}) || Engrave.monoItems().find(m => m.key === ${JSON.stringify(k)}))`;
    const atOf = keys => page.evaluate(ks => ks.map(k => CNListActivity.at(Engrave.items().get(k) || Engrave.monoItems().find(m => m.key === k))), keys);   // (the key the existing Newest / Oldest control sorts by: the latest activity of the piece, an approval included)
    const sortedBy = (xs, dir) => xs.every((x, i) => i === 0 || (dir === 'desc' ? xs[i - 1] >= x : xs[i - 1] <= x));
    const sameSet = (x, y) => JSON.stringify([...x].sort()) === JSON.stringify([...y].sort());
    const search = async q => { await page.fill('#egQ', q); await sleep(450); await settle(500); };
    const widget = rid0 => page.evaluate(r => { const x = (window.CharmNestMonogramUI.widgets() || []).find(w => w.state().rid === r); return x ? { state: x.state().state, letters: x.state().letters.join('') } : null; }, rid0);
    const approveMono = async (key, rid0) => {   // the widget's own Approve, on the card that is in front
      await until(async () => { const s = await widget(rid0); return s && s.state === 'ready'; }, 'the monogram ready');
      await page.click('#egQueue .rvItem[data-mono] [data-mw-approve]');
      return until(() => page.evaluate(k => Engrave.monogramUI().status(Orders.rows().find(r => r.key === k)).approved, key), 'the monogram approved');
    };
    const approveJob = async key => {
      await page.waitForSelector(`#egQueue .rvItem[data-key="${key}"] [data-a=approve]`, { timeout: 30000 }); await settle(500);
      await page.click(`#egQueue .rvItem[data-key="${key}"] [data-a=approve]`);
      return until(async () => ['approved', 'written'].includes(await stateOfKey(key)), 'the bar approved');
    };
    const openCard = async (key, tab) => { await page.evaluate(([k, t]) => { CN.setMode('engrave'); Engrave.restoreView({ tab: t, list: false, chosen: true, focus: k }); Engrave.render(); }, [key, tab]); await until(async () => (await cardKey() || {}).key === key, 'the card of ' + key, 30000); await settle(600); };

    // ═══ 1 · two chips; Placements as it was; Approved empty ═══
    await openTab('place'); await page.waitForSelector('#egQueue .egPlacementList .placementRow', { timeout: 30000 });
    await until(() => page.evaluate(() => Engrave.monoItems().length === 3 && Engrave.monoItems().every(m => m.st.state === 'ready')), 'the monograms made', 90000); await settle(1200);
    let cs = await chips();
    check(cs.map(c => c.tab).join() === 'place,approved' && cs[0].label === 'Placements' && cs[1].label === 'Approved', 'the Engrave bar has two chips, Placements then Approved: ' + JSON.stringify(cs.map(c => c.label + ':' + c.n)));
    check(cs[0].on && !cs[1].on && cs[0].pressed === 'true' && cs[1].pressed === 'false', 'Placements is the one that is on (aria-pressed too)');
    const pending0 = await page.evaluate(() => Engrave.pendingRows());
    check(cs[0].n === 7 && cs[0].n === pending0 && cs[1].n === 0, 'Placements counts the 7 rows still to settle (Engrave.pendingRows), Approved counts none yet: ' + cs[0].n + ' / ' + cs[1].n);
    const placeKeys0 = (await keysOf()).sort();
    check(placeKeys0.length === 7 && Object.values(K).every(k => placeKeys0.includes(k)), 'Placements lists the 7 rows: ' + placeKeys0.length);
    await shot('placements-1440');
    // the empty Approved tab says so, and offers the way back
    await pressChip('approved');
    check((await rows()).length === 0 && /No approved engravings or monograms/.test(await none()) && (await view()).tab === 'approved', 'the empty Approved tab says "No approved engravings or monograms here yet.": "' + await none() + '"');
    await shot('approved-empty-1440');
    await page.click('#engraveView [data-eg-place]'); await until(async () => (await chip('place')).on, 'back on Placements', 8000); await settle(500);
    check((await rows()).length === 7 && (await view()).tab === 'place', 'its "Show placements" button goes back to the 7 rows of Placements');

    // ═══ 2 · approvals move pieces into Approved, live ═══
    const tBack = Date.now(); await openCard(K.back, 'place'); await approveJob(K.back); await settle(900);
    let a = await chip('approved'), p = await chip('place');
    check(a.n === 1 && p.n === 6, 'the bar of "back" approved: Approved 1, Placements 6 (the chips follow the approval with no press): ' + a.n + ' / ' + p.n);
    const tMono = Date.now(); await page.evaluate(k => { Engrave.openMonogram(k); }, K.mono2); await page.waitForSelector('#egQueue .rvItem[data-mono] .mw', { timeout: 20000 });
    await approveMono(K.mono2, rid('mono2')); await settle(1500);
    a = await chip('approved'); p = await chip('place');
    check(a.n === 2 && p.n === 5, 'the monogram OSR approved: Approved 2, Placements 5: ' + a.n + ' / ' + p.n);
    await page.evaluate(([k]) => Engrave.skip(Engrave.items().get(k), 'Test Tester'), [K.anna]); await until(async () => (await stateOfKey(K.anna)) === 'skipped', 'refAnna cut plain'); await settle(1200);
    a = await chip('approved'); p = await chip('place');
    check(a.n === 2 && p.n === 4 && (await stateOfKey(K.anna)) === 'skipped', 'refAnna cut plain ("No engraving"): out of Placements (4), NOT counted in Approved (2): ' + a.n + ' / ' + p.n);
    check(await page.evaluate(() => Engrave.reviewedRows()) === 3, 'Engrave.reviewedRows() is unchanged in meaning: all 3 decided rows (2 approved + 1 cut plain)');

    // ═══ 3 · the Approved tab lists exactly the approved ones; Placements lists everything, as before ═══
    await openTab('place'); await settle(900);
    const placeKeys1 = (await keysOf()).sort();
    check(JSON.stringify(placeKeys1) === JSON.stringify(placeKeys0), 'Placements still lists the same 7 rows after the decisions (approved and cut-plain rows stay in it, calm): ' + placeKeys1.length);
    check((await rows()).filter(r => r.decided).length === 3 && (await rows()).slice(0, 4).every(r => !r.decided), 'in it the 4 rows still to settle come first, then the 3 decided');
    await pressChip('approved');
    let rs = await rows(), ak = rs.map(r => r.open);
    a = await chip('approved');
    check(rs.length === 2 && a.n === rs.length, 'the Approved tab shows 2 rows and its chip says 2 (the number is the rows shown): ' + rs.length + ' / ' + a.n);
    check(ak.includes(K.back) && ak.includes(K.mono2) && !ak.includes(K.anna), 'they are the approved bar and the approved monogram; the cut-plain piece is not there: ' + JSON.stringify(rs.map(r => r.kind[0] + r.rid.slice(-3))));
    const st = await Promise.all(ak.map(stateOfKey));
    check(st.every(s => s === 'approved' || s === 'written') && rs.every(r => r.decided) && rs.some(r => r.kind === 'monogram') && rs.some(r => r.kind === 'engraved'), 'every row is an approved piece (an engraving and a monogram), none still to settle: ' + st.join());
    check(rs.find(r => r.open === K.back).text.includes('Approved') && /A P V|O S R/.test(rs.find(r => r.open === K.mono2).text.replace(/\s/g, ' ')) || rs.find(r => r.open === K.mono2).kind === 'monogram', 'the rows are the Placements rows (the state word "Approved" on the engraving, the monogram with its letters)');
    { const at = await atOf(ak);
      check(sortedBy(at, 'desc') && at[ak.indexOf(K.back)] >= tBack && at[ak.indexOf(K.mono2)] >= tMono, 'Newest first by default, by the existing control\'s own key (latest activity: each approval is at least its own time): ' + JSON.stringify(at.map(x => x - tBack))); }
    check((await chip('place')).n === 4, 'the Placements chip is the same on this tab (4)');
    await shot('approved-1440');

    // ═══ 4 · the same controls: sort, date menu, search ═══
    const keysNewest = await keysOf();
    await page.click('#engraveView .ordBar .cnDir'); await settle(700);
    { const k2 = await keysOf(), at = await atOf(k2);
      check(sortedBy(at, 'asc') && k2.join() === [...keysNewest].reverse().join(), 'the sort button flips the Approved list (Oldest first): ' + JSON.stringify(at.map(x => x - tBack))); }
    await page.click('#engraveView .ordBar .cnDir'); await settle(700);
    check((await keysOf()).join() === keysNewest.join(), 'and back to Newest first');
    for (const rg of ['yesterday', 'today', '7']) {   // (the date menu's own predicate says which of the two approved pieces are in the range; the tab and its chip must say the same)
      await page.evaluate(rg => { CNListActivity.set('engrave-place', { range: rg }); Engrave.render(); }, rg); await settle(900);
      const want = await page.evaluate(([ks, rg]) => ks.filter(k => CNListActivity.matches(Engrave.items().get(k) || Engrave.monoItems().find(m => m.key === k), rg)), [[K.back, K.mono2], rg]);
      const got = await keysOf(), n = (await chip('approved')).n;
      check(sameSet(got, want) && n === want.length && (rg !== 'today' || want.includes(K.mono2)) && (want.length || /No approved engravings or monograms/.test(await none())), `the date menu "${rg}" applies to the Approved tab: ${got.length} row(s) = the chip ${n} = what the range allows ${want.length}`);
    }
    await page.evaluate(() => { CNListActivity.set('engrave-place', { range: 'all' }); Engrave.render(); }); await settle(700);
    // the chips alone: nothing is read or written
    counting = true; traffic.length = 0;
    await pressChip('place'); await pressChip('approved'); await pressChip('place'); await pressChip('approved');
    check(traffic.length === 0, 'four presses of the chips make no fetch / XHR / socket request of their own (nothing new is read or written; the PlacementFeed\'s own timed tick is not counted): ' + traffic.length + (traffic.length ? ' ' + JSON.stringify(traffic.slice(0, 4)) : ''));
    counting = false;
    // the search: kept across the chips, and each chip counts what it shows
    counting = true; traffic.length = 0;
    await search('OSR');
    check((await keysOf()).join() === K.mono2 && (await chip('approved')).n === 1, 'searching "OSR" on Approved lists the monogram only; the chip says 1');
    await pressChip('place');
    check((await page.inputValue('#egQ')) === 'OSR' && (await view()).q === 'OSR' && (await keysOf()).join() === K.mono2, 'the chip Placements keeps the search "OSR" in the box and in the list (the monogram, decided)');
    check((await chip('place')).n === 0 && (await chip('approved')).n === 1, 'and the chips count what the search leaves: Placements 0 (nothing left to settle), Approved 1');
    await pressChip('approved');
    check((await page.inputValue('#egQ')) === 'OSR' && (await keysOf()).join() === K.mono2, 'back on Approved the search is still there, with the same one row');
    await search(rid('refMarck'));
    check((await rows()).length === 0 && /Nothing here matches/.test(await none()) && (await chip('approved')).n === 0, 'searching a piece still to settle on Approved finds nothing (it is not approved): "' + await none() + '"');
    await pressChip('place');
    check((await keysOf()).join() === K.marck, 'the same search on Placements finds it');
    await page.click('#egQX'); await settle(900);
    check((await view()).q === '' && (await rows()).length === 7, 'clearing the search brings back the 7 rows of Placements');
    console.log('   (requests the page itself made while the search was typed and cleared:', JSON.stringify(traffic), ')');
    counting = false;

    // ═══ 5 · the card, Back / Next, Close, Esc from the Approved tab; approving inside it ═══
    await pressChip('approved');
    await page.focus(`#egQueue .placementRow[data-open="${K.back}"]`); await page.keyboard.press('Enter'); await until(async () => (await cardKey() || {}).key === K.back, 'the approved bar opens (Enter on its focused row)'); await settle(900);
    let c = await cardKey(), v = await view();
    check(c && c.key === K.back && c.state === 'approved' && v.tab === 'approved' && (await chip('approved')).on, 'Enter on a focused approved row opens its card, and the Approved chip stays on: ' + JSON.stringify(Object.assign({}, c, { tab: v.tab })));
    await page.click('#egQueue .rvItem [data-a=next]'); await until(async () => (await cardKey() || {}).key === K.mono2, 'Next reaches the other approved piece'); await settle(700);
    c = await cardKey(); v = await view();
    check(c && c.key === K.mono2 && c.mono && v.tab === 'approved', 'Next walks to the other approved piece (the monogram) and stays on the Approved tab');
    await page.click('#egQueue .rvItem [data-a=next]'); await until(async () => (await cardKey() || {}).key === K.back, 'Next wraps'); await settle(500);
    await page.click('#egQueue .rvItem [data-a=prev]'); await until(async () => (await cardKey() || {}).key === K.mono2, 'Back returns'); await settle(700);
    check((await cardKey()).key === K.mono2, 'Next, Next, Back walk the two approved pieces (and wrap), never to a piece the tab does not list');
    await page.keyboard.press('Escape'); await until(async () => !(await cardKey()), 'Esc closes the card', 15000); await settle(900);
    v = await view(); rs = await rows();
    check(v.tab === 'approved' && v.list === true && rs.length === 2 && (await chip('approved')).on, 'Esc closes the card back to the Approved list (2 rows, the chip still on)');
    // a chip pressed while a card is in front shows that chip's list
    await page.click(`#egQueue .placementRow[data-open="${K.back}"]`); await until(async () => (await cardKey() || {}).key === K.back, 'the bar opens again'); await settle(700);
    await page.click('#engraveView .ordBar .egTab[data-tab="place"]'); await until(async () => !(await cardKey()) && (await chip('place')).on, 'Placements pressed over a card', 10000); await settle(700);
    check((await rows()).length === 7 && (await view()).tab === 'place', 'pressing Placements over an open card shows the list of Placements (7 rows)');
    // approving inside the tab: the monogram of the mixed order, then its bar: the chip rises live
    await page.evaluate(k => { Engrave.restoreView({ tab: 'approved', list: false, chosen: true, focus: k }); Engrave.render(); }, K.monoMixed);
    await page.waitForSelector('#egQueue .rvItem[data-mono] .mw', { timeout: 20000 });
    check((await chip('approved')).on && (await chip('approved')).n === 2, 'a piece still to settle opened while Approved is on: the chip is on and says 2');
    await approveMono(K.monoMixed, rid('mixed'));
    await until(async () => (await chip('approved')).n === 3, 'Approved 3', 15000);
    check((await chip('approved')).n === 3 && (await view()).tab === 'approved', 'approving the monogram inside the Approved tab raises its number at once: 3');
    await until(async () => (await cardKey() || {}).key === K.barMixed, 'the card goes on to the bar of the same order', 30000);
    const tBar = Date.now(); await approveJob(K.barMixed); await until(async () => (await chip('approved')).n === 4, 'Approved 4', 15000); await settle(900);
    check((await chip('approved')).n === 4 && (await chip('place')).n === 2 && (await view()).tab === 'approved', 'and the bar after it: Approved 4, Placements 2 (mono APV and refMarck are all that is left)');
    await page.click('#egQueue .rvItem [data-a=close]').catch(() => {}); await until(async () => !(await cardKey()), 'the card closed', 15000); await settle(900);
    rs = await rows(); ak = rs.map(r => r.open);
    check(rs.length === 4 && [K.back, K.mono2, K.monoMixed, K.barMixed].every(k => ak.includes(k)) && !ak.includes(K.mono) && !ak.includes(K.marck) && !ak.includes(K.anna), 'the Approved list now holds the 4 approved pieces (2 engraved, 2 monograms), none of the 3 others: ' + JSON.stringify(rs.map(r => r.kind[0] + r.rid.slice(-3))));
    { const at = await atOf(ak), byOrder = []; rs.forEach((r, i) => { const o = byOrder.find(x => x.rid === r.rid) || byOrder[byOrder.push({ rid: r.rid, at: 0 }) - 1]; o.at = Math.max(o.at, at[i]); });   // (the rows of one order stay together, as in Placements: the ORDERS are newest first)
      check(sortedBy(byOrder.map(o => o.at), 'desc') && at[ak.indexOf(K.barMixed)] >= tBar, 'still Newest first (order by order, the rows of one order together), and the bar just approved is at least as new as its own approval: ' + JSON.stringify(byOrder.map(o => [o.rid.slice(-3), o.at - tBack]))); }
    await shot('approved-4-1440');

    // ═══ 6 · saved views ═══
    const land = async v0 => { await page.evaluate(v1 => { CN.setMode('engrave'); Engrave.restoreView(v1); Engrave.render(); }, Object.assign({ chosen: true, q: '' }, v0)); await settle(900); return Object.assign(await view(), { on: (await chips()).filter(x => x.on).map(x => x.tab).join(), card: await cardKey() }); };
    let r6 = await land({ tab: 'done' });
    check(r6.tab === 'approved' && r6.on === 'approved' && r6.list === true && (await rows()).length === 4, 'a saved view that says "done" lands on the Approved tab (its list): ' + JSON.stringify({ tab: r6.tab, on: r6.on }));
    r6 = await land({ tab: 'approved', list: true });
    check(r6.tab === 'approved' && r6.on === 'approved', 'a saved view that says "approved" does too');
    r6 = await land({ tab: 'done', openDone: K.back });
    check(r6.tab === 'approved' && r6.card && r6.card.key === K.back && r6.on === 'approved', 'a "done" view with the piece it had open (openDone) lands on the Approved tab with that piece\'s card');
    for (const t of ['mono', 'words', 'place', 'nonsense']) { r6 = await land({ tab: t, list: true }); check(r6.tab === 'place' && r6.on === 'place', `a saved view that says "${t}" lands on Placements`); }

    // ═══ 7 · phone width ═══
    await page.evaluate(() => { Engrave.restoreView({ tab: 'approved', list: true, chosen: true, focus: null, q: '' }); Engrave.render(); }); await settle(600);
    await page.setViewportSize({ width: 400, height: 900 }); await sleep(500);
    if (await page.evaluate(() => !document.getElementById('app').classList.contains('railOff'))) { await page.click('#btnRail').catch(() => {}); await sleep(500); }
    await page.evaluate(() => document.querySelectorAll('.cnNbX, .toast .x').forEach(b => { try { b.click(); } catch (_) {} })); await sleep(600);
    const o = await page.evaluate(() => { const bar = document.querySelector('#engraveView .ordBar'); return { sw: document.documentElement.scrollWidth, cw: document.documentElement.clientWidth, bar: bar ? [bar.scrollWidth, bar.clientWidth] : null, chips: [...document.querySelectorAll('#engraveView .ordBar .egTab[data-tab]')].map(b => Math.round(b.getBoundingClientRect().right) <= document.documentElement.clientWidth) }; });
    await shot('approved-400');
    check(o.sw <= o.cw + 1 && o.chips.every(Boolean), 'phone width (400 px): both chips are on the screen and the page has no sideways overflow (the bar itself scrolls sideways there, as it did before, with the search and the date and sort controls) ' + JSON.stringify(o));
    await page.setViewportSize({ width: 1440, height: 900 });
    console.log('   page errors:', JSON.stringify(w.errors));
    check(w.errors.length === 0, 'no console or page errors');
  } catch (e) { fails.push('crashed: ' + (e && e.stack || e)); console.log('  ✗ crashed: ' + (e && e.stack || e)); }
  finally { await w.close(); }
  finish();
})().catch(e => { console.error(e); process.exit(1); });

function finish() {
  console.log(`\nengrave-approved-tab: ${nCheck - fails.length}/${nCheck} checks passed` + (fails.length ? ' · FAILED: ' + fails.join(' | ') : ''));
  process.exit(fails.length ? 1 : 0);
}
