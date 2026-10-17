// MONOSEAL (Paul, 17 Oct 2026: "When I approved the monogram, I didn't see any stamp or seal animation, and we do not have a custom-made seal that is specifically for approving monograms ... a brand new seal that is specific for
// monograms and the stamping animation whenever a monogram is approved ... in all the same places as the engraving stamp and seal animations ... 100% consistent"): approving a monogram is a PRESS, as approving an engraving is.
// The REAL sorter (charm-nest-1.html) in headless Chromium on the offline fake site (bridge-server.cjs), motion ON (the wooden stamp is real), stand-in alphabets (no live data, no paid AI, nothing live).
//   1  the seal: the ninth family, Seal.FAMILY.monogram, drawn with the monogram's own three letters; its kind is the process seal "monogramApproved" (the face, the service and the timeline tables without a browser: monogram-seal-service.cjs)
//   2  the ENGRAVE TAB's detail card (mode "detail"): Approve reads "Approved" and cannot be pressed while the wooden stamp comes down on the new seal in the card's seal row; Seal.press resolved BEFORE the card said Approved
//      (the Approve button was hidden only afterwards); the seal shows who and when (aria text), the timeline event is written (type monogramApproved, small data), the stamp is kept on this computer; a repaint, a close and a
//      reopen show the seal at rest and press nothing again; Change and approve again presses a NEW seal and the earlier one stays (oldest first)
//   3  the ORDER WINDOW's compact card (.engMono): the same, the seal row lying across the card's top-right corner as the engraving cards' seals do (HDRSEAL), moving nothing; the other host of the same piece shows the seal at rest
//   4  another computer: the stamps are read from the order's timeline events (this computer's own kept stamps wiped): a seal at rest, no stamp comes down; the service API (sealsOf / sealHtml / stampsFromEvents) says the same
//   5  a piece of a line of two: its own seal, its own event (data.slot "C2", poolId "<line>_2"); the other piece has none
//   6  reduced motion: the seal is there at once, no stamp, the card still says Approved only after; a name bar put away (nobody signed in): nothing is approved, nothing stamped
//   7  the order timeline draws the monogram stamp (a person's action, not the Engraved step); phone width (400): nothing runs past the screen
//   node tests/charm-nest/monogram-seal.cjs [playwright-core dir]
// What a pass does NOT prove: a live order on the live page, the live timeline, or another real computer; only that the page's own code presses, records and re-reads the seal on the offline fake.
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const SHOTS = process.env.SHOTS || '/tmp/mu/MS1';
const fails = []; let nCheck = 0;
const check = (ok, msg) => { nCheck++; if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
const finish = () => { console.log(`\nmonogram-seal: ${nCheck - fails.length}/${nCheck} checks passed`); if (fails.length) { console.log('FAILED:\n - ' + fails.join('\n - ')); process.exitCode = 1; } };

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  try { require(path.join(pwDir, 'playwright-core')); } catch (_) { console.log('monogram-seal: no playwright-core, skipped'); return; }
  process.env.PW_DIR = pwDir;
  fs.mkdirSync(SHOTS, { recursive: true });
  const R = require('./_engraveRealWorld.cjs'), Base = require('./_orderFactsWorld.cjs'), A = require('./_monogramAlphabets.cjs');
  const V = (name, value) => ({ name, value });
  function orderOf(fx, now) {
    const at = (d, h, m) => { const t = new Date(now); t.setDate(t.getDate() + d); t.setHours(h, m, 0, 0); return Math.floor(t.getTime() / 1000); };
    const shipBy = at(fx.ship, 17, 0), line = (x, i) => ({ transactionId: fx.rid + (i + 1), listingId: x.listing, sku: x.sku, title: x.title, quantity: x.qty || 1, expectedShipDate: shipBy, variations: x.variations || [], metalKey: x.metalKey || 'gold', metalLabel: 'GF 14/20', personalization: x.personalization || [] });
    return { receiptId: fx.rid, orderNumber: fx.rid, createTs: at(fx.placed.day, fx.placed.h, fx.placed.m), updateTs: at(0, 12, 0), shipBy, buyer: { name: fx.buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: (fx.lines || [fx]).map(line) };
  }
  Base.orderOf = orderOf;
  const MONO_TITLE = 'Monogram Necklace Personalized Jewelry Initials Pendant';
  const mono = (words, extra) => Object.assign({ listing: '997905351', sku: 'MONOGRAM_5351', title: MONO_TITLE, variations: [V('Metal choice', 'Gold')], personalization: [words], metalKey: 'gold' }, extra || {});
  R.ORDERS.mA = { rid: '4200683017', buyer: 'Name One', placed: { day: 0, h: 9, m: 10 }, ship: 5, lines: [mono('APV')] };
  R.ORDERS.mB = { rid: '4202783520', buyer: 'Name Two', placed: { day: 0, h: 9, m: 20 }, ship: 5, lines: [mono('O S R')] };
  R.ORDERS.mP = { rid: '4290000141', buyer: 'Name Three', placed: { day: 0, h: 9, m: 30 }, ship: 5, lines: [mono('J.Q.X', { qty: 2 })] };
  R.ORDERS.mR = { rid: '4290000142', buyer: 'Name Four', placed: { day: 0, h: 9, m: 40 }, ship: 5, lines: [mono('E.M.T')] };
  Object.assign(R.EXPECT_JOBS, { mA: 0, mB: 0, mP: 0, mR: 0 });
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const w = await R.world({ viewport: { width: 1440, height: 900 }, reducedMotion: 'no-preference', shots: SHOTS });
  if (!w) { console.log('monogram-seal: no playwright-core, skipped'); return; }
  const { page, srv } = w, st = srv.st;
  const shot = async name => { try { await page.evaluate(() => document.querySelectorAll('.cnNbX').forEach(b => { try { b.click(); } catch (_) {} })); await sleep(200); await page.screenshot({ path: path.join(SHOTS, name + '.png') }); } catch (e) { console.log('   (no screenshot ' + name + ')'); } };
  try {
    A.seed(st, await A.make());
    await page.evaluate(async () => { await Master.load(true); });
    // this test is about the seal, not the engine's rules: every word is staged as meeting them (nothing is "Needs a person", nothing asks the AI)
    await page.evaluate(() => { const eb = CharmNestMonogram.build; CharmNestMonogram.build = function () { const r = eb.apply(this, arguments); if (r && r.ok && r.report && r.report.metrics && Array.isArray(r.report.metrics.violations)) r.report.metrics = Object.assign({}, r.report.metrics, { violations: [], ok: true }); return r; }; });
    await R.build(w, ['mA', 'mB', 'mP', 'mR']);
    const keysOf = rid => page.evaluate(r => Orders.rows().filter(x => x.order.receiptId === r).map(x => x.key), rid);
    const kA = (await keysOf(R.ORDERS.mA.rid))[0], kB = (await keysOf(R.ORDERS.mB.rid))[0], kP = (await keysOf(R.ORDERS.mP.rid))[0], kR = (await keysOf(R.ORDERS.mR.rid))[0];
    check(!!kA && !!kB && !!kP && !!kR, 'the four monogram orders are in the page');
    const ridA = R.ORDERS.mA.rid, ridB = R.ORDERS.mB.rid, ridP = R.ORDERS.mP.rid, ridR = R.ORDERS.mR.rid;

    // ═══ the instruments: every Seal.press (what it pressed, the card's state at the start and when the ink was down), every wooden stamp that appeared ═══
    await page.evaluate(() => {
      const S = window.Seal, orig = S.press; window.__press = []; window.__tools = [];
      const mwOf = el => { const row = el.closest('.mwSeal'); return el.closest('.mw') || (row && row.parentElement && row.parentElement.querySelector('.mw')) || null; };
      S.press = function (el) {
        const mw = el && mwOf(el), btn = mw && mw.querySelector('[data-mw-approve]'), ok = mw && mw.querySelector('[data-mw-approved]');
        const snap = () => ({ approveHidden: !!btn.hidden, approveText: btn.textContent.trim(), approveDisabled: !!btn.disabled, okHidden: !!ok.hidden, state: mw.dataset.state });
        const rec = { t: performance.now(), family: el && el.querySelector('svg') && el.querySelector('svg').dataset.sealFamily, kind: el && [...el.classList].find(c => /^seal-/.test(c)), compact: !!(el && el.closest('.engMono')), inRow: !!(el && el.closest('.mwSeal')), pending: !!(el && el.classList.contains('pending')), label: el && el.getAttribute('aria-label'), start: mw ? snap() : null, end: null, doneAt: 0 };
        window.__press.push(rec);
        const p = orig.apply(this, arguments); p.then(() => { rec.doneAt = performance.now(); if (mw) rec.end = snap(); }, () => {});
        return p;
      };
      new MutationObserver(ms => { for (const m of ms) for (const n of m.addedNodes) if (n.nodeType === 1 && n.classList && n.classList.contains('sealTool')) window.__tools.push({ t: performance.now(), family: (n.querySelector('svg') || { dataset: {} }).dataset.stampFamily }); }).observe(document.documentElement, { childList: true, subtree: true });
    });
    const presses = () => page.evaluate(() => window.__press.slice());
    const tools = () => page.evaluate(() => window.__tools.slice());
    const until = async (fn, what, arg, ms = 30000) => { const t0 = Date.now(); let v; for (;;) { v = await page.evaluate(fn, arg); if (v) return v; if (Date.now() - t0 > ms) { console.log('   (waiting for ' + what + ')'); return null; } await sleep(100); } };
    // (after an approval in the Engrave tab the card goes up into the tab's count, as an approved engraving's does: the state is read from the service, and the card is opened again to look at the seal at rest)
    const approvedOf = (key, letters) => page.evaluate(({ k, l }) => { const i = k.indexOf('#'), line = i < 0 ? k : k.slice(0, i), svc = Engrave.monogramUI(), row = Orders.rows().find(r => r.key === line), d = row && svc.describe(row, i < 0 ? null : k.slice(i + 1)); return !!(d && d.approved && !d.stamping && (!l || d.approved.letters === l)); }, { k: key, l: letters || '' });
    const waitApproved = async (key, letters, what) => { const t0 = Date.now(); for (;;) { if (await approvedOf(key, letters) && await page.evaluate(() => !document.querySelector('.sealTool'))) { await sleep(1500); return true; } if (Date.now() - t0 > 40000) { console.log('   (waiting for ' + what + ')'); return false; } await sleep(100); } };

    // ═══ 1 · the seal ═══
    const fam = await page.evaluate(() => ({ n: Object.keys(Seal.FAMILY).length, mono: !!Seal.FAMILY.monogram, ink: Seal.FAMILY.monogram && Seal.FAMILY.monogram.ink, kinds: Seal.INK.monogramApproved, model: Seal.modelOf({ how: 'monogramApproved', at: 1760000000000, by: 'X', letters: 'apv', n: 2, of: 3 }),
      title: Seal.titleOf({ how: 'monogramApproved', at: 1760000000000, by: 'Sam', letters: 'APV', n: 2, of: 3 }), html: Seal.html({ how: 'monogramApproved', at: 1760000000000, by: 'Sam', letters: 'APV', id: 'k' }, 50, 'monogramSeal') }));
    check(fam.n === 9 && fam.mono && fam.ink === '#4d3f99' && fam.kinds === '#4d3f99', 'Seal has nine families and the monogram one is indigo #4d3f99: ' + fam.n);
    check(fam.model.family === 'monogram' && fam.model.action === 'MONOGRAM' && fam.model.letters === 'APV' && fam.model.of === 3, 'modelOf: family monogram, action MONOGRAM, the letters and the piece: ' + JSON.stringify(fam.model));
    check(/^Monogram APV approved by Sam · .*2025.* · piece 2 of 3$/.test(fam.title), 'the seal says who and when: "' + fam.title + '"');
    check(/seal-monogramApproved/.test(fam.html) && /data-process-seal="k"/.test(fam.html) && /data-seal-family="monogram"/.test(fam.html) && />MONOGRAM</.test(fam.html), 'Seal.html draws the process seal of kind monogramApproved with the word MONOGRAM');

    // ═══ 2 · the Engrave tab's detail card ═══
    const openDetail = async key => { await page.evaluate(k => { CN.setMode('engrave'); Engrave.openMonogram(k); }, key); await page.waitForSelector('#egQueue .rvItem[data-mono] .mw[data-mode="detail"]', { timeout: 30000 }); };
    const detailReady = key => until(k => { const c = document.querySelector('#egQueue .rvItem[data-mono]'), m = c && c.querySelector('.mw'); return !!c && c.dataset.key === k && !!m && ['ready', 'approved'].includes(m.dataset.state) && !m.dataset.busy; }, 'the detail card of ' + key, key);
    await openDetail(kA); check(!!await detailReady(kA), 'the Engrave detail card of APV is open and its monogram is made');
    check(await page.evaluate(() => !document.querySelector('#egQueue .mwSeal .seal')), 'before the approval the card has no seal');
    await shot('1-detail-ready');
    // (a picture while the wooden stamp is down on the seal: the click is not waited for, the stamp is looked for, and the picture is taken as it touches)
    { const clicking = page.click('#egQueue .rvItem[data-mono] [data-mw-approve]'); await page.waitForSelector('.sealTool', { timeout: 6000 }).catch(() => {}); await sleep(430); try { await page.screenshot({ path: path.join(SHOTS, '2-detail-stamping.png') }); } catch (_) { /* no picture */ } await clicking; }
    check(await waitApproved(kA, 'APV', 'the approval and the end of the stamp'), 'Approve: the monogram is approved once the stamp has lifted (and the card goes up into the tab, as an approved engraving\'s does)');
    let pr = await presses(), tl = await tools();
    check(pr.length === 1 && pr[0].family === 'monogram' && pr[0].kind === 'seal-monogramApproved' && pr[0].inRow && !pr[0].compact, 'exactly one stamp was pressed, on a monogram seal in the widget\'s seal row: ' + JSON.stringify(pr.map(p => [p.family, p.kind, p.inRow])));
    check(pr[0] && pr[0].start && pr[0].start.approveText === 'Approved' && pr[0].start.approveDisabled && !pr[0].start.approveHidden && pr[0].start.okHidden, 'while the stamp came down the button read "Approved", could not be pressed, and the card did not yet say Approved: ' + JSON.stringify(pr[0] && pr[0].start));
    check(pr[0] && pr[0].end && !pr[0].end.approveHidden && pr[0].end.okHidden && pr[0].end.state === 'ready', 'Seal.press had resolved BEFORE the Approve button was hidden (the state was still "ready" when the ink was down): ' + JSON.stringify(pr[0] && pr[0].end));
    check(tl.length === 1 && tl[0].family === 'monogram', 'the wooden stamp was the monogram\'s own (a .sealTool of the monogram family appeared once): ' + JSON.stringify(tl.map(t => t.family)));
    await openDetail(kA); await detailReady(kA); await sleep(700);
    check(await page.evaluate(() => { const m = document.querySelector('#egQueue .rvItem[data-mono] .mw'), b = m.querySelector('[data-mw-approve]'), ok = m.querySelector('[data-mw-approved]'); return m.dataset.state === 'approved' && b.hidden && !ok.hidden && /Approved · Monogram APV/.test(ok.textContent) && !m.querySelector('[data-mw-change]').hidden; }), 'the card opened again says "Approved · Monogram APV" with its Change, as before');
    let seals = await page.evaluate(() => [...document.querySelectorAll('#egQueue .rvItem[data-mono] .mwSeal .seal')].map(s => ({ fam: s.querySelector('svg').dataset.sealFamily, label: s.getAttribute('aria-label'), pending: s.classList.contains('pending'), op: getComputedStyle(s).opacity, text: s.textContent, w: s.offsetWidth })));
    check(seals.length === 1 && seals[0].fam === 'monogram' && !seals[0].pending && +seals[0].op > .5 && Math.abs(seals[0].w - 50) < 2, 'one monogram seal rests in the card\'s seal row, inked and 50 px: ' + JSON.stringify(seals.map(s => [s.fam, s.op, s.w])));
    check(/^Monogram APV approved by Test Operator · /.test(seals[0].label) && /MONOGRAM/.test(seals[0].text) && /A/.test(seals[0].text) && /P/.test(seals[0].text) && /V/.test(seals[0].text), 'it carries who and when and the letters: "' + seals[0].label + '"');
    await shot('2-detail-approved');
    // the permanent record: one timeline event, small, a person's action and no milestone
    const evs = () => st.list('Order_Timeline').filter(e => e.type === 'monogramApproved');
    await page.waitForFunction(() => OrderTimeline.pending() === 0, null, { timeout: 15000 }).catch(() => {});
    let ev = evs();
    check(ev.length === 1 && ev[0].orderId === ridA && ev[0].lineKey === kA && ev[0].by === 'Test Operator' && ev[0].station === 'sorter' && ev[0].milestone === false, 'the timeline holds ONE monogramApproved event for the line, by the signed-in person, not a milestone: ' + JSON.stringify(ev.map(e => [e.orderId, e.lineKey, e.by, e.milestone])));
    check(ev[0] && ev[0].data && ev[0].data.letters === 'APV' && ev[0].data.n === 1 && ev[0].data.of === 1 && ev[0].data.poolId === kA + '_1' && ev[0].data.signedIn === true && JSON.stringify(ev[0].data).length < 600, 'its data says the letters, the piece, the pool id and that a person was signed in, and is small: ' + JSON.stringify(ev[0] && ev[0].data));
    check(ev[0] && ev[0].at > 1e12 && /^Monogram APV/.test(ev[0].text), 'its time is a real time and its text says what was approved: "' + (ev[0] && ev[0].text) + '"');
    // kept on this computer
    const kept = await page.evaluate(k => { const m = (JSON.parse(localStorage.getItem('cn.settings') || '{}').mono || {}).seals || {}; return m[k] || null; }, kA);
    check(kept && kept.length === 1 && kept[0].how === 'monogramApproved' && kept[0].by === 'Test Operator' && kept[0].letters === 'APV' && kept[0].at === ev[0].at, 'the stamp is kept on this computer (settings.mono.seals), with the event\'s own time: ' + JSON.stringify(kept));
    // a repaint, a close and a reopen: the seal at rest, nothing pressed again
    await page.evaluate(() => { Engrave.render(); Engrave.render(); }); await sleep(500);
    check((await presses()).length === 1 && await page.evaluate(() => document.querySelectorAll('#egQueue .mwSeal .seal').length === 1), 'a repaint of the Engrave tab keeps the one seal and presses nothing');
    await page.evaluate(() => { const c = document.querySelector('#egQueue .rvItem[data-mono] [data-a=close]'); if (c) c.click(); }); await sleep(900);
    await openDetail(kA); await detailReady(kA); await sleep(900);
    seals = await page.evaluate(() => [...document.querySelectorAll('#egQueue .rvItem[data-mono] .mwSeal .seal')].map(s => ({ pending: s.classList.contains('pending'), op: getComputedStyle(s).opacity })));
    check(seals.length === 1 && !seals[0].pending && +seals[0].op > .5 && (await presses()).length === 1 && (await tools()).length === 1, 'closed and reopened: the card is approved and shows the seal at rest; nothing was pressed again (' + (await presses()).length + ' press, ' + (await tools()).length + ' stamp)');
    // Change and a new letter, approve again: a NEW seal, the earlier one stays, oldest first
    await sleep(1200);
    await page.click('#egQueue .rvItem[data-mono] [data-mw-change]'); await page.click('#egQueue .rvItem[data-mono] [data-mw-letter="1"]'); await page.keyboard.type('Q');
    await until(() => { const m = document.querySelector('#egQueue .rvItem[data-mono] .mw'); return m && m.dataset.state === 'ready' && !m.dataset.busy; }, 'the changed monogram');
    check(await page.evaluate(() => document.querySelectorAll('#egQueue .mwSeal .seal').length === 1), 'while a changed monogram waits for its Approve the earlier seal stays');
    await page.click('#egQueue .rvItem[data-mono] [data-mw-approve]');
    check(await waitApproved(kA, 'AQV', 'the second approval'), 'approved again');
    await openDetail(kA); await detailReady(kA); await sleep(700);
    pr = await presses(); seals = await page.evaluate(() => [...document.querySelectorAll('#egQueue .rvItem[data-mono] .mwSeal .seal')].map(s => ({ at: +s.dataset.at, label: s.getAttribute('aria-label'), text: s.textContent, w: s.offsetWidth, vis: s.getBoundingClientRect().width > 0 })));
    check(pr.length === 2 && pr[1].family === 'monogram' && seals.length === 2 && seals[0].at < seals[1].at && /Monogram APV approved/.test(seals[0].label) && /Monogram AQV approved/.test(seals[1].label), 'the new approval pressed a NEW seal (2 presses) and the earlier seal stays, oldest first: ' + seals.map(s => s.label.slice(0, 22)).join(' | '));
    check(seals.every(x => x.w === 50 && x.vis), 'both seals keep the regular 50 px in the action row (the fitter never shrinks them): ' + JSON.stringify(seals.map(x => x.w)));
    check((await tools()).length === 2 && evs().length === 2, 'a second wooden stamp, a second timeline event (the replaced design leaves the first event in place)');
    await shot('2-detail-changed');
    const sealZoom = await page.evaluate(() => { const s = document.querySelector('#egQueue .mwSeal .seal:last-child'); return !!s && !s.hasAttribute('title') && s.getAttribute('role') === 'img' && s.tabIndex === 0 && s.hasAttribute('data-process-seal'); });
    check(sealZoom, 'a seal of the row is a process seal like the engraving\'s (a picture with its words, reachable by Tab, never forwarding a click to the button)');

    // ═══ 3 · the Order window's compact card ═══
    const openWin = async key => { await page.evaluate(async k => { CN.setMode('orders'); await OrderWin.open(k); }, key); await page.waitForSelector('#orderWin[open] #owEng .mw', { timeout: 30000 }); await sleep(900); };
    const winReady = () => until(() => { const m = document.querySelector('#owEng .mw'); return !!m && ['ready', 'approved'].includes(m.dataset.state) && !m.dataset.busy; }, 'the window\'s monogram');
    await openWin(kA); await winReady();
    let geo = await page.evaluate(() => { const card = document.querySelector('#owEng .engMono'), row = card.querySelector('.mwSeal'), seals = [...row.querySelectorAll('.seal')], c = card.getBoundingClientRect(), r = seals.map(s => s.getBoundingClientRect()), cs = getComputedStyle(row);
      return { n: seals.length, pos: cs.position, w: seals.map(s => s.offsetWidth), top: Math.round(r[0].top - c.top), over: Math.round(r[r.length - 1].right - c.right), cardH: Math.round(c.height), op: seals.map(s => +getComputedStyle(s).opacity), fam: seals.map(s => s.querySelector('svg').dataset.sealFamily) }; });
    check(geo.n === 2 && geo.pos === 'absolute' && geo.fam.every(f => f === 'monogram') && geo.w.every(x => x === 48) && geo.op.every(o => o > .5), 'the Order window\'s card shows both seals at rest, 48 px like the engraving cards\' seals, out of the flow: ' + JSON.stringify(geo));
    check(geo.top < -15 && geo.top > -40 && geo.over > 0 && geo.over <= 14, 'across the card\'s outer top-right corner (' + geo.top + ' px from its top, ' + geo.over + ' px past its right edge)');
    check((await presses()).length === 2, 'opening the window pressed nothing: the seals of another host rest');
    await shot('3-window-approved');
    await page.evaluate(() => OrderWin.close()); await sleep(500);
    // approving in the window: the compact host presses its own seal
    await openWin(kB); await winReady();
    const hB0 = await page.evaluate(() => Math.round(document.querySelector('#owEng .engMono').getBoundingClientRect().height));
    await page.evaluate(() => { window.__hs = { min: 1e9, max: 0, on: true }; const loop = () => { const h = window.__hs; if (!h.on) return; const c = document.querySelector('#owEng .engMono'); if (c) { const x = c.getBoundingClientRect().height; h.min = Math.min(h.min, x); h.max = Math.max(h.max, x); } requestAnimationFrame(loop); }; loop(); });
    { const clicking = page.click('#owEng .mw [data-mw-approve]'); await page.waitForSelector('.sealTool', { timeout: 6000 }).catch(() => {}); await sleep(430); try { await page.screenshot({ path: path.join(SHOTS, '3-window-stamping.png') }); } catch (_) { /* no picture */ } await clicking; }
    check(!!await until(() => { const m = document.querySelector('#owEng .mw'); return m && m.dataset.state === 'approved' && !document.querySelector('.sealTool'); }, 'the window\'s approval'), 'Approve in the window: the card says approved once the stamp has lifted');
    pr = await presses(); tl = await tools();
    check(pr.length === 3 && pr[2].compact && pr[2].family === 'monogram' && pr[2].inRow, 'one more press, on a monogram seal in the window\'s card: ' + JSON.stringify(pr.slice(2).map(p => [p.family, p.compact])));
    check(pr[2] && pr[2].start.approveText === 'Approved' && pr[2].start.approveDisabled && !pr[2].start.approveHidden && pr[2].end && !pr[2].end.approveHidden && pr[2].end.okHidden, 'there too the button read "Approved" and the card said Approved only after the ink was down: ' + JSON.stringify(pr[2] && [pr[2].start, pr[2].end]));
    check(tl.length === 3 && tl[2].family === 'monogram', 'and the wooden monogram stamp came down once more');
    const hw = await page.evaluate(() => { window.__hs.on = false; return window.__hs; });
    check(hw.max - hw.min <= 1.5, 'the seal moved nothing: the card kept one height through the approval (' + hw.min + '–' + hw.max + ' px; was ' + hB0 + ')');
    await shot('3-window-pressed');
    // the same piece in the Engrave tab shows that seal at rest, never pressed
    await page.evaluate(() => OrderWin.close()); await sleep(500);
    await openDetail(kB); await detailReady(kB); await sleep(700);
    check(await page.evaluate(() => { const s = document.querySelectorAll('#egQueue .rvItem[data-mono] .mwSeal .seal'); return s.length === 1 && !s[0].classList.contains('pending') && s[0].getAttribute('aria-label').indexOf('Monogram OSR approved by Test Operator') === 0; }) && (await presses()).length === 3, 'the Engrave tab\'s card of the piece approved in the window shows the same seal at rest (the service is shared), nothing pressed');
    await page.evaluate(() => { const c = document.querySelector('#egQueue .rvItem[data-mono] [data-a=close]'); if (c) c.click(); }); await sleep(600);

    // ═══ 4 · another computer: only the timeline knows ═══
    const wipe = await page.evaluate(async ({ k, rid }) => {
      const set = JSON.parse(localStorage.getItem('cn.settings') || '{}'); const had = Object.keys((set.mono || {}).seals || {}).length;
      delete S.settings.mono.seals; saveSettings();
      const svc = Engrave.monogramUI(); for (const c of svc.cards.values()) { c.seals = null; }
      svc.feedEvents(rid, []);   // (and the timeline not yet read, as on a computer that has just opened the page)
      const row = Orders.rows().find(r => r.key === k);
      const before = svc.sealsOf(row, null).length;
      await svc.loadSeals(rid, true);
      const after = svc.sealsOf(row, null), html = CharmNestMonogramUI.sealHtml(row, {});
      return { had, before, after, html, viaGlobal: CharmNestMonogramUI.sealsOf(row, null).length };
    }, { k: kB, rid: ridB });
    check(wipe.had >= 2 && wipe.before === 0, 'this computer\'s own kept stamps are wiped (it had ' + wipe.had + ' pieces\' stamps): the card knows none (' + wipe.before + ')');
    check(wipe.after.length === 1 && wipe.after[0].by === 'Test Operator' && wipe.after[0].letters === 'OSR' && wipe.viaGlobal === 1, 'the order\'s timeline hands the service the stamp (loadSeals): sealsOf says it: ' + JSON.stringify(wipe.after.map(s => [s.by, s.letters, s.n])));
    check(/class="sealRow mwSeal/.test(wipe.html) && /data-seal-family="monogram"/.test(wipe.html) && /Monogram OSR approved by Test Operator/.test(wipe.html), 'sealHtml draws it for any surface (a row of one monogram seal)');
    await openDetail(kB); await detailReady(kB); await sleep(700);
    check(await page.evaluate(() => { const s = document.querySelectorAll('#egQueue .rvItem[data-mono] .mwSeal .seal'); return s.length === 1 && !s[0].classList.contains('pending'); }) && (await presses()).length === 3 && (await tools()).length === 3, 'a card opened with only the timeline\'s stamp shows the seal at rest and presses nothing (another computer\'s approval is shown, not re-stamped)');
    const node = await page.evaluate(() => {
      const U = CharmNestMonogramUI, e = (at, by, data, extra) => Object.assign({ type: 'monogramApproved', at, by, lineKey: 'L1', data }, extra || {});
      const evs = [e(1.76e12, 'Ann', { letters: 'abc', n: 1, of: 2 }), e(1.76e12 + 400, 'Ann', { letters: 'ABC', n: 1, of: 2 }), e(1.76e12 + 5, 'Bob', { letters: 'XYZ', slot: 'C2', of: 2 }), e(1.77e12, 'Cy', { letters: 'ABC' }, { lineKey: 'L2' }), { type: 'designDropped', at: 1.76e12, lineKey: 'L1' }, e(0, 'Zed', {})];
      return { all: U.stampsFromEvents(evs, 'L1').map(s => [s.by, s.letters, s.n]), two: U.stampsFromEvents(evs, 'L1', 2).map(s => [s.by, s.letters, s.n]), other: U.stampsFromEvents(evs, 'L2').length, merged: U.mergeStamps([{ at: 1e12 + 100, by: 'A' }], [{ at: 1e12 + 900, by: 'A' }, { at: 2e12, by: 'B' }]).length };
    });
    check(JSON.stringify(node.all) === JSON.stringify([['Ann', 'ABC', 1], ['Bob', 'XYZ', 2]]) && JSON.stringify(node.two) === JSON.stringify([['Bob', 'XYZ', 2]]) && node.other === 1 && node.merged === 2, 'stampsFromEvents / mergeStamps: one press is one stamp (a second of leeway, per piece), a slot names its piece, other lines and other types are not read: ' + JSON.stringify(node));

    // an approval another computer makes while the Order window is open: the window's own timeline read brings it (OrderTimelineUI polls the open order), and the card draws the seal at rest
    await page.evaluate(() => { OrderTimelineUI.pollOpenMs = 800; });
    const pBefore = (await presses()).length, tBefore = (await tools()).length;
    await openWin(kB); await winReady();
    const n0 = await page.evaluate(() => document.querySelectorAll('#owEng .mwSeal .seal').length);
    st.put('Order_Timeline', `${ridB}~monogramApproved~foreign-1`, { orderId: ridB, type: 'monogramApproved', at: Date.now() - 30000, by: 'Other Computer', source: 'sorter', station: 'sorter', device: 'charm-nest-1', lineKey: kB, transactionId: '', sheetId: '', sheet: '', setId: '', text: 'Monogram OSR', data: { letters: 'OSR', slot: null, n: 1, of: 1, poolId: kB + '_1', signedIn: true }, milestone: false });
    const got = await until(n => document.querySelectorAll('#owEng .mwSeal .seal').length === n + 1, 'the window to read the other computer\'s stamp', n0, 20000);
    check(n0 === 1 && !!got, 'the Order window, open, draws the seal another computer pressed once its timeline read brings the event (' + n0 + ' seal, then ' + (await page.evaluate(() => document.querySelectorAll('#owEng .mwSeal .seal').length)) + ')');
    check(await page.evaluate(() => [...document.querySelectorAll('#owEng .mwSeal .seal')].some(x => /approved by Other Computer/.test(x.getAttribute('aria-label'))) && ![...document.querySelectorAll('#owEng .mwSeal .seal')].some(x => x.classList.contains('pending'))) && (await presses()).length === pBefore && (await tools()).length === tBefore, 'it names the other person, rests (not pending), and nothing came down for it');
    await page.evaluate(() => OrderWin.close()); await sleep(400);

    // ═══ 5 · a line of two pieces: each piece its own seal and event ═══
    const pieceKey = n => kP + '#C' + n;
    await openDetail(pieceKey(2)); check(!!await detailReady(pieceKey(2)), 'the card of piece 2 of 2 (J Q X) is open');
    await page.click('#egQueue .rvItem[data-mono] [data-mw-approve]');
    await waitApproved(pieceKey(2), 'JQX', 'piece 2 approved');
    await openDetail(pieceKey(2)); await detailReady(pieceKey(2)); await sleep(600);
    const ps = await page.evaluate(({ k }) => { const svc = Engrave.monogramUI(), row = Orders.rows().find(r => r.key === k); return { c1: svc.sealsOf(row, { slot: 'C1' }).length, c2: svc.sealsOf(row, { slot: 'C2' }).map(s => [s.letters, s.n, s.of]), label: (document.querySelector('#egQueue .mwSeal .seal') || { getAttribute: () => '' }).getAttribute('aria-label') }; }, { k: kP });
    check(ps.c1 === 0 && JSON.stringify(ps.c2) === JSON.stringify([['JQX', 2, 2]]) && /piece 2 of 2/.test(ps.label), 'piece 2 has its seal, piece 1 has none: ' + JSON.stringify(ps));
    await page.waitForFunction(() => OrderTimeline.pending() === 0, null, { timeout: 15000 }).catch(() => {});
    const pe = evs().filter(e => e.orderId === ridP);
    check(pe.length === 1 && pe[0].data.slot === 'C2' && pe[0].data.n === 2 && pe[0].data.of === 2 && pe[0].data.poolId === kP + '_2' && /Piece 2 of 2/.test(pe[0].text), 'its event names the piece (slot C2, poolId <line>_2): ' + JSON.stringify(pe.map(e => [e.data.slot, e.data.poolId, e.text])));
    await page.evaluate(() => { const c = document.querySelector('#egQueue .rvItem[data-mono] [data-a=close]'); if (c) c.click(); }); await sleep(500);

    // ═══ 6 · reduced motion, and nobody signed in ═══
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await openDetail(kR); await detailReady(kR);
    const before = (await tools()).length;
    await page.click('#egQueue .rvItem[data-mono] [data-mw-approve]');
    check(await waitApproved(kR, 'EMT', 'the reduced-motion approval'), 'reduced motion: the monogram is approved');
    await openDetail(kR); await detailReady(kR); await sleep(500);
    check((await tools()).length === before && await page.evaluate(() => { const s = document.querySelectorAll('#egQueue .rvItem[data-mono] .mwSeal .seal'); return s.length === 1 && !s[0].classList.contains('pending') && +getComputedStyle(s[0]).opacity > .5; }), 'with reduced motion no wooden stamp comes down, and the seal is there at once, inked');
    await page.emulateMedia({ reducedMotion: 'no-preference' });
    // nobody signed in: the small name bar opens, put away = nothing happens
    await page.evaluate(() => { window.__emp = B.employee; B.employee = ''; try { localStorage.removeItem('cn.employee'); } catch (_) {} });
    await page.evaluate(() => { const c = document.querySelector('#egQueue .rvItem[data-mono] [data-a=close]'); if (c) c.click(); }); await sleep(500);
    const p0 = (await presses()).length;
    await page.evaluate(() => { Engrave.monogramUI().approve; });
    const row0 = await page.evaluate(async () => { const svc = Engrave.monogramUI(); return svc.describe(Orders.rows().find(r => r.order.receiptId === '4200683017'), null).approved; });
    check(!!row0, 'the first order is still approved (its person stays)');
    await page.evaluate(() => { const set = JSON.parse(localStorage.getItem('cn.settings') || '{}'); window.__keepSeals = JSON.stringify(((set.mono || {}).seals) || {}); });
    // (a fresh piece: C1 of the pair, nobody signed in)
    await openDetail(pieceKey(1)); await detailReady(pieceKey(1));
    await page.click('#egQueue .rvItem[data-mono] [data-mw-approve]');
    const bar = await until(() => !!document.querySelector('.cnNameBar'), 'the name bar', null, 8000);
    check(!!bar, 'nobody signed in: Approve opens the small name bar (never a browser box)');
    await page.evaluate(() => { const x = document.querySelector('.cnNameBar .cnNbX'); if (x) x.click(); }); await sleep(900);
    const none = await page.evaluate(() => { const m = document.querySelector('#egQueue .rvItem[data-mono] .mw'); return { state: m.dataset.state, seals: document.querySelectorAll('#egQueue .mwSeal .seal').length }; });
    check(none.state === 'ready' && none.seals === 0 && (await presses()).length === p0, 'the name bar put away: nothing is approved and nothing is stamped: ' + JSON.stringify(none));
    await page.evaluate(() => { B.employee = window.__emp || 'Test Operator'; try { localStorage.setItem('cn.employee', B.employee); } catch (_) {} });
    await page.click('#egQueue .rvItem[data-mono] [data-mw-approve]');
    check(await waitApproved(pieceKey(1), 'JQX', 'the approval after signing in'), 'signed in again: the same Approve now approves, stamped by the person');
    await openDetail(pieceKey(1)); await detailReady(pieceKey(1)); await sleep(500);
    check((await presses()).length === p0 + 1 && await page.evaluate(() => /approved by Test Operator/.test((document.querySelector('#egQueue .mwSeal .seal') || { getAttribute: () => '' }).getAttribute('aria-label'))), 'and the stamp names Test Operator');
    await page.evaluate(() => { const c = document.querySelector('#egQueue .rvItem[data-mono] [data-a=close]'); if (c) c.click(); }); await sleep(500);

    // ═══ 7 · the order timeline, and a phone ═══
    await openWin(kA); await sleep(1500);
    const tlm = await until(() => { const n = document.querySelectorAll('#orderWin svg[data-seal-family="monogram"]'); return n.length >= 1 ? [...n].map(x => x.closest('[role=img],button,span,div') && 1).length : 0; }, 'a monogram stamp in the order timeline', null, 25000);
    check(!!tlm, 'the order timeline draws the monogram stamp of the approvals (' + tlm + ')');
    check(await page.evaluate(() => { const lane = document.querySelector('#orderWin .tlUI'); if (!lane) return true; const e = [...lane.querySelectorAll('[data-step="engraved"] svg[data-seal-family="monogram"], .tlStop[data-k="engraved"] svg[data-seal-family="monogram"]')]; return e.length === 0; }), 'and it is not the Engraved step');
    await page.evaluate(() => OrderWin.close()); await sleep(400);
    await page.setViewportSize({ width: 400, height: 900 }); await sleep(500);
    await page.evaluate(() => { document.querySelectorAll('.cnNbX, .toast .x').forEach(b => { try { b.click(); } catch (_) {} }); if (!document.getElementById('app').classList.contains('railOff')) { const b = document.getElementById('btnRail'); if (b) b.click(); } }); await sleep(600);
    await openWin(kA); await winReady();
    const ph = await page.evaluate(() => { const c = document.querySelector('#owEng .engMono'), row = c && c.querySelector('.mwSeal'), r = [...(row ? row.querySelectorAll('.seal') : [])].map(s => s.getBoundingClientRect()); return { n: r.length, right: Math.round(Math.max(...r.map(x => x.right))), left: Math.round(Math.min(...r.map(x => x.left))), vw: innerWidth, sw: document.documentElement.scrollWidth, cw: document.documentElement.clientWidth, win: (() => { const d = document.getElementById('orderWin').getBoundingClientRect(); return [Math.round(d.left), Math.round(d.right)]; })() }; });
    check(ph.n === 2 && ph.left >= 0 && ph.right <= ph.vw && ph.sw <= ph.cw + 1, 'phone width (400): the two seals are on the screen and the page has no sideways scroll: ' + JSON.stringify(ph));
    await page.evaluate(() => { const e = document.getElementById('owEngSec'); if (e) e.scrollIntoView({ block: 'center' }); }); await sleep(300);
    await shot('7-phone-window');
    await page.evaluate(() => OrderWin.close());
    await page.setViewportSize({ width: 1440, height: 900 }); await sleep(300);
    check(w.errors.length === 0, 'no page errors: ' + JSON.stringify(w.errors.slice(0, 3)));
  } catch (e) { check(false, 'the run broke: ' + String(e && e.stack || e).split('\n').slice(0, 4).join(' | ')); }
  finally { await w.close(); finish(); }
})().catch(e => { console.error(e); process.exitCode = 1; });
