// HOLDPROG / HOLDSTAMP (Paul, 27 Oct 2026, order 4209777939): "the whole process completed successfully, but it took over a minute to complete which is really bad and really long and there was
// no feedback to the user. No progress bar know anything to let us know what was happening and when it would've finish and actually did it complete successfully. I also did not see the stamp and
// seal animation when pressing the 'Hold Order' button. The Seal appeared but with no stamp animation."
// The REAL sorter page over the fake site (_reviewSendWorld.cjs), the order window's Hold Order (the piece row's, offSheets), with a stand-in for the hold engine that takes its time and reports
// each step as OrderHold.run does (start, sheetBegin, removed, fillBegin, fillFrom, fillPlaced, qr, sheetDone, held, done):
//   1  at once: a bar and the step it is on under the button, with the seconds; the order is held (marked) and the button is free ("In Review") while the sheets are still being put right
//   2  the ORDER HELD seal is stamped IN the window, at the press: the wooden stamp comes down on it, inside the open window, in sight
//   3  each step moves the bar and says where it is (taking it off the sheet, filling the free space, the QR label); it ends in a tick, "Done", and how long it took
//   6  RELEASED: once released, no ORDER HELD seal anywhere: the window and the Orders card show ORDER RELEASED, with the hold it ended ("held by … · when")
//   5  RELSTAMP: the window's own Release Hold, pressed: the RELEASE HOLD stamp comes down in the window, in sight (it came down on the Review card behind the window: heard, never seen)
//   PW_DIR=<playwright-core's node_modules> node tests/charm-nest/hold-order-progress.cjs   (CHROMIUM=<chrome>)
'use strict';
const assert = require('assert');
const W = require('./_reviewSendWorld.cjs');
const results = [];
const ok = (name, cond, extra) => { assert.ok(cond, name + (extra ? ' :: ' + extra : '')); results.push(name); console.log('  ✓ ' + name); };
const sleep = ms => new Promise(r => setTimeout(r, ms));

(async () => {
  const w = await W.open('production');
  if (!w) { console.log('hold-order-progress: skipped (playwright-core not found; set PW_DIR)'); return; }
  const { page } = w;
  try {
    await W.addOrders(w, ['sheeted', 'mate']);
    const sh = await W.seedSheet(w, { id: 'sh-hp1', orders: ['sheeted', 'mate'] });
    const rid = W.ORDERS.sheeted.rid, key = W.keyOf('sheeted'), pid = W.poolIdOf('sheeted');
    await page.evaluate(() => CN.setMode('orders'));
    await page.evaluate(k => OrderWin.open(k), key);
    const rb = `#owPcSum .revSendBtn[data-rev-host][data-rid="${rid}"]`;
    await page.waitForSelector(rb, { timeout: 20000 });
    // a stand-in engine that takes ~3 s and reports its steps as the real one does; the stamps and their tools are recorded where they land
    await page.evaluate(({ label, pid }) => {
      const T = window.__eng = { runs: [], real: window.OrderHold, done: false };
      const step = (o, s) => { s.at = Date.now(); if (o && typeof o.onStep === 'function') o.onStep(s); };
      window.OrderHold = Object.assign({}, T.real, {
        plan: async r => ({ rid: r, canHold: true, pieces: [{ poolId: pid, state: 'onSheet', sheetLabel: label }], sheets: [], fills: [], stays: [] }),
        run: async (r, o) => {
          T.runs.push({ r, name: o && o.name, onStep: typeof (o && o.onStep) === 'function' });
          const id = 'sh-hp1', pause = ms => new Promise(res => setTimeout(res, ms));
          step(o, { type: 'start', rid: r, sheets: [id] }); await pause(300);
          step(o, { type: 'sheetBegin', sheetId: id, label }); step(o, { type: 'lift', sheetId: id, poolIds: [pid] }); await pause(700);
          for (const row of Orders.rows()) if (String(row.order.receiptId) === r) { row.poolIds = []; row.state = 'held'; row.hold = row.reason = 'Taken off ' + label; }
          step(o, { type: 'removed', sheetId: id, removed: 1 }); await pause(300);
          step(o, { type: 'fillBegin', sheetId: id, spots: 1 }); step(o, { type: 'fillFrom', toSheetId: id, rid: '4170009999' }); await pause(700);
          step(o, { type: 'fillPlaced', sheetId: id, rid: '4170009999', poolIds: ['x'] }); await pause(300);
          step(o, { type: 'qr', sheetId: id }); await pause(300);
          step(o, { type: 'sheetDone', sheetId: id }); step(o, { type: 'held', rid: r }); step(o, { type: 'done' });
          T.done = true;
          return { ok: true, held: true, steps: [] };
        }
      });
      const S = window.Seal, real = S.press.bind(S);
      window.__stamps = []; window.__tools = []; window.__progTexts = [];
      setInterval(() => { const e = document.querySelector('#orderWin .revSendProg .rpTx b'); const t = e && e.textContent; if (t && window.__progTexts[window.__progTexts.length - 1] !== t) window.__progTexts.push(t); }, 30);
      S.press = seal => { const d = seal && seal.closest && seal.closest('dialog'); window.__stamps.push({ inWin: !!(d && d.id === 'orderWin'), kind: (seal.closest('[data-seal-kind]') || {}).dataset ? seal.closest('[data-seal-kind]').dataset.sealKind : '', at: Date.now() }); return real(seal); };
      new MutationObserver(ms => { for (const m of ms) for (const n of m.addedNodes) if (n.nodeType === 1 && n.classList.contains('sealTool')) { const r = n.getBoundingClientRect(), d = n.closest('dialog'); window.__tools.push({ inWin: !!(d && d.id === 'orderWin' && d.open), w: Math.round(r.width), at: Date.now() }); } }).observe(document, { childList: true, subtree: true });
    }, { label: sh.name, pid });
    const t0 = Date.now();
    await page.click(rb);
    // 1 · at once: the bar, the step, the seconds; the order held and the button free while the engine still runs
    await page.waitForFunction(s => !!document.querySelector('#orderWin .revSendProg'), null, { timeout: 5000 });
    const early = await page.evaluate(({ rid, rb }) => { const p = document.querySelector('#orderWin .revSendProg'); return { text: p.querySelector('.rpTx b').textContent, secs: p.querySelector('.rpTx small').textContent, val: +p.getAttribute('aria-valuenow'), role: p.getAttribute('role'), end: p.dataset.end || '' }; }, { rid, rb });
    ok('1a at the press a progress bar and the step it is on show under the button, with the seconds', early.role === 'progressbar' && /Holding the order|Order held|Taking/.test(early.text) && /^\d+ s$/.test(early.secs) && !early.end, JSON.stringify(early));
    await page.waitForFunction(r => CharmNestReviewMark.isMarked(r) && !ReviewSend.busy(r) && !window.__eng.done, rid, { timeout: 8000 });
    const mid = await page.evaluate(rb => ({ btn: document.querySelector(rb).textContent.trim(), busy: document.querySelector(rb).disabled }), rb);
    ok('1b the order is held (marked) and its button is free ("In Review") while the sheet is still being put right', mid.btn === 'In Review' && !mid.busy, JSON.stringify(mid) + ' after ' + (Date.now() - t0) + ' ms');
    if (process.env.SHOT) { await sleep(900); const b = await page.locator('#orderWin .revSendProg').boundingBox(); await page.screenshot({ path: process.env.SHOT + '-mid.png', clip: { x: Math.max(0, b.x - 520), y: Math.max(0, b.y - 70), width: 900, height: 130 } }); }
    // 2 · the ORDER HELD stamp, in the window, in sight
    await page.waitForFunction(() => window.__stamps.some(s => s.kind === 'hold' && s.inWin), null, { timeout: 8000 }).catch(() => {});
    await sleep(600);
    const st = await page.evaluate(() => ({ stamps: window.__stamps, tools: window.__tools }));
    ok('2 the ORDER HELD seal is stamped IN the order window at the press: the stamp comes down on it there, in sight', st.stamps.some(s => s.kind === 'hold' && s.inWin) && st.tools.some(t => t.inWin && t.w > 20), JSON.stringify(st));
    // 3 · the bar follows the steps and ends in a tick, "Done", and the time it took
    const seen = new Set();
    for (const t1 = Date.now(); Date.now() - t1 < 10000;) {
      const p = await page.evaluate(() => { const e = document.querySelector('#orderWin .revSendProg'); return e ? { text: e.querySelector('.rpTx b').textContent, val: +e.getAttribute('aria-valuenow'), end: e.dataset.end || '' } : null; });
      if (p) { seen.add(p.text.replace(/\d+/g, 'N')); if (p.end) break; }
      await sleep(80);
    }
    const end = await page.evaluate(() => { const e = document.querySelector('#orderWin .revSendProg'); return e && { text: e.querySelector('.rpTx b').textContent, secs: e.querySelector('.rpTx small').textContent, val: +e.getAttribute('aria-valuenow'), end: e.dataset.end, msg: [...document.querySelectorAll('#orderWin .revSendMsg')].map(n => n.textContent).join(' | ') }; });
    const words = [...new Set([...seen, ...(await page.evaluate(() => window.__progTexts)).map(x => x.replace(/\d+/g, 'N'))])];
    if (process.env.SHOT) { const b = await page.locator('#orderWin .revSendProg').boundingBox(); await page.screenshot({ path: process.env.SHOT + '-end.png', clip: { x: Math.max(0, b.x - 520), y: Math.max(0, b.y - 70), width: 900, height: 150 } }); }
    ok('3a each step moves the bar and says where it is: taken off the sheet, filling its free space, its QR label', words.some(x => /^Taken (it )?off /.test(x)) && words.some(x => /Filling/.test(x)) && words.some(x => /QR label/.test(x)), JSON.stringify(words));
    ok('3b it ends with a tick, "Done", the bar full, and the line says how long it took', end && end.end === 'ok' && end.val === 100 && end.text === 'Done' && /^\d+ s$/.test(end.secs) && /^Order held\. Taken off .+; it waits in Review\.$/.test(end.msg), JSON.stringify(end));
    // 5 · RELSTAMP (Paul, 27 Oct 2026: "When I released the hold on this particular order or any order from the detailed order modal I did not see the stamp or the seal. I only heard the noise"):
    //     the window's own Release Hold, pressed: the RELEASE HOLD stamp comes down IN the window, in sight, and the seal stays there beside the button that takes the place of Release Hold
    await page.evaluate(() => { window.OrderHold = window.__eng.real; });
    const relSel = '#orderWin [data-rel-hold]:not([aria-disabled="true"])';
    await page.waitForFunction(s => { const b = document.querySelector(s); return b && b.getBoundingClientRect().width > 0; }, relSel, { timeout: 15000 });
    const n0 = await page.evaluate(() => ({ s: window.__stamps.length, t: window.__tools.length }));
    await page.click(relSel);
    await page.waitForFunction(n0 => window.__stamps.slice(n0.s).some(s => s.kind === 'release' && s.inWin), n0, { timeout: 30000 }).catch(() => {});
    await sleep(800);
    const rel = await page.evaluate(n0 => { const g = [...document.querySelectorAll('#orderWin [data-ow-btn-seals] [data-seal-kind="release"]')].filter(e => e.getBoundingClientRect().width > 4); return { stamps: window.__stamps.slice(n0.s), tools: window.__tools.slice(n0.t), seals: g.length }; }, n0);
    ok('5 Release Hold pressed in the window: the RELEASE HOLD stamp comes down IN the window, in sight, and the seal stays there', rel.stamps.some(s => s.kind === 'release' && s.inWin) && rel.tools.some(t => t.inWin && t.w > 20) && rel.seals >= 1, JSON.stringify(rel));
    // 6 · RELEASED (Paul, 27 Oct 2026: "Once an order is released, it must be released everywhere and All hold Seals and UI must be updated in real-time to the Release Stamp/Seal ... so users know that a
    //     particular order was put on Hold in the past and then Released"): no ORDER HELD seal anywhere; the window and the Orders card show ORDER RELEASED, with the hold it ended
    const after = await page.evaluate(rid => { const T = CNSealTravel.of(rid), rel = T.find(r => r.kind === 'release') || {}; return { kinds: T.map(r => r.kind), held: rel.held || null, winHold: [...document.querySelectorAll('#orderWin [data-seal-kind="hold"]')].length, winRel: [...document.querySelectorAll('#orderWin [data-seal-kind="release"]')].length, face: (document.querySelector('#orderWin [data-seal-kind="release"] svg') || {}).textContent || '' }; }, rid);
    ok('6a released: the order\'s seals have no ORDER HELD any more; ORDER RELEASED stands in its place and knows the hold it ended', !after.kinds.includes('hold') && after.kinds.includes('release') && after.held && after.held.by === 'Test Operator', JSON.stringify(after));
    ok('6b the order window shows the ORDER RELEASED seal and no ORDER HELD seal', after.winHold === 0 && after.winRel >= 1 && /ORDER/.test(after.face) && /RELEASED/.test(after.face), JSON.stringify(after));
    await page.evaluate(() => { if (OrderWin.isOpen()) OrderWin.close(); }); await sleep(500);
    await page.evaluate(() => { CN.setMode('orders'); Orders.render(); });
    await page.waitForFunction(r => !!document.querySelector(`#ordItems [data-rid="${r}"] .relSeal`), rid, { timeout: 15000 });
    const card = await page.evaluate(r => { const n = document.querySelector(`#ordItems [data-rid="${r}"]`), s = n.querySelector('.relSeal'); return { words: s.querySelector('.relSealWords').innerText.replace(/\s+/g, ' ').trim(), face: (s.querySelector('svg') || {}).textContent || '', held: n.querySelectorAll('[data-seal-kind="hold"]').length }; }, rid);
    ok('6c the Orders card: ORDER RELEASED, by whom and when, and "held by …" under it; no ORDER HELD seal on the card', /^Order released by Test Operator .+ held by Test Operator · /.test(card.words) && /RELEASED/.test(card.face) && card.held === 0, JSON.stringify(card));
    ok('4 no script error on the page throughout', w.errors.length === 0, (w.errors || []).join(' | '));
  } finally { await w.close(); }
  console.log(`hold-order-progress: ${results.length} checks passed`);
})().catch(e => { console.error('FAIL:', e && e.message || e); process.exit(1); });
