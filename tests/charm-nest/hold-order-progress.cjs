// HOLDPROG / HOLDSTAMP (Paul, 27 Oct 2026, order 4209777939): "the whole process completed successfully, but it took over a minute to complete which is really bad and really long and there was
// no feedback to the user. No progress bar know anything to let us know what was happening and when it would've finish and actually did it complete successfully. I also did not see the stamp and
// seal animation when pressing the 'Hold Order' button. The Seal appeared but with no stamp animation."
// (HOLDSTAYS, Paul 28 Oct 2026: "The 'Hold' button should NOT remove a given charm off a sheet": the press only marks the order, so the bar of the take-off is gone.)
// The REAL sorter page over the fake site (_reviewSendWorld.cjs), the order window's Hold Order:
//   1  at once: the order is held (marked), its button "In Review", the line under it says it stays on its sheet; nothing is taken off (the hold engine is never asked, no bar)
//   2  the ORDER HELD seal is stamped IN the window, at the press: the wooden stamp comes down on it, inside the open window, in sight
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
    // (HOLDSTAYS, Paul 28 Oct 2026: Hold Order takes nothing off a sheet: the press only marks the order, so there is no long run and no progress bar any more. The engine is watched to prove it is never asked.)
    await page.evaluate(() => {
      const T = window.__eng = { runs: 0, real: window.OrderHold };
      window.OrderHold = Object.assign({}, T.real, { plan: async (...a) => { T.runs++; return T.real.plan(...a); }, run: async (...a) => { T.runs++; return T.real.run(...a); } });
      const S = window.Seal, real = S.press.bind(S);
      window.__stamps = []; window.__tools = [];
      S.press = seal => { const d = seal && seal.closest && seal.closest('dialog'); window.__stamps.push({ inWin: !!(d && d.id === 'orderWin'), kind: (seal.closest('[data-seal-kind]') || {}).dataset ? seal.closest('[data-seal-kind]').dataset.sealKind : '', at: Date.now() }); return real(seal); };
      new MutationObserver(ms => { for (const m of ms) for (const n of m.addedNodes) if (n.nodeType === 1 && n.classList.contains('sealTool')) { const r = n.getBoundingClientRect(), d = n.closest('dialog'); window.__tools.push({ inWin: !!(d && d.id === 'orderWin' && d.open), w: Math.round(r.width), at: Date.now() }); } }).observe(document, { childList: true, subtree: true });
    });
    const t0 = Date.now();
    await page.click(rb);
    // 1 · at once: the order held, its button "In Review", the line under it says the piece stays on its sheet; no bar, the engine never asked
    await page.waitForFunction(r => CharmNestReviewMark.isMarked(r) && !ReviewSend.busy(r), rid, { timeout: 8000 });
    await page.waitForFunction(() => /Stays on/.test([...document.querySelectorAll('#orderWin .revSendMsg')].map(n => n.textContent).join(' ')), null, { timeout: 8000 }).catch(() => {});
    const mid = await page.evaluate(rb => ({ btn: document.querySelector(rb).textContent.trim(), busy: document.querySelector(rb).disabled, bar: !!document.querySelector('#orderWin .revSendProg'), msg: [...document.querySelectorAll('#orderWin .revSendMsg')].map(n => n.textContent).join(' | '), runs: window.__eng.runs }), rb);
    ok('1 the press holds the order at once ("In Review"), says it stays on its sheet, and takes nothing off (no bar, the hold engine never asked)', mid.btn === 'In Review' && !mid.busy && !mid.bar && mid.runs === 0 && /^Order held\. Stays on .+ held, with its set, until this is released in Review\.$/.test(mid.msg), JSON.stringify(mid) + ' after ' + (Date.now() - t0) + ' ms');
    // 2 · the ORDER HELD stamp, in the window, in sight
    await page.waitForFunction(() => window.__stamps.some(s => s.kind === 'hold' && s.inWin), null, { timeout: 8000 }).catch(() => {});
    await sleep(600);
    const st = await page.evaluate(() => ({ stamps: window.__stamps, tools: window.__tools }));
    ok('2 the ORDER HELD seal is stamped IN the order window at the press: the stamp comes down on it there, in sight', st.stamps.some(s => s.kind === 'hold' && s.inWin) && st.tools.some(t => t.inWin && t.w > 20), JSON.stringify(st));
    // 5 · RELSTAMP (Paul, 27 Oct 2026: "When I released the hold on this particular order or any order from the detailed order modal I did not see the stamp or the seal. I only heard the noise"):
    //     the window's own Release Hold, pressed: the RELEASE HOLD stamp comes down IN the window, in sight, and the seal stays there beside the button that takes the place of Release Hold
    await page.evaluate(() => { window.OrderHold = window.__eng.real; });
    const relSel = '#orderWin [data-rel-hold]:not([aria-disabled="true"])';
    await page.waitForFunction(s => { const b = document.querySelector(s); return b && b.getBoundingClientRect().width > 0; }, relSel, { timeout: 15000 });
    await page.evaluate(() => window.Seal && Seal.whenIdle && Seal.whenIdle());   // (the ORDER HELD stamp has finished: a person presses Release Hold after it)
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
