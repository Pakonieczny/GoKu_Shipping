// ODB (Paul, 9 Nov 2026: "The 'Design Studio' button is missing from the Orders tab"): a gold "Design Studio" button in the Orders tab's own bar, beside Pull orders, that opens the
// "Design a charm" modal (charm-nest-ce-masters.js newDesign), the open path of the Design & Engrave tab's Charm Editor button, logged with from "ordersDesignStudio".
// The REAL page over the repo's fake site (_orderFactsWorld.cjs: offline, made-up orders, no Etsy, nothing live). What it proves:
//   1  the button is in #ordBar, the last control at its top right (CMP1, Paul 9 Nov 2026: it was right after Pull orders), says "Design Studio" with the pen nib, has the gold look (.cnCeBtn), is on, and the bar stays one slim row at 1500 px
//   2  hover fills it (white words), a keyboard focus outlines it, a press settles it (the .cnCeBtn states)
//   3  it is the same node, still last at the top right, after the list is drawn again (render, renderNow, a search, Cancelled and back)
//   4  pressed (a stand-in module), it calls newDesign once with from "ordersDesignStudio" and the way back "Back to Orders", logged designerNew:ordersDesignStudio
//   5  with the real charm-nest-ce-masters.js it opens the "Design a charm" modal on its New design side
//   6  the Sandbox workspace (settings sandbox "on", the page reloaded): the button is there at the top right, on, and opens the modal too
//   PW_DIR=<dir holding playwright-core> CHROMIUM=<chrome> node tests/charm-nest/orders-design-studio.cjs
'use strict';
const { world, seed } = require('./_orderFactsWorld.cjs');
const fails = []; let nCheck = 0;
const check = (ok, msg, ev) => { nCheck++; if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg + (ok || !ev ? '' : '  ' + ev)); };

const where = page => page.evaluate(() => {
  const b = document.getElementById('ordDesignBtn'), pull = document.getElementById('ordPull'), bar = document.getElementById('ordBar');
  if (!b) return { there: false };
  const r = b.getBoundingClientRect(), cs = getComputedStyle(b);
  return { there: true, n: document.querySelectorAll('#ordDesignBtn,.ordDesignBtn').length, inBar: bar.contains(b), afterPull: document.getElementById('ordTools').lastElementChild === b,   /* (CMP1, Paul 9 Nov 2026: the button moved to the top right corner, the last control of the head) */ text: b.textContent.trim(), icon: !!b.querySelector('svg'), on: !b.disabled,
    cls: b.className, shown: r.width > 0 && r.height > 0 && cs.visibility !== 'hidden' && cs.display !== 'none', h: Math.round(r.height), bg: cs.backgroundImage, radius: parseFloat(cs.borderRadius), cursor: cs.cursor,
    barH: Math.round(bar.getBoundingClientRect().height), level: Math.abs((r.top + r.bottom) / 2 - (pull.getBoundingClientRect().top + pull.getBoundingClientRect().bottom) / 2) <= 4 };
});

const realModal = async page => {
  await page.evaluate(() => { window.CharmNestCeMasters = window.__realM || window.CharmNestCeMasters; document.getElementById('ordDesignBtn').click(); });
  await page.waitForTimeout(700);
  const m = await page.evaluate(() => {
    const win = document.querySelector('.cem-win'); if (!win) return null;
    const on = win.querySelector('.cem-seg button.is-on');
    return { title: (win.querySelector('h1,h2,.cem-title') || {}).textContent || '', tabs: [...win.querySelectorAll('.cem-seg button')].map(x => x.textContent.trim()), newOpen: !!on && on.dataset.pane === 'new' };
  });
  await page.keyboard.press('Escape'); await page.waitForTimeout(400);
  const closed = await page.evaluate(() => !document.querySelector('.cem-win'));
  return { m, closed };
};

(async () => {
  const w = await world({ viewport: { width: 1500, height: 900 }, facts: false });
  if (!w) { console.log('SKIP: no browser (set PW_DIR and CHROMIUM)'); return; }
  const { page } = w;
  try {
    await seed(page, ['single', 'pairMatching', 'engraved']);
    await page.waitForTimeout(500);

    /* 1 · there, gold, beside Pull orders */
    const a = await where(page);
    check(a.there && a.n === 1 && a.inBar && a.afterPull && a.text === 'Design Studio' && a.icon && a.on && a.shown && /\bcnCeBtn\b/.test(a.cls) && /linear-gradient\(rgb\(255, 255, 255\)/.test(a.bg) && a.radius > 100 && a.cursor === 'pointer' && a.level && a.barH <= 44,
      `1 the "${a.text}" button is in the Orders bar, last at its top right (CMP1), gold (.cnCeBtn), on, level with Pull orders, ${a.h} px; the bar stays slim (${a.barH} px)`, JSON.stringify(a));

    /* 2 · hover, focus, pressed */
    await page.hover('#ordDesignBtn'); await page.waitForTimeout(350);
    const hov = await page.evaluate(() => { const b = document.getElementById('ordDesignBtn'), cs = getComputedStyle(b); return { fill: +getComputedStyle(b, '::before').opacity, color: cs.color, shadow: cs.boxShadow }; });
    await page.mouse.move(5, 5); await page.focus('#ordQ'); await page.keyboard.press('Shift');
    const foc = await page.evaluate(() => { const b = document.getElementById('ordDesignBtn'); b.focus(); const cs = getComputedStyle(b); const r = { fv: b.matches(':focus-visible'), outline: cs.outlineStyle + ' ' + cs.outlineWidth }; b.blur(); return r; });
    const sheet = await page.evaluate(() => [...document.styleSheets].some(s => { try { return [...s.cssRules].some(r => /\.btn\.cnCeBtn:active:not\(:disabled\)/.test(r.selectorText || '')); } catch (_) { return false; } }));
    check(hov.fill > 0.95 && hov.color === 'rgb(255, 255, 255)' && hov.shadow !== 'none' && foc.fv && foc.outline === 'solid 2px' && sheet,
      `2 hovered it fills with deep ink (${hov.fill}, white words); focused from the keyboard it is outlined (${foc.outline}); its pressed state is .cnCeBtn:active`, JSON.stringify({ hov, foc, sheet }));

    /* 3 · the same node through redraws */
    const n0 = await page.evaluateHandle(() => document.getElementById('ordDesignBtn'));
    await page.evaluate(() => { Orders.render(); Orders.renderNow(); });
    await page.fill('#ordQ', 'zzz-nothing'); await page.keyboard.press('Enter'); await page.waitForTimeout(400);
    await page.fill('#ordQ', ''); await page.keyboard.press('Enter'); await page.waitForTimeout(400);
    await page.evaluate(() => Orders.showPile('cancelled')); await page.waitForTimeout(500);
    const cx = await where(page);
    await page.evaluate(() => Orders.showPile('')); await page.waitForTimeout(500);
    await page.evaluate(() => { Orders.render(); Orders.renderNow(); }); await page.waitForTimeout(300);
    const same = await page.evaluate(n => n === document.getElementById('ordDesignBtn') && document.getElementById('ordTools').lastElementChild === n && document.querySelectorAll('#ordDesignBtn').length === 1, n0);
    check(same && cx.there && cx.afterPull && cx.shown, '3 the same node, still last at the top right of the head, after the list is drawn again (render, renderNow, a search, Cancelled and back); shown on Cancelled too', JSON.stringify({ same, cx }));

    /* 4 · the press: newDesign with from ordersDesignStudio, logged */
    await page.evaluate(() => {
      window.__log = []; window.CNDesignLog = { event: (kind, data) => window.__log.push([kind, data && data.from]) };
      window.__calls = []; window.__realM = window.CharmNestCeMasters;
      window.CharmNestCeMasters = { newDesign: o => { window.__calls.push(['new', o && o.from, o && o.back && o.back.label]); }, openPicker: () => {}, openDesign: () => {} };
    });
    await page.click('#ordDesignBtn'); await page.waitForTimeout(150);
    const pr = await page.evaluate(() => ({ calls: window.__calls, log: window.__log }));
    check(JSON.stringify(pr.calls) === JSON.stringify([['new', 'ordersDesignStudio', 'Back to Orders']]) && JSON.stringify(pr.log) === JSON.stringify([['designerNew', 'ordersDesignStudio']]),
      '4 pressed, it calls newDesign once (from "ordersDesignStudio", back "Back to Orders"), logged designerNew:ordersDesignStudio', JSON.stringify(pr));

    /* 5 · the real modal */
    const real = await realModal(page);
    check(!!real.m && real.m.tabs.join('|') === 'From the library|New design' && real.m.newOpen && real.closed,
      `5 with the real charm-nest-ce-masters.js it opens the "Design a charm" modal (${real.m && real.m.tabs.join(' / ')}) on New design; Esc closes it`, JSON.stringify(real));
    check(w.errors.length === 0, '  no page errors (Real): ' + w.errors.join(' | '));

    /* 6 · the Sandbox workspace */
    await page.evaluate(() => { const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); s.sandbox = 'on'; localStorage.setItem('cn.settings', JSON.stringify(s)); });
    await page.reload({ waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Sandbox && document.getElementById('modeSeg'), null, { timeout: 60000 });
    await page.evaluate(() => { CN.setMode('orders'); Orders.renderNow(); }); await page.waitForTimeout(800);
    const sb = await page.evaluate(() => ({ on: Sandbox.on(), cls: document.documentElement.classList.contains('sandbox') }));
    const s1 = await where(page);
    const sReal = await realModal(page);
    check(sb.on && s1.there && s1.n === 1 && s1.afterPull && s1.shown && s1.on && s1.text === 'Design Studio' && !!sReal.m && sReal.m.newOpen,
      `6 Sandbox workspace (${JSON.stringify(sb)}): the button is at the top right of the head, on, and opens the "Design a charm" modal`, JSON.stringify({ sb, s1, sReal }));
  } catch (e) { check(false, 'the test itself failed: ' + (e && e.stack || e)); }
  await w.close();
  console.log(fails.length ? `\nFAIL: ${fails.length} of ${nCheck}\n - ` + fails.join('\n - ') : `\nALL ${nCheck} PASS`);
  if (fails.length) process.exitCode = 1;
})();
