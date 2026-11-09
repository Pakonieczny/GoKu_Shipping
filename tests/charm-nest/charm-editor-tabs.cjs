// CEB1 (Paul, 9 Nov 2026): "1. Rename the Design Studio button to the Charm Editor button and name. 2. Resize the new renamed button to be the same size as the Charm Editor button. 3. I noticed
// that the Charm Editor button doesn't exist in the Review tab. Place it exactly in the same place on all 5 main tabs just like it is in the Design & Engrave."
// CEB2 (Paul, 9 Nov 2026): "Remove the Charm Editor button from the Nest tab." So: on the four other tabs as CEB1 put it; on Nest none, and no header row left for it (0  below).
// ONE "Charm Editor" button (charm-nest-bridge.js CharmEditorTabBtn, charm-nest-activity.css .cnCeTabBtn over the .cnCeBtn look) on Nest, Orders, Design & Engrave, Review and Library.
// The REAL page over the repo's fake site (_orderFactsWorld.cjs: offline, made-up orders, no Etsy, no paid AI, nothing live). What it proves:
//   0  on Nest: no Charm Editor button in the page's DOM for Nest, no #nestCeHead / .cnCeHead row, the sheet card (#sheets) with nothing put before it for the button
//   1  at 1400 px each of the four tabs has exactly ONE visible "Charm Editor" button, the same class, words, aria-label and tooltip, the last thing of the tab's header row, and the same box
//      (size, right and top offset from the content area) as the Design & Engrave tab's, within 1 px
//   2  no "Design Studio" anywhere in the page's words, tooltips or labels, on any of the five tabs
//   3  it is the same node, still last in its row, after each tab is drawn again (Orders, Design & Engrave, Review re-rendered; Library's Completed / Sheets switched; Nest left and shown again)
//   4  pressed on each tab, it opens the "Design a charm" modal's New design (newDesign once, from "charmEditorButton", the way back to THAT tab), logged designerNew with its tab
//   5  with the real modules: pressed on Review, the "Design a charm" modal is in view on top; with another modal window opened over the page meanwhile, Start designing opens the Charm Editor
//      in FRONT of it (CES1: the top of the top layer, what a click at its middle reaches)
//   6  at 390 px (a phone, the rail folded) the button is whole on the screen on every tab and the page does not scroll sideways
//   PW_DIR=<dir holding playwright-core> CHROMIUM=<chrome> node tests/charm-nest/charm-editor-tabs.cjs
'use strict';
const { world, seed } = require('./_orderFactsWorld.cjs');
const fails = []; let nCheck = 0;
const check = (ok, msg, ev) => { nCheck++; if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg + (ok || !ev ? '' : '  ' + ev)); };
const TABS = ['orders', 'engrave', 'review', 'library'];
const ROW = { orders: '#ordTools', engrave: '#engraveView > .egBar', review: '#reviewView > .ordBar', library: '#libView .libBar' };

const show = async (page, m, narrow) => {
  await page.evaluate(m => CN.setMode(m), m);
  if (narrow) await page.evaluate(() => document.getElementById('app').classList.add('railOff'));
  await page.waitForTimeout(900);
};
const look = (page, m) => page.evaluate(([m, rowSel]) => {
  const st = document.getElementById('stage'), sr = st.getBoundingClientRect();
  const vis = b => { const r = b.getBoundingClientRect(), cs = getComputedStyle(b); return r.width > 0 && r.height > 0 && cs.display !== 'none' && cs.visibility !== 'hidden' && !b.closest('.hidden'); };
  const all = [...document.querySelectorAll('button')].filter(b => vis(b) && /charm editor/i.test(b.textContent) && b.closest('#stage') && !b.closest('dialog,.cem-root'));
  const b = all[0], row = document.querySelector(rowSel);
  if (!b) return { n: 0 };
  const r = b.getBoundingClientRect();
  return { n: all.length, id: b.id, tab: b.dataset.ceTab, cls: [...b.classList].filter(c => c !== 'egDesignBtn').sort().join(' '), text: b.textContent.trim(), aria: b.getAttribute('aria-label'), title: b.title, on: !b.disabled,
    last: !!row && row.lastElementChild === b, box: { w: r.width, h: r.height, right: sr.right - r.right, top: r.top - sr.top }, left: r.left, rightPx: r.right, vw: innerWidth,
    hscroll: document.documentElement.scrollWidth > document.documentElement.clientWidth };
}, [m, ROW[m]]);
const near = (a, b) => ['w', 'h', 'right', 'top'].every(k => Math.abs(a[k] - b[k]) <= 1);
const fmt = x => `${x.w.toFixed(1)}×${x.h.toFixed(1)} px, ${x.right.toFixed(1)} px from the right, ${x.top.toFixed(1)} px from the top`;

(async () => {
  const w = await world({ viewport: { width: 1400, height: 900 }, facts: false });
  if (!w) { console.log('SKIP: no browser (set PW_DIR and CHROMIUM)'); return; }
  const { page } = w;
  try {
    await seed(page, ['single', 'pairMatching', 'engraved']);
    await page.waitForTimeout(500);

    /* 0 · not on Nest */
    const nestNone = async () => { await show(page, 'nest'); return page.evaluate(() => {
      const sh = document.getElementById('sheets'), prev = sh && sh.previousElementSibling;
      const vis = [...document.querySelectorAll('button')].filter(b => { const r = b.getBoundingClientRect(); return r.width > 0 && r.height > 0 && /charm editor/i.test(b.textContent) && b.closest('#stage') && !b.closest('dialog,.cem-root,.hidden'); });
      return { visible: vis.length, nestBtn: !!document.querySelector('#nestCeBtn,.cnCeTabBtn[data-ce-tab="nest"]'), head: !!document.querySelector('#nestCeHead,.cnCeHead'), sheets: !!sh,
        before: prev ? (prev.id || prev.className || prev.tagName) : null, beforeHasBtn: !!(prev && prev.querySelector && prev.querySelector('.cnCeTabBtn')) };
    }); };
    const n0 = await nestNone();
    check(n0.sheets && n0.visible === 0 && !n0.nestBtn && !n0.head && !n0.beforeHasBtn, '0 Nest: no Charm Editor button and no header row for it; the sheet card is the top of the tab', JSON.stringify(n0));

    /* 1 · one button, the same, in the same place */
    const at = {}, nodes = {};
    for (const m of TABS) { await show(page, m); at[m] = await look(page, m); nodes[m] = await page.evaluateHandle(sel => { const r = document.querySelector(sel); return r && r.lastElementChild; }, ROW[m]); }
    const ref = at.engrave;
    for (const m of TABS) {
      const a = at[m];
      check(a.n === 1 && a.tab === m && a.cls === ref.cls && /\bcnCeBtn\b/.test(a.cls) && /\bcnCeTabBtn\b/.test(a.cls) && a.text === 'Charm Editor' && a.aria === 'Charm Editor' && /Charm Editor/.test(a.title) && a.on && a.last && near(a.box, ref.box),
        `1 ${m}: one "${a.text}" button (${a.cls}), the last thing of its header row, ${a.box ? fmt(a.box) : '—'} (Design & Engrave: ${fmt(ref.box)})`, JSON.stringify(a));
    }

    /* 2 · no "Design Studio" in the UI */
    const ds = [];
    for (const m of TABS) {
      await show(page, m);
      ds.push(...await page.evaluate(m => {
        const hits = []; if (/Design Studio/i.test(document.body.innerText)) hits.push(m + ': words');
        for (const n of document.querySelectorAll('[title],[aria-label],[placeholder]')) for (const k of ['title', 'aria-label', 'placeholder']) if (/Design Studio/i.test(n.getAttribute(k) || '')) hits.push(`${m}: ${k} of ${n.id || n.className}`);
        return hits;
      }, m));
    }
    check(!ds.length, `2 no "Design Studio" in the words, tooltips or labels of the five tabs`, JSON.stringify(ds));

    /* 3 · the same node after each tab is drawn again */
    const again = {};
    await show(page, 'orders'); await page.evaluate(() => { Orders.render(); Orders.renderNow(); Orders.showPile('cancelled'); }); await page.waitForTimeout(400);
    await page.evaluate(() => Orders.showPile('')); await page.waitForTimeout(400);
    await show(page, 'engrave'); await page.evaluate(() => Engrave.render()); await page.waitForTimeout(400);
    await show(page, 'review'); await page.evaluate(() => { Review.render(); }); await page.waitForTimeout(400);
    await page.evaluate(() => { const b = document.querySelector('#reviewView .rvSeg [data-cseg="done"]'); if (b) b.click(); }); await page.waitForTimeout(400);
    await page.evaluate(() => { const b = document.querySelector('#reviewView .rvSeg [data-cseg="open"]'); if (b) b.click(); }); await page.waitForTimeout(400);
    await show(page, 'library');
    await page.evaluate(() => { const b = document.querySelector('#libTab [data-t="done"]'); if (b) b.click(); }); await page.waitForTimeout(500);
    await page.evaluate(() => { const b = document.querySelector('#libTab [data-t="current"]'); if (b) b.click(); }); await page.waitForTimeout(500);
    await page.evaluate(() => { const b = document.querySelector('#libKind [data-k="sheets"]'); if (b) b.click(); }); await page.waitForTimeout(500);
    await page.evaluate(() => { const b = document.querySelector('#libKind [data-k="sets"]'); if (b) b.click(); }); await page.waitForTimeout(500);
    await show(page, 'orders'); await show(page, 'nest');
    for (const m of TABS) {
      await show(page, m);
      again[m] = await page.evaluate(([n, sel]) => { const r = document.querySelector(sel); return { same: !!r && r.lastElementChild === n && n.isConnected, n: document.querySelectorAll('.cnCeTabBtn[data-ce-tab]').length }; }, [nodes[m], ROW[m]]);
      const a = await look(page, m);
      again[m].box = a.box; again[m].one = a.n === 1;
    }
    const n3 = await nestNone();
    check(TABS.every(m => again[m].same && again[m].one && near(again[m].box, ref.box)) && TABS.every(m => again[m].n === 4) && n3.visible === 0 && !n3.nestBtn && !n3.head,
      '3 each tab keeps the same node, last in its row, in the same place, after it is drawn again (4 buttons in the page, one a tab; still none on Nest after the round)', JSON.stringify({ again, n3 }));

    /* 4 · the press on each tab */
    await page.evaluate(() => {
      window.__log = []; window.CNDesignLog = { event: (kind, data) => window.__log.push([kind, data && data.from, data && data.tab]) };
      window.__calls = []; window.__realM = window.CharmNestCeMasters;
      window.CharmNestCeMasters = { newDesign: o => { window.__calls.push([o && o.from, o && o.tab, o && o.back && o.back.label]); }, openPicker: () => {}, openDesign: () => {} };
    });
    for (const m of TABS) { await show(page, m); await page.click(`.cnCeTabBtn[data-ce-tab="${m}"]`); await page.waitForTimeout(150); }
    const pr = await page.evaluate(() => ({ calls: window.__calls, log: window.__log }));
    const want = [['charmEditorButton', 'orders', 'Back to Orders'], ['charmEditorButton', 'engrave', 'Back to Design & Engrave'], ['charmEditorButton', 'review', 'Back to Review'], ['charmEditorButton', 'library', 'Back to Library']];
    check(JSON.stringify(pr.calls) === JSON.stringify(want) && JSON.stringify(pr.log) === JSON.stringify(TABS.map(m => ['designerNew', 'charmEditorButton', m])),
      '4 pressed on each tab, it opens "Design a charm" on New design once (newDesign from "charmEditorButton", the way back to that tab), logged designerNew with its tab', JSON.stringify(pr));
    await page.evaluate(() => { window.CharmNestCeMasters = window.__realM; });

    /* 5 · the real modal, and the Charm Editor in front */
    await show(page, 'review');
    await page.click('#rvCeBtn');
    await page.waitForFunction(() => !!document.querySelector('.cem-root [data-panel="new"]:not([hidden])'), null, { timeout: 15000 });
    const pick = await page.evaluate(() => {
      const win = document.querySelector('.cem-win'), r = win.getBoundingClientRect(), hit = document.elementFromPoint(r.left + r.width / 2, r.top + 30);
      const on = win.querySelector('.cem-seg button.is-on');
      return { title: win.querySelector('.cem-id h2').textContent, onTop: !!hit && win.contains(hit), newOpen: !!on && on.dataset.pane === 'new' };
    });
    await page.click('.cem-shape[data-shape="circle"]');
    await page.fill('.cem-root [data-cf="w"]', '15'); await page.locator('.cem-root [data-cf="w"]').press('Tab'); await page.waitForTimeout(300);
    // another window opens over the page meanwhile (a modal <dialog>, as the order window is): the editor must still open in front of it
    await page.evaluate(() => { const d = document.createElement('dialog'); d.id = '__other'; d.style.cssText = 'width:90vw;height:90vh;max-width:none;max-height:none'; d.textContent = 'another window'; document.body.appendChild(d); d.showModal(); });
    await page.evaluate(() => document.querySelector('.cem-root [data-act="start"]').click());
    await page.waitForFunction(() => !!(window.CharmEditor && CharmEditor.current), null, { timeout: 30000 });
    await page.waitForTimeout(600);
    const ed = await page.evaluate(() => {
      const layer = document.querySelector('dialog.ce-layer[open]'), root = document.querySelector('.ce-root'), r = root && root.getBoundingClientRect();
      const hit = r && document.elementFromPoint(r.left + r.width / 2, r.top + r.height / 2);
      const other = document.getElementById('__other');
      return { layer: !!layer, otherOpen: !!other && other.open, inFront: !!hit && !!root && root.contains(hit), notOther: !!hit && !other.contains(hit), back: (document.querySelector('.ce-root [data-act="back"], .ce-root .ce-back') || {}).textContent || '' };
    });
    check(pick.title === 'Design a charm' && pick.onTop && pick.newOpen && ed.layer && ed.otherOpen && ed.inFront && ed.notOther,
      `5 pressed on Review, "${pick.title}" opens on New design, in view on top; Start designing, with another modal window open over the page, opens the Charm Editor in front of it (CES1)`, JSON.stringify({ pick, ed }));
    check(w.errors.length === 0, '  no page errors: ' + w.errors.join(' | '));
  } catch (e) { check(false, 'the test itself failed: ' + (e && e.stack || e)); }
  await w.close();

  /* 6 · a phone */
  const p = await world({ viewport: { width: 390, height: 844 }, facts: false, touch: true });
  try {
    await seed(p.page, ['single', 'pairMatching', 'engraved']); await p.page.waitForTimeout(400);
    const ph = {};
    for (const m of TABS) { await show(p.page, m, true); const a = await look(p.page, m); ph[m] = { n: a.n, left: a.left && +a.left.toFixed(1), right: a.rightPx && +a.rightPx.toFixed(1), h: a.box && a.box.h, hscroll: a.hscroll }; }
    await show(p.page, 'nest', true); const phNest = await look(p.page, 'nest');
    check(phNest.n === 0, '6 at 390 px Nest has no Charm Editor button', JSON.stringify(phNest));
    check(TABS.every(m => ph[m].n === 1 && ph[m].left >= 0 && ph[m].right <= 390 && Math.abs(ph[m].h - 40) <= 1 && !ph[m].hscroll),
      '6 at 390 px the button is whole on the screen on every tab (40 px tall: a finger target, the same on every tab), and the page does not scroll sideways', JSON.stringify(ph));
    check(!p.errors.length, '6 no page errors on the phone: ' + p.errors.join(' | '));
  } catch (e) { check(false, 'the phone test failed: ' + (e && e.stack || e)); }
  await p.close();
  console.log(fails.length ? `\nFAIL: ${fails.length} of ${nCheck}\n - ` + fails.join('\n - ') : `\nALL ${nCheck} PASS`);
  if (fails.length) process.exitCode = 1;
})();
