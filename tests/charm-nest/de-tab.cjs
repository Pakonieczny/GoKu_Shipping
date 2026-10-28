// DESIGN & ENGRAVE · the tab (Paul, 28 Oct 2026: "What this also means is that in the charm editor users will be able to edit orders that may not have a requirement for back engraving. Since
// this is all part of the back engraving tab, I would suggest we rename the tab "Design & Engrave". And the Charm Editor will be its own entity within that new updated tab that will be
// accessible from the back engraving modal and vice versa from the charm editor back into the back engraving modal ... Also, the Charm Editor button in the back engraving Modal is way too
// small and hidden, and should be a lot more visible and prominent and easy to click"). Items 30-33 and the lead's D11.
// The real sorter page over the repo's fake site (_engraveRealWorld.cjs: the real intake, pool, classifier and fit; offline, made-up names), a front + back necklace ("both") and a stud earring
// pair that is never engraved ("earrings"):
//   T1  the tab reads "Design & Engrave" where the bar has room and "Design" where it has not; its mode and its link stay "engrave" (#engrave)
//   T2  (DESIGNBTN, Paul 28 Oct 2026: "Remove this entire menu from the UI" ... "Add a new button called 'Charm Editor' that opens the Design a Charm Modal") the Charm Designer bar
//       that stood first in the tab is gone; the "Charm Editor" button is the last thing in the tab's own bar, at its right end, the same node through the tab's redraws: gold, 36 px or
//       more, a darker hover and a keyboard outline, whole on a 390 px phone; pressed, it calls charm-nest-ce-masters.js newDesign (a stand-in, then the real "Design a charm" modal), logged
//   T3  the engraving card's "Open in Charm Editor": one big gold button in the card's head beside its ×
//   T4  a piece that is NOT engraved opens in the editor from its order window ("Open in Charm Editor" under the vector design): "piece 1 of 2 · no engraving", "Back to order"; a line drawn and
//       Save keep it for that piece (Charm_Pool_Art: who, what) and say so under the button; Back to order leaves the order window as it was; a piece on no sheet of this page opens from its master
//   T5  "Back to engraving": from the card, the editor closes on that piece's card; from the order window of an engraved piece, the order window is put away and its engraving card is in front;
//       with a change made, Back asks first (Keep editing / Discard changes / Save and go back)
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/de-tab.cjs      (SHOTS=<dir> for the screenshots)
'use strict';
const path = require('node:path');
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const R = require('./_engraveRealWorld.cjs');

(async () => {
  const shots = process.env.SHOTS || '/tmp/de-tab-shots';
  const w = await R.world({ shots, viewport: { width: 1440, height: 900 } });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page, st = w.srv.st;
  const settle = ms => R.settle(page, ms);
  try {
    const made = await R.build(w, ['both', 'earrings']);
    check('T0 the world: the necklace has its front and back jobs, the earring pair two pieces and no job', made.both.jobs.length === 2 && made.earrings.jobs.length === 0 && made.earrings.poolIds.length === 2, JSON.stringify({ both: made.both.jobs.map(j => j.state), ear: made.earrings.poolIds }));

    /* ── T1 · the tab's name ── */
    const tabAt = async width => {
      await page.setViewportSize({ width, height: 900 }); await settle(300);
      return page.evaluate(() => { const b = document.querySelector('#modeSeg button[data-mode="engrave"]'), vis = e => !!e && getComputedStyle(e).display !== 'none'; return { mode: b.dataset.mode, shown: [...b.querySelectorAll('.full,.tight')].filter(vis).map(e => e.textContent).join('|'), text: b.textContent.replace(/\d+$/, '').trim(), hash: location.hash, w: Math.round(b.getBoundingClientRect().width) }; });
    };
    await page.evaluate(() => { CN.setMode('orders'); }); await settle(200);
    await page.click('#modeSeg button[data-mode="engrave"]'); await settle(500);
    const wide = await tabAt(1440), narrow = await tabAt(1100);
    await page.setViewportSize({ width: 1440, height: 900 }); await settle(300);
    check(`T1 the tab reads "${wide.shown}" (${wide.w} px) at 1440 px and "${narrow.shown}" (${narrow.w} px) at 1100 px; its mode and link stay engrave (${wide.hash}); "Engraving" is gone from it`,
      wide.shown === 'Design & Engrave' && narrow.shown === 'Design' && wide.mode === 'engrave' && wide.hash === '#engrave' && !/Engraving/.test(wide.text), JSON.stringify({ wide, narrow }));

    /* ── T2 · the Charm Editor button (DESIGNBTN, Paul 28 Oct 2026: "Remove this entire menu from the UI" ... "Add a new button called 'Charm Editor' that opens the Design a Charm Modal") ── */
    await page.evaluate(() => {
      window.__log = []; window.CNDesignLog = { event: (kind, data) => window.__log.push([kind, data]) };
      window.__calls = []; window.__realM = window.CharmNestCeMasters; delete window.CharmNestCeMasters;   // (the real module is put back for T2e)
      Engrave.restoreView({ tab: 'place', list: true, chosen: true, focus: null }); Engrave.render();
    });
    await settle(300);
    const off = await page.evaluate(() => { const v = document.getElementById('engraveView'), eb = v.querySelector(':scope > .egBar'), b = document.getElementById('egDesignBtn'); return { bar: !!document.getElementById('cdBar') || !!document.querySelector('.cdBar'), words: /Charm Designer|design a new charm|Open charm|RECENT/i.test(v.textContent), first: v.firstElementChild === eb, last: !!b && eb.lastElementChild === b, disabled: !!b && b.disabled, title: b && b.title }; });
    check('T2a the Charm Designer bar is gone (no bar, none of its words, the tab\'s own bar first in the tab); the Charm Editor button is last in that bar, off while charm-nest-ce-masters.js is not on the page',
      !off.bar && !off.words && off.first && off.last && off.disabled && /still loading/.test(off.title || ''), JSON.stringify(off));
    await page.evaluate(() => {
      window.CharmNestCeMasters = { newDesign: o => { window.__calls.push(['new', o && o.from, o && o.back && o.back.label]); }, openPicker: o => { window.__calls.push(['open', o && o.from]); }, openDesign: (id, v) => { window.__calls.push(['design', id, v]); } };   // (a stand-in)
      Engrave.render();
    });
    await settle(300);
    const geo = () => page.evaluate(() => {
      const b = document.getElementById('egDesignBtn'), eb = document.querySelector('#engraveView > .egBar'), r = b.getBoundingClientRect(), e = eb.getBoundingClientRect(), t = eb.querySelector('.egTab[data-tab="place"]').getBoundingClientRect(), cs = getComputedStyle(b), ecs = getComputedStyle(eb);
      return { text: b.textContent.trim(), icon: !!b.querySelector('svg'), on: !b.disabled, w: Math.round(r.width), h: Math.round(r.height), font: parseFloat(cs.fontSize), bg: cs.backgroundImage, radius: parseFloat(cs.borderRadius), cursor: cs.cursor, left: Math.round(r.left), right: Math.round(r.right), barRight: Math.round(e.right - parseFloat(ecs.paddingRight)), oneRow: Math.abs((r.top + r.bottom) / 2 - (t.top + t.bottom) / 2) <= 8, lines: Math.round(b.querySelector('span').getBoundingClientRect().height / (parseFloat(cs.lineHeight) || 16)), vw: document.documentElement.clientWidth, sw: document.documentElement.scrollWidth, barOver: eb.scrollWidth - eb.clientWidth };
    });
    const g = await geo();
    await R.shot(w, 'DE-1-charm-editor-button');
    await page.hover('#egDesignBtn'); await settle(250);
    const hov = await page.evaluate(() => { const b = document.getElementById('egDesignBtn'), cs = getComputedStyle(b), fill = getComputedStyle(b, '::before'); return { bg: fill.backgroundImage.slice(0, 40), fill: +fill.opacity, color: cs.color, shadow: cs.boxShadow }; });
    { const e = await page.evaluate(() => { const r = document.querySelector('#engraveView > .egBar').getBoundingClientRect(); return { x: Math.max(0, r.left - 8), y: Math.max(0, r.top - 8), width: Math.min(r.width + 16, document.documentElement.clientWidth), height: r.height + 16 }; }); await R.shot(w, 'DE-1b-charm-editor-hover', { clip: e }); }
    await page.mouse.move(5, 5); await page.focus('#egQ'); await page.keyboard.press('Shift');
    const foc = await page.evaluate(() => { const b = document.getElementById('egDesignBtn'); b.focus(); const cs = getComputedStyle(b); return { fv: b.matches(':focus-visible'), outline: cs.outlineStyle + ' ' + cs.outlineWidth }; });
    await page.evaluate(() => document.getElementById('egDesignBtn').blur());
    check(`T2b the "${g.text}" button (${g.w}x${g.h} px, ${g.font} px words, the pen nib) is the white pill with a gold outline (CEBTN), on, at the right end of the tab's bar on the Placements / Approved row (right edge ${g.right}, the bar's ${g.barRight}); hovered it fills with deep ink (${hov.fill}, white words), focused from the keyboard it is outlined (${foc.outline})`,
      g.text === 'Charm Editor' && g.icon && g.on && g.h >= 36 && g.font >= 13 && /linear-gradient\(rgb\(255, 255, 255\)/.test(g.bg) && g.radius > 100 && g.cursor === 'pointer' && Math.abs(g.right - g.barRight) <= 2 && g.oneRow && g.lines <= 1 && g.barOver <= 1 && g.sw <= g.vw + 1
        && hov.fill > 0.95 && hov.color === 'rgb(255, 255, 255)' && hov.shadow !== 'none' && foc.fv && /^solid 2px$/.test(foc.outline), JSON.stringify({ g, hov, foc }));
    const node0 = await page.evaluateHandle(() => document.getElementById('egDesignBtn'));
    await page.evaluate(() => { Engrave.restoreView({ tab: 'approved', list: true, chosen: true, focus: null }); Engrave.render(); Engrave.restoreView({ tab: 'place', list: true, chosen: true, focus: null }); Engrave.render(); });
    await page.click('#egDesignBtn'); await settle(100);
    const after = await page.evaluate(n0 => ({ same: document.getElementById('egDesignBtn') === n0, last: document.querySelector('#engraveView > .egBar').lastElementChild === n0, n: document.querySelectorAll('#egDesignBtn, .egDesignBtn').length, calls: window.__calls, log: window.__log.map(l => l[0] + ':' + (l[1].from || '')) }), node0);
    check('T2c the button is the same node after the tab is drawn again (Approved, then Placements), still last in its bar; pressed, it opens the "Design a charm" modal on its New design side (newDesign, with the way back), logged',
      after.same && after.last && after.n === 1 && JSON.stringify(after.calls) === JSON.stringify([['new', 'charmEditorButton', 'Back to Design & Engrave']]) && after.log.join() === 'designerNew:charmEditorButton', JSON.stringify(after));
    // a phone (390 px): the bar wraps, the button keeps its size and stays on the screen, the page never scrolls sideways
    // (the left rail is put away first, as a phone has it: engrave-approved-tab.cjs; put back after)
    await page.setViewportSize({ width: 390, height: 844 }); await settle(500);
    const railed = await page.evaluate(() => !document.getElementById('app').classList.contains('railOff'));
    if (railed) { await page.click('#btnRail').catch(() => {}); await settle(500); }
    const ph = await geo();
    await R.shot(w, 'DE-1c-charm-editor-390');
    await page.setViewportSize({ width: 1440, height: 900 }); await settle(400);
    if (railed && await page.evaluate(() => document.getElementById('app').classList.contains('railOff'))) { await page.click('#btnRail').catch(() => {}); await settle(400); }
    check(`T2d at 390 px the button (${ph.w}x${ph.h} px, ${ph.left}..${ph.right}) is whole on the screen (${ph.vw} px), on one line, and the page does not scroll sideways (${ph.sw} px)`,
      ph.h >= 36 && ph.left >= 0 && ph.right <= ph.vw && ph.lines <= 1 && ph.sw <= ph.vw + 1, JSON.stringify(ph));
    // the real module: the button opens the real "Design a charm" modal, on New design
    await page.evaluate(() => { window.CharmNestCeMasters = window.__realM; Engrave.render(); });
    await settle(200);
    const real = await page.evaluate(() => !!window.CharmNestCeMasters && !document.getElementById('egDesignBtn').disabled);
    let modal = null;
    if (real) {
      await page.click('#egDesignBtn');
      await page.waitForSelector('.cem-root [data-panel="new"]:not([hidden])', { timeout: 15000 }).catch(() => {}); await settle(300);
      modal = await page.evaluate(() => { const r = document.querySelector('.cem-root'); return r ? { title: (r.querySelector('.cem-id h2') || {}).textContent, eyebrow: (r.querySelector('.cem-eyebrow') || {}).textContent, tabs: [...r.querySelectorAll('.cem-seg [data-pane]')].map(b => b.textContent.trim()), newOpen: !!r.querySelector('[data-panel="new"]:not([hidden])'), shapes: [...r.querySelectorAll('.cem-shape span')].map(s => s.textContent.trim()), busy: document.getElementById('egDesignBtn').getAttribute('aria-busy') } : null; });
      await R.shot(w, 'DE-1d-design-a-charm-modal');
      await page.click('.cem-root .cem-x'); await settle(400);
      modal.closed = await page.evaluate(() => !document.querySelector('.cem-root .cem-win') && !document.getElementById('egDesignBtn').hasAttribute('aria-busy'));
    }
    await page.mouse.move(5, 5);   // (the pointer off the page's buttons: T3 reads the card button's colour, not its hover)
    check(`T2e with the real charm-nest-ce-masters.js the button opens "${modal && modal.title}" (${modal && modal.tabs.join(' / ')}) on New design: ${modal && modal.shapes.join(', ')}; closed, the button is free again`,
      real && modal && modal.title === 'Design a charm' && /From the charm library, or a new one/i.test(modal.eyebrow) && modal.tabs.join('|') === 'From the library|New design' && modal.newOpen && modal.shapes.join('|') === 'Circle|Rounded rectangle|Heart|Tag|Your own outline' && modal.busy === 'true' && modal.closed, JSON.stringify({ real, modal }));

    /* ── T3 · the card's button ── */
    await R.openEngraveDetail(w, 'both', { face: 'back' }); await settle(800);
    const btn = await page.evaluate(() => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'), b = c.querySelector('.pvMain > .egCharmEdit'), r = b && b.getBoundingClientRect(), ap = c.querySelector('.pvApproval').getBoundingClientRect(), main = c.querySelector('.pvMain').getBoundingClientRect(), cv = c.querySelector('.backHost canvas'), cr = cv && cv.getBoundingClientRect(), old = document.querySelectorAll('.backHost .egCharmEdit').length; return b ? { text: b.textContent.trim(), w: Math.round(r.width), h: Math.round(r.height), font: parseFloat(getComputedStyle(b).fontSize), bg: getComputedStyle(b).backgroundImage, radius: getComputedStyle(b).borderRadius, place: Math.abs(r.left - main.left) < 2 && Math.abs(r.top - ap.top) < 3 && (!cr || r.bottom <= cr.top || r.right <= cr.left || r.left >= cr.right), old, n: c.querySelectorAll('.egCharmEdit').length } : null; });
    await R.shot(w, 'DE-2-card-button');
    check(`T3 the card's "Open in Charm Editor" is one white pill with a gold outline (CEBTN) ${btn && btn.w}x${btn && btn.h} px (${btn && btn.font} px words) at the top left of the work area, level with Approve, off the drawing`, btn && btn.text === 'Open in Charm Editor' && btn.n === 1 && btn.old === 0 && btn.w >= 160 && btn.h >= 36 && btn.font >= 13 && /linear-gradient\(rgb\(255, 255, 255\)/.test(btn.bg) && parseFloat(btn.radius) > 100 && btn.place, JSON.stringify(btn));

    /* ── T5a · Back to engraving, from the card ── */
    const backKey = await page.evaluate(() => document.querySelector('#egQueue .rvItem[data-kind=placement]').dataset.key);
    await page.click('#egQueue .rvItem[data-kind=placement] .pvMain > .egCharmEdit');
    await page.waitForSelector('.ce-root .ce-back', { timeout: 60000 }); await settle(300);
    const head = await page.evaluate(() => { const b = document.querySelector('.ce-root .ce-back'); return { text: b.textContent.trim(), first: b.parentElement.firstElementChild === b, sub: (document.querySelector('.ce-eyebrow') || {}).textContent }; });
    await R.shot(w, 'DE-3-editor-back-to-engraving');
    await page.click('.ce-root .ce-back'); await settle(500);
    const landed = await page.evaluate(() => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); return { open: !!document.querySelector('.ce-root'), mode: CN.S.mode, key: c && c.dataset.key }; });
    check(`T5a from the card the editor's head starts with "${head.text}"; pressed with nothing changed, the editor closes on this piece's engraving card`, /^Back to engraving$/i.test(head.text) && head.first && !landed.open && landed.mode === 'engrave' && landed.key === backKey, JSON.stringify({ head, landed, backKey }));

    /* ── T4 · a piece that is not engraved, from its order window ── */
    await R.openOrderWindow(w, 'earrings'); await settle(600);
    const owb = await page.evaluate(() => ({ btn: document.querySelectorAll('#orderWin .owCharmEdit, #orderWin .owCharmEditNote').length }));   // (OWNOCE, Paul 28 Oct 2026: "Remove this UI completely from the Detailed Order view modal")
    await R.shot(w, 'DE-4-order-window-button');
    const PID = made.earrings.poolIds[0];
    check(`T4a the order window has no "Open in Charm Editor" button nor its note (OWNOCE)`, owb && owb.btn === 0, JSON.stringify(owb));
    const t0 = Date.now();
    await page.evaluate(({ key, pid }) => { window.__pv = Engrave.openCharmEditor({ row: B.orders.byKey.get(key), poolId: pid, from: 'orderWindow' }); }, { key: made.earrings.key, pid: PID });   // (the piece opened as the order window's button opened it)
    await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settle(300);
    const openMs = Date.now() - t0;
    const ed = await page.evaluate(() => { const E = CharmEditor.current; return { title: document.querySelector('.ce-id h2').textContent, sub: document.querySelector('.ce-eyebrow').textContent, back: (document.querySelector('.ce-root .ce-back') || {}).textContent, n: E.items().length, face: E.face(), roles: E.items().map(i => i.role).join() }; });
    await R.shot(w, 'DE-5-editor-from-order');
    check(`T4b it opens in the Charm Editor in ${openMs} ms: ${ed.title}, "${ed.sub}", every part a layer (${ed.n}), the way back says "${(ed.back || '').trim()}"`,
      ed.title === 'COMET_9427' && /^order 4290000106 · piece 1 of 2 · no engraving$/.test(ed.sub) && /^Back to order$/i.test((ed.back || '').trim()) && ed.n >= 2 && ed.face === 'front' && /cut/.test(ed.roles), JSON.stringify(ed));
    // a line drawn on the earring (the editor's own extension call), then Save
    await page.evaluate(() => { const E = CharmEditor.current, f = E.frame(), cx = f.x0 + f.wPt / 2, cy = f.y0 + f.hPt / 2; E.ctx.addItems([{ kind: 'path', role: 'line', source: 'added', stroke: true, fill: false, lwPt: 0.3, subpaths: [[['m', [cx - 1, cy]], ['l', [cx + 1, cy]]]] }]); E.ctx.commit('test line'); });
    await page.click('.ce-root [data-act="save"]');
    for (let i = 0; i < 100 && !st.doc('Charm_Pool_Art', PID + '~front'); i++) await new Promise(r => setTimeout(r, 200));
    await settle(400);
    const rec = st.doc('Charm_Pool_Art', PID + '~front'), saved = await page.evaluate(() => ({ note: (document.querySelector('#orderWin .owCharmEditNote') || {}).textContent, hidden: (document.querySelector('#orderWin .owCharmEditNote') || {}).hidden, ow: OrderWin.isOpen(), ce: !!document.querySelector('.ce-root'), log: window.__log.filter(l => /^editor/.test(l[0])).map(l => [l[0], l[1].from, l[1].engraved, l[1].poolId]) }));
    check('T4c Save keeps the line for THAT piece (its Charm_Pool_Art record: who, one part added) ; the order window stays; opened and saved are logged (from the order window, not engraved)',
      rec && rec.by && (rec.added || []).length === 1 && saved.ow && !saved.ce && saved.log.some(l => l[0] === 'editorOpen' && l[1] === 'orderWindow' && l[2] === false && l[3] === PID) && saved.log.some(l => l[0] === 'editorSave' && l[1] === 'orderWindow'),
      JSON.stringify({ rec: rec && { by: rec.by, added: (rec.added || []).length }, saved }));
    // Back to order: the editor goes, the order window is as it was
    await page.evaluate(({ key, pid }) => { window.__pv = Engrave.openCharmEditor({ row: B.orders.byKey.get(key), poolId: pid, from: 'orderWindow' }); }, { key: made.earrings.key, pid: PID }); await page.waitForSelector('.ce-root .ce-back', { timeout: 60000 }); await settle(200);
    await page.click('.ce-root .ce-back'); await settle(400);
    const bo = await page.evaluate(() => ({ ce: !!document.querySelector('.ce-root'), ow: OrderWin.isOpen(), rid: OrderWin.isOpen() && OrderWin.rid() }));
    check('T4d "Back to order" closes the editor and leaves the order window open on that order', !bo.ce && bo.ow && String(bo.rid) === made.earrings.rid, JSON.stringify(bo));
    // a piece on no sheet of this page (an order outside this run): made from its master, with its own edit over it
    const PID2 = made.earrings.poolIds[1];
    await page.evaluate(pid => { for (const sh of allSheets()) if ((sh.charms || []).some(c => c.poolId === pid)) { sh.charms = sh.charms.filter(c => c.poolId !== pid); sh.placements = (sh.placements || []).filter(p => sh.charms.some(c => c.id === p.id)); } }, PID2);
    const gone = await page.evaluate(pid => !Pool.charmOf(pid), PID2);
    const t1 = Date.now();
    await page.evaluate(({ key, pid }) => { window.__pv = Engrave.openCharmEditor({ row: B.orders.byKey.get(key), poolId: pid, from: 'orderWindow' }); }, { key: made.earrings.key, pid: PID2 });
    await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settle(200);
    const master = await page.evaluate(() => ({ n: CharmEditor.current.items().length, sub: document.querySelector('.ce-eyebrow').textContent }));
    const masterMs = Date.now() - t1;
    await page.click('.ce-root [data-act="cancel"]'); await settle(200);
    check(`T4e a piece on no sheet of this page opens from its master drawing in ${masterMs} ms ("${master.sub}", ${master.n} layers)`, gone && master.n >= 2 && /piece 2 of 2 · no engraving/.test(master.sub), JSON.stringify({ gone, master }));
    await page.evaluate(() => { try { OrderWin.close(); } catch (_) {} }); await settle(300);

    /* ── T5b · Back to engraving from the order window of an engraved piece; a change asks first ── */
    await R.openOrderWindow(w, 'both'); await settle(600);
    await page.evaluate(({ key, pid }) => { window.__pv = Engrave.openCharmEditor({ row: B.orders.byKey.get(key), poolId: pid, from: 'orderWindow' }); }, { key: made.both.key, pid: made.both.poolIds[0] }); await page.waitForSelector('.ce-root .ce-back', { timeout: 60000 }); await settle(300);
    const sub2 = await page.evaluate(() => ({ sub: document.querySelector('.ce-eyebrow').textContent, back: document.querySelector('.ce-root .ce-back').textContent.trim() }));
    await page.evaluate(() => { const E = CharmEditor.current, f = E.frame(), cx = f.x0 + f.wPt / 2, cy = f.y0 + f.hPt / 2; E.ctx.addItems([{ kind: 'path', role: 'line', source: 'added', stroke: true, fill: false, subpaths: [[['m', [cx - 1, cy + 1]], ['l', [cx + 1, cy + 1]]]] }]); E.ctx.commit('test'); });
    await page.click('.ce-root .ce-back'); await settle(200);
    const asked = await page.evaluate(() => [...document.querySelectorAll('.ce-ask [data-ask]')].map(b => b.textContent.trim()));
    await R.shot(w, 'DE-6-back-asks');
    await page.click('.ce-ask [data-ask="no"]'); await settle(150);
    const still = await page.evaluate(() => !!document.querySelector('.ce-root') && !document.querySelector('.ce-ask'));
    await page.click('.ce-root .ce-back'); await settle(150); await page.click('.ce-ask [data-ask="yes"]');
    await page.waitForFunction(() => !document.querySelector('.ce-root') && !OrderWin.isOpen() && CN.S.mode === 'engrave' && !!document.querySelector('#egQueue .rvItem[data-kind=placement]'), null, { timeout: 15000 }).catch(() => {});
    await settle(500);
    const via = await page.evaluate(rid => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'), j = c && Engrave.items().get(c.dataset.key); return { ce: !!document.querySelector('.ce-root'), ow: OrderWin.isOpen(), mode: CN.S.mode, rid: j && String(j.row.order.receiptId) }; }, made.both.rid);
    const art2 = st.doc('Charm_Pool_Art', made.both.poolIds[0] + '~front');
    await R.shot(w, 'DE-7-landed-on-card');
    check(`T5b from the order window of an engraved piece ("${sub2.sub}") the way back is "${sub2.back}"; with a change it asks first (${asked.join(' / ')}), Keep editing stays, Discard changes saves nothing and lands on that piece's engraving card with the order window put away`,
      /^Back to engraving$/i.test(sub2.back) && asked.join('|') === 'Keep editing|Discard changes|Save and go back' && still && !via.ce && !via.ow && via.mode === 'engrave' && via.rid === made.both.rid && !art2, JSON.stringify({ sub2, asked, still, via, art2: !!art2 }));

    check('T6 no page error', w.errors.length === 0, JSON.stringify(w.errors).slice(0, 400));
  } catch (e) { check('the run', false, String(e && e.stack || e).slice(0, 600)); }
  finally { await w.close(); }
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\n${RESULTS.length - bad.length}/${RESULTS.length} passed${bad.length ? ' · FAILED: ' + bad.map(r => r.id.split(' ')[0]).join(', ') : ''}`);
  console.log('screenshots in ' + shots);
  process.exit(bad.length ? 1 : 0);
})();
