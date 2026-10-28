// DESIGN & ENGRAVE · the tab (Paul, 28 Oct 2026: "What this also means is that in the charm editor users will be able to edit orders that may not have a requirement for back engraving. Since
// this is all part of the back engraving tab, I would suggest we rename the tab "Design & Engrave". And the Charm Editor will be its own entity within that new updated tab that will be
// accessible from the back engraving modal and vice versa from the charm editor back into the back engraving modal ... Also, the Charm Editor button in the back engraving Modal is way too
// small and hidden, and should be a lot more visible and prominent and easy to click"). Items 30-33 and the lead's D11.
// The real sorter page over the repo's fake site (_engraveRealWorld.cjs: the real intake, pool, classifier and fit; offline, made-up names), a front + back necklace ("both") and a stud earring
// pair that is never engraved ("earrings"):
//   T1  the tab reads "Design & Engrave" where the bar has room and "Design" where it has not; its mode and its link stay "engrave" (#engrave)
//   T2  the Charm Designer bar is the first thing in the tab, the same node through the tab's redraws: New design and Open charm… call charm-nest-ce-masters.js (stand-ins here), off
//       while it is not on the page; the recent designs come from the design library (a stand-in CharmNestDesigns), newest first, four at most, and a press opens one; every press is logged
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

    /* ── T2 · the Charm Designer bar ── */
    await page.evaluate(() => {
      window.__log = []; window.CNDesignLog = { event: (kind, data) => window.__log.push([kind, data]) };
      window.__calls = [];
      Engrave.restoreView({ tab: 'place', list: true, chosen: true, focus: null }); Engrave.render();
    });
    await settle(300);
    const off = await page.evaluate(() => { const b = document.getElementById('cdBar'); return b ? { first: b.parentElement.id === 'engraveView' && b.parentElement.firstElementChild === b, newOff: b.querySelector('[data-cd="new"]').disabled, openOff: b.querySelector('[data-cd="open"]').disabled, recentHidden: b.querySelector('.cdRecent').hidden } : null; });
    check('T2a the Charm Designer bar is the first thing in the tab; with no charm-nest-ce-masters.js on the page its two buttons are off, and with no design library there is no recent list', off && off.first && off.newOff && off.openOff && off.recentHidden, JSON.stringify(off));
    const now = Date.now();
    await page.evaluate(now => {
      const designs = [
        { id: 'd1', name: 'Oak leaf', version: 2, at: now - 50000, by: 'Ana Test', orderId: '4290000103', thumbUrl: '' },
        { id: 'd2', name: 'Fix tag', version: 1, at: now - 1000, by: 'Ben Test', orderId: 'CUSTOM-20261028-1', customName: 'Fix tag' },
        { id: 'd3', name: 'Moon', version: 4, at: now - 9000, by: 'Ana Test' },
        { id: 'd4', name: 'Sun', version: 1, at: now - 30000, by: 'Ana Test' },
        { id: 'd5', name: 'Old star', version: 1, at: now - 900000, by: 'Ana Test' }];
      let fn = null;
      window.CharmNestDesigns = { search: q => designs.filter(d => !q || d.name.includes(q)), onChange: f => { fn = f; }, __add: d => { designs.push(d); if (fn) fn(); } };
      window.CharmNestCeMasters = { newDesign: o => { window.__calls.push(['new', o && o.from, o && o.back && o.back.label]); }, openPicker: o => { window.__calls.push(['open', o && o.from]); }, openDesign: (id, v) => { window.__calls.push(['design', id, v]); } };
      Engrave.render();
    }, now);
    await settle(300);
    const bar = await page.evaluate(() => { const b = document.getElementById('cdBar'), r = b.getBoundingClientRect(), eb = document.querySelector('#engraveView .egBar').getBoundingClientRect(); return { title: b.querySelector('.cdHd b').textContent, h: Math.round(r.height), above: r.bottom <= eb.top + 1, newOn: !b.querySelector('[data-cd="new"]').disabled, openOn: !b.querySelector('[data-cd="open"]').disabled, chips: [...b.querySelectorAll('.cdChip')].map(c => c.textContent.trim()), tips: [...b.querySelectorAll('.cdChip')].map(c => c.title) }; });
    await R.shot(w, 'DE-1-designer-bar');
    check(`T2b the bar says "${bar.title}", ${bar.h} px tall, above the tab's own bar; New design and Open charm… are on; the recent designs newest first, four: ${bar.chips.join(', ')}`,
      bar.title === 'Charm Designer' && bar.h <= 56 && bar.above && bar.newOn && bar.openOn && bar.chips.join('|') === 'Fix tag|Moon|Sun|Oak leaf' && /Custom · no order/.test(bar.tips[0]) && /order 4290000103/.test(bar.tips[3]) && /by Ana Test/.test(bar.tips[1]), JSON.stringify(bar));
    const node0 = await page.evaluateHandle(() => document.getElementById('cdBar'));
    await page.evaluate(() => { Engrave.restoreView({ tab: 'approved', list: true, chosen: true, focus: null }); Engrave.render(); Engrave.restoreView({ tab: 'place', list: true, chosen: true, focus: null }); Engrave.render(); });
    await page.click('#cdBar [data-cd="new"]'); await page.click('#cdBar [data-cd="open"]'); await page.click('#cdBar .cdChip[data-id="d3"]'); await settle(100);
    await page.evaluate(now => { CharmNestDesigns.__add({ id: 'd6', name: 'Brand new', version: 1, at: now, by: 'Cy Test' }); }, now + 10);
    const after = await page.evaluate(n0 => ({ same: document.getElementById('cdBar') === n0, first: document.querySelector('#engraveView').firstElementChild === n0, calls: window.__calls, chips: [...document.querySelectorAll('#cdBar .cdChip')].map(c => c.textContent.trim()), log: window.__log.map(l => l[0] + ':' + (l[1].action || l[1].designId || '')) }), node0);
    check('T2c the bar is the same node after the tab is drawn again (Approved, then Placements); New design, Open charm… and a recent design call the designer (with the way back), each logged; a design saved shows at once',
      after.same && after.first && JSON.stringify(after.calls) === JSON.stringify([['new', 'designerBar', 'Back to Design & Engrave'], ['open', 'designerBar'], ['design', 'd3', 4]]) && after.chips[0] === 'Brand new' && after.chips.length === 4 && after.log.join() === 'designerNew:,designerOpen:,designOpen:d3', JSON.stringify(after));

    /* ── T3 · the card's button ── */
    await R.openEngraveDetail(w, 'both', { face: 'back' }); await settle(800);
    const btn = await page.evaluate(() => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'), b = c.querySelector('.rh > .egCharmEdit'), r = b && b.getBoundingClientRect(), x = c.querySelector('.rh > .x').getBoundingClientRect(), old = document.querySelectorAll('.backHost .egCharmEdit').length; return b ? { text: b.textContent.trim(), w: Math.round(r.width), h: Math.round(r.height), font: parseFloat(getComputedStyle(b).fontSize), bg: getComputedStyle(b).backgroundColor, besideX: x.left >= r.right - 1 && x.left - r.right < 40, old, n: c.querySelectorAll('.egCharmEdit').length } : null; });
    await R.shot(w, 'DE-2-card-button');
    check(`T3 the card's "Open in Charm Editor" is one gold button ${btn && btn.w}x${btn && btn.h} px (${btn && btn.font} px words) in its head beside the × (the little pill in the drawing is gone)`, btn && btn.text === 'Open in Charm Editor' && btn.n === 1 && btn.old === 0 && btn.w >= 160 && btn.h >= 34 && btn.font >= 13 && btn.bg === 'rgb(169, 130, 63)' && btn.besideX, JSON.stringify(btn));

    /* ── T5a · Back to engraving, from the card ── */
    const backKey = await page.evaluate(() => document.querySelector('#egQueue .rvItem[data-kind=placement]').dataset.key);
    await page.click('#egQueue .rvItem[data-kind=placement] .rh > .egCharmEdit');
    await page.waitForSelector('.ce-root .ce-back', { timeout: 60000 }); await settle(300);
    const head = await page.evaluate(() => { const b = document.querySelector('.ce-root .ce-back'); return { text: b.textContent.trim(), first: b.parentElement.firstElementChild === b, sub: (document.querySelector('.ce-eyebrow') || {}).textContent }; });
    await R.shot(w, 'DE-3-editor-back-to-engraving');
    await page.click('.ce-root .ce-back'); await settle(500);
    const landed = await page.evaluate(() => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); return { open: !!document.querySelector('.ce-root'), mode: CN.S.mode, key: c && c.dataset.key }; });
    check(`T5a from the card the editor's head starts with "${head.text}"; pressed with nothing changed, the editor closes on this piece's engraving card`, /^Back to engraving$/i.test(head.text) && head.first && !landed.open && landed.mode === 'engrave' && landed.key === backKey, JSON.stringify({ head, landed, backKey }));

    /* ── T4 · a piece that is not engraved, from its order window ── */
    await R.openOrderWindow(w, 'earrings'); await settle(600);
    const owb = await page.evaluate(() => { const b = document.querySelector('#orderWin .owCharmEdit'), r = b && b.getBoundingClientRect(), v = document.getElementById('owVector').getBoundingClientRect(); return b ? { shown: !b.hidden && r.width > 0, on: !b.disabled, text: b.textContent.trim(), under: r.top >= v.bottom - 1, w: Math.round(r.width), h: Math.round(r.height), pool: b.dataset.pool, eng: !document.getElementById('owEng') || document.getElementById('owEng').hidden } : null; });
    await R.shot(w, 'DE-4-order-window-button');
    const PID = made.earrings.poolIds[0];
    check(`T4a the order window of an order with NO engraving shows "Open in Charm Editor" (${owb && owb.w}x${owb && owb.h} px) under the vector design, on, for its first piece`, owb && owb.shown && owb.on && owb.text === 'Open in Charm Editor' && owb.under && owb.h >= 30 && owb.pool === PID && owb.eng, JSON.stringify(owb));
    const t0 = Date.now();
    await page.click('#orderWin .owCharmEdit');
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
    check('T4c Save keeps the line for THAT piece (its Charm_Pool_Art record: who, one part added) and says so under the button; the order window stays; opened and saved are logged (from the order window, not engraved)',
      rec && rec.by && (rec.added || []).length === 1 && /Front artwork saved for this piece/.test(saved.note || '') && !saved.hidden && saved.ow && !saved.ce && saved.log.some(l => l[0] === 'editorOpen' && l[1] === 'orderWindow' && l[2] === false && l[3] === PID) && saved.log.some(l => l[0] === 'editorSave' && l[1] === 'orderWindow'),
      JSON.stringify({ rec: rec && { by: rec.by, added: (rec.added || []).length }, saved }));
    // Back to order: the editor goes, the order window is as it was
    await page.click('#orderWin .owCharmEdit'); await page.waitForSelector('.ce-root .ce-back', { timeout: 60000 }); await settle(200);
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
    await page.click('#orderWin .owCharmEdit'); await page.waitForSelector('.ce-root .ce-back', { timeout: 60000 }); await settle(300);
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
