// OWHEAD (Paul, 29 Oct 2026, the order window of order 4216320756, SS CARDS 1 - 2 CARDS: "1. Red Outline - Add the already existing "Edit Design" button. 2. Green Outline - Relocate the back/forth
// navigation chevrons and counter from the Red Outline to be inside the Green Outline, make the chevrons a bit larger and easier to click including the counter text to match. Don't make it too big,
// make it tasteful. 3. Green Outline - Remove the current text UI completely to make room for the navigation chevrons and counter."). The real sorter page over the repo's fake site
// (_engraveRealWorld.cjs: the real intake, pool, classifier and fit; offline, made-up names), an engraved comet necklace ("back") and a stud earring pair never engraved ("earrings"), both plain
// catalog charms, and a custom order made in the page:
//   H1  the "Updated live · last change … · who" line is gone from the window (no #owLive, none of its words)
//   H2  the walk (‹ N of M ›) is at the right end of the tab row, on every tab: chevrons of 32 px or more with a hover and a focus ring, the count larger than the old 10.5 px; Next and Previous still
//       walk the Orders list, the count follows
//   H3  Edit Design stands top right, just left of the ×, on a plain order; pressed, it opens THAT order's piece in the Charm Editor through the piece path (its subtitle "order <id> · piece …")
//   H4  a custom order keeps its "Design Charm", and its press opens the Charm Designer in design mode ("Custom order <id>")
//   H5  a piece the Charm Editor cannot open (its master not in the library; not in the pool yet) says so in a toast, and nothing opens
//   H6  1440, 1280, 1024 and 390 px: nothing in the header (the number, the rail's steps, Edit Design, ×) or the tab row (the tabs, the tab's tools, the walk) overlaps, nothing scrolls sideways,
//       and Edit Design is the same size at every width
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/ow-header.cjs      (SHOTS=<dir> for the screenshots)
'use strict';
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const R = require('./_engraveRealWorld.cjs');

(async () => {
  const shots = process.env.SHOTS || '/tmp/ow-header-shots';
  const w = await R.world({ viewport: { width: 1440, height: 900 }, shots });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page;
  await page.waitForFunction(() => !window.CNRole || ['ask', 'role', 'admin'].includes(CNRole.state()), null, { timeout: 20000 }).catch(() => {});
  await page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('design'); } catch (_) { /* no role step on this page */ } });   // (the "Laser or Design?" bar covers buttons)
  const settle = ms => R.settle(page, ms);
  const until = async (fn, ms) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) return null; await new Promise(r => setTimeout(r, 150)); } };
  const editor = () => page.evaluate(() => { const r = document.querySelector('.ce-root'); if (!r || !window.CharmEditor || !CharmEditor.current) return null; return { title: (r.querySelector('.ce-id h2') || {}).textContent || '', sub: (r.querySelector('.ce-eyebrow') || {}).textContent || '', n: CharmEditor.current.items().length }; });
  const editorUp = ms => until(async () => { const e = await editor(); return e ? e : null; }, ms || 60000);
  const closeEditor = async () => { if (!(await editor())) return; await page.click('.ce-root [data-act="cancel"]'); await until(() => page.evaluate(() => !document.querySelector('.ce-root') && !(window.CharmEditor && CharmEditor.current)), 15000); await settle(200); };
  const openWin = async (key, view) => {
    await page.evaluate(() => { CN.setMode('orders'); if (OrderWin.isOpen()) OrderWin.close(); });
    await until(() => page.evaluate(() => !document.querySelector('#orderWin[open]')), 5000);
    await page.evaluate(async ({ key, view }) => { await OrderWin.open(key, view ? { view } : undefined); }, { key, view });
    await until(() => page.evaluate(k => OrderWin.isOpen() && OrderWin.key() === k && (() => { const b = document.getElementById('owCpDesign'); return !!b && !b.hidden && !!b.offsetParent; })(), key), 8000); await settle(500);
  };
  const toasts = () => page.evaluate(() => [...document.querySelectorAll('#toasts .toast')].map(t => t.textContent.trim()));
  /** The header and the tab row, in numbers (CSS px). */
  const layout = () => page.evaluate(() => {
    const R = e => { if (!e) return null; const r = e.getBoundingClientRect(); return r.width && r.height ? { l: r.left, t: r.top, r: r.right, b: r.bottom, w: r.width, h: r.height } : null; };
    const q = s => document.querySelector(s), qa = s => [...document.querySelectorAll(s)];
    const head = q('#orderWin .owHead'), tabs = q('#orderWin .owTabsV'), nav = q('#owNav'), b = q('#owCpDesign');
    const vis = e => !!e && !e.closest('[hidden]') && getComputedStyle(e).display !== 'none' && !!R(e);
    const textBox = e => { if (!vis(e)) return null; const rg = document.createRange(); rg.selectNodeContents(e); const r = rg.getBoundingClientRect(); return r.width ? { l: r.left, t: r.top, r: r.right, b: r.bottom } : null; };
    const headParts = [['title', textBox(q('#owTitle'))], ...qa('#owRail .tlStop').filter(vis).map((s, i) => ['step' + i, R(s)]), ['send', vis(q('#owSendSheet')) ? R(q('#owSendSheet')) : null], ['editDesign', vis(b) ? R(b) : null], ['close', R(q('#owClose'))]].filter(p => p[1]);
    const tools = qa('#orderWin .owTools .owGrp.on > *').filter(vis).map((e, i) => ['tool' + i, R(e)]);
    const rowParts = [...qa('#orderWin .owTabsV > [role=tab]').filter(vis).map(t => ['tab:' + t.dataset.owView, R(t)]), ...tools, ['prev', vis(q('#owPrev')) ? R(q('#owPrev')) : null], ['pos', vis(q('#owPos')) ? R(q('#owPos')) : null], ['next', vis(q('#owNext')) ? R(q('#owNext')) : null]].filter(p => p[1]);
    const hit = (a, c) => a.l < c.r - 0.5 && c.l < a.r - 0.5 && a.t < c.b - 0.5 && c.t < a.b - 0.5;
    const over = list => { const out = []; for (let i = 0; i < list.length; i++) for (let j = i + 1; j < list.length; j++) if (hit(list[i][1], list[j][1])) out.push(list[i][0] + '×' + list[j][0]); return out; };
    const cs = b ? getComputedStyle(b) : null;
    return {
      vw: innerWidth, docSW: document.scrollingElement.scrollWidth, headSW: head.scrollWidth, headCW: head.clientWidth, tabsSW: tabs.scrollWidth, tabsCW: tabs.clientWidth, dlgSW: q('#orderWin').scrollWidth, dlgCW: q('#orderWin').clientWidth,
      headBox: R(head), tabsBox: R(tabs), navBox: R(nav), navIn: !!nav && nav.parentElement === tabs, navHidden: !nav || nav.hidden,
      prev: R(q('#owPrev')), next: R(q('#owNext')), prevDis: !!(q('#owPrev') || {}).disabled, nextDis: !!(q('#owNext') || {}).disabled, pos: R(q('#owPos')), posText: (q('#owPos') || {}).textContent || '', posFont: q('#owPos') ? parseFloat(getComputedStyle(q('#owPos')).fontSize) : 0,
      btn: b ? { box: R(b), text: b.textContent.trim(), inRight: !!b.closest('#orderWin .owHead .owRight'), beforeClose: b.nextElementSibling === q('#owClose'), h: b.offsetHeight, w: b.offsetWidth, font: cs.fontSize, pad: cs.padding, shown: vis(b) } : null,
      close: R(q('#owClose')), title: textBox(q('#owTitle')), railShown: vis(q('#owRail')),
      headOver: over(headParts), rowOver: over(rowParts), headParts: headParts.map(p => p[0]), rowParts: rowParts.map(p => p[0]),
      outside: [...headParts, ...rowParts].filter(p => p[1].l < -0.5 || p[1].r > innerWidth + 0.5).map(p => p[0])
    };
  });
  try {
    const made = await R.build(w, ['back', 'earrings']);
    const A = made.back, E = made.earrings;
    check(`H0 the world: two plain orders (${A.rid}: ${A.poolIds && A.poolIds.length} piece, ${E.rid}: ${E.poolIds && E.poolIds.length} pieces)`, A.poolIds && A.poolIds.length && E.poolIds && E.poolIds.length === 2, JSON.stringify({ A, E }).slice(0, 400));

    /* ── H1 · the text line is gone ── */
    await openWin(E.key);
    await settle(1500);   // (the order's timeline read: the line used to be drawn from it)
    const h1 = await page.evaluate(() => { const d = document.getElementById('orderWin'); return { live: !!document.getElementById('owLive') || !!d.querySelector('.owLive'), words: /updated live|last change/i.test(d.innerText), tools: (d.querySelector('.owTools .owGrp[data-for=info]') || {}).textContent || '' }; });
    await R.shot(w, 'OH-H1-1440');
    check(`H1 no "Updated live · last change … · who" line in the window (#owLive ${h1.live ? 'present' : 'gone'}, its words ${h1.words ? 'present' : 'gone'}, the Overview's tools "${h1.tools}")`, !h1.live && !h1.words && h1.tools === '', JSON.stringify(h1));

    /* ── H2 · the walk at the right end of the tab row ── */
    const l2 = await layout();
    const list = await page.evaluate(() => (Orders.walkRows ? Orders.walkRows() : Orders.visibleRows()).map(r => r.key));
    const i0 = list.indexOf(E.key);
    const navOk = l2.navIn && !l2.navHidden && l2.navBox && Math.abs(l2.tabsBox.r - l2.navBox.r) <= 14 && l2.navBox.t >= l2.tabsBox.t - 0.5 && l2.navBox.b <= l2.tabsBox.b + 0.5
      && l2.prev.w >= 32 && l2.prev.h >= 32 && l2.next.w >= 32 && l2.next.h >= 32 && l2.posFont >= 11.5 && l2.posText === `${i0 + 1} of ${list.length}` && l2.prev.r <= l2.pos.l + 0.5 && l2.pos.r <= l2.next.l + 0.5 && l2.prevDis === (i0 === 0) && l2.nextDis === (i0 === list.length - 1);
    check(`H2a the walk is in the tab row, at its right end (${l2.navBox && (l2.tabsBox.r - l2.navBox.r).toFixed(1)} px from it): ‹ ${l2.prev && l2.prev.w}x${l2.prev && l2.prev.h} · "${l2.posText}" ${l2.posFont} px · › ${l2.next && l2.next.w}x${l2.next && l2.next.h}${l2.prevDis ? ', ‹ off at the first' : ''}${l2.nextDis ? ', › off at the last' : ''}; nothing of it in the header`,
      navOk && !(await page.evaluate(() => !!document.querySelector('#orderWin .owHead #owPrev, #orderWin .owHead #owNext, #orderWin .owHead #owPos'))), JSON.stringify(l2));
    // (hover and keyboard focus are seen on the chevrons)
    const live = i0 < list.length - 1 ? '#owNext' : '#owPrev';
    const bg0 = await page.$eval(live, b => getComputedStyle(b).backgroundColor);
    await page.hover(live); await settle(300);
    const bg1 = await page.$eval(live, b => getComputedStyle(b).backgroundColor);
    await page.mouse.move(5, 895); await page.focus(live); await page.keyboard.press('Shift+Tab'); await page.keyboard.press('Tab'); await settle(100);
    const ring = await page.$eval(live, b => ({ focus: b.matches(':focus-visible'), outline: getComputedStyle(b).outlineStyle + ' ' + getComputedStyle(b).outlineWidth }));
    check(`H2b the chevrons show a hover (${bg0} → ${bg1}) and a keyboard ring (${ring.outline})`, bg0 !== bg1 && ring.focus && /solid 2px/.test(ring.outline), JSON.stringify({ bg0, bg1, ring }));
    // (Next and Previous walk the list the Orders tab shows, as before; on the Timeline and Sheet tabs too)
    const want = list[i0 + 1] ? [list[i0 + 1], '#owNext', '#owPrev'] : [list[i0 - 1], '#owPrev', '#owNext'];
    await page.click(want[1]); const moved = await until(() => page.evaluate(k => OrderWin.key() === k, want[0]), 10000); await settle(400);
    const pos1 = await page.evaluate(() => document.getElementById('owPos').textContent);
    await page.click(want[2]); const back = await until(() => page.evaluate(k => OrderWin.key() === k, E.key), 10000); await settle(400);
    const pos2 = await page.evaluate(() => document.getElementById('owPos').textContent);
    const perTab = [];
    for (const v of ['timeline', 'sheet', 'info']) { await page.click(`#orderWin .owTabsV [data-ow-view="${v}"]`).catch(() => {}); await settle(450); const l = await layout(); perTab.push([v, !l.navHidden && !!l.navBox && Math.abs(l.tabsBox.r - l.navBox.r) <= 14, l.rowOver.join(',')]); }
    check(`H2c ${want[1]} goes to ${want[0]} ("${pos1}") and ${want[2]} back to ${E.key} ("${pos2}"); the walk stays at the row's right end on every tab, clear of its tools (${perTab.map(p => p[0] + ':' + (p[1] ? 'ok' : 'off') + (p[2] ? '[' + p[2] + ']' : '')).join(' ')})`,
      !!moved && !!back && pos1 !== pos2 && pos2 === `${i0 + 1} of ${list.length}` && perTab.every(p => p[1] && !p[2]), JSON.stringify({ moved, back, pos1, pos2, perTab }));

    /* ── H3 · Edit Design on a plain order opens its piece ── */
    const l3 = await layout();
    check(`H3a Edit Design top right on a plain order: "${l3.btn && l3.btn.text}", in the header's right group, just left of the × (${l3.btn && (l3.close.l - l3.btn.box.r).toFixed(1)} px), ${l3.btn && l3.btn.w}x${l3.btn && l3.btn.h} px`,
      l3.btn && l3.btn.shown && l3.btn.text === 'Edit Design' && l3.btn.inRight && l3.btn.beforeClose && l3.close.l - l3.btn.box.r >= 0 && l3.close.l - l3.btn.box.r <= 12 && l3.btn.h >= 36, JSON.stringify(l3.btn));
    await page.click('#owCpDesign'); const e3 = await editorUp(); await R.shot(w, 'OH-H3-earrings-editor');
    await closeEditor();
    await openWin(A.key); const b3a = await layout();
    await page.click('#owCpDesign'); const e3a = await editorUp();
    await closeEditor();
    check(`H3b pressed: the earrings' Edit Design opens "${e3 && e3.title}" (${e3 && e3.sub}); the necklace's opens "${e3a && e3a.title}" (${e3a && e3a.sub}): each its own order's piece, through the piece path`,
      !!e3 && e3.sub.startsWith(`order ${E.rid} · piece 1 of 2`) && b3a.btn && b3a.btn.text === 'Edit Design' && !!e3a && e3a.title === 'COMET_9427' && e3a.sub.startsWith(`order ${A.rid} ·`), JSON.stringify({ e3, e3a }));
    /* ── H4 · a custom order keeps its design mode ── */
    const CU = await page.evaluate(src => {
      const base = Orders.rows().find(r => String(r.order.receiptId) === src), rid = '4290000199';
      const order = JSON.parse(JSON.stringify(base.order)); order.receiptId = rid; order.orderNumber = rid; order.lines = [Object.assign({}, order.lines[0], { transactionId: rid + '1', sku: 'CUSTOM_ORDER', title: 'Custom Charm, your drawing' })];
      const line = order.lines[0], key = CharmNestOrders.lineKey(order, line), row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null };
      B.orders.rows.push(row); B.orders.byKey.set(key, row); Orders.interpretAll();
      row.spec = row.spec || {}; row.spec.special = { group: 'custom', label: 'Custom order' }; row.state = 'pulled';
      Orders.render(); return { rid, key, needs: CharmNestCustomPiece.needsDesign(row) };
    }, E.rid);
    await openWin(CU.key);
    const l4 = await layout();
    await page.click('#owCpDesign'); const e4 = await editorUp(); await R.shot(w, 'OH-H4-custom-designer');
    await closeEditor();
    check(`H4 a custom order (needs a design: ${CU.needs}): "${l4.btn && l4.btn.text}" in the same place; pressed, the Charm Designer in design mode: "${e4 && e4.title}" (${e4 && e4.sub})`,
      CU.needs && l4.btn && l4.btn.text === 'Design Charm' && l4.btn.inRight && l4.btn.beforeClose && !!e4 && e4.sub.startsWith(`Custom order ${CU.rid}`), JSON.stringify({ CU, btn: l4.btn, e4 }));

    /* ── H5 · a piece that cannot open says why ── */
    const mk = await page.evaluate(src => {
      const base = Orders.rows().find(r => String(r.order.receiptId) === src), out = {};
      for (const [rid, sku, pooled] of [['4290000197', 'NOT_IN_LIBRARY_0001', true], ['4290000198', 'COMET_9427', false]]) {
        const order = JSON.parse(JSON.stringify(base.order)); order.receiptId = rid; order.orderNumber = rid; order.lines = [Object.assign({}, order.lines[0], { transactionId: rid + '1', sku })];
        const line = order.lines[0], key = CharmNestOrders.lineKey(order, line), row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: pooled ? 'pooled' : 'pulled', reason: null, claimedBy: null, poolIds: pooled ? [key + '_1'] : [], engrave: null, material: null };
        B.orders.rows.push(row); B.orders.byKey.set(key, row); out[pooled ? 'gone' : 'unpooled'] = { rid, key };
      }
      Orders.interpretAll();
      for (const v of Object.values(out)) { const r = B.orders.byKey.get(v.key); if (r.spec && r.spec.special) r.spec.special = null; v.plain = !CharmNestCustomPiece.needsDesign(r); }
      Orders.render(); return out;
    }, E.rid);
    await openWin(mk.gone.key); const t0 = (await toasts()).length;
    const l5 = await layout(); await page.click('#owCpDesign');
    const said = await until(async () => { const t = await toasts(); return t.slice(t0).find(x => /master library|cannot be opened/i.test(x)) || null; }, 30000);
    const none5 = !(await editor());
    await openWin(mk.unpooled.key); const t1 = (await toasts()).length;
    await page.click('#owCpDesign');
    const said2 = await until(async () => { const t = await toasts(); return t.slice(t0 > t1 ? 0 : t1).find(x => /not in the pool yet/i.test(x)) || null; }, 10000);
    await R.shot(w, 'OH-H5-toast');
    check(`H5 a piece whose master is not in the library: "${l5.btn && l5.btn.text}" says "${said}" and opens nothing (${none5}); a line with no piece in the pool: "${said2}"`,
      mk.gone.plain && l5.btn && l5.btn.text === 'Edit Design' && !!said && none5 && !!said2 && !(await editor()), JSON.stringify({ mk, said, said2, none5 }));

    /* ── H6 · the layout at four widths ── */
    const sizes = [];
    for (const [W, H] of [[1440, 900], [1280, 800], [1024, 768], [390, 844]]) {
      await page.setViewportSize({ width: W, height: H }); await settle(300);
      for (const k of [E.key, CU.key]) {
        await openWin(k); await page.mouse.move(2, H - 5); await settle(300);
        const l = await layout(); sizes.push([W, k === CU.key ? 'custom' : 'plain', l.btn && l.btn.h, l.btn && l.btn.font, l.btn && l.btn.pad]);
        if (k === E.key) await R.shot(w, `OH-H6-${W}`);
        check(`H6 ${W} px, ${k === CU.key ? 'the custom order' : 'a plain order'}: nothing in the header overlaps (${l.headParts.length} parts${l.railShown ? ', the rail shown' : ', no rail under 900 px'}${l.headOver.length ? ': ' + l.headOver.join(', ') : ''}), nor in the tab row (${l.rowParts.join(' ')}${l.rowOver.length ? ': ' + l.rowOver.join(', ') : ''}); nothing past the screen or scrolled sideways (doc ${l.docSW}/${l.vw}, header ${l.headSW}/${l.headCW}, row ${l.tabsSW}/${l.tabsCW}); Edit Design ${l.btn && l.btn.w}x${l.btn && l.btn.h}`,
          l.btn && l.btn.shown && !l.headOver.length && !l.rowOver.length && !l.outside.length && l.docSW <= l.vw && l.headSW <= l.headCW + 1 && l.tabsSW <= l.tabsCW + 1 && l.dlgSW <= l.dlgCW + 1 && (k === CU.key || (l.prev && l.next && l.prev.w >= 32 && l.next.w >= 32)), JSON.stringify(l));
      }
      // (the other tabs bring their own tools into the row: the Timeline's, the Sheet's Front / Back switch; the walk stays clear of them)
      const tabsAt = [];
      await openWin(E.key);   // (a plain order on a sheet: its Sheet tab opens)
      for (const v of ['timeline', 'sheet']) { await page.click(`#orderWin .owTabsV [data-ow-view="${v}"]`); await settle(500); const l = await layout(); tabsAt.push({ v, over: l.rowOver, out: l.outside, sw: l.tabsSW, cw: l.tabsCW, parts: l.rowParts, nav: !!l.navBox && Math.abs(l.tabsBox.r - l.navBox.r) <= 14 }); }
      if (W === 390) await R.shot(w, 'OH-H6-390-sheet');
      check(`H6 ${W} px, the Timeline and Sheet tabs: the walk at the row's right end, clear of their tools (${tabsAt.map(t => t.v + ': ' + t.parts.join(' ') + (t.over.length ? ' [' + t.over.join(', ') + ']' : '')).join(' · ')})`,
        tabsAt.every(t => !t.over.length && !t.out.length && t.sw <= t.cw + 1 && t.nav), JSON.stringify(tabsAt));
    }
    const same = sizes.every(s => s[2] === sizes[0][2] && s[3] === sizes[0][3] && s[4] === sizes[0][4]);
    check(`H6z Edit Design is the same size everywhere: ${[...new Set(sizes.map(s => `${s[2]} px high, ${s[3]} words, padding ${s[4]}`))].join(' | ')}`, same, JSON.stringify(sizes));
    check('H7 no page error', w.errors.length === 0, JSON.stringify(w.errors).slice(0, 400));
  } catch (e) { check('the run', false, String(e && e.stack || e).slice(0, 600)); }
  finally { await w.close(); }
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\n${RESULTS.length - bad.length}/${RESULTS.length} passed${bad.length ? ' · FAILED: ' + bad.map(r => r.id.split(' ')[0]).join(', ') : ''}`);
  console.log('screenshots in ' + shots);
  process.exit(bad.length ? 1 : 0);
})();
