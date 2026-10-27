// NOSEALROW (Paul, 26 Oct 2026: "Remove this extra duplicate and unneeded 'Sent to Sheet' button + Seal and the second 'Seals' menu completely from the UI of the Detailed Order modal"): the compact
// Seals row (#owSealRow) is gone from the window. What this test proves now, for every scenario (released, released twice, released with an approved engraving, known only from the timeline, a live
// press, three widths, and never released):
//   - the Order Details modal has no Release Hold panel (NOSTRIP) and no Seals row (source: no #owSealRow in the page; the bridge's cache token is the current one, -ns26);
//   - the order still has its Release Hold seals, one per press (OrderWin.seals(); none for an order never released; a live press is known to the open window without a reload);
//   - at rest a Release Hold seal is drawn nowhere in the window but beside a shown Release Hold button ([data-rel-hold] / [data-cu-release]). A released order's window shows no Release Hold button
//     of its own (the card's Release Hold is in its piece row only while the order is a Review card), so its seals are not drawn there at rest;
//   - beside a shown Release Hold button (the test's own, #testRelBtn, in the piece row where the card's Release Hold stands, as seals-window does with Cancel Order) they are drawn compact, one per
//     press, who and when on hover, inside the window's width at 1440 / 1024 / 390 px with no sideways scroll; when that button goes they leave the window with it (no row takes them);
//   - the Timeline tab still lists every release and the Orders row still carries the seal. (tests/charm-nest/seals-window.cjs covers every kind.)
// SEALWIN (Paul 20 Oct 2026: "All of these seals must be visible in all places where a given button is relevant ... This includes the Detailed Order view modal"): the big panel stayed gone, but the order's
// Release Hold seal(s) came back in the window, compact, one per press, in the Seals row (#owSealRow, removed 26 Oct, above) or beside a Release Hold button the window shows.
// NOSTRIP (Paul, 19 Oct 2026, of the Release Hold seal in the order window: "I asked for this to be completely removed from the Order Details modal"): the window has NO seal strip any more (#owSeals and paintSeals are gone).
// Under NOSTRIP this test proved the Release Hold seal was nowhere in the Order Details modal at rest (superseded by SEALWIN and NOSEALROW above: the large panel stays gone; the scenarios are the same),
// while the Timeline tab still lists every release and the Orders row still carries the seal. (The original RELWIN header follows, for what the scenarios are.)
// RELWIN (Paul, 19 Oct 2026): "The release hold seal should be visible also in the detailed Order popup modal. Make sure it's big enough to be visible and legible."
// RELHOLD (18 Oct) put the RELEASE HOLD seal on the Orders card, 72 px, with its words (Release Hold, by whom, the date and the time in Toronto). The order window showed it nowhere at rest: its header rail seal is a
// small hover card (charm-nest-piece-seals.js) and the Timeline tab draws the "released" step as a small lane seal. Now the window's seal strip, #owSeals (charm-nest-bridge.js paintSeals, the strip WINSEAL made for the
// approved engraving / monogram seals), holds the same 72 px seal with the same words (Orders.releaseSealHtml, the card's own markup) at the top of the Overview, above the approvals when there are any.
//   what is proved (the REAL page over the offline fake site, bridge-server.cjs; every name is made up; nothing is read or written that the window did not already read or write)
//     1  node: the code that draws it reads no network, sets no timer, adds no listener; it draws from paintNow() like the approvals; cache tokens bumped (-rw1)
//     2  a released order: the seal is in the window at rest (no hover, no scroll), family "release", >= 72 px, "Release Hold", who, the date and the time (Toronto), the first thing in the Overview
//     3  an order never released has none; the strip stays hidden (and the approvals-only strip of an engraved order is what it was)
//     4  LIVE: the window is open on an order in Review; the Release Hold press (the real button) makes the seal appear in the window by itself
//     5  a release known only from the order's timeline (no line record, no cloud record: a reload, another computer) shows too; when it was released more than once the LATEST is shown
//        (the earlier ones stay in the Timeline as they were)
//     6  a released order with an approved engraving: both the release seal and the approval seal show, one above the other, without overlap, inside the window
//     7  layout at 1440 / 1024 / 390 px: inside the window, no sideways scroll, long names wrap, the seal stays 72 px
//   AUDIT=1 prints, for a released order, every release-family seal anywhere in the window, where it is, and whether a person sees it without hovering (the audit of the request); SHOTS=<dir> keeps screenshots.
//   PW_DIR=<playwright node_modules> node tests/charm-nest/release-seal-window.cjs
// What a pass does NOT prove: a live order on the live page (live data, live fonts): only that the page's own code puts the seal in the window and keeps it right on the offline fake.
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const read = f => fs.readFileSync(path.join(root, f), 'utf8');
const R = require('./_engraveRealWorld.cjs');
const Base = require('./_orderFactsWorld.cjs');
const SHOTS = process.env.SHOTS || '', AUDIT = !!process.env.AUDIT;
const fails = []; let passed = 0;
const check = (ok, what) => { if (ok) passed++; else fails.push(what); console.log((ok ? '  ok   ' : '  FAIL ') + what); };
const V = (name, value) => ({ name, value });
const fx = (rid, buyer, extra) => Object.assign({ rid, sku: 'COMET_9427', listing: '18' + rid.slice(-6), title: 'Comet Necklace, Celestial Charm Necklace in 14k Gold Fill', buyer, placed: { day: 0, h: 9, m: 25 }, ship: 5, variations: [V('Metal choice', 'Gold')], personalization: [] }, extra || {});
const LIVE = fx('4290000301', 'Name Thirty'), PLAIN = fx('4290000302', 'Name Thirtyone'), ENG = fx('4290000303', 'Name Thirtytwo'), TLONLY = fx('4290000304', 'Name Thirtythree'),
  TWICE = fx('4290000305', 'Name Thirtyfour'), LONG = fx('4290000306', 'Name Thirtyfive'), HELD = fx('4290000307', 'Name Thirtysix');
const keyOf = f => `${f.rid}_${f.rid}1`;
const LONGNAME = 'Bartholomew-Maximilian Featherstonehaugh-Wolfeschlegelsteinhausenbergerdorff';

// ═══ 1 · the wiring, in the page's own files (no browser) ═══
{
  const bridge = read('charm-nest-bridge.js'), html = read('charm-nest-1.html');
  const fn = (src, start, end) => { const i = src.indexOf(start), j = src.indexOf(end, i + start.length); return i >= 0 && j > i ? src.slice(i, j) : ''; };
  const code = s => s.replace(/\/\*[\s\S]*?\*\//g, '').replace(/(^|[^:])\/\/[^\n]*/g, '$1');
  check(!/function paintSeals\(|function windowReleaseOf\(|paintSeals\(r\)/.test(code(bridge)) && !/id="owSeals"/.test(html) && !/\.owRelRow\{|\.owApproved\{/.test(html), 'source: the order window has no seal strip: no #owSeals, no paintSeals / windowReleaseOf, no strip CSS');
  check(!/owSealRow|ow-seal-row/.test(html), 'source: no Seals row in the page (Paul, 26 Oct 2026): no #owSealRow element, no .owSealRow CSS');
  check(/charm-nest-bridge\.js\?v=[^"]*-ns26[-"]/.test(html), 'source: the bridge\'s cache token is the current one (-ns26)');
}

(async () => {
  const W = await R.world({ viewport: { width: 1440, height: 900 }, reducedMotion: 'no-preference' });
  if (!W) { console.log('no playwright-core: the browser part is skipped'); return finish(); }
  const { page, srv } = W, st = srv.st;
  const saw = []; page.on('console', m => saw.push(`[${m.type()}] ${m.text()}`.slice(0, 400)));
  const sleep = ms => page.waitForTimeout(ms);
  const until = async (f, ms, what) => { const t = Date.now() + ms; for (;;) { const v = await f(); if (v) return v; if (Date.now() > t) throw new Error('timed out: ' + what); await sleep(250); } };
  const shot = async name => { if (!SHOTS) return; try { fs.mkdirSync(SHOTS, { recursive: true }); await page.screenshot({ path: path.join(SHOTS, name + '.png') }); } catch (_) {} };
  const writes = []; page.on('request', rq => { if (rq.method() !== 'GET' && rq.method() !== 'OPTIONS' && /claude|openai|anthropic|etsy/i.test(rq.url())) writes.push(rq.url()); });
  try {
    await page.evaluate(() => {
      window.__toasts = []; const was = window.toast; window.toast = (m, k, ms) => { window.__toasts.push([k || '', String(m)]); return was && was(m, k, ms); };
    });
    // ── the orders, through the real intake ──
    await page.evaluate(({ fixtures, orderSrc }) => {
      const orderOf = eval('(' + orderSrc + ')'), now = Date.now();
      for (const f of fixtures) { const order = orderOf(f, now); for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); } }
      Orders.interpretAll();
    }, { fixtures: [LIVE, PLAIN, ENG, TLONLY, TWICE, LONG, HELD], orderSrc: Base.orderOf.toString() });
    const T = await page.evaluate(() => Date.now());
    const AT1 = T - 3 * 3600e3, AT2 = T - 40 * 60e3, AT3 = T - 15 * 60e3;   // an older release, a later one, the latest
    // the ENG order: finished, its back engraving approved (the line's record keeps the seal: no Engrave job needed), and released with Release Hold (the line keeps who and when, as the press leaves it)
    await page.evaluate(({ k, at, relAt, rid }) => {
      const row = B.orders.byKey.get(k), text = 'NAME THIRTYTWO';
      row.spec.engraveCandidate = true; row.spec.engraving = { v: 1, mode: 'back', front: undefined, back: { text, lines: [text] }, source: 'note', textSource: 'labelled', confidence: 1, needsPerson: false };
      row.state = 'committed'; row.engrave = { needed: true, state: 'approved', approved: true, text, approvedBy: 'Ana', approvedAt: at, seals: [{ id: 'engraveApproved:' + at + ':Ana', how: 'engraveApproved', at, by: 'Ana' }] };
      row.releaseHold = { at: relAt, by: 'Cy Releaser', id: 'releaseHold:' + rid + ':' + relAt };
      Orders.interpretAll();
    }, { k: keyOf(ENG), at: AT1, relAt: AT2, rid: ENG.rid });
    // LONG: released by a person with a very long name
    await page.evaluate(({ k, relAt, rid, who }) => { B.orders.byKey.get(k).releaseHold = { at: relAt, by: who, id: 'releaseHold:' + rid + ':' + relAt }; }, { k: keyOf(LONG), relAt: AT3, rid: LONG.rid, who: LONGNAME });
    // TWICE: released at AT1 (its timeline) and again at AT3 (its line, the latest); TLONLY: only the timeline knows (a reload on another computer: no line record, no cloud record)
    const tl = (rid, at, by) => st.put('Order_Timeline', rid + '~released~oh-rel-' + at, { id: 'releaseHold:' + rid + ':' + at, type: 'released', at, by, orderId: rid, lineKey: '', transactionId: '', source: 'sorter', station: 'sorter', device: 'other-computer', sheetId: '', sheet: '', setId: '', milestone: false,
      text: `Released to the nesting queue by ${by} · Release Hold (Review)`, data: { seal: 'releaseHold', sealAt: at, sealId: 'releaseHold:' + rid + ':' + at, how: 'mark', from: 'review' } });
    tl(TLONLY.rid, AT2, 'Tim Elsewhere');
    tl(TWICE.rid, AT1, 'First Releaser');
    await page.evaluate(({ k, relAt, rid }) => { B.orders.byKey.get(k).releaseHold = { at: relAt, by: 'Second Releaser', id: 'releaseHold:' + rid + ':' + relAt }; }, { k: keyOf(TWICE), relAt: AT3, rid: TWICE.rid });
    tl(TWICE.rid, AT3, 'Second Releaser');
    // HELD: a hold let go the ordinary way (its step carries no Release Hold seal): not a Release Hold press, so no seal
    st.put('Order_Timeline', HELD.rid + '~released~oh-rel-plain', { id: 'oh-rel-' + HELD.rid, type: 'released', at: AT2, by: 'Plain Releaser', orderId: HELD.rid, lineKey: '', transactionId: '', source: 'sorter', station: 'sorter', device: 'other-computer', sheetId: '', sheet: '', setId: '', milestone: false, text: 'Released from hold by Plain Releaser', data: {} });
    await page.evaluate(() => { CN.setMode('orders'); Orders.showPile('', ''); Orders.render(); });

    // ── helpers over the window ──
    const closeWin = async () => { await page.evaluate(() => { const o = document.getElementById('orderWin'); if (o && o.open) document.getElementById('owClose').click(); }); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 8000 }).catch(() => {}); await sleep(250); };
    const openOv = async f => {
      await closeWin();
      await page.evaluate(k => OrderWin.open(k), keyOf(f));
      await page.waitForFunction(k => OrderWin.isOpen() && OrderWin.key() === k, keyOf(f));
      await page.waitForSelector('#owMeta [data-owf-chip="order"]', { timeout: 20000 });
      await sleep(1100);
    };
    const when = at => page.evaluate(at => ({ d: new Date(at).toLocaleString('en-US', { timeZone: 'America/Toronto', month: 'short', day: 'numeric', year: 'numeric' }), t: new Date(at).toLocaleTimeString('en-US', { timeZone: 'America/Toronto', hour: 'numeric', minute: '2-digit' }) }), at);
    /** the release seal(s) of the window's Overview strip: where, how big, what it says, whether it is seen with no hover */
    const rel = () => page.evaluate(() => {
      const dlg = document.getElementById('orderWin'), dr = dlg.getBoundingClientRect(), strip = document.getElementById('owSeals'), main = dlg.querySelector('.owMain'), mr = main.getBoundingClientRect();
      const vis = el => { for (let n = el; n && n.nodeType === 1; n = n.parentElement) { const cs = getComputedStyle(n); if (cs.display === 'none' || cs.visibility === 'hidden' || n.hidden || +cs.opacity === 0) return false; } const r = el.getBoundingClientRect(); return r.width > 4 && r.height > 4; };
      const box = e => { if (!e) return null; const r = e.getBoundingClientRect(); return { t: Math.round(r.top), b: Math.round(r.bottom), l: Math.round(r.left), r: Math.round(r.right), w: Math.round(r.width), h: Math.round(r.height) }; };
      const one = [...dlg.querySelectorAll('#owSeals [data-rel-seal]')], r0 = one[0] || null, face = r0 && r0.querySelector('.seal'), svg = face && face.querySelector('svg');
      const ap = strip && strip.querySelector('.owApRow'), apHd = strip && strip.querySelector('.owApHd');
      const fb = box(face); if (fb) { fb.lw = face.offsetWidth; fb.lh = face.offsetHeight; }   // (lw / lh: the seal's own size; the bounding box of a seal turned a few degrees is a little larger)
      return { n: one.length, shown: !!strip && !strip.hidden && vis(strip), seal: fb, at: r0 ? +r0.dataset.relSeal : 0, family: svg && svg.dataset.sealFamily, svgText: svg && svg.textContent, kind: face && [...face.classList].join(' '),
        words: r0 && r0.querySelector('.relSealWords') ? r0.querySelector('.relSealWords').innerText.replace(/\s+/g, ' ').trim() : '', by: r0 && r0.querySelector('.relSealBy') ? r0.querySelector('.relSealBy').textContent : '', aria: face && face.getAttribute('aria-label'),
        box: box(r0), strip: box(strip), ap: box(ap), apHd: box(apHd), hasAp: !!ap, apItems: strip ? strip.querySelectorAll('[data-ow-seal]').length : 0,
        inside: !!face && fb.l >= dr.left - 1 && fb.r <= dr.right + 1 && fb.t >= Math.max(dr.top, mr.top) - 1 && fb.b <= Math.min(dr.bottom, mr.bottom) + 1, inStrip: !!r0 && strip.contains(r0), first: (() => { const c = [...main.children].find(e => vis(e)); return c ? c.id || c.className : ''; })(),
        boxInside: !!r0 && r0.getBoundingClientRect().left >= dr.left - 1 && r0.getBoundingClientRect().right <= dr.right + 1, sideways: dlg.scrollWidth > dlg.clientWidth + 1 || main.scrollWidth > main.clientWidth + 1, stripHtml: strip ? strip.innerHTML.length : 0, hidden: strip ? strip.hidden : null, aboveNow: (() => { const nc = document.getElementById('owNowCard'); return !strip || !nc || nc.hidden || strip.getBoundingClientRect().bottom <= nc.getBoundingClientRect().top + 1; })() };
    });
    /** the audit: every release-family seal anywhere in the window (any tab), where it is and whether it is seen at rest */
    const inventory = () => page.evaluate(() => {
      const dlg = document.getElementById('orderWin'), dr = dlg.getBoundingClientRect();
      const where = el => el.closest('[data-ow-btn-seals]') ? 'beside its button' : el.closest('#owSeals') ? 'seal strip' : el.closest('#owNowCard') ? 'head card' : el.closest('#owEng') ? 'engraving card' : el.closest('.owfPop') ? 'chip card (hover)' : el.closest('#owMeta') ? 'facts' : el.closest('.owRail, #owRail, .owProg, [class*="rail"]') ? 'header rail' : el.closest('#owPcSum, .owPcRow') ? 'piece rows' : el.closest('[data-view="timeline"], .owVTimeline') ? 'Timeline tab' : el.closest('[data-view="sheet"]') ? 'Sheet tab' : (el.parentElement && (el.parentElement.id || el.parentElement.className)) || '?';
      const vis = el => { for (let n = el; n && n.nodeType === 1; n = n.parentElement) { const cs = getComputedStyle(n); if (cs.display === 'none' || cs.visibility === 'hidden' || n.hidden || +cs.opacity === 0) return false; } const r = el.getBoundingClientRect(); return r.width > 4 && r.height > 4; };
      return [...dlg.querySelectorAll('svg[data-seal-family="release"]')].map(v => { const s = v.closest('.seal') || v, r = s.getBoundingClientRect(); return { where: where(s), w: Math.round(r.width), visible: vis(s), inView: r.top >= dr.top - 1 && r.bottom <= dr.bottom + 1, cls: (s.className && s.className.baseVal !== undefined ? s.className.baseVal : s.className) + '' }; });
    });
    const tab = async name => { await page.evaluate(n => document.querySelector(`#orderWin [data-ow-view="${n}"]`).click(), name); await sleep(900); };

    // ═══ the audit of the request: a released order, every tab ═══
    if (AUDIT) {
      for (const [f, label] of [[ENG, 'released + approved engraving'], [TWICE, 'released twice']]) {
        await openOv(f);
        for (const v of ['info', 'timeline', 'sheet']) {
          await tab(v); const inv = await inventory();
          console.log(`  AUDIT ${label} · ${v} tab: release-family seals in the window: ${inv.length ? inv.map(x => `${x.where} ${x.w}px ${x.visible ? 'visible' : 'hidden'}${x.inView ? '' : ' (not in view)'} [${x.cls.slice(0, 40)}]`).join('; ') : 'none'}`);
          await shot(`audit-${f.rid}-${v}`);
        }
        await tab('info');
      }
      await openOv(PLAIN); console.log('  AUDIT never released: ' + JSON.stringify(await rel()).slice(0, 200));
      await closeWin(); return finish();
    }

    // ═══ 2 · no Release Hold panel and no Seals row in the Order Details modal, whatever the order's releases; the seals only beside a shown Release Hold button ═══
    // (the head card's ONE seal is the order's current milestone, whatever it is (Approved, On sheet, Released ...): that is the card's own, older than the strip, and stays; what is gone is the strip's large Release Hold panel)
    // (SEALWIN, Paul 20 Oct 2026: "All of these seals must be visible in all places where a given button is relevant ... This includes the Detailed Order view modal": the big panel stays gone (NOSTRIP), and the
    //  order's Release Hold seals are compact, one per press, beside a Release Hold button the window shows. NOSEALROW, Paul 26 Oct 2026: the Seals row (#owSealRow) that took a seal whose button was not
    //  shown is gone: no row, and a Release Hold seal is drawn nowhere in the window but beside a shown Release Hold button. A released order's window shows no Release Hold button of its own (the card's
    //  Release Hold is in its piece row only while the order is a Review card), so at rest its seals are not drawn in it; the test's own Release Hold button (#testRelBtn, put in the piece row where the
    //  card's stands, as seals-window puts its own Cancel Order) shows them beside a shown button)
    const RELBTN = '[data-rel-hold],[data-cu-release]';
    const winRel = () => page.evaluate(sel => {
      const dlg = document.getElementById('orderWin'), dr = dlg.getBoundingClientRect();
      const shown = b => !!b && b.isConnected && !b.hidden && !b.closest('[hidden]') && getComputedStyle(b).display !== 'none' && b.getBoundingClientRect().width > 0;
      const seals = [...dlg.querySelectorAll('[data-seal-kind="release"]')].map(n => { const g = n.closest('[data-ow-btn-seals]'), b = g && g.previousElementSibling, r = n.getBoundingClientRect();
        // (RELSTAMP / RELEASED, 27 Oct 2026: once released, the seal stays beside the Hold Order button that takes Release Hold's place in the piece row: that button counts as the release's own)
        return { beside: !!g, btn: b ? (b.id || b.className) : '', btnRel: !!b && (b.matches(sel) || b.matches('.revSendBtn')), btnShown: shown(b), title: n.title, w: Math.round(r.width), inside: r.left >= dr.left - 1 && r.right <= dr.right + 1 }; });   // (inside the window's width: at 390 px the piece row may be below the fold, the window scrolls down)
      return { row: !!document.getElementById('owSealRow') || !!document.querySelector('.owSealRow, .owSealRowHd, [data-testid="ow-seal-row"]'), btns: [...dlg.querySelectorAll(sel + ',.revSendBtn')].filter(b => !b.closest('[data-ow-btn-seals]') && shown(b)).length,
        seals, sideways: dlg.scrollWidth > dlg.clientWidth + 1 };
    }, RELBTN);
    const atRest = async (label, want) => {
      const inv = (await inventory()).filter(x => x.visible && x.where !== 'head card'), x = await rel();
      const panel = await page.evaluate(() => !!document.querySelector('#orderWin .relSeal, #orderWin .relSealWords'));
      const by = await page.evaluate(() => OrderWin.seals().filter(s => s.kind === 'release').map(s => s.by));
      let w = await winRel();
      check(x.n === 0 && !x.shown && x.strip === null && !panel, `${label}: no Release Hold panel (seal + "Release Hold / by / when") in the Order Details modal`);
      check(!w.row, `${label}: no Seals row (#owSealRow) in the page`);
      check(by.length === want, `${label}: the order has ${want} Release Hold seal(s), one per press (OrderWin.seals(): ${JSON.stringify(by)})`);
      check(inv.every(i => i.where === 'beside its button') && w.seals.every(s => s.beside && s.btnRel && s.btnShown) && (w.btns > 0 || !w.seals.length),
        `${label}: at rest, no Release Hold seal anywhere in the window but beside a shown Release Hold button (Release Hold buttons shown: ${w.btns}; drawn: ${JSON.stringify(w.seals)}; elsewhere: ${JSON.stringify(inv.filter(i => i.where !== 'beside its button'))})`);
      // beside a shown Release Hold button: the test's own, first among the piece row's buttons, where the card's Release Hold stands (the seals go beside the first shown one)
      await page.evaluate(() => { const b = document.createElement('button'); b.type = 'button'; b.className = 'btn relHoldBtn xs'; b.id = 'testRelBtn'; b.setAttribute('data-rel-hold', '1'); b.textContent = 'Release Hold'; document.querySelector('#orderWin .owPcRow .pcAct').prepend(b); OrderWin.paint(); });
      await sleep(300);
      w = await winRel(); const mine = w.seals.filter(s => s.btn === 'testRelBtn');
      check(want ? mine.length === want && w.seals.length === want && mine.every(s => s.btnShown && s.w <= 48 && s.inside) && by.every(n => mine.some(s => s.title.includes('by ' + n))) && !w.sideways : !w.seals.length,
        `${label}: beside a shown Release Hold button: ${want ? want + ' Release Hold seal(s), one per press, compact, who and when on hover, inside the window\'s width, no sideways scroll' : 'no Release Hold seal'} (${JSON.stringify(w.seals)} sideways=${w.sideways})`);
      await page.evaluate(() => { const b = document.getElementById('testRelBtn'); if (b) b.remove(); OrderWin.paint(); });
      await sleep(300);
      w = await winRel();
      check(!w.row && w.seals.every(s => s.btn !== 'testRelBtn' && s.beside && s.btnRel && s.btnShown), `${label}: when that button goes, its seals leave the window with it (no row takes them) (${JSON.stringify(w.seals)})`);
    };
    for (const [f, label, want] of [[TWICE, 'released twice', 2], [ENG, 'released + approved engraving', 1], [TLONLY, 'known only from the timeline', 1], [PLAIN, 'never released', 0]]) { await openOv(f); await atRest('2 ' + label, want); }
    // the Timeline tab keeps every release as it was
    await openOv(TWICE); await tab('timeline');
    const steps = await page.evaluate(() => [...document.querySelectorAll('#orderWin [data-view="timeline"] svg[data-seal-family="release"]')].length);
    check(steps >= 2, `2 the Timeline tab still lists both releases (${steps} Release Hold seals in its lanes)`);
    await tab('info');
    // ═══ 4 · LIVE: the real Release Hold press with the window open: the open window knows the new seal without a reload; at rest it is drawn only beside a shown Release Hold button; the Orders row carries it ═══
    await page.evaluate(async rid => { await CharmNestReviewMark.mark(rid, { by: 'Test Operator' }); CN.setMode('review'); Review.syncOrderItems(); Review.render(); }, LIVE.rid);
    await until(() => page.$(`#rvList .reviewListRow[data-rid="${LIVE.rid}"] [data-rel-hold]`), 20000, 'Release Hold on the marked row');
    await openOv(LIVE);
    await page.evaluate(rid => document.querySelector(`#rvList .reviewListRow[data-rid="${rid}"] [data-rel-hold]`).click(), LIVE.rid);
    const stamped = await until(async () => (await page.evaluate(rid => !!Orders.releaseStampOf(Orders.rows().filter(x => String(x.order.receiptId) === rid)), LIVE.rid)) || null, 30000, 'the release to be recorded').catch(() => null);
    check(!!stamped, '4 the press is recorded (Orders.releaseStampOf)');
    const known = await until(() => page.evaluate(() => OrderWin.seals().some(s => s.kind === 'release')), 15000, 'the open window to know the new Release Hold seal').catch(() => false);
    check(!!known, '4 the open window knows the new Release Hold seal without a reload (OrderWin.seals())');
    await sleep(2500);   // (the card's Release Hold leaves the window's piece row once its Review card is resolved: at rest after that)
    await atRest('4 after a live press', 1);
    await closeWin();
    await page.evaluate(() => { CN.setMode('orders'); Orders.showPile('', ''); Orders.render(); });
    await sleep(900);
    check(await page.evaluate(rid => !!document.querySelector(`#ordItems [data-rid="${rid}"] .relSeal`), LIVE.rid), '4 the Orders row still carries the Release Hold seal');
    // ═══ 7 · three widths ═══
    for (const [w, h] of [[1440, 900], [1024, 768], [390, 800]]) { await page.setViewportSize({ width: w, height: h }); await sleep(500); await openOv(LONG); await atRest(`7 at ${w} px`, 1); await shot(`07-long-${w}`); }
    await page.setViewportSize({ width: 1440, height: 900 });
    await closeWin();
    const errs = W.errors.filter(e => !/ResizeObserver/.test(e));
    check(errs.length === 0, `no page error (${errs.slice(0, 3).join(' | ')})`);
    check(W.warns.length === 0, `no "order view:" warning from the window (${W.warns.slice(0, 2).join(' | ')})`);
    check(writes.length === 0, 'no call to Etsy or an AI service');
    const bad = await page.evaluate(() => window.__toasts.filter(t => t[0] === 'bad'));
    check(!bad.length, `no failure toast (${JSON.stringify(bad)})`);
  } catch (e) {
    fails.push('the test stopped: ' + (e && e.stack || e));
    console.log('  FAIL the test stopped:', String(e && e.stack || e).split('\n').slice(0, 6).join('\n'));
    console.log(saw.slice(-20).join('\n'));
  } finally { await W.close(); }
  finish();
})();

function finish() {
  console.log(fails.length ? `\n${fails.length} FAILED, ${passed} passed:\n- ${fails.join('\n- ')}` : `\nrelease-seal-window: ${passed} checks passed`);
  process.exitCode = fails.length ? 1 : 0;
}
