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
  const paint = fn(bridge, 'function paintSeals(', '/** Paint the window from the row it is showing. */'), stamp = fn(bridge, 'function windowReleaseOf(', 'function paintSeals(');
  check(stamp.length > 100 && /releaseStampOf/.test(stamp) && /releaseSealHtml/.test(paint), 'source: the window has windowReleaseOf (the latest Release Hold of the order from what the page holds) and paintSeals draws the card\'s own releaseSealHtml');
  check(!/setInterval|setTimeout|requestAnimationFrame|addEventListener|\bfetch\(|XMLHttpRequest|\bapi\(|ensureSeals|loadSeals|\.load\(|\.refresh\(\)|Firestore|firebase|\.release\(|\.mark\(/i.test(code(stamp + paint)), 'source: no timer, no listener, no network read or write in the code that draws it');
  check(/paintSeals\(r\)/.test(fn(bridge, 'function paintNow(r) {', 'function splitLine')), 'source: it is still painted from paintNow() (the path an approval, a timeline step or a repaint already takes)');
  check(/charm-nest-bridge\.js\?v=[^"]*-rw1/.test(html) && /\.owRelRow\{/.test(html) && /data-ow-release/.test(bridge), 'source: the cache token of the changed script is bumped (-rw1) and the page has the strip\'s release CSS');
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
      const where = el => el.closest('#owSeals') ? 'seal strip' : el.closest('#owNowCard') ? 'head card' : el.closest('#owEng') ? 'engraving card' : el.closest('.owfPop') ? 'chip card (hover)' : el.closest('#owMeta') ? 'facts' : el.closest('.owRail, #owRail, .owProg, [class*="rail"]') ? 'header rail' : el.closest('#owPcSum, .owPcRow') ? 'piece rows' : el.closest('[data-view="timeline"], .owVTimeline') ? 'Timeline tab' : el.closest('[data-view="sheet"]') ? 'Sheet tab' : (el.parentElement && (el.parentElement.id || el.parentElement.className)) || '?';
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

    // ═══ 2 · a released order: the seal is in the window at rest ═══
    await openOv(TWICE); let r = await rel(); const w3 = await when(AT3);
    check(r.n === 1 && r.shown && r.inStrip, `2 the released order's window holds ONE Release Hold seal, in the strip, shown at rest (no hover): ${JSON.stringify([r.n, r.shown, r.inStrip])}`);
    check(r.family === 'release' && /RELEASE/.test(r.svgText || '') && /HOLD/.test(r.svgText || '') && /seal-releaseHold/.test(r.kind || ''), `2 it is the real RELEASE HOLD seal, the card's (${r.family} · ${r.svgText})`);
    check(r.seal && r.seal.lw >= 72 && r.seal.lh >= 72, `2 it is big: ${r.seal && r.seal.lw} x ${r.seal && r.seal.lh} px (the Orders card's seal is 72)`);
    check(/Release Hold/.test(r.words) && r.by === 'by Second Releaser' && r.words.includes(w3.d) && r.words.includes(w3.t) && r.at === AT3 && /Hold released to the nesting queue by Second Releaser/.test(r.aria || ''), `2 with "Release Hold", who, the date and the time of the LATEST release, not the earlier one (${r.words})`);
    check(r.inside && r.first === 'owSeals' && r.strip && r.strip.t >= 0, `2 inside the window and the scroll area, the first thing in the Overview: ${JSON.stringify([r.inside, r.first])}`);
    await shot('02-released-1440');
    // the Timeline keeps every release as it was (the earlier one too)
    await tab('timeline');
    const steps = await page.evaluate(() => [...document.querySelectorAll('#orderWin [data-view="timeline"] svg[data-seal-family="release"]')].length);
    check(steps >= 2, `2 the Timeline tab still lists both releases (${steps} Release Hold seals in its lanes)`);
    await shot('02-timeline-1440'); await tab('info');

    // ═══ 3 · never released: none ═══
    await openOv(PLAIN); r = await rel();
    check(r.n === 0 && !r.shown && r.hidden === true && r.stripHtml === 0 && r.first !== 'owSeals', `3 an order never released: no seal, the strip hidden and empty (${JSON.stringify([r.n, r.shown, r.stripHtml, r.first])})`);
    // a hold let go the ordinary way (its timeline step carries no Release Hold seal) is not a Release Hold press
    await openOv(HELD); r = await rel();
    check(r.n === 0 && !r.shown, '3 an order whose hold was let go the ordinary way (a "released" step with no Release Hold seal): none');

    // ═══ 6 · a released order with an approved engraving: both, one above the other, no overlap ═══
    await openOv(ENG); r = await rel(); const w2 = await when(AT2);
    // (NOWINSEAL, Paul 19 Oct 2026: the order window's approvals panel is removed: "Remove this UI from the Detailed Order view. I did not ask for this.": the strip holds the release seal alone)
    check(r.shown && r.n === 1 && !r.apItems && !r.hasAp && !r.apHd, `6 released + approved engraving: the strip holds the release seal ALONE, no approvals panel (${JSON.stringify([r.n, r.apItems, r.hasAp])})`);
    check(r.by === 'by Cy Releaser' && r.words.includes(w2.d) && r.seal.lw >= 72 && r.inside && r.aboveNow, `6 and the release seal is still the large one with who and when (${r.words}) inside the window`);
    await shot('06-released-and-approved-1440');

    // ═══ 5 · only the timeline knows (a reload on another computer): shown; the latest wins ═══
    await openOv(TLONLY); r = await rel();
    check(r.n === 1 && r.shown && r.by === 'by Tim Elsewhere' && r.at === AT2 && r.seal.lw >= 72, `5 a release known only from the order's timeline shows (by Tim Elsewhere): ${JSON.stringify([r.n, r.by, r.at === AT2])}`);
    // a newer release arrives through the timeline while the window is open (another computer): the seal moves to it
    const AT4 = AT3 + 5 * 60e3;
    tl(TLONLY.rid, AT4, 'Late Arrival');
    await page.evaluate(() => OrderWin.nudge());
    const moved = await until(async () => { const x = await rel(); return x.at === AT4 && x; }, 20000, 'the newer release to show').catch(() => null);
    check(moved && moved.n === 1 && moved.by === 'by Late Arrival', `5 live: a newer release from another computer replaces it (${moved && moved.by})`);

    // ═══ 4 · LIVE: the window is open on an order in Review; the real Release Hold press ═══
    await page.evaluate(async rid => { const m = await CharmNestReviewMark.mark(rid, { by: 'Test Operator' }); CN.setMode('review'); Review.syncOrderItems(); Review.render(); return m; }, LIVE.rid);
    await until(() => page.$(`#rvList .reviewListRow[data-rid="${LIVE.rid}"] [data-rel-hold]`), 20000, 'Release Hold on the marked row');
    await openOv(LIVE); r = await rel();
    check(r.n === 0 && !r.shown, '4 the window opens on an order in Review, not yet released: no seal');
    const t0 = Date.now();
    await page.evaluate(rid => document.querySelector(`#rvList .reviewListRow[data-rid="${rid}"] [data-rel-hold]`).click(), LIVE.rid);
    const live = await until(async () => { const x = await rel(); return x.n === 1 && x.shown && x; }, 30000, 'the seal to appear in the open window').catch(() => null);
    check(!!live, `4 live: the seal appears in the OPEN window by itself ${Date.now() - t0} ms after the press`);
    check(live && live.by === 'by Test Operator' && live.seal.lw >= 72 && live.at >= t0 - 1000 && live.inside, `4 and it is the press's: by Test Operator, now, large (${live && live.words})`);
    await shot('04-live-after-press');
    check(await page.evaluate(() => OrderWin.isOpen()), '4 the window is still open (the press did not close or reload it)');
    await closeWin();
    await page.evaluate(() => { CN.setMode('orders'); Orders.showPile('', ''); Orders.render(); });

    // ═══ 7 · layout at three widths, a very long name ═══
    for (const [w, h] of [[1440, 900], [1024, 768], [390, 800]]) {
      await page.setViewportSize({ width: w, height: h }); await sleep(500);
      await openOv(LONG); r = await rel();
      check(r.shown && r.n === 1 && r.seal.lw >= 72 && r.inside && r.boxInside && !r.sideways, `7 at ${w} px: the large seal (>= 72 px) and its words are inside the window, nothing sideways, the long name wraps (${JSON.stringify([r.seal && r.seal.lw, r.inside, r.boxInside, r.sideways, r.box && r.box.w])})`);
      const wrap = await page.evaluate(() => { const b = document.querySelector('#owSeals .relSealBy'), t = document.querySelector('#owSeals .relSealWords'); const lh = parseFloat(getComputedStyle(b).lineHeight) || 16; return { lines: Math.round(b.getBoundingClientRect().height / lh), clipped: t.scrollWidth > t.clientWidth + 1, size: getComputedStyle(b).fontSize }; });
      check(!wrap.clipped && /Featherstonehaugh/.test(await page.evaluate(() => document.querySelector('#owSeals .relSealBy').textContent)), `7 at ${w} px: the whole long name is there and not clipped (${JSON.stringify(wrap)})`);
      await shot(`07-long-${w}`);
      await openOv(ENG); r = await rel();
      check(r.shown && r.n === 1 && !r.apItems && r.boxInside && !r.sideways && r.aboveNow, `7 at ${w} px: an approved order too: the release seal alone, inside, nothing sideways, no approvals panel`);
      await shot(`07-both-${w}`);
    }
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
