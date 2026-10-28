// SEALPAIR + SEALGREEN (Paul, 28 Oct 2026, on an order whose Hold then Release made four seals within a minute, ORDER HELD, ON HOLD, RESTORED and ORDER RELEASED: "Out of these 4 Seals I only
// want to see the First one 'Order Held' and last one 'Order Released'. The two middle seals are pointless and useless and then just crowd up the whole timeline, so please remove them everywhere ...
// Please also change the colour of the 'Order Released' seal to be green so that it better represents a green light to go ahead.")
//   1  node: the cache tokens; the ORDER RELEASED seal's ink is green (#19663f) in Seal (its family and INK.releaseHold), in the travelling seals (CNSealTravel) and in the Orders card's CSS (.relSeal:
//      its words, border, wash and the dashed line of the hold it ended), and no release rule keeps the old orange
//   2  jsdom, the real Seal and timeline over the records each path writes (the app's own shapes):
//        a  Hold Order + its take-off + Release Hold (OrderHold.release: restores, then "released" with the seal): exactly ORDER HELD then ORDER RELEASED, the latter green; the compact rail
//           and the "Where it is now" card agree
//        b  the Review card's own "released" step (stamped with the press's time, before the lines are made up): the same two
//        c  the sheet window's "Take off and hold" (no Hold Order press) and its own Release hold: its hold reads ORDER HELD (never ON HOLD), no RESTORED
//        d  a restore with no hold seal and no release (the only trace of it) keeps its RESTORED seal
//        e  a second hold after a release is a seal of its own
//   3  the REAL sorter page (bridge-server.cjs fake site, _reviewSendWorld.cjs): Hold Order pressed (the real mark writes ORDER HELD's note; the take-off's "held" event recorded the way the sheet
//      window records it), then the Review card's real Release Hold (the real OrderHold.release: the real "restored" and "released"). The records are all kept; the order window's Timeline draws
//      exactly two seals, ORDER HELD then ORDER RELEASED, the latter in green ink; the Orders card's release seal reads green.
//   PW_DIR=<playwright node_modules> node tests/charm-nest/seal-hold-release-pair.cjs   (through scratchpad/int/slot.sh)
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const read = f => fs.readFileSync(path.join(root, f), 'utf8');
const fails = []; let passed = 0;
const check = (ok, what) => { if (ok) passed++; else fails.push(what); console.log((ok ? '  ok   ' : '  FAIL ') + what); };
const GREEN = '#19663f', ORANGE = /a2591c|162,\s*89,\s*28/i;

// ═══ 1 · the source ═══
{
  const html = read('charm-nest-1.html');
  check(['charm-nest-motion.js', 'charm-nest-timeline-ui.js', 'charm-nest-seal-travel.js'].every(f => new RegExp(f.replace(/\./g, '\\.') + '\\?v=[^"]*-sg1').test(html)), '1 · the three changed scripts carry -sg1 in charm-nest-1.html');
  const rules = html.match(/\.relSeal[^{]*\{[^}]*\}|\.relSealWords[^{]*\{[^}]*\}/g) || [];
  const col = rules.filter(r => /color|border|background/.test(r));
  check(rules.length >= 5 && !rules.some(r => ORANGE.test(r)) && col.some(r => /^\.relSeal\{/.test(r) && /--relInk:#19663f/.test(r) && /rgba\(25,102,63,\.38\)/.test(r) && /rgba\(25,102,63,\.07\)/.test(r)), `1 · the Orders card's ORDER RELEASED seal (.relSeal) is green, words, border and wash; no release rule keeps the orange (${rules.length} rules)`);
}

// ═══ 2 · jsdom: the real faces and timeline over each path's records ═══
const jsdomPart = async () => {
  const { JSDOM } = require('jsdom');
  const dom = new JSDOM('<main id="tl"></main><section id="rail"></section>', { url: 'http://127.0.0.1', runScripts: 'outside-only', pretendToBeVisual: true });
  const w = dom.window, d = w.document;
  w.matchMedia = () => ({ matches: false });
  w.Element.prototype.getAnimations = () => [];
  w.Element.prototype.animate = () => ({ finished: Promise.resolve(), cancel() {}, playState: 'finished' });
  w.eval(read('charm-nest-motion.js')); w.eval(read('charm-nest-timeline-ui.js')); w.eval(read('charm-nest-seal-travel.js'));
  const T = w.OrderTimelineUI, S = w.Seal, ST = w.CNSealTravel;
  check(S.FAMILY.release.ink === GREEN && S.INK.releaseHold === GREEN && ST.KINDS.release.ink === GREEN, `2 · the release ink is ${GREEN} in Seal.FAMILY, Seal.INK.releaseHold and CNSealTravel.KINDS (${S.FAMILY.release.ink}, ${S.INK.releaseHold}, ${ST.KINDS.release.ink})`);
  const face = S.svg({ how: 'releaseHold', at: Date.now(), by: 'Name One', id: 'releaseHold:1:1' });
  check(face.includes(`fill="${GREEN}" stroke="${GREEN}"`) && !ORANGE.test(face) && /ORDER/.test(face) && /RELEASED/.test(face), '2 · the ORDER RELEASED face: its outline, rings, words and icon are drawn in the green ink, no orange anywhere in it');
  const strip = ST.html([{ kind: 'release', how: 'releaseHold', at: Date.now(), by: 'Name One', id: 'r1' }]);
  check(new RegExp(`--stInk:${GREEN}`).test(strip) && !ORANGE.test(strip), '2 · the travelling ORDER RELEASED seal (a card with no Orders module) is green too');

  const sets = {}; let n = 0;
  w.OrderTimeline = { get: async id => ({ events: (sets[id] || []).slice(), cancelled: null }), onRecord: () => () => {} };
  const flush = async () => { for (let i = 0; i < 24; i++) await Promise.resolve(); await new Promise(r => setTimeout(r, 30)); for (let i = 0; i < 12; i++) await Promise.resolve(); };
  const E = (rid, type, at, extra) => Object.assign({ id: `${type}-${++n}`, type, orderId: rid, at, by: 'Name One', source: 'sorter', lineKey: '', text: '' }, extra || {});
  /** what a mount of the order draws: each seal's [action, family, ink], oldest first */
  const drawn = async (rid, compact) => {
    const host = d.createElement('div'); d.body.append(host);
    const tl = T.mount(host, { orderId: rid, live: false, compact: !!compact }); await flush();
    const out = [...host.querySelectorAll(compact ? '.tlSeal svg[data-seal-model], .tlSt[data-key] svg[data-seal-model]' : '.tlSt[data-key] svg[data-seal-model]')].map(s => { const m = JSON.parse(s.dataset.sealModel); const g = s.querySelector('g'); return { action: m.action, family: s.dataset.sealFamily, ink: g && g.getAttribute('stroke'), at: m.at, key: s.closest('[data-key]') && s.closest('[data-key]').dataset.key }; }).sort((a, b) => a.at - b.at);
    tl.destroy(); host.remove(); return out;
  };
  const say = list => list.map(x => `${x.action}/${x.family}/${x.ink}`).join(' · ');
  const T0 = Date.now() - 6 * 3600e3, s = 1000;

  // a · Paul's order: Hold Order (its note), the take-off's held, Release Hold: OrderHold.release made the line up again (restored) and wrote "released" with the seal
  const A = '4299900001', L = A + '_' + A + '1';
  sets[A] = [
    E(A, 'arrived', T0 - 3600e3, { by: 'Etsy', source: 'etsy' }),
    E(A, 'note', T0, { id: `rvmark.${A}.${T0}`, text: 'Order held by Name One', lineKey: L, data: { reviewMark: true, by: 'Name One' } }),
    E(A, 'held', T0 + 4 * s, { id: `sw-sh-1-${T0 + 4 * s}`, sheetId: 'sh-1', sheet: 'GF Sheet 2', text: 'Taken off GF Sheet 2 by Name One, on hold', data: { pieces: 1 } }),
    E(A, 'restored', T0 + 40 * s, { id: `${L}.${T0 + 40 * s}`, lineKey: L, text: 'Back in line · was Taken off GF Sheet 2 by Name One', data: { was: 'Taken off GF Sheet 2 by Name One' } }),
    E(A, 'released', T0 + 55 * s, { id: `oh-rel-${A}-${T0 + 38 * s}`, text: 'Released from hold by Name One · front of the queue', data: { frontAt: T0 + 38 * s, seal: 'releaseHold', sealAt: T0 + 38 * s, sealId: `releaseHold:${A}:${T0 + 38 * s}` } })
  ];
  const a = await drawn(A), held = a.filter(x => x.action === 'ORDER HELD'), rel = a.filter(x => x.action === 'ORDER RELEASED');
  check(a.length === 3 && a[0].action === 'ORDER RECEIVED' && held.length === 1 && rel.length === 1 && a.indexOf(held[0]) < a.indexOf(rel[0]) && held[0].key.startsWith('note~') && !a.some(x => /ON HOLD|RESTORED/.test(x.action)),
    `2a · Hold then Release: exactly two seals of it, ORDER HELD then ORDER RELEASED (beside the order's arrival): ${say(a)}`);
  check(rel[0] && rel[0].family === 'release' && rel[0].ink === GREEN && held[0].family === 'exceptions', `2a · ORDER RELEASED is the release family in green (${rel[0] && rel[0].ink}); ORDER HELD keeps the hold's red octagon`);
  const railA = await drawn(A, true);
  check(!railA.some(x => /ON HOLD|RESTORED/.test(x.action)), `2a · the compact rail draws neither ON HOLD nor RESTORED (${say(railA)})`);
  const evs = sets[A], now = (ev) => { const h = d.createElement('div'); h.innerHTML = T.nowStamps(evs, { ev }).seal; const sv = h.querySelector('svg[data-seal-model]'); return sv ? JSON.parse(sv.dataset.sealModel).action : ''; };
  check(now(evs[3]) === 'ORDER RELEASED' && now(evs[2]) === 'ORDER RELEASED' && now(evs[4]) === 'ORDER RELEASED', `2a · the "Where it is now" card asked about the RESTORED or the ON HOLD wears the latest real seal, ORDER RELEASED (${now(evs[3])}, ${now(evs[2])})`);
  check(T.sealed(evs[2]) && T.sealed(evs[3]), '2a · the records are untouched: held and restored are still person events the explainer and the hold card read (only their stamps are not drawn)');

  // b · the Review card's own "released" step: stamped with the press's time, the lines made up a moment AFTER it
  const B = '4299900002';
  sets[B] = [
    E(B, 'note', T0, { text: 'Order held by Name One', data: { reviewMark: true, by: 'Name One' } }),
    E(B, 'held', T0 + 3 * s, { text: 'Put on hold by Name One, on hold', data: { pieces: 0 } }),
    E(B, 'released', T0 + 60 * s, { id: `releaseHold:${B}:${T0 + 60 * s}`, text: 'Released to the nesting queue by Name One · Release Hold (Review)', data: { seal: 'releaseHold', sealAt: T0 + 60 * s, from: 'review' } }),
    E(B, 'restored', T0 + 63 * s, { text: 'Back in line · was Put on hold', data: { was: 'Put on hold' } }),
    E(B, 'restored', T0 + 64 * s, { lineKey: 'x2', text: 'Back in line · was Put on hold', data: { was: 'Put on hold' } })
  ];
  const b = await drawn(B);
  check(b.map(x => x.action).join() === 'ORDER HELD,ORDER RELEASED' && b[1].ink === GREEN, `2b · released at the press, restores after it: ORDER HELD then a green ORDER RELEASED (${say(b)})`);

  // c · the sheet window's "Take off and hold" (no Hold Order press), then its own Release hold (no seal of the button)
  const C = '4299900003';
  sets[C] = [
    E(C, 'held', T0, { text: 'Taken off SS Sheet 4 by Name One, on hold', sheet: 'SS Sheet 4', data: { pieces: 1 } }),
    E(C, 'restored', T0 + 90 * s, { text: 'Back in line · was Taken off SS Sheet 4', data: { was: 'Taken off SS Sheet 4' } }),
    E(C, 'released', T0 + 92 * s, { text: 'Hold released in the sheet window · back in line' })
  ];
  const c = await drawn(C);
  check(c.length === 2 && c[0].action === 'ORDER HELD' && c[0].key.startsWith('held~') && !c.some(x => /ON HOLD|RESTORED/.test(x.action)), `2c · a hold with no Hold Order press: its one seal reads ORDER HELD (never ON HOLD), no RESTORED (${say(c)})`);

  // d · a restore that is the only trace of something: no hold seal before it, no release around it
  const D = '4299900004';
  sets[D] = [E(D, 'arrived', T0, { by: 'Etsy', source: 'etsy' }), E(D, 'restored', T0 + 3600e3, { text: 'Back in line · was a hold kept from an earlier session', data: { was: 'a hold' } })];
  const dd = await drawn(D);
  check(dd.length === 2 && /RESTORED/.test(dd[1].action), `2d · a restore with no hold seal and no release keeps its RESTORED seal (the only trace of it): ${say(dd)}`);

  // e · Hold Order + Release Hold, then a later hold from the sheet window: the second hold is a seal of its own
  const F = '4299900005';
  sets[F] = sets[A].map(x => Object.assign({}, x, { orderId: F, id: x.id + '-f' })).concat([E(F, 'held', T0 + 600 * s, { text: 'Taken off SS Sheet 9 by Name Two, on hold', by: 'Name Two' })]);
  const f = await drawn(F);
  check(f.map(x => x.action).join() === 'ORDER RECEIVED,ORDER HELD,ORDER RELEASED,ORDER HELD', `2e · a hold after the release is a seal of its own: ${say(f)}`);
  w.close();
};

// ═══ 3 · the real sorter page ═══
const pagePart = async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  try { require(path.join(pwDir, 'playwright-core')); } catch (_) { console.log('seal-hold-release-pair: no playwright-core, the page part is skipped'); return; }
  const W = require('./_reviewSendWorld.cjs');
  const w = await W.open('production', { reducedMotion: 'reduce' });
  if (!w) { console.log('seal-hold-release-pair: no browser, the page part is skipped'); return; }
  const { page } = w, st = w.srv.st, rid = W.ORDERS.matched.rid, key = W.keyOf('matched');
  const until = async (f, ms, what) => { const t = Date.now() + ms; for (;;) { const v = await f(); if (v) return v; if (Date.now() > t) throw new Error('timed out: ' + what); await page.waitForTimeout(250); } };
  const tl = () => st.list('Order_Timeline').filter(e => String(e.orderId) === rid);
  try {
    await W.addOrders(w, ['matched']);
    await page.evaluate(() => { CN.setMode('orders'); Orders.render(); });
    await page.waitForSelector(`#ordItems [data-rid="${rid}"] .revSendBtn`, { timeout: 20000 });
    // the take-off: this world keeps its sheet in the cloud only, so the engine's run is stood in for, recording what the sheet window's take-off records (SheetWin takeOff: the line held,
    // and SheetEvents.order "held" with the sheet it came off); everything else is the page's own (the mark and its note, the Release Hold press, OrderHold.release, Review.repool)
    await page.evaluate(({ rid }) => {
      const real = window.OrderHold, T = window.__eng = { real, runs: 0 };
      window.OrderHold = Object.assign({}, real, {
        plan: async r => ({ rid: r, canHold: true, pieces: [{ poolId: r + '_p1', state: 'onSheet', sheetLabel: 'GF Sheet 2' }], sheets: [], fills: [], stays: [] }),
        run: async (r, o) => {
          T.runs++; const who = (o && o.name) || 'Test Operator', text = `Taken off GF Sheet 2 by ${who}`;
          for (const row of Orders.rows()) if (String(row.order.receiptId) === r) { row.poolIds = []; row.state = 'held'; row.hold = row.reason = text; row.heldAt = Date.now(); }
          window.SheetEvents.order({ type: 'held', orderId: r, id: `sw-sh-gf2-${Date.now()}`, by: who, sheetId: 'sh-gf2', sheet: 'GF Sheet 2', text: `${text}, on hold`, data: { pieces: 1 } });
          return { ok: true, held: true, steps: [] };
        }
      });
    }, { rid });
    // (the Hold Order button of the order window's piece row: the one that also takes the pieces off, HOLDBTN)
    await page.evaluate(k => OrderWin.open(k), key);
    const hb = '#owPcSum .revSendBtn[data-rev-host]';
    await page.waitForSelector(hb, { timeout: 20000 });
    const t0 = Date.now();
    await page.click(hb);
    await page.waitForFunction(r => CharmNestReviewMark.isMarked(r) && !ReviewSend.busy(r) && window.__eng.runs === 1, rid, { timeout: 30000 });
    await page.evaluate(() => { window.OrderHold = window.__eng.real; if (OrderWin.isOpen()) OrderWin.close(); }); await W.settle(page, 400);
    await until(() => tl().some(e => e.type === 'note') && tl().some(e => e.type === 'held'), 20000, 'the note and the held step in the cloud');
    // the Review card's Release Hold (the real press: ReviewRelease, OrderHold.release, Review.repool)
    await page.evaluate(() => { CN.setMode('review'); Review.render(); }); await W.settle(page, 400);
    const rel = `#rvList .reviewListRow[data-rid="${rid}"] [data-rel-hold]`;
    await page.waitForSelector(rel, { timeout: 20000 });
    await page.click(rel);
    const relEv = await until(() => tl().find(e => e.type === 'released' && e.data && e.data.seal === 'releaseHold'), 60000, 'the released step with its seal');
    await until(() => tl().some(e => e.type === 'restored'), 20000, 'the restored step');
    const kinds = tl().sort((x, y) => x.at - y.at).map(e => e.type + (e.type === 'note' ? '(hold)' : ''));
    const span = Math.round((relEv.at - t0) / 1000);
    check(['note(hold)', 'held', 'restored', 'released'].every(k => kinds.includes(k)), `3 · the records are all kept, as written by the page (${kinds.join(', ')}; the hold to the release in ${span} s)`);

    // the order window's Timeline tab
    await page.evaluate(() => { CN.setMode('orders'); Orders.render(); });
    await page.evaluate(k => OrderWin.open(k), key);
    await page.waitForFunction(() => window.OrderWin && OrderWin.isOpen(), null, { timeout: 20000 });
    await page.evaluate(() => OrderWin.view() === 'timeline' || document.querySelector('.owTabsV [data-ow-view="timeline"]').click());
    const seals = await until(() => page.evaluate(() => {
      const list = [...document.querySelectorAll('#orderWin .tlCanvas .tlSt[data-key] svg[data-seal-model]')].map(s => { const m = JSON.parse(s.dataset.sealModel), g = s.querySelector('g'); return { action: m.action, at: m.at, family: s.dataset.sealFamily, ink: g && g.getAttribute('stroke'), color: g && getComputedStyle(g).stroke }; });
      return list.some(x => x.action === 'ORDER RELEASED') ? list.sort((a, b) => a.at - b.at) : null;
    }), 30000, 'the Timeline tab drawing the release');
    const mine = seals.filter(x => x.action !== 'ORDER RECEIVED');
    check(mine.length === 2 && mine[0].action === 'ORDER HELD' && mine[1].action === 'ORDER RELEASED' && !seals.some(x => /ON HOLD|RESTORED/.test(x.action)),
      `3 · the order window's Timeline: exactly two seals for the hold and its release, ORDER HELD then ORDER RELEASED (${seals.map(x => x.action).join(' · ')})`);
    check(mine[1] && mine[1].family === 'release' && mine[1].ink === GREEN && mine[1].color === 'rgb(25, 102, 63)', `3 · ORDER RELEASED is drawn in green (${mine[1] && mine[1].ink}, computed ${mine[1] && mine[1].color})`);
    const nowSeal = await page.evaluate(() => { const s = document.querySelector('#orderWin .tlNowSeal svg[data-seal-model], #orderWin #owNowCard svg[data-seal-model]'); return s ? JSON.parse(s.dataset.sealModel).action : ''; });
    check(!/ON HOLD|RESTORED/.test(nowSeal), `3 · the window's "Where it is now" seal is no echo (${nowSeal || 'none drawn'})`);
    await page.evaluate(() => OrderWin.isOpen() && OrderWin.close()); await W.settle(page, 400);
    // the Orders card's release seal
    await page.evaluate(() => { CN.setMode('orders'); Orders.render(); });
    const card = await until(() => page.evaluate(r => {
      const n = document.querySelector(`#ordItems [data-rid="${r}"] .relSeal`); if (!n) return null;
      const b = n.querySelector('.relSealWords b'), g = n.querySelector('svg g'), cs = getComputedStyle(n);
      return { words: b && b.textContent, color: b && getComputedStyle(b).color, border: cs.borderTopColor, bg: cs.backgroundColor, ink: g && g.getAttribute('stroke') };
    }, rid), 20000, 'the release seal on the Orders card');
    check(card.words === 'Order released' && card.color === 'rgb(25, 102, 63)' && /rgba\(25, 102, 63, 0\.38\)/.test(card.border) && /rgba\(25, 102, 63, 0\.07\)/.test(card.bg) && card.ink === GREEN,
      `3 · the Orders card's ORDER RELEASED seal: green face, words, border and wash (${JSON.stringify(card)})`);
    check(w.errors.length === 0, '3 · no script error on the page: ' + (w.errors || []).join(' | '));
  } finally { await w.close(); }
};

(async () => {
  try { await jsdomPart(); } catch (e) { fails.push('jsdom: ' + (e && e.stack || e)); console.log('  FAIL jsdom: ' + (e && e.stack || e)); }
  try { await pagePart(); } catch (e) { fails.push('page: ' + (e && e.message || e)); console.log('  FAIL page: ' + (e && e.stack || e)); }
  console.log(`\nseal-hold-release-pair: ${passed} passed, ${fails.length} failed`);
  if (fails.length) process.exitCode = 1;
})();
