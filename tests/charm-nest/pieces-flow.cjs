// PIECES GATE, part 2 (the real page): EVERY PIECE of a multi-piece order is listed, carded, worded and approved on its own, in the ORDER DETAILS WINDOW and in the ENGRAVE TAB.
//
// Paul, 16 Oct 2026, order 4195059483 (header "2 pieces", ITS PIECE: ONE row "CORGI 34728 · SS · ×2", Engraving: ONE card with two approval stamps): "Customer ordered two of the same Charm, but the engraving
// only shows one, sometimes customers will request different engraving for each piece, even though the two pieces are the same. ALL multipiece orders must always show each individual piece with respect to all
// Engravings and Monograms."  Fixtures modelled on that order (made-up buyers, the COMET_9427 drawing stands for CORGI 34728): one line of one charm x2 in silver; the same with two different notes; x3; a plain
// charm x2 (nothing to engrave); a MIXED order (monogram x2, engraved bar x3, plain charm x2); an old whole-line approval of a x2 line; phone width.
// Over the REAL sorter page (charm-nest-1.html) in headless Chromium on the repo's offline fake site (_engraveRealWorld.cjs: real intake, pool, classifier, fit; stand-in monogram alphabets): no Etsy, no paid AI,
// nothing live. The slot spelling the model chose lives only in _piecesNames.cjs.
//   /tmp/cn-run.sh 900 tests/charm-nest/pieces-flow.cjs        (SHOTS=<dir> keeps screenshots; ONLY=F1,F5 runs some scenarios)
// What a pass does NOT prove: a live order on the live page (live data, live jobs, live Etsy), a live approval's server writes; only that the page's own code does this on the offline fake.
'use strict';
const fs = require('fs'), path = require('path');
const R = require('./_engraveRealWorld.cjs'), Base = require('./_orderFactsWorld.cjs'), A = require('./_monogramAlphabets.cjs'), N = require('./_piecesNames.cjs');
const SHOTS = process.env.SHOTS || '';
const only = (process.env.ONLY || '').split(',').filter(Boolean), want = id => !only.length || only.includes(id);

// the order as Etsy sends it: any number of lines, each with its own quantity, metal and words
function orderOf(fx, now) {
  const at = (d, h, m) => { const t = new Date(now); t.setDate(t.getDate() + d); t.setHours(h, m, 0, 0); return Math.floor(t.getTime() / 1000); };
  const shipBy = at(fx.ship, 17, 0), line = (x, i) => ({ transactionId: fx.rid + (i + 1), listingId: x.listing, sku: x.sku, title: x.title, quantity: x.qty || 1, expectedShipDate: shipBy, variations: x.variations || [], metalKey: x.metalKey || 'gold', metalLabel: x.metalLabel || 'GF 14/20', personalization: x.personalization || [], buyerMessage: '' });
  return { receiptId: fx.rid, orderNumber: fx.rid, createTs: at(fx.placed.day, fx.placed.h, fx.placed.m), updateTs: at(0, 12, 0), shipBy, buyer: { name: fx.buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: (fx.lines || [fx]).map(line) };
}
Base.orderOf = orderOf;   // (R.build serialises Base.orderOf into the page)
const V = (n, v) => ({ name: n, value: v });
const CORGI = (qty, words, extra) => Object.assign({ listing: '4494181970', sku: 'COMET_9427', title: 'Corgi Silhouette Charm Necklace: Sterling Silver, Gold Filled, Rose Gold, Handmade Animal Charm Jewelry Gift', qty, variations: [V('Metal choice', 'Silver'), V('Font Choice', 'Stylish')], personalization: words, metalKey: 'silver', metalLabel: 'SS' }, extra || {});
const BAR = qty => ({ listing: '1890000101', sku: 'COMET_9427', title: 'Comet Necklace, Celestial Charm Necklace in 14k Gold Fill', qty, variations: [V('Metal choice', 'Gold'), V('Font Choice', 'Stylish')], personalization: ['Name Six'], metalKey: 'gold' });
const CAT = qty => ({ listing: '1890000107', sku: 'SCHRODINGER CAT 1', title: 'Cat Necklace, Whimsical Cat Charm Gift', qty, variations: [V('Metal choice', 'Gold')], personalization: [], metalKey: 'gold' });
const MONO = qty => ({ listing: '997905351', sku: 'MONOGRAM_5351', title: 'Monogram Necklace Personalized Jewelry Initials Pendant', qty, variations: [V('Metal choice', 'Gold'), V('Chain length', '16 inches')], personalization: ['APV'], metalKey: 'gold' });
const ORD = (rid, buyer, hour, lines) => ({ rid, buyer, placed: { day: 0, h: hour, m: 10 }, ship: 9, lines });
R.ORDERS.cg2 = ORD('4295059401', 'Name One', 9, [CORGI(2, ['Name One'])]);                    // Paul's order: one line x2, one note: the same words on both pieces until a person edits one
R.ORDERS.cgDiff = ORD('4295059402', 'Name Two', 10, [CORGI(2, ['Anna', 'Ben'])]);            // two entries of the note: Anna on piece 1, Ben on piece 2
R.ORDERS.cg3 = ORD('4295059403', 'Name Three', 11, [CORGI(3, ['Name Three'])]);              // three pieces
R.ORDERS.plain2 = ORD('4295059404', 'Name Four', 12, [CAT(2)]);                              // two pieces, nothing to engrave: no cards, the Pieces list still lists both
R.ORDERS.mixed = ORD('4295059405', 'Name Five', 13, [MONO(2), BAR(3), CAT(2)]);              // monogram x2 + engraved bar x3 + plain charm x2 = 7 pieces, 5 with an engraving or a monogram
R.ORDERS.old = ORD('4295059406', 'Name Six', 14, [CORGI(2, ['Name Six'])]);                  // an old whole-line approval (migration)
R.ORDERS.ears2 = ORD('4295059407', 'Name Seven', 15, [{ listing: '1890000106', sku: 'COMET_9427', title: 'Comet Stud Earrings, Celestial Earrings in 14k Gold Fill', qty: 2, variations: [V('Metal choice', 'Gold'), V('Style', 'Stud earrings')], personalization: [], metalKey: 'gold' }]);   // earrings x2: never engraved, four pieces (two pairs)
R.ORDERS.discs2 = ORD('4295059408', 'Name Eight', 16, [{ listing: '1890000105', sku: 'COMET_9427', title: 'Initial Disc Necklace, Personalized Letter Discs', qty: 2, variations: [V('Metal choice', 'Gold'), V('Number of Discs', '3 Discs')], personalization: ['Tag 1: A, Tag 2: B, Tag 3: C'], metalKey: 'gold' }]);   // a 3-disc necklace x2: six pieces
// (a minimum of 1 job each: the world waits for these; the scenarios below count the real number)
Object.assign(R.EXPECT_JOBS, { cg2: 1, cgDiff: 1, cg3: 1, plain2: 0, mixed: 1, old: 1, ears2: 0, discs2: 1 });

const fails = [], tally = { pass: 0, fail: 0 }, perId = {};
let cur = '';
const note = (ok, what, more) => { tally[ok ? 'pass' : 'fail']++; (perId[cur] ||= { pass: 0, fail: 0 })[ok ? 'pass' : 'fail']++; console.log((ok ? '  PASS ' : '  FAIL ') + cur + ' ' + what + (more ? '  [' + String(more).slice(0, 300) + ']' : '')); if (!ok) fails.push(cur + ' ' + what + (more ? ' [' + String(more).slice(0, 200) + ']' : '')); };
const sleep = ms => new Promise(r => setTimeout(r, ms));

(async () => {
  const w = await R.world({ viewport: { width: 1440, height: 900 }, reducedMotion: 'reduce' });
  if (!w) { console.log('pieces-flow: no playwright-core, skipped'); return; }
  const { page, srv } = w;
  const cons = []; page.on('console', m => { if (m.type() === 'error') cons.push(m.text()); });
  const settle = (ms = 500) => R.settle(page, ms);
  try {
    // stand-in alphabets for the monogram line (this test is about pieces, not the letters)
    A.seed(srv.st, await A.make());
    await page.evaluate(async () => { await Master.load(true); });
    await page.evaluate(() => { const eb = CharmNestMonogram.build; CharmNestMonogram.build = function (l) { const r = eb.apply(this, arguments); if (r && r.ok && r.report && r.report.metrics && Array.isArray(r.report.metrics.violations)) r.report.metrics = Object.assign({}, r.report.metrics, { violations: [], ok: true }); return r; }; });
    const ALL = ['cg2', 'cgDiff', 'cg3', 'plain2', 'mixed', 'old', 'ears2', 'discs2'], NEED = { F1: ['cg2'], F2: ['cgDiff'], F3: ['cg2'], F4: ['cg2'], F5: ['cg3', 'cgDiff'], F6: ['cg3'], F7: ['plain2'], F8: ['mixed'], F9: ['old'], F10: ['cg3'], F12: ['cg2'], F13: ['ears2'], F14: ['discs2'] };
    const NAMES = only.length ? ALL.filter(n => only.some(id => (NEED[id] || ALL).includes(n))) : ALL;
    const made = await R.build(w, NAMES);
    const RID = n => R.ORDERS[n].rid;
    const lineKeys = n => page.evaluate(rid => Orders.rows().filter(r => r.order.receiptId === rid).map(r => r.key), RID(n));
    const jobsOf = n => page.evaluate(rid => [...Engrave.items().values()].filter(j => j.row && j.row.order && String(j.row.order.receiptId) === rid && !j.mono).map(j => ({ key: j.key, slot: j.slot || null, face: j.face === 'front' ? 'front' : 'back', state: j.state, text: (j.lines && j.lines.length ? j.lines.join('\n') : j.text) || '', copies: (j.copies || []).slice(), seals: ((j.engraveRec && j.engraveRec.seals) || []).length + (j.engravingSeals || []).length })).sort((a, b) => { const S = window.CharmNestEngraveSides, rk = x => (S && S.slotRank && x ? S.slotRank(x) : 0); return (rk(a.slot) - rk(b.slot)) || (a.key < b.key ? -1 : 1); }), RID(n));   // (piece order: unit-major, as the model ranks the slots)
    const tailOf = k => String(k).replace(/^\d+_\d+/, '');

    // ── what the Order details window shows, read in the page ──
    const WIN = () => {
      const dlg = document.getElementById('orderWin'), box = document.getElementById('owPcSum'), host = document.getElementById('owEng'), sec = document.getElementById('owEngSec');
      // the entries of the list, in document order: a line row that has piece rows under it is their header, never an entry of its own
      const rows = box ? [...box.querySelectorAll('.owPcRow')] : [], subs = box ? [...box.querySelectorAll('.owPcSub')] : [], ent = [];
      let pending = null, pendingSubs = 0;
      for (const n of box ? box.querySelectorAll('.owPcRow, .owPcSub') : []) {
        if (n.classList.contains('owPcRow')) { if (pending && !pendingSubs) ent.push(pending); pending = n; pendingSubs = 0; }
        else { pendingSubs++; ent.push(n); }
      }
      if (pending && !pendingSubs) ent.push(pending);
      const sub = (dlg && [...dlg.querySelectorAll('.owSub')].map(n => n.textContent).join(' | ')) || '', m = /(\d+)\s+pieces?/i.exec(sub);
      const cards = host ? [...host.querySelectorAll('.engCard')].map(c => { const ap = c.querySelector('[data-e=approve]'); return { piece: c.dataset.piece || '', slot: c.dataset.slot || '', state: c.dataset.state, title: ((c.querySelector('.engName') || {}).textContent || '').trim(), text: c.innerText.replace(/\s+/g, ' ').trim(), words: [...c.querySelectorAll('.engTxt')].map(x => x.textContent.trim()).join('|'), approve: !!ap && !ap.disabled, approveBtn: !!ap, seals: c.querySelectorAll('.seal').length }; }) : [];
      const monos = host ? [...host.querySelectorAll('[data-testid="ow-mono"]')].map(c => { const mw = c.querySelector('.mw'); return { key: c.dataset.key || '', piece: c.dataset.piece || '', text: c.innerText.replace(/\s+/g, ' ').trim(), state: mw ? mw.dataset.state : '', busy: mw ? mw.dataset.busy || '' : '', approve: !!c.querySelector('[data-mw-approve]'), letters: [...c.querySelectorAll('[data-mw-letter]')].map(i => i.value).join('') }; }) : [];
      const sr = sec ? sec.getBoundingClientRect() : null;
      return { header: sub, headerN: m ? +m[1] : null, listText: box ? box.innerText.replace(/\s+/g, ' ').trim() : '', rows: rows.length, subs: subs.length, entries: ent.length, entryTexts: ent.map(s => s.innerText.replace(/\s+/g, ' ').trim()),
        cards, monos, hostHidden: !host || host.hidden, secH: sr ? Math.round(sr.height * 10) / 10 : -1, scrollX: document.documentElement.scrollWidth - innerWidth, dlgScrollX: dlg ? dlg.scrollWidth - dlg.clientWidth : 0,
        outside: [...(host ? host.querySelectorAll('.engCard, [data-testid="ow-mono"]') : [])].filter(c => { const r = c.getBoundingClientRect(), d = dlg.getBoundingClientRect(); return r.right > d.right + 1 || r.left < d.left - 1; }).length };
    };
    const win = () => page.evaluate(`(${WIN})()`);
    const openWin = async (key, ms = 900) => {
      // (a window that is open is closed and waited for; one that is not is never "closed": the page keeps that state)
      if (await page.evaluate(() => { const d = document.getElementById('orderWin'); if (d && d.open) { try { OrderWin.close(); } catch (_) {} return true; } return false; })) await page.waitForFunction(() => { const d = document.getElementById('orderWin'); return !d || !d.open; }, null, { timeout: 5000 }).catch(() => {});
      await page.evaluate(async k => { CN.setMode('orders'); await OrderWin.open(k); }, key); await page.waitForSelector('#orderWin[open]', { timeout: 15000 }).catch(() => {});
      // (the list of pieces waits for the order's timeline, the Engraving section for its cards / widgets: both are settled before anything is read)
      await page.waitForFunction(() => { const b = document.getElementById('owPcSum'), h = document.getElementById('owEng'); return !!b && !b.hidden && !!b.querySelector('.owPcRow') && (!h || h.hidden || h.children.length > 0); }, null, { timeout: 20000 }).catch(() => {}); await settle(ms);
    };
    const until = async (fn, what, ms = 25000, arg) => { const t0 = Date.now(); let v; for (;;) { v = await page.evaluate(fn, arg); if (v) return v; if (Date.now() - t0 > ms) { console.log('   (waiting for ' + what + ')'); return null; } await sleep(120); } };

    // ── the Engrave tab's detail, read in the page ──
    const READ = () => {
      const c = document.querySelector('#egQueue .rvItem'); if (!c) return null; const q = s => c.querySelector(s);
      const on = c.querySelector('.egEarTab[aria-pressed=true] .egPiece');
      return { key: c.dataset.key, rid: ((q('.reviewIdentity .ttl') || {}).textContent || '').trim(), of: ((q('.egPcOf') || {}).textContent || (on || {}).textContent || '').trim(), tabs: [...c.querySelectorAll('.egPcTab, .egEarTab')].map(b => ({ on: b.getAttribute('aria-pressed') === 'true', piece: b.dataset.piece || b.dataset.ear, state: b.dataset.state, text: b.innerText.replace(/\s+/g, ' ').trim() })),
        cardPiece: (q('.engCard') || { dataset: {} }).dataset.piece || '', cardTxt: [...c.querySelectorAll('.engCard .engTxt')].map(x => x.textContent.trim()).join('|'), words: (q('.engp .engpWords') || {}).value || '', approve: !!q('[data-a=approve]') && !q('[data-a=approve]').disabled,
        text: c.innerText.replace(/\s+/g, ' ').trim(), mono: !!q('.mw'), scrollX: document.documentElement.scrollWidth - innerWidth };
    };
    const rd = () => page.evaluate(`(${READ})()`);
    const openDetail = async key => { await page.evaluate(k => { try { const d = document.getElementById('orderWin'); if (d && d.open) OrderWin.close(); } catch (_) {} CN.setMode('engrave'); Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: k, q: '' }); Engrave.render(); }, key); await page.waitForSelector('#egQueue .rvItem', { timeout: 20000 }).catch(() => {}); await settle(800); return rd(); };
    const press = async (sel, o) => { await page.evaluate(s => { const b = document.querySelector(s); if (b) b.click(); }, sel); await settle(500); if (!(o && o.hold) && await page.evaluate(() => { const b = document.querySelector('#egQueue .egGuide:not([hidden]) [data-g=leave]'); if (b) { b.click(); return true; } return false; })) await settle(500); return rd(); };
    const NEXT = '#egQueue .rvItem [data-a="next"]', PREV = '#egQueue .rvItem [data-a="prev"]';
    // type new words into the Engrave detail's words box and press its Save (the button wakes once the box is dirty: give the panel a moment between the two)
    const typeAndSave = async words => {
      const typed = await page.evaluate(w => { const ta = document.querySelector('#egQueue .rvItem .engp .engpWords'); if (!ta) return false; ta.focus(); ta.value = w; ta.dispatchEvent(new Event('input', { bubbles: true })); return true; }, words);
      await settle(350);
      const sv = await page.evaluate(() => { const b = document.querySelector('#egQueue [data-engp=save]'); if (!b) return 'none'; if (b.disabled) return 'disabled'; b.click(); return 'pressed'; });
      await settle(350);
      // (changing words that were already there asks "Why change it?": one tap on a reason, the page's own flow)
      const why = await page.evaluate(() => { const b = document.querySelector('#egQueue [data-engp="why"][data-code="asked"]'); if (!b) return false; b.click(); return true; });
      return { typed, save: sv, why };
    };
    // the height of the Engraving section and the place of the pieces list, watched on every animation frame: the window must not jump while a piece is approved
    const watchStart = () => page.evaluate(() => { window.__w = { on: true, h: [], t: [] }; const loop = () => { const w = window.__w; if (!w || !w.on) return; const s = document.getElementById('owEngSec'), p = document.getElementById('owPcSum'); if (s && !s.hidden) w.h.push(Math.round(s.getBoundingClientRect().height * 10) / 10); if (p && !p.hidden) w.t.push(Math.round(p.getBoundingClientRect().top * 10) / 10); requestAnimationFrame(loop); }; requestAnimationFrame(loop); });
    const watchStop = () => page.evaluate(() => { const w = window.__w || { h: [], t: [] }; w.on = false; const span = a => (a.length ? Math.round((Math.max(...a) - Math.min(...a)) * 10) / 10 : 0); return { frames: w.h.length, hSpan: span(w.h), tSpan: span(w.t) }; });
    const consoleBad = () => cons.filter(x => /TypeError|ReferenceError|is not defined|Cannot read|Uncaught|SyntaxError/.test(x));

    const K = {}; for (const n of NAMES) K[n] = await lineKeys(n);
    note(Object.values(K).every(a => a.length >= 1) && (!K.mixed || K.mixed.length === 3), 'fixtures are in the page (' + NAMES.length + ' orders; the mixed order has 3 lines)', NAMES.map(n => n + ':' + K[n].length).join(' '));

    // ═══ F1 · Paul's order in the ORDER DETAILS WINDOW: every piece listed, carded, worded, with its own Approve ═══
    cur = 'F1';
    if (want('F1')) {
      await openWin(K.cg2[0]);
      let s = await win();
      note((s.headerN === null || s.headerN === 2), 'the header, when it carries a count (OWHEAD removed the line), says "2 pieces"', s.header);
      note(s.entries === 2 && !/×\s*2/.test(s.listText), 'ITS PIECE lists TWO pieces, one row each, and no "×2" row (' + s.entries + ' entries)', s.listText);
      note(s.entryTexts.length === 2 && N.pieceWord(1, 2).test(s.entryTexts.join(' | ')) || (/piece\s*1/i.test(s.entryTexts[0] || '') && /piece\s*2/i.test(s.entryTexts[1] || '')), 'the two rows are named Piece 1 and Piece 2', s.entryTexts.join(' | '));
      note((s.headerN === null || s.headerN === s.entries), 'the header count and the list agree (' + s.headerN + ' / ' + s.entries + ')');
      note(s.cards.length === 2, 'the Engraving section has TWO cards, one per piece (' + s.cards.length + ')', s.cards.map(c => c.title).join(' | '));
      note(s.cards.length === 2 && N.pieceWord(1, 2).test(s.cards[0].title + ' ' + s.cards[0].text) && N.pieceWord(2, 2).test(s.cards[1].title + ' ' + s.cards[1].text), 'the cards are titled "Piece 1 of 2" and "Piece 2 of 2"', s.cards.map(c => c.title).join(' | '));
      note(s.cards.length === 2 && new Set(s.cards.map(c => c.piece)).size === 2 && s.cards.every(c => c.approve), 'each card has its own Approve and its own piece key', s.cards.map(c => c.piece + ':' + c.approve).join(' '));
      const js = await jobsOf('cg2');
      note(js.length === 2 && new Set(js.map(j => j.key)).size === 2 && js.every(j => j.copies.length === 1), 'two Engrave jobs, one pool id each (' + js.length + ' jobs: ' + js.map(j => tailOf(j.key) + '=' + j.copies.length).join(' ') + ')');
      note(s.cards.length === 2 && s.cards.every(c => /Name One/.test(c.words)), 'no note says "different": both pieces carry the buyer\'s whole text, shown on each', s.cards.map(c => c.words).join(' | '));
      if (SHOTS) await R.shot(w, 'pieces-window-cg2');
    }

    // ═══ F2 · different words per piece: the buyer's two entries are assigned in order ═══
    cur = 'F2';
    if (want('F2')) {
      await openWin(K.cgDiff[0]);
      const s = await win(), js = await jobsOf('cgDiff');
      note(js.length === 2 && js[0].text === 'Anna' && js[1].text === 'Ben', 'piece 1 holds "Anna", piece 2 holds "Ben" (the note\'s two parts, in order)', JSON.stringify(js.map(j => [tailOf(j.key), j.text])));
      note(s.cards.length === 2 && /Anna/.test(s.cards[0].words) && !/Ben/.test(s.cards[0].words) && /Ben/.test(s.cards[1].words) && !/Anna/.test(s.cards[1].words), 'each card shows its own words', s.cards.map(c => c.words).join(' | '));
    }

    // ═══ F3 · words are edited per piece: editing piece 2 never changes piece 1 ═══
    cur = 'F3';
    if (want('F3')) {
      const before = await jobsOf('cg2'), k2 = (before[1] || {}).key, k1 = (before[0] || {}).key;
      let r = k2 ? await openDetail(k2) : null;
      note(!!r && r.key === k2 && r.words === 'Name One', 'piece 2 opens in the Engrave tab with the same words as piece 1 (Name One)', r && (r.key + ' / ' + r.words));
      if (r && r.key === k2) {
        const ts = await typeAndSave('Name Two'); note(ts.typed && ts.save === 'pressed', 'the words box takes the new words and its Save is there to press', JSON.stringify(ts));
        await until(k => { const j = Engrave.items().get(k); return !!j && /Name Two/.test((j.lines || []).join(' ') + ' ' + (j.text || '')) && !['classify', 'ready', 'fitting'].includes(j.state); }, 'piece 2 refitted', 40000, k2);
        if (process.env.PF_DEBUG) console.log('   DEBUG F3 after save:', JSON.stringify(await page.evaluate(k => { const j = Engrave.items().get(k), row = B.orders.byKey.get(k.replace(/#.*$/, '')); return { job: j && { state: j.state, text: j.text, lines: j.lines, seq: j.lineInput }, ov: row && row.engravingOverride, panel: (document.querySelector('#egQueue .engp .engpWords') || {}).value, saveDisabled: !!(document.querySelector('#egQueue [data-engp=save]') || {}).disabled, msg: (document.querySelector('#egQueue .engp') || { innerText: '' }).innerText.slice(0, 200) }; }, k2)));
        const after = await jobsOf('cg2'), a1 = after.find(j => j.key === k1), a2 = after.find(j => j.key === k2);
        note(!!a1 && !!a2 && /Name Two/.test(a2.text) && a1.text === 'Name One', 'piece 2 now says "Name Two"; piece 1 still says "Name One"', JSON.stringify(after.map(j => [tailOf(j.key), j.text, j.state])));
        note(!!a1 && a1.state === (before[0] || {}).state, 'editing piece 2 left piece 1\'s state alone', (a1 || {}).state + ' / ' + (before[0] || {}).state);
      }
    }

    // ═══ F4 · approve each piece on its own; the order is done only when every piece is; nothing can skip a piece ═══
    cur = 'F4';
    if (want('F4')) {
      await openWin(K.cg2[0]);
      let s = await win(); const js0 = await jobsOf('cg2');
      note(s.cards.length === 2 && s.cards.every(c => c.approve), 'both pieces are waiting for their own Approve', JSON.stringify(s.cards.map(c => c.approve)));
      if (s.cards.length >= 1) {
        await watchStart();
        await page.evaluate(() => document.querySelector('#owEng .engCard [data-e=approve]').click());
        await until(() => [...Engrave.items().values()].some(j => String((j.row.order || {}).receiptId) === '4295059401' && j.state === 'approved'), 'first approval', 40000);
        await settle(900);
        const wj = await watchStop();
        note(wj.frames > 5 && wj.hSpan <= 2 && wj.tSpan <= 2, 'no layout jump while a piece is approved: the Engraving section keeps its height and the pieces list its place (within 2 px)', JSON.stringify(wj));
        const js = await jobsOf('cg2'); s = await win();
        note(js.length === 2 && js.filter(j => j.state === 'approved').length === 1 && js.filter(j => j.state !== 'approved').length === 1, 'Approve on the first card approved ONE piece only', JSON.stringify(js.map(j => [tailOf(j.key), j.state])));
        note(s.cards.length === 2 && s.cards.filter(c => c.approve).length === 1 && s.cards.filter(c => c.state === 'approved').length === 1, 'the other card still has its Approve; the approved one shows its seal in the Approve\'s place', JSON.stringify(s.cards.map(c => [c.state, c.approve, c.seals])));
        const O = await page.evaluate(rid => { const o = CharmNestEngraveOrder.countOf(rid); return o; }, RID('cg2'));
        note(O.n === 2 && O.approved === 1 && O.left === 1, 'the order is NOT done: 2 pieces, 1 approved, 1 left (worst-first)', JSON.stringify(O));
        const line = await page.evaluate(k => { const r = B.orders.byKey.get(k), e = r && r.engrave; return e ? { approved: !!e.approved, state: e.state } : null; }, K.cg2[0]);
        note(!!line && line.approved === false, 'the line\'s own record says not approved (Send to Sheet stays gated)', JSON.stringify(line));
        // the second
        await page.evaluate(() => { const b = [...document.querySelectorAll('#owEng .engCard [data-e=approve]')].find(x => !x.disabled); if (b) b.click(); });
        await until(() => [...Engrave.items().values()].filter(j => String((j.row.order || {}).receiptId) === '4295059401' && j.state === 'approved').length === 2, 'second approval', 40000);
        await settle(900);
        const O2 = await page.evaluate(rid => CharmNestEngraveOrder.countOf(rid), RID('cg2')), js2 = await jobsOf('cg2');
        note(O2.left === 0 && O2.approved === 2 && js2.every(j => j.state === 'approved' && j.seals >= 1), 'both approved on their own presses, each with its own seal; the order is done', JSON.stringify(O2) + ' ' + JSON.stringify(js2.map(j => j.seals)));
        note(new Set(js2.map(j => j.copies.join())).size === 2, 'each piece\'s record is its own pool id');
      }
    }

    // ═══ F5 · Engrave tab: the strip and Back / Next cycle every piece ("Piece 1 of 2"); nothing is skipped ═══
    cur = 'F5';
    if (want('F5')) {
      const j3 = await jobsOf('cg3'); const keys3 = j3.map(j => j.key);
      note(keys3.length === 3, 'a x3 line has three jobs (' + keys3.length + ')', keys3.map(tailOf).join(' '));
      let r = keys3[0] ? await openDetail(keys3[0]) : null;
      note(!!r && /^piece 1 of 3$/i.test(r.of) && r.tabs.length === 3 && r.tabs[0].on, 'the detail of piece 1 says "Piece 1 of 3" with one small button per piece', r && (r.of + ' / ' + r.tabs.length));
      if (process.env.PF_DEBUG) console.log('   DEBUG F5 head:', JSON.stringify(await page.evaluate(() => { const c = document.querySelector('#egQueue .rvItem'); return c ? { identity: (c.querySelector('.reviewIdentity') || {}).innerHTML, sw: [...c.querySelectorAll('[class*=egEar],[class*=egPc],[class*=Switch],[data-a=piece],[data-a=ear]')].map(n => n.className + '|' + n.textContent.trim().slice(0, 40)), text: c.innerText.slice(0, 300) } : null; })));
      const seen = [r && r.key];
      for (let i = 0; i < 2; i++) { r = await press(NEXT); seen.push(r && r.key); }
      note(seen.join() === keys3.join() && r && /^piece 3 of 3$/i.test(r.of), 'Next, Next visits piece 1, 2, 3 in order: no piece is skipped', seen.map(tailOf).join(' '));
      r = await press(PREV); r = await press(PREV);
      note(r && r.key === keys3[0] && /^piece 1 of 3$/i.test(r.of), 'Back, Back is piece 1 again', r && r.key);
      r = await press(PREV, { hold: true });
      note(!!r, 'Back from the first piece goes somewhere sensible (' + (r && r.of) + ')');
      // two pieces: the other order of the same words
      const j2 = (await jobsOf('cgDiff')).map(j => j.key); r = j2[0] ? await openDetail(j2[0]) : null;   // (a x2 line nobody has approved: an approved piece leaves the placement queue)
      note(!!r && /^piece 1 of 2$/i.test(r.of), 'a x2 line says "Piece 1 of 2"', r && r.of);
      r = await press(NEXT); note(!!r && /^piece 2 of 2$/i.test(r.of) && r.key === j2[1], 'Next goes to piece 2 of the SAME order', r && (r.of + ' ' + r.key));
      // each piece card in the Engrave detail carries its own words and its own state
      note(!!r && r.cardPiece === j2[1] && /Ben/.test(r.cardTxt), 'the detail card is piece 2\'s own (Ben)', r && (r.cardPiece + ' ' + r.cardTxt));
    }

    // ═══ F6 · the Engrave tab LIST: every piece is listed ═══
    cur = 'F6';
    if (want('F6')) {
      await page.evaluate(rid => { try { const d = document.getElementById('orderWin'); if (d && d.open) OrderWin.close(); } catch (_) {} CN.setMode('engrave'); Engrave.restoreView({ tab: 'place', list: true, chosen: true, focus: null, pin: null, openDone: null, q: rid }); Engrave.render(); }, RID('cg3'));
      await settle(900);
      const rows = await page.evaluate(rid => [...document.querySelectorAll('#egQueue .placementRow, #egQueue .doneRow')].filter(r => r.offsetParent !== null && String(r.dataset.rid || '') === rid).map(r => ({ key: r.dataset.key || r.dataset.open || '', text: r.innerText.replace(/\s+/g, ' ').trim(), ears: r.querySelectorAll('.egEar').length })), RID('cg3'));
      const joined = rows.map(r => r.text).join(' | ');
      note(rows.length >= 1 && rows.reduce((a, r) => a + Math.max(1, r.ears), 0) >= 3 && N.pieceWord(1, 3).test(joined) && N.pieceWord(2, 3).test(joined) && N.pieceWord(3, 3).test(joined), 'the list names Piece 1 of 3, Piece 2 of 3 and Piece 3 of 3 (as rows or as lines of one row)', joined);
      const counted = await page.evaluate(() => (document.querySelector('.egTab[data-tab=place] .n, .egTab .n') || {}).textContent || '');
      note(true, 'tab count text (informational): ' + counted);
    }

    // ═══ F7 · a plain charm x2: nothing to engrave, no card, but the Pieces list lists both ═══
    cur = 'F7';
    if (want('F7')) {
      await openWin(K.plain2[0]);
      const s = await win(), js = await jobsOf('plain2');
      note((s.headerN === null || s.headerN === 2) && s.entries === 2, 'the order says "2 pieces" and lists TWO pieces', s.header + ' / ' + s.entries + ' entries: ' + s.listText);
      note(s.cards.length === 0 && s.monos.length === 0 && js.length === 0, 'no engraving: no card, no job');
    }

    // ═══ F8 · MIXED order: monogram x2 + engraved bar x3 + plain charm x2 ═══
    cur = 'F8';
    if (want('F8')) {
      await openWin(K.mixed[0], 1500);
      await until(() => { const ws = [...document.querySelectorAll('#owEng .mw')]; return ws.length >= 1 && ws.every(e => !e.dataset.busy); }, 'the monogram widgets', 40000);
      let s = await win();
      note((s.headerN === null || s.headerN === 7) && s.entries === 7, 'the header says "7 pieces" and the list has 7 entries (2 + 3 + 2)', s.header + ' / ' + s.entries);
      note(s.monos.length === 2, 'the two monogram pieces each show the monogram widget (' + s.monos.length + ')', s.monos.map(m => m.key + ':' + m.letters).join(' | '));
      note(s.monos.length === 2 && s.monos.every(m => m.letters === 'APV'), 'the default letters are the three the buyer wrote, on each piece', s.monos.map(m => m.letters).join(' | '));
      note(s.monos.length === 2 && s.monos.every(m => m.approve), 'each monogram piece has its own Approve', JSON.stringify(s.monos.map(m => m.approve)));
      note(s.monos.length === 2 && N.pieceWord(1, 2).test(s.monos[0].text) && N.pieceWord(2, 2).test(s.monos[1].text), 'the monogram pieces are titled "Piece 1 of 2" and "Piece 2 of 2"', s.monos.map(m => m.text.slice(0, 40)).join(' | '));
      note(s.monos.length === 2 && s.monos.every((m, i) => m.key === K.mixed[0] + '#' + N.copySlot(i + 1)), 'each monogram piece is keyed like the contract says: <line>#C1, <line>#C2 (contract section 7)', s.monos.map(m => m.key.replace(/^\d+_/, '')).join(' | '));
      note(s.cards.length === 3 && N.pieceWord(1, 3).test(s.cards[0].title + ' ' + s.cards[0].text) && N.pieceWord(3, 3).test(s.cards[2].title + ' ' + s.cards[2].text), 'the engraved bar x3 has three cards "Piece 1 of 3" .. "Piece 3 of 3"', s.cards.map(c => c.title).join(' | '));
      const js = await jobsOf('mixed'); note(js.length === 3, 'the bar x3 has three jobs (' + js.length + ')');
      // the plain charm x2 adds no card; the section holds monogram pieces first (lines\' order), then the bar's
      const order = await page.evaluate(() => [...document.querySelectorAll('#owEng > *')].map(n => n.matches('[data-testid="ow-mono"]') ? 'mono' : n.classList.contains('engCard') ? 'card' : 'other').join(','));
      note(/^(mono,){2}(card,){2}card$/.test(order) || /^mono,mono,card,card,card$/.test(order), 'the section lists the pieces in the lines\' order: monogram, monogram, bar, bar, bar', order);
      // the whole order's pieces as the Engrave tab counts them
      const cnt = await page.evaluate(rid => CharmNestEngraveOrder.countOf(rid), RID('mixed'));
      note(cnt.n === 5, 'the order model counts 5 pieces with an engraving or a monogram', JSON.stringify(cnt));
      // approve monogram piece 1: piece 2 and the bars stay to approve
      if (s.monos.length >= 2) {
        await page.evaluate(() => { const b = document.querySelectorAll('#owEng [data-testid="ow-mono"] [data-mw-approve]')[0]; if (b) b.click(); });
        await settle(1500);
        s = await win();
        note(s.monos.length === 2 && s.monos[0].state === 'approved' && s.monos[1].state !== 'approved', 'approving monogram piece 1 does not approve piece 2', JSON.stringify(s.monos.map(m => m.state)));
        note(s.cards.every(c => c.state !== 'approved'), 'nor any of the bar\'s pieces');
        const files = await page.evaluate(() => { try { return Object.values(CustomSheet.entries()).flatMap(e => (e.files || []).map(f => f.name)); } catch (_) { return null; } });
        note(Array.isArray(files) && files.filter(f => /monogram/i.test(f)).length === 1 && /piece\s*1/i.test(files.filter(f => /monogram/i.test(f))[0] || ''), 'approving piece 1 writes ITS design only (one monogram file, named for piece 1)', JSON.stringify(files));
      }
      if (SHOTS) await R.shot(w, 'pieces-window-mixed');
    }

    // ═══ F9 · MIGRATION: an old whole-line approval of a x2 line: both pieces read as approved, the seals are kept, nothing asks again ═══
    cur = 'F9';
    if (want('F9')) {
      // approve for real (the real seals), then turn the jobs back into the OLD shape: ONE job under the line key holding both pieces, approved
      await openWin(K.old[0]);
      let s = await win();
      if (s.cards.length >= 1) {
        await page.evaluate(() => { const b = [...document.querySelectorAll('#owEng .engCard [data-e=approve]')][0]; if (b) b.click(); });
        await until(() => [...Engrave.items().values()].some(j => String((j.row.order || {}).receiptId) === '4295059406' && j.state === 'approved'), 'the old order\'s approval', 40000);
        await settle(800);
      }
      const legacy = await page.evaluate(k => {
        const row = B.orders.byKey.get(k), jobs = Engrave.jobsOf(row), done = jobs.find(j => j.state === 'approved'); if (!done) return null;
        const seals = [].concat((done.engraveRec && done.engraveRec.seals) || [], done.engravingSeals || []);
        for (const j of jobs) Engrave.items().delete(j.key);
        const old = Object.assign({}, done, { key: k, row, copies: row.poolIds.slice() }); delete old.slot; delete old.rowKey; delete old.linked;
        old.engraveRec = Object.assign({}, done.engraveRec || {}, { approved: true, state: 'approved' });
        try { delete row._engraveLinked; Object.defineProperty(row, 'engrave', { value: Object.assign({}, old.engraveRec), writable: true, enumerable: true, configurable: true }); } catch (e) { console.warn('legacy row', e && e.message); }
        Engrave.items().set(k, old);
        return { seals: seals.length, text: (old.lines || []).join(' / '), copies: old.copies.length };
      }, K.old[0]);
      note(!!legacy && legacy.copies === 2 && legacy.seals >= 1, 'an old whole-line record, approved with seals, covers both pieces of the x2 line', JSON.stringify(legacy));
      if (legacy) {
        // the page meets the old record again (a restore, a pull): the line is read piece by piece
        await page.evaluate(k => { const row = B.orders.byKey.get(k); try { if (Engrave.ensureJobs) Engrave.ensureJobs(row); else if (Engrave.ensureJob) Engrave.ensureJob(row); } catch (e) { console.warn('ensure', e && e.message); } try { if (Engrave.relinkAll) Engrave.relinkAll(); } catch (_) {} try { Engrave.render(); } catch (_) {} }, K.old[0]);
        await openWin(K.old[0], 1200);
        s = await win(); const js = await jobsOf('old');
        note(js.length === 2 && js.every(j => j.state === 'approved'), 'two jobs, BOTH approved: no approval was lost and none is asked again', JSON.stringify(js.map(j => [tailOf(j.key), j.state, j.seals])));
        note(js.length === 2 && js.every(j => j.text === 'Name Six') && js.every(j => j.seals >= legacy.seals), 'the same words and the seals already given are on each piece', JSON.stringify(js.map(j => [j.text, j.seals])));
        note(s.cards.length === 2 && s.cards.every(c => c.state === 'approved' && !c.approveBtn), 'both cards read approved and have no Approve button', JSON.stringify(s.cards.map(c => [c.state, c.approveBtn])));
        // an edit of ONE piece afterwards leaves the other's approval alone
        const k2 = (js[1] || {}).key;
        note(!!k2, 'the second piece has a job of its own to edit later', k2);
      }
    }

    // ═══ F10 · phone width (400 px): nothing runs past the window, no sideways scroll, no console error; and the window does not jump ═══
    cur = 'F10';
    if (want('F10')) {
      await page.setViewportSize({ width: 400, height: 860 }); await settle(500);
      await openWin(K.cg3[0], 1200);
      let s = await win();
      if (process.env.PF_DEBUG) console.log('   DEBUG F10:', JSON.stringify(await page.evaluate(() => { const dlg = document.getElementById('orderWin'), host = document.getElementById('owEng'), wide = []; for (const n of dlg.querySelectorAll('*')) { const r = n.getBoundingClientRect(); if (r.width > 0 && r.right > dlg.getBoundingClientRect().right + 1) wide.push((n.id || n.className || n.tagName).toString().slice(0, 40) + ':' + Math.round(r.left) + '-' + Math.round(r.right)); } return { dlg: Math.round(dlg.getBoundingClientRect().width), host: host ? Math.round(host.getBoundingClientRect().width) : 0, cards: [...host.children].map(c => Math.round(c.getBoundingClientRect().width)), over: wide.slice(0, 14) }; })));
      const pcOut = await page.evaluate(() => { const bad = [], dlg = document.getElementById('orderWin'); for (const sel of ['#owEng', '#owPcSum']) { const box = document.querySelector(sel); if (!box || box.hidden) continue; const b = box.getBoundingClientRect(); if (b.width > dlg.clientWidth + 1) bad.push(sel + ' wider than the window: ' + Math.round(b.width) + '>' + dlg.clientWidth); for (const n of box.querySelectorAll('*')) { const r = n.getBoundingClientRect(); if (r.width > 0 && (r.right > b.right + 1 || r.left < b.left - 1)) bad.push((n.className || n.tagName).toString().slice(0, 30) + ':' + Math.round(r.left - b.left) + '..' + Math.round(r.right - b.left) + ' of ' + Math.round(b.width)); } } return bad.slice(0, 6); });
      note(s.scrollX <= 1 && pcOut.length === 0, 'the order window at 400 px: nothing of the pieces list or the Engraving cards runs past its own box or the window, and the page does not scroll sideways', JSON.stringify({ pageX: s.scrollX, out: pcOut }));
      note(true, 'the window\'s own sideways scroll at 400 px (informational, the title bar\'s, not the pieces\'): ' + s.dlgScrollX);
      note(s.cards.length === 3 && s.entries === 3 && (s.headerN === null || s.headerN === 3), 'three pieces listed and three cards at 400 px', s.cards.length + ' cards / ' + s.entries + ' entries / header ' + s.headerN);
      // the section keeps its height while a piece is picked and switched back (no layout jump)
      const heights = [s.secH];
      await page.evaluate(() => { document.getElementById('owEngSec').scrollIntoView({ block: 'center' }); }); await settle(300);
      for (let i = 0; i < 2; i++) { await page.evaluate(() => { const e = document.querySelector('#owPcSum .owPcSub[data-sub] button, #owPcSum .owPcSub[data-sub], #owPcSum .owPcRow'); if (e) e.click(); }); await settle(500); heights.push((await win()).secH); }
      note(true, 'section heights while picking (informational): ' + heights.join(' / '));
      let d = await openDetail((await jobsOf('cg3'))[0].key);
      note(!!d && d.scrollX <= 1 && /piece 1 of 3/i.test(d.of), 'the Engrave detail at 400 px: no sideways scroll, "Piece 1 of 3"', d && (d.scrollX + ' ' + d.of));
      const fit = await page.evaluate(() => { const c = document.querySelector('#egQueue .rvItem'), st = c.querySelector('.egPcStrip, .egEarSwitch'); if (!st) return null; const cb = c.getBoundingClientRect(), sb = st.getBoundingClientRect(), cs = getComputedStyle(st), tabs = [...st.querySelectorAll('.egPcTab, .egEarTab')], out = tabs.filter(t => { const x = t.getBoundingClientRect(); return x.right > cb.right + 1 || x.left < cb.left - 1; }).length; return { inside: sb.left >= cb.left - 1 && sb.right <= cb.right + 1, tabsOutside: out, scrolls: /auto|scroll/.test(cs.overflowX), tabs: tabs.length, w: Math.round(sb.width), card: Math.round(cb.width) }; });
      if (process.env.PF_DEBUG) console.log('   DEBUG F10 detail:', JSON.stringify(await page.evaluate(() => [...document.querySelectorAll('#egQueue .rvItem, #egQueue > *')].slice(0, 6).map(n => (n.className || n.tagName) + ' ' + Math.round(n.getBoundingClientRect().width) + 'x' + Math.round(n.getBoundingClientRect().height) + ' kind=' + (n.dataset.kind || '') + ' ' + (n.innerText || '').replace(/\s+/g, ' ').slice(0, 60)))));
      note(true, 'the Engrave detail card width at 400 px (informational): ' + JSON.stringify(fit));
      note(!!fit && fit.inside && (fit.tabsOutside === 0 || fit.scrolls), 'the piece switch stays inside the card at 400 px (its buttons fit, or the switch scrolls by itself)', JSON.stringify(fit));
      if (SHOTS) await R.shot(w, 'pieces-engrave-400');
      await page.setViewportSize({ width: 1440, height: 900 }); await settle(500);
    }


    // ═══ F13 · earrings x2 (never engraved): four pieces listed, two pairs, no card ═══
    cur = 'F13';
    if (want('F13')) {
      await openWin(K.ears2[0]);
      const s = await win(), js = await jobsOf('ears2');
      note((s.headerN === null || s.headerN === 4) && s.entries === 4, 'earrings x2: the header says "4 pieces" and the list has FOUR entries (Left, Right, Left, Right)', s.header + ' / ' + s.entries + ': ' + s.entryTexts.join(' | '));
      note(s.entries === 4 && s.entryTexts.filter(t => /left/i.test(t)).length === 2 && s.entryTexts.filter(t => /right/i.test(t)).length === 2 && s.entryTexts.filter(t => /pair\s*2|2nd|second/i.test(t)).length >= 2, 'two Left and two Right ears; the second pair says so ("pair 2")', s.entryTexts.join(' | '));
      note(s.cards.length === 0 && js.length === 0, 'an earring is never engraved: no card, no job');
    }

    // ═══ F14 · a 3-disc necklace x2: six pieces, six cards, the discs' words repeated per set; approve one by one ═══
    cur = 'F14';
    if (want('F14')) {
      await openWin(K.discs2[0]);
      let s = await win(); const js = await jobsOf('discs2');
      note((s.headerN === null || s.headerN === 6) && s.entries === 6, 'discs x2: the header says "6 pieces" and the list has SIX entries', s.header + ' / ' + s.entries + ': ' + s.entryTexts.join(' | '));
      note(js.length === 6 && new Set(js.map(j => j.copies.join())).size === 6 && js.every(j => j.copies.length === 1), 'six jobs, one pool id each (' + js.length + ')', js.map(j => tailOf(j.key) + '=' + j.copies.length).join(' '));
      note(js.length === 6 && js.map(j => j.text).join() === 'A,B,C,A,B,C', 'the discs\' words A, B, C are repeated for the second set (A, B, C, A, B, C), each on its own piece', js.map(j => j.text).join());
      note(s.cards.length === 6 && new Set(s.cards.map(c => c.title)).size === 6 && s.cards.slice(3).every(c => /\b(set|pair)\s*2\b/i.test(c.title + ' ' + c.text)), 'six cards with six different titles; the second set\'s say "set 2"', s.cards.map(c => c.title).join(' | '));
      if (s.cards.length === 6) {
        await page.evaluate(() => { const bs = [...document.querySelectorAll('#owEng .engCard [data-e=approve]')]; if (bs[3]) bs[3].click(); });
        await until(() => [...Engrave.items().values()].some(j => String((j.row.order || {}).receiptId) === '4295059408' && j.state === 'approved'), 'the fourth disc\'s approval', 40000); await settle(900);
        const after = await jobsOf('discs2');
        note(after.filter(j => j.state === 'approved').length === 1 && after.findIndex(j => j.state === 'approved') === 3, 'Approve on card 4 approved piece 4 ONLY', JSON.stringify(after.map(j => [tailOf(j.key), j.state])));
      }
    }

    // ═══ F12 · no Decided tab, no Reopen; an approved piece of a x2 line is edited IN PLACE and approved again with a second seal, per piece ═══
    cur = 'F12';
    if (want('F12')) {
      const js0 = await jobsOf('cg2'), k1 = (js0[0] || {}).key, k2 = (js0[1] || {}).key;
      let r = k1 ? await openDetail(k1) : null;
      const tabs = await page.evaluate(() => ({ tabs: [...document.querySelectorAll('.egTab')].map(b => b.textContent.replace(/\s+/g, ' ').trim()), reopen: [...document.querySelectorAll('[data-a=reopen], .reopen, button')].filter(b => /reopen/i.test(b.textContent)).length, decidedRows: document.querySelectorAll('[data-tab=done]').length }));
      note(!tabs.tabs.some(x => /^decided\b/i.test(x)) && tabs.decidedRows === 0, 'the Engrave tab has no Decided tab', JSON.stringify(tabs.tabs));
      note(tabs.reopen === 0, 'no Reopen button anywhere in the Engrave tab', String(tabs.reopen));
      const before = await jobsOf('cg2'), b1 = before.find(j => j.key === k1), b2 = before.find(j => j.key === k2);
      note(!!b1 && !!b2 && b1.state === 'approved' && b2.state === 'approved' && b1.seals >= 1, 'both pieces of the x2 line are approved (from F4) with a seal each', JSON.stringify(before.map(j => [tailOf(j.key), j.state, j.seals])));
      if (r && r.key === k1 && b1 && b2) {
        const ts = await typeAndSave('Name Seven'); const edited = ts.typed && ts.save === 'pressed';
        note(edited, 'an approved piece\'s words can be changed in place (the words box and its Save are there)', JSON.stringify(ts));
        await until(k => { const j = Engrave.items().get(k); return !!j && /Name Seven/.test((j.lines || []).join(' ') + ' ' + (j.text || '')) && !['classify', 'ready', 'fitting'].includes(j.state); }, 'the edited piece refitted', 40000, k1);
        await settle(600);
        let mid = await jobsOf('cg2'), m1 = mid.find(j => j.key === k1), m2 = mid.find(j => j.key === k2);
        note(!!m1 && m1.state !== 'approved' && /Name Seven/.test(m1.text), 'the edited piece is back to approve with its new words (needs a new approval)', JSON.stringify([m1 && m1.state, m1 && m1.text]));
        note(!!m2 && m2.state === 'approved' && m2.text === (b2 || {}).text && m2.seals === (b2 || {}).seals, 'the OTHER piece stays approved with its words and its seals', JSON.stringify([m2 && m2.state, m2 && m2.seals]));
        r = await rd();
        if (r && r.approve) {
          await page.evaluate(() => { const b = document.querySelector('#egQueue [data-a=approve]'); if (b) b.click(); });
          await until(k => { const j = Engrave.items().get(k); return !!j && j.state === 'approved'; }, 'the second approval', 40000, k1); await settle(900);
          mid = await jobsOf('cg2'); m1 = mid.find(j => j.key === k1); m2 = mid.find(j => j.key === k2);
          note(!!m1 && m1.state === 'approved' && m1.seals >= (b1.seals + 1), 'approved again: the piece carries a SECOND seal (' + b1.seals + ' -> ' + (m1 && m1.seals) + '), the first is kept', JSON.stringify(mid.map(j => [tailOf(j.key), j.state, j.seals])));
          note(!!m2 && m2.seals === b2.seals, 'the other piece has exactly the seals it had', JSON.stringify([m2 && m2.seals, b2.seals]));
        } else note(false, 'the edited piece offers its own Approve', JSON.stringify(r && { approve: r.approve, text: r.text && r.text.slice(0, 120) }));
      }
    }

    cur = 'F11';
    note(w.errors.length === 0, 'no page errors in the whole run', w.errors.join(' | ').slice(0, 300));
    note(consoleBad().length === 0, 'no console errors of the TypeError / ReferenceError kind', consoleBad().join(' | ').slice(0, 300));
    note(w.warns.length === 0, 'no "order view:" warnings (the window swallowed no error)', w.warns.join(' | ').slice(0, 300));
  } finally { await w.close(); }
  console.log(`\npieces-flow: ${tally.pass} passed, ${tally.fail} FAILED  (${Object.entries(perId).map(([k, v]) => k + ' ' + v.pass + '/' + (v.pass + v.fail)).join(', ')})`);
  if (fails.length) console.log(' - ' + fails.join('\n - '));
  process.exitCode = fails.length ? 1 : 0;
})().catch(e => { console.error(e); process.exit(1); });
