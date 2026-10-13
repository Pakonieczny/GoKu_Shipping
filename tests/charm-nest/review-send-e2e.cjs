// REVSAND (review-send-1012): ONE real-world offline test of Send to Review, for BOTH workspaces (sandbox and production).
//
// Paul, 12 Oct 2026: a button on every order row marks the order for review; every order in Review runs the similar-charm suggestions; the vector file a person picks for a listing is
// recorded FOREVER and the next order of that listing is matched by it, in the Sandbox and in live mode alike; an order that is already on a sheet stays there and the sheet is HELD until the
// order is resolved in Review; the Library says exactly which order holds it.
//
// The REAL sorter page in headless Chromium over the repo's fake site (bridge-server.cjs: the real charmNest* handlers over an in-memory Firestore; tests/charm-nest/_reviewSendWorld.cjs),
// the master library holding the real drawings of tests/charm-nest/fixtures. Each workspace runs in its own world, in this order:
//    1  the Orders tab has one "Send to Review" button on every row, right-hand side, under Qty / Ship by
//    2  a press puts the order into Review (the Review tab's Open list), once; a second press never adds it again
//    3  the order's card offers the library's similar charms, the charm it is matched to now marked, nothing applied by itself
//    4  an order with nothing similar has no suggestion area at all
//    5  the pick is saved (one write), read back from the cloud, shared by both workspaces, and the review is closed by it
//    6  the NEXT order of the same listing is matched by the saved link at once, with nothing in Review; and after a page reload
//    7  the saved link survives the sandbox Reset and the history Purge (a new order after each is still matched by it)
//    8  an order on a saved sheet keeps its place (no record of the sheet, its pieces or its set changes) and the sheet is held
//    9  the Library names the order holding the sheet (card, '!' list, Approve line) and one tap opens it in Review; nothing else is named
//   10  the server refuses to complete the held sheet too
//   11  resolving the review releases the sheet at once (no extra press), the notes clear, and the server accepts it
// Every step is reported with the owner it belongs to (REVBTN, REVMARK, REVSUGGEST, REVLINK, REVHOLD, REVLIB); a later step that needs an earlier one says so when it is skipped.
// Offline only: no Etsy, no paid AI, no real buyer (every name is "Name One"...), nothing written anywhere outside the in-memory fake; the purge passcode is a throwaway of this process.
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/review-send-e2e.cjs [sandbox|production]   (CHROMIUM=<chrome>, SHOTS=<dir> saves pictures)
'use strict';
const assert = require('assert');
const W = require('./_reviewSendWorld.cjs');
const { ORDERS, NEAR } = W;

const only = process.argv[2] && /^(sandbox|production)$/.test(process.argv[2]) ? [process.argv[2]] : ['sandbox', 'production'];
const shots = process.env.SHOTS || '';
const results = [];
const ok = (cond, msg) => assert(cond, msg);
const J = x => JSON.stringify(x);
const sleep = ms => new Promise(r => setTimeout(r, ms));
const until = async (fn, ms = 8000, every = 120) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) return v; await sleep(every); } };

// the order's row/card in the Review tab: the list row (data-mkey "ord:rvm:RID") or the card (.rvItem[data-kind=reviewMark][data-rid])
const CARD = rid => `:is(#reviewView [data-mkey="ord:rvm:${rid}"], #reviewView .rvItem[data-kind="reviewMark"][data-rid="${rid}"])`;
// ── what the page shows, in one place (the DOM each owner published in INTERFACES.md) ──
const PAGE = {
  buttons: w => w.page.evaluate(() => [...document.querySelectorAll('#ordItems .revSendBtn')].map(b => { const r = b.getBoundingClientRect(), row = b.closest('.orderListRow') || b.closest('.hoverItem') || b.closest('[data-rid]'), rr = row && row.getBoundingClientRect(), facts = row && row.querySelector('.rowFacts'), f = facts && facts.getBoundingClientRect(); return { rid: b.dataset.rid, text: b.textContent.trim(), state: b.dataset.revState, disabled: b.disabled, cx: r.left + r.width / 2, cy: r.top + r.height / 2, rowL: rr && rr.left, rowR: rr && rr.right, rowT: rr && rr.top, rowB: rr && rr.bottom, factsBottom: f ? f.bottom : null, factsLeft: f ? f.left : null, btnLeft: r.left, btnRight: r.right, btnTop: r.top, visible: b.getClientRects().length > 0 }; })),
  rows: w => w.page.evaluate(() => Orders.rows().filter(r => r.state !== 'gone').map(r => String(r.order.receiptId))),
  msg: (w, rid) => w.page.evaluate(rid => { const n = document.querySelector(`#ordItems [data-rid="${rid}"] .revSendMsg`); return n ? n.textContent.trim() : ''; }, rid),
  reviewCard: (w, rid) => w.page.evaluate(({ rid, sel }) => { const c = document.querySelector(sel); return c ? { text: c.innerText.replace(/\s+/g, ' ').trim(), slots: [...c.querySelectorAll('.rvMarkSlot')].map(s => ({ row: s.dataset.row, html: s.innerHTML.length, text: s.innerText.replace(/\s+/g, ' ').trim(), picks: [...s.querySelectorAll('[data-dsku]')].map(b => b.dataset.dsku), current: [...s.querySelectorAll('[data-cur]')].map(b => b.dataset.cur), kids: s.children.length })), visible: c.getClientRects().length > 0, buttons: [...c.querySelectorAll('button')].map(b => b.textContent.trim()) } : null; }, { rid, sel: CARD(rid) }),
  openReview: async (w, seg) => { await w.page.evaluate(seg => { CN.setMode('review'); const v = Review.view(); v.cseg = seg || 'open'; v.filter = null; v.q = ''; Review.render(); }, seg); await W.settle(w.page, 500); },
  reviewItems: w => w.page.evaluate(() => Review.items().map(it => ({ kind: it.kind, key: it.key, rid: it.rid || (it.row && String(it.row.order.receiptId)) || null, done: !!it.done }))),
  mark: (w, rid) => w.page.evaluate(rid => { const M = window.CharmNestReviewMark; return M ? { is: M.isMarked(rid), info: M.info(rid), open: M.openRids() } : null; }, rid),
  hold: (w, sid) => w.page.evaluate(sid => { const H = window.CharmNestReviewHold; return H ? { held: H.isHeld(sid), by: H.heldBy(sid), why: H.reasonFor(sid) } : null; }, sid),
  sheetState: (w, sid) => w.page.evaluate(sid => { const x = LaserReview.explain('sheet', sid); return x ? { ready: x.ready, step: x.step, steps: Object.fromEntries(x.steps.map(s => [s.key, { state: s.state, detail: s.detail }])), next: x.nextText } : null; }, sid),
  libCard: (w, sid) => w.page.evaluate(sid => { const f = document.querySelector(`[data-flow-for="sheet:${sid}"]`), c = f && (f.closest('article') || f.closest('.libCard') || f.parentElement); const box = document.querySelector(`.approveBox[data-approve-for="sheet:${sid}"]`), btn = box && box.querySelector('[data-approve-btn]'); return c ? { text: c.innerText.replace(/\s+/g, ' ').trim(), approve: btn ? { disabled: btn.getAttribute('aria-disabled') === 'true', why: (box.querySelector('[data-approve-why]') || {}).textContent || '', label: btn.textContent.trim() } : null, bang: !!c.querySelector('[data-issues-open]') } : null; }, sid),
  libOpen: async (w, sid) => {
    const mark = w.srv.st.calls.length; await w.page.evaluate(() => CN.setMode('library'));
    try { await w.page.waitForSelector(`[data-flow-for="sheet:${sid}"]`, { timeout: 30000 }); }
    catch (e) { const d = await w.page.evaluate(() => ({ mode: CN.S.mode, lib: CN.S.library && { kind: CN.S.library.kind, rows: (CN.S.library.rows || []).length }, text: ((document.getElementById('libBody') || {}).innerText || '').slice(0, 160) })).catch(() => null); throw new Error(`the Library never showed ${sid}: ${J(d)} calls ${J(w.srv.st.calls.slice(mark).map(c => c.op))} sheet in cloud ${!!w.srv.st.doc(w.prefix + 'Charm_Nest_Sheets', sid)}`); }
    await W.settle(w.page, 900);
  },
  panel: w => w.page.evaluate(() => { const p = document.querySelector('.lisPanel'); return p ? { text: p.innerText.replace(/\s+/g, ' ').trim(), rows: [...p.querySelectorAll('.lisRow')].map(r => ({ id: r.dataset.id || '', text: r.innerText.replace(/\s+/g, ' ').trim() })) } : null; })
};

async function scenario(mode) {
  const M = x => `[${mode}] ${x}`;
  const done = new Set(), notes = {};
  const step = async (id, owner, title, needs, fn) => {
    const miss = needs.filter(n => !done.has(n));
    if (miss.length) { results.push({ mode, id, owner, title, status: 'SKIP', why: 'needs ' + miss.join(', ') }); return; }
    const t0 = Date.now();
    try { await fn(); done.add(id); results.push({ mode, id, owner, title, status: 'ok', ms: Date.now() - t0, note: notes[id] || '' }); }
    catch (e) { results.push({ mode, id, owner, title, status: 'FAIL', why: String(e && e.message || e).split('\n').slice(0, 3).join(' | ') }); }
  };
  const w = await W.open(mode);
  if (!w) { console.log('  - no playwright-core / chrome: not run'); return null; }
  const st = w.srv.st, P = w.prefix, other = mode === 'sandbox' ? '' : 'Sandbox_';
  const snap = async n => { if (shots) await W.shot(w, `rs-${mode}-${n}`); };
  const cR = rid => ORDERS[rid].rid;
  try {
    /* the day's orders: the listing to be linked (matched, with a near charm in the library), a listing with nothing like it, and (below) the sheet's */
    await W.addOrders(w, ['matched', 'alone']);
    const calls0 = st.calls.length;

    await step('1', 'REVBTN', 'every order row has ONE "Send to Review" button, right-hand side, under Qty / Ship by', [], async () => {
      await w.page.evaluate(() => { CN.setMode('orders'); Orders.render(); }); await W.settle(w.page, 500);
      const rows = await PAGE.rows(w), btns = await PAGE.buttons(w);
      ok(rows.length === 2 && btns.length === rows.length, `one button per row: ${btns.length} buttons for ${rows.length} rows ${J(btns.map(b => b.rid))}`);
      for (const b of btns) {
        ok(b.text === 'Send to Review' && b.state === 'idle' && b.visible && !b.disabled, `the button says "Send to Review", idle and enabled: ${J(b)}`);
        ok(b.cx > b.rowL + (b.rowR - b.rowL) * 0.8, `the button is on the right-hand side of its row (at Paul's window width): x ${Math.round(b.cx)} of ${Math.round(b.rowL)}..${Math.round(b.rowR)}`);
        ok(b.factsBottom != null && b.btnTop >= b.factsBottom - 1 && Math.abs(b.btnLeft - b.factsLeft) <= 3, `the button sits right under "Qty . Ship by", same left edge (facts ${Math.round(b.factsLeft)}..${Math.round(b.factsBottom)}, button left ${Math.round(b.btnLeft)} top ${Math.round(b.btnTop)})`);
      }
      ok(new Set(btns.map(b => b.rid)).size === btns.length, 'never two buttons for one order');
      // a narrower window (1440): the row folds its columns; the button follows "Qty . Ship by" and is never clipped or pushed off the screen
      await w.page.setViewportSize({ width: 1440, height: 900 }); await W.settle(w.page, 500);
      const narrow = await PAGE.buttons(w);
      ok(narrow.length === rows.length && narrow.every(b => b.visible && b.btnRight <= 1440 && b.btnLeft >= 0), 'at 1440 px every button is still on the screen: ' + J(narrow.map(b => [Math.round(b.btnLeft), Math.round(b.btnRight)])));
      ok(narrow.every(b => b.factsBottom != null && Math.abs(b.btnLeft - b.factsLeft) <= 3 ? b.btnTop >= b.factsBottom - 1 : true), 'and it does not overlap "Qty . Ship by"');
      ok(await w.page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), 'no sideways scroll');
      await w.page.setViewportSize({ width: 1700, height: 900 }); await W.settle(w.page, 400);
      await snap('1-orders');
    });

    await step('2', 'REVBTN+REVMARK', 'pressing it puts the order into Review (the Open list), once; pressing again never adds it twice', ['1'], async () => {
      const rid = cR('matched'), before = await PAGE.reviewItems(w);
      ok(!before.some(i => i.rid === rid), 'the order is not in Review before the press: ' + J(before));
      ok(!(await PAGE.mark(w, rid)).is, 'not marked before');
      const m0 = st.calls.length;
      await w.page.click(`#ordItems [data-rid="${rid}"] .revSendBtn`);
      ok(await until(async () => (await PAGE.mark(w, rid)).is), 'CharmNestReviewMark.isMarked(rid) is true after the press (REVMARK)');
      ok(await until(async () => (await PAGE.buttons(w)).find(b => b.rid === rid).state === 'in', 4000), 'the button now says it is in Review (REVBTN): ' + J((await PAGE.buttons(w)).find(b => b.rid === rid)));
      ok((await PAGE.buttons(w)).find(b => b.rid === rid).text === 'In Review', 'the label is "In Review"');
      const items = (await PAGE.reviewItems(w)).filter(i => i.rid === rid);
      ok(items.length === 1 && items[0].kind === 'reviewMark', 'ONE Review item of kind reviewMark for the order: ' + J(items));
      await PAGE.openReview(w, 'open');
      const card = await until(() => PAGE.reviewCard(w, rid), 6000);
      ok(card && card.visible, 'the order has a row in the Review tab\'s Open list (REVMARK)');
      ok(card.text.includes(rid), 'the card names the order: ' + card.text.slice(0, 160));
      // the record: one write at the press, in the workspace's own Charm_Custom_Orders, never in the other workspace's
      const doc = st.doc(P + 'Charm_Custom_Orders', rid + '_review');
      ok(doc && doc.review && doc.review.state === 'open', `the mark is stored at ${P}Charm_Custom_Orders/${rid}_review as open: ${J(doc)}`);
      ok(!st.doc(other + 'Charm_Custom_Orders', rid + '_review'), `nothing of it in ${other || 'production\'s'}Charm_Custom_Orders`);
      const puts = st.calls.slice(m0).filter(c => c.op === 'reviewMarkPut' && c.body.action === 'mark');
      ok(puts.length === 1, 'exactly one reviewMarkPut write for the press: ' + puts.length);
      // pressing again (the button opens the card, it never adds a second one)
      await w.page.evaluate(() => { CN.setMode('orders'); Orders.render(); }); await W.settle(w.page, 300);
      await w.page.click(`#ordItems [data-rid="${rid}"] .revSendBtn`); await W.settle(w.page, 500);
      ok(st.calls.slice(m0).filter(c => c.op === 'reviewMarkPut' && c.body.action === 'mark').length === 1, 'a second press writes nothing more');
      ok((await PAGE.reviewItems(w)).filter(i => i.rid === rid).length === 1, 'and the order is still ONE Review item');
      // the mark is no Timeline seal (seals are only for real milestones)
      const tl = st.calls.slice(m0).filter(c => c.op === 'timelineAdd').flatMap(c => c.body.events || []).filter(e => String(e.orderId || '') === rid && /seal/i.test(String(e.type || '')));
      ok(!tl.length, 'no seal on the order\'s Timeline: ' + J(tl.map(e => e.type)));
      // zero Etsy calls and no paid reading, for all of it
      const bad = st.calls.slice(m0).filter(c => /^(listOpenOrders|etsyOrderProxy|refreshEtsyToken|etsySandbox|etsyImages)$/.test(String(c.name)) || /^(startAgent|sandboxPullOrders)$/.test(String(c.op)) || /Agent-background|charmEngrave/.test(String(c.name)));
      ok(!bad.length, 'no Etsy call and no paid reading: ' + J(bad.map(c => c.name + ':' + c.op)));
    });

    await step('3', 'REVSUGGEST', 'its card offers the library\'s similar charms, the matched one marked, and nothing is applied by itself', ['2'], async () => {
      const rid = cR('matched'), key = W.keyOf('matched');
      const card = await until(async () => { const c = await PAGE.reviewCard(w, rid); return c && c.slots.some(s => s.picks.length || s.current.length) ? c : null; }, 6000);
      ok(card, 'the card has a suggestion area (.rvMarkSlot with charms): ' + J(await PAGE.reviewCard(w, rid)));
      const slot = card.slots.find(s => s.row === key) || card.slots[0];
      ok(slot.picks.includes(NEAR), `${NEAR} is offered as a similar charm: ${J(slot)}`);
      ok(slot.picks.length >= 1 && slot.picks.every(s => s !== 'SCHRODINGER CAT 1'), 'the charm it is matched to now is not offered as a tap: ' + J(slot.picks));
      ok(slot.current.includes('SCHRODINGER CAT 1'), 'the charm it is matched to now is marked (data-cur): ' + J(slot.current));
      const r = await W.readOf(w, 'matched');
      ok(r.designSku === 'SCHRODINGER CAT 1' && r.source !== 'alias', 'nothing was applied by itself: ' + J(r));
      ok(!st.calls.slice(calls0).some(c => c.op === 'aliasPut'), 'no link was written by the suggestions');
      const paid = st.calls.slice(calls0).filter(c => /^(listOpenOrders|etsyOrderProxy|refreshEtsyToken|etsySandbox|etsyImages)$/.test(String(c.name)) || /^(startAgent|sandboxPullOrders)$/.test(String(c.op)) || /Agent-background|charmEngrave/.test(String(c.name)));
      ok(!paid.length, 'the suggestions made no Etsy call and no paid reading: ' + J(paid.map(c => c.name + ':' + c.op)));
      await snap('3-suggestions');
    });

    await step('4', 'REVSUGGEST', 'an order with nothing similar in the library has NO suggestion area (the control: step 3 shows the area when something is similar)', ['3'], async () => {
      const rid = cR('alone');
      await w.page.evaluate(() => CN.setMode('orders')); await W.settle(w.page, 300);
      await w.page.click(`#ordItems [data-rid="${rid}"] .revSendBtn`);
      ok(await until(async () => (await PAGE.mark(w, rid)).is), 'the order is marked (REVMARK)');
      await PAGE.openReview(w, 'open');
      const card = await until(() => PAGE.reviewCard(w, rid), 6000);
      ok(card && card.visible, 'it has its card in Review');
      await sleep(600);
      const c2 = await PAGE.reviewCard(w, rid);
      ok(c2.slots.every(s => s.kids === 0 && s.picks.length === 0 && s.current.length === 0 && !s.text), 'its slot stays empty, no similar charm, not even a heading: ' + J(c2.slots));
      const strays = await w.page.evaluate(sel => { const c = document.querySelector(sel); return c ? [...c.querySelectorAll('.sibOffer,.sibAsk,.sibPicks,.dsPick,[data-dsku],[data-cur]')].length : -1; }, CARD(rid));
      ok(strays === 0, 'no suggestion element anywhere on the card: ' + strays);
      await snap('4-none');
    });

    let pickedAt = null;
    await step('5', 'REVSUGGEST+REVLINK+REVMARK', 'the pick is saved (one write), read back, the same for both workspaces, and it closes the review', ['2'], async () => {
      const rid = cR('matched'), lid = ORDERS.matched.listing, mark = st.calls.length;
      await PAGE.openReview(w, 'open');
      const btn = `${CARD(rid)} .rvMarkSlot [data-dsku="${NEAR}"]`;
      if (await w.page.$(btn)) { await w.page.click(btn); notes['5'] = 'tapped the suggested charm in the order\'s Review row'; }
      else {
        // REVSUGGEST's buttons are not on this page (step 3 says so): the pick goes through REVLINK's own entry point, so the saved link is still proven
        notes['5'] = 'NO suggested-charm button on the page (REVSUGGEST): the pick went through CharmNestReviewLink.pick instead';
        const r = await w.page.evaluate(async ({ rid, sku }) => { const row = Orders.rows().find(x => String(x.order.receiptId) === rid); try { return await CharmNestReviewLink.pick(row, sku, { by: 'Test Operator' }); } catch (e) { return { ok: false, why: String(e && e.message || e) }; } }, { rid, sku: NEAR });
        ok(r && r.ok, 'CharmNestReviewLink.pick saved the link (REVLINK): ' + J(r));
      }
      ok(await until(async () => !(await PAGE.mark(w, rid)).is, 10000), 'the review closed after the pick (REVMARK resolve, how "pick")');
      const info = (await PAGE.mark(w, rid)).info;
      ok(info && info.state === 'done' && info.how === 'pick' && info.sku === NEAR, 'the mark says who picked what: ' + J(info));
      const since = st.calls.slice(mark);
      const writes = since.filter(c => c.op === 'aliasPut');
      ok(writes.length === 1 && String(writes[0].body.listingId) === lid && String(writes[0].body.sku || (writes[0].body.pair && writes[0].body.pair.L)) === NEAR, 'ONE aliasPut for this listing and the picked charm (REVLINK): ' + J(writes.map(c => c.body)));
      ok(String(writes[0].body.fromSku || '').toUpperCase() === ORDERS.matched.sku, 'kept under the SKU of the line (one link per variation): ' + J(writes[0].body));
      ok(since.filter(c => c.op === 'reviewMarkPut' && c.body.action === 'resolve').length === 1, 'ONE resolve write for the mark');
      // read back from the cloud, as the page of ANY computer reads it, with and without the sandbox flag: the link is shared by both workspaces
      const read = flag => w.page.evaluate(async ({ lid, flag }) => { const r = await CN.api('charmNestLibrary', Object.assign({ op: 'aliasGet', listingId: lid }, flag ? { sandbox: true } : {})); return r; }, { lid, flag });
      for (const flag of [false, true]) { const r = await read(flag), txt = J(r); ok(txt.includes(NEAR), `aliasGet (${flag ? 'sandbox' : 'production'} read) returns the saved link ${NEAR}: ${txt.slice(0, 300)}`); }
      const found = W.linksFor(w, lid, NEAR);
      ok(found.length >= 1, 'the link is in a saved-link collection of the cloud: ' + J(found.map(f => f.path)));
      ok(found.some(f => !/^Sandbox_/.test(f.path)), `the link lives where a sandbox clean-up cannot reach it (found at ${found.map(f => f.path).join(', ')})`);
      pickedAt = found.map(f => f.path);
      ok(!(await PAGE.reviewItems(w)).some(i => i.rid === rid && i.kind === 'reviewMark' && !i.done), 'no open Review item is left for the order');
      await PAGE.openReview(w, 'done'); const c = await PAGE.reviewCard(w, rid);
      ok(c === null || /pick|picked|chose|SCHRODINGER CAT 2/i.test(c.text), 'the answer is kept under Completed: ' + J(c && c.text));
      await snap('5-picked');
    });

    await step('5b', 'REVSUGGEST+REVLINK', 'the order itself (not built yet) now reads as the picked charm, and the Orders tab says so', ['5'], async () => {
      const r = await until(async () => { const x = await W.readOf(w, 'matched'); return x && x.designSku === NEAR ? x : null; }, 4000);
      ok(r, 'after the pick the page reads the not-yet-built order as the picked charm: ' + J(await W.readOf(w, 'matched')));
      ok(!r.problems.length, 'with no question: ' + J(r));
    });

    await step('6', 'REVLINK', 'a NEW order of the same listing is matched by the saved link at once, nothing in Review; and after a page reload', ['5'], async () => {
      const mark = st.calls.length;
      await W.addOrders(w, ['repeat']);                                   // (the page's own pull step: the learned maps are read again, then the line is read)
      let r = await W.readOf(w, 'repeat');
      ok(r.designSku === NEAR && !r.problems.length, `the next order of the listing is read as ${NEAR} with no question: ${J(r)}`);
      ok(/alias|saved|link|person/i.test(String(r.source)) , 'it says the saved link matched it (not the exact name): ' + J(r));
      const items = await PAGE.reviewItems(w);
      ok(!items.some(i => i.rid === ORDERS.repeat.rid), 'nothing for it in Review: ' + J(items.filter(i => i.rid === ORDERS.repeat.rid)));
      ok(!(await PAGE.mark(w, ORDERS.repeat.rid)).is, 'it is not marked');
      ok(!st.calls.slice(mark).some(c => c.op === 'aliasPut' || c.op === 'reviewMarkPut'), 'matching it wrote nothing');
      // one link per variation: another SKU of the same listing is NOT pointed at the picked charm
      await W.addOrders(w, ['variant']);
      const v = await W.readOf(w, 'variant');
      ok(v.designSku === ORDERS.variant.sku && v.designSku !== NEAR, `a variation of the listing (another SKU) keeps its own charm, the link is for the SKU that was picked: ${J(v)}`);
      // a page that has forgotten everything (a second computer, the next morning) reads the same
      await W.reboot(w);
      await W.addOrders(w, ['again'], { loadMaps: false });               // (maps as the page loads them at the start of a shift)
      r = await W.readOf(w, 'again');
      ok(r.designSku === NEAR && !r.problems.length, `after a reload the next order is still read as ${NEAR}: ${J(r)}`);
      ok(!(await PAGE.reviewItems(w)).some(i => i.rid === ORDERS.again.rid), 'and still nothing in Review');
    });

    const rid5 = ORDERS.matched.rid;
    await step('7', 'REVLINK', 'the saved link survives the sandbox Reset and the history Purge (the next order is still matched)', ['6'], async () => {
      // the page's own ops on the fake site (Settings: "Reset the sandbox" / "Purge all run history"): the link must not be among what they delete
      // (controls first: the Reset and the Purge really delete something here, or "the link survived" would prove nothing)
      st.put(P + 'Charm_Custom_Orders', 'ctl_old', { key: 'ctl_old', receiptId: '4291999991', state: 'open' });
      st.put('Sandbox_Charm_Pool', 'ctl-pool', { poolId: 'ctl-pool', state: 'ready' });
      st.put('Charm_Nest_Runs', 'ctl-run', { runId: 'ctl-run', lines: {}, status: 'archived' }); st.put('Charm_Pool', 'ctl-pool', { poolId: 'ctl-pool', state: 'ready' });
      const rs = await W.resetSandbox(w); ok(rs && !rs.error, 'the sandbox reset ran on the fake site: ' + J(rs));
      ok(!st.doc('Sandbox_Charm_Pool', 'ctl-pool'), 'control: the Reset really deleted the sandbox\'s own records');
      ok(mode === 'sandbox' ? !st.doc('Sandbox_Charm_Custom_Orders', 'ctl_old') && !st.doc('Sandbox_Charm_Custom_Orders', rid5 + '_review') : !!st.doc('Charm_Custom_Orders', rid5 + '_review'), mode === 'sandbox' ? 'control: and the sandbox\'s review marks went with it' : 'control: the Reset left production\'s records alone');
      let found = W.linksFor(w, ORDERS.matched.listing, NEAR);
      ok(found.some(f => !/^Sandbox_/.test(f.path)), 'the link is still in the cloud after the Reset: ' + J(found.map(f => f.path)));
      await W.reboot(w); await W.addOrders(w, ['repeat'], { loadMaps: false });
      let r = await W.readOf(w, 'repeat');
      ok(r.designSku === NEAR && !r.problems.length, `after the Reset a new order is still read as ${NEAR}: ${J(r)}`);
      const pg = await W.purgeHistory(w); ok(pg && !pg.error, 'the history purge ran on the fake site: ' + J(pg));
      ok(!st.doc('Charm_Pool', 'ctl-pool') && !st.doc('Charm_Nest_Runs', 'ctl-run'), 'control: the Purge really deleted the run history');
      found = W.linksFor(w, ORDERS.matched.listing, NEAR);
      ok(found.some(f => !/^Sandbox_/.test(f.path)), 'the link is still in the cloud after the Purge: ' + J(found.map(f => f.path)));
      await W.reboot(w); await W.addOrders(w, ['again'], { loadMaps: false });
      r = await W.readOf(w, 'again');
      ok(r.designSku === NEAR && !r.problems.length, `after the Purge a new order is still read as ${NEAR}: ${J(r)}`);
      // the same, the way the page asks for it (Settings > Reset the sandbox) in the sandbox
      if (mode === 'sandbox') {
        await w.page.evaluate(() => { window.confirm = () => true; window.alert = () => {}; });
        const nav = w.page.waitForNavigation({ waitUntil: 'load', timeout: 60000 }).catch(() => null);
        await w.page.evaluate(() => { window.__reset = Sandbox.reset({ button: document.getElementById('stSandboxReset'), note: document.getElementById('stSandboxResetNote') }); });
        await nav; await W.ready(w, false); await w.page.evaluate(() => { B.orders.rows = []; B.orders.byKey = new Map(); });
        await W.addOrders(w, ['repeat'], { loadMaps: false });
        r = await W.readOf(w, 'repeat');
        ok(r.designSku === NEAR && !r.problems.length, `after the Settings button's Reset (and its reload) a new order is still read as ${NEAR}: ${J(r)}`);
      }
    });

    // ── the sheet: two orders on one saved sheet; one of them is sent to Review ──
    let sh = null, before = null;
    await step('8', 'REVHOLD+REVBTN', 'an order on a sheet keeps its place, the sheet is held, the button says so in one line', [], async () => {
      await W.reboot(w);
      sh = await W.seedSheet(w, { id: 'sh-rs1', orders: ['sheeted', 'mate'] });
      await PAGE.libOpen(w, sh.id);                       // (the Library reads the sheet: its readiness is known from here)
      const base = await until(async () => { const b = await PAGE.sheetState(w, sh.id); return b && b.steps.orders.state === 'done' && b.ready ? b : null; }, 8000);
      ok(base, 'before: the sheet is ready for the laser, nothing holds it: ' + J(await PAGE.sheetState(w, sh.id)));
      const baseCard = await PAGE.libCard(w, sh.id);
      ok(baseCard && !/Held by|In Review/.test(baseCard.text), 'before: the Library says nothing of any hold: ' + J(baseCard && baseCard.text.slice(0, 200)));
      await W.settle(w.page, 1500); before = W.sheetRecords(w, sh);   // (the records as the Library left them: what the press does to them is what is measured)
      const rid = cR('sheeted'), mark = st.calls.length;
      const can = await w.page.evaluate(rid => CharmNestReviewHold.canHold(rid), rid);
      ok(can && can.ok && can.sheets.length === 1 && can.sheets[0].sheetId === sh.id, 'canHold names the sheet (REVHOLD): ' + J(can));
      await w.page.evaluate(() => { CN.setMode('orders'); Orders.render(); }); await W.settle(w.page, 400);
      await w.page.click(`#ordItems [data-rid="${rid}"] .revSendBtn`);
      ok(await until(async () => (await PAGE.mark(w, rid)).is), 'the order is marked (REVMARK)');
      const msg = await until(() => PAGE.msg(w, rid), 4000);
      ok(msg && /GF Sheet 1|Sheet 1/.test(msg) && /held/i.test(msg) && /stay/i.test(msg), 'ONE plain line names the sheet, says it stays there and that the sheet is held (REVBTN): ' + J(msg));
      ok(msg.split(/\n/).length === 1 && msg.length < 140, 'and it is one short line: ' + msg.length);
      // it stays on its sheet: every record of the sheet, its pieces and its set is as it was (after the page's own writes have had time to land)
      await W.settle(w.page, 1200);
      ok(W.sheetRecords(w, sh) === before, 'nothing was lifted off, re-nested or rewritten: the sheet, pool and set records are unchanged: ' + W.recordsDiff(before, W.sheetRecords(w, sh)));
      ok(!st.calls.slice(mark).some(c => /^(putSheet|deleteSheet|restoreSheet|poolUpdate|poolPut|setUpdate|setAllocate|runPut)$/.test(String(c.op))), 'no write of a sheet, piece or set: ' + J(st.calls.slice(mark).map(c => c.op)));
      const where = await w.page.evaluate(rid => { const r = Orders.rows().find(x => String(x.order.receiptId) === rid), p = Orders.placeOf ? Orders.placeOf(r) : null; return { state: r.state, poolIds: r.poolIds, sheet: p && p.sheet || null }; }, rid);
      ok(where.state === 'written' && where.poolIds.length === 1, 'the order is still a written line with its piece: ' + J(where));
      const hb = await PAGE.hold(w, sh.id);
      ok(hb && hb.held && hb.by.length === 1 && hb.by[0].rid === rid, 'the sheet is held by exactly this order (CharmNestReviewHold.heldBy): ' + J(hb));
      ok(!hb.by.some(x => x.rid === cR('mate')), 'the other order on the sheet is not named');
      ok(/^In Review/.test(hb.by[0].reason) || /Review/.test(hb.why), 'the reason says In Review: ' + J(hb));
      const hs = await w.page.evaluate(rid => CharmNestReviewHold.sheetsHeldBy(rid), rid);
      ok(hs.length === 1 && hs[0].sheetId === sh.id, 'sheetsHeldBy(rid) names the sheet: ' + J(hs));
      const now = await PAGE.sheetState(w, sh.id);
      ok(now && !now.ready && now.steps.orders.state !== 'done', 'the sheet is no longer ready, its Order check step is not done: ' + J(now));
      ok(String(now.steps.orders.detail + ' ' + now.next).includes(rid) || /Review/.test(now.steps.orders.detail + now.next), 'the step says it is the review: ' + J(now.steps.orders));
    });

    await step('9', 'REVLIB', 'the Library says exactly which order holds the sheet, with one tap to open it in Review', ['8'], async () => {
      const rid = cR('sheeted'), mateRid = cR('mate');
      await PAGE.libOpen(w, sh.id);
      const card = await until(async () => { const c = await PAGE.libCard(w, sh.id); return c && c.text.includes(rid) ? c : null; }, 8000);
      ok(card, 'the sheet\'s card names the order holding it: ' + J(await PAGE.libCard(w, sh.id)));
      ok(/Held by order/i.test(card.text) && /in Review/i.test(card.text), 'in plain words (Held by order ... in Review): ' + card.text.slice(0, 400));
      const heldText = (card.text.match(/Held by orders? [^.]*? in Review/i) || [''])[0];
      ok(heldText.includes(rid) && !heldText.includes(mateRid), 'it names this order and not the other one on the sheet: ' + heldText);
      ok(card.approve && card.approve.disabled, 'the Approve button is grey: ' + J(card.approve));
      ok(card.approve.why.includes(rid), 'the Approve button\'s reason line names the order: ' + J(card.approve));
      ok(card.bang, 'the \'!\' is on the rail');
      await w.page.click(`[data-issues-open][data-issues-id="${sh.id}"]`);
      const panel = await until(() => PAGE.panel(w), 4000);
      ok(panel && panel.text.includes(rid) && /In Review/.test(panel.text), 'the \'!\' list has a row for the order, marked In Review: ' + J(panel));
      ok(!panel.rows.some(x => x.text.includes(mateRid) && /In Review/.test(x.text)), 'and none for the other order');
      await snap('9-library');
      // one tap on the order: Review opens at its card
      const clicked = await w.page.evaluate(rid => { const p = document.querySelector('.lisPanel'); const hit = [...p.querySelectorAll('.lisRow')].find(r => (r.dataset.id || r.innerText).includes(rid)); if (!hit) return false; hit.click(); return true; }, rid);
      ok(clicked, 'a row of the list can be pressed');
      const in2 = await until(() => w.page.evaluate(() => CN.S.mode === 'review' || document.getElementById('reviewView') && !document.getElementById('reviewView').classList.contains('hidden')), 4000);
      ok(in2, 'one tap opens the Review tab');
      const c = await until(() => PAGE.reviewCard(w, rid), 4000);
      ok(c && c.visible, 'at the order\'s card');
      // pressing Approve on the held sheet does nothing but say why
      await PAGE.libOpen(w, sh.id);
      const mark = st.calls.length;
      await w.page.evaluate(sid => { const b = document.querySelector(`.approveBox[data-approve-for="sheet:${sid}"] [data-approve-btn]`); if (b) b.click(); }, sh.id);
      await W.settle(w.page, 900);
      ok(W.sheetRecords(w, sh) === before, 'pressing Approve on the held sheet changed no record: ' + W.recordsDiff(before, W.sheetRecords(w, sh)) + ' ; calls ' + J(st.calls.slice(mark).map(c => c.op || c.name)));
      ok(!(await PAGE.sheetState(w, sh.id)).ready, 'and the sheet is still not ready');
      const cut = W.cutOf(w, sh); ok(!cut.laserDoneAt && !cut.seals.includes('laserDone'), 'no cut mark and no laser seal were written for it: ' + J(cut));
    });

    await step('10', 'REVHOLD', 'the server refuses to complete the held sheet as well', ['8'], async () => {
      const r = await w.page.evaluate(async sid => { try { return await CN.api('charmNestLibrary', { op: 'laserDone', kind: 'sheet', id: sid, done: true, by: 'Test Operator', stage: 'laser' }, { quiet: true }); } catch (e) { return { error: String(e && e.message || e), status: e && e.status }; } }, sh.id);
      ok(r && r.error && /not ready|review/i.test(r.error), 'laserDone is refused while the order is in Review: ' + J(r));
      const cut10 = W.cutOf(w, sh); ok(!cut10.laserDoneAt && !cut10.seals.includes('laserDone'), 'and the sheet is not marked cut, no laser seal: ' + J(cut10));
    });

    await step('11', 'REVMARK+REVHOLD+REVLIB', 'resolving the review releases the sheet at once, the notes clear, and the server accepts it', ['8'], async () => {
      const rid = cR('sheeted');
      await PAGE.openReview(w, 'open');
      const okBtn = await w.page.evaluate(sel => { const c = document.querySelector(sel); if (!c) return null; return [...c.querySelectorAll('button')].map(b => b.textContent.trim()); }, CARD(rid));
      ok(okBtn, 'the order\'s card is in the Open list');
      // the person's "charm was fine": the card's own button when it has one, else the API the button calls
      const clicked = await w.page.evaluate(sel => { const c = document.querySelector(sel); const b = c && [...c.querySelectorAll('button')].find(x => /^(Mark OK|OK|Charm is fine|Looks right|Resolve|Done|Dismiss)\b/i.test(x.textContent.trim())); if (b) { b.click(); return b.textContent.trim(); } return ''; }, CARD(rid));
      if (!clicked) await w.page.evaluate(rid => CharmNestReviewMark.resolve(rid, { by: 'Test Operator', how: 'ok' }), rid);
      ok(await until(async () => !(await PAGE.mark(w, rid)).is, 8000), `the review is resolved (${clicked ? 'button "' + clicked + '"' : 'API; the card has no OK button among ' + J(okBtn)})`);
      // (the page shows it at once; the write to the cloud follows within moments: wait for the record, never for a fixed time)
      await until(() => { const d = st.doc(P + 'Charm_Custom_Orders', rid + '_review'); return d && d.review && d.review.state === 'done'; }, 8000);
      const doc = st.doc(P + 'Charm_Custom_Orders', rid + '_review');
      ok(doc && doc.review && doc.review.state === 'done', 'the record says done: ' + J(doc));
      const hb = await PAGE.hold(w, sh.id);
      ok(hb && !hb.held && !hb.by.length, 'the sheet is not held any more: ' + J(hb));
      const now = await PAGE.sheetState(w, sh.id);
      ok(now && now.ready && now.steps.orders.state === 'done', 'the sheet is ready again with no extra press: ' + J(now));
      ok(W.sheetRecords(w, sh) === before, 'its records are still exactly as they were: ' + W.recordsDiff(before, W.sheetRecords(w, sh)));
      await PAGE.libOpen(w, sh.id);
      const card = await PAGE.libCard(w, sh.id);
      ok(card && !/Held by order/i.test(card.text) && !card.text.includes('In Review'), 'the Library note is gone at once: ' + J(card && card.text.slice(0, 300)));
      await snap('11-released');
      // and the server takes it
      const r = await w.page.evaluate(async sid => { try { return await CN.api('charmNestLibrary', { op: 'laserDone', kind: 'sheet', id: sid, done: true, by: 'Test Operator', stage: 'laser' }, { quiet: true }); } catch (e) { return { error: String(e && e.message || e), status: e && e.status }; } }, sh.id);
      ok(r && !r.error, 'laserDone is accepted once the review is resolved: ' + J(r));
    });

    // ── a sheet that is already cut cannot be held: the button says so in one line, the review itself is still made ──
    await step('12', 'REVHOLD+REVBTN', 'an order whose sheet is already cut is not sent to Review: the button says the sheet cannot be held in one line, and nothing is written', [], async () => {
      await W.reboot(w);
      const cutSh = await W.seedSheet(w, { id: 'sh-rs2', orders: ['variant'], n: 2, cut: true });
      const rid = cR('variant');
      await w.page.evaluate(() => { CN.setMode('orders'); Orders.render(); }); await W.settle(w.page, 400);
      const can = await until(async () => { const c = await w.page.evaluate(rid => CharmNestReviewHold.canHold(rid), rid); return c && c.ok === false ? c : null; }, 6000);
      ok(can && /already cut/i.test(can.why) && !/\n/.test(can.why), 'canHold says one line: the sheet is already cut (REVHOLD): ' + J(can));
      const cutBefore = W.cutOf(w, cutSh), recBefore = W.sheetRecords(w, cutSh), mark = st.calls.length;
      await w.page.click(`#ordItems [data-rid="${rid}"] .revSendBtn`);
      const msg = await until(() => PAGE.msg(w, rid), 4000);
      ok(msg && /already cut/i.test(msg) && /cannot be held|can.t be held/i.test(msg) && msg.split(/\n/).length === 1 && msg.length < 140, 'ONE plain line says it is already cut and cannot be held (REVBTN): ' + J(msg));
      await W.settle(w.page, 1200);
      ok(!(await PAGE.mark(w, rid)).is && !st.doc(P + 'Charm_Custom_Orders', rid + '_review'), 'nothing was marked (the button says so instead, as Paul asked), no record');
      ok(!st.calls.slice(mark).some(c => c.op === 'reviewMarkPut'), 'no write: ' + J(st.calls.slice(mark).map(c => c.op)));
      const hb = await PAGE.hold(w, cutSh.id);
      ok(hb && !hb.held && !hb.by.length, 'the cut sheet is not held: ' + J(hb));
      ok(J(W.cutOf(w, cutSh)) === J(cutBefore) && W.sheetRecords(w, cutSh) === recBefore, 'the cut sheet and its pieces are exactly as they were: ' + W.recordsDiff(recBefore, W.sheetRecords(w, cutSh)));
    });

    // ── guards ──
    await step('0', 'SAFETY', 'nothing left the fake site and the page threw no error', [], async () => {
      ok(!w.foreign.length, 'a request left the loopback: ' + J(w.foreign.slice(0, 3)));
      const errs = w.errors.filter(e => !/firebase stub|Failed to fetch/.test(e)); ok(!errs.length, 'page errors: ' + J(errs));
    });
    return { linkAt: pickedAt };
  } finally { await w.close(); }
}

(async () => {
  for (const mode of only) { console.log(`\n== ${mode} ==`); try { await scenario(mode); } catch (e) { results.push({ mode, id: '-', owner: 'TEST', title: 'the world', status: 'FAIL', why: String(e && e.stack || e).split('\n').slice(0, 4).join(' | ') }); } }
  const rank = r => (r.id === '0' ? 99 : parseFloat(String(r.id).replace('b', '.5')) || 0);
  const rows = results.slice().sort((a, b) => a.mode.localeCompare(b.mode) * -1 || rank(a) - rank(b));
  for (const r of rows) console.log(`${r.status === 'ok' ? '  ok  ' : r.status === 'SKIP' ? ' SKIP ' : ' FAIL '} [${r.mode}] ${String(r.id).padStart(2)} ${r.owner.padEnd(26)} ${r.title}${r.status === 'FAIL' ? '\n          -> ' + r.why : r.status === 'SKIP' ? ' (' + r.why + ')' : r.note ? '\n          (' + r.note + ')' : ''}`);
  const bad = results.filter(r => r.status === 'FAIL');
  console.log(bad.length ? `\nreview-send-e2e: ${bad.length} step(s) FAILED, ${results.filter(r => r.status === 'SKIP').length} skipped, ${results.filter(r => r.status === 'ok').length} ok` : `\nreview-send-e2e OK: ${results.filter(r => r.status === 'ok').length} steps in ${only.join(' + ')}${results.some(r => r.status === 'SKIP') ? ', ' + results.filter(r => r.status === 'SKIP').length + ' skipped' : ''}`);
  process.exit(bad.length ? 1 : 0);
})();
