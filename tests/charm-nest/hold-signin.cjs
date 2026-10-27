// SIGNIN (Paul, 27 Oct 2026, order 4202871419): "The Hold button does not seem to be working since your above update. I was not signed in and this is why it was not allowing me to put it
// on hold. The issue is that it did not prompt me for the password to proceed. There was no feedback to the user as to why things were not working. Once I signed in [it worked]."
// The REAL sorter page over the fake site (_reviewSendWorld.cjs: the real charmNest* handlers over an in-memory Firestore; the Employee Number door is the fake's own roster, 666666 = Test
// Operator), two orders on one saved sheet, the order window open on the first. Every press that needs a person goes through one helper (CNEmployee.needSignIn):
//   1  signed out, Hold Order pressed: the sign-in bar (the Employee Number, masked) opens IN the order window, right under that button, and says it is to hold this order; nothing is written
//   2  put away (✕): nothing is written, and the line under the button says why ("Not held: nobody is signed in…")
//   3  pressed again, the number typed: the person is signed in and the hold goes on by itself, under that person; the number is kept nowhere and logged nowhere
//   4  the same records as a press made signed in (the second order, the control): the mark and the timeline line, nothing more, nothing less
//   5  Release Hold (the same treatment): signed out, the bar opens under it (before it says "Releasing…"); put away, a note in the window says why and nothing is written; signed in, it releases
//   6  Change SKU's Save (the same treatment): the bar opens under Save in the Change SKU window; put away, the window's own line says why and nothing is changed
//   PW_DIR=<playwright-core's node_modules> node tests/charm-nest/hold-signin.cjs   (CHROMIUM=<chrome>; SHOT=<path prefix> keeps a picture of the prompt over the order window)
'use strict';
const assert = require('assert');
const W = require('./_reviewSendWorld.cjs');
const results = [];
const ok = (name, cond, extra) => { assert.ok(cond, name + (extra ? ' :: ' + extra : '')); results.push(name); console.log('  ✓ ' + name); };
const sleep = ms => new Promise(r => setTimeout(r, ms));
const NUM = '666666';   // (the fake roster's Test Operator: bridge-server.cjs PINS)

(async () => {
  const w = await W.open('production');
  if (!w) { console.log('hold-signin: skipped (playwright-core not found; set PW_DIR)'); return; }
  const { page } = w, st = w.srv.st, logs = [];
  page.on('console', m => logs.push(m.text()));
  // the writes made for an order since `mark` (reads left out), as "op:action by"
  const writes = (mark, rid) => st.calls.slice(mark).filter(c => JSON.stringify(c.body || {}).includes(rid) && !/Get$|^get|List$|^cancelList|Pending$|Repair$/.test(String(c.op || ''))).map(c => `${c.op}${c.body && c.body.action ? ':' + c.body.action : ''}${c.body && c.body.by ? ' by ' + c.body.by : ''}`);
  const signOut = async () => { await page.evaluate(() => { CNAccount.signOut(); }); await page.waitForFunction(() => !CNEmployee.name()); await sleep(200); };
  const bar = () => page.evaluate(() => { const b = document.querySelector('.cnNameBar[data-kind="ask"]'); if (!b) return null; const r = b.getBoundingClientRect(), d = b.closest('dialog'); return { inWin: d ? d.id : '', num: b.dataset.num, type: (b.querySelector('input') || {}).type, why: b.querySelector('.cnNbWhy').textContent, box: { top: Math.round(r.top), left: Math.round(r.left), right: Math.round(r.right), bottom: Math.round(r.bottom) }, onTop: (() => { const h = document.elementFromPoint(r.left + r.width / 2, r.top + r.height / 2); return !!(h && b.contains(h)); })() }; });
  const boxOf = sel => page.evaluate(s => { const e = document.querySelector(s); if (!e) return null; const r = e.getBoundingClientRect(); return { top: Math.round(r.top), left: Math.round(r.left), right: Math.round(r.right), bottom: Math.round(r.bottom), text: e.textContent.trim() }; }, sel);
  const under = (b, btn) => !!(b && btn && b.box.top >= btn.bottom - 2 && b.box.top - btn.bottom < 40 && b.box.left < btn.right && b.box.right > btn.left);
  const signIn = async () => { await page.fill('.cnNameBar[data-kind="ask"] input', NUM); await page.press('.cnNameBar[data-kind="ask"] input', 'Enter'); };
  try {
    await W.addOrders(w, ['sheeted', 'mate']);
    const sh = await W.seedSheet(w, { id: 'sh-hsi1', orders: ['sheeted', 'mate'] });
    const rid = W.ORDERS.sheeted.rid, mate = W.ORDERS.mate.rid;
    ok('0 the page signs people in with their Employee Number (the gate is on) and Test Operator is signed in to start', await page.evaluate(() => CNAccount.gated() && CNEmployee.name() === 'Test Operator'));
    await page.evaluate(() => CN.setMode('orders'));
    await page.evaluate(k => OrderWin.open(k), W.keyOf('sheeted'));
    const rb = `#owPcSum .revSendBtn[data-rev-host][data-rid="${rid}"]`;
    await page.waitForSelector(rb, { timeout: 20000 });
    await signOut();

    // 1 · signed out, Hold Order: the prompt, in the window, under the button; nothing written
    let m0 = st.calls.length;
    await page.click(rb);
    await page.waitForSelector('#orderWin .cnNameBar[data-kind="ask"] input', { timeout: 8000 });
    await sleep(250);
    const b1 = await bar(), hb = await boxOf(rb);
    if (process.env.SHOT) await page.screenshot({ path: process.env.SHOT + '-prompt.png' });
    ok('1a signed out, Hold Order opens the sign-in bar IN the order window, on top, asking for the Employee Number (masked)', b1 && b1.inWin === 'orderWin' && b1.onTop && b1.num === '1' && b1.type === 'password', JSON.stringify(b1));
    ok('1b the bar stands right under the Hold Order button and says it is to hold this order', under(b1, hb) && /Sign in to hold this order/.test(b1.why), JSON.stringify({ bar: b1, btn: hb }));
    ok('1c nothing is written while it asks: the order is not held', writes(m0, rid).length === 0 && !(await page.evaluate(r => CharmNestReviewMark.isMarked(r), rid)) && !st.doc('Charm_Custom_Orders', rid + '_review'), JSON.stringify(writes(m0, rid)));

    // 2 · put away: nothing written, and the line under the button says why
    await page.click('.cnNameBar[data-kind="ask"] .cnNbX');
    await page.waitForFunction(() => !document.querySelector('.cnNameBar[data-kind="ask"]'));
    await page.waitForFunction(() => [...document.querySelectorAll('#orderWin .revSendMsg')].some(n => n.getBoundingClientRect().width > 0 && /nobody is signed in/.test(n.textContent)), null, { timeout: 5000 }).catch(() => {});
    await sleep(400);
    const said = await page.evaluate(() => [...document.querySelectorAll('#orderWin .revSendMsg')].filter(n => n.getBoundingClientRect().width > 0).map(n => n.textContent).join(' | '));
    if (process.env.SHOT) await page.screenshot({ path: process.env.SHOT + '-put-away.png' });
    ok('2a put away: the line under Hold Order says why nothing happened', /^Not held: nobody is signed in\. Sign in to hold this order\.$/.test(said), said);
    ok('2b and nothing was written: the order is not held, nobody is signed in', writes(m0, rid).length === 0 && !(await page.evaluate(r => CharmNestReviewMark.isMarked(r), rid)) && !st.doc('Charm_Custom_Orders', rid + '_review') && !(await page.evaluate(() => CNEmployee.name())), JSON.stringify(writes(m0, rid)));

    // 3 · pressed again, the number typed: signed in, and the hold goes on by itself
    m0 = st.calls.length;
    await page.click(rb);
    await page.waitForSelector('#orderWin .cnNameBar[data-kind="ask"] input', { timeout: 8000 });
    await signIn();
    await page.waitForFunction(r => CNEmployee.name() === 'Test Operator' && CharmNestReviewMark.isMarked(r) && !ReviewSend.busy(r) && !HoldUI.busy(r), rid, { timeout: 30000 });
    await sleep(1500);
    const wA = writes(m0, rid), docA = JSON.parse(JSON.stringify(st.doc('Charm_Custom_Orders', rid + '_review') || null));   // (as it stands now: 5 adds its release to it)
    ok('3a the number signs Test Operator in, and the hold goes on by itself: the order is held under that person', docA && docA.review && docA.review.state === 'open' && docA.review.markedBy === 'Test Operator' && wA.includes('reviewMarkPut:mark by Test Operator'), JSON.stringify({ wA, docA }));
    const kept = await page.evaluate(n => { const all = []; for (const s of [localStorage, sessionStorage]) for (let i = 0; i < s.length; i++) { const k = s.key(i); all.push(k + '=' + s.getItem(k)); } return all.filter(x => x.includes(n)); }, NUM);
    ok('3b the Employee Number is kept nowhere in the browser and is in no log line', kept.length === 0 && !logs.some(l => l.includes(NUM)) && !st.calls.some(c => c.name !== 'firebaseOrders' && JSON.stringify(c.body || {}).includes(NUM)), JSON.stringify({ kept, logs: logs.filter(l => l.includes(NUM)) }));

    // 5 · Release Hold, signed out: asked under it; put away, a note in the window and nothing written; signed in, released (the window of 3, still open: the held order's Release Hold)
    const rel = '#orderWin [data-rel-hold]:not([aria-disabled="true"])';
    await page.waitForFunction(s => { const b = document.querySelector(s); return !!(b && b.onclick && b.getBoundingClientRect().width > 0); }, rel, { timeout: 15000 });
    await signOut();
    m0 = st.calls.length;
    await page.click(rel);
    await page.waitForSelector('#orderWin .cnNameBar[data-kind="ask"] input', { timeout: 8000 });
    await sleep(250);
    const b5 = await bar(), rbx = await boxOf('#orderWin [data-rel-hold]');
    if (process.env.SHOT) await page.screenshot({ path: process.env.SHOT + '-release-prompt.png' });
    ok('5a signed out, Release Hold asks under itself, in the window, before it says "Releasing…"', b5 && b5.inWin === 'orderWin' && under(b5, rbx) && /Sign in to release this hold/.test(b5.why) && rbx.text === 'Release Hold', JSON.stringify({ b5, rbx }));
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => !document.querySelector('.cnNameBar[data-kind="ask"]'));
    await page.waitForFunction(() => [...document.querySelectorAll('#orderWin .mNote')].some(n => /Not released: nobody is signed in/.test(n.textContent)), null, { timeout: 5000 }).catch(() => {});
    const note5 = await page.evaluate(() => [...document.querySelectorAll('#orderWin .mNote')].map(n => { const r = n.getBoundingClientRect(); return { text: n.querySelector('.mNoteT').textContent, w: Math.round(r.width), top: Math.round(r.top) }; }));
    if (process.env.SHOT) await page.screenshot({ path: process.env.SHOT + '-release-put-away.png' });
    ok('5b put away (Esc): a note in the window, under the button, says why; nothing was written and the order is still held', note5.some(n => n.text === 'Not released: nobody is signed in. Sign in to release this hold.' && n.w > 0 && n.top >= rbx.bottom - 2) && writes(m0, rid).length === 0 && await page.evaluate(r => CharmNestReviewMark.isMarked(r), rid), JSON.stringify({ note5, w: writes(m0, rid) }));
    m0 = st.calls.length;
    await page.click(rel);
    await page.waitForSelector('#orderWin .cnNameBar[data-kind="ask"] input', { timeout: 8000 });
    await signIn();
    await page.waitForFunction(() => CNEmployee.name() === 'Test Operator', null, { timeout: 10000 });
    for (const t0 = Date.now(); Date.now() - t0 < 20000 && !writes(m0, rid).some(x => /^reviewMarkPut:release/.test(x));) await sleep(200);
    const w5 = writes(m0, rid), d5 = st.doc('Charm_Custom_Orders', rid + '_review');
    ok('5c signed in with the number, the release goes on by itself, under that person', w5.includes('reviewMarkPut:release by Test Operator') && d5 && d5.release && d5.release.by === 'Test Operator', JSON.stringify({ w5, release: d5 && d5.release }));

    // 4 · the control: the other order, pressed signed in: the same records
    await page.evaluate(() => OrderWin.isOpen() && OrderWin.close()); await sleep(400);
    await page.evaluate(k => OrderWin.open(k), W.keyOf('mate'));
    const mbtn = `#owPcSum .revSendBtn[data-rev-host][data-rid="${mate}"]`;
    await page.waitForSelector(mbtn, { timeout: 20000 });
    const m1 = st.calls.length;
    await page.click(mbtn);
    await page.waitForFunction(r => CharmNestReviewMark.isMarked(r) && !ReviewSend.busy(r) && !HoldUI.busy(r), mate, { timeout: 30000 });
    await sleep(1500);
    const wB = writes(m1, mate), docB = st.doc('Charm_Custom_Orders', mate + '_review');
    const shape = d => d && { keys: Object.keys(d).sort(), review: Object.keys(d.review || {}).sort(), kind: d.kind, state: d.state, by: d.review && d.review.markedBy };
    ok('4 the same records as a press made signed in: the same writes (the mark, the timeline line) and the same mark', JSON.stringify(wA.slice().sort()) === JSON.stringify(wB.slice().sort()) && JSON.stringify(shape(docA)) === JSON.stringify(shape(docB)), JSON.stringify({ wA, wB, a: shape(docA), b: shape(docB) }));

    // 6 · Change SKU's Save, signed out: asked under Save in its own window; put away, the window's line says why; nothing changed
    await page.evaluate(() => OrderWin.isOpen() && OrderWin.close()); await sleep(500);
    await page.evaluate(k => OrderWin.open(k), W.keyOf('mate'));
    await page.waitForSelector('#owEditSku', { timeout: 15000 });
    const offer = await page.evaluate(() => !document.getElementById('owEditSku').disabled);
    if (!offer) console.log('  NOTE Change SKU is not offered for this piece in this world: 6 not checked');
    else {
      await signOut();
      await page.click('#owEditSku');
      await page.waitForSelector('#owSkuDlg[open] #owSkuInput');
      const pick = await page.evaluate(() => [...B.master.entries.keys()].find(k => k !== 'COMET_9427' && /SCHRODINGER CAT 2/.test(k)) || [...B.master.entries.keys()].find(k => k !== 'COMET_9427'));
      await page.fill('#owSkuInput', pick); await sleep(300);
      await page.waitForFunction(() => !document.getElementById('owSkuSave').disabled, null, { timeout: 5000 });
      m0 = st.calls.length;
      await page.click('#owSkuSave');
      await page.waitForSelector('#owSkuDlg .cnNameBar[data-kind="ask"] input', { timeout: 8000 });
      await sleep(250);
      const b6 = await bar(), sbx = await boxOf('#owSkuSave');
      ok('6a signed out, Change SKU\'s Save asks under itself, in the Change SKU window', b6 && b6.inWin === 'owSkuDlg' && b6.onTop && /Sign in to change this SKU/.test(b6.why) && b6.box.left < sbx.right + 2 && (b6.box.top >= sbx.bottom - 2 || b6.box.bottom <= sbx.top + 2), JSON.stringify({ b6, sbx }));
      await page.click('.cnNameBar[data-kind="ask"] .cnNbX');
      await page.waitForFunction(() => /nobody is signed in/.test(document.querySelector('#owSkuCheck .say').textContent), null, { timeout: 5000 }).catch(() => {});
      const line6 = await page.evaluate(() => ({ say: document.querySelector('#owSkuCheck .say').textContent, busy: document.getElementById('owSkuDlg').dataset.busy || '', sku: (Orders.rows().find(r => String(r.order.receiptId) === '4291000006') || { spec: {} }).spec.designSku }));
      ok('6b put away: the window\'s own line says why, and nothing is changed', /^Not changed: nobody is signed in\. Sign in to change this SKU\.$/.test(line6.say) && line6.busy === '' && line6.sku === 'COMET_9427' && !st.calls.slice(m0).some(c => /sku|alias|editSku/i.test(String(c.op || '')) && !/Get$|^get/.test(String(c.op || ''))), JSON.stringify(line6));
      await page.evaluate(() => { const d = document.getElementById('owSkuDlg'); if (d.open) d.close(); });
    }
    ok('7 no script error on the page throughout', w.errors.length === 0, (w.errors || []).join(' | '));
  } finally { await w.close(); }
  console.log(`hold-signin: ${results.length} checks passed`);
})().catch(e => { console.error('FAIL:', e && e.message || e); process.exit(1); });
