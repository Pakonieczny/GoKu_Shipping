// ADV-SPECIALS (Paul, 15 Oct 2026: "resilient against lost internet connections, interruptions and unexpected user behaviour"): a CUSTOM order's Send to Sheet, in the real sorter page
// (headless Chromium, the repo's offline fake site over the REAL charmNestLibrary handler and an in-memory Firestore; nothing live, nothing printed).
// What is lost, duplicated, stuck or wrong when the connection goes in the middle of the send, the page is reloaded, or the press is made twice?
//   A · the network is down at the press (context.setOffline): nothing is recorded, nothing is pretended (no sent decision, no cloud record, no seal), the card says it was saved for retry; a second press offline
//       changes nothing; with the network back the SAME press sends, and exactly one decision, one claim per line, one timeline event and one copy of each file exist
//   B · the press again after it was sent (a double click, a second bench): "already on the sheets", no second decision, no second event, the cloud record is not written again
//   C · the final cloud write is lost (the reply or the request, the 'sent' phase): the decision stays a recorded intent (who, when, which copy of which file), a reload finds it in the workspace and the page
//       finishes it by itself with the ORIGINAL signer and time: one 'sent' record, one history entry, one timeline event, never a second decision under the reloading person's name or a later time
//   D · a CHAIN-ONLY line (never cut) in the same order list stays "no design" however often the list is read again and across a reload: it used to flip back to "pulled" at every reading (the Orders list then
//       showed "pulled" instead of "chain only", and every intake took it up again to find it had nothing to cut)
//   node tests/charm-nest/adv-specials-send.cjs [playwright-core dir]
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const order = (rid, lines) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + String(rid).slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], metals: {}, lines });
const line = (tid, sku, title, variations, extra) => Object.assign({ transactionId: String(tid), listingId: String(1800000000 + (tid % 100000)), sku, title, quantity: 1, expectedShipDate: SHIP, variations: variations.map(([name, value]) => ({ name, value })), personalization: [], metalKey: 'gold', metalLabel: 'GF 14/20' }, extra || {});
const A = order(4175423830, [line(41754238301, 'CUSTOM-N-002-665442', 'Custom Name Necklace, Personalized Gold Charm Necklace', [['Metal', '14k Gold Filled']])]);
const B = order(4175423831, [line(41754238311, 'CUSTOM-N-003-665443', 'Custom Name Necklace, Personalized Gold Charm Necklace', [['Metal', '14k Gold Filled']])]);
const CH = order(4176744752, [line(41767447521, 'CHAIN_8941', 'CHAIN REPLACEMENT', [['Metal', 'Gold'], ['Length', '17 Inches']])]);   // chain only: never cut
const AK = '4175423830_41754238301', BK = '4175423831_41754238311', CK = '4176744752_41767447521';

const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
let DESIGN_AI = '';
async function designFixture() {
  global.window = global; global.PDFLib = require(path.join(root, 'vendor/pdf-lib-1.17.1.min.js'));
  const D = require(path.join(root, 'charm-nest-dxf.js'));
  const tag = DG(0, 'SECTION', 2, 'ENTITIES', 0, 'CIRCLE', 8, '0', 10, 0, 20, 0, 40, 7, 0, 'CIRCLE', 8, '0', 10, 0, 20, 5, 40, 1, 0, 'ENDSEC', 0, 'EOF');
  DESIGN_AI = Buffer.from((await D.toPdf(tag, 'tag.dxf')).bytes).toString('base64');   // an .ai is a PDF
}

(async () => {
  await designFixture();
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); return; }
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      if (/fonts\.googleapis|fonts\.gstatic/.test(r.request().url())) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      return r.abort();
    });
    // a lost final answer: the request of the 'sent' phase of customSheetPut never arrives (a fetch that rejects), while `drop.sent` is on
    const drop = { sent: false, dropped: 0 }, puts = [];
    await context.route('**/.netlify/functions/charmNestLibrary*', route => {
      let b = {}; try { b = JSON.parse(route.request().postData() || '{}'); } catch (_) { /* not JSON */ }
      if (b.op === 'customSheetPut') { puts.push({ ck: b.record && b.record.ck, phase: b.record && b.record.phase }); if (drop.sent && b.record && b.record.phase === 'sent') { drop.dropped++; return route.abort('connectionreset'); } }
      return route.fallback();
    });
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    const boot = async () => {
      await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
      await page.waitForFunction(() => !window.__oldPage && window.CN && window.Orders && window.Review && window.CustomSheet && window.OrderWin && CN.S.cloud.ok === true, null, { timeout: 60000 });
    };
    await boot();
    await page.evaluate(async orders => {
      await Orders.loadMaps(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
    }, [A, B, CH]);
    await page.click('#reviewView .egTab[data-k="customOrder"]');
    const card = rid => `#rvList .reviewListRow[data-rid="${rid}"]`;
    const dropDesign = (sel, name) => page.evaluate(({ sel, name, b64 }) => {
      const dt = new DataTransfer(); dt.items.add(new File([Uint8Array.from(atob(b64), c => c.charCodeAt(0))], name));
      const n = document.querySelector(sel); for (const t of ['dragenter', 'dragover', 'drop']) n.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt }));
    }, { sel, name, b64: DESIGN_AI });
    const addDesign = async (rid, name) => {
      await dropDesign(card(rid), name);
      await page.waitForFunction(() => document.querySelector('#cuDlg[open] .cuFile .cuThumb img'), null, { timeout: 30000 });
      await page.click('#cuDlg [data-x]');
      await page.waitForFunction(() => !document.querySelector('#cuDlg[open]'));
    };
    const snap = ck => page.evaluate(ck => { const e = B.customDesigns[ck]; return e ? { sent: e.sent ? { id: e.sent.id, at: e.sent.at, by: e.sent.by } : null, intent: e.sendIntent ? { id: e.sendIntent.id, at: e.sendIntent.at, by: e.sendIntent.by } : null, err: e.sendError || '', pending: !!e.sendCloudPending, files: e.files.length, cloud: e.files.map(F => !!F.cloud) } : null; }, ck);
    const cards = () => srv.st.list('Charm_Custom_Sheet').filter(d => /^card-/.test(d._id)), claims = () => srv.st.list('Charm_Custom_Sheet').filter(d => /^line-/.test(d._id));
    const events = lineKey => srv.st.list('Order_Timeline').filter(d => d.type === 'designSent' && d.lineKey === lineKey);
    const toasts = () => page.evaluate(() => [...document.querySelectorAll('#toasts .toast')].map(n => n.textContent));
    const ckA = 'custom:4175423830:CUSTOM-N-002-665442', ckB = 'custom:4175423831:CUSTOM-N-003-665443';
    // the network is back: the page notices at its next good answer (S.cloud.ok), which a read of the shop's maps gives at once
    const online = async () => { await context.setOffline(false); await page.evaluate(() => Orders.loadMaps(true)).catch(() => {}); await page.waitForFunction(() => CN.S.cloud.ok === true, null, { timeout: 30000 }); };

    /* ── A · the network is down at the press ── */
    await addDesign('4175423830', 'name-tag.ai');
    assert.equal((await snap(ckA)).files, 1, 'the design is on the card');
    await context.setOffline(true);
    await page.click(card('4175423830') + ' [data-cu-send]');
    await page.waitForFunction(ck => B.customDesigns[ck] && B.customDesigns[ck].sendError, ckA, { timeout: 30000 });
    let s = await snap(ckA);
    assert.equal(s.sent, null, 'offline at the press: no sent decision is pretended');
    assert.match(s.err, /fetch|network|offline/i, 'and the reason is kept for the retry: ' + s.err);
    assert.equal(cards().length, 0, 'no cloud record of a decision that was never made'); assert.equal(claims().length, 0); assert.equal(events(AK).length, 0, 'no timeline event');
    await page.waitForFunction(() => [...document.querySelectorAll('#toasts .toast')].some(n => /saved for retry/.test(n.textContent)), null, { timeout: 20000 }).catch(async () => { throw new Error('the page says it was saved for retry: ' + (await toasts()).join(' | ')); });
    // a second press while still offline (an impatient double click) changes nothing
    await page.click(card('4175423830') + ' [data-cu-send]').catch(() => {});
    await page.waitForTimeout(600);
    s = await snap(ckA); assert.equal(s.sent, null); assert.equal(cards().length, 0);
    // the network is back: the same press sends
    await online();
    await page.click(card('4175423830') + ' [data-cu-send]');
    await page.waitForFunction(k => CustomSheet.decisionOf(B.orders.byKey.get(k)), AK, { timeout: 30000 });
    await page.waitForFunction(() => !document.querySelector('#rvList .cuStat, #rvList .btn.working'), null, { timeout: 30000 });
    s = await snap(ckA);
    assert(s.sent && s.sent.by === 'Test Operator' && s.sent.at > 1e12 && !s.intent, 'sent by the operator: ' + JSON.stringify(s));
    assert.equal(cards().length, 1, 'exactly one cloud record of the decision'); assert.equal(cards()[0].phase, 'sent'); assert.equal(cards()[0].history.length, 1, 'one history entry');
    assert.equal(claims().length, 1, 'one claim per line'); assert.equal(events(AK).length, 1, 'one timeline event for the line');
    assert.equal([...srv.st.blobs.keys()].filter(k => /charmnest\/(sandbox\/)?custom\/4175423830\//.test(k)).length, 1, 'one copy of the file in the cloud, not one per attempt');
    /* ── B · the press again after it was sent ── */
    const putsBefore = puts.length;
    await page.evaluate(k => { const r = B.orders.byKey.get(k); return CustomSheet.send({ key: 'ord:custom:4175423830:CUSTOM-N-002-665442', rows: [r], row: r }); }, AK);
    await page.waitForTimeout(500);
    assert.equal(puts.length, putsBefore, 'a second send writes nothing');
    assert.deepEqual(await snap(ckA), s, 'the decision is the one it was: same id, same time, same signer'); assert.equal(cards().length, 1); assert.equal(events(AK).length, 1);
    assert((await toasts()).some(t => /already on the sheets/.test(t)), 'and it says so');

    /* ── C · the final cloud write is lost, then the page is reloaded ── */
    await page.click('#reviewView .egTab[data-k="customOrder"]').catch(() => {});
    await addDesign('4175423831', 'name-tag-2.ai');
    drop.sent = true;
    await page.click(card('4175423831') + ' [data-cu-send]');
    await page.waitForFunction(ck => B.customDesigns[ck] && B.customDesigns[ck].sendError, ckB, { timeout: 30000 });
    const mid = await snap(ckB);
    assert(drop.dropped >= 1, 'the final write was lost'); assert.equal(mid.sent, null, 'not sent as far as this page knows');
    assert(mid.intent && mid.intent.id && mid.intent.by === 'Test Operator' && mid.intent.at > 1e12, 'the decision is kept as a recorded intent: ' + JSON.stringify(mid));
    assert.equal(cards().length, 2); assert.equal(cards().find(c => c.rid === '4175423831').phase, 'pending', 'the cloud holds the pending phase of exactly this decision');
    assert.equal(events(BK).length, 0, 'the line has no "sent" event yet');
    // the other bench reloads the page (a closed tab, a crash): the workspace is saved, the network is back
    drop.sent = false;
    await page.evaluate(async () => { await Session.flush(); window.__oldPage = true; });
    await boot();
    await page.waitForFunction(k => B.orders.byKey.get(k) && CustomSheet.decisionOf(B.orders.byKey.get(k)), BK, { timeout: 45000 });
    await page.waitForFunction(() => !document.querySelector('#rvList .cuStat, #rvList .btn.working'), null, { timeout: 30000 });
    const after = await snap(ckB);
    assert.deepEqual(after.sent, { id: mid.intent.id, at: mid.intent.at, by: mid.intent.by }, 'finished with the ORIGINAL id, time and signer: ' + JSON.stringify(after));
    assert.equal(after.intent, null); assert.equal(after.err, '');
    const rec = cards().find(c => c.rid === '4175423831');
    assert.equal(cards().length, 2, 'still one record per order'); assert.equal(rec.phase, 'sent'); assert.deepEqual(rec.history.map(h => [h.id, h.by, h.at]), [[mid.intent.id, mid.intent.by, mid.intent.at]], 'one history entry, the original');
    assert.equal(claims().length, 2, 'one claim per line'); assert.equal(events(BK).length, 1, 'one timeline event');
    assert.equal(events(AK).length, 1, 'the first order is untouched by the reload');
    /* ── D · a chain-only line stays "no design" ── */
    const chain = () => page.evaluate(k => { const r = B.orders.byKey.get(k); return { state: r.state, noDesign: !!(r.spec && r.spec.noDesign), pool: r.poolIds.length }; }, CK);
    assert.deepEqual(await chain(), { state: 'noDesign', noDesign: true, pool: 0 }, 'chain only: nothing to cut (first reading)');
    for (let i = 0; i < 3; i++) await page.evaluate(() => { Orders.interpretAll(); Review.syncOrderItems(); });
    assert.deepEqual(await chain(), { state: 'noDesign', noDesign: true, pool: 0 }, 'read again three times: still "no design", not "pulled" (the intake takes up every pulled line)');
    await page.evaluate(async () => { await Session.flush(); window.__oldPage = true; });
    await boot();
    await page.waitForFunction(k => B.orders.byKey.get(k), CK, { timeout: 45000 });
    assert.deepEqual(await chain(), { state: 'noDesign', noDesign: true, pool: 0 }, 'and after a reload');
    assert.deepEqual(errors.filter(m => !/failed to fetch|networkerror|load failed|net::/i.test(m)), [], 'no page errors (the fetches the test cut itself are not errors)');
    console.log('  ok  A offline press · B press again · C lost final write + reload · D chain only stays no-design (Chromium)');
  } finally { await browser.close(); srv.close(); }
  console.log('adv-specials-send OK: a custom order\'s Send to Sheet is neither lost, doubled nor re-signed by a lost connection, a second press or a reload');
})().catch(e => { console.error(e); process.exit(1); });
