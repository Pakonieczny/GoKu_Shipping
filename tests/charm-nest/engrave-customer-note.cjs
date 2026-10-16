// ENGNOTE (engrave-ui-1012, round 2, Paul's point 6): "I see that you removed the customer note from this view. I didn't ask for you to remove that and you need to put that back so the user can understand and see
// what the customer wrote/communicated." The Engrave tab's detail (placementCard) shows the customer's own words ALWAYS, as ONE box titled "What the customer wrote": the personalisation, the buyer's message, the
// staff note and the customer's engraving-related option choices; nothing (no empty box) when the customer wrote and chose nothing. The blocks ENGTRIM removed stay removed.
// Real charm-nest-1.html in headless Chromium over the fake loopback site (bridge-server.cjs): no Etsy, no paid reading, no live write. Names are made up.
//   1  an order with personalisation, buyer message, staff note and options (front + back, a font): the box is in the Engrave detail, once, titled, with the exact words and the engraving options
//      only (the metal is not one of them), on a settled read, on an uncertain read and on a read a person decided (not only when the read is uncertain)
//   2  the blocks ENGTRIM removed are still absent: "From the order", Reading / Font / Decided by, the "NN% sure" pill, the block titles, the second words box, the reference picture, the head's face switch
//   3  an order where the customer wrote and chose nothing: no box at all (no title either); a part left empty is not drawn (only the options; only the personalisation)
//   4  the words are shown as text (no markup of the customer's is interpreted)
//   5  the card of an APPROVED piece (its row opens the same detailed card: there is no Decided row or detail any more) shows the same box (and none for an order with nothing); its row carries the words and the seal
//   6  the Review tab's card keeps its old "From the order" list and gets no second box
//   (the order window's own "What the customer wrote" is ENGMSG's, not tested here)
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/engrave-customer-note.cjs
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const SHIP = Math.floor(Date.UTC(2026, 9, 14, 17) / 1000), RID = '4195966044', TX = '5009123456790', KEY = `${RID}_${TX}`;
const WORDS = { pers: 'front - N\nback - Name One', buyer: 'Please use a plain look, thank you', staff: 'Name Two checked the spelling with the buyer', front: 'N' };
const ORDER = { receiptId: RID, orderNumber: RID, createTs: SHIP - 5 * 86400, updateTs: SHIP - 5 * 86400 + 60, shipBy: SHIP, buyer: { name: 'Name One' }, buyerMessage: WORDS.buyer, isGift: false, giftMessage: '', staffNote: WORDS.staff, messages: [],
  lines: [{ transactionId: TX, listingId: '4458930445', sku: 'MINI_0445', title: 'Mini Tag Initial Charm Add On Charm Personalized Letter Charm', quantity: 1, expectedShipDate: SHIP, metalKey: 'gold', metalLabel: 'GF 14/20',
    variations: [{ name: 'Charm Type', value: 'Tag1 (front+back)' }, { name: 'Font choice', value: 'Ace' }, { name: 'Metal Choice', value: 'Gold Filled' }], personalization: [WORDS.pers] }] };

// the reading itself (pure, node): which options are "about the engraving", placeholders stripped, nothing when nothing was written
{
  const assert = require('assert/strict');
  global.CharmNestOrders = require(path.join(root, 'charm-nest-orders.js'));
  const Said = require(path.join(root, 'charm-nest-engrave-said.js'));
  const row = (variations, spec) => ({ line: { variations, personalization: [] }, spec: Object.assign({ options: [] }, spec) });
  const names = (variations, spec) => Said.choicesOf(row(variations, spec)).map(c => c.name + '=' + c.value);
  assert.deepEqual(names([{ name: 'Metal', value: 'Gold Filled' }, { name: 'Necklace Length', value: '16"' }, { name: 'Size', value: 'Large' }]), [], 'metal, length and size are not about the engraving');
  assert.deepEqual(names([{ name: 'Personalization', value: 'Name One' }]), [], 'the personalisation box is said as the personalisation, not as an option');
  assert.deepEqual(names([{ name: 'Charm Type', value: 'Tag1 (front+back)' }, { name: 'Font choice', value: 'Ace' }, { name: 'Metal Choice', value: 'Gold' }]), ['Charm Type=Tag1 (front+back)', 'Font choice=Ace'], 'a value that names the faces, a font');
  assert.deepEqual(names([{ name: 'Metal/Both Sides Engraved? (Front/Back)', value: 'GOLD\u00b7Front' }]), ['Metal/Both Sides Engraved? (Front/Back)=GOLD\u00b7Front'], 'an option named for the sides');
  assert.deepEqual(names([{ name: 'Choose', value: 'Gold - 16" - 2 sides' }]), ['Choose=Gold - 16" - 2 sides'], 'a count of sides');
  assert.deepEqual(names([{ name: 'Engraving', value: 'No engraving' }]), ['Engraving=No engraving'], 'no engraving is a choice too');
  assert.deepEqual(Said.rowsOf(row([], { buyerMessage: ' Hi \uFFFC ', staffNote: '   ' })), [['Buyer message', 'Hi']], 'placeholders and blanks are not words');
  assert.equal(Said.html(row([{ name: 'Metal', value: 'Gold' }], {})), '', 'nothing written, nothing chosen: no box at all');
  assert.ok(/<h4 class="pvH">What the customer wrote<\/h4>/.test(Said.html(row([], { buyerMessage: 'Name One' }))) && !/<script/.test(Said.html(row([], { buyerMessage: '<script>x</script>' }))), 'the title, and the words are escaped');
  console.log('  ✓ the reading (node): engraving options only, placeholders stripped, escaped, nothing when nothing');
}

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { try { ({ chromium } = require('/opt/node22/lib/node_modules/playwright/node_modules/playwright-core')); } catch (__) { console.log('engrave-customer-note: playwright-core not found, skipped'); process.exit(0); } }
  const shots = process.env.SHOTS || ''; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] });
  const POOLID = KEY + '_1';
  srv.st.put('Charm_Nest_Sheets', 'gold-su-1', { id: 'gold-su-1', metal: 'gold', charms: [{ id: 'g0', poolId: POOLID, order: RID }], placements: [{ id: 'g0', cxPt: 40, cyPt: 40, angle: 0 }], poolIds: [POOLID], orders: [RID], sheetIndex: 1, fileBase: 'gold-su-1', runId: 'run-test-1' });
  srv.st.put('Charm_Pool', POOLID, { poolId: POOLID, orderId: RID, sheetId: 'gold-su-1', state: 'placed', material: 'gold' });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
  try {
    const context = await browser.newContext({ viewport: { width: 1280, height: 720 }, deviceScaleFactor: 1 });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.route(/\/\.netlify\/functions\/etsyMailOrderLink/, r => r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ ok: true, n: 0, engagements: [], active: null, conversation: null }) }));
    await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Engrave && window.CharmNestEngravePanel && window.CharmNestEngraveSaid && CN.S.cloud.ok === true, null, { timeout: 60000 });

    // ── the order (Front + back) and its back job, placed and settled ──
    await page.evaluate(async ({ order, KEY }) => {
      const line = order.lines[0];
      const row = { key: KEY, order, line, arrivedAt: Date.now() - 3600e3, spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [KEY + '_1'], engrave: null, material: null };
      B.orders.rows.push(row); B.orders.byKey.set(KEY, row);
      Orders.interpretAll(); await Orders.loadMaps(true); Orders.interpretAll();
      row.spec.engraveCandidate = true;
      const j = Engrave.ensureJob(row); Object.assign(j, { state: 'review', text: 'Name One', lines: ['Name One'], fit: { size: 10, capMm: 2, centre: [0, 0], angle: 0, weight: 'Regular' }, view: {}, verify: { geometry: { ok: true } }, source: 'personalization', confidence: 0.97 });
      row.engrave = { needed: true, state: 'review', approved: false, text: j.text };
      window.__calls = []; const call = DesignLink.call; DesignLink.call = function (name) { __calls.push(name); return call.apply(this, arguments); };
    }, { order: ORDER, KEY });

    // open the Engrave detail on the piece's back job after `patch` is applied to the job, `spec` to its reading and `line` to its Etsy line, and read what the detail draws, in one go
    const detail = ({ patch, spec, line }) => page.evaluate(async ({ KEY, patch, spec, line }) => {
      const row = B.orders.byKey.get(KEY), j = Engrave.jobOf(row, null, 'back'), was = {}, wasSpec = {}, wasLine = {};
      for (const k of Object.keys(patch || {})) { was[k] = j[k]; j[k] = patch[k]; }
      for (const k of Object.keys(spec || {})) { wasSpec[k] = row.spec[k]; row.spec[k] = spec[k]; }
      for (const k of Object.keys(line || {})) { wasLine[k] = row.line[k]; row.line[k] = line[k]; }
      setMode('engrave'); Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: j.key }); Engrave.render();
      const c = document.querySelector('#egQueue .rvItem[data-kind=placement]');
      const q = s => c ? c.querySelectorAll(s).length : -1, txt = c ? c.innerText.replace(/\s+/g, ' ') : '';
      const box = c && c.querySelector('[data-said]');
      const out = { has: !!c, text: txt, panel: q('.engp'), said: q('[data-said]'), saidInSide: q('.pvSide [data-said]'), saidText: box ? box.innerText.replace(/\s+/g, ' ').trim() : '', saidTitle: box && box.querySelector('.pvH') ? box.querySelector('.pvH').textContent : '',
        labels: box ? [...box.querySelectorAll('.lbl')].map(l => l.textContent) : [], boxes: box ? box.querySelectorAll('.owSaid').length : 0, injected: box ? box.querySelectorAll('.owSaid b, .owSaid i, .owSaid img, .owSaid script').length : -1,
        order: q('.pvOrder'), words: q('.pvWords'), oldBox: q('textarea[data-f=words]'), use: q('[data-a=usewords]'), conf: q('.rh .conf'), ref: q('.pvRef'), mainTitle: q('.pvMain > .pvH'), checkTitle: q('.pvCheck .pvH'), orderTitle: q('.pvOrder .pvH'), faceSw: q('.rh .egFaces'),
        titles: [...(c ? c.querySelectorAll('h4.pvH') : [])].map(h => h.textContent.trim()), thumbs: q('.engFace .engThumb'), emoji: q('[data-emoji]'), approve: q('[data-a=approve]'), seg: [...(c ? c.querySelectorAll('.engpMode:not(.engpFaceSw) .engpSeg') : [])].map(b => b.textContent.trim()).join('|') };
      for (const k of Object.keys(was)) j[k] = was[k]; for (const k of Object.keys(wasSpec)) row.spec[k] = wasSpec[k]; for (const k of Object.keys(wasLine)) row.line[k] = wasLine[k];
      return JSON.parse(JSON.stringify(out));
    }, { KEY, patch, spec, line });
    const shot = async name => { if (shots) await page.screenshot({ path: path.join(shots, name + '.png') }); };
    const METAL = [{ name: 'Metal Choice', value: 'Gold Filled' }];

    // ── 1 · everything the customer wrote and chose ──
    let d = await detail({});
    check(d.has && d.panel === 1, 'the Engrave detail is open and carries the options panel');
    check(d.said === 1 && d.saidInSide === 1 && d.boxes === 1, 'ONE "What the customer wrote" box is in the detail (beside the panel, in its side column): ' + JSON.stringify({ said: d.said, side: d.saidInSide, boxes: d.boxes }));
    check(d.saidTitle === 'What the customer wrote', 'it says plainly what it is: "' + d.saidTitle + '"');
    check(d.saidText.includes('front - N back - Name One'), 'it holds the personalisation exactly as the customer wrote it: "' + d.saidText + '"');
    check(d.saidText.includes(WORDS.buyer), 'it holds the buyer\'s message exactly: "' + WORDS.buyer + '"');
    check(d.saidText.includes(WORDS.staff), 'it holds the staff note exactly: "' + WORDS.staff + '"');
    check(d.labels.join('|') === 'Personalisation|Buyer message|Staff note|Options', 'each part under its own small label: ' + d.labels.join('|'));
    check(/Charm Type: Tag1 \(front\+back\)/.test(d.saidText) && /Font: Ace/.test(d.saidText), 'the customer\'s engraving options are in it (front + back, the font): "' + d.saidText.replace(/^.*Options/, 'Options') + '"');
    check(!/Metal Choice|Gold Filled/.test(d.saidText), 'an option that is not about the engraving (the metal) is not in it');
    await shot('said-settled-1280');
    d = await detail({ patch: { confidence: 0.45 } });
    check(d.said === 1 && d.saidText.includes(WORDS.buyer), 'on an uncertain read (45%) the box is there');
    d = await detail({ patch: { confidence: 0.99, source: 'personalization', decision: { by: 'Name One', at: Date.now(), text: 'Name One', note: 'confirmed' } } });
    check(d.said === 1 && d.saidText.includes(WORDS.staff), 'on a read a person has decided the box is still there (always visible, not only when uncertain)');
    d = await detail({ patch: { state: 'words', reason: 'Say what to engrave on the back', fit: null, view: null } });
    check(d.said === 1 && d.saidText.includes(WORDS.pers.split('\n')[0]), 'with words still to decide the box is there');
    d = await detail({ patch: { state: 'fitting', fit: null, view: null } });
    check(d.said === 1, 'while the placement is being prepared the box is there');

    // ── 2 · what ENGTRIM removed stays removed ──
    d = await detail({});
    check(d.order === 0 && d.orderTitle === 0 && !/From the order|Reading|Decided by|Personalization\b|Buyer's note/.test(d.text), 'no "From the order" list, no Reading / Font / Decided by rows, no old labels: ' + d.order);
    check(d.conf === 0 && !/\d+\s*%\s*sure|\bsure\b/i.test(d.text), 'no "NN% sure" pill');
    check(d.words === 0 && d.oldBox === 0 && d.use === 0 && !/Words on the|Use these words|Engrave these words/i.test(d.text), 'no second "Words on the back" box, no "Use these words"');
    check(d.ref === 0 && !/reference/i.test(d.text) && d.mainTitle === 0 && d.checkTitle === 0 && d.faceSw === 0, 'no reference picture, no big picture title, no "To check with the buyer" title, no head Front | Back switch');
    check(d.titles.join('|') === 'What the customer wrote', 'the box\'s is the ONLY block title on the detail: ' + JSON.stringify(d.titles));
    check(d.emoji === 1 && d.approve === 1 && d.seg === 'None|Front|Back|Front + back' && d.thumbs === 2, 'the panel, the card\'s two pictures, one Emoji button and Approve are all still there');

    // ── 3 · the customer wrote and chose nothing: no box; a part left empty is not drawn ──
    const NONE = { patch: {}, spec: { personalization: [], buyerMessage: '', staffNote: '' }, line: { variations: METAL, personalization: [] } };
    d = await detail(NONE);
    check(d.has && d.panel === 1 && d.said === 0 && !/What the customer wrote/.test(d.text) && d.titles.length === 0, 'the customer wrote and chose nothing: no box and no title: ' + JSON.stringify({ said: d.said, titles: d.titles }));
    d = await detail({ spec: NONE.spec, line: { variations: METAL.concat([{ name: 'Font choice', value: 'Ace' }]), personalization: [] } });
    check(d.said === 1 && d.labels.join('|') === 'Options' && /Font: Ace/.test(d.saidText), 'only an engraving option chosen: the box holds only the options: ' + d.labels.join('|'));
    d = await detail({ spec: { personalization: [WORDS.pers], buyerMessage: '', staffNote: '' }, line: { variations: METAL, personalization: [WORDS.pers] } });
    check(d.said === 1 && d.labels.join('|') === 'Personalisation', 'only the personalisation written: the box holds only that: ' + d.labels.join('|'));
    d = await detail({ spec: { personalization: [], buyerMessage: '  ', staffNote: '' }, line: { variations: METAL, personalization: [] } });
    check(d.said === 0, 'a message of blank spaces is nothing: no box');

    // ── 4 · shown as text ──
    d = await detail({ spec: { staffNote: 'Name Two wrote <b>bold</b> & <img src=x>' } });
    check(d.said === 1 && d.injected === 0 && d.saidText.includes('<b>bold</b> & <img src=x>'), 'the customer\'s words are text, never markup: ' + d.saidText.slice(-60));

    // ── 5 · an approved piece: its row opens the SAME detailed card, which shows the same box; the row in the one list says the words and carries the seal ──
    const APPROVED = { state: 'approved', approvedBy: 'Name One', approvedAt: Date.now() - 60e3, decision: { by: 'Name One', at: Date.now() - 60e3, text: 'Name One' } };
    const apprRow = () => page.evaluate(async KEY => {
      const row = B.orders.byKey.get(KEY), j = Engrave.jobOf(row, null, 'back'), was = { state: j.state, by: j.approvedBy, at: j.approvedAt, decision: j.decision };
      Object.assign(j, { state: 'approved', approvedBy: 'Name One', approvedAt: Date.now() - 60e3, decision: { by: 'Name One', at: Date.now() - 60e3, text: j.text } });
      setMode('engrave'); Engrave.restoreView({ list: true, chosen: true, focus: null, q: '' }); Engrave.render();
      const r = document.querySelector(`#egQueue .egPlacementList .placementRow[data-open="${j.key}"]`);
      const out = { row: !!r, words: r && r.querySelector('.w') ? r.querySelector('.w').textContent.trim() : '', seals: r ? r.querySelectorAll('.egPlacementSeals .seal').length : 0, stage: r && r.querySelector('[data-s]') ? r.querySelector('[data-s]').textContent.trim() : '', old: document.querySelectorAll('#egDone, #egBacks, .decidedRow, .doneDetail').length };
      Object.assign(j, { state: was.state, approvedBy: was.by, approvedAt: was.at, decision: was.decision }); Engrave.render();
      return out;
    }, KEY);
    let dd = await detail({ patch: APPROVED });
    check(dd.has && dd.said === 1 && dd.saidInSide === 1 && /what the customer wrote/i.test(dd.saidText) && dd.saidText.includes(WORDS.buyer) && dd.saidText.includes(WORDS.staff) && /Font: Ace/.test(dd.saidText), 'an approved piece\'s card shows the box with the customer\'s words and options: "' + dd.saidText.slice(0, 120) + '"');
    check(dd.panel === 1 && dd.approve === 0 && /Approved/.test(dd.text) && !/Decided by|Reading/.test(dd.text), 'it is the same detailed card (the options panel is there), with the calm "Approved" where the Approve was, and still no "Reading" / "Decided by" rows: ' + JSON.stringify({ panel: dd.panel, approve: dd.approve }));
    const ar = await apprRow();
    check(ar.row && ar.words === 'Name One' && ar.seals >= 1 && /Approved/.test(ar.stage) && ar.old === 0, 'and its row in the one list says the words and carries the seal (no Decided row anywhere): ' + JSON.stringify(ar));
    dd = await detail({ patch: APPROVED, spec: NONE.spec, line: NONE.line });
    check(dd.has && dd.said === 0, 'an approved piece whose customer wrote and chose nothing: no box');

    // ── 6 · the Review tab's card is not the Engrave detail ──
    const rv = await page.evaluate(KEY => { const j = Engrave.jobOf(B.orders.byKey.get(KEY), null, 'back'), c = Engrave.placementCard(j, 1); return { order: c.querySelectorAll('.pvOrder').length, said: c.querySelectorAll('[data-said]').length, panel: c.querySelectorAll('.engp').length, text: c.innerText.replace(/\s+/g, ' ') }; }, KEY);
    check(rv.panel === 0 && rv.order === 1 && rv.said === 0 && /From the order/.test(rv.text) && /Buyer's note/.test(rv.text), 'the Review tab\'s card (no panel) keeps its "From the order" list and gets no second box: ' + JSON.stringify({ order: rv.order, said: rv.said }));

    // the page was never asked anything it must not be
    const net = await page.evaluate(() => __calls.slice());
    const reads = srv.st.calls.filter(c => c.body && (c.body.mode === 'engraveIntent' || c.body.op === 'agent' && /engrave/i.test(JSON.stringify(c.body).slice(0, 400))));
    check(!net.includes('notes.set') && reads.length === 0, 'no staff note written, no paid reading: calls ' + JSON.stringify(net));
    check(errors.length === 0, 'no page errors: ' + errors.join(' | ').slice(0, 300));
  } finally { await browser.close(); srv.close && srv.close(); }
  if (fails.length) { console.log('engrave-customer-note: ' + fails.length + ' failed'); process.exit(1); }
  console.log('engrave-customer-note: ok');
})().catch(e => { console.error(e); process.exit(1); });
