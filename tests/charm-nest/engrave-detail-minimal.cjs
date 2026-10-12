// ENGTRIM (engrave-ui-1012): the Engrave tab's detail (placementCard) is minimal. Paul, 12 Oct 2026: the engraving UI is "full of needless text ... way too many needless text and titles and stuff. keep it very
// simple and minimalist". The options panel (charm-nest-engrave-panel.js) already edits the words, the font, the position and the mode of each face, and the card's Front / Back thumbnails pick the face, so the detail
// must not draw a second route to the same words, a second face switch, a reference picture of the other face, a "From the order" list, a "NN% sure" pill or the block titles. What a person has to decide or fix stays.
// Real charm-nest-1.html in headless Chromium over the fake loopback site (bridge-server.cjs): no Etsy, no paid reading, no live write. Names are made up.
//   1  a settled read (97%, personalisation box, no staff note), the detail at 1280x720: ABSENT: the "Words on the ..." box and its "Use these words", the "Front/Back . reference" picture (when the card's thumbnails are on
//      the page), the "From the order" list, the "NN% sure" pill, every block title, the head's Front | Back switch; PRESENT: the panel (one words box, font, Save, Cancel, ONE Emoji button), Approve, "No engraving", the big picture, Open order
//   2  an unsettled read (45%, or words still to decide) keeps ONE quiet box of the buyer's own words (Personalization / Buyer's note / Staff note, no title, no "Reading", no "Font", no "Decided by"); a staff note keeps it too
//   3  words still to decide (state words): ONE small "Engrave these words" button (the panel cannot confirm a read as it stands), the reason said once in the empty picture, one "No engraving" button
//   4  the Review tab's placement card (no panel there) is untouched: its words box and its pill are still drawn
//   5  a decided row's opened detail does not repeat the words or who decided (the row and its seals say it), and keeps the size, the file and the pieces
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/engrave-detail-minimal.cjs
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const SHIP = Math.floor(Date.UTC(2026, 9, 14, 17) / 1000), RID = '4195966044', TX = '5009123456790', KEY = `${RID}_${TX}`;
const ORDER = { receiptId: RID, orderNumber: RID, createTs: SHIP - 5 * 86400, updateTs: SHIP - 5 * 86400 + 60, shipBy: SHIP, buyer: { name: 'Name One' }, buyerMessage: 'Please put the date', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: TX, listingId: '4458930445', sku: 'MINI_0445', title: 'Mini Tag Initial Charm Add On Charm Personalized Letter Charm', quantity: 1, expectedShipDate: SHIP, metalKey: 'gold', metalLabel: 'GF 14/20',
    variations: [{ name: 'Charm Type', value: 'Tag1 (front+back)' }, { name: 'Font choice', value: 'Ace' }, { name: 'Metal Choice', value: 'Gold Filled' }], personalization: ['front - W\nback - 1. 27. 24'] }] };

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { try { ({ chromium } = require('/opt/node22/lib/node_modules/playwright/node_modules/playwright-core')); } catch (__) { console.log('engrave-detail-minimal: playwright-core not found, skipped'); process.exit(0); } }
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
    await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Engrave && window.CharmNestEngravePanel && CN.S.cloud.ok === true, null, { timeout: 60000 });

    // ── the order (Front + back, "front - W / back - 1. 27. 24") and its back job, placed and settled ──
    await page.evaluate(async ({ order, KEY }) => {
      const line = order.lines[0];
      const row = { key: KEY, order, line, arrivedAt: Date.now() - 3600e3, spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [KEY + '_1'], engrave: null, material: null };
      B.orders.rows.push(row); B.orders.byKey.set(KEY, row);
      Orders.interpretAll(); await Orders.loadMaps(true); Orders.interpretAll();
      row.spec.engraveCandidate = true;
      const j = Engrave.ensureJob(row); Object.assign(j, { state: 'review', text: '1. 27. 24', lines: ['1. 27. 24'], fit: { size: 10, capMm: 2, centre: [0, 0], angle: 0, weight: 'Regular' }, view: {}, verify: { geometry: { ok: true } }, source: 'personalization', confidence: 0.97 });
      row.engrave = { needed: true, state: 'review', approved: false, text: j.text };
      window.__calls = []; const call = DesignLink.call; DesignLink.call = function (name) { __calls.push(name); return call.apply(this, arguments); };
    }, { order: ORDER, KEY });
    const hasCard = await page.evaluate(() => !!(window.CharmNestEngraveCard && CharmNestEngraveCard.mountPiece));

    // open the Engrave detail on the piece's back job after `patch` is applied to the job (and `spec` to its line), and read what the detail draws, in one go (nothing else runs in between)
    const detail = (patch, spec) => page.evaluate(async ({ KEY, patch, spec }) => {
      const row = B.orders.byKey.get(KEY), j = Engrave.jobOf(row, null, 'back'), was = {}, wasSpec = {};
      for (const k of Object.keys(patch || {})) { was[k] = j[k]; j[k] = patch[k]; }
      for (const k of Object.keys(spec || {})) { wasSpec[k] = row.spec[k]; row.spec[k] = spec[k]; }
      setMode('engrave'); Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: j.key }); Engrave.render();
      const c = document.querySelector('#egQueue .rvItem[data-kind=placement]');
      const q = s => c ? c.querySelectorAll(s).length : -1, txt = c ? c.innerText.replace(/\s+/g, ' ') : '';
      const out = { has: !!c, text: txt, words: q('.pvWords'), oldBox: q('textarea[data-f=words]'), use: [...(c ? c.querySelectorAll('[data-a=usewords]') : [])].map(b => b.textContent.trim()), order: q('.pvOrder'), orderText: c && c.querySelector('.pvOrder') ? c.querySelector('.pvOrder').textContent.replace(/\s+/g, ' ') : '',
        orderTitle: q('.pvOrder .pvH'), conf: q('.rh .conf'), ref: q('.pvRef'), mainTitle: q('.pvMain > .pvH'), checkTitle: q('.pvCheck .pvH'), faceSw: q('.rh .egFaces'), headSw: q('.rh .egEarSwitch'), textareas: q('textarea:not(.owCompLine textarea):not(.cmLine textarea)'),   // (the team-message boxes are the order chat's, not the engraving's)
        panel: q('.engp'), seg: [...(c ? c.querySelectorAll('.engpMode:not(.engpFaceSw) .engpSeg') : [])].map(b => b.textContent.trim()).join('|'), panelWords: q('.engp .engpWords'), font: q('.engp .engpFont'), save: q('.engp [data-engp=save]'), cancel: q('.engp [data-engp=cancel]'),
        emoji: q('[data-emoji]'), approve: q('[data-a=approve]'), skip: q('[data-a=skip]'), skipText: c && c.querySelector('[data-a=skip]') ? c.querySelector('[data-a=skip]').textContent.replace(/\s+/g, ' ').trim() : '', backHost: q('.backHost'), openOrder: q('[data-open-order]'), noBack: [...(c ? c.querySelectorAll('.noBack') : [])].map(n => n.textContent.trim()), why: q('.pvSide .why'),
        thumbs: q('.engFace .engThumb'), faceSwitchOfPanel: q('.engpFaceSw'), engCard: q('.engCard'), titles: [...(c ? c.querySelectorAll('h4.pvH') : [])].map(h => h.textContent.trim()) };
      out.restore = () => {};
      for (const k of Object.keys(was)) j[k] = was[k]; for (const k of Object.keys(wasSpec)) row.spec[k] = wasSpec[k];
      return JSON.parse(JSON.stringify(out));
    }, { KEY, patch, spec });

    // ── 1 · a settled read ──
    let d = await detail({}, {});
    check(d.has && d.panel === 1, 'the Engrave detail is open and carries the options panel');
    check(d.words === 0 && d.oldBox === 0 && d.use.length === 0 && d.textareas === 1 && d.panelWords === 1, 'no second words box: no "Words on the ..." box, no "Use these words"; the panel\'s box is the one text area of the detail: ' + JSON.stringify({ words: d.words, oldBox: d.oldBox, use: d.use, textareas: d.textareas }));
    check(!/Words on the|Use these words|Engrave these words/i.test(d.text), 'and none of those words are on the detail');
    check(d.order === 0 && !/From the order|Reading|Decided by|Personalization|Buyer's note|Staff note/.test(d.text), 'no "From the order" list (a settled read, no staff note): ' + d.order);
    check(d.conf === 0 && !/\d+\s*%\s*sure|\bsure\b/i.test(d.text), 'no "NN% sure" pill, no percentage');
    check(d.checkTitle === 0 && d.orderTitle === 0 && !/To check with the buyer/.test(d.text), 'no "To check with the buyer" / "From the order" titles');
    check(d.faceSw === 0, 'the head draws no Front | Back switch (the panel picks the face: ' + (d.thumbs ? 'the card\'s thumbnails' : 'its own switch ' + d.faceSwitchOfPanel) + ')');
    if (hasCard) {
      check(d.ref === 0 && !/reference/i.test(d.text), 'no "Front . reference" picture: the card\'s Front and Back thumbnails (' + d.thumbs + ') are the pictures of the piece');
      check(d.thumbs === 2 && d.engCard === 1 && d.mainTitle === 0, 'the card is on the page with its two thumbnails and the big picture has no title of its own: ' + JSON.stringify({ thumbs: d.thumbs, cards: d.engCard, mainTitle: d.mainTitle }));
    } else console.log('  (no CharmNestEngraveCard in this build: the reference picture and the big picture\'s title are still drawn until it lands — checks skipped; titles now: ' + JSON.stringify(d.titles) + ')');
    check(d.seg === 'None|Front|Back|Front + back' && d.font === 1 && d.save === 1 && d.cancel === 1, 'the panel keeps the choices: ' + d.seg + ', one font drop-down, Save and Cancel');
    check(d.emoji === 1, 'ONE Emoji button in the detail (the panel\'s; the old box\'s is gone with the box): ' + d.emoji);
    check(d.approve === 1 && d.skip === 1 && /^No back engraving/.test(d.skipText) && d.backHost === 1 && d.openOrder === 1, 'Approve, the one "No engraving" button, the big picture and Open order stay: ' + JSON.stringify({ approve: d.approve, skip: d.skip, skipText: d.skipText, backHost: d.backHost, openOrder: d.openOrder }));
    if (shots) await page.screenshot({ path: path.join(shots, 'detail-settled.png') });

    // ── 2 · an unsettled read keeps ONE quiet box of the buyer's own words; a staff note keeps it too ──
    d = await detail({ confidence: 0.45 }, {});
    check(d.order === 1 && d.orderTitle === 0 && /Personalization/.test(d.orderText) && /front - W/.test(d.orderText) && /Buyer's note/.test(d.orderText) && /Please put the date/.test(d.orderText), 'a 45% read: one quiet box of the buyer\'s own words, no title: "' + d.orderText + '"');
    check(!/Reading|Font|Decided by/.test(d.orderText) && d.conf === 0 && !/%/.test(d.text), 'it carries no "Reading", no "Font", no "Decided by" and no percentage');
    d = await detail({ source: 'buyerMessage' }, {});
    check(d.order === 1, 'a read taken from the buyer\'s message (not the personalisation box) keeps the box as well');
    d = await detail({}, { staffNote: 'Name Two asked to change the date' });
    check(d.order === 1 && /Staff note/.test(d.orderText) && /change the date/.test(d.orderText), 'a staff note keeps the box even on a settled read: "' + d.orderText + '"');
    d = await detail({ confidence: 0.45, decision: { by: 'Name One', at: Date.now(), text: '1. 27. 24', note: 'confirmed' } }, {});
    check(d.order === 0, 'a read a person has decided on is settled: no box');

    // ── 3 · words still to decide ──
    d = await detail({ state: 'words', reason: 'Say what to engrave on the back', fit: null, view: null }, {});
    check(d.use.length === 1 && d.use[0] === 'Engrave these words' && d.words === 0 && d.oldBox === 0 && d.textareas === 1, 'words still to decide: ONE small "Engrave these words" button and still no second words box: ' + JSON.stringify({ use: d.use, words: d.words, textareas: d.textareas }));
    check(d.noBack.length === 1 && d.noBack[0] === 'Say what to engrave on the back' && d.why === 0, 'the reason is said once, in the empty picture, not again under a box: ' + JSON.stringify({ noBack: d.noBack, why: d.why }));
    check(d.skip === 1 && d.order === 1, 'one "No engraving" button (it used to be drawn twice), and the buyer\'s own words in the one quiet box');
    d = await detail({ state: 'classify', fit: null, view: null }, {});
    check(d.noBack.length === 1 && /Processing stopped before these words were read\.$/.test(d.noBack[0]) && !/below/.test(d.noBack[0]), 'a stopped read says so without pointing at a box "below": "' + d.noBack[0] + '"');
    d = await detail({ state: 'fitting', fit: null, view: null }, {});
    check(d.use.length === 0 && d.order === 0, 'a placement being prepared offers no confirm button and no box');

    // ── 4 · the Review tab's card is untouched ──
    const rv = await page.evaluate(KEY => { const j = Engrave.jobOf(B.orders.byKey.get(KEY), null, 'back'), c = Engrave.placementCard(j, 1); return { words: c.querySelectorAll('.pvWords').length, box: c.querySelectorAll('textarea[data-f=words]').length, use: [...c.querySelectorAll('[data-a=usewords]')].map(b => b.textContent.trim()), conf: c.querySelectorAll('.rh .conf').length, panel: c.querySelectorAll('.engp').length, order: c.querySelectorAll('.pvOrder').length }; }, KEY);
    check(rv.words === 1 && rv.box === 1 && rv.use.join() === 'Use these words' && rv.conf === 1 && rv.panel === 0 && rv.order === 1, 'the Review tab\'s placement card (no panel there) still has its words box, "Use these words", its pill and "From the order": ' + JSON.stringify(rv));

    // ── 5 · a decided row's opened detail ──
    const dd = await page.evaluate(async KEY => {
      const row = B.orders.byKey.get(KEY), j = Engrave.jobOf(row, null, 'back'), was = { state: j.state, by: j.approvedBy, at: j.approvedAt, decision: j.decision };
      Object.assign(j, { state: 'approved', approvedBy: 'Name One', approvedAt: Date.now() - 60e3, decision: { by: 'Name One', at: Date.now() - 60e3, text: j.text } });
      setMode('engrave'); Engrave.restoreView({ tab: 'done', list: false, chosen: true, openDone: j.key }); Engrave.render();
      const r = document.querySelector('#egDone .doneRow.open'), det = r && r.querySelector('.doneDetail');
      const out = { open: !!det, dts: det ? [...det.querySelectorAll('dt')].map(x => x.textContent.trim()) : [], rowWords: r && r.querySelector('.w') ? r.querySelector('.w').textContent.trim() : '', seals: r ? r.querySelectorAll('.sealRow, .engravingSeals').length : 0 };
      Object.assign(j, was); Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: j.key }); Engrave.render();
      return out;
    }, KEY);
    check(dd.open && dd.rowWords === '1. 27. 24' && dd.seals >= 1, 'the decided row says the words and carries the seals: ' + JSON.stringify({ rowWords: dd.rowWords, seals: dd.seals }));
    check(!dd.dts.includes('Words') && !dd.dts.includes('Decided') && dd.dts.includes('Size'), 'its opened detail does not say the words or the decision again, and keeps the size: ' + dd.dts.join(', '));

    // the page was never asked anything it must not be
    const net = await page.evaluate(() => __calls.slice());
    const reads = srv.st.calls.filter(c => c.body && (c.body.mode === 'engraveIntent' || c.body.op === 'agent' && /engrave/i.test(JSON.stringify(c.body).slice(0, 400))));
    check(!net.includes('notes.set') && reads.length === 0, 'no staff note written, no paid reading: calls ' + JSON.stringify(net));
    check(errors.length === 0, 'no page errors: ' + errors.join(' | ').slice(0, 300));
  } finally { await browser.close(); srv.close && srv.close(); }
  if (fails.length) { console.log('engrave-detail-minimal: ' + fails.length + ' failed'); process.exit(1); }
  console.log('engrave-detail-minimal: ok');
})().catch(e => { console.error(e); process.exit(1); });
