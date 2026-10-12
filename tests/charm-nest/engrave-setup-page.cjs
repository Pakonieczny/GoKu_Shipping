// ENGSETUP (front-back-1011): the engraving-setup editor in the REAL page (charm-nest-1.html, headless Chromium against the local fake site, offline: no Etsy, no paid reading, no live write).
// The pure parts are proved in engrave-setup.cjs; this proves the page's side of it:
//   1  order 4195966039 read by the real intake (Charm Type "Tag1 (front+back)", Font "Ace", "front - W / back - 1. 27. 24") is told by the control as Front + back, W, 1. 27. 24, Ace
//   2  Engrave.saveSetup stores row.engravingOverride on the LINE with the signed-in name, lineRecord keeps it and rowFromRecord brings it back, a fresh pull (Carry.adopt) keeps it
//   3  an undecided job takes the person's new words (no paid call), the control says it was edited and offers no Undo (point 8); saving the reading's own words is how a person gets them back
//   4  a DECIDED face is never deleted: mode "none" cuts it plain through the page's own skip (a "cut plain" seal by the person), the job and its seals stay
//   5  the control is mounted in the Engrave tab's card and in the order window's engraving card (the sheet window's is the same call; its own tests run it)
//   6  nothing asked Etsy or the reader: the fake site saw no engraveIntent call and no notes.set
//   SHOTS=<dir> node tests/charm-nest/engrave-setup-page.cjs [playwright-core dir]
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const SHIP = Math.floor(Date.UTC(2026, 9, 14, 17) / 1000), RID = '4195966039', TX = '5009123456789', KEY = `${RID}_${TX}`;
const ORDER = { receiptId: RID, orderNumber: RID, createTs: SHIP - 5 * 86400, updateTs: SHIP - 5 * 86400 + 60, shipBy: SHIP, buyer: { name: 'Test Buyer' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: TX, listingId: '4458930445', sku: 'MINI_0445', title: 'Mini Tag Initial Charm Add On Charm Personalized Letter Charm', quantity: 1, expectedShipDate: SHIP, metalKey: 'gold', metalLabel: 'GF 14/20',
    variations: [{ name: 'Charm Type', value: 'Tag1 (front+back)' }, { name: 'Font choice', value: 'Ace' }, { name: 'Metal Choice', value: 'Gold Filled' }], personalization: ['front - W\nback - 1. 27. 24'] }] };

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { try { ({ chromium } = require('/opt/node22/lib/node_modules/playwright/node_modules/playwright-core')); } catch (__) { console.log('engrave-setup-page: playwright-core not found, skipped'); process.exit(0); } }
  const shots = process.env.SHOTS || ''; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] });
  const POOLID = KEY + '_1';
  srv.st.put('Charm_Nest_Sheets', 'gold-su-1', { id: 'gold-su-1', metal: 'gold', charms: [{ id: 'g0', poolId: POOLID, order: RID }], placements: [{ id: 'g0', cxPt: 40, cyPt: 40, angle: 0 }], poolIds: [POOLID], orders: [RID], sheetIndex: 1, fileBase: 'gold-su-1', runId: 'run-test-1' });
  srv.st.put('Charm_Pool', POOLID, { poolId: POOLID, orderId: RID, sheetId: 'gold-su-1', state: 'placed', material: 'gold' });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: 1 });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.Engrave && window.OrderEngraving && window.CNEngravingSeals && window.CharmNestEngraveSetup && CN.S.cloud.ok === true, null, { timeout: 60000 });

    // ── the order, read by the real intake, and its back job as Engrave holds it ──
    const made = await page.evaluate(async ({ order, KEY }) => {
      const O = CharmNestOrders, line = order.lines[0];
      const row = { key: KEY, order, line, arrivedAt: Date.now() - 3600e3, spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [KEY + '_1'], engrave: null, material: null };
      B.orders.rows.push(row); B.orders.byKey.set(KEY, row);
      Orders.interpretAll(); await Orders.loadMaps(true); Orders.interpretAll();
      const sp = row.spec; sp.engraveCandidate = true;
      const j = Engrave.ensureJob(row); Object.assign(j, { state: 'review', text: '1. 27. 24', lines: ['1. 27. 24'], fit: { size: 10, capMm: 2, centre: [0, 0], angle: 0, weight: 'Regular' }, view: {}, verify: { geometry: { ok: true } }, source: 'personalization', confidence: 0.9 });
      row.engrave = { needed: true, state: 'review', approved: false, text: j.text };
      window.__calls = []; const call = DesignLink.call; DesignLink.call = function (name) { __calls.push(name); return call.apply(this, arguments); };
      Engrave.render = () => {};
      return { tagOption: sp.tagOption || null, font: sp.font || null, personalization: sp.personalization, hasEngraving: !!sp.engraving, state: row.state };
    }, { order: ORDER, KEY });
    check(made.tagOption && made.tagOption.mode === 'both', 'the intake read the Charm Type as both faces: ' + JSON.stringify(made.tagOption && { tag: made.tagOption.tag, mode: made.tagOption.mode }));
    check(made.hasEngraving, 'and spec.engraving is the intake\'s own reading (the editor uses it, not its fallback)');
    check(made.font && /ace/i.test(made.font.asked), 'and the font as Ace: ' + JSON.stringify(made.font && made.font.asked));

    // ── 1 · the control, in a slot, for the line of the job ──
    const mountIt = () => page.evaluate(KEY => { document.getElementById('__slot')?.remove(); const s = document.createElement('div'); s.id = '__slot'; s.style.cssText = 'position:fixed;left:20px;top:80px;width:420px;background:#fffefb;padding:10px;z-index:99999'; document.body.appendChild(s); CharmNestEngraveSetup.mount(s, Engrave.jobOf(B.orders.byKey.get(KEY)).row); }, KEY);
    const shown = () => page.evaluate(() => { const s = document.getElementById('__slot'); return { text: s.textContent.replace(/\s+/g, ' ').trim(), open: s.querySelector('.egSetup')?.dataset.open, hidden: s.hidden }; });
    await mountIt(); let t = (await shown()).text;
    check(/Engraves\s*Front \+ back/.test(t) && /Front W/.test(t) && /Back 1\. 27\. 24/.test(t) && /Ace/.test(t), 'order 4195966039 shows Front + back, W, 1. 27. 24, Ace: ' + t.slice(0, 160));
    check(/Charm Type/.test(t) && /Tag1 \(front\+back\)/.test(t), 'with the option that decided it (the intake\'s own why line): ' + t.slice(80, 330));
    if (shots) await page.screenshot({ path: path.join(shots, 'setup-view.png'), clip: { x: 0, y: 60, width: 460, height: 260 } });

    // ── 2 · Edit -> Back only -> Save through the page's own save path ──
    await page.click('#__slot [data-su=edit]'); await page.click('#__slot [data-su=mode][data-mode=back]');
    if (shots) await page.screenshot({ path: path.join(shots, 'setup-edit.png'), clip: { x: 0, y: 60, width: 460, height: 340 } });
    await page.click('#__slot [data-su=save]');
    await page.waitForFunction(KEY => !!B.orders.byKey.get(KEY).engravingOverride, KEY, { timeout: 10000 });
    const ov = await page.evaluate(KEY => { const r = B.orders.byKey.get(KEY); return r.engravingOverride; }, KEY);
    check(ov && ov.mode === 'back' && ov.by === 'Test Operator' && ov.at > 0, 'saved as row.engravingOverride by the signed-in login: ' + JSON.stringify(ov));
    t = (await shown()).text;
    check(/Set by Test Operator/.test(t) && /The system read: Front: W · Back: 1\. 27\. 24/.test(t) && !/Undo|back to the system/i.test(t), 'the control says it was edited and what the system read, and offers no Undo');
    const rec = await page.evaluate(KEY => { const row = B.orders.byKey.get(KEY), [k, r] = Orders.lineRecord(row), back = Orders.rowFromRecord(k, JSON.parse(JSON.stringify(r))); return { rec: r.engravingOverride, back: back.engravingOverride, key: k, plain: Orders.lineRecord(Object.assign({}, row, { engravingOverride: undefined }))[1] }; }, KEY);
    check(JSON.stringify(rec.rec) === JSON.stringify(ov) && JSON.stringify(rec.back) === JSON.stringify(ov), 'lineRecord keeps it and rowFromRecord brings it back');
    check(!('engravingOverride' in rec.plain), 'a line with no edit has no such key in its record (records are byte for byte as before)');
    const kept = await page.evaluate(KEY => { const row = B.orders.byKey.get(KEY); const fresh = { key: KEY, order: Object.assign({}, row.order, { updateTs: row.order.updateTs + 5 }), line: row.line, spec: null, problems: [], state: 'pulled', poolIds: [], engrave: null, metal: null };
      const was = B.orders.rows.slice(), run = B.run; B.run = { status: 'complete' }; Carry.capture(); B.run = run; const i = B.orders.rows.indexOf(row); B.orders.rows[i] = fresh; B.orders.byKey.set(KEY, fresh); Carry.adopt();
      const after = B.orders.byKey.get(KEY); const ok = !!after.engravingOverride; B.orders.rows[B.orders.rows.indexOf(after)] = row; B.orders.byKey.set(KEY, row); Engrave.relinkAll(); return ok; }, KEY);   // (the jobs of a line with a front are joined to the row again, as Carry.adopt does for the row it adopts: here the test puts the first row back)
    check(kept, 'a fresh pull of a changed order keeps the edit (Carry.adopt)');
    const jobAfter = await page.evaluate(KEY => { const j = Engrave.jobOf(B.orders.byKey.get(KEY)); return { state: j.state, lines: j.lines, face: j.face || 'back' }; }, KEY);
    check(jobAfter.state === 'review' && jobAfter.lines.join('/') === '1. 27. 24', 'the undecided back job is as it was: ' + JSON.stringify(jobAfter));

    // ── 3 · new words for the undecided back; no Undo; the reading's own words saved again ──
    await mountIt(); await page.click('#__slot [data-su=edit]');
    await page.fill('#__slot textarea[data-face=back]', '2. 3. 25'); await page.click('#__slot [data-su=save]');
    await page.waitForFunction(KEY => { const j = Engrave.jobOf(B.orders.byKey.get(KEY)); return j.lines.join('/') === '2. 3. 25'; }, KEY, { timeout: 10000 });
    const seeded = await page.evaluate(KEY => { const j = Engrave.jobOf(B.orders.byKey.get(KEY)); return { state: j.state, by: j.decision && j.decision.by, source: j.source, conf: j.confidence, ws: j.wordsSource }; }, KEY);
    check(seeded.by === 'Test Operator' && seeded.source === 'person' && seeded.conf === 1 && seeded.ws === 'spec.engraving' && ['ready', 'fitting', 'review'].includes(seeded.state), 'the undecided job took the new words as the person\'s own, no reading: ' + JSON.stringify(seeded));
    await mountIt(); t = (await shown()).text;
    check(!(await page.$('#__slot [data-su=undo]')) && !(await page.$('#__slot [data-su=undoYes]')) && !/Undo|back to the system/i.test(t) && !(await page.evaluate(() => [...document.querySelectorAll('#__slot button')].some(b => /undo|revert/i.test(b.textContent + ' ' + (b.title || ''))))), 'an edited line offers no Undo / revert control: ' + t.slice(0, 200));
    const again = await page.evaluate(async KEY => { const row = B.orders.byKey.get(KEY), r = await Engrave.saveSetup(row, { mode: 'both', front: { text: 'W' }, back: { text: '1. 27. 24' } }); return { ok: r.ok, mode: row.engravingOverride && row.engravingOverride.mode, hasUndo: typeof Engrave.undoSetup }; }, KEY);
    check(again.ok && again.mode === 'both' && again.hasUndo === 'undefined', 'saving the reading\'s own words is a plain save (the page has no undoSetup): ' + JSON.stringify(again));

    const back2 = await page.evaluate(KEY => Engrave.jobOf(B.orders.byKey.get(KEY), null, 'back').lines.join('/'), KEY);   // (the line has a front job again: the back job is asked for by its face)
    check(back2 === '1. 27. 24', 'and the undecided back has those words again: ' + back2);

    // ── 4 · a decided face is never deleted: mode none cuts it plain ──
    const dec = await page.evaluate(async KEY => {
      const row = B.orders.byKey.get(KEY), j = Engrave.jobOf(row, null, 'back'), at = Date.now() - 60000;
      Object.assign(j, { state: 'approved', approvedBy: 'Cy', approvedAt: at, backs: [] }); Object.assign(row.engrave, { state: 'approved', approved: true, approvedBy: 'Cy', approvedAt: at }); CNEngravingSeals.add(j, 'engraveApproved', 'Cy', at);
      const res = await Engrave.saveSetup(row, { mode: 'none' });
      const after = Engrave.jobOf(row, null, 'back');
      return { res: { ok: res.ok, plain: res.plain, none: res.none, removed: res.removed, message: res.message }, same: after === j, state: j.state, seals: CNEngravingSeals.list(j).map(s => s.how + '|' + s.by), kept: Engrave.items().has(j.key), eng: row.engrave && row.engrave.state };
    }, KEY);
    check(dec.res.ok && dec.res.plain.length === 1 && dec.res.removed.every(k => /~front$/.test(k)), 'mode none on an approved face: cut plain; only the front placement nobody had touched is removed: ' + JSON.stringify(dec.res));
    check(dec.same && dec.kept && dec.state === 'skipped' && dec.seals.includes('engraveApproved|Cy') && dec.seals.some(s => s.startsWith('engravePlain|Test Operator')), 'the job and its approval seal stay, a cut-plain seal by the signed-in person is added: ' + JSON.stringify(dec.seals));
    check(dec.eng === 'skipped', 'the line says skipped');

    // ── 5 · the control is in the Engrave tab's card and in the order window's engraving card ──
    const hosts = await page.evaluate(async ({ KEY, RID }) => {
      const row = B.orders.byKey.get(KEY), j = Engrave.jobOf(row, null, 'back');
      Object.assign(j, { state: 'review', approvedBy: null, approvedAt: null, decidedAt: null, decidedBy: null }); Object.assign(row.engrave, { state: 'review', approved: false, needed: true }); delete row.engravingOverride; CharmNestEngraveSetup.DRAFTS.clear();
      const card = Engrave.placementCard(j, 1), engTab = !!card.querySelector('[data-engsetup] .egSetup');
      const box = document.createElement('div'); document.body.appendChild(box); box.className = 'owEng'; OrderEngraving.mount(box, { rid: RID, key: KEY, compact: false });
      await new Promise(r => setTimeout(r, 400));
      return { engTab, tabText: card.querySelector('[data-engsetup]').textContent.replace(/\s+/g, ' ').trim(), ow: !!box.querySelector('.owEngSetup .egSetup'), owText: (box.querySelector('.owEngSetup') || { textContent: '' }).textContent.replace(/\s+/g, ' ').trim() };
    }, { KEY, RID }).catch(e => ({ err: String(e.message || e) }));
    check(hosts.engTab, 'the Engrave tab\'s card carries the control: ' + (hosts.tabText || hosts.err || '').slice(0, 120));
    check(hosts.ow, 'the order window\'s engraving card carries it: ' + (hosts.owText || hosts.err || '').slice(0, 120));

    // ── 5b · and in the sheet window's engraving card ──
    const sw = await page.evaluate(async ({ POOLID }) => {
      try {
        await SheetWin.open('gold-su-1', { select: POOLID });
        const until = async (f, ms) => { const t0 = Date.now(); while (Date.now() - t0 < ms) { const v = f(); if (v) return v; await new Promise(r => setTimeout(r, 100)); } return null; };
        const slot = await until(() => document.querySelector('[data-r2=eng] .swEngSetup .egSetup'), 15000);
        const host = document.querySelector('[data-r2=eng]');
        return { has: !!slot, text: slot ? slot.textContent.replace(/\s+/g, ' ').trim() : '', card: !!(host && host.querySelector('.swEng')), shape: slot ? [...document.querySelectorAll('[data-r2=eng] .swEngSetup')].length : 0 };
      } catch (e) { return { err: String(e && e.message || e) }; }
    }, { POOLID });
    if (shots) await page.screenshot({ path: path.join(shots, 'sheetwin.png') });
    check(sw.has && sw.shape === 1, 'the sheet window\'s engraving card carries it (once): ' + (sw.text || sw.err || JSON.stringify(sw)).slice(0, 120));

    // ── 6 · no Etsy, no reader ──
    const net = await page.evaluate(() => __calls.slice());
    const reads = srv.st.calls.filter(c => c.body && (c.body.mode === 'engraveIntent' || c.body.op === 'agent' && /engrave/i.test(JSON.stringify(c.body).slice(0, 400))));
    check(!net.includes('notes.set') && reads.length === 0, 'no staff note, no paid reading: calls ' + JSON.stringify(net));
    check(errors.length === 0, 'no page errors: ' + errors.join(' | ').slice(0, 300));
  } finally { await browser.close(); srv.close && srv.close(); }
  if (fails.length) { console.error('engrave-setup-page: FAILED\n - ' + fails.join('\n - ')); process.exit(1); }
  console.log('engrave-setup-page: ok');
})().catch(e => { console.error(e); process.exit(1); });
