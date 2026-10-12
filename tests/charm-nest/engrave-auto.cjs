// ENGAUTO (engrave-ui-1012, round 2, Paul's point 5): "All of these options default state is to be dynamically auto-selected intelligently by the system based on the rules of the system,
// order details, inclusive of customer instructions and choices made by the customer."
// CharmNestEngraveSetup.autoChoice(row, slot) is the system's own choice (mode, the words and font of each face, position 'auto', the rule that decided, needsPerson), ignoring any person's override.
//   1  (node, pure) many order lines read by the REAL intake (charm-nest-orders.js interpretLine): back-only note, front-only option, "front - X / back - Y", Tag options, an unlabelled "both",
//      a zodiac earring (no engraving, ever), counted discs, font words, no instructions, 2 sides on a design with its own art: autoChoice says what the rules say
//   2  autoChoice ignores a person's saved override, a person's words at a card (job.decision) and a person's "no engraving"; diffFromAuto names what a person's choice changed
//   3  (page, real charm-nest-1.html in headless Chromium over the fake site: no Etsy, no paid reading, no live write) the card and the panel OPEN on the system's choice when nobody saved anything:
//      the mode segment, the words and the font of each face; an earring has neither; a saved override wins after Save while autoChoice stays the system's; a decided face is locked with one line
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/engrave-auto.cjs
'use strict';
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js'));
const Setup = require(path.join(root, 'charm-nest-engrave-setup.js'));

let passed = 0, failed = 0;
const check = (ok, msg) => { if (ok) passed++; else failed++; console.log((ok ? '  ok  ' : '  FAIL ') + msg); };
const t = async (name, fn) => { try { await fn(); passed++; console.log('  ok  ' + name); } catch (e) { failed++; console.log('  FAIL ' + name + '\n      ' + String(e && e.stack || e).split('\n').slice(0, 8).join('\n      ')); } };
const V = (name, value) => ({ name, value });
const J = x => (x === undefined ? undefined : JSON.parse(JSON.stringify(x)));

/* ── the order lines: names are made up, the words are test data ── */
const ENT = {
  'HOCKEY 1- STICKS': { sku: 'HOCKEY 1- STICKS', members: 3, holes: 1, backKeepOut: [], widthPt: 30, heightPt: 34 },
  'MINI_0445': { sku: 'MINI_0445', members: 2, holes: 1, backKeepOut: [], widthPt: 30, heightPt: 14 },
  'INITIAL_DISC_4571': { sku: 'INITIAL_DISC_4571', members: 2, holes: 1, backKeepOut: [], widthPt: 36.5, heightPt: 36.5 },
  'HUGGIE HOOPS-ZODIAC': { sku: 'HUGGIE HOOPS-ZODIAC', members: 3, holes: 1, backKeepOut: [], widthPt: 23, heightPt: 28 },
  'BIRTHFLOWER_NECKLACE_DISC-9': { sku: 'BIRTHFLOWER_NECKLACE_DISC-9', members: 2, holes: 0, backKeepOut: [], widthPt: 48.354, heightPt: 56.287 },
};
const entryFor = s => ENT[String(s || '').toUpperCase()] || null;
const ctx = () => ({ optionMaps: {}, aliases: {}, noDesign: {}, listingSkus: {}, masterEntry: entryFor, masterLoose: () => '' });
const HOCKEY = { sku: 'HOCKEY 1- STICKS', title: 'Hockey stick necklace, personalised number charm', variations: [V('Metal Choice', 'Gold Filled')] };
const MINI = { sku: 'MINI_0445', listingId: '4458930445', title: 'Mini Tag Initial Charm Add On Charm Personalized Letter Charm' };
const TAGV = (type, font) => [V('Metal Choice', 'Gold Filled'), V('Charm Type', type)].concat(font === null ? [] : [V('Font Choice', font || 'Ace')]);
const HUGGIE = { sku: 'HUGGIE HOOPS-ZODIAC', title: 'Huggie Hoop Earrings with Astrological Charm, Handcrafted in Gold Vermeil, Solid 14k Gold', variations: [V('Metal Choice', 'Gold Filled')] };
const DISC = n => ({ sku: 'INITIAL_DISC_4571', listingId: '888', title: 'Initial Disc Necklace', variations: [V('Metal Choice', 'Gold'), V('Discs', n + ' Disc'), V('Engraving', 'Front&Back')] });
const FLOWER = { sku: 'BIRTHFLOWER_NECKLACE_DISC-9', listingId: '870782383', title: 'Birth Flower Necklace', variations: [V('Metal Choice', 'Gold'), V('Length/Engrave:1 side/2 sides?', 'Gold - 16" - 2 sides')] };
/** the line as the intake reads it: a row the way the page holds it ({ key, order, line, spec, state }) */
const rowOf = (line, order) => {
  const o = Object.assign({ receiptId: '4199999999', updateTs: 0, buyerMessage: '', staffNote: '', messages: [] }, order || {});
  const l = Object.assign({ transactionId: 't1', listingId: '1', sku: '', title: '', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: [], variations: [] }, line);
  return { key: o.receiptId + '_' + l.transactionId, order: o, line: l, spec: O.interpretLine(o, l, ctx()), state: 'pooled' };
};
const faceOut = f => (f ? [f.text, f.fontId, f.fontWord] : null);

/* what the rules say for each line: [name, line, order, expected { mode, front, back, source, needsPerson }] (front/back = [text, fontId, fontWord] or null; provisional back = words the reader will give) */
const SS = 'source-sans-3', ACE = 'jost-ace';
const CASES = [
  ['a number and nothing else on a hockey stick: the back, as every line always was', Object.assign({}, HOCKEY, { personalization: ['21'] }), null, { mode: 'back', front: null, back: ['21', SS, null], source: 'default', needsPerson: false, prov: true }],
  ['the same number with a note that is no face word', Object.assign({}, HOCKEY, { personalization: ['21'] }), { buyerMessage: 'rr' }, { mode: 'back', front: null, back: ['21', SS, null], source: 'default', needsPerson: false, prov: true }],
  ['two words in one box, nothing says which face: the back (as before), no invented front', Object.assign({}, HOCKEY, { personalization: ['rr 21'] }), null, { mode: 'back', front: null, back: ['rr 21', SS, null], source: 'default', needsPerson: false, prov: true }],
  ['a back-only note ("on the back only")', Object.assign({}, HOCKEY, { personalization: ['21'] }), { buyerMessage: 'Please engrave on the back only' }, { mode: 'back', front: null, back: ['21', SS, null], source: 'note', needsPerson: false, prov: true }],
  ['a back label in the personalisation box', Object.assign({}, HOCKEY, { personalization: ['back: 21'] }), null, { mode: 'back', front: null, back: ['21', SS, null], source: 'note', needsPerson: false, prov: true }],
  ['a front-only option, the number', Object.assign({}, HOCKEY, { personalization: ['21'], variations: [V('Metal Choice', 'Gold Filled'), V('Engraving', 'Front')] }), null, { mode: 'front', front: ['21', SS, null], back: null, source: 'option', needsPerson: false }],
  ['front - X / back - Y typed by the buyer, no option', Object.assign({}, HOCKEY, { personalization: ['front - rr\nback - 21'] }), null, { mode: 'both', front: ['rr', SS, null], back: ['21', SS, null], source: 'note', needsPerson: false }],
  ['"X on the front and Y on the back"', Object.assign({}, HOCKEY, { personalization: ['rr on the front and 21 on the back'] }), null, { mode: 'both', front: ['rr', SS, null], back: ['21', SS, null], source: 'note', needsPerson: false }],
  ['the Tag1 (front+back) option, labelled words, font Ace', Object.assign({}, MINI, { variations: TAGV('Tag1 (front+back)'), personalization: ['front - W\nback - 1. 27. 24'] }), null, { mode: 'both', front: ['W', ACE, null], back: ['1. 27. 24', ACE, null], source: 'option', needsPerson: false }],
  ['the Tag2 (front only) option', Object.assign({}, MINI, { variations: TAGV('Tag2 (front engrave)'), personalization: ['W'] }), null, { mode: 'front', front: ['W', ACE, null], back: null, source: 'option', needsPerson: false }],
  ['the Tag3 (back only) option', Object.assign({}, MINI, { variations: TAGV('Tag3 (back engraving)'), personalization: ['1. 27. 24'] }), null, { mode: 'back', front: null, back: ['1. 27. 24', ACE, null], source: 'option', needsPerson: false, prov: true }],
  ['Tag option for both faces, the words not labelled: the system asks, it does not guess', Object.assign({}, MINI, { variations: TAGV('Tag4 (front + back)'), personalization: ['rr\n21'] }), null, { mode: 'both', front: ['', ACE, null], back: ['', ACE, null], source: 'option', needsPerson: true }],
  ['a face with its own font word ("Front: m - Vibur"), the line font Ace for the other', Object.assign({}, MINI, { variations: TAGV('Tag1 (front+back)'), personalization: ['Front: m - Vibur\nBack: 1. 27. 24'] }), null, { mode: 'both', front: ['m', null, 'Vibur'], back: ['1. 27. 24', ACE, null], source: 'option', needsPerson: false }],
  ['a font named in the personalisation box', Object.assign({}, HOCKEY, { personalization: ['21\nfont: Ace'] }), null, { mode: 'back', front: null, back: ['21', ACE, null], source: 'default', needsPerson: false, prov: true }],
  ['the buyer chose blank (no engraving) but wrote words: none, and a person is asked', Object.assign({}, HOCKEY, { personalization: ['21'], variations: [V('Metal Choice', 'Gold Filled'), V('Engraving', 'Blank')] }), null, { mode: 'none', front: null, back: null, source: 'option', needsPerson: true }],
  ['no instructions at all: nothing to engrave', Object.assign({}, HOCKEY, { personalization: [] }), null, { mode: 'none', front: null, back: null, source: 'default', needsPerson: false }],
  ['a message that asks for rr on the FRONT while the box says 21: a person decides, the number is not silently put on the back', Object.assign({}, HOCKEY, { personalization: ['21'] }), { buyerMessage: 'Please put rr on the front' }, { mode: 'both', front: ['', SS, null], back: ['', SS, null], source: 'note', needsPerson: true }],
  ['a message about a front desk and being back at work is no instruction', Object.assign({}, HOCKEY, { personalization: ['Name One'] }), { buyerMessage: 'Can you send it to the front desk? Thanks, back at work Monday.' }, { mode: 'back', front: null, back: ['Name One', SS, null], source: 'default', needsPerson: false, prov: true }],
  ['a message that asks for the back stays the default back', Object.assign({}, HOCKEY, { personalization: ['21'] }), { buyerMessage: 'Please engrave my number on the back' }, { mode: 'back', front: null, back: ['21', SS, null], source: 'default', needsPerson: false, prov: true }],
  ['a zodiac earring with the sign typed in the box: never engraved', Object.assign({}, HUGGIE, { personalization: ['Capricorn'] }), null, { mode: 'none', front: null, back: null, source: 'rule', needsPerson: false, never: true }],
  ['a zodiac earring whose option says front + back: still never engraved', Object.assign({}, HUGGIE, { variations: [V('Metal Choice', 'Gold Filled'), V('Engraving', 'Front&Back')], personalization: ['front: A\nback: B'] }), null, { mode: 'none', front: null, back: null, source: 'rule', needsPerson: false, never: true }],
  ['2 sides on a design with its own art (a birth flower): the words go on the back', Object.assign({}, FLOWER, { personalization: ['Name One'] }), null, { mode: 'back', front: null, back: ['Name One', SS, null], source: 'sides', needsPerson: false }],
];

(async () => {
  console.log('ENGAUTO · the system\'s own choice');
  /* ═══ 1 · many lines ═══ */
  for (const [name, line, order, want] of CASES) {
    await t('1 ' + name, () => {
      const row = rowOf(line, order), a = Setup.autoChoice(row, null, { jobs: [] });
      assert.ok(a, 'a choice');
      assert.equal(a.mode, want.mode, 'mode ' + a.mode + ' (' + a.why + ')');
      assert.equal(a.source, want.source, 'source ' + a.source);
      assert.equal(a.needsPerson, want.needsPerson, 'needsPerson');
      assert.equal(a.position, 'auto');
      assert.equal(!!a.never, !!want.never, 'never');
      assert.deepEqual(a.faces, want.mode === 'both' ? ['front', 'back'] : want.mode === 'front' ? ['front'] : want.mode === 'back' ? ['back'] : [], 'faces');
      for (const f of ['front', 'back']) {
        const w = want[f]; if (!w) { assert.equal(a[f], null, f + ' is not engraved'); continue; }
        assert.ok(a[f], f + ' present'); assert.equal(a[f].text, w[0], f + ' words');
        if (w[1] !== null) assert.equal(a[f].fontId, w[1], f + ' font id ' + a[f].fontId);
        assert.equal(a[f].fontWord, w[2], f + ' own font word');
        assert.ok(a[f].fontId, f + ' always names the installed font it is engraved in');
        assert.equal(!!a[f].provisional, !!want.prov && f === 'back', f + ' provisional (the reader has not read it yet)');
      }
    });
  }
  await t('1x counted discs: each disc has its own words from the buyer\'s labels; the line-level choice is the same mode', () => {
    const row = rowOf(DISC(3), null); row.spec = O.interpretLine({ receiptId: '4199999999', updateTs: 0 }, Object.assign({ transactionId: 't1', quantity: 1, metalKey: 'gold', metalLabel: 'Gold' }, DISC(3), { personalization: ['Disc 1: front A back B\nDisc 2: front C back D\nDisc 3: front E back F'] }), ctx());
    for (const [slot, f, b] of [['D1', 'A', 'B'], ['D2', 'C', 'D'], ['D3', 'E', 'F']]) { const a = Setup.autoChoice(row, slot, { jobs: [] }); assert.deepEqual([a.mode, a.slot, a.front && a.front.text, a.back && a.back.text], ['both', slot, f, b], slot); }
    assert.equal(Setup.autoChoice(row, 'Q9', { jobs: [] }).slot, null, 'a slot that is no piece is the line');
  });
  await t('1y the Mini Tag option read BEFORE the intake wrote spec.engraving: the tag rule decides (source "tag")', () => {
    const row = rowOf(Object.assign({}, MINI, { variations: TAGV('Tag1 (front+back)'), personalization: ['front - W\nback - 1. 27. 24'] }), null);
    const spec = Object.assign({}, row.spec); delete spec.engraving; spec.tagOption = { name: 'Charm Type', value: 'Tag1 (front+back)', tag: 1, mode: 'both' };
    const a = Setup.autoChoice(Object.assign({}, row, { spec }), null, { jobs: [] });
    assert.deepEqual([a.mode, a.source, a.front && a.front.text, a.back && a.back.text], ['both', 'tag', 'W', '1. 27. 24']);
  });

  /* ═══ 2 · a person's word is never the system's ═══ */
  const base = () => rowOf(Object.assign({}, HOCKEY, { personalization: ['front - rr\nback - 21'] }), null);
  await t('2a a saved override changes what the control opens on, never autoChoice', () => {
    const row = base(), before = J(Setup.autoChoice(row, null, { jobs: [] }));
    row.engravingOverride = { mode: 'front', front: { text: 'Name Two', fontWord: 'Pristina' }, by: 'Name One', at: 1 };
    assert.deepEqual(J(Setup.autoChoice(row, null, { jobs: [] })), before, 'the system\'s choice is unchanged');
    const eff = Setup.setupFor(row);
    assert.deepEqual([eff.mode, eff.front.text, eff.front.fontWord, eff.edited], ['front', 'Name Two', 'Pristina', true], 'the effective setup is the person\'s');
  });
  await t('2b a person\'s words at a card (job.decision, source person) and a person\'s "no engraving" are not the reader\'s', () => {
    const row = rowOf(Object.assign({}, HOCKEY, { personalization: ['21'] }), null), key = row.key;
    const mk = x => Object.assign({ key, row, state: 'ready', lines: ['21'], text: '21', source: 'personalization', decision: null }, x);
    assert.equal(Setup.autoChoice(row, null, { jobs: [mk({})] }).back.from, 'reader', 'the reader\'s job words are the system\'s');
    const edited = Setup.autoChoice(row, null, { jobs: [mk({ text: 'Name Two', lines: ['Name Two'], source: 'person', decision: { by: 'Name One', note: 'edited' } })] });
    assert.deepEqual([edited.back.text, edited.back.from, edited.back.provisional], ['21', 'personalization', true], 'a person\'s edit is ignored: the buyer\'s own words stand');
    const decided = Setup.autoChoice(row, null, { jobs: [mk({ text: 'Name Two', lines: ['Name Two'], decision: { by: 'Name One', at: 1, text: 'Name Two' } })] });
    assert.equal(decided.back.text, '21', 'a decision at the card is ignored too');
    const none = Setup.autoChoice(row, null, { jobs: [mk({ state: 'none', text: null, lines: [], decision: { by: 'Name One', none: true } })] });
    assert.equal(none.mode, 'back', 'a person\'s "no engraving" is not the system\'s choice');
    const readerNone = Setup.autoChoice(row, null, { jobs: [mk({ state: 'none', text: null, lines: [], reason: 'No engraving requested' })] });
    assert.deepEqual([readerNone.mode, readerNone.source], ['none', 'reader'], 'but the reader\'s own "no engraving" is');
  });
  await t('2c diffFromAuto: what a person\'s choice changed, fonts by the installed font', () => {
    const row = base(), d = (now) => Setup.diffFromAuto(row, null, now, { jobs: [] }).parts.join(',');
    assert.equal(d({ mode: 'both', front: { text: 'rr' }, back: { text: '21' } }), '', 'the same choice');
    assert.equal(d({ mode: 'both', front: { text: ' rr ' }, back: { text: '21', fontWord: null } }), '', 'spaces and an unnamed font are the same');
    assert.equal(d({ mode: 'back', back: { text: '21' } }), 'mode', 'only the mode');
    assert.equal(d({ mode: 'both', front: { text: 'rx' }, back: { text: '21', fontWord: 'Ace' } }), 'front.text,back.font', 'the front words and the back font');
    assert.equal(d({ mode: 'front', front: { text: 'rr' }, back: { text: 'ignored' } }), 'mode', 'a face the mode does not engrave is not compared');
    assert.equal(d({ mode: 'both', front: { text: 'rr' }, back: { text: '21', font: 'Source Sans 3' } }), '', 'a font word that resolves to the line\'s font is no change');
    const ear = rowOf(Object.assign({}, HUGGIE, { personalization: ['Capricorn'] }), null);
    assert.equal(Setup.diffFromAuto(ear, null, { mode: 'back', back: { text: 'Capricorn' } }, { jobs: [] }).parts.join(','), 'mode,back.text', 'an earring: any engraving is a change against "none"');
  });
  await t('2d a row with no spec, or nothing to say, gives null; the shape holds no nested arrays', () => {
    assert.equal(Setup.autoChoice(null), null); assert.equal(Setup.autoChoice({ key: 'x' }), null);
    const a = Setup.autoChoice(base(), null, { jobs: [] }); const noNested = require('./_noNestedArrays.cjs'); noNested(J(a), 'autoChoice');
  });

  /* ═══ 3 · the real page: card and panel OPEN on it ═══ */
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { try { ({ chromium } = require('/opt/node22/lib/node_modules/playwright/node_modules/playwright-core')); } catch (__) { chromium = null; } }
  if (!chromium) console.log('engrave-auto: playwright-core not found, the page checks were skipped');
  else await pageChecks(chromium);

  console.log(`\nengrave-auto: ${passed} checks passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });

async function pageChecks(chromium) {
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1280, height: 800 }, deviceScaleFactor: 1 });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Engrave && window.CharmNestEngraveSetup && window.CharmNestEngravePanel && window.CharmNestEngraveCard && CN.S.cloud.ok === true, null, { timeout: 60000 });

    // the page cases: the same lines, read by the page's own intake (the fake site's reader stands in for the paid one: it hands back the personalisation box)
    const by = re => CASES.find(c => re.test(c[0]));
    const PAGE = [['number', /^a number and nothing/], ['front option', /^a front-only option/], ['labelled note', /^front - X/], ['tag both', /^the Tag1/], ['tag unlabelled', /^Tag option for both/], ['font words', /^a face with its own font/],
      ['back note', /^a back-only note/], ['message asks front', /^a message that asks for rr on the FRONT/], ['no words', /^no instructions/], ['earring', /^a zodiac earring with the sign/]].map(([n, re]) => [n, by(re)[1], by(re)[2]]);
    const SHIP = Math.floor(Date.UTC(2026, 9, 14, 17) / 1000);
    const opened = await page.evaluate(async ({ PAGE, SHIP }) => {
      const out = [], rows = [];
      PAGE.forEach(([name, line, extra], i) => {
        const rid = '41999999' + String(10 + i), order = Object.assign({ receiptId: rid, orderNumber: rid, createTs: SHIP - 5 * 86400, updateTs: SHIP - 5 * 86400 + 60, shipBy: SHIP, buyer: { name: 'Name One' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: [] }, extra || {});
        order.lines = [Object.assign({ transactionId: rid + '1', listingId: line.listingId || '1' + rid, quantity: 1, expectedShipDate: SHIP, metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [] }, line)];
        const key = CharmNestOrders.lineKey(order, order.lines[0]), row = { key, order, line: order.lines[0], arrivedAt: Date.now(), spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [key + '_1'], engrave: null, material: null };
        B.orders.rows.push(row); B.orders.byKey.set(key, row); rows.push([name, row]);
      });
      Orders.interpretAll(); await Orders.loadMaps(true); Orders.interpretAll();
      for (const [name, row] of rows) { try { Engrave.ensureJobs(row); await Engrave.classify(row); } catch (e) { out.push({ name, err: String(e.message) }); continue; } }
      const S = CharmNestEngraveSetup, C = CharmNestEngraveCard, P = CharmNestEngravePanel;
      for (const [name, row] of rows) {
        const auto = JSON.parse(JSON.stringify(S.autoChoice(row))), r = { name, key: row.key, auto, card: null, panel: null };
        const pieces = C.piecesOf([row], {}); r.card = pieces.map(p => ({ never: p.never, faces: ['front', 'back'].map(f => p.faces[f] ? [f, p.faces[f].text, p.faces[f].fontId, !!p.faces[f].plain] : null).filter(Boolean) }));
        out.push(r);
      }
      return out;
    }, { PAGE, SHIP });
    // the per-face panel reading needs the panel's own setFace: done one case at a time below
    for (const r of opened) {
      if (r.err) { check(false, 'page case ' + r.name + ': ' + r.err); continue; }
      const a = r.auto;
      const shown = await page.evaluate(async ({ key }) => {
        const row = B.orders.byKey.get(key), S = CharmNestEngraveSetup, P = CharmNestEngravePanel, a = S.autoChoice(row), out = { mode: null, faces: {}, mounted: false };
        P.DRAFTS.clear();
        const slot = document.createElement('div'); document.body.appendChild(slot);
        const first = a.faces[0] || 'back', h = P.mount(slot, row, { job: Engrave.jobOf(row, null, first), face: first });
        const p = slot.querySelector('.engp'); out.mounted = !!p;
        if (p) {
          out.mode = (slot.querySelector('.engpMode:not(.engpFaceSw) .engpSeg[aria-checked=true]') || {}).dataset?.mode || null;
          for (const f of a.faces) {
            h.setFace(f);
            const ta = slot.querySelector('.engpWords'), sel = slot.querySelector('.engpFont');
            out.faces[f] = { text: ta ? ta.value : null, fontId: sel ? S.fontIdOf(row, f, sel.value) : null, face: slot.querySelector('.engp').dataset.face, mode: slot.querySelector('.engp').dataset.mode };
          }
        }
        h.destroy && h.destroy(); slot.remove(); P.DRAFTS.clear(); return out;
      }, { key: r.key });
      if (a.never || !a.faces.length && (a.source === 'default' || a.never)) {
        if (a.never) check(!shown.mounted && r.card.length === 0, `${r.name}: an earring has no panel and no card: ${JSON.stringify({ panel: shown.mounted, cards: r.card.length })}`);
        else check(r.card.length === 0, `${r.name}: nothing to engrave, no card: ${JSON.stringify({ cards: r.card.length, panel: shown.mounted, mode: shown.mode })}`);
        if (!a.never && shown.mounted) check(shown.mode === 'none', `${r.name}: if the panel is drawn it opens on None: ${shown.mode}`);
        continue;
      }
      check(shown.mounted && shown.mode === a.mode, `${r.name}: the panel opens on the mode the system chose (${a.mode}): ${shown.mode}`);
      for (const f of a.faces) {
        const want = a[f], got = shown.faces[f];
        check(got && got.text === want.text, `${r.name}: the ${f} words open as the system's: "${got && got.text}" vs "${want.text}"`);
        check(got && got.fontId === want.fontId, `${r.name}: the ${f} font opens as the system's (${want.fontId}): ${got && got.fontId}`);
      }
      // the card: exactly the faces the system engraves, with its words; a face it does not engrave is no slot with words
      const piece = r.card[0];
      check(!!piece && piece.faces.filter(x => !x[3]).map(x => x[0]).join() === a.faces.join(), `${r.name}: the card shows the faces the system chose (${a.faces.join(' + ')}): ${piece ? piece.faces.map(x => x[0] + (x[3] ? '(plain)' : '')).join(' + ') : 'no card'}`);
      if (piece) for (const f of a.faces) { const fc = piece.faces.find(x => x[0] === f); check(!!fc && fc[1] === a[f].text, `${r.name}: the card's ${f} words are the system's: "${fc && fc[1]}" vs "${a[f].text}"`); }
    }

    // a staged edit that is cancelled puts the panel back on the system's choice; one that is only left behind (the card closed) is the panel's own draft: said, not failed (the panel's owner decides)
    const dr = await page.evaluate(async () => {
      const row = B.orders.rows.find(r => r.line.personalization && r.line.personalization[0] === '21' && r.line.sku === 'HOCKEY 1- STICKS' && !r.order.buyerMessage), P = CharmNestEngravePanel, S = CharmNestEngraveSetup, a = S.autoChoice(row), out = {};
      const open = () => { const slot = document.createElement('div'); document.body.appendChild(slot); const h = P.mount(slot, row, { job: Engrave.jobOf(row, null, 'back'), face: 'back' }); return { slot, h }; };
      const look = slot => { const p = slot.querySelector('.engp'); return { mode: p.dataset.mode, text: (slot.querySelector('.engpWords') || {}).value, front: (P.DRAFTS.get(row.key) || { faces: {} }).faces.front && P.DRAFTS.get(row.key).faces.front.text }; };
      P.DRAFTS.clear(); let { slot, h } = open(); out.first = look(slot);
      slot.querySelector('[data-mode=both]').click(); h.setFace('front'); const ta = slot.querySelector('.engpWords'); ta.value = 'rr'; ta.dispatchEvent(new Event('input', { bubbles: true })); out.staged = look(slot);
      slot.querySelector('[data-engp=cancel]').click(); out.cancelled = look(slot); h.destroy(); slot.remove();
      P.DRAFTS.clear(); ({ slot, h } = open()); slot.querySelector('[data-mode=both]').click(); h.setFace('front'); const tb = slot.querySelector('.engpWords'); tb.value = 'rr'; tb.dispatchEvent(new Event('input', { bubbles: true })); h.destroy(); slot.remove();
      ({ slot, h } = open()); out.reopened = look(slot); out.auto = { mode: a.mode, back: a.back.text }; h.destroy(); slot.remove(); P.DRAFTS.clear(); return out;
    });
    check(dr.first.mode === dr.auto.mode && dr.first.text === dr.auto.back && dr.staged.mode === 'both' && dr.cancelled.mode === dr.auto.mode && dr.cancelled.text === dr.auto.back, 'Cancel puts the panel back on the system\'s choice: ' + JSON.stringify({ staged: dr.staged, cancelled: dr.cancelled }));
    console.log('  note  an edit that is left unsaved and the panel closed comes back on the next open (the panel\'s module-level DRAFTS): ' + JSON.stringify(dr.reopened) + ' (system: ' + JSON.stringify(dr.auto) + ')');

    // a saved override wins after Save, autoChoice stays the system's, the card and the panel follow the person
    const ov = await page.evaluate(async () => {
      const row = B.orders.rows.find(r => r.line.personalization && r.line.personalization[0] === 'front - rr\nback - 21'), S = CharmNestEngraveSetup, P = CharmNestEngravePanel, C = CharmNestEngraveCard;
      if (!row) return { missing: true };
      const before = JSON.parse(JSON.stringify(S.autoChoice(row)));
      const res = await Engrave.saveSetup(row, { mode: 'front', front: { text: 'Name Two', fontWord: 'Pristina' } });
      const after = JSON.parse(JSON.stringify(S.autoChoice(row)));
      P.DRAFTS.clear(); const slot = document.createElement('div'); document.body.appendChild(slot);
      const h = P.mount(slot, row, { job: Engrave.jobOf(row, null, 'front'), face: 'front' });
      const sel = slot.querySelector('.engpFont'), out = { ok: !!(res && res.ok), same: JSON.stringify(before) === JSON.stringify(after), mode: (slot.querySelector('.engpSeg[aria-checked=true]') || {}).dataset?.mode, text: (slot.querySelector('.engpWords') || {}).value, font: sel ? S.fontIdOf(row, 'front', sel.value) : null, pristina: S.fontIdOf(row, 'front', 'Pristina') };
      const pc = C.piecesOf([row], {}); out.card = pc.map(p => ['front', 'back'].map(f => p.faces[f] && !p.faces[f].plain ? f + ':' + p.faces[f].text : null).filter(Boolean).join(' + '));
      out.override = row.engravingOverride && row.engravingOverride.mode;
      h.destroy && h.destroy(); slot.remove(); P.DRAFTS.clear(); return out;
    });
    check(!ov.missing && ov.ok && ov.same, 'a saved override leaves autoChoice exactly as it was: ' + JSON.stringify({ ok: ov.ok, same: ov.same }));
    check(ov.mode === 'front' && ov.text === 'Name Two' && ov.font && ov.font === ov.pristina, 'after Save the panel opens on the person\'s choice (front, Name Two, Pristina): ' + JSON.stringify({ mode: ov.mode, text: ov.text, font: ov.font }));
    check(ov.card && ov.card.length === 1 && ov.card[0] === 'front:Name Two', 'and the card shows only the Front with the person\'s words: ' + JSON.stringify(ov.card));

    // a decided face is locked with ONE short line and disabled inputs (the order here is NOT approved in Paul's image: its grey button is the dimmed Approve)
    const lock = await page.evaluate(() => {
      const row = B.orders.rows.find(r => r.line.personalization && r.line.personalization[0] === '21' && r.line.sku === 'HOCKEY 1- STICKS'), P = CharmNestEngravePanel, job = Engrave.jobOf(row, null, 'back'), was = { state: job.state, by: job.approvedBy };
      Object.assign(job, { state: 'approved', approvedBy: 'Name One' });
      P.DRAFTS.clear(); const slot = document.createElement('div'); document.body.appendChild(slot); P.mount(slot, row, { job, face: 'back' });
      const out = { words: slot.querySelector('.engpWords').disabled, font: slot.querySelector('.engpFont').disabled, pos: [...slot.querySelectorAll('.engpPb')].every(b => b.disabled), save: slot.querySelector('[data-engp=save]').disabled, locks: [...slot.querySelectorAll('.engpLock')].map(x => x.textContent), emoji: !!slot.querySelector('[data-emoji]') };
      Object.assign(job, { state: was.state, approvedBy: was.by }); slot.remove(); P.DRAFTS.clear(); return out;
    });
    check(lock.words && lock.font && lock.pos && lock.save && lock.locks.join('|') === 'Approved by Name One' && !lock.emoji, 'an approved face: words, font, position and Save are off, the Emoji button is gone, ONE line says "Approved by Name One": ' + JSON.stringify(lock));
    check(errors.length === 0, 'no page error: ' + errors.slice(0, 2).join(' | '));
  } finally { await browser.close(); await srv.close(); }
}
