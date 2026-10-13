// The Settings row that adds the ZODIAC SIGN drop-down to the Huggie Hoop Etsy listing (charm-nest-zodiac-etsy.js, ZODWRITE 12 Oct 2026) in a real browser.
// The page here is a bare stand-in (the row's own element, the page's class names, a stubbed repo lock that records what it was asked and can say "cancelled"); the cloud
// behind window.api is the REAL server module (netlify/functions/_charmNestEtsyZodiac.js) on the in-memory Firestore and a FAKE Etsy (tests/charm-nest/zodiac-etsy-write.cjs).
// No network, no real token, no password. It proves:
//   · the row is DORMANT: loading it makes no call at all; typing a listing makes at most the free status read; nothing reaches Etsy until Check the listing is pressed;
//   · Check the listing: the repo lock is asked ("modify"), 2 Etsy calls, never a write; the plain lines (6 -> 72 products, 12 signs, 7 in the repo / 5 new, listings_w);
//   · Add the drop-down only shows after a good check, stays dark until the exact words are typed, asks the lock again, sends ONE PUT, and says what Etsy holds;
//   · a cancelled password sends nothing; a 409 says Etsy has not enabled a third drop-down and offers no retry; a lost answer offers Check what Etsy holds;
//   · text from the server is drawn as text (a title with markup is not turned into elements); the row fits a phone width.
//   PW_DIR=<dir with playwright-core> CHROMIUM=<chrome> node tests/charm-nest/zodiac-etsy-ui.cjs      (skipped without a browser)
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || [path.join(root, 'node_modules'), '/opt/node22/lib/node_modules/playwright/node_modules'].find(d => fs.existsSync(path.join(d, 'playwright-core'))) || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (e) { console.log('skipped: no playwright-core (' + pwDir + ')'); process.exit(0); }
const exe = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
if (!fs.existsSync(exe)) { console.log('skipped: no browser at ' + exe); process.exit(0); }
const T = require('./zodiac-etsy-write.cjs');
const { world, LID, PHRASE, store } = T;
let n = 0; const ok = (c, m) => { assert(c, m); n++; console.log('  ok  ' + m); };

const PAGE = `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><style>
:root{--line:#d9d2c3;--card:#fff;--ink:#222;--ink70:#555;--gold2:#b8902f;--sage:#3b6b4a;--clay:#8a3a26;--mono:monospace}
body{margin:0;padding:12px;font:13px system-ui;background:#faf8f3}.field{display:grid;gap:4px}.help{font-size:11px;color:#777}
.btn{font:12px system-ui;border:1px solid var(--line);border-radius:6px;padding:4px 8px;background:#fff;cursor:pointer}.btn[disabled]{opacity:.5;cursor:default}.btn.gold{background:#e9d9a8}.spin{display:inline-block;width:8px;height:8px;border:2px solid #999;border-radius:50%}
</style></head><body><div class="field" id="stZodiacEtsy" style="grid-column:1 / -1"></div></body></html>`;

(async () => {
  const browser = await chromium.launch({ executablePath: exe, args: ['--no-sandbox'] });
  async function open(W, o) {
    o = o || {};
    const ctx = await browser.newContext({ viewport: { width: o.width || 900, height: 900 }, locale: 'en-US' });
    const page = await ctx.newPage(), log = [], asks = [];
    await page.exposeFunction('__server', async b => {
      log.push({ op: b.op, token: !!b.repoToken, confirm: b.confirm, hash: b.payloadHash });
      const c = { listingId: b.listingId, by: b.by };
      switch (b.op) {
        case 'zodiacListingPrepare': return W.mod.prepare(c);
        case 'zodiacListingWrite': return W.mod.write(Object.assign(c, { confirm: b.confirm, payloadHash: b.payloadHash }));
        case 'zodiacListingReconcile': return W.mod.reconcile(c);
        case 'zodiacListingStatus': return W.mod.status(c);
        default: throw new Error('unexpected op ' + b.op);
      }
    });
    await page.exposeFunction('__asked', a => { asks.push(a); return !!o.cancel; });
    await page.setContent(PAGE);
    await page.evaluate(() => {
      window.employeeName = () => 'Paul';
      window.CharmNestRepoLock = { mark() {}, unlocked: () => true, cancelled: e => !!e && e.cancelled === true, call: async (action, ask, fn) => { const cancel = await window.__asked({ action, target: ask && ask.target }); if (cancel) throw Object.assign(new Error('Not done: the charm repo password was not given'), { cancelled: true }); return fn('stub-token'); } };
      window.api = async (fnName, body) => { const r = await window.__server(body); return r; };
    });
    if (o.B) await page.evaluate(b => { window.B = b; }, o.B);   // (the sorter's own data, as the page holds it, before the row mounts)
    await page.addScriptTag({ path: path.join(root, 'charm-nest-zodiac-etsy.js') });
    return { ctx, page, log, asks };
  }
  const text = async (page, sel) => (await page.locator(sel).first().innerText()).replace(/\s+/g, ' ').trim();
  const row = '#stZodiacEtsy';

  /* 1 · dormant, then a full good run */
  { const W = world(), { ctx, page, log, asks } = await open(W);
    ok(await page.locator(row + ' label').count() === 1 && /ZODIAC SIGN drop-down/.test(await text(page, row + ' label')), 'the row is drawn with its label');
    ok(log.length === 0 && asks.length === 0 && W.S.calls.length === 0, 'DORMANT: loading the row made no cloud call, asked for no password, touched no Etsy');
    const check = page.getByRole('button', { name: 'Check the listing' });
    ok(await check.isDisabled(), 'Check the listing is dark with no listing typed');
    await page.fill('#zeListing', 'not a listing'); ok(await check.isDisabled(), 'and for text that is not a listing link or number');
    await page.fill('#zeListing', 'https://www.etsy.com/listing/' + LID + '/huggie-hoop-earrings?ref=x'); ok(await check.isEnabled(), 'a pasted Etsy link lights it');
    ok(log.length === 0 && W.S.calls.length === 0, 'typing alone calls nothing');
    await page.locator('#zeListing').blur(); await page.waitForTimeout(150);
    ok(log.every(l => l.op === 'zodiacListingStatus' && !l.token) && W.S.calls.length === 0, 'leaving the box makes at most the free status read (no password, no Etsy call): ' + JSON.stringify(log.map(l => l.op)));
    ok(await page.locator(row + ' button', { hasText: 'Add the drop-down' }).count() === 1 && !(await page.locator(row + ' button', { hasText: 'Add the drop-down' }).isVisible()), 'Add the drop-down is not shown before a check');

    await check.click(); await page.waitForFunction(() => /Checked: nothing has been written/.test(document.querySelector('#stZodiacEtsy .zeHead').textContent), null, { timeout: 5000 });
    ok(asks.length === 1 && asks[0].action === 'modify', 'Check asked the repo lock for "modify"');
    ok(log.filter(l => l.op === 'zodiacListingPrepare').length === 1 && log.filter(l => l.op === 'zodiacListingPrepare')[0].token === true, 'one prepare call, with the lock\'s token');
    ok(W.S.calls.length === 2 && W.S.puts.length === 0, 'two Etsy calls (scopes, one inventory read) and no write: ' + W.S.calls.map(c => c.kind).join(','));
    const body = await text(page, row);
    ok(/6 products/.test(body) && /72 products/.test(body) && /12 zodiac signs/.test(body), 'the plain lines say 6 products now, 72 after, 12 signs: ' + body.slice(0, 400));
    ok(/7 match charms already in the repo/.test(body) && /5 are new SKUs/.test(body) && /Aquarius/.test(body), 'seven SKUs match the repo, five are new');
    ok(/listings_w\): yes/.test(body) && /changes nothing about the price/.test(body), 'the permission and the price promise are stated');
    ok(/type exactly: ADD ZODIAC DROPDOWN ' + LID/.test(body.replace(/'/g, "'")) || body.includes('type exactly: ADD ZODIAC DROPDOWN ' + LID), 'it says the exact words to type');
    const add = page.getByRole('button', { name: 'Add the drop-down' });
    ok(await add.isVisible() && await add.isDisabled(), 'Add the drop-down appears after a good check and is dark');
    await page.fill('input[aria-label="the words that confirm the change"]', 'add zodiac dropdown ' + LID); ok(await add.isDisabled(), 'the words in lower case do not light it');
    await page.fill('input[aria-label="the words that confirm the change"]', 'ADD ZODIAC DROPDOWN 1718000002'); ok(await add.isDisabled(), 'another listing\'s words do not light it');
    await page.fill('input[aria-label="the words that confirm the change"]', PHRASE); ok(await add.isEnabled(), 'the exact words light it');
    ok(W.S.puts.length === 0, 'still nothing written to Etsy');
    await add.click(); await page.waitForFunction(() => /Written and checked/.test(document.querySelector('#stZodiacEtsy .zeHead').textContent), null, { timeout: 8000 });
    ok(asks.length === 2 && asks[1].action === 'modify', 'Add asked the repo lock again');
    const w = log.find(l => l.op === 'zodiacListingWrite'); ok(w && w.token && w.confirm === PHRASE && /^[0-9a-f]{64}$/.test(w.hash), 'the write carried the token, the typed words and the checked hash');
    ok(W.S.puts.length === 1 && W.S.puts[0].query.max_variations_supported === 3 && W.S.stored.products.length === 72, 'ONE PUT with max_variations_supported=3; Etsy holds 72 products');
    ok(!(await page.getByRole('button', { name: 'Add the drop-down' }).isVisible()) && !(await page.getByRole('button', { name: 'Check the listing' }).isVisible()), 'after it is written neither button is offered again');
    await ctx.close(); }

  /* 2 · a cancelled password sends nothing */
  { const W = world(), { ctx, page, log, asks } = await open(W, { cancel: true });
    await page.fill('#zeListing', LID); await page.getByRole('button', { name: 'Check the listing' }).click(); await page.waitForTimeout(200);
    ok(asks.length === 1 && log.filter(l => l.op !== 'zodiacListingStatus').length === 0 && W.S.calls.length === 0, 'a cancelled password: no call to the cloud, none to Etsy');
    ok(/password was not given/.test(await text(page, row + ' .zeHead')), 'and it says so'); await ctx.close(); }

  /* 3 · Etsy has not enabled a third drop-down: plain words, no retry */
  { const W = world({ sim: { thirdEnabled: false } }), { ctx, page } = await open(W);
    await page.fill('#zeListing', LID); await page.getByRole('button', { name: 'Check the listing' }).click(); await page.getByRole('button', { name: 'Add the drop-down' }).waitFor();
    await page.fill('input[aria-label="the words that confirm the change"]', PHRASE); await page.getByRole('button', { name: 'Add the drop-down' }).click();
    await page.waitForFunction(() => /third drop-down/.test(document.querySelector('#stZodiacEtsy .zeHead').textContent), null, { timeout: 8000 });
    ok(/has not enabled a third drop-down for this shop/.test(await text(page, row + ' .zeHead')) && /Nothing changed/.test(await text(page, row + ' .zeHead')), '409: "Etsy has not enabled a third drop-down for this shop\'s key. Nothing changed"');
    ok(W.S.puts.length === 1 && W.S.stored.products.length === 6, 'one PUT, listing unchanged');
    ok(!(await page.getByRole('button', { name: 'Add the drop-down' }).isVisible()), 'no retry button (a refused check needs a new check)'); await ctx.close(); }

  /* 4 · the answer is lost: Check what Etsy holds settles it */
  { const W = world({ sim: { applyThenHang: true } }), { ctx, page, asks } = await open(W);
    await page.fill('#zeListing', LID); await page.getByRole('button', { name: 'Check the listing' }).click(); await page.getByRole('button', { name: 'Add the drop-down' }).waitFor();
    await page.fill('input[aria-label="the words that confirm the change"]', PHRASE); await page.getByRole('button', { name: 'Add the drop-down' }).click();
    const settle = page.getByRole('button', { name: 'Check what Etsy holds' }); await settle.waitFor({ state: 'visible', timeout: 8000 });
    await page.waitForFunction(() => /may or may not have landed/.test(document.querySelector('#stZodiacEtsy .zeHead').textContent), null, { timeout: 12000 });   // (the row reads the record every 4 s while a write runs)
    ok(/may or may not have landed/.test(await text(page, row + ' .zeHead')), 'a lost answer says the change may or may not have landed');
    const calls0 = W.S.calls.length; await settle.click(); await page.waitForFunction(() => /Written/.test(document.querySelector('#stZodiacEtsy .zeHead').textContent), null, { timeout: 8000 });
    ok(W.S.calls.length === calls0 + 1 && W.S.puts.length === 1, 'ONE read settled it; no second PUT'); ok(asks.length === 3, 'each press asked the lock'); await ctx.close(); }

  /* 5 · a problem listing is shown as problems, with no write step; markup in the server's text stays text */
  { store.clear(); const W = world({ sim: { scopes: ['listings_r'] } }); store.set('EtsyMail_Listings/' + LID, { state: 'active', title: '<img src=x onerror="window.__pwned=1"> Huggie Hoop' });
    const { ctx, page } = await open(W);
    await page.fill('#zeListing', LID); await page.getByRole('button', { name: 'Check the listing' }).click(); await page.waitForFunction(() => /found a problem/.test(document.querySelector('#stZodiacEtsy .zeHead').textContent), null, { timeout: 5000 });
    ok(/listings_w/.test(await text(page, row + ' ul.bad')), 'a connection without listings_w is listed as a problem');
    ok(!(await page.getByRole('button', { name: 'Add the drop-down' }).isVisible()), 'and there is no write step');
    ok(await page.locator(row + ' img').count() === 0 && !(await page.evaluate(() => window.__pwned)), 'markup in the listing title is drawn as text, not as an element');
    ok(W.S.puts.length === 0, 'nothing was written'); await ctx.close(); }

  /* 6 · phone width */
  { const W = world(), { ctx, page } = await open(W, { width: 390 });
    await page.fill('#zeListing', LID); await page.getByRole('button', { name: 'Check the listing' }).click(); await page.getByRole('button', { name: 'Add the drop-down' }).waitFor();
    const over = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
    ok(over <= 1, 'the row fits a 390 px wide screen without sideways scrolling (' + over + ' px over)'); await ctx.close(); }

  /* 7 · ZODGO: the listing is worked out from the sorter's own data (orders, SKU tables, saved links): in memory only, no call at all */
  const TITLE = 'Huggie Hoop Earrings with Astrological Charm, Handcrafted in Gold Vermeil, Silver, Solid 14k Gold';
  const rowOf = (rid, lid, sku, title) => ({ order: { receiptId: rid }, line: { listingId: lid, sku, title: title == null ? '' : title }, state: 'pooled' });
  const mkB = (rows, tables, aliases) => ({ orders: { rows }, maps: { listingSkus: tables || {}, aliases: aliases || {} } });
  { // the finder itself (pure: it reads the data it is given and nothing else)
    const g = globalThis; delete g.CharmNestZodiacEtsy; require(path.join(root, 'charm-nest-zodiac-etsy.js')); const F = g.CharmNestZodiacEtsy.findListings;
    const eq = (a, b, m) => { assert.deepStrictEqual(JSON.parse(JSON.stringify(a)), b, m); n++; console.log('  ok  ' + m); };
    eq(F(undefined), [], 'no data: nobody named'); eq(F({}), [], 'an empty page: nobody named'); eq(F(mkB([])), [], 'no orders: nobody named');
    const one = F(mkB([rowOf('1', '1718000001', 'HUGGIE HOOPS-ZODIAC', TITLE), rowOf('2', '1718000001', ' huggie hoops-zodiac ', ''), rowOf('2', '1718000001', 'HUGGIE HOOPS-ZODIAC', TITLE)]));
    eq(one, [{ id: '1718000001', title: TITLE, orders: 2, lines: 3, table: false, link: false }], 'three lines of two orders with the SKU (any case, spaces trimmed): one listing, its title, 2 orders');
    eq(F(mkB([rowOf('1', '1718000001', 'HUGGIE HOOPS- ZODIAC', TITLE), rowOf('2', '1718000001', 'HUGGIE HOOPS- ZODIAC AVATAR', TITLE), rowOf('3', '1718000001', 'ZODIAC ICONS LIBRA (HUGGIE)', TITLE), rowOf('4', '1744372161', 'Huggie Hoops-Tennis Ball/Racket3', 'Tennis')])), [], 'the other SKUs (the master spellings, the Libra SKU, another huggie listing) are not this listing');
    eq(F(mkB([rowOf('1', 'abc', 'HUGGIE HOOPS-ZODIAC', TITLE), rowOf('2', '', 'HUGGIE HOOPS-ZODIAC', TITLE), rowOf('3', null, 'HUGGIE HOOPS-ZODIAC', TITLE), rowOf('4', '12', 'HUGGIE HOOPS-ZODIAC', TITLE)])), [], 'a line without a real listing number names nobody');
    eq(F(mkB([null, {}, { line: null }, { line: { sku: 5 } }, { line: { listingId: {}, sku: 'HUGGIE HOOPS-ZODIAC' } }, 7, 'x'], { '1718000001': null, '5': 'x' }, { '1': 5, '2': { bySku: 'x' } })), [], 'broken rows and tables are not evidence and do not throw');
    eq(F(mkB([], { '1718000001': { at: 1, n: 6, uni: 'HUGGIE HOOPS-ZODIAC' } })), [{ id: '1718000001', title: '', orders: 0, lines: 0, table: true, link: false }], 'a SKU table whose products all carry the SKU names the listing');
    eq(F(mkB([], { '1718000001': { at: 1, n: 0, gone: true, uni: 'HUGGIE HOOPS-ZODIAC' }, '1718000002': { at: 1, n: 72, products: [{ id: '1', sku: 'ZODIAC VIRGO (HUGGIE)', d: 0, pv: [] }, { id: '2', sku: 'ZODIAC ICONS LEO (HUGGIE)', d: 0, pv: [] }], names: {}, props: {} } })), [], 'a listing Etsy no longer has, or one already carrying twelve SKUs, is not named');
    eq(F(mkB([], {}, { '1718000001': { title: TITLE, bySku: { 'HUGGIE HOOPS-ZODIAC': { sku: 'X' } } }, '1718000002': { title: 'Other', pairBySku: { 'SOMETHING ELSE': {} } } })), [{ id: '1718000001', title: TITLE, orders: 0, lines: 0, table: false, link: true }], 'a saved link on that SKU names the listing and its title');
    const two = F(mkB([rowOf('1', '1718000001', 'HUGGIE HOOPS-ZODIAC', TITLE), rowOf('2', '1718000002', 'HUGGIE HOOPS-ZODIAC', 'Another zodiac hoop'), rowOf('3', '1718000002', 'HUGGIE HOOPS-ZODIAC', 'Another zodiac hoop')], { '1718000001': { at: 1, n: 6, uni: 'HUGGIE HOOPS-ZODIAC' } }));
    eq(two.map(c => [c.id, c.orders, c.table]), [['1718000002', 2, false], ['1718000001', 1, true]], 'two listings: the one with more orders first; the table evidence joins the orders'); }

  { // exactly one listing: filled in and named; nothing is called until a press
    const W = world(), B = mkB([rowOf('9001', LID, 'HUGGIE HOOPS-ZODIAC', TITLE), rowOf('9002', LID, 'HUGGIE HOOPS-ZODIAC', TITLE), rowOf('9003', '1744372161', 'Huggie Hoops-Tennis Ball/Racket3', 'Tennis')]);
    const { ctx, page, log, asks } = await open(W, { B });
    ok(await page.inputValue('#zeListing') === LID, 'ONE listing carries the SKU in the page\'s orders: its number is filled into the box');
    const said = await text(page, row);
    ok(said.includes('Found it from the orders this page holds: listing ' + LID) && said.includes('Huggie Hoop Earrings with Astrological Charm') && /2 order lines in this sorter carry the SKU HUGGIE HOOPS-ZODIAC/.test(said), 'and it says so in plain words, with the title and the number: ' + said.slice(0, 260));
    ok(/Press Check the listing/.test(await text(page, row + ' .zeHead')), 'the row says what to press');
    ok(log.length === 0 && asks.length === 0 && W.S.calls.length === 0, 'filling it in made no cloud call at all, asked for no password, touched no Etsy');
    ok(await page.getByRole('button', { name: 'Check the listing' }).isEnabled(), 'Check the listing is lit');
    ok(!(await page.getByRole('button', { name: 'Add the drop-down' }).isVisible()), 'Add the drop-down is still not shown before a check');
    // the typed go-ahead words are NOT removed by this piece: the check still ends in the exact words to type
    await page.getByRole('button', { name: 'Check the listing' }).click(); await page.getByRole('button', { name: 'Add the drop-down' }).waitFor();
    ok(asks.length === 1 && W.S.calls.length === 2 && W.S.puts.length === 0, 'Check from the filled-in number: one password ask, two Etsy calls, no write');
    ok((await text(page, row)).includes('type exactly: ADD ZODIAC DROPDOWN ' + LID) && await page.getByRole('button', { name: 'Add the drop-down' }).isDisabled(), 'the exact words are still asked for and the write button stays dark until they are typed');
    // a rescan never overwrites what the person did
    await page.fill('#zeListing', '1718000009'); await page.evaluate(() => window.CharmNestZodiacEtsy.rescan());
    ok(await page.inputValue('#zeListing') === '1718000009', 'a number typed by the person is never replaced by a rescan');
    await ctx.close(); }

  { // exactly one listing, named only by a SKU table (no orders): the number is filled in, the title is honestly unknown; markup in a title stays text
    const W = world(), { ctx, page } = await open(W, { B: mkB([], { [LID]: { at: Date.now(), n: 6, uni: 'HUGGIE HOOPS-ZODIAC' } }) });
    ok(await page.inputValue('#zeListing') === LID && /the sorter does not know its title/.test(await text(page, row)), 'a SKU table alone fills it in and says the title is not known'); await ctx.close();
    const W2 = world(), { ctx: c2, page: p2 } = await open(W2, { B: mkB([rowOf('1', LID, 'HUGGIE HOOPS-ZODIAC', '<img src=x onerror="window.__pwned=1"> Huggie Hoop')]) });
    ok(await p2.locator(row + ' img').count() === 0 && !(await p2.evaluate(() => window.__pwned)) && /<img src=x/.test(await text(p2, row)), 'markup in an order line\'s title is drawn as text'); await c2.close(); }

  { // several listings: listed with their titles and counts, the person picks, nothing is guessed or filled
    const W = world(), B = mkB([rowOf('1', LID, 'HUGGIE HOOPS-ZODIAC', TITLE), rowOf('2', '1718000002', 'HUGGIE HOOPS-ZODIAC', 'A second zodiac hoop listing'), rowOf('3', '1718000002', 'HUGGIE HOOPS-ZODIAC', 'A second zodiac hoop listing')]);
    const { ctx, page, log, asks } = await open(W, { B });
    ok(await page.inputValue('#zeListing') === '', 'TWO listings carry the SKU: the box stays empty (never a guess)');
    const said = await text(page, row);
    ok(/2 listings carry the SKU HUGGIE HOOPS-ZODIAC/.test(said) && said.includes('isting ' + LID) && said.includes('isting 1718000002') && said.includes('A second zodiac hoop listing') && said.includes('Huggie Hoop Earrings with Astrological Charm'), 'both are listed with number and title: ' + said.slice(0, 300));
    ok(await page.getByRole('button', { name: 'Use this one' }).count() === 2 && await page.getByRole('button', { name: 'Check the listing' }).isDisabled(), 'one "Use this one" per listing; Check is dark until one is chosen');
    ok(log.length === 0 && asks.length === 0 && W.S.calls.length === 0, 'listing them called nothing');
    await page.getByRole('button', { name: 'Use this one' }).nth(1).click();
    ok(await page.inputValue('#zeListing') === LID && await page.getByRole('button', { name: 'Check the listing' }).isEnabled() && await page.getByRole('button', { name: 'Chosen' }).count() === 1, 'choosing one fills its number, lights Check and marks it Chosen');
    ok(log.length === 0 && W.S.calls.length === 0, 'choosing called nothing either'); await ctx.close(); }

  { // none: the paste box exactly as today
    const W = world(), { ctx, page, log, asks } = await open(W, { B: mkB([rowOf('1', '1744372161', 'Huggie Hoops-Tennis Ball/Racket3', 'Tennis'), rowOf('2', '1700763608', 'SWAN', 'Swan')]) });
    ok(await page.inputValue('#zeListing') === '' && await page.getByRole('button', { name: 'Check the listing' }).isDisabled(), 'no listing carries the SKU: the box is empty and Check is dark, as before');
    ok(/Paste the listing's Etsy link or number/.test(await text(page, row + ' .zeHead')) && await page.locator(row + ' .zeFound').isHidden(), 'the row still says to paste the link or number, and shows no found line');
    ok(log.length === 0 && asks.length === 0 && W.S.calls.length === 0, 'nothing called'); await ctx.close(); }

  { // the data arrives after the row is on screen: a rescan fills one, and a second listing appearing later withdraws the fill (never a guess)
    const W = world(), { ctx, page } = await open(W, { B: mkB([]) });
    ok(await page.inputValue('#zeListing') === '', 'no orders yet: empty');
    await page.evaluate(([lid, t]) => { window.B.orders.rows.push({ order: { receiptId: '1' }, line: { listingId: lid, sku: 'HUGGIE HOOPS-ZODIAC', title: t }, state: 'pooled' }); window.CharmNestZodiacEtsy.rescan(); }, [LID, TITLE]);
    ok(await page.inputValue('#zeListing') === LID, 'the orders came in: the listing is filled in');
    await page.evaluate(() => { window.B.orders.rows.push({ order: { receiptId: '2' }, line: { listingId: '1718000002', sku: 'HUGGIE HOOPS-ZODIAC', title: 'Second' }, state: 'pooled' }); window.CharmNestZodiacEtsy.rescan(); });
    ok(await page.inputValue('#zeListing') === '' && await page.getByRole('button', { name: 'Use this one' }).count() === 2 && await page.getByRole('button', { name: 'Check the listing' }).isDisabled(), 'a second listing appears: the filled-in number is withdrawn and the person picks'); await ctx.close(); }

  { // Settings is a dialog: the row is hidden while it is closed. Opening it (the row becoming visible) is what makes the page look at its data again, with no timer and no call
    const W = world(), { ctx, page, log } = await open(W);
    await page.evaluate(() => { document.body.insertAdjacentHTML('afterbegin', '<div id="wrap" style="display:none"></div>'); document.getElementById('wrap').appendChild(document.getElementById('stZodiacEtsy')); });
    await page.evaluate(([lid, t]) => { window.B = { orders: { rows: [{ order: { receiptId: '1' }, line: { listingId: lid, sku: 'HUGGIE HOOPS-ZODIAC', title: t }, state: 'pooled' }] }, maps: { listingSkus: {}, aliases: {} } }; }, [LID, TITLE]);
    await page.waitForTimeout(200); ok(await page.inputValue('#zeListing') === '', 'while the dialog is closed the data is not even looked at');
    await page.evaluate(() => { document.getElementById('wrap').style.display = 'block'; });
    await page.waitForFunction(id => document.getElementById('zeListing').value === id, LID, { timeout: 3000 });
    ok(await page.inputValue('#zeListing') === LID && log.length === 0 && W.S.calls.length === 0, 'opening it looks at the page\'s own orders once, fills the listing in and calls nothing'); await ctx.close(); }

  await browser.close();
  console.log('zodiac-etsy-ui OK: ' + n + ' checks');
})().catch(e => { console.error(e); process.exit(1); });
