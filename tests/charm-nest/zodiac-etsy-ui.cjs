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

  await browser.close();
  console.log('zodiac-etsy-ui OK: ' + n + ' checks');
})().catch(e => { console.error(e); process.exit(1); });
