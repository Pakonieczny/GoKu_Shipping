// The Etsy chip on a master card of the Library (SWANSKU, 12 Oct 2026): "Put this charm's SKU on an Etsy listing". Real Chromium on the stand-in site; the Library
// function is a mock in front of the stand-in server: it answers the repo password door like the real one (a throwaway password, in memory) and answers the op
// listingEtsySkuWrite with a SCRIPTED answer: NO Etsy call, no real write, no network (every other request that is not loopback is aborted).
// It proves what Paul sees and presses:
//   · every master card has a small "Etsy" chip in its row, drawn locked until the repo is unlocked; nothing is sent by looking or by opening the panel;
//   · the panel opens INSIDE the card (no pop-up), says what to paste, understands Paul's own Etsy link (transaction id and all) and a bare number, and says in one plain
//     line exactly what pressing the button does ("Every product of Etsy listing 1700763608 will carry the SKU BEADY SWAN ...");
//   · pressing it while locked opens the ONE password dialog (action modify) and nothing is sent until the password is right; cancelling sends nothing and says so;
//   · the call carries exactly {op, listingId, sku, by = the signed-in name, repoToken}; while it runs the button shows its spinner and the box is off; the answer is said in words
//     (done / already there / the server's own refusal, "needs listings_w" included); a refusal that can be tried again offers "Try again", one that cannot is off;
//   · a card with several SKUs offers each of them and sends the one chosen; Escape and Close fold the panel; reduced motion; no page error.
//   · CATSKU (12 Oct 2026): opening the panel offers, as taps, the listings the page ALREADY reads as this charm (order lines, saved links and the look-alike twin; read from
//     what the page holds: no request of any kind is made by looking), never prefilled; a tap only puts the number in the box; the press is unchanged (password, one call).
//   PW_DIR=<dir with playwright-core> CHROMIUM=<chrome> node tests/charm-nest/etsy-link-ui.cjs      (skipped without a browser)
const fs = require('fs'), path = require('path'), assert = require('assert'), crypto = require('crypto');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (e) { console.log('skipped: no playwright-core (' + pwDir + ')'); process.exit(0); }
const exe = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
if (!fs.existsSync(exe)) { console.log('skipped: no browser at ' + exe); process.exit(0); }
const { start, Timestamp } = require('./bridge-server.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; console.log('  ok  ' + m); };
const RIGHT = 'r' + crypto.randomBytes(9).toString('hex');   // a throwaway password that exists only in this test's mock
const SHOTS = process.env.SHOTS_DIR || '';
const LINK = 'https://www.etsy.com/listing/1700763608/swan-charm-necklace-gold-swan-bird?transaction_id=5247484637';

(async () => {
  const srv = await start({ receipts: [], repoSeed: false });
  const { st, sorterOrigin } = srv;
  const at = Date.UTC(2026, 9, 8, 12);
  const entry = (sku, aiSku, extra) => st.put('Charm_Master_Index', sku, Object.assign({ sku, masterHash: 'ab'.repeat(32), widthPt: 33.3, heightPt: 25.7, areaPt2: 443, members: 3, holes: 1, upAngle: 73, aiPath: `charmnest/master/${aiSku || sku}.ai`, aiUrl: srv.blobUrl(`charmnest/master/${aiSku || sku}.ai`), thumbPath: `charmnest/master/${aiSku || sku}.png`, indexedAt: Timestamp.fromMillis(at) }, extra));
  entry('BEADY SWAN'); entry('CHAM_A', 'CHAM'); entry('CHAM_B', 'CHAM');   // (two SKUs of one charm share one file: one card)
  // CATSKU: the shop's real pair of look-alike names (two drawings: the card with the ring is 10.5 x 12.0 mm, the other 8.7 x 9.0 mm; both read "CAT1SITTINGCAT" once spaces and punctuation are set aside)
  entry('CAT 1 - SITTING CAT', 'CAT_1_-_SITTING_CAT', { widthPt: 29.746, heightPt: 33.958, holes: 0, upAngle: 90 }); entry('CAT 1- SITTING CAT', 'CAT_1-_SITTING_CAT', { widthPt: 24.692, heightPt: 25.608, holes: 0, upAngle: 90 });
  st.put('Charm_Master_Files', 'f1', { masterHash: 'ab'.repeat(32), name: 'LIVING.ai', indexedAt: Timestamp.fromMillis(at), charms: 5, labelled: 5, skus: ['BEADY SWAN', 'CHAM_A', 'CHAM_B', 'CAT 1 - SITTING CAT', 'CAT 1- SITTING CAT'], indexedBy: 'browser' });

  const log = [], held = new Map(); let script = null, delayMs = 0;
  const browser = await chromium.launch({ executablePath: exe, args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 1000 }, timezoneId: 'UTC', locale: 'en-US' });
  await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
  await ctx.route(/^https:\/\/firebasestorage\.googleapis\.com\//, r => r.fulfill({ status: 200, contentType: 'application/octet-stream', body: 'x' }));
  await ctx.route(/\/\.netlify\/functions\/charmNestLibrary$/, async route => {
    const req = route.request(); let b = {}; try { b = req.postDataJSON() || {}; } catch (_) { b = {}; }
    const json = (code, body) => route.fulfill({ status: code, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify(body) });
    log.push({ op: b.op, token: !!b.repoToken, action: b.action || null, listingId: b.listingId, sku: b.sku, by: b.by, keys: Object.keys(b).sort().join(',') });   // (never a password or token value)
    if (b.op === 'repoUnlock') {
      if (b.password === RIGHT) { const token = 'tk-' + crypto.randomBytes(8).toString('hex'), act = b.action || 'other'; held.set(token, { action: act, exp: Date.now() + 600000 }); return json(200, { ok: true, token, expiresAt: Date.now() + 600000, once: false, action: act }); }
      return json(200, { ok: false, error: 'wrong' });
    }
    if (b.op === 'listingEtsySkuWrite') {
      const t = held.get(b.repoToken);
      if (!b.repoToken || !t || Date.now() > t.exp || !['modify', 'upload', 'other'].includes(t.action)) return json(401, { error: 'repo-locked', need: 'modify', once: false, why: !b.repoToken ? 'missing' : 'invalid' });
      if (delayMs) await new Promise(r => setTimeout(r, delayMs));
      return json(200, typeof script === 'function' ? script(b) : script);
    }
    return route.fallback();
  });
  await ctx.addInitScript(() => {
    try { localStorage.setItem('cn.employee', 'Tester'); } catch (_) {}
    window.__confirmed = 0; window.confirm = () => { window.__confirmed++; return true; }; window.prompt = () => 'Tester'; window.alert = () => {};
    if (window.Notification) { try { Object.defineProperty(window, 'Notification', { value: undefined }); } catch (_) {} }
  });
  const page = await ctx.newPage(), errors = [];
  page.on('pageerror', e => errors.push('pageerror: ' + e.message));
  page.on('console', m => { if (m.type() === 'error' && !/Failed to load resource|favicon|net::ERR|__blob|CORS policy/.test(m.text())) errors.push('console: ' + m.text().slice(0, 200)); });
  await page.goto(`${sorterOrigin}/charm-nest-1.html`);
  await page.waitForFunction(() => window.CN && window.CN.S && window.Master && window.CharmNestRepoLock && window.CharmNestEtsyLink && CN.S.cloud.ok !== null, null, { timeout: 60000 });
  await page.evaluate(() => { const s = CN.S.settings; s.review = 'off'; s.naming = 'off'; s.notify = 'off'; s.sound = 'off'; s.runMode = 'manual'; });
  await page.evaluate(() => CN.setMode('master'));
  await page.waitForFunction(() => document.querySelectorAll('#mGrid .skuTile').length >= 4, null, { timeout: 30000 });
  const tile = sku => page.locator(`#mGrid .skuTile[data-sku="${sku}"]`);
  const opCalls = () => log.filter(x => x.op === 'listingEtsySkuWrite');
  const dialogs = () => page.evaluate(() => document.querySelectorAll('dialog[open]').length);
  const typeAndGo = async pw => { const f = page.locator('dialog.repoDlg[open] #repoPw'); await f.fill(pw); await f.press('Enter'); };

  // ── 1 · the chip on every card, locked-looking; nothing is sent by looking ──
  const chips = await page.evaluate(() => [...document.querySelectorAll('#mGrid .skuTile')].map(t => { const c = t.querySelector('[data-etsylink]'); return { sku: t.dataset.sku, has: !!c, text: c && c.textContent, repo: c && c.dataset.repo, locked: c && c.classList.contains('repoLocked'), inRow: !!(c && c.closest('.row')), title: c && c.title }; }));
  ok(chips.length === 4 && chips.every(c => c.has && c.text === 'Etsy' && c.repo === 'modify' && c.inRow), 'every master card has an "Etsy" chip in its row, marked as a repo "modify" control: ' + JSON.stringify(chips.map(c => c.sku)));
  ok(chips.every(c => c.locked), 'drawn locked until the repo is unlocked');
  ok(await page.evaluate(() => document.querySelectorAll('.elPanel').length) === 0, 'no panel is open by default');
  ok(log.every(x => x.op !== 'listingEtsySkuWrite' && x.op !== 'repoUnlock'), 'looking at the library sent nothing to the write and asked for no password');

  // ── 2 · the panel opens inside the card, says what to paste, understands the link, says what the button does ──
  await tile('BEADY SWAN').locator('[data-etsylink]').click();
  const p1 = await page.evaluate(() => { const t = document.querySelector('#mGrid .skuTile[data-sku="BEADY SWAN"]'), p = t.querySelector('.elPanel'); return p ? { inside: true, after: p.previousElementSibling && p.previousElementSibling.classList.contains('row'), say: p.querySelector('.elSay').textContent, placeholder: p.querySelector('input').placeholder, go: p.querySelector('.elActs .btn.gold').textContent, goOff: p.querySelector('.elActs .btn.gold').disabled, select: !!p.querySelector('select'), dialogs: document.querySelectorAll('dialog[open]').length, focused: document.activeElement === p.querySelector('input'), expanded: t.querySelector('[data-etsylink]').getAttribute('aria-expanded') } : { inside: false }; });
  ok(p1.inside && p1.after, 'the panel opens inside the card, right under its row (no pop-up)');
  ok(p1.say === "Paste the listing's Etsy link or its number." && p1.placeholder === 'Etsy listing link or number' && p1.go === 'Put SKU on Etsy' && p1.goOff && !p1.select && p1.dialogs === 0 && p1.focused && p1.expanded === 'true', 'it asks for the link or number, the button is off, a one-SKU card has no SKU box: ' + p1.say);
  const input = tile('BEADY SWAN').locator('.elPanel input'), say = tile('BEADY SWAN').locator('.elPanel .elSay'), go = tile('BEADY SWAN').locator('.elPanel .btn.gold');
  await input.fill('hello');
  ok((await say.textContent()) === 'That is not an Etsy listing link or number.' && await go.isDisabled(), 'something that is no listing is said plainly, the button stays off');
  await input.fill('https://www.etsy.com/shop/SomeShop'); ok(await go.isDisabled(), 'a shop link is no listing');
  await input.fill(LINK);
  const line = await say.textContent();
  ok(line === 'Every product of Etsy listing 1700763608 will carry the SKU BEADY SWAN. Prices, stock and options stay as they are; the listing as it is now is kept first.' && !(await go.isDisabled()), 'Paul\'s own link (with its transaction id) is understood; the line says exactly what happens: ' + line);
  await input.fill('1700763608'); ok((await say.textContent()) === line, 'the bare number says the same line');
  ok(opCalls().length === 0 && log.every(x => x.op !== 'repoUnlock'), 'typing sent nothing and asked for no password');
  if (SHOTS) { await page.waitForTimeout(300); await tile('BEADY SWAN').screenshot({ path: path.join(SHOTS, 'etsylink-1-link-typed.png') }); }

  // ── 3 · locked: pressing opens the ONE dialog, nothing is sent until the password is right; cancelling sends nothing and says so ──
  await go.click();
  await page.waitForSelector('dialog.repoDlg[open]', { timeout: 5000 });
  const d1 = await page.evaluate(() => { const x = document.querySelector('dialog.repoDlg[open]'); return { action: x.dataset.repoAction, count: document.querySelectorAll('dialog[open]').length, text: x.textContent }; });
  ok(d1.action === 'modify' && d1.count === 1, 'locked: the repo password dialog opens (action modify) and it is the only pop-up');
  ok(/Putting the SKU BEADY SWAN on Etsy listing 1700763608\./.test(d1.text), 'the dialog says what it is for: ' + d1.text.replace(/\s+/g, ' ').slice(0, 90));
  ok(opCalls().length === 0, 'nothing is sent to the write while the dialog is open');
  await page.click('dialog.repoDlg[open] [data-k=cancel]'); await page.waitForFunction(() => !document.querySelector('dialog.repoDlg'), null, { timeout: 5000 });
  ok((await say.textContent()) === 'Nothing was sent: the repo stayed locked.' && opCalls().length === 0 && !(await go.isDisabled()) && !(await input.isDisabled()), 'cancelled: it says nothing was sent, sent nothing, and the box and button are back');

  // ── 4 · the right password: one call with exactly the right body, a spinner while it runs, the answer in words ──
  script = b => ({ ok: true, state: 'linked', calls: 2, listingId: b.listingId, sku: b.sku, productIds: ['1', '2', '3', '4', '5', '6', '7', '8', '9'], products: 9, verified: true, beforePath: 'charmnest/custom-saves/1700763608/link-BEADY_SWAN-before.json' });
  delayMs = 1500;   // (long enough for a loaded machine to look at the spinner)
  await go.click();
  await page.waitForSelector('dialog.repoDlg[open]'); await typeAndGo(RIGHT);
  await page.waitForFunction(() => document.querySelector('#mGrid .elPanel .btn .spin') && !document.querySelector('dialog[open]'), null, { timeout: 5000 });   // (the password dialog takes a moment to close on a loaded machine)
  const run = await page.evaluate(() => { const p = document.querySelector('#mGrid .elPanel'); return { btn: p.querySelector('.btn.gold').textContent, off: p.querySelector('.btn.gold').disabled, inputOff: p.querySelector('input').disabled, say: p.querySelector('.elSay').textContent, dialogs: document.querySelectorAll('dialog[open]').length }; });
  ok(run.btn === 'Putting the SKU on Etsy…' && run.off && run.inputOff && /Reading the listing on Etsy/.test(run.say) && run.dialogs === 0, 'while it runs: the button shows its spinner and a label, the box is off, the dialog is gone: ' + JSON.stringify(run));
  await page.waitForFunction(() => /^Done:/.test(document.querySelector('#mGrid .elPanel .elSay').textContent), null, { timeout: 8000 });
  delayMs = 0;
  if (SHOTS) { await page.waitForTimeout(300); await tile('BEADY SWAN').screenshot({ path: path.join(SHOTS, 'etsylink-2-done.png') }); }
  ok((await say.textContent()) === 'Done: BEADY SWAN is now on all 9 products of listing 1700763608.', 'done, in words: ' + await say.textContent());
  ok(await page.evaluate(() => document.querySelector('#mGrid .elPanel .btn.gold').hidden), 'after "done" the button is gone (a second press has nothing to do)');
  const calls = opCalls();
  ok(calls.length === 1 && calls[0].token && calls[0].listingId === '1700763608' && calls[0].sku === 'BEADY SWAN' && calls[0].by === 'Tester', 'exactly ONE call, with the listing, the SKU, the signed-in name and a repo token: ' + JSON.stringify(calls[0]));
  ok(calls[0].keys === 'by,listingId,op,repoToken,sku', 'and nothing else in it: ' + calls[0].keys);
  ok(await page.evaluate(() => /is on Etsy listing 1700763608/.test(document.getElementById('toasts').textContent)), 'a toast says it too');
  await page.click('#mGrid .elPanel .btn.ghost');
  ok(await page.evaluate(() => document.querySelectorAll('.elPanel').length) === 0, 'Close folds the panel');

  // ── 5 · unlocked now: no dialog; refusals in words; retry offered or not; one press, one call ──
  const open = async (sku, value) => { await tile(sku).locator('[data-etsylink]').click(); await tile(sku).locator('.elPanel input').fill(value); };
  script = { ok: false, state: 'failed', reason: 'scope', text: 'Etsy refused the change. Paul needs to re-authorise Etsy with listings_w', retry: true, needs: 'listings_w', calls: 2 };
  await open('BEADY SWAN', LINK);
  const go2 = tile('BEADY SWAN').locator('.elPanel .btn.gold'), say2 = tile('BEADY SWAN').locator('.elPanel .elSay');
  const before = opCalls().length; await go2.click();
  await page.waitForFunction(() => /listings_w/.test(document.querySelector('#mGrid .elPanel .elSay').textContent), null, { timeout: 5000 });
  ok(await dialogs() === 0, 'unlocked: no password dialog');
  ok(opCalls().length === before + 1, 'one press, one call');
  ok((await say2.textContent()) === 'Etsy refused the change. Paul needs to re-authorise Etsy with listings_w.' && await page.evaluate(() => document.querySelector('#mGrid .elPanel .elSay').classList.contains('bad')), 'a scope refusal says what Paul must do, in the server\'s own words');
  ok((await go2.textContent()) === 'Try again' && !(await go2.isDisabled()), 'a refusal that can be tried again offers "Try again"');
  if (SHOTS) { await page.waitForTimeout(300); await tile('BEADY SWAN').screenshot({ path: path.join(SHOTS, 'etsylink-3-refused.png') }); }
  script = { ok: false, state: 'failed', reason: 'mixed-skus', text: "the listing's products carry different SKUs (SWAN_GF, SWAN_SS): nothing was changed, a person decides which products take this SKU", retry: false, details: ['SWAN_GF', 'SWAN_SS'], calls: 1 };
  await go2.click();
  await page.waitForFunction(() => /different SKUs/.test(document.querySelector('#mGrid .elPanel .elSay').textContent), null, { timeout: 5000 });
  ok(await go2.isDisabled() && /SWAN_GF, SWAN_SS/.test(await say2.textContent()), 'a refusal that cannot help (several SKUs on the listing) is said with the SKUs and the button is off');
  await tile('BEADY SWAN').locator('.elPanel input').press('Escape');
  ok(await page.evaluate(() => document.querySelectorAll('.elPanel').length) === 0, 'Escape folds the panel');

  // ── 6 · a card with several SKUs offers each; the one chosen is the one sent ──
  script = b => ({ ok: true, state: 'linked', calls: 2, listingId: b.listingId, sku: b.sku, productIds: ['1'], products: 1, verified: true, already: true });
  await tile('CHAM_A').locator('[data-etsylink]').click();
  const sel = tile('CHAM_A').locator('.elPanel select');
  ok(await sel.count() === 1 && (await sel.locator('option').allTextContents()).join('|') === 'CHAM_A|CHAM_B' && await sel.inputValue() === 'CHAM_A', 'a two-SKU card offers both SKUs, the card\'s own (first) chosen');
  await tile('CHAM_A').locator('.elPanel input').fill('1700763609');
  await sel.selectOption('CHAM_B');
  ok(/carry the SKU CHAM_B\./.test(await tile('CHAM_A').locator('.elPanel .elSay').textContent()), 'choosing the other SKU changes the line');
  if (SHOTS) { await page.waitForTimeout(300); await tile('CHAM_A').screenshot({ path: path.join(SHOTS, 'etsylink-4-two-skus.png') }); }
  await tile('CHAM_A').locator('.elPanel .btn.gold').click();
  await page.waitForFunction(() => /^Already there/.test(document.querySelector('#mGrid .elPanel .elSay').textContent), null, { timeout: 5000 });
  const last = opCalls().pop();
  ok(last.sku === 'CHAM_B' && last.listingId === '1700763609', 'the SKU chosen is the one sent: ' + last.sku);
  ok((await tile('CHAM_A').locator('.elPanel .elSay').textContent()) === 'Already there: all 1 product of listing 1700763609 carry CHAM_B. Nothing was changed.', 'an "already there" answer says so');

  // ── 6b · CATSKU: the listings the page already reads as this charm are offered as taps (nothing is fetched, nothing is prefilled) ──
  const LISTING_TITLE = 'Small Cat Charm Add On Charm 14k Gold filled Sterling Silver 14k Rose Gold or 14k Solid Gold Cat-Lover Accessory';
  const NEW_CAT = 'CAT 1 - SITTING CAT', OLD_CAT = 'CAT 1- SITTING CAT';
  const ln = (listingId, sku, title) => ({ listingId, sku, title, transactionId: String(Math.floor(Math.random() * 1e9)) });
  const rowsFixture = [
    { state: 'pulled', line: ln(1801964156, 'CAT 1- Sitting Cat', LISTING_TITLE), spec: { designSku: OLD_CAT } },                  // the cat listing: read as the look-alike twin (two orders)
    { state: 'pulled', line: ln('1801964156', 'CAT 1- Sitting Cat', LISTING_TITLE), spec: { designSku: OLD_CAT } },
    { state: 'pulled', line: ln('1700763609', NEW_CAT, 'Already right'), spec: { designSku: NEW_CAT } },                          // already carries the exact SKU: nothing to put
    { state: 'pulled', line: ln('1700763610', 'KITTY', 'Kitty Charm Necklace'), spec: { designSku: NEW_CAT } },                    // read as this charm by another route (a saved link)
    { state: 'gone', line: ln('1700763611', 'KITTY', 'Gone order'), spec: { designSku: NEW_CAT } },                               // its order is gone: not offered
    { state: 'pulled', line: ln('1700763612', 'CAT 1- Sitting Cat', 'Cat held between two drawings'), spec: { designSku: null, formHold: { candidates: [{ sku: OLD_CAT }, { sku: NEW_CAT }] } } },
    { state: 'pulled', line: ln('1700763616', 'OTHER', 'Another charm'), spec: { designSku: 'BEADY SWAN' } }                         // another design: not offered here
  ];
  const aliasFixture = { '1700763613': { sku: NEW_CAT, v: 2, title: 'Saved link listing' }, '1700763614': { bySku: { FELINE: NEW_CAT }, title: 'Saved per-SKU link' }, '1700763615': { sku: 'BEADY SWAN', v: 2, title: 'Other design link' } };
  const unit = await page.evaluate(([rows, aliases, NEWC, OLDC]) => {
    const E = window.CharmNestEtsyLink, like = s => (String(s).toUpperCase().replace(/[^A-Z0-9]+/g, '') === 'CAT1SITTINGCAT' ? [NEWC, OLDC] : [s]);
    const a = E.suggestions([NEWC], NEWC, { rows, aliases, like }), b = E.suggestions([OLDC], OLDC, { rows, aliases, like }), c = E.suggestions([NEWC], NEWC, { rows, aliases, like: null }), d = E.suggestions([NEWC], NEWC, { rows, aliases }, 2), e = E.suggestions([NEWC], NEWC, {}), f = E.suggestions([NEWC], NEWC, null);
    return { a, b, c, d, e, f };
  }, [rowsFixture, aliasFixture, NEW_CAT, OLD_CAT]);
  ok(JSON.stringify(unit.a.map(x => x.id)) === JSON.stringify(['1801964156', '1700763610', '1700763612', '1700763613', '1700763614']), 'suggestions: the cat listing first (two lines), then the one-line listings by number, then the saved links; not the exact one, the gone order, or another design: ' + unit.a.map(x => x.id).join(','));
  ok(unit.a[0].now === OLD_CAT && unit.a[1].now === '' && unit.a[2].now === '' && unit.a[0].title === LISTING_TITLE, 'the cat listing says what it reads as today (the look-alike twin), the others read as this charm already');
  ok(!unit.b.some(x => x.id === '1801964156'), 'the look-alike\'s own card does not offer the cat listing: its lines already carry that exact SKU');
  ok(!unit.c.some(x => x.id === '1801964156') && unit.c.some(x => x.id === '1700763610'), 'without the look-alike lister only listings read as this very design are offered');
  ok(unit.d.length === 2 && unit.e.length === 0 && unit.f.length === 0, 'at most the cap, and nothing at all from nothing (no rows, no aliases, no source)');

  const before6b = log.length;
  // the page's own order rows and saved links are set only for the moment a panel opens (that is the only moment the taps read them) and put back at once
  const lend = async (...open) => { await page.evaluate(([rows, al]) => { window.__was = { rows: B.orders.rows, aliases: B.maps.aliases }; B.orders.rows = rows; B.maps.aliases = Object.assign({}, B.maps.aliases || {}, al); }, [rowsFixture, aliasFixture]); try { for (const f of open) await f(); } finally { await page.evaluate(() => { B.orders.rows = window.__was.rows; B.maps.aliases = window.__was.aliases; }); } };
  await lend(() => tile(NEW_CAT).locator('[data-etsylink]').click());
  const p6 = await page.evaluate(() => { const p = document.querySelector('#mGrid .skuTile[data-sku="CAT 1 - SITTING CAT"] .elPanel'); const g = p.querySelector('.elPicks'); return { shown: !g.hidden, ids: [...g.querySelectorAll('button')].map(b => b.getAttribute('data-listing')), texts: [...g.querySelectorAll('button')].map(b => b.textContent.replace(/\s+/g, ' ')), tip: g.querySelector('button').title, input: p.querySelector('input').value, say: p.querySelector('.elSay').textContent, goOff: p.querySelector('.btn.gold').disabled, before: [...p.children].map(c => c.className.split(' ')[0]) }; });
  ok(p6.shown && p6.ids.join(',') === '1801964156,1700763610,1700763612,1700763613,1700763614', 'the panel of the ringed card offers the listings the page already reads as this charm: ' + p6.ids.join(', '));
  ok(p6.texts[0] === '1801964156 Small Cat Charm Add On Charm…' && p6.tip === 'now read as CAT 1- SITTING CAT', 'a tap is the listing number and a short title; its hint says what the listing reads as today: ' + p6.texts[0] + ' / ' + p6.tip);
  ok(p6.input === '' && p6.say === "Paste the listing's Etsy link or its number." && p6.goOff, 'nothing is prefilled: the box is empty and the button is off until a listing is chosen');
  ok(p6.before.join(',') === 'elHead,elPicks,elIn,elSay,elActs', 'the taps sit between the heading and the box: ' + p6.before.join(','));
  ok(log.length === before6b, 'looking and opening the panel made no request at all (the taps come from what the page holds)');
  if (SHOTS) { await page.waitForTimeout(300); await tile(NEW_CAT).screenshot({ path: path.join(SHOTS, 'etsylink-5-taps.png') }); }
  const opsBefore = opCalls().length;
  await tile(NEW_CAT).locator('.elPicks button[data-listing="1801964156"]').click();
  const p7 = await page.evaluate(() => { const p = document.querySelector('#mGrid .skuTile[data-sku="CAT 1 - SITTING CAT"] .elPanel'); return { input: p.querySelector('input').value, say: p.querySelector('.elSay').textContent, goOff: p.querySelector('.btn.gold').disabled, on: [...p.querySelectorAll('.elPicks .btn.on')].map(b => b.getAttribute('data-listing')), dialogs: document.querySelectorAll('dialog[open]').length }; });
  ok(p7.input === '1801964156' && /^Every product of Etsy listing 1801964156 will carry the SKU CAT 1 - SITTING CAT\./.test(p7.say) && !p7.goOff && p7.on.join() === '1801964156', 'a tap puts the number in the box and the plain line says what the button will do: ' + p7.say.slice(0, 80));
  ok(opCalls().length === opsBefore && p7.dialogs === 0, 'the tap sent nothing and asked for no password: the press is still the button');
  script = b => ({ ok: true, state: 'linked', calls: 2, listingId: b.listingId, sku: b.sku, productIds: Array.from({ length: 12 }, (_, i) => String(i + 1)), products: 12, verified: true, beforePath: 'charmnest/custom-saves/1801964156/link-CAT_1_SITTING_CAT-before.json' });
  await tile(NEW_CAT).locator('.elPanel .btn.gold').click();
  await page.waitForFunction(() => /^Done:/.test(document.querySelector('#mGrid .skuTile[data-sku="CAT 1 - SITTING CAT"] .elSay').textContent), null, { timeout: 8000 });
  const lastCat = opCalls().pop();
  ok(opCalls().length === opsBefore + 1 && lastCat.listingId === '1801964156' && lastCat.sku === NEW_CAT && lastCat.by === 'Tester' && lastCat.token && lastCat.keys === 'by,listingId,op,repoToken,sku', 'the press sends exactly the listing and the card\'s exact SKU (' + JSON.stringify(lastCat.sku) + '), the name and the token, nothing else: ' + lastCat.keys);
  ok((await tile(NEW_CAT).locator('.elPanel .elSay').textContent()) === 'Done: CAT 1 - SITTING CAT is now on all 12 products of listing 1801964156.' && await page.evaluate(() => document.querySelector('#mGrid .skuTile[data-sku="CAT 1 - SITTING CAT"] .elPicks').hidden), 'done, in the words Paul expects; the taps fold away');
  await tile(NEW_CAT).locator('.elPanel .btn.ghost.xs:not([data-listing])').click();
  // the look-alike twin's card, another design's card, and a card with nothing to offer
  await tile('CHAM_A').locator('[data-etsylink]').click();   // (section 6 left its panel open: the chip folds it)
  await lend(() => tile(OLD_CAT).locator('[data-etsylink]').click(), () => tile('BEADY SWAN').locator('[data-etsylink]').click(), () => tile('CHAM_A').locator('[data-etsylink]').click());
  const p8 = await page.evaluate(() => { const q = sku => { const g = document.querySelector('#mGrid .skuTile[data-sku="' + sku + '"] .elPicks'); return g ? { hidden: g.hidden, ids: [...g.querySelectorAll('button')].map(b => b.getAttribute('data-listing')), tips: [...g.querySelectorAll('button')].map(b => b.title) } : null; }; return { old: q('CAT 1- SITTING CAT'), swan: q('BEADY SWAN'), cham: q('CHAM_A') }; });
  ok(p8.old && p8.old.ids.join() === '1700763609,1700763610' && p8.old.tips.join() === 'now read as CAT 1 - SITTING CAT,now read as CAT 1 - SITTING CAT', 'the look-alike twin\'s own card does not offer the cat listing (its lines already carry that exact SKU); it offers the listings read as the OTHER twin, with a hint saying so: ' + p8.old.ids.join(','));
  ok(p8.swan && p8.swan.ids.join() === '1700763616,1700763615', 'another charm\'s card offers its own listings (a saved link, an order line), nothing of the cat: ' + (p8.swan && p8.swan.ids.join(',')));
  ok(p8.cham && p8.cham.hidden && p8.cham.ids.length === 0, 'a card with no listing to offer shows no taps and no new words');
  for (const sku of [OLD_CAT, 'BEADY SWAN', 'CHAM_A']) await tile(sku).locator('[data-etsylink]').click();

  // ── 7 · reduced motion ──
  await page.emulateMedia({ reducedMotion: 'reduce' });
  await tile('BEADY SWAN').locator('[data-etsylink]').click();
  ok(await page.evaluate(() => getComputedStyle(document.querySelector('.elPanel')).animationName === 'none'), 'reduced motion: the panel does not animate');
  ok(errors.length === 0, 'no page errors: ' + errors.join(' | '));
  console.log('\netsy-link-ui: ' + n + ' checks passed');
  await browser.close(); srv.close && srv.close();
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
