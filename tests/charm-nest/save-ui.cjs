// Save a custom design to the charm repository: the designs window's strip (SAVEUI; plans/save-to-repo-1011/CONTRACT.md sections 6 and 8).
// The window (#cuDlg, CustomSheet in charm-nest-bridge.js) gets ONE strip between its add box and its foot: a switch before the press, a card of four steps
// (Sheet placement, Repository save, SKU created, Etsy linked) while it works, one calm line when it is done, one quiet line when it is not offered; the
// repository password is typed in the strip itself (never a second pop-up); the window waits for the save (up to 20 s) before it goes back into its card.
// The engine is a FAKE (the contract's CharmNestKeep shape: make(ctx) -> open / on / run / retry / unlock / settled): nothing is saved, no Etsy call, no
// paid call; the unlock goes through the REAL charm-nest-repo-lock.js and the stand-in server's real repoUnlock (a throwaway password, in memory).
// Real Chromium on the stand-in site (bridge-server.cjs); every request that is not to the loopback is aborted. Reads the cases of the contract:
//   strip states (offered, off, locked, running, linked, saved, held, not offered with each reason) · the four rows open together and the Sheet row follows busy /
//   sent / sendError · no second dialog (also when locked) · a wrong password shakes and clears · the locked-out line · reduced motion: no animate call, no ring ·
//   the sandbox: label, rehearsed rows, the writing ops never called · offline · Retry on the failing row only · the header decodes ACORN&#39;S LEAF and &#x27; ·
//   the hold rule (the window waits for settled, up to 20 s; closes at once on a failure; closes as today with the strip off) · keyboard · small screens.
//   SHOTS_DIR=<dir> also saves the pictures of the window in its states.
//   node tests/charm-nest/save-ui.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>; skipped without a browser)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('skipped: no playwright-core (' + pwDir + ')'); process.exit(0); }
const exe = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
if (!fs.existsSync(exe)) { console.log('skipped: no browser at ' + exe); process.exit(0); }
const { start, REPO_TEST } = require('./bridge-server.cjs');
const SHOTS = process.env.SHOTS_DIR || '';
const ONLY = (process.env.ONLY || '').split(',').filter(Boolean), want = k => !ONLY.length || ONLY.includes(k);
// KEEPSAVE (16 Oct 2026): the switch ON now records a save intent that starts at once (no Send to Sheet, no 'ready' state). Scenes B C D F G H J J2 R encode the old PRESS model
// (switch ON by default after KEEPOFF was already stale on main: scene B timed out waiting for phase 'ready' on pristine main) and are kept for reference behind LEGACY_PRESS=1;
// their live equivalents are tests/charm-nest/keep-durable-browser.cjs (S1-S5, R, X) and keep-durable.cjs (the engine and keeper with a fault at every step).
const LEGACY = process.env.LEGACY_PRESS === '1';
const fails = []; let n = 0;
const ok = (c, m) => { n++; if (!c) fails.push(m); console.log((c ? '  ok   ' : '  FAIL ') + m); };

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
// a leaf: two arcs for its outline (one closed cut line) and a small hole near its stem
const LEAF_DXF = DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES',
  0, 'LWPOLYLINE', 8, 'CUT', 90, 2, 70, 1, 10, 0, 20, 0, 42, -0.62, 10, 13, 20, 0, 42, -0.62,
  0, 'CIRCLE', 8, 'CUT', 10, 3.2, 20, 0, 40, 0.9, 0, 'ENDSEC', 0, 'EOF');
const order = (rid, listing, sku, title) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + String(rid).slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: String(rid) + '1', listingId: String(listing), sku, title, quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' }] });

/** The fake engine, in the page: the contract's CharmNestKeep (CONTRACT.md section 8) with scripted answers, the REAL lock for the password. */
function fakeKeep() {
  const W = window.__kw = { calls: [], made: [], steps: null, retrySteps: null, open: {}, settledAsked: [], openGap: 0, early: false, needToken: true, log: [] };
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const FAKE = {
    durable(deps) { return W.real.durable(deps); },   // (KEEPSAVE: the REAL keeper runs the fake engine)
    make(ctx) {
      const L = [], sku = () => W.sku || 'ACORN_LEAF_4821';
      const emit = ev => { W.log.push(ev); if (ev.done) W.out = { done: ev.done, held: ev.reason === 'locked' ? 'locked' : '', reason: ev.reason || '', retry: ev.retry !== false, text: ev.text || '', sku: ev.sku || sku() }; for (const f of L) try { f(ev); } catch (_) {} };
      let running = null;
      const play = async steps => { for (const s of steps) { if (s === 'throw') throw new Error('the reply was cut off'); if (typeof s === 'number') await sleep(s); else emit(s); } };
      const heldRun = () => { emit({ row: 'sku', state: 'done', text: sku() }); emit({ row: 'repo', state: 'locked', text: 'Not saved: needs the repository password' }); emit({ row: 'etsy', state: 'idle', text: 'Waits for the repository' }); emit({ done: 'held', sku: sku() }); };
      const go = async (steps, label, arg) => {
        W.calls.push(arg ? label + ':' + arg : label);
        if (W.needToken && !ctx.sandbox && !(ctx.lock && ctx.lock.unlocked('upload'))) return heldRun();
        await play(steps || []);
      };
      const k = {
        on: f => { L.push(f); },
        async open() {
          W.calls.push('open'); W.made.push({ rid: ctx.rid, listingId: ctx.listingId, title: ctx.title, line: ctx.line, sandbox: ctx.sandbox, by: ctx.by, hasCharm: !!ctx.charm, hasParsed: !!ctx.parsed, hash: ctx.F && ctx.F.hash });
          if (W.openGap) await sleep(W.openGap);
          return Object.assign({ offered: true, why: '', sku: sku(), group: 'all', aliasWorks: true, saved: null, locked: !(ctx.lock && ctx.lock.unlocked('upload')), rehearsal: !!ctx.sandbox, reuse: null }, W.open);
        },
        run() { return running = go(W.steps, 'run'); },
        outcome() { return W.out || null; },
        retry(which) { return running = go(W.retrySteps || W.steps, 'retry', which); },
        async unlock(pw) {
          W.calls.push('unlock');
          const r = await ctx.lock.submitInline('upload', pw, { target: 'test' });
          return { ok: !!r.ok, reason: r.ok ? undefined : r.reason, retryAfterSec: r.retryAfterSec };
        },
        settled(ms) { W.settledAsked.push(ms); return Promise.race([Promise.resolve(running).catch(() => {}), sleep(W.early ? 250 : ms)]); }
      };
      return k;
    }
  };
  // (the page now loads the real charm-nest-keep.js too: the fake answers for the page unless a scene asks for the real one)
  W.real = null; Object.defineProperty(window, 'CharmNestKeep', { configurable: true, get: () => (W.noEngine ? undefined : W.useReal ? W.real : FAKE), set: v => { W.real = v; } });
}
const steps = {
  linked: [{ row: 'sku', state: 'work', text: 'Choosing a SKU…' }, 260, { row: 'sku', state: 'done', text: 'ACORN_LEAF_4821' }, { row: 'repo', state: 'work', text: 'Saving ACORN_LEAF_4821.ai…' }, 420,
    { row: 'repo', state: 'done', text: 'Saved to the repository' }, { row: 'etsy', state: 'work', text: 'Setting the SKU on Etsy…' }, 420, { row: 'etsy', state: 'done', text: 'Linked on Etsy' }, { done: 'linked', sku: 'ACORN_LEAF_4821' }],
  etsyFail: [{ row: 'sku', state: 'done', text: 'ACORN_LEAF_4821' }, { row: 'repo', state: 'work', text: 'Saving ACORN_LEAF_4821.ai…' }, 200, { row: 'repo', state: 'done', text: 'Saved to the repository' }, { row: 'etsy', state: 'work', text: 'Setting the SKU on Etsy…' }, 200,
    { row: 'etsy', state: 'failed', text: 'Etsy refused the change. Paul needs to re-authorise Etsy with listings_w', retry: true }, { done: 'saved', sku: 'ACORN_LEAF_4821' }],
  offline: [{ row: 'sku', state: 'failed', text: 'You are offline. Nothing was saved; the design is still on this order.', retry: true }, { row: 'repo', state: 'idle', text: 'Waits for the SKU' }, { row: 'etsy', state: 'idle', text: 'Waits for the repository' }, { done: 'held', sku: 'ACORN_LEAF_4821' }],
  repoFail: [{ row: 'sku', state: 'done', text: 'ACORN_LEAF_4821' }, { row: 'repo', state: 'failed', text: 'the file could not be uploaded', retry: true }, { row: 'etsy', state: 'idle', text: 'Waits for the repository' }, { done: 'held', sku: 'ACORN_LEAF_4821' }],
  rehearsal: [{ row: 'sku', state: 'work', text: 'Choosing a SKU…' }, 240, { row: 'sku', state: 'done', text: 'ACORN_LEAF_4821' }, { row: 'repo', state: 'rehearsed', text: 'Rehearsed: would save ACORN_LEAF_4821.ai' }, 200, { row: 'etsy', state: 'rehearsed', text: 'Rehearsed: would set the SKU on listing …4821' }, { done: 'rehearsed', sku: 'ACORN_LEAF_4821' }],
  crash: [{ row: 'sku', state: 'done', text: 'ACORN_LEAF_4821' }, { row: 'repo', state: 'done', text: 'Saved to the repository' }, { row: 'etsy', state: 'work', text: 'Setting the SKU on Etsy…' }, 300, 'throw'],
  slow: [{ row: 'sku', state: 'work', text: 'Choosing a SKU…' }, 4000, { row: 'sku', state: 'done', text: 'ACORN_LEAF_4821' }, { row: 'repo', state: 'work', text: 'Saving ACORN_LEAF_4821.ai…' }, 60000]
};

async function main() {
  const browser = await chromium.launch({ executablePath: exe, args: ['--no-sandbox'] });
  const shot = async (page, name) => { if (!SHOTS) return; fs.mkdirSync(SHOTS, { recursive: true }); await page.evaluate(() => { const b = document.querySelector('.cnNameBar'); if (b) b.style.visibility = 'hidden'; }); await page.waitForTimeout(450); await page.locator('#cuDlg').screenshot({ path: path.join(SHOTS, name + '.png') }); await page.evaluate(() => { const b = document.querySelector('.cnNameBar'); if (b) b.style.visibility = ''; }); };
  const mk = async (opts = {}) => {
    const srv = await start({ receipts: [], repoSeed: opts.locked === false ? undefined : false });
    const ctx = await browser.newContext({ viewport: opts.viewport || { width: 1440, height: 950 }, timezoneId: 'UTC', locale: 'en-US', reducedMotion: opts.reduced ? 'reduce' : 'no-preference' });
    await ctx.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await ctx.addInitScript(sandbox => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: sandbox ? 'on' : 'off' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} }, !!opts.sandbox);
    await ctx.addInitScript(fakeKeep);
    await ctx.addInitScript(() => {
      window.__confirmed = 0; window.confirm = () => { window.__confirmed++; return true; }; window.prompt = () => 'Test Operator'; window.alert = () => {};
      window.__attempts = []; window.addEventListener('repo-auth-attempt', e => window.__attempts.push(e.detail));
      window.__anim = { n: 0, ring: 0 }; const A = Element.prototype.animate; Element.prototype.animate = function (k, o) { if (this.closest && this.closest('#cuKeep')) window.__anim.n++; return A.call(this, k, o); };
      window.__toasts = []; window.addEventListener('DOMContentLoaded', () => { const t = window.toast; if (typeof t === 'function' && !t.__spy) { window.toast = function (m, k) { window.__toasts.push(String(m)); return t.apply(this, arguments); }; window.toast.__spy = true; } });
      window.__ringSeen = 0; new MutationObserver(ms => { for (const m of ms) if (m.target.classList && m.target.classList.contains('cuRing')) window.__ringSeen++; }).observe(document, { subtree: true, attributes: true, attributeFilter: ['class'] });
      window.__ops = []; window.__opsHook = () => { const a = window.api; if (!a || a.__spy) return; const s = async function (fn, body, o) { try { window.__ops.push([fn, body && body.op, body && !!body.sandbox]); } catch (_) {} return a.apply(this, arguments); }; s.__spy = true; window.api = s; };
    });
    const page = await ctx.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('console', m => { if (m.type() === 'warning' && /Save (strip|keeper)/.test(m.text())) console.log('  .. page warning:', m.text().slice(0, 300)); });
    page.on('pageerror', e => { errors.push(String(e.message)); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 3).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomSheet && CN.S.cloud.ok === true, null, { timeout: 60000 });
    return { srv, ctx, page, errors, close: async () => { await ctx.close(); await srv.close(); } };
  };
  /** The orders on the page (each its own custom line), the cards drawn. */
  const seed = (page, orders) => page.evaluate(async orders => {
    await Orders.loadMaps(true);
    for (const order of orders) for (const line of order.lines) {
      const key = CharmNestOrders.lineKey(order, line);
      const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row);
    }
    // (read as custom orders the way Claude's reading says it: no model is called, the answers are put where the reading would be)
    B.maps.customRead = Object.assign({}, B.maps.customRead || {}, Object.fromEntries(orders.flatMap(o => o.lines.map(l => [CharmNestOrders.lineKey(o, l), { kind: 'custom', confidence: 0.96, summary: 'the buyer\'s own design' }]))));
    Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
  }, orders);
  /** A design on a card, programmatically as a drop does it (a .ai made from the leaf), the window open on it and its row read. */
  const dropDesign = async (page, rid, name = 'Test_Charm.ai', pick = true) => {
    await page.evaluate(async ({ rid, name, dxf }) => {
      const key = Object.keys(Object.fromEntries(B.orders.byKey)).find(k => k.startsWith(rid + '_')), it = Review.customItemFor(key);
      if (!it) throw new Error('no custom card for ' + rid);
      const r = await CharmNestDXF.toPdf(new TextEncoder().encode(dxf), 'x.dxf');
      CustomSheet.add(it, [new File([r.bytes], name)]);
    }, { rid, name, dxf: LEAF_DXF });
    await page.waitForFunction(() => document.querySelectorAll('#cuDlg[open] .cuFile .cuThumb img').length >= 1, null, { timeout: 30000 });
    if (pick) await page.click('#cuDlg .cuFile .cuM[data-m="gold"]');
  };
  /** A run under way, as in the shop (past its pool step): a line sent now goes onto its sheet at once. */
  const startRun = page => page.evaluate(() => { const day = new Date().toISOString().slice(0, 10); B.run = { runId: `run-${day}-keep`, day, setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] }; });
  /** Every frame: where the strip and the window stand (what changed is kept, with its time). */
  const trace = page => page.evaluate(() => {
    const T = window.__tr = { on: true, f: [], t0: performance.now() };
    const vis = n => !!n && !n.hidden && !n.closest('[hidden]') && n.getClientRects().length > 0;
    let last = '';
    const loop = () => { if (!T.on) return; const h = document.getElementById('cuKeep'), d = document.getElementById('cuDlg');
      const rows = [...h.querySelectorAll('.cuKr')], st = h.querySelector('.cuKeepSteps'), ln = h.querySelector('.cuKeepLine');
      const rec = { ph: h.dataset.phase, open: !!d.open, steps: vis(st), rowsVis: rows.filter(vis).length, rows: rows.map(r => r.dataset.s).join(','), line: vis(ln) ? ln.querySelector('b').textContent : '', ring: ln.classList.contains('cuRing'), sw: vis(h.querySelector('.cuKeepSw')), calls: __kw.calls.join('>') };
      const k = JSON.stringify(rec); if (k !== last) { last = k; rec.t = Math.round(performance.now() - T.t0); T.f.push(rec); }
      requestAnimationFrame(loop); };
    requestAnimationFrame(loop);
  });
  const traced = page => page.evaluate(() => { __tr.on = false; return __tr.f; });
  const toasts = page => page.evaluate(() => (window.__toasts || []).slice());
  /** The tour that carries the sent card has ended (the card is back in its list). */
  const tourDone = page => page.waitForFunction(() => !document.querySelector('.mGhost, #tourLayer .tourPiece, #tourLayer .tourCard') && CN.S.mode === 'review', null, { timeout: 30000 });
  const closeWin = async page => { await page.evaluate(() => CustomSheet.shut()); await page.waitForFunction(() => !document.querySelector('dialog[open]')); await page.waitForTimeout(80); };
  const strip = page => page.evaluate(() => {
    const h = document.getElementById('cuKeep'), vis = n => !!n && !n.hidden && !n.closest('[hidden]') && n.getClientRects().length > 0;
    const rows = [...h.querySelectorAll('.cuKr')].map(r => ({ r: r.dataset.r, s: r.dataset.s, t: r.querySelector('.cuKd').textContent, b: [...r.querySelectorAll('.cuKa button')].map(b => b.textContent), vis: vis(r) }));
    const line = h.querySelector('.cuKeepLine'), sw = h.querySelector('.cuSwitch'), pw = h.querySelector('.cuKeepPw');
    return { shown: vis(h), phase: h.dataset.phase, sandbox: h.dataset.sandbox, sw: vis(h.querySelector('.cuKeepSw')) && vis(sw), on: sw.getAttribute('aria-checked') === 'true', swDisabled: sw.disabled, label: h.querySelector('#cuKeepL').textContent, preview: h.querySelector('#cuKeepP').textContent,
      pill: vis(h.querySelector('.cuKeepPill')) ? h.querySelector('.cuKeepPill').textContent : '', now: vis(h.querySelector('[data-k-now]')), pw: vis(pw), pwMsg: pw.querySelector('.cuKeepMsg').textContent, unlockDisabled: pw.querySelector('[data-k-unlock]').disabled,
      steps: vis(h.querySelector('.cuKeepSteps')), rows, line: vis(line) ? { main: line.querySelector('b').textContent, sub: line.querySelector('.sub').textContent, cls: line.className, btns: [...line.querySelectorAll('button')].map(b => b.textContent) } : null, dialogs: document.querySelectorAll('dialog[open]').length, busy: h.getAttribute('aria-busy') };
  });
  const settle = page => page.evaluate(() => Promise.race([Promise.all(document.getAnimations().filter(a => { try { return a.effect.getComputedTiming().iterations !== Infinity; } catch (_) { return true; } }).map(a => a.finished.catch(() => null))), new Promise(r => setTimeout(r, 3000))]).then(() => new Promise(r => setTimeout(r, 60))));
  const ready = (page, phase, ms = 8000) => page.waitForFunction(p => { const h = document.getElementById('cuKeep'); return h && h.dataset.phase === p; }, phase, { timeout: ms });

  try {
    /* ── A · the strip, locked page (served locked): states, password, keyboard, no second dialog ── */
    if (want('A')) {
      const w = await mk(); const { page } = w;
      await seed(page, [order(4196630529, 1800004821, 'ACORN&#39;S LEAF', "Custom Charm - Acorn's Leaf, Personalized"), order(4196630530, 1800004822, 'ACORN&#x27;S LEAF', 'Custom Charm Two')]);
      await page.evaluate(() => { window.__opsHook(); });
      await page.click('#reviewView .egTab[data-k="customOrder"]');
      await page.waitForSelector('#rvList .reviewListRow[data-rid="4196630529"]');
      // before any design: the window is exactly as it was, and the strip is not there
      await page.evaluate(() => { const it = Review.customItemFor(Object.keys(Object.fromEntries(B.orders.byKey)).find(k => k.startsWith('4196630529_'))); CustomSheet.open(it, {}); });
      await page.waitForSelector('#cuDlg[open]');
      let s = await strip(page);
      ok(!s.shown, 'no design yet: the strip is not shown (the window is as it was)');
      await closeWin(page);
      await dropDesign(page, '4196630529');
      await ready(page, 'off');
      await settle(page);
      s = await strip(page);
      ok(s.shown && s.sw && !s.on && s.phase === 'off', 'a design read, the repository locked: the switch shows, OFF by default (' + s.phase + ')');
      ok(s.label === 'Save to the charm repository and link this listing' && s.preview === 'New SKU ACORN_LEAF_4821 · switch on to make later orders of this listing use this design', 'the exact words, the switch being off (REVDROP: nothing is linked yet, so the line says what switching it on does): ' + s.label + ' / ' + s.preview);
      ok(!s.pw && !s.steps && !s.line && !s.now, 'off: only the switch row (no password row, no steps, no line)');
      const hdr = await page.evaluate(() => ({ sub: document.getElementById('cuDlgSub').textContent, all: document.getElementById('cuDlg').textContent }));
      ok(/ACORN'S LEAF/.test(hdr.sub) && !/&#/.test(hdr.all), 'the header shows ACORN\'S LEAF for ACORN&#39;S LEAF, no raw &# anywhere in the window: ' + hdr.sub);
      await shot(page, '1-idle');
      const made = await page.evaluate(() => __kw.made[0]);
      ok(made.rid === '4196630529' && made.listingId === '1800004821' && made.hasCharm && made.hasParsed && made.by === 'Test Operator' && made.sandbox === false && made.line.sku === "ACORN'S LEAF" && made.title === "Custom Charm - Acorn's Leaf, Personalized", 'the engine is given the order, its listing, the read design and the signed-in name: ' + JSON.stringify(made).slice(0, 200));
      // keyboard: the switch toggles with Space and with Enter; turning it on while locked opens the password row and the focus goes to the field
      await page.focus('#cuKeepSwitch');
      await page.keyboard.press('Space');
      await page.waitForFunction(() => document.querySelector('#cuKeep .cuKeepPw') && !document.querySelector('#cuKeep .cuKeepPw').hidden);
      await settle(page);
      s = await strip(page);
      ok(s.on && s.pw && s.pwMsg === 'Locked: type the repository password to save' && s.dialogs === 1, 'Space turns the switch on; locked, the password row opens with its line, and the window is still the only pop-up');
      ok(await page.evaluate(() => document.activeElement && document.activeElement.id === 'cuKeepPw'), 'the focus goes to the password field when it opens');
      await shot(page, '3-locked');
      await page.keyboard.press('Shift+Tab'); await page.keyboard.press('Shift+Tab');
      await page.focus('#cuKeepSwitch'); await page.keyboard.press('Enter');
      s = await strip(page); ok(!s.on, 'Enter turns it off again');
      await page.keyboard.press('Enter'); await settle(page); s = await strip(page); ok(s.on && s.pw, 'and on');
      // a wrong password: the field is clear at once, it shakes, one plain line; nothing saved
      const before = await page.evaluate(() => __kw.calls.length);
      await page.fill('#cuKeepPw', 'not-the-password-' + Math.random());
      await page.keyboard.press('Enter');
      await page.waitForFunction(() => document.querySelector('#cuKeepMsg .spin') || /not right/.test(document.getElementById('cuKeepMsg').textContent), null, { timeout: 5000 });
      ok(await page.evaluate(() => document.getElementById('cuKeepPw').value === ''), 'the field is empty from the moment it is sent');
      await page.waitForFunction(() => /not right/.test(document.getElementById('cuKeepMsg').textContent), null, { timeout: 8000 });
      s = await strip(page);
      ok(s.pwMsg === 'That password is not right. Try again.' && s.dialogs === 1 && (await page.evaluate(() => document.getElementById('cuKeepMsg').classList.contains('bad'))), 'a wrong password: "That password is not right. Try again.", no pop-up');
      ok(await page.evaluate(() => document.getElementById('cuKeepPw').classList.contains('cuShake') || document.getAnimations().some(a => a.animationName === 'cuShake')), 'the field shakes (cuShake)');
      ok(await page.evaluate(() => window.__attempts.length === 1 && window.__attempts[0].ok === false && window.__attempts[0].reason === 'wrong' && window.__attempts[0].action === 'upload'), 'the wrong try fires repo-auth-attempt (wrong, upload) with no password in it');
      ok(await page.evaluate(() => !CharmNestRepoLock.unlocked('upload')), 'still locked');
      // the spinner while the server checks, with its label
      await page.evaluate(() => { const real = CN.api; window.__slow = true; const a = window.api; });
      // the right password (KEEPSAVE): the save was asked for already, so the moment the lock opens it runs BY ITSELF (no Send to Sheet, no Save press); the row folds away
      await page.evaluate(steps => { __kw.steps = steps; }, steps.linked);
      await page.fill('#cuKeepPw', REPO_TEST.password);
      await page.keyboard.press('Enter');
      await page.waitForFunction(() => CharmNestRepoLock.unlocked('upload'), null, { timeout: 8000 });
      await page.waitForFunction(() => document.querySelector('#cuKeep .cuKeepPw').hidden, null, { timeout: 4000 });
      await page.waitForFunction(() => document.getElementById('cuKeep').dataset.phase === 'linked', null, { timeout: 20000 }).catch(async e => { console.log('DBG', JSON.stringify(await page.evaluate(() => ({ calls: __kw.calls, intents: localStorage.getItem('cn.keepIntents.v1'), phase: document.getElementById('cuKeep').dataset.phase, online: CN.S.cloud.ok }))).slice(0, 1500)); throw e; });
      s = await strip(page);
      ok(!s.pw && s.dialogs === 1, 'the right password: unlocked, the password row folds away, still one pop-up only');
      ok(await page.evaluate(() => __kw.calls.filter(c => c === 'run').length === 1 && !__kw.calls.some(c => /^retry/.test(c))), 'the moment the lock opened the save ran by itself, exactly once (nobody pressed Send to Sheet or Save)');
      await shot(page, '2-offered');
      ok(await page.evaluate(() => !!localStorage.getItem('cn.keepIntents.v1') && Object.values(JSON.parse(localStorage.getItem('cn.keepIntents.v1')).intents).some(r => r.state === 'done' && r.rid === '4196630529')), 'the save is recorded in browser storage (cn.keepIntents.v1) and ended done');
      ok(!(await page.evaluate(() => JSON.stringify([...document.querySelectorAll('#cuDlg *')].map(e => e.value || '')).includes('test-'))), 'no password is left anywhere in the window');
      await w.close();
    }

    /* ── B · the repository unlocked: the switch is ON, the press opens the four steps, the save runs beside the Sheet, the window waits and closes calmly ── */
    if (LEGACY && want('B')) {
      const w = await mk({ locked: false }); const { page } = w;
      await seed(page, [order(4196630531, 1800004821, 'ACORN&#39;S LEAF', "Custom Charm - Acorn's Leaf, Personalized"), order(4196630532, 1800004822, 'LEAF ONE', 'Custom Charm Leaf'), order(4196630533, 1800004823, 'LEAF TWO', 'Custom Charm Leaf Two')]);
      await startRun(page);
      await page.click('#reviewView .egTab[data-k="customOrder"]');
      await dropDesign(page, '4196630531');
      await ready(page, 'ready'); await settle(page);
      let s = await strip(page);
      ok(s.on && s.phase === 'ready' && s.sw && !s.pw && s.dialogs === 1, 'a token held: the switch is ON by default and nothing is asked');
      await shot(page, '2-offered');
      await page.evaluate(() => { __kw.steps = null; const W = __kw; W.sku = 'ACORN_LEAF_4821'; });
      await page.evaluate(steps => { __kw.steps = steps; }, steps.linked);
      // the engine's first call must come after the Seal stamp (the intent is pressed); and the window never makes a second dialog
      await page.evaluate(() => { const make = CharmNestKeep.make; CharmNestKeep.make = ctx => { const k = make(ctx), run = k.run; k.run = function () { __kw.sawRecorded = Object.values(CustomSheet.entries()).some(e => e.rid === ctx.rid && e.sendIntent && !e.sent); __kw.sheetBusy = true; return run.apply(this, arguments); }; return k; }; });
      // (the strip, made before the wrap above, is made again for this order: another card)
      await closeWin(page); await dropDesign(page, '4196630532'); await ready(page, 'ready'); await settle(page);
      await trace(page);
      const t0 = Date.now();
      await page.click('#cuDlg [data-send]');
      await page.waitForFunction(() => { const h = document.getElementById('cuKeep'); return h.dataset.phase === 'linked'; }, null, { timeout: 40000 });
      await page.waitForFunction(() => !document.getElementById('cuDlg').open, null, { timeout: 15000 });
      const tr = await traced(page);
      const firstSteps = tr.find(f => f.steps), runAt = tr.find(f => /run/.test(f.calls)), linkedAt = tr.find(f => f.ph === 'linked'), lineAt = tr.find(f => /^Linked/.test(f.line)), closedAt = tr.find(f => f.t > (linkedAt || { t: 0 }).t && !f.open);
      ok(firstSteps && firstSteps.rowsVis === 4 && firstSteps.rows.split(',')[0] !== undefined, 'the four rows open together: the steps card is shown with all four rows in the same frame (' + (firstSteps && firstSteps.rowsVis) + ' of 4)');
      ok(firstSteps && firstSteps.ph === 'running' && firstSteps.rows.split(',')[0] === 'work', 'the Sheet row is working from the first frame (the existing busy words)');
      ok(runAt && firstSteps && Math.abs(runAt.t - firstSteps.t) < 1500 && (await page.evaluate(() => __kw.sawRecorded)) === true, 'the save starts with the press, beside the stamp and the opening of the rows (the send already recorded; its work never waits for the animation): the run came ' + (firstSteps.t - runAt.t) + ' ms before the rows');
      const seq = [...new Set(tr.map(f => f.rows))];
      ok(seq.some(r => /^work,(idle|work),(idle|work),idle$/.test(r)) && seq.some(r => /^(work|done),work/.test(r)), 'the rows go on one after the other: ' + seq.slice(0, 7).join(' | '));
      ok(await page.evaluate(() => __kw.settledAsked.length >= 1 && __kw.settledAsked.every(x => x === 20000)), 'the window asks the engine to settle, for up to 20 s');
      ok(tr.some(f => f.ph === 'linked' && f.open) && linkedAt && lineAt, 'it ends as one calm line "' + (lineAt && lineAt.line) + '"');
      ok(tr.some(f => f.ring) && (await page.evaluate(() => __ringSeen)) >= 1, 'the line takes one soft sage ring');
      ok(closedAt && closedAt.t - lineAt.t >= 800, 'the window stays a calm moment after all work is finished (' + (closedAt && lineAt ? closedAt.t - lineAt.t : '?') + ' ms), then goes back into its card');
      ok(tr.every(f => f.open === true || f.t >= (closedAt || { t: 0 }).t), 'it never closed before the save ended');
      ok(tr.every(f => !(f.open === false && f.ph === 'running')), 'the window was open the whole time the save was running');
      const toastsB = await toasts(page);
      ok(!toastsB.some(t => /Etsy not updated|not saved/i.test(t)), 'no failure toast on a good ending: ' + toastsB.join(' | ').slice(0, 120));
      await tourDone(page); await page.waitForTimeout(300);
      await page.click('#reviewView .rvSeg [data-cseg="done"]'); await page.waitForSelector('#rvList [data-rid="4196630532"]'); await page.waitForTimeout(200);
      ok((await page.evaluate(() => document.querySelector('#rvList .reviewListRow[data-rid="4196630532"] .cuKeepNote') ? document.querySelector('#rvList .reviewListRow[data-rid="4196630532"] .cuKeepNote').textContent : '')) === 'Linked · ACORN_LEAF_4821', 'the order\'s card keeps a quiet line: Linked · ACORN_LEAF_4821');
      // a key pressed in the calm moment ends it at once
      await page.click('#reviewView .rvSeg [data-cseg="open"]'); await page.waitForSelector('#rvList [data-rid="4196630533"]');
      await page.evaluate(() => { __kw.steps = null; });
      await page.evaluate(steps => { __kw.steps = steps; }, steps.linked);
      await dropDesign(page, '4196630533'); await ready(page, 'ready'); await settle(page);
      await trace(page);
      await page.click('#cuDlg [data-send]');
      // (the calm moment begins once the save AND the Sheet are finished: the foot's spinner has gone)
      await page.waitForFunction(() => document.getElementById('cuKeep').dataset.phase === 'linked' && document.querySelector('#cuKeep .cuKeepLine') && !document.querySelector('#cuKeep .cuKeepLine').hidden && !document.querySelector('#cuSum .cuStat') && document.getElementById('cuDlg').open, null, { timeout: 40000 });
      await page.waitForTimeout(60);
      await page.evaluate(() => { const c = Motion.dialogClose; Motion.dialogClose = function () { __tr.closeCall = Math.round(performance.now() - __tr.t0); __tr.sealBusy = !!(window.Seal && Seal.busy && Seal.busy()); return c.apply(this, arguments); }; });
      const tKey = await page.evaluate(() => { __tr.keyAt = Math.round(performance.now() - __tr.t0); return __tr.keyAt; });
      await page.keyboard.press('Space');
      await page.waitForFunction(() => !document.getElementById('cuDlg').open, null, { timeout: 6000 });
      const tr2 = await traced(page), shutAt = tr2.find(f => !f.open && f.t >= tKey);
      ok(shutAt && shutAt.t - tKey < 500, 'any press ends the calm moment at once (' + (shutAt ? shutAt.t - tKey : '?') + ' ms after the key)');
      await w.close();
    }

    /* ── shared by C–I ── */
    const FRESH = (rid, listing, sku, title) => order(rid, listing || 1800004821, sku || 'ACORN&#39;S LEAF', title || "Custom Charm - Acorn's Leaf, Personalized");
    const tabCustom = page => page.click('#reviewView .egTab[data-k="customOrder"]');
    const openWin = async (page, rid) => { await page.evaluate(rid => { const it = Review.customItemFor(Object.keys(Object.fromEntries(B.orders.byKey)).find(k => k.startsWith(rid + '_'))); CustomSheet.open(it, {}); }, rid); await page.waitForSelector('#cuDlg[open]'); await page.waitForFunction(() => document.getElementById('cuKeep').dataset.phase !== undefined, null, { timeout: 8000 }); };
    const clickDone = async (page, rid) => { await tourDone(page); await page.waitForTimeout(250); await page.click('#reviewView .rvSeg [data-cseg="done"]'); await page.waitForSelector(`#rvList .reviewListRow[data-rid="${rid}"]`); await page.waitForTimeout(150); };
    const noteOf = (page, rid) => page.evaluate(rid => { const n = document.querySelector(`#rvList .reviewListRow[data-rid="${rid}"] .cuKeepNote`); return n ? n.textContent.replace(/\s+/g, ' ').trim() : ''; }, rid);
    const press = page => page.click('#cuDlg [data-send]');
    const winClosed = (page, ms = 40000) => page.waitForFunction(() => !document.getElementById('cuDlg').open, null, { timeout: ms });
    const hookClose = page => page.evaluate(() => { if (Motion.dialogClose.__w) return; const c = Motion.dialogClose; Motion.dialogClose = function () { if (window.__tr && __tr.closeCall == null) __tr.closeCall = Math.round(performance.now() - __tr.t0); return c.apply(this, arguments); }; Motion.dialogClose.__w = true; });
    const closeAt = page => page.evaluate(() => __tr.closeCall);
    const tagLine = (page, tag) => page.evaluate(tag => { __tr.tag = tag; }, tag);

    /* ── C · failures, and Retry that runs from the failing row only ── */
    if (LEGACY && want('C')) {
      const w = await mk({ locked: false }); const { page } = w;
      await seed(page, [FRESH(4196630541), FRESH(4196630542, 1800004822, 'LEAF TWO', 'Custom Leaf Two'), FRESH(4196630543, 1800004823, 'LEAF THREE', 'Custom Leaf Three'), FRESH(4196630545, 1800004825, 'LEAF FIVE', 'Custom Leaf Five'), FRESH(4196630546, 1800004826, 'LEAF SIX', 'Custom Leaf Six'), FRESH(4196630544, 1800004824, 'LEAF FOUR', 'Custom Leaf Four')]);
      await startRun(page); await tabCustom(page);
      // C1 · Etsy refuses: the repository save stays, the window closes at once, the toast says it, the card keeps Retry for Etsy only
      await page.evaluate(s => { __kw.steps = s; }, steps.etsyFail);
      await dropDesign(page, '4196630541'); await ready(page, 'ready'); await settle(page);
      await trace(page); await hookClose(page);
      await press(page); await winClosed(page);
      let tr = await traced(page), closeCall = await closeAt(page);
      const savedAt = tr.find(f => f.ph === 'saved'), sheetDoneAt = tr.find(f => f.rows.split(',')[0] === 'done');
      ok(savedAt && savedAt.open && savedAt.steps && savedAt.rows === 'work,done,done,failed' || (savedAt && /^(work|done),done,done,failed$/.test(savedAt.rows)), 'Etsy refused: the strip stands as saved, the repository step done, the Etsy step failed (' + (savedAt && savedAt.rows) + ')');
      ok(sheetDoneAt && closeCall != null && closeCall - sheetDoneAt.t < 600, 'a failure closes at once (no calm moment): ' + (closeCall - (sheetDoneAt || { t: 0 }).t) + ' ms after the Sheet row finished');
      let tt = await toasts(page);
      ok(tt.some(t => /^ACORN_LEAF_4821 saved to the repository\. Etsy not updated: Etsy refused the change\. Paul needs to re-authorise Etsy with listings_w\. Open the order to retry\.$/.test(t)), 'the toast names the step and the reason: ' + tt.join(' | ').slice(0, 220));
      await clickDone(page, '4196630541');
      ok((await noteOf(page, '4196630541')) === 'Saved to the repository · Etsy not updated' && (await page.locator('#rvList [data-rid="4196630541"] [data-cu-keep-retry="etsy"]').count()) === 1, 'the order\'s card keeps one quiet line with Retry for Etsy');
      await page.evaluate(() => { __kw.retrySteps = [{ row: 'etsy', state: 'work', text: 'Setting the SKU on Etsy…' }, 1800, { row: 'etsy', state: 'done', text: 'Linked on Etsy' }, { done: 'linked', sku: 'ACORN_LEAF_4821' }]; });
      await page.click('#rvList [data-rid="4196630541"] [data-cu-keep-retry="etsy"]');
      await page.waitForFunction(() => /Saving to the repository/.test((document.querySelector('#rvList [data-rid="4196630541"] .cuKeepNote') || {}).textContent || '') && document.querySelector('#rvList [data-rid="4196630541"] .cuKeepNote .spin'), null, { timeout: 4000 });
      ok(true, 'while it works the card shows a small spinner and "Saving to the repository…"');
      await page.waitForFunction(() => /Linked · ACORN_LEAF_4821/.test((document.querySelector('#rvList [data-rid="4196630541"] .cuKeepNote') || {}).textContent || ''), null, { timeout: 8000 });
      const c1 = await page.evaluate(() => ({ calls: __kw.calls.slice(), keep: CustomSheet.keepOf(Object.keys(CustomSheet.entries())[0]) }));
      ok(c1.calls.filter(c => c === 'run').length === 1 && c1.calls.includes('retry:etsy') && !c1.calls.includes('retry:repo'), 'Retry ran from the Etsy row only (one run, one retry:etsy): ' + c1.calls.join(' > '));
      ok(c1.keep && c1.keep.rows.sku.state === 'done' && c1.keep.rows.repo.state === 'done' && c1.keep.rows.etsy.state === 'done', 'the rows before the failing one stayed done');
      tt = await toasts(page);
      ok(tt.some(t => /Order 4196630541: ACORN_LEAF_4821 saved to the repository and linked on Etsy/.test(t)), 'the window was gone: a toast says it was linked');

      // C2 · the repository save fails: held, closes at once, toast, the card says Retry (unlocked: Retry, not Save now)
      await tabCustom(page); await page.click('#reviewView .rvSeg [data-cseg="open"]').catch(() => {});
      await page.evaluate(s => { __kw.steps = s; __kw.calls.length = 0; }, steps.repoFail);
      await dropDesign(page, '4196630542'); await ready(page, 'ready'); await settle(page);
      await trace(page); await hookClose(page);
      await press(page); await winClosed(page);
      tr = await traced(page); closeCall = await closeAt(page);
      const heldAt = tr.find(f => f.ph === 'held');
      ok(heldAt && heldAt.open && /^(work|done),failed,done,idle$/.test(heldAt.rows), 'the repository save failed: the strip stands as held, the step failed, Etsy waits (' + (heldAt && heldAt.rows) + ')');
      tt = await toasts(page);
      ok(tt.some(t => /^Order 4196630542: not saved to the repository: the file could not be uploaded\. Open the order to save it\.$/.test(t)), 'the toast: ' + tt.slice(-1)[0]);
      await clickDone(page, '4196630542');
      ok((await noteOf(page, '4196630542')) === 'Not saved to the repository' && (await page.locator('#rvList [data-rid="4196630542"] [data-cu-keep-retry="repo"]').innerText()) === 'Retry', 'the card: "Not saved to the repository" with Retry');
      await page.evaluate(() => { __kw.retrySteps = [{ row: 'repo', state: 'work', text: 'Saving ACORN_LEAF_4821.ai…' }, 300, { row: 'repo', state: 'done', text: 'Saved to the repository' }, { row: 'etsy', state: 'work', text: 'Setting the SKU on Etsy…' }, 300, { row: 'etsy', state: 'done', text: 'Linked on Etsy' }, { done: 'linked', sku: 'ACORN_LEAF_4821' }]; });
      await page.click('#rvList [data-rid="4196630542"] [data-cu-keep-retry="repo"]');
      await page.waitForFunction(() => /Linked · ACORN_LEAF_4821/.test((document.querySelector('#rvList [data-rid="4196630542"] .cuKeepNote') || {}).textContent || ''), null, { timeout: 8000 });
      ok(await page.evaluate(() => __kw.calls.includes('retry:repo') && __kw.calls.filter(c => c === 'run').length === 1), 'Retry from the card ran the repository step again, not the whole save');

      // C3 · the Sheet fails while the save works: the strip stays (Sheet row failed), "Retry send" finishes the Sheet, and the save is not made twice
      await tabCustom(page); await page.click('#reviewView .rvSeg [data-cseg="open"]').catch(() => {});
      await page.evaluate(s => { __kw.steps = s; __kw.calls.length = 0; window.__failSheet = true; const real = Review.repool; Review.repool = async function () { if (window.__failSheet) throw new Error('The sheet is busy, try again'); return real.apply(this, arguments); }; }, steps.linked);
      await dropDesign(page, '4196630543'); await ready(page, 'ready'); await settle(page);
      await press(page);
      await page.waitForFunction(() => document.getElementById('cuKeep').dataset.phase === 'linked' && document.querySelector('#cuKeep .cuKr[data-r="sheet"]').dataset.s === 'failed', null, { timeout: 40000 });
      await page.waitForTimeout(1600); await settle(page);
      let s = await strip(page);
      ok(await page.evaluate(() => document.getElementById('cuDlg').open) && s.steps && !s.line && s.rows[0].s === 'failed' && /sheet is busy/.test(s.rows[0].t), 'the Sheet failed: the window stays open, the steps card stays, the Sheet row says why: ' + s.rows[0].t);
      ok(s.rows[0].b.length === 0 && (await page.locator('#cuDlg [data-send]').innerText()) === 'Retry send', 'the Sheet row has no Retry of its own; the foot says "Retry send"');
      ok(s.rows[1].s === 'done' && s.rows[2].s === 'done' && s.rows[3].s === 'done', 'the save went on beside it and finished');
      await shot(page, '7-sheetfail');
      await page.evaluate(() => { window.__failSheet = false; });
      await page.click('#cuDlg [data-send]');
      await page.waitForFunction(() => document.querySelector('#cuKeep .cuKeepLine') && !document.querySelector('#cuKeep .cuKeepLine').hidden && /^Linked/.test(document.querySelector('#cuKeep .cuKeepLine b').textContent), null, { timeout: 20000 });
      ok(await page.evaluate(() => __kw.calls.filter(c => c === 'run').length === 1 && !__kw.calls.some(c => /^retry/.test(c))), 'Retry send did not save a second time');
      await winClosed(page, 15000);

      // C5 · the save is slower than the wait: the engine is asked to settle for up to 20 s, the window goes on, the card shows it working
      await tabCustom(page); await page.click('#reviewView .rvSeg [data-cseg="open"]').catch(() => {});
      await page.evaluate(s => { __kw.steps = s; __kw.early = true; __kw.settledAsked.length = 0; __kw.calls.length = 0; }, steps.slow);
      await dropDesign(page, '4196630545'); await ready(page, 'ready'); await settle(page);
      await press(page); await winClosed(page);
      ok(await page.evaluate(() => __kw.settledAsked.length === 1 && __kw.settledAsked[0] === 20000), 'the window asked the engine to settle for 20000 ms');
      await clickDone(page, '4196630545');
      ok(/Saving to the repository/.test(await noteOf(page, '4196630545')) && (await page.evaluate(() => !!document.querySelector('#rvList [data-rid="4196630545"] .cuKeepNote .spin'))), 'the wait ran out: the window went back into its card; the card shows the save still working, with a small spinner');
      await page.evaluate(() => { __kw.early = false; });

      // C6 · the engine stops without an ending (a cut reply, a time-out): the working step fails with Retry, nothing hangs, nothing is lost
      await tabCustom(page); await page.click('#reviewView .rvSeg [data-cseg="open"]').catch(() => {});
      await page.evaluate(s => { __kw.steps = s; __kw.calls.length = 0; }, steps.crash);
      await dropDesign(page, '4196630546'); await ready(page, 'ready'); await settle(page);
      await trace(page); await hookClose(page);
      await press(page); await winClosed(page);
      tr = await traced(page);
      const crashAt = tr.find(f => f.ph === 'saved');
      ok(crashAt && crashAt.open && /^(work|done),done,done,failed$/.test(crashAt.rows), 'the engine stopped with no ending: the Etsy step fails, the saved steps stay done (' + (crashAt && crashAt.rows) + ')');
      tt = await toasts(page);
      ok(tt.some(t => /^ACORN_LEAF_4821 saved to the repository\. Etsy not updated: Etsy did not answer in time\. Retry checks the listing first and never writes twice\. Open the order to retry\.$/.test(t)), 'the toast says Etsy did not answer and that Retry never writes twice: ' + tt.slice(-1)[0]);
      await clickDone(page, '4196630546');
      ok((await noteOf(page, '4196630546')) === 'Saved to the repository · Etsy not updated' && (await page.locator('#rvList [data-rid="4196630546"] [data-cu-keep-retry="etsy"]').count()) === 1, 'the card keeps Retry for Etsy');

      // C4 · the switch OFF: today's behaviour, nothing asked of the engine
      await tabCustom(page); await page.click('#reviewView .rvSeg [data-cseg="open"]').catch(() => {});
      await page.evaluate(() => { __kw.calls.length = 0; __kw.settledAsked.length = 0; });
      await dropDesign(page, '4196630544'); await ready(page, 'ready'); await settle(page);
      await page.click('#cuKeepSwitch'); await ready(page, 'off');
      await trace(page); await hookClose(page);
      await press(page); await winClosed(page);
      tr = await traced(page);
      ok(await page.evaluate(() => !__kw.calls.some(c => /^(run|retry)/.test(c)) && __kw.settledAsked.length === 0), 'switch OFF: the engine is not asked to run or settle');
      ok(tr.every(f => !f.steps || f.ph !== 'running') && !tr.some(f => f.ring), 'switch OFF: the four steps never open, no line, no ring');
      await w.close();
    }

    /* ── J · the real engine (charm-nest-keep.js, real charm-nest-sku.js, the stand-in's real handlers over in-memory Firestore): Etsy is a stand-in answer, nothing leaves this machine ── */
    if (LEGACY && want('J')) {
      const w = await mk({ locked: false }); const { page, srv } = w;
      await seed(page, [FRESH(4196630701)]); await startRun(page); await tabCustom(page);
      await page.evaluate(() => { __kw.useReal = true; window.__etsy = []; const real = window.api; window.api = async function (fn, body, o) { if (body && body.op === 'customEtsySkuWrite') { window.__etsy.push(Object.keys(body).sort().join()); return { ok: true, state: 'linked' }; } return real.apply(this, arguments); }; });
      await dropDesign(page, '4196630701'); await ready(page, 'ready'); await settle(page);
      let s = await strip(page);
      const skuShown = (/New SKU (\S+) /.exec(s.preview) || [])[1] || '';
      ok(s.shown && s.on && s.phase === 'ready' && !!skuShown, 'the real engine offers the save for a custom design: ' + s.preview);
      await shot(page, '2b-offered-real');
      await trace(page);
      await press(page);
      await page.waitForFunction(() => ['linked', 'saved', 'held'].includes(document.getElementById('cuKeep').dataset.phase), null, { timeout: 60000 });
      const end = await page.evaluate(() => document.getElementById('cuKeep').dataset.phase);
      await winClosed(page, 30000);
      const tr = await traced(page);
      s = null;
      ok(end === 'linked', 'the real engine, all four steps: the strip ends "' + end + '" (' + JSON.stringify(tr.filter(f => f.ph !== 'ready').slice(0, 14).map(f => f.rows)) + ')');
      ok(await page.evaluate(() => window.__etsy.length === 1 && window.__etsy[0] === 'op,rid'), 'the Etsy step was asked once with the order\'s number alone (op, rid)');
      const keep = await page.evaluate(() => CustomSheet.keepOf(Object.keys(CustomSheet.entries())[0]));
      const idx = srv.st.list('Charm_Master_Index').map(e => e._id);
      ok(keep && keep.sku && idx.includes(keep.sku), 'the design is in the (in-memory) master index under ' + (keep && keep.sku) + ' (' + idx.length + ' SKUs in all)');
      await w.close();
    }

    /* ── J2 · the real engine, Etsy refusing, a reload, and Retry: the saved record decides what the strip says, and nothing is saved twice ── */
    if (LEGACY && want('J2')) {
      const w = await mk({ locked: false }); const { page, srv } = w;
      const ORD = [FRESH(4196630702)];
      const rig = async answer => page.evaluate(answer => { __kw.useReal = true; window.__calls = []; window.__answer = answer; if (!window.__rigged) { window.__rigged = true; const real = window.api; window.api = async function (fn, body, o) { const op = body && body.op; if (/^custom(SaveCommit|SkuCheck|EtsySkuWrite|SaveGet)$/.test(op || '')) window.__calls.push(op); if (op === 'customEtsySkuWrite') return window.__answer; return real.apply(this, arguments); }; } }, answer);
      await seed(page, ORD); await startRun(page); await tabCustom(page);
      await rig({ ok: false, state: 'failed', reason: 'scope', retry: true });
      await dropDesign(page, '4196630702'); await ready(page, 'ready'); await settle(page);
      await press(page);
      await page.waitForFunction(() => document.getElementById('cuKeep').dataset.phase === 'saved', null, { timeout: 60000 });
      await winClosed(page, 30000);
      let calls = await page.evaluate(() => window.__calls.join());
      ok(/customSaveCommit/.test(calls) && (calls.match(/customEtsySkuWrite/g) || []).length === 1 && (calls.match(/customSaveCommit/g) || []).length === 1, 'Etsy refused: the repository was saved once, Etsy asked once: ' + calls);
      const tt1 = await toasts(page);
      ok(tt1.some(t => /saved to the repository\. Etsy not updated: Etsy refused the change\. Paul needs to re-authorise Etsy with listings_w\. Open the order to retry\./.test(t)), 'the toast says why (the engine\'s words): ' + tt1.slice(-1)[0].slice(0, 150));
      // a reload: the page has forgotten everything but the cloud
      await page.reload({ waitUntil: 'load' });
      await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomSheet && CN.S.cloud.ok === true, null, { timeout: 60000 });
      await seed(page, ORD); await startRun(page);
      await rig({ ok: true, state: 'linked' });
      await page.waitForFunction(() => Object.keys(CustomSheet.entries()).length > 0, null, { timeout: 20000 }); await page.waitForTimeout(300);
      await openWin(page, '4196630702');
      await ready(page, 'saved', 20000); await settle(page);
      let s = await strip(page);
      ok(s.steps && s.rows[1].s === 'done' && s.rows[2].s === 'done' && s.rows[3].s === 'failed' && s.rows[3].b.join() === 'Retry' && s.rows[1].b.length === 0 && s.rows[2].b.length === 0 && s.line && /Etsy not updated/.test(s.line.main), 'after a reload the record says it: saved, SKU done, Etsy not updated with Retry on that row only: ' + JSON.stringify(s.rows.map(r => r.s + ':' + r.t)));
      await shot(page, '13-saved-after-reload');
      await page.click('#cuKeep [data-k-retry="etsy"]');
      await page.waitForFunction(() => document.getElementById('cuKeep').dataset.phase === 'linked', null, { timeout: 30000 });
      calls = await page.evaluate(() => window.__calls.join());
      ok(!/customSaveCommit/.test(calls) && /customEtsySkuWrite/.test(calls), 'Retry after the reload asked Etsy again and saved nothing a second time: ' + calls);
      await page.waitForFunction(() => document.querySelector('#cuKeep .cuKeepLine') && !document.querySelector('#cuKeep .cuKeepLine').hidden && /^Linked · /.test(document.querySelector('#cuKeep .cuKeepLine b').textContent), null, { timeout: 8000 });
      ok(true, 'and the rows fold into one "Linked · SKU" line');
      await w.close();
    }

    /* ── R · the save under way, the window waiting for it (the picture of "running"; the window stays for the save, up to 20 s) ── */
    if (LEGACY && want('R')) {
      const w = await mk({ locked: false }); const { page } = w;
      await seed(page, [FRESH(4196630611)]); await startRun(page); await tabCustom(page);
      await page.evaluate(s => { __kw.steps = s; }, steps.slow);
      await dropDesign(page, '4196630611'); await ready(page, 'ready'); await settle(page);
      await press(page);
      await page.waitForFunction(() => [...document.querySelectorAll('#cuKeep .cuKr')].map(r => r.dataset.s).join() === 'done,work,done,idle', null, { timeout: 40000 });
      await page.waitForTimeout(900);
      const s = await strip(page);
      ok(s.phase === 'running' && s.steps && s.busy === 'true' && s.dialogs === 1 && s.rows.every(r => r.vis) && s.rows[1].t === 'Saving ACORN_LEAF_4821.ai…', 'running: the Sheet row done, the repository row working with its words and a spinner, the SKU shown, Etsy waiting: ' + JSON.stringify(s.rows.map(r => r.s + ':' + r.t)));
      ok(await page.evaluate(() => document.getElementById('cuDlg').open && !!document.querySelector('#cuKeep .cuKr[data-s="work"] .spin')), 'the window waits for the save (it is still open) and the working row has its small spinner');
      await shot(page, '4-running');
      await w.close();
    }

    /* ── D · pressed while locked: held, the card says Save now, the strip asks for the password, then it saves ── */
    if (LEGACY && want('D')) {
      const w = await mk(); const { page } = w;
      await seed(page, [FRESH(4196630551)]); await startRun(page); await tabCustom(page);
      await page.evaluate(s => { __kw.steps = s; }, steps.linked);
      await dropDesign(page, '4196630551'); await ready(page, 'off'); await settle(page);
      await page.click('#cuKeepSwitch');
      await page.waitForFunction(() => !document.querySelector('#cuKeep .cuKeepPw').hidden); await settle(page);
      await trace(page); await hookClose(page);
      await press(page); await winClosed(page);
      const tr = await traced(page);
      const heldAt = tr.find(f => f.ph === 'held' && f.steps);
      ok(heldAt && heldAt.open && heldAt.steps && /^(work|done),locked,done,idle$/.test(heldAt.rows), 'pressed locked: the steps open, the repository row is locked, Etsy waits (' + (heldAt && heldAt.rows) + ')');
      ok(await page.evaluate(() => __kw.calls.includes('run') && !__kw.calls.includes('unlock')), 'the engine was asked to run once and nothing was unlocked');
      const tt = await toasts(page);
      ok(tt.some(t => /^Order 4196630551: not saved to the repository: needs the repository password\. Open the order to save it\.$/.test(t)), 'the toast: ' + tt.slice(-1)[0]);
      await clickDone(page, '4196630551');
      ok((await noteOf(page, '4196630551')) === 'Not saved to the repository' && (await page.locator('#rvList [data-rid="4196630551"] [data-cu-keep-retry="repo"]').innerText()) === 'Save now', 'the card: "Not saved to the repository" with Save now');
      await page.click('#rvList [data-rid="4196630551"] [data-cu-keep-retry="repo"]');
      await page.waitForSelector('#cuDlg[open]'); await ready(page, 'held'); await page.waitForTimeout(700);
      let s = await strip(page);
      ok(s.phase === 'held' && s.steps && s.pw && s.rows[1].s === 'locked' && s.rows[1].b.join() === 'Save later' && s.dialogs === 1, 'reopened: held, the repository row locked with "Save later", the password row in the strip, no second pop-up');
      ok(await page.evaluate(() => { const m = document.querySelector('#cuKeep .cuKr[data-r="repo"] .lock'); return !!m && m.getAttribute('data-repo') === 'upload' && m.getBoundingClientRect().width > 6 && /repo|url|svg|data/i.test(getComputedStyle(m).webkitMaskImage || getComputedStyle(m).maskImage || ''); }), 'the padlock is the repository lock\'s own mark (its mask)');
      await shot(page, '6-held');
      await page.click('#cuKeep [data-k-later]'); await ready(page, 'off'); await settle(page);
      s = await strip(page);
      ok(s.phase === 'off' && !s.steps && !s.pw && s.now && !(await page.evaluate(() => !document.getElementById('cuKeepSwitch').hidden)), 'Save later: the steps fold back; the switch row offers "Save now" (the Sheet is sent, the switch is of no use)');
      await page.click('#cuKeep [data-k-now]');
      await page.waitForFunction(() => !document.querySelector('#cuKeep .cuKeepPw').hidden); await settle(page);
      ok(await page.evaluate(() => document.activeElement && document.activeElement.id === 'cuKeepPw'), 'Save now while locked asks for the password in the strip, focus in the field');
      await page.fill('#cuKeepPw', 'wrong-' + Math.random()); await page.keyboard.press('Enter');
      await page.waitForFunction(() => /not right/.test(document.getElementById('cuKeepMsg').textContent), null, { timeout: 8000 });
      ok(await page.evaluate(() => !__kw.calls.slice(1).some(c => c === 'run') || __kw.calls.filter(c => c === 'run').length === 1), 'a wrong password saves nothing');
      await page.fill('#cuKeepPw', REPO_TEST.password); await page.keyboard.press('Enter');
      await page.waitForFunction(() => document.getElementById('cuKeep').dataset.phase === 'linked', null, { timeout: 20000 });
      await page.waitForFunction(() => document.querySelector('#cuKeep .cuKeepLine') && !document.querySelector('#cuKeep .cuKeepLine').hidden, null, { timeout: 8000 });
      s = await strip(page);
      ok(await page.evaluate(() => __kw.calls.filter(c => c === 'run').length === 2) && s.line && /^Linked · ACORN_LEAF_4821$/.test(s.line.main) && !s.pw && s.dialogs === 1, 'the right password: the save runs by itself, the rows fold into "Linked · ACORN_LEAF_4821"');
      ok(!(await page.evaluate(() => [...document.querySelectorAll('#cuDlg input')].some(i => i.value))), 'no password stays in the window');
      await w.close();
    }

    /* ── E · why a save is not offered: one quiet line each ── */
    if (want('E')) {
      const w = await mk({ locked: false }); const { page } = w;
      await seed(page, [FRESH(4196630561), FRESH(4196630562, 1800004822), FRESH(4196630563, 1800004823), FRESH(4196630564, 1800004824), FRESH(4196630565, 1800004825), FRESH(4196630566, 1800004826)]);
      await startRun(page); await tabCustom(page);
      const quiet = async (rid, words, label, extra) => {
        await ready(page, 'quiet'); await settle(page);
        const s = await strip(page);
        ok(s.shown && s.phase === 'quiet' && !s.sw && !s.steps && !s.pw && s.line && s.line.main === words && /quiet/.test(s.line.cls) && s.dialogs === 1, label + ': one quiet line "' + (s.line && s.line.main) + '"');
        if (extra) await extra(s);
      };
      // two designs
      await dropDesign(page, '4196630561');
      await page.evaluate(async dxf => { const it = Review.customItemFor(Object.keys(Object.fromEntries(B.orders.byKey)).find(k => k.startsWith('4196630561_'))); const r = await CharmNestDXF.toPdf(new TextEncoder().encode(dxf), 'x.dxf'); CustomSheet.add(it, [new File([r.bytes], 'Second_Charm.ai')]); }, LEAF_DXF.replace('0.9', '1.4'));
      await page.waitForFunction(() => document.querySelectorAll('#cuDlg[open] .cuFile').length >= 2, null, { timeout: 30000 });
      await quiet('4196630561', 'Saving works for one design at a time.', 'two designs', async () => { await shot(page, '8-quiet'); });
      await closeWin(page);
      // the listing takes the buyer's own design (the engine's code, then its own sentence)
      await page.evaluate(() => { __kw.open = { offered: false, why: 'own' }; });
      await dropDesign(page, '4196630562');
      await quiet('4196630562', "This listing takes the buyer's own design every time, so it is not linked to one charm.", 'own listing (code)');
      await closeWin(page);
      await page.evaluate(() => { __kw.open = { offered: false, why: 'This listing is a custom add-on.' }; });
      await dropDesign(page, '4196630563');
      await quiet('4196630563', 'This listing is a custom add-on.', 'own listing (the engine\'s sentence)');
      await closeWin(page);
      await page.evaluate(() => { __kw.open = { offered: false, why: 'linked', sku: 'KEEP_0042' }; });
      await dropDesign(page, '4196630564');
      await quiet('4196630564', 'This listing is already linked to KEEP_0042.', 'already linked');
      await closeWin(page);
      // offline
      await page.evaluate(() => { __kw.open = {}; });
      await dropDesign(page, '4196630565');
      await closeWin(page);
      await page.evaluate(() => { CN.S.cloud.ok = false; });
      await openWin(page, '4196630565');
      await ready(page, 'off'); await settle(page);
      ok((await strip(page)).sw, 'offline: the switch is still offered (KEEPSAVE: a save asked for offline waits for the connection, see keep-durable-browser.cjs S4)');
      await page.evaluate(() => { CN.S.cloud.ok = true; });
      await closeWin(page);
      // the engine is not on the page: the window is exactly as it was
      await page.evaluate(() => { __kw.noEngine = true; });
      await dropDesign(page, '4196630566');
      await page.waitForTimeout(400);
      const s = await strip(page);
      ok(!s.shown && s.phase === '', 'no engine on the page: no strip, the window is as it was');
      await page.evaluate(() => { __kw.noEngine = false; });
      await w.close();
    }

    /* ── F · the sandbox: a rehearsal, plainly labelled; no password asked even when locked ── */
    if (LEGACY && want('F')) {
      const w = await mk({ sandbox: true }); const { page } = w;
      await seed(page, [FRESH(4196630571)]); await startRun(page); await tabCustom(page);
      await page.evaluate(s => { window.__opsHook(); __kw.steps = s; }, steps.rehearsal);
      await dropDesign(page, '4196630571'); await ready(page, 'ready'); await settle(page);
      let s = await strip(page);
      ok(s.sandbox === '1' && s.pill === 'Sandbox: nothing is saved' && s.on && !s.pw && s.sw, 'the sandbox: the label "Sandbox: nothing is saved", the switch on, the repository password is never asked (' + s.pill + ')');
      ok(await page.evaluate(() => __kw.made[0].sandbox === true), 'the engine is told it is the sandbox');
      await shot(page, '9-sandbox');
      await trace(page);
      await press(page);
      await page.waitForFunction(() => document.getElementById('cuKeep').dataset.phase === 'rehearsed', null, { timeout: 40000 });
      await page.waitForFunction(() => document.querySelector('#cuKeep .cuKeepLine') && !document.querySelector('#cuKeep .cuKeepLine').hidden, null, { timeout: 10000 });
      s = await strip(page);
      ok(s.line && s.line.main === 'Rehearsal complete. Nothing was saved.', 'it ends: "' + (s.line && s.line.main) + '"');
      await winClosed(page);
      const tr = await traced(page);
      ok(tr.some(f => f.ph === 'running' && f.steps), 'the same four steps ran');
      const ops = await page.evaluate(() => window.__ops.map(o => o[1] || o[0]));
      ok(!ops.some(o => /unlock|repo(Save|Upload|Change)|etsy|master(Save|Write)|alias|saveCustom/i.test(o)), 'the strip made no repository, alias, Etsy or unlock call in the sandbox: [' + [...new Set(ops)].join(', ') + ']');
      ok(await page.evaluate(() => !__kw.calls.includes('unlock')), 'no unlock was attempted');
      const tt = await toasts(page);
      ok(!tt.some(t => /not saved to the repository/.test(t)), 'no failure toast for a rehearsal');
      await w.close();
    }

    /* ── G · reduced motion: nothing animated by the strip; the spinner still tells it works ── */
    if (LEGACY && want('G')) {
      const w = await mk({ locked: false, reduced: true }); const { page } = w;
      await seed(page, [FRESH(4196630581)]); await startRun(page); await tabCustom(page);
      await page.evaluate(s => { __kw.steps = s; window.__spin = 0; new MutationObserver(() => { if (document.querySelector('#cuKeep .cuKr[data-s="work"] .cuKd .spin')) window.__spin++; }).observe(document.documentElement, { subtree: true, childList: true, attributes: true }); }, steps.linked);
      await dropDesign(page, '4196630581'); await ready(page, 'ready'); await settle(page);
      ok(await page.evaluate(() => Motion.reduced()), 'the page reads reduced motion');
      await trace(page);
      await press(page);
      await page.waitForFunction(() => document.getElementById('cuKeep').dataset.phase === 'linked', null, { timeout: 40000 });
      await winClosed(page);
      const tr = await traced(page);
      ok(tr.some(f => f.ph === 'running' && f.rowsVis === 4), 'the four rows still show');
      ok((await page.evaluate(() => window.__anim.n)) === 0, 'not one animate() call in the strip');
      ok((await page.evaluate(() => window.__ringSeen)) === 0 && !tr.some(f => f.ring), 'no ring');
      ok((await page.evaluate(() => window.__spin)) > 0, 'a small spinner still shows beside the working row');
      ok(await page.evaluate(() => !document.getAnimations().some(a => /cuKeepRing|cuShake/.test(a.animationName || ''))), 'no ring or shake animation runs');
      await w.close();
    }

    /* ── H · a small screen: stacked rows, 44 px targets, nothing wider than the window ── */
    if (LEGACY && want('H')) {
      const w = await mk({ viewport: { width: 700, height: 940 } }); const { page } = w;
      await seed(page, [FRESH(4196630591), FRESH(4196630592, 1800004822, 'LEAF TWO', 'Custom Leaf Two')]); await startRun(page); await tabCustom(page);
      const fit = () => page.evaluate(() => { const d = document.getElementById('cuDlg'); return d.scrollWidth <= d.clientWidth + 1 && document.documentElement.scrollWidth <= window.innerWidth + 1; });
      await dropDesign(page, '4196630591'); await ready(page, 'off'); await settle(page);
      await page.click('#cuKeepSwitch'); await page.waitForFunction(() => !document.querySelector('#cuKeep .cuKeepPw').hidden); await settle(page);
      const m = await page.evaluate(() => { const r = n => n.getBoundingClientRect(), i = r(document.getElementById('cuKeepPw')), b = r(document.querySelector('#cuKeep [data-k-unlock]')), t = r(document.querySelector('#cuKeepSwitch')); return { i: i.height, iw: i.width, b: b.height, t: t.width + 'x' + t.height }; });
      ok(m.i >= 44 && m.b >= 44, 'the password field and Unlock are at least 44 px tall (' + m.i + ', ' + m.b + ')');
      ok(await fit(), 'locked, switch on: nothing is wider than the window');
      await shot(page, '10-small-locked');
      await page.fill('#cuKeepPw', REPO_TEST.password); await page.keyboard.press('Enter');
      await page.waitForFunction(() => CharmNestRepoLock.unlocked('upload'), null, { timeout: 8000 }); await page.waitForFunction(() => document.querySelector('#cuKeep .cuKeepPw').hidden);
      await page.evaluate(s => { __kw.steps = s; }, steps.etsyFail);
      await press(page); await winClosed(page);
      await tourDone(page); await page.waitForTimeout(300);
      await openWin(page, '4196630591'); await ready(page, 'saved'); await page.waitForTimeout(700);
      const rb = await page.evaluate(() => { const b = document.querySelector('#cuKeep .cuKr[data-r="etsy"] [data-k-retry]'); return b ? b.getBoundingClientRect().height : 0; });
      ok(rb >= 44, 'the Retry button is at least 44 px tall (' + rb + ')');
      const stacked = await page.evaluate(() => [...document.querySelectorAll('#cuKeep .cuKr')].every(r => { const t = r.querySelector('.cuKt').getBoundingClientRect(), d = r.querySelector('.cuKd').getBoundingClientRect(); return d.top >= t.bottom - 2 && d.left < t.left + 40 + 24; }));
      ok(stacked, 'each step\'s words sit under its name');
      ok(await fit(), 'the saved state fits too');
      await shot(page, '11-small-saved');
      await w.close();
    }

    /* ── I · a design saved before: the record decides what the strip says ── */
    if (want('I')) {
      const w = await mk({ locked: false }); const { page } = w;
      await seed(page, [FRESH(4196630601), FRESH(4196630602, 1800004822), FRESH(4196630603, 1800004823), FRESH(4196630604, 1800004824), FRESH(4196630605, 1800004825)]);
      await startRun(page); await tabCustom(page);
      // linked on Etsy before
      await page.evaluate(() => { __kw.open = { saved: { sku: 'ACORN_LEAF_4821', etsy: { state: 'linked' } } }; });
      await dropDesign(page, '4196630601'); await ready(page, 'linked'); await settle(page);
      let s = await strip(page);
      ok(s.shown && s.sw && s.on && !s.steps && s.line && s.line.main === 'Saved to the repository as ACORN_LEAF_4821; future orders of this listing will use it' && /Linked on Etsy listing …4821/.test(s.line.sub) && !(await page.evaluate(() => document.querySelector('#cuKeep .cuKeepLine').classList.contains('cuRing'))), 'saved and linked before (KEEPUI: the switch shows ON, ONE calm line says where it is saved and that later orders use it, without a ring): "' + (s.line && s.line.main) + '"');
      await shot(page, '5-linked'); await closeWin(page);
      // saved, Etsy not updated: the steps with Retry on Etsy only
      await page.evaluate(() => { __kw.open = { saved: { sku: 'ACORN_LEAF_4821', etsy: { state: 'failed', text: 'Etsy refused the change' } }, aliasWorks: false }; });
      await dropDesign(page, '4196630602'); await ready(page, 'saved'); await settle(page);
      s = await strip(page);
      // (KEEPUI: ONE calm line says the one thing that needs a person, with its one button; the rows do not say it again)
      ok(!s.steps && s.line && s.line.main === 'Needs attention: Etsy refused the change' && s.line.btns && s.line.btns.join() === 'Try again', 'saved, Etsy not updated: ONE line "Needs attention: Etsy refused the change" with ONE button, Try again: ' + JSON.stringify(s.line));
      ok(s.line && /^Saved to the repository as ACORN_LEAF_4821, but Etsy was not updated\. Until Etsy has the new SKU, the next order of this listing will still ask\.$/.test(s.line.sub), 'the line says what was saved, and why Etsy matters: ' + (s.line && s.line.sub));
      await shot(page, '12-saved');
      // (KEEPSAVE: a save in the cloud's record that this browser holds no intent for: Retry adopts its Etsy step, which asks the cloud for the Etsy step alone; nothing is built or saved again)
      await page.evaluate(() => { const real = window.api; window.__etsyAsked = []; window.api = async function (fn, body, o) { if (body && body.op === 'customEtsySkuWrite') { window.__etsyAsked.push(body.rid + '/' + body.listingId); return { ok: true, state: 'linked' }; } return real.apply(this, arguments); }; });
      await page.click('#cuKeep .cuKeepLine [data-k-retry="again"]');
      await page.waitForFunction(() => document.getElementById('cuKeep').dataset.phase === 'linked', null, { timeout: 15000 });
      ok(await page.evaluate(() => window.__etsyAsked.length === 1 && !__kw.calls.includes('run') && !__kw.calls.some(c => /^retry/.test(c))), 'Retry in the window asked for the Etsy step only (order and listing), once');
      await closeWin(page);
      // the design is already in the repository (reuse), and the Etsy reading that will not match
      await page.evaluate(() => { __kw.open = { reuse: 'ACORN_LEAF_0007', sku: 'ACORN_LEAF_0007' }; });
      await dropDesign(page, '4196630603'); await ready(page, 'off'); await settle(page);
      s = await strip(page);
      ok(s.label === 'Save to the charm repository and link this listing' && s.preview === 'Already in the repository as ACORN_LEAF_0007 · switch on to link this listing to it', 'the design already in the repository: ' + s.preview);
      await closeWin(page);
      await page.evaluate(() => { __kw.open = { aliasWorks: false }; });
      await dropDesign(page, '4196630604'); await ready(page, 'off'); await settle(page);
      s = await strip(page);
      ok(s.preview === 'New SKU ACORN_LEAF_4821 · switch on to save it; Etsy gets the new SKU too, or the next order will still ask', 'the alias will not carry it: ' + s.preview);
      await closeWin(page);
      // the engine takes time to answer: "Checking this listing…" with a small spinner
      await page.evaluate(() => { __kw.open = {}; __kw.openGap = 1500; });
      await dropDesign(page, '4196630605', 'Test_Charm.ai', false);
      await ready(page, 'opening', 4000);
      s = await strip(page);
      ok(s.line && s.line.main === 'Checking this listing…' && (await page.evaluate(() => !!document.querySelector('#cuKeep .cuKeepLine .spin'))), 'while the engine looks: "Checking this listing…" with a small spinner');
      await ready(page, 'off', 8000);
      await w.close();
    }
  } finally { await browser.close(); }
  console.log(`\nsave-ui: ${n - fails.length}/${n} checks passed`);
  if (fails.length) { console.log('\nFAILED:\n - ' + fails.join('\n - ')); process.exit(1); }
}
main().catch(e => { console.error(e); process.exit(1); });
