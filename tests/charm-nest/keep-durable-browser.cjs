// KEEPSAVE (Paul, 16 Oct 2026): the designs window's switch records a save INTENT that runs by itself; real Chromium on the stand-in site (bridge-server.cjs).
// The keeper (charm-nest-keep.js CharmNestKeep.durable) and the bridge are the REAL ones; the engine is the FAKE of save-ui.cjs (scripted answers) except in scene R,
// which runs the real engine against the stand-in's real handlers. Nothing leaves this machine: every request that is not to the loopback is aborted; no Etsy, no paid call.
//   S1 switch ON starts the save at once (no Send to Sheet), the card says so, a reopened window shows the state and a NEW order's switch is OFF (REVDROP)
//   S2 locked: waiting for the password with the window CLOSED, then the lock opens (here, by the person elsewhere) and it completes by itself
//   S3 a reload in the middle: the intent is found again and completes when the password is given · S4 offline then online · S5 switch OFF / file removed cancel
//   S6 password wrong then right in the strip · R the real engine end to end (nothing saved twice after a reload) · X sandbox: rehearsed, no writing op
//   node tests/charm-nest/keep-durable-browser.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>; skipped without a browser)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('skipped: no playwright-core (' + pwDir + ')'); process.exit(0); }
const exe = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
if (!fs.existsSync(exe)) { console.log('skipped: no browser at ' + exe); process.exit(0); }
const { start, REPO_TEST } = require('./bridge-server.cjs');
const SHOTS = process.env.SHOTS_DIR || '';
const ONLY = (process.env.ONLY || '').split(',').filter(Boolean), want = k => !ONLY.length || ONLY.includes(k);
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
      steps: vis(h.querySelector('.cuKeepSteps')), rows, line: vis(line) ? { main: line.querySelector('b').textContent, sub: line.querySelector('.sub').textContent, cls: line.className } : null, dialogs: document.querySelectorAll('dialog[open]').length, busy: h.getAttribute('aria-busy') };
  });
  const settle = page => page.evaluate(() => Promise.race([Promise.all(document.getAnimations().filter(a => { try { return a.effect.getComputedTiming().iterations !== Infinity; } catch (_) { return true; } }).map(a => a.finished.catch(() => null))), new Promise(r => setTimeout(r, 3000))]).then(() => new Promise(r => setTimeout(r, 60))));
  const ready = (page, phase, ms = 8000) => page.waitForFunction(p => { const h = document.getElementById('cuKeep'); return h && h.dataset.phase === p; }, phase, { timeout: ms });

  try {
    const FRESH = (rid, listing, sku, title) => order(rid, listing || 1800004821, sku || 'ACORN&#39;S LEAF', title || "Custom Charm - Acorn's Leaf, Personalized");
    const tabCustom = page => page.click('#reviewView .egTab[data-k="customOrder"]');
    const openWin = async (page, rid) => { await page.evaluate(rid => { const it = Review.customItemFor(Object.keys(Object.fromEntries(B.orders.byKey)).find(k => k.startsWith(rid + '_'))); CustomSheet.open(it, {}); }, rid); await page.waitForSelector('#cuDlg[open]'); };
    const note = (page, rid) => page.evaluate(rid => { const n = document.querySelector(`#rvList .reviewListRow[data-rid="${rid}"] .cuKeepNote`); return n ? { text: n.textContent.replace(/\s+/g, ' ').trim(), state: n.dataset.keepState || '' } : null; }, rid);
    const noteIs = (page, rid, re, ms = 20000) => page.waitForFunction(([rid, src]) => { const n = document.querySelector(`#rvList .reviewListRow[data-rid="${rid}"] .cuKeepNote`); return !!n && new RegExp(src).test(n.textContent); }, [rid, re.source], { timeout: ms });
    const intents = page => page.evaluate(() => Object.values(JSON.parse(localStorage.getItem('cn.keepIntents.v1') || '{"intents":{}}').intents));
    const runs = page => page.evaluate(() => __kw.calls.filter(c => c === 'run').length);
    const unlockOutside = page => page.evaluate(async pw => { const r = await CharmNestRepoLock.submitInline('upload', pw, { target: 'test' }); return r.ok; }, REPO_TEST.password);
    const reloadPage = async (page, orders) => {
      await page.reload({ waitUntil: 'load' });
      await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomSheet && CN.S.cloud.ok === true, null, { timeout: 60000 });
      await seed(page, orders); await startRun(page); await tabCustom(page);
    };

    /* ── S1 · the switch ON starts the save at once (unlocked); the window closed, the card says it; a reopened window shows it; a new order's switch is OFF ── */
    if (want('S1')) {
      const w = await mk({ locked: false }); const { page } = w;
      await seed(page, [FRESH(4196630801), FRESH(4196630802, 1800004822, 'LEAF TWO', 'Custom Leaf Two')]); await startRun(page); await tabCustom(page);
      await page.evaluate(s => { __kw.steps = s; }, steps.linked);
      await dropDesign(page, '4196630801'); await ready(page, 'off'); await settle(page);
      ok(await runs(page) === 0, 'S1: a design dropped, the switch OFF by default: nothing runs');
      await page.click('#cuKeepSwitch');
      await page.waitForFunction(() => __kw.calls.includes('run'), null, { timeout: 15000 });
      ok(await page.evaluate(() => !document.querySelector('#cuDlg [data-send]').classList.contains('working')), 'S1: the save started although Send to Sheet was never pressed');
      const rec0 = (await intents(page))[0];
      ok(rec0 && rec0.rid === '4196630801' && rec0.hash && rec0.state !== 'off' && rec0.key.startsWith('live|4196630801|1800004821|'), 'S1: the intent is in browser storage (order + listing + file hash): ' + (rec0 && rec0.key));
      await page.waitForFunction(() => document.getElementById('cuKeep').dataset.phase === 'linked', null, { timeout: 30000 });
      await closeWin(page);
      await noteIs(page, '4196630801', /Saved and linked · ACORN_LEAF_4821/);
      ok((await note(page, '4196630801')).state === 'linked', 'S1: the card says "' + (await note(page, '4196630801')).text + '"');
      ok(await runs(page) === 1, 'S1: the engine ran once');
      await openWin(page, '4196630801'); await ready(page, 'linked'); await settle(page);
      const s = await strip(page);
      ok(s.phase === 'linked' && !s.sw, 'S1: reopened, the window shows the finished save (not a switch back at OFF): ' + s.phase);
      await closeWin(page);
      await dropDesign(page, '4196630802'); await ready(page, 'off'); await settle(page);
      ok((await strip(page)).phase === 'off', 'S1: another order\'s window opens with the switch OFF (REVDROP)');
      await w.close();
    }

    /* ── S2 · locked: the switch ON, the window CLOSED, the password given elsewhere: it completes by itself ── */
    if (want('S2')) {
      const w = await mk(); const { page } = w;
      await seed(page, [FRESH(4196630811)]); await startRun(page); await tabCustom(page);
      await page.evaluate(s => { __kw.steps = s; }, steps.linked);
      await dropDesign(page, '4196630811'); await ready(page, 'off'); await settle(page);
      await page.click('#cuKeepSwitch');
      await page.waitForFunction(() => document.getElementById('cuKeep').dataset.phase === 'waiting', null, { timeout: 15000 });
      let st = await strip(page);
      ok(st.pw && st.dialogs === 1, 'S2: locked, switch ON: the password row opens in the strip, no second pop-up');
      await closeWin(page);
      await noteIs(page, '4196630811', /Waiting for the repository password/);
      ok((await note(page, '4196630811')).state === 'password' && await runs(page) === 0, 'S2: the window closed: the card says it waits for the password, nothing ran');
      await page.waitForTimeout(1500);
      ok(await runs(page) === 0, 'S2: it keeps waiting (no run, no error)');
      ok(await unlockOutside(page), 'S2: the password is given (here by the person in another place)');
      await noteIs(page, '4196630811', /Saved and linked/, 30000);
      ok(await runs(page) === 1, 'S2: the moment the lock opened it completed by itself, ONE run, nobody pressed anything');
      ok(!(await toasts(page)).some(t => /not saved|Needs attention/i.test(t)), 'S2: no failure toast');
      await w.close();
    }

    /* ── S3 · a reload in the middle: the intent comes back ── */
    if (want('S3')) {
      const w = await mk(); const { page } = w; const ORD = [FRESH(4196630821)];
      await seed(page, ORD); await startRun(page); await tabCustom(page);
      await dropDesign(page, '4196630821'); await ready(page, 'off'); await settle(page);
      await page.click('#cuKeepSwitch');
      await page.waitForFunction(() => document.getElementById('cuKeep').dataset.phase === 'waiting', null, { timeout: 15000 });
      await page.evaluate(() => CustomSheet.shut()); await page.waitForTimeout(300);
      ok((await intents(page)).length === 1, 'S3: one intent stored before the reload');
      await reloadPage(page, ORD);
      await page.waitForFunction(() => Object.keys(CustomSheet.entries()).length > 0, null, { timeout: 20000 });
      await noteIs(page, '4196630821', /Waiting for the repository password/);
      ok(await page.evaluate(() => CustomSheet.keepIntents().length === 1 && CustomSheet.keepIntents()[0].state === 'waiting'), 'S3: after the reload the card shows the save still waiting for the password (from the stored intent)');
      await page.evaluate(s => { __kw.steps = s; }, steps.linked);
      ok(await unlockOutside(page), 'S3: the password given after the reload');
      await noteIs(page, '4196630821', /Saved and linked/, 40000);
      ok(await runs(page) === 1, 'S3: the save the person asked for before the reload completed after it, once');
      await w.close();
    }

    /* ── S4 · offline, then online ── */
    if (want('S4')) {
      const w = await mk({ locked: false }); const { page } = w; 
      await seed(page, [FRESH(4196630831)]); await startRun(page); await tabCustom(page);
      await page.evaluate(s => { __kw.steps = s; }, steps.linked);
      await dropDesign(page, '4196630831'); await ready(page, 'off'); await settle(page);
      await page.evaluate(() => { CN.S.cloud.ok = false; });
      await page.click('#cuKeepSwitch');
      await closeWin(page);
      await noteIs(page, '4196630831', /Waiting for the connection/);
      ok((await note(page, '4196630831')).state === 'connection' && await runs(page) === 0, 'S4: offline: the card says it waits for the connection, nothing ran');
      await page.evaluate(() => { CN.S.cloud.ok = true; window.dispatchEvent(new Event('online')); });
      await noteIs(page, '4196630831', /Saved and linked/, 30000);
      ok(await runs(page) === 1, 'S4: back online it completed by itself, once');
      await w.close();
    }

    /* ── S5 · the switch OFF, and the file removed, cancel a save that is not in the repository ── */
    if (want('S5')) {
      const w = await mk(); const { page } = w;
      await seed(page, [FRESH(4196630841), FRESH(4196630842, 1800004822, 'LEAF TWO', 'Custom Leaf Two')]); await startRun(page); await tabCustom(page);
      await page.evaluate(s => { __kw.steps = s; }, steps.linked);
      await dropDesign(page, '4196630841'); await ready(page, 'off'); await settle(page);
      await page.click('#cuKeepSwitch'); await page.waitForFunction(() => document.getElementById('cuKeep').dataset.phase === 'waiting');
      await page.click('#cuKeepSwitch'); await ready(page, 'off');
      ok((await intents(page))[0].state === 'off', 'S5: switch OFF while it waits: the intent is off');
      await closeWin(page);
      ok(await unlockOutside(page), 'S5: password given later');
      await page.waitForTimeout(2500);
      ok(await runs(page) === 0 && !(await note(page, '4196630841')), 'S5: nothing was saved and the card has no line (nothing saved)');
      // the file taken off the order cancels a waiting save
      await page.evaluate(() => CharmNestRepoLock.forget());
      await dropDesign(page, '4196630842'); await ready(page, 'off'); await settle(page);
      await page.click('#cuKeepSwitch'); await page.waitForFunction(() => document.getElementById('cuKeep').dataset.phase === 'waiting');
      await page.click('#cuDlg .cuFile [data-rm]');
      await page.waitForFunction(() => { const r = CustomSheet.keepIntents().find(x => x.rid === '4196630842'); return r && r.state === 'off'; }, null, { timeout: 8000 });
      ok(true, 'S5: the file removed from the order switched its waiting save off');
      await w.close();
    }

    /* ── R · the real engine, the stand-in's real handlers: switch ON, nothing else, and a reload saves nothing twice ── */
    if (want('R')) {
      const w = await mk({ locked: false }); const { page, srv } = w; const ORD = [FRESH(4196630851)];
      await seed(page, ORD); await startRun(page); await tabCustom(page);
      await page.evaluate(() => { __kw.useReal = true; window.__etsy = []; const real = window.api; window.api = async function (fn, body, o) { if (body && body.op === 'customEtsySkuWrite') window.__etsy.push(Object.keys(body).filter(k => k !== 'sandbox').join()); return real.apply(this, arguments); }; });
      await dropDesign(page, '4196630851'); await ready(page, 'off'); await settle(page);
      await page.click('#cuKeepSwitch');
      await page.waitForFunction(() => ['linked', 'saved', 'held'].includes(document.getElementById('cuKeep').dataset.phase), null, { timeout: 90000 }).catch(async e => { console.log('DBG', JSON.stringify(await page.evaluate(() => ({ phase: document.getElementById('cuKeep').dataset.phase, intents: CustomSheet.keepIntents().map(r => ({ state: r.state, wait: r.wait, why: r.why, fails: r.fails, sku: r.sku, repo: r.repo })), online: CN.S.cloud.ok, unlocked: CharmNestRepoLock.unlocked('upload'), rows: CustomSheet.keepOf(Object.keys(CustomSheet.entries())[0]) && CustomSheet.keepOf(Object.keys(CustomSheet.entries())[0]).rows }))).slice(0, 1800)); throw e; });
      const end = await page.evaluate(() => document.getElementById('cuKeep').dataset.phase);
      ok(end === 'linked', 'R: the real engine, switch ON and nothing else: the strip ends "' + end + '"');
      const idx = srv.st.list('Charm_Master_Index').map(e => e._id);
      const sku = (await intents(page))[0].sku;
      ok(sku && idx.includes(sku), 'R: the design is in the (in-memory) master index under ' + sku);
      ok(await page.evaluate(() => window.__etsy.length === 1 && /^op,rid/.test(window.__etsy[0])), 'R: the Etsy step was asked once');
      const files = srv.st.list ? idx.length : 0;
      await reloadPage(page, ORD);
      await page.waitForFunction(() => Object.keys(CustomSheet.entries()).length > 0, null, { timeout: 20000 });
      await page.waitForTimeout(2500);
      ok((await srv.st.list('Charm_Master_Index')).length === idx.length, 'R: after a reload nothing more is written to the repository (' + idx.length + ' SKUs)');
      ok((await intents(page))[0].state === 'done', 'R: the finished save is still recorded as done after the reload');
      await w.close();
    }

    /* ── X · the sandbox rehearses: it never writes the real repository ── */
    if (want('X')) {
      const w = await mk({ sandbox: true }); const { page } = w;
      await seed(page, [FRESH(4196630861)]); await startRun(page); await tabCustom(page);
      await page.evaluate(s => { window.__opsHook(); __kw.steps = s; }, steps.rehearsal);
      await dropDesign(page, '4196630861'); await ready(page, 'off'); await settle(page);
      await page.click('#cuKeepSwitch');
      await page.waitForFunction(() => document.getElementById('cuKeep').dataset.phase === 'rehearsed', null, { timeout: 30000 });
      const r = (await intents(page))[0];
      ok(r.sandbox === true && r.key.startsWith('sbx|') && r.outcome === 'rehearsed', 'X: a sandbox intent has its own key and ends rehearsed: ' + r.key.slice(0, 12));
      const ops = await page.evaluate(() => window.__ops.map(o => o[1] || o[0]));
      ok(!ops.some(o => /unlock|repo(Save|Upload|Change)|etsy|master(Save|Write)|alias|saveCustom/i.test(o)), 'X: no repository, alias, Etsy or unlock call in the sandbox');
      await w.close();
    }
  } finally { await browser.close(); }
  console.log(`\nkeep-durable-browser: ${n - fails.length}/${n} checks passed`);
  if (fails.length) { console.log('\nFAILED:\n - ' + fails.join('\n - ')); process.exit(1); }
}
main().catch(e => { console.error(e); process.exit(1); });
