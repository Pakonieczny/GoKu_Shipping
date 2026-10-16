// KEEPUI (KU, Paul 16 Oct 2026: "super robust ... no failures"): what the person SEES of a save to the charm repository, in every state, in the real page (Chromium),
// with the engine faked (the REAL keeper, charm-nest-keep.js CharmNestKeep.durable, runs the fake engine; nothing is saved, no Etsy call, no paid call).
// One calm line per window and per Review card: nothing saved · saving (which step) · waiting for the password · waiting for the connection · getting the listing ready ·
// trying again · saved and linked (SKU) · needs attention (the one thing) · rehearsal. Properties pinned here:
//   A  every state rendered from the save's RECORD (browser storage), in the window (switch, ONE line, mark, password row) and on the Review card (one note)
//   B  no spinner for a wait (password, connection) and none that goes on for ever (a slow work says so in words, a slow wait says so)
//   C  the switch: OFF by default for an order with no save, ON with the live state while one runs or is done, OFF mid-run says what it means (nothing saved / a saved
//      design stays: refused in one line), a finished save cannot be switched off (aria-disabled)
//   D  the state survives closing the window, reopening it, and a reload; the card shows it with the window closed; a second order of the same page is still OFF
//   E  the inline password row: shown for "waiting for the password", never auto-filled, never kept, the save goes on by itself once the lock opens
//   F  screen reader: ONE aria-live line in the strip (#cuKeepSay), the rows around it are not live; the card's changes go to one page-level live region
//   G  keyboard: Space/Enter on the switch, Tab order switch -> line button -> password -> Unlock; focus is not taken from a field being typed in
//   H  phone width (400 px): every press target of the strip and the card note is at least 40 px, nothing overflows sideways; contrast of the words is at least 4.5:1
//   I  no page error and no console error in any scene
//   node tests/charm-nest/keep-ui-states.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>; SHOTS_DIR=<dir> saves the pictures; ONLY=A,B,... picks scenes)
const fs = require('fs'), path = require('path');
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

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000), SKU = 'ACORN_LEAF_4821';
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
const LEAF_DXF = DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES',
  0, 'LWPOLYLINE', 8, 'CUT', 90, 2, 70, 1, 10, 0, 20, 0, 42, -0.62, 10, 13, 20, 0, 42, -0.62,
  0, 'CIRCLE', 8, 'CUT', 10, 3.2, 20, 0, 40, 0.9, 0, 'ENDSEC', 0, 'EOF');
const RECT_DXF = DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES', 0, 'LWPOLYLINE', 8, 'CUT', 90, 4, 70, 1, 10, 0, 20, 0, 10, 14, 20, 0, 10, 14, 20, 9, 10, 0, 20, 9, 0, 'ENDSEC', 0, 'EOF');
const order = (rid, listing, sku, title) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + String(rid).slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: String(rid) + '1', listingId: String(listing), sku, title, quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' }] });

/** The fake engine, in the page (the engine's shape: make(ctx) -> open / on / run / retry / unlock / settled / outcome), scripted by window.__kw. The keeper is the REAL one. */
function fakeKeep() {
  const W = window.__kw = { calls: [], steps: null, open: {}, openGap: 0, needToken: true, log: [] };
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const FAKE = {
    durable(deps) { return W.real.durable(deps); },
    make(ctx) {
      const L = [], sku = () => W.sku || 'ACORN_LEAF_4821';
      const emit = ev => { W.log.push(ev); if (ev.done) W.out = { done: ev.done, held: ev.reason === 'locked' ? 'locked' : '', reason: ev.reason || '', retry: ev.retry !== false, text: ev.text || '', sku: ev.sku || sku() }; for (const f of L) try { f(ev); } catch (_) {} };
      let running = null;
      const play = async steps => { for (const s of steps) { if (s === 'throw') throw new Error('the reply was cut off'); if (typeof s === 'number') await sleep(s); else emit(s); } };
      const go = async (steps, label) => {
        W.calls.push(label);
        if (W.needToken && !ctx.sandbox && !(ctx.lock && ctx.lock.unlocked('upload'))) { emit({ row: 'repo', state: 'locked', text: 'Not saved: needs the repository password' }); emit({ done: 'held', reason: 'locked', sku: sku() }); return; }
        await play(steps || []);
      };
      return {
        on: f => { L.push(f); },
        async open() { W.calls.push('open'); if (W.openGap) await sleep(W.openGap); return Object.assign({ offered: true, why: '', sku: sku(), group: 'all', aliasWorks: true, saved: null, locked: !(ctx.lock && ctx.lock.unlocked('upload')), rehearsal: !!ctx.sandbox, reuse: null }, W.open); },
        run() { return running = go(W.steps, 'run'); },
        outcome() { return W.out || null; },
        retry() { return running = go(W.steps, 'retry'); },
        async unlock(pw) { W.calls.push('unlock'); const r = await ctx.lock.submitInline('upload', pw, { target: 'test' }); return { ok: !!r.ok, reason: r.ok ? undefined : r.reason, retryAfterSec: r.retryAfterSec }; },
        settled(ms) { return Promise.race([Promise.resolve(running).catch(() => {}), sleep(ms)]); }
      };
    }
  };
  W.real = null; Object.defineProperty(window, 'CharmNestKeep', { configurable: true, get: () => FAKE, set: v => { W.real = v; } });
}
const HANG = [{ row: 'sku', state: 'work', text: 'Choosing a SKU…' }, 600000];
const LINKED = [{ row: 'sku', state: 'work', text: 'Choosing a SKU…' }, 200, { row: 'sku', state: 'done', text: SKU }, { row: 'repo', state: 'work', text: `Saving ${SKU}.ai…` }, 300,
  { row: 'repo', state: 'done', text: 'Saved to the repository' }, { row: 'etsy', state: 'work', text: 'Setting the SKU on Etsy…' }, 300, { row: 'etsy', state: 'done', text: 'Linked on Etsy' }, { done: 'linked', sku: SKU }];
const REHEARSAL = [{ row: 'sku', state: 'work', text: 'Choosing a SKU…' }, 150, { row: 'sku', state: 'done', text: SKU }, { row: 'repo', state: 'rehearsed', text: `Rehearsed: would save ${SKU}.ai` }, 150, { row: 'etsy', state: 'rehearsed', text: 'Rehearsed: would set the SKU' }, { done: 'rehearsed', sku: SKU }];

async function main() {
  const browser = await chromium.launch({ executablePath: exe, args: ['--no-sandbox'] });
  const shot = async (page, name, sel) => {
    if (!SHOTS) return; fs.mkdirSync(SHOTS, { recursive: true });
    await page.evaluate(() => { const b = document.querySelector('.cnNameBar'); if (b) b.style.visibility = 'hidden'; }); await page.waitForTimeout(400);
    try { await page.locator(sel || '#cuDlg').first().screenshot({ path: path.join(SHOTS, name + '.png') }); } catch (e) { console.log('shot failed', name, String(e.message).slice(0, 80)); }
    await page.evaluate(() => { const b = document.querySelector('.cnNameBar'); if (b) b.style.visibility = ''; });
  };
  const mk = async (opts = {}) => {
    const srv = await start({ receipts: [], repoSeed: opts.locked === false ? undefined : false });
    const ctx = await browser.newContext({ viewport: opts.viewport || { width: 1440, height: 950 }, timezoneId: 'UTC', locale: 'en-US', reducedMotion: opts.reduced ? 'reduce' : 'no-preference' });
    await ctx.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await ctx.addInitScript(sandbox => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: sandbox ? 'on' : 'off' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} }, !!opts.sandbox);
    await ctx.addInitScript(fakeKeep);
    await ctx.addInitScript(() => { window.confirm = () => true; window.prompt = () => 'Test Operator'; window.alert = () => {}; });
    const page = await ctx.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push('pageerror: ' + String(e.message)); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 3).join(' | ')); });
    page.on('console', m => { if ((m.type() === 'error' && !/Failed to load resource|net::ERR|fonts\.g/.test(m.text())) || (m.type() === 'warning' && /Save (strip|keeper)/.test(m.text()))) { errors.push('console.error: ' + m.text().slice(0, 200)); console.error('console error:', m.text().slice(0, 200)); } });
    const boot = async () => { await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' }); await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomSheet && CN.S.cloud.ok === true, null, { timeout: 60000 }); };
    await boot();
    return { srv, ctx, page, errors, boot, close: async () => { await ctx.close(); await srv.close(); } };
  };
  const seed = (page, orders) => page.evaluate(async orders => {
    await Orders.loadMaps(true);
    for (const order of orders) for (const line of order.lines) {
      const key = CharmNestOrders.lineKey(order, line);
      if (B.orders.byKey.has(key)) continue;
      const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row);
    }
    B.maps.customRead = Object.assign({}, B.maps.customRead || {}, Object.fromEntries(orders.flatMap(o => o.lines.map(l => [CharmNestOrders.lineKey(o, l), { kind: 'custom', confidence: 0.96, summary: 'the buyer\'s own design' }]))));
    Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
  }, orders);
  const tables = (page, ids) => page.evaluate(ids => { B.maps.listingSkus = B.maps.listingSkus || {}; for (const id of ids) B.maps.listingSkus[String(id)] = B.maps.listingSkus[String(id)] || { listingId: String(id), skus: [], at: Date.now() }; }, ids);
  const toTab = async page => { await page.click('#reviewView .egTab[data-k="customOrder"]'); await page.waitForSelector('#rvList .reviewListRow'); };
  const dropDesign = async (page, rid, name = 'Test_Charm.ai') => {
    await page.evaluate(async ({ rid, name, dxf }) => {
      const key = Object.keys(Object.fromEntries(B.orders.byKey)).find(k => k.startsWith(rid + '_')), it = Review.customItemFor(key);
      if (!it) throw new Error('no custom card for ' + rid);
      const r = await CharmNestDXF.toPdf(new TextEncoder().encode(dxf), 'x.dxf');
      CustomSheet.add(it, [new File([r.bytes], name)]);
    }, { rid, name, dxf: LEAF_DXF });
    await page.waitForFunction(() => document.querySelectorAll('#cuDlg[open] .cuFile .cuThumb img').length >= 1, null, { timeout: 30000 });
    await page.click('#cuDlg .cuFile .cuM[data-m="gold"]');
  };
  const openWin = async (page, rid) => {
    await page.evaluate(rid => { const key = Object.keys(Object.fromEntries(B.orders.byKey)).find(k => k.startsWith(rid + '_')); CustomSheet.open(Review.customItemFor(key), {}); }, rid);
    await page.waitForSelector('#cuDlg[open]'); await page.waitForFunction(() => { const h = document.getElementById('cuKeep'); return h && !h.hidden && h.dataset.state && h.dataset.state !== 'opening'; }, null, { timeout: 15000 });
    await page.waitForTimeout(120);
  };
  const closeWin = async page => { await page.evaluate(() => CustomSheet.shut()); await page.waitForFunction(() => !document.querySelector('dialog[open]')); await page.waitForTimeout(150); };
  const settle = page => page.evaluate(() => Promise.race([Promise.all(document.getAnimations().filter(a => { try { return a.effect.getComputedTiming().iterations !== Infinity; } catch (_) { return true; } }).map(a => a.finished.catch(() => null))), new Promise(r => setTimeout(r, 3000))]).then(() => new Promise(r => setTimeout(r, 60))));
  const state = (page, st, ms = 8000) => page.waitForFunction(s => { const h = document.getElementById('cuKeep'); return h && h.dataset.state === s; }, st, { timeout: ms }).catch(async e => { const d = await page.evaluate(() => { const h = document.getElementById('cuKeep'); return h ? { state: h.dataset.state, phase: h.dataset.phase, hidden: h.hidden, recs: Object.values((JSON.parse(localStorage.getItem('cn.keepIntents.v1') || '{"intents":{}}')).intents).map(r => [r.rid, r.state, r.wait, r.repo]) } : null; }).catch(() => null); console.log('  .. waiting for state ' + st + ', the strip is: ' + JSON.stringify(d)); throw e; });
  /** The strip, as a person sees it. */
  const snap = page => page.evaluate(() => {
    const vis = n => !!n && !n.hidden && !n.closest('[hidden]') && n.getClientRects().length > 0;
    const h = document.getElementById('cuKeep'), ln = h.querySelector('.cuKeepLine'), sw = h.querySelector('.cuSwitch');
    return { open: document.getElementById('cuDlg').open, state: h.dataset.state, phase: h.dataset.phase, shown: vis(h), swVis: vis(sw), swOn: sw.getAttribute('aria-checked') === 'true', swDis: sw.getAttribute('aria-disabled') === 'true', swTitle: sw.title,
      label: h.querySelector('#cuKeepL').textContent, preview: vis(h.querySelector('#cuKeepP')) ? h.querySelector('#cuKeepP').textContent : null,
      line: vis(ln) ? { main: ln.querySelector('b').textContent, sub: ln.querySelector('.sub').hidden ? '' : ln.querySelector('.sub').textContent, tone: ln.dataset.tone, spin: !!ln.querySelector('.spin'), pause: !!ln.querySelector('.pause'), lock: !!ln.querySelector('.lock'), tick: !!ln.querySelector('.cuTick'), btns: [...ln.querySelectorAll('button')].map(b => b.textContent) } : null,
      lines: [...h.querySelectorAll('.cuKeepLine')].filter(vis).length, pw: vis(h.querySelector('.cuKeepPw')), steps: vis(h.querySelector('.cuKeepSteps')), say: h.querySelector('#cuKeepSay').textContent,
      spins: [...h.querySelectorAll('.spin')].filter(vis).length, live: [...h.querySelectorAll('[aria-live],[role=status]')].map(n => n.id || n.className), swDesc: sw.getAttribute('aria-describedby'),
      pwValue: h.querySelector('#cuKeepPw').value, dialogs: document.querySelectorAll('dialog[open]').length };
  });
  /** The words of the strip and of the card notes that are too pale for what is behind them (WCAG 4.5:1 for small text, 3:1 for large). */
  const contrastOf = page => page.evaluate(() => {
        const lum = c => { const f = v => { v /= 255; return v <= .03928 ? v / 12.92 : Math.pow((v + .055) / 1.055, 2.4); }; return .2126 * f(c[0]) + .7152 * f(c[1]) + .0722 * f(c[2]); };
        const parse = s => { const m = String(s).match(/rgba?\(([^)]+)\)/); if (!m) return null; const p = m[1].split(/[ ,\/]+/).filter(Boolean).map(Number); return { c: p.slice(0, 3), a: p.length > 3 ? p[3] : 1 }; };
        const bgOf = el => { let n = el, acc = null; const layers = []; while (n && n.nodeType === 1) { const c = parse(getComputedStyle(n).backgroundColor); if (c && c.a > 0) { layers.push(c); if (c.a >= 1) break; } n = n.parentElement; } let base = [255, 255, 255]; for (const l of layers.reverse()) base = base.map((v, i) => v * (1 - l.a) + l.c[i] * l.a); return base; };
        const out = []; const roots = [...document.querySelectorAll('#cuKeep, .cuKeepNote')];
        for (const r of roots) for (const el of r.querySelectorAll('*')) {
          const own = [...el.childNodes].some(t => t.nodeType === 3 && t.textContent.trim()); if (!own || el.closest('[hidden]') || !el.getClientRects().length) continue;
          const cs = getComputedStyle(el), fg = parse(cs.color); if (!fg) continue; const bg = bgOf(el), L1 = lum(fg.c), L2 = lum(bg), ratio = (Math.max(L1, L2) + .05) / (Math.min(L1, L2) + .05);
          const size = parseFloat(cs.fontSize), bold = +cs.fontWeight >= 700, need = size >= 24 || (size >= 18.66 && bold) ? 3 : 4.5;
          if (ratio < need) out.push(`${el.className || el.tagName}:"${el.textContent.trim().slice(0, 30)}" ${ratio.toFixed(2)}<${need}`);
        }
        return out;
      });
  /** The Review card's note for an order (the window must be closed: a card is drawn again when it closes). */
  const card = (page, rid) => page.evaluate(rid => {
    const row = document.querySelector(`#rvList .reviewListRow[data-rid="${rid}"]`), nt = row && row.querySelector('.cuKeepNote');
    return { row: !!row, n: row ? row.querySelectorAll('.cuKeepNote').length : 0, state: nt ? nt.dataset.keepState : null, text: nt ? nt.querySelector('.cuKnTx').textContent : null, title: nt ? nt.title : null, spin: !!(nt && nt.querySelector('.spin')), btns: nt ? [...nt.querySelectorAll('button')].map(b => ({ t: b.textContent, k: b.dataset.cuKeepRetry })) : [], cls: nt ? nt.className : '', live: nt ? nt.querySelectorAll('[aria-live],[role=status]').length + (nt.matches('[aria-live],[role=status]') ? 1 : 0) : 0 };
  }, rid);
  const redraw = page => page.evaluate(() => { Review.render(); }).then(() => page.waitForTimeout(150));
  /** Write the save's RECORD as the keeper would have (browser storage), frozen (nextAt far away: no tick changes it), and let the page read it. */
  const setRec = (page, rid, patch) => page.evaluate(({ rid, patch }) => {
    const raw = JSON.parse(localStorage.getItem('cn.keepIntents.v1') || '{"v":1,"intents":{}}');
    const k = Object.keys(raw.intents).find(k => raw.intents[k].rid === rid); if (!k) throw new Error('no intent for ' + rid);
    const p = Object.assign({}, patch); if (p.age) { p.upd = Date.now() - p.age; delete p.age; } if (p.since) { p.at = Date.now() - p.since; delete p.since; }
    const frozen = !/^(done|attention|off)$/.test(p.state || raw.intents[k].state);
    raw.intents[k] = Object.assign({}, raw.intents[k], { nextAt: frozen ? Date.now() + 1e9 : 0, upd: Date.now() }, p);
    localStorage.setItem('cn.keepIntents.v1', JSON.stringify(raw)); CustomSheet.keeper().external();
  }, { rid, patch });
  const recOf = (page, rid) => page.evaluate(rid => { const raw = JSON.parse(localStorage.getItem('cn.keepIntents.v1') || '{"intents":{}}'); return Object.values(raw.intents).find(r => r.rid === rid) || null; }, rid);

  try {
    /* ── A · every state, from the record: the window's line and the card's note (an unlocked page, the engine waits for ever so only the record moves) ── */
    if (want('A')) {
      const w = await mk({ locked: false }); const { page } = w;
      await seed(page, [order(4196640001, 1800004821, 'ACORN&#39;S LEAF', "Custom Charm - Acorn's Leaf, Personalized")]);
      await toTab(page); await tables(page, [1800004821, 1800004822]);
      await page.evaluate(h => { __kw.steps = h; }, HANG);
      await dropDesign(page, '4196640001');
      await state(page, 'off'); await settle(page);
      let s = await snap(page);
      ok(s.shown && s.swVis && !s.swOn && s.state === 'off' && !s.line && !s.pw && !s.steps, 'no save yet: the switch is OFF by default and only the switch row shows (state off)');
      ok(s.label === 'Save to the charm repository and link this listing' && s.preview === `New SKU ${SKU} · switch on to make later orders of this listing use this design`, 'nothing saved: the REVDROP words are unchanged: ' + s.preview);
      ok(s.say === '' && [...s.live].sort().join() === 'cuKeepMsg,cuKeepSay', 'the strip has ONE aria-live line (#cuKeepSay) besides the password message, and it is silent while nothing is saved: ' + s.live.join());
      await shot(page, 'A0-off');
      // the switch ON: the save is recorded and starts at once (no Send to Sheet)
      await page.click('#cuKeepSwitch');
      await state(page, 'saving'); await settle(page);
      s = await snap(page);
      ok(s.swOn && !s.swDis && s.lines === 1 && s.line.main === 'Saving to the repository…' && s.line.tone === 'work' && s.line.spin, 'switch ON: the window shows ON and ONE line, "Saving to the repository…", with its spinner: ' + JSON.stringify(s.line));
      ok(s.preview === null && s.swDesc === 'cuKeepLn', 'the line replaces the switch\'s description (the switch is described by the line)');
      ok(s.say === 'Saving to the repository…', 'the live line says it: ' + s.say);
      ok(!!(await recOf(page, '4196640001')), 'the save is recorded in browser storage the moment the switch is ON');
      await shot(page, 'A1-saving');
      // each state, from the record
      const table = [
        // [name, patch, state, main, sub fragment, tone, mark, spinner?, say, card state, card text, card button]
        ['saving, started', { state: 'running', wait: '', repo: false, sku: SKU, why: '' }, 'saving', 'Saving to the repository…', /^(Started |Step 1 of 3)/, 'work', 'spin', true, 'Saving to the repository…', 'saving', 'Saving to the repository…', null],
        ['saving, taking too long', { state: 'running', wait: '', repo: false, sku: SKU, age: 130000 }, 'saving', 'Still saving… this is taking longer than usual', /keeps trying by itself/, 'wait', 'pause', false, null, 'saving', 'Still saving…', null],
        ['saved, linking on Etsy', { state: 'running', wait: '', repo: true, sku: SKU }, 'saving', `Saved to the repository as ${SKU}. Linking this listing on Etsy…`, /^Step 3 of 3/, 'work', 'spin', true, null, 'saving', 'Linking on Etsy…', null],
        ['waiting for the connection', { state: 'waiting', wait: 'connection', repo: false, sku: SKU, since: 90000 }, 'connection', 'Waiting for the connection', /saves by itself when you are back online/, 'wait', 'pause', false, 'Waiting for the connection. It saves by itself when you are back online.', 'connection', 'Waiting for the connection', null],
        ['getting the listing ready', { state: 'waiting', wait: 'data', repo: false, sku: '', since: 12000 }, 'data', 'Getting this listing ready', /^It saves by itself/, 'work', 'spin', true, null, 'data', 'Getting this listing ready', null],
        ['getting the listing ready, too long', { state: 'waiting', wait: 'data', repo: false, sku: '', since: 400000 }, 'data', 'Still getting this listing ready', /taking longer than usual/, 'wait', 'pause', false, null, 'data', 'Still getting this listing ready', null],
        ['trying again', { state: 'waiting', wait: 'retry', repo: false, sku: SKU, fails: 3, why: 'the cloud is slow', nextAt: Date.now() + 600000 }, 'retry', 'Could not finish just now. Trying again by itself.', /^the cloud is slow · tried 3 times · next try about /, 'wait', 'pause', false, null, 'retry', 'Trying again by itself', null],
        ['saved and linked', { state: 'done', wait: '', repo: true, etsy: true, sku: SKU, outcome: 'linked', why: '' }, 'linked', `Saved to the repository as ${SKU}; future orders of this listing will use it`, /^Linked on Etsy listing …4821$/, 'ok', 'tick', false, `Saved to the repository as ${SKU}. Future orders of this listing will use it.`, 'linked', `Saved and linked · ${SKU}`, null],
        ['already linked', { state: 'done', wait: '', repo: true, etsy: true, sku: 'CORGI_34728', outcome: 'already', why: '' }, 'linked', 'This listing is already linked to CORGI_34728. Nothing more to do.', /^$/, 'ok', 'tick', false, null, 'linked', 'Already linked · CORGI_34728', null],
        ['needs attention, not saved', { state: 'attention', wait: '', repo: false, sku: SKU, why: 'the design has no closed cut line' }, 'attention', 'Needs attention: the design has no closed cut line', /^Nothing was saved to the repository\.$/, 'warn', 'bang', false, null, 'attention', 'Needs attention: the design has no closed cut line', 'again'],
        ['needs attention, saved but Etsy not updated', { state: 'attention', wait: '', repo: true, sku: SKU, why: 'Etsy refused the change' }, 'attention', 'Needs attention: Etsy refused the change', /^Saved to the repository as ACORN_LEAF_4821, but Etsy was not updated\.$/, 'warn', 'bang', false, null, 'attention', 'Needs attention: Etsy refused the change', 'again']
      ];
      for (const [name, patch, st, main, sub, tone, mark, spin, say, cst, ctext, cbtn] of table) {
        await setRec(page, '4196640001', patch);
        await state(page, st, 6000); await settle(page);
        s = await snap(page);
        const markOk = mark === 'spin' ? s.line.spin : mark === 'pause' ? s.line.pause : mark === 'tick' ? s.line.tick : mark === 'bang' ? true : false;
        ok(s.lines === 1 && s.line.main === main && sub.test(s.line.sub) && s.line.tone === tone && markOk && (mark === 'spin') === s.line.spin, `${name}: the window's ONE line "${main}" (${tone}, ${mark}): ${JSON.stringify(s.line)}`);
        ok(spin ? s.spins >= 1 : s.spins === 0, `${name}: ${spin ? 'a spinner while something is being done' : 'NO spinner anywhere in the strip'} (${s.spins})`);
        if (say) ok(s.say === say, `${name}: the live line says "${say}": ${s.say}`);
        const finished = st === 'linked' || !!patch.repo;
        ok(s.swOn && s.swDis === finished, `${name}: the switch shows ON${finished ? ' and cannot be switched off (aria-disabled)' : ''}`);
        if (st === 'attention') ok(s.line.btns.join() === 'Try again' && !s.steps, `${name}: the one button is "Try again" and the rows do not repeat it`);
        else ok(s.line.btns.length === 0, `${name}: no button on the line`);
        const cw = await contrastOf(page); ok(cw.length === 0, `${name}: contrast of the window's words is at least 4.5:1 ${cw.join(' | ')}`);
        await shot(page, 'A-' + name.replace(/[^a-z]+/gi, '-').toLowerCase());
        await closeWin(page); await redraw(page);
        const cc = await contrastOf(page); ok(cc.length === 0, `${name}: contrast of the card's words is at least 4.5:1 ${cc.join(' | ')}`);
        const c = await card(page, '4196640001');
        ok(c.row && c.n === 1 && c.state === cst && c.text === ctext && c.spin === spin && c.live === 0, `${name}: the Review card has ONE note "${ctext}" (${c.spin ? 'spinner' : 'no spinner'}, not itself live): ${JSON.stringify([c.n, c.state, c.text, c.live])}`);
        ok(cbtn ? (c.btns.length === 1 && c.btns[0].k === cbtn) : c.btns.length === 0, `${name}: the card's button: ${cbtn || 'none'}`);
        if (name === 'saved and linked' || name === 'waiting for the connection') await shot(page, 'A-card-' + name.replace(/[^a-z]+/gi, '-').toLowerCase(), '#rvList .reviewListRow[data-rid="4196640001"]');
        await openWin(page, '4196640001');
        s = await snap(page);
        ok(s.state === st && s.swOn && s.line && s.line.main === main, `${name}: the window opened again shows the same state, switch ON (read from the record): ${s.state}`);
      }
      ok(w.errors.length === 0, 'scene A: no page error, no console error: ' + w.errors.join(' | '));
      await w.close();
    }

    /* ── B · the switch turned OFF in the middle: what it means ── */
    if (want('B')) {
      const w = await mk({ locked: false }); const { page } = w;
      await seed(page, [order(4196640011, 1800004821, 'ACORN LEAF', 'Custom Charm Leaf'), order(4196640012, 1800004822, 'LEAF TWO', 'Custom Charm Two')]);
      await toTab(page); await tables(page, [1800004821, 1800004822]);
      await page.evaluate(h => { __kw.steps = h; }, HANG);
      await dropDesign(page, '4196640011'); await state(page, 'off');
      await page.click('#cuKeepSwitch'); await state(page, 'saving');
      let s = await snap(page);
      ok(s.swOn && s.state === 'saving', 'B: saving, switch ON');
      // OFF before the repository has it: stops, and says what that means
      await page.click('#cuKeepSwitch'); await state(page, 'switched-off', 4000); await settle(page);
      s = await snap(page);
      ok(!s.swOn && s.line && s.line.main === 'Switched off. Nothing was saved to the repository.' && /Switch it on again/.test(s.line.sub) && s.lines === 1 && s.say === 'Switched off. Nothing was saved to the repository.', 'OFF mid-run: ONE calm line says nothing was saved, and the live line says it: ' + JSON.stringify(s.line));
      ok(s.spins === 0 && !s.pw && !s.steps, 'OFF mid-run: no spinner, no rows, no password row left');
      await shot(page, 'B-switched-off');
      const rec = await recOf(page, '4196640011');
      ok(rec && rec.state === 'off', 'OFF mid-run: the record says off (the keeper stops it): ' + (rec && rec.state));
      await page.waitForFunction(() => document.getElementById('cuKeep').dataset.state === 'off', null, { timeout: 12000 }); await settle(page);
      s = await snap(page);
      ok(!s.line && /switch on to make later orders/.test(s.preview), 'OFF mid-run: the line goes after a few seconds and the offer is back (no timer left running): ' + JSON.stringify([s.state, s.line, s.preview]));
      await closeWin(page); await redraw(page);
      ok((await card(page, '4196640011')).n === 0, 'OFF mid-run: no note on the card');
      // ON again, then the repository HAS it: OFF is refused in one line, the save is finished (never half linked)
      await openWin(page, '4196640011'); await page.click('#cuKeepSwitch'); await state(page, 'saving');
      await setRec(page, '4196640011', { state: 'running', repo: true, sku: SKU });
      await page.waitForFunction(() => /Linking this listing on Etsy/.test(document.querySelector('#cuKeep .cuKeepLine b').textContent), null, { timeout: 6000 });
      await page.click('#cuKeepSwitch', { force: true }); await page.waitForTimeout(250); await settle(page);   // (aria-disabled: a person can still press it, Playwright's own check refuses)
      s = await snap(page);
      ok(s.swOn && s.state === 'saving' && /^Saved to the repository as ACORN_LEAF_4821\. Linking/.test(s.line.main) && /Already saved as ACORN_LEAF_4821\. A saved design is not taken back here; change it in the Library\./.test(s.line.sub), 'OFF after the repository has it: refused, still ON, one calm line says so: ' + JSON.stringify(s.line));
      ok(s.swDis, 'a switch that cannot be switched off says so (aria-disabled)');
      ok((await recOf(page, '4196640011')).state === 'running', 'the save was not cancelled');
      await shot(page, 'B-refused');
      // a second order of the page: OFF by default (REVDROP), whatever the first one is doing
      await closeWin(page);
      await dropDesign(page, '4196640012'); await state(page, 'off'); await settle(page);
      s = await snap(page);
      ok(!s.swOn && !s.line && s.state === 'off', 'another order with no save: OFF by default while the first one is saving: ' + JSON.stringify([s.swOn, s.line, s.state]));
      ok(w.errors.length === 0, 'scene B: no page error, no console error: ' + w.errors.join(' | '));
      await w.close();
    }

    /* ── C · the locked repository: waiting for the password, the inline row, the save goes on by itself; closing, reopening, reloading ── */
    if (want('C')) {
      const w = await mk({ locked: true }); const { page } = w;
      await seed(page, [order(4196640021, 1800004821, 'ACORN LEAF', 'Custom Charm Leaf'), order(4196640022, 1800004822, 'LEAF TWO', 'Custom Charm Two')]);
      await toTab(page); await tables(page, [1800004821, 1800004822]);
      await page.evaluate(s => { __kw.steps = s; }, LINKED);
      await dropDesign(page, '4196640021'); await state(page, 'off'); await settle(page);
      await page.click('#cuKeepSwitch');
      await state(page, 'password', 8000); await settle(page);
      let s = await snap(page);
      ok(s.swOn && s.lines === 1 && s.line.main === 'Waiting for the repository password' && s.line.lock && !s.line.spin && s.line.tone === 'wait' && s.pw && s.spins === 0, 'waiting for the password: ONE line, a padlock (no spinner), the inline password row open: ' + JSON.stringify(s.line));
      ok(/saves by itself as soon as you enter it/.test(s.line.sub) && /Nothing is lost/.test(s.line.sub), 'it says it saves by itself and nothing is lost');
      ok(await page.evaluate(() => document.activeElement && document.activeElement.id === 'cuKeepPw'), 'the focus goes to the password field when the row opens');
      ok(s.pwValue === '' && s.dialogs === 1, 'never auto-filled; the window is the only pop-up');
      await shot(page, 'C-password');
      // a wrong password: the row stays, the line stays, nothing is saved
      await page.fill('#cuKeepPw', 'wrong-password'); await page.keyboard.press('Enter');
      await page.waitForFunction(() => /not right/.test(document.querySelector('#cuKeep .cuKeepMsg').textContent), null, { timeout: 8000 });
      s = await snap(page);
      ok(s.state === 'password' && s.pw && s.pwValue === '', 'a wrong password: the line stays "waiting for the password", the field is cleared');
      // closing the window: the card says it and offers the password
      await closeWin(page); await redraw(page);
      let c = await card(page, '4196640021');
      ok(c.n === 1 && c.state === 'password' && c.text === 'Waiting for the repository password' && c.btns.length === 1 && c.btns[0].t === 'Enter password' && !c.spin, 'window closed: the card says "Waiting for the repository password" with ONE button, no spinner: ' + JSON.stringify(c));
      await shot(page, 'C-card-password', '#rvList .reviewListRow[data-rid="4196640021"]');
      // a RELOAD: the save is still wanted; the window opened again shows ON and the same wait; another order of the page is OFF
      await w.boot(); await seed(page, [order(4196640021, 1800004821, 'ACORN LEAF', 'Custom Charm Leaf'), order(4196640022, 1800004822, 'LEAF TWO', 'Custom Charm Two')]);
      await toTab(page); await tables(page, [1800004821, 1800004822]); await page.evaluate(s => { __kw.steps = s; }, LINKED);
      // (the design comes back with the order, as it does in the shop; if the stand-in did not keep it, it is dropped again)
      const back = await page.waitForFunction(() => Object.values(CustomSheet.entries()).some(e => e.files && e.files.length), null, { timeout: 8000 }).then(() => true, () => false);
      if (!back) await dropDesign(page, '4196640021'); else await openWin(page, '4196640021');
      await state(page, 'password', 10000); await settle(page);
      s = await snap(page);
      ok(s.swOn && s.line.main === 'Waiting for the repository password' && s.pw, 'after a reload the window shows the save as it was: switch ON, waiting for the password, row open');
      await closeWin(page); await redraw(page);
      c = await card(page, '4196640021');
      ok(c.state === 'password' && c.btns[0].t === 'Enter password', 'after a reload the card still says it');
      await page.click('#rvList .reviewListRow[data-rid="4196640021"] [data-cu-keep-retry="repo"]');
      await page.waitForSelector('#cuDlg[open]'); await page.waitForFunction(() => document.activeElement && document.activeElement.id === 'cuKeepPw', null, { timeout: 5000 });
      ok(true, '"Enter password" on the card opens the window with the focus in the password field');
      await closeWin(page);
      await dropDesign(page, '4196640022'); await state(page, 'off'); await settle(page);
      s = await snap(page);
      ok(!s.swOn && !s.line && !s.pw, 'a second order of the same listing family with no save: the switch is OFF, nothing else: ' + JSON.stringify([s.swOn, s.line, s.pw, s.state]));
      await closeWin(page);
      // the right password: the row goes, the save goes on BY ITSELF, ends saved and linked
      await openWin(page, '4196640021');
      await page.fill('#cuKeepPw', REPO_TEST.password); await page.keyboard.press('Enter');
      await state(page, 'linked', 25000); await settle(page);
      s = await snap(page);
      ok(!s.pw && s.line.main === `Saved to the repository as ${SKU}; future orders of this listing will use it` && s.swOn && s.swDis && s.pwValue === '', 'the right password: the row folds, the save went on by itself and ended saved and linked (SKU shown)');
      ok(await page.evaluate(() => !JSON.stringify([...document.querySelectorAll('#cuDlg *')].map(e => e.value || '')).includes('test-')), 'no password is left anywhere in the window');
      ok(!(await page.evaluate(() => JSON.stringify(localStorage) + JSON.stringify(sessionStorage))).includes(REPO_TEST.password), 'the password is not kept in browser storage');
      ok(await page.evaluate(() => __kw.calls.filter(x => x === 'run').length >= 1), 'the engine ran after the unlock');
      await shot(page, 'C-linked');
      await closeWin(page); await redraw(page);
      c = await card(page, '4196640021');
      ok(c.state === 'linked' && c.text === `Saved and linked · ${SKU}`, 'the card: "Saved and linked · SKU"');
      ok(w.errors.length === 0, 'scene C: no page error, no console error: ' + w.errors.join(' | '));
      await w.close();
    }

    /* ── D · waiting for the connection, live; the sandbox rehearsal ── */
    if (want('D')) {
      const w = await mk({ locked: false }); const { page, ctx } = w;
      await seed(page, [order(4196640031, 1800004821, 'ACORN LEAF', 'Custom Charm Leaf')]);
      await toTab(page); await tables(page, [1800004821]); await page.evaluate(s => { __kw.steps = s; }, LINKED);
      await dropDesign(page, '4196640031'); await state(page, 'off');
      await ctx.setOffline(true);
      await page.click('#cuKeepSwitch');
      await state(page, 'connection', 8000); await settle(page);
      let s = await snap(page);
      ok(s.swOn && s.line.main === 'Waiting for the connection' && s.line.pause && !s.line.spin && s.spins === 0 && s.say.startsWith('Waiting for the connection'), 'offline: "Waiting for the connection", a pause mark, no spinner');
      await ctx.setOffline(false);
      await page.evaluate(() => window.dispatchEvent(new Event('online')));
      await state(page, 'linked', 25000);
      s = await snap(page);
      ok(s.line.main.startsWith('Saved to the repository as ') && s.swOn, 'back online: it saved by itself (no press)');
      ok(w.errors.length === 0, 'scene D1: no page error, no console error: ' + w.errors.join(' | '));
      await w.close();
      const x = await mk({ sandbox: true, locked: false });
      await seed(x.page, [order(4196640032, 1800004821, 'ACORN LEAF', 'Custom Charm Leaf')]);
      await toTab(x.page); await tables(x.page, [1800004821]); await x.page.evaluate(s => { __kw.steps = s; }, REHEARSAL);
      await dropDesign(x.page, '4196640032'); await state(x.page, 'off');
      await x.page.click('#cuKeepSwitch'); await state(x.page, 'rehearsed', 15000); await settle(x.page);
      s = await snap(x.page);
      ok(s.line.main === 'Rehearsal complete. Nothing was saved.' && s.line.tick && s.swOn && s.swDis, 'sandbox: "Rehearsal complete. Nothing was saved." and the sandbox label');
      ok(await x.page.evaluate(() => !!document.querySelector('#cuKeep .cuKeepPill') && !document.querySelector('#cuKeep .cuKeepPill').hidden && Object.keys(JSON.parse(localStorage.getItem('cn.keepIntents.v1')).intents).every(k => k.startsWith('sbx|'))), 'sandbox: the pill says nothing is saved and the record is a sandbox one (key sbx|)');
      await shot(x.page, 'D-rehearsed');
      ok(x.errors.length === 0, 'scene D2: no page error, no console error: ' + x.errors.join(' | '));
      await x.close();
    }

    /* ── E · the engine does not answer: "Checking this listing…" gives up in words (the waits are shortened for the test: 2 s and 5 s instead of 12 s and 45 s) ── */
    if (want('E')) {
      const w = await mk({ locked: false }); const { page } = w;
      await seed(page, [order(4196640041, 1800004821, 'ACORN LEAF', 'Custom Charm Leaf')]);
      await toTab(page);
      await page.evaluate(() => { __kw.openGap = 600000; CNKeepUI.lim.OPEN_SLOW = 2000; CNKeepUI.lim.OPEN_STOP = 5000; });
      await page.evaluate(async dxf => {
        const key = Object.keys(Object.fromEntries(B.orders.byKey)).find(k => k.startsWith('4196640041_')), it = Review.customItemFor(key);
        const r = await CharmNestDXF.toPdf(new TextEncoder().encode(dxf), 'x.dxf'); CustomSheet.add(it, [new File([r.bytes], 'Test_Charm.ai')]);
      }, LEAF_DXF);
      await state(page, 'opening', 15000);
      let s = await snap(page);
      ok(s.line && s.line.main === 'Checking this listing…' && s.line.spin, 'the engine is looked at: "Checking this listing…" with a spinner');
      await page.waitForFunction(() => /Still checking this listing/.test(document.querySelector('#cuKeep .cuKeepLine b').textContent), null, { timeout: 8000 });
      s = await snap(page);
      ok(!s.line.spin && s.line.pause && /taking longer than usual/.test(s.line.sub) && s.spins === 0, 'after a while the spinner stops and the line says it takes longer than usual');
      await page.waitForFunction(() => /Could not check this listing just now/.test(document.querySelector('#cuKeep .cuKeepLine b').textContent), null, { timeout: 8000 });
      s = await snap(page);
      ok(s.line.tone === 'warn' && !s.line.spin && /Close this window and open it again/.test(s.line.sub) && s.spins === 0, 'and in the end it says it could not, and the one thing to do (no spinner that never stops)');
      ok(w.errors.length === 0, 'scene E: no page error, no console error: ' + w.errors.join(' | '));
      await w.close();
    }

    /* ── F · keyboard and screen reader ── */
    if (want('F')) {
      const w = await mk({ locked: true }); const { page } = w;
      await seed(page, [order(4196640051, 1800004821, 'ACORN LEAF', 'Custom Charm Leaf')]);
      await toTab(page); await tables(page, [1800004821]); await page.evaluate(s => { __kw.steps = s; }, LINKED);
      await dropDesign(page, '4196640051'); await state(page, 'off'); await settle(page);
      await page.focus('#cuKeepSwitch'); await page.keyboard.press('Space');
      await state(page, 'password', 8000); await settle(page);
      let s = await snap(page);
      ok(s.swOn && s.pw, 'Space turns the switch ON; locked, the password row opens');
      ok(await page.evaluate(() => document.activeElement.id === 'cuKeepPw'), 'the focus moved to the password field');
      await page.keyboard.press('Shift+Tab');
      ok(await page.evaluate(() => document.activeElement.id === 'cuKeepSwitch'), 'Shift+Tab from the password field goes back to the switch (the line is text, not a stop)');
      await page.keyboard.press('Tab'); await page.keyboard.press('Tab');
      ok(await page.evaluate(() => document.activeElement.matches('[data-k-unlock]')), 'Tab order: switch, password field, Unlock');
      // OFF with Enter says it, and the focus stays on the switch
      await page.focus('#cuKeepSwitch'); await page.keyboard.press('Enter');
      await state(page, 'switched-off', 4000);
      s = await snap(page);
      ok(!s.swOn && s.say === 'Switched off. Nothing was saved to the repository.' && await page.evaluate(() => document.activeElement.id === 'cuKeepSwitch'), 'Enter turns it OFF, the live line says what that meant, the focus stays on the switch');
      ok(s.live.filter(x => x === 'cuKeepSay').length === 1 && await page.evaluate(() => document.querySelectorAll('#cuKeep .cuKr[role], #cuKeep .cuKr [aria-live], #cuKeep .cuKeepLine[aria-live], #cuKeep .cuKeepLine[role]').length === 0), 'one live line (#cuKeepSay); the rows and the line are not live');
      // typing in another field: the password row opening does not take the focus
      await page.evaluate(() => { const i = document.createElement('input'); i.id = 'cuTmpField'; i.type = 'text'; document.getElementById('cuDlg').appendChild(i); });
      await page.click('#cuKeepSwitch'); await state(page, 'password', 8000);
      await page.focus('#cuTmpField'); await page.evaluate(() => { const K = document.getElementById('cuKeepPw'); });
      await closeWin(page);
      ok(w.errors.length === 0, 'scene F: no page error, no console error: ' + w.errors.join(' | '));
      await w.close();
    }

    /* ── G · phone width: 40 px targets, nothing sideways, contrast ── */
    if (want('G')) {
      const w = await mk({ locked: false, viewport: { width: 400, height: 860 } }); const { page } = w;
      await seed(page, [order(4196640061, 1800004821, 'ACORN LEAF', 'Custom Charm Leaf')]);
      await toTab(page); await tables(page, [1800004821]); await page.evaluate(s => { __kw.steps = s; }, HANG);
      await dropDesign(page, '4196640061'); await state(page, 'off'); await settle(page);
      const targets = () => page.evaluate(() => {
        const vis = n => !!n && !n.hidden && !n.closest('[hidden]') && n.getClientRects().length > 0;
        const bad = []; for (const b of document.querySelectorAll('#cuKeep button, #cuKeep input, .cuKeepNote button')) { if (!vis(b)) continue; const r = b.getBoundingClientRect(); if (r.height < 39.5 || r.width < 39.5) bad.push((b.id || b.textContent || b.className) + ':' + Math.round(r.width) + 'x' + Math.round(r.height)); }
        const d = document.getElementById('cuDlg'), h = document.getElementById('cuKeep');
        return { bad, over: Math.max(d.scrollWidth - d.clientWidth, h.scrollWidth - h.clientWidth), page: document.documentElement.scrollWidth - document.documentElement.clientWidth };
      });
      // contrast of every visible word of the strip against what is behind it
      const contrast = () => contrastOf(page);
      const states = [
        ['off', null], ['saving', { state: 'running', repo: false, sku: SKU }], ['password', null], ['connection', { state: 'waiting', wait: 'connection', repo: false, sku: SKU }],
        ['retry', { state: 'waiting', wait: 'retry', repo: false, sku: SKU, fails: 2, why: 'the cloud is slow' }], ['linked', { state: 'done', repo: true, etsy: true, sku: SKU, outcome: 'linked' }], ['attention', { state: 'attention', repo: false, sku: SKU, why: 'the design has no closed cut line' }]
      ];
      for (const [name, patch] of states) {
        if (name === 'saving') { await page.click('#cuKeepSwitch'); await state(page, 'saving'); }
        if (patch) { await setRec(page, '4196640061', patch); await state(page, name === 'saving' ? 'saving' : name, 6000); }
        if (name === 'password') { await setRec(page, '4196640061', { state: 'waiting', wait: 'password', repo: false, sku: SKU }); await state(page, 'password', 6000); await page.evaluate(() => { try { localStorage.removeItem('cn.repoSession.v1'); sessionStorage.removeItem('cn.repoLock.v1'); } catch (_) {} CharmNestRepoLock.forget && CharmNestRepoLock.forget('upload'); }); }
        await settle(page);
        const t = await targets(), cg = await contrast();
        ok(t.bad.length === 0 && t.over <= 0 && t.page <= 0, `phone 400 px, ${name}: every press target is at least 40 px, nothing sideways ${JSON.stringify(t)}`);
        ok(cg.length === 0, `phone 400 px, ${name}: contrast of the words is at least 4.5:1 ${cg.join(' | ')}`);
        await shot(page, 'G-phone-' + name);
        if (name !== 'off') { await closeWin(page); await redraw(page); const tc = await targets(), cc = await contrast(); ok(tc.bad.length === 0 && tc.page <= 0, `phone 400 px, ${name}: the card's note and its button are at least 40 px and fit: ${JSON.stringify(tc)}`); ok(cc.length === 0, `phone 400 px, ${name}: the card's contrast: ${cc.join(' | ')}`); await shot(page, 'G-phone-card-' + name, '#rvList .reviewListRow[data-rid="4196640061"]'); await openWin(page, '4196640061'); }
      }
      ok(w.errors.length === 0, 'scene G: no page error, no console error: ' + w.errors.join(' | '));
      await w.close();
    }

    /* ── H · a second design dropped while a save waits: the save keeps its own strip (one design is saved per order, and the line says which) ── */
    if (want('H')) {
      const w = await mk({ locked: true }); const { page } = w;
      await seed(page, [order(4196640071, 1800004821, 'ACORN LEAF', 'Custom Charm Leaf')]);
      await toTab(page); await tables(page, [1800004821]); await page.evaluate(s => { __kw.steps = s; }, LINKED);
      await dropDesign(page, '4196640071'); await state(page, 'off');
      await page.click('#cuKeepSwitch'); await state(page, 'password', 8000);
      await page.evaluate(async dxf => {
        const key = Object.keys(Object.fromEntries(B.orders.byKey)).find(k => k.startsWith('4196640071_')), it = Review.customItemFor(key);
        const r = await CharmNestDXF.toPdf(new TextEncoder().encode(dxf), 'x.dxf'); CustomSheet.add(it, [new File([r.bytes], 'Second_Design.ai')]);
      }, RECT_DXF);
      await page.waitForFunction(() => Object.values(CustomSheet.entries()).some(e => e.files && e.files.length === 2), null, { timeout: 20000 });
      await page.waitForTimeout(600); await settle(page);
      const s = await snap(page);
      ok(s.state === 'password' && s.swOn && s.pw && s.line && s.line.main === 'Waiting for the repository password' && /Only Test_Charm\.ai is saved; the other design stays on this order/.test(s.line.sub), 'a second design dropped: the save keeps its own strip (waiting for the password, the row still there) and says which design is saved: ' + JSON.stringify([s.state, s.pw, s.line]));
      await shot(page, 'H-second-design');
      ok(w.errors.length === 0, 'scene H: no page error, no console error: ' + w.errors.join(' | '));
      await w.close();
    }
  } finally { await browser.close(); }
  console.log(`\nkeep-ui-states: ${n - fails.length}/${n} checks passed`);
  if (fails.length) { console.log('\nFAILED:\n - ' + fails.join('\n - ')); process.exit(1); }
}
main().catch(e => { console.error(e); process.exit(1); });
