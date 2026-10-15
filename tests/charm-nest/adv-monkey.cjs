// ADV-MONKEY (Paul, 15 Oct 2026: "final test before I push everything to production … resilient against lost internet connections, interruptions
// and unexpected user behaviour"): ONE seeded, deterministic clumsy user on the REAL sorter page (headless Chromium, the fake site of bridge-server.cjs, the
// real-drawing world of _engraveRealWorld.cjs: real master designs, real pool, real classifier and fit; 400 more pulled orders for a production-sized Orders
// tab, a dozen of them carrying markup in every field Etsy or a person can fill). No Etsy call, no live record, nothing paid; every request off the loopback is aborted.
// The same seed plays the same clumsy day. What he does:
//   1  tab storm        every tab in a random order, two clicks at a time, the fake site answering late; Back / Forward in the middle of it
//   2  double clicks    three quick clicks on a seeded pick of the buttons of every tab; the same press three times with the mouse and three times in ONE tick on the buttons that
//                       write (Pull orders, Send to Review, Nest, Approve engraving, Approve, Commit): what they wrote must be written once
//   3  hostile text     markup, 30 000 letters, emoji, right-to-left, a lone surrogate, NUL, a number longer than any order, an empty box, into the inputs of every tab, window and
//                       dialog (search, order number, personalization, names, the message box, Settings), Enter and Tab after each; the orders that carry markup in every field are
//                       found by it and put on screen as a list and as cards; a hostile employee name signs an approval and a message
//   4  wrong files      a text file called .pdf, an empty .ai, a zip that is no zip, noise called .pdf, an .exe, names with markup: put in the file box and dropped on the page and
//                       on a Review card; text dragged over them; nothing may stay "reading", the veil must go. A real vector file dropped twice on a Review card: ONE question
//                       (#cuAsk), Esc adds nothing, a drop while it is open is ignored, "yes" three times adds the design once
//   5  closing early    a message sent twice over and the window closed at once is stored once; Settings saved twice and closed; Back with a window open
//   6  a phone          375 px wide: no sideways page, the tab and the search reachable, the windows fit and their close buttons are on screen
//   7  keys             "/" , Ctrl+K, Esc, Tab, Enter, arrows, Space and random keys on a tab
//   8  the address      a typed #bogus after Back, a refresh in the middle of a load, Back out of the app and forward, the network gone while he clicks around, a start with
//                       settings and a name made of markup
// After every step the page must be free of: an uncaught error or unhandled rejection or console error, a swallowed TypeError, a dialog or spinner (on screen) or veil that
// Esc and the network going quiet do not clear, a blank screen, markup made from text (any [data-pwn] element, any executed __pwn()), and a write that went twice for one
// press. Each failure names its step; the run goes on to the end, then fails. Steps 8, 6 and 7 (in that order) are skipped (and it says so) when the shared machine has already used
// MONKEY_BUDGET_S (200) seconds, so a busy box does not cut the run off. About 3 minutes on a quiet machine.
//   node tests/charm-nest/adv-monkey.cjs [playwright-core dir]      (MONKEY_SEED=<n> plays another day; MONKEY_TRACE=1 prints every step)
const path = require('path'), fs = require('fs'), os = require('os');
const R = require('./_engraveRealWorld.cjs');
const R2 = require('./round2-smoke.cjs');

const SEED = +process.env.MONKEY_SEED || 20261015, TRACE = !!process.env.MONKEY_TRACE;
const sleep = ms => new Promise(r => setTimeout(r, ms));
const rnd = (seed => () => { seed = (seed + 0x6D2B79F5) | 0; let t = Math.imul(seed ^ (seed >>> 15), 1 | seed); t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t; return ((t ^ (t >>> 14)) >>> 0) / 4294967296; })(SEED);
const pick = a => a[Math.floor(rnd() * a.length)];
const shuffle = a => { const b = a.slice(); for (let i = b.length - 1; i > 0; i--) { const j = Math.floor(rnd() * (i + 1)); [b[i], b[j]] = [b[j], b[i]]; } return b; };

/* ── what he types: markup that would run or show if text were ever made into HTML, and plain stress ── */
const MARKUP = [
  t => `<b data-pwn>x</b><img src=x onerror="__pwn('${t}:img')">`,
  t => `"><svg onload="__pwn('${t}:svg')"><b data-pwn>`,
  t => `' data-pwn='1' autofocus onfocus='__pwn("${t}:sq")' x='`,
  t => `" data-pwn="1" autofocus onfocus="__pwn('${t}:dq')" x="`,
  t => `</textarea></dialog></script><script>__pwn('${t}:script')</script><b data-pwn>`,
  t => `<a href="javascript:__pwn('${t}:href')" data-pwn>click</a>`,
  t => '${__pwn("' + t + ':tpl")}{{constructor.constructor("__pwn(1)")()}}'
];
const STRESS = [
  '', '   ', '\n\n\n', 'A'.repeat(30000), 'W'.repeat(8000) + ' ' + 'i'.repeat(8000), '😀👨‍👩‍👧‍👦🇨🇦 Ünï çödé ‮RTL‬ ​‍﷽ 𝕏 \ud800 lone \u0000 nul ᾍ',
  '12345678901234567890123', '#4176 208 841', '-1', '0', 'NaN', '__proto__', 'constructor', "'; DROP TABLE orders;--", '../../etc/passwd', '%00%0d%0a', '\t\t\r\n tab and return', 'ß'.repeat(300),
  '(', '[', '\\', '(?<', '.*+?^${}()|[]\\', '4290000103'
];
const NUMBERS = ['-1', '0', '99999999999999999999', '1e308', '0.0000001', '7', ''];
// pressing these changes what the world holds beyond repair for the next step: left to the targeted presses (2) and the end of the run
const DESTRUCTIVE = /delete|purge|reset|wipe|abandon|clear|remove|cancel order|sign ?out|lock now|discard|give up|stop|restore|undo|download|print|\bsend\b|commit|complete|approve|release|hold|rehearse|sandbox/i;

/* ── the instruments ── */
const issues = [];
let step = 'boot';
const bad = (kind, text) => { const line = `[${step}] ${kind}: ${String(text).replace(/\s+/g, ' ').slice(0, 300)}`; if (!issues.some(x => x.line === line)) { issues.push({ line }); console.log('  ✗ ' + line); } };
const say = s => { if (TRACE) console.log('  · ' + s); };
const T0 = Date.now(); let lastPhase = Date.now();
const BUDGET_S = +process.env.MONKEY_BUDGET_S || 200;   // the machine is shared and slow at times: the last steps are skipped, and said so, rather than the run being cut off
const over = what => { if ((Date.now() - T0) / 1000 < BUDGET_S) return false; console.log(`  – skipped, over the ${BUDGET_S} s budget: ${what}`); return true; };
const phase = name => { console.log(`  ${((Date.now() - lastPhase) / 1000).toFixed(0)} s  ${name}`); lastPhase = Date.now(); };
const IGNORE = /firebase stub|Failed to load resource|net::ERR_|favicon|WebGL|GPU stall/i;
const SWALLOWED = /TypeError|ReferenceError|RangeError|SyntaxError|Cannot read prop|is not a function|is not defined|is not iterable|undefined \(reading/;
const NETWORK = /Failed to fetch|NetworkError|Load failed|AbortError|aborted|timed out|net::/i;   // (a lost connection, logged by a handler that coped with it)

// run in every page, before its own scripts: report an unhandled rejection, an executed __pwn(), and any element made from the markup above; and the two finders
const INSTALL = () => {
  if (window.__monkeyOn) return; window.__monkeyOn = 1;
  const mk = m => { try { window.__mk(m); } catch (_) { /* the binding is not there yet */ } };
  addEventListener('unhandledrejection', e => mk('unhandled rejection: ' + String((e.reason && (e.reason.stack || e.reason.message)) || e.reason).split('\n').slice(0, 3).join(' | ')));
  window.__pwn = tag => mk('XSS: ' + tag + ' ran');
  const seen = n => { if (n.nodeType === 1 && ((n.matches && n.matches('[data-pwn]')) || (n.querySelector && n.querySelector('[data-pwn]')))) mk('markup made from text: ' + (n.outerHTML || '').slice(0, 140)); };
  new MutationObserver(ms => { for (const m of ms) { if (m.type === 'attributes') seen(m.target); for (const n of m.addedNodes) seen(n); } }).observe(document, { childList: true, subtree: true, attributes: true, attributeFilter: ['data-pwn'] });
  const shown = e => { const b = e.getBoundingClientRect(), s = getComputedStyle(e); return b.width > 3 && b.height > 3 && s.visibility !== 'hidden' && s.pointerEvents !== 'none' && !e.closest('[hidden],.hidden,[inert]'); };
  const scope = () => [...document.querySelectorAll('dialog[open]')].pop() || document.getElementById('stage') || document.body;
  window.__mkButtons = () => [...scope().querySelectorAll('button:not([disabled]),[role=button]:not([aria-disabled="true"]),summary')].filter(shown);
  window.__mkInputs = () => [...scope().querySelectorAll('input:not([type=file]):not([type=checkbox]):not([type=radio]):not([type=range]):not([type=hidden]):not([type=button]):not([type=submit]):not([type=color]):not([readonly]):not([disabled]), textarea:not([readonly]):not([disabled]), [contenteditable="true"]')].filter(shown);
  window.__mkLabel = e => String(e.innerText || e.value || e.title || e.getAttribute('aria-label') || e.placeholder || e.id || '').trim().replace(/\s+/g, ' ').slice(0, 40);
};

(async () => {
  const w = await R.world({ viewport: { width: 1440, height: 950 } });
  if (!w) { console.log('  – no playwright-core: not run'); return; }
  const { page, context, srv } = w, st = srv.st, ORIGIN = srv.sorterOrigin;
  page.setDefaultTimeout(8000);
  let inflight = 0, slow = 0, navigated = 0;
  const isFn = r => /\.netlify\/functions\//.test(r.url());
  page.on('request', r => { if (isFn(r)) inflight++; });
  page.on('requestfinished', r => { if (isFn(r)) inflight = Math.max(0, inflight - 1); });
  page.on('requestfailed', r => { if (isFn(r)) inflight = Math.max(0, inflight - 1); });
  page.on('pageerror', e => bad('uncaught error', String(e.stack || e.message).split('\n').slice(0, 3).join(' | ')));
  page.on('console', m => { const t = m.text(); if (m.type() === 'error' && !IGNORE.test(t)) bad('console error', t); else if (m.type() === 'warning' && SWALLOWED.test(t) && !IGNORE.test(t) && !NETWORK.test(t)) bad('swallowed exception', t); });
  page.on('framenavigated', f => { if (f === page.mainFrame()) navigated++; });
  page.on('dialog', d => d.dismiss().catch(() => {}));   // (a real alert / confirm / prompt the page raises past the stubs is answered "no")
  await page.exposeFunction('__mk', m => bad('page', m));
  const STUBS = () => { window.confirm = () => true; window.alert = () => {}; };
  await context.addInitScript(INSTALL); await context.addInitScript(STUBS); await page.evaluate(INSTALL); await page.evaluate(STUBS);
  // the fake site answers late while `slow` is set: loads are in flight while he moves on
  await context.route(/\/\.netlify\/functions\//, async r => { if (slow) await sleep(slow); r.fallback().catch(() => {}); });
  const ev = async (fn, arg) => { try { return await page.evaluate(fn, arg); } catch (e) { say('evaluate: ' + String(e.message).split('\n')[0].slice(0, 120)); return undefined; } };
  const settleNet = async (ms = 10000) => { for (const t = Date.now(); Date.now() - t < ms && inflight > 0;) await sleep(100); };
  const esc3 = async () => { for (let i = 0; i < 3; i++) await page.keyboard.press('Escape').catch(() => {}); };
  const READS = new Set(['masterGet', 'masterGetMany', 'masterList', 'masterListFiles', 'runList', 'history', 'backList', 'setGet', 'setList', 'releaseGet', 'runGet', 'laserDoneList', 'aliasGet', 'laserStatus', 'ping', 'timelineGet', 'poolList', 'findSheets', 'cancelList', 'cancelCheck', 'flowState', 'bridgeLog', 'customGet', 'listingPhotos', 'partialSearchList', 'partialPolicyGet']);
  const writes = from => st.calls.slice(from).filter(c => c.name === 'charmNestLibrary' ? !(READS.has(c.op) || /^(get|list|find)[A-Z]/.test(c.op || '')) : ['firebaseOrders', 'designArchive'].includes(c.name) && c.body && Object.keys(c.body).length > 0);
  const canon = c => JSON.stringify([c.name, c.op, c.body], (k, v) => typeof v === 'number' && v > 1e11 ? 'T' : /^(id|eventId|uid|nonce|clientId|startId|stamp|seq|tick)$/.test(k) ? 'I' : v);
  let onPhone = false;
  const mode = m => ev(m => CN.setMode(m), m);
  const label = (a, ...r) => `${a} ${r.join(' ')}`;

  /* what a step must leave true */
  async function checkpoint(name) {
    step = name;
    await esc3(); await settleNet(); await sleep(200);
    const look = () => ev(() => {
      const shown = e => { const b = e.getBoundingClientRect(); return b.width > 0 && b.height > 0 && getComputedStyle(e).visibility !== 'hidden' && !e.closest('[hidden],.hidden'); };
      const out = { dialogs: [...document.querySelectorAll('dialog[open]')].map(d => d.id || d.className), spin: [], veil: !!document.querySelector('#dropAll.on'), toasts: document.querySelectorAll('#toasts .toast').length, views: [], blocked: null, mode: window.CN && CN.S.mode, ghosts: [], ask: [...document.querySelectorAll('#cuAsk')].filter(shown).length, working: window.Engrave ? [...Engrave.items().values()].filter(j => ['classify', 'reclassify', 'ready', 'fitting'].includes(j.state)).length : 0 };
      // (on screen: a thumbnail below the fold of its list waits for the eye, by design, and is not a spinner anyone sees)
      const onScreen = e => { const b = e.getBoundingClientRect(); if (b.bottom < 0 || b.top > innerHeight || b.right < 0 || b.left > innerWidth) return false; for (let p = e.parentElement; p && p !== document.body; p = p.parentElement) { const s = getComputedStyle(p); if (/(auto|scroll|hidden|clip)/.test(s.overflowY + s.overflowX)) { const c = p.getBoundingClientRect(); if (b.bottom < c.top || b.top > c.bottom || b.right < c.left || b.left > c.right) return false; } } return true; };
      for (const e of document.querySelectorAll('.spin, .spinner, .cmSpin, [aria-busy="true"], [role=progressbar]')) if (shown(e) && onScreen(e)) out.spin.push((e.id || String(e.className) || e.tagName) + ' in #' + ((e.closest('[id]') || {}).id || '?') + ' [' + ((e.closest('[data-key],.egCard,.engCard,[data-job],.rvItem,section,.card') || e.parentElement || {}).innerText || '').replace(/\s+/g, ' ').trim().slice(0, 70) + ']');
      out.ghosts = [...document.querySelectorAll('.mGhost,.mLand,.mPlus,.mLift,.mdGhost,.mdGhostBack,.dndLift,.fxBox,.dndDock,[data-fx-hide],.dndSource,html[data-library-drag]')].map(e => String(e.className || e.tagName)).slice(0, 4);
      out.views = ['sheets', 'ordersView', 'designView', 'masterView', 'efficiencyView', 'engraveView', 'reviewView', 'libView', 'charmsView'].filter(id => { const e = document.getElementById(id); return e && !e.classList.contains('hidden'); });
      const tab = document.querySelector('.modeSeg button[data-mode="orders"]');
      if (tab) { tab.scrollIntoView({ inline: 'center', block: 'nearest' }); const b = tab.getBoundingClientRect(), top = document.elementFromPoint(Math.min(innerWidth - 2, Math.max(2, b.left + b.width / 2)), b.top + b.height / 2); out.blocked = top && (top === tab || tab.contains(top)) ? null : (top ? String(top.id || top.className || top.tagName) : 'nothing'); }
      return out;
    });
    // a window's copy flying home, a thumbnail still decoding: given up to 8 s on a loaded machine; what is still there then is stuck
    let r = await look();
    // (an engraving still being read or fitted in the background is work, not a stuck spinner: waited for, up to 40 s)
    for (const t = Date.now(); r && (r.ghosts.length || r.spin.length) && Date.now() - t < 8000;) { await sleep(400); r = await look(); }
    if (!r) return bad('page', 'the page did not answer');
    // (400 orders with words: the engravings are read and fitted one after another in the background for minutes; the Engrave tab says "Working N" meanwhile, by design)
    if (r.working) r.spin = r.spin.filter(x => !/ in #(engraveView|egQueue)/.test(x));
    if (r.dialogs.length) bad('stuck', 'dialog still open after Esc x3: ' + r.dialogs.join(','));
    if (r.spin.length) bad('stuck', 'spinner still turning after 8 s with the network quiet: ' + r.spin.slice(0, 4).join(' | '));
    if (r.veil) bad('stuck', 'the page-wide drop veil is still on');
    if (r.ask) bad('stuck', 'the question before a first vector file (#cuAsk) is still open after Esc x3');
    if (r.ghosts.length) bad('stuck', 'flight leftovers after 8 s: ' + r.ghosts.join(','));
    if (r.blocked && !r.dialogs.length && !onPhone) bad('stuck', 'the Orders tab cannot be clicked, covered by ' + r.blocked);
    if (r.views.length !== 1) bad('blank', `${r.views.length} views shown (${r.views.join(',')}) on mode "${r.mode}"`);
    if (r.toasts > 3) bad('stuck', r.toasts + ' toasts stacked');
    const dom = await ev(() => document.querySelectorAll('[data-pwn]').length); if (dom) bad('xss', dom + ' element(s) made from typed text are in the page');
    say(`checkpoint "${name}" mode=${r.mode}`);
  }
  /** three quick presses at a spot, as a hand does; then what the press wrote must have been written once */
  async function press(what, x, y, n = 3) {
    const from = st.calls.length;
    await page.mouse.click(x, y, { clickCount: n, delay: 12 });
    await sleep(300); await settleNet(6000);
    const seen = new Map(); for (const c of writes(from)) seen.set(canon(c), (seen.get(canon(c)) || 0) + 1);
    for (const [k, v] of seen) if (v > 1) bad('twice', `${what} ×${n} wrote the same thing ${v} times: ${k.slice(0, 200)}`);
    if (TRACE && seen.size) say(`${what}: wrote ${[...seen.entries()].map(([k, v]) => v + '×' + k.slice(0, 70)).join(' ; ')}`);
    return seen.size;
  }
  /** the same press three times in ONE tick (a touch and its click, a key held): for the buttons that write */
  async function pressSync(what, sel, n = 3) {
    const from = st.calls.length;
    const hit = await ev(({ sel, n }) => { const e = [...document.querySelectorAll(sel)].find(x => { const b = x.getBoundingClientRect(); return b.width > 2 && b.height > 2 && !x.disabled && !x.closest('[hidden],.hidden'); }); if (!e) return false; for (let i = 0; i < n; i++) e.click(); return true; }, { sel, n });
    if (!hit) return false;
    await sleep(300); await settleNet(6000);
    const seen = new Map(); for (const c of writes(from)) seen.set(canon(c), (seen.get(canon(c)) || 0) + 1);
    for (const [k, v] of seen) if (v > 1) bad('twice', `${what} (${sel}) ×${n} in one tick wrote the same thing ${v} times: ${k.slice(0, 200)}`);
    if (TRACE && seen.size) say(`${what} (sync): wrote ${[...seen.entries()].map(([k, v]) => v + '×' + k.slice(0, 70)).join(' ; ')}`);
    return true;
  }
  const centreOf = async (sel) => ev(sel => { const e = [...document.querySelectorAll(sel)].find(x => { const b = x.getBoundingClientRect(); return b.width > 2 && b.height > 2 && !x.closest('[hidden],.hidden'); }); if (!e) return null; e.scrollIntoView({ block: 'center', inline: 'center' }); const b = e.getBoundingClientRect(); return { x: b.left + b.width / 2, y: b.top + b.height / 2 }; }, sel);
  const treble = async (what, sel) => { const c = await centreOf(sel); if (!c) return false; await press(what, c.x, c.y); return true; };

  /* ═══ the world: the six real orders, the Library, 400 more orders, a dozen with markup in every field ═══ */
  step = 'world';
  const made = await R.build(w);
  say('built: ' + Object.entries(made).map(([k, v]) => k + ':' + v.state + '/' + v.jobs.map(j => j.state).join('+')).join(' '));
  R2.seedSmall(st, srv.blobUrl);
  const NAMES = ['Hannah Whitford', 'Grace Hopper', 'Ada Byron', 'Åsa Öberg', '李小龍', 'Zoë Müller-Smith', "Seán O'Brien"];
  const HOSTILE = MARKUP.map((m, i) => { const t = n => m(n + i); return { buyer: t('buyer'), sku: i % 2 ? t('sku') : 'COMET_9427', title: t('title'), msg: t('message'), gift: t('gift'), note: t('staff'), pers: [t('personalization'), 'line two ' + t('p2')], vars: [{ name: t('varname'), value: t('varvalue') }, { name: 'Font Choice', value: t('font') }], qty: 1 + i % 3 }; });
  const seedRows = () => ev(({ hostile, names }) => {
    const now = Date.now(), SHIP = Math.floor(now / 1000) + 4 * 86400; let n = 0;
    const add = (rid, f) => {
      const key0 = String(rid); if ([...B.orders.byKey.keys()].some(k => k.startsWith(key0 + '_'))) return;
      const o = { receiptId: key0, orderNumber: key0, createTs: SHIP - 3 * 86400 - (rid % 997) * 60, updateTs: SHIP - 3 * 86400, shipBy: SHIP, buyer: { name: f.buyer }, buyerMessage: f.msg || '', isGift: !!f.gift, giftMessage: f.gift || '', staffNote: f.note || '', messages: [],
        lines: [{ transactionId: rid + '1', listingId: '19' + key0.slice(-6), sku: f.sku, title: f.title, quantity: f.qty || 1, expectedShipDate: SHIP, variations: f.vars || [], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: f.pers || [], buyerMessage: f.msg || '' }] };
      const line = o.lines[0], key = CharmNestOrders.lineKey(o, line);
      const row = { key, order: o, line, arrivedAt: now, spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null };
      B.orders.rows.push(row); B.orders.byKey.set(key, row); n++;
    };
    hostile.forEach((f, i) => add(4390000001 + i, f));
    for (let i = 0; i < 400; i++) add(4300000000 + i * 7 + 1, { buyer: names[i % names.length] + ' ' + i, sku: ['COMET_9427', 'SCHRODINGER CAT 1', 'UNKNOWN_SKU_' + (i % 9), 'TINY_BAR_' + i, ''][i % 5], title: 'Charm necklace ' + i, pers: i % 3 ? ['Name ' + i] : [], vars: [{ name: 'Metal choice', value: ['Gold', 'Silver', 'Rose Gold'][i % 3] }], qty: 1 + (i % 4 === 0) });
    Orders.interpretAll(); try { ListMedia.prepare(B.orders.rows); } catch (_) {} CN.setMode('orders'); Orders.render();   // (prepared as the pull prepares its rows: the pictures are asked for)
    return n;
  }, { hostile: HOSTILE, names: NAMES });
  say('seeded ' + await seedRows() + ' orders');
  await checkpoint('world seeded');
  console.log(`world ready: ${await ev(() => Orders.rows().length)} lines, ${await ev(() => Engrave.items().size)} engraving jobs (${Object.entries(made).map(([k, v]) => k + ':' + v.jobs.map(j => j.state).join('+')).join(' ')})`);
  const cov = new Set();   // what was really pressed / typed into (printed at the end: a monkey that never found the button proves nothing)

  /* ═══ 1 · tab storm ═══ */
  {
    const TABS = ['nest', 'orders', 'engrave', 'review', 'library', 'design'];
    const clickTab = async t => {
      const inMore = ['design', 'master'].includes(t);
      try {
        if (inMore) { await page.locator('#moreMenu > summary').click({ timeout: 1500, noWaitAfter: true }); await page.locator(`#moreMenu button[data-mode="${t}"]`).click({ timeout: 1500, noWaitAfter: true }); }
        else await page.locator(`.modeSeg button[data-mode="${t}"]`).click({ timeout: 1500, noWaitAfter: true });
      } catch (e) { say(`tab ${t}: ${String(e.message).split('\n')[0].slice(0, 100)}`); }
    };
    slow = 700;
    for (let round = 0; round < 1; round++) {
      step = 'tab storm ' + (round + 1);
      for (const t of shuffle(TABS)) { await clickTab(t); await sleep(Math.floor(rnd() * 60)); if (rnd() < .2) { await ev(() => history.back()); await sleep(40); await ev(() => history.forward()); } }
    }
    slow = 0;
    await checkpoint('after the tab storm');
  }

  phase('1 tab storm');
  /* ═══ 2 · double clicks ═══ */
  {
    for (const m of ['nest', 'orders', 'review']) {
      await mode(m); await sleep(900); step = 'triple clicks on ' + m;
      const names = (await ev(() => __mkButtons().map(e => [__mkLabel(e), e.id, String(e.className).slice(0, 40)]))) || [];
      const safe = names.filter(([t, id, c]) => !DESTRUCTIVE.test(t + ' ' + id) && !/modeSeg|modeArrow|navBtn|moreMenu|topbar/.test(c + ' ' + id) && id !== 'btnSettings' && id !== 'btnRail');
      say(`${m}: ${names.length} buttons, ${safe.length} safe`);
      for (const [t, id] of shuffle(safe).slice(0, 1)) {
        const c = await ev(({ t, id }) => { const e = __mkButtons().find(x => x.id === id && __mkLabel(x) === t); if (!e) return null; e.scrollIntoView({ block: 'center' }); const b = e.getBoundingClientRect(); return { x: b.left + b.width / 2, y: b.top + b.height / 2 }; }, { t, id });
        if (!c) continue;
        step = `triple click "${t}" on ${m}`; cov.add(`${m}:${t}`);
        await press(`"${t}"`, c.x, c.y); await esc3(); await sleep(150);
      }
      await checkpoint('triple clicks on ' + m);
    }
    // the buttons that write: the word "Start", "Pull", "Approve", "Commit", "Nest", "Send" … three presses with the mouse, then three in one tick
    const WRITERS = [
      ['orders', 'Pull orders', '#ordPull'], ['orders', 'Send to Review', '#ordItems .revSendBtn:not([disabled])'], ['orders', 'Release hold', '.relHold:not([data-gate])'], ['nest', 'Nest all ready', '#btnNestAll'], ['nest', 'Nest', '[data-r="nest"]:not([disabled])'],
      ['engrave', 'Approve engraving', '[data-a="approve"]:not([disabled])'], ['engrave', 'Approve engraving (list)', '.egApproveButton:not([disabled])'], ['review', 'Complete order', '#rvList [data-complete]'], ['review', 'Send to Sheet', '#rvList [data-send]'],
      ['library', 'Approve', '[data-approve-btn]:not([disabled])'], ['library', 'Commit set', '#rbCommit']
    ];
    for (const [m, what, sel] of WRITERS) {
      await mode(m); await sleep(700); step = `${what} pressed three times (${m})`;
      if (m === 'engrave') { await R.openEngraveDetail(w, 'back', {}).catch(() => {}); await sleep(500); }
      if (await treble(what, sel)) cov.add(`write:${what}`); else say(`no ${sel} on ${m}`);
      if (m === 'engrave') { await R.openEngraveDetail(w, 'discs', {}).catch(() => {}); await sleep(500); }
      if (await pressSync(what, sel)) cov.add(`write-sync:${what}`);
      await esc3();
    }
    await checkpoint('the buttons that write');
  }

  phase('2 double clicks');
  /* ═══ 3 · hostile text into every input ═══ */
  let mkI = 0, strI = 0;
  async function typeAll(surface, max = 12) {
    const fields = (await ev(() => __mkInputs().map((e, i) => [i, e.tagName.toLowerCase() + (e.type ? ':' + e.type : ''), e.id, __mkLabel(e)]))) || [];
    say(`${surface}: ${fields.length} inputs: ${fields.map(f => f[1] + '#' + f[2] + '(' + f[3].slice(0, 14) + ')').join(' ')}`);
    for (const [i, kind, id, name] of shuffle(fields).slice(0, max)) {
      const list = kind.endsWith(':number') ? shuffle(NUMBERS).slice(0, 3) : [MARKUP[mkI++ % MARKUP.length]('typed' + mkI), STRESS[strI++ % STRESS.length]];
      for (const p of list) {
        step = `${surface}: "${name || id || kind}" ← ${JSON.stringify(p.slice(0, 22))}${p.length > 22 ? '…(' + p.length + ')' : ''}`;
        const ok = await ev(({ id, kind, name }) => { const e = __mkInputs().find(x => x.id === id && (x.tagName.toLowerCase() + (x.type ? ':' + x.type : '')) === kind && (id || __mkLabel(x) === name)); if (!e) return false; e.focus(); if (e.select) e.select(); return document.activeElement === e; }, { id, kind, name });
        if (!ok) continue;
        cov.add('type:' + surface);
        const n0 = navigated;
        await page.keyboard.insertText(p).catch(() => {}); await sleep(50);
        await page.keyboard.press('Enter').catch(() => {}); await sleep(60);
        await page.keyboard.press('Tab').catch(() => {}); await sleep(40);
        if (navigated !== n0) bad('navigated', `Enter in "${name || id}" reloaded or left the page: ${page.url().slice(0, 100)}`);
        if (p.length > 20000) { const t0 = Date.now(); await ev(() => 1); if (Date.now() - t0 > 15000) bad('stuck', `${p.length} characters in "${name || id}" froze the page for ${Date.now() - t0} ms`); }
      }
    }
  }
  const tour = async () => { for (const m of ['orders', 'engrave', 'review', 'library', 'nest']) { await mode(m); await sleep(350); } await ev(() => { document.querySelector('#libKind button[data-k="sheets"]') && document.querySelector('#libKind button[data-k="sheets"]').click(); }); await sleep(300); await ev(() => document.querySelector('#libKind button[data-k="sets"]') && document.querySelector('#libKind button[data-k="sets"]').click()); await sleep(300); };
  const hostileRid = String(4390000001), realKey = R.keyOf('back');
  {
    const surfaces = [
      ['orders', async () => { await mode('orders'); }], ['library', async () => { await mode('library'); }],
      ['engraving editor', async () => { await R.openEngraveDetail(w, 'both', { face: 'back' }); }],
      ['order window (markup)', async () => { await mode('orders'); await ev(k => OrderWin.open(k), `${hostileRid}_${hostileRid}1`); await sleep(1000); }],
      ['sheet window', async () => { await mode('library'); await ev(() => CN.openLibrarySheet('gfA1')); await sleep(1000); }],
      ['settings', async () => { await mode('nest'); await ev(() => { try { openSettingsNow(); } catch (e) { return e.message; } }); await sleep(600); }],
    ];
    // the orders that carry markup in every field, put on screen: found by the markup itself (the search reads every field), as a list and as cards, and in Review by number
    step = 'the markup orders on screen';
    await mode('orders'); await sleep(500);
    const shownN = await ev(() => { const q = document.getElementById('ordQ'); q.value = 'data-pwn'; q.dispatchEvent(new Event('input', { bubbles: true })); return 1; }); await sleep(900);
    say('rows found by "data-pwn": ' + await ev(() => document.querySelectorAll('#ordItems [data-key]').length));
    for (const v of ['Cards', 'List']) { await ev(v => { const b = [...document.querySelectorAll('#ordersView button')].find(x => x.textContent.trim() === v); b && b.click(); }, v); await sleep(700); }
    await ev(() => { const q = document.getElementById('ordQ'); q.value = ''; q.dispatchEvent(new Event('input', { bubbles: true })); });
    await mode('review'); await sleep(500);
    for (const rid of [hostileRid, '4390000002', '4390000004']) { await ev(rid => { const q = document.getElementById('rvOrderFind'); if (q) { q.value = rid; q.dispatchEvent(new Event('input', { bubbles: true })); } }, rid); await sleep(500); }
    await ev(() => { const q = document.getElementById('rvOrderFind'); if (q) { q.value = ''; q.dispatchEvent(new Event('input', { bubbles: true })); } });
    cov.add('markup orders on screen');
    await checkpoint('the markup orders on screen');
    for (const [name, open] of surfaces) {
      step = 'open ' + name;
      try { await open(); } catch (e) { say(`open ${name}: ${String(e.message).split('\n')[0].slice(0, 100)}`); }
      await typeAll(name, name === 'settings' ? 4 : 2);
      if (/window|employee|settings/.test(name)) await checkpoint('hostile text: ' + name); else { await mode('nest'); await checkpoint('hostile text: ' + name); }
      if (name === 'engraving editor') await tour();
    }
    // the global search: "/" where nothing is being typed, Ctrl+K anywhere
    for (const m of ['orders']) {
      await mode(m); await sleep(500); step = 'search on ' + m;
      await esc3(); await ev(() => document.activeElement && document.activeElement.blur());
      for (const how of ['/', 'Control+k']) {
        await page.keyboard.press(how); await sleep(250);
        const open = await ev(() => window.OrderSearch && OrderSearch.isOpen());
        if (!open) { say(`${how} did not open the search on ${m}`); continue; }
        cov.add('search');
        for (const p of [MARKUP[mkI++ % MARKUP.length]('search' + mkI), ...shuffle(STRESS).slice(0, 2), '4290000103']) {
          step = `search on ${m}: ${JSON.stringify(p.slice(0, 22))}`;
          await page.keyboard.press('Control+a'); await page.keyboard.insertText(p).catch(() => {}); await sleep(220);
          await page.keyboard.press('ArrowDown').catch(() => {}); await page.keyboard.press('Enter').catch(() => {}); await sleep(250);
          if (await ev(() => !!document.querySelector('dialog[open]#orderWin'))) { await ev(() => OrderWin.close && OrderWin.close()); await sleep(250); }
          if (!(await ev(() => OrderSearch.isOpen()))) { await page.keyboard.press(how); await sleep(200); }
        }
        await page.keyboard.press('Escape'); await sleep(250);
      }
      await checkpoint('search on ' + m);
    }
    // a hostile name signs the work: approve once, send a message, then look at every place the name is written
    step = 'a hostile employee name signs the work';
    const evil = MARKUP[0]('employee'), evil2 = MARKUP[2]('employee2');
    await ev(n => { localStorage.setItem('cn.employee', n); }, evil);
    await R.openEngraveDetail(w, 'back', { face: 'back' }).catch(() => {});
    await ev(() => { const b = document.querySelector('.egApproveButton:not([disabled])'); if (b) b.click(); }); await sleep(900);
    await mode('orders'); await ev(k => OrderWin.open(k), realKey); await sleep(900);
    await ev(() => { const i = document.getElementById('owInput'); if (i) { i.value = 'signed by a hostile name'; i.dispatchEvent(new Event('input', { bubbles: true })); document.getElementById('owSend').click(); } }); await sleep(800);
    for (const v of ['timeline', 'sheet', 'info']) { await ev(v => { const b = document.querySelector(`[data-ow-view="${v}"]`); b && b.click(); }, v); await sleep(500); }
    await tour();
    await ev(n => { localStorage.setItem('cn.employee', n); }, evil2);
    await tour();
    await ev(() => { localStorage.setItem('cn.employee', 'Test Operator'); });
    await checkpoint('a hostile employee name signed the work');
  }

  phase('3 hostile text');
  /* ═══ 4 · wrong files ═══ */
  {
    const noise = Buffer.alloc(1500000); for (let i = 0; i < noise.length; i++) noise[i] = Math.floor(rnd() * 256);
    const PNG = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAC0lEQVR4nGP4DwQACfsD/Z8fLAAAAABJRU5ErkJggg==', 'base64');
    const FILES = [
      { name: 'notes.pdf', type: 'application/pdf', buf: Buffer.from('this is not a pdf at all') }, { name: 'empty.ai', type: 'application/postscript', buf: Buffer.alloc(0) },
      { name: 'fake.zip', type: 'application/zip', buf: Buffer.from('PK\u0003\u0004 nope nope nope') }, { name: 'noise.pdf', type: 'application/pdf', buf: noise }, { name: 'virus.exe', type: 'application/x-msdownload', buf: Buffer.from('MZ\u0090') },
      { name: 'GF <img src=x onerror=__pwn("name")>.pdf', type: 'application/pdf', buf: Buffer.from('%PDF-1.4\n%%EOF') }, { name: '"><b data-pwn>.ai', type: '', buf: Buffer.from('%!PS-Adobe-3.0\n') },
      { name: 'photo.png', type: 'image/png', buf: PNG }, { name: 'no-extension', type: '', buf: Buffer.from('x') }, { name: '.hidden.dxf', type: '', buf: Buffer.from('0\nSECTION\n0\nEOF\n') }
    ];
    const inPage = FILES.map(f => ({ name: f.name, type: f.type, b64: f.buf.toString('base64') }));
    const reading = () => ev(() => CN.S.sources.filter(s => !['ready', 'error', 'master', 'nested', 'done'].includes(s.state)).map(s => s.name + ':' + s.state));
    await mode('nest'); await sleep(500); step = 'wrong files in the file box';
    await page.setInputFiles('#fileInput', FILES.map(f => ({ name: f.name, mimeType: f.type || 'application/octet-stream', buffer: f.buf }))).catch(e => say('setInputFiles: ' + e.message.split('\n')[0]));
    cov.add('files:box');
    for (const t = Date.now(); Date.now() - t < 60000;) { if ((await ev(() => CN.S.sources.length)) >= 4 && !((await reading()) || []).length) break; await sleep(500); }
    const left = await reading(); if (left && left.length) bad('stuck', 'wrong files still "reading" after 60 s: ' + left.join(', '));
    say('sources: ' + JSON.stringify(await ev(() => CN.S.sources.map(s => s.name.slice(0, 18) + ':' + s.state))));
    await checkpoint('wrong files in the file box');
    // dropped: on the page, on each metal's card, on Review's cards; text only; a charm id that is nobody's; a drag that leaves and comes back
    const drop = (sel, o) => ev(async ({ sel, o, files }) => {
      const targets = sel === 'document' ? [document] : [...document.querySelectorAll(sel)].slice(0, 3); if (!targets.length) return 0;
      for (const el of targets) {
        const dt = new DataTransfer();
        if (o.text != null) dt.setData('text/plain', o.text);
        if (o.charm != null) dt.setData('text/charm', o.charm);
        if (o.files) for (const f of files) dt.items.add(new File([Uint8Array.from(atob(f.b64), c => c.charCodeAt(0))], f.name, { type: f.type }));
        const fire = (type, target = el) => target.dispatchEvent(new DragEvent(type, { bubbles: true, cancelable: true, dataTransfer: dt }));
        fire('dragenter'); fire('dragover'); if (o.leave) { fire('dragleave'); fire('dragenter'); fire('dragover'); } if (!o.noDrop) fire('drop'); else fire('dragleave');
      }
      return targets.length;
    }, { sel, o, files: inPage });
    for (const [name, sel] of [['the page', 'document'], ['a Review card', '#rvList > .reviewListRow']]) {
      if (name === 'a Review card') { await mode('review'); await sleep(700); } else await mode('nest');
      for (const [what, o] of [['wrong files', { files: true }], ['wrong files, leaving and coming back', { files: true, leave: true }], ['dragged text', { text: 'hello <b data-pwn>' }], ['an unknown charm', { charm: 'nobody:9', text: 'x' }], ['a drag that never drops', { files: true, noDrop: true }]]) {
        step = `drop ${what} on ${name}`;
        const n = await drop(sel, o); if (n) cov.add('drop:' + name);
        await sleep(500);
        if (await ev(() => !!document.querySelector('#dropAll.on'))) bad('stuck', `the page-wide drop veil stays on after "${what}" on ${name}`);
      }
      for (const t = Date.now(); Date.now() - t < 30000;) { if (!((await reading()) || []).length) break; await sleep(500); }
      const stuck = await reading(); if (stuck && stuck.length) bad('stuck', `files dropped on ${name} still "reading" after 30 s: ${stuck.join(', ')}`);
      await checkpoint('wrong files dropped on ' + name);
    }
    // a real vector file on a Review card that takes designs: the question before the first one (#cuAsk) asked twice, answered by Esc, a drop while it is open, then "yes"
    {
      const vec = { name: 'GF_comet.ai', type: 'application/postscript', b64: fs.readFileSync(path.join(__dirname, 'fixtures', 'COMET_9427.ai')).toString('base64') };
      const dropVec = n => ev(async ({ vec, n }) => {
        const el = document.querySelector('#rvList > .reviewListRow.cuDropOk') || document.querySelector('#rvList > .reviewListRow'); if (!el) return 0;
        for (let i = 0; i < n; i++) { const dt = new DataTransfer(); dt.items.add(new File([Uint8Array.from(atob(vec.b64), c => c.charCodeAt(0))], vec.name, { type: vec.type })); for (const t of ['dragenter', 'dragover', 'drop']) el.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt })); }
        return 1;
      }, { vec, n });
      const asks = () => ev(() => ({ n: document.querySelectorAll('#cuAsk').length, shown: [...document.querySelectorAll('#cuAsk')].filter(e => e.getBoundingClientRect().width > 0 && !e.hidden).length, yes: !!document.querySelector('#cuAsk [data-ask-yes]'), no: !!document.querySelector('#cuAsk [data-ask-no]') }));
      const designs = from => st.calls.slice(from).flatMap(c => ((c.body && c.body.events) || []).filter(e => e.type === 'designDropped'));
      await mode('review'); await sleep(900); step = 'a vector file dropped twice on a Review card';
      const from = st.calls.length;
      const dropped = await dropVec(2); say('vector file dropped on a Review card: ' + dropped);
      if (dropped) {
        await sleep(600); const a1 = await asks();
        if (!a1.n) say('no question (#cuAsk) on a first vector file: not in this build');
        else {
          cov.add('cuAsk');
          if (a1.n > 1) bad('twice', `two drops made ${a1.n} questions (#cuAsk)`);
          await page.keyboard.press('Escape'); await sleep(500);
          const a2 = await asks(); if (a2.shown) bad('stuck', 'Esc did not close the question (#cuAsk)');
          if (designs(from).length) bad('twice', 'Esc on the question still added the design');
          step = 'a vector file dropped while the question is open';
          await dropVec(1); await sleep(500); await dropVec(1); await sleep(500);
          const a3 = await asks(); if (a3.n > 1) bad('twice', `a drop while the question is open made ${a3.n} questions`);
          step = 'the question answered yes';
          const yes = await centreOf('#cuAsk [data-ask-yes]'); if (yes) await page.mouse.click(yes.x, yes.y, { clickCount: 3, delay: 12 });
          await sleep(1500); await settleNet();
          const rows = await ev(() => [...document.querySelectorAll('#cuList .cuFile')].filter(x => !x._leaving).length), added = designs(from).length;
          if (rows !== 1 || added > 1) bad('twice', `one file dropped, a second drop and a third while the question was open, "yes" pressed three times: ${rows} design row(s) and ${added} designDropped event(s), once expected`);
          if ((await asks()).shown) bad('stuck', 'the question (#cuAsk) is still open after yes');
        }
      }
      await checkpoint('a vector file on a Review card');
    }
    // a file into the order window's picture box, into a custom order's designs box
    step = 'a wrong file in the order window';
    await mode('orders'); await ev(k => OrderWin.open(k), realKey); await sleep(1000);
    await page.setInputFiles('#owFile', FILES.map(f => ({ name: f.name, mimeType: f.type || 'application/octet-stream', buffer: f.buf }))).catch(e => say('owFile: ' + e.message.split('\n')[0]));
    await sleep(700); await checkpoint('a wrong file in the order window');
  }

  phase('4 wrong files');
  /* ═══ 5 · closing early ═══ */
  {
    step = 'a message sent, the window closed at once';
    const rid = made.back.rid, msgs = () => [...st.docs.entries()].filter(([k, v]) => k.startsWith(`Brites_Orders/${rid}/messages/`) && v.text === 'closed early').length;
    const m0 = msgs();
    await mode('orders'); await ev(k => OrderWin.open(k), realKey); await sleep(1000);
    slow = 900;
    await ev(() => { const i = document.getElementById('owInput'); i.focus(); i.value = 'closed early'; i.dispatchEvent(new Event('input', { bubbles: true })); });
    await page.keyboard.press('Enter'); await page.keyboard.press('Enter'); await page.keyboard.press('Escape');   // sent twice over, the window shut at once
    slow = 0; await sleep(1500); await settleNet(); await sleep(1000);
    const sent = msgs() - m0; if (sent !== 1) bad('twice', `a message sent and the window closed at once was stored ${sent} times (once expected)`);
    await ev(k => OrderWin.open(k), realKey); await sleep(900); await checkpoint('a message sent, the window closed at once');

    step = 'settings saved and closed at once';
    await mode('nest');
    await ev(() => { try { openSettingsNow(); } catch (_) {} }); await sleep(700);
    if (await ev(() => !!document.querySelector('#dlgSettings[open]'))) {
      cov.add('settings');
      const c = await centreOf('#btnSaveSettings'); if (c) { await page.mouse.click(c.x, c.y, { clickCount: 2, delay: 10 }); await page.keyboard.press('Escape'); }
      await sleep(600);
      const stored = await ev(() => { try { return JSON.parse(localStorage.getItem('cn.settings')); } catch (e) { return 'unreadable: ' + e.message; } });
      if (!stored || typeof stored === 'string') bad('settings', 'the saved settings cannot be read back: ' + stored);
    } else say('settings did not open');
    await checkpoint('settings saved and closed at once');

    step = 'Back with a window open';
    await mode('orders'); await ev(k => OrderWin.open(k), realKey); await sleep(800);
    await ev(() => history.back()); await sleep(500); await ev(() => history.forward()); await sleep(500);
    await checkpoint('Back with a window open');
  }

  phase('5 closing early');
  /* ═══ 8 · the address, a refresh, the network gone ═══ */
  if (!over('the address, a refresh, the network gone')) {
    step = 'a typed address';
    await mode('orders'); await sleep(500);
    for (const h of ['bogus', 'a\\']) {
      step = `address #${h}`;
      await ev(h => { location.hash = h; }, h); await sleep(350);
      await ev(() => history.back()); await sleep(250); await ev(() => history.forward()); await sleep(250);
      await checkpoint(`address #${h}`);
      await mode('orders');
    }
    await seedRows();
    step = 'a refresh in the middle of a load';
    slow = 900;
    await page.locator('.modeSeg button[data-mode="library"]').click({ timeout: 2000, noWaitAfter: true }).catch(() => {});
    await sleep(150);
    await page.reload({ waitUntil: 'commit' }).catch(() => {}); slow = 0;
    await page.waitForFunction(() => window.CN && window.Orders && CN.S.cloud.ok === true, null, { timeout: 60000 }).catch(() => bad('stuck', 'the page never finished starting after a refresh in the middle of a load'));
    await checkpoint('a refresh in the middle of a load');
    await seedRows();
    step = 'Back out of the app and forward again';
    await page.goBack({ timeout: 8000 }).catch(() => {}); await sleep(300);
    await page.goBack({ timeout: 8000 }).catch(() => {}); await sleep(300);
    await page.goForward({ timeout: 8000 }).catch(() => {}); await sleep(300);
    await page.goForward({ timeout: 8000 }).catch(() => {});
    await page.waitForFunction(() => window.CN && window.Orders && CN.S.cloud.ok === true, null, { timeout: 60000 }).catch(() => bad('stuck', 'the page never finished starting after Back and Forward'));
    await checkpoint('Back out of the app and forward again');
    await seedRows();
    // the network gone while he clicks around
    step = 'the network gone while he clicks around';
    await context.setOffline(true);
    for (let round = 0; round < 2; round++) for (const t of shuffle(['nest', 'orders', 'engrave', 'review', 'library', 'library', 'orders'])) { await page.locator(`.modeSeg button[data-mode="${t}"]`).click({ timeout: 1500, noWaitAfter: true }).catch(() => {}); await sleep(120); }
    for (const [what, sel] of [['Pull orders', '#ordPull'], ['Nest all ready', '#btnNestAll']]) { const c = await centreOf(sel); if (c) await page.mouse.click(c.x, c.y, { clickCount: 2, delay: 10 }); await sleep(250); }
    await sleep(1500); await context.setOffline(false); await sleep(500);
    await checkpoint('the network gone while he clicks around');
    // settings that cannot be read: the page starts with its own
    step = 'start with settings and a name made of markup';
    await ev(() => { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, runMode: '<b data-pwn>', dsOrigin: 'javascript:__pwn(1)', budgetS: 'NaN', minPt: -5, sound: {}, review: [], naming: null })); localStorage.setItem('cn.employee', '<b data-pwn>x</b><img src=x onerror="__pwn(\'emp\')">'); });
    await page.reload({ waitUntil: 'load' }).catch(() => {});
    await page.waitForFunction(() => window.CN && window.Orders && CN.S.cloud.ok !== null, null, { timeout: 60000 }).catch(() => bad('stuck', 'the page never started with settings and a name made of markup'));
    await seedRows(); await tour(); await checkpoint('start with settings and a name made of markup');
  }

  phase('8 the address, a refresh, the network gone');
  /* ═══ 6 · a phone ═══ */
  if (!over('a phone')) {
    await page.setViewportSize({ width: 375, height: 667 }); onPhone = true; await sleep(600);
    const reach = sel => ev(sel => { const e = document.querySelector(sel); if (!e) return 'missing'; e.scrollIntoView({ block: 'center', inline: 'center' }); const b = e.getBoundingClientRect(); if (b.width < 2 || b.height < 2) return 'no size'; const cx = b.left + b.width / 2, cy = b.top + b.height / 2; if (cx < 0 || cx > innerWidth || cy < 0 || cy > innerHeight) return `centre off screen (${Math.round(cx)},${Math.round(cy)})`; const t = document.elementFromPoint(cx, cy); return t && (t === e || e.contains(t) || t.contains(e)) ? 'ok' : 'covered by ' + (t && (t.id || t.className || t.tagName)); }, sel);
    for (const m of ['orders']) {
      await mode(m); await sleep(600); step = 'phone: ' + m;
      const over = await ev(() => document.documentElement.scrollWidth - innerWidth); if (over > 2) bad('layout', `${m}: the page scrolls sideways by ${over} px at 375 px`);
      const tab = await reach(`.modeSeg button[data-mode="${m}"]`); if (tab !== 'ok') bad('layout', `the ${m} tab cannot be tapped at 375 px: ${tab}`);
      const must = { orders: ['#ordQ', '#ordPull'], library: ['#libSearch'], nest: ['#btnRail'] }[m] || [];
      if (m === 'nest') await ev(() => document.getElementById('btnRail').scrollIntoView());
      for (const sel of must) { const r = await reach(sel); if (r !== 'ok') bad('layout', `${sel} on ${m} cannot be used at 375 px: ${r}`); }
      const wide = await ev(() => { const W = innerWidth, out = []; const scroller = e => { for (let p = e.parentElement; p && p !== document.body; p = p.parentElement) { const s = getComputedStyle(p); if (/(auto|scroll)/.test(s.overflowX) && p.scrollWidth > p.clientWidth + 1) return true; } return false; }; for (const e of document.querySelectorAll('#stage button, #stage input, #stage select, #stage textarea, .topbar button')) { const b = e.getBoundingClientRect(); if (b.width < 2 || e.closest('[hidden],.hidden,.railOff,aside') || getComputedStyle(e).visibility === 'hidden') continue; if ((b.left > W - 4 || b.right < 4) && !scroller(e)) out.push((e.id || __mkLabel(e) || e.tagName).slice(0, 24)); } return out.slice(0, 6); });
      if (wide && wide.length) say(`phone ${m}: controls beyond the right edge with no way to scroll to them: ${wide.join(', ')}`), cov.add('layout-note:' + m + ':' + wide.join('/'));
      await checkpoint('phone: ' + m);
    }
    // the windows: their close button is on screen and works
    for (const [name, open, close] of [['order window', async () => { await mode('orders'); await ev(k => OrderWin.open(k), `${hostileRid}_${hostileRid}1`); await sleep(1000); }, '#owClose'], ['sheet window', async () => { await mode('library'); await sleep(600); await ev(() => CN.openLibrarySheet('gfA1')); await page.waitForSelector('dialog[open] [data-r="close"]', { timeout: 6000 }).catch(() => {}); await sleep(500); }, 'dialog[open] [data-r="close"]'],
      ['settings', async () => { await ev(() => { try { openSettingsNow(); } catch (_) {} }); await sleep(700); }, '#dlgSettings .dlgFoot button[value="cancel"]']]) {
      step = 'phone: ' + name; try { await open(); } catch (_) {}
      if (!(await ev(() => !!document.querySelector('dialog[open]')))) { say(name + ' did not open'); continue; }
      const dw = await ev(() => { const d = [...document.querySelectorAll('dialog[open]')].pop(), b = d.getBoundingClientRect(); return { id: d.id, w: Math.round(b.width), l: Math.round(b.left), r: Math.round(b.right), h: Math.round(b.height) }; });
      say(`phone ${name}: dialog #${dw.id} ${dw.w}px`);
      if (dw.w > 375 + 2 || dw.l < -2 || dw.r > 375 + 2) bad('layout', `the ${name} is ${dw.w} px wide (${dw.l}..${dw.r}) on a 375 px screen`);
      const r = await reach(close); if (r !== 'ok') bad('layout', `the ${name}'s close button cannot be used at 375 px: ${r}`);
      await esc3(); await sleep(300);
    }
    await page.setViewportSize({ width: 1440, height: 950 }); onPhone = false; await sleep(500);
    await checkpoint('back to a desk');
  }

  phase('6 a phone');
  /* ═══ 7 · keys ═══ */
  if (!over('random keys')) {
    const KEYS = ['Tab', 'Shift+Tab', 'Enter', 'Space', 'Escape', 'ArrowDown', 'ArrowUp', 'ArrowLeft', 'ArrowRight', '/', 'Control+k', 'Home', 'End', 'PageDown', 'Backspace', 'Delete', 'a', '1', '?', 'Control+z', 'Control+a', 'Shift+Tab', 'Tab'];
    for (const m of ['engrave']) {
      await mode(m); await sleep(600); step = 'random keys on ' + m;
      for (let i = 0; i < 30; i++) { await page.keyboard.press(pick(KEYS)).catch(() => {}); await sleep(Math.floor(rnd() * 35)); }
      cov.add('keys:' + m);
      await checkpoint('random keys on ' + m);
    }
  }

  phase('7 keys');
  console.log('covered: ' + [...cov].join(' | '));
  await w.close();
  console.log(issues.length ? `\nadv-monkey: ${issues.length} problem(s)` : '\nadv-monkey: ok, the clumsy user left nothing broken');
  process.exitCode = issues.length ? 1 : 0;
})().catch(e => { console.error(e); process.exitCode = 1; });
