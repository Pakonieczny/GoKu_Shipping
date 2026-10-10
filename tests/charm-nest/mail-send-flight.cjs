// The Charm Sorter's customer messages send the way the inbox does (Paul, 10 Oct 2026: "Make sure to incorporate the same
// animation from the Email app that we use to animate the text flying from the input text box to the chat history above.
// These animations must feel exactly the same"). The animation is ONE component, send-flight.js (window.SendFlight), loaded
// by both pages; the sorter's Customer tab and the engraving card's message column call it through the same contract the
// inbox does (prepare before the box is emptied, launch into the bubble, retract when the server refuses, flash, hold the
// thread's redraw while the words are in the air). Headless Chromium against the fake site (bridge-server.cjs); the inbox
// link's endpoint is answered here, so nothing reaches Etsy, the inbox or a customer.
//   · the words: a copy of every word of what was written lifts from the box inside the open window (a pop-up would cover
//     them on the page), flies onto its place in the bubble; the bubble is there at once and keeps the state chip it has today;
//   · the box is emptied at once and stays as tall as it was until the words have landed, then eases back (the inbox's
//     settle: edge copies, no height animation); the history slides up on transforms (rows), never the thread box itself;
//   · the thread is not redrawn while the words are in the air (the server's answer comes in the middle of the flight), then
//     it is, with the same bubble id, so nothing jumps;
//   · only transform / opacity / clip-path move; the frames are even when the machine is quiet;
//   · a send the server refuses: the bubble folds away, the words are back in the box (and in its draft), the outbox is
//     empty, the box flashes; if new words were typed meanwhile, the refused message keeps its place as "Not sent" with
//     Edit (nothing is lost); a lost connection is NOT a refusal (the message stays, "Sending…", the outbox keeps it);
//   · the sandbox side flies the same (its send is the sandbox's own); the phone width the same; the engraving card's
//     message column (CustomerMail.cardPane) the same; prefers-reduced-motion: the bubble appears and the box is fitted at once;
//   · the inbox page loads the same file (no inline copy any more), its public API is unchanged, and its own flight still
//     runs (tests/etsy-mail/adv-inbox-ui.cjs is run as a child unless SF_NO_INBOX=1).
//   node tests/charm-nest/mail-send-flight.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, SF_SHOTS=<folder for pictures>)
const path = require('path'), fs = require('fs'), os = require('os'), cp = require('child_process');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const A = { rid: '4176208841', tid: '41762088411', sku: 'TINY_TAG' };
const order = (o, buyer) => ({ receiptId: o.rid, orderNumber: o.rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: o.tid, listingId: '1800000' + o.tid.slice(-3), sku: o.sku, title: o.sku.replace(/_/g, ' ') + ' necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: ['Initial: H'], buyerMessage: '' }] });
const TEXT = 'Hello Hannah, thank you for the H. We will engrave it in a lovely script and send it on its way very soon. Many thanks, CustomBrites';
const WORDS = TEXT.split(/\s+/).length;
const shots = process.env.SF_SHOTS || '';

// the customer's side, answered here
function makeFake() {
  const now = Date.now();
  const messages = [];
  for (let i = 0; i < 24; i++) messages.push(i % 2
    ? { id: 'c' + i, side: 'customer', who: 'Hannah Whitford', atMs: now - (30 - i) * 3600e3, text: 'CUST-' + i + ': an H please, thank you so much, that sounds lovely. ' + (i % 3 ? 'Could it be in the same gold as the chain?' : '') }
    : { id: 'c' + i, side: 'us', who: 'Paul', atMs: now - (30 - i) * 3600e3, text: 'CUST-' + i + ': which initial would you like on the back of the necklace? We can do a script or a block letter.', status: 'sent', delivered: true });
  const eng = { id: 'eng-sf-1', receiptId: A.rid, sandbox: false, v: 1, status: 'open', unread: 0, createdAtMs: now - 30 * 3600e3, startedAtMs: now - 30 * 3600e3, customer: { name: 'Hannah Whitford' }, scope: 'order', link: 'thread', threadId: 't1', messages };
  return { eng, mode: 'ok', delay: 150, asks: [], nextError: 'The inbox link refused it (test)' };
}

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const fails = []; const check = (ok, msg) => { console.log((ok ? '  ✓ ' : '  ✗ ') + msg); if (!ok) fails.push(msg); };

  // ── what needs no browser: both pages load the one file, the build ships it, the API is the inbox's ──
  const inboxHtml = fs.readFileSync(path.join(root, 'etsy-mail-1.html'), 'utf8'), sorterHtml = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
  check(/<script src="send-flight\.js\?v=[^"]*-sf1[^"]*"><\/script>/.test(inboxHtml), 'the inbox page loads send-flight.js');
  check(!/window\.SendFlight\s*=/.test(inboxHtml) && !/em-flight-layer\s*\{/.test(inboxHtml), 'the inbox page no longer carries its own copy of the component or its rules');
  check(/<script src="send-flight\.js\?v=[^"]*-sf1[^"]*"><\/script>/.test(sorterHtml) && sorterHtml.indexOf('send-flight.js') < sorterHtml.indexOf('charm-nest-mail.js?v='), 'the sorter page loads the same file, before the customer mail script');
  check(/charm-nest-mail\.js\?v=20261005-pv-seal-zp1-mr2-mq\d+(-[a-z0-9]+)*-sf1(-[a-z0-9]+)*"/.test(sorterHtml), 'the customer mail script keeps every earlier cache token and adds -sf1');
  check(/"send-flight\.js"/.test(fs.readFileSync(path.join(root, 'scripts/build-public.cjs'), 'utf8')), 'the public build ships send-flight.js');
  const SF = require(path.join(root, 'send-flight.js'));
  check(JSON.stringify(Object.keys(SF).sort()) === JSON.stringify(['afterFlight', 'flash', 'holding', 'launch', 'prepare', 'retract']), 'the public API is the inbox component\'s own: ' + Object.keys(SF).sort().join(', '));

  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fake = makeFake();

  /** A fresh sorter page with one order, connected to the (fake) inbox link, at a size and in a world (real / sandbox). */
  async function sorter({ width = 1440, height = 900, sandbox = false, reduced = false, openOrder = true } = {}) {
    const context = await browser.newContext({ viewport: { width, height }, reducedMotion: reduced ? 'reduce' : 'no-preference' });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.route(/\/\.netlify\/functions\/etsyMailOrderLink/, async r => {
      let b = {}; try { b = JSON.parse(r.request().postData() || '{}'); } catch (_) {}
      const op = b.op, eng = fake.eng;
      if (op === 'ask') {
        fake.asks.push(b);
        if (fake.mode === 'offline') return r.abort();
        await new Promise(x => setTimeout(x, fake.delay));
        if (fake.mode === 'refuse') return r.fulfill({ status: 400, contentType: 'application/json', body: JSON.stringify({ error: fake.nextError }) });
        eng.messages.push({ id: 'out_' + b.clientId, itemId: b.clientId, side: 'us', who: 'Test Operator', atMs: Date.now(), text: b.text, status: 'queued', images: [], cards: [] }); eng.v++;
        return r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(Object.assign({}, eng, { messages: eng.messages.slice(), earlier: [] })) });
      }
      const body = op === 'order' ? { ok: true, n: 1, engagements: [eng], active: eng, conversation: { customer: eng.customer } }
        : op === 'thread' ? eng : op === 'health' ? { ok: true, level: 'ok' } : op === 'history_info' ? { ok: true, total: 2, threads: [] } : { ok: true, n: 1, changes: [], v: 1 };
      return r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) });
    });
    await context.addInitScript(sb => {
      try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: sb ? 'on' : 'off' })); localStorage.setItem('cn.employee', 'Test Operator'); localStorage.setItem('cn.mail.station', JSON.stringify('k-sf-test')); localStorage.setItem('cn.mail.tab', JSON.stringify('customer')); sessionStorage.setItem('__seeded', '1'); } } catch (_) {}
      window.prompt = () => 'Test Operator';
      // every Element.animate call, for what the flight is made of
      const A0 = Element.prototype.animate; window.__anims = [];
      Element.prototype.animate = function (k, o) {
        try { const props = new Set(); for (const f of Array.isArray(k) ? k : k ? [k] : []) for (const p of Object.keys(f)) if (!['offset', 'easing', 'composite'].includes(p)) props.add(p);
          window.__anims.push({ t: performance.now(), cls: String(typeof this.className === 'string' ? this.className : '').slice(0, 40), tag: this.tagName, props: [...props], dur: o && o.duration, delay: (o && o.delay) || 0 }); } catch (_) {}
        return A0.call(this, k, o);
      };
    }, sandbox);
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.CustomerMail && window.SheetWin && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async ({ orders }) => {
      await Orders.loadMaps(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); CN.setMode('orders'); Orders.render();
    }, { orders: [order(A, 'Hannah Whitford')] });
    if (openOrder) {
      await page.click(`#ordItems [data-key="${A.rid}_${A.tid}"]`);
      await page.waitForFunction(() => { const d = document.getElementById('orderWin'); return d.open && !d.getAnimations({ subtree: true }).some(a => a.playState === 'running' && a.effect && a.effect.getTiming().iterations !== Infinity); });
      await page.click('#owTabCust');
      await page.waitForFunction(() => /CUST-23/.test(document.querySelector('#owPaneCust .cmThread').textContent));
    }
    return { page, context, errors };
  }

  /** Press Send in a pane and watch it frame by frame. */
  async function sendWatch(page, pane, text, { ms = 2400, typeDuring = '' } = {}) {
    await page.fill(`${pane} textarea[data-cm="input"]`, text);
    await page.waitForTimeout(250);
    return page.evaluate(async ({ pane, ms, typeDuring }) => {
      const root = document.querySelector(pane), ta = root.querySelector('textarea[data-cm="input"]'), th = root.querySelector('.cmThread'), comp = root.querySelector('.cmComp');
      const out = { frames: [], writes: [], longTasks: [], deltas: [], toasts: [] };
      let po = null; try { po = new PerformanceObserver(l => l.getEntries().forEach(e => out.longTasks.push({ at: Math.round(e.startTime), d: Math.round(e.duration) }))); po.observe({ entryTypes: ['longtask'] }); } catch (_) {}
      const mo = new MutationObserver(ms2 => ms2.forEach(m => { const el = m.target; if (!el.closest || !el.closest('.cmThread')) return; const s = el.style; ['height', 'paddingTop', 'paddingBottom', 'marginBottom', 'top', 'left', 'width'].forEach(k => { if (s[k] && !out.writes.includes(k)) out.writes.push(k); }); }));
      mo.observe(th, { attributes: true, attributeFilter: ['style'], subtree: true });
      const firstId = th.lastElementChild.getAttribute('data-mid'), a0 = window.__anims.length;   // the newest message there already: it is the one the history is judged by
      const first = { getBoundingClientRect: () => { const e = th.querySelector('[data-mid="' + firstId + '"]'); return e ? e.getBoundingClientRect() : { top: 0 }; } };
      out.firstTop0 = Math.round(first.getBoundingClientRect().top); out.taH0 = Math.round(ta.getBoundingClientRect().height); out.compTop0 = Math.round(comp.getBoundingClientRect().top);
      out.threadTop0 = Math.round(th.getBoundingClientRect().top);
      const t0 = performance.now(); out.t0 = t0; let last = t0, run = true, probe = null, id = null;
      const tick = now => {
        const dt = now - last; out.deltas.push(dt); last = now;
        const L = document.querySelector('.em-flight-layer');
        if (!id && th.lastElementChild.getAttribute('data-mid') !== firstId) id = th.lastElementChild.getAttribute('data-mid');   // the new bubble's id is its clientId
        const bub = id ? th.querySelector('[data-mid="' + id + '"]') : null;
        if (bub && !probe) { probe = bub; bub.__sf = 1; }
        out.frames.push({ dt: Math.round(dt), t: Math.round(now - t0), taH: Math.round(ta.getBoundingClientRect().height), layer: !!L, words: L ? L.querySelectorAll('.em-flight-y').length : 0, layerIn: L ? (L.parentNode.id || L.parentNode.tagName) : '', hid: !!(bub && bub.classList.contains('em-flight-text-hidden')),
          sameNode: !!(bub && bub.__sf === 1), has: !!bub, bands: document.querySelectorAll('.em-flight-band').length, firstTop: Math.round(first.getBoundingClientRect().top), compTop: Math.round(comp.getBoundingClientRect().top),
          threadTop: Math.round(th.getBoundingClientRect().top), threadTf: getComputedStyle(th).transform, val: ta.value.length });
        if (run) requestAnimationFrame(tick);
      };
      requestAnimationFrame(tick);
      root.querySelector('[data-cm="send"]').click();
      if (typeDuring) setTimeout(() => { ta.value = typeDuring; ta.dispatchEvent(new Event('input', { bubbles: true })); }, 60);
      await new Promise(r => setTimeout(r, ms));
      run = false; mo.disconnect(); if (po) po.disconnect(); out.deltas.shift();
      out.id = id; out.value = ta.value; out.taH1 = Math.round(ta.getBoundingClientRect().height); out.sendDisabled = root.querySelector('[data-cm="send"]').disabled;
      out.anims = window.__anims.slice(a0).map(a => Object.assign({}, a, { t: Math.round(a.t - t0) }));
      const b = id && th.querySelector('[data-mid="' + id + '"]');
      out.bubble = b ? { text: b.querySelector('.cmBody') ? b.querySelector('.cmBody').textContent : '', chip: b.querySelector('.cmSt') ? b.querySelector('.cmSt').textContent : '', cls: b.className, sameNode: b.__sf === 1 } : null;
      out.outbox = JSON.parse(localStorage.getItem('cn.mail.outbox') || '[]').length;
      out.draftKeys = Object.keys(JSON.parse(localStorage.getItem('cn.mail.drafts') || '{}')).length;
      out.layerLeft = !!document.querySelector('.em-flight-layer'); out.bandsLeft = document.querySelectorAll('.em-flight-band').length;
      return out;
    }, { pane, ms, typeDuring });
  }
  const quiet = os.loadavg()[0] / os.cpus().length < 1.2;
  const lastWithLayer = T => Math.max(...T.frames.filter(f => f.layer).map(f => f.t));

  try {
    // ═══ 1 · the Customer tab, on the desktop ═══
    console.log('\n1 · the order window\'s Customer tab, desktop width');
    let s = await sorter();
    check(await s.page.evaluate(() => typeof window.SendFlight === 'object' && !!document.getElementById('sendFlightCss')), 'the sorter has SendFlight and its rules (one style element)');
    fake.mode = 'ok'; fake.asks.length = 0;
    let T = await sendWatch(s.page, '#owPaneCust', TEXT);
    const fl = T.frames.filter(f => f.layer);
    check(fl.length > 20, `the flight is on screen for ${fl.length} frames (${lastWithLayer(T)} ms)`);
    check(fl.every(f => f.layerIn === 'orderWin'), 'the flying words are drawn inside the open window (a modal window would cover them on the page): ' + [...new Set(fl.map(f => f.layerIn))]);
    check(Math.max(...fl.map(f => f.words)) === WORDS, `a copy of every word flies: ${Math.max(...fl.map(f => f.words))} of ${WORDS}`);
    const f0 = T.frames[0];
    check(f0.has && f0.hid, 'the bubble is there from the first frame, its own text held back while the words fly');
    check(T.frames.every(f => f.val === 0) && T.value === '' && T.sendDisabled, 'the box is emptied at once and its Send is off');
    const pre = T.frames.filter(f => f.layer && f.t < lastWithLayer(T) - 160);
    check(pre.every(f => Math.abs(f.taH - T.taH0) <= 1) && T.taH1 < T.taH0 - 10, `the box keeps its height while the words fly (${T.taH0}px) and settles after (${T.taH1}px)`);
    check(T.frames.some(f => f.bands > 0) && T.anims.some(a => a.cls.includes('em-flight-band')), 'it settles with the inbox\'s edge copies (no height animation)');
    check(fake.asks.length === 1 && fake.asks[0].text === TEXT && fake.asks[0].sandbox === false, 'one message went to the (fake) link, once, as a real-side send');
    check(pre.length > 5 && pre.every(f => f.sameNode), 'the thread is not redrawn while the words are in the air (the server answered in the middle): the bubble is the same node throughout');
    check(!!T.bubble && T.bubble.text === TEXT && /Etsy helper|Sending|Queued|Delivered|Sent/.test(T.bubble.chip), `the bubble that lands carries the state chip it has today and the whole text: "${T.bubble && T.bubble.chip}"`);
    check(T.bubble && T.bubble.cls.includes('cmMsg us'), 'it is the same kind of bubble the thread always drew');
    check(T.frames.at(-1).has && !T.frames.at(-1).hid && !T.layerLeft && T.bandsLeft === 0, 'afterwards: the layer, the hidden text and the edge copies are all gone');
    const dur = T.anims.filter(a => a.cls.includes('em-flight-x')).map(a => a.dur);
    check(dur.length === WORDS && dur.every(d => d >= 440 && d <= 640), 'each word flies 440-640 ms, as in the inbox: ' + [...new Set(dur)].join('/'));
    check(T.anims.some(a => a.cls.includes('cmMsg') && a.dur === 300 && a.props.includes('clipPath') && a.props.includes('opacity')), 'the bubble opens with the inbox\'s 300 ms grow (clip-path + opacity)');
    const slid = T.anims.filter(a => a.cls.includes('cmMsg') && a.dur === 300 && a.props.length === 1 && a.props[0] === 'transform');
    check(slid.length >= 3, `the history slides on its rows' transforms (${slid.length} rows)`);
    check(!T.anims.some(a => a.cls.includes('cmThread')) && T.frames.every(f => f.threadTf === 'none' && f.threadTop === T.threadTop0), 'the thread box itself never moves (its rows do)');
    const tr = T.frames.filter(f => f.t > 0 && f.layer);
    check(Math.abs(tr[0].firstTop - T.firstTop0) <= 3, `the history starts where it was before Send (no jump): ${tr[0].firstTop} vs ${T.firstTop0}`);
    check(tr.at(-1).firstTop < T.firstTop0 - 20, 'the history ends pushed up by the new bubble');
    check(tr.some(f => f.t > 40 && f.t < 260 && f.firstTop < tr[0].firstTop - 2 && f.firstTop > tr.at(-1).firstTop + 2), 'the history slides up through in-between positions');
    check(pre.every(f => Math.abs(f.compTop - T.compTop0) <= 1), 'the composer stays where it was while the words fly');
    check(T.writes.length === 0, 'no layout property (height, padding, margin) is written on the rows during the flight' + (T.writes.length ? ': ' + T.writes : ''));
    check(!s.errors.length, 'no page errors' + (s.errors.length ? ': ' + s.errors.join(' | ') : ''));

    // the words' own flight is smooth: the server's answer (which redraws the pane) is kept out of it here, and the press itself
    // (the first frame: measuring, drawing the bubble) is judged apart, as the inbox's test does. The machine is shared, so the
    // best of three tries counts (a slow frame from somebody else's work is not the flight's).
    fake.delay = 3000;
    let best = null;
    for (let k = 0; k < 3; k++) {
      await s.page.fill('#owPaneCust textarea[data-cm="input"]', '');
      T = await sendWatch(s.page, '#owPaneCust', TEXT, { ms: 1800 });
      const flyEnd = lastWithLayer(T), inFlight = T.frames.filter(f => f.t > 120 && f.t < flyEnd);
      const r = { flyEnd, n: inFlight.length, worst: Math.max(...inFlight.map(f => f.dt)), press: T.frames[0].dt, after: Math.max(...T.frames.filter(f => f.t >= flyEnd).map(f => f.dt)),
        longIn: T.longTasks.filter(l => l.at - T.t0 > 120 && l.at - T.t0 < flyEnd).length };
      if (!best || r.worst < best.worst) best = r;
      if (r.worst <= 34 && !r.longIn) break;
      await new Promise(x => setTimeout(x, 3200));
    }
    console.log(`  · flight ${best.flyEnd} ms, ${best.n} frames, worst ${best.worst} ms; the press ${best.press} ms; after landing worst ${best.after} ms; quiet machine: ${quiet}`);
    check(best.worst <= 34, `no frame over 34 ms while the words fly (best of three: worst ${best.worst} ms)`);
    check(!best.longIn, 'no long task while the words fly');
    check(best.after <= 150, `the landing (the held redraw, the box settling) never stalls over 150 ms (${best.after} ms)`);
    await new Promise(r => setTimeout(r, 3200)); fake.delay = 150;
    // a second message right behind the first: the first flight is cut short, the second flies, nothing is lost
    fake.delay = 700; fake.asks.length = 0;
    await s.page.fill('#owPaneCust textarea[data-cm="input"]', 'First quick one.'); await s.page.click('#owPaneCust [data-cm="send"]');
    await s.page.waitForTimeout(120);
    await s.page.fill('#owPaneCust textarea[data-cm="input"]', 'And a second one right behind it.'); await s.page.click('#owPaneCust [data-cm="send"]');
    await s.page.waitForTimeout(2600);
    const two = await s.page.evaluate(() => [...document.querySelectorAll('#owPaneCust .cmThread .cmMsg.us .cmBody')].map(b => b.textContent).slice(-2));
    check(two[0] === 'First quick one.' && two[1] === 'And a second one right behind it.' && fake.asks.length === 2 && await s.page.evaluate(() => !document.querySelector('.em-flight-layer')), 'two quick sends: both bubbles are there in order, both reached the link, no layer left behind');
    fake.delay = 150;

    // ═══ 2 · the server refuses (right away, and after the words have landed) ═══
    console.log('\n2 · a send the server refuses');
    fake.mode = 'refuse'; fake.delay = 100; fake.asks.length = 0;
    T = await sendWatch(s.page, '#owPaneCust', TEXT, { ms: 2600 });
    check(T.value === TEXT, 'the words are back in the box');
    check(T.bubble === null && T.outbox === 0, 'the refused bubble is gone from the thread and the outbox is empty (nothing is left to send twice)');
    check(T.anims.some(a => a.cls.includes('cmMsg') && a.dur === 260 && a.props.includes('clipPath')), 'the bubble folded away with the inbox\'s 260 ms retract (clip-path + opacity)');
    check(T.anims.some(a => a.props.includes('boxShadow') && a.dur === 1400), 'the box flashes, as the inbox\'s does');
    check(T.draftKeys >= 1, 'the words are in the box\'s draft again (a reload keeps them)');
    check(await s.page.evaluate(() => /Not sent: .*back in the box/.test(document.getElementById('toasts') ? document.getElementById('toasts').textContent : document.body.textContent)), 'a plain "Not sent … back in the box" message says why');
    check(T.sendDisabled === false, 'Send is on again for the restored words');
    await s.page.fill('#owPaneCust textarea[data-cm="input"]', '');
    fake.delay = 1500; fake.asks.length = 0;
    T = await sendWatch(s.page, '#owPaneCust', TEXT, { ms: 3800 });
    check(T.frames.some(f => f.layer) && T.value === TEXT && T.bubble === null && T.outbox === 0, 'refused after the words had landed: the landed bubble folds away and the words are back');
    await s.page.fill('#owPaneCust textarea[data-cm="input"]', '');

    // new words typed meanwhile: the refused one stays as "Not sent" with Edit, nothing is lost
    fake.delay = 900; fake.asks.length = 0;
    T = await sendWatch(s.page, '#owPaneCust', 'This one will be refused.', { ms: 2600, typeDuring: 'Something else I am already writing' });
    check(T.value === 'Something else I am already writing' && T.outbox === 1 && !!T.bubble && /Not sent/.test(T.bubble.chip), 'new words typed meanwhile are not touched: the refused message stays in the thread as "Not sent" (outbox keeps it)');
    check(await s.page.evaluate(() => !!document.querySelector('#owPaneCust [data-cm-do="edit"]')), 'and offers Edit and send again');
    await s.page.click('#owPaneCust [data-cm-do="discard"]'); await s.page.fill('#owPaneCust textarea[data-cm="input"]', '');

    // a lost connection is not a refusal
    fake.mode = 'offline'; fake.delay = 0;
    T = await sendWatch(s.page, '#owPaneCust', 'Sent while the connection is down.', { ms: 2000 });
    check(T.value === '' && T.outbox === 1 && !!T.bubble && /Sending/.test(T.bubble.chip), 'no connection: the bubble stays "Sending…" and the outbox keeps it (it is tried again), nothing folds away');
    await s.page.evaluate(() => { localStorage.removeItem('cn.mail.outbox'); });
    check(!s.errors.length, 'no page errors in the refusals' + (s.errors.length ? ': ' + s.errors.join(' | ') : ''));
    fake.mode = 'ok'; fake.delay = 150;
    await s.context.close();

    // ═══ 3 · the sandbox side ═══
    console.log('\n3 · the Sandbox side (its send is simulated and stays in the sorter)');
    s = await sorter({ sandbox: true });
    fake.asks.length = 0;
    T = await sendWatch(s.page, '#owPaneCust', TEXT);
    check(fake.asks.length === 1 && fake.asks[0].sandbox === true, 'the message went out as a sandbox message');
    check(T.frames.filter(f => f.layer).length > 20 && Math.max(...T.frames.map(f => f.words)) === WORDS && T.value === '' && !!T.bubble && T.bubble.text === TEXT, 'the same flight: every word flies, the box is emptied, the bubble holds the whole text');
    check(T.taH1 < T.taH0 - 10 && T.bandsLeft === 0 && !T.layerLeft, 'and the same settle');
    fake.mode = 'refuse'; fake.delay = 100;
    T = await sendWatch(s.page, '#owPaneCust', TEXT, { ms: 2400 });
    check(T.value === TEXT && T.bubble === null && T.outbox === 0, 'a refused sandbox send folds back the same way');
    fake.mode = 'ok'; fake.delay = 150;
    await s.context.close();

    // ═══ 4 · the phone ═══
    console.log('\n4 · the phone width');
    s = await sorter({ width: 390, height: 800 });
    fake.asks.length = 0;
    T = await sendWatch(s.page, '#owPaneCust', TEXT);
    check(T.frames.filter(f => f.layer).length > 20 && Math.max(...T.frames.map(f => f.words)) === WORDS && T.frames.filter(f => f.layer).every(f => f.layerIn === 'orderWin'), 'the words fly on the phone');
    check(T.value === '' && !!T.bubble && T.bubble.text === TEXT && T.taH1 <= T.taH0 && T.bandsLeft === 0 && !T.layerLeft, 'the box is emptied, the bubble holds the text, nothing is left behind');
    check(T.writes.length === 0 && !T.anims.some(a => a.cls.includes('cmThread')), 'compositor properties only; the thread box stays');
    if (shots) { await s.page.screenshot({ path: path.join(shots, 'sorter-phone-after.png') }); }
    check(!s.errors.length, 'no page errors on the phone');
    await s.context.close();

    // ═══ 5 · reduced motion ═══
    console.log('\n5 · prefers-reduced-motion');
    s = await sorter({ reduced: true });
    T = await sendWatch(s.page, '#owPaneCust', TEXT, { ms: 1500 });
    check(!T.frames.some(f => f.layer) && !T.anims.some(a => a.cls.includes('em-flight')), 'no words fly and no flight animation is made');
    check(T.frames[0].has && T.frames[0].taH < T.taH0 - 10 && !!T.bubble && T.bubble.text === TEXT && T.value === '', 'the bubble is there at once and the box is fitted at once');
    fake.mode = 'refuse'; fake.delay = 100;
    T = await sendWatch(s.page, '#owPaneCust', TEXT, { ms: 1500 });
    check(T.value === TEXT && T.bubble === null && !T.anims.some(a => a.dur === 260), 'a refused send puts the words back without folding (no motion)');
    fake.mode = 'ok'; fake.delay = 150;
    await s.context.close();

    // ═══ 6 · the engraving card's message column (CustomerMail.cardPane), in a pop-up window ═══
    console.log('\n6 · the engraving card\'s message column');
    s = await sorter({ openOrder: false });
    await s.page.evaluate(() => {
      const row = B.orders.rows[0];
      const host = CustomerMail.cardPane({ key: row.key, row });
      const dlg = document.createElement('dialog'); dlg.id = 'sfCard'; dlg.style.cssText = 'width:560px;height:520px;padding:0;border:1px solid #888;overflow:hidden';
      const box = document.createElement('div'); box.id = 'sfCardBox'; box.style.cssText = 'display:flex;flex-direction:column;height:400px;overflow:hidden;margin:12px;border:1px solid #ccc;border-radius:12px';
      host.style.cssText = 'flex:1 1 auto;min-height:0'; box.appendChild(host); dlg.appendChild(box); document.body.appendChild(dlg); dlg.showModal();
    });
    await s.page.waitForFunction(() => /CUST-23/.test(document.querySelector('#sfCardBox .cmThread').textContent));
    fake.asks.length = 0;
    T = await sendWatch(s.page, '#sfCardBox', TEXT);
    check(T.frames.filter(f => f.layer).length > 20 && Math.max(...T.frames.map(f => f.words)) === WORDS && T.frames.filter(f => f.layer).every(f => f.layerIn === 'sfCard'), 'the card\'s words fly too, inside the window it is in');
    check(T.value === '' && !!T.bubble && T.bubble.text === TEXT && T.taH1 < T.taH0 - 10 && fake.asks.length === 1, 'box emptied, bubble holds the text, box settles, one message sent');
    check(T.bubble && /Etsy helper|Sending|Queued|Delivered|Sent/.test(T.bubble.chip), 'with its state chip: "' + (T.bubble && T.bubble.chip) + '"');
    fake.mode = 'refuse'; fake.delay = 100;
    T = await sendWatch(s.page, '#sfCardBox', TEXT, { ms: 2400 });
    check(T.value === TEXT && T.bubble === null && T.outbox === 0, 'a refused card send folds back the same way');
    fake.mode = 'ok'; fake.delay = 150;
    check(!s.errors.length, 'no page errors on the card' + (s.errors.length ? ': ' + s.errors.join(' | ') : ''));
    await s.context.close();

    // ═══ 7 · the inbox: the same file, its own flight still runs ═══
    console.log('\n7 · the inbox runs the same file');
    if (process.env.SF_NO_INBOX === '1') console.log('  – skipped (SF_NO_INBOX=1)');
    else {
      const r = cp.spawnSync(process.execPath, [path.join(root, 'tests/etsy-mail/adv-inbox-ui.cjs')], { env: Object.assign({}, process.env), encoding: 'utf8', timeout: 240000 });
      const o = (r.stdout || '') + (r.stderr || '');
      check(r.status === 0 && /the flying words are drawn on screen/.test(o) && !/FAIL/.test(o), 'adv-inbox-ui.cjs (the inbox\'s own flight, smoothness and box checks) passes with the shared file' + (r.status === 0 ? '' : '\n' + o.slice(-600)));
    }
  } finally { await browser.close(); await srv.close(); }
  if (fails.length) { console.error(`\n${fails.length} failed`); process.exit(1); }
  console.log('\nmail-send-flight: all checks passed');
}
main().catch(e => { console.error(e); process.exit(1); });
