// How a person works each station app, through the real pages: the sign-in the page asks for, the scanner page with its camera, the one real
// action of the station (scan, done, print), and the sign-out button. Nothing here reaches into a page's code except where a page has no button for
// it (the Sorting batch's sticker button is a row in a table: its own function is called, as the table's button does).
//   const A = Apps(W, cast);  const p = await A.pin(ctx, 'assembly-2.html', 'Michael V.');  await A.scan(scanner, rid);  await A.signOut(p);
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../../..');
const { sleep } = require('./world.cjs');

/** polls fn (in node) until it answers something true; says what it waited for when it never does */
async function until(fn, what, ms) {
  const t0 = Date.now(); ms = ms || 20000;
  for (;;) { let v; try { v = await fn(); } catch (_) { v = false; } if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out waiting for ' + what); await sleep(80); }
}

function Apps(W, cast) {
  const A = {};
  A.until = until;

  /** the person's six digits are made up per run and only ever typed into the page's own box; they are never printed or kept in a result */
  const pinOf = name => { const p = cast.pins[name]; if (!p) throw new Error('no made-up number for ' + name); return p; };

  /* ───────── the PIN pages: weld-1, assembly-1..4, shipping-1..3, design-message(-1) ───────── */
  A.pin = async function (ctx, file, name, o) {
    o = o || {};
    if (!ctx) ctx = await W.context({ label: file });                 // (every station computer is its own browser profile: the pages keep the login in localStorage, as one name per computer)
    const page = await W.open(ctx, file);
    await page.waitForFunction(() => window.StationSession && window.StationActivity && document.getElementById('employeeNumberInput'), null, { timeout: 40000 });
    await sleep(500);
    if (o.signIn === false) return page;
    await A.pinSignIn(page, name);
    return page;
  };
  A.pinSignIn = async function (page, name) {
    await page.waitForSelector('#employeeLoginBtn', { state: 'visible', timeout: 30000 });
    await page.evaluate(() => { const i = document.getElementById('employeeNumberInput'); i.value = ''; if (i.dataset) i.dataset.raw = ''; });
    await page.focus('#employeeNumberInput');
    await page.keyboard.type(pinOf(name));
    await page.click('#employeeLoginBtn', { force: true });
    await page.waitForFunction(() => window.isEmployeeLoggedIn === true && window.StationActivity && window.StationActivity.who(), null, { timeout: 25000 });
  };
  A.pinSignOut = async function (page) {
    await page.click('#signOutBtn', { force: true });
    await page.waitForFunction(() => !window.isEmployeeLoggedIn || window.isEmployeeLoggedIn === false, null, { timeout: 15000 });
  };

  /* ───────── a scanner page (weld-scan-1, assembly-scan-N, shipping-scan-N, sort-scan): its camera is a canvas showing the order's QR ───────── */
  A.scanner = async function (ctx, file) {
    const page = await W.open(ctx, file, { camera: true });
    await page.waitForFunction(() => window.__showQr && window.QRCode && window.jsQR, null, { timeout: 30000 });
    await sleep(600);
    return page;
  };
  /** show an order's QR to the scanner page and wait until the page has read it (its own field shows the code) */
  A.scan = async function (scanner, text, o) {
    o = o || {};
    await scanner.evaluate(() => window.__showQr(''));
    await sleep(700);                                       // (a blank frame between two codes: the page ignores a repeat of the last code)
    const ok = await scanner.evaluate(t => window.__showQr(t), text);
    if (!ok) throw new Error('the camera could not draw the QR for ' + text);
    await until(() => scanner.evaluate(t => { const i = document.getElementById('orderNumInput'); return !!i && i.value === t; }, text), 'the scanner page to read ' + text, o.ms || 25000);
    return true;
  };
  /** the desktop page has taken the phone's order (its order box shows it) */
  A.desktopHas = (page, rid, ms) => until(() => page.evaluate(id => { const i = document.getElementById('etsyOrderNumber'); return !!i && i.value === id; }, rid), 'the desktop page to show ' + rid, ms || 25000);

  /** one order typed in (the way a person types it and presses Enter) */
  A.type = async function (page, rid) {
    await page.fill('#etsyOrderNumber', rid); await page.focus('#etsyOrderNumber'); await page.keyboard.press('Enter');
    await sleep(900);
  };
  /** a message to the Team on the order (the chat box): "QA1" / "Done" are the assembly stamps */
  A.say = async function (page, text) {
    await page.fill('#britesMsgInput', text);
    await page.evaluate(() => document.getElementById('goScreenTwoBtn').click());
    await until(() => page.evaluate(() => document.getElementById('britesMsgInput').value === ''), 'the message to be sent', 15000).catch(() => {});
    await sleep(400);
  };
  A.flush = page => page.evaluate(() => window.StationActivity && StationActivity.flush && StationActivity.flush()).then(() => sleep(250));

  /* ───────── the shipping label and the order done ───────── */
  A.shipBuyAndPrint = async function (page) {
    await page.evaluate(() => { document.getElementById('ccShipmentId').value = 'SHIP1'; const b = document.getElementById('ccBuy'); b.disabled = false; b.click(); });
    await sleep(900);
  };
  A.shipComplete = async function (page) {
    await page.evaluate(() => { document.getElementById('trackingNumberInput').value = 'TRK123'; document.getElementById('carrierSelect').value = 'chitchats'; });
    await page.click('#completeOrderBtn');
    await sleep(900);
  };

  /* ───────── Sorting: sorting.html, sorting-2.html (the name chip takes a number or a name), the QR Printer frame, the phone scanner sort-scan ───────── */
  A.sorting = async function (ctx, file, name, o) {
    o = o || {};
    if (!ctx) ctx = await W.context({ label: file });
    const page = await W.open(ctx, file);
    await page.waitForFunction(() => window.StationSession && window.StationActivity && (window.SortTL || window.SortAct) && document.querySelector('#sortingAsChip .st-as-name'), null, { timeout: 40000 });
    await sleep(400);
    if (o.signIn !== false) await A.sortingSignIn(page, name);
    return page;
  };
  A.sortingSignIn = async function (page, name) {
    await page.click('#sortingAsChip .st-as-name');
    await page.waitForSelector('#sortingAsChip .st-as-input:not([hidden])', { timeout: 10000 });
    await page.keyboard.type(pinOf(name)); await page.keyboard.press('Enter');
    await page.waitForFunction(n => { const w = window.StationActivity && StationActivity.who(); return !!w && w.person === n; }, name, { timeout: 25000 });
  };
  /** a batch typed into the order box (comma separated), as a person types it; the page shows a tile per piece */
  A.sortBatch = async function (page, ids) {
    await page.fill('#etsyOrderNumber', ids.join(',')); await page.focus('#etsyOrderNumber'); await page.keyboard.press('Enter');
    await page.waitForFunction(n => (window.cachedOrderItems || []).length >= n && document.getElementById('previewCell0'), ids.length, { timeout: 30000 });
    await sleep(500);
  };
  /** the sticker button of an order's row (it is a table button: the page's own function is called with that row's cell) */
  A.sortSticker = async function (page, rid) {
    const i = await page.evaluate(r => window.cachedOrderItems.findIndex(t => String(t.typedOrderNumber || t.receipt_id) === r), rid);
    if (i < 0) throw new Error('the batch on screen has no order ' + rid);
    await page.evaluate(n => openIframePrinterForListing(n), i);
    await until(() => page.evaluate(() => [...document.querySelectorAll('iframe')].some(f => /QR/.test(f.src))), 'the QR Printer frame', 20000);
    await sleep(900);
  };
  /** what the hidden QR Printer frame did: labels asked for from the PDF maker (the stand-in), print calls of its own window */
  A.qrFrame = async function (page) {
    for (const f of page.frames()) {
      if (!/QR(%20|\s)Printer/i.test(f.url())) continue;
      try { return await f.evaluate(() => ({ made: (window.__pdfMade || []).length, prints: (window.__prints || []).length, text: (window.__pdfMade || []).map(m => m.text).join(' ').slice(0, 2000), url: location.pathname })); } catch (_) {}
    }
    return null;
  };

  /* ───────── Design: design.html / design-1.html (the name is set in the order chat), design-message(-1) are PIN pages (A.pin) ───────── */
  A.design = async function (ctx, file, name, rid, o) {
    o = o || {};
    if (!ctx) ctx = await W.context({ label: file });
    const page = await W.open(ctx, file);
    await page.waitForFunction(() => typeof proceedToPrint === 'function' && window.StationSession && StationSession.page() && window.StationActivity && typeof BritesChat !== 'undefined', null, { timeout: 60000 });
    await page.evaluate(() => { window.buildNewOrderList = async () => {}; window.ensureSelectedPreviews = async () => {}; });   // (the page's own list of open Etsy orders is not under test)
    if (o.signIn !== false) await A.designSignIn(page, name, rid);
    return page;
  };
  A.designSignIn = async function (page, name, rid) {
    await page.evaluate(r => BritesChat.open(r), rid);
    await page.waitForSelector('#bcWho', { state: 'attached', timeout: 20000 });
    await page.evaluate(n => { document.getElementById('bcWho').click(); const i = document.querySelector('#bcWho input'); i.value = n; i.dispatchEvent(new Event('blur')); }, name);
    await page.waitForFunction(n => { const w = StationActivity.who(); return !!w && w.person === n; }, name, { timeout: 20000 });
  };
  A.designChat = async function (page, text) {
    await page.evaluate(t => { const i = document.getElementById('bcInput'); i.value = t; i.dispatchEvent(new Event('input')); }, text);
    await page.evaluate(() => { const b = document.getElementById('bcSend'); b.disabled = false; b.click(); });
    await sleep(700);
  };
  /** a design finished and its label printed, through the page's own print step (the page's list of open orders is what it has no fake for) */
  A.designFinish = async function (page, rid, pieces) {
    await page.evaluate(async ([r, n]) => {
      orderCache[r] = Array.from({ length: n }, () => ({ quantity: 1, sku: 'CH-PET-GF', title: 'Pet Portrait Charm', receipt_id: r, listing_id: 1912340031 }));
      pendingLists = { gold: [r] }; pendingJobs = buildPrintJobs(pendingLists);
      await proceedToPrint();
    }, [rid, pieces || 1]);
    await sleep(900);
  };

  /* ───────── the Inbox: etsy-mail-1.html, its own sign-in (user name and password) ───────── */
  A.inbox = async function (ctx, user, o) {
    o = o || {};
    if (!ctx) ctx = await W.context({ label: 'etsy-mail-1' });
    const page = await W.open(ctx, 'etsy-mail-1.html');
    await page.waitForSelector('#siUser', { state: 'visible', timeout: 40000 });
    if (o.signIn !== false) await A.inboxSignIn(page, user);
    return page;
  };
  A.inboxSignIn = async function (page, user) {
    await page.fill('#siUser', user); await page.fill('#siPass', 'pw-' + user); await page.click('#siBtn');
    await page.waitForFunction(() => document.body.classList.contains('authed') && window.StationActivity && StationActivity.who(), null, { timeout: 40000 });
  };
  /** open a conversation, type the reply, press Send via Etsy (the page queues it; the shop's own recorder notes it) */
  A.inboxReply = async function (page, threadId, text) {
    await page.click(`[data-id="${threadId}"]`);
    await page.waitForSelector('#emDraftText', { timeout: 20000 });
    await page.fill('#emDraftText', text);
    await page.click('#emSendEtsyBtn');
    await sleep(1200);
  };

  /* ───────── the Sorter app (charm-nest-1.html): the name bar, then "Laser or Design?" for anybody who is not the Admin ───────── */
  A.sorterApp = async function (ctx, o) {
    o = o || {};
    if (!ctx) ctx = await W.context({ label: 'sorter ' + (o.label || '') });
    await ctx.addInitScript(() => {
      try { if (location.hostname === '127.0.0.1') localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: 'off', sandboxStream: 'off' })); } catch (_) {}
      window.confirm = () => true; window.prompt = () => null;
    });
    const page = await W.open(ctx, 'charm-nest-1.html', { sorter: true });
    await page.waitForFunction(() => window.CN && window.Orders && window.CNAct && window.CNEmployee && window.StationActivity && window.StationSession && window.CNRole && document.readyState === 'complete', null, { timeout: 90000 });
    return page;
  };
  A.sorterName = async function (page, name) {
    await page.evaluate(() => { CNEmployee.edit({}); });
    await page.waitForSelector('.cnNameBar[data-kind="edit"] input', { timeout: 15000 });
    await page.fill('.cnNameBar input', name); await page.press('.cnNameBar input', 'Enter');
  };
  /** a non-Admin: the name, then the one question in the same bar */
  A.sorterRole = async function (page, name, role) {
    await A.sorterName(page, name);
    await page.waitForSelector('.cnNameBar[data-kind="role"]', { timeout: 20000 });
    await page.click(`.cnNameBar[data-kind="role"] [data-role="${role}"]`);
    await page.waitForFunction(r => { const w = StationActivity.who(); return !!w && w.role === r; }, role, { timeout: 20000 });
  };
  /** the Admin: the door says so, no question is asked */
  A.sorterAdmin = async function (page, name) {
    await A.sorterName(page, name);
    await page.waitForFunction(() => { const w = StationActivity.who(); return !!w; }, null, { timeout: 25000 });
  };

  /* ───────── Welding (weld-1): PIN, then "Welding or Matching?"; more than one person at once ("Add person" while somebody is signed in) ───────── */
  A.weld = async function (ctx, name, task, o) {
    o = o || {};
    if (!ctx) ctx = await W.context({ label: 'weld-1' });
    const page = await W.open(ctx, 'weld-1.html');
    await page.waitForFunction(() => window.StationSession && window.StationActivity && document.getElementById('employeeNumberInput') && typeof weldRoster === 'function', null, { timeout: 40000 });
    await sleep(500);
    if (name) await A.weldSignIn(page, name, task);
    return page;
  };
  A.weldSignIn = async function (page, name, task) {
    const boxUp = await page.evaluate(() => { const b = document.getElementById('employeeLoginBtn'); return !!b && b.offsetParent !== null; });
    if (!boxUp) { await page.click('#weldAddPerson'); await page.waitForSelector('#employeeLoginBtn', { state: 'visible', timeout: 15000 }); }
    await page.evaluate(() => { const i = document.getElementById('employeeNumberInput'); i.value = ''; if (i.dataset) i.dataset.raw = ''; });
    await page.focus('#employeeNumberInput');
    await page.keyboard.type(pinOf(name));
    await page.click('#employeeLoginBtn', { force: true });
    const btn = task === 'welding' ? '#weldTaskWelding' : '#weldTaskMatching';
    await page.waitForSelector(btn, { state: 'visible', timeout: 25000 });
    await page.click(btn);
    await page.waitForFunction(([n, t]) => weldRoster().some(p => p.name === n && p.task === t), [name, task], { timeout: 25000 });
    await sleep(400);
  };
  A.weldChipOut = async function (page, name, task) {
    await page.click(`.weld-chip[data-name="${name}"][data-task="${task}"]`);
    await page.waitForFunction(([n, t]) => !weldRoster().some(p => p.name === n && p.task === t), [name, task], { timeout: 15000 });
  };

  return A;
}
module.exports = { Apps, until };
