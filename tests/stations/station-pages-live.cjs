// What the station pages and the Inbox feed into the Employee efficiency portal, end to end in a real browser with the real
// station-session.js, station-activity.js and station-mail-signals.js; every /.netlify/functions call goes to a fake in this
// file and every other host is aborted. Nothing is sent to anybody, nothing real is touched.
//   INBOX (etsy-mail-1.html, contact success and failure per operator): an AI draft = "reply drafted"; a reply sent = one
//   "reply sent" with how it started (ai draft / ai draft edited) and how long the customer waited (first reply / waited);
//   the Etsy helper's word = "reply delivered", "reply delivered · images not sent", "reply unconfirmed" or "reply failed · CODE",
//   once each and under the person who sent it; a reply still on its way survives a reload; the queue refusing it stays
//   "reply not sent"; no customer text, address or PIN in any request; a dead network never throws or blocks the page.
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=… node tests/stations/station-pages-live.cjs
'use strict';
const http = require('http'), fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));

const ts = ms => ({ _ts: true, ms });
const T0 = Date.now();
const acts = [], sent = [];                       // every activity event the door got; every request (url + body) the pages made
let netDown = false;                              // the fake network: every call fails while true
const CUSTOMER = ['Jane Doe', '12 Main St', 'Test Buyer'];

/* ── the inbox world: five conversations, each with its own draft slot ── */
const IDS = { A: 'etsy_conv_2001', B: 'etsy_conv_2002', C: 'etsy_conv_2003', D: 'etsy_conv_2004', E: 'etsy_conv_2005', F: 'etsy_conv_2006', G: 'etsy_conv_2007' };
const thread = (k, extra) => Object.assign({ id: IDS[k], customerName: 'Cust ' + k, status: 'etsy_scraped', unread: false, etsyOrderId: '35210005' + (IDS[k].slice(-2)),
  lastInboundAt: ts(T0 - 3600e3), awaitingReplySince: ts(T0 - 3600e3), updatedAt: ts(T0 - 3600e3), etsyConversationUrl: 'https://example.invalid/c/' + IDS[k] }, extra || {});
const THREADS = [thread('A'), thread('B'), thread('C'), thread('D'),
  thread('E', { awaitingReplySince: undefined, lastInboundAt: ts(T0 - 7200e3), lastOutboundAt: ts(T0 - 3600e3), lastOperatorReplyAt: ts(T0 - 3600e3), status: 'auto_replied' }), thread('F'), thread('G')];
const inbound = (id, ms) => ({ id: id + '_in', direction: 'inbound', senderName: 'Cust', senderRole: 'customer', text: 'Hello, where is my order, Jane Doe?', timestamp: ts(ms), createdAt: ts(ms) });
const outbound = (id, ms) => ({ id: id + '_out', direction: 'outbound', senderName: 'CustomBrites', senderRole: 'staff', source: 'etsy', text: 'Thanks for writing.', timestamp: ts(ms), createdAt: ts(ms) });
const world = {};
for (const id of Object.values(IDS)) world[id] = { messages: [inbound(id, T0 - 3600e3)], draft: null };
world[IDS.D].messages = [inbound(IDS.D, T0 - 7200e3), outbound(IDS.D, T0 - 7000e3), Object.assign(inbound(IDS.D + 'b', T0 - 1800e3), { id: IDS.D + '_in2' })];
world[IDS.D].meta = { awaiting: T0 - 1800e3 };
world[IDS.E].messages = [inbound(IDS.E, T0 - 7200e3), outbound(IDS.E, T0 - 3600e3)];
const AI_TEXT = 'Hello! Your order ships tomorrow, and I will send the tracking number as soon as it does.';
THREADS.find(t => t.id === IDS.D).awaitingReplySince = ts(T0 - 1800e3);

function fake(method, url, body) {
  const u = new URL(url, 'http://x'), name = u.pathname.split('/').pop(), q = Object.fromEntries(u.searchParams);
  let b = {}; try { b = body ? JSON.parse(body) : {}; } catch (_) {}
  if (name === 'firebaseOrders') {
    if (method === 'GET') return { __status: 404, error: 'not found' };
    if (Array.isArray(b.activity)) { acts.push(...b.activity); return { success: true, written: b.activity.length, duplicate: 0, refused: 0, scrubbed: 0 }; }
    return { success: true };
  }
  if (name === 'firestoreProxy') {
    if (q.op === 'list' && q.coll === 'EtsyMail_Threads') return { docs: THREADS };
    if (q.op === 'listSub') return { docs: (world[q.id] || { messages: [] }).messages };
    if (q.op === 'get' && q.coll === 'EtsyMail_Drafts') { const w = world[String(q.id).replace(/^draft_/, '')]; return w && w.draft ? { exists: true, doc: w.draft } : { exists: false }; }
    if (q.op === 'get') return { exists: false };
    return { ok: true };
  }
  if (name === 'etsyMailAuth') return { ok: true, username: 'paul', displayName: 'Paul Inbox', role: 'owner' };
  if (name === 'etsyMailDraftReply' && method === 'POST') return { ok: true, text: AI_TEXT, aiConfidence: 0.9, toolCalls: [], model: 'fake' };
  if (name === 'etsyMailDraftSend' && method === 'POST' && b.op === 'enqueue') {
    if (b.threadId === IDS.F) return { __status: 400, error: 'The Etsy helper refused it' };
    const w = world[b.threadId];
    w.draft = { id: 'draft_' + b.threadId, threadId: b.threadId, text: b.text, status: 'queued', queuedAt: ts(Date.now()) };
    return { ok: true, draftId: 'draft_' + b.threadId, attachments: b.attachments || [], text: b.text };
  }
  if (name === 'etsyMailDraftSend' && q.op === 'status') {
    const w = world[String(q.draftId).replace(/^draft_/, '')];
    return w && w.draft ? { ok: true, draft: w.draft } : { __status: 404, error: 'Draft not found' };
  }
  return { ok: true };
}
const server = http.createServer((req, res) => {
  const u = decodeURIComponent(req.url.split('?')[0]);
  if (u.startsWith('/.netlify/functions/')) {
    let body = ''; req.on('data', d => { body += d; });
    req.on('end', () => {
      sent.push(req.url + ' ' + body);
      if (netDown) { res.destroy(); return; }
      const out = fake(req.method, req.url, body), status = out.__status || 200; delete out.__status;
      res.writeHead(status, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(out));
    });
    return;
  }
  const f = path.join(root, u);
  if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
  res.writeHead(200, { 'Content-Type': /\.js$/.test(f) ? 'text/javascript' : /\.html$/.test(f) ? 'text/html' : 'application/octet-stream' });
  fs.createReadStream(f).pipe(res);
}).listen(0);

const wait = ms => new Promise(r => setTimeout(r, ms));
async function until(fn, what, ms = 15000) { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out waiting for ' + what); await wait(100); } }
const origin = () => `http://127.0.0.1:${server.address().port}`;
const flush = async page => { try { await page.evaluate(() => window.StationActivity && StationActivity.flush()); } catch (_) {} await wait(250); };
const mine = device => acts.filter(e => e.device === device);
const details = list => list.map(e => e.action + ': ' + e.detail);

async function openPage(browser, ctx, file, query = '') {
  const page = await ctx.newPage(), errors = [];
  page.on('pageerror', e => errors.push(String(e && e.message || e)));
  await page.goto(origin() + '/' + file + query);
  return { page, errors };
}
async function newCtx(browser, seed) {
  const ctx = await browser.newContext({ viewport: { width: 1400, height: 950 } });
  await ctx.route(x => !/^http:\/\/127\.0\.0\.1[:/]/.test(x.href), r => r.abort());
  await ctx.addInitScript(seed || (() => {}));
  return ctx;
}

/* ── the inbox ── */
async function inbox(browser) {
  const seed = () => { try { if (!localStorage.getItem('etsymail_session')) { localStorage.setItem('etsymail_session', 'tok'); localStorage.setItem('etsymail_session_profile', JSON.stringify({ username: 'paul', displayName: 'Paul Inbox', role: 'owner' })); } } catch (_) {} };
  const ctx = await newCtx(browser, seed);
  const { page, errors } = await openPage(browser, ctx, 'etsy-mail-1.html');
  const device = 'etsy-mail-1';
  await until(() => page.evaluate(() => window.StationActivity && StationActivity.who()), 'the inbox sign-in');
  const open = async id => { await page.click(`[data-id="${id}"]`); await page.waitForSelector('#emDraftText', { timeout: 10000 }); await wait(400); };
  const send = async () => { await page.click('#emSendEtsyBtn'); await wait(300); };
  const aiDraft = async () => { await page.click('#emAiDraftBtn'); await page.waitForFunction(t => document.getElementById('emDraftText').value.includes('ships tomorrow'), null, { timeout: 10000 }); };
  const only = pred => { const l = mine(device).filter(pred); return l; };

  // 1 · A: an AI draft sent as it is, to a customer waiting for a first reply
  await open(IDS.A); await aiDraft(); await send();
  // 2 · B: an AI draft the operator changed first
  await open(IDS.B); await aiDraft(); await page.fill('#emDraftText', AI_TEXT + ' Thank you for your patience, Jane.'); await send();
  // 3 · C: typed from scratch (no AI draft), first reply
  await open(IDS.C); await page.fill('#emDraftText', 'Hi there, it ships today.'); await send();
  // 4 · D: the customer wrote again after our reply: a later reply, typed
  await open(IDS.D); await page.fill('#emDraftText', 'Sorry for the wait, it is on its way.'); await send();
  // 5 · E: a follow-up nobody was waiting for (the Waiting folder is not on screen here, so the page's own call is made): no wait token
  await page.evaluate(([tid, oid]) => MailSignals.sent({ threadId: tid, orderId: oid, text: 'One more thing: you can reach us here any time.', englishText: 'One more thing: you can reach us here any time.', sinceMs: 0, firstReply: null }), [IDS.E, '35210005' + IDS.E.slice(-2)]);
  // 6 · F: the queue refuses it
  await open(IDS.F); await page.fill('#emDraftText', 'This one is refused by the server.'); await send();
  await until(async () => { await flush(page); return mine(device).length >= 7; }, 'the send events');
  let got = mine(device);
  const byText = re => got.filter(e => re.test(e.detail));
  assert.strictEqual(byText(/^reply drafted$/).length, 2, 'two AI drafts were asked for: ' + details(got).join(' | '));
  const sentEv = got.filter(e => /^reply sent/.test(e.detail));
  assert.strictEqual(sentEv.length, 5, 'five replies were queued, the refused one is not one: ' + details(got).join(' | '));
  const sentBy = oid => sentEv.find(e => e.orderId === oid).detail;
  const oidOf = k => '35210005' + IDS[k].slice(-2);
  assert(/^reply sent · ai draft · first reply (59|60|61)m$/.test(sentBy(oidOf('A'))), 'A: ' + sentBy(oidOf('A')));
  assert(/^reply sent · ai draft edited · first reply (59|60|61)m$/.test(sentBy(oidOf('B'))), 'B: ' + sentBy(oidOf('B')));
  assert(/^reply sent · first reply (59|60|61)m$/.test(sentBy(oidOf('C'))), 'C: ' + sentBy(oidOf('C')));
  assert(/^reply sent · waited (29|30|31)m$/.test(sentBy(oidOf('D'))), 'D: ' + sentBy(oidOf('D')));
  assert.strictEqual(sentBy(oidOf('E')), 'reply sent', 'E: a follow-up nobody waited for has no wait: ' + sentBy(oidOf('E')));
  assert(got.some(e => e.action === 'error' && e.detail === 'reply not sent' && e.orderId === oidOf('F')), 'F: the refused queueing is "reply not sent"');
  for (const e of got) { assert.strictEqual(e.person, 'Paul Inbox'); assert.strictEqual(e.station, 'inbox'); assert.strictEqual(e.device, device); }
  console.log('inbox: drafted x2, sent x5 (ai draft / edited / first reply / waited / plain), refused x1');

  // the Etsy helper's word: A delivered, B delivered with images missing, C unconfirmed, D failed with its code, E (nobody reads it) later
  const set = (k, status, extra) => { world[IDS[k]].draft = Object.assign({}, world[IDS[k]].draft, { status }, extra || {}); };
  set('A', 'sent'); set('B', 'sent_text_only'); set('C', 'sent_unverified'); set('D', 'failed', { sendErrorCode: 'NO_ETSY_TAB', sendError: 'Open the Etsy tab for Jane Doe at 12 Main St' });
  // the open conversation's own status watch settles the one on screen; the page's slow look settles the rest within its first rounds
  await until(async () => { await flush(page); return mine(device).filter(e => /^reply (delivered|unconfirmed|failed)/.test(e.detail)).length >= 4; }, 'the four outcomes', 70000);
  got = mine(device);
  const out = k => got.filter(e => /^reply (delivered|unconfirmed|failed)/.test(e.detail) && e.orderId === oidOf(k)).map(e => e.action + ': ' + e.detail);
  assert.deepStrictEqual(out('A'), ['note: reply delivered']); assert.deepStrictEqual(out('B'), ['note: reply delivered · images not sent']);
  assert.deepStrictEqual(out('C'), ['note: reply unconfirmed']); assert.deepStrictEqual(out('D'), ['error: reply failed · NO_ETSY_TAB']);
  assert.deepStrictEqual(out('E'), [], 'E: no draft slot, no outcome');
  console.log('inbox: delivered, delivered (images not sent), unconfirmed, failed with its code: once each');

  // a reload keeps the reply that is still on its way (G), and its outcome is recorded once, under the same person
  await open(IDS.G); await page.fill('#emDraftText', 'A reply that is still on its way when the page reloads.'); await send();
  await until(() => page.evaluate(() => MailSignals.pending() >= 1), 'the reply on its way');
  await flush(page);
  await page.reload();
  await until(() => page.evaluate(() => window.MailSignals && MailSignals.pending() >= 1 && StationActivity.who()), 'the pending reply after the reload');
  set('G', 'sent');
  await until(async () => { await flush(page); return mine(device).some(e => e.detail === 'reply delivered' && e.orderId === oidOf('G')); }, 'the outcome after the reload', 40000);
  await wait(1500); await flush(page);
  assert.strictEqual(mine(device).filter(e => /^reply (delivered|unconfirmed|failed)/.test(e.detail) && e.orderId === oidOf('G')).length, 1, 'recorded once');
  console.log('inbox: a reply still on its way survives a reload; its outcome is recorded once');

  // privacy: fixed words and numbers only; never a customer name, address, message text or a PIN
  const blob = JSON.stringify(acts) + sent.filter(s => /"activity"|"session"/.test(s)).join('\n');
  for (const w of [...CUSTOMER, 'ships tomorrow', 'Thank you for your patience', 'Sorry for the wait']) assert(!blob.includes(w), 'no customer text in what is recorded about the people: ' + w);
  assert(!/\b\d{6}\b/.test(JSON.stringify(acts.map(e => e.detail))), 'no 6-digit number in any detail');

  // a dead network: nothing throws, nothing blocks, and the reply box still works
  netDown = true;
  await open(IDS.F); await page.fill('#emDraftText', 'sent while the network is down'); await page.click('#emSendEtsyBtn'); await wait(1200);
  const stillTyping = await page.evaluate(() => !!document.getElementById('emDraftText'));
  netDown = false;
  assert(stillTyping, 'the page still works');
  assert.strictEqual(errors.filter(e => !/Failed to fetch|Load failed|NetworkError|net::/.test(e)).length, 0, 'page errors: ' + errors.join(' | '));
  await ctx.close();
  console.log('inbox: no customer text or PIN recorded; a dead network breaks nothing');
}

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    await inbox(browser);
    console.log('station pages live OK');
  } finally { await browser.close(); server.close(); }
})().catch(e => { console.error(e); process.exit(1); });
