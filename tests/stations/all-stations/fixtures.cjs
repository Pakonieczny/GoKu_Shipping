// The invented shop of the all-stations end-to-end test: made-up orders, made-up buyers and the answers the station pages' own functions give
// (the Etsy order proxy and its pictures, the shipping helpers). Nothing here is real: no customer, no address, no number a person could type.
// `answer(name, method, query, body, state)` returns undefined when the backend should hand the call to the bridge, else { status?, body } or a body.
'use strict';

const LINE = (id, title, qty, sku, listing, vars) => ({ transaction_id: Number(id) * 10 + (listing % 7), receipt_id: Number(id), listing_id: listing, title, quantity: qty, sku, variations: vars || [],
  price: { amount: 2400, divisor: 100, currency_code: 'USD' }, is_digital: false });
const ORDER = (id, buyer, lines, extra) => Object.assign({
  receipt_id: Number(id), order_id: Number(id), name: buyer, status: 'Paid', is_gift: false, message_from_buyer: '', buyer_email: 'buyer@example.invalid',
  first_line: '1 Example Street', second_line: '', city: 'Exampleton', state: 'ON', zip: 'A1A 1A1', country_iso: 'CA', formatted_address: buyer + '\n1 Example Street\nExampleton ON A1A 1A1\nCanada',
  create_timestamp: 1790000000, created_timestamp: 1790000000, shipped: false, was_shipped: false, transactions: lines
}, extra || {});

// each action of the test uses its own order, so what the board and the person page say can be told apart
const ORDERS = {};
const add = (id, buyer, lines, extra) => { ORDERS[id] = ORDER(id, buyer, lines, extra); return id; };
const R = {
  weldA: add('3820000101', 'Wendy Stone', [LINE('3820000101', 'Birthstone Stud Earrings', 1, 'ST-BIRTH-GF', 1912340001, [{ formatted_name: 'Style', formatted_value: 'Studs' }])]),
  weldB: add('3820000102', 'Walt Marsh', [LINE('3820000102', 'Pet Portrait Stud Earrings', 1, 'ST-PET-SS', 1912340002, [{ formatted_name: 'Style', formatted_value: 'Studs' }])]),
  weldC: add('3820000103', 'Willa Pike', [LINE('3820000103', 'Tiny Initial Stud Earrings', 2, 'ST-INI-GF', 1912340003, [{ formatted_name: 'Style', formatted_value: 'Studs' }])]),
  asm1: add('3820000201', 'Ada Brook', [LINE('3820000201', 'Moon Charm, gold', 2, 'CH-MOON-GF', 1912340011, [{ formatted_name: 'Size', formatted_value: 'M' }]), LINE('3820000201', 'Heart Charm, rose', 1, 'CH-HEART-RG', 1912340012)]),
  asm2: add('3820000202', 'Abel Reed', [LINE('3820000202', 'Star Charm, silver', 1, 'CH-STAR-SS', 1912340013)]),
  asm3: add('3820000203', 'Ava Frost', [LINE('3820000203', 'Leaf Charm, gold', 1, 'CH-LEAF-GF', 1912340014)]),
  asm4: add('3820000204', 'Alma Cole', [LINE('3820000204', 'Bee Charm, silver', 3, 'CH-BEE-SS', 1912340015)]),
  asm5: add('3820000205', 'Aldo Rowe', [LINE('3820000205', 'Moon Charm, gold', 2, 'CH-MOON-GF', 1912340011, [{ formatted_name: 'Size', formatted_value: 'S' }])]),
  asm6: add('3820000206', 'Alba Ness', [LINE('3820000206', 'Leaf Charm, silver', 1, 'CH-LEAF-SS', 1912340016)]),      // (D-AS2: one person, two Assembly pages of one computer)
  asm7: add('3820000207', 'Arlo Dunn', [LINE('3820000207', 'Bee Charm, gold', 2, 'CH-BEE-GF', 1912340017)]),
  asm8: add('3820000208', 'Aria Penn', [LINE('3820000208', 'Star Charm, gold', 1, 'CH-STAR-GF', 1912340018)]),
  ship1: add('3820000301', 'Sam Clarke', [LINE('3820000301', 'Moon Charm, gold', 1, 'CH-MOON-GF', 1912340011), LINE('3820000301', 'Leaf Charm, gold', 1, 'CH-LEAF-GF', 1912340014)]),
  ship2: add('3820000302', 'Sue Dane', [LINE('3820000302', 'Heart Charm, rose', 1, 'CH-HEART-RG', 1912340012)]),
  ship3: add('3820000303', 'Seth Lake', [LINE('3820000303', 'Star Charm, silver', 2, 'CH-STAR-SS', 1912340013)]),
  ship4: add('3820000304', 'Sky Dunn', [LINE('3820000304', 'Leaf Charm, gold', 1, 'CH-LEAF-GF', 1912340014), LINE('3820000304', 'Heart Charm, rose', 2, 'CH-HEART-RG', 1912340012)]),
  sort1: add('3820000401', 'Sara Wood', [LINE('3820000401', 'Name Charm', 1, 'CH-NAME-GF', 1912340021)]),
  sort2: add('3820000402', 'Simon Vale', [LINE('3820000402', 'Initial Charm', 2, 'CH-INI-SS', 1912340022)]),
  sort3: add('3820000403', 'Sana Hill', [LINE('3820000403', 'Name Charm', 1, 'CH-NAME-GF', 1912340021)]),
  sort4: add('3820000404', 'Silas Ward', [LINE('3820000404', 'Initial Charm', 1, 'CH-INI-SS', 1912340022)]),
  des1: add('3820000501', 'Dina Moss', [LINE('3820000501', 'Pet Portrait Charm', 1, 'CH-PET-GF', 1912340031)]),
  des2: add('3820000502', 'Dean Hale', [LINE('3820000502', 'Custom Photo Charm', 1, 'CH-PHOTO-SS', 1912340032)]),
  des3: add('3820000503', 'Dora Finch', [LINE('3820000503', 'Name Necklace Charm', 1, 'CH-NAME-RG', 1912340033)]),
  des4: add('3820000504', 'Dale Park', [LINE('3820000504', 'Pet Portrait Charm', 1, 'CH-PET-GF', 1912340031)]),
  des5: add('3820000505', 'Drew Lane', [LINE('3820000505', 'Pet Portrait Charm', 1, 'CH-PET-GF', 1912340031)]),
  des6: add('3820000506', 'Dixie Gray', [LINE('3820000506', 'Custom Photo Charm', 1, 'CH-PHOTO-SS', 1912340032)]),
  las1: add('3820000601', 'Lia Ford', [LINE('3820000601', 'Pet Portrait Charm', 1, 'CH-PET-GF', 1912340031)]),
  las2: add('3820000602', 'Lou Vance', [LINE('3820000602', 'Name Charm', 2, 'CH-NAME-GF', 1912340021)]),
  inb1: add('3820000701', 'Ingrid Hart', [LINE('3820000701', 'Moon Charm, gold', 1, 'CH-MOON-GF', 1912340011)]),
  inb2: add('3820000702', 'Ivan Peck', [LINE('3820000702', 'Leaf Charm, gold', 1, 'CH-LEAF-GF', 1912340014)])
};
let cancelled = new Set();
const PNG = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAC0lEQVR4nGP4DwQACfsD/Z8gVq4AAAAASUVORK5CYII=', 'base64');

function set(o) { if (o && Array.isArray(o.cancelled)) cancelled = new Set(o.cancelled); }

const reply = (body, status) => ({ status: status || 200, body });

/* ───────── the inbox (etsy-mail-1.html): two conversations, operators with a password made up from their user name, no AI, no Etsy ─────────
   The send is the one place the shop's own code runs under the fake: etsyMailDraftSend `enqueue` records the reply with the REAL recordOutcome
   (netlify/functions/_etsyMailLearning.js, IN1's write side), over the same Firestore, as the real function does at that moment. */
const OPS = { ines: { display: 'Ines I.', role: 'operator' }, ivo: { display: 'Ivo L.', role: 'operator' }, paul: { display: 'Paul K', role: 'owner' } };
const THREAD_IDS = { A: 'etsy_conv_8801', B: 'etsy_conv_8802' };
const THREAD_ORDER = { [THREAD_IDS.A]: 'inb1', [THREAD_IDS.B]: 'inb2' };
const threadFields = (id, now) => { const rid = ORDERS[R[THREAD_ORDER[id]]], ms = now - 3600e3; return { id, customerName: rid.name, etsyUsername: rid.name.toLowerCase().replace(/\W+/g, ''), status: 'etsy_scraped', unread: false, etsyOrderId: String(rid.receipt_id),
  lastInboundAt: { _ts: true, ms }, awaitingReplySince: { _ts: true, ms }, updatedAt: { _ts: true, ms }, etsyConversationUrl: 'https://example.invalid/c/' + id }; };
const inboxMsgs = (id, now) => [{ id: id + '_m1', direction: 'inbound', senderName: ORDERS[R[THREAD_ORDER[id]]].name, senderRole: 'customer', text: 'Hello! When will my order ship?', timestamp: { _ts: true, ms: now - 7200e3 }, createdAt: { _ts: true, ms: now - 7200e3 } }];
const who = u => ({ ok: true, username: u, displayName: OPS[u].display, role: OPS[u].role });
const inboxDraftSent = new Set();
async function inboxAnswer(name, method, q, b, headers, ctx) {
  const now = ctx.now();
  if (name === 'firestoreProxy') {
    if (q.op === 'list' && q.coll === 'EtsyMail_Threads') return reply({ docs: Object.values(THREAD_IDS).map(id => threadFields(id, now)) });
    if (q.op === 'listSub') return reply({ docs: inboxMsgs(q.id, now) });
    if (q.op === 'get' && q.coll === 'EtsyMail_Config' && q.id === 'autoPipeline') return reply({ exists: true, doc: { enabled: false, manualAiDraftAutoSend: false, threshold: 0.8 } });   // auto-send stays OFF
    if (q.op === 'get') return reply({ exists: false });
    return reply({ ok: true });
  }
  if (name === 'etsyMailAuth') {
    if (b.op === 'login') { const u = OPS[b.username]; return u && b.password === 'pw-' + b.username ? reply(Object.assign(who(b.username), { sessionToken: 'tok-' + b.username })) : reply({ error: 'bad', reason: 'BAD_PASSWORD' }, 401); }
    if (b.op === 'currentUser') { const tok = headers['x-etsymail-session'] || '', m = /^tok-(.+)$/.exec(tok); return m && OPS[m[1]] ? reply(who(m[1])) : reply({ error: 'no', reason: 'SESSION_NOT_FOUND' }, 401); }
    return reply({ ok: true });
  }
  if (name === 'etsyMailGmailConfig') return reply({ ok: true, enabled: false });
  if (name === 'etsyMailDraftSend' && method === 'POST' && b.op === 'enqueue') {
    const id = String(b.threadId || ''), th = THREAD_ORDER[id] ? threadFields(id, now) : null;
    if (th) { const { id: _i, ...rest } = th; await ctx.db.collection('EtsyMail_Threads').doc(id).set(rest); }
    await require('../../../netlify/functions/_etsyMailLearning.js').recordOutcome({ db: ctx.db, admin: ctx.admin, draftId: 'draft_' + id, threadId: id, prev: null, sentText: String(b.text || ''), sendOrigin: b.sendOrigin === 'auto' ? 'auto' : 'manual', employeeName: b.employeeName, polished: false });
    return reply({ ok: true, draftId: 'draft_' + id, attachments: b.attachments || [], text: b.text });
  }
  if (name === 'etsyMailDraftSend') return reply({ ok: true, draft: { status: 'queued' } });
  if (name === 'etsyMailThreads') return reply({ ok: true });
  // the inbox page asks the AI (Haiku summarizeThread, detectLanguage, translate) when a conversation opens: in this shop the AI is OFF, the answer is a plain "no"
  // and the page goes on without it. The backend counts the attempt (stats.paidTried) but nothing is ever bought.
  if (name === 'etsyMailDraftReply') return reply({ ok: false, error: 'AI is off in this test' });
  return undefined;
}
const INBOX_FNS = new Set(['firestoreProxy', 'etsyMailAuth', 'etsyMailGmailConfig', 'etsyMailDraftSend', 'etsyMailThreads', 'etsyMailDraftReply']);

function answer(name, method, q, b, st, ctx, headers) {
  if (INBOX_FNS.has(name)) return inboxAnswer(name, method, q, b, headers || {}, ctx);
  if (name === 'etsyOrderProxy') {
    const o = ORDERS[String(q.orderId || '')];
    if (!o) return reply({ error: 'no such receipt (a made-up shop)' }, 404);
    return reply(Object.assign({}, o, { status: cancelled.has(String(o.receipt_id)) ? 'Canceled' : 'Paid' }));
  }
  if (name === 'etsyImages') return reply({ results: [{ listing_id: Number(q.listingId) || 0, rank: 1, url_75x75: `/__pic/${q.listingId}.png`, url_170x135: `/__pic/${q.listingId}.png`, url_570xN: `/__pic/${q.listingId}.png`, url_fullxfull: `/__pic/${q.listingId}.png` }] });
  if (name === 'ssGetOrderAddress') return reply({ success: true, found: false });
  if (name === 'ssGetGiftMessage') return reply({ success: true, giftMessage: '', giftFrom: '' });
  if (name === 'testChitChats') {
    if (q.resource === 'shipment') return reply({ shipment: { id: q.id, carrier: 'usps', carrier_tracking_code: 'TRK123', postage_label_png_url: 'https://chitchats.test/label.png', status: 'ready' } });
    if (q.resource === 'label') return q.format === 'png' ? { status: 200, body: PNG, headers: { 'Content-Type': 'image/png' } } : reply({ error: 'no pdf in the test' }, 404);
    return reply({ success: true, shipments: [], data: [] });
  }
  if (name === 'trackOrderProxy') return reply({ orderId: 987654, orderNumber: b && b.receiptId });
  if (name === 'smartyVerify') return reply({ success: true, verified: false, candidates: [] });
  if (name === 'qz-sign') return reply('');
  if (name === 'authGate') return reply({ locked: false });
  if (name === 'refreshEtsyToken') return reply({ access_token: 'tok', refresh_token: 'ref', expires_in: 3600 });
  return undefined;
}
module.exports = { ORDERS, R, answer, set, PNG, LINE, ORDER, OPS, THREAD_IDS };
