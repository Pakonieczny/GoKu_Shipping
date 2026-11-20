/* tests/charm-nest/etsy-shape.cjs — charm-nest-etsy-shape.js against the Design Station's own code.
   The station functions (decorateReceipt, pullEtsyOrderDetails, hydrateOrder, applyHydration, lineForBridge, orderForBridge,
   the orders.detail command, the metal classifier …) are cut out of design-1.html by name and run in a vm context with
   minimal stubs; Etsy answers reach them through the real netlify/functions/listOpenOrders.js and etsyOrderProxy.js (their
   Etsy fetch faked). Every receipt is shaped both ways and the answers must be equal, key order included.
   Run: node tests/charm-nest/etsy-shape.cjs */
'use strict';
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const Module = require('node:module');

const root = path.resolve(__dirname, '../..');
const Shape = require(path.join(root, 'charm-nest-etsy-shape.js'));
const html = fs.readFileSync(path.join(root, 'design-1.html'), 'utf8');

/* ═══ 1 · cutting declarations out of design-1.html ═══ */
const KEYWORDS_BEFORE_REGEX = new Set(['return', 'typeof', 'instanceof', 'in', 'of', 'new', 'delete', 'void', 'throw', 'case', 'do', 'else', 'yield', 'await']);
function skipString(src, i, q) { for (let j = i + 1; j < src.length; j++) { if (src[j] === '\\') { j++; continue; } if (src[j] === q) return j; } throw new Error('unterminated string at ' + i); }
function skipRegex(src, i) {
  let inClass = false;
  for (let j = i + 1; j < src.length; j++) {
    const c = src[j];
    if (c === '\\') { j++; continue; }
    if (c === '[') inClass = true; else if (c === ']') inClass = false;
    else if (c === '/' && !inClass) { while (/[a-z]/i.test(src[j + 1])) j++; return j; }
    else if (c === '\n') throw new Error('unterminated regex at ' + i);
  }
  throw new Error('unterminated regex at ' + i);
}
function skipTemplate(src, i) {
  for (let j = i + 1; j < src.length; ) {
    const c = src[j];
    if (c === '\\') { j += 2; continue; }
    if (c === '`') return j;
    if (c === '$' && src[j + 1] === '{') { j = scan(src, j + 1, 'close'); continue; }
    j++;
  }
  throw new Error('unterminated template at ' + i);
}
/** From src[i]: 'close' — src[i] opens a bracket, returns the index just past its match; 'semicolon' — returns the index
    just past the first ';' outside any bracket. Strings, templates, comments and regex literals are stepped over. */
function scan(src, i, mode) {
  const stack = []; let lastSig = '', lastWord = '';
  for (; i < src.length; i++) {
    const c = src[i], n = src[i + 1];
    if (c === '/' && n === '/') { const e = src.indexOf('\n', i); i = e < 0 ? src.length : e; continue; }
    if (c === '/' && n === '*') { i = src.indexOf('*/', i + 2) + 1; continue; }
    if (c === '"' || c === "'") { i = skipString(src, i, c); lastSig = 'v'; lastWord = ''; continue; }
    if (c === '`') { i = skipTemplate(src, i); lastSig = 'v'; lastWord = ''; continue; }
    if (c === '/' && (lastSig === '' || '(,=:[!&|?{};+-*%<>~^'.includes(lastSig) || (lastSig === 'w' && KEYWORDS_BEFORE_REGEX.has(lastWord)))) { i = skipRegex(src, i); lastSig = 'v'; lastWord = ''; continue; }
    if (/\s/.test(c)) continue;
    if (/[\w$]/.test(c)) { let j = i; while (j < src.length && /[\w$]/.test(src[j])) j++; lastWord = src.slice(i, j); lastSig = 'w'; i = j - 1; continue; }
    if ('({['.includes(c)) stack.push(c);
    else if (')}]'.includes(c)) { stack.pop(); if (mode === 'close' && !stack.length) return i + 1; }
    else if (c === ';' && mode === 'semicolon' && !stack.length) return i + 1;
    lastSig = c; lastWord = '';
  }
  throw new Error('scan ran off the end');
}
function only(re, what) {
  const hits = [...html.matchAll(re)];
  if (hits.length !== 1) throw new Error(`design-1.html: expected exactly one ${what}, found ${hits.length}`);
  return hits[0];
}
/** `function NAME(…) {…}` (async too), whole. */
function fnSrc(name) {
  const m = only(new RegExp(`(?:async\\s+)?function\\s+${name}\\s*\\(`, 'g'), `function ${name}`);
  const paramsEnd = scan(html, m.index + m[0].length - 1, 'close');
  const body = html.indexOf('{', paramsEnd);
  if (html.slice(paramsEnd, body).trim()) throw new Error(`function ${name}: no body after its parameters`);
  return html.slice(m.index, scan(html, body, 'close'));
}
/** `const NAME = …;`, whole (one statement: a declaration list included). */
function constSrc(name) {
  const m = only(new RegExp(`\\b(?:const|let|var)\\s+${name}\\s*=`, 'g'), `declaration of ${name}`);
  return html.slice(m.index, scan(html, m.index + m[0].length, 'semicolon'));
}
/** A bridge command `async "NAME"(…) {…}` as `async function as(…) {…}`. */
function commandSrc(name, as) {
  const m = only(new RegExp(`async\\s+"${name.replace('.', '\\.')}"\\s*\\(`, 'g'), `command "${name}"`);
  const open = m.index + m[0].length - 1, paramsEnd = scan(html, open, 'close'), body = html.indexOf('{', paramsEnd);
  return { src: `async function ${as}${html.slice(open, paramsEnd)} ${html.slice(body, scan(html, body, 'close'))}`, text: html.slice(m.index, scan(html, body, 'close')) };
}
/** The comparator orders.snapshot sorts its answer with. */
function snapshotComparator() {
  const { text } = commandSrc('orders.snapshot', 'ordersSnapshot');
  const at = text.indexOf('orders.sort(');
  if (at < 0 || text.indexOf('orders.sort(', at + 1) >= 0) throw new Error('orders.snapshot: expected one orders.sort(');
  const open = at + 'orders.sort'.length;
  return `const stationCompare = ${text.slice(open + 1, scan(text, open, 'close') - 1)};`;
}

/* ═══ 2 · Etsy, faked under the real Netlify functions ═══ */
const etsy = { receipts: new Map(), pages: new Map() };
const etsyHeaders = { get: () => null };
const answer = (status, value) => { const body = JSON.stringify(value); return { ok: status >= 200 && status < 300, status, headers: etsyHeaders, json: async () => JSON.parse(body), text: async () => body }; };
async function fakeEtsyFetch(url) {
  const u = new URL(String(url));
  let m = /\/shops\/[^/]+\/receipts\/(\d+)$/.exec(u.pathname);
  if (m) return etsy.receipts.has(m[1]) ? answer(200, etsy.receipts.get(m[1])) : answer(404, { error: 'Receipt not found' });
  if (/\/shops\/[^/]+\/receipts$/.test(u.pathname)) { const results = etsy.pages.get(Number(u.searchParams.get('offset') || 0)) || []; return answer(200, { count: results.length, results }); }
  throw new Error('unexpected Etsy URL ' + url);
}
Object.assign(process.env, { SHOP_ID: '7654321', CLIENT_ID: 'test-keystring', CLIENT_SECRET: 'test-shared-secret' });   // fake values for the handlers' env checks
const realLoad = Module._load, realFetch = global.fetch;
Module._load = function (request) { return request === 'node-fetch' ? fakeEtsyFetch : realLoad.apply(this, arguments); };
global.fetch = fakeEtsyFetch;   // listOpenOrders.js keeps global.fetch as it loads
const listOpenOrders = require(path.join(root, 'netlify/functions/listOpenOrders.js'));
const etsyOrderProxy = require(path.join(root, 'netlify/functions/etsyOrderProxy.js'));
Module._load = realLoad; global.fetch = realFetch;

/** The station's apiFetch, answered by the Netlify handlers. */
async function apiFetch(url) {
  const u = new URL(url, 'http://station.test');
  const handler = { '/listOpenOrders': listOpenOrders.handler, '/etsyOrderProxy': etsyOrderProxy.handler }[u.pathname];
  if (!handler) throw new Error('unexpected station call ' + url);
  const res = await handler({ queryStringParameters: Object.fromEntries(u.searchParams), headers: { 'Access-Token': 'test-access' } });
  const body = res.body;
  return { ok: res.statusCode >= 200 && res.statusCode < 300, status: res.statusCode, headers: { get: k => (res.headers || {})[k] ?? null }, json: async () => JSON.parse(body), text: async () => body };
}

/* ═══ 3 · the station, in a vm ═══ */
const stationErrors = [];
const station = (() => {
  const ctx = vm.createContext({
    console: { log() {}, info() {}, warn: (...a) => stationErrors.push(['warn', ...a]), error: (...a) => stationErrors.push(['error', ...a]) },
    setTimeout, clearTimeout, apiFetch, db: null, __msgDocs: new Map(), __notes: new Map(),
  });
  const stubs = `
    var orderCache = {}, allOpenReceipts = [], currentReceipts = [], detailCache = new Map();
    var staffNoteIDs = new Set(), staffNoteTexts = new Map(), msgOrderIDs = new Set(), completedOrders = new Set(), selectedOrders = new Set();
    var rtSelected = new Map(), rtClaims = new Map(), RT = { clientId: "station-under-test" }, etsyMeter = { total: 0 };
    var Cursor = { glide() {}, act: async () => {} }, FN = "/.netlify/functions", __puts = [];
    function cacheIsFresh() { return false; }
    function cachePut(rid, txs, stamp, receipt) { __puts.push({ rid: String(rid), txs, stamp, receipt }); }
    function paintRow() {} function scheduleFilterRefresh() {} function setMessageFlag() {}
    function etsyBraked() { return false; }
    async function sleep() {}
    async function fetch(url) { const rid = decodeURIComponent(/orderId=([^&]+)/.exec(url)[1]); return { ok: true, json: async () => ({ success: true, data: { "Staff Note": __notes.get(rid) } }) }; }
  `;
  const parts = [
    stubs,
    ...['METALS', 'METAL_ORDER', 'METAL_BY_KEY', 'METAL_KEYS', 'SOLID_RE', 'KARAT_RE', 'GOLD_FILLED_RE', 'SOLID_UNKNOWN', 'PAGE_SIZE'].map(constSrc),
    fnSrc('normMetalText'), fnSrc('metalKeyFromText'), fnSrc('metalKeyForTx'), constSrc('isRealMetal'), fnSrc('resolveTxMetal'),
    fnSrc('txMetalResolved'), fnSrc('metalCountsFor'), fnSrc('rowMetalFor'), fnSrc('countsAreEmpty'), fnSrc('metalLabelFor'), fnSrc('txNeedsAttention'),
    constSrc('receiptStamp'), fnSrc('fetchOpenOrdersSequential'), fnSrc('decorateReceipt'), fnSrc('pullEtsyOrderDetails'),
    constSrc('__hydrateInflight'), fnSrc('hydrateOrder'), fnSrc('applyHydration'),
    constSrc('metalOf'), fnSrc('lineForBridge'), fnSrc('ttlCache'), constSrc('notesCache'), fnSrc('staffNoteOf'), fnSrc('messagesOf'), fnSrc('orderForBridge'),
    commandSrc('orders.detail', 'ordersDetail').src, snapshotComparator(),
    `({ metalKeyFromText, metalKeyForTx, txMetalResolved, metalCountsFor, rowMetalFor, countsAreEmpty, metalLabelFor, txNeedsAttention, receiptStamp,
        fetchOpenOrdersSequential, decorateReceipt, lineForBridge, orderForBridge, ordersDetail, stationCompare, notesCache, msgCache, METAL_BY_KEY })`,
  ];
  const api = vm.runInContext(parts.join('\n;\n'), ctx, { filename: 'design-1.html (cut)' });
  api.ctx = ctx;
  return api;
})();

/* ═══ 4 · fixtures: Etsy Open API v3 receipts ═══ */
let txSeq = 5100000000, listingSeq = 1500000000, productSeq = 20000000000;
const vr = (name, value, propertyId = 513, valueId = 1001) => ({ property_id: propertyId, value_id: valueId, formatted_name: name, formatted_value: value });
const pers = value => ({ property_id: 54, value_id: null, formatted_name: 'Personalization', formatted_value: value });
const money = amount => ({ amount, divisor: 100, currency_code: 'USD' });
function tx(o = {}, drop = []) {
  const id = txSeq++;
  const t = Object.assign({
    transaction_id: id, title: 'Dainty Initial Necklace', description: 'A tiny initial on a fine chain.', seller_user_id: 111, buyer_user_id: 222,
    create_timestamp: 1763000000, created_timestamp: 1763000000, paid_timestamp: 1763000100, shipped_timestamp: null, quantity: 1, listing_image_id: 7000000001,
    receipt_id: 0, is_digital: false, file_data: '', listing_id: listingSeq++, transaction_type: 'listing', product_id: productSeq++, sku: 'NECK-INIT-01',
    price: money(2999), shipping_cost: money(0), variations: [], product_data: [], shipping_profile_id: 99, min_processing_days: 1, max_processing_days: 3,
    shipping_method: null, shipping_upgrade: null, expected_ship_date: 1763500000, buyer_coupon: 0, shop_coupon: 0,
  }, o);
  drop.forEach(k => delete t[k]);
  return t;
}
function receipt(id, o = {}, txs = [], drop = []) {
  const r = Object.assign({
    receipt_id: id, receipt_type: 0, seller_user_id: 111, seller_email: 'shop@example.test', buyer_user_id: 222, buyer_email: 'buyer@example.test',
    name: 'Ada Buyer', first_line: '1 Test Street', second_line: '', city: 'Springfield', state: 'IL', zip: '62701', status: 'Paid',
    formatted_address: 'Ada Buyer\n1 Test Street\nSpringfield, IL 62701', country_iso: 'US', payment_method: 'cc', payment_email: '',
    message_from_seller: null, message_from_buyer: '', message_from_payment: null, is_paid: true, is_shipped: false,
    create_timestamp: 1763000000, created_timestamp: 1763000000, update_timestamp: 1763100000, updated_timestamp: 1763100000,
    is_gift: false, gift_message: '', gift_sender: '', grandtotal: money(2999), subtotal: money(2999), total_price: money(2999),
    total_shipping_cost: money(0), total_tax_cost: money(0), total_vat_cost: money(0), discount_amt: money(0), gift_wrap_price: money(0),
    shipments: [], transactions: txs, refunds: [],
  }, o);
  (r.transactions || []).forEach(t => { if (t && 'receipt_id' in t) t.receipt_id = id; });
  drop.forEach(k => delete r[k]);
  return r;
}

const F = [];
const add = (name, r, extras = {}) => F.push({ name, raw: r, extras });
let rid = 4200000001;
add('gf-personalized', receipt(rid++, {}, [tx({ title: 'Dainty Name Necklace', sku: 'NECK-GF-01', variations: [vr('Metal', '14k Gold Filled'), pers('Emma')] })]),
  { staffNote: 'Use the &quot;script&quot; font', msgDocs: 2 });
add('rose-gf-color-qty2', receipt(rid++, {}, [tx({ quantity: 2, variations: [vr('Color', 'Rose Gold Filled'), vr('Length', '16 inches', 514, 2001)] })]),
  { hasNote: false, staffNote: 'text of a note no longer in the index' });
add('silver-other-variation', receipt(rid++, { message_from_buyer: 'Can you rush this?' }, [tx({ variations: [vr('Style', 'Sterling Silver', 515, 3001), pers('J &amp; K')] })]),
  { hasNote: true, staffNote: '', hasMessages: true, completed: true });
add('solid14-metal-var', receipt(rid++, {}, [tx({ title: 'Solid Gold Initial Necklace', variations: [vr('Metal', '14k Solid Gold')] })]), { msgDocs: 6 });
add('solid10-and-goldengrave', receipt(rid++, {}, [tx({ variations: [vr('Metal Type', 'Solid Gold 10K')] }), tx({ variations: [vr('Metal', '10K Gold + Engrave'), pers('Mia')] })]),
  { staffNote: 'Two pieces — keep together', msgDocs: 2 });
add('solid-no-karat', receipt(rid++, {}, [tx({ title: 'Heart Charm', variations: [vr('Metal', 'Solid Gold')] })]));
add('title-only-silver', receipt(rid++, {}, [tx({ title: 'Sterling Silver Birth Flower Ring', sku: 'RING-BF' })]), { hasNote: true, staffNote: 'Size 7', completed: true });
add('title-only-14k', receipt(rid++, {}, [tx({ title: '14k Solid Gold Huggie Hoops' })]), { msgDocs: 6 });
add('title-solid-no-karat', receipt(rid++, {}, [tx({ title: 'Solid Gold Tiny Star Studs' })]), { staffNote: 'Check the karat with the buyer', msgDocs: 2 });
add('multi-line-entities', receipt(rid++, { message_from_buyer: 'Please gift wrap &amp; don&#39;t include the receipt — &quot;thanks&quot;' }, [
  tx({ title: 'Mom &amp; Me Necklace', variations: [vr('Metal', 'Gold Filled'), pers('&quot;Mom&quot; &amp; me')] }),
  tx({ title: 'Kid&#39;s Name Bracelet', quantity: 2, expected_ship_date: 1763400000, variations: [vr('Colour', 'Sterling Silver'), pers('Leo &#39;24')] }),
  tx({ title: 'Birthstone Charm', variations: [vr('Color', 'Rose Gold'), vr('Chain', 'Gold extender 2&quot;', 516, 4001)] }),
]));
add('not-requested', receipt(rid++, {}, [tx({ variations: [vr('Metal', 'Sterling Silver'), pers('Not requested on this item.')] })]), { hasNote: true, staffNote: 'Plain', hasMessages: true });
add('receipt-personalization', receipt(rid++, { personalization: '  Ava &amp; Noah  ' }, [tx({ variations: [vr('Metal', '14k Gold Filled')] })]), { msgDocs: 6 });
add('gift-order', receipt(rid++, { is_gift: true, gift_message: 'Happy b&#39;day &amp; love you', gift_sender: 'Grandma' }, [tx({ variations: [vr('Metal', 'Gold')] })]),
  { staffNote: 'Gift — no prices', msgDocs: 2, completed: true });
add('missing-optional', receipt(rid++, { country: 'CA', creation_tsz: 1700000000 },
  [tx({ title: 'Gold Hoop Earrings' }, ['sku', 'product_id', 'listing_id', 'variations', 'quantity', 'expected_ship_date'])],
  ['name', 'city', 'country_iso', 'message_from_buyer', 'is_gift', 'gift_message', 'create_timestamp', 'created_timestamp']));
add('zero-stamps-fallbacks', receipt(rid++, { update_timestamp: 0, updated_timestamp: 0, last_modified_tsz: 1763123456, dispatch_date: 1763600000 },
  [tx({ expected_ship_date: 0, variations: [vr('Metal', 'Sterling Silver')] })]), { hasNote: true, staffNote: 'Late', completed: true });
add('no-timestamps', receipt(rid++, {}, [tx({ variations: [vr('Metal', 'Gold Filled')] }, ['expected_ship_date', 'create_timestamp', 'created_timestamp'])],
  ['create_timestamp', 'created_timestamp', 'update_timestamp', 'updated_timestamp']), { msgDocs: 6 });
add('sku-variants', receipt(rid++, {}, [
  tx({ sku: '', variations: [vr('Metal', '14K Gold + Engrave')] }),
  tx({ sku: '  BRC-14K-02  ', variations: [vr('Metal', '14k Gold')] }),
  tx({ sku: 'ACORN&#39;S LEAF', variations: [vr('Metal', '10k Gold Filled')] }),
]), { staffNote: 'SKU check', msgDocs: 2 });
add('quantities', receipt(rid++, {}, [tx({ quantity: 3, variations: [vr('Metal', 'Gold Filled')] }), tx({ quantity: '2', variations: [vr('Metal', 'Sterling Silver')] }), tx({ quantity: 0, variations: [vr('Metal', 'Rose Gold Filled')] })]));
add('metal-var-wins', receipt(rid++, {}, [tx({ variations: [vr('Metal', 'Sterling Silver'), vr('Chain', 'Gold extender', 516, 4002)] })]), { hasNote: true, staffNote: 'x', hasMessages: true });
add('solid-var-karat-in-title', receipt(rid++, {}, [tx({ title: '14k Solid Gold Bar Necklace', variations: [vr('Metal', 'Solid Gold')] })]), { msgDocs: 6 });
add('empty-transactions', receipt(rid++, { message_from_buyer: 'Nothing listed?' }, []), { staffNote: 'Ask Etsy', msgDocs: 2 });
add('pre-set-item-fields', receipt(rid++, { message_from_buyer: 'Receipt note' }, [tx({ message_from_buyer: 'Line note', personalization: ['Pre'], variations: [vr('Metal', 'Gold Filled'), pers('From variation')] })]));
add('is-personalized-flag', receipt(rid++, {}, [tx({ is_personalized: true, variations: [vr('Metal', 'Gold Filled')] })]), { hasNote: true, staffNote: 'flag', completed: true });
add('odd-variations', receipt(rid++, {}, [tx({ variations: [null, { formatted_name: 'Metal' }, { property_id: 600, value_id: null, formatted_value: 'Silver' }, { formatted_name: 'Note', formatted_value: '' }] })]), { msgDocs: 6 });
add('colour-14-20-and-gf', receipt(rid++, {}, [tx({ variations: [vr('Colour', '14/20 Yellow Gold Filled')] }), tx({ variations: [vr('Finish', 'Rose GF', 517, 5001)] })]), { staffNote: 'GF both', msgDocs: 2 });
add('personalization-entities', receipt(rid++, { message_from_buyer: 'Lily&#39;s &lt;3 &amp; more' }, [tx({ variations: [vr('Metal', 'Sterling Silver'), { property_id: 54, value_id: null, formatted_name: 'Personalization (max 10 letters)', formatted_value: ' Lily&#39;s &amp; &quot;Bo&quot; ' }] })]));
add('string-timestamps', receipt(rid++, { update_timestamp: '1763300000', create_timestamp: '1763000000' }, [tx({ expected_ship_date: '1763700000', variations: [vr('Metal', 'Gold Filled')] })]), { hasNote: true, staffNote: 'strings', hasMessages: true });
add('solid-unknown-then-other-14k', receipt(rid++, {}, [tx({ variations: [vr('Metal', 'Solid'), vr('Option', '14k Solid Gold', 518, 6001)] })]), { msgDocs: 6 });
add('cancelled-shipped-flags', receipt(rid++, { status: 'Fully Refunded', is_paid: true }, [tx({ variations: [vr('Metal', 'Gold Filled')] })]), { staffNote: 'refund', msgDocs: 2 });
add('partially-refunded', receipt(rid++, { status: 'Partially Refunded', is_shipped: false }, [tx({ variations: [vr('Metal', 'Sterling Silver')] })]));
add('shipped', receipt(rid++, { status: 'Completed', is_shipped: true, message_from_buyer: 'thanks!' }, [tx({ variations: [vr('Metal', 'Rose Gold Filled')] })]), { hasNote: true, staffNote: 'shipped', completed: true });

/* message docs as Firestore holds them, newest first (the station asks for the newest 5) */
function msgDocsFor(n, id) {
  if (!n) return [];
  const pool = [
    { senderName: 'Paul', text: 'Use &quot;Mom&quot; &amp; heart', timestamp: { toMillis: () => 1763200500123 }, imageUrl: null },
    { senderName: '', text: 'photo attached', timestamp: { seconds: 1763200400 }, imageUrl: 'https://example.test/p.jpg' },
    { senderName: 'Kim', timestamp: null },
    { text: 'no sender', timestamp: { toMillis: () => 1763200200000 } },
    { senderName: 'Paul', text: 'second', timestamp: { seconds: 1763200100 }, imageUrl: '' },
    { senderName: 'Paul', text: 'oldest (beyond the 5)', timestamp: { seconds: 1763200000 } },
  ];
  return pool.slice(0, n).map(d => Object.assign({}, d, { text: d.text == null ? d.text : `${d.text} [${id}]` }));
}
const snap = d => ({ data: () => d });
station.ctx.db = { collection: () => ({ doc: id => ({ collection: () => ({ orderBy: () => ({ limit: n => ({ get: async () => ({ docs: (station.ctx.__msgDocs.get(String(id)) || []).slice(0, n).map(snap) }) }) }) }) }) }) };

/** What the station reads from Firestore for one order, set into its stubs; and the same as the module's extras. */
function setFirestore(id, ex, { sweptNotes = true } = {}) {
  const c = station.ctx; id = String(id);
  station.notesCache.clear(); station.msgCache.clear(); c.staffNoteTexts.clear(); c.__notes.clear(); c.__msgDocs.clear();
  c.staffNoteIDs = new Set(); c.msgOrderIDs = new Set(); c.completedOrders = new Set(); c.orderCache = {}; c.__puts = [];
  const hasNote = ex.hasNote !== undefined ? !!ex.hasNote : !!String(ex.staffNote || '').trim();
  if (hasNote) { c.staffNoteIDs.add(id); c.__notes.set(id, ex.staffNote); if (sweptNotes && ex.staffNote) c.staffNoteTexts.set(id, String(ex.staffNote)); }
  if (ex.hasMessages) c.msgOrderIDs.add(id);
  if (ex.completed) c.completedOrders.add(id);
  const docs = msgDocsFor(ex.msgDocs || 0, id);
  c.__msgDocs.set(id, docs);
  const extras = { staffNote: ex.staffNote, messages: Shape.messageRowsFromDocs(docs.map(snap)), completed: !!ex.completed };
  if (ex.hasNote !== undefined) extras.hasNote = ex.hasNote;
  if (ex.hasMessages !== undefined) extras.hasMessages = ex.hasMessages;
  return extras;
}

/* ═══ 5 · comparing ═══ */
const short = v => { try { const s = JSON.stringify(v); return s === undefined ? String(v) : s.length > 80 ? s.slice(0, 77) + '...' : s; } catch (_) { return String(v); } };
/** First difference between two values (key order included), or null. Works across the vm's realm. */
function diff(a, b, p = '$') {
  if (Object.is(a, b)) return null;
  const kind = v => v === null ? 'null' : Array.isArray(v) ? 'array' : typeof v;
  const ka = kind(a), kb = kind(b);
  if (ka !== kb) return `${p}: station ${ka} ${short(a)} / module ${kb} ${short(b)}`;
  if (ka === 'array') {
    if (a.length !== b.length) return `${p}.length: station ${a.length} / module ${b.length}`;
    for (let i = 0; i < a.length; i++) { const d = diff(a[i], b[i], `${p}[${i}]`); if (d) return d; }
    return null;
  }
  if (ka === 'object') {
    const ak = Object.keys(a), bk = Object.keys(b);
    if (ak.join('|') !== bk.join('|')) return `${p} keys: station [${ak}] / module [${bk}]`;
    for (const k of ak) { const d = diff(a[k], b[k], `${p}.${k}`); if (d) return d; }
    return null;
  }
  return `${p}: station ${short(a)} / module ${short(b)}`;
}
const withoutTouched = r => { const o = {}; for (const [k, v] of Object.entries(r)) if (k !== '_touched') o[k] = v; return o; };

let failed = 0, checks = 0;
function check(label, fn) {
  return Promise.resolve().then(fn).then(problem => {
    checks++;
    if (problem) { failed++; console.log(`FAIL ${label} — ${problem}`); } else console.log(`ok   ${label}`);
  }, err => { checks++; failed++; console.log(`FAIL ${label} — threw ${err && err.stack || err}`); });
}
/** Station vs module for each named pair, plus byte equality of the JSON. */
function compare(pairs) {
  for (const [what, st, mod] of pairs) {
    const d = diff(st, mod);
    if (d) return `${what} ${d}`;
    if (JSON.stringify(st) !== JSON.stringify(mod)) return `${what}: JSON differs`;
  }
  return null;
}
const unchanged = (before, value, what) => JSON.stringify(value) === before ? null : `${what} was changed by the module`;

/* ═══ 6 · the checks ═══ */
(async () => {
  const J = v => JSON.parse(JSON.stringify(v));
  for (const [i, f] of F.entries()) etsy.receipts.set(String(f.raw.receipt_id), f.raw);

  for (const [i, f] of F.entries()) {
    const id = String(f.raw.receipt_id), tag = `${String(i + 1).padStart(2, '0')} ${f.name}`, nTx = (f.raw.transactions || []).length;

    // A · list sweep (listOpenOrders → decorateReceipt → orderForBridge)
    await check(`list    ${tag} (${nTx} line${nTx === 1 ? '' : 's'})`, async () => {
      const extras = setFirestore(id, f.extras);
      const listed = J(f.raw), before = JSON.stringify(listed);
      const r = station.decorateReceipt(J(listed));
      const order = await station.orderForBridge(r);
      const p = Shape.prepareReceipt(listed), o = Shape.shapeOrder(p, extras);
      const lines = r.transactions.map((t, k) => ['line ' + k, station.lineForBridge(J(t), r), Shape.shapeLine(p.transactions[k], p)]);
      return compare([['receipt', r, p], ['order', order, o], ...lines]) || unchanged(before, listed, 'the receipt');
    });

    // B · fresh detail read (Etsy getShopReceipt → etsyOrderProxy → orders.detail { fresh })
    await check(`detail  ${tag}`, async () => {
      const extras = setFirestore(id, f.extras, { sweptNotes: false });
      station.ctx.allOpenReceipts = [];
      const res = await station.ordersDetail({ receiptId: id, fresh: true });
      const rc = station.ctx.__puts.at(-1).receipt;
      const bare = J(f.raw), before = JSON.stringify(bare);
      const p = Shape.prepareReceipt(bare, { via: 'detail', receiptId: id });
      const proxied = JSON.parse((await etsyOrderProxy.handler({ queryStringParameters: { orderId: id }, headers: { 'access-token': 't' } })).body);
      const pp = Shape.prepareReceipt(proxied);
      return compare([['receipt', rc, p], ['receipt (via proxy answer)', rc, pp], ['order', res.order, Shape.shapeOrder(p, extras)],
        ['status', { gone: res.gone, status: res.status, isShipped: res.isShipped }, Shape.detailStatus(bare)],
        ['status (via proxy answer)', { gone: res.gone, status: res.status, isShipped: res.isShipped }, Shape.detailStatus(proxied)]]) || unchanged(before, bare, 'the receipt');
    });

    // C · fresh detail read folded into the listed order (orders.detail { fresh } with the order on the list)
    await check(`refold  ${tag}`, async () => {
      const extras = setFirestore(id, f.extras);
      const listed = J(f.raw); listed.update_timestamp = 1762000000; listed.message_from_buyer = 'an older note'; listed.list_only_field = 'kept';
      if (i % 3 === 0) delete listed.transactions;
      station.ctx.allOpenReceipts = [station.decorateReceipt(J(listed))];
      const res = await station.ordersDetail({ receiptId: id, fresh: true });
      const r = withoutTouched(station.ctx.allOpenReceipts[0]);
      const before = JSON.stringify(listed);
      const p = Shape.prepareReceipt(J(f.raw), { via: 'detail', listed });
      return compare([['receipt', r, p], ['order', res.order, Shape.shapeOrder(p, extras)]]) || unchanged(before, listed, 'the listed receipt');
    });

    // D · a listed order without its items, hydrated (orders.detail → hydrateOrder → applyHydration)
    await check(`hydrate ${tag}`, async () => {
      const extras = setFirestore(id, f.extras);
      const listed = J(f.raw); if (i % 2) listed.transactions = []; else delete listed.transactions;
      station.ctx.allOpenReceipts = [station.decorateReceipt(J(listed))];
      const res = await station.ordersDetail({ receiptId: id });
      const r = withoutTouched(station.ctx.allOpenReceipts[0]);
      const before = JSON.stringify(listed), detail = J(f.raw);
      const p = Shape.prepareReceipt(listed, { detail, receiptId: id });
      return compare([['receipt', r, p], ['order', res.order, Shape.shapeOrder(p, extras)]]) || unchanged(before, listed, 'the listed receipt');
    });
  }

  // E · a hydrate whose detail read failed (pullEtsyOrderDetails → null): the order stays unhydrated
  await check('hydrate  read failed (404 from Etsy)', async () => {
    const f = F[0], id = '4299999999', extras = setFirestore(id, {});
    const listed = J(f.raw); listed.receipt_id = Number(id); delete listed.transactions;
    station.ctx.allOpenReceipts = [station.decorateReceipt(J(listed))];
    const res = await station.ordersDetail({ receiptId: id });
    const p = Shape.prepareReceipt(listed, { detail: null });
    const errs = stationErrors.splice(0);   // the station logs nothing for a non-ok read; anything here is a surprise
    return compare([['receipt', withoutTouched(station.ctx.allOpenReceipts[0]), p], ['order', res.order, Shape.shapeOrder(p, extras)]]) || (errs.length ? `station logged ${short(errs)}` : null);
  });

  // F · withNotes:false (orders.snapshot { withNotes:false })
  await check('notes    withNotes:false', async () => {
    const f = F.find(x => x.name === 'gf-personalized'), id = String(f.raw.receipt_id);
    const extras = setFirestore(id, f.extras);
    const r = station.decorateReceipt(J(f.raw));
    return compare([['order', await station.orderForBridge(r, { withNotes: false }), Shape.shapeOrder(Shape.prepareReceipt(J(f.raw)), Object.assign({}, extras, { withNotes: false }))]]);
  });

  // G · a whole snapshot: two list pages with a repeated receipt (fetchOpenOrdersSequential), shaped and sorted
  await check('snapshot two pages, repeats, shaped and sorted', async () => {
    const page0 = F.map(f => J(f.raw));
    for (let k = 0; page0.length < Shape.PAGE_SIZE; k++) page0.push(receipt(4300000000 + k, { update_timestamp: 1763100000 + k }, [tx({ expected_ship_date: k % 5 ? 1763500000 + (k % 7) * 86400 : 0, variations: [vr('Metal', ['Gold Filled', 'Sterling Silver', 'Rose Gold Filled', '14k Solid Gold', 'Solid Gold 10k'][k % 5])] })]));
    const lastFresh = J(page0.at(-1)); lastFresh.update_timestamp += 50; lastFresh.message_from_buyer = 'added while the sweep ran';
    const thirdFresh = J(page0[2]); thirdFresh.update_timestamp += 9;
    const page1 = [lastFresh, ...Array.from({ length: 8 }, (_, k) => receipt(4310000000 + k, {}, [tx({ variations: [vr('Metal', 'Gold Filled')] })])), thirdFresh];
    etsy.pages = new Map([[0, page0], [Shape.PAGE_SIZE, page1]]);
    const raw = await station.fetchOpenOrdersSequential();
    const collected = Shape.collectReceiptPages([J(page0), { results: J(page1) }]);
    const listProblem = compare([['listed receipts', raw, collected]]);
    if (listProblem) return listProblem;
    if (raw.length !== Shape.PAGE_SIZE + 8) return `expected ${Shape.PAGE_SIZE + 8} receipts, got ${raw.length}`;
    const exFor = r => { const f = F.find(x => String(x.raw.receipt_id) === String(r.receipt_id)); return f ? f.extras : {}; };
    const stationOrders = [];
    for (const r of raw) { const ex = exFor(r); setFirestore(r.receipt_id, ex); stationOrders.push(await station.orderForBridge(station.decorateReceipt(r))); }
    stationOrders.sort(station.stationCompare);
    const moduleOrders = collected.map(r => Shape.shapeReceipt(r, setFirestore(r.receipt_id, exFor(r)))).sort(Shape.compareOrders);
    return compare([['snapshot', stationOrders, moduleOrders]]);
  });

  // H · the exported helpers on their own
  const corpus = ['14k Gold Filled', '14K Gold + Engrave', '10K Gold + Engrave', '14k gold + engrave chain', '14k Gold', '10k Gold Filled', '14k Solid Gold', 'Solid Gold 10k',
    'Solid gold 14 karat', 'solid 14kt', 'Solid Gold', 'Solid', 'Rose Gold Filled', 'Rose GF', '14/20 Rose Gold', 'Sterling Silver', 'silvr', 'Yellow', 'Gold &quot;plated&quot;',
    '14k Gold Filled + solid gold chain', '', null, undefined, 42, 'gf', 'Platinum', 'Rose_gold-filled', 'Solid Rose Gold 14k'];
  await check('helper   metalKeyFromText (loose and solidStrict)', () => {
    for (const s of corpus) for (const o of [undefined, { solidStrict: true }, { solidStrict: false }]) {
      const a = o === undefined ? station.metalKeyFromText(s) : station.metalKeyFromText(s, o), b = o === undefined ? Shape.metalKeyFromText(s) : Shape.metalKeyFromText(s, o);
      if (a !== b) return `${short(s)} ${short(o)}: station ${short(a)} / module ${short(b)}`;
    }
    return null;
  });
  const allTx = () => F.flatMap(f => J(f.raw.transactions || [])).concat([{ title: 'Solid Gold', personalization: '14k' }, { title: 'x', personalization: 'Sterling' }, {}]);
  await check('helper   metalKeyForTx / txMetalResolved / metalOf / metalLabelFor', () => {
    for (const t of allTx()) {
      for (const o of [{}, { wide: true }]) { const a = station.metalKeyForTx(J(t), o), b = Shape.metalKeyForTx(J(t), o); if (a !== b) return `metalKeyForTx ${short(t.title)} ${short(o)}: ${a} / ${b}`; }
      const st = J(t), mt = J(t), before = JSON.stringify(mt);
      const a = station.txMetalResolved(st), b = Shape.txMetalResolved(mt);
      if (a !== b) return `txMetalResolved ${short(t.title)}: ${a} / ${b}`;
      if (JSON.stringify(mt) !== before) return 'txMetalResolved changed its argument';
      const la = station.metalLabelFor(st), lb = Shape.metalLabelFor(Object.assign(J(t), { _metalKey: st._metalKey }));
      if (la !== lb) return `metalLabelFor ${short(t.title)}: ${short(la)} / ${short(lb)}`;
      const lineKey = station.lineForBridge(J(t), {}).metalKey;
      if (lineKey !== Shape.metalOf(J(t))) return `metalOf ${short(t.title)}: ${lineKey} / ${Shape.metalOf(J(t))}`;
    }
    return null;
  });
  await check('helper   metalCountsFor / rowMetalFor / countsAreEmpty / txNeedsAttention', () => {
    const groups = F.map(f => J(f.raw.transactions || [])).concat([[], [{ quantity: 'x', variations: [vr('Metal', 'Gold')] }]]);
    for (const txs of groups) {
      const before = JSON.stringify(txs), a = station.metalCountsFor(J(txs)), b = Shape.metalCountsFor(txs);
      const d = diff(J(a), b); if (d) return 'metalCountsFor ' + d;
      if (JSON.stringify(txs) !== before) return 'metalCountsFor changed its argument';
      if (station.rowMetalFor(a) !== Shape.rowMetalFor(b)) return 'rowMetalFor';
      if (station.countsAreEmpty(a) !== Shape.countsAreEmpty(b)) return 'countsAreEmpty';
    }
    if (station.countsAreEmpty(null) !== Shape.countsAreEmpty(null)) return 'countsAreEmpty(null)';
    for (const t of allTx().concat([{ personalization: '  ' }, { personalization: ' x ' }, { message_from_buyer: '   ' }, { message_from_buyer: 'hi' }, { is_personalized: 'true' }]))
      if (station.txNeedsAttention(J(t)) !== Shape.txNeedsAttention(J(t))) return `txNeedsAttention ${short(t)}`;
    return null;
  });
  await check('helper   receiptStamp', () => {
    for (const r of [null, undefined, {}, { update_timestamp: 5 }, { updated_timestamp: '7' }, { last_modified_tsz: 9 }, { update_timestamp: 0, updated_timestamp: 0, last_modified_tsz: 3 }, { update_timestamp: 'x' }, ...F.map(f => f.raw)])
      if (!Object.is(station.receiptStamp(r), Shape.receiptStamp(r))) return `receiptStamp ${short(r)}`;
    return null;
  });
  await check('helper   METALS / METAL_BY_KEY match the station', () => diff(J(Object.fromEntries(Object.entries(station.METAL_BY_KEY).map(([k, m]) => [k, { ...m, match: m.match.map(String) }]))),
    J(Object.fromEntries(Object.entries(Shape.METAL_BY_KEY).map(([k, m]) => [k, { ...m, match: m.match.map(String) }])))));

  if (stationErrors.length) { failed++; console.log(`FAIL station logged errors: ${short(stationErrors)}`); }
  console.log(`${checks} checks, ${F.length} fixture receipts, ${failed} failed`);
  console.log(failed ? 'FAIL etsy-shape' : 'PASS etsy-shape');
  process.exit(failed ? 1 : 0);
})().catch(err => { console.log(`FAIL etsy-shape — ${err && err.stack || err}`); process.exit(1); });
