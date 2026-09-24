// The Design Station left on indefinitely: nothing it keeps may grow with time, a sorter sweep must not read the whole
// completion history, a claim must not query the page once per order, and a network blip must never sign it out of Etsy.
// Everything runs on slices of design-1.html and netlify/functions/firebaseOrders.js in a vm; no network, no live service.
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm'), path = require('node:path');
const root = path.join(__dirname, '../..');
const html = fs.readFileSync(path.join(root, 'design-1.html'), 'utf8');
const fnSrc = fs.readFileSync(path.join(root, 'netlify/functions/firebaseOrders.js'), 'utf8');
const cutFrom = (src, from, to) => { const a = src.indexOf(from), b = src.indexOf(to, a + from.length); assert(a >= 0 && b > a, `missing ${from.trim().slice(0, 60)}`); return src.slice(a, b); };
const cut = (from, to) => cutFrom(html, from, to);
const quiet = { log() {}, info() {}, warn() {}, error() {} };
const clock = { t: Date.UTC(2026, 8, 24, 7, 0, 0) };
class FakeDate extends Date { constructor(...a) { if (a.length) super(...a); else super(clock.t); } static now() { return clock.t; } }
const tick = () => new Promise(r => setImmediate(r));

const sections = [];
const section = (name, fn) => sections.push({ name, fn });

/* ── 1 · a sorter sweep asks about the open orders only; a person's Refresh still reads everything ── */
section('sorter sweeps read completions and notes for the open orders only', async () => {
  const code = cut('/* ═══ 3 · STATE', '/* SORT MODEL') + cut('/** Bounded-concurrency runner. */', '/** Serialises starts')
    + cut('/* ═══ 9 · COMPLETION LEDGER', '/* Targeted completion poll') + cut('async function refreshStaffNoteIDs(', '/* === 10b')
    + cut('/**\n * Pull the open-order list', '/** Re-fetch items for rows') + cut("/** The Refresh button's sequence", 'function wireUI()');
  const done = new Set(['C1', 'O7x']), notes = new Set(['O3', 'H9']), history = Array.from({ length: 500 }, (_, i) => 'H' + i);
  let open = [...Array.from({ length: 250 }, (_, i) => 'O' + i), 'C1', 'L1'], urls = [], failFull = false, failDc = false, onDc = null;
  const store = new Map([['ledger', JSON.stringify({ L1: clock.t })]]);
  const c = vm.createContext({
    console: quiet, Date: FakeDate, FN: '/fn', LS: { ledger: 'ledger', ledgerPurge: 'purge' }, LEDGER_TTL_DAYS: 60, LEDGER_MAX: Infinity,
    localStorage: { getItem: k => store.has(k) ? store.get(k) : null, setItem: (k, v) => store.set(k, String(v)), removeItem: k => store.delete(k) },
    fetch: async url => {
      urls.push(url); const q = new URL(url, 'http://x').searchParams, ok = b => ({ ok: true, status: 200, json: async () => b });
      if (q.get('designCompleted')) return failFull ? { ok: false, status: 500, json: async () => ({ error: 'deadline' }) } : ok({ success: true, orderNumbers: [...done, ...history] });
      if (q.get('dcFor')) { const ids = q.get('dcFor').split(','); if (onDc) await onDc(ids); if (failDc) throw new TypeError('Failed to fetch'); return ok({ success: true, orderNumbers: ids.filter(id => done.has(id) || history.includes(id)) }); }
      if (q.get('staffNotes')) return ok({ success: true, orderNumbers: [...notes] });
      if (q.get('staffNotesFor')) return ok({ success: true, orderNumbers: q.get('staffNotesFor').split(',').filter(id => notes.has(id)) });
      throw new Error('unexpected ' + url);
    },
    window: {}, SETTINGS: { autoHeal: false }, Activity: { show() {}, msg() {}, hide() {}, progress() {} },
    fetchOpenOrdersSequential: async () => open.map(id => ({ receipt_id: id })), cacheIsFresh: () => false, detailCache: new Map(),
    decorateReceipt() {}, receiptStamp: () => 0, pruneStaleSelection() {}, markStaffNoteOrders() {}, refreshMessageFlags() {},
    Bridge: { hooks: { swept() {}, orders() {} } }, fetchAllLocksOnce: async () => {}, ensureSelectedPreviews: async () => {},
    abortAllUpdateWork() {}, resetMessageFlags() {}, visibleReceiptIds: () => [], repairMissingReceiptData: async () => {}, healZeroMetalRows: async () => {}
  });
  vm.runInContext(code + `;var lastClearedReceipts = [], searchSnapshot = null, updateRunId = 0;
    function applyFilters() { currentReceipts = allOpenReceipts.filter(r => !completedOrders.has(String(r.receipt_id))); }
    this.api = { refreshOrders, restoreCompleted, completed: completedOrders, notes: () => staffNoteIDs, shown: () => currentReceipts.map(r => String(r.receipt_id)) };`, c);
  const { api } = c;
  await api.refreshOrders();                                    // the station's own Refresh button
  assert(urls.some(u => /designCompleted=1/.test(u)) && urls.some(u => /staffNotes=1/.test(u)), "a person's Refresh still reads every completion and note");
  assert(!api.shown().includes('C1') && api.shown().includes('O7x') === false && api.shown().includes('O1'));
  // since that read: O7 is completed at another bench, C1 is undone at another bench, L1 stays completed in this browser's ledger
  done.add('O7'); done.delete('C1'); urls = [];
  await api.refreshOrders({ heal: false, targeted: true });     // the sorter's intake sweep
  assert(!urls.some(u => /designCompleted=1|staffNotes=1/.test(u)), 'a sorter sweep never reads the whole history: ' + urls.filter(u => /=1\b/.test(u)).join(' '));
  const asked = urls.filter(u => /dcFor=/.test(u)).map(u => new URL(u, 'http://x').searchParams.get('dcFor').split(','));
  assert(asked.length >= 3 && asked.every(ids => ids.length <= 100), 'completions are asked about in chunks of at most 100 ids');
  assert.deepEqual(asked.flat().sort(), [...open].sort(), 'every open order is asked about, and only those');
  assert(urls.filter(u => /staffNotesFor=/.test(u)).every(u => new URL(u, 'http://x').searchParams.get('staffNotesFor').split(',').length <= 100));
  assert(api.completed.has('O7') && !api.shown().includes('O7'), 'an order completed elsewhere since the last sweep is filtered out');
  assert(!api.completed.has('C1') && api.shown().includes('C1'), 'an order undone elsewhere comes back, as the full read would have it');
  assert(api.completed.has('L1') && !api.shown().includes('L1'), "this browser's ledger still counts");
  assert(api.notes().has('O3') && !api.notes().has('H9'), 'staff notes of the open orders');
  // a failed read keeps what is known instead of clearing it first
  const before = api.completed.size; failFull = true;
  await api.restoreCompleted();
  assert.equal(api.completed.size, before, 'a failed full read keeps the completions already known');
  failDc = true; urls = []; await api.refreshOrders({ heal: false, targeted: true }); failDc = false;
  assert(api.completed.has('O7') && api.completed.has('H3'), 'a failed targeted read keeps them too');
  // a completion made here while the answer is on its way is not reversed by that (older) answer
  onDc = async () => { onDc = null; api.completed.add('O9'); }; await api.refreshOrders({ heal: false, targeted: true });
  assert(api.completed.has('O9'), 'a completion made during the read stands');
  // and a sandbox reset (completedOrders.clear()) during the read is not undone by the answer
  onDc = async () => { onDc = null; api.completed.clear(); }; await api.refreshOrders({ heal: false, targeted: true });
  assert(!api.completed.has('O7') && !api.completed.has('O9'), 'a reset made during the read stands');
});

section('the bridge sweep asks for the targeted read', async () => {
  let args = null; const st = { Date: FakeDate, SANDBOX: false, allOpenReceipts: [], completedOrders: new Set(), etsyMeter: { total: 0 }, etsyBraked: () => false, sweptRecently: () => false, SWEEP_MIN_MS: 90000, Cursor: { act: async () => {}, glide() {} }, refreshOrders: async a => { args = a; }, receiptStamp: () => 0, detailCache: new Map(), makeQueue: () => fn => fn(), hydrateOrder: async () => [], applyHydration() {}, orderForBridge: async r => r, etsyState: () => ({}), S: {}, METAL_ORDER: [] };
  vm.createContext(st); vm.runInContext('var cmds={' + cut('    async "orders.snapshot"', '    /** One paged list sweep') + '};', st);
  await st.cmds['orders.snapshot']({ refresh: true, intake: true }, () => {});
  assert(args && args.targeted === true && args.heal === false, 'orders.snapshot refreshes with { targeted: true }: ' + JSON.stringify(args));
});

/* ── 2 · one DOM pass per claim, un-claim and lock repaint (F4), and the lock view stays live for the sorter (12f) ── */
section('claims and lock repaints index the page once', async () => {
  const { JSDOM } = require('jsdom');
  const ids = Array.from({ length: 300 }, (_, i) => String(3500000000 + i));
  const dom = new JSDOM('<div id="rows">' + ids.map(id => `<div class="orderRow" data-receipt="${id}"></div><div class="tile" data-receipt="${id}"></div>`).join('') + '</div>');
  const document = dom.window.document; let queries = 0, hidden = true, active = true, fetches = [];
  const qsa = document.querySelectorAll.bind(document); document.querySelectorAll = sel => { queries++; return qsa(sel); };
  Object.defineProperty(document, 'hidden', { get: () => hidden });
  const c = vm.createContext({ console: quiet, document, Date: FakeDate, FN: '/fn', RT: { clientId: 'me' }, S: { runId: 'run1' }, cursorRows: async () => {},
    $$: (sel, rootEl = document) => Array.from(rootEl.querySelectorAll(sel)), setTimeout: () => 0, clearTimeout() {},
    Bridge: { active: () => active, hooks: { locks() {} } },
    fetch: async url => { fetches.push(url); return { ok: true, status: 200, json: async () => ({ locks: {}, unlocks: [], claims: {}, unclaims: [], now: clock.t }) }; } });
  vm.runInContext(cut('let rtSelected = new Map();', '/* Micro-batched writer') + cut('async function fetchAllLocksOnce()', 'let __healTimer = null;')
    + ';var cmds={' + cut('    async claim(a) {', '    async "orders.snapshot"') + '};this.st={rtSelected:()=>rtSelected,rtClaims:()=>rtClaims};this.apply=applyAllLocksToDOM;this.poll=fetchLocksOnce;', c);
  queries = 0; await c.cmds.claim({ receiptIds: ids, runId: 'run1' });
  assert(queries <= 1, `a claim of 300 orders queries the page once, not per order (${queries})`);
  assert.equal(document.querySelectorAll('.orderRow.claimed').length, 300); assert.equal(document.querySelectorAll('.tile.claimed').length, 300);
  queries = 0; await c.cmds.unclaim({ receiptIds: ids });
  assert(queries <= 1, `an un-claim of 300 orders queries the page once (${queries})`); assert.equal(document.querySelectorAll('.claimed').length, 0);
  ids.forEach(id => { c.st.rtSelected().set(id, 'other'); c.st.rtClaims().set(id, { run: 'r' }); });
  queries = 0; c.apply(); assert(queries <= 1, `a repaint of 300 locks and 300 claims queries the page once (${queries})`);
  assert.equal(document.querySelectorAll('.orderRow.rtLocked').length, 300);
  fetches = []; await c.poll(); assert(fetches.some(u => /rtSince=/.test(u)), 'while the sorter drives the station, a hidden tab still polls its locks');
  active = false; fetches = []; await c.poll(); assert.equal(fetches.length, 0, 'a hidden tab on its own still rests');
});

/* ── 3 · nothing kept per order, per listing, per commit or per run grows without bound ── */
section('caches are bounded', async () => {
  // Etsy meter: an order not read for 10 minutes leaves the per-order map
  const m = { console: quiet, Date: FakeDate, etsyMeter: { total: 0, times: [], maxQps: 0, minute: [], buckets: {}, byOrder: {}, day: '', dayTotal: 0, status429: 0, errors: 0, lastAt: 0, alarm: null, brakeUntil: 0 },
    ETSY_GUARD: { burstPerMinute: 1e9, per10Min: 1e9, sameOrderPer10Min: 4, brakeMs: 300000 }, ETSY_LS: 'meter', localStorage: { setItem() {} }, toast() {}, paintEtsyHud() {}, Bridge: { active: () => false } };
  vm.createContext(m); vm.runInContext(cut('function etsyBucketKey(t)', 'function paintEtsyHud()'), m);
  for (let i = 0; i < 1000; i++) m.etsyRecord('/fn/etsyOrderProxy?orderId=' + i);
  clock.t += 11 * 60000; m.etsyRecord('/fn/etsyOrderProxy?orderId=5');
  assert(Object.keys(m.etsyMeter.byOrder).length <= 1, `per-order read times drop out after 10 minutes (${Object.keys(m.etsyMeter.byOrder).length} kept)`);

  // listing photos: the 500 most recently used in memory; an empty answer is asked again after 5 minutes
  let requests = 0, answer = [{ url_570xN: 'x.jpg' }];
  const img = { Map, Date: FakeDate, JSON, NS: ':test', FN: '/fn', runImageTask: fn => fn(), fetch: async () => { requests++; return { ok: true, status: 200, json: async () => answer }; } };
  vm.createContext(img); vm.runInContext(cut('const __imagesCache', '/** Route an external image') + ';this.cache=__imagesCache;this.load=fetchListingImages;', img);
  for (let i = 0; i < 600; i++) await img.load(String(i));
  assert(img.cache.size <= 500, `listing photos in memory are capped at 500 (${img.cache.size})`);
  requests = 0; await img.load('599'); assert.equal(requests, 0, 'a recent listing is served from memory');
  answer = []; await img.load('new'); await img.load('new'); assert.equal(requests, 1, 'an empty answer is not re-asked at once');
  clock.t += 6 * 60000; answer = [{ url_570xN: 'y.jpg' }]; const later = await img.load('new');
  assert.equal(requests, 2, 'an empty answer is asked again after 5 minutes'); assert.equal(later.length, 1);

  // notes and messages read for the sorter: 10-minute TTL, 500 most recent
  const notes = { Date: FakeDate, Map };
  vm.createContext(notes);
  let cacheSrc; try { cacheSrc = cut('  /* Staff notes and messages read for the sorter', '  async function staffNoteOf(rid) {'); } catch (_) { cacheSrc = cut('  const notesCache = new Map()', '  async function staffNoteOf(rid) {'); }
  vm.runInContext(cacheSrc + ';this.notes=notesCache;', notes);
  for (let i = 0; i < 600; i++) notes.notes.set(String(i), 'note ' + i);
  assert(notes.notes.size <= 500, `staff notes kept for the sorter are capped at 500 (${notes.notes.size})`);
  clock.t += 11 * 60000; assert(!notes.notes.has('599'), 'and a note is read again after 10 minutes');

  // the memory of the sorter's commits: newest 2,000 orders a run, a retried commit is still answered, old runs go
  const store = new Map(); const mem = { Date: FakeDate, Set, Object, JSON, LSK: { commits: 'commits' }, S: { sorterSelected: new Set() }, localStorage: { getItem: k => store.has(k) ? store.get(k) : null, setItem: (k, v) => store.set(k, String(v)) } };
  vm.createContext(mem); vm.runInContext(cut('  const readJson = (k, d) =>', '  function rememberSorterSelection()') + ';this.remember=rememberCommit;this.prior=priorCommit;', mem);
  mem.remember('old', ['z1'], { completed: ['z1'], refused: [] }); clock.t += 50 * 864e5;
  for (let b = 0; b < 30; b++) { const ids = Array.from({ length: 100 }, (_, i) => `${b}-${i}`); mem.remember('auto', ids, { completed: ids, refused: [] }); }
  const saved = JSON.parse(store.get('commits'));
  assert(saved.auto.ids.length <= 2000 && saved.auto.result.completed.length <= 2000, `a perpetual run keeps its newest 2,000 orders (${saved.auto.ids.length})`);
  const last = Array.from({ length: 100 }, (_, i) => `29-${i}`); assert.deepEqual(mem.prior('auto', last).completed, last, 'a retried commit is answered from memory');
  assert(!saved.old, 'a run not heard from for 45 days is forgotten');

  // the sandbox ledger: 2 days and 5,000 orders (production keeps 60 days, uncapped)
  const consts = src => { const t = /const LEDGER_TTL_DAYS\s*=\s*([^;]+);/.exec(src), mx = /const LEDGER_MAX\s*=\s*([^;]+);/.exec(src); return SANDBOX => ({ ttl: vm.runInNewContext(t[1], { SANDBOX }), max: mx ? vm.runInNewContext(mx[1], { SANDBOX, Infinity }) : Infinity }); };
  const ledgerFor = consts(html);
  assert.deepEqual(ledgerFor(true), { ttl: 2, max: 5000 }, 'sandbox ledger: 2 days, 5,000 orders'); assert.deepEqual(ledgerFor(false), { ttl: 60, max: Infinity }, 'production ledger unchanged');
  const lstore = new Map(); const led = { console: quiet, Date: FakeDate, LS: { ledger: 'ledger', ledgerPurge: 'purge' }, LEDGER_TTL_DAYS: 2, LEDGER_MAX: 5000, localStorage: { getItem: k => lstore.has(k) ? lstore.get(k) : null, setItem: (k, v) => lstore.set(k, String(v)) }, FN: '/fn', fetch: async () => ({}) };
  vm.createContext(led); vm.runInContext(cut('/* ═══ 9 · COMPLETION LEDGER', '/* Targeted completion poll') + ';this.add=addToLocalCompleted;this.load=loadLocalCompleted;', led);
  led.add(Array.from({ length: 6000 }, (_, i) => 'S' + i)); assert(led.load().size <= 5000, `the sandbox ledger is capped at 5,000 (${led.load().size})`);

  // the ledger purge also runs from the completion poll (a tab open for weeks), not only at boot
  let purges = 0; const poll = { purgeLedger: () => { purges++; }, pollCompletedTargeted: async () => false, setTimeout: () => 0, clearTimeout() {}, COMPLETE_STEPS: [5000], __completeIdle: 0, __completeTimer: null };
  vm.createContext(poll); vm.runInContext('var __completeIdle=0,__completeTimer=null;' + cut('function startCompletionPolling()', 'function wakeCompletionPolling') + ';startCompletionPolling();', poll);
  await tick(); assert.equal(purges, 1, 'the completion poll purges the ledger (once a day, by its own date)');

  // History: the sandbox's archive in its own database, production's name unchanged; at most the newest 50,000 rows
  const idbName = /const IDB_NAME\s*=\s*([^;]+);/.exec(html)[1];
  assert.equal(vm.runInNewContext(idbName, { NS: ':sandbox' }), 'designStationArchive:sandbox'); assert.equal(vm.runInNewContext(idbName, { NS: '' }), 'designStationArchive');
  const deleted = [], metas = []; const arc = { ARCHIVE_LOCAL_MAX: 50000, idbDeleteKeys: async (_, keys) => { deleted.push(...keys); }, idbPut: async (_, row) => { metas.push(row); } };
  vm.createContext(arc); vm.runInContext('var archiveIndex=[],archiveById=new Map(),archiveFloor=0,archiveOlderDone=false;' + cut('function sortArchive()', '/** Load whatever we already have on disk') + ';this.set=r=>{archiveIndex=r;sortArchive();};this.trim=trimArchive;this.len=()=>archiveIndex.length;this.floor=()=>archiveFloor;', arc);
  arc.set(Array.from({ length: 50010 }, (_, i) => ({ receiptId: 'A' + i, at: 1000 + i }))); await arc.trim();
  assert.equal(arc.len(), 50000); assert.deepEqual(deleted.sort(), Array.from({ length: 10 }, (_, i) => 'A' + i).sort(), 'the ten oldest rows leave the local copy');
  assert.equal(arc.floor(), 1010); assert(metas.some(x => x.k === 'floor' && x.at === 1010), 'and the floor is remembered for the Older button');
  const boot = cut('async function boot() {', '\n}\n');
  assert(!/loadArchiveFromDisk\(|syncArchive\(/.test(boot), 'boot neither loads nor syncs the History archive');
  const detail = { Map, ARCHIVE_DETAIL_MAX: 200 };
  vm.createContext(detail); vm.runInContext('var archiveDetail=new Map();' + cut('function keepArchiveDetail(', 'async function trimArchiveDetailStore') + ';this.keep=keepArchiveDetail;this.size=()=>archiveDetail.size;', detail);
  for (let i = 0; i < 300; i++) detail.keep(String(i), { i }); assert.equal(detail.size(), 200, 'opened History records: the 200 most recent in memory');

  // sandbox.reset clears every sandbox-only key and cache, not only the ledger
  const keys = new Map([['ledger:sandbox', '{}'], ['commits:sandbox', '{}'], ['orders:sandbox', '{}'], ['images:sandbox', '[]'], ['token', 'keep']]);
  const sb = { SANDBOX: true, completedOrders: new Set(['1']), LS: { ledger: 'ledger:sandbox' }, LSK: { commits: 'commits:sandbox' }, CACHE_KEY: 'orders:sandbox', __imagesKey: 'images:sandbox', detailCache: new Map([['1', {}]]), __imagesCache: new Map([['1', []]]), __imagesSaved: new Map([['1', {}]]), __imagesMiss: new Map(), notesCache: new Map([['1', 'n']]), msgCache: new Map([['1', []]]), allOpenReceipts: [1], S: {}, localStorage: { removeItem: k => keys.delete(k) } };
  vm.createContext(sb); vm.runInContext('var cmds={' + cut('    async "sandbox.reset"() {', '    /** The open receipts exactly as Etsy returned them') + '};', sb);
  await sb.cmds['sandbox.reset']();
  assert.deepEqual([...keys.keys()], ['token'], 'sandbox.reset removes the sandbox ledger, commit memory, order and photo caches');
  assert(!sb.detailCache.size && !sb.__imagesCache.size && !sb.notesCache.size, 'and their copies in memory');

  // storage full: the essential write drops the rebuildable caches (both namespaces) and lands
  const full = new Map([['designStation.orders.v2', 'x'.repeat(10)], ['designStation.listingImages.v1:sandbox', 'y']]); let room = false;
  const ls = { getItem: k => full.get(k) ?? null, removeItem: k => { full.delete(k); room = !full.has('designStation.orders.v2'); }, setItem: (k, v) => { if (!room) { const e = new Error('full'); e.name = 'QuotaExceededError'; throw e; } full.set(k, v); } };
  const q = { console: quiet, localStorage: ls, __imagesSaved: new Map([['1', {}]]) };
  vm.createContext(q); vm.runInContext(cut('function isQuotaError(e)', '/* ═══ 1 · TINY UI KIT') + ';this.put=setItemEssential;this.saved=__imagesSaved;', q);
  q.put('access_token', 'A9');
  assert.equal(full.get('access_token'), 'A9'); assert(!full.has('designStation.listingImages.v1:sandbox') && !q.saved.size, 'the caches went, the token landed');
  assert(/setItemEssential\(LS\.printJobs/.test(html) && /setItemEssential\(LS\.printLists/.test(html), 'the label hand-off to the print page survives a full quota the same way');
});

/* ── 4 · the chat listener backs off and says so once (12g) ── */
section('the chat listener backs off to a minute and toasts once', async () => {
  const waits = [], toasts = []; let pending = null;
  const c = { console: quiet, Math, S: { rid: '1', seq: 0, retries: 0 }, el: { live: null, thread: {} }, COLL: 'Brites_Orders', detach() {}, toast: t => toasts.push(t), setTimeout: (fn, ms) => { waits.push(ms); pending = fn; return 1; },
    db: { collection: () => ({ doc: () => ({ collection: () => ({ orderBy: () => ({ onSnapshot: (ok, bad) => { bad(new Error('permission-denied')); return () => {}; } }) }) }) }) } };
  vm.createContext(c); vm.runInContext(cut('  function subscribe() {', '  /* ── sending ──') + ';this.subscribe=subscribe;', c);
  c.subscribe(); for (let i = 0; i < 6; i++) { const f = pending; pending = null; f(); }
  assert.equal(toasts.length, 1, `one "Chat offline" toast per outage (${toasts.length})`);
  assert.deepEqual(waits, [4000, 8000, 16000, 32000, 60000, 60000, 60000], 'retries back off to once a minute');
});

/* ── 5 · the function answers per-order questions with bounded parallel "in" queries ── */
section('firebaseOrders: dcFor and staffNotesFor run ten-id "in" queries five at a time', async () => {
  let inFlight = 0, most = 0, queries = 0, gets = 0;
  const query = (ids, hit) => ({ get: async () => { queries++; inFlight++; most = Math.max(most, inFlight); await tick(); await tick(); inFlight--; return { docs: ids.filter(hit).map(id => ({ id, data: () => ({ 'Staff Note': 'call buyer' }) })) }; } });
  const where = hit => (field, op, ids) => { assert.equal(field, '__name__'); assert.equal(op, 'in'); assert(ids.length <= 10); return query(ids, hit); };
  const admin = { firestore: { FieldPath: { documentId: () => '__name__' } } }, parseIds = s => s.split(',').map(x => x.trim()).filter(Boolean);
  const ids = Array.from({ length: 100 }, (_, i) => String(i));
  const dc = { CORS: {}, COMPLETED_COLL: 'c', parseIds, admin, col: () => ({ where: where(id => Number(id) % 7 === 0) }) };
  vm.createContext(dc); vm.runInContext('this.lookup=async function(event){' + cutFrom(fnSrc, '      if (event.queryStringParameters?.dcFor)', '      /* ?staffNotesFor') + '};', dc);
  const r1 = JSON.parse((await dc.lookup({ queryStringParameters: { dcFor: ids.join(',') } })).body).orderNumbers;
  assert.deepEqual(Array.from(r1), ids.filter(id => Number(id) % 7 === 0), 'dcFor answers in the order asked'); assert.equal(queries, 10);
  assert(most > 1 && most <= 5, `dcFor runs its ten queries up to five at a time (${most} at once)`);
  inFlight = most = queries = 0;
  const sn = { CORS: {}, PREFIX: '', parseIds, admin, db: { collection: () => ({ where: where(id => Number(id) % 9 === 0), doc: () => ({ get: async () => { gets++; return { exists: false }; } }) }) } };
  vm.createContext(sn); vm.runInContext('this.lookup=async function(event){' + cutFrom(fnSrc, '      /* ?staffNotesFor', '      /* ?rtFor=') + '};', sn);
  const r2 = JSON.parse((await sn.lookup({ queryStringParameters: { staffNotesFor: ids.join(',') } })).body).orderNumbers;
  assert.equal(gets, 0, 'staffNotesFor makes no document get per order'); assert.equal(queries, 10);
  assert.deepEqual(Array.from(r2), ids.filter(id => Number(id) % 9 === 0), 'staffNotesFor answers in the order asked'); assert(most > 1 && most <= 5);
});

/* ── 6 · Etsy tokens: a network failure is never a sign-out (dormancy finding 1) ── */
function tokenWorld({ framed, locks = null, online = null }) {
  let quotaCode = '';
  try { quotaCode = cut('function isQuotaError(e)', '/* ═══ 1 · TINY UI KIT'); } catch (_) { quotaCode = 'function setItemEssential(k, v) { localStorage.setItem(k, v); }'; }
  const code = quotaCode + cut('let __refreshPromise = null;', 'function randomString(') + cut('let __oauthInFlight = false;', '/* ── Connect Etsy on the sorter') + cut('async function apiFetch(path, init = {}) {', '/** Receipt fetch with retries');
  const store = new Map(), emits = [], timers = [], conn = [], fetches = [], listeners = {};
  const c = vm.createContext({
    console: quiet, URL, URLSearchParams, Math, JSON, Number, String, Promise, Error, Object, Date: FakeDate, __imagesSaved: new Map(),
    localStorage: { getItem: k => (store.has(k) ? store.get(k) : null), setItem: (k, v) => store.set(k, String(v)), removeItem: k => store.delete(k) },
    TOKEN_KEYS: { access: 'access_token', refresh: 'refresh_token', expires: 'token_expires_at' },
    FN: 'https://design-1.example/.netlify/functions', SANDBOX: false, ETSY_LS: 'meter', ETSY_GUARD: { brakeMs: 300000 },
    etsyMeter: { brakeUntil: 0 }, isEtsyPath: p => /^\/(etsy|listOpenOrders|etsyImages)/i.test(p), runEtsyTask: fn => fn(),
    ETSY_CLIENT_ID: 'x', ETSY_REDIRECT: 'https://example', ETSY_SCOPE: 's', LS: { pkce: 'pkce', verifier: 'v' },
    $: () => null, paintBrandStrip() {}, toast() {}, setTimeout: (fn, ms) => { timers.push({ fn, ms }); return timers.length; }, clearTimeout() {},
    stashVerifier() {}, codeChallengeFor: async () => 'c', randomString: () => 'r', AbortSignal: undefined,
    navigator: locks ? { locks } : {},
    Bridge: { framed: () => framed, active: () => true, emit: (t, p) => emits.push({ t, p }), ticker() {} },
    window: { location: { href: 'https://design-1.example/design-1.html' }, addEventListener: (ev, fn) => { (listeners[ev] = listeners[ev] || []).push(fn); } },
    Response: class { constructor(b, i) { this.status = i.status; this.headers = { get: () => null }; } }
  });
  c.fetch = async (url, init) => { fetches.push(url.replace(/^.*functions/, '')); return c.net(url, init); };
  vm.runInContext(code + ';this.api={apiFetch,refreshAccessToken,startOAuth,ensureFreshToken};', c);
  c.setConn = (k, t) => { conn.push(t); };   // the badge itself needs the page
  return { c, store, emits, timers, conn, fetches, listeners, api: c.api };
}
const seed = w => { w.store.set('access_token', 'A1'); w.store.set('refresh_token', 'R1'); w.store.set('token_expires_at', String(Math.floor(clock.t / 1000) - 8 * 3600)); };
const netDown = async () => { throw new TypeError('Failed to fetch'); };
const etsyOk = (url, init) => /refreshEtsyToken/.test(url) ? { ok: true, status: 200, json: async () => ({ access_token: 'A2', refresh_token: 'R2', expires_in: 3600 }) }
  : { ok: true, status: 200, headers: { get: () => null }, json: async () => ({}) };

section('a network failure during the Etsy token refresh keeps the sign-in and retries', async () => {
  for (const framed of [true, false]) {
    const w = tokenWorld({ framed }); seed(w); w.c.net = netDown;
    const href = w.c.window.location.href;
    await assert.rejects(w.api.apiFetch('/listOpenOrders?limit=100'), /Etsy token refresh failed \(network\)/, 'the call fails as a passing (network) error');
    assert.equal(w.store.get('access_token'), 'A1'); assert.equal(w.store.get('refresh_token'), 'R1'); assert(w.store.get('token_expires_at'), 'every token key is kept');
    assert.equal(w.emits.filter(e => e.t === 'etsy.signin').length, 0, 'no sign-in is asked of the sorter');
    assert.equal(w.c.window.location.href, href, framed ? 'the frame stays' : 'the standalone station never navigates to Etsy sign-in');
    assert(w.conn.includes('Etsy: waiting for the network'), 'a quiet connection warning');
    // the retry: 15 s, 30 s, 60 s … at most 5 minutes, and at once when the browser is back online
    const retries = () => w.timers.filter(t => t.ms >= 15000 && t.ms <= 300000 && t.ms % 15000 === 0);
    for (let i = 0; i < 6; i++) { const t = retries().at(-1); clock.t += t.ms; t.fn(); await tick(); await tick(); }
    assert.deepEqual(retries().map(t => t.ms), [15000, 30000, 60000, 120000, 240000, 300000, 300000], 'capped backoff');
    assert(w.listeners.online && w.listeners.online.length, "a retry on the window's online event");
    w.c.net = etsyOk; w.listeners.online.forEach(fn => fn()); await tick(); await tick(); await tick();
    assert.equal(w.store.get('access_token'), 'A2', 'back online, the refresh lands'); assert.equal(w.store.get('refresh_token'), 'R2');
    assert.equal((await w.api.apiFetch('/listOpenOrders?limit=100')).status, 200);
  }
});

section('only a refused refresh signs out; a recovery after a sign-in request is reported', async () => {
  for (const [status, body, out] of [[400, { error: 'invalid_grant' }, true], [400, { error: 'Missing refresh_token' }, true], [429, {}, false], [503, {}, false], [500, { error: 'Unexpected token < in JSON' }, false]]) {
    const w = tokenWorld({ framed: true }); seed(w);
    w.c.net = async () => ({ ok: false, status, json: async () => body });
    const err = await w.api.refreshAccessToken().then(() => null, e => e);
    assert(err, `HTTP ${status} rejects`);
    assert.equal(!!err.signedOut, out, `HTTP ${status} ${body.error || ''}: signedOut=${out}`);
    assert.equal(w.store.has('access_token'), !out, out ? 'a refused refresh token signs the station out' : 'a passing failure keeps the tokens');
    assert.equal(w.emits.some(e => e.t === 'etsy.signin'), out);
  }
  // the station asked the sorter for a sign-in (a 401 that a refresh did not cure); a later good refresh says it is back
  const w = tokenWorld({ framed: true }); seed(w);
  await w.api.startOAuth('api-401'); assert(w.emits.some(e => e.t === 'etsy.signin'));
  w.c.net = etsyOk; await w.api.refreshAccessToken();
  const back = w.emits.find(e => e.t === 'etsy.connected');
  assert(back && back.p.etsy.signedIn === true && back.p.etsy.expiresAt > clock.t, 'etsy.connected follows the recovery');
  const ping = cut('    async ping() {', '    /** The sorter reset the sandbox');
  const p = { S: {}, Date: FakeDate, paintBanner() {}, etsyMeterSnapshot: () => ({}), etsyState: () => ({ signedIn: true }) };
  vm.createContext(p); vm.runInContext('var cmds={' + ping + '};', p); assert.equal((await p.cmds.ping()).signedIn, true, 'the ping reply carries signedIn');
});

section('one refresh at a time across tabs, and a tab that waited uses the token another refreshed', async () => {
  const requests = [];
  let w;
  const locks = { request: async (name, opts, fn) => { requests.push(name); if (typeof opts === 'function') fn = opts;
    // while this tab waited, another one refreshed: a fresh token is in the shared storage
    w.store.set('access_token', 'OTHER'); w.store.set('token_expires_at', String(Math.floor(clock.t / 1000) + 3600)); return fn(); } };
  w = tokenWorld({ framed: false, locks }); seed(w); w.c.net = etsyOk;
  const r = await w.api.apiFetch('/listOpenOrders?limit=100');
  assert.equal(r.status, 200); assert.deepEqual(requests, ['etsy-token-refresh'], 'the refresh runs under the etsy-token-refresh lock');
  assert(!w.fetches.includes('/refreshEtsyToken'), 'the token another tab refreshed is used, not refreshed again');
  assert.equal(w.store.get('access_token'), 'OTHER');
});

section('the station asks once for persistent storage, without waiting on it', async () => {
  const src = cut('function askPersistentStorage()', '\n}\n') + '\n}';
  let asked = 0; const c = { console: quiet, Promise, navigator: { storage: { persisted: async () => false, persist: async () => { asked++; return true; } } } };
  vm.createContext(c); vm.runInContext(src + ';this.ask=askPersistentStorage;', c);
  assert.equal(c.ask(), undefined, 'returns at once'); await tick(); await tick(); assert.equal(asked, 1);
  const bare = { console: quiet, Promise, navigator: {} }; vm.createContext(bare); vm.runInContext(src + ';askPersistentStorage();', bare);
  assert(/askPersistentStorage\(\)/.test(cut('async function boot() {', '\n}\n')), 'boot asks');
});

(async () => {
  let failed = 0;
  for (const s of sections) {
    try { await s.fn(); console.log('ok   ' + s.name); }
    catch (e) { failed++; console.log('FAIL ' + s.name + '\n     ' + String(e && e.message || e).split('\n')[0]); }
  }
  if (failed) { console.error(`${failed} of ${sections.length} station uptime checks failed`); process.exitCode = 1; }
  else console.log(`Station uptime OK: ${sections.length} checks — targeted sweeps, one DOM pass per claim, bounded caches and ledgers, chat backoff, parallel "in" queries, and Etsy tokens that survive a network blip`);
})();
