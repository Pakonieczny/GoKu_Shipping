/* PHOTOPAUSE (Paul, 20 Nov 2026, "Yes, lift it": the photo lookup pause lives only in the cloud, and the owner's Resume Etsy clears it; computers
   store nothing of production). Offline: the real image cache (netlify/functions/_etsyImageCache.js) on a fake Firestore with a fake clock and a
   fake Etsy (no network), and the real page code (charm-nest-bridge.js ListMedia) in jsdom.
   A  a usage report, a budget block (with blockedAt, and an older one with only updatedAt) and a listing's retryAt set BEFORE the last Resume
      Etsy no longer pause: each alone and all together, by read() and readMany(); the old block is dropped by the next reservation
   B  the same set AFTER the Resume still pauses (each alone and all together)
   C  the 500-a-day cap holds after a Resume
   D  with no Resume ever recorded (no switch document, or the switch on again after a 429: resumedAt null) every pause holds, as before
   E  a NEW 429 after the Resume pauses again (blockedAt kept; an older block is not carried into it); liftBlock clears the block
   F  the cache's Etsy fetch goes through the ETSYSTOP guard: while the shop is stopped nothing reaches Etsy, and a 429 turns the switch on
   G  the page keeps nothing in localStorage, removes the old keys, and its pause is only the cloud's latest answer
     node tests/charm-nest/photo-pause-resume.cjs */
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), crypto = require('node:crypto');
const root = path.join(__dirname, '../..');
const realFetch = require('node-fetch');
const { createImageCache } = require(path.join(root, 'netlify/functions/_etsyImageCache.js'));
const EtsyStop = require(path.join(root, 'netlify/functions/_etsyStop.js'));
const DAY = 86400000, T0 = Date.UTC(2026, 10, 20, 15, 0, 0), R = T0 - 60000, BEFORE = R - 3600000, AFTER = R + 30000;
const BUDGET = 'EtsyApi_Config/listingImages_' + crypto.createHash('sha256').update('test').digest('hex').slice(0, 24);
globalThis.fetch = async () => { throw new Error('tripwire: the built-in fetch was called'); };

const results = []; const check = (ok, what, data) => { results.push(!!ok); console.log(`${ok ? 'PASS' : 'FAIL'}  ${what}${ok ? '' : '  ' + JSON.stringify(data)}`); };

// ── a fake Firestore (transactions read before they write, as Firestore requires), a fake clock and a fake Etsy that counts what reaches it
function fixture({ seed = {}, status = 200, headers = {} } = {}) {
  const data = new Map(Object.entries(seed).map(([k, v]) => [k, structuredClone(v)]));
  let clock = T0, serial = Promise.resolve(); const urls = [];
  const ref = p => ({ path: p, get: async () => ({ data: () => structuredClone(data.get(p)), exists: data.has(p) }) });
  const db = { collection: name => ({ doc: id => ref(name + '/' + id) }), runTransaction: fn => { const p = serial.then(async () => { let wrote = false; const writes = []; const out = await fn({ get: r => { assert(!wrote, 'a transaction reads before it writes'); return r.get(); }, set: (r, v) => { wrote = true; writes.push(() => data.set(r.path, structuredClone(v))); } }); writes.forEach(f => f()); return out; }); serial = p.catch(() => {}); return p; } };
  const etsy = async url => {
    url = String(url); assert(/^https:\/\/api\.etsy\.com\/v3\/application\/listings\//.test(url), 'only the listing image addresses: ' + url); urls.push(url);
    const ids = url.includes('/listings/batch?') ? new URL(url).searchParams.get('listing_ids').split(',') : null;
    const body = status === 200 ? (ids ? { results: ids.map(listing_id => ({ listing_id, images: [{ rank: 1, url_570xN: 'first.jpg' }] })) } : { results: [{ rank: 1, url_570xN: 'first.jpg' }] }) : { error: 'Too Many Requests' };
    return new realFetch.Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json', ...headers } });
  };
  const make = fetchImpl => createImageCache({ db, env: { CLIENT_ID: 'test' }, now: () => clock, sleep: async ms => { clock += ms; }, fetch: fetchImpl || etsy });
  return { data, api: make, etsy, calls: () => urls.length, advance: ms => { clock += ms; }, doc: k => data.get(k) };
}
const STOP = { stopped: false, since: 0, resumedAt: R, resumedBy: 'Paul', rev: 2 };
const usage = at => ({ etsy: { reported_at: at, remaining_today: 4, limit_per_day: 100 } });
const block = at => ({ attempts: [], blockedUntil: T0 + DAY, blockedAt: at, updatedAt: at, cap: 500 });
const oldBlock = at => ({ attempts: [], blockedUntil: T0 + DAY, updatedAt: at, cap: 500 });   // (written before blockedAt was kept)
const item = at => ({ images: [], retryAt: T0 + DAY, fetchedAt: at, leaseUntil: 0, emptyUntil: 0 });
const set = (stop, parts) => Object.assign(stop ? { 'EtsyApi_Config/stop': stop } : {}, parts);
const kinds = at => ({
  'usage report': { 'EtsyApi_Config/usage': usage(at) },
  'budget block': { [BUDGET]: block(at) },
  'old budget block (updatedAt only)': { [BUDGET]: oldBlock(at) },
  'listing retryAt': { 'Etsy_Listing_Image_Cache/123': item(at) },
  'all three': { 'EtsyApi_Config/usage': usage(at), [BUDGET]: block(at), 'Etsy_Listing_Image_Cache/123': item(at) }
});
async function run(seed) {
  const one = fixture({ seed }), r1 = await one.api().read('123'), p1 = await one.api().pausedUntil();
  const many = fixture({ seed }), r2 = (await many.api().readMany(['123', '456']))['123'];
  return { read: { source: r1.source, status: r1.status, retryAt: r1.retryAt, calls: one.calls() }, readMany: { source: r2.source, status: r2.status, calls: many.calls() }, pause: p1, budget: one.doc(BUDGET) };
}

(async () => {
  // A — set before the last Resume Etsy: no longer a pause
  for (const [name, parts] of Object.entries(kinds(BEFORE))) {
    const o = await run(set(STOP, parts));
    check(o.read.status === 200 && o.read.source === 'etsy' && o.read.calls === 1 && o.readMany.status === 200 && o.readMany.calls === 1 && o.pause === 0,
      `A ${name} from before the Resume: no pause (read and readMany each ask Etsy once; the cloud's pause is 0)`, o);
  }
  { const f = fixture({ seed: set(STOP, { [BUDGET]: oldBlock(BEFORE) }) }); await f.api().read('123'); const b = f.doc(BUDGET);
    f.advance(5000); await f.api().read('456');
    check(b.blockedUntil === 0 && b.updatedAt === T0 && f.calls() === 2 && (await f.api().pausedUntil()) === 0, 'A the next reservation drops the old block, so its newer updatedAt never brings it back', { b, calls: f.calls() }); }

  // B — the same set after the Resume still pauses
  for (const [name, parts] of Object.entries(kinds(AFTER))) {
    const o = await run(set(STOP, parts)), itemOnly = name === 'listing retryAt';
    check(o.read.status === 429 && o.read.calls === 0 && o.readMany.status === 429 && ['paused', 'budget-paused'].includes(o.readMany.source) && o.readMany.calls === (itemOnly ? 1 : 0) && (itemOnly ? o.pause === 0 : o.pause > T0),
      `B ${name} from after the Resume: still pauses (no Etsy call for it${itemOnly ? '; a listing pause is that listing\'s only' : '; the cloud\'s pause says until when'})`, o);
  }

  // C — the 500-a-day cap holds after a Resume
  { const stamps = Array.from({ length: 500 }, (_, i) => BEFORE + i), o = await run(set(STOP, { [BUDGET]: { attempts: stamps, cap: 500, updatedAt: BEFORE + 499 } }));
    check(o.read.status === 429 && o.read.source === 'budget-paused' && o.read.retryAt === stamps[0] + DAY && o.read.calls === 0 && o.readMany.calls === 0 && o.pause === stamps[0] + DAY, 'C 500 lookups in the last 24 h: paused after a Resume too, until the oldest is a day old', o);
    const p = await run(set(STOP, { [BUDGET]: { attempts: stamps.slice(1), cap: 500 } }));
    check(p.read.calls === 1 && p.read.status === 200 && p.budget.attempts.length === 500, 'C 499: the 500th lookup still runs', p.read); }

  // D — no Resume ever recorded: every pause holds, as before
  for (const stop of [null, { stopped: true, since: T0 - 10000, fn: 'etsyOrderProxy', resumedAt: null, resumedBy: null, rev: 3 }]) {
    for (const [name, parts] of Object.entries(kinds(BEFORE))) {
      const o = await run(set(stop, parts)), itemOnly = name === 'listing retryAt';
      check(o.read.status === 429 && o.read.calls === 0 && o.readMany.status === 429 && o.readMany.calls === (itemOnly ? 1 : 0) && (itemOnly ? o.pause === 0 : o.pause > T0),
        `D ${stop ? 'switch on again (resumedAt null)' : 'no switch document'}: ${name} still pauses, as before`, o);
    }
  }

  // E — a NEW 429 after the Resume pauses again; an older block is not carried into it; liftBlock clears a block
  { const f = fixture({ seed: set(STOP, { [BUDGET]: block(BEFORE) }), status: 429, headers: { 'retry-after': '7200' } });
    const r = await f.api().read('123'), b = f.doc(BUDGET), it = f.doc('Etsy_Listing_Image_Cache/123');
    const again = await f.api().read('456'), many = (await f.api().readMany(['789']))['789'], pause = await f.api().pausedUntil();
    check(r.status === 429 && f.calls() === 1 && b.blockedUntil === T0 + 7200000 && b.blockedAt === T0 && it.fetchedAt === T0 && it.retryAt === T0 + 7200000 && again.status === 429 && many.source === 'budget-paused' && pause === T0 + 7200000,
      'E a new 429 after the Resume: blocked for its Retry-After (blockedAt kept; the old day-long block is not carried), every other listing paused, no second Etsy call', { r, b, it, again, many, pause, calls: f.calls() }); }
  { const f = fixture({ seed: set(STOP, { [BUDGET]: { ...block(AFTER), attempts: [AFTER] } }) }); const lifted = await f.api().liftBlock(), b = f.doc(BUDGET);
    check(lifted === true && b.blockedUntil === 0 && b.attempts.length === 1 && (await f.api().pausedUntil()) === 0, 'E liftBlock (Resume Etsy) clears the block and keeps the day\'s attempts', b); }

  // F — the cache's Etsy fetch goes through the ETSYSTOP guard
  { const src = fs.readFileSync(path.join(root, 'netlify/functions/_etsyImageCache.js'), 'utf8');
    check(/fetch:require\("\.\/_etsyStop"\)\.guard\(require\("node-fetch"\), "_etsyImageCache"\)/.test(src) && (src.match(/\bfetch\(/g) || []).length === 2, 'F the production cache uses the guarded fetch (its only two fetch calls)', null);
    let cloud = { stopped: true, since: T0 - 1000, rev: 1 }; EtsyStop._setStore({ async get() { return JSON.parse(JSON.stringify(cloud)); }, async set(p) { cloud = Object.assign({}, cloud, JSON.parse(JSON.stringify(p))); } });
    const f = fixture({ seed: set(STOP, {}) }), guarded = EtsyStop.guard(f.etsy, '_etsyImageCache');
    const r = await f.api(guarded).read('123');
    check(r.status === 429 && f.calls() === 0, 'F while the shop is stopped the lookup never reaches Etsy', { r, calls: f.calls() });
    cloud = { stopped: false, resumedAt: R, rev: 2 }; EtsyStop._clearCache();
    const g = fixture({ seed: set(STOP, {}), status: 429 }), r2 = await g.api(EtsyStop.guard(g.etsy, '_etsyImageCache')).read('123');
    check(r2.status === 429 && g.calls() === 1 && cloud.stopped === true && cloud.fn === '_etsyImageCache', 'F a 429 after the Resume turns the shop\'s switch on again (ETSYSTOP)', { cloud, calls: g.calls() }); }

  // G — the page: nothing in localStorage, the old keys removed, the pause only the cloud's latest answer
  const { JSDOM } = require('jsdom'); const source = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  check(!/localStorage\.(setItem|getItem)\([^)]*[Pp]ause/.test(source) && !/photoPauseStorage/.test(source), 'G the bridge neither writes nor reads a photo pause in localStorage', null);
  const page = saved => {
    const dom = new JSDOM('<main></main>', { runScripts: 'outside-only', url: 'https://fixture.invalid/' }), w = dom.window, calls = [], writes = []; let clock = Date.now(), reply = async () => ({ images: {} });
    Object.defineProperty(w, 'localStorage', { value: { getItem: k => saved.get(k) ?? null, setItem: (k, v) => { writes.push(k); saved.set(k, String(v)); }, removeItem: k => saved.delete(k), key: i => [...saved.keys()][i] ?? null, get length() { return saved.size; } } });
    w.Date.now = () => clock; w.S = { cloud: { ok: true } }; w.ListZoom = { clean() {}, detach() {}, bind() {} }; w.cors = x => x; w.esc = x => String(x);
    w.api = async (_, body) => { calls.push(body); return reply(body); }; w.Image = class { set src(v) { queueMicrotask(() => this.onload?.()); } };
    w.eval(source.slice(source.indexOf('const ListMedia ='), source.indexOf('const fmtT =')) + ';this.media=ListMedia;');
    return { w, dom, calls, writes, saved, now: () => clock, advance: ms => { clock += ms; }, setReply: fn => { reply = fn; }, rows: ids => ids.map(listingId => ({ line: { listingId } })) };
  };
  const status = (p, id) => { const host = p.w.document.createElement('span'); host.setAttribute('data-listing', ''); p.w.document.body.append(host); p.w.media.watch(host, () => p.w.media.listing(id), id); return new Promise(r => setTimeout(() => r(host.textContent), 20)); };
  const saved = new Map([['cn.listingPhotoPause.v2', String(Date.now() + DAY)], ['cn.listingPhotoPause', String(Date.now() + DAY)], ['cn.listingPhotoPause.v1', String(Date.now() + DAY)], ['cn.settings', '{"a":1}']]);
  const p = page(saved);
  check(![...saved.keys()].some(k => k.startsWith('cn.listingPhotoPause')) && saved.get('cn.settings') === '{"a":1}', 'G on load the old pause keys (v2 and every older one) are removed; other keys stay', [...saved.keys()]);
  await p.w.media.prepare(p.rows(['1001']));
  check(p.calls.length === 1 && p.calls[0].prepare === true, 'G a pause an older page stored is never read: the first lookup asks the cloud live', p.calls);
  p.setReply(async b => ({ images: {}, states: Object.fromEntries(b.listingIds.map(id => [id, 'budget-paused'])), retryAt: p.now() + DAY }));
  await p.w.media.prepare(p.rows(['1002'])); const shown = await status(p, '1002');
  check(shown.startsWith('Photo lookup paused') && !p.writes.some(k => /Pause/i.test(k)) && ![...saved.keys()].some(k => /Pause/i.test(k)), 'G the cloud says paused: the card says so, and nothing about it is kept in the browser', { shown, writes: p.writes });
  p.setReply(async b => ({ images: {}, states: Object.fromEntries(b.listingIds.map(id => [id, 'sandbox-cache-only'])), retryAt: 0 }));   // (the owner pressed Resume Etsy: the cloud's next answer says no pause)
  await p.w.media.prepare(p.rows(['1003'])); const after = await status(p, '1003');
  check(p.calls.at(-2).prepare === false && p.calls.at(-1).prepare === true && p.calls.at(-1).listingIds.join() === '1003' && !after.startsWith('Photo lookup paused'), 'G the cloud\'s next answer says no pause: the pause is gone at once (never the longest one seen) and the photo still missing is looked up live right away', { calls: p.calls.slice(-2), after });
  p.setReply(async b => ({ images: {}, states: Object.fromEntries(b.listingIds.map(id => [id, 'budget-paused'])), retryAt: p.now() + DAY })); await p.w.media.prepare(p.rows(['1005']));
  p.setReply(async b => ({ images: {}, states: Object.fromEntries(b.listingIds.map(id => [id, 'budget-paused'])), retryAt: p.now() + 3600000 })); await p.w.media.prepare(p.rows(['1006']));
  p.advance(3600001); p.setReply(async b => ({ images: Object.fromEntries(b.listingIds.map(id => [id, 'https://i.etsystatic.com/x.jpg'])), retryAt: 0 })); await p.w.media.prepare(p.rows(['1007']));
  check(p.calls.at(-1).prepare === true && p.w.media.peek('1007'), 'G a shorter pause in the latest answer replaces a longer one: an hour later the page asks live again', p.calls.slice(-3));
  p.dom.window.close();
  const fresh = page(new Map(saved)); await fresh.w.media.prepare(fresh.rows(['2001']));
  check(fresh.calls[0].prepare === true, 'G after a refresh the page holds no pause of its own: the cloud decides', fresh.calls); fresh.dom.window.close();

  const failed = results.filter(x => !x).length;
  console.log(failed ? `FAILED: ${failed} of ${results.length}` : `photo-pause-resume OK · ${results.length} checks · fake Etsy only, no network`);
  process.exit(failed ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
