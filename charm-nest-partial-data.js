/* Partial sheets, the page's data layer (PS3, Paul 7 Oct 2026: "each sheet needs its own individual Partial Sheet repo in the Nest tab").
   `window.PartialSheets` is what the Partial Sheet panel (PS1) and the nester (PS2) call; it adds the signed-in person's name to every write itself
   (window.CNEmployee.name() || window.B.employee: nobody types a name) and caches per page. The contract with every field: plans/partial-sheets/contract.md.

     await PartialSheets.list(metal, { inUse, used, force })  -> { items:[card], rev, at, policies, typical }   (ONE op, charmNestLibrary partialList)
     PartialSheets.cached(metal)   PartialSheets.changed()   PartialSheets.on(fn) -> off
     PartialSheets.policy(metal) -> { mode:'auto'|'new', wMm, hMm }  (sync, from the page's cache; also window.partialPolicy)
     await PartialSheets.loadPolicy({ force })   await PartialSheets.setPolicy(metal, { mode, wMm, hMm })
     await PartialSheets.stocks([partialId...]) -> { [id]: { stockId, revision, wPt, hPt, profileJson } }   (the stock under a partial, read once per id, for a trial pack)
     await PartialSheets.claim(metal, partialId, sheetId, { sheetName, nesting, swap })   await PartialSheets.release(sheetId)   await PartialSheets.use(partialId, sheetId)
     await PartialSheets.plan(metal, pieces, { order })  -> { fitsAll, needed, partials, needMm2, haveMm2, short, estimate:true }   (uses the cached list: no call when the panel has it)

     await PartialSheets.history({ stockId, revision? } | { sheetId }, { force })  -> { ok, stock, cuts:[{ n, revision, at, by, sheetId, sheetName, setName, via, rings, areaMm2, bboxMm, exact }], rev }   (ONE op, sheetHistory:
         every cut of one physical sheet, oldest first; cached by stockId + revision: one call per sheet shown)
     await PartialSheets.searchAll({ force, more, limit? }) -> { items:[card], rev, at, more }   (ONE op, partialSearchList: every partial, every status and metal, newest cut first; the modal filters it in the browser)
     await PartialSheets.make({ metal, wMm, hMm }) -> { ok, item: card }   (ONE op, sheetMake: a blank sheet of the person's size, 5 to 500 mm each side, any number of them; a card of kind 'new')
     await PartialSheets.remove(id, reason)        -> { ok, item: card }   (ONE op, sheetDelete: soft delete of an available sheet nobody holds; the reason, 3 to 300 characters, is kept with who and when)
     (make / remove send the signed-in person themselves, like every write here, and mark the lists changed: on(fn) hears { metal:'*', reason:'changed' }; no polling)
     (history / searchAll announce themselves with on(fn) as { reason: 'history' | 'search' } and no metal: the Partial Sheet panel ignores them)

   Cost (the Google bill): NO timer, NO polling. A list is asked when the panel opens, after something this page changed (changed()), on a Refresh press
   (force: reads in full, the safety net) and when the tab is looked at again after a minute. Every ask sends the revision of the last answer (ifRev): nothing new is
   answered from ONE tiny document read on the server and no card is read. An answer less than 20 s old is reused with NO call at all. The setting is read once per
   page (and rides on every list answer). */
(function init() {
  'use strict';
  if (!window.CN || !window.CharmNestPartial) { setTimeout(init, 150); return; }
  const C = window.CN, P = window.CharmNestPartial, METALS = ['rose', 'gold10k', 'gold14k'], FRESH_MS = 20000, STALE_MS = 60000;
  const DEFAULT_POLICY = { mode: 'auto', wMm: 100, hMm: 50 }, SIZE_MM = [5, 500];
  const api = (body, label, quiet) => C.api('charmNestLibrary', body, { label: label || 'Partial sheets', quiet: !!quiet });
  const need = metal => { if (!METALS.includes(metal)) throw new Error('Choose Rose Gold, 10K Gold or 14K Gold'); return metal; };
  // the signed-in person (the sorter's sign-in, or the Design Station's); nobody signed in is sent as "" and kept as none
  const who = () => { try { return String(window.CNEmployee?.name?.() || window.B?.employee || '').trim(); } catch (_) { return String(window.B?.employee || '').trim(); } };

  const lists = {}, inflight = {}, listeners = new Set(), policies = {}; let policyLoaded = false, policyFlight = null, backfillTried = false;
  const emit = info => { for (const fn of [...listeners]) { try { fn(info || {}); } catch (_) {} } };
  const setPolicies = p => { if (!p) return; for (const m of METALS) if (p[m]) policies[m] = { mode: p[m].mode === 'new' ? 'new' : 'auto', wMm: +p[m].wMm || DEFAULT_POLICY.wMm, hMm: +p[m].hMm || DEFAULT_POLICY.hMm, by: p[m].by || '', at: p[m].at || null }; policyLoaded = true; };

  const key = o => (o.inUse ? '1' : '0') + (o.used ? '1' : '0');
  async function list(metal, o = {}) {
    need(metal);
    const k = key(o), cur = lists[metal], same = cur && cur.key === k;
    if (!o.force && same && Date.now() - cur.at < FRESH_MS) return cur;
    const f = metal + k + (o.force ? 'f' : '');
    if (inflight[f]) return inflight[f];
    inflight[f] = (async () => {
      try {
        const body = { op: 'partialList', metal, ...(o.inUse ? { inUse: true } : {}), ...(o.used ? { used: true } : {}), ...(same && !o.force ? { ifRev: cur.rev } : {}), ...(o.force ? { verify: true } : {}), ...(backfillTried ? { backfill: false } : {}) };
        const r = await api(body, 'Reading partial sheets');
        backfillTried = true;
        if (r.unchanged && same) { cur.at = Date.now(); return cur; }
        if (r.unchanged) throw new Error('Partial sheets: nothing to show yet');   // (cannot happen: ifRev is only sent with a cached answer of the same kind)
        setPolicies(r.policies);
        const out = { metal, key: k, items: r.items || [], rev: r.rev, at: Date.now(), policies: r.policies, typical: r.typical, more: !!r.more, ...(r.backfilled ? { backfilled: r.backfilled } : {}) };
        lists[metal] = out; emit({ metal, reason: 'list' });
        return out;
      } finally { delete inflight[f]; }
    })();
    return inflight[f];
  }
  const cached = metal => lists[metal] || null;
  // something this page changed (a cut, a claim, a release, the setting): the next list asks again (with its revision: one tiny read when nothing else moved)
  function changed() { for (const m of Object.keys(lists)) lists[m].at = 0; if (all) all.at = 0; for (const h of hist.values()) h.at = 0; emit({ metal: '*', reason: 'changed' }); }
  function on(fn) { listeners.add(fn); return () => listeners.delete(fn); }

  const policy = metal => { const p = policies[metal]; return p ? { mode: p.mode, wMm: p.wMm, hMm: p.hMm } : { ...DEFAULT_POLICY }; };
  async function loadPolicy(o = {}) {
    if (policyLoaded && !o.force) return { ...policies };
    if (policyFlight) return policyFlight;
    policyFlight = api({ op: 'partialPolicyGet' }, 'Reading the partial sheet setting').then(r => { setPolicies(r.policies); return { ...policies }; }).finally(() => { policyFlight = null; });
    return policyFlight;
  }
  async function setPolicy(metal, p = {}) {
    need(metal);
    const r = await api({ op: 'partialPolicySet', metal, mode: p.mode, ...(p.wMm != null ? { wMm: p.wMm } : {}), ...(p.hMm != null ? { hMm: p.hMm } : {}), by: who() }, 'Saving the partial sheet setting');
    setPolicies(r.policies); changed(); emit({ metal, reason: 'policy' });
    return r.policy;
  }

  // swap: the sheet gives back the physical sheet it holds and takes this one in ONE server transaction (a refused claim then loses nothing)
  async function claim(metal, id, sheetId, o = {}) {
    need(metal);
    const r = await api({ op: 'partialClaim', metal, id, sheetId, by: who(), ...(o.sheetName ? { sheetName: o.sheetName } : {}), ...(o.nesting === false ? { nesting: false } : {}), ...(o.swap ? { swap: true } : {}) }, 'Reserving the partial sheet');
    changed(); return r;
  }
  /* the physical sheets under some partials (for a trial pack: the solver packs against the stock's profile). Only the ids not read yet are asked; a revision's profile never changes. */
  const stockCache = new Map();
  async function stocks(ids) {
    const want = [...new Set((ids || []).map(String))], miss = want.filter(id => !stockCache.has(id));
    if (miss.length) { const r = await api({ op: 'partialStocks', ids: miss.slice(0, 20) }, 'Reading the partial sheets'); for (const [id, v] of Object.entries(r.stocks || {})) if (v && !v.missing && v.current) stockCache.set(id, v); }
    const out = {}; for (const id of want) if (stockCache.has(id)) out[id] = stockCache.get(id);
    return out;
  }
  async function release(sheetId) { const r = await api({ op: 'partialRelease', sheetId, by: who() }, 'Giving the partial sheet back'); changed(); return r; }
  async function use(id, sheetId, o = {}) { const r = await api({ op: 'partialUse', id, sheetId, by: who(), ...(o.sheetName ? { sheetName: o.sheetName } : {}) }, 'Marking the partial sheet used'); changed(); return r; }

  async function plan(metal, pieces, o = {}) {
    need(metal);
    const l = await list(metal), cards = (l.items || []).filter(c => c.status === 'available');
    return { ...P.planFor(cards, pieces, { order: o.order, typical: l.typical }), metal };
  }

  /* The history of one physical sheet (OptionsHistory draws it). Cached by stockId + revision: a revision's history never changes, so a caller that knows the revision (a card, the
     sheet's roseRevision) is answered from the cache with NO call when the cache is as new; one that does not ({ sheetId }) reuses the answer for 20 s and after something this page
     changed (changed()) asks again. One call per sheet shown. */
  const hist = new Map(), histStock = new Map(), histFlight = {};
  async function history(arg, o = {}) {
    const a = arg || {}, sheetId = a.sheetId ? String(a.sheetId) : '', stockId = String(a.stockId || (sheetId && histStock.get(sheetId)) || '');
    if (!stockId && !sheetId) throw new Error('Choose a sheet');
    const want = a.revision != null && a.revision !== '' && Number.isFinite(+a.revision) ? +a.revision : null, cur = stockId ? hist.get(stockId) : null;
    if (cur && !o.force && (want !== null ? cur.revision >= want : Date.now() - cur.at < FRESH_MS)) return cur.data;
    const f = stockId || 'sheet:' + sheetId;
    if (histFlight[f]) return histFlight[f];
    histFlight[f] = (async () => {
      try {
        const r = await api(stockId ? { op: 'sheetHistory', stockId } : { op: 'sheetHistory', sheetId }, 'Reading the sheet history');
        if (!r || !r.stock || !r.stock.id) throw new Error('Sheet history: nothing to show');
        hist.delete(r.stock.id); hist.set(r.stock.id, { revision: +r.stock.revision || 0, at: Date.now(), data: r });
        if (hist.size > 40) hist.delete(hist.keys().next().value);
        if (sheetId) histStock.set(sheetId, r.stock.id);
        emit({ reason: 'history', stockId: r.stock.id });
        return r;
      } finally { delete histFlight[f]; }
    })();
    return histFlight[f];
  }

  /* Every partial, all statuses and metals, for the modal's search: ONE list (the search itself runs in the browser, never a read per keystroke). Asked when the modal opens, after something
     this page changed (the next ask sends the revision: one tiny read when nothing moved), on Refresh (force: in full) and { more: true } for the older ones when `more`. No timer. */
  let all = null; const allFlight = {};
  async function searchAll(o = {}) {
    const cur = all;
    if (!o.force && !o.more && cur && Date.now() - cur.at < FRESH_MS) return cur;
    const f = o.more ? 'more' : o.force ? 'force' : 'list';
    if (allFlight[f]) return allFlight[f];
    allFlight[f] = (async () => {
      try {
        if (o.more) {
          const last = cur && cur.more ? cur.items[cur.items.length - 1] : null; if (!last) return cur;
          const r = await api({ op: 'partialSearchList', before: last.cutAt }, 'Reading older partial sheets');
          if (all !== cur) return all;   // (the list was read again meanwhile)
          const seen = new Set(cur.items.map(c => c.id));
          all = { ...cur, items: [...cur.items, ...(r.items || []).filter(c => !seen.has(c.id))], more: !!r.more, at: Date.now() };
          emit({ reason: 'search' }); return all;
        }
        const r = await api({ op: 'partialSearchList', ...(o.limit ? { limit: o.limit } : {}), ...(cur && !o.force && cur.rev ? { ifRev: cur.rev } : {}), ...(o.force ? { verify: true } : {}) }, 'Reading all partial sheets');
        if (r.unchanged && cur) { cur.at = Date.now(); return cur; }
        if (r.unchanged) throw new Error('Partial sheets: nothing to show yet');   // (cannot happen: ifRev is only sent with a cached answer)
        all = { items: r.items || [], rev: r.rev, at: Date.now(), more: !!r.more };
        emit({ reason: 'search' }); return all;
      } finally { delete allFlight[f]; }
    })();
    return allFlight[f];
  }

  // the tab is looked at again after a minute: the panel (if open) asks again, with its revision
  try { document.addEventListener('visibilitychange', () => { if (document.visibilityState !== 'visible') return; const t = Object.values(lists); if (t.length && t.some(l => Date.now() - l.at > STALE_MS)) { changed(); emit({ metal: '*', reason: 'visible' }); } }); } catch (_) {}

  /* A person makes a blank sheet (any size, as many as they like) or deletes an available one (a soft delete that stays in the sheet's history). Both are ONE server transaction and
     mark every cached list changed (the next list sends its revision: one tiny read when nothing else moved). The sheet's cached history is dropped: it now says made / deleted. */
  async function make(o = {}) {
    need(o.metal);
    const wMm = +o.wMm, hMm = +o.hMm;
    if (![wMm, hMm].every(n => Number.isFinite(n) && n >= SIZE_MM[0] && n <= SIZE_MM[1])) throw new Error(`The new sheet's width and height can each be ${SIZE_MM[0]} to ${SIZE_MM[1]} mm`);
    const r = await api({ op: 'sheetMake', metal: o.metal, wMm, hMm, by: who() }, 'Making a new sheet');
    if (r && r.item && r.item.stockId) hist.delete(r.item.stockId);
    changed(); return r;
  }
  async function remove(id, reason) {
    const why = String(reason == null ? '' : reason).trim();
    if (why.length < 3 || why.length > 300) throw new Error('Say why you are deleting this sheet (3 to 300 characters)');
    const r = await api({ op: 'sheetDelete', id: String(id || ''), reason: why, by: who() }, 'Deleting the sheet');
    if (r && r.item && r.item.stockId) { hist.delete(r.item.stockId); for (const k of [...stockCache.keys()]) if (k.startsWith(r.item.stockId + '-')) stockCache.delete(k); }
    changed(); return r;
  }

  window.PartialSheets = { list, cached, changed, on, policy, loadPolicy, setPolicy, claim, release, use, plan, stocks, history, searchAll, make, remove, estimateFit: P.estimateFit, METALS, DEFAULT_POLICY };
  window.partialPolicy = metal => window.PartialSheets.policy(metal);
})();
