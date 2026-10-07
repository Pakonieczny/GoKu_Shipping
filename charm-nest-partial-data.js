/* Partial sheets, the page's data layer (PS3, Paul 7 Oct 2026: "each sheet needs its own individual Partial Sheet repo in the Nest tab").
   `window.PartialSheets` is what the Partial Sheet panel (PS1) and the nester (PS2) call; it adds the signed-in person's name to every write itself
   (window.CNEmployee.name() || window.B.employee: nobody types a name) and caches per page. The contract with every field: plans/partial-sheets/contract.md.

     await PartialSheets.list(metal, { inUse, used, force })  -> { items:[card], rev, at, policies, typical }   (ONE op, charmNestLibrary partialList)
     PartialSheets.cached(metal)   PartialSheets.changed()   PartialSheets.on(fn) -> off
     PartialSheets.policy(metal) -> { mode:'auto'|'new', wMm, hMm }  (sync, from the page's cache; also window.partialPolicy)
     await PartialSheets.loadPolicy({ force })   await PartialSheets.setPolicy(metal, { mode, wMm, hMm })
     await PartialSheets.claim(metal, partialId, sheetId, { sheetName, nesting })   await PartialSheets.release(sheetId)   await PartialSheets.use(partialId, sheetId)
     await PartialSheets.plan(metal, pieces, { order })  -> { fitsAll, needed, partials, needMm2, haveMm2, short, estimate:true }   (uses the cached list: no call when the panel has it)

   Cost (the Google bill): NO timer, NO polling. A list is asked when the panel opens, after something this page changed (changed()), on a Refresh press
   (force: reads in full, the safety net) and when the tab is looked at again after a minute. Every ask sends the revision of the last answer (ifRev): nothing new is
   answered from ONE tiny document read on the server and no card is read. An answer less than 20 s old is reused with NO call at all. The setting is read once per
   page (and rides on every list answer). */
(function init() {
  'use strict';
  if (!window.CN || !window.CharmNestPartial) { setTimeout(init, 150); return; }
  const C = window.CN, P = window.CharmNestPartial, METALS = ['rose', 'gold10k', 'gold14k'], FRESH_MS = 20000, STALE_MS = 60000;
  const DEFAULT_POLICY = { mode: 'auto', wMm: 100, hMm: 50 };
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
  function changed() { for (const m of Object.keys(lists)) lists[m].at = 0; emit({ metal: '*', reason: 'changed' }); }
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

  async function claim(metal, id, sheetId, o = {}) {
    need(metal);
    const r = await api({ op: 'partialClaim', metal, id, sheetId, by: who(), ...(o.sheetName ? { sheetName: o.sheetName } : {}), ...(o.nesting === false ? { nesting: false } : {}) }, 'Reserving the partial sheet');
    changed(); return r;
  }
  async function release(sheetId) { const r = await api({ op: 'partialRelease', sheetId, by: who() }, 'Giving the partial sheet back'); changed(); return r; }
  async function use(id, sheetId, o = {}) { const r = await api({ op: 'partialUse', id, sheetId, by: who(), ...(o.sheetName ? { sheetName: o.sheetName } : {}) }, 'Marking the partial sheet used'); changed(); return r; }

  async function plan(metal, pieces, o = {}) {
    need(metal);
    const l = await list(metal), cards = (l.items || []).filter(c => c.status === 'available');
    return { ...P.planFor(cards, pieces, { order: o.order, typical: l.typical }), metal };
  }

  // the tab is looked at again after a minute: the panel (if open) asks again, with its revision
  try { document.addEventListener('visibilitychange', () => { if (document.visibilityState !== 'visible') return; const t = Object.values(lists); if (t.length && t.some(l => Date.now() - l.at > STALE_MS)) { changed(); emit({ metal: '*', reason: 'visible' }); } }); } catch (_) {}

  window.PartialSheets = { list, cached, changed, on, policy, loadPolicy, setPolicy, claim, release, use, plan, estimateFit: P.estimateFit, METALS, DEFAULT_POLICY };
  window.partialPolicy = metal => window.PartialSheets.policy(metal);
})();
