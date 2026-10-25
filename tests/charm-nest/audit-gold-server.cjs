// AUDIT (gold family, server parity): Rose Gold / 10K / 14K sheets and the server's own write paths (charmNestLibrary putSheet, flowApply setMember, approveGate)
// judged against the owner's rules (docs/charm-nest/set-rules.md R4, R12, C3, C7) and against what the page refuses.
// Fixtures only (made-up ids and orders); the real charmNestLibrary handler over the in-memory Firestore of bridge-server.cjs. No network beyond localhost.
// Each scenario prints OK or BREAK; the file exits 1 when any BREAK is found (a BREAK is a reproduced rule break, not a suspicion).
'use strict';
const assert = require('node:assert/strict');
const I = require('../../charm-nest-set-invariants.js'), LF = require('../../charm-nest-flow.js');

const breaks = [];
const verdict = (name, ok, detail) => { console.log(`${ok ? 'OK   ' : 'BREAK'} ${name}${ok ? '' : ' :: ' + detail}`); if (!ok) breaks.push(name); };

(async () => {
  const { start } = require('./bridge-server.cjs');
  const SH = 'Charm_Nest_Sheets', SET = 'Charm_Nest_Sets', RUNS = 'Charm_Nest_Runs';
  const srv = await start({ receipts: [] }), { st } = srv, now = Date.now(), ts = { toMillis: () => now }, day = '2026-10-25';
  const post = async body => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }); return { status: r.status, ...await r.json() }; };
  const lines = {}; let n = 0;
  const mk = (id, o = {}) => {
    const order = String(5100000000 + ++n), pool = [order + '_1_1'], { link: lk, metal, ...rest } = o, orders = [order];
    lines[order + '_1'] = { orderId: order, state: 'written', quantity: 1, poolIds: pool.slice(), engraveCandidate: false };
    if (lk) { const [lo, copy] = lk; pool.push(`${lo}_1_${copy}`); orders.push(lo); lines[lo + '_1'] = lines[lo + '_1'] || { orderId: lo, state: 'written', quantity: 2, poolIds: [], engraveCandidate: false }; lines[lo + '_1'].poolIds.push(`${lo}_1_${copy}`); }
    st.put(SH, id, { id, runId: 'run-g', metal: metal || 'gold', day, status: 'complete', placedCount: pool.length, charmCount: pool.length, density: .7, poolIds: pool, orders, verification: { ok: true }, releaseFull: true, page: n, sheetIndex: 1, updatedAt: ts, createdAt: ts, ...rest });
  };
  const setDoc = (id, seq, sheetIds, o = {}) => st.put(SET, id, { setId: id, seq, day, runId: 'run-g', group: 'dispatch', sheetIds, materials: [], orders: {}, status: 'open', updatedAt: ts, createdAt: ts, ...o });
  // the state of the cloud as the checker reads it (every sheet, every set)
  const cloudCheck = (opts) => {
    const sheets = st.list(SH).map(d => ({ ...d, id: d._id }));
    const sets = st.list(SET).map(d => ({ ...d, id: d._id }));
    return I.check(sets, sheets, Object.assign({ membership: false, unformed: false }, opts || {}));
  };
  const rulesOf = (res, setId) => res.violations.filter(v => v.set === setId).map(v => v.ruleNo);

  // Set 30: a formed, uncommitted set (GF + SS, tied by an order). Set 31: a committed set. Set 32: a set record with no sheet yet (allocated, nothing in it).
  setDoc('set-30', 30, ['g-gf', 'g-ss']); setDoc('set-31', 31, ['c-gf', 'c-ss'], { status: 'complete', committedAt: now - 9000 }); setDoc('set-32', 32, []); setDoc('set-33', 33, ['d-gf', 'd-ss'], { status: 'complete', committedAt: now - 9000, runId: 'run-old' });
  mk('g-gf', { setId: 'set-30', setSeq: 30, link: ['5199000001', 1] }); mk('g-ss', { setId: 'set-30', setSeq: 30, metal: 'silver', link: ['5199000001', 2] });
  mk('c-gf', { setId: 'set-31', setSeq: 31, link: ['5199000002', 1] }); mk('c-ss', { setId: 'set-31', setSeq: 31, metal: 'silver', link: ['5199000002', 2] });
  mk('d-gf', { setId: 'set-33', setSeq: 33, runId: 'run-old', link: ['5199000004', 1] }); mk('d-ss', { setId: 'set-33', setSeq: 33, runId: 'run-old', metal: 'silver', link: ['5199000004', 2] });
  // Rose Gold sheets in no set, NOT cut (no roseCutAt), no tick (no solidIncluded), no yes (no cutApproved): R12 (a) is not met for any of them
  mk('rg-a', { metal: 'rose', draft: true, setId: null, sheetIndex: null, releaseFull: false, density: .4, link: ['5199000003', 2] });
  st.put(SH, 'g-gf', { poolIds: st.doc(SH, 'g-gf').poolIds.concat(['5199000003_1_1']), orders: st.doc(SH, 'g-gf').orders.concat(['5199000003']) });      // rg-a shares an order with GF of Set 30
  mk('rg-b', { metal: 'rose', draft: true, setId: null, sheetIndex: null, releaseFull: false, density: .4 });
  mk('rg-c', { metal: 'rose', draft: true, setId: null, sheetIndex: null, releaseFull: false, density: .4 });
  mk('rg-d', { metal: 'rose', draft: true, setId: null, sheetIndex: null, releaseFull: false, density: .4, rosePlanHash: 'h-1', rosePlanJson: '{}', runId: 'run-old' });   // a calculated line, NOT cut (its run is finished: the Library edits it from the records)
  st.put(RUNS, 'run-old', { runId: 'run-old', status: 'complete', lines });
  st.put(RUNS, 'run-g', { runId: 'run-g', status: 'running', lines });
  try {
    // ── S1 · R12 (a) at putSheet: an uncut, unticked Rose Gold sheet saved INTO the formed open Set 30 ──
    // The page never sends this (a Rose Gold sheet joins only by its own Cut Sheet press, which records the cut). The server is the last line: does it refuse it?
    {
      const r = await post({ op: 'putSheet', sheet: { id: 'rg-a', draft: false, setId: 'set-30', setSeq: 30, sheetIndex: 1 } });
      // (the page's set save lists the sheet in its set record too: judge the cloud as it then stands)
      const was = st.doc(SET, 'set-30').sheetIds; st.put(SET, 'set-30', { sheetIds: was.concat(st.doc(SH, 'rg-a').setId === 'set-30' ? ['rg-a'] : []) });
      const res = cloudCheck({}), rs = rulesOf(res, 'set-30'); st.put(SET, 'set-30', { sheetIds: was });
      verdict('S1 putSheet: an uncut, unticked RG sheet into a formed open set is refused (R12 a)', r.status === 409,
        `server answered ${r.status}; the sheet record now says setId=${st.doc(SH, 'rg-a').setId}; check() on the cloud: [${rs.join(', ')}] ${I.explain(res).filter(l => /RG|Rose/.test(l)).slice(0, 1).join('')}`);
      st.put(SH, 'rg-a', { draft: true, setId: null, setSeq: null, sheetIndex: null });      // (put back for the next scenario)
    }
    // ── S2 · R12 (a) at flowApply setMember: the same sheet into the COMMITTED Set 31 (SetEdit.verifyMoves has no green-line rule; the page's plan adds a cutSheet step AFTER this one) ──
    {
      const r = await post({ op: 'flowApply', by: 'Audit', steps: [{ type: 'setMember', moves: [{ sheetId: 'rg-b', to: 'set-31' }] }] });
      const d = st.doc(SH, 'rg-b'), res = cloudCheck({}), rs = rulesOf(res, 'set-31');
      verdict('S2 flowApply setMember: an uncut RG sheet with no yes into a committed set is refused (R12 a)', r.status === 409,
        `server answered ${r.status}; rg-b now setId=${d.setId} roseCutAt=${d.roseCutAt || null} cutApproved=${d.cutApproved || null} solidIncluded=${d.solidIncluded}; check() (committed sets judged): [${rs.join(', ')}]`);
    }
    // ── S3 · R4 at putSheet: a Rose Gold sheet saved into a set record that holds NO sheet: it would START a set ──
    {
      const r = await post({ op: 'putSheet', sheet: { id: 'rg-c', draft: false, setId: 'set-32', setSeq: 32, sheetIndex: 1 } });
      const d = st.doc(SH, 'rg-c'), res = cloudCheck({}), rs = rulesOf(res, 'set-32');
      verdict('S3 putSheet: a RG sheet never starts a set (R4)', r.status === 409 || d.setId !== 'set-32',
        `server answered ${r.status}; rg-c is now the only sheet of set-32 (setId=${d.setId}); check(): [${rs.join(', ')}]`);
      // and the approval gate for that one-sheet set: approveGate skips every set of fewer than 2 sheets (setGatesFor), so the owner's rules are not read for it at all
      st.put(SET, 'set-32', { sheetIds: ['rg-c'] });
      const a = await post({ op: 'flowApply', by: 'Audit', steps: [{ type: 'seal', kind: 'set', id: 'set-32' }] });
      const page = require('../../charm-nest-readiness.js').setGate({ setId: 'set-32', seq: 32, sheetIds: ['rg-c'] }, [{ ...st.doc(SH, 'rg-c'), id: 'rg-c' }]);
      const sd = st.doc(SET, 'set-32'), approved = sd.processReady === true || (Array.isArray(sd.processSeals) && sd.processSeals.length > 0);
      verdict('S3b approval: a set holding only a RG sheet (R1/R4 broken) is never approved', !approved,
        `processReady=${sd.processReady} seals=${(sd.processSeals || []).length}`);
      // (note, not a break by itself: approveGate lets the seal step through without reading the rules for a set of fewer than 2 sheets; the seal step's own readiness grants nothing here)
      console.log(`      note: approveGate answered ${a.status} for the one-sheet set (the rules were not read); the page's setGate says ready=${page.ready}`);
    }
    // ── S4 · R12 (a) in the Library plan itself: a Rose Gold sheet whose green line is CALCULATED but not cut (rosePlanHash, no roseCutAt) dropped on the committed Set 31 ──
    // owesLine() is false for it (it has a saved line), so the plan asks no "Cut Sheet" yes and adds no cutSheet step: it joins with neither a cut nor a yes.
    {
      LF.configure({ api: body => post(body).then(x => { if (x.status >= 400) throw new Error(x.error); return x; }), live: () => null, include: async () => {}, employee: () => 'Audit', rows: () => [],
        relabelSet: async () => {}, setFiles: async () => {}, applyMembership: async () => {} });
      const p = await LF.plan({ kind: 'sheet', id: 'rg-d', to: { set: 'set-33' } });
      const types = p.steps.map(s => s.type), asks = p.confirm.map(c => c.key);
      const cutsIt = types.includes('cutSheet') || types.includes('roseJoin');
      let after = '';
      if (p.ok && !cutsIt) { const c = await LF.commit(p, { by: 'Audit', confirmed: asks }); const d = st.doc(SH, 'rg-d'), res = cloudCheck({}); after = `; commit ok=${c.ok}${c.error ? ' (' + c.error + ')' : ''}; rg-d setId=${d.setId} roseCutAt=${d.roseCutAt || null} solidIncluded=${d.solidIncluded}; check() of Set 33: [${rulesOf(res, 'set-33').join(', ')}]`; }
      if (!p.ok) console.log('      S4 plan refused: ' + JSON.stringify(p.needs.map(x => [x.key, x.label])));
      verdict('S4 Library plan: an RG sheet with a calculated but uncut line joins a committed set only with its cut or its Cut Sheet yes (R12 a)', !p.ok || cutsIt,
        `plan ok=${p.ok} steps=[${types.join(', ')}] confirms=[${asks.join(', ')}]${after}`);
    }
    // ── S5 · R10: a formed, right set whose GF shares an order with a Rose Gold sheet OUTSIDE every set that cannot join yet (no cut) is a set with a partner to add:
    //    "That set cannot be approved for laser cutting until the partner is in it" (set-rules.md R10; check() says canApprove false). The page reads it from set.cardinalWait (derived on the page,
    //    never saved). Does the server's approveGate refuse it? Sheets made ready to approve as in tests/charm-nest/set-repair.cjs part 8 (label made, nothing to engrave).
    {
      const image = 'data:image/svg+xml,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="30" height="20"><rect width="30" height="20"/></svg>');
      const ready = id => ({ stock: { wIn: 6, hIn: 4.5 }, outputs: { ai: { path: id + '.ai', url: srv.sorterOrigin + '/' + id + '.ai' }, preview: { path: id + '.png', url: image } } });
      const labelled = id => { const d = st.doc(SH, id); st.put(SH, id, { ...ready(id), label: { files: [{ path: id + '-qr.png', url: image, payload: d.orders[0], orders: d.orders }], orders: d.orders } }); };
      setDoc('set-40', 40, ['p-gf', 'p-ss'], { status: 'labelled', materials: ['gold', 'silver'] });
      mk('p-gf', { setId: 'set-40', setSeq: 40, link: ['5199000040', 1] }); mk('p-ss', { setId: 'set-40', setSeq: 40, metal: 'silver', link: ['5199000040', 2] });
      { ['p-gf', 'p-ss'].forEach(labelled); st.put(RUNS, 'run-g', { runId: 'run-g', status: 'running', lines });
        const a0 = await post({ op: 'flowApply', by: 'Audit', steps: [{ type: 'seal', kind: 'set', id: 'set-40' }] });
        console.log('      S5 control (no partner): server ' + a0.status + ' processReady=' + st.doc(SET, 'set-40').processReady + ' ' + (a0.error || '').slice(0, 200)); }
      for (const [variant, extra] of [['uncut, no line', {}], ['uncut, line calculated', { rosePlanHash: 'h-40', rosePlanJson: '{}' }]]) {
        const rgId = 'p-rg-' + (extra.rosePlanHash ? 'lined' : 'bare');
        const lo = String(5199000041 + (extra.rosePlanHash ? 1 : 0));
        mk(rgId, { metal: 'rose', draft: true, setId: null, sheetIndex: null, releaseFull: false, density: .4, link: [lo, 2], ...extra });
        st.put(SH, 'p-gf', { poolIds: st.doc(SH, 'p-gf').poolIds.concat([lo + '_1_1']), orders: st.doc(SH, 'p-gf').orders.concat([lo]) });
        ['p-gf', 'p-ss', rgId].forEach(labelled);
        st.put(RUNS, 'run-g', { runId: 'run-g', status: 'running', lines });
        st.put(SET, 'set-40', { processReady: false, processSeals: [] }); for (const id of ['p-gf', 'p-ss']) st.put(SH, id, { processReady: false, processSeals: [] });
        const res = cloudCheck({}), row = res.sets.find(x => x.id === 'set-40'), partner = res.partners.find(x => x.set === 'set-40');
        assert(partner && row.canApprove === false, 'the checker names the Rose Gold partner and says the set cannot be approved: ' + JSON.stringify(res.partners));
        const a = await post({ op: 'flowApply', by: 'Audit', steps: [{ type: 'seal', kind: 'set', id: 'set-40' }] });
        const sd = st.doc(SET, 'set-40'); console.log('      S5 server:', a.status, (a.error || '').slice(0, 220));
        verdict(`S5 approveGate (${variant} partner): a set with a Rose Gold partner to add is not approved (R10)`, sd.processReady !== true,
          `server answered ${a.status}; set-40 processReady=${sd.processReady}; check(): partner ${partner.name} (${partner.reasons.join('; ')}), canApprove=${row.canApprove}`);
        // (the next variant starts from a set without this partner's order)
        st.put(SH, 'p-gf', { poolIds: st.doc(SH, 'p-gf').poolIds.filter(k => !k.startsWith(lo)), orders: st.doc(SH, 'p-gf').orders.filter(o => o !== lo) }); st.put(SH, rgId, { archived: true });
      }
    }
  } finally { srv.close(); }
  console.log(breaks.length ? `\n${breaks.length} BREAK(S): ${breaks.join(' | ')}` : '\nPASS: audit-gold-server: no break found');
  process.exit(breaks.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(2); });
