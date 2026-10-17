// SETFIX (Paul, 17 Oct 2026: "fix the below"): the two caveats the set-rule work left open.
//   A  "Set approval: a set holding an uncut Rose Gold, 10K or 14K sheet can no longer be approved until that sheet is cut or the set is rebuilt. Existing sets that break your rule will
//      be blocked at approval." -> a blocked set is never a dead end: it says exactly what to press, and that press is offered where it is blocked (the set's card and the button under
//      Approve for laser cutting): "Cut RG Sheet 1" (the existing Cut Sheet press, only after the person's yes in its window) for a sheet whose green line is not cut, else
//      "Rebuild this set correctly". The server's refusal names the same press.
//   B  "Rejected approvals: a sheet someone drags into a formed set can make that set unapprovable." -> the set as it would be after the drop is judged by the owner's rules first
//      (charm-nest-set-repair.js joinCheck over CharmNestSetInvariants.check); a drop that would make a FORMED set break a rule is refused with the rule and what to do instead, by the
//      Library plan (LibraryFlow) and by the server's write (putSheet, flowApply setMember) alike. A drop that keeps the set right is exactly as before.
// Fixtures only (made-up ids and orders); the server parts run the real charmNestLibrary handler over the in-memory Firestore of bridge-server.cjs. No network beyond localhost.
//   node tests/charm-nest/set-approval-fix.cjs
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const SRp = require('../../charm-nest-set-repair.js'), RD = require('../../charm-nest-readiness.js'), LF = require('../../charm-nest-flow.js');

const piece = (order, n) => `${order}_${order}1_${n}`;
let single = 9100000000;
const own = k => Array.from({ length: k }, () => piece(++single, 1));
const link = (a, b, order) => { a.poolIds.push(piece(order, 1)); b.poolIds.push(piece(order, 2)); };
const sheet = (id, metal, no, o = {}) => ({ id, metal, page: no, sheetIndex: no, releaseFull: true, placedCount: 40, density: .76, draft: false, poolIds: own(2), runId: 'run-1', ...o });
const setOf = (setId, seq, ids, o = {}) => ({ setId, id: setId, seq, name: `Set-${seq}`, runId: 'run-1', status: 'open', sheetIds: ids, ...o });

(async () => {
  /* ── A1 · the remedy: what to press, in the owner's words ───────────────────────────────────────────────────────────────────────────── */
  {
    // a Rose Gold sheet that shares an order with the set, no green line: the press is the Cut Sheet press, and nothing else is needed
    const gf = sheet('a-gf', 'gold', 1, { setId: 's-a' }), ss = sheet('a-ss', 'silver', 1, { setId: 's-a' }), rg = sheet('a-rg', 'rose', 1, { setId: 's-a', releaseFull: false, density: .5 });
    link(gf, ss, 4300000001); link(gf, rg, 4300000002);
    const set = setOf('s-a', 7, ['a-gf', 'a-ss', 'a-rg']);
    const info = SRp.diagnose([set], [gf, ss, rg]).bySet['s-a'];
    assert(info, 'the set is called wrong (R12)'); assert.deepEqual(info.rules, ['R12']);
    assert.deepEqual({ kind: info.remedy.kind, button: info.remedy.button, sheets: info.remedy.sheets, then: info.remedy.then }, { kind: 'cut', button: 'Cut RG Sheet 1', sheets: ['a-rg'], then: undefined });
    assert.equal(info.remedy.say, 'Press "Cut RG Sheet 1" (the Cut Sheet press: it draws its green line and records its cut), and Set 7 can then be approved.');
    // the gate (the grey line under Approve for laser cutting) says the press; the plan's need does too
    const b = SRp.breaksOf({ bySet: { 's-a': info } }, 's-a'), st = { ...set, setBreaks: b };
    const gate = RD.setGate(st, [gf, ss, rg]);
    assert.equal(gate.ready, false); assert.equal(gate.blockers[0].rule, 'setBreaks');
    assert.match(gate.reason, /^Set-7 · breaks your rules: RG Sheet 1 not cut yet · press "Cut RG Sheet 1"/);
    assert.equal(gate.rules.remedy.button, 'Cut RG Sheet 1', 'the gate hands the press on to the button under Approve');
    assert.match(RD.explain(st, { kind: 'set', sheets: [gf, ss, rg] }).nextText, /Press "Cut RG Sheet 1" \(the Cut Sheet press/);
    // the Library's set view: the press is right there, with the sentence that says it
    const html = SRp.lineHtml(st);
    assert.match(html, /<button type="button" class="btn sm" data-cut-set="s-a">Cut RG Sheet 1<\/button><button type="button" class="btn sm" data-rebuild-set="s-a">Rebuild this set correctly<\/button>/);
    assert.match(html, /data-set-break-todo>Press &quot;Cut RG Sheet 1&quot;/);
    // a Rose Gold sheet that shares no order: cut it, or rebuild the set without it
    const rg2 = sheet('a-rg2', 'rose', 2, { setId: 's-b', releaseFull: false }), gf2 = { ...gf, id: 'b-gf', setId: 's-b' }, ss2 = { ...ss, id: 'b-ss', setId: 's-b' };
    const i2 = SRp.diagnose([setOf('s-b', 8, ['b-gf', 'b-ss', 'a-rg2'])], [gf2, ss2, rg2]).bySet['s-b'];
    assert.equal(i2.remedy.kind, 'cut'); assert.match(i2.remedy.say, /Or press "Rebuild this set correctly": Set 8 is made again without it until it is cut\.$/);
    // with another fault as well: cut first (the rebuild then keeps it with its unit), then rebuild
    const gf3 = sheet('c-gf3', 'gold', 3, { setId: 's-a' });
    const i3 = SRp.diagnose([{ ...set, sheetIds: set.sheetIds.concat('c-gf3') }], [gf, ss, rg, gf3]).bySet['s-a'];
    assert.deepEqual(i3.rules.slice().sort(), ['R12', 'R7']); assert.equal(i3.remedy.then, 'rebuild');
    assert.equal(i3.remedy.say, 'Press "Cut RG Sheet 1" first (the Cut Sheet press: it draws its green line and records its cut), then "Rebuild this set correctly".');
    assert.equal(i3.remedy.server, 'Press "Cut RG Sheet 1" on its card in the Library (the Cut Sheet press: it draws its green line and records its cut), then "Rebuild this set correctly"');
    // a 14K sheet the same way (its own code in its name)
    const k = sheet('d-14', 'gold14k', 1, { setId: 's-d', releaseFull: false }), gf4 = { ...gf, id: 'd-gf', setId: 's-d' }, ss4 = { ...ss, id: 'd-ss', setId: 's-d' }; link(gf4, k, 4300000004);
    assert.equal(SRp.diagnose([setOf('s-d', 9, ['d-gf', 'd-ss', 'd-14'])], [gf4, ss4, k]).bySet['s-d'].remedy.button, 'Cut 14K Sheet 1');
    // anything else is the rebuild, as before (and a set that is right carries no remedy at all)
    const i5 = SRp.diagnose([setOf('s-e', 10, ['b-gf', 'b-ss', 'c-gf3'])], [{ ...gf2, setId: 's-e' }, { ...ss2, setId: 's-e' }, { ...gf3, setId: 's-e' }]).bySet['s-e'];
    assert.deepEqual([i5.remedy.kind, i5.remedy.button, i5.remedy.server], ['rebuild', 'Rebuild this set correctly', 'Press "Rebuild this set correctly" on its card in the Library']);
    assert.doesNotMatch(SRp.lineHtml({ setId: 's-e', setBreaks: SRp.breaksOf({ bySet: { 's-e': i5 } }, 's-e') }), /data-cut-set/);
    assert.equal(SRp.diagnose([setOf('s-f', 11, ['b-gf', 'b-ss'])], [{ ...gf2, setId: 's-f' }, { ...ss2, setId: 's-f' }]).bySet['s-f'], undefined);
    console.log('A1 ok: the remedy says exactly what to press');

    /* ── A2 · the Cut press: never by itself, only after the yes; refusals say what to do ─────────────────────────────────────────────── */
    const said = [], cuts = [], asked = [];
    const page = { sheetId: 'a-rg', metal: 'rose', status: 'complete', persisted: true, persistedDone: true };
    const env = (o = {}) => ({ by: () => 'Paul', cloud: () => true, pages: () => [page], holding: () => false, say: (t, k) => said.push([k || 'bad', t]), refresh: () => {}, confirmCut: async w => { asked.push(w); return o.yes !== false; }, cut: async (ids, by) => { cuts.push([ids, by]); page.roseCutAt = 1; return ids.map(id => ({ id, cut: true })); }, ...o.env });
    let r = await SRp.cutGold(st, { env: env({ yes: false }) });
    assert.deepEqual([r.ok, r.cancelled, cuts.length], [false, true, 0], 'Cancel: nothing is cut, no line is drawn');
    assert.equal(asked[0].title, 'Cut RG Sheet 1?'); assert.equal(asked[0].confirm, 'Cut'); assert.match(asked[0].what, /A recorded cut is permanent\./); assert.equal(asked[0].nothing, 'Nothing is added until you press Cut.');
    r = await SRp.cutGold(st, { env: env({ env: { by: () => '' } }) }); assert.equal(r.refused.key, 'who'); assert.equal(cuts.length, 0);
    r = await SRp.cutGold(st, { env: env({ env: { pages: () => [] } }) }); assert.equal(r.refused.key, 'away');
    assert.equal(r.refused.reason, 'RG Sheet 1 is not open on this page, so its green line cannot be cut from here. Open it on the Nest tab and press Cut Sheet there; Set 7 can then be approved');
    r = await SRp.cutGold(st, { env: env({ env: { pages: () => [{ ...page, status: 'nesting' }] } }) }); assert.equal(r.refused.key, 'busy'); assert.equal(cuts.length, 0);
    r = await SRp.cutGold(st, { env: env() });
    assert.equal(r.ok, true, JSON.stringify(r)); assert.deepEqual(cuts, [[['a-rg'], 'Paul']], 'the yes runs the Cut Sheet press for that sheet, with the name of the person');
    assert.match(said.at(-1)[1], /^RG Sheet 1 cut\. Set 7 follows your rules and can be approved for laser cutting\.$/);
    // after it, the sheet is cut: the set follows the rules and is not flagged any more
    assert.equal(SRp.diagnose([set], [gf, ss, { ...rg, roseCutAt: 1 }]).bySet['s-a'], undefined);
    // fix(): the button under Approve presses the remedy's first press (the Cut press here, the rebuild for any other fault)
    let used = null; r = await SRp.fix({ setId: 's-e', setBreaks: SRp.breaksOf({ bySet: { 's-e': i5 } }, 's-e') }, { env: { run: () => null, say: () => {}, refresh: () => { used = 'refresh'; }, api: async () => { used = 'rebuild'; return { set: null }; } } });
    assert.equal(used, 'rebuild', 'a set with no uncut sheet is rebuilt (its own window first)');
    // the page code: the button beside the grey reason, and the set card's Cut button
    const bridge = fs.readFileSync(path.join(__dirname, '../../charm-nest-bridge.js'), 'utf8'), repair = fs.readFileSync(path.join(__dirname, '../../charm-nest-set-repair.js'), 'utf8');
    assert(bridge.includes("data-approve-fix=\"${esc(c.fix.kind)}\">${esc(c.fix.button)}</button>") && bridge.includes("else if(t.matches('[data-approve-fix]'))pressFix(box,t);") && bridge.includes('await SetRepair.fix(st);'), 'the Approve box offers the press and runs it');
    assert(repair.includes("card.querySelector('[data-cut-set]')"), 'the set card wires its Cut button');
    // the press is the existing Cut Sheet press of the Library (LibraryFlow.cutLine), never a line added any other way
    assert(/F\.cutLine\(\{ kind: 'sheet', id: ids\[0\], sheetIds: ids, by \}\)/.test(repair));
    console.log('A2 ok: the Cut press runs only after the yes, and says what to do when it cannot');
  }

  /* ── B1 · joinCheck: the set after the drop, judged against the set before ──────────────────────────────────────────────────────────── */
  {
    const gf = sheet('j-gf', 'gold', 1, { setId: 's-j' }), ss = sheet('j-ss', 'silver', 1, { setId: 's-j' }); link(gf, ss, 4400000001);
    const set = setOf('s-j', 3, ['j-gf', 'j-ss']);
    const loose = sheet('j-gf2', 'gold', 2, { draft: true, setId: null, sheetIndex: null });
    let r = SRp.joinCheck(set, ['j-gf2'], [gf, ss, loose], [set]);
    assert.equal(r.ok, false); assert.deepEqual(r.rules, ['R7']);
    assert.equal(r.label, 'GF Draft 2 cannot go into Set 3: Set 3 would be bigger than the minimum');
    assert.equal(r.detail, 'Set 3 already holds a completed GF and a completed SS sheet (GF Sheet 1 and SS Sheet 1), and no order ties GF Draft 2 to its sheets, so Set 3 does not need it. A set holds only the sheets it needs (your rule R7): with it, Set 3 could not be approved for laser cutting. Leave GF Draft 2 out: it forms the next set with a completed SS sheet.');
    // a sheet that shares an order with the set belongs in it (the cardinal rule): allowed, as before
    const tied = sheet('j-gf3', 'gold', 3, { draft: true, setId: null }); link(gf, tied, 4400000002);
    assert.equal(SRp.joinCheck(set, ['j-gf3'], [gf, ss, tied], [set]).ok, true);
    // a partial sheet: refused (R3), whatever it shares
    const part = sheet('j-p', 'silver', 4, { draft: true, setId: null, releaseFull: false, density: .3 }); link(gf, part, 4400000003);
    r = SRp.joinCheck(set, ['j-p'], [gf, ss, part], [set]); assert.equal(r.ok, false); assert.deepEqual(r.rules, ['R3']); assert.match(r.detail, /a partial sheet never joins a set \(your rule R3\)/);
    // a Rose Gold sheet joins only by its own Cut Sheet press: read as cut, allowed (the press is asked for at the drop)
    const rg = sheet('j-rg', 'rose', 1, { draft: true, setId: null, releaseFull: false }); link(gf, rg, 4400000004);
    assert.equal(SRp.joinCheck(set, ['j-rg'], [gf, ss, rg], [set]).ok, true);
    // a set still forming (no completed SS yet) is the run's business; a committed set is SetEdit's
    const forming = setOf('s-k', 4, ['k-gf']), kgf = sheet('k-gf', 'gold', 1, { setId: 's-k' });
    assert.deepEqual([SRp.joinCheck(forming, ['j-gf2'], [kgf, loose], [forming]).ok, SRp.joinCheck(forming, ['j-gf2'], [kgf, loose], [forming]).skipped], [true, 'forming']);
    assert.equal(SRp.joinCheck({ ...set, committedAt: 5, status: 'complete' }, ['j-gf2'], [gf, ss, loose], [set]).skipped, 'committed');
    // a fault the set already had is not the drop's: a unit already spare that its own partner joins stays the same fault
    const spare = sheet('j-sp', 'gold', 5, { setId: 's-j' }), mate = sheet('j-mate', 'gold', 6, { draft: true, setId: null }); link(spare, mate, 4400000005);
    const set2 = { ...set, sheetIds: ['j-gf', 'j-ss', 'j-sp'] };
    assert.equal(SRp.diagnose([set2], [gf, ss, spare]).bySet['s-j'].rules[0], 'R7', 'the set was already bigger than the minimum');
    assert.equal(SRp.joinCheck(set2, ['j-mate'], [gf, ss, spare, mate], [set2]).ok, true, 'its partner may join it (the rebuild is the way out of that fault)');
    assert.equal(SRp.joinCheck(set2, ['j-gf2'], [gf, ss, spare, loose], [set2]).ok, false, 'a NEW spare unit is still refused');
    // the server's write reads only the rules a saved record always answers
    assert.equal(SRp.joinCheck(set, ['j-gf2'], [gf, ss, loose], [set], { rules: ['partial'] }).ok, true);
    console.log('B1 ok: joinCheck refuses exactly the drops that make a formed set break a rule');
  }

  /* ── B2 · the server and the Library plan over the real handler ─────────────────────────────────────────────────────────────────────── */
  {
    const { start } = require('./bridge-server.cjs');
    const SH = 'Charm_Nest_Sheets', SET = 'Charm_Nest_Sets', RUNS = 'Charm_Nest_Runs';
    const srv = await start({ receipts: [] }), { st } = srv, now = Date.now(), ts = { toMillis: () => now }, day = '2026-10-17';
    const post = async body => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }); return { status: r.status, ...await r.json() }; };
    const lines = {}; let n = 0;
    const mk = (id, o = {}) => {
      const order = String(3800000000 + ++n), pool = [order + '_1_1'], { link: lk, metal, ...rest } = o, orders = [order];
      lines[order + '_1'] = { orderId: order, state: 'written', quantity: 1, poolIds: pool.slice(), engraveCandidate: false };
      if (lk) { const [lo, copy] = lk; pool.push(`${lo}_1_${copy}`); orders.push(lo); lines[lo + '_1'] = lines[lo + '_1'] || { orderId: lo, state: 'written', quantity: 2, poolIds: [], engraveCandidate: false }; lines[lo + '_1'].poolIds.push(`${lo}_1_${copy}`); }
      st.put(SH, id, { id, runId: 'run-x', metal: metal || 'gold', day, status: 'complete', placedCount: pool.length, charmCount: pool.length, density: .7, poolIds: pool, orders, verification: { ok: true }, releaseFull: true, page: n, sheetIndex: 1, updatedAt: ts, createdAt: ts, ...rest });
    };
    st.put(SET, 'set-20', { setId: 'set-20', seq: 20, day, runId: 'run-x', group: 'dispatch', sheetIds: ['x-gf', 'x-ss'], materials: ['gold', 'silver'], orders: {}, status: 'open', updatedAt: ts, createdAt: ts });
    st.put(SET, 'set-21', { setId: 'set-21', seq: 21, day, runId: 'run-x', group: 'dispatch', sheetIds: ['y-gf', 'y-ss'], materials: ['gold', 'silver'], orders: {}, status: 'complete', committedAt: now - 9000, updatedAt: ts, createdAt: ts });
    mk('x-gf', { setId: 'set-20', setSeq: 20, link: ['3899000001', 1] }); mk('x-ss', { setId: 'set-20', setSeq: 20, metal: 'silver', link: ['3899000001', 2] });
    mk('y-gf', { setId: 'set-21', setSeq: 21, link: ['3899000002', 1] }); mk('y-ss', { setId: 'set-21', setSeq: 21, metal: 'silver', link: ['3899000002', 2] });
    mk('x-loose', { draft: true, setId: null, sheetIndex: null });                                   // a completed GF sheet no order ties to Set 20
    mk('x-tied', { draft: true, setId: null, sheetIndex: null, link: ['3899000003', 2] }); st.put(SH, 'x-gf', { poolIds: st.doc(SH, 'x-gf').poolIds.concat(['3899000003_1_1']), orders: st.doc(SH, 'x-gf').orders.concat(['3899000003']) });
    st.put(RUNS, 'run-x', { runId: 'run-x', status: 'running', lines });
    const snap = () => JSON.stringify([...st.docs.entries()].filter(([k]) => /^Charm_Nest_(Sheets|Sets)\//.test(k)));
    try {
      // the plan's read: flowState answers what the move would do to the set
      let r = await post({ op: 'flowState', sheetIds: ['x-loose'], setIds: ['set-20'], move: { kind: 'sheet', id: 'x-loose', to: { set: 'set-20' } } });
      assert.equal(r.status, 200); assert.equal(r.rules.ok, false, JSON.stringify(r.rules)); assert.equal(r.rules.setId, 'set-20'); assert.deepEqual(r.rules.rules, ['R7']);
      assert.match(r.rules.label, /^GF Draft \d+ cannot go into Set 20: Set 20 would be bigger than the minimum$/);
      r = await post({ op: 'flowState', sheetIds: ['x-tied'], setIds: ['set-20'], move: { kind: 'sheet', id: 'x-tied', to: { set: 'set-20' } } });
      assert.equal(r.rules.ok, true, 'a sheet that shares an order with Set 20 may join it: ' + JSON.stringify(r.rules));
      // the Library plan over it: refused with the rule, nothing to commit (the card flies back); the tied sheet plans as before
      LF.configure({ api: body => post(body).then(x => { if (x.status >= 400) throw new Error(x.error); return x; }), live: id => ({ runHere: true, draft: true, dispatchSetId: 'set-20', can: { ok: true, byHand: true }, split: [] }), include: async () => {}, employee: () => 'Paul', rows: () => [] });
      let p = await LF.plan({ kind: 'sheet', id: 'x-loose', to: { set: 'set-20' } });
      assert.equal(p.ok, false); assert.deepEqual(p.needs.map(x => x.key), ['setRules']); assert.match(p.needs[0].detail, /your rule R7/); assert.equal(p.confirm.length, 0);
      p = await LF.plan({ kind: 'sheet', id: 'x-tied', to: { set: 'set-20' } });
      assert.equal(p.ok, true, JSON.stringify(p.needs)); assert.deepEqual(p.steps.map(x => x.type), ['include']);
      // the server's write: putSheet into the formed set is refused, and nothing is written, whichever page sends it
      const before = snap();
      r = await post({ op: 'putSheet', sheet: { id: 'x-loose', draft: false, setId: 'set-20', setSeq: 20, sheetIndex: 2 } });
      assert.equal(r.status, 409, JSON.stringify(r)); assert.equal(r.refused, 'setRules'); assert.equal(r.rule, 'surplus'); assert.match(r.error, /^GF Draft \d+ cannot go into Set 20: Set 20 would be bigger than the minimum\. Set 20 already holds/);
      assert.equal(snap(), before, 'not one record changed');
      // ... and flowApply's set edit into it the same
      r = await post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'setMember', moves: [{ sheetId: 'x-loose', to: 'set-20' }] }] });
      assert.equal(r.status, 409, JSON.stringify(r)); assert.equal(r.refused, 'setRules'); assert.equal(snap(), before);
      // a move that keeps the set right is written as before; a save that does not change membership is never asked
      r = await post({ op: 'putSheet', sheet: { id: 'x-tied', draft: false, setId: 'set-20', setSeq: 20, sheetIndex: 2 } }); assert.equal(r.status, 200, JSON.stringify(r)); assert.equal(st.doc(SH, 'x-tied').setId, 'set-20');
      r = await post({ op: 'putSheet', sheet: { id: 'x-gf', draft: false, setId: 'set-20', setSeq: 20, sheetIndex: 1, density: .71 } }); assert.equal(r.status, 200, JSON.stringify(r));
      // a committed set is not judged here (its edit is SetEdit's rule): the loose sheet goes into Set 21 by the committed-set edit
      r = await post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'setMember', moves: [{ sheetId: 'x-loose', to: 'set-21' }] }] });
      assert.notEqual(r.refused, 'setRules', JSON.stringify(r));
      // the approval gate names the press that puts a set right
      mk('z-gf', { setId: 'set-22', setSeq: 22, link: ['3899000009', 1] }); mk('z-ss', { setId: 'set-22', setSeq: 22, metal: 'silver', link: ['3899000010', 1] }); mk('z-rg', { setId: 'set-22', setSeq: 22, metal: 'rose', releaseFull: false, density: .5, link: ['3899000009', 2] });
      st.put(SET, 'set-22', { setId: 'set-22', seq: 22, day, runId: 'run-x', group: 'dispatch', sheetIds: ['z-gf', 'z-ss', 'z-rg'], materials: ['gold', 'silver', 'rose'], orders: {}, status: 'labelled', updatedAt: ts, createdAt: ts });
      st.put(RUNS, 'run-x', { runId: 'run-x', status: 'running', lines });
      r = await post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'seal', kind: 'set', id: 'set-22' }] });
      assert.equal(r.status, 409); assert.match(r.error, /^Set 22 breaks your rules: Rose Gold sheet RG Sheet 1 is not cut yet \(no green line\), so it cannot be approved for laser cutting\. Press "Cut RG Sheet 1" on its card in the Library \(the Cut Sheet press: it draws its green line and records its cut\)$/);
      assert.equal(r.remedy.kind, 'cut');
    } finally { srv.close(); }
    console.log('B2 ok: the server and the Library plan refuse a drop that breaks a formed set, and only that');
  }
  console.log('PASS: set-approval-fix: a set blocked at approval says exactly what to press and offers that press (Cut ... for an uncut Rose Gold / 10K / 14K sheet, after its own yes; else Rebuild this set correctly), the server names the same press; a drop that would make a formed set break a rule is refused with the rule and what to do instead, by the plan and by the server, and a drop that keeps the set right is unchanged');
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
