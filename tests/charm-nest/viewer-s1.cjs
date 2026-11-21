// S1 (Paul, 21 Nov 2026): Review answers made on a staff computer (a viewer) in cloud mode "on" stayed only in that page's memory; the cloud never saw them.
//   What was wrong: on a viewer Review.repool answers false at once; the page's runLinesPatch carried only state / poolIds / reason / hold / wait / engrave, never an override; and the
//   cloud world's applyToRows leaves a row whose cloud copy did not change, so the page kept its private copy. Broken: Skip piece (lines skipped in page memory only), Use this size and
//   a material's "Use it" (the override on the page row only; the "Decided" step lost), and Try again on a Too big card (repool → false, in silence).
//   C  the library in mode "on" (offline, _cloudFakes.cjs) and the cloud's own code:
//      C1 runLinesPatch with decision: true from a viewer saves a skip (state skipped, reason, hold) and an override (materialOverride, sizeOverride) on the run's line, answers the lines as
//         the cloud now has them, and writes the "Skipped" / "Decided" step on each order's timeline ONCE (sent twice: one step)
//      C2 without decision a page's patch writes exactly what it wrote before (no override); an answer that sets another state, or a bad material, is refused in words
//      C3 the cloud acts on it: the world reads the line with the override (runFacts, afterRead: the question gone); the intake waits for that reading and then takes the line with the override
//         (rowOf); a skipped line is never made (pool); a hold that names its lines takes off those lines' pieces only (take-off scopeOf); a skipped line stays skipped there
//   P  the page: Review's real S1 code (charm-nest-bridge.js) and the real CharmNestRunFollow.decideRows (charm-nest-run-follow.js) in a bare context whose api is the offline library:
//      P1 Skip piece: the cloud's line is skipped, the card's question is gone from the row (the cloud's answer), one "Skipped" step, said in words
//      P2 Skip piece of a line with pieces: also the cloud's take-off is asked for that line only (takeOffAsk hold, lineKeys)
//      P3 a material "Use it": the cloud's line carries materialOverride, the row's needsMaterial question is gone, one "Decided" step
//      P4 Use this size: sizeOverride in the cloud, missingSize gone; a line with pieces is made again by the cloud (remakeline asked, its answer said)
//      P5 Try again (Too big): the cloud's remake is put (runCmdPut remakeline, the open line), its answer said; never silent
//      P6 a refusal is said and nothing is changed on the row; mode "off" (or a page that holds the run): the presses go their old way (the source branches on viewerAnswers() first)
//      P7 charm-nest-1.html loads the changed bridge and run-follow under new tokens (-s1)
// Offline only: no Etsy, no paid call, no live write. Made-up names.
//   NODE_PATH=<deps> node tests/charm-nest/viewer-s1.cjs
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '../..');
let failed = 0;
const log0 = console.log.bind(console);
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 1400))); if (!cond) failed++; };
const CV = 'cloud14-20261120', PAGE = 'd0michelle1';

async function main() {
  process.env.EDIT_PASSCODE = 'test-pass';
  const T0 = Date.UTC(2026, 10, 21, 16, 0, 0);
  const F = require('./_cloudFakes.cjs')({ now: T0 });
  const quiet = console.log, qw = console.warn; console.log = () => {}; console.warn = () => {};
  try {
    const lib = F.load('charmNestLibrary.js'), Intake = F.load('_charmNestIntake.js'), World = F.load('_charmNestWorld.js'), TakeOff = F.load('_charmNestTakeOff.js');
    const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
    const R = 'run-2026-11-21-s1';
    const K = n => `43100001${n}0_43100001${n}01`, rid = k => k.split('_')[0];
    const A = K(1), Bk = K(2), C = K(3), D = K(4), E = K(5), G = K(6), H = K(7), D2 = `${rid(D)}_${rid(D)}02`;
    const seed = () => {
      F.reset(); F.clock.set(T0);
      F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'auto', cloudSteps: ['pull', 'pool', 'nest', 'takeoff', 'cmd:remakeline', 'cmd:release'], flippedAt: T0 - 3600e3 }); lib._driveForget();
      F.seed('Charm_Nest_Open/run', { runId: R, at: T0 - 3600e3 });
      F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
      F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'running', step: 'nest', mode: 'auto', day: '2026-11-21', releasePolicy: 2, sheets: {}, holds: {}, errors: [], updatedAt: F.Timestamp.fromMillis(T0 - 60e3),
        orders: [A, Bk, C, D, E, G, H].map(rid),
        lines: {
          [A]: { orderId: rid(A), state: 'held', reason: 'its metal is not read, so it cannot be put on a sheet', poolIds: [] },   // needsMaterial (material answer)
          [Bk]: { orderId: rid(Bk), state: 'unmatched', reason: 'no design for size L', poolIds: [] },   // missingSize (size answer, no pieces)
          [C]: { orderId: rid(C), state: 'held', reason: 'its metal is not read, so it cannot be put on a sheet', poolIds: [] },   // needsMaterial (Skip piece)
          [D]: { orderId: rid(D), state: 'pooled', reason: null, poolIds: [D + '_1'] },   // Skip piece of a line with a piece made
          [D2]: { orderId: rid(D), state: 'pooled', reason: null, poolIds: [D2 + '_1'] },   // the other line of that order: kept
          [E]: { orderId: rid(E), state: 'oversize', reason: 'oversize for the GF plate', poolIds: [] },   // Too big: Try again
          [G]: { orderId: rid(G), state: 'pooled', reason: null, poolIds: [G + '_1'] },   // missingSize answered on a line with a piece (made again by the cloud)
          [H]: { orderId: rid(H), state: 'held', reason: 'its metal is not read', poolIds: [] } } });
    };
    const lines = async () => { const g = await P({ op: 'runGet', runId: R }); return (g.run && g.run.lines) || {}; };
    const steps = async (r, type) => ((await P({ op: 'timelineGet', orderId: r, derive: false })).events || []).filter(e => e.type === type);
    seed();

    /* ═══ C · the cloud ═══ */
    const who = 'Michelle', at = T0 - 1000;
    const dec = (k, type, kind, say) => ({ type, id: `${k}.${kind}.${at}`, at, by: who, text: `${type === 'skipped' ? 'Skipped' : 'Decided'} by ${who} · ${kind}: ${say}`, data: { kind, answer: say, from: 'review' } });
    let r = await P({ op: 'runLinesPatch', runId: R, dev: PAGE, by: who, cv: CV, decision: true, lines: {
      [C]: { state: 'skipped', reason: `piece skipped by ${who}`, hold: `piece skipped by ${who}`, heldAt: at, decided: dec(C, 'skipped', 'needsMaterial', 'Skip piece') },
      [H]: { materialOverride: 'silver', decided: dec(H, 'decided', 'needsMaterial', 'material SS') } } });
    let L = await lines();
    ok('C1 mode on, a viewer\'s Review answer (decision: true) is saved on the run\'s lines: the skip (skipped, its reason and hold) and the material (materialOverride), stamped as the page\'s',
      r.http === 200 && r.ok && L[C].state === 'skipped' && /skipped by Michelle/.test(L[C].hold) && L[H].materialOverride === 'silver' && L[H]._lp && L[H]._lp.dev === PAGE, { r, c: L[C], h: L[H] });
    ok('C1b ... the answer comes back whole: the lines as the cloud now has them', r.lines && r.lines[C] && r.lines[C].state === 'skipped' && r.lines[H] && r.lines[H].materialOverride === 'silver' && r.decided === 2, r);
    let sk = await steps(rid(C), 'skipped'), de = await steps(rid(H), 'decided');
    ok('C1c ... one "Skipped" and one "Decided" step on the orders\' timelines, written by the cloud', sk.length === 1 && /Skipped by Michelle · needsMaterial: Skip piece/.test(sk[0].text) && sk[0].data && sk[0].data.cloud === true && de.length === 1 && de[0].by === who, { sk, de });
    r = await P({ op: 'runLinesPatch', runId: R, dev: PAGE, by: who, cv: CV, decision: true, lines: { [C]: { state: 'skipped', reason: `piece skipped by ${who}`, hold: `piece skipped by ${who}`, heldAt: at, decided: dec(C, 'skipped', 'needsMaterial', 'Skip piece') } } });
    sk = await steps(rid(C), 'skipped');
    ok('C1d ... the same answer sent twice (a retry) leaves one step', r.ok && sk.length === 1, sk.length);
    // C2 the door as before, and refusals in words
    r = await P({ op: 'runLinesPatch', runId: R, dev: PAGE, by: who, cv: CV, lines: { [A]: { materialOverride: 'gold', reason: 'x' } } });
    L = await lines();
    ok('C2 without decision a page\'s patch writes what it wrote before: no override', r.ok && L[A].reason === 'x' && L[A].materialOverride === undefined, L[A]);
    r = await P({ op: 'runLinesPatch', runId: R, dev: PAGE, by: who, cv: CV, decision: true, lines: { [A]: { state: 'committed' } } });
    const r2 = await P({ op: 'runLinesPatch', runId: R, dev: PAGE, by: who, cv: CV, decision: true, lines: { [A]: { materialOverride: 'Gold; drop' } } });
    L = await lines();
    ok('C2b an answer that sets another state, or a material that is none, is refused in words and changes nothing', r.http >= 400 && /does not set "committed"/.test(r.error) && r2.http >= 400 && /not a material/.test(r2.error) && L[A].state === 'held' && L[A].materialOverride === undefined, { r, r2 });
    // C3 the cloud acts on what was saved
    const wl = (k, facts, problems) => ({ key: k, order: { receiptId: rid(k), createTs: 1763700000 }, line: { transactionId: k.split('_')[1], sku: 'MOON_7' }, spec: { designSku: 'MOON_7', material: null, size: null, problems }, problems, runId: R, run: World.runFacts(facts) });
    const needsMat = [{ kind: 'needsMaterial', metalLabel: 'none' }], hBefore = { orderId: rid(H), state: 'held', reason: 'its metal is not read', poolIds: [] };
    const fresh = World.afterRead({ spec: { problems: needsMat.slice(), material: null }, state: 'held', materialOverride: L[H].materialOverride }, null, null);
    ok('C3 the cloud\'s world carries the answer (runFacts) and reads the line with it: its metal is silver, the question gone', World.runFacts(L[H]).materialOverride === 'silver' && World.runFacts(L[C]).state === 'skipped' && fresh.spec.material === 'silver' && fresh.problems.length === 0, fresh);
    const rowH = Intake.rowOf(H, L[H], wl(H, L[H], []), R), staleH = Intake.rowOf(H, L[H], wl(H, hBefore, needsMat), R);
    ok('C3b ... the intake waits while the world read the line without the answer, then takes it with the override (its metal silver)', staleH === null && !!rowH && rowH.materialOverride === 'silver' && rowH.material === 'silver', { rowH: rowH && { m: rowH.material, o: rowH.materialOverride } });
    const made = await Intake.pool({ db: F.db, runId: R, run: { runId: R, releasePolicy: 2, lines: { [C]: L[C] } }, budgetEnd: Date.now() + 5000 });
    ok('C3c ... a skipped line is never made into pieces (the intake does not take it)', made && made.made === 0 && !made.waitWorld && !made.pooled, made);
    const job = { kind: 'hold', lineKeys: [D], at: T0, by: who }, o = { placement: { [D + '_1']: { state: 'sheet', sheetId: 'sh1' }, [D2 + '_1']: { state: 'sheet', sheetId: 'sh1' } }, sheets: [{ id: 'sh1' }] };
    const sc = TakeOff.scopeOf(job, o, { [D]: { state: 'skipped', poolIds: [D + '_1'] }, [D2]: { state: 'pooled', poolIds: [D2 + '_1'] } }, rid(D));
    const all = TakeOff.scopeOf({ kind: 'hold', lineKeys: [], at: T0 }, o, { [D]: {}, [D2]: {} }, rid(D));
    ok('C3d a hold that names its lines (Skip piece) takes off those lines\' pieces only; a hold that names none takes the whole order, as before',
      sc.lineKeys.join() === D && (sc.onSheets.get('sh1') || { ids: [] }).ids.join() === D + '_1' && all.lineKeys.length === 2 && all.onSheets.get('sh1').ids.length === 2, { sc: { k: sc.lineKeys, ids: [...sc.onSheets.values()].map(x => x.ids) }, all: all.lineKeys });
    const tsrc = fs.readFileSync(path.join(ROOT, 'netlify/functions/_charmNestTakeOff.js'), 'utf8');
    ok('C3e ... and the take-off leaves a skipped line skipped (only its pieces go)', /l\.state !== "held" && l\.state !== "skipped"\)/.test(tsrc));

    /* ═══ P · the page ═══ */
    seed();
    const bsrc = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8');
    const i0 = bsrc.indexOf('/* ── S1 (Paul, 21 Nov 2026: Review answers'), i1 = bsrc.indexOf('/** The face a Review card is about');
    const s1src = bsrc.slice(i0, i1) + '\n;this.__s1 = { cloudAnswer, cloudSkip, cloudOverride, cloudRemake, viewerAnswers };';
    const ri = bsrc.indexOf('window.CNRelayAsk = async function'), relaySrc = bsrc.slice(ri, bsrc.indexOf('\n};\n', ri) + 3);
    const followSrc = fs.readFileSync(path.join(ROOT, 'charm-nest-run-follow.js'), 'utf8');
    const KIND_WORDS = { needsMaterial: 'Options · metal', missingSize: 'Size', oversize: 'Too big' };
    function page(o) {
      const toasts = [], calls = [], ls = new Map();
      const w = { console: { log() {}, warn() {}, error() {} }, Promise, Date, JSON, Math, Map, Set, Object, Array, String, Number, Error, RegExp,
        setTimeout: (f, ms) => { const t = setTimeout(f, Math.min(ms || 0, 5)); if (t && t.unref) t.unref(); return t; }, clearTimeout, setInterval: () => 0, clearInterval() {} };
      w.window = w; w.self = w;
      w.localStorage = { getItem: k => (ls.has(k) ? ls.get(k) : null), setItem: (k, v) => ls.set(k, String(v)), removeItem: k => ls.delete(k) };
      ls.set('cn.dev', PAGE);
      w.B = { run: o.holdsRun ? { runId: R } : null, employee: who, orders: { rows: o.rows, byKey: new Map(o.rows.map(x => [x.key, x])) } };
      w.toast = (m, k) => { toasts.push([k || '', String(m)]); };
      w.humanAct = () => true; w.CNListActivity = { touch() {} };
      w.TYPE_WORDS = { decided: 'Decided', skipped: 'Skipped', held: 'Held' }; w.KIND_WORDS = KIND_WORDS;
      w.skipSeen = new Map(); w.synced = 0; w.syncOrderItems = () => { w.synced++; };
      w.rowsOf = it => (it.rows && it.rows.length ? it.rows : it.row ? [it.row] : []).filter(x => x.state !== 'gone');
      w.Orders = { rows: () => o.rows, render() {} };
      w.employeeName = () => who;
      w.CNViewer = { on: () => o.mode !== 'off', lane: name => ['takeoff', 'cmd:remakeline'].includes(name) };
      w.api = w.__api = async (fn, body) => {
        calls.push(body);
        const res = await P(Object.assign({ cv: CV }, body));
        if (res.http >= 400) throw Object.assign(new Error(res.error || 'HTTP ' + res.http), { status: res.http });
        return res;
      };
      w.CN = { api: (fn, body) => w.__api(fn, body) };
      const ctx = vm.createContext(w);
      vm.runInContext(followSrc, ctx);
      vm.runInContext(relaySrc, ctx);
      // (the relay's own wait is the cloud's pass, minutes: here the request is put for real and the cloud's answer is the one a pass gives)
      const realRelay = w.CNRelayAsk;
      w.CNRelayAsk = async (cmd, onStep, maxMs, opt) => {
        const put = await w.__api('charmNestLibrary', { op: 'runCmdPut', runId: R, dev: PAGE, by: who, cmd });
        w.__asked = { cmd, opt, put };
        return put && put.id ? { ok: true, text: `${(cmd.ids || []).length} order line(s) made again: 1 piece(s) on GF Draft 1` } : { ok: false, text: (put && put.error) || 'not sent' };
      };
      w.__realRelay = realRelay;
      vm.runInContext(s1src, ctx);
      return { w, toasts, calls, S: w.__s1 };
    }
    const rowOf = (k, problems, extra) => Object.assign({ key: k, order: { receiptId: rid(k) }, line: { transactionId: k.split('_')[1] }, spec: { staffNote: '' }, problems: problems.slice(), state: 'held', reason: null, poolIds: [], cloud: 'sig0' }, extra || {});
    // P1 Skip piece
    { const row = rowOf(C, [{ kind: 'needsMaterial' }]), { w, toasts, S } = page({ rows: [row] });
      const a = await S.cloudSkip({ kind: 'needsMaterial', key: 'ord:mat:x', rows: [row] }, who);
      const l = (await lines())[C], sk1 = await steps(rid(C), 'skipped');
      ok('P1 Skip piece on a viewer: the cloud\'s line is skipped (its reason and hold), the row takes the cloud\'s answer and asks nothing, the card\'s list is drawn again',
        a.ok && l.state === 'skipped' && /piece skipped by Michelle/.test(l.hold) && row.state === 'skipped' && row.problems.length === 0 && /piece skipped by Michelle/.test(row.hold) && w.synced > 0 && w.skipSeen.get(C) === true, { a, l, row });
      ok('P1b ... one "Skipped" step on its timeline (the cloud\'s), and the person is told it is skipped in the cloud', sk1.length === 1 && /Skipped by Michelle · Options · metal: Skip piece/.test(sk1[0].text) && toasts.some(t => t[0] === 'ok' && /Skipped in the cloud: this piece is never made/.test(t[1])), { sk1, toasts }); }
    // P2 Skip piece of a line with a piece made
    { const row = rowOf(D, [{ kind: 'needsMaterial' }], { state: 'pooled', poolIds: [D + '_1'] }), { toasts, S } = page({ rows: [row] });
      const a = await S.cloudSkip({ kind: 'needsMaterial', rows: [row] }, who);
      const ask = Object.values((F.doc('Charm_Nest_TakeOff/' + rid(D)) || {}).asks || {});
      ok('P2 Skip piece of a line with pieces: skipped in the cloud and the cloud\'s take-off is asked for THAT line only (hold, lineKeys), said in words',
        a.ok && (await lines())[D].state === 'skipped' && ask.length === 1 && ask[0].kind === 'hold' && ask[0].lineKeys.join() === D && ask[0].by === who && toasts.some(t => t[0] === 'ok' && /the cloud takes its pieces off the sheet/.test(t[1])), { ask, toasts }); }
    // P3 a material
    { const row = rowOf(H, [{ kind: 'needsMaterial' }]), { toasts, S } = page({ rows: [row] });
      const a = await S.cloudOverride({ kind: 'needsMaterial', rows: [row] }, 'materialOverride', 'silver', 'material SS', { material: 'silver' }, who);
      const l = (await lines())[H], de1 = await steps(rid(H), 'decided');
      ok('P3 a material\'s "Use it" on a viewer: materialOverride on the cloud\'s line, the row reads silver and its question is gone, one "Decided" step, said',
        a.ok && l.materialOverride === 'silver' && row.materialOverride === 'silver' && row.spec.material === 'silver' && !row.problems.length && de1.length === 1 && /Decided by Michelle · Options · metal: material SS/.test(de1[0].text) && de1[0].data.material === 'silver'
        && toasts.some(t => t[0] === 'ok' && /Material SS: saved in the cloud\. The cloud reads the order again with it and makes its pieces/.test(t[1])), { l, row, de1, toasts }); }
    // P4 a size: no pieces, then a line with a piece (made again by the cloud)
    { const row = rowOf(Bk, [{ kind: 'missingSize' }], { state: 'unmatched' }), row2 = rowOf(G, [{ kind: 'missingSize' }], { state: 'pooled', poolIds: [G + '_1'] }), { w, toasts, S } = page({ rows: [row, row2] });
      const a = await S.cloudOverride({ kind: 'missingSize', rows: [row] }, 'sizeOverride', 'M', 'size M', { size: 'M' }, who);
      const b = await S.cloudOverride({ kind: 'missingSize', rows: [row2] }, 'sizeOverride', 'M', 'size M', { size: 'M' }, who);
      for (let t = 0; t < 60 && !toasts.some(x => /^The cloud: /.test(x[1])); t++) await new Promise(res => setTimeout(res, 50));
      const L4 = await lines(), cmds = (F.doc('Charm_Nest_Run_Cmds/' + R) || {}).cmds || {}, rm = Object.values(cmds).find(c => c && c.type === 'remakeline');
      ok('P4 Use this size on a viewer: sizeOverride on the cloud\'s lines, missingSize gone from the rows', a.ok && b.ok && L4[Bk].sizeOverride === 'M' && L4[G].sizeOverride === 'M' && row.spec.size === 'M' && !row.problems.length && !row2.problems.length, { b: L4[Bk], g: L4[G] });
      ok('P4b ... the line with a piece is made again by the cloud (remakeline put for it, always), the cloud\'s answer said', !!rm && ((rm.args && rm.args.ids) || []).join() === G && w.__asked.opt && w.__asked.opt.always === true && toasts.some(t => t[0] === 'ok' && /^The cloud: 1 order line\(s\) made again/.test(t[1])), { rm, toasts }); }
    // P5 Try again (Too big)
    { const row = rowOf(E, [{ kind: 'oversize' }], { state: 'oversize' }), { w, toasts, S } = page({ rows: [row] });
      const before = Object.keys((F.doc('Charm_Nest_Run_Cmds/' + R) || {}).cmds || {}).length;
      const ans = await S.cloudRemake([E], 'Try again: MOON_7 too big for the plate');
      const cmds = Object.values((F.doc('Charm_Nest_Run_Cmds/' + R) || {}).cmds || {});
      ok('P5 Try again on a Too big card from a viewer: the cloud\'s remake is put for the line (remakeline) and its answer is said; never silent',
        ans.ok && cmds.length === before + 1 && w.__asked.cmd.type === 'remakeline' && w.__asked.cmd.ids.join() === E && toasts.length >= 2 && toasts.some(t => /The cloud makes the piece again/.test(t[1])), { ans, toasts });
      const none = await S.cloudRemake([], 'x');
      ok('P5b ... with no open line to make again it says so', none.none && toasts.some(t => t[0] === 'bad' && /Nothing to make again/.test(t[1]))); }
    // P6 refusals; mode off / a page that holds the run
    { const row = rowOf(A, [{ kind: 'needsMaterial' }]), { toasts, S } = page({ rows: [row] });
      const a = await S.cloudOverride({ kind: 'needsMaterial', rows: [row] }, 'materialOverride', 'Gold; drop', 'material ?', null, who);
      ok('P6 a refused answer is said ("Not saved in the cloud ... Nothing was changed") and the row keeps its question', !a.ok && row.problems.length === 1 && !row.materialOverride && toasts.some(t => t[0] === 'bad' && /Not saved in the cloud: .*not a material.*Nothing was changed/.test(t[1])), toasts); }
    { const off = page({ rows: [], mode: 'off' }), mine = page({ rows: [], holdsRun: true });
      ok('P6b mode off, and a page that holds the run: viewerAnswers() is false (the presses go their old way)', off.S.viewerAnswers() === false && mine.S.viewerAnswers() === false && page({ rows: [] }).S.viewerAnswers() === true); }
    const cardSrc = bsrc.slice(bsrc.indexOf('    if (it.kind === "needsMaterial") {\n      c.innerHTML = head("Needs material"'), bsrc.indexOf('  async function row_material(it, m, who)'));
    ok('P6c each broken press asks viewerAnswers() first and keeps its old way after it: material Use it, Use this size, Try again, Skip piece',
      /data-a=mat\]"\)\.onclick[\s\S]{0,400}if \(viewerAnswers\(\)\) return saving\(c, async \(\) => \{ const a = await cloudOverride\(it, "materialOverride"[\s\S]{0,700}answered\(it, "decided", `material \$\{labelOf\(m\)\}`[^\n]*row_material\(it, m, who\)/.test(cardSrc)
      && /data-a=size\]"\)\.onclick[\s\S]{0,400}if \(viewerAnswers\(\)\) return saving\(c, async \(\) => \{ const a = await cloudOverride\(it, "sizeOverride"[\s\S]{0,500}r\.sizeOverride = s;[^\n]*await repool\(r\)/.test(cardSrc)
      && /data-a=retry\]"\)\.onclick = \(\) => \(viewerAnswers\(\) \? saving\(c, \(\) => cloudRemake\([\s\S]{0,300}: repool\(r\)\)/.test(cardSrc)
      && /skipB\.onclick = \(\) => \{ const who = by\(\); if \(!who\) return; if \(viewerAnswers\(\)\) return saving\(c, \(\) => cloudSkip\(it, who\)\)\(\);[^\n]*\n\s*for \(const rr of rowsOf\(it\)\) \{ rr\.state = "skipped"/.test(cardSrc));
    const html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8');
    ok('P7 charm-nest-1.html loads the changed bridge and run-follow under new tokens (-s1)', /charm-nest-bridge\.js\?v=[^"]*-s1(?:-\w+)*"/.test(html) && /charm-nest-run-follow\.js\?v=[^"]*-s1(?:-\w+)*"/.test(html));
  } finally { console.log = quiet; console.warn = qw; }
}

main().then(() => { log0(failed ? `FAIL viewer-s1 (${failed})` : 'PASS viewer-s1'); process.exit(failed ? 1 : 0); })
  .catch(e => { log0('FAIL viewer-s1: ' + (e && e.stack || e)); process.exit(1); });
