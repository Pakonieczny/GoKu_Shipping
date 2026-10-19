// CARDSEAL (Paul, 19 Oct 2026): "I'm also not seeing the Approved Engraving/Monograms Seals in each order card in the Orders tab. All orders that are approved for engraving, or monograms by the user must have a clear seal that is
// easily visible to the user whether they are inside the Detail Order view or looking at a list view in the Orders tab."   (this test is the Orders tab's LIST CARDS: cards and the compact list; the order window is another branch's)
// charm-nest-order-seals.js (window.CNOrderSeals) reads, from what is already in memory, the approved engraving jobs (each piece and face: Engrave.jobsOf + CNEngravingSeals) and the approved monogram pieces (CNMonogramSeals) of an
// order's lines; the bridge's Orders list draws its strip on the card and keeps its signature in the card's stamp (so an approval repaints the card).
//   1  node: the module over fake roots (the real CNEngravingSeals.list / CNMonogramSeals): back, front + back, half-approved, a bar cut plain, a reopened job, Left / Right, monogram pieces, no seal in memory, no reader
//   2  node: the source: the card builders carry the strip and its signature, the repaint calls, the manifests and cache tokens, and the module makes no read, timer or listener
//   3  the REAL page (the engraving-UI real world, _engraveRealWorld.cjs: real orders through the real intake, pool, classifier and fit) in the Orders tab, list and cards: an approved back shows its seal, front + back two,
//      a half-approved order says "1 of 3 approved", an unapproved order none, a monogram its MONOGRAM seal (never BACK ENGRAVING), live repaint, and not one request, timer or ensureSeals call for it
//   ORDLIST + SEAL15 (Paul, 19 Oct 2026): the Orders rows and cards are laid out like the Engraving list: each engraved line says its caption, one line per piece and face (chip, words, state), and its seals stand in the row's seals cell,
//   one chip + seal per piece and face (the Engrave tab's own Engrave.listLine); the separate strip ("1 of 2 approved") is no longer drawn on them. The list seal is 57 px (1.5 x 38) in both tabs (--list-seal).
//   node tests/charm-nest/order-card-seals.cjs [playwright-core dir]        (PW_DIR=<playwright node_modules> CHROMIUM=<chrome> SHOTS=<dir>)
'use strict';
const fs = require('fs'), os = require('os'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const SHOTS = process.env.SHOTS || path.join(os.tmpdir(), 'order-card-seals');
const read = f => fs.readFileSync(path.join(root, f), 'utf8');
const fails = []; let nCheck = 0;
const check = (ok, msg) => { nCheck++; if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
const BAD = /BACK ENGRAVING|Back engraving|FRONT ENGRAVING|CUT PLAIN|engraveApproved|engravePlain/;
const count = (s, re) => (String(s).match(re) || []).length;

// ═══ 1 · the module, in node ═══
{
  const Seals = require(path.join(root, 'charm-nest-engraving-seals.js')), Mono = require(path.join(root, 'charm-nest-monogram-seals.js')), Sides = require(path.join(root, 'charm-nest-engrave-sides.js')), Mod = require(path.join(root, 'charm-nest-order-seals.js'));
  const T = 1.76e12;
  // (the engraving seals module is the real one for list(); its html needs the page's Seal, so a stand-in draws each seal as a small marker carrying its words)
  const SealStand = { titleOf: s => `${s.how} by ${s.by}`, BASE_SIZE: 50 };
  const sealMark = s => `<span class="seal" data-mark="${s.how}${s.face === 'front' ? ':front' : ''}:${s.by}">${s.face === 'front' ? 'FRONT ENGRAVING' : s.how === 'engravePlain' ? 'CUT PLAIN' : 'BACK ENGRAVING'}</span>`;
  const Eng = Object.assign({}, Seals, { html: job => { const l = Seals.list(job); return l.length ? `<span class="sealRow engravingSeals" data-seal-group>${l.map(sealMark).join('')}</span>` : ''; } });
  const line = (key, extra) => Object.assign({ key, order: { receiptId: key.split('_')[0] }, line: { sku: 'SKU_' + key }, spec: { designSku: 'SKU_' + key } }, extra || {});
  const job = (row, key, o) => Object.assign({ key, row, state: 'review', lines: [], copies: [] }, o || {});
  const apv = (at, by, extra) => Object.assign({ id: `engraveApproved:${at}:${by}`, how: 'engraveApproved', at, by }, extra || {});
  const world = (jobsByLine, extra) => Mod.make(Object.assign({ Engrave: { jobsOf: r => jobsByLine[r.key] || [] }, CNEngravingSeals: Eng, CharmNestEngraveSides: Sides, Seal: SealStand, console: { warn() {} } }, extra || {}));

  // — a back, approved: one seal, labelled; nothing else
  { const r = line('900_1'), M = world({ '900_1': [job(r, '900_1', { state: 'approved', engravingSeals: [apv(T, 'Ana')] })] }).of([r]), html = world({ '900_1': [job(r, '900_1', { state: 'approved', engravingSeals: [apv(T, 'Ana')] })] }).html(M);
    check(M.total === 1 && M.approved === 1 && M.units[0].face === 'back' && M.units[0].done, 'node: an approved back is one approved unit of one: ' + JSON.stringify([M.total, M.approved]));
    check(count(html, /class="ordSeal"/g) === 1 && /Back engraving approved/.test(html) && /data-mark="engraveApproved:Ana"/.test(html) && /by Ana/.test(html) && !/ordSealsCount/.test(html), 'node: the strip has its one seal, "Back engraving approved", who; no count for a single piece');
    check(/data-approved="1" data-total="1"/.test(html) && /role="group"/.test(html) && /title="engraveApproved by Ana"/.test(html), 'node: the strip says what it holds and carries the seal\'s own caption as its title');
  }
  // — front + back, both approved: two seals, "2 of 2"
  { const r = line('901_1'), jobs = [job(r, '901_1~front', { face: 'front', state: 'written', engravingSeals: [apv(T, 'Ben', { face: 'front' })] }), job(r, '901_1', { state: 'approved', engravingSeals: [apv(T + 1, 'Ben')] })], W = world({ '901_1': jobs }), M = W.of([r]), html = W.html(M);
    check(M.total === 2 && M.approved === 2 && M.units.map(u => u.face).join() === 'front,back', 'node: a front and a back are two units, front first');
    check(count(html, /class="ordSeal"/g) === 2 && /Front engraving approved/.test(html) && /Back engraving approved/.test(html) && /data-mark="engraveApproved:front:Ben"/.test(html) && /<b>2 of 2<\/b> approved/.test(html) && /data-complete="1"/.test(html), 'node: two seals (FRONT and BACK), "2 of 2 approved", complete');
  }
  // — front approved, back still to approve: one seal and "1 of 2" (amber); the back's words only when approved
  { const r = line('902_1'), jobs = [job(r, '902_1~front', { face: 'front', state: 'approved', engravingSeals: [apv(T, 'Cai', { face: 'front' })] }), job(r, '902_1', { state: 'review' })], W = world({ '902_1': jobs }), M = W.of([r]), html = W.html(M);
    check(M.total === 2 && M.approved === 1 && count(html, /class="ordSeal"/g) === 1 && /Front engraving approved/.test(html) && !/Back engraving approved/.test(html) && /<b>1 of 2<\/b> approved/.test(html) && /data-complete="0"/.test(html), 'node: half approved: the front\'s seal and "1 of 2 approved" (not complete)');
  }
  // — nothing approved: no strip at all (the existing "needs approval" cues stay untouched: nothing is drawn, nothing invented)
  { const r = line('903_1'), W = world({ '903_1': [job(r, '903_1', { state: 'review' }), job(r, '903_1#D2', { state: 'words' })] }), M = W.of([r]);
    check(M.approved === 0 && M.total === 2 && W.html(M) === '' && M.sig === '', 'node: an order whose engraving is not approved yet shows no seal (and has no signature)');
    const none = world({}).of([r]); check(none.total === 0 && world({}).html(none) === '', 'node: a line with no engraving at all: nothing');
  }
  // — "no engraving" (CUT PLAIN) is not an approved engraving: no seal, not counted
  { const r = line('904_1'), jobs = [job(r, '904_1#D1', { slot: 'D1', state: 'approved', engravingSeals: [apv(T, 'Dee')] }), job(r, '904_1#D2', { slot: 'D2', state: 'skipped', decidedAt: T, decidedBy: 'Dee', engravingSeals: [{ id: 'p', how: 'engravePlain', at: T, by: 'Dee' }] }), job(r, '904_1#D3', { slot: 'D3', state: 'none' })], W = world({ '904_1': jobs }), M = W.of([r]), html = W.html(M);
    check(M.total === 1 && M.approved === 1 && !/CUT PLAIN|engravePlain/.test(html) && count(html, /class="ordSeal"/g) === 1 && !/ordSealsCount/.test(html), 'node: a disc cut plain and a disc with no engraving are not engravings: no seal for them, not counted (the order is not made to look unfinished)');
    const only = world({ '905_1': [job(line('905_1'), '905_1', { state: 'skipped', engravingSeals: [{ id: 'p', how: 'engravePlain', at: T, by: 'Dee' }] })] }).of([line('905_1')]); check(only.approved === 0 && only.total === 0, 'node: an order that is only "no engraving" has no approval seal');
  }
  // — a job that was approved once and waits again (state review) with its old seals: the seals are history, not an approval
  { const r = line('906_1'), W = world({ '906_1': [job(r, '906_1', { state: 'review', engravingSeals: [apv(T, 'Eli')] })] }), M = W.of([r]); check(M.approved === 0 && W.html(M) === '', 'node: a placement that waits for its approval again shows none of its earlier seals');
    const legacy = world({ '907_1': [job(line('907_1'), '907_1', { state: 'written' })] }).of([line('907_1')]); check(legacy.approved === 1 && legacy.units[0].seals[0].how === 'engraveApproved', 'node: a written back with no seal of its own is approved, its legacy seal (the Engrave tab shows it too)'); }
  // — Left / Right (a pair) and discs: a seal for each approved piece, in line order, each named; the approved ones counted of all
  { const r = line('908_1'), jobs = [job(r, '908_1#L', { slot: 'L', state: 'approved', engravingSeals: [apv(T, 'Fay')] }), job(r, '908_1#R', { slot: 'R', state: 'approved', engravingSeals: [apv(T + 5, 'Fay')] })], W = world({ '908_1': jobs }), M = W.of([r]), html = W.html(M);
    check(count(html, /class="ordSeal"/g) === 2 && html.indexOf('Left ear') > 0 && html.indexOf('Left ear') < html.indexOf('Right ear') && /<b>2 of 2<\/b>/.test(html), 'node: an earring pair\'s Left and Right: two seals in order, each named');
    const discs = [1, 2, 3].map(n => job(r, '908_1#D' + n, { slot: 'D' + n, state: n === 2 ? 'approved' : 'review', engravingSeals: n === 2 ? [apv(T, 'Gus')] : [] })), h3 = world({ '908_1': discs }).html(world({ '908_1': discs }).of([r]));
    check(count(h3, /class="ordSeal"/g) === 1 && /Disc 2/.test(h3) && /<b>1 of 3<\/b> approved/.test(h3), 'node: three discs, the second approved: its seal, "Disc 2", "1 of 3 approved"');
  }
  // — several lines of one order: each line's seals in line order, named by the line's SKU; the count is the order's
  { const a = line('909_1'), b = line('909_2'), W = world({ '909_1': [job(a, '909_1', { state: 'approved', engravingSeals: [apv(T, 'Hal')] })], '909_2': [job(b, '909_2', { state: 'review' })] }), M = W.of([a, b]), html = W.html(M);
    check(M.total === 2 && M.approved === 1 && /<b>1 of 2<\/b> approved/.test(html) && /SKU_909_1/.test(html) && !/SKU_909_2/.test(html), 'node: two lines, one approved: "1 of 2 approved", the seal named by its line\'s SKU');
  }
  // — the line's own record when Engrave holds no job (a line read from the records)
  { const r = line('910_1', { engrave: { needed: true, state: 'written', approved: true } }), W = world({}), M = W.of([r]); check(M.approved === 1 && M.total === 1, 'node: no job but the line\'s own record says approved: its seal');
    const r2 = line('911_1', { engrave: { needed: false, state: 'none', approved: true } }); check(world({}).of([r2]).total === 0, 'node: a record that says "no engraving needed" is no engraving'); }
  // — escaping
  { const r = line('912_1'), W = world({ '912_1': [job(r, '912_1', { state: 'approved', engravingSeals: [apv(T, '<img src=x onerror=1>')] })] }), html = W.html(W.of([r])); check(!/<img src=x/.test(html.replace(/data-mark="[^"]*"/g, '').replace(/title="[^"]*"/g, '')) && /&lt;img/.test(html), 'node: a name is escaped'); }
  // — signatures: change exactly when the strip does
  { const r = line('913_1'), j = job(r, '913_1', { state: 'review' }), W = world({ '913_1': [j] }), s0 = W.of([r]).sig; j.state = 'approved'; j.engravingSeals = [apv(T, 'Ivy')]; const s1 = W.of([r]).sig, s1b = W.of([r]).sig; j.engravingSeals = [apv(T, 'Ivy'), apv(T + 9, 'Ivy')]; const s2 = W.of([r]).sig;
    check(s0 === '' && s1 && s1 === s1b && s2 !== s1, 'node: the signature is empty while nothing is approved, stable while nothing changes, new with a new seal'); }
  // — no Engrave, no seals module: nothing, nothing thrown
  { const r = line('914_1'); const W = Mod.make({}); check(W.of([r]).total === 0 && W.html(W.of([r])) === '' && W.html(null) === '' && W.of(null).total === 0 && W.of(r).total === 0, 'node: a page without the readers shows nothing'); }
  // — monogram: its own seals only, never an engraving seal, never a request
  { const ml = (key, qty) => line(key, { spec: { special: { monogram: true }, quantity: qty || 1, designSku: 'MONOGRAM_1' } });
    const stamps = { '920_1|': [{ id: 'monogramApproved:2000:Ana', how: 'monogramApproved', at: 2000, by: 'Ana', letters: 'APV' }], '921_1|C1': [{ id: 'monogramApproved:3000:Ben', how: 'monogramApproved', at: 3000, by: 'Ben', letters: 'JQX' }] };
    const calls = { ensure: 0, load: 0, events: 0 };
    const ui = { sealsOf: (r, slot) => (stamps[(r.parentRow || r).key + '|' + (slot || '')] || []).map(s => Object.assign({}, s)), sealHtml: (r, slot) => { const a = ui.sealsOf(r, slot); return a.length ? `<span class="sealRow mwSeal" data-seal-group>${a.map(s => `<span class="seal" data-mono="${s.id}">MONOGRAM</span>`).join('')}</span>` : ''; },
      service: () => ({ pieces: r => (r.spec.quantity > 1 ? [1, 2].map(n => ({ key: r.key + '#C' + n, slot: 'C' + n, n, of: 2, lineKey: r.key, poolId: r.key + '_' + n })) : [{ key: r.key, slot: null, n: 1, of: 1, lineKey: r.key, poolId: r.key + '_1' }]) }),
      status: (r, slot) => { const ap = !!(stamps[r.key + '|' + (slot || '')] || []).length; return { state: ap ? 'approved' : 'ready', label: ap ? 'Approved' : 'Ready to approve', letters: 'JQX', approved: ap }; },
      ensureSeals: () => { calls.ensure++; }, loadSeals: () => { calls.load++; }, events: () => { calls.events++; } };
    const MS = Mono.make({ CharmNestMonogramUI: ui, console: { warn() {} } });
    // (Engrave HOLDS a job object for the monogram line's key, as a stand-in would: the module must never read it as an engraving)
    const W = Mod.make({ Engrave: { jobsOf: () => [{ key: 'x', state: 'approved' }] }, CNEngravingSeals: Eng, CNMonogramSeals: MS, CharmNestEngraveSides: Sides, Seal: SealStand, console: { warn() {} } });
    const m1 = ml('920_1', 1), M1 = W.of([m1]), h1 = W.html(M1);
    check(M1.total === 1 && M1.approved === 1 && M1.units[0].kind === 'monogram' && count(h1, /class="ordSeal"/g) === 1 && /Monogram approved/.test(h1) && /data-mono="monogramApproved:2000:Ana"/.test(h1) && /A P V|J Q X/.test(h1) && !BAD.test(h1.replace(/Monogram approved/g, '')), 'node: an approved monogram: its MONOGRAM seal and words, never an engraving seal (even with an Engrave job in the way): ' + h1.slice(0, 100));
    const m2 = ml('921_1', 2), M2 = W.of([m2]), h2 = W.html(M2);
    check(M2.total === 2 && M2.approved === 1 && /Piece 1 of 2/.test(h2) && !/Piece 2 of 2/.test(h2) && /<b>1 of 2<\/b> approved/.test(h2) && count(h2, /class="ordSeal"/g) === 1, 'node: a monogram of two pieces, the first approved: its seal, "Piece 1 of 2", "1 of 2 approved"');
    const m3 = ml('922_1', 1), M3 = W.of([m3]); check(M3.approved === 0 && M3.total === 1 && W.html(M3) === '', 'node: a monogram nobody approved: none');
    check(calls.ensure === 0 && calls.load === 0 && calls.events === 0, 'node: the module never asks the timeline (ensureSeals / loadSeals / events not called)');
    // approved by the widget but its seal is not in memory yet: no seal, and still no request
    const W2 = Mod.make({ CNMonogramSeals: Mono.make({ CharmNestMonogramUI: Object.assign({}, ui, { sealsOf: () => [], sealHtml: () => '' }), console: { warn() {} } }), Seal: SealStand, console: { warn() {} } }), a1 = W2.of([m1]);
    check(a1.approved === 0 && W2.html(a1) === '' && calls.ensure === 0, 'node: an approved piece whose seal is not in memory shows none until it is (nothing is asked for it)');
    // a piece changed since its approval waits for its new one
    const W3 = Mod.make({ CNMonogramSeals: Mono.make({ CharmNestMonogramUI: Object.assign({}, ui, { status: () => ({ state: 'ready', label: 'Ready to approve', letters: 'APV', approved: false }) }), console: { warn() {} } }), Seal: SealStand, console: { warn() {} } });
    check(W3.of([m1]).approved === 0 && W3.html(W3.of([m1])) === '', 'node: a monogram changed since its approval shows its earlier seal as no approval');
    // a monogram line beside an engraved one: both, in line order, one count
    const eng = line('923_1'), W4 = Mod.make({ Engrave: { jobsOf: r => (r.key === '923_1' ? [job(eng, '923_1', { state: 'approved', engravingSeals: [apv(T, 'Jo')] })] : []) }, CNEngravingSeals: Eng, CNMonogramSeals: MS, CharmNestEngraveSides: Sides, Seal: SealStand, console: { warn() {} } }), M4 = W4.of([eng, m1]);
    check(M4.total === 2 && M4.approved === 2 && M4.units.map(u => u.kind).join() === 'engrave,monogram', 'node: an engraved line and a monogram line of one order: both, in line order');
  }
}

// ═══ 2 · the source ═══
{
  const bridge = read('charm-nest-bridge.js'), html = read('charm-nest-1.html'), build = read('scripts/build-public.cjs'), css = read('charm-nest-activity.css'), mod = read('charm-nest-order-seals.js');
  const code = s => s.replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/[^\n]*/g, '');
  const fn = (src, start, end) => { const i = src.indexOf(start), j = src.indexOf(end, i + 10); assert(i > 0 && j > i, 'markers ' + start); return src.slice(i, j); };
  const group = fn(bridge, 'function groupNode(', 'function renderBody()'), single = fn(bridge, 'const { sp, m, st, shown, due, where, fc, attn, why, gateBtn, mail, team, sealRec, seals, held, rel } = lineView(r)', 'const number=node.querySelector');
  check(/for \(const v of V\) v\.e = engLine\(v\.r\)/.test(group) && /v\.e \? v\.e\.sig : ""/.test(group) && /lineSealsHtml\(v\.r, v\.e, null\)/.test(group) && !/CNOrderSeals\.html|cardSealHtml/.test(code(group)), 'source: a grouped order\'s pieces each draw their own Engraving-row lines and seals cell, with the line\'s signature in the stamp; no strip in the head');
  check(/e = engLine\(r\)/.test(single) && /e \? e\.sig : ""\]\)/.test(single) && /lineIdHtml\(r, e,/.test(single) && !/CNOrderSeals\.html|cardSealHtml/.test(code(single)), 'source: a one-line order\'s card / row draws the Engraving-row lines and seals cell, with the line\'s signature in its stamp; no strip');
  check(/function listLine\(row\)/.test(bridge) && /return \{ listLine, openMonogram/.test(bridge), 'source: the Engrave tab publishes listLine, the one reading both lists draw a line from');
  check(/const monoMoved|function monoMoved/.test(bridge) && /monoTimer = 0; try \{ Orders\.render\(\)/.test(bridge) && /,render,\(\)=>Orders\.render\(\)\]\)try\{show\(\);\}/.test(bridge), 'source: an engraving approval and a monogram change both repaint the Orders list (Orders.render: coalesced, drawn only while the tab is on screen)');
  check(/const cardSeals = rows =>/.test(bridge) && !/ensureSeals/.test(code(group)) && !/CNMonogramSeals|CNEngravingSeals/.test(code(group)) && !/CNMonogramSeals|CNEngravingSeals/.test(code(single)), 'source: the card builders go through CNOrderSeals only (no seal module, no ensureSeals, from a card)');
  const c = code(mod);
  check(!/setTimeout|setInterval|requestAnimationFrame|addEventListener|MutationObserver|fetch\(|XMLHttpRequest|onSnapshot|api\(|ensureSeals|loadSeals|localStorage|\.events\(/.test(c), 'source: the module makes no read, timer or listener and never asks the timeline');
  check(/charm-nest-order-seals\.js\?v=\d+-cs1/.test(html) && /"charm-nest-order-seals\.js"/.test(build) && html.indexOf('charm-nest-order-seals.js') > html.indexOf('charm-nest-monogram-seals.js') && html.indexOf('charm-nest-order-seals.js') < html.indexOf('charm-nest-bridge.js?v='), 'source: the module is loaded by the page after the two seal modules and before the bridge, and is in the published asset list');
  for (const [file, re] of [['charm-nest-bridge.js', /charm-nest-bridge\.js\?v=[^"]*-ol1"/], ['charm-nest-activity.css', /charm-nest-activity\.css\?v=[^"]*-ol1"/]]) check(re.test(html), 'source: the cache-buster of ' + file + ' is bumped (-ol1)');
  check(/:root\{--list-seal:57px\}/.test(css) && /\.doneRow:is\(\.placementRow,\.decidedRow\) \.sealRow\{margin:0;padding:0;--seal-fit:var\(--list-seal\)!important\}/.test(css) && /\.egPlacementSeals \.sealRow\{margin:0;padding:0;max-width:none;--seal-fit:var\(--list-seal\)!important\}/.test(css), 'source: ONE list seal size (--list-seal: 57 px = 1.5 x 38), used by the Engraving rows and the Orders rows and cards');
}

// ═══ 3 · the real page ═══
(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  try { require(path.join(pwDir, 'playwright-core')); } catch (_) { console.log('order-card-seals: no playwright-core, the browser part is skipped'); finish(); return; }
  process.env.PW_DIR = pwDir;
  const R = require('./_engraveRealWorld.cjs'), V = (name, value) => ({ name, value });
  const MONO_T = 'Monogram Necklace Personalized Jewelry Initials Pendant';
  const mono = (rid, buyer, pers, qty, m) => ({ rid, sku: 'MONOGRAM_5351', listing: '997905351', title: MONO_T, buyer, placed: { day: 0, h: 10, m }, ship: 5, qty, variations: [V('Metal choice', 'Gold')], personalization: [pers] });
  Object.assign(R.ORDERS, { m1: mono('4290000131', 'Name Eleven', 'APV', 1, 20), m2: mono('4290000132', 'Name Twelve', 'JQX', 2, 30), m3: mono('4290000133', 'Name Thirteen', 'OSR', 1, 40), m4: mono('4290000134', 'Name Fourteen', 'KLM', 1, 50) });
  for (const k of ['m1', 'm2', 'm3', 'm4']) R.EXPECT_JOBS[k] = 0;
  // an order of TWO lines (one card for the order, ENGONE): the line with the words, and a second charm of the same design
  R.ORDERS.g2 = Object.assign({}, R.ORDERS.back, { rid: '4290000141', listing: '1890000141', buyer: 'Name Fifteen', placed: { day: 0, h: 11, m: 0 }, more: [{ sku: 'COMET_9427', title: R.ORDERS.back.title, variations: [V('Metal choice', 'Silver'), V('Font Choice', 'Stylish')] }] });
  R.EXPECT_JOBS.g2 = 1;
  fs.mkdirSync(SHOTS, { recursive: true });
  const w = await R.world({ viewport: { width: 1700, height: 1000 }, shots: SHOTS });
  if (!w) { console.log('order-card-seals: no browser, the page part is skipped'); finish(); return; }
  const page = w.page, sleep = ms => new Promise(r => setTimeout(r, ms));
  const req = []; page.on('request', rq => { if (/\/\.netlify\/functions|firestore|googleapis|firebase/i.test(rq.url())) req.push(rq.method() + ' ' + rq.url().replace(/^https?:\/\/[^/]+/, '') + ' ' + String(rq.postData() || '').slice(0, 90)); });
  try {
    const made = await R.build(w, ['back', 'front', 'both', 'discs', 'g2', 'm1', 'm2', 'm3', 'm4']);
    check(made.back.jobs.length === 1 && made.both.jobs.length === 2 && made.discs.jobs.length === 3 && made.front.jobs.length === 1, 'the real intake made the jobs: back 1, front 1, front + back 2, three discs 3');
    const RID = n => R.ORDERS[n].rid;
    // ── approvals, as the page holds them: the job's state and its seal (Engrave.approve ends so), the monogram widget's status and the stamps its timeline events carry ──
    await page.evaluate(({ rids }) => {
      const T = Date.now(), rowOf = rid => Orders.rows().filter(r => r.order.receiptId === rid);
      const approve = (job, by, at) => { job.state = 'approved'; job.approvedBy = by; job.approvedAt = at; CNEngravingSeals.add(job, 'engraveApproved', by, at); job.row.engrave = Object.assign(job.row.engrave || {}, { needed: true, state: 'approved', approved: true }); };
      window.__approve = approve;
      for (const j of Engrave.jobsOf(rowOf(rids.back)[0])) approve(j, 'Ana Test', T - 3600e3);
      Engrave.jobsOf(rowOf(rids.both)[0]).forEach((j, i) => approve(j, 'Ben Test', T - 1800e3 - i * 1000));
      approve(Engrave.jobsOf(rowOf(rids.discs)[0])[0], 'Cai Test', T - 900e3);     // only disc 1 of 3
      { const g = rowOf(rids.g2); approve(Engrave.jobsOf(g[0])[0], 'Hal Test', T - 700e3); Engrave.ensureJob(g[1]); }     // two lines: the first approved, the second has its job and waits
      window.__ma = {}; window.__ensure = 0; window.__tl = 0;
      const svc = Engrave.monogramUI(), ens = svc.ensureSeals, lo = svc.loadSeals;
      svc.ensureSeals = (...a) => { __ensure++; (window.__stacks = window.__stacks || []).push(String(new Error().stack).split('\n').slice(2, 7).map(x => x.trim().replace(/\?v=[^:)\s]*/, '').replace(/\(.*\//, '(')).join(' < ')); return ens && ens.apply(svc, a); }; svc.loadSeals = (...a) => { __ensure++; return lo && lo.apply(svc, a); };
      if (window.OrderTimeline && OrderTimeline.get) { const g = OrderTimeline.get; OrderTimeline.get = function () { __tl++; return g.apply(this, arguments); }; }
      svc.status = (row, ask) => { const r = row.parentRow || row; if (!(r.spec && r.spec.special && r.spec.special.monogram)) return null; const of = Math.max(1, +(r.spec.quantity || 1)), slot = typeof ask === 'string' && /^C\d+$/.test(ask) ? ask : of > 1 ? 'C1' : null, ap = !!__ma[r.key + '|' + (slot || '')];
        return { state: ap ? 'approved' : 'ready', label: ap ? 'Approved' : 'Ready to approve', letters: r.spec.quantity > 1 ? 'JQX' : '', approved: ap, dirty: false, sent: false, rid: String(r.order.receiptId), lineKey: r.key, pieceKey: slot ? r.key + '#' + slot : r.key, slot, n: slot ? +slot.slice(1) : 1, of }; };
      const m1 = rowOf(rids.m1)[0], m2 = rowOf(rids.m2)[0], m4 = rowOf(rids.m4)[0];
      __ma[m1.key + '|'] = 1; __ma[m2.key + '|C1'] = 1; __ma[m4.key + '|'] = 1;      // m4: approved by the widget, but no seal for it in memory (approved on another computer)
      svc.feedEvents(rids.m1, [{ type: 'monogramApproved', id: 'ma1', at: T - 600e3, by: 'Dee Test', lineKey: m1.key, data: { n: 1, of: 1, letters: 'APV' } }]);
      svc.feedEvents(rids.m2, [{ type: 'monogramApproved', id: 'ma2', at: T - 500e3, by: 'Eve Test', lineKey: m2.key, data: { n: 1, of: 2, letters: 'JQX' } }]);
      window.__ensure = 0; CN.setMode('orders'); Orders.render();
    }, { rids: { back: RID('back'), both: RID('both'), discs: RID('discs'), g2: RID('g2'), m1: RID('m1'), m2: RID('m2'), m4: RID('m4') } });
    await R.settle(page, 900);
    const cards = () => page.evaluate(() => Object.fromEntries([...document.querySelectorAll('#ordItems > [data-rid]')].map(n => {
      const cells = [...n.querySelectorAll('.egPlacementSeals')], seals = cells.flatMap(c => [...c.querySelectorAll('.seal')]);
      return [n.dataset.rid, {
        n: seals.length, text: n.innerText.replace(/\s+/g, ' '), html: n.innerHTML, cls: n.className,
        actions: seals.map(x => { const svg = x.querySelector('svg') || x; try { return JSON.parse(svg.getAttribute('data-seal-model')).action; } catch (_) { return '?'; } }),
        chips: cells.flatMap(c => [...c.querySelectorAll('.egEarSeals .egPiece')].map(x => x.textContent.trim().toUpperCase())),
        lines: [...n.querySelectorAll('.ordEngId .egEar')].map(x => x.innerText.replace(/\s+/g, ' ').trim()),
        label: [...n.querySelectorAll('.ordEngId > .purchaseLabel')].map(x => x.textContent.trim()).join('|'), mode: (n.querySelector('.ordEngId .egMode') || {}).textContent || '',
        strip: !!n.querySelector('.ordSeals, .ordSealsCount'), headSeals: n.querySelectorAll('.ogHead .egPlacementSeals .seal, .ogHead .ordSeals').length,
        sealPx: seals.length ? seals[0].offsetWidth : 0,   // (the layout size: the seal is drawn turned a few degrees, so its bounding box is wider) clipped: seals.some(x => { const b = x.getBoundingClientRect(), c = n.getBoundingClientRect(); return b.top < c.top - 1 || b.bottom > c.bottom + 1 || b.right > c.right + 1; })
      }];
    })));
    const verify = async mode => {
      const c = await cards();
      const at = n => c[RID(n)];
      check(!!at('back') && at('back').n === 1 && at('back').actions.join() === 'BACK ENGRAVING' && at('back').label === 'Words on the back' && !at('back').strip, `${mode}: an approved back: "Words on the back" and its BACK ENGRAVING seal in the row's seals cell (no strip)`);
      check(!!at('front') && at('front').n === 0 && at('front').label === 'Words on the front', `${mode}: an order whose engraving is not approved shows no seal, and says "Words on the front"`);
      check(!!at('both') && at('both').n === 2 && at('both').actions.join() === 'FRONT ENGRAVING,BACK ENGRAVING' && at('both').chips.join() === 'FRONT,BACK' && /front \+ back/i.test(at('both').mode) && at('both').label === 'Words on the front and back' && at('both').lines.length === 2, `${mode}: front + back: the "Front + back" chip, a FRONT and a BACK line, and a chip + seal for each, FRONT then BACK: ${JSON.stringify([at('both').chips, at('both').lines])}`);
      check(!!at('discs') && at('discs').n === 1 && at('discs').lines.length === 3 && /^DISC 1 OF 3/i.test(at('discs').lines[0]) && at('discs').chips.join() === 'DISC 1 OF 3' && at('discs').actions.join() === 'BACK ENGRAVING', `${mode}: three discs, one approved: one line per disc, and the seal beside its own "DISC 1 OF 3" chip: ${JSON.stringify([at('discs').lines, at('discs').chips])}`);
      check(!!at('g2') && /\bordGroup\b/.test(at('g2').cls) && at('g2').n === 1 && at('g2').headSeals === 0 && !at('g2').strip && at('g2').actions.join() === 'BACK ENGRAVING', `${mode}: an order of two lines is one card: the approved piece carries its seal on its own row, nothing in the head`);
      check(!!at('m1') && at('m1').n === 1 && at('m1').actions.join() === 'MONOGRAM' && at('m1').label === 'The letters' && !/BACK ENGRAVING|FRONT ENGRAVING/.test(at('m1').html), `${mode}: an approved monogram: "The letters" and its MONOGRAM seal, never a BACK ENGRAVING one`);
      check(!!at('m2') && at('m2').n === 1 && at('m2').actions.join() === 'MONOGRAM' && at('m2').chips.join() === 'PIECE 1 OF 2' && at('m2').lines.length === 2, `${mode}: a monogram of two pieces, one approved: a line per piece, the seal beside "PIECE 1 of 2": ${JSON.stringify(at('m2').chips)}`);
      check(!!at('m3') && at('m3').n === 0, `${mode}: a monogram nobody approved shows no seal`);
      check(!!at('m4') && at('m4').n === 0, `${mode}: a monogram approved on another computer, whose seal is not in memory, shows none (and asks for none)`);
      check(at('back').sealPx === 57 && at('m1').sealPx === 57 && at('both').sealPx === 57, `${mode}: the engraving and monogram seals are the list size, 57 px (1.5 x 38): ${at('back').sealPx} / ${at('m1').sealPx} / ${at('both').sealPx}`);
      check(Object.values(c).every(x => !x.clipped), `${mode}: no seal runs out of its row or card`);
      check(Object.values(c).every(x => !x.strip), `${mode}: no "N of M approved" strip on any row or card`);
      return c;
    };
    check(await page.evaluate(() => !!(window.CNOrderSeals && CNOrderSeals.of && CNOrderSeals.html)), 'the page loads charm-nest-order-seals.js (CNOrderSeals)');
    check(await page.evaluate(() => Orders.view().view || (CN.S.settings.orderView || 'list')) !== 'cards', 'the Orders tab is in its list mode');
    const list = await verify('list mode');
    await R.shot(w, 'order-card-seals-list');
    await page.evaluate(() => { Orders.view().view = 'cards'; Orders.render(); });
    await R.settle(page, 900);
    const cardsMode = await verify('cards mode');
    check(Object.values(cardsMode).some(x => /\bocard\b/.test(x.cls)), 'cards mode really drew cards (.ocard)');
    await R.shot(w, 'order-card-seals-cards');
    // ── live: the card is repainted when the data changes, through the existing repaint (Orders.render) ──
    await page.evaluate(({ rid }) => { window.__renders = 0; const rows = Orders.rows().filter(r => r.order.receiptId === rid); __approve(Engrave.jobsOf(rows[0])[0], 'Fay Test', Date.now()); Orders.render(); }, { rid: RID('front') });
    await R.settle(page, 700);
    let c = await cards();
    check(c[RID('front')].n === 1 && c[RID('front')].actions.join() === 'FRONT ENGRAVING', 'live: an engraving approved in memory shows its seal on the card at the next repaint (no reload): ' + c[RID('front')].actions.join());
    await page.evaluate(({ rid }) => { const j = Engrave.jobsOf(Orders.rows().filter(r => r.order.receiptId === rid)[0]).filter(x => x.face !== 'front')[0]; j.state = 'review'; Orders.render(); }, { rid: RID('both') });
    await R.settle(page, 700);
    c = await cards();
    check(c[RID('both')].lines.length === 2 && !/approved/i.test(c[RID('both')].lines[1].replace(/^BACK\s*/i, '').replace(/NAME|ONE|\//g, '')) && c[RID('both')].actions[0] === 'FRONT ENGRAVING', 'live: a back that waits for its approval again says its new state on its own line (its earlier seal stays as history, as on the Engraving row); the FRONT keeps its own: ' + JSON.stringify([c[RID('both')].lines, c[RID('both')].chips]));
    await page.evaluate(({ k, rid }) => { __ma[k + '|'] = 1; Engrave.monogramUI().feedEvents(rid, [{ type: 'monogramApproved', id: 'ma3', at: Date.now(), by: 'Gus Test', lineKey: k, data: { n: 1, of: 1, letters: 'OSR' } }]); Orders.render(); },
      { k: made.m3.key, rid: RID('m3') });
    await R.settle(page, 700);
    c = await cards();
    check(c[RID('m3')].n === 1 && c[RID('m3')].actions.join() === 'MONOGRAM', 'live: a monogram approved shows its seal at the next repaint');
    // the cards that did not change are not built again (a card is drawn again only when its seals changed)
    await page.evaluate(({ rid }) => { document.querySelector(`#ordItems > [data-rid="${rid}"]`).dataset.mark = 'kept'; Orders.render(); }, { rid: RID('back') });
    await R.settle(page, 500);
    check(await page.evaluate(({ rid }) => document.querySelector(`#ordItems > [data-rid="${rid}"]`).dataset.mark === 'kept', { rid: RID('back') }), 'a card whose seals did not change is not drawn again (cached by its signature)');
    // ── nothing was asked for BY THE STRIP: no request, no timeline read, no ensureSeals ──
    // (the page's own redraw of the list already asks the service for the stamp of a monogram that is approved with none in memory, m4: charm-nest-monogram-ui.js ensureSeals, rate-limited by the service to 4 asks an order, through
    //  the order model that every card's Send to Review reads; that is not the strip's: it is measured here by itself, and a full redraw of every card with the strips off and then on must ask no more with them on)
    // (the page reads on its own clock too, a placement read of the orders and the cancelled ids: those are not the strip's; what the strip could ask for is the order timeline, the monogram's stamp or a back file: none of those may be asked)
    const ours = r => /timeline|monogram|engrave|seal|backPreview|backPool/i.test(r), ops = rs => [...new Set(rs.map(r => (/"op":"(\w+)"/.exec(r) || [])[1] || r.split(' ')[1]))];
    const win = async (fn, ms) => { const r0 = req.length; await page.evaluate(() => { window.__ensure = 0; window.__tl = 0; }); await fn(); await R.settle(page, ms || 900); const a = await page.evaluate(() => ({ ensure: window.__ensure, tl: window.__tl })), rs = req.slice(r0); return { ensure: a.ensure, tl: a.tl, asked: rs.filter(ours).length, ops: ops(rs) }; };
    const alone = await win(() => page.evaluate(() => { const rows = Orders.rows(); for (let i = 0; i < 20; i++) CNOrderSeals.html(CNOrderSeals.of(rows)); }), 150);
    check(alone.ensure === 0 && alone.tl === 0 && alone.asked === 0, `the strip's own work (CNOrderSeals.of + html over every line, 20 times) calls no ensureSeals, reads no timeline, asks for nothing: ${JSON.stringify(alone)}`);
    const off = await win(() => page.evaluate(() => { window.__cns = window.CNOrderSeals; window.CNOrderSeals = { of: () => null, html: () => '' }; Orders.render(); }));
    const on = await win(() => page.evaluate(() => { window.CNOrderSeals = window.__cns; Orders.render(); }));
    check(on.ensure <= off.ensure && on.tl <= off.tl && on.asked <= off.asked, `a full redraw of every card with the strips asks no more than one without them: with ${JSON.stringify(on)}, without ${JSON.stringify(off)}`);
    check((await cards())[RID('back')].n === 1, 'and the seals are there after it');
    const lineWork = await win(() => page.evaluate(() => { const rows = Orders.rows(); for (let i = 0; i < 20; i++) for (const r of rows) Engrave.listLine(r); }), 150);
    check(lineWork.ensure === 0 && lineWork.tl === 0 && lineWork.asked === 0, `Engrave.listLine over every line, 20 times, calls no ensureSeals, reads no timeline, asks for nothing: ${JSON.stringify(lineWork)}`);
    console.log('   (who asks for the stamp of m4, the page\'s own path):', JSON.stringify(((await page.evaluate(() => window.__stacks || [])).slice(0, 1))).slice(0, 400));
    // ── a long list stays fast ──
    const ms = await page.evaluate(() => { const rows = Orders.rows(); const t0 = performance.now(); for (let i = 0; i < 40; i++) CNOrderSeals.of(rows); return (performance.now() - t0) / 40; });
    check(ms < 25, `CNOrderSeals.of over every line on the page costs ${ms.toFixed(2)} ms (map reads, no scan)`);
    console.log('   page errors:', JSON.stringify(w.errors));
    check(w.errors.length === 0, 'no page errors');
    // the screenshot: list mode, the approved orders in view
    await page.evaluate(() => { Orders.view().view = 'list'; Orders.render(); document.getElementById('ordBody').scrollTop = 0; });
    await R.settle(page, 900);
    const shot = await R.shot(w, 'order-card-seals');
    console.log('   screenshot:', shot);
    // ── SEAL15: the Engrave tab's list rows draw the same seals at the same 57 px; nothing is clipped ──
    await page.evaluate(() => { CN.setMode('engrave'); Engrave.restoreView({ tab: 'approved', list: true, chosen: true, focus: null, pin: null, openDone: null, q: '' }); Engrave.render(); });
    await R.settle(page, 1200);
    const eg = await page.evaluate(() => { const ss = [...document.querySelectorAll('.egPlacementList .egPlacementSeals .seal')]; return { n: ss.length, px: [...new Set(ss.map(x => x.offsetWidth))], clipped: ss.filter(x => { const row = x.closest('.doneRow'), b = x.getBoundingClientRect(), c = row.getBoundingClientRect(); return b.top < c.top - 1 || b.bottom > c.bottom + 1 || b.right > c.right + 1; }).length }; });
    check(eg.n >= 4 && eg.px.length === 1 && eg.px[0] === 57 && eg.clipped === 0, `the Engrave tab's list seals are 57 px too, none clipped: ${JSON.stringify(eg)}`);
    console.log('   screenshot:', await R.shot(w, 'engrave-list-seals'));
  } catch (e) { fails.push('crashed: ' + (e && e.stack || e)); console.log('  ✗ crashed: ' + (e && e.stack || e)); try { await R.shot(w, 'crash'); } catch (_) {} }
  finally { await Promise.resolve(w.close()).catch(() => {}); }
  finish();
})().catch(e => { console.error(e); process.exit(1); });

function finish() {
  console.log(`\norder-card-seals: ${nCheck - fails.length}/${nCheck} checks passed` + (fails.length ? ' · FAILED: ' + fails.join(' | ') : ''));
  process.exit(fails.length ? 1 : 0);
}
