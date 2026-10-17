// MONOSEAL2 (Paul, 17 Oct 2026): "ensure that the monogram stamp and seal animations appear in all the same places as the engraving stamp and seal animations already do so that way everything is 100% consistent."
// THE SURFACES the monogram widget does not draw itself (charm-nest-monogram-seals.js; the seal of the Approve button in the Engrave card and in the order window's .engMono card, the Timeline and the rail are the CORE's):
//   S1  the Engrave tab's list row of a monogram piece: the engraved row's own `.egPlacementSeals` cell, the seal(s) at rest; the stand-in carries `seals` so the Engrave lists sort and date-filter it by its approval time
//   S2  the order window's Overview: a Monogram fact (chip "Monogram", its card and its Details cell) with the piece's seal(s); engraveOf(line) stays untouched (no r.engrave on a monogram line)
//   S3  the Sheet tab and the Sheet window's piece panel: the Monogram panel (the letters, "Approved by X" + the seal(s), or "Monogram waits for approval in Engraving", View in Engraving -> EngraveLink.openMonogram)
// For every surface: an APPROVED monogram shows its seal; an UNAPPROVED one shows none; a piece of a line of several pieces shows its OWN seal only; NEVER a legacy "BACK ENGRAVING" seal (CNEngravingSeals invents one for any
// approved object with no seals); an ENGRAVED piece's row / cell / panel is what it was; a page with no reader shows nothing at all and nothing breaks.
// The CORE (charm-nest-monogram-ui.js, another branch) owns the readers `CharmNestMonogramUI.sealsOf(row, slot)` and `.sealHtml(row, slot)`: here they are STAND-INS installed on the page, and the widget service's status of a piece is a stand-in too
// (approved or not), so this test is about the surfaces and not about building a monogram.
//   1  node: the module (charm-nest-monogram-seals.js) over fake roots: the adapter, the lists, the fact, the panel, the pieces, CNListActivity over a stand-in, the wiring in the page's files
//   2  the REAL page over the repo's fake site (the order-engraving-everywhere.cjs world): S1, S2, S3 (Sheet tab and Sheet window), the page with no reader, no page errors, no network write
//   node tests/charm-nest/monogram-seal-surfaces.cjs [playwright-core dir]        (PW_DIR=<playwright node_modules> CHROMIUM=<chrome> SHOTS=<dir>)
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const SHOTS = process.env.SHOTS || '/tmp/mu/MSEAL';
const read = f => fs.readFileSync(path.join(root, f), 'utf8');
const fails = []; let nCheck = 0;
const check = (ok, msg) => { nCheck++; if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
const BAD = /BACK ENGRAVING|FRONT ENGRAVING|CUT PLAIN|engraveApproved|engravePlain/;

// ═══ 1 · the module, in node ═══
{
  const Seals = require(path.join(root, 'charm-nest-monogram-seals.js')), Act = require(path.join(root, 'charm-nest-activity.js'));
  const line = (key, qty) => ({ key, order: { receiptId: key.split('_')[0] }, line: {}, spec: { special: { monogram: true }, quantity: qty || 1 } });
  const L1 = line('900_1', 1), L2 = line('900_2', 2), PLAIN = { key: '900_3', order: { receiptId: '900' }, spec: { designSku: 'X_1' } };
  const stamps = { '900_1|': [{ id: 'monogramApproved:2000:Ana', how: 'monogramApproved', at: 2000, by: 'Ana' }], '900_2|C1': [{ id: 'monogramApproved:3000:Ben', how: 'monogramApproved', at: 3000, by: 'Ben', letters: 'JQX' }] };
  const stand = o => Object.assign({
    sealsOf: (r, slot) => (stamps[(r.parentRow || r).key + '|' + (slot || '')] || []).map(s => Object.assign({}, s)),
    sealHtml: (r, slot) => { const a = stand().sealsOf(r, slot); return a.length ? `<span class="sealRow stand" data-seal-group>${a.map(s => `<span class="seal" data-standin="${s.id}">M</span>`).join('')}</span>` : ''; },
    service: () => ({ pieces: r => (r.spec.quantity > 1 ? [1, 2].map(n => ({ key: r.key + '#C' + n, slot: 'C' + n, n, of: 2, lineKey: r.key, poolId: r.key + '_' + n })) : [{ key: r.key, slot: null, n: 1, of: 1, lineKey: r.key, poolId: r.key + '_1' }]) }),
    status: (r, slot) => { const ap = !!(stamps[r.key + '|' + (slot || '')] || []).length; return { state: ap ? 'approved' : 'ready', label: ap ? 'Approved' : 'Ready to approve', letters: 'JQX', approved: ap }; }
  }, o || {});
  const world = (ui, extra) => Seals.make(Object.assign({ CharmNestMonogramUI: ui, console: { warn() {} } }, extra || {}));
  const S = world(stand());

  check(S.isMonogram(L1) && S.isMonogram({ parentRow: L1 }) && !S.isMonogram(PLAIN) && !S.isMonogram(null), 'node: a monogram line is the orders module\'s special.monogram (a piece\'s parentRow too); a plain line and nothing are not');
  check(S.READ.seals === 'sealsOf' && S.READ.html === 'sealHtml' && S.READ.how === 'monogramApproved', 'node: the ONE place that names the CORE\'s readers and its stamp is READ: ' + JSON.stringify(S.READ));
  // the lists
  const a1 = S.list(L1, null);
  check(a1.length === 1 && a1[0].at === 2000 && a1[0].by === 'Ana' && a1[0].how === 'monogramApproved', 'node: list() is the reader\'s stamps: ' + JSON.stringify(a1));
  check(S.list(L1).length === 1 && S.list({ parentRow: L1 }, null).length === 1, 'node: the slot is optional; a piece\'s row reads its line');
  check(S.list(L2, 'C1').length === 1 && S.list(L2, 'C2').length === 0 && S.list(L2).length === 0, 'node: a piece of a line of several has its OWN seals (piece 1 yes, piece 2 none)');
  check(S.list(PLAIN, null).length === 0 && S.list(null).length === 0, 'node: a line that is no monogram has none');
  const noisy = world(stand({ sealsOf: () => [{ id: 'a', how: 'monogramApproved', at: 9, by: 'Z' }, { id: 'a', how: 'monogramApproved', at: 9, by: 'Z' }, { id: 'b', how: 'engraveApproved', at: 1, by: 'E' }, { how: 'monogramApproved', at: 3, by: 'Y' }, null, 'x'] }));
  const nl = noisy.list(L1);
  check(nl.length === 2 && nl[0].at === 3 && nl[1].id === 'a' && nl[0].id === 'monogramApproved:3:Y', 'node: only monogram stamps are kept, once each, oldest first, an id is made for one without: ' + JSON.stringify(nl.map(s => s.id)));
  check(world(stand({ sealsOf: () => { throw new Error('boom'); } })).list(L1).length === 0, 'node: a reader that throws shows nothing');
  check(S.signature(a1) === S.signature(S.list(L1)) && S.signature(a1) !== S.signature([]) && S.lastAt(a1) === 2000 && S.lastAt([]) === 0, 'node: signature() changes exactly with the seals; lastAt() is the last approval time');
  // the markup
  const h1 = S.html(L1, null);
  check(/^<span class="monoSeals"/.test(h1) && /data-standin="monogramApproved:2000:Ana"/.test(h1) && /data-seal-count="1"/.test(h1) && !BAD.test(h1), 'node: html() is the reader\'s own sealRow in a .monoSeals wrapper, nothing of the engraving');
  check(S.html(L2, 'C2') === '' && S.html(PLAIN) === '' && S.html(L2, 'C1').includes('monogramApproved:3000:Ben'), 'node: no seal, no markup (an unapproved piece and a plain line have none); a piece\'s own is its own');
  // no reader at all: nothing anywhere
  const none = world(null), none2 = world({ status: () => null, service: () => null });
  check(!none.readable() && !none2.readable() && none.list(L1).length === 0 && none.html(L1) === '' && none.facts(L1) === null, 'node: a page without the readers shows nothing (list, html, fact)');
  check(!BAD.test(none.panel(none.pieceFor(L1, '900_1_1'))), 'node: and its panel never says BACK ENGRAVING');
  // the reader with seals but no html reader: Seal draws them only when it has the monogram family (else it would draw another kind of seal)
  const noHtml = stand({ sealHtml: undefined }), Seal = { BASE_SIZE: 50, FAMILY: {}, html: (s, size, cls) => `<span class="seal ${cls}" data-how="${s.how}"></span>` };
  check(world(noHtml, { Seal }).html(L1) === '', 'node: seals but no html reader and a Seal without the monogram family: nothing (never another kind of seal)');
  Seal.FAMILY.monogram = {};
  const fb = world(noHtml, { Seal }).html(L1);
  check(/monogramSeal/.test(fb) && /data-how="monogramApproved"/.test(fb) && /class="sealRow monogramSeals"/.test(fb), 'node: with the family, Seal.html draws them (class monogramSeal): ' + fb.slice(0, 120));
  // the fact (the order window's Monogram cell / chip)
  const f1 = S.facts(L1);
  check(f1 && f1.approved === true && /^<span>approved · J Q X<\/span>/.test(f1.html) && /data-standin/.test(f1.html) && !BAD.test(f1.html), 'node: the fact of an approved line: "approved · J Q X" and its seal: ' + (f1 && f1.text));
  const f2 = S.facts(L2);
  check(f2 && f2.approved === false && /Piece 1 of 2 · approved/.test(f2.text) && /Piece 2 of 2 · ready to approve/.test(f2.text) && (f2.html.match(/data-standin=/g) || []).length === 1, 'node: a line of two pieces, one approved: each piece in its own line, only piece 1\'s seal: ' + (f2 && f2.text));
  const unapproved = world(stand({ sealsOf: () => [] }));
  check(unapproved.facts(L1) === null && S.facts(PLAIN) === null, 'node: a monogram nobody approved has no fact (as an unengraved line has none); a plain line has none');
  // the pieces
  const p1 = S.pieceFor(L2, '900_2_2'), p0 = S.pieceFor(L2, '900_2_1');
  check(p1 && p1.slot === 'C2' && p1.n === 2 && p1.of === 2 && p0.slot === 'C1' && S.pieceFor(L2, 'nope').slot === 'C1' && S.pieceFor(L2, '').slot === 'C1' && S.pieceFor(L1, '900_1_1').slot === null && S.pieceFor(PLAIN, '900_3_1') === null, 'node: a charm\'s pool id says which piece of the line it is; a line of one piece has no slot; a plain line is no piece');
  const noSvc = world({ sealsOf: stand().sealsOf });
  check(noSvc.pieces(L2).length === 1 && noSvc.pieces(L2)[0].key === '900_2' && noSvc.pieceFor(L2, '900_2_2').slot === null, 'node: without the widget service a line is one piece (its line key)');
  // the panel
  const pa = S.panel(S.pieceFor(L1, '900_1_1'));
  check(/class="fLabel">Monogram</.test(pa) && /data-state="approved"/.test(pa) && /The monogram is approved/.test(pa) && /Approved by Ana/.test(pa) && /data-standin/.test(pa) && /data-e="monogram"/.test(pa) && /View in Engraving/.test(pa) && /<div class="words">J Q X</.test(pa) && !BAD.test(pa) && !/No back engraving/i.test(pa) && !/Approve engraving|egApproveButton/.test(pa), 'node: the panel of an approved piece: title, "Approved by Ana", the letters, its seal, View in Engraving, no Approve button');
  const pw = S.panel(S.pieceFor(L2, '900_2_2'));
  check(/data-state="approve"/.test(pw) && /Monogram waits for approval in Engraving/.test(pw) && /View in Engraving/.test(pw) && !/data-standin/.test(pw) && !/Approved by/.test(pw) && /Piece 2 of 2/.test(pw) && !BAD.test(pw), 'node: the panel of a piece not approved: "Monogram waits for approval in Engraving", the same button, no seal, its piece named');
  check(/Piece 1 of 2/.test(S.panel(p0)) && /Approved by Ben/.test(S.panel(p0)) && !/Approved by Ben/.test(pw), 'node: each piece of a line of two has its own panel');
  check(/class="egSealLine"><div class="egWhy">.*<\/div><span class="monoSeals"/.test(pa) && !/egHistory/.test(pa), 'node: an approved piece\'s seal stands beside "Approved by X" (egSealLine), not in a history block');
  const changed = world(stand({ status: () => ({ state: 'ready', label: 'Ready to approve', letters: 'JQX', approved: false }) })), pc = changed.panel(changed.pieceFor(L1, '900_1_1'));
  check(/data-state="approve"/.test(pc) && /egHistory"><span class="monoSeals"/.test(pc) && !/egSealLine/.test(pc) && /waits for approval/.test(pc), 'node: a piece that waits but has an earlier approval keeps that seal below, as a history (as the engraving card does)');
  check(S.panel(null) === '' && S.panel({}) === '', 'node: no piece, no panel');
  check(!/class="fLabel"/.test(S.panel(S.pieceFor(L1, '900_1_1'), { compact: true })) && /egCompact/.test(S.panel(S.pieceFor(L1, '900_1_1'), { compact: true })), 'node: compact: no title line, the compact card');
  // the activity (a stand-in carries seals[]: the Engrave lists sort and date-filter it by its approval time)
  const NOW = Date.now(), T = Date.UTC(2026, 8, 1);
  const mk = seals => ({ key: 'k', mono: true, state: 'approved', row: { order: { receiptId: '1', updateTs: T / 1000 } }, seals });
  check(Act.at(mk([])) === T && Act.at(mk([{ at: NOW - 5000 }])) === NOW - 5000 && Act.at(mk([{ at: NOW - 9000 }, { at: NOW - 5000 }])) === NOW - 5000, 'node: CNListActivity.at() of a stand-in is its last seal\'s time when later than its order\'s');
  check(Act.matches(mk([{ at: NOW - 5000 }]), 'today', NOW) && !Act.matches(mk([]), 'today', NOW) && Act.compare(mk([{ at: NOW - 5000 }]), mk([]), 'desc') < 0, 'node: so an approved monogram is in Today, an old unapproved one is not, and the approved one sorts first (newest first)');

  // the wiring in the page's own files
  const bridge = read('charm-nest-bridge.js'), sw = read('charm-nest-sheetwin.js'), html = read('charm-nest-1.html'), build = read('scripts/build-public.cjs'), css = read('charm-nest-activity.css'), facts = read('charm-nest-order-facts.js');
  const fn = (src, start, end) => { const i = src.indexOf(start), j = src.indexOf(end, i + 10); assert(i > 0 && j > i, 'markers ' + start); return src.slice(i, j); };
  const code = s => s.replace(/\/\/[^\n]*/g, '');   // (the comments say what is NOT used; the code is what is checked)
  const items = fn(bridge, 'function monoItemsNow()', '/** The Engrave tab\'s own lists'), prow = fn(bridge, 'function monoPlacementRow(grp)', '/** Back / Next from a monogram card');
  check(/m\.seals = window\.CNMonogramSeals \? CNMonogramSeals\.list\(row, pc\.slot\) : \[\]/.test(items) && !/CNEngravingSeals/.test(code(items)), 'source: the stand-ins carry seals[] from the monogram\'s reader, never from CNEngravingSeals');
  check(/class="egPlacementSeals"/.test(prow) && /CNMonogramSeals\.signature\(m\.seals\)/.test(prow) && /row\._seals !== stamp/.test(prow) && !/CNEngravingSeals/.test(code(prow)),'source: monoPlacementRow has the engraved row\'s own cell, painted only when the seals change, with the monogram\'s markup');
  const facts2 = fn(bridge, 'function factsOf(r)', '/** The facts drawn into #owMeta');
  check(/CNMonogramSeals\.facts\(r\)/.test(facts2) && /label: "Monogram"/.test(facts2) && /chip: "Monogram"/.test(facts2) && /hasEng \|\| !window\.CNMonogramSeals \? null/.test(facts2) && !/r\.engrave\s*=|engrave \|\|=|\.engrave\s*\|\|=/.test(facts2), 'source: factsOf tells a monogram line\'s seals as the "Monogram" cell and chip, only when no engraving cell is told, and creates no r.engrave');
  check(/engraving\.title|e\.title \|\| 'Engraving'/.test(facts) && /pop: \{ title: e\.title \|\| 'Engraving'/.test(facts), 'source: the fact card is titled by the line\'s own word (Monogram), "Engraving" for every other line');
  check(/CNMonogramSeals\.pieceFor\(lr,x0\?\.poolId \|\| SV\.focus\)/.test(bridge) && /monoPc\?CNMonogramSeals\.panel\(monoPc\):CNEngravingSeals\.panel\(eng\)/.test(bridge) && /if\(monoPc\)CNMonogramSeals\.wire\(/.test(bridge), 'source: the Sheet tab draws the monogram panel for a monogram piece and the engraving panel for every other');
  check(/CNMonogramSeals\.pieceFor\(rowOf\(x\), x\.poolId \|\| x\.id\)/.test(sw) && /CNMonogramSeals\.panel\(mp\)/.test(sw) && /function goMonogram\(/.test(sw) && /CNMonogramSeals\.openIn\(mp/.test(sw), 'source: the Sheet window draws the monogram panel for a monogram piece, its button through EngraveLink.openMonogram');
  check(/charm-nest-monogram-seals\.js\?v=\d+-mseal1/.test(html) && /"charm-nest-monogram-seals\.js"/.test(build), 'source: the new module is loaded by the page and is in the published asset list');
  for (const [file, re] of [['charm-nest-bridge.js', /charm-nest-bridge\.js\?v=[^"]*-mseal1[^"]*"/], ['charm-nest-sheetwin.js', /charm-nest-sheetwin\.js\?v=[^"]*-mseal1[^"]*"/], ['charm-nest-activity.css', /charm-nest-activity\.css\?v=[^"]*-mseal1[^"]*"/], ['charm-nest-order-facts.js', /charm-nest-order-facts\.js\?v=[^"]*-mseal1[^"]*"/]]) check(re.test(html), 'source: the cache-buster of ' + file + ' is bumped');
  check(/\.monoSeals\{display:contents\}/.test(css) && /\.monoSeals>\.sealRow>\.seal\{/.test(css), 'source: the CSS for the wrapper lives beside .sealRow.engravingSeals (charm-nest-activity.css)');
  check(!/CNEngravingSeals/.test(read('charm-nest-monogram-seals.js').replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/[^\n]*/g, '')), 'source: the module never calls CNEngravingSeals');
}

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('monogram-seal-surfaces: no playwright-core, the browser part is skipped'); finish(); return; }
  fs.mkdirSync(SHOTS, { recursive: true });
  const { start } = require('./bridge-server.cjs');

  // ═══ the world: three monogram lines (one approved, one not, one of two pieces with only the first approved), two engraved bars (one approved with its seal, one to approve), all on one sheet ═══
  const DAY = 86400, NOW_S = Math.floor(Date.now() / 1000), SHIP = NOW_S + 3 * DAY, OLD = NOW_S - 20 * DAY, SH = 'sheet-mseal';
  const MONO_T = 'Monogram Necklace Personalized Jewelry Initials Pendant';
  const M1 = { rid: '4200000011', tids: ['42000000111'], skus: ['MONOGRAM_5351'], listing: '997905351', title: MONO_T, pers: ['APV'], qty: 1, letters: 'APV' };      // approved
  const M2 = { rid: '4200000022', tids: ['42000000221'], skus: ['MONOGRAM_9317'], listing: '606439317', title: 'Monogram Necklace Dainty Monogram Necklace', pers: ['OSR'], qty: 1, letters: 'OSR' };   // not approved
  const M3 = { rid: '4200000033', tids: ['42000000331'], skus: ['MONOGRAM_5351'], listing: '997905351', title: MONO_T, pers: ['JQX'], qty: 2, letters: 'JQX' };      // two pieces: the first approved
  const E1 = { rid: '4200000044', tids: ['42000000441'], skus: ['FIREBIRD_2'], qty: 1 };     // engraved, approved (one BACK ENGRAVING seal)
  const E2 = { rid: '4200000055', tids: ['42000000551'], skus: ['OWL_CHARM'], qty: 1 };     // engraved, to approve
  const pool = (o, i, c) => `${o.rid}_${o.tids[i]}_${c || 1}`, keyOf = (o, i) => `${o.rid}_${o.tids[i]}`;
  const line = (o, i) => ({ transactionId: o.tids[i], listingId: o.listing || '18000' + o.tids[i].slice(-5), sku: o.skus[i], title: o.title || o.skus[i].replace(/_/g, ' ') + ' charm', quantity: o.qty, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: '14k Gold Filled', personalization: o.pers || '' });
  const order = (o, buyer) => ({ receiptId: o.rid, orderNumber: o.rid, createTs: OLD, updateTs: OLD + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: o.tids.map((t, i) => line(o, i)) });
  const PIECES = [[M1, 0, 1], [M2, 0, 1], [M3, 0, 1], [M3, 0, 2], [E1, 0, 1], [E2, 0, 1]];
  function seed(st) {
    const pl = [], ch = [];
    for (let i = 0; i < 12; i++) {
      const id = 'b' + i, x = PIECES[i] || null, rid = x ? x[0].rid : String(4179000000 + i);
      pl.push({ id, cxPt: 16 + (i % 6) * 30, cyPt: 16 + Math.floor(i / 6) * 30, angle: 0, wPt: 22, hPt: 22 });
      ch.push({ id, name: `${rid} · SKU${i}`, poolId: x ? pool(x[0], x[1], x[2]) : `${rid}_${rid}1_1`, order: rid, sku: x ? x[0].skus[x[1]] : 'SKU' + i });
    }
    st.put('Charm_Nest_Sheets', SH, { id: SH, metal: 'gold', sheetIndex: 1, setSeq: 1, day: '2026-10-05', status: 'written', fileBase: 'GF_Oct.5.26_Set-1_Sheet-1', folder: 'GF_Oct.5.26_Set-1_Sheet-1', stock: { wPt: 230, hPt: 90 }, orders: [...new Set(ch.map(c => c.order))], placements: pl, charms: ch, backPool: [] });
    for (const [o, i, c] of PIECES) st.put('Charm_Pool', pool(o, i, c), { poolId: pool(o, i, c), orderId: o.rid, transactionId: o.tids[i], lineKey: keyOf(o, i), sku: o.skus[i], material: 'gold', copy: c, quantity: o.qty, state: 'written', sheetId: SH, sheetName: 'GF_Oct.5.26_Set-1_Sheet-1', updatedAt: Date.now() });
  }

  const srv = await start({ receipts: [] }); seed(srv.st);
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  let page = null;
  try {
    const context = await browser.newContext({ viewport: { width: 1500, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.route(/\/\.netlify\/functions\/etsyMailOrderLink/, r => r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ engagements: [], active: null, conversation: null, ok: true, n: 0 }) }));
    await context.addInitScript(() => {
      try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); localStorage.setItem('cn.tour.seen', '1'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {}
      window.prompt = () => 'Test Operator';
    });
    page = await context.newPage(); const errors = [], writes = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    page.on('request', rq => { if (rq.method() !== 'GET' && rq.method() !== 'OPTIONS' && /claude|openai|anthropic|etsy/i.test(rq.url())) writes.push(rq.url()); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.SheetWin && window.Engrave && window.CNEngravingSeals && CN.S.cloud.ok === true, null, { timeout: 60000 });
    check(await page.evaluate(() => !!(window.CNMonogramSeals && CNMonogramSeals.list && CNMonogramSeals.panel)), 'the page loads charm-nest-monogram-seals.js (CNMonogramSeals)');

    // ── the orders, their jobs, the stand-ins ──
    await page.evaluate(async ({ orders, pools, jobs, T }) => {
      await Orders.loadMaps(true);
      const rows = new Map();
      for (const [o, ps] of orders) for (const [i, l] of o.lines.entries()) { const key = CharmNestOrders.lineKey(o, l); const row = { key, order: o, line: l, arrivedAt: Date.now(), spec: null, problems: [], state: 'written', reason: null, claimedBy: null, poolIds: ps[i], engrave: null, material: 'gold', metal: 'gold' }; B.orders.rows.push(row); B.orders.byKey && B.orders.byKey.set(key, row); rows.set(key, row); }
      Orders.interpretAll();
      Engrave.renderBack = () => { const cv = document.createElement('canvas'); cv.width = 300; cv.height = 300; return cv; };
      for (const [k, st, text] of jobs) {
        const row = rows.get(k), job = Engrave.ensureJob(row), done = st === 'approved';
        Object.assign(job, { state: st, text, lines: text ? text.split(' // ') : [], activityAt: T, approvedBy: done ? 'Paul' : null, approvedAt: done ? T - 3600e3 : null, backs: [], fit: st === 'review' || done ? {} : null, view: st === 'review' || done ? {} : null, verify: { geometry: { ok: true } } });
        if (done) job.engravingSeals = [{ id: 'old1', how: 'engraveApproved', at: T - 3600e3, by: 'Paul' }];
        row.engrave = { needed: true, state: st, approved: done, text, approvedBy: job.approvedBy, approvedAt: job.approvedAt, seals: done ? job.engravingSeals : [] };
      }
    }, { orders: [[order(M1, 'Cari Moll'), [[pool(M1, 0)]]], [order(M2, 'Ada Byrne'), [[pool(M2, 0)]]], [order(M3, 'Mia Lund'), [[pool(M3, 0, 1), pool(M3, 0, 2)]]], [order(E1, 'Ola Berg'), [[pool(E1, 0)]]], [order(E2, 'Eli Voss'), [[pool(E2, 0)]]]],
      jobs: [[keyOf(E1, 0), 'approved', 'GOOD // LUCK'], [keyOf(E2, 0), 'review', 'LOVE // MOM']], T: Date.now() });
    const mono = await page.evaluate(() => Orders.rows().filter(r => r.spec && r.spec.special && r.spec.special.monogram).map(r => r.key));
    check(mono.length === 3 && [M1, M2, M3].every(o => mono.includes(keyOf(o, 0))), 'the intake reads the three monogram lines: ' + mono.join(' '));
    const eng0 = await page.evaluate(() => Object.fromEntries(Orders.rows().filter(r => r.spec && r.spec.special && r.spec.special.monogram).map(r => [r.key, [r.engrave == null, OrderTimelineUI.engraveOf(r)]])));
    check(Object.values(eng0).every(v => v[0] && v[1] !== true), 'a monogram line has no r.engrave and engraveOf(line) is not true, to begin with: ' + JSON.stringify(eng0));

    // the widget service's word for a piece (approved or not) and the CORE's two readers, as STAND-INS (the surfaces are what is tested)
    const NOW = Date.now(), A1 = { at: NOW - 60000, by: 'Ana Test', letters: 'APV' }, A3 = { at: NOW - 120000, by: 'Ben Test', letters: 'JQX' };
    await page.evaluate(({ k1, k3, A1, A3, ls }) => {
      window.__ma = { [k1 + '|']: A1, [k3 + '|C1']: A3 };
      const svc = Engrave.monogramUI(), MU = window.CharmNestMonogramUI, keyOf = (row, slot) => (row.parentRow || row).key + '|' + (slot || '');
      svc.status = (row, ask) => {
        const r = row.parentRow || row; if (!(r.spec && r.spec.special && r.spec.special.monogram)) return null;
        const of = Math.max(1, +(r.spec.quantity || 1)), slot = typeof ask === 'string' && /^C\d+$/.test(ask) ? ask : of > 1 ? 'C1' : null, ap = !!__ma[keyOf(r, slot)];
        return { state: ap ? 'approved' : 'ready', label: ap ? 'Approved' : 'Ready to approve', letters: ls[r.key] || '', approved: ap, dirty: false, sent: false, rid: String(r.order.receiptId), lineKey: r.key, pieceKey: slot ? r.key + '#' + slot : r.key, slot, n: slot ? +slot.slice(1) : 1, of };
      };
      window.__readers = {
        sealsOf: (row, slot) => { const a = __ma[keyOf(row, slot)]; return a ? [{ id: 'monogramApproved:' + a.at + ':' + a.by, how: 'monogramApproved', at: a.at, by: a.by, letters: a.letters }] : []; },
        sealHtml: (row, slot) => { const s = __readers.sealsOf(row, slot); return s.length ? `<span class="sealRow standInRow" data-seal-group data-seal-count="${s.length}" role="group" aria-label="Monogram approval history">` + s.map(x => `<span class="seal standIn" data-standin-monogram="${x.id}" data-at="${x.at}" tabindex="0" role="img" aria-label="Monogram approved by ${x.by}" style="display:inline-block"><svg viewBox="0 0 50 50" width="100%" height="100%"><circle cx="25" cy="25" r="23" fill="#fff" stroke="#6b4aa0" stroke-width="3"/><text x="25" y="29" text-anchor="middle" font-size="8" fill="#6b4aa0">MONOGRAM</text></svg></span>`).join('') + '</span>' : ''; }
      };
      Object.assign(MU, __readers);
      window.__mlinks = []; EngraveLink.openMonogram = async a => { __mlinks.push({ rid: a.rid, key: a.key, closeFirst: typeof a.closeFirst }); return true; };
    }, { k1: keyOf(M1, 0), k3: keyOf(M3, 0), A1, A3, ls: { [keyOf(M1, 0)]: 'APV', [keyOf(M2, 0)]: 'OSR', [keyOf(M3, 0)]: 'JQX' } });

    // ═══ S1 · the Engrave tab ═══
    const openList = () => page.evaluate(() => { CN.setMode('engrave'); Engrave.restoreView({ tab: 'place', list: true, chosen: true, focus: null, pin: null, openDone: null }); Engrave.render(); });
    const waitRows = n => page.waitForFunction(n => document.querySelectorAll('#egQueue .egPlacementList .doneRow, #egQueue .doneRow').length >= n, n, { timeout: 30000 });
    await openList(); await waitRows(6); await sleep(1200);
    await page.evaluate(() => document.querySelectorAll('.cnNbX, .toast .x').forEach(b => { try { b.click(); } catch (_) {} }));
    const rowsOf = () => page.evaluate(() => [...document.querySelectorAll('#egQueue .doneRow')].map(r => {
      const c = r.querySelector(':scope > .egPlacementSeals'), seals = c ? [...c.querySelectorAll('.seal')] : [], rr = r.getBoundingClientRect();
      return { kind: r.dataset.kind || 'engraved', quiet: r.classList.contains('quietRow'), rid: r.dataset.rid, slot: r.dataset.slot || '', decided: r.classList.contains('isDecided'), cell: !!c, seals: seals.length, standIn: seals.filter(s => s.hasAttribute('data-standin-monogram')).length,
        models: seals.map(s => (s.querySelector('svg') || {}).getAttribute && s.querySelector('svg').getAttribute('data-seal-model') || '').join('|'), text: c ? c.innerText.trim() : '', html: c ? c.innerHTML : '',
        inside: seals.every(s => { const q = s.getBoundingClientRect(); return q.width >= 20 && q.right <= rr.right + 1 && q.left >= rr.left - 1; }) };
    }));
    let rs = await rowsOf();
    const mrow = (o, slot) => rs.find(r => r.kind === 'monogram' && r.rid === o.rid && (slot == null || r.slot === slot));
    check(rs.filter(r => r.kind === 'monogram').length === 4, 'S1 the Engrave list has the four monogram pieces (M1, M2, M3 piece 1, M3 piece 2): ' + rs.map(r => r.kind[0] + r.rid.slice(-3) + r.slot).join(' '));
    check(mrow(M1) && mrow(M1).cell && mrow(M1).standIn === 1 && mrow(M1).seals === 1 && mrow(M1).inside && mrow(M1).decided, 'S1 the approved monogram\'s row has its seal in the seals cell, inside the row: ' + JSON.stringify(mrow(M1) && { seals: mrow(M1).seals, standIn: mrow(M1).standIn, inside: mrow(M1).inside }));
    check(mrow(M2) && mrow(M2).seals === 0 && mrow(M2).text === '' && !mrow(M2).decided, 'S1 the unapproved monogram\'s row has no seal (its cell is empty)');
    check(mrow(M3, 'C1') && mrow(M3, 'C1').standIn === 1 && mrow(M3, 'C2') && mrow(M3, 'C2').seals === 0, 'S1 a line of two pieces: piece 1 (approved) has its seal, piece 2 has none');
    check(rs.filter(r => r.kind === 'monogram').every(r => !BAD.test(r.html) && !BAD.test(r.models) && !BAD.test(r.text)), 'S1 never a BACK ENGRAVING seal on a monogram row (no seal model, no word of the engraving)');
    const e1 = rs.find(r => r.kind === 'engraved' && r.rid === E1.rid), e2 = rs.find(r => r.kind === 'engraved' && r.rid === E2.rid);
    if (e1 && !e1.quiet) check(e1.seals === 1 && /BACK ENGRAVING/.test(e1.models) && e1.standIn === 0, 'S1 an engraved row is what it was: its one BACK ENGRAVING seal (' + e1.models + ')');
    else console.log('  – (the engraved row E1 is not drawn in this light world: ' + JSON.stringify(e1 && { quiet: e1.quiet }) + '; engraved rows are the baseline tests\' to cover)');
    if (e2 && !e2.quiet) check(e2.seals === 0 && !e2.decided, 'S1 an engraved row still to approve has no seal and is not calm');
    // the stand-ins and the activity lists
    const act = await page.evaluate(({ k1, k2, k3 }) => { const it = Engrave.monoItems(), get = (k, s) => it.find(m => m.lineKey === k && (m.slot || '') === (s || '')); const m1 = get(k1), m2 = get(k2), m3a = get(k3, 'C1'), m3b = get(k3, 'C2');
      return { n: it.length, seals: [m1, m2, m3a, m3b].map(m => m.seals.length), at1: CNListActivity.at(m1), at2: CNListActivity.at(m2), t1: m1.seals[0] && m1.seals[0].at, today1: CNListActivity.matches(m1, 'today'), week1: CNListActivity.matches(m1, '7'), week2: CNListActivity.matches(m2, '7'), cmp: CNListActivity.compare(m1, m2, 'desc'), state: [m1, m2].map(m => m.state), approvedAt: m1.approvedAt, hasEngrave: it.some(m => 'engravingSeals' in m) }; }, { k1: keyOf(M1, 0), k2: keyOf(M2, 0), k3: keyOf(M3, 0) });
    check(act.n === 4 && act.seals.join() === '1,0,1,0' && act.t1 === A1.at && act.at1 === A1.at && act.at2 < act.at1 && act.state.join() === 'approved,review', 'S1 the stand-ins carry m.seals (1,0,1,0), and CNListActivity.at(approved monogram) is its approval time: ' + JSON.stringify(act));
    // (the hazard this test guards is real: the engraving seals invent a legacy BACK ENGRAVING seal at time 0 for an approved stand-in, so every "no BACK ENGRAVING" check above and below means something)
    const hazard = await page.evaluate(k => { const m = Engrave.monoItems().find(x => x.lineKey === k), l = CNEngravingSeals.list(m); return { n: l.length, legacy: l.some(s => String(s.id).startsWith('legacy:') && s.at === 0), html: CNEngravingSeals.html(m).length > 0 }; }, keyOf(M1, 0));
    check(hazard.legacy && hazard.html, 'S1 (witness) CNEngravingSeals.list/html over an approved stand-in WOULD invent a legacy BACK ENGRAVING seal at time 0: ' + JSON.stringify(hazard));
    check(act.week1 === true && act.week2 === false && act.cmp < 0 && !act.hasEngrave, 'S1 the date filter "Last 7 days" keeps the monogram approved a minute ago and drops the unapproved one from 20 days ago; newest first puts the approved one first; no engravingSeals invented');
    // the list is sorted by it: filter to the last 7 days
    await page.evaluate(() => { CNListActivity.set('engrave-place', { range: '7' }); Engrave.render(); }); await sleep(900);
    rs = await rowsOf();
    check(rs.some(r => r.kind === 'monogram' && r.rid === M1.rid) && !rs.some(r => r.kind === 'monogram' && r.rid === M2.rid), 'S1 the Engrave list\'s "Last 7 days" date filter keeps the monogram approved a minute ago and hides the old unapproved one: ' + rs.map(r => r.kind[0] + r.rid.slice(-3)).join(' '));
    await page.evaluate(() => { CNListActivity.set('engrave-place', { range: 'all' }); Engrave.render(); }); await sleep(900);
    // no needless redraw
    const still = await page.evaluate(async rid => { const cell = () => document.querySelector(`#egQueue .doneRow[data-kind=monogram][data-rid="${rid}"] > .egPlacementSeals`); const c0 = cell(), s0 = c0.querySelector('.seal'); s0.__keep = 1; const seen = [], attrs = []; const mo = new MutationObserver(m => { for (const x of m) { if (x.type === 'attributes') attrs.push(x.attributeName + '@' + (x.target.className && x.target.className.baseVal === undefined ? String(x.target.className).split(' ')[0] : 'svg') + ':' + (x.oldValue === x.target.getAttribute(x.attributeName) ? 'same' : 'new')); else seen.push(x.type); } }); mo.observe(c0, { subtree: true, childList: true, characterData: true, attributes: true, attributeOldValue: true });
      for (let i = 0; i < 3; i++) { Engrave.render(); await new Promise(r => setTimeout(r, 350)); } mo.disconnect(); const c1 = cell(); return { same: c1 === c0, kept: !!(c1 && c1.querySelector('.seal') && c1.querySelector('.seal').__keep), changes: seen.length, attrs }; }, M1.rid);
    // (what Seal itself writes again on every look at its row (charm-nest-motion.js fit: data-seal-count to the value it has) is not the cell being drawn again: the cell's content is never rewritten and no attribute changes to a NEW value)
    check(still.same && still.kept && still.changes === 0 && still.attrs.every(a => /:same$/.test(a)), 'S1 three more renders of the tab leave the seals cell alone (same element, same seal, no node added or removed, no attribute changed): ' + JSON.stringify(still));
    // an approval that arrives later is painted on the row
    await page.evaluate(({ k2, at }) => { __ma[k2 + '|'] = { at, by: 'Cy Late', letters: 'OSR' }; Engrave.render(); }, { k2: keyOf(M2, 0), at: Date.now() - 1000 });
    await sleep(900); rs = await rowsOf();
    check(mrow(M2) && mrow(M2).standIn === 1 && mrow(M2).decided, 'S1 a monogram approved while the list is open gets its seal on its row at the next render');
    await page.evaluate(k2 => { delete __ma[k2 + '|']; Engrave.render(); }, keyOf(M2, 0)); await sleep(700); rs = await rowsOf();
    check(mrow(M2) && mrow(M2).seals === 0 && !mrow(M2).decided, 'S1 and the seal is gone from it again when the approval is');
    await page.screenshot({ path: path.join(SHOTS, 'S1-list-1500.png') });
    // the narrow list: the seal under the pieces, nothing sideways
    await page.setViewportSize({ width: 560, height: 900 }); await sleep(700);
    const narrow = await page.evaluate(rid => { const r = document.querySelector(`#egQueue .doneRow[data-kind=monogram][data-rid="${rid}"]`), s = r && r.querySelector(':scope > .egPlacementSeals .seal'); if (!r || !s) return null; const a = r.getBoundingClientRect(), b = s.getBoundingClientRect(); return { inside: b.left >= a.left - 1 && b.right <= a.right + 1 && b.width >= 20, sw: document.documentElement.scrollWidth, cw: document.documentElement.clientWidth }; }, M1.rid);
    await page.screenshot({ path: path.join(SHOTS, 'S1-list-560.png') });
    check(narrow && narrow.inside && narrow.sw <= narrow.cw + 1, 'S1 on a narrow screen the seal is inside its row and the page does not overflow sideways: ' + JSON.stringify(narrow));
    await page.setViewportSize({ width: 1500, height: 900 }); await sleep(500);

    // ═══ S2 · the order window's Overview ═══
    const closeWin = async () => { await page.evaluate(() => { const o = document.getElementById('orderWin'); if (o && o.open) document.getElementById('owClose').click(); }); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 8000 }).catch(() => {}); await page.waitForTimeout(300); };
    const openOv = async (o, opts) => { await page.evaluate(({ k, opts }) => OrderWin.open(k, opts), { k: keyOf(o, 0), opts }); await page.waitForFunction(k => OrderWin.isOpen() && OrderWin.key() === k, keyOf(o, 0)); await page.waitForSelector('#owMeta [data-owf-chip="order"]', { timeout: 20000 }); await sleep(900); };
    const facts = () => page.evaluate(() => {
      const chip = document.querySelector('#owMeta [data-owf-chip="engraving"]'), more = document.querySelector('#owMeta .owfMore');
      if (more && more.getAttribute('aria-expanded') !== 'true') more.click();
      return new Promise(res => setTimeout(() => {
        const cells = [...document.querySelectorAll('#owMeta .owfList .m')].map(m => ({ label: m.querySelector('i').textContent, text: m.innerText.replace(/\s+/g, ' ').trim(), seals: [...m.querySelectorAll('.seal')].length, standIn: m.querySelectorAll('[data-standin-monogram]').length, models: [...m.querySelectorAll('[data-seal-model]')].map(n => n.getAttribute('data-seal-model')).join('|'), html: m.innerHTML }));
        res({ chip: chip ? chip.textContent.trim() : null, chipAria: chip ? chip.getAttribute('aria-label') : null, cells });
      }, 400));
    });
    const popOf = async () => { await page.click('#owMeta [data-owf-chip="engraving"]'); await sleep(600); return page.evaluate(() => { const p = document.querySelector('.owfPop:not([hidden])'); return p ? { title: (p.querySelector('.owfPopHd') || {}).textContent, text: p.innerText.replace(/\s+/g, ' ').trim(), standIn: p.querySelectorAll('[data-standin-monogram]').length, html: p.innerHTML } : null; }); };
    await openOv(M1);
    let f = await facts(); let cellM = f.cells.find(c => c.label === 'Monogram');
    check(f.chip === 'Monogram' && /Monogram: Monogram, approved/.test(f.chipAria || ''), 'S2 the approved monogram line\'s Overview has a "Monogram" chip: ' + JSON.stringify([f.chip, f.chipAria]));
    check(cellM && cellM.standIn === 1 && /approved · A P V/.test(cellM.text) && !f.cells.some(c => c.label === 'Engraving') && !BAD.test(cellM.html), 'S2 its Details list has the Monogram cell (the letters and its seal) and no Engraving cell: ' + JSON.stringify(cellM && cellM.text));
    let pop = await popOf();
    check(pop && /^\s*Monogram/i.test(pop.title) && pop.standIn === 1 && /approved · A P V/.test(pop.text) && !BAD.test(pop.html), 'S2 the chip\'s card is titled Monogram and holds the seal: ' + JSON.stringify(pop && { title: pop.title, standIn: pop.standIn }));
    await page.keyboard.press('Escape').catch(() => {}); await page.screenshot({ path: path.join(SHOTS, 'S2-overview-M1.png') });
    const after = await page.evaluate(k => { const r = Orders.rows().find(x => x.key === k); return { engrave: r.engrave == null, eo: OrderTimelineUI.engraveOf(r), job: !!Engrave.jobOf(r) }; }, keyOf(M1, 0));
    check(after.engrave && after.eo !== true && !after.job, 'S2 opening the window did not make an engraving of the monogram line: r.engrave still empty, engraveOf(line) not true, no Engrave job: ' + JSON.stringify(after));
    await closeWin();
    await openOv(M2); f = await facts();
    check(f.chip === null && !f.cells.some(c => c.label === 'Monogram' || c.label === 'Engraving'), 'S2 an unapproved monogram line has no Monogram fact (and no Engraving one): ' + JSON.stringify(f.cells.map(c => c.label)));
    await closeWin();
    await openOv(M3); f = await facts(); cellM = f.cells.find(c => c.label === 'Monogram');
    check(f.chip === 'Monogram' && cellM && /Piece 1 of 2 · approved/.test(cellM.text) && /Piece 2 of 2 · ready to approve/.test(cellM.text) && cellM.standIn === 1, 'S2 a line of two pieces: each piece in its line, only the approved piece\'s seal: ' + JSON.stringify(cellM && cellM.text));
    check(!BAD.test(cellM.html) && !BAD.test(cellM.models), 'S2 never a BACK ENGRAVING seal in the Monogram cell');
    await closeWin();
    await openOv(E1); f = await facts(); const engCell = f.cells.find(c => c.label === 'Engraving');
    check(f.chip !== null && f.chip !== 'Monogram' && engCell && engCell.seals === 1 && /BACK ENGRAVING/.test(engCell.models) && !f.cells.some(c => c.label === 'Monogram'), 'S2 an engraved line is what it was: the Engraving chip/cell with its one BACK ENGRAVING seal, no Monogram cell: ' + JSON.stringify([f.chip, engCell && engCell.models]));
    pop = await popOf(); check(pop && /^\s*Engraving/i.test(pop.title), 'S2 and its card is still titled Engraving: ' + (pop && pop.title));
    await page.keyboard.press('Escape').catch(() => {}); await closeWin();
    // an approval that arrives while the window is open is told in the fact
    await openOv(M2); await page.evaluate(k2 => { __ma[k2 + '|'] = { at: Date.now() - 500, by: 'Cy Late', letters: 'OSR' }; }, keyOf(M2, 0));
    await page.evaluate(() => OrderWin.paint());
    const late = await page.waitForFunction(() => { const c = document.querySelector('#owMeta [data-owf-chip="engraving"]'); return c && c.textContent.trim() === 'Monogram'; }, null, { timeout: 8000 }).then(() => true, () => false);
    check(late, 'S2 a monogram approved while its window is open shows its Monogram chip at the window\'s next paint');
    await page.evaluate(k2 => { delete __ma[k2 + '|']; OrderWin.paint(); }, keyOf(M2, 0));
    check(await page.waitForFunction(() => !document.querySelector('#owMeta [data-owf-chip="engraving"]'), null, { timeout: 8000 }).then(() => true, () => false), 'S2 and the chip is gone again when the approval is');
    await closeWin();

    // ═══ S3 · the Sheet tab ═══
    const TAB = '#owSheetPanel [data-engraving-panel]', WIN = '[data-r2=eng]';
    const panelOf = sel => page.evaluate(sel => {
      const host = document.querySelector(sel); if (!host) return { host: false };
      const c = host.querySelector('.swEng'); if (!c || !(c.offsetParent || c.getClientRects().length)) return { host: true, card: false, text: host.innerText.trim() };
      const b = c.querySelector('[data-e=monogram]'), seals = [...c.querySelectorAll('.seal')];
      return { host: true, card: true, mono: c.classList.contains('monoCard'), state: c.dataset.state, text: c.innerText.replace(/\s+/g, ' ').trim(), html: c.outerHTML, label: (host.querySelector('.fLabel') || {}).textContent || '', pill: (c.querySelector('.egPill') || {}).textContent || '',
        words: (c.querySelector('.words') || {}).textContent || '', button: b ? { text: b.textContent.trim(), disabled: b.disabled } : null, approve: !!c.querySelector('[data-e=approve], .egApproveButton'), seals: seals.length, standIn: seals.filter(s => s.hasAttribute('data-standin-monogram')).length,
        models: seals.map(s => (s.querySelector('svg') || {}).getAttribute && s.querySelector('svg').getAttribute('data-seal-model') || '').join('|'), engButton: (c.querySelector('.egApproveButton') || {}).textContent || '', inside: seals.every(s => { const q = s.getBoundingClientRect(), r = c.getBoundingClientRect(); return q.width >= 20 && q.right <= r.right + 1 && q.left >= r.left - 1; }) };
    }, sel);
    const waitPanel = (sel, cls, ms = 15000) => page.waitForFunction(({ sel, cls }) => { const e = document.querySelector(sel + ' .swEng'); return e && (e.offsetParent || e.getClientRects().length) && (!cls || e.classList.contains(cls)); }, { sel, cls }, { timeout: ms }).then(() => true, () => false);
    const openTab = async (o, pick) => { await openOv(o, { view: 'sheet' }).catch(() => {}); await page.waitForFunction(() => OrderWin.view() === 'sheet' && OrderWin._sheet() && document.querySelector('#owSheetPanel .owPieces'), null, { timeout: 25000 }); if (pick) { await page.evaluate(n => { const li = [...document.querySelectorAll('#owSheetPanel .owPieces li')].find(l => l.textContent.includes(n)); if (li) li.click(); }, pick); await sleep(500); } };
    await page.evaluate(k => OrderWin.open(k, { view: 'sheet' }), keyOf(M1, 0));
    await page.waitForFunction(() => OrderWin.view() === 'sheet' && OrderWin._sheet() && document.querySelector('#owSheetPanel .owPieces'), null, { timeout: 25000 });
    check(await waitPanel(TAB, 'monoCard'), 'S3 Sheet tab, the approved monogram: the Monogram panel (not "No back engraving")');
    let c = await panelOf(TAB);
    check(c.mono && c.state === 'approved' && c.label === 'Monogram' && /Approved/.test(c.pill) && /Approved by Ana Test/.test(c.text) && c.words === 'A P V' && c.standIn === 1 && c.seals === 1 && c.inside && c.button && /View in Engraving/.test(c.button.text) && !c.approve && !/No back engraving/i.test(c.text) && !BAD.test(c.html) && !BAD.test(c.models), 'S3 Sheet tab: title, "Approved by Ana Test", the letters, its seal, View in Engraving, no Approve button, no BACK ENGRAVING: ' + JSON.stringify({ text: c.text.slice(0, 140), seals: c.seals }));
    await page.screenshot({ path: path.join(SHOTS, 'S3-sheet-tab-M1.png') });
    await page.click(TAB + ' [data-e=monogram]'); await page.waitForFunction(() => window.__mlinks.length >= 1, null, { timeout: 8000 }).catch(() => {});
    const l1 = await page.evaluate(() => __mlinks[__mlinks.length - 1]);
    check(l1 && l1.rid === M1.rid && l1.key === keyOf(M1, 0), 'S3 Sheet tab: View in Engraving asks EngraveLink.openMonogram for that order and piece: ' + JSON.stringify(l1));
    await page.waitForFunction(() => !document.querySelector('#owSheetPanel [data-e=monogram][aria-busy=true]'), null, { timeout: 5000 }).catch(() => {});
    if (!(await page.evaluate(() => OrderWin.isOpen()))) await page.evaluate(k => OrderWin.open(k, { view: 'sheet' }), keyOf(M1, 0));
    await closeWin();
    await page.evaluate(k => OrderWin.open(k, { view: 'sheet' }), keyOf(M2, 0));
    await page.waitForFunction(() => OrderWin.view() === 'sheet' && OrderWin._sheet() && document.querySelector('#owSheetPanel .owPieces'), null, { timeout: 25000 });
    check(await waitPanel(TAB, 'monoCard'), 'S3 Sheet tab, the unapproved monogram: the Monogram panel');
    c = await panelOf(TAB);
    check(c.mono && c.state === 'approve' && /Monogram waits for approval in Engraving/.test(c.text) && c.seals === 0 && c.button && /View in Engraving/.test(c.button.text) && !c.approve && !/Approved by/.test(c.text) && !/No back engraving/i.test(c.text) && !BAD.test(c.html), 'S3 Sheet tab: "Monogram waits for approval in Engraving", the same button, no seal: ' + JSON.stringify({ text: c.text.slice(0, 120), seals: c.seals }));
    await closeWin();
    await page.evaluate(k => OrderWin.open(k, { view: 'sheet' }), keyOf(M3, 0));
    await page.waitForFunction(() => OrderWin.view() === 'sheet' && OrderWin._sheet() && document.querySelector('#owSheetPanel .owPieces'), null, { timeout: 25000 });
    check(await waitPanel(TAB, 'monoCard'), 'S3 Sheet tab, the line of two pieces: a Monogram panel');
    const pick = async n => { await page.evaluate(n => { const li = [...document.querySelectorAll('#owSheetPanel .owPieces li')].find(l => /copy\s*(\d)/.test(l.textContent) && l.textContent.replace(/\s+/g, ' ').includes('copy ' + n + ' of')); if (li) li.click(); }, n); await sleep(700); };
    await pick(2); c = await panelOf(TAB);
    check(c.mono && /Piece 2 of 2/.test(c.text) && c.state === 'approve' && c.seals === 0 && /waits for approval/.test(c.text), 'S3 Sheet tab: piece 2 of the line (not approved) says it waits and has no seal: ' + JSON.stringify({ text: c.text.slice(0, 100) }));
    await pick(1); c = await panelOf(TAB);
    check(c.mono && /Piece 1 of 2/.test(c.text) && c.state === 'approved' && c.standIn === 1 && /Approved by Ben Test/.test(c.text), 'S3 Sheet tab: piece 1 (approved) has its own seal and "Approved by Ben Test": ' + JSON.stringify({ text: c.text.slice(0, 100) }));
    await closeWin();
    await page.evaluate(k => OrderWin.open(k, { view: 'sheet' }), keyOf(E1, 0));
    await page.waitForFunction(() => OrderWin.view() === 'sheet' && OrderWin._sheet() && document.querySelector('#owSheetPanel .owPieces'), null, { timeout: 25000 });
    await waitPanel(TAB); c = await panelOf(TAB);
    check(c.card && !c.mono && c.state === 'approved' && /Back engraving/.test(c.label) && c.engButton === 'Approved' && c.seals === 1 && /BACK ENGRAVING/.test(c.models) && c.words !== undefined, 'S3 Sheet tab: an engraved piece keeps its engraving card (Back engraving, Approved, its one BACK ENGRAVING seal on the button): ' + JSON.stringify({ label: c.label, state: c.state, models: c.models }));
    await closeWin();
    await page.evaluate(k => OrderWin.open(k, { view: 'sheet' }), keyOf(E2, 0));
    await page.waitForFunction(() => OrderWin.view() === 'sheet' && OrderWin._sheet() && document.querySelector('#owSheetPanel .owPieces'), null, { timeout: 25000 });
    await waitPanel(TAB); c = await panelOf(TAB);
    check(c.card && !c.mono && c.state === 'approve' && c.approve && c.seals === 0, 'S3 Sheet tab: an engraved piece still to approve keeps its card and its Approve button');
    await closeWin();

    // ═══ S3 · the Sheet window ═══
    const openWin = async o => {
      await page.evaluate(() => { try { SheetWin.close && SheetWin.close(); } catch (_) {} const d = document.querySelector('dialog.sheetWin[open]'); if (d) d.close(); }); await sleep(500);
      await page.evaluate(({ sh, ps }) => { SheetWin.open(sh, { select: ps }); }, { sh: SH, ps: o });
      await page.waitForFunction(() => SheetWin.isOpen && SheetWin.isOpen(), null, { timeout: 15000 }); await sleep(900);
    };
    await openWin(pool(M1, 0));
    check(await waitPanel(WIN, 'monoCard'), 'S3 Sheet window, the approved monogram: the Monogram panel');
    c = await panelOf(WIN);
    check(c.mono && c.state === 'approved' && /Approved by Ana Test/.test(c.text) && c.words === 'A P V' && c.standIn === 1 && c.seals === 1 && c.inside && c.button && !c.approve && !/No back engraving/i.test(c.text) && !BAD.test(c.html), 'S3 Sheet window: "Approved by Ana Test", the letters, its seal, View in Engraving: ' + JSON.stringify({ text: c.text.slice(0, 120) }));
    await page.screenshot({ path: path.join(SHOTS, 'S3-sheet-window-M1.png') });
    await page.click(WIN + ' [data-e=monogram]'); await page.waitForFunction(() => window.__mlinks.length >= 2, null, { timeout: 8000 }).catch(() => {});
    const l2 = await page.evaluate(() => __mlinks[__mlinks.length - 1]);
    check(l2 && l2.rid === M1.rid && l2.key === keyOf(M1, 0) && l2.closeFirst === 'function', 'S3 Sheet window: View in Engraving asks EngraveLink.openMonogram for that piece, with this window to close first: ' + JSON.stringify(l2));
    await sleep(500);
    await openWin(pool(M2, 0));
    check(await waitPanel(WIN, 'monoCard'), 'S3 Sheet window, the unapproved monogram: the Monogram panel');
    c = await panelOf(WIN);
    check(c.mono && c.state === 'approve' && /Monogram waits for approval in Engraving/.test(c.text) && c.seals === 0 && c.button && !c.approve && !BAD.test(c.html), 'S3 Sheet window: waits for approval, the same button, no seal');
    await openWin(pool(M3, 0, 2));
    check(await waitPanel(WIN, 'monoCard'), 'S3 Sheet window, piece 2 of the line of two: a Monogram panel');
    c = await panelOf(WIN);
    check(c.mono && /Piece 2 of 2/.test(c.text) && c.state === 'approve' && c.seals === 0, 'S3 Sheet window: piece 2 (not approved) has no seal');
    await openWin(pool(M3, 0, 1)); await waitPanel(WIN, 'monoCard'); c = await panelOf(WIN);
    check(c.mono && /Piece 1 of 2/.test(c.text) && c.standIn === 1 && /Approved by Ben Test/.test(c.text), 'S3 Sheet window: piece 1 has its seal, "Approved by Ben Test"');
    await openWin(pool(E1, 0)); await waitPanel(WIN); c = await panelOf(WIN);
    check(c.card && !c.mono && c.state === 'approved' && /Back engraving/.test(c.label) && c.engButton === 'Approved' && c.seals === 1 && /BACK ENGRAVING/.test(c.models), 'S3 Sheet window: an engraved piece keeps its engraving card and its one BACK ENGRAVING seal');
    await openWin(pool(E2, 0)); await waitPanel(WIN); c = await panelOf(WIN);
    check(c.card && !c.mono && c.state === 'approve' && c.approve, 'S3 Sheet window: an engraved piece to approve keeps its Approve button');
    await page.evaluate(() => { try { SheetWin.close && SheetWin.close(); } catch (_) {} const d = document.querySelector('dialog.sheetWin[open]'); if (d) d.close(); });

    // ═══ the page with no reader: nothing shown, nothing broken ═══
    await page.evaluate(() => { delete CharmNestMonogramUI.sealsOf; delete CharmNestMonogramUI.sealHtml; });
    await openList(); await sleep(1200); rs = await rowsOf();
    check(rs.filter(r => r.kind === 'monogram').length === 4 && rs.filter(r => r.kind === 'monogram').every(r => r.seals === 0 && r.text === ''), 'no reader: the Engrave list\'s monogram rows have no seals (and are still four rows)');
    await openOv(M1); f = await facts();
    check(f.chip === null && !f.cells.some(c2 => c2.label === 'Monogram'), 'no reader: no Monogram fact in the Overview');
    await closeWin();
    await page.evaluate(k => OrderWin.open(k, { view: 'sheet' }), keyOf(M1, 0));
    await page.waitForFunction(() => OrderWin.view() === 'sheet' && OrderWin._sheet() && document.querySelector('#owSheetPanel .owPieces'), null, { timeout: 25000 });
    await waitPanel(TAB, 'monoCard'); c = await panelOf(TAB);
    check(c.mono && c.seals === 0 && c.button && !BAD.test(c.html), 'no reader: the Sheet tab\'s panel shows no seal (and no BACK ENGRAVING)');
    await closeWin();

    console.log('   page errors:', JSON.stringify(errors), '· AI / Etsy writes:', writes.length);
    check(errors.length === 0, 'no page errors');
    check(writes.length === 0, 'no call to Claude, OpenAI or Etsy was made');
  } catch (e) { fails.push('crashed: ' + (e && e.stack || e)); console.log('  ✗ crashed: ' + (e && e.stack || e)); try { if (page) await page.screenshot({ path: path.join(SHOTS, 'crash.png') }); } catch (_) {} }
  finally { await Promise.resolve(browser.close()).catch(() => {}); try { await Promise.resolve(srv.close()); } catch (_) { /* gone with the process */ } }
  finish();
})().catch(e => { console.error(e); process.exit(1); });

function finish() {
  console.log(`\nmonogram-seal-surfaces: ${nCheck - fails.length}/${nCheck} checks passed` + (fails.length ? ' · FAILED: ' + fails.join(' | ') : ''));
  process.exit(fails.length ? 1 : 0);
}
