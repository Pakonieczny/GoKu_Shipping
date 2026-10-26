// MONOGRAM pipeline (MU4, 16 Oct 2026): the whole chain of one monogram order, WITHOUT a browser, on Paul's REAL 78-letter alphabet file (fixtures/SKUs_MONOGRAM_2024-1015.ai). Fast (about a minute and a half), the gate before a push.
//   1  the order is read:   the seven listings of the table, any title that says monogram; 'A.P.V' 'APV' 'apv' 'a p v', accents, a 2-letter and a 4-letter value, digits, emoji, non-Latin, empty -> special.monogram, the letters
//      (or the plain reason), no SKU / metal / option question, no engraving, the size option -> the envelope of the piece
//   2  the three alphabets: the 78 SKUs make three 26-letter families named Left / Mid / Right; a missing letter is named; letter 1 comes from the Left alphabet, 2 from the Middle, 3 from the Right (the page's own glyph() through a fake
//      library), only those three files are fetched, a letter typed back is not fetched again, a missing letter / a load that fails are plain errors and a retry works
//   3  the engine with the edited letters ON: Engine.build (and autoFit for a word that breaks a rule, as the card does): one connected piece, 14.8 mm tall, no NaN, report.metrics; the edits really are on by default (opts.edited false
//      gives the plain glyphs); the same letters give the same piece; all 78 letters are built in 26 spread words (monogram-sweep.cjs adds Paul's 50 words and the 20 worst)
//   4  the piece size: opts.fit 16 / 18 / 20 mm and the order's size option (SIZEFIT 18MM, 1.6 cm), through build and autoFit: the width never passes the envelope, the proportions stay, still ONE piece, metrics kept
//   5  the design file: toPdf = 'Monogram APV.ai' is a valid PDF-compatible .ai, re-read by the app's own reader as ONE closed piece of the right size, the right counters, no font and no text, the same bytes twice, a fitted piece too;
//      the cut-file preflight (CustomSheet.read's steps) refuses an empty / broken file with the plain message
//   6  the AI assembler offline: the page's loop (UI.assistLoop) with the REAL engine, the REAL server module (_charmNestMonogramAssist.run) and a FAKE anthropic; the real client is never loaded; a round is kept only if better;
//      absurd numbers are clamped; a malformed answer, no key, the kill switch and the day's 3 calls leave the automatic result
//   What is NOT here (it needs the page): CustomSheet.add / replace on re-approve / Send to Sheet, the canvas silhouettes, the widget: tests/charm-nest/monogram-flow.cjs.
//   node tests/charm-nest/monogram-pipeline.cjs        (run it through /tmp/cn-run.sh 600)
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..'), fnDir = path.join(root, 'netlify/functions');
global.ClipperLib = require(path.join(root, 'vendor/clipper-6.4.2.js'));
const O = require(path.join(root, 'charm-nest-orders.js'));
const M = require(path.join(root, 'charm-nest-monogram.js')), F = require(path.join(root, 'charm-nest-monogram-fit.js')), EDITS = require(path.join(root, 'charm-nest-monogram-edits.js'));
const UI = require(path.join(root, 'charm-nest-monogram-ui.js'));
const { CharmNestPDF: P, Geom, PDFLib } = require(path.join(fnDir, '_charmNestPdf.js'));
const R = require('./_monogramReal.cjs');
// the real model client is never loaded here: a stand-in takes its place in the require cache, and any call that reaches it is a bug of this test
const clientPath = path.join(fnDir, '_etsyMailAnthropic.js');
const clientStub = { __stub: true, strayCalls: 0, callClaudeRaw: async () => { clientStub.strayCalls++; throw new Error('no real model call in this test'); } };
require.cache[require.resolve(clientPath)] = { id: clientPath, filename: clientPath, loaded: true, exports: clientStub };

const T0 = Date.now(), fails = []; let nCheck = 0;
const check = (ok, msg) => { nCheck++; if (!ok) fails.push(msg); if (!ok || process.env.VERBOSE) console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
const sleep = ms => new Promise(r => setTimeout(r, ms));
const section = t => console.log('\n── ' + t);
const A = [...'ABCDEFGHIJKLMNOPQRSTUVWXYZ'];
const nv = r => (r && r.ok && r.report && r.report.metrics && r.report.metrics.violations ? r.report.metrics.violations.length : 99);
const dist = r => (r && r.report && r.report.metrics && Number.isFinite(+r.report.metrics.distance) ? +r.report.metrics.distance : 0);
const clone = r => JSON.parse(JSON.stringify(r));
const ringsKey = r => JSON.stringify(r.rings.map(g => g.pts));
const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 12, 17) / 1000);

// ── 1 helpers: an Etsy-shaped order line, read by the real orders module ──
const orderOf = rid => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + String(rid).slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: [] });
const lineOf = (o = {}) => Object.assign({ transactionId: '5001', listingId: '997905351', sku: 'MONOGRAM_5351', title: 'Monogram Necklace Personalized Jewelry Initials Pendant', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal choice', value: 'Gold' }, { name: 'Chain length', value: '16 inches' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: ['APV'] }, o);
const ctxOf = () => ({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: {}, masterEntry: () => null, masterLoose: () => '' });
const readLine = (l, c) => O.interpretLine(orderOf('4200000001'), l, c || ctxOf());
// the card's own reading of a line: the words the order holds, then the engine's lettersOf (charm-nest-monogram-ui.js wordsOf / buyerLetters)
const cardWords = sp => String((sp.special && sp.special.words) || '').trim() || (sp.personalization || []).join(' ').trim();
const TABLE = ['997905351', '606439317', '1844317367', '1009221671', '1844317391', '1008031671', '603425508'];

(async () => {
  const L = await R.load();

  // ═══ 1 · the order is read ═══
  section('1 · the order is read');
  {
    const WORDS = [   // what the buyer wrote -> the letters the card shows ('' = not three letters: the card says why and gives the three boxes)
      ['APV', 'APV'], ['A.P.V', 'APV'], ['A.P.V.', 'APV'], ['apv', 'APV'], ['a.p.v', 'APV'], ['a p v', 'APV'], ['A P V', 'APV'], ['  A  P  V  ', 'APV'], ['A-P-V', 'APV'], ['A, P, V', 'APV'], ['Initials: APV', 'APV'], ['Initials: A P V', 'APV'],
      ['Á-P-V', 'APV'], ['ÅÖÑ', 'AON'], ['A 😀 P 😀 V', 'APV'], ['ＡＰＶ', 'APV'], ['\n A\nP\nV ', 'APV'],
      ['AP', ''], ['A.P', ''], ['A', ''], ['APVX', ''], ['A.P.V.X', ''], ['Jane Doe', ''], ['123', ''], ['!!! ???', ''], ['😀😀😀', ''], ['', ''], ['   ', ''],
      ['АПВ', ''], ['ΑΠΒ', ''], ['山田太', ''], ['أبج', ''], ['אבג', '']      // (Cyrillic, Greek, Chinese, Arabic, Hebrew: not Latin letters, so nothing is guessed; a person types the three Latin letters)
    ];
    for (const id of TABLE) {
      for (const [text, want] of WORDS) {
        const sp = readLine(lineOf({ listingId: id, sku: 'MONOGRAM_' + id.slice(-4), personalization: [text] }));
        const mono = !!(sp.special && sp.special.monogram);
        const e = M.lettersOf(cardWords(sp));
        const ok = mono && sp.special.own === true && sp.problems.length === 0 && sp.ownDesign === true && sp.noDesign === false && sp.engraveCandidate === false && (want ? e.ok && e.letters.join('') === want : !e.ok && typeof e.why === 'string' && e.why.length > 3);
        if (!ok) check(false, `listing ${id}, buyer wrote ${JSON.stringify(text)}: monogram=${mono}, card words ${JSON.stringify(cardWords(sp))}, letters ${e.ok ? e.letters.join('') : 'none (' + e.why + ')'}, expected ${want || 'none'}; problems ${sp.problems.length}, engraveCandidate ${sp.engraveCandidate}`);
      }
    }
    check(true, `the seven listings of the table x ${WORDS.length} buyer strings: every line is a custom monogram (its own design, asks nothing, no engraving), the card reads the letters or says why not`);
    check(O.specialOf.addOn.monograms && Object.keys(O.specialOf.addOn.monograms).sort().join() === TABLE.slice().sort().join(), 'the listing table holds exactly the seven listings (MONOGRAM_LISTINGS)');
    // the orders module and the engine read the same letters (the orders module keeps its own short copy)
    for (const [text] of WORDS) { const a = M.lettersOf(text), b = O.specialOf.addOn.monoLettersIn(text); check((a.ok ? a.letters.join('') : null) === (b ? b.join('') : null), `the two readings agree on ${JSON.stringify(text)}`); }
    check(M.lettersOf('AP').why === '2 letters, a monogram needs 3' && M.lettersOf('APVX').why === '4 letters, a monogram needs 3' && /digit/.test(M.lettersOf('123').why) && /no letters/.test(M.lettersOf('АПВ').why), 'the plain reasons: 2 letters, 4 letters, only digits, no letters');
    // a title that says monogram (a listing outside the table) with exactly three letters; a bracelet with a name is not one
    const t1 = readLine(lineOf({ listingId: '777001', sku: 'NEWMONO_1', title: 'Custom Monogram Pendant Necklace, 3 Initials', personalization: ['J.Q.X'] }));
    check(t1.special && t1.special.monogram && t1.special.words === 'J.Q.X' && M.lettersOf(cardWords(t1)).letters.join('') === 'JQX' && /three letters \(J Q X\)/.test(t1.special.why), 'a title that says monogram + three letters in the box is read the same way (J.Q.X -> JQX), with the reason');
    check(!(readLine(lineOf({ listingId: '777002', sku: 'NEWMONO_2', title: 'Monogram Bracelet Personalized Name Bracelet', personalization: ['Amy'] })).special || {}).monogram, 'a monogram bracelet with a name is not a custom monogram');
    check(!(readLine(lineOf({ listingId: '777003', sku: 'HEART_CHARM', title: 'Heart Charm Necklace', personalization: ['A P V'] })).special || {}).monogram, 'a regular charm with three letters is not a monogram');
    // the size option: the envelope of the piece (SIZEFIT)
    const sized = (v) => readLine(lineOf({ variations: [{ name: 'Metal choice', value: 'Gold' }].concat(v ? [{ name: 'Size', value: v }] : []) })).size;
    check(sized(null) === null && sized('18MM') === '18MM' && sized('16 mm') === '16MM' && sized('Large') === 'L', 'the size option of the line: none, 18MM, "16 mm" -> 16MM, "Large" -> L: ' + [sized(null), sized('18MM'), sized('16 mm'), sized('Large')].join(' / '));
    check(F.fitFromSize(sized(null)) === null && JSON.stringify(F.fitFromSize(sized('18MM'))) === '{"maxWidthMm":18}' && F.fitFromSize(sized('Large')) === null && F.fitFromSize('1.6CM').maxWidthMm === 16 && F.fitFromSize('20MM').maxWidthMm === 20, 'the size is the maximum width of the piece (fitFromSize); no size or a size that is not in millimetres = the natural width');
  }

  // ═══ 2 · the three alphabets ═══
  section('2 · the three alphabets');
  const fams = M.familiesOf(L.skus), slots = M.slotsOf(fams, {});
  check(L.skus.length === 78 && L.skus.every(s => /^MONOGRAM_(LEFT|MID|RIGHT)_[A-Z]$/.test(s)), 'Paul\'s file gives 78 SKUs MONOGRAM_LEFT_A … MONOGRAM_MID_A … MONOGRAM_RIGHT_Z');
  check(fams.length === 3 && fams.every(f => f.count === 26 && f.missing.length === 0) && slots.ok && slots.left.base === 'MONOGRAM_LEFT_{L}' && slots.middle.base === 'MONOGRAM_MID_{L}' && slots.right.base === 'MONOGRAM_RIGHT_{L}' && ['left', 'middle', 'right'].every(k => slots.by[k] === 'name'), 'three 26-letter families, found by their names: Left, Middle, Right (no picking)');
  {
    const f2 = M.familiesOf(L.without('MONOGRAM_MID_Q')), s2 = M.slotsOf(f2, {});
    check(f2.find(f => f.base === 'MONOGRAM_MID_{L}').missing.join('') === 'Q' && s2.ok, 'a library without MONOGRAM_MID_Q: the Middle family names the missing Q');
    check(!M.slotsOf(M.familiesOf(L.skus.filter(k => !/RIGHT/.test(k))), {}).ok && /no Right alphabet/i.test(M.slotsOf(M.familiesOf(L.skus.filter(k => !/RIGHT/.test(k))), {}).why), 'no Right alphabet: the setup says so (and a person can choose another)');
    check(!M.slotsOf(M.familiesOf([]), {}).ok, 'no alphabet at all: the setup state');
  }
  // the page's own glyph() (charm-nest-monogram-ui.js) over a fake library that serves Paul's per-SKU master files
  const service = (skus, o = {}) => {
    const fetched = [], entries = new Map(skus.map(s => [s, { sku: s, aiPath: 'charmnest/master/' + s + '.ai', indexedAt: 1 }])), calls = { n: 0 };
    const d = { rows: () => [], skus: () => skus, settings: () => ({}), saveSettings() {}, toast() {}, changed() {}, query: () => '', CNProgress: null, Engine: M, Geom, P, itemOf: () => null,
      CustomSheet: { cardOf: () => null, entries: () => ({}), add() {}, remove() {}, open() {} },
      Master: { entryFor: s => entries.get(s) || null, fetchEntry: async s => entries.get(s) || null },
      Pool: { masterCharm: async e => { fetched.push(e.sku); calls.n++; if (o.fail && o.fail(e.sku, calls.n)) throw new Error('network down'); return L.masterCharm(e); } } };
    return { ui: UI.make(d), fetched };
  };
  {
    const { ui, fetched } = service(L.skus), abc = ui.alphabets();
    check(abc.ok && abc.left.name === 'Monogram Left' && abc.middle.name === 'Monogram Mid' && abc.right.name === 'Monogram Right', 'the page finds the three alphabets: ' + [abc.left, abc.middle, abc.right].map(f => f && f.name).join(' · '));
    const g = await Promise.all([ui.glyph('left', 'A', abc), ui.glyph('middle', 'P', abc), ui.glyph('right', 'V', abc)]);
    check(JSON.stringify(fetched.slice().sort()) === JSON.stringify(['MONOGRAM_LEFT_A', 'MONOGRAM_MID_P', 'MONOGRAM_RIGHT_V']), 'APV: letter 1 from the LEFT alphabet, 2 from the MIDDLE, 3 from the RIGHT, and only those three files are fetched: ' + fetched.join(' '));
    check(g.map(x => x.sku).join() === 'MONOGRAM_LEFT_A,MONOGRAM_MID_P,MONOGRAM_RIGHT_V' && g.every(x => x.rings.length >= 1 && x.fill === 'auto' && x.metrics && x.metrics.baselinePt != null), 'each glyph is the letter\'s outline with its counters, its baseline and its font size');
    const direct = await L.word('APV'), r1 = M.build({ left: g[0], middle: g[1], right: g[2] }), r2 = M.build(direct);
    check(R.sound(r1).length === 0 && ringsKey(r1) === ringsKey(r2), 'built from what the page fetched = built from the file directly (same piece, point for point)');
    await ui.glyph('middle', 'P', abc); await ui.glyph('left', 'A', abc);
    check(fetched.length === 3, 'a letter typed back is not fetched again');
    await ui.glyph('middle', 'S', abc);
    check(fetched.length === 4 && fetched[3] === 'MONOGRAM_MID_S', 'a changed letter fetches only that letter: ' + fetched.slice(3).join(' '));
  }
  {
    const { ui } = service(L.without('MONOGRAM_MID_Q')), abc = ui.alphabets();
    let err = null; try { await ui.glyph('middle', 'Q', abc); } catch (e) { err = e; }
    check(err && err.kind === 'missing' && /Middle alphabet has no “Q”/.test(err.message) && /Monogram Mid has no Q yet\. Add it to the library/.test(err.help), 'a letter missing from an alphabet is a plain error naming it: "' + (err && err.message) + '" / "' + (err && err.help) + '"');
    check((await ui.glyph('left', 'Q', abc)).sku === 'MONOGRAM_LEFT_Q', 'the same letter of the other alphabets is still there');
  }
  {
    const { ui, fetched } = service(L.skus, { fail: (sku, n) => n === 1 }), abc = ui.alphabets();
    let err = null; try { await ui.glyph('left', 'A', abc); } catch (e) { err = e; }
    check(err && err.kind === 'load', 'a letter file that fails to load is a load error (Try again), not a missing letter');
    const again = await ui.glyph('left', 'A', abc);
    check(again && again.sku === 'MONOGRAM_LEFT_A' && fetched.length === 2, 'and Try again fetches it again (the failure is not kept)');
  }

  // ═══ 3 · the engine with the edited letters ON ═══
  section('3 · the engine, edited letters on');
  const natural = {}, plainOf = {};      // word -> the finished piece as the card shows it (the plain build, or autoFit's when that is better), and the plain build
  const make = async w => { let r = M.build(await L.word(w)); plainOf[w] = r; if (nv(r) && typeof M.autoFit === 'function') { const f = M.autoFit(await L.word(w), { autoFitMs: 4000 }); if (nv(f) < nv(r) || (nv(f) === nv(r) && dist(f) < dist(r))) r = f; } return r; };
  for (const w of ['APV', 'OSR', 'JQX']) {
    const t0 = Date.now(); natural[w] = await make(w); const r = natural[w], bad = R.sound(r);
    check(bad.length === 0, `${w}: ${bad.join('; ') || `one connected piece, ${r.wMm.toFixed(1)} x ${r.hMm.toFixed(1)} mm, ${r.rings.filter(g => g.hole).length} counters, ${r.report.metrics.violations.length} rule(s) broken (${Date.now() - t0} ms)`}`);
    check(r.placed.map(p => p.slot + ':' + p.letter).join(' ') === `left:${w[0]} middle:${w[1]} right:${w[2]}`, `${w}: Left ${w[0]}, Middle ${w[1]}, Right ${w[2]} in the piece`);
    const hOf = p => p.bboxPt[3] - p.bboxPt[1]; check(hOf(r.placed[1]) > Math.max(hOf(r.placed[0]), hOf(r.placed[2])), `${w}: the centre letter is the biggest`);
  }
  {
    // the checks can fail: a piece in two parts, a NaN, a wrong height, no metrics, a counter outside, a broken build are all caught (a gate that cannot fail proves nothing)
    const r = natural.APV, mut = f => { const c = clone(r); f(c); return R.sound(c); };
    check(R.sound(r).length === 0, 'control: the good piece is sound');
    check(mut(c => c.rings.push({ pts: c.rings[0].pts.map(p => [p[0] + 40, p[1]]), hole: false })).some(b => /outer rings/.test(b)), 'control: two outer rings are caught');
    check(mut(c => { c.rings[1].pts[0][0] = NaN; }).some(b => /NaN/.test(b)) && mut(c => { c.hMm = Infinity; }).some(b => /NaN/.test(b)), 'control: NaN / Infinity are caught');
    check(mut(c => { c.hMm = 17; c.bboxPt[3] = 17 / R.MM; }).some(b => /tall/.test(b)), 'control: a piece 17 mm tall is caught');
    check(mut(c => { c.report.metrics = null; }).some(b => /metrics/.test(b)) && mut(c => { c.report.connected = false; }).some(b => /connected/.test(b)), 'control: missing metrics and connected:false are caught');
    check(mut(c => { c.rings.push({ pts: [[900, 900], [901, 900], [901, 901]], hole: true }); }).some(b => /outside/.test(b)) && R.sound({ ok: false, why: 'no touch' })[0].startsWith('not built'), 'control: a counter outside the outline and a refused build are caught');
  }
  {
    // the edited letters are on by default; edited:false gives the plain glyphs. Which letters are edited: the edits table of ED-X
    const inSet = { left: Object.keys(EDITS.L), middle: Object.keys(EDITS.M), right: Object.keys(EDITS.R) };
    check(inSet.left.length > 5 && inSet.middle.length > 5 && inSet.right.length > 5, `the edits table holds letters of all three alphabets (L ${inSet.left.length}, M ${inSet.middle.length}, R ${inSet.right.length})`);
    const w = inSet.left[0] + inSet.middle[0] + inSet.right[0], raw = await L.word(w);
    const pre = (f, l) => M.preparedLetter(f, l.letter, l);
    check(['L', 'M', 'R'].every((f, i) => pre(f, [raw.left, raw.middle, raw.right][i]).edited === true), `${w}: every letter of the word is an edited one, and preparedLetter says so`);
    const on = M.build(raw), off = M.build(raw, { edited: false });
    check(R.sound(on).length === 0 && R.sound(off).length === 0 && ringsKey(on) !== ringsKey(off), `${w}: the default build uses the edited letters (a different piece from opts.edited:false), both are sound pieces`);
    const again = M.build(raw); check(ringsKey(again) === ringsKey(on) && again.wMm === on.wMm, 'the same letters give the same piece, point for point');
    const calib = await L.word('ESG'), e1 = M.build(calib), e2 = M.build(calib, { edited: false }); check(R.sound(e1).length === 0 && e1.report.metrics.violations.length === 0 && Math.abs(e1.wMm - e2.wMm) < 0.5 && Math.abs(e1.hMm - e2.hMm) < 0.2, `ESG, the reference, with the edited letters (the right G is one, Paul's ED-P) stays inside every rule and its size: ${e1.wMm.toFixed(2)} x ${e1.hMm.toFixed(2)} mm against ${e2.wMm.toFixed(2)} x ${e2.hMm.toFixed(2)} mm with the plain glyphs`);
  }
  {
    // all 78 letters: 26 words that use every letter of every alphabet once (the 50 words and the 20 worst: monogram-sweep.cjs)
    const SPREAD = A.map((_, i) => A[i] + A[(i * 7 + 3) % 26] + A[(i * 11 + 5) % 26]);
    check(['left', 'middle', 'right'].every((s, k) => new Set(SPREAD.map(w => w[k])).size === 26), 'the 26 spread words use every letter in every slot');
    const bad = [], broken = []; let hMin = 99, hMax = 0, ms = 0, slow = ['', 0];
    for (const w of SPREAD) {
      const t0 = Date.now(); let r = null; try { r = M.build(await L.word(w)); } catch (e) { bad.push(w + ' threw ' + e.message); continue; }
      const t = Date.now() - t0; ms += t; if (t > slow[1]) slow = [w, t];
      const b = R.sound(r); if (b.length) { bad.push(w + ': ' + b.join('; ')); continue; }
      hMin = Math.min(hMin, r.hMm); hMax = Math.max(hMax, r.hMm); if (nv(r)) broken.push(w);
    }
    check(bad.length === 0, `all 78 letters in 26 words: each one connected piece, ~14.8 mm tall (${hMin.toFixed(2)}-${hMax.toFixed(2)} mm), no NaN, report.metrics present; ${broken.length} of them still break a rule (the card then runs autoFit and the AI); ${Math.round(ms / 26)} ms each, slowest ${slow[0]} ${slow[1]} ms` + (bad.length ? ': ' + bad.join(' | ') : ''));
  }

  // ═══ 4 · the piece size ═══
  section('4 · the piece size (16 / 18 / 20 mm, the order\'s size option)');
  const fittedOf = {};
  for (const w of ['APV', 'OSR']) {
    for (const mm of F.ENVELOPES) {
      // (MONOHOOPS, Paul 19 Oct 2026: the hoops keep their real size, so the proportions kept are the LETTERS', report.hoops.lettersMm)
      const lettersAspect = x => { const lm = x.report.hoops && x.report.hoops.lettersMm; return lm ? lm[0] / lm[1] : x.wMm / x.hMm; };
      const fitted = M.build(await L.word(w), { fit: { maxWidthMm: mm } }), sc = fitted.report.fit && fitted.report.fit.scale; fittedOf[w + mm] = fitted;
      // (a plain build, not the autoFit result: the fit scales the finished layout, it never re-judges it)
      const plain = plainOf[w], want = Math.min(1, mm / plain.wMm), bad = R.sound(fitted, { scale: want });
      check(bad.length === 0 && fitted.wMm <= mm + 1e-3 && Math.abs(sc - want) < 1e-3 && Math.abs(lettersAspect(fitted) - lettersAspect(plain)) < 1e-3 && JSON.stringify(fitted.report.metrics.violations) === JSON.stringify(plain.report.metrics.violations),
        `${w} at ${mm} mm: ${bad.join('; ') || `${fitted.wMm.toFixed(2)} x ${fitted.hMm.toFixed(2)} mm (scale ${want.toFixed(3)}), same proportions, one piece, same judged rules`}`);
      check(fitted.report.fit && fitted.report.fit.maxWidthMm === mm && (want < 1 ? fitted.report.fit.applied === true : fitted.report.fit.applied === false), `${w} at ${mm} mm: report.fit says what was done`);
    }
  }
  {
    const fit = F.fitFromSize('18MM'), a = M.autoFit(await L.word('JQX'), { fit, autoFitMs: 2500 });
    check(R.sound(a, { scale: Math.min(1, 18 / a.report.fit.designMm[0]) }).length === 0 && a.wMm <= 18 + 1e-3 && a.report.fit && a.report.fit.maxWidthMm === 18, `autoFit with the size option 18MM: ${a.wMm.toFixed(2)} x ${a.hMm.toFixed(2)} mm, sound`);
    const none = M.build(await L.word('APV'), { fit: F.fitFromSize('Large') || undefined });
    check(R.sound(none).length === 0 && !none.report.fit && none.wMm === plainOf.APV.wMm, 'a size that is no size (null envelope) leaves the natural piece (no report.fit)');
    const big = M.build(await L.word('APV'), { fit: F.fitFromSize('40MM') }); check(big.report.fit.applied === false && Math.abs(big.wMm - plainOf.APV.wMm) < 1e-9, 'a roomy size (40 mm) never enlarges the piece (it only shrinks)');
  }

  // ═══ 5 · the design file ═══
  section('5 · the design file (Monogram APV.ai)');
  {
    const r = natural.APV, bytes = await M.toPdf(r, { title: 'Monogram APV' }), again = await M.toPdf(r, { title: 'Monogram APV' });
    check(Buffer.from(bytes.slice(0, 5)).toString() === '%PDF-' && /^Monogram [A-Z]{3}\.ai$/.test('Monogram APV.ai') && UI.FILE_RE.test('Monogram APV.ai') && !UI.FILE_RE.test('Monogram AP.ai') && UI.FILE_RE.exec('Monogram APV.ai')[1] === 'APV', 'a PDF-compatible .ai named Monogram APV.ai (the name the page finds an approved design by)');
    check(Buffer.compare(Buffer.from(bytes), Buffer.from(again)) === 0, 'the same piece gives the same bytes twice');
    const raw = Buffer.from(bytes).toString('latin1');
    check(!/\/Type\s*\/Font\b/.test(raw) && !/\bBT\b/.test(raw) && !/\/Subtype\s*\/(TrueType|Type1|Type0)/.test(raw), 'no font and no text in the file: only closed cut paths');
    const doc = await PDFLib.PDFDocument.load(bytes), title = doc.getTitle(), pg = doc.getPage(0).getSize();
    check(title === 'Monogram APV' && doc.getPageCount() === 1 && pg.width > r.bboxPt[2] && pg.height > r.bboxPt[3], 'one page, titled Monogram APV, larger than the piece by the page margin: ' + pg.width.toFixed(1) + ' x ' + pg.height.toFixed(1) + ' pt');
    const pf = await R.preflight(bytes, 'Monogram APV.ai');
    check(pf.pieces === 1 && Math.abs(pf.wMm - r.wMm) < 0.05 && Math.abs(pf.hMm - r.hMm) < 0.05, `the cut-file preflight reads it as ONE closed piece of ${pf.wMm.toFixed(2)} x ${pf.hMm.toFixed(2)} mm (the preview says ${r.wMm.toFixed(2)} x ${r.hMm.toFixed(2)})`);
    // the cut lines: the outer ring and every counter, as the reader sees them
    const parsed = await P.parseSource(bytes, 'Monogram APV.ai'), g = P.groupCharms(parsed, { minPt: 6 }), live = g.charms.filter(c => c.mergedInto == null);
    const charm = live[0], cuts = P.cutLinesOf(charm);
    check(live.length === 1 && g.orphans.length === 0 && 1 + cuts.length === r.rings.length, `the reader finds one charm: its outline + ${cuts.length} cut lines = the ${r.rings.length} rings of the preview (${r.rings.filter(x => x.hole).length} counters)`);
    check(r.rings.every(x => x.pts.length >= 3) && r.rings.length === parsed.segments.length, 'and nothing else is in the file (every drawn path is one ring)');
    // a fitted piece: the file is the piece at its size
    for (const mm of F.ENVELOPES) {
      const fr = fittedOf['APV' + mm], fb = await M.toPdf(fr, { title: 'Monogram APV' }), fp = await R.preflight(fb, 'Monogram APV.ai');
      check(fp.pieces === 1 && Math.abs(fp.wMm - fr.wMm) < 0.05 && Math.abs(fp.hMm - fr.hMm) < 0.05 && fp.wMm <= mm + 0.05, `the file of the ${mm} mm piece is ${fp.wMm.toFixed(2)} x ${fp.hMm.toFixed(2)} mm (one piece)`);
    }
    // the file survives an Illustrator-style round trip through the page reader: re-saved bytes read the same
    const resaved = await doc.save({ useObjectStreams: false }), rp = await R.preflight(new Uint8Array(resaved), 'Monogram APV.ai');
    check(rp.pieces === 1 && Math.abs(rp.wMm - pf.wMm) < 0.01, 'a re-saved copy of the file reads the same');
    // a design written for a changed letter replaces by NAME: another word, another name
    const o2 = natural.OSR, b2 = await M.toPdf(o2, { title: 'Monogram OSR' }); check((await R.preflight(b2, 'Monogram OSR.ai')).pieces === 1 && Buffer.compare(Buffer.from(b2), Buffer.from(bytes)) !== 0, 'another word is another file (OSR)');
    // a monogram that was never built cannot be written
    let e1 = null; try { await M.toPdf({ ok: false, why: 'x' }, {}); } catch (e) { e1 = e; } check(e1 && /no finished monogram/.test(e1.message), 'a monogram that was not built cannot be written: ' + (e1 && e1.message));
    // the preflight refuses what is not a design, with the plain message
    for (const [what, buf, re] of [['text', new Uint8Array(Buffer.from('hello')), /PDF-compatible/], ['an empty page', new Uint8Array(await (async () => { const d = await PDFLib.PDFDocument.create(); d.addPage([100, 100]); return d.save(); })()), /no closed cut outline/]]) {
      let err = null; try { await R.preflight(buf, 'x.ai'); } catch (e) { err = e; } check(err && re.test(err.message), `the cut-file preflight refuses ${what}: "${err && err.message}"`);
    }
  }

  // ═══ 6 · the AI assembler, offline ═══
  section('6 · the AI assembler (fake anthropic, real engine, real server module)');
  {
    const PNG = 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==';
    const IDENT = { scale: 1, sx: 1, dx: 0, dy: 0 };
    const fakeDb = () => {
      const docs = new Map(); let lock = Promise.resolve();
      const merge = (cur, patch) => { const out = Object.assign({}, cur); for (const [k, v] of Object.entries(patch)) out[k] = v && v.__union ? (out[k] || []).concat(v.items) : v; return out; };
      const doc = (coll, id) => ({ get: async () => ({ exists: docs.has(coll + '/' + id), id, data: () => docs.get(coll + '/' + id) }), set: async (v, o) => { const k = coll + '/' + id; docs.set(k, o && o.merge ? merge(docs.get(k) || {}, v) : merge({}, v)); } });
      const db = { docs, collection: c => ({ doc: id => doc(c, id) }), runTransaction: fn => { const run = lock.then(async () => { const writes = []; const out = await fn({ get: r => r.get(), set: (r, v, o) => writes.push([r, v, o]) }); for (const [r, v, o] of writes) await r.set(v, o); return out; }); lock = run.catch(() => {}); return run; } };
      return { db, FV: { arrayUnion: (...items) => ({ __union: true, items }), serverTimestamp: () => Date.now() } };
    };
    const reply = obj => ({ stop_reason: 'end_turn', usage: { input_tokens: 1200, output_tokens: 80, cache_read_input_tokens: 900 }, content: [{ type: 'text', text: typeof obj === 'string' ? obj : JSON.stringify(obj) }] });
    const fakeAnthropic = answer => { const calls = []; return { calls, callClaudeRaw: async a => { calls.push(a); return answer(a, calls.length); } }; };
    const freshAssist = env => { Object.assign(process.env, env || {}); delete require.cache[require.resolve(path.join(fnDir, '_charmNestMonogramAssist.js'))]; return require(path.join(fnDir, '_charmNestMonogramAssist.js')); };
    for (const k of ['ANTHROPIC_API_KEY', 'CHARM_NEST_MONOGRAM_AI', 'CHARM_NEST_MONOGRAM_MODEL', 'CHARM_NEST_MONOGRAM_EFFORT']) delete process.env[k];
    const Assist = freshAssist();
    // a word that really breaks a rule in the engine (found among Paul's 50): the plain build is the base the loop improves
    const cand = ['ECO', 'RBK', 'PWR', 'VWW', 'KWQ'], found = [];
    for (const w of cand) { const r = M.build(await L.word(w)); if (nv(r) > 0 && nv(r) < 99) found.push([w, r]); if (found.length >= 2) break; }
    check(found.length >= 1, 'a real word that breaks a rule is available for the assembler: ' + found.map(f => f[0] + ' (' + nv(f[1]) + ')').join(' '));
    const [word, base] = found[0] || ['ECO', M.build(await L.word('ECO'))], parts = await L.word(word);
    const ask = (db, FV, fa, log) => async req => {
      const image = 'data:image/png;base64,' + PNG.split(',')[1];
      const svg = UI.assistSvg(M, req.res);       // (the picture the page would rasterise: it must be the cut piece itself)
      log.push({ req, svg });
      return Assist.run({ orderKey: '4200683017_42006830171', letters: word.split(''), round: req.round, adjust: req.adjust, metrics: req.metrics, violations: req.violations, history: req.history, image }, { db, FV, anthropic: fa, refPng: null, now: Date.UTC(2026, 9, 16, 12) });
    };
    // 1 · a sensible nudge: whatever the real engine makes of it, the result is never worse and always a sound piece
    {
      const { db, FV } = fakeDb(), log = [], nudge = { adjust: { L: { scale: 1.06, sx: 1.12, dx: 0.4, dy: -0.2 }, M: IDENT, R: { scale: 1.04, sx: 1.1, dx: -0.3, dy: -0.2 } }, why: 'Widened both sides a little and moved them toward the centre.' };
      const fa = fakeAnthropic(async () => reply(nudge));
      const out = await UI.assistLoop({ E: M, parts, base, rounds: 3, ask: ask(db, FV, fa, log) });
      const b = R.sound(out.res);
      check(['assembled', 'needs'].includes(out.status) && out.calls >= 1 && out.calls <= 3 && fa.calls.length === out.calls && b.length === 0 && nv(out.res) <= nv(base), `a nudge from the fake model on ${word}: ${out.status}, ${out.calls} call(s), rules broken ${nv(base)} -> ${nv(out.res)}, sound piece ${b.join('; ')}`);
      check(log.every(l => /<svg[^>]*>/.test(l.svg.svg) && l.svg.svg.includes(M.svgPath(l.req.res).replace(/&/g, '&amp;')) && l.svg.w > 100), 'the picture handed to the model is the cut piece (its path) with the contacts painted');
      check(fa.calls.every(c => c.system && c.system[0].cache_control && Array.isArray(c.messages[0].content) && c.messages[0].content.some(x => x.type === 'image')), 'every request carries the cached system prompt and the pictures');
      if (out.accepted) check(!!out.why && Object.values(out.adjust).every(a => a.scale >= 0.75 && a.scale <= 1.3), 'a kept round carries the model\'s sentence and numbers inside the engine\'s bounds');
      check(clientStub.strayCalls === 0, 'the real model client was never reached');
    }
    // 2 · absurd numbers are clamped (server) and the engine copes (no NaN, one piece)
    {
      const { db, FV } = fakeDb(), log = [], wild = { adjust: { L: { scale: 9, sx: -3, dx: 50, dy: 77 }, M: { scale: 0, sx: 99, dx: -50, dy: 9 }, R: { scale: 'big', sx: null, dx: 1e9, dy: NaN } }, why: 'x'.repeat(900) };
      const fa = fakeAnthropic(async () => reply(wild));
      const out = await UI.assistLoop({ E: M, parts, base, rounds: 3, ask: ask(db, FV, fa, log) });
      check(R.sound(out.res).length === 0 && nv(out.res) <= nv(base) && out.calls <= 3 && out.why.length <= 240, `absurd numbers (scale 9, dx 50, NaN …): clamped, the result is a sound piece and not worse (${out.status}, ${out.calls} calls)`);
      const first = await Assist.run({ orderKey: 'k', letters: ['A', 'P', 'V'], round: 1, adjust: {}, image: PNG, violations: [], metrics: {} }, { db: fakeDb().db, FV, anthropic: fakeAnthropic(async () => reply(wild)), refPng: null });
      check(first.adjust && first.adjust.L.scale === 1.3 && first.adjust.L.dx === 2 && first.adjust.M.scale === 0.8 && first.adjust.M.sx === 1.25 && first.clamped, 'the server clamps to the contract: Left scale 1.3, dx 2; Middle scale 0.8, sx 1.25');
    }
    // 3 · failures leave the automatic result
    for (const [name, answer, status] of [['malformed JSON', async () => reply('this is not json'), 'failed'], ['a refusal', async () => ({ stop_reason: 'refusal', content: [] }), 'failed'], ['a throw', async () => { throw new Error('network down'); }, 'failed']]) {
      const { db, FV } = fakeDb(), log = [], fa = fakeAnthropic(answer);
      const out = await UI.assistLoop({ E: M, parts, base, rounds: 3, ask: ask(db, FV, fa, log) });
      check(out.status === status && out.res === base && out.accepted === 0 && out.calls === 1, `${name}: the loop stops after one call, the automatic result is left as it is (${out.status}: ${out.reason})`);
    }
    {
      const { db, FV } = fakeDb(), log = [], fa = fakeAnthropic(async () => reply({ adjust: { L: IDENT, M: IDENT, R: IDENT }, why: 'nothing to change' }));
      const out = await UI.assistLoop({ E: M, parts, base, rounds: 3, ask: ask(db, FV, fa, log) });
      check(out.status === 'needs' && out.calls === 3 && out.accepted === 0 && out.res === base && out.remaining.length === nv(base), 'a model that changes nothing: 3 rounds, nothing kept, "needs a person" with the rules still broken named');
    }
    // 4 · no key / the kill switch / the day's limit
    {
      const { db, FV } = fakeDb(), log = [];
      let out = await UI.assistLoop({ E: M, parts, base, rounds: 3, ask: ask(db, FV, undefined, log) });
      check(out.status === 'skipped' && out.res === base && /ANTHROPIC_API_KEY/.test(out.reason), 'no ANTHROPIC_API_KEY on the server: skipped, nothing asked, the automatic result stays: ' + out.reason);
      const A2 = freshAssist({ CHARM_NEST_MONOGRAM_AI: 'off' }); const fa = fakeAnthropic(async () => reply({ adjust: { L: IDENT, M: IDENT, R: IDENT }, why: 'x' }));
      const sk = await A2.run({ orderKey: 'k', letters: ['A', 'P', 'V'], round: 1, adjust: {}, image: PNG }, { db, FV, anthropic: fa });
      check(sk.skipped && /switched off/.test(sk.skipped) && fa.calls.length === 0, 'the kill switch (CHARM_NEST_MONOGRAM_AI=off): skipped, the model is never called');
      delete process.env.CHARM_NEST_MONOGRAM_AI; freshAssist();
      const lim = fakeAnthropic(async () => reply({ adjust: { L: IDENT, M: IDENT, R: IDENT }, why: 'nothing' })), shared = fakeDb(), got = [];
      for (let i = 0; i < 5; i++) got.push(await Assist.run({ orderKey: 'k2', letters: ['A', 'P', 'V'], round: 1, adjust: {}, image: PNG }, { db: shared.db, FV: shared.FV, anthropic: lim, refPng: null, now: Date.UTC(2026, 9, 16, 12) }));
      check(lim.calls.length === 3 && got.slice(0, 3).every(x => x.adjust) && got[3].limited && got[4].limited, 'the day\'s 3 calls per order: the 4th and 5th are refused without a call');
    }
    check(clientStub.strayCalls === 0 && !process.env.ANTHROPIC_API_KEY, 'at the end: no real model call was made, no key was set');
  }

  console.log(`\n   (${Math.round((Date.now() - T0) / 1000)} s)`);
  console.log(fails.length ? `\nmonogram-pipeline: ${fails.length} FAILED of ${nCheck}:\n - ${fails.join('\n - ')}` : `\nmonogram-pipeline OK: ${nCheck} checks`);
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
