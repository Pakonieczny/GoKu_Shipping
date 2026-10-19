// PIECES · MONOGRAMS, the service without a browser (PM, 16 Oct 2026, order 4195059483 "Mandy Bellamy": "ALL multipiece orders must always show each individual piece with respect to all Engravings and Monograms").
// A monogram line of quantity N is N pieces (pieceKey "<lineKey>#C<u>", a line of one piece keeps its line's key). This is the page's own monogram service (charm-nest-monogram-ui.js make()) over Paul's REAL 78-letter alphabet file and the
// real engine, with a FAKE CustomSheet that keeps the card's files the way the real one does (add: qty, metal, tag; remove). The real CustomSheet, the widgets, the lists and the Order window are in pieces-monogram.cjs (browser).
//   1  the pieces of a line: quantity 1 is one piece with the line's own key (byte for byte what it was), quantity N is N pieces, keys <line>#C1..#CN, pool ids <line>_1..N
//   2  the buyer's words per piece: the same three letters on every piece (default); "ABC / XYZ", "ABC & XYZ", "ABC and XYZ", "ABC, XYZ", two lines, two entries, "1: ABC 2: XYZ", "ABC XYZ" give piece i part i; a count that does not match,
//      or a part that is not three letters, gives every piece the whole words (never a guess)
//   3  approve ONE piece: its own design, quantity 1, named for its piece, tagged with its line and slot; the other piece is untouched and still to approve; both approved = two designs, in the order of the pieces
//   4  changing a piece after its approval replaces ONLY its design (never two designs for one piece); the other piece's design, name and metal stay; the replacement keeps the metal of the design it replaces
//   5  identical letters on two pieces are still two designs (one each, quantity 1): never one design of quantity 2, never two designs for one piece
//   6  MIGRATION: a design approved before pieces ("Monogram APV.ai", quantity 2, no piece in its name) reads as pieces 1 and 2 BOTH approved, nothing asked again; changing piece 2 (or 1) afterwards splits it in place into one design for each
//      piece it covered (the same file copied) and replaces only that piece's; a quantity-1 design is exactly what it was (name, quantity, no tag)
//   7  sent: a card sent to the sheets says which file each copy went as (sent.lines): each piece reads "sent" with ITS letters; a sent piece is never changed
//   9  the order model: each monogram piece is a piece of its order (pieceKey, slot, label, state, count, step, strip)
//   8  states: the counts (pieces waiting), the status of each piece, the events' piece fields; a quantity change adds / removes pieces; two pieces with the same letters ask the model ONCE
//   node tests/charm-nest/pieces-monogram-service.cjs        (run it through /tmp/cn-run.sh 600)
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
global.ClipperLib = require(path.join(root, 'vendor/clipper-6.4.2.js'));
const M = require(path.join(root, 'charm-nest-monogram.js')), UI = require(path.join(root, 'charm-nest-monogram-ui.js'));
const Sides = require(path.join(root, 'charm-nest-engrave-sides.js'));
const { CharmNestPDF: P, Geom } = require(path.join(root, 'netlify/functions/_charmNestPdf.js'));
const R = require('./_monogramReal.cjs');
const fails = []; let nCheck = 0;
const check = (ok, msg) => { nCheck++; if (!ok) fails.push(msg); if (!ok || process.env.VERBOSE) console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
const section = t => console.log('\n── ' + t);

(async () => {
  const T0 = Date.now(), L = await R.load();
  // a monogram order line as the page holds it
  const lineOf = (rid, tx, qty, words, extra) => ({ key: `${rid}_${tx}`, state: 'pulled', order: { receiptId: rid }, line: { transactionId: tx, quantity: qty, sku: 'MONOGRAM_5351' }, poolIds: [], spec: Object.assign({ quantity: qty, personalization: [words], special: { monogram: true, words }, material: 'gold' }, extra || {}) });
  // the page's service over a fake library and a fake CustomSheet that keeps files as the real one does
  function world(rows, o = {}) {
    o = o || {};
    const store = {}, entries = new Map(L.skus.map(s => [s, { sku: s, aiPath: 'charmnest/master/' + s + '.ai', indexedAt: 1 }])), toasts = [], asks = [], changes = { n: 0 };
    let ids = 0;
    const ckOf = it => it.key;
    const CS = {
      entries: () => store,
      cardOf: it => (it ? { ck: ckOf(it) } : null),
      add(it, files, opts = {}) {
        const ck = ckOf(it), e = store[ck] || (store[ck] = { ck, rid: it.rid, files: [], sent: null });
        if (e.sent) { toasts.push('sent'); return; }
        const last = e.files[e.files.length - 1], metal = opts.metal || (last ? last.metal : 'gold'), qty = opts.qty >= 1 ? Math.min(99, Math.round(opts.qty)) : (o.defaultQty || 1);
        for (const f of files) e.files.push(Object.assign({ id: 'f' + (++ids), name: f.name, size: f.size, state: 'ready', metal, qty, pieces: 1, wMm: 14, hMm: 14.8, bytes: new Uint8Array([1, 2, 3]) }, opts.mono ? { mono: Object.assign({ line: String(opts.mono.line), slot: opts.mono.slot || null }, opts.mono.hoops ? { hoops: opts.mono.hoops } : {}) } : {}));
      },
      remove(ck, id) { const e = store[ck]; if (!e || e.sent) return; e.files = e.files.filter(f => f.id !== id); if (!e.files.length) delete store[ck]; },
      open() {}
    };
    const d = { rows: () => rows, skus: () => L.skus, settings: () => o.settings || (o.settings = {}), saveSettings() {}, toast: m => toasts.push(m), changed: () => { changes.n++; }, CNProgress: null, Engine: M, Geom, P, Sides,
      itemOf: row => ({ key: 'ord:custom:' + row.order.receiptId + ':X', rid: row.order.receiptId, rows: [row], row }), CustomSheet: CS,
      Master: { entryFor: s => entries.get(s) || null, fetchEntry: async s => entries.get(s) || null }, Pool: { masterCharm: async e => L.masterCharm(e) } };
    d.pictureOf = async () => 'data:image/png;base64,AA';
    if (o.assist) d.assist = async (payload, opts) => { asks.push(payload); return o.assist(payload, opts); };
    const ui = UI.make(d);
    const piece = (row, n) => ui.cards.get(ui.pieceKey(row, n)) || null;
    const card = (row, n) => { ui.status(row, n); return ui.cards.get(ui.pieceKey(row, n)); };
    // set a piece's letters as a person would (the widget's editTo) and wait for its build
    const type = async (row, n, word) => { const c = card(row, n); c.draft = word.split(''); c.status = 'building'; await ui.rebuild(c); return c; };
    const names = ck => ((store[ck] || { files: [] }).files).map(f => f.name);
    return { ui, d, store, CS, toasts, asks, card, type, names, entries };
  }
  const left = (w, row) => w.ui.pieces(row).filter(p => !w.ui.status(row, p.n).approved).length;       // (pieces still to approve; ui.count() is the pieces not yet on a sheet)

  // ═══ 1 · the pieces of a line ═══
  section('1 · the pieces of a line');
  {
    const one = lineOf('4200683017', '111', 1, 'APV'), two = lineOf('4195059483', '222', 2, 'APV'), six = lineOf('4195059483', '333', 6, 'APV');
    const w = world([one, two, six]);
    const p1 = w.ui.pieces(one), p2 = w.ui.pieces(two), p6 = w.ui.pieces(six);
    check(p1.length === 1 && p1[0].key === one.key && p1[0].slot === null && p1[0].of === 1 && p1[0].poolId === one.key + '_1', 'quantity 1: ONE piece, the line\'s own key and no slot (what it always was): ' + JSON.stringify(p1.map(p => [p.key, p.slot])));
    check(p2.map(p => p.key).join() === `${two.key}#C1,${two.key}#C2` && p2.map(p => p.slot).join() === 'C1,C2' && p2.map(p => p.poolId).join() === `${two.key}_1,${two.key}_2`, 'quantity 2: two pieces, <line>#C1 and <line>#C2, pool ids <line>_1 and <line>_2');
    check(p6.length === 6 && p6.every((p, i) => p.n === i + 1 && p.of === 6 && p.slot === 'C' + (i + 1)), 'quantity 6: six pieces in order');
    if (Sides.slotInfo) check(p2.every(p => Sides.SLOT.test(p.slot)) && p2.every(p => Sides.slotInfo(p.slot).kind === 'copy'), 'the slots are the engraving model\'s own grammar (Sides.SLOT, kind copy): ' + p2.map(p => p.slot).join());
    else check(p2.every(p => /^C\d{1,3}$/.test(p.slot)), 'the slots are C1..Cn (the engraving model of this tree does not know them yet): ' + p2.map(p => p.slot).join());
    check(w.ui.count() === 1 + 2 + 6, 'the service counts pieces waiting: ' + w.ui.count());
    check(w.ui.piecesNow().length === 9 && w.ui.rows().length === 3, 'piecesNow lists the 9 pieces of the 3 lines');
    const a = w.card(two, 1), b = w.card(two, 2), z = w.card(one);
    check(a !== b && a.key === `${two.key}#C1` && b.key === `${two.key}#C2` && z.key === one.key && z.slot === null && z.of === 1, 'each piece has its OWN card state (key, slot, n, of)');
    check(w.ui.status(two, 2).pieceKey === `${two.key}#C2` && w.ui.status(two, 2).lineKey === two.key && w.ui.status(two, 2).n === 2 && w.ui.status(two, 2).of === 2 && w.ui.status(one).pieceKey === one.key, 'status() names the piece: pieceKey, lineKey, n, of');
    check(w.ui.describe(two, 2).piece.tag === 'Piece 2 of 2' && w.ui.describe(one).piece.tag === '', 'the tag is "Piece 2 of 2"; a line of one piece has none');
    check(w.ui.pieceKey(two, 'C2') === `${two.key}#C2` && w.ui.pieceKey(two, `${two.key}#C2`) === `${two.key}#C2` && w.ui.pieceKey(two, 2) === `${two.key}#C2` && w.ui.pieceKey(one, 'C1') === one.key, 'a piece is asked for by slot, by key or by number; a line of one piece has only its own key');
  }

  // ═══ 2 · the buyer's words per piece ═══
  section('2 · the buyer\'s words, piece by piece');
  {
    const cases = [
      ['APV', 2, ['APV', 'APV'], 'the same three letters on every piece'],
      ['A P V', 3, ['APV', 'APV', 'APV'], 'spaced letters are one monogram, the same on every piece'],
      ['ABC / XYZ', 2, ['ABC', 'XYZ'], 'a slash'], ['ABC & XYZ', 2, ['ABC', 'XYZ'], 'an ampersand'], ['ABC and XYZ', 2, ['ABC', 'XYZ'], '"and"'], ['ABC, XYZ', 2, ['ABC', 'XYZ'], 'a comma'],
      ['ABC\nXYZ', 2, ['ABC', 'XYZ'], 'two lines'], ['ABC XYZ', 2, ['ABC', 'XYZ'], 'two groups of three letters'], ['1: ABC 2: XYZ', 2, ['ABC', 'XYZ'], 'numbered'], ['A.B.C / X.Y.Z', 2, ['ABC', 'XYZ'], 'dotted letters'],
      ['ABC, XYZ, QRS', 3, ['ABC', 'XYZ', 'QRS'], 'three parts for three pieces'],
      ['ABC XYZ', 3, ['', '', ''], 'two parts for three pieces: nothing is guessed, every piece gets the whole words (which are six letters)'],
      ['ABC, XY', 2, ['', '', ''], 'a part that is not three letters: nothing is guessed']
    ];
    for (const [words, q, want, why] of cases) {
      const row = lineOf('4195059483', '444', q, words), w = world([row]);
      const got = Array.from({ length: q }, (_, i) => w.ui.lettersOf(w.card(row, i + 1)).join(''));
      const ok = want.some(Boolean) ? got.join() === want.slice(0, q).join() : got.every(x => x === got[0]);
      check(ok, `"${words.replace(/\n/g, '\\n')}" on ${q} pieces -> ${got.map(x => x || '–').join(' | ')}  (${why})`);
    }
    // the whole words reach each piece's "The buyer wrote" line
    const row = lineOf('4195059483', '445', 2, 'ABC XYZ'), w = world([row]);
    check(w.ui.describe(row, 1).buyer.words === 'ABC' && w.ui.describe(row, 2).buyer.words === 'XYZ' && w.ui.describe(row, 2).buyer.letters.join('') === 'XYZ', 'each piece says what the buyer wrote FOR IT (ABC / XYZ)');
    const same = lineOf('4195059483', '446', 2, 'APV'), w2 = world([same]);
    check(w2.ui.describe(same, 1).buyer.words === 'APV' && w2.ui.describe(same, 2).buyer.words === 'APV', 'and the same words on both when the buyer wrote one monogram');
  }

  // ═══ 3 · approving ONE piece ═══
  section('3 · approve one piece: its own design, quantity 1');
  {
    const row = lineOf('4195059483', '555', 2, 'ABC / XYZ'), w = world([row], { defaultQty: 2 }), ck = 'ord:custom:4195059483:X';
    const c1 = await w.type(row, 1, 'ABC'), c2 = w.card(row, 2);
    check(w.ui.describe(row, 1).state === 'ready' && w.ui.describe(row, 2).state !== 'approved', 'piece 1 is made and ready; piece 2 is not approved');
    await w.ui.approve(c1);
    check(w.names(ck).join('|') === 'Monogram ABC - piece 1.ai', 'approving piece 1 puts ONE design on the card, named for its piece: ' + w.names(ck).join('|'));
    const f = w.store[ck].files[0];
    check(f.qty === 1 && f.mono && f.mono.line === row.key && f.mono.slot === 'C1', 'quantity 1 (not the line\'s 2), tagged with its line and slot: qty ' + f.qty + ' ' + JSON.stringify(f.mono));
    check(w.ui.status(row, 1).approved === true && w.ui.status(row, 1).state === 'approved' && w.ui.status(row, 2).approved === false, 'piece 1 reads approved; piece 2 does NOT (approval of one piece never approves another)');
    check(left(w, row) === 1 && w.ui.count() === 2, 'one piece left to approve (' + left(w, row) + '); both still wait for the sheet (' + w.ui.count() + ')');
    // a second press on the approved piece changes nothing
    await w.ui.approve(c1);
    check(w.names(ck).length === 1, 'approving the same piece again changes nothing');
    const c2b = await w.type(row, 2, 'XYZ'); await w.ui.approve(c2b);
    check(w.names(ck).join('|') === 'Monogram ABC - piece 1.ai|Monogram XYZ - piece 2.ai', 'piece 2 approved on its own: two designs, one for each piece, in the order of the pieces: ' + w.names(ck).join('|'));
    check(w.store[ck].files.every(g => g.qty === 1) && w.store[ck].files[1].mono.slot === 'C2', 'both quantity 1; piece 2 tagged C2');
    check(w.ui.status(row, 1).approved && w.ui.status(row, 2).approved && left(w, row) === 0, 'both pieces approved, none left to approve');
    // the order of the files follows the pieces, whatever order they were approved in
    const row2 = lineOf('4195059483', '556', 3, 'APV'), w2 = world([row2]), ck2 = 'ord:custom:4195059483:X';
    for (const [n, word] of [[3, 'CCC'], [1, 'AAA'], [2, 'BBB']]) { const c = await w2.type(row2, n, word); await w2.ui.approve(c); }
    check(w2.names(ck2).join('|') === 'Monogram AAA - piece 1.ai|Monogram BBB - piece 2.ai|Monogram CCC - piece 3.ai', 'approved 3, 1, 2: the files stand 1, 2, 3 (Send to Sheet cuts the n-th design as copy n): ' + w2.names(ck2).join('|'));
    // a quantity-1 line is exactly what it was
    const one = lineOf('4200683017', '557', 1, 'APV'), w3 = world([one], { defaultQty: 1 }), co = await w3.type(one, undefined, 'APV'); await w3.ui.approve(co);
    const g = w3.store['ord:custom:4200683017:X'].files[0];
    // (MONOSEND, Paul 18 Oct 2026: the approval also sends the card's designs, so a line of one piece is ONE piece, tagged with its line (no slot): the send gives it to this line, never to another line of the card)
    check(g.name === 'Monogram APV.ai' && g.qty === 1 && !!g.mono && g.mono.line === one.key && g.mono.slot == null, 'a line of ONE piece: "Monogram APV.ai", quantity 1, tagged with its line and no slot: ' + g.name + ' x' + g.qty + ' ' + JSON.stringify(g.mono));
    const done = []; // onApproved details
  }

  // ═══ 4 · changing a piece after its approval ═══
  section('4 · changing a letter after approval replaces only that piece\'s design');
  {
    const row = lineOf('4195059483', '666', 2, 'APV'), w = world([row]), ck = 'ord:custom:4195059483:X';
    const a = await w.type(row, 1, 'APV'), b = await w.type(row, 2, 'APV'); await w.ui.approve(a); await w.ui.approve(b);
    w.store[ck].files[0].metal = 'silver'; w.store[ck].files[1].metal = 'rose';        // (a person set a metal for each design)
    const idA = w.store[ck].files[0].id, idB = w.store[ck].files[1].id;
    const a2 = await w.type(row, 1, 'AQV');
    check(w.ui.describe(row, 1).dirty === true && w.ui.describe(row, 1).approved.letters === 'APV' && w.ui.describe(row, 2).dirty === false, 'piece 1 edited: it reads "dirty" (Replace approved); piece 2 is untouched');
    await w.ui.approve(a2);
    check(w.names(ck).join('|') === 'Monogram AQV - piece 1.ai|Monogram APV - piece 2.ai', 'piece 1\'s design was REPLACED, piece 2\'s stays: ' + w.names(ck).join('|'));
    check(w.store[ck].files.length === 2 && w.store[ck].files[0].id !== idA && w.store[ck].files[1].id === idB, 'never two designs for one piece: two files, the second one is the very same file as before');
    check(w.store[ck].files[0].metal === 'silver' && w.store[ck].files[1].metal === 'rose', 'the replacement keeps the metal of the design it replaced (silver); piece 2 keeps its own (rose)');
    check(w.ui.status(row, 1).approved && w.ui.status(row, 2).approved && w.ui.status(row, 1).letters === 'AQV' && w.ui.status(row, 2).letters === 'APV', 'both read approved, each with its own letters');
    // changing the other piece after, then back to the same letters
    const b2 = await w.type(row, 2, 'ZZZ'); await w.ui.approve(b2);
    check(w.names(ck).join('|') === 'Monogram AQV - piece 1.ai|Monogram ZZZ - piece 2.ai', 'piece 2 changed: only piece 2\'s design is replaced: ' + w.names(ck).join('|'));
    check(w.store[ck].files.map(g => g.mono.slot).join() === 'C1,C2', 'the tags stay C1, C2 in order');
    // an edit that is not approved leaves the approved design as it was
    await w.type(row, 1, 'BBB');
    check(w.names(ck).join('|') === 'Monogram AQV - piece 1.ai|Monogram ZZZ - piece 2.ai', 'a letter typed but not approved changes no design');
  }

  // ═══ 5 · identical letters ═══
  section('5 · identical letters on two pieces: still one design for each piece');
  {
    const row = lineOf('4195059483', '777', 2, 'APV'), w = world([row]), ck = 'ord:custom:4195059483:X';
    const a = await w.type(row, 1, 'APV'), b = await w.type(row, 2, 'APV');
    await w.ui.approve(a);
    check(w.ui.status(row, 1).approved && !w.ui.status(row, 2).approved, 'piece 1 approved with APV; piece 2, the same letters, is NOT approved by it');
    await w.ui.approve(b);
    check(w.names(ck).join('|') === 'Monogram APV - piece 1.ai|Monogram APV - piece 2.ai' && w.store[ck].files.every(f => f.qty === 1), 'two designs of quantity 1, one for each piece (never one design of quantity 2): ' + w.names(ck).join('|'));
    // the two share the one build (same letters) but not their state
    check(w.card(row, 1).result === w.card(row, 2).result, 'the finished monogram is built once for both (same letters)');
    const a2 = await w.type(row, 1, 'XYZ'); check(w.card(row, 2).resultLetters === 'APV' && w.ui.describe(row, 2).letters.join('') === 'APV', 'editing piece 1 never changes piece 2');
    await w.ui.approve(a2);
    check(w.names(ck).join('|') === 'Monogram XYZ - piece 1.ai|Monogram APV - piece 2.ai', 'and replaces only piece 1\'s design');
  }

  // ═══ 6 · migration of a design approved before pieces ═══
  section('6 · migration: one design of quantity 2 is pieces 1 and 2 both approved');
  {
    // (MONOHOOPS, Paul 19 Oct 2026: the old design's size is left unread here (0): a design approved before the hoops whose size differs from the piece made now is asked again, the upgrade is monogram-hoops.cjs's)
    const legacy = (ck, name, qty, extra) => ({ ck, rid: '4195059483', files: [Object.assign({ id: 'old1', name, qty, metal: 'gold', state: 'ready', pieces: 1, wMm: 0, hMm: 0, bytes: new Uint8Array([9, 9]), cloud: { path: 'charmnest/custom/x.pdf' }, hash: 'h1', thumb: 'data:image/png;base64,AA' }, extra || {})], sent: null });
    const ck = 'ord:custom:4195059483:X';
    const row = lineOf('4195059483', '888', 2, 'APV'), w = world([row]);
    w.store[ck] = legacy(ck, 'Monogram APV.ai', 2);
    check(w.ui.status(row, 1).approved && w.ui.status(row, 2).approved && w.ui.status(row, 1).state === 'approved' && w.ui.status(row, 2).state === 'approved', 'the old design (APV x2) reads as piece 1 AND piece 2 both approved');
    check(left(w, row) === 0, 'nothing is left to approve: no second approval is asked');
    const c2 = w.card(row, 2); await w.ui.rebuild(c2);
    check(w.ui.describe(row, 2).state === 'approved' && w.ui.describe(row, 2).approved.letters === 'APV', 'and still after the piece is built');
    await w.ui.approve(c2);
    check(w.names(ck).join('|') === 'Monogram APV.ai', 'pressing Approve on an approved piece (same letters) changes nothing: ' + w.names(ck).join('|'));
    // change piece 2: the old design is split, only piece 2's is replaced
    const x2 = await w.type(row, 2, 'XYZ'); await w.ui.approve(x2);
    check(w.names(ck).join('|') === 'Monogram APV - piece 1.ai|Monogram XYZ - piece 2.ai', 'changing piece 2 splits the old design: piece 1 keeps APV, piece 2 has XYZ: ' + w.names(ck).join('|'));
    check(w.store[ck].files[0].id === 'old1' && w.store[ck].files[0].qty === 1 && w.store[ck].files[0].mono.slot === 'C1' && w.store[ck].files[0].hash === 'h1' && w.store[ck].files[0].cloud.path === 'charmnest/custom/x.pdf', 'piece 1\'s design is the very same file (id, hash, cloud copy), now quantity 1 and tagged C1');
    check(w.ui.status(row, 1).approved && w.ui.status(row, 2).approved && w.ui.status(row, 1).letters === 'APV' && w.ui.status(row, 2).letters === 'XYZ', 'both pieces approved, each with its letters');
    // change piece 1 instead
    const row2 = lineOf('4195059483', '889', 2, 'APV'), w2 = world([row2]);
    w2.store[ck] = legacy(ck, 'Monogram APV.ai', 2);
    const y1 = await w2.type(row2, 1, 'KLM'); await w2.ui.approve(y1);
    check(w2.names(ck).join('|') === 'Monogram KLM - piece 1.ai|Monogram APV - piece 2.ai' && w2.store[ck].files.every(f => f.qty === 1), 'changing piece 1 instead: piece 1 has KLM, piece 2 keeps APV (the old file): ' + w2.names(ck).join('|'));
    check(w2.store[ck].files[1].id === 'old1' && w2.store[ck].files[1].mono.slot === 'C2', 'piece 2\'s design is the old file, tagged C2');
    // three pieces, the middle one changed: the old file becomes pieces 1 and 3
    const row3 = lineOf('4195059483', '890', 3, 'APV'), w3 = world([row3]);
    w3.store[ck] = legacy(ck, 'Monogram APV.ai', 3);
    check([1, 2, 3].every(n => w3.ui.status(row3, n).approved), 'quantity 3: the old design x3 reads as all three pieces approved');
    const m2 = await w3.type(row3, 2, 'MMM'); await w3.ui.approve(m2);
    check(w3.names(ck).join('|') === 'Monogram APV - piece 1.ai|Monogram MMM - piece 2.ai|Monogram APV - piece 3.ai' && w3.store[ck].files.every(f => f.qty === 1), 'the middle piece changed: pieces 1 and 3 keep APV, piece 2 is MMM, in the order of the pieces: ' + w3.names(ck).join('|'));
    check(w3.store[ck].files.map(f => f.mono.slot).join() === 'C1,C2,C3' && [1, 2, 3].every(n => w3.ui.status(row3, n).approved), 'tagged C1, C2, C3; all three approved');
    // an old design of quantity 1 on a line of two pieces covers piece 1 only
    const row4 = lineOf('4195059483', '891', 2, 'APV'), w4 = world([row4]); w4.store[ck] = legacy(ck, 'Monogram APV.ai', 1);
    check(w4.ui.status(row4, 1).approved && !w4.ui.status(row4, 2).approved, 'an old design of quantity 1 on a line of two covers piece 1 only; piece 2 is to approve');
    // the old design of a different word than the buyer's: the piece shows the approved letters
    const row5 = lineOf('4195059483', '892', 2, 'APV'), w5 = world([row5]); w5.store[ck] = legacy(ck, 'Monogram XYZ.ai', 2);
    const e1 = w5.card(row5, 1), e2 = w5.card(row5, 2);
    check(w5.ui.status(row5, 1).approved && w5.ui.status(row5, 2).approved, 'an old design of other letters than the buyer\'s (a person changed them) still covers both pieces');
  }

  // ═══ 7 · sent ═══
  section('7 · a card sent to the sheets: each piece reads the file ITS copy went as');
  {
    const ck = 'ord:custom:4195059483:X', row = lineOf('4195059483', '999', 2, 'APV'), w = world([row]);
    const mk = (id, name, slot) => ({ id, name, qty: 1, metal: 'gold', state: 'ready', pieces: 1, mono: { line: row.key, slot } });
    w.store[ck] = { ck, rid: '4195059483', files: [mk('fa', 'Monogram AAA - piece 1.ai', 'C1'), mk('fb', 'Monogram BBB - piece 2.ai', 'C2')], sent: { id: 's1', at: 1, by: 'Anna', lines: { [row.key]: [{ f: 'fa', i: 0 }, { f: 'fb', i: 0 }] } } };
    check(w.ui.status(row, 1).state === 'sent' && w.ui.status(row, 1).letters !== undefined && w.ui.describe(row, 1).approved.letters === 'AAA' && w.ui.describe(row, 2).approved.letters === 'BBB', 'sent: piece 1 is AAA, piece 2 is BBB, both "On the sheets"');
    // a hydrated card (the cloud keeps no tags): the same answer, from the copy mapping
    w.store[ck].files.forEach(f => delete f.mono);
    check(w.ui.describe(row, 1).approved.letters === 'AAA' && w.ui.describe(row, 2).approved.letters === 'BBB' && w.ui.status(row, 2).state === 'sent', 'a card rebuilt from the cloud (no tags) reads the same, from sent.lines');
    const c = w.card(row, 2); c.draft = 'ZZZ'.split(''); await w.ui.rebuild(c); await w.ui.approve(c);
    check(w.names(ck).join('|') === 'Monogram AAA - piece 1.ai|Monogram BBB - piece 2.ai' && w.ui.count() === 0, 'a sent piece is never changed: approving does nothing, nothing is waiting for the sheet');
    // an old design sent as quantity 2: both copies went as the one file
    const row2 = lineOf('4195059483', '998', 2, 'APV'), w2 = world([row2]);
    w2.store[ck] = { ck, rid: '4195059483', files: [{ id: 'fo', name: 'Monogram APV.ai', qty: 2, metal: 'gold', state: 'ready', pieces: 1 }], sent: { id: 's2', at: 1, by: 'Anna', lines: { [row2.key]: [{ f: 'fo', i: 0 }, { f: 'fo', i: 0 }] } } };
    check(w2.ui.describe(row2, 1).approved.letters === 'APV' && w2.ui.describe(row2, 2).approved.letters === 'APV' && w2.ui.status(row2, 1).sent && w2.ui.status(row2, 2).sent, 'an old design sent as one file of quantity 2: both pieces read sent with its letters');
  }

  // ═══ 8 · counts, a quantity change, the model asked once ═══
  section('8 · counts, quantity changes, one paid answer for two identical pieces');
  {
    const row = lineOf('4195059483', '1010', 2, 'APV'), w = world([row]), ck = 'ord:custom:4195059483:X';
    const c1 = await w.type(row, 1, 'APV'); await w.ui.approve(c1);
    row.spec.quantity = 3; row.line.quantity = 3;
    check(w.ui.pieces(row).length === 3 && w.ui.status(row, 1).approved && !w.ui.status(row, 3).approved && left(w, row) === 2, 'the quantity raised to 3: a third piece appears, piece 1 keeps its approval, 2 pieces are left to approve');
    row.spec.quantity = 1; row.line.quantity = 1;
    check(w.ui.pieces(row).length === 1 && w.ui.pieces(row)[0].key === row.key, 'the quantity lowered to 1: the line is one piece with its own key again');
    // the quantity lowered after two pieces were approved: the line is one piece again, its design is piece 1's, and a new approval leaves no extra copy behind
    {
      const rq = lineOf('4195059483', '1012', 2, 'APV'), wq = world([rq]), ckq = 'ord:custom:4195059483:X';
      for (const [n, word] of [[1, 'AAA'], [2, 'BBB']]) { const c = await wq.type(rq, n, word); await wq.ui.approve(c); }
      rq.spec.quantity = 1; rq.line.quantity = 1;
      check(wq.ui.pieces(rq).length === 1 && wq.ui.describe(rq).approved && wq.ui.describe(rq).approved.letters === 'AAA', 'quantity lowered to 1: the line reads as approved with piece 1\'s design (A A A)');
      const cq = await wq.type(rq, undefined, 'CCC'); await wq.ui.approve(cq);
      check(wq.names(ckq).join('|') === 'Monogram CCC.ai', 'a new approval replaces piece 1\'s design and removes the design of the piece the line no longer has (no extra copy is cut): ' + wq.names(ckq).join('|'));
    }
    // the model: two pieces, the same letters (a word that breaks a rule), asked once
    const orig = M.build, mk = r => { if (r && r.ok && r.report && r.report.metrics) r.report.metrics = Object.assign({}, r.report.metrics, { violations: ['The left and middle letters are not held by one contact at the top and bottom.'] }); return r; };
    M.build = function () { return mk(orig.apply(this, arguments)); };
    const origFit = M.autoFit; M.autoFit = undefined;
    try {
      const row2 = lineOf('4195059483', '1011', 2, 'APV');
      let calls = 0;
      const w2 = world([row2], { assist: async () => { calls++; await new Promise(r => setTimeout(r, 30)); return { adjust: { L: { scale: 1, sx: 1, dx: 0, dy: 0 }, M: { scale: 1, sx: 1, dx: 0, dy: 0 }, R: { scale: 1, sx: 1, dx: 0, dy: 0 } }, why: 'tried' }; } });
      const a = w2.card(row2, 1), b = w2.card(row2, 2);
      // both pieces are looked at (a view) so the model may be asked
      a.views.add({ host: null, o: {}, mode: 'detail', dead: true }); b.views.add({ host: null, o: {}, mode: 'detail', dead: true });
      a.draft = 'APV'.split(''); b.draft = 'APV'.split('');
      await Promise.all([w2.ui.rebuild(a), w2.ui.rebuild(b)]);
      await new Promise(r => setTimeout(r, 400));
      check(calls >= 1 && calls <= 3, 'two pieces with the same letters: the model\'s rounds are asked ONCE for both (' + calls + ' calls, at most 3 rounds, not 6); asks per line: ' + w2.asks.length);
    } finally { M.build = orig; M.autoFit = origFit; }
  }

  // ═══ 9 · the order model: each monogram piece is a piece of its order ═══
  section('9 · the order model (charm-nest-engrave-order.js)');
  {
    const Ord = require(path.join(root, 'charm-nest-engrave-order.js'));
    const mono = lineOf('77', '1', 2, 'APV', { material: 'gold' }), bar = { key: '77_2', state: 'pooled', order: { receiptId: '77' }, line: { transactionId: '2', quantity: 1, sku: 'BAR' }, poolIds: [], spec: { quantity: 1, material: 'silver' } };
    const st = { '77_1#C1': 'approved', '77_1#C2': 'to-approve' };
    const per = r => (r.key === '77_1' ? [{ pieceKey: '77_1#C1', slot: 'C1', n: 1, of: 2, state: st['77_1#C1'] }, { pieceKey: '77_1#C2', slot: 'C2', n: 2, of: 2, state: st['77_1#C2'] }] : null);
    const ps = Ord.piecesOf('77', { rows: [mono], monoState: () => 'to-approve', monoPieces: per });
    check(ps.length === 2 && ps.map(p => p.pieceKey).join() === '77_1#C1,77_1#C2' && ps.map(p => p.state).join() === 'approved,to-approve' && ps.every(p => p.kind === 'monogram' && p.faces.length === 0 && p.jobKeys.length === 0 && p.lineKey === '77_1'), 'a monogram line of quantity 2 is TWO pieces of its order, each with its own state: ' + ps.map(p => p.pieceKey + ':' + p.state).join(' '));
    check(ps.map(p => p.slot).join() === 'C1,C2' && ps.map(p => p.label).join() === 'Monogram 1,Monogram 2' && ps[0].piece.row === mono, 'slots C1 and C2, labelled "Monogram 1", "Monogram 2"; the card model\'s piece holds the line');
    const ps0 = Ord.piecesOf('77', { rows: [mono], monoState: () => 'to-approve' });
    check(ps0.length === 2 && ps0.map(p => p.pieceKey).join() === '77_1#C1,77_1#C2' && ps0.every(p => p.state === 'to-approve'), 'without the page\'s per-piece answer every piece has the line\'s state (still two pieces)');
    const one = lineOf('77', '3', 1, 'APV'), p1 = Ord.piecesOf('77', { rows: [one], monoState: () => 'to-approve' });
    check(p1.length === 1 && p1[0].pieceKey === one.key && p1[0].slot === null && p1[0].label === 'Monogram', 'a line of one piece is one piece with its line\'s key (as before)');
    const c = Ord.countOf('77', { rows: [mono, bar], monoState: r => (r.key === '77_1' ? 'to-approve' : null), monoPieces: per });
    check(c.n === 2 && c.approved === 1 && c.left === 1, 'countOf counts the two pieces: ' + JSON.stringify(c));
    check(Ord.nextUnapproved('77', '77_1#C1', { rows: [mono], monoState: () => 'to-approve', monoPieces: per }).pieceKey === '77_1#C2' && Ord.step('77', '77_1#C1', 1, { rows: [mono], monoState: () => 'to-approve', monoPieces: per }).pieceKey === '77_1#C2' && Ord.step('77', '77_1#C2', 1, { rows: [mono], monoState: () => 'to-approve', monoPieces: per }) === null, 'nextUnapproved and step walk piece 1 -> piece 2 and stop at the end');
    const html = Ord.stripHtml(ps, '77_1#C1', {});
    check(/Piece 1 of 2/.test(html) && /data-piece="77_1#C2"/.test(html) && (html.match(/class="egPcTab"/g) || []).length === 2, 'the strip says "Piece 1 of 2" and has a button for each piece');
    // (the guide's next piece, "after piece 1 the detail goes on to piece 2", reads the page's rows: pieces-monogram.cjs checks it in the page)
  }

  console.log(`\n${nCheck - fails.length}/${nCheck} checks passed in ${((Date.now() - T0) / 1000).toFixed(1)} s`);
  if (fails.length) { console.log('FAILED:\n - ' + fails.join('\n - ')); process.exit(1); }
})().catch(e => { console.error(e); process.exit(2); });
