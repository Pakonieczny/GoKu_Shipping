/* PIECEPICK and NOSIMLIST (Paul, 26 Nov 2026), Node only: run with `node tests/charm-nest/piece-pick.cjs`.
   A · the piece pills: one per piece of the card's open lines (an earring pair's Left and Right, a line's copies), the line picked pressed, the pick kept per card.
   B · a design dropped for one charm (F.line) goes on that line alone (planLines), and the send covers only the lines its designs were dropped for (sendRows); designs with no line spread as before.
   C · the Review list card draws no similar-charm list (reviewRow, siblingStamp), the order window opens its Suggest tray for an order held in Review, the other held charms are released after the send. */
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '..', '..');
const src = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8');
let passed = 0, failed = 0;
const ok = (name, cond, detail) => { console.log((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + JSON.stringify(detail).slice(0, 900))); if (cond) passed++; else failed++; };
const block = (from, to) => { const a = src.indexOf(from), b = src.indexOf(to, a); if (a < 0 || b < a) throw new Error('block not found: ' + from.slice(0, 60)); return src.slice(a, b + to.length); };

/* ── A and B · the bridge's own code, run on plain lines ── */
const RID = '4249000001';
const line = (n, q, o) => Object.assign({ key: `${RID}_${n}`, order: { receiptId: RID }, line: { sku: 'SKU' + n, listingId: '1' }, spec: { quantity: q, designSku: 'DESIGN_' + n, material: 'gold' } }, o || {});
const pairs = new Set();
const ctx = vm.createContext({ Math, Object, String, Array, JSON, Set, Map, Number,
  window: {}, linesOf: it => it.rows, openLines: it => it.rows.filter(r => !r.closed), pairPlan: r => pairs.has(r.key) ? Array.from({ length: 2 * r.spec.quantity }, (_, i) => ({ side: i % 2 ? 'R' : 'L' })) : null,
  labelOf: m => ({ gold: 'GF', silver: 'SS' }[m] || m), txt: s => String(s || ''), esc: s => String(s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c])),
  ckOf: it => String(it.key || '').replace(/^[a-z]+:/, ''), cardOf: it => ({ ck: String(it.key || '').replace(/^[a-z]+:/, ''), sent: false, e: null, dropOk: true, files: [] }), redraw: () => {}, ver: 0 });
const pp = block('  /* PIECEPICK (Paul, 26 Nov 2026: "when an order has multiple pieces', '  const targetsOf = e => new Set(((e && e.files) || []).map(F => F && !F.mono && F.line).filter(Boolean));');
const mc = block('  /* MULTICHARM (25 Nov 2026, Paul: "Ability to add several charms', '  /* MULTICHARM end */');
vm.runInContext(mc + '\n' + pp + '\nthis.X = { planLines, cardPieces, pickable, pickRow, setPick, pillsHtml, cardPills, sendRows, targetsOf, picks };', ctx);
const X = ctx.X;

{ // A · the pills
  const a = line(1, 1), b = line(2, 1), c = line(3, 2); pairs.add(a.key);
  const it = { key: 'rvx:custom:' + RID + ':A', kind: 'reviewAct', rows: [a, b, c] };
  const ps = X.cardPieces(it);
  ok('A1 one piece per piece: the pair line Left + Right, then 1, then 2 copies', ps.length === 5 && ps[0].side === 'L' && ps[1].side === 'R' && ps[0].line === a.key && ps[2].line === b.key && ps[3].line === c.key && ps[4].line === c.key, ps.map(p => [p.n, p.line, p.side]));
  ok('A2 several lines: the card picks one charm', X.pickable(it) === true);
  ok('A3 nothing pressed yet: the first line is the one replaced', X.pickRow(it) === a);
  X.setPick(it, b.key);
  ok('A4 a pill pressed: that line is picked, kept for the card', X.pickRow(it) === b && X.picks.get('custom:' + RID + ':A') === b.key);
  X.setPick(it, 'nope');
  ok('A5 a line not on the card is never picked', X.pickRow(it) === b);
  const h = X.pillsHtml(it, c.key);
  ok('A6 the Design & Engrave pills: five buttons, the two of line 3 pressed, "Pieces 4–5 of 5"', (h.match(/class="egPcTab/g) || []).length === 5 && (h.match(/aria-pressed="true"/g) || []).length === 2 && h.includes('Pieces 4–5 of 5') && h.includes('data-metal="GF"') && h.includes('egPcStrip'), h.slice(0, 300));
  ok('A7 the card draws them (an order of several pieces that takes designs)', X.cardPills(it).includes('data-rv-piece="' + b.key + '"'));
  ok('A8 a custom listing\'s card draws none', X.cardPills(Object.assign({}, it, { kind: 'customOrder' })) === '');
  const solo = { key: 'rvx:custom:' + RID + ':S', kind: 'reviewAct', rows: [line(9, 1)] };
  ok('A9 an order of one piece: no pills, nothing to pick', X.cardPills(solo) === '' && X.pickable(solo) === false);
  const pairOnly = { key: 'rvx:custom:' + RID + ':P', kind: 'reviewAct', rows: [a] };
  ok('A10 one pair line: its two pills shown, both pressed (one charm), nothing else to pick', (X.cardPills(pairOnly).match(/aria-pressed="true"/g) || []).length === 2 && X.pickable(pairOnly) === false);
}
{ // B · where a design goes
  const a = line(1, 1), b = line(2, 1), c = line(3, 2), rows = [a, b, c];
  const Fb = { id: 'f1', qty: 1, line: b.key }, L1 = X.planLines([Fb], rows, () => 1, false);
  ok('B1 a design dropped for charm 2 goes on line 2 alone', L1[b.key].length === 1 && L1[a.key].length === 0 && L1[c.key].length === 0, L1);
  const Fc = { id: 'f2', qty: 2, line: c.key }, L2 = X.planLines([Fc], rows, () => 1, false);
  ok('B2 a design for a line of 2 copies: both copies on that line', L2[c.key].length === 2 && !L2[a.key].length && !L2[b.key].length, L2);
  const Fm = { id: 'f3', qty: 1, line: a.key, pieces: 3, charmQty: { 0: 1, 1: 1, 2: 1 } }, L3 = X.planLines([Fm], rows, () => 3, false);
  ok('B3 a file of several charms dropped for one charm: all of it on that line', L3[a.key].length === 3 && !L3[b.key].length, L3);
  const Fo = { id: 'f4', qty: 3 }, L4 = X.planLines([Fo], rows, () => 1, false);
  ok('B4 a design with no line spreads over the lines as before', L4[a.key].length === 1 && L4[b.key].length === 1 && L4[c.key].length === 1, L4);
  const it = { key: 'rvx:custom:' + RID + ':A', kind: 'reviewAct', rows };
  ok('B5 the send covers only the lines its designs were dropped for', JSON.stringify(X.sendRows(it, { files: [Fb] }).map(r => r.key)) === JSON.stringify([b.key]));
  ok('B6 two designs for two charms: both lines, the third untouched', JSON.stringify(X.sendRows(it, { files: [Fb, Fc] }).map(r => r.key)) === JSON.stringify([b.key, c.key]));
  ok('B7 designs with no line (a custom listing, an older card): every open line, as before', X.sendRows(it, { files: [Fo] }).length === 3);
  ok('B8 a monogram\'s design keeps its own line rule', X.sendRows(it, { files: [{ id: 'm', mono: { line: a.key }, line: b.key }] }).length === 3);
  b.closed = true;
  ok('B9 the charm it was dropped for can no longer take designs: the send covers nothing (it is refused in words)', X.sendRows(it, { files: [Fb] }).length === 0 && X.targetsOf({ files: [Fb] }).size === 1);
}

/* ── C · the wiring, read from the code ── */
const rr = block('  function reviewRow(it) {', '    reviewRows.set(it.key,{stamp,node});return node;');
ok('C1 the Review list card no longer fills a Send to Review order\'s slots with similar charms', !/fillMarkSlots\(node,it\)/.test(rr));
ok('C2 an unknown SKU card asks its offer without the similar list', rr.includes(',sibQ,{noSimilar:true})'));
ok('C3 the card draws the pills and shows the piece picked', rr.includes('CustomSheet.cardPills(ax)') && rr.includes('ListMedia.pair(shown)') && rr.includes('node._mediaRow=shown'));
const so = block('  function similarOffer(row, p, group, it, o) {', 'return lead;');
ok('C4 similarOffer: noSimilar leaves the similar charms out', /o && o\.noSimilar/.test(src.slice(src.indexOf('  function similarOffer(row, p, group, it, o) {'), src.indexOf('  function similarOffer(row, p, group, it, o) {') + 900)) && so.length > 0);
ok('C5 the order window opens its Suggest tray for an order held in Review', /choices\.length > 0 && inReview\(\)/.test(src));
ok('C6 Release Hold with designs sends them and releases the order\'s other held charms', /async function sendOwn\(target, how\)/.test(src) && /return !!\(await sendOwn\(/.test(src) && /liftOrder\(rid, who, rest, null, null\)/.test(src));
ok('C7 the drop asks which piece and this order or the repository', /Which piece does the file replace\?/.test(src) && /Update the charm repository/.test(src) && /data-ask-keep="1"/.test(src));
ok('C8 the repository switch uses the listing of the charm the design replaces', /F0 && F0\.line && linesOf\(it\)\.find\(r => r\.key === F0\.line\)/.test(src));

console.log(`piece-pick: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
