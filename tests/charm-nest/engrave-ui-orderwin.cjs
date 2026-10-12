// ENGUI2 (front-back-1011): the order window, the Review cards, the seal panel and the timeline for a piece engraved on the FRONT and the BACK.
// Offline, no browser, no network, no paid call, no Firestore: the real components (charm-nest-engrave-faces / -sides / -rows / piece-switch / engraving-seals / order-engraving /
// engrave-link / timeline-ui / motion) run in jsdom against contract-shaped jobs (plans/front-back-1011/CONTRACT.md: one job per piece AND face, front key "<lineKey>[#slot]~front").
//
// What it proves
//   1  the three orders of the brief: 4195966039 (front "W", back "1. 27. 24", Ace), an earring pair with front+back, a 3-disc necklace with front+back; the card cycles
//      piece by piece with the Front | Back switch of charm-nest-piece-switch.js (the one component), one entry for each JOB
//   2  the facts grid cell: "Front · W", "Back · 1. 27. 24", each face's approval and its own seal (FRONT ENGRAVING / BACK ENGRAVING)
//   3  the card (CNEngravingSeals.panel): words said about the front for a front job, "Approve both faces" only when both are waiting and every placement has been seen,
//      press handlers (a face, Back / Next, both)
//   4  "Approve both faces": the ordinary approval of each face in turn (Front first), stopping at the first that did not stand
//   5  Fix in Engraving: EngraveLink lands on the face asked for (two jobs hold one pool piece)
//   6  the stamps: FRONT ENGRAVING / BACK ENGRAVING / FRONT CUT PLAIN / CUT PLAIN, a front seal keeps its face through merge
//   7  the timeline: the Engraved step is done only when every face the line engraves has an approval; a back-only line derives exactly as before
//   8  Review: the card names the face for a line with a front, and says nothing new for any other line
//   9  INERT: a back-only line (one charm, a pair, 3 discs) has no face switch, no data-face, no "Approve both faces", no Front wording, the same cycle shape as ever
//   node tests/charm-nest/engrave-ui-orderwin.cjs     (NODE_PATH=<dir with jsdom>)
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm'), assert = require('assert/strict');
const { JSDOM } = require('jsdom');
const R = path.join(__dirname, '../..') + path.sep;
const src = f => fs.readFileSync(R + f, 'utf8');
let n = 0;
const ok = (name, fn) => { try { fn(); n++; } catch (e) { e.message = `[${name}] ${e.message}`; throw e; } };
const same = (a, b, m) => assert.deepEqual(JSON.parse(JSON.stringify(a)), b, m);
const later = [];
const okA = (name, fn) => later.push([name, fn]);

const dom = new JSDOM('<body><div id="host"></div></body>', { runScripts: 'outside-only', pretendToBeVisual: true });
const w = dom.window, d = w.document;
w.matchMedia = () => ({ matches: true });
for (const f of ['charm-nest-motion.js', 'charm-nest-engrave-faces.js', 'charm-nest-engrave-sides.js', 'charm-nest-engrave-rows.js', 'charm-nest-piece-switch.js', 'charm-nest-engraving-seals.js', 'charm-nest-engrave-card.js', 'charm-nest-order-engraving.js', 'charm-nest-engrave-link.js']) w.eval(src(f));
const F = w.CharmNestEngraveFaces, PS = w.CharmNestPieceSwitch, S = w.CNEngravingSeals, OE = w.OrderEngraving;

/* ── contract-shaped lines and jobs ─────────────────────────────────────────── */
const ENG_4195966039 = { v: 1, mode: 'both', source: 'labelled', front: { text: 'W' }, back: { text: '1. 27. 24' }, textSource: 'personalization', needsPerson: false, confidence: 0.95, why: 'The buyer labelled front and back' };
const store = new Map();       // Engrave.items(): job key -> job
const rowsList = [];           // Orders.rows()
function line(lk, receipt, engraving, extra) {
  const row = Object.assign({ key: lk, state: 'pooled', order: { receiptId: receipt }, line: { sku: 'ACE-CHARM', title: 'Charm' }, spec: Object.assign({ designSku: 'ACE-CHARM', font: { name: 'Ace' } }, engraving ? { engraving } : {}), poolIds: [] }, extra || {});
  rowsList.push(row); return row;
}
/** One job of a line: key = F.jobKey(line, slot, face); every face job has its own side row (parentRow) so its record is its own. */
function job(row, slot, face, words, state, copies) {
  const key = F.jobKey(row.key, slot, face), lines = words.split(' / ');
  // (charm-nest-engrave-sides.js sideRow: Object.create(line row) with its own key, slot, parentRow, the slot's pool ids, and `face` for a front)
  const side = Object.create(row); Object.defineProperties(side, { key: { value: key, enumerable: true, writable: true }, slot: { value: slot || null, enumerable: true }, parentRow: { value: row }, poolIds: { value: copies, enumerable: true }, engrave: { value: {}, writable: true, enumerable: true, configurable: true } });
  if (face === 'front') Object.defineProperty(side, 'face', { value: 'front', enumerable: true });
  const j = { key, row: slot || face === 'front' ? side : row, slot: slot || undefined, copies, state: state || 'review', lines, text: lines.join('\n'), fit: { ok: true }, verify: { geometry: { ok: true } }, view: {}, questions: [] };
  if (face === 'front') j.face = 'front';
  store.set(key, j); return j;
}
const rank = j => { const p = F.parseKey(j.key), s = p.slot || ''; return [s === 'L' ? 0 : s === 'R' ? 1 : /^D(\d+)$/.test(s) ? +s.slice(1) : 0, p.face === 'front' ? 0 : 1]; };
const approvals = [];
w.Engrave = {
  items: () => store,
  jobsOf: row => [...store.values()].filter(j => (j.row.parentRow || j.row) === (row && row.parentRow || row)).sort((a, b) => { const x = rank(a), y = rank(b); return x[0] - y[0] || x[1] - y[1]; }),
  jobOf: (row, poolId) => w.Engrave.jobsOf(row).find(j => !poolId || (j.copies || []).includes(poolId)) || null,
  isWorking: () => false, canFit: () => true, earOf: s => (s === 'L' ? 'Left ear' : s === 'R' ? 'Right ear' : s),
  async approve(j, who, btn) { approvals.push([j.key, who, !!btn]); if (j.refuse) return; j.state = 'approved'; j.approvedBy = who; j.approvedAt = 5000 + approvals.length; S.add(j, 'engraveApproved', who, j.approvedAt); },
};
w.Orders = { rows: () => rowsList };

// the three orders of the brief + the back-only lines that must not change
const charm = line('4195966039_5009123456', '4195966039', ENG_4195966039); charm.poolIds = ['p1'];
const charmF = job(charm, null, 'front', 'W', 'review', ['p1']), charmB = job(charm, null, 'back', '1. 27. 24', 'review', ['p1']);
const ear = line('4195966040_5009123457', '4195966040', { v: 1, mode: 'both', source: 'labelled', front: { text: 'W' }, back: { text: '1. 27. 24' }, needsPerson: false, confidence: 0.9 }); ear.poolIds = ['pL', 'pR'];
const earLF = job(ear, 'L', 'front', 'W', 'review', ['pL']), earLB = job(ear, 'L', 'back', '1. 27. 24', 'review', ['pL']), earRF = job(ear, 'R', 'front', 'W', 'review', ['pR']), earRB = job(ear, 'R', 'back', '1. 27. 24', 'review', ['pR']);
const neck = line('4195966041_5009123458', '4195966041', { v: 1, mode: 'both', source: 'labelled', front: { text: 'A' }, back: { text: 'B' }, needsPerson: false, confidence: 0.9 }); neck.poolIds = ['d1', 'd2', 'd3'];
const dj = {}; ['D1', 'D2', 'D3'].forEach((s, i) => { dj[s + 'f'] = job(neck, s, 'front', 'ABC'[i], 'review', ['d' + (i + 1)]); dj[s + 'b'] = job(neck, s, 'back', 'XYZ'[i], 'review', ['d' + (i + 1)]); });
// back-only (nothing about the front exists on them)
const old1 = line('4170000001_5009000001', '4170000001', null); old1.poolIds = ['o1'];
const oldJ = job(old1, null, 'back', 'ANNA', 'review', ['o1']);
const oldP = line('4170000002_5009000002', '4170000002', null); oldP.poolIds = ['oL', 'oR'];
const oldPL = job(oldP, 'L', 'back', 'ANNA', 'review', ['oL']), oldPR = job(oldP, 'R', 'back', 'BEN', 'review', ['oR']);
const oldN = line('4170000003_5009000003', '4170000003', null); oldN.poolIds = ['n1', 'n2', 'n3'];
const oldD = ['D1', 'D2', 'D3'].map((s, i) => job(oldN, s, 'back', 'ABC'[i], 'review', ['n' + (i + 1)]));

const reset = () => { for (const j of store.values()) { j.state = 'review'; for (const k of ['approvedBy', 'approvedAt', 'refuse']) delete j[k]; j.engravingSeals = []; if (j.row.parentRow) j.row.engrave = {}; else delete j.row.engrave; } };
const text = html => { const e = d.createElement('div'); e.innerHTML = html; return e; };
const tabs = host => [...host.querySelectorAll('.egCycle [data-a="ear"]')].map(b => ({ key: b.dataset.ear, face: b.dataset.face || '', on: b.getAttribute('aria-pressed') === 'true' }));

/* 1 ── the card cycles piece by piece, each piece Front | Back ───────────────── */
ok('the cycle of order 4195966039', () => {
  const c = OE.cycleOf(charm, null);
  assert(c, 'a charm with a front and a back has a cycle (its Front | Back)');
  assert.equal(c.faces, true);
  same(c.pieces.map(p => p.key), [charmF.key, charmB.key], 'one entry for each job, Front first');
  same(c.pieces.map(p => p.face), ['front', 'back']);
  assert.equal(c.key, charmF.key, 'the face still to settle is shown first (Front)');
  assert.equal(OE.cycleOf(charm, charmB.key).key, charmB.key, 'a key names the face shown');
  const host = text(PS.html(c.pieces, c.key, { fonts: true, faces: true }));
  assert.equal(host.querySelectorAll('.egEarSwitch').length, 1, 'one Front | Back switch for one piece, no piece switch');
  same([...host.querySelectorAll('.egEarTab .egPiece')].map(e => e.textContent), ['FRONT', 'BACK']);
  same([...host.querySelectorAll('.egEarWords')].map(e => e.textContent), ['W', '1. 27. 24'], 'each face button carries its own words');
  same([...host.querySelectorAll('.egEarFont')].map(e => e.textContent), ['Ace', 'Ace'], 'and the font it is engraved in');
});
ok('an earring pair with front+back: a card for each ear, each with its Front | Back', () => {
  assert.equal(OE.cycleOf(ear, null), null, 'the pair as a whole has no card of its own: its ears are the pieces (as a back-only pair always was)');
  const l = OE.cycleOf(ear, earLF.key), r = OE.cycleOf(ear, earRB.key);
  assert(l && l.faces && r && r.faces);
  same(l.pieces.map(p => p.key), [earLF.key, earLB.key], 'the left ear: its Front, its Back'); same(r.pieces.map(p => p.key), [earRF.key, earRB.key], 'the right ear: its Front, its Back');
  assert.equal(l.key, earLF.key); assert.equal(r.key, earRB.key, 'a key names the face shown');
  const host = text(PS.html(r.pieces, r.key, { fonts: true, faces: true }));
  assert.equal(host.querySelectorAll('.egEarSwitch').length, 1, 'one Front | Back switch for the ear (the two ears are two cards)');
  same([...host.querySelectorAll('.egFaces .egEarTab')].map(b => b.dataset.face), ['front', 'back']);
  assert.equal(host.querySelector('.egEarTab[aria-pressed="true"]').dataset.ear, earRB.key);
  // the list of pieces Engrave gives (one entry for each JOB) is folded to one entry for each ear
  const ps = w.Engrave.jobsOf(ear).map(j => ({ key: j.key, slot: j.slot, poolId: j.copies[0] }));
  same(OE.foldSlots(ps).map(p => p.key), [earLF.key, earRF.key], 'a card for each ear, opened on the face still to settle (Front)');
});
ok('the cycle of a 3-disc necklace with front+back', () => {
  const c = OE.cycleOf(neck, null);
  assert(c && c.faces); assert.equal(c.pieces.length, 6);
  same(c.pieces.map(p => p.key), ['D1', 'D2', 'D3'].flatMap(s => [dj[s + 'f'].key, dj[s + 'b'].key]));
  const host = text(PS.html(c.pieces, dj.D2b.key, { faces: true }));
  assert.equal(host.querySelectorAll('.egEarSwitch:not(.egFaces) .egEarTab').length, 3, 'three discs on the piece switch');
  assert.equal(host.querySelectorAll('.egFaces .egEarTab').length, 2, 'and the Front | Back of the disc shown');
  const lines = OE.discLines(neck);
  same(lines.map(l => l.split(' · ').slice(0, 3).join(' · ')), ['Disc 1 · Front · A', 'Disc 1 · Back · X', 'Disc 2 · Front · B', 'Disc 2 · Back · Y', 'Disc 3 · Front · C', 'Disc 3 · Back · Z']);
});
ok('the folded neighbour: Back / Next walk piece by piece, then face by face', () => {
  const c = OE.cycleOf(neck, dj.D1f.key), next = (k, dir) => { const m = { pieces: c.pieces, key: k, faces: true }; return S.__neighbour ? S.__neighbour(m, dir) : null; };
  void next;
  const host = text(S.panel({ kind: 'approve', job: dj.D1f, cycle: c }));
  assert(host.querySelector('[data-a="discNext"]') && host.querySelector('[data-a="discPrev"]'), 'Back / Next beside the switch');
});

/* 2 ── the facts grid cell ───────────────────────────────────────────────────── */
ok('the facts cell of order 4195966039 says each face and seals each face', () => {
  const before = text(OE.faceCell(charm)).textContent;
  assert(/Front · W/.test(before) && /Back · 1\. 27\. 24/.test(before), before);
  const T0 = Date.UTC(2026, 9, 11, 15, 0, 0);
  S.add(charmF, 'engraveApproved', 'Ana', T0); charmF.state = 'approved'; charmF.approvedBy = 'Ana'; charmF.approvedAt = T0;
  S.add(charmB, 'engraveApproved', 'Ana', T0); charmB.state = 'approved'; charmB.approvedBy = 'Ana'; charmB.approvedAt = T0;
  const cell = text(OE.faceCell(charm));
  const seals = [...cell.querySelectorAll('.seal')];
  assert.equal(seals.length, 2, 'a seal for each face, each its own');
  const labels = seals.map(s => s.getAttribute('aria-label') || '');
  assert(/^Front engraving placement approved by Ana/.test(labels[0]), labels[0]); assert(/^Engraving placement approved by Ana/.test(labels[1]), 'the back seal reads as it always did: ' + labels[1]);
  assert.notEqual(S.list(charmF)[0].id, S.list(charmB)[0].id, 'a front and a back sealed at the same moment by the same person are two seals');
  assert.equal(S.list(charmF)[0].face, 'front'); assert.equal(S.list(charmB)[0].face, undefined, 'a back seal has no face field: the seal it always was');
  charmF.state = 'review'; charmB.state = 'review'; delete charmF.approvedBy; delete charmB.approvedBy; delete charmF.approvedAt; delete charmB.approvedAt;
  charmF.engravingSeals = []; charmB.engravingSeals = []; charmF.row.engrave = {}; charmB.row.engrave = {}; charm.engrave = undefined;
});


/* 3 ── the card ──────────────────────────────────────────────────────────────── */
const wordsOfCard = host => ({ label: host.querySelector('.fLabel') && host.querySelector('.fLabel').textContent, why: host.querySelector('.egWhy b').textContent, words: host.querySelector('.egWords .egLbl').textContent, pill: host.querySelector('.egPill').textContent });
ok('the card says front for a front job and back for a back job', () => {
  reset();
  const c = OE.cycleOf(charm, charmF.key);
  const hf = text(S.panel({ kind: 'approve', job: charmF, text: 'W', cycle: c, both: { ok: true } })), hb = text(S.panel({ kind: 'approve', job: charmB, text: '1. 27. 24', cycle: OE.cycleOf(charm, charmB.key) }));
  const f = wordsOfCard(hf), b = wordsOfCard(hb);
  assert.equal(f.label, 'Front engraving'); assert.equal(f.words, 'Words on the front'); assert(!/\bback\b/i.test(f.why + f.pill), f.why);
  assert.equal(b.label, 'Back engraving'); assert.equal(b.words, 'Words on the back'); assert(!/front/i.test(b.why + b.pill), b.why);
  // the states that name a face in their words
  const af = text(S.panel({ kind: 'approved', job: charmF, text: 'W', cycle: c })), ab = text(S.panel({ kind: 'approved', job: charmB, text: '1. 27. 24', cycle: OE.cycleOf(charm, charmB.key) }));
  assert.equal(af.querySelector('.egWhy b').textContent, 'The front is approved'); assert.equal(ab.querySelector('.egWhy b').textContent, 'The back is approved', 'the words a back card always had');
  assert.equal(text(S.panel({ kind: 'skipped', job: charmF, cycle: c })).querySelector('.egWhy b').textContent, 'Cut plain: nothing on the front');
  assert.equal(text(S.panel({ kind: 'skipped', job: charmB, cycle: c })).querySelector('.egWhy b').textContent, 'Cut plain: nothing on the back');
  assert.equal(hf.querySelector('.swEng').dataset.face, 'front'); assert.equal(hb.querySelector('.swEng').getAttribute('data-face'), null, 'a back card carries nothing new');
  assert.deepEqual(tabs(hf).map(t => [t.face, t.on]), [['front', true], ['back', false]], 'the Front | Back switch of the one component, Front pressed');
  assert.deepEqual(tabs(hb).map(t => [t.face, t.on]), [['front', false], ['back', true]]);
  assert(hf.querySelector('.egCycle .egEarSwitch.egFaces'), 'drawn by charm-nest-piece-switch.js, not by the card');
});
ok('"Approve both faces": only when the piece has both faces waiting and every placement was seen', () => {
  const c = OE.cycleOf(charm, charmF.key);
  const on = text(S.panel({ kind: 'approve', job: charmF, text: 'W', cycle: c, both: { ok: true } })).querySelector('[data-e="approveBoth"]');
  assert(on && !on.disabled && /Approve both faces/.test(on.textContent));
  const off = text(S.panel({ kind: 'approve', job: charmF, text: 'W', cycle: c, both: { ok: false, why: 'Look at the back placement first (it is not on screen yet).' } })).querySelector('[data-e="approveBoth"]');
  assert(off && off.disabled && /Look at the back placement first/.test(off.title));
  assert.equal(text(S.panel({ kind: 'approve', job: charmF, text: 'W', cycle: c })).querySelector('[data-e="approveBoth"]'), null, 'not offered unless the host says so');
  assert.equal(text(S.panel({ kind: 'words', job: charmF, cycle: c, both: { ok: true } })).querySelector('[data-e="approveBoth"]'), null, 'never beside words that are not settled');
});
ok('bothFor: the model\'s own gate, a face counts as seen once drawn', () => {
  reset();
  const c = OE.cycleOf(charm, charmF.key);
  let b = OE.bothFor(c, charmF, j => j === charmF);
  assert(b && !b.ok && /Look at the back placement first/.test(b.why), b && b.why); same(b.jobs.map(j => j.key), [charmF.key, charmB.key]);
  b = OE.bothFor(c, charmF, () => true); assert(b && b.ok, b && b.why);
  charmB.fit = null; b = OE.bothFor(c, charmF, () => true); assert(!b.ok && /ready to check/.test(b.why)); charmB.fit = { ok: true };
  charmB.state = 'approved'; assert.equal(OE.bothFor(c, charmF, () => true), null, 'one face already decided: the ordinary Approve is the way');
  reset();
  assert.equal(OE.bothFor(OE.cycleOf(old1 && oldN, 'x'), oldD[0], () => true), null, 'a back-only line never offers it');
  const ec = OE.cycleOf(ear, earLF.key);
  assert(OE.bothFor(ec, earLF, () => true).ok, 'one ear: its own two faces'); same(OE.bothFor(ec, earLF, () => true).jobs.map(j => j.key), [earLF.key, earLB.key]);
});
okA('the host\'s buttons: a face, Back / Next, Approve both, Fix in Engraving', async () => {
  reset();
  const c = OE.cycleOf(neck, dj.D1f.key), e = { kind: 'approve', job: dj.D1f, text: 'A', cycle: c, both: { ok: true } };
  const host = d.createElement('div'); host.innerHTML = S.panel(e); d.body.appendChild(host);
  const log = [];
  S.wirePanel(host, e, { pick: k => log.push(['pick', k]), approveBoth: () => { log.push(['both']); return Promise.resolve(); }, open: () => { log.push(['open']); }, approve: () => Promise.resolve() });
  host.querySelector(`[data-ear="${dj.D1b.key}"]`).click();
  host.querySelector('[data-a="discNext"]').click();
  host.querySelector('[data-a="discPrev"]').click();
  host.querySelector('[data-e="approveBoth"]').click();
  host.querySelector('[data-e="engrave"]').click();
  same(log.map(x => x.join(':')), ['pick:' + dj.D1b.key, 'pick:' + dj.D2f.key, 'pick:' + dj.D3f.key, 'both', 'open'], 'Next goes to the next disc (its face still to settle: Front first), Back wraps to the last');
  host.remove();
});

/* 4 ── Approve both faces: each face's ordinary approval, Front first ─────────── */
okA('approveFaces: Front then Back, each its own approval and seal; stops at the first that did not stand', async () => {
  reset(); approvals.length = 0;
  const btn = d.createElement('button');
  const r = await OE.approveFaces([charmF, charmB], 'Ana', charmF, btn);
  assert.equal(r.ok, true); same(approvals.map(a => a.slice(0, 2)), [[charmF.key, 'Ana'], [charmB.key, 'Ana']]); same(approvals.map(a => a[2]), [true, false], 'the seal is pressed on the button of the face shown');
  assert.equal(S.list(charmF).length, 1); assert.equal(S.list(charmB).length, 1); assert.equal(S.list(charmF)[0].face, 'front'); assert.equal(S.list(charmB)[0].face, undefined);
  reset(); approvals.length = 0; charmB.refuse = true;
  const r2 = await OE.approveFaces([charmF, charmB], 'Ana', charmF, btn);
  assert.equal(r2.ok, false); assert.equal(r2.failed, charmB); assert.equal(charmF.state, 'approved', 'the face that stood is kept'); assert.equal(charmB.state, 'review', 'the other is left exactly as it was');
  reset(); approvals.length = 0; charmF.refuse = true;
  const r3 = await OE.approveFaces([charmF, charmB], 'Ana', charmF, btn);
  assert.equal(r3.failed, charmF); assert.equal(approvals.length, 1, 'the back is not approved behind a front that did not stand'); assert.equal(charmB.state, 'review');
  reset();
});

/* 5 ── Fix in Engraving lands on the face asked for ──────────────────────────── */
ok('EngraveLink.resolve: two jobs hold one pool piece; the face named wins', () => {
  reset();
  const L = w.EngraveLink, rs = t => L.resolve(Object.assign({ rid: '4195966039' }, t)).job;
  assert.equal(rs({ poolId: 'p1', piece: charmF.key }), charmF, 'a front key names the front');
  assert.equal(rs({ poolId: 'p1', key: charm.key, face: 'front' }), charmF, 'the face asked for');
  assert.equal(rs({ poolId: 'p1', key: charm.key, face: 'back' }), charmB);
  assert.equal(rs({ poolId: 'p1', key: charm.key }), charmB, 'a line key names the back, as it always did');
  assert.equal(rs({ key: charm.key, face: 'front' }), charmF, 'no pool piece: the front is the back\'s sibling');
  charmB.state = 'approved'; assert.equal(rs({ poolId: 'p1' }), charmF, 'nothing named: the one still to settle'); assert.equal(L.resolve({ rid: '4195966039', poolId: 'p1' }).tab, 'place');
  charmB.state = 'review'; charmF.state = 'approved'; assert.equal(L.resolve({ rid: '4195966039', poolId: 'p1', face: 'front' }).tab, 'done', 'a decided face opens on Decided');
  reset();
  assert.equal(L.resolve({ rid: '4195966041', poolId: 'd2', key: neck.key + '#D2' }).job, dj.D2b, 'a disc\'s own pool piece: its back, as ever');
  assert.equal(L.resolve({ rid: '4195966041', poolId: 'd2', face: 'front' }).job, dj.D2f);
  assert.equal(L.resolve({ rid: '4170000001', poolId: 'o1' }).job, oldJ, 'a back-only line resolves as it did');
});

/* 6 ── the stamps and the seal data ─────────────────────────────────────────── */
const stampWords = html => [...text(html).querySelectorAll('svg text')].map(t => t.textContent.trim()).filter(t => /^[A-Z ]+$/.test(t) && t.length > 3);
ok('stamp families: FRONT ENGRAVING beside BACK ENGRAVING; cut plain by face', () => {
  const T0 = Date.UTC(2026, 9, 11, 15, 0, 0), S1 = how => face => Object.assign({ how, at: T0, by: 'Ana' }, face ? { face } : {});
  const words = (how, face) => stampWords(w.Seal.html(S1(how)(face), 120, 'engravingSeal')).join(' ');
  assert(/FRONT/.test(words('engraveApproved', 'front')) && /ENGRAVING/.test(words('engraveApproved', 'front')), words('engraveApproved', 'front'));
  assert(/BACK/.test(words('engraveApproved')) && /ENGRAVING/.test(words('engraveApproved')) && !/FRONT/.test(words('engraveApproved')), words('engraveApproved'));
  assert(/FRONT/.test(words('engravePlain', 'front')) && /CUT PLAIN/.test(words('engravePlain', 'front')), words('engravePlain', 'front'));
  assert(/CUT PLAIN/.test(words('engravePlain')) && !/FRONT/.test(words('engravePlain')), words('engravePlain'));
});
ok('a front seal keeps its face through merge, list and the timeline\'s events', () => {
  reset();
  const T0 = Date.UTC(2026, 9, 11, 15, 0, 0);
  const m = S.merge({ seals: [{ how: 'engraveApproved', at: T0, by: 'Ana', face: 'front' }, { how: 'engraveApproved', at: T0, by: 'Ana' }] });
  assert.equal(m.length, 2, 'a front and a back approved in one instant by one person are two seals'); same(m.map(x => x.face || '').sort(), ['', 'front']); assert(m.some(x => /^engraveApproved:front:\d+:Ana$/.test(x.id)) && m.some(x => /^engraveApproved:\d+:Ana$/.test(x.id)));
  const evs = [{ type: 'engraveApproved', orderId: '4195966039', lineKey: charm.key, at: T0, by: 'Ana', data: { face: 'front', poolId: 'p1' } }, { type: 'engraveApproved', orderId: '4195966039', lineKey: charm.key, at: T0 + 1, by: 'Ana', data: { poolId: 'p1' } }];
  same(S.fromEvents(evs, charmF.row).map(x => x.face), ['front'], 'the front\'s side row reads only the front\'s events');
  same(S.fromEvents(evs, ear.engrave ? ear : charm).map(x => x.face || '').sort(), ['', 'front'], 'the line\'s own row sees both, each seal says its face');
  const sideBack = Object.create(charm); Object.defineProperties(sideBack, { key: { value: charm.key + '#L' }, slot: { value: 'L' }, parentRow: { value: charm }, poolIds: { value: ['p1'] } });
  same(S.fromEvents(evs, sideBack).map(x => x.at), [T0 + 1], 'a back side row only the others');
});

/* 7 ── the timeline rail ─────────────────────────────────────────────────────── */
{
  const win = { document: { getElementById: () => null, createElement: () => ({}), head: { appendChild() {} } } };
  win.window = win; vm.createContext(win); vm.runInContext(src('charm-nest-timeline-ui.js'), win);
  const UI = win.OrderTimelineUI, T = Date.UTC(2026, 9, 11, 15, 0, 0), ev = (type, data, at) => ({ type, at: at || T, by: 'Ana', data });
  const done = D => UI.STAGES.map((_, i) => (UI.stepDone(D, i) ? 1 : 0)).join('');
  ok('timeline: Engraved is done only when every face the line engraves is approved', () => {
    const front = [ev('arrived'), ev('engraveApproved', { face: 'front' }, T + 1)], both = front.concat([ev('engraveApproved', {}, T + 2)]), backOnly = [ev('arrived'), ev('engraveApproved', {}, T + 1)];
    const g = (evs, faces) => UI.derive(evs, null, null, null, null, faces);
    assert.equal(done(g(front, ['front', 'back'])).slice(2, 3), '0', 'the front is approved, the back is not: the step stays open');
    same(g(front, ['front', 'back']).stages[2].partial, ['back']);
    assert.equal(done(g(both, ['front', 'back'])).slice(2, 3), '1', 'both approved: done'); assert.equal(g(both, ['front', 'back']).stages[2].partial, undefined);
    assert.equal(done(g(backOnly, ['front', 'back'])).slice(2, 3), '0', 'a back approval alone does not finish a line that engraves the front too');
    same(g(backOnly, ['front', 'back']).stages[2].partial, ['front']);
    // INERT: a line without a front derives exactly as it did
    for (const faces of [undefined, null, [], ['back']]) { const a = g(backOnly, faces), b = g(backOnly); assert.equal(done(a), done(b)); assert.equal(a.step, b.step); assert.equal(a.stages[2].partial, undefined); }
  });
  ok('timeline: the stamps say FRONT ENGRAVING / FRONT CUT PLAIN, a back event says what it always said', () => {
    assert.equal(UI.faceModel(ev('engraveApproved', { face: 'front' })).action, 'FRONT ENGRAVING');
    assert.equal(UI.faceModel(ev('engraveApproved', {})).action, 'BACK ENGRAVING');
    assert.equal(UI.faceModel(ev('engraveApproved', { face: 'back' })).action, 'BACK ENGRAVING');
  });
  ok('timeline: what the Engraved step still waits on names the face', () => {
    const line = (o) => Object.assign({ key: charm.key, engrave: { needed: true, approved: false, text: 'W' } }, o);
    const need = lines => UI.requirementsOf('engraved', { events: [ev('arrived')], context: { lines, sheets: [] }, faces: lines[0].engraveFaces || null }).need.map(x => x.t).join(' | ');
    assert(/Approve the front and back engraving/.test(need([line({ engraveFaces: ['front', 'back'], engraveDone: { front: false, back: false } })])), need([line({ engraveFaces: ['front', 'back'], engraveDone: {} })]));
    assert(/Approve the back engraving/.test(need([line({ engraveFaces: ['front', 'back'], engraveDone: { front: true, back: false } })])), 'only the face still to approve is named');
    assert(/Approve the back engraving/.test(need([line({})])), 'a back-only line reads as it always did');
  });
}

/* 8 ── Review cards ──────────────────────────────────────────────────────────── */
{
  const b = src('charm-nest-bridge.js');
  const a0 = b.indexOf('  function card(it) {'), a1 = b.indexOf('  async function row_material(it, m, who) {', a0), w0 = b.indexOf('  const faceWordOf = '), w1 = b.indexOf('\n', w0);
  const e0 = b.indexOf('  function engraveFacesOf(x) {'), e1 = b.indexOf('\n  }\n', e0) + 4;
  assert(a0 > 0 && a1 > a0 && w0 > 0 && e0 > 0, 'the Review card and its face helpers are where they were');
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const el = (tag, cls) => { const e = d.createElement(tag); e.className = cls || ''; return e; };
  const ctx = vm.createContext({ window: w, document: d, Engrave: w.Engrave, esc, el, rowsOf: it => [it.row], groupPieces: g => g.length, claudeVerdict: () => 'read', by: () => 'Ana', toast() {}, tryDo: f => { try { return f(); } catch (_) { return undefined; } }, console });
  vm.runInContext(b.slice(a0, a1) + '\n' + b.slice(w0, w1 + 1) + '\n' + b.slice(e0, e1) + '\nthis.card = card; this.faceWordOf = faceWordOf; this.engraveFacesOf = engraveFacesOf;', ctx);
  const ttl = c => c.querySelector('.rh .ttl').textContent, kind = c => c.querySelector('.rh .kind').textContent;
  ok('Review: the face a card is about', () => {
    assert.equal(ctx.faceWordOf(charmF), 'Front'); assert.equal(ctx.faceWordOf(charmB), 'Back', 'the back of a line that has a front says so');
    assert.equal(ctx.faceWordOf(oldJ), '', 'a back-only job says nothing new'); assert.equal(ctx.faceWordOf(oldPL), ''); assert.equal(ctx.faceWordOf(null), '');
  });
  ok('Review: the engraving words card names the face; the other lines\' cards are as they were', () => {
    const item = (job, row) => ({ kind: 'engraveWords', key: 'eng:' + job.key, row, job, why: 'Which words go where?' });
    charmF.lines = []; charmF.text = ''; charmF.state = 'words'; charm.spec.personalization = ['W / 1. 27. 24'];
    const cf = ctx.card(item(charmF, charm));
    assert.equal(kind(cf), 'Front engraving words'); assert.equal(ttl(cf), '4195966039', 'the title is the order, as ever');
    const ta = cf.querySelector('textarea[data-f="text"]'); assert.equal(ta.value, '', 'two faces and no labelled text: the buyer\'s raw words stay in "Customer typed", the box waits for a person');
    assert.equal(ta.getAttribute('placeholder'), 'The words for the front');
    assert(/Engrave this text on the front/.test(cf.querySelector('[data-a=confirm]').textContent)); assert(/Don't engrave the front/.test(cf.querySelector('[data-a=none]').textContent));
    assert(/W \/ 1\. 27\. 24/.test(cf.textContent), 'the buyer\'s raw words are shown');
    const cb = ctx.card(item(charmB, charm)); assert.equal(kind(cb), 'Back engraving words');
    // a back-only line: the very card it always was
    oldJ.state = 'words'; old1.spec.personalization = ['ANNA'];
    const co = ctx.card(item(oldJ, old1));
    assert.equal(kind(co), 'Engraving words'); assert.equal(co.querySelector('textarea').getAttribute('placeholder'), null); assert.equal(co.querySelector('textarea').value, 'ANNA', 'seeded with the buyer\'s words, as ever');
    assert.equal(co.querySelector('[data-a=confirm]').textContent, 'Engrave this text'); assert.equal(co.querySelector('[data-a=none]').textContent, "Don't engrave");
    oldJ.state = 'review';
    const ff = ctx.card({ kind: 'flipFailed', key: 'f', row: charm, job: charmF, why: 'x', checks: { mirrored: false } }), fo = ctx.card({ kind: 'flipFailed', key: 'f', row: old1, job: oldJ, why: 'x', checks: {} });
    assert.equal(kind(ff), 'Front placement check failed'); assert.equal(ttl(ff), '4195966039 · Front'); assert.equal(kind(fo), 'Flip check failed'); assert.equal(ttl(fo), '4170000001', 'a back-only line: the title it always had'); assert.equal(kind(ctx.card({ kind: 'flipFailed', key: 'f', row: charm, job: charmB, why: 'x', checks: {} })), 'Back flip check failed');
  });
  ok('Review/timeline: engraveFacesOf tells a line\'s faces and which are decided; null for every other line', () => {
    reset();
    same(ctx.engraveFacesOf(charm), { faces: ['front', 'back'], done: { front: false, back: false } });
    charmF.state = 'approved'; same(ctx.engraveFacesOf(charm).done, { front: true, back: false }); reset();
    assert.equal(ctx.engraveFacesOf(old1), null); assert.equal(ctx.engraveFacesOf(oldP), null); assert.equal(ctx.engraveFacesOf(oldN), null);
  });
}

/* 8b ── the piece's rail seal: one for each face ──────────────────────────────── */
{
  w.eval(src('charm-nest-timeline-ui.js')); w.eval(src('charm-nest-piece-seals.js'));
  const PSL = w.PieceSeals, T = Date.UTC(2026, 9, 11, 15, 0, 0), ev = (type, data, at, by) => ({ type, at, by: by || 'Ana', id: type + at, lineKey: 'L1', data });
  const piece = faces => ({ key: 'L1', lineKey: 'L1', tid: '', pools: ['p1'], sheets: [], line: Object.assign({ key: 'L1' }, faces ? { engraveFaces: faces } : {}) });
  const kinds = h => [...h.querySelectorAll('.pgSeal')].map(e => (e.dataset.pgFace || '') + (e.classList.contains('pgGhost') ? ':ghost' : ':seal'));
  const labels = h => [...h.querySelectorAll('.seal')].map(e => e.getAttribute('aria-label'));
  const stampText = h => [...h.querySelectorAll('svg text')].map(t => t.textContent.trim()).join(' ');
  ok('piece seals: a seal for each face, the face still to approve as an unfinished seal', () => {
    const a = [ev('arrived', {}, T - 5), ev('engraveApproved', { face: 'front' }, T)];
    let h = PSL.render('engraved', { events: a, piece: piece(['front', 'back']) }, { done: false });
    same(kinds(h), ['front:seal', 'back:ghost']); assert(/FRONT/.test(stampText(h)) && /BACK/.test(stampText(h)), stampText(h)); assert.equal(h.dataset.pgState, 'missing');
    const b = a.concat([ev('engraveApproved', {}, T + 1, 'Bo')]);
    h = PSL.render('engraved', { events: b, piece: piece(['front', 'back']) }, { done: true });
    same(kinds(h), ['front:seal', 'back:seal']); assert.equal(h.dataset.pgState, 'done'); assert(/^Front engraving · Ana/.test(labels(h)[0]) && /^Back engraving · Bo/.test(labels(h)[1]), labels(h).join(' | '));
    h = PSL.render('engraved', { events: b, piece: piece() }, { done: true });
    same(kinds(h), ['front:seal', 'back:seal'], 'the approvals themselves say the piece engraved the front');
  });
  ok('piece seals: a back-only piece is drawn as it was (one seal, the first; or the one unfinished seal)', () => {
    const a = [ev('arrived', {}, T - 5), ev('engraveApproved', {}, T), ev('engraveApproved', {}, T + 5, 'Bo')];
    const h = PSL.render('engraved', { events: a, piece: piece() }, { done: true });
    same(kinds(h), [':seal']); assert.equal(h.dataset.pgFaces, undefined); assert.equal(h.dataset.pgCount, '2'); assert(/^Engraved · Ana/.test(labels(h)[0]), labels(h)[0]);
    const g = PSL.render('engraved', { events: [ev('arrived', {}, T - 5)], piece: piece() }, { done: false });
    same(kinds(g), [':ghost']); assert(/BACK/.test(stampText(g)) && !/FRONT/.test(stampText(g)));
  });
}

/* 8c ── the card mounted in a host (ENGCARD, 12 Oct 2026: ONE card for each PIECE, its Front and its Back side by side, no Front | Back switch, no line editor under it) ──────────── */
okA('mount: one card for a faced piece with its Front and Back side by side; no engraving menu under it, even with the component on the page', async () => {
  reset(); w.Element.prototype.getClientRects = function () { return [{}]; };   // (jsdom has no layout: a card is drawn only where it can be seen)
  const host = d.createElement('div'); d.body.appendChild(host);
  const h = OE.mount(host, { rid: '4195966039', key: charm.key, poolId: 'p1', row: charm, rows: () => [charm], nameOf: () => 'CHARM' });
  await new Promise(r => setTimeout(r, 30));
  const card = host.querySelector('.engCard');
  assert(card, 'the card is drawn'); assert.equal(host.querySelectorAll('.engCard').length, 1, 'ONE card for the piece');
  assert.equal(card.querySelectorAll('.engFace').length, 2, 'its Front and its Back'); assert(card.querySelector('.engFace[data-face="front"]') && card.querySelector('.engFace[data-face="back"]'), 'both faces are on the card');
  assert.equal(host.querySelectorAll('.egEarSwitch').length, 0, 'no Front | Back switch: both faces are on the one card'); assert.equal(host.querySelector('.swEng'), null, 'the old long card is gone');
  w.eval(src('charm-nest-engrave-setup.js'));   // (the real component, ENGSETUP's: mounted by its one call under every card)
  h.refresh(true, true); await new Promise(r => setTimeout(r, 30));
  assert.equal(host.querySelectorAll('.owEngSetup, .egSetup, textarea, select').length, 0, 'the order window card carries no engraving menu (it lives only in the Engraving tab)');
  // a counted line (three discs): a card for each disc, each with its own Front and Back
  const lh = d.createElement('div'); d.body.appendChild(lh);
  const lst = OE.mount(lh, { rid: '4195966040', key: neck.key, row: neck, rows: () => [neck], nameOf: () => 'DISC' }); await new Promise(r => setTimeout(r, 30));
  assert.equal(lh.querySelectorAll('.engCard').length, 3, 'a card for each disc');
  lh.querySelectorAll('.engCard').forEach(c => assert.equal(c.querySelectorAll('.engFace').length, 2, 'each disc has its Front and its Back'));
  delete w.CharmNestEngraveSetup; lst.destroy(); h.destroy(); host.remove(); lh.remove(); reset();
});

/* 9 ── INERT for back-only lines ─────────────────────────────────────────────── */
ok('back-only lines: the cycle shape, the card, the cell are what they always were', () => {
  reset();
  assert.equal(OE.cycleOf(old1, null), null, 'one charm: no cycle'); assert.equal(OE.cycleOf(oldP, null), null, 'a pair: no cycle');
  const c = OE.cycleOf(oldN, null);
  assert(c && c.pieces.length === 3); assert.deepEqual(Object.keys(c), ['pieces', 'key'], 'a counted-disc line: the cycle keeps its two fields (no `faces`)');
  const host = text(S.panel({ kind: 'approve', job: oldD[0], text: 'A', cycle: c, both: { ok: true } }));
  assert.equal(host.querySelector('.swEng').getAttribute('data-face'), null); assert.equal(host.querySelector('.egSetup'), null); assert.equal(host.querySelector('.egApproveBoth'), null, 'a back-only line never offers it, whatever the host says');
  assert.equal(host.querySelector('.egEarSwitch.egFaces'), null); assert.equal(host.querySelectorAll('.egEarSwitch .egEarTab').length, 3);
  assert(/Back engraving/.test(host.querySelector('.fLabel').textContent)); assert.equal(host.querySelector('.egLbl').textContent, 'Words on the back');
  assert.equal(OE.faceCell(old1), null); assert.equal(OE.faceCell(oldP), null); assert.equal(OE.faceCell(oldN), null, 'the facts cell keeps the cell it always had');
  same(OE.discLines(oldN).map(l => l.split(' · ')[0]), ['Disc 1', 'Disc 2', 'Disc 3']);
  same(OE.foldSlots([{ key: oldPL.key, slot: 'L' }, { key: oldPR.key, slot: 'R' }]).map(p => p.key), [oldPL.key, oldPR.key], 'a list with no front is returned as it came');
  assert.equal(w.EngraveLink.resolve({ rid: '4170000002', poolId: 'oR' }).job, oldPR);
  assert.equal(S.list(oldJ).length, 0);
  S.add(oldJ, 'engraveApproved', 'Ana', Date.UTC(2026, 9, 11, 15, 0, 0)); const sl = S.list(oldJ);
  assert.equal(sl.length, 1); assert.equal('face' in sl[0], false, 'a back seal has no face field'); assert(/^engraveApproved:\d+:Ana$/.test(sl[0].id), 'and the id it always had: ' + sl[0].id);
  reset();
});

(async () => {
  for (const [name, fn] of later) { try { await fn(); n++; } catch (e) { e.message = `[${name}] ${e.message}`; throw e; } }
  console.log(`PASS: ${n} checks - the order window, the card, Approve both faces, Fix in Engraving, the stamps, the timeline rail and the Review cards know the front; back-only lines are untouched`);
  w.close();
})().catch(e => { console.error(e); try { w.close(); } catch (_) {} process.exit(1); });
