// An approved / saved piece's card always shows its back engraving, options and preview (Paul, 16 Oct 2026: "the back engraving must always be visible, whether approved by the user or not").
// The upkeep (shelveWritten) lets a written piece go of its placement (view, mask, fit) once its back is saved in the cloud; the card of such a piece used to find no placement, say "the words need a decision"
// and draw no preview. Now opening the card builds the placement again (reviveShelved, exactly as it was approved), and a placement that cannot be built again shows the saved picture of its back.
// The real module code runs with small stand-ins for the geometry, the fonts and the page. No network.   node tests/charm-nest/engrave-written-card.cjs
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const bridge = fs.readFileSync(path.join(__dirname, '../../charm-nest-bridge.js'), 'utf8');
const slice = (source, from, to) => { const a = source.indexOf(from), b = source.indexOf(to, a); assert(a >= 0 && b > a, 'slice ' + from); return source.slice(a, b); };
const load = (code, env, names) => new Function(...Object.keys(env), `${code}\n;return {${names}};`)(...Object.values(env));
let n = 0; const ok = (c, m) => { assert(c, m); n++; };

function world() {
  let charm = { id: 'c', bits: new Uint8Array([1, 1, 0, 1]), backKeepOut: [] }, fontReady = true;
  const calls = { render: 0, loadFontSet: 0 }, jobs = new Map(), made = [];
  const G = {
    backView: (c, o) => ({ mask: { bits: c.bits, w: 2, h: 2, res: 6, ox: 0, oy: 0 }, upAngle: o.upAngle, cx: 0, cy: 0, members: [], detail: {} }),
    engraveMask: (v, o) => Object.assign({}, v.mask, { marginMm: o.marginMm }),
    layoutLines: (lines, font, size, gap, angle, centre) => { const cmds = [{ t: lines.join('|'), size, gap, angle, centre }]; return { cmds, glyphs: [{ cmds }] }; },
    verifyInk: () => ({ ok: true }),
  };
  const document = { createElement: tag => { const el = { tag, style: {}, children: [], appendChild(c) { this.children.push(c); } }; return el; } };
  const env = { window: { CharmNestAssets: { url: u => 'proxied:' + u } }, document, CharmNestAssets: { url: u => 'proxied:' + u }, F_: { ok: true }, G, EG: { cardKey: null }, items: () => jobs, isWorking: () => false,
    fontFor: w => ({ weight: w }), fitOpts: () => ({ lineGap: 0.18 }), charmFor: () => charm, fontSetOf: () => (fontReady ? {} : null), fontIdOf: () => 'sans',
    loadFontSet: () => { calls.loadFontSet++; fontReady = true; return Promise.resolve({}); }, loadFonts: async () => {}, render: () => { calls.render++; }, recalledJob: j => !!(j && j.recalledFrom && j.recalledFrom.recalled),
    faceOf: j => (j && j.face === 'front' ? 'front' : 'back'), faceWord: j => (j && j.face === 'front' ? 'front' : 'back'), isFront: j => !!(j && j.face === 'front'), fontSetOfJob: () => null };
  const api = load(slice(bridge, '  const hasPlacement = job =>', '  /** Parse the written back file'), env, 'hasPlacement, shelveWritten, reviveShelved, savedBackPicture');
  const job = (key, extra) => {
    const view = G.backView(charm, { upAngle: 90 }), mask = G.engraveMask(view, { marginMm: 0.8 }), layout = G.layoutLines(['Hi'], { weight: 400 }, 5, 0.18, 0, [1, 2]);
    const j = Object.assign({ key, state: 'written', approvedAt: 5, approvedBy: 'P', copies: ['p1'], text: 'Hi', lines: ['Hi'], lineGap: 0.18, view, mask, verify: { geometry: { ok: true } },
      fit: { size: 5, capMm: 2, weight: 400, angle: 0, centre: [1, 2], metrics: {}, lines: ['Hi'], layout, glyphs: layout.glyphs, cmds: layout.cmds },
      backs: [{ poolId: 'p1', approvedAt: 5, outputs: { ai: { url: 'https://x/p1.ai' }, png: { url: 'https://x/p1.png' } } }], row: { order: { receiptId: 'o' }, engrave: { state: 'written' } } }, extra || {});
    jobs.set(key, j); return j;
  };
  return { api, calls, job, setCharm: c => { charm = c; }, charm: () => charm, fontDown: () => { fontReady = false; }, made, document };
}

(async () => {
  // 1 · a piece the upkeep let go of its placement gets it back when its card is opened, exactly as approved
  { const w = world(), j = w.job('a'), cmds = JSON.stringify(j.fit.cmds);
    ok(w.api.shelveWritten() === 1 && j.view === null && j.mask === null && j.fit === null, 'the upkeep let the written piece go of its placement (the state the card used to meet)');
    w.api.reviveShelved(j);
    ok(j.view && j.mask && j.fit && JSON.stringify(j.fit.cmds) === cmds, 'opening its card builds the same placement again: the preview and every control have something to draw');
    ok(j.state === 'written', 'and the piece stays approved / saved: looking at it changes nothing');
    const was = j.fit; w.api.reviveShelved(j); ok(j.fit === was, 'a piece that has its placement is left alone'); }
  // 2 · an approved piece (not yet written) whose placement was shelved is handled the same
  { const w = world(), j = w.job('b', { state: 'approved' }); j.writtenFit = { size: 5, capMm: 2, weight: 400, lines: ['Hi'], centre: [1, 2], angle: 0, approvedAt: 5, upAngle: 90, marginMm: 0.8, lineGap: 0.18, maskKey: null };
    const key = (m => { let h = 2166136261; for (let i = 0; i < m.bits.length; i++) h = Math.imul(h ^ m.bits[i], 16777619) >>> 0; return [m.w, m.h, m.res, m.ox, m.oy, h].join(':'); })(j.mask);
    j.writtenFit.maskKey = key; j.view = j.mask = j.fit = null;
    w.api.reviveShelved(j); ok(j.view && j.fit, 'an approved piece with a kept placement is built again too'); }
  // 3 · what is never touched: a piece in review, a recalled set's piece, a piece that kept no placement
  { const w = world(), r = w.job('r', { state: 'review' }), rc = w.job('rc', { recalledFrom: { recalled: true } }), none = w.job('n');
    r.view = r.mask = r.fit = null; rc.writtenFit = { approvedAt: 5 }; rc.view = rc.mask = rc.fit = null; none.view = none.mask = none.fit = null;
    for (const j of [r, rc, none]) w.api.reviveShelved(j);
    ok(!r.view && !rc.view && !none.view, 'a piece in review, a recalled set\'s piece and a piece with no kept placement are left as they are'); }
  // 4 · the charm's back changed since the approval: nothing is invented, nothing throws; the saved picture stands in
  { const w = world(), j = w.job('c'), pic = { children: [], appendChild(c) { this.children.push(c); } };
    w.api.shelveWritten(); w.setCharm(Object.assign({}, w.charm(), { bits: new Uint8Array([1, 0, 0, 1]) }));
    w.api.reviveShelved(j); ok(!j.view && !j.fit, 'a charm whose back changed is not given a placement it was not approved on');
    ok(w.api.savedBackPicture(pic, j) === true && pic.children.length === 1, 'the card shows the saved picture of the back instead of an empty box');
    const im = pic.children[0]; ok(im.tag === 'img' && im.src === 'proxied:https://x/p1.png' && /^The saved back/.test(im.alt), 'it is the approved back file\'s picture, read through the page\'s own asset door: ' + im.src);
    const front = w.job('f', { face: 'front' }); front.backs = [{ poolId: 'p1', approvedAt: 5, face: 'front', outputs: { png: { url: 'https://x/f.png' } } }, { poolId: 'p1', approvedAt: 9, outputs: { png: { url: 'https://x/b.png' } } }];
    const pf = { children: [], appendChild(c) { this.children.push(c); } }; w.api.savedBackPicture(pf, front); ok(pf.children[0].src === 'proxied:https://x/f.png' && /front/.test(pf.children[0].alt), 'a front piece shows its own face\'s picture, never the back\'s');
    const bare = w.job('e', { backs: [] }), pe = { children: [], appendChild(c) { this.children.push(c); } }; ok(w.api.savedBackPicture(pe, bare) === false && !pe.children.length, 'a piece with no saved picture says so (false): the card keeps its one plain line'); }
  // 5 · the font is still loading: nothing is built yet, the card is drawn again once it has come, and only once
  { const w = world(), j = w.job('d'); w.api.shelveWritten(); w.fontDown();
    w.api.reviveShelved(j); ok(!j.view, 'no placement is built before its font is there'); await new Promise(r => setImmediate(r));
    ok(w.calls.loadFontSet === 1 && w.calls.render === 1, 'the font is asked for and the page drawn again once: ' + JSON.stringify(w.calls));
    w.api.reviveShelved(j); ok(j.view && w.calls.render === 1, 'the next opening builds it, with no further redraw (never a loop)'); }

  // the words the card says: an approved piece is never "waiting for a decision"
  { const src = slice(bridge, '  const waitingReason = (job, panel) =>', '  const matchesQ');
    const mk = () => load(src, { ready: 0, canFit: () => true, facedJob: j => j.face === 'front', faceWord: j => j.face || 'back', OPENREAD: ['reclassify', 'classify'], preparingNow: () => false, readsFree: () => false, DECIDED: ['approved', 'written', 'skipped'] }, 'waitingReason');
    const { waitingReason } = mk();
    for (const state of ['approved', 'written']) ok(!/need a decision/.test(waitingReason({ state })) && /saved .*engraving/i.test(waitingReason({ state })), `a ${state} piece without its placement says it is the saved engraving: ${waitingReason({ state })}`);
    ok(/words need a decision/.test(waitingReason({ state: 'words' })) && /cut plain/.test(waitingReason({ state: 'skipped' })), 'a piece that does wait for words, and a cut-plain one, say what they always said'); }
  console.log('engrave-written-card OK ·', n, 'checks');
})().catch(e => { console.error(e); process.exit(1); });
