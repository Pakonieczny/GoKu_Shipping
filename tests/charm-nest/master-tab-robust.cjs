// A master file read three ways must come out the same, and nothing on it may be lost in silence (MASTER2, Paul 14 Oct 2026: SPIDER - WEB_1, one label under two touching bodies,
// was written by neither the Master tab nor reported as anything but "1 charm with no SKU under it").
// Every sheet below is built here (tests/charm-nest/_masterScene.cjs) and indexed by the local script (staged), by the server route and by the Master tab in headless Chromium,
// over an in-memory library. The same rules are asked of every sheet:
//   · every SKU label of the sheet is in the library afterwards, or named by the run as refused or left for a person;
//   · the three routes agree on which SKUs they wrote, which are pairs, how many bodies each holds, and which charms are left without a SKU;
//   · a pair is never written as one body; the other body of a pair built into its label is not "a charm with no SKU";
//   · the same file added again writes nothing; after a library that lost one SKU, only that one is built.
//   PW_DIR=/opt/node-tools/node_modules node tests/charm-nest/master-tab-robust.cjs [only-a-name-part]
'use strict';
const assert = require('assert');
const { scene } = require('./_masterScene.cjs'), R = require('./_masterRoutes.cjs');
const { CharmNestPDF: PDF } = require('../../netlify/functions/_charmNestPdf.js');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const only = process.argv[2] || '';

const A = '0.2 0.35 0.85', B = '0.85 0.2 0.2', C = '0.2 0.6 0.3';
const control = { bodies: [{ shape: 'house', x: 480, y: 150, w: 45, h: 45, ink: C }], labels: [{ text: 'CONTROL_1', cx: 502.5, top: 146 }] };
const merge = (...parts) => ({ page: [620, 330], bodies: [].concat(...parts.map(p => p.bodies || [])), rings: [].concat(...parts.map(p => p.rings || [])), labels: [].concat(...parts.map(p => p.labels || [])), tj: [].concat(...parts.map(p => p.tj || [])) });
// a row of two bodies (house, notch) at x0 with one label under the middle of the row (cx = x0 + 46)
const row2 = (label, o = {}) => ({
  bodies: [{ shape: 'house', x: o.x0 || 60, y: 150, w: 45, h: 45, ink: A }, { shape: o.second || 'notch', x: (o.x0 || 60) + 45 + (o.gap == null ? 2 : o.gap), y: 150, w: 45, h: 45, ink: o.sameInk ? A : B }],
  labels: [].concat(label).map((t, i) => (typeof t === 'string' ? { text: t, cx: o.cx || (o.x0 || 60) + 46 + (o.gap || 0) / 2, top: 149 - (o.drop || 0) - i * 8 } : t))
});

// name → { sheet, want: { SKU: bodies (1 = one body), … }, unlabelled: how many charms are left without a SKU, why: regexp the leftover must be explained with }
const CASES = [
  { name: 'the pair of 14 Oct: slash label "Spider / Web_1"', sheet: merge(row2('Spider / Web_1'), control), want: { 'SPIDER - WEB_1': 2, CONTROL_1: 1 }, unlabelled: 0 },
  { name: 'the same label with a plain hyphen', sheet: merge(row2('SPIDER - WEB_1'), control), want: { 'SPIDER - WEB_1': 2, CONTROL_1: 1 }, unlabelled: 0 },
  { name: 'an en dash typed in the label', sheet: merge(row2('Spider – Web_1'), control), want: { 'SPIDER - WEB_1': 2, CONTROL_1: 1 }, unlabelled: 0 },
  { name: 'lower case label', sheet: merge(row2('spider web_1'), control), want: { 'SPIDER WEB_1': 2, CONTROL_1: 1 }, unlabelled: 0 },
  { name: 'two SKU lines under one pair (both SKUs are the same two-body design)', sheet: merge(row2(['MITTENS_1', 'MITTENS_2_ALT']), control), want: { MITTENS_1: 2, MITTENS_2_ALT: 2, CONTROL_1: 1 }, unlabelled: 0 },
  { name: 'the label split in two text runs', sheet: merge(row2([{ runs: ['Spider /', 'Web_1'], cx: 106, top: 149 }]), control), want: { 'SPIDER - WEB_1': 2, CONTROL_1: 1 }, unlabelled: 0 },
  { name: 'bodies touching (no gap)', sheet: merge(row2('TOUCH_PAIR_1', { gap: 0 }), control), want: { TOUCH_PAIR_1: 2, CONTROL_1: 1 }, unlabelled: 0 },
  { name: 'label 1.2 pt inside the bodies\' boxes', sheet: merge(row2('POKE_PAIR_1', { drop: -1.2 }), control), want: { POKE_PAIR_1: 2, CONTROL_1: 1 }, unlabelled: 0 },
  { name: 'a pair with a jump ring beside each body', sheet: merge(Object.assign(row2('RINGS_PAIR_1'), { rings: [{ cx: 78, cy: 199.5, r: 3.25, inner: 1.85 }, { cx: 128, cy: 199.5, r: 3.25, inner: 1.85 }] }), control), want: { RINGS_PAIR_1: 2, CONTROL_1: 1 }, unlabelled: 0 },
  { name: 'two bodies with a label each: two designs, nothing left over', sheet: merge({ bodies: row2('x').bodies, labels: [{ text: 'LEFT_ONE_1', cx: 82.5, top: 149 }, { text: 'RIGHT_ONE_2', cx: 129.5, top: 149 }] }, control), want: { LEFT_ONE_1: 1, RIGHT_ONE_2: 1, CONTROL_1: 1 }, unlabelled: 0 },
  { name: 'label under the left body only: no guess, the other body is named', sheet: merge({ bodies: row2('x').bodies, labels: [{ text: 'LEFT_ONLY_1', cx: 80, top: 149 }] }, control), want: { LEFT_ONLY_1: 1, CONTROL_1: 1 }, unlabelled: 1, why: /touches a row of 2 bodies \(LEFT_ONLY_1\)/ },
  { name: 'a lone mark "L" beside a label is not part of it', sheet: merge({ bodies: [{ shape: 'house', x: 60, y: 150, w: 45, h: 45, ink: A }], labels: [{ runs: ['L', 'MITTENS_1'], cx: 82.5, top: 149 }] }, control), want: { MITTENS_1: 1, CONTROL_1: 1 }, unlabelled: 0 },
  { name: 'a callout ("FRONT") beside a label is not part of it', sheet: merge({ bodies: [{ shape: 'house', x: 60, y: 150, w: 45, h: 45, ink: A }], labels: [{ runs: ['FRONT', 'ABC_1'], cx: 82.5, top: 149 }] }, control), want: { ABC_1: 1, CONTROL_1: 1 }, unlabelled: 0 },
  { name: 'two labels side by side are two SKUs of the charm, not one', sheet: merge({ bodies: [{ shape: 'house', x: 60, y: 150, w: 45, h: 45, ink: A }], labels: [{ runs: ['ABC_1', 'DEF_2'], cx: 82.5, top: 149 }] }, control), want: { ABC_1: 1, DEF_2: 1, CONTROL_1: 1 }, unlabelled: 0 },
  { name: 'a half label that cannot be joined: the charm is named with the text left under it', sheet: merge({ bodies: [{ shape: 'house', x: 60, y: 150, w: 45, h: 45, ink: A }], labels: [{ runs: ['Spider /', 'FRONT'], cx: 82.5, top: 149 }] }, control), want: { CONTROL_1: 1 }, unlabelled: 1, why: /written under it, not read as a SKU: “Spider \/”/ },
  { name: 'a label in three runs (word, slash, word) is read as typed', sheet: merge({ bodies: [{ shape: 'house', x: 60, y: 150, w: 45, h: 45, ink: A }], labels: [{ runs: ['Spider', '/', 'Web_1'], cx: 82.5, top: 149 }] }, control), want: { 'SPIDER - WEB_1': 1, CONTROL_1: 1 }, unlabelled: 0 },
  { name: 'a word gap in a text array is a space (Hello -400 World_1)', sheet: merge({ bodies: [{ shape: 'house', x: 60, y: 150, w: 45, h: 45, ink: A }], tj: [{ parts: ['Hello', -400, 'World_1'], x: 66, y: 143, size: 6 }] }, control), want: { 'HELLO WORLD_1': 1, CONTROL_1: 1 }, unlabelled: 0 },
  { name: 'a small charm beside a wide one keeps its own label', sheet: merge({ bodies: [{ shape: 'house', x: 60, y: 150, w: 60, h: 50, ink: A }, { shape: 'blob', x: 122, y: 150, w: 24, h: 24, hole: false, ink: B }], labels: [{ text: 'WIDE_ONE_1', cx: 90, top: 149 }, { text: 'SMALL_TWO_2', cx: 134, top: 149 }] }, control), want: { WIDE_ONE_1: 1, SMALL_TWO_2: 1, CONTROL_1: 1 }, unlabelled: 0 },
  { name: 'two pairs touching in one row: no pair is guessed, the bodies are named', sheet: merge({ bodies: [{ shape: 'house', x: 20, y: 150, w: 45, h: 45, ink: A }, { shape: 'notch', x: 67, y: 150, w: 45, h: 45, ink: B }, { shape: 'blob', x: 116, y: 150, w: 45, h: 45, ink: C }, { shape: 'house', x: 163, y: 150, w: 45, h: 45, ink: B }], labels: [{ text: 'PAIR_X_1', cx: 66, top: 149 }, { text: 'PAIR_Y_1', cx: 162, top: 149 }] }, control), want: { PAIR_X_1: 1, PAIR_Y_1: 1, CONTROL_1: 1 }, unlabelled: 2, why: /touches a row of 4 bodies/ },
  { name: 'two identical bodies under one label (twins): left for a person, and said', sheet: merge(row2('TWINS_1', { sameInk: true, second: 'house' }), control), want: { TWINS_1: 1, CONTROL_1: 1 }, unlabelled: 1, why: /in a row of 2 bodies with TWINS_1: / },
  { name: 'three bodies under one label (a set): left for a person, and said', sheet: merge({ bodies: [{ shape: 'house', x: 60, y: 150, w: 45, h: 45, ink: A }, { shape: 'notch', x: 107, y: 150, w: 45, h: 45, ink: B }, { shape: 'blob', x: 154, y: 150, w: 45, h: 45, ink: C }], labels: [{ text: 'SET_OF_3_1', cx: 129.5, top: 149 }] }, control), want: { SET_OF_3_1: 1, CONTROL_1: 1 }, unlabelled: 2, why: /in a row of 3 bodies with SET_OF_3_1/ },
  { name: 'bodies far apart under one wide label: not a row, the other body is named', sheet: merge({ bodies: [{ shape: 'house', x: 60, y: 150, w: 45, h: 45, ink: A }, { shape: 'notch', x: 230, y: 150, w: 45, h: 45, ink: B }], labels: [{ text: 'FAR_APART_1', cx: 82.5, top: 149 }] }, control), want: { FAR_APART_1: 1, CONTROL_1: 1 }, unlabelled: 1, why: /mm from FAR_APART_1/ }
];

(async () => {
  const lab = {};   // per case: what each route did
  const srv = await R.start({ receipts: [] });
  const br = await R.browser({ srv });
  try {
    for (const c of CASES) {
      if (only && !c.name.includes(only)) continue;
      const bytes = await scene(c.sheet);
      // clear the library between cases
      for (const k of [...srv.st.docs.keys()]) if (/^Charm_Master_(Index|Files)\//.test(k)) srv.st.docs.delete(k);
      await br.page.evaluate(() => Master.load(true));
      const out = {};
      // the script (staged)
      { const s = await R.viaScript(bytes, 'case.ai'); out.script = { skus: Object.fromEntries(s.entries.map(e => [e.sku, e.pair ? e.pair.bodies : 1])), unlabelled: s.report.unlabelled.length, why: s.report.unlabelledWhy || {} }; }
      // the server route
      { const r = await R.viaServer(bytes, 'case.ai', srv, 'j' + n); const idx = r.srv.st.list('Charm_Master_Index'); out.server = { skus: Object.fromEntries(idx.map(e => [e._id, e.pair ? e.pair.bodies : 1])), unlabelled: r.job.result.unlabelled.length, why: r.job.result.unlabelledWhy || {} };
        for (const k of [...srv.st.docs.keys()]) if (/^Charm_Master_(Index|Files)\//.test(k)) srv.st.docs.delete(k); }
      // the Master tab
      { await br.page.evaluate(() => Master.load(true)); const r = await br.upload(bytes, 'case.ai'); const idx = srv.st.list('Charm_Master_Index');
        out.tab = { skus: Object.fromEntries(idx.map(e => [e._id, e.pair ? e.pair.bodies : 1])), unlabelled: r.unlabelled.length, why: await br.page.evaluate(() => { const j = [...B.master.jobs.values()].pop(); return j.unlabelledWhy || {}; }), state: r.state, error: r.error };
        // the same file again: nothing new is built
        const again = await br.upload(bytes, 'case.ai'); out.tab.again = { written: again.written, held: again.held, state: again.state };
        // a library that lost one SKU: only that one is rebuilt
        const lost = Object.keys(c.want).sort()[0]; srv.st.docs.delete('Charm_Master_Index/' + lost); await br.page.evaluate(() => Master.load(true));
        const fix = await br.upload(bytes, 'case.ai'); out.tab.fix = { lost, written: fix.written, entries: fix.entries }; }
      lab[c.name] = out;
      const mine = JSON.stringify(c.want, Object.keys(c.want).sort());
      for (const route of ['script', 'server', 'tab']) {
        const got = JSON.stringify(out[route].skus, Object.keys(out[route].skus).sort());
        ok(got === mine, `${c.name} · ${route}: SKUs and their bodies ${got} should be ${mine}`);
        ok(out[route].unlabelled === c.unlabelled, `${c.name} · ${route}: ${out[route].unlabelled} charm(s) without a SKU, should be ${c.unlabelled}`);
        if (c.why && c.unlabelled) ok(Object.values(out[route].why).some(w => c.why.test(w)), `${c.name} · ${route}: the leftover is explained (${JSON.stringify(out[route].why)})`);
      }
      ok(out.tab.state === 'done' && !out.tab.error, `${c.name} · tab: finished (${out.tab.error})`);
      ok(out.tab.again.written === 0 && out.tab.again.state === 'done' && out.tab.again.held >= 1, `${c.name} · tab: the same file again builds nothing ${JSON.stringify(out.tab.again)}`);
      ok(out.tab.fix.written >= 1 && out.tab.fix.entries.includes(out.tab.fix.lost) && out.tab.fix.entries.length <= 2, `${c.name} · tab: after losing ${out.tab.fix.lost} only it is built ${JSON.stringify(out.tab.fix)}`);
      console.log('  ok · ' + c.name);
    }
    ok(br.errors.length === 0, 'no page error: ' + br.errors.join(' | '));
  } finally { await br.close(); srv.close(); }
  console.log(`master-tab-robust: ${n} checks passed`);
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
