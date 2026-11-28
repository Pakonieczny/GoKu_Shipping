'use strict';
/* FRONTBACK (Paul, 28 Nov 2026, order 4251007183, ANARCHY 56633, line 4251007183_5316468909, Back "Galli"; Front pressed): "Get rid of the option for both ... user should just be able to choose front
   and back if there's engraving on both the system should just add engraving to both front and back the user has ultimate control"; "Why would you immediately copy the words from the back engraving
   text box to the front engraving box"; "when I tried to move the text to front engraving it does not work. The engraving does not appear on the front"; "Why does the front engraving also not show any
   of the options that are supposed to be right underneath the main charm work area?"; "when I type in words into a text box for front engraving nothing happens when I try to delete the back
   engraving ... when I come back to the back engraving, it's still there".
   The REAL panel (charm-nest-engrave-panel.js), its model (charm-nest-engrave-setup.js, -faces.js) and the work area's own code (charm-nest-bridge.js svBar, stagedFaceFit, taken from the source),
   driven in node with a stand-in page (no browser, no network, made-up words but Paul's "Galli").
     a  back-only "Galli", Front pressed: the front box is empty, the back keeps "Galli" and stays engraved; no Both / Front & Back option
     b  words typed on the Front: the front is engraved, the stage the work area and the thumbnails get carries them; the options under the work area are the front's as they are the back's
     c  both faces with words: both engraved, the commit is told both
     d  the back's words deleted: the back is no longer engraved; Front, Back, Front: the back stays empty, the front keeps its words
     e  the words of the only engraved face deleted: nothing engraved = None (the pill on None, Approve lit, it keeps no engraving: the commit None makes); switching faces never brings them back
     f  None: both plain; then Front and words: the front only
     g  a front-only line (None | Front) still works
     h  a piece of several (discs): one piece's faces never touch another piece's words
     i  the card left without Approve: discarded; Approve keeps exactly the staged faces
     j  old data / drafts that say 'both': both faces engraved, no crash, no words lost
     k  ANARCHY's front: a face whose middle has no room for the words: the work area places them where the face has room (the place its thumbnail and its own fit find), as the back's were
     l  a saved face with no words (the front of an old 'both' line still waiting for its words) is not engraved: Approve follows the face that has words
     m  the options row and the handles act on the FACE SHOWN: the card opened on the back, both faces placed and approved, Front pressed: the row edits the front's own job (and back again, the back's)
   EF_ROOT=<another checkout> node tests/charm-nest/rt1128-frontback.cjs      (exit 1 while a check fails) */
const fs = require('fs'), path = require('path');
const ROOT = process.env.EF_ROOT ? path.resolve(process.env.EF_ROOT) : path.join(__dirname, '../..');
global.self = global; global.window = global;   // (the page's modules read the page's own objects from their root: the stand-in Engrave below)
let pass = 0, fail = 0; const ok = (c, m, more) => { if (c) { pass++; console.log('  ok   ' + m); } else { fail++; console.log('  FAIL ' + m + (more !== undefined ? '  [' + String(more).slice(0, 400) + ']' : '')); } };
const J = x => { try { return JSON.stringify(x); } catch (_) { return String(x); } };
const wait = ms => new Promise(r => setTimeout(r, ms));
const P = require(path.join(ROOT, 'charm-nest-engrave-panel.js')), S = require(path.join(ROOT, 'charm-nest-engrave-setup.js')), F = require(path.join(ROOT, 'charm-nest-engrave-faces.js'));

/* ── the stand-in page: the jobs of each line, the work area's one call, the commit ── */
const STAGES = [], COMMITS = [], JOBS = new Map();   // line key -> jobs
global.Engrave = {
  jobsOf: row => JOBS.get(((row && row.parentRow) || row || {}).key) || [],
  showStage: (job, stage) => { STAGES.push({ job: typeof job === 'string' ? job : job && job.key, stage: stage ? JSON.parse(JSON.stringify(stage)) : null }); return { ok: true }; },
  // the commit as the bridge's commitStageNow keeps it: the setup (buildOverride of the patch), then every engraved face approved with its words, a face no longer engraved set to none
  commitStage: async (line, st, o) => {
    COMMITS.push(JSON.parse(JSON.stringify(st)));
    const patch = st.patch || {}; if (patch.mode || patch.front !== undefined || patch.back !== undefined) line.engravingOverride = S.buildOverride(line, patch, 'Test Operator', Date.now());
    const on = st.choice === 'both' ? ['front', 'back'] : st.choice === 'none' ? [] : [st.choice], slot = st.slot || null, jobs = JOBS.get(line.key) || [];
    for (const f of ['front', 'back']) {
      let j = jobs.find(x => F.faceOf(x) === f && (x.slot || null) === slot);
      if (on.includes(f)) { const t = String(st.faces[f].text || '').trim(), ls = t.split(/\r?\n/).map(s => s.trim()).filter(Boolean); if (!j) { j = { key: F.keyOf(line, f, slot), face: f, slot, row: line, copies: [line.key + '_1'] }; jobs.push(j); } Object.assign(j, { state: o && o.approve === false ? 'review' : 'approved', text: ls.join('\n'), lines: ls, lineInput: ls, approvedAt: Date.now(), fit: { centre: [0, 0], size: 6 } }); }
      else if (j) Object.assign(j, { state: 'none', decision: { none: true, by: 'Test Operator' } });
    }
    JOBS.set(line.key, jobs);
    return { ok: true, approved: o && o.approve === false ? [] : jobs.filter(j => on.includes(F.faceOf(j)) && (j.slot || null) === slot).map(j => j.key), none: !on.length };
  }
};
const doc = { activeElement: null, body: {}, documentElement: {}, head: { appendChild() {} }, getElementById: () => null, createElement: () => ({ style: {}, dataset: {}, setAttribute() {}, appendChild() {}, getContext: () => null }), querySelector: () => null, querySelectorAll: () => [], defaultView: {} };
function slotEl() {
  const L = {};
  return { ownerDocument: doc, innerHTML: '', hidden: false, isConnected: true, parentElement: null, dataset: {}, style: {}, textContent: '',
    addEventListener(t, f) { (L[t] || (L[t] = [])).push(f); }, removeEventListener(t, f) { L[t] = (L[t] || []).filter(x => x !== f); },
    querySelector: () => null, querySelectorAll: () => [], contains: () => true, closest: () => null, fire(t, e) { for (const f of L[t] || []) f(e); } };
}
const unesc = s => String(s).replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&amp;/g, '&');
/** What the panel draws now: its options, the pressed one, the engraved faces (data-choice), the face shown and its words box. */
function view(el) {
  const h = el.innerHTML, attr = (n) => { const m = new RegExp('<div class="engp"[^>]*\\sdata-' + n + '="([^"]*)"').exec(h); return m ? m[1] : null; };
  const opts = [...h.matchAll(/data-engp="choice" data-choice="(\w+)"[^>]*aria-pressed="(true|false)"[^>]*>([^<]*)<\/button>/g)].map(m => ({ c: m[1], pressed: m[2] === 'true', label: unesc(m[3]) }));
  const ta = /<textarea class="engpWords"[^>]*data-face="(\w+)"[^>]*>([^<]*)<\/textarea>/.exec(h);
  return { options: opts.map(o => o.c), labels: opts.map(o => o.label), pressed: opts.filter(o => o.pressed).map(o => o.c), choice: attr('choice'), active: attr('active'), box: ta ? { face: ta[1], words: unesc(ta[2]) } : null };
}
function mount(line, o) {
  o = o || {}; const el = slotEl(), jobs = () => JOBS.get(line.key) || [], job = o.job || jobs().find(j => F.faceOf(j) === (o.face || 'back') && (j.slot || null) === (o.slot || null)) || jobs()[0];
  const b = P.mount(el, line, { job, face: o.face || 'back', slot: o.slot, jobs });
  const key = (line.key) + (o.slot ? '#' + o.slot : '');
  return {
    el, b, key,
    press: c => el.fire('click', { target: { closest: () => ({ dataset: { engp: 'choice', choice: c, mode: c }, disabled: false }) } }),
    type: (value) => { const f = P.stateOf(key).active; el.fire('input', { type: 'input', target: { dataset: { engp: 'words', face: f }, value, isConnected: true, selectionStart: value.length, selectionEnd: value.length, ownerDocument: doc, style: {} } }); },
    thumb: f => b.setActive(f, 'thumb'),
    view: () => { b.draw(); return view(el); },
    st: () => P.stateOf(key),
    stage: () => P.stageOf(key),
    leave: () => { el.isConnected = false; b.destroy(); P.sweep(); }
  };
}
const lastStage = () => { const s = STAGES.filter(x => x.stage).slice(-1)[0]; return s ? s.stage : null; };
const settle = async key => { for (let i = 0; i < 100; i++) { const bz = P.busyOf(key); if (!bz.running && !bz.pending && !bz.committing) return; await wait(20); } };

/* ── the lines: Paul's ANARCHY "Galli" on the back, a front-only bar, a line of two discs, an old "both" ── */
let n = 0;
function backLine(words) {
  const key = '4251007183_53164689' + String(++n).padStart(2, '0');
  const line = { key, state: 'pooled', order: { receiptId: '4251007183' }, line: { sku: 'ANARCHY_56633', title: 'Anarchy charm' }, poolIds: [key + '_1'],
    spec: { designSku: 'ANARCHY_56633', engraveCandidate: true, personalization: [words], engraving: { v: 1, mode: 'back', source: 'note', textSource: 'single', needsPerson: false, confidence: 0.9, raw: words, front: null, back: { text: words, lines: [words], fontWord: null } } } };
  JOBS.set(key, [{ key, face: 'back', slot: null, row: line, copies: [key + '_1'], state: 'review', text: words, lines: [words], lineInput: [words], fit: { centre: [0, 0], size: 6 } }]);
  return line;
}

(async () => {
  /* a · Front pressed on the back-only "Galli" */
  { const line = backLine('Galli'), c = mount(line, { face: 'back' }), v0 = c.view();
    ok(v0.options.join('|') === 'none|front|back' && !v0.labels.some(l => /both|&/i.test(l)), 'a0 the choice is None | Front | Back: no "Front & Back" / "Both" option', J(v0.labels));
    ok(v0.choice === 'back' && v0.box && v0.box.face === 'back' && v0.box.words === 'Galli', 'a0 it opens on the back, "Galli", engraved', J(v0));
    c.press('front'); const v1 = c.view(), s1 = c.st();
    ok(v1.box && v1.box.face === 'front' && v1.box.words === '', 'a1 Front pressed: the front box is EMPTY (nothing copied or moved from the back)', J(v1.box));
    ok(s1.faces.back.text === 'Galli', 'a2 the back keeps "Galli"', J(s1.faces));
    ok(s1.choice === 'back' && v1.pressed.join() === 'front' && s1.active === 'front', 'a3 the back is still engraved (the faces engraved: back); Front is the face shown', J({ choice: s1.choice, pressed: v1.pressed, active: s1.active }));
    const st = lastStage(); ok(st && st.choice === 'back' && st.active === 'front' && st.faces.back.text === 'Galli' && st.faces.front.text === '', 'a4 the work area is told: the front shown, plain; the back engraved "Galli"', J(st));
    c.press('back'); const v2 = c.view(); ok(v2.box && v2.box.face === 'back' && v2.box.words === 'Galli' && c.st().choice === 'back', 'a5 Back again: "Galli", as it was', J(v2));
    c.leave(); }

  /* b · words typed on the Front */
  { const line = backLine('Galli'), c = mount(line, { face: 'back' }); c.press('front'); c.type('Xy');
    const s = c.st(), st = lastStage();
    ok(s.choice === 'both', 'b1 words typed on the Front: the front is engraved (the back too: both have words)', s.choice);
    ok(st && st.active === 'front' && st.faces.front.text === 'Xy' && ['front', 'both'].includes(st.choice), 'b2 the stage the work area and the thumbnails are painted from (Engrave.showStage) carries the front\'s words at once', J(st));
    ok(c.view().box.words === 'Xy', 'b3 the front box shows "Xy"');
    c.leave(); }

  /* c · both faces with words: both engraved, the commit says both */
  { const line = backLine('Galli'), c = mount(line, { face: 'back' }); c.press('front'); c.type('Mia');
    ok(c.st().choice === 'both' && c.st().gate === 'approve', 'c1 front "Mia" + back "Galli": both engraved, Approve lit', J(c.st()));
    COMMITS.length = 0; const pressed = c.b.press(); await settle(c.key);
    const cm = COMMITS[0] || {};
    ok(pressed && cm.choice === 'both' && cm.faces.front.text === 'Mia' && cm.faces.back.text === 'Galli' && (cm.patch || {}).mode === 'both', 'c2 Approve: the commit is told both faces, each with its own words', J(cm));
    const js = JOBS.get(line.key); ok(js.some(j => j.face === 'front' && j.state === 'approved' && j.text === 'Mia') && js.some(j => F.faceOf(j) === 'back' && j.state === 'approved' && j.text === 'Galli'), 'c3 and both faces are kept approved with their words', J(js.map(j => [j.key, j.state, j.text])));
    c.leave(); }

  /* d · the back's words deleted while the front has words */
  { const line = backLine('Galli'), c = mount(line, { face: 'back' }); c.press('front'); c.type('Mia'); c.press('back'); c.type('');
    ok(c.st().choice === 'front', 'd1 the back\'s words deleted: the back is no longer engraved (only the front)', c.st().choice);
    c.press('front'); const vf = c.view(); c.press('back'); const vb = c.view(); c.press('front'); const vf2 = c.view();
    ok(vb.box && vb.box.face === 'back' && vb.box.words === '' && c.st().faces.back.text === '', 'd2 Front, then Back: the back box stays empty (the deletion stuck)', J(vb.box));
    ok(vf.box.words === 'Mia' && vf2.box.words === 'Mia' && c.st().choice === 'front', 'd3 the front keeps "Mia" and stays the one engraved face', J({ vf, vf2, choice: c.st().choice }));
    COMMITS.length = 0; c.b.press(); await settle(c.key);
    const cm = COMMITS[0] || {}; ok(cm.choice === 'front' && (cm.patch || {}).mode === 'front', 'd4 Approve: the commit keeps the front only; the back is written as not engraved', J(cm));
    ok((JOBS.get(line.key).find(j => F.faceOf(j) === 'back') || {}).state === 'none', 'd5 and the back job is set to no engraving', J(JOBS.get(line.key).map(j => [j.key, j.state])));
    c.leave(); }

  /* e · the words of the only engraved face deleted: nothing engraved = None */
  { const line = backLine('Galli'), c = mount(line, { face: 'back' }); c.type('');
    const s = c.st(), v = c.view(), st = lastStage();
    ok(s.choice === 'none' && v.pressed.join() === 'none' && s.gate === 'approve' && st.choice === 'none', 'e1 the only engraved face\'s words deleted: nothing is engraved, the same as None: the pill on None, Approve lit, the work area plain', J({ choice: s.choice, pressed: v.pressed, gate: s.gate, stage: st && st.choice }));
    c.press('front'); const s2 = c.st(), v2 = c.view(); c.press('back'); const s3 = c.st(), vb = c.view();
    ok(s2.choice === 'none' && s3.choice === 'none' && v2.pressed.join() === 'none' && v2.box.face === 'front' && vb.box.face === 'back' && vb.box.words === '' && s3.gate === 'approve', 'e2 Front, then Back: each face shown, still nothing engraved (None), the back still empty: its words never come back', J({ s2: s2.choice, s3: s3.choice, v2, vb }));
    COMMITS.length = 0; const pressed = c.b.press(); await settle(c.key); const cm = COMMITS[0] || {};
    ok(pressed && cm.choice === 'none' && (cm.patch || {}).mode === 'none', 'e3 Approve keeps it as no engraving: the very commit None makes', J(cm));
    ok((JOBS.get(line.key).find(j => F.faceOf(j) === 'back') || {}).state === 'none', 'e4 and the back is set to no engraving', J(JOBS.get(line.key).map(j => [j.key, j.state])));
    c.leave(); }
  { const line = backLine('Galli'), c = mount(line, { face: 'back' }); c.type(''); c.press('front'); c.type('Ro'); const s4 = c.st();
    ok(s4.choice === 'front' && s4.gate === 'approve' && c.view().pressed.join() === 'front', 'e5 words typed on the front then: the front alone is engraved, Approve lit, the pill on Front', J({ choice: s4.choice, gate: s4.gate }));
    c.leave(); }

  /* f · None, then Front and words */
  { const line = backLine('Galli'), c = mount(line, { face: 'back' }); c.press('none');
    const s = c.st(), st = lastStage(), v = c.view();
    ok(s.choice === 'none' && v.pressed.join() === 'none' && st.choice === 'none' && s.gate === 'approve', 'f1 None: both faces plain, None selected, Approve lit (None staged)', J({ s: s.choice, pressed: v.pressed, st: st.choice, gate: s.gate }));
    c.press('front'); const v2 = c.view();
    ok(v2.box.face === 'front' && v2.box.words === '', 'f2 Front pressed after None: the front box is empty (never the back\'s "Galli")', J(v2.box));
    c.type('Kai'); const s3 = c.st();
    ok(s3.choice === 'front' && s3.faces.back.text === '' && s3.gate === 'approve', 'f3 words typed: the front ONLY is engraved', J({ choice: s3.choice, back: s3.faces.back, gate: s3.gate }));
    c.press('back'); const v4 = c.view(); ok(v4.box.words === '' && c.st().choice === 'front', 'f4 Back pressed: the back stays plain and empty', J({ box: v4.box, choice: c.st().choice }));
    c.leave(); }
  { const line = backLine('Galli'), c = mount(line, { face: 'back' }); c.press('none'); c.press('back'); const v = c.view();
    ok(v.box.words === 'Galli' && c.st().choice === 'back' && !c.st().dirty, 'f5 None, then Back: the back\'s own "Galli" comes back, as it always did (nothing staged)', J({ v, st: c.st() }));
    c.press('front'); ok(c.view().box.words === '' && c.st().choice === 'back', 'f6 then Front: empty, the back still engraved', J(c.st()));
    c.leave(); }

  /* g · a front-only line */
  { const key = '4251007183_5316400001', line = { key, state: 'pooled', order: { receiptId: '4251007183' }, line: { sku: 'BAR_1', title: 'Bar' }, poolIds: [key + '_1'],
      spec: { designSku: 'BAR_1', engraveCandidate: true, engraveFrontOnly: { why: 'front only' }, personalization: ['Lea'], engraving: { v: 1, mode: 'front', source: 'profile', textSource: 'single', raw: 'Lea', front: { text: 'Lea', lines: ['Lea'], fontWord: null }, back: null } } };
    JOBS.set(key, [{ key: key + '~front', face: 'front', slot: null, row: line, copies: [key + '_1'], state: 'review', text: 'Lea', lines: ['Lea'], lineInput: ['Lea'], fit: { centre: [0, 0], size: 6 } }]);
    const c = mount(line, { face: 'front' }), v = c.view();
    ok(v.options.join('|') === 'none|front' && v.box.face === 'front' && v.box.words === 'Lea' && c.st().choice === 'front', 'g1 a front-only line: None | Front, the front "Lea"', J(v));
    c.type('Lea Ann'); ok(c.st().choice === 'front' && c.st().gate === 'approve' && lastStage().faces.front.text === 'Lea Ann', 'g2 its words changed: the front engraved with them, Approve lit', J(c.st()));
    c.press('none'); ok(c.st().choice === 'none', 'g3 None: no engraving'); c.press('front'); ok(c.view().box.words === 'Lea Ann' && c.st().choice === 'front', 'g4 Front again: its own words come back', J(c.view()));
    c.leave(); }

  /* h · a line of two discs: two pieces */
  { const key = '4251007183_5316400002', line = { key, state: 'pooled', order: { receiptId: '4251007183' }, line: { sku: 'DISC_2', title: 'Two discs', quantity: 1 }, poolIds: [key + '_1', key + '_2'],
      spec: { designSku: 'DISC_2', engraveCandidate: true, personalization: ['Ann'], engraving: { v: 1, mode: 'back', source: 'note', textSource: 'single', raw: 'Ann', front: null, back: { text: 'Ann', lines: ['Ann'], fontWord: null } } } };
    JOBS.set(key, ['D1', 'D2'].map((sl, i) => ({ key: key + '#' + sl, face: 'back', slot: sl, row: line, copies: [key + '_' + (i + 1)], state: 'review', text: 'Ann', lines: ['Ann'], lineInput: ['Ann'], fit: { centre: [0, 0], size: 6 } })));
    const c1 = mount(line, { face: 'back', slot: 'D1' }), c2 = mount(line, { face: 'back', slot: 'D2' });
    c1.press('front'); c1.type('Bo'); c1.press('back'); c1.type(''); c1.press('front');
    const v2 = c2.view(), s2 = c2.st(), s1 = c1.st();
    ok(s1.choice === 'front' && s1.faces.front.text === 'Bo', 'h1 disc 1: front "Bo", its back deleted', J(s1));
    ok(s2.choice === 'back' && v2.box.face === 'back' && v2.box.words === 'Ann' && s2.faces.front.text === '', 'h2 disc 2 untouched: its back "Ann", its front empty', J({ s2, v2 }));
    c2.press('front'); ok(c2.view().box.words === '' && c1.st().faces.front.text === 'Bo' && c2.st().faces.back.text === 'Ann', 'h3 Front pressed on disc 2: empty there, disc 1 still "Bo"');
    c1.leave(); c2.leave(); }

  /* i · left without Approve: discarded; Approve keeps exactly what is staged */
  { const line = backLine('Galli'); let c = mount(line, { face: 'back' }); c.press('front'); c.type('Zed'); c.press('back'); c.type('');
    const before = JSON.stringify(JOBS.get(line.key)), ov = line.engravingOverride; c.leave();
    ok(!P.stateOf(c.key) && JSON.stringify(JOBS.get(line.key)) === before && line.engravingOverride === ov, 'i1 the card left with no Approve: the draft is gone, nothing kept', J(P.stateOf(c.key)));
    ok(STAGES.slice(-3).some(x => x.stage === null), 'i2 and the work area is told to show what is kept (showStage(job, null))');
    c = mount(line, { face: 'back' }); const v = c.view();
    ok(v.box.face === 'back' && v.box.words === 'Galli' && c.st().choice === 'back' && c.st().faces.front.text === '', 'i3 the piece opened again: back "Galli", front empty: what is kept', J(v));
    c.press('front'); c.type('Zed'); COMMITS.length = 0; c.b.press(); await settle(c.key);
    const cm = COMMITS[0] || {};
    ok(cm.choice === 'both' && cm.faces.front.text === 'Zed' && cm.faces.back.text === 'Galli', 'i4 Approve keeps exactly the staged faces: front "Zed" and back "Galli"', J(cm));
    c.leave(); c = mount(line, { face: 'back' });
    ok(c.st().choice === 'both' && c.view().box.words === 'Galli' && (c.press('front'), c.view().box.words === 'Zed'), 'i5 opened again: both faces kept, each with its words', J(c.st()));
    c.leave(); }

  /* j · old data and drafts that say 'both' */
  { const key = '4251007183_5316400003', line = { key, state: 'pooled', order: { receiptId: '4251007183' }, line: { sku: 'TAG_1', title: 'Tag' }, poolIds: [key + '_1'],
      spec: { designSku: 'TAG_1', engraveCandidate: true, personalization: ['front - W\nback - 1. 27. 24'], engraving: { v: 1, mode: 'both', source: 'option', textSource: 'labelled', raw: 'front - W\nback - 1. 27. 24', front: { text: 'W', lines: ['W'], fontWord: null }, back: { text: '1. 27. 24', lines: ['1. 27. 24'], fontWord: null } } },
      engravingOverride: { mode: 'both', front: { text: 'W', lines: ['W'], fontWord: null }, back: { text: '1. 27. 24', lines: ['1. 27. 24'], fontWord: null }, by: 'Old Session', at: 1 } };
    JOBS.set(key, [{ key, face: 'back', slot: null, row: line, copies: [key + '_1'], state: 'approved', text: '1. 27. 24', lines: ['1. 27. 24'], lineInput: ['1. 27. 24'], fit: { centre: [0, 0], size: 6 } }, { key: key + '~front', face: 'front', slot: null, row: line, copies: [key + '_1'], state: 'approved', text: 'W', lines: ['W'], lineInput: ['W'], fit: { centre: [0, 0], size: 6 } }]);
    let threw = '', c = null; try { c = mount(line, { face: 'back' }); } catch (e) { threw = e.message; }
    const v = c ? c.view() : {}; ok(!threw && c.st().choice === 'both' && v.box.words === '1. 27. 24' && v.pressed.join() === 'back', 'j1 a line saved as "both": both faces engraved, the back shown "1. 27. 24", the Back option selected', threw || J({ st: c && c.st(), v }));
    c.press('front'); ok(c.view().box.words === 'W' && c.st().choice === 'both' && !c.st().dirty, 'j2 Front: "W"; still both, nothing staged', J(c.st()));
    let h = ''; try { h = P.html(S.read(line, { jobs: JOBS.get(key) }), { key, mode: 'both', faces: { front: { text: 'W', font: '' }, back: { text: '1. 27. 24', font: '' } }, face: 'front' }, {}); } catch (e) { h = 'THREW ' + e.message; }
    const vh = view({ innerHTML: h });
    ok(!/^THREW/.test(h) && vh.choice === 'both' && vh.box && vh.box.words === 'W' && vh.pressed.join() === 'front', 'j3 an old-shaped draft { mode: "both" } draws: both faces engraved, the front "W", Front selected', h.slice(0, 200));
    c.leave(); }

  /* l · a saved face with no words is not engraved */
  { const key = '4251007183_5316400004', line = { key, state: 'pooled', order: { receiptId: '4251007183' }, line: { sku: 'TAG_2', title: 'Tag' }, poolIds: [key + '_1'],
      spec: { designSku: 'TAG_2', engraveCandidate: true, personalization: ['Galli'], engraving: { v: 1, mode: 'both', source: 'option', textSource: 'unlabelled', needsPerson: true, raw: 'Galli', front: null, back: { text: 'Galli', lines: ['Galli'], fontWord: null } } } };
    JOBS.set(key, [{ key, face: 'back', slot: null, row: line, copies: [key + '_1'], state: 'approved', text: 'Galli', lines: ['Galli'], lineInput: ['Galli'], fit: { centre: [0, 0], size: 6 } }, { key: key + '~front', face: 'front', slot: null, row: line, copies: [key + '_1'], state: 'words', text: '', lines: [], lineInput: [] }]);
    const c = mount(line, { face: 'back' }), s = c.st();
    ok(s.choice === 'back' && s.gate === 'approve', "l1 an old 'both' line whose front still waits for words: the front is not engraved (no words), the back is; Approve follows the back, lit", J(s));
    c.press('front'); const v = c.view(); ok(v.box.face === 'front' && v.box.words === '' && c.st().choice === 'back', 'l2 Front pressed: its box empty, still the back only', J({ v, choice: c.st().choice }));
    COMMITS.length = 0; c.b.press(); await settle(c.key); const cm = COMMITS[0] || {};
    ok(cm.choice === 'back' && (cm.patch || {}).mode === 'back' && (JOBS.get(key).find(j => j.face === 'front') || {}).state === 'none', 'l3 Approve keeps the back alone; the front is set to no engraving', J({ cm, jobs: JOBS.get(key).map(j => [j.key, j.state]) }));
    c.leave(); }

  /* k · the work area (charm-nest-bridge.js): ANARCHY's front, and the options under it */
  await partK();
  /* m · the options row on the face shown */
  partM();
  console.log(`\nrt1128-frontback: ${pass} passed, ${fail} failed`); process.exit(fail ? 1 : 0);
})().catch(e => { console.log('HARNESS ERROR', e && e.stack || e); process.exit(2); });

/* ── m · the row under the work area and the drawing's handles act on the face shown (charm-nest-bridge.js: rowJob, setRow and the row's presses, taken from the source) ── */
function partM() {
  const src = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8');
  const node = () => ({ disabled: false, title: '', value: '', hidden: false, textContent: '', style: { setProperty() {} }, querySelector: () => null });
  const out = node(), cen = node(), sel = node(), cap = node(), grp = Object.assign(node(), { querySelector: s => (s === 'output' ? out : null) }), sp = node(), turn = node(), steps = [node(), node()];
  const ctl = { querySelector: s => (/centre/.test(s) ? cen : /linecount/.test(s) ? sel : /data-cap/.test(s) ? cap : /spacingControl/.test(s) ? grp : /spacing/.test(s) ? sp : /turn"\]$/.test(s) ? turn : null), querySelectorAll: s => (/turnStep/.test(s) ? steps : []) };
  const card = { dataset: {}, querySelector: s => (s === '.ctl' ? ctl : null) };
  // Paul's piece after both faces were approved: the card opened on the BACK ("Galli", one line, the system's turn), the FRONT placed with "Mi / a" on two lines, its own line mode, line gap and turn
  const back = { key: 'L', face: 'back', state: 'approved', fit: { capMm: 2.0, centre: [1, 1] }, lines: ['Galli'], lineMode: 'auto', lineGap: null, turn: 0 };
  const front = { key: 'L~front', face: 'front', state: 'approved', fit: { capMm: 1.4, centre: [2, 2] }, lines: ['Mi', 'a'], lineMode: '2', lineGap: 0.25, turn: 15 };
  // the row as the card drew it for its own job (the back)
  cen.disabled = false; sel.disabled = false; sel.value = 'auto'; cap.textContent = '2.00 mm'; grp.hidden = true; sp.value = '120'; turn.value = '0'; turn.disabled = false;
  const calls = [], items = new Map([[back.key, back], [front.key, front]]);
  const env = { job: back, card, backHost: { querySelector: () => null }, DRAFTS: new Map(), capText: f => f.capMm.toFixed(2) + ' mm', fitOpts: j => ({ lineGap: (j && j.lineGap) ?? 0.216 }), turnOf: j => j.turn || 0,
    placeEditable: j => ['review', 'approved'].includes(j.state), document: { activeElement: null }, maskCentroid: () => [0, 0], centreText: j => calls.push(['centre', j.key]), fitNewWords: async j => calls.push(['words', j.key]),
    toast: () => {}, render: () => {}, items: () => items, nudge: (j, dx, dy) => calls.push(['nudge', j.key, dx, dy]) };
  let R = null, why = '';
  try { const a = src.indexOf('    let rowJob = job, rowOwn = null;'), b = src.indexOf('    const svTarget = () => {', a); if (a < 0 || b < 0) throw new Error('no rowJob in this bridge'); const k = Object.keys(env);
    R = new Function(...k, src.slice(a, b) + '\nreturn { setRow, rowCentre, rowNudge, rowWords, get row() { return rowJob; } };')(...k.map(x => env[x])); } catch (e) { why = e.message; }
  ok(!!R, 'm0 the work area has ONE job for its row: the face shown\'s (rowJob, setRow)', why);
  if (!R) { for (const id of ['m1', 'm2', 'm3', 'm4', 'm5', 'm6']) ok(false, id + ' (no row of the face shown)'); }
  else {
    R.setRow(front);
    ok(R.row === front && card._rowJob === front && !cen.disabled && !sel.disabled && !turn.disabled && !steps.some(b => b.disabled), 'm1 Front pressed on the back\'s card, both faces approved: the row is the FRONT\'s, live (Centre, Lines, the turn)', J({ cen: cen.disabled, sel: sel.disabled, turn: turn.disabled }));
    ok(cap.textContent === '1.40 mm' && sel.value === '2' && sp.value === '139' && out.textContent === '139%' && grp.hidden === false && turn.value === '15', 'm2 it shows the front\'s own cap height, line mode, line spacing (two lines: the slider shows) and turn', J({ cap: cap.textContent, sel: sel.value, sp: sp.value, grp: grp.hidden, turn: turn.value }));
    R.rowCentre(); ok(calls.some(c => c[0] === 'centre' && c[1] === 'L~front') && !calls.some(c => c[1] === 'L'), 'm3 Centre centres the FRONT\'s words (never the back\'s)', J(calls));
    calls.length = 0; const n0 = R.rowNudge(0.05, 0); front.state = 'review'; const n1 = R.rowNudge(0.05, 0);
    ok(!n0 && n1 && calls.length === 1 && calls[0][1] === 'L~front', 'm4 the arrow keys move the front\'s words (an approved face is not reopened by them, as on the back)', J({ n0, n1, calls }));
    front.state = 'approved'; R.setRow(back);
    ok(R.row === back && cap.textContent === '2.00 mm' && sel.value === 'auto' && turn.value === '0' && grp.hidden === true && !cen.disabled, 'm5 Back again: the row is the back\'s again, its own values', J({ cap: cap.textContent, sel: sel.value, turn: turn.value, grp: grp.hidden }));
    R.setRow(null); ok(cen.disabled && sel.disabled && cap.textContent === '— mm' && turn.value === '0', 'm6 staged words on a face with no placement: the row waits for them (svBar lights it), the turn is the whole piece\'s', J({ cen: cen.disabled, cap: cap.textContent }));
  }
  // every control of the row asks it; the other face's placed job gets this card's own handles; nothing hides the row of the face shown
  ok(/else if \(a === "centre"\) rowCentre\(\);/.test(src), 'm7 Centre: the face shown');
  ok(/const j = rowJob; if \(!j \|\| rowBusy\(j\) \|\| !startEdit\(j\)\) return;[\s\S]{0,200}j\.lineMode=lineControl\.value;\s*if \(j === job\) await applyWords\(\); else await rowWords\(j\);/.test(src), 'm8 Lines: the face shown\'s words, fitted again with it');
  ok(/const j=rowJob;if\(!card\.isConnected \|\| !j \|\| !placeEditable\(j\)\)return;[\s\S]{0,120}setLineSpacing\(j,pending,measure\)/.test(src), 'm9 Line spacing: the face shown\'s');
  ok(/tj = \(\) => rowJob \|\| job;/.test(src) && /const r = turnTo\(tj\(\), deg\);/.test(src), 'm10 the turn: the face shown\'s job (on staged words, the whole piece)');
  ok(/rowNudge\(e\.key === "ArrowLeft"/.test(src) && !/card\.dataset\.svOwn === "0"/.test(src), 'm11 the arrow keys: the face shown\'s words; nothing gates them on the card\'s own face');
  ok(/setRow\(ja\); wire\(bc, ja\);/.test(src) && /const wire = \(bc, job\) =>/.test(src), 'm12 the other face\'s placed job is moved, sized and turned by the very handles of this card\'s own face (wire)');
  ok(/svHooks\(tgt\.face, tgt\.words, tgt\.seq, true\)/.test(src) && /svHooks\(t\.face, t\.words, t\.seq, true\)/.test(src), 'm13 the row is never hidden for the face shown (data-sv-own stays 1 on every staged drawing)');
  ok(/EG\.cardKey !== job\.key && c\._rowJob !== job/.test(src), 'm14 a change made on the face shown paints this card (refresh / repaintTurn follow the face shown)');
}

/* ── k · the work area's own code, taken from charm-nest-bridge.js and run on the real geometry and font ── */
async function partK() {
  const src = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8');
  const cut = (from, to) => { const a = src.indexOf(from); if (a < 0) throw new Error('not in the bridge: ' + from); const b = src.indexOf(to, a + from.length); if (b < 0) throw new Error('no end for: ' + from); return src.slice(a, b); };
  const G = require(path.join(ROOT, 'charm-nest-geom.js')), Fit = require(path.join(ROOT, 'charm-nest-engrave-fit.js')), ot = require(path.join(ROOT, 'vendor/opentype-1.3.4.min.js'));
  const font = ot.loadSync(path.join(ROOT, 'vendor/fonts/SourceSans3-Regular.otf')), PT = 72 / 25.4;
  const env = { G, O: { capsText: t => t, fontOfPhrase: () => '', pieceFont: () => ({ id: 'sans' }), DEFAULT_FONT: 'sans' }, S: { settings: { engraveMarginMm: 0.088 } }, draftSet: () => ({ Regular: font }), pinFont: f => f,
    pieceIndexOf: () => null, loadFonts: async () => {}, loadFontSet: async () => {}, STAGED_FACES: new Map(), EG: {}, window: { CharmNestEngraveFit: Fit } };
  const text = cut('  const fitOpts = (job, byHand) => {', '  const items = () => B.engrave.items;') + cut('  const faceArtOf = (charm, front) =>', '\n') + '\n' + cut('  const draftLines = text =>', '\n') + '\n'
    + cut('  function maskCentroid(m) {', '  function centreText(job)') + cut('  function stagedFaceFit(job, face, fv, rec) {', '  /** A drag of the staged words');
  const k = Object.keys(env), mk = new Function(...k, text + '\nreturn { stagedFaceFit, maskCentroid };')(...k.map(x => env[x]));
  // a made-up plate, 20 x 12 mm, with a jump-ring hole; its FRONT carries its own keep-out over the whole middle (as a design with its own front art does: ANARCHY's front), its back none
  const poly = pts => pts.map((p, i) => [i ? 'l' : 'm', p]).concat([['h']]), bb = pts => [Math.min(...pts.map(p => p[0])), Math.min(...pts.map(p => p[1])), Math.max(...pts.map(p => p[0])), Math.max(...pts.map(p => p[1]))];
  const rect = (x, y, w, h) => [[x, y], [x + w, y], [x + w, y + h], [x, y + h]], ring = (cx, cy, r) => [...Array(48).keys()].map(i => [cx + r * Math.cos(i / 48 * 2 * Math.PI), cy + r * Math.sin(i / 48 * 2 * Math.PI)]);
  const seg = (pts, o) => Object.assign({ kind: 'path', closed: true, subpaths: [poly(pts)], bbox: bb(pts), lwPt: 0.25, paintOp: 'S', layer: 'CUT', stroke: true, fill: false, strokeRGB: [0, 0, 0] }, o || {});
  const W = 20 * PT, H = 12 * PT, outline = seg(rect(0, 0, W, H)), hole = seg(ring(W / 2, H - 1.4 * PT, 0.7 * PT)), middle = seg(rect(3 * PT, 3.2 * PT, W - 6 * PT, H - 6.4 * PT), { layer: 'FRONT KEEP-OUT' });
  const plate = keepOn => ({ outline, members: [outline, hole], bbox: outline.bbox, widthPt: W, heightPt: H, frontKeepOut: keepOn === 'front' ? [middle] : [], backKeepOut: keepOn === 'back' ? [middle] : [] });
  const fvOf = (charm, face) => ({ view: G.backView(charm, { res: 6, upAngle: 90, ...(face === 'front' ? { face: 'front' } : {}) }), charm });
  const job = { key: 'L', row: { spec: {} }, lineGap: null };
  const fitFace = (charm, face, words, rec) => { rec = rec || { face, centre: null, size: null, view: null, mask: null }; const fv = fvOf(charm, face); rec.text = words; const sf = mk.stagedFaceFit(job, face, fv, rec); return { rec, fv, sf, fit: sf.fit, mask: sf.mask }; };
  const charm = plate('front');
  const b = fitFace(charm, 'back', 'Galli'), f = fitFace(charm, 'front', 'Galli');
  const c0 = mk.maskCentroid(f.mask), onMetal = (m, p) => { const x = Math.floor((p[0] - m.ox) * m.res), y = Math.floor((p[1] - m.oy) * m.res); return x >= 0 && y >= 0 && x < m.w && y < m.h && !!m.bits[y * m.w + x]; };
  ok(!!b.fit && b.fit.glyphs.length > 0, 'k0 the back of the plate: "Galli" is drawn on it (its middle has metal)', J(b.fit && b.fit.size));
  ok(!onMetal(f.mask, c0), 'k0 the front of the plate: the middle of its metal is not metal for words (its own keep-out), as ANARCHY\'s front', J(c0));
  ok(!!f.fit && f.fit.glyphs.length > 0 && G.verifyInk(f.fit.cmds, f.mask).ok, 'k1 the FRONT\'s staged "Galli" is drawn in the work area, on the face\'s metal (it was nothing: tried at the middle only)', J({ fit: !!f.fit, note: f.rec.note }));
  const q = Fit.quickPlace({ charm, face: 'front', lines: ['Galli'], mask: f.mask, opts: { minCapMm: 1.6, maxHeightFrac: 0.4, lineGap: 0.216, minStrokeMm: 0, minGapMm: 0, measure: false } }, { Regular: font }, G);
  ok(!!f.fit && q.status === 'ready' && Math.hypot(f.fit.centre[0] - q.fit.centre[0], f.fit.centre[1] - q.fit.centre[1]) < 1e-6, 'k2 at the place the face\'s own placement finds (the place its thumbnail draws them: CharmNestEngraveFit.quickPlace)', J({ at: f.fit && f.fit.centre, quick: q.fit && q.fit.centre }));
  const f2 = (() => { f.rec.text = 'Gallia'; const sf = mk.stagedFaceFit(job, 'front', f.fv, f.rec); return sf.fit; })();
  ok(!!f2 && f2.glyphs.length > 0 && !!f.fit && Math.hypot(f2.centre[0] - f.fit.centre[0], f2.centre[1] - f.fit.centre[1]) < 1e-6, 'k3 a letter more ("Gallia"): drawn at once at the same place', J(f2 && f2.centre));
  // the same plate with the keep-out on its BACK: the back's staged words are placed exactly the same way (no face of its own)
  const charm2 = plate('back'), b2 = fitFace(charm2, 'back', 'Galli'), q2 = Fit.quickPlace({ charm: charm2, face: 'back', lines: ['Galli'], mask: b2.mask, opts: { minCapMm: 1.6, maxHeightFrac: 0.4, lineGap: 0.216, minStrokeMm: 0, minGapMm: 0, measure: false } }, { Regular: font }, G);
  ok(!!b2.fit && q2.status === 'ready' && Math.hypot(b2.fit.centre[0] - q2.fit.centre[0], b2.fit.centre[1] - q2.fit.centre[1]) < 1e-6, 'k4 a BACK with no room in its middle: placed the same way (front and back alike)', J({ fit: !!b2.fit }));
  // words with room nowhere: said as a placed face's draft says it
  const shut = Object.assign(plate('front'), { frontKeepOut: [seg(rect(-PT, -PT, W + 2 * PT, H + 2 * PT), { layer: 'FRONT KEEP-OUT' })] }), big = fitFace(shut, 'front', 'Galli');   // (a front kept out everywhere)
  ok(!big.fit && big.rec.note === 'No room for these words' && /if \(!f\.ok\) \{ rec\.note = "No room for these words"; return rec; \}/.test(src), 'k5 words with room nowhere: "No room for these words" on the work area, the words a placed back says (draftCompute)', J({ fit: !!big.fit, note: big.rec.note }));
  ok(/backHost\.appendChild\(cv\); draftNote\(card, r\);/.test(src) && /\} draftNote\(card, r\); queueWorkRulers\(backHost\._ez\); \};/.test(src), 'k5 and the staged face\'s drawing puts that note on the work area (when it is drawn and at every letter)');

  // the options row under the work area (Centre, Lines, the cap height, Line spacing): the card opened on the BACK, the FRONT shown with words staged
  const barText = cut('    const svBar = (t, el) => {', '    /** A change from the bar on STAGED words');
  const ctlOf = () => { const n = sel => ({ disabled: true, title: '', value: '', hidden: true, textContent: '', style: { setProperty() {} }, querySelector: () => null, sel }); const c = { centre: n('centre'), linecount: n('linecount'), cap: n('cap'), grp: n('grp'), sp: n('sp') };
    return { c, querySelector: s => (/centre/.test(s) ? c.centre : /linecount/.test(s) ? c.linecount : /data-cap/.test(s) ? c.cap : /spacingControl/.test(s) ? c.grp : /spacing/.test(s) ? c.sp : null) }; };
  const barFor = (cardFace) => {
    const ctl = ctlOf(), card = { dataset: { svOwn: '0' }, querySelector: s => (s === '.ctl' ? ctl : null) }, jb = { key: 'L', face: cardFace, fit: { centre: [0, 0], size: 6 }, lines: ['Galli'], lineGap: null };
    const e2 = { card, job: jb, rowJob: jb, held: null, faceOf: j => j.face, capText: () => '1.20 mm', fitOpts: (j) => ({ lineGap: (j && j.lineGap) ?? 0.216 }), document: { activeElement: null }, DRAFTS: new Map() }, k2 = Object.keys(e2);
    const svBar = new Function(...k2, barText + '\nreturn svBar;')(...k2.map(x => e2[x]));
    return { svBar, card, ctl };
  };
  const css = (/\.rvItem\[data-sv-own=\\"0\\"\]([^ ]*) \.placeView>\.ctl>\*\{visibility:hidden\}/.exec(src) || [])[1];
  const shown = card => card.dataset.svOwn !== '0' || (css === ':not([data-sv-bar=\\"1\\"])' && card.dataset.svBar === '1');   // (svCss: the row is hidden while the area shows another face's drawing, unless the row is the staged words')
  const el = { _svRec: { fitNow: { glyphs: [{}], lines: ['Xy'], capMm: 1.2 }, lineMode: null, lineGap: null } };
  { const x = barFor('back'); x.svBar({ kind: 'view', face: 'front', words: ['Xy'] }, el);
    ok(x.card._svBarOn === true && !x.ctl.c.centre.disabled && !x.ctl.c.linecount.disabled && shown(x.card), 'k6 the card opened on the back, the FRONT shown with "Xy" staged: the options under the work area are there, live, and are the front\'s words\' (as the back\'s are)', J({ on: x.card._svBarOn, centre: x.ctl.c.centre.disabled, data: x.card.dataset, css }));
    x.svBar({ kind: 'job', face: 'back', ja: null }, null);
    ok(x.card._svBarOn === false && x.card.dataset.svBar === '0', 'k7 the back\'s own drawing again: the row is the back job\'s again (never left acting on the front\'s staged words)', J(x.card.dataset)); }
  { const x = barFor('front'); x.svBar({ kind: 'view', face: 'back', words: ['Xy'] }, el);
    ok(x.card._svBarOn === true && shown(x.card), 'k8 and the other way round: a card opened on the front, the BACK shown with words staged: the same options', J({ on: x.card._svBarOn, data: x.card.dataset })); }
  ok(/try \{ svBar\(tgt, bc\); \} catch/.test(src) && /try \{ svBar\(null, el\); \} catch/.test(src), 'k9 every drawing of the work area that is not staged words lets the row go (mountBack, svInPlace)');
  // the cache token: the page loads the changed files
  const html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8');
  ok(/charm-nest-engrave-panel\.js\?v=[^"]*-ef1(?:-\w+)*"/.test(html) && /charm-nest-bridge\.js\?v=[^"]*-ef1(?:-\w+)*"/.test(html), 'k10 charm-nest-1.html loads the new charm-nest-engrave-panel.js and charm-nest-bridge.js (token -ef1)');
}
