// ENGSETUP (front-back-1011): the line's engraving-setup editor (charm-nest-engrave-setup.js, SURFACES row 6). Offline: jsdom, the real face model, a stand-in for the page's jobs. No browser,
// no network, no paid reading, no Firestore.
//
// What it proves
//   1  the reading: order 4195966039 (Charm Type "Tag1 (front+back)", Ace, "front - W / back - 1. 27. 24") shows Front + back, W, 1. 27. 24, Ace, the system's why line and its source and
//      confidence; the same from TAGSKU's tag option alone (the intake has not written spec.engraving yet); a back-only line is ONE quiet line and nothing else
//   2  mount: draws into a slot, Edit opens it inline (no dialog), every mode button (None / Front only / Back only / Front + back) shows the right boxes, Cancel puts it away
//   3  edits: words, a font per face (default: the line's font), the patch carries only what was touched, Save is off until something changed
//   4  the override: by = the signed-in name (never typed), kept from the previous one, a face the mode drops is dropped; JSON round trip; the system's reading comes back on Undo
//   5  the save path (apply): store -> ensureJobs -> reconcile; an UNDECIDED face job is removed, a DECIDED one is cut plain (never deleted) and says so first (a second press);
//      nothing is touched while the front jobs cannot be made; mode none ends undecided jobs; a face asked for again is opened again; the name put away changes nothing
//   6  waits and failures: a small labelled spinner while saving, one plain line on failure with the edit kept; a card drawn again gets the staged edit back
//   7  an unlabelled "both" line shows the buyer's raw words, two empty boxes and the prompt; nothing is assumed
//   8  locks: a decided face's words are locked; pieces with different words are told "differs per piece"
//   node tests/charm-nest/engrave-setup.cjs
'use strict';
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
let JSDOM; for (const p of ['jsdom', '/tmp/jsdom-mod/node_modules/jsdom', '/tmp/ps1-jsdom/node_modules/jsdom', '/tmp/ENGSIZE/nm/node_modules/jsdom']) { try { ({ JSDOM } = require(p)); break; } catch (_) { /* next */ } }
if (!JSDOM) { console.log('engrave-setup: jsdom is not installed here, skipped'); process.exit(0); }
const F = require(path.join(root, 'charm-nest-engrave-faces.js'));
const S = require(path.join(root, 'charm-nest-engrave-setup.js'));
let n = 0;
const ok = (msg, cond) => { assert.ok(cond, msg); n++; };
const eq = (msg, a, b) => { assert.deepEqual(a, b, msg); n++; };
const tick = () => new Promise(r => setImmediate(r));
const dom = new JSDOM('<!doctype html><html><head></head><body><div id="slot"></div></body></html>', { pretendToBeVisual: true });
const doc = dom.window.document, W = dom.window;
const fresh = () => { const s = doc.createElement('div'); doc.body.replaceChildren(s); return s; };
const reset = () => { S.DRAFTS.clear(); S.NOTES.clear(); };
const text = el => el.textContent.replace(/\s+/g, ' ').trim();
const fire = (el, type) => el.dispatchEvent(new W.Event(type, { bubbles: true }));
const type = (el, value) => { el.value = value; fire(el, 'input'); };

const LK = '4195966039_5009123456789';
const WHY = 'Charm Type “Tag1 (front+back)” asks for both faces; the note is labelled front - W / back - 1. 27. 24';
const reading = (x = {}) => Object.assign({ v: 1, mode: 'both', source: 'option', front: { text: 'W', lines: ['W'], fontWord: null }, back: { text: '1. 27. 24', lines: ['1. 27. 24'], fontWord: null }, textSource: 'labelled', needsPerson: false, confidence: 0.95, why: WHY, option: { name: 'Charm Type', value: 'Tag1 (front+back)' } }, x);
const mkLine = (o = {}) => ({ key: LK, state: 'pooled', order: { receiptId: '4195966039' }, line: { transactionId: '5009123456789' }, poolIds: [LK + '_1'],
  spec: Object.assign({ designSku: '5X8.5', engraveCandidate: true, personalization: ['front - W', 'back - 1. 27. 24'], font: { asked: 'Ace', id: 'jost', name: 'Jost', source: 'option' },
    tagOption: { name: 'Charm Type', value: 'Tag1 (front+back)', tag: 1, mode: 'both', shape: 'plain rounded rectangle', sku: '5X8.5', how: 'rule:tag-shape' }, engraving: reading() }, o.spec || {}), ...(o.row || {}) });
const job = (face, state, lines, extra = {}) => Object.assign({ key: F.jobKey(LK, null, face), face, slot: null, state, lines: lines.slice(), text: lines.join('\n') }, extra);

(async () => {
  /* 1 ── the reading */
  { reset();
    const line = mkLine(), vm = S.read(line, { jobs: [] });
    eq('mode', vm.mode, 'both'); eq('label', vm.modeLabel, 'Front + back');
    eq('front words', vm.rows[0].text, 'W'); eq('back words', vm.rows[1].text, '1. 27. 24');
    eq('the line font is Ace', vm.rows[0].font.word, 'Ace'); eq('installed font', vm.rows[0].font.name, 'Jost');
    ok('why is the system\'s own line', vm.why === WHY);
    ok('source and confidence', /Charm Type option/.test(vm.from) && /95% sure/.test(vm.from));
    const slot = fresh(); S.mount(slot, line, { jobs: () => [] });
    const t = text(slot);
    ok('shows Front + back', /Engraves\s*Front \+ back/.test(t)); ok('front W', /Front W/.test(t)); ok('back words', /Back 1\. 27\. 24/.test(t)); ok('Ace', /Ace/.test(t));
    ok('the why line is on the card', t.includes(WHY)); ok('from the option, 95% sure', /from the Charm Type option/.test(t) && /95% sure/.test(t));
    ok('no dialog anywhere', !doc.querySelector('dialog,[role=dialog]'));
    // the intake has not written spec.engraving yet: TAGSKU's tag option and the labelled note give the same reading
    const fb = mkLine({ spec: { engraving: undefined } }); delete fb.spec.engraving;
    const v2 = S.read(fb, { jobs: [] });
    eq('fallback mode', v2.mode, 'both'); eq('fallback front', v2.rows[0].text, 'W'); eq('fallback back', v2.rows[1].text, '1. 27. 24'); eq('fallback font', v2.rows[0].font.word, 'Ace');
    ok('fallback names the option', /Charm Type/.test(v2.why) && /front - W/.test(v2.why) && /option/.test(v2.from));
    // a side row (job.row) reads the line
    const side = Object.create(line); Object.defineProperty(side, 'parentRow', { value: line });
    eq('a side row reads its line', S.read(side, { jobs: [] }).mode, 'both');
    // a back-only line: one quiet line, no face rows, no why
    const old = { key: 'r_t', state: 'pooled', spec: { engraveCandidate: true, personalization: ['Anna'] }, poolIds: [] };
    const s2 = fresh(); S.mount(s2, old, { jobs: () => [job('back', 'review', ['Anna'])] });
    eq('back-only is one line', text(s2), 'Engraves Back only Edit nothing in the listing or the note says otherwise');
    ok('back-only draws no face rows', !s2.querySelector('.egSuFace'));
    // a line with nothing to engrave draws nothing
    const plain = { key: 'p_t', state: 'pooled', spec: { engraveCandidate: false, personalization: [] }, poolIds: [] };
    const s3 = fresh(); S.mount(s3, plain, { jobs: () => [] }); eq('nothing to say', s3.innerHTML, ''); ok('slot hidden', s3.hidden);
  }

  /* 2 ── mount, Edit, every mode, Cancel */
  { reset();
    const line = mkLine(), saved = [], slot = fresh();
    S.mount(slot, line, { jobs: () => [], save: async p => { saved.push(p); return { ok: true }; } });
    slot.querySelector('[data-su=edit]').click();
    ok('opens inline', slot.querySelector('[data-open="1"]') && slot.querySelector('textarea[data-face=front]') && slot.querySelector('textarea[data-face=back]'));
    eq('four modes', [...slot.querySelectorAll('[data-su=mode]')].map(b => text(b)), ['None', 'Front only', 'Back only', 'Front + back']);
    eq('Front + back pressed', slot.querySelector('[aria-pressed=true]').dataset.mode, 'both');
    ok('save is off before a change', slot.querySelector('[data-su=save]').disabled);
    const boxes = () => [...slot.querySelectorAll('textarea[data-su=words]')].map(t => t.dataset.face);
    for (const [mode, faces] of [['none', []], ['front', ['front']], ['back', ['back']], ['both', ['front', 'back']]]) {
      slot.querySelector(`[data-mode=${mode}]`).click();
      eq('boxes for ' + mode, boxes(), faces); eq('pressed ' + mode, slot.querySelector('[aria-pressed=true]').dataset.mode, mode);
    }
    slot.querySelector('[data-mode=front]').click();
    eq('front keeps its words', slot.querySelector('textarea[data-face=front]').value, 'W');
    ok('save on after a mode change', !slot.querySelector('[data-su=save]').disabled);
    slot.querySelector('[data-su=cancel]').click();
    ok('cancel closes it', slot.querySelector('[data-open="0"]') && !slot.querySelector('textarea')); eq('nothing saved', saved.length, 0);
    // the staged edit of a cancelled card is gone: it opens on the reading again
    slot.querySelector('[data-su=edit]').click(); eq('opens on the reading', slot.querySelector('[aria-pressed=true]').dataset.mode, 'both');
  }

  /* 3 ── words, fonts, the patch */
  { reset();
    const line = mkLine(), saved = [], slot = fresh();
    S.mount(slot, line, { jobs: () => [], save: async p => { saved.push(p); line.engravingOverride = S.buildOverride(line, p, 'Ana_M', 5); return { ok: true }; } });
    slot.querySelector('[data-su=edit]').click();
    const front = () => slot.querySelector('textarea[data-face=front]'), back = () => slot.querySelector('textarea[data-face=back]');
    type(front(), 'W.'); ok('save on after words', !slot.querySelector('[data-su=save]').disabled);
    type(front(), 'W'); ok('save off again when the words are as they were', slot.querySelector('[data-su=save]').disabled);
    type(front(), 'W.');
    const sel = slot.querySelector('select[data-face=back]');
    ok('first font option is the line\'s font', /The line.s font \(Ace → Jost\)/.test(sel.options[0].text));
    ok('the shop\'s fonts are offered', ['Stylish', 'Pristina', 'Angelina', 'Typewriter', 'Comic', 'Vibur', 'Ace', 'Britannic Bold'].every(w => [...sel.options].some(o => o.value === w)));
    sel.value = 'Vibur'; fire(sel, 'change');
    slot.querySelector('[data-su=save]').click(); await tick();
    eq('one save', saved.length, 1);
    eq('the patch holds only what was touched', saved[0], { front: { text: 'W.', fontWord: null }, back: { text: '1. 27. 24', fontWord: 'Vibur' } });
    // saved: the card is closed and tells the edit
    const t = text(slot);
    ok('closed again', slot.querySelector('[data-open="0"]')); ok('says it was edited by the login', /Set by Ana_M/.test(t));
    ok('the system\'s reading is kept beside', /The system read: Front: W · Back: 1\. 27\. 24/.test(t)); ok('Undo my edit is offered', !!slot.querySelector('[data-su=undo]'));
    ok('the new words and font show', /Front W\./.test(t) && /Vibur/.test(t));
    eq('the face font', S.read(line, { jobs: [] }).rows[1].fontWord, 'Vibur');
  }

  /* 4 ── the override */
  { reset();
    const line = mkLine();
    const ov = S.buildOverride(line, { mode: 'both', front: { text: 'W', fontWord: null } }, 'Ana_M', 1760190000000);
    eq('override shape', ov, { mode: 'both', front: { text: 'W', lines: ['W'], fontWord: null }, by: 'Ana_M', at: 1760190000000 });
    line.engravingOverride = ov;
    const ov2 = S.buildOverride(line, { back: { text: ' 2 \n 7 ', fontWord: 'Ace' } }, 'Ben_K', 2);
    eq('merged with the previous override', ov2, { mode: 'both', front: { text: 'W', lines: ['W'], fontWord: null }, back: { text: '2\n7', lines: ['2', '7'], fontWord: 'Ace' }, by: 'Ben_K', at: 2 });
    eq('a face the mode drops is dropped', S.buildOverride({ engravingOverride: ov2 }, { mode: 'back' }, 'x', 3), { mode: 'back', back: ov2.back, by: 'x', at: 3 });
    eq('survives JSON', JSON.parse(JSON.stringify(ov2)), ov2);
    const set = F.setupOf({ spec: line.spec }, ov2);
    eq('wins over the reading', [set.mode, set.source, set.edited, set.back.lines], ['both', 'person', true, ['2', '7']]);
    ok('by is the login, set.by', set.by === 'Ben_K');
    delete line.engravingOverride;
    eq('the reading comes back', S.read(line, { jobs: [] }).rows.map(r => r.text), ['W', '1. 27. 24']);
  }

  /* 5 ── the save path */
  const faceEngine = (l, jobs) => { const setup = S.setupFor(l); for (const p of F.planFaces(setup, [null])) if (setup.faces.length && !jobs.some(j => F.faceOf(j) === p.face && !j.slot)) jobs.push(job(p.face, 'classify', [])); };
  const fakeEnv = (jobs, o = {}) => {
    const log = [];
    const env = { log, who: () => (o.who === undefined ? 'Ana_M' : o.who), now: () => 1760190000000,
      store: (l, ov) => { log.push(['store', ov ? ov.mode : null]); if (ov) l.engravingOverride = ov; else delete l.engravingOverride; },
      jobsOf: () => jobs.slice(), ensureJobs: l => { log.push(['ensure']); if (o.engine !== false) faceEngine(l, jobs); return jobs; }, slots: () => [null],
      remove: j => { log.push(['remove', j.key]); jobs.splice(jobs.indexOf(j), 1); }, skip: async (j, by) => { log.push(['skip', j.key, by]); j.state = 'skipped'; },
      setNone: (j) => { log.push(['none', j.key]); j.state = 'none'; }, reopen: j => { log.push(['reopen', j.key]); j.state = 'words'; },
      seed: async (j, seed) => { log.push(['seed', j.key, seed.lines.join('/')]); j.lines = seed.lines.slice(); j.text = seed.text; }, changed: () => log.push(['changed']) };
    return env;
  };
  { reset();
    // back + front approved/undecided: both -> front only: the undecided back is removed
    const line = mkLine({ spec: { engraving: reading({ source: 'default' }) } }); const jobs = [job('front', 'review', ['W']), job('back', 'review', ['1. 27. 24'])];
    const env = fakeEnv(jobs), res = await S.apply(line, { mode: 'front' }, env);
    ok('ok', res.ok); eq('removed the undecided back', res.removed, [F.jobKey(LK, null, 'back')]); eq('no plain', res.plain, []);
    eq('jobs left', jobs.map(j => j.face), ['front']); eq('by is the login', line.engravingOverride.by, 'Ana_M'); eq('order of work', env.log.map(x => x[0]), ['store', 'ensure', 'remove', 'changed']);
  }
  { reset();
    // the back is DECIDED (approved): it is never deleted, it becomes cut plain
    const line = mkLine(); const jobs = [job('front', 'review', ['W']), job('back', 'approved', ['1. 27. 24'], { approvedBy: 'Cy' })];
    const vm = S.read(line, { jobs }); ok('the back is locked', vm.rows[1].locked && vm.rows[1].lockBy === 'Cy');
    const cons = S.consequences(line, jobs, F.setupOf({ spec: line.spec }, { mode: 'front' }), vm.setup);
    eq('says what will happen', [cons.plain.length, cons.remove.length], [1, 0]); ok('plain words', /already approved by Cy: saving cuts it plain/.test(cons.lines[0]));
    const env = fakeEnv(jobs), res = await S.apply(line, { mode: 'front' }, env);
    eq('cut plain, not removed', [res.plain, res.removed], [[F.jobKey(LK, null, 'back')], []]); eq('the job is still there', jobs.length, 2); eq('as skipped', jobs.find(j => j.face === 'back').state, 'skipped');
    ok('by the signed-in name', env.log.some(x => x[0] === 'skip' && x[2] === 'Ana_M'));
    // the card asks first (inline, a second press)
    const slot = fresh(); line.engravingOverride = undefined; delete line.engravingOverride; const saved = [];
    S.mount(slot, line, { jobs: () => [job('front', 'review', ['W']), job('back', 'approved', ['1. 27. 24'], { approvedBy: 'Cy' })], save: async p => { saved.push(p); return { ok: true }; } });
    slot.querySelector('[data-su=edit]').click();
    ok('the decided back\'s words are locked', slot.querySelector('textarea[data-face=back]').disabled && /approved by Cy: open it in Engraving to change it/.test(text(slot)));
    slot.querySelector('[data-mode=front]').click(); slot.querySelector('[data-su=save]').click();
    eq('first press only asks', saved.length, 0); ok('the question is a plain line in the card', /already approved by Cy/.test(text(slot)) && !doc.querySelector('dialog'));
    ok('keep it as it is puts it away', (slot.querySelector('[data-su=keep]').click(), !slot.querySelector('[data-su=saveYes]')));
    slot.querySelector('[data-su=save]').click(); slot.querySelector('[data-su=saveYes]').click(); await tick();
    eq('second press saves', saved, [{ mode: 'front' }]);
  }
  { reset();
    // the front engine is not in the page yet: the choice is kept, no job is removed or cut
    const line = mkLine({ spec: { engraving: reading({ mode: 'back', source: 'default', front: null }) } }); const jobs = [job('back', 'review', ['1. 27. 24'])];
    const env = fakeEnv(jobs, { engine: false }), res = await S.apply(line, { mode: 'front', front: { text: 'W' } }, env);
    ok('saved', res.ok && res.pending); eq('asked for front', res.added, ['front']); eq('the back job is untouched', jobs.map(j => [j.face, j.state]), [['back', 'review']]);
    eq('no remove / skip', env.log.filter(x => ['remove', 'skip', 'none'].includes(x[0])), []);
    ok('the override is kept', line.engravingOverride.mode === 'front');
    // ... a face that already has an undecided job still takes the person's words while the new face waits (nothing is removed or cut)
    const l3 = mkLine({ spec: { engraving: reading({ mode: 'back', source: 'default', front: null }) } }), j3 = [job('back', 'review', ['1. 27. 24'])], e3 = fakeEnv(j3, { engine: false });
    const r3 = await S.apply(l3, { mode: 'both', front: { text: 'W' }, back: { text: '2. 3. 25' } }, e3);
    ok('pending and seeded', r3.ok && r3.pending && r3.seeded.length === 1 && e3.log.some(x => x[0] === 'seed') && !e3.log.some(x => ['remove', 'skip', 'none'].includes(x[0])));
    // ... and a back-only line stays inert when the choice is "back only"
    const l2 = mkLine({ spec: { engraving: reading({ mode: 'back', source: 'default', front: null }) } }), j2 = [job('back', 'approved', ['1. 27. 24'])], e2 = fakeEnv(j2, { engine: false });
    const r2 = await S.apply(l2, { mode: 'back' }, e2); ok('inert', r2.ok && !r2.pending && j2[0].state === 'approved' && !e2.log.some(x => ['remove', 'skip', 'none', 'seed'].includes(x[0])));
  }
  { reset();
    // mode none: undecided -> setNone, decided -> cut plain
    const line = mkLine(); const jobs = [job('front', 'review', ['W']), job('back', 'written', ['1. 27. 24'], { approvedBy: 'Cy' })];
    const env = fakeEnv(jobs), res = await S.apply(line, { mode: 'none' }, env);
    eq('none', [res.none, res.plain], [[F.jobKey(LK, null, 'front')], [F.jobKey(LK, null, 'back')]]); eq('states', jobs.map(j => j.state), ['none', 'skipped']);
    // and asked for again: the 'none' job is opened again, the cut-plain one that is still not asked for stays
    const r2 = await S.apply(line, { mode: 'front', front: { text: 'W' } }, env);
    eq('front reopened', r2.reopened, [F.jobKey(LK, null, 'front')]); eq('back stays cut plain', jobs.find(j => j.face === 'back').state, 'skipped');
    eq('the reopened face takes the person\'s words', jobs.find(j => j.face === 'front').lines, ['W']);
    // a cut-plain face that was asked for all along is a decision, not reopened by an unrelated save
    const r3 = await S.apply(line, { front: { text: 'W2' } }, env); eq('no reopen', r3.reopened, []);
  }
  { reset();
    // the name is put away: nothing happens; a line already cut keeps its decided face
    const line = mkLine(), jobs = [job('front', 'review', ['W'])], env = fakeEnv(jobs, { who: '' }), res = await S.apply(line, { mode: 'back' }, env);
    ok('cancelled', res.cancelled && !res.ok); ok('nothing stored', !line.engravingOverride && env.log.length === 0);
    const cut = mkLine({ row: { state: 'written' } }), j3 = [job('front', 'written', ['W']), job('back', 'written', ['1. 27. 24'])], r3 = await S.apply(cut, { mode: 'front' }, fakeEnv(j3));
    ok('a cut line keeps its engraving', !r3.ok && /already cut/.test(r3.message) && !cut.engravingOverride && j3.every(j => j.state === 'written'));
    const gone = mkLine({ row: { state: 'gone' } }); ok('a line that left the pull is not changed', !(await S.apply(gone, { mode: 'back' }, fakeEnv([]))).ok);
  }
  { reset();
    // undo: the system's reading comes back; undecided jobs follow the reading
    const line = mkLine({ row: { engravingOverride: { mode: 'front', front: { text: 'W', lines: ['W'], fontWord: null }, by: 'Ana_M', at: 1 } } }); const jobs = [job('front', 'review', ['W'])];
    const env = fakeEnv(jobs), res = await S.apply(line, null, env, { undo: true });
    ok('undone', res.ok && !line.engravingOverride); eq('the back is made again (asked for by the reading)', jobs.map(j => j.face), ['front', 'back']);
  }
  { reset();
    // an undecided job takes the person's new words (no paid reading); a job still classifying is left to the pipeline
    const line = mkLine({ spec: { engraving: reading({ source: 'default', mode: 'back', front: null }) } }); const jobs = [job('back', 'review', ['old'])];
    const env = fakeEnv(jobs), res = await S.apply(line, { back: { text: 'new' } }, env);
    eq('seeded', res.seeded, [F.jobKey(LK, null, 'back')]); eq('job words', jobs[0].lines, ['new']);
    const j2 = [job('back', 'classify', [])], r2 = await S.apply(mkLine(), { mode: 'back', back: { text: 'x' } }, fakeEnv(j2)); eq('classify is left alone', r2.seeded, []);
  }

  /* 6 ── waits, failures, a card drawn again */
  { reset();
    const line = mkLine(); let release; const slot = fresh();
    const opts = { jobs: () => [], save: () => new Promise(r => { release = r; }) };
    S.mount(slot, line, opts); slot.querySelector('[data-su=edit]').click();
    type(slot.querySelector('textarea[data-face=front]'), 'X'); slot.querySelector('[data-su=save]').click();
    ok('a small labelled spinner', slot.querySelector('.owSpin') && /Saving/.test(text(slot))); ok('the controls wait', slot.querySelector('[data-su=save]').disabled && slot.querySelector('textarea').disabled);
    release({ ok: false, message: 'The cloud did not answer. Try again.' }); await tick();
    ok('one plain line on failure', /The cloud did not answer\. Try again\./.test(slot.querySelector('[role=alert]').textContent) && !slot.querySelector('.owSpin'));
    eq('the edit is kept', slot.querySelector('textarea[data-face=front]').value, 'X'); ok('save is on again', !slot.querySelector('[data-su=save]').disabled);
    // the host draws its card again: a new slot gets the staged edit back, still open
    const slot2 = fresh(); S.mount(slot2, line, opts);
    ok('staged edit comes back', slot2.querySelector('textarea[data-face=front]') && slot2.querySelector('textarea[data-face=front]').value === 'X');
    // a save that throws is the same plain line
    S.DRAFTS.clear(); const slot3 = fresh(); S.mount(slot3, line, { jobs: () => [], save: async () => { throw new Error('boom'); } });
    slot3.querySelector('[data-su=edit]').click(); slot3.querySelector('[data-mode=back]').click(); slot3.querySelector('[data-su=save]').click(); await tick();
    ok('a throw is one plain line', /Not saved: boom/.test(slot3.querySelector('[role=alert]').textContent));
    // the event hosts may listen to
    S.DRAFTS.clear(); const slot4 = fresh(); let heard = null; slot4.addEventListener('cn-engrave-setup', e => { heard = e.detail; });
    S.mount(slot4, line, { jobs: () => [], save: async () => ({ ok: true, plain: [] }) }); slot4.querySelector('[data-su=edit]').click(); slot4.querySelector('[data-mode=back]').click(); slot4.querySelector('[data-su=save]').click(); await tick();
    ok('a saved edit is announced', heard && heard.key === LK);
    // the host draws its card again WHILE the save waits (the bridge redraws the cards inside a save): the new slot is told when it ends, the spinner does not stay
    S.DRAFTS.clear(); S.NOTES.clear(); { const slotA = fresh(); let go; let heardB = null;
      const o6 = { jobs: () => [], save: () => new Promise(r => { go = r; }) };
      S.mount(slotA, line, o6); slotA.querySelector('[data-su=edit]').click(); slotA.querySelector('[data-mode=back]').click(); slotA.querySelector('[data-su=save]').click();
      const slotB = fresh(); slotB.addEventListener('cn-engrave-setup', e => { heardB = e.detail; }); S.mount(slotB, line, o6); slotA.remove();
      ok('the new slot shows the wait too', slotB.querySelector('.owSpin') && /Saving/.test(text(slotB)));
      line.engravingOverride = S.buildOverride(line, { mode: 'back' }, 'Ana_M', 9); go({ ok: true, plain: [] }); await tick();
      ok('and is told it ended: no spinner, the plain Saved line, Undo on offer', !slotB.querySelector('.owSpin') && /Saved/.test(text(slotB)) && slotB.querySelector('[data-su=undo]'));
      ok('the announcement reaches the slot that is on screen', heardB && heardB.key === LK);
      delete line.engravingOverride; S.DRAFTS.clear(); S.NOTES.clear(); }
    // the name put away: calm, nothing said
    S.DRAFTS.clear(); const slot5 = fresh(); S.mount(slot5, line, { jobs: () => [], save: async () => ({ ok: false, cancelled: true }) });
    slot5.querySelector('[data-su=edit]').click(); slot5.querySelector('[data-mode=back]').click(); slot5.querySelector('[data-su=save]').click(); await tick();
    ok('put away is not an error', !slot5.querySelector('[role=alert]') && !slot5.querySelector('.owSpin') && slot5.querySelector('textarea'));
  }

  /* 7 ── unlabelled both: nothing assumed */
  { reset();
    S.DRAFTS.clear(); S.NOTES.clear();
    const line = mkLine({ spec: { personalization: ['W 1. 27. 24'], engraving: reading({ front: null, back: null, textSource: 'unlabelled', needsPerson: true, confidence: 0.5, why: 'The note does not say which words go on the front and which on the back' }) } });
    const slot = fresh(); S.mount(slot, line, { jobs: () => [job('front', 'words', []), job('back', 'words', [])], save: async p => { line.engravingOverride = S.buildOverride(line, p, 'Ana_M', 9); return { ok: true }; } });
    ok('opens by itself', !!slot.querySelector('textarea[data-face=front]'));
    eq('two EMPTY boxes', [...slot.querySelectorAll('textarea')].map(t => t.value), ['', '']);
    ok('the buyer\'s raw words', slot.querySelector('.egSuRaw').textContent === 'W 1. 27. 24');
    ok('the plain prompt', /Say which words go on the front and which on the back/.test(text(slot)));
    ok('the save waits for a person', slot.querySelector('[data-su=save]').disabled);
    type(slot.querySelector('textarea[data-face=front]'), 'W'); type(slot.querySelector('textarea[data-face=back]'), '1. 27. 24');
    slot.querySelector('[data-su=save]').click(); await tick();
    const after = S.read(line, { jobs: [] }); eq('the person said', after.rows.map(r => r.text), ['W', '1. 27. 24']); ok('no longer needs a person', !after.needsPerson);
  }
  { reset();
    // the same line from TAGSKU's option alone (no spec.engraving yet)
    S.DRAFTS.clear();
    const line = mkLine({ spec: { personalization: ['W 1. 27. 24'] } }); delete line.spec.engraving;
    const vm = S.read(line, { jobs: [] }); ok('needs a person', vm.needsPerson && vm.mode === 'both' && vm.rows.every(r => r.text === '')); ok('raw words', vm.raw === 'W 1. 27. 24');
  }
  { reset();
    // switching to a single face: the labelled words follow; a face never gets the other's words when the note labels them
    S.DRAFTS.clear();
    const line = mkLine(), slot = fresh(); S.mount(slot, line, { jobs: () => [], save: async () => ({ ok: true }) }); slot.querySelector('[data-su=edit]').click();
    slot.querySelector('[data-mode=back]').click(); eq('back keeps its labelled words', slot.querySelector('textarea[data-face=back]').value, '1. 27. 24');
    // back only -> both on a line whose reading has no front: the front box is seeded from the buyer's labelled words
    S.DRAFTS.clear();
    const l2 = mkLine({ spec: { engraving: reading({ mode: 'back', front: null, source: 'default' }) } }), s2 = fresh(); S.mount(s2, l2, { jobs: () => [], save: async () => ({ ok: true }) });
    s2.querySelector('[data-su=edit]').click(); s2.querySelector('[data-mode=both]').click();
    eq('front reseeded from the labelled note', s2.querySelector('textarea[data-face=front]').value, 'W'); eq('back stays', s2.querySelector('textarea[data-face=back]').value, '1. 27. 24');
    // an unlabelled one-face note moves with the mode
    S.DRAFTS.clear();
    const l3 = { key: 'z_1', state: 'pooled', spec: { engraveCandidate: true, personalization: ['Anna'], engraving: reading({ mode: 'back', front: null, back: { text: 'Anna', lines: ['Anna'], fontWord: null }, source: 'default', textSource: 'single', why: '' }) }, poolIds: [] }, s3 = fresh();
    S.mount(s3, l3, { jobs: () => [], save: async () => ({ ok: true }) }); s3.querySelector('[data-su=edit]').click(); s3.querySelector('[data-mode=front]').click();
    eq('the single note follows to the front', s3.querySelector('textarea[data-face=front]').value, 'Anna');
    s3.querySelector('[data-mode=both]').click(); eq('but with both faces the note is not copied onto the new one', [s3.querySelector('textarea[data-face=front]').value, s3.querySelector('textarea[data-face=back]').value], ['', 'Anna']);
  }

  /* 8 ── pieces */
  { reset();
    S.DRAFTS.clear();
    const line = mkLine({ spec: { engraving: reading() } }), jobs = [job('back', 'review', ['A'], { slot: 'L', key: F.jobKey(LK, 'L', 'back') }), job('back', 'review', ['B'], { slot: 'R', key: F.jobKey(LK, 'R', 'back') }), job('front', 'review', ['W'], { slot: 'L', key: F.jobKey(LK, 'L', 'front') }), job('front', 'review', ['W'], { slot: 'R', key: F.jobKey(LK, 'R', 'front') })];
    const vm = S.read(line, { jobs }); eq('back words differ per piece', vm.rows.map(r => r.perPiece), [false, true]); eq('front is shared', vm.rows[0].text, 'W');
    const slot = fresh(); S.mount(slot, line, { jobs: () => jobs }); ok('view says so', /differs per piece/.test(text(slot)));
    slot.querySelector('[data-su=edit]').click(); ok('no words box for the back', !slot.querySelector('textarea[data-face=back]') && /change them on each piece/.test(text(slot)) && !!slot.querySelector('select[data-face=back]'));
  }
  console.log(`engrave-setup: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
