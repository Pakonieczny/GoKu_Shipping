// ENGSTAGE · the stage, drawn (Paul, 30 Oct 2026: "every letter typed into the text box immediately updates itself in BOTH the Thumbnail image and in the main charm preview in the middle of the
// screen ... they should be able to toggle back and in between any of the four options and everything should be seamless ... tasteful quick and easy to understand, transitions and animations";
// "Also the hover states for everything here are inadequate").
// His case: order 4214051539, BALLETSHOES, "Ruby" on the Back; he pressed Front, then Approve, and the card showed "Words to confirm" under the Front thumbnail, an empty work area saying "The order
// gives no words for the front", an "Engrave these words" button. Here: a made-up back-only necklace with "Ruby" on its back (the real COMET drawing, the real fit), offline (_engraveRealWorld.cjs).
// Engrave.showStage(job, stage) is called directly (the panel's one call), stage = { key, choice, active, faces: { front: { text, font, pos }, back } }:
//   S1–S6  none / back / front (a face with NO job) / both, each active face: both thumbnails ([data-eng-thumb]) and the work area (.backHost[data-face][data-words][data-stage-seq]) draw the right face
//          and words; the work area is never empty, never a message ("Words to confirm", "gives no words", "being prepared", "Engrave these words")
//   K1     every keystroke ("R", "Ru", "Rub", "Ruby" on the front) is on the thumbnail AND the work area in the first animation frame after the call (performance.now + requestAnimationFrame)
//   R1–R3  30 rapid alternating calls (the face switched each time, some within one frame, some a few ms apart): the last state is drawn, no older state is ever painted after a newer one, no
//          backlog of turns or fades
//   W1–W2  the front waiting for words with no placement (Paul's state after Front + Approve): the work area draws the plain front of the charm, no reason, no "Engrave these words"
//   Z1     zoom and rulers (ENGVIEW) on a staged face with no job
//   H1–H2  the thumbnails' hover (border toward gold, a pixel's lift, a shadow) and keyboard focus (a 2 px gold ring, 2 px out)
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/engrave-stage-view.cjs      (SHOTS=<dir> for screenshots)
'use strict';
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const Q = '#egQueue .rvItem[data-kind=placement]';
const FORBIDDEN = /Words to confirm|gives no words|being prepared|Engrave these words|Preparing the|No (front|back) engraving/i;

(async () => {
  const R = require('./_engraveRealWorld.cjs');
  R.ORDERS.ruby = Object.assign({}, R.ORDERS.back, { rid: '4290000171', listing: '1890000171', buyer: 'Name Seventeen', personalization: ['Ruby'] }); R.EXPECT_JOBS.ruby = 1;
  R.ORDERS.ruby2 = Object.assign({}, R.ORDERS.back, { rid: '4290000172', listing: '1890000172', buyer: 'Name Eighteen', personalization: ['Ruby'] }); R.EXPECT_JOBS.ruby2 = 1;
  const shots = process.env.SHOTS || '/tmp/engrave-stage-view-shots';
  const w = await R.world({ viewport: { width: 1440, height: 900 }, shots });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page;
  await page.waitForFunction(() => !window.CNRole || ['ask', 'role', 'admin'].includes(CNRole.state()), null, { timeout: 20000 }).catch(() => {});
  await page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('design'); } catch (_) { /* no role step */ } });
  const until = async (fn, ms) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) return null; await new Promise(r => setTimeout(r, 200)); } };
  // what the card shows now: the thumbnails, the work area, its ink (dark pixels), every text on the card
  const look = () => page.evaluate(({ Q, FORBIDDEN }) => {
    const c = document.querySelector(Q); if (!c) return null;
    const host = c.querySelector('.backHost'), el = host && host.querySelector(':scope > canvas, :scope > .egStagePv'), r = el ? el.getBoundingClientRect() : null;
    const ink = cv => { try { const g = cv.getContext('2d'), d = g.getImageData(0, 0, cv.width, cv.height).data; let n = 0; for (let i = 0; i < d.length; i += 4) if (d[i] < 70 && d[i + 1] < 70 && d[i + 2] < 70 && d[i + 3] > 200) n++; return n; } catch (_) { return -1; } };
    const thumb = f => { const b = c.querySelector(`[data-eng-thumb="${f}"]`); if (!b) return null; const t = b.querySelector('.engpvText'), br = b.getBoundingClientRect(); return { cur: b.getAttribute('aria-current') === 'true', eng: b.dataset.engraved === '1', text: t && !t.hidden ? t.textContent : '', shown: br.width > 0 && br.height > 0, pic: !!b.querySelector('.engpvDraw canvas, .engpvDraw img') }; };
    const text = c.innerText.replace(/\s+/g, ' ');
    return { key: c.dataset.key, face: host && host.dataset.face, words: host && host.dataset.words, seq: host ? +host.dataset.stageSeq : -1, el: el ? (el.tagName === 'CANVAS' ? 'canvas' : 'pv') : null, size: r ? [Math.round(r.width), Math.round(r.height)] : null,
      ink: el && el.tagName === 'CANVAS' ? ink(el) : -1, viewFace: el ? el._viewFace || null : null, front: thumb('front'), back: thumb('back'), bad: (text.match(FORBIDDEN) || [])[0] || ((host && host.textContent.match(FORBIDDEN)) || [])[0] || '',
      own: c.dataset.svOwn, ctl: (() => { const b = c.querySelector('.placeView > .ctl > *'); return b ? getComputedStyle(b).visibility : 'none'; })(), jobs: Engrave.jobsOf(Engrave.items().get(c.dataset.key).row).map(j => [j.face || 'back', j.state, j.text]), use: c.querySelectorAll('[data-a=usewords]').length, why: c.querySelectorAll('.ctl .why, .backHost .noBack').length, turning: host ? host.getAnimations().length : 0 };
  }, { Q, FORBIDDEN: FORBIDDEN.source });
  // one stage, painted (the call, then two frames and the turn's time)
  const stage = (choice, active, front, back) => ({ choice, active, faces: { front: { text: front || '', font: '', pos: null }, back: { text: back || '', font: '', pos: null } } });
  const show = async (st, ms = 450) => { const r = await page.evaluate(([Q, st]) => { const c = document.querySelector(Q); return Engrave.showStage(Engrave.items().get(c.dataset.key), st ? Object.assign({ key: c.dataset.key.replace(/~front$/, '') }, st) : null); }, [Q, st]); await R.settle(page, ms); return r; };
  const low = s => String(s || '').toLowerCase();
  const fine = (L, msg) => !!L && !!L.el && L.size && L.size[0] > 100 && L.size[1] > 100 && !L.bad && !L.use && !L.why ? true : (console.log('   ', msg || '', JSON.stringify(L)), false);
  try {
    const made = await R.build(w, ['ruby', 'ruby2']);
    const rk = made.ruby.key;
    check('0 the world: "Ruby" on the back of a back-only charm, fitted; no front job', made.ruby.jobs.length === 1 && made.ruby.jobs[0].face === 'back' && made.ruby.jobs[0].fitted, JSON.stringify(made.ruby.jobs));
    await R.openEngraveDetail(w, 'ruby', { face: 'back' }); await R.settle(page, 900);
    const L0 = await look();
    check('V1 committed: both thumbnails are drawn (a piece that can have both), the Back engraved and edited, the Front the plain charm; no state caption', !!L0 && L0.front && L0.back && L0.front.shown && L0.back.shown && L0.front.pic && L0.back.pic && L0.back.eng && !L0.front.eng && L0.back.cur && !L0.front.cur && !L0.bad && /ruby/i.test(L0.back.text) && !L0.front.text, JSON.stringify(L0));
    check('V2 committed: the work area draws the back with its words (data-face back, data-words Ruby)', fine(L0) && L0.face === 'back' && low(L0.words) === 'ruby' && L0.el === 'canvas', JSON.stringify(L0));

    // ═══ S · the four choices and the active face, on a piece with ONLY a back job ═══
    await show(stage('none', 'back', '', 'Ruby'));
    const Snone = await look();
    check('S1 None, active back: both thumbnails plain (no words), the work area the plain back (no words), drawn', fine(Snone) && !Snone.front.eng && !Snone.back.eng && !Snone.front.text && !Snone.back.text && Snone.face === 'back' && Snone.words === '' && Snone.ink > 0 && Snone.back.cur, JSON.stringify(Snone));
    await show(stage('back', 'back', '', 'Ruby'));
    const Sback = await look();
    check('S2 Back "Ruby", active back: the back thumbnail engraved with Ruby, the front plain; the work area the back with Ruby (more ink than plain)', fine(Sback) && Sback.back.eng && /ruby/i.test(Sback.back.text) && !Sback.front.eng && !Sback.front.text && Sback.face === 'back' && low(Sback.words) === 'ruby' && Sback.ink > Snone.ink + 20, JSON.stringify({ Sback, plain: Snone.ink }));
    await show(stage('front', 'front', '', 'Ruby'));
    const SfrontPlain = await look();
    check('S3 Front with no words yet, active front (the front has NO job): the work area turned to the front of the charm, plain; the back thumbnail plain; no message', fine(SfrontPlain) && SfrontPlain.face === 'front' && SfrontPlain.words === '' && SfrontPlain.viewFace === 'front' && SfrontPlain.front.cur && SfrontPlain.front.eng && !SfrontPlain.back.eng && !SfrontPlain.back.text && SfrontPlain.ink > 0, JSON.stringify(SfrontPlain));
    await show(stage('front', 'front', 'Ann', 'Ruby'));
    const Sfront = await look();
    await R.shot(w, 'STAGE-front-Ann');
    check('S4 Front "Ann" (no front job): the front thumbnail shows Ann, the work area the front of the charm with Ann fitted on it (more ink than the plain front); the back\'s own controls are hidden meanwhile', fine(Sfront) && Sfront.face === 'front' && low(Sfront.words) === 'ann' && /ann/i.test(Sfront.front.text) && Sfront.front.eng && !Sfront.back.eng && Sfront.ink > SfrontPlain.ink + 20 && Sfront.own === '0' && Sfront.ctl !== 'visible', JSON.stringify({ Sfront, plain: SfrontPlain.ink }));
    await show(stage('both', 'front', 'Ann', 'Ruby'));
    const Sboth = await look();
    check('S5 Both, active front: both thumbnails engraved (Ann, Ruby), the work area the front with Ann', fine(Sboth) && Sboth.front.eng && Sboth.back.eng && /ann/i.test(Sboth.front.text) && /ruby/i.test(Sboth.back.text) && Sboth.face === 'front' && low(Sboth.words) === 'ann' && Sboth.front.cur && !Sboth.back.cur, JSON.stringify(Sboth));
    await show(stage('both', 'back', 'Ann', 'Ruby'));
    const Sboth2 = await look();
    check('S6 Both, active back: the work area turned back to the back with Ruby; the thumbnails keep both words; the back\'s own controls (Centre, Lines, the turn) are back', fine(Sboth2) && Sboth2.own === '1' && Sboth2.ctl === 'visible' && Sboth2.face === 'back' && low(Sboth2.words) === 'ruby' && Sboth2.back.cur && /ann/i.test(Sboth2.front.text) && /ruby/i.test(Sboth2.back.text) && Sboth2.ink > Snone.ink + 20, JSON.stringify(Sboth2));
    await show(stage('back', 'front', '', 'Ruby'));
    const Soff = await look();
    check('S7 Back only but the front active (about to type on it): the plain front in the work area, the front thumbnail plain and marked', fine(Soff) && Soff.face === 'front' && Soff.words === '' && Soff.front.cur && !Soff.front.eng && !Soff.front.text && Soff.back.eng, JSON.stringify(Soff));

    // ═══ K · a keystroke reaches the thumbnail and the work area at once: painted inside the call (no frame waited for), within one frame's time ═══
    // (the frame of this machine now, idle: ten frames with nothing to paint; a machine running other tests beside this one has longer frames, and a frame is what is asked)
    const idle = await page.evaluate(() => new Promise(res => { const ts = []; const f = t => { ts.push(t); if (ts.length < 11) requestAnimationFrame(f); else { const d = ts.slice(1).map((t, i) => t - ts[i]).sort((a, b) => a - b); res(d[Math.floor(d.length / 2)]); } }; requestAnimationFrame(f); }));
    const FRAME = Math.max(16.7, idle);   // (one frame of this machine)
    const median = a => { const v = a.slice().sort((x, y) => x - y); return v.length % 2 ? v[(v.length - 1) / 2] : (v[v.length / 2 - 1] + v[v.length / 2]) / 2; };   // (one keystroke that meets the garbage collector or a busy machine is not the rule)
    const keys = await page.evaluate(async Q => {
      const c = document.querySelector(Q), job = Engrave.items().get(c.dataset.key), key = c.dataset.key.replace(/~front$/, ''), out = [];
      for (const typed of ['R', 'Ru', 'Rub', 'Ruby']) {
        const t0 = performance.now(), r = Engrave.showStage(job, { key, choice: 'both', active: 'front', faces: { front: { text: typed, font: '', pos: null }, back: { text: 'Ruby', font: '', pos: null } } });
        { const h = document.querySelector(Q + ' .backHost'), th = document.querySelector(Q + ' [data-eng-thumb="front"] .engpvText'); out.push({ typed, seq: r.seq, drawn: +h.dataset.stageSeq, cost: h.dataset.paintMs, words: h.dataset.words, thumb: th && !th.hidden ? th.textContent : '', ms: Math.round((performance.now() - t0) * 10) / 10 }); }   // (read in the same task: nothing has run since the call)
        await new Promise(res => setTimeout(res, 40));
      }
      return out;
    }, Q);
    check('K1 every keystroke is on the front thumbnail AND in the work area when the call returns (no frame, no hop)', keys.every(k => k.drawn === k.seq && low(k.words) === low(k.typed) && low(k.thumb) === low(k.typed)), JSON.stringify(keys));
    check(`K2 keystroke to paint within one frame, ${FRAME.toFixed(1)} ms (${keys.map(k => k.ms + ' [' + k.cost + ']').join(', ')} ms)`, median(keys.map(k => k.ms)) <= FRAME, JSON.stringify({ idle, FRAME, keys }));
    // the same on the back, the face with its own fitted job (the staged words its draft)
    await show(stage('both', 'back', 'Ann', 'Ruby'));
    const keysB = await page.evaluate(async Q => {
      const c = document.querySelector(Q), job = Engrave.items().get(c.dataset.key), key = c.dataset.key.replace(/~front$/, ''), out = [];
      for (const typed of ['Rub', 'Ruby', 'Ruby J', 'Ruby Jo']) {
        const t0 = performance.now(), r = Engrave.showStage(job, { key, choice: 'both', active: 'back', faces: { front: { text: 'Ann', font: '', pos: null }, back: { text: typed, font: '', pos: null } } });
        { const h = document.querySelector(Q + ' .backHost'), th = document.querySelector(Q + ' [data-eng-thumb="back"] .engpvText'), d = Engrave.draftOf(job.key); out.push({ typed, seq: r.seq, drawn: +h.dataset.stageSeq, cost: h.dataset.paintMs, words: h.dataset.words, draft: d ? d.lines.join(' ') : null, thumb: th && !th.hidden ? th.textContent : '', ms: Math.round((performance.now() - t0) * 10) / 10 }); }
        await new Promise(res => setTimeout(res, 40));
      }
      return out;
    }, Q);
    check(`K3 typing on the back (its own fitted job): the thumbnail, the work area and its draft follow when the call returns, within a frame (${keysB.map(k => k.ms + ' [' + k.cost + ']').join(', ')} ms)`, keysB.every(k => k.drawn === k.seq && low(k.words) === low(k.typed) && low(k.thumb).replace(/\s+/g, ' ') === low(k.typed) && (low(k.draft) === low(k.typed) || (k.draft === null && low(k.typed) === 'ruby'))) && median(keysB.map(k => k.ms)) <= FRAME, JSON.stringify({ idle, FRAME, keysB }));   // ("Ruby" is the job's own words: its fitted drawing stands, no draft)

    // ═══ T · a face pressed right after a letter (the same frame): the work area is on the new face when the call returns; the card drawn again is never an empty work area ═══
    const press = await page.evaluate(Q => {
      const c = document.querySelector(Q), job = Engrave.items().get(c.dataset.key), key = c.dataset.key.replace(/~front$/, ''), h = () => document.querySelector(Q + ' .backHost');
      const st = (active, f) => ({ key, choice: 'both', active, faces: { front: { text: f, font: '', pos: null }, back: { text: 'Ruby', font: '', pos: null } } });
      Engrave.showStage(job, st('back', 'Ann')); const a = Engrave.showStage(job, st('front', 'Ann'));
      const one = { seq: a.seq, drawn: +h().dataset.stageSeq, face: h().dataset.face, words: h().dataset.words, cur: (document.querySelector(Q + ' [data-eng-thumb][aria-current="true"]') || { dataset: {} }).dataset.engThumb };
      Engrave.render();   // (the tab draws the detail afresh, as it does at each change: a commit landing after Approve)
      const h2 = h(), el = h2 && h2.querySelector(':scope > canvas, :scope > .egStagePv');
      const two = { face: h2 && h2.dataset.face, words: h2 && h2.dataset.words, el: !!el, w: el ? el.width || el.offsetWidth : 0 };
      return { one, two };
    }, Q);
    check('T1 a face pressed in the same frame as a letter: the work area and the thumbnails are on it when the call returns', press.one.drawn === press.one.seq && press.one.face === 'front' && low(press.one.words) === 'ann' && press.one.cur === 'front', JSON.stringify(press.one));
    check('T2 the card drawn again (Engrave.render) never shows an empty work area: the last drawing of that face holds its place until the new one is made', press.two.el && press.two.w > 0 && press.two.face === 'front' && low(press.two.words) === 'ann', JSON.stringify(press.two));
    await R.settle(page, 400);

    // ═══ R · 30 rapid alternating calls: the last state wins, nothing older is painted after it, no backlog ═══
    const rapid = await page.evaluate(async Q => {
      const c = document.querySelector(Q), job = Engrave.items().get(c.dataset.key), key = c.dataset.key.replace(/~front$/, ''), host = c.querySelector('.backHost');
      const seen = [], mo = new MutationObserver(() => seen.push(+host.dataset.stageSeq)); mo.observe(host, { attributes: true, attributeFilter: ['data-stage-seq'] });
      let maxAnims = 0, ghosts = 0; const watch = setInterval(() => { maxAnims = Math.max(maxAnims, host.getAnimations().length); ghosts = Math.max(ghosts, host.querySelectorAll('.egStageGhost').length); }, 4);
      let last = null;
      for (let i = 0; i < 30; i++) {
        const active = i % 2 ? 'back' : 'front', st = { key, choice: i % 3 ? 'both' : 'front', active, faces: { front: { text: 'F' + i, font: '', pos: null }, back: { text: 'B' + i, font: '', pos: null } } };
        last = { st, r: Engrave.showStage(job, st) };
        if (i % 4 === 1) await new Promise(r => setTimeout(r, 7)); else if (i % 7 === 3) await new Promise(r => requestAnimationFrame(r));
      }
      await new Promise(r => setTimeout(r, 900)); clearInterval(watch); mo.disconnect();
      const th = f => { const t = document.querySelector(Q + ` [data-eng-thumb="${f}"] .engpvText`); return t && !t.hidden ? t.textContent : ''; };
      return { seq: last.r.seq, want: last.st, drawn: +host.dataset.stageSeq, face: host.dataset.face, words: host.dataset.words, front: th('front'), back: th('back'), cur: (document.querySelector(Q + ' [data-eng-thumb][aria-current="true"]') || { dataset: {} }).dataset.engThumb,
        seen, monotone: seen.every((v, i) => !i || v >= seen[i - 1]), maxAnims, ghosts, anims: host.getAnimations().length, flipping: host.classList.contains('egFlipping'), el: !!host.querySelector(':scope > canvas, :scope > .egStagePv') };
    }, Q);
    const wantF = rapid.want.active, wantW = rapid.want.faces[wantF].text, shownW = rapid.want.choice === 'both' || rapid.want.choice === wantF ? wantW : '';
    check('R1 after 30 rapid alternating calls the work area and the thumbnails show exactly the last state', rapid.drawn === rapid.seq && rapid.face === wantF && low(rapid.words) === low(shownW) && rapid.cur === wantF && low(rapid.front) === low(rapid.want.choice === 'back' ? '' : rapid.want.faces.front.text) && low(rapid.back) === low(rapid.want.choice === 'front' ? '' : rapid.want.faces.back.text) && rapid.el, JSON.stringify(rapid));
    check('R2 no older state was ever painted after a newer one (data-stage-seq never went down)', rapid.monotone && rapid.seen.length > 0, JSON.stringify(rapid.seen));
    check('R3 no backlog: at most one turn running at a time, at most one fade, nothing left running', rapid.maxAnims <= 1 && rapid.ghosts <= 1 && rapid.anims === 0 && !rapid.flipping, JSON.stringify({ maxAnims: rapid.maxAnims, ghosts: rapid.ghosts, anims: rapid.anims, flipping: rapid.flipping }));
    await R.shot(w, 'STAGE-after-rapid');

    // ═══ Z · zoom and rulers on a staged face that has no job ═══
    await show(stage('both', 'front', 'Ann', 'Ruby'));
    const z0 = await page.evaluate(Q => { const h = document.querySelector(Q + ' .backHost'), cv = h.querySelector(':scope > canvas'); return { zoom: !!h.querySelector('.egZoom'), rx: !!h.querySelector('.egRuleX'), ry: !!h.querySelector('.egRuleY'), k: cv && cv._map ? cv._map.k : 0, face: h.dataset.face }; }, Q);
    await page.evaluate(() => Engrave.workZoomCmd('in')); await R.settle(page, 300);
    const z1 = await page.evaluate(Q => { const h = document.querySelector(Q + ' .backHost'), cv = h.querySelector(':scope > canvas'); return { k: cv && cv._map ? cv._map.k : 0, face: h.dataset.face, words: h.dataset.words, staged: !!(cv && cv._staged) }; }, Q);
    await page.evaluate(() => Engrave.workZoomCmd('fit')); await R.settle(page, 300);
    check('Z1 zoom and rulers on the staged front (no job): the zoom box and both rulers are there, zoom in draws it larger, still the front with Ann', z0.zoom && z0.rx && z0.ry && z0.face === 'front' && z1.k > z0.k * 1.02 && z1.face === 'front' && low(z1.words) === 'ann' && z1.staged, JSON.stringify({ z0, z1 }));

    // ═══ H · the thumbnails' hover and keyboard focus ═══
    await show(stage('both', 'back', 'Ann', 'Ruby'));
    await page.mouse.move(5, 5); await R.settle(page, 200);
    const rest = await page.evaluate(Q => { const b = document.querySelector(Q + ' [data-eng-thumb="front"]'), s = getComputedStyle(b); return { border: s.borderTopColor, shadow: s.boxShadow, transform: s.transform, cursor: s.cursor }; }, Q);
    await page.hover(Q + ' [data-eng-thumb="front"]'); await R.settle(page, 250);
    const hov = await page.evaluate(Q => { const b = document.querySelector(Q + ' [data-eng-thumb="front"]'), s = getComputedStyle(b); return { border: s.borderTopColor, shadow: s.boxShadow, transform: s.transform, cursor: s.cursor, hover: b.matches(':hover'), media: matchMedia('(hover:hover)').matches }; }, Q);
    const ty = m => { const v = /matrix\(([^)]+)\)/.exec(m || ''); return v ? +v[1].split(',')[5] : 0; };
    check('H1 hover on a thumbnail: the border warms to the gold, it lifts a pixel with a soft shadow, the pointer cursor', hov.hover && hov.border !== rest.border && /169, 130, 63/.test(hov.border) && hov.shadow !== 'none' && ty(hov.transform) < -0.5 && hov.cursor === 'pointer', JSON.stringify({ rest, hov }));
    await page.mouse.move(5, 5);
    await page.evaluate(Q => { const b = document.querySelector(Q + ' [data-eng-thumb="back"]'); b.focus(); }, Q);
    await page.keyboard.press('Shift+Tab'); await R.settle(page, 150);
    const foc = await page.evaluate(Q => { const a = document.activeElement, s = a ? getComputedStyle(a) : null; return { thumb: a && a.dataset ? a.dataset.engThumb || '' : '', fv: !!a && a.matches(':focus-visible'), outline: s ? [s.outlineStyle, s.outlineWidth, s.outlineOffset, s.outlineColor] : null }; }, Q);
    check('H2 keyboard focus on a thumbnail: a 2 px gold ring, 2 px out', foc.thumb === 'front' && foc.fv && foc.outline[0] === 'solid' && foc.outline[1] === '2px' && foc.outline[2] === '2px' && /169, 130, 63/.test(foc.outline[3]), JSON.stringify(foc));
    await page.evaluate(() => document.activeElement && document.activeElement.blur());

    // ═══ D · a drag in the work area is the stage's place (CharmNestEngravePanel.stagePos); drawn back from the stage, the words stay where they were let go ═══
    await page.evaluate(() => { window.__pos = []; CharmNestEngravePanel.stagePos = (key, face, pos) => { window.__pos.push({ key, face, pos }); }; });
    await show(stage('both', 'front', 'Ann', 'Ruby'));
    const inkX = () => page.evaluate(Q => { const cv = document.querySelector(Q + ' .backHost > canvas'), g = cv.getContext('2d'), d = g.getImageData(0, 0, cv.width, cv.height).data; let n = 0, sx = 0; for (let i = 0; i < d.length; i += 4) if (d[i] < 40 && d[i + 1] < 40 && d[i + 2] < 40 && d[i + 3] > 200) { n++; sx += (i / 4) % cv.width; } return { n, x: n ? sx / n : 0 }; }, Q);
    const d0 = await inkX(), box = await page.evaluate(Q => { const r = document.querySelector(Q + ' .backHost > canvas').getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2, w: r.width }; }, Q);
    await page.mouse.move(box.x, box.y); await page.mouse.down(); for (let k = 1; k <= 5; k++) { await page.mouse.move(box.x - k * box.w * 0.02, box.y, { steps: 1 }); await page.waitForTimeout(20); } await page.mouse.up(); await R.settle(page, 200);
    const d1 = await inkX(), calls = await page.evaluate(() => window.__pos.slice());
    const got = calls[calls.length - 1];
    check('D1 the staged front\'s words dragged left: they move at once, and the place let go is told to the stage (stagePos: the front, a point of the charm and of the frame drawn)', calls.length === 1 && got.face === 'front' && got.key === rk && got.pos && got.pos.charm.every(Number.isFinite) && got.pos.centre.every(Number.isFinite) && d1.x < d0.x - 2, JSON.stringify({ d0, d1, calls }));
    await show(Object.assign(stage('both', 'front', 'Ann', 'Ruby'), { faces: { front: { text: 'Ann', font: '', pos: got && got.pos }, back: { text: 'Ruby', font: '', pos: null } } }));
    const d2 = await inkX();
    check('D2 the stage drawn again with that place: the words stay exactly where they were let go (no jump back)', Math.abs(d2.x - d1.x) < 0.5 && Math.abs(d2.n - d1.n) < 20, JSON.stringify({ d1, d2 }));
    await page.evaluate(() => { delete CharmNestEngravePanel.stagePos; });

    // ═══ C · showStage(job, null): the committed state again ═══
    await show(null);
    const Lc = await look();
    check('C1 showStage(job, null): the committed state again (the back with Ruby, the front plain)', fine(Lc) && Lc.face === 'back' && low(Lc.words) === 'ruby' && Lc.back.eng && !Lc.front.eng && /ruby/i.test(Lc.back.text) && !Lc.front.text, JSON.stringify(Lc));

    // ═══ W · Paul's state after Front + Approve: a front job waiting for words, with no placement ═══
    const k2 = made.ruby2.key;
    const sv = await page.evaluate(async k => { const row = B.orders.byKey.get(k); try { return await Engrave.saveSetup(row, { mode: 'both', front: { text: '' } }); } catch (e) { return { ok: false, message: String(e && e.message || e) }; } }, k2);
    const fj = await until(() => page.evaluate(k => { const f = Engrave.jobsOf(B.orders.byKey.get(k)).find(j => j.face === 'front'); return f && !['classify', 'reclassify', 'ready', 'fitting'].includes(f.state) ? { key: f.key, state: f.state, fit: !!f.fit, reason: f.reason || '' } : null; }, k2), 60000);
    if (fj && !fj.fit) {
      await page.evaluate(k => { CN.setMode('engrave'); Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: k }); Engrave.render(); }, fj.key);
      await page.waitForSelector(Q + `[data-key="${fj.key}"]`, { timeout: 15000 }).catch(() => {}); await R.settle(page, 900);
      const W0 = await look();
      await R.shot(w, 'STAGE-front-waiting');
      check(`W1 the front waiting for words (${fj.state}, no placement): the work area draws the plain front of the charm; no reason, no "Engrave these words", no "Words to confirm"`, fine(W0) && W0.face === 'front' && W0.words === '' && W0.el === 'canvas' && W0.ink > 0 && W0.front && W0.front.cur, JSON.stringify({ fj, W0 }));
      await show(stage('both', 'front', 'Ann', 'Ruby'));
      const W1 = await look();
      check('W2 the same front with words staged: drawn on it at once, in the thumbnail too', fine(W1) && W1.face === 'front' && low(W1.words) === 'ann' && /ann/i.test(W1.front.text) && W1.ink > W0.ink + 20, JSON.stringify({ W1, plain: W0.ink }));
    } else check('W1 a front job waiting for words was made (saveSetup with an empty front)', false, JSON.stringify({ sv, fj }));
    check('E1 no page error', w.errors.length === 0, JSON.stringify(w.errors).slice(0, 600));
    console.log('screenshots in ' + shots);
  } finally { await w.close(); }
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\n${RESULTS.length - bad.length}/${RESULTS.length} checks pass`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
