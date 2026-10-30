// ENGSTAGE (Paul, 30 Oct 2026): the Design & Engrave card's choose, type, see, Approve flow, as a BLACK-BOX browser suite written against the spec's selectors and behaviour only
// (plans: stage-spec/SPEC.md, "Selectors"), not against any implementation. Paul's words: (1) the white selection pill must work on every option, Back included; (2) Front then Approve gave an
// empty work area, "Words to confirm", "The order gives no words for the front", "Engrave these words" and no Approve: every letter typed must show at once in the thumbnail AND the work area,
// toggling between options must be seamless, and fast clicking plus Approve must just work; (3) a changed choice on an approved piece re-lights Approve, and leaving without Approve reverts it;
// (4) a clean Front & Back choice; and "the hover states for everything here are inadequate".
//
// On the real sorter page over the repo's fake site (_engraveRealWorld.cjs: offline, real drawings, real fit, made-up names, nothing live), COMET necklaces engraved on the BACK only with the
// words "Ruby" (the shape of Paul's order 4214051539, BALLETSHOES "Ruby" on the back), most of them approved first, plus the two-face SCHRODINGER CAT piece "both" (front "W", back "Name One"):
//   S1  Paul's case 2: back "Ruby" approved, Front pressed: the work area shows the front at once (data-face=front), never empty, no forbidden text; the words MOVE with the one engraved
//       face (the lead's decision, 30 Oct: the front holds "Ruby"); "Mia" typed over them shows in the Front thumbnail and in .backHost data-words within one frame per keystroke;
//       Approve lit; pressed: calm "Approved" within 100 ms; committed: the front approved with "Mia", the back removed or plain
//   S2  Front & Back: front words and back words typed (switching by the thumbnails), both shown live; ONE Approve: both faces approved with the right words; card, list row and order window show both
//   S3  item 3: an approved piece, Back -> None lights Approve at once; Next (no Approve) discards it; back on the piece: Back, calm Approved, the list row's status as before
//   S4  item 1: each option gives exactly one aria-pressed="true"; the selected option has the same white pill whichever it is (None, Front, Back, Front & Back)
//   S5  choice Back, the Front thumbnail clicked and a letter typed: the choice becomes Front & Back
//   S6  an added face with no words (Front & Back on the Ruby piece: the added front starts empty): Approve disabled, no explanatory text, a press on it anyway changes nothing;
//       a front word typed lights Approve
//   S7  fuzz: 60 random actions with no waits (options, thumbnails, keys, Backspace, Approve), 3 seeds (two with real input events, one as an in-page burst; a back-only and a two-face piece):
//       after idle the UI equals its own data-choice / data-active, the thumbnails match the words box, committed equals staged after a last Approve, no duplicate jobs, no page errors
//   S8  the median keystroke-to-paint latency over 30 keys is <= 16 ms (thumbnail .engpvText and .backHost data-stage-seq / data-words)
//   S9  across all of the above, none of the forbidden texts is ever on screen
//   S10 hover, focus and pointer states of the left column (thumbnails, options, words box, Aa, Emoji, font picker) and of Approve
// The drive helpers fall back to the OLD controls (.engpSeg[data-mode], .engFace .engThumb) ONLY to press things on a build that lacks the spec's hooks, so a baseline run still reproduces
// Paul's flow; every READ is through the spec's hooks, and a missing hook is a FAIL of its own.
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/engrave-stage-flow.cjs     (exit 1 while any check is red; every line is PASS|FAIL id evidence)
'use strict';
const RESULTS = [];
const INFOS = [];
/** a measured line that is reported, never a FAIL (the coordinator's "report-only for now") */
const info = (id, text) => { INFOS.push({ id, text }); console.log(`INFO  ${id}: ${text}`); };
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}${ok ? '' : '  ' + String(evidence == null ? '' : evidence).slice(0, 1600)}`); };
const J = x => { try { return JSON.stringify(x); } catch (_) { return String(x); } };
const T0 = Date.now(), BUDGET = 170000;
const ONLY = (process.env.STAGE_ONLY || '').split(',').map(x => x.trim()).filter(Boolean), want = id => !ONLY.length || ONLY.includes(id);   // (STAGE_ONLY=S1,S10: only those scenarios, for a quick look)
const FORBIDDEN = ['Cut it plain', 'Keep it as it is', 'already approved', 'Words to confirm', 'Engrave these words', 'gives no words', 'being prepared', 'Approve to make this a regular order'];
const NORM = s => String(s == null ? '' : s).replace(/\s+/g, ' ').trim().toLowerCase();

/* ── the in-page probe: reads the spec's hooks only (the drive part may fall back to the old controls, flagged) ── */
function installProbe(FORBIDDEN) {
  const S = window.__stg = window.__stg || {};
  S.FORBIDDEN = FORBIDDEN;
  const norm = s => String(s == null ? '' : s).replace(/\s+/g, ' ').trim().toLowerCase();
  S.norm = norm;
  const vis = e => { if (!e || !e.isConnected) return false; const r = e.getBoundingClientRect(); if (r.width <= 0 || r.height <= 0) return false; const cs = getComputedStyle(e); if (cs.visibility === 'hidden' || cs.display === 'none') return false; return !e.closest('[hidden]'); };
  S.vis = vis;
  /** the work area draws something: a canvas with a layout box (offset size: a rotateY turn in flight still counts), not hidden */
  S.drawn = host => !!host && [...host.querySelectorAll('canvas')].some(c => c.isConnected && c.offsetWidth > 0 && c.offsetHeight > 0 && !c.closest('[hidden]') && getComputedStyle(c).visibility !== 'hidden' && getComputedStyle(c).display !== 'none');
  S.panel = pk => { const all = [...document.querySelectorAll('.engp[data-key]')].filter(p => p.dataset.key === pk); return all.find(vis) || all[0] || null; };
  S.scope = pk => { const p = S.panel(pk); return p ? (p.closest('.rvItem') || p.closest('#orderWin') || p.parentElement) : null; };
  S.front = () => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'), p = c && c.querySelector('.engp[data-key]'); return p ? p.dataset.key : null; };
  /** the words drawn in a thumbnail: its visible .engpvText */
  S.thumbWords = t => [...t.querySelectorAll('.engpvText')].filter(x => { if (x.hidden || x.closest('[hidden]')) return false; const cs = getComputedStyle(x); return cs.visibility !== 'hidden' && cs.display !== 'none' && parseFloat(cs.opacity || '1') > 0.05; }).map(x => x.textContent).join(' ');
  /** the element a press goes to (S.legacy: found only through the old controls) */
  S.target = (pk, kind, arg) => {
    S.legacy = false; const p = S.panel(pk), sc = S.scope(pk); if (!p || !sc) return null;
    const vfirst = list => list.find(vis) || list[0] || null;
    if (kind === 'choice') { let e = vfirst([...p.querySelectorAll(`[data-engp="choice"][data-choice="${arg}"]`)]); if (!e) { e = p.querySelector(`.engpSeg[data-engp="mode"][data-mode="${arg}"]`); if (e) S.legacy = true; } return e; }
    if (kind === 'thumb') { let e = vfirst([...sc.querySelectorAll(`[data-eng-thumb="${arg}"]`)]); if (!e) { e = sc.querySelector(`.engFace[data-face="${arg}"] .engThumb`); if (e) S.legacy = true; } return e; }
    if (kind === 'words') return p.querySelector('.engpWords');
    if (kind === 'font') return p.querySelector('select.engpFont');
    if (kind === 'alpha') return p.querySelector('[data-alpha]') || [...p.querySelectorAll('button')].find(b => /^A\s*α$/.test(b.textContent.trim())) || null;
    if (kind === 'emoji') return p.querySelector('[data-emoji]') || [...p.querySelectorAll('button')].find(b => b.textContent.trim() === 'Emoji') || null;
    if (kind === 'approve') return vfirst([...sc.querySelectorAll('[data-a="approve"]')].filter(vis));
    if (kind === 'next' || kind === 'prev') return sc.querySelector(`[data-a="${kind}"]`);
    return null;
  };
  S.approveState = sc => {
    const ap = [...sc.querySelectorAll('[data-a="approve"]')].filter(vis), calm = [...sc.querySelectorAll('[data-a="approved"]')].filter(vis);
    return { approve: ap.length ? { enabled: ap.some(b => !b.disabled && b.getAttribute('aria-disabled') !== 'true'), text: ap.map(b => b.textContent.trim()).join('|') } : null, calm: calm.length ? calm.map(b => b.textContent.trim()).join('|') : null };
  };
  /** everything the spec's hooks say about the piece's card right now */
  S.snap = pk => {
    const p = S.panel(pk); if (!p) return { open: false, front: S.front() };
    const sc = S.scope(pk), opts = [...p.querySelectorAll('[data-engp="choice"][data-choice]')];
    const th = f => [...sc.querySelectorAll(`[data-eng-thumb="${f}"]`)].find(vis) || sc.querySelector(`[data-eng-thumb="${f}"]`);
    const thumb = f => { const t = th(f); return t ? { cur: t.getAttribute('aria-current') === 'true', eng: t.dataset.engraved === '1', words: S.thumbWords(t) } : null; };
    const ta = p.querySelector('.engpWords'), host = sc.querySelector('.backHost');
    return Object.assign({ open: true, choice: p.dataset.choice == null ? null : p.dataset.choice, active: p.dataset.active == null ? null : p.dataset.active, dirty: p.dataset.dirty === '1',
      options: opts.map(o => o.dataset.choice), pressed: opts.filter(o => o.getAttribute('aria-pressed') === 'true').map(o => o.dataset.choice),
      thumbs: { front: thumb('front'), back: thumb('back') },
      box: ta ? ta.value : null, placeholder: ta ? ta.placeholder : null, boxDisabled: ta ? !!ta.disabled : null,
      host: host ? { face: host.dataset.face == null ? null : host.dataset.face, words: host.dataset.words == null ? null : host.dataset.words, seq: host.dataset.stageSeq == null ? null : host.dataset.stageSeq, canvas: S.drawn(host), busy: host.getAttribute('aria-busy') === 'true' } : null,
      busy: !!sc.querySelector('[aria-busy="true"]') || p.getAttribute('aria-busy') === 'true', busyWhat: [...sc.querySelectorAll('[aria-busy="true"]')].map(e => (e.tagName + '.' + String(e.className).split(' ').join('.') + (e.dataset.engThumb ? '[data-eng-thumb=' + e.dataset.engThumb + ']' : '') + (e.dataset.a ? '[data-a=' + e.dataset.a + ']' : '')).slice(0, 60)).join(', ') || undefined }, S.approveState(sc));
  };
  /** the staged words of each face: the box for the active face, the thumbnails for the other */
  S.staged = pk => { const s = S.snap(pk); if (!s.open) return null; const faces = s.choice === 'both' ? ['front', 'back'] : s.choice === 'front' || s.choice === 'back' ? [s.choice] : []; const words = {}; for (const f of faces) words[f] = f === s.active && s.box != null ? s.box : (s.thumbs[f] ? s.thumbs[f].words : ''); return { choice: s.choice, faces, words, active: s.active, box: s.box }; };
  /** what the jobs hold for the piece (committed): each face's job, duplicates, the sheets' entries */
  S.committed = pk => {
    const lineKey = pk.split('#')[0], slot = pk.includes('#') ? pk.split('#')[1] : null, row = B.orders.byKey.get(lineKey); if (!row) return null;
    const F = window.CharmNestEngraveFaces, fo = j => (F && F.faceOf ? F.faceOf(j) : (j.face === 'front' ? 'front' : 'back'));
    const jobs = Engrave.jobsOf(row).filter(j => j && !j.archived && !j.editingBack && (j.slot || null) === slot);
    const by = { front: jobs.filter(j => fo(j) === 'front'), back: jobs.filter(j => fo(j) === 'back') };
    const face = f => { const j = by[f][0]; return j ? { key: j.key, state: j.state, text: j.text || '', approvedAt: j.approvedAt || 0 } : null; };
    const pids = new Set(jobs.flatMap(j => j.copies || [])), sheet = [];
    try { for (const sh of allSheets()) for (const b of sh.backPool || []) if (pids.has(b.poolId)) sheet.push({ sheet: sh.sheetId, poolId: b.poolId, face: F && F.faceOf ? F.faceOf(b) : (b.face === 'front' ? 'front' : 'back') }); } catch (_) { /* no sheets */ }
    return { front: face('front'), back: face('back'), counts: { front: by.front.length, back: by.back.length }, sheet, working: jobs.some(j => ['classify', 'reclassify', 'ready', 'fitting'].includes(j.state)) };
  };
  /* forbidden texts, anywhere on screen in the Engrave tab or the order window, the whole run (one scan per frame at most) */
  S.seen = S.seen || {}; S.recOn = true; S.phase = '';
  const scan = () => { S.scanQ = 0; if (!S.recOn) return; for (const r of [document.getElementById('engraveView'), document.getElementById('orderWin')]) { if (!r) continue; const t = r.innerText || '', lo = t.toLowerCase(); for (const f of S.FORBIDDEN) { const i = lo.indexOf(f.toLowerCase()); if (i >= 0 && !S.seen[f]) S.seen[f] = { phase: S.phase, ctx: t.slice(Math.max(0, i - 50), i + f.length + 50).replace(/\s+/g, ' ') }; } } };
  if (!S.mo) { S.mo = new MutationObserver(() => { if (!S.scanQ) S.scanQ = requestAnimationFrame(scan); }); S.mo.observe(document.body, { subtree: true, childList: true, characterData: true, attributes: true, attributeFilter: ['hidden', 'style', 'class'] }); }
  /* every input in a words box: one frame later (after every rAF of that frame), do the active thumbnail and the work area show the box's value? */
  S.inputs = [];
  if (!S.inputOn) { S.inputOn = true; document.addEventListener('input', e => {
    const ta = e.target; if (!ta || !ta.matches || !ta.matches('.engp .engpWords')) return;
    const p = ta.closest('.engp'), pk = p && p.dataset.key, t0 = performance.now();
    requestAnimationFrame(() => setTimeout(() => {
      const p2 = S.panel(pk), sc = p2 && S.scope(pk); if (!p2 || !sc) { S.inputs.push({ pk, gone: true }); return; }
      const ta2 = p2.querySelector('.engpWords'), val = ta2 ? ta2.value : '', act = p2.dataset.active || null, th = act && sc.querySelector(`[data-eng-thumb="${act}"]`), host = sc.querySelector('.backHost'), tw = th ? S.thumbWords(th) : null;
      S.inputs.push({ pk, val, active: act, thumb: tw, hostWords: host && host.dataset.words != null ? host.dataset.words : null, hostFace: host && host.dataset.face != null ? host.dataset.face : null, okThumb: tw != null && norm(tw) === norm(val), okHost: !!host && host.dataset.words != null && norm(host.dataset.words) === norm(val) && host.dataset.face === act, dt: Math.round(performance.now() - t0) });
    }, 0));
  }, true); }
  /* the press of Approve: ms from the click to a calm "Approved" in the piece's card (the click's capture listener runs before the button's own) */
  S.armApprove = pk => {
    const ap = S.ap = { pk, tClick: null, tCalm: null, done: false };
    const calmNow = () => { const sc = S.scope(pk); if (!sc) return false; const st = S.approveState(sc); return !!st.calm && /Approved/i.test(st.calm) && !(st.approve && st.approve.enabled); };
    const mo = new MutationObserver(() => { if (ap.tClick != null && ap.tCalm == null && calmNow()) ap.tCalm = performance.now(); });
    const loop = () => { if (ap.tCalm == null && calmNow()) ap.tCalm = performance.now(); if (ap.tCalm == null && performance.now() - ap.tClick < 1500) requestAnimationFrame(loop); else { mo.disconnect(); document.removeEventListener('click', onClick, true); ap.done = true; } };
    const onClick = e => { const b = e.target && e.target.closest && e.target.closest('[data-a="approve"]'); if (b && ap.tClick == null) { ap.tClick = performance.now(); mo.observe(document.body, { subtree: true, childList: true, attributes: true, characterData: true }); requestAnimationFrame(loop); } };
    document.addEventListener('click', onClick, true);
  };
  /* frame-by-frame look at the piece's card after the next click, for `ms` */
  S.sample = (pk, ms) => {
    const out = S.samples = { list: [], done: false, tClick: null }, t0 = performance.now();
    const onClick = () => { if (out.tClick == null) out.tClick = performance.now(); };
    document.addEventListener('click', onClick, true);
    const tick = after => {
      const p = S.panel(pk), sc = p && S.scope(pk), host = sc && sc.querySelector('.backHost'), txt = sc ? (sc.innerText || '') : '', lo = txt.toLowerCase();
      out.list.push({ t: Math.round(performance.now() - (out.tClick == null ? t0 : out.tClick)), after, open: !!p, choice: p ? p.dataset.choice || null : null, face: host && host.dataset.face != null ? host.dataset.face : null, canvas: S.drawn(host),
        hostText: host ? (host.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 100) : '', forb: S.FORBIDDEN.filter(f => lo.includes(f.toLowerCase())) });
      const now = performance.now();
      if (!p && out.tClick != null && out.list.length > 2) { out.done = true; document.removeEventListener('click', onClick, true); return; }   // (the tab moved on)
      if ((out.tClick != null && now - out.tClick > ms) || now - t0 > ms + 4000) { out.done = true; document.removeEventListener('click', onClick, true); return; }
      next();
    };
    // (a look is taken after every rAF callback of a frame: `after` says whether the click came before that frame began)
    const next = () => requestAnimationFrame(() => { const after = out.tClick != null; setTimeout(() => tick(after), 0); });
    next();
  };
  /* a wait for the running transitions / animations inside an element to end (at most `max` ms) */
  S.calmAnims = (el, max) => new Promise(res => { const t0 = performance.now(); const go = () => { const run = (el && el.isConnected ? el.getAnimations({ subtree: true }) : []).filter(a => a.playState === 'running' && !(a.effect && a.effect.getTiming && a.effect.getTiming().iterations === Infinity)); if (!run.length || performance.now() - t0 > max) res(run.length); else requestAnimationFrame(go); }; requestAnimationFrame(go); });
  /** wait until an element's hover / focus styles hold still (3 frames alike, none of its transitions running; at most `max` ms): a transition that starts a frame late is waited for too */
  S.settle = (el, max) => new Promise(res => { const t0 = performance.now(); let last = '', same = 0; const go = () => { if (!el || !el.isConnected) return res(false); const sig = JSON.stringify(S.styles(el)), run = el.getAnimations({ subtree: true }).filter(a => a.playState === 'running' && !(a.effect && a.effect.getTiming && a.effect.getTiming().iterations === Infinity)).length; same = sig === last && !run ? same + 1 : 0; last = sig; if (same >= 3 || performance.now() - t0 > max) res(same >= 3); else requestAnimationFrame(go); }; requestAnimationFrame(go); });
  const parseColor = c => { let m = /rgba?\(([^)]+)\)/.exec(c || ''); if (m) { const q = m[1].split(/[\s,/]+/).filter(Boolean).map(Number); return { r: q[0], g: q[1], b: q[2], a: q.length > 3 ? q[3] : 1 }; } m = /color\(srgb ([^)]+)\)/.exec(c || ''); if (m) { const q = m[1].split(/[\s/]+/).filter(Boolean).map(Number); return { r: q[0] * 255, g: q[1] * 255, b: q[2] * 255, a: q.length > 3 ? q[3] : 1 }; } return null; };
  S.parseColor = parseColor;
  /** the background actually seen under the middle of an element (a sliding pill under the option counts) */
  S.effBg = el => {
    const r = el.getBoundingClientRect(), x = r.left + r.width / 2, y = r.top + r.height / 2;
    for (const e of document.elementsFromPoint(x, y)) { const cs = getComputedStyle(e), c = parseColor(cs.backgroundColor); if (c && c.a > 0.05 && parseFloat(cs.opacity || '1') > 0.05) return { rgb: [c.r, c.g, c.b].map(Math.round), a: c.a, who: e === el ? 'option' : el.contains(e) ? 'inside' : (String(e.className || e.tagName)).slice(0, 40), shadow: cs.boxShadow }; }
    return null;
  };
  /** hover / focus styles of a control: the element, its buttons, and the pill under it */
  S.styles = el => {
    const probes = [el, ...el.querySelectorAll('button,[role=button],.engThumb')];
    const r = el.getBoundingClientRect();
    // (a sliding pill under a choice option is its own element, usually pointer-events:none, so hit testing never finds it: any element of the option's group that lies under the option's middle is probed too, in DOM order)
    const cx = r.left + r.width / 2, cy = r.top + r.height / 2, grp = el.matches('[data-engp="choice"]') ? el.parentElement : null;
    if (grp) for (const e of grp.querySelectorAll('*')) { if (e === el || el.contains(e) || e.contains(el) || e.matches('[data-engp="choice"]') || e.closest('[data-engp="choice"]')) continue; const q = e.getBoundingClientRect(); if (q.width > 0 && q.height > 0 && cx >= q.left && cx <= q.right && cy >= q.top && cy <= q.bottom) probes.push(e); }
    return probes.map(e => { const cs = getComputedStyle(e); return { who: e === el ? 'self' : String(e.className || e.tagName).slice(0, 30), bg: cs.backgroundColor, bc: [cs.borderTopColor, cs.borderRightColor, cs.borderBottomColor, cs.borderLeftColor].join(' '), bs: cs.boxShadow, cursor: cs.cursor, outline: [cs.outlineStyle, cs.outlineWidth, cs.outlineColor].join(' '), ow: parseFloat(cs.outlineWidth) || 0, os: cs.outlineStyle, oc: cs.outlineColor, transform: cs.transform, opacity: cs.opacity, hover: e.matches(':hover'), attrs: e === el ? [...e.attributes].filter(a => /^(aria-|data-|disabled|class)/.test(a.name)).map(a => a.name + '=' + a.value).join(' ').slice(0, 160) : undefined }; });
  };
  const FOCUSABLE = 'button:not([disabled]),textarea:not([disabled]),select:not([disabled]),input:not([disabled]),a[href],[tabindex]:not([tabindex="-1"])';
  S.focusTarget = el => (el.matches(FOCUSABLE) || el.tabIndex >= 0 ? el : el.querySelector(FOCUSABLE) || el);
  /** focus the tabbable element just before `ft` in the document's tab order (so a real Tab lands on `ft`) */
  S.focusBefore = ft => {
    if (document.activeElement && document.activeElement.blur) document.activeElement.blur();
    const all = [...document.querySelectorAll(FOCUSABLE)].filter(e => vis(e) && e.tabIndex >= 0);
    const i = all.indexOf(ft); const prev = i > 0 ? all[i - 1] : null;
    if (prev) prev.focus({ preventScroll: true });
    return { prev: prev ? String(prev.className || prev.tagName).slice(0, 30) : null, tabbable: i >= 0 };
  };
  return true;
}

/* the in-page burst: the actions of one fuzz seed fired as separate tasks (setTimeout 0 between), no frame waited for */
async function burst({ pk, acts }) {
  const S = window.__stg, log = [];
  const fire = el => {
    const r = el.getBoundingClientRect(), o = { bubbles: true, cancelable: true, composed: true, clientX: r.left + r.width / 2, clientY: r.top + r.height / 2, button: 0, view: window };
    el.dispatchEvent(new PointerEvent('pointerdown', Object.assign({ pointerId: 1, pointerType: 'mouse', isPrimary: true }, o))); el.dispatchEvent(new MouseEvent('mousedown', o));
    if (el.focus) el.focus({ preventScroll: true });
    el.dispatchEvent(new PointerEvent('pointerup', Object.assign({ pointerId: 1, pointerType: 'mouse', isPrimary: true }, o))); el.dispatchEvent(new MouseEvent('mouseup', o)); el.dispatchEvent(new MouseEvent('click', o));
  };
  const setter = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value').set;
  const key = (ta, ch) => {
    if (document.activeElement !== ta) { ta.focus({ preventScroll: true }); ta.setSelectionRange(ta.value.length, ta.value.length); }
    const s = ta.selectionStart, e = ta.selectionEnd, v = ta.value, k = ch === null ? 'Backspace' : ch;
    if (!ta.dispatchEvent(new KeyboardEvent('keydown', { key: k, bubbles: true, cancelable: true }))) return;
    let nv, at;
    if (ch === null) { if (s === e && s === 0) return; nv = s === e ? v.slice(0, s - 1) + v.slice(e) : v.slice(0, s) + v.slice(e); at = s === e ? s - 1 : s; }
    else { nv = v.slice(0, s) + ch + v.slice(e); at = s + 1; }
    setter.call(ta, nv); ta.setSelectionRange(at, at);
    ta.dispatchEvent(new InputEvent('input', { bubbles: true, inputType: ch === null ? 'deleteContentBackward' : 'insertText', data: ch }));
    ta.dispatchEvent(new KeyboardEvent('keyup', { key: k, bubbles: true }));
  };
  const tb = performance.now();
  for (const a of acts) {
    const rec = { k: a.k, arg: a.arg, on: S.front(), t: Math.round(performance.now() - tb) };
    if (!S.panel(pk) || !S.vis(S.panel(pk))) {
      const row = B.orders.byKey.get(pk.split('#')[0]), j = Engrave.jobsOf(row).find(x => x.state !== 'none');
      if (!j) { log.push({ k: 'ended', why: 'the piece is a regular order now (None approved): no card to go back to' }); break; }
      rec.reopen = true; Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: j.key }); Engrave.render();
    }
    const p = S.panel(pk);
    if (!p) { rec.missing = true; log.push(rec); await new Promise(r => setTimeout(r, 0)); continue; }
    if (a.k === 'choice' || a.k === 'thumb') { const el = S.target(pk, a.k, a.arg); if (el && !el.disabled) fire(el); else rec.missing = true; }
    else if (a.k === 'key' || a.k === 'bs') { const ta = S.target(pk, 'words'); if (ta && !ta.disabled) key(ta, a.k === 'bs' ? null : a.arg); else rec.missing = true; }
    else if (a.k === 'approve') { const el = S.target(pk, 'approve'), en = !!el && !el.disabled && el.getAttribute('aria-disabled') !== 'true'; rec.enabled = en; rec.staged = S.staged(pk); if (en) fire(el); }
    log.push(rec);
    await new Promise(r => setTimeout(r, 0));
  }
  return log;
}

/* the keystroke latency: 30 keys dispatched in the page (a letter, then Backspace, in turn), each timed from its dispatch to the thumbnail's words and the work area's repaint (data-stage-seq bumped, data-words right) */
async function latency({ pk, n }) {
  const S = window.__stg; S.recOn = false;
  const setter = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value').set, root = document.getElementById('engraveView') || document.body, out = [];
  const letters = 'mialeorunbstycmialeorunbstycmialeorunbsty';
  // (context: how long a frame takes on this machine right now, measured just before the keys)
  const fr = []; { let prev = await new Promise(r => requestAnimationFrame(r)); for (let k = 0; k < 15; k++) { const t = await new Promise(r => requestAnimationFrame(r)); fr.push(t - prev); prev = t; } }
  const frameMs = fr.sort((a, b) => a - b)[7];
  for (let i = 0; i < n; i++) {
    // (keys come at any moment of a frame, as typing does: after a frame plus a varying 0..15 ms)
    await new Promise(r => requestAnimationFrame(() => setTimeout(r, (i * 7) % 16)));
    const p = S.panel(pk); if (!p) { out.push({ gone: true }); continue; }
    const sc = S.scope(pk), ta = p.querySelector('.engpWords'); if (!ta || ta.disabled) { out.push({ noBox: true }); continue; }
    const h0 = sc.querySelector('.backHost'), seq0 = h0 && h0.dataset.stageSeq != null ? h0.dataset.stageSeq : null;
    const th0 = (() => { const a0 = p.dataset.active, t = a0 && sc.querySelector(`[data-eng-thumb="${a0}"]`); return t ? S.norm(S.thumbWords(t)) : null; })();   // (the thumbnail must CHANGE to the new words: a stale one that already shows them, e.g. after a Backspace, does not count)
    if (document.activeElement !== ta) { ta.focus({ preventScroll: true }); ta.setSelectionRange(ta.value.length, ta.value.length); }
    const del = i % 2 === 1, ch = del ? null : letters[i], s = ta.selectionStart, e = ta.selectionEnd, v = ta.value;
    const nv = del ? (s === e ? v.slice(0, Math.max(0, s - 1)) + v.slice(e) : v.slice(0, s) + v.slice(e)) : v.slice(0, s) + ch + v.slice(e), at = del ? (s === e ? Math.max(0, s - 1) : s) : s + 1, want = S.norm(nv);
    let tThumb = null, tHost = null;
    const look = () => {
      const p3 = S.panel(pk), sc3 = p3 && S.scope(pk); if (!sc3) return;
      const act = p3.dataset.active, th = act && sc3.querySelector(`[data-eng-thumb="${act}"]`), h = sc3.querySelector('.backHost');
      if (tThumb == null && th) { const tw = S.norm(S.thumbWords(th)); if (tw === want && tw !== th0) tThumb = performance.now() - t0; }
      if (tHost == null && h && h.dataset.stageSeq != null && h.dataset.stageSeq !== seq0 && S.norm(h.dataset.words) === want) tHost = performance.now() - t0;
    };
    const mo = new MutationObserver(look); mo.observe(root, { subtree: true, childList: true, attributes: true, characterData: true });
    const t0 = performance.now();
    ta.dispatchEvent(new KeyboardEvent('keydown', { key: del ? 'Backspace' : ch, bubbles: true, cancelable: true }));
    setter.call(ta, nv); ta.setSelectionRange(at, at);
    ta.dispatchEvent(new InputEvent('input', { bubbles: true, inputType: del ? 'deleteContentBackward' : 'insertText', data: ch }));
    ta.dispatchEvent(new KeyboardEvent('keyup', { key: del ? 'Backspace' : ch, bubbles: true }));
    const sync = performance.now() - t0;
    look();
    await new Promise(res => { const loop = () => { look(); if ((tThumb != null && tHost != null) || performance.now() - t0 > 250) res(); else requestAnimationFrame(loop); }; requestAnimationFrame(loop); });
    mo.disconnect();
    out.push({ thumb: tThumb == null ? null : Math.round(tThumb * 10) / 10, host: tHost == null ? null : Math.round(tHost * 10) / 10, sync: Math.round(sync * 10) / 10 });
  }
  S.recOn = true;
  return { list: out, frame: frameMs };
}

/* a seeded random */
const rng = seed => { let a = seed >>> 0; return () => { a = (a + 0x6D2B79F5) >>> 0; let t = a; t = Math.imul(t ^ (t >>> 15), t | 1); t ^= t + Math.imul(t ^ (t >>> 7), t | 61); return ((t ^ (t >>> 14)) >>> 0) / 4294967296; }; };
function fuzzActions(seed, n, lastApprove) {
  const r = rng(seed), out = [], L = 'MiaRubyLeoAvaJones';
  for (let i = 0; i < n; i++) {
    const x = r();
    if (lastApprove && i === n - 1) out.push({ k: 'approve' });
    else if (x < 0.2) out.push({ k: 'choice', arg: ['none', 'front', 'back', 'both'][Math.floor(r() * 4)] });
    else if (x < 0.35) out.push({ k: 'thumb', arg: r() < 0.5 ? 'front' : 'back' });
    else if (x < 0.68) out.push({ k: 'key', arg: L[Math.floor(r() * L.length)] });
    else if (x < 0.8) out.push({ k: 'bs' });
    else out.push({ k: 'approve' });
  }
  return out;
}
const median = a => { const s = a.slice().sort((x, y) => x - y); return s.length ? (s.length % 2 ? s[(s.length - 1) / 2] : (s[s.length / 2 - 1] + s[s.length / 2]) / 2) : NaN; };

module.exports = { installProbe, burst, latency, fuzzActions, rng, FORBIDDEN, NORM };
if (require.main === module) (async () => {
  const R = require('./_engraveRealWorld.cjs');
  // the fixtures: COMET necklaces engraved on the BACK only, words "Ruby" (Paul's BALLETSHOES order had "Ruby" on the back); made-up buyers and order numbers
  const ONE = { stRuby: '4290000301', stDuo: '4290000302', stKeep: '4290000303', stUi: '4290000304', stFz1: '4290000305', stFz2: '4290000306' };
  Object.entries(ONE).forEach(([n, rid], i) => { R.ORDERS[n] = Object.assign({}, R.ORDERS.back, { rid, listing: '18900003' + String(i + 1).padStart(2, '0'), buyer: 'Name Stage ' + (i + 1), personalization: ['Ruby'] }); R.EXPECT_JOBS[n] = 1; });
  const w = await R.world({ shots: process.env.SHOTS || '/tmp/engrave-stage-flow-shots' });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page, errors = [];
  page.on('pageerror', e => errors.push(e.message));
  await page.waitForFunction(() => !window.CNRole || ['ask', 'role', 'admin'].includes(CNRole.state()), null, { timeout: 20000 }).catch(() => {}).then(() => page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('design'); } catch (_) { /* none */ } }));
  const frames = (n = 2) => page.evaluate(n => new Promise(r => { const go = k => (k ? requestAnimationFrame(() => go(k - 1)) : setTimeout(r, 0)); go(n); }), n);
  const phase = p => page.evaluate(p => { if (window.__stg) window.__stg.phase = p; }, p).catch(() => {});
  const until = async (f, ms, every = 100) => { const t0 = Date.now(); for (;;) { const v = await f(); if (v || Date.now() - t0 > ms) return v; await new Promise(r => setTimeout(r, every)); } };
  const timeLeft = () => BUDGET - (Date.now() - T0);
  let made = null;
  const pkOf = n => made[n].key;
  const snap = pk => page.evaluate(pk => window.__stg.snap(pk), pk);
  const committed = pk => page.evaluate(pk => window.__stg.committed(pk), pk);
  const neutral = { x: 1436, y: 4 };
  /** the piece's card in the Engrave tab's detail (navigation, as the existing tests open it), waited for on the panel's own hooks */
  const openPiece = async (name, face = 'back') => {
    const key = pkOf(name);
    const jk = await page.evaluate(({ key, face }) => {
      const row = B.orders.byKey.get(key); if (!row) return null;
      const jobs = Engrave.jobsOf(row), live = jobs.filter(x => x.state !== 'none'), j = live.find(x => (x.face === 'front' ? 'front' : 'back') === face) || live[0] || jobs[0]; if (!j) return null;
      try { if (window.OrderWin && OrderWin.close) OrderWin.close(); } catch (_) { /* none */ }
      CN.setMode('engrave'); Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: j.key }); Engrave.render(); return j.key;
    }, { key, face });
    await page.waitForFunction(pk => { const S = window.__stg, p = S.panel(pk); return !!p && S.vis(p); }, key, { timeout: 8000, polling: 'raf' }).catch(() => {});
    await page.waitForFunction(pk => { const S = window.__stg, sc = S.scope(pk), h = sc && sc.querySelector('.backHost'); return !!(h && h.querySelector('canvas')); }, key, { timeout: 3000, polling: 'raf' }).catch(() => {});
    await frames(2);
    return jk;
  };
  const toList = async () => {
    await page.evaluate(() => { try { if (window.OrderWin && OrderWin.close) OrderWin.close(); } catch (_) { /* none */ } CN.setMode('engrave'); Engrave.restoreView({ tab: 'place', list: true, chosen: true, focus: null }); Engrave.render(); });
    await page.waitForSelector('#egQueue .egPlacementList .placementRow', { timeout: 10000 }).catch(() => {}); await frames(2);
    await new Promise(r => setTimeout(r, 300));   // (a person's visit to the list: a build that forgets a left piece's draft a moment later has done so before the piece is opened again; S3 tests the quick return itself)
  };
  const rowStatus = rid => page.evaluate(rid => { const r = document.querySelector(`#egQueue .placementRow[data-rid="${rid}"]`); if (!r) return null; const st = [...r.querySelectorAll('[data-stage]')]; return { s: st.map(x => x.dataset.s || ''), text: st.map(x => x.textContent.trim()), seals: ((r.querySelector('.egPlacementSeals') || {}).innerText || '').replace(/\s+/g, ' ').trim(), words: ((r.querySelector('.engravingIdentity') || {}).innerText || '').replace(/\s+/g, ' ').trim(), vec: [...r.querySelectorAll('[data-vector]')].map(v => v.getAttribute('aria-label') || '') }; }, rid);
  /** a real click on the piece's control (Playwright's actionability waits apply); { ok, legacy } */
  const press = async (pk, kind, arg) => {
    const h = await page.evaluateHandle(([pk, kind, arg]) => window.__stg.target(pk, kind, arg), [pk, kind, arg]);
    const el = h.asElement(), legacy = await page.evaluate(() => !!window.__stg.legacy);
    if (!el) return { ok: false, legacy, why: 'not found' };
    try { await el.click({ timeout: 3000 }); return { ok: true, legacy }; } catch (e) { return { ok: false, legacy, why: String(e.message).split('\n')[0].slice(0, 160) }; }
  };
  /** real typing into the piece's words box: a click in it, the caret at the end (or all selected), then the keys */
  const typeInto = async (pk, text, o = {}) => {
    const r = await press(pk, 'words'); if (!r.ok) return r;
    await page.evaluate(([pk, all]) => { const ta = window.__stg.target(pk, 'words'); if (ta) { ta.focus(); if (all) ta.setSelectionRange(0, ta.value.length); else ta.setSelectionRange(ta.value.length, ta.value.length); } }, [pk, !!o.replace]);
    await page.evaluate(() => { window.__stg.inputs = []; });
    await page.keyboard.type(text, { delay: 45 });
    await frames(2);
    return { ok: true, inputs: await page.evaluate(() => window.__stg.inputs.slice()) };
  };
  const approvedish = s => ['approved', 'written'].includes(s);
  const plainish = c => !c || ['none', 'skipped'].includes(c.state);
  /** idle: nothing busy and nothing on the card or in the jobs changing for 1.2 s. waitIdle.changing: what still changed at the end (for the evidence of a card that never holds still) */
  const flat = (o, pre = '', out = {}) => { if (o && typeof o === 'object') { for (const k of Object.keys(o)) flat(o[k], pre ? pre + '.' + k : k, out); } else out[pre] = o; return out; };
  const waitIdle = async (pk, ms = 15000) => {
    let last = null, same = 0; const t0 = Date.now(), changing = {};
    for (;;) {
      // (a commit still landing is not idle: the card calm and not dirty while a face it shows engraved is not approved yet, e.g. a queued second commit between its fit and its approval)
      const v = await page.evaluate(pk => { const S = window.__stg, s = S.snap(pk), c = S.committed(pk), st = S.staged(pk), landing = !!(s.open && !s.dirty && s.calm && st && c && st.faces.some(f => !(c[f] && ['approved', 'written'].includes(c[f].state)))); return { s, c, landing, busy: !!(s.busy || (s.host && s.host.busy) || (c && c.working) || landing) }; }, pk);
      const f = flat({ s: v.s, c: v.c }), diff = last ? Object.keys(Object.assign({}, f, last)).filter(k => J(f[k]) !== J(last[k])) : ['(first)'];
      if (!diff.length && !v.busy) same++; else { same = 0; if (Date.now() - t0 > ms - 3000) for (const k of diff) changing[k] = String(J(f[k])).slice(0, 40); }
      last = f;
      waitIdle.landing = v.landing;
      if (same >= 12) { waitIdle.changing = null; return true; }
      if (Date.now() - t0 > ms) { waitIdle.changing = changing; return false; }
      await new Promise(r => setTimeout(r, 100));
    }
  };
  /** the commit of a press settles: `want(committed)` holds, or nothing changes for 2 s with nothing at work (a build that never commits is not waited on for long) */
  const settle = async (pk, want, ms = 20000) => {
    let last = '', same = 0; const t0 = Date.now();
    for (;;) {
      const c = await committed(pk); if (c && want(c) && !c.working) return c;
      const sig = J(c); if (sig === last && !(c && c.working)) same++; else same = 0; last = sig;
      if (same >= 20 || Date.now() - t0 > ms) return c;
      await new Promise(r => setTimeout(r, 100));
    }
  };
  /** ms from `t0` (Date.now at the press) until the jobs hold `faces` written (each with its words) and no other face engraved; null after 20 s */
  const landing = async (pk, t0, faces, words) => {
    for (;;) {
      const c = await committed(pk);
      if (c && ['front', 'back'].every(f => (faces.includes(f) ? c[f] && c[f].state === 'written' && (!words || NORM(c[f].text) === NORM(words[f])) : plainish(c[f])))) return Date.now() - t0;
      if (Date.now() - t0 > 20000) return null;
      await new Promise(r => setTimeout(r, 50));
    }
  };
  const landInfo = (id, ms) => info(id, `last press to all faces written ${ms == null ? 'over 20 s' : ms + ' ms'} (target <= 4000 ms on an unloaded machine; load ${require('os').loadavg().map(x => Math.round(x * 10) / 10).join(' ')})`);
  const lap = id => console.log(`   -- ${id} at ${Math.round((Date.now() - T0) / 1000)} s`);
  /** the colour a person sees inside an option (a screenshot of it, read back in the page: a sliding pill drawn by a ::before counts): the per-channel median of 5 points clear of the label and the mark */
  const seenColour = async (pk, c) => {
    const r = await page.evaluate(([pk, c]) => { const S = window.__stg, p = S.panel(pk), el = p && [...p.querySelectorAll(`[data-engp="choice"][data-choice="${c}"]`)].find(S.vis); if (!el) return null; const b = el.getBoundingClientRect(); return { x: b.left + scrollX, y: b.top + scrollY, w: b.width, h: b.height }; }, [pk, c]);
    if (!r || r.w < 14 || r.h < 12) return null;
    const buf = await page.screenshot({ clip: { x: r.x, y: r.y, width: r.w, height: r.h } });
    const px = await page.evaluate(async ([bytes, w, h]) => {
      const bm = await createImageBitmap(new Blob([new Uint8Array(bytes)], { type: 'image/png' })), cv = document.createElement('canvas'); cv.width = bm.width; cv.height = bm.height;
      const g = cv.getContext('2d'); g.drawImage(bm, 0, 0); const k = bm.width / w;
      return [[6, h / 2], [w - 6, h / 2], [w / 2, h - 5], [w / 4, h - 5], [3 * w / 4, h - 5]].map(([x, y]) => [...g.getImageData(Math.min(bm.width - 1, Math.round(x * k)), Math.min(bm.height - 1, Math.round(y * k)), 1, 1).data].slice(0, 3));
    }, [[...buf], r.w, r.h]);
    return [0, 1, 2].map(i => px.map(q => q[i]).sort((a, b) => a - b)[2]);
  };
  const skipRest = id => check(id, false, `not run: the ${Math.round(BUDGET / 1000)} s budget is spent (${Math.round((Date.now() - T0) / 1000)} s)`);

  try {
    made = await R.build(w, Object.keys(ONE).concat(['both']));
    await page.evaluate(installProbe, FORBIDDEN);
    { const sheets = await page.evaluate(() => { const out = []; for (const sh of allSheets()) if (sh.charms && sh.charms.length) { if (!sh.folderPath) sh.folderPath = 'charmnest/sheets/' + sh.sheetId; out.push({ sheetId: sh.sheetId, poolIds: sh.charms.map(x => x.poolId) }); } return out; });
      for (const sh of sheets) w.srv.st.put('Charm_Nest_Sheets', sh.sheetId, { sheetId: sh.sheetId, poolIds: sh.poolIds, backPool: [] }); }
    console.log(`   built in ${Math.round((Date.now() - T0) / 1000)} s: ${J(Object.fromEntries(Object.entries(made).map(([n, m]) => [n, m.jobs.map(j => [j.face, j.state, j.text, j.fitted])])))}`);
    // the backs approved first (the green Approve of the card, as a person does; the page's own approve only if the button never gets there)
    const setup = {};
    for (const n of ['stRuby', 'stDuo', 'stKeep', 'stUi', 'stFz1']) {
      await openPiece(n);
      const r = await press(pkOf(n), 'approve');
      let st = await until(async () => { const c = await committed(pkOf(n)); return c && c.back && approvedish(c.back.state) ? c.back.state : null; }, 12000);
      if (!st) { await page.evaluate(k => { const j = Engrave.items().get(k); return j && Engrave.approve(j, 'Test Operator'); }, made[n].jobs[0].key).catch(() => {}); st = await until(async () => { const c = await committed(pkOf(n)); return c && c.back && approvedish(c.back.state) ? c.back.state + ' (by the page\'s approve)' : null; }, 12000); }
      setup[n] = st || ('not approved: ' + J(r));
    }
    // (each approved back written to its sheet before the scenarios start: nothing of the setup still at work under them)
    await until(() => page.evaluate(ks => ks.every(k => { const j = Engrave.items().get(k); return j && j.state === 'written'; }), ['stRuby', 'stDuo', 'stKeep', 'stUi', 'stFz1'].map(n => made[n].jobs[0].key)), 30000, 200);
    for (const n of Object.keys(setup)) { const c = await committed(pkOf(n)); setup[n] = c && c.back ? c.back.state + (c.sheet.length ? ' (on its sheet)' : '') : setup[n]; }
    check('S0 the fixtures: five back-only "Ruby" pieces approved, one in review, the two-face piece built', Object.values(setup).every(s => /^(approved|written)/.test(s)) && made.stFz2.jobs.length === 1 && made.both.jobs.length === 2, J(setup));
    console.log(`   setup: ${J(setup)} (${Math.round((Date.now() - T0) / 1000)} s)`);

    /* ════ S1 · Paul's case 2 ════ */
    await phase('S1'); lap('S1');
    if (want('S1')) {
      const pk = pkOf('stRuby');
      await openPiece('stRuby');
      const s0 = await snap(pk);
      check('S1 the spec\'s hooks are there: .engp[data-choice][data-active], the [data-engp=choice] options, [data-eng-thumb] Front and Back, .backHost[data-face][data-words][data-stage-seq]',
        s0.open && s0.choice != null && s0.active != null && s0.options.length >= 3 && !!s0.thumbs.front && !!s0.thumbs.back && !!s0.host && s0.host.face != null && s0.host.words != null && s0.host.seq != null, J(s0));
      check('S1 starts as Paul\'s: Back chosen with "Ruby", calm Approved', s0.choice === 'back' && s0.calm && /Approved/i.test(s0.calm) && !(s0.approve && s0.approve.enabled) && NORM(s0.box) === 'ruby', J({ choice: s0.choice, calm: s0.calm, approve: s0.approve, box: s0.box }));
      await page.evaluate(pk => window.__stg.sample(pk, 900), pk);
      const pr = await press(pk, 'choice', 'front');
      await page.waitForFunction(() => window.__stg.samples.done, null, { timeout: 8000 }).catch(() => {});
      const sm = await page.evaluate(() => window.__stg.samples);
      const after = sm.list.filter(x => x.after), first = after[0] || {};
      const s1 = await snap(pk), s1text = await page.evaluate(pk => { const sc = window.__stg.scope(pk); return sc ? sc.innerText || '' : ''; }, pk);
      check('S1 Front is pressed through the spec\'s option [data-engp=choice][data-choice=front]', pr.ok && !pr.legacy, J(pr));
      check('S1 the work area shows the FRONT in the first frame after the press (data-face=front, the charm drawn)', first.face === 'front' && first.canvas, J({ first, n: after.length }));
      check('S1 the work area is never empty and stays on the front for 0.9 s, no message in it', after.length > 0 && after.every(x => x.canvas && x.face === 'front'), J(after.filter(x => !(x.canvas && x.face === 'front')).slice(0, 4)));
      check('S1 none of "gives no words", "Engrave these words", "Words to confirm", "being prepared" (nor any forbidden text) in the card after Front', after.every(x => !x.forb.length) && !FORBIDDEN.some(f => NORM(s1text).includes(NORM(f))), J([...new Set(after.flatMap(x => x.forb))]));
      check('S1 Front alone is chosen: one pressed option (front), data-choice=front, data-active=front, the Back thumbnail plain, the Front thumbnail current', s1.open && J(s1.pressed) === '["front"]' && s1.choice === 'front' && s1.active === 'front' && s1.thumbs.back && !s1.thumbs.back.eng && !NORM(s1.thumbs.back.words) && s1.thumbs.front && s1.thumbs.front.cur,
        J({ pressed: s1.pressed, choice: s1.choice, active: s1.active, thumbs: s1.thumbs }));
      // (the lead's decision, 30 Oct: switching the single engraved face MOVES the words: the buyer's "Ruby" goes to the front, the back is no longer engraved)
      check('S1 the words move with the face: the front box, the Front thumbnail and the work area hold "Ruby"', NORM(s1.box) === 'ruby' && s1.thumbs.front && NORM(s1.thumbs.front.words) === 'ruby' && s1.host && NORM(s1.host.words) === 'ruby' && s1.host.face === 'front', J({ box: s1.box, thumbs: s1.thumbs, host: s1.host }));
      const ty = await typeInto(pk, 'Mia', { replace: true });
      const ins = (ty.inputs || []).filter(x => x.pk === pk);
      check('S1 each keystroke of "Mia" is on the Front thumbnail (.engpvText) within one frame', ins.length === 3 && ins.every(x => x.okThumb && x.active === 'front'), J(ins));
      check('S1 each keystroke of "Mia" is in the work area (.backHost data-words, data-face=front) within one frame', ins.length === 3 && ins.every(x => x.okHost), J(ins));
      const s2 = await snap(pk);
      check('S1 Approve is lit (enabled) with "Mia" typed', s2.approve && s2.approve.enabled && !s2.calm && NORM(s2.box) === 'mia', J({ approve: s2.approve, calm: s2.calm, box: s2.box, dirty: s2.dirty }));
      await page.evaluate(pk => { window.__stg.armApprove(pk); window.__stg.sample(pk, 1500); }, pk);
      const tPress1 = Date.now(), ap = await press(pk, 'approve');
      await page.waitForFunction(() => window.__stg.ap.done && window.__stg.samples.done, null, { timeout: 8000 }).catch(() => {});
      const at = await page.evaluate(() => ({ ap: window.__stg.ap, samples: window.__stg.samples.list.filter(x => x.after) }));
      const ms = at.ap.tCalm != null && at.ap.tClick != null ? Math.round(at.ap.tCalm - at.ap.tClick) : null;
      check('S1 Approve pressed: the button reads a calm "Approved" within 100 ms', ap.ok && ms != null && ms <= 100, J({ press: ap, ms }));
      const shown = at.samples.filter(x => x.open);
      check('S1 after Approve the card is never an empty work area and never shows a forbidden text', shown.every(x => x.canvas && !x.forb.length), J(shown.filter(x => !x.canvas || x.forb.length).slice(0, 3)));
      const c1 = await settle(pk, c => c.front && approvedish(c.front.state) && NORM(c.front.text) === 'mia' && plainish(c.back));
      landInfo('S1 (Front "Mia", the back taken away)', await landing(pk, tPress1, ['front'], { front: 'Mia' }));
      check('S1 committed: the front job exists, approved, with the words "Mia"', !!c1.front && approvedish(c1.front.state) && NORM(c1.front.text) === 'mia', J(c1.front));
      check('S1 committed: choice Front means the back is no longer engraved: its job removed or plain (none), not approved', plainish(c1.back) && !(c1.back && c1.back.approvedAt), J(c1.back));
      const c1s = (await until(async () => { const c = await committed(pk); return c && !c.sheet.some(e => e.face === 'back') ? c : null; }, 8000, 200)) || await committed(pk);
      check('S1 one job per face, no orphan sheet entry for the plain back', c1s.counts.front <= 1 && c1s.counts.back <= 1 && !c1s.sheet.some(e => e.face === 'back'), J({ counts: c1s.counts, sheet: c1s.sheet }));
      await openPiece('stRuby', 'front');
      const s3 = await snap(pk);
      check('S1 back on the piece: Front chosen, calm Approved, the Front thumbnail "Mia", the Back plain', s3.open && s3.choice === 'front' && !s3.dirty && s3.calm && !(s3.approve && s3.approve.enabled) && s3.thumbs.front && NORM(s3.thumbs.front.words) === 'mia' && s3.thumbs.back && !s3.thumbs.back.eng, J(s3));
      if (w.shots) await R.shot(w, 'stage-S1').catch(() => {});
    }

    /* ════ S2 · Front & Back ════ */
    await phase('S2'); lap('S2');
    if (!want('S2')) { /* not asked for */ } else if (timeLeft() < 0) skipRest('S2'); else {
      const pk = pkOf('stDuo');
      await openPiece('stDuo');
      const b = await press(pk, 'choice', 'both');
      await frames(1);
      const s0 = await snap(pk);
      check('S2 a Front & Back option exists and is chosen: one pressed (both), data-choice=both, both thumbnails engraved', b.ok && !b.legacy && J(s0.pressed) === '["both"]' && s0.choice === 'both' && s0.thumbs.front && s0.thumbs.front.eng && s0.thumbs.back && s0.thumbs.back.eng, J({ press: b, pressed: s0.pressed, choice: s0.choice, thumbs: s0.thumbs }));
      const tf = await press(pk, 'thumb', 'front'); await frames(1);
      const s1 = await snap(pk);
      check('S2 the Front thumbnail makes the front active: aria-current, data-active=front, the work area on the front', tf.ok && s1.active === 'front' && s1.thumbs.front && s1.thumbs.front.cur && s1.host && s1.host.face === 'front', J({ press: tf, active: s1.active, thumbs: s1.thumbs, host: s1.host }));
      const t1 = await typeInto(pk, 'Ava', { replace: true });
      const tb = await press(pk, 'thumb', 'back'); await frames(1);
      const t2 = await typeInto(pk, 'Leo', { replace: true });
      const s2 = await snap(pk);
      const i1 = (t1.inputs || []).filter(x => x.pk === pk), i2 = (t2.inputs || []).filter(x => x.pk === pk);
      check('S2 the front words show live on the Front thumbnail and the work area, key by key', i1.length === 3 && i1.every(x => x.okThumb && x.okHost && x.active === 'front'), J(i1));
      check('S2 the Back thumbnail switches to the back; the back words show live on it and in the work area, key by key', tb.ok && i2.length === 3 && i2.every(x => x.okThumb && x.okHost && x.active === 'back'), J({ tb, i2 }));
      check('S2 both thumbnails show their own words at once (Front "Ava", Back "Leo"), still Front & Back', s2.choice === 'both' && s2.thumbs.front && NORM(s2.thumbs.front.words) === 'ava' && s2.thumbs.back && NORM(s2.thumbs.back.words) === 'leo', J({ choice: s2.choice, thumbs: s2.thumbs }));
      await page.evaluate(pk => window.__stg.armApprove(pk), pk);
      const tPress2 = Date.now(), ap = await press(pk, 'approve');
      await page.waitForFunction(() => window.__stg.ap.done, null, { timeout: 5000 }).catch(() => {});
      const apT = await page.evaluate(() => window.__stg.ap);
      const ms = apT.tCalm != null && apT.tClick != null ? Math.round(apT.tCalm - apT.tClick) : null;
      check('S2 ONE Approve press: calm "Approved" within 100 ms', ap.ok && ms != null && ms <= 100, J({ press: ap, ms }));
      const want2 = c => c.front && c.back && approvedish(c.front.state) && approvedish(c.back.state) && NORM(c.front.text) === 'ava' && NORM(c.back.text) === 'leo';
      const c = await settle(pk, want2);
      landInfo('S2 (Front "Ava" and Back "Leo", one press)', await landing(pk, tPress2, ['front', 'back'], { front: 'Ava', back: 'Leo' }));
      check('S2 committed: the front approved with "Ava" AND the back approved with "Leo", from the one press', !!c && want2(c), J(c));
      check('S2 one job per face, at most one sheet entry per face', c.counts.front <= 1 && c.counts.back <= 1 && ['front', 'back'].every(f => c.sheet.filter(e => e.face === f).length <= 1), J({ counts: c.counts, sheet: c.sheet }));
      await waitIdle(pk, 8000);
      await openPiece('stDuo');
      const s3 = await snap(pk);
      check('S2 the card shows both: Front & Back chosen, calm Approved, Front "Ava", Back "Leo", both engraved', s3.open && s3.choice === 'both' && !s3.dirty && s3.calm && s3.thumbs.front && s3.thumbs.front.eng && NORM(s3.thumbs.front.words) === 'ava' && s3.thumbs.back && s3.thumbs.back.eng && NORM(s3.thumbs.back.words) === 'leo', J(s3));
      await toList();
      const row = await rowStatus(made.stDuo.rid);
      check('S2 the list row shows both faces\' words and both approved', !!row && /\bava\b/.test(NORM(row.words)) && /\bleo\b/.test(NORM(row.words)) && row.s.filter(Boolean).length >= 2 && row.s.filter(Boolean).every(approvedish), J(row));
      const owOk = await page.evaluate(k => { if (!window.OrderWin || !OrderWin.open) return null; CN.setMode('orders'); OrderWin.open(k); return true; }, pk);
      if (owOk) {
        const ow = await until(() => page.evaluate(() => { const d = document.getElementById('orderWin'); if (!d) return null; const S = window.__stg, faceWords = f => [...d.querySelectorAll(`[data-eng-thumb="${f}"], .engFace[data-face="${f}"]`)].map(t => S.thumbWords(t)).join(' '); const v = { front: faceWords('front'), back: faceWords('back') }; return S.norm(v.front).includes('ava') && S.norm(v.back).includes('leo') ? v : null; }), 8000, 150)
          || await page.evaluate(() => { const d = document.getElementById('orderWin'), S = window.__stg; return d ? { front: [...d.querySelectorAll('[data-eng-thumb="front"], .engFace[data-face="front"]')].map(t => S.thumbWords(t)).join(' '), back: [...d.querySelectorAll('[data-eng-thumb="back"], .engFace[data-face="back"]')].map(t => S.thumbWords(t)).join(' ') } : null; });
        check('S2 the order window\'s thumbnails show both: Front "Ava", Back "Leo"', !!ow && NORM(ow.front).includes('ava') && NORM(ow.back).includes('leo'), J(ow));
        await page.evaluate(() => { try { OrderWin.close(); } catch (_) { /* none */ } CN.setMode('engrave'); });
      } else check('S2 the order window\'s thumbnails show both (the order window is not on this page: not checked)', true);
    }

    /* ════ S3 · item 3: a changed choice re-lights Approve; leaving discards it ════ */
    await phase('S3'); lap('S3');
    if (!want('S3')) { /* not asked for */ } else if (timeLeft() < 0) skipRest('S3'); else {
      const pk = pkOf('stKeep'), rid = made.stKeep.rid;
      await toList();
      const row0 = await rowStatus(rid), c0 = await committed(pk);
      await openPiece('stKeep');
      const s0 = await snap(pk);
      const nn = await page.evaluate(([pk]) => new Promise(res => { const S = window.__stg, el = S.target(pk, 'choice', 'none'), legacy = S.legacy; if (!el) return res({ found: false }); el.click(); const now = S.snap(pk); requestAnimationFrame(() => setTimeout(() => res({ found: true, legacy, now, frame: S.snap(pk) }), 0)); }), [pk]);
      const nf = nn.frame || {};
      check('S3 on the approved piece (calm Approved), None pressed: data-choice=none, one pressed option, Approve enabled within one frame', s0.calm && nn.found && !nn.legacy && nf.choice === 'none' && J(nf.pressed) === '["none"]' && nf.approve && nf.approve.enabled && !nf.calm && nf.dirty,
        J({ before: { calm: s0.calm, choice: s0.choice }, found: nn.found, legacy: nn.legacy, choice: nf.choice, pressed: nf.pressed, approve: nf.approve, calm: nf.calm, dirty: nf.dirty, sync: nn.now && { approve: nn.now.approve, choice: nn.now.choice } }));
      check('S3 None staged draws both thumbnails plain and the work area without words', nf.thumbs && nf.thumbs.back && !nf.thumbs.back.eng && !NORM(nf.thumbs.back.words) && nf.host && !NORM(nf.host.words), J({ thumbs: nf.thumbs, host: nf.host }));
      const nx = await press(pk, 'next');
      const left = await until(() => page.evaluate(pk => window.__stg.front() !== pk ? window.__stg.front() || 'nothing' : null, pk), 5000);
      const c1 = await committed(pk);
      check('S3 Next (no Approve) leaves the piece, and the change is discarded: the back still approved, the same approval, the same words', nx.ok && !!left && c1.back && approvedish(c1.back.state) && c1.back.approvedAt === c0.back.approvedAt && c1.back.text === c0.back.text && plainish(c1.front), J({ nx, left, before: c0.back, after: c1 }));
      const pv = await press(left === 'nothing' ? pk : left, 'prev');
      let back = await until(() => page.evaluate(pk => window.__stg.front() === pk, pk), 4000);
      const how = back ? '‹ Back' : 'reopened';
      if (!back) { await openPiece('stKeep'); back = true; }
      await page.waitForFunction(pk => { const S = window.__stg, p = S.panel(pk), sc = p && S.scope(pk); return !!(sc && sc.querySelector('.backHost canvas')); }, pk, { timeout: 8000 }).catch(() => {}); await frames(2);
      const s2 = await snap(pk);
      check(`S3 back on the piece (${how}): Back chosen, not dirty, calm Approved, the Back thumbnail "Ruby"`, s2.open && s2.choice === 'back' && J(s2.pressed) === '["back"]' && !s2.dirty && s2.calm && /Approved/i.test(s2.calm) && !(s2.approve && s2.approve.enabled) && s2.thumbs.back && s2.thumbs.back.eng && NORM(s2.thumbs.back.words) === 'ruby',
        J({ prev: pv, s2: { choice: s2.choice, pressed: s2.pressed, dirty: s2.dirty, calm: s2.calm, approve: s2.approve, thumbs: s2.thumbs } }));
      await toList();
      const row1 = await rowStatus(rid);
      check('S3 the list row\'s status reads as before (approved, its Engraved seal)', !!row0 && !!row1 && J([row0.s, row0.text, row0.seals]) === J([row1.s, row1.text, row1.seals]) && (row1.s.some(approvedish) || /approved|saved to sheet|engraved/i.test(row1.text.join(' ') + ' ' + row1.seals)), J({ row0, row1 }));
    }

    /* ════ S4 · item 1: one pressed option, the same white pill on each ════ */
    await phase('S4'); lap('S4');
    if (!want('S4')) { /* not asked for */ } else if (timeLeft() < 0) skipRest('S4'); else {
      const pk = pkOf('stUi');
      await openPiece('stUi');
      const seen = {}, presses = [];
      for (const c of ['front', 'back', 'none', 'both', 'back']) {
        const r = await press(pk, 'choice', c);
        await page.mouse.move(neutral.x, neutral.y);
        await page.evaluate(([pk, c]) => { const S = window.__stg, p = S.panel(pk), el = p && p.querySelector(`[data-engp="choice"][data-choice="${c}"]`); return Promise.all([S.calmAnims(p, 600), el ? S.settle(el, 600) : true]); }, [pk, c]);
        const v = await page.evaluate(([pk, c]) => { const S = window.__stg, p = S.panel(pk); if (!p) return null; const opts = [...p.querySelectorAll('[data-engp="choice"][data-choice]')], el = opts.find(o => o.dataset.choice === c); return { pressed: opts.filter(o => o.getAttribute('aria-pressed') === 'true').map(o => o.dataset.choice), bg: el ? S.effBg(el) : null, own: el ? { bg: getComputedStyle(el).backgroundColor, shadow: getComputedStyle(el).boxShadow } : null }; }, [pk, c]);
        presses.push({ c, ok: r.ok, legacy: r.legacy, pressed: v && v.pressed });
        if (v && !seen[c]) { v.seen = r.ok && !r.legacy ? await seenColour(pk, c) : null; seen[c] = v; }
      }
      check('S4 each option clicked gives exactly one aria-pressed="true", the one clicked (Front, Back, None, Front & Back, Back)', presses.every(p => p.ok && !p.legacy && J(p.pressed) === J([p.c])), J(presses));
      const look = c => seen[c] && seen[c].seen;
      const white = b => !!b && b.every(x => x >= 235);
      const same = (a, b) => !!a && !!b && a.every((x, i) => Math.abs(x - b[i]) <= 3);
      check('S4 the selected option is a white pill whichever it is (Front, Back, None, Front & Back): the colour seen inside it', ['front', 'back', 'none', 'both'].every(c => white(look(c))), J(Object.fromEntries(['front', 'back', 'none', 'both'].map(c => [c, { seen: look(c), computed: seen[c] && seen[c].bg }]))));
      check('S4 the selected pill looks the same on Back as on Front, None and Front & Back (the colour seen, and the same computed background and shadow)', same(look('front'), look('back')) && same(look('front'), look('none')) && same(look('front'), look('both')) && ['back', 'none', 'both'].every(c => seen[c] && seen.front && J(seen[c].bg && [seen[c].bg.rgb, seen[c].bg.shadow]) === J(seen.front.bg && [seen.front.bg.rgb, seen.front.bg.shadow])), J(Object.fromEntries(['front', 'back', 'none', 'both'].map(c => [c, seen[c] && { seen: seen[c].seen, bg: seen[c].bg }]))));
      if (w.shots) await R.shot(w, 'stage-S4').catch(() => {});
    }

    /* ════ S5 · typing on a plain face adds it ════ */
    await phase('S5'); lap('S5');
    if (!want('S5')) { /* not asked for */ } else if (timeLeft() < 0) skipRest('S5'); else {
      const pk = pkOf('stUi');
      await toList(); await openPiece('stUi');
      const s0 = await snap(pk);
      const tf = await press(pk, 'thumb', 'front'); await frames(1);
      const s1 = await snap(pk);
      check('S5 choice Back, the Front thumbnail clicked: the front is active, the choice still Back, the words box empty with a "Front words" placeholder, the front still plain',
        s0.choice === 'back' && tf.ok && !tf.legacy && s1.active === 'front' && s1.choice === 'back' && s1.box === '' && /front/i.test(s1.placeholder || '') && s1.thumbs.front && !s1.thumbs.front.eng, J({ s0: { choice: s0.choice }, tf, s1: { active: s1.active, choice: s1.choice, box: s1.box, placeholder: s1.placeholder, thumbs: s1.thumbs } }));
      const ty = await typeInto(pk, 'J');
      const s2 = await snap(pk);
      check('S5 the first letter typed on the plain front makes the choice Front & Back: both engraved, the Front "J", the Back still "Ruby", the Front & Back option pressed',
        ty.ok && s2.choice === 'both' && J(s2.pressed) === '["both"]' && s2.thumbs.front && s2.thumbs.front.eng && NORM(s2.thumbs.front.words) === 'j' && s2.thumbs.back && s2.thumbs.back.eng && NORM(s2.thumbs.back.words) === 'ruby', J({ choice: s2.choice, pressed: s2.pressed, thumbs: s2.thumbs, inputs: ty.inputs }));
    }

    /* ════ S6 · an added face with empty words: Approve disabled, nothing explained (the lead's decision, 30 Oct: Front & Back on the Ruby piece adds an EMPTY front) ════ */
    await phase('S6'); lap('S6');
    if (!want('S6')) { /* not asked for */ } else if (timeLeft() < 0) skipRest('S6'); else {
      const pk = pkOf('stUi');
      await toList(); await openPiece('stUi');
      const lines = () => page.evaluate(pk => { const sc = window.__stg.scope(pk); return sc ? (sc.innerText || '').split('\n').map(x => x.replace(/\s+/g, ' ').trim()).filter(Boolean) : []; }, pk);
      const l0 = await lines();
      const f = await press(pk, 'choice', 'both'); await frames(2);
      const s1 = await snap(pk), l1 = await lines();
      const added = l1.filter(x => !l0.includes(x) && (x.match(/[A-Za-z]{2,}/g) || []).length >= 3);
      check('S6 Front & Back on the Ruby piece adds an EMPTY front: data-choice=both, the Front thumbnail engraved with no words, the Back still "Ruby"; Approve there but disabled, no calm Approved',
        f.ok && !f.legacy && s1.choice === 'both' && s1.thumbs.front && s1.thumbs.front.eng && !NORM(s1.thumbs.front.words) && s1.thumbs.back && NORM(s1.thumbs.back.words) === 'ruby' && s1.approve && !s1.approve.enabled && !s1.calm,
        J({ f, choice: s1.choice, active: s1.active, box: s1.box, thumbs: s1.thumbs, approve: s1.approve, calm: s1.calm }));
      check('S6 no explanatory text appears (no new sentence in the card, no forbidden text)', !added.length && !FORBIDDEN.some(x => l1.join(' ').toLowerCase().includes(x.toLowerCase())), J(added));
      // a press on the disabled Approve anyway (as Paul pressed it): nothing happens: the card keeps drawing the same face, says nothing, and nothing is committed
      const c0 = await committed(pk), face0 = s1.host && s1.host.face;
      await page.evaluate(pk => window.__stg.sample(pk, 1500), pk);
      const h = await page.evaluateHandle(pk => { const sc = window.__stg.scope(pk); return sc && [...sc.querySelectorAll('[data-a="approve"]')].find(window.__stg.vis); }, pk);
      const ae = h.asElement();
      // (a disabled button gets no click event, so the press is marked by hand just before it)
      await page.evaluate(() => { const o = window.__stg.samples; if (o && o.tClick == null) o.tClick = performance.now(); });
      if (ae) await ae.click({ force: true, timeout: 3000 }).catch(() => {});
      await page.waitForFunction(() => window.__stg.samples.done, null, { timeout: 8000 }).catch(() => {});
      const sm = (await page.evaluate(() => window.__stg.samples.list)).filter(x => x.after);
      const c1 = await settle(pk, c => J([c.front, c.back]) === J([c0.front, c0.back]), 6000);
      const fine = x => x.open && x.canvas && x.face === face0 && x.choice === 'both' && !x.forb.length;
      check('S6 pressing the disabled Approve anyway changes nothing: the same face drawn, no forbidden text, nothing committed', !!ae && sm.length > 0 && sm.every(fine) && J([c1.front, c1.back]) === J([c0.front, c0.back]),
        J({ pressed: !!ae, face0, odd: sm.filter(x => !fine(x)).slice(0, 3), before: { front: c0.front, back: c0.back && c0.back.state }, after: { front: c1.front, back: c1.back && c1.back.state } }));
      // then a front word typed lights Approve (staged only; the piece is left, so nothing is kept)
      const tf = await press(pk, 'thumb', 'front'); const ty = await typeInto(pk, 'Zoe');
      const s2 = await snap(pk);
      check('S6 a word typed on the empty front lights Approve', tf.ok && ty.ok && s2.choice === 'both' && s2.active === 'front' && NORM(s2.box) === 'zoe' && s2.approve && s2.approve.enabled && !s2.calm, J({ tf, choice: s2.choice, active: s2.active, box: s2.box, approve: s2.approve, calm: s2.calm }));
    }

    /* ════ S8 · keystroke to paint ════ */
    await phase('S8'); lap('S8');
    if (!want('S8')) { /* not asked for */ } else if (timeLeft() < 0) skipRest('S8'); else {
      const pk = pkOf('stUi');
      await toList(); await openPiece('stUi');
      await press(pk, 'words');
      const latR = await page.evaluate(latency, { pk, n: 30 }).catch(() => null);
      const lat = latR ? Object.assign(latR.list, { frame: latR.frame }) : [];
      const th = lat.map(x => x.thumb == null ? Infinity : x.thumb), ho = lat.map(x => x.host == null ? Infinity : x.host), both = lat.map((x, i) => Math.max(th[i], ho[i]));
      const mT = median(th), mH = median(ho), mB = median(both);
      const mS = median(lat.map(x => x.sync == null ? Infinity : x.sync));
      check(`S8 the median keystroke-to-paint over 30 keys is <= 16 ms (thumbnail ${mT} ms, work area ${mH} ms, both ${mB} ms)`, lat.length === 30 && mB <= 16, J({ medianThumb: mT, medianHost: mH, medianInputHandler: mS, medianFrameNow: lat.frame && Math.round(lat.frame * 10) / 10, load: require('os').loadavg().map(x => Math.round(x * 10) / 10), missingThumb: lat.filter(x => x.thumb == null).length, missingHost: lat.filter(x => x.host == null).length, sample: lat.slice(0, 6) }));
    }

    /* ════ S10 · hover, focus and pointer ════ */
    await phase('S10'); lap('S10');
    if (!want('S10')) { /* not asked for */ } else if (timeLeft() < 0) skipRest('S10'); else {
      const pk = pkOf('stUi');
      await toList(); await openPiece('stUi');
      await typeInto(pk, 'Z');   // (Approve lit)
      const hoverMedia = await page.evaluate(() => matchMedia('(hover: hover)').matches);
      const s0 = await snap(pk);
      const ctl = [['Front thumbnail', 'thumb', 'front', 1], ['Back thumbnail', 'thumb', 'back', 1]].concat((s0.options.length ? s0.options : ['none', 'front', 'back', 'both']).map(c => [`option ${c}`, 'choice', c, 1]))
        .concat([['words box', 'words', null, 0], ['Aα', 'alpha', null, 1], ['Emoji', 'emoji', null, 1], ['font picker', 'font', null, 1], ['Approve', 'approve', null, 1]]);
      const hov = [], foc = [];
      for (const [name, kind, arg, clicky] of ctl) {
        const h = await page.evaluateHandle(([pk, kind, arg]) => window.__stg.target(pk, kind, arg), [pk, kind, arg]);
        const el = h.asElement(), legacy = await page.evaluate(() => !!window.__stg.legacy);
        if (!el) { hov.push({ name, missing: true }); foc.push({ name, missing: true }); continue; }   // (an old control found in place of the spec's hook is still measured, and fails as "legacy")
        await page.mouse.move(neutral.x, neutral.y); await page.evaluate(e => window.__stg.settle(e, 600), el);
        const rest = await page.evaluate(e => window.__stg.styles(e), el);
        let hovered = null, hoverErr = null; try { await el.hover({ timeout: 3000 }); await page.evaluate(e => window.__stg.settle(e, 600), el); hovered = await page.evaluate(e => window.__stg.styles(e), el); } catch (e) { hovered = null; hoverErr = String(e.message).split('\n')[0].slice(0, 120); }
        const changed = !!hovered && hovered.some((x, i) => rest[i] && (x.bg !== rest[i].bg || x.bc !== rest[i].bc || x.bs !== rest[i].bs));
        const pointer = !!hovered && (hovered[0].cursor === 'pointer' || hovered.slice(1).some(x => x.cursor === 'pointer'));
        hov.push({ name, clicky, legacy, changed, pointer, rest: rest[0], hov: hovered && hovered[0], hoverErr });
        // keyboard focus: a real Tab from the tabbable element before it (a control out of the tab order is focused by script right after that Tab)
        await page.mouse.move(neutral.x, neutral.y);
        const base = await page.evaluate(e => { const S = window.__stg, ft = S.focusTarget(e); if (document.activeElement && document.activeElement.blur) document.activeElement.blur(); return S.styles(ft)[0]; }, el);
        const bf = await page.evaluate(e => window.__stg.focusBefore(window.__stg.focusTarget(e)), el);
        await page.keyboard.press('Tab');
        const fr = await page.evaluate(async e => { const S = window.__stg, ft = S.focusTarget(e); let how = 'Tab'; if (!(document.activeElement === ft || e.contains(document.activeElement))) { how = 'script after Tab'; ft.focus({ preventScroll: true }); } await S.settle(document.activeElement, 600); const a = document.activeElement; return { how, on: a === ft || e.contains(a), fv: a.matches(':focus-visible'), st: S.styles(a)[0], self: S.styles(e)[0] }; }, el);
        const ring = x => x && ((x.os !== 'none' && x.ow >= 2 && (S_alpha(x.oc) > 0.1)) || (x.bs !== 'none' && x.bs !== base.bs));
        foc.push({ name, legacy, how: fr.how, before: bf.prev, on: fr.on, fv: fr.fv, ring: fr.on && (ring(fr.st) || ring(fr.self)), outline: fr.st.outline, bs: fr.st.bs });
        await page.evaluate(() => { if (document.activeElement && document.activeElement.blur) document.activeElement.blur(); });
      }
      const hb = hov.filter(x => x.missing || x.legacy || !x.changed), pb = hov.filter(x => x.clicky && (x.missing || x.legacy || !x.pointer)), fb = foc.filter(x => x.missing || x.legacy || !x.ring);
      const why = x => (x.missing ? 'missing' : (x.legacy ? 'no spec hook (old control): ' : '') + (x.hoverErr ? 'hover failed: ' + x.hoverErr : x.changed === undefined ? '' : x.changed ? 'hover changes' : `hover changes nothing (bg ${x.rest && x.rest.bg}, border ${x.rest && x.rest.bc.split(' rgb')[0]}, shadow ${x.rest && x.rest.bs}; :hover ${x.hov && x.hov.hover}; ${x.hov && x.hov.attrs})`));
      const fwhy = x => (x.missing ? 'missing' : (x.legacy ? 'no spec hook (old control): ' : '') + `${x.ring ? 'ring' : 'NO ring'} via ${x.how}${x.on ? '' : ' (focus did not land)'}, focus-visible ${x.fv}, outline ${x.outline}, shadow ${x.bs}`);
      check(`S10 hover changes the background, border or shadow of every left-column control and of the lit Approve (hover:hover ${hoverMedia})`, !hb.length, hb.map(x => `${x.name}: ${why(x)}`).join(' | '));
      check('S10 keyboard focus (Tab) shows a visible ring on every one (outline >= 2 px, or a box-shadow ring)', !fb.length, fb.map(x => `${x.name}: ${fwhy(x)}`).join(' | '));
      check('S10 the cursor is a pointer on the clickable ones (thumbnails, options, Aα, Emoji, font picker, Approve)', !pb.length, pb.map(x => `${x.name}: ${x.missing ? 'missing' : (x.legacy ? 'no spec hook (old control), ' : '') + 'cursor ' + (x.hov && x.hov.cursor)}`).join(' | '));
      // a disabled Approve (Front & Back with an empty front, as S6): no hover effect, no pointer
      const f = await press(pk, 'choice', 'both'); await frames(1);
      const dis = await page.evaluateHandle(pk => { const sc = window.__stg.scope(pk); return sc && [...sc.querySelectorAll('[data-a="approve"]')].find(b => window.__stg.vis(b) && (b.disabled || b.getAttribute('aria-disabled') === 'true')); }, pk);
      const de = dis.asElement();
      if (de) {
        await page.mouse.move(neutral.x, neutral.y); await page.evaluate(e => window.__stg.settle(e, 600), de);
        const r0 = await page.evaluate(e => window.__stg.styles(e)[0], de); await de.hover({ timeout: 3000, force: true }).catch(() => {}); await page.evaluate(e => window.__stg.settle(e, 600), de);
        const r1 = await page.evaluate(e => window.__stg.styles(e)[0], de);
        check('S10 a disabled Approve has no hover effect, no pointer cursor, and reduced opacity', r1.cursor !== 'pointer' && r0.bg === r1.bg && r0.bc === r1.bc && r0.bs === r1.bs && parseFloat(r0.opacity) < 1, J({ rest: r0, hover: r1 }));
      } else check('S10 a disabled Approve has no hover effect, no pointer cursor, and reduced opacity', false, 'no disabled Approve after Front & Back with an empty front: ' + J({ f, approve: (await snap(pk)).approve }));
      await page.mouse.move(neutral.x, neutral.y);
    }

    /* ════ S7 · the fuzz ════ */
    // (None + Approve makes the piece a regular order with no card, as after any None + Approve today: seeds 1 and 3 never press a lit Approve with None staged, so their 60 actions all land; seed 2 may, and then ends there)
    const seeds = [{ seed: 1, name: 'stFz1', how: 'real input', lastApprove: true, keepCard: true }, { seed: 2, name: 'stFz2', how: 'in-page burst', lastApprove: false }, { seed: 3, name: 'both', how: 'real input', lastApprove: true, keepCard: true }];
    for (const sd of seeds) {
      const tag = `S7 seed ${sd.seed} (${sd.how}, ${{ stFz1: 'approved back piece', stFz2: 'back piece in review', both: 'two-face piece in review' }[sd.name]})`;
      await phase('S7.' + sd.seed);
      if (timeLeft() < 15000) { skipRest(tag); continue; }
      if (!want('S7')) continue;
      lap(tag);
      const pk = pkOf(sd.name), errs0 = errors.length, acts = fuzzActions(1000 + sd.seed * 7919, 60, sd.lastApprove);
      await toList(); await openPiece(sd.name);
      let log = []; const tS = Date.now();
      if (sd.how === 'in-page burst') log = await page.evaluate(burst, { pk, acts });
      else {
        for (const a of acts) {
          if (Date.now() - tS > 30000 || timeLeft() < 5000) { log.push({ k: 'cut', why: 'the actions took over 30 s: cut short' }); break; }
          const rec = { k: a.k, arg: a.arg, t: Date.now() - tS };
          const info = await page.evaluate(([pk, a]) => {
            const S = window.__stg, p = S.panel(pk); if (!p || !S.vis(p)) return { gone: true, on: S.front() };
            const kind = a.k === 'key' || a.k === 'bs' ? 'words' : a.k, el = S.target(pk, kind, a.arg); if (!el) return { missing: true };
            if (el.scrollIntoViewIfNeeded) el.scrollIntoViewIfNeeded(false);
            const r = el.getBoundingClientRect(), enabled = !el.disabled && el.getAttribute('aria-disabled') !== 'true';
            const hx = r.left + r.width / 2, hy = r.top + Math.min(r.height / 2, 18), top = document.elementFromPoint(hx, hy), hit = !!top && (el === top || el.contains(top)); const over = hit ? null : top ? (top.tagName + '.' + String(top.className || '').split(' ').slice(0, 2).join('.')).slice(0, 60) : 'nothing';
            return { x: hx, y: hy, hit, over, enabled, focused: document.activeElement === el, staged: a.k === 'approve' ? S.staged(pk) : undefined, legacy: S.legacy };
          }, [pk, a]);
          if (info.gone) {
            if (!(await page.evaluate(pk => Engrave.jobsOf(B.orders.byKey.get(pk.split('#')[0])).some(x => x.state !== 'none'), pk))) { log.push({ k: 'ended', why: 'the piece is a regular order now (None approved): no card to go back to' }); break; }
            rec.reopen = info.on; await openPiece(sd.name); log.push(rec); continue;
          }
          if (info.missing || ((a.k === 'key' || a.k === 'bs') && !info.enabled)) { rec.missing = true; log.push(rec); continue; }
          if (!info.hit) rec.blocked = info.over;   // (something else is on top where the mouse presses: the press does not reach the control)
          if (a.k === 'approve') { rec.enabled = info.enabled && info.hit; rec.staged = info.staged; if (sd.keepCard && info.staged && info.staged.choice === 'none') { rec.enabled = false; rec.skipped = 'None staged'; log.push(rec); continue; } }
          rec.x = Math.round(info.x); rec.y = Math.round(info.y); rec.focused = info.focused;
          rec.at = Date.now();
          if (a.k === 'key' || a.k === 'bs') { if (!info.focused) await page.mouse.click(info.x, info.y); await page.keyboard.press(a.k === 'bs' ? 'Backspace' : a.arg); }
          else await page.mouse.click(info.x, info.y);
          log.push(rec);
        }
        const lp = [...log].reverse().find(a => a.k === 'approve' && a.enabled), le = log.reduce((m, a, i) => (['choice', 'key', 'bs'].includes(a.k) && !a.missing ? i : m), -1);
        if (lp && log.indexOf(lp) > le && lp.staged && lp.staged.faces.length) landInfo(`${tag}`, await landing(pk, lp.at, lp.staged.faces, lp.staged.words));
      }
      const idle = await waitIdle(pk, 20000), stillChanging = waitIdle.changing, stillLanding = waitIdle.landing;   // (20 s: a queued second commit re-fits and re-approves; past that it is a stall)
      const hasCard = await page.evaluate(pk => Engrave.jobsOf(B.orders.byKey.get(pk.split('#')[0])).some(x => x.state !== 'none'), pk);
      if (hasCard && !(await page.evaluate(pk => !!window.__stg.panel(pk), pk))) { await openPiece(sd.name); await waitIdle(pk, 5000); }
      const s = await snap(pk), c = await committed(pk), stg = await page.evaluate(pk => window.__stg.staged(pk), pk);
      const fz = s.choice === 'both' ? ['front', 'back'] : s.choice === 'front' || s.choice === 'back' ? [s.choice] : [];
      const errs = errors.slice(errs0), cut = log.find(a => a.k === 'cut'), ended = log.find(a => a.k === 'ended');
      const lastEdit = log.reduce((m, a, i) => (['choice', 'key', 'bs'].includes(a.k) && !a.missing ? i : m), -1), lastAp = log.reduce((m, a, i) => (a.k === 'approve' && a.enabled ? i : m), -1);
      const must = lastAp > lastEdit, pressAt = lastAp >= 0 ? log[lastAp].staged : null;
      const cFaces = c ? ['front', 'back'].filter(f => c[f] && !plainish(c[f])) : [];
      if (cut) check(`${tag}: the 60 actions ran`, false, cut.why);
      check(`${tag}: no page errors`, !errs.length, errs.join(' | ').slice(0, 400));
      check(`${tag}: idle reached (no busy state, nothing at work)`, idle, J({ stillChanging, stillLanding, busy: s.busy, busyWhat: s.busyWhat, host: s.host, working: c && c.working, approve: s.approve, calm: s.calm, dirty: s.dirty, jobs: c && [c.front && c.front.state, c.back && c.back.state] }));
      if (!hasCard) {
        // (None was approved during the run: the piece is a regular order, in no list and with no card, as after any None + Approve; what it holds must be exactly that)
        const ok = cFaces.length === 0 && !!pressAt && pressAt.choice === 'none';
        check(`${tag}: ended as a regular order (None approved, after action ${log.findIndex(a => a.k === 'ended')}): no card, no engraved face, the last lit Approve had None staged`, ok, J({ ended: !!ended, pressAt, committed: c && { front: c.front, back: c.back } }));
      } else {
        const uiOk = s.open && s.choice != null && J(s.pressed) === J([s.choice]) && s.active != null && s.thumbs[s.active] && s.thumbs[s.active].cur && ['front', 'back'].every(f => !s.thumbs[f] || (!!s.thumbs[f].eng === fz.includes(f)) && (f === s.active || !s.thumbs[f].cur)) && s.host && s.host.face === s.active;
        check(`${tag}: the UI equals its own data-choice / data-active (one pressed option, the current thumbnail, data-engraved, the work area's face)`, uiOk, J({ choice: s.choice, active: s.active, pressed: s.pressed, thumbs: s.thumbs, host: s.host }));
        // (the active face engraved: its thumbnail and the work area show the box's words; a plain active face (the other one engraved): the box is empty, nothing drawn; with None nothing is drawn, whatever the box holds)
        const activeEng = fz.includes(s.active);
        const wordsOk = s.open && s.box != null && !!s.thumbs[s.active] && !!s.host && (activeEng ? NORM(s.thumbs[s.active].words) === NORM(s.box) && NORM(s.host.words) === NORM(s.box) : !NORM(s.thumbs[s.active].words) && !NORM(s.host.words) && (s.choice === 'none' || s.box === '')) && ['front', 'back'].every(f => fz.includes(f) || !s.thumbs[f] || !NORM(s.thumbs[f].words));
        check(`${tag}: the thumbnails and the work area match the words box`, wordsOk, J({ box: s.box, active: s.active, choice: s.choice, thumbs: s.thumbs, host: s.host }));
        const allApproved = cFaces.length > 0 && cFaces.every(f => approvedish(c[f].state));
        const apOk = s.dirty ? !s.calm && !!s.approve && s.approve.enabled === (s.choice === 'none' || (fz.length > 0 && fz.every(f => NORM(stg && stg.words[f])))) : (allApproved ? !(s.approve && s.approve.enabled) && !!s.calm : true);
        check(`${tag}: Approve says what the state is (lit only when dirty with words on every engraved face, or None; calm Approved when nothing is staged on an approved piece)`, s.open && s.choice != null && apOk, J({ dirty: s.dirty, approve: s.approve, calm: s.calm, staged: stg, committed: c && [c.front && c.front.state, c.back && c.back.state] }));
        const eqWords = !!c && !!stg && J(cFaces) === J(stg.faces) && stg.faces.every(f => c[f] && NORM(c[f].text) === NORM(stg.words[f]));
        // (after a last lit press, the jobs must hold what was staged AT THE PRESS (the spec: "after Approve settles the committed state equals what was staged at the press"), not merely what the card shows now: a card that threw the staged state away looks consistent)
        const eqPress = !!c && !!pressAt && J(cFaces) === J(pressAt.faces) && pressAt.faces.every(f => c[f] && NORM(c[f].text) === NORM(pressAt.words[f]) && approvedish(c[f].state));
        const eq = must ? eqPress && eqWords : eqWords && stg.faces.every(f => approvedish(c[f].state));
        check(`${tag}: ${must ? 'Approve was pressed after the last edit: committed equals what was staged at that press (faces approved, words), and the card shows it, not dirty' : 'not dirty means committed equals staged (faces, words)'}`, must ? eq && !s.dirty : (s.dirty || eqWords),
          J({ lastEdit, lastApprove: lastAp, dirty: s.dirty, approve: s.approve, calm: s.calm, staged: stg, committed: c && { front: c.front, back: c.back }, pressAt }));
      }
      const dup = !c || c.counts.front > 1 || c.counts.back > 1 || ['front', 'back'].some(f => c.sheet.filter(e => e.face === f).length > 1) || c.sheet.some(e => plainish(c[e.face]));
      check(`${tag}: no duplicate job for a face, no duplicate or orphan sheet entry`, !dup, J(c && { counts: c.counts, sheet: c.sheet, front: c.front && c.front.state, back: c.back && c.back.state }));
      if (process.env.STAGE_RECORD) { try { const fs = require('fs'), path = require('path'); fs.mkdirSync(process.env.STAGE_RECORD, { recursive: true }); const f = path.join(process.env.STAGE_RECORD, `seed${sd.seed}-${Date.now()}.json`); fs.writeFileSync(f, JSON.stringify({ seed: sd.seed, piece: sd.name, how: sd.how, keepCard: !!sd.keepCard, pk, acts, log, end: { snap: s, committed: c } }, null, 1)); console.log('   recorded ' + f); } catch (e) { console.log('   (record failed: ' + e.message + ')'); } }
      console.log(`   ${tag}: ${log.length} actions (${log.filter(a => a.k === 'approve' && a.enabled).length} lit Approve presses, ${log.filter(a => a.blocked).length} presses covered by something else${log.some(a => a.blocked) ? ' (' + [...new Set(log.filter(a => a.blocked).map(a => a.blocked))].slice(0, 3).join(', ') + ')' : ''}, ${log.filter(a => a.skipped).length} skipped with None staged, ${log.filter(a => a.reopen).length} reopenings, ${log.filter(a => a.missing).length} missing targets), ${Math.round((Date.now() - T0) / 1000)} s`);
    }

    /* ════ S9 · forbidden texts, the whole run ════ */
    const seen = await page.evaluate(() => window.__stg.seen);
    check('S9 none of the forbidden texts was ever on screen in the Engrave tab or the order window ("Cut it plain", "Keep it as it is", "already approved", "Words to confirm", "Engrave these words", "gives no words", "being prepared", "Approve to make this a regular order")', !Object.keys(seen).length, J(seen));
    check('S9 no page errors in the whole run', errors.length === 0, errors.slice(0, 5).join(' | ').slice(0, 500));
  } catch (e) { console.error(e); check('ran', false, String(e && e.stack || e)); }
  finally { await w.close(); }
  const bad = RESULTS.filter(r => !r.ok);
  if (INFOS.length) console.log(`(${INFOS.length} report-only INFO lines above)`);
  console.log(`engrave-stage-flow: ${RESULTS.length - bad.length}/${RESULTS.length} passed in ${Math.round((Date.now() - T0) / 1000)} s` + (bad.length ? `, ${bad.length} failed` : ''));
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });

function S_alpha(c) { const m = /rgba?\(([^)]+)\)/.exec(c || ''); if (!m) { const n = /color\(srgb ([^)]+)\)/.exec(c || ''); if (!n) return 0; const q = n[1].split(/[\s/]+/).filter(Boolean).map(Number); return q.length > 3 ? q[3] : 1; } const q = m[1].split(/[\s,/]+/).filter(Boolean).map(Number); return q.length > 3 ? q[3] : 1; }
