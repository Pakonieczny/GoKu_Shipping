// ENGSTAGE repro (A): the lost face approval. A one-press Approve with Front & Back staged ends with one face approved and the other back in "review"
// (approvedAt 0), the card calm and not dirty (engrave-stage-flow.cjs S7 seeds 1 and 3, runs on a081dd4 / 18be30d).
//
// It replays the fuzz of engrave-stage-flow.cjs (the same seeded actions, the same real-input executor), or a recording that suite wrote (STAGE_RECORD=<dir>), on fresh
// copies of the piece, with a TRACER installed in the page before the actions: every write to a job's state / approvedAt / approvedBy / text / fit (and to fit.centre, the
// placement) is logged with a timestamp and the top 6 stack frames, and so is every call into the Engrave API from outside the bridge (moveTo, approve, draftFit,
// saveSetup, commitStage, decideWords, showStage, showFace ...) and every press of Approve. When the end state shows the loss, it prints the trace of the job that lost its
// approval from the last lit press onwards, and writes the whole trace as JSON.
//
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/engrave-stage-repro.cjs                   (seeds 1 and 3, up to 3 fresh copies each)
//   REPRO_SEEDS=3 REPRO_TRIES=5 node tests/charm-nest/engrave-stage-repro.cjs
//   node tests/charm-nest/engrave-stage-repro.cjs <recording.json>                                                      (one recorded run, its timings kept)
//   REPRO_OUT=<dir> for the trace files (default /tmp/engrave-stage-repro). Exit 0 when it reproduced (the trace is the product), 2 when it did not.
'use strict';
const fs = require('fs'), path = require('path');
const R = require('./_engraveRealWorld.cjs');
const K = require('./engrave-stage-flow.cjs');
const OUT = process.env.REPRO_OUT || '/tmp/engrave-stage-repro';
const J = x => { try { return JSON.stringify(x); } catch (_) { return String(x); } };

/* the tracer, in the page: property traps on the piece's jobs (and on each fit's centre), wrappers on the Engrave API, the Approve presses */
function installTracer(lineKey) {
  const T = window.__trace = { lines: [], t0: performance.now(), lineKey };
  const now = () => Math.round((performance.now() - T.t0) * 10) / 10;
  const frames = () => (new Error().stack || '').split('\n').slice(3, 9).map(x => x.trim().replace(/^at /, '').replace(/https?:\/\/[^/]+\//g, '').replace(/\?[^:)]*/g, '')).join(' < ');
  const sum = (f, v) => {
    if (v == null) return v;
    if (f === 'fit') return v && typeof v === 'object' ? { centre: v.centre ? v.centre.map(n => Math.round(n * 1000) / 1000) : null, glyphs: (v.glyphs || []).length, id: v.__tid || (v.__tid = Math.random().toString(36).slice(2, 7)) } : v;
    if (typeof v === 'string') return v.slice(0, 40);
    return v;
  };
  const wrapCentre = (j, fit) => {
    if (!fit || typeof fit !== 'object' || fit.__tcentre) return;
    try {
      let c = fit.centre; Object.defineProperty(fit, '__tcentre', { value: true });
      Object.defineProperty(fit, 'centre', { configurable: true, enumerable: true, get() { return c; }, set(nv) { T.lines.push({ t: now(), key: j.key, f: 'fit.centre', from: c && c.map(n => Math.round(n * 1000) / 1000), to: nv && nv.map(n => Math.round(n * 1000) / 1000), st: frames() }); c = nv; } });
    } catch (_) { /* a frozen fit */ }
  };
  const FIELDS = ['state', 'approvedAt', 'approvedBy', 'text', 'fit', 'lines', 'reason'];
  const wrap = j => {
    if (!j || typeof j !== 'object' || j.__traced) return;
    Object.defineProperty(j, '__traced', { value: true });
    for (const f of FIELDS) {
      let v = j[f];
      Object.defineProperty(j, f, { configurable: true, enumerable: true, get() { return v; }, set(nv) {
        if (nv !== v && !(f === 'lines' && J2(nv) === J2(v))) T.lines.push({ t: now(), key: j.key, f, from: sum(f, v), to: sum(f, nv), st: frames() });
        v = nv; if (f === 'fit') wrapCentre(j, nv);
      } });
    }
    wrapCentre(j, j.fit);
  };
  const J2 = x => { try { return JSON.stringify(x); } catch (_) { return String(x); } };
  const mine = j => j && typeof j.key === 'string' && j.key.startsWith(lineKey);
  for (const j of Engrave.items().values()) if (mine(j)) wrap(j);
  // (a job made later, the front's at a commit, is traced from the moment it is put in the list)
  const m = Engrave.items();
  if (!m.__tset) { const set = m.set.bind(m); m.set = (k, j) => { if (mine(j)) { wrap(j); T.lines.push({ t: now(), key: j.key, f: '(job made)', to: j.state, st: frames() }); } return set(k, j); }; m.__tset = true; }
  // calls into the Engrave API from outside the bridge (the panel, the card): name, first argument's key, time
  for (const name of ['moveTo', 'approve', 'draftFit', 'saveSetup', 'commitStage', 'decideWords', 'showFace', 'fitJob', 'nudge', 'stagePos', 'approveRow']) {   // (showStage / stageNow are paints and reads, called every frame: left out)
    const f = Engrave[name]; if (typeof f !== 'function' || f.__traced) continue;
    const w = function (...a) { const k = a[0] && (a[0].key || (typeof a[0] === 'string' ? a[0] : null) || (a[0].stage && a[0].stage.key)); if (!k || String(k).startsWith(lineKey)) T.lines.push({ t: now(), call: 'Engrave.' + name, arg: k || null, st: frames() }); return f.apply(this, a); };
    w.__traced = true; Engrave[name] = w;
  }
  const P = window.CharmNestEngravePanel;
  if (P) for (const name of ['stagePos', 'commit', 'restore']) { const f = P[name]; if (typeof f === 'function' && !f.__traced) { const w = function (...a) { T.lines.push({ t: now(), call: 'Panel.' + name, arg: typeof a[0] === 'string' ? a[0] : null, st: frames() }); return f.apply(this, a); }; w.__traced = true; P[name] = w; } }
  document.addEventListener('click', e => { const b = e.target && e.target.closest && e.target.closest('[data-a="approve"],[data-engp="choice"],[data-eng-thumb]'); if (!b) return; const p = document.querySelector('.engp[data-key]'); T.lines.push({ t: now(), press: b.dataset.a ? b.dataset.a : b.dataset.engThumb ? 'thumb:' + b.dataset.engThumb : 'choice:' + b.dataset.choice, disabled: !!b.disabled, choice: p && p.dataset.choice, dirty: p && p.dataset.dirty === '1' }); }, true);
  return true;
}

(async () => {
  const rec = process.argv[2] ? JSON.parse(fs.readFileSync(process.argv[2], 'utf8')) : null;
  const seeds = rec ? [rec.seed] : (process.env.REPRO_SEEDS || '1,3').split(',').map(Number);
  const tries = rec ? 1 : +(process.env.REPRO_TRIES || 3);
  // copies of the two fuzz pieces: "Ruby" COMET backs (approved first, like stFz1) and the two-face SCHRODINGER CAT piece (in review, like "both")
  const names = [];
  for (const sd of seeds) for (let i = 0; i < tries; i++) {
    const n = `rp${sd}_${i}`, rid = String(4290000400 + sd * 10 + i);
    if (sd === 3) R.ORDERS[n] = Object.assign({}, R.ORDERS.both, { rid, listing: '18' + rid.slice(2), buyer: 'Name Repro ' + n });
    else R.ORDERS[n] = Object.assign({}, R.ORDERS.back, { rid, listing: '18' + rid.slice(2), buyer: 'Name Repro ' + n, personalization: ['Ruby'] });
    R.EXPECT_JOBS[n] = sd === 3 ? 2 : 1; names.push([sd, n]);
  }
  const w = await R.world({ shots: OUT });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page, errors = [];
  page.on('pageerror', e => errors.push(e.message));
  await page.waitForFunction(() => !window.CNRole || ['ask', 'role', 'admin'].includes(CNRole.state()), null, { timeout: 20000 }).catch(() => {}).then(() => page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('design'); } catch (_) { /* none */ } }));
  let found = 0;
  try {
    const made = await R.build(w, names.map(x => x[1]));
    await page.evaluate(K.installProbe, K.FORBIDDEN);
    { const sheets = await page.evaluate(() => { const out = []; for (const sh of allSheets()) if (sh.charms && sh.charms.length) { if (!sh.folderPath) sh.folderPath = 'charmnest/sheets/' + sh.sheetId; out.push({ sheetId: sh.sheetId, poolIds: sh.charms.map(x => x.poolId) }); } return out; });
      for (const sh of sheets) w.srv.st.put('Charm_Nest_Sheets', sh.sheetId, { sheetId: sh.sheetId, poolIds: sh.poolIds, backPool: [] }); }
    const openPiece = async key => {
      await page.evaluate(key => { const row = B.orders.byKey.get(key), jobs = Engrave.jobsOf(row), live = jobs.filter(x => x.state !== 'none'), j = live.find(x => x.face !== 'front') || live[0] || jobs[0]; try { OrderWin.close(); } catch (_) { /* none */ } CN.setMode('engrave'); Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: j.key }); Engrave.render(); }, key);
      await page.waitForFunction(pk => { const S = window.__stg, p = S.panel(pk); return !!p && S.vis(p); }, key, { timeout: 8000, polling: 'raf' }).catch(() => {});
      await page.waitForFunction(pk => { const S = window.__stg, sc = S.scope(pk), h = sc && sc.querySelector('.backHost'); return !!(h && h.querySelector('canvas')); }, key, { timeout: 3000, polling: 'raf' }).catch(() => {});
    };
    // the seed-1 pieces start approved and written, as stFz1 does in the suite
    for (const [sd, n] of names) if (sd !== 3) {
      await openPiece(made[n].key);
      const h = await page.evaluateHandle(pk => window.__stg.target(pk, 'approve'), made[n].key); const el = h.asElement(); if (el) await el.click({ timeout: 3000 }).catch(() => {});
      for (let i = 0; i < 150; i++) { const st = await page.evaluate(k => (Engrave.items().get(k) || {}).state, made[n].jobs[0].key); if (st === 'written') break; await new Promise(r => setTimeout(r, 200)); }
    }
    console.log(`   built and set up: ${J(Object.fromEntries(names.map(([, n]) => [n, made[n].jobs.map(j => j.state)])))}`);
    const plain = c => !c || ['none', 'skipped'].includes(c.state), approvedish = s => ['approved', 'written'].includes(s);
    for (const [sd, n] of names) {
      if (found) break;
      const pk = made[n].key, acts = rec ? rec.acts : K.fuzzActions(1000 + sd * 7919, 60, true), times = rec ? rec.log.map(x => x.t) : null;
      await page.evaluate(() => { try { Engrave.restoreView({ tab: 'place', list: true, chosen: true, focus: null }); Engrave.render(); } catch (_) { /* none */ } });
      await new Promise(r => setTimeout(r, 300));
      await openPiece(pk);
      await page.evaluate(installTracer, pk);
      const log = [], tS = Date.now();
      for (let i = 0; i < acts.length; i++) {
        const a = acts[i];
        if (times && times[i] != null) { const wait = times[i] - (Date.now() - tS); if (wait > 0) await new Promise(r => setTimeout(r, wait)); }   // (a recording keeps its own pace)
        const r1 = { i, k: a.k, arg: a.arg, t: Date.now() - tS };
        const info = await page.evaluate(([pk, a]) => {
          const S = window.__stg, p = S.panel(pk); if (!p || !S.vis(p)) return { gone: true };
          const kind = a.k === 'key' || a.k === 'bs' ? 'words' : a.k, el = S.target(pk, kind, a.arg); if (!el) return { missing: true };
          if (el.scrollIntoViewIfNeeded) el.scrollIntoViewIfNeeded(false);
          const r = el.getBoundingClientRect(), enabled = !el.disabled && el.getAttribute('aria-disabled') !== 'true';
          if (a.k === 'approve' && window.__trace) window.__trace.lines.push({ t: Math.round((performance.now() - window.__trace.t0) * 10) / 10, executor: 'approve press ' + (enabled ? 'LIT' : 'not lit'), staged: S.staged(pk) });
          const hx = r.left + r.width / 2, hy = r.top + Math.min(r.height / 2, 18), top = document.elementFromPoint(hx, hy), hit = !!top && (el === top || el.contains(top)); const over = hit ? null : top ? (top.tagName + '.' + String(top.className || '').split(' ').slice(0, 2).join('.')).slice(0, 60) : 'nothing';
          return { x: hx, y: hy, hit, over, enabled, focused: document.activeElement === el, staged: a.k === 'approve' ? S.staged(pk) : undefined };
        }, [pk, a]);
        if (info.gone) { r1.gone = true; log.push(r1); await openPiece(pk); continue; }
        if (info.missing || ((a.k === 'key' || a.k === 'bs') && !info.enabled)) { r1.missing = true; log.push(r1); continue; }
        if (!info.hit) r1.blocked = info.over;   // (something else is on top where the mouse presses: the press does not reach the control)
        if (a.k === 'approve') { r1.enabled = info.enabled && info.hit; r1.staged = info.staged; if (info.staged && info.staged.choice === 'none') { r1.enabled = false; r1.skipped = true; log.push(r1); continue; } }
        if (a.k === 'key' || a.k === 'bs') { if (!info.focused) await page.mouse.click(info.x, info.y); await page.keyboard.press(a.k === 'bs' ? 'Backspace' : a.arg); }
        else await page.mouse.click(info.x, info.y);
        log.push(r1);
      }
      // the end: 12 s for the commit to land, then what the jobs hold against what was staged at the last lit press
      const tEnd = Date.now(); let c = null, last = '', same = 0;
      while (Date.now() - tEnd < 12000) { c = await page.evaluate(pk => window.__stg.committed(pk), pk); const sig = J(c); same = sig === last && !c.working ? same + 1 : 0; last = sig; if (same >= 15) break; await new Promise(r => setTimeout(r, 100)); }
      const s = await page.evaluate(pk => window.__stg.snap(pk), pk);
      const lastLit = [...log].reverse().find(x => x.k === 'approve' && x.enabled), lastEdit = log.reduce((m, x) => (['choice', 'key', 'bs'].includes(x.k) && !x.missing ? x.i : m), -1);
      const stg = await page.evaluate(pk => window.__stg.staged(pk), pk);
      const lost = s.open && !s.dirty && stg ? stg.faces.filter(f => !(c[f] && approvedish(c[f].state))) : [];
      const verdict = lost.length ? `REPRODUCED: the card is not dirty (calm ${J(s.calm)}, Approve ${J(s.approve)}) yet ${lost.join('+')} is not approved (staged ${J(stg.words)}); last lit press: action ${lastLit ? lastLit.i : '-'}, last edit: action ${lastEdit}` : s.open ? `not reproduced (dirty ${s.dirty}, faces ${J(stg && stg.faces)})` : 'not applicable (the card is gone)';
      console.log(`   ${n} (seed ${sd}): ${verdict}; end: dirty ${s.dirty}, calm ${s.calm}, approve ${J(s.approve)}; committed front ${J(c.front && [c.front.state, c.front.text, c.front.approvedAt])}, back ${J(c.back && [c.back.state, c.back.text, c.back.approvedAt])}`);
      const tr = await page.evaluate(() => window.__trace);
      fs.mkdirSync(OUT, { recursive: true });
      const file = path.join(OUT, `repro-seed${sd}-${n}-${Date.now()}.json`);
      fs.writeFileSync(file, JSON.stringify({ seed: sd, piece: n, pk, verdict, acts, log, end: { snap: s, committed: c }, trace: tr.lines }, null, 1));
      console.log(`   trace (${tr.lines.length} lines) and actions: ${file}`);
      if (lost && lost.length) {
        found++;
        // the decisive lines: from the last lit press on, the press itself, the API calls, and every write to the face(s) that lost their approval
        // (the decisive lines: every lit press, every press that reached Approve, the commits, and the writes of state / approvedAt / text to the face(s) that lost their approval)
        const keys = lost.map(f => (f === 'front' ? pk + '~front' : pk));
        for (const x of tr.lines.filter(x => (x.executor && /LIT/.test(x.executor)) || x.press === 'approve' || (x.call && /commitStage|approve|saveSetup/.test(x.call)) || (keys.includes(x.key) && ['state', 'approvedAt', 'text', '(job made)'].includes(x.f)))) console.log('     ' + J(Object.assign({}, x, { st: x.st && x.st.split(' < ').slice(0, 4).join(' < ') })).slice(0, 520));
      }
    }
  } catch (e) { console.error(e); }
  finally { await w.close(); }
  console.log(found ? 'engrave-stage-repro: reproduced' : 'engrave-stage-repro: not reproduced');
  if (errors.length) console.log('page errors: ' + errors.slice(0, 3).join(' | '));
  process.exit(found ? 0 : 2);
})().catch(e => { console.error(e); process.exit(1); });
