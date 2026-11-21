// ITEM 9 (Paul, 21 Nov 2026): an already-approved back / front engraving EDITED from the Library sheet preview (Engrave.openBack: the editingBack job), then Approve, on a staff computer
// (a viewer) with the cloud carrying every run (mode "on").
//   What happened: approveOnce skipped the cloud lane and the viewer guard for an edit, so the file was built on the page; with the cloud writing the run's files saveBacks returned at
//   once (the edit lost without a word), otherwise its save was refused ("Workspace checkpoint waits before recording the approved back"; the cloud refuses a page's backPut in mode on)
//   and the card said Approved with its seal while nothing was saved.
//   Now: the edit goes to the cloud as an edit of its line's job (engraveApprove { edit }): the cloud reopens that job in its record and its back-file task writes that one piece's
//   file (built, checked, uploaded, backPut naming the back it replaces, the line's record written); the card waits ("Sent to the cloud ...") and the seal comes down when the cloud says
//   the file is written (Engrave.cloudEditSaved).
//   S  the press in mode "on" (offline, _cloudFakes.cjs): an edit of a written job reopens it (state review, approving, the piece back in the job), its task is origin "edit" for that
//      piece only, naming the back it replaces and what the job said before; a press with no edit is still refused as "approved already"; an edit naming another line's piece is
//      refused; drive off: `local` as before
//   E  the REAL page over the fake site (_engraveRealWorld.cjs): mode off, a back approved and then edited from the sheet preview is written by the page as before ("Back engraving
//      updated", the back record has the new words); then the page made a viewer in mode on: the same back edited again is SENT to the cloud (a task of origin "edit"), nothing is
//      written by the page and the card never says Approved first; the cloud's own back-file unit (_charmEngraveFiles.js backUnit, run in-process on the same fake site, the line no
//      longer in the run) writes the new file, the back record and the line's record; the page's watch then brings the seal down ("Back engraving updated", written, the new approval)
// Offline only: no Etsy, no paid call, no live write. Made-up names.
//   NODE_PATH=<deps> PW_DIR=<playwright> CHROMIUM=<chrome> node tests/charm-nest/viewer-item9.cjs
'use strict';
const path = require('path');
let failed = 0;
const log0 = console.log.bind(console);
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 1500))); if (!cond) failed++; };
const CV = 'cloud14-20261121', PAGE = 'd0michelle1';

async function partS() {
  process.env.EDIT_PASSCODE = 'test-pass';
  const T0 = Date.UTC(2026, 10, 21, 15, 0, 0);
  const F = require('./_cloudFakes.cjs')({ now: T0 });
  const quiet = console.log, qw = console.warn; console.log = () => {}; console.warn = () => {};
  try {
    const lib = F.load('charmNestLibrary.js');
    const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
    const R = 'run-2026-11-21-item9', L = '4241500132_42415001321', P1 = L + '_1', SH = 'gold-s41-item9', WAS = T0 - 86400e3;
    const place = { centre: [40.5, 22.25], angle: 0, size: 9.5, lines: ['Name Two'], weight: 'Regular', lineGap: 0.18, fittedMax: 11, upAngle: 90, cx: 40, cy: 22, fontKey: 'source-sans-3' };
    const seed = mode => {
      F.reset(); F.clock.set(T0);
      F.seed('Charm_Nest_Open/drive', { mode, runMode: 'manual', cloudSteps: ['engrave', 'files'], flippedAt: T0 - 3600e3 }); lib._driveForget();
      F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'running', step: 'nest', mode: 'auto', day: '2026-11-21', sheets: {}, holds: {}, errors: [], orders: [] });
      F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
      F.seed('Charm_Nest_Engrave/' + L, { key: L, order: L.split('_')[0], rev: 3, at: WAS, rec: JSON.stringify({ v: 1, override: null, engrave: { needed: true, state: 'written', approved: true, text: 'Name One' },
        jobs: [{ key: L, state: 'written', text: 'Name One', lines: ['Name One'], approvedAt: WAS, approvedBy: 'Ana', place: Object.assign({}, place, { lines: ['Name One'] }), copyOverrides: [P1] }] }) });
    };
    const lineJob = () => { const d = F.doc('Charm_Nest_Engrave/' + L); return (JSON.parse(d.rec).jobs || []).find(j => j.key === L); };
    const tasks = () => F.docs ? F.docs('Charm_Engrave_Files') : null;
    seed('on');
    const press = extra => P(Object.assign({ op: 'engraveApprove', runId: R, lineKey: L, jobKey: L, at: T0, by: 'Michelle', dev: PAGE, cv: CV, text: 'Name Two', place, fields: { lineInput: ['Name Two'] } }, extra || {}));
    let r = await press();
    ok('S1 mode on: Approve of an approved, written face with NO edit is still refused in words (approved already: change the words or the place first)', r.http === 409 && /approved already/.test(r.error || '') && lineJob().state === 'written', r);
    r = await press({ edit: { poolId: '4241999999_42419999991_1', sheetId: SH, was: WAS } });
    ok('S2 ... an edit naming a piece of ANOTHER order line is refused in words, nothing written', r.http === 400 && /does not name a piece of that order line/.test(r.error || '') && lineJob().state === 'written', r);
    r = await press({ edit: { poolId: P1, sheetId: SH, was: WAS, sku: 'COMET_9427', copy: 1 } });
    const job = lineJob(), t = F.doc('Charm_Engrave_Files/' + (r.taskId || 'none'));
    ok('S3 ... the EDIT is the change: the line\'s job is reopened in the cloud\'s record (review, waiting on this approval, the new words and place, the piece its own again)',
      r.http === 200 && r.ok && job.state === 'review' && job.approving && job.approving.at === T0 && job.approving.edit === true && job.text === 'Name Two' && job.place && job.place.lines[0] === 'Name Two' && !(job.copyOverrides || []).includes(P1),
      { r, job });
    ok('S4 ... its task writes that one piece (origin "edit", copies [piece]), names the back it replaces and what the job said before',
      !!t && t.origin === 'edit' && t.state === 'waiting' && JSON.stringify(t.copies) === JSON.stringify([P1]) && t.edit && t.edit.poolId === P1 && t.edit.was === WAS && t.edit.sheetId === SH && t.edit.prev && t.edit.prev.state === 'written' && t.edit.prev.text === 'Name One' && t.runId === R,
      t);
    seed('off');
    r = await press({ edit: { poolId: P1, sheetId: SH, was: WAS } });
    ok('S5 drive off: the press is not the cloud\'s (`local`: the page saves the edit as before), nothing written', r.http === 409 && r.local === true && lineJob().state === 'written', r);
    // the cloud's own reading of an edit whose line the run no longer holds (its order cut and done): the line as the edit names it
    const Files = F.load('_charmEngraveFiles.js');
    ok('S6 the cloud\'s back-file unit knows the edit\'s line without the run (the piece, its order, its line): never "its order line is no longer in the run"', typeof Files.runTask === 'function'
      && /editLine\(t, t\.lineKey\)/.test(require('fs').readFileSync(path.join(__dirname, '../../netlify/functions/_charmEngraveFiles.js'), 'utf8')));
  } finally { console.log = quiet; console.warn = qw; }
}

async function partE() {
  let Rw; try { Rw = require('./_engraveRealWorld.cjs'); } catch (e) { ok('E world', false, String(e)); return; }
  const w = await Rw.world({ viewport: { width: 1440, height: 900 } });
  if (!w) { log0('E: no playwright-core: the real page was not run'); return; }
  const page = w.page, st = w.srv.st;
  const until = async (fn, ms) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) return null; await new Promise(r => setTimeout(r, 200)); } };
  const doc = async (c, id) => { const d = await st.db.collection(c).doc(id).get(); return d.exists ? d.data() : null; };
  const R = 'run-2026-11-21-item9';
  try {
    const made = await Rw.build(w, ['back']);
    const key = made.back.jobs[0] && made.back.jobs[0].key;
    // the piece and its sheet as a saved sheet in the cloud: its folder, its file name, its pieces
    const info = await page.evaluate(k => { const j = Engrave.items().get(k), pid = j.copies[0], sh = allSheets().find(s => (s.charms || []).some(c => c.poolId === pid)); return { pid, sheetId: sh.sheetId, fileBase: sh.fileBase, folderPath: sh.folderPath, runId: sh.runId || null }; }, key);
    const lineKey = info.pid.replace(/_\d+$/, '');
    {
      const cur = (await doc('Charm_Nest_Sheets', info.sheetId)) || {};
      st.put('Charm_Nest_Sheets', info.sheetId, Object.assign({}, cur, { id: info.sheetId, sheetId: info.sheetId, metal: 'gold', fileBase: info.fileBase, folderPath: info.folderPath, runId: 'run-real', outputs: { ai: { path: info.folderPath + '/' + info.fileBase + '.ai' } }, backPool: cur.backPool || [] }));
      const pool = (await doc('Charm_Pool', info.pid)) || {};
      st.put('Charm_Pool', info.pid, Object.assign({}, pool, { sheetId: info.sheetId, state: pool.state && pool.state !== 'pooled' ? pool.state : 'placed' }));
    }
    // ── 1 · mode off: approve the back as before (the page writes its file) ──
    await Rw.openEngraveDetail(w, 'back', { face: 'back' }); await Rw.settle(page, 500);
    await page.evaluate(k => Engrave.approve(Engrave.items().get(k)), key);
    const first = await until(async () => { const j = await page.evaluate(k => { const j = Engrave.items().get(k); return { state: j.state, at: j.approvedAt }; }, key); const b = await doc('Charm_Pool_Back', info.pid); return j.state === 'written' && b && b.approvedAt === j.at ? { j, b } : null; }, 120000);
    ok('E0 mode off: the back is approved and written by the page, as before', !!first, first);
    // the Library's sheet preview opens it again and the words are changed, then Approve (what a person does: the card's words box, then Approve, pressed until it is taken)
    const editOnce = async (words, o = {}) => {
      await page.evaluate(({ pid, sid }) => Engrave.openBack(pid, sid), { pid: info.pid, sid: info.sheetId });
      const ek = 'back:' + info.pid;
      await until(() => page.evaluate(ek => { const j = Engrave.items().get(ek); return !!(j && j.fit && j.verify); }, ek), 60000); await Rw.settle(page, 500);
      // (the new words placed on the piece as the card's words box does: approveOnce's own new-words path, fitNewWords -> fitJob; the box, where the card has one, says the same)
      const placed = await page.evaluate(async ({ ek, words }) => {
        const j = Engrave.items().get(ek);
        j.text = words; j.lineInput = [words]; j.lines = [words]; j.fit = null; j.verify = null; j.wantSize = null;
        await Engrave.fitJob(j);
        const box = document.querySelector('[data-f="words"]'); if (box) box.value = words;
        return { text: j.text, fitted: !!(j.fit && j.verify && j.verify.geometry && j.verify.geometry.ok) };
      }, { ek, words });
      const out = { presses: 0, placed };
      for (let i = 0; i < 6; i++) {
        out.presses++;
        const r = await page.evaluate(async ({ ek, words }) => {
          const j = Engrave.items().get(ek), card = document.querySelector('#egQueue .rvItem[data-kind=placement]'), box = document.querySelector('[data-f="words"]');
          if (box && box.value !== words) box.value = words;
          const b = card && card.querySelector('[data-a="approve"]');
          const p = Engrave.approve(j, null, b); await new Promise(r => setTimeout(r, 400));
          const label = b ? b.textContent : null; await p.catch(() => {});
          const why = card && card.querySelector('.egApproveWhy');
          return { state: j.state, approving: !!(j.approving && j.approving.cloud), text: j.text, label, why: why && !why.hidden ? why.textContent.trim() : '' };
        }, { ek, words });
        out.last = r;
        if (r.state === 'written' || r.approving || o.stopWhen && o.stopWhen(r)) break;
        await new Promise(r => setTimeout(r, 800));
      }
      return out;
    };
    await page.evaluate(() => { window.__said = []; const t0 = window.toast; window.toast = function (t) { window.__said.push(String(t)); return t0 && t0.apply(this, arguments); }; });
    const off = await editOnce('Name Two');
    const offBack = await until(async () => { const b = await doc('Charm_Pool_Back', info.pid); return b && b.text === 'Name Two' ? b : null; }, 60000);
    const offSaid = await page.evaluate(() => window.__said.slice());
    ok('E1 mode off: an approved back edited from the sheet preview is written by the page as before (the back record has the new words, "Back engraving updated")',
      !!offBack && offBack.approvedAt > first.b.approvedAt && offSaid.some(t => /Back engraving updated/.test(t)) && !(await st.db.collection('Charm_Engrave_Files').get()).docs.length,
      { off, back: offBack && { text: offBack.text, at: offBack.approvedAt }, said: offSaid });
    // ── 2 · the page is a viewer, the cloud carries the run in mode "on" ──
    st.put('Charm_Nest_Open', 'drive', { mode: 'on', runMode: 'manual', cloudSteps: ['engrave', 'files', 'takeoff'], flippedAt: Date.now() - 3600e3 });
    st.put('Charm_Nest_Runs', R, { runId: R, status: 'running', step: 'nest', mode: 'auto', day: '2026-11-21', sheets: {}, holds: {}, errors: [], orders: [], lines: {} });
    st.put('Charm_Nest_Run_Carrier', R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: Date.now() + 3600e3, since: Date.now() - 3600e3 } });
    for (const h of Object.values(st.handlers || {})) try { if (h && typeof h._driveForget === 'function') h._driveForget(); } catch (_) {}
    const viewer = await page.evaluate(R => {
      B.run = null;
      const F = window.CharmNestRunFollow, st0 = F.state; F.state = () => Object.assign(st0(), { runId: R });
      window.CNViewer = window.CNViewer || {}; window.__viewerOn = CNViewer.on; CNViewer.on = () => true;
      Session.freeze(); window.__said = [];
      return { frozen: Session.frozen() };
    }, R);
    ok('E2 the page is a viewer: its workspace frozen, no run held, the run followed', viewer.frozen === true, viewer);
    const before = await doc('Charm_Pool_Back', info.pid);
    const on = await editOnce('Name Three', { stopWhen: r => /Not approved/.test(r.why) });
    const task = await until(async () => { const s = await st.db.collection('Charm_Engrave_Files').where('origin', '==', 'edit').get(); return s.docs.length ? s.docs[0].data() : null; }, 20000);
    const ej = await page.evaluate(pid => { const j = Engrave.items().get('back:' + pid); return { state: j.state, approving: j.approving || null, said: window.__said.slice() }; }, info.pid);
    const afterPress = await doc('Charm_Pool_Back', info.pid);
    const rec = await doc('Charm_Nest_Engrave', lineKey);
    const recJob = rec ? (JSON.parse(rec.rec).jobs || []).find(j => j.key === task.jobKey) : null;
    ok('E3 mode on: Approve of the edit on the viewer is SENT to the cloud (a task of origin "edit" for that piece, the line\'s job), the card waits on it',
      !!task && task.runId === R && task.lineKey === lineKey && JSON.stringify(task.copies) === JSON.stringify([info.pid]) && task.text === 'Name Three' && task.edit && task.edit.was === before.approvedAt && ej.approving && ej.approving.cloud && ej.state === 'review',
      { on, task: task && { runId: task.runId, jobKey: task.jobKey, copies: task.copies, text: task.text, edit: task.edit }, ej });
    ok('E4 ... nothing was written by the page, never "Approved" before the file, never the browser\'s save ("Workspace checkpoint", "could not be saved"), never "Back engraving updated" yet',
      afterPress.approvedAt === before.approvedAt && afterPress.text === 'Name Two' && on.last.label !== 'Approved' && !ej.said.concat([on.last.why]).some(t => /Workspace checkpoint|could not be saved on this browser|Back engraving updated/.test(t)),
      { label: on.last.label, why: on.last.why, said: ej.said, back: { text: afterPress.text, at: afterPress.approvedAt } });
    ok('E5 ... the cloud\'s record has the line\'s job reopened for the edit (waiting on this approval)', !!recJob && recJob.state === 'review' && recJob.approving && recJob.approving.at === task.at && recJob.text === 'Name Three', recJob && { state: recJob.state, approving: recJob.approving, text: recJob.text });
    // ── 3 · the cloud's back-file unit, in-process on the same fake site (the run's lines do not hold this order: a cut sheet's piece) ──
    const poolBefore = await doc('Charm_Pool', info.pid);
    const Lib = st.handlers.charmNestLibrary;
    st.put('Charm_Nest_Drive', 'lease', { nonce: 'item9-pass', state: 'running', at: Date.now() });
    const fence = { path: 'Charm_Nest_Drive/lease', nonce: 'item9-pass', runId: R };
    const Files = require(path.join(Rw.root, 'netlify/functions/_charmEngraveFiles.js'));
    const run = (await Lib.call({ op: 'runGet', runId: R }, {})).run || { runId: R, lines: {} };
    const said = [];
    const ctx = { db: st.db, run, runId: R, fence, bucket: st.bucket, read: b => Lib.call(b, {}), call: (b, unit) => Lib.call(b, { unit, fence }), say: s => said.push(String(s)), budgetEnd: Date.now() + 240000 };
    let unitErr = null; try { await Files.backUnit(ctx); } catch (e) { unitErr = String(e && e.stack || e); }
    const t2 = await doc('Charm_Engrave_Files', task.id);
    const back2 = await doc('Charm_Pool_Back', info.pid);
    const rec2 = await doc('Charm_Nest_Engrave', lineKey), recJob2 = rec2 ? (JSON.parse(rec2.rec).jobs || []).find(j => j.key === task.jobKey) : null;
    const pool2 = await doc('Charm_Pool', info.pid);
    const sheet2 = await doc('Charm_Nest_Sheets', info.sheetId);
    const fileThere = back2 && back2.outputs && back2.outputs.ai && st.blobs.has(back2.outputs.ai.path) && st.blobs.has(back2.outputs.png.path);
    ok('E6 the cloud writes the new back file: its task is done, the back record has the new words under this approval, its .ai and picture are in Storage, the sheet lists it',
      !unitErr && t2 && t2.state === 'done' && back2 && back2.text === 'Name Three' && back2.approvedAt === task.at && back2.approvedBy === task.by && fileThere && (sheet2.backPool || []).some(b => b.poolId === info.pid && b.approvedAt === task.at),
      { unitErr, task: t2 && { state: t2.state, why: t2.why, note: t2.note, lastError: t2.lastError }, back: back2 && { text: back2.text, at: back2.approvedAt }, said: said.slice(0, 5) });
    ok('E7 ... the line\'s record says written with the new words and this approval\'s seal; the piece\'s state is not moved by an edit',
      !!recJob2 && recJob2.state === 'written' && recJob2.approvedAt === task.at && recJob2.text === 'Name Three' && !recJob2.approving && (recJob2.engravingSeals || []).some(s => s.at === task.at) && pool2 && pool2.state === poolBefore.state,
      { job: recJob2 && { state: recJob2.state, at: recJob2.approvedAt, text: recJob2.text }, pool: pool2 && pool2.state, before: poolBefore && poolBefore.state });
    // ── 4 · the page's watch sees the cloud's file written: the seal comes down ──
    const done = await until(() => page.evaluate(pid => { const j = Engrave.items().get('back:' + pid); return j && j.state === 'written' && !j.approving ? { state: j.state, at: j.approvedAt, by: j.approvedBy, seals: (j.engravingSeals || []).map(s => s.at), said: window.__said.slice() } : null; }, info.pid), 30000);
    ok('E8 the viewer\'s card: written under the cloud\'s approval, its seal, "Back engraving updated" said only now',
      !!done && done.at === task.at && done.seals.includes(task.at) && done.said.some(t => /Back engraving updated/.test(t)), done);
    await page.evaluate(() => { CNViewer.on = window.__viewerOn; });
  } catch (e) { ok('E run', false, String(e && e.stack || e).split('\n').slice(0, 4).join(' | ')); }
  ok('E no page error', w.errors.length === 0, w.errors.slice(0, 2).join(' | '));
  await w.close();
}

(async () => {
  // (part S in a process of its own: the fake site of part E and the offline cloud of part S each load the library with their own database, and one process keeps one copy of it)
  if (process.env.IT9_PART === 'offline') { await partS(); }
  else {
    if (!process.env.SKIP_PAGE) await partE();
    const r = require('child_process').spawnSync(process.execPath, [__filename], { env: Object.assign({}, process.env, { IT9_PART: 'offline' }), encoding: 'utf8', timeout: 180000 });
    const out = String(r.stdout || '') + String(r.stderr || ''); for (const l of out.split('\n')) if (/^(OK|FAIL) /.test(l)) log0(l);
    if (r.status !== 0) { failed++; if (!/^(OK|FAIL) /m.test(out)) log0(out.slice(0, 1500)); }
  }
  log0(failed ? `FAIL viewer-item9 (${failed})` : 'PASS viewer-item9');
  process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL viewer-item9: ' + (e && e.stack || e)); process.exit(1); });
