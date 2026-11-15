/*  charm-nest-options-sheets.js — the Sheets card of the Options window (SHEETSLIST, Paul, 15 Nov 2026; reworked SHEETSANYWHERE the same day).
 *  SHEETSLIST: "I should be able to easily control in this screen: 1. consolidating sheets by dragging and dropping one sheet onto another. 2. deleting unnecessary sheets and easily being able to choose
 *   which sheet is to be draft 1, then which sheet is to be draft 2 etc. 3. The user should be able to choose sheet 1 2 3 4 in whatever order they decide by dragging the sheets around ..."
 *  SHEETSANYWHERE: "inside the options menu I just see two drafts that are locked ... I wanted the drafts to be part of the partial sheets view so then I can drag the sheets from one sheet to another and amalgamate
 *   sheets as needed, and then I need a way to choose which sheet is gonna be draft one ... I should be able to empty them out. Combine them with other sheets ..." Every sheet is a picture tile (as the partial
 *   sheets are); drag a tile BETWEEN two tiles to put it there, ONTO a tile to combine, "Empty this sheet" to move its pieces onto the others, Delete for an empty one.
 *  On the computer that carries the run the Gate does it (Gate.reorder / consolidate / emptySheet / deleteEmpty); on a computer that only follows the run the same gesture is SENT to the one that carries it
 *  (runCmdPut on the cloud; charm-nest-run-follow.js runCommands does it there and answers with runCmdDone) and the answer is shown here. This file draws and listens; it writes nothing itself.
 *  window.OptionsSheets: mount(M) -> element | null · paint(M) · destroy(M) */
(function (root) {
  'use strict';
  const doc = root.document; if (!doc || root.OptionsSheets) return;
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const G = () => root.Gate && typeof root.Gate.sheetsOf === 'function' ? root.Gate : null;
  const solid = m => m === 'gold10k' || m === 'gold14k';
  const NO_CARRIER = 'No computer is carrying the run right now: open the app on the computer that runs it, or press Resume here to carry it on this computer.';
  const CSS = `
.osSheets .osShList{list-style:none;margin:0;padding:0;display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:18px}
.osSh{position:relative;display:flex;flex-direction:column;gap:8px;padding:12px;border:1px solid var(--line);border-radius:14px;background:var(--paper2,transparent);outline:none;transition:box-shadow .14s,border-color .14s,transform .14s;touch-action:pan-y}
.osSh:focus-visible{box-shadow:0 0 0 2px var(--gold2)}
.osSh.fixed{background:repeating-linear-gradient(135deg,transparent 0 9px,rgba(120,110,90,.06) 9px 18px)}
.osShPic{position:relative;width:100%;border-radius:8px;background:#fffefb;border:1px solid var(--line);overflow:hidden;cursor:grab;display:grid;place-items:center}
.osSh.fixed .osShPic{cursor:default}
.osShPic canvas,.osShPic img{display:block;width:100%;height:100%;object-fit:contain;pointer-events:none;-webkit-user-drag:none}
.osShPic .osShNone{font-size:11.5px;color:var(--ink45,#888);padding:6px;text-align:center}
.osShHead{display:flex;align-items:baseline;justify-content:space-between;gap:8px}
.osShName{font:500 20px var(--serif,inherit);line-height:1.15}
.osShMeta{font-size:12.5px;color:var(--ink45,#777)}
.osShBadges{display:flex;gap:6px;flex-wrap:wrap;min-height:0}
.osShBadge{font-size:11px;padding:2px 8px;border-radius:99px;border:1px solid var(--line);color:var(--ink45,#777)}
.osShBadge.lock{border-color:#b3402e;color:#b3402e}
.osShBar{height:4px;border-radius:3px;background:var(--line);overflow:hidden}
.osShBar i{display:block;height:100%;background:var(--gold2,#b8913f)}
.osShNote{font-size:12px;color:var(--ink45,#777)}
.osShNote.bad{color:#b3402e}
.osShActs{display:flex;gap:6px;flex-wrap:wrap}
.osSh.dragging{opacity:.92;z-index:5;box-shadow:0 14px 34px rgba(30,24,16,.25);pointer-events:none;transition:none}
.osSh.onto{border-color:var(--gold2);box-shadow:0 0 0 3px var(--gold2)}
.osSh.onto.refused{border-color:#b3402e;box-shadow:0 0 0 3px #b3402e}
.osSh.before::before,.osSh.after::after{content:"";position:absolute;top:6px;bottom:6px;width:4px;border-radius:2px;background:var(--gold2)}
.osSh.before::before{left:-11px}.osSh.after::after{right:-11px}
.osShTag{position:absolute;left:50%;top:38%;transform:translateX(-50%);max-width:92%;text-align:center;font-size:12px;font-weight:600;padding:4px 11px;border-radius:99px;background:var(--gold2,#b8913f);color:#fff;pointer-events:none;z-index:2;box-shadow:0 4px 14px rgba(0,0,0,.25)}
.osShTag.refused{background:#b3402e}
.osShAsk{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:12px;font-size:13px}
.osShSaid{margin:12px 0 0;font-size:13px}
.osShSaid.bad{color:#b3402e}
.osShSpin{display:inline-block;width:11px;height:11px;margin-right:7px;vertical-align:-1px;border:2px solid var(--line);border-top-color:var(--gold2,#b8913f);border-radius:50%;animation:osShSpin .8s linear infinite}
@keyframes osShSpin{to{transform:rotate(360deg)}}
html.osShDrag,html.osShDrag *{cursor:grabbing!important;user-select:none!important}`;
  let styled = false;
  const style = () => { if (styled) return; styled = true; const s = doc.createElement('style'); s.textContent = CSS; doc.head.appendChild(s); };
  const RF = () => root.CharmNestRunFollow || null;
  const api = body => root.CN && typeof root.CN.api === 'function' ? root.CN.api('charmNestLibrary', body, { quiet: true }) : Promise.reject(new Error('no connection'));
  const devId = () => { try { return RF().dev(); } catch (_) { return ''; } };
  const who = () => { try { return String((root.B && root.B.employee) || '').trim(); } catch (_) { return ''; } };

  function mount(M) {
    if (!M || !solid(M.m) || !G()) return null;
    style();
    const el = doc.createElement('section'); el.className = 'osCard osSheets'; el.dataset.card = 'sheets'; el.setAttribute('aria-label', 'Sheets'); el.hidden = true;
    el.innerHTML = `<header class="osCardHead"><div><h3 class="osCardTitle">Sheets</h3><p class="osLead" data-sh="lead"></p></div></header><ol class="osShList" data-sh="list"></ol><div class="osShAsk" data-sh="ask" hidden></div><p class="osShSaid" data-sh="said" role="status" hidden></p>`;
    M.sheetsUI = { el, sig: '', drag: null, ask: null, note: {}, rel: null, sub: null, poll: 0 };
    const list = el.querySelector('[data-sh="list"]');
    list.addEventListener('pointerdown', e => down(M, e));
    list.addEventListener('keydown', e => key(M, e));
    list.addEventListener('click', e => {
      const d = e.target.closest && e.target.closest('[data-sh-del]'); if (d) return del(M, d.closest('li').dataset.id);
      const m = e.target.closest && e.target.closest('[data-sh-empty]'); if (m) emptyAsk(M, m.closest('li').dataset.id);
    });
    return el;
  }
  const rowsOf = M => { try { return G().sheetsOf(M.m); } catch (_) { return []; } };
  const relayed = M => { const r = rowsOf(M); return !!(r.length && r[0].relay); };

  function pictures(S, rows) {
    for (const r of rows) {
      const box = S.el.querySelector(`[data-pic="${CSS_ESC(r.id)}"]`); if (!box) continue;
      if (r.live) {
        const cv = doc.createElement('canvas'), pg = G().pageOf && G().pageOf(r.id), C = root.CN;
        try { cv.width = Math.round(240 * Math.min(2, root.devicePixelRatio || 1)); cv.height = Math.max(20, Math.round(cv.width / (r.aspect || 2))); C.paintPreview(cv, pg, true, 0); box.innerHTML = ''; box.appendChild(cv); } catch (_) { box.innerHTML = '<span class="osShNone">picture not ready</span>'; }
      } else if (r.picture) {
        const img = doc.createElement('img'); img.alt = r.name; img.decoding = 'async'; img.loading = 'lazy'; img.onerror = () => { box.innerHTML = '<span class="osShNone">picture not available</span>'; }; img.crossOrigin = 'anonymous'; box.innerHTML = ''; box.appendChild(img);   /* (the page is cross-origin isolated: a picture from the cloud's storage is loaded through the app's own loader, as the sheet cards do; a plain src is blocked) */ try { const A = root.CharmNestAssets; if (A && typeof A.sheetImage === 'function') Promise.resolve(A.sheetImage(img, r.id, r.picture)).catch(() => img.onerror()); else img.src = r.picture; } catch (_) { img.src = r.picture; }
      } else box.innerHTML = '<span class="osShNone">' + (r.pieces ? 'picture loading…' : 'empty sheet') + '</span>';
    }
  }
  const CSS_ESC = v => String(v).replace(/["\\]/g, '\\$&');

  function paint(M, force) {
    const S = M && M.sheetsUI; if (!S || !G()) return;
    if (S.drag || S.ask) return;
    const rows = rowsOf(M), said = G().sheetsSaid ? G().sheetsSaid(M.m) : null, showSaid = S.rel ? '' : said && Date.now() - said.at < 600000 ? said.text : '';
    S.el.hidden = rows.length < 2;
    const sig = JSON.stringify([rows, showSaid, S.note, S.rel]);
    if (!force && sig === S.sig) return; S.sig = sig;
    const arrange = rows.length ? rows[0].arrange : true;
    S.el.querySelector('[data-sh="lead"]').textContent = arrange ? 'Drag a sheet between two sheets to put it there (Draft 1 is the first). Drop a sheet onto another to combine them; what does not fit goes on to the next sheet. Empty a sheet to move all its pieces onto the others.' : 'There is no open run to arrange sheets in.';
    const keepFocus = S.el.contains(doc.activeElement) && doc.activeElement.closest ? (doc.activeElement.closest('li') || {}).dataset : null;
    S.el.querySelector('[data-sh="list"]').innerHTML = rows.map(r => {
      const badges = [r.inSet ? 'In set' : '', r.cutLine ? 'Cut line drawn' : '', r.cut ? 'Cut' : '', r.locked ? 'Locked' : ''].filter(Boolean).map(b => `<span class="osShBadge${b === 'Locked' ? ' lock' : ''}">${b}</span>`).join('');
      const why = S.note[r.id] || (r.locked ? r.lockWhy : !r.canDelete && r.whyDelete && /^Move its/.test(r.whyDelete) ? '' : !r.movable && r.arrange ? (r.whySource || r.whyTarget || '') : '');
      const empty = r.arrange && !r.locked && r.pieces > 0 && r.canSource;
      const pieces = `${r.pieces} piece${r.pieces === 1 ? '' : 's'}${r.pct == null ? '' : ' · ' + r.pct + '% full'}${r.busy ? ' · working…' : ''}`;
      return `<li class="osSh${r.movable ? '' : ' fixed'}" data-id="${esc(r.id)}" tabindex="0" aria-label="${esc(r.name)}, ${r.pieces} pieces${r.pct == null ? '' : ', ' + r.pct + ' percent full'}${r.locked ? ', locked: ' + esc(r.lockWhy) : ''}. Alt plus Up or Down arrow moves it.">
        <div class="osShPic" data-pic="${esc(r.id)}" style="aspect-ratio:${r.aspect || 2}" title="${r.movable ? 'Drag to move or combine' : esc(why || 'This sheet keeps its place')}"></div>
        <div class="osShHead"><span class="osShName">${esc(r.name)}</span></div>
        <span class="osShMeta">${esc(r.size)}<br>${pieces}</span>
        <span class="osShBar" aria-hidden="true"><i style="width:${r.pct == null ? 0 : r.pct}%"></i></span>
        <span class="osShBadges">${badges}</span>
        ${why ? `<span class="osShNote${S.note[r.id] ? ' bad' : ''}">${esc(why)}</span>` : ''}
        <span class="osShActs">${empty ? '<button type="button" class="btn ghost xs" data-sh-empty>Empty this sheet</button>' : ''}${r.canDelete ? '<button type="button" class="btn ghost xs" data-sh-del>Delete</button>' : ''}</span></li>`;
    }).join('');
    pictures(S, rows);
    const sd = S.el.querySelector('[data-sh="said"]'), text = S.rel ? S.rel.text : showSaid;
    sd.hidden = !text; sd.classList.toggle('bad', !!(S.rel && S.rel.bad));
    sd.innerHTML = (S.rel && S.rel.spin ? '<span class="osShSpin" aria-hidden="true"></span>' : '') + esc(text);
    if (keepFocus && keepFocus.id) { const n = [...S.el.querySelectorAll('li')].find(x => x.dataset.id === keepFocus.id); if (n) { try { n.focus({ preventScroll: true }); } catch (_) {} } }
  }
  const fail = (M, id, why) => { const S = M.sheetsUI; if (!S) return; S.note[id] = why; paint(M, true); setTimeout(() => { if (S.note[id] === why) { delete S.note[id]; paint(M, true); } }, 6000); };

  /* ── the computer that only follows the run: the gesture is sent to the one that carries it ── */
  function stopRel(S) { if (S.poll) { clearInterval(S.poll); S.poll = 0; } if (S.sub) { try { S.sub.stop(); } catch (_) {} S.sub = null; } }
  async function readRel(M) {
    const S = M.sheetsUI; if (!S || !S.rel || !S.rel.id || !S.rel.spin) return;
    let res = null; try { res = await api({ op: 'runCmdList', runId: S.rel.runId }); } catch (_) { return; }
    const c = ((res && res.cmds) || []).find(x => x && x.id === S.rel.id);
    if (c && (c.state === 'done' || c.state === 'failed')) {
      stopRel(S); const bad = c.state === 'failed'; S.rel = { text: c.text || (bad ? 'The computer that carries the run could not do that.' : 'Done'), bad, spin: false };
      paint(M, true); setTimeout(() => { if (S.rel && !S.rel.spin) { S.rel = null; paint(M, true); } }, 90000);
    } else if (Date.now() - S.rel.at > 11 * 60000) {
      stopRel(S); S.rel = { text: 'The computer that carries the run did not answer. Look at the sheets again, then try once more.', bad: true, spin: false }; paint(M, true);
    }
  }
  async function send(M, cmd) {
    const S = M.sheetsUI; if (!S) return false;
    if (S.rel && S.rel.spin) { S.rel.text += ' (wait for it to finish first)'; paint(M, true); return false; }
    const rows = rowsOf(M), runId = (rows[0] && rows[0].runId) || (RF() && RF().state().runId), dev = devId();
    if (!runId || !dev) { S.rel = { text: 'There is no open run to send this to.', bad: true, spin: false }; paint(M, true); return false; }
    S.rel = { text: 'Finding the computer that carries the run…', spin: true, runId, at: Date.now() }; paint(M, true);
    try {
      const c = await api({ op: 'runCarrier', runId, dev, check: true });
      if (!c || !c.alive || c.mine) { S.rel = { text: NO_CARRIER, bad: true, spin: false }; paint(M, true); return false; }
      const name = (c.carrier && c.carrier.by) || 'the computer that carries the run';
      const put = await api({ op: 'runCmdPut', runId, cmd: Object.assign({ metal: M.m }, cmd), by: who(), dev });
      if (!put || put.error || !put.id) throw new Error((put && put.error) || 'not sent');
      S.rel = { id: put.id, runId, name, at: Date.now(), spin: true, text: `Sent to ${name}… ` }; paint(M, true);
      if (root.CNSync && typeof root.CNSync.follow === 'function' && !S.sub) { try { S.sub = root.CNSync.follow({ name: 'run-cmds', kinds: 'r', wants: k => k === 'r:' + runId, read: () => readRel(M) }); } catch (_) { S.sub = null; } }
      if (!S.poll) S.poll = setInterval(() => readRel(M), 8000);
      readRel(M); return true;
    } catch (e) { S.rel = { text: 'Not sent: ' + ((e && e.message) || e), bad: true, spin: false }; paint(M, true); return false; }
  }

  /* ── the four gestures: the Gate here, or a command sent to the computer that carries the run ── */
  async function doReorder(M, ids, id) {
    if (relayed(M)) return void send(M, { type: 'reorder', ids });
    const r = G().reorder(M.m, ids); if (r && r.ok === false) fail(M, id, r.why); else paint(M, true);
  }
  async function doCombine(M, srcId, tgtId) {
    if (relayed(M)) return void send(M, { type: 'consolidate', sourceId: srcId, targetId: tgtId });
    const r = await G().consolidate(srcId, tgtId); if (r && r.ok === false && r.why) fail(M, srcId, r.why); paint(M, true);
  }
  async function doEmpty(M, id) {
    if (relayed(M)) return void send(M, { type: 'empty', sheetId: id });
    const r = await G().emptySheet(id); if (r && r.ok === false && r.why) fail(M, id, r.why); paint(M, true);
  }
  async function doDelete(M, id) {
    if (relayed(M)) return void send(M, { type: 'delete', sheetId: id });
    const r = await G().deleteEmpty(id); if (r && r.ok === false) fail(M, id, r.why); paint(M, true);
  }

  /* ── keyboard: Alt+ArrowUp / Alt+ArrowLeft move a focused tile earlier, Alt+ArrowDown / Alt+ArrowRight later ── */
  function key(M, e) {
    const k = e.key; if (!e.altKey || !['ArrowUp', 'ArrowDown', 'ArrowLeft', 'ArrowRight'].includes(k)) return;
    const li = e.target.closest && e.target.closest('li.osSh'); if (!li) return; e.preventDefault();
    const ids = [...M.sheetsUI.el.querySelectorAll('li.osSh')].map(x => x.dataset.id), i = ids.indexOf(li.dataset.id), j = i + (k === 'ArrowUp' || k === 'ArrowLeft' ? -1 : 1);
    if (j < 0 || j >= ids.length) return;
    ids.splice(i, 1); ids.splice(j, 0, li.dataset.id);
    Promise.resolve(doReorder(M, ids, li.dataset.id)).then(() => { const n = [...M.sheetsUI.el.querySelectorAll('li')].find(x => x.dataset.id === li.dataset.id); if (n) n.focus({ preventScroll: true }); });
  }

  /* ── pointer: the picture drags at once; the rest of the tile after 6 px ── */
  function down(M, e) {
    if (e.button != null && e.button > 0) return;
    const li = e.target.closest && e.target.closest('li.osSh'); if (!li || (e.target.closest && e.target.closest('button'))) return;
    const row = rowsOf(M).find(r => r.id === li.dataset.id); if (!row || !row.movable) return;
    const S = M.sheetsUI, st = { li, id: row.id, x: e.clientX, y: e.clientY, on: false, rows: null, over: null, mode: null, pid: e.pointerId };
    const grip = !!(e.target.closest && e.target.closest('.osShPic'));
    const move = ev => {
      if (!st.on) { if (!grip && Math.hypot(ev.clientX - st.x, ev.clientY - st.y) < 6) return; st.on = true; S.drag = st; st.rows = rowsOf(M); li.classList.add('dragging'); doc.documentElement.classList.add('osShDrag'); }
      li.style.transform = `translate(${ev.clientX - st.x}px,${ev.clientY - st.y}px)`; target(M, st, ev);
    };
    const end = ev => {
      doc.removeEventListener('pointermove', move, true); doc.removeEventListener('pointerup', end, true); doc.removeEventListener('pointercancel', end, true);
      if (!st.on) return; doc.documentElement.classList.remove('osShDrag'); li.classList.remove('dragging'); li.style.transform = ''; S.drag = null;
      const over = st.over, mode = st.mode; clear(M);
      if (ev.type === 'pointercancel' || !over) return paint(M, true);
      drop(M, st, over, mode);
    };
    doc.addEventListener('pointermove', move, true); doc.addEventListener('pointerup', end, true); doc.addEventListener('pointercancel', end, true);
  }
  const clear = M => { for (const n of M.sheetsUI.el.querySelectorAll('.onto,.before,.after,.refused')) n.classList.remove('onto', 'before', 'after', 'refused'); for (const t of M.sheetsUI.el.querySelectorAll('.osShTag')) t.remove(); };
  function target(M, st, ev) {
    clear(M); st.over = null; st.mode = null;
    const cards = [...M.sheetsUI.el.querySelectorAll('li.osSh')].filter(n => n !== st.li);
    const hit = cards.find(n => { const r = n.getBoundingClientRect(); return ev.clientY >= r.top - 6 && ev.clientY <= r.bottom + 6 && ev.clientX >= r.left - 9 && ev.clientX <= r.right + 9; });
    if (!hit) return;
    const r = hit.getBoundingClientRect(), fx = (ev.clientX - r.left) / Math.max(1, r.width), fy = (ev.clientY - r.top) / Math.max(1, r.height), row = st.rows.find(x => x.id === hit.dataset.id), me = st.rows.find(x => x.id === st.id);
    if (fx > 0.26 && fx < 0.74 && fy > 0.1 && fy < 0.9) {
      const why = !row.movable && row.locked ? `${row.name}: ${row.lockWhy}` : !me.canSource ? `${me.name} ${me.whySource}` : !row.canTarget ? `${row.name} ${row.whyTarget}` : '';
      hit.classList.add('onto'); if (why) hit.classList.add('refused');
      const tag = doc.createElement('span'); tag.className = 'osShTag' + (why ? ' refused' : ''); tag.textContent = why || `Combine onto ${row.name}`; hit.appendChild(tag);
      st.over = hit.dataset.id; st.mode = why ? 'refused:' + why : 'onto';
    } else {
      const before = fx <= 0.26 || (fx >= 0.74 ? false : fy <= 0.5); hit.classList.add(before ? 'before' : 'after'); st.over = hit.dataset.id; st.mode = before ? 'before' : 'after';
    }
  }
  function ask(M, html, yes, label) {
    const S = M.sheetsUI, box = S.el.querySelector('[data-sh="ask"]'); box.hidden = false;
    box.innerHTML = `<span>${html}</span><button type="button" class="btn ghost xs" data-sh-no>Cancel</button><button type="button" class="btn sage xs" data-sh-yes>${esc(label)}</button>`;
    box.querySelector('[data-sh-no]').onclick = () => closeAsk(M);
    box.querySelector('[data-sh-yes]').onclick = () => { const a = S.ask; closeAsk(M); yes(a); };
    box.querySelector('[data-sh-yes]').focus();
  }
  function drop(M, st, overId, mode) {
    const S = M.sheetsUI;
    if (mode && mode.startsWith('refused:')) return fail(M, st.id, mode.slice(8));
    if (mode === 'onto') {
      const src = st.rows.find(r => r.id === st.id), tgt = st.rows.find(r => r.id === overId);
      S.ask = { src, tgt };
      return ask(M, `Combine ${src.pieces} piece${src.pieces === 1 ? '' : 's'} from ${esc(src.name)} onto ${esc(tgt.name)}? What does not fit goes on to the next sheets, then stays on ${esc(src.name)}.${src.relay ? ' The computer that carries the run does it.' : ''}`, a => doCombine(M, a.src.id, a.tgt.id), 'Combine');
    }
    const ids = [...S.el.querySelectorAll('li.osSh')].map(x => x.dataset.id).filter(id => id !== st.id), at = ids.indexOf(overId);
    ids.splice(mode === 'before' ? at : at + 1, 0, st.id);
    doReorder(M, ids, st.id);
  }
  function closeAsk(M) { const S = M.sheetsUI; if (!S) return; S.ask = null; const box = S.el.querySelector('[data-sh="ask"]'); box.hidden = true; box.innerHTML = ''; paint(M, true); }

  /* ── empty a sheet / delete an empty one (one confirmation each) ── */
  function emptyAsk(M, id) {
    const row = rowsOf(M).find(r => r.id === id); if (!row || row.locked || !row.canSource) return;
    M.sheetsUI.ask = { empty: id };
    ask(M, `Empty ${esc(row.name)}? Its ${row.pieces} piece${row.pieces === 1 ? '' : 's'} move onto the other sheets in your order (the sheets before it first, then the ones after). What fits nowhere stays on ${esc(row.name)}.${row.relay ? ' The computer that carries the run does it.' : ''}`, a => doEmpty(M, a.empty), 'Empty it');
  }
  function del(M, id) {
    const row = rowsOf(M).find(r => r.id === id); if (!row || !row.canDelete) return;
    M.sheetsUI.ask = { del: id };
    ask(M, `Delete ${esc(row.name)}? It is empty. The sheets after it move up one.`, a => doDelete(M, a.del), 'Delete');
  }
  function destroy(M) { try { if (M && M.sheetsUI) { stopRel(M.sheetsUI); doc.documentElement.classList.remove('osShDrag'); M.sheetsUI.el.remove(); M.sheetsUI = null; } } catch (_) {} }
  root.OptionsSheets = { mount, paint, destroy };
})(window);
