/*  charm-nest-options-sheets.js — the Sheets card of the Options window (SHEETSLIST, Paul, 15 Nov 2026; reworked SHEETSANYWHERE the same day).
 *  SHEETSLIST: "I should be able to easily control in this screen: 1. consolidating sheets by dragging and dropping one sheet onto another. 2. deleting unnecessary sheets and easily being able to choose
 *   which sheet is to be draft 1, then which sheet is to be draft 2 etc. 3. The user should be able to choose sheet 1 2 3 4 in whatever order they decide by dragging the sheets around ..."
 *  SHEETSANYWHERE: "inside the options menu I just see two drafts that are locked ... I wanted the drafts to be part of the partial sheets view so then I can drag the sheets from one sheet to another and amalgamate
 *   sheets as needed, and then I need a way to choose which sheet is gonna be draft one ... I should be able to empty them out. Combine them with other sheets ..." Every sheet is a picture tile (as the partial
 *   sheets are); drag a tile BETWEEN two tiles to put it there, ONTO a tile to combine, "Empty this sheet" to move its pieces onto the others, Delete for an empty one.
 *  On the computer that carries the run the Gate does it (Gate.reorder / consolidate / emptySheet / deleteEmpty); on a computer that only follows the run the same gesture is SENT to the one that carries it
 *  (runCmdPut on the cloud; charm-nest-run-follow.js runCommands does it there and answers with runCmdDone) and the answer is shown here. This file draws and listens; it writes nothing itself.
 *  ONEGRID (Paul, 15 Nov 2026): "I wanted the partial sheets and the unused sheets, the custom sheets all to be at the bottom together and just identified correctly, and I should be able to move already
 *   partially and fully nested sheets onto new nested sheets and all those charms should move over, and then ... drag another partial sheet onto a partial sheet and have those parts be amalgamated,
 *   and then I should be able to delete the sheet that I just dropped onto another sheet if need be, or I might wanna keep it". There is no Sheets card any more: this run's sheets are the FIRST tiles
 *   of the one grid "All <metal> sheets" (the repository's cards follow in the same grid, drawn by charm-nest-options-modal.js). A run sheet dropped on an Available repository card moves all its pieces
 *   onto it (Gate.seatOn); dropped on another run sheet it is combined and KEPT, empty. A request sent to the carrying computer shows what it was, can be cancelled while it waits (runCmdCancel).
 *  window.OptionsSheets: mount(M) -> null · place(M) · paint(M) · destroy(M) */
(function (root) {
  'use strict';
  const doc = root.document; if (!doc || root.OptionsSheets) return;
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const G = () => root.Gate && typeof root.Gate.sheetsOf === 'function' ? root.Gate : null;
  const solid = m => m === 'gold10k' || m === 'gold14k';
  const NO_CARRIER = 'No computer is carrying the run right now: open the app on the computer that runs it, or press Resume here to carry it on this computer.';
  const CSS = `
.osShWrap{display:contents}
.ohcGrid:has(.osShWrap){grid-template-columns:repeat(auto-fill,minmax(min(100%,300px),1fr));gap:14px}
.ohcGrid:has(.osShWrap) .ohcBig{grid-column:1/-1}
.osShHead2{margin:6px 0 10px}.osShHead2 h4{margin:0 0 2px;font:500 20px var(--serif,inherit)}
.osKind{display:inline-block;align-self:flex-start;font-size:10.5px;font-weight:700;letter-spacing:.07em;text-transform:uppercase;padding:2px 9px;border-radius:99px;border:1px solid var(--line);color:var(--ink45,#777);background:var(--card2,transparent)}
.osKind.nest{border-color:var(--gold2,#b8913f);color:#7a5a1a}.osKind.empty{border-color:#8a8579;color:#5b554c}
.ohc>.osKind{margin:-6px 0 10px}
.ohc.onto{outline:3px solid var(--gold2,#b8913f);outline-offset:1px}.ohc.onto.refused{outline-color:#b3402e}
.osShWait{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
.osSh{position:relative;min-width:0;box-sizing:border-box;display:flex;flex-direction:column;gap:8px;padding:12px;border:1px solid var(--line);border-radius:14px;background:var(--paper2,transparent);outline:none;transition:box-shadow .14s,border-color .14s,transform .14s;touch-action:pan-y}
.osSh:focus-visible{box-shadow:0 0 0 2px var(--gold2)}
.osSh.fixed{background:repeating-linear-gradient(135deg,transparent 0 9px,rgba(120,110,90,.06) 9px 18px)}
.osShPic{position:relative;width:100%;max-width:340px;margin:0 auto;border-radius:8px;background:#fffefb;border:1px solid var(--line);overflow:hidden;cursor:grab;display:grid;place-items:center}
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
.osShAsk{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin:8px 0;font-size:13px}
.osShSaid{margin:8px 0;font-size:13px}
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

  const NAMES = { gold10k: '10K Gold', gold14k: '14K Gold' };
  /** The state is made here; the pieces (the head above the grid, the tiles inside the grid) are put in place by place(), after every draw of the repository. */
  function mount(M) {
    if (!M || !solid(M.m) || !G()) return null;
    style();
    const head = doc.createElement('div'); head.className = 'osShHead2'; head.dataset.card = 'sheets';
    head.innerHTML = `<h4 data-sh="title"></h4><p class="osLead" data-sh="lead"></p><div class="osShAsk" data-sh="ask" hidden></div><p class="osShSaid" data-sh="said" role="status" hidden></p>`;
    const wrap = doc.createElement('div'); wrap.className = 'osShWrap'; wrap.dataset.sh = 'list';
    const solo = doc.createElement('div'); solo.className = 'osCards ohcGrid';
    M.sheetsUI = { el: head, wrap, solo, sig: '', drag: null, ask: null, note: {}, rel: null, say: null, sub: null, poll: 0 };
    wrap.addEventListener('pointerdown', e => down(M, e));
    wrap.addEventListener('keydown', e => key(M, e));
    wrap.addEventListener('click', e => {
      const d = e.target.closest && e.target.closest('[data-sh-del]'); if (d) return del(M, d.closest('.osSh').dataset.id);
      const m = e.target.closest && e.target.closest('[data-sh-empty]'); if (m) emptyAsk(M, m.closest('.osSh').dataset.id);
    });
    head.addEventListener('click', e => { if (e.target.closest && e.target.closest('[data-sh-cancel]')) cancelRel(M); });
    return null;
  }
  const gridOf = M => M.sheetsUI.wrap.parentNode;
  /** Put the head above the results and the tiles first in the grid (the repository's own grid when it has cards, else a grid of their own). Called after every draw of the repository. */
  function place(M) {
    const S = M && M.sheetsUI; if (!S || !M.source) return;
    const res = M.source.querySelector('[data-os="results"]'), host = M.host; if (!res) return;
    if (S.el.parentNode !== res.parentNode || S.el.nextSibling !== res) res.parentNode.insertBefore(S.el, res);
    if (host && host.parentNode === res) { if (S.solo.parentNode) S.solo.remove(); if (S.wrap.parentNode !== host || host.firstChild !== S.wrap) host.prepend(S.wrap); }
    else { if (S.solo.parentNode !== res || res.firstChild !== S.solo) res.prepend(S.solo); if (S.wrap.parentNode !== S.solo) S.solo.appendChild(S.wrap); }
    paint(M);
  }
  const rowsOf = M => { try { return G().sheetsOf(M.m); } catch (_) { return []; } };
  const relayed = M => { const r = rowsOf(M); return !!(r.length && r[0].relay); };

  function pictures(S, rows) {
    for (const r of rows) {
      const box = S.wrap.querySelector(`[data-pic="${CSS_ESC(r.id)}"]`); if (!box) continue;
      if (r.live) {
        const cv = doc.createElement('canvas'), pg = G().pageOf && G().pageOf(r.id), C = root.CN;
        try { cv.width = Math.round(240 * Math.min(2, root.devicePixelRatio || 1)); cv.height = Math.max(20, Math.round(cv.width / (r.aspect || 2))); C.paintPreview(cv, pg, true, 0); box.innerHTML = ''; box.appendChild(cv); } catch (_) { box.innerHTML = '<span class="osShNone">picture not ready</span>'; }
      } else if (r.picture) {
        const img = doc.createElement('img'); img.alt = r.name; img.decoding = 'async'; img.loading = 'lazy'; img.onerror = () => { box.innerHTML = '<span class="osShNone">picture not available</span>'; }; img.crossOrigin = 'anonymous'; box.innerHTML = ''; box.appendChild(img);   /* (the page is cross-origin isolated: a picture from the cloud's storage is loaded through the app's own loader, as the sheet cards do; a plain src is blocked) */ try { const A = root.CharmNestAssets; if (A && typeof A.sheetImage === 'function') Promise.resolve(A.sheetImage(img, r.id, r.picture)).catch(() => img.onerror()); else img.src = r.picture; } catch (_) { img.src = r.picture; }
      } else box.innerHTML = '<span class="osShNone">' + (r.pieces ? 'picture loading…' : 'empty sheet') + '</span>';
    }
  }
  const CSS_ESC = v => String(v).replace(/["\\]/g, '\\$&');

  const ST = () => (root.OptionsStudio && typeof root.OptionsStudio.state === 'function' ? root.OptionsStudio.state().all : null) || {};
  const cardInfo = id => { try { return root.OptionsStudio && OptionsStudio.cardOf ? OptionsStudio.cardOf(id) : null; } catch (_) { return null; } };
  const cardLabel = c => c ? `${c.kind === 'new' ? 'new sheet' : 'partial sheet'} ${Math.round(+c.wMm) || '?'} × ${Math.round(+c.hMm) || '?'} mm` : 'that sheet';
  const plural = n => `${n} piece${n === 1 ? '' : 's'}`;
  const ago = at => { const m = Math.floor((Date.now() - at) / 60000); return m < 1 ? 'just now' : m === 1 ? '1 min ago' : m + ' min ago'; };
  const relWords = r => {
    if (!r.sent) return r.text;
    return `${r.label || 'A request'} — sent to ${r.name} ${ago(r.at)}, waiting for that computer` + (r.warn ? '. ' + r.warn : '') + (Date.now() - r.at > 60000 ? '. That computer has not answered: check Brites Manager is open on it (reload it once if it was open since before today\'s update)' : '');
  };

  function paint(M, force) {
    const S = M && M.sheetsUI; if (!S || !G()) return;
    if (S.drag || S.ask) return;
    try { if (!relayed(M) && G().ensureOrder) G().ensureOrder(M.m); } catch (_) {}   // (ORDERCLOUD: the first time the grid shows on the carrier, the order is written once so every computer numbers the drafts alike)
    const f = ST().metal, hide = !!f && f !== M.m, rows = hide ? [] : rowsOf(M);
    const said = G().sheetsSaid ? G().sheetsSaid(M.m) : null, showSaid = S.rel || S.say ? '' : said && Date.now() - said.at < 600000 ? said.text : '';
    const sig = JSON.stringify([rows, showSaid, S.note, S.rel, S.say, S.rel && S.rel.spin ? Math.floor((Date.now() - S.rel.at) / 15000) : 0, hide, M.m]);
    if (!force && sig === S.sig) return; S.sig = sig;
    S.el.querySelector('[data-sh="title"]').textContent = `All ${NAMES[M.m] || ''} sheets`;
    const arrange = rows.length ? rows[0].arrange : true;
    S.el.querySelector('[data-sh="lead"]').textContent = arrange ? 'This run\'s sheets come first, then the sheets in the repository. Drag a run sheet between two run sheets to reorder, onto another run sheet to combine (the one you drop stays, empty), or onto an Available sheet to move all its pieces onto it.' : 'There is no open run to arrange sheets in.';
    const keepFocus = S.wrap.contains(doc.activeElement) && doc.activeElement.closest ? (doc.activeElement.closest('.osSh') || {}).dataset : null;
    S.wrap.innerHTML = rows.map(r => {
      const badges = [r.inSet ? 'In set' : '', r.cutLine ? 'Cut line drawn' : '', r.cut ? 'Cut' : '', r.locked ? 'Locked' : ''].filter(Boolean).map(b => `<span class="osShBadge${b === 'Locked' ? ' lock' : ''}">${b}</span>`).join('');
      const empty0 = !r.pieces, why = S.note[r.id] || (r.locked ? r.lockWhy : !r.canDelete && r.whyDelete && /^Move its/.test(r.whyDelete) ? '' : !r.movable && r.arrange ? (r.whySource || r.whyTarget || '') : '');
      const empty = r.arrange && !r.locked && r.pieces > 0 && r.canSource;
      const pieces = `${plural(r.pieces)}${r.pct == null ? '' : ' · ' + r.pct + '% full'}${r.busy ? ' · working…' : ''}`;
      const kind = empty0 ? '<span class="osKind empty">Empty sheet · in this run</span>' : '<span class="osKind nest">Nested · in this run</span>';
      const keptNote = empty0 && r.arrange && !r.locked ? '<span class="osShNote">Kept: new pieces can be nested on it. Drop another run sheet on it to move pieces here.</span>' : '';
      return `<div class="osSh${r.movable ? '' : ' fixed'}" data-id="${esc(r.id)}" tabindex="0" aria-label="${esc(r.name)}, nested in this run, ${r.pieces} pieces${r.pct == null ? '' : ', ' + r.pct + ' percent full'}${r.locked ? ', locked: ' + esc(r.lockWhy) : ''}. Alt plus Up or Down arrow moves it.">
        ${kind}
        <div class="osShPic" data-pic="${esc(r.id)}" style="aspect-ratio:${r.aspect || 2}" title="${r.movable ? 'Drag to move or combine' : esc(why || 'This sheet keeps its place')}"></div>
        <div class="osShHead"><span class="osShName">${esc(r.name)}</span></div>
        <span class="osShMeta">${esc(r.size)}<br>${pieces}</span>
        <span class="osShBar" aria-hidden="true"><i style="width:${r.pct == null ? 0 : r.pct}%"></i></span>
        <span class="osShBadges">${badges}</span>
        ${why ? `<span class="osShNote${S.note[r.id] ? ' bad' : ''}">${esc(why)}</span>` : keptNote}
        <span class="osShActs">${empty ? '<button type="button" class="btn ghost xs" data-sh-empty>Empty this sheet</button>' : ''}${r.canDelete ? '<button type="button" class="btn ghost xs" data-sh-del>Delete this sheet</button>' : ''}</span></div>`;
    }).join('');
    pictures(S, rows);
    const sd = S.el.querySelector('[data-sh="said"]'), cur = S.rel || S.say, text = cur ? (S.rel ? relWords(S.rel) : S.say.text) : showSaid;
    sd.hidden = !text; sd.classList.toggle('bad', !!(cur && cur.bad));
    sd.innerHTML = (cur && cur.spin ? '<span class="osShSpin" aria-hidden="true"></span>' : '') + esc(text) + (S.rel && S.rel.sent && S.rel.spin ? ' <button type="button" class="btn ghost xs" data-sh-cancel>Cancel this request</button>' : '');
    if (keepFocus && keepFocus.id) { const n = [...S.wrap.querySelectorAll('.osSh')].find(x => x.dataset.id === keepFocus.id); if (n) { try { n.focus({ preventScroll: true }); } catch (_) {} } }
  }
  const fail = (M, id, why) => { const S = M.sheetsUI; if (!S) return; S.note[id] = why; paint(M, true); setTimeout(() => { if (S.note[id] === why) { delete S.note[id]; paint(M, true); } }, 6000); };
  const sayFor = (M, say, ms) => { const S = M.sheetsUI; if (!S) return; S.say = say; paint(M, true); if (say && !say.spin) setTimeout(() => { if (S.say === say) { S.say = null; paint(M, true); } }, ms || 90000); };

  /* ── the computer that only follows the run: the gesture is sent to the one that carries it ── */
  function stopRel(S) { if (S.poll) { clearInterval(S.poll); S.poll = 0; } if (S.sub) { try { S.sub.stop(); } catch (_) {} S.sub = null; } }
  const endRel = (M, rel) => { const S = M.sheetsUI; stopRel(S); S.rel = rel; paint(M, true); setTimeout(() => { if (S.rel === rel) { S.rel = null; paint(M, true); } }, 90000); };
  async function readRel(M) {
    const S = M.sheetsUI; if (!S || !S.rel || !S.rel.id || !S.rel.spin) return;
    const rel = S.rel; let res = null; try { res = await api({ op: 'runCmdList', runId: rel.runId }); } catch (_) { return; }
    const c = ((res && res.cmds) || []).find(x => x && x.id === rel.id);
    if (S.rel !== rel) return;
    if (c && (c.state === 'done' || c.state === 'failed')) {
      const bad = c.state === 'failed'; endRel(M, { text: c.text || (bad ? 'The computer that carries the run could not do that.' : 'Done'), bad, spin: false });
      if (rel.kind === 'seat' && !bad) { try { root.OptionsStudio && OptionsStudio.reload && OptionsStudio.reload(); } catch (_) {} }
    } else if (c && c.state === 'cancelled') endRel(M, { text: `Cancelled: ${rel.label || 'the request'}. Nothing was done.`, bad: false, spin: false });
    else if (Date.now() - rel.at > 11 * 60000) endRel(M, { text: 'The computer that carries the run did not answer. Look at the sheets again, then try once more.', bad: true, spin: false });
    else paint(M);
  }
  async function cancelRel(M) {
    const S = M.sheetsUI, r = S && S.rel; if (!r || !r.sent || !r.spin) return;
    let res = null; try { res = await api({ op: 'runCmdCancel', runId: r.runId, id: r.id, dev: devId() }); } catch (e) { r.warn = 'It could not be cancelled (' + ((e && e.message) || e) + ')'; return paint(M, true); }
    if (S.rel !== r) return;
    if (res && res.cancelled) endRel(M, { text: `Cancelled: ${r.label || 'the request'}. Nothing was done.`, bad: false, spin: false });
    else { r.warn = 'It has already started on that computer, so it cannot be cancelled'; paint(M, true); readRel(M); }
  }
  async function send(M, cmd, label) {
    const S = M.sheetsUI; if (!S) return false;
    if (S.rel && S.rel.spin) { S.rel.warn = 'Cancel it first (or wait for it to finish) before asking for something else'; paint(M, true); return false; }
    const rows = rowsOf(M), runId = (rows[0] && rows[0].runId) || (RF() && RF().state().runId), dev = devId();
    if (!runId || !dev) { S.rel = { text: 'There is no open run to send this to.', bad: true, spin: false }; paint(M, true); return false; }
    S.rel = { text: 'Finding the computer that carries the run…', spin: true, runId, at: Date.now() }; paint(M, true);
    try {
      const c = await api({ op: 'runCarrier', runId, dev, check: true });
      if (!c || !c.alive || c.mine) { S.rel = { text: NO_CARRIER, bad: true, spin: false }; paint(M, true); return false; }
      const name = (c.carrier && c.carrier.by) || 'the computer that carries the run';
      const put = await api({ op: 'runCmdPut', runId, cmd: Object.assign({ metal: M.m, label }, cmd), by: who(), dev });
      if (!put || put.error || !put.id) throw new Error((put && put.error) || 'not sent');
      S.rel = { id: put.id, runId, name, at: Date.now(), spin: true, sent: true, label, kind: cmd.type }; paint(M, true);
      if (root.CNSync && typeof root.CNSync.follow === 'function' && !S.sub) { try { S.sub = root.CNSync.follow({ name: 'run-cmds', kinds: 'r', wants: k => k === 'r:' + runId, read: () => readRel(M) }); } catch (_) { S.sub = null; } }
      if (!S.poll) S.poll = setInterval(() => readRel(M), 8000);
      readRel(M); return true;
    } catch (e) { S.rel = { text: 'Not sent: ' + ((e && e.message) || e), bad: true, spin: false }; paint(M, true); return false; }
  }

  /* ── the gestures: the Gate here, or a command sent to the computer that carries the run ── */
  async function doReorder(M, ids, id, label) {
    if (relayed(M)) return void send(M, { type: 'reorder', ids }, label || 'Change the order of the sheets');
    const r = G().reorder(M.m, ids); if (r && r.ok === false) fail(M, id, r.why); else paint(M, true);
  }
  async function doCombine(M, srcId, tgtId, label) {
    if (relayed(M)) return void send(M, { type: 'consolidate', sourceId: srcId, targetId: tgtId }, label);
    const r = await G().consolidate(srcId, tgtId); if (r && r.ok === false && r.why) fail(M, srcId, r.why); paint(M, true);
  }
  async function doEmpty(M, id, label) {
    if (relayed(M)) return void send(M, { type: 'empty', sheetId: id }, label);
    const r = await G().emptySheet(id); if (r && r.ok === false && r.why) fail(M, id, r.why); paint(M, true);
  }
  async function doDelete(M, id, label) {
    if (relayed(M)) return void send(M, { type: 'delete', sheetId: id }, label);
    const r = await G().deleteEmpty(id); if (r && r.ok === false) fail(M, id, r.why); paint(M, true);
  }
  const nameOfRow = (M, id) => { const r = rowsOf(M).find(x => x.id === id); return r ? r.name : 'a sheet'; };

  /* ── a run sheet onto a physical sheet of the repository: all its pieces move onto it (Gate.seatOn; PartialNest does the work) ── */
  async function seatAsk(M, me, pid) {
    const S = M.sheetsUI, c = cardInfo(pid), label = cardLabel(c), n = me.pieces, onStep = st => { if (st && st.text && S.say && S.say.spin) { S.say.text = st.text; paint(M, true); } };
    let words = '';
    sayFor(M, { text: `Checking whether ${me.name}'s ${plural(n)} fit on that ${label}…`, spin: true });
    if (relayed(M)) {
      const pg = G().pageOf && G().pageOf(me.id), PN = root.PartialNest;
      if (pg && pg.charms && pg.charms.length && PN && PN.preview && PN.canSeat) {
        try { const can = PN.canSeat(pg); if (!can.ok) return sayFor(M, { text: can.why, bad: true }); const r = await PN.preview(pg, [pid], { onStep }); if (!r || !r.ok) return sayFor(M, { text: (r && (r.why || r.reason)) || 'That sheet cannot take them now.', bad: true }); words = r.words || ''; }
        catch (e) { return sayFor(M, { text: String((e && e.message) || e), bad: true }); }
      }
    } else {
      const r = await G().seatOn(me.id, pid, { preview: true, onStep }); if (!r || !r.ok) return sayFor(M, { text: (r && r.why) || 'That sheet cannot take them now.', bad: true }); words = r.text || '';
    }
    S.say = null;
    S.ask = { seat: true };
    ask(M, `Move ${esc(me.name)}'s ${plural(n)} onto this ${esc(label)}? ${words ? esc(words) : ''}${relayed(M) && !words ? 'The computer that carries the run does it.' : ''}`, () => doSeat(M, me, pid, label, words), 'Move them', 'Cancel');
  }
  async function doSeat(M, me, pid, label, words) {
    const S = M.sheetsUI, n = me.pieces, head = `${me.name}'s ${plural(n)}`;
    if (relayed(M)) return void send(M, { type: 'seat', sheetId: me.id, partialId: pid }, `Move ${head} onto a ${label}`);
    sayFor(M, { text: `${head} are being nested on this ${label}…`, spin: true });
    const r = await G().seatOn(me.id, pid, { onStep: st => { if (st && st.text && S.say && S.say.spin) { S.say.text = `${head}: ${st.text}`; paint(M, true); } } });
    if (r && r.ok) { sayFor(M, { text: `${head} are being nested on this ${label}. ${words || ''}`.trim() }); try { root.OptionsStudio && OptionsStudio.reload && OptionsStudio.reload(); } catch (_) {} }
    else sayFor(M, { text: (r && r.why) || 'The pieces were not moved.', bad: true });
  }
  const whyCard = (M, me, c) => {
    if (!c) return 'That sheet is not in the list. Press Refresh.';
    if (c.metal !== M.m) return `That is ${NAMES[c.metal] || 'another metal'}: ${me.name} is ${NAMES[M.m] || ''}.`;
    const st = c.status || 'available';
    if (st === 'inUse') return `Taken by ${c.inUseBySheetName || 'another sheet'}: only an Available sheet can take pieces.`;
    if (st !== 'available') return `${st === 'used' ? 'Used' : st === 'discarded' ? 'Discarded' : 'Deleted'}: only an Available sheet can take pieces.`;
    if (!me.pieces) return `${me.name} has no pieces to move.`;
    if (!me.canSource) return `${me.name} ${me.whySource}`;
    return '';
  };

  /* ── keyboard: Alt+ArrowUp / Alt+ArrowLeft move a focused tile earlier, Alt+ArrowDown / Alt+ArrowRight later ── */
  function key(M, e) {
    const k = e.key; if (!e.altKey || !['ArrowUp', 'ArrowDown', 'ArrowLeft', 'ArrowRight'].includes(k)) return;
    const li = e.target.closest && e.target.closest('.osSh'); if (!li) return; e.preventDefault();
    const ids = [...M.sheetsUI.wrap.querySelectorAll('.osSh')].map(x => x.dataset.id), i = ids.indexOf(li.dataset.id), j = i + (k === 'ArrowUp' || k === 'ArrowLeft' ? -1 : 1);
    if (j < 0 || j >= ids.length) return;
    ids.splice(i, 1); ids.splice(j, 0, li.dataset.id);
    Promise.resolve(doReorder(M, ids, li.dataset.id, `Swap the order: ${nameOfRow(M, li.dataset.id)} moves ${j < i ? 'earlier' : 'later'}`)).then(() => { const n = [...M.sheetsUI.wrap.querySelectorAll('.osSh')].find(x => x.dataset.id === li.dataset.id); if (n) n.focus({ preventScroll: true }); });
  }

  /* ── pointer: the picture drags at once; the rest of the tile after 6 px. The grid scrolls while a tile is held near its top or bottom edge. ── */
  const scroller = el => { for (let n = el; n && n !== doc.body; n = n.parentNode) { if (n.nodeType === 1) { const o = getComputedStyle(n).overflowY; if ((o === 'auto' || o === 'scroll') && n.scrollHeight > n.clientHeight) return n; } } return doc.scrollingElement; };
  function down(M, e) {
    if (e.button != null && e.button > 0) return;
    const li = e.target.closest && e.target.closest('.osSh'); if (!li || (e.target.closest && e.target.closest('button'))) return;
    const row = rowsOf(M).find(r => r.id === li.dataset.id); if (!row || !row.movable) return;
    const S = M.sheetsUI, st = { li, id: row.id, x: e.clientX, y: e.clientY, on: false, rows: null, over: null, mode: null, pid: e.pointerId, last: null, tick: 0 };
    const grip = !!(e.target.closest && e.target.closest('.osShPic'));
    const stop = () => { if (st.tick) { clearInterval(st.tick); st.tick = 0; } };
    const move = ev => {
      if (!st.on) { if (!grip && Math.hypot(ev.clientX - st.x, ev.clientY - st.y) < 6) return; st.on = true; S.drag = st; st.rows = rowsOf(M); li.classList.add('dragging'); doc.documentElement.classList.add('osShDrag');
        st.tick = setInterval(() => { const ev2 = st.last; if (!ev2) return; const sc = scroller(S.wrap.parentNode), h = root.innerHeight || 800, dy = ev2.clientY < 70 ? -14 : ev2.clientY > h - 70 ? 14 : 0; if (dy && sc) { const was = sc.scrollTop; sc.scrollTop += dy; if (sc.scrollTop !== was) { st.y -= 0; target(M, st, ev2); } } }, 30); }
      st.last = ev; li.style.transform = `translate(${ev.clientX - st.x}px,${ev.clientY - st.y}px)`; target(M, st, ev);
    };
    const end = ev => {
      doc.removeEventListener('pointermove', move, true); doc.removeEventListener('pointerup', end, true); doc.removeEventListener('pointercancel', end, true); stop();
      if (!st.on) return; doc.documentElement.classList.remove('osShDrag'); li.classList.remove('dragging'); li.style.transform = ''; S.drag = null;
      const over = st.over, mode = st.mode; clear(M);
      if (ev.type === 'pointercancel' || !over) return paint(M, true);
      drop(M, st, over, mode);
    };
    doc.addEventListener('pointermove', move, true); doc.addEventListener('pointerup', end, true); doc.addEventListener('pointercancel', end, true);
  }
  const clear = M => { const g = gridOf(M) || M.sheetsUI.wrap; for (const n of g.querySelectorAll('.onto,.before,.after,.refused')) n.classList.remove('onto', 'before', 'after', 'refused'); for (const t of g.querySelectorAll('.osShTag')) t.remove(); };
  function target(M, st, ev) {
    clear(M); st.over = null; st.mode = null;
    const grid = gridOf(M); if (!grid) return;
    const tiles = [...M.sheetsUI.wrap.querySelectorAll('.osSh')].filter(n => n !== st.li), cards = [...grid.querySelectorAll(':scope > .ohc')];
    const hit = tiles.concat(cards).find(n => { const r = n.getBoundingClientRect(); return r.width > 0 && ev.clientY >= r.top - 6 && ev.clientY <= r.bottom + 6 && ev.clientX >= r.left - 9 && ev.clientX <= r.right + 9; });
    if (!hit) return;
    const me = st.rows.find(x => x.id === st.id);
    if (hit.classList.contains('ohc')) {   // a physical sheet of the repository: the whole card is the target
      const why = whyCard(M, me, cardInfo(hit.dataset.id));
      hit.classList.add('onto'); if (why) hit.classList.add('refused');
      const tag = doc.createElement('span'); tag.className = 'osShTag' + (why ? ' refused' : ''); tag.textContent = why || `Move ${me.name}'s ${plural(me.pieces)} onto this sheet`; hit.appendChild(tag);
      st.over = hit.dataset.id; st.mode = why ? 'refused:' + why : 'seat'; return;
    }
    const r = hit.getBoundingClientRect(), fx = (ev.clientX - r.left) / Math.max(1, r.width), fy = (ev.clientY - r.top) / Math.max(1, r.height), row = st.rows.find(x => x.id === hit.dataset.id);
    if (fx > 0.26 && fx < 0.74 && fy > 0.1 && fy < 0.9) {
      const why = !row.movable && row.locked ? `${row.name}: ${row.lockWhy}` : !me.canSource ? `${me.name} ${me.whySource}` : !row.canTarget ? `${row.name} ${row.whyTarget}` : '';
      hit.classList.add('onto'); if (why) hit.classList.add('refused');
      const tag = doc.createElement('span'); tag.className = 'osShTag' + (why ? ' refused' : ''); tag.textContent = why || `Combine onto ${row.name}`; hit.appendChild(tag);
      st.over = hit.dataset.id; st.mode = why ? 'refused:' + why : 'onto';
    } else {
      const before = fx <= 0.26 || (fx >= 0.74 ? false : fy <= 0.5); hit.classList.add(before ? 'before' : 'after'); st.over = hit.dataset.id; st.mode = before ? 'before' : 'after';
    }
  }
  function ask(M, html, yes, label, no) {
    const S = M.sheetsUI, box = S.el.querySelector('[data-sh="ask"]'); box.hidden = false;
    box.innerHTML = `<span>${html}</span><button type="button" class="btn ghost xs" data-sh-no>${esc(no || 'Cancel')}</button><button type="button" class="btn sage xs" data-sh-yes>${esc(label)}</button>`;
    box.querySelector('[data-sh-no]').onclick = () => closeAsk(M);
    box.querySelector('[data-sh-yes]').onclick = () => { const a = S.ask; closeAsk(M); yes(a); };
    box.querySelector('[data-sh-yes]').focus();
    try { box.scrollIntoView({ block: 'nearest' }); } catch (_) {}
  }
  function drop(M, st, overId, mode) {
    const S = M.sheetsUI;
    if (mode && mode.startsWith('refused:')) return fail(M, st.id, mode.slice(8));
    const src = st.rows.find(r => r.id === st.id);
    if (mode === 'seat') return void seatAsk(M, src, overId);
    if (mode === 'onto') {
      const tgt = st.rows.find(r => r.id === overId);
      S.ask = { src, tgt };
      return ask(M, `Combine ${plural(src.pieces)} from ${esc(src.name)} onto ${esc(tgt.name)}? What does not fit goes on to the next sheets, then stays on ${esc(src.name)}. ${esc(src.name)} is kept, empty, until you delete it.${src.relay ? ' The computer that carries the run does it.' : ''}`, a => doCombine(M, a.src.id, a.tgt.id, `Combine ${src.name} onto ${tgt.name}`), 'Combine');
    }
    const ids = [...S.wrap.querySelectorAll('.osSh')].map(x => x.dataset.id).filter(id => id !== st.id), at = ids.indexOf(overId);
    ids.splice(mode === 'before' ? at : at + 1, 0, st.id);
    doReorder(M, ids, st.id, `Swap the order: ${src.name} goes ${mode === 'before' ? 'before' : 'after'} ${nameOfRow(M, overId)}`);
  }
  function closeAsk(M) { const S = M.sheetsUI; if (!S) return; S.ask = null; const box = S.el.querySelector('[data-sh="ask"]'); box.hidden = true; box.innerHTML = ''; paint(M, true); }

  /* ── empty a sheet / delete an empty one (one confirmation each) ── */
  function emptyAsk(M, id) {
    const row = rowsOf(M).find(r => r.id === id); if (!row || row.locked || !row.canSource) return;
    M.sheetsUI.ask = { empty: id };
    ask(M, `Empty ${esc(row.name)}? Its ${plural(row.pieces)} move onto the other sheets in your order (the sheets before it first, then the ones after). What fits nowhere stays on ${esc(row.name)}.${row.relay ? ' The computer that carries the run does it.' : ''}`, a => doEmpty(M, a.empty, `Empty ${row.name}`), 'Empty it');
  }
  function del(M, id) {
    const row = rowsOf(M).find(r => r.id === id); if (!row || !row.canDelete) return;
    M.sheetsUI.ask = { del: id };
    ask(M, `Delete ${esc(row.name)}? It is empty. The sheets after it move up one.`, a => doDelete(M, a.del, `Delete ${row.name}`), 'Delete this sheet');
  }
  function destroy(M) { try { if (M && M.sheetsUI) { stopRel(M.sheetsUI); doc.documentElement.classList.remove('osShDrag'); M.sheetsUI.el.remove(); M.sheetsUI.wrap.remove(); M.sheetsUI.solo.remove(); M.sheetsUI = null; } } catch (_) {} }
  root.OptionsSheets = { mount, place, paint, destroy };
})(window);
