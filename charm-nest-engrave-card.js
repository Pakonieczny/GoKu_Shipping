/* The engraving CARD of ONE PIECE (Paul, 12 Oct 2026: "show the cards for each piece that needs to be engraved ... the cards are currently full of needless text ... front and back
 * side-by-side visible in a larger card so that the user can automatically understand that there's a front engraving and a back engraving as part of the same piece ... keep it very
 * simple and minimalist"). The contract is plans/engrave-ui-1012/CONTRACT.md section C.
 *
 *   CharmNestEngraveCard.piecesOf(rows, o) -> [Piece]      one entry per PIECE of the lines given (a charm, one ear, one disc), each with its Front and its Back
 *       Piece = { pieceKey, rid, lineKey, slot, row, title, never, state, faces: { front: Face|null, back: Face|null }, jobs: [job], approve: { jobs, ok, why }|null, seals: [seal] }
 *       Face  = { face, key, job|null, text, fontWord, fontId, position, state, working, foreign? }
 *       state (of a face and of the card, the worst of its faces): words | busy | approve | approved | plain.   o: { nameOf(row), isWorking(job), seen(job), events }
 *       A face the line does not engrave is null (a quiet empty slot). Back is ALWAYS null for an earring (CharmNestEngraveFaces.neverOf): an earring has no card at all unless a decided
 *       front job is already there, and then only its Front.
 *   CharmNestEngraveCard.html(piece, o)      the card's markup (o.approve === false: no Approve / Approved block, the Engrave detail's card; o.current: the face shown, aria-current)
 *   CharmNestEngraveCard.fill(card, piece, o)  puts each face's picture into its thumbnail (CharmNestEngravePreview.render when it exists, else the app's own drawing: one place, thumbOf)
 *   CharmNestEngraveCard.sig(piece, o)       what the card shows, in a line (drawn again only when it changes)
 *   CharmNestEngraveCard.mountPiece(slot, { row, key, face, onPick(face), approve:false, nameOf }) -> { update({ face }), refresh(), destroy(), el }   the card in the Engrave tab's detail
 *       (a press on a face calls onPick(face) and navigates nowhere)
 *   CharmNestEngraveCard.stateOf(job) / facesOf(...)  the small pure parts
 * The words on the card: the piece name, "Front", "Back", the engraved text, "Approve" / "Approved", "Words to confirm" (a face whose words are not settled), "Preparing" (with its small
 * spinner, while a face is being fitted) and the one failure line of an approval that did not go through. Everything else lives in the Engrave tab. Loads in the page and in node. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestEngraveCard = api;
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const dep = (name, file) => { const g = root && root[name]; if (g) return g; try { return typeof require === 'function' ? require(file) : null; } catch (_) { return null; } };
  const Faces = () => dep('CharmNestEngraveFaces', './charm-nest-engrave-faces.js');
  const Setup = () => (root && root.CharmNestEngraveSetup) || null;
  const tryDo = (f, d) => { try { return f(); } catch (_) { return d; } };
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const DECIDED = ['approved', 'written', 'skipped', 'none'];
  const WORD = { words: 'Words to confirm', busy: 'Preparing', approve: 'To approve', approved: 'Approved', plain: 'No engraving' };
  const RANK = { words: 5, busy: 4, approve: 3, approved: 2, plain: 1 };
  const faceOf = x => { const F = Faces(); return F && F.faceOf ? F.faceOf(x) : x && (x.face === 'front' || /~front$/.test(String(x.key || ''))) ? 'front' : 'back'; };
  const jobKey = (lineKey, slot, face) => { const F = Faces(); return F && F.jobKey ? F.jobKey(lineKey, slot, face) : String(lineKey) + (slot ? '#' + slot : '') + (face === 'front' ? '~front' : ''); };
  const slotLabel = s => { const F = Faces(); return F && F.slotLabel ? F.slotLabel(s) : s === 'L' ? 'Left' : s === 'R' ? 'Right' : /^D\d+$/.test(s || '') ? 'Disc ' + s.slice(1) : ''; };
  const lineOf = row => (row && row.parentRow) || row || null;
  const linesText = v => (Array.isArray(v) ? v : String(v == null ? '' : v).split(/\r?\n/)).map(x => String(x).replace(/\s+/g, ' ').trim()).filter(Boolean);

  /* ── the small pure parts ──────────────────────────────────────────────── */
  /** The state of one face from its job: words (to confirm) | busy (being read or fitted) | approve | approved | plain. A face with no job yet but asked for is busy. */
  function stateOf(job, o) {
    if (!job) return 'busy';
    const s = job.state, working = !!(o && o.isWorking && tryDo(() => o.isWorking(job), false));
    if (s === 'approved' || s === 'written') return 'approved';
    if (s === 'skipped' || s === 'none') return 'plain';
    if (s === 'review') return 'approve';
    if (s === 'ready' || s === 'fitting') return 'busy';
    if (s === 'classify' || s === 'reclassify') return working ? 'busy' : 'words';
    return 'words';   // words, blocked
  }
  const worst = states => states.filter(Boolean).reduce((a, b) => (!a || RANK[b] > RANK[a] ? b : a), '') || 'plain';
  const wordsOfJob = j => linesText((j && j.lines && j.lines.length ? j.lines : j && j.text) || '').join('\n');

  /** The faces of one piece. `jobs`: the piece's jobs (any face); `setup`: the line's setup (faces it asks for, with the words it has), for a face with no job yet. */
  function facesOf(lineKey, slot, jobs, setup, o) {
    o = o || {}; const never = !!o.never, P = root && root.CharmNestEngravePreview, Pos = root && root.CharmNestEngravePosition, out = { front: null, back: null };
    for (const face of ['front', 'back']) {
      if (face === 'back' && never) continue;                                   // (an earring never has a back slot, whatever a job says)
      const job = (jobs || []).find(j => faceOf(j) === face) || null, asks = !!(setup && setup.faces && setup.faces.includes(face)), w = setup && setup[face];
      if (!job && !asks) continue;
      if (job && (job.state === 'none' || job.state === 'skipped')) { out[face] = { face, key: job.key, job, text: '', fontWord: null, fontId: null, position: null, state: 'plain', working: false, plain: true }; continue; }
      const text = job ? wordsOfJob(job) || (w && w.lines ? w.lines.join('\n') : '') : (w && w.lines ? w.lines.join('\n') : '');
      const fontWord = (job && job.faceFontWord) || (w && w.fontWord) || null;
      const row = job ? job.row : o.row;
      out[face] = { face, key: job ? job.key : jobKey(lineKey, slot, face), job, text, fontWord,
        fontId: P && P.fontIdOf ? tryDo(() => P.fontIdOf(lineOf(row) || row, face, fontWord), null) : null,
        position: job && Pos && Pos.of ? tryDo(() => Pos.of(job), null) : null,
        state: stateOf(job, o), working: !!(job && o.isWorking && tryDo(() => o.isWorking(job), false)) };
    }
    return out;
  }

  /** The seals of a piece, every face's: the one merge the rest of the page uses (CNEngravingSeals.merge reads a job, its record, its saved backs). */
  function sealsOf(jobs, extra) {
    const S = root && root.CNEngravingSeals; if (!S || !S.merge) return [];
    const recs = []; for (const j of jobs || []) recs.push(j, j && j.row && j.row.engrave, ...((j && j.backs) || []), j && j.editOriginal);
    return tryDo(() => S.merge(...recs, ...(extra || [])), []) || [];
  }

  /** The pieces of the lines given. A line with jobs: one piece for each slot (a pair's ears, a counted line's discs; a plain line is one piece), Front and Back from its jobs. A line with no
   *  job yet but a setup that asks for faces (a person's choice, the intake's reading): one piece from the setup. A line that engraves nothing gives no piece. */
  function piecesOf(rows, o) {
    o = o || {}; const E = root && root.Engrave, F = Faces(), S = Setup(), out = [];
    const list = (typeof rows === 'function' ? rows() : rows) || [];
    for (const r0 of Array.isArray(list) ? list : [list]) {
      const line = lineOf(r0); if (!line || line.state === 'gone') continue;
      const rid = String((line.order && line.order.receiptId) || ''), never = !!(F && F.neverOf && F.neverOf(line));
      const name = o.nameOf ? tryDo(() => o.nameOf(line), '') : '';
      const jobs = (E && E.jobsOf ? tryDo(() => E.jobsOf(line), []) : []).filter(j => j && !j.editingBack);
      const setup = tryDo(() => (S && S.setupFor ? S.setupFor(line) : F && F.setupOf ? F.setupOf(line) : null), null);
      const groups = jobs.length && F && F.groupPieces ? F.groupPieces(jobs) : [];
      const make = (slot, pjobs) => {
        const faces = facesOf(line.key, slot, pjobs, setup, Object.assign({}, o, { never, row: line }));
        const present = ['front', 'back'].map(f => faces[f]).filter(Boolean);
        const jobsOfPiece = ['front', 'back'].map(f => faces[f] && faces[f].job).filter(Boolean);
        const seals = sealsOf(jobsOfPiece);
        const real = present.filter(f => !f.plain);
        // nothing to engrave, nothing decided by a person: no card (an earring: only a decided legacy front)
        if (!real.length && !seals.length) return;
        if (never && !(faces.front && faces.front.job && DECIDED.includes(faces.front.job.state))) return;
        const slotWord = slotLabel(slot), title = [name, slotWord].filter(Boolean).join(' · ') || slotWord || 'Piece';
        const review = present.filter(f => f.job && f.job.state === 'review');
        let approve = null;
        if (review.length) {
          const js = review.map(f => f.job), gone = js.some(j => j.row && j.row.state === 'gone');
          const busy = j => !!(o.isWorking && tryDo(() => o.isWorking(j), false)) || !!(j._approvalTask || j._backTask || j.approvalPreparing || j.stamping || j.backSaving);
          if (gone) approve = { jobs: js, ok: false, why: 'This order has left the pull' };
          else if (js.length === 1) approve = { jobs: js, ok: !busy(js[0]), why: busy(js[0]) ? 'Still being prepared' : '' };
          else { const g = F && F.approveAllGate ? tryDo(() => F.approveAllGate(js, { busy, shown: j => !o.seen || o.seen(j) }), { ok: false, why: '' }) : { ok: true, why: '' }; approve = { jobs: js, ok: !!g.ok, why: g.why || '' }; }
        }
        out.push({ pieceKey: F && F.pieceKeyOf ? F.pieceKeyOf(jobsOfPiece[0] ? jobsOfPiece[0].key : jobKey(line.key, slot, 'back')) : line.key + (slot ? '#' + slot : ''),
          rid, lineKey: line.key, slot: slot || null, row: line, title, name, never, faces, jobs: jobsOfPiece, approve, seals,
          state: worst(present.map(f => f.state)) });
      };
      if (groups.length) for (const g of groups) make(g.slot || null, g.jobs);
      else if (setup && setup.faces && setup.faces.length && (setup.source !== 'default' || (line.engrave && line.engrave.needed))) make(null, []);
    }
    return out;
  }

  /* ── the markup ─────────────────────────────────────────────────────────── */
  const faceTitle = f => (f === 'front' ? 'Front' : 'Back');
  function faceHtml(piece, face, o) {
    const f = piece.faces[face], key = f ? f.key : jobKey(piece.lineKey, piece.slot, face), empty = !f || f.plain || (!f.text && f.state === 'plain');
    const cur = o && o.current === face ? ' aria-current="true"' : '';
    let txt = '';
    if (!empty) {
      if (f.state === 'busy' && !f.text) txt = '<span class="engTxt engCue"><span class="owSpin" aria-hidden="true"></span>Preparing</span>';
      else if (f.state === 'words' && !f.text) txt = '<span class="engTxt engCue">Words to confirm</span>';
      else if (f.text) { const one = f.text.split('\n').join(' / '); txt = `<span class="engTxt" title="${esc(f.text)}"${f.fontId && root && root.CharmNestEngravePreview && root.CharmNestEngravePreview.fontFamilyOf ? ` style="font-family:${esc(tryDo(() => root.CharmNestEngravePreview.fontFamilyOf(f.fontId), ''))}"` : ''}>${esc(one)}</span>`; }
      else txt = '<span class="engTxt"></span>';
    } else txt = '<span class="engTxt"></span>';
    return `<div class="engFace" data-face="${face}" data-key="${esc(key)}" data-empty="${empty ? 1 : 0}"${cur}><button type="button" class="engThumb" data-e="open" aria-label="${faceTitle(face)} engraving: open"></button><span class="engLbl">${faceTitle(face)}</span>${txt}</div>`;
  }
  /** The Approve block: one button when a face waits for approval (disabled, with a title, until every placement is ready), else "Approved" with the seals pressed beside it, else the seals alone. */
  function actHtml(piece, o) {
    const S = root && root.CNEngravingSeals, Seal = root && root.Seal, seals = piece.seals || [];
    const sealRow = seals.length && Seal && S ? `<span class="sealRow egButtonSeal" data-seal-group data-seal-count="${seals.length}" role="group" aria-label="Engraving approval history">${seals.map(s => Seal.html(s, Seal.BASE_SIZE || 50, 'engravingSeal')).join('')}</span>` : '';
    if (piece.approve) {
      const ap = piece.approve;
      return `<span class="egApproveWrap"><button type="button" class="btn sage sm egApproveButton egAsk${ap.ok ? '' : ' egOff'}" data-e="approve"${ap.ok ? '' : ` disabled title="${esc(ap.why || 'Not ready to approve')}"`}>Approve</button>${sealRow}</span>`;
    }
    if (piece.state === 'approved' || (piece.state === 'plain' && seals.length)) {
      const all = piece.state === 'approved';
      return `<span class="egApproveWrap">${all ? '<button type="button" class="btn sage sm egApproveButton" disabled>Approved</button>' : ''}${sealRow}</span>`;
    }
    return seals.length ? `<span class="egApproveWrap">${sealRow}</span>` : '';
  }
  function html(piece, o) {
    o = o || {}; const word = WORD[piece.state] || '';
    const faces = ['front', 'back'].filter(f => f === 'front' || !piece.never);
    const shown = piece.never ? ['front'] : faces;
    const act = o.approve === false ? '' : actHtml(piece, o);
    return `<article class="engCard" data-piece="${esc(piece.pieceKey)}" data-state="${esc(piece.state)}" data-rid="${esc(piece.rid)}" data-key="${esc(piece.lineKey)}"${piece.slot ? ` data-slot="${esc(piece.slot)}"` : ''}>`
      + `<header class="engHd"><b class="engName" title="${esc(piece.title)}">${esc(piece.title)}</b><i class="engDot" data-s="${esc(piece.state)}" role="img" aria-label="${esc(word)}" title="${esc(word)}"></i></header>`
      + `<div class="engFaces" data-n="${shown.length}">${shown.map(f => faceHtml(piece, f, o)).join('')}</div>`
      + (act ? `<div class="engAct">${act}</div>` : '') + '<div class="engFail" role="alert" hidden></div></article>';
  }

  /* ── the picture of a face: ONE place (thumbOf). The live-font component when it is on the page, else the app's own drawing ── */
  const PX = 160;
  function thumbOf(btn, piece, face, o) {
    const f = piece.faces[face]; btn.replaceChildren(); if (!f || f.plain) return;
    const P = root && root.CharmNestEngravePreview;
    if (P && P.render) {
      try { P.render(btn, { row: lineOf(piece.row), text: f.text, fontId: f.fontId, fontWord: f.fontWord, position: f.position, face }); return; } catch (_) { btn.replaceChildren(); }
    }
    const E = root && root.Engrave, job = f.job;
    // the placement as it is cut (the fitted words on the charm), when the job has one
    if (job && job.fit && job.view && E && E.renderBack) { try { const cv = E.renderBack(job, PX, { hatch: false, grid: false }); cv.setAttribute('role', 'img'); cv.setAttribute('aria-label', faceTitle(face)); btn.append(cv); return; } catch (_) {} }
    // the approved file of that face
    const back = job ? (job.backs || []).filter(b => b && faceOf(b) === face).reduce((a, b) => (!a || (+b.approvedAt || 0) >= (+a.approvedAt || 0) ? b : a), null) : null;
    const src = back && ((back.outputs && back.outputs.png && back.outputs.png.url) || back.png || back.preview);
    if (src && root.document) { const im = root.document.createElement('img'); im.alt = faceTitle(face); im.crossOrigin = 'anonymous'; im.src = o && o.imageUrl ? o.imageUrl(src) : src; btn.append(im); return; }
    // the charm's drawing, as the order window's Vector design draws it
    const PM = root && root.PieceMedia;
    if (PM && PM.vectorThumb && root.document) {
      const want = String(piece.pieceKey) + '|' + face; btn._want = want;
      Promise.resolve(tryDo(() => PM.vectorThumb(lineOf(piece.row), { px: PX }), null)).then(u => { if (!u || btn._want !== want || !btn.isConnected) return; const im = root.document.createElement('img'); im.alt = faceTitle(face); im.src = u; btn.replaceChildren(im); }, () => {});
    }
  }
  function fill(card, piece, o) {
    for (const face of ['front', 'back']) { const b = card.querySelector(`.engFace[data-face="${face}"] .engThumb`); if (b) thumbOf(b, piece, face, o); }
    tryDo(() => root.Seal && root.Seal.fitGroups && root.Seal.fitGroups(card));
  }
  const ids = typeof WeakMap === 'function' ? new WeakMap() : null; let idN = 0;
  const idOf = x => { if (!x || typeof x !== 'object' || !ids) return 0; let n = ids.get(x); if (!n) ids.set(x, n = ++idN); return n; };
  function sig(piece, o) {
    return JSON.stringify([piece.pieceKey, piece.title, piece.state, !!piece.never, piece.approve ? [piece.approve.ok, piece.approve.why, piece.approve.jobs.length] : 0, (piece.seals || []).map(s => s.id).join(','), o && o.current || '',
      ['front', 'back'].map(f => { const x = piece.faces[f]; return x ? [x.state, x.text, x.fontWord, x.fontId, x.position && [x.position.x, x.position.y, x.position.size, x.position.angle], idOf(x.job && x.job.fit), idOf(x.job && x.job.view), x.job && +x.job.approvedAt || 0, x.job && (x.job.backs || []).length] : 0; })]);
  }

  /* ── the card's own rules (the Sheet tab's card keeps its own) ── */
  const CSS = '.engCard{container:engc/inline-size;box-sizing:border-box;width:100%;min-width:0;padding:10px;border:1px solid var(--line);border-radius:12px;background:var(--card);display:grid;gap:6px;align-content:start;overflow-wrap:anywhere}'
    + '.engHd{display:flex;align-items:center;gap:6px;min-width:0}.engName{flex:1 1 0;min-width:0;font:600 12.5px/1.3 var(--sans);color:var(--ink);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}'
    + '.engDot{flex:none;width:8px;height:8px;border-radius:50%;background:var(--ink45);opacity:.85}.engDot[data-s=words]{background:#b04a32;opacity:1}.engDot[data-s=approve]{background:#b8892f;opacity:1}.engDot[data-s=approved]{background:#5f7a5b;opacity:1}.engDot[data-s=plain]{background:var(--line);opacity:1}'
    + '.engFaces{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;min-width:0}'
    + '.engFace{display:flex;flex-direction:column;gap:3px;min-width:0}.engLbl{order:-1;font:600 11px/1.2 var(--sans);color:var(--ink)}.engFace[data-empty="1"] .engLbl{opacity:.4}'
    + '.engThumb{appearance:none;display:block;position:relative;box-sizing:border-box;width:100%;min-width:0;aspect-ratio:1;padding:0;margin:0;border:1px solid var(--line);border-radius:9px;background:#fff;overflow:hidden;cursor:pointer;font:inherit;color:inherit}'
    + '.engThumb canvas,.engThumb img,.engThumb svg{width:100%;height:100%;object-fit:contain;display:block}.engThumb:focus-visible{outline:2px solid var(--slate,#68899d);outline-offset:2px}.engThumb[aria-busy=true]{cursor:progress}'
    + '.engFace[data-empty="1"] .engThumb{border-style:dashed;background:transparent}.engFace[aria-current=true] .engThumb{outline:2px solid var(--gold,#a9823f);outline-offset:0}'
    + '.engTxt{display:block;min-height:1.3em;font:12.5px/1.3 var(--serif);color:var(--ink);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.engTxt.engCue{display:flex;align-items:center;gap:5px;font-style:italic;color:var(--ink45);font-size:11.5px}'
    + '.engTxt .owSpin{flex:none;width:10px;height:10px}.engOpening{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;background:rgba(255,255,255,.85)}'
    + '.engAct{display:grid;min-width:0}.engAct:empty{display:none}.engCard .egApproveWrap{display:grid;gap:4px;width:100%;min-width:0;margin:0}'
    + '.engCard .egApproveWrap>.egButtonSeal{position:static;left:auto;top:auto;width:auto;height:auto;max-width:100%;margin:0;padding:0;gap:4px;justify-content:center}'
    + '.engCard .engAct .btn.egApproveButton{min-width:0;width:100%;height:30px;padding:4px 8px;font-size:13px;white-space:nowrap}.engCard .engAct .btn.egApproveButton:disabled{opacity:1;cursor:default}.engCard .engAct .btn.egOff{background:var(--card2);border-color:var(--line);color:var(--ink45)}'
    + '.engFail{font:11.5px/1.35 var(--sans);color:#8a3a26;background:var(--claySoft);border-radius:8px;padding:5px 8px;min-width:0}.engFail[hidden]{display:none}'
    + '@container engc (max-width:150px){.engCard{padding:6px;gap:4px}.engFaces{gap:4px}.engName{font-size:11px}.engLbl{font-size:10px}.engTxt{font-size:11px}.engCard .engAct .btn.egApproveButton{height:26px;font-size:12px}}';
  function css() {
    const d = root && root.document; if (!d || d.getElementById('engCardCss')) return;
    const s = d.createElement('style'); s.id = 'engCardCss'; s.textContent = CSS; (d.head || d.documentElement).appendChild(s);
  }

  /** A small labelled spinner on the thumbnail while it opens (the order window closes as Engraving opens). */
  function opening(btn, run) {
    if (!run || typeof run.then !== 'function') return;
    btn.setAttribute('aria-busy', 'true'); const w = root.document.createElement('span'); w.className = 'engOpening'; w.setAttribute('role', 'status'); w.innerHTML = '<span class="owSpin" aria-hidden="true"></span>'; w.title = 'Opening'; btn.append(w);
    const back = () => { btn.removeAttribute('aria-busy'); w.remove(); };
    run.then(back, back);
  }
  /** The card as an element, drawn and filled; `onOpen(face, key, btn)` is told of a press on a face (its thumbnail, its title or the piece's name). */
  function card(piece, o) {
    css(); const d = root.document, host = d.createElement('div'); host.innerHTML = html(piece, o); const el = host.firstElementChild;
    fill(el, piece, o);
    const open = (face, btn) => { try { const run = o && o.onOpen ? o.onOpen(face, (piece.faces[face] && piece.faces[face].key) || jobKey(piece.lineKey, piece.slot, face), btn, piece) : null; opening(btn, run); } catch (e) { tryDo(() => console.warn('engraving card: open', e)); } };
    el.querySelectorAll('.engFace').forEach(fe => { const face = fe.dataset.face, btn = fe.querySelector('.engThumb'); fe.addEventListener('click', () => open(face, btn)); });
    el.querySelector('.engName')?.addEventListener('click', () => { const f = piece.faces.front && !piece.faces.back ? 'front' : piece.faces.back ? 'back' : 'front'; open(f, el.querySelector(`.engFace[data-face="${f}"] .engThumb`)); });
    return el;
  }

  /** The card in the Engrave tab's detail: the same markup, no Approve; the face shown carries aria-current; a press on a face calls onPick(face). */
  function mountPiece(slot, ctx) {
    css(); ctx = Object.assign({}, ctx); let el = null, last = '', gone = false;
    const find = () => {
      const row = lineOf(ctx.row), F = Faces(), pk = ctx.key && F ? F.pieceKeyOf(ctx.key) : '';
      const ps = piecesOf([row], { nameOf: ctx.nameOf, isWorking: ctx.isWorking }); return ps.find(p => p.pieceKey === pk) || ps.find(p => p.jobs.some(j => j.key === ctx.key)) || ps[0] || null;
    };
    const o = () => ({ approve: false, current: ctx.face || '', onOpen: face => { if (ctx.onPick) tryDo(() => ctx.onPick(face)); return null; } });
    function draw(force) {
      if (gone) return; const p = find();
      if (!p) { last = ''; slot.replaceChildren(); el = null; return; }
      const s = sig(p, o()); if (!force && s === last && el && el.isConnected) return; last = s;
      const n = card(p, o()); if (el && el.isConnected) el.replaceWith(n); else slot.replaceChildren(n); el = n;
    }
    draw(true);
    return { get el() { return el; }, update(patch) { if (patch) Object.assign(ctx, patch); draw(false); }, refresh() { draw(true); }, destroy() { gone = true; slot.replaceChildren(); el = null; },
      thumb: face => (el ? el.querySelector(`.engFace[data-face="${face}"] .engThumb`) : null) };
  }

  return { piecesOf, facesOf, stateOf, html, fill, card, sig, mountPiece, thumbOf, sealsOf, css, WORD };
});
