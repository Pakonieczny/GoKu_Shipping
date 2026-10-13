/* The engraving CARD of ONE PIECE (Paul, 12 Oct 2026: "show the cards for each piece that needs to be engraved ... the cards are currently full of needless text ... front and back
 * side-by-side visible in a larger card so that the user can automatically understand that there's a front engraving and a back engraving as part of the same piece ... keep it very
 * simple and minimalist"). The contract is plans/engrave-ui-1012/CONTRACT.md section C.
 *
 *   CharmNestEngraveCard.piecesOf(rows, o) -> [Piece]      one entry per PIECE of the lines given (a charm, one ear, one disc), each with its Front and its Back
 *       Piece = { pieceKey, rid, lineKey, slot, row, title, never, state, faces: { front: Face|null, back: Face|null }, jobs: [job], approve: { jobs, ok, why }|null, seals: [seal] }
 *       Face  = { face, key, job|null, text, fontWord, fontId, position, state, working, foreign? }
 *       state (of a face and of the card, the worst of its faces): words | busy | approve | approved | plain.   o: { nameOf(row), isWorking(job), seen(job), events }
 *       ENGSMART (Paul, 12 Oct 2026, round 2: "if there is no engraving, it should not show it, if there is only back engraving then show that, only front then show only that, both then both"):
 *       a face the engraving does not need is null and is NOT drawn at all: no dashed empty slot, no "Front" over nothing. The faces are the line's setup (what the system read or a person chose), or, while a
 *       person is changing the mode in the Engrave tab's panel, the STAGED mode (o.stage = { mode, faces: { front: { text, font }, back: {...} } }); a face that has a live placement job (not cut plain) is shown too: it is what will be approved.
 *       A line that engraves nothing has no card (o.keepEmpty: the Engrave detail keeps the piece's header while None is staged). Back is ALWAYS null for an earring (CharmNestEngraveFaces.neverOf): an
 *       earring has no card at all unless a decided front job is already there, and then only its Front.
 *   CharmNestEngraveCard.html(piece, o)      the card's markup (o.approve === false: no Approve / Approved block, the Engrave detail's card; o.current: the face shown, aria-current)
 *   CharmNestEngraveCard.fill(card, piece, o)  puts each face's picture into its thumbnail (CharmNestEngravePreview.render when it exists, else the app's own drawing: one place, thumbOf)
 *   CharmNestEngraveCard.sig(piece, o)       what the card shows, in a line (drawn again only when it changes)
 *   CharmNestEngraveCard.mountPiece(slot, { row, key, face, onPick(face), approve:false, nameOf }) -> { update({ face }), refresh(), destroy(), el }   the card in the Engrave tab's detail
 *       (a press on a face calls onPick(face) and navigates nowhere)
 *   CharmNestEngraveCard.openOf(piece) / openHtml(piece)  the Engraving button (ENGBUTTON: `btn ghost sm egOpen`, "Fix in Engraving →" / "View in Engraving →"): the face it opens, its wording, can it open now
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
  /** The words of a job as they were GIVEN (ENGAUTO): the fit lays them out again (job.lines is the cut's layout: "NAME" / "ONE", capitals for Ace), job.lineInput keeps what went in; a job a person decided, or laid
   *  out by hand (lineMode "preserve"), shows its lines as they are. */
  const wordsOfJob = j => {
    const ls = linesText((j && j.lines && j.lines.length ? j.lines : j && j.text) || '');
    if (j && !DECIDED.includes(j.state) && j.lineMode !== 'preserve' && Array.isArray(j.lineInput) && j.lineInput.length) {
      const li = linesText(j.lineInput), run = v => v.join(' ').replace(/\s+/g, ' ').trim().toLowerCase();
      if (li.length && run(li) === run(ls)) return li.join('\n');
    }
    return ls.join('\n');
  };

  const facesOfMode = m => (m === 'both' ? ['front', 'back'] : m === 'front' ? ['front'] : m === 'back' ? ['back'] : []);
  const stagedOf = (stage, face) => { const x = stage && stage.faces && stage.faces[face]; return x == null ? null : typeof x === 'string' ? { text: x, font: '' } : { text: String(x.text == null ? '' : x.text), font: String(x.font == null ? '' : x.font) }; };
  /** The faces of one piece, ONLY the ones the engraving needs. `jobs`: the piece's jobs (any face); `setup`: the line's setup (the faces it asks for, with the words it has); `o.stage`: a mode a person has
   *  staged and not saved (it replaces the setup: the faces that mode asks for, with the words and font typed so far). A face nobody asks for is null (and is not drawn); the one exception is a face that has a
   *  live placement (a job that is not cut plain): it is what will be engraved or approved, so it is shown. A face a person cut plain is not an engraving: null. */
  function facesOf(lineKey, slot, jobs, setup, o) {
    o = o || {}; const never = !!o.never, P = root && root.CharmNestEngravePreview, Pos = root && root.CharmNestEngravePosition, out = { front: null, back: null };
    const stage = o.stage && typeof o.stage === 'object' ? o.stage : null;
    const want = stage ? facesOfMode(stage.mode) : setup && setup.faces ? setup.faces : null;   // (no setup at all: the jobs say)
    for (const face of ['front', 'back']) {
      if (face === 'back' && never) continue;                                   // (an earring never has a back slot, whatever a job says)
      const job = (jobs || []).find(j => faceOf(j) === face) || null, w = setup && setup[face], st = stagedOf(stage, face);
      const plainJob = !!job && (job.state === 'none' || job.state === 'skipped');
      if (plainJob && !stage) continue;                                         // (cut plain by a person: that face is not engraved)
      // (a staged mode is the person's word and decides alone; otherwise the setup's faces, and any face that has a live placement: it is what will be engraved or approved, whatever the setup says)
      const asks = stage ? want.includes(face) : !!(want && want.includes(face)) || !!job;
      if (!asks) continue;
      const live = plainJob ? null : job;                                       // (a face staged on again after it was cut plain has no placement until Save)
      const text = st ? linesText(st.text).join('\n') : live ? wordsOfJob(live) || (w && w.lines ? w.lines.join('\n') : '') : (w && w.lines ? w.lines.join('\n') : '');
      const fontWord = st ? st.font || (live && live.faceFontWord) || (w && w.fontWord) || null : (live && live.faceFontWord) || (w && w.fontWord) || null;
      const row = live ? live.row : o.row;
      out[face] = { face, key: job ? job.key : jobKey(lineKey, slot, face), job: live, text, fontWord,
        fontId: P && P.fontIdOf ? tryDo(() => P.fontIdOf(lineOf(row) || row, face, fontWord), null) : null,
        position: live && Pos && Pos.of ? tryDo(() => Pos.of(live), null) : null,
        state: live ? stateOf(live, o) : stage ? 'plain' : 'busy', working: !!(live && o.isWorking && tryDo(() => o.isWorking(live), false)), staged: !live && !!stage };
    }
    return out;
  }

  /** The seals of a piece, every face's: the one merge the rest of the page uses (CNEngravingSeals.merge reads a job, its record, its saved backs). */
  function sealsOf(jobs, extra) {
    const S = root && root.CNEngravingSeals; if (!S || !S.merge) return [];
    // (the line's own record, row.engrave, is the piece's only when the piece has one job: with a Front and a Back it is shared and carries no face, so it would read as a second Back seal)
    const recs = [], shared = (jobs || []).length < 2; for (const j of jobs || []) recs.push(j, shared && j && j.row && j.row.engrave, ...((j && j.backs) || []), j && j.editOriginal);
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
        const jobsOfPiece = present.map(f => f.job).filter(Boolean);
        const seals = sealsOf(jobsOfPiece);
        // nothing to engrave: no card (an earring: only a decided legacy front); o.keepEmpty keeps the header of the piece the Engrave tab is editing while None is staged
        if (!present.length && !o.keepEmpty) return;
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
      else if (o.stage || (setup && setup.faces && setup.faces.length && (setup.source !== 'default' || (line.engrave && line.engrave.needed)))) make(null, []);
    }
    return out;
  }

  /* ── the markup ─────────────────────────────────────────────────────────── */
  const faceTitle = f => (f === 'front' ? 'Front' : 'Back');
  /** The face of the card the Engrave detail is editing: the one asked for when the card shows it; the only face when the card shows one (it is selected by itself); none when `o.current` is not given (the order window's card). */
  function currentOf(piece, o) {
    if (!o || o.current === undefined) return '';
    const shown = ['front', 'back'].filter(f => piece.faces[f]);
    return shown.includes(o.current) ? o.current : shown.length === 1 ? shown[0] : '';
  }
  /** One face that the engraving needs: its picture, its title and its words. There is no markup for a face that is not engraved (no empty slot). */
  function faceHtml(piece, face, cur) {
    const f = piece.faces[face], key = f.key;
    let txt;
    if (f.state === 'busy' && !f.text) txt = '<span class="engTxt engCue"><span class="owSpin" aria-hidden="true"></span>Preparing</span>';
    else if (f.state === 'words' && !f.text) txt = '<span class="engTxt engCue">Words to confirm</span>';
    else if (f.text) { const one = f.text.split('\n').join(' / '); txt = `<span class="engTxt" title="${esc(f.text)}"${f.fontId && root && root.CharmNestEngravePreview && root.CharmNestEngravePreview.fontFamilyOf ? ` style="font-family:${esc(tryDo(() => root.CharmNestEngravePreview.fontFamilyOf(f.fontId), ''))}"` : ''}>${esc(one)}</span>`; }
    else txt = '<span class="engTxt"></span>';
    return `<div class="engFace" data-face="${face}" data-key="${esc(key)}"${cur === face ? ' aria-current="true"' : ''}><button type="button" class="engThumb" data-e="open" aria-label="${faceTitle(face)} engraving: open"></button><span class="engLbl">${faceTitle(face)}</span>${txt}</div>`;
  }
  /** The Approve block, in the card's header row (a card of 207px is 170px tall, not 210): one small Approve button while a face waits for approval (disabled, with a title, until every placement is ready;
   *  an empty small seal row waits beside it, the seal is pressed on it); once the piece is approved its seals, small, in the button's place. Nothing else. */
  const SEAL_PX = 24;
  function actHtml(piece, o) {
    const S = root && root.CNEngravingSeals, Seal = root && root.Seal, seals = piece.seals || [];
    const row = (n, inner) => `<span class="sealRow egButtonSeal" data-seal-group data-seal-max="${SEAL_PX}" data-seal-count="${n}" role="group" aria-label="Engraving approval history">${inner}</span>`;
    if (piece.approve) {
      const ap = piece.approve;
      return `<span class="egApproveWrap"><button type="button" class="btn sage sm egApproveButton egAsk${ap.ok ? '' : ' egOff'}" data-e="approve"${ap.ok ? '' : ` disabled title="${esc(ap.why || 'Not ready to approve')}"`}>Approve</button>${row(0, '')}</span>`;
    }
    if (seals.length && Seal && S && (piece.state === 'approved' || piece.state === 'plain')) return `<span class="egApproveWrap">${row(seals.length, seals.map(s => Seal.html(s, SEAL_PX, 'engravingSeal')).join(''))}</span>`;
    return '';
  }
  /* ── the Engraving button (ENGBUTTON, Paul 12 Oct 2026, round 2b: "bring back the engraving button that we had, not just a cheap clickable card") ──
   * The very button the order window's card had before round 1 (charm-nest-engraving-seals.js panel(): `btn ghost sm egOpen`, `btn gold` while the words are unclear, "Fix in Engraving →",
   * "View in Engraving →" once the piece is decided, "Opening Engraving…" with the small spinner while it opens). One per card, in the order window only (the Engrave tab's own card has none: it
   * is already there). It opens the piece through the card's onOpen, i.e. EngraveLink.open({ rid, key, poolId, piece, face }). The two thumbnails stay a second, quieter way in. */
  const OPEN_WORD = { fix: 'Fix in Engraving', view: 'View in Engraving', busy: 'Opening Engraving…' };
  /** What the button does for this piece: the face it opens (the one that needs a person most, else the first shown), its wording, and whether it can open now (else why not). */
  function openOf(piece) {
    const shown = ['front', 'back'].filter(f => piece.faces && piece.faces[f]);
    const face = shown.reduce((a, f) => (!a || (RANK[piece.faces[f].state] || 0) > (RANK[piece.faces[a].state] || 0) ? f : a), '') || 'front';
    const L = root && root.EngraveLink, E = root && root.Engrave, line = lineOf(piece.row);
    const why = line && line.state === 'gone' ? 'This order has left the pull' : root && root.document && !(L && L.open) && !(E && E.restoreView) ? 'Engraving is not ready' : '';   // (the model alone, with no page, has nothing to wait for)
    return { face, done: piece.state === 'approved' || piece.state === 'plain', words: piece.state === 'words', ok: !why, why };
  }
  function openHtml(piece) {
    const g = openOf(piece), word = g.done ? OPEN_WORD.view : OPEN_WORD.fix;
    return `<div class="engOpenRow"><button type="button" class="btn ${g.words ? 'gold' : 'ghost'} sm egOpen" data-e="engrave" data-face="${g.face}"${g.ok ? '' : ` aria-disabled="true" title="${esc(g.why)}"`} aria-label="${esc(word + ': ' + piece.title)}">${word} <span aria-hidden="true">→</span></button></div>`;
  }
  function html(piece, o) {
    o = o || {}; const word = WORD[piece.state] || '';
    const shown = ['front', 'back'].filter(f => piece.faces[f]), cur = currentOf(piece, o);
    const act = o.approve === false ? '' : actHtml(piece, o);
    return `<article class="engCard" data-piece="${esc(piece.pieceKey)}" data-state="${esc(piece.state)}" data-rid="${esc(piece.rid)}" data-key="${esc(piece.lineKey)}"${piece.slot ? ` data-slot="${esc(piece.slot)}"` : ''}>`
      + `<header class="engHd"><i class="engDot" data-s="${esc(piece.state)}" role="img" aria-label="${esc(word)}" title="${esc(word)}"></i><b class="engName" title="${esc(piece.title)}">${esc(piece.title)}</b>${act ? `<span class="engAct">${act}</span>` : ''}</header>`
      + (shown.length ? `<div class="engFaces" data-n="${shown.length}">${shown.map(f => faceHtml(piece, f, cur)).join('')}</div>` : '')
      + (o.approve === false ? '' : openHtml(piece))
      + '<div class="engFail" role="alert" hidden></div></article>';
  }

  /* ── the picture of a face: ONE place (thumbOf). The live-font component when it is on the page, else the app's own drawing ── */
  const PX = 160;
  function thumbOf(btn, piece, face, o) {
    const f = piece.faces[face]; btn.replaceChildren(); if (!f) return;
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
  /** Paints the live previews of a card (or of every card inside `scope`) again, once the card is in the page: the preview scales long words down to the box it finds, and a card is built before it has one. */
  function repaint(scope) {
    const U = root && root.CharmNestEngravePreview; if (!U || !U.update || !scope || !scope.querySelectorAll) return;
    for (const b of scope.querySelectorAll('.engThumb')) if (b._engpv && b.isConnected) tryDo(() => U.update(b, {}));
  }
  const ids = typeof WeakMap === 'function' ? new WeakMap() : null; let idN = 0;
  const idOf = x => { if (!x || typeof x !== 'object' || !ids) return 0; let n = ids.get(x); if (!n) ids.set(x, n = ++idN); return n; };
  function sig(piece, o) {
    return JSON.stringify([piece.pieceKey, piece.title, piece.state, !!piece.never, piece.approve ? [piece.approve.ok, piece.approve.why, piece.approve.jobs.length] : 0, (piece.seals || []).map(s => s.id).join(','), currentOf(piece, o),
      ['front', 'back'].map(f => { const x = piece.faces[f]; return x ? [x.state, !!x.staged, x.text, x.fontWord, x.fontId, x.position && [x.position.x, x.position.y, x.position.size, x.position.angle], idOf(x.job && x.job.fit), idOf(x.job && x.job.view), x.job && +x.job.approvedAt || 0, x.job && (x.job.backs || []).length] : 0; })]);
  }

  /* ── the card's own rules (the Sheet tab's card keeps its own) ── */
  const CSS = '.engCard{container:engc/inline-size;box-sizing:border-box;width:100%;min-width:0;padding:7px;border:1px solid var(--line);border-radius:12px;background:var(--card);display:grid;gap:4px;align-content:start;overflow-wrap:anywhere}'
    + '.engHd{display:flex;align-items:center;gap:6px;min-width:0;min-height:22px}'
    + '.engDot{flex:none;width:8px;height:8px;border-radius:50%;background:var(--ink45);opacity:.85}.engDot[data-s=words]{background:#b04a32;opacity:1}.engDot[data-s=approve]{background:#b8892f;opacity:1}.engDot[data-s=approved]{background:#5f7a5b;opacity:1}.engDot[data-s=plain]{background:var(--line);opacity:1}'
    + '.engName{flex:1 1 0;min-width:0;font:600 12.5px/1.3 var(--sans);color:var(--ink);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}'
    + '.engAct{flex:none;display:flex;align-items:center;min-width:0}.engAct:empty{display:none}.engCard .egApproveWrap{position:relative;display:flex;align-items:center;margin:0;min-width:0;overflow:visible}'
    + '.engCard .egApproveWrap>.egButtonSeal{position:static;left:auto;top:auto;z-index:auto;width:auto;height:auto;margin:0;padding:0;gap:2px;max-width:none;flex:none;justify-content:flex-start}.engCard .egApproveWrap>.egApproveButton~.egButtonSeal{position:absolute;right:-4px;left:auto;top:50%;width:24px;height:24px;margin:-12px 0 0}'
    + '.engCard .engAct .btn.egApproveButton{min-width:0;height:22px;padding:0 10px;font-size:12px;line-height:1;white-space:nowrap}.engCard .engAct .btn.egApproveButton:disabled{opacity:1;cursor:default}.engCard .engAct .btn.egOff{background:var(--card2);border-color:var(--line);color:var(--ink45)}'
    + '.engFaces{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:6px;min-width:0}'
    + '.engFace{display:flex;flex-direction:column;gap:2px;min-width:0}.engLbl{order:-1;font:600 11px/13px var(--sans);color:var(--ink)}'
    + '.engThumb{appearance:none;display:block;position:relative;box-sizing:border-box;width:100%;max-width:104px;min-width:0;aspect-ratio:1;padding:0;margin:0;border:1px solid var(--line);border-radius:9px;background:#fff;overflow:hidden;cursor:pointer;font:inherit;color:inherit}'
    + '.engThumb canvas,.engThumb img,.engThumb svg{width:100%;height:100%;object-fit:contain;display:block}.engThumb:focus-visible{outline:2px solid var(--slate,#68899d);outline-offset:2px}.engThumb[aria-busy=true]{cursor:progress}'
    + '.engFace[aria-current=true] .engThumb{outline:2px solid var(--gold,#a9823f);outline-offset:0}'
    + '.engFaces[data-n="1"]{grid-template-columns:minmax(0,1fr)}.engFaces[data-n="1"] .engFace{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));grid-template-rows:auto auto 1fr;column-gap:6px;row-gap:2px;align-items:start}.engFaces[data-n="1"] .engThumb{grid-column:1;grid-row:1/4}.engFaces[data-n="1"] .engLbl{grid-column:2;grid-row:1;order:0}.engFaces[data-n="1"] .engTxt{grid-column:2;grid-row:2;display:-webkit-box;-webkit-box-orient:vertical;-webkit-line-clamp:4;white-space:normal;font-size:13px;line-height:16px;min-height:0}.engFaces[data-n="1"] .engTxt.engCue{display:flex;white-space:normal}'
    + '.engTxt{display:block;min-height:14px;font:12px/14px var(--serif);color:var(--ink);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.engTxt.engCue{display:flex;align-items:center;gap:5px;font-style:italic;color:var(--ink45);font-size:11px}'
    + '.engTxt .owSpin{flex:none;width:10px;height:10px}.engOpening{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;background:rgba(255,255,255,.85)}'
    + '.engFail{font:11.5px/1.35 var(--sans);color:#8a3a26;background:var(--claySoft);border-radius:8px;padding:5px 8px;min-width:0}.engFail[hidden]{display:none}'
    + '@container engc (max-width:150px){.engCard{padding:5px;gap:3px}.engHd{flex-wrap:wrap;row-gap:3px}.engName{font-size:11px}.engAct{flex:1 1 100%}.engCard .egApproveWrap{width:100%}.engCard .engAct .btn.egApproveButton{width:100%}.engFaces{gap:4px}.engLbl{font-size:10px}.engTxt{font-size:11px}.engFaces[data-n="1"] .engTxt{font-size:11px;line-height:13px}}';
  /* The states of the Engraving button and of the thumbnails (ENGBUTTON). The button is the app's own `.btn.ghost.sm` (gold while the words are unclear). Its states are written ONCE, for the class
     `.egOpen` wherever it stands (this card, the order window's Sheet view, the sheet window, Review: charm-nest-engraving-seals.js draws the same button), so it looks and answers the same everywhere:
     default / hover (a warmer fill, a darker edge, the arrow steps right) / focus-visible (the app's gold ring) / pressed (one pixel down, darker fill) / disabled (aria-disabled: faded, not-allowed,
     the reason is its title: the app's convention) / loading (aria-busy: the labelled spinner, full strength, progress cursor). The card only sets the button's size (the Approve button's 22px).
     A thumbnail lifts a pixel on hover, presses down, and has its own focus ring; a thumbnail that cannot open is not-allowed. */
  const NOPE = ':not([aria-disabled=true]):not([aria-busy=true]):not(:disabled)';
  const OPEN_CSS = '.egOpen{transition:background-color .13s ease,border-color .13s ease,opacity .15s ease,transform .08s ease,filter .13s ease}.egOpen span[aria-hidden]{display:inline-block;transition:transform .13s ease}'
    + `.egOpen${NOPE}:hover{opacity:1;background:var(--paper2);border-color:var(--ink25)}.egOpen.gold${NOPE}:hover{background:var(--gold2);border-color:var(--gold2)}.egOpen${NOPE}:hover span[aria-hidden]{transform:translateX(3px)}`
    + '.egOpen:focus-visible{outline:2px solid var(--gold);outline-offset:1px}.egOpen.gold:focus-visible{outline-color:var(--ink)}'
    + `.egOpen${NOPE}:active{transform:translateY(1px);background:var(--line);border-color:var(--ink45)}.egOpen.gold${NOPE}:active{background:var(--gold);border-color:var(--gold);filter:brightness(.9)}`
    + '.egOpen[aria-disabled=true]{opacity:.45;cursor:not-allowed;transform:none}'
    + '.egOpen[aria-busy=true]{opacity:1;cursor:progress;transform:none}'
    + '@media (prefers-reduced-motion:reduce){.egOpen,.egOpen span[aria-hidden]{transition:none}.egOpen:hover span[aria-hidden]{transform:none}.egOpen:active{transform:none}}';
  const BTN_CSS = '.engOpenRow{display:flex;min-width:0}'
    + '.engCard .egOpen{flex:1 1 auto;min-width:0;display:inline-flex;align-items:center;justify-content:center;gap:6px;height:22px;padding:0 10px;font-size:12px;line-height:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}'
    + '.engThumb{transition:transform .13s ease,border-color .13s ease,box-shadow .17s ease}'
    + `.engThumb${NOPE}:hover{border-color:var(--ink45);box-shadow:0 4px 12px rgba(20,16,10,.14);transform:translateY(-1px)}.engThumb${NOPE}:active{border-color:var(--ink);box-shadow:none;transform:translateY(1px)}`
    + '.engThumb[aria-disabled=true]{cursor:not-allowed;opacity:.7}'
    // (the room for the button row: the card keeps its height at about 175px at 207px wide, so the three-card row still fits a 1280x720 window without scrolling: a gap less, a pixel less above and below, and the two pictures 80px)
    + '.engCard{gap:3px;padding:6px 7px}@container engc (max-width:215px){.engFaces{grid-template-columns:repeat(2,minmax(0,80px));justify-content:space-between}.engThumb{max-width:80px}}'
    + '@container engc (max-width:150px){.engCard .egOpen{height:auto;min-height:22px;padding:3px 6px;font-size:11px;line-height:1.15;white-space:normal;text-align:center}}'
    + '@media (prefers-reduced-motion:reduce){.engThumb{transition:none}.engThumb:hover,.engThumb:active{transform:none}}';
  /** The button's states, for every place that draws the button (once per page, at load; the card's own rules go in with the first card). */
  function openCss() {
    const d = root && root.document; if (!d || !d.head || d.getElementById('engOpenCss')) return;
    const s = d.createElement('style'); s.id = 'engOpenCss'; s.textContent = OPEN_CSS; d.head.appendChild(s);
  }
  function css() {
    const d = root && root.document; if (!d || d.getElementById('engCardCss')) return;
    openCss(); const s = d.createElement('style'); s.id = 'engCardCss'; s.textContent = CSS + BTN_CSS; (d.head || d.documentElement).appendChild(s);
  }

  /** The wait of an opening, said on the card: the Engraving button turns into "Opening Engraving…" with the small spinner (it stays where it is, focus kept, and presses are ignored), and the
   *  thumbnail that was pressed shows the same spinner. `on` false puts everything back. */
  function waiting(el, from, on) {
    const btn = el.querySelector('.egOpen'), th = from && from.classList && from.classList.contains('engThumb') ? from : null;
    if (on) {
      el.dataset.opening = '1';
      if (btn) { btn._was = btn.innerHTML; btn.setAttribute('aria-busy', 'true'); btn.innerHTML = '<span class="spin" aria-hidden="true"></span>' + OPEN_WORD.busy; }
      if (th) { th.setAttribute('aria-busy', 'true'); const w = root.document.createElement('span'); w.className = 'engOpening'; w.setAttribute('role', 'status'); w.setAttribute('aria-label', OPEN_WORD.busy); w.title = OPEN_WORD.busy; w.innerHTML = '<span class="owSpin" aria-hidden="true"></span>'; th.append(w); }
    } else {
      delete el.dataset.opening;
      if (btn && btn.isConnected && btn._was != null) { btn.removeAttribute('aria-busy'); btn.innerHTML = btn._was; btn._was = null; }
      for (const t of el.querySelectorAll('.engThumb[aria-busy]')) { t.removeAttribute('aria-busy'); t.querySelector('.engOpening')?.remove(); }
    }
  }
  /** The ways into Engraving from a card: the Engraving button (the card's one button) and, quieter, the two thumbnails (each a real button: Enter and Space press it). All go through
   *  `o.onOpen(face, key, button, piece)`, which returns the promise of the opening (EngraveLink.open); while it runs the card says so (waiting) and a second press does nothing. A card that has no
   *  Engraving button (the Engrave tab's own: o.approve === false) is only a face picker: a press calls onOpen and nothing waits. A piece that cannot be opened (openOf: not ready, order gone)
   *  keeps its button, greyed and not pressable, with the reason as its title (the app's aria-disabled button). */
  function wireOpen(el, piece, o) {
    const btn = el.querySelector('.egOpen'), g = btn ? openOf(piece) : { ok: true, why: '' };
    let busy = false;
    const keyOf = face => (piece.faces[face] && piece.faces[face].key) || jobKey(piece.lineKey, piece.slot, face);
    const press = (face, from) => {
      if (busy || !g.ok) return;
      let run = null;
      try { run = o && o.onOpen ? o.onOpen(face, keyOf(face), from, piece) : null; } catch (e) { tryDo(() => console.warn('engraving card: open', e)); return; }
      if (!run || typeof run.then !== 'function') return;
      busy = true; waiting(el, from, true);
      const back = () => { busy = false; waiting(el, from, false); };
      run.then(back, back);
    };
    for (const fe of el.querySelectorAll('.engFace')) {
      const face = fe.dataset.face, th = fe.querySelector('.engThumb'); if (!th) continue;
      th.setAttribute('aria-label', btn ? `${faceTitle(face)} engraving of ${piece.title}: open in Engraving` : `${faceTitle(face)} engraving of ${piece.title}`);
      if (!g.ok) { th.setAttribute('aria-disabled', 'true'); th.title = g.why; }
      th.addEventListener('click', () => press(face, th));
    }
    if (btn) btn.addEventListener('click', () => press(btn.dataset.face || openOf(piece).face, btn));
  }
  /** The card as an element, drawn and filled; `onOpen(face, key, btn, piece)` is told of a press on the Engraving button or on a face's thumbnail. */
  function card(piece, o) {
    css(); const d = root.document, host = d.createElement('div'); host.innerHTML = html(piece, o); const el = host.firstElementChild;
    fill(el, piece, o);
    wireOpen(el, piece, o);
    return el;
  }

  /** The card in the Engrave tab's detail: the same markup, no Approve; the face shown carries aria-current (a card of one face selects it by itself); a press on a face calls onPick(face).
   *  `stage` (what the panel has staged and not saved: { mode, faces: { front: { text, font }, back } } or null) decides which faces the card draws, so the faces follow the mode control at once; None
   *  keeps the piece's header and draws no face (keepEmpty). `update({ face, stage })`; a new `stage` redraws at once. `label(face, text)` puts the words being typed under that face's picture. */
  function mountPiece(slot, ctx) {
    css(); ctx = Object.assign({}, ctx); let el = null, last = '', gone = false;
    const find = () => {
      const row = lineOf(ctx.row), F = Faces(), pk = ctx.key && F ? F.pieceKeyOf(ctx.key) : '';
      const ps = piecesOf([row], { nameOf: ctx.nameOf, isWorking: ctx.isWorking, stage: ctx.stage || null, keepEmpty: true }); return ps.find(p => p.pieceKey === pk) || ps.find(p => p.jobs.some(j => j.key === ctx.key)) || ps[0] || null;
    };
    const o = () => ({ approve: false, current: ctx.face || '', onOpen: face => { if (ctx.onPick) tryDo(() => ctx.onPick(face)); return null; } });
    function draw(force) {
      if (gone) return; const p = find();
      if (!p) { last = ''; slot.replaceChildren(); el = null; return; }
      const s = sig(p, o()); if (!force && s === last && el && el.isConnected) return; last = s;
      const n = card(p, o()); if (el && el.isConnected) el.replaceWith(n); else slot.replaceChildren(n); el = n; repaint(n);
    }
    draw(true);
    return { get el() { return el; }, update(patch) { const staged = !!patch && Object.prototype.hasOwnProperty.call(patch, 'stage'); if (patch) Object.assign(ctx, patch); draw(staged); }, refresh() { draw(true); }, destroy() { gone = true; slot.replaceChildren(); el = null; },
      thumb: face => (el ? el.querySelector(`.engFace[data-face="${face}"] .engThumb`) : null),
      faces: () => (el ? [...el.querySelectorAll('.engFace')].map(f => f.dataset.face) : []),
      label(face, text) { const t = el && el.querySelector(`.engFace[data-face="${face}"] .engTxt`); if (!t) return; const w = linesText(text); t.className = 'engTxt'; t.textContent = w.join(' / '); if (w.length) t.title = w.join('\n'); else t.removeAttribute('title'); } };
  }

  openCss();
  return { piecesOf, facesOf, stateOf, html, fill, card, sig, mountPiece, thumbOf, repaint, sealsOf, css, WORD, openOf, openHtml, OPEN_WORD };
});
