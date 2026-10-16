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
  const Sides = () => dep('CharmNestEngraveSides', './charm-nest-engrave-sides.js');
  const tryDo = (f, d) => { try { return f(); } catch (_) { return d; } };
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const DECIDED = ['approved', 'written', 'skipped', 'none'];
  const WORD = { words: 'Words to confirm', busy: 'Preparing', approve: 'To approve', approved: 'Approved', plain: 'No engraving' };
  const RANK = { words: 5, busy: 4, approve: 3, approved: 2, plain: 1 };
  const faceOf = x => { const F = Faces(); return F && F.faceOf ? F.faceOf(x) : x && (x.face === 'front' || /~front$/.test(String(x.key || ''))) ? 'front' : 'back'; };
  const jobKey = (lineKey, slot, face) => { const F = Faces(); return F && F.jobKey ? F.jobKey(lineKey, slot, face) : String(lineKey) + (slot ? '#' + slot : '') + (face === 'front' ? '~front' : ''); };
  const slotLabel = s => { const F = Faces(); return F && F.slotLabel ? F.slotLabel(s) : s === 'L' ? 'Left' : s === 'R' ? 'Right' : /^D\d+$/.test(s || '') ? 'Disc ' + s.slice(1) : ''; };
  /** PIECES (Paul, 16 Oct 2026: "a card for each piece, titled Piece 1 of 2"): the card of one copy of a line of quantity N is told by its number and the line's count, the small tag the discs have ("Disc 2 of 3"); every other slot keeps its label. */
  const slotInfo = s => { const S = Sides(); return S && S.slotInfo ? tryDo(() => S.slotInfo(s), null) : null; };
  const slotTitle = (s, copies, shp) => { const i = slotInfo(s), S = Sides(); return i && i.kind === 'copy' ? 'Piece ' + i.n + (copies > 1 ? ' of ' + copies : '') : i && S && S.labelOf ? S.labelOf(s, shp) : slotLabel(s); };   // (a pair or a set bought twice: "Left · pair 2", "Disc 1 · set 2", the model's own words)
  const lineOf = row => (row && row.parentRow) || row || null;
  const linesText = v => (Array.isArray(v) ? v : String(v == null ? '' : v).split(/\r?\n/)).map(x => String(x).replace(/\s+/g, ' ').trim()).filter(Boolean);
  /* ENGPICK (Paul, 13 Oct 2026: "I chose the gold and the silver, but the engraving card did not adapt"): two pieces of one order are often ONE design in two metals, so their cards read the same (the same name, the same
   * words, the same picture). A card of an order with more than one line names its metal exactly as the order window's "Its pieces" list does: the metal's dot and "SS" / "GF" after the name. */
  const METAL_CODE = { gold: 'GF', silver: 'SS', rose: 'RG', gold10k: '10K', gold14k: '14K' }, METAL_COLOR = { gold: '#c8a24e', silver: '#8d95a0', rose: '#c08578', gold10k: '#b08d2a', gold14k: '#d9b545' };
  /** The metal of a line: { key, code, color } as the page's own METALS table says it (the model alone: the same table here), or null when the line has none. */
  function metalOf(row) {
    const L = lineOf(row), key = String((L && (L.material || (L.spec && L.spec.material))) || ''); if (!key) return null;
    const O = root && root.CharmNestOrders, code = (O && O.CARD_TAG && O.CARD_TAG[key]) || METAL_CODE[key] || ''; if (!code) return null;
    let color = METAL_COLOR[key] || ''; try { if (typeof METALS !== 'undefined') color = (METALS.find(m => m.key === key) || {}).color || color; } catch (_) {}
    return { key, code, color };
  }
  /** How many live lines the order of this line has (the lines given count too: a model with no page). */
  function lineCountOf(line, given) {
    const rid = String((line && line.order && line.order.receiptId) || ''), seen = new Set();
    for (const r of tryDo(() => (root && root.Orders && root.Orders.rows ? root.Orders.rows() : []), []) || []) if (r && r.state !== 'gone' && String((r.order && r.order.receiptId) || '') === rid) seen.add(lineOf(r).key);
    for (const r of given || []) { const l = lineOf(r); if (l && l.state !== 'gone' && String((l.order && l.order.receiptId) || '') === rid) seen.add(l.key); }
    return seen.size;
  }

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

  /** TRUE RELATIVE SIZE (SETBAR, Paul, 13 Oct 2026: "see both bar charms side-by-side"): the pieces of ONE line that is a set of different designs are drawn at one scale. `rel` (0..1) is a piece's long side over the longest
   *  of the set's (the longest fills its picture box, a smaller one is that fraction of it, same shape). Geometry only (the preview's own frameFor); nothing for a piece with no geometry yet, nor for any other line. */
  function relOfSet(list) {
    const P = root && root.CharmNestEngravePreview, set = (list || []).filter(p => p && p.design);
    if (!P || typeof P.frameFor !== 'function' || set.length < 2) return;
    const dims = set.map(p => tryDo(() => { const f = (p.faces.back && p.faces.back.job) || (p.faces.front && p.faces.front.job) || null, F = P.frameFor({ job: f, row: p.row, slot: p.slot, face: 'back' }), b = F && F.box; return b && b.length === 4 && b.every(Number.isFinite) ? [b[2] - b[0], b[3] - b[1]] : null; }, null));
    const long = dims.map(d => (d ? Math.max(d[0], d[1]) : 0)), top = Math.max(...long); if (!(top > 0)) return;
    set.forEach((p, i) => { if (long[i] > 0) { p.rel = Math.min(1, long[i] / top); p.longPt = long[i]; p.dimsPt = dims[i]; } });
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
      const many = lineCountOf(line, list) > 1, metal = metalOf(line);   // (ENGPICK: an order of several lines names each card's metal)
      const jobs = (E && E.jobsOf ? tryDo(() => E.jobsOf(line), []) : []).filter(j => j && !j.editingBack);
      const setup = tryDo(() => (S && S.setupFor ? S.setupFor(line) : F && F.setupOf ? F.setupOf(line) : null), null);
      const groups = jobs.length && F && F.groupPieces ? F.groupPieces(jobs) : [];
      const copyTotal = groups.filter(g => { const i = slotInfo(g.slot); return i && i.kind === 'copy'; }).length;   // (PIECES: the copies of this line, for "Piece 1 of 2")
      const shp = (() => { const S = Sides(); return S && S.shape ? tryDo(() => S.shape(groups.map(g => g.slot)), null) : null; })();   // (the line's units: with more than one pair or set each piece says its own)
      const make = (slot, pjobs) => {
        const faces = facesOf(line.key, slot, pjobs, setup, Object.assign({}, o, { never, row: line }));
        const present = ['front', 'back'].map(f => faces[f]).filter(Boolean);
        const jobsOfPiece = present.map(f => f.job).filter(Boolean);
        const seals = sealsOf(jobsOfPiece);
        // nothing to engrave: no card (an earring: only a decided legacy front); o.keepEmpty keeps the header of the piece the Engrave tab is editing while None is staged
        if (!present.length && !o.keepEmpty) return;
        if (never && !(faces.front && faces.front.job && DECIDED.includes(faces.front.job.state))) return;
        // SETBAR: a piece of a SET of different designs is its own design ("SKINNY 0191 · 16″"), never the line's joined name with "Disc 2" (charm-nest-engrave-sides.js setPieceOf; every other piece: as before)
        const S2 = Sides(), sps = S2 && S2.setPieceOf ? tryDo(() => S2.setPieceOf(line, slot), null) : null, lenWord = sps ? S2.lengthText(sps) : '', pname = sps ? sps.name : name;
        const slotWord = sps ? (shp && shp.multi && slotInfo(slot) ? 'set ' + slotInfo(slot).unit : '') : slotTitle(slot, copyTotal, shp), title = [pname, many && metal ? metal.code : '', lenWord, slotWord].filter(Boolean).join(' · ') || slotWord || 'Piece';
        const review = present.filter(f => f.job && f.job.state === 'review');
        let approve = null;
        if (review.length) {
          const js = review.map(f => f.job), gone = js.some(j => j.row && j.row.state === 'gone');
          const busy = j => !!(o.isWorking && tryDo(() => o.isWorking(j), false)) || !!(j._approvalTask || j._backTask || j.approvalPreparing || j.stamping || j.backSaving);
          if (gone) approve = { jobs: js, ok: false, why: 'This order has left the pull' };
          else if (js.length === 1) approve = { jobs: js, ok: !busy(js[0]), why: busy(js[0]) ? 'Still being prepared' : '' };
          else { const g = F && F.approveAllGate ? tryDo(() => F.approveAllGate(js, { busy, shown: j => !o.seen || o.seen(j) }), { ok: false, why: '' }) : { ok: true, why: '' }; approve = { jobs: js, ok: !!g.ok, why: g.why || '' }; }
        }
        // PIECES: the copies of a line bought several times (and the second pair or set) carry their number as a small tag under the name, the way the discs' tags are told: "Piece 1 of 2", "Disc 1 · set 2" (the title keeps it whole)
        const si = slotInfo(slot), tagWord = !sps && si && (si.kind === 'copy' || (shp && shp.multi)) ? slotWord : (sps && shp && shp.multi && si ? slotWord : '');
        out.push({ tag: tagWord, pieceKey: F && F.pieceKeyOf ? F.pieceKeyOf(jobsOfPiece[0] ? jobsOfPiece[0].key : jobKey(line.key, slot, 'back')) : line.key + (slot ? '#' + slot : ''),
          rid, lineKey: line.key, slot: slot || null, row: line, title, name: pname, lineName: name, design: sps || null, lengthText: lenWord, never, faces, jobs: jobsOfPiece, approve, seals,
          metal: metal ? metal.code : '', metalKey: metal ? metal.key : '', metalColor: metal ? metal.color : '', many,   // (ENGPICK: the code "SS" / "GF", its key and colour; `many`: the order has more than one line, so the title says the metal)
          state: worst(present.map(f => f.state)) });
      };
      const was = out.length;
      if (groups.length) for (const g of groups) make(g.slot || null, g.jobs);
      else if (o.stage || (setup && setup.faces && setup.faces.length && (setup.source !== 'default' || (line.engrave && line.engrave.needed)))) {
        // (PIECES: a line of several pieces with no job yet is one card for each piece, as it will be once its jobs are made; a line of one piece: the one card it always had)
        const S2 = Sides(), plist = S2 && S2.pieceList ? tryDo(() => S2.pieceList({}, line), []) : [], pslots = [...new Set((plist || []).map(p => p.slot))];
        if (pslots.length > 1 && pslots.every(Boolean)) for (const sl of pslots) make(sl, []); else make(null, []);
      }
      relOfSet(out.slice(was));
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
    // (ENGPRE, Paul 13 Oct: "a charm does not have to be placed on that sheet for it to be approved/updated": a piece on hold, waiting, pulled or on no sheet yet is opened like any other, its job is made at once, Engrave.early; only the reasons below keep the button off)
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
    // (ENGPICK: a card of an order of several lines is that piece's: the dot is its METAL's, as on the "Its pieces" list, and the metal's code stays after the name when the name gives way; the state is said by the button, the seals and the words below)
    const mt = piece.many && piece.metal && piece.metalColor ? piece : null;
    const dot = `<i class="engDot" data-s="${esc(piece.state)}"${mt ? ` data-metal="${esc(piece.metalKey)}" style="--mc:${esc(piece.metalColor)}"` : ''} role="img" aria-label="${esc(word)}" title="${esc(word)}"></i>`;
    const head = mt && piece.name && String(piece.title).startsWith(piece.name) ? piece.name : '', tail = head ? String(piece.title).slice(head.length) : '';
    // (PIECES: a piece with a number of its own, "Piece 1 of 2": its name and metal on the first line, the number as a small tag under it, never cut off by the Approve button)
    const tagged = piece.tag && String(piece.title).endsWith(piece.tag) ? String(piece.title).slice(0, piece.title.length - piece.tag.length).replace(/\s*·\s*$/, '') : null;
    const nm = tagged != null ? `<span class="engNm">${esc(tagged)}</span><span class="engPc">${esc(piece.tag)}</span>` : head && tail ? `<span class="engNm">${esc(head)}</span><span class="engTail">${esc(tail)}</span>` : esc(piece.title);
    return `<article class="engCard" data-piece="${esc(piece.pieceKey)}" data-state="${esc(piece.state)}" data-rid="${esc(piece.rid)}" data-key="${esc(piece.lineKey)}"${mt ? ` data-metal="${esc(piece.metalKey)}"` : ''}${piece.slot ? ` data-slot="${esc(piece.slot)}"` : ''}>`
      + `<header class="engHd">${dot}<b class="engName${tagged != null ? ' engTagged' : ''}" title="${esc(piece.title)}">${nm}</b>${act ? `<span class="engAct">${act}</span>` : ''}</header>`
      + (shown.length ? `<div class="engFaces" data-n="${shown.length}">${shown.map(f => faceHtml(piece, f, cur)).join('')}</div>` : '')
      + (o.approve === false ? '' : openHtml(piece))
      + '<div class="engFail" role="alert" hidden></div></article>';
  }

  /* ── the picture of a face: ONE place (thumbOf). The live-font component when it is on the page, else the app's own drawing ── */
  const PX = 416;   // (ENGBIG: the fallback drawings below, used only when the live preview is not on the page, are made for the order window's largest picture, 208px, at twice the pixel density; the preview itself draws its own)
  /** The saved picture (PNG) of a face's approved file, when it has one: the picture of last resort when the page holds no geometry to draw the charm from. */
  function savedPicture(job, face, o) {
    const back = job ? (job.backs || []).filter(b => b && faceOf(b) === face).reduce((a, b) => (!a || (+b.approvedAt || 0) >= (+a.approvedAt || 0) ? b : a), null) : null;
    const src = back && ((back.outputs && back.outputs.png && back.outputs.png.url) || back.png || back.preview);
    return src ? (o && o.imageUrl ? o.imageUrl(src) : src) : '';
  }
  function thumbOf(btn, piece, face, o) {
    const f = piece.faces[face]; btn.replaceChildren(); if (!f) return;
    const P = root && root.CharmNestEngravePreview;
    if (P && P.render) {
      // ENGTHUMB (round 2): the picture is the CHARM seen from this side (the job's own view, else the piece's pool charm; the front with the design's engraved art), the words laid on it where the job
      // puts them (the preview's own positionOf: the work area's frame). A face with no geometry at all keeps the saved picture of its approved file, else the line's Vector design (the preview's own fallbacks).
      try { P.render(btn, { row: lineOf(piece.row), job: f.job, slot: piece.slot, text: f.text, fontId: f.fontId, fontWord: f.fontWord, position: P.positionOf ? P.positionOf(f.job, { row: lineOf(piece.row), slot: piece.slot }) : f.position, face, image: savedPicture(f.job, face, o), rel: piece.rel }); return; } catch (_) { btn.replaceChildren(); }
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
    for (const b of scope.querySelectorAll('.engThumb,.engSetPic')) if (b._engpv && b.isConnected) tryDo(() => U.update(b, {}));
  }
  const ids = typeof WeakMap === 'function' ? new WeakMap() : null; let idN = 0;
  const idOf = x => { if (!x || typeof x !== 'object' || !ids) return 0; let n = ids.get(x); if (!n) ids.set(x, n = ++idN); return n; };
  function sig(piece, o) {
    return JSON.stringify([piece.pieceKey, piece.title, piece.rel || 1, piece.metalKey || '', !!piece.many, piece.state, !!piece.never, piece.approve ? [piece.approve.ok, piece.approve.why, piece.approve.jobs.length] : 0, (piece.seals || []).map(s => s.id).join(','), currentOf(piece, o),
      ['front', 'back'].map(f => { const x = piece.faces[f]; return x ? [x.state, !!x.staged, x.text, x.fontWord, x.fontId, x.position && [x.position.x, x.position.y, x.position.size, x.position.angle], idOf(x.job && x.job.fit), idOf(x.job && x.job.view), x.job && +x.job.approvedAt || 0, x.job && (x.job.backs || []).length] : 0; })]);
  }

  /* ── the SET STRIP (SETBAR, Paul, 13 Oct 2026: "the user can see both bar charms side-by-side, and then be able to manipulate each one"; "the thinner bar always sits on the shorter chain") ──
   * ONE line of the Engrave detail's work area for a line that is a SET of different designs (the large bar and the small bar): both bars drawn side by side at ONE scale (each cell is as wide as the piece is long, so the
   * thinner bar looks thinner), each with its name and the chain it hangs on ("16″", from the set's rule, never worked out here), the one being edited ringed. A press on a bar is the very press of the piece switch above
   * (data-a="ear", data-ear=<job key>: the card's own handler shows that piece, same face). Nothing is stored or asked; no button approves anything. When the buyer asked for another layout a calm line says a person decides. */
  const LAYOUT_NOTE = 'The buyer asked for a different layout, so a person decides which bar hangs on which chain.';
  /** The one calm line of a set whose layout the buyer asked for (the flag is SETBAR's rule reading, charm-nest-engrave-sides.js setPieces), else ''. */
  const layoutNoteOf = row => { const S = Sides(), sp = S && S.setPieces ? tryDo(() => S.setPieces(lineOf(row)), null) : null; return sp && sp.layoutRequested ? LAYOUT_NOTE : ''; };
  const mmText = pt => (Math.round(pt * 25.4 / 72 * 10) / 10).toString();
  /** The strip's element for `row` (a line) with `key` the job shown, or null when the line is no set of two or more different designs (then nothing is drawn). `face`: the face shown (a piece's job of that face is the one a press opens). */
  function setStrip(row, o) {
    o = o || {}; const d = root && root.document; if (!d) return null;
    const line = lineOf(row), S = Sides(), set = line && S && S.setPieces ? tryDo(() => S.setPieces(line), null) : null; if (!set || set.pieces.length < 2) return null;
    const ps = tryDo(() => piecesOf([line], { nameOf: o.nameOf, isWorking: o.isWorking, keepEmpty: true }), []).filter(p => p.design && p.slot);
    if (ps.length < 2) return null;
    const F = Faces(), cur = o.key && F ? F.pieceKeyOf(o.key) : '', face = o.face === 'front' ? 'front' : 'back', P = root && root.CharmNestEngravePreview;
    const total = ps.reduce((a, p) => a + (p.longPt || 1), 0) || ps.length;
    const el = d.createElement('div'); el.className = 'engSet'; el.setAttribute('role', 'group'); el.setAttribute('aria-label', 'The bars of this set, side by side at their true relative size');
    for (const p of ps) {
      const on = p.pieceKey === cur, job = (p.faces[face] && p.faces[face].job) || (p.faces[face === 'front' ? 'back' : 'front'] && p.faces[face === 'front' ? 'back' : 'front'].job) || p.jobs[0] || null;
      const nm = S.pieceName(p.design), len = S.lengthText(p.design), dim = p.dimsPt ? ` · ${mmText(p.dimsPt[0])} × ${mmText(p.dimsPt[1])} mm` : '';
      const b = d.createElement('button'); b.type = 'button'; b.className = 'engSetPc'; b.dataset.piece = p.pieceKey; b.dataset.slot = p.slot; if (job) { b.dataset.a = 'ear'; b.dataset.ear = job.key; } else b.disabled = true;
      b.setAttribute('aria-pressed', on ? 'true' : 'false'); if (on) b.setAttribute('aria-current', 'true');
      b.style.flex = `${(p.longPt || 1) / total} 1 0%`;
      b.title = `${on ? 'Shown now' : 'Show'}: ${nm} (${p.design.name})${dim}${len ? ` · hangs on the ${len} chain` : ''}`;
      const pic = d.createElement('span'); pic.className = 'engSetPic'; pic.setAttribute('aria-hidden', 'true');
      if (p.dimsPt) { const w = p.dimsPt[0], h = p.dimsPt[1], pad = Math.max(w, h) * 0.04; pic.style.aspectRatio = ((w + 2 * pad) / (h + 2 * pad)).toFixed(4); }
      const lb = d.createElement('span'); lb.className = 'engSetLbl'; lb.textContent = nm;
      b.append(pic, lb); if (len) { const ln = d.createElement('span'); ln.className = 'engSetLen'; ln.textContent = len; ln.setAttribute('aria-label', `chain ${len}`); b.append(ln); }
      el.appendChild(b);
      if (P && P.render && job) tryDo(() => P.render(pic, { row: line, job, slot: p.slot, text: '', face: faceOf(job), position: null }));
    }
    const note = layoutNoteOf(line), wrap = d.createElement('div'); wrap.className = 'engSetWrap'; wrap.append(el);
    if (note) { const n = d.createElement('div'); n.className = 'engSetNote'; n.setAttribute('role', 'status'); n.textContent = note; wrap.append(n); }
    css(); return wrap;
  }

  /* ── the card's own rules (the Sheet tab's card keeps its own) ── */
  const SET_CSS = '.engSetWrap{display:grid;gap:5px;min-width:0;max-width:340px}.pvMain.egWide>.engSetWrap{max-width:calc(100% - 180px)}.engSet{display:flex;align-items:flex-start;gap:14px;min-width:0;padding:3px}'
    + '.engSetPc{appearance:none;flex:1 1 0;min-width:0;display:grid;justify-items:center;gap:2px;margin:0;padding:0;border:0;background:transparent;font:inherit;color:inherit;cursor:pointer}'
    + '.engSetPc:focus-visible{outline:2px solid var(--slate,#68899d);outline-offset:3px;border-radius:7px}.engSetPc:disabled{cursor:default;opacity:.6}'
    + '.engSetPc:not(:disabled):hover .engSetPic{border-color:var(--ink45)}.engSetPc[aria-pressed=true]{cursor:default}.engSetPc[aria-pressed=true] .engSetPic{outline:2px solid var(--gold,#a9823f);outline-offset:2px}.engSetPc[aria-pressed=true] .engSetLbl{color:var(--ink)}'
    + '.engSetPic{display:block;box-sizing:border-box;width:100%;min-width:0;aspect-ratio:4/1;background:#fff;border:1px solid var(--line);border-radius:7px;overflow:hidden;transition:border-color .13s ease}'
    + '.engSetLbl{margin-top:3px;font:600 11.5px/14px var(--sans);color:var(--ink70);max-width:100%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}'
    + '.engSetLen{font:600 11px/13px var(--mono);color:var(--ink70);white-space:nowrap}'
    + '.engSetNote{font:12px/1.4 var(--sans);color:var(--ink70);background:var(--goldSoft,#f6efdc);border:1px solid var(--line);border-radius:8px;padding:4px 9px}'
    + '@media (prefers-reduced-motion:reduce){.engSetPic{transition:none}}';
  const CSS = '.engCard{container:engc/inline-size;box-sizing:border-box;width:100%;min-width:0;padding:7px;border:1px solid var(--line);border-radius:12px;background:var(--card);display:grid;gap:4px;align-content:start;overflow-wrap:anywhere}'
    + '.engHd{display:flex;align-items:center;gap:6px;min-width:0;min-height:22px}'
    + '.engDot{flex:none;width:8px;height:8px;border-radius:50%;background:var(--ink45);opacity:.85}.engDot[data-s=words]{background:#b04a32;opacity:1}.engDot[data-s=approve]{background:#b8892f;opacity:1}.engDot[data-s=approved]{background:#5f7a5b;opacity:1}.engDot[data-s=plain]{background:var(--line);opacity:1}'
    + '.engDot[data-metal]{background:var(--mc);opacity:1}'
    + '.engName{flex:1 1 0;min-width:0;font:600 12.5px/1.3 var(--sans);color:var(--ink);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}'
    + '.engName.engTagged{display:grid;gap:1px;line-height:1.25}.engName>.engPc{font:600 9.5px/1.2 var(--mono);letter-spacing:.07em;text-transform:uppercase;color:var(--ink45);overflow:hidden;text-overflow:ellipsis}'
    + '.engName:has(>.engTail){display:flex}.engName>.engNm{min-width:0;overflow:hidden;text-overflow:ellipsis}.engName>.engTail{flex:none;white-space:pre}'
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
     A thumbnail darkens its edge and gains a soft shadow on hover (it does not move: a hovered card must not shift under the pointer), presses down one pixel, and has its own focus ring; a thumbnail that cannot open is not-allowed. */
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
    + `.engThumb${NOPE}:hover{border-color:var(--ink45);box-shadow:0 4px 12px rgba(20,16,10,.14)}.engThumb${NOPE}:active{border-color:var(--ink);box-shadow:none;transform:translateY(1px)}`
    + '.engThumb[aria-disabled=true]{cursor:not-allowed;opacity:.7}'
    // (the room for the button row: a gap less, a pixel less above and below, and the two pictures 80px. These are the card's own rules, kept by the Engrave tab's detail card; the order window's Engraving section sets its pictures twice as large (160px and 208px) in charm-nest-1.html, ENGBIG)
    + '.engCard{gap:3px;padding:6px 7px}@container engc (max-width:215px){.engFaces{grid-template-columns:repeat(2,minmax(0,80px));justify-content:space-between}.engThumb{max-width:80px}}'
    + '@container engc (max-width:150px){.engCard .egOpen{padding:0 4px;font-size:11px}.engCard .egOpen span[aria-hidden]{display:none}.engCard .egOpen[aria-busy=true]{height:auto;min-height:22px;padding:3px 4px;line-height:1.15;white-space:normal;text-align:center}}'   // (a card of 122px, the 1024px window: "Fix in Engraving" on one line without its arrow; while it opens the words may take two lines)
    + '@media (prefers-reduced-motion:reduce){.engThumb{transition:none}.engThumb:active{transform:none}}';
  /** The button's states, for every place that draws the button (once per page, at load; the card's own rules go in with the first card). */
  function openCss() {
    const d = root && root.document; if (!d || !d.head || d.getElementById('engOpenCss')) return;
    const s = d.createElement('style'); s.id = 'engOpenCss'; s.textContent = OPEN_CSS; d.head.appendChild(s);
  }
  function css() {
    const d = root && root.document; if (!d || d.getElementById('engCardCss')) return;
    openCss(); const s = d.createElement('style'); s.id = 'engCardCss'; s.textContent = CSS + BTN_CSS + SET_CSS; (d.head || d.documentElement).appendChild(s);
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
    const btn = el.querySelector('.egOpen'); let busy = false;
    const keyOf = face => (piece.faces[face] && piece.faces[face].key) || jobKey(piece.lineKey, piece.slot, face);
    const things = () => [...(btn ? [btn] : []), ...el.querySelectorAll('.engFace .engThumb')];
    /** Is the piece openable NOW? Asked again at every press, hover and focus (a piece that was on hold may have reached its sheet since the card was drawn): the button and the pictures follow it. */
    const sync = () => {
      const g = btn ? openOf(piece) : { ok: true, why: '' };
      for (const t of things()) {
        if (g.ok) { if (t.getAttribute('aria-disabled')) { t.removeAttribute('aria-disabled'); t.removeAttribute('title'); } }
        else { t.setAttribute('aria-disabled', 'true'); t.title = g.why; }
      }
      return g;
    };
    const press = (face, from) => {
      if (busy) return; const g = sync(); if (!g.ok) return;
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
      th.addEventListener('click', () => press(face, th));
    }
    if (btn) btn.addEventListener('click', () => press(btn.dataset.face || openOf(piece).face, btn));
    if (btn) { sync(); for (const t of things()) { t.addEventListener('pointerenter', sync); t.addEventListener('focus', sync); } }
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
  return { piecesOf, facesOf, stateOf, html, fill, card, sig, mountPiece, thumbOf, repaint, sealsOf, css, WORD, openOf, openHtml, OPEN_WORD, metalOf, lineCountOf, setStrip, layoutNoteOf, relOfSet, LAYOUT_NOTE };
});
