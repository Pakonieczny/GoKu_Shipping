/* The engraving of the PIECES of an order, drawn wherever the order window shows it (Paul, 12 Oct 2026: "show the cards for each piece that needs to be engraved ... the cards are currently
 * full of needless text ... front and back side-by-side in a larger card"; plans/engrave-ui-1012/CONTRACT.md section C). One card for each PIECE (a charm, one ear of a pair, one disc), Front and
 * Back in the SAME card (charm-nest-engrave-card.js draws it: CharmNestEngraveCard); this file keeps it alive: which pieces the order has, their jobs, the one Approve button and the way into
 * the Engrave tab, with every rule the earlier card had (the approval and its seal pressed on the button through CNEngravingSeals.press inside Engrave.approve, an approval another computer
 * makes shown from the timeline, "Approve" for the faces of a piece in turn, never a name typed, every wait said by a small spinner).
 *
 *   OrderEngraving.mount(host, { rid, key, rows?, nameOf?, poolId?, piece?, row?, events?, changed?, sheetEng?, label?, meta?, compact?, pieces? })
 *       -> { update(ctx), destroy(), refresh(), el, mode: 'one' | 'list' }
 *       rid     the order (receipt id)           key    the order line's key
 *       rows    a function giving every line of the order (the cards of all of them: the section never follows the piece switcher); else `pieces` ([{ key, row, label }]), else the line `row`/`key`
 *       nameOf  the name of a line, as the card says it (default: its design's name)
 *       events  the order's timeline events (an array, or a function giving them): an approval another computer makes shows here from them, within the timeline feed's own 2.5 s read
 *       changed called when something other than this card's own press changed the engraving, and after this card's own approval: the host repaints what depends on it
 *     The host's DIRECT CHILDREN are the cards (`.engCard`, one for each piece that has something to engrave); the host is hidden when there is none. Nothing else is drawn in it.
 *   OrderEngraving.engOf(ctx) -> { row, job, eng, poolId, loading }   what the earlier single card would show, as data (no drawing)
 *   OrderEngraving.cycleOf / discLines / faceCell / foldSlots / bothFor / approveFaces / backOf / unmount   as before (the bridge and the sheet window call them)
 *
 * Live without a request of its own: Engrave's jobs are in this page, so a change made in the Engraving tab, the Sheet tab or the sheet window is seen within a second by comparing them; the
 * timeline feed the order window already reads says what another computer did. Seals are never touched here: Engrave.approve adds the new one, the card shows all of them, and a card is never
 * drawn again while a seal is being pressed.
 */(function (root) {
  'use strict';
  const doc = () => root.document;
  const tryDo = (f, d) => { try { return f(); } catch (e) { try { console.warn('order engraving:', e); } catch (_) {} return d; } };
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const DONE = ['approved', 'written'];
  const cors = u => (root.CharmNestAssets && root.CharmNestAssets.url ? tryDo(() => root.CharmNestAssets.url(u), u) : u);
  const say = (msg, kind, ms) => { try { if (typeof root.toast === 'function') root.toast(msg, kind || '', ms || 3600); } catch (_) {} };
  /* ── the FACE of an engraving (charm-nest-engrave-faces.js; a job that does not say "front" is the back, so a back-only line is drawn and does exactly what it always did) ── */
  const FACES = () => root.CharmNestEngraveFaces || null;
  const parseKey = key => { const F = FACES(); if (F && F.parseKey) return F.parseKey(key); const m = /^(.*?)(?:#(L|R|D\d{1,2}))?(~front)?$/.exec(String(key == null ? '' : key)); return { key: String(key == null ? '' : key), lineKey: m ? m[1] : '', rowKey: m ? m[1] : '', slot: m && m[2] || null, face: m && m[3] ? 'front' : 'back' }; };
  const faceOf = x => { const F = FACES(); return F && F.faceOf ? F.faceOf(x) : x && (x.face === 'front' || /~front$/.test(String(x.key || ''))) ? 'front' : 'back'; };
  /** The words of the card for one face: what it says of "the back" it says of "the front" for a front job, and nothing else is touched. */
  const aboutFace = (text, face) => (face === 'front' ? String(text).replace(/\bback\b/g, 'front').replace(/\bBack\b/g, 'Front') : text);


  /* ── the card itself is charm-nest-engrave-card.js; here only the wait of a line still being read ── */
  const CARD = () => root.CharmNestEngraveCard || null;
  function css() {
    const d = doc(); if (!d || d.getElementById('owEngCss')) return;
    const s = d.createElement('style'); s.id = 'owEngCss';
    // (.engSaid: the customer's own words under the cards, one quiet block: a hairline at its left, small mono labels in front of each part, no box and no title)
    s.textContent = '.owEngWait{display:flex;align-items:center;gap:8px;min-width:0;padding:9px 12px;border:1px solid var(--line);border-radius:12px;background:var(--card);font:11px var(--sans);color:var(--ink45)}'
      + '.engSaid{margin:8px 0 0;padding:1px 0 1px 9px;border-left:2px solid var(--line);min-width:0;font:13px/1.45 var(--serif);color:var(--ink);overflow-wrap:anywhere}.engSaid[hidden]{display:none}'
      + '.engSaid .esName{font:700 9.5px/1.4 var(--mono);letter-spacing:.08em;text-transform:uppercase;color:var(--ink45);margin-top:6px}.engSaid .esName:first-child{margin-top:0}'
      + '.engSaid .esRow{min-width:0}.engSaid .esFaces{font:12.5px/1.45 var(--sans);color:var(--ink70)}'
      + '.engSaid i{font:700 9.5px var(--mono);letter-spacing:.08em;text-transform:uppercase;color:var(--ink45);font-style:normal;margin-right:2px}.engSaid .esSep{margin:0 3px;color:var(--ink45)}';
    (d.head || d.documentElement).appendChild(s);
  }
  const waitNode = text => { const n = doc().createElement('div'); n.className = 'owEngWait'; n.setAttribute('role', 'status'); n.setAttribute('aria-live', 'polite'); n.innerHTML = `<span class="owSpin" aria-hidden="true"></span><span>${esc(text)}</span>`; return n; };

  /* ── what the customer wrote for the engraving, ONE quiet block under the cards (Paul, 12 Oct 2026, round 2: "in the detailed order modal I don't see the customers message for the back engraving") ──
   * Per engraved line: the customer's own words as they wrote them (the personalisation box, the buyer's message, the staff note) and the engraving choices among their options (front / back, the font), each under its
   * small label, and, only when the piece has BOTH faces, which words are engraved on which: "Front W  Back Name One". Nothing for a line the customer wrote and chose nothing for; no title, no sentence, no percentage.
   * The words and the options are read by charm-nest-engrave-said.js (the Engrave tab's box reads the same); when that file is not on the page the three plain parts are read here. */
  const oneLine = t => String(t == null ? '' : t).split(/\r?\n/).map(x => x.replace(/\s+/g, ' ').trim()).filter(Boolean).join(' / ');
  function saidRows(row) {
    const S = root.CharmNestEngraveSaid, L = (row && row.parentRow) || row || {}, sp = L.spec || {}, line = L.line || {}, O = root.CharmNestOrders;
    let rows = S && S.rowsOf ? tryDo(() => S.rowsOf(L), null) : null;
    if (!Array.isArray(rows)) {
      const vis = x => String(O && O.visible ? O.visible(x) : x == null ? '' : x).trim(); rows = [];
      const pers = (sp.personalization && sp.personalization.length ? sp.personalization : line.personalization || []).map(vis).filter(Boolean);
      if (pers.length) rows.push(['Personalisation', pers.join('\n')]);
      if (vis(sp.buyerMessage)) rows.push(['Buyer message', vis(sp.buyerMessage)]);
      if (vis(sp.staffNote)) rows.push(['Staff note', vis(sp.staffNote)]);
    }
    return rows.map(([k, v]) => [String(k), k === 'Options' ? String(v).split(/\r?\n/).map(x => x.trim()).filter(Boolean).join(' \u00b7 ') : oneLine(v)]).filter(([, v]) => v);
  }
  /** The block's data: one entry per line that has a card, { name, rows: [[label, text]], faces: [['Front', words], ['Back', words]] | null }; the lines the customer wrote and chose nothing for are left out. */
  function saidOf(rows, pieces) {
    const out = [], seen = new Set();
    for (const row of rows || []) {
      const line = (row && row.parentRow) || row; if (!line || seen.has(line.key)) continue;
      const mine = (pieces || []).filter(p => p && p.lineKey === line.key); if (!mine.length) continue;   // (a line with nothing to engrave has no card and no words here)
      seen.add(line.key);
      const parts = saidRows(line), one = mine.length === 1 ? mine[0] : null, f = one && one.faces || {};
      const faces = f.front && f.back && f.front.text && f.back.text && !f.front.plain && !f.back.plain ? [['Front', oneLine(f.front.text)], ['Back', oneLine(f.back.text)]] : null;
      if (!parts.length) continue;   // (the customer wrote and chose nothing: nothing is said, whatever the faces read)
      out.push({ name: mine[0].name || '', rows: parts, faces });
    }
    return out;
  }
  function saidHtml(entries) {
    const many = entries.length > 1, row = ([k, v]) => `<div class="esRow" data-said="${esc(k)}"><i>${esc(k)}</i> ${esc(v)}</div>`;
    return entries.map(e => (many && e.name ? `<div class="esName">${esc(e.name)}</div>` : '')
      + e.rows.filter(r => r[0] !== 'Options').map(row).join('')
      + (e.faces ? `<div class="esRow esFaces" data-said="faces">${e.faces.map(([f, t]) => `<i>${esc(f)}</i> ${esc(t)}`).join(' <span class="esSep" aria-hidden="true">\u00b7</span> ')}</div>` : '')
      + e.rows.filter(r => r[0] === 'Options').map(row).join('')).join('');
  }


  /* ── what a piece's back engraving is, as the card shows it ── */
  const ordersRows = () => tryDo(() => (root.Orders && root.Orders.rows && root.Orders.rows()) || [], []);
  function rowFor(ctx) {
    if (ctx.row) return ctx.row;
    const rows = ordersRows();
    // (a job's own key, "<line key>#L" for the left ear, "<line key>~front" for a front, names its line)
    const lineKey = parseKey(ctx.key).lineKey;
    return rows.find(r => r.key === ctx.key) || rows.find(r => r.key === lineKey) || (ctx.poolId && rows.find(r => (r.poolIds || []).includes(ctx.poolId))) || null;
  }
  function jobFor(row, poolId, key) {
    const E = root.Engrave; if (!E || !E.items) return null;
    // one ear (or one disc) of a pair, or the front of a charm, has its own job: the key it was asked by, else its piece, before the line's
    // (a key that IS a job's key names that job and no other face of the piece: a back card asked by its own plain key is never shown the piece's front because the front is the one still to settle)
    if (key) { const own = tryDo(() => E.items().get(String(key)), null); if (own && !own.editingBack) return own; }
    const hit = row && E.jobOf ? tryDo(() => (poolId ? E.jobOf(row, poolId) : E.jobOf(row)), null) : null; if (hit) return hit;
    if (!poolId) return null;
    // (the piece's other face is a different engraving: the face the key names, else the one still to settle, else the back; never just the first job that holds the copy)
    const held = [...E.items().values()].filter(j => !j.editingBack && (j.copies || []).includes(poolId)), want = key && parseKey(key).face === 'front' ? 'front' : '';
    return held.find(j => want && faceOf(j) === want) || (want ? null : held.find(j => OPEN_STATES.includes(j.state)) || held.find(j => faceOf(j) === 'back') || held[0]) || null;
  }
  /** The counted discs of one line, one at a time (Paul, 10 Oct 2026: "cycle through all the chosen discs one by one ... everywhere the engraving option is presented"): the line's jobs
   *  as the Engrave tab's own switch component tells them (charm-nest-piece-switch.js: each disc's words, font by name and state), and the disc shown. null for every other line (a single, a pair,
   *  a plain quantity-N line: one job, or the Left and the Right), so those cards are drawn exactly as they always were. The disc shown: `key` when it is one of them, else the first disc still
   *  to settle, else Disc 1. */
  const DISC = /^D\d{1,2}$/, OPEN_STATES = ['reclassify', 'classify', 'words', 'blocked', 'ready', 'fitting', 'review'];
  const fontOfJob = job => {
    const S = root.CharmNestEngraveSides, rec = S && S.pieceRecord ? tryDo(() => S.pieceRecord(job), null) : null;   // DISCREAD's record of the disc: its font, else the line's
    const f = (rec && rec.font) || (job && (job.font || (job.piece && job.piece.font) || (job.row && job.row.spec && job.row.spec.font))), asked = String((f && typeof f === 'object' && f.asked) || (job && job.requests && job.requests.font) || '');
    const name = typeof f === 'string' ? f : f && typeof f === 'object' ? String(f.name || f.installed || '') : '';
    return { name: name || asked, asked };
  };
  /** The piece the card is for, when the host names one (an ear's side row, or a key with a slot): null for the line as a whole. */
  const slotWanted = (row, key) => parseKey(key).slot || (row && row.slot) || null;
  function cycleOf(row, key) {
    const E = root.Engrave, PS = root.CharmNestPieceSwitch; if (!E || !E.jobsOf || !PS || !row) return null;
    const jobs = (tryDo(() => E.jobsOf(row), []) || []).filter(j => j && !j.editingBack);
    const F = FACES(), front = !!F && jobs.some(j => F.isFront(j));
    let scope = jobs, faces = false;
    if (front) {
      // a line with a FRONT job: the discs of a counted line stay one card that cycles (each disc with its Front | Back), a single charm or one ear is a card with its Front | Back
      const groups = F.groupPieces(jobs), discs = groups.length >= 2 && groups.every(g => DISC.test(String(g.slot || '')));
      const want = slotWanted(row, key), one = discs ? null : groups.length === 1 ? groups[0] : want ? groups.find(g => g.slot === want) : null;
      if (!discs && !(one && one.front && one.back)) return null;
      scope = discs ? groups.flatMap(g => g.jobs) : one.jobs; faces = groups.some(g => g.front && g.back);
    } else if (jobs.length < 2 || !jobs.every(j => DISC.test(String(j.slot || '')))) return null;
    const of = F && F.pieceCount ? F.pieceCount(scope) : scope.length;
    const pieces = PS.list(scope, { of, fontOf: fontOfJob, isWorking: j => !!(E.isWorking && tryDo(() => E.isWorking(j), false)) });
    // (DISCREAD's one record of each disc rides along: its index, tag, words and font are what the switch and the cell say)
    const S = root.CharmNestEngraveSides, recs = S && S.pieceRecords ? tryDo(() => S.pieceRecords(scope, { of }), []) || [] : [];
    for (const p of pieces) p.rec = recs.find(r => r.key === p.key) || null;
    const at = pieces.find(p => p.key === key) || pieces.find(p => OPEN_STATES.includes(p.state)) || pieces[0];
    return faces ? { pieces, key: at.key, faces: true } : { pieces, key: at.key };
  }
  /** The line's engraving said disc by disc ("Disc 1 · J · Approved"), for the one cell that sums it up; null for a line that is not counted discs.
   *  A counted line with a front says each disc's faces ("Disc 1 · Front · J · Approved", "Disc 1 · Back · Q · Placement to check"). */
  function discLines(row) {
    const c = cycleOf(row, null); if (!c) return null;
    if (c.faces) return faceLines(c);
    return c.pieces.map(p => { const r = p.rec || p, words = r.words; return `${r.label || p.label}${words ? ' · ' + words : ''}${p.stage ? ' · ' + p.stage : ''}`; });
  }
  /** One line for each face of each piece, Front before Back: "[Left ·] Front · W · Approved". */
  function faceLines(c) {
    const F = FACES(), groups = F.groupPieces(c.pieces.map(p => p.job).filter(Boolean)), many = groups.length > 1, out = [];
    for (const g of groups) for (const j of g.jobs) {
      const p = c.pieces.find(x => x.key === j.key) || {}, words = p.words || ((j.lines || []).join(' / '));
      out.push(`${many && g.label ? g.label + ' · ' : ''}${F.label(faceOf(j))}${words ? ' · ' + words : ''}${p.stage ? ' · ' + p.stage : ''}`);
    }
    return out;
  }
  /** The facts grid's "Engraving" cell for a line that has a FRONT job (Paul: "Front: W · Back: 1. 27. 24" with each face's approval and its own seals): one line for each face of each piece, with the
   *  seals of every face after them (Front first; a seal says FRONT ENGRAVING or BACK ENGRAVING on its stamp). null for every line without a front, which keeps the cell it always had.
   *  `extra`: seals the order's timeline holds that no job carries (an older compact row), laid on a single charm's faces by the face they say. */
  function faceCell(r, wait, extra) {
    const E = root.Engrave, F = FACES(), S = root.CNEngravingSeals; if (!E || !E.jobsOf || !F || !S || !r) return null;
    const jobs = (tryDo(() => E.jobsOf(r), []) || []).filter(j => j && !j.editingBack);
    if (!jobs.some(j => F.isFront(j))) return null;
    const PS = root.CharmNestPieceSwitch; if (!PS) return null;
    const groups = F.groupPieces(jobs), ordered = groups.flatMap(g => g.jobs);
    const pieces = PS.list(ordered, { of: groups.length, fontOf: fontOfJob, isWorking: j => !!(E.isWorking && tryDo(() => E.isWorking(j), false)) });
    const lines = faceLines({ pieces });
    const sealsOf = j => {
      const own = groups.length === 1 && Array.isArray(extra) ? extra.filter(x => faceOf(x) === faceOf(j)) : [];
      return S.html(own.length ? { seals: S.merge(j, { seals: own }) } : j);
    };
    return `<span>${lines.map(esc).join('<br>')}</span>${ordered.map(sealsOf).join('')}`;
  }
  /** "Approve both faces" for the piece a cycle shows: { jobs: [Front, Back], ok, why } when that piece has a Front and a Back and both wait for approval (state review), else null. `ok` is the model's own
   *  gate (Faces.approveAllGate: every placement ready, nothing running, and each placement seen: `seen(job)`), `why` its plain reason when not. Offered by the card here and by the order window's Sheet tab. */
  function bothFor(c, job, seen) {
    const F = FACES(), E = root.Engrave; if (!F || !F.approveAllGate || !c || !c.faces || !job) return null;
    const g = F.groupPieces(c.pieces.map(p => p.job).filter(Boolean)).find(x => x.jobs.some(j => j.key === job.key));
    if (!g || !g.front || !g.back || g.front.state !== 'review' || g.back.state !== 'review') return null;
    const gate = F.approveAllGate(g.jobs, { busy: j => !!(E && E.isWorking && tryDo(() => E.isWorking(j), false)), shown: j => !seen || seen(j) });
    return { jobs: g.jobs, ok: gate.ok, why: gate.why };
  }
  /** The ordinary approval of each job in turn (Front first), each with its own seal and back file, as if each Approve had been pressed; the button given is the shown job's own, where its seal is pressed.
   *  Stops at the first that did not stand (the rest are left as they are). { ok, failed } */
  async function approveFaces(jobs, who, shownJob, shownBtn) {
    for (const j of jobs || []) {
      if (j.state !== 'review') continue;
      await root.Engrave.approve(j, who, j === shownJob ? shownBtn || undefined : undefined);
      if (!DONE.includes(j.state)) return { ok: false, failed: j };
    }
    return { ok: true, failed: null };
  }
  /** The back records Engrave holds for a job (the written ones carry the saved picture): this piece's, the newest. */
  function backOf(job, poolId) {
    const face = faceOf(job), all = (job.backs || []).filter(b => b && faceOf(b) === face), mine = poolId ? all.filter(b => b.poolId === poolId) : [];   // (a front's record is never shown as its back's picture)
    const list = mine.length ? mine : all;
    return list.reduce((a, b) => (!a || (+b.approvedAt || 0) >= (+a.approvedAt || 0) ? b : a), null);
  }
  /** One job read the way the Sheet tab and the sheet window read it (the same kinds, the same words). */
  function fromJob(job, saved, poolId) {
    const E = root.Engrave, s = job.state;
    const base = { job, saved: saved || undefined };
    if (s === 'none') return Object.assign(base, { kind: 'none' });
    if (DONE.includes(s)) return Object.assign(base, { kind: 'approved', back: backOf(job, poolId) || undefined, by: job.approvedBy, at: job.approvedAt, text: job.text });
    if (s === 'skipped') return Object.assign(base, { kind: 'skipped', by: job.approvedBy, text: job.text });
    if (s === 'review') return Object.assign(base, { kind: 'approve', text: job.text });
    if (s === 'words' || s === 'blocked') return Object.assign(base, { kind: 'words', text: job.text, reason: job.reason });
    const working = !!(E && E.isWorking && tryDo(() => E.isWorking(job), false));
    const note = working ? (s === 'classify' ? 'Reading the words…' : aboutFace('Fitting the words on the back…', faceOf(job)))
      : s === 'classify' ? 'Confirm them in Engraving, or wait for them to be read'
      : E && E.canFit && !tryDo(() => E.canFit(job), true) ? 'Waits for its sheet to be written, then it is fitted' : 'Waits its turn in Engraving';
    return Object.assign(base, { kind: 'preparing', text: job.text, working, note });
  }
  function fromSaved(re) {
    if (re.needed) return { kind: re.approved ? 'approved' : 'words', text: re.text, by: re.approvedBy, at: re.approvedAt, saved: re };
    return { kind: re.state === 'skipped' ? 'skipped' : 'none', text: re.text, saved: re };
  }
  /** An approval of this line the timeline holds that is newer than anything this card could have seen: another computer's. */
  function foreignApproval(row, job, events, seenAt) {
    const S = root.CNEngravingSeals; if (!S || !row || !Array.isArray(events) || !events.length) return null;
    if (job && (DONE.includes(job.state) || job.state === 'skipped' || job.stamping || job.approvalPreparing || job.approvalIntent)) return null;
    // (the other face's approval is not this card's: only the events that say this job's face are read; an event that says nothing is a back's)
    const face = job ? faceOf(job) : 'back', mine = events.filter(e => faceOf(e) === face);
    const seals = tryDo(() => S.fromEvents(mine, row), []).filter(x => x.how === 'engraveApproved' && x.by && +x.at > 0);
    const newest = seals.reduce((a, b) => (!a || b.at > a.at ? b : a), null);
    if (!newest || newest.at <= Math.max(+(job && job.approvedAt) || 0, (seenAt || 0) - 2000)) return null;
    const ev = mine.find(e => e.type === 'engraveApproved' && +e.at === newest.at);
    return { by: newest.by, at: newest.at, text: ev && ev.data && ev.data.text ? String(ev.data.text) : '' };
  }
  function engOf(ctx, seenAt) {
    const row = rowFor(ctx), poolId = ctx.poolId || (row && (row.poolIds || [])[0]) || '';
    if (row && row.loading) return { row, poolId, loading: true, job: null, eng: null };
    if (!root.Engrave || !root.CNEngravingSeals) return { row, poolId, job: null, eng: { kind: 'none' } };
    const job = jobFor(row, poolId, ctx.key), saved = job && (job.slot || (job.row && job.row.parentRow)) ? job.row.engrave || null : row && row.engrave || null;   // (a piece's, or a face's, own record)
    let eng;
    if (job) eng = fromJob(job, saved, poolId);
    else {
      const alt = typeof ctx.sheetEng === 'function' ? tryDo(() => ctx.sheetEng(), null) : null;
      eng = alt && alt.kind && alt.kind !== 'none' ? Object.assign({}, alt, { saved: saved || alt.saved }) : saved ? fromSaved(saved) : { kind: 'none' };
    }
    const events = typeof ctx.events === 'function' ? tryDo(() => ctx.events(), null) : ctx.events;
    const f = ['approve', 'words', 'preparing', 'none'].includes(eng.kind) ? foreignApproval(row, job, events, seenAt) : null;
    if (f) eng = Object.assign({}, eng, { kind: 'approved', by: f.by, at: f.at, text: eng.text || f.text, note: undefined, working: false, foreign: true });
    return { row, poolId, job, eng };
  }

  const ids = new WeakMap(); let idN = 0;
  const idOf = o => { if (!o || typeof o !== 'object') return 0; let n = ids.get(o); if (!n) ids.set(o, n = ++idN); return n; };

  /* ── the card on a page ── */
  const live = new Set(); let timer = 0, nextId = 0;
  const hidden = () => { try { return doc().hidden; } catch (_) { return false; } };
  function tick() { if (hidden()) return; for (const m of [...live]) tryDo(() => m._tick()); }
  function arm() { if (!timer && live.size) timer = root.setInterval(tick, 1000); }
  function disarm() { if (timer && !live.size) { root.clearInterval(timer); timer = 0; } }
  if (root.document && root.document.addEventListener) root.document.addEventListener('visibilitychange', () => { if (!hidden()) tick(); });
  /* ── an approval another computer made, read from the order's timeline (the earlier card's rule, kept) ── */
  /** An approval of this line the timeline holds that is newer than anything this card could have seen: another computer's. */
  function foreignApproval(row, job, events, seenAt) {
    const S = root.CNEngravingSeals; if (!S || !row || !Array.isArray(events) || !events.length) return null;
    if (job && (DONE.includes(job.state) || job.state === 'skipped' || job.stamping || job.approvalPreparing || job.approvalIntent)) return null;
    // (the other face's approval is not this card's: only the events that say this job's face are read; an event that says nothing is a back's)
    const face = job ? faceOf(job) : 'back', mine = events.filter(e => faceOf(e) === face);
    const seals = tryDo(() => S.fromEvents(mine, row), []).filter(x => x.how === 'engraveApproved' && x.by && +x.at > 0);
    const newest = seals.reduce((a, b) => (!a || b.at > a.at ? b : a), null);
    if (!newest || newest.at <= Math.max(+(job && job.approvedAt) || 0, (seenAt || 0) - 2000)) return null;
    return { by: newest.by, at: newest.at };
  }
  /** The piece with every face another computer approved shown as approved (its seal laid with the others). */
  function withForeign(piece, events, seenAt) {
    if (!Array.isArray(events) || !events.length) return piece;
    const extra = [];
    for (const f of ['front', 'back']) {
      const x = piece.faces[f]; if (!x || !x.job || x.state === 'approved' || x.state === 'plain') continue;
      const fa = foreignApproval(piece.row, x.job, events, seenAt); if (!fa) continue;
      x.state = 'approved'; x.foreign = fa; extra.push({ how: 'engraveApproved', at: fa.at, by: fa.by, ...(f === 'front' ? { face: 'front' } : {}) });
    }
    if (!extra.length) return piece;
    const C = CARD(), present = ['front', 'back'].map(f => piece.faces[f]).filter(Boolean);
    const RANK = { words: 5, busy: 4, approve: 3, approved: 2, plain: 1 };
    piece.state = present.reduce((a, b) => (!a || RANK[b.state] > RANK[a] ? b.state : a), '') || 'plain';
    if (piece.approve && !present.some(f => f.job && f.state === 'approve')) piece.approve = null;
    else if (piece.approve) piece.approve = Object.assign({}, piece.approve, { jobs: piece.approve.jobs.filter(j => present.some(f => f.job === j && f.state === 'approve')) });
    piece.seals = C && C.sealsOf ? C.sealsOf(piece.jobs, [{ seals: extra }]) : piece.seals;
    return piece;
  }
  const badToasts = () => new Map([...(root.document ? root.document.querySelectorAll('#toasts .toast.bad') : [])].map(n => [n, n.dataset.n]));

  /* ── the cards on a page ── */
  const defaultName = row => String((row && row.spec && row.spec.designSku) || (row && row.line && (row.line.sku || row.line.title)) || 'Piece').replace(/_+/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 40);
  function mountCards(host, ctx0) {
    css();
    if (!host) return null;
    if (host._orderEngraving) tryDo(() => host._orderEngraving.destroy());
    let ctx = Object.assign({}, ctx0), gone = false, seenAt = Date.now(), attached = false, ro = null, told0 = null;
    const cards = new Map();                 // pieceKey -> { el, sig }
    const busy = new Set(), seen = new Set(), fails = new Map();   // pieces being approved; job keys put in front of the person; the one failure line of a card
    let last = new Map(), asking = false, waitEl = null, kindsSeen = '', loading = false;
    const mode = Array.isArray(ctx0 && ctx0.pieces) && ctx0.pieces.length > 1 ? 'list' : 'one';
    const ident = c => [c.rid, c.key].join('|');
    // (a host we hid ourselves has no box: the order window being open is what counts, as the window being drawn)
    const visible = () => { if (!host.isConnected) return false; const dlg = host.closest && host.closest('dialog'); if (dlg) return !!dlg.open; const n = host.hidden ? host.parentElement : host; return !!(n && n.getClientRects().length); };
    const poke = () => { tryDo(() => root.RunCtl && root.RunCtl.poke && root.RunCtl.poke()); tryDo(() => root.OrderWin && root.OrderWin.nudge && root.OrderWin.nudge()); };
    const told = () => { if (typeof ctx.changed === 'function') tryDo(() => ctx.changed()); };
    const rowsOf = () => {
      if (typeof ctx.rows === 'function') return (tryDo(() => ctx.rows(), []) || []).filter(Boolean);
      if (Array.isArray(ctx.pieces) && ctx.pieces.length) return ctx.pieces.map(p => p.row || rowFor({ key: p.key, poolId: p.poolId })).filter(Boolean);
      const r = rowFor(ctx); return r ? [r] : [];
    };
    const nameOf = row => { const n = typeof ctx.nameOf === 'function' ? tryDo(() => ctx.nameOf(row), '') : ''; return n || (ctx.label && !ctx.pieces && !ctx.rows ? ctx.label : '') || defaultName(row); };
    const events = () => (typeof ctx.events === 'function' ? tryDo(() => ctx.events(), null) : ctx.events);
    const isWorking = j => !!(root.Engrave && root.Engrave.isWorking && tryDo(() => root.Engrave.isWorking(j), false));
    /** What the cards show now, as data. */
    function read() {
      const C = CARD(), rows = rowsOf(), vis = visible();
      const pieces = C ? C.piecesOf(rows, { nameOf, isWorking, seen: j => vis || seen.has(j.key) }) : [];
      const ev = events(); for (const p of pieces) withForeign(p, ev, seenAt);
      return { rows, pieces, loading: rows.some(r => r && r.loading) };
    }
    const keyOf = p => p.pieceKey + '|' + p.rid + '|' + p.lineKey;
    /** The customer's words block: the host's next sibling inside the section (never a child of the host: the host's children are the cards). Drawn again only when what it says changes. */
    let saidEl = null, saidSig = '';
    function paintSaid(r) {
      const entries = r && !r.loading ? tryDo(() => saidOf(r.rows, r.pieces), []) || [] : [];
      if (!saidEl && entries.length && host.parentNode) { saidEl = doc().createElement('div'); saidEl.className = 'engSaid'; saidEl.id = 'owEngSaid'; saidEl.setAttribute('role', 'group'); saidEl.setAttribute('aria-label', 'What the customer wrote'); saidEl.hidden = true; host.parentNode.insertBefore(saidEl, host.nextSibling); }
      if (!saidEl) return;
      const sig = JSON.stringify(entries);
      if (sig !== saidSig) { saidSig = sig; saidEl.innerHTML = entries.length ? saidHtml(entries) : ''; }
      saidEl.hidden = !entries.length;
    }
    /** A press on a face: that piece and that face open in the Engrave tab (EngraveLink, the order window closes as it opens). */
    function openEngrave(face, key, btn, piece) {
      const cur = last.get(keyOf(piece)) || piece, fj = cur.faces[face] && cur.faces[face].job, any = fj || cur.jobs[0] || null, link = root.EngraveLink;
      const t = { rid: String(cur.rid || ctx.rid || ''), key: cur.lineKey, poolId: ((any && any.copies) || [])[0] || (cur.row && (cur.row.poolIds || [])[0]) || '', piece: fj ? fj.key : (any ? any.key : key), face };
      if (link && typeof link.open === 'function') return Promise.resolve(link.open(t)).catch(e => { say('Could not open Engraving: ' + (e && e.message || e), 'bad', 5000); });
      return fallbackOpen({ job: any, eng: { kind: cur.state === 'approved' ? 'approved' : '' } }, t).catch(e => { say('Could not open Engraving: ' + (e && e.message || e), 'bad', 5000); });
    }
    function showFail(el, msg) { const f = el && el.querySelector('.engFail'); if (!f) return; f.textContent = msg || ''; f.hidden = !msg; }
    /** The one Approve button of a card: the faces of this piece that wait for approval are approved in turn (Front first), each with its own seal and back file, exactly as if each had been pressed. */
    async function approve(piece, btn) {
      const S = root.CNEngravingSeals, k = keyOf(piece); if (!S || !root.Engrave || !btn) return;
      const cur = last.get(k) || piece, ap = cur.approve; if (!ap || !ap.ok) { refresh(true, true); return; }
      const jobs = ap.jobs.filter(j => j.state === 'review'); if (!jobs.length) { refresh(false, true); return; }
      // (a card that is out of date approves nothing: only the placements it showed, still waiting for approval, are approved)
      const drawn = (btn.closest('.engCard') || {})._drawn;
      if (drawn && jobs.some(j => !drawn.has(j) || drawn.get(j)[0] !== j.fit || drawn.get(j)[1] !== j.view)) { refresh(true, true); return; }
      const was = new Map(jobs.map(j => [j, [j.fit, j.view]]));
      let who = S.who();
      if (typeof who !== 'string') {
        if (asking) return; asking = true;
        try { who = await who; } finally { asking = false; }
        if (!who || gone) return;
        if (!btn.isConnected) btn = host.querySelector(`.engCard[data-piece="${cssEsc(cur.pieceKey)}"] [data-e=approve]`);   // (the card was drawn again while the name was typed)
        if (!btn || btn.disabled) return;
      }
      const now = (last.get(k) || {}).approve;
      if (!now || !now.ok || now.jobs.length !== jobs.length || now.jobs.some(j => !was.has(j) || j.state !== 'review' || j.fit !== was.get(j)[0] || j.view !== was.get(j)[1])) { refresh(true, true); return; }
      busy.add(k); fails.delete(k); showFail(btn.closest('.engCard'), '');
      S.busyLabel(btn);
      const wasToasts = badToasts(); let ok = true, msg = '';
      try {
        const run = approveFaces(jobs, who, jobs[0], btn);   // (the first approval starts at once and puts "Approved" on the button: the wait is said again, with its spinner, until the stamp)
        S.busyLabel(btn);
        const done = await run;
        if (!done.ok) { ok = false; msg = `The ${faceOf(done.failed)} was not approved`; }
        else if (jobs.some(j => !DONE.includes(j.state))) ok = false;
        if (!ok && jobs.some(j => j.row && j.row.state === 'gone')) msg = 'This order has left the pull (cancelled or shipped)';
      } catch (e) { ok = false; msg = (e && e.message) || 'Not approved'; }
      finally { busy.delete(k); }
      if (!ok) {
        const toasted = [...badToasts()].filter(([n, c]) => wasToasts.get(n) !== c).map(([n]) => n.dataset.msg).filter(Boolean).pop();
        if (toasted) msg = toasted;
        if (msg) fails.set(k, msg);
        if (btn.isConnected) { btn.disabled = false; btn.removeAttribute('aria-busy'); btn.textContent = 'Approve'; showFail(btn.closest('.engCard'), msg); }
      }
      if (gone) return;
      if (ok) poke();
      refresh(true, true);
      if (ok) told();
    }
    const cssEsc = v => (root.CSS && root.CSS.escape ? root.CSS.escape(v) : String(v).replace(/["\\]/g, '\\$&'));
    const mkCard = piece => {
      const C = CARD(), el = C.card(piece, { imageUrl: u => (/^https?:/.test(u) ? cors(u) : u), onOpen: (face, key, btn, p) => openEngrave(face, key, btn, p || piece) });
      // (what each placement looked like when this card was drawn: an approval presses only what the person saw)
      el._drawn = new Map(piece.jobs.map(j => [j, [j.fit, j.view]]));
      const b = el.querySelector('[data-e=approve]'); if (b) b.addEventListener('click', () => { if (!b.disabled) approve(piece, b); });
      const f = fails.get(keyOf(piece)); if (f && piece.state === 'approve') showFail(el, f); else fails.delete(keyOf(piece));
      return el;
    };
    /** Look again; draw only the cards whose content has changed (or all at once when asked). `quiet`: the host is not told. */
    function refresh(force, quiet) {
      if (gone || !host.isConnected) return;
      attached = true;
      const C = CARD(); if (!C) return;
      const r = read(), vis = visible();
      paintSaid(r);
      // (this card's own approval is on its way: its card is left as it is until the approval stands, i.e. the stamp has landed)
      const stamping = r.pieces.some(p => p.jobs.some(j => j.stamping));
      for (const p of r.pieces) last.set(keyOf(p), p);
      const keys = r.pieces.map(keyOf);
      for (const k of [...last.keys()]) if (!keys.includes(k)) last.delete(k);
      const sigs = new Map(r.pieces.map(p => [keyOf(p), C.sig(p)]));
      const kinds = r.pieces.map(p => keyOf(p) + ':' + p.state).join(',');
      const loadedNow = !!r.loading !== loading;   // (the order stopped being read, or started: what is on screen is drawn again, e.g. the wait goes when the order turns out not to be there)
      loading = !!r.loading;
      const changed = force || loadedNow || keys.join('\n') !== [...cards.keys()].join('\n') || r.pieces.some(p => (cards.get(keyOf(p)) || {}).sig !== sigs.get(keyOf(p)));
      if (!changed && !(r.loading && !r.pieces.length && !waitEl)) { if (vis) for (const p of r.pieces) for (const j of p.jobs) seen.add(j.key); return; }
      // (something is here to show but cannot be drawn yet: the window is not on screen, or a seal is being pressed: it says so)
      const wait = () => { if (!host.firstChild && (r.pieces.length || r.loading)) { host.hidden = false; waitEl = waitNode('Reading…'); host.replaceChildren(waitEl); } };
      if (!vis) return wait();
      if (stamping) return wait();   // (drawn once the seal has landed)
      if (root.Seal && root.Seal.defer && root.Seal.defer('order-engraving:' + id0, () => refresh(true, quiet))) return wait();
      if (!r.pieces.length) {
        if (r.loading) { host.hidden = false; if (!waitEl || !waitEl.isConnected) { waitEl = waitNode('Reading…'); host.replaceChildren(waitEl); } cards.clear(); return; }
        cards.clear(); waitEl = null; host.replaceChildren(); host.hidden = true;
        if (told0 !== '' && !quiet) told();
        told0 = ''; return;
      }
      waitEl = null; host.hidden = false;
      const next = new Map(), els = []; let built = false;
      for (const p of r.pieces) {
        const k = keyOf(p), old = cards.get(k);
        // (a piece being approved keeps the card it has until the approval stands)
        if (old && old.el.isConnected && (busy.has(k) || (!force && old.sig === sigs.get(k)))) { next.set(k, old); els.push(old.el); continue; }
        const el = mkCard(p); built = true; next.set(k, { el, sig: sigs.get(k) }); els.push(el);
      }
      const same = els.length === host.children.length && els.every((e, i) => host.children[i] === e);
      if (!same) host.replaceChildren(...els);
      cards.clear(); for (const [k, v] of next) cards.set(k, v);
      for (const p of r.pieces) for (const j of p.jobs) seen.add(j.key);
      if (built || !same) { tryDo(() => root.Seal && root.Seal.fitGroups && root.Seal.fitGroups(host)); tryDo(() => C.repaint && C.repaint(host)); }   // (the live previews are scaled to the box they find: the cards are in the page now)
      // (kindsSeen is set BEFORE the host is told: told() draws the order window again, which calls update() -> refresh() while this call is still on the stack)
      const first = told0 === null, was = kindsSeen; told0 = kinds; kindsSeen = kinds;
      if (!first && !quiet && kinds !== was) told();
    }
    const id0 = ++nextId;
    function reset() {
      busy.clear(); seen.clear(); fails.clear(); last = new Map(); cards.clear(); waitEl = null; loading = false; told0 = null; kindsSeen = ''; seenAt = Date.now();
      host.replaceChildren(); host.hidden = true;
      saidSig = ''; if (saidEl) { saidEl.innerHTML = ''; saidEl.hidden = true; }
    }
    const handle = {
      el: host, mode,
      update(next) {
        if (gone) return;
        const was = ident(ctx); ctx = Object.assign({}, next);
        if (ident(ctx) !== was) { reset(); refresh(true, true); } else refresh(false);
      },
      refresh(force) { refresh(force); },
      destroy() {
        if (gone) return; gone = true; live.delete(handle); disarm();
        if (ro) { tryDo(() => ro.disconnect()); ro = null; }
        host.replaceChildren(); host.hidden = true; if (host._orderEngraving === handle) delete host._orderEngraving;
        if (saidEl) { tryDo(() => saidEl.remove()); saidEl = null; }
      },
      _tick() {
        // (a host taken out of the page takes the cards with it; one not put in yet is waited for)
        if (!host.isConnected) return attached ? handle.destroy() : undefined;
        if (!visible()) return;
        refresh(false);
      }
    };
    // (cards drawn while their window was still closed are fitted when the window shows: the seals are fitted to a box with a size)
    if (root.ResizeObserver) ro = tryDo(() => { const o = new root.ResizeObserver(() => { if (!gone && visible()) { if (told0 === null) refresh(true, true); else tryDo(() => root.Seal && root.Seal.fitGroups && root.Seal.fitGroups(host)); } }); o.observe(host); if (host.parentElement) o.observe(host.parentElement); return o; }, null);
    host._orderEngraving = handle; live.add(handle); arm();
    reset(); refresh(true, true);
    return handle;
  }

  /** One entry for each PIECE of a line (an ear, a disc), whatever its faces: the entries Engrave.piecesOf gives are one per job, so a piece with a Front and a Back would be listed twice. The entry of a piece
   *  is its job still to settle (else its first), so the card opens on the face that needs the person. A list with no front is returned as it came. */
  function foldSlots(ps) {
    if (!Array.isArray(ps)) return ps;
    const E = root.Engrave, open = p => { const j = p && E && E.items ? tryDo(() => E.items().get(p.key), null) : null; return !!j && OPEN_STATES.includes(j.state); };
    const by = new Map(); let faced = false;
    for (const p of ps) { const k = p.slot || parseKey(p.key).slot || ''; if (parseKey(p.key).face === 'front') faced = true; (by.has(k) ? by.get(k) : by.set(k, []).get(k)).push(p); }
    if (!faced) return ps;
    return [...by.values()].map(list => list.find(open) || list[0]);
  }
  /** The way to Engrave while EngraveLink is not here: the Sheet tab's own (close the window, open that order's engraving). */
  async function fallbackOpen(r, t) {
    const E = root.Engrave; if (!E || !E.restoreView) throw new Error('Engraving is not ready');
    try { if (root.OrderWin && root.OrderWin.close) await root.OrderWin.close(); } catch (_) {}
    const job = r.job, done = DONE.concat(['skipped']).includes(job && job.state) || (r.eng && r.eng.kind === 'approved');
    E.restoreView(Object.assign({}, E.view(), { tab: done ? 'done' : 'place', focus: done ? null : (job && job.key) || t.key, list: false, chosen: true, q: done ? t.rid : '' }));
    if (root.CN && root.CN.setMode) root.CN.setMode('engrave');
    E.render();
  }
  /** The one door: the cards of an order's pieces. */
  const mount = (host, ctx0) => mountCards(host, ctx0);

  root.OrderEngraving = {
    mount,
    unmount: host => { if (host && host._orderEngraving) host._orderEngraving.destroy(); },
    engOf: ctx => engOf(ctx || {}, 0),
    cycleOf, discLines, faceCell, foldSlots, bothFor, approveFaces, backOf,
    said: { of: saidOf, rows: saidRows, html: saidHtml },   // (the customer's words block as data: saidOf(rows, pieces) -> [{ name, rows: [[label, text]], faces: [['Front', w], ['Back', w]] | null }])
    version: 5
  };
})(typeof window !== 'undefined' ? window : globalThis);
