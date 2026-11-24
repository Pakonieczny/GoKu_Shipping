/* charm-nest-engrave-classify.js — THE READING OF A LINE'S ENGRAVING WORDS, one copy for the page and the cloud (cloud plan v2, build step 11, C3).
 *
 * What was the bridge's (charm-nest-bridge.js Engrave: classifyOnce and what it reads with: raiseCandidate, the rules' turn (readByRules, SETBAR-C), the faces' reading
 * (classifyFaces), the pieces' and the discs' own words (PIECES, DISCREAD), the free reading of a line not pooled yet (ENGPRE freeWords) and the paid reader's answer
 * (settleReading)) is here, word for word; the page builds it over its own objects (Engrave's job map, ensureJobs, setReady / toWords / setNone, agentCall, the log), so
 * the page reads exactly as it always did. The cloud builds it over a line's records (Charm_World_Lines, Charm_Nest_Engrave: netlify/functions/_charmEngraveClassify.js),
 * so a line is read with no computer open: the rules first, the paid reader (Claude, through the cloud-only op engraveIntentStart) only when the rules cannot settle it.
 *
 *   make(env) → { classifyOnce, raiseCandidate, readByRules, settleReading, freeWords, classifyFaces, facedSetup, unsplitDefault, seedOf, takeAsks, askWhy, personalRead, ... }
 *   env (every member a function, read when used, never at load):
 *     items()                 the job map (key → job); a reading whose map was replaced meanwhile writes nothing more
 *     ensureJobs(row)         the line's jobs, made when missing
 *     setNone(job, why) · toWords(job, why) · setReady(job[, wake])   the three ends of a reading
 *     needEvent(job)          the order's timeline line "Back engraving: …" (once per line and words)
 *     agent(meta, kind, text, extra)   the log
 *     agentCall(mode, ask, opts)       the paid reader: { engrave, text, source, ... } | { skipped } | { error }; the cloud may answer { pending: true } (its reading is under way:
 *                                      the jobs stay "classify" and the line is read again later, the reading taken up again, never paid for twice)
 *     FACES() · READER() · SIDES() · ASKS() · O() · seals()   the shared modules (CharmNestEngraveFaces, CharmNestEngraveRead, CharmNestEngraveSides, CharmNestEngraveAsks,
 *                                      CharmNestOrders, CNEngravingSeals); any may be null (the page's own guards stay)
 *     neverEngraves(row) · neverWhy(row) · NEVER_OPEN(job)   ZODNOBACK
 *     faceOf(x)               a job's face ("front" | "back")
 *     trust()                 Settings' engraving confidence (S.settings.engraveConfidence)
 *     customSent(row)         a line sent to a custom sheet (CustomSheet.sentOf)
 *     finished(row)           a line that is over (CharmNestOrders.FINISHED_LINE)
 *     jobsOf(row) · orderRows(row)   (ENGNOTE, optional) a line's jobs and the lines of its order; on the page, Engrave.jobsOf and Orders.rows are read when they are not given
 *   ENGNOTE (Paul, 20 Nov 2026, "Yes, use ENG: notes"): a staff message "ENG: <words>" gives the engraving words of the order's one line that takes them (engReading, giveEng); a new or changed
 *   note reads again the lines that wait for a person (engFollow, asked by raiseCandidate on every read of a line).
 *   Pure apart from env: no network, no storage, no DOM. Loads in the page (window.CharmNestEngraveClassify) and in node (module.exports). */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestEngraveClassify = api;
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const OPEN = ['classify', 'reclassify'];
  /** The paid reading's key: made from the question with the words as Etsy gave them (ENGENT), so a reading asked before is taken up again, never paid for twice. */
  function askKey(ask, raw) {
    let key = 2166136261; for (const ch of JSON.stringify(Object.assign({}, ask, raw))) key = Math.imul(key ^ ch.charCodeAt(0), 16777619);
    return (key >>> 0).toString(36);
  }
  /* ENGNOTE (Paul, 20 Nov 2026: "Yes, use ENG: notes"): a staff message on the order whose text starts "ENG:" (any case, spaces allowed around the colon) gives the engraving words: the rest of it,
     trimmed, in the writer's own spelling, case and line breaks, with the degree signs and symbols put right as for the buyer's words (fixDegrees, fixSymbols). The latest one (by its time) wins.
     → { text, lines, by, at, raw } or null. Pure. */
  const ENG_RE = /^\s*eng\s*:\s*/i;
  function engNoteOf(messages, R) {
    let best = null;
    for (const m of Array.isArray(messages) ? messages : []) {
      const t = String((m && m.text) || ''); if (!ENG_RE.test(t)) continue;
      const at = Number.isFinite(+m.at) ? +m.at : 0;
      if (best && at < best.at) continue;   // (the latest wins; two at the same time: the later in the list)
      best = { raw: t, at, by: String((m && (m.senderName || m.by)) || '').trim() || null };
    }
    if (!best) return null;
    let text = best.raw.replace(ENG_RE, '').split(/\r?\n/).map(x => x.trim()).filter(Boolean).join('\n');
    if (!text) return null;
    if (R) { try { const fd = R.fixDegrees(text); if (fd && fd.count) text = fd.text; const fs = R.fixSymbols ? R.fixSymbols(text) : null; if (fs && fs.count) text = fs.text; } catch (_) { /* the words as written */ } }
    return { text, lines: text.split('\n'), by: best.by, at: best.at, raw: best.raw };
  }
  /* ENGNOTE · which line of the order the note's words are for. h: { rows (the order's lines), jobsOf(row), READER(), neverEngraves(row), finished(row), customSent(row) }.
     The note's line is chosen among ALL the order's lines that take engraving (one to engrave, or whose option says ENGRAVED; never an earring, a BLANK line, a custom / add-on / no-design line, one
     that is over), a line a person decided or approved included, so a note never jumps to another line: the line already holding this very note's words; else exactly one line with the explicit
     engraving option (decide() token ENGRAVED, "CHARM + Engraving"); else the order's only engraving line. That line takes the words only while no person decided it (mine); when a person did,
     it is left as it is and no other line takes them (decided: the card says a newer note differs). Several lines and none explicit: each undecided one is shown the words to confirm (suggest).
     → { note, mine, decided, suggest, target } or null (no ENG note). Pure. */
  const lineOfRow = r => (r && r.parentRow) || r;
  function engPick(line, h) {
    line = lineOfRow(line); const sp = line && line.spec; if (!sp) return null;
    const R = h && h.READER ? h.READER() : null;
    const note = engNoteOf(sp.messages || (line.order && line.order.messages), R); if (!note) return null;
    const jobsOf = r => { try { return ((h.jobsOf && h.jobsOf(r)) || []).filter(j => j && !j.archived); } catch (_) { return []; } };
    const byKey = new Map(); for (const r of (h && h.rows) || []) { const l = lineOfRow(r); if (l && l.key) byKey.set(l.key, l); } byKey.set(line.key, line);
    const rows = [...byKey.values()];
    const tokenOf = r => { if (!R) return ''; try { return R.decide({ options: ((r.line && r.line.variations) || []).map(v => ({ name: v.name || v.formatted_name || '', value: v.value || v.formatted_value || '' })), words: false, note: false }).token || ''; } catch (_) { return ''; } };
    const takes = r => {
      const s = r && r.spec; if (!s || !s.designSku || s.noDesign || s.ownDesign || (s.special && (s.special.notCut || s.special.own))) return false;
      if (r.state === 'gone' || (h.finished && h.finished(r)) || (h.neverEngraves && h.neverEngraves(r)) || (h.customSent && h.customSent(r))) return false;
      const tok = tokenOf(r); if (tok === 'BLANK') return false;   // (BLANK: no engraving, whatever a note says)
      return !!s.engraveCandidate || tok === 'ENGRAVED';
    };
    const decided = r => {
      if (r.engravingOverride) return true;   // (a person's own setup of the line)
      if (jobsOf(r).some(j => (j.decision && (j.decision.by || j.decision.none)) || ['approved', 'written', 'skipped'].includes(j.state) || +j.approvedAt > 0 || j.approvalIntent)) return true;
      const e = r.engrave; return !!(e && e.approved && (e.approvedBy || (Array.isArray(e.seals) && e.seals.length)));   // (a line cut plain or approved by a person, its jobs not at hand)
    };
    const of = t => { const me = !!t && t.key === line.key, dec = me && decided(line); return { note, mine: me && !dec, decided: dec, suggest: false, target: t ? t.key : null }; };
    const holder = rows.find(r => jobsOf(r).some(j => j.wordsSource === 'staffNote:eng' && j.quote === note.raw));
    if (holder) return of(holder);
    const all = rows.filter(takes);
    if (!all.some(r => r.key === line.key)) return { note, mine: false, decided: false, suggest: false, target: null };
    const explicit = all.filter(r => tokenOf(r) === 'ENGRAVED');
    const target = explicit.length === 1 ? explicit[0] : !explicit.length && all.length === 1 ? all[0] : null;
    if (target) return of(target);
    return { note, mine: false, decided: false, suggest: !decided(line), target: null, cands: all.map(r => r.key) };
  }
  /** The words a person-decided line's card shows when a NEWER note gives other words (the decision is kept, never changed): "" when there is nothing to say. Same h as engPick. */
  function engNotice(line, h) {
    line = lineOfRow(line); const p = engPick(line, h); if (!p || !p.decided) return '';
    const js = ((h.jobsOf && h.jobsOf(line)) || []).filter(j => j && !j.archived && ((j.decision && (j.decision.by || j.decision.none)) || ['approved', 'written', 'skipped', 'none'].includes(j.state)));
    const norm = t => String(t == null ? '' : t).replace(/\s+/g, ' ').trim(), words = js.map(j => norm(j.text)).filter(Boolean), e = line.engrave || {};
    if (!words.length && e.text) words.push(norm(e.text));
    if (words.length && words.every(w => w === norm(p.note.text))) return '';   // (the decision is the note's words)
    const at = Math.max(0, ...js.map(j => +j.approvedAt || +j.decidedAt || +(j.decision && j.decision.at) || 0), +e.approvedAt || 0, +e.decidedAt || 0);
    if (at && p.note.at && p.note.at <= at) return '';   // (the person decided after the note)
    const cut = js.length ? js.every(j => ['skipped', 'none'].includes(j.state)) : !e.needed;
    return `Newer staff note by ${p.note.by || 'a staff member'}: “${p.note.text.replace(/\n/g, ' / ').slice(0, 160)}” — ${cut ? 'the decision (no engraving) was kept' : 'the approved words were kept'}`;
  }

  function make(env) {
    const E = env || {};
    const items = () => E.items();
    const ensureJobs = row => E.ensureJobs(row);
    const setNone = (j, why) => E.setNone(j, why);
    const toWords = (j, why) => E.toWords(j, why);
    const setReady = (...a) => E.setReady(...a);
    const needEvent = j => (E.needEvent ? E.needEvent(j) : undefined);
    const agent = (...a) => (E.agent ? E.agent(...a) : undefined);
    const agentCall = (...a) => E.agentCall(...a);
    const FACES = () => (E.FACES ? E.FACES() : null) || null;
    const READER = () => (E.READER ? E.READER() : null) || null;
    const SIDES = () => (E.SIDES ? E.SIDES() : null) || null;
    const ASKS = () => (E.ASKS ? E.ASKS() : null) || null;
    const Ord = () => (E.O ? E.O() : null) || null;
    const SEALS = () => E.seals();
    const neverEngraves = row => E.neverEngraves(row);
    const neverWhy = row => E.neverWhy(row);
    const NEVER_OPEN = j => E.NEVER_OPEN(j);
    const faceOf = x => E.faceOf(x);
    const isFront = x => faceOf(x) === 'front';
    const lineRowOf = row => (row && row.parentRow) || row;

    /* FOLLOWASK (Paul, 15 Nov 2026): "the engraving ... needs to also be smart and interpreted so that the system doesn't conflate user instructions with the actual things they want on the charm". The words lose the
       instruction text ("on the left side please", "all caps", a web link); the asks stay on the job (job.asks) for applyAsks after the fit. Rule-based, no model. Returns the lines to engrave; job.askUnsure when a person must look. */
    function takeAsks(job, lines) {
      const A = ASKS(); job.askUnsure = false; delete job.applied;
      if (!A || !lines || !lines.length) return lines;
      let r; try { r = A.asksOf(lines.join("\n")); } catch (_) { return lines; }
      job.asks = r.asks.length ? r.asks : undefined; job.askUnsure = !!r.unsure;
      const out = r.words.split("\n").map(x => x.trim()).filter(Boolean);
      if (r.asks.some(a => a.type === "reference")) job.askRef = true; else delete job.askRef;
      return out;
    }
    const askWhy = job => job.askUnsure ? "Check the requested inscription: it may be an instruction" : job.askRef ? "A web link was sent with the order: it is not engraved. Say what goes on the charm" : null;
    /** The line's personalisation read by the rules (null: no reader, or nothing written). */
    function personalRead(row) {
      const R = READER(), line = lineRowOf(row), sp = line && line.spec; if (!R || !sp || !(sp.personalization || []).length) return null;
      try { return R.readPersonalisation(sp.personalization, { lengths: R.lengthsOfLine(line.line) }); } catch (_) { return null; }
    }
    /** Nobody else has spoken about the engraving (a buyer message, a staff note, an engraving-like message): the rules read the personalisation field alone only then, as the disc reading does. */
    function spokeAlone(sp) { const O = Ord(); return !String(sp.buyerMessage || "").trim() && !String(sp.staffNote || "").trim() && !(sp.messages || []).some(m => (O && O.engravingNote ? O.engravingNote(m && m.text) : true)); }
    /** A different layout of the set asked for by the buyer (the personalisation or the message): the clause, or "" (a set only: a line of more than one piece, or one that names two chain lengths). */
    function layoutAsked(row, jobs) {
      const R = READER(), line = lineRowOf(row), sp = line && line.spec; if (!R || !sp) return "";
      let lengths = []; try { lengths = R.lengthsOfLine(line.line); } catch (_) { lengths = []; }
      if (!(jobs.length > 1 || lengths.length > 1)) return "";
      return R.layoutClause((sp.personalization || []).join("\n")) || R.layoutClause(sp.buyerMessage || "");
    }
    /** Give one job the words the rules read (or none), and put it to a person: "Words to confirm". */
    function giveRead(row, j, words, why, how, confidence) {
      const sp = row.spec, lines = takeAsks(j, String(words || "").split(/\r?\n/).map(x => x.trim()).filter(Boolean));   // (FOLLOWASK: an instruction in the words is an ask, not engraving)
      if (askWhy(j)) { why = askWhy(j); confidence = 0; }
      j.lineInput = null; j.lineMode = "auto"; j.wantSize = null; j.decision = null;
      Object.assign(j, { text: lines.join("\n"), lines, source: "personalization", quote: (sp.personalization || []).join(" / ") || null, confidence: lines.length ? confidence || 0 : 0, questions: [why], claudeReasoning: null, requests: { side: faceOf(j), font: null, handwriting: false, image: false }, wordsSource: "personalization:" + how });   // (the reading stays on the card as its note once the preview is drawn and the reason line is gone, as the faces reading's own reason does)
      toWords(j, why);
      if (!j.pre) needEvent(j);
      return j;
    }
    /** The rules' turn at a line, before the disc reading and the paid reader. true: the line is settled (its jobs wait for a person to confirm), false: go on as before. */
    async function readByRules(row, jobs) {
      const R = READER(); if (!R) return false;
      const sp = row.spec, owner = items(), say = (a, b) => `${a}${b ? " — " + b : ""}`;
      const stop = () => items() !== owner || row.state === "gone";
      const all = jobs; jobs = jobs.filter(j => OPEN.includes(j.state) && owner.get(j.key) === j);   // (only the jobs still to be read: one a person decided or approved is never touched)
      if (!jobs.length) return false;
      // 1. a different layout asked for: never guessed, a person decides which words go on which bar (the per-length wordings stay beside it)
      const layout = layoutAsked(row, jobs), rd = personalRead(row);
      if (layout) {
        const why = say(`The buyer asks for a different layout: “${layout.slice(0, 160)}”. A person decides which words go on which piece`, rd && rd.labelled ? "the buyer wrote " + R.wordings(rd) : "");
        for (const j of jobs) { giveRead(row, j, "", why, "layout"); if (stop()) return true; }
        agent({ engrave: true }, "ENGRAVE", `${row.order.receiptId} · ${sp.designSku}: the buyer asks for a different layout (“${layout.slice(0, 80)}”): waits for a person`);
        return true;
      }
      // 2. the yes / no / ask of the line from its options (ENGRAVED / BLANK) and what was written: a BLANK line with a note that asks for engraving, an ENGRAVED line with nothing written
      const line = lineRowOf(row), dec = R.decide({ options: ((line.line && line.line.variations) || []).map(v => ({ name: v.name || v.formatted_name || "", value: v.value || v.formatted_value || "" })), words: (sp.personalization || []).length > 0,
        note: !!(String(sp.buyerMessage || "").trim() && R.noteAsksEngraving(sp.buyerMessage)) || !!(String(sp.staffNote || "").trim() && R.noteAsksEngraving(sp.staffNote)), anyNote: !spokeAlone(sp) });
      if (dec.engrave === "ask") {   // (BLANK with words or a note that asks for engraving, ENGRAVED with nothing written, two options that disagree: nothing is engraved until a person decides, and the card says why)
        const has = (sp.personalization || []).length > 0, lab = !!(rd && rd.labelled);
        const proposal = has && !lab ? (rd && !rd.unsure ? rd.whole : sp.personalization.join("\n")) : "";
        const why = say(dec.why.charAt(0).toUpperCase() + dec.why.slice(1), lab ? R.say(rd) : rd && !rd.unsure && rd.changed ? R.say(rd) : "");
        for (const j of jobs) { giveRead(row, j, proposal, why, "ask"); if (stop()) return true; }
        agent({ engrave: true }, "ENGRAVE", `${row.order.receiptId} · ${sp.designSku}: ${dec.why}`);
        return true;
      }
      // 3. the words themselves
      if (!rd || rd.unsure || !spokeAlone(sp)) return false;
      if (rd.labelled) {
        const isSet = (jobs.length > 1 && !copiesOnly(jobs)) || rd.lengths.length > 1;   // (PIECES: two copies of one charm are not a set of two pieces: one label, its words on each copy)
        if (!isSet && rd.pieces.length === 1) {   // one necklace, one label: its words are the line's
          for (const j of jobs) { giveRead(row, j, rd.pieces[0].words, say(R.say(rd), "confirm the words"), "labelled", 0.9); if (stop()) return true; }
          return true;
        }
        const asg = R.assign(rd, R.slotsOf(row, all)), note = R.say(rd);
        const usable = asg.unmatched.length === 0 && Object.keys(asg.bySlot).length > 0;   // (every wording found its piece; a piece with no wording of its own is asked, never given another's)
        const got = Object.values(asg.bySlot).map(x => x.label).join(" and ");
        for (const j of jobs) {
          const mine = usable && j.slot ? asg.bySlot[j.slot] : null, where = jobs.length > 1 && j.slot ? " (" + (SIDES().earOf(j.slot) || j.slot) + ")" : "";
          giveRead(row, j, mine ? mine.words : "", mine ? say(note, `${mine.label} piece${where}: confirm the words`) : usable ? say(`The buyer wrote words for ${got} only: say what goes on this piece${where}, or no engraving`, note) : say(`Say which words go on this piece${where}`, note), mine ? "labelled" : "labelled-open", mine ? 0.9 : 0);
          if (stop()) return true;
        }
        agent({ engrave: true }, "ENGRAVE", `${row.order.receiptId} · ${sp.designSku}: ${R.say(rd)}${usable ? "" : " — which piece gets which is for a person"}, no paid reading`);
        return true;
      }
      if (rd.changed && jobs.length === 1) {   // the words with the notes taken out and the degree signs put right
        giveRead(row, jobs[0], rd.whole, say(R.say(rd), "confirm the words"), "cleaned", 0.9);
        agent({ engrave: true }, "ENGRAVE", `${row.order.receiptId} · ${sp.designSku}: ${R.say(rd)}, no paid reading`);
        return true;
      }
      return false;
    }
    const jobFace = j => (j && (j.face === "front" || /~front$/.test(String(j.key))) ? "front" : "back");   // (the face of a job, in a line: the Faces module is the one reader, this is the same rule for the code below)
    /** Are all these jobs copies of one thing (slots "C1".."CN")? Then they are not a set of different pieces. */
    const copiesOnly = jobs => { const S = SIDES(); return !!S && jobs.length > 0 && jobs.every(j => j.slot && S.isCopy(j.slot)); };
    /** PIECES: the words of each piece of a line cut by unit (quantity >= 2), from the personalisation field alone, when the note says plainly which words go on which piece (charm-nest-engrave-sides.js wordsForPieces), else null:
     *  then every piece gets the one reading of the line. Never for a line of quantity 1 (the disc reading below handles those). */
    function pieceWordsOf(row, jobs) {
      const S = SIDES(); if (!S || !S.wordsForPieces) return null;
      const mine = jobs.filter(j => jobFace(j) !== "front" && j.slot); if (mine.length < 2 || !mine.some(j => S.isCopy(j.slot) || S.unitOf(j.slot) > 1)) return null;
      const O = Ord();
      return S.wordsForPieces(row.spec || {}, mine.map(j => j.slot), { engravingNote: O ? O.engravingNote : null });
    }
    /** Each piece of a line cut by unit takes ITS words from the note (pieceWordsOf). A note that numbers the pieces, has one entry for each or says "the same on all" is ready for its fit; a plain list ("Mom & Dad") is a proposal for a person to confirm. */
    async function giveUnitWords(row, jobs, split) {
      const sp = row.spec, owner = items(), S = SIDES();
      for (const j of jobs) {
        if (!OPEN.includes(j.state) || items().get(j.key) !== j || !j.slot || jobFace(j) === "front") continue;
        const lines = String(split.words[j.slot] || "").split(/\r?\n/).map(x => x.trim()).filter(Boolean); if (!lines.length) continue;
        j.lineInput = null; j.lineMode = "auto"; j.wantSize = null; j.decision = null;
        Object.assign(j, { text: lines.join("\n"), lines, source: "personalization", quote: (sp.personalization || []).join(" / "), confidence: split.sure ? 1 : 0, questions: [], claudeReasoning: null, requests: { side: jobFace(j), font: null, handwriting: false, image: false }, wordsSource: "personalization:" + split.how, wordsHow: "pieces" });
        agent({ engrave: true }, "ENGRAVE", `${row.order.receiptId} · ${sp.designSku} · ${S.slotWord(j)}: "${lines.join(" / ")}" read from the note (${split.how}), no paid reading`);
        if (split.sure) await setReady(j); else toWords(j, `The note may give each piece its own words: confirm the words of ${S.slotWord(j) || "this piece"}`);
        if (items() !== owner || row.state === "gone") return;
      }
    }
    /** The words of each disc of a counted line, from its personalisation field alone (charm-nest-engrave-sides.js splitWords), or null when the note does not say plainly. */
    function discWordsOf(row, jobs) {
      const S = SIDES(); if (!S || !S.wordsForDiscs) return null;
      const O = Ord();
      return S.wordsForDiscs(row.spec || {}, jobs.map(j => j.slot), { engravingNote: O ? O.engravingNote : null });
    }
    /** The line's engraving setup when the line is read face by face (a front job, a front in the setup, or a person's own back words); null for a line that engraves the back as it always did (the reader decides). */
    function facedSetup(row, jobs) {
      const F = FACES(); if (!F) return null;
      const setup = F.setupOf(row);
      // (a reading the intake marked for a person goes to the cards for a person too: the paid reader never decides it)
      // (BFSIDES: a back line whose words the intake itself read, "2 sides" on a design with its own front art, is read from the setup too: no paid reading, nothing guessed)
      const ownPieces = !!(setup.edited && setup.pieces && row.engravingOverride && row.engravingOverride.pieces);   // (a person gave a piece its own words)
      return jobs.some(isFront) || setup.faces.includes("front") || (setup.faces.includes("back") && (setup.back || ownPieces) && (setup.edited || setup.sides)) || setup.needsPerson ? setup : null;
    }
    /** SETWORDS: the intake's own default-face reading (a bar's front, a listing's face: spec.engraving source "profile") of a line of several pieces that could not give each piece its words, with no person's word on it and
     *  no listing profile of its own (PIECE_LISTINGS reads its pieces itself): its words are read by the rules (readByRules) and the usual path, its face stays the one the rule gave. */
    function unsplitDefault(row, jobs, setup) {
      const line = lineRowOf(row), sp = line && line.spec, e = sp && sp.engraving;
      if (!e || e.source !== "profile" || setup.edited || !(jobs.length > 1) || row.engravingOverride || (line && line.engravingOverride)) return false;
      const O = Ord();
      try { if (O.pieceListing && O.pieceListing(line.line)) return false; } catch (_) { return false; }
      if (e.needsPerson && !e.pieces) return true;
      // (a SET, a line that names two chain lengths, whose words carry chain-length or Left / Right labels: the splitter gave them out in order, but no bar of a set is "left" and a length belongs to its chain: the rules decide, as before)
      const R = READER(), rd = personalRead(row); let lens = []; try { lens = R ? R.lengthsOfLine(line.line) : []; } catch (_) { lens = []; }
      return lens.length > 1 && !!(rd && rd.labelled && rd.pieces.some(p => p.kind === "length" || p.kind === "side"));
    }
    /** engraveCandidate is the intake's word about WORDS ("something is written to engrave"). A line the buyer asked to engrave on a face without writing any (the intake's reading: needsPerson, "nowords"), or
     *  that a person set up with faces, is raised here so a person is asked rather than the line being cut plain. Custom, add-on and no-design lines are never raised. In memory only: it is read again on every pull. */
    function raiseCandidate(row) {
      const raised = raiseOnce(row);
      try { engFollow(row); } catch (_) { /* a note is followed at the next look */ }   // (ENGNOTE: every read of a line asks this, on the page (Orders.interpretAll) as in the cloud)
      return raised;
    }
    function raiseOnce(row) {
      row = (row && row.parentRow) || row; const sp = row && row.spec; if (!sp) return false; if (neverEngraves(row)) return false; if (sp.engraveCandidate) return true;   // (an earring is never raised: ZODNOBACK)
      const F = FACES(); if (!F || !sp.designSku || sp.noDesign || sp.ownDesign || (sp.special && (sp.special.notCut || sp.special.own)) || (E.customSent && E.customSent(row))) return false;
      // SETBAR-C: the option says ENGRAVED (listing 532401548 "GOLD·Set·ENGRAVED") and the buyer wrote nothing anywhere: the buyer paid for engraving, so a person is asked what to engrave (it was cut plain, in silence)
      { const R = READER(); if (R && row.state !== "gone" && !(E.finished && E.finished(row)) && !(row.engrave && !OPEN.includes(row.engrave.state))) { let d = null; try { d = R.decide({ options: ((row.line && row.line.variations) || []).map(v => ({ name: v.name || v.formatted_name || "", value: v.value || v.formatted_value || "" })), words: false, note: false }); } catch (_) { d = null; }
        if (d && d.token === "ENGRAVED" && d.engrave === "ask") { sp.engraveCandidate = true; sp.engraveRaised = true; return true; } } }
      let st; try { st = F.setupOf(row); } catch (_) { return false; }
      if (!st.faces.length || !(st.needsPerson || st.edited)) return false;
      sp.engraveCandidate = true; sp.engraveRaised = true; return true;
    }
    /** What a face job starts with from the setup, or null. A back line the intake read as "back" with nothing a person said keeps the old reader: no seed. */
    function seedOf(setup, job) {
      const F = FACES(); if (!F || !setup) return null;
      if (setup.mode === "back" && !setup.edited && !(setup.sides && setup.back)) return null;   // (a back line the intake read as "back" with no words of its own keeps the reader; "2 sides" on a design with its own front art carries the back's words)
      return F.seedFor(setup, faceOf(job), job.slot || null);   // (the ear's own words when the buyer gave different words for each ear or disc)
    }
    /** The words of each face job of a line that has a front or a person's own words: from the setup, no paid reading, nothing guessed. A face with no words goes to a person with the buyer's own words
     *  beside it. Only the jobs still to be read are touched: a face already decided stays as it is. */
    async function classifyFaces(row, jobs, setup) {
      const sp = row.spec, owner = items(), job = jobs[0], F = FACES();
      const todo = jobs.filter(j => OPEN.includes(j.state) && items().get(j.key) === j);
      const quote = (sp.personalization || []).join(" / ") || setup.raw || null;   // (words only in a note: the intake keeps them as `raw`, so the card shows what the buyer wrote)
      // (a person's words for ONE piece, engravingOverride.pieces[slot]: that job is read from its own piece's setup, so it needs no one while a sibling piece still does)
      const pieceSetup = j => (j.slot && row.engravingOverride && row.engravingOverride.pieces && F ? F.setupOf(lineRowOf(row), undefined, j.slot) : setup);
      for (const j of todo) {
        const face = faceOf(j), sj = pieceSetup(j), seed = seedOf(sj, j);
        j.state = "classify"; j.row.engrave = { needed: false, state: "classify", seals: SEALS().merge(j.row.engrave, j) };
        j.lineInput = null; j.lineMode = "auto"; j.wantSize = null; j.decision = null;
        if (seed && !sj.needsPerson) {
          delete j.seededFrom;
          Object.assign(j, { text: seed.text, lines: seed.lines.slice(), source: seed.source, quote, confidence: seed.confidence, questions: [], claudeReasoning: null, requests: { side: face, font: seed.fontWord || null, handwriting: false, image: false }, wordsSource: "spec.engraving", faceFontWord: seed.fontWord || null, ...(seed.how === "single" ? { wordsHow: "single" } : {}) });
          agent({ engrave: true }, "ENGRAVE", `${row.order.receiptId} · ${sp.designSku}${j.slot ? " · " + SIDES().slotWord(j) : ""} · ${face}: "${seed.text.replace(/\n/g, " / ")}" taken from ${seed.how === "person" ? "the person's setup" : "the order"} (${setup.why || setup.mode}), no paid reading`);
          await setReady(j);
        } else {
          // a face with no words, or a reading the intake marked for a person: the card asks (with the intake's own reason), the words it found stand as the proposal, nothing is approved and nothing is paid for
          const why0 = sj.needsPerson ? (sj.why || "The note does not say which words go on the front and which on the back") : `The order gives no words for the ${face}`;
          const rdNone = !seed && setup.mode === "none" ? personalRead(row) : null;   // (SETBAR-C: a BLANK line that has words proposes the words read by the rules, never the labelled wordings of two pieces glued together)
          const proposal = seed ? seed.text : setup.mode === "none" ? (rdNone && rdNone.labelled && !rdNone.unsure ? "" : rdNone && rdNone.changed && !rdNone.unsure && !rdNone.labelled ? rdNone.whole : (sp.personalization || []).join("\n")) : "", lines = proposal.split(/\r?\n/).map(x => x.trim()).filter(Boolean);
          const why = why0 + (rdNone && rdNone.changed && !rdNone.unsure && READER() ? " — " + READER().say(rdNone) : "");
          Object.assign(j, { text: lines.join("\n"), lines, source: "personalization", quote, confidence: 0, questions: [why], claudeReasoning: null, requests: { side: face, font: seed && seed.fontWord || null, handwriting: false, image: false }, wordsSource: "spec.engraving", faceFontWord: seed && seed.fontWord || null });
          toWords(j, why);
        }
        if (items() !== owner || row.state === "gone") return job;
        if (!j.pre) needEvent(j);   // (ENGPRE: a line read before it is pooled writes nothing to the order's timeline: its edit and its approval do)
      }
      return job;
    }
    /** The question the paid reader is asked about a line (built from the line's records only: the order's words, the design, the slots and chains; never a picture), and its key. */
    function intentAsk(row, jobs, engravable) {
      const sp = row.spec, S = SIDES();
      // (a line cut into slots names them in the question, so the model says which words go on which ear or disc; any other line asks exactly as before)
      const slots = jobs.length > 1 ? [...new Set(jobs.map(j => j.slot).filter(Boolean).filter(sl => !S.isCopy(sl)).map(sl => S.baseSlot(sl)))] : null;   // (PIECES: the reader is asked once, about the ears / discs of ONE unit; a copy of a charm is no slot to it)
      // (SETBAR-C: when the chain each piece hangs on is known, the reader is told, so a label "16\"= ..." lands on the right slot; unknown: not sent, the request is byte for byte what it was)
      const chains = (() => { const R = READER(); if (!R || !slots || slots.length < 2) return null; const sl = R.slotsOf(row, jobs); return sl && sl.every(x => x.length != null) ? Object.fromEntries(sl.map(x => [x.slot, x.length])) : null; })();
      const ask = { order: row.order.receiptId, sku: sp.designSku, title: row.line.title, form: sp.form, quantity: sp.quantity, engravable, personalization: sp.personalization, buyerMessage: sp.buyerMessage, staffNote: sp.staffNote, messages: sp.messages, ...(slots && slots.length > 1 ? { slots } : {}), ...(chains ? { pieceLengths: chains } : {}) };
      // (ENGENT: the question carries the words decoded; its key is made from the words as Etsy gave them, as it always was, so a reading already asked for before the words were decoded is taken up again, never paid for twice)
      return { ask, key: askKey(ask, Ord().rawWords(row.order, row.line)) };
    }
    async function classifyOnce(row, opts) {
      const jobs = ensureJobs(row), job = jobs[0]; const sp = row.spec; const owner = items();
      for (const j of jobs) SEALS().keep(j);
      // NO EARRING IS EVER ENGRAVED (ZODNOBACK): nothing is read, nothing is paid for, nothing asks; a job with an approval on it stays as it is
      if (neverEngraves(row)) { for (const j of jobs) if (NEVER_OPEN(j) && j.state !== "none") setNone(j, neverWhy(row)); return job; }
      if (!sp.engraveCandidate && !raiseCandidate(row)) { for (const j of jobs) setNone(j, "no personalisation, message or note"); return job; }
      // a person said "no engraving" for this line: it stays so (never the paid reader again)
      const said = FACES() ? FACES().setupOf(row) : null;
      if (said && said.edited && said.mode === "none") { for (const j of jobs) setNone(j, `no engraving — decided by ${said.by || "a person"}`); return job; }
      // "1 side" on a design with its own front art (the intake's reading, no person's word against it): only the art is engraved, the buyer's words are not; the card says so, nothing is paid for or asked
      if (said && !said.edited && said.mode === "none" && said.sides && !said.needsPerson) { for (const j of jobs) setNone(j, said.why || "1 side: only the design's own art is engraved"); return job; }
      // ENGNOTE (Paul, 20 Nov 2026): a staff note "ENG: <words>" gives the words of the order's one line that takes them: ready, no paid reading; when several lines could take them, each is shown them to confirm
      { const eng = engReading(row); if (eng && (eng.mine || eng.suggest)) return giveEng(row, jobs, eng); }
      // a line with a front (or a person's own words for a face, or a reading marked for a person): each face job takes its words from the line's setup, no paid reading (charm-nest-engrave-faces.js)
      // SETWORDS (Paul, 9 Nov 2026; tests/charm-nest/engrave-read-world.cjs): BARFRONT (28 Oct) gives every bar necklace the FRONT, the set of 2 bars of listing 532401548 too. Its default reading hands a line of several pieces
      // to the pieces' splitter, which knows no chain lengths: "16\"= 33*0'45\" N … (the asterisk is …) 18\"= Ginger" waited whole, asterisks and note to the shop included, for a person to split by hand. The bar rule
      // decides the FACE; the WORDS of such a line (the intake's default reading, unsplit, no person's word, no listing profile of its own) are read here as before BARFRONT: per chain length, notes out, ° for *, BLANK / ENGRAVED, a layout asked for.
      const faceSetup = facedSetup(row, jobs), setWords = !!faceSetup && unsplitDefault(row, jobs, faceSetup);
      if (faceSetup && !setWords) return classifyFaces(row, jobs, faceSetup);
      // SETBAR-C: the buyer's words read by plain rules before any paid reader: a label per piece (16"= ... 18"= ...), a note to the shop left out, an asterisk that is a degree sign, a different layout asked for, BLANK / ENGRAVED
      if (await readByRules(row, jobs)) return job;
      // Etsy offers engraving on these designs; legacy catalog estimates are not eligibility rules.
      const engravable = !neverEngraves(row);   // (always true here: an earring never gets this far, the line above puts it away; the server answers "no engraving" for engravable:false too)
      // a line cut into slots that already holds the words (read before it was known to be a pair) asks a person which ear gets which: no new paid reading
      if (jobs.length > 1 && jobs.every(j => j.seededFrom && j.text)) {
        for (const j of jobs) { const earOf = SIDES().slotWord(j); delete j.seededFrom; j.lines = j.lines && j.lines.length ? j.lines : String(j.text).split(/\r?\n/).map(x => x.trim()).filter(Boolean); toWords(j, `Say which words go on the ${earOf ? earOf.toLowerCase() : "piece"}`); }
        return job;
      }
      // the discs of one counted line whose note says plainly which words go on which disc ("Tag 1: J, Tag 2: Q", DISCREAD): each disc takes its own words here, no paid reading;
      // anything the note leaves open (or another voice on the order: a buyer message, a staff note) goes to the reader below as before
      // PIECES: a line of quantity >= 2 whose note says plainly which words go on which piece (numbered, one entry each, "the same on all", a short list): each piece takes its own. A piece the line gained after its sibling was read
      // starts with the sibling's words for a person to confirm. Anything else is read once (below) and EVERY piece gets the whole reading.
      const unitSplit = pieceWordsOf(row, jobs);
      if (unitSplit) { await giveUnitWords(row, jobs, unitSplit); return job; }
      { const open = jobs.filter(j => OPEN.includes(j.state)), copied = open.filter(j => j.copiedFrom && j.text);
        if (copied.length && copied.length === open.length) { for (const j of copied) { delete j.seededFrom; toWords(j, "This piece was added to the order: confirm the words (copied from the piece before it)"); } return job; } }
      const discSplit = discWordsOf(row, jobs);
      if (discSplit) {
        for (const j of jobs) {
          const w = discSplit.words[+String(j.slot).slice(1) - 1];
          j.lineInput = null; j.lineMode = "auto"; j.wantSize = null; j.decision = null;
          Object.assign(j, { text: w, lines: [w], source: "personalization", quote: (sp.personalization || []).join(" / "), confidence: 1, questions: [], claudeReasoning: null, requests: { side: "back", font: null, handwriting: false, image: false }, wordsSource: "personalization:" + discSplit.how });
          agent({ engrave: true }, "ENGRAVE", `${row.order.receiptId} · ${sp.designSku} · ${SIDES().slotWord(j)}: "${w}" read from the note (${discSplit.how}), no paid reading`);
          await setReady(j);
        }
        return job;
      }
      // ENGPRE: a line read before it is pooled (opts.free) never starts the paid reader: its words are the buyer's own (freeWords), for a person to confirm and approve
      if (opts && opts.free) return freeWords(row, jobs);
      for (const j of jobs) {
        j.state = "classify"; j.row.engrave = { needed: false, state: "classify",seals:SEALS().merge(j.row.engrave,j) };
        // A fresh reading replaces the line split, size and decision kept for earlier words. The fit restored the first words
        // it had seen, so an order whose words changed on Etsy showed its new words but was engraved with the old ones.
        j.lineInput = null; j.lineMode = "auto"; j.wantSize = null; j.decision = null;
      }
      let r = null;
      // the Claude job already asked about these very words is taken up again (its poll cut off by the network, a reload)
      // rather than paid for a second time; one that failed is replaced once
      const { ask, key } = intentAsk(row, jobs, engravable);
      const prior = job.claudeJob && job.claudeJob.ask === key ? job.claudeJob.id : null;
      try { r = await agentCall("engraveIntent", ask, { label: `Claude reads the words of ${row.order.receiptId}`, background: true, existingId: prior, retryFailed: !!prior, askKey: key, onStarted: id => { job.claudeJob = { id, ask: key }; } }); }
      catch (e) { r = { skipped: e.message }; }
      // (the cloud's reader answers `pending` while its reading is under way: the jobs stay "classify", the line is read again later and the reading taken up again by its id, never paid for twice. A page's never does)
      if (r && r.pending) return job;
      // ENGENT: the reader's answer is the customer's words read back; an answer to a question asked before the words were decoded may still carry Etsy's entities ("&#39;"): the words are read decoded, nothing is asked again
      if (r && !r.skipped && !r.error) { const O = Ord(), d = x => (typeof x === "string" && x.indexOf("&") >= 0 ? O.decodeEntities(x) : x); r = Object.assign({}, r, { text: d(r.text), sourceQuote: d(r.sourceQuote) }, Array.isArray(r.pieces) ? { pieces: r.pieces.map(p => (p && typeof p.text === "string" ? Object.assign({}, p, { text: d(p.text) }) : p)) } : {}); }
      const live = jobs.filter(j => items().get(j.key) === j && j.state === "classify");
      if (items() !== owner || row.state === "gone" || !live.length) return job;
      for (const j of live) await settleReading(j, r);
      return job;
    }
    /** ENGPRE · the words of a line whose piece is not pooled yet, with NO paid reading (Paul, 13 Oct: a clear personalisation, a typed name and a font word, costs nothing). The buyer's personalisation field, the font
     *  request taken out (BFFONT), is the proposal. When it is the only thing the buyer wrote (no message, no staff note), short, it is the words (the very reading the reader gives such a line) and the job goes on to its fit;
     *  anything else is a proposal for a person to confirm ("Words to confirm", the words box already holding it). A line cut into slots asks which words go on which piece. Nothing is written to the order's timeline, the pool or a sheet. */
    async function freeWords(row, jobs) {
      const sp = row.spec, owner = items(), todo = jobs.filter(j => OPEN.includes(j.state) && owner.get(j.key) === j);
      const pers = (sp.personalization || []).map(x => String(x == null ? "" : x)), text = pers.join("\n");
      const O = Ord(), fr = O && O.fontReadText ? O.fontReadText(text, { cueOnly: true }) : { changed: false, lines: [] };
      const lines = (fr.changed ? fr.lines : text.split(/\r?\n/)).map(x => String(x).trim()).filter(Boolean);
      const alone = !String(sp.buyerMessage || "").trim() && !String(sp.staffNote || "").trim() && !(sp.messages || []).length;
      const unitSplit = pieceWordsOf(row, todo);   // (PIECES: the note says which words go on which piece of a line cut by unit)
      if (unitSplit) { await giveUnitWords(row, todo, unitSplit); return todo[0] || jobs[0]; }
      for (const j of todo) {
        j.lineInput = null; j.lineMode = "auto"; j.wantSize = null; j.decision = null;
        const face = faceOf(j), req = { side: face, font: null, handwriting: false, image: false };
        const own = takeAsks(j, lines), awhy = askWhy(j), clearOwn = alone && own.length >= 1 && own.length <= 4 && own.every(x => x.length <= 40) && !awhy;   // (FOLLOWASK: "Anna (on the left side please)" is the word Anna and an ask)
        if (todo.length > 1 && !copiesOnly(todo)) {
          Object.assign(j, { text: "", lines: [], source: "personalization", quote: pers.join(" / ") || null, confidence: 0, questions: [], claudeReasoning: null, requests: req });
          toWords(j, `Say which words go on the ${(SIDES().slotWord(j) || "piece").toLowerCase()}`);
        } else if (clearOwn) {
          Object.assign(j, { text: own.join("\n"), lines: own.slice(), source: "personalization", quote: pers.join(" / ") || null, confidence: 1, questions: [], claudeReasoning: null, requests: req, wordsSource: "personalization:clear" });
          agent({ engrave: true }, "ENGRAVE", `${row.order.receiptId} · ${sp.designSku}: "${j.text.replace(/\n/g, " / ")}" taken from the personalisation, no paid reading`);
          await setReady(j, false);
        } else {
          Object.assign(j, { text: own.join("\n"), lines: own.slice(), source: "personalization", quote: pers.join(" / ") || null, confidence: 0, questions: [], claudeReasoning: null, requests: req, wordsSource: "personalization:proposal" });
          if (awhy) toWords(j, awhy); else if (own.length) toWords(j, "Check the requested inscription"); else toWords(j, "Enter the requested inscription");
        }
        if (items() !== owner || row.state === "gone") return todo[0] || jobs[0];
      }
      return jobs[0];
    }
    /** One job takes the reading (a line cut into slots hands each its own: `r.pieces` names the words of each ear or disc; without them every slot gets the one text). */
    async function settleReading(job, r) {
      const row = job.row, sp = row.spec, O = Ord();
      if (!r || r.skipped || r.error) {
        // Keep the verbatim proposal reviewable even when the classifier is unavailable.
        // (BFFONT: a line that is only a font request - "Typewriter font" - is not a word to engrave; the font itself is spec.fontText)
        const fr0 = O && O.fontReadText ? O.fontReadText(sp.personalization.join("\n"), { cueOnly: true }) : { changed: false, lines: [] };
        job.text = fr0.changed && fr0.lines.length ? fr0.lines.join("\n") : sp.personalization.join("\n"); job.lines = fr0.changed && fr0.lines.length ? fr0.lines : sp.personalization.slice(); job.source = "personalization"; job.confidence = 0; job.questions = [`Claude was unavailable (${(r && (r.skipped || r.error)) || "no answer"}) — confirm the words`]; job.requests = { side: "back", font: null, handwriting: false, image: false };
        { const R = READER(); if (R && job.lines.length) { const fd = R.fixDegrees(job.lines.join("\n")); if (fd.count) { job.lines = fd.text.split("\n"); job.text = fd.text; } const fs = R.fixSymbols ? R.fixSymbols(job.lines.join("\n")) : null; if (fs && fs.count) { job.lines = fs.text.split("\n"); job.text = fs.text; } } }   // (SETBAR-C: the buyer's asterisk in a coordinate is the degree sign here too)
        job.lines = takeAsks(job, job.lines); job.text = job.lines.join("\n");   // (FOLLOWASK)
        if (askWhy(job)) return toWords(job, askWhy(job));
        return job.lines.length ? setReady(job) : toWords(job, "Enter the requested inscription");
      }
      if (job.slot && !(SIDES() && SIDES().isCopy(job.slot)) && Array.isArray(r.pieces) && r.pieces.length) {
        const mine = r.pieces.find(x => x && x.slot === (SIDES() ? SIDES().baseSlot(job.slot) : job.slot));   // (a later unit's piece reads the words of the same ear or disc of the first unit)
        if (!mine) { Object.assign(job, { text: r.text || "", lines: String(r.text || "").split(/\r?\n/).map(x => x.trim()).filter(Boolean), source: r.source, quote: r.sourceQuote, confidence: r.confidence, requests: r.requests, questions: (r.questions || []).slice() }); return toWords(job, `Say which words go on the ${(SIDES().slotWord(job) || "piece").toLowerCase()}`); }
        r = Object.assign({}, r, { text: String(mine.text || ""), engrave: !!String(mine.text || "").trim() });
      }
      job.text = r.text || ""; job.lines = job.text.split(/\r?\n/).map(s => s.trim()).filter(Boolean); job.source = r.source; job.quote = r.sourceQuote; job.confidence = r.confidence; job.requests = r.requests; job.questions = r.questions || []; job.claudeReasoning = r.reasoning || null; job.ignored = Array.isArray(r.ignored) ? r.ignored.slice(0, 8) : [];
      const originalLines=[(sp.personalization || []).join("\n"),sp.buyerMessage,sp.staffNote,job.quote].filter(Boolean)
        .map(t=>String(t).split(/\r?\n/).map(t=>t.trim()).filter(Boolean))
        .find(lines=>lines.length && lines.join(" ").replace(/\s+/g," ")===job.text.replace(/\s+/g," ").trim());
      if(originalLines) {job.lines=originalLines;job.text=originalLines.join("\n");}
      // SETBAR-C: the reader copies the buyer's words verbatim, asterisk and all: an asterisk standing for the degree sign inside a coordinate ("33*0'45\" N") is the degree sign (charm-nest-engrave-read.js fixDegrees; nothing else is touched)
      { const R = READER(); if (R && job.lines.length) { const fd = R.fixDegrees(job.lines.join("\n")); if (fd.count) { job.lines = fd.text.split("\n"); job.text = fd.text; } const fs = R.fixSymbols ? R.fixSymbols(job.lines.join("\n")) : null; if (fs && fs.count) { job.lines = fs.text.split("\n"); job.text = fs.text; } } }
      // BFFONT: a line of the reading that is only a font request ("Typewriter font") is never engraved; its font is read from the order's own words (spec.fontText) and, when the reader named none, from the request itself
      { const fr = O && O.fontReadText ? O.fontReadText(job.lines.join("\n"), { cueOnly: true }) : { changed: false, lines: [], requests: [] };
        if (fr.changed && fr.lines.length) { const ask = fr.requests.find(q => q.asked); job.lines = fr.lines; job.text = fr.lines.join("\n"); if (ask && !(job.requests && job.requests.font)) job.requests = Object.assign({}, job.requests, { font: ask.asked }); } }
      // FOLLOWASK (Paul, 15 Nov 2026): the reader's words lose any instruction to the shop ("on the left", "all caps", a web link); the asks wait on the job for the fit
      job.lines = takeAsks(job, job.lines); job.text = job.lines.join("\n");
      agent({ engrave: true }, "ENGRAVE", `${row.order.receiptId} · ${sp.designSku}${job.slot ? " · " + SIDES().slotWord(job) : ""}: Claude reads ${r.engrave ? `"${job.text.replace(/\n/g, " / ")}" from ${r.source} (${Math.round(r.confidence * 100)}%)` : "no engraving"}${job.questions.length ? ` · ${job.questions.length} question(s)` : ""}${job.ignored.length ? ` · left out (a note to the shop): ${job.ignored.map(x => "“" + x + "”").join(", ")}` : ""}`, { reason: r.reasoning || null });
      // Confidence describes the interpretation, not whether a preview can be drawn.
      // Nothing here approves or exports a proposed inscription.
      if (askWhy(job)) return toWords(job, askWhy(job));
      if (!r.engrave && !job.lines.length) {
        // Settings' engraving confidence: 0 is a setting (trust every reading); it used to be read as 0.8, like an empty field
        const trust = E.trust ? E.trust() : null;
        if (job.confidence < (trust != null && trust !== "" && Number.isFinite(+trust) ? +trust : 0.65) || job.questions.length)
          return toWords(job, "Check the requested inscription");
        setNone(job, "No engraving requested"); return job;
      }
      if (!job.lines.length) return toWords(job, "Enter the requested inscription");
      return setReady(job);
    }
    /* ── ENGNOTE: which line of the order a staff note's words are for (engPick, below make: the page's panel asks it too) ── */
    const jobsOfLine = r => { try { return (E.jobsOf ? E.jobsOf(r) : root && root.Engrave && typeof root.Engrave.jobsOf === "function" ? root.Engrave.jobsOf(r) : []) || []; } catch (_) { return []; } };
    let byOrder = null;   // (FASTOPEN: order → its rows, listed once for a turn of the page; every line of 1,600 walked all 1,600)
    function orderRowsOf(line) {
      try {
        if (E.orderRows) return E.orderRows(line) || [];
        if (!line.order || !(root && root.Orders && typeof root.Orders.rows === "function")) return [];
        if (!byOrder) { byOrder = new Map(); Promise.resolve().then(() => { byOrder = null; }); for (const r of root.Orders.rows()) if (r && r.order) { const k = String(r.order.receiptId); (byOrder.get(k) || byOrder.set(k, []).get(k)).push(r); } }
        return (byOrder.get(String(line.order.receiptId)) || []).slice();
      } catch (_) { return []; }
    }
    const pickHooks = () => ({ READER, jobsOf: jobsOfLine, neverEngraves, finished: E.finished, customSent: E.customSent });
    function engReading(row) { const line = lineRowOf(row); return line && line.spec ? engPick(line, Object.assign({ rows: orderRowsOf(line) }, pickHooks())) : null; }
    /** The note's words on the line's jobs still to read: ready (one job, or copies of one charm), or, when the line has several faces or pieces or the note names no line, words to confirm. No paid reading. */
    async function giveEng(row, jobs, eng) {
      const sp = row.spec, owner = items(), job = jobs[0], n = eng.note, who = n.by || "a staff member", said = `“${n.text.replace(/\n/g, " / ").slice(0, 120)}”`;
      const todo = jobs.filter(j => OPEN.includes(j.state) && owner.get(j.key) === j);
      const many = todo.length > 1 && !copiesOnly(todo), ready = eng.mine && !many;
      const why = ready ? `Words from the staff note by ${who} (ENG: ${said})`
        : eng.mine ? `The staff note by ${who} gives the words ${said} for this line: say which piece or face they go on`
        : `The staff note by ${who} gives the words ${said} but does not say which line of the order they are for: confirm them for this piece, or say what goes on it`;
      for (const j of todo) {
        j.lineInput = null; j.lineMode = "auto"; j.wantSize = null; j.decision = null;
        delete j.asks; delete j.applied; delete j.askRef; j.askUnsure = false;   // (the staff's words are the words: nothing in them is an instruction to the shop)
        Object.assign(j, { text: n.text, lines: n.lines.slice(), source: "staffNote", quote: n.raw, confidence: ready ? 1 : 0, questions: ready ? [] : [why], claudeReasoning: null, ignored: [], requests: { side: faceOf(j), font: null, handwriting: false, image: false }, wordsSource: ready ? "staffNote:eng" : "staffNote:suggest" });
        if (ready) { await setReady(j); if (j.state === "ready") { j.reason = why; j.row.engrave = Object.assign({}, j.row.engrave, { reason: why }); } }
        else { toWords(j, why); if (!j.pre) needEvent(j); }
        if (items() !== owner || row.state === "gone") return job;
      }
      agent({ engrave: true }, "ENGRAVE", `${row.order.receiptId} · ${sp.designSku}: ${ready ? `"${n.text.replace(/\n/g, " / ")}" taken from the staff note by ${who} (ENG:), no paid reading` : why}`);
      return job;
    }
    /** A new or changed note: the jobs of the line that wait for a person (words to confirm), a line the note's words now go to whose reading said none, and a job that took the words of an
     *  earlier note, are read again (reclassify) at the next look. A person's decision, an approval and anything under way are never touched. → true when a job was set to be read again. */
    function engFollow(row) {
      const line = lineRowOf(row); if (!line || !line.spec) return false;
      const eng = engReading(line); if (!eng) return false;
      let hit = false;
      for (const j of jobsOfLine(line)) {
        if (!j || j.archived || j.decision || ["approved", "written", "skipped"].includes(j.state) || OPEN.includes(j.state) || +j.approvedAt > 0 || j.approvalIntent || j.stamping || j.approvalPreparing || j.editingBack) continue;
        const fromEng = /^staffNote:/.test(String(j.wordsSource || "")), seen = fromEng && j.quote === eng.note.raw;
        const again = eng.mine || eng.suggest ? !seen && (j.state === "words" || fromEng || (eng.mine && j.state === "none")) : fromEng;
        if (!again) continue;
        Object.assign(j, { state: "reclassify", fit: null, verify: null, view: null, mask: null, lineInput: null, wantSize: null, reason: null, missing: null });
        if (j.row) j.row.engrave = Object.assign({}, j.row.engrave, { needed: false, state: "reclassify" });
        hit = true;
      }
      return hit;
    }
    return { classifyOnce, raiseCandidate, readByRules, giveRead, takeAsks, askWhy, personalRead, spokeAlone, layoutAsked, jobFace, copiesOnly, pieceWordsOf, giveUnitWords, discWordsOf,
      facedSetup, unsplitDefault, seedOf, classifyFaces, freeWords, settleReading, intentAsk, engReading, engFollow, engNoteOf: m => engNoteOf(m, READER()) };
  }
  return { make, askKey, OPEN, engNoteOf, engPick, engNotice };
});
