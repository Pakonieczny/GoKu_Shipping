/*  charm-nest-monogram-ui.js — the monogram of an order line: the WIDGET (MUW, Paul, 16 Oct 2026) and, until the Engrave tab has switched over, the old "Monograms" pane (MONOGRAM / MONOGRAMUI, Paul, 15 Oct 2026, orders 4200683017 "APV" and 4202783520 "OSR").
 *  ═══════════════════════════════════════════════════════════════════════
 *  MUW — ONE reusable widget for ONE monogram order line, in two densities, in place of the Front / Back faces:
 *      CharmNestMonogramUI.mountLine(host, { rid, lineKey, row, mode: "detail" | "compact", gate, onChange, onApproved }) -> { el, mode, refresh, update, destroy, state, focus, approve, setLetters }
 *      CharmNestMonogramUI.thumbHtml(row, { px }) / fillThumbs(scope) / thumb(row, { px, into, onReady })   the small still picture for the piece card
 *      CharmNestMonogramUI.status(row) -> { state, label, letters, approved, sent }    ·   .widgets()   ·   .service() / .setFactory(fn)
 *  detail = the Engrave detail card (the picture beside the letters, wide); compact = the Order details window (one tight block that keeps ONE height: three single-line slots, a message may replace them). Both draw the same state
 *  (the state of a line lives in the service, per order line: a letter typed in one is in the other) with the same parts: the preview of the geometry that will be cut, in the line's metal; the three letter boxes (type one letter,
 *  swap two neighbours, rotate); the buyer's own words; ONE size line (the piece's mm, the metal, and the size the buyer chose, charm-nest-monogram-fit.js); ONE calm assembly line ("Assembled with AI: …",
 *  only for a word the AI made pass); a plain message when the letters cannot be read or a letter is missing; Approve / "Approved · Monogram APV" with Change / "On the sheet". States: reading, letters, building, ready, check, approved, sent, error.
 *  MONOSEND (Paul, 18 Oct 2026): there is no "Send to Sheet →" link and no designs window in the monogram's way any more: ONE press of the green Approve approves (the seal, the timeline event) AND sends the card's designs to the
 *  nesting queue (CustomSheet.send, as the window's Send to Sheet does), on the buyer's metal, one piece per piece ordered. An approved piece not on a sheet says why in its note line (status(row).queue / describe().queue: sending,
 *  wait (other pieces of the card to approve), metal (the buyer's metal cannot be read: the designs window opens, nothing is guessed), blocked, error, idle) and keeps Approve to finish it; on the sheet it is final.
 *  MONOHOOPS (Paul, 19 Oct 2026): every piece the engine makes carries its two hoops (the picture, the approved design, the sheet); a design approved before them and not on a sheet gets them with its letters unchanged and its approval kept
 *  (see MONOHOOPS below: addHoops / upgradeCard, which CustomSheet.send asks first); its file's monogram tag says { hoops: 2 }.
 *  The contract, with the hooks (data-mw-*) and the events (monogram:change, monogram:approved), is /tmp/mu/CONTRACT.md. The CSS is the `.mw*` block of charm-nest-1.html.
 *  PIECES (Paul, 16 Oct 2026, order 4195059483: "ALL multipiece orders must always show each individual piece with respect to all Engravings and Monograms"): a monogram line of quantity N is N PIECES. Piece u of N is the pieceKey "<lineKey>#C<u>"
 *  (slot C<u>, the grammar of charm-nest-engrave-sides.js; a line of one piece keeps its line's key and no slot, exactly as it was). Everything above is per PIECE: mountLine(host, { ..., slot: "C2" }) (or lineKey "<line>#C2", or piece: 2), thumb /
 *  thumbHtml(row, { slot }), status(row, slot), describe(row, slot), the card states, the Approve and the design file. A piece starts with the three letters the buyer wrote (the same on each; or its own part when the words plainly hold N monograms:
 *  "ABC / XYZ"), is edited on its own and approved on its own; approving writes THAT piece's design, quantity 1, "Monogram APV - piece 2.ai" tagged { mono: { line, slot } } on the card (CustomSheet.add opts qty / mono / metal), kept in piece order
 *  (the send cuts the n-th design as copy n of the line). Never two designs for one piece; changing a piece after its approval replaces only its design; identical letters on two pieces are still two designs. A design made before pieces
 *  ("Monogram APV.ai", quantity k) covers the first k pieces nobody else has (both approved, nothing asked again) and is split in place into one design per piece when one of them is changed. svc.pieces(row) / piecesNow() / pieceKey(row, ask).
 *  MONOSEAL (Paul, 17 Oct 2026: "When I approved the monogram, I didn't see any stamp or seal animation, and we do not have a custom-made seal that is specifically for approving monograms ... a brand new seal that is specific for monograms and
 *  the stamping animation whenever a monogram is approved by the user ... in all the same places as the engraving stamp and seal animations ... 100% consistent"). Approving a monogram is now a PRESS, exactly as approving an engraving is:
 *    1  who: the person signed in on this computer (deps.who(): a name, or a promise of one: the small name bar opens when nobody is signed in, never a browser box; put away = nothing happens);
 *    2  the design is put on the order's custom designs as before; then ONE STAMP is made: { id: "monogramApproved:<at>:<name>", how: "monogramApproved", at, by, letters: "APV", n: piece, of: pieces of the line };
 *    3  the new seal (the ninth family of charm-nest-motion.js: Seal.FAMILY.monogram, an indigo ARCH with the three letters, "MONOGRAM", the date and the time) is laid UNSEEN in the widget's seal row (`.mwSeal`, beside the Approve button in the
 *       "detail" widget, across the card's top-right corner in the Order window's `.engMono` card as the engraving cards' seals are) while the Approve button reads "Approved" and cannot be pressed; the wooden stamp comes down on the seal
 *       (Seal.press, in the widget the person pressed Approve in: the other host just shows the seal), ink, ripple, lift; ONLY THEN the card says Approved (the service reports the new state, the Engrave tab moves the card up to Decided);
 *    4  the timeline event is written (type "monogramApproved", id "<lineKey>[#C2]-<at>", station sorter, data { letters, slot, n, of, poolId, name, ck, signedIn }): the permanent record, and the only one other computers can read;
 *    5  Change and approve again: a NEW seal is pressed and the earlier seals stay (oldest first, the newest on top), as for engravings. The seals of a card are the stamps made here (kept in the service, and on this computer in
 *       settings.mono.seals[pieceKey], so a reload or a reopen shows them at rest, never pressed again: only a fresh approval animates) plus the stamps of the order's timeline events (another computer's approval: shown at rest, no animation).
 *       A seal never touches the design files (the custom-sheet record has no field for it): the file record is what it was.
 *  THE SEAL API for every other surface (library, sheet window, laser, stations, list rows, search): all read-only and safe to call at any time (a surface that has no service gets [] / ""):
 *      CharmNestMonogramUI.sealsOf(row, { slot } | "C2")        -> [{ id, how: "monogramApproved", at, by, letters, n, of }] oldest first: this computer's stamps of that piece merged with the timeline's events already fetched
 *      CharmNestMonogramUI.sealHtml(row, { slot, size, cls })     -> '<span class="sealRow mwSeal ..." data-seal-group>' + one Seal.html(stamp, size, "monogramSeal") per stamp + '</span>', or "" when the piece has none
 *      CharmNestMonogramUI.stampsFromEvents(events, lineKey, n?)  -> the same stamps, read from an order's timeline events (type "monogramApproved") with no service; n = the piece number (omitted: every piece of the line)
 *      CharmNestMonogramUI.mergeStamps(a, b, ...)                 -> stamps of one piece, de-duplicated (one press is one seal: a second of leeway) and sorted
 *      service().feedEvents(rid, events) / loadSeals(rid)         -> hands a timeline to the service (a surface that already holds the order's events) / fetches it (once in 20 s per order)
 *      service().onChange(fn)                                     -> told when a piece's state changes, a stamp arriving included (status.seals = how many)
 *    Press a stamp yourself with Seal.press(sealEl) for a seal you made `pending` (the widget does it for an approval; a surface never presses a seal it only shows).
 *  ═══════════════════════════════════════════════════════════════════════
 *  Paul: "Monograms are currently manually assembled by combining 3 individual letters from 3 separate alphabets ... Each alphabet of letters is made to fit the Left / Middle / Right side of the monogram ... the centre letter
 *  bigger ... The buyer will always provide 3 individual letters ... Each letter must be recognized by the system and fetched from the corresponding alphabet ... combine the 3 letters ... a perfect singular charm piece ...
 *  This process preview and options should be shown in the same UI as we currently show back/front engravings ... User should be able to change the letters and the entire monogram should be remade ... include loading
 *  spinners/progress bars ... minimalist."  And: "Make sure the system understands these three alphabets and all they ask to use and how to apply them ... based on which letters are chosen by the buyer."
 *
 *  What a card is: one open monogram order line (charm-nest-orders.js: special.monogram). Left to right the buyer's three letters are the LEFT, the MIDDLE (the big centre) and the RIGHT letter: letter 1 is taken from the
 *  Left alphabet, letter 2 from the Middle alphabet, letter 3 from the Right alphabet (the families MONOGRAM_LEFT_*, MONOGRAM_MID_*, MONOGRAM_RIGHT_* of the charm repository, found by their names:
 *  CharmNestMonogram.slotsOf). The card says so plainly ("The buyer wrote A P V · Left A · Middle P · Right V"), shows the three letters as three big boxes a person can retype (or swap with a neighbour), the finished
 *  charm drawn large (the geometry that will be cut: CharmNestMonogram.build) and ONE button, Approve. Editing a letter remakes the whole monogram (a small delay, a spinner and the page's progress bar while the
 *  three letters are fetched and joined; only the newest edit ever shows). Approve writes the charm as a design file (.ai, "Monogram APV.ai") and puts it on the order's custom designs (CustomSheet.add), where the
 *  metal and the number of pieces are what they are for every custom order, and sends it to the sheets (MONOSEND). Changing the letters after approving replaces that design (never a second one).
 *  Loads in the page; the bridge makes it: CharmNestMonogramUI.make({ ...the page's own parts }).
 *
 *  MONOAI (Paul, 15 Oct 2026: "we do not sell many of these monograms and they need intelligent assembly: the AI should have freedom to resize ... just like a human would"). A word that still breaks a rule of the
 *  engine (report.metrics.violations) after Engine.autoFit is shown to a vision model beside the ESG reference (charmNestMonogramAssist-background through CN.agentCall "monoAssist"): it answers only the numbers of the engine's
 *  adjust contract { L, M, R: { scale, sx, dx, dy } } and one sentence; the page applies them ONLY through Engine.build (which clamps them), re-measures, and keeps a round only if the number of broken rules drops, or stays equal
 *  while the distance to ESG drops. At most 3 rounds per word, once per order (the final adjust, the sentence and the rounds are kept in settings.mono.assist, so reopening never pays again), at most 3 calls per order per day
 *  (also counted on the server); a word that still fails is state "check" ("Ready to approve"). ESG-like words never call it. A failure of the model or the network leaves the automatic result.
 *  (MONOAPPROVE, Paul 20 Oct 2026: "Remove this red text and Improve again link from the UI completely" / "if the user presses it that means they approve!!": no "Needs a person: …" line, no Improve again, no
 *  "Approve anyway" and no second press on any monogram surface; a flagged card shows the plain Approve and ONE press approves it, the same seal and result as a ready card)
 *  ═══════════════════════════════════════════════════════════════════════ */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === "object" && module.exports) module.exports = api; else root.CharmNestMonogramUI = api;
})(typeof self !== "undefined" ? self : this, function (root) {
  "use strict";
  const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const SLOTS = [["left", "Left"], ["middle", "Middle"], ["right", "Right"]];
  const FILE_RE = /^Monogram ([A-Z]{3})\.ai$/;
  const PIECE_RE = /^Monogram ([A-Z]{3})(?: - piece (\d{1,3}))?\.ai$/;     // (PIECES: "Monogram APV.ai" is a line of one piece, or a design made before the pieces were told apart; "Monogram APV - piece 2.ai" is piece 2 of a line of several)
  const MAX_PIECES = 400;
  const WAIT_MS = 320;                 // (a letter typed: the monogram is remade this long after the last key)
  const LOAD_MS = 30000;               // (a letter file that has not come in this long is given up: a message and a Try again, never a spinner that stays)
  const CACHE_MAX = 40;                // (monograms made this session, by letters: typing a letter back is instant)
  const GONE = ["gone", "committed", "skipped", "noDesign"];
  const TONES = { gold: ["#d9bd78", "#5a4520", "Gold"], gold10k: ["#dec58a", "#5a4520", "10K gold"], gold14k: ["#d4b46a", "#5a4520", "14K gold"], silver: ["#c8ccd2", "#4a4e55", "Silver"], rose: ["#e2b4a1", "#693e33", "Rose gold"] };
  const timeout = (p, ms, what) => new Promise((res, rej) => { const t = setTimeout(() => rej(new Error(what + " took too long")), ms); p.then(v => { clearTimeout(t); res(v); }, e => { clearTimeout(t); rej(e); }); });
  const later = () => new Promise(r => setTimeout(r, 0));
  const spaced = s => String(s || "").split("").join(" ");
  const cap = s => { s = String(s || ""); return s.charAt(0).toUpperCase() + s.slice(1); };


  /* ═══ MONOAI: what the engine says of a result, and the loop that lets the model nudge it (pure: the engine, the model and the picture are handed in, so the offline test runs it with fakes) ═══ */
  const AI_ROUNDS = 3, AI_DAY_MAX = 3, AI_KEEP = 60, AI_PX = 560;
  const metricsOf = res => (res && res.report && res.report.metrics && typeof res.report.metrics === "object" ? res.report.metrics : null);
  /** The rules a result breaks, in words ([] = every hard rule met); null when the engine does not report them (an older engine: the AI is never asked). */
  const violationsOf = res => { const m = metricsOf(res); return m && Array.isArray(m.violations) ? m.violations.map(v => String(typeof v === "string" ? v : (v && (v.text || v.rule)) || "a rule is broken")) : null; };
  /** The broken rules the model can answer (the letters' layout: not the hoops, which the engine places on the piece the model's numbers make). */
  const aiRulesOf = res => { const m = metricsOf(res); return m && Array.isArray(m.violations) ? m.violations.filter(v => !(v && v.key === "hoops")).map(v => String(typeof v === "string" ? v : (v && (v.text || v.rule)) || "a rule is broken")) : null; };
  const distanceOf = res => { const m = metricsOf(res), v = m && (m.distance != null ? m.distance : m.esgDistance); return Number.isFinite(+v) && v !== null && v !== "" ? +v : 0; };
  const IDENT = { scale: 1, sx: 1, dx: 0, dy: 0 };
  const adjustOf = res => { const a = res && res.report && res.report.adjust || {}, one = x => Object.assign({}, IDENT, x && typeof x === "object" ? x : {}); return { L: one(a.L), M: one(a.M), R: one(a.R) }; };
  /** Is the candidate better than the best so far: fewer broken rules, or the same number and a smaller distance to ESG. */
  const betterThan = (a, b) => a.v < b.v || (a.v === b.v && a.dist < b.dist - 1e-6);
  /** The loop. o: { E (engine), parts ({left,middle,right}), base (the result to improve), opts (extra build options), rounds, ask(req) -> the model's answer, cancelled(), onRound(n) }.
   *  A round asks, builds with the answer's adjust through Engine.build (clamped there), measures, keeps it only if better; it stops when no rule is broken or after `rounds` (3 at most) answers.
   *  -> { status: assembled | needs | failed | limited | cancelled | passed | unsupported, res (the best result), adjust, why (of the last kept round), rounds, calls, accepted, remaining (the rules still broken), reason } */
  async function assistLoop(o) {
    const E = o.E, max = Math.max(1, Math.min(AI_ROUNDS, Math.floor(o.rounds || AI_ROUNDS))), v0 = violationsOf(o.base);
    const out = { status: "unsupported", res: o.base, adjust: adjustOf(o.base), why: "", rounds: 0, calls: 0, accepted: 0, remaining: v0 || [], reason: "" };
    if (!v0) { out.reason = "the engine does not report the rules"; return out; }
    if (!v0.length) { out.status = "passed"; return out; }
    let best = { res: o.base, adjust: out.adjust, v: v0.length, vt: v0, dist: distanceOf(o.base) }, stop = "";
    const history = [];
    for (let round = 1; round <= max; round++) {
      if (o.cancelled && o.cancelled()) { stop = "cancelled"; break; }
      if (o.onRound) o.onRound(round);
      let ans; try { ans = await o.ask({ round, adjust: best.adjust, metrics: metricsOf(best.res), violations: best.vt, history: history.slice(-2), res: best.res }); } catch (e) { ans = { error: String((e && e.message) || e) }; }
      out.calls++;
      if (o.cancelled && o.cancelled()) { stop = "cancelled"; break; }
      if (!ans || ans.skipped || ans.error || ans.declined || !ans.adjust || typeof ans.adjust !== "object") { stop = ans && ans.limited ? "limited" : ans && ans.skipped ? "skipped" : "failed"; out.reason = String((ans && (ans.skipped || ans.error)) || "no usable answer").slice(0, 160); break; }
      out.rounds = round;
      let cand = null; try { cand = E.build(o.parts, Object.assign({}, o.opts || {}, { adjust: ans.adjust })); } catch (_) { cand = null; }
      const cv = cand && cand.ok ? violationsOf(cand) : null, mine = cv ? { v: cv.length, dist: distanceOf(cand) } : null;
      if (mine && betterThan(mine, best)) {
        best = { res: cand, adjust: adjustOf(cand), v: mine.v, vt: cv, dist: mine.dist }; out.accepted++; out.why = String(ans.why || "").replace(/\s+/g, " ").trim().slice(0, 240);
        if (!mine.v) break;
      } else history.push({ adjust: cand && cand.report && cand.report.adjust ? adjustOf(cand) : ans.adjust, why: String(ans.why || "").slice(0, 200), violations: mine ? mine.v : best.v + 1 });
    }
    out.res = best.res; out.adjust = best.adjust; out.remaining = best.vt;
    out.status = stop || (best.v ? "needs" : "assembled");
    if (!out.why && out.accepted) out.why = "Adjusted the letters.";
    return out;
  }
  /** The candidate as the model sees it: the cut piece in the card's gold on white, one thin cut line, the contacts painted (upper blue, lower magenta) from report.metrics.contacts [{ zone, xPt, yPt, wMm }] (points, y up,
   *  the frame of res.bboxPt). Fixed size (AI_PX on the long side): the same recipe draws the ESG reference constant (scripts/make-monogram-ref.cjs). -> { svg, w, h } */
  function assistSvg(E, res, o) {
    o = o || {}; const b = res.bboxPt, w = b[2] - b[0], h = b[3] - b[1], pad = Math.max(w, h) * 0.06, k = (o.px || AI_PX) / (Math.max(w, h) + 2 * pad), t = TONES.gold;
    const W = Math.round((w + 2 * pad) * k), H = Math.round((h + 2 * pad) * k), MMPT = 72 / 25.4, m = metricsOf(res), dots = [];
    if (o.patches !== false && m && Array.isArray(m.contacts)) for (const c of m.contacts) {
      if (!c || !Number.isFinite(+c.xPt) || !Number.isFinite(+c.yPt)) continue;
      const col = c.zone === "upper" ? "#1769ff" : c.zone === "lower" ? "#e0189b" : "#8a8f98", r = Math.max(0.35, Math.min(1.2, (+c.wMm || 0.6) / 2 + 0.1)) * MMPT;
      dots.push(`<circle cx="${(+c.xPt - b[0]).toFixed(2)}" cy="${(b[3] - +c.yPt).toFixed(2)}" r="${r.toFixed(2)}" fill="${col}" fill-opacity="0.55" stroke="${col}" stroke-width="${(1.5 / k).toFixed(3)}"/>`);
    }
    const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="${(-pad).toFixed(2)} ${(-pad).toFixed(2)} ${(w + 2 * pad).toFixed(2)} ${(h + 2 * pad).toFixed(2)}"><rect x="${(-pad).toFixed(2)}" y="${(-pad).toFixed(2)}" width="${(w + 2 * pad).toFixed(2)}" height="${(h + 2 * pad).toFixed(2)}" fill="#fff"/><path d="${esc(E.svgPath(res))}" fill="${t[0]}" fill-rule="evenodd" stroke="${t[1]}" stroke-width="${(2.5 / k).toFixed(3)}" stroke-linejoin="round"/>${dots.join("")}</svg>`;
    return { svg, w: W, h: H };
  }

  /* ═══ MONOSEAL: the stamps of monogram approvals (pure: no page, no service) ═══ */
  const SEAL_HOW = "monogramApproved";
  const SAME_PRESS_MS = 1500;           // (one press is one seal, however many records say it: the local stamp and the timeline event of the same approval are a second apart at most)
  const SEAL_KEEP = 8, SEAL_PIECES_KEEP = 150;       // (kept on this computer: the newest 8 stamps of a piece, the newest 150 pieces)
  const pieceNo = slot => { const m = /^C(\d{1,3})$/.exec(String(slot || "")); return m ? +m[1] : 1; };
  /** One stamp, as the seal draws it and the records keep it; null when it is not one (no time). */
  function stampOf(o) {
    if (!o || !(+o.at > 0)) return null;
    const at = Math.round(+o.at), by = String(o.by == null ? "" : o.by).trim(), L = String(o.letters || "").toUpperCase(), s = { id: o.id || `${SEAL_HOW}:${at}:${by}`, how: SEAL_HOW, at, by };
    if (/^[A-Z]{3}$/.test(L)) s.letters = L;
    if (+o.n >= 1) s.n = Math.round(+o.n);
    if (+o.of > 1) s.of = Math.round(+o.of);
    return s;
  }
  /** Stamps of the same press are one (a second of leeway), the earlier record kept; oldest first. */
  function mergeStamps() {
    const out = [];
    for (const list of arguments) for (const s0 of Array.isArray(list) ? list : []) {
      // (the same press: the same id, or a second apart for the same piece with the same letters, whoever the two records name: the local stamp and the timeline's event of one approval; two approvals with other letters are two stamps whenever they were made)
      const s = stampOf(s0); if (!s || out.some(x => x.id === s.id || (Math.abs(x.at - s.at) <= SAME_PRESS_MS && (x.n || 1) === (s.n || 1) && (!x.letters || !s.letters || x.letters === s.letters)))) continue;
      out.push(s);
    }
    return out.sort((a, b) => a.at - b.at || (a.id < b.id ? -1 : 1));
  }
  /** The stamps an order's timeline holds for the piece n of the line lineKey (n omitted: every piece of it): its monogramApproved events. */
  function stampsFromEvents(events, lineKey, n) {
    const out = [];
    for (const e of Array.isArray(events) ? events : []) {
      if (!e || e.type !== SEAL_HOW || !(+e.at > 0)) continue;
      if (lineKey != null && String(e.lineKey || "") !== String(lineKey)) continue;
      const dd = e.data && typeof e.data === "object" ? e.data : {}, no = +dd.n >= 1 ? Math.round(+dd.n) : pieceNo(dd.slot);
      if (n != null && no !== +n) continue;
      out.push({ at: +e.at, by: e.by, letters: dd.letters, n: no, of: dd.of });
    }
    return mergeStamps(out);
  }

  function make(d) {
    const cards = new Map();           // pieceKey -> { key (= the line's key for a line of one piece, "<lineKey>#C2" for piece 2 of a line of several), lineKey, slot, n, of, row, draft (the letters a person typed), status, result, err, seq, timer, views (the mounted widgets) }
    const inflight = new Map();        // line key|alphabet key -> the AI assembly being asked for (a second piece with the same letters waits for it: one paid answer, never two)
    const glyphs = new Map();          // sku|file|time -> Promise<{ letter, rings, sku }>
    const results = new Map();         // alphabets|letters -> a finished build (the oldest is dropped first)
    let famCache = null, famSig = "", aiServerOff = false;   // (aiServerOff: the server said the assist is switched off or has no key: no more jobs this session)
    const abcKey = abc => abc.sig + "|" + abc.fams.map(f => f.base + "#" + f.count).join(",");
    const E = () => d.Engine || root.CharmNestMonogram;
    const doc = d.doc || root.document;
    const rowsNow = () => (d.rows() || []).filter(r => r && r.spec && r.spec.special && r.spec.special.monogram && !GONE.includes(r.state));
    // what the buyer wrote: the personalisation box and the options that ask for initials (the orders module found them), else the box as the order holds it, else a short message that is just three letters
    const wordsOf = row => {
      const sp = row.spec || {}, w = String((sp.special && sp.special.words) || "").trim() || (sp.personalization || []).join(" ").trim();
      if (w) return w;
      const m = String(sp.buyerMessage || "").trim(); return m.length <= 40 && E() && E().lettersOf && E().lettersOf(m).ok ? m : "";
    };

    /* ── PIECES (Paul, 16 Oct 2026, order 4195059483: "ALL multipiece orders must always show each individual piece with respect to all Engravings and Monograms") ──
       A monogram line of quantity N is N pieces: piece u of N is pieceKey "<lineKey>#C<u>" (slot C<u>; the grammar of charm-nest-engrave-sides.js), its pool id "<lineKey>_<u>". A line of one piece is exactly what it always was
       (pieceKey = the line's key, slot null). Every piece has its own card here: its own letters, its own build, its own Approve and its own design file (quantity 1). */
    const unitsOf = row => Math.max(1, Math.min(MAX_PIECES, Math.round(+(row && row.spec && row.spec.quantity) || +(row && row.line && row.line.quantity) || 1)));
    const slotKey = (lineKey, slot) => (slot ? lineKey + "#" + slot : lineKey);
    const slotNo = slot => { const m = /^C(\d{1,3})$/.exec(String(slot || "")); return m ? +m[1] : 1; };
    /** The pieces of a line, in order: [{ key, slot, n, of, lineKey, poolId }]. */
    function piecesOfLine(row) {
      const of = unitsOf(row);
      return Array.from({ length: of }, (_, i) => { const slot = of > 1 ? "C" + (i + 1) : null; return { key: slotKey(row.key, slot), slot, n: i + 1, of, lineKey: row.key, poolId: row.key + "_" + (i + 1) }; });
    }
    /** The slot a piece is asked for (a slot, a piece key, a piece number, a { slot } option) as this line knows it: null for a line of one piece. */
    function slotFor(row, ask) {
      const of = unitsOf(row); if (of < 2) return null;
      let v = ask && typeof ask === "object" ? (ask.slot != null ? ask.slot : ask.pieceKey != null ? ask.pieceKey : ask.piece) : ask;
      if (v == null || v === "") return "C1";
      if (typeof v === "number") return v >= 1 && v <= of ? "C" + v : "C1";
      v = String(v); const i = v.indexOf("#"); if (i >= 0) v = v.slice(i + 1);
      const n = /^C\d{1,3}$/.test(v) ? slotNo(v) : /^\d{1,3}$/.test(v) ? +v : 0;
      return n >= 1 && n <= of ? "C" + n : "C1";
    }
    const pieceTag = (n, of) => (of > 1 ? `Piece ${n} of ${of}` : "");
    const ok3 = w => { const r = E() && E().lettersOf ? E().lettersOf(w) : null; return !!(r && r.ok); };
    /** What the buyer wrote for each of the N pieces. Deterministic, no paid call: when the words plainly hold N parts (numbered "1: ABC, 2: XYZ", N lines, N entries, "ABC / XYZ", "ABC & XYZ", "ABC and XYZ", "ABC XYZ") and every part reads as three letters, piece i gets
     *  part i; otherwise EVERY piece gets the whole words (the same monogram on each, each editable and approved on its own). A line of one piece: its words, as ever. */
    function wordsFor(row, n) {
      const of = unitsOf(row), all = wordsOf(row);
      if (of < 2 || !all) return all;
      const S = d.Sides || root.CharmNestEngraveSides;
      let parts = null;
      if (S && typeof S.splitWords === "function") { try { const r = S.splitWords([all], of); if (r && r.ok && r.words.length === of && r.words.every(ok3)) parts = r.words; } catch (_) { parts = null; } }
      if (!parts && !ok3(all)) {
        for (const re of [/\n+/, /\s*[,;|\/&]\s*|\s+and\s+/i, /\s+/]) { const p = String(all).split(re).map(x => x.trim()).filter(Boolean); if (p.length === of && p.every(ok3)) { parts = p; break; } }
      }
      return parts ? parts[Math.max(1, Math.min(of, n || 1)) - 1] : all;
    }

    /* ── the alphabets: the families the library holds, and which three are Left, Middle, Right (by name, or what a person chose once) ── */
    function families() {
      const skus = d.skus(); const sig = skus.length + ":" + (skus[0] || "") + ":" + (skus[skus.length - 1] || "");
      if (famCache && sig === famSig) return famCache;
      famSig = sig; famCache = (E() && E().familiesOf ? E().familiesOf(skus) : []) || []; return famCache;
    }
    function alphabets() {
      const set = d.settings(), saved = (set && set.mono) || {}, fams = families();
      const r = E() && E().slotsOf ? E().slotsOf(fams, saved) : { ok: false, left: null, middle: null, right: null, sets: [], by: {}, why: "the monogram engine did not load" };
      return Object.assign({ fams }, r, { sig: ["left", "middle", "right"].map(k => r[k] ? r[k].base + "#" + r[k].count : "-").join("|") });
    }
    /** "Choose the Right alphabet." / "Choose the Left and Middle alphabets." / "Choose the three alphabets." (the slots no family names yet) */
    function chooseText(abc) {
      const need = [["left", "Left"], ["middle", "Middle"], ["right", "Right"]].filter(x => !abc[x[0]]).map(x => x[1]);
      return need.length === 3 || !need.length ? "Choose the three alphabets." : `Choose the ${need.join(" and ")} alphabet${need.length > 1 ? "s" : ""}.`;
    }
    function setAlphabet(slot, base) {
      const set = d.settings(), next = Object.assign({}, set.mono || {}); if (base) next[slot] = base; else delete next[slot];
      set.mono = next; try { d.saveSettings(); } catch (_) { /* kept for this session */ }
      for (const c of cards.values()) if (c.views.size) schedule(c, 0);
    }

    /* ── a letter of an alphabet: its drawing's rings (the letter as drawn: outline and counters) ── */
    const skuFor = (fam, letter) => fam.base.replace(/\{[LN]\}/, letter);
    function missing(slot, letter, abc) {
      const fam = abc[slot], err = new Error(`The ${slot === "middle" ? "Middle" : slot === "left" ? "Left" : "Right"} alphabet has no “${letter}”`);
      err.kind = "missing"; err.help = fam ? `${fam.name} has no ${letter} yet. Add it to the library, or type another letter.` : "";
      return err;
    }
    async function glyph(slot, letter, abc) {
      const fam = abc[slot], sku = fam && fam.letters && fam.letters[letter];
      if (!sku) throw missing(slot, letter, abc);
      let entry = d.Master.entryFor(sku); if (!entry) { try { entry = await d.Master.fetchEntry(sku); } catch (_) { entry = null; } }
      if (!entry) { const e = new Error(`${sku} is not in the library`); e.kind = "missing"; e.help = "Index the monogram file in Master again"; throw e; }
      const key = sku + "|" + (entry.aiPath || (entry.sizes && Object.keys(entry.sizes).length ? "sized" : "")) + "|" + (entry.indexedAt || entry.updatedAt || "");
      if (!glyphs.has(key)) glyphs.set(key, timeout((async () => {
        const src = await d.Pool.masterCharm(entry, null), charm = src && src.charms && src.charms[0];
        if (!charm || !charm.outline) throw new Error(`${sku} has no outline`);
        const G = d.Geom, rings = G.flatten(charm.outline, 16).filter(r => r.length > 2);
        for (const m of d.P.cutLinesOf(charm)) for (const r of G.flatten(m, 16)) if (r.length > 2) rings.push(r);
        if (!rings.length) throw new Error(`${sku} has no outline`);
        const gm = src.parsed && src.parsed.glyph;      // (the glyph's baseline and font size, written by the master reader for a letter drawn from live text: the engine puts the three letters on one baseline)
        return { letter, rings, sku, fill: "auto", metrics: gm && Number.isFinite(gm.baselinePt) ? { baselinePt: gm.baselinePt, emPt: gm.emPt } : null };      // ("auto": a font's contours may overlap, the overlap is ink: CharmNestMonogram fillOf)
      })(), LOAD_MS, `Letter ${letter} of ${fam.name}`).catch(e => { glyphs.delete(key); if (!e.kind) e.kind = "load"; throw e; }));
      return glyphs.get(key);
    }

    /* ── one card's state ── */
    function cardOf(row, ask) {
      const slot = slotFor(row, ask), key = slotKey(row.key, slot);
      let c = cards.get(key);
      if (!c) { c = { key, lineKey: row.key, slot, n: slot ? slotNo(slot) : 1, of: unitsOf(row), row, draft: null, approved: "", status: "idle", result: null, resultLetters: "", err: null, seq: 0, timer: 0, host: null, el: null, approving: false, hint: "", wantApprove: false, ai: null, fit: false, confirmed: false, views: new Set(), unlocked: false, prog: null, quiet: false, queued: false, aiKey: "", abcSig: "", sig: "", tsig: "", seals: null, sealNew: "", stamping: false, asking: false, pressView: null }; cards.set(key, c); }
      c.row = row; c.of = unitsOf(row); return c;
    }
    const lettersOk = ls => ls.length === 3 && ls.every(x => /^[A-Z]$/.test(x));
    function buyerLetters(c) {
      const w = wordsFor(c.row, c.n), r = E() && E().lettersOf ? E().lettersOf(w) : { ok: false, letters: null, why: "" };
      if (r.ok) return { ls: r.letters.slice(), why: "" };
      const some = (w.match(/[A-Za-z]/g) || []).map(x => x.toUpperCase());      // ("A.P": two of three, the person adds the third; a name or four letters: nothing is guessed)
      return { ls: some.length && some.length < 3 ? [...some, "", ""].slice(0, 3) : ["", "", ""], why: r.why || "" };
    }
    const readLetters = c => c.draft ? c.draft.slice() : buyerLetters(c).ls;
    // the order's custom card (Review builds its list to say: asked a few times a second while a person types, so for half a second it is the one found a moment ago)
    const itemOf = (c, fresh) => { const t = Date.now(); if (!fresh && c.item && t - c.itemAt < 500) return c.item; const it = d.itemOf(c.row); if (it) { c.item = it; c.itemAt = t; } return it; };
    /** PIECES: the design file of ONE piece of a line of several, or null. A sent card says exactly which file each copy of the line went as (sent.lines[lineKey], copy n = the n-th run of charms); an unsent one by the file's own tag
     *  ({ mono: { line, slot } }, or "Monogram APV - piece 2.ai" when the tag is lost); a design made before the pieces were told apart ("Monogram APV.ai", quantity k, no piece in its name) covers the k pieces nobody else has, in order, so a line
     *  approved as ONE design of quantity 2 reads as pieces 1 and 2 both approved, with nothing asked again. { file, letters, sent, ck, legacy (made before the pieces) } */
    const soleLine = (c, card) => !rowsNow().some(r => { if (r.key === c.lineKey) return false; const o = itemOf(cardOf(r)), k = o && d.CustomSheet.cardOf(o); return !!k && k.ck === card.ck; });
    function coverage(c, e, card) {
      const files = (e.files || []).filter(F => PIECE_RE.test(F.name || "")), map = new Map(), sole = soleLine(c, card);
      for (const F of files) {
        const pn = PIECE_RE.exec(F.name)[2];
        if (F.mono && F.mono.line === c.lineKey) { const n = slotNo(F.mono.slot); if (n >= 1 && n <= c.of && !map.has(n)) map.set(n, { file: F, legacy: false }); }
        else if (!F.mono && pn && sole && +pn >= 1 && +pn <= c.of && !map.has(+pn)) map.set(+pn, { file: F, legacy: false });
      }
      if (sole) {
        const free = []; for (let n = 1; n <= c.of; n++) if (!map.has(n)) free.push(n);
        let i = 0; for (const F of files) { if (F.mono || PIECE_RE.exec(F.name)[2]) continue; for (let k = 0, q = Math.max(1, F.qty | 0); k < q && i < free.length; k++) map.set(free[i++], { file: F, legacy: true }); }
      }
      return map;
    }
    function pieceFile(c, e, card) {
      if (!e) return null;
      const lines = (e.sent || e.sendIntent || {}).lines, pcs = lines && lines[c.lineKey];
      if (Array.isArray(pcs)) {
        const copy = pcs.filter(pc => pc && pc.i === 0)[c.n - 1], F = copy && (e.files || []).find(x => x.id === copy.f);
        if (F && PIECE_RE.test(F.name || "")) return { file: F, letters: PIECE_RE.exec(F.name)[1], sent: !!e.sent, ck: e.ck, legacy: !F.mono && !PIECE_RE.exec(F.name)[2] };
        if (e.sent) return null;
      }
      const hit = coverage(c, e, card).get(c.n);
      return hit ? { file: hit.file, letters: PIECE_RE.exec(hit.file.name)[1], sent: !!e.sent, ck: e.ck, legacy: hit.legacy } : null;
    }
    function approvedFile(c, fresh) {
      const it = itemOf(c, fresh), card = it && d.CustomSheet.cardOf(it), e = card && d.CustomSheet.entries()[card.ck];
      if (c.of > 1) return pieceFile(c, e, card);
      // (a line of ONE piece: its own "Monogram APV.ai"; and, when the quantity was lowered to 1, the design its piece 1 had: "Monogram APV - piece 1.ai" tagged with this line)
      const files = e ? (e.files || []).filter(F => FILE_RE.test(F.name || "") || (F.mono && F.mono.line === c.lineKey && slotNo(F.mono.slot) === 1 && PIECE_RE.test(F.name || ""))) : [];
      if (!files.length) return null;
      const lettersOf = F => PIECE_RE.exec(F.name)[1], mine = readLetters(c).join("");
      let f = (c.approved && files.find(F => lettersOf(F) === c.approved)) || files.find(F => lettersOf(F) === mine);
      if (!f && !rowsNow().some(r => { if (r === c.row) return false; const o = itemOf(cardOf(r)), k = o && d.CustomSheet.cardOf(o); return !!k && k.ck === card.ck; })) f = files[files.length - 1];   // (the card's only monogram line owns the file)
      return f ? { file: f, letters: lettersOf(f), sent: !!(e && e.sent), ck: e.ck } : null;
    }
    // what a monogram is made from: the three letter files (their SKU, file and indexing time), so a monogram made before stays good until one of ITS letters changes in the library
    const sigOf = (abc, ls) => SLOTS.map(([slot], i) => { const fam = abc[slot], sku = fam && fam.letters && fam.letters[ls[i]], e = sku && d.Master.entryFor(sku); return (sku || "?") + "@" + (e ? (e.aiPath || "") + "/" + (e.indexedAt || e.updatedAt || "") : ""); }).join("|");
    function remember(key, res) { results.delete(key); results.set(key, res); while (results.size > CACHE_MAX) results.delete(results.keys().next().value); }
    // MUW: the size the buyer chose ("18MM": the widest the finished piece may be, charm-nest-monogram-fit.js) as the engine's opts.fit; the piece only shrinks to it. Part of the cache key: another size is another monogram.
    const Fit = () => d.Fit || root.CharmNestMonogramFit || null;
    const fitOf = c => { const F = Fit(), sz = c && c.row && c.row.spec && c.row.spec.size; return F && F.fitFromSize && sz ? F.fitFromSize(sz) : null; };
    const buildOpts = c => { const f = fitOf(c); return f ? { fit: f } : {}; };
    const fitKey = c => { const f = fitOf(c); return f ? "|w" + f.maxWidthMm : ""; };
    function markBuilding(c) {
      clearTimeout(c.timer); c.seq++; c.ai = null; c.fit = false;      // (anything still on its way for an older edit is stale from now on)
      c.status = "building"; c.err = null; paint(c);
    }
    function schedule(c, wait) {
      markBuilding(c); c.quiet = false;
      c.timer = setTimeout(() => rebuild(c), wait == null ? WAIT_MS : wait);
    }

    /* ── making the monogram ── */
    async function rebuild(c) {
      clearTimeout(c.timer);
      const letters = readLetters(c), seq = ++c.seq;
      if (!lettersOk(letters)) { c.status = "words"; c.result = null; c.err = null; paint(c); return; }
      const abc = alphabets(); c.abcSig = abcKey(abc);
      if (!abc.ok) { c.status = "setup"; c.result = null; c.err = null; paint(c); return; }
      const key = sigOf(abc, letters) + fitKey(c), cached = results.get(key);
      if (cached) { c.status = "ready"; c.result = cached; c.resultLetters = letters.join(""); c.resultKey = key; c.err = null; remember(key, cached); if (wantAi(c)) assist(c, key, letters, abc, seq); paint(c); settled(c); return; }
      c.status = "building"; c.err = null; c.prog = { done: 0, total: 4 }; paint(c);
      const bar = d.CNProgress && !c.quiet ? d.CNProgress.start(`Building monogram ${letters.join("")}`) : null;      // (a picture asked for by a list thumbnail alone is made quietly: no bar, no AI)
      try {
        if (bar && bar.set) bar.set(0, 4);
        // COST (Oct 2026): the edited letters' table (charm-nest-monogram-edits.js, 0.8 MB) is loaded by the page the first time a
        // monogram is made (d.ready), beside the letters; nothing is built without it (the plain glyphs are not what is cut)
        const ready = d.ready ? Promise.resolve().then(d.ready).then(() => null, e => e || new Error("the edited letters did not load")) : null;
        let done = 0;
        const parts = await Promise.all(SLOTS.map(([slot], i) => glyph(slot, letters[i], abc).then(g => { if (bar && bar.set) bar.set(++done, 4); if (seq === c.seq) { c.prog = { done, total: 4 }; paint(c); } return g; })));
        if (seq !== c.seq) return;                       // (a newer edit took over: this answer is never shown)
        const notReady = ready && await ready; if (notReady) throw notReady;
        if (seq !== c.seq) return;
        await later();                                   // (the spinner is on the screen before the joining starts)
        if (seq !== c.seq) return;
        const trio = { left: parts[0], middle: parts[1], right: parts[2] };
        let res = E().build(trio, buildOpts(c));
        if (seq !== c.seq) return;
        // a word that breaks a rule is fitted by the engine's own search first (Engine.autoFit: deterministic, a few seconds at most); the AI comes after, only if it still fails
        const broken = res && res.ok ? violationsOf(res) : null;
        if (broken && broken.length && typeof E().autoFit === "function") {
          c.fit = true; paint(c); await paintFirst(); if (seq !== c.seq) return;
          try { const f = E().autoFit(trio, buildOpts(c)); const fv = f && f.ok ? violationsOf(f) : null; if (fv && betterThan({ v: fv.length, dist: distanceOf(f) }, { v: broken.length, dist: distanceOf(res) })) res = f; } catch (_) { /* the plain build stays */ }
          if (seq !== c.seq) return;
        }
        if (bar && bar.set) bar.set(4, 4);
        if (!res || !res.ok) { c.status = "error"; c.result = null; c.err = { kind: "join", text: joinWhy(res && res.why) }; }
        else { c.status = "ready"; c.result = res; c.resultLetters = letters.join(""); c.resultKey = key; c.err = null; remember(key, res); if (wantAi(c)) assist(c, key, letters, abc, seq, { parts: trio }); }
      } catch (e) {
        if (seq !== c.seq) return;
        c.status = "error"; c.result = null;
        c.err = e && e.kind === "missing" ? { kind: "missing", text: String(e.message).slice(0, 160), help: e.help || "" } : { kind: "load", text: loadWhy(e) };
      } finally {
        if (bar) bar.end();
        if (seq === c.seq) { c.fit = false; c.prog = null; paint(c); settled(c); }
      }
    }
    const wantAi = c => !c.quiet || c.views.size > 0;        // (the AI is only asked for a monogram a person is looking at)
    const joinWhy = why => /touch/.test(why || "") ? "These letters do not touch each other. Try other letters, or the letters in another order." : /separate parts/.test(why || "") ? "One of these letters is made of separate parts that stay loose, so it cannot be one charm." : String(why || "The letters could not be joined").slice(0, 160);
    const loadWhy = e => /took too long/.test(String(e && e.message)) ? `${String(e.message).slice(0, 90)}. Check the connection and try again.` : "A letter could not be loaded from the library. Check the connection and try again.";

    /* ── MONOAI: the AI assembler of a word that still breaks a rule ── */
    const aiBusy = c => !!(c.ai && c.ai.state === "working");
    const flaggedOf = c => (c.status === "ready" && c.result ? violationsOf(c.result) : null) || [];       // (the rules the card's monogram still breaks: [] when none, or when the engine does not say)
    const limitText = left => String(left[0] || "").replace(/\.$/, "") + (left.length > 1 ? ` (+${left.length - 1} more)` : "");
    /** The engine states a broken rule with its numbers and limits (and hands the AI exactly that); a person is told it in plain words. Display only: state().assembly.limit keeps the engine's text. */
    const plainRule = t => {
      t = String(t || "").trim().replace(/\.$/, "");
      const pair = /(left and middle|middle and right) letters/.exec(t);
      if (/not held by one contact/.test(t)) return `the ${pair ? pair[1] : "neighbouring"} letters are not joined at both the top and the bottom`;
      if (/centre letter lies/.test(t)) return "the middle letter overlaps its neighbours too much";
      if (/letter lies .* inside/.test(t)) return "the letters overlap too much";
      if (/closer than .* without touching/.test(t)) return "some strokes run very close together without touching";
      if (/inner openings/.test(t)) return "a letter's inner openings are too closed";
      if (/as wide as it is high|overall width \/ height/.test(t)) return "the monogram is too wide or too narrow for its height";
      if (/differ .* in ink height/.test(t)) return "the two side letters differ too much in height";
      if (/stands .* above the centre|in ink height/.test(t)) return "a side letter sits too high or too low next to the middle one";
      if (/in width/.test(t)) return "the letter widths are out of balance";
      if (/outline lies outside|of its width beside the centre/.test(t)) return "a side letter is mostly hidden behind the middle one";
      return null;
    };
    const plainNote = t => { const p = plainRule(t); return p ? cap(p) + "." : String(t || ""); };
    const paintFirst = () => new Promise(r => { const raf = root.requestAnimationFrame; if (typeof raf === "function") raf.call(root, () => setTimeout(r, 0)); else setTimeout(r, 0); });
    const callAi = () => !!(d.assist || d.agentCall || (root.CN && root.CN.agentCall));
    const aiOn = () => { const m = (d.settings() || {}).mono; return !(m && m.ai === "off") && d.ai !== false; };
    const today = () => new Date(d.now ? d.now() : Date.now()).toISOString().slice(0, 10);
    const mstore = () => { const set = d.settings(); if (!set.mono || typeof set.mono !== "object") set.mono = {}; return set.mono; };
    const keepSettings = () => { try { d.saveSettings(); } catch (_) { /* kept for this session */ } };
    const recKey = (c, key) => c.row.key + "|" + key;
    const dayCount = c => { const r = (mstore().assistDay || {})[c.row.key]; return r && r.day === today() ? r.n | 0 : 0; };
    function bumpDay(c, n) { const m = mstore(), day = today(), next = {}; for (const [k, v] of Object.entries(m.assistDay || {})) if (v && v.day === day) next[k] = v; next[c.row.key] = { day, n }; m.assistDay = next; keepSettings(); }
    /** What the loop ended with, kept (adjust, the sentence, rounds, status) so reopening the order applies it through the engine and never asks again. */
    function keepRec(c, key, rec) {
      const m = mstore(), all = Object.assign({}, m.assist); all[recKey(c, key)] = rec;
      const old = Object.keys(all).sort((a, b) => (all[a].at || 0) - (all[b].at || 0)); while (old.length > AI_KEEP) delete all[old.shift()];
      m.assist = all; keepSettings();
    }
    async function trioOf(letters, abc) { const p = await Promise.all(SLOTS.map(([slot], i) => glyph(slot, letters[i], abc))); return { left: p[0], middle: p[1], right: p[2] }; }
    /** The candidate as a 2x PNG data URL (the cut piece on white, contacts painted). */
    async function pictureOf(res) {
      if (d.pictureOf) return d.pictureOf(res);
      const a = assistSvg(E(), res), img = new root.Image(); img.src = "data:image/svg+xml;charset=utf-8," + encodeURIComponent(a.svg);
      await img.decode();
      const cv = doc.createElement("canvas"); cv.width = a.w; cv.height = a.h; const g = cv.getContext("2d"); g.fillStyle = "#fff"; g.fillRect(0, 0, a.w, a.h); g.drawImage(img, 0, 0, a.w, a.h);
      return cv.toDataURL("image/png");
    }
    async function askModel(c, letters, req) {
      const image = await pictureOf(req.res);
      const payload = { orderKey: c.row.key, letters, round: req.round, adjust: req.adjust, metrics: req.metrics, violations: req.violations, history: req.history, image };
      bumpDay(c, dayCount(c) + 1);                       // (counted as it is asked: a call that fails on the way may still have been paid for)
      const label = `Assembling monogram ${letters.join("")} · round ${req.round}`;
      const ans = d.assist ? await d.assist(payload, { label, background: true }) : await (d.agentCall || root.CN.agentCall)("monoAssist", payload, { label, background: true });
      if (ans && ans.limited) bumpDay(c, AI_DAY_MAX);
      return ans;
    }
    /** After a card's monogram is made (and fitted by the engine): a word that still breaks a rule is assembled with the AI, at most 3 rounds, once per order (what was kept is applied again, never asked again);
     *  Never throws; the card keeps its picture while it waits. */
    async function assist(c, key, letters, abc, seq, o) {
      // (PIECES: two pieces of one line with the same letters are one question to the model: the second waits for the first and applies its kept answer, never asking, never paying, twice)
      const rk = recKey(c, key), other = inflight.get(rk);
      if (other) { c.aiKey = key; c.ai = { state: "working", round: 0, status: "" }; paint(c); try { await other; } catch (_) { /* the first one's trouble */ } if (seq !== c.seq) return; }
      const run = assistRun(c, key, letters, abc, seq, o);
      if (!other) { inflight.set(rk, run); run.then(() => { if (inflight.get(rk) === run) inflight.delete(rk); }, () => { if (inflight.get(rk) === run) inflight.delete(rk); }); }
      return run;
    }
    async function assistRun(c, key, letters, abc, seq, o) {
      o = o || {}; c.aiKey = key;
      const res0 = c.result, v0 = (violationsOf(res0) || null) && aiRulesOf(res0);      // (MONOHOOPS: a hoop that found no sound spot is for a person, never a question to the model)
      if (!v0 || !v0.length) {                               // (ESG-like and every word that meets the rules: never asked; one the AI made pass keeps its line when the finished monogram comes from memory)
        const kept = (mstore().assist || {})[recKey(c, key)]; c.ai = kept && kept.status === "assembled" ? { state: "done", status: "assembled", why: kept.why, rounds: kept.rounds, cached: true } : null; return;
      }
      c.ai = { state: "working", round: 0, status: "" };
      try {
        const parts = o.parts || await trioOf(letters, abc);
        if (seq !== c.seq) return;
        const rec = (mstore().assist || {})[recKey(c, key)];
        if (rec) {
          const r = rec.adjust ? E().build(parts, Object.assign({ adjust: rec.adjust }, buildOpts(c))) : null, rv = r && r.ok ? violationsOf(r) : null;
          if (rv && betterThan({ v: rv.length, dist: distanceOf(r) }, { v: v0.length, dist: distanceOf(res0) })) { c.result = r; remember(key, r); }
          c.ai = { state: "done", status: rec.status, why: rec.why, rounds: rec.rounds, cached: true }; return;
        }
        if (!aiOn() || aiServerOff || !callAi()) { c.ai = { state: "done", status: "off" }; return; }
        const room = Math.min(AI_ROUNDS, AI_DAY_MAX - dayCount(c));
        if (room < 1) { c.ai = { state: "done", status: "limited" }; return; }
        const out = await assistLoop({ E: E(), parts, base: res0, opts: buildOpts(c), rounds: room, cancelled: () => seq !== c.seq, ask: req => askModel(c, letters, req), onRound: n => { if (c.ai) { c.ai.round = n; paint(c); } } });
        if (seq !== c.seq) return;
        if (out.accepted && out.res) { c.result = out.res; remember(key, out.res); }
        const kept = out.status === "assembled" || out.status === "needs" || (out.status === "failed" && out.accepted);
        if (kept) keepRec(c, key, { adjust: out.adjust, why: out.why, rounds: out.rounds, status: out.remaining.length ? "needs" : "assembled", limit: limitText(out.remaining), at: Date.now() });
        if (out.status === "skipped") aiServerOff = true;
        c.ai = { state: "done", status: kept ? (out.remaining.length ? "needs" : "assembled") : out.status === "skipped" ? "off" : out.status, why: out.why, rounds: out.rounds, reason: out.reason };
      } catch (e) {
        if (seq === c.seq) c.ai = { state: "done", status: "failed", reason: String((e && e.message) || e).slice(0, 120) };
      } finally {
        if (seq === c.seq) { if (c.ai && c.ai.state === "working") c.ai.state = "done"; paint(c); settled(c); }
      }
    }
    // (MONOAPPROVE, Paul 20 Oct 2026: there is no "Improve again" any more: a flagged card is approved as it is, with one press)
    function setAi(v) { const m = mstore(); m.ai = v === "off" ? "off" : "on"; keepSettings(); for (const c of cards.values()) if (c.views.size) paint(c); }
    function settled(c) { if (c.wantApprove && c.status === "ready" && !aiBusy(c)) { c.wantApprove = false; approve(c); } else { if (c.status !== "building" && !aiBusy(c)) c.wantApprove = false; if (c.status === "ready" && !aiBusy(c)) hoopsSoon(c); } }      // (MONOHOOPS: a design approved before the hoops gets them once its piece is made)

    /* ── MONOSEAL: the stamps of a piece: made here (kept in the card, and on this computer in settings.mono.seals[pieceKey]) and read from the order's timeline (another computer's, or an earlier session's) ── */
    const SEALS = () => d.Seal || root.Seal || null;
    const ridOfCard = c => String((c.row.order || {}).receiptId || "");
    const evCache = new Map(), evAsked = new Map();        // order number -> its monogramApproved events as the timeline last said them / when the timeline was last asked
    const sealStore = () => { const m = mstore(); if (!m.seals || typeof m.seals !== "object") m.seals = {}; return m.seals; };
    /** The stamps of this piece the card has: those kept on this computer (read once), merged with the timeline's. Oldest first, copies. */
    function cardSeals(c) {
      if (!c.seals) { let kept = []; try { kept = mergeStamps(sealStore()[c.key]); } catch (_) { kept = []; } c.seals = kept; }
      return c.seals;
    }
    const sealsFor = c => mergeStamps(c.seals, stampsFromEvents(evCache.get(ridOfCard(c)), c.lineKey, c.n));
    function keepSeals(c) {
      try {
        const all = sealStore(); all[c.key] = mergeStamps(c.seals).slice(-SEAL_KEEP);
        const keys = Object.keys(all); if (keys.length > SEAL_PIECES_KEEP) { keys.sort((a, b) => ((all[a] || []).slice(-1)[0] || { at: 0 }).at - ((all[b] || []).slice(-1)[0] || { at: 0 }).at); for (const k of keys.slice(0, keys.length - SEAL_PIECES_KEEP)) delete all[k]; }
        keepSettings();
      } catch (_) { /* the stamp stands in the card and in the timeline */ }
    }
    /** An order's timeline reached the service (a host that holds it, or the fetch below): the cards of that order show the stamps it carries, at rest. */
    function feedEvents(rid, events) {
      rid = String(rid == null ? "" : rid); if (!rid || !Array.isArray(events)) return;
      const mine = events.filter(e => e && e.type === SEAL_HOW), sig = x => JSON.stringify((x || []).map(e => [e.id || "", e.at, e.by, e.lineKey]));
      if (sig(evCache.get(rid)) === sig(mine) && evCache.has(rid)) return;
      evCache.set(rid, mine);
      for (const c of cards.values()) if (ridOfCard(c) === rid) paint(c);
    }
    function loadSeals(rid, force) {
      rid = String(rid == null ? "" : rid); if (!rid || typeof d.events !== "function") return null;
      const t = Date.now(); if (!force && t - (evAsked.get(rid) || 0) < 20000) return null;
      evAsked.set(rid, t);
      let run; try { run = Promise.resolve(d.events(rid)); } catch (_) { run = Promise.resolve(null); }
      return run.then(list => { if (Array.isArray(list)) feedEvents(rid, list); else evAsked.delete(rid); }, () => { evAsked.delete(rid); });
    }
    /** MSLIST: a list that shows an approved piece which has no stamp here (approved on another computer, or before stamps existed) asks the order's timeline for it, a few times and no more: at once, then 30 s, 2 min and 10 min later
     *  (the design can reach this computer a moment before the event does); an order whose timeline never holds one (an approval from before this feature) is not asked again, so a list never polls the server. null: not asked now. */
    const evEnsure = new Map(), ENSURE_WAIT = [0, 30000, 120000, 600000];      // order number -> { n: times asked, at: when last }
    function ensureSeals(rid) {
      rid = String(rid == null ? "" : rid); if (!rid || typeof d.events !== "function") return null;
      const e = evEnsure.get(rid) || { n: 0, at: 0 }, t = Date.now();
      if (e.n >= ENSURE_WAIT.length || t - e.at < ENSURE_WAIT[e.n]) return null;
      evEnsure.set(rid, { n: e.n + 1, at: t });
      return loadSeals(rid, true);
    }
    /** Who presses Approve: { ok, by }. A page with no deps.who records no name (a test's fake service); with one, nobody signed in means the name bar, and a name bar put away means nothing happens. */
    async function whoApproves(c) {
      if (typeof d.who !== "function") return { ok: true, by: "" };
      let w; try { w = d.who(); } catch (_) { w = ""; }
      if (w && typeof w.then === "function") { c.asking = true; try { w = await w; } catch (_) { w = ""; } finally { c.asking = false; } }
      const by = String(w || "").trim(); return { ok: !!by, by };
    }
    /** The widget the approval is pressed in: the one the person pressed Approve in (the other host shows the seal at rest), else the first one on screen. */
    function pressViewOf(c) {
      const live = [...c.views].filter(v => !v.dead && v.r && v.r.seals && v.el && v.el.isConnected);
      return (c.pressView && live.includes(c.pressView) ? c.pressView : null) || live.find(v => v.el.getClientRects().length) || null;
    }
    /** Presses the stamp that was just laid (unseen) in the widget's seal row; never throws; resolves when the ink is down and the stamp has lifted. */
    async function pressSeal(c, stamp) {
      const S = SEALS(); if (!S || typeof S.press !== "function" || !stamp) return;
      const v = pressViewOf(c); if (!v) return;
      const el = [...v.r.seals.children].find(n => n.getAttribute && n.getAttribute("data-process-seal") === stamp.id);
      if (!el) return;
      try { await S.press(el); } catch (e) { if (root.console) root.console.warn("Monogram seal", e); }
    }
    /** The approval's seal: made, kept, the timeline event queued (MSDUR: at once, before the press: the stamp takes about 2.3 s, and a reload or a closed tab in that time used to leave the seal kept on this computer alone, with no permanent record anywhere; the event id is the line, the piece and the time, so it is one event whatever happens next and it changes nothing on the screen),
     *  laid unseen in every widget of the piece, pressed in the one the person used; the card says Approved afterwards (the caller paints it). */
    async function sealApproval(c, done, by, at) {
      const stamp = stampOf({ at, by, letters: done.letters, n: c.n, of: c.of });
      cardSeals(c); c.seals = mergeStamps(c.seals, [stamp]); keepSeals(c);
      // (MONOSEAL step 4, 26 Oct 2026: the timeline event is written once the ink is down, never while the stamp is still coming down; a press that fails still writes it)
      const S = SEALS();
      if (S && c.views.size) {
        c.sealNew = stamp.id; c.stamping = true; for (const v of c.views) if (v.r && v.r.seals) v.r.seals._sig = null;
        paint(c);
        try { await pressSeal(c, stamp); } finally { c.stamping = false; c.sealNew = ""; for (const v of c.views) if (v.r && v.r.seals) v.r.seals._sig = null; recordSeal(c, done, stamp); }
      } else recordSeal(c, done, stamp);
      return stamp;
    }
    /** The permanent record: one timeline event for the press (id: the line, the piece and the time, so a retry is the same event), small data: the letters, the piece, its pool id, the file. */
    function recordSeal(c, done, stamp) {
      if (typeof d.record !== "function") return;
      try {
        d.record(c.row, { orderId: ridOfCard(c), type: SEAL_HOW, id: `${c.lineKey}${c.slot ? "#" + c.slot : ""}-${stamp.at}`, at: stamp.at, by: stamp.by, station: "sorter", device: "charm-nest-1", lineKey: c.lineKey,
          transactionId: String((c.row.line && c.row.line.transactionId) || ""), text: `Monogram ${done.letters}${c.of > 1 ? " · " + pieceTag(c.n, c.of) : ""}`.slice(0, 200),
          data: { letters: done.letters, slot: c.slot || null, n: c.n, of: c.of, poolId: c.lineKey + "_" + c.n, name: done.name, ck: done.ck || "", replaced: !!done.replaced, signedIn: !!stamp.by } });
      } catch (e) { if (root.console) root.console.warn("Monogram seal: timeline", e); }
    }

    const fileNameFor = (c, letters) => (c.of > 1 ? `Monogram ${letters} - piece ${c.n}.ai` : `Monogram ${letters}.ai`);
    /** The files of one line keep the order of its pieces (the send cuts the n-th design as the n-th copy of the line): sorted among their own places in the card's list. */
    function orderFiles(e, lineKey) {
      const at = [], mine = [];
      (e.files || []).forEach((F, i) => { if (F.mono && F.mono.line === lineKey) { at.push(i); mine.push(F); } });
      mine.sort((a, b) => slotNo(a.mono.slot) - slotNo(b.mono.slot));
      at.forEach((i, k) => { e.files[i] = mine[k]; });
    }
    /** A design made before the pieces were told apart covers k pieces ("Monogram APV.ai" x 2). Before one of them is changed it is split, in place, into one design for each piece it covered (the very same file, copied: nothing
     *  already approved is made again or asked again); the piece being changed gets its new design next, and has no old one to remove. Returns the piece's own old design left to remove (null: none). */
    function detach(c, old) {
      const it = itemOf(c, true), card = it && d.CustomSheet.cardOf(it), e = card && d.CustomSheet.entries()[card.ck]; if (!e) return old;
      const F = old.file, cov = coverage(c, e, card), mine = [...cov.entries()].filter(([, v]) => v.file === F).map(([n]) => n).sort((a, b) => a - b);
      if (mine.length < 2) return old;
      if (F.state !== "ready") { F.qty = Math.max(1, (F.qty | 0) - 1); return null; }       // (a file still being read cannot be copied: it covers one piece less)
      const others = mine.filter(n => n !== c.n), at = e.files.indexOf(F), made = [];
      for (const n of others.slice(1)) made.push(Object.assign({}, F, { id: "mono-" + Date.now().toString(36) + "-" + Math.random().toString(36).slice(2, 8), name: `Monogram ${old.letters} - piece ${n}.ai`, qty: 1, mono: { line: c.lineKey, slot: "C" + n } }));
      e.files.splice(at + 1, 0, ...made);
      F.mono = { line: c.lineKey, slot: "C" + others[0] }; F.qty = 1; F.name = `Monogram ${old.letters} - piece ${others[0]}.ai`;       // (the original file stays the first of the other pieces')
      orderFiles(e, c.lineKey);
      return null;
    }

    /** A piece whose approved design is not what its card shows now (other letters, or a remake of them): Approve replaces it. */
    const dirtyOf = (c, ap) => !!(ap && !ap.sent && (readLetters(c).join("") !== ap.letters || (c.status === "ready" && c.approvedKey && c.resultKey && c.resultKey !== c.approvedKey)));
    /* ═══ MONOHOOPS (Paul, 19 Oct 2026: "The Monograms need 2 hoops per each monogram ... aligned on the same horizontal axis"). The engine puts the two hoops on every piece it makes (charm-nest-monogram.js, section 3b), so the
       card's picture shows them and every design approved from now on carries them; its file says so on the order's designs: the monogram tag { line, slot, hoops: 2 } (HOOPS.VERSION). A design ON A SHEET (or cut) is never
       touched. A design approved BEFORE the hoops and not on a sheet yet keeps its approval and gets its hoops, its letters unchanged: the piece is made again from the same letters (the same engine, the same kept
       adjustments), and when the piece made now is the piece that was approved (the same letters, and the letters' size the same as the approved file was read at, within HOOPS_SAME_MM: report.hoops.lettersMm against the
       design's wMm x hMm) its design is replaced in place by the same piece with its hoops: same name, metal, piece and quantity, no new seal, no new timeline event (the approval stands: it was of those letters).
       That happens as soon as the card has made its piece (a list thumbnail is enough), and at the latest when the order's designs are sent (CustomSheet.send asks upgradeCard first, from the Approve here or the designs
       window). A design whose letters can no longer be made the same (an alphabet changed since) is NOT changed silently: it is not sent, its card says "approve it again", and Approve then replaces it with a fresh approval. ═══ */
    const HOOPS_SAME_MM = 0.1;
    const hoopsV = () => { const H = E() && E().HOOPS; return H && H.VERSION ? H.VERSION : 0; };
    const hoopsTag = res => (res && res.report && res.report.hoops && res.report.hoops.version) || 0;
    /** An approved design not on a sheet that was made before the hoops. */
    const hoopsDue = ap => !!(ap && !ap.sent && ap.file && hoopsV() && !((+(ap.file.mono || {}).hoops || 0) >= hoopsV()));
    /** What the approved design is against the piece the card has made now: "letters" (the same letters, without the hoops: it gets them), "piece" (already this very piece, hoops and all: nothing to do),
     *  false (another piece), or null (not known yet: the piece not made, or the design not read). */
    function approvedVs(c, ap) {
      const r = c.result, h = r && r.report && r.report.hoops; if (!r || !r.ok || c.status !== "ready" || !h) return null;
      if (c.resultLetters !== ap.letters || readLetters(c).join("") !== ap.letters) return false;
      const F = ap.file, lm = h.lettersMm || [r.wMm, r.hMm], near = (w, hh) => Math.abs(F.wMm - w) <= HOOPS_SAME_MM && Math.abs(F.hMm - hh) <= HOOPS_SAME_MM;
      if (!(+F.wMm > 0 && +F.hMm > 0) || F.state !== "ready") return null;
      return h.ok && near(r.wMm, r.hMm) ? "piece" : near(lm[0], lm[1]) ? "letters" : false;
    }
    const sameAsApproved = (c, ap) => { const v = approvedVs(c, ap); return v == null ? null : !!v; };
    /** The piece's approved design must be approved again before it can have its hoops (its letters cannot be made the same any more). */
    const hoopsRedo = (c, ap) => hoopsDue(ap) && readLetters(c).join("") === ap.letters && sameAsApproved(c, ap) === false;
    /** The approved design of a piece made before the hoops, replaced by the same piece with them (see above). -> true when it was (or did not need to be). Never throws. */
    async function addHoops(c) {
      const ap = approvedFile(c, true); if (!hoopsDue(ap)) return true;
      if (c.hoopsBusy) return c.hoopsBusy;
      const run = (async () => {
        const vs = approvedVs(c, ap); if (vs === "piece") return true; if (vs !== "letters") return false;      // ("piece": the design is this piece with its hoops already, its file only lacks the tag)
        const it = itemOf(c, true); if (!it) return false;
        try { await putDesign(c, it, ap.letters, ap, ap.file.metal || lineMetal(c)); c.approvedKey = c.resultKey; } catch (e) { if (root.console) root.console.warn("Monogram hoops", e); return false; }
        try { d.changed(); } catch (_) { /* the lists follow */ }
        paint(c); return true;
      })();
      c.hoopsBusy = run; try { return await run; } finally { c.hoopsBusy = null; }
    }
    /** The card has made its piece: a design approved before the hoops gets them now (quietly, one at a time). */
    function hoopsSoon(c) { try { const ap = approvedFile(c); if (hoopsDue(ap) && approvedVs(c, ap) === "letters") addHoops(c); } catch (_) { /* at the send */ } }
    /** CustomSheet.send asks this before it sends a card: every monogram piece of the card whose approved design was made before the hoops gets them (its piece made first when the card has not made it).
     *  -> { ok, n (designs given their hoops), why (when not ok: what a person must do) } */
    async function upgradeCard(it) {
      const ck = it && d.CustomSheet.cardOf ? (d.CustomSheet.cardOf(it) || {}).ck : null; if (!ck || !hoopsV()) return { ok: true, n: 0 };
      let n = 0;
      for (const r of rowsNow()) {
        const o = itemOf(cardOf(r)), k = o && d.CustomSheet.cardOf(o); if (!k || k.ck !== ck) continue;
        for (const p of piecesOfLine(r)) {
          const c = cardOf(r, p.slot), ap = approvedFile(c, true); if (!hoopsDue(ap)) continue;
          const tag = `Monogram ${ap.letters}${c.of > 1 ? ` (${pieceTag(c.n, c.of).toLowerCase()})` : ""}`;
          if (readLetters(c).join("") !== ap.letters) return { ok: false, n, why: `${tag} has letters changed since it was approved: approve it again (it gets its two hoops then)` };
          if (sameAsApproved(c, ap) == null) { if (!c.views.size) c.quiet = true; try { await rebuild(c); } catch (_) { /* said below */ } }
          const same = sameAsApproved(c, ap);
          if (same !== true) return { ok: false, n, why: same === false ? `${tag} was approved before the hoops and cannot be made with the same letters any more: open it, check it and approve it again` : `${tag} could not be made again to add its hoops: open it and try again` };
          const had = approvedVs(c, ap) === "piece";
          if (!(await addHoops(c))) return { ok: false, n, why: `${tag}: its hoops could not be added to the approved design, try again` };
          if (!had) n++;
        }
      }
      return { ok: true, n };
    }
    /** The design of a piece written onto the order's custom card: the PDF of the card's piece, quantity 1, tagged with its line, its piece and the hoops version, on `metal`; `old` (that piece's design it replaces) goes. -> its name */
    async function putDesign(c, it, letters, old, metal) {
      const bytes = await E().toPdf(c.result, { title: "Monogram " + letters });
      const name = fileNameFor(c, letters), file = new File([bytes], name, { type: "application/illustrator" }), hv = hoopsTag(c.result);
      if (c.of > 1) {
        // PIECES: THIS piece's design, quantity 1, tagged with its piece (the order of the files is the order of the pieces: the send gives copy n to the n-th design); the design it replaces is ITS OWN, no other piece's
        if (old && old.legacy) old = detach(c, old);
        d.CustomSheet.add(it, [file], { quiet: true, qty: 1, mono: Object.assign({ line: c.lineKey, slot: c.slot }, hv ? { hoops: hv } : {}), metal: metal || undefined }); c.itemAt = 0;
        const kd = d.CustomSheet.cardOf && d.CustomSheet.cardOf(it), en = kd && d.CustomSheet.entries()[kd.ck];
        if (!en || !en.files.some(F => F.name === name && F.mono && F.mono.line === c.lineKey && F.mono.slot === c.slot)) throw new Error("the order's designs would not take it");
        orderFiles(en, c.lineKey);
      } else { d.CustomSheet.add(it, [file], { quiet: true, qty: 1, mono: Object.assign({ line: c.lineKey, slot: null }, hv ? { hoops: hv } : {}), metal: metal || undefined }); c.itemAt = 0; }     // (first, so the metal a person set carries over to the new design; MONOSEND: one piece, tagged with its line, so the send gives it to this line)
      if (old) d.CustomSheet.remove(old.ck, old.file.id);                     // (and the one it replaces goes: a monogram is one design, never two)
      { const kd2 = d.CustomSheet.cardOf && d.CustomSheet.cardOf(it), en2 = kd2 && d.CustomSheet.entries()[kd2.ck];      // (a design of a piece the line no longer has (its quantity was lowered) would be cut as an extra copy: it goes)
        if (en2) for (const F of [...(en2.files || [])]) if (F.mono && F.mono.line === c.lineKey && slotNo(F.mono.slot) > c.of) d.CustomSheet.remove(kd2.ck, F.id);
        // (MONOSEND: with no metal of its own and none a person set, the design has none: it is never given another line's metal; the designs window asks for it)
        const F0 = !metal && en2 && (en2.files || []).find(F => F.name === name && F.mono && F.mono.line === c.lineKey); if (F0) F0.metal = null; }
      return name;
    }
    async function approve(c) {
      if (c.approving || c.stamping || c.asking) return;
      // MONOSEND: a piece on a sheet is final (a second press is nothing); one approved already and not changed is not approved again: the press only puts it on the next sheet
      // (MONOHOOPS: one approved before the hoops whose letters cannot be made the same any more is approved again here: a fresh design, a fresh seal)
      { const ap = approvedFile(c, true); if (ap && ap.sent) return; if (ap && !dirtyOf(c, ap) && !hoopsRedo(c, ap)) return queuePress(c); }
      if (!c.result) return;
      if (c.status === "building" || aiBusy(c)) { c.wantApprove = true; return; }
      if (c.status !== "ready") return;
      // (MONOAPPROVE, Paul 20 Oct 2026: "if the user presses it that means they approve!!": a monogram that still breaks a rule is approved by the one press too, never asked twice)
      const it = itemOf(c, true); if (!it) { d.toast("This order has no custom card to put the monogram on", "bad", 5000); return; }
      const letters = c.resultLetters; let old = approvedFile(c, true);
      if (old && old.sent) return;
      if (old && old.letters === letters && !hoopsRedo(c, old)) return queuePress(c);
      // MONOSEAL: who presses. The signed-in name, else the name bar (a name bar put away: nothing happens, nothing is lost); the letters may have been edited while it was open
      const seq0 = c.seq, who = await whoApproves(c); if (!who.ok) return;
      if (seq0 !== c.seq || c.status !== "ready" || c.resultLetters !== letters || c.approving) return;
      old = approvedFile(c, true); if (old && old.sent) return; if (old && old.letters === letters && !hoopsRedo(c, old)) return queuePress(c, who);
      const replaced = !!old;
      // MONOSEND: the metal is the one a person set on the design it replaces, else the buyer's own (the option chosen at purchase, read into the line: spec.material, what the designs window calls "… on the order"); never another line's
      const metal = (old && old.file.metal) || lineMetal(c);
      c.approving = true; c.err = null; paint(c);
      let done = null, at = 0;
      try {
        const name = await putDesign(c, it, letters, old, metal);
        c.approved = letters; c.approvedKey = c.resultKey; c.unlocked = false; at = Date.now();
        const kard = d.CustomSheet.cardOf && d.CustomSheet.cardOf(it);
        done = { rid: String((c.row.order || {}).receiptId || ""), lineKey: c.lineKey, pieceKey: c.key, slot: c.slot, n: c.n, of: c.of, letters, name, replaced, ck: (kard && kard.ck) || (old && old.ck) || "", by: who.by, at };      // (MONOSEAL: who pressed and when: the monogram:approved event and onApproved say it too)
        if (!canSend()) d.toast(`Monogram ${letters}${c.of > 1 ? ` (${pieceTag(c.n, c.of).toLowerCase()})` : ""} is on order ${c.row.order.receiptId}'s designs`, "ok", 6000);
      } catch (e) { c.err = { kind: "save", text: "Not approved: " + String((e && e.message) || e).slice(0, 140) }; d.toast(c.err.text, "bad", 6000); }   // (the monogram stays on the card, Approve is still there to try again)
      finally {
        c.approving = false;
        // MONOSEAL: the stamp comes down BEFORE the card says Approved (the state reads as it was until the ink is down: describe() with c.stamping); never throws
        if (done) { try { await sealApproval(c, done, who.by, at); } catch (e) { if (root.console) root.console.warn("Monogram seal", e); } }
        c.stamping = false; c.sealNew = ""; paint(c);
        if (done) approvedTold(c, done);
        try { d.changed(); } catch (_) { /* the pane keeps what it drew */ }
      }
      // MONOSEND (Paul, 18 Oct 2026): the same press puts it in the nesting queue, on the next sheet of its metal, with no designs window
      if (done) await queue(c, { by: who.by, press: true });
    }

    /* ═══ MONOSEND (Paul, 18 Oct 2026, order 4200683017: "remove the 'Send to sheet' button and replace it with the same green 'Approve' button we use for the engravings ... This pop-up modal is not necessary for Monogram
       process. Once the user presses the 'Approve' button on a given Monogram order then that particular Monogram should automatically be added to the Nesting queue and be added to the next available Sheet. The Metal/Sheet
       colour would have been already chosen by the buyer"). One press: the design is made and put on the order's custom designs (as before; on the buyer's metal, one piece), the seal comes down, then the card's designs are
       sent exactly as the designs window's Send to Sheet sends them (CustomSheet.send: the recorded decision, the cloud receipt, Review.repool: the next sheet of that metal with room), with no window, no second stamp and no tour of the Review tab.
       Not sent, and said plainly on the card: a piece of the same card still waiting for its approval (the card goes when every piece has its design), a metal that cannot be read from the order (never guessed: the
       designs window opens so a person picks it), a design that cannot be cut (the window says why), a send that failed (kept for retry: Approve tries again). A piece on a sheet is final; a press while it is on its way
       is nothing; two computers are kept apart by the send's own recorded decision (one per card: the cloud refuses the second before anything is placed). ═══ */
    const pause = ms => new Promise(r => setTimeout(r, ms));
    const READ_MS = 30000;               // (the design just made is read (its outline and size) before it is sent: this long at most)
    const METAL_TXT = "The buyer’s metal could not be read from the order. Pick it in the designs window.";
    /** The buyer's metal for this line (the option they chose at purchase, read into the line), or "" when it cannot be read. */
    const lineMetal = c => { const m = c.row && c.row.spec && c.row.spec.material; return m && TONES[m] ? m : ""; };
    const canSend = () => !!(d.CustomSheet && typeof d.CustomSheet.send === "function");
    const cardEntry = (c, fresh) => { const it = itemOf(c, fresh), card = it && d.CustomSheet.cardOf(it), e = card && d.CustomSheet.entries()[card.ck]; return { it, card, e: e || null }; };
    /** The monogram pieces that share this piece's custom card, and how many have their design: { total, done } (total 0: none is waiting). */
    function waitingOf(c, card) {
      let total = 0, done = 0;
      for (const r of rowsNow()) {
        const o = r === c.row ? itemOf(c) : itemOf(cardOf(r)), k = o && d.CustomSheet.cardOf(o); if (!k || k.ck !== card.ck) continue;
        for (const p of piecesOfLine(r)) { total++; if (approvedFile(p.key === c.key ? c : cardOf(r, p.slot))) done++; }
      }
      return done < total ? { total, done } : { total: 0, done };
    }
    /** Where an approved piece stands on its way to the sheets (null: on a sheet, not approved, or a page with no send): { kind, text, can (Approve is offered to finish it), warn }. */
    function queueOf(c, ap) {
      if (!ap || ap.sent || !canSend()) return null;
      const { card, e } = cardEntry(c, false);
      if (c.queueing || (e && e.sendIntent && !e.sendError) || (card && card.busy)) return { kind: "sending", text: "Going to the next sheet…", can: false, warn: false };
      if (e && e.sendError) return { kind: "error", text: "Not on a sheet yet: " + String(e.sendError).slice(0, 140), can: true, warn: true };
      if (c.queue) return Object.assign({ can: true, warn: true }, c.queue);
      if (hoopsRedo(c, ap)) return { kind: "hoops", text: "Approved before the hoops were added, and these letters now make a different piece: check it and approve it again.", can: true, warn: true };      // (MONOHOOPS)
      if (!e || !card) return null;
      // (the other pieces first: a card goes to the sheets whole, so a metal is asked for once, when the last piece is approved, never at every piece)
      const w = waitingOf(c, card); if (w.total) return { kind: "wait", text: `Goes to the sheet once every piece is approved (${w.done} of ${w.total})`, can: false, warn: false };
      if (e.files.some(F => F.state === "ready" && !F.metal)) return { kind: "metal", text: METAL_TXT, can: true, warn: true };
      const why = d.CustomSheet.notReady ? d.CustomSheet.notReady(e) : "";
      if (why) return { kind: "blocked", text: "Not on a sheet yet: " + why, can: true, warn: true };
      return { kind: "idle", text: "Approved, not on a sheet yet.", can: true, warn: false };
    }
    /** Approve pressed on a piece approved already (before this rule, a metal picked since, a send to try again): only the queue; no new design, no new seal. */
    async function queuePress(c, who) {
      if (c.queueing || !canSend()) { paint(c); return c.queueing || false; }
      const w = who || await whoApproves(c); if (!w.ok) return false;
      return queue(c, { by: w.by, press: true });
    }
    /** Puts the approved piece's card on the sheets, once (a second call while it runs is the same run). -> true when the card is on the sheets. */
    function queue(c, o) {
      if (!canSend()) return Promise.resolve(false);
      if (c.queueing) return c.queueing;
      const run = queueRun(c, o || {}).catch(e => { c.queue = { kind: "error", text: "Not on a sheet yet: " + String((e && e.message) || e).slice(0, 140) }; return false; });
      c.queueing = run; paint(c);
      return run.then(v => { if (c.queueing === run) c.queueing = null; for (const x of cards.values()) if (x === c || x.lineKey === c.lineKey || x.views.size) paint(x); try { d.changed(); } catch (_) { /* the lists follow at their next draw */ } return v; });
    }
    /** The designs window opened for a person to finish what the order does not say: the card is drawn again when it closes (a metal picked, or the designs sent from it). */
    function openWindow(c, it) {
      if (!d.CustomSheet.open) return;
      d.CustomSheet.open(it, {});
      if (c.watching || typeof d.CustomSheet.isOpen !== "function") return;
      c.watching = true;
      const tick = () => { if (d.CustomSheet.isOpen()) { setTimeout(tick, 400); return; } c.watching = false; for (const x of cards.values()) if (x.lineKey === c.lineKey || x.views.size) paint(x); };
      setTimeout(tick, 400);
    }
    async function queueRun(c, o) {
      c.queue = null;
      const say = (t, k) => { if (o.press) d.toast(t, k || "", 6000); };
      let { it, card, e } = cardEntry(c, true);
      if (!it || !card) { c.queue = { kind: "error", text: "This order has no custom card to send the monogram from." }; return false; }
      if (e && e.sent) return true;
      for (const t0 = Date.now(); e && e.files.some(F => F.state === "reading") && Date.now() - t0 < READ_MS;) { await pause(120); ({ it, card, e } = cardEntry(c, true)); }
      if (!e || !card) { c.queue = { kind: "error", text: "The approved design is not on the order’s designs." }; return false; }
      if (e.sent) return true;
      if (!e.sendIntent) {       // (a send recorded already, from here or another tab, is finished as it was recorded)
        if (e.files.some(F => F.state === "reading")) { c.queue = { kind: "error", text: "The design is still being read. Press Approve to try again." }; return false; }
        const ap = approvedFile(c, true); if (!ap) { c.queue = { kind: "error", text: "The approved design is not on the order’s designs." }; return false; }
        const w = waitingOf(c, card); if (w.total) { say(`Monogram ${ap.letters}${c.of > 1 ? ` (${pieceTag(c.n, c.of).toLowerCase()})` : ""} is approved · it goes to the sheet once every piece is approved (${w.done} of ${w.total})`, "ok"); return false; }
        if (e.files.some(F => F.state === "ready" && !F.metal)) { say(`Order ${ridOfCard(c)}: the buyer’s metal could not be read. Pick it, then Send to Sheet`, "bad"); if (o.press) openWindow(c, it); return false; }      // (once, at the card's last piece: never guessed)
        if (d.CustomSheet.notReady && d.CustomSheet.notReady(e)) { if (o.press) openWindow(c, it); return false; }
      }
      // the designs window's own send, without the window: the same decision, receipt and placement (Review.repool: the next sheet of the design's metal); the monogram's seal was the press, so no second stamp
      await d.CustomSheet.send(it, { by: o.by || undefined, stamp: false, tour: false });
      ({ e } = cardEntry(c, true));
      if (e && e.sent) return true;
      if (!(e && e.sendError)) c.queue = { kind: "error", text: "Not on a sheet yet. Press Approve to try again." };
      return false;
    }

    /* ── drawing ── */
    /* The picture is the CUT piece: the finished monogram filled with the metal it will be cut in, one thin dark cut line round the outline and round every counter (the holes are open: the card shows through),
       centred with an even margin and scaled to the box. The line is 1 CSS pixel whatever the scale (vector-effect), so it stays a hairline and crisp on a 2x screen. */
    // the metal of the line: the one a person set on the approved design, else the order's own, else a neutral gold
    const metalOf = c => { const ap = approvedFile(c), m = (ap && ap.file && ap.file.metal) || (c.row.spec && c.row.spec.material) || ""; return TONES[m] ? m : ""; };
    function previewSvg(res, metal, o) {
      o = o || {}; const b = res.bboxPt, w = b[2] - b[0], h = b[3] - b[1], pad = Math.max(w, h) * 0.06, t = TONES[metal] || TONES.gold;
      return `<svg viewBox="${(-pad).toFixed(2)} ${(-pad).toFixed(2)} ${(w + 2 * pad).toFixed(2)} ${(h + 2 * pad).toFixed(2)}" role="img" aria-label="${esc(o.label || "The monogram as it will be cut, in " + t[2])}" preserveAspectRatio="xMidYMid meet" shape-rendering="geometricPrecision"><path d="${esc(E().svgPath(res))}" fill="${t[0]}" fill-rule="evenodd" stroke="${t[1]}" stroke-width="${o.stroke || 1.25}" stroke-linejoin="round" vector-effect="non-scaling-stroke"/></svg>`;
    }
    const slotName = i => SLOTS[i][1];
    /** One repaint of a line's state everywhere it is shown: the old pane's card, every mounted widget, the list thumbnails; then the change is told once if it changed. */
    function paint(c) {
      if (c.views.size || thumbEls.size || listeners.size) { try { paintViews(c); } catch (e) { if (root.console) root.console.warn("Monogram widget", e); } }
    }
    function approveBy(c) { if (c.status === "building" || aiBusy(c)) c.wantApprove = true; else approve(c); }
    // Esc in a letter box: the edit is taken back (the letters the card last showed), a second Esc leaves the box
    function revert(c) {
      const shown = c.resultLetters ? c.resultLetters.split("") : null, ls = readLetters(c);
      if (!shown || ls.join("") === shown.join("")) return false;
      c.draft = shown; c.hint = ""; c.wantApprove = false; schedule(c, 0); return true;
    }

    /** What a line shows before its first build: the letters of its approved design when a person changed them before approving, else the buyer's. */
    function begin(c) {
      if (c.status !== "idle") return;
      const ap = approvedFile(c);
      if (ap && !c.draft && ap.letters !== buyerLetters(c).ls.join("")) c.draft = ap.letters.split("");     // (a person changed the letters before approving: the card shows the approved ones)
      if (ap && !c.approved) c.approved = ap.letters;
    }
    /* ═══ MUW (Paul, 16 Oct 2026): the monogram WIDGET. One monogram order line, shown in the Engrave tab's detail ("detail") and in the Order details window ("compact"), in place of the Front / Back faces; the same
       state for both (a letter typed in one is in the other). The contract is /tmp/mu/CONTRACT.md (kept in the repository's plans folder by the lead). The pane above is the old way and goes when the Engrave tab has switched. ═══ */
    const widgets = new Set(), thumbEls = new Set(), listeners = new Set();
    const lineOf = row => (row && row.parentRow) || row || null;
    const isMono = row => { const r = lineOf(row); return !!(r && r.spec && r.spec.special && r.spec.special.monogram && !GONE.includes(r.state)); };
    const lineKeyOf = key => String(key == null ? "" : key).replace(/#C\d{1,3}$/, "");       // (a piece key "<lineKey>#C2" names its line too)
    const rowByKey = key => { const k = lineKeyOf(key); return (d.rows() || []).find(r => r && r.key === k) || null; };
    /** Which piece of its line a call asks for: a slot ("C2"), a piece key, a piece number, or the piece key given as the line key. */
    const askOf = o => (o && typeof o === "object" ? (o.slot != null ? o.slot : o.pieceKey != null ? o.pieceKey : /#C\d/.test(String(o.lineKey || "")) ? o.lineKey : o.piece) : o);
    const rowOf = o => {
      const r = lineOf(o.row); if (r) return r;
      if (o.lineKey) return rowByKey(o.lineKey);
      if (o.rid != null) { const ls = rowsNow().filter(x => String(x.order.receiptId) === String(o.rid)); return ls.length === 1 ? ls[0] : null; }
      return null;
    };
    const mm1 = x => (+x).toFixed(1), mmTxt = x => (Number.isInteger(+x) ? String(+x) : (+x).toFixed(1));
    const LABEL = { reading: "Reading the letters", letters: "Letters to confirm", building: "Making…", ready: "Ready to approve", check: "Ready to approve", approved: "Approved", sent: "On the sheets", error: "Needs a look" };

    /** Everything a person (or a test) can know about one line, in one object (the contract's state()). Pure reading: it paints nothing and asks nothing. */
    function describe(c) {
      const ls = readLetters(c), ap = c.stamping ? null : approvedFile(c), sent = !!(ap && ap.sent), buyer = buyerLetters(c), said = wordsFor(c.row, c.n);   // (MONOSEAL: while the stamp comes down the monogram is not yet said to be approved: nothing moves in the page's lists until the ink is down)
      const dirty = !!(ap && !sent && (ls.join("") !== ap.letters || (c.status === "ready" && c.approvedKey && c.resultKey && c.resultKey !== c.approvedKey)));
      const settled = !!(ap && !dirty && !sent);
      const busy = c.approving ? "saving" : aiBusy(c) ? "ai" : c.fit ? "fitting" : c.status === "building" ? "building" : "";
      const left = flaggedOf(c), ok = c.status === "ready" && !!c.result;
      const state = sent ? "sent" : settled ? "approved" : busy ? "building" : ok ? (left.length ? "check" : "ready") : c.status === "error" || c.status === "setup" ? "error" : c.status === "words" ? "letters" : "reading";
      const metal = metalOf(c), r = c.result && c.status !== "error" ? c.result : null, f = fitOf(c), rf = r && r.report && r.report.fit;
      let sizeMm = null; if (r && +r.wMm > 0) sizeMm = [+r.wMm, +r.hMm]; else if (ap && ap.file && +ap.file.wMm > 0) sizeMm = [+ap.file.wMm, +ap.file.hMm];
      // a real failure (a missing letter, a file that did not load, letters that cannot join, no alphabets, an approval that did not save)
      let error = null;
      if (c.status === "error" && c.err) error = c.err;
      else if (c.status === "setup") { const abc = alphabets(); error = { kind: "setup", text: !abc.fams.length ? "The monogram alphabets are not in the charm library yet." : chooseText(abc), help: !abc.fams.length ? "Add one design per letter in Master: Monogram_Left_A to Z, Monogram_Mid_A to Z and Monogram_Right_A to Z." : "Pick them once; the choice is kept." }; }
      else if (c.status === "ready" && c.err && c.err.kind === "save") error = c.err;
      let message = settled || sent ? null : error;
      if (!message && state === "letters" && !c.draft) {
        const why = buyer.why ? cap(buyer.why) : "";
        message = { kind: "letters", text: said ? `Three letters could not be read from the order${why ? ": " + why.charAt(0).toLowerCase() + why.slice(1) : ""}.` : "The order does not give any letters.", help: (said ? `The buyer wrote “${said}”. ` : "") + "Type the three letters below." };
      }
      const a = c.ai; let assembly = { status: "", why: "", limit: "", detail: "" };
      if (ok && !busy) {
        if (left.length) assembly = { status: a && (a.status === "failed" || a.status === "limited" || a.status === "off") ? a.status : "needs", why: "", limit: limitText(left), detail: a && a.status === "failed" ? "The AI could not help this time, so the automatic layout is kept." : a && a.status === "limited" ? "The AI has had its 3 tries for this order today." : "" };
        else if (a && a.status === "assembled") assembly = { status: "assembled", why: a.why || "adjusted the letters.", limit: "", detail: "" };
      }
      const warnings = ok ? (((r.report && r.report.warnings) || []).map(w => cap(String(w))).filter(Boolean)) : [];
      return {
        state, busy, round: busy === "ai" && c.ai ? c.ai.round | 0 : 0, letters: ls, lettersOk: lettersOk(ls),
        piece: { key: c.key, lineKey: c.lineKey, slot: c.slot, n: c.n, of: c.of, tag: pieceTag(c.n, c.of), poolId: c.lineKey + "_" + c.n },
        buyer: { words: said, letters: buyer.ls.every(Boolean) ? buyer.ls.slice() : null, why: buyer.why || "" },
        seals: cardSeals(c) && sealsFor(c), stamping: !!c.stamping, queue: settled ? queueOf(c, ap) : null,
        canApproveBase: ok && !busy && !sent && !settled && !c.stamping, approved: ap ? { letters: ap.letters, name: ap.file.name, sent } : null, dirty, locked: sent || (settled && !c.unlocked), unlocked: !!c.unlocked,
        metal, sizeMm, fit: f ? { maxWidthMm: f.maxWidthMm, scale: rf && rf.scale != null ? rf.scale : 1, applied: !!(rf && rf.applied) } : null,
        assembly, notes: warnings, hint: c.hint || "", error, message, prog: c.prog ? { done: c.prog.done, total: c.prog.total } : null
      };
    }
    const statusOf = (c, st) => ({ state: st.state, label: st.state === "building" && st.busy === "ai" ? "Assembling…" : LABEL[st.state] || "", letters: st.lettersOk ? st.letters.join("") : "", approved: !!st.approved, dirty: !!st.dirty, sent: st.state === "sent", rid: String((c.row.order || {}).receiptId || ""), lineKey: c.lineKey, pieceKey: c.key, slot: c.slot, n: c.n, of: c.of, seals: st.seals.length, queue: st.queue ? st.queue.kind : "" });
    const gateWhy = v => { try { return v.o.gate ? String(v.o.gate() || "") : ""; } catch (_) { return ""; } };
    function stateFor(c, v, st) {
      const gate = gateWhy(v), out = Object.assign({}, st, { canApprove: st.canApproveBase && !gate, approveWhy: gate || (st.canApproveBase ? "" : st.state === "approved" || st.state === "sent" ? "Already approved" : st.busy ? "Making the monogram…" : "Type the three letters first"), rid: String((c.row.order || {}).receiptId || ""), lineKey: c.lineKey, pieceKey: c.key, slot: c.slot, n: c.n, of: c.of });
      delete out.canApproveBase; return out;
    }

    /* ── telling the page: one call when a line's state really changed ── */
    function tellChange(c, st) {
      const sig = JSON.stringify([st.state, st.busy, st.letters, st.approved && st.approved.letters, st.approved && st.approved.sent, st.dirty, st.locked, st.canApproveBase, st.error && st.error.kind, st.assembly.status, st.seals.map(x => x.id), st.stamping, st.queue && st.queue.kind]);
      if (sig === c.sig) return; c.sig = sig;
      const status = statusOf(c, st);
      for (const fn of [...listeners]) { try { fn(c.row, status); } catch (_) { /* a listener's trouble is its own */ } }
      for (const v of [...c.views]) {
        const s = stateFor(c, v, st);
        try { if (v.o.onChange) v.o.onChange(s); } catch (_) { /* the host's trouble is its own */ }
        try { v.host.dispatchEvent(new root.CustomEvent("monogram:change", { bubbles: true, detail: s })); } catch (_) { /* no events in this page */ }
      }
    }
    function approvedTold(c, info) {
      for (const v of [...c.views]) {
        try { if (v.o.onApproved) v.o.onApproved(info); } catch (_) { /* the host's trouble is its own */ }
        try { v.host.dispatchEvent(new root.CustomEvent("monogram:approved", { bubbles: true, detail: info })); } catch (_) { /* no events in this page */ }
      }
    }

    /* ── the widget's markup: minimal, built once, then only its text and attributes change (a letter box keeps its caret) ── */
    const ICON_SWAP = `<svg viewBox="0 0 16 16" width="13" height="13" aria-hidden="true"><path d="M3 5.5h10M10.5 3l2.5 2.5-2.5 2.5M13 10.5H3M5.5 8 3 10.5 5.5 13" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>`;
    const ICON_ROT = `<svg viewBox="0 0 16 16" width="13" height="13" aria-hidden="true"><path d="M13 8a5 5 0 1 1-1.7-3.8M13 2.6v3.1h-3.1" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>`;
    const ICON_OK = `<svg viewBox="0 0 16 16" width="13" height="13" aria-hidden="true"><path d="M3.2 8.4 6.5 11.5 12.8 4.8" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></svg>`;
    function viewHtml(mode) {
      const small = mode === "compact" ? " sm" : "";
      return `<div class="mwBody"><div class="mwPic" data-mw-pic aria-busy="false"><div class="mwArt"></div>`
        + `<div class="mwBusy" data-mw-spin role="status" hidden><i aria-hidden="true"></i><span></span><em class="mwBar" data-mw-bar role="progressbar" aria-valuemin="0" aria-valuemax="100"><b></b></em></div></div>`
        + `<div class="mwSide"><div class="mwLtrs" role="group" aria-label="The three letters">`
        + SLOTS.map(([slot, label], i) => (i ? `<button type="button" class="mwSw" data-mw-swap="${i - 1}" aria-label="Swap the ${slotName(i - 1)} and ${label} letters" title="Swap ${slotName(i - 1)} and ${label}">${ICON_SWAP}</button>` : "")
          + `<label class="mwLt" data-slot="${slot}"><input data-mw-letter="${i}" maxlength="2" inputmode="text" autocapitalize="characters" autocomplete="off" autocorrect="off" spellcheck="false" aria-label="${label} letter"><span>${label}</span></label>`).join("")
        + `<button type="button" class="mwSw mwRot" data-mw-rotate aria-label="Move the first letter to the end" title="Move the first letter to the end">${ICON_ROT}</button></div>`
        + `<p class="mwSays" data-mw-says></p><p class="mwLine" data-mw-size></p>`
        + `<p class="mwLine mwAsm" data-mw-assembly hidden><span></span></p>`
        + `<p class="mwLine mwNote" data-mw-note hidden></p><div class="mwMsg" data-mw-msg hidden></div>`
        + `<span class="mwSr" role="status" aria-live="polite" data-mw-live></span><div class="mwAct"><button type="button" class="btn sage${small}" data-mw-approve disabled>Approve</button><span class="mwOk" data-mw-approved hidden>${ICON_OK}<span></span></span>`
        + `<button type="button" class="btn ghost${small}" data-mw-change hidden>Change</button></div></div></div>`;      // (MONOSEND, Paul 18 Oct 2026: no "Send to Sheet →" link any more: Approve puts the monogram on the next sheet)
    }
    function msgHtml(m) {
      if (!m) return "";
      let sel = "";
      if (m.kind === "setup") {
        const abc = alphabets();
        if (abc.fams.length) sel = `<span class="mwAbcs">` + SLOTS.map(([slot, label]) => `<label><span>${label}</span><select data-mw-abc="${slot}" aria-label="${label} alphabet"><option value="">${abc.by[slot] === "name" && abc[slot] ? "Automatic · " + esc(abc[slot].name) : "Automatic"}</option>${abc.fams.map(f => `<option value="${esc(f.base)}"${abc.by[slot] === "saved" && abc[slot] && abc[slot].base === f.base ? " selected" : ""}>${esc(f.name)} · ${f.count} letters</option>`).join("")}</select></label>`).join("") + `</span>`;
      }
      return `<span class="mwMsgTx"${m.kind === "letters" ? "" : ' role="alert"'}>${esc(m.text)}</span>${m.help ? `<small>${esc(m.help)}</small>` : ""}${sel}${m.kind === "load" ? `<button type="button" class="btn ghost sm" data-mw-retry>Try again</button>` : ""}`;
    }
    const picHint = (c, st) => {
      if (st.approved && !st.dirty && !c.result) return `Monogram ${st.approved.letters} is on the order’s designs`;
      if (st.state === "letters") return "Type the three letters";
      if (c.status === "setup") return "Choose the alphabets";
      if (c.status === "error") return "No picture yet";
      return "";
    };
    const pictures = new WeakMap();      // (a result's picture per metal: drawn once, however many widgets show it and however often they repaint)
    function pictureHtml(res, metal) { let m = pictures.get(res); if (!m) pictures.set(res, m = {}); return m[metal || ""] || (m[metal || ""] = previewSvg(res, metal)); }
    function fillView(c, v, st) {
      const el = v.el, R = v.r; if (!el || !R) return;
      const gate = gateWhy(v), can = st.canApproveBase && !gate, settled = !!(st.approved && !st.dirty && !st.approved.sent), sent = !!(st.approved && st.approved.sent);
      el.dataset.state = st.state; el.dataset.busy = st.busy; el.dataset.rid = String((c.row.order || {}).receiptId || ""); el.dataset.key = c.key;
      el.dataset.line = c.lineKey; el.dataset.slot = c.slot || ""; el.dataset.piece = String(c.n); el.dataset.of = String(c.of);
      { const lab = c.of > 1 ? `Monogram, ${pieceTag(c.n, c.of).toLowerCase()}` : "Monogram"; if (el.getAttribute("aria-label") !== lab) el.setAttribute("aria-label", lab); }
      // the picture: the geometry that will be cut, in the line's metal (a new one being made: the old stays, faded, so nothing jumps)
      let art = "";
      if (c.result && (c.status === "ready" || c.status === "building")) art = pictureHtml(c.result, st.metal);
      else { const h = picHint(c, st); art = h ? `<div class="mwHint">${esc(h)}</div>` : ""; }
      if (R.art._shown !== art) { R.art.innerHTML = art; R.art._shown = art; }
      const busy = !!st.busy || st.state === "reading";
      R.pic.classList.toggle("busy", busy); R.pic.setAttribute("aria-busy", busy ? "true" : "false"); R.spin.hidden = !busy; R.spin.dataset.busy = st.busy;
      const tx = st.busy === "saving" ? "Saving…" : st.busy === "ai" ? (st.round ? `Assembling with AI · round ${st.round} of ${AI_ROUNDS}…` : "Checking the monogram…") : st.busy === "fitting" ? "Fitting to the size…" : st.state === "reading" ? "Reading the letters…" : "Making the monogram…";
      if (R.spinTx.textContent !== tx) R.spinTx.textContent = tx;
      const pct = st.busy === "building" && st.prog ? Math.round(100 * st.prog.done / st.prog.total) : st.busy === "ai" && st.round ? Math.round(100 * (st.round - 0.5) / AI_ROUNDS) : null;
      R.bar.classList.toggle("ind", pct == null); if (pct != null) { R.bar.firstChild.style.width = pct + "%"; R.bar.setAttribute("aria-valuenow", String(pct)); } else { R.bar.firstChild.style.width = ""; R.bar.removeAttribute("aria-valuenow"); }
      // the three boxes (never touched while a person is typing in one)
      R.ins.forEach((inp, i) => { if (doc.activeElement !== inp && inp.value !== (st.letters[i] || "")) inp.value = st.letters[i] || ""; inp.disabled = st.locked || st.stamping; inp.setAttribute("aria-invalid", st.state === "letters" && !st.letters[i] ? "true" : "false"); });
      R.sw.forEach(b => { b.disabled = st.locked || st.stamping; });
      // what the buyer wrote (compact: only when it needs a look)
      const seen = st.buyer.letters ? spaced(st.buyer.letters.join("")) : st.buyer.words;
      // (MONOLETTERS, Paul 17 Oct 2026: "the system did not use the buyer's requested letters": a card starts with the buyer's letters in the order they wrote them; one a person turned or swapped (the arrow moves the first letter to the end: A P V -> P V A) says what the buyer wrote, with one press to put them back)
      const differs = !!(st.buyer.letters && st.buyer.letters.join("") !== st.letters.join(""));
      const says = seen ? `The buyer wrote <b>${esc(seen)}</b>${differs && !st.locked ? ` · <button type="button" class="mwLink" data-mw-buyer title="Put the buyer's letters back, in the order they wrote them">Use these</button>` : ""}` : "The buyer wrote nothing";
      if (R.says._shown !== says) { R.says.innerHTML = says; R.says._shown = says; }
      R.says.hidden = st.message && st.message.kind === "letters" ? true : st.state !== "letters" && !(st.buyer.letters && st.buyer.letters.join("") !== st.letters.join(""));      // (an unreadable order's message says what the buyer wrote already; the order's own words stand in the strip above the widget, so the line is only for letters that differ from them)
      // ONE size line
      const seg = []; if (st.sizeMm) { seg.push(`${mm1(st.sizeMm[0])} × ${mm1(st.sizeMm[1])} mm`); if (st.metal) seg.push(TONES[st.metal][2]); }
      if (st.fit && st.sizeMm) seg.push(st.fit.applied ? `scaled to the buyer’s ${mmTxt(st.fit.maxWidthMm)} mm` : `within the buyer’s ${mmTxt(st.fit.maxWidthMm)} mm`);
      const sz = seg.join(" · "); if (R.size.textContent !== sz) R.size.textContent = sz; R.size.hidden = !sz;
      // ONE calm line about the assembly, and one quiet note (a letter refused, the first thing the engine warns about, how to replace an approved monogram)
      // (MONOAPPROVE, Paul 20 Oct 2026: "Remove this red text and Improve again link from the UI completely": a word that still breaks a rule says nothing here, and the engine's warning does not take the line's place)
      const as = st.assembly, line = as.status === "assembled" ? `Assembled with AI: ${as.why}` : "", flagged = !!as.status && as.status !== "assembled";
      const unl = st.unlocked && !st.dirty && st.approved && !st.approved.sent ? "Change a letter to replace the approved monogram." : "";
      const n0 = st.notes[0] ? plainNote(st.notes[0]) : "";
      // MONOSEND: an approved piece not on a sheet says why (a metal to pick, a send to retry, the other pieces still to approve) before the engine's warning; "on its way" and "not sent yet" only when there is no warning
      const qa = settled && st.queue ? st.queue : null, qn = qa && (qa.warn || qa.kind === "wait" || !n0) ? qa : null;
      const note = st.hint || unl || (qn ? qn.text : "") || (line || flagged ? "" : n0) || "", noteWarn = !!st.hint || (!!qn && note === qn.text && qn.warn) || (!!n0 && note === n0);
      if (v.mode === "compact") {      // compact: the Order window must not jump, so the assembly line, the hint and the warning share ONE single-line slot (the full text is its title) and says / size / slot keep their height when empty
        const pick = st.hint ? { tx: st.hint, warn: true, status: "" } : qn && qn.warn ? { tx: qn.text, warn: true, status: "" } : line ? { tx: line, warn: false, status: as.status } : unl ? { tx: unl, warn: false, status: "" } : qn ? { tx: qn.text, warn: false, status: "" } : n0 && !flagged ? { tx: n0, warn: true, status: "" } : null;
        R.asm.hidden = !pick; R.asm.dataset.status = pick ? pick.status : ""; R.asm.classList.toggle("warn", !!pick && pick.warn);
        const tx = pick ? pick.tx : ""; if (R.asmTx.textContent !== tx) R.asmTx.textContent = tx; R.asm.title = pick ? (pick.tx + (st.notes.length > 1 && !pick.status ? "\n" + st.notes.slice(1).map(plainNote).join("\n") : "")) : "";
        R.note.hidden = true;
        R.size.title = sz; R.says.title = R.says.textContent;
      } else {
        R.asm.hidden = !line; R.asm.dataset.status = as.status || ""; if (R.asmTx.textContent !== line) R.asmTx.textContent = line; R.asm.classList.remove("warn");
        R.note.hidden = !note; if (R.note.textContent !== note) R.note.textContent = note; R.note.classList.toggle("warn", noteWarn); R.note.title = st.notes.length > 1 ? st.notes.map(plainNote).join("\n") : "";
      }
      // a message that needs a person
      const msg = st.message, mh = msg ? msg.kind + "|" + msg.text + "|" + (msg.help || "") : "";
      R.msg.hidden = !msg; R.msg.dataset.kind = msg ? msg.kind : ""; if (msg) el.setAttribute("data-msg", ""); else el.removeAttribute("data-msg");      // (a message replaces the compact widget's lines: the one allowed growth)
      if (R.msg._shown !== mh) { R.msg.innerHTML = msgHtml(msg); R.msg._shown = mh; }
      // the actions: Approve, or Approved with a way to change it
      // (MONOSEND: Approve approves AND puts the monogram on the next sheet of the buyer's metal; an approved piece that is not on a sheet yet keeps it, to finish that: pick the metal, retry the send)
      const offer = !!(settled && qa && qa.can && !st.stamping);
      R.approve.hidden = sent || (settled && !offer); R.approve.disabled = offer ? !!gate : !can; R.approve.textContent = st.stamping ? "Approved" : offer ? "Approve" : st.dirty ? "Replace approved" : "Approve";
      R.approve.title = st.stamping ? "" : offer ? (gate || (qa.kind === "metal" || qa.kind === "blocked" ? "Open the order’s designs to pick what is missing" : "Put this monogram on the next sheet")) : can ? "Approve this monogram and put it on the next sheet" : (gate || (busy ? "Making the monogram…" : st.state === "letters" || st.state === "error" ? "Type the three letters first" : ""));
      el.dataset.queue = qa ? qa.kind : "";
      // (compact, an approved piece not on a sheet: Approve and Change share the one row with nothing else, so the Order window keeps its height; the note line and the seal say it is approved)
      R.ok.hidden = !(settled || sent) || (offer && v.mode === "compact"); const okTx = (sent ? "On the sheet" : "Approved") + (st.approved ? " · Monogram " + st.approved.letters : ""); if (R.okTx.textContent !== okTx) R.okTx.textContent = okTx;
      { const lv = sent ? "On the sheet." : st.stamping ? "Approved." : settled ? (qa && qa.kind === "sending" ? "Approved. Going to the next sheet." : qa ? "Approved. " + qa.text : "Approved.") : busy || st.message ? "" : st.state === "ready" || st.state === "check" ? "The monogram is ready." : ""; if (R.live.textContent !== lv) R.live.textContent = lv; }   // (for a screen reader: what the picture and the buttons show)
      R.change.hidden = !(settled && st.locked) || !!(qa && qa.kind === "sending");      // (a piece on its way to the sheet cannot be changed: its design is being sent)
      paintSeals(c, v, st);
    }
    /* MONOSEAL: the widget's seal row (`.mwSeal`): every stamp of the piece, oldest first, the newest on top. Built again only when the list of stamps changes (an element
       mid-press is never touched); the stamp just made waits unseen (`pending`) in the widget the approval is pressed in, and is pressed by pressSeal. */
    function paintSeals(c, v, st) {
      const R = v.r, row = R && R.seals, S = SEALS(); if (!row) return;
      const list = S ? st.seals : [], sig = list.map(x => x.id).join("|");
      if (row._sig === sig) return;
      row._sig = sig;
      const pend = c.sealNew && c.stamping && pressViewOf(c) === v ? c.sealNew : "";
      row.innerHTML = list.map(x => S.html(x, S.BASE_SIZE, "monogramSeal" + (x.id === pend ? " pending" : ""))).join("");
      row.dataset.sealCount = String(list.length); row.hidden = !list.length;
      if (list.length && S.fit) { try { S.fit(row); } catch (_) { /* the stylesheet's size stands */ } }
    }
    function paintViews(c) {
      const st = describe(c);
      for (const v of [...c.views]) if (!v.dead) fillView(c, v, st);
      paintThumbs(c, st);
      tellChange(c, st);
    }

    /* ── the widget's input: the same rules as the pane's boxes (one capital letter per box, a digit or an emoji is refused calmly) ── */
    function editTo(c, ls) {
      c.draft = ls; c.wantApprove = false;
      if (lettersOk(ls)) schedule(c); else { clearTimeout(c.timer); c.seq++; c.status = "words"; c.result = null; c.err = null; paint(c); }
    }
    function typeInto(v, i, t, inputType) {
      const c = v.c, typed = String(t.value || ""), l = typed.normalize("NFD").replace(/[^A-Za-z]/g, "").slice(-1).toUpperCase();
      if (typed && !l) { t.value = readLetters(c)[i] || ""; t.select(); c.hint = "Letters A to Z only"; paint(c); return; }
      c.hint = ""; t.value = l; const ls = readLetters(c); ls[i] = l; editTo(c, ls);
      if (l && i < 2 && inputType !== "deleteContentBackward") { const nx = v.r.ins[i + 1]; if (nx && !nx.disabled) nx.focus(); }
    }
    const viewApprove = v => { if (v.dead || gateWhy(v)) return; v.c.pressView = v; approveBy(v.c); };      // (MONOSEAL: the stamp is pressed in the widget the person pressed Approve in)
    function wireView(v) {
      const el = v.el;
      el.addEventListener("input", ev => { const t = ev.target; if (t.matches && t.matches("[data-mw-letter]")) typeInto(v, +t.getAttribute("data-mw-letter"), t, ev.inputType); });
      el.addEventListener("paste", ev => {
        const t = ev.target; if (!t.matches || !t.matches("[data-mw-letter]")) return;
        const c = v.c, text = (ev.clipboardData && ev.clipboardData.getData("text")) || "", r = E().lettersOf(text);
        if (!r.ok) { if (String(text).replace(/\s+/g, "").length > 1) { ev.preventDefault(); c.hint = cap(r.why || "Paste the three letters"); paint(c); } return; }
        ev.preventDefault(); c.hint = ""; editTo(c, r.letters.slice()); v.r.ins[2].focus();
      });
      el.addEventListener("keydown", ev => {
        const t = ev.target; if (!t.matches || !t.matches("[data-mw-letter]")) return;
        const c = v.c, i = +t.getAttribute("data-mw-letter"), ins = v.r.ins;
        if (ev.key === "Enter") { ev.preventDefault(); if (c.status === "ready" || c.status === "building") viewApprove(v); }
        else if (ev.key === "Escape") { if (revert(c)) ev.stopPropagation(); else t.blur(); }
        else if (ev.key === "Backspace" && !t.value && i > 0) { ev.preventDefault(); ins[i - 1].focus(); }
        else if (ev.key === "ArrowLeft" && i > 0 && t.selectionStart === 0) { ev.preventDefault(); ins[i - 1].focus(); }
        else if (ev.key === "ArrowRight" && i < 2 && t.selectionEnd === t.value.length) { ev.preventDefault(); ins[i + 1].focus(); }
      });
      el.addEventListener("focusin", ev => { if (ev.target.matches && ev.target.matches("[data-mw-letter]")) ev.target.select(); });
      el.addEventListener("focusout", ev => {       // (an approved monogram opened with Change is locked again when nothing was changed and the person leaves it)
        const c = v.c; if (!c.unlocked || el.contains(ev.relatedTarget) || Date.now() - (c.unlockedAt || 0) < 400) return;
        if (!describe(c).dirty) { c.unlocked = false; paint(c); }
      });
      el.addEventListener("change", ev => { const s = ev.target.closest && ev.target.closest("[data-mw-abc]"); if (s) setAlphabet(s.dataset.mwAbc, s.value); });
      el.addEventListener("click", ev => {
        const c = v.c, hit = sel => ev.target.closest && ev.target.closest(sel);
        const sw = hit("[data-mw-swap]");
        if (sw) { const i = +sw.dataset.mwSwap, ls = readLetters(c); [ls[i], ls[i + 1]] = [ls[i + 1], ls[i]]; c.hint = ""; editTo(c, ls); return; }
        if (hit("[data-mw-rotate]")) { const ls = readLetters(c); c.hint = ""; editTo(c, [ls[1], ls[2], ls[0]]); return; }
        if (hit("[data-mw-approve]")) viewApprove(v);
        else if (hit("[data-mw-buyer]")) { c.hint = ""; editTo(c, buyerLetters(c).ls); }
        else if (hit("[data-mw-change]")) { c.unlocked = true; c.unlockedAt = Date.now(); paint(c); if (v.r.ins[0]) v.r.ins[0].focus(); }
        else if (hit("[data-mw-retry]")) schedule(c, 0);
      });
    }
    function buildEl(v) {
      const host = v.host; host.replaceChildren();
      const el = doc.createElement("section"); el.className = "mw"; el.dataset.mode = v.mode; el.setAttribute("aria-label", "Monogram");
      el.innerHTML = viewHtml(v.mode); v.el = el;
      const q = s => el.querySelector(s);
      v.r = { pic: q("[data-mw-pic]"), art: q(".mwArt"), spin: q("[data-mw-spin]"), spinTx: q("[data-mw-spin] span"), bar: q("[data-mw-bar]"), ins: [...el.querySelectorAll("[data-mw-letter]")], sw: [...el.querySelectorAll(".mwSw")], says: q("[data-mw-says]"), size: q("[data-mw-size]"),
        asm: q("[data-mw-assembly]"), asmTx: q("[data-mw-assembly] > span"), note: q("[data-mw-note]"), msg: q("[data-mw-msg]"), approve: q("[data-mw-approve]"), ok: q("[data-mw-approved]"), okTx: q("[data-mw-approved] > span"), change: q("[data-mw-change]"), live: q("[data-mw-live]") };
      // MONOSEAL: the seal row. The detail widget holds it in its action row beside the Approve button (in the flow, as the Engrave card's seals rest beside theirs); the compact one (the Order window's `.engMono` card) holds it as the
      // host's next child, so the card's stylesheet can lay it across the card's top-right corner as it lays the engraving cards' seals (HDRSEAL). Empty and hidden until a stamp exists.
      const sr = doc.createElement("span"); sr.className = "sealRow mwSeal"; sr.hidden = true; sr.setAttribute("data-seal-group", ""); sr.dataset.sealCount = "0"; sr.setAttribute("role", "group"); sr.setAttribute("aria-label", "Monogram approval history"); sr.dataset.testid = "mw-seals";
      v.r.seals = sr; sr._sig = null;
      wireView(v); host.append(el);
      if (v.mode === "compact") host.append(sr); else v.r.ok.after(sr);
    }
    /** Points a widget at a line (and builds what it shows): a line never made is made now; one made quietly for a thumbnail gets its AI check now that a person looks at it. */
    function attach(v, row, ask) {
      const c = cardOf(row, ask); if (v.c && v.c !== c) v.c.views.delete(v);
      v.c = c; c.views.add(v); c.quiet = false;
      if (!v.el) buildEl(v);
      begin(c); pullEvents(v, true);
      if (c.status === "idle") schedule(c, 0);
      else if (c.status === "ready" && c.resultKey && c.aiKey !== c.resultKey && !aiBusy(c)) { paint(c); assist(c, c.resultKey, c.resultLetters.split(""), alphabets(), c.seq); }
      else paint(c);
    }
    /** MONOSEAL: the order's timeline stamps for a widget: from the host that holds them (opts.events: a function returning the order's events, the Order window's), else asked once (and again after 20 s at most) of deps.events. */
    function pullEvents(v, first) {
      const c = v.c; if (!c) return;
      if (typeof v.o.events === "function") { let evs = null; try { evs = v.o.events(); } catch (_) { evs = null; } if (Array.isArray(evs)) feedEvents(ridOfCard(c), evs); }
      else if (first) loadSeals(ridOfCard(c));
    }
    function mountLine(host, o) {
      o = o || {}; if (!host) return null;
      const row = rowOf(o); if (!row || !isMono(row)) return null;
      const ask0 = askOf(o);
      if (host._mw && host._mw.destroy) host._mw.destroy();
      const v = { host, o: Object.assign({}, o), c: null, el: null, r: null, dead: false, mode: o.mode === "compact" ? "compact" : "detail" };
      const setOpts = () => { v.mode = v.o.mode === "compact" ? "compact" : "detail"; };
      setOpts();
      const api = {
        state: () => stateFor(v.c, v, describe(v.c)),
        refresh() {
          if (v.dead) return;
          const row2 = rowByKey(v.c.lineKey) || v.c.row; cardOf(row2, v.c.slot); pullEvents(v, false);
          const k = abcKey(alphabets());
          if (v.c.abcSig && v.c.abcSig !== k && v.c.status !== "idle" && v.c.status !== "building") schedule(v.c, 0); else paint(v.c);      // (the library gained or lost an alphabet: the monogram follows)
        },
        update(ctx) {
          if (v.dead) return; ctx = ctx || {};
          const was = v.mode, key = v.c.key; Object.assign(v.o, ctx); setOpts();
          if (("lineKey" in ctx || "rid" in ctx) && !("row" in ctx)) v.o.row = null;
          const row2 = ctx.row || ctx.lineKey || ctx.rid != null ? rowOf(v.o) : null, ask2 = "slot" in ctx || "pieceKey" in ctx || "piece" in ctx || /#C\d/.test(String(ctx.lineKey || "")) ? askOf(ctx) : (row2 && row2.key === v.c.lineKey ? v.c.slot : undefined);
          if (v.mode !== was) { v.el = null; v.r = null; }
          if (row2 && isMono(row2) && (slotKey(row2.key, slotFor(row2, ask2)) !== key || !v.el)) attach(v, row2, ask2); else if (!v.el) attach(v, v.c.row, v.c.slot); else paint(v.c);
        },
        destroy() {
          if (v.dead) return; v.dead = true; if (v.c) v.c.views.delete(v); widgets.delete(api);
          if (v.host._mw === api) v.host._mw = null; if (v.r && v.r.seals) v.r.seals.remove(); if (v.el) v.el.remove(); v.el = null; v.r = null;
        },
        focus() { const ins = v.r && v.r.ins, t = ins && (ins.find(x => !x.value && !x.disabled) || ins.find(x => !x.disabled)); if (t) { try { t.focus(); } catch (_) { /* no layout */ } } },
        approve() { viewApprove(v); },
        setLetters(ls) { const a = Array.from({ length: 3 }, (_, i) => String((ls || [])[i] || "").replace(/[^A-Za-z]/g, "").slice(0, 1).toUpperCase()); v.c.hint = ""; editTo(v.c, a); }
      };
      // (el, mode and lineKey are not enumerable: a page that hands the widget to a test or a logger never serialises the DOM)
      Object.defineProperties(api, { el: { get: () => v.el }, mode: { get: () => v.mode }, lineKey: { get: () => v.c && v.c.lineKey }, pieceKey: { get: () => v.c && v.c.key }, slot: { get: () => v.c && v.c.slot } });
      attach(v, row, ask0); v.host._mw = api; widgets.add(api);
      return api;
    }

    /* ── the list thumbnail of a monogram line (the Engrave piece card; replaces the Front / Back cells): the cut piece in the line's metal, small and still ── */
    const qList = []; let qBusy = false;
    function ensureQuiet(c) { if (c.status !== "idle" || c.queued) return; c.queued = true; qList.push(c); Promise.resolve().then(pump); }
    async function pump() {
      if (qBusy) return; qBusy = true;
      try {
        while (qList.length) {
          const c = qList.shift(); c.queued = false; if (c.status !== "idle") continue;
          c.quiet = !c.views.size; begin(c); markBuilding(c);
          try { await rebuild(c); } catch (_) { /* the line shows its letters */ }
        }
      } finally { qBusy = false; }
    }
    function thumbInner(c, st) {
      const l = st.lettersOk ? st.letters.join("") : "";
      if (c.result && (c.status === "ready" || c.status === "building")) return previewSvg(c.result, st.metal, { stroke: 0.8, label: l ? "Monogram " + l : "Monogram" });
      const tx = st.letters.map(x => x || "–").join(" ");
      return `<b class="mwThumbTx">${esc(tx)}</b>${st.busy || st.state === "reading" ? `<i class="mwThumbSpin" aria-hidden="true"></i>` : ""}`;
    }
    function paintThumb(el, c, st, force) {
      const sig = [st.state, st.busy ? 1 : 0, c.result && (c.status === "ready" || c.status === "building") ? c.resultKey || "r" : "", st.metal, st.letters.join("")].join("|");
      if (!force && el._mwSig === sig) return; el._mwSig = sig;
      el.dataset.state = st.state; el.innerHTML = thumbInner(c, st);
      if (el._mwReady && c.result && c.status === "ready") { const f = el._mwReady; el._mwReady = null; try { f(el); } catch (_) { /* the caller's trouble */ } }
    }
    function paintThumbs(c, st) {
      if (!thumbEls.size) return;
      for (const el of [...thumbEls]) {
        if (!el.isConnected && Date.now() - el._mwAt > 8000) { thumbEls.delete(el); continue; }
        if (el.dataset.mwThumb === c.key) paintThumb(el, c, st);
      }
    }
    function registerThumb(el, row, ask) {
      const r = lineOf(row); if (!r || !isMono(r)) return null;
      const c = cardOf(r, ask); el.dataset.mwThumb = c.key; el._mwAt = Date.now(); thumbEls.add(el); ensureQuiet(c);
      paintThumb(el, c, describe(c), true); return c;
    }
    const pxVar = px => ` style="--mw-px:${Math.max(24, Math.min(400, Math.round(+px || 104)))}px"`;
    function thumbHtml(row, o) {
      o = o || {}; const r = lineOf(row); if (!r || !isMono(r)) return "";
      const c = cardOf(r, askOf(o)), st = describe(c); ensureQuiet(c);
      return `<span class="mwThumb" data-mw-thumb="${esc(c.key)}" data-state="${esc(st.state)}"${pxVar(o.px)}>${thumbInner(c, st)}</span>`;
    }
    function fillThumbs(scope) {
      if (!scope || !scope.querySelectorAll) return;
      const list = [...scope.querySelectorAll("[data-mw-thumb]")]; if (scope.matches && scope.matches("[data-mw-thumb]")) list.unshift(scope);
      for (const el of list) {
        const key = el.dataset.mwThumb, row = rowByKey(key) || (cards.get(key) && cards.get(key).row); if (row) registerThumb(el, row, key);
      }
    }
    function thumb(row, o) {
      o = o || {}; const r = lineOf(row); if (!r || !isMono(r)) return null;
      const el = doc.createElement("span"); el.className = "mwThumb"; if (o.px) el.style.setProperty("--mw-px", Math.max(24, Math.min(400, Math.round(+o.px))) + "px");
      el._mwReady = typeof o.onReady === "function" ? o.onReady : null; registerThumb(el, r, askOf(o));
      if (o.into && o.into.replaceChildren) o.into.replaceChildren(el);
      return el;
    }
    const status = (row, ask) => { const r = lineOf(row); if (!r || !isMono(r)) return null; const c = cardOf(r, ask); return statusOf(c, describe(c)); };
    const onChange = fn => { if (typeof fn !== "function") return () => {}; listeners.add(fn); return () => listeners.delete(fn); };
    /** Every piece of every open monogram line: [{ key, slot, n, of, lineKey, poolId, row }], line by line, piece by piece. */
    const piecesNow = () => rowsNow().reduce((all, r) => all.concat(piecesOfLine(r).map(p => Object.assign(p, { row: r }))), []);
    const waiting = () => piecesNow().filter(p => { const c = cards.get(p.key), ap = c ? approvedFile(c) : null; return !(ap && ap.sent); });
    const svc = { count: () => waiting().length, rows: rowsNow, pieces: row => { const r = lineOf(row); return r && isMono(r) ? piecesOfLine(r).map(p => Object.assign(p, { row: r })) : []; }, piecesNow, pieceKey: (row, ask) => { const r = lineOf(row); return r ? slotKey(r.key, slotFor(r, ask)) : ""; }, families, alphabets, lettersOf: readLetters, cards, rebuild, approve, queue, approvedFile, glyph, setAlphabet, results,
      mountLine, thumb, thumbHtml, fillThumbs, status, onChange, feedEvents, loadSeals, ensureSeals, upgradeCard, addHoops,
      sealsOf: (row, ask) => { const r = lineOf(row); if (!r || !isMono(r)) return []; const c = cardOf(r, askOf(ask)); cardSeals(c); return sealsFor(c); },
      sealHtml: (row, o) => {
        o = o || {}; const r = lineOf(row), S = SEALS(); if (!r || !isMono(r) || !S) return "";
        const c = cardOf(r, askOf(o)); cardSeals(c); const list = sealsFor(c); if (!list.length) return "";
        const size = Math.round(+o.size) || S.BASE_SIZE;
        return `<span class="sealRow mwSeal${o.cls ? " " + esc(o.cls) : ""}${o.size ? " mini" : ""}"${o.size ? ` data-seal-max="${size}"` : ""} data-seal-group data-seal-count="${list.length}" role="group" aria-label="Monogram approval history">` + list.map(x => S.html(x, size, "monogramSeal")).join("") + `</span>`;
      }, isMonogram: isMono, widgets: () => [...widgets], describe: (row, ask) => { const r = lineOf(row); return r && isMono(r) ? describe(cardOf(r, ask)) : null; }, setAi };
    current = svc; made.push(svc);
    return svc;
  }
  // the page's one service: the last one made (the bridge makes it once), or the one a factory makes when asked (setFactory), so any module can mount a line without reaching for Engrave
  let current = null, factory = null; const made = [];
  const service = () => current || (factory ? (current = factory() || null) : null);
  const via = name => (a, b) => { const s = (b && b.ui) || service(); return s && s[name] ? s[name](a, b) : (name === "thumbHtml" || name === "sealHtml" ? "" : name === "sealsOf" ? [] : null); };
  return { make, FILE_RE, assistLoop, assistSvg, violationsOf, distanceOf, adjustOf, betterThan, AI_ROUNDS, AI_DAY_MAX, service, setFactory: f => { factory = typeof f === "function" ? f : null; },
    mountLine: via("mountLine"), thumb: via("thumb"), thumbHtml: via("thumbHtml"), status: via("status"), sealsOf: via("sealsOf"), sealHtml: via("sealHtml"), stampOf, stampsFromEvents, mergeStamps, SEAL_HOW, isMonogram: (row, o) => { const s = (o && o.ui) || service(); return !!(s && s.isMonogram(row)); },
    fillThumbs: scope => { const s = service(); if (s) s.fillThumbs(scope); }, widgets: () => made.reduce((all, s) => all.concat(s.widgets()), []) };
});
