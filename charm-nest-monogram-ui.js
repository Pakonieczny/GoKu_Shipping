/*  charm-nest-monogram-ui.js — the monogram of an order line: the WIDGET (MUW, Paul, 16 Oct 2026) and, until the Engrave tab has switched over, the old "Monograms" pane (MONOGRAM / MONOGRAMUI, Paul, 15 Oct 2026, orders 4200683017 "APV" and 4202783520 "OSR").
 *  ═══════════════════════════════════════════════════════════════════════
 *  MUW — ONE reusable widget for ONE monogram order line, in two densities, in place of the Front / Back faces:
 *      CharmNestMonogramUI.mountLine(host, { rid, lineKey, row, mode: "detail" | "compact", gate, sheetLink, onChange, onApproved }) -> { el, mode, refresh, update, destroy, state, focus, approve, setLetters }
 *      CharmNestMonogramUI.thumbHtml(row, { px }) / fillThumbs(scope) / thumb(row, { px, into, onReady })   the small still picture for the piece card
 *      CharmNestMonogramUI.status(row) -> { state, label, letters, approved, sent }    ·   .widgets()   ·   .service() / .setFactory(fn)
 *  detail = the Engrave detail card (the picture beside the letters, wide); compact = the Order details window (one tight block that keeps ONE height: three single-line slots, a message may replace them). Both draw the same state
 *  (the state of a line lives in the service, per order line: a letter typed in one is in the other) with the same parts: the preview of the geometry that will be cut, in the line's metal; the three letter boxes (type one letter,
 *  swap two neighbours, rotate); the buyer's own words; ONE size line (the piece's mm, the metal, and the size the buyer chose, charm-nest-monogram-fit.js); ONE assembly line ("Assembled with AI: …" / "Needs a person: …" with a quiet
 *  Improve again); a plain message when the letters cannot be read or a letter is missing; Approve / "Approved · Monogram APV" with Change / "On the sheet". States: reading, letters, building, ready, check, approved, sent, error.
 *  The contract, with the hooks (data-mw-*) and the events (monogram:change, monogram:approved), is /tmp/mu/CONTRACT.md. The CSS is the `.mw*` block of charm-nest-1.html.
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
 *  metal, the number of pieces and Send to Sheet are what they are for every custom order. Changing the letters after approving replaces that design (never a second one).
 *  Loads in the page; the bridge makes it: CharmNestMonogramUI.make({ ...the page's own parts }).
 *
 *  MONOAI (Paul, 15 Oct 2026: "we do not sell many of these monograms and they need intelligent assembly: the AI should have freedom to resize ... just like a human would"). A word that still breaks a rule of the
 *  engine (report.metrics.violations) after Engine.autoFit is shown to a vision model beside the ESG reference (charmNestMonogramAssist-background through CN.agentCall "monoAssist"): it answers only the numbers of the engine's
 *  adjust contract { L, M, R: { scale, sx, dx, dy } } and one sentence; the page applies them ONLY through Engine.build (which clamps them), re-measures, and keeps a round only if the number of broken rules drops, or stays equal
 *  while the distance to ESG drops. At most 3 rounds per word, once per order (the final adjust, the sentence and the rounds are kept in settings.mono.assist, so reopening never pays again), at most 3 calls per order per day
 *  (also counted on the server); a word that still fails says "Needs a person: <limit>" and Approve then asks for a second press. ESG-like words never call it. A failure of the model or the network leaves the automatic result.
 *  ═══════════════════════════════════════════════════════════════════════ */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === "object" && module.exports) module.exports = api; else root.CharmNestMonogramUI = api;
})(typeof self !== "undefined" ? self : this, function (root) {
  "use strict";
  const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const SLOTS = [["left", "Left"], ["middle", "Middle"], ["right", "Right"]];
  const FILE_RE = /^Monogram ([A-Z]{3})\.ai$/;
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

  function make(d) {
    const cards = new Map();           // lineKey -> { key, row, draft (the letters a person typed), status, result, err, seq, timer, views (the mounted widgets) }
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
    function cardOf(row) {
      let c = cards.get(row.key);
      if (!c) { c = { key: row.key, row, draft: null, approved: "", status: "idle", result: null, resultLetters: "", err: null, seq: 0, timer: 0, host: null, el: null, approving: false, hint: "", wantApprove: false, ai: null, fit: false, confirmed: false, views: new Set(), unlocked: false, prog: null, quiet: false, queued: false, aiKey: "", abcSig: "", sig: "", tsig: "" }; cards.set(row.key, c); }
      c.row = row; return c;
    }
    const lettersOk = ls => ls.length === 3 && ls.every(x => /^[A-Z]$/.test(x));
    function buyerLetters(c) {
      const w = wordsOf(c.row), r = E() && E().lettersOf ? E().lettersOf(w) : { ok: false, letters: null, why: "" };
      if (r.ok) return { ls: r.letters.slice(), why: "" };
      const some = (w.match(/[A-Za-z]/g) || []).map(x => x.toUpperCase());      // ("A.P": two of three, the person adds the third; a name or four letters: nothing is guessed)
      return { ls: some.length && some.length < 3 ? [...some, "", ""].slice(0, 3) : ["", "", ""], why: r.why || "" };
    }
    const readLetters = c => c.draft ? c.draft.slice() : buyerLetters(c).ls;
    // the order's custom card (Review builds its list to say: asked a few times a second while a person types, so for half a second it is the one found a moment ago)
    const itemOf = (c, fresh) => { const t = Date.now(); if (!fresh && c.item && t - c.itemAt < 500) return c.item; const it = d.itemOf(c.row); if (it) { c.item = it; c.itemAt = t; } return it; };
    function approvedFile(c, fresh) {
      const it = itemOf(c, fresh), card = it && d.CustomSheet.cardOf(it), e = card && d.CustomSheet.entries()[card.ck];
      const files = e ? (e.files || []).filter(F => FILE_RE.test(F.name || "")) : [];
      if (!files.length) return null;
      const lettersOf = F => FILE_RE.exec(F.name)[1], mine = readLetters(c).join("");
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
      clearTimeout(c.timer); c.seq++; c.confirmed = false; c.ai = null; c.fit = false;      // (anything still on its way for an older edit is stale from now on)
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
      if (cached) { c.status = "ready"; c.result = cached; c.resultLetters = letters.join(""); c.resultKey = key; c.err = null; c.confirmed = false; remember(key, cached); if (wantAi(c)) assist(c, key, letters, abc, seq); paint(c); settled(c); return; }
      c.status = "building"; c.err = null; c.prog = { done: 0, total: 4 }; paint(c);
      const bar = d.CNProgress && !c.quiet ? d.CNProgress.start(`Building monogram ${letters.join("")}`) : null;      // (a picture asked for by a list thumbnail alone is made quietly: no bar, no AI)
      try {
        if (bar && bar.set) bar.set(0, 4);
        let done = 0;
        const parts = await Promise.all(SLOTS.map(([slot], i) => glyph(slot, letters[i], abc).then(g => { if (bar && bar.set) bar.set(++done, 4); if (seq === c.seq) { c.prog = { done, total: 4 }; paint(c); } return g; })));
        if (seq !== c.seq) return;                       // (a newer edit took over: this answer is never shown)
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
        else { c.status = "ready"; c.result = res; c.resultLetters = letters.join(""); c.resultKey = key; c.err = null; c.confirmed = false; remember(key, res); if (wantAi(c)) assist(c, key, letters, abc, seq, { parts: trio }); }
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
    const plainLimit = t => { const m = /^(.*?)( \(\+\d+ more\))?$/.exec(String(t || "")); return (plainRule(m[1]) || m[1]) + (m[2] || ""); };
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
     *  o.again (Improve again) asks once more from the card's current result, within the day's 3 calls. Never throws; the card keeps its picture while it waits. */
    async function assist(c, key, letters, abc, seq, o) {
      o = o || {}; c.aiKey = key;
      const res0 = c.result, v0 = violationsOf(res0);
      if (!v0 || !v0.length) {                               // (ESG-like and every word that meets the rules: never asked; one the AI made pass keeps its line when the finished monogram comes from memory)
        const kept = (mstore().assist || {})[recKey(c, key)]; c.ai = kept && kept.status === "assembled" && !o.again ? { state: "done", status: "assembled", why: kept.why, rounds: kept.rounds, cached: true } : null; return;
      }
      c.ai = { state: "working", round: 0, status: "" }; c.confirmed = false;
      try {
        const parts = o.parts || await trioOf(letters, abc);
        if (seq !== c.seq) return;
        const rec = !o.again && (mstore().assist || {})[recKey(c, key)];
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
        if (out.accepted && out.res) { c.result = out.res; c.confirmed = false; remember(key, out.res); }
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
    /** "Improve again" on a flagged card: the kept answer is forgotten and the AI is asked again from what the card shows (still bounded by the day's 3 calls per order). */
    function improve(c) {
      if (aiBusy(c) || c.status !== "ready" || !c.result || !c.resultKey) return;
      const m = mstore(), all = Object.assign({}, m.assist); delete all[recKey(c, c.resultKey)]; m.assist = all; keepSettings();
      assist(c, c.resultKey, c.resultLetters.split(""), alphabets(), c.seq, { again: true }); paint(c);
    }
    function setAi(v) { const m = mstore(); m.ai = v === "off" ? "off" : "on"; keepSettings(); for (const c of cards.values()) if (c.views.size) paint(c); }
    function settled(c) { if (c.wantApprove && c.status === "ready" && !aiBusy(c)) { c.wantApprove = false; approve(c); } else if (c.status !== "building" && !aiBusy(c)) c.wantApprove = false; }

    async function approve(c) {
      if (c.approving || !c.result) return;
      if (c.status === "building" || aiBusy(c)) { c.wantApprove = true; return; }
      if (c.status !== "ready") return;
      // a monogram that still breaks a rule after the engine's search and the AI is approved only when the person confirms it (a second press), the limit named
      const left = flaggedOf(c);
      if (left.length && !c.confirmed) { c.confirmed = true; paint(c); return; }
      const it = itemOf(c, true); if (!it) { d.toast("This order has no custom card to put the monogram on", "bad", 5000); return; }
      const letters = c.resultLetters, old = approvedFile(c, true);
      if (old && old.sent) return;
      if (old && old.letters === letters) { paint(c); return; }
      c.approving = true; c.err = null; paint(c);
      let done = null;
      try {
        const bytes = await E().toPdf(c.result, { title: "Monogram " + letters });
        const file = new File([bytes], `Monogram ${letters}.ai`, { type: "application/illustrator" });
        d.CustomSheet.add(it, [file], { quiet: true }); c.itemAt = 0;            // (first, so the metal a person set carries over to the new design)
        if (old) d.CustomSheet.remove(old.ck, old.file.id);                     // (and the one it replaces goes: a monogram is one design, never two)
        c.approved = letters; c.approvedKey = c.resultKey; c.unlocked = false;
        const kard = d.CustomSheet.cardOf && d.CustomSheet.cardOf(it);
        done = { rid: String((c.row.order || {}).receiptId || ""), lineKey: c.key, letters, name: `Monogram ${letters}.ai`, replaced: !!old, ck: (kard && kard.ck) || (old && old.ck) || "" };
        d.toast(`Monogram ${letters} is on order ${c.row.order.receiptId}'s designs · set its metal, then Send to Sheet`, "ok", 6000);
      } catch (e) { c.err = { kind: "save", text: "Not approved: " + String((e && e.message) || e).slice(0, 140) }; d.toast(c.err.text, "bad", 6000); }   // (the monogram stays on the card, Approve is still there to try again)
      finally {
        c.approving = false; paint(c);
        if (done) approvedTold(c, done);
        try { d.changed(); } catch (_) { /* the pane keeps what it drew */ }
      }
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
    const rowByKey = key => (d.rows() || []).find(r => r && r.key === key) || null;
    const rowOf = o => {
      const r = lineOf(o.row); if (r) return r;
      if (o.lineKey) return rowByKey(o.lineKey);
      if (o.rid != null) { const ls = rowsNow().filter(x => String(x.order.receiptId) === String(o.rid)); return ls.length === 1 ? ls[0] : null; }
      return null;
    };
    const mm1 = x => (+x).toFixed(1), mmTxt = x => (Number.isInteger(+x) ? String(+x) : (+x).toFixed(1));
    const LABEL = { reading: "Reading the letters", letters: "Letters to confirm", building: "Making…", ready: "Ready to approve", check: "Needs a person", approved: "Approved", sent: "On the sheets", error: "Needs a look" };

    /** Everything a person (or a test) can know about one line, in one object (the contract's state()). Pure reading: it paints nothing and asks nothing. */
    function describe(c) {
      const ls = readLetters(c), ap = approvedFile(c), sent = !!(ap && ap.sent), buyer = buyerLetters(c), said = wordsOf(c.row);
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
        buyer: { words: said, letters: buyer.ls.every(Boolean) ? buyer.ls.slice() : null, why: buyer.why || "" },
        canApproveBase: ok && !busy && !sent && !settled, approved: ap ? { letters: ap.letters, name: ap.file.name, sent } : null, dirty, locked: sent || (settled && !c.unlocked), unlocked: !!c.unlocked,
        metal, sizeMm, fit: f ? { maxWidthMm: f.maxWidthMm, scale: rf && rf.scale != null ? rf.scale : 1, applied: !!(rf && rf.applied) } : null,
        assembly, notes: warnings, hint: c.hint || "", confirmed: !!c.confirmed, error, message, prog: c.prog ? { done: c.prog.done, total: c.prog.total } : null,
        again: ok && !busy && left.length > 0 && !sent && aiOn() && !aiServerOff && callAi()
      };
    }
    const statusOf = (c, st) => ({ state: st.state, label: st.state === "building" && st.busy === "ai" ? "Assembling…" : LABEL[st.state] || "", letters: st.lettersOk ? st.letters.join("") : "", approved: !!st.approved, dirty: !!st.dirty, sent: st.state === "sent", rid: String((c.row.order || {}).receiptId || ""), lineKey: c.key });
    const gateWhy = v => { try { return v.o.gate ? String(v.o.gate() || "") : ""; } catch (_) { return ""; } };
    function stateFor(c, v, st) {
      const gate = gateWhy(v), out = Object.assign({}, st, { canApprove: st.canApproveBase && !gate, approveWhy: gate || (st.canApproveBase ? "" : st.state === "approved" || st.state === "sent" ? "Already approved" : st.busy ? "Making the monogram…" : "Type the three letters first"), rid: String((c.row.order || {}).receiptId || ""), lineKey: c.key });
      delete out.canApproveBase; return out;
    }

    /* ── telling the page: one call when a line's state really changed ── */
    function tellChange(c, st) {
      const sig = JSON.stringify([st.state, st.busy, st.letters, st.approved && st.approved.letters, st.approved && st.approved.sent, st.dirty, st.locked, st.canApproveBase, st.error && st.error.kind, st.assembly.status]);
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
        + `<p class="mwLine mwAsm" data-mw-assembly hidden><span></span> <button type="button" class="mwLink" data-mw-again hidden title="Ask the AI to adjust the letters once more">Improve again</button></p>`
        + `<p class="mwLine mwNote" data-mw-note hidden></p><div class="mwMsg" data-mw-msg hidden></div>`
        + `<span class="mwSr" role="status" aria-live="polite" data-mw-live></span><div class="mwAct"><button type="button" class="btn sage${small}" data-mw-approve disabled>Approve</button><span class="mwOk" data-mw-approved hidden>${ICON_OK}<span></span></span>`
        + `<button type="button" class="btn ghost${small}" data-mw-change hidden>Change</button><button type="button" class="mwLink" data-mw-sheet hidden title="Choose the metal and send it to a sheet">Send to Sheet <span aria-hidden="true">→</span></button></div></div></div>`;
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
      R.ins.forEach((inp, i) => { if (doc.activeElement !== inp && inp.value !== (st.letters[i] || "")) inp.value = st.letters[i] || ""; inp.disabled = st.locked; inp.setAttribute("aria-invalid", st.state === "letters" && !st.letters[i] ? "true" : "false"); });
      R.sw.forEach(b => { b.disabled = st.locked; });
      // what the buyer wrote (compact: only when it needs a look)
      const seen = st.buyer.letters ? spaced(st.buyer.letters.join("")) : st.buyer.words;
      const says = seen ? `The buyer wrote <b>${esc(seen)}</b>` : "The buyer wrote nothing";
      if (R.says._shown !== says) { R.says.innerHTML = says; R.says._shown = says; }
      R.says.hidden = st.message && st.message.kind === "letters" ? true : st.state !== "letters" && !(st.buyer.letters && st.buyer.letters.join("") !== st.letters.join(""));      // (an unreadable order's message says what the buyer wrote already; the order's own words stand in the strip above the widget, so the line is only for letters that differ from them)
      // ONE size line
      const seg = []; if (st.sizeMm) { seg.push(`${mm1(st.sizeMm[0])} × ${mm1(st.sizeMm[1])} mm`); if (st.metal) seg.push(TONES[st.metal][2]); }
      if (st.fit && st.sizeMm) seg.push(st.fit.applied ? `scaled to the buyer’s ${mmTxt(st.fit.maxWidthMm)} mm` : `within the buyer’s ${mmTxt(st.fit.maxWidthMm)} mm`);
      const sz = seg.join(" · "); if (R.size.textContent !== sz) R.size.textContent = sz; R.size.hidden = !sz;
      // ONE calm line about the assembly, and one quiet note (a letter refused, the first thing the engine warns about, how to replace an approved monogram)
      const as = st.assembly, line = !as.status ? "" : as.status === "assembled" ? `Assembled with AI: ${as.why}` : `Needs a person: ${plainLimit(as.limit)}${as.detail ? ". " + as.detail : ""}`;
      const unl = st.unlocked && !st.dirty && st.approved && !st.approved.sent ? "Change a letter to replace the approved monogram." : "";
      const n0 = st.notes[0] ? plainNote(st.notes[0]) : "", note = st.hint || unl || (line ? "" : n0) || "", noteWarn = !!st.hint || (!!n0 && note === n0);
      if (v.mode === "compact") {      // compact: the Order window must not jump, so the assembly line, the hint and the warning share ONE single-line slot (the full text is its title) and says / size / slot keep their height when empty
        const pick = st.hint ? { tx: st.hint, warn: true, status: "" } : line ? { tx: line, warn: as.status !== "assembled", status: as.status } : unl ? { tx: unl, warn: false, status: "" } : n0 ? { tx: n0, warn: true, status: "" } : null;
        R.asm.hidden = !pick; R.asm.dataset.status = pick ? pick.status : ""; R.asm.classList.toggle("warn", !!pick && pick.warn);
        const tx = pick ? pick.tx : ""; if (R.asmTx.textContent !== tx) R.asmTx.textContent = tx; R.asm.title = pick ? (pick.tx + (st.notes.length > 1 && !pick.status ? "\n" + st.notes.slice(1).map(plainNote).join("\n") : "")) : "";
        R.again.hidden = !(st.again && !!line && !!pick && pick.status !== ""); R.note.hidden = true;
        R.size.title = sz; R.says.title = R.says.textContent;
      } else {
        R.asm.hidden = !line; R.asm.dataset.status = as.status || ""; if (R.asmTx.textContent !== line) R.asmTx.textContent = line; R.asm.classList.toggle("warn", as.status !== "" && as.status !== "assembled"); R.again.hidden = !(st.again && !!line);
        R.note.hidden = !note; if (R.note.textContent !== note) R.note.textContent = note; R.note.classList.toggle("warn", noteWarn); R.note.title = st.notes.length > 1 ? st.notes.map(plainNote).join("\n") : "";
      }
      // a message that needs a person
      const msg = st.message, mh = msg ? msg.kind + "|" + msg.text + "|" + (msg.help || "") : "";
      R.msg.hidden = !msg; R.msg.dataset.kind = msg ? msg.kind : ""; if (msg) el.setAttribute("data-msg", ""); else el.removeAttribute("data-msg");      // (a message replaces the compact widget's lines: the one allowed growth)
      if (R.msg._shown !== mh) { R.msg.innerHTML = msgHtml(msg); R.msg._shown = mh; }
      // the actions: Approve, or Approved with a way to change it
      R.approve.hidden = settled || sent; R.approve.disabled = !can; R.approve.textContent = st.confirmed ? "Approve anyway" : st.dirty ? "Replace approved" : "Approve";
      R.approve.title = can ? (st.confirmed ? "It still has the problem named here: put it on the order’s designs anyway" : "Put this monogram on the order’s designs") : (gate || (busy ? "Making the monogram…" : st.state === "letters" || st.state === "error" ? "Type the three letters first" : ""));
      R.ok.hidden = !(settled || sent); const okTx = (sent ? "On the sheet" : "Approved") + (st.approved ? " · Monogram " + st.approved.letters : ""); if (R.okTx.textContent !== okTx) R.okTx.textContent = okTx;
      { const lv = sent ? "On the sheet." : settled ? "Approved." : busy || st.message ? "" : st.state === "ready" ? "The monogram is ready." : st.state === "check" ? "The monogram is ready, but it needs a person to look at it." : ""; if (R.live.textContent !== lv) R.live.textContent = lv; }   // (for a screen reader: what the picture and the buttons show)
      R.change.hidden = !(settled && st.locked);
      R.sheet.hidden = !(settled && v.sheetLink);
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
      if (lettersOk(ls)) schedule(c); else { clearTimeout(c.timer); c.seq++; c.status = "words"; c.result = null; c.err = null; c.confirmed = false; paint(c); }
    }
    function typeInto(v, i, t, inputType) {
      const c = v.c, typed = String(t.value || ""), l = typed.normalize("NFD").replace(/[^A-Za-z]/g, "").slice(-1).toUpperCase();
      if (typed && !l) { t.value = readLetters(c)[i] || ""; t.select(); c.hint = "Letters A to Z only"; paint(c); return; }
      c.hint = ""; t.value = l; const ls = readLetters(c); ls[i] = l; editTo(c, ls);
      if (l && i < 2 && inputType !== "deleteContentBackward") { const nx = v.r.ins[i + 1]; if (nx && !nx.disabled) nx.focus(); }
    }
    const viewApprove = v => { if (v.dead || gateWhy(v)) return; approveBy(v.c); };
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
        else if (hit("[data-mw-change]")) { c.unlocked = true; c.unlockedAt = Date.now(); paint(c); if (v.r.ins[0]) v.r.ins[0].focus(); }
        else if (hit("[data-mw-again]")) improve(c);
        else if (hit("[data-mw-retry]")) schedule(c, 0);
        else if (hit("[data-mw-sheet]")) { const it = itemOf(c, true); if (it) d.CustomSheet.open(it, {}); }
      });
    }
    function buildEl(v) {
      const host = v.host; host.replaceChildren();
      const el = doc.createElement("section"); el.className = "mw"; el.dataset.mode = v.mode; el.setAttribute("aria-label", "Monogram");
      el.innerHTML = viewHtml(v.mode); v.el = el;
      const q = s => el.querySelector(s);
      v.r = { pic: q("[data-mw-pic]"), art: q(".mwArt"), spin: q("[data-mw-spin]"), spinTx: q("[data-mw-spin] span"), bar: q("[data-mw-bar]"), ins: [...el.querySelectorAll("[data-mw-letter]")], sw: [...el.querySelectorAll(".mwSw")], says: q("[data-mw-says]"), size: q("[data-mw-size]"),
        asm: q("[data-mw-assembly]"), asmTx: q("[data-mw-assembly] > span"), again: q("[data-mw-again]"), note: q("[data-mw-note]"), msg: q("[data-mw-msg]"), approve: q("[data-mw-approve]"), ok: q("[data-mw-approved]"), okTx: q("[data-mw-approved] > span"), change: q("[data-mw-change]"), sheet: q("[data-mw-sheet]"), live: q("[data-mw-live]") };
      wireView(v); host.append(el);
    }
    /** Points a widget at a line (and builds what it shows): a line never made is made now; one made quietly for a thumbnail gets its AI check now that a person looks at it. */
    function attach(v, row) {
      const c = cardOf(row); if (v.c && v.c !== c) v.c.views.delete(v);
      v.c = c; c.views.add(v); c.quiet = false;
      if (!v.el) buildEl(v);
      begin(c);
      if (c.status === "idle") schedule(c, 0);
      else if (c.status === "ready" && c.resultKey && c.aiKey !== c.resultKey && !aiBusy(c)) { paint(c); assist(c, c.resultKey, c.resultLetters.split(""), alphabets(), c.seq); }
      else paint(c);
    }
    function mountLine(host, o) {
      o = o || {}; if (!host) return null;
      const row = rowOf(o); if (!row || !isMono(row)) return null;
      if (host._mw && host._mw.destroy) host._mw.destroy();
      const v = { host, o: Object.assign({}, o), c: null, el: null, r: null, dead: false, mode: o.mode === "compact" ? "compact" : "detail", sheetLink: false };
      const setOpts = () => { v.mode = v.o.mode === "compact" ? "compact" : "detail"; v.sheetLink = v.o.sheetLink == null ? v.mode === "detail" : !!v.o.sheetLink; };
      setOpts();
      const api = {
        state: () => stateFor(v.c, v, describe(v.c)),
        refresh() {
          if (v.dead) return;
          const row2 = rowByKey(v.c.key) || v.c.row; cardOf(row2);
          const k = abcKey(alphabets());
          if (v.c.abcSig && v.c.abcSig !== k && v.c.status !== "idle" && v.c.status !== "building") schedule(v.c, 0); else paint(v.c);      // (the library gained or lost an alphabet: the monogram follows)
        },
        update(ctx) {
          if (v.dead) return; ctx = ctx || {};
          const was = v.mode, key = v.c.key; Object.assign(v.o, ctx); setOpts();
          if (("lineKey" in ctx || "rid" in ctx) && !("row" in ctx)) v.o.row = null;
          const row2 = ctx.row || ctx.lineKey || ctx.rid != null ? rowOf(v.o) : null;
          if (v.mode !== was) { v.el = null; v.r = null; }
          if (row2 && isMono(row2) && (row2.key !== key || !v.el)) attach(v, row2); else if (!v.el) attach(v, v.c.row); else paint(v.c);
        },
        destroy() {
          if (v.dead) return; v.dead = true; if (v.c) v.c.views.delete(v); widgets.delete(api);
          if (v.host._mw === api) v.host._mw = null; if (v.el) v.el.remove(); v.el = null; v.r = null;
        },
        focus() { const ins = v.r && v.r.ins, t = ins && (ins.find(x => !x.value && !x.disabled) || ins.find(x => !x.disabled)); if (t) { try { t.focus(); } catch (_) { /* no layout */ } } },
        approve() { viewApprove(v); },
        setLetters(ls) { const a = Array.from({ length: 3 }, (_, i) => String((ls || [])[i] || "").replace(/[^A-Za-z]/g, "").slice(0, 1).toUpperCase()); v.c.hint = ""; editTo(v.c, a); }
      };
      // (el, mode and lineKey are not enumerable: a page that hands the widget to a test or a logger never serialises the DOM)
      Object.defineProperties(api, { el: { get: () => v.el }, mode: { get: () => v.mode }, lineKey: { get: () => v.c && v.c.key } });
      attach(v, row); v.host._mw = api; widgets.add(api);
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
    function registerThumb(el, row) {
      const r = lineOf(row); if (!r || !isMono(r)) return null;
      const c = cardOf(r); el.dataset.mwThumb = c.key; el._mwAt = Date.now(); thumbEls.add(el); ensureQuiet(c);
      paintThumb(el, c, describe(c), true); return c;
    }
    const pxVar = px => ` style="--mw-px:${Math.max(24, Math.min(400, Math.round(+px || 104)))}px"`;
    function thumbHtml(row, o) {
      o = o || {}; const r = lineOf(row); if (!r || !isMono(r)) return "";
      const c = cardOf(r), st = describe(c); ensureQuiet(c);
      return `<span class="mwThumb" data-mw-thumb="${esc(c.key)}" data-state="${esc(st.state)}"${pxVar(o.px)}>${thumbInner(c, st)}</span>`;
    }
    function fillThumbs(scope) {
      if (!scope || !scope.querySelectorAll) return;
      const list = [...scope.querySelectorAll("[data-mw-thumb]")]; if (scope.matches && scope.matches("[data-mw-thumb]")) list.unshift(scope);
      for (const el of list) {
        const key = el.dataset.mwThumb, row = rowByKey(key) || (cards.get(key) && cards.get(key).row); if (row) registerThumb(el, row);
      }
    }
    function thumb(row, o) {
      o = o || {}; const r = lineOf(row); if (!r || !isMono(r)) return null;
      const el = doc.createElement("span"); el.className = "mwThumb"; if (o.px) el.style.setProperty("--mw-px", Math.max(24, Math.min(400, Math.round(+o.px))) + "px");
      el._mwReady = typeof o.onReady === "function" ? o.onReady : null; registerThumb(el, r);
      if (o.into && o.into.replaceChildren) o.into.replaceChildren(el);
      return el;
    }
    const status = row => { const r = lineOf(row); if (!r || !isMono(r)) return null; const c = cardOf(r); return statusOf(c, describe(c)); };
    const onChange = fn => { if (typeof fn !== "function") return () => {}; listeners.add(fn); return () => listeners.delete(fn); };
    const waiting = () => rowsNow().filter(r => { const c = cards.get(r.key), ap = c ? approvedFile(c) : null; return !(ap && ap.sent); });
    const svc = { count: () => waiting().length, rows: rowsNow, families, alphabets, lettersOf: readLetters, cards, rebuild, approve, approvedFile, glyph, setAlphabet, results, improve,
      mountLine, thumb, thumbHtml, fillThumbs, status, onChange, isMonogram: isMono, widgets: () => [...widgets], describe: row => { const r = lineOf(row); return r && isMono(r) ? describe(cardOf(r)) : null; }, setAi };
    current = svc; made.push(svc);
    return svc;
  }
  // the page's one service: the last one made (the bridge makes it once), or the one a factory makes when asked (setFactory), so any module can mount a line without reaching for Engrave
  let current = null, factory = null; const made = [];
  const service = () => current || (factory ? (current = factory() || null) : null);
  const via = name => (a, b) => { const s = (b && b.ui) || service(); return s && s[name] ? s[name](a, b) : (name === "thumbHtml" ? "" : null); };
  return { make, FILE_RE, assistLoop, assistSvg, violationsOf, distanceOf, adjustOf, betterThan, AI_ROUNDS, AI_DAY_MAX, service, setFactory: f => { factory = typeof f === "function" ? f : null; },
    mountLine: via("mountLine"), thumb: via("thumb"), thumbHtml: via("thumbHtml"), status: via("status"), isMonogram: (row, o) => { const s = (o && o.ui) || service(); return !!(s && s.isMonogram(row)); },
    fillThumbs: scope => { const s = service(); if (s) s.fillThumbs(scope); }, widgets: () => made.reduce((all, s) => all.concat(s.widgets()), []) };
});
