/*  charm-nest-monogram-ui.js — the Engrave tab's "Monograms" (MONOGRAM / MONOGRAMUI, Paul, 15 Oct 2026, orders 4200683017 "APV" and 4202783520 "OSR").
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
  const timeout = (p, ms, what) => new Promise((res, rej) => { const t = setTimeout(() => rej(new Error(what + " took too long")), ms); p.then(v => { clearTimeout(t); res(v); }, e => { clearTimeout(t); rej(e); }); });
  const later = () => new Promise(r => setTimeout(r, 0));
  const spaced = s => String(s || "").split("").join(" ");
  const cap = s => { s = String(s || ""); return s.charAt(0).toUpperCase() + s.slice(1); };

  function make(d) {
    const cards = new Map();           // lineKey -> { key, row, draft (the letters a person typed), status, result, err, seq, timer, host, el }
    const glyphs = new Map();          // sku|file|time -> Promise<{ letter, rings, sku }>
    const results = new Map();         // alphabets|letters -> a finished build (the oldest is dropped first)
    let famCache = null, famSig = "", pane = null, abcSig = "";
    const abcKey = abc => abc.sig + "|" + abc.fams.map(f => f.base + "#" + f.count).join(",");
    const E = () => d.Engine || root.CharmNestMonogram;
    const doc = d.doc || root.document;
    const rowsNow = () => (d.rows() || []).filter(r => r && r.spec && r.spec.special && r.spec.special.monogram && !GONE.includes(r.state));
    const nameOf = r => { const b = (r.order || {}).buyer; return String((b && b.name) || "").trim(); };
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
    function setAlphabet(slot, base) {
      const set = d.settings(), next = Object.assign({}, set.mono || {}); if (base) next[slot] = base; else delete next[slot];
      set.mono = next; try { d.saveSettings(); } catch (_) { /* kept for this session */ }
      paintAlphabets();
      for (const c of cards.values()) if (c.host) schedule(c, 0);
    }

    /* ── a letter of an alphabet: its drawing's rings (the letter as drawn: outline and counters) ── */
    const skuFor = (fam, letter) => fam.base.replace(/\{[LN]\}/, letter);
    function missing(slot, letter, abc) {
      const fam = abc[slot], err = new Error(`The ${slot === "middle" ? "Middle" : slot === "left" ? "Left" : "Right"} alphabet has no “${letter}”`);
      err.kind = "missing"; err.help = fam ? `${fam.name} lacks ${fam.missing.join(" ")} · add ${skuFor(fam, letter)} to the charm library` : "";
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
      if (!c) { c = { key: row.key, row, draft: null, approved: "", status: "idle", result: null, resultLetters: "", err: null, seq: 0, timer: 0, host: null, el: null, approving: false, hint: "", wantApprove: false }; cards.set(row.key, c); }
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
    function schedule(c, wait) {
      clearTimeout(c.timer); c.seq++;                       // (anything still on its way for an older edit is stale from now on)
      c.timer = setTimeout(() => rebuild(c), wait == null ? WAIT_MS : wait);
      c.status = "building"; c.err = null; paint(c);
    }

    /* ── making the monogram ── */
    async function rebuild(c) {
      clearTimeout(c.timer);
      const letters = readLetters(c), seq = ++c.seq;
      if (!lettersOk(letters)) { c.status = "words"; c.result = null; c.err = null; paint(c); return; }
      const abc = alphabets();
      if (!abc.ok) { c.status = "setup"; c.result = null; c.err = null; paint(c); return; }
      const key = sigOf(abc, letters), cached = results.get(key);
      if (cached) { c.status = "ready"; c.result = cached; c.resultLetters = letters.join(""); c.resultKey = key; c.err = null; remember(key, cached); paint(c); settled(c); return; }
      c.status = "building"; c.err = null; paint(c);
      const bar = d.CNProgress ? d.CNProgress.start(`Building monogram ${letters.join("")}`) : null;
      try {
        if (bar && bar.set) bar.set(0, 4);
        let done = 0;
        const parts = await Promise.all(SLOTS.map(([slot], i) => glyph(slot, letters[i], abc).then(g => { if (bar && bar.set) bar.set(++done, 4); return g; })));
        if (seq !== c.seq) return;                       // (a newer edit took over: this answer is never shown)
        await later();                                   // (the spinner is on the screen before the joining starts)
        if (seq !== c.seq) return;
        const res = E().build({ left: parts[0], middle: parts[1], right: parts[2] });
        if (seq !== c.seq) return;
        if (bar && bar.set) bar.set(4, 4);
        if (!res || !res.ok) { c.status = "error"; c.result = null; c.err = { kind: "join", text: joinWhy(res && res.why) }; }
        else { c.status = "ready"; c.result = res; c.resultLetters = letters.join(""); c.resultKey = key; c.err = null; remember(key, res); }
      } catch (e) {
        if (seq !== c.seq) return;
        c.status = "error"; c.result = null;
        c.err = e && e.kind === "missing" ? { kind: "missing", text: String(e.message).slice(0, 160), help: e.help || "" } : { kind: "load", text: loadWhy(e) };
      } finally {
        if (bar) bar.end();
        if (seq === c.seq) { paint(c); settled(c); }
      }
    }
    const joinWhy = why => /touch/.test(why || "") ? "These letters do not touch each other. Try other letters, or the letters in another order." : /separate parts/.test(why || "") ? "One of these letters is made of separate parts that stay loose, so it cannot be one charm." : String(why || "The letters could not be joined").slice(0, 160);
    const loadWhy = e => /took too long/.test(String(e && e.message)) ? `${String(e.message).slice(0, 90)}. Check the connection and try again.` : "A letter could not be loaded from the library. Check the connection and try again.";
    function settled(c) { if (c.wantApprove && c.status === "ready") { c.wantApprove = false; approve(c); } else if (c.status !== "building") c.wantApprove = false; }

    async function approve(c) {
      if (c.approving || !c.result) return;
      if (c.status === "building") { c.wantApprove = true; return; }
      if (c.status !== "ready") return;
      const it = itemOf(c, true); if (!it) { d.toast("This order has no custom card to put the monogram on", "bad", 5000); return; }
      const letters = c.resultLetters, old = approvedFile(c, true);
      if (old && old.sent) return;
      if (old && old.letters === letters) { paint(c); return; }
      c.approving = true; c.err = null; paint(c);
      try {
        const bytes = await E().toPdf(c.result, { title: "Monogram " + letters });
        const file = new File([bytes], `Monogram ${letters}.ai`, { type: "application/illustrator" });
        d.CustomSheet.add(it, [file], { quiet: true }); c.itemAt = 0;            // (first, so the metal a person set carries over to the new design)
        if (old) d.CustomSheet.remove(old.ck, old.file.id);                     // (and the one it replaces goes: a monogram is one design, never two)
        c.approved = letters; c.approvedKey = c.resultKey;
        d.toast(`Monogram ${letters} is on order ${c.row.order.receiptId}'s designs · set its metal, then Send to Sheet`, "ok", 6000);
      } catch (e) { c.err = { kind: "save", text: "Not approved: " + String((e && e.message) || e).slice(0, 140) }; d.toast(c.err.text, "bad", 6000); }   // (the monogram stays on the card, Approve is still there to try again)
      finally { c.approving = false; paint(c); try { d.changed(); } catch (_) { /* the pane keeps what it drew */ } }
    }

    /* ── drawing ── */
    /* The picture is the CUT piece: the finished monogram filled with the metal it will be cut in, one thin dark cut line round the outline and round every counter (the holes are open: the card shows through),
       centred with an even margin and scaled to the box. The line is 1 CSS pixel whatever the scale (vector-effect), so it stays a hairline and crisp on a 2x screen. */
    const TONES = { gold: ["#d9bd78", "#5a4520", "Gold"], gold10k: ["#dec58a", "#5a4520", "10K gold"], gold14k: ["#d4b46a", "#5a4520", "14K gold"], silver: ["#c8ccd2", "#4a4e55", "Silver"], rose: ["#e2b4a1", "#693e33", "Rose gold"] };
    // the metal of the line: the one a person set on the approved design, else the order's own, else a neutral gold
    const metalOf = c => { const ap = approvedFile(c), m = (ap && ap.file && ap.file.metal) || (c.row.spec && c.row.spec.material) || ""; return TONES[m] ? m : ""; };
    function previewSvg(res, metal) {
      const b = res.bboxPt, w = b[2] - b[0], h = b[3] - b[1], pad = Math.max(w, h) * 0.06, t = TONES[metal] || TONES.gold;
      return `<svg viewBox="${(-pad).toFixed(2)} ${(-pad).toFixed(2)} ${(w + 2 * pad).toFixed(2)} ${(h + 2 * pad).toFixed(2)}" role="img" aria-label="The monogram as it will be cut, in ${esc(t[2])}" preserveAspectRatio="xMidYMid meet" shape-rendering="geometricPrecision"><path d="${esc(E().svgPath(res))}" fill="${t[0]}" fill-rule="evenodd" stroke="${t[1]}" stroke-width="1.25" stroke-linejoin="round" vector-effect="non-scaling-stroke"/></svg>`;
    }
    const slotName = i => SLOTS[i][1];
    function buildCard(c) {
      const row = c.row, o = row.order || {}, who = nameOf(row);
      const host = doc.createElement("article"); host.className = "moCard"; host.dataset.key = row.key; host.dataset.rid = String(o.receiptId || "");
      host.setAttribute("aria-label", `Monogram for order ${o.receiptId || ""}`);
      host.innerHTML = `<header class="moHd"><i class="moDot" aria-hidden="true"></i><b class="moName">Order ${esc(o.receiptId)}${who ? " · " + esc(who) : ""}</b><span class="moSt" aria-live="polite"></span></header>`
        + `<div class="moBody"><div class="moPic" aria-busy="false"><div class="moArt"></div><div class="moSpin" role="status" hidden><i aria-hidden="true"></i><span>Building…</span></div></div>`
        + `<div class="moSide"><div class="moLts" role="group" aria-label="The three letters">`
        + SLOTS.map(([slot, label], i) => (i ? `<button type="button" class="moSw" data-mo-swap="${i - 1}" aria-label="Swap the ${slotName(i - 1)} and ${label} letters" title="Swap ${slotName(i - 1)} and ${label}"><svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true"><path d="M3 5.5h10M10.5 3l2.5 2.5-2.5 2.5M13 10.5H3M5.5 8 3 10.5 5.5 13" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg></button>` : "")
          + `<label class="moLt" data-slot="${slot}"><span>${label}</span><input data-mo-l="${i}" maxlength="2" inputmode="text" autocapitalize="characters" autocomplete="off" autocorrect="off" spellcheck="false" aria-label="${label} letter"></label>`).join("")
        + `</div><p class="moSays"></p><p class="moMeta" aria-live="polite"></p><div class="moMsg" hidden></div>`
        + `<div class="moAct"><button type="button" class="btn gold" data-mo="approve" disabled>Approve</button><button type="button" class="btn ghost" data-mo="open" hidden>Set metal · Send to Sheet</button></div></div></div>`;
      c.host = host;
      c.el = { dot: host.querySelector(".moDot"), st: host.querySelector(".moSt"), open: host.querySelector('[data-mo="open"]'), approve: host.querySelector('[data-mo="approve"]'),
        pic: host.querySelector(".moPic"), art: host.querySelector(".moArt"), spin: host.querySelector(".moSpin"), spinTxt: host.querySelector(".moSpin span"), ins: [...host.querySelectorAll("[data-mo-l]")], says: host.querySelector(".moSays"), meta: host.querySelector(".moMeta"), msg: host.querySelector(".moMsg") };
      wire(c);
      return host;
    }
    function paint(c) {
      const h = c.host, el = c.el; if (!h || !el || !h.isConnected) return;
      const ls = readLetters(c), ap = approvedFile(c), sent = !!(ap && ap.sent), buyer = buyerLetters(c), busy = c.status === "building" || c.approving;
      // (the letters in the boxes are not the approved ones, or the alphabets they were made from have changed since)
      const dirty = !!(ap && !sent && (ls.join("") !== ap.letters || (c.status === "ready" && c.approvedKey && c.resultKey && c.resultKey !== c.approvedKey)));
      const state = sent ? "sent" : ap && !dirty && c.status !== "error" ? "approved" : c.status === "ready" ? "ready" : busy ? "busy" : c.status === "error" || c.status === "setup" ? "alert" : "words";
      h.dataset.state = state; el.dot.dataset.s = state;
      // the picture (while a new one is being made the old one stays, faded, so the card never jumps)
      let art = "";
      const metal = metalOf(c);
      if (c.result && (c.status === "ready" || c.status === "building")) art = previewSvg(c.result, metal);
      else if (ap && (c.status === "setup" || c.status === "idle" || c.status === "error" || c.status === "words" && !c.draft)) art = `<div class="moHint">Monogram ${esc(ap.letters)} is on the order's designs</div>`;
      else if (c.status === "setup") art = `<div class="moHint">Choose the three alphabets below</div>`;
      else if (c.status === "words") art = `<div class="moHint">${esc(buyer.why && !c.draft ? cap(buyer.why) + ". Type the three letters." : "Type the three letters")}</div>`;
      else if (c.status === "error") art = `<div class="moHint">No picture yet</div>`;
      if (el.art._shown !== art) { el.art.innerHTML = art; el.art._shown = art; }
      el.pic.classList.toggle("busy", busy); el.pic.setAttribute("aria-busy", busy ? "true" : "false");
      el.spin.hidden = !busy; el.spinTxt.textContent = c.approving ? "Saving…" : "Building…";
      // the three boxes (never touched while a person is typing in one)
      el.ins.forEach((inp, i) => { if (doc.activeElement !== inp && inp.value !== (ls[i] || "")) inp.value = ls[i] || ""; inp.disabled = sent; inp.setAttribute("aria-invalid", c.status === "words" && !ls[i] ? "true" : "false"); });
      // "The buyer wrote A P V · Left A · Middle P · Right V"
      const said = wordsOf(c.row), seen = buyer.ls.every(Boolean) ? spaced(buyer.ls.join("")) : said;
      el.says.innerHTML = (said ? `The buyer wrote <b>${esc(seen)}</b>` : "The buyer wrote nothing") + " · " + ls.map((x, i) => `${slotName(i)} <b>${esc(x || "–")}</b>`).join(" · ");
      // size and anything worth a look
      const notes = [];
      if (c.result && c.status === "ready") { const r = c.result; notes.push(`<span class="moSize">Actual size ${(+r.wMm).toFixed(1)} × ${(+r.hMm).toFixed(1)} mm${metal ? " · " + TONES[metal][2] : ""}</span>`); const adj = ((r.report && r.report.altered) || []).map(a => a.text).filter(Boolean); if (adj.length) notes.push(`<span class="moAdj">Adjusted: ${esc(adj.join(" · "))}</span>`); for (const w of (r.report && r.report.warnings) || []) notes.push(`<span class="moWarn">${esc(cap(w))}</span>`); }
      if (c.hint) notes.push(`<span class="moWarn">${esc(c.hint)}</span>`);
      el.meta.innerHTML = notes.join("");
      // a message that needs a person (a missing letter, a load that failed, letters that cannot join)
      const err = (c.status === "error" || (c.err && c.err.kind === "save" && c.status === "ready")) && c.err;
      el.msg.hidden = !err; el.msg.dataset.kind = err ? err.kind : "";
      el.msg.innerHTML = err ? `<span role="alert">${esc(err.text)}</span>${err.help ? `<small>${esc(err.help)}</small>` : ""}${err.kind === "load" ? `<button type="button" class="btn ghost sm" data-mo="retry">Try again</button>` : ""}` : "";
      // the one button
      const can = c.status === "ready" && !busy && !!c.result;
      el.st.textContent = sent ? "On the sheets" : ap && !dirty ? "Approved" : c.status === "ready" ? "Ready to approve" : c.status === "building" ? "Making…" : c.status === "error" || c.status === "setup" ? "Needs a look" : "Waiting for letters";
      el.open.hidden = !(ap && !dirty && !sent);
      el.approve.hidden = !!(sent || (ap && !dirty));
      el.approve.disabled = !can; el.approve.textContent = dirty ? "Replace approved" : "Approve";
      el.approve.title = can ? "Put this monogram on the order's designs" : busy ? "Making the monogram…" : "Type the three letters first";
    }

    function wire(c) {
      const host = c.host;
      host.addEventListener("input", ev => {
        const t = ev.target; if (!t.matches || !t.matches("[data-mo-l]")) return;
        const i = +t.getAttribute("data-mo-l"), typed = String(t.value || ""), v = typed.normalize("NFD").replace(/[^A-Za-z]/g, "").slice(-1).toUpperCase();
        if (typed && !v) { t.value = readLetters(c)[i] || ""; t.select(); c.hint = "Letters A to Z only"; paint(c); return; }      // (a digit, a sign, an emoji: the box keeps its letter and a person is told once, calmly)
        c.hint = "";
        t.value = v; const ls = readLetters(c); ls[i] = v; c.draft = ls; c.wantApprove = false;
        if (lettersOk(ls)) schedule(c); else { clearTimeout(c.timer); c.seq++; c.status = "words"; c.result = null; c.err = null; paint(c); }
        if (v && i < 2 && ev.inputType !== "deleteContentBackward") { const nx = c.el.ins[i + 1]; if (nx) nx.focus(); }
      });
      host.addEventListener("paste", ev => {
        const t = ev.target; if (!t.matches || !t.matches("[data-mo-l]")) return;
        const text = (ev.clipboardData && ev.clipboardData.getData("text")) || "", r = E().lettersOf(text);
        if (!r.ok) { if (String(text).replace(/\s+/g, "").length > 1) { ev.preventDefault(); c.hint = cap(r.why || "Paste the three letters"); paint(c); } return; }
        ev.preventDefault(); c.draft = r.letters.slice(); c.hint = ""; schedule(c); c.el.ins[2].focus();
      });
      host.addEventListener("keydown", ev => {
        const t = ev.target; if (!t.matches || !t.matches("[data-mo-l]")) return;
        const i = +t.getAttribute("data-mo-l");
        if (ev.key === "Enter") { ev.preventDefault(); if (c.status === "ready" || c.status === "building") approveBy(c); }
        else if (ev.key === "Escape") { if (revert(c)) ev.stopPropagation(); else t.blur(); }
        else if (ev.key === "Backspace" && !t.value && i > 0) { ev.preventDefault(); c.el.ins[i - 1].focus(); }
        else if (ev.key === "ArrowLeft" && i > 0 && t.selectionStart === 0) { ev.preventDefault(); c.el.ins[i - 1].focus(); }
        else if (ev.key === "ArrowRight" && i < 2 && t.selectionEnd === t.value.length) { ev.preventDefault(); c.el.ins[i + 1].focus(); }
      });
      host.addEventListener("focusin", ev => { if (ev.target.matches && ev.target.matches("[data-mo-l]")) ev.target.select(); });
      host.addEventListener("click", ev => {
        const sw = ev.target.closest && ev.target.closest("[data-mo-swap]");
        if (sw) { const i = +sw.dataset.moSwap, ls = readLetters(c); [ls[i], ls[i + 1]] = [ls[i + 1], ls[i]]; c.draft = ls; c.hint = ""; c.wantApprove = false; if (lettersOk(ls)) schedule(c); else { c.seq++; c.status = "words"; c.result = null; paint(c); } return; }
        const b = ev.target.closest && ev.target.closest("[data-mo]"); if (!b) return;
        if (b.dataset.mo === "approve") approveBy(c);
        else if (b.dataset.mo === "retry") schedule(c, 0);
        else if (b.dataset.mo === "open") { const it = itemOf(c, true); if (it) d.CustomSheet.open(it, {}); }
      });
    }
    function approveBy(c) { if (c.status === "building") c.wantApprove = true; else approve(c); }
    // Esc in a letter box: the edit is taken back (the letters the card last showed), a second Esc leaves the box
    function revert(c) {
      const shown = c.resultLetters ? c.resultLetters.split("") : null, ls = readLetters(c);
      if (!shown || ls.join("") === shown.join("")) return false;
      c.draft = shown; c.hint = ""; c.wantApprove = false; schedule(c, 0); return true;
    }

    /* ── the pane ── */
    function alphabetsHtml(abc) {
      const names = ["left", "middle", "right"].map(k => abc[k] ? abc[k].name : "–");
      const opt = (slot, f) => `<option value="${esc(f.base)}"${abc.by[slot] === "saved" && abc[slot] && abc[slot].base === f.base ? " selected" : ""}>${esc(f.name)} · ${f.count} letters</option>`;
      const sel = ([slot, label]) => `<label class="moAbc"><span>${label}</span><select data-mo-abc="${slot}" aria-label="${label} alphabet"><option value="">${abc.by[slot] === "name" && abc[slot] ? "Automatic · " + esc(abc[slot].name) : "Automatic"}</option>${abc.fams.map(f => opt(slot, f)).join("")}</select></label>`;
      const note = !abc.fams.length ? `<p class="moHint">No 26-letter alphabet is in the charm library yet. Add the monogram file in Master: one design per letter, named Monogram_Left_A … Monogram_Left_Z, Monogram_Mid_A … and Monogram_Right_A …</p>`
        : !abc.ok ? `<p class="moHint">${esc(cap(abc.why))}</p>` : "";
      return `<details class="moAbcs"${abc.ok ? "" : " open"}><summary><span>Alphabets</span><em>${abc.ok ? esc(names.join(" · ")) : "to choose"}</em></summary><div class="moAbcRow">${SLOTS.map(sel).join("")}</div>${note}</details>`;
    }
    function paintAlphabets() {
      if (!pane) return; const box = pane.querySelector(".moAbcWrap"); if (!box) return;
      const abc = alphabets(), was = box.querySelector("details"), open = !!(was && was.open) || !abc.ok; box.innerHTML = alphabetsHtml(abc); abcSig = abcKey(abc);
      const dt = box.querySelector("details"); if (dt && open) dt.open = true;
      box.querySelectorAll("[data-mo-abc]").forEach(s => s.onchange = () => setAlphabet(s.dataset.moAbc, s.value));
    }
    const ordered = rows => rows.slice().sort((a, b) => String(a.order.receiptId).localeCompare(String(b.order.receiptId)) || String(a.key).localeCompare(String(b.key)));
    /** Brings the pane up to the orders: a card for every monogram line, none for a line that has gone; the cards that are there are drawn again, never rebuilt (a letter box keeps its caret). */
    function refresh(o) {
      if (!pane) return; o = o || {};
      const list = pane.querySelector(".moList"); if (!list) return;
      const changed = abcSig !== "" && abcSig !== abcKey(alphabets());       // (the library gained or lost an alphabet, or a person chose others: the picker and every monogram follow)
      if (changed) paintAlphabets();
      const all = ordered(rowsNow()), keys = new Set(all.map(r => r.key));
      for (const [k, c] of [...cards]) if (!keys.has(k)) { clearTimeout(c.timer); c.seq++; if (c.host) c.host.remove(); cards.delete(k); }
      // the tab's search box: order, buyer, the words or the letters ("OSR"); a card that is not shown keeps what it holds
      const q = String(d.query ? d.query() || "" : "").trim().toLowerCase();
      const rows = q ? all.filter(r => { const c = cards.get(r.key); return [r.order.receiptId, nameOf(r), wordsOf(r), c ? readLetters(c).join("") : ""].join(" ").toLowerCase().includes(q); }) : all, shown = new Set(rows.map(r => r.key));
      for (const c of cards.values()) if (c.host && !shown.has(c.key)) { c.host.remove(); c.host = null; c.el = null; }
      const none = list.querySelector(".moNone"), noneText = all.length ? `<b>No monogram matches “${esc(q)}”</b><span>Clear the search box to see them all.</span>` : `<b>No monograms waiting</b><span>An order of a custom monogram listing shows here with its three letters.</span>`;
      if (rows.length && none) none.remove();
      if (!rows.length) { if (!none) list.innerHTML = `<div class="egEmpty moNone">${noneText}</div>`; else if (none._t !== noneText) none.innerHTML = noneText; list.firstElementChild._t = noneText; }
      let prev = null;
      for (const r of rows) {
        const c = cardOf(r); let fresh = false;
        if (!c.host || !c.el || c.host.parentNode !== list) { buildCard(c); fresh = true; }
        if (prev ? c.host.previousElementSibling !== prev : list.firstElementChild !== c.host) { if (prev) prev.after(c.host); else list.prepend(c.host); }
        prev = c.host;
        if (fresh) start(c); else if (changed && c.status !== "idle") schedule(c, 0); else paint(c);
      }
      if (o.focus) focus(o.focus);
    }
    // a card that has just come on the screen: what it already holds is drawn at once; a card with nothing yet is made
    function start(c) {
      paint(c);
      if (c.status !== "idle") return;
      const ap = approvedFile(c);
      if (ap && !c.draft && ap.letters !== buyerLetters(c).ls.join("")) c.draft = ap.letters.split("");     // (a person changed the letters before approving: the card shows the approved ones)
      if (ap && !c.approved) c.approved = ap.letters;
      schedule(c, 0);
    }
    function focus(key) {
      const c = cards.get(key); if (!c || !c.host) return;
      try { c.host.scrollIntoView({ block: "center", behavior: "auto" }); } catch (_) { /* no layout */ }
      c.host.classList.add("moFocus"); setTimeout(() => c.host && c.host.classList.remove("moFocus"), 1600);
    }
    function mount(host, o) {
      o = o || {}; pane = host;
      host.innerHTML = `<div class="moPane"><div class="moAbcWrap"></div><div class="moList"></div></div>`;
      for (const c of cards.values()) { c.host = null; c.el = null; }          // (the old cards' nodes went with the old pane: each is built again from what it holds)
      paintAlphabets();
      refresh(o);
      return { destroy() { pane = null; for (const c of cards.values()) { c.host = null; c.el = null; } } };
    }
    const waiting = () => rowsNow().filter(r => { const c = cards.get(r.key), ap = c ? approvedFile(c) : null; return !(ap && ap.sent); });
    return { count: () => waiting().length, rows: rowsNow, mount, refresh, focus, families, alphabets, lettersOf: readLetters, cards, rebuild, approve, approvedFile, glyph, setAlphabet, results };
  }
  return { make, FILE_RE };
});
