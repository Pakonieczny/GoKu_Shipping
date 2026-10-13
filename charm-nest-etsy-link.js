/*  charm-nest-etsy-link.js
 *  ═══════════════════════════════════════════════════════════════════════
 *  "Put this charm's SKU on an Etsy listing" (SWANSKU, 12 Oct 2026; Paul: "Update this etsy listing SKU to match this Charm in our repo").
 *  One small control on a master card of the Library (Master files tab): an "Etsy" chip in the card's row opens a short panel under it:
 *  the listing's Etsy link or number, one plain line that says exactly what will happen, and one button. The write is the server's op
 *  listingEtsySkuWrite (netlify/functions/_charmNestEtsyLink.js): behind the repo password (the same lock as every other change to a charm:
 *  charm-nest-repo-lock.js asks only when this computer is not unlocked), reads the listing on Etsy first, keeps a copy of it, changes only the
 *  SKU of its products, one write. Nothing here decides anything: the page sends {listingId, sku, by} and says the server's answer in words.
 *
 *    CharmNestEtsyLink.mount(chip, { skus, repoApi, toast, who })   the chip is the card's button; skus = the card's SKUs (the first is the card's own);
 *                                                                    repoApi(action, ask, body) = the page's protected call; who() = the signed-in name;
 *                                                                    listings(sku) = optional: the taps to offer (default: suggestions() from what the page holds);
 *                                                                    skuTable(id) = optional: the SKUs now on that listing's products (the page's free listingSkus read: the stored
 *                                                                    table, no password, at most one Etsy read when none is stored), so the line BEFORE the press can say what is replaced
 *    CharmNestEtsyLink.listingIdOf(text)                            "…/listing/1700763608/…?transaction_id=…" or "1700763608" -> "1700763608", else ""
 *    CharmNestEtsyLink.wording(answer, listingId, sku)              the line the panel shows for the server's answer: { text, kind: "ok" | "bad", again: boolean }
 *    CharmNestEtsyLink.skuGroups(table)                             ETSYFAM: a listing's stored SKU table -> { groups: [{ sku, n }], total } when its products carry 2+ different SKUs (blank is its own), else null
 *    CharmNestEtsyLink.suggestions(skus, sku, src, max)             CATSKU: the listings the page ALREADY reads as this charm, to tap instead of pasting a link: [{ id, title, now }]
 *
 *  The taps (CATSKU, 12 Oct 2026) are read from what the page already holds, at the moment the panel opens: the order lines it loaded (window.B.orders.rows, each line's
 *  resolved design), the saved links it loaded (window.B.maps.aliases) and the library's look-alike lister (Master.likeFor). No Firestore read, no Etsy call, nothing new is
 *  fetched. A tap only puts the listing's number in the box (the same as pasting it): it never fills in a write, and the button and the password are exactly as before.
 *  A listing counts when one of its lines resolved to this card's design OR to a design that reads alike once case, spaces and punctuation are set aside (the cat listing's order
 *  read as "CAT 1- SITTING CAT" while the card Paul wants on it is "CAT 1 - SITTING CAT": two drawings, one name), or when a saved link names this design for the listing; and
 *  only while some line of it does not already carry the exact SKU about to be written.
 *
 *  REPLACE (ETSYFAM, 13 Oct 2026; Paul: "Replace each set of etsy SKU's with the provided charm repo SKU"): a listing whose products carry several different SKUs
 *  (X and X-CO, blanks, the shop's placeholders) is no longer refused: every product takes the card's SKU. When the listing's SKU table shows more than one SKU the line
 *  before the press names them with their counts ("20 x Sports 27-Bike and 20 x Sports 27-Bike-CO") and the answer after repeats them (`replaced`); a listing with one SKU
 *  (or none) has exactly the words it always had.
 *
 *  The text comes from the server's own `text`; this file adds only what the person sees around it. Everything is drawn with textContent.
 *  ═══════════════════════════════════════════════════════════════════════ */
(function (root) {
  "use strict";
  const doc = root.document;
  const LISTING = /^\d{3,20}$/;
  const listingIdOf = v => {
    const s = String(v == null ? "" : v).trim();
    if (LISTING.test(s)) return s;
    const m = /\/listing\/(\d{3,20})(?:[/?#]|$)/.exec(s);
    return m ? m[1] : "";
  };
  const plural = (n, w) => n + " " + w + (n === 1 ? "" : "s");

  /* ── the listings this page already reads as THIS charm (CATSKU) ── */
  const upSku = s => String(s == null ? "" : s).trim().toUpperCase();
  const plainTitle = t => String(t == null ? "" : t).replace(/&#0*39;/g, "'").replace(/&quot;/g, "\"").replace(/&amp;/g, "&").replace(/\s+/g, " ").trim();
  const shortTitle = t => { const s = plainTitle(t); if (s.length <= 30) return s; const cut = s.slice(0, 30), at = cut.lastIndexOf(" "); return (at > 14 ? cut.slice(0, at) : cut) + "…"; };
  /** skus: the card's SKUs; sku: the one about to be written; src: { rows: the page's order rows ({ line, spec: { designSku }, state }), aliases: listing id -> saved answer, like: sku -> the master SKUs that read alike }.
   *  → [{ id, title, now }] most lines first, at most `max` (5); `now` is the look-alike design the listing reads as today ("" when it already reads as this card). */
  function suggestions(skus, sku, src, max) {
    src = src || {};
    const mine = new Set((Array.isArray(skus) ? skus : [skus]).map(upSku).filter(Boolean)), want = upSku(sku), twin = new Set();
    if (typeof src.like === "function") for (const s of mine) { let l = []; try { l = src.like(s) || []; } catch (_) { l = []; } for (const k of l) if (!mine.has(upSku(k))) twin.add(upSku(k)); }
    const by = new Map(), get = id => { let e = by.get(id); if (!e) by.set(id, e = { id, title: "", n: 0, off: 0, now: "", said: false }); return e; };
    for (const r of Array.isArray(src.rows) ? src.rows : []) {
      const l = r && r.line; if (!l || r.state === "gone") continue;
      const sp = (r && r.spec) || {}, id = listingIdOf(l.listingId);   // (a line that waits between two look-alike drawings has no design yet: its candidates count)
      const ds = [sp.designSku].concat(sp.formHold && Array.isArray(sp.formHold.candidates) ? sp.formHold.candidates.map(c => c && c.sku) : []).map(upSku).filter(Boolean);
      const d = ds.find(x => mine.has(x)) || ds.find(x => twin.has(x));
      if (!d || !id) continue;
      const e = get(id); e.n++; if (!e.title) e.title = plainTitle(l.title);
      if (upSku(l.sku) !== want) e.off++;   // this line's own Etsy SKU is not yet the one to be written
      if (!mine.has(d)) e.now = d;
    }
    const al = src.aliases && typeof src.aliases === "object" ? src.aliases : {};
    for (const k of Object.keys(al)) {
      const a = al[k], id = listingIdOf(k); if (!a || typeof a !== "object" || !id) continue;
      const names = [a.sku].concat(a.bySku && typeof a.bySku === "object" ? Object.keys(a.bySku).map(x => a.bySku[x]) : []);
      if (!names.some(v => typeof v === "string" && mine.has(upSku(v)))) continue;   // a person's saved link to this design
      const e = get(id); e.said = true; if (!e.title) e.title = plainTitle(a.title);
    }
    return [...by.values()].filter(e => e.off > 0 || (!e.n && e.said)).sort((x, y) => y.n - x.n || (x.id < y.id ? -1 : 1)).slice(0, max > 0 ? max : 5).map(e => ({ id: e.id, title: e.title, now: e.now }));
  }
  /* what the page holds right now (all of it already loaded; reading it costs nothing) */
  function pageSource() {
    try { const B = root.B, M = root.Master; return { rows: B && B.orders && B.orders.rows, aliases: B && B.maps && B.maps.aliases, like: M && typeof M.likeFor === "function" ? s => M.likeFor(s) : null }; } catch (_) { return {}; }
  }

  /* ── several SKUs on one listing (ETSYFAM): what is replaced, in words ── */
  const MAX_GROUPS = 12;
  const plainSku = s => String(s == null ? "" : s).replace(/&quot;/g, "\"").replace(/&#0*39;/g, "'").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&amp;/g, "&").trim();
  /** A listing's stored SKU table ({ products: [{ sku, d }] } or { uni }) -> { groups: [{ sku, n }] biggest first, total } when its live products carry two or more different SKUs (a blank is its own), else null. */
  function skuGroups(table) {
    const list = table && Array.isArray(table.products) ? table.products.filter(p => p && !p.d) : [];
    if (!list.length) return null;
    const by = new Map();
    for (const p of list) { const t = plainSku(p.sku), k = t.toUpperCase(), g = by.get(k); if (g) g.n++; else by.set(k, { sku: t, n: 1 }); }
    if (by.size < 2) return null;
    return { groups: [...by.values()].sort((a, b) => b.n - a.n || (a.sku < b.sku ? -1 : a.sku > b.sku ? 1 : 0)), total: list.length };
  }
  /** [{ sku, n }] (+ the products of groups not listed) -> "20 x Sports 27-Bike and 20 x Sports 27-Bike-CO" (a blank SKU reads "(no SKU)") */
  function groupsText(list, more) {
    const g = (Array.isArray(list) ? list : []).filter(x => x && typeof x === "object"), shown = g.slice(0, MAX_GROUPS);
    const parts = shown.map(x => (+x.n || 0) + " x " + (plainSku(x.sku) || "(no SKU)"));
    const rest = (+more || 0) + g.slice(MAX_GROUPS).reduce((n, x) => n + (+x.n || 0), 0);
    if (rest > 0) parts.push(rest + " x other SKUs");
    return parts.length > 1 ? parts.slice(0, -1).join(", ") + " and " + parts[parts.length - 1] : parts[0] || "";
  }

  /** DOVEHUG: the words for an answer that left the Huggie products alone (`huggie`: { products, keyed, restored: [{ sku, n }], left }); a listing without Huggie products has the words it always had. */
  function huggieWording(r, listingId, sku, h) {
    const H = +h.products || 0, K = +h.keyed || 0, L = +h.left || 0, one = n => (n === 1 ? "is" : "are"), carries = n => (n === 1 ? "carries" : "carry");
    const restored = (Array.isArray(h.restored) ? h.restored : []).filter(x => x && (+x.n || 0) > 0), R = restored.reduce((n, x) => n + (+x.n || 0), 0);
    const huggies = plural(H, "Huggie product"), others = `the ${plural(K, "product")} of listing ${listingId} that ${one(K)} not Huggie`;
    const stuck = L > 0 ? ` ${plural(L, "Huggie product")} still ${carries(L)} ${sku}: no earlier SKU is known for ${L === 1 ? "it" : "them"}, so ${L === 1 ? "it was" : "they were"} left alone.` : "";
    if (r.already) { const rest = H - L, tail = rest > 0 ? `, and the ${L > 0 ? "other " : ""}${plural(rest, "Huggie product")} ${rest === 1 ? "does" : "do"} not` : ""; return { text: `Already there: ${others} ${carries(K)} ${sku}${tail}.${stuck} Nothing was changed.`, kind: "ok", again: false }; }
    const parts = restored.map(x => `${plural(+x.n || 0, "Huggie product")} put back to ${plainSku(x.sku)}`), back = R > 0 ? ` ${parts.length > 1 ? parts.slice(0, -1).join(", ") + " and " + parts[parts.length - 1] : parts[0]}.` : ` The ${huggies} ${H === 1 ? "was" : "were"} left alone.`;
    const said = Array.isArray(r.replaced) && r.replaced.length ? ` It replaced ${groupsText(r.replaced, r.replacedMore)}.` : "";
    return { text: `Done: ${sku} is on the ${plural(K, "product")} of listing ${listingId} that ${one(K)} not Huggie.${said}${back}${stuck}${r.beforePath ? " The listing as it was is kept." : ""}${r.verified === false ? " (Etsy's answer was not compared.)" : ""}`, kind: "ok", again: false };
  }
  /* what the server answered, in the panel's words */
  function wording(r, listingId, sku) {
    if (!r || typeof r !== "object") return { text: "Etsy was not changed: the server sent no answer.", kind: "bad", again: true };
    if (r.ok) {
      if (r.huggie && typeof r.huggie === "object" && +r.huggie.products > 0) return huggieWording(r, listingId, sku, r.huggie);
      const n = +r.products || (Array.isArray(r.productIds) ? r.productIds.length : 0), all = n ? "all " + plural(n, "product") : "every product";
      if (r.already) return { text: `Already there: ${all} of listing ${listingId} carry ${sku}. Nothing was changed.`, kind: "ok", again: false };
      const said = Array.isArray(r.replaced) && r.replaced.length ? ` It replaced ${groupsText(r.replaced, r.replacedMore)}${r.beforePath ? "; the listing as it was is kept" : ""}.` : "";   // (the products carried several different SKUs)
      return { text: `Done: ${sku} is now on ${all} of listing ${listingId}.${said}${r.verified === false ? " (Etsy's answer was not compared.)" : ""}`, kind: "ok", again: false };
    }
    const keep = r.beforePath ? " The listing as it was is kept." : "";
    const text = (r.text ? r.text.charAt(0).toUpperCase() + r.text.slice(1) : "Etsy was not changed") + (/[.!?]$/.test(r.text || "") ? "" : ".") + keep;
    return { text, kind: "bad", again: r.retry !== false };
  }

  const CSS = `
.elPanel{display:grid;gap:5px;margin-top:2px;padding-top:7px;border-top:1px solid var(--line);animation:elIn .18s ease-out}
@keyframes elIn{from{opacity:0;transform:translateY(-3px)}to{opacity:1;transform:none}}
.elPanel .elHead{font:600 11px var(--mono);color:var(--ink70)}
.elPanel input.elIn,.elPanel select.elSel{width:100%;box-sizing:border-box;border:1px solid var(--line);border-radius:6px;padding:4px 6px;font-size:11.5px;background:var(--card);color:var(--ink)}
.elPanel input.elIn:focus,.elPanel select.elSel:focus{outline:none;border-color:var(--gold2)}
.elPanel .elSay{min-height:30px;font-size:11px;line-height:1.45;color:var(--ink70);overflow-wrap:anywhere}
.elPanel .elSay b{font:700 11px var(--mono);color:var(--ink)}
.elPanel .elSay.ok{color:var(--sage)}
.elPanel .elSay.bad{color:#8a3a26}
.elPanel .elSay.fresh{animation:elIn .22s ease-out}
.elPanel .elSay .spin{margin-right:6px}
.elPanel .elPicks{display:grid;gap:3px}
.elPanel .elPicks[hidden]{display:none}
.elPanel .elPicks.off{opacity:.5;pointer-events:none}
.elPanel .elPicks .btn{display:block;width:100%;box-sizing:border-box;text-align:left;white-space:normal;overflow-wrap:anywhere;line-height:1.3;font-weight:400}
.elPanel .elPicks .btn b{font:700 11px var(--mono)}
.elPanel .elPicks .btn.on{border-color:var(--gold2);background:var(--card2)}
.elPanel .elActs{display:flex;gap:4px;align-items:center;flex-wrap:wrap}
.elPanel .elActs .btn .spin{margin-right:6px}
@media (prefers-reduced-motion:reduce){.elPanel,.elPanel .elSay.fresh{animation:none}}
`;
  function css() { try { if (!doc || doc.getElementById("cnEtsyLinkCss")) return; const s = doc.createElement("style"); s.id = "cnEtsyLinkCss"; s.textContent = CSS; doc.head.appendChild(s); } catch (_) { /* unstyled, still works */ } }
  const el = (tag, cls, text) => { const e = doc.createElement(tag); if (cls) e.className = cls; if (text != null) e.textContent = text; return e; };

  /* The panel of one card. `chip` toggles it; it lives inside the card (no pop-up); the password dialog is the repo lock's own and only appears when this computer is locked. */
  function mount(chip, o) {
    css();
    o = o || {};
    const skus = (Array.isArray(o.skus) ? o.skus : [o.skus]).map(s => String(s || "")).filter(Boolean);
    if (!chip || !skus.length) return;
    let panel = null, busy = false;
    const close = () => { if (panel && !busy) { panel.remove(); panel = null; chip.setAttribute("aria-expanded", "false"); } };
    chip.setAttribute("aria-expanded", "false");
    chip.onclick = () => {
      if (panel) return close();
      const row = chip.closest(".row") || chip;
      panel = el("div", "elPanel");
      const head = el("div", "elHead", "Put the SKU on an Etsy listing");
      let sel = null;
      if (skus.length > 1) { sel = el("select", "elSel"); sel.setAttribute("aria-label", "SKU"); for (const s of skus) { const op = el("option", "", s); op.value = s; sel.appendChild(op); } }
      const input = el("input", "elIn"); input.type = "text"; input.placeholder = "Etsy listing link or number"; input.setAttribute("aria-label", "Etsy listing link or number");
      input.autocomplete = "off"; input.spellcheck = false; input.setAttribute("inputmode", "url");
      const say = el("div", "elSay"); say.setAttribute("aria-live", "polite");
      const picks = el("div", "elPicks"); picks.setAttribute("role", "group"); picks.setAttribute("aria-label", "Listings already read as this charm"); picks.hidden = true;
      const acts = el("div", "elActs"), go = el("button", "btn gold xs", "Put SKU on Etsy"), no = el("button", "btn ghost xs", "Close");
      go.type = "button"; no.type = "button"; go.disabled = true; go.setAttribute("data-repo", "modify");
      acts.append(go, no); panel.append(head); if (sel) panel.append(sel); panel.append(picks, input, say, acts);
      try { if (root.CharmNestEtsyBatch) root.CharmNestEtsyBatch.attach(panel, { repoApi: o.repoApi, toast: o.toast, who: o.who, hold: on => { busy = on; } }); } catch (_) { /* the batch line is optional (charm-nest-etsy-batch.js) */ }
      row.after(panel);
      try { if (root.CharmNestRepoLock) root.CharmNestRepoLock.mark(panel); } catch (_) { /* the padlock is only a hint */ }
      chip.setAttribute("aria-expanded", "true");
      const skuNow = () => (sel ? sel.value : skus[0]);
      const markPick = () => { const id = listingIdOf(input.value); for (const b of picks.children) b.classList.toggle("on", b.getAttribute("data-listing") === id); };
      const drawPicks = () => {   // the listings the page already reads as this charm (nothing is fetched); a tap puts the number in the box, as pasting would
        picks.textContent = "";
        let list = []; try { list = (typeof o.listings === "function" ? o.listings(skuNow()) : suggestions(skus, skuNow(), pageSource())) || []; } catch (_) { list = []; }
        for (const s of list) {
          const b = el("button", "btn ghost xs"); b.type = "button"; b.setAttribute("data-listing", s.id); b.append(el("b", "", s.id), s.title ? "  " + shortTitle(s.title) : "");
          if (s.now) b.title = "now read as " + s.now;
          b.onclick = () => { if (busy) return; input.value = s.id; idle(); try { input.focus({ preventScroll: true }); } catch (_) { /* focus is a convenience */ } };
          picks.appendChild(b);
        }
        picks.hidden = !picks.children.length; markPick();
      };
      /* what the listing's products carry now (ETSYFAM): asked once per listing and panel, from the page's free SKU table read (no password, no write);
         "wait" while it is on its way, null when the products carry one SKU (or none) or it cannot be read: then the line is the one it always was */
      const known = new Map();
      const learn = id => {
        known.set(id, "wait");
        Promise.resolve().then(() => o.skuTable(id)).then(r => skuGroups(r && r.tables && r.tables[id]), () => null)
          .then(g => { known.set(id, g); if (panel && !busy && listingIdOf(input.value) === id) idle(); });
      };
      const idle = () => {   // the plain line: what pressing the button will do
        const id = listingIdOf(input.value);
        markPick();
        say.className = "elSay"; say.textContent = "";
        if (!input.value.trim()) { say.textContent = "Paste the listing's Etsy link or its number."; go.disabled = true; return; }
        if (!id) { say.textContent = "That is not an Etsy listing link or number."; go.disabled = true; return; }
        if (typeof o.skuTable === "function" && !known.has(id)) learn(id);
        const rep = known.get(id);
        if (rep === "wait") { say.append(el("span", "spin"), "Reading the SKUs now on listing " + id + "…"); go.disabled = true; return; }
        if (rep) {   // the products carry several different SKUs: every one of them is replaced, and the line says which
          say.append("Every product of Etsy listing " + id + " will carry the SKU ", el("b", "", skuNow()), " in place of the SKUs they carry now: " + groupsText(rep.groups) + ". Prices, stock and options stay as they are; the listing as it is now, with those SKUs, is kept first.");
          go.disabled = false; return;
        }
        say.append("Every product of Etsy listing " + id + " will carry the SKU ", el("b", "", skuNow()), ". Prices, stock and options stay as they are; the listing as it is now is kept first.");
        go.disabled = false;
      };
      input.addEventListener("input", idle); if (sel) sel.addEventListener("change", () => { drawPicks(); idle(); });
      no.onclick = close;
      input.addEventListener("keydown", ev => { if (ev.key === "Enter" && !go.disabled) go.click(); else if (ev.key === "Escape") close(); });
      go.onclick = async () => {
        const id = listingIdOf(input.value), sku = skuNow();
        if (busy || !id) return;
        if (typeof o.repoApi !== "function") { say.className = "elSay bad"; say.textContent = "The charm repo lock did not load: reload the page."; return; }
        busy = true; go.disabled = true; input.disabled = true; if (sel) sel.disabled = true; no.disabled = true; picks.classList.add("off");
        const label = go.textContent; go.textContent = ""; go.append(el("span", "spin"), "Putting the SKU on Etsy…");
        say.className = "elSay"; say.textContent = "Reading the listing on Etsy, keeping a copy, then changing only the SKU. This takes a few seconds.";
        let word = null;
        try {
          const by = typeof o.who === "function" ? String(o.who() || "").trim() : "";
          const r = await o.repoApi("modify", { what: `Putting the SKU ${sku} on Etsy listing ${id}.`, target: `${sku} → Etsy ${id}`, go: "Put SKU on Etsy" }, { op: "listingEtsySkuWrite", listingId: id, sku, by });
          word = wording(r, id, sku);
          if (word.kind === "ok" && typeof o.toast === "function") o.toast(`${sku} is on Etsy listing ${id}`, "ok", 4000);
        } catch (e) {
          word = e && e.cancelled ? { text: "Nothing was sent: the repo stayed locked.", kind: "bad", again: true }
            : { text: "Etsy may not have been changed: " + String((e && e.message) || e) + ". Look at the listing before pressing again.", kind: "bad", again: false };
        }
        busy = false; no.disabled = false; go.textContent = label; picks.classList.remove("off");
        say.className = "elSay fresh " + word.kind; say.textContent = word.text;
        if (word.kind === "ok") { go.hidden = true; picks.hidden = true; no.textContent = "Close"; return; }
        input.disabled = false; if (sel) sel.disabled = false; go.disabled = !word.again;
        if (word.again) go.textContent = "Try again";
      };
      drawPicks(); idle(); try { input.focus({ preventScroll: true }); } catch (_) { /* focus is a convenience */ }
    };
  }

  root.CharmNestEtsyLink = { mount, listingIdOf, wording, suggestions, skuGroups };
})(typeof window !== "undefined" ? window : globalThis);
