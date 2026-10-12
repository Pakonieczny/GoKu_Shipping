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
 *                                                                    repoApi(action, ask, body) = the page's protected call; who() = the signed-in name
 *    CharmNestEtsyLink.listingIdOf(text)                            "…/listing/1700763608/…?transaction_id=…" or "1700763608" -> "1700763608", else ""
 *    CharmNestEtsyLink.wording(answer, listingId, sku)              the line the panel shows for the server's answer: { text, kind: "ok" | "bad", again: boolean }
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

  /* what the server answered, in the panel's words */
  function wording(r, listingId, sku) {
    if (!r || typeof r !== "object") return { text: "Etsy was not changed: the server sent no answer.", kind: "bad", again: true };
    if (r.ok) {
      const n = +r.products || (Array.isArray(r.productIds) ? r.productIds.length : 0), all = n ? "all " + plural(n, "product") : "every product";
      if (r.already) return { text: `Already there: ${all} of listing ${listingId} carry ${sku}. Nothing was changed.`, kind: "ok", again: false };
      return { text: `Done: ${sku} is now on ${all} of listing ${listingId}.${r.verified === false ? " (Etsy's answer was not compared.)" : ""}`, kind: "ok", again: false };
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
      const acts = el("div", "elActs"), go = el("button", "btn gold xs", "Put SKU on Etsy"), no = el("button", "btn ghost xs", "Close");
      go.type = "button"; no.type = "button"; go.disabled = true; go.setAttribute("data-repo", "modify");
      acts.append(go, no); panel.append(head); if (sel) panel.append(sel); panel.append(input, say, acts);
      row.after(panel);
      try { if (root.CharmNestRepoLock) root.CharmNestRepoLock.mark(panel); } catch (_) { /* the padlock is only a hint */ }
      chip.setAttribute("aria-expanded", "true");
      const skuNow = () => (sel ? sel.value : skus[0]);
      const idle = () => {   // the plain line: what pressing the button will do
        const id = listingIdOf(input.value);
        say.className = "elSay"; say.textContent = "";
        if (!input.value.trim()) { say.textContent = "Paste the listing's Etsy link or its number."; go.disabled = true; return; }
        if (!id) { say.textContent = "That is not an Etsy listing link or number."; go.disabled = true; return; }
        say.append("Every product of Etsy listing " + id + " will carry the SKU ", el("b", "", skuNow()), ". Prices, stock and options stay as they are; the listing as it is now is kept first.");
        go.disabled = false;
      };
      input.addEventListener("input", idle); if (sel) sel.addEventListener("change", idle);
      no.onclick = close;
      input.addEventListener("keydown", ev => { if (ev.key === "Enter" && !go.disabled) go.click(); else if (ev.key === "Escape") close(); });
      go.onclick = async () => {
        const id = listingIdOf(input.value), sku = skuNow();
        if (busy || !id) return;
        if (typeof o.repoApi !== "function") { say.className = "elSay bad"; say.textContent = "The charm repo lock did not load: reload the page."; return; }
        busy = true; go.disabled = true; input.disabled = true; if (sel) sel.disabled = true; no.disabled = true;
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
        busy = false; no.disabled = false; go.textContent = label;
        say.className = "elSay fresh " + word.kind; say.textContent = word.text;
        if (word.kind === "ok") { go.hidden = true; no.textContent = "Close"; return; }
        input.disabled = false; if (sel) sel.disabled = false; go.disabled = !word.again;
        if (word.again) go.textContent = "Try again";
      };
      idle(); try { input.focus({ preventScroll: true }); } catch (_) { /* focus is a convenience */ }
    };
  }

  root.CharmNestEtsyLink = { mount, listingIdOf, wording };
})(typeof window !== "undefined" ? window : globalThis);
