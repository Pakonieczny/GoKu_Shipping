/*  charm-nest-zodiac-etsy.js
 *  ═══════════════════════════════════════════════════════════════════════
 *  "Add the ZODIAC SIGN drop-down to the Huggie Hoop Etsy listing" (ZODWRITE, 12 Oct 2026; Paul: "You will need to add the 3rd dropdown and include all of the
 *  available zodiac options"). DORMANT: this row sits in Settings and does nothing until a person presses it. Pressing Check the listing asks the server to READ the
 *  listing (two Etsy calls, nothing written to Etsy) and keep a safety copy; the server's answer is shown in plain words. Pressing Add the drop-down is a
 *  second, separate step that only appears when the check passed, only lights when the exact words are typed, and asks the Charm repo password again. The write
 *  itself is the server's (netlify/functions/_charmNestEtsyZodiac.js + charmNestZodiacEtsy-background.js): one PUT, never retried, never undone from here.
 *  Nothing here decides anything: the page sends {listingId, confirm, payloadHash, by} and says the server's answers in words (drawn with textContent only).
 *
 *    CharmNestZodiacEtsy.mount(host, { api, lock, who })   host = the row's element; api(fn, body, opts) = the page's cloud call; lock = CharmNestRepoLock; who() = the signed-in name
 *    CharmNestZodiacEtsy.listingIdOf(text)                  "…/listing/1718000001/…" or "1718000001" -> "1718000001", else ""
 *    CharmNestZodiacEtsy.lines(rec)                         the plain lines for a stored/checked record: { head, facts[], problems[], notes[], kind, buttons:{ check, write, settle }, phrase }
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
  const money = n => (Number.isFinite(+n) ? "$" + (Math.round(+n * 100) / 100).toFixed(2).replace(/\.00$/, "") : "?");
  const cap = s => { s = String(s == null ? "" : s); return s.charAt(0).toUpperCase() + s.slice(1); };
  const stop = s => (/[.!?]$/.test(s) ? s : s + ".");

  /* The plain lines of a record. `rec` is the server's status (or the prepare answer): { state, ready, phrase, summary, reasons, warnings, result, lastRefusal }.
     buttons.check = Check the listing may be pressed; buttons.write = the typed-words step shows; buttons.settle = Check what Etsy holds shows. */
  function lines(rec) {
    const out = { head: "", facts: [], problems: [], notes: [], kind: "", buttons: { check: true, write: false, settle: false }, phrase: (rec && rec.phrase) || "" };
    if (!rec || typeof rec !== "object") { out.head = "Paste the listing's Etsy link or number, then press Check the listing."; return out; }
    const st = rec.state || "none", s = rec.summary || null, res = rec.result || null;
    const ids = rec.listingId ? "listing " + rec.listingId : "this listing";
    if (s) {
      const opts = (s.options || []).filter(o => o && o.values && o.values.length);
      const kinds = opts.map(o => plural(o.values.length, (o.name || "option").toLowerCase())).join(" × ");
      if (s.productsBefore != null) out.facts.push(`Now on Etsy: ${plural(s.productsBefore, "product")}${kinds ? " (" + kinds + ")" : ""}${Number.isFinite(+s.prices?.min) ? ", priced " + (s.prices.min === s.prices.max ? money(s.prices.min) : money(s.prices.min) + " to " + money(s.prices.max)) : ""}.`);
      if (s.productsAfter) out.facts.push(`After: ${plural(s.productsAfter, "product")} — each of those also chooses one of the ${(s.signs || []).length || 12} zodiac signs. Every price, stock count and processing profile stays exactly as it is now; the zodiac sign changes nothing about the price.`);
      if ((s.signs || []).length) {
        const repo = s.signs.filter(x => x.from === "repo"), fresh = s.signs.filter(x => x.from !== "repo");
        out.facts.push(`SKUs: ${plural(s.signs.length, "different SKU")}, one per sign. ${repo.length} match charms already in the repo (${repo.map(x => x.sign).join(", ")}); ${fresh.length} are new SKUs for the repo to take later (${fresh.map(x => x.sign).join(", ")}).`);
      }
      if (s.scopes) out.facts.push(s.scopes.listings_w === true ? "Etsy permission to change listings (listings_w): yes." : s.scopes.listings_w === false ? "Etsy permission to change listings (listings_w): MISSING." : "Etsy permission to change listings (listings_w): not confirmed.");
      if (s.title) out.facts.unshift(`${cap(ids)}: ${String(s.title).slice(0, 110)}`);
    }
    for (const r of rec.reasons || []) out.problems.push(cap(stop(String(r))));
    for (const w of rec.warnings || []) out.notes.push(cap(stop(String(w))));
    const refusal = rec.text && rec.ok === false ? cap(stop(rec.text)) : "";
    switch (st) {
      case "prepared": out.head = "Checked: nothing has been written to Etsy. A safety copy of the listing is kept."; out.kind = "ok"; out.buttons.write = rec.ready === true; break;
      case "blocked": out.head = "The check found a problem, so nothing can be written."; out.kind = "bad"; break;
      case "stale": out.head = "The listing changed after the check (or a safety copy was missing). Nothing was sent: check the listing again."; out.kind = "bad"; break;
      case "writing": out.head = "Handing the change to Etsy…"; out.kind = "busy"; out.buttons.check = false; break;
      case "putting": out.head = "The change was sent to Etsy; waiting for its answer…"; out.kind = "busy"; out.buttons.check = false; out.buttons.settle = true; break;
      case "written": out.head = res && res.text ? cap(stop(res.text)) : "Written."; out.kind = "ok"; out.buttons.check = false; out.notes.push("The listing as it was and a ready restore body are kept in storage (charmnest/zodiac-1012/). Nothing here undoes the change."); break;
      case "mismatch": out.head = "Etsy accepted the change, but its listing is not exactly what was sent. Nothing was undone or re-sent."; out.kind = "bad"; out.buttons.check = false; for (const d of (res && res.details) || []) out.problems.push(cap(stop(String(d)))); out.notes.push("The listing as it was and a restore body are kept in storage (charmnest/zodiac-1012/). A person should look at the listing."); break;
      case "unknown": out.head = res && res.text ? cap(stop(res.text)) : "The change may or may not have landed."; out.kind = "bad"; out.buttons.check = false; out.buttons.settle = true; break;
      case "refused": out.head = res && res.text ? cap(stop(res.text)) : (rec.lastRefusal ? "Not written: " + rec.lastRefusal.reason + "." : "Not written. Nothing changed on Etsy."); out.kind = "bad"; for (const d of (res && res.details) || []) out.problems.push(cap(stop(String(d)))); break;
      default: out.head = refusal || "Not checked yet. Press Check the listing."; out.kind = refusal ? "bad" : ""; break;
    }
    return out;
  }

  const CSS = `
.zePanel{display:grid;gap:6px}
.zePanel .zeRow{display:flex;gap:6px;align-items:center;flex-wrap:wrap}
.zePanel input.zeIn{flex:1 1 220px;min-width:0;box-sizing:border-box;border:1px solid var(--line);border-radius:6px;padding:5px 7px;font-size:12px;background:var(--card);color:var(--ink)}
.zePanel input.zeIn:focus{outline:none;border-color:var(--gold2)}
.zePanel .zeHead{font-size:12px;line-height:1.45;color:var(--ink70);overflow-wrap:anywhere}
.zePanel .zeHead.ok{color:var(--sage)}
.zePanel .zeHead.bad{color:var(--clay)}
.zePanel ul.zeList{margin:0;padding-left:16px;font-size:11.5px;line-height:1.5;color:var(--ink70)}
.zePanel ul.zeList.bad{color:var(--clay)}
.zePanel .zeSay{font-size:11.5px;color:var(--ink70)}
.zePanel .zeSay b{font:700 11.5px var(--mono);color:var(--ink)}
.zePanel .btn .spin{margin-right:6px}
.zePanel [hidden]{display:none !important}
`;
  function css() { try { if (!doc || doc.getElementById("cnZodiacEtsyCss")) return; const s = doc.createElement("style"); s.id = "cnZodiacEtsyCss"; s.textContent = CSS; doc.head.appendChild(s); } catch (_) { /* unstyled, still works */ } }
  const el = (tag, cls, text) => { const e = doc.createElement(tag); if (cls) e.className = cls; if (text != null) e.textContent = text; return e; };

  function mount(host, o) {
    css();
    o = o || {};
    if (!host || host.dataset.zeMounted) return;
    host.dataset.zeMounted = "1";
    const api = () => (typeof o.api === "function" ? o.api : root.api), lock = () => o.lock || root.CharmNestRepoLock;
    host.replaceChildren();
    const label = el("label", "", "Etsy: add the ZODIAC SIGN drop-down to a listing"); label.setAttribute("for", "zeListing");
    const panel = el("div", "zePanel");
    const input = el("input", "zeIn"); input.id = "zeListing"; input.type = "text"; input.placeholder = "Etsy listing link or number"; input.autocomplete = "off"; input.spellcheck = false; input.setAttribute("inputmode", "url");
    const check = el("button", "btn ghost xs", "Check the listing"); check.type = "button"; check.disabled = true; check.setAttribute("data-repo", "modify");
    const row1 = el("div", "zeRow"); row1.append(input, check);
    const head = el("div", "zeHead"); head.setAttribute("role", "status"); head.setAttribute("aria-live", "polite");
    const facts = el("ul", "zeList"), problems = el("ul", "zeList bad"), notes = el("ul", "zeList");
    const say = el("div", "zeSay"), phraseIn = el("input", "zeIn"); phraseIn.type = "text"; phraseIn.autocomplete = "off"; phraseIn.spellcheck = false; phraseIn.setAttribute("aria-label", "the words that confirm the change");
    const go = el("button", "btn gold xs", "Add the drop-down"); go.type = "button"; go.disabled = true; go.setAttribute("data-repo", "modify");
    const row2 = el("div", "zeRow"); row2.append(phraseIn, go);
    const settle = el("button", "btn ghost xs", "Check what Etsy holds"); settle.type = "button"; settle.setAttribute("data-repo", "modify");
    const helpEl = el("div", "help", "Reads the listing (two Etsy calls) and keeps a copy of it first. Nothing is written to Etsy until you also type the confirming words and press Add the drop-down, which asks for the Charm repo password. Only the Huggie Hoop listing (SKU HUGGIE HOOPS-ZODIAC) is accepted.");
    panel.append(row1, head, facts, problems, notes, say, row2, settle);
    host.append(label, panel, helpEl);
    try { if (lock() && lock().mark) lock().mark(host); } catch (_) { /* the padlock is only a hint */ }

    let rec = null, listingId = "", busy = false, poll = null, seq = 0, msg = null;   // msg = a line that stays until the next action: { text, kind }
    const by = () => { try { return typeof o.who === "function" ? String(o.who() || "").trim() : ""; } catch (_) { return ""; } };
    const fill = (ul, arr) => { ul.replaceChildren(...arr.map(t => el("li", "", t))); ul.hidden = !arr.length; };
    const mark = () => { try { if (lock() && lock().mark) lock().mark(host); } catch (_) { /* hint only */ } };
    const paint = () => {
      const L = lines(rec);
      head.className = "zeHead " + (msg ? msg.kind : L.kind === "busy" ? "" : L.kind); head.textContent = msg ? msg.text : L.head;
      fill(facts, L.facts); fill(problems, L.problems); fill(notes, L.notes);
      const valid = !!listingIdOf(input.value);
      check.hidden = !L.buttons.check; check.disabled = busy || !valid; input.disabled = busy;
      settle.hidden = !L.buttons.settle || !listingId; settle.disabled = busy;
      const phrase = L.phrase || (listingId ? "ADD ZODIAC DROPDOWN " + listingId : "");
      const showWrite = L.buttons.write && !!phrase;
      row2.hidden = !showWrite; say.hidden = !showWrite;
      if (showWrite) { say.replaceChildren("To go ahead, type exactly: ", el("b", "", phrase)); phraseIn.placeholder = phrase; phraseIn.disabled = busy; go.disabled = busy || phraseIn.value.trim() !== phrase; }
      return L;
    };
    const spin = (btn, text) => { const sp = el("span", "spin"); sp.setAttribute("aria-hidden", "true"); btn.setAttribute("aria-busy", "true"); btn.replaceChildren(sp, doc.createTextNode(text)); };
    const unspin = (btn, text) => { btn.removeAttribute("aria-busy"); btn.replaceChildren(doc.createTextNode(text)); };
    const errText = e => {
      if (e && e.cancelled) return "Nothing was sent: the Charm repo password was not given.";
      const d = e && e.data;
      if (d && d.error === "repo-locked") return "Nothing was sent: the Charm repo is locked. Press the button again and give the password.";
      return "Not done: " + String((d && (d.text || d.error)) || (e && e.message) || e) + ". If a change might have been sent, press Check what Etsy holds.";
    };
    const read = async () => {   // the stored record of this listing (free: no password, no Etsy call)
      const id = listingIdOf(input.value); if (!id || typeof api() !== "function") return;
      const mine = ++seq;
      try {
        const r = await api()("charmNestLibrary", { op: "zodiacListingStatus", listingId: id }, { quiet: true });
        if (mine !== seq || id !== listingIdOf(input.value)) return;
        listingId = id; rec = r && r.found ? r : null;
        paint();
        if (rec && (rec.state === "writing" || rec.state === "putting")) watch();
      } catch (_) { /* a failed read changes nothing */ }
    };
    const watch = () => {   // while a write is running: read the record every 4 s (up to 12 minutes), no password
      if (poll) return; const t0 = Date.now();
      poll = setInterval(async () => {
        if (Date.now() - t0 > 12 * 60000) { clearInterval(poll); poll = null; return; }
        if (!busy) await read();
        if (!rec || (rec.state !== "writing" && rec.state !== "putting")) { clearInterval(poll); poll = null; }
      }, 4000);
    };
    const call = (op, extra, ask) => {
      const L = lock(), A = api();
      if (!L || typeof A !== "function") return Promise.reject(Object.assign(new Error("The charm repo lock did not load: reload the page"), { data: null }));
      return L.call("modify", ask, tok => A("charmNestLibrary", Object.assign({ op, listingId, by: by(), repoToken: tok }, extra), { quiet: true }));
    };
    /* one press: fn() may return { text, kind } for a line that stays on screen after the record is shown */
    const run = async (btn, text, doing, working, fn) => {
      if (busy) return;
      busy = true; msg = { text: working, kind: "" }; paint(); spin(btn, doing);
      let after = null;
      try { after = await fn(); } catch (e) { after = { text: errText(e), kind: "bad" }; }
      busy = false; msg = after && after.text ? after : null; unspin(btn, text); paint(); mark();
    };

    input.addEventListener("input", () => { listingId = listingIdOf(input.value); msg = null; if (rec && rec.listingId !== listingId) rec = null; paint(); });
    input.addEventListener("change", read);
    input.addEventListener("keydown", ev => { if (ev.key === "Enter") { ev.preventDefault(); if (!check.disabled) check.click(); } });
    phraseIn.addEventListener("input", paint);
    phraseIn.addEventListener("keydown", ev => { if (ev.key === "Enter") { ev.preventDefault(); if (!go.disabled) go.click(); } });

    check.onclick = () => run(check, "Check the listing", "Checking…", "Reading the listing on Etsy and keeping a copy. This takes a few seconds; nothing is written to Etsy.", async () => {
      listingId = listingIdOf(input.value); if (!listingId) return null;
      const r = await call("zodiacListingPrepare", {}, { title: "Check an Etsy listing", what: `Reading Etsy listing ${listingId} (two Etsy calls) and keeping a safety copy. Nothing is written to Etsy.`, target: `Etsy ${listingId}`, go: "Check" });
      rec = r && typeof r === "object" ? Object.assign({ listingId }, r) : null; phraseIn.value = "";
      return rec && rec.ok === false && !rec.state ? { text: cap(stop(rec.text || "Not checked")), kind: "bad" } : null;
    });
    go.onclick = () => {
      const phrase = (rec && rec.phrase) || "ADD ZODIAC DROPDOWN " + listingId;
      if (busy || !rec || rec.ready !== true || !rec.payloadHash || phraseIn.value.trim() !== phrase) return;
      return run(go, "Add the drop-down", "Sending to Etsy…", "Reading the listing once more, then handing the change to Etsy. Do not press again.", async () => {
        const r = await call("zodiacListingWrite", { confirm: phraseIn.value.trim(), payloadHash: rec.payloadHash }, { title: "Change a live Etsy listing", what: `Adding the ZODIAC SIGN drop-down to Etsy listing ${listingId}. This changes the live listing and is not undone automatically.`, target: `Etsy ${listingId} + ZODIAC SIGN`, go: "Add the drop-down" });
        phraseIn.value = "";
        const accepted = !!(r && r.ok && (r.state === "writing" || r.state === "putting"));
        if (accepted) { rec = Object.assign({}, rec, { state: r.state }); paint(); watch(); }
        await read();
        return accepted ? null : { text: cap(stop((r && r.text) || "Not written. Nothing was sent")), kind: "bad" };
      });
    };
    settle.onclick = () => run(settle, "Check what Etsy holds", "Reading Etsy…", "Reading the listing on Etsy once, to see whether the change landed. Nothing is written.", async () => {
      const r = await call("zodiacListingReconcile", {}, { title: "Check an Etsy listing", what: `Reading Etsy listing ${listingId} once to see whether the change landed. Nothing is written to Etsy.`, target: `Etsy ${listingId}`, go: "Check" });
      await read();
      return r && r.ok === false && r.text ? { text: cap(stop(r.text)), kind: "bad" } : null;
    });
    paint();
    return { read, paint };
  }

  function auto() {
    if (!doc) return;
    const host = doc.getElementById("stZodiacEtsy");
    if (host) mount(host, { who: () => { try { return typeof employeeName === "function" ? employeeName() : ""; } catch (_) { return ""; } } });
  }
  if (doc) { if (doc.readyState === "loading") doc.addEventListener("DOMContentLoaded", auto); else auto(); }

  root.CharmNestZodiacEtsy = { mount, listingIdOf, lines };
})(typeof window !== "undefined" ? window : globalThis);
