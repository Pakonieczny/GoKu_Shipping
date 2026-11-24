/* charm-nest-nest-ask.js — NESTASK (Paul, 23 Nov 2026: "when the user presses the release hold button, show them a new pop-up Modal that asks the user to choose whether to Nest the given charm on a Sheet or to
   skip adding the charm to a sheet. Set default to Nest a given Charm"). One question, one click: Nest it (focused, so Enter is enough), Skip it, or Cancel / Esc, which does nothing at all.
     CNNestAsk.ask({ rid, orders, pieces: [{ row, sku, facts: [words] }], files: [{ thumb, name, facts }], metals: [long names], print, cannot }) → Promise<"nest" | "skip" | null>
   It only asks. What each answer does is ReviewRelease's (charm-nest-bridge.js: choose, press). */
(function (root) {
  "use strict";
  const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const MAX = 6;   // (pictures in a row; the rest are counted)
  const CSS = `dialog.naDlg{width:min(780px,94vw);max-height:94vh;padding:0;border:0;border-radius:18px;background:var(--card);color:var(--ink);box-shadow:0 28px 90px rgba(20,16,10,.38);overflow:auto}
dialog.naDlg::backdrop{background:rgba(20,18,15,.55)}dialog.naDlg[open]{animation:naIn .16s cubic-bezier(.2,.8,.2,1)}@keyframes naIn{from{opacity:0;transform:translateY(8px)}}
.naBox{display:grid;grid-template-columns:300px minmax(0,1fr);gap:32px;padding:32px}
.naBox.many{grid-template-columns:minmax(0,1fr);gap:26px}
.naPics{display:grid;gap:12px;align-content:start;min-width:0}.naPic{position:relative;display:grid;place-items:center;width:100%;height:270px;border:1px solid var(--line);border-radius:14px;background:#fff;overflow:hidden;font:600 12px var(--sans);color:var(--ink45)}
.naPic img,.naPic canvas{position:absolute;inset:0;width:100%;height:100%;object-fit:contain}.naPic.file img{inset:9%;width:82%;height:82%}.naPic.photo{height:150px}.naPic.photo img{object-fit:cover}
.naBox.many .naPics{display:flex;flex-wrap:wrap;gap:18px}.naBox.many .naPc{width:158px;display:grid;gap:8px;align-content:start}
.naBox.many .naPic{height:158px}.naBox.many .naPic.photo{display:none}
.naPc>span{font:12.5px/1.45 var(--sans);color:var(--ink70);overflow-wrap:anywhere}.naPc>span b{display:block;color:var(--ink);font-weight:650}.naMore{align-self:center;font:600 15px var(--sans);color:var(--ink45)}
.naAsk{display:flex;flex-direction:column;min-width:0}.naOrd{font:650 11px/1.3 var(--sans);letter-spacing:.09em;text-transform:uppercase;color:var(--gold)}
.naAsk h3{margin:7px 0 14px;font:400 31px/1.14 var(--serif);text-wrap:balance}
.naSku{font:600 16px/1.35 var(--mono);overflow-wrap:anywhere;margin-bottom:11px}
.naFacts{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:26px}.naFacts span{padding:6px 12px;border:1px solid var(--line);border-radius:999px;background:var(--card2);font:600 13px/1.2 var(--sans)}
.naGo{display:grid;gap:13px}.naBox.many .naGo{grid-template-columns:1fr 1fr}
.naCard{display:block;width:100%;text-align:left;padding:20px 22px;border:2px solid var(--line);border-radius:14px;background:var(--card);color:var(--ink);cursor:pointer;transition:border-color .12s,background .12s,box-shadow .12s}
.naCard b{display:block;font:400 24px/1.2 var(--serif)}.naCard i{display:block;margin-top:5px;font:normal 14px/1.45 var(--sans);color:var(--ink70)}
.naCard:hover{background:var(--card2);border-color:var(--ink25)}.naCard.nest{background:#d8b86f;border-color:var(--gold);color:#2a2013}.naCard.nest:hover{background:var(--gold2)}.naCard.nest i{color:#4d3c17}
.naCard:focus{outline:none;box-shadow:0 0 0 4px rgba(169,130,63,.34)}.naCard.skip:focus{border-color:var(--ink45);box-shadow:0 0 0 4px rgba(91,85,76,.2)}
.naFoot{display:flex;align-items:center;gap:14px;margin-top:16px;font:12.5px/1.45 var(--sans);color:var(--ink45)}.naFoot button{border:0;background:none;padding:6px 10px;margin:0 -10px 0 auto;border-radius:8px;font:600 13px var(--sans);color:var(--ink70);cursor:pointer}.naFoot button:hover{background:var(--paper2)}
@media(max-width:720px){.naBox,.naBox.many{grid-template-columns:minmax(0,1fr);gap:20px;padding:22px}.naBox:not(.many) .naPics:has(.photo){grid-template-columns:1fr 1fr}.naBox:not(.many) .naPic{height:170px}
.naBox.many .naGo{grid-template-columns:1fr}.naAsk h3{font-size:26px}.naFacts{margin-bottom:20px}.naBox.many .naPc{width:calc(50% - 9px)}}
@media(prefers-reduced-motion:reduce){dialog.naDlg[open]{animation:none}}`;
  let open = null;
  function ask(o) {
    if (open) return open.p;
    const doc = root.document, LM = root.ListMedia, pcs = (o.pieces || []).filter(Boolean), files = (o.files || []).filter(Boolean), all = pcs.concat(files), many = all.length > 1, one = all[0] || {};
    if (!doc.getElementById("naCss")) { const s = doc.createElement("style"); s.id = "naCss"; s.textContent = CSS; doc.head.appendChild(s); }
    const photo = x => (LM && LM.peek && x.row.line ? LM.peek(x.row.line.listingId) : null);
    const pic = x => x.row ? `<div class="naPic" data-na-vector></div>${photo(x) ? `<div class="naPic photo"><img alt="Etsy listing photo" src="${esc(photo(x))}"></div>` : ""}` : `<div class="naPic file">${x.thumb ? `<img alt="" src="${esc(x.thumb)}">` : esc(x.name || "Design")}</div>`;
    const facts = x => (x.facts || []).filter(Boolean), it = many ? "them" : "it", charm = many ? "these charms" : "this charm", metals = o.metals || [];
    const where = o.cannot ? `It cannot go on a sheet as it is: ${o.cannot}` : `${many ? "Go" : "Goes"} on the next ${metals.length === 1 ? metals[0] + " " : ""}sheet${metals.length > 1 ? "s" : ""} with room`;
    const d = doc.createElement("dialog"); d.className = "naDlg"; d.setAttribute("aria-labelledby", "naT"); d.dataset.rid = String(o.rid || "");
    d.innerHTML = `<div class="naBox${many ? " many" : ""}"><div class="naPics">${all.slice(0, MAX).map(x => many ? `<div class="naPc">${pic(x)}<span><b>${esc(x.sku || x.name)}</b>${esc(facts(x).join(" · "))}</span></div>` : pic(x)).join("")}${all.length > MAX ? `<span class="naMore">+ ${all.length - MAX} more</span>` : ""}</div>
      <div class="naAsk"><span class="naOrd">${o.orders > 1 ? `${o.orders} orders` : "Order " + esc(o.rid)}</span><h3 id="naT">Put ${charm} on a sheet?</h3>
        ${many ? "" : `<div class="naSku">${esc(one.sku || one.name)}</div><div class="naFacts">${facts(one).map(f => `<span>${esc(f)}</span>`).join("")}</div>`}
        <div class="naGo"><button type="button" class="naCard nest" data-na="nest"><b>Nest ${it}</b><i>${esc(where)}</i></button><button type="button" class="naCard skip" data-na="skip"><b>Skip ${it}</b><i>Release the order. No charm is made.</i></button></div>
        <div class="naFoot">${o.print ? "<span>The QR label prints either way.</span>" : ""}<button type="button" data-na="">Cancel</button></div></div></div>`;
    const from = doc.activeElement; let done;
    const p = new Promise(r => { done = r; });
    open = { p, d };
    const end = v => { if (!open) return; open = null; d.close(); d.remove(); if (v === null && from && from.isConnected && from.focus) from.focus({ preventScroll: true }); done(v); };
    d.querySelectorAll("[data-na]").forEach(b => b.onclick = () => end(b.dataset.na || null));
    d.addEventListener("cancel", e => { e.preventDefault(); end(null); });   // Esc
    d.addEventListener("close", () => end(null));
    d.addEventListener("keydown", e => {
      e.stopPropagation();
      if (!["ArrowUp", "ArrowDown", "ArrowLeft", "ArrowRight"].includes(e.key)) return;
      e.preventDefault(); const c = [...d.querySelectorAll(".naCard")]; c[c.indexOf(doc.activeElement) === 0 ? 1 : 0].focus();
    });
    doc.body.appendChild(d); d.showModal();
    d.querySelectorAll("[data-na-vector]").forEach((n, i) => { if (LM && pcs[i]) LM.vectorInto(n, pcs[i].row); });
    const nest = d.querySelector(o.cannot ? ".naCard.skip" : ".naCard.nest"); nest.focus();
    d.addEventListener("focusout", () => setTimeout(() => { if (open && open.d === d && !(doc.activeElement && doc.activeElement.closest("button") && d.contains(doc.activeElement))) nest.focus(); }, 0));   // (a press on the page's empty part, or the page taking the focus: Enter is Nest still)
    return p;
  }
  root.CNNestAsk = { ask, isOpen: () => !!open };
})(typeof window !== "undefined" ? window : globalThis);
