/*  charm-nest-efficiency-custom.js — the Employee efficiency console's Custom pieces tab (#efficiency/custom; Design & Engrave items 35 to 37 and D7).
 *  Paul, 28 Oct 2026: "all of this must be tracked by employee, and I need to know when employees are commissioning custom pieces, especially when they
 *  don't belong to an order that way we can correctly track our rates and who creates the errors and who is creating the solutions".
 *  For the day or range the console's bar shows (Efficiency.api.range):
 *    · four figures: custom pieces commissioned, of them with no order (flagged), fix pieces, mistakes put down to someone
 *    · per employee: commissioned, no order, fixes made, mistakes put down to them, orders worked and the mistake rate (mistakes ÷ orders worked),
 *      design saves; a name opens that person's page
 *    · per day (a range): commissioned, no order, fixes
 *    · every piece: when, who, the design and version, metal, quantity, the order (it opens the order window) or "Custom · no order" with its name
 *      and reason; a fix says which order it fixes, who made the mistake and, when the person changed it, who the order's history suggested
 *  Read-only: the console's gated function, op custom (netlify/functions/employeeEfficiency.js). Asked when the tab is shown, when the range or
 *  Real | Sandbox changes, and at most every 20 s while the console's own live read runs (it pauses when the page is hidden): no timer of its own.
 *    EfficiencyCustom.mount(el) → { destroy }   (charm-nest-efficiency.js mounts it while the tab is shown)
 *    EfficiencyCustom.norm(answer) → the view model (a missing field is zero or empty, never a crash)                                          */
(function (root) {
  "use strict";
  if (root.EfficiencyCustom) return;
  const doc = root.document;
  const options = { refreshMs: 20000 };
  const METAL = { gold: "GF", silver: "SS", rose: "RG", gold10k: "10K", gold14k: "14K" };
  const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const N = v => { v = +v; return Number.isFinite(v) ? v : 0; };
  const nf = n => Math.round(N(n)).toLocaleString("en-US");
  const pct = v => (v == null ? "—" : `${(Math.round(N(v) * 10) / 10).toString()}%`);
  const timeFmt = new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", hour: "numeric", minute: "2-digit" });
  const dayFmt = new Intl.DateTimeFormat("en-US", { timeZone: "UTC", weekday: "short", month: "short", day: "numeric" });
  const dayOf = d => { const [y, m, dd] = String(d).split("-").map(Number); return dayFmt.format(new Date(Date.UTC(y, m - 1, dd, 12))); };

  /** the view model */
  function norm(r) {
    r = r || {};
    const arr = v => (Array.isArray(v) ? v : []);
    const pieces = arr(r.pieces).filter(p => p && p.id).map(p => ({ id: String(p.id), at: N(p.at), day: String(p.day || ""), by: String(p.by || ""), orderId: String(p.orderId || ""), orderless: !p.orderId,
      customId: String(p.customId || ""), customName: String(p.customName || ""), reason: String(p.reason || ""), designId: String(p.designId || ""), version: N(p.version) || null, name: String(p.name || ""), sku: String(p.sku || ""),
      metal: String(p.metal || ""), quantity: Math.max(1, N(p.quantity)),
      fix: p.fix && typeof p.fix === "object" ? { orderId: String(p.fix.orderId || ""), customId: String(p.fix.customId || ""), causedBy: String(p.fix.causedBy || ""), suggested: String(p.fix.suggested || ""), changed: !!p.fix.changed, why: String(p.fix.why || "") } : null }));
    const people = arr(r.people).filter(p => p && p.name).map(p => ({ name: String(p.name), sent: N(p.sent), pieces: N(p.pieces), orderless: N(p.orderless), fixes: N(p.fixes), caused: N(p.caused), changed: N(p.changed),
      orders: N(p.orders), saved: N(p.saved), opened: N(p.opened), imported: N(p.imported), perDay: N(p.perDay), errorRate: p.errorRate == null ? null : N(p.errorRate), orderlessShare: p.orderlessShare == null ? null : N(p.orderlessShare) }));
    const byDay = arr(r.byDay).filter(d => d && d.day).map(d => ({ day: String(d.day), sent: N(d.sent), pieces: N(d.pieces), orderless: N(d.orderless), fixes: N(d.fixes), caused: N(d.caused) }));
    const t = r.totals || {};
    return { from: String(r.from || ""), to: String(r.to || ""), days: N(r.days) || 1, pieces, people, byDay,
      totals: { sent: N(t.sent), pieces: N(t.pieces), orderless: N(t.orderless), fixes: N(t.fixes), caused: N(t.caused), people: N(t.people) },
      notes: arr(r.notes).map(String).filter(Boolean), partial: !!r.partial };
  }

  function style() {
    if (doc.getElementById("efcStyle")) return;
    const s = doc.createElement("style"); s.id = "efcStyle";
    s.textContent = `
.efc{display:grid;gap:14px;min-width:0}
.efc .efKpis{grid-template-columns:repeat(4,minmax(0,1fr))}
.efc .efKV.flag{color:var(--clay)}
.efcTW{overflow-x:auto;min-width:0}
.efcT{width:100%;border-collapse:collapse;font-size:12.5px;font-variant-numeric:tabular-nums}
.efcT th{font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:var(--ink45);font-weight:700;text-align:right;padding:9px 12px 7px;white-space:nowrap;border-bottom:1px solid var(--line2)}
.efcT th:first-child,.efcT td:first-child{text-align:left}
.efcT td{padding:8px 12px;text-align:right;border-bottom:1px solid var(--line2);white-space:nowrap}
.efcT tr:last-child td{border-bottom:0}
.efcT .flag{color:var(--clay);font-weight:700}
.efcName{border:0;background:transparent;padding:0;font:650 12.5px var(--sans);color:var(--ink);cursor:pointer}.efcName:hover{text-decoration:underline}
.efcDays{display:grid;gap:3px;padding:10px 12px}
.efcDay{display:grid;grid-template-columns:96px minmax(0,1fr) auto;gap:10px;align-items:center;font-size:11.5px;color:var(--ink70)}
.efcBar{display:flex;height:10px;border-radius:3px;overflow:hidden;background:var(--line2);min-width:0}
.efcBar i{display:block;height:100%;background:var(--gold)}.efcBar i.no{background:var(--clay)}
.efcFilter{display:inline-flex;gap:4px;margin-left:auto}
.efcFilter button{border:1px solid var(--line);background:var(--card);border-radius:999px;padding:2px 10px;font:650 11px var(--sans);color:var(--ink70);letter-spacing:0;text-transform:none}
.efcFilter button.on{background:var(--ink);color:var(--card);border-color:var(--ink)}
.efcList{display:grid}
.efcRow{display:grid;grid-template-columns:72px minmax(0,1fr) auto;gap:4px 12px;padding:10px 14px;border-bottom:1px solid var(--line2);align-items:start}
.efcRow:last-child{border-bottom:0}
.efcRow.no{box-shadow:inset 3px 0 0 var(--clay)}
.efcWhen{font-size:11.5px;color:var(--ink45);font-variant-numeric:tabular-nums}
.efcWhat{display:grid;gap:3px;min-width:0}
.efcWhat b{font-weight:650}
.efcSub{font-size:11.5px;color:var(--ink70);overflow-wrap:anywhere}
.efcTag{display:inline-block;font:700 9.5px var(--mono);letter-spacing:.04em;padding:2px 6px;border-radius:4px;text-transform:uppercase;vertical-align:1px;margin-right:6px}
.efcTag.no{background:var(--clay);color:#fff}.efcTag.fix{background:var(--goldSoft);color:#7a5a1d}.efcTag.m{background:var(--card2);border:1px solid var(--line);color:var(--ink70)}
.efcOrd{border:0;background:transparent;padding:0;font:650 11.5px var(--mono);color:var(--gold);cursor:pointer}.efcOrd:hover{text-decoration:underline}
.efcEmpty{padding:26px 16px;text-align:center;color:var(--ink45);font-size:12.5px}
.efcNote{font-size:11.5px;color:var(--ink45);margin:0 2px}
@container ef (max-width:640px){.efc .efKpis{grid-template-columns:repeat(2,minmax(0,1fr))}.efc .efKpi:nth-child(3){border-left:0}.efcRow{grid-template-columns:minmax(0,1fr) auto}.efcWhen{grid-column:1/-1}.efcDay{grid-template-columns:72px minmax(0,1fr) auto}}`;
    doc.head.appendChild(s);
  }

  function mount(el) {
    const E = root.Efficiency, api = E && E.api;
    if (!api) throw new Error("the console is not loaded");
    style();
    const st = { M: null, busy: false, err: "", at: 0, gen: 0, ctl: null, filter: "all", sig: "", dead: false };
    el.innerHTML = `<div class="efc">
  <div class="efLoadMod efcWait" role="status"><span class="spin" aria-hidden="true"></span><span>Reading custom pieces…</span></div>
  <div class="efcBody" hidden>
    <section class="efCard" aria-label="Custom pieces at a glance"><div class="efKpis">
      <div class="efKpi" data-k="sent"><span class="efKL">Custom pieces</span><b class="efKV">0</b><span class="efKS"></span></div>
      <div class="efKpi" data-k="orderless"><span class="efKL">No order</span><b class="efKV">0</b><span class="efKS">Not part of an Etsy order</span></div>
      <div class="efKpi" data-k="fixes"><span class="efKL">Fix pieces</span><b class="efKV">0</b><span class="efKS">Made to fix a mistake</span></div>
      <div class="efKpi" data-k="caused"><span class="efKL">Mistakes</span><b class="efKV">0</b><span class="efKS">Put down to a person</span></div>
    </div></section>
    <p class="efcNote" hidden></p>
    <section aria-label="By employee"><div class="efLabel">By employee</div><div class="efCard efcTW"><table class="efcT"><thead><tr><th>Person</th><th title="Custom pieces this person sent to a sheet">Commissioned</th><th title="Of them, pieces that belong to no Etsy order">No order</th><th title="Pieces this person made to fix a mistake">Fixes made</th><th title="Fix pieces whose mistake was put down to this person">Mistakes</th><th title="Different orders this person worked in these days">Orders worked</th><th title="Mistakes put down to this person, per order worked">Mistake rate</th><th title="Designs and artwork this person saved in the Charm Editor">Design saves</th></tr></thead><tbody class="efcPeople"></tbody></table></div></section>
    <section class="efcDaysSec" aria-label="By day"><div class="efLabel">By day</div><div class="efCard efcDays"></div></section>
    <section aria-label="Every custom piece"><div class="efLabel">Pieces <b class="efcN"></b><span class="efcFilter" role="group" aria-label="Show"><button type="button" data-f="all" class="on">All</button><button type="button" data-f="no">No order</button><button type="button" data-f="fix">Fixes</button></span></div><div class="efCard efcList"></div></section>
  </div>
</div>`;
    const q = s => el.querySelector(s);
    const V = { wait: q(".efcWait"), body: q(".efcBody"), note: q(".efcNote"), people: q(".efcPeople"), daysSec: q(".efcDaysSec"), days: q(".efcDays"), list: q(".efcList"), n: q(".efcN"),
      kpi: Object.fromEntries([...el.querySelectorAll(".efKpi")].map(k => [k.dataset.k, k.querySelector(".efKV")])), kpiSent: el.querySelector('.efKpi[data-k="sent"] .efKS') };

    async function load(why) {
      if (st.dead) return;
      if (st.ctl) { try { st.ctl.abort(); } catch (_) {} }
      const gen = ++st.gen, R = api.range();
      st.busy = true; st.ctl = root.AbortController ? new AbortController() : null;
      if (!st.M || why === "range") { V.wait.hidden = false; V.wait.lastChild.textContent = "Reading custom pieces…"; V.body.style.opacity = st.M ? ".55" : ""; }
      try {
        const j = await api.call({ op: "custom", day: R.today ? undefined : R.day, days: R.days }, { signal: st.ctl && st.ctl.signal });
        if (st.dead || gen !== st.gen) return;
        st.M = norm(j); st.err = ""; st.at = Date.now(); paint();
      } catch (e) {
        if (st.dead || gen !== st.gen || (e && e.name === "AbortError")) return;
        st.err = (e && e.message) || "The custom pieces could not be read";
        if (!st.M) { V.wait.hidden = false; V.wait.innerHTML = `<span class="efKeyErr"></span><button type="button" data-retry>Try again</button>`; V.wait.firstChild.textContent = "The custom pieces could not be read just now."; }
        else { V.wait.hidden = true; V.note.hidden = false; V.note.textContent = "Not updated just now: the pieces shown are from the last answer. They are asked again in a moment."; st.sig = ""; st.at = Date.now() - options.refreshMs / 2; }   // (what was read stays, said plainly; the console's live read asks again)
      } finally { if (gen === st.gen) { st.busy = false; V.body.style.opacity = ""; } }
    }
    function paint() {
      const M = st.M; if (!M) return;
      V.wait.hidden = true; V.body.hidden = false;
      const sig = JSON.stringify([M.totals, M.people, M.byDay, M.pieces.map(p => [p.id, p.fix && p.fix.causedBy]), st.filter, M.notes]);
      if (sig === st.sig) return; st.sig = sig;
      const t = M.totals;
      V.kpi.sent.textContent = nf(t.sent); V.kpi.orderless.textContent = nf(t.orderless); V.kpi.fixes.textContent = nf(t.fixes); V.kpi.caused.textContent = nf(t.caused);
      V.kpi.orderless.classList.toggle("flag", t.orderless > 0);
      V.kpiSent.textContent = t.sent ? `${nf(t.pieces)} ${t.pieces === 1 ? "piece" : "pieces"} · ${nf(t.people)} ${t.people === 1 ? "person" : "people"}` : M.days > 1 ? `In ${M.days} days` : "Sent to a sheet";
      V.note.hidden = !M.notes.length; V.note.textContent = M.notes.join(" ");
      // per employee
      V.people.innerHTML = M.people.length ? M.people.map(p => `<tr data-name="${esc(p.name)}"><td><button type="button" class="efcName" data-person="${esc(p.name)}">${esc(p.name)}</button></td><td>${p.sent ? nf(p.sent) : "—"}</td><td${p.orderless ? ' class="flag"' : ""}>${p.orderless ? nf(p.orderless) : "—"}</td><td>${p.fixes ? nf(p.fixes) : "—"}</td><td${p.caused ? ' class="flag"' : ""}>${p.caused ? nf(p.caused) : "—"}</td><td>${p.orders ? nf(p.orders) : "—"}</td><td${p.errorRate ? ' class="flag"' : ""} title="${p.caused ? `${nf(p.caused)} of ${nf(p.orders)} orders worked` : ""}">${p.orders ? pct(p.errorRate) : "—"}</td><td>${p.saved ? nf(p.saved) : "—"}</td></tr>`).join("")
        : `<tr><td colspan="8" class="efcEmpty">No custom pieces and no design work in these days.</td></tr>`;
      // per day (a range)
      V.daysSec.hidden = M.days <= 1;
      if (M.days > 1) {
        const max = Math.max(1, ...M.byDay.map(d => d.sent));
        V.days.innerHTML = M.byDay.slice().reverse().map(d => `<div class="efcDay" data-day="${esc(d.day)}"><span>${esc(dayOf(d.day))}</span><span class="efcBar" title="${nf(d.sent)} commissioned · ${nf(d.orderless)} with no order · ${nf(d.fixes)} fixes"><i style="width:${((d.sent - d.orderless) / max * 100).toFixed(1)}%"></i><i class="no" style="width:${(d.orderless / max * 100).toFixed(1)}%"></i></span><span>${d.sent ? `${nf(d.sent)}${d.orderless ? ` · <b class="flag">${nf(d.orderless)} no order</b>` : ""}${d.fixes ? ` · ${nf(d.fixes)} ${d.fixes === 1 ? "fix" : "fixes"}` : ""}` : "—"}</span></div>`).join("");
      }
      paintList();
    }
    function paintList() {
      const M = st.M, f = st.filter, list = M.pieces.filter(p => f === "all" || (f === "no" && p.orderless) || (f === "fix" && p.fix));
      el.querySelectorAll(".efcFilter button").forEach(b => { const on = b.dataset.f === f; b.classList.toggle("on", on); b.setAttribute("aria-pressed", on); });
      V.n.textContent = M.pieces.length ? nf(list.length) : "";
      V.list.innerHTML = list.length ? list.map(p => {
        const metal = METAL[p.metal] || "", design = `${p.name || p.sku || p.designId || "Custom piece"}${p.version ? ` v${p.version}` : ""}`;
        const order = p.orderless ? `<span class="efcTag no">Custom · no order</span>${p.customName ? `<b>${esc(p.customName)}</b>` : ""}${p.customId ? ` <span class="efcSub">${esc(p.customId)}</span>` : ""}`
          : `Order <button type="button" class="efcOrd" data-order="${esc(p.orderId)}">${esc(p.orderId)}</button>${p.customName ? ` · ${esc(p.customName)}` : ""}`;
        const fx = p.fix ? `<div class="efcSub"><span class="efcTag fix">Fix</span>Fixes ${p.fix.orderId ? `order <button type="button" class="efcOrd" data-order="${esc(p.fix.orderId)}">${esc(p.fix.orderId)}</button>` : esc(p.fix.customId || "an earlier piece")} · ${p.fix.causedBy ? `mistake by <b>${esc(p.fix.causedBy)}</b>` : "mistake not put down to anyone"}${p.fix.changed && p.fix.suggested ? ` (the order's history said ${esc(p.fix.suggested)})` : ""} · fixed by <b>${esc(p.by)}</b></div>` : "";
        return `<div class="efcRow${p.orderless ? " no" : ""}" data-id="${esc(p.id)}"><span class="efcWhen">${p.at ? esc(timeFmt.format(new Date(p.at))) : ""}${M.days > 1 && p.day ? `<br>${esc(dayOf(p.day))}` : ""}</span>
  <div class="efcWhat"><div>${order}</div><div class="efcSub">${esc(design)}${metal ? ` · <span class="efcTag m">${metal}</span>` : ""}${p.quantity > 1 ? ` × ${nf(p.quantity)}` : ""} · commissioned by <button type="button" class="efcName" data-person="${esc(p.by)}">${esc(p.by)}</button></div>${p.reason ? `<div class="efcSub">Reason: ${esc(p.reason)}</div>` : ""}${fx}</div>
  <span></span></div>`;
      }).join("") : `<div class="efcEmpty">${M.pieces.length ? "None of this kind in these days." : M.days > 1 ? `No custom pieces in these ${M.days} days.` : "No custom pieces this day."}</div>`;
    }
    const onClick = e => {
      const b = e.target && e.target.closest && e.target.closest("button"); if (!b || !el.contains(b)) return;
      if (b.dataset.f) { st.filter = b.dataset.f; st.sig = ""; if (st.M) paint(); return; }
      if (b.dataset.person) return api.openPerson(b.dataset.person);
      if (b.dataset.order) return api.openOrder(b, b.dataset.order);
      if (b.hasAttribute("data-retry")) { V.wait.innerHTML = `<span class="spin" aria-hidden="true"></span><span>Reading custom pieces…</span>`; load("retry"); }
    };
    el.addEventListener("click", onClick);
    const offs = [api.onRange(() => load("range")), api.onView(() => { st.M = null; st.sig = ""; V.body.hidden = true; load("range"); }),
      api.onLive(() => { if (!st.busy && Date.now() - st.at >= options.refreshMs) load("live"); })];   // (the console's own live read paces it: nothing runs while the page is hidden)
    load("range");
    return { destroy() { st.dead = true; if (st.ctl) { try { st.ctl.abort(); } catch (_) {} } for (const off of offs) { try { off(); } catch (_) {} } el.removeEventListener("click", onClick); el.textContent = ""; } };
  }

  root.EfficiencyCustom = { mount, norm, options };
})(window);
