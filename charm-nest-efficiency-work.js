/*  charm-nest-efficiency-work.js — "Completed with Complete Order" in the Employee efficiency console (Paul, 9 Nov 2026: "This also needs to be linked to
 *  the employee efficiency tab so that I can clearly track what the employees did when they completed orders").
 *  One small table, drawn the same on the Overview (everybody, per person and station, with an "All" line per station) and on a person's page:
 *    orders completed and pieces completed (Complete Order presses only: a scan only opens an order, never work done), average and median time
 *    from the order being opened at the station to Complete Order, and orders opened but never completed.
 *  The rows come from the server's answer (employeeEfficiency op overview: people[].work and business.work; op person: work), read by
 *  netlify/functions/_stationWork.js from the rollups it already reads. No request of its own, nothing stored.
 *
 *    EfficiencyWork.render(host, rows, { people: true|false, all: rows?, empty: "text" })   rows: [{ name?, station, completed, pieces, avgMin,
 *        medianMin, viewedOnly }]  */
(function (root) {
  "use strict";
  if (root.EfficiencyWork) return;
  const doc = root.document;
  const NAMES = { welding: "Welding", assembly: "Assembly", shipping: "Shipping" };
  const N = v => { const x = Number(v); return Number.isFinite(x) ? x : 0; };
  const esc = s => String(s == null ? "" : s).replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
  const mins = v => { if (v == null || !Number.isFinite(Number(v))) return "—"; const m = Number(v); if (m < 1) return Math.max(1, Math.round(m * 60)) + " s"; if (m < 60) return (Math.round(m * 10) / 10) + " min"; return Math.floor(m / 60) + " h " + Math.round(m % 60) + " min"; };
  const when = at => { try { return at > 1e12 ? new Date(at).toLocaleString(undefined, { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }) : ""; } catch (_) { return ""; } };
  function css() {
    if (doc.getElementById("ewkStyle")) return;
    const s = doc.createElement("style"); s.id = "ewkStyle";
    s.textContent = `
.ewk{display:grid;gap:2px;font-size:13px;min-width:0}
.ewkH,.ewkR{display:grid;grid-template-columns:minmax(0,1.4fr) minmax(0,.9fr) repeat(5,minmax(0,.7fr));gap:8px;align-items:baseline;padding:7px 10px;min-width:0}
.ewk.solo .ewkH,.ewk.solo .ewkR{grid-template-columns:minmax(0,1fr) repeat(5,minmax(0,.7fr))}
.ewkH>*,.ewkR>*{min-width:0;overflow-wrap:anywhere}
.ewkH{font-size:11px;font-weight:700;letter-spacing:.04em;text-transform:uppercase;color:var(--ink45,#938c80);border-bottom:1px solid rgba(0,0,0,.07)}
.ewkR{border-radius:8px}.ewkR:nth-child(even){background:rgba(0,0,0,.025)}
.ewkR.all{font-weight:700;background:rgba(31,122,77,.07)}
.ewkR b{font-variant-numeric:tabular-nums}
.ewkN{font-variant-numeric:tabular-nums;text-align:right}
.ewkH .ewkN{text-align:right}
.ewkSt{color:var(--ink70,#5b554c)}
.ewkEmpty{padding:12px 10px;color:var(--ink45,#938c80)}
.ewkFoot{padding:6px 10px 2px;font-size:11.5px;color:var(--ink45,#938c80)}
@media (max-width:760px){.ewkH{display:none}.ewkR,.ewk.solo .ewkR{grid-template-columns:1fr 1fr 1fr}.ewkR>[data-l]::before{content:attr(data-l) " ";color:var(--ink45,#938c80);font-weight:400;font-size:11px}.ewkN{text-align:left}}`;
    (doc.head || doc.documentElement).appendChild(s);
  }
  function row(r, withName, cls) {
    const name = withName ? `<span data-l="">${esc(r.name || "")}</span>` : "";
    return `<div class="ewkR${cls ? " " + cls : ""}">${name}<span class="ewkSt" data-l="">${esc(NAMES[r.station] || r.station || "")}</span>`
      + `<b class="ewkN" data-l="Orders">${N(r.completed)}</b><span class="ewkN" data-l="Pieces">${N(r.pieces)}</span>`
      + `<span class="ewkN" data-l="Average">${mins(r.avgMin)}</span><span class="ewkN" data-l="Median">${mins(r.medianMin)}</span>`
      + `<span class="ewkN" data-l="Not completed">${N(r.viewedOnly)}</span></div>`;
  }
  function render(host, rows, o) {
    if (!host) return;
    o = o || {}; css();
    const people = o.people !== false, list = (Array.isArray(rows) ? rows : []).filter(r => r && NAMES[r.station]);
    const all = (Array.isArray(o.all) ? o.all : []).filter(r => r && NAMES[r.station]);
    const sig = JSON.stringify([people, list, all]);
    if (host._ewk === sig) return; host._ewk = sig;
    if (!list.length && !all.length) { host.innerHTML = `<div class="ewk"><div class="ewkEmpty">${esc(o.empty || "No order was completed with Complete Order in this period.")}</div></div>`; return; }
    const head = `<div class="ewkH">${people ? "<span>Person</span>" : ""}<span>Station</span><span class="ewkN" title="Orders completed with the Complete Order button">Orders</span><span class="ewkN" title="Pieces of the completed orders">Pieces</span><span class="ewkN" title="Average time from opening the order at the station to Complete Order">Average</span><span class="ewkN" title="Median time from opening the order at the station to Complete Order">Median</span><span class="ewkN" title="Orders opened (scanned) here and never completed">Not completed</span></div>`;
    const order = r => ["welding", "assembly", "shipping"].indexOf(r.station);
    const body = list.slice().sort((a, b) => (people ? String(a.name).localeCompare(String(b.name)) : 0) || order(a) - order(b)).map(r => row(r, people)).join("")
      + all.slice().sort((a, b) => order(a) - order(b)).map(r => row(Object.assign({ name: "All" }, r), people, "all")).join("");
    host.innerHTML = `<div class="ewk${people ? "" : " solo"}" role="table" aria-label="Orders completed with Complete Order">${head}${body}<div class="ewkFoot">Only a Complete Order press counts as work done; a scan only opens the order. Times run from opening the order at the station to Complete Order.</div></div>`;
  }
  root.EfficiencyWork = { render };
})(typeof window !== "undefined" ? window : globalThis);
