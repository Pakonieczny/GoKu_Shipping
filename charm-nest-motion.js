/* Charm Nest · motion and seals.
   Paul, 27 Sep 20:09-20:24: nothing a click moves may just vanish or pop up in front of the user. Whatever leaves a list
   is seen going where it goes: a card lifts, flies to the tab or switch that now holds it, and that tab answers with a
   pulse and a note that says what arrived, with Undo where there is one. Whatever stays glides into the gap, and whatever
   comes back flies in from where it was. Slow enough to follow (about a second and a half), quiet, and the same
   everywhere. Motion.reconcile is the one list update every keyed list uses: nodes carry data-mkey, a stable name for
   the thing they show, so a card redrawn under the same name glides instead of flashing.

   The seals (Paul, 20:09-20:18): each completion of a custom order is stamped where it was done. "QR label printed" is
   a round blue postmark with a QR mark; "Order completed" (the Complete Order button, no label printed) is a green
   scalloped seal with a check. Each carries the date, the time and the name of the person who pressed, in the centre
   where they read best. Every later print adds a seal of its own, so a card can carry both kinds. A new seal is pressed
   by a wooden stamp that comes down on the button, and the button takes the seal's colour. The seals never block the
   button under them: a press on a seal over the button presses the button (the seal sinks with it); a press on a seal
   elsewhere only makes it wobble. */
(function (root) {
  "use strict";
  const doc = root.document;
  const reduced = () => { try { return root.matchMedia("(prefers-reduced-motion: reduce)").matches; } catch (_) { return false; } };
  // how long each part takes (ms): slow enough to follow, never so slow it keeps anyone waiting (clicks go on meanwhile)
  const T = { stamp: 1200, hold: 700, fly: 1500, slide: 760, fade: 480, grow: 900 };
  const wait = ms => new Promise(r => setTimeout(r, ms));
  const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const resolve = to => { try { const e = typeof to === "function" ? to() : typeof to === "string" ? doc.querySelector(to) : to; return e && e.isConnected ? e : null; } catch (_) { return null; } };
  const visible = e => { if (!e || !e.isConnected) return false; const r = e.getBoundingClientRect(); return r.width > 0 && r.height > 0 && r.bottom > 0 && r.top < innerHeight && r.right > 0 && r.left < innerWidth; };
  /** The layer copies fly on: the page's own, or, for something inside an open window (a modal dialog sits above the
   *  whole page), one inside that window. near: the element the motion starts from or its list. */
  function layer(near) {
    const dlg = near && near.closest ? near.closest("dialog[open]") : null;
    if (dlg) { let l = dlg.querySelector(":scope > .motionLayer"); if (!l) { l = doc.createElement("div"); l.className = "motionLayer"; l.setAttribute("aria-hidden", "true"); dlg.appendChild(l); } return l; }
    let l = doc.getElementById("motionLayer");
    if (!l) { l = doc.createElement("div"); l.id = "motionLayer"; l.setAttribute("aria-hidden", "true"); doc.body.appendChild(l); }
    return l;
  }
  /** The nearest ancestor that answers container queries: a copy is laid out at the same width, so it looks the same. */
  function containerOf(n, self) {
    for (let a = self ? n : n.parentElement; a && a !== doc.body; a = a.parentElement) { const ct = getComputedStyle(a).containerType; if (ct && ct !== "normal") return a; }
    return null;
  }
  const innerWidthOf = c => { const cs = getComputedStyle(c); return c.clientWidth - (parseFloat(cs.paddingLeft) || 0) - (parseFloat(cs.paddingRight) || 0); };
  /** A still copy of `node` where it stands, on the motion layer (canvases carry their pixels over). */
  function ghost(node, rect, cw, near) {
    rect = rect || node.getBoundingClientRect();
    const g = node.cloneNode(true);
    g.removeAttribute("id"); for (const x of g.querySelectorAll("[id]")) x.removeAttribute("id");
    const a = node.querySelectorAll("canvas"), b = g.querySelectorAll("canvas");
    a.forEach((c, i) => { try { const d = b[i]; d.width = c.width; d.height = c.height; d.getContext("2d").drawImage(c, 0, 0); } catch (_) {} });
    const box = doc.createElement("div"); box.className = "mGhost";
    // laid out at its list's width (a node already taken out of its list is given that width by the caller)
    if (cw == null && node.isConnected) { const ctr = containerOf(node); cw = ctr ? innerWidthOf(ctr) : null; }
    Object.assign(box.style, { left: rect.left + "px", top: rect.top + "px", width: Math.max(rect.width, cw || 0) + "px", height: rect.height + "px", transformOrigin: rect.width / 2 + "px " + rect.height / 2 + "px" });
    if (cw) box.style.containerType = "inline-size";
    Object.assign(g.style, { width: rect.width + "px", height: rect.height + "px", margin: "0", boxSizing: "border-box", animation: "none", transform: "none" });
    g.classList.add("mCopy");
    box.appendChild(g); box.inert = true; layer(near || (node.isConnected ? node : null)).appendChild(box);
    box._rect = rect; box._card = g;
    return box;
  }
  /** Where a moved thing is now: the target lifts and settles once, and a "+1" rises from it. */
  function arrive(to, opts = {}) {
    const t = resolve(to); if (!t) return;
    if (!reduced()) t.animate([{ transform: "scale(1)" }, { transform: "scale(1.16)", offset: .32 }, { transform: "scale(.97)", offset: .62 }, { transform: "scale(1)" }], { duration: 760, easing: "ease-out" });
    t.classList.remove("mGot"); void t.offsetWidth; t.classList.add("mGot"); setTimeout(() => t.classList.remove("mGot"), 1600);
    if (opts.plus !== false && !reduced()) {
      const r = t.getBoundingClientRect(), p = doc.createElement("span"); p.className = "mPlus" + (opts.tone ? " " + opts.tone : ""); p.textContent = opts.plus || "+1";
      Object.assign(p.style, { left: r.left + r.width / 2 + "px", top: r.top + "px" }); layer(t).appendChild(p);
      p.animate([{ transform: "translate(-50%,0) scale(.7)", opacity: 0 }, { transform: "translate(-50%,-14px) scale(1)", opacity: 1, offset: .3 }, { transform: "translate(-50%,-30px) scale(1)", opacity: 0 }], { duration: 1300, easing: "ease-out", fill: "forwards" }).finished.then(() => p.remove(), () => p.remove());
    }
    if (opts.note) note(to, opts.note);
  }
  /** The copy flies to `to` (an element, a selector or a function giving one): it lifts, curves up and over, shrinks
   *  into the target and fades as it lands. A target out of sight (or none) makes it fade where it is instead. */
  async function fly(g, to, opts = {}) {
    const t = resolve(to), r = g._rect || g.getBoundingClientRect(), ms = opts.ms || T.fly;
    if (!t || !visible(t) || reduced()) {
      await g.animate([{ opacity: 1, transform: "none" }, { opacity: 0, transform: "scale(.97)" }], { duration: reduced() ? 200 : T.fade, easing: "ease", fill: "forwards" }).finished.catch(() => {});
      g.remove(); if (t) arrive(t, opts); return;
    }
    const tr = t.getBoundingClientRect();
    const dx = tr.left + tr.width / 2 - (r.left + r.width / 2), dy = tr.top + tr.height / 2 - (r.top + r.height / 2);
    const s = Math.max(.03, Math.min(.4, (tr.height * 1.4) / r.height, (tr.width * 1.2) / r.width));
    const bend = Math.min(140, 36 + Math.abs(dx) * .12 + Math.abs(dy) * .08) * (dy <= 0 ? 1 : -.4);
    const card = g._card;
    if (card) card.animate([{ boxShadow: "0 1px 2px rgba(0,0,0,.06)" }, { boxShadow: "0 22px 44px rgba(30,24,16,.24)", offset: .16 }, { boxShadow: "0 10px 24px rgba(30,24,16,.16)", offset: .7 }, { boxShadow: "0 2px 6px rgba(30,24,16,.1)" }], { duration: ms, fill: "forwards" });
    const a = g.animate([
      { transform: "translate(0,0) scale(1)", opacity: 1 },
      { transform: "translate(0,-10px) scale(1.015)", opacity: 1, offset: .16 },
      { transform: `translate(${dx * .42}px,${dy * .42 - bend}px) scale(${(1 + s) / 2.3})`, opacity: 1, offset: .58 },
      { transform: `translate(${dx * .9}px,${dy * .9 - bend * .15}px) scale(${s * 1.25})`, opacity: .85, offset: .88 },
      { transform: `translate(${dx}px,${dy}px) scale(${s})`, opacity: 0 }
    ], { duration: ms, easing: "cubic-bezier(.5,.05,.3,1)", fill: "forwards" });
    setTimeout(() => arrive(to, opts), ms * .86);
    await a.finished.catch(() => {}); g.remove();
  }
  /** A copy of what arrives flies in from `from` into `node`'s place (the node shows once it has landed). */
  async function flyIn(from, node, opts = {}) {
    const f = resolve(from); if (!node || !node.isConnected) return;
    if (!f || !visible(f) || !visible(node) || reduced()) { grow(node); return; }
    const r = node.getBoundingClientRect(), fr = f.getBoundingClientRect(), g = ghost(node, r);
    node.style.visibility = "hidden";
    const dx = fr.left + fr.width / 2 - (r.left + r.width / 2), dy = fr.top + fr.height / 2 - (r.top + r.height / 2);
    const s = Math.max(.03, Math.min(.4, (fr.height * 1.4) / r.height, (fr.width * 1.2) / r.width));
    const a = g.animate([
      { transform: `translate(${dx}px,${dy}px) scale(${s})`, opacity: 0 },
      { transform: `translate(${dx * .6}px,${dy * .6 - 40}px) scale(${(1 + s) / 2.3})`, opacity: 1, offset: .38 },
      { transform: "translate(0,-8px) scale(1.012)", opacity: 1, offset: .82 },
      { transform: "translate(0,0) scale(1)", opacity: 1 }
    ], { duration: opts.ms || T.fly, easing: "cubic-bezier(.4,.05,.25,1)", fill: "forwards", delay: opts.delay || 0 });
    await a.finished.catch(() => {});
    node.style.visibility = ""; g.remove();
    node.animate([{ boxShadow: "0 0 0 3px rgba(169,130,63,.45)" }, { boxShadow: "0 0 0 0 rgba(169,130,63,0)" }], { duration: 1400, easing: "ease-out" });
  }
  /** Something new in a list opens its own room and fades in, rather than appearing all at once. */
  function grow(node, opts = {}) {
    if (!node || !node.isConnected || reduced()) return;
    const h = node.getBoundingClientRect().height; if (!h) return;
    node.animate([{ opacity: 0, transform: "translateY(-6px)", maxHeight: "0px", overflow: "hidden" }, { opacity: 0, maxHeight: h + "px", offset: .45, overflow: "hidden" }, { opacity: 1, transform: "none", maxHeight: h + "px", overflow: "hidden" }], { duration: opts.ms || T.grow, easing: "cubic-bezier(.3,.1,.2,1)", delay: opts.delay || 0, fill: "backwards" });
  }
  /** Something removed with no destination fades and folds away where it was. */
  async function fade(g) {
    await g.animate([{ opacity: 1, transform: "none", filter: "none" }, { opacity: 0, transform: "scale(.96)", filter: "blur(1px)" }], { duration: T.fade, easing: "ease-in", fill: "forwards" }).finished.catch(() => {});
    g.remove();
  }

  /* ── where things went: registered by the action just before its list redraws, read by that list's reconcile ── */
  const leaves = new Map(), arrivals = new Map();
  /** mkey leaves its list for `spec.to` (with spec.note, spec.stamp, spec.hold); good for a few seconds. */
  function expect(mkey, spec) { leaves.set(String(mkey), Object.assign({ until: Date.now() + (spec.ttl || 10000) }, spec)); }
  function expectIn(mkey, spec) { arrivals.set(String(mkey), Object.assign({ until: Date.now() + (spec.ttl || 10000) }, spec)); }
  const take = (m, k) => { const e = m.get(k); if (!e) return null; m.delete(k); return e.until > Date.now() ? e : null; };
  const pending = (k) => { const e = leaves.get(String(k)); return !!(e && e.until > Date.now()); };

  /** The one keyed list update: `nodes` in order, in `host`. With opts.animate (the same view as before, drawn already)
   *  what left is seen going (to where its action said, else it fades), what stayed glides into place, and what is new
   *  opens its room, or flies in from where its action said it came from. */
  function reconcile(host, nodes, opts = {}) {
    const on = opts.animate !== false && !reduced() && host.isConnected && host.getClientRects().length > 0;
    const before = new Map(), ctr = on ? containerOf(host, true) : null, cw = ctr ? innerWidthOf(ctr) : null;
    if (on) for (const n of host.children) { const k = n.dataset && n.dataset.mkey; if (k && !n._mLeaving) before.set(k, { node: n, rect: n.getBoundingClientRect() }); }
    const keep = new Set(nodes);
    nodes.forEach((node, i) => { if (host.children[i] !== node) host.insertBefore(node, host.children[i] || null); });
    for (const n of [...host.children]) if (!keep.has(n)) n.remove();
    if (!on || !before.size) { for (const n of nodes) if (n.dataset && n.dataset.mkey) take(arrivals, n.dataset.mkey); return; }
    const now = new Map(); for (const n of nodes) { const k = n.dataset && n.dataset.mkey; if (k) now.set(k, n); }
    const clip = (opts.clip || host.closest(".scroll") || host).getBoundingClientRect();
    const seen = r => r.bottom > clip.top && r.top < clip.bottom;
    // what left: a copy where it stood goes where it went (its stamp first, when it has one)
    let hold = 0; const gone = [];
    for (const [k, b] of before) {
      if (now.has(k)) continue;
      const spec = take(leaves, k) || (opts.leave ? opts.leave(k, b.node) : null);
      if (!seen(b.rect)) { if (spec && spec.to) arrive(spec.to, spec); continue; }
      const g = ghost(b.node, b.rect, cw, host);
      const h = spec && spec.stamp ? T.stamp + T.hold : 0; hold = Math.max(hold, h);
      gone.push([g, spec]);
    }
    for (const [g, spec] of gone) (async () => {
      if (spec && spec.stamp) { try { await Seal.stampOn(g, spec.stamp); } catch (_) {} await wait(T.hold); }
      if (spec && spec.to) await fly(g, spec.to, spec); else await fade(g);
    })();
    // what stayed glides from where it was (after a stamp, once the stamped card has lifted off)
    for (const [k, n] of now) {
      const b = before.get(k); if (!b) continue;
      const r = n.getBoundingClientRect(), dx = b.rect.left - r.left, dy = b.rect.top - r.top;
      if (Math.abs(dx) < 1 && Math.abs(dy) < 1) continue;
      if (!seen(r) && !seen(b.rect)) continue;
      n.animate([{ transform: `translate(${dx}px,${dy}px)` }, { transform: "none" }], { duration: T.slide, delay: hold ? hold + 120 : 0, easing: "cubic-bezier(.3,.1,.2,1)", fill: "backwards" });
    }
    // what is new: from where it came, or opening its own room
    for (const [k, n] of now) {
      if (before.has(k)) continue;
      const spec = take(arrivals, k);
      if (spec && spec.from) flyIn(spec.from, n, spec); else if (seen(n.getBoundingClientRect())) grow(n, { delay: hold ? hold + 120 : 0 });
    }
  }

  /* ── a note under where something went: what arrived, and Undo while it can be undone ── */
  const notes = [];
  function note(anchor, spec) {
    const a = resolve(anchor); if (!a || !spec) return null;
    const n = doc.createElement("div"); n.className = "mNote" + (spec.tone ? " " + spec.tone : ""); n.setAttribute("role", "status");
    n.innerHTML = `<i class="mNoteArrow"></i><span class="mNoteT"></span><span class="mNoteA"></span><i class="mNoteBar"></i>`;
    n.querySelector(".mNoteT").textContent = spec.text || "";
    let done = false;
    const close = () => { if (done) return; done = true; clearTimeout(n._t); const i = notes.indexOf(n); if (i >= 0) notes.splice(i, 1);
      n.animate([{ opacity: 1, transform: "none" }, { opacity: 0, transform: "translateY(-6px)" }], { duration: 320, easing: "ease-in", fill: "forwards" }).finished.then(() => { n.remove(); place(); }, () => n.remove()); };
    for (const act of spec.actions || []) {
      const b = doc.createElement("button"); b.type = "button"; b.className = "mNoteBtn"; b.textContent = act.label; if (act.title) b.title = act.title;
      b.onclick = e => { e.stopPropagation(); close(); try { act.fn(); } catch (_) {} };
      n.querySelector(".mNoteA").appendChild(b);
    }
    const x = doc.createElement("button"); x.type = "button"; x.className = "mNoteX"; x.setAttribute("aria-label", "Dismiss"); x.textContent = "×"; x.onclick = e => { e.stopPropagation(); close(); };
    n.querySelector(".mNoteA").appendChild(x);
    n._anchor = anchor; doc.body.appendChild(n); notes.push(n);
    while (notes.length > 3) { const old = notes.shift(); old.remove(); }
    const ms = spec.ms || 9000;
    n.querySelector(".mNoteBar").style.animationDuration = ms + "ms";
    let left = ms, from = Date.now();
    const arm = () => { from = Date.now(); n._t = setTimeout(close, left); };
    n.onmouseenter = () => { clearTimeout(n._t); left -= Date.now() - from; n.classList.add("paused"); };
    n.onmouseleave = () => { n.classList.remove("paused"); arm(); };
    arm(); place();
    if (!reduced()) n.animate([{ opacity: 0, transform: "translateY(-8px) scale(.98)" }, { opacity: 1, transform: "none" }], { duration: 420, easing: "cubic-bezier(.3,1.3,.5,1)" });
    n.close = close;
    return n;
  }
  /** Notes stand under their anchor, one under another. */
  function place() {
    let below = new Map();
    for (const n of notes) {
      const a = resolve(n._anchor); if (!a) continue;
      const r = a.getBoundingClientRect(), key = Math.round(r.left) + ":" + Math.round(r.bottom), y0 = below.get(key) || r.bottom + 10;
      const w = n.offsetWidth, x = Math.max(8, Math.min(innerWidth - w - 8, r.left + r.width / 2 - 26));
      n.style.left = x + "px"; n.style.top = y0 + "px";
      n.querySelector(".mNoteArrow").style.left = Math.max(12, Math.min(w - 20, r.left + r.width / 2 - x - 6)) + "px";
      n.querySelector(".mNoteArrow").hidden = below.has(key);
      below.set(key, y0 + n.offsetHeight + 6);
    }
  }
  addEventListener("resize", place);
  /** The target of a move, pulsed where it is (a move that could not be seen still says where it went). */
  function pulse(to, opts) { arrive(to, Object.assign({ plus: false }, opts || {})); }

  /* ════ Seals ════ */
  const Seal = (() => {
    const INK = { print: "#22408f", button: "#19663f" };
    let uid = 0;
    const hash = s => { let h = 2166136261; for (const c of String(s)) { h ^= c.charCodeAt(0); h = Math.imul(h, 16777619); } return h >>> 0; };
    /** Every seal of a record, oldest first: its stamps, or for a record from before them, what it kept. */
    function list(rec) {
      if (!rec) return [];
      let st = Array.isArray(rec.stamps) && rec.stamps.length ? rec.stamps.map(s => Object.assign({}, s)) : [];
      if (!st.length) {
        if (rec.how === "button" && rec.completedAt) st.push({ how: "button", at: +rec.completedAt, by: rec.completedBy || "" });
        if (rec.printedAt) st.push({ how: "print", at: +rec.printedAt, by: rec.printedBy || "" });
        if (+rec.prints > 1 && rec.lastPrintedAt && +rec.lastPrintedAt !== +rec.printedAt) st.push({ how: "print", at: +rec.lastPrintedAt, by: rec.lastPrintedBy || "" });
      }
      st.sort((a, b) => a.at - b.at);
      let n = 0; for (const s of st) if (s.how !== "button") s.n = ++n;
      const counted = +rec.prints || 0; if (counted > n) { let k = counted - n; for (const s of st) if (s.how !== "button") s.n += k; }
      return st;
    }
    const pt = (r, a) => [60 + r * Math.cos(a * Math.PI / 180), 60 + r * Math.sin(a * Math.PI / 180)];
    function arc(r, a0, a1, sweep) {
      const [x0, y0] = pt(r, a0), [x1, y1] = pt(r, a1), span = sweep ? (a1 - a0 + 360) % 360 : (a0 - a1 + 360) % 360;
      return `M${x0.toFixed(2)} ${y0.toFixed(2)}A${r} ${r} 0 ${span > 180 ? 1 : 0} ${sweep} ${x1.toFixed(2)} ${y1.toFixed(2)}`;
    }
    function star(a, r, s) {
      const [cx, cy] = pt(r, a); let d = "";
      for (let i = 0; i < 10; i++) { const rr = i % 2 ? s * .42 : s, t = (i * 36 - 90) * Math.PI / 180; d += (i ? "L" : "M") + (cx + rr * Math.cos(t)).toFixed(2) + " " + (cy + rr * Math.sin(t)).toFixed(2); }
      return `<path d="${d}Z" stroke="none"/>`;
    }
    function scallops(n, rv, rc) {
      let d = ""; const step = 360 / n;
      for (let i = 0; i < n; i++) { const a0 = i * step, am = a0 + step / 2, a1 = a0 + step, [x0, y0] = pt(rv, a0), [xm, ym] = pt(rc, am), [x1, y1] = pt(rv, a1); d += (i ? "" : `M${x0.toFixed(2)} ${y0.toFixed(2)}`) + `Q${xm.toFixed(2)} ${ym.toFixed(2)} ${x1.toFixed(2)} ${y1.toFixed(2)}`; }
      return `<path d="${d}Z" stroke-width="2.4"/>`;
    }
    // a small QR mark: three finder squares and a few modules
    const qr = `<g stroke="none"><path d="M51 22h7v7h-7zM52.4 23.4v4.2h4.2v-4.2zM62 22h7v7h-7zM63.4 23.4v4.2h4.2v-4.2zM51 33h7v7h-7zM52.4 34.4v4.2h4.2v-4.2z" fill-rule="evenodd"/><rect x="53.6" y="24.6" width="1.8" height="1.8"/><rect x="64.6" y="24.6" width="1.8" height="1.8"/><rect x="53.6" y="35.6" width="1.8" height="1.8"/><path d="M60 31h2v2h-2zM63 33h2v2h-2zM66 31h3v2h-3zM61 36h2v4h-2zM64 37h2v3h-2zM67 35h2v2h-2zM67 38h2v2h-2z"/></g>`;
    const check = `<path d="M51.5 31.5l5.2 5.4 11.8-12.4" fill="none" stroke-width="3.6" stroke-linecap="round" stroke-linejoin="round"/>`;
    const dateOf = t => { const d = new Date(+t || Date.now()); return `${String(d.getDate()).padStart(2, "0")} ${d.toLocaleString("en-US", { month: "short" }).toUpperCase()} ${d.getFullYear()}`; };
    const timeOf = t => new Date(+t || Date.now()).toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit" });
    const kindOf = st => st.how === "button" ? "button" : "print";
    const rotOf = st => { const h = hash(String(st.at) + (st.by || "")); return kindOf(st) === "button" ? 5 + (h % 8) : -(5 + (h % 9)); };
    const whoOf = st => String(st.by || "").trim() || "Sorting station";
    /** One seal as SVG: its ring words, and the date, time and name in the middle where they read best. */
    function svg(st) {
      const k = kindOf(st), ink = INK[k], id = "sl" + (++uid), seed = (hash(st.at + "|" + st.by) % 997) + 1;
      // the name, as big as the middle allows: a long one is shortened to the first name and an initial, then set narrower
      let nm = whoOf(st).toUpperCase().replace(/\s+/g, " ");
      if (nm.length > 13) { const w = nm.split(" "); nm = w.length > 1 ? `${w[0]} ${w[w.length - 1][0]}.` : nm; }
      if (nm.length > 16) nm = nm.slice(0, 15) + "…";
      const nfs = Math.min(9.4, (64 / Math.max(1, nm.length) - .5) / .68), fit = nfs < 7.4 ? ` textLength="64" lengthAdjust="spacingAndGlyphs"` : "";
      const top = k === "button" ? "ORDER COMPLETED" : "QR LABEL PRINTED";
      const foot = k === "button" ? "NO LABEL PRINTED" : `PRINT Nº ${st.n || 1}`;
      const edge = k === "button" ? scallops(30, 54.2, 58.4) + `<circle cx="60" cy="60" r="51.6" stroke-width="1"/>` : `<circle cx="60" cy="60" r="55.6" stroke-width="3.2"/><circle cx="60" cy="60" r="52" stroke-width=".9"/>`;
      return `<svg viewBox="0 0 120 120" aria-hidden="true" focusable="false"><defs>` +
        `<filter id="${id}f" x="-6%" y="-6%" width="112%" height="112%" color-interpolation-filters="sRGB">` +
        `<feTurbulence type="fractalNoise" baseFrequency=".05" numOctaves="2" seed="${seed}" result="lo"/>` +
        `<feDisplacementMap in="SourceGraphic" in2="lo" scale="1.8" xChannelSelector="R" yChannelSelector="G" result="rough"/>` +
        `<feTurbulence type="fractalNoise" baseFrequency=".95" numOctaves="1" seed="${seed + 11}" result="hi"/>` +
        `<feColorMatrix in="hi" type="matrix" values="0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 3.6 0 0 0 -.72" result="holes"/>` +
        `<feComposite in="rough" in2="holes" operator="in" result="inked"/>` +
        `<feColorMatrix in="lo" type="matrix" values="0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1.7 0 0 0 .12" result="press"/>` +
        `<feComposite in="inked" in2="press" operator="in"/></filter>` +
        `<path id="${id}t" d="${arc(44.2, 158, 22, 1)}"/><path id="${id}b" d="${arc(50.6, 143, 37, 0)}"/></defs>` +
        `<g filter="url(#${id}f)" fill="${ink}" stroke="${ink}">` +
        `<g fill="none">${edge}<circle cx="60" cy="60" r="41.4" stroke-width="1.3"/></g>` +
        `<text stroke="none" font-family="-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif" font-size="8.6" font-weight="800" letter-spacing="1.25"><textPath href="#${id}t" startOffset="50%" text-anchor="middle">${esc(top)}</textPath></text>` +
        `<text stroke="none" font-family="-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif" font-size="7" font-weight="800" letter-spacing="1.1"><textPath href="#${id}b" startOffset="50%" text-anchor="middle">${esc(foot)}</textPath></text>` +
        star(150, 47.4, 2.6) + star(30, 47.4, 2.6) +
        (k === "button" ? check : qr) +
        `<path d="M25 44.5h70M22 72.5h76" stroke-width="1" fill="none"/>` +
        `<text x="60" y="56.4" text-anchor="middle" stroke="none" font-family="ui-monospace,Menlo,Consolas,'Courier New',monospace" font-size="10.4" font-weight="800">${esc(dateOf(st.at))}</text>` +
        `<text x="60" y="68.2" text-anchor="middle" stroke="none" font-family="ui-monospace,Menlo,Consolas,'Courier New',monospace" font-size="9.8" font-weight="700">${esc(timeOf(st.at))}</text>` +
        `<text x="60" y="84" text-anchor="middle" stroke="none" font-family="-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif" font-size="${Math.max(7.4, nfs).toFixed(2)}" font-weight="800" letter-spacing=".5"${fit}>${esc(nm)}</text>` +
        `</g></svg>`;
    }
    function titleOf(st) {
      const d = new Date(+st.at || Date.now()), when = d.toLocaleString("en-US", { weekday: "short", month: "short", day: "numeric", year: "numeric", hour: "numeric", minute: "2-digit" });
      return kindOf(st) === "button" ? `Completed with the Complete Order button by ${whoOf(st)} · ${when} (no label printed then)` : `QR label printed by ${whoOf(st)} · ${when}${st.n > 1 ? ` · print ${st.n}` : ""}`;
    }
    /** One seal, as it sits on a card. */
    function html(st, size, extra) {
      return `<span class="seal seal-${kindOf(st)}${extra ? " " + extra : ""}" style="--rot:${rotOf(st)}deg;--sz:${size}px" title="${esc(titleOf(st))}" data-at="${+st.at || 0}">${svg(st)}</span>`;
    }
    const sizeFor = n => n <= 2 ? 112 : n <= 4 ? 100 : 88;
    /** The card's seals, oldest first, beside the button they belong with. pending: the time of a seal just pressed
     *  (it waits unseen until pressPending stamps it); size: a fixed size (the order window's small seals). */
    function row(rec, opts = {}) {
      const st = list(rec); if (!st.length) return "";
      const shown = st.slice(-8), more = st.length - shown.length, size = opts.size || sizeFor(shown.length);
      return `<span class="sealRow n${Math.min(shown.length, 5)}${opts.size ? " mini" : ""}" role="img" aria-label="${esc(st.map(titleOf).join("; "))}">` +
        (more ? `<span class="sealMore" title="${esc(st.slice(0, more).map(titleOf).join("\n"))}">+${more} earlier</span>` : "") +
        shown.map(s => html(s, size, opts.pending && +s.at >= opts.pending ? "pending" : "")).join("") + `</span>`;
    }
    /** Every seal waiting to be pressed, pressed where it shows (one out of sight just shows). */
    function pressPending() {
      for (const s of doc.querySelectorAll(".seal.pending")) {
        if (!visible(s) || reduced()) { s.classList.remove("pending"); continue; }
        press(s);
      }
    }
    /** The wooden stamp comes down on a seal already in its place, and leaves it inked. */
    async function press(seal) {
      const r = seal.getBoundingClientRect(), size = r.width / 1.0, kind = seal.classList.contains("seal-button") ? "button" : "print";
      const rot = parseFloat(getComputedStyle(seal).getPropertyValue("--rot")) || -8;
      const t = doc.createElement("span"); t.className = "sealTool"; t.innerHTML = tool(kind);
      Object.assign(t.style, { position: "fixed", left: r.left - size * .02 + "px", top: r.top - size * .02 + "px", width: size * 1.04 + "px", height: size * 1.04 + "px" });
      layer(seal).appendChild(t);
      const down = t.animate([
        { transform: `translate(34px,-46px) rotate(${rot - 16}deg) scale(1.7)`, opacity: 0, filter: "drop-shadow(30px 40px 18px rgba(20,14,6,.26))" },
        { opacity: 1, offset: .22 },
        { transform: `translate(0,0) rotate(${rot}deg) scale(1)`, opacity: 1, filter: "drop-shadow(2px 3px 3px rgba(20,14,6,.4))" }
      ], { duration: 560, easing: "cubic-bezier(.62,0,.92,.5)", fill: "forwards" });
      await down.finished.catch(() => {});
      seal.classList.remove("pending"); seal.classList.add("wet");
      const btn = btnOf(seal); if (btn) btn.classList.add(kind === "button" ? "sealedDone" : "sealedPrint");
      const ring = doc.createElement("span"); ring.className = "sealRing"; ring.style.cssText = `position:fixed;left:${r.left}px;top:${r.top}px;width:${r.width}px;height:${r.height}px;--ink:${INK[kind]}`; layer(seal).appendChild(ring);
      ring.animate([{ transform: "scale(.86)", opacity: .42 }, { transform: "scale(1.42)", opacity: 0 }], { duration: 760, easing: "cubic-bezier(.2,.7,.3,1)", fill: "forwards" }).finished.then(() => ring.remove(), () => ring.remove());
      const up = t.animate([
        { transform: `rotate(${rot}deg) scale(1)`, opacity: 1 },
        { transform: `rotate(${rot}deg) scale(.95)`, opacity: 1, offset: .22 },
        { transform: `translate(-16px,-40px) rotate(${rot + 8}deg) scale(1.55)`, opacity: 0 }
      ], { duration: 700, easing: "cubic-bezier(.2,.7,.3,1)", fill: "forwards" });
      await wait(260); seal.classList.remove("wet");
      await up.finished.catch(() => {}); t.remove();
    }
    const hasPrint = rec => list(rec).some(s => s.how !== "button");
    /** The wooden stamp seen from above: its knob, its turned base, the rubber's inked rim. */
    function tool(kind) {
      const ink = INK[kind], id = "st" + (++uid);
      return `<svg viewBox="0 0 120 120" aria-hidden="true"><defs>` +
        `<radialGradient id="${id}a" cx=".36" cy=".3" r=".85"><stop offset="0" stop-color="#d9a868"/><stop offset=".55" stop-color="#a66c35"/><stop offset="1" stop-color="#5d3818"/></radialGradient>` +
        `<radialGradient id="${id}b" cx=".38" cy=".3" r=".75"><stop offset="0" stop-color="#f6d6a0"/><stop offset=".5" stop-color="#c98e4e"/><stop offset="1" stop-color="#7c4a1f"/></radialGradient></defs>` +
        `<circle cx="60" cy="60" r="58" fill="${ink}"/><circle cx="60" cy="60" r="55" fill="url(#${id}a)"/>` +
        `<g fill="none" stroke="#3d240e" stroke-opacity=".16" stroke-width=".9"><ellipse cx="60" cy="60" rx="46" ry="44"/><ellipse cx="60" cy="61" rx="38" ry="35"/><path d="M18 52c14-6 26-5 40 0s26 6 44-1"/><path d="M20 72c16 5 30 4 42-1s24-5 38 1"/></g>` +
        `<circle cx="60" cy="60" r="47" fill="none" stroke="#2c1a09" stroke-opacity=".22" stroke-width="1.2"/>` +
        `<circle cx="61.5" cy="62" r="27" fill="#2c1a09" opacity=".25"/><circle cx="60" cy="60" r="26" fill="url(#${id}b)"/>` +
        `<ellipse cx="51" cy="50" rx="10" ry="6.5" fill="#fff" opacity=".28" transform="rotate(-24 51 50)"/></svg>`;
    }
    /** A new seal pressed onto `spec.btn` (a selector inside host, or an element): the stamp comes down, the ink is left,
     *  the stamp lifts away, and the button takes the seal's colour. */
    async function stampOn(host, spec) {
      const btn = typeof spec.btn === "string" ? host.querySelector(spec.btn) : spec.btn; if (!btn) return;
      const st = spec.stamp || spec, kind = kindOf(st), size = spec.size || 112;
      if (spec.label) btn.innerHTML = esc(spec.label);
      btn.disabled = false; btn.removeAttribute("aria-busy"); btn.classList.remove("working");
      const hr = host.getBoundingClientRect(), br = btn.getBoundingClientRect();
      const cx = br.left + Math.min(br.width * .64, br.width - 18) - hr.left, cy = br.top + br.height / 2 - hr.top;
      const s = doc.createElement("span"); s.innerHTML = html(st, size, "loose"); const seal = s.firstChild;
      Object.assign(seal.style, { left: cx - size / 2 + "px", top: cy - size / 2 + "px", opacity: "0" });
      const t = doc.createElement("span"); t.className = "sealTool"; t.innerHTML = tool(kind);
      Object.assign(t.style, { left: cx - size * .52 + "px", top: cy - size * .52 + "px", width: size * 1.04 + "px", height: size * 1.04 + "px" });
      host.appendChild(seal); host.appendChild(t);
      const rot = rotOf(st), paint = () => btn.classList.add(kind === "button" ? "sealedDone" : "sealedPrint");
      if (reduced()) { seal.style.opacity = ""; t.remove(); paint(); return; }
      const down = t.animate([
        { transform: `translate(34px,-46px) rotate(${rot - 16}deg) scale(1.7)`, opacity: 0, filter: "drop-shadow(30px 40px 18px rgba(20,14,6,.26))" },
        { opacity: 1, offset: .22 },
        { transform: `translate(0,0) rotate(${rot}deg) scale(1)`, opacity: 1, filter: "drop-shadow(2px 3px 3px rgba(20,14,6,.4))" }
      ], { duration: 560, easing: "cubic-bezier(.62,0,.92,.5)", fill: "forwards" });
      await down.finished.catch(() => {});
      // the press: ink on the paper and the button, a thud, a ring through the paper
      seal.style.opacity = ""; seal.classList.add("wet"); paint();
      const card = host._card || host;
      card.animate([{ transform: "translateY(0)" }, { transform: "translateY(1.6px)", offset: .3 }, { transform: "translateY(0)" }], { duration: 240, easing: "ease-out" });
      const ring = doc.createElement("span"); ring.className = "sealRing"; ring.style.cssText = `left:${cx - size / 2}px;top:${cy - size / 2}px;width:${size}px;height:${size}px;--ink:${INK[kind]}`; host.appendChild(ring);
      ring.animate([{ transform: "scale(.86)", opacity: .42 }, { transform: "scale(1.42)", opacity: 0 }], { duration: 760, easing: "cubic-bezier(.2,.7,.3,1)", fill: "forwards" }).finished.then(() => ring.remove(), () => ring.remove());
      const up = t.animate([
        { transform: `rotate(${rot}deg) scale(1)`, opacity: 1, filter: "drop-shadow(2px 3px 3px rgba(20,14,6,.4))" },
        { transform: `rotate(${rot}deg) scale(.95)`, opacity: 1, offset: .22 },
        { transform: `translate(-16px,-40px) rotate(${rot + 8}deg) scale(1.55)`, opacity: 0, filter: "drop-shadow(26px 36px 18px rgba(20,14,6,.2))" }
      ], { duration: 700, easing: "cubic-bezier(.2,.7,.3,1)", fill: "forwards" });
      await wait(260); seal.classList.remove("wet");
      await up.finished.catch(() => {}); t.remove();
    }
    /* a seal over its button: a press there presses the button (and the seal sinks with it); anywhere else it wobbles */
    const btnOf = s => { const r = s.closest(".sealRow"); return r && r.parentElement ? r.parentElement.querySelector("[data-seal-btn]") : null; };
    const inside = (el, e) => { const r = el.getBoundingClientRect(); return e.clientX >= r.left && e.clientX <= r.right && e.clientY >= r.top && e.clientY <= r.bottom; };
    let pressed = null;
    doc.addEventListener("pointermove", e => {
      const s = e.target.closest && e.target.closest(".sealRow .seal"), b = s && btnOf(s), on = !!(b && !b.disabled && inside(b, e));
      for (const x of doc.querySelectorAll(".btn.hov")) if (x !== (on ? b : null)) x.classList.remove("hov");
      if (on) b.classList.add("hov");
      if (s) s.style.cursor = on ? "pointer" : "";
    }, { passive: true });
    doc.addEventListener("pointerdown", e => {
      const s = e.target.closest && e.target.closest(".sealRow .seal"), b = s ? btnOf(s) : e.target.closest && e.target.closest("[data-seal-btn]");
      if (!b || b.disabled) return;
      if (s && !inside(b, e)) return;
      pressed = b; b.classList.add("act");
      const row = b.parentElement && b.parentElement.querySelector(".sealRow"); if (row) row.classList.add("press");
    });
    const release = () => { if (!pressed) return; pressed.classList.remove("act"); const row = pressed.parentElement && pressed.parentElement.querySelector(".sealRow"); if (row) { row.classList.remove("press"); row.classList.remove("spring"); void row.offsetWidth; row.classList.add("spring"); } pressed = null; };
    doc.addEventListener("pointerup", release); doc.addEventListener("pointercancel", release);
    doc.addEventListener("click", e => {
      const s = e.target.closest && e.target.closest(".sealRow .seal"); if (!s) return;
      e.stopPropagation(); e.preventDefault();
      const b = btnOf(s);
      if (b && !b.disabled && inside(b, e)) { b.click(); return; }
      s.classList.remove("wobble"); void s.offsetWidth; s.classList.add("wobble");
    }, true);
    return { list, html, row, svg, stampOn, press, pressPending, hasPrint, titleOf, INK };
  })();

  root.Motion = { T, ghost, fly, flyIn, grow, fade, arrive, pulse, note, expect, expectIn, pending, reconcile, reduced, wait };
  root.Seal = Seal;
})(typeof window !== "undefined" ? window : globalThis);
