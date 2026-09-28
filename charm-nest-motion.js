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
    // the lift shadow is a still layer under the copy that only fades (a shadow animated per frame repainted the whole
    // card on every frame: a big card's flight stuttered)
    if (g._card) {
      const sh = doc.createElement("i"); sh.className = "mLift"; g.insertBefore(sh, g._card);
      Object.assign(sh.style, { width: g._card.style.width, height: g._card.style.height, borderRadius: getComputedStyle(g._card).borderRadius });
      sh.animate([{ opacity: 0 }, { opacity: 1, offset: .16 }, { opacity: .6, offset: .7 }, { opacity: .2 }], { duration: ms, fill: "forwards" });
    }
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
  /** What `node`'s room moves: each element after it on screen (up to its window, or the page) with how far it stands
   *  from where it would be without `node` (dy, negative: it would stand higher). Measured once, by layout, so an
   *  opening or closing room is played on transforms alone: what follows glides, and the frame round it (a window's
   *  edge) takes its new size once. */
  function shift(node) {
    const bound = node.closest("dialog") || doc.body, list = [];
    for (let a = node; a && a !== bound && a !== doc.body && list.length < 400; a = a.parentElement)
      for (let s = a.nextElementSibling; s && list.length < 400; s = s.nextElementSibling) list.push(s);
    if (!list.length) return [];
    const before = list.map(s => s.getBoundingClientRect());
    const was = node.style.display; node.style.display = "none";
    const after = list.map(s => s.getBoundingClientRect().top);
    node.style.display = was;
    const seen = r => r.height > 0 && r.bottom > 0 && r.top < innerHeight;
    return list.map((s, i) => [s, after[i] - before[i].top]).filter(([s, dy], i) => Math.abs(dy) >= 1 && seen(before[i]));
  }
  /** Something new in a list opens its own room and fades in, rather than appearing all at once. */
  function grow(node, opts = {}) {
    if (!node || !node.isConnected || reduced()) return;
    const h = node.getBoundingClientRect().height; if (!h) return;
    const o = { duration: opts.ms || T.grow, easing: "cubic-bezier(.3,.1,.2,1)", delay: opts.delay || 0, fill: "backwards" };
    // room: false — the rows around it already glide into their places (reconcile), so it only fades in where it stands;
    // folding its height as well dropped them back first, and they jumped before gliding
    if (opts.room !== false) {
      // its room opens: what follows glides down from where it stood while it is still unseen (its height is not
      // animated: a layout property per frame, Paul's "jerky"); `add` so a room closing nearby at once adds to it
      for (const [s, dy] of shift(node)) s.animate([{ transform: `translateY(${dy}px)` }, { transform: "none", offset: .45 }, { transform: "none" }], Object.assign({ composite: "add" }, o));
      node.animate([{ opacity: 0, transform: "translateY(-6px)" }, { opacity: 0, offset: .45 }, { opacity: 1, transform: "none" }], o);
      return;
    }
    node.animate([{ opacity: 0, transform: "translateY(-6px)" }, { opacity: 0, offset: .3 }, { opacity: 1, transform: "none" }], o);
  }
  /** `node` leaves its room: it goes (opts.frames, else it folds up from its foot as it fades) while what follows it
   *  rises into its room (from opts.at of the way), all on transform, opacity and clip-path; then opts.done takes it
   *  out, and the frame round it takes its new size once. Returns { finished (true when it played to its end), cancel }
   *  (cancel: it stays, whole, and done is not called). */
  function shut(node, opts = {}) {
    const o = { duration: opts.ms || T.fade, easing: opts.easing || "cubic-bezier(.4,0,.2,1)", fill: "forwards" };
    const moved = !node.isConnected || reduced() ? [] : shift(node), at = opts.at || 0;
    const own = node.animate(opts.frames || [{ opacity: 1, clipPath: "inset(0 0 0 0)" }, { opacity: 0, clipPath: "inset(0 0 100% 0)" }], o);
    const rise = moved.map(([s, dy]) => s.animate([{ transform: "none" }, ...(at ? [{ transform: "none", offset: at }] : []), { transform: `translateY(${dy}px)` }], Object.assign({ composite: "add" }, o)));
    let off = false;
    const end = () => { for (const a of rise) a.cancel(); own.cancel(); };
    const finished = own.finished.then(() => true, () => false).then(ok => {
      if (off) return false;
      try { if (opts.done) opts.done(); } finally { end(); }   // out of the layout and its lifts dropped in one step: no jump
      return ok;
    });
    return { finished, cancel() { if (off) return; off = true; end(); } };
  }
  /** Something removed with no destination fades and folds away where it was. */
  async function fade(g) {
    await g.animate([{ opacity: 1, transform: "none" }, { opacity: 0, transform: "scale(.96)" }], { duration: T.fade, easing: "ease-in", fill: "forwards" }).finished.catch(() => {});
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
      if (spec && spec.from) flyIn(spec.from, n, spec); else if (seen(n.getBoundingClientRect())) grow(n, { delay: hold ? hold + 120 : 0, room: false });
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
    /* The stamp's shadow: it was a drop-shadow filter animated per frame (every frame repainted the stamp with a new
       blur). Now two still soft discs under the stamp, one blurred as far from the paper, one tight as it touches,
       that only move and cross-fade: [x, y, blur, alpha] as the filter had them, in the stamp's own space. */
    const SHADE = { far: [30, 40, 18, .26], near: [2, 3, 3, .4], lift: [26, 36, 18, .2] };
    function shadow(t, S, from, to, o) {
      const R = S * 58 / 120, disc = (b, A) => {
        const s = b / 2, pad = Math.ceil(2 * s + 2), i = doc.createElement("i"); i.className = "sealShade"; i.setAttribute("aria-hidden", "true");
        i.style.cssText = `position:absolute;z-index:-1;left:${-pad}px;top:${-pad}px;width:${S + 2 * pad}px;height:${S + 2 * pad}px;pointer-events:none;opacity:0;` +
          `background:radial-gradient(circle,rgba(20,14,6,${A}) ${Math.max(0, R - 2 * s).toFixed(1)}px,rgba(20,14,6,${A / 2}) ${R.toFixed(1)}px,rgba(20,14,6,0) ${(R + 2 * s).toFixed(1)}px)`;
        t.insertBefore(i, t.firstChild); return i;
      };
      const soft = disc(18, .26), hard = disc(3, .4);
      const to2 = (a, b, opt) => {
        const at = x => `translate(${x[0]}px,${x[1]}px)`, soft0 = a[2] > 6 ? a[3] / SHADE.far[3] : 0, soft1 = b[2] > 6 ? b[3] / SHADE.far[3] : 0;
        const k = Object.assign({ fill: "forwards" }, opt);
        soft.animate([{ transform: at(a), opacity: soft0 }, { transform: at(b), opacity: soft1 }], k);
        hard.animate([{ transform: at(a), opacity: a[2] > 6 ? 0 : 1 }, { transform: at(b), opacity: b[2] > 6 ? 0 : 1 }], k);
      };
      to2(from, to, o);
      return { to: to2 };
    }
    /** The wooden stamp comes down on a seal already in its place, and leaves it inked. */
    async function press(seal) {
      const r = seal.getBoundingClientRect(), size = r.width / 1.0, kind = seal.classList.contains("seal-button") ? "button" : "print";
      const rot = parseFloat(getComputedStyle(seal).getPropertyValue("--rot")) || -8;
      const t = doc.createElement("span"); t.className = "sealTool"; t.innerHTML = tool(kind);
      Object.assign(t.style, { position: "fixed", left: r.left - size * .02 + "px", top: r.top - size * .02 + "px", width: size * 1.04 + "px", height: size * 1.04 + "px" });
      layer(seal).appendChild(t);
      const down = t.animate([
        { transform: `translate(34px,-46px) rotate(${rot - 16}deg) scale(1.7)`, opacity: 0 },
        { opacity: 1, offset: .22 },
        { transform: `translate(0,0) rotate(${rot}deg) scale(1)`, opacity: 1 }
      ], { duration: 560, easing: "cubic-bezier(.62,0,.92,.5)", fill: "forwards" });
      shadow(t, size * 1.04, SHADE.far, SHADE.near, { duration: 560, easing: "cubic-bezier(.62,0,.92,.5)" });
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
        { transform: `translate(34px,-46px) rotate(${rot - 16}deg) scale(1.7)`, opacity: 0 },
        { opacity: 1, offset: .22 },
        { transform: `translate(0,0) rotate(${rot}deg) scale(1)`, opacity: 1 }
      ], { duration: 560, easing: "cubic-bezier(.62,0,.92,.5)", fill: "forwards" });
      const shade = shadow(t, size * 1.04, SHADE.far, SHADE.near, { duration: 560, easing: "cubic-bezier(.62,0,.92,.5)" });
      await down.finished.catch(() => {});
      // the press: ink on the paper and the button, a thud, a ring through the paper
      seal.style.opacity = ""; seal.classList.add("wet"); paint();
      const card = host._card || host;
      card.animate([{ transform: "translateY(0)" }, { transform: "translateY(1.6px)", offset: .3 }, { transform: "translateY(0)" }], { duration: 240, easing: "ease-out" });
      const ring = doc.createElement("span"); ring.className = "sealRing"; ring.style.cssText = `left:${cx - size / 2}px;top:${cy - size / 2}px;width:${size}px;height:${size}px;--ink:${INK[kind]}`; host.appendChild(ring);
      ring.animate([{ transform: "scale(.86)", opacity: .42 }, { transform: "scale(1.42)", opacity: 0 }], { duration: 760, easing: "cubic-bezier(.2,.7,.3,1)", fill: "forwards" }).finished.then(() => ring.remove(), () => ring.remove());
      const up = t.animate([
        { transform: `rotate(${rot}deg) scale(1)`, opacity: 1 },
        { transform: `rotate(${rot}deg) scale(.95)`, opacity: 1, offset: .22 },
        { transform: `translate(-16px,-40px) rotate(${rot + 8}deg) scale(1.55)`, opacity: 0 }
      ], { duration: 700, easing: "cubic-bezier(.2,.7,.3,1)", fill: "forwards" });
      shade.to(SHADE.near, SHADE.lift, { duration: 700, easing: "cubic-bezier(.2,.7,.3,1)" });
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

  /* ════ Pop-ups: every window grows out of what opened it, and goes back into it ════
     Paul, 28 Sep 19:27-19:30: the sheet window's opening, everywhere a pop-up is. The window's surface grows out of the
     thing clicked, opaque from the first frame and in its colour; the header and the rest of the window come in after
     it, one part after another; closing, the window goes back into what opened it (or fades, when that is gone). The
     real window opens and closes at once: the motion is drawn over it, never waited on (nothing here holds anything up
     for more than 700 ms), and it never throws. Any <dialog> gets it: showModal and close are wrapped below, and a
     dialog opened just after a click grows out of what was clicked. A dialog with data-no-grow (or the sheet window,
     which has its own) is left alone. */
  const DG = { grow: "cubic-bezier(.3,0,.1,1)", back: "cubic-bezier(.4,0,.2,1)", ease: "cubic-bezier(.2,.8,.2,1)", surface: 600, tint: 250, panel: 380, head: 560, part: 380, step: 45, close: 480, cap: 700 };
  const SRC = "button,a[href],summary,[role=button],[role=menuitem],[role=tab],[role=option],img,label,[data-id],[tabindex]:not([tabindex='-1'])";
  let press = null;
  function remember(el, e) {
    if (!el || el.nodeType !== 1 || el === doc.body || el === doc.documentElement) return;
    try { const r = el.getBoundingClientRect(); press = { el, rect: { left: r.left, top: r.top, width: r.width, height: r.height }, pt: e && e.clientX != null ? { x: e.clientX, y: e.clientY } : null, at: Date.now() }; } catch (_) {}
  }
  doc.addEventListener("pointerdown", e => { const t = e.target; if (t && t.closest) remember(t.closest(SRC) || t, e); }, true);
  doc.addEventListener("keydown", e => { if (e.key === "Enter" || e.key === " ") remember(doc.activeElement); }, true);
  // (left alone: the sheet window, which has its own; the order window, being rebuilt as a full-screen view with its own)
  const skips = d => !d || d.hasAttribute("data-no-grow") || d.classList.contains("sheetWin") || d.id === "orderWin";
  const isModal = d => { try { return d.matches(":modal"); } catch (_) { return true; } };
  const rectOf = r => r && { left: r.left, top: r.top, width: r.width, height: r.height };
  /** The colour of what was clicked: its own, else the nearest box behind it that has one. */
  function tintOf(el) {
    for (let n = el, i = 0; n && n.nodeType === 1 && i < 6; n = n.parentElement, i++) {
      const c = getComputedStyle(n).backgroundColor, m = /rgba?\(([^)]*)\)/.exec(c || ""); if (!m) continue;
      const p = m[1].split(/[\s,/]+/).filter(Boolean); if (p.length < 4 || parseFloat(p[3]) > .6) return c;
    }
    return "";
  }
  /** What a window grows out of: the element (or rect) it was given, else what was pressed a moment ago. An element
   *  gone or out of sight by now still gives where it was pressed. */
  function sourceFor(dlg, from) {
    let el = null, rect = null;
    if (from && from.nodeType === 1) { el = from; if (press && press.el === from) rect = press.rect; }
    else if (from && typeof from.left === "number") rect = rectOf(from);
    else if (!from && press && Date.now() - press.at < 1500) { el = press.el; rect = press.rect; }
    if (el && dlg.contains(el)) return null;
    if (el && visible(el)) rect = rectOf(el.getBoundingClientRect());
    if (!rect || !(rect.width >= 1 && rect.height >= 1)) return null;
    return { el, rect, pt: press && press.el === el ? press.pt : null, tint: el && el.isConnected ? tintOf(el) : "" };
  }
  /** A source nearly as big as the window (a whole sheet's canvas, a big card) would leave almost nothing to see grow:
   *  the window grows out of the point pressed in it instead (its centre when that is not known), and goes back there. */
  function spot(r, D, pt) {
    if (!r || r.width * r.height < .3 * D.width * D.height) return r;
    const x = pt && pt.x >= r.left && pt.x <= r.left + r.width ? pt.x : r.left + r.width / 2, y = pt && pt.y >= r.top && pt.y <= r.top + r.height ? pt.y : r.top + r.height / 2;
    return { left: x - 24, top: y - 16, width: 48, height: 32 };
  }
  /** Where a closing window goes back to: what it grew out of while that is in sight (for a menu's item, the menu's
   *  button once the menu has closed); null: it fades where it is. */
  function homeOf(src, to) {
    let el = to !== undefined ? resolve(to) : src && src.el;
    if (el && !visible(el)) { const d = el.closest && el.closest("details:not([open])"); el = d ? d.querySelector(":scope > summary") : null; }
    return el && visible(el) ? el : null;
  }
  const OPEN = new WeakMap();
  function stopOpen(dlg) {
    const st = OPEN.get(dlg); if (!st) return; OPEN.delete(dlg);
    clearTimeout(st.t); for (const a of st.anims) { try { a.cancel(); } catch (_) {} }
    if (st.surf) st.surf.remove(); dlg.classList.remove("mdGrow");
  }
  /** The parts of a window that come in one after another: its children, or the children of its single wrapper. */
  function partsOf(dlg) {
    let box = dlg, kids = [];
    for (let i = 0; i < 3; i++) {
      kids = [...box.children].filter(n => !/^(SCRIPT|STYLE|TEMPLATE|DIALOG|LINK)$/.test(n.tagName) && !n.classList.contains("mdSurface") && !n.classList.contains("motionLayer") && n.getClientRects().length && getComputedStyle(n).position !== "fixed");
      if (kids.length === 1 && kids[0].children.length > 1) { box = kids[0]; continue; }
      break;
    }
    return kids.slice(0, 40);
  }
  const isHead = n => /^(HEADER|H1|H2|H3)$/.test(n.tagName) || /(^|\s)[\w-]*Head(\s|$)/.test(n.getAttribute("class") || "");
  /** The window, just shown, grows out of `from` (an element, a rect, or by default what was just pressed; none: from a
   *  little smaller than itself): its surface grows, the frame's colour giving way to its own, then its header and its
   *  parts come in. Resolves when the surface has its size. */
  function dialogOpen(dlg, from, opts = {}) {
    try {
      if (!dlg || !dlg.open || (skips(dlg) && !opts.force)) return Promise.resolve();
      stopOpen(dlg);
      const st = { anims: [] }; OPEN.set(dlg, st);
      const run = (el, frames, o) => { const a = el.animate(frames, Object.assign({ duration: DG.surface, easing: DG.grow, fill: "backwards" }, o)); st.anims.push(a); return a; };
      const src = sourceFor(dlg, from); dlg._mdSrc = src;
      const done = ms => { st.t = setTimeout(() => { if (OPEN.get(dlg) === st) stopOpen(dlg); }, ms); return wait(Math.min(ms, DG.cap)); };
      if (reduced()) { run(dlg, [{ opacity: 0 }, { opacity: 1 }], { duration: 160, easing: "ease" }); return done(200); }
      dlg.classList.add("mdIn");
      const D = dlg.getBoundingClientRect(); if (D.width < 2 || D.height < 2) return done(0);
      const B = src ? spot(src.rect, D, src.pt) : { left: D.left + D.width * .07, top: D.top + D.height * .07, width: D.width * .86, height: D.height * .86 };
      // the surface: the window's own box and colours, laid over the screen (so no window's edge cuts it), under its parts
      const cs = getComputedStyle(dlg), surf = doc.createElement("div"); surf.className = "mdSurface"; surf.setAttribute("aria-hidden", "true");
      Object.assign(surf.style, { left: D.left + "px", top: D.top + "px", width: D.width + "px", height: D.height + "px", borderRadius: cs.borderRadius, backgroundColor: cs.backgroundColor, backgroundImage: cs.backgroundImage, boxShadow: cs.boxShadow });
      const tint = doc.createElement("b"); tint.style.background = (src && src.tint) || cs.backgroundColor; surf.appendChild(tint);
      dlg.appendChild(surf); st.surf = surf;
      const g = surf.getBoundingClientRect();
      if (Math.abs(g.left - D.left) > .5 || Math.abs(g.top - D.top) > .5) { surf.style.left = 2 * D.left - g.left + "px"; surf.style.top = 2 * D.top - g.top + "px"; }
      dlg.classList.add("mdGrow");
      const C = `translate(${B.left - D.left}px,${B.top - D.top}px) scale(${B.width / D.width},${B.height / D.height})`;
      const grown = run(surf, [{ transform: C }, { transform: "none" }], { fill: "both" });
      run(tint, [{ opacity: 1 }, { opacity: 0 }], { duration: DG.tint, easing: "cubic-bezier(.4,0,.6,1)", fill: "both" });
      // the header once the window has its size; the rest one after another, from as the surface settles
      let i = 0;
      partsOf(dlg).forEach((n, k) => {
        if (k === 0 && isHead(n)) run(n, [{ opacity: 0, transform: "translateY(-8px)" }, { opacity: 1, transform: "none" }], { duration: 320, delay: DG.head, easing: DG.ease });
        else run(n, [{ opacity: 0, transform: "translateY(10px)" }, { opacity: 1, transform: "none" }], { duration: DG.part, delay: DG.panel + Math.min(i++, 6) * DG.step, easing: DG.ease });
      });
      // the window is its own surface again once grown (a window that grows meanwhile is never left without its colour)
      const land = () => { if (OPEN.get(dlg) !== st || !st.surf) return; st.surf.remove(); st.surf = null; dlg.classList.remove("mdGrow"); };
      // (the tidying keeps the animations' own time, so a slowed animation is not cut short; a hidden tab, which draws
      // no frames, is tidied a while later all the same)
      Promise.race([grown.finished.catch(() => {}), wait(3000)]).then(land);
      warm();
      const total = DG.panel + 6 * DG.step + DG.part + 120, clock = dlg.animate([], { duration: total }); st.anims.push(clock);
      clock.finished.then(() => { if (OPEN.get(dlg) === st) stopOpen(dlg); }, () => {});
      done(15000);
      return Promise.race([grown.finished.catch(() => {}), wait(DG.cap)]);
    } catch (_) { try { stopOpen(dlg); } catch (__) {} return Promise.resolve(); }
  }
  /* the copy a closed window leaves behind: drawn from the page's own styles inside a closed shadow root, so nothing
     that looks for the window's contents (or counts open dialogs) ever finds it */
  let sheetKey = "", sheetObj = null;
  function pageSheet() {
    try {
      if (!("adoptedStyleSheets" in doc) || !root.CSSStyleSheet) return null;
      const list = [...doc.styleSheets]; let key = String(list.length), text = "";
      for (const s of list) { try { key += "," + s.cssRules.length; } catch (_) { key += ",x"; } }
      if (sheetObj && key === sheetKey) return sheetObj;
      for (const s of list) {
        try {
          if (s.disabled) continue;
          const body = [...s.cssRules].map(r => r.cssText).filter(t => !/^@(import|charset)/i.test(t)).join("\n"), m = s.media && s.media.mediaText;
          text += m && m !== "all" ? `@media ${m}{${body}}\n` : body + "\n";
        } catch (_) {}
      }
      const sh = new CSSStyleSheet(); sh.replaceSync(text); sheetObj = sh; sheetKey = key; return sh;
    } catch (_) { return null; }
  }
  /** The page's styles for the closing copy, made ready when the page is idle once a window has opened (building them,
   *  and the browser compiling them, took 45-80 ms on the first close of a session: a stall at the start of the flight). */
  let warmed = null, warming = false;
  function warm() {
    if (warming) return; warming = true;
    const go = () => {
      warming = false;
      try {
        const sh = pageSheet(); if (!sh || sh === warmed) return; warmed = sh;
        const host = doc.createElement("div"); host.setAttribute("aria-hidden", "true"); host.style.cssText = "position:fixed;left:0;top:0;width:0;height:0;overflow:hidden;visibility:hidden;pointer-events:none";
        const sr = host.attachShadow({ mode: "closed" }); sr.adoptedStyleSheets = [sh]; sr.innerHTML = '<div class="dlg"><div class="dlgHead"></div></div>';
        doc.body.appendChild(host); getComputedStyle(sr.firstElementChild).color; host.remove();
      } catch (_) {}
    };
    // (after the window has grown, never during its flight)
    setTimeout(() => { if (root.requestIdleCallback) root.requestIdleCallback(go, { timeout: 2000 }); else go(); }, DG.cap + 500);
  }
  /** Everything the closing needs, read while the window is still open. */
  function snapshot(dlg) {
    const D = rectOf(dlg.getBoundingClientRect()); if (!D || D.width < 2 || D.height < 2) return null;
    // (a window closed while it still grows is bare, its surface drawn apart: its own colours are read without that)
    const bare = dlg.classList.contains("mdGrow"); if (bare) dlg.classList.remove("mdGrow");
    const cs = getComputedStyle(dlg); let back = "";
    try { if (isModal(dlg)) back = getComputedStyle(dlg, "::backdrop").backgroundColor; } catch (_) {}
    const snap = { dlg, D, back, radius: cs.borderRadius, bg: cs.backgroundColor, bgi: cs.backgroundImage, shadow: cs.boxShadow, src: dlg._mdSrc || null, copy: null, scroll: [] };
    if (bare) dlg.classList.add("mdGrow");
    if (dlg.getElementsByTagName("*").length > 5000) return snap;
    const c = dlg.cloneNode(true), a = dlg.getElementsByTagName("*"), b = c.getElementsByTagName("*");
    for (let i = 0; i < a.length; i++) { const n = a[i]; if (n.scrollTop || n.scrollLeft) snap.scroll.push([b[i], n.scrollTop, n.scrollLeft]); }
    if (dlg.scrollTop || dlg.scrollLeft) snap.scroll.push([c, dlg.scrollTop, dlg.scrollLeft]);
    const ca = dlg.querySelectorAll("canvas"), cb = c.querySelectorAll("canvas");
    ca.forEach((x, i) => { try { const y = cb[i]; y.width = x.width; y.height = x.height; y.getContext("2d").drawImage(x, 0, 0); } catch (_) {} });
    const fa = dlg.querySelectorAll("input,textarea,select"), fb = c.querySelectorAll("input,textarea,select");
    fa.forEach((x, i) => { const y = fb[i]; if (!y) return; try { if (x.type === "checkbox" || x.type === "radio") y.checked = x.checked; else if (x.type !== "file") y.value = x.value; } catch (_) {} });
    for (const x of c.querySelectorAll(".mdSurface,iframe,video,audio,object,embed,script")) x.remove();
    for (const x of c.querySelectorAll("dialog")) x.removeAttribute("open");
    for (const x of c.querySelectorAll("[autofocus]")) x.removeAttribute("autofocus");
    c.removeAttribute("autofocus"); c.setAttribute("open", ""); c.classList.remove("mdIn", "mdGrow", "closing");
    snap.copy = c;
    return snap;
  }
  /** The closed window's copy goes back into what it grew out of (opts.to, else its source), or fades where it is. */
  function flyBack(snap, opts = {}) {
    try {
      const { dlg, D } = snap, still = reduced();
      // (the window it lay over, if one is still open: never one opened after it, which the copy would cover)
      const mine = dlg._mdSeq || Infinity, seqOf = d => d._mdSeq || 0;
      const under = [...doc.querySelectorAll("dialog[open]")].filter(d => d !== dlg && isModal(d) && seqOf(d) < mine).sort((a, b) => seqOf(a) - seqOf(b)).pop() || null;
      const L = layer(under), A = [];
      const run = (el, frames, o) => { const a = el.animate(frames, Object.assign({ duration: DG.close, easing: DG.back, fill: "forwards" }, o)); A.push(a); return a; };
      const host = doc.createElement("div"); host.className = "mdGhost"; host.setAttribute("aria-hidden", "true"); host.inert = true;
      Object.assign(host.style, { left: D.left + "px", top: D.top + "px", width: D.width + "px", height: D.height + "px", willChange: "transform,opacity" });
      let shade = null;
      if (snap.back && !/rgba\(0, 0, 0, 0\)|transparent/.test(snap.back)) { shade = doc.createElement("div"); shade.className = "mdGhostBack"; shade.setAttribute("aria-hidden", "true"); shade.style.background = snap.back; L.appendChild(shade); }
      L.appendChild(host);
      const sr = host.attachShadow({ mode: "closed" }), sheet = snap.copy ? pageSheet() : null; if (sheet) sr.adoptedStyleSheets = [sheet];
      const box = (bg, more) => { const x = doc.createElement("div"); Object.assign(x.style, { position: "absolute", inset: "0", borderRadius: snap.radius, background: bg }, more || {}); return x; };
      sr.appendChild(box(snap.bg, { backgroundImage: snap.bgi, boxShadow: snap.shadow }));
      const c = sheet ? snap.copy : null;
      if (c) {
        Object.assign(c.style, { position: "absolute", inset: "0", margin: "0", width: "100%", height: "100%", maxWidth: "none", maxHeight: "none", minWidth: "0", minHeight: "0", background: "transparent", boxShadow: "none", animation: "none", transition: "none", transform: "none", opacity: "1", zIndex: "auto" });
        sr.appendChild(c);
        for (const [n, t, l] of snap.scroll) { try { if (n) { n.scrollTop = t; n.scrollLeft = l; } } catch (_) {} }
      }
      const tint = box((snap.src && snap.src.tint) || snap.bg, { opacity: "0" }); sr.appendChild(tint);
      if (shade) run(shade, [{ opacity: 1 }, { opacity: 0 }], { duration: still ? 120 : 440, easing: "ease" });
      const home = still ? null : homeOf(snap.src, opts.to);
      if (!home) run(host, [{ opacity: 1, transform: "none" }, { opacity: 0, transform: still ? "none" : "translateY(6px) scale(.985)" }], { duration: still ? 120 : 220, easing: "ease" });
      else {
        const T = spot(home.getBoundingClientRect(), D, snap.src && snap.src.pt);
        run(host, [{ transform: "none" }, { transform: `translate(${T.left - D.left}px,${T.top - D.top}px) scale(${T.width / D.width},${T.height / D.height})` }]);
        run(host, [{ opacity: 1 }, { opacity: 1, offset: .8 }, { opacity: 0 }], { easing: "linear" });
        if (c) run(c, [{ opacity: 1 }, { opacity: 0 }], { duration: 180, easing: "cubic-bezier(.4,0,1,1)" });
        run(tint, [{ opacity: 0 }, { opacity: 1 }], { duration: 200, easing: "ease" });
        if (opts.arrive !== false) setTimeout(() => { try { if (home.isConnected) home.animate([{ transform: "scale(1)" }, { transform: "scale(1.06)", offset: .4 }, { transform: "scale(1)" }], { duration: 360, easing: "ease-out" }); } catch (_) {} }, DG.close * .8);
      }
      const end = () => { host.remove(); if (shade) shade.remove(); };
      // (nothing waits on this: the window is closed already; a hidden tab, which draws no frames, loses the copy later)
      return Promise.race([Promise.all(A.map(a => a.finished.catch(() => {}))), wait(3000)]).then(end, end);
    } catch (_) { return Promise.resolve(); }
  }
  const nativeClose = root.HTMLDialogElement ? root.HTMLDialogElement.prototype.close : null;
  const nativeShow = root.HTMLDialogElement ? root.HTMLDialogElement.prototype.showModal : null;
  /** Closes the window now (dialog.open is false when this returns; returnValue, Esc and focus as the browser does them),
   *  and a copy of it goes back into what opened it. opts.to: where it goes instead; opts.returnValue. */
  function dialogClose(dlg, opts = {}) {
    if (!dlg || !dlg.open) return Promise.resolve();
    let snap = null;
    // (_mdSnap: an Esc whose cancel handler closes the window itself leaves this one copy, not a second from the Esc)
    if (!skips(dlg)) { try { snap = snapshot(dlg); } catch (_) {} try { stopOpen(dlg); } catch (_) {} dlg._mdClosing = true; dlg._mdSnap = null; }
    try { if (opts.returnValue !== undefined) nativeClose.call(dlg, opts.returnValue); else nativeClose.call(dlg); }
    finally { dlg.classList.remove("mdIn"); }
    return snap ? flyBack(snap, opts) : Promise.resolve();
  }
  /** The next opening of `dlg` grows out of `from` (for a window opened later than just after its click). */
  const from = (dlg, src) => { if (dlg) dlg._mdFrom = src || null; };
  if (nativeShow && nativeClose && !root.HTMLDialogElement.prototype._mdWrapped) {
    const P = root.HTMLDialogElement.prototype; P._mdWrapped = true;
    let seq = 0;   // the order windows were opened in, so a closing copy never lands over a window opened after it
    P.showModal = function () {
      if (!this.open) this._mdSeq = ++seq;
      if (this.open || skips(this)) return nativeShow.apply(this, arguments);
      const f = this._mdFrom; this._mdFrom = null; this._mdClosing = false; this._mdSnap = null;
      if (!reduced()) this.classList.add("mdIn");
      try { nativeShow.apply(this, arguments); } catch (e) { this.classList.remove("mdIn"); throw e; }
      this._mdShown = dialogOpen(this, f);
    };
    P.close = function (rv) {
      if (!this.open || skips(this)) return arguments.length ? nativeClose.call(this, rv) : nativeClose.call(this);
      dialogClose(this, arguments.length ? { returnValue: rv } : {});
    };
    // closed by the browser itself (Esc, a form's method="dialog"): read while still open, gone back into once closed
    const early = d => {
      if (!(d instanceof root.HTMLDialogElement) || !d.open || skips(d)) return;
      let rec = null; try { rec = d._mdSnap = { snap: snapshot(d), at: Date.now(), used: false }; } catch (_) { return; }
      // (drawn before the next frame, so the window never blinks out between its closing and its copy)
      requestAnimationFrame(() => { if (!d.open && d._mdSnap === rec && !rec.used && rec.snap) { rec.used = true; flyBack(rec.snap); } });
    };
    // (the closing copy's styles are made ready once the page has loaded, again after a window opens if they changed)
    if (doc.readyState === "complete") warm(); else root.addEventListener("load", () => warm(), { once: true });
    doc.addEventListener("cancel", e => early(e.target), true);
    doc.addEventListener("submit", e => { const f = e.target, how = (e.submitter && e.submitter.getAttribute("formmethod")) || (f && f.getAttribute && f.getAttribute("method")); if (f && /^dialog$/i.test(how || "")) early(f.closest("dialog")); }, true);
    doc.addEventListener("close", e => {
      const d = e.target; if (!(d instanceof root.HTMLDialogElement)) return;
      if (d.open) return;
      const ours = d._mdClosing, s = d._mdSnap; d._mdClosing = false; d._mdSnap = null;
      try { stopOpen(d); } catch (_) {} d.classList.remove("mdIn");
      if (!ours && s && s.snap && !s.used && Date.now() - s.at < 1500 && !skips(d)) { s.used = true; flyBack(s.snap); }
    }, true);
  }

  /* ════ the lighter pop: a menu or a panel that opens under its button zooms out of it and fades in ════ */
  function popIn(el, anchor) {
    try {
      if (!el || !el.isConnected) return;
      if (reduced()) { el.animate([{ opacity: 0 }, { opacity: 1 }], { duration: 120, easing: "ease" }); return; }
      const r = el.getBoundingClientRect(); if (!r.width || !r.height) return;
      const a = anchor && anchor.isConnected ? anchor.getBoundingClientRect() : null;
      const ox = a ? Math.max(0, Math.min(r.width, a.left + a.width / 2 - r.left)) : r.width / 2, oy = a ? Math.max(0, Math.min(r.height, a.top + a.height / 2 - r.top)) : 0, o = `${ox}px ${oy}px`;
      // (its origin set on the panel for the flight, not in the keyframes, which kept the animation off the compositor)
      const was = el.style.transformOrigin; el.style.transformOrigin = o; const mine = el.style.transformOrigin;
      const an = el.animate([{ opacity: 0, transform: "scale(.88)" }, { opacity: 1, transform: "none" }], { duration: 300, easing: DG.grow });
      const back = () => { if (el.style.transformOrigin === mine) el.style.transformOrigin = was; };
      an.finished.then(back, back);
    } catch (_) {}
  }
  // every <details> that opens as a pop-up (its panel laid over the page: the run menu, Workspace, a sheet's Options…)
  try {
    new MutationObserver(recs => {
      for (const r of recs) {
        const d = r.target; if (d.tagName !== "DETAILS" || !d.open || r.oldValue !== null) continue;
        const panel = [...d.children].find(n => n.tagName !== "SUMMARY" && n.getClientRects().length); if (!panel) continue;
        const pos = getComputedStyle(panel).position; if (pos === "absolute" || pos === "fixed") popIn(panel, d.querySelector(":scope > summary"));
      }
    }).observe(doc.documentElement, { attributes: true, attributeFilter: ["open"], attributeOldValue: true, subtree: true });
  } catch (_) {}

  /* ── Front | Back · engraving, one turn at a time (Paul, 28 Sep: "a beautiful animation on every sheet view"). A sheet
     turns to its edge, is drawn from the side asked for, and comes round. A press while it turns is taken up by that turn:
     the side drawn at its edge is the one asked for last, and a press after the edge turns it once more when it has come
     round — never a second turn started over the first. The side it ends on is always the last one asked for.
     o: el() the element(s) that turn, now; shown() the side drawn now; paint(face) draws a side (at the edge, or at once when
     it cannot turn); still() no motion now; persp a perspective() prefix; delay(i) a stagger. ── */
  function turner(o) {
    const S = { want: null, busy: false };
    const go = delay => {
      const to = S.want; if (to == null || to === o.shown()) return;
      const els = [].concat((o.el && o.el()) || []).filter(e => e && e.isConnected && e.animate);
      if (!els.length || reduced() || (o.still && o.still())) return o.paint(to);
      S.busy = true;
      const d = to === "back" ? 1 : -1, P = o.persp || "";
      const edge = () => o.paint(S.want), done = () => { S.busy = false; if (S.want !== o.shown()) go(0); };
      Promise.all(els.map((e, i) => e.animate([{ transform: P + "rotateY(0)" }, { transform: P + `rotateY(${90 * d}deg)` }],
        { duration: 300, delay: (delay || 0) + (o.delay ? o.delay(i) : 0), easing: "cubic-bezier(.4,0,1,1)" }).finished))
        .then(() => { edge(); return Promise.all(els.filter(e => e.isConnected).map(e => e.animate([{ transform: P + `rotateY(${-90 * d}deg)` }, { transform: P + "rotateY(0)" }], { duration: 380, easing: "cubic-bezier(.2,.8,.2,1)" }).finished)); },
          () => edge())
        .catch(() => {}).then(done);
    };
    return { ask(face, delay) { S.want = face; if (!S.busy) go(delay); }, busy: () => S.busy, want: () => S.want };
  }
  /** Words landing on a sheet drawn from behind: the drawing without them fades out over the one with them, never a pop.
      A plate still turning comes round first on its plain back (with its spinner), then the words fade in. */
  function landIn(cv, paint) {
    const turning = () => (doc.getAnimations ? doc.getAnimations() : []).filter(a => { const t = a.effect && a.effect.target; if (!t || !t.contains || !t.contains(cv) || a.playState !== "running") return false;
      try { return a.effect.getKeyframes().some(k => /rotateY/.test(k.transform || "")); } catch (_) { return false; } });
    const go = () => {
      if (!cv || !cv.isConnected || reduced() || !cv.width || !cv.height || !cv.offsetWidth || !cv.offsetParent) return paint();
      const c = doc.createElement("canvas"); c.width = cv.width; c.height = cv.height;
      try { c.getContext("2d").drawImage(cv, 0, 0); } catch (_) { return paint(); }
      c.className = "mLand"; c.setAttribute("aria-hidden", "true");
      Object.assign(c.style, { position: "absolute", left: cv.offsetLeft + "px", top: cv.offsetTop + "px", width: cv.offsetWidth + "px", height: cv.offsetHeight + "px", margin: "0", pointerEvents: "none", zIndex: "1" });
      cv.after(c); paint();
      c.animate([{ opacity: 1 }, { opacity: 0 }], { duration: 420, easing: "ease", fill: "forwards" }).finished.catch(() => {}).then(() => c.remove());
    };
    let n = 0;
    const wait = () => { const t = turning(); if (!t.length || ++n > 6) return go(); Promise.all(t.map(a => a.finished.catch(() => {}))).then(wait); };
    wait();
  }

  root.Motion = { T, ghost, fly, flyIn, grow, shut, fade, arrive, pulse, note, expect, expectIn, pending, reconcile, reduced, wait, layer, dialogOpen, dialogClose, from, popIn, turner, landIn };
  root.Seal = Seal;
})(typeof window !== "undefined" ? window : globalThis);
