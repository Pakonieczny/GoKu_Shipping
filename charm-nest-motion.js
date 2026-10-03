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
  /* A row another animation is already carrying (the Send to Sheet tour lifts the card itself): its list makes no copy
     of its own for it, however often it redraws while it is away. By its key, not its node: a redraw makes a new node.
     A list may also hold such a row where it stood while it is carried (carrying(k): the Review list does), so nothing
     moves under the carrier; the carrier lets go (carry(k, 0)) once the list is out of sight. */
  const carried = new Map();
  function carry(mkey, ms = 8000) { const k = String(mkey || ""); if (!k) return; if (ms > 0) carried.set(k, Date.now() + ms); else carried.delete(k); }
  const isCarried = k => { const u = carried.get(String(k)); if (!u) return false; if (u > Date.now()) return true; carried.delete(String(k)); return false; };

  /** The one keyed list update: `nodes` in order, in `host`. With opts.animate (the same view as before, drawn already)
   *  what left is seen going (to where its action said, else it fades), what stayed glides into place, and what is new
   *  opens its room, or flies in from where its action said it came from. */
  function reconcile(host, nodes, opts = {}) {
    if(Seal.defer(host,()=>reconcile(host,nodes,opts)))return;
    const on = opts.animate !== false && !reduced() && host.isConnected && host.getClientRects().length > 0;
    const before = new Map(), ctr = on ? containerOf(host, true) : null, cw = ctr ? innerWidthOf(ctr) : null;
    // (a carried row the list holds where it stood: not glided, not copied as it leaves, and not new either)
    const held = new Set();
    if (on) for (const n of host.children) { const k = n.dataset && n.dataset.mkey; if (!k) continue; if (isCarried(k)) held.add(k); else if (!n._mLeaving) before.set(k, { node: n, rect: n.getBoundingClientRect() }); }
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
      // (a seal already pressed on the button before the card left, stamp.still, is only carried: no stamp, no hold)
      const h = spec && spec.stamp && !spec.stamp.still ? T.stamp + T.hold : 0; hold = Math.max(hold, h);
      gone.push([g, spec]);
    }
    for (const [g, spec] of gone) (async () => {
      if (spec && spec.stamp) { try { await Seal.stampOn(g, spec.stamp); } catch (_) {} if (!spec.stamp.still) await wait(T.hold); }
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
      if (before.has(k) || held.has(k)) continue;
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
    const BASE_SIZE = 50, META_INK = '#12294c';
    const INK = { print: "#65737f", sheet: "#65737f", button: "#77518a", laserReady: "#98721f", laserDone: "#98721f", engraveApproved:"#296c58", engravePlain:"#296c58" };
    // One outline drives both the ink face and the wooden/rubber head. Signatures remain record data, never a live viewer name.
    const FAMILY = {
      received:{name:'Received',ink:'#315e93',d:'M60 4A56 56 0 1 1 59.99 4Z'},
      prepared:{name:'Prepared',ink:'#65737f',d:'M18 6H102L114 18V102L102 114H18L6 102V18Z'},
      engraving:{name:'Back Engraving',ink:'#296c58',d:'M4 60A56 48 0 1 1 116 60A56 48 0 1 1 4 60Z'},
      laser:{name:'Laser Cutting',ink:'#98721f',d:'M31 7H89L116 60L89 113H31L4 60Z'},
      finishing:{name:'Finishing',ink:'#276a74',d:'M60 5L112 20V73Q112 98 60 115Q8 98 8 73V20Z'},
      fulfilment:{name:'Fulfilment',ink:'#77518a',d:'M60 5C72-2 84 6 87 17C101 14 114 25 109 39C123 48 123 69 109 78C114 92 101 106 87 103C84 115 72 122 60 115C48 122 36 115 33 103C19 106 6 92 11 78C-3 69-3 48 11 39C6 25 19 14 33 17C36 6 48-2 60 5Z'},
      exceptions:{name:'Exceptions',ink:'#9f483d',d:'M36 6H84L114 36V84L84 114H36L6 84V36Z'},
      cancelled:{name:'Cancelled Orders',ink:'#982f48',d:'M27 6H93L114 27V40Q95 60 114 80V93L93 114H27L6 93V80Q25 60 6 40V27Z'}
    };
    const SYMBOL = {
      received:'<path d="M8 2h9l4 4v11H8zM17 2v5h4M11 9h7M11 12h7M3 15h5l2 4h8l2-4h4v8H3zM14 14v4m-2-2 2 2 2-2"/>',
      prepared:'<path d="M2 3h20v18H2z"/><circle cx="8" cy="10" r="3"/><path d="M8 6V4M14 7c0-3 3-3 3 0 3-3 5 0 2 3l-2 2zM5 17h5m5-1h5M19 2v4m-2-2h4"/>',
      engraving:'<circle cx="10" cy="14" r="7"/><circle cx="10" cy="4" r="2"/><path d="M19 2l4 4-9 9-4 2 2-4zM16 5l4 4M6 12h6M6 15h5M18 19a8 8 0 0 1-5 4m2-3-2 3 4 .5"/>',
      laser:'<path d="M8 1h8v3H8zM9 4h6v3H9zM10 7h4v3h-4zM12 10v8M4 18h16l3 5H1zM8 14l2 2m6-2-2 2M12 20v2m-5-3 3-1m7 1-3-1"/>',
      laserReady:'<path d="M8 1h8v3H8zM9 4h6v3H9zM10 7h4v3h-4zM4 18h16l3 5H1zM13 13h9m-3-3 3 3-3 3"/>',
      finishing:'<circle cx="8" cy="15" r="6"/><circle cx="15" cy="16" r="6"/><path d="M5 5h6l-3 4zM17 2l5 4-6 7-3 1 1-3zM19 12l2 2m-3 0v3"/>',
      fulfilment:'<path d="M2 7l9-5 8 5v11l-8 5-9-5zM2 7l9 5 8-5M11 12v11M7 4l8 5M16 14h8m-3-3 3 3-3 3"/>',
      exceptions:'<circle cx="10" cy="10" r="8"/><path d="M7 6v8m6-8v8M18 12l6 11H12zM18 16v3m0 2v.2"/>',
      cancelled:'<path d="M5 1h11l4 4v18H5zM16 1v5h4M8 7h8M8 10h7M8 13l9 8m0-8-9 8"/>',
      qr:'<path d="M6 8V2h12v6M3 8h18v11h-3M6 19H3V8M6 13h12v10H6zM8 15h2v2H8zM14 15h2v2h-2zM8 19h2m3 0h3M18 10h1"/>',
      plain:'<circle cx="9" cy="15" r="7"/><circle cx="9" cy="5" r="2"/><path d="M15 7h9m-3-3 3 3-3 3"/>',
      complete:'<path d="M3 7l8-4 8 4-8 4zM3 7v12l8 4V11M19 7v5M7 5l8 4M3 19l8 4 3-1M17 20l2 2 5-6"/>',
      set:'<path d="M2 5l10-4 10 4-10 4zM2 10l10 4 10-4M2 15l10 4 10-4M2 20l10 4 10-4M15 12l3 3 5-6"/>'
    };
    const familyOf = st => st.how==='button'?'fulfilment':st.how==='laserReady' || st.how==='laserDone'?'laser':st.how==='engraveApproved' || st.how==='engravePlain'?'engraving':'prepared';
    function face(model, opts={}) {
      model = Object.assign({}, model);
      model.family=FAMILY[model.family]?model.family:'received';
      opts = Object.assign({}, opts, {ghost:!!(opts.ghost || model.ghost), signer:!!opts.signer && !model.status});
      if(opts.ghost)model.ghost=true;
      if (!model.date && +model.at > 0) model.date = dateOf(model.at);
      if (!model.time && +model.at > 0) model.time = timeOf(model.at);
      const f=FAMILY[model.family] || FAMILY.received, ink=opts.ghost?'#92968f':f.ink;
      const action=String(model.action || f.name).toUpperCase(), words=action.split(/\s+/), lines=(['BACK ENGRAVING','ORDER RECEIVED','ORDER COMPLETE'].includes(action) || action.length>14) && words.length>1?[words.slice(0,Math.ceil(words.length/2)).join(' '),words.slice(Math.ceil(words.length/2)).join(' ')]:[action];
      const serifFree='system-ui,-apple-system,BlinkMacSystemFont,Segoe UI,Arial,sans-serif';
      const txt=(value,y,size,weight=700,color=ink)=>`<text x="60" y="${y}" text-anchor="middle" fill="${color}" stroke="none" font-family="${serifFree}" font-size="${size}" font-weight="${weight}" letter-spacing="0">${esc(value)}</text>`;
      // Explicit typographic widths keep every glyph inside the actual family silhouette,
      // including the oval's lower shoulder and the cancellation ticket's narrow waist.
      const widths={' ': .3, I:.3,J:.5,L:.56,M:.86,W:.96,F:.6,E:.61,P:.65,S:.65,T:.62,O:.78,G:.78,C:.72,Q:.78,':':.28,'.':.28,'—':.8};
      const fitted=(value,y,size,width,color=ink)=>{
        value=String(value);const natural=[...value].reduce((n,c)=>n+(widths[c] ?? (/\d/.test(c)?.6:.68)),0)*size;
        return txt(value,y,size,700,color).replace('<text ',`<text data-seal-text textLength="${Math.min(width,natural).toFixed(2)}" lengthAdjust="spacingAndGlyphs" `);
      };
      const labelWidth=model.family==='cancelled'?62:lines.length===2?84:model.family==='prepared'?96:88;
      const signer=String(model.by || '').trim() || 'Not recorded', sigLines=signer.match(/.{1,21}(?:\s|$)|\S+/g)?.map(s=>s.trim()) || [signer];
      const H=opts.signer?137+sigLines.length*12:120;
      let trim='';
      if(model.family==='received' || model.family==='fulfilment')trim=`<path d="${f.d}" transform="translate(60 60) scale(.86) translate(-60 -60)" fill="none" stroke-width=".9" stroke-dasharray=".2 2.7" stroke-linecap="round"/>`;
      if(model.family==='prepared')trim='<path d="M15 17h8m-4-4v8M97 17h8m-4-4v8M15 103h8m-4-4v8M97 103h8m-4-4v8" fill="none" stroke-width=".8"/>';
      if(model.family==='engraving')trim='<path d="M16 37l11-11 3 8-11 6zM104 37L93 26l-3 8 11 6zM16 83l11 11 3-8-11-6zM104 83L93 94l-3-8 11-6z" fill="none" stroke-width="1.1"/>';
      if(model.family==='laser')trim='<g fill="none" stroke-width="1.2"><circle cx="30" cy="22" r="2"/><circle cx="90" cy="22" r="2"/><circle cx="20" cy="75" r="2"/><circle cx="100" cy="75" r="2"/></g>';
      const symbol=model.path?`<path d="${esc(model.path)}"/>`:SYMBOL[model.icon || model.family] || SYMBOL[model.family] || SYMBOL.received;
      return `<svg viewBox="0 0 120 ${H}" aria-hidden="true" focusable="false" data-seal-family="${esc(model.family || 'received')}" data-seal-model="${esc(JSON.stringify(model))}"><g fill="${ink}" stroke="${ink}" stroke-linecap="round" stroke-linejoin="round"${opts.ghost?' opacity=".55"':''}>`+
        `<path data-seal-paper d="${f.d}" fill="#fffefb" stroke="none" opacity="0"/>`+
        `<path data-seal-outline d="${f.d}" fill="rgba(255,254,251,.08)" stroke-width="2.7"${opts.ghost?' stroke-dasharray="3 3"':''}/><path d="${f.d}" transform="translate(60 60) scale(.93) translate(-60 -60)" fill="none" stroke-width=".9"/>${trim}`+
        `<g transform="translate(46.2 13) scale(1.15)" fill="none" stroke-width="1.45">${symbol}</g>`+
        lines.map((line,i)=>fitted(line,lines.length===2?54+i*14:64,lines.length===2 || line.length>10?14.6:15.8,labelWidth)).join('')+
        (opts.ghost || model.status?'':fitted(model.date || 'NOT RECORDED',82,model.date?14:11,model.family==='engraving'?82:86,META_INK)+fitted(model.time || '—',96,14.2,model.family==='engraving'?62:model.family==='finishing'?66:72,META_INK))+
        (opts.signer?'<path d="M32 124h56" fill="none" stroke-width=".7" opacity=".5"/>'+txt('Signed by',135,8,500,META_INK)+sigLines.map((s,i)=>txt(s,147+i*12,10,600,META_INK)).join(''):'')+'</g></svg>';
    }
    // A rounded wooden contact, with a faint cushioned paper tail. Generated locally: no network/audio-file delay.
    const Sound=(()=>{
      let context=null, buffer=null, contacts=0, lastContact=0;
      const volume=.6;
      const sync=()=>{doc.documentElement.dataset.stampAudio=context?.state || 'not-created';doc.documentElement.dataset.stampAudioReady=String(!!buffer);doc.documentElement.dataset.stampContacts=String(contacts);};
      function samples(rate=44100) {
        const a=new Float32Array(Math.ceil(rate*.32));let seed=928371,lp=0,paper=0,peak=0;
        const alpha=1-Math.exp(-2*Math.PI*1100/rate);
        for(let i=0;i<a.length;i++){
          const t=i/rate;seed=(Math.imul(seed,1664525)+1013904223)>>>0;const n=seed/2147483648-1;
          lp+=alpha*(n-lp);paper+=(1-Math.exp(-2*Math.PI*550/rate))*(n-paper);
          const onset=1-Math.exp(-t/.0018),body=.46*Math.sin(2*Math.PI*225*t)*Math.exp(-t/.040)+.16*Math.sin(2*Math.PI*390*t)*Math.exp(-t/.027)+.05*Math.sin(2*Math.PI*780*t)*Math.exp(-t/.016);
          const v=onset*(body+.38*lp*Math.exp(-t/.028)+.05*paper*Math.exp(-t/.074))*Math.min(1,(.32-t)/.02);
          a[i]=v;peak=Math.max(peak,Math.abs(v));
        }
        const gain=.08/(peak || 1);for(let i=0;i<a.length;i++)a[i]*=gain;
        return a;
      }
      function unlock() {
        try {
          const C=root.AudioContext || root.webkitAudioContext;if(!C)return Promise.resolve(false);
          if(!context || context.state==='closed'){context=new C({latencyHint:'interactive'});buffer=null;context.onstatechange=sync;}
          if(!buffer){const a=samples(context.sampleRate),ready=context.createBuffer(1,a.length,context.sampleRate);ready.copyToChannel(a,0);buffer=ready;}
          const resumed=context.state==='running'?Promise.resolve():context.resume();
          return Promise.resolve(resumed).then(()=>{sync();return context.state==='running';},()=>{sync();return false;});
        }catch(_){sync();return Promise.resolve(false);}
      }
      function play() {
        if(doc.visibilityState==='hidden' || !context || context.state!=='running' || !buffer)return false;
        let source, gain;
        try {
          source=context.createBufferSource();gain=context.createGain();source.buffer=buffer;gain.gain.value=volume;
          source.connect(gain);gain.connect(context.destination);source.onended=()=>{source.disconnect();gain.disconnect();};source.start();contacts++;lastContact=Date.now();sync();return true;
        }catch(_){try{source?.disconnect();gain?.disconnect();}catch(__){}return false;}
      }
      // These trusted gestures unlock before the async approval/export and 560ms descent, including a first-ever approval.
      for(const type of ['pointerdown','click','keydown','touchend'])doc.addEventListener(type,e=>{if(e.isTrusted)unlock();},{capture:true,passive:true});
      sync();
      return {unlock,play,samples,volume,state:()=>({state:context?.state || 'not-created',contacts,lastContact,volume})};
    })();
    let uid = 0, stamping=0, pressChain=Promise.resolve();
    const pendingDraws=new Map();
    const idleWaiters=new Set();
    const busy=()=>stamping>0;
    async function whenIdle(){while(busy())await new Promise(resolve=>idleWaiters.add(resolve));}
    function defer(key,fn){if(!busy())return false;pendingDraws.set(key,fn);return true;}
    function releaseDraws(){
      if(busy())return;
      requestAnimationFrame(()=>{
        if(busy())return;
        const work=[...pendingDraws.values()];pendingDraws.clear();
        for(const fn of work){try{fn();}catch(e){console.error('After stamping',e);}}
      });
    }
    // A second click, shortcut or pending redraw must never dismiss the surface under the stamp.
    for(const type of ['click','keydown'])doc.addEventListener(type,e=>{if(busy()){e.preventDefault();e.stopImmediatePropagation();}},true);
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
      let n = 0; for (const s of st) if (kindOf(s) === "print") s.n = ++n;
      const counted = +rec.prints || 0; if (counted > n) { let k = counted - n; for (const s of st) if (kindOf(s) === "print") s.n += k; }
      return st;
    }
    const dateOf = t => { const d = new Date(+t || Date.now()); return `${String(d.toLocaleString("en-US",{timeZone:"America/Toronto",day:"2-digit"})).padStart(2, "0")} ${d.toLocaleString("en-US", { timeZone:"America/Toronto", month: "short" }).toUpperCase()} ${d.toLocaleString("en-US",{timeZone:"America/Toronto",year:"numeric"})}`; };
    const timeOf = t => new Date(+t || Date.now()).toLocaleTimeString("en-US", { timeZone:"America/Toronto", hour: "numeric", minute: "2-digit" });
    const engravingOf=st=>st.how==='engraveApproved' || st.how==='engravePlain';
    const processOf = st => st.how === 'sheet' || st.how === 'laserReady' || st.how === 'laserDone' || engravingOf(st);
    const kindOf = st => processOf(st) ? st.how : st.how === "button" ? "button" : "print";
    const green = k => k === 'button' || k === 'laserDone' || k === 'engraveApproved';
    const rotOf = st => { const h = hash(String(st.at) + (st.by || "")); return green(kindOf(st)) ? 5 + (h % 8) : -(5 + (h % 9)); };
    const whoOf = st => String(st.by || "").trim() || (processOf(st) ? "Not recorded" : "Sorting station");
    function modelOf(st) {
      if (st.model) return Object.assign({}, st.model);
      if (st.family) return Object.assign({}, st);
      const action = st.how === 'sheet' ? 'SENT TO SHEET' : st.how === 'button' ? 'ORDER COMPLETE' : st.how === 'laserReady' ? 'LASER READY' : st.how === 'laserDone' ? 'LASER CUT' : st.how === 'engraveApproved' ? 'BACK ENGRAVING' : st.how === 'engravePlain' ? 'CUT PLAIN' : 'QR LABEL PRINTED';
      const icon = st.how === 'sheet' ? 'prepared' : st.how === 'button' ? 'complete' : st.how === 'laserReady' ? 'laserReady' : st.how === 'laserDone' ? 'laser' : st.how === 'engravePlain' ? 'plain' : st.how === 'engraveApproved' ? 'engraving' : 'qr';
      return { family:familyOf(st), action, icon, at:+st.at || 0, by:String(st.by || ''), scope:st.scope || '', n:st.n || 0 };
    }
    /** The selected hallmark family. Operator identity belongs only in the enlarged historical detail. */
    function svg(st) { return face(modelOf(st)); }
    function titleOf(st) {
      const d = new Date(+st.at || Date.now()), when = d.toLocaleString("en-US", { timeZone:"America/Toronto", weekday: "short", month: "short", day: "numeric", year: "numeric", hour: "numeric", minute: "2-digit" });
      if(st.how==='sheet')return `Sent to sheet${st.by?' by '+st.by:' · operator not recorded'} · ${st.at?when:'time not recorded'}`;
      if(engravingOf(st))return `${st.how==='engravePlain'?'Engraving waived · cut plain':'Engraving placement approved'}${st.by?' by '+st.by:' · operator not recorded'} · ${st.at?when:'time not recorded'}`;
      if(processOf(st))return `${st.how==='laserReady'?'Laser ready':st.scope==='set'?'Set completed':'Sheet completed'}${st.by?' by '+st.by:' · operator not recorded'} · ${st.at?when:'time not recorded'}`;
      return kindOf(st) === "button" ? `Completed with the Complete Order button by ${whoOf(st)} · ${when} (no label printed then)` : `QR label printed by ${whoOf(st)} · ${when}${st.n > 1 ? ` · print ${st.n}` : ""}`;
    }
    /** One seal, as it sits on a card (one in a row is reached with Tab, and read large in the lens below). */
    function html(st, size = BASE_SIZE, extra) {
      size = BASE_SIZE;
      const reach = /\bloose\b/.test(extra || "") ? "" : ` tabindex="0" role="img" aria-label="${esc(titleOf(st))}"`;
      return `<span class="seal seal-${kindOf(st)}${extra ? " " + extra : ""}" style="--rot:${rotOf(st)}deg;--sz:${size}px" data-at="${+st.at || 0}"${processOf(st)?` data-process-seal="${esc(st.id || '')}" data-seal-owner="${esc(st.owner || '')}"`:''}${reach}>${svg(st)}</span>`;
    }
    const sizeFor = () => BASE_SIZE;
    /** The card's seals, oldest first, beside the button they belong with. pending: the time of a seal just pressed
     *  (it waits unseen until pressPending stamps it); size: a fixed size (the order window's small seals). */
    function row(rec, opts = {}) {
      const st = list(rec); if (!st.length) return "";
      const shown = st, more = 0, size = BASE_SIZE;
      return `<span class="sealRow n${Math.min(shown.length, 5)}${opts.size ? " mini" : ""}"${opts.size ? ` data-seal-max="${Math.round(+opts.size) || BASE_SIZE}"` : ""} data-seal-group data-seal-count="${shown.length}" role="group" aria-label="${st.length === 1 ? "Seal" : st.length + " seals"}">` +
        (more ? `<span class="sealMore" title="${esc(st.slice(0, more).map(titleOf).join("\n"))}">+${more} earlier</span>` : "") +
        shown.map(s => html(s, size, opts.pending && +s.at >= opts.pending ? "pending" : "")).join("") + `</span>`;
    }
    /** One uniform fit for a history group. Independent seals keep the canonical size. */
    let groupSizeObserver=null;
    const fitParents=new Set();
    function fit(row) {
      const seals=[...row.children].filter(n=>n.classList.contains('seal'));
      // a strip that asks for smaller seals (the order window's one line, data-seal-max) never grows them past that
      const MAX=Math.max(16,Math.min(BASE_SIZE,+row.dataset.sealMax||BASE_SIZE));
      if(!seals.length)return MAX;
      row.style.setProperty('--seal-fit',MAX+'px');
      const parent=row.parentElement, cs=getComputedStyle(row), ps=parent?getComputedStyle(parent):null;
      if(parent && groupSizeObserver && !fitParents.has(parent)){fitParents.add(parent);groupSizeObserver.observe(parent);}
      let available=parent?.clientWidth || parent?.getBoundingClientRect().width || row.clientWidth || seals.length*BASE_SIZE;
      if(ps)available-= (parseFloat(ps.paddingLeft)||0)+(parseFloat(ps.paddingRight)||0);
      const cap=cs.maxWidth;let capW=Infinity;if(cap && cap!=='none')capW=cap.endsWith('%')?available*parseFloat(cap)/100:parseFloat(cap)||Infinity;
      if(cs.position==='absolute'){
        available=Math.min(available,capW);
        for(const side of ['left','right'])if(/^[\d.]+px$/.test(cs[side] || ''))available-=parseFloat(cs[side]);
      } else if(ps && /flex/.test(ps.display) && ps.flexDirection!=='column') {
        // a sibling that grows to take what is left (the order window's one line of words) gives its room up: it is not a cost
        const siblings=[...parent.children].filter(n=>n!==row && getComputedStyle(n).position!=='absolute' && !(parseFloat(getComputedStyle(n).flexGrow)>0));
        for(const n of siblings){const ns=getComputedStyle(n);available-=n.getBoundingClientRect().width+(parseFloat(ns.marginLeft)||0)+(parseFloat(ns.marginRight)||0);}
        available-=(parseFloat(ps.columnGap)||0)*siblings.length;
        // (the cap and the room the siblings leave are two limits on the one width, not two costs to add up)
        available=Math.min(available,capW);
      } else available=Math.min(available,capW);
      available-=(parseFloat(cs.marginLeft)||0)+(parseFloat(cs.marginRight)||0);
      const rowWidth=row.clientWidth || row.getBoundingClientRect().width;
      if(rowWidth>0)available=Math.min(available,rowWidth-(parseFloat(cs.paddingLeft)||0)-(parseFloat(cs.paddingRight)||0));
      const gap=parseFloat(cs.columnGap)||parseFloat(cs.gap)||4;
      const other=[...row.children].filter(n=>!seals.includes(n)).reduce((n,e)=>n+e.getBoundingClientRect().width,0);
      let size=seals.length===1?MAX:Math.max(1,Math.min(MAX,(available-other-gap*(row.children.length-1))/seals.length));
      if(row.classList.contains('egButtonSeal') && row.closest('.swEng')){
        // This row intentionally overlaps half a seal onto its button. Solve the
        // available width before applying the fit, rather than measuring the previous fit.
        const button=parent?.querySelector('.egApproveButton'),room=(parent?.clientWidth || 0)-(button?.offsetWidth || 144)-8-(parseFloat(cs.paddingLeft)||0)-(parseFloat(cs.paddingRight)||0)-gap*(seals.length-1);
        size=Math.max(1,Math.min(BASE_SIZE,room/(seals.length-.5)));
      }
      const value=(Math.floor(size*100)/100)+'px';
      if(row.style.getPropertyValue('--seal-fit')!==value)row.style.setProperty('--seal-fit',value);
      row.dataset.sealCount=String(seals.length);return size;
    }
    function fitGroups(scope=doc) {
      if(busy()){defer('seal-layout',()=>fitGroups(scope));return;}
      for(const parent of fitParents)if(!parent.isConnected){groupSizeObserver?.unobserve(parent);fitParents.delete(parent);}
      if(scope.matches?.('.sealRow'))fit(scope);
      for(const row of scope.querySelectorAll?.('.sealRow') || [])fit(row);
    }
    let fitting=0;
    const scheduleFit=()=>{if(!fitting)fitting=requestAnimationFrame(()=>{fitting=0;fitGroups();});};
    const rowObserver=new MutationObserver(()=>{scheduleFit();zoom.check();});
    rowObserver.observe(doc.documentElement,{subtree:true,childList:true});
    if(root.ResizeObserver){groupSizeObserver=new root.ResizeObserver(scheduleFit);groupSizeObserver.observe(doc.documentElement);}
    root.addEventListener('resize',scheduleFit);scheduleFit();
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
    function queued(work) {
      stamping++;
      const run=pressChain.catch(()=>{}).then(work);pressChain=run;
      return run.finally(()=>{stamping--;if(!busy()){for(const resolve of idleWaiters)resolve();idleWaiters.clear();}releaseDraws();});
    }
    function press(seal) {
      if (!seal) return Promise.resolve();
      if(seal._press)return seal._press;
      const run=queued(()=>pressOnce(seal));seal._press=run;
      run.finally(()=>{if(seal._press===run)seal._press=null;}).catch(()=>{});
      return run;
    }
    async function incomingMotion(seal) {
      const surface=seal.closest('.rvItem,.swEng,.libCard,.setCard') || seal.parentElement;
      const animations=[...(surface?.getAnimations?.({subtree:true}) || []),...[...doc.querySelectorAll('.mGhost')].filter(n=>!n.contains(seal)).flatMap(n=>n.getAnimations?.({subtree:true}) || [])];
      for(let n=seal.parentElement;n;n=n.parentElement)animations.push(...(n.getAnimations?.() || []));
      const finite=[...new Set(animations)].filter(a=>a.playState==='running' && Number.isFinite(a.effect?.getComputedTiming().endTime));
      await Promise.allSettled(finite.map(a=>a.finished));
    }
    function geometry(seal) {
      const face=seal.querySelector('svg'), sr=seal.getBoundingClientRect(), fr=face?.getBoundingClientRect(), r=fr?.width>0?fr:sr;
      const cs=getComputedStyle(seal), numeric=v=>/^[\d.]+px$/.test(v || '')?parseFloat(v):0;
      const w=seal.offsetWidth || numeric(cs.width) || r.width, h=seal.offsetHeight || numeric(cs.height) || w;
      return {left:r.left+r.width/2-w/2,top:r.top+r.height/2-h/2,width:w,height:h,rect:r};
    }
    // A native scroll or responsive panel move can shift the paper while a fixed tool is travelling.
    // Keep its base and contact ripple centred on the actual seal until the full press has finished.
    function anchorFixed(seal,...nodes) {
      const targets=new Set(nodes);let frame=0,active=true;
      function place(){
        if(!active || !seal.isConnected)return;
        const g=geometry(seal);
        for(const node of targets)if(node.isConnected)Object.assign(node.style,{left:g.left+'px',top:g.top+'px',width:g.width+'px',height:g.height+'px'});
      }
      function tick(){
        frame=0;if(!active || !seal.isConnected || ![...targets].some(n=>n.isConnected))return;
        place();frame=requestAnimationFrame(tick);
      }
      place();frame=requestAnimationFrame(tick);
      return {add(node){targets.add(node);place();},stop(){active=false;cancelAnimationFrame(frame);targets.clear();}};
    }
    // If another animation manager cancels a tool, resume its remaining travel before revealing ink or advancing.
    async function finishPhase(animation,node,end,duration) {
      const started=root.performance.now();
      try{await animation.finished;return;}catch(_){}
      const remaining=Math.max(0,duration-(root.performance.now()-started));
      if(!remaining)return;
      const restart=root.performance.now();
      if(node.isConnected){
        const cs=getComputedStyle(node), resumed=node.animate([{transform:cs.transform,opacity:cs.opacity},end],{duration:remaining,easing:'cubic-bezier(.2,.7,.3,1)',fill:'forwards'});
        try{await resumed.finished;return;}catch(_){}
      }
      await wait(Math.max(0,remaining-(root.performance.now()-restart)));
    }
    async function pressOnce(seal) {
      if(reduced() || !visible(seal)){seal.classList.remove('pending');return;}
      await incomingMotion(seal);
      if(!seal.isConnected || !visible(seal))return;
      const g=geometry(seal), size=g.width, kind=Object.keys(INK).find(k=>seal.classList.contains('seal-'+k)) || 'print';
      const rot=parseFloat(getComputedStyle(seal).getPropertyValue('--rot')) || 0;
      const t=doc.createElement('span');t.className='sealTool';t.innerHTML=tool(kind,seal);
      Object.assign(t.style,{position:'fixed',left:g.left+'px',top:g.top+'px',width:g.width+'px',height:g.height+'px'});
      const ink=FAMILY[t.querySelector('svg')?.dataset.stampFamily]?.ink || INK[kind];
      layer(seal).appendChild(t);
      const anchor=anchorFixed(seal,t);
      try {
      const down=t.animate([
        {transform:`translate(34px,-46px) rotate(${rot-16}deg) scale(1.7)`,opacity:0},
        {opacity:1,offset:.22},
        {transform:`translate(0,0) rotate(${rot}deg) scale(1)`,opacity:1}
      ],{duration:560,easing:'cubic-bezier(.62,0,.92,.5)',fill:'forwards'});
      const shade=shadow(t,size,SHADE.far,SHADE.near,{duration:560,easing:'cubic-bezier(.62,0,.92,.5)'});
      await finishPhase(down,t,{transform:`translate(0,0) rotate(${rot}deg) scale(1)`,opacity:1},560);
      Sound.play();seal.classList.remove('pending');seal.classList.add('wet');
      const btn=btnOf(seal);if(btn)btn.classList.add(green(kind)?'sealedDone':'sealedPrint');
      const ring=doc.createElement('span');ring.className='sealRing';ring.style.cssText=`position:fixed;left:${g.left}px;top:${g.top}px;width:${g.width}px;height:${g.height}px;--ink:${ink}`;layer(seal).appendChild(ring);
      anchor.add(ring);
      const ripple=ring.animate([{transform:'scale(.86)',opacity:.42},{transform:'scale(1.42)',opacity:0}],{duration:760,easing:'cubic-bezier(.2,.7,.3,1)',fill:'forwards'});
      ripple.finished.then(()=>ring.remove(),()=>ring.remove());
      const up=t.animate([
        {transform:`rotate(${rot}deg) scale(1)`,opacity:1},
        {transform:`rotate(${rot}deg) scale(.95)`,opacity:1,offset:.22},
        {transform:`translate(-16px,-40px) rotate(${rot+8}deg) scale(1.55)`,opacity:0}
      ],{duration:700,easing:'cubic-bezier(.2,.7,.3,1)',fill:'forwards'});
      const lifted=finishPhase(up,t,{transform:`translate(-16px,-40px) rotate(${rot+8}deg) scale(1.55)`,opacity:0},700);
      shade.to(SHADE.near,SHADE.lift,{duration:700,easing:'cubic-bezier(.2,.7,.3,1)'});
      await wait(260);seal.classList.remove('wet');
      await Promise.allSettled([lifted,ripple.finished,...(seal.getAnimations?.() || []).filter(a=>Number.isFinite(a.effect?.getComputedTiming().endTime)).map(a=>a.finished)]);
      } finally {anchor.stop();t.remove();}
    }
    const hasPrint = rec => list(rec).some(s => kindOf(s) === "print");
    const WOOD_URL=(()=>{try{return new URL('charm-nest-stamp-walnut.png',doc.currentScript?.src || doc.baseURI).href;}catch(_){return 'charm-nest-stamp-walnut.png';}})();
    /** The rubber die and walnut head share the exact outline of the ink they leave. */
    function tool(kind, source) {
      if(typeof source==='string'){const holder=doc.createElement('span');holder.innerHTML=source;source=holder;}
      const sourceSvg=source?.matches?.('svg')?source:source?.querySelector?.('svg');
      const family=sourceSvg?.dataset.sealFamily || (FAMILY[kind]?kind:familyOf({how:kind}));
      const f=FAMILY[family] || FAMILY.prepared, d=sourceSvg?.querySelector('[data-seal-outline]')?.getAttribute('d') || f.d, id='st'+(++uid);
      return `<svg viewBox="0 0 120 120" aria-hidden="true" data-stamp-family="${family}"><defs>`+
        `<clipPath id="${id}grain"><path d="${d}"/></clipPath><clipPath id="${id}knob"><circle cx="60" cy="53" r="26"/></clipPath>`+
        `<linearGradient id="${id}light" x1="0" y1="0" x2="1" y2="1"><stop stop-color="#ffead0" stop-opacity=".4"/><stop offset=".32" stop-color="#fff1d8" stop-opacity=".07"/><stop offset=".75" stop-color="#241108" stop-opacity=".32"/><stop offset="1" stop-color="#150a04" stop-opacity=".65"/></linearGradient>`+
        `<radialGradient id="${id}knobLight" cx=".30" cy=".24" r=".8"><stop stop-color="#fff1d2" stop-opacity=".55"/><stop offset=".42" stop-color="#de9d5c" stop-opacity=".02"/><stop offset=".73" stop-color="#391809" stop-opacity=".3"/><stop offset="1" stop-color="#120905" stop-opacity=".82"/></radialGradient>`+
        `<radialGradient id="${id}stem" cx=".30" cy=".20" r=".8"><stop stop-color="#d59656"/><stop offset=".45" stop-color="#713b1e"/><stop offset="1" stop-color="#291208"/></radialGradient></defs>`+
        `<path data-stamp-contact d="${d}" fill="${f.ink}" stroke="${f.ink}" stroke-width="2.7" stroke-linejoin="round"/>`+
        `<g transform="translate(60 59) scale(.975) translate(-60 -60)"><path d="${d}" fill="#673519" stroke="#2e170c" stroke-width="2"/>`+
        `<g clip-path="url(#${id}grain)"><image href="${esc(WOOD_URL)}" x="0" y="0" width="120" height="120" preserveAspectRatio="xMidYMid slice"/><path d="${d}" fill="url(#${id}light)"/></g>`+
        `<path d="${d}" fill="none" stroke="#f6c68d" stroke-opacity=".53" stroke-width="1.8" transform="translate(60 60) scale(.95) translate(-60 -60)"/><path d="${d}" fill="none" stroke="#28160c" stroke-opacity=".55" stroke-width="1" transform="translate(60 60) scale(.89) translate(-60 -60)"/></g>`+
        `<ellipse cx="62" cy="64" rx="28" ry="22" fill="#201008" opacity=".30"/><circle cx="60" cy="60" r="21" fill="url(#${id}stem)" stroke="#e7ae76" stroke-opacity=".35"/>`+
        `<circle cx="60" cy="53" r="26" fill="#7e4425" stroke="#29160c" stroke-width="1.3"/><image href="${esc(WOOD_URL)}" x="31" y="21" width="65" height="63" clip-path="url(#${id}knob)" preserveAspectRatio="xMidYMid slice"/>`+
        `<circle cx="60" cy="53" r="26" fill="url(#${id}knobLight)"/><path d="M40 42C43 31 57 27 66 30" fill="none" stroke="#fff1d4" stroke-opacity=".49" stroke-width="1.4" stroke-linecap="round"/><ellipse cx="47" cy="37" rx="8" ry="4" fill="#fff5df" opacity=".18" transform="rotate(-28 47 37)"/></svg>`;
    }
    /** A new seal pressed onto `spec.btn` (a selector inside host, or an element): the stamp comes down, the ink is left,
     *  the stamp lifts away, and the button takes the seal's colour. */
    function stampOn(host, spec) {
      const btn=typeof spec.btn==='string'?host.querySelector(spec.btn):spec.btn;
      if(!btn)return Promise.resolve();
      if(btn._sealStamp)return btn._sealStamp;
      const run=queued(()=>stampOnOnce(host,spec,btn));btn._sealStamp=run;
      run.finally(()=>{if(btn._sealStamp===run)btn._sealStamp=null;}).catch(()=>{});
      return run;
    }
    async function stampOnOnce(host,spec,btn) {
      if(!host.isConnected || !btn.isConnected)return;
      const st=spec.stamp || spec, kind=kindOf(st), size=BASE_SIZE;
      if(spec.label)btn.innerHTML=esc(spec.label);
      btn.disabled=false;btn.removeAttribute('aria-busy');btn.classList.remove('working');
      await incomingMotion(btn);
      const hr=host.getBoundingClientRect(),br=btn.getBoundingClientRect();
      const cx=br.left+Math.min(br.width*.64,br.width-18)-hr.left,cy=br.top+br.height/2-hr.top;
      const holder=doc.createElement('span');holder.innerHTML=html(st,size,'loose');const seal=holder.firstChild;
      Object.assign(seal.style,{left:cx-size/2+'px',top:cy-size/2+'px',opacity:'0','--seal-fit':size+'px'});
      const t=doc.createElement('span');t.className='sealTool';t.innerHTML=tool(kind,seal);
      Object.assign(t.style,{left:cx-size/2+'px',top:cy-size/2+'px',width:size+'px',height:size+'px'});
      host.appendChild(seal);host.appendChild(t);
      const rot=rotOf(st),paint=()=>btn.classList.add(kind==='button'?'sealedDone':'sealedPrint');
      if(reduced() || spec.still){seal.style.opacity='';t.remove();paint();return;}
      const down=t.animate([
        {transform:`translate(34px,-46px) rotate(${rot-16}deg) scale(1.7)`,opacity:0},
        {opacity:1,offset:.22},
        {transform:`translate(0,0) rotate(${rot}deg) scale(1)`,opacity:1}
      ],{duration:560,easing:'cubic-bezier(.62,0,.92,.5)',fill:'forwards'});
      const shade=shadow(t,size,SHADE.far,SHADE.near,{duration:560,easing:'cubic-bezier(.62,0,.92,.5)'});
      await finishPhase(down,t,{transform:`translate(0,0) rotate(${rot}deg) scale(1)`,opacity:1},560);
      Sound.play();seal.style.opacity='';seal.classList.add('wet');paint();
      const card=host._card || host;
      const settle=card.animate([{transform:'translateY(0)'},{transform:'translateY(1.6px)',offset:.3},{transform:'translateY(0)'}],{duration:240,easing:'ease-out'});
      const ring=doc.createElement('span');ring.className='sealRing';ring.style.cssText=`left:${cx-size/2}px;top:${cy-size/2}px;width:${size}px;height:${size}px;--ink:${FAMILY[t.querySelector('svg')?.dataset.stampFamily]?.ink || INK[kind]}`;host.appendChild(ring);
      const ripple=ring.animate([{transform:'scale(.86)',opacity:.42},{transform:'scale(1.42)',opacity:0}],{duration:760,easing:'cubic-bezier(.2,.7,.3,1)',fill:'forwards'});
      ripple.finished.then(()=>ring.remove(),()=>ring.remove());
      const up=t.animate([
        {transform:`rotate(${rot}deg) scale(1)`,opacity:1},
        {transform:`rotate(${rot}deg) scale(.95)`,opacity:1,offset:.22},
        {transform:`translate(-16px,-40px) rotate(${rot+8}deg) scale(1.55)`,opacity:0}
      ],{duration:700,easing:'cubic-bezier(.2,.7,.3,1)',fill:'forwards'});
      const lifted=finishPhase(up,t,{transform:`translate(-16px,-40px) rotate(${rot+8}deg) scale(1.55)`,opacity:0},700);
      shade.to(SHADE.near,SHADE.lift,{duration:700,easing:'cubic-bezier(.2,.7,.3,1)'});
      await wait(260);seal.classList.remove('wet');
      await Promise.allSettled([lifted,ripple.finished,settle.finished,...(seal.getAnimations?.() || []).filter(a=>Number.isFinite(a.effect?.getComputedTiming().endTime)).map(a=>a.finished)]);t.remove();
    }
    /* a seal over its button: a press there presses the button (and the seal sinks with it); anywhere else it wobbles */
    // (the button just before its row: an open card has its print seals on Print QR label and its Complete Order seals on
    // Complete Order, 29 Sep; else the row's first sealed button, as a completed card has one)
    const btnOf = s => {
      if(s.hasAttribute('data-process-seal'))return null; // historical seals never forward a click to Complete or Undo
      const r = s.closest(".sealRow"); if (!r) return null;
      let p = r.previousElementSibling; while (p && p.classList.contains("sealRow")) p = p.previousElementSibling;
      if (p && p.matches("[data-seal-btn]")) return p;
      return r.parentElement ? r.parentElement.querySelector("[data-seal-btn]") : null;
    };
    const rowOf = b => { const n = b.nextElementSibling; return n && n.classList.contains("sealRow") ? n : b.parentElement && b.parentElement.querySelector(".sealRow"); };
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
      const row = rowOf(b); if (row) row.classList.add("press");
    });
    const release = () => { if (!pressed) return; pressed.classList.remove("act"); const row = rowOf(pressed); if (row) { row.classList.remove("press"); row.classList.remove("spring"); void row.offsetWidth; row.classList.add("spring"); } pressed = null; };
    doc.addEventListener("pointerup", release); doc.addEventListener("pointercancel", release);

    /* ── the seal zoom (Paul, 2 Oct 18:56: "get rid of the hover states on all the seals and just add a compelling zooming
       animation where the seal grows in size in its current position, almost like a magnifying glass ... it should zoom very
       little if it's already large, more if it's smaller, even more if it's very small: an adaptive system", and 18:56
       "a 750 ms delay so the zoom does not happen immediately if the person runs the cursor quickly across the screen"; then 2 Oct,
       "change the delay before showing the zoom to 500 ms").
       There is no second seal and no bubble. The seal itself grows from its own centre, turns to stand upright, lifts on a
       soft shadow, takes a paper backing so nothing under it shows through, comes to the top and goes back the same way.
       Moved by transform, filter and opacity only (the paper is one path's opacity), interruptible at any frame, and always
       fully in view: it is nudged away from the viewport's edge, the top bar and any scrolling box that holds it; a box that
       only clips it (an overflow:hidden strip or card) is opened for as long as it is zoomed and closed again after.
       Mouse: the pointer must rest on the same seal for ZOOM_DELAY, or a click zooms it at once. Tab (:focus-visible) and a tap zoom at once. Leaving,
       blur, Esc, scrolling, a tap elsewhere or the window going away puts it back. The one scale curve is pure
       (Seal.zoomScale, Motion.sealZoomScale), so large seals grow a little and tiny ones a little more.
       Gentle (Paul, 3 Oct: "all of these seals are too big ... they don't have to zoom in so large it's obsessive. Especially on the
       flow chart, zoom is way excessive for the seal"): a seal of 64 px or more grows ×1.08, 56 px ×1.15, 40 px ×1.3, 24 px ×1.55 and
       16 px or less ×1.8, and whatever the curve says the grown seal is never wider than ZOOM_CAP (96 px), or ZOOM_CAP_TIGHT (72 px) in
       the order timeline and the order window. A seal already wider than its cap only lifts and stands upright (×1, never smaller). ── */
    const ZOOM_DELAY = 500;
    const ZOOM_ANCHORS = [[16, 1.8], [24, 1.55], [40, 1.3], [56, 1.15], [64, 1.08]];
    const ZOOM_CAP = 96, ZOOM_CAP_TIGHT = 72, ZOOM_TIGHT = ".tlUI, #orderWin, .owNowCard";
    /** The zoom of a seal as drawn `size` px across: 1.08 for a large one up to 1.8 for a tiny one, by one smooth curve. With a `cap`
     *  (the widest the grown seal may be, px) it is held to that, but never below 1. */
    function zoomScale(size, cap) {
      const n = +size, A = ZOOM_ANCHORS, c = +cap;
      let k;
      if (!(n > 0) || n >= A[A.length - 1][0]) k = A[A.length - 1][1];
      else if (n <= A[0][0]) k = A[0][1];
      else {
        let i = 0; while (n > A[i + 1][0]) i++;
        const [s0, k0] = A[i], [s1, k1] = A[i + 1], t = Math.log(n / s0) / Math.log(s1 / s0), e = t * t * (3 - 2 * t);
        k = k0 + (k1 - k0) * e;
      }
      if (n > 0 && c > 0) k = Math.max(1, Math.min(k, c / n));
      return Math.round(k * 1000) / 1000;
    }
    /** The widest `el` may grow to: a seal of the order timeline or the order window 72 px, any other 96 (data-seal-cap on it or a parent says otherwise). */
    function zoomCap(el) {
      const own = el && el.closest ? el.closest("[data-seal-cap]") : null, n = own ? +own.getAttribute("data-seal-cap") : 0;
      return n > 0 ? n : el && el.closest && el.closest(ZOOM_TIGHT) ? ZOOM_CAP_TIGHT : ZOOM_CAP;
    }
    const ZOOM_GROW = 300, ZOOM_BACK = 230, ZOOM_EASE = "cubic-bezier(.2,.9,.25,1.14)", ZOOM_BACK_EASE = "cubic-bezier(.3,0,.2,1)", ZOOM_Z = 900, ZOOM_M = 6;
    /** The lift's soft shadow, drawn in the seal's own pixels (the grow scales it): about 8% down and 13% blurred of the grown seal, so a small seal gets a small one. */
    const zoomShadow = size => `drop-shadow(0 ${Math.max(1.5, size * .08).toFixed(1)}px ${Math.max(2.5, size * .13).toFixed(1)}px rgba(30,26,20,.26))`;
    (() => {
      if (doc.getElementById("sealZoomCss")) return;
      const st = doc.createElement("style"); st.id = "sealZoomCss";
      st.textContent = ".sealZoomed,.sealZoomed svg{mix-blend-mode:normal!important}.sealZoomed,.sealZoomed:focus-visible{outline:0!important}";   // (a grown seal needs no square focus ring: its growing is the sign of focus)
      (doc.head || doc.documentElement).appendChild(st);
    })();
    const parseM = t => {
      let m = /^matrix\(([^)]+)\)$/.exec(t || ""); if (m) { const v = m[1].split(",").map(Number); if (v.length === 6 && v.every(isFinite)) return v; }
      m = /^matrix3d\(([^)]+)\)$/.exec(t || ""); if (m) { const v = m[1].split(",").map(Number); if (v.length === 16 && v.every(isFinite)) return [v[0], v[1], v[4], v[5], v[12], v[13]]; }
      return [1, 0, 0, 1, 0, 0];
    };
    const ZS = new WeakMap(), ACT = new Set();   // seal -> its zoom: { el, on, anim, paper, undo, k, rect, touch, keyboard, managed }; ACT: every zoom still on screen
    const Zm = { cur: null, want: null, t: 0, watch: 0, point: null, hover: false, pre: null, off: null };   // pre: the seal already grown when a press began; off: the seal a click put back (the pointer resting on it must not bring it round again)
    const paperOf = s => s.querySelector("[data-seal-paper]");
    /** What clips or covers a grown seal, worked out for the seal's final rectangle; → what to open and where to nudge. */
    function zoomPlan(el, k, r, W, H) {
      const cx = r.left + r.width / 2, cy = r.top + r.height / 2, hw = W * k / 2, hh = H * k / 2, vw = root.innerWidth || 1200, vh = root.innerHeight || 800;
      let L = ZOOM_M, T = ZOOM_M, R = vw - ZOOM_M, B = vh - ZOOM_M;
      if (!el.closest("dialog")) {   // below the top bar, as the page's own views are
        const tb = doc.querySelector(".topbar"), q = tb && tb.getClientRects().length ? tb.getBoundingClientRect() : null;
        if (q && q.bottom <= cy && q.bottom < vh / 2) T = Math.max(T, q.bottom + 4);
      }
      const clippers = [];
      for (let a = el.parentElement; a && a !== doc.body && a !== doc.documentElement; a = a.parentElement) {
        const cs = getComputedStyle(a), clip = cs.overflowX !== "visible" || cs.overflowY !== "visible", cv = cs.contentVisibility === "auto";
        if (!clip && !cv) continue;
        const ar = a.getBoundingClientRect(); if (!(a.clientWidth > 0 && a.clientHeight > 0)) continue;
        const box = { l: ar.left + a.clientLeft, t: ar.top + a.clientTop, r: ar.left + a.clientLeft + a.clientWidth, b: ar.top + a.clientTop + a.clientHeight };
        const scrolls = /auto|scroll/.test(cs.overflowX + cs.overflowY) && (a.scrollWidth > a.clientWidth + 1 || a.scrollHeight > a.clientHeight + 1) || a.scrollLeft || a.scrollTop;
        clippers.push({ a, box, scrolls: !!scrolls, cv });
      }
      // a box that scrolls cannot be opened (its content would move): the seal stays inside it, if it fits there
      for (const c of clippers) if (c.scrolls && 2 * hw <= c.box.r - c.box.l && 2 * hh <= c.box.b - c.box.t) { L = Math.max(L, c.box.l + 2); T = Math.max(T, c.box.t + 2); R = Math.min(R, c.box.r - 2); B = Math.min(B, c.box.b - 2); }
      // (never so far that the seal's own place is left uncovered: the pointer there would leave and come back)
      const nudge = (c, h, lo, hi, base) => Math.max(-(h - base / 2), Math.min(h - base / 2, hi - lo < 2 * h ? (lo + hi) / 2 - c : Math.max(lo + h, Math.min(hi - h, c)) - c));
      const dx = nudge(cx, hw, L, R, W), dy = nudge(cy, hh, T, B, H);
      const rect = { left: cx + dx - hw, top: cy + dy - hh, right: cx + dx + hw, bottom: cy + dy + hh };
      const open = clippers.filter(c => !c.scrolls && (rect.left < c.box.l - .5 || rect.top < c.box.t - .5 || rect.right > c.box.r + .5 || rect.bottom > c.box.b + .5));
      return { dx, dy, rect, open };
    }
    /** Raises the seal over everything and opens what would clip it; → a function that puts every one of them back. */
    function zoomLift(el, open, k) {
      const saved = [], keep = (n, ...props) => saved.push([n, props.map(p => [p, n.style.getPropertyValue(p), n.style.getPropertyPriority(p)])]);
      keep(el, "--zk"); el.style.setProperty("--zk", String(k));   // (the grown seal's scale, for a ring or glow of the seal's own that must stay thin: calc(3px / var(--zk, 1)))
      for (const c of open) { keep(c.a, "overflow", "content-visibility"); c.a.style.setProperty("overflow", "visible"); if (c.cv) c.a.style.setProperty("content-visibility", "visible"); }
      for (let a = el; a && a !== doc.body && a !== doc.documentElement; a = a.parentElement) {
        if (a !== el && a.matches && a.matches("dialog")) break;
        const cs = getComputedStyle(a), pos = cs.position;
        const ctx = a === el || pos === "fixed" || pos === "sticky" || (pos !== "static" && cs.zIndex !== "auto") || +cs.opacity < 1 || cs.transform !== "none" || cs.filter !== "none" || cs.isolation === "isolate" || cs.mixBlendMode !== "normal" || /layout|paint|strict|content/.test(cs.contain) || (cs.containerType && cs.containerType !== "normal") || /transform|opacity|filter/.test(cs.willChange) || cs.perspective !== "none";
        if (ctx) { keep(a, "position", "z-index"); if (pos === "static") a.style.setProperty("position", "relative"); a.style.setProperty("z-index", String(ZOOM_Z)); }
      }
      el.classList.add("sealZoomed");
      return () => { el.classList.remove("sealZoomed"); for (const [n, props] of saved) for (const [p, v, pr] of props) { if (v) n.style.setProperty(p, v, pr); else n.style.removeProperty(p); } };
    }
    /** Starts (or resumes, from wherever a return had got to) the zoom of `el`. → true when it is zooming. */
    function zoomShow(el, o = {}) {
      if (!canZoom(el)) return false;
      cancelWant();
      if (Zm.cur && Zm.cur.el !== el) zoomHide(false);
      let st = ZS.get(el); if (st && st.on) { if (o.keyboard != null) st.keyboard = !!o.keyboard; return true; }
      const cs = getComputedStyle(el), from = { transform: cs.transform, filter: cs.filter, opacity: cs.opacity }, pfrom = paperOf(el) ? getComputedStyle(paperOf(el)).opacity : "0";
      if (st) { st.anim && st.anim.cancel(); st.paper && st.paper.cancel(); if (st.undo) { st.undo(); st.undo = null; } } else st = { undo: null };
      // the place it rests in, measured with no zoom on it and nothing opened
      const bs = getComputedStyle(el), base = parseM(bs.transform), r = el.getBoundingClientRect();
      const W = el.offsetWidth || r.width, H = el.offsetHeight || r.height, size = Math.min(W, H) || Math.min(r.width, r.height), k = zoomScale(size, zoomCap(el));
      const p = zoomPlan(el, k, r, W, H);
      st.undo = zoomLift(el, p.open, k);
      const to = { transform: `matrix(${k},0,0,${k},${base[4] + p.dx},${base[5] + p.dy})`, filter: zoomShadow(size), opacity: "1" };
      const quick = reduced(), ms = quick ? 120 : ZOOM_GROW, ease = quick ? "ease-out" : ZOOM_EASE;
      st.anim = el.animate ? el.animate([from, to], { duration: ms, easing: ease, fill: "forwards" }) : null;
      const pp = paperOf(el); st.paper = pp && pp.animate ? pp.animate([{ opacity: pfrom }, { opacity: 1 }], { duration: quick ? 100 : 220, delay: quick ? 0 : 30, easing: "ease-out", fill: "forwards" }) : null;
      Object.assign(st, { el, on: true, k, rect: p.rect, home: { l: r.left, t: r.top, r: r.right, b: r.bottom }, keyboard: !!o.keyboard, touch: !!o.touch, managed: !!o.managed });
      ZS.set(el, st); ACT.add(st); Zm.cur = st; el.dataset.sealZoom = k.toFixed(2);
      if (!Zm.watch) Zm.watch = setInterval(zoomWatch, 250);
      return true;
    }
    /** Puts the seal back (now: at once; else the way it came, from where it is). */
    function zoomHide(now, el) {
      const st = el ? ZS.get(el) : Zm.cur; if (!st) return;
      const e = st.el, done = () => { if (st.on || ZS.get(e) !== st) return; st.anim && st.anim.cancel(); st.paper && st.paper.cancel(); if (st.undo) st.undo(); st.undo = null; delete e.dataset.sealZoom; ZS.delete(e); ACT.delete(st); };
      if (!st.on) { if (now) done(); return; }
      st.on = false; if (Zm.cur === st) Zm.cur = null;
      if (!Zm.cur && Zm.watch) { clearInterval(Zm.watch); Zm.watch = 0; }
      if (now || !e.isConnected || !e.animate) { done(); return; }
      const cs = getComputedStyle(e), from = { transform: cs.transform, filter: cs.filter, opacity: cs.opacity }, pp = paperOf(e), pfrom = pp ? getComputedStyle(pp).opacity : "0";
      st.anim && st.anim.cancel(); st.paper && st.paper.cancel();
      const bs = getComputedStyle(e), to = { transform: bs.transform, filter: bs.filter, opacity: bs.opacity }, quick = reduced();
      const a = st.anim = e.animate([from, to], { duration: quick ? 100 : ZOOM_BACK, easing: quick ? "ease-out" : ZOOM_BACK_EASE });
      st.paper = pp && pp.animate ? pp.animate([{ opacity: pfrom }, { opacity: 0 }], { duration: quick ? 80 : 160, easing: "ease-in", fill: "forwards" }) : null;
      a.finished.then(done, () => {});
    }
    function zoomWatch() {
      const st = Zm.cur; if (!st) return;
      const s = st.el;
      if (!canZoom(s) || !visible(s)) return zoomHide(true);
      if (!st.keyboard && !st.touch && !st.managed && !pointed(s) && !atHome(st, Zm.point)) zoomHide(false);
    }
    function cancelWant() { clearTimeout(Zm.t); Zm.t = 0; Zm.want = null; }
    const sealAt = t => t && t.closest ? t.closest(".seal, .laserSeal") : null;
    /** Any seal on the page that is drawn and ready (a pending one, still waiting for its stamp, is not). */
    const canZoom = s => !!(s && s.isConnected && !s.classList.contains("pending") && !s.closest(".mGhost,[inert]") && s.getClientRects().length);
    const zoomable = s => canZoom(s) && s.matches(".seal, .laserSeal");
    /** The pointer is still where the seal stood (a seal nudged well away from a screen edge may no longer cover that spot: the pointer
     *  there is still resting on it, and taking the zoom back would only bring it round again). */
    const atHome = (st, p) => !!(st && st.home && p && p.x >= st.home.l && p.x <= st.home.r && p.y >= st.home.t && p.y <= st.home.b);
    function pointed(s) {
      if (!Zm.point || typeof doc.elementFromPoint !== "function") return Zm.hover;
      const t = doc.elementFromPoint(Zm.point.x, Zm.point.y); return !!t && (t === s || s.contains(t));
    }
    doc.addEventListener("pointerover", e => {
      if (e.pointerType === "touch") return;
      const s = sealAt(e.target); if (!zoomable(s)) return;
      Zm.point = { x: e.clientX, y: e.clientY };
      if (Zm.cur && Zm.cur.el === s) { Zm.hover = true; Zm.cur.keyboard = false; return; }
      if (Zm.want === s || Zm.off === s) return;   // moving between SVG descendants must not restart the wait
      zoomHide(false); cancelWant(); Zm.hover = true; Zm.want = s;
      Zm.t = setTimeout(() => { Zm.t = 0; if (Zm.hover && Zm.want === s && zoomable(s) && pointed(s)) zoomShow(s); else cancelWant(); }, ZOOM_DELAY);
    }, true);
    doc.addEventListener("pointerout", e => {
      if (e.pointerType === "touch") return;
      const from = sealAt(e.target); if (!from || from.contains(e.relatedTarget)) return;
      if (from === Zm.off) Zm.off = null;
      if (from === Zm.want) cancelWant();
      if (Zm.cur && Zm.cur.el === from && !atHome(Zm.cur, { x: e.clientX, y: e.clientY })) zoomHide(false);
    }, true);
    doc.addEventListener("pointermove", e => {
      if (e.pointerType === "touch") return;
      Zm.point = { x: e.clientX, y: e.clientY };
      const s = sealAt(e.target);
      if (Zm.want && s !== Zm.want) cancelWant();
      if (Zm.cur && !Zm.cur.touch && !Zm.cur.managed && s !== Zm.cur.el) { if (!atHome(Zm.cur, Zm.point)) zoomHide(false); }
      else if (Zm.cur && s === Zm.cur.el) { Zm.cur.keyboard = false; Zm.hover = true; }
    }, true);
    // a tap or a press elsewhere puts a zoomed seal back
    // (a zoom its owner manages, the timeline's, is put back by that owner: it knows which of its own dots a press is on)
    doc.addEventListener("pointerdown", e => { if (Zm.cur && Zm.cur.managed) return; if ((Zm.cur || Zm.want) && sealAt(e.target) !== (Zm.cur ? Zm.cur.el : Zm.want)) { zoomHide(false); cancelWant(); } }, true);
    // the keyboard: Tab onto a seal zooms it at once; Esc (and only that: a window it lies in stays open) puts it back
    doc.addEventListener("focusin", e => { const s = sealAt(e.target); if (s && s === e.target && zoomable(s) && s.matches(":focus-visible")) zoomShow(s, { keyboard: true }); }, true);
    doc.addEventListener("focusout", e => { const s = sealAt(e.target); if (s && Zm.cur && s === Zm.cur.el && !Zm.hover && !(e.relatedTarget && zoomable(sealAt(e.relatedTarget)))) zoomHide(false); }, true);
    root.addEventListener("keydown", e => { if (e.key === "Escape" && (Zm.cur || Zm.want) && !(Zm.cur && Zm.cur.managed)) { e.preventDefault(); e.stopImmediatePropagation(); cancelWant(); zoomHide(false); } }, true);
    // a click: on a seal over its button it presses the button; elsewhere it zooms the seal at once, no waiting for the rest, and a second
    // click (or tap) on it, once it was already grown when the press began, puts it back (Zm.pre: a zoom that only just came by resting
    // is not "second"). A press that comes from the keyboard (detail 0) never toggles.
    doc.addEventListener("click", e => {
      const s = sealAt(e.target); if (!s) return;
      const row = s.closest && s.closest(".sealRow .seal") === s;
      if (row) {
        const b = btnOf(s);
        e.stopPropagation(); e.preventDefault();
        if (b && !b.disabled && inside(b, e)) { Zm.pre = null; if (!(Zm.cur && Zm.cur.el === s)) { cancelWant(); Zm.off = s; } b.click(); return; }   // (the button answers: no zoom comes late on top of its answer)
      } else if (!zoomable(s)) return;
      const touch = e.pointerType === "touch" || (e.pointerType == null && Zm.lastTouch), was = Zm.pre === s; Zm.pre = null;
      if (!touch && e.detail) { Zm.point = { x: e.clientX, y: e.clientY }; Zm.hover = true; }
      if ((touch || e.detail) && was && Zm.cur && Zm.cur.el === s) { zoomHide(false); if (!touch) Zm.off = s; return; }
      if (zoomable(s)) zoomShow(s, { touch: !!touch });
    }, true);
    doc.addEventListener("pointerdown", e => { Zm.lastTouch = e.pointerType === "touch"; const s = sealAt(e.target); Zm.pre = s && Zm.cur && Zm.cur.on && Zm.cur.el === s ? s : null; }, true);
    // Enter or Space on a seal that has the focus grows it, or puts it back
    doc.addEventListener("keydown", e => {
      const s = (e.key === "Enter" || e.key === " ") && sealAt(e.target); if (!s || s !== e.target || !zoomable(s) || s.closest("button, a")) return;
      e.preventDefault(); e.stopPropagation();
      if (Zm.cur && Zm.cur.el === s) zoomHide(false); else zoomShow(s, { keyboard: true });
    }, true);
    const zoomAway = () => { cancelWant(); for (const st of [...ACT]) zoomHide(true, st.el); };
    root.addEventListener("scroll", () => {
      const st = Zm.cur;
      if (st && st.keyboard) { const el = st.el; zoomHide(true); requestAnimationFrame(() => { if (doc.activeElement === el) zoomShow(el, { keyboard: true }); }); return; }   // (its place moved: worked out again)
      zoomAway();
    }, true);
    root.addEventListener("resize", zoomAway);
    root.addEventListener("blur", zoomAway);
    doc.addEventListener("visibilitychange", () => { if (doc.visibilityState === "hidden") zoomAway(); });
    doc.addEventListener("close", e => { if (e.target && e.target.contains && (e.target.contains(Zm.cur && Zm.cur.el) || e.target.contains(Zm.want))) zoomAway(); }, true);
    const zoom = {
      DELAY: ZOOM_DELAY, scale: zoomScale, cap: zoomCap, show: zoomShow, hide: zoomHide, away: zoomAway,
      get current() { return Zm.cur ? Zm.cur.el : null; },
      /** Where the seal is, or will be once zoomed (the page rectangle that tooltips and cards stay clear of). */
      rectOf(el) { const st = ZS.get(el); return st && st.on ? Object.assign({}, st.rect) : null; },
      check() { if (Zm.want && !Zm.want.isConnected) cancelWant(); for (const st of [...ACT]) if (!st.el.isConnected) zoomHide(true, st.el); }
    };
    return { list, html, row, svg, face, modelOf, tool, sound:Sound, stampOn, press, pressPending, hasPrint, titleOf, INK, FAMILY, BASE_SIZE, zoom, zoomScale, busy, whenIdle, defer, fit, fitGroups };
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
  /* Each of the page's style sheets is copied once, as its own sheet, and kept while it is unchanged (by its rule
     count): a sheet the page adds later (a tour's, a window's own, put in on first use) costs only its own few rules at
     the next close, never the whole page's again (that rebuild, 45-60 ms of copying and compiling every rule of the
     page, stood at the start of the designs window's way back when the Send to Sheet tour had just put its styles in). */
  const copies = new WeakMap();
  let sheetKey = "", sheetList = null;
  function copyOf(s) {
    let n; try { if (s.disabled) return null; n = s.cssRules.length; } catch (_) { return null; }
    const had = copies.get(s); if (had && had.n === n) return had.sheet;
    try {
      const body = [...s.cssRules].map(r => r.cssText).filter(t => !/^@(import|charset)/i.test(t)).join("\n"), m = s.media && s.media.mediaText;
      const sh = new CSSStyleSheet(); sh.replaceSync(m && m !== "all" ? `@media ${m}{${body}}` : body);
      copies.set(s, { n, sheet: sh }); return sh;
    } catch (_) { return null; }
  }
  function pageSheet() {
    try {
      if (!("adoptedStyleSheets" in doc) || !root.CSSStyleSheet) return null;
      const list = [...doc.styleSheets]; let key = String(list.length);
      for (const s of list) { try { key += "," + (s.disabled ? "d" : s.cssRules.length); } catch (_) { key += ",x"; } }
      if (sheetList && key === sheetKey) return sheetList;
      sheetList = list.map(copyOf).filter(Boolean); sheetKey = key; return sheetList;
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
        const sr = host.attachShadow({ mode: "closed" }); sr.adoptedStyleSheets = sh; sr.innerHTML = '<div class="dlg"><div class="dlgHead"></div></div>';
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
      const sr = host.attachShadow({ mode: "closed" }), list = snap.copy ? pageSheet() : null, sheet = list && list.length ? list : null; if (sheet) sr.adoptedStyleSheets = sheet;
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
    if(Seal.busy())return Seal.whenIdle().then(()=>dialogClose(dlg,opts));
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
      const args=[...arguments];if(Seal.defer(this,()=>this.showModal(...args)))return;
      if (!this.open) this._mdSeq = ++seq;
      if (this.open || skips(this)) return nativeShow.apply(this, arguments);
      const f = this._mdFrom; this._mdFrom = null; this._mdClosing = false; this._mdSnap = null;
      if (!reduced()) this.classList.add("mdIn");
      try { nativeShow.apply(this, arguments); } catch (e) { this.classList.remove("mdIn"); throw e; }
      this._mdShown = dialogOpen(this, f);
    };
    P.close = function (rv) {
      const args=[...arguments];if(Seal.defer(this,()=>this.close(...args)))return;
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
    doc.addEventListener("cancel", e => {if(Seal.busy()){e.preventDefault();return;}early(e.target);}, true);
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

  root.Motion = { sealZoomScale: Seal.zoomScale, T, ghost, fly, flyIn, grow, shut, fade, arrive, pulse, note, expect, expectIn, pending, carry, carrying: isCarried, reconcile, reduced, wait, layer, dialogOpen, dialogClose, from, popIn, turner, landIn };
  root.Seal = Seal;
})(typeof window !== "undefined" ? window : globalThis);
