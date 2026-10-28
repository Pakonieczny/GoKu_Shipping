/* charm-nest-custom-piece.js — SEND TO SHEET: a design from the Charm Designer put on the next sheet of the metal a person picks, marked handmade
 * custom, for an order or for none (Design & Engrave, items 17, 22, 24, 25, 26, 28, 29; the lead's D4, D5, D7).
 *   (Paul, 28 Oct 2026: "Users also need the ability to search and open any charm existing in our charm repository and have full control over
 *   manipulating that charm, and then send that charm to the next given sheet color, chosen by the user and market as handmade custom. Also custom
 *   orders that need a custom charm design now must all be able to be opened in the charm design designer ... save it and then that would get
 *   attached to the Custom order and that design would be sent to the next available sheet and the colour of the sheet would have to be decided by
 *   the user at that same time so you're gonna have to have some sort of a sophisticated pop-up modal that presents the user with all of these
 *   options." · "sometimes the user just needs to add a charm onto a sheet and that custom order can be named by the user so the user can easily
 *   identify that extra piece that was added, and not linked to an official order." · "I need to know when employees are commissioning custom
 *   pieces, especially when they don't belong to an order ... who creates the errors and who is creating the solutions")
 *
 *   CharmNestCustomPiece.open({ designId, version, orderId?, poolId?, design? }) -> Promise<{ poolIds, orderId, sendId } | null>
 *       THE pop-up: the design and its version drawn at its exact size with its mm, what it is attached to (this custom order, another order found by
 *       its number or its buyer, or no order: a name and a reason; a fix names the order or piece it fixes and who made the original mistake, read
 *       from that order's history and changeable), the metal with the sheet it goes onto, how many, always "handmade custom". Confirm → send().
 *       orderId: opened from a custom order (attached to it, prefilled). poolId: the piece this one fixes. design: the design already in hand (no read).
 *       null when it is put away.
 *   CharmNestCustomPiece.send({ designId, version, metal, quantity, orderId|null, customName, reason?, fixes: { orderId, poolId, lineKey, causedBy,
 *       suggested } | null, by, design? }) -> Promise<{ poolIds, orderId, sendId }>
 *       The pieces go on through Pool.addCustomPiece (the custompool workstream: the pool, the nest, the files, the frozen design); ONE
 *       CNDesignLog "customPieceSent" event per send names who sent it and, for a fix, who made the mistake (charm-nest-design-log.js writes the
 *       order's timeline, the efficiency count and the Design_Log record from it). An orderless piece's CUSTOM-<yyyymmdd>-<n> is given by the server.
 *   CharmNestCustomPiece.openDesigner(item | row) -> Promise
 *       A custom order in the Charm Designer: opened on the design already attached to it (its latest version), else on the .ai / .dxf dropped on its
 *       card, else blank; its Save keeps the design in the design library as a new version attached to the order (CharmNestDesigns.put, orderId)
 *       and offers the pop-up prefilled with that order.
 *   Hooks the bridge calls (each one line there): wire(node, item) on a Custom Orders card (the "Open in Charm Designer" button in its column, and the
 *   design attached to the order across its foot); orderWin(row, at) in the order window's header. In the Charm Editor: the "Send to sheet…" command
 *   (CharmEditor.commands.sendToSheet) and a button beside Save (CharmEditor.onOpen).
 *   attachedOf(orderId) -> [{ id, version, name, widthMm, heightMm, hash, at, by, sends }]: the designs attached to an order, newest first.
 *
 * What it reads, never asks the cloud for: the order lines this page holds (B.orders), the order search's index (OrderSearch.query), the sheets of
 * the run (pagesOf, LiveNest.closed) and the design library's own local index (CharmNestDesigns.search). It asks the cloud for a design only when one
 * is not in hand (CharmNestDesigns.get) and for an order's history only when a person picks the order a fix fixes (CNDesignLog.causeOf: one read).
 * It writes nothing itself: the design library and the pool do, under the signed-in person. In node (module.exports) the pure parts only, for tests. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestCustomPiece = api;
})(typeof self !== 'undefined' ? self : typeof globalThis !== 'undefined' ? globalThis : this, function (root) {
  'use strict';
  const doc = root && root.document ? root.document : null;
  const MM = 25.4 / 72, PT = 72 / 25.4;
  // the five metals of the sheets, as the page names them (charm-nest-1.html METALS); the page's own colours are used when it has them
  const METALS = [
    { key: 'gold', code: 'GF', word: 'Gold filled', color: '#c8a24e' }, { key: 'silver', code: 'SS', word: 'Sterling silver', color: '#8d95a0' },
    { key: 'rose', code: 'RG', word: 'Rose gold filled', color: '#c08578' }, { key: 'gold10k', code: '10K', word: '10K solid gold', color: '#b08d2a' },
    { key: 'gold14k', code: '14K', word: '14K solid gold', color: '#d9b545' }];
  const REASONS = [['forgotten', 'Forgotten piece', 'a piece an order should have had'], ['fix', 'Fix a mistake', 'a piece made again because one was wrong'],
    ['sample', 'Sample', 'made to show or to try'], ['other', 'Other', 'say what it is in its name']];
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const fin = v => typeof v === 'number' && Number.isFinite(v);
  const metalOf = k => METALS.find(m => m.key === k || m.code === String(k || '').toUpperCase()) || null;
  const metalKey = k => (metalOf(k) || {}).key || '';
  const codeOf = k => (metalOf(k) || {}).code || String(k || '');
  const plural = (n, w) => `${n} ${w}${n === 1 ? '' : 's'}`;
  const now = () => (root.performance && root.performance.now ? root.performance.now() : Date.now());
  const ymd = (t = Date.now()) => { const d = new Date(t); return `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, '0')}${String(d.getDate()).padStart(2, '0')}`; };
  const when = t => { if (!(+t > 0)) return ''; const d = new Date(+t), today = new Date(); return d.toDateString() === today.toDateString() ? d.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' }) : d.toLocaleDateString([], { month: 'short', day: 'numeric' }) + ' ' + d.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' }); };
  const fnv = s => { let h = 2166136261; s = String(s); for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 16777619); } return (h >>> 0).toString(36); };
  /** The CUSTOM-<yyyymmdd>-<n> an orderless piece's pool id starts with (D4), or ''. */
  const customIdOf = id => { const m = /^(CUSTOM-\d{8}-\d{1,6})(?:_|$)/.exec(String(id || '')); return m ? m[1] : ''; };

  /* ═══ 1 · the page around it (every name guarded: in node, and in a page that does not have it yet) ═══ */
  // (the signed-in person: charm-nest-bridge.js CNEmployee, its one sign-in; nobody signed in: its bar asks under the button pressed, and the press goes on once signed in)
  const who = () => { try { const E = root.CNEmployee; return E && typeof E.name === 'function' ? String(E.name() || '').trim() : ''; } catch (_) { return ''; } };
  const askWho = (why, action) => { try { const E = root.CNEmployee; if (E && typeof E.need === 'function') return Promise.resolve(E.need(why, { action })).then(n => String(n || '').trim(), () => ''); } catch (_) {} return Promise.resolve(who()); };
  const say = (text, kind, ms) => { try { if (typeof toast === 'function') toast(text, kind || '', ms || 5000); } catch (_) {} };   // eslint-disable-line no-undef
  const pagesFor = metal => { try { return typeof pagesOf === 'function' ? (pagesOf(metal) || []) : []; } catch (_) { return []; } };   // eslint-disable-line no-undef
  const colourOf = k => { try { const L = root.CN && Array.isArray(root.CN.METALS) ? root.CN.METALS : null, m = L && L.find(x => x.key === k); if (m && m.color) return m.color; } catch (_) {} return (metalOf(k) || {}).color || ''; };
  const log = (kind, data) => { try { if (root.CNDesignLog && typeof root.CNDesignLog.event === 'function') root.CNDesignLog.event(kind, data); } catch (_) {} };
  const rowsAll = () => { try { return root.Orders && typeof root.Orders.rows === 'function' ? root.Orders.rows() : (root.B && root.B.orders && root.B.orders.rows) || []; } catch (_) { return []; } };
  const rowsOf = x => !x ? [] : x.order && x.line ? [x] : (x.rows && x.rows.length ? x.rows : x.row ? [x.row] : []).filter(r => r && r.order);
  const el = (tag, cls, html) => { const n = doc.createElement(tag); if (cls) n.className = cls; if (html != null) n.innerHTML = html; return n; };
  const reduced = () => { try { return !!(root.matchMedia && root.matchMedia('(prefers-reduced-motion: reduce)').matches); } catch (_) { return false; } };

  /* ═══ 2 · a design as drawn: its parts placed in the face (pt, y down), its exact size, its picture ═══ */
  const I = [1, 0, 0, 1, 0, 0];
  const isM = m => Array.isArray(m) && m.length === 6 && m.every(fin);
  const mul = (m, n) => [m[0] * n[0] + m[2] * n[1], m[1] * n[0] + m[3] * n[1], m[0] * n[2] + m[2] * n[3], m[1] * n[2] + m[3] * n[3], m[0] * n[4] + m[2] * n[5] + m[4], m[1] * n[4] + m[3] * n[5] + m[5]];
  const isI = m => Math.abs(m[0] - 1) < 1e-12 && Math.abs(m[1]) < 1e-12 && Math.abs(m[2]) < 1e-12 && Math.abs(m[3] - 1) < 1e-12 && Math.abs(m[4]) < 1e-9 && Math.abs(m[5]) < 1e-9;
  const ap = (m, p) => [m[0] * p[0] + m[2] * p[1] + m[4], m[1] * p[0] + m[3] * p[1] + m[5]];
  const xf = (subs, m) => subs.map(sp => sp.map(o => (o[0] === 'm' || o[0] === 'l' ? [o[0], ap(m, o[1])] : o[0] === 'c' ? ['c', ap(m, o[1]), ap(m, o[2]), ap(m, o[3])] : o.slice())));
  const hasXf = it => (fin(it.tx) && it.tx) || (fin(it.ty) && it.ty) || (fin(it.sx) && it.sx !== 1) || (fin(it.sy) && it.sy !== 1) || (fin(it.rot) && it.rot);
  const model = () => (root.CharmEditor && root.CharmEditor.model) || null;
  let geoMemo = null;
  const geo = () => { const M = model(); if (!M || typeof M.createGeo !== 'function') return null; return geoMemo || (geoMemo = M.createGeo(null)); };
  /** A path kept as SVG data (a store that cannot keep an array in an array): its subpaths back. */
  function fromD(d) {
    const tok = String(d || '').match(/[MLCZ]|[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?/g) || [], subs = []; let sp = null, i = 0;
    const n = () => +tok[i++], pt = () => [n(), n()];
    while (i < tok.length) { const c = tok[i++]; if (c === 'M') { sp = [['m', pt()]]; subs.push(sp); } else if (!sp) break; else if (c === 'L') sp.push(['l', pt()]); else if (c === 'C') sp.push(['c', pt(), pt(), pt()]); else if (c === 'Z') sp.push(['h']); }
    return subs;
  }
  function bboxOf(subs) {
    const M = model(); if (M && typeof M.bboxOf === 'function') return M.bboxOf(subs);      // (the tight box, curve extremes included: the editor's own)
    let b = null; const add = p => { b = b ? [Math.min(b[0], p[0]), Math.min(b[1], p[1]), Math.max(b[2], p[0]), Math.max(b[3], p[1])] : [p[0], p[1], p[0], p[1]]; };
    for (const sp of subs || []) for (const o of sp) for (const p of o.slice(1)) if (Array.isArray(p)) add(p);
    return b;
  }
  const union = (a, b) => (!a ? b : !b ? a : [Math.min(a[0], b[0]), Math.min(a[1], b[1]), Math.max(a[2], b[2]), Math.max(a[3], b[3])]);
  const f4 = v => Math.round(v * 1e4) / 1e4;
  const dOf = subs => { let d = ''; for (const sp of subs) for (const o of sp) { if (o[0] === 'm') d += 'M' + f4(o[1][0]) + ' ' + f4(o[1][1]); else if (o[0] === 'l') d += 'L' + f4(o[1][0]) + ' ' + f4(o[1][1]); else if (o[0] === 'c') d += 'C' + [o[1], o[2], o[3]].map(p => f4(p[0]) + ' ' + f4(p[1])).join(' '); else if (o[0] === 'h') d += 'Z'; } return d; };
  const faceOf = d => (d && d.faces && (d.faces.front || d.faces.back)) || null;
  /** Every visible part of a face, placed (the editor's result: a path or group with its `matrix`, a text with its outlines already placed). A part
   *  hidden in the editor is not engraved, so it is not drawn; a hidden cut line is still the piece's outline. → [{ role, subs, box, filled, grey, even }]
   *  On the page the design library's own reading (CharmNestDesigns.leaves: exactly the parts its hash covers and the laser file draws), so the picture is
   *  what will be cut; this file's own walk where the library is not loaded. A part is filled or stroked by the editor's rule (the library's paintFill). */
  const greyOf = f => (Array.isArray(f) && f.length >= 3 && Math.abs(f[0] - f[1]) < 0.02 && Math.abs(f[1] - f[2]) < 0.02 ? +f[0] / (f[0] > 1 ? 255 : 1) : null);
  const filledBy = (kind, role, fill, stroke) => (kind === 'text' ? !(role === 'line' || role === 'cut') : role === 'hatch' ? true : role === 'art' ? (!!fill || !stroke) : false);
  function leavesOf(face) {
    const D = root.CharmNestDesigns;
    if (D && typeof D.leaves === 'function' && face) {
      try {
        const L = D.leaves({ front: face }).front || [];
        return L.filter(l => l && Array.isArray(l.subpaths) && l.subpaths.length).map(l => { const role = ['cut', 'line', 'hatch', 'art'].includes(l.role) ? l.role : 'art';
          return { role, subs: l.subpaths, box: bboxOf(l.subpaths), filled: filledBy(l.kind, role, l.fill, l.stroke), grey: greyOf(l.fill), even: !!l.even }; });
      } catch (_) { /* this file's own walk */ }
    }
    const out = [], G = geo(), M = model();
    const own = it => (isM(it.matrix) ? it.matrix : G && hasXf(it) ? (() => { try { return G.matrix(it); } catch (_) { return null; } })() : null);
    const walk = (items, P, off) => {
      for (const it of items || []) {
        if (!it || typeof it !== 'object') continue;
        const hidden = off || !!it.hidden;
        if (it.kind === 'group') { const m = own(it); walk(it.children, m ? (M ? M.mul(P, m) : mul(P, m)) : P, hidden); continue; }
        const role = ['cut', 'line', 'hatch', 'art'].includes(it.role) ? it.role : 'art';
        if (hidden && role !== 'cut') continue;
        let subs = Array.isArray(it.subpaths) ? it.subpaths : typeof it.subpaths === 'string' ? fromD(it.subpaths) : typeof it.d === 'string' ? fromD(it.d) : [];
        subs = subs.filter(sp => Array.isArray(sp) && sp.length > 1);
        if (!subs.length) continue;
        if (it.kind !== 'text') { const m = own(it), W = m ? mul(P, m) : P; if (!isI(W)) subs = xf(subs, W); }
        out.push({ role, subs, box: bboxOf(subs), filled: filledBy(it.kind, role, it.fill, it.stroke), grey: greyOf(it.fill), even: !!(it.even || it.evenOdd || it.fillRule === 'evenodd') });
      }
    };
    walk(face && face.items, I, false);
    return out;
  }
  /** The exact size a design is cut at: its cut line's box (all of its ink when it has no cut line yet), in mm. → { wMm, hMm, box } | null */
  function sizeOf(design) {
    const L = leavesOf(faceOf(design)), cut = L.filter(l => l.role === 'cut');
    let b = null; for (const l of cut.length ? cut : L) b = union(b, l.box);
    return b ? { wMm: (b[2] - b[0]) * MM, hMm: (b[3] - b[1]) * MM, box: b } : null;
  }
  const hex = c => { const m = /^#?([0-9a-f]{6})$/i.exec(String(c || '')); if (!m) return [200, 200, 200]; const v = parseInt(m[1], 16); return [v >> 16 & 255, v >> 8 & 255, v & 255]; };
  const mix = (c, t, k) => { const a = hex(c), b = hex(t); return '#' + a.map((v, i) => Math.round(v + (b[i] - v) * k).toString(16).padStart(2, '0')).join(''); };
  let gid = 0;
  /** The plate's tones: light, the metal, deep, and its edge; no metal yet: white paper and the ink's edge. */
  const tones = mc => (mc ? [mix(mc, '#ffffff', 0.55), mc, mix(mc, '#000000', 0.18), mix(mc, '#000000', 0.42)] : ['#ffffff', '#fbfaf7', '#f1ede5', '#1c1a17']);
  /** A picture drawn by previewSvg put in another metal, in place (no path drawn again). */
  function tint(svg, metal) {
    if (!svg) return; const t = tones(metal ? colourOf(metal) : '');
    for (const st of svg.querySelectorAll('stop[data-t]')) st.setAttribute('stop-color', t[+st.dataset.t]);
    const cut = svg.querySelector('[data-cp-cut]'); if (cut) cut.setAttribute('stroke', t[3]);
  }
  /** The design drawn as an SVG at its EXACT size (CSS millimetres, so 1 mm on the screen is 1 mm of the piece at 100% zoom), on the metal it goes on
   *  when one is given: its cut outline filled with that metal, its engraving dark on it (each grey of a traced image kept, D2). o.fitMm [w, h]: the room
   *  it has; a design bigger than that is drawn smaller and says by how much (scale). o.px: a thumbnail of that many pixels instead.
   *  → { svg, wMm, hMm (the drawing with its margin), scale } | null (nothing drawn yet) */
  function previewSvg(design, o) {
    o = o || {};
    const L = leavesOf(faceOf(design)); let b = null; for (const l of L) b = union(b, l.box);
    if (!b) return null;
    const pad = (o.padMm != null ? o.padMm : 1.2) * PT, vb = [b[0] - pad, b[1] - pad, b[2] - b[0] + 2 * pad, b[3] - b[1] + 2 * pad];
    const wMm = vb[2] * MM, hMm = vb[3] * MM;
    let scale = 1, w, h;
    if (o.px) { const k = o.px / Math.max(vb[2], vb[3]); w = Math.max(1, Math.round(vb[2] * k)) + 'px'; h = Math.max(1, Math.round(vb[3] * k)) + 'px'; scale = 0; }
    else { const fit = o.fitMm || [58, 46]; scale = Math.min(1, fit[0] / wMm, fit[1] / hMm); w = +(wMm * scale).toFixed(3) + 'mm'; h = +(hMm * scale).toFixed(3) + 'mm'; }
    const mc = o.metal ? colourOf(o.metal) : '', id = 'cpG' + (++gid), cut = L.filter(l => l.role === 'cut'), ink = L.filter(l => l.role !== 'cut'), INK = '#2b2620';
    let defs = '', body = '';
    if (cut.length) {   // (the body in the metal: three stops and an edge, recoloured in place when another metal is picked, tint)
      const t = tones(mc);
      defs = `<defs><linearGradient id="${id}" x1="0" y1="0" x2="1" y2="1"><stop offset="0" data-t="0" stop-color="${t[0]}"/><stop offset=".55" data-t="1" stop-color="${t[1]}"/><stop offset="1" data-t="2" stop-color="${t[2]}"/></linearGradient></defs>`;
      body += `<path data-cp-cut d="${dOf([].concat(...cut.map(l => l.subs)))}" fill="url(#${id})" fill-rule="evenodd" stroke="${t[3]}" stroke-width="1" vector-effect="non-scaling-stroke"/>`;
    }
    for (const l of ink) {
      const op = l.grey == null ? 0.9 : Math.max(0.08, 1 - l.grey);
      body += l.filled ? `<path d="${dOf(l.subs)}" fill="${INK}" fill-opacity="${+op.toFixed(3)}" fill-rule="${l.even ? 'evenodd' : 'nonzero'}"/>`
        : `<path d="${dOf(l.subs)}" fill="none" stroke="${INK}" stroke-opacity="${+op.toFixed(3)}" stroke-width="1" vector-effect="non-scaling-stroke" stroke-linecap="round" stroke-linejoin="round"/>`;
    }
    const svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="${vb.map(f4).join(' ')}" width="${w}" height="${h}" role="img" aria-label="${esc((design && design.name) || 'design')}">${defs}${body}</svg>`;
    return { svg, wMm, hMm, scale };
  }

  /* ═══ 3 · where the piece goes: the next open sheet of its metal (D5), named as every surface names it ═══ */
  /** The sheet a custom piece of `metal` goes onto in the run, as the intake picks one (LiveNest.intakePage, read only: nothing is made here): the
   *  order's own sheet of that metal when the order already has pieces on one; for Gold filled and Sterling silver the earliest open sheet with room;
   *  for the others the newest open one. null: none takes it, a new sheet is started. */
  function pickSheet(list, o) {
    o = o || {};
    const closed = o.closed || (p => !!(p.laserDoneAt || p.roseCutAt || p.recalled || p.releaseFull || p.runHold || p.intakeFinalized));
    const room = o.room || (() => true);
    const open = p => !!p && Array.isArray(p.charms) && p.charms.length > 0 && !closed(p) && !p.laserDoneAt && !p.roseCutAt && !p.recalled && (!o.run || !o.run.runId || !p.runId || p.runId === o.run.runId);
    const pages = (list || []).filter(Boolean);
    if (o.orderId) { const home = pages.find(p => open(p) && p.charms.some(c => String(c.order) === String(o.orderId))); if (home) return home; }
    const fast = o.metal === 'gold' || o.metal === 'silver';
    return (fast ? pages : pages.slice().reverse()).find(p => open(p) && room(p)) || null;
  }
  function sheetName(p, pages, metal) {
    const N = root.CharmNestSheetName, i = (pages || []).indexOf(p), rec = i >= 0 && +p.page !== i + 1 ? Object.assign({}, p, { page: i + 1 }) : p;
    try { if (N && typeof N.name === 'function') { const n = N.name(rec); if (n) return n; } } catch (_) {}
    return `${codeOf(metal || p.metal)} Sheet ${p.sheetIndex || p.page || i + 1}`;
  }
  /** → { name ("SS Sheet 4" | "a new SS sheet"), fresh, sheetId } */
  function goesOnto(metal, orderId) {
    const code = codeOf(metal), fresh = { name: `a new ${code} sheet`, fresh: true, sheetId: null };
    if (!metalOf(metal)) return { name: '', fresh: false, sheetId: null };
    try {   // (custompool's own answer, when the pool has one: it is the one that places the piece)
      const P = root.Pool; if (P && typeof P.customTarget === 'function') { const t = P.customTarget(metal, { orderId: orderId || null }); if (t !== undefined) return t ? { name: sheetName(t, pagesFor(metal), metal), fresh: false, sheetId: t.sheetId || null } : fresh; }
    } catch (_) { /* read from the sheets below */ }
    const pages = pagesFor(metal), LN = root.LiveNest, CNx = root.CN;
    const t = pickSheet(pages, { metal, orderId, run: (root.B && root.B.run) || null,
      closed: LN && typeof LN.closed === 'function' ? p => { try { return LN.closed(p); } catch (_) { return true; } } : undefined,
      room: CNx && typeof CNx.topupRoom === 'function' ? p => { try { return CNx.topupRoom(p) > 0; } catch (_) { return true; } } : undefined });
    return t ? { name: sheetName(t, pages, metal), fresh: false, sheetId: t.sheetId || null } : fresh;
  }

  /* ═══ 4 · who made the original mistake (D7): the order's own history ═══ */
  // (charm-nest-design-log.js causeOf answers it on the page; this is the same reading for a page without that file, and for the node test)
  const STEP = [['engraveApproved', 'approved the engraving'], ['monogramApproved', 'approved the monogram'], ['engraveChanged', 'changed the engraving'], ['designSent', 'sent the design to the sheet'],
    ['designDropped', 'added the design'], ['customDecided', 'decided the custom order'], ['decided', 'approved the order'], ['placed', 'placed it on the sheet'], ['laserDone', 'cut it on the laser'],
    ['sorted', 'sorted it'], ['welded', 'welded it'], ['assembled', 'assembled it'], ['packed', 'packed it']];
  const NOBODY = /^(|system|etsy|operator|someone|unknown|unattributed|charm sorter|sorter|auto|automatic|claude)$/i;
  /** events: the order's timeline. → { suggested, why, candidates: [{ name, step, type, at }] }: the person of the step a mistake is most likely made
   *  at, latest first within a step (the engraving or the design a person approved, before placing and cutting). o.lineKey: that piece's steps only. */
  function culprits(events, o) {
    o = o || {};
    const rank = new Map(STEP.map(([t], i) => [t, i])), said = new Map(STEP);
    const name = v => { const s = String(v == null ? '' : v).trim(); return /\p{L}/u.test(s) && !NOBODY.test(s) ? s.slice(0, 80) : ''; };
    const evs = (events || []).filter(e => e && rank.has(e.type) && name(e.by) && !(e.data && e.data.fix) && (!o.lineKey || !e.lineKey || e.lineKey === o.lineKey))
      .sort((a, b) => rank.get(a.type) - rank.get(b.type) || (+b.at || 0) - (+a.at || 0));
    const seen = new Set(), candidates = [];
    for (const e of evs) { const k = name(e.by) + '|' + e.type; if (seen.has(k)) continue; seen.add(k); candidates.push({ name: name(e.by), step: said.get(e.type), type: e.type, at: +e.at || 0 }); }
    const top = candidates[0];
    return { suggested: top ? top.name : '', why: top ? `${top.name} ${top.step}` : '', candidates: candidates.slice(0, 12) };
  }
  const causes = new Map();   // order id + piece -> Promise of its reading (one read of an order's history a session)
  function causeOf(orderId, o) {
    o = o || {}; const k = String(orderId) + '|' + (o.lineKey || '');
    if (causes.has(k)) return causes.get(k);
    const L = root.CNDesignLog, T = root.OrderTimeline;
    const p = L && typeof L.causeOf === 'function' ? Promise.resolve(L.causeOf(orderId, { lineKey: o.lineKey || undefined }))
      : T && typeof T.get === 'function' ? Promise.resolve(T.get(String(orderId))).then(j => culprits(j && j.events, o)) : Promise.resolve({ suggested: '', why: '', candidates: [] });
    causes.set(k, p); p.catch(() => causes.delete(k));
    return p;
  }

  /* ═══ 5 · the designs: kept as read, and which order each is attached to (CharmNestDesigns, the designs workstream) ═══ */
  const designs = new Map();      // "id@version" -> the design
  const attached = new Map();     // order id -> Map(design id -> its summary): saved or sent from this page
  let idxVer = 0, idxMemo = new Map(), subscribed = false;
  const dkey = (id, v) => `${id}@${+v || 0}`;
  function remember(d) { if (d && d.id) designs.set(dkey(d.id, d.version), d); return d; }
  async function designOf(id, version, given, o) {
    if (given && given.faces) return remember(Object.assign({ id, version }, given));
    const hit = designs.get(dkey(id, version)); if (hit && !(o && o.open)) return hit;
    const D = root.CharmNestDesigns; if (!D || typeof D.get !== 'function') throw new Error('the design library is not on this page — reload it');
    const r = await D.get(id, version, o && o.open ? { open: true } : undefined), d = r && r.design ? r.design : r;   // (open: the library logs it opened, designOpen)
    if (!d || !d.faces) throw new Error('that design could not be read');
    return remember(Object.assign({ id, version }, d));
  }
  function subscribe() {
    if (subscribed) return; const D = root.CharmNestDesigns; if (!D || typeof D.onChange !== 'function') return;
    subscribed = true; try { D.onChange(() => { idxVer++; idxMemo = new Map(); for (const rid of mounted.keys()) repaint(rid); }); } catch (_) {}
  }
  /** The designs the library's local index has for an order (no read: its own cached index). */
  function indexed(rid) {
    const D = root.CharmNestDesigns; if (!D || typeof D.search !== 'function') return [];
    subscribe();
    const m = idxMemo.get(rid); if (m && m.v === idxVer) return m.list;
    let list = [];
    try {
      const r = D.search(rid);
      if (r && typeof r.then === 'function') { idxMemo.set(rid, { v: idxVer, list: [] }); r.then(x => { idxMemo.set(rid, { v: idxVer, list: pickOrder(x, rid) }); repaint(rid); }, () => {}); return []; }
      list = pickOrder(r, rid);
    } catch (_) { list = []; }
    idxMemo.set(rid, { v: idxVer, list });
    return list;
  }
  const pickOrder = (r, rid) => (Array.isArray(r) ? r : r && Array.isArray(r.designs) ? r.designs : []).filter(d => d && d.id && String(d.orderId || '') === rid);
  function summaryOf(d, extra) {
    const sz = d.widthMm ? { wMm: +d.widthMm, hMm: +d.heightMm } : sizeOf(d) || { wMm: 0, hMm: 0 }, pic = d.faces ? previewSvg(d, { px: 38 }) : null;
    return Object.assign({ id: d.id, version: +d.version || 0, name: d.name || 'Custom design', widthMm: sz.wMm, heightMm: sz.hMm, hash: d.hash || '', at: +d.at || Date.now(), by: d.by || '', svg: pic ? pic.svg : '', thumbUrl: d.thumbUrl || '', sends: [] }, extra || {});
  }
  function attach(rid, d, extra) {
    rid = String(rid || ''); if (!rid || !d || !d.id) return null;
    const m = attached.get(rid) || attached.set(rid, new Map()).get(rid), had = m.get(d.id);
    const s = summaryOf(d, extra); if (had) { s.sends = had.sends; if ((+had.version || 0) > s.version) return had; }
    m.set(d.id, s); repaint(rid); return s;
  }
  /** The designs attached to an order, newest first: those saved or sent here, and those the library's index says (each at its latest version). */
  function attachedOf(rid) {
    rid = String(rid || ''); if (!rid) return [];
    const out = new Map(attached.get(rid) || []);
    for (const d of indexed(rid)) { const had = out.get(d.id); if (!had || (+d.version || 0) > (+had.version || 0)) out.set(d.id, Object.assign(summaryOf(d), had ? { sends: had.sends } : {})); }
    return [...out.values()].sort((a, b) => (+b.at || 0) - (+a.at || 0));
  }

  /* ═══ 6 · the send ═══ */
  async function send(o) {
    o = o || {};
    const metal = metalKey(o.metal); if (!metal) throw new Error('pick the metal it is cut in');
    const quantity = Math.floor(+o.quantity || 1); if (!(quantity >= 1 && quantity <= 99)) throw new Error('how many: 1 to 99');
    const orderId = o.orderId ? String(o.orderId) : null, customName = String(o.customName || '').replace(/\s+/g, ' ').trim().slice(0, 60);
    if (!orderId && !customName) throw new Error('a piece with no order needs a name, so it is easy to pick out');
    const by = String(o.by || who() || '').trim(); if (!by) throw new Error('nobody is signed in');
    const design = await designOf(o.designId, o.version, o.design);
    const P = root.Pool; if (!P || typeof P.addCustomPiece !== 'function') throw new Error('this page cannot put custom pieces on a sheet yet — reload it');
    const fx = o.fixes && (o.fixes.orderId || o.fixes.poolId) ? { orderId: o.fixes.orderId ? String(o.fixes.orderId) : null, poolId: o.fixes.poolId || null, lineKey: o.fixes.lineKey || null,
      causedBy: String(o.fixes.causedBy || '').trim().slice(0, 80) || null, suggested: String(o.fixes.suggested || '').trim().slice(0, 80) || null } : null;
    const sendId = 'cp-' + Date.now().toString(36) + Math.random().toString(36).slice(2, 7);
    const res = await P.addCustomPiece({ design, designId: design.id, version: design.version, hash: design.hash || null, metal, orderId, customName: customName || null, quantity, by,
      reason: o.reason || null, fixes: fx, handmade: true, sendId });
    const poolIds = (Array.isArray(res) ? res : res && Array.isArray(res.poolIds) ? res.poolIds : []).map(String);
    const madeId = (res && !Array.isArray(res) && (res.orderId || res.customId)) || orderId || customIdOf(poolIds[0]) || null;
    // ONE event for the send (charm-nest-design-log.js: the order's timeline, the efficiency count, the Design_Log record; a fix's mistake is put on the order it fixes)
    log('customPieceSent', { orderId: madeId || '', customName: customName || undefined, designId: design.id, version: design.version, name: design.name, metal, quantity, poolIds, sendId,
      reason: o.reason ? (REASONS.find(r => r[0] === o.reason) || [0, o.reason])[1] : undefined, fixes: fx || undefined, by });
    if (orderId) { const s = attach(orderId, design); if (s) { s.sends.push({ metal, quantity, sheet: o.sheet || '', at: Date.now(), by, poolIds }); repaint(orderId); } }
    return { poolIds, orderId: madeId, sendId };
  }

  /* ═══ 7 · the pop-up ═══ */
  const NIB = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M12 3.5 6 12l2.8 6.5h6.4L18 12Z"/><path d="M12 3.5V11"/><circle cx="12" cy="12.6" r="1.5"/><path d="M8.8 21h6.4"/></svg>';   // (the editor's own pen nib, charm-nest-bridge.js CHARM_EDIT_ICON)
  const ARROW = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M5 12h13"/><path d="m13 6 6 6-6 6"/></svg>';
  function css() {
    if (!doc || doc.getElementById('cpCss')) return;
    const s = doc.createElement('style'); s.id = 'cpCss';
    // (the app's own tokens and dialog (charm-nest-1.html: dialog, .dlgHead, .dlgFoot, .btn); the metal chips and the count are the designs window's, .cuM and .cuQty; plum is the
    //  colour custom pieces wear on the sheets, so everything here that says "custom" wears it)
    s.textContent = [
      'dialog.cpDlg{width:min(780px,95vw)}dialog.cpDlg[open]{animation:cpIn .22s cubic-bezier(.2,.8,.2,1)}@keyframes cpIn{from{opacity:0;transform:translateY(10px) scale(.985)}}',
      '.cpDlg .dlgHead{align-items:flex-start}.cpHd{display:grid;gap:3px;min-width:0}.cpHd .sub{font:12px var(--sans);color:var(--ink45);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}',
      '.cpTag{display:inline-flex;align-items:center;gap:6px;padding:4px 10px;border-radius:999px;background:#f5eefc;border:1px solid rgba(125,86,168,.35);color:#5d3f82;font:700 10px var(--sans);letter-spacing:.08em;text-transform:uppercase;white-space:nowrap}.cpTag i{width:7px;height:7px;border-radius:50%;background:#8e6bb8}',
      '.cpX{border:0;background:none;width:30px;height:30px;border-radius:50%;color:var(--ink45);font:400 21px/1 var(--sans);cursor:pointer}.cpX:hover{background:var(--paper2);color:var(--ink)}',
      '.cpBody{display:grid;grid-template-columns:minmax(230px,262px) minmax(0,1fr);overflow:auto;min-height:0;flex:1 1 auto}',
      '.cpLeft{padding:16px 18px;border-right:1px solid var(--line2);background:var(--card2);display:grid;align-content:start;gap:12px}',
      '.cpPlate{position:relative;display:grid;place-items:center;height:212px;border-radius:12px;border:1px solid var(--line);overflow:hidden;background-color:#fff;background-image:linear-gradient(rgba(28,26,23,.055) 1px,transparent 1px),linear-gradient(90deg,rgba(28,26,23,.055) 1px,transparent 1px);background-position:center}',
      '.cpPlate svg{display:block;filter:drop-shadow(0 1.5px 2px rgba(30,24,16,.18));transition:transform .2s ease}.cpPlate .cpSk{width:46%;height:46%;border-radius:12px;background:linear-gradient(90deg,var(--paper2),var(--card2),var(--paper2));background-size:200% 100%;animation:cpSk 1.2s linear infinite}@keyframes cpSk{to{background-position:-200% 0}}',
      '.cpScale{position:absolute;left:10px;bottom:9px;display:flex;align-items:center;gap:6px;font:600 10px var(--mono);color:var(--ink70)}.cpScale i{display:block;height:5px;border:1.5px solid var(--ink70);border-top:0}',
      '.cpTrue{position:absolute;right:9px;top:8px;font:700 9.5px var(--sans);letter-spacing:.06em;text-transform:uppercase;color:var(--ink45)}',
      '.cpMeta{display:grid;gap:4px;min-width:0}.cpMeta b{font:600 14px var(--sans);color:var(--ink);overflow-wrap:anywhere}.cpV{display:inline-block;font:700 10px var(--mono);padding:2px 7px;border-radius:999px;background:#efe7f8;color:#5d3f82;margin-left:6px;vertical-align:2px}',
      '.cpDim{font:600 15px var(--mono);color:var(--ink);letter-spacing:-.01em}.cpDim span{font:500 11px var(--sans);color:var(--ink45);margin-left:5px}.cpMeta .dim{font:11.5px/1.45 var(--sans);color:var(--ink70)}.cpHash{font:600 10.5px var(--mono);color:var(--ink45)}',
      '.cpRight{padding:4px 18px 10px;min-width:0}.cpSec{padding:12px 0;border-bottom:1px solid var(--line2)}.cpSec:last-child{border-bottom:0}',
      '.cpSec h4{margin:0 0 9px;font:700 10px var(--sans);letter-spacing:.1em;text-transform:uppercase;color:var(--ink45)}.cpRow{display:flex;align-items:center;gap:14px}.cpRow h4{margin:0;margin-right:auto}',
      '.cpOpts{display:grid;gap:7px}.cpOpt{display:grid;grid-template-columns:18px minmax(0,1fr);gap:10px;align-items:start;padding:9px 11px;border:1px solid var(--line);border-radius:10px;background:var(--card);cursor:pointer;text-align:left;width:100%;box-sizing:border-box;font:inherit;color:inherit;transition:border-color .15s,box-shadow .15s,background-color .15s}',
      '.cpOpt:hover{border-color:var(--ink25)}.cpOpt.on{border-color:var(--gold);box-shadow:0 0 0 1px var(--gold) inset;background:#fffdf7}.cpOpt:focus-visible{outline:2px solid var(--gold);outline-offset:2px}',
      '.cpDot{width:16px;height:16px;margin-top:1px;border-radius:50%;border:1.5px solid var(--ink25);box-sizing:border-box;display:grid;place-items:center;background:#fff}.cpOpt.on .cpDot{border-color:var(--gold)}.cpOpt.on .cpDot::after{content:"";width:8px;height:8px;border-radius:50%;background:var(--gold)}',
      '.cpOptT{display:grid;gap:2px;min-width:0}.cpOptT b{font:600 13px var(--sans);color:var(--ink)}.cpOptT span{font:12px/1.4 var(--sans);color:var(--ink70);overflow-wrap:anywhere}.cpOptT .mono{font-size:12px;color:var(--ink)}',
      '.cpRel{display:flex!important;align-items:flex-start;gap:8px;font:12px/1.45 var(--sans);color:var(--ink70);cursor:pointer}.cpRel input{margin:2px 0 0;accent-color:var(--gold);flex:none}.cpRel b{color:var(--ink);font-weight:600}',
      '.cpSub{display:grid;gap:9px;margin:2px 0 4px 29px}.cpSub[hidden],.cpDlg [hidden]{display:none!important}',
      '.cpIn{width:100%;box-sizing:border-box;padding:7px 10px;border:1px solid var(--line);border-radius:8px;background:var(--card);color:var(--ink);font:500 13px var(--sans)}.cpIn:focus{outline:2px solid rgba(169,130,63,.35);border-color:var(--gold)}.cpIn.cpShake{animation:cpShake .45s ease-in-out;border-color:var(--clay)}',
      '@keyframes cpShake{0%,100%{transform:none}25%{transform:translateX(-4px)}50%{transform:translateX(4px)}75%{transform:translateX(-2px)}}',
      '.cpLbl{display:grid;gap:4px;font:600 11.5px var(--sans);color:var(--ink70)}.cpLbl i{font-style:normal;font-weight:500;color:var(--ink45)}',
      '.cpHits{display:grid;gap:1px;max-height:172px;overflow:auto;border:1px solid var(--line2);border-radius:9px;padding:3px;background:var(--card)}.cpHits:empty{display:none}',
      '.cpHit{display:flex;align-items:baseline;gap:9px;width:100%;padding:6px 9px;border:0;border-radius:7px;background:none;text-align:left;cursor:pointer;font:12px var(--sans);color:var(--ink70);min-width:0}.cpHit b{font:600 12px var(--mono);color:var(--ink)}.cpHit span{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}',
      '.cpHit:hover,.cpHit.on{background:var(--paper2)}.cpNone{font:12px var(--sans);color:var(--ink45);padding:4px 2px}',
      '.cpPick{display:flex;align-items:center;gap:9px;padding:7px 10px;border-radius:9px;background:var(--goldSoft);border:1px solid var(--goldLine);font:12.5px var(--sans);color:var(--ink70);min-width:0}.cpPick b{font:600 12.5px var(--mono);color:var(--ink)}.cpPick span{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}',
      '.cpLink{margin-left:auto;border:0;background:none;padding:2px 0;font:600 12px var(--sans);color:#6a4a93;text-decoration:underline;text-underline-offset:2px;cursor:pointer;white-space:nowrap}',
      '.cpChips{display:flex;flex-wrap:wrap;gap:6px}.cpChip{border:1px solid var(--line);border-radius:999px;padding:5px 11px;font:600 11.5px var(--sans);background:var(--card);color:var(--ink70);cursor:pointer;transition:background-color .15s,border-color .15s,color .15s}',
      '.cpChip:hover{border-color:var(--ink45);color:var(--ink)}.cpChip.on{background:var(--velvet);border-color:var(--velvet);color:#fff}.cpChip:focus-visible{outline:2px solid var(--gold);outline-offset:2px}',
      '.cpHint{font:11.5px/1.45 var(--sans);color:var(--ink45)}.cpHint b{font-family:var(--mono);font-weight:600;color:var(--ink70)}',
      '.cpCause{display:grid;gap:8px;padding:10px 12px;border-radius:10px;border:1px solid #ecd2c6;background:#fcf5f1}.cpCauseHd{display:flex;align-items:baseline;gap:8px;font:700 10px var(--sans);letter-spacing:.08em;text-transform:uppercase;color:#8a3a26}',
      '.cpCauseHd span{font:500 11px var(--sans);letter-spacing:0;text-transform:none;color:var(--ink45)}.cpCause .cpIn{max-width:260px}.cpWhyL{font:12px/1.45 var(--sans);color:var(--ink70)}.cpWhyL b{color:var(--ink)}',
      '.cpCands .cpChip{font-weight:500}.cpCands .cpChip em{font-style:normal;color:var(--ink45);margin-left:4px}.cpCands .cpChip.on em{color:rgba(255,255,255,.7)}',
      '.cpSpin{display:inline-block;width:11px;height:11px;border:2px solid rgba(0,0,0,.14);border-top-color:var(--gold);border-radius:50%;animation:spin .7s linear infinite;vertical-align:-1px;margin-right:6px}',
      '.cpGoes{display:flex;align-items:center;gap:7px;margin-top:10px;font:12.5px var(--sans);color:var(--ink70);min-height:20px}.cpGoes svg{width:15px;height:15px;color:var(--gold);flex:none}.cpGoes b{color:var(--ink)}.cpGoes .new{color:#6a4a93}',
      '.cpDlg .cuMetals{justify-content:flex-start}',
      '.cpWho{font:12px var(--sans);color:var(--ink70);min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.cpWho b{color:var(--ink);font-weight:600}',
      '.cpWhy{font:600 11.5px var(--sans);color:#8a6420;text-align:right}.cpWhy.bad{color:#a4432c}.cpWhy.cpNudge{animation:cpShake .45s ease-in-out}',
      '.cpDlg .dlgFoot .btn{min-height:34px}',
      '@media(max-width:740px){.cpBody{grid-template-columns:1fr}.cpLeft{border-right:0;border-bottom:1px solid var(--line2)}.cpPlate{height:180px}.cpSub{margin-left:0}.cpDlg .dlgFoot{flex-wrap:wrap}.cpDlg .dlgFoot .left{flex:1 1 100%}}',
      // the Custom Orders card: the button in its column, and the design attached to the order across its foot
      '.btn.cpOpenBtn{display:inline-flex;align-items:center;gap:7px;background:#fff;border-color:rgba(125,86,168,.6);color:#4d3470}.btn.cpOpenBtn svg{width:16px;height:16px;flex:none}',
      '.btn.cpOpenBtn:hover{background:#f8f4fc;border-color:#8e6bb8}.btn.cpOpenBtn[aria-busy=true]{opacity:.65;cursor:progress}.btn.cpOpenBtn:focus-visible{outline:2px solid #8e6bb8;outline-offset:2px}',
      '.cpStrip{grid-column:1/-1;display:flex;align-items:center;gap:10px 14px;flex-wrap:wrap;padding:9px 12px;border-radius:10px;background:#faf7fd;border:1px solid rgba(125,86,168,.22);animation:aiIn .25s ease-out}',
      '.cpStLbl{font:700 10px var(--sans);letter-spacing:.08em;text-transform:uppercase;color:#6a4a93;white-space:nowrap}',
      '.cpStPic{width:44px;height:44px;box-sizing:border-box;padding:3px;border-radius:8px;background:#fff;border:1px solid var(--line);display:grid;place-items:center;overflow:hidden;cursor:pointer}.cpStPic svg,.cpStPic img{max-width:100%;max-height:100%}',
      '.cpStMeta{display:grid;gap:2px;min-width:0;flex:1 1 220px}.cpStMeta b{font:600 12.5px var(--sans);color:var(--ink);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.cpStMeta .dim{font:11.5px var(--sans);color:var(--ink70)}.cpStMeta .ok{color:#3c5a39;font-weight:600}',
      '.btn.cpOwBtn{display:inline-flex;align-items:center;gap:5px;background:#fff;border-color:rgba(125,86,168,.6);color:#4d3470}.btn.cpOwBtn svg{width:14px;height:14px}',
      '@media(prefers-reduced-motion:reduce){dialog.cpDlg[open],.cpStrip,.cpIn.cpShake,.cpWhy.cpNudge,.cpPlate .cpSk{animation:none}.cpPlate svg{transition:none}}'
    ].join('\n');
    (doc.head || doc.documentElement).appendChild(s);
  }

  let M = null;   // the pop-up open now: { dlg, st, resolve, promise }
  function orderInfo(rid) {
    rid = String(rid || '');
    const rows = rowsAll().filter(r => r && r.order && String(r.order.receiptId) === rid && r.state !== 'gone'), r = rows[0], sp = r && r.spec, x = sp && sp.special;
    return { rid, rows, buyer: (r && r.order.buyer && r.order.buyer.name) || '', label: x ? x.label || 'Custom order' : (r && r.line && r.line.title) || '', sku: (sp && sp.designSku) || (r && r.line && r.line.sku) || '', metal: metalKey(sp && sp.material) };
  }
  /** Orders a person types: the order search's index (every order this page knows, its number or its buyer), else the lines of the pull. */
  function findOrders(q, max = 8) {
    q = String(q || '').trim(); if (!q) return [];
    const S = root.OrderSearch, out = [];
    try {
      if (S && typeof S.query === 'function') for (const h of (S.query(q).hits || [])) { const e = h && h.e; if (e && e.rid && /^\d{4,}$/.test(e.rid)) out.push({ rid: String(e.rid), buyer: e.buyer || '', what: (e.skus && e.skus[0]) || (e.titles && e.titles[0]) || '' }); if (out.length >= max) break; }
    } catch (_) { /* the pull below */ }
    if (!out.length) {
      const d = q.replace(/[\s#-]/g, ''), lo = q.toLowerCase(), seen = new Set();
      for (const r of rowsAll()) {
        if (!r || !r.order) continue; const rid = String(r.order.receiptId); if (seen.has(rid)) continue;
        const buyer = (r.order.buyer && r.order.buyer.name) || '';
        if ((/^\d+$/.test(d) && rid.includes(d)) || (!/^\d+$/.test(d) && buyer.toLowerCase().includes(lo))) { seen.add(rid); out.push({ rid, buyer, what: (r.spec && r.spec.designSku) || (r.line && r.line.sku) || '' }); if (out.length >= max) break; }
      }
    }
    return out;
  }
  /** The pieces of an order this page holds, to say which one a fix fixes: [{ poolId, lineKey, label }] */
  function piecesOf(rid) {
    const out = [];
    for (const r of orderInfo(rid).rows) {
      const sku = (r.spec && r.spec.designSku) || (r.line && r.line.sku) || 'piece', m = codeOf(r.spec && r.spec.material), ids = (r.poolIds || []).filter(Boolean);
      if (ids.length) ids.forEach((id, i) => out.push({ poolId: String(id), lineKey: r.key, label: `${sku}${ids.length > 1 ? ' #' + (i + 1) : ''}${m ? ' · ' + m : ''}` }));
      else out.push({ poolId: null, lineKey: r.key, label: `${sku}${m ? ' · ' + m : ''}` });
    }
    return out.slice(0, 12);
  }

  /** The Review card of an order's custom line that takes a design (Review.customItemFor), or null. */
  function customItemOf(rid) {
    const R = root.Review; if (!R || typeof R.customItemFor !== 'function') return null;
    for (const r of orderInfo(rid).rows) if (needsDesign(r)) { try { const it = R.customItemFor(r.key, r); if (it && !it.done) return it; } catch (_) {} }
    return null;
  }
  /** That card released (the card's own Release Hold: CustomPrint.complete, under the person signed in). → Promise<boolean> */
  function release(it) {
    const C = root.CustomPrint; if (!C || typeof C.complete !== 'function' || !it) return Promise.resolve(false);
    try { return Promise.resolve(C.complete(it)).then(v => !!v, e => { console.warn('[custom piece] release', e); return false; }); } catch (e) { console.warn('[custom piece] release', e); return Promise.resolve(false); }
  }

  /** THE pop-up. See the header. */
  function open(o) {
    o = o || {};
    if (!doc) return Promise.resolve(null);
    if (M) { try { M.dlg.querySelector('[data-cp-send]').focus(); } catch (_) {} return M.promise; }
    css();
    const t0 = now(), ctx = o.orderId ? orderInfo(o.orderId) : null;
    if (ctx) ctx.item = customItemOf(ctx.rid);
    const st = { designId: String(o.designId || (o.design && o.design.id) || ''), version: +o.version || +(o.design && o.design.version) || 0, design: null, loadErr: '',
      ctx, attach: ctx ? 'this' : 'none', other: null, q: '', hits: [], hitAt: -1,
      name: '', reason: o.poolId && !ctx ? 'fix' : '', fix: null, fq: '', fhits: [], fhitAt: -1, cause: null, causeBusy: false, causedBy: '', causeEdited: false,
      metal: (ctx && ctx.metal) || '', qty: Math.max(1, Math.min(99, Math.floor(+o.quantity || 1))), busy: false, err: '' };
    if (o.poolId && !ctx) { const rid = String(o.poolId).split('_')[0], pc = piecesOf(rid).find(p => p.poolId === String(o.poolId)); st.fix = { rid, poolId: String(o.poolId), lineKey: pc ? pc.lineKey : null, label: pc ? pc.label : '' }; }
    const dlg = el('dialog', 'cpDlg'); dlg.id = 'cpDlg'; dlg.setAttribute('aria-labelledby', 'cpT');
    const ord = ctx ? `<button type="button" class="cpOpt" data-cp-attach="this" role="radio"><span class="cpDot"></span><span class="cpOptT"><b>This custom order</b><span><span class="mono">${esc(ctx.rid)}</span>${ctx.buyer ? ' · ' + esc(ctx.buyer) : ''}${ctx.label ? ' · ' + esc(ctx.label) : ''}${ctx.sku ? ' · ' + esc(ctx.sku) : ''}</span></span></button>` : '';
    dlg.innerHTML = `<div class="dlg">
<div class="dlgHead"><div class="cpHd"><h3 id="cpT">Send to sheet</h3><span class="sub">The piece is cut exactly as designed, on the next sheet of the metal you pick</span></div>
  <div class="right"><span class="cpTag" title="Every piece sent from here is marked handmade custom on its sheet and in its files"><i></i>Handmade custom</span><button type="button" class="cpX" data-cp-close aria-label="Close">×</button></div></div>
<div class="cpBody">
  <section class="cpLeft" aria-label="The design">
    <div class="cpPlate" data-cp-plate><div class="cpSk"></div></div>
    <div class="cpMeta" data-cp-meta><b>Reading the design…</b></div>
  </section>
  <div class="cpRight">
    <section class="cpSec"><h4 id="cpAttT">Attach to</h4><div class="cpOpts" role="radiogroup" aria-labelledby="cpAttT">${ord}
      ${ctx && ctx.item ? `<label class="cpSub cpRel" data-cp-sub="this"><input type="checkbox" data-cp-release checked><span>Release order <b class="mono">${esc(ctx.rid)}</b> from Review once its piece is on the sheet: its custom design is done</span></label>` : ''}
      <button type="button" class="cpOpt" data-cp-attach="other" role="radio"><span class="cpDot"></span><span class="cpOptT"><b>Another order</b><span>Find it by its order number or its buyer</span></span></button>
      <div class="cpSub" data-cp-sub="other" hidden><input class="cpIn" data-cp-q type="search" autocomplete="off" spellcheck="false" placeholder="Order number or buyer" aria-label="Order number or buyer"><div class="cpHits" data-cp-hits role="listbox"></div><div data-cp-other></div></div>
      <button type="button" class="cpOpt" data-cp-attach="none" role="radio"><span class="cpDot"></span><span class="cpOptT"><b>No order</b><span>A piece of its own: forgotten, a fix, a sample</span></span></button>
      <div class="cpSub" data-cp-sub="none" hidden>
        <label class="cpLbl">Name <i>required: what it is, so it is easy to pick out on the sheet</i><input class="cpIn" data-cp-name maxlength="60" autocomplete="off" placeholder="e.g. Replacement heart for Emma"></label>
        <div class="cpLbl">Why it is made<div class="cpChips" data-cp-reasons role="radiogroup" aria-label="Why it is made">${REASONS.map(([k, t, s]) => `<button type="button" class="cpChip" data-cp-reason="${k}" role="radio" title="${esc(s)}">${esc(t)}</button>`).join('')}</div></div>
        <div class="cpSub" data-cp-fix hidden style="margin:0">
          <div class="cpLbl">The order or piece it fixes<input class="cpIn" data-cp-fq type="search" autocomplete="off" spellcheck="false" placeholder="Order number or buyer" aria-label="The order it fixes"></div>
          <div class="cpHits" data-cp-fhits role="listbox"></div><div data-cp-fixed></div><div data-cp-cause></div>
        </div>
        <div class="cpHint">It gets its own number, <b>CUSTOM-${ymd()}-…</b>, rides a sheet and the laser like any piece, and has no shipping label.</div>
      </div></div></section>
    <section class="cpSec"><h4 id="cpMetT">Metal</h4><div class="cuMetals" role="radiogroup" aria-labelledby="cpMetT">${METALS.map(m => `<button type="button" class="cuM" data-cp-metal="${m.key}" role="radio" title="${esc(m.word)}" style="--mc:${esc(colourOf(m.key))}"><i></i>${esc(m.code)}</button>`).join('')}</div>
      <div class="cpGoes" data-cp-goes aria-live="polite"></div></section>
    <section class="cpSec cpRow"><h4 id="cpQtyT">How many</h4><span class="cuQty" role="group" aria-labelledby="cpQtyT"><button type="button" data-cp-qty="-1" aria-label="One fewer">−</button><b data-cp-n>1</b><button type="button" data-cp-qty="1" aria-label="One more">+</button></span></section>
  </div>
</div>
<div class="dlgFoot"><span class="left cpWho" data-cp-who></span><span class="cpWhy" data-cp-why aria-live="polite"></span><button type="button" class="btn ghost sm" data-cp-close>Cancel</button><button type="button" class="btn gold sm" data-cp-send>Send to sheet</button></div>
</div>`;
    doc.body.appendChild(dlg);
    let resolve; const promise = new Promise(r => { resolve = r; });
    M = { dlg, st, resolve, promise, t0 };
    const q = s => dlg.querySelector(s);
    const done = v => { if (!M || M.dlg !== dlg) return; M = null; try { dlg.close(); } catch (_) {} dlg.remove(); resolve(v || null); };
    M.done = done;

    /* ── painting: each part on its own, inputs never rebuilt (focus and caret stay where they are) ── */
    function paintDesign() {
      const plate = q('[data-cp-plate]'), meta = q('[data-cp-meta]');
      if (st.loadErr) { plate.innerHTML = ''; meta.innerHTML = `<b>The design could not be read</b><span class="dim">${esc(st.loadErr)}</span>`; return; }
      const d = st.design; if (!d) return;
      const pic = previewSvg(d, { metal: st.metal || null, fitMm: [58, 48] }), sz = d.widthMm ? { wMm: +d.widthMm, hMm: +d.heightMm } : sizeOf(d) || { wMm: 0, hMm: 0 };
      if (pic) {
        const sc = pic.scale;
        plate.innerHTML = pic.svg + `<span class="cpTrue">${sc >= 0.999 ? 'Actual size' : `Shown at ${Math.round(sc * 100)}%`}</span><span class="cpScale"><i style="width:${+(10 * sc).toFixed(3)}mm"></i>10 mm</span>`;
        plate.style.backgroundSize = `${+(5 * sc).toFixed(3)}mm ${+(5 * sc).toFixed(3)}mm`;
      } else plate.innerHTML = '<span class="cpHint">Nothing drawn yet</span>';
      meta.innerHTML = `<div><b>${esc(d.name || 'Custom design')}</b><span class="cpV" title="Version ${esc(st.version)} of this design">v${esc(st.version)}</span></div>`
        + `<div class="cpDim" title="The size it is cut at: its cut line">${sz.wMm.toFixed(2)} × ${sz.hMm.toFixed(2)}<span>mm</span></div>`
        + `<span class="dim">${d.by ? 'Saved by ' + esc(d.by) : 'Saved'}${d.at ? ' · ' + esc(when(d.at)) : ''}${d.source && d.source.sku ? ' · from ' + esc(d.source.sku) : ''}</span>`
        + (d.hash ? `<span class="cpHash" title="The design's fingerprint: the files of its sheet are checked against it, so the piece is cut exactly as designed">#${esc(String(d.hash).slice(0, 10))}</span>` : '');
    }
    const hitsHtml = (list, at, none) => list.length ? list.map((h, i) => `<button type="button" class="cpHit${i === at ? ' on' : ''}" data-cp-hit="${i}" role="option" aria-selected="${i === at}"><b>${esc(h.rid)}</b><span>${esc([h.buyer, h.what].filter(Boolean).join(' · '))}</span></button>`).join('') : none ? `<div class="cpNone">${esc(none)}</div>` : '';
    function paintAttach() {
      for (const b of dlg.querySelectorAll('[data-cp-attach]')) { const on = b.dataset.cpAttach === st.attach; b.classList.toggle('on', on); b.setAttribute('aria-checked', String(on)); }
      const rel = q('[data-cp-sub="this"]'); if (rel) rel.hidden = st.attach !== 'this';
      q('[data-cp-sub="other"]').hidden = st.attach !== 'other'; q('[data-cp-sub="none"]').hidden = st.attach !== 'none';
      const hits = q('[data-cp-hits]'), other = q('[data-cp-other]'), qi = q('[data-cp-q]');
      qi.hidden = !!st.other; hits.innerHTML = st.other ? '' : hitsHtml(st.hits, st.hitAt, st.q.trim() ? 'No order found' : '');
      other.innerHTML = st.other ? `<div class="cpPick"><b>${esc(st.other.rid)}</b><span>${esc([st.other.buyer, st.other.what].filter(Boolean).join(' · '))}</span><button type="button" class="cpLink" data-cp-unpick="other">Change</button></div>` : '';
      for (const b of dlg.querySelectorAll('[data-cp-reason]')) { const on = b.dataset.cpReason === st.reason; b.classList.toggle('on', on); b.setAttribute('aria-checked', String(on)); }
      q('[data-cp-fix]').hidden = st.reason !== 'fix';
      paintFix();
    }
    function paintFix() {
      const fq = q('[data-cp-fq]'), fh = q('[data-cp-fhits]'), fixed = q('[data-cp-fixed]'), cause = q('[data-cp-cause]');
      fq.hidden = !!st.fix; fh.innerHTML = st.fix ? '' : hitsHtml(st.fhits, st.fhitAt, st.fq.trim() ? 'No order found' : '');
      if (!st.fix) { fixed.innerHTML = ''; cause.innerHTML = ''; return; }
      const f = st.fix, info = orderInfo(f.rid), pcs = piecesOf(f.rid);
      fixed.innerHTML = `<div class="cpPick"><b>${esc(f.rid)}</b><span>${esc([info.buyer || f.buyer, f.label].filter(Boolean).join(' · '))}</span><button type="button" class="cpLink" data-cp-unpick="fix">Change</button></div>`
        + (pcs.length > 1 ? `<div class="cpChips" style="margin-top:7px" role="radiogroup" aria-label="Which piece">${[{ poolId: null, lineKey: null, label: 'The whole order' }].concat(pcs).map((p, i) => `<button type="button" class="cpChip${(p.poolId || null) === (f.poolId || null) && (p.lineKey || null) === (f.lineKey || null) ? ' on' : ''}" data-cp-piece="${i - 1}">${esc(p.label)}</button>`).join('')}</div>` : '');
      if (st.causeBusy && !st.cause) { cause.innerHTML = `<div class="cpCause"><div class="cpCauseHd">Original mistake by</div><div class="cpWhyL"><span class="cpSpin"></span>Reading order ${esc(f.rid)}'s history…</div></div>`; return; }
      const c = st.cause || { suggested: '', why: '', candidates: [] }, names = [...new Map(c.candidates.map(x => [x.name, x])).values()];
      cause.innerHTML = `<div class="cpCause"><div class="cpCauseHd">Original mistake by<span>${c.error ? 'its history could not be read' : names.length ? `from order ${esc(f.rid)}'s history` : 'nobody is named in its history'}</span></div>`
        + `<input class="cpIn" data-cp-causer autocomplete="off" maxlength="80" placeholder="Who made the mistake" aria-label="Who made the original mistake" value="${esc(st.causedBy)}">`
        + (c.why ? `<div class="cpWhyL">${st.causedBy && st.causedBy === c.suggested ? 'Suggested: ' : 'The history says '}<b>${esc(c.why)}</b>${c.candidates[0] && c.candidates[0].at ? ' · ' + esc(when(c.candidates[0].at)) : ''}</div>` : '')
        + (names.length > 1 || (names.length === 1 && st.causedBy !== names[0].name) ? `<div class="cpChips cpCands">${names.slice(0, 6).map(x => `<button type="button" class="cpChip${x.name === st.causedBy ? ' on' : ''}" data-cp-causer-pick="${esc(x.name)}" title="${esc(x.name + ' ' + x.step)}">${esc(x.name)}<em>${esc(x.step)}</em></button>`).join('')}</div>` : '')
        + `<div class="cpHint">You and ${st.causedBy ? esc(st.causedBy) : 'the person named'} are both kept with this piece: who fixed it, and who the mistake was.</div></div>`;
    }
    function paintMetal() {
      for (const b of dlg.querySelectorAll('[data-cp-metal]')) { const on = b.dataset.cpMetal === st.metal; b.classList.toggle('on', on); b.setAttribute('aria-checked', String(on)); }
      const g = st.goes = st.metal ? goesNow() : null, box = q('[data-cp-goes]');
      box.innerHTML = !st.metal ? '<span>Pick the metal: the piece goes onto the next open sheet of it</span>' : `${ARROW}<span>Goes onto ${g.fresh ? `<b class="new">${esc(g.name)}</b>` : `<b>${esc(g.name)}</b>`}${st.qty > 1 ? ` · ${plural(st.qty, 'piece')}` : ''}</span>`;
      tint(q('[data-cp-plate] svg'), st.metal);     // (the piece is drawn in the metal it goes on)
    }
    const orderNow = () => (st.attach === 'this' && st.ctx ? st.ctx.rid : st.attach === 'other' && st.other ? st.other.rid : null);
    const goesNow = () => goesOnto(st.metal, orderNow());
    function whyNot() {
      if (st.loadErr) return 'The design could not be read';
      if (!st.design) return 'Reading the design…';
      if (st.attach === 'other' && !st.other) return 'Find the order to attach it to';
      if (st.attach === 'none') {
        if (!st.name.trim()) return 'Name the piece';
        if (!st.reason) return 'Say why it is made';
        if (st.reason === 'fix' && !st.fix) return 'Pick the order or piece it fixes';
      }
      if (!st.metal) return 'Pick the metal';
      return '';
    }
    function sync() {
      const why = st.err || whyNot(), b = q('[data-cp-send]'), w = q('[data-cp-why]'), g = st.metal ? st.goes || (st.goes = goesNow()) : null;
      w.textContent = st.busy ? '' : why; w.classList.toggle('bad', !!st.err);
      b.setAttribute('aria-disabled', String(!!whyNot() || st.busy)); b.disabled = st.busy;
      b.innerHTML = st.busy ? '<span class="cpSpin"></span>Sending…' : esc(`Send ${st.qty > 1 ? st.qty + ' ' : ''}to ${g ? g.name : 'sheet'}`);
      q('[data-cp-n]').textContent = String(st.qty);
      for (const x of dlg.querySelectorAll('[data-cp-qty]')) x.disabled = st.busy || (+x.dataset.cpQty < 0 ? st.qty <= 1 : st.qty >= 99);
      const by = who(); q('[data-cp-who]').innerHTML = by ? `Sent by <b>${esc(by)}</b> · handmade custom` : 'You sign in when you send';
    }
    const all = () => { paintAttach(); paintMetal(); sync(); };

    /* ── what a press does ── */
    function setAttach(k) { if (st.busy || !k || (k === 'this' && !st.ctx)) return; st.attach = k; st.err = ''; all(); const f = k === 'other' ? q('[data-cp-q]') : k === 'none' ? q('[data-cp-name]') : null; if (f && !f.hidden) requestAnimationFrame(() => { try { f.focus(); } catch (_) {} }); }
    function pickOther(h) { if (!h) return; st.other = h; st.q = ''; st.hits = []; st.hitAt = -1; q('[data-cp-q]').value = ''; st.err = ''; all(); }
    function pickFix(h) { if (!h) return; st.fix = { rid: h.rid, buyer: h.buyer || '', poolId: null, lineKey: null, label: '' }; st.fq = ''; st.fhits = []; st.fhitAt = -1; q('[data-cp-fq]').value = ''; st.err = ''; loadCause(); all(); }
    function loadCause() {
      const f = st.fix; if (!f) return;
      st.cause = null; st.causeBusy = true;
      causeOf(f.rid, { lineKey: f.lineKey || undefined }).then(c => c || { suggested: '', why: '', candidates: [] }, e => ({ suggested: '', why: '', candidates: [], error: String((e && e.message) || e) })).then(c => {
        if (!M || M.st !== st || st.fix !== f) return;
        st.cause = Object.assign({ candidates: [] }, c); st.causeBusy = false;
        if (!st.causeEdited) st.causedBy = c.suggested || '';
        paintFix(); sync();
      });
      paintFix();
    }
    async function press() {
      const why = whyNot();
      if (why || st.busy) { const w = q('[data-cp-why]'); w.textContent = why; w.classList.remove('cpNudge'); void w.offsetWidth; w.classList.add('cpNudge'); if (why === 'Name the piece') { const n = q('[data-cp-name]'); n.classList.remove('cpShake'); void n.offsetWidth; n.classList.add('cpShake'); n.focus(); } return; }
      const by = who() || await askWho('Kept with this custom piece: who sent it, and when.', 'send this piece to a sheet');
      if (!by || !M || M.st !== st) return;
      st.busy = true; st.err = ''; sync();
      const g = st.goes = goesNow(), orderId = orderNow(), none = st.attach === 'none';
      try {
        const out = await send({ designId: st.designId, version: st.version, design: st.design, metal: st.metal, quantity: st.qty, orderId, customName: none ? st.name.trim() : '', reason: none ? st.reason : null,
          fixes: none && st.reason === 'fix' && st.fix ? { orderId: st.fix.rid, poolId: st.fix.poolId, lineKey: st.fix.lineKey, causedBy: st.causedBy.trim(), suggested: st.cause ? st.cause.suggested : '' } : null, by, sheet: g.name });
        // (the custom order this was designed for: its line is released from Review once its piece is on, as the card's own Release Hold does it: CustomPrint, its record, its seal, its step
        //  on the timeline; custompool's piece is an extra piece of the order and settles nothing by itself)
        const rel = q('[data-cp-release]');
        if (st.attach === 'this' && st.ctx && st.ctx.item && rel && rel.checked) release(st.ctx.item).then(v => { out.released = v; });
        say(`${plural(st.qty, 'handmade custom piece')} → ${g.name}${orderId ? ` · order ${orderId}` : out.orderId ? ` · ${out.orderId}` : ` · “${st.name.trim()}”`}`, 'ok', 6000);
        done(out);
      } catch (e) { if (!M || M.st !== st) return; st.busy = false; st.err = `Not sent: ${String((e && e.message) || e).slice(0, 160)}`; sync(); }
    }
    function step(k, d) {   // the arrows in a list of orders found
      const list = k === 'fix' ? st.fhits : st.hits; if (!list.length) return;
      const at = (k === 'fix' ? st.fhitAt : st.hitAt) + d, v = (at + list.length) % list.length;
      if (k === 'fix') st.fhitAt = v; else st.hitAt = v; paintAttach();
    }
    /** Every key in the pop-up. The Charm Editor under it takes the keys of the whole window first (it hands them to the element's own __ceKey), so
     *  the same function is that element's __ceKey: a letter typed here never changes the editor's tool, Delete never deletes its parts. */
    function key(e) {
      const t = e.target, k = e.key;
      if (k === 'Escape') { e.preventDefault(); if ((t.matches && t.matches('[data-cp-q],[data-cp-fq]')) && t.value) { t.value = ''; t.dispatchEvent(new Event('input', { bubbles: true })); return; } done(null); return; }
      if (t.matches && t.matches('[data-cp-q],[data-cp-fq]')) {
        const fx = t.matches('[data-cp-fq]');
        if (k === 'ArrowDown' || k === 'ArrowUp') { e.preventDefault(); step(fx ? 'fix' : 'other', k === 'ArrowDown' ? 1 : -1); return; }
        if (k === 'Enter') { e.preventDefault(); const list = fx ? st.fhits : st.hits, at = Math.max(0, fx ? st.fhitAt : st.hitAt); if (list[at]) (fx ? pickFix : pickOther)(list[at]); return; }
      }
      if (k === 'Enter' && t.matches && t.matches('[data-cp-name],[data-cp-causer]')) { e.preventDefault(); if (!whyNot()) press(); }
    }
    dlg.addEventListener('keydown', key);
    dlg.addEventListener('focusin', e => { if (e.target) e.target.__ceKey = key; });
    dlg.addEventListener('cancel', e => { e.preventDefault(); done(null); });
    dlg.addEventListener('click', e => {
      const t = e.target, c = s => t.closest(s);
      if (e.target === dlg) { const r = dlg.getBoundingClientRect(); if (e.clientX < r.left || e.clientX > r.right || e.clientY < r.top || e.clientY > r.bottom) done(null); return; }   // (a press on the shade outside it)
      if (c('[data-cp-close]')) { if (!st.busy) done(null); return; }
      if (c('[data-cp-send]')) { press(); return; }
      if (st.busy) return;
      const a = c('[data-cp-attach]'); if (a) { setAttach(a.dataset.cpAttach); return; }
      const h = c('[data-cp-hit]'); if (h) { const fx = !!h.closest('[data-cp-fhits]'), list = fx ? st.fhits : st.hits; (fx ? pickFix : pickOther)(list[+h.dataset.cpHit]); return; }
      const u = c('[data-cp-unpick]'); if (u) { if (u.dataset.cpUnpick === 'fix') { st.fix = null; st.cause = null; st.causedBy = ''; st.causeEdited = false; } else st.other = null; all(); const f = q(u.dataset.cpUnpick === 'fix' ? '[data-cp-fq]' : '[data-cp-q]'); requestAnimationFrame(() => { try { f.focus(); } catch (_) {} }); return; }
      const r = c('[data-cp-reason]'); if (r) { st.reason = r.dataset.cpReason; st.err = ''; all(); if (st.reason === 'fix' && !st.fix) requestAnimationFrame(() => { try { q('[data-cp-fq]').focus(); } catch (_) {} }); return; }
      const p = c('[data-cp-piece]'); if (p && st.fix) { const i = +p.dataset.cpPiece, pc = i >= 0 ? piecesOf(st.fix.rid)[i] : null; Object.assign(st.fix, { poolId: pc ? pc.poolId : null, lineKey: pc ? pc.lineKey : null, label: pc ? pc.label : '' }); st.causeEdited = false; loadCause(); all(); return; }
      const w = c('[data-cp-causer-pick]'); if (w) { st.causedBy = w.dataset.cpCauserPick; st.causeEdited = true; paintFix(); sync(); return; }
      const m = c('[data-cp-metal]'); if (m) { st.metal = m.dataset.cpMetal; st.err = ''; paintMetal(); sync(); return; }
      const n = c('[data-cp-qty]'); if (n && !n.disabled) { st.qty = Math.max(1, Math.min(99, st.qty + +n.dataset.cpQty)); paintMetal(); sync(); }
    });
    dlg.addEventListener('input', e => {
      const t = e.target;
      if (t.matches('[data-cp-q]')) { st.q = t.value; st.hits = findOrders(t.value); st.hitAt = st.hits.length ? 0 : -1; paintAttach(); sync(); }
      else if (t.matches('[data-cp-fq]')) { st.fq = t.value; st.fhits = findOrders(t.value); st.fhitAt = st.fhits.length ? 0 : -1; paintFix(); sync(); }
      else if (t.matches('[data-cp-name]')) { st.name = t.value; st.err = ''; sync(); }
      else if (t.matches('[data-cp-causer]')) { st.causedBy = t.value; st.causeEdited = true; for (const b of dlg.querySelectorAll('[data-cp-causer-pick]')) b.classList.toggle('on', b.dataset.cpCauserPick === t.value.trim()); }
    });

    all();
    if (st.fix) loadCause();
    try { dlg.showModal(); } catch (_) { dlg.setAttribute('open', ''); }
    requestAnimationFrame(() => { try { (st.attach === 'none' ? q('[data-cp-name]') : q(st.metal ? '[data-cp-send]' : '[data-cp-metal]')).focus({ preventScroll: true }); } catch (_) {} });
    M.openMs = now() - t0;
    // the design: in hand (just saved), kept from before, or read once from the library
    designOf(st.designId, st.version, o.design).then(d => { if (!M || M.st !== st) return; st.design = d; st.version = +d.version || st.version; paintDesign(); sync(); },
      e => { if (!M || M.st !== st) return; st.loadErr = String((e && e.message) || e); paintDesign(); sync(); });
    return promise;
  }

  /* ═══ 8 · a custom order in the Charm Designer (item 22) ═══ */
  /** Does this order line take a design of its own: a custom piece, a rework, a special order that is cut (never chain only, never a monogram, which
   *  makes its own design from the buyer's letters), not finished or gone. */
  function needsDesign(row, it) {
    const sp = row && row.spec, x = sp && sp.special;
    if (!x || x.notCut || x.monogram || !['custom', 'rework', 'other'].includes(x.group)) return false;
    if (['gone', 'skipped'].includes(row.state) || sp.customDone || (it && it.done)) return false;
    return true;
  }
  const blankFace = () => ({ face: 'front', unitPt: true, frame: { x0: 0, y0: 0, wPt: 20 * PT, hPt: 20 * PT }, items: [] });
  /** The design dropped on the order's card (its .ai, or its .dxf read into one), as editor layers: each piece of it, every part a layer. null: none. */
  async function orderFileFaces(it) {
    const CS = root.CustomSheet, P = root.CharmNestPDF, AE = root.CharmNestArtEdit;
    if (!CS || !P || !AE || typeof CS.cardOf !== 'function') return null;
    const c = CS.cardOf(it), F = c && c.e && (c.e.files || []).find(f => f && f.state === 'ready'); if (!F) return null;
    let bytes = F.bytes;
    if (!(bytes && bytes.length) && F.cloud) {
      let url = F.cloud.url || '';
      try { if (!url && F.cloud.path && root.CN && typeof root.CN.api === 'function') url = (await root.CN.api('charmNestOutput', { op: 'url', path: F.cloud.path })).url; } catch (_) { url = ''; }
      try { bytes = url && root.CharmNestAssets ? await root.CharmNestAssets.bytes(url) : null; } catch (_) { bytes = null; }
    }
    if (!(bytes && bytes.length)) return null;
    const parsed = await P.parseSource(bytes, F.name), g = await (P.groupCharmsAsync || P.groupCharms)(parsed, { minPt: 6 });
    const items = [];
    (g.charms || []).forEach((ch, ci) => { for (const x of AE.itemsOf(ch, null, 'front')) items.push(Object.assign({}, x, { id: `f${ci}.${x.id}`, kind: 'path', source: 'added' })); });   // (ids of their own: two pieces of one file never share one)
    return items.length ? { faces: { front: { face: 'front', unitPt: true, frame: null, items } }, file: F.name } : null;
  }
  /** The engraving's own fonts and font engine (charm-nest-bridge.js charmEditFonts / charmEditText, CNCharmEditText): words typed in the designer
   *  come back as the same outlines the engraving cuts. Without them the editor's plain stand-in. */
  async function textEngine() {
    const T = root.CNCharmEditText;
    if (!T || typeof T.text !== 'function') return { fonts: [], text: undefined };
    let fonts = []; try { fonts = await T.fonts(null); } catch (_) { fonts = []; }
    return { fonts, text: T.text };
  }
  let designing = null, live = null;    // live: what the designer opened from here is working on ({ orderId, design, name, source, saved })
  const busyCards = new Set();
  function openDesigner(x, o) {
    if (designing) return designing;
    designing = designOnce(x, o || {}).catch(e => { console.warn('[custom piece] Charm Designer:', e); say(`Charm Designer: ${String((e && e.message) || e).slice(0, 160)}`, 'bad', 7000); return null; }).finally(() => { designing = null; });
    return designing;
  }
  async function designOnce(x, o) {
    const CE = root.CharmEditor;
    if (!CE || typeof CE.open !== 'function') { say('The Charm Designer is not loaded on this page — reload it', 'bad'); return null; }
    if (CE.current) { say('The Charm Designer is already open'); return null; }
    const row = rowsOf(x)[0]; if (!row) return null;
    const rid = String(row.order.receiptId), info = orderInfo(rid);
    const by = who() || await askWho('Kept with the design you make for this order.', 'design this custom order'); if (!by) return null;
    busyCards.add(rid); repaint(rid);
    try {
      // (a press in the order window: the window steps aside first, the designer is a workspace over the whole page)
      const W = root.OrderWin; if (W && typeof W.isOpen === 'function' && W.isOpen()) { try { W.close(); } catch (_) {} for (let i = 0; i < 60 && W.isOpen(); i++) await new Promise(r => setTimeout(r, 16)); }
      const att = attachedOf(rid)[0] || null;
      let base = null, faces = null, source = { kind: 'new' }, from = 'blank';
      if (att) { try { base = await designOf(att.id, att.version, null, { open: true }); faces = base.faces; source = base.source || source; from = 'design'; } catch (e) { console.warn('[custom piece] the attached design could not be read:', e && e.message); } }
      if (!faces) { try { const f = await orderFileFaces(x); if (f) { faces = f.faces; source = { kind: 'import', file: f.file }; from = 'file'; } } catch (e) { console.warn('[custom piece] the order\'s file could not be read:', e && e.message); } }
      const blank = !faces; if (blank) faces = { front: blankFace() };
      const name = (att && att.name) || (base && base.name) || `${info.sku || 'Custom'} · ${rid}`;
      const T = await textEngine();
      log('editorOpen', { orderId: rid, sku: info.sku, designId: base ? base.id : undefined, version: base ? base.version : undefined, name, file: source.file, by });
      const l = live = { orderId: rid, design: base, name, source, saved: null, sent: null };
      let res;
      try {
        res = await CE.open({ title: name, subtitle: `Custom order ${rid}${info.buyer ? ' · ' + info.buyer : ''}${info.label ? ' · ' + info.label : ''} · Save, then send it to a sheet`, faces, startFace: 'front',
          fonts: T.fonts, textToPaths: T.text, lockRoles: blank ? [] : ['cut'],
          design: { id: base ? base.id : null, version: base ? base.version : null, name, orderId: rid, source }, orderId: rid });   // (design, orderId: what the design library's own hooks read)
      } finally { live = null; }
      if (!res) return null;
      busyCards.delete(rid); repaint(rid);
      const saved = await saveResult(res, { base: l.saved || base, orderId: rid, name, source, by, from });
      if (!saved) return null;
      // (sent from inside the designer already, and nothing changed since: the pop-up is not offered a second time)
      if (l.sent && l.sent.id === saved.id && l.sent.version === saved.version) return l.sent.out;
      return open({ designId: saved.id, version: saved.version, orderId: rid, design: saved.design });
    } finally { busyCards.delete(rid); repaint(rid); }
  }
  /* The editor says a drawn shape's paint as true / false ("the role's own pen"); the design library keeps a colour. Each true is written as the pen the laser gives
     that role (charm-nest-custom-pool.js paintOf: a cut line black, a line engraving red, a fill the hatching blue), each false as none: the same parts, the same
     file, a colour the library takes. */
  const PEN = { cut: { stroke: [0, 0, 0] }, line: { stroke: [1, 0, 0] }, hatch: { fill: [0, 0, 1] }, art: { fill: [0, 0, 1], stroke: [1, 0, 0] } };
  function pens(items) {
    return (items || []).map(it => {
      if (!it || typeof it !== 'object') return it;
      const o = Object.assign({}, it), pen = PEN[o.role] || {};
      for (const k of ['stroke', 'fill']) if (typeof o[k] === 'boolean') o[k] = o[k] && pen[k] ? pen[k].slice() : null;
      if (Array.isArray(o.children)) o.children = pens(o.children);
      return o;
    });
  }
  const pensOf = faces => Object.fromEntries(Object.entries(faces).map(([f, fd]) => [f, fd && Array.isArray(fd.items) ? Object.assign({}, fd, { items: pens(fd.items) }) : fd]));
  /** What the editor handed back, kept in the design library as a version attached to the order (D8, item 24). The same drawing saved twice is one version.
   *  → { id, version, hash, design } | null */
  async function saveResult(res, c) {
    const faces = res && res.faces; if (!faces || !(faces.front || faces.back)) return null;
    const draft = { faces }, sz = sizeOf(draft);
    if (!sz) { say('Nothing to keep: the design has no drawing yet', 'bad', 6000); return null; }
    const sig = fnv(JSON.stringify(faces)), Mo = model();
    // nothing changed since the version it was opened on (the editor's own diff), or the very drawing kept a moment ago: that version, no new one
    const same = c.base && c.base.id && (c.base.sig === sig || (!c.base.sig && res.changes && Mo && typeof Mo.changeCount === 'function' && Object.values(res.changes).every(ch => !ch || Mo.changeCount(ch) === 0)));
    if (same) return { id: c.base.id, version: c.base.version, hash: c.base.hash, design: c.base };
    if (res.design && res.design.id && res.design.version) {   // (the design library's own hook on the editor already kept this save)
      const d = remember(Object.assign({ name: c.name, faces, widthMm: sz.wMm, heightMm: sz.hMm, source: c.source, orderId: c.orderId || null, by: c.by, at: Date.now(), sig }, res.design));
      if (c.orderId) attach(c.orderId, d);
      return { id: d.id, version: d.version, hash: d.hash, design: d };
    }
    const D = root.CharmNestDesigns; if (!D || typeof D.put !== 'function') { say('The design library is not on this page yet — the design was not kept', 'bad', 8000); return null; }
    const design = { name: c.name, faces: pensOf(faces), widthMm: +sz.wMm.toFixed(3), heightMm: +sz.hMm.toFixed(3), source: c.source || { kind: 'new' }, by: c.by };
    if (c.base && c.base.id) design.id = c.base.id;
    if (c.orderId) design.orderId = String(c.orderId);
    const r = await D.put(design);
    if (!r || !r.id) throw new Error('the design library did not keep the design');
    // the version as the library keeps it (its put holds it in memory: no read), the very object the pool checks against its hash
    let kept = null; try { kept = typeof D.get === 'function' ? await D.get(r.id, r.version) : null; } catch (_) { kept = null; }
    const d = remember(Object.assign({}, design, kept && kept.faces ? kept : {}, { id: r.id, version: +r.version || 1, hash: r.hash || (kept && kept.hash) || '', at: +r.at || Date.now(), sig }));
    log('editorSave', { orderId: c.orderId || '', designId: d.id, version: d.version, name: d.name, faces: Object.keys(faces).filter(f => faces[f]), by: c.by });
    if (c.orderId) attach(c.orderId, d);
    return { id: d.id, version: d.version, hash: d.hash, design: d };
  }

  /* ── the Custom Orders card and the order window ── */
  const mounted = new Map();     // order id -> Set of its cards on the page (their parts are painted again when a design is attached or sent)
  function repaint(rid) { const set = mounted.get(String(rid)); if (!set) return; for (const n of [...set]) { if (!n.isConnected) { set.delete(n); continue; } try { paintCard(n, n._cpIt); } catch (_) {} } }
  /** A Custom Orders card drawn (CustomSheet.wire): its "Open in Charm Designer" and the design attached to its order. */
  function wire(node, it) {
    const row = rowsOf(it)[0]; if (!doc || !node || !row) return;
    const rid = String(row.order.receiptId), set = mounted.get(rid) || mounted.set(rid, new Set()).get(rid);
    for (const n of [...set]) if (!n.isConnected) set.delete(n);
    node._cpIt = it; set.add(node);
    paintCard(node, it);
  }
  function paintCard(node, it) {
    const row = rowsOf(it)[0], acts = node.querySelector('.rowActions');
    let b = node.querySelector('[data-cp-design]'), strip = node.querySelector('.cpStrip');
    if (!row || !acts || !needsDesign(row, it)) { if (b) b.remove(); if (strip) strip.remove(); return; }
    css();
    const rid = String(row.order.receiptId), att = attachedOf(rid)[0] || null, busy = busyCards.has(rid);
    if (!b) {
      b = el('button', 'btn sm cpOpenBtn'); b.type = 'button'; b.dataset.cpDesign = '';
      b.addEventListener('click', ev => { ev.stopPropagation(); openDesigner(node._cpIt || it); });
      const hint = acts.querySelector('.cuHint'); if (hint) acts.insertBefore(b, hint); else acts.appendChild(b);
    }
    const label = busy ? 'Opening the Charm Designer…' : att ? 'Edit in Charm Designer' : 'Open in Charm Designer';
    if (b.dataset.l !== label) { b.innerHTML = NIB + `<span>${esc(label)}</span>`; b.dataset.l = label; }
    b.setAttribute('aria-busy', String(busy));
    b.title = att ? `Open ${att.name} v${att.version} in the Charm Designer: a save is a new version, attached to this order` : 'Design this order\'s charm in the Charm Designer (start blank, from its dropped .ai / .dxf, or import one there); its Save is attached to this order and offers Send to sheet';
    if (!att) { if (strip) strip.remove(); return; }
    if (!strip) { strip = el('div', 'cpStrip'); strip.setAttribute('role', 'group'); node.appendChild(strip); strip.addEventListener('click', ev => { ev.stopPropagation(); const t = ev.target; const a = attachedOf(rid)[0]; if (!a) return; if (t.closest('[data-cp-strip-send]')) open({ designId: a.id, version: a.version, orderId: rid }); else if (t.closest('[data-cp-strip-pic]')) openDesigner(node._cpIt || it); }); }
    const last = att.sends && att.sends[att.sends.length - 1];
    const sig = [att.id, att.version, last ? last.at : 0].join('|'); if (strip.dataset.sig === sig) return; strip.dataset.sig = sig;
    strip.setAttribute('aria-label', `Design attached to order ${rid}`);
    strip.innerHTML = `<span class="cpStLbl">Charm Designer</span><button type="button" class="cpStPic" data-cp-strip-pic title="Open it in the Charm Designer">${att.svg || (att.thumbUrl ? `<img alt="" src="${esc(att.thumbUrl)}">` : NIB)}</button>`
      + `<span class="cpStMeta"><b>${esc(att.name)}<span class="cpV">v${esc(att.version)}</span></b><span class="dim mono">${(+att.widthMm || 0).toFixed(2)} × ${(+att.heightMm || 0).toFixed(2)} mm</span>`
      + `<span class="dim">${last ? `<span class="ok">On ${esc(last.sheet || codeOf(last.metal) + ' sheet')} × ${last.quantity}</span> · sent by ${esc(last.by)} · ${esc(when(last.at))}` : `Attached${att.by ? ' by ' + esc(att.by) : ''}${att.at ? ' · ' + esc(when(att.at)) : ''} · not on a sheet yet`}</span></span>`
      + `<button type="button" class="btn ${last ? 'ghost' : 'gold'} sm" data-cp-strip-send title="Choose the metal and send this design to its next sheet, marked handmade custom">${last ? 'Send again…' : 'Send to sheet…'}</button>`;
  }
  /** The order window's header (OrderWin paint): "Charm Designer" beside its Send to Sheet for a custom order that takes a design. */
  function orderWin(r, at) {
    if (!doc) return;
    let b = doc.getElementById('owCpDesign');
    if (!r || !at || !at.parentNode || !r.order || !(needsDesign(r) || attachedOf(String(r.order.receiptId)).length)) { if (b) b.hidden = true; return; }
    css();
    if (!b) { b = el('button', 'btn xs cpOwBtn'); b.type = 'button'; b.id = 'owCpDesign'; at.before(b); }
    const rid = String(r.order.receiptId), att = attachedOf(rid)[0];
    b.hidden = false; b.innerHTML = NIB + `<span>${att ? 'Edit design' : 'Charm Designer'}</span>`;
    b.title = att ? `${att.name} v${att.version} is attached to this order: open it in the Charm Designer` : 'Design this custom order\'s charm in the Charm Designer';
    b.onclick = () => { const R = root.Review; let it = null; try { it = R && typeof R.customItemFor === 'function' ? R.customItemFor(r.key, r) : null; } catch (_) { it = null; } openDesigner(it || { rows: [r], row: r }); };
  }

  /* ── the Charm Editor: "Send to sheet…" (item 17: any charm opened there, a master too, sent to the next sheet of a metal, handmade custom) ── */
  function editorTitle() { try { const h = doc.querySelector('.ce-root .ce-id h2'); return h ? h.textContent.trim() : ''; } catch (_) { return ''; } }
  async function fromEditor(ctx) {
    const CE = root.CharmEditor, cur = CE && CE.current; if (!cur || typeof cur.result !== 'function') return null;
    const by = who() || await askWho('Kept with the design you send.', 'send this design to a sheet'); if (!by) return null;
    const known = (ctx && ctx.design && typeof ctx.design === 'object' ? ctx.design : null) || (live && live.design) || null;
    const orderId = (live && live.orderId) || (known && known.orderId) || (ctx && ctx.orderId) || null;
    const base = known && known.id ? Object.assign({}, known, live && live.saved ? live.saved : {}) : live && live.saved ? live.saved : null;
    let saved;
    try { saved = await saveResult(cur.result(), { base, orderId, name: (live && live.name) || (known && known.name) || editorTitle() || 'Custom design', source: (live && live.source) || (known && known.source) || { kind: 'new' }, by }); }
    catch (e) { say(`Not kept: ${String((e && e.message) || e).slice(0, 160)}`, 'bad', 7000); return null; }
    if (!saved) return null;
    const l = live; if (l) l.saved = saved.design;
    const out = await open({ designId: saved.id, version: saved.version, orderId, design: saved.design });
    if (l && out) l.sent = { id: saved.id, version: saved.version, out };
    return out;
  }
  function editorButton() {
    try {
      const save = doc && doc.querySelector('.ce-root [data-act="save"]'); if (!save || save.parentNode.querySelector('[data-cp-ce-send]')) return;
      const b = doc.createElement('button'); b.type = 'button'; b.className = 'ce-btn ce-btn--ghost'; b.dataset.cpCeSend = ''; b.textContent = 'Send to sheet…';
      b.title = 'Keep this design as a new version and put it on the next sheet of a metal you pick, marked handmade custom';
      b.addEventListener('click', ev => { ev.stopPropagation(); fromEditor({}); });
      save.parentNode.insertBefore(b, save);
    } catch (_) { /* the editor draws its own foot */ }
  }
  function hookEditor() {
    const CE = root.CharmEditor; if (!CE || CE.__cpHooked) return; CE.__cpHooked = true;
    (CE.commands || (CE.commands = {})).sendToSheet = { title: 'Send to sheet…', run: ctx => fromEditor(ctx), enabled: () => !!(root.CharmNestDesigns && root.Pool && typeof root.Pool.addCustomPiece === 'function') };
    (CE.onOpen || (CE.onOpen = [])).push(() => editorButton());
  }
  if (doc) hookEditor();

  return {
    open, send, openDesigner, wire, orderWin, attachedOf, needsDesign, goesOnto, fromEditor,
    isOpen: () => !!M, current: () => (M ? { st: M.st, openMs: M.openMs } : null),
    REASONS, METALS,
    _t: { leavesOf, sizeOf, previewSvg, culprits, pickSheet, customIdOf, needsDesign, findOrders, piecesOf, saveResult, designs, attached }
  };
});
