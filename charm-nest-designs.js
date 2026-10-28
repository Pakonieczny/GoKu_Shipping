/* charm-nest-designs.js — THE DESIGN LIBRARY: every charm opened, changed or designed in the Charm Editor, kept with every version (DESIGNS, Paul, 28 Oct 2026).

   Paul: "this charm editor must have a repository of all opened, modified designed pieces, and this repository must be searchable ... Users must be able to pull up their
   previous work and this repository must grow with every new update or change to any charm using the charm editor." · "The system must also track which order each design
   was attached to if there was a given order or if the user simply added a custom piece that was not attached to a given order" · "You need to ensure the sanctity of the
   designed charm. It's exact dimensions and user designed features, and that has to be linked to that specific Custom order." (items 18, 21 and 27; the lead's D6 and D8)

   THE CLOUD (charmNestLibrary.js DESIGNS; this file is the ONE rule both sides use for what a design is, how it is kept and what its hash is):
     Charm_Designs/{id}                 the LATEST version's fields, what a list shows (no faces), and every order and person any version had
     Charm_Designs/{id}/versions/{n}    every version whole: its faces, size, source, order, who and when, hash, picture. Never changed, never deleted (D8: every save is a version)
     a version's faces are kept as path strings (Firestore refuses an array inside an array); a very big one (a traced photograph) is kept in Storage beside its record.
     Its picture: a small PNG in Storage (charmnest/designs/thumbs/<hash>.png; charmnest/sandbox/... in the sandbox), sent inside the one designPut call.
     designPut  { design: { id?, name, faces: { front: FaceDoc, back?: FaceDoc }, widthMm, heightMm, source: { kind: master|import|new|piece|design, sku?, file?, poolId?,
                  designId?, version? }, orderId?, customName? }, thumb? (PNG, base64), by }   → { id, version, at, hash, rev, prev, row }
     designList { q?, by?, orderId?, limit?, cursor?, since? | ifRev? }   → { designs: [row], next, upTo, unchanged?, reset? }
                  since / ifRev: the upTo of the page's last answer ("<epoch>.<n>"): nothing saved since is { unchanged: true } for ONE read; else only the designs saved since.
     designGet  { id, version?, history? }                                 → { design, history? }
     row        { id, name, version, thumbUrl, widthMm, heightMm, orderId, customName, by, at, source, hash, orders, createdBy, createdAt }

   SANCTITY (D6): a version's HASH is the sha-256 of canonical(): its size and, face by face, every part as it is MADE (its path in the face, its role and its paint; a part hidden
   in the editor is not engraved, so it is not in it; a cut line hidden is still cut). leaves() gives those parts, so whoever writes laser files for a design draws exactly
   what the hash covers, and verify() checks a design against its hash. A design's size is its cut line's (checked to 0.1 mm when it has one: what the laser cuts is its size).

   THE PAGE: window.CharmNestDesigns
     put(design) → Promise<{ id, version, at, hash, row }>   checked here by the same rule as the cloud (nothing is sent that would be refused), its picture drawn, one call;
                                                               design.by or the signed-in person; widthMm / heightMm from the cut line when not given
     list(opts)  → Promise<[row]>    the library, newest first, from this computer's index; it asks the cloud only what was saved since (since), at most every 20 s
                                     (opts.force asks now; opts.cloud with q / by / orderId asks the cloud's own search too, for designs older than this computer holds)
     get(id, version?, { history, open }) → Promise<design | null>   a version whole (faces ready for CharmEditor.open), kept in memory: a version never changes;
                                     open: true when it is opened in the editor (logged as designOpen, CNDesignLog)
     search(q, { limit }) → [row]    at once, on this computer's index: every word of q in the name, custom name, order, SKU, file, person or id
     rows() → [row]                  the index as it stands, newest first          onChange(fn) → off    fn({ kind: 'put' | 'refresh' | 'tab', row? })
     setHost({ api, who, sandbox })  the page's cloud and signed-in person (defaults: the page's api(), B.employee, WORKSPACE_SANDBOX)
     useStore({ load, save })        where the index is kept between reloads: THIS file keeps it in memory and localStorage (cn.designs.v1, ":sandbox" in the sandbox,
                                     so a sandbox reset clears it); charm-nest-store.js (the local-first store, not on main yet) replaces it with this one call.
     pure (also in node): clean, encodeFaces, decodeFaces, leaves, canonical, hash, verify, sizeOf, wordsOf, fold, LIMITS
   UMD: window.CharmNestDesigns in the page, module.exports in node (the server and the tests). */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === "object" && module.exports) module.exports = api;
  if (root && root.document) root.CharmNestDesigns = api;
})(typeof self !== "undefined" ? self : typeof globalThis !== "undefined" ? globalThis : this, function (root) {
  "use strict";
  const MM = 25.4 / 72;
  const FACES = ["front", "back"], ROLES = new Set(["cut", "line", "hatch", "art"]), KINDS = new Set(["path", "text", "group"]);
  const SOURCES = new Set(["master", "import", "new", "piece", "design"]);
  /* What one version may hold (the cloud refuses more, and so does put before anything is sent). A version's faces up to INLINE bytes are kept in its record; up to
     `bytes` in Storage beside it (a traced photograph: D2's levels of smooth paths). Coordinates are points, y down, kept to a thousandth of a point (0.35 micron). */
  const LIMITS = { bytes: 4000000, inline: 700000, items: 4000, depth: 8, ops: 300000, coord: 20000, name: 120, text: 400, extra: 16, sizeMm: 1000, sizeTolMm: 0.1, thumb: 98304 };
  const ID = /^[A-Za-z0-9_.:~-]{1,64}$/, DESIGN_ID = /^[A-Za-z0-9_-]{4,64}$/, ORDER_ID = /^(?:\d{4,20}|CUSTOM-\d{8}-\d{1,5})$/, EXTRA = /^[A-Za-z][A-Za-z0-9_]{0,30}$/;
  const KNOWN = new Set(["id", "kind", "role", "source", "name", "hidden", "locked", "subpaths", "d", "children", "stroke", "fill", "lwPt", "weightMm", "even", "evenOdd", "fillRule",
    "text", "fontKey", "sizePt", "align", "lineGap", "tx", "ty", "sx", "sy", "rot", "matrix", "pivot", "bbox", "extra"]);
  const fin = v => typeof v === "number" && Number.isFinite(v);
  const str = (v, n) => (v == null ? "" : String(v)).slice(0, n);
  const r3 = v => { const r = Math.round(v * 1000) / 1000; return Object.is(r, -0) ? 0 : r; };
  const f3 = v => String(r3(v));
  const no = why => ({ error: why });

  /* ── matrices (SVG order [a,b,c,d,e,f], as charm-nest-charm-editor.js) and the tight box of subpaths (Bézier extremes included) ── */
  const I = [1, 0, 0, 1, 0, 0];
  const mul = (m, n) => [m[0] * n[0] + m[2] * n[1], m[1] * n[0] + m[3] * n[1], m[0] * n[2] + m[2] * n[3], m[1] * n[2] + m[3] * n[3], m[0] * n[4] + m[2] * n[5] + m[4], m[1] * n[4] + m[3] * n[5] + m[5]];
  const ap = (m, p) => [m[0] * p[0] + m[2] * p[1] + m[4], m[1] * p[0] + m[3] * p[1] + m[5]];
  const isI = m => m[0] === 1 && m[1] === 0 && m[2] === 0 && m[3] === 1 && m[4] === 0 && m[5] === 0;
  const bez = (a, b, c, d, t) => { const u = 1 - t; return u * u * u * a + 3 * u * u * t * b + 3 * u * t * t * c + t * t * t * d; };
  function cubicExt(a0, a1, a2, a3, add) {
    const d0 = a1 - a0, d1 = a2 - a1, d2 = a3 - a2, A = d0 - 2 * d1 + d2, B = 2 * (d1 - d0), C = d0;
    if (Math.abs(A) < 1e-12) { if (Math.abs(B) > 1e-12) { const t = -C / B; if (t > 0 && t < 1) add(t); } return; }
    const disc = B * B - 4 * A * C; if (disc < 0) return;
    const s = Math.sqrt(disc); for (const t of [(-B + s) / (2 * A), (-B - s) / (2 * A)]) if (t > 0 && t < 1) add(t);
  }
  function boxOf(subs, box) {
    const b = box || [Infinity, Infinity, -Infinity, -Infinity], add = (x, y) => { if (x < b[0]) b[0] = x; if (y < b[1]) b[1] = y; if (x > b[2]) b[2] = x; if (y > b[3]) b[3] = y; };
    for (const sp of subs || []) {
      let cur = null, start = null;
      for (const o of sp) {
        if (o[0] === "m" || o[0] === "l") { add(o[1][0], o[1][1]); cur = o[1]; if (o[0] === "m") start = o[1]; }
        else if (o[0] === "c") {
          const p1 = o[1], p2 = o[2], p3 = o[3], a = cur || p1; add(p3[0], p3[1]);
          const at = t => add(bez(a[0], p1[0], p2[0], p3[0], t), bez(a[1], p1[1], p2[1], p3[1], t));
          cubicExt(a[0], p1[0], p2[0], p3[0], at); cubicExt(a[1], p1[1], p2[1], p3[1], at); cur = p3;
        } else if (o[0] === "h") cur = start;
      }
    }
    return b;
  }
  const realBox = b => (b && b[0] <= b[2] && b[1] <= b[3] ? b : null);
  /** An item's own transform when it does not state its matrix: T(c + t) · R(rot) · S(sx, sy) · T(−c) about the centre c of its geometry's tight box (the editor's rule). */
  function matrixOf(it) {
    const tx = fin(it.tx) ? it.tx : 0, ty = fin(it.ty) ? it.ty : 0, sx = fin(it.sx) ? it.sx : 1, sy = fin(it.sy) ? it.sy : 1, rot = fin(it.rot) ? it.rot : 0;
    if (!tx && !ty && sx === 1 && sy === 1 && !rot) return I;
    const b = realBox(boxOf(it.subpaths)), cx = b ? (b[0] + b[2]) / 2 : 0, cy = b ? (b[1] + b[3]) / 2 : 0, th = rot * Math.PI / 180, c = Math.cos(th), s = Math.sin(th);
    const a = c * sx, bb = s * sx, cc = -s * sy, d = c * sy;
    return [a, bb, cc, d, cx + tx - (a * cx + cc * cy), cy + ty - (bb * cx + d * cy)];
  }
  const defaultTransform = it => !(fin(it.tx) && it.tx) && !(fin(it.ty) && it.ty) && (!fin(it.sx) || it.sx === 1) && (!fin(it.sy) || it.sy === 1) && !(fin(it.rot) && it.rot);

  /* ── paths: the editor's subpaths ([["m",[x,y]], ["l",[x,y]], ["c",p1,p2,p3], ["h"]] …) and the string a record keeps ("M1 2L3 4C…Z", charm-nest-art-edit.js's vocabulary) ── */
  const isPt = p => Array.isArray(p) && p.length === 2 && fin(p[0]) && fin(p[1]) && Math.abs(p[0]) <= LIMITS.coord && Math.abs(p[1]) <= LIMITS.coord;
  /** The subpaths checked and rounded, or null when they are not a path this app writes. Counts the operators into stats.ops. */
  function cleanSubs(subs, stats) {
    if (!Array.isArray(subs)) return null;
    const out = [];
    for (const sp of subs) {
      if (!Array.isArray(sp) || !sp.length || !Array.isArray(sp[0]) || sp[0][0] !== "m") return null;
      const o2 = [];
      for (const o of sp) {
        if (!Array.isArray(o)) return null;
        const k = o[0];
        if (k === "m" || k === "l") { if (o.length !== 2 || !isPt(o[1])) return null; o2.push([k, [r3(o[1][0]), r3(o[1][1])]]); }
        else if (k === "c") { if (o.length !== 4 || !isPt(o[1]) || !isPt(o[2]) || !isPt(o[3])) return null; o2.push(["c", [r3(o[1][0]), r3(o[1][1])], [r3(o[2][0]), r3(o[2][1])], [r3(o[3][0]), r3(o[3][1])]]); }
        else if (k === "h") { if (o.length !== 1) return null; o2.push(["h"]); }
        else return null;
      }
      stats.ops += o2.length; out.push(o2);
    }
    return out;
  }
  function toD(subs) {
    let d = "";
    for (const sp of subs || []) for (const o of sp) {
      if (o[0] === "m") d += "M" + f3(o[1][0]) + " " + f3(o[1][1]);
      else if (o[0] === "l") d += "L" + f3(o[1][0]) + " " + f3(o[1][1]);
      else if (o[0] === "c") d += "C" + f3(o[1][0]) + " " + f3(o[1][1]) + " " + f3(o[2][0]) + " " + f3(o[2][1]) + " " + f3(o[3][0]) + " " + f3(o[3][1]);
      else d += "Z";
    }
    return d;
  }
  function fromD(d) {
    const tok = String(d || "").match(/[MLCZ]|[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?/g) || [], subs = []; let sp = null, i = 0;
    const pt = () => [+tok[i++], +tok[i++]];
    while (i < tok.length) {
      const c = tok[i++];
      if (c === "M") { sp = [["m", pt()]]; subs.push(sp); }
      else if (!sp) return [];
      else if (c === "L") sp.push(["l", pt()]);
      else if (c === "C") sp.push(["c", pt(), pt(), pt()]);
      else if (c === "Z") sp.push(["h"]);
    }
    return subs;
  }
  /** A paint: [r, g, b] from 0 to 1, a CSS colour word or #hex, null for none; undefined when it is none of these. */
  function cleanPaint(v) {
    if (v == null || v === false || v === "none") return null;
    if (Array.isArray(v)) return v.length === 3 && v.every(x => fin(x) && x >= 0 && x <= 1) ? v.map(r3) : undefined;
    if (typeof v === "string" && /^(?:#[0-9a-f]{3,8}|[a-z]{3,20})$/i.test(v.trim())) return v.trim().toLowerCase();
    return undefined;
  }
  const paintKey = p => (p == null ? "" : Array.isArray(p) ? p.map(f3).join(",") : String(p));

  /* ── clean: a design checked whole, as charmNestLibrary.js cleanArt checks a piece's edit (the page asks it before sending; the cloud again before keeping) ── */
  function cleanItem(x, depth, seen, stats, face) {
    const at = (x && x.id != null ? `${face} · ${String(x.id).slice(0, 40)}` : `${face} · a part`) + ": ";
    if (!x || typeof x !== "object" || Array.isArray(x)) return no(at + "not a part");
    if (++stats.items > LIMITS.items) return no(`a design has at most ${LIMITS.items} parts`);
    if (depth > LIMITS.depth) return no(at + `groups go at most ${LIMITS.depth} deep`);
    const id = String(x.id == null ? "" : x.id); if (!ID.test(id)) return no(at + "its id is not a short name");
    if (seen.has(id)) return no(at + "the id is used twice on this face"); seen.add(id);
    const kind = x.kind == null ? (Array.isArray(x.children) ? "group" : "path") : String(x.kind); if (!KINDS.has(kind)) return no(at + "its kind must be path, text or group (a picture is traced to paths first)");
    const role = x.role == null && kind === "group" ? "art" : String(x.role || ""); if (!ROLES.has(role)) return no(at + "its role must be cut, line, hatch or art");
    const out = { id, kind, role };
    if (x.source != null) out.source = str(x.source, 20);
    if (x.name != null && x.name !== "") out.name = str(x.name, LIMITS.name);
    if (x.hidden === true) out.hidden = true;
    if (x.locked === true) out.locked = true;
    for (const k of ["tx", "ty", "sx", "sy", "rot"]) if (x[k] != null) { if (!fin(x[k]) || Math.abs(x[k]) > 1e6) return no(at + `its ${k} is not a number`); out[k] = x[k]; }
    if ((out.sx != null && Math.abs(out.sx) < 1e-9) || (out.sy != null && Math.abs(out.sy) < 1e-9)) return no(at + "it is scaled to nothing");
    const nums = (v, n) => Array.isArray(v) && v.length === n && v.every(q => fin(q) && Math.abs(q) <= 1e6);
    if (x.matrix != null) { if (!nums(x.matrix, 6)) return no(at + "its matrix is six numbers"); out.matrix = x.matrix.slice(); }
    if (x.pivot != null && nums(x.pivot, 2)) out.pivot = x.pivot.slice();
    if (x.bbox != null && nums(x.bbox, 4)) out.bbox = x.bbox.slice();
    for (const k of ["stroke", "fill"]) if (x[k] !== undefined) { const p = cleanPaint(x[k]); if (p === undefined) return no(at + `its ${k} is not a colour ([r, g, b] from 0 to 1)`); out[k] = p; }
    if (x.lwPt != null) { if (!fin(x.lwPt) || x.lwPt < 0 || x.lwPt > 20) return no(at + "its line width is 0 to 20 pt"); out.lwPt = r3(x.lwPt); }
    if (x.weightMm != null) { if (!fin(x.weightMm) || Math.abs(x.weightMm) > 5) return no(at + "its weight is −5 to 5 mm"); out.weightMm = r3(x.weightMm); }
    if (x.even === true) out.even = true;
    if (x.evenOdd === true) out.evenOdd = true;
    if (x.fillRule === "evenodd" || x.fillRule === "nonzero") out.fillRule = x.fillRule;
    if (kind === "text") {
      out.text = str(x.text, LIMITS.text);
      if (x.fontKey != null) out.fontKey = str(x.fontKey, 40);
      if (x.sizePt != null) { if (!fin(x.sizePt) || x.sizePt <= 0 || x.sizePt > 2000) return no(at + "its size is not a size"); out.sizePt = x.sizePt; }
      if (x.align != null) out.align = ["left", "center", "right"].includes(x.align) ? x.align : "left";
      if (x.lineGap != null && fin(x.lineGap) && x.lineGap > 0 && x.lineGap <= 10) out.lineGap = x.lineGap;
    }
    if (kind === "group") {
      if (!Array.isArray(x.children)) return no(at + "a group has its parts");
      if (!out.matrix && !defaultTransform(out)) return no(at + "a group that is moved, turned or scaled comes with its matrix (as the Charm Editor hands it back)");
      out.children = [];
      for (const c of x.children) { const r = cleanItem(c, depth + 1, seen, stats, face); if (r.error) return r; out.children.push(r.item); }
      // a group placed by a matrix alone (no tx … rot: an importer's): the matrix is put into its parts, so the editor opens it where it is drawn and hashed
      if (out.matrix && defaultTransform(out)) { if (!isI(out.matrix)) { const e = pushDown(out.children, out.matrix); if (e) return no(at + e); } plain(out); }
    } else {
      // a text comes with its outlines (the editor's result: already placed in the face); a path with its own geometry
      if (x.subpaths == null && typeof x.d === "string") x = Object.assign({}, x, { subpaths: fromD(x.d) });
      const subs = cleanSubs(x.subpaths == null && kind === "text" ? null : x.subpaths, stats);
      if (!subs) return no(at + (kind === "text" ? "a text comes with its outlines (the Charm Editor's result has them)" : "its path is not m / l / c / h with numbers within ±" + LIMITS.coord + " pt"));
      if (kind === "path" && !subs.length) return no(at + "its path is empty");
      out.subpaths = subs;
      // the editor places a part by tx … rot (about its tight box) and states the matrix they make: a matrix alone is put into the geometry (so the editor opens it where it is
      // drawn and hashed); a matrix that is not what tx … rot make is refused (which of the two is the part?)
      if (kind === "path" && out.matrix) {
        if (defaultTransform(out)) { if (!isI(out.matrix)) out.subpaths = bake(out.subpaths, out.matrix); plain(out); }
        else if (!agrees(out.matrix, matrixOf(out))) return no(at + "its matrix is not what its move, turn and scale (tx … rot) make");
      }
    }
    // what else an item carries travels with it (the editor's rule: layer, origin, a member id ...): short plain values only
    let extra = 0;
    for (const k of Object.keys(x)) {
      if (KNOWN.has(k) || k[0] === "_" || !EXTRA.test(k)) continue;
      const v = x[k]; if (!(typeof v === "boolean" || (typeof v === "string" && v.length <= 200) || fin(v))) continue;
      if (++extra > LIMITS.extra) break;
      out[k] = v;
    }
    return { item: out };
  }
  const near = (a, b, tol) => Math.abs(a - b) <= tol;
  const agrees = (m, n) => [0, 1, 2, 3].every(i => near(m[i], n[i], 1e-6 * Math.max(1, Math.abs(n[i])))) && near(m[4], n[4], 0.01) && near(m[5], n[5], 0.01);   // (a box's centre of rounded points: a hundredth of a point)
  const plain = it => { for (const k of ["tx", "ty", "sx", "sy", "rot", "matrix", "pivot", "bbox"]) delete it[k]; return it; };
  const bake = (subs, m) => transform(subs, m).map(sp => sp.map(o => (o[0] === "h" ? o : [o[0]].concat(o.slice(1).map(p => [r3(p[0]), r3(p[1])])))));
  /** A group's matrix M put into its parts (cleaned): each path's geometry placed by M · its own, each group's parts in turn. Words cannot be (their tx … rot are the group's). */
  function pushDown(items, M) {
    for (const c of items) {
      if (c.kind === "text") return "words in a group placed by a matrix alone: move the group by tx / ty (as the Charm Editor does)";
      const W = mul(M, Array.isArray(c.matrix) ? c.matrix : matrixOf(c));
      if (c.kind === "group") { const e = pushDown(c.children, W); if (e) return e; } else c.subpaths = bake(c.subpaths, W);
      plain(c);
    }
    return null;
  }
  function cleanFace(fd, face, stats) {
    if (!fd || typeof fd !== "object" || Array.isArray(fd)) return no(`the ${face} is not a face`);
    if (!Array.isArray(fd.items)) return no(`the ${face} has no parts list`);
    let frame = null;
    if (fd.frame != null) {
      const f = fd.frame; if (!f || ![f.x0, f.y0, f.wPt, f.hPt].every(v => fin(v) && Math.abs(v) <= LIMITS.coord) || !(f.wPt > 0 && f.hPt > 0)) return no(`the ${face}'s frame is not a box`);
      frame = { x0: r3(f.x0), y0: r3(f.y0), wPt: r3(f.wPt), hPt: r3(f.hPt) };
    }
    const seen = new Set(), items = [];
    for (const it of fd.items) { const r = cleanItem(it, 0, seen, stats, face); if (r.error) return r; items.push(r.item); }
    return { face: { face, unitPt: true, frame, items } };
  }
  const cleanOrder = v => { const s = String(v == null ? "" : v).trim(); return ORDER_ID.test(s) ? s : null; };
  /** A design checked whole: { design, stored, bytes, ops, items } or { error }. `design` is the wire form (subpaths), rounded; `stored` its faces as a record keeps them. */
  function clean(d) {
    if (!d || typeof d !== "object" || Array.isArray(d)) return no("the design is missing");
    const stats = { ops: 0, items: 0 };
    const id = d.id == null || d.id === "" ? null : String(d.id); if (id != null && !DESIGN_ID.test(id)) return no("the design's id is not a short name (letters, digits, - and _)");
    const faces = d.faces; if (!faces || typeof faces !== "object" || Array.isArray(faces)) return no("the design has no faces");
    for (const k of Object.keys(faces)) if (!FACES.includes(k) && faces[k] != null) return no(`a face is the front or the back (not "${String(k).slice(0, 20)}")`);
    const out = {};
    for (const f of FACES) { if (faces[f] == null) continue; const r = cleanFace(faces[f], f, stats); if (r.error) return r; out[f] = r.face; }
    if (!out.front && !out.back) return no("the design has no faces");
    if (stats.ops > LIMITS.ops) return no(`the design has ${stats.ops} path operators; one design has at most ${LIMITS.ops}`);
    const w = +d.widthMm, h = +d.heightMm;
    if (!(fin(w) && fin(h) && w > 0 && h > 0 && w <= LIMITS.sizeMm && h <= LIMITS.sizeMm)) return no(`the design's size (widthMm × heightMm) is missing or not a size (up to ${LIMITS.sizeMm} mm)`);
    // the size is the cut line's: what the laser cuts is the piece (D6: its exact dimensions)
    const cut = cutBoxMm(out);
    if (cut && (Math.abs(cut.widthMm - w) > LIMITS.sizeTolMm || Math.abs(cut.heightMm - h) > LIMITS.sizeTolMm)) return no(`the design's size says ${r3(w)} × ${r3(h)} mm but its cut line measures ${r3(cut.widthMm)} × ${r3(cut.heightMm)} mm`);
    const s = d.source && typeof d.source === "object" && !Array.isArray(d.source) ? d.source : {};
    const kind = String(s.kind || ""); if (!SOURCES.has(kind)) return no("the design's source is master, import, new, piece or design");
    const source = { kind };
    if (s.sku != null && s.sku !== "") source.sku = str(s.sku, 60);
    if (s.file != null && s.file !== "") source.file = str(s.file, 200);
    if (s.poolId != null && s.poolId !== "") { const p = String(s.poolId); if (!/^[\w\-~]{4,80}$/.test(p)) return no("the source's poolId is not a piece's id"); source.poolId = p; }
    if (s.designId != null && s.designId !== "") { const p = String(s.designId); if (!DESIGN_ID.test(p)) return no("the source's designId is not a design's id"); source.designId = p; }
    if (s.version != null && s.version !== "") { if (!(Number.isInteger(+s.version) && +s.version > 0)) return no("the source's version is not a version"); source.version = +s.version; }
    const orderId = d.orderId == null || d.orderId === "" ? null : cleanOrder(d.orderId);
    if (d.orderId != null && d.orderId !== "" && !orderId) return no("the design's order is an order number or CUSTOM-<yyyymmdd>-<n>");
    const name = str(d.name, LIMITS.name).trim() || source.sku || "Untitled design";
    const design = { ...(id ? { id } : {}), name, faces: out, widthMm: r3(w), heightMm: r3(h), source, orderId, customName: str(d.customName, 80).trim() || null };
    const stored = encodeFaces(out), bytes = JSON.stringify(stored).length;
    if (bytes > LIMITS.bytes) return no(`the design is ${Math.ceil(bytes / 1000)} KB; one version is at most ${LIMITS.bytes / 1000} KB`);
    return { design, stored, bytes, ops: stats.ops, items: stats.items };
  }

  /* ── kept: the faces with each path as a string (no array inside an array), and back ── */
  function encodeItem(it) {
    const o = Object.assign({}, it);
    if (o.subpaths) { o.d = toD(o.subpaths); delete o.subpaths; }
    if (o.children) o.children = o.children.map(encodeItem);
    return o;
  }
  function decodeItem(it) {
    const o = Object.assign({}, it);
    if (typeof o.d === "string") { o.subpaths = fromD(o.d); delete o.d; }
    if (Array.isArray(o.children)) o.children = o.children.map(decodeItem);
    return o;
  }
  const encodeFaces = faces => Object.fromEntries(FACES.filter(f => faces && faces[f]).map(f => [f, Object.assign({}, faces[f], { items: (faces[f].items || []).map(encodeItem) })]));
  const decodeFaces = faces => Object.fromEntries(FACES.filter(f => faces && faces[f]).map(f => [f, Object.assign({}, faces[f], { items: (faces[f].items || []).map(decodeItem) })]));

  /* ── what is made: every part placed in its face (the laser's view of the design) ── */
  const transform = (subs, m) => (isI(m) ? subs : subs.map(sp => sp.map(o => (o[0] === "h" ? ["h"] : o[0] === "c" ? ["c", ap(m, o[1]), ap(m, o[2]), ap(m, o[3])] : [o[0], ap(m, o[1])]))));
  /** { front: [leaf], back: [leaf] }: leaf { id, kind, role, fill, stroke, lwPt, even, subpaths (in the face, pt, y down) }. A part hidden in the editor is not engraved
      (left out); a cut line hidden is still cut. A text's outlines are already in the face (the editor's result); a path is placed by its matrix (or its tx…rot). */
  function leaves(faces) {
    const out = {};
    const walk = (items, P, off, list) => {
      for (const it of items || []) {
        if (!it) continue;
        const hid = off || it.hidden === true;
        if (it.kind === "group") { walk(it.children, mul(P, Array.isArray(it.matrix) ? it.matrix : matrixOf(it)), hid, list); continue; }
        if (hid && it.role !== "cut") continue;
        const subs = it.subpaths || (typeof it.d === "string" ? fromD(it.d) : []);
        const placed = it.kind === "text" ? subs : transform(subs, mul(P, Array.isArray(it.matrix) ? it.matrix : matrixOf(Object.assign({}, it, { subpaths: subs }))));
        list.push({ id: it.id, kind: it.kind || "path", role: it.role, fill: it.fill == null ? null : it.fill, stroke: it.stroke == null ? null : it.stroke, lwPt: it.lwPt == null ? null : it.lwPt, even: !!(it.even || it.evenOdd || it.fillRule === "evenodd"), subpaths: placed });
      }
    };
    for (const f of FACES) if (faces && faces[f]) walk(faces[f].items, I, false, out[f] = []);
    return out;
  }
  /** The text the hash is taken of: the size and, face by face, every part as made (role, paint, line width, fill rule, its path in the face to a thousandth of a point). */
  function canonical(design) {
    const L = leaves(design.faces);
    return JSON.stringify(["cn-design-1", f3(+design.widthMm), f3(+design.heightMm), FACES.map(f => [f, (L[f] || []).map(l => [l.role, paintKey(l.fill), paintKey(l.stroke), l.lwPt == null ? "" : f3(l.lwPt), l.even ? 1 : 0, toD(l.subpaths)])])]);
  }
  /** The cut line's box in mm, of the front when it has one, else the back: { widthMm, heightMm } or null (no cut line). */
  function cutBoxMm(faces) {
    const L = leaves(faces);
    for (const f of FACES) {
      const cuts = (L[f] || []).filter(l => l.role === "cut"); if (!cuts.length) continue;
      let b = null; for (const l of cuts) b = boxOf(l.subpaths, b || undefined);
      if (realBox(b)) return { widthMm: (b[2] - b[0]) * MM, heightMm: (b[3] - b[1]) * MM, face: f };
    }
    return null;
  }
  /** A design's size when it is not given: its cut line's, else its frame's. */
  function sizeOf(faces) {
    const c = cutBoxMm(faces || {}); if (c) return { widthMm: r3(c.widthMm), heightMm: r3(c.heightMm) };
    for (const f of FACES) { const fr = faces && faces[f] && faces[f].frame; if (fr && fr.wPt > 0 && fr.hPt > 0) return { widthMm: r3(fr.wPt * MM), heightMm: r3(fr.hPt * MM) }; }
    return null;
  }

  /* ── sha-256: node's crypto, the browser's crypto.subtle, or this small one (a page opened from a file) ── */
  function sha256js(text) {
    const b = typeof TextEncoder === "function" ? new TextEncoder().encode(text) : Uint8Array.from(unescape(encodeURIComponent(text)), c => c.charCodeAt(0));
    const K = [], H = [1779033703, 3144134277, 1013904242, 2773480762, 1359893119, 2600822924, 528734635, 1541459225];
    for (let i = 0, n = 2; i < 64; n++) { let p = true; for (let j = 2; j * j <= n; j++) if (n % j === 0) { p = false; break; } if (p) K[i++] = (Math.pow(n, 1 / 3) % 1) * 4294967296 | 0; }
    const len = b.length, words = new Uint32Array(((len + 9 + 63) >> 6) << 4);
    for (let i = 0; i < len; i++) words[i >> 2] |= b[i] << (24 - (i % 4) * 8);
    words[len >> 2] |= 0x80 << (24 - (len % 4) * 8); words[words.length - 1] = len * 8; words[words.length - 2] = Math.floor(len / 536870912);
    const w = new Uint32Array(64), rr = (x, n) => (x >>> n) | (x << (32 - n));
    for (let o = 0; o < words.length; o += 16) {
      for (let i = 0; i < 64; i++) w[i] = i < 16 ? words[o + i] : (rr(w[i - 2], 17) ^ rr(w[i - 2], 19) ^ (w[i - 2] >>> 10)) + w[i - 7] + (rr(w[i - 15], 7) ^ rr(w[i - 15], 18) ^ (w[i - 15] >>> 3)) + w[i - 16] | 0;
      let [a, bb, c, d, e, f, g, h] = H;
      for (let i = 0; i < 64; i++) {
        const t1 = h + (rr(e, 6) ^ rr(e, 11) ^ rr(e, 25)) + ((e & f) ^ (~e & g)) + K[i] + w[i] | 0, t2 = (rr(a, 2) ^ rr(a, 13) ^ rr(a, 22)) + ((a & bb) ^ (a & c) ^ (bb & c)) | 0;
        h = g; g = f; f = e; e = d + t1 | 0; d = c; c = bb; bb = a; a = t1 + t2 | 0;
      }
      H[0] = H[0] + a | 0; H[1] = H[1] + bb | 0; H[2] = H[2] + c | 0; H[3] = H[3] + d | 0; H[4] = H[4] + e | 0; H[5] = H[5] + f | 0; H[6] = H[6] + g | 0; H[7] = H[7] + h | 0;
    }
    return H.map(x => (x >>> 0).toString(16).padStart(8, "0")).join("");
  }
  const nodeCrypto = (() => { try { return typeof module === "object" && module.exports && typeof require === "function" && !(root && root.document) ? require("crypto") : null; } catch (_) { return null; } })();
  /** sha-256 hex of a text, synchronously (node), else null. */
  const sha256Sync = text => (nodeCrypto ? nodeCrypto.createHash("sha256").update(text, "utf8").digest("hex") : null);
  async function sha256(text) {
    const s = sha256Sync(text); if (s) return s;
    const subtle = root && root.crypto && root.crypto.subtle;
    if (subtle && typeof TextEncoder === "function") { try { const buf = await subtle.digest("SHA-256", new TextEncoder().encode(text)); return [...new Uint8Array(buf)].map(x => x.toString(16).padStart(2, "0")).join(""); } catch (_) { /* below */ } }
    return sha256js(text);
  }
  /** The geometry hash of a design (wire form, as clean() gives it, or as designGet answers it). */
  const hash = design => sha256(canonical(design));
  /** Whether a design (as designGet answers it) is still exactly what was hashed when it was saved. */
  async function verify(design) { return !!(design && design.hash) && (await hash(design)) === design.hash; }

  /* ── words: what a search finds a design by ── */
  const fold = s => String(s == null ? "" : s).normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase().replace(/[^a-z0-9#\-_. ]+/g, " ").replace(/\s+/g, " ").trim();
  const hayOf = r => fold([r.name, r.customName, r.orderId, (r.orders || []).join(" "), r.source && r.source.sku, r.source && r.source.file, r.source && r.source.poolId, r.by, r.createdBy, r.id].filter(Boolean).join(" | "));
  /** The words the cloud's own search keeps for a design (whole words: one is asked of Firestore, the rest are matched in the answer). */
  const wordsOf = r => [...new Set(fold(hayOf(r)).split(/[^a-z0-9]+/).filter(w => w && w.length <= 32))].slice(0, 60);

  /* ═══ THE PAGE ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════ */
  const doc = root && root.document ? root.document : null;
  let host = {};
  const hostApi = () => host.api || (typeof root.api === "function" ? root.api : null);
  const who = () => { try { if (host.who) return String(host.who() || "").trim(); } catch (_) { return ""; } try { return String((root.B && root.B.employee) || "").trim(); } catch (_) { return ""; } };
  const inSandbox = () => { try { if (host.sandbox != null) return typeof host.sandbox === "function" ? !!host.sandbox() : !!host.sandbox; } catch (_) { return false; } try { return typeof WORKSPACE_SANDBOX !== "undefined" && !!WORKSPACE_SANDBOX; } catch (_) { return false; } };   // eslint-disable-line no-undef
  async function call(body, opts) {
    const a = hostApi(); if (!a) throw new Error("the design library has no connection to the cloud on this page");
    return a("charmNestLibrary", body, Object.assign({ quiet: true }, opts || {}));
  }
  const log = (kind, data) => { try { if (root.CNDesignLog && typeof root.CNDesignLog.event === "function") root.CNDesignLog.event(kind, data); } catch (_) { /* the log is never in the way */ } };

  /* ── where the index is kept between reloads: memory and localStorage here (useStore swaps it for charm-nest-store.js's in one call) ── */
  const KEEP_ROWS = 2000;
  const lsKey = () => "cn.designs.v1" + (inSandbox() ? ":sandbox" : "");
  const localStore = {
    load() { try { const t = root.localStorage && root.localStorage.getItem(lsKey()); const v = t ? JSON.parse(t) : null; return v && v.v === 1 && Array.isArray(v.rows) ? v : null; } catch (_) { return null; } },
    save(state) {
      // newest first; a computer short of room keeps fewer (the cloud holds them all: the next list asks for what is missing)
      let rows = state.rows.slice(0, KEEP_ROWS);
      for (let i = 0; i < 4; i++) {
        try { root.localStorage && root.localStorage.setItem(lsKey(), JSON.stringify({ v: 1, upTo: rows.length === state.rows.length ? state.upTo : null, at: Date.now(), rows })); return true; }
        catch (_) { rows = rows.slice(0, Math.floor(rows.length / 2)); }
      }
      return false;
    }
  };
  let store = localStore;
  const idx = { rows: new Map(), list: null, upTo: null, checkedAt: 0, loaded: false, world: null };
  const full = new Map();   // "id@version" → the design whole (a version never changes)
  const FULL_MAX = 40, FRESH_MS = 20000, PART = 500, LOCAL_MAX = 5000;
  const listeners = new Set();
  const tell = ev => { for (const fn of [...listeners]) { try { fn(ev); } catch (e) { console.warn("design library: listener", e); } } };
  function ensureLoaded() {
    const world = inSandbox() ? "sandbox" : "production";
    if (idx.loaded && idx.world === world) return;
    idx.rows.clear(); idx.list = null; idx.upTo = null; idx.checkedAt = 0; idx.loaded = true; idx.world = world;
    const v = store.load && store.load(); if (!v) return;
    for (const r of v.rows || []) if (r && r.id) idx.rows.set(r.id, r);
    idx.upTo = v.upTo || null;
  }
  let saveTimer = 0;
  function persist() { if (saveTimer) return; saveTimer = setTimeout(() => { saveTimer = 0; try { store.save({ rows: rowsNow(), upTo: idx.upTo }); } catch (e) { console.warn("design library: keep", e); } }, 0); }
  function rowsNow() { if (!idx.list) idx.list = [...idx.rows.values()].sort((a, b) => (b.at || 0) - (a.at || 0) || String(a.id).localeCompare(String(b.id))); return idx.list; }
  /** Puts a row in the index when it is newer than the one held. → whether it changed */
  function upsert(r) {
    if (!r || !r.id) return false;
    const cur = idx.rows.get(r.id); if (cur && (cur.version || 0) > (r.version || 0)) return false;
    if (cur && cur.version === r.version && cur.at === r.at && cur.thumbUrl === r.thumbUrl) return false;
    idx.rows.set(r.id, r); idx.list = null; return true;
  }
  // another tab of this computer saved or refreshed: its index is read again (no call)
  if (root && typeof root.addEventListener === "function") root.addEventListener("storage", e => { if (!e || e.key !== lsKey() || !idx.loaded) return; idx.loaded = false; ensureLoaded(); tell({ kind: "tab" }); });

  let refreshing = null;
  /** The cloud asked what was saved since this computer's index (one read when nothing was); the whole library, newest first, the first time. */
  function refresh(opts) {
    ensureLoaded();
    if (refreshing) return refreshing;
    if (!(opts && opts.force) && idx.checkedAt && Date.now() - idx.checkedAt < FRESH_MS) return Promise.resolve(rowsNow());
    refreshing = (async () => {
      let since = idx.upTo, cursor = null, upTo = null, changed = false;
      for (let part = 0; part < 40; part++) {
        const r = await call(Object.assign({ op: "designList", limit: PART }, since ? { since } : {}, cursor != null ? { cursor } : {}));
        if (r.reset) { idx.rows.clear(); idx.list = null; changed = true; since = null; cursor = null; upTo = null; continue; }   // (the cloud's counter began again: a sandbox reset)
        if (upTo == null) upTo = r.upTo || null;
        if (r.unchanged) break;
        for (const row of r.designs || []) if (upsert(row)) changed = true;
        if (r.next == null || (!since && idx.rows.size >= LOCAL_MAX)) break;
        cursor = r.next;
      }
      if (upTo) idx.upTo = upTo;
      idx.checkedAt = Date.now();
      if (changed) { persist(); tell({ kind: "refresh" }); } else if (upTo) persist();
      return rowsNow();
    })().finally(() => { refreshing = null; });
    return refreshing;
  }
  /** Instant search of this computer's index: every word of q somewhere in a design's words; a name that starts with the first word, or an order / SKU / id that is it, first. */
  function search(q, opts) {
    ensureLoaded();
    const limit = (opts && opts.limit) || 200, rows = rowsNow(), terms = fold(q).split(" ").filter(Boolean);
    if (!terms.length) return rows.slice(0, limit);
    const hits = [];
    for (const r of rows) {
      const h = r._hay || (Object.defineProperty(r, "_hay", { value: hayOf(r), enumerable: false, configurable: true }), r._hay);
      if (terms.every(t => h.includes(t))) hits.push(r);
    }
    const t0 = terms[0], score = r => (fold(r.orderId) === t0 || fold(r.source && r.source.sku) === t0 || r.id === t0 ? 2 : 0) + (fold(r.name).startsWith(t0) || fold(r.customName).startsWith(t0) ? 1 : 0);
    if (hits.length > 1) { const s = new Map(hits.map(r => [r, score(r)])); hits.sort((a, b) => s.get(b) - s.get(a)); }   // (stable: newest first within a score)
    return hits.slice(0, limit);
  }
  async function list(opts) {
    opts = opts || {};
    if (opts.cloud && (opts.q || opts.by || opts.orderId)) {
      const r = await call({ op: "designList", ...(opts.q ? { q: String(opts.q) } : {}), ...(opts.by ? { by: String(opts.by) } : {}), ...(opts.orderId ? { orderId: String(opts.orderId) } : {}), limit: opts.limit || 200 });
      let changed = false; for (const row of r.designs || []) if (upsert(row)) changed = true;
      if (changed) { persist(); tell({ kind: "refresh" }); }
      return r.designs || [];
    }
    if (!opts.local) { try { await refresh(opts); } catch (e) { if (!idx.rows.size) throw e; console.warn("design library: the cloud was not asked", e); } }
    let rows = opts.q ? search(opts.q, { limit: LOCAL_MAX }) : rowsNow().slice();
    if (opts.by) rows = rows.filter(r => fold(r.by) === fold(opts.by) || fold(r.createdBy) === fold(opts.by));
    if (opts.orderId) rows = rows.filter(r => r.orderId === String(opts.orderId) || (r.orders || []).includes(String(opts.orderId)));
    return rows.slice(0, opts.limit || rows.length);
  }
  function keepFull(d) { const k = d.id + "@" + d.version; full.delete(k); full.set(k, d); while (full.size > FULL_MAX) full.delete(full.keys().next().value); }
  const logOpen = d => log("designOpen", { designId: d.id, version: d.version, name: d.name, sku: (d.source && d.source.sku) || null, orderId: d.orderId || null, customName: d.customName || null, faces: FACES.filter(f => d.faces && d.faces[f]) });
  /** A version whole (the latest without `version`): faces ready for CharmEditor.open. Read-only: it is the copy this page keeps. opts.open: it is being opened in the
      editor (the employee log's designOpen); opts.history: the list of its versions too. */
  async function get(id, version, opts) {
    const v = +version > 0 ? +version : 0, hit = v ? full.get(id + "@" + v) : null;
    if (hit && !(opts && opts.history)) { if (opts && opts.open) logOpen(hit); return hit; }
    const r = await call({ op: "designGet", id: String(id), ...(v ? { version: v } : {}), ...(opts && opts.history ? { history: true } : {}) });
    if (!r || !r.design) return null;
    keepFull(r.design);
    if (opts && opts.open) logOpen(r.design);
    ensureLoaded(); if (upsert(rowOf(r.design))) { persist(); tell({ kind: "refresh" }); }
    return opts && opts.history ? Object.assign({}, r.design, { history: r.history || [] }) : r.design;
  }
  const ROW = ["id", "name", "version", "thumbUrl", "widthMm", "heightMm", "orderId", "customName", "by", "at", "source", "hash", "orders", "createdBy", "createdAt"];
  const rowOf = d => Object.fromEntries(ROW.filter(k => d[k] !== undefined).map(k => [k, d[k]]));

  /* ── the picture: the charm's body in a light gold, its engraving in ink, a hole a hole (every cut line in one even-odd fill) ── */
  const THUMB_PX = 192, INK = "#2f2a24", BODY = "#f4ecd9", EDGE = "#a8823a";
  const paintFill = l => (l.kind === "text" ? !(l.role === "line" || l.role === "cut") : l.role === "hatch" ? true : l.role === "art" ? (!!l.fill || !l.stroke) : false);   // (the editor's rule)
  const paintStroke = l => (l.kind === "text" ? (l.role === "line" || l.role === "cut") : (l.role === "cut" || l.role === "line") ? true : l.role === "art" ? !!l.stroke : false);
  const inkOf = p => (Array.isArray(p) && p[0] === p[1] && p[1] === p[2] ? `rgb(${Math.round(p[0] * 255)},${Math.round(p[0] * 255)},${Math.round(p[0] * 255)})` : INK);   // (a trace's grey keeps its grey, D2; a laser pen's blue or red is drawn as ink)
  function pathOf(subs, P2) { const p = new P2(); for (const sp of subs) for (const o of sp) { if (o[0] === "m") p.moveTo(o[1][0], o[1][1]); else if (o[0] === "l") p.lineTo(o[1][0], o[1][1]); else if (o[0] === "c") p.bezierCurveTo(o[1][0], o[1][1], o[2][0], o[2][1], o[3][0], o[3][1]); else p.closePath(); } return p; }
  /** The design's picture as PNG base64 (no "data:" prefix), or null where there is no canvas or nothing to draw. The front, or the back when the front is empty. */
  function thumbPng(faces, px) {
    const P2 = root && root.Path2D; if (!doc || !P2) return null;
    const L = leaves(faces || {}), list = (L.front && L.front.length ? L.front : L.back) || []; if (!list.length) return null;
    let b = null; for (const l of list) b = boxOf(l.subpaths, b || undefined); b = realBox(b); if (!b) return null;
    for (const size of [px || THUMB_PX, 128, 96]) {
      const pad = Math.max(3, size * 0.04), w = Math.max(b[2] - b[0], 1e-3), h = Math.max(b[3] - b[1], 1e-3), k = (size - 2 * pad) / Math.max(w, h);
      const cv = doc.createElement("canvas"); cv.width = Math.max(8, Math.round(w * k + 2 * pad)); cv.height = Math.max(8, Math.round(h * k + 2 * pad));
      const ctx = cv.getContext("2d"); if (!ctx) return null;
      ctx.setTransform(k, 0, 0, k, pad - b[0] * k, pad - b[1] * k); ctx.lineJoin = "round"; ctx.lineCap = "round";
      const cuts = list.filter(l => l.role === "cut");
      if (cuts.length) { const body = pathOf(cuts.flatMap(l => l.subpaths), P2); ctx.fillStyle = BODY; ctx.fill(body, "evenodd"); }
      for (const l of list) {
        if (l.role === "cut") continue;
        const p = pathOf(l.subpaths, P2);
        if (paintFill(l)) { ctx.fillStyle = inkOf(l.fill); ctx.fill(p, l.even ? "evenodd" : "nonzero"); }
        if (paintStroke(l)) { ctx.strokeStyle = inkOf(l.stroke); ctx.lineWidth = Math.max(0.9 / k, (l.lwPt || 0.25)); ctx.stroke(p); }
      }
      if (cuts.length) { ctx.strokeStyle = EDGE; ctx.lineWidth = 1.2 / k; for (const l of cuts) ctx.stroke(pathOf(l.subpaths, P2)); }
      const b64 = String(cv.toDataURL("image/png")).split(",")[1] || "";
      if (b64 && b64.length * 0.75 <= LIMITS.thumb) return b64;
    }
    return null;
  }

  /** Saves a new version of a design (a new design without `id`). → { id, version, at, hash, row } */
  async function put(design, opts) {
    opts = opts || {};
    const by = String((design && design.by) || who() || "").trim();
    if (!by) throw Object.assign(new Error("Sign in to save this design: every version says who made it"), { needSignIn: true });
    const d = Object.assign({}, design);
    if (!(+d.widthMm > 0 && +d.heightMm > 0)) { const s = sizeOf(d.faces); if (s) Object.assign(d, s); }
    const c = clean(d); if (c.error) throw Object.assign(new Error(c.error), { refused: true });
    let thumb = null; if (opts.thumb !== false) { try { thumb = thumbPng(c.design.faces); } catch (e) { console.warn("design library: picture", e); } }
    const r = await call({ op: "designPut", design: c.design, ...(thumb ? { thumb } : {}), by }, { label: "Saving the design to the library", background: true });
    if (!r || !r.id) throw new Error((r && r.error) || "the design was not saved");
    const saved = Object.assign({}, c.design, r.row || {}, { id: r.id, version: r.version, at: r.at, hash: r.hash, by });
    keepFull(saved);
    ensureLoaded();
    if (upsert(r.row || rowOf(saved))) { if (idx.upTo && r.prev && idx.upTo === r.prev) idx.upTo = r.rev; persist(); }   // (this save is the only one since the index's revision: the index stays current without a call)
    tell({ kind: "put", row: idx.rows.get(r.id) });
    // (the employee log, charm-nest-design-log.js: an editorSave and this designSave of one order in one burst are one step of its timeline)
    log("designSave", { designId: r.id, version: r.version, name: c.design.name, sku: c.design.source.sku || null, orderId: c.design.orderId || null, customName: c.design.customName || null, poolId: c.design.source.poolId || null, faces: FACES.filter(f => c.design.faces[f]), by });
    return { id: r.id, version: r.version, at: r.at, hash: r.hash, row: idx.rows.get(r.id) || r.row };
  }
  function onChange(fn) { if (typeof fn === "function") listeners.add(fn); return () => listeners.delete(fn); }
  function setHost(h) { host = Object.assign({}, host, h || {}); }
  /** charm-nest-store.js's way in: { load() → { rows, upTo } | null, save({ rows, upTo }) }. The index is read again from it. */
  function useStore(s) { store = s && typeof s.load === "function" && typeof s.save === "function" ? s : localStore; idx.loaded = false; }

  return {
    LIMITS, MM,
    // pure (the cloud uses these too)
    clean, encodeFaces, decodeFaces, leaves, canonical, hash, sha256, sha256Sync, verify, sizeOf, cutBoxMm, wordsOf, fold, hayOf, toD, fromD, matrixOf,
    // the page
    put, get, list, search, refresh, rows: () => { ensureLoaded(); return rowsNow(); }, onChange, setHost, useStore, thumbPng, rowOf,
    _reset: () => { idx.loaded = false; idx.rows.clear(); idx.list = null; idx.upTo = null; idx.checkedAt = 0; full.clear(); listeners.clear(); }
  };
});
