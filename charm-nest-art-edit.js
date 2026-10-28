/* charm-nest-art-edit.js — ONE piece's own artwork, edited (ART EDIT, Paul, 27 Oct 2026).

   Paul: "The user needs full control similar to Adobe Illustrator over each individual layer on any charm ... select and fully manipulate (size, position, rotation, deletion,
   copy, paste, add/remove text) each layer of engravings and cut lines, outlines ... e.g. add a date inside a given calendar day square and update the month name located on top
   of the calendar charm."

   The master drawing of a SKU (charmnest/master/<SKU>.ai, read once by Pool.masterCharm) is shared by every order of that SKU: a piece's charm is a shallow copy of it
   (Pool.cloneCharm), its `members` the very segments of the master. A change a person makes to ONE piece must never reach the master or another order. So the change is kept
   beside the piece, as its own record (charmNestLibrary.js op_artPut, Charm_Pool_Art), and this file puts it over the master whenever the piece's charm is made:

     apply(charm, record, face)   → a NEW charm: its members deep-copied, the parts removed, the changed parts redrawn, the new parts added, a hash of its own, the keep-outs as they
                                    were. The charm handed in, its members and the parsed master are never touched. A part of the master that is drawn in one group with a changed or
                                    removed part (a per-SKU master draws the whole charm in ONE form) can only be written again from its geometry: such parts are marked
                                    `artRedraw` (and `synthetic`, the word charm-nest-pdf.js already uses for a member with no bytes of its own, as integrateRings does for a welded ring),
                                    and that group is dropped from the copied bytes (dropIndices). charm-nest-pdf.js buildSheet then writes this piece from its members; every other
                                    piece keeps the master's bytes exactly as before.
     memberId(charm, index)       → the stable id of a part of the master: its place in the charm's members and a hash of its geometry ("12-1x9k3ab"). The same drawing gives the same
                                    ids on every computer and every reload; a drawing made again (a new master file) gives others, so an old edit is never put on a new drawing.
     frame(view)                  → the editor's frame (points, y DOWN, in the face's view: job.view, the frame renderBack draws, view.M then view.R) and back: toItem / toSource.
     itemsOf(charm, view, face)   → the parts as the editor shows them (ids, roles, paths in the view frame).
     encode(changes, ctx)         → the record to save, from the editor's { removed, changed, added } (items in the view frame, with tx ty sx sy rot about the item's own box centre, or the
                                    `matrix` charm-nest-charm-editor.js states for a path or a group; a text's outlines as that editor places them in the face, see leavesOf).
     decode(record, charm, view)  → the editor's changes again, from a saved record.
     (27 Oct 2026, Paul: "I need fine font thickness control over each font") a part's weightMm (the weight its outlines were made at: mm thicker or thinner than the font draws them) is
     kept with its record entry, its segment (artWeightMm) and its item again (itemsOf, decode), so words opened again say their weight; the outlines in `d` already have it. A line's lwPt is
     written as the editor drew it (paintOf; charm-nest-pdf.js artOps writes it).
     load / save / clear / piece / sync   the cloud (artGet / artList / artPut) and the page (the host the bridge sets: setHost).

   Roles (what the laser does with a part; the laser knows its pens by colour, charm-nest-geom.js pathRole):
     cut    the cut line: a closed black hairline (a new shape: allowed only on a piece that is not on a sheet yet; see save)
     line   a line engraving: a coloured stroke (red when the editor says no colour)
     hatch  a hatched area: a blue [0 0 1] fill, as every hatching of the masters
     art    the editor's word for "as drawn": a fill is hatched (blue), a stroke is a line (red). Words arrive as outlines: filled, so hatched.
   GREY (Design & Engrave, 28 Oct 2026, Paul: "All images must be converted to a vector format ... the colour should be a greyscale using the black as base"): a part of a traced image
   (charm-nest-ce-image.js) is a hatch fill marked `artGrey` with its own grey [g, g, g]. It keeps that grey everywhere (the record, the server's check, the piece's segment: fillRGB, no
   hatchBlue; charm-nest-pdf.js artOps then writes it as it is), so the laser operator gives each grey its own power. A part not so marked is written exactly as before (blue).

   Who and when: every record carries `by` and `at` (charmNestLibrary.js). A saved record changes the piece's front (or back): an engraving approved on that face is approved again
   (the host's `changed`, Engrave.artChanged: "This charm's front changed since the words were approved — fit the words again", restoreWritten's words), and a sheet that holds
   the piece writes its files again (filesOwed on its record, writeAsPlaced on this page).

   UMD: window.CharmNestArtEdit in the page, module.exports in Node (the tests). No network here: the host's api does it. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === "object" && module.exports) module.exports = api;
  if (root) root.CharmNestArtEdit = api;
})(typeof self !== "undefined" ? self : typeof globalThis !== "undefined" ? globalThis : this, function (root) {
  "use strict";
  const VERSION = 1, FACES = ["front", "back"], ROLES = new Set(["cut", "line", "hatch", "art"]);
  const RED = [1, 0, 0], BLUE = [0, 0, 1], BLACK = [0, 0, 0], CUT_HAIRLINE_PT = 0.25, OFF_METAL_TOL_PT = 0.5;
  const SUFFIX = /~ae[0-9a-z]+$/;   // (the mark this file puts after a piece's hash: charm-nest-bridge.js soloBase reads a stored hash without it)
  const geom = () => (root && root.CharmNestGeom) || (typeof require === "function" ? require("./charm-nest-geom.js") : null);
  const pdf = () => (root && root.CharmNestPDF) || null;
  const WHY = {
    gone: "this part is not in the piece's drawing any more (the master was drawn again since): open the piece again",
    outline: "the outline is the piece itself: it is changed, never removed",
    back: "the back shows only the cut lines: on the back, parts are added; the drawing itself is changed on the front",
    notPath: "this part is live text or a picture in the master, not a path: outline it in the master first",
    group: "it is drawn in one group with live text or a picture, which cannot be drawn again from its geometry: outline that in the master first",
    path: "its path could not be read (only m, l, c and h, with finite numbers)",
    offMetal: "it falls outside the piece's outline: an engraving stays on the metal",
    cutPlaced: "a cut line is the shape the sheet was nested with: it is not changed on a piece already on a sheet. Engraving can be changed; to change the cut, take the piece off its sheet and let it be nested again",
    oneChange: "a part of the drawing is changed into one path (a group is added as new parts)"
  };

  /* ── numbers and matrices (PDF convention, as charm-nest-geom.js: x' = a x + c y + e, y' = b x + d y + f; mul(m, k) applies m first, then k) ── */
  const fnv = s => { let h = 0x811c9dc5; for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 0x01000193) >>> 0; } return h.toString(36); };
  const ID = [1, 0, 0, 1, 0, 0];
  const mul = (m, k) => [m[0] * k[0] + m[1] * k[2], m[0] * k[1] + m[1] * k[3], m[2] * k[0] + m[3] * k[2], m[2] * k[1] + m[3] * k[3], m[4] * k[0] + m[5] * k[2] + k[4], m[4] * k[1] + m[5] * k[3] + k[5]];
  const ap = (m, x, y) => [m[0] * x + m[2] * y + m[4], m[1] * x + m[3] * y + m[5]];
  const inv = m => { const d = m[0] * m[3] - m[1] * m[2]; if (!(Math.abs(d) > 1e-12)) return null; return [m[3] / d, -m[1] / d, -m[2] / d, m[0] / d, (m[2] * m[5] - m[3] * m[4]) / d, (m[1] * m[4] - m[0] * m[5]) / d]; };
  const isMat = m => Array.isArray(m) && m.length === 6 && m.every(Number.isFinite);
  const finite = p => Array.isArray(p) && p.length >= 2 && Number.isFinite(+p[0]) && Number.isFinite(+p[1]);
  const bboxOf = pts => { let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity; for (const p of pts) { if (p[0] < x0) x0 = p[0]; if (p[0] > x1) x1 = p[0]; if (p[1] < y0) y0 = p[1]; if (p[1] > y1) y1 = p[1]; } return pts.length ? [x0, y0, x1, y1] : null; };
  const ptsOf = subs => { const out = []; for (const sp of subs || []) for (const o of sp) for (let i = 1; i < o.length; i++) out.push(o[i]); return out; };
  const faceKey = f => (f === "front" || f === "back" ? f : null);
  /** The record key of a piece's face, as Charm_Pool_Back's: the pool id for the back, "<poolId>~front" for the front (charm-nest-engrave-faces.js recKey). */
  const recKey = (poolId, face) => String(poolId) + (face === "front" ? "~front" : "");

  /* ── the editor's frame ──
     The editor works in points, y DOWN, in the frame the face's view draws (job.view = CharmNestGeom.backView: a charm point p goes to R(M(p)), y up; M mirrors the back about the
     outline's centre, it is the identity for the front; R turns the piece hoop-up about the same centre). An item point is that view point with y negated: (x, -y). */
  function frame(view) {
    const M = view && isMat(view.M) ? view.M : ID, R = view && isMat(view.R) ? view.R : ID, F = mul(M, R), Fi = inv(F) || ID;
    return { M, R, F, Fi, toItem: p => { const q = ap(F, +p[0], +p[1]); return [q[0], -q[1]]; }, toSource: p => ap(Fi, +p[0], -p[1]) };
  }
  const mapSubs = (subs, fn) => (subs || []).map(sp => sp.map(o => (o[0] === "h" ? ["h"] : [o[0]].concat(o.slice(1).map(fn)))));
  /** An item's own matrix in the item frame: scale (sx, sy), then turn (rot degrees: clockwise on screen, as canvas and SVG turn in y-down), both about the centre of its box `bb`, then move (tx, ty). */
  function itemMatrix(it, bb) {
    const sx = Number.isFinite(+it.sx) && it.sx !== null && it.sx !== "" ? +it.sx : 1, sy = Number.isFinite(+it.sy) && it.sy !== null && it.sy !== "" ? +it.sy : 1, th = (Number.isFinite(+it.rot) ? +it.rot : 0) * Math.PI / 180;
    const tx = Number.isFinite(+it.tx) ? +it.tx : 0, ty = Number.isFinite(+it.ty) ? +it.ty : 0, cx = bb ? (bb[0] + bb[2]) / 2 : 0, cy = bb ? (bb[1] + bb[3]) / 2 : 0;
    const c = Math.cos(th), s = Math.sin(th), a = c * sx, b = s * sx, cc = -s * sy, d = c * sy;
    return [a, b, cc, d, cx + tx - (a * cx + cc * cy), cy + ty - (b * cx + d * cy)];
  }
  /** Is `subs` a path this file can read: m first, then l / c / h, finite numbers? */
  function okSubs(subs) {
    if (!Array.isArray(subs) || !subs.length) return false;
    for (const sp of subs) {
      if (!Array.isArray(sp) || !sp.length || !Array.isArray(sp[0]) || sp[0][0] !== "m") return false;
      for (const o of sp) {
        if (!Array.isArray(o)) return false;
        if (o[0] === "m" || o[0] === "l") { if (o.length !== 2 || !finite(o[1])) return false; }
        else if (o[0] === "c") { if (o.length !== 4 || !finite(o[1]) || !finite(o[2]) || !finite(o[3])) return false; }
        else if (o[0] !== "h") return false;
      }
    }
    return true;
  }

  /* ── the path as a string (what the record keeps: Firestore refuses an array inside an array) ── */
  const f3 = v => String(Math.round(v * 1000) / 1000);
  function toD(subs) {
    let d = "";
    for (const sp of subs || []) for (const o of sp) {
      if (o[0] === "m") d += "M" + f3(o[1][0]) + " " + f3(o[1][1]);
      else if (o[0] === "l") d += "L" + f3(o[1][0]) + " " + f3(o[1][1]);
      else if (o[0] === "c") d += "C" + [o[1], o[2], o[3]].map(p => f3(p[0]) + " " + f3(p[1])).join(" ");
      else if (o[0] === "h") d += "Z";
    }
    return d;
  }
  function fromD(d) {
    const tok = String(d || "").match(/[MLCZ]|[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?/g) || [], subs = []; let sp = null, i = 0;
    const n = () => +tok[i++], pt = () => [n(), n()];
    while (i < tok.length) {
      const c = tok[i++];
      if (c === "M") { sp = [["m", pt()]]; subs.push(sp); }
      else if (!sp) return null;
      else if (c === "L") sp.push(["l", pt()]);
      else if (c === "C") sp.push(["c", pt(), pt(), pt()]);
      else if (c === "Z") sp.push(["h"]);
      else return null;
    }
    return okSubs(subs) ? subs : null;
  }

  /* ── ids ── */
  function geomKey(m) {
    if (!m) return "none";
    const r = v => Math.round(+v * 100);
    if (Array.isArray(m.subpaths) && m.subpaths.length) return (m.kind || "path") + ":" + m.subpaths.map(sp => sp.map(o => o[0] + o.slice(1).map(p => r(p[0]) + "," + r(p[1])).join(" ")).join("")).join("|") + ":" + (m.fill ? "f" : "") + (m.stroke ? "s" : "");
    return (m.kind || "?") + ":" + (m.bbox || []).map(r).join(",") + ":" + (m.str || "");
  }
  const idCache = new WeakMap();
  /** Every part's id, in the order of the charm's members (a part an edit added carries its own: the item's id). */
  function memberIds(charm) {
    const list = charm && Array.isArray(charm.members) ? charm.members : [];
    let got = idCache.get(list);
    if (!got || got.n !== list.length) { got = { n: list.length, ids: list.map((m, i) => (m && m.artMemberId) || (i + "-" + fnv(geomKey(m)))) }; idCache.set(list, got); }   // (a list that grew since is read again)
    return got.ids;
  }
  /** The stable id of the part at `index` of the charm's members. */
  const memberId = (charm, index) => { const ids = memberIds(charm); return index >= 0 && index < ids.length ? ids[index] : null; };
  /** The drawing a record was made on: one hash of every part's id. */
  const baseKeyOf = charm => fnv(memberIds(baseOf(charm)).join(","));

  /* ── roles and paint ── */
  const chromatic = c => Array.isArray(c) && c.length >= 3 && (Math.max(c[0], c[1], c[2]) - Math.min(c[0], c[1], c[2])) > 0.15;
  const isPath = m => !!m && m.kind === "path" && Array.isArray(m.subpaths) && m.subpaths.length > 0 && (!!m.stroke || !!m.fill);
  function roleOf(m, charm) {
    if (!m) return "art";
    if (m.artRole && ROLES.has(m.artRole)) return m.artRole;
    const G = geom();
    if ((charm && m === charm.outline) || (G && G.isCutLine(m))) return "cut";
    if (m.kind !== "path") return "art";
    if (m.hatchBlue || (m.fill && !m.stroke)) return "hatch";
    if (m.stroke) return "line";
    return "art";
  }
  /** A colour the editor sent: [r, g, b] (0..1, or 0..255), "#rgb" / "#rrggbb", true (the role's own), null / false / "none" (none); undefined when it said nothing. */
  function colour(v) {
    if (v === undefined) return undefined;
    if (v === null || v === false || v === "none" || v === "") return null;
    if (v === true) return true;
    if (Array.isArray(v) && v.length >= 3 && v.slice(0, 3).every(x => Number.isFinite(+x))) { const k = v.slice(0, 3).some(x => +x > 1) ? 255 : 1; return v.slice(0, 3).map(x => Math.max(0, Math.min(1, +x / k))); }
    const m = /^#([0-9a-f]{3}|[0-9a-f]{6})$/i.exec(String(v).trim());
    if (m) { const h = m[1].length === 3 ? m[1].split("").map(c => c + c).join("") : m[1]; return [0, 2, 4].map(i => parseInt(h.slice(i, i + 2), 16) / 255); }
    return true;
  }
  /** The paint a part of `role` is written with (the laser's pens): { stroke, fill, lwPt, even }. `member`: the master part it changes (its own line colour and width are kept). */
  function paintOf(role, it, member) {
    it = it || {};
    const s = colour(it.stroke), f = colour(it.fill), lw = +it.lwPt > 0 ? +it.lwPt : null, even = it.even === true;
    if (role === "cut") return { stroke: BLACK, fill: null, lwPt: lw || (member && member.stroke && member.lwPt) || CUT_HAIRLINE_PT, even: false };
    const grey = it.artGrey === true && isGrey(f) ? f : null;   // (a traced image's grey stays its grey)
    if (role === "hatch") return grey ? { stroke: null, fill: grey, lwPt: null, even, artGrey: true } : { stroke: null, fill: BLUE, lwPt: null, even };
    const line = () => ({ stroke: Array.isArray(s) && chromatic(s) ? s : member && member.stroke && chromatic(member.strokeRGB) ? member.strokeRGB.slice(0, 3) : RED, fill: null, lwPt: lw || (member && member.stroke && member.lwPt) || CUT_HAIRLINE_PT, even: false });
    if (role === "line") return line();
    const filled = f !== undefined ? !!f : member ? !!member.fill : s === undefined || !s;
    return filled ? (grey ? { stroke: null, fill: grey, lwPt: null, even, artGrey: true } : { stroke: null, fill: BLUE, lwPt: null, even }) : line();
  }
  /** [g, g, g]: a grey (each channel the same, to a thousandth). */
  function isGrey(c) { return Array.isArray(c) && c.length >= 3 && Math.max(c[0], c[1], c[2]) - Math.min(c[0], c[1], c[2]) <= 0.001; }
  /** A layer name of this charm's master for a new part of `role` (the master's own ENGRAVE / HATCH / CUT), or null (the part stays on the piece's own layer). */
  function layerFor(role, base) {
    const names = [...new Set((base.members || []).map(m => m && m.layer).filter(Boolean).map(String))];
    const find = re => names.find(n => re.test(n)) || null;
    if (role === "cut") return (base.outline && base.outline.layer) || find(/^cut/i);
    if (role === "hatch") return find(/^hatch/i) || find(/^(?:engrave|engraving)/i);
    return find(/^(?:engrave|engraving)/i) || find(/^(?:hatch|front detail)/i);
  }

  /* ── the piece's master (the charm with its own edits taken off) ── */
  // what an edit replaces on the piece's charm; the master's are kept under `artFrom` (references, never copies: the checkpoint keeps one of each)
  const BASE_KEYS = ["members", "outline", "extras", "dropIndices", "topIndices", "bbox", "hash", "thumb", "bits", "w", "h", "scale", "bboxOuter", "areaPt2", "open", "widthPt", "heightPt", "centerPt", "strokePt"];
  const pick = (o, keys) => { const out = {}; for (const k of keys) if (o[k] !== undefined) out[k] = o[k]; return out; };
  // (and the keys the master does NOT have, so an edit's own — a dropped group, a trace — never stays on a piece taken back to the master)
  const masterOf = base => { const m = pick(base, BASE_KEYS), absent = BASE_KEYS.filter(k => base[k] === undefined); if (absent.length) m.absent = absent; return m; };
  /** The piece's charm as the master draws it: the charm itself when it carries no edit. */
  function baseOf(charm) {
    if (!charm || !charm.artFrom) return charm;
    const b = Object.assign({}, charm, charm.artFrom);
    for (const k of (charm.artFrom.absent || []).concat(["absent", "artFrom", "artEdit", "artBack", "artTrace", "artThumb"])) delete b[k];
    return b;
  }
  /** A segment with its own geometry arrays (its points, box, matrix and the held state ranges); references to other segments (a mirror's original, a chain's parts) stay references. */
  function cloneSeg(m) {
    if (!m || typeof m !== "object") return m;
    const c = Object.assign({}, m);
    if (Array.isArray(m.subpaths)) c.subpaths = m.subpaths.map(sp => sp.map(o => o.map(v => (Array.isArray(v) ? v.slice() : v))));
    for (const k of ["bbox", "ctm", "held", "clipBox", "strokeRGB", "fillRGB", "matrix"]) if (Array.isArray(m[k])) c[k] = m[k].slice();
    if (Array.isArray(m.children)) c.children = m.children.map(cloneSeg);
    return c;
  }
  function outlinePolys(base) { const G = geom(); try { return base && base.outline && G ? G.flatten(base.outline, 12) : null; } catch (_) { return null; } }
  /** Every point of the path on the metal (inside the outline, or within half a point of its line). */
  function onMetal(subs, polys) {
    const G = geom(); if (!polys || !polys.length || !G) return true;
    for (const poly of G.flatten({ subpaths: subs }, 8)) for (const p of poly) if (!G.pointInPolys(p[0], p[1], polys) && G.distToPolys(p[0], p[1], polys) > OFF_METAL_TOL_PT) return false;
    return true;
  }
  /** A part of the record as a segment of the piece (in the charm's own points, y up), written from its geometry. */
  function segOf(e, member, base) {
    const subs = fromD(e.d); if (!subs) return null;
    const role = ROLES.has(e.role) ? e.role : "art";
    const seg = { kind: "path", subpaths: subs, bbox: bboxOf(ptsOf(subs)), closed: subs.every(sp => sp[sp.length - 1][0] === "h"), depth: 0, start: -1, end: -1, synthetic: true, artRedraw: true, artRole: role, artMemberId: e.id };
    if (e.keep && member) {
      for (const k of ["stroke", "fill", "lwPt", "paintOp", "hatchBlue", "hatchLine", "hatchStrip", "manufacturingRole"]) if (member[k] !== undefined) seg[k] = member[k];
      seg.strokeRGB = (member.strokeRGB || BLACK).slice(); seg.fillRGB = (member.fillRGB || BLACK).slice();
      if (!seg.paintOp) seg.paintOp = seg.fill ? (seg.stroke ? "B" : "f") : "S";
      if (seg.hatchLine && !(seg.stroke && !seg.fill)) delete seg.hatchLine;
    } else {
      const stroke = Array.isArray(e.stroke) ? e.stroke : null, fill = Array.isArray(e.fill) ? e.fill : null;
      Object.assign(seg, { stroke: !!stroke, fill: !!fill, strokeRGB: (stroke || BLACK).slice(), fillRGB: (fill || BLACK).slice(), lwPt: +e.lwPt > 0 ? +e.lwPt : CUT_HAIRLINE_PT, paintOp: (fill ? (stroke ? "B" : "f") : "S") + (fill && e.even ? "*" : "") });
      seg.manufacturingRole = role === "cut" ? "cut" : role === "hatch" || (role === "art" && fill) ? "hatch" : "engrave";
      if (fill && !stroke && (role === "hatch" || role === "art")) { if (e.artGrey === true && isGrey(fill)) seg.artGrey = true; else seg.hatchBlue = true; }
    }
    seg.layer = member ? (member.layer != null ? member.layer : null) : layerFor(role, base);
    if (e.text) seg.artText = String(e.text).slice(0, 200);
    if (e.group) seg.artGroup = String(e.group);
    if (Number.isFinite(+e.weightMm) && +e.weightMm) seg.artWeightMm = +e.weightMm;
    return seg;
  }
  const emptyRec = rec => !rec || (!(rec.removed || []).length && !Object.keys(rec.changed || {}).length && !(rec.added || []).length);
  /** The hash, the master kept, the notes: the last step of apply. A piece with no edit left is the master's again. */
  function finish(out, base) {
    const faces = FACES.filter(f => out.artEdit && out.artEdit[f]);
    if (!faces.length) { delete out.artEdit; delete out.artFrom; out.hash = base.hash; return out; }
    out.artFrom = masterOf(base);
    out.hash = String(base.hash || "").replace(SUFFIX, "") + "~ae" + fnv(faces.map(f => f + ":" + (out.artEdit[f].at || 0) + ":" + (out.artEdit[f].key || "")).join("|"));
    return out;
  }
  const recSig = rec => fnv(JSON.stringify([rec.baseKey || "", rec.removed || [], Object.keys(rec.changed || {}).sort().map(k => [k, rec.changed[k].d, rec.changed[k].role, rec.changed[k].keep ? 1 : 0, rec.changed[k].stroke || null, rec.changed[k].fill || null]), (rec.added || []).map(x => [x.id, x.d, x.role, x.stroke || null, x.fill || null])]));

  /** The FRONT: the master's parts, the edit put over them. */
  function applyFront(charm, rec) {
    const base = baseOf(charm), ids = memberIds(base), at = new Map(ids.map((id, i) => [id, i]));
    const out = Object.assign({}, charm, pick(base, BASE_KEYS));
    for (const k of BASE_KEYS) if (base[k] === undefined) delete out[k];
    out.artEdit = Object.assign({}, charm.artEdit || {}); delete out.artEdit.front; delete out.artTrace; delete out.artThumb;
    if (emptyRec(rec)) return finish(out, base);
    const note = { at: +rec.at || 0, by: rec.by || null, key: recSig(rec), removed: 0, changed: 0, added: 0, refused: [], stale: [] };
    out.artEdit.front = note;
    if (rec.baseKey && rec.baseKey !== baseKeyOf(base)) { note.stale.push("*"); note.why = WHY.gone; return finish(out, base); }
    const members = base.members || [], outlineAt = members.indexOf(base.outline), polys = outlinePolys(base), cutOk = rec.cut === true;
    const no = (id, why) => note.refused.push({ id, why });
    const topOf = m => (!m || m.synthetic ? null : m.parent != null ? m.parent : m.index != null ? m.index : null);
    const remove = new Set(), change = new Map();
    for (const id of rec.removed || []) {
      const i = at.get(id); if (i == null) { note.stale.push(id); continue; }
      if (i === outlineAt) { no(id, WHY.outline); continue; }
      if (roleOf(members[i], base) === "cut" && !cutOk) { no(id, WHY.cutPlaced); continue; }
      remove.add(i);
    }
    for (const [id, e] of Object.entries(rec.changed || {})) {
      const i = at.get(id); if (i == null) { note.stale.push(id); continue; }
      const m = members[i]; if (!isPath(m)) { no(id, WHY.notPath); continue; }
      const seg = segOf(e, m, base); if (!seg) { no(id, WHY.path); continue; }
      const cut = seg.artRole === "cut" || roleOf(m, base) === "cut";
      if (cut && !cutOk) { no(id, WHY.cutPlaced); continue; }
      if (i === outlineAt && seg.artRole !== "cut") { no(id, WHY.outline); continue; }
      if (seg.artRole !== "cut" && !onMetal(seg.subpaths, polys)) { no(id, WHY.offMetal); continue; }
      seg.artChanged = true; seg.artMemberId = id; change.set(i, seg);
    }
    // a top-level group of the master that holds a removed or changed part is dropped from the copied bytes and its other parts are drawn again from their geometry:
    // a group that also holds live text or a picture cannot be drawn again, so an edit inside it is refused (the rest of the record stands)
    const groups = new Map(); members.forEach((m, i) => { const t = topOf(m); if (t != null) (groups.get(t) || groups.set(t, []).get(t)).push(i); });
    for (const i of [...remove, ...change.keys()]) {
      const t = topOf(members[i]); if (t == null) continue;
      if ((groups.get(t) || []).some(j => !remove.has(j) && !change.has(j) && !isPath(members[j]))) { no(ids[i], WHY.group); remove.delete(i); change.delete(i); }
    }
    const dropTops = new Set(); for (const i of [...remove, ...change.keys()]) { const t = topOf(members[i]); if (t != null) dropTops.add(t); }
    const list = [], copyOf = new Map(); let cutTouched = false;
    members.forEach((m, i) => {
      if (remove.has(i)) { if (roleOf(m, base) === "cut") cutTouched = true; return; }
      let c;
      if (change.has(i)) { c = change.get(i); if (c.artRole === "cut" || roleOf(m, base) === "cut") cutTouched = true; }
      else { c = cloneSeg(m); c.artMemberId = ids[i]; const t = topOf(m); if (t != null && dropTops.has(t)) { c.synthetic = true; c.artRedraw = true; } }
      copyOf.set(m, c); list.push(c);
    });
    for (const e of rec.added || []) {
      const seg = segOf(e, null, base); if (!seg) { no(e.id, WHY.path); continue; }
      if (seg.artRole === "cut") { if (!cutOk) { no(e.id, WHY.cutPlaced); continue; } cutTouched = true; }
      else if (!onMetal(seg.subpaths, polys)) { no(e.id, WHY.offMetal); continue; }
      seg.artAdded = true; list.push(seg); note.added++;
    }
    note.removed = remove.size; note.changed = change.size;
    out.members = list;
    out.outline = copyOf.get(base.outline) || base.outline;
    if (Array.isArray(base.extras)) out.extras = base.extras.map(x => copyOf.get(x)).filter(Boolean);
    out.dropIndices = new Set([...(base.dropIndices || []), ...dropTops]);
    if (cutTouched) { out.artTrace = true; note.cut = true; }   // a new shape: its silhouette is traced again (ready) before it is nested
    out.artThumb = true;                                       // its picture is drawn again (ready)
    return finish(out, base);
  }
  /** The BACK: the cut lines as they are, the parts a person added kept as `artBack` (in the charm's own points as seen from the front: buildBackFile turns them over with the cut). */
  function applyBack(charm, rec) {
    const base = baseOf(charm), out = Object.assign({}, charm);
    out.artEdit = Object.assign({}, charm.artEdit || {}); delete out.artEdit.back; delete out.artBack;
    if (emptyRec(rec)) return finish(out, base);
    const note = { at: +rec.at || 0, by: rec.by || null, key: recSig(rec), removed: 0, changed: 0, added: 0, refused: [], stale: [] };
    out.artEdit.back = note;
    if (rec.baseKey && rec.baseKey !== baseKeyOf(base)) { note.stale.push("*"); note.why = WHY.gone; return finish(out, base); }
    for (const id of (rec.removed || []).concat(Object.keys(rec.changed || {}))) note.refused.push({ id, why: WHY.back });
    const polys = outlinePolys(base), art = [];
    for (const e of rec.added || []) {
      const seg = segOf(e, null, base); if (!seg) { note.refused.push({ id: e.id, why: WHY.path }); continue; }
      if (seg.artRole === "cut") { note.refused.push({ id: e.id, why: WHY.back }); continue; }
      if (!onMetal(seg.subpaths, polys)) { note.refused.push({ id: e.id, why: WHY.offMetal }); continue; }
      seg.artAdded = true; art.push(seg);
    }
    note.added = art.length;
    if (art.length) out.artBack = art;
    return finish(out, base);
  }
  /** A NEW charm: `charm` (a piece's charm, edited or not) with the record of one face put over its master. A null record takes that face's edit off. Never changes `charm`. */
  function apply(charm, rec, face) {
    if (!charm || !Array.isArray(charm.members)) return charm;
    face = faceKey(face) || faceKey(rec && rec.face) || "front";
    return face === "back" ? applyBack(charm, rec) : applyFront(charm, rec);
  }

  /* ── the editor's changes → the record ── */
  /** The leaves of an item (a path or text, or a group of them): { id, role, kind, subpaths (item frame, as drawn), M (its matrix, the group's after its own), paint fields }.
   *  (CHARM EDIT, 27 Oct 2026: what charm-nest-charm-editor.js hands back is read as it says it is. A path or a group states its own `matrix` (geometry -> parent), made about the TIGHT box of its
   *  curves: that one is used, not one made again here about the box of its control points, which differs on any curve that is turned or scaled. A text's `subpaths` are its outlines ALREADY placed
   *  in the face, its groups' turns included: nothing is put on them again. An item that states no matrix (decode's, a caller's own) is placed by its tx ty sx sy rot as before.) */
  function leavesOf(item, own, parentM, parentRole, out, prefix) {
    out = out || [];
    if (!item || typeof item !== "object") return out;
    if (item.kind === "group" && Array.isArray(item.children)) {
      // the group turns and scales about the box of its children as each child places itself
      const kids = item.children.map((ch, k) => ({ ch, id: ch && ch.id != null ? String(ch.id) : (item.id != null ? item.id : prefix || "g") + "." + k }));
      let bb = null;
      if (!isMat(item.matrix)) for (const { ch } of kids) { if (!ch || !okSubs(ch.subpaths)) continue; const m = itemMatrix(ch, bboxOf(ptsOf(ch.subpaths))); const b = bboxOf(ptsOf(mapSubs(ch.subpaths, p => ap(m, +p[0], +p[1])))); if (b) bb = bb ? [Math.min(bb[0], b[0]), Math.min(bb[1], b[1]), Math.max(bb[2], b[2]), Math.max(bb[3], b[3])] : b; }
      const G = mul(isMat(item.matrix) ? item.matrix : itemMatrix(item, bb), parentM || ID);
      for (const { ch, id } of kids) leavesOf(Object.assign({}, ch, { id, group: item.group || item.id }), null, G, ROLES.has(item.role) ? item.role : parentRole, out, id);
      return out;
    }
    const subs = okSubs(item.subpaths) ? item.subpaths : own ? own() : null, placed = item.kind === "text" && okSubs(item.subpaths);
    out.push({ id: item.id != null ? String(item.id) : prefix || "", role: ROLES.has(item.role) ? item.role : parentRole || null, kind: item.kind === "text" ? "text" : "path",
      subpaths: subs, M: !subs ? null : placed ? ID : mul(isMat(item.matrix) ? item.matrix : itemMatrix(item, bboxOf(ptsOf(subs))), parentM || ID),
      stroke: item.stroke, fill: item.fill, lwPt: item.lwPt, even: item.even, text: item.text, group: item.group, weightMm: item.weightMm, ...(item.artGrey === true ? { artGrey: true } : {}) });
    return out;
  }
  const toSource = (leaf, fr) => (leaf.subpaths ? mapSubs(leaf.subpaths, p => fr.toSource(ap(leaf.M, +p[0], +p[1]))) : null);
  /** WEIGHT (27 Oct 2026, Paul: "I need fine font thickness control over each font"): the weight the outlines were made at (mm thicker or thinner than the font draws them), kept with
   *  them so the words opened again say it and a new weight grows them by the difference. The outlines in `d` already have it: nothing here or in the laser file applies it again. */
  const weightRec = leaf => { const w = +leaf.weightMm; return Number.isFinite(w) && Math.abs(w) >= 0.0005 && Math.abs(w) <= 5 ? { weightMm: Math.round(w * 1000) / 1000 } : {}; };
  const paintRec = p => Object.assign({ stroke: p.stroke ? p.stroke.map(v => Math.round(v * 1000) / 1000) : null, fill: p.fill ? p.fill.map(v => Math.round(v * 1000) / 1000) : null }, p.lwPt ? { lwPt: Math.round(p.lwPt * 1000) / 1000 } : {}, p.even ? { even: true } : {}, p.artGrey ? { artGrey: true } : {});
  /** The record to save for one face of one piece, from the editor's changes. Throws { refused: [{ id, why }] } when any part cannot be kept as asked: nothing is half saved.
   *  ctx = { charm (the piece's charm on the page, edited or not), view (the face's view the editor drew: job.view), face, placed (the piece is on a sheet) } */
  function encode(changes, ctx) {
    ctx = ctx || {}; changes = changes || {};
    const face = faceKey(ctx.face); if (!face) throw new Error("an artwork edit is of the front or the back");
    const charm = ctx.charm; if (!charm || !Array.isArray(charm.members)) throw new Error("the piece's charm is not on this page");
    const base = baseOf(charm), ids = memberIds(base), at = new Map(ids.map((id, i) => [id, i])), fr = frame(ctx.view), polys = outlinePolys(base);
    const rec = { v: VERSION, face, baseKey: baseKeyOf(base), removed: [], cutIds: [], changed: {}, added: [], cut: false }, refused = [];
    const no = (id, why) => refused.push({ id: String(id), why });
    // (an item sent with its move, scale or turn and no path of its own is moved from where the piece draws it NOW: an edit saved before stays the start of the next)
    const now = new Map(); for (const m of (charm.members || []).concat(charm.artBack || [])) if (m && m.artMemberId && isPath(m)) now.set(String(m.artMemberId), m);
    const ownOf = (id, m) => () => mapSubs(((now.get(String(id)) || m || {}).subpaths) || [], fr.toItem);
    for (const id of new Set((changes.removed || []).map(String))) {
      const i = at.get(id); if (i == null) { no(id, WHY.gone); continue; }
      if (face === "back") { no(id, WHY.back); continue; }
      if (base.members[i] === base.outline) { no(id, WHY.outline); continue; }
      if (roleOf(base.members[i], base) === "cut") rec.cutIds.push(id);
      rec.removed.push(id);
    }
    for (const [id, item] of Object.entries(changes.changed || {})) {
      const i = at.get(id); if (i == null) { no(id, WHY.gone); continue; }
      if (rec.removed.includes(id)) { no(id, "it is removed and changed at once"); continue; }
      if (face === "back") { no(id, WHY.back); continue; }
      const m = base.members[i]; if (!isPath(m)) { no(id, WHY.notPath); continue; }
      const leaves = leavesOf(item, ownOf(id, m)); if (leaves.length !== 1) { no(id, WHY.oneChange); continue; }
      const leaf = leaves[0], was = roleOf(m, base), role = leaf.role || was, subs = toSource(leaf, fr);
      if (!subs || !okSubs(subs)) { no(id, WHY.path); continue; }
      if (m === base.outline && role !== "cut") { no(id, WHY.outline); continue; }
      if (role !== "cut" && !onMetal(subs, polys)) { no(id, WHY.offMetal); continue; }
      if (role === "cut" || was === "cut") rec.cutIds.push(id);
      const keep = role === was && leaf.stroke === undefined && leaf.fill === undefined && (leaf.lwPt == null) && (leaf.even == null);
      const e = Object.assign({ kind: "path", role, d: toD(subs) }, keep ? { keep: true } : paintRec(paintOf(role, leaf, m)), weightRec(leaf));
      if (leaf.text != null && leaf.text !== "") e.text = String(leaf.text).slice(0, 200);
      rec.changed[id] = e;
    }
    const seen = new Set();
    for (const item of changes.added || []) for (const leaf of leavesOf(item, item && item.id != null && now.has(String(item.id)) ? ownOf(item.id, null) : null)) {
      const id = String(leaf.id || ""); if (!/^[A-Za-z0-9_.:-]{1,64}$/.test(id)) { no(id || "?", "a new part needs a short id (letters, digits, _ . : -)"); continue; }
      if (seen.has(id) || at.has(id)) { no(id, "the id is used twice"); continue; } seen.add(id);
      const role = leaf.role || "art", subs = toSource(leaf, fr);
      if (!subs || !okSubs(subs)) { no(id, WHY.path); continue; }
      if (face === "back" && role === "cut") { no(id, WHY.back); continue; }
      if (role !== "cut" && !onMetal(subs, polys)) { no(id, WHY.offMetal); continue; }
      const e = Object.assign({ id, kind: leaf.kind, role, d: toD(subs) }, paintRec(paintOf(role, leaf, null)), weightRec(leaf));
      if (leaf.text != null && leaf.text !== "") e.text = String(leaf.text).slice(0, 200);
      if (leaf.group != null) e.group = String(leaf.group).slice(0, 64);
      rec.added.push(e);
    }
    rec.cut = rec.cutIds.length > 0 || Object.values(rec.changed).some(e => e.role === "cut") || rec.added.some(e => e.role === "cut");
    if (rec.cut && ctx.placed) no("cut", WHY.cutPlaced);
    if (refused.length) throw Object.assign(new Error(refused.length === 1 ? `Not saved: ${refused[0].why}` : `Not saved: ${refused.length} parts cannot be kept as asked (${refused[0].why})`), { refused, artEdit: true });
    return rec;
  }
  /** The editor's changes again, from a saved record (items in the view frame, nothing moved: their paths are where the record put them). */
  function decode(rec, charm, view) {
    const fr = frame(view), out = { removed: [], changed: {}, added: [] };
    if (!rec) return out;
    const item = e => Object.assign({ id: e.id, kind: e.kind || "path", role: e.role, subpaths: mapSubs(fromD(e.d) || [], fr.toItem), tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 },
      e.keep ? {} : { stroke: e.stroke || null, fill: e.fill || null }, e.lwPt ? { lwPt: e.lwPt } : {}, e.even ? { even: true } : {}, e.text ? { text: e.text } : {}, e.group ? { group: e.group } : {}, weightRec(e), e.artGrey ? { artGrey: true } : {});
    out.removed = (rec.removed || []).slice();
    for (const [id, e] of Object.entries(rec.changed || {})) out.changed[id] = item(Object.assign({}, e, { id }));
    out.added = (rec.added || []).map(item);
    return out;
  }
  /** The parts of one face as the editor shows them: [{ id, origin "master" | "added", changed, kind, role, subpaths (view frame), stroke, fill, lwPt, editable, layer }]. */
  function itemsOf(charm, view, face) {
    if (!charm || !Array.isArray(charm.members)) return [];
    face = faceKey(face) || (view && view.face === "front" ? "front" : "back");
    const fr = frame(view), ids = memberIds(charm), list = [];
    const one = (m, id, origin) => ({ id, origin, changed: !!m.artChanged, kind: m.kind === "text" ? "text" : "path", role: roleOf(m, charm), subpaths: isPath(m) ? mapSubs(m.subpaths, fr.toItem) : [],
      stroke: m.stroke ? (m.strokeRGB || BLACK).slice(0, 3) : null, fill: m.fill ? (m.hatchBlue ? BLUE : (m.fillRGB || BLACK).slice(0, 3)) : null, lwPt: m.lwPt || null, even: String(m.paintOp || "").endsWith("*"),
      editable: isPath(m) && !(face === "back"), layer: m.layer || null, tx: 0, ty: 0, sx: 1, sy: 1, rot: 0, ...(m.artText ? { text: m.artText } : {}), ...(m.artGroup ? { group: m.artGroup } : {}), ...(m.artWeightMm ? { weightMm: m.artWeightMm } : {}), ...(m.artGrey ? { artGrey: true } : {}) });
    charm.members.forEach((m, i) => { if (!m) return; if (face === "back" && roleOf(m, charm) !== "cut") return; list.push(one(m, ids[i], m.artAdded ? "added" : "master")); });
    if (face === "back") for (const m of charm.artBack || []) list.push(Object.assign(one(m, m.artMemberId, "added"), { editable: true }));
    return list;
  }

  /* ── the page: cloud records, the pieces made again, a save ── */
  let host = {};
  const cache = new Map();   // recKey → { rec | null }
  const remember = (poolId, face, rec) => cache.set(recKey(poolId, face), { rec: rec || null });
  /** The faces the pool row says have an edit: { front: at, back: at } (0 when none). */
  const flagsOf = row => { const a = row && row.artEdit && typeof row.artEdit === "object" ? row.artEdit : {}; return { front: +a.front > 0 ? +a.front : 0, back: +a.back > 0 ? +a.back : 0 }; };
  const said = (row, face) => !!(row && row.artEdit && typeof row.artEdit === "object" && Object.prototype.hasOwnProperty.call(row.artEdit, face));
  /** Read the records of these faces (each { poolId, face, at }) that this page does not hold as at least that new: one artList call for all of them. */
  async function load(list) {
    const keys = [...new Set((list || []).filter(x => x && x.poolId && faceKey(x.face)).filter(x => { const c = cache.get(recKey(x.poolId, x.face)); return !c || (+x.at > 0 && (!c.rec || +c.rec.at < +x.at)); }).map(x => recKey(x.poolId, x.face)))];
    if (!keys.length) return 0;
    if (!host.api) throw new Error("the page cannot read artwork edits yet");
    for (let i = 0; i < keys.length; i += 200) {
      const part = keys.slice(i, i + 200), res = await host.api("charmNestLibrary", { op: "artList", keys: part }, { quiet: true });
      if (!res || res.error || !res.arts) throw new Error((res && res.error) || "the artwork edits could not be read");
      for (const k of part) cache.set(k, { rec: res.arts[k] || null });
    }
    return keys.length;
  }
  /** The picture drawn again, or (a cut line changed) the silhouette traced again: what the page does with a charm apply made before it is shown or nested. */
  async function ready(c, parsed) {
    const P = pdf(); if (!c || !P) return c;
    if (c.artTrace && P.buildSilhouettes) {
      const mark = String(c.hash || "").match(SUFFIX);
      await P.buildSilhouettes(parsed || null, [c], +c.scale || 6);
      if (mark) c.hash = String(c.hash || "").replace(SUFFIX, "") + mark[0];
      delete c.artTrace; delete c.artThumb;
    } else if (c.artThumb && P.thumbnail) {
      try { c.thumb = await P.thumbnail(c, 168); } catch (_) { /* the master's picture stands in until it is drawn */ }
      delete c.artThumb;
    }
    return c;
  }
  /** The charm of a piece made from the master (a reload, a recall, a checkpoint, a line pooled again), with the piece's own edits put over it. `row`: its pool row (its artEdit stamps
   *  say whether it has any; a piece with none costs no read). A piece whose edit cannot be read throws: it is never cut from the master without it. */
  async function piece(charm, poolId, row) {
    if (!charm || !poolId || !Array.isArray(charm.members)) return charm;
    row = row || (host.rowOf ? host.rowOf(poolId) : null);
    const fl = flagsOf(row);
    for (const f of FACES) if (!fl[f] && said(row, f)) remember(poolId, f, null);   // (taken off elsewhere: the row says so)
    const want = FACES.filter(f => fl[f] || (cache.get(recKey(poolId, f)) || {}).rec || (charm.artEdit && charm.artEdit[f]));
    if (!want.length) return charm;
    try { await load(want.filter(f => fl[f]).map(f => ({ poolId, face: f, at: fl[f] }))); }
    catch (e) { throw Object.assign(new Error(`piece ${poolId}: its own artwork edit could not be read (${(e && e.message) || e}): try again`), { transient: true }); }
    let out = charm;
    for (const f of FACES) { const c = cache.get(recKey(poolId, f)); if (c) out = apply(out, c.rec, f); }
    if (out === charm) return charm;
    // (an edit made on a drawing the master has since been drawn again from is not put on the new drawing: the piece is cut as the master draws it, and a person is told)
    for (const f of FACES) { const n = out.artEdit && out.artEdit[f]; if (n && (n.stale.length || n.refused.length) && host.note) { try { host.note(poolId, f, n); } catch (_) { /* said once the page can */ } } }
    return ready(out, host.parsedOf ? host.parsedOf(charm) : null);
  }
  /** After a checkpoint is restored: every piece on this page's sheets whose pool row names an edit this page's charm does not carry (made on another computer since, or taken off)
   *  is made again with it. Only those pieces' records are read. → the number of pieces changed. */
  async function sync() {
    if (!host.liveCharms) return 0;
    const todo = new Map();
    for (const c of host.liveCharms() || []) {
      if (!c || !c.poolId || todo.has(c.poolId)) continue;
      const row = host.rowOf ? host.rowOf(c.poolId) : null, fl = flagsOf(row);
      if (FACES.some(f => (fl[f] || 0) !== ((c.artEdit && c.artEdit[f] && +c.artEdit[f].at) || 0) && (fl[f] || said(row, f)))) todo.set(c.poolId, { c, row });
    }
    if (!todo.size) return 0;
    await load([...todo.values()].flatMap(({ c, row }) => FACES.filter(f => flagsOf(row)[f]).map(f => ({ poolId: c.poolId, face: f, at: flagsOf(row)[f] }))));
    let n = 0;
    for (const [poolId, { c, row }] of todo) {
      const next = await piece(c, poolId, row); if (next === c) continue;
      if (host.swap) host.swap(poolId, next, c); n++;
      // (what changed is said as a save here says it: the face's approval and the sheet's files)
      if (host.changed) for (const f of FACES) { const was = (c.artEdit && c.artEdit[f] && c.artEdit[f].key) || "", now = (next.artEdit && next.artEdit[f] && next.artEdit[f].key) || ""; if (was !== now) await host.changed(poolId, f, { charm: next, record: (cache.get(recKey(poolId, f)) || {}).rec || null, sync: true }); }
    }
    return n;
  }
  /** Save a person's edit of one face of one piece: checked (encode), sent (artPut), put over the piece's charm on this page (the sheets hold the new charm), and its consequences
   *  (the face's approved engraving approved again, the sheet's files written again: host.changed). An edit with nothing left in it takes the piece back to the master.
   *  ctx = { charm, view, by, placed, expectedAt }. → { record, charm, sheetId, filesOwed } */
  async function save(poolId, face, changes, ctx) {
    ctx = ctx || {}; face = faceKey(face); if (!face) throw new Error("an artwork edit is of the front or the back");
    if (!host.api) throw new Error("the page cannot save artwork edits yet");
    const charm = ctx.charm || (host.charmOf ? host.charmOf(poolId) : null); if (!charm) throw new Error("this piece's charm is not on this page");
    const by = String(ctx.by || (host.who ? host.who() : "") || "").trim(); if (!by) throw new Error("sign in to save an artwork edit");
    const placed = ctx.placed != null ? !!ctx.placed : !!(host.placed && host.placed(poolId));
    const art = encode(changes, { charm, view: ctx.view, face, placed });
    return put(poolId, face, emptyRec(art) ? null : art, by, charm, ctx);
  }
  /** Take one face's edit off: the piece is drawn and cut as the master draws it again. */
  async function clear(poolId, face, ctx) {
    ctx = ctx || {}; face = faceKey(face); if (!face) throw new Error("an artwork edit is of the front or the back");
    const charm = ctx.charm || (host.charmOf ? host.charmOf(poolId) : null);
    const by = String(ctx.by || (host.who ? host.who() : "") || "").trim(); if (!by) throw new Error("sign in to change an artwork edit");
    return put(poolId, face, null, by, charm, ctx);
  }
  async function put(poolId, face, art, by, charm, ctx) {
    if (!host.api) throw new Error("the page cannot save artwork edits yet");
    // (the edit this page last saw of that face: the cloud refuses the save when another computer saved one since; a page that never read it expects what the piece's own charm or pool row says)
    const had = cache.get(recKey(poolId, face)), row = host.rowOf ? host.rowOf(poolId) : null;
    const expectedAt = ctx.expectedAt != null ? +ctx.expectedAt || 0 : had ? (had.rec ? +had.rec.at || 0 : 0) : (charm && charm.artEdit && charm.artEdit[face] && +charm.artEdit[face].at) || flagsOf(row)[face] || 0;
    let res;
    try { res = await host.api("charmNestLibrary", Object.assign({ op: "artPut", poolId, face, by, expectedAt }, art ? { art } : { clear: true })); }
    catch (e) { const d = (e && e.data) || {}; throw Object.assign(new Error(d.error || (e && e.message) || "the artwork edit was not saved"), { conflict: !!d.conflict, cutRefused: !!d.cutRefused, ...(d.cutRefused ? { refused: [{ id: "cut", why: d.error }] } : {}) }); }
    if (!res || !res.ok) { const d = res || {}; throw Object.assign(new Error(d.error || "the artwork edit was not saved"), { conflict: !!d.conflict, cutRefused: !!d.cutRefused }); }
    const rec = res.art || null;
    remember(poolId, face, rec);
    if (host.flag) host.flag(poolId, face, rec ? rec.at : null);
    let applied = charm ? apply(charm, rec, face) : null;
    if (applied) applied = await ready(applied, host.parsedOf ? host.parsedOf(charm) : null);
    if (applied && host.swap) host.swap(poolId, applied, charm, rec, face);
    // (cut: a cut line changed, now or in the edit this one replaces: the piece's other face stands on that cut too)
    const info = { charm: applied, record: rec, sheetId: res.sheetId || null, filesOwed: res.filesOwed || null, cut: !!(rec && rec.cut) || !!(charm && charm.artEdit && charm.artEdit.front && charm.artEdit.front.cut) };
    if (host.changed) await host.changed(poolId, face, info);
    return info;
  }
  /** The page's own ways (charm-nest-bridge.js sets them): api, rowOf, charmOf, placed, who, flag, swap, changed, parsedOf, liveCharms, note. */
  function setHost(h) { host = Object.assign({}, h || {}); return host; }

  return { VERSION, FACES, ROLES: [...ROLES], SUFFIX, WHY, recKey, frame, itemMatrix, toD, fromD, memberId, memberIds, baseKeyOf, baseOf, roleOf, paintOf, apply, encode, decode, itemsOf,
    load, ready, piece, sync, save, clear, setHost, host: () => host, cached: (poolId, face) => { const c = cache.get(recKey(poolId, face)); return c ? c.rec : undefined; }, forget: () => cache.clear(), remember };
});
