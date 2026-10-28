/* charm-nest-custom-pool.js — A DESIGN SENT TO A SHEET IS A REAL PIECE (CUSTOMPOOL; Paul, 28 Oct 2026, items 27 and 28; the lead's D4, D5 and D6).

   Paul: "send that charm to the next given sheet color, chosen by the user and market as handmade custom" · "You need to ensure the sanctity of the designed charm. It's exact
   dimensions and user designed features, and that has to be linked to that specific Custom order." · "sometimes the user just needs to add a charm onto a sheet and that custom
   order can be named by the user so the user can easily identify that extra piece that was added, and not linked to an official order."

   Pool.addCustomPiece({ design, version, hash, metal, orderId, customName, quantity, by, fixes? }) -> Promise<poolIds>     (the contract the Send to sheet pop-up calls, interfaces.md)
     1. leavesOf   the design's FRONT FaceDoc (charm-nest-charm-editor.js: points, y down) flattened into its parts, each with its role's pen as the laser knows them: a cut line a
                   black hairline (always closed), a line engraving a red stroke (or its own colour), a hatch or art fill blue, or its own grey (D2: a traced level keeps its grey);
                   in the charm's own points (y up), every number on the thousandth of a point the files are written with
     2. pdfOf      those parts on a page of their own, in CUT / ENGRAVE / HATCH layers as a master's are
     3. readOf     read back exactly as a custom design file is (CustomSheet's reader: parse, group, rings, silhouettes): it must be ONE piece, with nothing left out
     4. writtenOf  that piece written ALONE by the sheet writer itself (CharmNestPDF.buildSheet) and read back from the file: its parts there must be the design's parts, every one,
                   and their hash is the piece's geometry hash (geomHash), what every later file of its sheet is checked against
     5. the pool rows (charmNestLibrary customPiecePut: a piece that belongs to no order is numbered CUSTOM-<yyyymmdd>-<n> there), one charm per copy named by its pool id, put on
        the next open sheet of the chosen metal in the current run as an arriving order's pieces are (LiveNest.intakePage; an order's piece goes to the order's own sheet of that
        metal, R16), a new sheet when none is open; and nested there (startNest, or the sheet's next turn when it is being nested right now)
   SANCTITY (D6): every such piece carries `frozen` { designId, version, designHash, geomHash, widthMm, heightMm, customId, name, orderless, by, at }. Its size is frozen: the nest
   writes it at 100 % (holdSize: never the 97.1 % every other charm is written at), the overlap fix never shrinks it, Manual Control moves and turns it but never resizes it (sizeWhy
   says why), and before a sheet holding one is saved its .ai is read back (checkWritten): a frozen piece written at another size, or with other parts than its hash, stops the
   save, loudly (the sheet's card and a toast say which piece and why).
   ORDERLESS (D4): orderId null → the piece's order is its own number, CUSTOM-<yyyymmdd>-<n>, and it is named "<name> · Custom · no order" on the sheet, in the files and on the
   sheet's QR label; it rides the sheet and the laser like any piece and is counted with them, but it is no order of the shop: the QR label's list of orders (what the stations sort
   and ship by) and its set's orders leave it out (shipIds), and the label names it under the title instead (labelNotes). It has no shipping label of its own.
   An ORDER's piece (orderId given) is that order's piece: named "<order> · Custom · <name>", it goes with the order's other pieces of its metal, its QR label and its set.
   The back of a design (faces.back) is kept with the design (its id and version ride on the piece); the pool side writes the front, the piece itself.

   UMD: window.CharmNestCustomPool in the page (Pool.addCustomPiece is set from here; loaded after charm-nest-bridge.js), module.exports in node (the pure parts, for the tests). */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === "object" && module.exports) module.exports = api;
  if (root) root.CharmNestCustomPool = api;
})(typeof self !== "undefined" ? self : typeof globalThis !== "undefined" ? globalThis : this, function (root) {
  "use strict";
  const MM = 25.4 / 72, PT = 72 / 25.4;
  const ROLES = new Set(["cut", "line", "hatch", "art"]), METALS = ["gold", "silver", "rose", "gold10k", "gold14k"];
  const BLACK = [0, 0, 0], RED = [1, 0, 0], BLUE = [0, 0, 1], HAIR_PT = 0.25;
  const LAYER = { cut: "CUT", line: "ENGRAVE", fill: "HATCH" };
  const ORDERLESS = /^CUSTOM-\d{8}-\d{1,6}$/;
  const I = [1, 0, 0, 1, 0, 0];
  const isMat = m => Array.isArray(m) && m.length === 6 && m.every(Number.isFinite);
  // SVG order, as the editor's model: mul(m, n) applies n first, then m
  const mul = (m, n) => [m[0] * n[0] + m[2] * n[1], m[1] * n[0] + m[3] * n[1], m[0] * n[2] + m[2] * n[3], m[1] * n[2] + m[3] * n[3], m[0] * n[4] + m[2] * n[5] + m[4], m[1] * n[4] + m[3] * n[5] + m[5]];
  const ap = (m, p) => [m[0] * p[0] + m[2] * p[1] + m[4], m[1] * p[0] + m[3] * p[1] + m[5]];
  const inv = m => { const d = m[0] * m[3] - m[1] * m[2]; if (!(Math.abs(d) > 1e-12)) return null; return [m[3] / d, -m[1] / d, -m[2] / d, m[0] / d, (m[2] * m[5] - m[3] * m[4]) / d, (m[1] * m[4] - m[0] * m[5]) / d]; };
  const q3 = v => Math.round(v * 1000) / 1000;
  const f3 = v => { const r = q3(v); return (Object.is(r, -0) ? 0 : r).toFixed(3); };
  const fw = v => String(q3(v));   // (a number as the files write it: charm-nest-pdf.js syntheticOps' own)
  const finite = p => Array.isArray(p) && p.length >= 2 && Number.isFinite(+p[0]) && Number.isFinite(+p[1]);
  const editorModel = () => (root && root.CharmEditor && root.CharmEditor.model) || (typeof require === "function" ? (() => { try { return require("./charm-nest-charm-editor.js").model; } catch (_) { return null; } })() : null);

  /** An order id (or a charm, a pool row) that is no order of the shop: a custom piece's own number. */
  function orderless(x) {
    const v = x && typeof x === "object" ? (x.frozen && x.frozen.orderless ? x.frozen.customId : x.orderId || x.order || "") : x;
    return ORDERLESS.test(String(v == null ? "" : v).split("/")[0]);
  }
  const frozenOf = c => (c && c.frozen && typeof c.frozen === "object" && c.frozen.geomHash ? c.frozen : null);
  const isFrozen = c => !!frozenOf(c);

  /* ── 1 · the design's parts ── */
  /** A path the files can carry: m first, then l / c / h, finite numbers. */
  function okSubs(subs) {
    if (!Array.isArray(subs) || !subs.length) return false;
    for (const sp of subs) {
      if (!Array.isArray(sp) || !sp.length || !Array.isArray(sp[0]) || sp[0][0] !== "m") return false;
      for (const o of sp) {
        if (!Array.isArray(o)) return false;
        if (o[0] === "m" || o[0] === "l") { if (!finite(o[1])) return false; }
        else if (o[0] === "c") { if (!finite(o[1]) || !finite(o[2]) || !finite(o[3])) return false; }
        else if (o[0] !== "h") return false;
      }
    }
    return true;
  }
  const mapSubs = (subs, fn) => subs.map(sp => sp.map(o => (o[0] === "h" ? ["h"] : [o[0]].concat(o.slice(1).map(fn)))));
  /** A colour the editor says: [r, g, b] (0..1 or 0..255), "#rgb" / "#rrggbb", true (the role's own), null / false / "none" (none); undefined when it says nothing. */
  function colour(v) {
    if (v === undefined) return undefined;
    if (v === null || v === false || v === "none" || v === "") return null;
    if (Array.isArray(v) && v.length >= 3 && v.slice(0, 3).every(x => Number.isFinite(+x))) { const k = v.slice(0, 3).some(x => +x > 1) ? 255 : 1; return v.slice(0, 3).map(x => Math.max(0, Math.min(1, +x / k))); }
    const m = /^#([0-9a-f]{3}|[0-9a-f]{6})$/i.exec(String(v).trim());
    if (m) { const h = m[1].length === 3 ? m[1].split("").map(c => c + c).join("") : m[1]; return [0, 2, 4].map(i => parseInt(h.slice(i, i + 2), 16) / 255); }
    return true;
  }
  const chromatic = c => Array.isArray(c) && Math.max(c[0], c[1], c[2]) - Math.min(c[0], c[1], c[2]) > 0.15;
  const grey = c => Array.isArray(c) && Math.max(c[0], c[1], c[2]) - Math.min(c[0], c[1], c[2]) <= 0.02;
  /** The pen a part of `role` is written with, as the laser knows them (charm-nest-art-edit.js paintOf's rules): { kind: "cut" | "line" | "fill", stroke, fill, lwPt, even }. */
  function paintOf(role, it) {
    const s = colour(it.stroke), f = colour(it.fill), lw = +it.lwPt > 0 ? +it.lwPt : HAIR_PT, even = it.even === true || it.evenOdd === true || it.fillRule === "evenodd";
    if (role === "cut") return { kind: "cut", stroke: BLACK, fill: null, lwPt: lw, even: false };
    const line = () => ({ kind: "line", stroke: chromatic(s) ? s : RED, fill: null, lwPt: lw, even: false });   // (a black stroke is a cut line to the laser: an engraved line is never black)
    // (D2: a traced level keeps its grey, the darkest black; a fill that names no grey is the hatching blue)
    const own = () => ({ kind: "fill", stroke: null, fill: Array.isArray(f) && (grey(f) || chromatic(f)) ? f : BLUE, lwPt: null, even });
    if (role === "line") return line();
    if (role === "hatch") return own();
    return (f !== undefined ? !!f : !s) ? own() : line();
  }
  /** An item's own matrix (geometry -> its parent): the one the editor hands back, else made from tx ty sx sy rot about its tight box's centre (the editor's own rule). */
  function matrixOf(it, geo) {
    if (isMat(it.matrix)) return it.matrix;
    const none = !(+it.tx) && !(+it.ty) && !(+it.rot) && (it.sx == null || +it.sx === 1) && (it.sy == null || +it.sy === 1);
    if (none) return I;
    if (geo && typeof geo.matrix === "function") return geo.matrix(it);
    throw new Error(`the part ${it.name || it.id} is turned or sized and the charm editor's model is not loaded here`);
  }
  /**
   * The parts of a FaceDoc as the laser will cut and engrave them: [{ id, role, kind, subpaths (the face's points), stroke, fill, lwPt, even }], in z-order. A hidden part is not
   * engraved (what the person sees is what the laser does; a hidden cut line is still cut). A text carries its outlines placed in the face already (the editor's result).
   * opts.textToPaths: the font engine, for a text saved without its outlines. → { parts, skipped: [names] }
   */
  function partsOf(face, opts) {
    const M = editorModel(), geo = M && typeof M.createGeo === "function" ? M.createGeo(opts && opts.textToPaths) : null, mm = M && M.mul ? M.mul : mul;
    const parts = [], skipped = [];
    const walk = (items, P, hid) => {
      for (const it of items || []) {
        if (!it || typeof it !== "object") continue;
        const off = !!(hid || it.hidden);
        if (it.kind === "group") { walk(it.children, mm(P, matrixOf(it, geo)), off); continue; }
        const role = ROLES.has(it.role) ? it.role : "art";
        if (off && role !== "cut") continue;
        let subs = null;
        if (it.kind === "text") subs = okSubs(it.subpaths) ? it.subpaths : geo && M.outlinesOf ? M.outlinesOf(it, geo, P) : null;   // (the editor's result places a text's outlines in the face, groups included)
        else if (okSubs(it.subpaths)) { const W = mm(P, matrixOf(it, geo)); subs = mapSubs(it.subpaths, p => ap(W, p)); }
        if (!okSubs(subs)) { if (it.kind !== "text" || String(it.text || "").trim()) skipped.push(String(it.text || it.name || it.id || "a part")); continue; }
        const pen = paintOf(role, it);
        // (a cut line is a closed line: every subpath of it ends closed)
        if (pen.kind === "cut") subs = subs.map(sp => (sp[sp.length - 1][0] === "h" ? sp : sp.concat([["h"]])));
        parts.push(Object.assign({ id: String(it.id == null ? parts.length : it.id), role, subpaths: subs }, pen));
      }
    };
    walk(face && face.items, I, false);
    return { parts, skipped };
  }
  /** The bounding box of parts (their points, control points included: the page around them only needs to hold them). */
  function boxOf(parts) {
    let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
    for (const pt of parts) for (const sp of pt.subpaths) for (const o of sp) for (let i = 1; i < o.length; i++) { const p = o[i]; if (p[0] < x0) x0 = p[0]; if (p[0] > x1) x1 = p[0]; if (p[1] < y0) y0 = p[1]; if (p[1] > y1) y1 = p[1]; }
    return x0 <= x1 ? [x0, y0, x1, y1] : null;
  }
  /**
   * The design's front as the piece is written: its parts in the charm's own points (y up), placed on a page of their own with room round them (as a lone charm's file is,
   * buildSingleCharm: an eighth of its larger side, so it is never read as an artboard frame), every number on the thousandth of a point. → { parts, page: { wPt, hPt }, cut, widthMm, heightMm, skipped }
   */
  function leavesOf(design, opts) {
    const face = design && design.faces && design.faces.front;
    if (!face || !Array.isArray(face.items) || !face.items.length) throw new Error("the design has no front to cut");
    const { parts, skipped } = partsOf(face, opts);
    const up = parts.map(pt => Object.assign({}, pt, { subpaths: mapSubs(pt.subpaths, p => [+p[0], -p[1]]) }));
    const cuts = up.filter(pt => pt.kind === "cut");
    if (!cuts.length) throw new Error("the design has no cut line: a piece is the shape its cut outline gives it");
    const cb = boxOf(cuts), all = boxOf(up), lw = Math.max(...up.map(pt => +pt.lwPt || 0), HAIR_PT);
    const pad = Math.max(lw / 2 + 2, 0.125 * Math.max(cb[2] - cb[0], cb[3] - cb[1]));
    const dx = pad - all[0], dy = pad - all[1];
    const out = up.map(pt => Object.assign({}, pt, { subpaths: mapSubs(pt.subpaths, p => [q3(p[0] + dx), q3(p[1] + dy)]) }));
    return { parts: out, page: { wPt: q3(all[2] - all[0] + 2 * pad), hPt: q3(all[3] - all[1] + 2 * pad) }, cut: cb, widthMm: (cb[2] - cb[0]) * MM, heightMm: (cb[3] - cb[1]) * MM, skipped };
  }

  /* ── 2 · the hash of a piece's parts ── */
  /** One part as one line: stroke ("s") or fill ("f"), then its subpaths on the thousandth of a point. Colours are left out: the writer turns a black fill blue (the hatching
   *  rule), and what is checked is the shape the laser follows. */
  function keyOf(kind, subpaths) {
    return kind + "|" + subpaths.map(sp => sp.map(o => o[0] + o.slice(1).map(p => f3(p[0]) + "," + f3(p[1])).join(" ")).join("")).join("/");
  }
  const kindOfPen = pt => (pt.kind === "fill" ? "f" : "s");
  const kindOfSeg = s => (s.fill && s.stroke ? "b" : s.fill ? "f" : "s");
  // (two 53-bit string hashes, cyrb53: a check that the parts are the same, not a secret)
  function h53(str, seed) {
    let h1 = 0xdeadbeef ^ seed, h2 = 0x41c6ce57 ^ seed;
    for (let i = 0; i < str.length; i++) { const ch = str.charCodeAt(i); h1 = Math.imul(h1 ^ ch, 2654435761); h2 = Math.imul(h2 ^ ch, 1597334677); }
    h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507) ^ Math.imul(h2 ^ (h2 >>> 13), 3266489909);
    h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507) ^ Math.imul(h1 ^ (h1 >>> 13), 3266489909);
    return (4294967296 * (2097151 & h2) + (h1 >>> 0)).toString(16).padStart(14, "0");
  }
  /** The hash of a list of part keys (order-free: the writer may write them in another order). */
  function hashOf(keys) { const s = keys.slice().sort().join("\n"); return "g1-" + h53(s, 7) + h53(s, 1031) + "-" + keys.length; }
  const leafKeys = parts => parts.map(pt => keyOf(kindOfPen(pt), pt.subpaths));

  /* ── 3 · the page of the design (a PDF the reader and the writer take as any design file) ── */
  async function pdfOf(L, title) {
    const PL = root && root.PDFLib; if (!PL) throw new Error("the PDF library is not loaded");
    const { PDFDocument, PDFName, PDFString } = PL;
    const doc = await PDFDocument.create();
    doc.setTitle(title || "Custom design"); doc.setProducer("Brites Charm Nesting Station"); doc.setCreator("Brites Charm Nesting Station · Design & Engrave");
    const page = doc.addPage([L.page.wPt, L.page.hPt]);
    page.node.normalize();
    const props = doc.context.obj({}), refs = [], tags = {};
    page.node.Resources().set(PDFName.of("Properties"), props);
    const tagOf = name => { if (tags[name]) return tags[name]; const ref = doc.context.register(doc.context.obj({ Type: "OCG", Name: PDFString.of(name) })); refs.push(ref); tags[name] = "L" + refs.length; props.set(PDFName.of(tags[name]), ref); return tags[name]; };
    let ops = "";
    for (const pt of L.parts) {
      const s = pt.stroke || BLACK, f = pt.fill || BLACK;
      ops += `/OC /${tagOf(LAYER[pt.kind])} BDC q ${fw(s[0])} ${fw(s[1])} ${fw(s[2])} RG ${fw(f[0])} ${fw(f[1])} ${fw(f[2])} rg ${fw(pt.lwPt || HAIR_PT)} w `;
      for (const sp of pt.subpaths) for (const o of sp) { if (o[0] === "m" || o[0] === "l") ops += `${fw(o[1][0])} ${fw(o[1][1])} ${o[0]} `; else if (o[0] === "c") ops += `${fw(o[1][0])} ${fw(o[1][1])} ${fw(o[2][0])} ${fw(o[2][1])} ${fw(o[3][0])} ${fw(o[3][1])} c `; else ops += "h "; }
      ops += (pt.kind === "fill" ? (pt.even ? "f*" : "f") : "S") + " Q EMC\n";
    }
    page.node.set(PDFName.of("Contents"), doc.context.register(doc.context.flateStream(ops)));
    doc.catalog.set(PDFName.of("OCProperties"), doc.context.obj({ OCGs: refs, D: { Order: refs, ON: refs, BaseState: "ON" } }));
    return await doc.save({ useObjectStreams: false });
  }

  /* ── 4 · read back (page) ── */
  const page_ = () => (root && root.CN) || null;
  const P_ = () => (root && root.CharmNestPDF) || null;
  /** The design's page read as any custom design file is (CustomSheet's reader): parse, group, loose ink within reach of the piece joined to it, rings, silhouettes. It must be ONE piece. */
  async function readOf(bytes, name) {
    const P = P_(), S = page_().S, parsed = await P.parseSource(bytes, name);
    const g = await (P.groupCharmsAsync || P.groupCharms)(parsed, { minPt: +S.settings.minPt || 6 });
    for (const m of g.orphans || []) {
      if (!m.bbox) continue;
      const c = g.charms.find(c => { const b = c.bbox, pad = 2; return m.bbox[0] >= b[0] - pad && m.bbox[1] >= b[1] - pad && m.bbox[2] <= b[2] + pad && m.bbox[3] <= b[3] + pad; });
      if (c && !c.members.includes(m)) c.members.push(m);
    }
    const charms = g.charms.filter(c => !c.open);
    if (charms.length !== 1) throw new Error(charms.length ? `the design reads as ${charms.length} separate pieces: a custom piece is ONE closed cut outline (draw the parts inside it, or send them as designs of their own)` : "its cut line is not closed: a piece needs one closed cut outline");
    const left = (g.orphans || []).filter(m => !charms[0].members.includes(m) && (m.kind === "path" || m.kind === "text"));
    if (left.length) throw new Error(`${left.length === 1 ? "a part of the design lies" : left.length + " parts of the design lie"} outside its cut outline: they would not be on the piece`);
    { const r = P.integrateRings(charms[0]); if (r.left.length) throw new Error("a hoop could not join its piece — " + r.left[0]); }
    await P.buildSilhouettes(parsed, charms, +S.settings.silhouetteRes || 6);
    return { parsed, group: g, charm: charms[0] };
  }

  /* ── 5 · the parts as a written sheet file has them ── */
  function contentOf(PL, doc, page) {
    const c = page.node.Contents(); if (!c) return new Uint8Array(0);
    const list = c instanceof PL.PDFArray ? c.asArray().map(r => doc.context.lookup(r)) : [doc.context.lookup(c) || c];
    const parts = list.filter(Boolean).map(s => (s instanceof PL.PDFRawStream ? PL.decodePDFRawStream(s).decode() : s.getContents ? s.getContents() : new Uint8Array(0)));
    const n = parts.reduce((a, b) => a + b.length + 1, 0), out = new Uint8Array(n); let at = 0;
    for (const p of parts) { out.set(p, at); at += p.length; out[at++] = 10; }
    return out;
  }
  /**
   * The parts of the charms written as forms "Charm<i>" (buildSheet's names) for the placement indexes `want`, read back from the bytes of a sheet's file: Map i -> { keys, det }
   * (keys: each part's key in the charm's own points, the placement's matrix undone; det: the scale it was written at, squared). Only those forms are read.
   */
  async function writtenOf(bytes, want) {
    const PL = root.PDFLib, P = P_(), doc = await PL.PDFDocument.load(bytes, { ignoreEncryption: true, updateMetadata: false }), page = doc.getPage(0);
    const xo = page.node.Resources() && page.node.Resources().lookup(PL.PDFName.of("XObject")), forms = new Map();
    if (xo && xo.entries) for (const [k, v] of xo.entries()) {
      const nm = k.decodeText(), m = /^Charm(\d+)(?:-|$)/.exec(nm); if (!m || !want.has(+m[1])) continue;
      const s = doc.context.lookup(v); if (!s || !(s instanceof PL.PDFRawStream)) continue;
      forms.set(nm, { i: +m[1], x: { subtype: "Form", bytes: PL.decodePDFRawStream(s).decode(), matrix: I.slice(), bbox: null } });
    }
    const out = new Map(); if (!forms.size) return out;
    const { segs } = P.interpret(contentOf(PL, doc, page), name => (forms.get(name) || {}).x || null, I.slice(), 0, {}, {}, {});
    for (const s of segs) {
      const f = s.kind === "xobj" && forms.get(s.name); if (!f) continue;
      const keys = [];
      for (const k of s.children || []) {
        if (k.kind !== "path" || !(k.stroke || k.fill) || !Array.isArray(k.subpaths)) continue;
        const back = inv(isMat(k.ctm) ? k.ctm : s.matrix) || I;
        keys.push(keyOf(kindOfSeg(k), mapSubs(k.subpaths, p => ap(back, p))));
      }
      const m = s.matrix; out.set(f.i, { keys, det: m ? Math.abs(m[0] * m[3] - m[1] * m[2]) : 0 });
    }
    return out;
  }

  /* ── 6 · the piece's design, made once a session (one pool source per design, version and geometry) ── */
  const making = new Map();
  const safe = s => String(s || "design").replace(/[^\w.\-]+/g, "_").slice(0, 60);
  /**
   * The design as a piece: { src (the pool source: its page, parsed), charm (the piece as read), geomHash, widthMm, heightMm, parts }. Refused, in words a person reads, when
   * the design cannot be cut exactly as it was made.
   */
  function sourceFor(design, version, opts) {
    const L = leavesOf(design, opts), want = hashOf(leafKeys(L.parts)), k = `${design.id}|${version}|${want}`;
    if (making.has(k)) return making.get(k);
    const task = makeSource(design, version, L, want).catch(e => { making.delete(k); throw e; });
    making.set(k, task); return task;
  }
  async function makeSource(design, version, L, want) {
    const CN = page_(), P = P_(), S = CN.S;
    if (L.skipped.length) throw new Error(`${L.skipped.length === 1 ? "a part" : L.skipped.length + " parts"} of the design could not be read (${L.skipped.slice(0, 3).join(", ")}): open it in the Charm Editor and save it again`);
    const id = "cdes:" + want.slice(3, 19), had = S.poolSources && S.poolSources[id];
    if (had && had.charms && had.charms[0] && had.charms[0].frozen && had.charms[0].frozen.geomHash) { had.usedAt = Date.now(); return { src: had, charm: had.charms[0], geomHash: had.charms[0].frozen.geomHash, widthMm: L.widthMm, heightMm: L.heightMm, parts: L.parts.length }; }
    const name = `${safe(design.name || design.id)}-v${version}.pdf`;
    const bytes = await pdfOf(L, `${design.name || "Custom design"} · v${version}`);
    const { parsed, group, charm } = await readOf(bytes, name);
    const src = { id, pool: true, custom: true, design: true, name: `${design.name || "Custom design"} (design ${design.id} v${version})`, sku: "CUSTOM", bytes, hash: (CN.sha256 ? await CN.sha256(bytes) : want), parsed, group, charms: [charm], metal: null, state: "ready", t0: performance.now(), cloud: null, persisting: null, usedAt: Date.now() };
    Object.assign(charm, { id: id + ":0", sourceId: id, sourceName: src.name, index: 0, name: design.name || "Custom design", sku: "CUSTOM", namedBy: "custom", excluded: false, cloud: null, upAngle: null, engravable: false, backKeepOut: null, custom: true });
    // the piece written alone by the sheet writer, and read back from that file: what the laser gets must be the design, every part of it
    const one = { charm, angle: 0, cxPt: L.page.wPt, cyPt: L.page.hPt, scale: 1 };
    const sheet = await P.buildSheet({ sheet: { wPt: 2 * L.page.wPt, hPt: 2 * L.page.hPt, strokeRGB: [1, 0, 0], strokePt: 0.5 }, placements: [one], sources: new Map([[id, parsed]]), title: "custom piece check" });
    const got = (await writtenOf(sheet, new Set([0]))).get(0);
    if (!got) throw new Error("the sheet writer did not write the design: it cannot be cut as it was made");
    const geomHash = hashOf(got.keys);
    if (geomHash !== want) {
      const mine = new Set(leafKeys(L.parts)), theirs = new Set(got.keys), lost = [...mine].filter(x => !theirs.has(x)).length, extra = [...theirs].filter(x => !mine.has(x)).length;
      throw new Error(`the piece would not be cut exactly as designed (${lost ? `${lost} of its ${mine.size} parts changed or left out` : ""}${lost && extra ? ", " : ""}${extra ? `${extra} part${extra === 1 ? "" : "s"} the design does not have` : ""}): it is not sent. Open it in the Charm Editor: a ring or a part outside the outline is usually the cause`);
    }
    charm.frozen = { geomHash };   // (the piece's own fields follow per copy; the source's charm keeps its hash so the next send of it is not read again)
    S.poolSources[id] = src;
    return { src, charm, geomHash, widthMm: L.widthMm, heightMm: L.heightMm, parts: L.parts.length };
  }

  /* ── 7 · Pool.addCustomPiece ── */
  const words = s => String(s == null ? "" : s).replace(/\s+/g, " ").trim();
  const liveRun = () => { const r = root.B && root.B.run; return r && !["complete", "stopped", "abandoned"].includes(r.status) ? r : null; };
  const lineCuts = m => m === "rose" || (typeof root.CharmNestRose !== "undefined" && typeof root.CharmNestRose.cuts === "function" && root.CharmNestRose.cuts(m));
  /** The next open sheet of `metal` for these pieces, as an arriving order's pieces find theirs (Pool.attachPool's rules): the order's own sheet of the metal (R16), else the
   *  intake's sheet (LiveNest.intakePage), a new one when that is closed or another run's. */
  function sheetFor(metal, run, rid, list) {
    const CN = page_(), LN = root.LiveNest, pages = () => CN.pagesOf(metal), shut = p => !!(LN && LN.closed && LN.closed(p)) || !!p.laserDoneAt || !!p.roseCutAt || !!p.recalled;
    let page = null, home = false;
    if (rid && LN && typeof LN.orderHome === "function") { page = LN.orderHome(metal, run, rid, { skip: list }); if (page) { home = true; if (typeof LN.reopenHome === "function") LN.reopenHome(page, rid); } }
    if (!page) page = LN && typeof LN.intakePage === "function" ? LN.intakePage(metal, run, rid || list[0].order, list) : pages()[pages().length - 1];
    if (!home && (!page || (run && page.runId && page.runId !== run.runId) || shut(page))) page = CN.addPage(metal);
    if (!home && rid && lineCuts(metal) && root.Pool && typeof root.Pool.greenFits === "function" && !root.Pool.greenFits(page, rid, run)) {   // (Decision 2: one GF and/or one SS partner sheet, in one set)
      const alt = pages().find(p => p !== page && !shut(p) && (!run || !p.runId || p.runId === run.runId) && root.Pool.greenFits(p, rid, run));
      page = alt || CN.addPage(metal);
    }
    return page;
  }
  /** The pieces join their sheet and are nested there: at once, or, while that sheet is being nested, in its next turn (feedOn). */
  function placeOn(page, list, run) {
    const CN = page_(), S = CN.S, metal = page.metal;
    if (run) page.runId = run.runId; else if (!page.runId) page.runId = CN.uid ? CN.uid() : "custom-" + Date.now().toString(36);
    S.sheets[metal].active = CN.pagesOf(metal).indexOf(page); if (!page.el && CN.showPage) CN.showPage(metal, S.sheets[metal].active);
    for (const c of list) if (!page.charms.includes(c)) page.charms.push(c);
    const busy = ["nesting", "finishing"].includes(page.status);
    if (busy) page.feedWait = [...new Set((page.feedWait || []).concat(list.map(c => c.id)))];
    if (page.placements.length) { page.intakeAppend = true; page.appendOnly = true; page.dirty = true; if (!busy && page.status !== "queued") page.status = "ready"; CN.renderCard(page); }
    else CN.sheetDirty(page);
    if (!busy && page.status !== "queued" && (run || S.settings.autoNest !== "off")) { page._byHand = !run; CN.startNest(page); }
    else if (run && root.RunCtl && typeof root.RunCtl.poke === "function") root.RunCtl.poke();
  }
  /**
   * Pool.addCustomPiece({ design, version, hash, metal, orderId, customName, quantity, by, fixes? }) -> Promise<poolIds>
   *   design     the design library's design ({ id, name, faces: { front, back? }, widthMm, heightMm }); version, hash: its version and the library's hash of it
   *   metal      gold | silver | rose | gold10k | gold14k (D5: the colour of the sheet)
   *   orderId    the order the piece belongs to, or null: then customName names it, and it is numbered CUSTOM-<yyyymmdd>-<n>
   *   quantity   1 to 99 copies;  by: who sent it (the signed-in person);  fixes: { orderId, poolId, causedBy } when it fixes a mistake (D7), kept on its pool rows
   */
  async function addCustomPiece(o) {
    o = o || {};
    const CN = page_(), B = root.B;
    if (!CN || !CN.S || !B || !root.CharmNestPDF) throw new Error("the sorter is not loaded");
    const design = o.design, version = Math.max(0, Math.floor(+o.version) || 0), metal = String(o.metal || ""), qty = Math.floor(+o.quantity || 1);
    const rid = o.orderId == null || o.orderId === "" ? "" : String(o.orderId).split("/")[0].trim(), name = words(o.customName).slice(0, 80), by = words(o.by || B.employee);
    if (!design || !design.id || !design.faces || !design.faces.front) throw new Error("no design was given (its id and its front)");
    if (!METALS.includes(metal)) throw new Error(`pick the sheet's metal (${METALS.join(", ")})`);
    if (!(qty >= 1 && qty <= 99)) throw new Error("send 1 to 99 copies at a time");
    if (rid && !/^\d{5,20}$/.test(rid)) throw new Error(`${rid} is not an order number`);
    if (!rid && !name) throw new Error("a piece that belongs to no order needs a name, so it can be found on its sheet");
    if (!by) throw new Error("nobody is signed in: sign in first, the piece is recorded under your name");
    if (!CN.S.cloud.ok) throw new Error("the cloud cannot be reached: a custom piece is numbered and recorded there first. Send it again once it is back");
    const made = await sourceFor(design, version, o);
    const P = root.CharmNestPDF, S = CN.S, st = CN.stockFor ? CN.stockFor(metal) : null, inset = +S.settings.insetPt || 0, c0 = made.charm;
    if (st && (c0.areaPt2 > (st.wPt - 2 * inset) * (st.hPt - 2 * inset) * (+S.settings.maxFill || 0.8) || Math.min(c0.widthPt, c0.heightPt) > Math.max(st.wPt, st.hPt) - 2 * inset))
      throw new Error(`the piece (${(c0.widthPt * MM).toFixed(1)} × ${(c0.heightPt * MM).toFixed(1)} mm) does not fit the ${CN.labelOf ? CN.labelOf(metal) : metal} plate`);
    // the design's file, kept once per design and version (a sheet holding the piece writes its files from it on any computer)
    if (!made.src.cloud && CN.uploadBytes) { try { made.src.cloud = await CN.uploadBytes(`charmnest/custom/designs/${safe(design.id)}/v${version}-${made.geomHash.slice(3, 15)}.pdf`, made.src.bytes, "application/pdf", "Saving the custom design"); } catch (e) { console.warn("[custom piece] design file not kept", e); } }
    const cloud = made.src.cloud;
    const run = liveRun(), req = "cp" + Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
    const ask = () => CN.api("charmNestLibrary", { op: "customPiecePut", requestId: req, by, piece: { designId: String(design.id), version, designHash: o.hash || null, geomHash: made.geomHash, customName: name, metal, quantity: qty, orderId: rid || null,
      widthMm: made.widthMm, heightMm: made.heightMm, aiPath: (cloud && cloud.path) || null, charmHash: c0.hash || null, runId: run ? run.runId : null, setId: run ? run.setId || null : null, day: CN.today ? CN.today() : null, fixes: o.fixes || null } }, { label: "Numbering the custom piece" });
    let rep; try { rep = await ask(); } catch (e) { if (!e || !(e.transient || +e.status >= 500)) throw e; rep = await ask(); }   // (the same request again: the server gives it the same number)
    if (!rep || !rep.ok || !Array.isArray(rep.pools) || !rep.pools.length) throw new Error((rep && rep.error) || "the piece was not recorded");
    const at = Date.now(), n = rep.pools.length, list = [];
    for (const r of rep.pools) {
      const c = Object.assign({}, c0, { id: `${made.src.id}:${r.poolId}`, pinned: null }); delete c.frontAt; delete c.manual;
      Object.assign(c, {
        name: (rep.orderless ? `${name} · Custom · no order` : `${rid} · Custom · ${name || design.name || "design"}`) + (n > 1 ? ` · ${r.copy}/${n}` : ""),
        custom: true, order: r.orderId, orderDate: +r.orderDate || at, arrivedAt: +r.arrivedAt || at, poolId: r.poolId, metal, lineKey: r.lineKey, excluded: false, engravable: false, backKeepOut: null, sku: "CUSTOM",
        orderInfo: { receiptId: r.orderId, transactionId: r.transactionId, sku: "CUSTOM", copy: r.copy, quantity: n, custom: true, file: name || design.name || "", ...(rep.orderless ? { orderless: true } : {}) },
        cloud: cloud ? { ai: cloud.url, aiPath: cloud.path, png: null, pngPath: null } : null,
        frozen: { designId: String(design.id), version, designHash: o.hash || null, geomHash: made.geomHash, widthMm: +made.widthMm.toFixed(3), heightMm: +made.heightMm.toFixed(3), customId: rep.customId, name: name || null, orderless: !!rep.orderless, by, at }
      });
      list.push(c);
      B.pool.rows.set(r.poolId, Object.assign({}, r, { updatedAt: at }));
    }
    const page = sheetFor(metal, run, rid, list);
    placeOn(page, list, run);
    try { root.Session && root.Session.schedule && root.Session.schedule(); } catch (_) { /* kept with the next checkpoint */ }
    const sheetWords = (CN.labelOf ? CN.labelOf(metal) : metal) + (page.page ? ` sheet ${page.page}` : "");
    try { CN.agent({ metal, pool: true }, "POOL", `${rep.orderless ? `${rep.customId} "${name}" (no order)` : `Order ${rid}`} · custom design ${design.id} v${version} × ${n} → ${sheetWords} at its exact size (${made.widthMm.toFixed(2)} × ${made.heightMm.toFixed(2)} mm), sent by ${by}`); } catch (_) {}
    try { root.CNDesignLog && root.CNDesignLog.event("customPiecePooled", { designId: String(design.id), version, orderId: rep.orderId, customId: rep.customId, orderless: !!rep.orderless, name, metal, quantity: n, poolIds: rep.poolIds, sheet: page.sheetId || null, by, ...(o.fixes ? { fixes: o.fixes } : {}) }); } catch (_) {}
    return rep.poolIds.slice();
  }

  /* ── 8 · sanctity on the sheet (the page's nest and the sheet window call these) ── */
  /** Every frozen piece of a sheet written at its exact size (finishNest, after the shrink every other charm gets). → how many. */
  function holdSize(placements, byId) {
    let n = 0;
    for (const p of placements || []) { const c = byId && byId.get ? byId.get(p.id) : null; if (isFrozen(c)) { if (p.scale !== 1) n++; p.scale = 1; delete p.fixScale; } }
    return n;
  }
  /** Why a piece's size cannot change ("" when it can): a custom design sent at its exact size. */
  function sizeWhy(c) {
    const f = frozenOf(c); if (!f) return "";
    return `${f.orderless && f.name ? `"${f.name}"` : "This custom piece"} keeps the exact size it was designed at (${f.widthMm != null ? (+f.widthMm).toFixed(2) : "?"} × ${f.heightMm != null ? (+f.heightMm).toFixed(2) : "?"} mm, design ${f.designId} v${f.version}): it can be moved and turned, not resized.`;
  }
  /**
   * The files a sheet is about to be saved with, read back for its frozen pieces (finishNest, before the save): each written at 100 % and with exactly the parts of its hash.
   * spec: the spec the .ai was written from (its placements carry the charms, their scale and their layer). → { ok, n, why? } (why: one sentence for the card and the toast)
   */
  async function checkWritten(sh, bytes, spec) {
    const list = ((spec && spec.placements) || []).map((pl, i) => ({ pl, i, c: pl.charm })).filter(x => isFrozen(x.c));
    if (!list.length) return { ok: true, n: 0 };
    const bad = [], nm = c => (frozenOf(c).orderless && frozenOf(c).name ? `"${frozenOf(c).name}"` : c.name || c.poolId || "a custom piece");
    for (const x of list) if (Math.abs((+x.pl.scale || 1) - 1) > 1e-9) bad.push(`${nm(x.c)} would be written at ${(x.pl.scale * 100).toFixed(1)} % of its size`);
    let got = null; try { got = await writtenOf(bytes, new Set(list.map(x => x.i))); } catch (e) { bad.push(`the sheet's file could not be read back (${(e && e.message) || e})`); }
    if (got) for (const x of list) {
      const w = got.get(x.i), f = frozenOf(x.c);
      if (!w) { bad.push(`${nm(x.c)} is not in the file`); continue; }
      if (Math.abs(Math.sqrt(w.det) - 1) > 1e-4) bad.push(`${nm(x.c)} is written at ${(Math.sqrt(w.det) * 100).toFixed(2)} % of its size`);
      else if (hashOf(w.keys) !== f.geomHash) bad.push(`${nm(x.c)}'s parts in the file are not its design's (design ${f.designId} v${f.version}: ${f.geomHash} expected, ${hashOf(w.keys)} written)`);
    }
    if (!bad.length) return { ok: true, n: list.length };
    return { ok: false, n: list.length, bad, why: `Not saved: a custom piece is not written exactly as it was designed — ${bad.slice(0, 2).join("; ")}${bad.length > 2 ? ` and ${bad.length - 2} more` : ""}` };
  }

  /* ── 9 · the sheet's QR label (Sets.onSheetSaved / labelsReady call these) ── */
  /** The orders a label lists for the stations to sort and ship: the shop's orders only (a piece that belongs to no order has no shipping of its own). */
  const shipIds = ids => (ids || []).filter(id => !orderless(id));
  /** The words a sheet's label carries under its title for its pieces that belong to no order: "Custom · no order: <name>" (as many as there are, the count past three). */
  function labelNotes(placed) {
    const names = []; for (const c of placed || []) { const f = frozenOf(c); if (f && f.orderless) names.push(f.name || f.customId); }
    if (!names.length) return [];
    const uniq = [...new Set(names)], shown = uniq.slice(0, 2).map(n => `Custom · no order: ${n}`);
    return uniq.length > 2 ? shown.concat([`+ ${uniq.length - 2} more custom piece${uniq.length - 2 === 1 ? "" : "s"} (no order)`]) : shown;
  }

  const api = { ORDERLESS, orderless, isFrozen, frozenOf, partsOf, leavesOf, keyOf, hashOf, leafKeys, paintOf, pdfOf, readOf, writtenOf, sourceFor, addCustomPiece, holdSize, sizeWhy, checkWritten, shipIds, labelNotes, version: 1 };
  // (the contract's own door: Pool.addCustomPiece; the bridge's Pool is made before this file loads)
  if (root && root.Pool && !root.Pool.addCustomPiece) root.Pool.addCustomPiece = addCustomPiece;
  return api;
});
