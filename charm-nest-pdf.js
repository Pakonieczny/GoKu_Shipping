/*  charm-nest-pdf.js — everything that touches the PDF side of a charm sheet.
 *  ═══════════════════════════════════════════════════════════════════════
 *  Browser only. Depends on the vendored pdf-lib (window.PDFLib) for
 *  reading and writing, and on pdf.js (window.pdfjsLib) for the independent
 *  render-based verification of the written file.
 *
 *    parseSource(bytes, name)      → { pageW, pageH, mediaBox, segments, … }
 *    groupCharms(parsed, opts)     → { charms, frame, orphans }
 *    buildSilhouettes(parsed, charms, scale) → fills in bits/area/hash/thumb
 *    buildSheet(spec)              → { bytes, labelledBytes, report }
 *    buildSingleCharm(charm)       → bytes (one charm, alone, own artboard)
 *    verifyRendered(bytes, spec)   → render each optional-content layer alone
 *
 *  ONE PARSER, ONE INDEX SPACE. The same tokenizer that finds the charm
 *  geometry also records every top-level drawing segment's byte range in the
 *  page content, so the writer blanks non-member segments byte-for-byte —
 *  construction operators included — and there is nothing to keep aligned
 *  between two different readers. Form XObjects are one segment each at the
 *  top level (so their `Do` is kept or blanked as a unit) and are also walked
 *  for geometry, so an outline living inside one is still found.
 *  ═══════════════════════════════════════════════════════════════════════ */
(function (root) {
  "use strict";

  /* ═══ 1 · content-stream lexer ═════════════════════════════════════════ */
  const WS = new Set([0x00, 0x09, 0x0A, 0x0C, 0x0D, 0x20]);
  const DELIM = new Set("()<>[]{}/%".split("").map(c => c.charCodeAt(0)));
  const isRegular = c => !WS.has(c) && !DELIM.has(c);

  /** Tokenise a content stream (Uint8Array) into instructions {op, args, start, end}. */
  function lex(bytes) {
    const n = bytes.length, out = [];
    let i = 0, operands = [], opStart = -1;
    const num = (s) => { const v = parseFloat(s); return Number.isFinite(v) ? v : 0; };
    function skipWs() { while (i < n && WS.has(bytes[i])) i++; }
    function readName() { let s = ""; i++; while (i < n && isRegular(bytes[i])) s += String.fromCharCode(bytes[i++]); return s.replace(/#([0-9A-Fa-f]{2})/g, (_, h) => String.fromCharCode(parseInt(h, 16))); }
    function readString() { // (…) with nesting
      let depth = 0, s = "";
      for (; i < n; i++) {
        const c = bytes[i];
        if (c === 0x5C) { i++; s += String.fromCharCode(bytes[i] || 0); continue; }
        if (c === 0x28) { depth++; if (depth === 1) continue; }
        if (c === 0x29) { depth--; if (depth === 0) { i++; break; } }
        s += String.fromCharCode(c);
      }
      return s;
    }
    function readHex() { let s = ""; i++; while (i < n && bytes[i] !== 0x3E) { const c = bytes[i++]; if (!WS.has(c)) s += String.fromCharCode(c); } i++; return s; }
    function readObject() { // returns a JS value for one operand
      skipWs(); if (i >= n) return undefined;
      const c = bytes[i];
      if (c === 0x2F) return { name: readName() };
      if (c === 0x28) return { str: readString() };
      if (c === 0x3C) { if (bytes[i + 1] === 0x3C) { i += 2; const d = {}; for (;;) { skipWs(); if (bytes[i] === 0x3E && bytes[i + 1] === 0x3E) { i += 2; break; } if (i >= n) break; const k = readObject(); const v = readObject(); if (k && k.name != null) d[k.name] = v; } return { dict: d }; } return { str: readHex() }; }
      if (c === 0x5B) { i++; const a = []; for (;;) { skipWs(); if (bytes[i] === 0x5D) { i++; break; } if (i >= n) break; a.push(readObject()); } return { arr: a }; }
      if (c === 0x25) { while (i < n && bytes[i] !== 0x0A && bytes[i] !== 0x0D) i++; return readObject(); }
      if ((c >= 0x30 && c <= 0x39) || c === 0x2B || c === 0x2D || c === 0x2E) { let s = ""; while (i < n && isRegular(bytes[i])) s += String.fromCharCode(bytes[i++]); return num(s); }
      // operator
      let s = ""; while (i < n && isRegular(bytes[i])) s += String.fromCharCode(bytes[i++]);
      if (s === "true") return true; if (s === "false") return false; if (s === "null") return null;
      return { op: s || String.fromCharCode(bytes[i++]) };
    }
    while (i < n) {
      skipWs(); if (i >= n) break;
      const start = i;
      const v = readObject();
      if (v === undefined) break;
      if (v && typeof v === "object" && v.op !== undefined) {
        let op = v.op, end = i;
        if (op === "BI") { // inline image: BI … ID <data> EI
          let j = i; // find ID
          for (; j < n - 1; j++) if (bytes[j] === 0x49 && bytes[j + 1] === 0x44 && WS.has(bytes[j + 2] || 0x20) && (j === 0 || !isRegular(bytes[j - 1]))) { j += 3; break; }
          for (; j < n - 1; j++) if (WS.has(bytes[j - 1] || 0x20) && bytes[j] === 0x45 && bytes[j + 1] === 0x49 && (j + 2 >= n || WS.has(bytes[j + 2]))) { j += 2; break; }
          i = end = j;
        }
        out.push({ op, args: operands, start: opStart >= 0 ? opStart : start, end });
        operands = []; opStart = -1;
      } else {
        if (opStart < 0) opStart = start;
        operands.push(v);
      }
    }
    return out;
  }

  /* ═══ 2 · interpreter → drawing segments ═══════════════════════════════ */
  const mul = (m, k) => [ // m × k  (apply m first, then k)
    m[0] * k[0] + m[1] * k[2], m[0] * k[1] + m[1] * k[3],
    m[2] * k[0] + m[3] * k[2], m[2] * k[1] + m[3] * k[3],
    m[4] * k[0] + m[5] * k[2] + k[4], m[4] * k[1] + m[5] * k[3] + k[5]];
  const ap = (m, x, y) => [m[0] * x + m[2] * y + m[4], m[1] * x + m[3] * y + m[5]];
  const scaleOf = m => Math.sqrt(Math.abs(m[0] * m[3] - m[1] * m[2])) || 1;
  const cmyk = (c, m, y, k) => [(1 - c) * (1 - k), (1 - m) * (1 - k), (1 - y) * (1 - k)];
  const lum = c => c ? (0.2126 * c[0] + 0.7152 * c[1] + 0.0722 * c[2]) : 0;
  const bboxOf = pts => { let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity; for (const p of pts) { if (p[0] < x0) x0 = p[0]; if (p[0] > x1) x1 = p[0]; if (p[1] < y0) y0 = p[1]; if (p[1] > y1) y1 = p[1]; } return pts.length ? [x0, y0, x1, y1] : null; };
  const bbUnion = (a, b) => !a ? b : !b ? a : [Math.min(a[0], b[0]), Math.min(a[1], b[1]), Math.max(a[2], b[2]), Math.max(a[3], b[3])];
  const bbArea = b => b ? Math.max(0, b[2] - b[0]) * Math.max(0, b[3] - b[1]) : 0;
  const bbInter = (a, b) => { const r = [Math.max(a[0], b[0]), Math.max(a[1], b[1]), Math.min(a[2], b[2]), Math.min(a[3], b[3])]; return r[2] > r[0] && r[3] > r[1] ? r : null; };

  function colorFrom(args, space, spaces) {
    const nums = args.filter(a => typeof a === "number");
    if (args.some(a => a && a.name)) return [0.5, 0.5, 0.5];      // pattern → unknown mid-grey
    const sp = spaces[space] || space;
    if (typeof sp === "string" && /^Separation|^DeviceN/.test(sp)) return nums.length ? [1 - nums[0], 1 - nums[0], 1 - nums[0]] : [0, 0, 0];
    if (nums.length === 1) return [nums[0], nums[0], nums[0]];
    if (nums.length === 3) return nums.slice(0, 3);
    if (nums.length >= 4) return cmyk(nums[0], nums[1], nums[2], nums[3]);
    return [0, 0, 0];
  }

  /**
   * Walk one content stream. `resolve` looks up resources through pdf-lib.
   * Top-level segments get byte ranges; nested (form XObject) paths are
   * returned in `inner` with `parent` = the index of their Do segment.
   */
  function interpret(bytes, resolve, ctm0, depth, spacesIn) {
    const ins = lex(bytes);
    const segs = [], inner = [];
    let ctm = ctm0.slice(), stack = [];
    let fill = [0, 0, 0], stroke = [0, 0, 0], lw = 1, fillSpace = "DeviceGray", strokeSpace = "DeviceGray";
    const spaces = spacesIn || {};
    let path = null, pathStart = -1, pendingClip = false, clipBox = null;
    let inText = false, textStart = -1, tm = null, tlm = null, fontSize = 1, textPts = [], textChars = 0;
    const startPath = (i) => { if (!path) { path = { sub: [], cur: null, start: null, pts: [] }; pathStart = ins[i].start; } };
    const addPt = (p) => path.pts.push(p);
    for (let i = 0; i < ins.length; i++) {
      const { op, args } = ins[i];
      switch (op) {
        case "q": stack.push({ ctm: ctm.slice(), fill, stroke, lw, fillSpace, strokeSpace, clipBox }); break;
        case "Q": { const s = stack.pop(); if (s) { ctm = s.ctm; fill = s.fill; stroke = s.stroke; lw = s.lw; fillSpace = s.fillSpace; strokeSpace = s.strokeSpace; clipBox = s.clipBox; } break; }
        case "cm": if (args.length >= 6) ctm = mul(args.slice(0, 6).map(Number), ctm); break;
        case "w": lw = +args[0] || 0; break;
        case "g": fill = [+args[0], +args[0], +args[0]]; break;
        case "G": stroke = [+args[0], +args[0], +args[0]]; break;
        case "rg": fill = args.slice(0, 3).map(Number); break;
        case "RG": stroke = args.slice(0, 3).map(Number); break;
        case "k": fill = cmyk(...args.slice(0, 4).map(Number)); break;
        case "K": stroke = cmyk(...args.slice(0, 4).map(Number)); break;
        case "cs": fillSpace = args[0] && args[0].name || "DeviceGray"; break;
        case "CS": strokeSpace = args[0] && args[0].name || "DeviceGray"; break;
        case "sc": case "scn": fill = colorFrom(args, fillSpace, spaces); break;
        case "SC": case "SCN": stroke = colorFrom(args, strokeSpace, spaces); break;
        // ── path construction ──
        case "m": { startPath(i); const p = ap(ctm, +args[0], +args[1]); path.cur = { segs: [["m", p]], first: p, last: p, closed: false }; path.sub.push(path.cur); addPt(p); break; }
        case "l": { startPath(i); if (!path.cur) { const p0 = ap(ctm, +args[0], +args[1]); path.cur = { segs: [["m", p0]], first: p0, last: p0, closed: false }; path.sub.push(path.cur); } const p = ap(ctm, +args[0], +args[1]); path.cur.segs.push(["l", p]); path.cur.last = p; addPt(p); break; }
        case "c": case "v": case "y": {
          startPath(i); if (!path.cur) break;
          let c1, c2, p;
          if (op === "c") { c1 = ap(ctm, +args[0], +args[1]); c2 = ap(ctm, +args[2], +args[3]); p = ap(ctm, +args[4], +args[5]); }
          else if (op === "v") { c1 = path.cur.last; c2 = ap(ctm, +args[0], +args[1]); p = ap(ctm, +args[2], +args[3]); }
          else { c1 = ap(ctm, +args[0], +args[1]); p = ap(ctm, +args[2], +args[3]); c2 = p; }
          path.cur.segs.push(["c", c1, c2, p]); path.cur.last = p; addPt(c1); addPt(c2); addPt(p); break;
        }
        case "h": if (path && path.cur) { path.cur.closed = true; path.cur.segs.push(["h"]); path.cur.last = path.cur.first; } break;
        case "re": {
          startPath(i); const [x, y, w, h] = args.slice(0, 4).map(Number);
          const a = ap(ctm, x, y), b = ap(ctm, x + w, y), c = ap(ctm, x + w, y + h), d = ap(ctm, x, y + h);
          path.cur = { segs: [["m", a], ["l", b], ["l", c], ["l", d], ["h"]], first: a, last: a, closed: true, rect: true };
          path.sub.push(path.cur); addPt(a); addPt(b); addPt(c); addPt(d); break;
        }
        case "W": case "W*": pendingClip = true; break;
        // ── painting ──
        case "S": case "s": case "f": case "F": case "f*": case "B": case "B*": case "b": case "b*": case "n": {
          if (!path) { pendingClip = false; break; }
          const closes = op === "s" || op === "b" || op === "b*";
          if (closes && path.cur) { path.cur.closed = true; path.cur.segs.push(["h"]); }
          const doStroke = /S|s|B|b/.test(op), doFill = /f|F|B|b/.test(op);
          const bbox = bboxOf(path.pts);
          const allClosed = path.sub.length > 0 && path.sub.every(s => s.closed || (s.first && s.last && Math.hypot(s.first[0] - s.last[0], s.first[1] - s.last[1]) < 0.05));
          const seg = {
            kind: pendingClip && op === "n" ? "clip" : (op === "n" ? "noop" : "path"),
            start: pathStart, end: ins[i].end, paintOp: op,
            stroke: doStroke, fill: doFill, strokeRGB: stroke.slice(), fillRGB: fill.slice(), lwPt: lw * scaleOf(ctm),
            closed: allClosed, subpaths: path.sub.map(s => s.segs), bbox, depth
          };
          if (pendingClip) clipBox = bbox;
          if (seg.kind === "clip") { seg.clipBox = bbox; }
          (depth === 0 ? segs : inner).push(seg);
          path = null; pathStart = -1; pendingClip = false; break;
        }
        // ── text ──
        case "BT": inText = true; textStart = ins[i].start; tm = [1, 0, 0, 1, 0, 0]; tlm = tm; textPts = []; textChars = 0; break;
        case "Tf": fontSize = Math.abs(+args[1]) || 1; break;
        case "Tm": if (args.length >= 6) { tm = args.slice(0, 6).map(Number); tlm = tm; } break;
        case "Td": case "TD": tlm = mul([1, 0, 0, 1, +args[0], +args[1]], tlm); tm = tlm; break;
        case "T*": tlm = mul([1, 0, 0, 1, 0, -fontSize * 1.2], tlm); tm = tlm; break;
        case "Tj": case "TJ": case "'": case "\"": {
          const str = op === "TJ" ? (args[0] && args[0].arr || []).filter(a => a && a.str != null).map(a => a.str).join("") : (args[args.length - 1] && args[args.length - 1].str) || "";
          const m = mul(tm, ctm); const sz = fontSize * scaleOf(m) / scaleOf(ctm) * scaleOf(ctm);
          const p0 = ap(m, 0, 0), p1 = ap(m, Math.max(1, str.length) * fontSize * 0.55, fontSize);
          textPts.push(p0, p1); textChars += str.length;
          tm = mul([1, 0, 0, 1, str.length * fontSize * 0.55, 0], tm); void sz; break;
        }
        case "ET": {
          if (inText) {
            const bbox = bboxOf(textPts);
            (depth === 0 ? segs : inner).push({ kind: "text", start: textStart, end: ins[i].end, bbox, chars: textChars, fillRGB: fill.slice(), depth });
          }
          inText = false; break;
        }
        case "BI": (depth === 0 ? segs : inner).push({ kind: "image", start: ins[i].start, end: ins[i].end, bbox: bboxOf([ap(ctm, 0, 0), ap(ctm, 1, 0), ap(ctm, 1, 1), ap(ctm, 0, 1)]), depth }); break;
        case "sh": (depth === 0 ? segs : inner).push({ kind: "shading", start: ins[i].start, end: ins[i].end, bbox: clipBox, depth }); break;
        case "Do": {
          const name = args[0] && args[0].name;
          const x = resolve && name ? resolve(name) : null;
          const seg = { kind: "xobj", start: ins[i].start, end: ins[i].end, name, bbox: null, depth, children: [] };
          if (x && x.subtype === "Image") {
            seg.bbox = bboxOf([ap(ctm, 0, 0), ap(ctm, 1, 0), ap(ctm, 1, 1), ap(ctm, 0, 1)]);
          } else if (x && x.subtype === "Form" && x.bytes && depth < 6) {
            const m = mul(x.matrix || [1, 0, 0, 1, 0, 0], ctm);
            if (x.bbox) seg.bbox = bboxOf([ap(m, x.bbox[0], x.bbox[1]), ap(m, x.bbox[2], x.bbox[1]), ap(m, x.bbox[2], x.bbox[3]), ap(m, x.bbox[0], x.bbox[3])]);
            // a form's own colour spaces (a Separation "All" cut line, say) must not fall back to the page's
            const sub = interpret(x.bytes, x.resolve || resolve, m, depth + 1, x.spaces || spaces);
            // flatten grandchildren too: a form that only invokes another form still carries that form's paths
            const flat = (list) => list.flatMap(k => k.kind === "xobj" && k.children && k.children.length ? [k].concat(flat(k.children)) : [k]);
            const kids = flat(sub.segs.concat(sub.inner));
            let bb = null; for (const k of kids) { k.parentTop = true; bb = bbUnion(bb, k.bbox); }
            seg.children = kids; if (bb) seg.bbox = bb;
          }
          (depth === 0 ? segs : inner).push(seg);
          break;
        }
        default: break;
      }
    }
    return { segs, inner };
  }

  /* ═══ 3 · pdf-lib plumbing ═════════════════════════════════════════════ */
  const L = () => root.PDFLib;

  function decodeStream(stream) {
    const { decodePDFRawStream, PDFRawStream } = L();
    if (stream instanceof PDFRawStream) return decodePDFRawStream(stream).decode();
    if (stream && typeof stream.getContents === "function") return stream.getContents();
    return new Uint8Array(0);
  }
  /** Concatenated, decoded page content — identical for parse and write. */
  function pageContentBytes(doc, page) {
    const { PDFArray, PDFName } = L();
    const node = page.node;
    const raw = node.get(PDFName.of("Contents"));
    const obj = raw ? doc.context.lookup(raw) : null;
    const parts = [];
    if (obj instanceof PDFArray) for (const r of obj.asArray()) { const s = doc.context.lookup(r); if (s) parts.push(decodeStream(s)); }
    else if (obj) parts.push(decodeStream(obj));
    const total = parts.reduce((n, p) => n + p.length + 1, 0), out = new Uint8Array(total);
    let o = 0; for (const p of parts) { out.set(p, o); o += p.length; out[o++] = 0x0A; }
    return out;
  }
  function numsOf(arr) { return arr && arr.asArray ? arr.asArray().map(n => (n && n.asNumber) ? n.asNumber() : +n) : null; }
  /** Resource resolver for `Do` and colour spaces. */
  function makeResolver(doc, resources) {
    const { PDFName, PDFDict } = L();
    const cache = new Map();
    const lookupDict = (d, key) => { if (!(d instanceof PDFDict)) return null; const v = d.get(PDFName.of(key)); return v ? doc.context.lookup(v) : null; };
    const spaces = {};
    const csDict = lookupDict(resources, "ColorSpace");
    if (csDict instanceof PDFDict) for (const [k, v] of csDict.entries()) {
      const cs = doc.context.lookup(v); const arr = cs && cs.asArray ? cs.asArray() : null;
      if (arr && arr[0] && arr[0].encodedName) spaces[k.decodeText()] = arr[0].encodedName.slice(1);
    }
    const resolve = (name) => {
      if (cache.has(name)) return cache.get(name);
      let out = null;
      try {
        const xd = lookupDict(resources, "XObject");
        const x = xd ? lookupDict(xd, name) : null;
        if (x && x.dict) {
          const sub = x.dict.get(PDFName.of("Subtype")); const subtype = sub && sub.encodedName ? sub.encodedName.slice(1) : "";
          out = { subtype };
          if (subtype === "Form") {
            out.bytes = decodeStream(x);
            out.bbox = numsOf(doc.context.lookup(x.dict.get(PDFName.of("BBox"))));
            out.matrix = numsOf(doc.context.lookup(x.dict.get(PDFName.of("Matrix"))));
            const r = doc.context.lookup(x.dict.get(PDFName.of("Resources")));
            if (r) { const sub = makeResolver(doc, r); out.resolve = sub.resolve; out.spaces = sub.spaces; }
          }
        }
      } catch (_) { out = null; }
      cache.set(name, out); return out;
    };
    return { resolve, spaces };
  }

  function isPdfBytes(bytes) {
    const head = new TextDecoder("latin1").decode(bytes.subarray(0, 1400));
    return head.indexOf("%PDF-") >= 0;
  }

  /* ═══ 4 · parseSource ══════════════════════════════════════════════════ */
  async function parseSource(bytes, name) {
    const { PDFDocument, PDFName } = L();
    if (!isPdfBytes(bytes)) {
      const head = new TextDecoder("latin1").decode(bytes.subarray(0, 64));
      const legacy = /^%!PS-Adobe/.test(head);
      throw new Error(legacy
        ? `${name}: this .ai was saved without PDF compatibility (PostScript-only). Re-save in Illustrator with "Create PDF Compatible File" on.`
        : `${name}: not a PDF-compatible .ai or .pdf file.`);
    }
    let doc;
    try { doc = await PDFDocument.load(bytes, { ignoreEncryption: true, updateMetadata: false }); }
    catch (e) { throw new Error(`${name}: could not read the PDF structure (${e.message || e}).`); }
    if (!doc.getPageCount()) throw new Error(`${name}: no pages.`);
    const page = doc.getPage(0);
    const mb = page.getMediaBox();
    const ab = (() => { try { return page.getArtBox(); } catch (_) { return null; } })();
    const content = pageContentBytes(doc, page);
    const resNode = page.node.Resources();
    const { resolve, spaces } = makeResolver(doc, resNode);
    const { segs, inner } = interpret(content, resolve, [1, 0, 0, 1, 0, 0], 0, spaces);
    segs.forEach((s, i) => { s.index = i; });
    // Flatten form children for detection/grouping with a pointer to their top-level segment
    const nested = [];
    segs.forEach(s => { if (s.kind === "xobj" && s.children) s.children.forEach(k => { k.parent = s.index; nested.push(k); }); });
    return {
      name, bytes, doc, page,
      pageW: mb.width, pageH: mb.height, mediaBox: [mb.x, mb.y, mb.x + mb.width, mb.y + mb.height],
      artBox: ab ? [ab.x, ab.y, ab.x + ab.width, ab.y + ab.height] : null,
      rotate: (() => { try { return page.getRotation().angle || 0; } catch (_) { return 0; } })(),
      content, segments: segs, nested, inner,
      counts: { paths: segs.filter(s => s.kind === "path").length + nested.filter(s => s.kind === "path").length, top: segs.length, xobjects: segs.filter(s => s.kind === "xobj").length, text: segs.filter(s => s.kind === "text").length }
    };
  }

  /* ═══ 5 · grouping ═════════════════════════════════════════════════════ */
  /* Geometry helpers: flatten a segment's subpaths to polylines, test points
     against them (even-odd), and measure point-to-outline distance. Bounding
     boxes are only a last resort — a small charm parked in the empty corner
     of a round charm's box is NOT inside it, and a jump ring drawn beside a
     body IS part of it. */
  function flatten(seg, steps) {
    steps = steps || 8; const polys = [];
    for (const sub of seg.subpaths || []) {
      let poly = [], cur = null;
      for (const sg of sub) {
        if (sg[0] === "m") { if (poly.length > 1) polys.push(poly); poly = [sg[1]]; cur = sg[1]; }
        else if (sg[0] === "l") { poly.push(sg[1]); cur = sg[1]; }
        else if (sg[0] === "c" && cur) { const [a, b, c] = [sg[1], sg[2], sg[3]]; for (let i = 1; i <= steps; i++) { const t = i / steps, u = 1 - t; poly.push([u * u * u * cur[0] + 3 * u * u * t * a[0] + 3 * u * t * t * b[0] + t * t * t * c[0], u * u * u * cur[1] + 3 * u * u * t * a[1] + 3 * u * t * t * b[1] + t * t * t * c[1]]); } cur = c; }
      }
      if (poly.length > 1) polys.push(poly);
    }
    return polys;
  }
  function pointInPolys(x, y, polys) { // even-odd across all closed subpaths
    let inside = false;
    for (const poly of polys) for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) {
      const xi = poly[i][0], yi = poly[i][1], xj = poly[j][0], yj = poly[j][1];
      if ((yi > y) !== (yj > y) && x < (xj - xi) * (y - yi) / (yj - yi) + xi) inside = !inside;
    }
    return inside;
  }
  function distToPolys(x, y, polys) {
    let best = Infinity;
    for (const poly of polys) for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) {
      const ax = poly[j][0], ay = poly[j][1], bx = poly[i][0], by = poly[i][1];
      const dx = bx - ax, dy = by - ay, L = dx * dx + dy * dy;
      const t = L ? Math.max(0, Math.min(1, ((x - ax) * dx + (y - ay) * dy) / L)) : 0;
      const px = ax + t * dx - x, py = ay + t * dy - y; const d = px * px + py * py; if (d < best) best = d;
    }
    return Math.sqrt(best);
  }
  /** Sample points that represent a segment: its own polyline points, or box corners+centre. */
  function samples(seg, polysCache) {
    if (seg.kind === "path") { const polys = polysCache.get(seg) || flatten(seg, 4); polysCache.set(seg, polys); const pts = polys.flat(); if (pts.length > 60) { const step = Math.ceil(pts.length / 60); return pts.filter((_, i) => i % step === 0); } return pts; }
    const b = seg.bbox; return [[(b[0] + b[2]) / 2, (b[1] + b[3]) / 2], [b[0], b[1]], [b[2], b[1]], [b[2], b[3]], [b[0], b[3]]];
  }
  const insideFrac = (pts, polys) => pts.length ? pts.filter(p => pointInPolys(p[0], p[1], polys)).length / pts.length : 0;
  const minDist = (pts, polys) => { let d = Infinity; for (const p of pts) { const v = distToPolys(p[0], p[1], polys); if (v < d) d = v; } return d; };

  /**
   * opts: { minPt (default 6), darkMax (0.35 luminance), framePct (0.8), touchPt (2.5) }
   * A charm = one outline segment + every other segment assigned to it.
   */
  function groupCharms(parsed, opts) {
    opts = Object.assign({ minPt: 6, darkMax: 0.35, framePct: 0.8, touchPt: 2.5, nearPt: 6 }, opts || {});
    const pageArea = parsed.pageW * parsed.pageH;
    const all = parsed.segments.concat(parsed.nested);
    // Frames are never charm material: page-sized paths of any colour, and any closed
    // rectangle that encloses two or more outline candidates (a sheet box drawn into a
    // larger artboard). The design document's "never draw the sheet outline into a
    // silhouette probe" trap — the first live sheet hit it.
    const preDrawable = all.filter(s => s.bbox && s.kind !== "clip" && s.kind !== "noop" && !(s.kind === "xobj" && s.children && s.children.length));
    const achromatic0 = c => c && (Math.max(c[0], c[1], c[2]) - Math.min(c[0], c[1], c[2])) <= 0.15;
    const cand0 = preDrawable.filter(s => s.kind === "path" && s.stroke && s.closed && achromatic0(s.strokeRGB) && (s.bbox[2] - s.bbox[0]) >= opts.minPt && (s.bbox[3] - s.bbox[1]) >= opts.minPt);
    const isRectLike = s => s.kind === "path" && s.closed && s.subpaths.length === 1 && s.subpaths[0].filter(x => x[0] !== "h").length <= 5 && !s.subpaths[0].some(x => x[0] === "c");
    const encloses = (box, s) => s.bbox[0] >= box[0] - 0.5 && s.bbox[1] >= box[1] - 0.5 && s.bbox[2] <= box[2] + 0.5 && s.bbox[3] <= box[3] + 0.5;
    const frames = preDrawable.filter(s => s.kind === "path" && (bbArea(s.bbox) >= pageArea * opts.framePct ||
      (isRectLike(s) && cand0.filter(c => c !== s && encloses(s.bbox, c)).length >= 2)));
    const drawable = preDrawable.filter(s => !frames.includes(s));
    // An outline is a closed, achromatic stroke (black, grey OR white — the reference
    // sheet strokes one charm in white). Coloured strokes are engraving detail.
    const achromatic = c => c && (Math.max(c[0], c[1], c[2]) - Math.min(c[0], c[1], c[2])) <= 0.15;
    const isOutline = s => s.kind === "path" && s.stroke && s.closed && achromatic(s.strokeRGB) &&
      (s.bbox[2] - s.bbox[0]) >= opts.minPt && (s.bbox[3] - s.bbox[1]) >= opts.minPt;
    let frame = frames.length ? frames.reduce((a, b) => bbArea(b.bbox) > bbArea(a.bbox) ? b : a) : null;
    let cands = drawable.filter(isOutline);
    let rule = "stroked-dark-closed";
    if (!cands.length) {
      rule = "filled-dark-closed";
      cands = drawable.filter(s => s.kind === "path" && s.fill && s.closed && lum(s.fillRGB) <= opts.darkMax && bbArea(s.bbox) < pageArea * opts.framePct && (s.bbox[2] - s.bbox[0]) >= opts.minPt && (s.bbox[3] - s.bbox[1]) >= opts.minPt);
    }
    const polysCache = new Map();
    const polysOf = s => { let p = polysCache.get(s); if (!p) { p = flatten(s, 8); polysCache.set(s, p); } return p; };
    // 1 · outlines vs details: largest first; a candidate geometrically inside an
    //     accepted outline is a detail (hole, engraving frame, inner ring); a small
    //     candidate touching an accepted outline's stroke is an attached ring.
    cands.sort((a, b) => bbArea(b.bbox) - bbArea(a.bbox));
    const outlines = [], merged = new Map();
    const largestArea = cands.length ? bbArea(cands[0].bbox) : 0;
    for (const s of cands) {
      const pts = samples(s, polysCache);
      let host = null;
      // innermost container wins: iterate smallest → largest among accepted outlines
      const byAreaAsc = outlines.slice().sort((a, b) => bbArea(a.bbox) - bbArea(b.bbox));
      for (const o of byAreaAsc) { if (!bbInter(s.bbox, o.bbox)) continue; if (insideFrac(pts, polysOf(o)) >= 0.6) { host = o; break; } }
      if (!host) {
        const small = bbArea(s.bbox) <= 0.12 * largestArea || Math.max(s.bbox[2] - s.bbox[0], s.bbox[3] - s.bbox[1]) <= 30;
        if (small) {
          let bestD = Infinity, bestO = null, secondD = Infinity;
          for (const o of outlines) { const g = Math.max(opts.touchPt, opts.nearPt); const grown = [s.bbox[0] - g, s.bbox[1] - g, s.bbox[2] + g, s.bbox[3] + g]; if (!bbInter(grown, o.bbox)) continue; const d = minDist(pts, polysOf(o)); if (d < bestD) { secondD = bestD; bestD = d; bestO = o; } else if (d < secondD) secondD = d; }
          const touch = opts.touchPt + (s.lwPt || 0) / 2 + ((bestO && bestO.lwPt) || 0) / 2;
          // touching wins outright; a ring that merely floats near two outlines (an already-nested sheet fed back
          // in) attaches only when it is clearly closer to one of them — never to whichever neighbour is a hair nearer
          if (bestO && (bestD <= touch || (bestD <= Math.max(opts.touchPt, opts.nearPt) + touch && (secondD === Infinity || secondD >= 2 * Math.max(bestD, 0.5))))) host = bestO;
        }
      }
      if (host) merged.set(s, host); else outlines.push(s);
    }
    // 2 · every other drawable segment → the outline whose polygon holds most of its
    //     points; ties → smaller outline; then contact distance; then box overlap;
    //     then nearest centre within 24 pt; else orphan.
    const charms = outlines.map((o, i) => ({ index: i, outline: o, members: [], bbox: o.bbox.slice(), extras: [] }));
    const byOutline = new Map(charms.map(c => [c.outline, c]));
    const orphans = [];
    for (const s of drawable) {
      if (s === frame) continue;
      if (byOutline.has(s)) { byOutline.get(s).members.push(s); continue; }
      if (merged.has(s)) { const c = byOutline.get(merged.get(s)); c.members.push(s); c.bbox = bbUnion(c.bbox, s.bbox); continue; }
      const pts = samples(s, polysCache);
      // something far bigger than any outline it could belong to is not a detail of it (a guide, a stray frame)
      const biggestNear = charms.filter(c => bbInter(s.bbox, c.outline.bbox)).reduce((m, c) => Math.max(m, bbArea(c.outline.bbox)), 0);
      if (bbArea(s.bbox) > 3 * biggestNear && biggestNear > 0) { orphans.push(s); continue; }
      const near = charms.filter(c => bbInter([s.bbox[0] - opts.touchPt, s.bbox[1] - opts.touchPt, s.bbox[2] + opts.touchPt, s.bbox[3] + opts.touchPt], c.outline.bbox));
      let best = null, bestF = 0;
      for (const c of near) { const f = insideFrac(pts, polysOf(c.outline)); if (f > bestF || (f === bestF && f > 0 && best && bbArea(c.outline.bbox) < bbArea(best.outline.bbox))) { best = c; bestF = f; } }
      if (!best || bestF < 0.5) {
        let bestD = Infinity, bestC = null;
        for (const c of near) { const d = minDist(pts, polysOf(c.outline)); if (d < bestD) { bestD = d; bestC = c; } }
        if (bestC && bestD <= opts.touchPt + (s.lwPt || 0) / 2) { best = bestC; bestF = 1; }
      }
      if (!best || bestF <= 0) {
        const a = bbArea(s.bbox); let bestR = 0;
        for (const c of near) { const it = bbInter(s.bbox, c.outline.bbox); const r = it ? (a > 1e-6 ? bbArea(it) / a : 1) : 0; if (r > bestR) { bestR = r; best = c; } }
        if (bestR <= 0) best = null;
      }
      if (best) { best.members.push(s); best.bbox = bbUnion(best.bbox, s.bbox); continue; }
      const cx = (s.bbox[0] + s.bbox[2]) / 2, cy = (s.bbox[1] + s.bbox[3]) / 2;
      let nearC = null, nd = 24;
      for (const c of charms) { const d = distToPolys(cx, cy, polysOf(c.outline)); if (d < nd) { nd = d; nearC = c; } }
      if (nearC) { nearC.members.push(s); nearC.bbox = bbUnion(nearC.bbox, s.bbox); nearC.extras.push(s); } else orphans.push(s);
    }
    // Top-level membership: a nested segment brings its whole Do; a Do goes to the charm holding most of its children
    for (const c of charms) {
      const tops = new Map();
      for (const m of c.members) { const t = m.parent != null ? m.parent : m.index; if (t != null) tops.set(t, (tops.get(t) || 0) + 1); }
      c.topIndices = [...tops.keys()];
    }
    const claim = new Map();
    for (const c of charms) for (const t of c.topIndices) { const seg = parsed.segments[t]; if (seg && seg.kind === "xobj") { const n = c.members.filter(m => m.parent === t).length; const cur = claim.get(t); if (!cur || n > cur.n) claim.set(t, { c, n }); } }
    for (const c of charms) c.topIndices = c.topIndices.filter(t => { const seg = parsed.segments[t]; return !(seg && seg.kind === "xobj") || claim.get(t).c === c; });
    charms.forEach(c => { c.strokePt = Math.max(0.5, c.outline.lwPt || 0.5); });
    parsed._frames = frames.filter(s => bbArea(s.bbox) < pageArea * opts.framePct);   // drawn plate frames, for detectWorkArea (page-sized ones are not plates)
    return { charms, frame, frames, orphans, rule, outlineCount: outlines.length, mergedCount: merged.size };
  }

  /* ═══ 5b · work area ═══════════════════════════════════════════════════
     The plate the charms were laid out on. Priority: a drawn frame (a rectangle enclosing ≥ 2 charms), then the
     artboard if it is plate-sized, then the tight extent of the largest cluster of charms (charms within `gapPt`
     of one another). Matched to the given plates with a tolerance, since a nested block never fills its plate exactly. */
  function detectWorkArea(parsed, charms, plates, opts) {
    opts = Object.assign({ gapPt: 20, tolMm: 6 }, opts || {});
    const MM = 25.4 / 72;
    const matchPlate = (wPt, hPt) => {
      let best = null;
      for (const p of plates || []) { const dw = Math.abs(wPt * MM - p.wMm), dh = Math.abs(hPt * MM - p.hMm); const fits = wPt * MM <= p.wMm + opts.tolMm && hPt * MM <= p.hMm + opts.tolMm; const score = dw + dh; if (fits && (!best || score < best.score)) best = { id: p.id, wMm: p.wMm, hMm: p.hMm, score }; }
      // a block must fill most of the plate it is matched to, or it is just a few charms and tells us nothing
      if (best && (wPt * MM) * (hPt * MM) < 0.5 * best.wMm * best.hMm) best = null;
      return best;
    };
    const frames = (parsed._frames || []);
    if (frames.length) { const f = frames[0].bbox; const w = f[2] - f[0], h = f[3] - f[1]; return { source: "frame", wPt: w, hPt: h, bbox: f, count: charms.length, match: matchPlate(w, h), outside: 0 }; }
    const ab = matchPlate(parsed.pageW, parsed.pageH);
    if (ab) return { source: "artboard", wPt: parsed.pageW, hPt: parsed.pageH, bbox: [0, 0, parsed.pageW, parsed.pageH], count: charms.length, match: ab, outside: 0 };
    // clusters by bbox proximity (single-link)
    const n = charms.length, parent = charms.map((_, i) => i);
    const find = i => parent[i] === i ? i : (parent[i] = find(parent[i]));
    const near = (a, b, g) => !(a[2] + g < b[0] || b[2] + g < a[0] || a[3] + g < b[1] || b[3] + g < a[1]);
    for (let i = 0; i < n; i++) for (let j = i + 1; j < n; j++) if (near(charms[i].bbox, charms[j].bbox, opts.gapPt)) parent[find(i)] = find(j);
    const groups = new Map(); charms.forEach((c, i) => { const r = find(i); if (!groups.has(r)) groups.set(r, []); groups.get(r).push(c); });
    let best = null;
    for (const list of groups.values()) { const bb = list.reduce((a, c) => a ? [Math.min(a[0], c.bbox[0]), Math.min(a[1], c.bbox[1]), Math.max(a[2], c.bbox[2]), Math.max(a[3], c.bbox[3])] : c.bbox.slice(), null); const area = (bb[2] - bb[0]) * (bb[3] - bb[1]); if (!best || area > best.area) best = { bb, list, area }; }
    if (!best) return { source: "none", wPt: parsed.pageW, hPt: parsed.pageH, bbox: [0, 0, parsed.pageW, parsed.pageH], count: 0, match: null, outside: 0 };
    const w = best.bb[2] - best.bb[0], h = best.bb[3] - best.bb[1];
    return { source: "cluster", wPt: w, hPt: h, bbox: best.bb, count: best.list.length, match: matchPlate(w, h), outside: n - best.list.length };
  }

  /* ═══ 6 · silhouettes + thumbnails ═════════════════════════════════════ */
  function pathToCanvas(ctx, seg, tx) {
    const P = (p) => tx(p[0], p[1]);
    for (const sub of seg.subpaths) for (const s of sub) {
      if (s[0] === "m") { const p = P(s[1]); ctx.moveTo(p[0], p[1]); }
      else if (s[0] === "l") { const p = P(s[1]); ctx.lineTo(p[0], p[1]); }
      else if (s[0] === "c") { const a = P(s[1]), b = P(s[2]), c = P(s[3]); ctx.bezierCurveTo(a[0], a[1], b[0], b[1], c[0], c[1]); }
      else if (s[0] === "h") ctx.closePath();
    }
  }
  function makeCanvas(w, h) {
    if (typeof OffscreenCanvas !== "undefined") return new OffscreenCanvas(w, h);
    const c = document.createElement("canvas"); c.width = w; c.height = h; return c;
  }
  /** Flood from the border over `open` cells (1 = passable). Returns reached mask. */
  function floodFromBorder(open, w, h) {
    const seen = new Uint8Array(w * h), stack = new Int32Array(w * h);
    let sp = 0;
    const push = (i) => { if (!seen[i] && open[i]) { seen[i] = 1; stack[sp++] = i; } };
    for (let x = 0; x < w; x++) { push(x); push((h - 1) * w + x); }
    for (let y = 0; y < h; y++) { push(y * w); push(y * w + w - 1); }
    while (sp) { const i = stack[--sp]; const x = i % w, y = (i - x) / w; if (x > 0) push(i - 1); if (x < w - 1) push(i + 1); if (y > 0) push(i - w); if (y < h - 1) push(i + w); }
    return seen;
  }
  function fnv(str) { let h = 0x811c9dc5; for (let i = 0; i < str.length; i++) { h ^= str.charCodeAt(i); h = Math.imul(h, 0x01000193) >>> 0; } return h.toString(16).padStart(8, "0"); }

  /**
   * For each charm: rasterise ONLY the outline path, flood from the border,
   * everything unreached + the stroke itself is the silhouette. Adds
   * bits/w/h/scale/areaPt2/open/hash/thumb to each charm.
   */
  async function buildSilhouettes(parsed, charms, scale, onProgress) {
    scale = scale || 6;
    for (let ci = 0; ci < charms.length; ci++) {
      const c = charms[ci], o = c.outline;
      const pad = Math.max(c.strokePt / 2, ...c.members.map(m => (m.lwPt || 0) / 2)) + 1;
      // canvas covers the whole charm (outline + every member), so ink outside the outline still counts as material
      const bx0 = c.bbox[0] - pad, by0 = c.bbox[1] - pad, bx1 = c.bbox[2] + pad, by1 = c.bbox[3] + pad;
      const w = Math.max(2, Math.ceil((bx1 - bx0) * scale)), h = Math.max(2, Math.ceil((by1 - by0) * scale));
      const cv = makeCanvas(w, h), ctx = cv.getContext("2d", { willReadFrequently: true });
      ctx.clearRect(0, 0, w, h);
      const tx = (x, y) => [(x - bx0) * scale, (by1 - y) * scale];
      ctx.beginPath(); pathToCanvas(ctx, o, tx);
      ctx.lineWidth = Math.max(c.strokePt * scale, 1.5); ctx.strokeStyle = "#000"; ctx.lineJoin = "round"; ctx.lineCap = "round";
      ctx.stroke();
      if (o.fill) { ctx.fillStyle = "#000"; ctx.fill(o.paintOp.endsWith("*") ? "evenodd" : "nonzero"); }
      const img = ctx.getImageData(0, 0, w, h).data;
      const open = new Uint8Array(w * h); let inkN = 0;
      for (let i = 0, j = 3; i < w * h; i++, j += 4) { if (img[j] > 40) inkN++; else open[i] = 1; }
      const reached = floodFromBorder(open, w, h);
      const bits = new Uint8Array(w * h); let n = 0, interior = 0;
      for (let i = 0; i < w * h; i++) { if (!reached[i]) { bits[i] = 1; n++; if (open[i]) interior++; } }
      // Final silhouette: outline + every member (rings, details, text, engraving), flood-filled
      // together so that a jump ring's hole is solid material — nothing may nest inside a ring —
      // exactly what the render verifier will see when it floods the written layer.
      const others = c.members.filter(m => m !== o);
      if (others.length) {
        drawSegments(ctx, others, tx, scale, true);           // on top of the stroked/filled outline already drawn
        const img2 = ctx.getImageData(0, 0, w, h).data;
        const open2 = new Uint8Array(w * h);
        for (let i = 0, j = 3; i < w * h; i++, j += 4) if (img2[j] <= 40) open2[i] = 1;
        const reached2 = floodFromBorder(open2, w, h);
        n = 0; for (let i = 0; i < w * h; i++) { bits[i] = reached2[i] ? 0 : 1; n += bits[i]; }
      }
      c.bits = bits; c.w = w; c.h = h; c.scale = scale;
      c.bboxOuter = [bx0, by0, bx1, by1];
      c.areaPt2 = n / (scale * scale);
      c.open = interior < 0.03 * w * h && inkN > 0;       // fill leaked through a gap → outline is not closed at raster resolution
      c.hash = fnv(signature(bits, w, h) + "|" + Math.round((bx1 - bx0) * 2) + "x" + Math.round((by1 - by0) * 2) + "|" + c.members.length);
      c.thumb = await thumbnail(c, 168);
      c.widthPt = bx1 - bx0; c.heightPt = by1 - by0;
      c.centerPt = [(bx0 + bx1) / 2, (by0 + by1) / 2];   // rotation centre, PDF user space
      if (onProgress) onProgress(ci + 1, charms.length);
      await new Promise(r => setTimeout(r, 0));
    }
    return charms;
  }
  function signature(bits, w, h) {
    const G = 24, cnt = new Uint16Array(G * G), tot = new Uint16Array(G * G);
    for (let y = 0; y < h; y++) { const gy = Math.min(G - 1, (y * G / h) | 0); for (let x = 0; x < w; x++) { const g = gy * G + Math.min(G - 1, (x * G / w) | 0); tot[g]++; if (bits[y * w + x]) cnt[g]++; } }
    let s = ""; for (let i = 0; i < G * G; i++) s += cnt[i] * 2 >= tot[i] ? "1" : "0"; return s;
  }
  /** Thumbnail for the operator and for Claude: light background so white strokes show,
   *  and the charm's CUT OUTLINE drawn again in red on top so it is unmistakable. */
  async function thumbnail(c, size) {
    const b = c.bbox, pad = 2;
    const w = b[2] - b[0] + pad * 2, h = b[3] - b[1] + pad * 2, s = size / Math.max(w, h);
    const W = Math.max(8, Math.round(w * s)), H = Math.max(8, Math.round(h * s));
    const cv = makeCanvas(W, H), ctx = cv.getContext("2d");
    ctx.fillStyle = "#ece7dc"; ctx.fillRect(0, 0, W, H);
    const tx = (x, y) => [(x - b[0] + pad) * s, (b[3] + pad - y) * s];
    drawSegments(ctx, c.members, tx, s);
    ctx.beginPath(); pathToCanvas(ctx, c.outline, tx); ctx.strokeStyle = "rgba(190,40,40,.9)"; ctx.lineWidth = Math.max(1, 0.6 * s); ctx.stroke();
    return cv.convertToBlob ? await blobToDataUrl(await cv.convertToBlob({ type: "image/png" })) : cv.toDataURL("image/png");
  }
  /** Draw segments; `solid` paints everything opaque black (for silhouettes) instead of in colour. */
  function drawSegments(ctx, segs, tx, s, solid) {
    for (const seg of segs) {
      if (seg.kind === "path") {
        ctx.beginPath(); pathToCanvas(ctx, seg, tx);
        if (seg.fill) { ctx.fillStyle = solid ? "#000" : css(seg.fillRGB); ctx.fill(seg.paintOp.endsWith("*") ? "evenodd" : "nonzero"); }
        if (seg.stroke) { ctx.strokeStyle = solid ? "#000" : (isPaperWhite(seg.strokeRGB) ? "#2a2724" : css(seg.strokeRGB)); ctx.lineWidth = Math.max(solid ? 1.5 : 0.6, (seg.lwPt || 0.5) * s); ctx.stroke(); }
      } else if (seg.bbox) {
        const a = tx(seg.bbox[0], seg.bbox[3]), b2 = tx(seg.bbox[2], seg.bbox[1]);
        ctx.fillStyle = solid ? "#000" : seg.kind === "text" ? "rgba(80,80,80,.55)" : "rgba(120,120,160,.25)";
        ctx.fillRect(a[0], a[1], Math.max(1, b2[0] - a[0]), Math.max(1, b2[1] - a[1]));
      }
    }
  }
  // a white-stroked cut line is invisible on a white preview; it is still a cut line, so it is shown in ink
  const isPaperWhite = c => c && Math.min(c[0] || 0, c[1] || 0, c[2] || 0) >= 0.92;
  const css = c => `rgb(${Math.round((c[0] || 0) * 255)},${Math.round((c[1] || 0) * 255)},${Math.round((c[2] || 0) * 255)})`;
  const blobToDataUrl = blob => new Promise((res, rej) => { const r = new FileReader(); r.onload = () => res(r.result); r.onerror = rej; r.readAsDataURL(blob); });

  /* ═══ 7 · writer ═══════════════════════════════════════════════════════ */
  /** Blank every top-level segment not in `keep` (byte range → spaces). */
  function isolate(content, segments, keep) {
    const out = content.slice();
    const keepSet = new Set(keep);
    for (const s of segments) {
      if (keepSet.has(s.index) || s.kind === "clip" || s.kind === "noop") continue;
      if (s.start < 0 || s.end <= s.start) continue;
      out.fill(0x20, s.start, s.end);
    }
    return out;
  }

  function ocgOps(tag) {
    const { PDFOperator, PDFOperatorNames, PDFName } = L();
    return PDFOperator.of(PDFOperatorNames.BeginMarkedContentSequence, [PDFName.of("OC"), PDFName.of(tag)]);
  }

  /**
   * spec = {
   *   sheet: { wPt, hPt, name, strokeRGB },
   *   placements: [{ charm, angle, cxPt, cyPt }],      cx/cy = sheet pt, y-down
   *   sources: Map(sourceId → parsed),
   *   labelled: bool, title, meta: { … copied into the Info dict }
   * }
   * Returns Uint8Array. Charms are embedded as form XObjects whose content
   * is the ORIGINAL page content with every non-member segment blanked, so
   * the output carries exactly the original paths, nothing added.
   */
  async function buildSheet(spec) {
    const { PDFDocument, PDFName, PDFString, PDFDict, PDFRef, rgb, StandardFonts, pushGraphicsState, popGraphicsState, concatTransformationMatrix, drawObject, endMarkedContent, PDFArray } = L();
    const out = await PDFDocument.create();
    out.setTitle(spec.title || "Charm sheet"); out.setProducer("Brites Charm Nesting Station"); out.setCreator("Brites Charm Nesting Station");
    const page = out.addPage([spec.sheet.wPt, spec.sheet.hPt]);
    page.node.normalize();
    const res = page.node.Resources();
    const props = out.context.obj({}); res.set(PDFName.of("Properties"), props);
    const ocgRefs = [];
    const addOCG = (name, tag) => { const ocg = out.context.obj({ Type: "OCG", Name: PDFString.of(name) }); const ref = out.context.register(ocg); ocgRefs.push(ref); props.set(PDFName.of(tag), ref); return ref; };

    // sheet outline on its own layer
    addOCG("SHEET (do not cut)", "ocSheet");
    const sc = spec.sheet.strokeRGB || [1, 0, 0];
    page.pushOperators(ocgOps("ocSheet"));
    page.drawRectangle({ x: 0, y: 0, width: spec.sheet.wPt, height: spec.sheet.hPt, borderColor: rgb(sc[0], sc[1], sc[2]), borderWidth: spec.sheet.strokePt || 0.5 });
    page.pushOperators(endMarkedContent());

    // one copied page per source, so each charm's xobject shares its source resources
    const copies = new Map();
    for (const [sid, parsed] of spec.sources) {
      const [copied] = await out.copyPages(parsed.doc, [0]);
      const content = pageContentBytes(out, copied);
      const rawRes = copied.node.get(PDFName.of("Resources"));
      let resRef;
      if (rawRes instanceof PDFRef) resRef = rawRes;
      else if (rawRes instanceof PDFDict) resRef = out.context.register(rawRes);
      else resRef = out.context.register(out.context.obj({}));
      copies.set(sid, { copied, content, resRef, parsed });
    }
    const font = spec.labelled ? await out.embedFont(StandardFonts.Helvetica) : null;
    const usedNames = new Set(["SHEET (do not cut)"]);
    spec.placements.forEach((pl, i) => {
      const c = pl.charm, src = copies.get(c.sourceId);
      if (!src) return;
      const tag = "ocCharm" + i;
      let name = (c.name || c.slug || `charm-${String(i + 1).padStart(2, "0")}`).replace(/[^\x20-\x7E]/g, "").slice(0, 60) || `charm-${i + 1}`;
      if (usedNames.has(name)) { let k = 2; while (usedNames.has(name + "-" + k)) k++; name = name + "-" + k; }
      usedNames.add(name);
      addOCG(name, tag);
      const bytes = isolate(src.content, src.parsed.segments, c.topIndices);
      const pad = c.strokePt / 2 + 1;
      const bb = [c.bbox[0] - pad, c.bbox[1] - pad, c.bbox[2] + pad, c.bbox[3] + pad];
      const xobj = out.context.flateStream(bytes, { Type: "XObject", Subtype: "Form", BBox: bb, Matrix: [1, 0, 0, 1, 0, 0], Resources: src.resRef });
      const xref = out.context.register(xobj);
      const key = page.node.newXObject("Charm" + i, xref);
      // T(centre on sheet, y-up) · R(−θ) · T(−source centre)
      const th = -pl.angle * Math.PI / 180, cs = Math.cos(th), sn = Math.sin(th);
      const ox = pl.cxPt, oy = spec.sheet.hPt - pl.cyPt, cx = c.centerPt[0], cy = c.centerPt[1];
      const e = ox - (cs * cx - sn * cy), f = oy - (sn * cx + cs * cy);
      page.pushOperators(ocgOps(tag), pushGraphicsState(), concatTransformationMatrix(cs, sn, -sn, cs, e, f), drawObject(key), popGraphicsState(), endMarkedContent());
      pl.layerName = name;
    });
    if (spec.labelled) {
      addOCG("LABELS (do not cut)", "ocLabels");
      page.pushOperators(ocgOps("ocLabels"));
      spec.placements.forEach((pl, i) => {
        const x = pl.cxPt, y = spec.sheet.hPt - pl.cyPt, t = String(i + 1);
        page.drawCircle({ x, y, size: 7, color: rgb(1, 1, 1), borderColor: rgb(0.66, 0.51, 0.25), borderWidth: 0.6, opacity: 0.92 });
        page.drawText(t, { x: x - font.widthOfTextAtSize(t, 7) / 2, y: y - 2.5, size: 7, font, color: rgb(0.1, 0.09, 0.08) });
      });
      const foot = `${spec.title || "Charm sheet"} · ${spec.placements.length} charms · ${new Date().toISOString().slice(0, 16).replace("T", " ")}`;
      page.drawText(foot, { x: 4, y: 3, size: 5.5, font, color: rgb(0.45, 0.42, 0.38) });
      page.pushOperators(endMarkedContent());
    }
    // optional content: every layer ON, listed in order → Illustrator shows one layer per charm
    const order = out.context.obj(ocgRefs), on = out.context.obj(ocgRefs);
    out.catalog.set(PDFName.of("OCProperties"), out.context.obj({ OCGs: out.context.obj(ocgRefs), D: out.context.obj({ Order: order, ON: on, BaseState: "ON" }) }));
    void PDFArray;
    if (spec.meta) { try { out.setSubject(JSON.stringify(spec.meta).slice(0, 4000)); } catch (_) { /* ignore */ } }
    return await out.save({ useObjectStreams: false });
  }

  /** One charm alone on its own artboard (for the permanent per-charm copy). */
  async function buildSingleCharm(charm, parsed) {
    const pad = charm.strokePt / 2 + 2;
    const w = charm.bbox[2] - charm.bbox[0] + pad * 2, h = charm.bbox[3] - charm.bbox[1] + pad * 2;
    const c = Object.assign({}, charm, { sourceId: "one" });
    return buildSheet({
      sheet: { wPt: w, hPt: h, strokeRGB: [1, 1, 1], strokePt: 0.01 },
      placements: [{ charm: c, angle: 0, cxPt: w / 2, cyPt: h / 2 }],
      sources: new Map([["one", parsed]]),
      title: charm.name || charm.slug || "charm"
    });
  }

  /* ═══ 8 · render-based verification (pdf.js, optional content) ═════════ */
  /**
   * Renders the written sheet once per charm layer with every other layer
   * hidden, at `res` px/pt, and checks: ink stays inside the sheet inset,
   * every charm layer has ink, and no two charm silhouettes overlap.
   * spec = { wPt, hPt, insetPt, res, onProgress(done,total) }
   */
  async function verifyRendered(bytes, spec) {
    const pdfjs = root.pdfjsLib;
    if (!pdfjs) throw new Error("pdf.js not loaded");
    const res = spec.res || 6;
    const doc = await pdfjs.getDocument({ data: bytes.slice(), isEvalSupported: false }).promise;
    const page = await doc.getPage(1);
    const occ = await doc.getOptionalContentConfig();
    const flat = (o) => (o || []).flatMap(x => Array.isArray(x) ? flat(x) : (x && x.order) ? flat(x.order) : [x]);
    const ids = flat(occ.getOrder()).filter(id => typeof id === "string");
    const layers = ids.map(id => ({ id, name: (occ.getGroup(id) || {}).name || id }));
    const charmLayers = layers.filter(l => !/^SHEET|^LABELS/.test(l.name));
    const vp = page.getViewport({ scale: res });
    const W = Math.ceil(vp.width), H = Math.ceil(vp.height);
    const cv = makeCanvas(W, H), ctx = cv.getContext("2d", { willReadFrequently: true });
    const idGrid = new Int16Array(W * H).fill(-1);
    const inset = Math.floor((spec.insetPt || 0) * res * 0.999);
    const erodePx = spec.erodePt > 0 ? Math.ceil(spec.erodePt * res) + 1 : 0;   // negative clearance: strokes may overlap this much (+1 px raster slack)
    let overlapPx = 0, outsidePx = 0; const pairs = new Set(), empty = [], detail = {};
    for (let li = 0; li < charmLayers.length; li++) {
      for (const l of layers) occ.setVisibility(l.id, l.id === charmLayers[li].id);
      ctx.fillStyle = "#fff"; ctx.fillRect(0, 0, W, H);
      await page.render({ canvasContext: ctx, viewport: vp, optionalContentConfigPromise: Promise.resolve(occ), background: "rgba(255,255,255,1)" }).promise;
      const img = ctx.getImageData(0, 0, W, H).data;
      const open = new Uint8Array(W * H); let ink = 0;
      for (let i = 0, j = 0; i < W * H; i++, j += 4) { if (img[j] + img[j + 1] + img[j + 2] < 720) ink++; else open[i] = 1; }
      if (!ink) { empty.push(charmLayers[li].name); continue; }
      const reached = floodFromBorder(open, W, H);
      let solid = reached;                                   // 1 = not material
      if (erodePx) {                                          // shrink the silhouette by erodePx before the overlap test
        solid = new Uint8Array(W * H);
        for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) {
          const i = y * W + x; if (reached[i]) { solid[i] = 1; continue; }
          let keep = 1;
          for (let dy = -erodePx; dy <= erodePx && keep; dy++) for (let dx = -erodePx; dx <= erodePx; dx++) { const xx = x + dx, yy = y + dy; if (xx < 0 || yy < 0 || xx >= W || yy >= H || reached[yy * W + xx]) { keep = 0; break; } }
          if (!keep) solid[i] = 1;
        }
      }
      for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) {
        const i = y * W + x; if (reached[i]) continue;
        if (solid[i]) continue;                               // eroded rim: allowed to overlap / enter the inset band
        if (x < inset || y < inset || x >= W - inset || y >= H - inset) { outsidePx++; }
        if (idGrid[i] >= 0) { overlapPx++; const key = charmLayers[idGrid[i]].name + " ↔ " + charmLayers[li].name; pairs.add(key); const d = detail[key] || (detail[key] = { px: 0, x0: 1e9, y0: 1e9, x1: -1, y1: -1 }); d.px++; if (x < d.x0) d.x0 = x; if (y < d.y0) d.y0 = y; if (x > d.x1) d.x1 = x; if (y > d.y1) d.y1 = y; }
        idGrid[i] = li;
      }
      if (spec.onProgress) spec.onProgress(li + 1, charmLayers.length);
      await new Promise(r => setTimeout(r, 0));
    }
    try { doc.destroy(); } catch (_) { /* ignore */ }
    const overlapDetail = Object.fromEntries(Object.entries(detail).map(([k, d]) => [k, { px: d.px, boxPt: [d.x0, d.y0, d.x1, d.y1].map(v => +(v / res).toFixed(1)) }]));
    return { ok: overlapPx === 0 && outsidePx === 0 && empty.length === 0, overlapPx, outsidePx, emptyLayers: empty, overlappingPairs: [...pairs], overlapDetail, layers: charmLayers.length, res, erodePx };
  }

  root.CharmNestPDF = { parseSource, groupCharms, detectWorkArea, buildSilhouettes, buildSheet, buildSingleCharm, verifyRendered, isPdfBytes, lex, interpret, isolate, thumbnail, drawSegments, pathToCanvas };
})(typeof window !== "undefined" ? window : self);
