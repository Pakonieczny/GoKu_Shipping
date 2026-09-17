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
    function readString() { // (…) with nesting; escapes decoded, so the result is the string's bytes as latin1 chars
      let depth = 0, s = "";
      const ESC = { n: 10, r: 13, t: 9, b: 8, f: 12, "(": 40, ")": 41, "\\": 92 };
      for (; i < n; i++) {
        const c = bytes[i];
        if (c === 0x5C) {
          i++; const e = bytes[i];
          if (e === 0x0A || e === 0x0D) { if (e === 0x0D && bytes[i + 1] === 0x0A) i++; continue; }   // line continuation
          if (e >= 0x30 && e <= 0x37) { let oct = ""; let k = 0; while (k < 3 && bytes[i] >= 0x30 && bytes[i] <= 0x37) { oct += String.fromCharCode(bytes[i]); i++; k++; } i--; s += String.fromCharCode(parseInt(oct, 8) & 255); continue; }
          const ch = String.fromCharCode(e || 0); s += String.fromCharCode(ESC[ch] != null ? ESC[ch] : (e || 0)); continue;
        }
        if (c === 0x28) { depth++; if (depth === 1) continue; }
        if (c === 0x29) { depth--; if (depth === 0) { i++; break; } }
        s += String.fromCharCode(c);
      }
      return s;
    }
    function readHex() { // <…> → the string's bytes as latin1 chars (same shape as a literal string)
      let h = ""; i++; while (i < n && bytes[i] !== 0x3E) { const c = bytes[i++]; if (!WS.has(c)) h += String.fromCharCode(c); } i++;
      if (h.length % 2) h += "0";
      let s = ""; for (let k = 0; k < h.length; k += 2) { const v = parseInt(h.slice(k, k + 2), 16); s += String.fromCharCode(Number.isFinite(v) ? v : 0); }
      return s;
    }
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
  function interpret(bytes, resolve, ctm0, depth, spacesIn, layersIn, fontsIn) {
    const ins = lex(bytes);
    const segs = [], inner = [];
    const fonts = fontsIn || {}; let fontName = null, textRaw = "", textStrs = [];
    const layerNames = layersIn || {}; const mc = [];   // marked-content stack: the innermost /OC layer name applies to every segment
    const curLayer = () => { for (let k = mc.length - 1; k >= 0; k--) if (mc[k]) return mc[k]; return null; };
    const push = (list, seg) => { seg.layer = curLayer(); list.push(seg); };
    let ctm = ctm0.slice(), stack = [];
    let fill = [0, 0, 0], stroke = [0, 0, 0], lw = 1, fillSpace = "DeviceGray", strokeSpace = "DeviceGray";
    const spaces = spacesIn || {};
    let path = null, pathStart = -1, pendingClip = false, clipBox = null;
    let inText = false, textStart = -1, tm = null, tlm = null, fontSize = 1, textPts = [], textChars = 0;
    let textPieces = [], piece = null;
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
        case "BDC": { const tag = args[0] && args[0].name, pr = args[1]; const key = pr && pr.name; mc.push(tag === "OC" && key ? (layerNames[key] || key) : (pr && pr.dict && pr.dict.Title ? String(pr.dict.Title.str || pr.dict.Title) : null)); break; }
        case "BMC": mc.push(null); break;
        case "EMC": mc.pop(); break;
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
          push(depth === 0 ? segs : inner, seg);
          path = null; pathStart = -1; pendingClip = false; break;
        }
        // ── text ──
        case "BT": inText = true; textStart = ins[i].start; tm = [1, 0, 0, 1, 0, 0]; tlm = tm; textPts = []; textChars = 0; textRaw = ""; textStrs = []; textPieces = []; piece = null; break;
        case "Tf": fontSize = Math.abs(+args[1]) || 1; fontName = args[0] && args[0].name || null; break;
        case "Tm": if (args.length >= 6) { tm = args.slice(0, 6).map(Number); tlm = tm; } piece = null; break;
        case "Td": case "TD": tlm = mul([1, 0, 0, 1, +args[0], +args[1]], tlm); tm = tlm; piece = null; break;
        case "T*": tlm = mul([1, 0, 0, 1, 0, -fontSize * 1.2], tlm); tm = tlm; piece = null; break;
        case "Tj": case "TJ": case "'": case "\"": {
          const str = op === "TJ" ? (args[0] && args[0].arr || []).filter(a => a && a.str != null).map(a => a.str).join("") : (args[args.length - 1] && args[args.length - 1].str) || "";
          const font = fontName && fonts[fontName];
          const dec = font ? font.decode(str) : null;                          // { text, ok } or null when the font is unknown
          const glyphs = font && !font.simple ? Math.ceil(str.length / 2) : str.length;
          const wEm = font && font.widthOf ? font.widthOf(str) : glyphs * 0.55;   // advance in em, from /Widths when the font has them
          const m = mul(tm, ctm);
          const p0 = ap(m, 0, 0), p1 = ap(m, Math.max(0.55, wEm) * fontSize, fontSize), pDesc = ap(m, 0, -0.22 * fontSize);
          textPts.push(p0, p1, pDesc); textChars += glyphs;
          textRaw += str; textStrs.push(dec ? dec : { text: null, ok: false });
          // every newly positioned string is its own piece: Illustrator writes a whole sheet of labels in one BT…ET block
          if (op === "'" || op === "\"") piece = null;
          if (!piece) { piece = { pts: [], chars: 0, raw: "", strs: [], font: fontName }; textPieces.push(piece); }
          piece.pts.push(p0, p1, pDesc); piece.chars += glyphs; piece.raw += str; piece.strs.push(dec ? dec : { text: null, ok: false });
          if (op === "'" || op === "\"") { tlm = mul([1, 0, 0, 1, 0, -fontSize * 1.2], tlm); tm = tlm; piece = null; }
          tm = mul([1, 0, 0, 1, wEm * fontSize, 0], tm); break;
        }
        case "ET": {
          if (inText) {
            const bbox = bboxOf(textPts);
            const decoded = textStrs.every(t => t && t.ok) ? textStrs.map(t => t.text).join("") : null;
            const pieces = textPieces.length > 1 ? textPieces.map(p => { const d = p.strs.every(t => t && t.ok) ? p.strs.map(t => t.text).join("") : null; return { bbox: bboxOf(p.pts), chars: p.chars, str: d, raw: p.raw, font: p.font, undecodable: d == null && p.chars > 0 }; }) : null;
            push(depth === 0 ? segs : inner, { kind: "text", start: textStart, end: ins[i].end, bbox, chars: textChars, fillRGB: fill.slice(), depth, str: decoded, raw: textRaw, font: fontName, undecodable: decoded == null && textChars > 0, pieces });
          }
          inText = false; break;
        }
        case "BI": push(depth === 0 ? segs : inner, { kind: "image", start: ins[i].start, end: ins[i].end, bbox: bboxOf([ap(ctm, 0, 0), ap(ctm, 1, 0), ap(ctm, 1, 1), ap(ctm, 0, 1)]), depth }); break;
        case "sh": push(depth === 0 ? segs : inner, { kind: "shading", start: ins[i].start, end: ins[i].end, bbox: clipBox, depth }); break;
        case "Do": {
          const name = args[0] && args[0].name;
          const x = resolve && name ? resolve(name) : null;
          const seg = { kind: "xobj", start: ins[i].start, end: ins[i].end, name, bbox: null, depth, children: [], layer: curLayer() };
          if (x && x.subtype === "Image") {
            seg.bbox = bboxOf([ap(ctm, 0, 0), ap(ctm, 1, 0), ap(ctm, 1, 1), ap(ctm, 0, 1)]);
          } else if (x && x.subtype === "Form" && x.bytes && depth < 6) {
            const m = mul(x.matrix || [1, 0, 0, 1, 0, 0], ctm);
            if (x.bbox) seg.bbox = bboxOf([ap(m, x.bbox[0], x.bbox[1]), ap(m, x.bbox[2], x.bbox[1]), ap(m, x.bbox[2], x.bbox[3]), ap(m, x.bbox[0], x.bbox[3])]);
            // a form's own colour spaces (a Separation "All" cut line, say) must not fall back to the page's
            const sub = interpret(x.bytes, x.resolve || resolve, m, depth + 1, x.spaces || spaces, x.layers || layerNames, x.fonts || fonts);
            for (const k of sub.segs.concat(sub.inner)) if (!k.layer) k.layer = seg.layer;
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
  /** Before a source page is copied into an output: Illustrator's private data (its native document, in 64 KB chunks),
      the thumbnail and annotations are not drawing and are never copied — a per-SKU file is the charm, not the master. */
  function stripSourceExtras(doc) {
    const { PDFName } = L();
    try { const node = doc.getPages()[0].node; for (const k of ["PieceInfo", "Thumb", "Annots", "Metadata", "AF"]) node.delete(PDFName.of(k)); } catch (_) { /* a page without them */ }
  }
  /** pdf-lib writes every object its context holds, reachable or not; after copyPages the copied page's original content
      stream and anything else no longer referenced would still be written. Delete what the catalog cannot reach. */
  function pruneUnreachable(doc) {
    const { PDFDict, PDFArray, PDFRef, PDFStream } = L();
    const ctx = doc.context, seen = new Set(), stack = [];
    const push = v => { if (v instanceof PDFRef) { const key = v.toString(); if (!seen.has(key)) { seen.add(key); const o = ctx.lookup(v); if (o) stack.push(o); } } else if (v instanceof PDFDict) { for (const [, val] of v.entries()) push(val); } else if (v instanceof PDFArray) { for (const val of v.asArray()) push(val); } else if (v instanceof PDFStream) push(v.dict); };
    const t = ctx.trailerInfo; for (const k of ["Root", "Info", "Encrypt", "ID"]) if (t[k]) push(t[k]);
    while (stack.length) push(stack.pop());
    let removed = 0; for (const [ref] of ctx.enumerateIndirectObjects()) if (!seen.has(ref.toString())) { ctx.delete(ref); removed++; }
    return removed;
  }

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
    // Illustrator layers: /OC /MC0 BDC … EMC, where Properties/MC0 is an OCG with a /Name
    const layers = {};
    const propDict = lookupDict(resources, "Properties");
    if (propDict instanceof PDFDict) for (const [k, v] of propDict.entries()) {
      try { const g = doc.context.lookup(v); const nm = g && g.get ? doc.context.lookup(g.get(PDFName.of("Name"))) : null; layers[k.decodeText()] = nm && nm.decodeText ? nm.decodeText() : k.decodeText(); } catch (_) { layers[k.decodeText()] = k.decodeText(); }
    }
    const csDict = lookupDict(resources, "ColorSpace");
    if (csDict instanceof PDFDict) for (const [k, v] of csDict.entries()) {
      const cs = doc.context.lookup(v); const arr = cs && cs.asArray ? cs.asArray() : null;
      if (arr && arr[0] && arr[0].encodedName) spaces[k.decodeText()] = arr[0].encodedName.slice(1);
    }
    // fonts: one decoder per resource name, so a text run's string can be read back (SKU labels under charms)
    const fonts = {};
    const fontDict = lookupDict(resources, "Font");
    if (fontDict instanceof PDFDict) for (const [k, v] of fontDict.entries()) {
      try { const fd = doc.context.lookup(v); if (fd instanceof PDFDict) fonts[k.decodeText()] = makeFontDecoder(doc, fd); } catch (_) { /* an unreadable font just leaves its text undecoded */ }
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
            if (r) { const sub = makeResolver(doc, r); out.resolve = sub.resolve; out.spaces = sub.spaces; out.layers = sub.layers; out.fonts = sub.fonts; }
          }
        }
      } catch (_) { out = null; }
      cache.set(name, out); return out;
    };
    return { resolve, spaces, layers, fonts };
  }

  /* ═══ 3b · font decoding — text runs back to strings ═══════════════════
     A simple font (Type1 / TrueType / Type3) is one byte per glyph, read through its /Encoding (a base encoding plus
     /Differences of glyph names) or its /ToUnicode CMap. A composite font (Type0, what Illustrator writes) is two bytes
     per glyph and is readable only through /ToUnicode; without one the run is kept raw and flagged `undecodable`, which
     is what sends the master's labels to the vision fallback. Nothing is guessed. */
  const GLYPH_NAMES = { space: " ", hyphen: "-", minus: "-", endash: "–", emdash: "—", period: ".", periodcentered: "·", bullet: "•", middot: "·", underscore: "_", slash: "/", backslash: "\\", colon: ":", semicolon: ";", comma: ",", parenleft: "(", parenright: ")", bracketleft: "[", bracketright: "]", braceleft: "{", braceright: "}", plus: "+", equal: "=", asterisk: "*", numbersign: "#", ampersand: "&", percent: "%", quotesingle: "'", quotedbl: "\"", question: "?", exclam: "!", at: "@", dollar: "$", less: "<", greater: ">", bar: "|", tilde: "~", asciicircum: "^", grave: "`", zero: "0", one: "1", two: "2", three: "3", four: "4", five: "5", six: "6", seven: "7", eight: "8", nine: "9" };
  const LIGATURES = { fi: "fi", fl: "fl", ff: "ff", ffi: "ffi", ffl: "ffl", f_f: "ff", f_i: "fi", f_l: "fl", f_f_i: "ffi", f_f_l: "ffl", longs: "s", quoteright: "\u2019", quoteleft: "\u2018", quotedblleft: "\u201c", quotedblright: "\u201d", ellipsis: "\u2026", degree: "\u00b0", multiply: "\u00d7", registered: "\u00ae", copyright: "\u00a9", trademark: "\u2122" };
  function glyphNameToChar(name) {
    if (!name) return null;
    if (name.length === 1) return name;
    if (GLYPH_NAMES[name] != null) return GLYPH_NAMES[name];
    let m = /^uni([0-9A-Fa-f]{4})/.exec(name); if (m) return String.fromCharCode(parseInt(m[1], 16));
    m = /^u([0-9A-Fa-f]{4,6})$/.exec(name); if (m) return String.fromCodePoint(parseInt(m[1], 16));
    if (LIGATURES[name]) return LIGATURES[name];
    if (/^[A-Za-z](_[A-Za-z])+$/.test(name)) return name.replace(/_/g, "");   // f_f_l and friends: the letters, joined
    return null;                                                  // gXX / cidXX / anything else: unknown on purpose
  }
  /** Parse a ToUnicode CMap (text) → { map: Map(code → string), bytes: 1|2 }. */
  function parseCMap(text) {
    const map = new Map(); let bytes = 0;
    const hexNum = h => parseInt(h, 16);
    const utf16 = h => { let s = ""; for (let i = 0; i + 4 <= h.length; i += 4) s += String.fromCharCode(parseInt(h.slice(i, i + 4), 16)); try { return decodeURIComponent(encodeURIComponent(s)); } catch (_) { return s; } };
    const cs = /begincodespacerange([\s\S]*?)endcodespacerange/g; let m;
    while ((m = cs.exec(text))) { const hs = m[1].match(/<([0-9A-Fa-f]+)>/g) || []; for (const h of hs) { const L = (h.length - 2) / 2; if (L === 1 || L === 2) { bytes = Math.max(bytes, L); } } }
    const bc = /beginbfchar([\s\S]*?)endbfchar/g;
    while ((m = bc.exec(text))) { const hs = m[1].match(/<([0-9A-Fa-f]*)>/g) || []; for (let i = 0; i + 1 < hs.length; i += 2) { const src = hs[i].slice(1, -1), dst = hs[i + 1].slice(1, -1); if (!bytes) bytes = src.length / 2; map.set(hexNum(src), utf16(dst)); } }
    const br = /beginbfrange([\s\S]*?)endbfrange/g;
    while ((m = br.exec(text))) {
      const body = m[1]; const re = /<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]+)>\s*(<([0-9A-Fa-f]*)>|\[([^\]]*)\])/g; let r;
      while ((r = re.exec(body))) {
        const lo = hexNum(r[1]), hi = hexNum(r[2]); if (!bytes) bytes = r[1].length / 2;
        if (r[4] != null) { const base = r[4]; const bl = base.length; for (let c = lo; c <= hi && c - lo < 65536; c++) { const last = parseInt(base.slice(bl - 4), 16) + (c - lo); map.set(c, utf16(base.slice(0, bl - 4) + last.toString(16).padStart(4, "0"))); } }
        else { const arr = (r[5].match(/<([0-9A-Fa-f]*)>/g) || []).map(h => utf16(h.slice(1, -1))); arr.forEach((s, i) => { if (lo + i <= hi) map.set(lo + i, s); }); }
      }
    }
    return { map, bytes: bytes || 1 };
  }
  function makeFontDecoder(doc, fd) {
    const { PDFName, PDFDict } = L();
    const get = (d, key) => { if (!(d instanceof PDFDict)) return null; const v = d.get(PDFName.of(key)); return v ? doc.context.lookup(v) : null; };
    const nameOf = v => (v && v.encodedName ? v.encodedName.slice(1) : null);
    const subtype = nameOf(get(fd, "Subtype")) || "";
    const simple = subtype !== "Type0";
    let toUni = null;
    try { const tu = get(fd, "ToUnicode"); if (tu && (tu.getContents || tu.contents)) toUni = parseCMap(new TextDecoder("latin1").decode(decodeStream(tu))); } catch (_) { toUni = null; }
    // one-byte encodings: latin1 base, /Differences on top
    const enc = new Array(256); for (let c = 0; c < 256; c++) enc[c] = String.fromCharCode(c);
    if (simple) {
      const e = get(fd, "Encoding");
      if (e instanceof PDFDict) {
        const diff = get(e, "Differences"); const arr = diff && diff.asArray ? diff.asArray() : null;
        if (arr) { let code = 0; for (const it of arr) { const v = doc.context.lookup(it); if (v && v.asNumber) code = v.asNumber(); else if (v && v.encodedName) { const ch = glyphNameToChar(v.encodedName.slice(1)); if (ch != null) enc[code] = ch; else if (code < 32 || code > 126) enc[code] = null; code++; } } }
      }
    }
    // widths, in em: simple fonts from /FirstChar + /Widths; composite from the descendant's /W and /DW
    let widthOf = null;
    try {
      if (simple) {
        const first = get(fd, "FirstChar"), W = get(fd, "Widths");
        const warr = W && W.asArray ? W.asArray().map(x => { const v = doc.context.lookup(x); return v && v.asNumber ? v.asNumber() : 0; }) : null;
        const f0 = first && first.asNumber ? first.asNumber() : 0;
        if (warr && warr.length) widthOf = s => { let w = 0; for (let i = 0; i < s.length; i++) { const c = s.charCodeAt(i) - f0; w += (c >= 0 && c < warr.length && warr[c] ? warr[c] : 500) / 1000; } return w; };
      } else {
        const desc = get(fd, "DescendantFonts"); const d0 = desc && desc.asArray ? doc.context.lookup(desc.asArray()[0]) : null;
        const dw = (get(d0, "DW") || {}).asNumber ? get(d0, "DW").asNumber() : 1000;
        const Wa = get(d0, "W"); const w = new Map();
        if (Wa && Wa.asArray) { const a = Wa.asArray().map(x => doc.context.lookup(x)); for (let i = 0; i < a.length;) { const c = a[i] && a[i].asNumber ? a[i].asNumber() : 0; const nx = a[i + 1]; if (nx && nx.asArray) { nx.asArray().forEach((x, j) => { const v = doc.context.lookup(x); w.set(c + j, v && v.asNumber ? v.asNumber() : dw); }); i += 2; } else { const c2 = nx && nx.asNumber ? nx.asNumber() : c; const v = a[i + 2] && a[i + 2].asNumber ? a[i + 2].asNumber() : dw; for (let k = c; k <= c2 && k - c < 65536; k++) w.set(k, v); i += 3; } } }
        widthOf = s => { let t = 0; for (let i = 0; i + 1 < s.length; i += 2) { const cid = (s.charCodeAt(i) << 8) | s.charCodeAt(i + 1); t += (w.has(cid) ? w.get(cid) : dw) / 1000; } return t; };
      }
    } catch (_) { widthOf = null; }
    function decode(str) {
      const out = []; let ok = true;
      if (simple) {
        for (let i = 0; i < str.length; i++) { const c = str.charCodeAt(i); const u = toUni && toUni.map.has(c) ? toUni.map.get(c) : enc[c]; if (u == null) { ok = false; out.push("�"); } else out.push(u); }
      } else {
        if (!toUni) return { text: null, ok: false };
        const B = toUni.bytes === 1 ? 1 : 2;
        for (let i = 0; i + B - 1 < str.length; i += B) { const c = B === 1 ? str.charCodeAt(i) : ((str.charCodeAt(i) << 8) | str.charCodeAt(i + 1)); if (toUni.map.has(c)) out.push(toUni.map.get(c)); else { ok = false; out.push("�"); } }
      }
      return { text: out.join(""), ok };
    }
    return { simple, subtype, decode, widthOf, hasToUnicode: !!toUni };
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
    const { resolve, spaces, layers, fonts } = makeResolver(doc, resNode);
    const { segs, inner } = interpret(content, resolve, [1, 0, 0, 1, 0, 0], 0, spaces, layers, fonts);
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
    opts = Object.assign({ minPt: 6, darkMax: 0.35, framePct: 0.8, touchPt: 2.5, nearPt: 6, ringMaxPt: 13 }, opts || {});
    // Illustrator writes a group's objects contiguously, so the content-stream order is the artist's grouping.
    // Every drawable gets a stream position; a form's children take their Do's position (kept in child order).
    const ordOf = new Map();
    for (const s of parsed.segments) if (s.bbox) ordOf.set(s, s.start);
    for (const s of parsed.segments) if (s.kind === "xobj" && s.children && s.children.length) { const n = s.children.length; s.children.forEach((k, i) => ordOf.set(k, s.start + (i + 1) / (n + 1))); }
    const ord = s => (ordOf.has(s) ? ordOf.get(s) : (s.start || 0));
    const pageArea = parsed.pageW * parsed.pageH;
    const all = parsed.segments.concat(parsed.nested);
    // Frames are never charm material: page-sized paths of any colour, and any closed
    // rectangle that encloses two or more outline candidates (a sheet box drawn into a
    // larger artboard). The design document's "never draw the sheet outline into a
    // silhouette probe" trap — the first live sheet hit it.
    const preDrawable = all.filter(s => s.bbox && s.kind !== "clip" && s.kind !== "noop" && !(s.kind === "xobj" && s.children && s.children.length));
    const achromatic0 = c => c && (Math.max(c[0], c[1], c[2]) - Math.min(c[0], c[1], c[2])) <= 0.15;
    // An outline drawn as several open strokes whose ends meet (a bar from a U-shape plus a line) is one closed outline.
    // Chain single-subpath achromatic open strokes by coincident endpoints; a fully closed chain becomes a synthetic
    // outline whose real parts stay the charm's members (the writer copies the parts; the synthetic path is geometry only).
    const chained = chainOpenStrokes(preDrawable.filter(s => s.kind === "path" && s.stroke && !s.closed && achromatic0(s.strokeRGB) && s.subpaths && s.subpaths.length === 1), 1.0);
    const partOf = new Map(); for (const ch of chained) for (const part of ch.parts) partOf.set(part, ch);
    const cand0 = preDrawable.concat(chained).filter(s => s.kind === "path" && s.stroke && s.closed && achromatic0(s.strokeRGB) && (s.bbox[2] - s.bbox[0]) >= opts.minPt && (s.bbox[3] - s.bbox[1]) >= opts.minPt);
    const isRectLike = s => s.kind === "path" && s.closed && s.subpaths.length === 1 && s.subpaths[0].filter(x => x[0] !== "h").length <= 5 && !s.subpaths[0].some(x => x[0] === "c");
    const encloses = (box, s) => s.bbox[0] >= box[0] - 0.5 && s.bbox[1] >= box[1] - 0.5 && s.bbox[2] <= box[2] + 0.5 && s.bbox[3] <= box[3] + 0.5;
    const frames = preDrawable.filter(s => s.kind === "path" && (bbArea(s.bbox) >= pageArea * opts.framePct ||
      (isRectLike(s) && cand0.filter(c => c !== s && encloses(s.bbox, c)).length >= 2)));
    const drawable = preDrawable.filter(s => !frames.includes(s));
    for (const ch of chained) ordOf.set(ch, Math.min(...ch.parts.map(ord)));
    const drawableWithChains = drawable.concat(chained.filter(ch => !frames.includes(ch)));
    // An outline is a closed, achromatic stroke (black, grey OR white — the reference
    // sheet strokes one charm in white). Coloured strokes are engraving detail.
    const achromatic = c => c && (Math.max(c[0], c[1], c[2]) - Math.min(c[0], c[1], c[2])) <= 0.15;
    // A solid dark shape with no stroke (an anchor, a star) is cut along its fill edge: it is an outline too.
    // Largest-first containment below turns a dark fill that sits inside a stroked outline back into a detail.
    const isOutline = s => s.kind === "path" && s.closed && (s.bbox[2] - s.bbox[0]) >= opts.minPt && (s.bbox[3] - s.bbox[1]) >= opts.minPt &&
      ((s.stroke && achromatic(s.strokeRGB)) || (!s.stroke && s.fill && lum(s.fillRGB) <= opts.darkMax));
    let frame = frames.length ? frames.reduce((a, b) => bbArea(b.bbox) > bbArea(a.bbox) ? b : a) : null;
    let cands = drawableWithChains.filter(isOutline);
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
    // stroked outlines first (largest first), then solid dark shapes: a fill only stands alone when no stroked
    // outline holds or overlaps it (a compass's black fill can spill past its own stroked outline's box)
    const isFillCand = s => !s.stroke && s.fill;
    cands.sort((a, b) => (isFillCand(a) - isFillCand(b)) || (bbArea(b.bbox) - bbArea(a.bbox)));
    const outlines = [], merged = new Map();
    const largestArea = cands.length ? bbArea(cands[0].bbox) : 0;
    for (const s of cands) {
      const pts = samples(s, polysCache);
      let host = null;
      // innermost container wins: iterate smallest → largest among accepted outlines
      const byAreaAsc = outlines.slice().sort((a, b) => bbArea(a.bbox) - bbArea(b.bbox));
      for (const o of byAreaAsc) { if (!bbInter(s.bbox, o.bbox)) continue; if (insideFrac(pts, polysOf(o)) >= 0.6) { host = o; break; } }
      if (!host && isFillCand(s)) {
        // a solid shape overlapping a stroked outline is that charm's fill, not a charm of its own
        let bestO = null, bestR = 0;
        for (const o of outlines) { if (isFillCand(o) || !bbInter(s.bbox, o.bbox)) continue; const f = insideFrac(pts, polysOf(o)); if (f > bestR) { bestR = f; bestO = o; } }   // by real containment only: on a dense sheet boxes overlap, shapes do not
        if (bestO && bestR >= 0.3) host = bestO;
      }
      if (!host) {
        // an attached ring: ring-sized, one simple subpath, and written next to its charm in the stream
        const maxDim = Math.max(s.bbox[2] - s.bbox[0], s.bbox[3] - s.bbox[1]);
        const ringLike = maxDim <= opts.ringMaxPt && s.subpaths.length === 1 && s.subpaths[0].length <= 20;
        const small = ringLike && (bbArea(s.bbox) <= 0.12 * largestArea || maxDim <= 30);
        if (small) {
          let bestD = Infinity, bestO = null, secondD = Infinity;
          for (const o of outlines) { const g = Math.max(opts.touchPt, opts.nearPt); const grown = [s.bbox[0] - g, s.bbox[1] - g, s.bbox[2] + g, s.bbox[3] + g]; if (!bbInter(grown, o.bbox)) continue; const d = minDist(pts, polysOf(o)); if (d < bestD) { secondD = bestD; bestD = d; bestO = o; } else if (d < secondD) secondD = d; }
          const touch = opts.touchPt + (s.lwPt || 0) / 2 + ((bestO && bestO.lwPt) || 0) / 2;
          // touching wins outright; a ring that merely floats near two outlines (an already-nested sheet fed back
          // in) attaches only when it is clearly closer to one of them — never to whichever neighbour is a hair nearer
          if (bestO && (bestD <= touch || (bestD <= Math.max(opts.touchPt, opts.nearPt) + touch && (secondD === Infinity || secondD >= 2 * Math.max(bestD, 0.5))))) {
            // on a nested sheet a ring can touch a neighbour's charm too: it belongs to the outline it was drawn with
            const big = cands.filter(c => Math.max(c.bbox[2] - c.bbox[0], c.bbox[3] - c.bbox[1]) > opts.ringMaxPt).sort((a, b) => ord(a) - ord(b));
            const o = ord(s); let prev = null, next = null; for (const c of big) { if (ord(c) < o) prev = c; else if (!next) next = c; }
            if (bestO === prev || bestO === next || (!prev && !next)) host = bestO;
          }
        }
      }
      if (host) merged.set(s, host); else outlines.push(s);
    }
    // 2 · every other drawable segment → the outline whose polygon holds most of its
    //     points; ties → smaller outline; then contact distance; then box overlap;
    //     then nearest centre within 24 pt; else orphan.
    const charms = outlines.map((o, i) => ({ index: i, outline: o, members: [], bbox: o.bbox.slice(), extras: [], layer: o.layer || null }));
    const byOutline = new Map(charms.map(c => [c.outline, c]));
    const orphans = [];
    // stream neighbours: the charms whose outlines were written just before and just after a segment
    const outlineByOrd = charms.slice().sort((a, b) => ord(a.outline) - ord(b.outline));
    const streamNeighbours = (s) => { const o = ord(s); let prev = null, next = null; for (const c of outlineByOrd) { if (ord(c.outline) < o) prev = c; else { next = c; break; } } return [prev, next].filter(Boolean); };
    const relation = (s, pts, c) => { const f = insideFrac(pts, polysOf(c.outline)); if (f >= 0.5) return { f, d: 0 }; const d = minDist(pts, polysOf(c.outline)); return { f, d }; };
    for (const s of drawable) {
      if (s === frame) continue;
      if (byOutline.has(s)) { byOutline.get(s).members.push(s); continue; }
      if (partOf.has(s)) { const ch = partOf.get(s); const c = byOutline.get(ch) || (merged.has(ch) ? byOutline.get(merged.get(ch)) : null); if (c) { c.members.push(s); c.bbox = bbUnion(c.bbox, s.bbox); continue; } }
      if (merged.has(s)) { const c = byOutline.get(merged.get(s)); c.members.push(s); c.bbox = bbUnion(c.bbox, s.bbox); continue; }
      const pts = samples(s, polysCache);
      // 2a · the artist's grouping first: a detail belongs to the charm it was drawn with (the outline before or after
      //      it in the stream) whenever it sits inside or against that outline. Only when neither stream neighbour
      //      holds it does pure geometry decide — that is what stops a dense sheet's touching charms swapping details.
      {
        const nb = streamNeighbours(s).filter(c => bbInter([s.bbox[0] - opts.nearPt, s.bbox[1] - opts.nearPt, s.bbox[2] + opts.nearPt, s.bbox[3] + opts.nearPt], c.outline.bbox));
        let pick = null, pr = null;
        for (const c of nb) { const r = relation(s, pts, c); if (!pick || r.f > pr.f || (r.f === pr.f && r.d < pr.d)) { pick = c; pr = r; } }
        // by containment, or by touch when the detail fits within that outline's box (a 30 mm bar can touch a 13 mm charm; it is not part of it)
        const fitsIn = (c) => { const b = c.outline.bbox, g = opts.nearPt; return s.bbox[0] >= b[0] - g && s.bbox[1] >= b[1] - g && s.bbox[2] <= b[2] + g && s.bbox[3] <= b[3] + g; };
        if (pick && (pr.f >= 0.5 || (pr.d <= opts.touchPt + (s.lwPt || 0) / 2 && fitsIn(pick)))) { pick.members.push(s); pick.bbox = bbUnion(pick.bbox, s.bbox); continue; }
      }
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

  /** Chain open strokes by coincident endpoints into closed synthetic outlines. */
  function chainOpenStrokes(segs, tolPt) {
    const ends = s => { const sub = s.subpaths[0]; const first = sub[0] && sub[0][0] === "m" ? sub[0][1] : null; let last = null; for (const o of sub) { if (o[0] === "m" || o[0] === "l") last = o[1]; else if (o[0] === "c") last = o[3]; } return first && last ? [first, last] : null; };
    const pts = []; segs.forEach((s, i) => { const e = ends(s); if (!e) return; if (Math.hypot(e[0][0] - e[1][0], e[0][1] - e[1][1]) <= tolPt) return; pts.push({ s, i, k: 0, p: e[0] }, { s, i, k: 1, p: e[1] }); });
    const mate = new Map();
    for (const a of pts) { if (mate.has(a)) continue; let best = null, bd = tolPt; for (const b of pts) { if (b.s === a.s || mate.has(b)) continue; const d = Math.hypot(a.p[0] - b.p[0], a.p[1] - b.p[1]); if (d <= bd) { bd = d; best = b; } } if (best) { mate.set(a, best); mate.set(best, a); } }
    const seen = new Set(), out = [];
    for (const start of pts) {
      if (seen.has(start.s) || !mate.has(start)) continue;
      // walk: leave `start.s` from its other end, cross to the mate, and so on until back at start
      const parts = [], poly = []; let cur = start.s, enterK = start.k, closed = false; const visited = new Set();
      while (cur && !visited.has(cur)) {
        visited.add(cur); parts.push(cur);
        const flat = flatten(cur, 16)[0] || []; const pl = enterK === 0 ? flat : flat.slice().reverse(); poly.push(...pl);
        const exit = pts.find(q => q.s === cur && q.k !== enterK); const m = exit && mate.get(exit);
        if (!m) break; if (m.s === start.s) { closed = true; break; } cur = m.s; enterK = m.k;
      }
      if (!closed || parts.length < 2) continue;
      parts.forEach(pp => seen.add(pp.s || pp));
      const bb = parts.reduce((a, pp) => bbUnion(a, pp.bbox), null);
      const sub = poly.map((pt, i) => [i ? "l" : "m", pt]); sub.push(["h"]);
      out.push({ kind: "path", synthetic: true, parts, stroke: true, fill: false, closed: true, strokeRGB: parts[0].strokeRGB, lwPt: Math.max(...parts.map(pp => pp.lwPt || 0)), paintOp: "S", subpaths: [sub], bbox: bb, start: Math.min(...parts.map(pp => pp.start)), end: Math.max(...parts.map(pp => pp.end)), depth: parts[0].depth, parent: parts[0].parent });
      parts.forEach(pp => seen.add(pp));
    }
    return out;
  }

  /* ═══ 5a · SKU labels under charms (master files) ═════════════════════
     Each charm in a master file has its SKU as a text object directly under it. The rule, precisely (design §6.1):
     a text segment whose decoded string matches the SKU pattern (after trim + upper-case; an optional size suffix
     after a middle dot or a space), whose bounding-box TOP edge is at most `gapPt` below the charm outline's bottom
     edge, and whose horizontal centre lies within the outline's horizontal extent widened by `widen` on each side.
     Two qualifying outlines → the nearer bottom edge wins. Labels are never part of the charm: they are dropped from
     every charm's members (and top-level indices) so a SKU string can never be cut or written into a per-SKU file. */
  // The shop's SKUs are free text ("T-Rex_84495", "Huggie Hoops- Umbrella", "Cheer 1 - Megaphone(CHEER)"): a label is any
  // one-line string of the characters SKUs use, 2–60 long, read in upper case. Anything stricter is a setting (skuPattern).
  const SKU_PATTERN_DEFAULT = /^[A-Z0-9][A-Z0-9 _.,'&()+\-]{1,60}$/;
  const SKU_PATTERN_LEGACY = "^[A-Z]{2,4}-[A-Z0-9]{2,6}(-[A-Z0-9]{1,4})?$";
  /** "BR-CMP-01 · S" → { sku, size } or null. A size rides after " · " (or "•"); with a strict pattern a plain space works too. */
  function parseSkuLabel(str, pattern) {
    pattern = pattern || SKU_PATTERN_DEFAULT;
    const s = String(str == null ? "" : str).replace(/�/g, "").replace(/\s+/g, " ").trim().toUpperCase();
    if (!s) return null;
    const sep = /^(.+?)\s*[·•]\s*([A-Z0-9]{1,3})$/.exec(s);
    if (sep && pattern.test(sep[1])) return { sku: sep[1], size: sep[2] };
    if (pattern.test(s)) return { sku: s, size: null };
    const m = /^(.+?)(?:\s*[·•]\s*|\s+)([A-Z0-9]{1,3})$/.exec(s);
    if (m && pattern.test(m[1])) return { sku: m[1], size: m[2] };
    return null;
  }
  function labelCharms(parsed, charms, opts) {
    opts = Object.assign({ pattern: SKU_PATTERN_DEFAULT, gapPt: 18, widen: 0.25 }, opts || {});
    const runs = parsed.segments.concat(parsed.nested).filter(s => s.kind === "text");
    // a block that positions several strings is read string by string, each with its own box; the block stays the segment
    const texts = []; for (const r of runs) { if (r.pieces && r.pieces.length > 1) for (const p of r.pieces) texts.push(Object.assign({}, p, { seg: r })); else texts.push(Object.assign({}, r, { seg: r })); }
    const labels = new Map(), unlabelled = [], orphans = [], duplicates = [], undecodable = [], labelSegs = new Set();
    const live = charms.filter(c => c.mergedInto == null);
    // The lines under a charm are its SKUs, one per line: the first line sits within gapPt of the outline's bottom edge,
    // and every further line hangs directly under the line before it (a stacked list). Only the charm directly above the
    // list owns it; a charm without a list is reported unlabelled, and one SKU under two charms is reported, not shared.
    const lines = []; for (const t of texts) {
      if (t.undecodable || t.str == null) { if (t.chars > 0) undecodable.push({ bbox: t.bbox, chars: t.chars, font: t.font || null }); continue; }
      const lab = parseSkuLabel(t.str, opts.pattern); if (!lab || !t.bbox) continue;
      lines.push({ t, lab, cx: (t.bbox[0] + t.bbox[2]) / 2, top: t.bbox[3], bottom: t.bbox[1], h: Math.max(1, t.bbox[3] - t.bbox[1]) });
    }
    lines.sort((a, b) => b.top - a.top);                                 // top of the page first, so a list is met first line first
    const attached = [];                                                 // { line, charm }
    const owner = new Map();                                             // "SKU" or "SKU__SIZE" → first charm index
    const under = (cx, c) => { const b = c.outline.bbox, w = b[2] - b[0]; return cx >= b[0] - w * opts.widen && cx <= b[2] + w * opts.widen; };
    for (const L of lines) {
      let best = null, bestGap = Infinity;
      for (const c of live) {
        const gap = c.outline.bbox[1] - L.top;                          // outline bottom (y-up) minus label top
        if (gap < -1 || gap > opts.gapPt) continue;
        if (!under(L.cx, c)) continue;
        if (gap < bestGap) { bestGap = gap; best = c; }
      }
      if (!best) {                                                       // no outline right above: a line of a list already attached?
        let chain = null, chainGap = Infinity;
        for (const a of attached) { const gap = a.line.bottom - L.top; if (gap < -0.5 * L.h || gap > 1.8 * L.h) continue; if (!under(L.cx, a.charm)) continue; if (Math.abs(a.line.cx - L.cx) > Math.max(a.line.t.bbox[2] - a.line.t.bbox[0], L.t.bbox[2] - L.t.bbox[0])) continue; if (gap < chainGap) { chainGap = gap; chain = a.charm; } }
        if (!chain) { orphans.push({ sku: L.lab.sku, size: L.lab.size, str: L.t.str, bbox: L.t.bbox }); continue; }
        best = chain; bestGap = chainGap;
      }
      labelSegs.add(L.t.seg); attached.push({ line: L, charm: best });
      const key = L.lab.size ? `${L.lab.sku}__${L.lab.size}` : L.lab.sku;
      if (!owner.has(key)) owner.set(key, []);
      const prev = labels.get(best.index);
      if (prev) { if (!prev.extra.some(x => x.sku === L.lab.sku && x.size === L.lab.size)) { const x = { sku: L.lab.sku, size: L.lab.size, str: L.t.str, bbox: L.t.bbox }; prev.extra.push(x); owner.get(key).push({ charm: best, ref: x, primary: false }); } continue; }
      const l = { sku: L.lab.sku, size: L.lab.size, seg: L.t.seg, gap: bestGap, str: L.t.str, bbox: L.t.bbox, extra: [] };
      labels.set(best.index, l); owner.get(key).push({ charm: best, ref: l, primary: true });
      best.label = L.t.seg; best.sku = L.lab.sku; best.skuSize = L.lab.size;
    }
    // One SKU under several charms is that design in several sizes (the sheet writes no size letters): the outlines are
    // ranked by size and lettered S/L, S/M/L, XS/S/M/L, XS/S/M/L/XL. Two of them the same size (within 3 %) are a real
    // duplicate: the first keeps the SKU, the rest are reported. A label that carries its own " · S" keeps it.
    const LADDER = { 2: ["S", "L"], 3: ["S", "M", "L"], 4: ["XS", "S", "M", "L"], 5: ["XS", "S", "M", "L", "XL"] };
    const dim = c => { const b = c.outline.bbox; return Math.max(b[2] - b[0], b[3] - b[1]); };
    const drop = (charm, ref) => { const l = labels.get(charm.index); if (!l) return; if (ref === l) { if (l.extra.length) { const nx = l.extra.shift(); l.sku = nx.sku; l.size = nx.size; l.str = nx.str; l.bbox = nx.bbox; charm.sku = nx.sku; charm.skuSize = nx.size; } else { labels.delete(charm.index); charm.sku = null; charm.skuSize = null; charm.label = null; } } else l.extra = l.extra.filter(x => x !== ref); };
    for (const [key, uses] of owner) {
      if (uses.length < 2) continue;
      const byCharm = new Map(); for (const u of uses) if (!byCharm.has(u.charm.index)) byCharm.set(u.charm.index, u);
      const list = [...byCharm.values()]; if (list.length < 2) continue;
      if (uses[0].ref.size) { for (const u of list.slice(1)) { duplicates.push({ sku: u.ref.sku, size: u.ref.size, also: `charm #${list[0].charm.index}`, charmIndex: u.charm.index, firstIndex: list[0].charm.index }); drop(u.charm, u.ref); } continue; }
      list.sort((a, b) => dim(a.charm) - dim(b.charm));
      const kept = [list[0]];
      for (const u of list.slice(1)) { const last = kept[kept.length - 1]; if (dim(u.charm) <= dim(last.charm) * 1.03) { duplicates.push({ sku: u.ref.sku, size: null, also: `charm #${last.charm.index} (same size)`, charmIndex: u.charm.index, firstIndex: last.charm.index }); drop(u.charm, u.ref); } else kept.push(u); }
      if (kept.length < 2) continue;
      const letters = LADDER[Math.min(kept.length, 5)];
      kept.forEach((u, i) => { const size = i < letters.length ? letters[i] : "L" + (i - letters.length + 2); u.ref.size = size; if (u.primary) u.charm.skuSize = size; u.ref.sizeSource = "rank"; });
      if (kept.length > 5) duplicates.push({ sku: kept[0].ref.sku, size: null, also: `${kept.length} sizes`, charmIndex: kept[5].charm.index, firstIndex: kept[0].charm.index });
    }
    for (const [index, l] of labels) { const c = live.find(x => x.index === index); if (c) c.extraSkus = l.extra; }
    // a label text is never charm material, whichever charm the grouping attached it to
    for (const c of charms) {
      const before = c.members.length;
      c.members = c.members.filter(m => !labelSegs.has(m));
      if (c.members.length !== before) { recomputeTopIndices(c, parsed); c.bbox = c.members.reduce((a, m) => bbUnion(a, m.bbox), null) || c.outline.bbox.slice(); }   // the label's box never widens the charm
      if (c.mergedInto == null && !labels.has(c.index)) unlabelled.push(c.index);
    }
    let skuCount = 0; for (const l of labels.values()) skuCount += 1 + l.extra.length;
    return { labels, unlabelled, orphans, duplicates, undecodable, skuCount };
  }
  /** Top-level indices a charm's writer keeps, from its current members (a Do stays while any of its children is a member). */
  function recomputeTopIndices(c, parsed) {
    const tops = new Set();
    for (const m of c.members) { const t = m.parent != null ? m.parent : m.index; if (t != null) tops.add(t); }
    c.topIndices = (c.topIndices || []).filter(t => tops.has(t)).concat([...tops].filter(t => !(c.topIndices || []).includes(t)));
    void parsed;
    return c.topIndices;
  }
  /** The sorter's cut-line rule: a closed path with an achromatic stroke. Blue and red strokes/fills are front-only detail. */
  const achromaticCol = col => col && (Math.max(col[0], col[1], col[2]) - Math.min(col[0], col[1], col[2])) <= 0.15;
  function isCutLine(m) { return !!m && m.kind === "path" && m.stroke && m.closed && achromaticCol(m.strokeRGB); }
  /** Inner cut lines of a charm: closed achromatic strokes other than its outline (hoop holes, windows). */
  function cutLinesOf(c) { return c.members.filter(m => m !== c.outline && isCutLine(m)); }
  /** A segment with every point and Bézier handle mapped through M (points and control points alike). */
  function transformSegment(seg, M) {
    const P = p => ap(M, p[0], p[1]);
    const subpaths = (seg.subpaths || []).map(sub => sub.map(s => s[0] === "m" || s[0] === "l" ? [s[0], P(s[1])] : s[0] === "c" ? ["c", P(s[1]), P(s[2]), P(s[3])] : s.slice()));
    const pts = []; for (const sub of subpaths) for (const s of sub) for (let i = 1; i < s.length; i++) pts.push(s[i]);
    return Object.assign({}, seg, { subpaths, bbox: bboxOf(pts), lwPt: (seg.lwPt || 0) * scaleOf(M), transformed: true, original: seg.original || seg });
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
      // an outline is open when the border flood leaks into it (little unreached empty interior). A solid shape has
      // no empty interior at all — it is closed by construction — so the test applies to stroked outlines only.
      c.open = !o.fill && interior < 0.03 * w * h && inkN > 0;
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
   *   placements: [{ charm, angle, cxPt, cyPt, scale? }], cx/cy = sheet pt, y-down; scale = uniform shrink about the charm centre (1 = as drawn)
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
      stripSourceExtras(parsed.doc);
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
      // T(centre on sheet, y-up) · S(scale) · R(−θ) · T(−source centre)
      const sc = pl.scale || 1, th = -pl.angle * Math.PI / 180, cs = Math.cos(th) * sc, sn = Math.sin(th) * sc;
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
    pruneUnreachable(out);
    return await out.save({ useObjectStreams: false });
  }

  /** One charm alone on its own artboard (for the permanent per-charm copy). */
  async function buildSingleCharm(charm, parsed) {
    // the page around a lone charm is padded by an eighth of its larger side (at least the stroke plus 2 pt): a charm that
    // filled 80 % of its own page would read as an artboard frame when the per-SKU file is parsed again
    const bw = charm.bbox[2] - charm.bbox[0], bh = charm.bbox[3] - charm.bbox[1];
    const pad = Math.max((charm.strokePt || 0.5) / 2 + 2, 0.125 * Math.max(bw, bh));
    const w = charm.bbox[2] - charm.bbox[0] + pad * 2, h = charm.bbox[3] - charm.bbox[1] + pad * 2;
    // the rotation centre normally comes from buildSilhouettes (canvas); without one (server indexing, tests) the bbox centre is the same point
    const c = Object.assign({}, charm, { sourceId: "one", centerPt: charm.centerPt || [(charm.bbox[0] + charm.bbox[2]) / 2, (charm.bbox[1] + charm.bbox[3]) / 2], strokePt: charm.strokePt || Math.max(0.5, charm.outline.lwPt || 0.5) });
    return buildSheet({
      sheet: { wPt: w, hPt: h, strokeRGB: [1, 1, 1], strokePt: 0.01 },
      placements: [{ charm: c, angle: 0, cxPt: w / 2, cyPt: h / 2 }],
      sources: new Map([["one", parsed]]),
      title: charm.name || charm.slug || "charm"
    });
  }

  /* ═══ 7b · back file: one engraved piece, mirrored, hoop-up ═══════════
     spec = {
       charm, parsed, cutMembers: [segments]      the outline + inner cut lines (the sorter's isCutLine rule)
       cx, cy                                      mirror axis x and rotation centre, source pt
       angleDeg                                    rotation applied AFTER the mirror, about (cx, cy), CCW y-up
       padPt                                       page margin around the piece (5 mm by default)
       glyphs: [{ cmds:[{type:"M"|"L"|"C"|"Q"|"Z", x, y, x1, y1, x2, y2}] }]   text outlines, back-frame pt, origin at (cx, cy), y-up
       view: "asSeenFromBack" | "frontCoordinates" title, meta
     }
     The CUT OUTLINE (reference) layer carries the ORIGINAL cut bytes under a mirror·rotate·translate matrix whenever
     the cut members are whole top-level segments of the source (nothing redrawn); when a cut line shares a form
     XObject with front detail, the exact transformed Béziers are written instead and `reference.redrawn` says so.
     The ENGRAVE layer is the text as filled paths — never a font reference. */
  async function buildBackFile(spec) {
    const { PDFDocument, PDFName, PDFString, PDFOperator, PDFOperatorNames, rgb, pushGraphicsState, popGraphicsState, concatTransformationMatrix, drawObject, endMarkedContent, moveTo, lineTo, appendBezierCurve, closePath, fill, setFillingRgbColor, setStrokingRgbColor, setLineWidth, stroke } = L();
    const c = spec.charm, parsed = spec.parsed, cut = spec.cutMembers, cx = spec.cx, cy = spec.cy, th = (spec.angleDeg || 0) * Math.PI / 180, pad = spec.padPt == null ? 5 / MMPT : spec.padPt;
    const M = [-1, 0, 0, 1, 2 * cx, 0];
    const R = [Math.cos(th), Math.sin(th), -Math.sin(th), Math.cos(th), cx - cx * Math.cos(th) + cy * Math.sin(th), cy - cx * Math.sin(th) - cy * Math.cos(th)];
    const MR = mul(M, R);                                          // mirror first, then rotate
    const back = cut.map(m => transformSegment(m, MR));
    const bb = back.reduce((a, s) => bbUnion(a, s.bbox), null) || [cx - 10, cy - 10, cx + 10, cy + 10];
    const lw = Math.max(...cut.map(m => m.lwPt || 0.5), 0.5);
    const pw = bb[2] - bb[0] + 2 * pad + lw, ph = bb[3] - bb[1] + 2 * pad + lw;
    const T = [1, 0, 0, 1, pad + lw / 2 - bb[0], pad + lw / 2 - bb[1]];
    const out = await PDFDocument.create();
    out.setTitle(spec.title || "Back"); out.setProducer("Brites Charm Nesting Station"); out.setCreator("Brites Charm Nesting Station");
    const page = out.addPage([pw, ph]); page.node.normalize();
    const res = page.node.Resources(); const props = out.context.obj({}); res.set(PDFName.of("Properties"), props);
    const ocgRefs = [];
    const addOCG = (name, tag) => { const ocg = out.context.obj({ Type: "OCG", Name: PDFString.of(name) }); const ref = out.context.register(ocg); ocgRefs.push(ref); props.set(PDFName.of(tag), ref); return ref; };
    const cm = m => concatTransformationMatrix(m[0], m[1], m[2], m[3], m[4], m[5]);
    const frontView = spec.view === "frontCoordinates";
    const Mp = [-1, 0, 0, 1, pw, 0];
    const cutSet = new Set(cut);
    const topOf = m => (m.parent != null ? m.parent : m.index);
    const tops = [...new Set(cut.map(topOf).filter(t => t != null))];
    const shared = c.members.some(m => !cutSet.has(m) && tops.includes(topOf(m)));
    const reference = { redrawn: shared || cut.some(m => m.synthetic), tops };
    // 1 · CUT OUTLINE (reference)
    addOCG("CUT OUTLINE (reference)", "ocCut");
    page.pushOperators(ocgOps("ocCut"), pushGraphicsState());
    if (frontView) page.pushOperators(cm(Mp));
    if (!reference.redrawn) {
      stripSourceExtras(parsed.doc);
      const [copied] = await out.copyPages(parsed.doc, [0]);
      const content = pageContentBytes(out, copied);
      const rawRes = copied.node.get(PDFName.of("Resources"));
      const { PDFRef, PDFDict } = L();
      const resRef = rawRes instanceof PDFRef ? rawRes : rawRes instanceof PDFDict ? out.context.register(rawRes) : out.context.register(out.context.obj({}));
      const bytes = isolate(content, parsed.segments, tops);
      const srcBB = cut.reduce((a, s) => bbUnion(a, s.bbox), null);
      const xobj = out.context.flateStream(bytes, { Type: "XObject", Subtype: "Form", BBox: [srcBB[0] - lw, srcBB[1] - lw, srcBB[2] + lw, srcBB[3] + lw], Matrix: [1, 0, 0, 1, 0, 0], Resources: resRef });
      const key = page.node.newXObject("Cut0", out.context.register(xobj));
      page.pushOperators(cm(T), cm(R), cm(M), drawObject(key));
    } else {
      page.pushOperators(cm(T), setStrokingRgbColor(0, 0, 0));
      for (const s of back) {
        page.pushOperators(setLineWidth(s.lwPt || lw));
        for (const sub of s.subpaths) for (const o of sub) {
          if (o[0] === "m") page.pushOperators(moveTo(o[1][0], o[1][1]));
          else if (o[0] === "l") page.pushOperators(lineTo(o[1][0], o[1][1]));
          else if (o[0] === "c") page.pushOperators(appendBezierCurve(o[1][0], o[1][1], o[2][0], o[2][1], o[3][0], o[3][1]));
          else if (o[0] === "h") page.pushOperators(closePath());
        }
        page.pushOperators(stroke());
      }
    }
    page.pushOperators(popGraphicsState(), endMarkedContent());
    // 2 · ENGRAVE: text as filled paths in the back frame (origin at the charm centre after mirror + rotation)
    addOCG("ENGRAVE", "ocEngrave");
    page.pushOperators(ocgOps("ocEngrave"), pushGraphicsState());
    if (frontView) page.pushOperators(cm(Mp));
    page.pushOperators(cm([1, 0, 0, 1, cx + T[4], cy + T[5]]), setFillingRgbColor(0, 0, 0));
    let glyphCount = 0;
    for (const g of spec.glyphs || []) {
      let cur = null, first = null;
      for (const k of g.cmds) {
        if (k.type === "M") { page.pushOperators(moveTo(k.x, k.y)); cur = [k.x, k.y]; first = cur; }
        else if (k.type === "L") { page.pushOperators(lineTo(k.x, k.y)); cur = [k.x, k.y]; }
        else if (k.type === "C") { page.pushOperators(appendBezierCurve(k.x1, k.y1, k.x2, k.y2, k.x, k.y)); cur = [k.x, k.y]; }
        else if (k.type === "Q") { const c1 = [cur[0] + 2 / 3 * (k.x1 - cur[0]), cur[1] + 2 / 3 * (k.y1 - cur[1])], c2 = [k.x + 2 / 3 * (k.x1 - k.x), k.y + 2 / 3 * (k.y1 - k.y)]; page.pushOperators(appendBezierCurve(c1[0], c1[1], c2[0], c2[1], k.x, k.y)); cur = [k.x, k.y]; }
        else if (k.type === "Z") { page.pushOperators(closePath()); cur = first; }
      }
      page.pushOperators(PDFOperator.of(PDFOperatorNames.FillNonZero)); glyphCount++;
    }
    page.pushOperators(popGraphicsState(), endMarkedContent());
    const order = out.context.obj(ocgRefs), on = out.context.obj(ocgRefs);
    out.catalog.set(PDFName.of("OCProperties"), out.context.obj({ OCGs: out.context.obj(ocgRefs), D: out.context.obj({ Order: order, ON: on, BaseState: "ON" }) }));
    if (spec.meta) { try { out.setSubject(JSON.stringify(Object.assign({ view: spec.view || "asSeenFromBack", reference }, spec.meta)).slice(0, 4000)); } catch (_) { /* ignore */ } }
    void fill; void rgb;
    pruneUnreachable(out);
    const bytes = await out.save({ useObjectStreams: false });
    return { bytes, wPt: pw, hPt: ph, reference, glyphCount, frame: { M, R, T, bbox: bb } };
  }
  const MMPT = 25.4 / 72;

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

  root.CharmNestPDF = { parseSource, groupCharms, detectWorkArea, buildSilhouettes, buildSheet, buildSingleCharm, buildBackFile, verifyRendered, isPdfBytes, lex, interpret, isolate, thumbnail, drawSegments, pathToCanvas,
    parseSkuLabel, labelCharms, recomputeTopIndices, isCutLine, cutLinesOf, transformSegment, flatten, parseCMap, glyphNameToChar, SKU_PATTERN_DEFAULT, SKU_PATTERN_LEGACY, mul, ap, signature, fnv };
})(typeof window !== "undefined" ? window : self);
