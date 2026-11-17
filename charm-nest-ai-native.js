/* charm-nest-ai-native.js — THE PRODUCTION .AI WITH TRUE TOP-LEVEL ILLUSTRATOR LAYERS (AINATIVE; Paul, 17 Nov 2026: "The illustrator download still does not have the layers organized exactly like in a DXF ...
 * they can't be under one directory ... they have to be organized just like the DXF").
 *
 * A PDF whose layers are optional-content groups opens in Illustrator with every layer as a SUBLAYER under one parent. An Illustrator-saved .ai is that same PDF plus Illustrator's own document
 * (the "private data": PieceInfo / Illustrator / Private → AIMetaData and AIPrivateData1..N), which is what makes the layers top-level. That document is plain text (\r line ends, the %AI5 layer
 * syntax: %AI5_BeginLayer … Lb / (Name) Ln … LB / %AI5_EndLayer--) held in ONE zstd frame behind the marker `%AI24_ZStandard_Data`, cut in 65536-byte streams. This file writes both parts:
 *   · the PDF part (pdf-lib): the layers as top-level OCGs as Illustrator writes them (Intent, Usage, /Order top layer first), every layer's drawing at page level for any other viewer;
 *   · the document text: the header and setup of the owner's own Illustrator-saved sample (charm-nest-ai-template.js, made by tools/make-ai-template.cjs) with the artboard, bounds, title and layer
 *     count put in, then one %AI5 layer per layer of the .dxf, bottom layer first, each path as the sample writes it (`N As` = segments, D = direction, Xa / XA = C M Y K R G B, f / s / S);
 *   · the text is stored in a zstd frame of RAW blocks (valid zstd, no compressor needed in the browser).
 * Input: what CharmNestExport.dxf(paths, layers, { collectOnly: true }) answers ({ entities, layers }): the very entities the .dxf is written from, layers in the .dxf's table order (bottom first).
 *   nativeAi(got, pageW, pageH, title) -> Promise<Uint8Array>      fromComposed(bytes, title) -> Promise<Uint8Array>  (a composed sheet's bytes, as productionAi takes them)
 * UMD: window / worker CharmNestAiNative; module.exports in node. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root) root.CharmNestAiNative = api;
})(typeof self !== 'undefined' ? self : typeof globalThis !== 'undefined' ? globalThis : this, function (root) {
  'use strict';
  const lib = () => (root && root.PDFLib) || (typeof require === 'function' ? require('./vendor/pdf-lib-1.17.1.min.js') : null);
  const template = () => (root && root.CharmNestAiTemplate) || (typeof require === 'function' ? require('./charm-nest-ai-template.js') : null);
  const MM = 25.4 / 72;
  const MARK = '%AI24_ZStandard_Data', CHUNK = 65536, ZBLOCK = 131072;
  /** Illustrator's own layer colours (index, then R G B), as the sample's layers carry them. */
  const LAYER_COLOURS = [[0, 79, 128, 255], [1, 255, 79, 79], [3, 79, 79, 255], [4, 255, 79, 255], [8, 0, 0, 0], [9, 255, 102, 0], [20, 102, 102, 0]];

  /* ── numbers ── */
  const num = v => { const r = +(+v).toFixed(6); if (!Number.isFinite(r)) throw new Error('The .ai contains a non-finite coordinate.'); return Object.is(r, -0) ? '0' : String(r); };
  /** C M Y K for an RGB 0-255 colour (Illustrator keeps the RGB beside it; black is its rich black as the sample writes it). */
  function cmyk(rgb) {
    const r = rgb[0] / 255, g = rgb[1] / 255, b = rgb[2] / 255;
    if (r + g + b === 0) return [0.749721527099609, 0.679194331169128, 0.670496702194214, 0.901457250118256];
    const k = 1 - Math.max(r, g, b); if (k >= 1) return [0, 0, 0, 1];
    return [(1 - r - k) / (1 - k), (1 - g - k) / (1 - k), (1 - b - k) / (1 - k), k];
  }
  const colourWords = rgb => cmyk(rgb).concat(rgb.map(v => v / 255)).map(num).join(' ');
  /** A layer name as Illustrator's XML id: a space is "_", any other character that is not a letter, digit, "-" or "_" is "_x<hex>_" ("The #" is "The__x23_"). */
  const xmlId = name => String(name).replace(/[^A-Za-z0-9_-]/g, c => (c === ' ' ? '_' : '_x' + c.charCodeAt(0).toString(16).toUpperCase() + '_'));
  const psString = s => String(s).replace(/[\\()]/g, c => '\\' + c).replace(/[^\x20-\x7e]/g, '?');

  /* ── paths ── */
  const anchors = b => { const a = []; for (let i = 0; i < b.pts.length; i += 3) a.push(b.pts[i]); return a; };
  const area = b => { const a = anchors(b); let s = 0; for (let i = 0; i < a.length; i++) { const p = a[i], q = a[(i + 1) % a.length]; s += p[0] * q[1] - q[0] * p[1]; } return s / 2; };
  function inside(pt, poly) { let c = false; for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) { const a = poly[i], b = poly[j]; if ((a[1] > pt[1]) !== (b[1] > pt[1]) && pt[0] < (b[0] - a[0]) * (pt[1] - a[1]) / (b[1] - a[1]) + a[0]) c = !c; } return c; }
  const reversed = b => ({ pts: b.pts.slice().reverse(), closed: b.closed });
  /** The path lines of a Bezier run [P0, c1, c2, P1, …]: `x y m`, then `x y L` for a straight piece and `x1 y1 x2 y2 x3 y3 C` for a curve. → [lines, segments] */
  function pathLines(b) {
    const p = b.pts, out = [num(p[0][0]) + ' ' + num(p[0][1]) + ' m']; let n = 0;
    for (let i = 1; i + 2 < p.length; i += 3) {
      const a = p[i - 1], c1 = p[i], c2 = p[i + 1], z = p[i + 2];
      if (c1[0] === a[0] && c1[1] === a[1] && c2[0] === z[0] && c2[1] === z[1]) out.push(num(z[0]) + ' ' + num(z[1]) + ' L');
      else out.push(num(c1[0]) + ' ' + num(c1[1]) + ' ' + num(c2[0]) + ' ' + num(c2[1]) + ' ' + num(z[0]) + ' ' + num(z[1]) + ' C');
      n++;
    }
    return [out, n];
  }
  /** One entity as the sample writes an object: `0 Ae`, `*u`, `N As`, the direction, the colour and the graphic state, the path, f / s / S, `*U`. */
  function objectLines(e) {
    const o = ['0 Ae', '*u'];
    if (e.kind === 'spline') {
      const [pl, n] = pathLines(e.b), w = Math.max(0.1, (e.lw || 0) / 100 / MM);
      o.push(n + ' As', (area(e.b) > 0 ? 1 : 0) + ' D', '0 R', colourWords(e.rgb) + ' XA', '0 1 0 0 0 Xy', '0 J 0 j ' + num(w) + ' w 10 M []0 d', '0 XR', ...pl, e.b.closed ? 's' : 'S');
    } else {
      // the rings of a fill: an outer ring runs counter-clockwise, a ring inside an odd number of others (a hole) clockwise, so the compound path fills as the .dxf's solid hatch does
      const polys = e.loops.map(l => anchors(l)), rings = e.loops.map((l, i) => {
        let depth = 0; for (let j = 0; j < polys.length; j++) if (j !== i && polys[j].length > 2 && inside(l.pts[0], polys[j])) depth++;
        const ccw = area(l) > 0, want = depth % 2 === 0; return (ccw === want) ? l : reversed(l);
      });
      rings.forEach((l, i) => {
        const [pl, n] = pathLines(l);
        o.push(n + ' As', (area(l) > 0 ? 1 : 0) + ' D');
        if (i === 0) o.push('0 O', colourWords(e.rgb) + ' Xa', '0 1 0 0 0 Xy', '0 J 0 j 0 w 10 M []0 d', '0 XR');
        o.push(...pl, 'f');
      });
    }
    o.push('*U');
    return o;
  }
  const boundsOf = got => {
    let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
    for (const e of got.entities) for (const b of (e.kind === 'spline' ? [e.b] : e.loops)) for (const p of b.pts) { if (p[0] < x0) x0 = p[0]; if (p[0] > x1) x1 = p[0]; if (p[1] < y0) y0 = p[1]; if (p[1] > y1) y1 = p[1]; }
    return Number.isFinite(x0) ? [x0, y0, x1, y1] : [0, 0, 1, 1];
  };

  /* ── the document text ── */
  const uuid = () => { const h = []; const c = root && root.crypto && root.crypto.getRandomValues ? root.crypto : null; for (let i = 0; i < 16; i++) h.push(c ? c.getRandomValues(new Uint8Array(1))[0] : Math.floor(Math.random() * 256)); h[6] = (h[6] & 15) | 64; h[8] = (h[8] & 63) | 128; const x = h.map(v => v.toString(16).padStart(2, '0')).join(''); return x.slice(0, 8) + '-' + x.slice(8, 12) + '-' + x.slice(12, 16) + '-' + x.slice(16, 20) + '-' + x.slice(20); };
  const pad2 = v => String(v).padStart(2, '0');
  function stamps(d) {
    const off = -d.getTimezoneOffset(), sign = off < 0 ? '-' : '+', oh = pad2(Math.floor(Math.abs(off) / 60)), om = pad2(Math.abs(off) % 60);
    const iso = d.getFullYear() + '-' + pad2(d.getMonth() + 1) + '-' + pad2(d.getDate()) + 'T' + pad2(d.getHours()) + ':' + pad2(d.getMinutes()) + ':' + pad2(d.getSeconds()) + sign + oh + ':' + om;
    const pdf = 'D:' + d.getFullYear() + pad2(d.getMonth() + 1) + pad2(d.getDate()) + pad2(d.getHours()) + pad2(d.getMinutes()) + pad2(d.getSeconds()) + sign + oh + "'" + om + "'";
    const h12 = d.getHours() % 12 || 12, ap = d.getHours() < 12 ? 'AM' : 'PM';
    const creation = d.getFullYear() + '-' + pad2(d.getMonth() + 1) + '-' + pad2(d.getDate()) + ' ' + h12 + ':' + pad2(d.getMinutes()) + ' ' + ap;
    return { iso, pdf, creation };
  }
  /** The sample's header and setup with this sheet's title, date, bounds, artboard and layer count put in. */
  function headText(got, pageW, pageH, title, when) {
    let t = template().head;
    const must = (re, rep) => { if (!re.test(t)) throw new Error('The Illustrator template does not hold ' + re); t = t.replace(re, rep); };
    const bb = boundsOf(got), art = [0, 0, pageW, pageH], n = got.layers.length;
    const cx = pageW / 2, cy = pageH / 2;
    must(/%%Title: \([^\r]*\)\r/, () => '%%Title: (' + psString(title || 'Charm production sheet') + '.ai)\r');
    must(/%%CreationDate: [^\r]*\r/, () => '%%CreationDate: ' + when.creation + '\r');
    must(/%%BoundingBox: [^\r]*\r/, () => '%%BoundingBox: ' + [Math.floor(bb[0]), Math.floor(bb[1]), Math.ceil(bb[2]), Math.ceil(bb[3])].join(' ') + '\r');
    must(/%%HiResBoundingBox: [^\r]*\r/, () => '%%HiResBoundingBox: ' + bb.map(num).join(' ') + '\r');
    must(/%AI3_Cropmarks: [^\r]*\r/, () => '%AI3_Cropmarks: ' + art.map(num).join(' ') + '\r');
    must(/%AI3_TemplateBox: [^\r]*\r/, () => '%AI3_TemplateBox: ' + [cx, cy, cx, cy].map(num).join(' ') + '\r');
    must(/%AI3_TileBox: [^\r]*\r/, () => '%AI3_TileBox: ' + [cx - 384, cy - 294, cx + 384, cy + 294].map(num).join(' ') + '\r');
    must(/%AI5_NumLayers: \d+\r/, () => '%AI5_NumLayers: ' + n + '\r');
    must(/%AI5_OpenViewLayers: \d+\r/, () => '%AI5_OpenViewLayers: ' + '7'.repeat(n) + '\r');
    // the artboard and the artwork's bounds in the document data (the slice source bounds)
    must(/(%_\(height\) \/String \(xmlnode-nodename\) ,\r%_)[^ \r]+( \/Real \(xmlnode-nodevalue\))/, (m, a, b) => a + num(bb[3] - bb[1]) + b);
    must(/(%_\(width\) \/String \(xmlnode-nodename\) ,\r%_)[^ \r]+( \/Real \(xmlnode-nodevalue\))/, (m, a, b) => a + num(bb[2] - bb[0]) + b);
    must(/(%_\(y\) \/String \(xmlnode-nodename\) ,\r%_)[^ \r]+( \/Real \(xmlnode-nodevalue\))/, (m, a, b) => a + num(bb[1]) + b);
    must(/(%_\(x\) \/String \(xmlnode-nodename\) ,\r%_)[^ \r]+( \/Real \(xmlnode-nodevalue\))/, (m, a, b) => a + num(bb[0]) + b);
    must(/%_[-\d.]+ [-\d.]+ \/RealPointRelToROrigin\r%_ \(PositionPoint1\) ,/, () => '%_' + num(art[0]) + ' ' + num(art[3]) + ' /RealPointRelToROrigin\r%_ (PositionPoint1) ,');
    must(/%_[-\d.]+ [-\d.]+ \/RealPointRelToROrigin\r%_ \(PositionPoint2\) ,/, () => '%_' + num(art[2]) + ' ' + num(art[1]) + ' /RealPointRelToROrigin\r%_ (PositionPoint2) ,');
    must(/%_\([0-9a-f-]{36}\) \/String \(ArtboardUUID\) ,/, () => '%_(' + uuid() + ') /String (ArtboardUUID) ,');
    return t;
  }
  /** The layers, bottom layer first, each with its objects. */
  function layersText(got) {
    const by = new Map(got.layers.map(l => [l, []]));
    for (const e of got.entities) (by.get(e.layer) || by.set(e.layer, []).get(e.layer)).push(e);
    const out = []; let i = 0;
    for (const [name, list] of by) {
      const c = LAYER_COLOURS[i++ % LAYER_COLOURS.length];
      out.push('%AI5_BeginLayer', '1 1 1 1 0 0 1 ' + c[0] + ' ' + c[1] + ' ' + c[2] + ' ' + c[3] + ' 0 50 0 Lb', '(' + psString(name) + ') Ln', '0 AE', '%_/ArtDictionary :', '%_/XMLUID : (' + xmlId(name) + ') ; (AI10_ArtUID) ,', '%_;', '%_', '0 A', '0 Xw');
      for (const e of list) out.push(...objectLines(e));
      out.push('LB', '%AI5_EndLayer--');
    }
    return out.join('\r') + '\r';
  }
  const TRAILER = '%%PageTrailer\rgsave annotatepage grestore showpage\r%%Trailer\r%%EOF';
  function documentText(got, pageW, pageH, title, when) { return headText(got, pageW, pageH, title, when) + layersText(got) + TRAILER; }
  const latin1 = s => { const u = new Uint8Array(s.length); for (let i = 0; i < s.length; i++) u[i] = s.charCodeAt(i) & 255; return u; };

  /* ── zstd: a frame of raw blocks (valid zstd without a compressor) ── */
  function zstdStored(data) {
    const blocks = Math.max(1, Math.ceil(data.length / ZBLOCK)), out = new Uint8Array(4 + 2 + blocks * 3 + data.length);
    out.set([0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x58], 0);   // magic, frame header (no size, no checksum, no dictionary), window 2 MB
    let o = 6;
    for (let i = 0; i < blocks; i++) {
      const part = data.subarray(i * ZBLOCK, Math.min(data.length, (i + 1) * ZBLOCK)), last = i === blocks - 1 ? 1 : 0, h = last | (0 << 1) | (part.length << 3);
      out[o++] = h & 255; out[o++] = (h >> 8) & 255; out[o++] = (h >> 16) & 255; out.set(part, o); o += part.length;
    }
    return out;
  }
  /** The private data blocks: `%AI24_ZStandard_Data` + the frame, cut in 65536-byte pieces. */
  function privateBlocks(text) {
    const frame = zstdStored(latin1(text)), tag = latin1(MARK), all = new Uint8Array(tag.length + frame.length); all.set(tag, 0); all.set(frame, tag.length);
    const parts = []; for (let i = 0; i < all.length; i += CHUNK) parts.push(all.subarray(i, Math.min(all.length, i + CHUNK)));
    return parts;
  }

  /* ── the PDF part ── */
  function xmp(title, when, ids, pageW, pageH, nLayers) {
    const esc = s => String(s).replace(/[&<>]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]));
    return '<?xpacket begin="﻿" id="W5M0MpCehiHzreSzNTczkc9d"?>\n<x:xmpmeta xmlns:x="adobe:ns:meta/" x:xmptk="Adobe XMP Core 9.1-c003 1.000000, 0000/00/00-00:00:00        ">\n <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">\n  <rdf:Description rdf:about=""\n    xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:xmp="http://ns.adobe.com/xap/1.0/" xmlns:xmpMM="http://ns.adobe.com/xap/1.0/mm/"\n    xmlns:stRef="http://ns.adobe.com/xap/1.0/sType/ResourceRef#" xmlns:stEvt="http://ns.adobe.com/xap/1.0/sType/ResourceEvent#" xmlns:illustrator="http://ns.adobe.com/illustrator/1.0/"\n    xmlns:pdf="http://ns.adobe.com/pdf/1.3/" xmlns:xmpTPg="http://ns.adobe.com/xap/1.0/t/pg/" xmlns:stDim="http://ns.adobe.com/xap/1.0/sType/Dimensions#">\n'
      + '   <dc:format>application/pdf</dc:format>\n   <dc:title><rdf:Alt><rdf:li xml:lang="x-default">' + esc(title) + '</rdf:li></rdf:Alt></dc:title>\n   <xmp:CreatorTool>Adobe Illustrator 29.5 (Macintosh)</xmp:CreatorTool>\n'
      + '   <xmp:CreateDate>' + when.iso + '</xmp:CreateDate>\n   <xmp:ModifyDate>' + when.iso + '</xmp:ModifyDate>\n   <xmp:MetadataDate>' + when.iso + '</xmp:MetadataDate>\n'
      + '   <xmpMM:RenditionClass>proof:pdf</xmpMM:RenditionClass>\n   <xmpMM:OriginalDocumentID>uuid:' + ids.orig + '</xmpMM:OriginalDocumentID>\n   <xmpMM:DocumentID>xmp.did:' + ids.doc + '</xmpMM:DocumentID>\n   <xmpMM:InstanceID>uuid:' + ids.inst + '</xmpMM:InstanceID>\n'
      + '   <xmpMM:History><rdf:Seq><rdf:li rdf:parseType="Resource"><stEvt:action>saved</stEvt:action><stEvt:instanceID>xmp.iid:' + ids.doc + '</stEvt:instanceID><stEvt:when>' + when.iso + '</stEvt:when><stEvt:softwareAgent>Adobe Illustrator 29.5 (Macintosh)</stEvt:softwareAgent><stEvt:changed>/</stEvt:changed></rdf:li></rdf:Seq></xmpMM:History>\n'
      + '   <illustrator:Type>Document</illustrator:Type>\n   <pdf:Producer>Adobe PDF library 17.00</pdf:Producer>\n   <xmpTPg:NPages>1</xmpTPg:NPages>\n   <xmpTPg:HasVisibleTransparency>False</xmpTPg:HasVisibleTransparency>\n   <xmpTPg:HasVisibleOverprint>False</xmpTPg:HasVisibleOverprint>\n'
      + '   <xmpTPg:MaxPageSize rdf:parseType="Resource"><stDim:w>' + num(pageW * MM) + '</stDim:w><stDim:h>' + num(pageH * MM) + '</stDim:h><stDim:unit>Millimeters</stDim:unit></xmpTPg:MaxPageSize>\n'
      + '  </rdf:Description>\n </rdf:RDF>\n</x:xmpmeta>\n<?xpacket end="w"?>';
  }
  const utf8 = s => (typeof TextEncoder !== 'undefined' ? new TextEncoder().encode(s) : latin1(unescape(encodeURIComponent(s))));

  /** The sheet as a native Illustrator file. → Promise<Uint8Array> */
  async function nativeAi(got, pageW, pageH, title) {
    const L = lib(); if (!L) throw new Error('pdf-lib is not loaded');
    if (!got || !Array.isArray(got.entities) || !Array.isArray(got.layers) || !got.layers.length) throw new Error('the production layers were not given');
    const when = stamps(new Date()), name = String(title || 'Charm production sheet');
    const text = documentText(got, pageW, pageH, name, when), blocks = privateBlocks(text), head = latin1(text.slice(0, text.indexOf('%%EndComments') + '%%EndComments\r'.length));
    const doc = await L.PDFDocument.create(), ctx = doc.context, page = doc.addPage([pageW, pageH]);
    const N = L.PDFName.of, S = L.PDFString.of;
    // the layers: top-level OCGs as Illustrator writes them, in the .dxf's order (bottom first); /Order lists the top layer first
    const refs = [], tag = new Map();
    for (const l of got.layers) {
      const ref = ctx.register(ctx.obj({ Type: 'OCG', Name: S(String(l)), Intent: ['View', 'Design'], Usage: { CreatorInfo: { Creator: S('Adobe Illustrator 29.5'), Subtype: 'Artwork' } } }));
      refs.push(ref); tag.set(l, 'MC' + (refs.length - 1));
    }
    const props = ctx.obj({}); got.layers.forEach((l, i) => props.set(N(tag.get(l)), refs[i]));
    page.node.Resources().set(N('Properties'), props);
    // each layer's drawing at page level, for any other viewer (what the document text holds)
    const rgb1 = c => (c || [0, 0, 0]).map(v => num(v / 255)).join(' ');
    const pdfPath = b => { let t = num(b.pts[0][0]) + ' ' + num(b.pts[0][1]) + ' m '; for (let i = 1; i + 2 < b.pts.length; i += 3) t += b.pts[i].map(num).join(' ') + ' ' + b.pts[i + 1].map(num).join(' ') + ' ' + b.pts[i + 2].map(num).join(' ') + ' c '; return t + (b.closed ? 'h ' : ''); };
    const by = new Map(got.layers.map(l => [l, []])); for (const e of got.entities) (by.get(e.layer) || by.set(e.layer, []).get(e.layer)).push(e);
    let ops = '';
    for (const [l, list] of by) {
      ops += '/OC /' + tag.get(l) + ' BDC\n';
      for (const e of list) {
        if (e.kind === 'spline') ops += 'q ' + rgb1(e.rgb) + ' RG ' + num(Math.max(0.1, (e.lw || 0) / 100 / MM)) + ' w 1 J 1 j ' + pdfPath(e.b) + 'S Q\n';
        else ops += 'q ' + rgb1(e.rgb) + ' rg ' + e.loops.map(pdfPath).join('') + 'f* Q\n';   // (a fill: its compound path alone; Illustrator's own import of the .dxf keeps a hatch's boundary curves inside the hatch, not as lines of their own)
      }
      ops += 'EMC\n';
    }
    page.node.set(N('Contents'), ctx.register(ctx.flateStream(utf8(ops))));
    doc.catalog.set(N('OCProperties'), ctx.obj({ OCGs: refs, D: { ON: refs, Order: refs.slice().reverse(), RBGroups: [] } }));
    // Illustrator's own document
    const metaRef = ctx.register(ctx.stream(head, {})), blockRefs = blocks.map(b => ctx.register(ctx.stream(b, {})));
    const priv = { AIMetaData: metaRef };
    blockRefs.forEach((r, i) => { priv['AIPrivateData' + (i + 1)] = r; });
    Object.assign(priv, { ContainerVersion: 12, CreatorVersion: 29, NumBlock: blocks.length, RoundtripStreamType: 2, RoundtripVersion: 24 });
    const privRef = ctx.register(ctx.obj(priv)), pieceRef = ctx.register(ctx.obj({ LastModified: S(when.pdf), Private: privRef }));
    page.node.set(N('PieceInfo'), ctx.obj({ Illustrator: pieceRef }));
    page.node.set(N('LastModified'), S(when.pdf));
    const box = [0, 0, pageW, pageH];
    for (const k of ['TrimBox', 'ArtBox', 'BleedBox', 'CropBox']) page.node.set(N(k), ctx.obj(box));
    const ids = { orig: uuid().replace(/-/g, '').toUpperCase(), doc: uuid(), inst: uuid() };
    const xref = ctx.register(ctx.stream(utf8(xmp(name, when, ids, pageW, pageH, got.layers.length)), { Type: 'Metadata', Subtype: 'XML' }));
    doc.catalog.set(N('Metadata'), xref);
    return await doc.save({ useObjectStreams: false });
  }
  /** A composed sheet's bytes (as CharmNestExport.productionAi takes them) as a native Illustrator file. */
  async function fromComposed(bytes, title) {
    const E = root && root.CharmNestExport || (typeof require === 'function' ? require('./charm-nest-export.js') : null), P = root && root.CharmNestPDF;
    if (!E || !P) throw new Error('the export code is not loaded');
    const parsed = await P.parseSource(bytes, 'Production sheet');
    return nativeAi(E.dxf(E.productionPaths(parsed), E.layerNames(parsed), { collectOnly: true }), parsed.pageW, parsed.pageH, title);
  }
  return { nativeAi, fromComposed, documentText, zstdStored, privateBlocks, objectLines, pathLines, xmlId, version: 1 };
});
