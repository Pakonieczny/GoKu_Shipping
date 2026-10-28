/* charm-nest-ce-import.js — VECTOR IMPORT: an existing vector design brought into the Charm Editor and worked on there (Paul, 28 Oct 2026: "the designer would either
 * bring in an existing vector design either an illustrator file or the XF file into the charm editor and then work on that file there and then save it"; item 23 and the
 * lead's D10: the editor imports .ai, .pdf, .svg and .dxf). The design keeps its EXACT size ("You need to ensure the sanctity of the designed charm. It's exact dimensions
 * and user designed features"): every reader works in points, 1:1, so a part placed in a face is the size it was drawn.
 *
 *   CharmNestCeImport.read(file) -> Promise<{ items, widthMm, heightMm, warnings, outlineId, charms, type, name, units }>
 *       file: a File or Blob with a name, or { name, bytes } / { name, text } (node). items: Charm Editor Items (charm-nest-charm-editor.js header), flat leaves, points,
 *       y down, the drawing's TIGHT box (Bézier extremes included) at 0,0, so widthMm × heightMm is its exact size. Each: { id, kind:'path', role (guessed), source:'added',
 *       name, subpaths, stroke, fill, lwPt, evenOdd, layer, closed }. outlineId: the part that is the charm's outline (the cut of a new design), or null.
 *       charms (an .ai holding several charms): [{ name, ids, outlineId, widthMm, heightMm }]. warnings: what was left out and why, in the shop's words.
 *         .ai / .pdf  the app's own reader, as Pool.masterCharm reads a master: CharmNestPDF.parseSource and groupCharms (both run in the compute worker in the page,
 *                     charm-nest-background.js), the roles as the art edit reads a master's parts (CharmNestArtEdit.roleOf)
 *         .svg        read here: path data (every command, arcs included), rect, circle, ellipse, line, polyline, polygon, use; transforms; viewBox and units; <style> classes
 *         .dxf        CharmNestDXF's parser and entity walk (LINE, LWPOLYLINE / POLYLINE with bulges, ARC, CIRCLE, ELLIPSE, HATCH, INSERT blocks, layers, $INSUNITS); a SPLINE
 *                     becomes Béziers here (exactly, by knot insertion; a rational one fitted within a few millionths of its size)
 *   CharmNestCeImport.faceDoc(result)  a FaceDoc of the design: framed by the drawing, its outline the cut ("Open as a new design": CharmNestCeMasters builds it with this)
 *   CharmNestCeImport.pick(o)          the small role picker shown before anything is placed -> Promise<{ mode: 'place' | 'new', result } | null>
 *   CharmNestCeImport.run(ctx, file, info)  the editor's importer (CharmEditor.importers: the Import button, a drop, a paste): read, pick, then "Place into this charm" (one
 *                                      group on the face, centred on the charm or where the file was dropped, at its exact size, one undo step) or "Open as a new design"
 *                                      (designOf(result) handed to CharmNestCeMasters: openNew(design) when it has one, else openDesign(design), an unsaved design opened in
 *                                      design mode, saved as a new design of the library, source { kind: 'import', file, type })
 *   CharmNestCeImport.guessRole(part), subset(result, ids), readers: { svg, dxf, pdf }
 *
 * ROLES (the lead, 28 Oct 2026, item 23: "red stroke = line engraving, black/blue fill = hatch, a CUT layer or black hairline = cut", shown in a small role picker before
 * placing): a layer named for cutting is cut, one named for engraving is engraved (a fill hatched, a stroke a line); else a fill is hatched, a coloured stroke is a line
 * engraving, and a black or grey hairline that closes is a cut line (an open one, or a thick one, is a line). The picker shows the parts in groups (layer and paint) with
 * the role each will take; a press on Cut / Line / Hatch / Art changes the whole group, and the editor's own role menu changes any one part afterwards.
 * Nothing here writes anything: the editor's Save does (and the design library behind it). CNDesignLog gets one 'vectorImport' event per import placed or opened. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestCeImport = api;
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const doc = root && root.document ? root.document : null;
  const MM = 25.4 / 72, PT = 72 / 25.4;               // mm per pt, pt per mm
  const KAPPA = 0.5522847498;
  const ROLES = ['cut', 'line', 'hatch', 'art'];
  const ROLE_NAME = { cut: 'Cut', line: 'Line', hatch: 'Hatch', art: 'Art' };
  const ROLE_SAYS = { cut: 'Cut line: the laser cuts through', line: 'Line engrave: engraved along the line', hatch: 'Hatch engrave: engraved as a fill', art: "Artwork: as drawn (a fill hatched, a stroke a line)" };
  const BLUE = [0, 0, 1], BLACK = [0, 0, 0];
  const HAIRLINE_MAX_PT = 1;                           // a stroke up to 1 pt (0.35 mm) is a hairline: what a cut line is drawn with (0.001 in, 0.25 pt, 0.5 pt, 1 pt)
  const MAX_BYTES = 60 * 1024 * 1024;
  const MERGE_OPEN_OVER = 40;                          // a DXF layer with more loose lines than this keeps them as one part (else 500 strokes are 500 layers)
  // (the same words charm-nest-dxf.js reads a layer by: a layer named for cutting is cut, one named for engraving is engraved, whatever its colour)
  const CUT_LAYER = /^(?:.*[\s_-])?(?:cut|cuts|cutout|cut-out|cutline|cut line|outline|thru|through)(?:$|[\s:_()\/-])/i;
  const ENGRAVE_LAYER = /^(?:.*[\s_-])?(?:engrav\w*|etch\w*|hatch\w*|mark\w*|score\w*|front detail|artwork)(?:$|[\s:_()\/-])/i;
  const fin = v => typeof v === 'number' && Number.isFinite(v);
  const r6 = v => { const r = Math.round(v * 1e6) / 1e6; return Object.is(r, -0) ? 0 : r; };
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const mm2 = v => (Math.round(v * 100) / 100).toFixed(2);
  const tick = () => new Promise(r => setTimeout(r, 0));
  const now = () => (root.performance && root.performance.now ? root.performance.now() : Date.now());
  /** Cooperative slicing: work runs until `ms` have gone by, then gives the page a frame (step() -> true when it did). */
  function slicer(ms) { let t0 = now(); return { async step() { if (now() - t0 > ms) { await tick(); t0 = now(); return true; } return false; } }; }

  /* the app's own modules (loaded before this file in the page; require()'d in node) */
  const need = (name, file) => (root && root[name]) || (typeof require === 'function' ? (() => { try { return require(file); } catch (e) { return null; } })() : null);
  const CE = () => need('CharmEditor', './charm-nest-charm-editor.js');
  const DXF = () => need('CharmNestDXF', './charm-nest-dxf.js');
  const AE = () => need('CharmNestArtEdit', './charm-nest-art-edit.js');
  const model = () => { const c = CE(); return c && c.model; };
  let seq = 0;
  const newId = () => { const m = model(); return m && m.newId ? m.newId() : 'ci' + Date.now().toString(36) + (++seq).toString(36); };

  /* ═══ 1 · matrices (SVG order: [a,b,c,d,e,f] maps x,y to a·x + c·y + e, b·x + d·y + f; mul(m, n) applies n first) ═══ */
  const I = [1, 0, 0, 1, 0, 0];
  const mul = (m, n) => [m[0] * n[0] + m[2] * n[1], m[1] * n[0] + m[3] * n[1], m[0] * n[2] + m[2] * n[3], m[1] * n[2] + m[3] * n[3], m[0] * n[4] + m[2] * n[5] + m[4], m[1] * n[4] + m[3] * n[5] + m[5]];
  const ap = (m, p) => [m[0] * p[0] + m[2] * p[1] + m[4], m[1] * p[0] + m[3] * p[1] + m[5]];
  const scaleOf = m => Math.sqrt(Math.abs(m[0] * m[3] - m[1] * m[2])) || 0;
  const mapSubs = (subs, f) => (subs || []).map(sp => sp.map(o => (o[0] === 'h' ? ['h'] : [o[0]].concat(o.slice(1).map(f)))));

  /* ═══ 2 · subpaths: clean, close, measure ═══ */
  const endOf = op => (op[0] === 'c' ? op[3] : op[1]);
  /** Lines of no length dropped, a subpath that ends where it began closed (it was drawn closed: an outline clicked back onto its first point), empty ones gone. */
  function cleanSubs(subs, eps) {
    const out = [];
    for (const sp of subs || []) {
      if (!sp || !sp.length || sp[0][0] !== 'm') continue;
      const s = [sp[0]]; let cur = sp[0][1], closed = false;
      for (let i = 1; i < sp.length; i++) {
        const o = sp[i];
        if (o[0] === 'h') { closed = true; break; }
        if (o[0] === 'm') continue;
        if (!o.slice(1).every(p => Array.isArray(p) && fin(p[0]) && fin(p[1]))) continue;
        const e = endOf(o);
        if (o[0] === 'l' && Math.abs(e[0] - cur[0]) <= 1e-9 && Math.abs(e[1] - cur[1]) <= 1e-9) continue;
        s.push(o); cur = e;
      }
      if (s.length < 2) continue;
      const a = s[0][1];
      if (!closed && Math.abs(cur[0] - a[0]) <= eps && Math.abs(cur[1] - a[1]) <= eps && s.length > 2) closed = true;
      if (closed) {
        // a closing line drawn onto the start is the close itself
        const last = s[s.length - 1];
        if (last[0] === 'l' && Math.abs(last[1][0] - a[0]) <= eps && Math.abs(last[1][1] - a[1]) <= eps && s.length > 2) s.pop();
        else if (last[0] === 'c' && (last[3][0] !== a[0] || last[3][1] !== a[1]) && Math.abs(last[3][0] - a[0]) <= eps && Math.abs(last[3][1] - a[1]) <= eps) s[s.length - 1] = ['c', last[1], last[2], a.slice()];
        s.push(['h']);
      }
      out.push(s);
    }
    return out;
  }
  const isClosed = subs => subs.length > 0 && subs.every(sp => sp[sp.length - 1][0] === 'h');
  function tightBox(subs) {
    const m = model(); if (m && m.bboxOf) return m.bboxOf(subs);
    let b = null; for (const sp of subs) for (const o of sp) for (const p of o.slice(1)) b = b ? [Math.min(b[0], p[0]), Math.min(b[1], p[1]), Math.max(b[2], p[0]), Math.max(b[3], p[1])] : [p[0], p[1], p[0], p[1]];
    return b;
  }
  const union = (a, b) => (!a ? b : !b ? a : [Math.min(a[0], b[0]), Math.min(a[1], b[1]), Math.max(a[2], b[2]), Math.max(a[3], b[3])]);
  const area = b => (b ? Math.max(0, b[2] - b[0]) * Math.max(0, b[3] - b[1]) : 0);
  const opCount = (subs, k) => subs.reduce((n, sp) => n + sp.filter(o => o[0] === k).length, 0);

  /* ═══ 3 · roles ═══ */
  const achromatic = c => !!c && Math.max(c[0], c[1], c[2]) - Math.min(c[0], c[1], c[2]) <= 0.15;
  const hatchBlue = c => !!c && c[2] >= 0.5 && c[2] - Math.max(c[0], c[1]) >= 0.4;
  const white = c => !!c && Math.min(c[0], c[1], c[2]) >= 0.97;
  /** The role a part is guessed to take: { fill, stroke (rgb 0..1 or null), lwPt, closed, layer }. */
  function guessRole(p) {
    const L = String((p && p.layer) || '').trim();
    if (L && CUT_LAYER.test(L) && !(p.fill && !p.stroke && hatchBlue(p.fill))) return 'cut';   // (a blue fill is hatching wherever it was drawn: charm-nest-geom.js pathRole)
    if (L && ENGRAVE_LAYER.test(L)) return p.fill ? 'hatch' : 'line';
    if (p.fill) return 'hatch';
    if (p.stroke) return !achromatic(p.stroke) ? 'line' : p.closed && !(+p.lwPt > HAIRLINE_MAX_PT) ? 'cut' : 'line';
    return 'art';
  }
  function colourWord(c) {
    if (!c) return 'no colour';
    const mx = Math.max(c[0], c[1], c[2]), mn = Math.min(c[0], c[1], c[2]), l = (mx + mn) / 2;
    if (mx - mn <= 0.15) return l < 0.2 ? 'black' : l > 0.92 ? 'white' : 'grey';
    let h; const d = mx - mn;
    if (mx === c[0]) h = ((c[1] - c[2]) / d) % 6; else if (mx === c[1]) h = (c[2] - c[0]) / d + 2; else h = (c[0] - c[1]) / d + 4;
    h = (h * 60 + 360) % 360;
    return h < 15 || h >= 345 ? 'red' : h < 45 ? 'orange' : h < 70 ? 'yellow' : h < 165 ? 'green' : h < 195 ? 'cyan' : h < 255 ? 'blue' : h < 290 ? 'purple' : 'pink';
  }
  const cssRgb = c => (c ? `rgb(${c.map(v => Math.round(Math.max(0, Math.min(1, v)) * 255)).join(',')})` : 'none');

  /* ═══ 4 · the result: parts placed at 0,0, ids, the outline ═══ */
  /** parts: { subpaths (pt, y down), stroke, fill, lwPt, evenOdd, layer, name, role?, charm?, outline? } -> the read result (in slices of 8 ms: a sheet of 132 charms is 120 ms of work). */
  async function finish(parts, meta) {
    const warnings = (meta.warnings || []).slice(), slice = slicer(8);
    let box = null; const keep = [];
    for (const p of parts) {
      await slice.step();
      const subs = cleanSubs(p.subpaths, 1e-3);
      if (!subs.length) continue;
      const b = tightBox(subs); if (!b) continue;
      keep.push(Object.assign({}, p, { subpaths: subs, box: b })); box = union(box, b);
    }
    if (!keep.length || !box) throw new Error(`${meta.name}: nothing drawn in it that can be cut or engraved${warnings.length ? ' (' + warnings.join('; ') + ')' : ''}`);
    const x0 = box[0], y0 = box[1], items = [];
    for (let i = 0; i < keep.length; i++) {
      await slice.step();
      const p = keep[i];
      const subs = mapSubs(p.subpaths, q => [r6(q[0] - x0), r6(q[1] - y0)]), closed = isClosed(subs);
      const it = { id: newId(), kind: 'path', role: ROLES.includes(p.role) ? p.role : guessRole(Object.assign({}, p, { closed })), source: 'added', name: p.name || defaultName(p, i),
        subpaths: subs, stroke: p.stroke ? p.stroke.slice(0, 3) : null, fill: p.fill ? p.fill.slice(0, 3) : null, lwPt: p.stroke && fin(p.lwPt) && p.lwPt > 0 ? r6(p.lwPt) : null,
        layer: p.layer || null, closed, tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 };
      if (p.evenOdd) { it.evenOdd = true; it.even = true; }   // (the editor draws evenOdd; the art edit writes `even`)
      p.item = it; items.push(it);
    }
    // the outline: the one the reader named, else the largest closed part (a cut line before any other) that spans most of the drawing
    let outline = keep.find(p => p.outline) || null;
    if (!outline) {
      const big = keep.filter(p => p.item.closed && area(p.box) >= 0.5 * area(box)).sort((a, b) => (b.item.role === 'cut') - (a.item.role === 'cut') || area(b.box) - area(a.box));
      outline = big[0] || null;
    }
    const charms = meta.charms && meta.charms.length > 1 ? meta.charms.map((c, k) => {
      const mine = keep.filter(p => p.charm === k); let b = null; for (const p of mine) b = union(b, p.box);
      const o = mine.find(p => p.outline || p.charmOutline);
      return b ? { name: c.name || `Charm ${k + 1}`, ids: mine.map(p => p.item.id), outlineId: o ? o.item.id : null, widthMm: (b[2] - b[0]) * MM, heightMm: (b[3] - b[1]) * MM } : null;
    }).filter(Boolean) : null;
    if (!outline && meta.type !== 'pdf') warnings.push('no outline found: the parts are placed as drawn (a new design gets its cut line when a part is set to Cut)');
    return { name: meta.name, type: meta.type, units: meta.units || 'pt', items, widthMm: (box[2] - box[0]) * MM, heightMm: (box[3] - box[1]) * MM,
      outlineId: outline ? outline.item.id : null, charms, warnings };
  }
  const SHAPE_NAME = { path: 'Path', rect: 'Rectangle', circle: 'Circle', ellipse: 'Ellipse', line: 'Line', polyline: 'Polyline', polygon: 'Polygon' };
  function defaultName(p, i) {
    const what = p.fill ? `${colourWord(p.fill)} fill` : p.stroke ? `${colourWord(p.stroke)} line` : 'part';
    return (p.layer ? p.layer + ' · ' : '') + what[0].toUpperCase() + what.slice(1) + ' ' + (i + 1);
  }
  /** The parts with these ids, at 0,0 again (one charm of an .ai that holds several). */
  function subset(r, ids) {
    const want = new Set(ids || []), items = r.items.filter(it => want.has(it.id));
    if (!items.length || items.length === r.items.length) return r;
    let b = null; for (const it of items) b = union(b, tightBox(it.subpaths));
    const moved = items.map(it => Object.assign({}, it, { subpaths: mapSubs(it.subpaths, q => [r6(q[0] - b[0]), r6(q[1] - b[1])]) }));
    const ch = (r.charms || []).find(c => c.ids.length === items.length && c.ids.every(id => want.has(id)));
    const outlineId = want.has(r.outlineId) ? r.outlineId : ch ? ch.outlineId : null;
    return Object.assign({}, r, { items: moved, widthMm: (b[2] - b[0]) * MM, heightMm: (b[3] - b[1]) * MM, outlineId, charms: null, name: ch && r.charms.length > 1 ? `${r.name} · ${ch.name}` : r.name });
  }

  /* ═══ 5 · SVG ═══ */
  /** A small XML reader: elements, attributes (entities decoded, the DOCTYPE's own entities too: Illustrator writes xmlns="&ns_svg;"), text of <style>, nothing else. */
  function parseXml(src) {
    const ents = { amp: '&', lt: '<', gt: '>', quot: '"', apos: "'" };
    const decode = s => (s.indexOf('&') < 0 ? s : s.replace(/&(#x[0-9a-f]+|#\d+|[a-z_][\w.-]*);/gi, (m, e) => {
      if (e[0] === '#') { const v = e[1] === 'x' || e[1] === 'X' ? parseInt(e.slice(2), 16) : parseInt(e.slice(1), 10); try { return String.fromCodePoint(v); } catch (er) { return ''; } }
      return ents[e] != null ? ents[e] : m;
    }));
    const top = { name: '#root', local: '#root', attrs: {}, children: [], text: '' }, stack = [top], n = src.length;
    const isWs = c => c === 32 || c === 9 || c === 10 || c === 13;
    let i = 0;
    while (i < n) {
      const lt = src.indexOf('<', i);
      const cur = stack[stack.length - 1];
      if (lt < 0) { if (cur.local === 'style') cur.text += decode(src.slice(i)); break; }
      if (lt > i && cur.local === 'style') cur.text += decode(src.slice(i, lt));
      if (src.startsWith('<!--', lt)) { const e = src.indexOf('-->', lt + 4); i = e < 0 ? n : e + 3; continue; }
      if (src.startsWith('<![CDATA[', lt)) { const e = src.indexOf(']]>', lt + 9); if (cur.local === 'style') cur.text += src.slice(lt + 9, e < 0 ? n : e); i = e < 0 ? n : e + 3; continue; }
      if (src.startsWith('<?', lt)) { const e = src.indexOf('?>', lt + 2); i = e < 0 ? n : e + 2; continue; }
      if (src.startsWith('<!', lt)) {
        let j = lt + 2, depth = 0;
        for (; j < n; j++) { const c = src[j]; if (c === '[') depth++; else if (c === ']') depth--; else if (c === '>' && depth <= 0) break; }
        src.slice(lt, j + 1).replace(/<!ENTITY\s+([\w.-]+)\s+(?:"([^"]*)"|'([^']*)')\s*>/g, (m, k, a, b) => { ents[k] = a != null ? a : b; return m; });
        i = j + 1; continue;
      }
      if (src[lt + 1] === '/') {
        const e = src.indexOf('>', lt), name = src.slice(lt + 2, e < 0 ? n : e).trim();
        for (let k = stack.length - 1; k > 0; k--) if (stack[k].name === name) { stack.length = k; break; }
        i = e < 0 ? n : e + 1; continue;
      }
      let j = lt + 1; while (j < n && !isWs(src.charCodeAt(j)) && src[j] !== '/' && src[j] !== '>') j++;
      const name = src.slice(lt + 1, j), attrs = {}; let self = false;
      for (;;) {
        while (j < n && isWs(src.charCodeAt(j))) j++;
        if (j >= n) break;
        if (src[j] === '>') { j++; break; }
        if (src[j] === '/' && src[j + 1] === '>') { j += 2; self = true; break; }
        let k = j; while (k < n && !isWs(src.charCodeAt(k)) && src[k] !== '=' && src[k] !== '>' && !(src[k] === '/' && src[k + 1] === '>')) k++;
        const an = src.slice(j, k); j = k;
        while (j < n && isWs(src.charCodeAt(j))) j++;
        let val = '';
        if (src[j] === '=') {
          j++; while (j < n && isWs(src.charCodeAt(j))) j++;
          const q = src[j];
          if (q === '"' || q === "'") { const e = src.indexOf(q, j + 1); val = src.slice(j + 1, e < 0 ? n : e); j = e < 0 ? n : e + 1; }
          else { let e = j; while (e < n && !isWs(src.charCodeAt(e)) && src[e] !== '>') e++; val = src.slice(j, e); j = e; }
        }
        if (an) attrs[an] = decode(val); else j++;
      }
      const node = { name, local: name.slice(name.indexOf(':') + 1), attrs, children: [], text: '', parent: cur };
      cur.children.push(node);
      if (!self) stack.push(node);
      i = j;
    }
    return top;
  }
  /* CSS: the declarations of a style attribute, and the simple rules of a <style> element (.cls-1, #id, tag, tag.cls, a list of them; a descendant selector is read by
     its last part). Illustrator writes every paint as a class: <style>.cls-1{fill:#e30613;}</style>. */
  function parseDecl(s) {
    const out = {};
    for (const part of String(s || '').split(';')) { const k = part.indexOf(':'); if (k < 0) continue; const p = part.slice(0, k).trim().toLowerCase(), v = part.slice(k + 1).replace(/!important/i, '').trim(); if (p) out[p] = v; }
    return out;
  }
  function cssRules(text) {
    const rules = []; let order = 0;
    text = String(text || '').replace(/\/\*[\s\S]*?\*\//g, '');
    let i = 0;
    while (i < text.length) {
      const o = text.indexOf('{', i); if (o < 0) break;
      const head = text.slice(i, o).trim();
      // the block: nested braces skipped (@media, @font-face)
      let depth = 1, j = o + 1; for (; j < text.length && depth; j++) { if (text[j] === '{') depth++; else if (text[j] === '}') depth--; }
      const body = text.slice(o + 1, j - 1); i = j;
      if (!head || head[0] === '@') continue;
      const decl = parseDecl(body);
      for (const raw of head.split(',')) {
        const last = raw.trim().split(/[\s>+~]+/).pop(); if (!last) continue;
        const m = /^([a-zA-Z][\w-]*|\*)?((?:[.#][\w-]+)*)$/.exec(last); if (!m) continue;
        const sel = { tag: m[1] && m[1] !== '*' ? m[1].toLowerCase() : null, id: null, classes: [] };
        (m[2].match(/[.#][\w-]+/g) || []).forEach(t => { if (t[0] === '#') sel.id = t.slice(1); else sel.classes.push(t.slice(1)); });
        rules.push({ sel, spec: (sel.id ? 100 : 0) + sel.classes.length * 10 + (sel.tag ? 1 : 0), order: order++, decl });
      }
    }
    return rules.sort((a, b) => a.spec - b.spec || a.order - b.order);
  }
  const matches = (sel, node) => {
    if (sel.tag && sel.tag !== node.local.toLowerCase()) return false;
    if (sel.id && node.attrs.id !== sel.id) return false;
    if (sel.classes.length) { const cl = String(node.attrs.class || '').split(/\s+/); if (!sel.classes.every(c => cl.includes(c))) return false; }
    return true;
  };
  const NAMED = { black: [0, 0, 0], white: [1, 1, 1], red: [1, 0, 0], lime: [0, 1, 0], green: [0, 0.5, 0], blue: [0, 0, 1], yellow: [1, 1, 0], cyan: [0, 1, 1], aqua: [0, 1, 1],
    magenta: [1, 0, 1], fuchsia: [1, 0, 1], gray: [0.5, 0.5, 0.5], grey: [0.5, 0.5, 0.5], silver: [0.75, 0.75, 0.75], maroon: [0.5, 0, 0], navy: [0, 0, 0.5], olive: [0.5, 0.5, 0],
    purple: [0.5, 0, 0.5], teal: [0, 0.5, 0.5], orange: [1, 0.647, 0], darkgray: [0.663, 0.663, 0.663], darkgrey: [0.663, 0.663, 0.663], lightgray: [0.827, 0.827, 0.827], darkred: [0.545, 0, 0], darkblue: [0, 0, 0.545] };
  /** A paint: [r,g,b] (0..1), null for none; `ctx` resolves currentColor and url(#gradient). */
  function parsePaint(v, ctx) {
    v = String(v == null ? '' : v).trim();
    if (!v || v === 'none' || v === 'transparent') return null;
    if (/^currentcolor$/i.test(v)) return parsePaint(ctx.color || 'black', {});
    if (/^url\(/i.test(v)) {
      const m = /^url\(\s*['"]?#([^'")\s]+)['"]?\s*\)\s*(.*)$/i.exec(v), g = m && ctx.byId && ctx.byId(m[1]);
      const stop = g && firstStop(g, ctx, 0);
      if (stop !== undefined) return stop || [0.5, 0.5, 0.5];
      return m && m[2] ? parsePaint(m[2], ctx) : [0.5, 0.5, 0.5];   // (a pattern, or a gradient with no stops: a mid grey, still a fill)
    }
    let m;
    if ((m = /^#([0-9a-f]{3,8})$/i.exec(v))) { let h = m[1]; if (h.length === 3 || h.length === 4) h = h.split('').map(c => c + c).join(''); return [0, 2, 4].map(k => parseInt(h.slice(k, k + 2), 16) / 255); }
    if ((m = /^rgba?\(([^)]*)\)$/i.exec(v))) { const p = m[1].split(/[\s,\/]+/).filter(Boolean).slice(0, 3); return p.map(x => (/%$/.test(x) ? parseFloat(x) / 100 : parseFloat(x) / 255)).map(x => (fin(x) ? Math.max(0, Math.min(1, x)) : 0)); }
    if (NAMED[v.toLowerCase()]) return NAMED[v.toLowerCase()].slice();
    return [0, 0, 0];
  }
  function firstStop(g, ctx, depth) {
    if (!g || depth > 4) return undefined;
    if (g.local !== 'linearGradient' && g.local !== 'radialGradient') return g.local === 'pattern' ? null : undefined;
    const st = g.children.find(c => c.local === 'stop');
    if (st) { const d = parseDecl(st.attrs.style); return parsePaint(d['stop-color'] || st.attrs['stop-color'] || 'black', ctx) || [0.5, 0.5, 0.5]; }
    const href = g.attrs.href || g.attrs['xlink:href'];
    return href && href[0] === '#' ? firstStop(ctx.byId(href.slice(1)), ctx, depth + 1) : undefined;
  }
  function parseTransform(s) {
    let M = I.slice();
    const re = /(matrix|translate|scale|rotate|skewX|skewY)\s*\(([^)]*)\)/gi; let m;
    while ((m = re.exec(String(s || '')))) {
      const a = (m[2].match(/[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?/g) || []).map(Number), k = m[1].toLowerCase();
      let T = I;
      if (k === 'matrix' && a.length >= 6) T = a.slice(0, 6);
      else if (k === 'translate') T = [1, 0, 0, 1, a[0] || 0, a[1] || 0];
      else if (k === 'scale') T = [a[0] == null ? 1 : a[0], 0, 0, a[1] == null ? (a[0] == null ? 1 : a[0]) : a[1], 0, 0];
      else if (k === 'rotate') { const r = (a[0] || 0) * Math.PI / 180, c = Math.cos(r), sn = Math.sin(r), R = [c, sn, -sn, c, 0, 0]; T = a.length >= 3 ? mul([1, 0, 0, 1, a[1], a[2]], mul(R, [1, 0, 0, 1, -a[1], -a[2]])) : R; }
      else if (k === 'skewx') T = [1, 0, Math.tan((a[0] || 0) * Math.PI / 180), 1, 0, 0];
      else if (k === 'skewy') T = [1, Math.tan((a[0] || 0) * Math.PI / 180), 0, 1, 0, 0];
      M = mul(M, T);
    }
    return M;
  }
  /** SVG path data -> subpaths in user units (every command; arcs as Béziers of 90° or less, quadratics raised to cubics exactly). Stops at the first error, as SVG does. */
  const NUM_RE = /[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?/y;
  function pathSubs(d) {
    d = String(d || ''); const n = d.length, subs = [];
    let i = 0, sp = null, cx = 0, cy = 0, sx = 0, sy = 0, kx = null, ky = null, qx = null, qy = null, cmd = null;
    const ws = () => { while (i < n) { const c = d.charCodeAt(i); if (c === 32 || c === 9 || c === 10 || c === 13 || c === 12 || c === 44) i++; else break; } };
    const num = () => { ws(); NUM_RE.lastIndex = i; const m = NUM_RE.exec(d); if (!m) return NaN; i = NUM_RE.lastIndex; return +m[0]; };
    const flag = () => { ws(); const c = d[i]; if (c === '0' || c === '1') { i++; return c === '1' ? 1 : 0; } return NaN; };
    const moveTo = (x, y) => { sp = [['m', [x, y]]]; subs.push(sp); cx = sx = x; cy = sy = y; };
    const open = () => { if (!sp) moveTo(cx, cy); };
    const nums = k => { const a = []; for (let t = 0; t < k; t++) { const v = num(); if (!fin(v)) return null; a.push(v); } return a; };
    for (;;) {
      ws(); if (i >= n) break;
      const ch = d[i];
      if (/[A-Za-z]/.test(ch)) {
        i++; cmd = ch;
        if (ch === 'Z' || ch === 'z') { if (sp) { sp.push(['h']); sp = null; } cx = sx; cy = sy; kx = qx = null; continue; }
        if ('MmLlHhVvCcSsQqTtAa'.indexOf(ch) < 0) break;
      } else if (!cmd || cmd === 'Z' || cmd === 'z') break;
      const rel = cmd >= 'a', C = cmd.toUpperCase(), ox = rel ? cx : 0, oy = rel ? cy : 0;
      let a, cubic = null, quad = null;
      if (C === 'M') { if (!(a = nums(2))) break; moveTo(ox + a[0], oy + a[1]); cmd = rel ? 'l' : 'L'; kx = qx = null; continue; }
      if (C === 'L') { if (!(a = nums(2))) break; open(); cx = ox + a[0]; cy = oy + a[1]; sp.push(['l', [cx, cy]]); }
      else if (C === 'H') { if (!(a = nums(1))) break; open(); cx = (rel ? cx : 0) + a[0]; sp.push(['l', [cx, cy]]); }
      else if (C === 'V') { if (!(a = nums(1))) break; open(); cy = (rel ? cy : 0) + a[0]; sp.push(['l', [cx, cy]]); }
      else if (C === 'C') { if (!(a = nums(6))) break; open(); cubic = [[ox + a[0], oy + a[1]], [ox + a[2], oy + a[3]], [ox + a[4], oy + a[5]]]; }
      else if (C === 'S') { if (!(a = nums(4))) break; open(); cubic = [kx != null ? [2 * cx - kx, 2 * cy - ky] : [cx, cy], [ox + a[0], oy + a[1]], [ox + a[2], oy + a[3]]]; }
      else if (C === 'Q') { if (!(a = nums(4))) break; open(); quad = [[ox + a[0], oy + a[1]], [ox + a[2], oy + a[3]]]; }
      else if (C === 'T') { if (!(a = nums(2))) break; open(); quad = [qx != null ? [2 * cx - qx, 2 * cy - qy] : [cx, cy], [ox + a[0], oy + a[1]]]; }
      else if (C === 'A') {
        const rx = num(), ry = num(), rot = num(), fa = flag(), fs = flag(), x = num(), y = num();
        if (![rx, ry, rot, fa, fs, x, y].every(fin)) break;
        open(); for (const o of arcCubics(cx, cy, rx, ry, rot, fa, fs, ox + x, oy + y)) sp.push(o);
        cx = ox + x; cy = oy + y;
      }
      if (cubic) { sp.push(['c', cubic[0], cubic[1], cubic[2]]); kx = cubic[1][0]; ky = cubic[1][1]; cx = cubic[2][0]; cy = cubic[2][1]; qx = null; }
      else if (quad) { const [q, p] = quad; sp.push(['c', [cx + 2 / 3 * (q[0] - cx), cy + 2 / 3 * (q[1] - cy)], [p[0] + 2 / 3 * (q[0] - p[0]), p[1] + 2 / 3 * (q[1] - p[1])], p]); qx = q[0]; qy = q[1]; cx = p[0]; cy = p[1]; kx = null; }
      else { kx = qx = null; }
    }
    return subs;
  }
  /** An elliptical arc (SVG endpoint form, its implementation notes F.6.5) as cubic Béziers of at most 90° each, ending exactly on the end point. */
  function arcCubics(x1, y1, rx, ry, phiDeg, fa, fs, x2, y2) {
    if (x1 === x2 && y1 === y2) return [];
    rx = Math.abs(rx); ry = Math.abs(ry);
    if (!rx || !ry) return [['l', [x2, y2]]];
    const phi = phiDeg * Math.PI / 180, cp = Math.cos(phi), sp = Math.sin(phi);
    const dx = (x1 - x2) / 2, dy = (y1 - y2) / 2, x1p = cp * dx + sp * dy, y1p = -sp * dx + cp * dy;
    const lam = (x1p * x1p) / (rx * rx) + (y1p * y1p) / (ry * ry);
    if (lam > 1) { const s = Math.sqrt(lam); rx *= s; ry *= s; }
    const rx2 = rx * rx, ry2 = ry * ry, den = rx2 * y1p * y1p + ry2 * x1p * x1p;
    let co = den ? Math.sqrt(Math.max(0, (rx2 * ry2 - den) / den)) : 0; if (fa === fs) co = -co;
    const cxp = co * rx * y1p / ry, cyp = -co * ry * x1p / rx;
    const cx = cp * cxp - sp * cyp + (x1 + x2) / 2, cy = sp * cxp + cp * cyp + (y1 + y2) / 2;
    const ang = (ux, uy, vx, vy) => Math.atan2(ux * vy - uy * vx, ux * vx + uy * vy);
    const t1 = ang(1, 0, (x1p - cxp) / rx, (y1p - cyp) / ry);
    let dt = ang((x1p - cxp) / rx, (y1p - cyp) / ry, (-x1p - cxp) / rx, (-y1p - cyp) / ry);
    if (!fs && dt > 0) dt -= 2 * Math.PI; else if (fs && dt < 0) dt += 2 * Math.PI;
    const nSeg = Math.max(1, Math.ceil(Math.abs(dt) / (Math.PI / 2) - 1e-9)), step = dt / nSeg, k = 4 / 3 * Math.tan(step / 4);
    const at = t => [cx + rx * Math.cos(t) * cp - ry * Math.sin(t) * sp, cy + rx * Math.cos(t) * sp + ry * Math.sin(t) * cp];
    const der = t => [-rx * Math.sin(t) * cp - ry * Math.cos(t) * sp, -rx * Math.sin(t) * sp + ry * Math.cos(t) * cp];
    const out = [];
    for (let s = 0; s < nSeg; s++) {
      const a = t1 + s * step, b = a + step, p0 = at(a), p3 = s === nSeg - 1 ? [x2, y2] : at(b), da = der(a), db = der(b);
      out.push(['c', [p0[0] + k * da[0], p0[1] + k * da[1]], [p3[0] - k * db[0], p3[1] - k * db[1]], p3]);
    }
    return out;
  }
  function rectSubs(x, y, w, h, rx, ry) {
    if (!(w > 0 && h > 0)) return [];
    rx = Math.min(Math.max(0, rx), w / 2); ry = Math.min(Math.max(0, ry), h / 2);
    if (!(rx > 0 && ry > 0)) return [[['m', [x, y]], ['l', [x + w, y]], ['l', [x + w, y + h]], ['l', [x, y + h]], ['h']]];
    const kx = rx * KAPPA, ky = ry * KAPPA, X = x + w, Y = y + h;
    return [[['m', [x + rx, y]], ['l', [X - rx, y]], ['c', [X - rx + kx, y], [X, y + ry - ky], [X, y + ry]], ['l', [X, Y - ry]], ['c', [X, Y - ry + ky], [X - rx + kx, Y], [X - rx, Y]],
      ['l', [x + rx, Y]], ['c', [x + rx - kx, Y], [x, Y - ry + ky], [x, Y - ry]], ['l', [x, y + ry]], ['c', [x, y + ry - ky], [x + rx - kx, y], [x + rx, y]], ['h']]];
  }
  function ellipseSubs(cx, cy, rx, ry) {
    if (!(rx > 0 && ry > 0)) return [];
    const kx = rx * KAPPA, ky = ry * KAPPA;
    return [[['m', [cx + rx, cy]], ['c', [cx + rx, cy + ky], [cx + kx, cy + ry], [cx, cy + ry]], ['c', [cx - kx, cy + ry], [cx - rx, cy + ky], [cx - rx, cy]],
      ['c', [cx - rx, cy - ky], [cx - kx, cy - ry], [cx, cy - ry]], ['c', [cx + kx, cy - ry], [cx + rx, cy - ky], [cx + rx, cy]], ['h']]];
  }
  const pointsOf = s => { const a = (String(s || '').match(/[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?/g) || []).map(Number), out = []; for (let k = 0; k + 1 < a.length; k += 2) out.push([a[k], a[k + 1]]); return out; };
  // lengths: user units are CSS px (96 to the inch) unless the file is Illustrator's, whose px are points
  const UNIT_PX = { px: 1, pt: 96 / 72, pc: 16, mm: 96 / 25.4, cm: 96 / 2.54, in: 96, q: 96 / 101.6, em: 16, ex: 8 };
  const UNIT_PT = { pt: 1, pc: 12, mm: PT, cm: 10 * PT, in: 72, q: PT / 4 };
  function lenOf(v, ref) {
    const m = /^\s*([-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?)\s*([a-z%]*)\s*$/i.exec(String(v == null ? '' : v)); if (!m) return null;
    const x = +m[1], u = m[2].toLowerCase();
    if (!u) return x;
    if (u === '%') return ref ? x / 100 * ref : null;
    return UNIT_PX[u] ? x * UNIT_PX[u] : x;
  }
  const NO_DRAW = new Set(['defs', 'clipPath', 'mask', 'pattern', 'marker', 'linearGradient', 'radialGradient', 'symbol', 'style', 'script', 'title', 'desc', 'metadata', 'foreignObject', 'filter', 'namedview', 'font', 'font-face', 'cursor', 'view']);
  const INHERIT = ['fill', 'stroke', 'stroke-width', 'fill-rule', 'visibility', 'color', 'fill-opacity', 'stroke-opacity'];
  const PRES = ['fill', 'stroke', 'stroke-width', 'fill-rule', 'display', 'visibility', 'opacity', 'fill-opacity', 'stroke-opacity', 'color'];
  const autoId = id => /^(?:path|rect|circle|ellipse|line|polyline|polygon|g|use|svg|layer|group|shape|_x?\d)[-_]?\d*$/i.test(id) || /^[a-z]+[-_]?\d+(?:[-_]\d+)*$/i.test(id);
  const nameOf = node => { const n = node.attrs['inkscape:label'] || node.attrs['data-name'] || node.attrs.id || ''; return String(n).replace(/_x([0-9A-F]{2,4})_/g, (m, h) => String.fromCharCode(parseInt(h, 16))).trim(); };
  async function readSvg(text, name) {
    const warn = [], parts = [], slice = slicer(8);
    const head = text.slice(0, 4000);
    const xml = parseXml(text);
    const all = [], ids = new Map();
    (function index(nd) { for (const c of nd.children) { all.push(c); if (c.attrs.id != null && !ids.has(c.attrs.id)) ids.set(c.attrs.id, c); index(c); } })(xml);
    const svg = all.find(nd => nd.local === 'svg');
    if (!svg) throw new Error(`${name}: not an SVG drawing (no <svg> in it)`);
    const illustrator = /Generator:\s*Adobe Illustrator/i.test(head) || svg.attrs['data-name'] != null || /ns\.adobe\.com/i.test(head);
    const pxPt = illustrator ? 1 : 0.75;
    // the root: its width and height in physical units, its viewBox in user units
    const vb = pointsOf(svg.attrs.viewBox).length >= 2 ? (String(svg.attrs.viewBox).match(/[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?/g) || []).map(Number).slice(0, 4) : null;
    const ptLen = v => { const m = /^\s*([-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?)\s*([a-z%]*)\s*$/i.exec(String(v == null ? '' : v)); if (!m) return null; const u = m[2].toLowerCase(); if (u === '%' || u === 'em' || u === 'ex') return null; return +m[1] * (u && u !== 'px' ? UNIT_PT[u] || pxPt : pxPt); };
    const physical = v => /\d\s*(mm|cm|in|pt|pc|q)\s*$/i.test(String(v || ''));
    let W = ptLen(svg.attrs.width), H = ptLen(svg.attrs.height), M;
    if (vb && vb.length === 4 && vb[2] > 0 && vb[3] > 0) {
      if (W && !H) H = W * vb[3] / vb[2]; else if (H && !W) W = H * vb[2] / vb[3];
      let sx = W ? W / vb[2] : pxPt, sy = H ? H / vb[3] : pxPt;
      if (!/none/i.test(svg.attrs.preserveAspectRatio || '')) { const s = /slice/i.test(svg.attrs.preserveAspectRatio || '') ? Math.max(sx, sy) : Math.min(sx, sy); sx = sy = s; }
      M = [sx, 0, 0, sy, -vb[0] * sx, -vb[1] * sy];
    } else M = [pxPt, 0, 0, pxPt, 0, 0];
    const units = physical(svg.attrs.width) || physical(svg.attrs.height) ? 'as the file states (' + String(svg.attrs.width || svg.attrs.height).replace(/[\d.\s-]+/g, '') + ')' : illustrator ? "Illustrator's points (72 to the inch)" : 'CSS pixels (96 to the inch)';
    if (!physical(svg.attrs.width) && !physical(svg.attrs.height)) warn.push(`no physical size in the file: read in ${units}`);
    const vw = vb ? vb[2] : (lenOf(svg.attrs.width) || 100), vh = vb ? vb[3] : (lenOf(svg.attrs.height) || 100), vd = Math.hypot(vw, vh) / Math.SQRT2;
    const rules = cssRules(all.filter(nd => nd.local === 'style').map(nd => nd.text).join('\n'));
    const ctxPaint = { byId: id => ids.get(id) || null };
    let texts = 0, images = 0, unpainted = 0, whites = 0, clips = 0;
    const style0 = { fill: 'black', stroke: 'none', 'stroke-width': '1', 'fill-rule': 'nonzero', color: 'black' };
    function styleOf(node, parent) {
      const s = {}; for (const k of INHERIT) if (parent[k] !== undefined) s[k] = parent[k];
      const set = (k, v) => { v = String(v).trim(); if (v === 'inherit') { if (parent[k] !== undefined) s[k] = parent[k]; } else s[k] = v; };
      for (const k of PRES) if (node.attrs[k] != null) set(k, node.attrs[k]);
      for (const r of rules) if (matches(r.sel, node)) for (const k of Object.keys(r.decl)) set(k, r.decl[k]);
      if (node.attrs.style) { const dd = parseDecl(node.attrs.style); for (const k of Object.keys(dd)) set(k, dd[k]); }
      return s;
    }
    function emit(node, subs, ctm, st, layer) {
      if (!subs.length) return;
      const fillOp = parseFloat(st['fill-opacity']), strokeOp = parseFloat(st['stroke-opacity']);
      let fill = fin(fillOp) && fillOp <= 0 ? null : parsePaint(st.fill, Object.assign({ color: st.color }, ctxPaint));
      const stroke = fin(strokeOp) && strokeOp <= 0 ? null : parsePaint(st.stroke, Object.assign({ color: st.color }, ctxPaint));
      const sw = lenOf(st['stroke-width'], vd);
      if (stroke && fin(sw) && sw <= 0) { if (!fill) { unpainted++; return; } }
      if (fill && white(fill) && !stroke) { whites++; return; }   // (white is no ink: a white shape is the artboard's background or a knock-out the laser cannot make)
      if (fill && white(fill)) fill = null;
      if (!fill && !stroke) { unpainted++; return; }
      const nm = nameOf(node);
      parts.push({ subpaths: mapSubs(subs, p => ap(ctm, p)), fill, stroke: stroke && !(fin(sw) && sw <= 0) ? stroke : null, lwPt: stroke ? (fin(sw) ? sw : 1) * scaleOf(ctm) : null,
        evenOdd: /evenodd/i.test(st['fill-rule'] || ''), layer: layer || null, name: nm && !autoId(nm) ? nm : (layer ? layer + ' · ' : '') + (SHAPE_NAME[node.local] || 'Path') + ' ' + (parts.length + 1) });
    }
    async function walk(node, ctm, parentStyle, layer, depth, viaUse) {
      for (const c of node.children) {
        await slice.step();
        const L = c.local;
        if (NO_DRAW.has(L) && !(viaUse && L === 'symbol')) continue;
        if (L === 'text' || L === 'textPath' || L === 'tspan') { texts++; continue; }
        if (L === 'image') { images++; continue; }
        const st = styleOf(c, parentStyle);
        if (/none/i.test(st.display || '') || /hidden|collapse/i.test(st.visibility || '') || parseFloat(st.opacity) === 0) continue;
        if (c.attrs['clip-path'] || c.attrs.mask) clips++;
        let M2 = c.attrs.transform ? mul(ctm, parseTransform(c.attrs.transform)) : ctm;
        const a = c.attrs, X = k => lenOf(a[k], vw) || 0, Y = k => lenOf(a[k], vh) || 0, R = k => lenOf(a[k], vd) || 0;
        if (L === 'g' || L === 'a' || L === 'switch' || L === 'symbol') {
          // a layer: a top-level group by its name; a deeper group only when its name says cut or engrave (Illustrator writes every group with an id)
          const nm = nameOf(c), isLayer = depth === 0 || a['inkscape:groupmode'] === 'layer' || (nm && (CUT_LAYER.test(nm) || ENGRAVE_LAYER.test(nm)));
          await walk(c, M2, st, isLayer && nm ? nm : layer, depth + 1, viaUse);
        } else if (L === 'svg') {
          const vb2 = (String(a.viewBox || '').match(/[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?/g) || []).map(Number), w = lenOf(a.width, vw), h = lenOf(a.height, vh);
          M2 = mul(M2, [1, 0, 0, 1, X('x'), Y('y')]);
          if (vb2.length === 4 && vb2[2] > 0 && vb2[3] > 0 && w && h) { const s = Math.min(w / vb2[2], h / vb2[3]); M2 = mul(M2, [s, 0, 0, s, -vb2[0] * s, -vb2[1] * s]); }
          await walk(c, M2, st, layer, depth + 1, viaUse);
        } else if (L === 'use') {
          const href = a.href || a['xlink:href'], ref = href && href[0] === '#' ? ids.get(href.slice(1)) : null;
          if (!ref || (viaUse || 0) > 6) continue;
          await walk({ children: [ref] }, mul(M2, [1, 0, 0, 1, X('x'), Y('y')]), st, layer, depth, (viaUse || 0) + 1);
        } else if (L === 'path') emit(c, pathSubs(a.d), M2, st, layer);
        else if (L === 'rect') { let rx = a.rx != null ? R('rx') : null, ry = a.ry != null ? R('ry') : null; if (rx == null) rx = ry || 0; if (ry == null) ry = rx; emit(c, rectSubs(X('x'), Y('y'), X('width'), Y('height'), rx, ry), M2, st, layer); }
        else if (L === 'circle') emit(c, ellipseSubs(X('cx'), Y('cy'), R('r'), R('r')), M2, st, layer);
        else if (L === 'ellipse') emit(c, ellipseSubs(X('cx'), Y('cy'), X('rx'), Y('ry')), M2, st, layer);
        else if (L === 'line') emit(c, [[['m', [X('x1'), Y('y1')]], ['l', [X('x2'), Y('y2')]]]], M2, Object.assign({}, st, { fill: 'none' }), layer);   // (a line has no area to fill)
        else if (L === 'polyline' || L === 'polygon') { const pts = pointsOf(a.points); if (pts.length >= 2) emit(c, [[['m', pts[0]]].concat(pts.slice(1).map(p => ['l', p]), L === 'polygon' ? [['h']] : [])], M2, st, layer); }
        else if (c.children && c.children.length) await walk(c, M2, st, layer, depth + 1, viaUse);   // (an unknown container: what it holds is still drawn)
      }
    }
    await walk(svg, M, styleOf(svg, style0), null, 0, 0);
    if (texts) warn.push(`${texts} live text object${texts > 1 ? 's' : ''} left out: outline the text first (Illustrator: Type › Create Outlines)`);
    if (images) warn.push(`${images} picture${images > 1 ? 's' : ''} left out: a picture is imported on its own and traced`);
    if (whites) warn.push(`${whites} white shape${whites > 1 ? 's' : ''} left out: white is no ink`);
    if (clips) warn.push(`${clips} clipping mask${clips > 1 ? 's' : ''} not applied: the parts are imported whole`);
    if (unpainted) warn.push(`${unpainted} part${unpainted > 1 ? 's' : ''} with no fill and no stroke left out`);
    return finish(parts, { name, type: 'svg', units, warnings: warn });
  }

  /* ═══ 6 · DXF ═══ */
  /** A DXF SPLINE as Béziers: the B-spline cut at every knot (Boehm's insertion until each knot is p-fold), each span then IS a Bézier of degree p: a cubic as it is, a
   *  quadratic raised exactly, a line a line. A rational span (weights that differ) or a degree over 3 is fitted span by span (Hermite cubics, split until within 2.5 × 10⁻⁵
   *  of the curve's size). A spline given only by fit points is drawn smooth through them. -> { start, cmds: [["L", p] | ["C", c1, c2, p]], closed } (charm-nest-dxf.js) */
  function splineBezier(e) {
    let deg = 3, flags = 0; const knots = [], weights = [], ctrl = [], fit = []; let cx = null, fx = null, t0 = null, t1 = null, tx = null, ux = null;
    for (const [c, v] of e.codes) {
      const f = parseFloat(v);
      if (c === 71) deg = f | 0; else if (c === 70) flags = f | 0;
      else if (c === 40) knots.push(f); else if (c === 41) weights.push(f);
      else if (c === 10) cx = f; else if (c === 20 && cx != null) { ctrl.push([cx, f]); cx = null; }
      else if (c === 11) fx = f; else if (c === 21 && fx != null) { fit.push([fx, f]); fx = null; }
      else if (c === 12) tx = f; else if (c === 22 && tx != null) { t0 = [tx, f]; tx = null; }
      else if (c === 13) ux = f; else if (c === 23 && ux != null) { t1 = [ux, f]; ux = null; }
    }
    const closed = (flags & 1) === 1;
    if (deg >= 1 && ctrl.length >= deg + 1 && knots.length === ctrl.length + deg + 1 && knots.every(fin) && ctrl.every(p => fin(p[0]) && fin(p[1]))) {
      const segs = bsplineBeziers(deg, ctrl, knots, weights.length === ctrl.length && weights.every(w => fin(w) && w > 0) ? weights : null);
      if (segs && segs.length) return { start: segs[0][0], cmds: segs.map(s => (s.length === 2 ? ['L', s[1]] : ['C', s[1], s[2], s[3]])), closed };
    }
    if (fit.length >= 2) return catmull(fit, closed, t0, t1);
    return null;
  }
  function bsplineBeziers(p, P, U, W) {
    let Q = P.map((q, i) => { const w = W ? W[i] : 1; return [q[0] * w, q[1] * w, w]; }), K = U.slice();
    const lo = K[p], hi = K[Q.length]; if (!(hi > lo)) return null;
    const eps = (hi - lo) * 1e-10, vals = [];
    for (let i = p; i <= Q.length; i++) if (!vals.length || K[i] - vals[vals.length - 1] > eps) vals.push(K[i]);
    for (const u of vals) {
      for (let guard = 0; guard <= p; guard++) {
        let s = 0; for (const x of K) if (Math.abs(x - u) <= eps) s++;
        if (s >= p) break;
        let k = -1; for (let i = 0; i < K.length - 1; i++) if (K[i] <= u + eps) k = i;
        if (k < p || k >= Q.length + p) break;
        const n = Q.length - 1, NQ = [];
        for (let i = 0; i <= n + 1; i++) {
          if (i <= k - p) NQ.push(Q[i]);
          else if (i <= k - s) { const den = K[i + p] - K[i], a = den ? (u - K[i]) / den : 0; NQ.push([a * Q[i][0] + (1 - a) * Q[i - 1][0], a * Q[i][1] + (1 - a) * Q[i - 1][1], a * Q[i][2] + (1 - a) * Q[i - 1][2]]); }
          else NQ.push(Q[i - 1]);
        }
        K.splice(k + 1, 0, u); Q = NQ;
      }
    }
    const out = [];
    for (let j = 0; j + 1 < vals.length; j++) {
      let k = -1; for (let i = 0; i < K.length; i++) if (Math.abs(K[i] - vals[j]) <= eps) k = i;
      if (k - p < 0 || k >= Q.length) return null;
      const H = Q.slice(k - p, k + 1), wsame = H.every(h => Math.abs(h[2] - H[0][2]) <= 1e-12 * Math.abs(H[0][2]));
      const pr = h => [h[0] / h[2], h[1] / h[2]];
      if (p === 1) out.push([pr(H[0]), pr(H[1])]);
      else if (wsame && p === 3) out.push(H.map(pr));
      else if (wsame && p === 2) { const a = pr(H[0]), q = pr(H[1]), b = pr(H[2]); out.push([a, [a[0] + 2 / 3 * (q[0] - a[0]), a[1] + 2 / 3 * (q[1] - a[1])], [b[0] + 2 / 3 * (q[0] - b[0]), b[1] + 2 / 3 * (q[1] - b[1])], b]); }
      else for (const c of fitRational(H)) out.push(c);
    }
    // consecutive spans share their end points exactly
    for (let j = 1; j < out.length; j++) out[j][0] = out[j - 1][out[j - 1].length - 1];
    return out;
  }
  /** A rational Bézier of any degree (homogeneous points) as cubics: Hermite pieces (the curve's own ends and tangents) split until each lies within tolerance. */
  function fitRational(H) {
    const p = H.length - 1;
    const bern = (n, t) => { const b = [1]; for (let j = 1; j <= n; j++) { for (let i = j; i > 0; i--) b[i] = (b[i] || 0) * (1 - t) + b[i - 1] * t; b[0] *= (1 - t); } return b; };
    const at = t => { const b = bern(p, t); let x = 0, y = 0, w = 0; for (let i = 0; i <= p; i++) { x += b[i] * H[i][0]; y += b[i] * H[i][1]; w += b[i] * H[i][2]; } return [x, y, w]; };
    const dAt = t => { const b = bern(p - 1, t); let x = 0, y = 0, w = 0; for (let i = 0; i < p; i++) { x += p * b[i] * (H[i + 1][0] - H[i][0]); y += p * b[i] * (H[i + 1][1] - H[i][1]); w += p * b[i] * (H[i + 1][2] - H[i][2]); } return [x, y, w]; };
    const C = t => { const a = at(t); return [a[0] / a[2], a[1] / a[2]]; };
    const D = t => { const a = at(t), d = dAt(t); return [(d[0] * a[2] - a[0] * d[2]) / (a[2] * a[2]), (d[1] * a[2] - a[1] * d[2]) / (a[2] * a[2])]; };
    let size = 0; { const pts = H.map(h => [h[0] / h[2], h[1] / h[2]]); const xs = pts.map(q => q[0]), ys = pts.map(q => q[1]); size = Math.hypot(Math.max(...xs) - Math.min(...xs), Math.max(...ys) - Math.min(...ys)); }
    const tol = Math.max(size * 2.5e-5, 1e-9), out = [];
    const bz = (c, s) => { const u = 1 - s; return [u * u * u * c[0][0] + 3 * u * u * s * c[1][0] + 3 * u * s * s * c[2][0] + s * s * s * c[3][0], u * u * u * c[0][1] + 3 * u * u * s * c[1][1] + 3 * u * s * s * c[2][1] + s * s * s * c[3][1]]; };
    (function piece(a, b, depth) {
      const h = (b - a) / 3, P0 = C(a), P3 = C(b), d0 = D(a), d1 = D(b);
      const c = [P0, [P0[0] + h * d0[0], P0[1] + h * d0[1]], [P3[0] - h * d1[0], P3[1] - h * d1[1]], P3];
      let err = 0; for (const s of [0.25, 0.5, 0.75]) { const q = C(a + (b - a) * s), r = bz(c, s); err = Math.max(err, Math.hypot(q[0] - r[0], q[1] - r[1])); }
      if (err > tol && depth < 12) { const m = (a + b) / 2; piece(a, m, depth + 1); piece(m, b, depth + 1); } else out.push(c);
    })(0, 1, 0);
    return out;
  }
  /** Smooth Béziers through fit points (Catmull-Rom; the end tangents the spline gives, when it gives them). */
  function catmull(pts, closed, t0, t1) {
    const n = pts.length, P = i => (closed ? pts[(i + n) % n] : pts[Math.max(0, Math.min(n - 1, i))]);
    const cmds = [], len = (a, b) => Math.hypot(b[0] - a[0], b[1] - a[1]);
    const segs = closed ? n : n - 1;
    for (let i = 0; i < segs; i++) {
      const a = P(i), b = P(i + 1), d = len(a, b);
      let ta = [(P(i + 1)[0] - P(i - 1)[0]) / 2, (P(i + 1)[1] - P(i - 1)[1]) / 2], tb = [(P(i + 2)[0] - P(i)[0]) / 2, (P(i + 2)[1] - P(i)[1]) / 2];
      if (!closed && i === 0 && t0) { const l = Math.hypot(t0[0], t0[1]) || 1; ta = [t0[0] / l * d, t0[1] / l * d]; }
      if (!closed && i === segs - 1 && t1) { const l = Math.hypot(t1[0], t1[1]) || 1; tb = [t1[0] / l * d, t1[1] / l * d]; }
      cmds.push(['C', [a[0] + ta[0] / 3, a[1] + ta[1] / 3], [b[0] - tb[0] / 3, b[1] - tb[1] / 3], b]);
    }
    return { start: pts[0], cmds, closed };
  }
  async function readDxf(text, name) {
    const D = DXF(); if (!D || !D.collect) throw new Error(`${name}: the DXF reader is not loaded on this page`);
    let docx; try { docx = D.parse(text); } catch (e) { throw new Error(`${name}: ${e.message}`); }
    const { items, skipped } = D.collect(docx, { spline: splineBezier });
    const warn = [];
    if (!items.length) throw new Error(`${name}: nothing drawn in it that can be cut${Object.keys(skipped).length ? ` (only ${Object.keys(skipped).join(', ')})` : ''}`);
    // units: the header's, else millimetres, else inches for a drawing too small to be a charm in millimetres (as charm-nest-dxf.js toPdf decides)
    let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
    for (const it of items) for (const s of it.subs) for (const p of [s.start].concat(s.cmds.flatMap(c => c.slice(1)))) { const q = ap([it.M[0], it.M[1], it.M[2], it.M[3], it.M[4], it.M[5]], p); if (q[0] < x0) x0 = q[0]; if (q[0] > x1) x1 = q[0]; if (q[1] < y0) y0 = q[1]; if (q[1] > y1) y1 = q[1]; }
    const ins = parseInt(docx.header.$INSUNITS, 10), span = Math.max(x1 - x0, y1 - y0), UN = D.UNITS || {};
    let [units, k] = UN[ins] || (span > 0 && span < 3 ? ['in', 72] : ['mm', PT]);
    if (!UN[ins]) { warn.push(`no units in the file: read as ${units === 'in' ? 'inches (it is too small to be millimetres)' : 'millimetres'}`); units += ' (assumed)'; }
    // points, y down; open strokes of one layer and colour joined where their ends meet (a hundredth of a millimetre), as the .dxf a custom order brings is read
    const at = (M, p) => { const q = ap(M, p); return [q[0] * k, -q[1] * k]; };
    const groups = new Map(), parts = [], slice = slicer(8);
    for (const it of items) {
      await slice.step();
      const subs = it.subs.map(s => ({ start: at(it.M, s.start), cmds: s.cmds.map(c => (c[0] === 'L' ? ['L', at(it.M, c[1])] : ['C', at(it.M, c[1]), at(it.M, c[2]), at(it.M, c[3])])), closed: !!s.closed }));
      if (it.fill) { parts.push({ subpaths: subs.map(toOps), fill: it.rgb, stroke: null, evenOdd: true, layer: it.layer }); continue; }
      const key = it.rgb.map(v => v.toFixed(3)).join(',') + '|' + it.layer;
      if (!groups.has(key)) groups.set(key, { rgb: it.rgb, layer: it.layer, subs: [] });
      groups.get(key).subs.push(...subs);
    }
    for (const g of groups.values()) {
      const joined = D.chain ? D.chain(g.subs, 0.01 * PT) : g.subs, open = joined.filter(s => !s.closed);
      for (const s of joined) if (s.closed || open.length <= MERGE_OPEN_OVER) parts.push({ subpaths: [toOps(s)], stroke: g.rgb, fill: null, lwPt: null, layer: g.layer });
      if (open.length > MERGE_OPEN_OVER) parts.push({ subpaths: open.map(toOps), stroke: g.rgb, fill: null, lwPt: null, layer: g.layer, name: `${g.layer} · ${open.length} lines` });
    }
    const sk = Object.entries(skipped).filter(([t]) => t !== 'hidden layer');
    if (sk.length) warn.push(`left out: ${sk.map(([t, n]) => `${n} ${t}`).join(', ')} (text and dimensions are not cut: outline text first)`);
    if (skipped['hidden layer']) warn.push(`${skipped['hidden layer']} part${skipped['hidden layer'] > 1 ? 's' : ''} on layers turned off left out`);
    return finish(parts, { name, type: 'dxf', units, warnings: warn });
  }
  const toOps = s => [['m', s.start]].concat(s.cmds.map(c => (c[0] === 'L' ? ['l', c[1]] : ['c', c[1], c[2], c[3]])), s.closed ? [['h']] : []);

  /* ═══ 7 · .ai / .pdf: the app's own reader ═══ */
  /** An outline the grouping joined from open strokes (charm-nest-pdf.js chainOpenStrokes: its parts in walk order): the parts' own subpaths joined end to end, curves kept
   *  (the grouping's own copy of it is a polyline, for geometry only). The parts' other subpaths (a closed hole drawn in one of them) are returned beside it. */
  function joinedOutline(o) {
    const subOf = s => { const list = s.subpaths || []; let best = 0, gap = -1; list.forEach((sp, i) => { if (sp.some(op => op[0] === 'h')) return; const a = sp[0][1], b = endOf(sp[sp.length - 1]); const g = Math.hypot(a[0] - b[0], a[1] - b[1]); if (g > gap) { gap = g; best = i; } }); return best; };
    const rev = sp => { const segs = sp.filter(op => op[0] !== 'h'), pts = [segs[0][1]]; for (let i = 1; i < segs.length; i++) pts.push(endOf(segs[i])); const out = [['m', pts[pts.length - 1]]]; for (let i = segs.length - 1; i >= 1; i--) out.push(segs[i][0] === 'c' ? ['c', segs[i][2], segs[i][1], pts[i - 1]] : ['l', pts[i - 1]]); return out; };
    const d = (a, b) => Math.hypot(a[0] - b[0], a[1] - b[1]);
    const runs = o.parts.map(s => s.subpaths[subOf(s)].filter(op => op[0] !== 'h')), extra = [];
    o.parts.forEach(s => (s.subpaths || []).forEach((sp, i) => { if (i !== subOf(s)) extra.push(sp); }));
    let line = runs[0].slice();
    if (runs.length > 1) { const nx = runs[1], e = endOf(line[line.length - 1]), s0 = line[0][1]; const toNext = Math.min(d(e, nx[0][1]), d(e, endOf(nx[nx.length - 1]))), fromStart = Math.min(d(s0, nx[0][1]), d(s0, endOf(nx[nx.length - 1]))); if (fromStart < toNext) line = rev(line); }
    for (let k = 1; k < runs.length; k++) {
      let r = runs[k]; const e = endOf(line[line.length - 1]);
      if (d(e, endOf(r[r.length - 1])) < d(e, r[0][1])) r = rev(r);
      if (d(e, r[0][1]) > 1e-6) line.push(['l', r[0][1]]);
      line.push(...r.slice(1));
    }
    line.push(['h']);
    return { subs: [line], extra };
  }
  async function readPdf(bytes, name) {
    const P = root && root.CharmNestPDF; if (!P || !P.parseSource) throw new Error(`${name}: the PDF reader is not loaded on this page`);
    const parsed = await P.parseSource(bytes, name);
    const g = await (P.groupCharmsAsync || P.groupCharms)(parsed, { minPt: 6 });
    const A = AE(), warn = [], parts = [], rot = ((+parsed.rotate || 0) % 360 + 360) % 360, slice = slicer(8);
    // PDF points, y up -> points, y down (the page turned as it is shown)
    const up = rot === 90 ? q => [q[1], -q[0]] : rot === 180 ? q => [-q[0], -q[1]] : rot === 270 ? q => [-q[1], q[0]] : q => q;
    const toItem = q => { const r = up(q); return [r[0], -r[1]]; };
    const paint = m => ({ stroke: m.stroke ? (m.strokeRGB || BLACK).slice(0, 3) : null, fill: m.fill ? (m.hatchBlue ? BLUE : (m.fillRGB || BLACK).slice(0, 3)) : null, lwPt: m.stroke ? m.lwPt || null : null,
      evenOdd: /\*$/.test(String(m.paintOp || '')), layer: m.layer || null });
    let texts = 0, pictures = 0;
    const counted = m => { if (m.kind === 'text' && !m.outlined) texts++; else if (m.kind === 'image' || m.kind === 'shading' || (m.kind === 'xobj' && !(m.children && m.children.length))) pictures++; };
    const add = (m, subs, role, charm, isOutline) => parts.push(Object.assign(paint(m), { subpaths: mapSubs(subs, toItem), role, charm, outline: !!isOutline }));
    const roleOf = (m, c) => (A && A.roleOf ? A.roleOf(m, c) : guessRole(Object.assign(paint(m), { closed: !!m.closed })));
    for (let ci = 0; ci < g.charms.length; ci++) {
      const c = g.charms[ci], chain = new Set((c.outline && c.outline.parts) || []);
      if (chain.size) { const j = joinedOutline(c.outline); add(c.outline, j.subs, 'cut', ci, true); if (j.extra.length) add(c.outline, j.extra, 'cut', ci, false); }
      for (const m of c.members) {
        await slice.step();
        if (!m || chain.has(m)) continue;
        if (m.kind !== 'path' || !Array.isArray(m.subpaths) || !(m.stroke || m.fill)) { counted(m); continue; }
        add(m, m.subpaths, m === c.outline ? 'cut' : roleOf(m, c), ci, m === c.outline);
      }
    }
    if (!g.charms.length) {
      // no outline: every drawn path on the page but the page's own frames
      const frames = new Set(g.frames || []);
      for (const m of parsed.segments.concat(parsed.nested || [])) {
        await slice.step();
        if (!m || frames.has(m) || m.outlined) continue;
        if (m.kind !== 'path' || !Array.isArray(m.subpaths) || !(m.stroke || m.fill)) { if (m.kind !== 'clip' && m.kind !== 'noop' && !(m.kind === 'xobj' && m.children && m.children.length)) counted(m); continue; }
        add(m, m.subpaths, roleOf(m, null), -1, false);
      }
      warn.push('no charm outline found in it: every drawn part is imported, roles guessed by colour');
    } else for (const m of g.orphans || []) { if (m && m.kind === 'path' && Array.isArray(m.subpaths) && (m.stroke || m.fill)) add(m, m.subpaths, roleOf(m, null), -1, false); else if (m) counted(m); }
    const beside = (g.markers || []).length + (g.sampleText || []).length + (g.stray || []).length;
    if (beside) warn.push(`${beside} note${beside > 1 ? 's' : ''} beside the charm left out (size marks, sample words, loose ink), as the library reads a master`);
    if (texts) warn.push(`${texts} live text object${texts > 1 ? 's' : ''} left out: outline the text first (Illustrator: Type › Create Outlines)`);
    if (pictures) warn.push(`${pictures} picture${pictures > 1 ? 's' : ''} left out: a picture is imported on its own and traced`);
    if (g.charms.length > 1) {
      // several charms: one outline each; the largest is THE outline of the file (a new design is that charm, with the rest beside it, unless one charm is chosen)
      const big = g.charms.reduce((a, b) => (area(b.outline.bbox) > area(a.outline.bbox) ? b : a));
      for (const p of parts) if (p.outline && p.charm !== g.charms.indexOf(big)) { p.outline = false; p.charmOutline = true; }
    }
    return finish(parts, { name, type: 'pdf', units: 'pt', warnings: warn, charms: g.charms.map((c, i) => ({ name: `Charm ${i + 1}` })) });
  }

  /* ═══ 8 · read: the file -> the result ═══ */
  const nameOfFile = f => String((f && f.name) || 'drawing');
  function kindOf(name, head) {
    const ext = (/\.([a-z0-9]+)$/i.exec(name) || [])[1];
    const e = ext ? ext.toLowerCase() : '';
    if (/^%PDF/.test(head) || /^%!PS-Adobe/.test(head)) return 'pdf';
    if (e === 'ai' || e === 'pdf') return 'pdf';
    if (e === 'svg' || /<svg[\s>]/i.test(head)) return 'svg';
    if (e === 'dxf' || /^\s*0\s*[\r\n]+\s*SECTION/.test(head)) return 'dxf';
    return null;
  }
  // (a Blob has bytes() and text() METHODS in a current browser: only data given as data is taken as it is)
  const given = v => v instanceof Uint8Array ? v : v instanceof ArrayBuffer ? new Uint8Array(v) : ArrayBuffer.isView(v) ? new Uint8Array(v.buffer, v.byteOffset, v.byteLength) : null;
  async function bytesOf(file) {
    if (!file) throw new Error('no file');
    if (given(file.bytes)) return given(file.bytes);
    if (typeof file.text === 'string') return null;
    if (typeof file.arrayBuffer === 'function') return new Uint8Array(await file.arrayBuffer());
    throw new Error('not a file');
  }
  const textOf = b => { try { return new TextDecoder('utf-8', { fatal: true }).decode(b); } catch (e) { return new TextDecoder('latin1').decode(b); } };
  /** The file read into the editor's items: see the header. */
  async function read(file) {
    const name = nameOfFile(file), size = file && (+file.size || (given(file.bytes) ? given(file.bytes).length : 0) || (typeof file.text === 'string' ? file.text.length : 0));
    if (size > MAX_BYTES) throw new Error(`${name}: ${Math.round(size / 1048576)} MB is too large to be one charm's drawing`);
    const bytes = await bytesOf(file), text0 = typeof file.text === 'string' ? file.text : null;
    const head = text0 != null ? text0.slice(0, 4096) : new TextDecoder('latin1').decode(bytes.subarray(0, 4096));
    const kind = kindOf(name, head.replace(/^﻿/, ''));
    if (!kind) throw new Error(`${name}: not a drawing this editor reads (.ai, .pdf, .svg or .dxf)`);
    const t0 = now();
    let r;
    if (kind === 'pdf') r = await readPdf(bytes || new TextEncoder().encode(text0), name);
    else if (kind === 'svg') r = await readSvg(text0 != null ? text0 : textOf(bytes), name);
    else r = await readDxf(text0 != null ? text0 : textOf(bytes), name);
    r.ms = Math.round(now() - t0);
    return r;
  }

  /** A FaceDoc of the design (the editor's open() contract): framed by the drawing, its outline the cut. */
  function faceDoc(r, face) {
    const items = r.items.map(it => (it.id === r.outlineId ? Object.assign({}, it, { role: 'cut', name: it.name && !/^outline/i.test(it.name) ? 'Outline · ' + it.name : 'Outline' }) : Object.assign({}, it)));
    return { face: face || 'front', unitPt: true, frame: { x0: 0, y0: 0, wPt: r6(r.widthMm * PT), hPt: r6(r.heightMm * PT) }, items };
  }
  /** The design-library shape of a new design made from a file (charm-nest-designs.js designPut): what "Open as a new design" hands CharmNestCeMasters. */
  function designOf(r) {
    return { name: String(r.name || 'Imported design').replace(/\.(ai|pdf|svg|dxf)$/i, ''), faces: { front: faceDoc(r, 'front') }, widthMm: +r.widthMm.toFixed(4), heightMm: +r.heightMm.toFixed(4),
      source: { kind: 'import', file: r.name, type: r.type }, outlineId: r.outlineId, warnings: r.warnings.slice() };
  }

  /* ═══ 9 · the role picker ═══ */
  const CSS = `
.cei-root{ --ink:#1c1d1d; --hair:#e6e4e0; --muted:#6d6a66; --gold:#a58a52; --gold-lt:#cdb98a; --hov:#f2eee4; --hov-line:#c6bfaf; --band:#faf9f7;
  --sans:'Nunito Sans','Helvetica Neue',Helvetica,Arial,sans-serif; --serif:'Cormorant','Hoefler Text',Georgia,serif;
  --c-cut:#1c1d1d; --c-line:#d4291f; --c-hatch:#1a5fc4; --c-art:#57534e;
  position:fixed; inset:0; z-index:2147482600; background:rgba(28,29,29,.30); display:flex; align-items:center; justify-content:center; padding:16px;
  font-family:var(--sans); font-size:13px; line-height:1.4; color:var(--ink); -webkit-font-smoothing:antialiased; text-align:left; }
.cei-root *,.cei-root *::before,.cei-root *::after{ box-sizing:border-box; }
.cei-root button,.cei-root select{ font-family:inherit; margin:0; border-radius:0; box-shadow:none; text-transform:none; letter-spacing:normal; min-width:0; min-height:0; }
.cei-card{ width:min(640px,100%); max-height:100%; overflow:auto; background:#fff; border:1px solid var(--ink); box-shadow:0 18px 44px rgba(28,29,29,.22); padding:20px 22px 16px; outline:none; }
.cei-eye{ font-size:10.5px; font-weight:700; letter-spacing:.22em; text-transform:uppercase; color:var(--gold); }
.cei-card h3{ margin:2px 0 4px; font-family:var(--serif); font-weight:500; font-size:22px; line-height:1.15; overflow-wrap:anywhere; }
.cei-size{ font-size:12.5px; color:var(--muted); }
.cei-size b{ color:var(--ink); font-weight:700; font-variant-numeric:tabular-nums; }
.cei-body{ display:grid; grid-template-columns:minmax(0,200px) minmax(0,1fr); gap:16px; margin:14px 0 10px; }
.cei-pre{ border:1px solid var(--hair); background:var(--band); aspect-ratio:1; display:flex; align-items:center; justify-content:center; min-height:0; }
.cei-pre svg{ width:100%; height:100%; display:block; }
.cei-p{ fill:none; stroke:none; vector-effect:non-scaling-stroke; stroke-width:1.1px; stroke-linejoin:round; transition:opacity .11s; }
.cei-p.is-fill.cei-p--cut{ fill:var(--c-cut); } .cei-p.is-stroke.cei-p--cut{ stroke:var(--c-cut); }
.cei-p.is-fill.cei-p--line{ fill:var(--c-line); } .cei-p.is-stroke.cei-p--line{ stroke:var(--c-line); }
.cei-p.is-fill.cei-p--hatch{ fill:var(--c-hatch); } .cei-p.is-stroke.cei-p--hatch{ stroke:var(--c-hatch); }
.cei-p.is-fill.cei-p--art{ fill:var(--c-art); } .cei-p.is-stroke.cei-p--art{ stroke:var(--c-art); }
.cei-p.is-evenodd{ fill-rule:evenodd; }
.cei-pre.is-hot .cei-p:not(.is-hot){ opacity:.18; }
.cei-h{ font-size:10.5px; font-weight:700; letter-spacing:.18em; text-transform:uppercase; color:var(--muted); margin:0 0 6px; }
.cei-rows{ display:flex; flex-direction:column; gap:6px; }
.cei-row{ display:grid; grid-template-columns:14px minmax(0,1fr) auto; align-items:center; gap:8px; padding:5px 6px; border:1px solid transparent; }
.cei-row:hover{ background:var(--hov); border-color:var(--hov-line); }
.cei-paint{ width:12px; height:12px; display:inline-block; border:1.5px solid transparent; }
.cei-lab{ min-width:0; font-size:12.5px; overflow-wrap:anywhere; }
.cei-lab em{ display:block; font-style:normal; font-size:11px; color:var(--muted); }
.cei-seg{ display:inline-flex; border:1px solid var(--hair); }
.cei-seg button{ display:inline-flex; align-items:center; gap:4px; border:0; border-left:1px solid var(--hair); background:#fff; color:var(--muted); font-size:11px; font-weight:700; padding:5px 7px; cursor:pointer; }
.cei-seg button:first-child{ border-left:0; }
.cei-seg button:hover{ background:var(--hov); color:var(--ink); }
.cei-seg button[aria-pressed="true"]{ background:var(--ink); color:#fff; }
.cei-sw{ width:9px; height:9px; flex:none; display:inline-block; }
.cei-sw--cut{ border:1.5px solid currentColor; } .cei-sw--line{ border:1.5px solid var(--c-line); } .cei-sw--hatch{ background:var(--c-hatch); } .cei-sw--art{ background:var(--c-art); }
.cei-seg button[aria-pressed="true"] .cei-sw--art{ background:#d6d3ce; }
.cei-pickc{ display:flex; align-items:center; gap:8px; margin:0 0 10px; font-size:12.5px; }
.cei-pickc select{ flex:1; min-width:0; border:1px solid var(--hair); background:#fff; padding:6px 8px; font-size:12.5px; }
.cei-note{ margin:4px 0 0; font-size:11.5px; line-height:1.5; color:var(--muted); }
.cei-note.is-warn{ color:#8a3a26; }
.cei-wait{ margin:18px 0 10px; height:2px; background:var(--hair); overflow:hidden; position:relative; }
.cei-wait::after{ content:''; position:absolute; top:0; bottom:0; width:35%; background:var(--gold); animation:cei-run 1s linear infinite; }
@keyframes cei-run{ from{ left:-35%; } to{ left:100%; } }
.cei-err{ margin:14px 0 8px; font-size:12.5px; color:#8a3a26; overflow-wrap:anywhere; }
.cei-foot{ display:flex; flex-wrap:wrap; justify-content:flex-end; gap:8px; margin-top:14px; }
.cei-btn{ display:inline-flex; align-items:center; justify-content:center; border:1px solid var(--ink); background:var(--ink); color:#fff; cursor:pointer; font-weight:700; font-size:11px;
  text-transform:uppercase !important; letter-spacing:.2em !important; padding:10px 16px; }
.cei-btn:hover{ background:#33342f; } .cei-btn:focus-visible{ outline:2px solid var(--gold-lt); outline-offset:2px; }
.cei-btn--ghost{ background:transparent; border-color:transparent; color:var(--muted); }
.cei-btn--ghost:hover{ background:var(--hov); border-color:var(--hov-line); color:var(--ink); }
.cei-btn--line{ background:#fff; color:var(--ink); } .cei-btn--line:hover{ background:var(--hov); }
.cei-btn--gold{ background:var(--gold); border-color:var(--gold); } .cei-btn--gold:hover{ background:#a2854b; border-color:#a2854b; }
@media (max-width:560px){ .cei-card{ padding:16px 14px 12px; } .cei-body{ grid-template-columns:minmax(0,1fr); } .cei-pre{ max-height:180px; aspect-ratio:auto; height:180px; }
  .cei-row{ grid-template-columns:14px minmax(0,1fr); } .cei-seg{ grid-column:1 / -1; justify-self:start; } .cei-foot .cei-btn{ flex:1 1 auto; } }
`;
  function injectCss() {
    if (!doc || doc.getElementById('ceiCss')) return;
    const s = doc.createElement('style'); s.id = 'ceiCss'; s.textContent = CSS; (doc.head || doc.documentElement).appendChild(s);
  }
  /** The groups the picker shows: the parts by layer, paint and the role they were given (one role per group). */
  function groupsOf(items) {
    const map = new Map();
    for (const it of items) {
      const paintKey = it.fill ? 'F' + colourWord(it.fill) : it.stroke ? 'S' + colourWord(it.stroke) + (+it.lwPt > HAIRLINE_MAX_PT ? 'w' : 'h') + (it.closed ? 'c' : 'o') : 'N';
      const key = (it.layer || '') + '|' + it.role + '|' + paintKey;
      let g = map.get(key);
      if (!g) {
        const paint = it.fill ? `${colourWord(it.fill)} fill` : it.stroke ? `${colourWord(it.stroke)} ${+it.lwPt > HAIRLINE_MAX_PT ? 'line ' + mm2(it.lwPt * MM) + ' mm' : 'hairline'}${it.closed ? ', closed' : ', open'}` : 'no paint';
        g = { key, role: it.role, guess: it.role, ids: [], label: (it.layer ? `Layer ${it.layer} · ` : '') + paint, fill: it.fill, stroke: it.stroke };
        map.set(key, g);
      }
      g.ids.push(it.id);
    }
    return [...map.values()].sort((a, b) => ROLES.indexOf(a.guess) - ROLES.indexOf(b.guess) || b.ids.length - a.ids.length);
  }
  /** The role picker: o = { name, read: () => Promise<result>, place: bool, newDesign: bool, faceName } -> Promise<{ mode, result } | null>. */
  function pick(o) {
    if (!doc || !doc.body) return Promise.reject(new Error('the role picker needs a page'));
    injectCss();
    return new Promise(resolve => {
      const prevFocus = doc.activeElement;
      const rootEl = doc.createElement('div'); rootEl.className = 'cei-root';
      rootEl.innerHTML = `<div class="cei-card" role="dialog" aria-modal="true" tabindex="-1" aria-label="Import ${esc(o.name)}"><div class="cei-eye">Import vector</div><h3>${esc(o.name)}</h3>
        <div class="cei-size">Reading the drawing…</div><div class="cei-wait" role="progressbar" aria-label="Reading"></div><div class="cei-foot"><button class="cei-btn cei-btn--ghost" type="button" data-a="cancel">Cancel</button></div></div>`;
      doc.body.appendChild(rootEl);
      const card = rootEl.querySelector('.cei-card');
      let done = false, R = null, view = null, groups = [], chosen = null;
      const finish = v => {
        if (done) return; done = true;
        rootEl.remove();
        try { if (prevFocus && prevFocus.focus) prevFocus.focus({ preventScroll: true }); } catch (e) { /* the page moved on */ }
        resolve(v);
      };
      const primary = () => (o.place ? 'place' : o.newDesign ? 'new' : null);
      // the keys: while the editor is open its own key handler sees every key first and hands one aimed at an element that carries __ceKey to it (charm-nest-charm-editor.js onKey)
      const onKey = e => {
        if (e.key === 'Escape') { e.preventDefault(); finish(null); return; }
        if (e.key === 'Enter' && !(e.target && /^(BUTTON|SELECT)$/.test(e.target.tagName)) && R && primary()) { e.preventDefault(); go(primary()); }
      };
      const arm = () => { card.__ceKey = onKey; rootEl.querySelectorAll('button,select').forEach(b => { b.__ceKey = onKey; }); };
      card.addEventListener('keydown', e => { e.stopPropagation(); onKey(e); });
      rootEl.addEventListener('pointerdown', e => { if (e.target === rootEl) { e.preventDefault(); card.focus({ preventScroll: true }); } });
      for (const t of ['dragover', 'drop']) rootEl.addEventListener(t, e => { e.preventDefault(); e.stopPropagation(); });   // (a second file let go over the picker is not opened by the browser in place of the app)
      rootEl.addEventListener('click', e => {
        const a = e.target.closest('[data-a]'); if (a) { if (a.dataset.a === 'cancel') finish(null); else go(a.dataset.a); return; }
        const rb = e.target.closest('[data-role]'), row = e.target.closest('[data-g]');
        if (rb && row) { const g = groups.find(x => x.key === row.dataset.g); if (g) { g.role = rb.dataset.role; paint(); } }
      });
      rootEl.addEventListener('change', e => { if (e.target.matches('[data-charm]')) { chosen = e.target.value; show(); } });
      rootEl.addEventListener('pointerover', e => { const row = e.target.closest('[data-g]'); hot(row ? row.dataset.g : null); });
      rootEl.addEventListener('pointerout', e => { if (!e.relatedTarget || !rootEl.contains(e.relatedTarget) || !e.relatedTarget.closest('[data-g]')) hot(null); });
      arm(); card.focus({ preventScroll: true });
      function hot(key) {
        const pre = rootEl.querySelector('.cei-pre'); if (!pre) return;
        const g = groups.find(x => x.key === key), ids = new Set(g ? g.ids : []);
        pre.classList.toggle('is-hot', !!g);
        pre.querySelectorAll('.cei-p').forEach(p => p.classList.toggle('is-hot', ids.has(p.dataset.id)));
      }
      const roleOfId = () => { const m = new Map(); for (const g of groups) for (const id of g.ids) m.set(id, g.role); return m; };
      function applied(mode) {
        const roles = roleOfId();
        const items = view.items.map(it => Object.assign({}, it, { role: mode === 'new' && it.id === view.outlineId ? 'cut' : roles.get(it.id) || it.role }));
        return Object.assign({}, view, { items });
      }
      function go(mode) {
        if (!R || !view || (mode === 'place' && !o.place) || (mode === 'new' && !o.newDesign)) return;
        finish({ mode, result: applied(mode) });
      }
      /** A part's class in the preview: painted as the editor paints its role (charm-nest-charm-editor.js paintFill / paintStroke). */
      function classOf(it, roles) {
        const M = model(), t = Object.assign({}, it, { role: roles.get(it.id) || it.role });
        const pf = M && M.paintFill ? M.paintFill(t) : t.role === 'hatch' || (t.role === 'art' && !!t.fill), ps = M && M.paintStroke ? M.paintStroke(t) : t.role === 'cut' || t.role === 'line' || (t.role === 'art' && !!t.stroke);
        return `cei-p cei-p--${t.role}${pf ? ' is-fill' : ''}${ps ? ' is-stroke' : ''}${t.evenOdd ? ' is-evenodd' : ''}`;
      }
      // the preview drawn part by part in slices of 8 ms (a sheet of 132 charms is 33 000 path commands: one string of it held the page for 100 ms)
      let fillToken = 0;
      async function fillPreview(svg) {
        const my = ++fillToken, sl = slicer(8), f = v => Math.round(v * 100) / 100;
        let roles = roleOfId();
        for (const it of view.items) {
          if (await sl.step()) roles = roleOfId();   // (a role pressed meanwhile)
          if (my !== fillToken || done) return;
          let d = '';
          for (const sub of it.subpaths) for (const op of sub) {
            const k = op[0];
            if (k === 'c') d += 'C' + f(op[1][0]) + ' ' + f(op[1][1]) + ' ' + f(op[2][0]) + ' ' + f(op[2][1]) + ' ' + f(op[3][0]) + ' ' + f(op[3][1]);
            else if (k === 'h') d += 'Z'; else d += (k === 'm' ? 'M' : 'L') + f(op[1][0]) + ' ' + f(op[1][1]);
          }
          const el = doc.createElementNS('http://www.w3.org/2000/svg', 'path');
          el.setAttribute('d', d); el.setAttribute('data-id', it.id); el.setAttribute('class', classOf(it, roles));
          svg.appendChild(el);
        }
      }
      function paint() {
        const roles = roleOfId(), byId = new Map(view.items.map(it => [it.id, it]));
        rootEl.querySelectorAll('.cei-p').forEach(el => { const it = byId.get(el.dataset.id); if (it) el.setAttribute('class', classOf(it, roles)); });
        rootEl.querySelectorAll('[data-g]').forEach(row => { const g = groups.find(x => x.key === row.dataset.g); row.querySelectorAll('[data-role]').forEach(b => b.setAttribute('aria-pressed', String(!!g && b.dataset.role === g.role))); });
      }
      function show() {
        const keepRoles = new Map(groups.map(g => [g.key, g.role]));
        view = chosen && chosen !== 'all' && R.charms ? subset(R, (R.charms[+chosen] || {}).ids) : R;
        groups = groupsOf(view.items); for (const g of groups) if (keepRoles.has(g.key)) g.role = keepRoles.get(g.key);
        const wPt = view.widthMm * PT, hPt = view.heightMm * PT, pad = Math.max(wPt, hPt) * 0.04 + 0.5;
        const outline = view.items.find(it => it.id === view.outlineId), ob = outline ? tightBox(outline.subpaths) : null;
        const rows = groups.map(g => `<div class="cei-row" data-g="${esc(g.key)}"><i class="cei-paint" style="${g.fill ? `background:${cssRgb(g.fill)}` : ''}${g.stroke ? `;border-color:${cssRgb(g.stroke)}` : ''}${g.fill && white(g.fill) ? ';border-color:#ccc' : ''}"></i>`
          + `<span class="cei-lab">${esc(g.label)}<em>${g.ids.length} part${g.ids.length > 1 ? 's' : ''}${g.ids.includes(view.outlineId) ? ' · the outline' : ''}</em></span>`
          + `<span class="cei-seg" role="group" aria-label="What the laser does">${ROLES.map(r => `<button type="button" data-role="${r}" title="${esc(ROLE_SAYS[r])}" aria-pressed="false"><i class="cei-sw cei-sw--${r}"></i>${ROLE_NAME[r]}</button>`).join('')}</span></div>`).join('');
        const charmPick = R.charms && R.charms.length > 1 ? `<label class="cei-pickc">Charm <select data-charm><option value="all"${!chosen || chosen === 'all' ? ' selected' : ''}>All ${R.charms.length} charms · ${mm2(R.widthMm)} × ${mm2(R.heightMm)} mm</option>`
          + R.charms.map((c, k) => `<option value="${k}"${String(k) === chosen ? ' selected' : ''}>${esc(c.name)} · ${mm2(c.widthMm)} × ${mm2(c.heightMm)} mm</option>`).join('') + '</select></label>' : '';
        const notes = [];
        if (o.newDesign) notes.push(outline ? `Open as a new design: the outline (${mm2((ob[2] - ob[0]) * MM)} × ${mm2((ob[3] - ob[1]) * MM)} mm) is its cut line.` : 'Open as a new design: no outline was found, so it opens with no cut line until a part is set to Cut.');
        if (o.place) notes.push(`Place into this charm: one group on the ${esc(o.faceName || 'face')}, centred, at its exact size.`);
        const warns = (view.warnings || []).slice(0, 4).map(w => `<p class="cei-note is-warn">${esc(w[0].toUpperCase() + w.slice(1))}</p>`).join('');
        card.innerHTML = `<div class="cei-eye">Import vector · ${esc(String(view.type || '').toUpperCase() === 'PDF' ? (/\.ai$/i.test(view.name) ? 'AI' : 'PDF') : String(view.type || '').toUpperCase())}</div><h3>${esc(view.name)}</h3>`
          + `<div class="cei-size"><b data-size>${mm2(view.widthMm)} × ${mm2(view.heightMm)} mm</b> · exact size · ${view.items.length} part${view.items.length > 1 ? 's' : ''}</div>`
          + `<div class="cei-body"><div class="cei-pre" aria-hidden="true"><svg viewBox="${-pad} ${-pad} ${wPt + 2 * pad} ${hPt + 2 * pad}" preserveAspectRatio="xMidYMid meet"></svg></div>`
          + `<div>${charmPick}<p class="cei-h">What the laser does</p><div class="cei-rows">${rows}</div>${notes.map(t => `<p class="cei-note">${t}</p>`).join('')}${warns}</div></div>`
          + `<div class="cei-foot"><button class="cei-btn cei-btn--ghost" type="button" data-a="cancel">Cancel</button>`
          + (o.newDesign ? `<button class="cei-btn ${o.place ? 'cei-btn--line' : 'cei-btn--gold'}" type="button" data-a="new">Open as a new design</button>` : '')
          + (o.place ? `<button class="cei-btn cei-btn--gold" type="button" data-a="place">Place into this charm</button>` : '') + `</div>`;
        paint(); arm(); fillPreview(card.querySelector('.cei-pre svg'));
        const first = card.querySelector(`[data-a="${primary() || 'cancel'}"]`); try { (first || card).focus({ preventScroll: true }); } catch (e) { /* fine */ }
      }
      Promise.resolve().then(() => o.read()).then(r => { if (done) return; R = r; show(); }, err => {
        if (done) return;
        card.innerHTML = `<div class="cei-eye">Import vector</div><h3>${esc(o.name)}</h3><p class="cei-err" role="alert">${esc((err && err.message) || err)}</p>`
          + `<div class="cei-foot"><button class="cei-btn" type="button" data-a="cancel">Close</button></div>`;
        arm(); const b = card.querySelector('[data-a]'); try { b.focus({ preventScroll: true }); } catch (e) { /* fine */ }
      });
    });
  }

  /* ═══ 10 · the importer: read, pick, place or open ═══ */
  const ACCEPT = ['.ai', '.pdf', '.svg', '.dxf', 'image/svg+xml', 'application/pdf', 'application/illustrator', 'application/dxf', 'image/vnd.dxf', 'image/x-dxf'];
  const masters = () => { const M = root && root.CharmNestCeMasters; return M && (typeof M.openNew === 'function' || typeof M.openDesign === 'function') ? M : null; };
  const log = (kind, data) => { try { if (root.CNDesignLog && typeof root.CNDesignLog.event === 'function') root.CNDesignLog.event(kind, data); } catch (e) { /* the log never stops the work */ } };
  const whoIs = () => { try { return typeof root.employeeName === 'function' ? String(root.employeeName() || '') : ''; } catch (e) { return ''; } };
  /** The design as one group in the open face: centred on the charm's frame (or where the file was dropped: `at`, pt), every point 1:1 (its exact size), one undo step. */
  function place(ctx, r, at) {
    const cur = root.CharmEditor && root.CharmEditor.current;
    const fr = (ctx && typeof ctx.frame === 'function' && ctx.frame()) || (cur && cur.frame && cur.frame()) || { x0: 0, y0: 0, wPt: r.widthMm * PT, hPt: r.heightMm * PT };
    const c = Array.isArray(at) && fin(at[0]) && fin(at[1]) ? at : [fr.x0 + fr.wPt / 2, fr.y0 + fr.hPt / 2];
    const dx = c[0] - r.widthMm * PT / 2, dy = c[1] - r.heightMm * PT / 2;
    const kids = r.items.map(it => { const o = Object.assign({}, it, { id: newId(), subpaths: mapSubs(it.subpaths, q => [r6(q[0] + dx), r6(q[1] + dy)]) }); delete o.closed; return o; });
    const add = kids.length > 1 ? [{ id: newId(), kind: 'group', role: 'art', source: 'added', name: r.name, imported: r.name, children: kids, tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 }] : kids.map(k => Object.assign(k, { name: r.name }));
    ctx.addItems(add, { select: true, label: 'Import ' + r.name });
    if (typeof ctx.commit === 'function') ctx.commit('Import ' + r.name);
    if (typeof ctx.say === 'function') ctx.say(`${r.name} placed · ${mm2(r.widthMm)} × ${mm2(r.heightMm)} mm, its exact size`);
    return add[0];
  }
  /** CharmEditor.importers[].run: the file read, the roles picked, then placed in this charm (where it was dropped: info.at) or opened as a new design. */
  async function run(ctx, file, info) {
    const name = nameOfFile(file), cur = root.CharmEditor && root.CharmEditor.current;
    const canPlace = !!(ctx && typeof ctx.addItems === 'function'), M = masters();
    const faceName = (ctx && typeof ctx.face === 'function' && ctx.face()) || (cur && cur.face && cur.face()) || 'face';
    const got = await pick({ name, read: () => read(file), place: canPlace, newDesign: !!M, faceName: typeof faceName === 'string' ? faceName : (faceName && faceName.face) || 'face' });
    if (!got) return null;
    const r = got.result, roles = {}; for (const it of r.items) roles[it.role] = (roles[it.role] || 0) + 1;
    log('vectorImport', { file: name, type: r.type, mode: got.mode, via: (info && info.via) || null, widthMm: +r.widthMm.toFixed(3), heightMm: +r.heightMm.toFixed(3), parts: r.items.length, roles, by: whoIs() || undefined });
    if (got.mode === 'place') return place(ctx, r, info && info.at);
    // a new design: CharmNestCeMasters closes this editor through its own Cancel and keeps the design open until it is saved, so it is handed over once this import's step has ended
    const d = designOf(r);
    setTimeout(() => {
      Promise.resolve().then(() => (typeof M.openNew === 'function' ? M.openNew(d, { from: ctx || null, read: r }) : M.openDesign(d, {})))
        .catch(e => { try { root.console && root.console.error('vector import: open as a new design', e); } catch (_) { /* fine */ } });
    }, 0);
    return d;
  }
  const importer = { id: 'vector', title: 'Vector drawing (.ai, .pdf, .svg, .dxf)', accept: ACCEPT, run };
  // (first in the list: an .svg is image/svg+xml, and the picture importer takes image/*; the vector reader is the one that keeps it a drawing)
  if (root && root.CharmEditor && doc) {
    const CEd = root.CharmEditor, list = CEd.importers || (CEd.importers = []);
    if (!list.some(x => x && x.id === importer.id)) list.unshift(importer);
  }

  return { read, pick, run, place, faceDoc, designOf, subset, guessRole, importer, ACCEPT, readers: { svg: readSvg, dxf: readDxf, pdf: readPdf }, splineBezier, pathSubs, parseXml, version: 1 };
});
