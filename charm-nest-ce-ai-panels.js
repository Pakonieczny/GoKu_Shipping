/* charm-nest-ce-ai-panels.js — THE CHARM EDITOR'S RIGHT DOCK, MADE AS ADOBE ILLUSTRATOR'S PANEL DOCK (AIDOCK; Paul, 28 Oct 2026: "I need you to update the UI for the Entire
 * Charm Editor, all context menus, layer area etc.. The idea is that my employees know how to operate Adobe illustrator very well ... My expectation is that you basically
 * replicate the UI for illustrator while also keeping some of the specific functions that are dedicated to our platform available ... I'm not looking for you to change the
 * functionality of things as they appear to run really well this is more about the front facing user facing UI and interface").
 *
 * This file builds the dock only (the menu bar, tools, control bar, context menus, shortcuts and the theme are the shell's). It plugs into charm-nest-charm-editor.js as
 * CharmEditor.aiDock: the workspace calls mount(D) once its chrome is wired and hands over its own calls in D; every control here runs one of them, or an existing command
 * (CharmEditor.commands: charm-nest-ce-ops.js, -ce-extras.js, -ce-image.js) or the editor's call behind one of its data-* hooks (data-act, data-role, data-font, data-case,
 * data-opt ...: under data-aim here, so the page's own controls stay the only ones answering to those), so nothing new is done to the work. Without this file the dock is the editor's own.
 *
 *   THE DOCK        Illustrator's: an expanded column of panel groups (tabs; the ☰ panel menu at the right of each tab bar; a double-click on a tab minimises the
 *                   group; a tab dragged to another group, to the icon strip or to the column moves the panel, remembered in localStorage 'ce.aidock'), an icon strip of
 *                   collapsed panels beside it (a click pops the panel out to the left), » at the top collapses the whole dock to the icon-and-label strip, « expands it.
 *                   Default: Properties over Layers, with Transform | Align | Pathfinder, Character, Stroke | Swatches and Image Trace as icons; the editor's extension
 *                   panels (CharmEditor.panels: the Library) stay in their own column (.ce-xpanel, its tabs drawn as these) and have an icon in the strip.
 *                   A narrow window (≤ 1180 px) collapses the dock to icons on its own; a phone-width one (≤ 900 px) keeps the editor's drawer, panels as flyouts.
 *   LAYERS          Illustrator's rows: the visibility column (⌥-click: the others hidden or shown; ⌘-click: Outline view, the editor's ⌘Y), the lock column (⌥-click: the
 *                   others), the layer colour (our ROLE colour: cut, line, hatch, artwork) as the bar at the row's left and as the selection square, the disclosure triangle
 *                   (⌥-click: everything inside), a thumbnail, the name (double-click: rename; a double-click elsewhere on the row: Options), our badges ("kept: on a
 *                   sheet", added here), the target circle (a double ring when targeted, filled when the part carries its own weight, letter spacing, warp or path) and the selection
 *                   square (a click selects; dragged onto another row it moves the selection into that layer). Rows drag to reorder with an insertion line, and into a
 *                   group over its row's middle. Bottom bar: "N Layers", Locate Object, Make / Release Clipping Mask, Release to Layers (ungroup), Collect in New Layer
 *                   (group), Delete. The panel menu: Duplicate, Delete, Options for …, Make / Release Clipping Mask, Locate Object, Collect in New Layer, Release to
 *                   Layers, Hide Others / Show All Layers, Outline view, Lock Others / Unlock All Layers, Panel Options (row size, thumbnails).
 *   PROPERTIES      context-sensitive as Illustrator's: Transform (reference point, X Y W H, the link, the angle, flip), Appearance (Fill and Stroke as our roles, the line
 *                   width, the weight), Character and Paragraph for words, Image Trace for a traced picture, Pathfinder for two or more, Align, Quick Actions (only the
 *                   commands this page has, only when they can run); with nothing selected, the Document: the artboard (Front / Back), units, rulers and grids, guides,
 *                   snapping, Fit Artboard, Unlock All, Show All, the laser check.
 *   TRANSFORM, ALIGN (Align To: selection, key object, artboard = the charm, the charm's centre of area), PATHFINDER, STROKE, SWATCHES (our engraving colours), CHARACTER
 *   (the CETEXT fonts, style, size, leading, tracking, case, alignment), IMAGE TRACE (the traced picture's settings; Trace… opens the Image step: Re-trace image)
 *
 * The theme is the shell's: every colour, the font and the row height are its --ai-* tokens on .ce-root, with Illustrator's Medium Dark as the fallback.
 * Hooks for the shell and other files: CharmEditor.aiDock.add({ id, title, icon, key, group, mount(ctx, el), update(ctx), unmount() }) (a panel of another file as a tab of this
 * dock; see add below), CharmEditor.aiDock.openPanel(id) (the shell's Window menu), .panels (the ids, titles and keys), D.S.aiRef (the reference point, [fx, fy]). If the shell's command
 * catalog (CharmEditor.ai.catalog) is there, its labels and keys are used for the panel menus' shortcuts (catalogKey); nothing waits for it.
 *
 * UMD: window.CharmEditorAiPanels and CharmEditor.aiDock in the page; module.exports in node (the pure parts: the layout's clean-up and the row model, for tests). */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api;
  else if (root) { root.CharmEditorAiPanels = api; const CE = root.CharmEditor || (root.CharmEditor = {}); CE.aiDock = api; }
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  // (one reference point for the whole editor: the Control bar's locator (the shell's S.ref, kept in 'ce.view') and the Transform panel's are the same)
  const refOf = P => (P && P.D && P.D.S && Array.isArray(P.D.S.ref) ? P.D.S.ref : (P && P.prefs && P.prefs.ref) || [0, 0]);
  function setRef(P, r) { if (P.prefs) P.prefs.ref = r.slice(); if (P.D && P.D.S) { P.D.S.ref = r.slice(); P.D.S.aiRef = r.slice(); try { const o = JSON.parse((localStorage.getItem('ce.view')) || '{}') || {}; o.ref = r.slice(); localStorage.setItem('ce.view', JSON.stringify(o)); } catch (_) { /* storage blocked */ } } }
  const doc = root && root.document ? root.document : null;
  const MM = 25.4 / 72;
  const fin = v => typeof v === 'number' && Number.isFinite(v);
  const num = (v, d) => (fin(v) ? v : d);
  const trim = (v, dp) => String(+(+v).toFixed(dp));
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const numIn = v => { const x = parseFloat(String(v == null ? '' : v).replace(',', '.').replace(/−/g, '-')); return fin(x) ? x : NaN; };
  const LS_LAYOUT = 'ce.aidock', LS_PREFS = 'ce.aidock.prefs';
  const lsGet = k => { try { return root.localStorage ? root.localStorage.getItem(k) : null; } catch (e) { return null; } };
  const lsSet = (k, v) => { try { if (root.localStorage) root.localStorage.setItem(k, v); } catch (e) { /* storage blocked: the layout lasts this session */ } };
  const NARROW = 1180, DRAWER = 900;

  /* ── the panels: ids, Illustrator's names and Window-menu keys (shown in the tooltips; the keys themselves are the shell's) ── */
  const ALL = ['properties', 'layers', 'transform', 'align', 'pathfinder', 'stroke', 'swatches', 'character', 'trace'];
  const TITLE = { properties: 'Properties', layers: 'Layers', transform: 'Transform', align: 'Align', pathfinder: 'Pathfinder', stroke: 'Stroke', swatches: 'Swatches', character: 'Character', trace: 'Image Trace' };
  const WKEY = { layers: 'F7', transform: '⇧F8', align: '⇧F7', pathfinder: '⇧⌘F9', stroke: '⌘F10', character: '⌘T', swatches: '', properties: '', trace: '' };
  const DEF_LAYOUT = () => ({ v: 1, collapsed: false,
    main: [{ tabs: ['properties'], active: 'properties', min: false }, { tabs: ['layers'], active: 'layers', min: false }],
    strip: [{ tabs: ['transform', 'align', 'pathfinder'], active: 'transform' }, { tabs: ['character'], active: 'character' }, { tabs: ['stroke', 'swatches'], active: 'stroke' }, { tabs: ['trace'], active: 'trace' }] });
  /* the panels other files add (CharmEditor.aiDock.add): id -> { id, title, icon, key, group, mount(ctx, el), update?(ctx), unmount?() } */
  const EXT = new Map();
  /** A layout read back (or dropped on the floor): every panel exactly once, the groups that are left; an added panel missing goes where it asked (group: a panel's id: its
   *  tabs; 'main': a group of its own in the column; else an icon of its own), anything else missing as an icon group of its own. */
  function cleanLayout(o) {
    if (!o || o.v !== 1 || !Array.isArray(o.main) || !Array.isArray(o.strip)) o = DEF_LAYOUT();
    const seen = new Set();
    const grp = (g, main) => {
      const tabs = (g && Array.isArray(g.tabs) ? g.tabs : []).map(String).filter(id => ALL.includes(id) && !seen.has(id) && !!seen.add(id));
      if (!tabs.length) return null;
      const out = { tabs, active: tabs.includes(g.active) ? g.active : tabs[0] }; if (main) out.min = !!g.min; return out;
    };
    const L = { v: 1, collapsed: !!o.collapsed, main: o.main.map(g => grp(g, true)).filter(Boolean), strip: o.strip.map(g => grp(g, false)).filter(Boolean) };
    const miss = ALL.filter(id => !seen.has(id)), loose = [];
    for (const id of miss) {
      const d = EXT.get(id), want = d && d.group ? String(d.group) : '';
      const host = want && want !== 'main' ? L.main.concat(L.strip).find(g => g.tabs.includes(want)) : null;
      if (host) host.tabs.push(id); else if (want === 'main') L.main.push({ tabs: [id], active: id, min: false }); else if (d) L.strip.push({ tabs: [id], active: id }); else loose.push(id);
    }
    if (loose.length) L.strip.push({ tabs: loose, active: loose[0] });
    return L;
  }
  const DEF_PREFS = () => ({ rowSize: 'medium', thumbs: true, swList: false, alignTo: null, gap: '', ref: [0, 0] });
  function cleanPrefs(o) {
    const p = DEF_PREFS(); if (!o || typeof o !== 'object') return p;
    if (['small', 'medium', 'large'].includes(o.rowSize)) p.rowSize = o.rowSize;
    if (typeof o.thumbs === 'boolean') p.thumbs = o.thumbs; if (typeof o.swList === 'boolean') p.swList = o.swList;
    if (['selection', 'key', 'charm', 'centre'].includes(o.alignTo)) p.alignTo = o.alignTo;
    if (Array.isArray(o.ref) && o.ref.length === 2 && o.ref.every(v => [0, 0.5, 1].includes(v))) p.ref = o.ref.slice();
    return p;
  }
  const readJson = k => { try { return JSON.parse(lsGet(k) || 'null'); } catch (e) { return null; } };

  /* ── icons: drawn here in Illustrator's manner (an 18-unit box, hairline strokes, solid where Illustrator is solid); no Adobe artwork ── */
  const S18 = (d, x) => `<svg viewBox="0 0 18 18" fill="none" stroke="currentColor" stroke-width="1.1" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"${x || ''}>${d}</svg>`;
  const F = 'fill="currentColor" stroke="none"';
  const SQA = 'M2.5 2.5h8.5v8.5H2.5z', SQB = 'M7 7h8.5v8.5H7z';
  const IC = {
    eye: S18(`<path d="M1.6 9C3.3 6.1 5.9 4.6 9 4.6s5.7 1.5 7.4 4.4C14.7 11.9 12.1 13.4 9 13.4S3.3 11.9 1.6 9Z"/><circle cx="9" cy="9" r="2.3" ${F}/>`),
    lock: S18(`<rect x="4.6" y="8" width="8.8" height="6.6" rx=".8" ${F}/><path d="M6.6 8V6a2.4 2.4 0 0 1 4.8 0v2"/>`),
    triR: `<svg viewBox="0 0 10 10" aria-hidden="true"><path d="M3 1.8 7.4 5 3 8.2Z" fill="currentColor"/></svg>`,
    triD: `<svg viewBox="0 0 10 10" aria-hidden="true"><path d="M1.8 3h6.4L5 7.4Z" fill="currentColor"/></svg>`,
    menu: S18('<path d="M3.5 5h11M3.5 9h11M3.5 13h11"/>'),
    collapse: S18('<path d="m4.5 5 4 4-4 4M9.5 5l4 4-4 4"/>'),
    expand: S18('<path d="m8.5 5-4 4 4 4M13.5 5l-4 4 4 4"/>'),
    locate: S18('<circle cx="7.8" cy="7.8" r="4.3"/><path d="m11 11 3.8 3.8"/>'),
    clip: S18(`<rect x="2.8" y="2.8" width="8.4" height="8.4"/><circle cx="11.2" cy="11.2" r="4.2" fill="currentColor" fill-opacity=".35"/>`),
    collect: S18('<path d="M2.5 5h4l1.2 1.5h7.8v8h-13Z"/><path d="M9 8.6v4M7 10.6h4"/>'),
    release: S18('<path d="M2.5 5h4l1.2 1.5h7.8v8h-13Z"/><path d="M7 10.6h4"/><path d="M12.5 2.5 15 5" stroke-width=".9"/>'),
    trash: S18('<path d="M3.5 5h11M7 5V3.4h4V5M5 5l.7 9.6h6.6L13 5M7.6 7.5v4.8M10.4 7.5v4.8"/>'),
    link: S18('<path d="M7.6 10.4 10.4 7.6"/><path d="m8.6 5.4 1.3-1.3a2.6 2.6 0 0 1 3.7 3.7l-1.3 1.3"/><path d="m9.4 12.6-1.3 1.3a2.6 2.6 0 0 1-3.7-3.7l1.3-1.3"/>'),
    angle: S18('<path d="M3.5 14h11M3.5 14 12 4.5"/><path d="M8.6 14a5 5 0 0 0-1.4-3.5"/>'),
    flipH: S18(`<path d="M9 2.5v13" stroke-dasharray="1.6 1.6"/><path d="M7 5 2.5 13H7Z"/><path d="M11 5l4.5 8H11Z" fill="currentColor"/>`),
    flipV: S18(`<path d="M2.5 9h13" stroke-dasharray="1.6 1.6"/><path d="M5 7 13 2.5V7Z"/><path d="M5 11l8 4.5V11Z" fill="currentColor"/>`),
    caret: `<svg class="aip-caret" viewBox="0 0 8 8" aria-hidden="true"><path d="M1.2 2.6h5.6L4 5.8Z" fill="currentColor"/></svg>`,
    more: `<svg viewBox="0 0 18 18" aria-hidden="true"><circle cx="4.5" cy="9" r="1.15" fill="currentColor"/><circle cx="9" cy="9" r="1.15" fill="currentColor"/><circle cx="13.5" cy="9" r="1.15" fill="currentColor"/></svg>`,
    pin: S18(`<path d="M9 2.6a3.4 3.4 0 0 1 3.4 3.4c0 2.6-3.4 6.4-3.4 6.4S5.6 8.6 5.6 6A3.4 3.4 0 0 1 9 2.6Z"/><circle cx="9" cy="6" r="1.1" ${F}/><path d="M5 15.4h8"/>`),
    // the panels' own icons (the icon strip)
    properties: S18(`<path d="M3 4.5h12M3 9h12M3 13.5h12"/><circle cx="6" cy="4.5" r="1.6" ${F}/><circle cx="12" cy="9" r="1.6" ${F}/><circle cx="8" cy="13.5" r="1.6" ${F}/>`),
    layers: S18('<path d="m9 2.8 6.4 3.2L9 9.2 2.6 6Z"/><path d="m2.6 9 6.4 3.2L15.4 9"/><path d="m2.6 12 6.4 3.2 6.4-3.2"/>'),
    transform: S18(`<rect x="4" y="4" width="10" height="10" stroke-dasharray="2 1.6"/><rect x="2.6" y="2.6" width="2.8" height="2.8" ${F}/><rect x="12.6" y="12.6" width="2.8" height="2.8" ${F}/><rect x="12.6" y="2.6" width="2.8" height="2.8"/><rect x="2.6" y="12.6" width="2.8" height="2.8"/>`),
    align: S18(`<path d="M2.8 2.5v13"/><rect x="5" y="4.5" width="9" height="3.4" ${F}/><rect x="5" y="10.1" width="5.6" height="3.4" ${F}/>`),
    pathfinder: S18(`<path d="${SQA}"/><path d="${SQB}" fill="currentColor" fill-opacity=".55"/>`),
    stroke: S18('<path d="M2.5 4.5h13" stroke-width=".9"/><path d="M2.5 9h13" stroke-width="1.8"/><path d="M2.5 13.8h13" stroke-width="3"/>', ' stroke-linecap="butt"'),
    swatches: S18(`<rect x="2.5" y="2.5" width="5.6" height="5.6" fill="currentColor"/><rect x="9.9" y="2.5" width="5.6" height="5.6"/><rect x="2.5" y="9.9" width="5.6" height="5.6"/><rect x="9.9" y="9.9" width="5.6" height="5.6" fill="currentColor" fill-opacity=".5"/>`),
    character: `<svg viewBox="0 0 18 18" aria-hidden="true"><path d="M2.6 14.6 6.8 3.4h1.4l4.2 11.2h-1.6l-1.1-3.1H5.3l-1.1 3.1Zm3.2-4.4h3.4L7.5 5.4Z" fill="currentColor"/><path d="M12.8 7.8h3M14.3 6.3v3" stroke="currentColor" stroke-width="1.1"/></svg>`,
    trace: S18('<rect x="2.5" y="3" width="13" height="12" rx="1"/><path d="m2.8 13 4.2-4.6 3 3.2 2-2 3.2 3.4" fill="currentColor" fill-opacity=".35"/><circle cx="11.6" cy="6.6" r="1.3"/>'),
    xpanel: S18('<rect x="2.5" y="3" width="13" height="12" rx="1"/><path d="M10.5 3v12M12 6h2M12 8.5h2"/>'),
    // Align: the line the edges go to, the shapes solid (Illustrator's Align panel)
    alL: S18(`<rect x="4" y="4" width="10" height="3.6" ${F}/><rect x="4" y="10.4" width="6" height="3.6" ${F}/><path d="M2.5 2v14"/>`),
    alC: S18(`<rect x="3.5" y="4" width="11" height="3.6" ${F}/><rect x="5.8" y="10.4" width="6.4" height="3.6" ${F}/><path d="M9 2v14"/>`),
    alR: S18(`<rect x="4" y="4" width="10" height="3.6" ${F}/><rect x="8" y="10.4" width="6" height="3.6" ${F}/><path d="M15.5 2v14"/>`),
    alT: S18(`<rect x="4" y="4" width="3.6" height="10" ${F}/><rect x="10.4" y="4" width="3.6" height="6" ${F}/><path d="M2 2.5h14"/>`),
    alM: S18(`<rect x="4" y="3.5" width="3.6" height="11" ${F}/><rect x="10.4" y="5.8" width="3.6" height="6.4" ${F}/><path d="M2 9h14"/>`),
    alB: S18(`<rect x="4" y="4" width="3.6" height="10" ${F}/><rect x="10.4" y="8" width="3.6" height="6" ${F}/><path d="M2 15.5h14"/>`),
    diT: S18(`<rect x="5.5" y="2.5" width="7" height="3" ${F}/><rect x="4" y="8" width="10" height="2.4" ${F}/><rect x="6.5" y="13.5" width="5" height="2" ${F}/><path d="M1.8 2.5h14.4M1.8 8h14.4M1.8 13.5h14.4" stroke-width=".8"/>`),
    diM: S18(`<rect x="5.5" y="2" width="7" height="3" ${F}/><rect x="4" y="7.8" width="10" height="2.4" ${F}/><rect x="6.5" y="13" width="5" height="2" ${F}/><path d="M1.8 3.5h14.4M1.8 9h14.4M1.8 14h14.4" stroke-width=".8"/>`),
    diB: S18(`<rect x="5.5" y="2.5" width="7" height="3" ${F}/><rect x="4" y="7.6" width="10" height="2.4" ${F}/><rect x="6.5" y="13.5" width="5" height="2" ${F}/><path d="M1.8 5.5h14.4M1.8 10h14.4M1.8 15.5h14.4" stroke-width=".8"/>`),
    diL: S18(`<rect x="2.5" y="5.5" width="3" height="7" ${F}/><rect x="8" y="4" width="2.4" height="10" ${F}/><rect x="13.5" y="6.5" width="2" height="5" ${F}/><path d="M2.5 1.8v14.4M8 1.8v14.4M13.5 1.8v14.4" stroke-width=".8"/>`),
    diC: S18(`<rect x="2" y="5.5" width="3" height="7" ${F}/><rect x="7.8" y="4" width="2.4" height="10" ${F}/><rect x="13" y="6.5" width="2" height="5" ${F}/><path d="M3.5 1.8v14.4M9 1.8v14.4M14 1.8v14.4" stroke-width=".8"/>`),
    diR: S18(`<rect x="2.5" y="5.5" width="3" height="7" ${F}/><rect x="7.6" y="4" width="2.4" height="10" ${F}/><rect x="13.5" y="6.5" width="2" height="5" ${F}/><path d="M5.5 1.8v14.4M10 1.8v14.4M15.5 1.8v14.4" stroke-width=".8"/>`),
    spV: S18(`<rect x="4" y="2" width="10" height="3.4" ${F}/><rect x="4" y="12.6" width="10" height="3.4" ${F}/><path d="M9 7v4M7.6 7h2.8M7.6 11h2.8" stroke-width=".9"/>`),
    spH: S18(`<rect x="2" y="4" width="3.4" height="10" ${F}/><rect x="12.6" y="4" width="3.4" height="10" ${F}/><path d="M7 9h4M7 7.6v2.8M11 7.6v2.8" stroke-width=".9"/>`),
    // Pathfinder: Illustrator's two squares, what is kept solid
    pfUnite: S18(`<path d="M2.5 2.5h8.5V7h4.5v8.5H7V11H2.5z" ${F}/>`),
    pfMinus: S18(`<path d="M2.5 2.5h8.5V7H7v4H2.5z" ${F}/><path d="${SQB}" stroke-dasharray="1.6 1.4"/>`),
    pfIntersect: S18(`<path d="${SQA}"/><path d="${SQB}"/><path d="M7 7h4v4H7z" ${F}/>`),
    pfExclude: S18(`<path d="M2.5 2.5h8.5v8.5H2.5z M7 7h8.5v8.5H7z" fill="currentColor" fill-rule="evenodd" stroke="none"/><path d="M7 7h4v4H7z"/>`),
    pfDivide: S18(`<path d="M2.5 2.5h8.5V7H7v4H2.5z" fill="currentColor" fill-opacity=".45" stroke="none"/><path d="M7 7h4v4H7z" ${F}/><path d="M11 7h4.5v8.5H7V11h4z" fill="currentColor" fill-opacity=".75" stroke="none"/><path d="${SQA}"/><path d="${SQB}"/>`),
    pfTrim: S18(`<path d="M2.5 2.5h8.5V7H7v4H2.5z" fill="currentColor" fill-opacity=".45"/><path d="${SQB}" ${F}/>`),
    // Paragraph alignment
    pL: S18('<path d="M3 4h12M3 7.3h8M3 10.6h12M3 13.9h8"/>'),
    pC: S18('<path d="M3 4h12M5 7.3h8M3 10.6h12M5 13.9h8"/>'),
    pR: S18('<path d="M3 4h12M7 7.3h8M3 10.6h12M7 13.9h8"/>'),
    // the Document section
    grid: S18('<rect x="2.5" y="2.5" width="13" height="13"/><path d="M2.5 6.8h13M2.5 11.2h13M6.8 2.5v13M11.2 2.5v13" stroke-width=".7"/>'),
    guides: S18('<path d="M1.5 6.5h15M11.5 1.5v15" stroke-dasharray="2.2 1.6"/>'),
    smart: S18('<rect x="5.5" y="5.5" width="7" height="7"/><path d="M9 1.5v2.6M9 13.9v2.6M1.5 9h2.6M13.9 9h2.6" stroke-width=".9"/>'),
    snapGrid: S18(`<path d="M2.5 6.8h13M2.5 11.2h13M6.8 2.5v13M11.2 2.5v13" stroke-width=".7"/><circle cx="11.2" cy="6.8" r="2" ${F}/>`),
    snapGuides: S18(`<path d="M1.5 9h15" stroke-dasharray="2.2 1.6"/><rect x="5" y="9" width="6" height="5" ${F}/>`),
    fit: S18('<rect x="5" y="5.5" width="8" height="7"/><path d="M2 5V2h3M13 2h3v3M16 13v3h-3M5 16H2v-3"/>'),
    // Stroke: caps, corners, where the line sits (the laser draws round: the others shown as Illustrator shows them, not available)
    capButt: S18('<path d="M3 9h9" stroke-width="5" stroke-linecap="butt"/><path d="M12 4v10" stroke-width=".7"/>'),
    capRound: S18('<path d="M3 9h9" stroke-width="5" stroke-linecap="round"/><path d="M12 4v10" stroke-width=".7"/>'),
    capProj: S18('<path d="M3 9h9" stroke-width="5" stroke-linecap="square"/><path d="M12 4v10" stroke-width=".7"/>'),
    joinMiter: S18('<path d="M3 15V4h11" stroke-width="4" stroke-linejoin="miter" stroke-linecap="butt"/>'),
    joinRound: S18('<path d="M3 15V4h11" stroke-width="4" stroke-linejoin="round" stroke-linecap="butt"/>'),
    joinBevel: S18('<path d="M3 15V4h11" stroke-width="4" stroke-linejoin="bevel" stroke-linecap="butt"/>'),
    stCenter: S18('<rect x="4" y="4" width="10" height="10" stroke-width="3" stroke-opacity=".55"/><rect x="4" y="4" width="10" height="10" stroke-width=".7"/>'),
    stInside: S18('<rect x="5.5" y="5.5" width="7" height="7" stroke-width="3" stroke-opacity=".55"/><rect x="4" y="4" width="10" height="10" stroke-width=".7"/>'),
    stOutside: S18('<rect x="2.5" y="2.5" width="13" height="13" stroke-width="3" stroke-opacity=".55"/><rect x="4" y="4" width="10" height="10" stroke-width=".7"/>')
  };
  const PICON = id => IC[id] || IC.xpanel;

  /* ── the look: every colour and the font from the shell's --ai-* tokens (fallbacks: Illustrator's Medium Dark) ── */
  const CSS = `
.ce-root .ce-dock.aip,.ce-root .aip-fly,.ce-root .aip-menu,.ce-root .aip-modal,.ce-root .ce-xpanel.aip-x{
  --aip-frame:var(--ai-frame,#424242); --aip-panel:var(--ai-panel,#535353); --aip-hi:var(--ai-panel-hi,#646464); --aip-border:var(--ai-border,#3a3a3a);
  --aip-input:var(--ai-input,#454545); --aip-input-b:var(--ai-input-border,#393939); --aip-text:var(--ai-text,#dedede); --aip-dim:var(--ai-text-dim,#aaaaaa);
  --aip-off:var(--ai-text-disabled,#7b7b7b); --aip-hover:var(--ai-hover,#5c5c5c); --aip-accent:var(--ai-accent,#378ef0); --aip-accent-text:var(--ai-accent-text,#ffffff);
  --aip-menu:var(--ai-menu,#4b4b4b); --aip-menu-hover:var(--ai-menu-hover,#378ef0);
  --aip-font:var(--ai-font,"Adobe Clean","Source Sans 3","Segoe UI",system-ui,-apple-system,sans-serif); --aip-fs:var(--ai-fs,12px); --aip-row:var(--ai-row,24px);
  --aip-lc-cut:#ececec; --aip-lc-line:#ff5c50; --aip-lc-hatch:#4f8ff8; --aip-lc-art:#c3b597; --aip-lc-mixed:#b48cf2;
  --aip-c-cut:#1c1d1d; --aip-c-line:#d4291f; --aip-c-hatch:#1a5fc4; --aip-c-art:#57534e;
}
.ce-root .ce-dock.aip{ padding:0; border:0; border-left:1px solid var(--aip-border); background:var(--aip-frame); color:var(--aip-text); font-family:var(--aip-font); font-size:var(--aip-fs); line-height:1.3;
  display:flex; flex-direction:column; overflow:visible; user-select:none; -webkit-user-select:none; --aip-rowh:var(--aip-row); }
@media (min-width:${DRAWER + 1}px){ .ce-root .ce-dock.aip{ width:auto; position:relative; z-index:6; } }
.ce-root .ce-dock.aip.is-rows-small{ --aip-rowh:20px; } .ce-root .ce-dock.aip.is-rows-large{ --aip-rowh:34px; }
.ce-root .aip *{ box-sizing:border-box; }
.aip-head{ flex:none; height:15px; display:flex; justify-content:flex-end; align-items:center; background:var(--aip-frame); border-bottom:1px solid var(--aip-border); }
.ce-root .aip-head button{ height:15px; min-width:22px; padding:0 4px; border:0; background:none; color:var(--aip-dim); cursor:pointer; display:inline-flex; align-items:center; justify-content:center; font-size:13px; line-height:1; }
.ce-root .aip-head button svg{ width:12px; height:12px; } .ce-root .aip-head button:hover{ color:var(--aip-text); background:var(--aip-hover); }
.ce-root .aip-head [data-act="dock"]{ display:none; }
.aip-cols{ flex:1 1 auto; min-height:0; display:flex; align-items:stretch; }
.aip-strip{ flex:none; width:36px; display:flex; flex-direction:column; background:var(--aip-frame); border-right:1px solid var(--aip-border); overflow-y:auto; overflow-x:hidden; }
.aip-strip.is-empty{ display:none; }
.aip-sg{ display:flex; flex-direction:column; padding:3px 0; border-bottom:1px solid var(--aip-border); }
.aip-sg.aip-drop{ background:var(--aip-hover); }
.ce-root .aip-ico{ height:30px; width:100%; display:flex; align-items:center; gap:8px; padding:0 0 0 8px; border:0; background:none; color:var(--aip-dim); cursor:pointer; white-space:nowrap; text-align:left; font:inherit; font-size:var(--aip-fs); }
.ce-root .aip-ico svg{ width:18px; height:18px; flex:none; }
.aip-ico span{ display:none; overflow:hidden; text-overflow:ellipsis; color:var(--aip-text); }
.ce-root .aip-ico:hover{ background:var(--aip-hover); color:var(--aip-text); }
.ce-root .aip-ico.is-on{ background:var(--aip-panel); color:var(--aip-text); box-shadow:inset 2px 0 0 var(--aip-accent); }
.aip.is-labels .aip-strip{ width:136px; } .aip.is-labels .aip-ico span{ display:inline; }
.aip-main{ flex:1 1 auto; width:clamp(236px,19vw,284px); min-width:0; min-height:0; display:flex; flex-direction:column; background:var(--aip-frame); }
.aip.is-icons .aip-main{ display:none; }
.aip-main.aip-drop{ box-shadow:inset 0 -3px 0 var(--aip-accent); }
.aip-grp{ display:flex; flex-direction:column; min-height:0; flex:0 1 auto; max-height:62%; border-bottom:1px solid var(--aip-border); background:var(--aip-panel); }
.aip-grp.is-fill{ flex:1 1 0; max-height:none; min-height:140px; }
.aip-grp.is-min{ flex:none; max-height:none; min-height:0; }
.aip-grp.is-min .aip-gbody{ display:none; }
.aip-tabs{ flex:none; display:flex; align-items:stretch; height:24px; background:var(--aip-frame); border-bottom:1px solid var(--aip-border); }
.aip-tabs.aip-drop{ box-shadow:inset 0 0 0 1px var(--aip-accent); }
.ce-root .aip-tab{ flex:0 1 auto; min-width:0; padding:0 10px; border:0; border-right:1px solid var(--aip-border); background:none; color:var(--aip-dim); font:inherit; font-size:11px; cursor:pointer; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; line-height:23px; }
.ce-root .aip-tab.is-on{ background:var(--aip-panel); color:var(--aip-text); margin-bottom:-1px; }
.ce-root .aip-tab:not(.is-on):hover{ color:var(--aip-text); background:var(--aip-hover); }
.aip-tabs__fill{ flex:1 1 auto; min-width:4px; }
.ce-root .aip-pmenu{ flex:none; width:26px; border:0; background:none; color:var(--aip-dim); cursor:pointer; padding:0; display:inline-flex; align-items:center; justify-content:center; }
.ce-root .aip-pmenu svg{ width:14px; height:14px; } .ce-root .aip-pmenu:hover{ color:var(--aip-text); background:var(--aip-hover); }
.aip-gbody{ flex:1 1 auto; min-height:0; display:flex; flex-direction:column; background:var(--aip-panel); overflow:hidden; }
.aip-store{ display:none !important; }
.aip-pb{ flex:1 1 auto; min-height:0; display:flex; flex-direction:column; }
.aip-pp{ flex:1 1 auto; min-height:0; overflow-y:auto; overflow-x:hidden; padding:4px 10px 10px; }
.aip-tabghost{ position:fixed; z-index:2147483000; pointer-events:none; padding:4px 10px; background:var(--ai-panel,#535353); color:var(--ai-text,#dedede); border:1px solid var(--ai-accent,#378ef0); font:11px var(--ai-font,"Segoe UI",system-ui,sans-serif); box-shadow:0 6px 18px rgba(0,0,0,.35); }
/* the fly-out of a collapsed panel (Illustrator: an icon clicked pops its group out to the left) */
.aip-fly{ position:absolute; right:100%; top:0; width:clamp(236px,19vw,284px); max-height:calc(100% - 4px); display:flex; flex-direction:column; background:var(--aip-panel);
  border:1px solid var(--aip-border); box-shadow:-12px 12px 32px rgba(0,0,0,.42); z-index:8; color:var(--aip-text); font-family:var(--aip-font); font-size:var(--aip-fs); }
.aip-fly .aip-grp{ flex:1 1 auto; max-height:none; border:0; min-height:0; }
.aip-fly .aip-grp.has-layers{ height:min(560px,calc(100vh - 140px)); }
/* ── Layers rows ── */
.ce-root .aip .ce-layers{ flex:1 1 auto; min-height:0; padding:0; background:var(--aip-panel); overflow-y:auto; }
.ce-root .aip .ce-layer{ display:flex; align-items:stretch; gap:0; height:var(--aip-rowh); min-height:0; padding:0; margin:0; border:0; border-bottom:1px solid var(--aip-border); background:var(--aip-panel);
  color:var(--aip-text); box-shadow:none; position:relative; contain-intrinsic-block-size:auto var(--aip-rowh); transition:none; }
.ce-root .aip .ce-layer + .ce-layer{ margin-top:0; }
.ce-root .aip .ce-layer:hover{ background:var(--aip-panel); }
.ce-root .aip .ce-layer.is-sel,.ce-root .aip .ce-layer.is-sel:hover{ background:var(--aip-hi); border-color:var(--aip-border); box-shadow:none; }
.ce-root .aip .ce-layer.is-sel .ce-layer__name{ font-weight:400; }
.ce-root .aip .ce-layer.is-hidden{ opacity:1; } .ce-root .aip .ce-layer.is-hidden .aip-rb{ opacity:.5; }
.ce-root .aip .ce-layer.is-locked .ce-layer__name{ color:var(--aip-text); }
.ce-root .aip .ce-layer.is-dragging{ opacity:.45; }
.ce-root .aip .ce-layer.is-drop-above,.ce-root .aip .ce-layer.is-drop-below{ box-shadow:none; }
.ce-root .aip .ce-layer.is-drop-above::after,.ce-root .aip .ce-layer.is-drop-below::after{ content:""; position:absolute; left:44px; right:0; height:3px; background:var(--aip-text); z-index:3; pointer-events:none; }
.ce-root .aip .ce-layer.is-drop-above::after{ top:-2px; } .ce-root .aip .ce-layer.is-drop-below::after{ bottom:-2px; }
.ce-root .aip .ce-layer.is-drop-into,.ce-root .aip .ce-layer.aip-sqdrop{ outline:2px solid var(--aip-text); outline-offset:-2px; background:var(--aip-hover); }
.ce-root .aip .ce-layer__b{ flex:none; width:22px; height:auto; border:0; border-right:1px solid var(--aip-border); border-radius:0; background:none; color:var(--aip-text); cursor:pointer; padding:0; display:flex; align-items:center; justify-content:center; opacity:1; }
.ce-root .aip .ce-layer__b svg{ display:block; width:15px; height:15px; }
.ce-root .aip .ce-layer__b:hover{ background:var(--aip-hover); color:var(--aip-text); }
.aip-rb{ flex:1 1 auto; min-width:0; display:flex; align-items:center; gap:4px; padding-left:calc(6px + var(--depth,0) * 14px); position:relative; }
.aip-lc{ position:absolute; left:0; top:0; bottom:0; width:3px; background:var(--lc); }
.ce-root .aip .ce-layer__tw{ flex:none; width:12px; height:100%; border:0; background:none; color:var(--aip-text); cursor:pointer; padding:0; display:inline-flex; align-items:center; justify-content:center; }
.ce-root .aip .ce-layer__tw svg{ width:9px; height:9px; }
.aip-tw0{ flex:none; width:12px; }
.aip-thumb{ flex:none; width:calc(var(--aip-rowh) - 6px); height:calc(var(--aip-rowh) - 6px); background:#fff; border:1px solid var(--aip-border); display:flex; align-items:center; justify-content:center; overflow:hidden; color:#222; }
.ce-root .aip .aip-thumb.ce-layer__fold{ width:calc(var(--aip-rowh) - 6px); height:calc(var(--aip-rowh) - 6px); color:#222; }
.aip-thumb svg{ width:100%; height:100%; display:block; }
.aip.is-nothumb .aip-thumb{ display:none; }
.aip-thumb .t-cut{ stroke:var(--aip-c-cut); } .aip-thumb .t-line{ stroke:var(--aip-c-line); } .aip-thumb .t-hatch{ stroke:var(--aip-c-hatch); } .aip-thumb .t-art{ stroke:var(--aip-c-art); }
.aip-thumb path{ fill:none; stroke-width:1px; vector-effect:non-scaling-stroke; }
.aip-thumb path.is-f.t-cut{ fill:var(--aip-c-cut); } .aip-thumb path.is-f.t-line{ fill:var(--aip-c-line); } .aip-thumb path.is-f.t-hatch{ fill:var(--aip-c-hatch); } .aip-thumb path.is-f.t-art{ fill:var(--aip-c-art); }
.aip-thumb path.is-f:not(.is-s){ stroke:none; } .aip-thumb path.is-eo{ fill-rule:evenodd; }
.ce-root .aip .ce-layer__name{ flex:1 1 auto; min-width:0; font-size:var(--aip-fs); line-height:1.2; color:var(--aip-text); overflow:hidden; text-overflow:ellipsis; white-space:nowrap; padding-left:2px; }
.ce-root .aip .ce-layer__name input{ width:100%; height:calc(var(--aip-rowh) - 4px); font:inherit; color:var(--aip-text); background:var(--aip-input); border:1px solid var(--aip-accent); padding:0 4px; outline:none; border-radius:2px; }
.aip-bdg{ flex:none; width:14px; height:14px; color:var(--aip-dim); display:inline-flex; } .aip-bdg svg{ width:14px; height:14px; }
.ce-root .aip .ce-layer__new{ flex:none; width:5px; height:5px; border-radius:50%; background:#d9b56c; margin:0 2px; }
.ce-root .aip-target{ flex:none; width:22px; border:0; background:none; padding:0; cursor:pointer; display:flex; align-items:center; justify-content:center; }
.aip-target i{ display:block; width:10px; height:10px; border-radius:50%; border:1.2px solid var(--aip-dim); }
.ce-layer.is-sel .aip-target i{ border-color:var(--aip-text); outline:1.2px solid var(--aip-text); outline-offset:1.6px; width:8px; height:8px; }
.aip-target.is-app i{ background:var(--aip-dim); } .ce-layer.is-sel .aip-target.is-app i{ background:var(--aip-text); }
.ce-root .aip-selsq{ flex:none; width:20px; border:0; background:none; padding:0; cursor:pointer; display:flex; align-items:center; justify-content:center; }
.aip-selsq i{ display:block; width:8px; height:8px; background:var(--lc); box-shadow:0 0 0 1px rgba(0,0,0,.45); opacity:0; }
.ce-layer.is-sel .aip-selsq i{ opacity:1; cursor:grab; }
.ce-layer.aip-has-sel:not(.is-sel) .aip-selsq i{ opacity:1; width:5px; height:5px; }
.aip-sqghost{ position:fixed; z-index:2147483000; width:9px; height:9px; pointer-events:none; box-shadow:0 0 0 1px rgba(0,0,0,.5); }
.ce-root .aip .ce-layers__empty{ color:var(--aip-dim); font-size:11.5px; }
.ce-root .aip .ce-pane__bar{ flex:none; display:flex; align-items:center; gap:0; height:28px; padding:0 4px 0 9px; border:0; border-top:1px solid var(--aip-border); background:var(--aip-panel); }
.aip-count{ flex:1 1 auto; min-width:0; color:var(--aip-dim); font-size:11px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.ce-root .aip .ce-pane__bar .ce-act{ flex:none; width:26px; min-width:0; height:24px; min-height:0; border:0; border-radius:3px; background:none; color:var(--aip-text); padding:0; display:inline-flex; align-items:center; justify-content:center; }
.ce-root .aip .ce-pane__bar .ce-act svg{ width:16px; height:16px; }
.ce-root .aip .ce-pane__bar .ce-act:not(:disabled):hover{ background:var(--aip-hover); border:0; color:var(--aip-text); }
.ce-root .aip .ce-pane__bar .ce-act:disabled{ opacity:.35; }
/* ── the panels' controls (Illustrator's: small fields, icon buttons, outlined buttons) ── */
.aip-kind{ font-weight:700; font-size:var(--aip-fs); color:var(--aip-text); padding:7px 0 7px; }
.aip-sec{ padding:8px 0 10px; border-top:1px solid var(--aip-border); }
.aip-sec:first-child{ border-top:0; }
.aip-sec__h{ display:flex; align-items:center; justify-content:space-between; height:18px; margin:0 0 7px; font-size:11px; font-weight:700; color:var(--aip-text); }
.ce-root .aip-more{ width:24px; height:18px; border:0; border-radius:3px; background:none; color:var(--aip-dim); cursor:pointer; padding:0; display:inline-flex; align-items:center; justify-content:center; }
.ce-root .aip-more svg{ width:16px; height:16px; } .ce-root .aip-more:hover{ background:var(--aip-hover); color:var(--aip-text); }
.aip-sub{ font-size:11px; color:var(--aip-dim); margin:8px 0 5px; }
.aip-row{ display:flex; align-items:center; gap:6px; min-width:0; margin-top:6px; }
.aip-row:first-child{ margin-top:0; }
.aip-row > .aip-f{ flex:1 1 0; }
.aip-f{ display:flex; align-items:center; gap:5px; min-width:0; }
.aip-f > span,.aip-lab{ flex:none; color:var(--aip-dim); font-size:11px; white-space:nowrap; }
.aip-lab{ min-width:44px; }
.ce-root .aip-in{ flex:1 1 auto; min-width:0; width:100%; height:22px; padding:0 4px 0 5px; border:1px solid var(--aip-input-b); border-radius:3px; background:var(--aip-input); color:var(--aip-text);
  font:inherit; font-size:var(--aip-fs); line-height:20px; outline:none; box-shadow:none; margin:0; font-variant-numeric:tabular-nums; }
.ce-root .aip-in:focus{ border-color:var(--aip-accent); box-shadow:0 0 0 1px var(--aip-accent); }
.ce-root .aip-in:disabled{ color:var(--aip-off); background:transparent; }
.ce-root .aip-in::placeholder{ color:var(--aip-off); }
.ce-root .aip-ib{ flex:none; width:26px; height:24px; border:0; border-radius:3px; background:none; color:var(--aip-text); cursor:pointer; padding:0; display:inline-flex; align-items:center; justify-content:center; }
.ce-root .aip-ib svg{ width:17px; height:17px; }
.ce-root .aip-ib:not(:disabled):hover{ background:var(--aip-hover); }
.ce-root .aip-ib.is-on{ background:var(--aip-hi); box-shadow:inset 0 0 0 1px var(--aip-dim); }
.ce-root .aip-ib:disabled,.ce-root .aip-ib.is-off{ opacity:.33; cursor:default; }
.aip-ibs{ display:flex; flex-wrap:wrap; gap:2px; }
.ce-root .aip-btn{ height:24px; min-width:0; padding:0 9px; border:1px solid var(--aip-dim); border-radius:4px; background:none; color:var(--aip-text); font:inherit; font-size:11px; cursor:pointer; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; display:inline-flex; align-items:center; justify-content:center; gap:4px; }
.ce-root .aip-btn:not(:disabled):hover{ background:var(--aip-hover); border-color:var(--aip-text); }
.ce-root .aip-btn:disabled{ opacity:.35; cursor:default; }
.ce-root .aip-btn.is-cta{ background:var(--aip-accent); border-color:var(--aip-accent); color:var(--aip-accent-text); border-radius:12px; padding:0 14px; }
.aip-qa{ display:grid; grid-template-columns:1fr 1fr; gap:6px; }
.ce-root .aip-dd{ flex:1 1 auto; min-width:0; height:22px; padding:0 4px 0 6px; border:1px solid var(--aip-input-b); border-radius:3px; background:var(--aip-input); color:var(--aip-text); font:inherit; font-size:var(--aip-fs);
  cursor:pointer; display:flex; align-items:center; gap:4px; text-align:left; }
.aip-dd > span{ flex:1 1 auto; min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.ce-root .aip-dd:disabled{ color:var(--aip-off); cursor:default; }
.aip-caret{ width:8px; height:8px; flex:none; color:var(--aip-dim); }
.ce-root .aip-ddc{ flex:none; width:16px; height:22px; margin-left:-5px; border:1px solid var(--aip-input-b); border-left:0; border-radius:0 3px 3px 0; background:var(--aip-input); color:var(--aip-dim); cursor:pointer; padding:0; display:inline-flex; align-items:center; justify-content:center; }
.aip-f .aip-in.has-ddc{ border-radius:3px 0 0 3px; }
/* the reference point locator and the Transform grid */
.aip-tf{ display:grid; grid-template-columns:36px minmax(0,1fr) minmax(0,1fr) 24px; grid-template-rows:auto auto; gap:6px 6px; align-items:center; }
.aip-loc{ grid-row:1 / 3; grid-column:1; width:34px; height:34px; display:grid; grid-template-columns:repeat(3,1fr); grid-template-rows:repeat(3,1fr); position:relative; }
.aip-loc::before{ content:""; position:absolute; inset:5px; border:1px solid var(--aip-dim); pointer-events:none; }
.ce-root .aip-loc button{ position:relative; z-index:1; border:0; background:none; padding:0; display:flex; align-items:center; justify-content:center; cursor:pointer; }
.aip-loc button i{ display:block; width:6px; height:6px; border:1px solid var(--aip-dim); background:var(--aip-panel); }
.aip-loc button.is-on i{ background:var(--aip-text); border-color:var(--aip-text); }
.aip-tf .aip-link{ grid-row:1 / 3; grid-column:4; }
.aip-tf .aip-link.is-on{ color:var(--aip-text); } .aip-tf .aip-link:not(.is-on){ color:var(--aip-off); }
.aip-sw{ display:inline-block; width:16px; height:16px; flex:none; border:1px solid rgba(0,0,0,.45); position:relative; background:var(--c,#fff); }
.aip-sw.is-stroke{ background:transparent; box-shadow:inset 0 0 0 3.5px var(--c,#fff); }
.aip-sw.is-none{ background:#fff; overflow:hidden; box-shadow:none; } .aip-sw.is-none::after{ content:""; position:absolute; left:-3px; right:-3px; top:50%; height:1.5px; background:#e0281f; transform:rotate(-45deg); }
.aip-sw.is-mixed{ background:linear-gradient(135deg,var(--aip-c-line) 0 50%,var(--aip-c-hatch) 50%); }
.ce-root .aip-swb{ display:inline-flex; align-items:center; gap:5px; height:22px; padding:0 4px; border:1px solid transparent; border-radius:3px; background:none; color:var(--aip-text); cursor:pointer; font:inherit; font-size:11px; }
.ce-root .aip-swb:hover{ border-color:var(--aip-input-b); background:var(--aip-hover); }
.aip-sws{ display:flex; flex-wrap:wrap; gap:4px; padding:10px; }
.ce-root .aip-swc{ width:24px; height:24px; padding:0; border:1px solid var(--aip-border); cursor:pointer; background:none; display:flex; align-items:center; justify-content:center; }
.ce-root .aip-swc.is-on{ outline:2px solid var(--aip-text); outline-offset:1px; }
.aip-swc .aip-sw{ width:22px; height:22px; border:0; }
.aip-swc b{ display:none; }
.aip.is-swlist .aip-sws,.aip-fly.is-swlist .aip-sws{ flex-direction:column; gap:0; padding:4px 0; }
.ce-root .aip.is-swlist .aip-swc,.ce-root .aip-fly.is-swlist .aip-swc{ width:100%; height:24px; justify-content:flex-start; gap:8px; padding:0 10px; border:0; border-bottom:1px solid var(--aip-border); outline:0; color:var(--aip-text); font:inherit; font-size:var(--aip-fs); }
.aip.is-swlist .aip-swc .aip-sw,.aip-fly.is-swlist .aip-swc .aip-sw{ width:16px; height:16px; border:1px solid rgba(0,0,0,.45); }
.aip.is-swlist .aip-swc b,.aip-fly.is-swlist .aip-swc b{ display:inline; font-weight:400; }
.aip.is-swlist .aip-swc.is-on,.aip-fly.is-swlist .aip-swc.is-on{ background:var(--aip-hi); }
.aip-note{ font-size:11px; line-height:1.45; color:var(--aip-dim); margin-top:8px; }
.aip-kv{ display:grid; grid-template-columns:auto 1fr; gap:6px 10px; align-items:center; }
.aip-kv > span{ color:var(--aip-dim); font-size:11px; }
.aip-kv > b{ font-weight:400; color:var(--aip-text); }
.aip-slider{ position:relative; height:4px; border-radius:2px; background:var(--aip-input); margin:4px 0; }
.aip-slider i{ position:absolute; left:0; top:0; bottom:0; border-radius:2px; background:var(--aip-dim); }
.aip-check{ display:flex; align-items:center; gap:7px; font-size:11px; color:var(--aip-text); }
.aip-check i{ width:12px; height:12px; border:1px solid var(--aip-dim); border-radius:2px; display:inline-flex; align-items:center; justify-content:center; font-style:normal; font-size:10px; line-height:1; }
.aip-pb[data-aip-panel] [hidden]{ display:none !important; }
.aip-fontname{ font-size:12.5px; }
/* the extension panels' column (the Library), its tabs drawn as these */
.ce-root .ce-xpanel.aip-x{ border:0; border-left:1px solid var(--aip-border); }
.ce-root .ce-xpanel.aip-x .ce-dock__tabs{ height:24px; background:var(--aip-frame); border-bottom:1px solid var(--aip-border); }
.ce-root .ce-xpanel.aip-x .ce-dock__tab{ flex:0 1 auto; padding:0 10px; border:0; border-right:1px solid var(--aip-border); font:11px var(--aip-font); letter-spacing:0; text-transform:none; color:var(--aip-dim); }
.ce-root .ce-xpanel.aip-x .ce-dock__tab.is-on{ background:var(--aip-panel); color:var(--aip-text); }
.ce-root .ce-xpanel.aip-x .ce-dock__tab:not(.is-on):not(:disabled):hover{ background:var(--aip-hover); color:var(--aip-text); }
.ce-root .ce-xpanel.aip-x .ce-dock__x{ margin-left:auto; width:26px; border:0; background:none; color:var(--aip-dim); font-size:13px; }
.ce-root .ce-xpanel.aip-x .ce-dock__x:hover{ color:var(--aip-text); background:var(--aip-hover); }
/* the dialogs (Options, Panel Options): Illustrator's, over the work */
.aip-modal{ position:absolute; inset:0; z-index:45; background:rgba(0,0,0,.28); display:flex; align-items:center; justify-content:center; padding:16px; }
.aip-dlg{ width:min(380px,100%); background:var(--aip-panel); color:var(--aip-text); border:1px solid var(--aip-border); border-radius:5px; box-shadow:0 24px 60px rgba(0,0,0,.5); font-family:var(--aip-font); font-size:var(--aip-fs); }
.aip-dlg h3{ margin:0; padding:16px 18px 6px; font:700 15px var(--aip-font); color:var(--aip-text); }
.aip-dlg__b{ padding:8px 18px 4px; display:grid; grid-template-columns:auto 1fr; gap:9px 12px; align-items:center; }
.aip-dlg__b > span{ color:var(--aip-dim); font-size:11.5px; text-align:right; }
.aip-dlg__b label.aip-check{ grid-column:2; }
.ce-root .aip-dlg select{ height:22px; border:1px solid var(--aip-input-b); border-radius:3px; background:var(--aip-input); color:var(--aip-text); font:inherit; }
.aip-dlg__f{ display:flex; justify-content:flex-end; gap:8px; padding:14px 18px 16px; }
.ce-root .aip-dlg input[type="checkbox"]{ accent-color:var(--aip-accent); margin:0; }
@media (max-width:${NARROW}px){ .aip-main,.aip-fly{ width:min(284px,calc(100vw - 80px)); } }
@media (max-width:${DRAWER}px){
  .ce-root .ce-dock.aip{ width:min(340px,calc(100% - 52px)); border:1px solid var(--aip-border); }
  .ce-root .ce-dock.aip .aip-main{ width:auto; }
  .ce-root .aip-head [data-act="dock"]{ display:inline-flex; }
  .ce-root .aip-head [data-aim="collapse"]{ display:none; }
  .aip-fly{ width:min(284px,calc(100vw - 96px)); }
}
`;
  function injectCss() {
    if (!doc || doc.getElementById('ceAiPanelsCss')) return;
    const s = doc.createElement('style'); s.id = 'ceAiPanelsCss'; s.textContent = CSS; (doc.head || doc.documentElement).appendChild(s);
  }

  /* ── small builders ── */
  const ib = (attrs, icon, title, cls) => `<button class="aip-ib${cls ? ' ' + cls : ''}" type="button" ${attrs} title="${esc(title)}" aria-label="${esc(title)}">${icon}</button>`;
  const field = (k, label, title, o) => `<label class="aip-f" title="${esc(title)}">${label ? `<span>${label}</span>` : ''}<input class="aip-in${o && o.ddc ? ' has-ddc' : ''}" data-aif="${k}" inputmode="decimal" autocomplete="off" spellcheck="false" aria-label="${esc(title)}"${o && o.ph ? ` placeholder="${esc(o.ph)}"` : ''}>${o && o.ddc ? `<button class="aip-ddc" type="button" data-aim="${o.ddc}" aria-label="${esc(title)}: presets" title="Presets">${IC.caret}</button>` : ''}</label>`;
  const sec = (id, title, body, more) => `<section class="aip-sec" data-aip-sec="${id}"><div class="aip-sec__h"><span>${esc(title)}</span>${more ? `<button class="aip-more" type="button" data-aim="open:${more}" title="More Options: the ${esc(TITLE[more] || more)} panel" aria-label="More Options">${IC.more}</button>` : ''}</div>${body}</section>`;
  const locHtml = () => `<div class="aip-loc" role="radiogroup" aria-label="Reference point">${[0, 0.5, 1].map(fy => [0, 0.5, 1].map(fx => `<button type="button" data-aim="ref:${fx},${fy}" role="radio" aria-checked="false" title="Reference point: ${['top', 'middle', 'bottom'][fy * 2]} ${['left', 'centre', 'right'][fx * 2]}" aria-label="Reference point ${fx},${fy}"><i></i></button>`).join('')).join('')}</div>`;
  const tfHtml = () => `<div class="aip-tf">${locHtml()}`
    + field('x', 'X:', 'X position of the reference point, mm from the zero point (↑ ↓ 0.1 mm, Shift 1 mm)') + field('w', 'W:', 'Width, mm (↑ ↓ 0.1 mm, Shift 1 mm)')
    + `<button class="aip-ib aip-link" type="button" data-aim="link" title="Constrain Width and Height Proportions" aria-label="Constrain Width and Height Proportions">${IC.link}</button>`
    + field('y', 'Y:', 'Y position of the reference point, mm from the zero point (↑ ↓ 0.1 mm, Shift 1 mm)') + field('h', 'H:', 'Height, mm (↑ ↓ 0.1 mm, Shift 1 mm)') + '</div>';
  const ALIGN_BTNS = () => `<div class="aip-ibs">${ib('data-aim="al:left"', IC.alL, 'Horizontal Align Left')}${ib('data-aim="al:hcenter"', IC.alC, 'Horizontal Align Center')}${ib('data-aim="al:right"', IC.alR, 'Horizontal Align Right')}${ib('data-aim="al:top"', IC.alT, 'Vertical Align Top')}${ib('data-aim="al:vcenter"', IC.alM, 'Vertical Align Center')}${ib('data-aim="al:bottom"', IC.alB, 'Vertical Align Bottom')}</div>`;
  const DIST_BTNS = () => `<div class="aip-ibs">${ib('data-aim="di:top"', IC.diT, 'Vertical Distribute Top')}${ib('data-aim="di:vcenter"', IC.diM, 'Vertical Distribute Center')}${ib('data-aim="di:bottom"', IC.diB, 'Vertical Distribute Bottom')}${ib('data-aim="di:left"', IC.diL, 'Horizontal Distribute Left')}${ib('data-aim="di:hcenter"', IC.diC, 'Horizontal Distribute Center')}${ib('data-aim="di:right"', IC.diR, 'Horizontal Distribute Right')}</div>`;
  const PF_SHAPE = [['ops.unite', IC.pfUnite, 'Unite'], ['ops.minus', IC.pfMinus, 'Minus Front'], ['ops.intersect', IC.pfIntersect, 'Intersect'], ['ops.exclude', IC.pfExclude, 'Exclude']];
  const PF_FIND = [['ops.divide', IC.pfDivide, 'Divide'], ['ops.trim', IC.pfTrim, 'Trim']];
  const pfBtns = list => `<div class="aip-ibs">${list.map(([id, icon, t]) => ib(`data-aim="x:${id}"`, icon, t)).join('')}</div>`;
  const ALIGN_TO = { selection: 'Align to Selection', key: 'Align to Key Object', charm: 'Align to Artboard (the charm)', centre: "Align to the charm's centre of area" };
  const PT_PRESETS = [6, 7, 8, 9, 10, 11, 12, 14, 18, 21, 24, 36, 48, 60, 72];
  const LW_PRESETS = [0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.4, 0.5, 0.75, 1];
  const TRACK_PRESETS = [-100, -75, -50, -25, -10, 0, 10, 25, 50, 75, 100, 200];
  const NUDGE = { x: [0.1, 1], y: [0.1, 1], w: [0.1, 1], h: [0.1, 1], r: [1, 15], pt: [0.5, 2], lw: [0.01, 0.05], wt: [0.01, 0.05], track: [10, 50], lead: [0.5, 2], gap: [0.1, 1] };

  /** The panels' bodies (built once per workspace; moved between groups as the layout says). */
  function buildBodies(P) {
    const D = P.D, mk = (id, html, cls) => { const el = doc.createElement('div'); el.className = 'aip-pb' + (cls ? ' ' + cls : ''); el.dataset.aipPanel = id; el.innerHTML = html; return el; };
    const B = {};
    // PROPERTIES: context-sensitive, its sections shown as the selection asks
    const fillStroke = `<div class="aip-row"><span class="aip-lab">Fill</span><button class="aip-swb" type="button" data-aim="rolemenu" data-aip-fill title="Fill: what the laser does (hatch engrave fills; the Swatches are the roles)"><i class="aip-sw"></i><span></span>${IC.caret}</button></div>`
      + `<div class="aip-row"><span class="aip-lab">Stroke</span><button class="aip-swb" type="button" data-aim="rolemenu" data-aip-stroke title="Stroke: what the laser does (a cut or a line engraving follows the line)"><i class="aip-sw is-stroke"></i><span></span>${IC.caret}</button>`
      + `<span data-aip-when="lw" class="aip-f" style="flex:1 1 auto">${field('lw', '', 'Stroke weight: the line width on the charm, mm, as the laser file writes it (↑ ↓ 0.01 mm, Shift 0.05 mm)', { ddc: 'lwmenu' }).replace(/^<label class="aip-f"/, '<label class="aip-f" style="flex:1 1 auto"')}</span></div>`
      + `<div class="aip-row" data-aip-when="wt"><span class="aip-lab">Weight</span>${field('wt', '', 'Weight: every stroke of the letters or the hatched part this much thicker (+) or thinner (−) than drawn, mm (↑ ↓ 0.01 mm, Shift 0.05 mm)')}<button class="aip-ib" type="button" data-aim="wt0" title="Back to the font's own weight" aria-label="Back to the font's own weight">${D.IC.weight || IC.stroke}</button></div>`;
    const charHtml = full => `<div class="aip-row"><button class="aip-dd aip-fontname" type="button" data-aim="fontmenu" title="Font family (the engraving fonts)"><span data-aip-font>Font</span>${IC.caret}</button></div>`
      + `<div class="aip-row"><button class="aip-dd" type="button" data-aim="stylemenu" title="Font style"><span data-aip-fstyle>Regular</span>${IC.caret}</button></div>`
      + `<div class="aip-row">${field('pt', '<b style="font:700 11px var(--aip-font)">T</b><small style="font-size:8px">T</small>', 'Font size, pt (↑ ↓ 0.5 pt, Shift 2 pt)', { ddc: 'ptmenu' })}${field('lead', '<b style="font:700 10px var(--aip-font)">A↕</b>', 'Leading, pt (empty: Auto)', { ph: '(Auto)' })}</div>`
      + `<div class="aip-row">${field('track', '<b style="font:700 10px var(--aip-font)">VA</b>', 'Tracking, thousandths of an em (↑ ↓ 10, Shift 50)', { ddc: 'trackmenu' })}${full ? '<span class="aip-f"></span>' : ''}</div>`
      + `<div class="aip-row aip-ibs" role="group" aria-label="Letter case">${['', 'upper', 'lower', 'title', 'small'].map(c => `<button class="aip-ib" type="button" data-aim="case:${c}" role="menuitemradio" aria-checked="false" title="${esc((D.CASE_NAME || {})[c] || c)}" aria-label="${esc((D.CASE_NAME || {})[c] || c)}"><b style="font:700 11px var(--aip-font)">${esc((D.CASE_SIGN || {})[c] || 'Aa')}</b></button>`).join('')}</div>`;
    const paraHtml = `<div class="aip-row aip-ibs" role="group" aria-label="Paragraph alignment">${ib('data-aim="talign:left" role="menuitemradio" aria-checked="false"', IC.pL, 'Align left')}${ib('data-aim="talign:center" role="menuitemradio" aria-checked="false"', IC.pC, 'Align center')}${ib('data-aim="talign:right" role="menuitemradio" aria-checked="false"', IC.pR, 'Align right')}<span style="flex:1 1 auto"></span><button class="aip-btn" type="button" data-aim="alphafind" title="Glyphs: letters, words and phrases of other alphabets, found by their English">Glyphs…</button></div>`;
    const traceHtml = `<div class="aip-kv" data-aip-trace></div>`;
    const docHtml = sec('doc', 'Document', `<div class="aip-row"><span class="aip-lab">Artboard</span><div class="aip-ibs" style="flex:1 1 auto"><button class="aip-btn" type="button" data-aim="face:front" style="flex:1 1 0">Front</button><button class="aip-btn" type="button" data-aim="face:back" style="flex:1 1 0">Back</button></div></div>`
        + `<div class="aip-row"><span class="aip-lab">Units</span><span class="aip-dd" style="cursor:default"><span>Millimeters</span></span></div>`)
      + sec('rulers', 'Rulers & Grids', `<div class="aip-ibs">${ib('data-aim="opt:grid" role="menuitemcheckbox"', IC.grid, 'Show Grid (the millimetre grid)')}</div>`)
      + sec('guides', 'Guides', `<div class="aip-ibs">${ib('data-aim="opt:showGuides" role="menuitemcheckbox"', IC.guides, 'Show Guides (⌘;)')}</div>`)
      + sec('snap', 'Snap Options', `<div class="aip-ibs">${ib('data-aim="opt:smart" role="menuitemcheckbox"', IC.smart, 'Smart Guides (⌘U)')}${ib('data-aim="opt:snapGrid" role="menuitemcheckbox"', IC.snapGrid, 'Snap to Grid')}${ib('data-aim="opt:snapGuides" role="menuitemcheckbox"', IC.snapGuides, 'Snap to Guides')}</div>`)
      + sec('docqa', 'Quick Actions', `<div class="aip-qa"><button class="aip-btn" type="button" data-aim="zoom:fit" title="Fit Artboard in Window (⌘0)">Fit Artboard</button><button class="aip-btn" type="button" data-aim="unlockAll" title="Unlock All (⌥⌘2)">Unlock All</button><button class="aip-btn" type="button" data-aim="showAll" title="Show All (⌥⌘3)">Show All</button><button class="aip-btn" type="button" data-aim="x:cx.check" title="The laser check: what the laser cannot make">Laser Check</button></div>`);
    B.properties = mk('properties', `<div class="aip-pp"><div class="aip-kind" data-aip-kind>No Selection</div>`
      + sec('transform', 'Transform', tfHtml() + `<div class="aip-row">${field('r', IC.angle.replace('<svg', '<svg width="15" height="15"'), 'Rotate, degrees (↑ ↓ 1°, Shift 15°)')}<span style="flex:1 1 auto"></span>${ib('data-aim="x:ops.flipH"', IC.flipH, 'Flip Along Horizontal Axis (left to right)')}${ib('data-aim="x:ops.flipV"', IC.flipV, 'Flip Along Vertical Axis (top to bottom)')}</div>`, 'transform')
      + sec('appearance', 'Appearance', fillStroke, 'stroke')
      + sec('character', 'Character', charHtml(false), 'character')
      + sec('paragraph', 'Paragraph', paraHtml)
      + sec('trace', 'Image Trace', traceHtml + `<div class="aip-row"><button class="aip-btn" type="button" data-aim="x:imageRetrace" style="flex:1 1 auto" title="Image Trace: the Image step on this picture, its settings, Re-trace">Image Trace…</button></div>`, 'trace')
      + sec('pathfinder', 'Pathfinder', pfBtns(PF_SHAPE.concat(PF_FIND)), 'pathfinder')
      + sec('align', 'Align', ALIGN_BTNS(), 'align')
      + sec('quick', 'Quick Actions', '<div class="aip-qa" data-aip-qa></div>')
      + docHtml + '</div>');
    // LAYERS: the editor's own list and its button bar, moved in
    B.layers = mk('layers', '');
    B.layers.appendChild(D.layersEl);
    B.layers.appendChild(P.bar);
    // TRANSFORM
    B.transform = mk('transform', `<div class="aip-pp"><div class="aip-sec" style="border:0;padding-top:8px">${tfHtml()}<div class="aip-row" style="margin-top:8px">${field('r', IC.angle.replace('<svg', '<svg width="15" height="15"'), 'Rotate, degrees (↑ ↓ 1°, Shift 15°)')}<span class="aip-f"></span></div>`
      + `<div class="aip-row" style="margin-top:10px">${ib('data-aim="x:ops.flipH"', IC.flipH, 'Flip Horizontal')}${ib('data-aim="x:ops.flipV"', IC.flipV, 'Flip Vertical')}<span style="flex:1 1 auto"></span><button class="aip-btn" type="button" data-aim="x:ops.move" title="Move… (⇧⌘M)">Move…</button><button class="aip-btn" type="button" data-aim="x:ops.scale" title="Scale…">Scale…</button></div>`
      + `<div class="aip-note" data-aip-none>Select an object to transform it.</div></div></div>`);
    // ALIGN
    B.align = mk('align', `<div class="aip-pp"><div class="aip-sub">Align Objects:</div>${ALIGN_BTNS()}<div class="aip-sub">Distribute Objects:</div>${DIST_BTNS()}`
      + `<div class="aip-row" style="margin-top:12px;align-items:flex-end"><div style="flex:1 1 auto;min-width:0"><div class="aip-sub" style="margin-top:0">Distribute Spacing:</div><div class="aip-row">${ib('data-aim="di:vspace"', IC.spV, 'Vertical Distribute Space')}${ib('data-aim="di:hspace"', IC.spH, 'Horizontal Distribute Space')}${field('gap', '', 'Spacing, mm: empty spreads them evenly between the outermost two; a value is counted from the key object (or the first)', { ph: 'Auto' })}</div></div>`
      + `<div style="flex:none"><div class="aip-sub" style="margin-top:0">Align To:</div><button class="aip-dd" type="button" data-aim="altomenu" style="width:78px" title="Align To"><span data-aip-alto>Auto</span>${IC.caret}</button></div></div>`
      + `<div class="aip-row" data-aip-keyrow hidden><span class="aip-lab">Key Object</span><select class="aip-in" data-aip-key aria-label="Key Object"></select></div>`
      + `<div class="aip-note" data-aip-altonote></div></div>`);
    // PATHFINDER
    B.pathfinder = mk('pathfinder', `<div class="aip-pp"><div class="aip-sub">Shape Modes:</div>${pfBtns(PF_SHAPE)}<div class="aip-sub">Pathfinders:</div>${pfBtns(PF_FIND)}<div class="aip-note">Curves stay curves. Each is one step: ⌘Z takes it back.</div></div>`);
    // STROKE
    B.stroke = mk('stroke', `<div class="aip-pp"><div class="aip-row" style="margin-top:8px"><span class="aip-lab">Weight:</span>${field('lw', '', 'Stroke weight: the line width on the charm, mm, as the laser file writes it (↑ ↓ 0.01 mm, Shift 0.05 mm)', { ddc: 'lwmenu' })}<button class="aip-ib" type="button" data-aim="lw0" title="Back to the width it came with" aria-label="Back to the width it came with">${IC.stroke}</button></div>`
      + `<div class="aip-row"><span class="aip-lab">Cap:</span><div class="aip-ibs">${ib('disabled', IC.capButt, 'Butt Cap (the laser draws round ends)')}${ib('', IC.capRound, 'Round Cap: the laser draws round ends', 'is-on')}${ib('disabled', IC.capProj, 'Projecting Cap (the laser draws round ends)')}</div></div>`
      + `<div class="aip-row"><span class="aip-lab">Corner:</span><div class="aip-ibs">${ib('disabled', IC.joinMiter, 'Miter Join (the laser draws round corners)')}${ib('', IC.joinRound, 'Round Join: the laser draws round corners', 'is-on')}${ib('disabled', IC.joinBevel, 'Bevel Join (the laser draws round corners)')}</div></div>`
      + `<div class="aip-row"><span class="aip-lab">Align Stroke:</span><div class="aip-ibs">${ib('', IC.stCenter, 'Align Stroke to Center: the laser follows the line', 'is-on')}${ib('disabled', IC.stInside, 'Align Stroke to Inside (not for a laser)')}${ib('disabled', IC.stOutside, 'Align Stroke to Outside (not for a laser)')}</div></div>`
      + `<div class="aip-note" data-aip-none>Select a line, a cut or a line engraving to set its weight.</div></div>`);
    // SWATCHES: our engraving colours (the roles)
    B.swatches = mk('swatches', `<div class="aip-sws" role="listbox" aria-label="Swatches">${D.ROLES.map(r => `<button class="aip-swc" type="button" data-aim="role:${r}" title="${esc(D.ROLE_NAME[r] + ' · ' + D.ROLE_SAYS[r])}" aria-label="${esc(D.ROLE_NAME[r])}"><i class="aip-sw${r === 'cut' || r === 'line' ? ' is-stroke' : ''}" style="--c:var(--aip-c-${r})"></i><b>${esc(D.ROLE_NAME[r])}</b></button>`).join('')}</div>`
      + `<div class="aip-pp" style="flex:none;padding-top:0"><div class="aip-note">Our colours are what the laser does: a swatch clicked sets it for what is selected (or for the next part drawn).</div></div>`);
    // CHARACTER
    B.character = mk('character', `<div class="aip-pp"><div class="aip-sec" style="border:0">${charHtml(true)}</div><div class="aip-sec"><div class="aip-sec__h"><span>Paragraph</span></div>${paraHtml}</div><div class="aip-note" data-aip-none>Select words, or pick the Type tool, to set their character.</div></div>`);
    // IMAGE TRACE
    B.trace = mk('trace', `<div class="aip-pp"><div class="aip-kv" data-aip-trace></div><div class="aip-row" style="margin-top:12px"><span style="flex:1 1 auto"></span><button class="aip-btn" type="button" data-aim="import" data-aip-place title="Place an image (PNG, JPG, WebP): it is traced as it comes in">Place Image…</button><button class="aip-btn is-cta" type="button" data-aim="x:imageRetrace" title="Trace: the Image step on this picture, its settings, then Place">Trace…</button></div><div class="aip-note" data-aip-none>Select a traced picture to see its settings, or place an image.</div></div>`);
    return B;
  }

  /* ════ THE LAYERS ROWS ════ */
  const roleOf = (D, it) => { if (it.kind !== 'group') return it.role; const rs = new Set(); D.model.walk(it.children || [], c => { if (c.kind !== 'group') rs.add(c.role); }); return rs.size === 1 ? [...rs][0] : rs.size ? 'mixed' : it.role; };
  const lcOf = role => `var(--aip-lc-${['cut', 'line', 'hatch', 'art'].includes(role) ? role : 'mixed'})`;
  /** "Outline · kept: on a sheet" → ["Outline", "kept: on a sheet"]: the platform's note becomes a badge on the row. */
  const splitName = s => { const m = /^(.*?)\s·\s(kept:.*)$/.exec(String(s)); return m ? [m[1], m[2]] : [String(s), '']; };
  const hasLook = it => !!((it.weightMm && Math.abs(it.weightMm) > 1e-9) || it.warp || it.envelope || it.blend || it.tracking || it.pathD || it.clip);
  const thumbCache = new WeakMap();
  /** A row's thumbnail: its own drawing in its own frame, the parts in the laser's colours on white (Illustrator's thumbnail); a very large drawing shows its box. */
  function thumbOf(D, it) {
    const hit = thumbCache.get(it); if (hit != null) return hit;
    const M = D.model, geo = D.geo;
    let out = '';
    try {
      const b = geo.drawn(it, M.I);
      if (b && fin(b[0])) {
        const w = Math.max(b[2] - b[0], 1e-3), h = Math.max(b[3] - b[1], 1e-3), s = Math.max(w, h) * 1.12, cx = (b[0] + b[2]) / 2, cy = (b[1] + b[3]) / 2;
        let body = '', budget = 24000, n = 0;
        const leaf = (lf, W) => {
          if (budget <= 0 || lf.hidden) return;
          const d = M.dOf(geo.raw(lf).subpaths || []); if (!d) return;
          budget -= d.length; if (budget < 0 && n) return; n++;
          const cls = `t-${['cut', 'line', 'hatch', 'art'].includes(lf.role) ? lf.role : 'art'}${M.paintFill(lf) ? ' is-f' : ''}${M.paintStroke(lf) ? ' is-s' : ''}${lf.evenOdd || lf.even || lf.fillRule === 'evenodd' ? ' is-eo' : ''}`;
          const g = D.greyOf(lf);
          body += `<path class="${cls}" d="${d}" transform="matrix(${W.map(v => +v.toFixed(5)).join(' ')})"${g ? ` style="fill:${g};stroke:none"` : ''}/>`;
        };
        const rec = (x, W) => { if (x.kind === 'group') { for (const c of x.children || []) rec(c, M.mul(W, geo.matrix(c))); } else leaf(x, W); };
        rec(it, geo.matrix(it));
        if (budget < 0) body = `<rect x="${b[0]}" y="${b[1]}" width="${w}" height="${h}" fill="#d9d9d9"/>` + body;
        out = `<svg viewBox="${(cx - s / 2).toFixed(3)} ${(cy - s / 2).toFixed(3)} ${s.toFixed(3)} ${s.toFixed(3)}" preserveAspectRatio="xMidYMid meet" aria-hidden="true">${body}</svg>`;
      }
    } catch (e) { out = ''; }
    thumbCache.set(it, out);
    return out;
  }
  function rowsHtml(P, its) {
    const D = P.D, S = D.S;
    let h = '';
    const row = (it, depth) => {
      const isG = it.kind === 'group', opn = isG && S.open.has(it.id), role = roleOf(D, it);
      const [nm, kept] = splitName(D.rowName(it));
      const says = `${nm} · ${D.ROLE_NAME[role] || (role === 'mixed' ? 'Mixed roles' : '')}${kept ? ' · ' + kept : ''}${it.source === 'added' ? ' · added here' : ''}`;
      h += `<div class="ce-layer aip-row${it.hidden ? ' is-hidden' : ''}${it.locked ? ' is-locked' : ''}${isG ? ' is-group' : ''}" data-id="${esc(it.id)}" role="option" aria-selected="false" tabindex="-1" style="--depth:${depth};--lc:${lcOf(role)}">`
        + `<button class="ce-layer__b aip-eye" type="button" data-lact="eye" aria-label="${it.hidden ? 'Show' : 'Hide'}" aria-pressed="${!it.hidden}" title="${esc(D.eyeSays(it))}" data-aip-tip="⌥-click: hide or show the others · ⌘-click: Outline view (⌘Y)">${it.hidden ? '' : IC.eye}</button>`
        + `<button class="ce-layer__b aip-lock" type="button" data-lact="lock" aria-label="${it.locked ? 'Unlock' : 'Lock'}" aria-pressed="${!!it.locked}" title="${it.locked ? 'Unlock' : 'Lock'}" data-aip-tip="⌥-click: lock or unlock the others">${it.locked ? IC.lock : ''}</button>`
        + `<span class="aip-rb"><i class="aip-lc" aria-hidden="true"></i>`
        + (isG ? `<button class="ce-layer__tw aip-tw" type="button" data-lact="twist" aria-label="${opn ? 'Collapse' : 'Expand'}" aria-expanded="${opn}" title="${opn ? 'Collapse' : 'Expand'} · ⌥-click: everything inside">${opn ? IC.triD : IC.triR}</button>` : '<span class="aip-tw0"></span>')
        + `<span class="aip-thumb${isG ? ' ce-layer__fold' : ''}" aria-hidden="true">${P.prefs.thumbs ? thumbOf(D, it) : ''}</span>`
        + `<span class="ce-layer__name" title="${esc(says)}">${esc(nm)}</span>`
        + (kept ? `<i class="aip-bdg" title="${esc(kept)}">${IC.pin}</i>` : '')
        + (it.source === 'added' ? '<i class="ce-layer__new" title="Added here"></i>' : '')
        + '</span>'
        + `<button class="aip-target${hasLook(it) ? ' is-app' : ''}" type="button" data-lact="target" title="Click to target${hasLook(it) ? ' · it has an appearance of its own: a weight, letter spacing, a warp or a path' : ''}" aria-label="Target ${esc(nm)}"><i></i></button>`
        + `<button class="aip-selsq" type="button" data-lact="sel" title="Click to select · drag the square to another layer to move the selection there" aria-label="Select ${esc(nm)}"><i></i></button>`
        + '</div>';
      if (opn) for (let i = it.children.length - 1; i >= 0; i--) row(it.children[i], depth + 1);
    };
    for (let i = its.length - 1; i >= 0; i--) row(its[i], 0);
    if (P.count) { const n = its.length; P.count.textContent = `${n} Layer${n === 1 ? '' : 's'}`; P.count.title = `${n} on the ${S.face === 'back' ? 'back' : 'front'} (the artboard)`; }
    return h;
  }
  /** The selection painted on (the editor sets is-sel): the small square on a layer that holds what is selected. */
  function paint(P) {
    const D = P.D, S = D.S, anc = new Set();
    for (const id of S.sel) { const e = D.entry(id); if (e) for (const a of e.anc || []) anc.add(a); }
    for (const r of D.layersEl.querySelectorAll('.ce-layer')) r.classList.toggle('aip-has-sel', anc.has(r.dataset.id));
    const n = S.sel.length, loc = P.bar.querySelector('[data-aim="locate"]'); if (loc) loc.disabled = !n;
    const clip = P.bar.querySelector('[data-aim="clip"]'); if (clip) { const un = D.xOn('cx.unclip'), mk = D.xOn('cx.clip'); clip.disabled = !(un || mk); clip.title = un ? 'Release Clipping Mask (⌥⌘7)' : 'Make Clipping Mask (⌘7)'; }
  }

  /* ════ THE DOCK: layout, groups, the icon strip, the fly-out ════ */
  const groupAt = (L, k) => (k && k[0] === 'm' ? L.main[+k.slice(1)] : k && k[0] === 's' ? L.strip[+k.slice(1)] : null);
  function modeOf(P) {
    const w = root.innerWidth || 1280;
    if (w <= DRAWER) return 'drawer';
    if (w <= NARROW) return P.narrowOpen ? 'full' : 'narrow';
    return P.layout.collapsed ? 'collapsed' : 'full';
  }
  function saveLayout(P) { lsSet(LS_LAYOUT, JSON.stringify(P.layout)); }
  function savePrefs(P) { lsSet(LS_PREFS, JSON.stringify(P.prefs)); }
  const tabsHtml = (g, key) => `<div class="aip-tabs" role="tablist" data-aip-tabs="${key}">${g.tabs.map(id => `<button class="aip-tab${id === g.active ? ' is-on' : ''}" type="button" role="tab" aria-selected="${id === g.active}" data-aip-tab="${id}" title="${esc(TITLE[id] + (WKEY[id] ? ' (' + WKEY[id] + ')' : '') + ' · double-click: minimise · drag: move the panel')}">${esc(TITLE[id])}</button>`).join('')}`
    + `<span class="aip-tabs__fill"></span><button class="aip-pmenu" type="button" data-aim="pmenu" data-aip-for="${g.active}" aria-label="${esc(TITLE[g.active])} panel menu" title="Panel menu">${IC.menu}</button></div>`;
  function render(P) {
    const D = P.D, L = P.layout, mode = P.mode = modeOf(P);
    const icons = mode === 'collapsed' || mode === 'narrow';
    const dk = D.dock;
    dk.classList.toggle('is-icons', icons); dk.classList.toggle('is-labels', mode === 'collapsed'); dk.classList.toggle('is-drawer', mode === 'drawer');
    const flyWas = P.fly && !P.fly.hidden ? P.flyFor : null;
    closeFly(P, true);
    for (const id of ALL) if (P.bodies[id]) P.store.appendChild(P.bodies[id]);
    // the expanded column: the group holding Layers fills what is left (or the last open one)
    let fillAt = L.main.findIndex(g => !g.min && g.active === 'layers'); if (fillAt < 0) fillAt = L.main.findIndex(g => !g.min && g.tabs.includes('layers'));
    if (fillAt < 0) for (let i = L.main.length - 1; i >= 0; i--) if (!L.main[i].min) { fillAt = i; break; }
    P.main.innerHTML = icons ? '' : L.main.map((g, i) => `<section class="aip-grp${g.min ? ' is-min' : ''}${i === fillAt ? ' is-fill' : ''}" data-aip-g="m${i}" aria-label="${esc(TITLE[g.active])}">${tabsHtml(g, 'm' + i)}<div class="aip-gbody"></div></section>`).join('');
    if (!icons) L.main.forEach((g, i) => { if (!g.min) { P.main.querySelector(`[data-aip-g="m${i}"] .aip-gbody`).appendChild(P.bodies[g.active]); extMount(P, g.active); } });
    // the icon strip: the collapsed groups (all of them when the dock is collapsed), then the extension panels
    const sgs = (icons ? L.main.map((g, i) => ['m' + i, g]) : []).concat(L.strip.map((g, i) => ['s' + i, g]));
    const xps = D.X.panels || [];
    P.strip.innerHTML = sgs.map(([k, g]) => `<div class="aip-sg" data-aip-sg="${k}">${g.tabs.map(id => `<button class="aip-ico" type="button" data-aip-ico="${id}" data-aip-src="${k}" title="${esc(TITLE[id] + (WKEY[id] ? ' (' + WKEY[id] + ')' : ''))}" aria-label="${esc(TITLE[id])}">${PICON(id)}<span>${esc(TITLE[id])}</span></button>`).join('')}</div>`).join('')
      + (xps.length ? `<div class="aip-sg" data-aip-sg="x">${xps.map(p => `<button class="aip-ico" type="button" data-aip-xp="${esc(p.id)}" title="${esc(p.title || p.id)}" aria-label="${esc(p.title || p.id)}">${typeof p.icon === 'string' && /^\s*<svg/i.test(p.icon) ? p.icon : IC.xpanel}<span>${esc(p.title || p.id)}</span></button>`).join('')}</div>` : '');
    P.strip.classList.toggle('is-empty', !sgs.length && !xps.length);
    P.collapseBtn.innerHTML = icons ? IC.expand : IC.collapse;
    P.collapseBtn.title = icons ? 'Expand Panels' : 'Collapse to Icons';
    P.collapseBtn.setAttribute('aria-label', P.collapseBtn.title);
    if (flyWas) { const k = sgs.find(([key, g]) => g.tabs.includes(flyWas)); if (k) openFly(P, k[0], flyWas); }
    paintStrip(P);
    sync(P);
  }
  function paintStrip(P) {
    const on = P.fly && !P.fly.hidden ? P.flyFor : null, xon = P.D.xPanelOn();
    for (const b of P.strip.querySelectorAll('[data-aip-ico]')) b.classList.toggle('is-on', b.dataset.aipIco === on);
    for (const b of P.strip.querySelectorAll('[data-aip-xp]')) b.classList.toggle('is-on', b.dataset.aipXp === xon);
  }
  /** A collapsed panel popped out to the left of its icon, its group's tabs above it. */
  function openFly(P, key, id) {
    const g = groupAt(P.layout, key); if (!g || !g.tabs.includes(id)) return;
    g.active = id;
    for (const b of P.fly.querySelectorAll('.aip-pb')) P.store.appendChild(b);
    P.fly.innerHTML = `<section class="aip-grp${id === 'layers' ? ' has-layers' : ''}" data-aip-g="${key}" aria-label="${esc(TITLE[id])}">${tabsHtml(g, key)}<div class="aip-gbody"></div></section>`;
    P.fly.querySelector('.aip-gbody').appendChild(P.bodies[id]); extMount(P, id);
    P.fly.classList.toggle('is-swlist', !!P.prefs.swList);
    P.fly.hidden = false; P.flyFor = id; P.flyKey = key;
    const ico = P.strip.querySelector(`[data-aip-ico="${id}"]`), dr = P.D.dock.getBoundingClientRect();
    const ir = ico ? ico.getBoundingClientRect() : dr, fh = P.fly.getBoundingClientRect().height;
    P.fly.style.top = Math.max(2, Math.min(ir.top - dr.top, dr.height - fh - 2)) + 'px';
    paintStrip(P); sync(P);
    if (id === 'layers') { P.D.renderLayers(true); P.D.paintLayerSel(true); }
  }
  function closeFly(P, quiet) {
    if (!P.fly || P.fly.hidden) return;
    for (const b of P.fly.querySelectorAll('.aip-pb')) P.store.appendChild(b);
    P.fly.hidden = true; P.flyFor = null; P.fly.innerHTML = '';
    if (!quiet) paintStrip(P);
  }
  /** A panel brought into view: its tab in the column (un-minimised), or its fly-out. (The shell's Window menu: CharmEditor.aiDock.openPanel.) */
  function openPanel(P, id) {
    if (!ALL.includes(id)) return false;
    const L = P.layout, icons = P.mode === 'collapsed' || P.mode === 'narrow';
    const mi = L.main.findIndex(g => g.tabs.includes(id));
    if (P.D.dock.hidden) { P.D.S.dock = true; P.D.layoutDock(); }
    if (mi >= 0 && !icons) { L.main[mi].active = id; L.main[mi].min = false; saveLayout(P); render(P); if (id === 'layers') P.D.paintLayerSel(true); return true; }
    const key = mi >= 0 ? 'm' + mi : 's' + L.strip.findIndex(g => g.tabs.includes(id));
    openFly(P, key, id); return true;
  }
  function layoutChanged(P) { saveLayout(P); render(P); P.D.layoutDock(); }   // (the editor's layoutDock refits the work area to the dock's new width)

  /* ── a tab or an icon dragged to another group, to the strip or to the column (Illustrator's docking) ── */
  function moveTab(P, id, toKey, beforeId) {
    const L = P.layout, from = [...L.main.map((g, i) => ['m' + i, g]), ...L.strip.map((g, i) => ['s' + i, g])].find(([, g]) => g.tabs.includes(id));
    if (!from) return;
    let dest = toKey === 'new-main' ? { tabs: [], active: id, min: false } : toKey === 'new-strip' ? { tabs: [], active: id } : groupAt(L, toKey);
    if (!dest) return;
    const src = from[1];
    if (dest === src && src.tabs.length === 1) return;
    src.tabs = src.tabs.filter(t => t !== id);
    if (src.active === id) src.active = src.tabs[0];
    const at = beforeId && dest.tabs.includes(beforeId) ? dest.tabs.indexOf(beforeId) : dest.tabs.length;
    dest.tabs.splice(at, 0, id); dest.active = id;
    if (toKey === 'new-main') L.main.push(dest); else if (toKey === 'new-strip') L.strip.push(dest);
    L.main = L.main.filter(g => g.tabs.length); L.strip = L.strip.filter(g => g.tabs.length);
    layoutChanged(P);
  }
  function wireTabDrag(P) {
    const D = P.D;
    let drag = null;
    const clear = () => { for (const el of D.dock.querySelectorAll('.aip-drop')) el.classList.remove('aip-drop'); };
    const targetAt = (x, y) => {
      const el = doc.elementFromPoint(x, y); if (!el || !el.closest) return null;
      const tabs = el.closest('[data-aip-tabs]'); if (tabs && D.dock.contains(tabs)) { const t = el.closest('[data-aip-tab]'); return { el: tabs, key: tabs.dataset.aipTabs, before: t ? t.dataset.aipTab : null }; }
      const sg = el.closest('[data-aip-sg]'); if (sg && sg.dataset.aipSg !== 'x') { const t = el.closest('[data-aip-ico]'); return { el: sg, key: sg.dataset.aipSg, before: t ? t.dataset.aipIco : null }; }
      if (el.closest('.aip-strip')) return { el: P.strip, key: 'new-strip' };
      if (el.closest('.aip-main')) return { el: P.main, key: 'new-main' };
      return null;
    };
    const move = e => {
      if (!drag) return;
      if (!drag.live) { if (Math.hypot(e.clientX - drag.x0, e.clientY - drag.y0) < 7) return; drag.live = true; drag.ghost = doc.createElement('div'); drag.ghost.className = 'aip-tabghost'; drag.ghost.textContent = TITLE[drag.id]; D.rootEl.appendChild(drag.ghost); closeFly(P); }
      drag.ghost.style.left = (e.clientX + 10) + 'px'; drag.ghost.style.top = (e.clientY + 8) + 'px';
      clear(); const t = targetAt(e.clientX, e.clientY); drag.t = t; if (t) t.el.classList.add('aip-drop');
    };
    const up = () => {
      const d = drag; drag = null; doc.removeEventListener('pointermove', move, true); doc.removeEventListener('pointerup', up, true); doc.removeEventListener('pointercancel', up, true);
      clear(); if (!d || !d.live) return;
      if (d.ghost) d.ghost.remove();
      P.dragged = Date.now();
      if (d.t) moveTab(P, d.id, d.t.key, d.t.before);
    };
    D.dock.addEventListener('pointerdown', e => {
      if (e.button) return;
      const t = e.target.closest('[data-aip-tab],[data-aip-ico]'); if (!t) return;
      drag = { id: t.dataset.aipTab || t.dataset.aipIco, x0: e.clientX, y0: e.clientY, live: false };
      doc.addEventListener('pointermove', move, true); doc.addEventListener('pointerup', up, true); doc.addEventListener('pointercancel', up, true);
    });
  }

  /* ════ SYNC: every panel's controls from the work and the selection, at most once a frame ════ */
  function selState(P) {
    const D = P.D, S = D.S, es = D.selEntries(), leaves = D.selLeaves().filter(l => !l.locked), texts = leaves.filter(l => l.kind === 'text');
    const tool = String(S.tool || ''), shapeTool = ['rect', 'ellipse', 'line', 'polygon'].includes(tool) || /^shape-/.test(tool);
    return { es, n: es.length, leaves, texts, tool, newText: tool === 'text' && !es.length, newShape: shapeTool && !es.length, s: D.sizeRead() };
  }
  function kindOf(P, I) {
    const D = P.D;
    if (!I.n) return I.newText ? 'Type Tool' : I.newShape ? 'Shape Tool' : 'No Selection';
    const k = new Set(I.es.map(e => (e.it.kind === 'path' && (e.it.subpaths || []).length > 1 ? 'compound' : e.it.kind)));
    if (D.xOn('imageRetrace') && I.es.every(e => e.it.kind === 'group')) return 'Image Tracing';
    if (k.size > 1) return 'Mixed Objects';
    const one = [...k][0];
    return one === 'text' ? 'Type' : one === 'group' ? 'Group' : one === 'compound' ? 'Compound Path' : 'Path';
  }
  const setVal = (inp, v) => { if (doc.activeElement === inp) return; inp.value = v; inp.__v0 = v; };
  function fieldsTo(P, r) {
    const D = P.D, b = r ? (P.liveBox || D.selBox()) : null, ref = refOf(P);
    for (const inp of P.root.querySelectorAll('[data-aif="x"],[data-aif="y"],[data-aif="w"],[data-aif="h"],[data-aif="r"]')) {
      const k = inp.dataset.aif; inp.disabled = !r;
      if (!r) { setVal(inp, ''); continue; }
      let v = r[k];
      if (k === 'x' || k === 'y') { const span = b ? (k === 'x' ? b[2] - b[0] : b[3] - b[1]) * MM : (k === 'x' ? r.w : r.h); v = r[k] + (k === 'x' ? ref[0] : ref[1]) * span; }
      setVal(inp, k === 'r' ? (r.r == null ? '' : trim(r.r, 2) + '°') : D.fmtMm(v) + ' mm');
    }
  }
  function sync(P) {
    if (!P.root || P.dead) return;
    const D = P.D, S = D.S, I = selState(P), R = P.root;
    const showText = I.texts.length > 0 || I.newText;
    const strokes = I.leaves.filter(l => D.paintStroke(l)), weighs = I.leaves.filter(l => D.weighable(l));
    const showLw = strokes.length > 0 || I.newShape || (I.newText && (S.textRole === 'line' || S.textRole === 'cut'));
    const showWt = D.canWeigh() && (weighs.length > 0 || (I.newText && !(S.textRole === 'line' || S.textRole === 'cut')));
    const ops = root.CharmEditorOps && root.CharmEditorOps.ops;
    // which sections Properties shows
    const vis = { transform: !!I.s, appearance: I.leaves.length > 0 || I.newText || I.newShape, character: showText, paragraph: showText, trace: D.xOn('imageRetrace'),
      pathfinder: I.n >= 2 && D.xHas('ops.unite'), align: I.n >= 1 && !!ops, quick: false, doc: !I.n && !I.newText && !I.newShape };
    vis.rulers = vis.guides = vis.snap = vis.docqa = vis.doc;
    // Quick Actions: only what this page has, only what can run now
    const qa = [];
    if (I.n) {
      const x = (id, label) => { if (D.xOn(id)) qa.push(`<button class="aip-btn" type="button" data-aim="x:${id}" title="${esc(label)}">${esc(label)}</button>`); };
      if (I.n >= 2) qa.push('<button class="aip-btn" type="button" data-aim="group" title="Group (⌘G)">Group</button>');
      if (I.es.some(e => e.it.kind === 'group')) qa.push('<button class="aip-btn" type="button" data-aim="ungroup" title="Ungroup (⇧⌘G)">Ungroup</button>');
      x('ops.offset', 'Offset Path…'); x('ops.outline', 'Outline Stroke'); x('cx.onPath', 'Type on a Path'); x('cx.offPath', 'Release Type on Path');
      x('cx.clip', 'Make Clipping Mask'); x('cx.unclip', 'Release Clipping Mask'); x('ops.contour', 'Cut Line Around Art…'); x('cx.jumpRing', 'Jump Ring on Top');
      qa.push(`<button class="aip-btn" type="button" data-aim="arrangemenu" title="Arrange">Arrange ${IC.caret}</button>`);
    }
    const qaEl = R.querySelector('[data-aip-qa]'), qaH = qa.join('');
    if (qaEl && qaEl.__h !== qaH) { qaEl.innerHTML = qaH; qaEl.__h = qaH; }
    vis.quick = qa.length > 0;
    for (const s of R.querySelectorAll('[data-aip-sec]')) s.hidden = !vis[s.dataset.aipSec];
    for (const k of R.querySelectorAll('[data-aip-kind]')) k.textContent = kindOf(P, I);
    // the Transform fields, the reference point, the link
    fieldsTo(P, I.s);
    for (const b of R.querySelectorAll('[data-aim^="ref:"]')) { const [fx, fy] = b.dataset.aim.slice(4).split(',').map(Number), on = fx === refOf(P)[0] && fy === refOf(P)[1]; b.classList.toggle('is-on', on); b.setAttribute('aria-checked', String(on)); }
    for (const b of R.querySelectorAll('.aip-link')) { b.classList.toggle('is-on', !!S.link); b.setAttribute('aria-pressed', String(!!S.link)); }
    // Appearance: Fill and Stroke as our roles, the line width, the weight
    const roles = new Set(I.leaves.map(l => l.role)), role = I.leaves.length ? (roles.size === 1 ? [...roles][0] : 'mixed') : (S.tool === 'text' ? S.textRole : S.shapeRole);
    const anyFill = I.leaves.length ? I.leaves.some(l => D.paintFill(l)) : role === 'hatch', anyStroke = I.leaves.length ? strokes.length > 0 : role !== 'hatch';
    const swPaint = (btn, on, stroke) => { if (!btn) return; const sw = btn.querySelector('.aip-sw'), sp = btn.querySelector('span'); sw.className = 'aip-sw' + (stroke ? ' is-stroke' : '') + (!on ? ' is-none' : role === 'mixed' ? ' is-mixed' : ''); sw.style.setProperty('--c', `var(--aip-c-${role})`); sp.textContent = !on ? 'None' : role === 'mixed' ? 'Mixed' : D.ROLE_SHORT[role] || ''; };
    for (const b of R.querySelectorAll('[data-aip-fill]')) swPaint(b, anyFill, false);
    for (const b of R.querySelectorAll('[data-aip-stroke]')) swPaint(b, anyStroke, true);
    for (const el of R.querySelectorAll('[data-aip-when="lw"]')) el.hidden = !showLw;
    for (const el of R.querySelectorAll('[data-aip-when="wt"]')) el.hidden = !showWt;
    const lw = strokes.length ? D.lwOf(strokes[0]) : S.lw, lwSame = strokes.every(l => Math.abs(D.lwOf(l) - lw) < 1e-9);
    for (const inp of R.querySelectorAll('[data-aif="lw"]')) { inp.disabled = !showLw; setVal(inp, !showLw ? '' : lwSame ? D.fmtLw(lw) + ' mm' : ''); inp.placeholder = lwSame ? '' : 'Mixed'; }
    const wt = weighs.length ? D.weightOf(weighs[0]) : S.textWeight, wtSame = weighs.every(l => Math.abs(D.weightOf(l) - wt) < 1e-9);
    for (const inp of R.querySelectorAll('[data-aif="wt"]')) { inp.disabled = !showWt; setVal(inp, !showWt ? '' : wtSame ? D.fmtWt(wt) + ' mm' : ''); inp.placeholder = wtSame ? '' : 'Mixed'; }
    // Character
    const fk = I.texts.length ? I.texts[0].fontKey : S.textFont, fSame = I.texts.every(t => t.fontKey === fk), fo = D.fonts.find(f => f.key === fk);
    for (const el of R.querySelectorAll('[data-aip-font]')) { el.textContent = !showText ? '' : fSame ? (fo ? fo.name : String(fk == null ? 'Font' : fk)) : 'Mixed'; el.style.fontFamily = fSame && fo && fo.family ? fo.family : ''; }
    const st = I.texts.length ? null : S.textStyle || {}, allOn = k => (st ? !!st[k] : I.texts.length > 0 && I.texts.every(t => !!t[k]));
    const bold = allOn('bold'), ital = allOn('italic');
    for (const el of R.querySelectorAll('[data-aip-fstyle]')) el.textContent = !showText ? '' : bold && ital ? 'Bold Italic' : bold ? 'Bold' : ital ? 'Italic' : 'Regular';
    const sz = I.texts.length ? num(I.texts[0].sizePt, 10) : S.textSize, szSame = I.texts.every(t => Math.abs(num(t.sizePt, 10) - sz) < 1e-6);
    for (const inp of R.querySelectorAll('[data-aif="pt"]')) { inp.disabled = !showText; setVal(inp, showText && szSame ? trim(sz, 2) + ' pt' : ''); }
    const tr0 = I.texts.length ? num(I.texts[0].tracking, 0) : 0, trSame = I.texts.every(t => num(t.tracking, 0) === tr0);
    for (const inp of R.querySelectorAll('[data-aif="track"]')) { inp.disabled = !I.texts.length; setVal(inp, I.texts.length && trSame ? String(tr0) : ''); }
    const lg = I.texts.length ? num(I.texts[0].lineGap, 0) : 0, lgSame = I.texts.every(t => num(t.lineGap, 0) === lg);
    for (const inp of R.querySelectorAll('[data-aif="lead"]')) { inp.disabled = !I.texts.length; setVal(inp, I.texts.length && lgSame && lg > 0 ? trim(lg * sz, 2) + ' pt' : ''); }
    const cs = I.texts.length ? (I.texts.every(t => (t.letterCase || '') === (I.texts[0].letterCase || '')) ? I.texts[0].letterCase || '' : null) : (st && st.letterCase) || '';
    for (const b of R.querySelectorAll('[data-aim^="case:"]')) { const on = showText && cs != null && b.dataset.aim.slice(5) === cs; b.classList.toggle('is-on', on); b.setAttribute('aria-checked', String(on)); b.disabled = !showText; }
    const al = I.texts.length ? (I.texts.every(t => (t.align || 'left') === (I.texts[0].align || 'left')) ? I.texts[0].align || 'left' : null) : (st && st.align) || 'left';
    for (const b of R.querySelectorAll('[data-aim^="talign:"]')) { const on = showText && b.dataset.aim.slice(7) === al; b.classList.toggle('is-on', on); b.setAttribute('aria-checked', String(on)); b.disabled = !showText; }
    for (const b of R.querySelectorAll('[data-aim="alphafind"],[data-aim="fontmenu"],[data-aim="stylemenu"],[data-aim="ptmenu"]')) b.disabled = !showText;
    for (const b of R.querySelectorAll('[data-aim="trackmenu"]')) b.disabled = !I.texts.length;
    // the commands' buttons: there only when this page has the command, lit when it can run
    for (const b of R.querySelectorAll('[data-aim^="x:"]')) { const id = b.dataset.aim.slice(2); b.hidden = !D.xHas(id); b.disabled = !D.xOn(id); }
    for (const b of R.querySelectorAll('[data-aim="group"]')) b.disabled = I.n < 2;
    for (const b of R.querySelectorAll('[data-aim="ungroup"]')) b.disabled = !I.es.some(e => e.it.kind === 'group');
    for (const b of R.querySelectorAll('[data-aim="lw0"],[data-aim="wt0"]')) b.disabled = b.dataset.aim === 'lw0' ? !showLw : !showWt;
    // Align: what can be aligned to what
    const to = P.prefs.alignTo || (I.n >= 2 ? 'selection' : 'charm');
    if (to === 'key' && !S.sel.includes(P.key)) P.key = S.sel.length ? S.sel[S.sel.length - 1] : null;
    const canA = !!ops && (to === 'selection' ? I.n >= 2 : to === 'key' ? I.n >= 2 && !!P.key : I.n >= 1);
    const gapSet = String(P.prefs.gap || '') !== '';
    for (const b of R.querySelectorAll('[data-aim^="al:"]')) b.disabled = !canA;
    for (const b of R.querySelectorAll('[data-aim^="di:"]')) b.disabled = !ops || I.n < (/space$/.test(b.dataset.aim) && gapSet ? 2 : 3);
    for (const el of R.querySelectorAll('[data-aip-alto]')) el.textContent = P.prefs.alignTo ? { selection: 'Selection', key: 'Key Object', charm: 'Artboard', centre: 'Centre' }[P.prefs.alignTo] : (I.n >= 2 ? 'Selection' : 'Artboard');
    for (const el of R.querySelectorAll('[data-aip-altonote]')) el.textContent = !ops ? 'Align is not on this page.' : to === 'charm' ? 'Artboard: the charm\'s outline, its edges and the middle of its box.' : to === 'centre' ? 'The charm\'s centre of area: where it balances (a heart\'s sits below its box\'s middle).' : to === 'key' ? 'The key object stays put; the rest come to it.' : '';
    for (const row of R.querySelectorAll('[data-aip-keyrow]')) {
      row.hidden = to !== 'key' || I.n < 2; if (row.hidden) continue;
      const sel = row.querySelector('select'), opts = I.es.map(e => `<option value="${esc(e.it.id)}"${e.it.id === P.key ? ' selected' : ''}>${esc(splitName(D.rowName(e.it))[0])}</option>`).join('');
      if (sel.__h !== opts) { sel.innerHTML = opts; sel.__h = opts; }
    }
    for (const inp of R.querySelectorAll('[data-aif="gap"]')) setVal(inp, P.prefs.gap ? P.prefs.gap + ' mm' : '');
    // Image Trace: the picture's settings (read: Trace… opens the Image step to change them)
    const im = root.CharmNestCeImage; let tset = null;
    try { tset = im && typeof im.traceSet === 'function' && I.n ? im.traceSet(D.ectx) : null; } catch (e) { tset = null; }
    const ts = tset && tset.settings ? tset.settings : null;
    const kvH = ts ? [['Preset', 'Custom'], ['View', 'Tracing Result'], ['Mode', ts.mode === 'line' ? 'Black and White' : 'Grayscale'], [ts.mode === 'line' ? 'Threshold' : 'Grays', ts.mode === 'line' ? 'Auto' : String(ts.levels || 4)],
      ['Paths', Math.round(num(ts.detail, 0.5) * 100) + '%'], ['Smoothness', Math.round(num(ts.smooth, 0.5) * 100) + '%'], ['Noise', num(ts.despeckle, 2) + ' px'], ['Invert', ts.invert ? 'On' : 'Off']]
      .map(([k, v]) => `<span>${esc(k)}:</span><b>${esc(v)}</b>`).join('') : tset ? '<span>Settings:</span><b>kept with the picture</b>' : '';
    for (const el of R.querySelectorAll('[data-aip-trace]')) if (el.__h !== kvH) { el.innerHTML = kvH; el.__h = kvH; }
    for (const b of R.querySelectorAll('[data-aip-place]')) b.hidden = !(D.X.importers || []).length;
    // the "select something" notes of the panels that need a selection
    const need = { transform: !I.s, stroke: !showLw, character: !showText, trace: !tset };
    for (const id of Object.keys(need)) { const b = P.bodies[id]; if (b) for (const el of b.querySelectorAll('[data-aip-none]')) el.hidden = !need[id]; }
    // the Document section: the artboards (faces), the toggles
    for (const b of R.querySelectorAll('[data-aim^="face:"]')) { const f = b.dataset.aim.slice(5); b.disabled = !S.faces[f]; b.classList.toggle('is-cta', f === S.face); }
    for (const b of R.querySelectorAll('[data-aim^="opt:"]')) { const on = !!S[b.dataset.aim.slice(4)]; b.classList.toggle('is-on', on); b.setAttribute('aria-checked', String(on)); }
    // Swatches: the role of what is selected
    for (const b of R.querySelectorAll('.aip-swc[data-aim^="role:"]')) b.classList.toggle('is-on', b.dataset.aim.slice(5) === role);
    D.dock.classList.toggle('is-swlist', !!P.prefs.swList);
    paint(P);
    extEach(P, 'update');
  }
  /* ── the panels other files add: a body each, mounted the first time it is shown, updated with the rest ── */
  function extBody(P, id) {
    if (P.bodies[id]) return;
    const b = doc.createElement('div'); b.className = 'aip-pb'; b.dataset.aipPanel = id;
    const el = doc.createElement('div'); el.className = 'aip-pp aip-xbody'; b.appendChild(el);
    b.__def = EXT.get(id); b.__el = el; P.bodies[id] = b; P.store.appendChild(b);
  }
  function extMount(P, id) {
    const b = P.bodies[id]; if (!b || !b.__def || b.__mounted) return;
    b.__mounted = true;
    try { b.__def.mount(P.D.ectx, b.__el); } catch (e) { try { root.console.error('CharmEditor.aiDock panel ' + id + '.mount:', e); } catch (_) { /* fine */ } }
  }
  function extEach(P, name) {
    for (const id of EXT.keys()) { const b = P.bodies[id]; if (!b || !b.__mounted || typeof b.__def[name] !== 'function') continue; try { b.__def[name](P.D.ectx); } catch (e) { try { root.console.error('CharmEditor.aiDock panel ' + id + '.' + name + ':', e); } catch (_) { /* fine */ } } }
  }
  function schedule(P) { if (P.raf || P.dead) return; P.raf = root.requestAnimationFrame(() => { P.raf = 0; sync(P); }); }

  /* ════ ACTIONS ════ */
  function applyTree(ctx, next, sel) {
    const cur = ctx.items();
    if (next && next !== cur) {
      const map = new Map(), n = Math.min(cur.length, next.length);
      for (let i = 0; i < n; i++) if (cur[i] !== next[i]) map.set(cur[i].id, next[i]);
      if (map.size) ctx.replaceItems(map);
      if (next.length > cur.length) ctx.addItems(next.slice(cur.length), { select: false });
      else if (next.length < cur.length) ctx.removeItems(cur.slice(next.length).map(it => it.id));
    }
    if (sel) ctx.select(sel);
  }
  /** Align and Distribute: charm-nest-ce-ops.js's own operations (the property bar's Align flyout runs the same), one step. */
  function alignRun(P, kind, how) {
    const D = P.D, O = root.CharmEditorOps && root.CharmEditorOps.ops, ctx = D.ectx; if (!O) return;
    const n = D.S.sel.length, to = P.prefs.alignTo || (n >= 2 ? 'selection' : 'charm');
    let r;
    if (kind === 'al') r = O.alignItems(ctx.items(), ctx.selection(), ctx.geo, how, { to, key: P.key, frame: ctx.frame() });
    else { const g = P.prefs.gap === '' ? null : numIn(P.prefs.gap); r = O.distributeItems(ctx.items(), ctx.selection(), ctx.geo, how, { gapMm: fin(g) ? g : null, key: (to === 'key' && P.key) || null }); }
    if (!r || r.error) { D.say(r ? r.error : 'Nothing happened', 3200); return; }
    applyTree(ctx, r.items, r.sel); ctx.commit(kind === 'al' ? 'Align' : 'Distribute');
  }
  /** Tracking or leading of the selected words (the extras' letter spacing does the same: retext, one step). */
  function setTextField(P, key, v) {
    const D = P.D, ctx = D.ectx, M = D.model, geo = D.geo;
    const texts = D.selLeaves().filter(l => l.kind === 'text' && !l.locked); if (!texts.length) return;
    const map = {};
    for (const it of texts) {
      if (key === 'tracking') { v = Math.max(-500, Math.min(2000, Math.round(v))); if (num(it.tracking, 0) === v) continue; const n = M.retext(it, { tracking: v }, geo); if (!v) delete n.tracking; map[it.id] = n; }
      else { const gap = fin(v) && v > 0 ? Math.round(v / num(it.sizePt, 10) * 1e4) / 1e4 : 0; if (num(it.lineGap, 0) === gap) continue; const n = M.retext(it, { lineGap: gap || undefined }, geo); if (!gap) delete n.lineGap; map[it.id] = n; }
    }
    if (Object.keys(map).length) { ctx.replaceItems(map); ctx.commit(key === 'tracking' ? 'Tracking' : 'Leading'); }
    else sync(P);
  }
  function commitAif(P, k, raw) {
    const D = P.D, v = numIn(raw);
    if (k === 'gap') { P.prefs.gap = fin(v) ? trim(v, 3) : ''; savePrefs(P); sync(P); return; }
    if (k === 'lead') { setTextField(P, 'lineGap', fin(v) ? v : 0); return; }
    if (!fin(v)) { sync(P); return; }
    if (k === 'x' || k === 'y') {
      const s = D.sizeRead(), b = D.selBox(); if (!s || !b) return;
      const f = k === 'x' ? refOf(P)[0] : refOf(P)[1], span = (k === 'x' ? b[2] - b[0] : b[3] - b[1]) * MM;
      D.commitField(k, v - f * span);
    } else if (k === 'w' || k === 'h' || k === 'r') D.commitField(k, v, refOf(P).slice());
    else if (k === 'pt') D.setTextSize(v);
    else if (k === 'lw') D.setStrokeMm(v);
    else if (k === 'wt') D.setWeightMm(v);
    else if (k === 'track') setTextField(P, 'tracking', v);
  }
  function wireField(P, inp) {
    const k = inp.dataset.aif;
    inp.addEventListener('focus', () => { inp.__v0 = inp.value; try { inp.select(); } catch (e) { /* fine */ } });
    inp.addEventListener('blur', () => { if (inp.__revert) { inp.__revert = false; sync(P); return; } if (inp.value !== inp.__v0) { inp.__v0 = inp.value; commitAif(P, k, inp.value); } });
    // (the editor hears every key first, on the window; a field of the dock gets Enter, Esc and the arrows through __ceKey, typing goes on as usual)
    inp.__ceKey = e => {
      if (e.key === 'Enter') { e.preventDefault(); if (inp.value !== inp.__v0) { inp.__v0 = inp.value; commitAif(P, k, inp.value); } root.requestAnimationFrame(() => { sync(P); try { inp.select(); } catch (er) { /* fine */ } }); return; }
      if (e.key === 'Escape') { e.preventDefault(); inp.__revert = true; inp.blur(); return; }
      if ((e.key === 'ArrowUp' || e.key === 'ArrowDown') && NUDGE[k]) {
        e.preventDefault();
        const st = NUDGE[k][e.shiftKey ? 1 : 0], v0 = numIn(inp.value), v = Math.round(((fin(v0) ? v0 : 0) + (e.key === 'ArrowUp' ? st : -st)) * 1000) / 1000;
        const unit = { x: ' mm', y: ' mm', w: ' mm', h: ' mm', lw: ' mm', wt: ' mm', gap: ' mm', r: '°', pt: ' pt', lead: ' pt', track: '' }[k] || '';
        inp.value = trim(v, 3) + unit; inp.__v0 = inp.value;
        if (k === 'lw') P.D.setStrokeMm(v, true); else if (k === 'wt') P.D.setWeightMm(v, true); else commitAif(P, k, v);
      }
    };
  }

  /* ── the menus: the shell's .ce-menu, placed and closed by the editor's own placeMenu / closeMenus ── */
  const mi = (label, attrs, on, key, tog) => `<button class="ce-item${tog ? ' ce-item--tog' + (tog === 2 ? ' is-on' : '') : ''}${on ? '' : ' is-off'}" type="button" role="${tog ? 'menuitemcheckbox' : 'menuitem'}"${tog ? ` aria-checked="${tog === 2}"` : ''} ${attrs}${on ? '' : ' disabled aria-disabled="true"'}>${tog ? '<i class="ce-tick"></i>' : ''}${esc(label)}${key ? `<em>${esc(key)}</em>` : ''}</button>`;
  const SEP = '<div class="ce-menu__sep"></div>';
  /** A shortcut as the shell's catalog says it, when it has one (CharmEditor.ai.catalog), else ours. */
  function catalogKey(id, ours) {
    try { const c = root.CharmEditor && root.CharmEditor.ai && root.CharmEditor.ai.catalog; const e = c && (typeof c.get === 'function' ? c.get(id) : c[id]); if (e && typeof e.key === 'string' && e.key) return e.key; } catch (e) { /* the shell's own */ }
    return ours || '';
  }
  const xmi = (D, id, label, key) => (D.xHas(id) ? mi(label, `data-aim="x:${id}"`, D.xOn(id), catalogKey(id, key)) : '');
  function openMenu(P, html, anchor, cls) {
    const D = P.D;
    D.closeMenus(); D.hideTip && D.hideTip();
    P.menu.className = 'ce-menu aip-menu' + (cls ? ' ' + cls : '');
    P.menu.innerHTML = html;
    P.menuFor = anchor;
    D.placeMenu(P.menu, anchor.getBoundingClientRect(), false);
  }
  function panelMenuHtml(P, id) {
    const D = P.D, S = D.S, n = S.sel.length, es = D.selEntries(), one = n === 1 ? es[0] : null;
    const nm = one ? `“${splitName(D.rowName(one.it))[0].slice(0, 24)}”` : n ? 'Selection' : '';
    let h = '';
    if (id === 'layers') {
      h = mi(`Duplicate ${nm}`.trim(), 'data-aim="dup"', n > 0, '⌘D') + mi(`Delete ${nm}`.trim(), 'data-aim="del"', n > 0, '⌫') + SEP
        + mi(`Options for ${nm || '…'}…`, 'data-aim="lopt"', !!one, '') + SEP
        + xmi(D, 'cx.clip', 'Make Clipping Mask', '⌘7') + xmi(D, 'cx.unclip', 'Release Clipping Mask', '⌥⌘7') + (D.xHas('cx.clip') ? SEP : '')
        + mi('Locate Object', 'data-aim="locate"', n > 0, '') + mi('Collect in New Layer', 'data-aim="group"', n >= 2, '⌘G') + mi('Release to Layers (Sequence)', 'data-aim="ungroup"', es.some(e => e.it.kind === 'group'), '⇧⌘G') + SEP
        + mi('Hide Others', 'data-aim="hideOthers"', n > 0, '') + mi('Show All Layers', 'data-aim="showAll"', true, '') + xmi(D, 'cx.outline', 'Outline / Preview', '⌘Y')
        + mi('Lock Others', 'data-aim="lockOthers"', n > 0, '') + mi('Unlock All Layers', 'data-aim="unlockAll"', true, '') + SEP
        + mi('Panel Options…', 'data-aim="popt"', true, '');
    } else if (id === 'transform') {
      h = xmi(D, 'ops.flipH', 'Flip Horizontal') + xmi(D, 'ops.flipV', 'Flip Vertical') + xmi(D, 'ops.rot90', 'Rotate 90° CW') + xmi(D, 'ops.rot90l', 'Rotate 90° CCW') + SEP
        + xmi(D, 'ops.move', 'Move…', '⇧⌘M') + xmi(D, 'ops.rotate', 'Rotate…') + xmi(D, 'ops.scale', 'Scale…') + xmi(D, 'ops.reflect', 'Reflect…') + SEP
        + xmi(D, 'ops.again', 'Transform Again', '⌘D') + xmi(D, 'ops.repeat', 'Step and Repeat…');
    } else if (id === 'align') {
      const to = P.prefs.alignTo;
      h = ['selection', 'key', 'charm', 'centre'].map(v => mi(ALIGN_TO[v], `data-aim="alto:${v}"`, true, '', to === v ? 2 : 1)).join('') + mi('Auto (Selection, or the Artboard for one)', 'data-aim="alto:"', true, '', !to ? 2 : 1) + SEP
        + xmi(D, 'ops.centre', 'Centre on the Charm');
    } else if (id === 'pathfinder') {
      h = xmi(D, 'ops.compound', 'Make Compound Path', '⌘8') + xmi(D, 'ops.release', 'Release Compound Path', '⌥⇧⌘8') + xmi(D, 'ops.outline', 'Outline Stroke') + xmi(D, 'ops.offset', 'Offset Path…');
    } else if (id === 'stroke') {
      h = mi('Reset Weight', 'data-aim="lw0"', true, '') + xmi(D, 'ops.outline', 'Outline Stroke');
    } else if (id === 'swatches') {
      h = mi('Small Thumbnail View', 'data-aim="swview:grid"', true, '', P.prefs.swList ? 1 : 2) + mi('List View', 'data-aim="swview:list"', true, '', P.prefs.swList ? 2 : 1);
    } else if (id === 'character') {
      h = mi('Bold', 'data-aim="tstyle:bold"', true, '⌘B') + mi('Italic', 'data-aim="tstyle:italic"', true, '⌘I') + SEP
        + ['upper', 'lower', 'title', 'small', ''].map(c => mi((D.CASE_NAME || {})[c] || c, `data-aim="case:${c}"`, true, '')).join('') + SEP + mi('Glyphs…', 'data-aim="alphafind"', true, '');
    } else if (id === 'trace') {
      h = xmi(D, 'imageRetrace', 'Trace…') + ((D.X.importers || []).length ? mi('Place Image…', 'data-aim="import"', true, '') : '');
    } else if (id === 'properties') {
      h = ALL.filter(x => x !== 'properties').map(x => mi(TITLE[x], `data-aim="open:${x}"`, true, WKEY[x])).join('');
    }
    const inMain = P.layout.main.some(g => g.tabs.includes(id));
    return h + SEP + (inMain ? mi('Move to the Icon Panels', `data-aim="tostrip:${id}"`, true, '') : mi('Dock in the Panel Column', `data-aim="tomain:${id}"`, true, ''));
  }
  function menuFor(P, kind, anchor) {
    const D = P.D, S = D.S;
    if (kind === 'rolemenu') return openMenu(P, `<div class="ce-menu__head">Swatches · what the laser does</div>${D.ROLES.map(r => `<button class="ce-item" type="button" data-aim="role:${r}" role="menuitemradio"><i class="ce-sw ce-sw--${r}"></i>${esc(D.ROLE_NAME[r])}<em>${esc(D.ROLE_SAYS[r])}</em></button>`).join('')}`, anchor);
    if (kind === 'fontmenu') return openMenu(P, `<div class="ce-menu__head">Font</div>` + D.fonts.map(f => `<button class="ce-item ce-font" type="button" data-aim="font:${esc(f.key)}" data-keep role="menuitemradio"${f.kind ? ` data-kind="${esc(f.kind)}"` : ''}${f.family ? ` style="font-family:${esc(f.family)}"` : ''} title="${esc(f.name)}">${esc(f.name)}</button>`).join(''), anchor);
    if (kind === 'stylemenu') {
      const texts = D.selLeaves().filter(l => l.kind === 'text'), noBold = texts.length ? texts.every(t => { const f = D.fonts.find(x => x.key === t.fontKey); return !!(f && f.bold === false); }) : !!((D.fonts.find(x => x.key === S.textFont) || {}).bold === false);
      return openMenu(P, [['regular', 'Regular', true], ['bold', 'Bold', !noBold], ['italic', 'Italic', true], ['bolditalic', 'Bold Italic', !noBold]].map(([v, l, on]) => mi(l, `data-aim="fstyle:${v}"`, on, '')).join(''), anchor);
    }
    if (kind === 'ptmenu') return openMenu(P, PT_PRESETS.map(v => mi(v + ' pt', `data-aim="pt:${v}"`, true, '')).join(''), anchor);
    if (kind === 'lwmenu') return openMenu(P, LW_PRESETS.map(v => mi(v + ' mm', `data-aim="lw:${v}"`, true, '')).join(''), anchor);
    if (kind === 'trackmenu') return openMenu(P, TRACK_PRESETS.map(v => mi(String(v), `data-aim="track:${v}"`, true, '')).join(''), anchor);
    if (kind === 'altomenu') return openMenu(P, ['selection', 'key', 'charm', 'centre'].map(v => mi(ALIGN_TO[v], `data-aim="alto:${v}"`, true, '', P.prefs.alignTo === v ? 2 : 1)).join('') + mi('Auto', 'data-aim="alto:"', true, '', !P.prefs.alignTo ? 2 : 1), anchor);
    if (kind === 'arrangemenu') { const n = S.sel.length > 0; return openMenu(P, mi('Bring to Front', 'data-aim="arrange:front"', n, '⇧⌘]') + mi('Bring Forward', 'data-aim="arrange:forward"', n, '⌘]') + mi('Send Backward', 'data-aim="arrange:backward"', n, '⌘[') + mi('Send to Back', 'data-aim="arrange:back"', n, '⇧⌘['), anchor); }
  }

  /* ── Layers: the others hidden or locked, a layer's options, locate, the selection moved into a layer ── */
  const flagged = (it, flag, v) => { const n = Object.assign({}, it, { [flag]: v }); if (flag === 'locked') { delete n._autoLock; delete n._lockedIn; } return n; };
  function dropFromSel(P, ids) {
    const D = P.D, set = new Set(ids);
    D.setSel(D.S.sel.filter(s => { const e = D.entry(s); return !set.has(s) && !(e && e.anc.some(a => set.has(a))); }));
  }
  /** ⌥-click on an eye or a lock (Illustrator: every other layer at that level hidden / locked, or all of them back when they already are). One step. */
  function othersFlag(P, id, flag) {
    const D = P.D, en = D.entry(id); if (!en) return;
    const sibs = en.parent ? en.parent.children || [] : D.items(), others = sibs.filter(s => s.id !== id);
    if (!others.length) return;
    const allOn = others.every(s => !!s[flag]), map = new Map();
    for (const s of others) if (!!s[flag] === allOn) map.set(s.id, flagged(s, flag, !allOn));
    if (!allOn && en.it[flag]) map.set(id, flagged(en.it, flag, false));
    if (!map.size) return;
    D.commit(D.model.replaceIn(D.items(), map));
    if (!allOn) dropFromSel(P, others.map(s => s.id));
  }
  /** Hide Others / Lock Others of the panel menu: the existing "everything else" commands when the page has them, else every other top-level layer. */
  function othersOfSel(P, flag) {
    const D = P.D, cmd = flag === 'hidden' ? 'ops.hideOthers' : 'ops.lockOthers';
    if (D.xOn(cmd)) { D.xRun(cmd); return; }
    const keep = new Set(D.S.sel.map(s => { const e = D.entry(s); return e ? (e.anc[0] || s) : s; })), map = new Map();
    for (const t of D.items()) if (!keep.has(t.id) && !t[flag]) map.set(t.id, flagged(t, flag, true));
    if (map.size) D.commit(D.model.replaceIn(D.items(), map));
  }
  function allOf(P, flag) {
    const D = P.D, cmd = flag === 'hidden' ? 'ops.showAll' : 'ops.unlockAll';
    if (D.xHas(cmd)) { D.xRun(cmd); return; }
    D.setAll(flag, false);
  }
  /** LOCATE OBJECT: every layer holding the selection opened, its row scrolled into view. */
  function locate(P) {
    const D = P.D; if (!D.S.sel.length) return;
    openPanel(P, 'layers');
    for (const id of D.S.sel) { const e = D.entry(id); if (e) for (const a of e.anc) D.S.open.add(a); }
    D.renderLayers(); D.paintLayerSel(true);
  }
  /** The selection square dragged onto a row: the selected parts move into that layer (into a group: on top of what it holds; onto a part: just above it), one step. */
  function moveInto(P, ids, targetId) {
    const D = P.D, M = D.model, geo = D.geo;
    let items = D.items(), ix = M.indexTree(items, geo);
    const t = ix.get(targetId); if (!t) return false;
    ids = M.topmost(ids, ix).filter(id => id !== targetId && !t.anc.includes(id) && !ix.get(id).it.locked);
    if (!ids.length) return false;
    const order = []; M.walk(items, it => { if (ids.includes(it.id)) order.push(it.id); });
    const into = t.it.kind === 'group', kids = into ? (t.it.children || []).filter(c => !order.includes(c.id)) : [];
    if (into && !kids.length) return false;
    const anchorId = into ? kids[kids.length - 1].id : targetId;
    items = M.normaliseAncestors(items, order.concat(anchorId), geo); ix = M.indexTree(items, geo);
    const moving = order.map(id => { const e = ix.get(id); return M.pushDown(e.it, null, geo, e.P, true); });
    items = M.removeIds(items, order, true); ix = M.indexTree(items, geo);
    const a = ix.get(anchorId); if (!a) return false;
    const pid = a.parent ? a.parent.id : null, list = (pid == null ? items : a.parent.children || []).slice();
    list.splice(list.findIndex(x => x.id === anchorId) + 1, 0, ...moving);
    const next = pid == null ? list : M.replaceIn(items, new Map([[pid, Object.assign({}, a.parent, { children: list })]]), true);
    if (into) D.S.open.add(targetId);
    D.commit(M.pruneEmpty(next), order);
    return true;
  }
  function wireSelSquare(P) {
    const D = P.D, L = D.layersEl;
    let sq = null;
    const clear = () => { for (const r of L.querySelectorAll('.aip-sqdrop')) r.classList.remove('aip-sqdrop'); };
    const move = e => {
      if (!sq) return;
      if (!sq.live) { if (Math.hypot(e.clientX - sq.x0, e.clientY - sq.y0) < 4) return; sq.live = true; sq.ghost = doc.createElement('i'); sq.ghost.className = 'aip-sqghost'; sq.ghost.style.background = getComputedStyle(sq.row).getPropertyValue('--lc') || '#4f8ff8'; D.rootEl.appendChild(sq.ghost); }
      sq.ghost.style.left = (e.clientX - 4) + 'px'; sq.ghost.style.top = (e.clientY - 4) + 'px';
      clear(); const el = doc.elementFromPoint(e.clientX, e.clientY), r = el && el.closest && el.closest('.ce-layer');
      sq.target = r && L.contains(r) && r !== sq.row ? r.dataset.id : null; if (sq.target) r.classList.add('aip-sqdrop');
    };
    const up = () => {
      const s = sq; sq = null; doc.removeEventListener('pointermove', move, true); doc.removeEventListener('pointerup', up, true); doc.removeEventListener('pointercancel', up, true);
      clear(); if (!s || !s.live) return;
      if (s.ghost) s.ghost.remove();
      P.noClick = Date.now();
      if (s.target) moveInto(P, D.S.sel.slice(), s.target);
    };
    L.addEventListener('pointerdown', e => {
      const b = e.target.closest('.aip-selsq'); if (!b || e.button) return;
      const row = b.closest('.ce-layer'); if (!row || !(row.classList.contains('is-sel') || row.classList.contains('aip-has-sel'))) return;
      sq = { x0: e.clientX, y0: e.clientY, row, live: false };
      doc.addEventListener('pointermove', move, true); doc.addEventListener('pointerup', up, true); doc.addEventListener('pointercancel', up, true);
    });
    // a double-click on a row (not its name, which renames): its Options, as Illustrator's
    L.addEventListener('dblclick', e => {
      if (e.target.closest('.ce-layer__name') || e.target.closest('[data-lact]')) return;
      const row = e.target.closest('.ce-layer'); if (row) layerOptions(P, row.dataset.id);
    });
  }
  /** The editor asks about a click on a row's own buttons (the editor's eye / lock / twisty clicks stay its own; these are Illustrator's additions). */
  function lact(P, a, id, e) {
    const D = P.D;
    if (a === 'eye') { if (e.metaKey || e.ctrlKey) { if (D.xHas('cx.outline')) D.xRun('cx.outline'); else D.say('Outline view is not on this page', 2400); return true; } if (e.altKey) { othersFlag(P, id, 'hidden'); return true; } return false; }
    if (a === 'lock') { if (e.altKey) { othersFlag(P, id, 'locked'); return true; } return false; }
    if (a === 'twist') {
      if (!e.altKey) return false;
      const en = D.entry(id); if (!en) return true;
      const open = !D.S.open.has(id), rec = it => { if (it.kind !== 'group') return; if (open) D.S.open.add(it.id); else D.S.open.delete(it.id); (it.children || []).forEach(rec); };
      rec(en.it); D.renderLayers(); return true;
    }
    if (a === 'target' || a === 'sel') { if (Date.now() - (P.noClick || 0) < 400) return true; D.rowClick(id, e); return true; }
    return false;
  }

  /* ── the dialogs: Layer Options and Panel Options (Illustrator's), over the work; Esc and Cancel leave them, Enter is OK ── */
  function dialog(P, title, bodyHtml, onOk) {
    const D = P.D;
    if (D.S.ask) return;
    const m = doc.createElement('div'); m.className = 'aip-modal'; m.setAttribute('role', 'dialog'); m.setAttribute('aria-modal', 'true'); m.setAttribute('aria-label', title);
    m.innerHTML = `<form class="aip-dlg" novalidate><h3>${esc(title)}</h3><div class="aip-dlg__b">${bodyHtml}</div><div class="aip-dlg__f"><button class="aip-btn" type="button" data-aip-dlg="cancel">Cancel</button><button class="aip-btn is-cta" type="submit">OK</button></div></form>`;
    D.rootEl.appendChild(m);
    const close = () => { if (D.S.ask === ask) D.S.ask = null; m.remove(); try { D.win.focus({ preventScroll: true }); } catch (e) { /* fine */ } };
    const ask = { done: () => close() };
    D.S.ask = ask;   // (the editor's keys stand still while a dialog is open; Esc closes it)
    m.querySelector('form').addEventListener('submit', e => { e.preventDefault(); const f = m.querySelector('form'); close(); onOk(f); });
    m.querySelector('[data-aip-dlg="cancel"]').addEventListener('click', close);
    m.addEventListener('pointerdown', e => { if (e.target === m) close(); });
    const first = m.querySelector('input[type="text"],select,input'); if (first) root.requestAnimationFrame(() => { try { first.focus(); if (first.select) first.select(); } catch (e) { /* fine */ } });
  }
  function layerOptions(P, id) {
    const D = P.D, en = D.entry(id); if (!en) return;
    const it = en.it, role = roleOf(D, it);
    const roleSel = `<select name="role" aria-label="Color">${role === 'mixed' ? '<option value="" selected>Mixed (as they are)</option>' : ''}${D.ROLES.map(r => `<option value="${r}"${r === role ? ' selected' : ''}>${esc(D.ROLE_NAME[r])}</option>`).join('')}</select>`;
    dialog(P, it.kind === 'group' ? 'Layer Options' : 'Options', `<span>Name:</span><input class="aip-in" type="text" name="name" maxlength="60" value="${esc(D.rowName(it))}" aria-label="Name">`
      + `<span>Color:</span>${roleSel}`
      + `<label class="aip-check"><input type="checkbox" name="show"${it.hidden ? '' : ' checked'}> Show</label>`
      + `<label class="aip-check"><input type="checkbox" name="lock"${it.locked ? ' checked' : ''}> Lock</label>`, f => {
      const e2 = D.entry(id); if (!e2) return;
      const name = String(f.elements.name.value || '').trim(), show = f.elements.show.checked, lock = f.elements.lock.checked, nr = f.elements.role.value;
      let n = e2.it;
      if (name !== String(D.rowName(e2.it)) && name !== String(e2.it.name || '')) n = Object.assign({}, n, { name: name || undefined });
      if (n.hidden === show) n = Object.assign({}, n, { hidden: !show });
      if (!!n.locked !== lock) n = flagged(n, 'locked', lock);
      if (n !== e2.it) { D.commit(D.model.replaceIn(D.items(), new Map([[id, n]]))); if (!show || lock) dropFromSel(P, [id]); }
      if (nr && nr !== role && !lock) { D.setSel([id]); D.setRole(nr); }
    });
  }
  function panelOptions(P) {
    const pr = P.prefs;
    dialog(P, 'Layers Panel Options', `<span>Row Size:</span><select name="rows" aria-label="Row Size">${['small', 'medium', 'large'].map(v => `<option value="${v}"${pr.rowSize === v ? ' selected' : ''}>${v[0].toUpperCase() + v.slice(1)}</option>`).join('')}</select>`
      + `<label class="aip-check"><input type="checkbox" name="thumbs"${pr.thumbs ? ' checked' : ''}> Show Thumbnails</label>`, f => {
      pr.rowSize = f.elements.rows.value; pr.thumbs = f.elements.thumbs.checked; savePrefs(P); applyPrefs(P); P.D.renderLayers(true);
    });
  }
  function applyPrefs(P) {
    const dk = P.D.dock;
    dk.classList.toggle('is-rows-small', P.prefs.rowSize === 'small'); dk.classList.toggle('is-rows-large', P.prefs.rowSize === 'large');
    dk.classList.toggle('is-nothumb', !P.prefs.thumbs); dk.classList.toggle('is-swlist', !!P.prefs.swList);
    P.D.S.aiRef = refOf(P).slice();
  }

  /** Every data-aim of the dock and its menus. */
  function act(P, a, el, e) {
    const D = P.D, S = D.S;
    const [k, v] = a.includes(':') ? [a.slice(0, a.indexOf(':')), a.slice(a.indexOf(':') + 1)] : [a, ''];
    switch (k) {
      case 'collapse': { const w = root.innerWidth || 1280; if (w <= DRAWER) return; if (w <= NARROW) P.narrowOpen = !P.narrowOpen; else P.layout.collapsed = !P.layout.collapsed; closeFly(P); layoutChanged(P); return; }
      case 'pmenu': return openMenu(P, panelMenuHtml(P, el.dataset.aipFor), el);
      case 'open': { D.closeMenus(); openPanel(P, v); return; }
      case 'ref': { setRef(P, v.split(',').map(Number)); savePrefs(P); applyPrefs(P); sync(P); return; }
      case 'al': case 'di': return alignRun(P, k, v);
      case 'alto': { P.prefs.alignTo = v || null; savePrefs(P); D.closeMenus(); sync(P); return; }
      case 'locate': return locate(P);
      case 'clip': { if (D.xOn('cx.unclip')) D.xRun('cx.unclip'); else if (D.xOn('cx.clip')) D.xRun('cx.clip'); return; }
      case 'fstyle': { D.closeMenus(); D.setTextStyle({ bold: /bold/.test(v) || undefined, italic: /italic/.test(v) || undefined }); return; }
      case 'pt': { D.closeMenus(); D.setTextSize(+v); return; }
      case 'lw': { D.closeMenus(); D.setStrokeMm(+v); return; }
      case 'track': { D.closeMenus(); setTextField(P, 'tracking', +v); return; }
      case 'arrange': { D.closeMenus(); D.arrangeSel(v); return; }
      case 'dup': { D.closeMenus(); D.duplicate(); return; }
      case 'del': { D.closeMenus(); D.deleteSel(); return; }
      case 'lopt': { D.closeMenus(); if (S.sel.length === 1) layerOptions(P, S.sel[0]); return; }
      case 'popt': { D.closeMenus(); panelOptions(P); return; }
      case 'hideOthers': { D.closeMenus(); othersOfSel(P, 'hidden'); return; }
      case 'lockOthers': { D.closeMenus(); othersOfSel(P, 'locked'); return; }
      case 'showAll': { D.closeMenus(); allOf(P, 'hidden'); return; }
      case 'unlockAll': { D.closeMenus(); allOf(P, 'locked'); return; }
      case 'swview': { D.closeMenus(); P.prefs.swList = v === 'list'; savePrefs(P); applyPrefs(P); if (P.fly) P.fly.classList.toggle('is-swlist', P.prefs.swList); return; }
      case 'tostrip': case 'tomain': { D.closeMenus(); moveTab(P, v, k === 'tostrip' ? 'new-strip' : 'new-main'); return; }
      // (the editor's own calls behind its data-* hooks, here under data-aim so the page's own controls stay the only ones answering to theirs)
      case 'x': { D.closeMenus(); if (D.xOn(v)) D.xRun(v); return; }
      case 'group': { D.closeMenus(); D.groupSel(); return; }
      case 'ungroup': { D.closeMenus(); D.ungroupSel(); return; }
      case 'lw0': { D.closeMenus(); D.resetStroke(); return; }
      case 'wt0': return D.setWeightMm(0);
      case 'link': { S.link = !S.link; D.syncProps(); return; }
      case 'import': { D.closeMenus(); D.xPick(); return; }
      case 'face': return D.switchFace(v);
      case 'opt': return D.opt(v);
      case 'zoom': return D.zoomCmd(v);
      case 'role': { D.closeMenus(); D.setRole(v); return; }
      case 'case': return D.setTextStyle({ letterCase: v || undefined });
      case 'talign': return D.setTextStyle({ align: v });
      case 'tstyle': return D.toggleTextStyle(v);
      case 'font': { const f = D.fonts.find(x => String(x.key) === v); if (f) D.setFont(f.key); return; }
      case 'alphafind': { D.closeMenus(); D.alphaFind(); return; }
      default: if (/menu$/.test(k)) return menuFor(P, k, el);
    }
  }

  /* ════ MOUNT ════ */
  function mount(D) {
    if (!doc || !D || !D.dock || !D.layersEl) return null;
    injectCss();
    const dk = D.dock, P = { D, layout: cleanLayout(readJson(LS_LAYOUT)), prefs: cleanPrefs(readJson(LS_PREFS)), key: null, narrowOpen: false, raf: 0, dead: false };
    // the editor's own pieces kept: the face tabs (hidden: the artboards are the shell's document tabs and the Properties' Document section) and the fold button,
    // the button bar (Illustrator's bottom bar of the Layers panel)
    const faceTabs = dk.querySelector('.ce-dock__tabs'), fold = faceTabs && faceTabs.querySelector('[data-act="dock"]');
    P.bar = dk.querySelector('.ce-pane__bar');
    dk.classList.add('aip');
    dk.innerHTML = '';
    const head = doc.createElement('div'); head.className = 'aip-head';
    if (fold) { fold.className = 'aip-fold'; fold.innerHTML = '×'; fold.title = 'Close the panels'; fold.setAttribute('aria-label', 'Close the panels'); head.appendChild(fold); }
    P.collapseBtn = doc.createElement('button'); P.collapseBtn.type = 'button'; P.collapseBtn.dataset.aim = 'collapse'; head.appendChild(P.collapseBtn);
    const cols = doc.createElement('div'); cols.className = 'aip-cols';
    P.strip = doc.createElement('div'); P.strip.className = 'aip-strip'; P.strip.setAttribute('role', 'toolbar'); P.strip.setAttribute('aria-label', 'Collapsed panels'); P.strip.setAttribute('aria-orientation', 'vertical');
    P.main = doc.createElement('div'); P.main.className = 'aip-main';
    cols.appendChild(P.strip); cols.appendChild(P.main);
    P.fly = doc.createElement('div'); P.fly.className = 'aip-fly'; P.fly.hidden = true;
    P.store = doc.createElement('div'); P.store.className = 'aip-store'; P.store.setAttribute('aria-hidden', 'true');
    dk.appendChild(head); dk.appendChild(cols); dk.appendChild(P.fly); dk.appendChild(P.store);
    if (faceTabs) { faceTabs.hidden = true; P.store.appendChild(faceTabs); }
    P.root = dk;
    // the Layers bottom bar: N Layers, Locate Object, Make / Release Clipping Mask, Release to Layers, Collect in New Layer, Delete (the editor's forward / backward
    // buttons stay in it, unseen: Illustrator has none there; ⌘] ⌘[ and the menus do it)
    if (P.bar) {
      const btn = (attrs, icon, title) => { const b = doc.createElement('span'); b.innerHTML = `<button class="ce-act" type="button" ${attrs} title="${esc(title)}" aria-label="${esc(title)}">${icon}</button>`; return b.firstChild; };
      const keep = a => P.bar.querySelector(`[data-act="${a}"]`);
      const fw = keep('forward'), bw = keep('backward'), gr = keep('group'), ug = keep('ungroup'), del = keep('delete');
      P.bar.innerHTML = '';
      P.count = doc.createElement('span'); P.count.className = 'aip-count'; P.bar.appendChild(P.count);
      for (const b of [fw, bw]) if (b) { b.hidden = true; P.bar.appendChild(b); }
      P.bar.appendChild(btn('data-aim="locate"', IC.locate, 'Locate Object'));
      if (D.xHas('cx.clip')) P.bar.appendChild(btn('data-aim="clip"', IC.clip, 'Make/Release Clipping Mask'));
      if (ug) { ug.innerHTML = IC.release; ug.title = 'Release to Layers: the group’s parts in its place (Ungroup, ⇧⌘G)'; ug.setAttribute('aria-label', 'Release to Layers (Ungroup)'); P.bar.appendChild(ug); }
      if (gr) { gr.innerHTML = IC.collect; gr.title = 'Collect in New Layer: the selection as one group (⌘G)'; gr.setAttribute('aria-label', 'Collect in New Layer (Group)'); P.bar.appendChild(gr); }
      if (del) { del.innerHTML = IC.trash; del.title = 'Delete Selection (⌫)'; del.setAttribute('aria-label', 'Delete Selection'); P.bar.appendChild(del); }
    }
    P.bodies = buildBodies(P);
    for (const id of ALL) if (P.bodies[id]) P.store.appendChild(P.bodies[id]);
    takePending();
    for (const id of EXT.keys()) extBody(P, id);
    P.layout = cleanLayout(P.layout);
    for (const inp of dk.querySelectorAll('[data-aif]')) wireField(P, inp);
    for (const s of dk.querySelectorAll('[data-aip-key]')) s.addEventListener('change', () => { P.key = s.value; });
    { const xp = D.rootEl.querySelector('.ce-xpanel'); if (xp) xp.classList.add('aip-x'); }
    // the panel menus
    P.menu = doc.createElement('div'); P.menu.className = 'ce-menu aip-menu'; P.menu.setAttribute('role', 'menu'); P.menu.hidden = true; D.rootEl.appendChild(P.menu);
    P.menu.addEventListener('click', e => {
      const m = e.target.closest('[data-aim]');
      if (m && !m.disabled) { e.stopPropagation(); act(P, m.dataset.aim, m, e); if (!m.hasAttribute('data-keep') && !/menu$/.test(m.dataset.aim)) D.closeMenus(); return; }
      const it = e.target.closest('.ce-item'); if (it && !it.hasAttribute('data-keep')) root.setTimeout(() => D.closeMenus(), 0);   // (a font picked keeps its menu open, as the bar's does)
    });
    // the dock's own clicks
    dk.addEventListener('click', e => {
      const t = e.target;
      if (P.dragged && Date.now() - P.dragged < 300) { e.stopPropagation(); return; }
      const tab = t.closest('[data-aip-tab]');
      if (tab) { const key = tab.closest('[data-aip-g]').dataset.aipG, g = groupAt(P.layout, key); if (!g) return; if (g.active === tab.dataset.aipTab && !(key[0] === 'm' && g.min)) return; g.active = tab.dataset.aipTab; if (key[0] === 'm') g.min = false; saveLayout(P); if (P.fly.contains(tab)) openFly(P, key, g.active); else render(P); if (g.active === 'layers') D.paintLayerSel(true); return; }
      const ico = t.closest('[data-aip-ico]'); if (ico) { if (!P.fly.hidden && P.flyFor === ico.dataset.aipIco) closeFly(P); else openFly(P, ico.dataset.aipSrc, ico.dataset.aipIco); return; }
      const xp = t.closest('[data-aip-xp]'); if (xp) { closeFly(P); D.xPanel(D.xPanelOn() === xp.dataset.aipXp ? null : xp.dataset.aipXp); paintStrip(P); return; }
      const m = t.closest('[data-aim]');
      if (m && !m.disabled) { if (/menu$/.test(m.dataset.aim) && P.skipFor === m) { P.skipFor = null; return; } act(P, m.dataset.aim, m, e); }
    });
    dk.addEventListener('dblclick', e => {
      const tab = e.target.closest('[data-aip-tab]'); if (!tab) return;
      const key = tab.closest('[data-aip-g]').dataset.aipG, g = groupAt(P.layout, key); if (!g || key[0] !== 'm') return;
      g.min = !g.min; layoutChanged(P);
    });
    // a press outside folds a fly-out away; a press on a menu's own button while it is open closes it (and does not open it again)
    P.onDocDown = e => {
      const t = e.target;
      P.skipFor = !P.menu.hidden && P.menuFor && P.menuFor.contains(t) ? P.menuFor : null;
      if (!P.fly.hidden && !P.fly.contains(t) && !(t.closest && (t.closest('.aip-ico') || t.closest('.ce-menu') || t.closest('.aip-modal')))) closeFly(P);
    };
    doc.addEventListener('pointerdown', P.onDocDown, true);
    wireTabDrag(P); wireSelSquare(P);
    applyPrefs(P);
    render(P);
    const out = {
      rowsHtml: its => rowsHtml(P, its), paint: () => paint(P), update: () => schedule(P),
      live: r => { if (!P.dead) { P.liveBox = r ? [0, 0, (r.w || 0) / MM, (r.h || 0) / MM] : null; fieldsTo(P, r); P.liveBox = null; } },
      lact: (a, id, e) => lact(P, a, id, e), layout: () => { const m = modeOf(P); if (m !== P.mode) render(P); else sync(P); },
      unmount: () => { P.dead = true; if (P.raf) root.cancelAnimationFrame(P.raf); doc.removeEventListener('pointerdown', P.onDocDown, true); if (P.menu) P.menu.remove(); extEach(P, 'unmount'); if (liveP === P) liveP = null; },
      openPanel: id => openPanel(P, String(id)), state: () => ({ mode: P.mode, layout: JSON.parse(JSON.stringify(P.layout)), prefs: JSON.parse(JSON.stringify(P.prefs)), fly: P.fly.hidden ? null : P.flyFor })
    };
    api.current = out; liveP = P;
    return out;
  }

  /** CharmEditor.aiDock.add({ id, title, icon?, key?, group?, mount(ctx, el), update?(ctx), unmount?() }): a panel of another file as a tab of this dock, as Illustrator's
   *  panels are: dragged, minimised, collapsed to an icon and remembered like the others. icon: an <svg> (18-unit box reads best); key: the Window menu's key, shown in its
   *  tooltip; group: a panel's id to share its tabs ('properties', 'layers', 'character' ...), 'main' for a group of its own in the column, none for an icon of its own.
   *  mount is called the first time the panel is shown (el: its body; ctx: the editor's extension ctx), update at most once a frame after the work or the selection
   *  changed, unmount as the editor closes. A file loaded before this one can leave its panels in CharmEditor.aiDockPanels (an array); they are taken in. -> true when added. */
  let liveP = null;
  function add(def) {
    if (!def || typeof def !== 'object' || def.id == null || String(def.id) === '' || typeof def.mount !== 'function') return false;
    const id = String(def.id);
    if (ALL.includes(id) && !EXT.has(id)) return false;   // (the dock's own panels are not replaced)
    if (!ALL.includes(id)) ALL.push(id);
    EXT.set(id, def); TITLE[id] = String(def.title || id); WKEY[id] = def.key ? String(def.key) : '';
    if (typeof def.icon === 'string' && /^\s*<svg/i.test(def.icon)) IC[id] = def.icon;
    if (liveP && !liveP.dead) { extBody(liveP, id); liveP.layout = cleanLayout(liveP.layout); render(liveP); }
    return true;
  }
  function takePending() {
    const CE = root && root.CharmEditor, list = CE && Array.isArray(CE.aiDockPanels) ? CE.aiDockPanels.splice(0) : [];
    for (const d of list) add(d);
  }

  const api = { mount, add, cleanLayout, cleanPrefs, splitName, DEF_LAYOUT, get panels() { return ALL.map(id => ({ id, title: TITLE[id], key: WKEY[id], added: EXT.has(id) })); }, version: 1, current: null,
    openPanel: id => (api.current ? api.current.openPanel(id) : false) };
  return api;
});
