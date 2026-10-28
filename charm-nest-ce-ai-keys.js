/* charm-nest-ce-ai-keys.js — THE CHARM EDITOR AS ILLUSTRATOR, PART TWO: the right-click menus, the keyboard and Edit ▸ Keyboard Shortcuts… (Paul, 28 Oct 2026: "my employees know
 * how to operate Adobe illustrator very well and they know the exact position, size, iconography ..., groupings of options, all expected keyboard shortcuts (Illustrator,
 * Photoshop) ... everything needs to be familiar to a person who is an expert in Adobe illustrator ... I'm not looking for you to change the functionality of things").
 *
 * This file changes no work. It is built on charm-nest-ce-ai-shell.js's ONE COMMAND CATALOG (window.CharmEditorAi: every command's Illustrator menu path, name, key and
 * what it runs) and the workspace handle the editor gives the shell (w: st(), act(), x, modal()):
 *   - CONTEXT MENUS: Illustrator's contextual menu for what was right-clicked (nothing, a path, a group, words, a traced image, several things), on the work area and on
 *     a Layers row, in Illustrator's order: Undo / Redo, the clipboard, what can be done to that kind of object, Transform ▸ Arrange ▸ Select ▸, and the charm's own last
 *     (Send to Sheet…, Laser Check…). Only what can be done is shown, as Illustrator's do; Undo, Redo and Paste are greyed instead. Every row runs the editor's own action
 *     (data-ctx), an extension command (data-xcmd) or a catalog command (data-ai): nothing is done here that the Object menu does not do.
 *   - THE KEYS: one dispatcher (key / keyUp), called by the editor's onKey after its own guards (the words box, a field, a dialog, a tool's own keys) and before its own
 *     keys. A catalog entry with a key runs here unless the editor does it itself (`own`); Illustrator's keys win over the editor's older ones (M, L, R, ⌘2, ⌘B ...);
 *     the Photoshop keys kept are the ones Illustrator gives nothing to. ⌘Space / ⌥⌘Space are the Zoom tool while held, as in both.
 *   - Select ▸ First Object Above / Next Object Above / Next Object Below / Last Object Below (⌥⇧⌘] ⌥⌘] ⌥⌘[ ⌥⇧⌘[): extension commands (CharmEditor.commands 'ai.sel*'),
 *     a change of the selection only.
 *   - Edit ▸ Keyboard Shortcuts… (⌥⇧⌘K): Illustrator's dialog, read-only: the Set (Illustrator Defaults), Tools / Menu Commands, a search, the tree of menus, Mac or
 *     Windows keys, Export Text…. It replaces the shell's own sheet (CharmEditorAi.openShortcuts).
 *
 *   window.CharmEditorAiKeys (also CharmEditorAi.keys)
 *     key(e, w) -> true when the key was used (the editor stops there);  keyUp(e, w)
 *     contextList(w, src) -> [items]   the right-click menu as the shell's items (src: 'stage' | 'layers');  openContext(el, cx, cy, w, src) -> true when shown
 *     contextCase(w) -> 'none' | 'path' | 'group' | 'text' | 'image' | 'multi'
 *     table(w, mac) -> [{ view: 'tools' | 'menu', menu, path, label, key, keyText, note, id }]   every command and its key (the sheet, tests, another menu)
 *     label(id, mac) -> '⌥⇧⌘K' | 'Alt+Shift+Ctrl+K' | ''   a command's shortcut as Illustrator spells it
 *     bind({ id, key, label, path, run(w, e), when?(w), on?(w), note? })   another key (a docked panel's own); it is listed on the sheet
 *     showShortcuts(w?, opts?) -> { box, close }   Edit ▸ Keyboard Shortcuts…
 */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api;
  else { root.CharmEditorAiKeys = api; api.attach(); }
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const doc = root && root.document ? root.document : null;
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const A = () => (root && root.CharmEditorAi && Array.isArray(root.CharmEditorAi.catalog) ? root.CharmEditorAi : null);
  const safe = (f, d) => { try { return f(); } catch (er) { try { root.console && root.console.error('CharmEditorAiKeys:', er); } catch (e2) { /* fine */ } return d; } };
  const MAC = !!(root && root.navigator) && (/Mac|iPhone|iPad|iPod/.test(root.navigator.platform || '') || /Mac OS X/.test(root.navigator.userAgent || ''));
  const isMac = () => { const a = A(); return a ? !!a.mac : MAC; };
  const keyLabel = (spec, mac) => { const a = A(); return a ? a.keyLabel(spec, mac) : String(spec || ''); };

  /* ═══ 1 · Select ▸ First / Next Object Above, Next / Last Object Below: the stacking order of the list the selection is in (later in the list is above) ═══ */
  const selectable = it => it && !it.hidden && !it.locked;
  function stackPick(ctx, how) {
    const sel = ctx.selection(), first = sel.length ? ctx.entry(sel[0]) : null;
    const list = first && first.parent && Array.isArray(first.parent.children) ? first.parent.children : ctx.items();
    if (!list.length) return false;
    const at = sel.map(id => list.findIndex(it => it.id === id)).filter(i => i >= 0);
    let pick = -1;
    if (how === 'first') { for (let i = list.length - 1; i >= 0; i--) if (selectable(list[i])) { pick = i; break; } }
    else if (how === 'last') { for (let i = 0; i < list.length; i++) if (selectable(list[i])) { pick = i; break; } }
    else if (at.length) {
      if (how === 'above') { for (let i = Math.max(...at) + 1; i < list.length; i++) if (selectable(list[i])) { pick = i; break; } }
      else for (let i = Math.min(...at) - 1; i >= 0; i--) if (selectable(list[i])) { pick = i; break; }
    }
    if (pick < 0) return false;
    ctx.select([list[pick].id]);
    return true;
  }
  const anyItems = ctx => ctx.items().some(selectable), anySel = ctx => ctx.selection().length > 0;
  const SELECT_CMDS = {
    'ai.selFirstAbove': { title: 'First Object Above', key: 'Mod+Alt+Shift+]', group: 'Select', menu: false, context: false, enabled: anyItems, run: ctx => { stackPick(ctx, 'first'); } },
    'ai.selNextAbove': { title: 'Next Object Above', key: 'Mod+Alt+]', group: 'Select', menu: false, context: false, enabled: anySel, run: ctx => { stackPick(ctx, 'above'); } },
    'ai.selNextBelow': { title: 'Next Object Below', key: 'Mod+Alt+[', group: 'Select', menu: false, context: false, enabled: anySel, run: ctx => { stackPick(ctx, 'below'); } },
    'ai.selLastBelow': { title: 'Last Object Below', key: 'Mod+Alt+Shift+[', group: 'Select', menu: false, context: false, enabled: anyItems, run: ctx => { stackPick(ctx, 'last'); } }
  };
  const SELECT_ORDER = ['ai.selFirstAbove', 'ai.selNextAbove', 'ai.selNextBelow', 'ai.selLastBelow'];

  /* ═══ 2 · the keys ═══ */
  const EXTRA = [];   // bind(): keys of another file, and this file's own (the Select commands above)
  for (const id of SELECT_ORDER) EXTRA.push({ id, key: SELECT_CMDS[id].key, label: SELECT_CMDS[id].title, path: 'Select', x: id, mine: true });
  function bind(b) {
    if (!b || !b.id || !b.key || (typeof b.run !== 'function' && !b.x)) return false;
    const i = EXTRA.findIndex(x => x.id === b.id); if (i >= 0) EXTRA.splice(i, 1);
    EXTRA.push(Object.assign({ path: 'Other', label: String(b.id) }, b)); keyCache = null;
    return true;
  }
  let keyCache = null;
  /** The entries a key can run here: the catalog's (not the ones the editor's own onKey does: `own`, `ownKey`) and the bound ones, parsed once. */
  function keyed() {
    const a = A(); if (!a) return [];
    if (keyCache && keyCache.cat === a.catalog && keyCache.n === a.catalog.length) return keyCache.list;
    const list = [];
    for (const e of a.catalog) if (e.key && !e.own && !e.ownKey) { const k = a.parse(e.key); if (k) list.push({ e, k, cat: true }); }
    for (const b of EXTRA) { const k = a.parse(b.key); if (k) list.push({ e: b, k, cat: false }); }
    keyCache = { cat: a.catalog, n: a.catalog.length, list };
    return list;
  }
  /** Tab and Shift+Tab hide the panels only from the work itself: on a button or a link they move the focus, as everywhere. */
  function workFocused(e, w) {
    const t = e.target;
    if (!t || t === doc.body || t === doc.documentElement || t === w.win || t === w.root) return true;
    return !!(t.closest && t.closest('.ce-stage'));
  }
  let spring = null;   // ⌘Space held: the Zoom tool until the keys are let go, then the tool that was in hand
  function key(e, w) {
    const a = A(); if (!a || !w || !e || e.isComposing || e.defaultPrevented) return false;
    const k = e.key, mod = !!(e.metaKey || e.ctrlKey);
    // the temporary Zoom tool: ⌘Space (in), ⌥⌘Space (out: the Zoom tool zooms out with Alt held), as in Illustrator and Photoshop
    if (mod && (k === ' ' || e.code === 'Space')) {
      e.preventDefault();
      if (!spring) { const s = safe(() => w.st(), null); if (s && s.tool !== 'zoom') { spring = { prev: s.tool }; w.act('tool', 'zoom'); } }
      return true;
    }
    for (const { e: en, k: kk, cat } of keyed()) {
      if (!a.matches(kk, e)) continue;
      if (kk.key === 'Tab' && !workFocused(e, w)) return false;
      if (en.keyWhen && !safe(() => en.keyWhen(w), false)) continue;
      if (cat) {
        const r = safe(() => a.resolve(w, en.id), null);
        if (!r) continue;   // (the editor cannot do it here: the key is the editor's own, or an extension's, or the browser's)
        e.preventDefault();
        if (r.on) a.run(en.id);
        return true;
      }
      if (en.when && !safe(() => en.when(w), false)) continue;
      if (en.x) { if (!w.x.has(en.x)) continue; e.preventDefault(); if (w.x.enabled(en.x)) w.x.run(en.x); return true; }
      e.preventDefault();
      if (!en.on || safe(() => en.on(w), false)) safe(() => en.run(w, e));
      return true;
    }
    if (k === 'F1') { e.preventDefault(); return true; }   // (the browser's help: never over the work)
    return false;
  }
  function keyUp(e, w) {
    if (!spring || !w) return false;
    const k = e.key;
    if (k === ' ' || e.code === 'Space' || k === 'Meta' || k === 'Control' || k === 'OS') {
      const prev = spring.prev; spring = null;
      const s = safe(() => w.st(), null); if (s && s.tool === 'zoom' && prev) w.act('tool', prev);
      return true;
    }
    return false;
  }

  /* ═══ 3 · the context menus ═══ */
  function selKinds(w) {
    const cur = root.CharmEditor && root.CharmEditor.current, ctx = cur && cur.ctx;
    if (!ctx || typeof ctx.selection !== 'function') return null;
    return ctx.selection().map(id => { const en = ctx.entry(id); return en ? en.it.kind : null; }).filter(Boolean);
  }
  function contextCase(w) {
    const s = w.st(); if (!s.n) return 'none';
    if (w.x.has('imageRetrace') && w.x.enabled('imageRetrace')) return 'image';
    const kinds = selKinds(w) || [];
    if (kinds.length && kinds.every(k => k === 'text')) return 'text';
    if (s.n > 1) return 'multi';
    return kinds[0] === 'group' || s.anyGroup ? 'group' : 'path';
  }
  /** The right-click menu for what is selected now (the editor selects what was right-clicked first, as Illustrator does). */
  function contextList(w, src) {
    const a = A(); if (!a) return [];
    const s = w.st(), kind = contextCase(w);
    const SEP = { sep: true };
    const R = (id, over) => { const r = safe(() => a.resolve(w, id), null); return r ? Object.assign(r, over || {}) : null; };
    const on = r => (r && r.on ? r : null);                                    // (contextual: shown when it can be done)
    const sub = (label, kids) => { kids = tidy(kids); return kids.length ? { label, kids, on: kids.some(k => !k.sep && k.on) } : null; };
    const X = (xid, label) => (w.x.has(xid) ? { e: { id: xid, x: xid }, id: xid, xid, label: label || w.x.title(xid), key: a.keyLabel((EXTRA.find(b => b.x === xid) || {}).key || ''), on: w.x.enabled(xid), chk: null } : null);
    const list = [];
    // Undo / Redo: greyed when there is nothing to take back, as Illustrator's
    list.push(R('edit.undo'), R('edit.redo'), SEP);
    if (kind === 'none') {
      list.push(R('edit.paste'), on(R('edit.pasteFront')), on(R('edit.pasteBack')), R('edit.pasteInPlace'), SEP,
        R('view.zoomIn'), R('view.zoomOut'), R('view.fit'), R('view.actual'), SEP,
        R('view.rulers'), R('view.grid'), R('view.guides'), on(R('view.clearGuides')), SEP,
        R('view.outline'), SEP, R('sel.all'));
    } else {
      list.push(R('edit.cut'), R('edit.copy'), R('edit.paste'), R('edit.pasteInPlace'), R('edit.clear'), SEP);
      if (kind === 'text') {
        list.push(on(R('type.edit')), sub('Font', a.dynItems(w, 'fonts')), sub('Size', a.dynItems(w, 'sizes')), sub('Font Style', a.subItems(w, 'Type', 'Font Style')),
          sub('Change Case', a.dynItems(w, 'cases')), sub('Paragraph', a.subItems(w, 'Type', 'Paragraph')), on(R('type.glyphs')), SEP,
          on(R('type.onPath', { label: 'Type on a Path' })), on(R('type.offPath', { label: 'Release Type on a Path' })), SEP);
      }
      if (kind === 'image') list.push(sub('Image Trace', [on(R('obj.retrace', { label: 'Re-trace…' }))]), SEP);
      // Group / Ungroup, Join, Simplify (Illustrator's own order)
      list.push(s.n > 1 ? R('obj.group') : null, s.anyGroup ? R('obj.ungroup') : null, on(R('obj.join')), on(R('obj.close')), kind !== 'text' ? on(R('obj.simplify')) : null, SEP);
      // the masks and compound paths, then the path operations a charm designer reaches for (Object ▸ Path in Illustrator: the cut line is ours)
      list.push(on(R('obj.clip', { label: 'Make Clipping Mask' })), on(R('obj.unclip', { label: 'Release Clipping Mask' })),
        on(R('obj.compound', { label: 'Make Compound Path' })), on(R('obj.uncompound', { label: 'Release Compound Path' })), SEP);
      if (kind !== 'text') list.push(on(R('obj.offset')), on(R('obj.outline')), on(R('obj.contour')), on(R('obj.blendRelease', { label: 'Release Blend' })), on(R('obj.warpExpand', { label: 'Expand Envelope' })), SEP);
      // Transform ▸ Arrange ▸ Select ▸
      list.push(sub('Transform', a.subItems(w, 'Object', 'Transform')), sub('Arrange', a.subItems(w, 'Object', 'Arrange')));
    }
    const selKids = SELECT_ORDER.map(id => X(id)).concat([SEP, sub('Same', a.subItems(w, 'Select', 'Same')), R('sel.inverse')]);
    list.push(sub('Select', selKids), SEP);
    // the charm's own, where Illustrator has Add to Library / Collect for Export / Export Selection…
    list.push(s.n ? on(R('obj.jumpRing')) : null, R('file.sendToSheet'), R('view.check'));
    return tidy(list);
  }
  /** No separator first, last or twice; a submenu tidied the same way. */
  function tidy(list) {
    const out = [];
    for (const it of list) {
      if (!it) continue;
      if (it.sep) { if (out.length && !out[out.length - 1].sep) out.push(it); continue; }
      out.push(it.kids ? Object.assign({}, it, { kids: tidy(it.kids) }) : it);
    }
    while (out.length && out[out.length - 1].sep) out.pop();
    return out;
  }
  function openContext(el, cx, cy, w, src) {
    const a = A(); if (!a || !w || !el || typeof a.menuHtml !== 'function' || typeof a.openAt !== 'function') return false;
    const list = safe(() => contextList(w, src), null); if (!list) return false;
    el.classList.add('ce-aictx'); el.dataset.ctxkind = contextCase(w);   // (not data-case: that is the letter-case buttons' hook in the editor)
    // (the list goes in through openAt: it first closes the menu open before, which may be this same element, emptied as it closes)
    if (!a.openAt(el, cx, cy, tidy(list))) return false;
    el.classList.add('is-in');
    return true;
  }

  /* ═══ 4 · the shortcuts as a table: every command, Illustrator's Tools list and menus ═══ */
  const TOOL_ORDER = ['select', 'direct', 'marquee', 'pen', 'anchorAdd', 'anchorDel', 'anchorConvert', 'text', 'line', 'rect', 'shape-rrect', 'ellipse', 'polygon', 'shape-star',
    'shape-heart', 'shape-ring', 'shape-bail', 'pencil', 'scissors', 'tool.rotate', 'tool.reflect', 'tool.scale', 'cw-free', 'cw-persp', 'cw-blend', 'hand', 'zoom'];
  const BUILTIN = ['select', 'marquee', 'text', 'line', 'rect', 'ellipse', 'polygon', 'hand', 'zoom'];
  const MOD_ROWS = mac => [
    ['Hand Tool (temporary)', 'Space', 'hold, then drag'],
    ['Zoom In (temporary)', keyLabel('Mod+Space', mac), 'hold, then click or drag a box' + (mac ? ' (if macOS gives ⌘Space to Spotlight, use the Z key)' : '')],
    ['Zoom Out (temporary)', keyLabel('Mod+Alt+Space', mac), 'hold, then click'],
    ['Nudge', mac ? '← ↑ → ↓' : 'Left Up Right Down', '0.05 mm (Keyboard Increment)'],
    ['Nudge ×20', mac ? '⇧ ← ↑ → ↓' : 'Shift+arrows', '1 mm'],
    ['Duplicate While Dragging', mac ? '⌥-drag' : 'Alt+drag', ''],
    ['Constrain (proportions, 45°, 15° turns)', mac ? '⇧-drag' : 'Shift+drag', ''],
    ['Resize From the Center', mac ? '⌥-drag a handle' : 'Alt+drag a handle', 'or Ctrl'],
    ['Select Inside a Group', mac ? '⌥-click' : 'Alt+click', 'or double-click'],
    ['Cancel / Deselect / Selection Tool', 'Esc', 'one at a time; then closes the editor'],
    ['Delete', mac ? '⌫' : 'Del', 'or Backspace'],
    ['Edit Text', mac ? '↩' : 'Enter', 'or double-click the words'],
    ['Undo (Photoshop: Step Backward)', keyLabel('Mod+Alt+Z', mac), 'Photoshop\'s key, kept: Illustrator gives it nothing']
  ];
  function table(w, mac) {
    const a = A(); if (!a) return [];
    mac = mac == null ? isMac() : !!mac;
    const has = id => !w || BUILTIN.includes(id) || safe(() => w.x.hasTool(id), false);
    const lab = e => (typeof e.label === 'function' ? (w ? safe(() => e.label(w), '') : '') : e.menuLabel || e.label) || e.id;
    const kt = e => (e.keyText ? (e.key === 'Delete' ? (mac ? '⌫' : 'Del') : e.key === 'Enter' ? (mac ? '↩' : 'Enter') : e.keyText) : (e.key || e.sheetKey) ? keyLabel(e.key || e.sheetKey, mac) : '');
    const avail = e => !(e.x && w && !w.x.has(e.x)) && !(e.when && w && !safe(() => e.when(w), false));
    const rows = [];
    // Tools: Illustrator's Tools list order
    const catTools = new Map(a.catalog.filter(e => e.path === 'Tools' && /^tool\./.test(e.id)).map(e => [e.id, e]));
    for (const id of TOOL_ORDER) {
      if (catTools.has(id)) { const e = catTools.get(id); rows.push({ view: 'tools', menu: 'Tools', path: 'Tools', id: e.id, label: lab(e).replace(/ Tool.*$/, '') + ' Tool', key: e.key, keyText: kt(e), note: e.sheet || '' }); continue; }
      const t = a.tools[id]; if (!t || !has(id)) continue;
      rows.push({ view: 'tools', menu: 'Tools', path: 'Tools', id: 'tool:' + id, label: t.name, key: t.key || '', keyText: t.key ? keyLabel(t.key, mac) : '', note: '' });
    }
    // the tool keys that are not a tool here (Illustrator's Rotate / Reflect / Scale say where it is done) are in the catalog's Tools path: listed above; the others below
    for (const [n, k, note] of MOD_ROWS(mac)) rows.push({ view: 'tools', menu: 'Temporary Tools and Modifiers', path: 'Temporary Tools and Modifiers', id: 'mod:' + n, label: n, key: '', keyText: k, note });
    // Menu Commands: menu by menu, submenu by submenu, as the menu bar
    const seen = new Set();
    const push = (menu, e, subName, over) => {
      if (!e || seen.has(e.id) || !avail(e)) return; seen.add(e.id);
      rows.push(Object.assign({ view: 'menu', menu, path: menu + (subName ? ' ▸ ' + subName : ''), id: e.id, label: e.dyn ? lab(e) + ' (each)' : lab(e), key: e.key || e.sheetKey || '', keyText: kt(e), note: e.sheet || '' }, over || {}));
    };
    const byId = new Map(a.catalog.map(e => [e.id, e]));
    for (const [menu, spec] of a.menus || []) {
      for (const it of spec) {
        if (it === '-') continue;
        if (typeof it === 'string') push(menu, byId.get(it));
        else if (it.sub) for (const e of a.catalog) { if (e.path === menu && e.sub === it.sub) push(menu, e, it.sub); }
        else if (it.more && w) for (const c of w.x.list()) { if (c.menu === false || a.catalog.some(e => e.x === c.id) || seen.has(c.id) || /^ai\./.test(c.id)) continue; seen.add(c.id); rows.push({ view: 'menu', menu, path: menu + ' ▸ ' + (c.group || 'More'), id: c.id, label: c.title || c.id, key: c.key || '', keyText: c.key ? keyLabel(c.key, mac) : '', note: '' }); }
        else if (it.window) {
          for (const e of a.catalog) if (e.path === 'Window') push(menu, e);
          for (const wi of a.windowItems || []) { if (!wi || !wi.label || seen.has('win.x.' + wi.id)) continue; seen.add('win.x.' + wi.id); rows.push({ view: 'menu', menu, path: menu, id: 'win.x.' + wi.id, label: wi.label, key: wi.key || '', keyText: wi.key ? keyLabel(wi.key, mac) : '', note: '' }); }
        }
      }
      // the menu's entries with a key and no row of their own (Increase Font Size, Hide Panels ...)
      for (const e of a.catalog) if (e.path === menu && !seen.has(e.id) && (e.key || e.sheetKey)) push(menu, e, e.sub);
      for (const b of EXTRA) if (b.path === menu && !seen.has(b.id) && (!b.x || !w || w.x.has(b.x))) { seen.add(b.id); rows.push({ view: 'menu', menu, path: menu, id: b.id, label: b.label, key: b.key, keyText: keyLabel(b.key, mac), note: b.note || '' }); }
    }
    for (const e of a.catalog) if (e.path === 'Tools' && !/^tool\./.test(e.id) && !seen.has(e.id) && avail(e)) push('Other', e);
    for (const b of EXTRA) if (!seen.has(b.id) && (!b.x || !w || w.x.has(b.x))) { seen.add(b.id); rows.push({ view: 'menu', menu: b.path || 'Other', path: b.path || 'Other', id: b.id, label: b.label, key: b.key, keyText: keyLabel(b.key, mac), note: b.note || '' }); }
    return rows;
  }
  function label(id, mac) {
    const a = A(); if (!a) return '';
    const e = a.catalog.find(x => x.id === id) || EXTRA.find(x => x.id === id);
    const m = mac == null ? isMac() : !!mac;
    if (e) return e.key === 'Delete' && e.keyText ? (m ? '⌫' : 'Del') : e.key === 'Enter' && e.keyText ? (m ? '↩' : 'Enter') : (e.key || e.sheetKey) ? keyLabel(e.key || e.sheetKey, m) : '';
    const t = a.tools[id]; return t && t.key ? keyLabel(t.key, mac) : '';
  }

  /* ═══ 5 · Edit ▸ Keyboard Shortcuts… — Illustrator's dialog, read-only ═══ */
  const CSS = `
.ce-aik{ position:fixed; inset:0; z-index:60; display:flex; align-items:center; justify-content:center; background:rgba(0,0,0,.28); font-family:var(--ai-font,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif); font-size:var(--ai-fs,12px); color:var(--ai-text,#e3e3e3); }
.ce-aik__card{ width:min(640px,calc(100vw - 24px)); height:min(560px,calc(100vh - 24px)); display:flex; flex-direction:column; background:var(--ai-panel,#535353); border:1px solid var(--ai-menu-border,#2f2f2f); border-radius:6px; box-shadow:0 18px 50px rgba(0,0,0,.45); padding:14px 16px 12px; gap:10px; box-sizing:border-box; }
.ce-aik__title{ font-weight:700; font-size:13px; margin:0; color:var(--ai-text,#e3e3e3); }
.ce-aik__row{ display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
.ce-aik__lab{ color:var(--ai-text,#e3e3e3); }
.ce-aik select,.ce-aik input[type=search]{ height:22px; box-sizing:border-box; border:1px solid var(--ai-input-border,#3a3a3a); border-radius:3px; background:var(--ai-input,#454545); color:var(--ai-text,#e3e3e3); font:inherit; padding:0 6px; outline:none; }
.ce-aik select:focus-visible,.ce-aik input[type=search]:focus{ border-color:var(--ai-accent,#2680eb); box-shadow:0 0 0 1px var(--ai-accent,#2680eb); }
.ce-aik select[disabled]{ color:var(--ai-text-dim,#a9a9a9); }
.ce-aik__set{ min-width:220px; }
.ce-aik__q{ margin-left:auto; display:flex; align-items:center; gap:6px; }
.ce-aik__q input{ width:190px; }
.ce-aik__os{ display:inline-flex; border:1px solid var(--ai-input-border,#3a3a3a); border-radius:3px; overflow:hidden; }
.ce-aik__os button{ height:20px; padding:0 10px; border:0; background:var(--ai-input,#454545); color:var(--ai-text-dim,#a9a9a9); font:inherit; cursor:default; }
.ce-aik__os button.is-on{ background:var(--ai-accent,#2680eb); color:var(--ai-accent-text,#fff); }
.ce-aik__grid{ flex:1; min-height:0; overflow:auto; border:1px solid var(--ai-border,#3a3a3a); background:var(--ai-input,#454545); border-radius:2px; }
.ce-aik__t{ width:100%; border-collapse:collapse; table-layout:fixed; }
.ce-aik__t th{ position:sticky; top:0; z-index:1; text-align:left; font-weight:400; color:var(--ai-text-dim,#a9a9a9); background:var(--ai-panel-hi,#5c5c5c); border-bottom:1px solid var(--ai-border,#3a3a3a); height:22px; padding:0 8px; }
.ce-aik__t th:nth-child(2),.ce-aik__t td:nth-child(2){ width:34%; }
.ce-aik__t td{ height:var(--ai-row,22px); padding:0 8px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; border-bottom:1px solid rgba(0,0,0,.08); cursor:default; }
.ce-aik__t tr.is-head td{ font-weight:700; }
.ce-aik__t tr.is-head td::before{ content:''; display:inline-block; width:0; height:0; margin-right:6px; vertical-align:1px; border:4px solid transparent; border-top:5px solid currentColor; border-bottom:0; }
.ce-aik__t tr.is-head.is-shut td::before{ border:4px solid transparent; border-left:5px solid currentColor; border-right:0; vertical-align:0; }
.ce-aik__t tr.is-sub td:first-child{ padding-left:22px; }
.ce-aik__t tr.is-sub.is-head td:first-child{ padding-left:22px; font-weight:600; }
.ce-aik__t tr.is-deep td:first-child{ padding-left:40px; }
.ce-aik__t tr.is-cur td{ background:var(--ai-menu-hover,#2680eb); color:var(--ai-accent-text,#fff); }
.ce-aik__t td kbd{ font:inherit; }
.ce-aik__none td{ color:var(--ai-text-dim,#a9a9a9); text-align:center; height:44px; }
.ce-aik__desc{ min-height:32px; color:var(--ai-text-dim,#a9a9a9); line-height:1.35; }
.ce-aik__foot{ display:flex; align-items:center; gap:8px; }
.ce-aik__foot .ce-aik__gap{ flex:1; }
.ce-aik__btn{ height:24px; padding:0 14px; border-radius:12px; border:1px solid var(--ai-text-dim,#a9a9a9); background:transparent; color:var(--ai-text,#e3e3e3); font:600 var(--ai-fs,12px)/1 var(--ai-font,sans-serif); cursor:default; }
.ce-aik__btn:hover{ background:var(--ai-hover,#404040); }
.ce-aik__btn--cta{ background:var(--ai-accent,#2680eb); border-color:var(--ai-accent,#2680eb); color:var(--ai-accent-text,#fff); }
.ce-aik__btn--cta:hover{ background:var(--ai-accent,#2680eb); filter:brightness(1.08); }
@media (max-width:560px){ .ce-aik__q{ margin-left:0; width:100%; } .ce-aik__q input{ flex:1; width:auto; } .ce-aik__set{ min-width:0; flex:1; } }
`;
  function injectCss() {
    if (!doc || doc.getElementById('ceAiKeysCss')) return;
    const s = doc.createElement('style'); s.id = 'ceAiKeysCss'; s.textContent = CSS; (doc.head || doc.documentElement).appendChild(s);
  }
  const VIEWS = [['tools', 'Tools'], ['menu', 'Menu Commands']];
  function showShortcuts(w, opts) {
    const a = A(); if (!doc || !a) return null;
    w = w || (typeof a.handle === 'function' ? a.handle() : null) || (root.CharmEditor && root.CharmEditor.current && root.CharmEditor.current.ai) || null;
    opts = opts || {};
    injectCss();
    let mac = opts.mac != null ? !!opts.mac : (a.prefs && a.prefs().keys ? a.prefs().keys === 'mac' : isMac());
    let view = opts.view === 'tools' ? 'tools' : opts.view === 'menu' ? 'menu' : 'menu';
    const shut = new Set();
    const box = doc.createElement('div'); box.className = 'ce-aik';
    box.innerHTML = `<div class="ce-aik__card" role="dialog" aria-modal="true" aria-labelledby="ceAikT">
<h3 class="ce-aik__title" id="ceAikT">Keyboard Shortcuts</h3>
<div class="ce-aik__row"><label class="ce-aik__lab" for="ceAikSet">Set:</label><select class="ce-aik__set" id="ceAikSet" disabled title="The Charm Editor uses Illustrator's default keys: they cannot be changed here"><option>Illustrator Defaults (Read-Only)</option></select>
<span class="ce-aik__os" role="radiogroup" aria-label="Keys for"><button type="button" role="radio" data-aik-os="mac">Mac</button><button type="button" role="radio" data-aik-os="win">Windows</button></span></div>
<div class="ce-aik__row"><select data-aik-view aria-label="Tools or Menu Commands">${VIEWS.map(([v, n]) => `<option value="${v}">${n}</option>`).join('')}</select>
<label class="ce-aik__q"><span class="ce-aik__lab">Search</span><input type="search" data-aik-q autocomplete="off" spellcheck="false" aria-label="Search the shortcuts" placeholder=""></label></div>
<div class="ce-aik__grid" tabindex="-1"><table class="ce-aik__t" role="treegrid" aria-label="Shortcuts"><thead><tr><th>Command</th><th>Shortcut</th></tr></thead><tbody data-aik-rows></tbody></table></div>
<div class="ce-aik__desc" data-aik-desc aria-live="polite">Illustrator's default keys. A Photoshop key is kept only where Illustrator gives that key nothing. Keys the browser keeps for itself (new tab, close tab, quit) are not used.</div>
<div class="ce-aik__foot"><button type="button" class="ce-aik__btn" data-aik="export">Export Text…</button><span class="ce-aik__gap"></span><button type="button" class="ce-aik__btn" data-aik="cancel">Cancel</button><button type="button" class="ce-aik__btn ce-aik__btn--cta" data-aik="ok">OK</button></div></div>`;
    let closeFn = null;
    const cleanup = () => { if (box.parentNode) box.parentNode.removeChild(box); };
    if (w && typeof w.modal === 'function') {
      // (the editor's keys wait while it is open, as for its own questions; its Esc closes it)
      w.win.appendChild(box);
      const done = w.modal(box, () => {});
      closeFn = ok => { done(!!ok); cleanup(); };
    } else {
      doc.body.appendChild(box);
      const onEsc = ev => { if (ev.key === 'Escape') { ev.preventDefault(); ev.stopPropagation(); closeFn(false); } };
      root.addEventListener('keydown', onEsc, true);
      closeFn = () => { root.removeEventListener('keydown', onEsc, true); cleanup(); };
    }
    const q = box.querySelector('[data-aik-q]'), tb = box.querySelector('[data-aik-rows]'), desc = box.querySelector('[data-aik-desc]'), viewSel = box.querySelector('[data-aik-view]');
    let rows = [], cur = null;
    const paint = () => {
      rows = table(w, mac);
      const needle = String(q.value || '').trim().toLowerCase();
      const hit = r => !needle || (r.label + ' ' + r.path + ' ' + r.keyText + ' ' + (r.note || '')).toLowerCase().includes(needle);
      let h = '';
      const list = rows.filter(r => r.view === view && hit(r));
      if (view === 'tools') {
        let grp = null;
        for (const r of list) {
          if (r.menu !== grp) { grp = r.menu; if (grp !== 'Tools') h += `<tr class="is-head" data-aik-head="${esc(grp)}"><td colspan="2">${esc(grp)}</td></tr>`; }
          if (shut.has(grp) && !needle) continue;
          h += rowHtml(r, grp !== 'Tools' ? 'is-sub' : '');
        }
      } else {
        let m = null, p = null;
        for (const r of list) {
          if (r.menu !== m) { m = r.menu; p = null; h += `<tr class="is-head${shut.has(m) && !needle ? ' is-shut' : ''}" data-aik-head="${esc(m)}" aria-expanded="${!(shut.has(m) && !needle)}"><td colspan="2">${esc(m)}</td></tr>`; }
          if (shut.has(m) && !needle) continue;
          const subName = r.path !== m ? r.path.slice(m.length + 3) : null;
          if (subName && subName !== p) { h += `<tr class="is-head is-sub${shut.has(r.path) && !needle ? ' is-shut' : ''}" data-aik-head="${esc(r.path)}"><td colspan="2">${esc(subName)}</td></tr>`; }
          p = subName;
          if (subName && shut.has(r.path) && !needle) continue;
          h += rowHtml(r, subName ? 'is-deep' : 'is-sub');
        }
      }
      tb.innerHTML = h || '<tr class="ce-aik__none"><td colspan="2">No command matches.</td></tr>';
      box.querySelectorAll('[data-aik-os]').forEach(b => { const on = (b.dataset.aikOs === 'mac') === mac; b.classList.toggle('is-on', on); b.setAttribute('aria-checked', String(on)); });
      viewSel.value = view;
      if (cur) { const tr = tb.querySelector(`tr[data-aik-id="${cssEsc(cur)}"]`); if (tr) tr.classList.add('is-cur'); }
    };
    const rowHtml = (r, cls) => `<tr class="${cls}" data-aik-id="${esc(r.id)}" tabindex="-1"><td title="${esc(r.label)}">${esc(r.label)}</td><td><kbd>${esc(r.keyText)}</kbd></td></tr>`;
    const pick = tr => {
      tb.querySelectorAll('tr.is-cur').forEach(x => x.classList.remove('is-cur'));
      if (!tr || !tr.dataset.aikId) { cur = null; return; }
      tr.classList.add('is-cur'); cur = tr.dataset.aikId;
      const r = rows.find(x => x.id === cur);
      if (r) desc.textContent = `${r.path} ▸ ${r.label}${r.keyText ? '  ·  ' + r.keyText : '  ·  no shortcut'}${r.note ? '  —  ' + r.note : ''}`;
      try { tr.scrollIntoView({ block: 'nearest' }); } catch (e) { /* fine */ }
    };
    box.addEventListener('click', ev => {
      const os = ev.target.closest('[data-aik-os]'); if (os) { mac = os.dataset.aikOs === 'mac'; if (a.setPrefs) a.setPrefs({ keys: mac ? 'mac' : 'win' }); paint(); return; }
      const b = ev.target.closest('[data-aik]');
      if (b) { if (b.dataset.aik === 'export') exportText(); else closeFn(b.dataset.aik === 'ok'); return; }
      const hd = ev.target.closest('[data-aik-head]'); if (hd) { const k = hd.dataset.aikHead; if (shut.has(k)) shut.delete(k); else shut.add(k); paint(); return; }
      const tr = ev.target.closest('tr[data-aik-id]'); if (tr) pick(tr);
      if (ev.target === box) closeFn(false);
    });
    viewSel.addEventListener('change', () => { view = viewSel.value === 'tools' ? 'tools' : 'menu'; cur = null; paint(); });
    q.addEventListener('input', paint);
    box.addEventListener('keydown', ev => {
      if (ev.key === 'ArrowDown' || ev.key === 'ArrowUp') {
        if (ev.target === viewSel) return;
        ev.preventDefault();
        const trs = [...tb.querySelectorAll('tr[data-aik-id]')]; if (!trs.length) return;
        const i = trs.findIndex(t => t.dataset.aikId === cur);
        pick(trs[i < 0 ? 0 : Math.max(0, Math.min(trs.length - 1, i + (ev.key === 'ArrowDown' ? 1 : -1)))]);
      } else if (ev.key === 'Enter' && ev.target === q) { ev.preventDefault(); }
    });
    function exportText() {
      const lines = ['Charm Editor — Keyboard Shortcuts (Illustrator Defaults) — ' + (mac ? 'Mac' : 'Windows'), ''];
      for (const [v, n] of VIEWS) {
        lines.push(n.toUpperCase());
        let p = null;
        for (const r of table(w, mac).filter(x => x.view === v)) { if (r.path !== p) { p = r.path; lines.push('', '  ' + p); } lines.push('    ' + r.label + (r.keyText ? '\t' + r.keyText : '') + (r.note ? '\t(' + r.note + ')' : '')); }
        lines.push('');
      }
      try {
        const blob = new root.Blob([lines.join('\n')], { type: 'text/plain' }), url = root.URL.createObjectURL(blob), aEl = doc.createElement('a');
        aEl.href = url; aEl.download = 'Charm Editor Keyboard Shortcuts.txt'; doc.body.appendChild(aEl); aEl.click(); aEl.remove(); setTimeout(() => root.URL.revokeObjectURL(url), 2000);
      } catch (er) { /* the browser refused the download: nothing to do */ }
      return lines.join('\n');
    }
    paint();
    requestAnimationFrame(() => { try { q.focus({ preventScroll: true }); } catch (e) { /* fine */ } });
    return { box, close: ok => closeFn(!!ok), exportText, paint, setView: v => { view = v === 'tools' ? 'tools' : 'menu'; paint(); } };
  }
  const cssEsc = s => (root.CSS && root.CSS.escape ? root.CSS.escape(s) : String(s).replace(/["\\]/g, '\\$&'));
  const requestAnimationFrame = f => (root.requestAnimationFrame ? root.requestAnimationFrame(f) : setTimeout(f, 16));

  /* ═══ 6 · plugging in: the Select commands on the editor's extension points, this file's sheet and right-click menus on the shell ═══ */
  let attached = false;
  function attach() {
    if (!doc) return;
    const CE = root.CharmEditor || (root.CharmEditor = {});
    CE.commands = CE.commands && typeof CE.commands === 'object' ? CE.commands : {};
    for (const id of Object.keys(SELECT_CMDS)) if (!CE.commands[id]) CE.commands[id] = SELECT_CMDS[id];
    const a = A();
    if (a) { a.keys = api; a.openShortcuts = () => showShortcuts(); a.showShortcuts = (w, o) => showShortcuts(w, o); attached = true; }
    else if (!attached && doc.readyState === 'loading') doc.addEventListener('DOMContentLoaded', () => { if (A() && !attached) attach(); }, { once: true });
  }

  const api = { key, keyUp, contextList, contextCase, openContext, table, label, bind, showShortcuts, attach, stackPick, SELECT_CMDS, version: 1 };
  return api;
});
