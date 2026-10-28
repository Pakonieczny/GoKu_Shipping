/* charm-nest-ce-ai-shell.js — THE CHARM EDITOR AS ILLUSTRATOR: the menu bar, the context menus, the keyboard and the preferences (Paul, 28 Oct 2026: "update the UI for the Entire
 * Charm Editor, all context menus, layer area etc.. The idea is that my employees know how to operate Adobe illustrator very well and they know the exact position, size,
 * iconography ..., groupings of options, all expected keyboard shortcuts (Illustrator, Photoshop). My expectation is that you basically replicate the UI for illustrator while
 * also keeping some of the specific functions that are dedicated to our platform available ... I'm not looking for you to change the functionality of things").
 *
 * This file changes no work: every item it shows runs something the editor (charm-nest-charm-editor.js) or one of its extension files already does. It holds the ONE COMMAND
 * CATALOG of the editor's front and draws from it the menu bar (File Edit Object Type Select Effect View Window Help, in Illustrator's order, with Illustrator's item names,
 * groupings, submenus and shortcut column), the Tools panel's hover cards, Edit ▸ Preferences (UI brightness, canvas colour, the Toolbar's columns) and Help. The right-click
 * menus, the keys themselves and the shortcuts sheet are charm-nest-ce-ai-keys.js's (a helper of its own), built on this catalog: an entry's `key` is what the menus show and
 * what that file binds. An item the editor cannot do is not shown; an item is greyed only where Illustrator greys it (nothing selected, nothing to paste ...).
 *
 *   window.CharmEditorAi (also CharmEditor.ai once the editor is loaded)
 *     catalog                [{ id, path, label, key?, x?, ctx?, run?, on?, chk?, when? }]   every command: path 'Edit' / 'Object ▸ Path' (its place in the menus), key in the editor's
 *                            spelling ('Mod+Shift+Z': Mod is ⌘ on a Mac, Ctrl elsewhere), x: an extension command's id (CharmEditor.commands) it runs, ctx: the editor's own
 *                            right-click action it runs; run(w, arg) / on(w) / chk(w) / when(w) take the workspace handle w (below)
 *     tools                  { [toolId]: { name, key, desc } }       the Tools panel's names and letters (the hover cards, the shortcuts sheet)
 *     windowItems            [{ id, label, key?, run(w), chk?(w), when?(w) }]   more Window menu entries (a docked panel of charm-nest's own registers itself here)
 *     keyLabel(spec, mac?)   'Mod+Shift+Z' -> '⇧⌘Z' on a Mac, 'Shift+Ctrl+Z' on Windows (Illustrator's own spelling on each)
 *     mac                    true on a Mac (the shortcut column's glyphs)
 *     prefs() / setPrefs(p)  { ui: 'dark' | 'mdark' | 'mlight' | 'light', canvas: 'match' | 'white', cols: 1 | 2 }  (localStorage 'ce.ai', per person, per browser)
 *     mount(w)               called by the editor as a workspace opens: -> { menuKey(e), keyUp(e), closed(), tip(id), sync(), focusBar(), openAt(el, cx, cy, list) }
 *     resolve(w, id) / menuHtml(list) / subItems(w, path, name) / dynItems(w, 'fonts' | 'sizes' | 'cases') / openAt(el, cx, cy, list)   the menus' parts, for another menu
 *     openShortcuts()        Edit ▸ Keyboard Shortcuts… (a read-only sheet here; charm-nest-ce-ai-keys.js may replace it with its own)
 *     run(id, arg)           a catalog command on the open workspace (a docked panel's own menu reuses the catalog this way)
 *
 *   w (the workspace handle the editor hands in; charm-nest-charm-editor.js builds it)
 *     st()   the state the menus read: { n, anyText, anyGroup, anyPath, canUndo, canRedo, clip, face, faces, tool, opts, outline, rulers, dock, xpanel, panels, importers,
 *            back, screen, fonts, font, size, style, align, letterCase, textTool, editing }
 *     act(name, arg)   the editor's own actions (undo, cut, group, lock, face, opt, zoom, save, cancel, import ...)
 *     x      { has(id), enabled(id), run(id), title(id) }  the extension commands;   root, win   the elements;   modal(el) -> close()   a dialog over the work
 */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api;
  else { root.CharmEditorAi = api; if (root.CharmEditor && typeof root.CharmEditor === 'object') root.CharmEditor.ai = api; }
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const doc = root && root.document ? root.document : null;
  const nav = root && root.navigator ? root.navigator : null;
  const MAC = !!nav && (/Mac|iPhone|iPad|iPod/.test(nav.platform || '') || /Mac OS X/.test(nav.userAgent || ''));
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

  /* ═══ 1 · keys: the editor's spelling, Illustrator's labels, an event matched ═══ */
  /** 'Mod+Alt+Shift+8' -> { mod, shift, alt, key: '8' } (Mod, Cmd, Ctrl and ⌘ are the platform's command key, as the editor's own keys take either). */
  function parse(spec) {
    const raw = String(spec || ''); if (!raw) return null;
    const parts = raw === '+' ? ['+'] : raw.replace(/\+\+$/, '+Plus').split('+').map(p => p.trim()).filter(Boolean);
    if (!parts.length) return null;
    const k = { mod: false, shift: false, alt: false, key: parts.pop() };
    if (k.key === 'Plus') k.key = '+';
    for (const p of parts.map(q => q.toLowerCase())) {
      if (/^(mod|cmd|ctrl|control|meta|⌘)$/.test(p)) k.mod = true; else if (/^(shift|⇧)$/.test(p)) k.shift = true; else if (/^(alt|opt|option|⌥)$/.test(p)) k.alt = true;
    }
    return k;
  }
  const MAC_KEY = { Tab: '⇥', Backspace: '⌫', Delete: '⌦', Enter: '↩', Escape: 'Esc', ArrowLeft: '←', ArrowRight: '→', ArrowUp: '↑', ArrowDown: '↓', Space: 'Space', '-': '-', '=': '+', '+': '+' };
  const WIN_KEY = { Tab: 'Tab', Backspace: 'Backspace', Delete: 'Del', Enter: 'Enter', Escape: 'Esc', ArrowLeft: 'Left', ArrowRight: 'Right', ArrowUp: 'Up', ArrowDown: 'Down', Space: 'Space', '-': '-', '=': '+', '+': '+' };
  /** Illustrator's own spelling of a shortcut: ⌥⇧⌘8 on a Mac, Alt+Shift+Ctrl+8 on Windows. */
  function keyLabel(spec, mac) {
    const k = parse(spec); if (!k) return '';
    const m = mac == null ? MAC : !!mac, map = m ? MAC_KEY : WIN_KEY;
    const name = map[k.key] || (k.key.length === 1 ? k.key.toUpperCase() : k.key);
    if (m) return (k.alt ? '⌥' : '') + (k.shift ? '⇧' : '') + (k.mod ? '⌘' : '') + name;
    return [k.alt ? 'Alt' : '', k.shift ? 'Shift' : '', k.mod ? 'Ctrl' : '', name].filter(Boolean).join('+');
  }
  const CODE = { "'": 'Quote', ';': 'Semicolon', ']': 'BracketRight', '[': 'BracketLeft', ',': 'Comma', '.': 'Period', '\\': 'Backslash', '/': 'Slash', '`': 'Backquote', '-': 'Minus', '=': 'Equal' };
  /** Does a key event say this shortcut (the letter by its key or its physical key, so a Shift that changes the character still matches)? */
  function matches(spec, e) {
    const k = typeof spec === 'string' ? parse(spec) : spec; if (!k) return false;
    const mod = !!(e.metaKey || e.ctrlKey);
    if (k.mod !== mod || k.alt !== !!e.altKey) return false;
    const key = String(e.key || ''), code = String(e.code || '');
    if (k.key === '+') return (key === '+' || key === '=' || code === 'Equal' || code === 'NumpadAdd');
    if (k.shift !== !!e.shiftKey) return false;
    if (/^F\d+$/.test(k.key) || k.key.length > 1) return key === k.key || (k.key === 'Space' && (key === ' ' || code === 'Space'));
    const want = k.key.toLowerCase();
    if (key.toLowerCase() === want) return true;
    if (/^[a-z]$/.test(want)) return code === 'Key' + want.toUpperCase();
    if (/^[0-9]$/.test(want)) return code === 'Digit' + want || code === 'Numpad' + want;
    return !!CODE[want] && code === CODE[want];
  }

  /* ═══ 2 · preferences (Edit ▸ Preferences ▸ User Interface): per person, per browser ═══ */
  const UI_LEVELS = [['dark', 'Dark'], ['mdark', 'Medium Dark'], ['mlight', 'Medium Light'], ['light', 'Light']];
  const DEFAULT_PREFS = { ui: 'mdark', canvas: 'match', cols: 1 };   // (Medium Dark: Illustrator's own default brightness since CS6; Canvas Color: Match User Interface Brightness)
  function prefs() {
    let p = {};
    try { p = JSON.parse((root.localStorage && root.localStorage.getItem('ce.ai')) || '{}') || {}; } catch (e) { p = {}; }
    const out = Object.assign({}, DEFAULT_PREFS);
    if (UI_LEVELS.some(l => l[0] === p.ui)) out.ui = p.ui;
    if (p.canvas === 'white' || p.canvas === 'match') out.canvas = p.canvas;
    if (p.cols === 2 || p.cols === 1) out.cols = p.cols;
    if (p.keys === 'mac' || p.keys === 'win') out.keys = p.keys;
    return out;
  }
  function setPrefs(patch) {
    const p = Object.assign(prefs(), patch || {});
    try { if (root.localStorage) root.localStorage.setItem('ce.ai', JSON.stringify(p)); } catch (e) { /* storage blocked: this session only */ }
    if (live && live.w && typeof live.w.applyPrefs === 'function') live.w.applyPrefs(p);
    return p;
  }

  /* ═══ 3 · THE CATALOG: every command once — its place in the menus, its name, its key, what it runs ═══ */
  const st = w => w.st();
  const sel = w => st(w).n > 0;
  const textish = w => { const s = st(w); return s.anyText || (s.textTool && !s.n); };
  const C = [];
  const add = (path, list) => { for (const e of list) C.push(Object.assign({ path }, e)); };
  const act = (name, arg) => w => w.act(name, arg);
  const xe = (id, x, label, key, extra) => Object.assign({ id, x, label, key }, extra || {});

  add('File', [
    { id: 'file.open', label: 'Open…', key: 'Mod+O', when: w => st(w).panels.some(p => p.id === 'xlib'), run: w => w.act('xpanel', 'xlib'), sheet: 'Opens the Library: a design or a charm from the library' },
    { id: 'file.close', label: 'Close', run: act('cancel') },
    { id: 'file.back', label: w => st(w).back || 'Close and Return', when: w => !!st(w).back, run: act('back') },
    { id: 'file.save', label: 'Save', key: 'Mod+S', run: act('save'), own: true },   // (own: the editor's own key already saves)
    xe('file.sendToSheet', 'sendToSheet', 'Send to Sheet…'),
    { id: 'file.place', label: 'Place…', key: 'Mod+Shift+P', when: w => st(w).importers > 0, run: act('import') }
  ]);
  add('Edit', [
    { id: 'edit.undo', label: 'Undo', key: 'Mod+Z', on: w => st(w).canUndo, run: act('undo'), own: true },
    { id: 'edit.redo', label: 'Redo', key: 'Mod+Shift+Z', on: w => st(w).canRedo, run: act('redo'), own: true },
    { id: 'edit.cut', label: 'Cut', key: 'Mod+X', ctx: 'cut', on: sel, own: true },
    { id: 'edit.copy', label: 'Copy', key: 'Mod+C', ctx: 'copy', on: sel, own: true },
    { id: 'edit.paste', label: 'Paste', key: 'Mod+V', ctx: 'paste', on: w => st(w).clip, own: true },
    { id: 'edit.pasteFront', label: 'Paste in Front', key: 'Mod+F', on: w => st(w).clip, run: act('pasteFront') },
    { id: 'edit.pasteBack', label: 'Paste in Back', key: 'Mod+B', on: w => st(w).clip, run: act('pasteBack') },
    { id: 'edit.pasteInPlace', label: 'Paste in Place', key: 'Mod+Shift+V', ctx: 'pasteInPlace', on: w => st(w).clip, own: true },
    { id: 'edit.clear', label: 'Clear', key: 'Delete', ctx: 'del', on: sel, own: true, keyText: MAC ? '⌫' : 'Del' },
    { id: 'edit.duplicate', label: 'Duplicate', ctx: 'dup', on: sel, sheetKey: 'Mod+D', sheet: 'when there is no transform to repeat; or Alt-drag' },
    { id: 'edit.shortcuts', label: 'Keyboard Shortcuts…', key: 'Mod+Alt+Shift+K', run: () => api.openShortcuts() },
    { id: 'edit.prefs.general', label: 'General…', key: 'Mod+K', run: w => prefsDialog(w, 'general'), sub: 'Preferences' },
    { id: 'edit.prefs.ui', label: 'User Interface…', run: w => prefsDialog(w, 'ui'), sub: 'Preferences' },
    { id: 'edit.prefs.guides', label: 'Guides & Grid…', run: w => prefsDialog(w, 'guides'), sub: 'Preferences' }
  ]);
  add('Object', [
    xe('obj.again', 'ops.again', 'Transform Again', 'Mod+D', { sub: 'Transform', ownKey: true }),
    xe('obj.move', 'ops.move', 'Move…', 'Mod+Shift+M', { sub: 'Transform' }),
    xe('obj.rotate', 'ops.rotate', 'Rotate…', null, { sub: 'Transform' }),
    xe('obj.reflect', 'ops.reflect', 'Reflect…', null, { sub: 'Transform' }),
    xe('obj.scale', 'ops.scale', 'Scale…', null, { sub: 'Transform' }),
    xe('obj.flipH', 'ops.flipH', 'Flip Horizontal', null, { sub: 'Transform' }),
    xe('obj.flipV', 'ops.flipV', 'Flip Vertical', null, { sub: 'Transform' }),
    xe('obj.rot90', 'ops.rot90', 'Rotate 90° Clockwise', null, { sub: 'Transform' }),
    xe('obj.rot90l', 'ops.rot90l', 'Rotate 90° Counterclockwise', null, { sub: 'Transform' }),
    xe('obj.repeat', 'ops.repeat', 'Step and Repeat…', null, { sub: 'Transform' }),
    { id: 'obj.front', label: 'Bring to Front', key: 'Mod+Shift+]', ctx: 'front', on: sel, sub: 'Arrange', own: true },
    { id: 'obj.forward', label: 'Bring Forward', key: 'Mod+]', ctx: 'forward', on: sel, sub: 'Arrange', own: true },
    { id: 'obj.backward', label: 'Send Backward', key: 'Mod+[', ctx: 'backward', on: sel, sub: 'Arrange', own: true },
    { id: 'obj.back', label: 'Send to Back', key: 'Mod+Shift+[', ctx: 'back', on: sel, sub: 'Arrange', own: true },
    xe('obj.centre', 'ops.centre', 'Center on the Charm', null, { sub: 'Align' }),
    xe('obj.alignBar', 'ops.alignBar', 'Align & Distribute…', null, { sub: 'Align', menuOk: true }),
    { id: 'obj.group', label: 'Group', key: 'Mod+G', ctx: 'group', on: w => st(w).n > 1, own: true },
    { id: 'obj.ungroup', label: 'Ungroup', key: 'Mod+Shift+G', ctx: 'ungroup', on: w => st(w).anyGroup, own: true },
    { id: 'obj.lock', label: 'Selection', key: 'Mod+2', on: sel, run: act('lock'), sub: 'Lock' },
    xe('obj.lockOthers', 'ops.lockOthers', 'Everything Else', null, { sub: 'Lock' }),
    { id: 'obj.unlockAll', label: 'Unlock All', key: 'Mod+Alt+2', run: act('unlockAll') },
    { id: 'obj.hide', label: 'Selection', key: 'Mod+3', on: sel, run: act('hide'), sub: 'Hide' },
    xe('obj.hideOthers', 'ops.hideOthers', 'Everything Else', null, { sub: 'Hide' }),
    { id: 'obj.showAll', label: 'Show All', key: 'Mod+Alt+3', run: act('showAll') },
    xe('obj.join', 'pathJoin', 'Join', 'Mod+J', { sub: 'Path' }),
    xe('obj.close', 'pathClose', 'Close Path', null, { sub: 'Path' }),
    xe('obj.outline', 'ops.outline', 'Outline Stroke', null, { sub: 'Path' }),
    xe('obj.offset', 'ops.offset', 'Offset Path…', null, { sub: 'Path' }),
    xe('obj.contour', 'ops.contour', 'Cut Line Round the Art…', null, { sub: 'Path' }),
    xe('obj.simplify', 'pathSimplify', 'Simplify…', null, { sub: 'Path' }),
    xe('obj.smooth', 'pathSmooth', 'Smooth', null, { sub: 'Path' }),
    xe('obj.anchCorner', 'anchorsCorner', 'Convert Anchor Points to Corner', null, { sub: 'Path' }),
    xe('obj.anchSmooth', 'anchorsSmooth', 'Convert Anchor Points to Smooth', null, { sub: 'Path' }),
    xe('obj.anchRemove', 'anchorsRemove', 'Remove Anchor Points', null, { sub: 'Path' }),
    xe('obj.anchCut', 'anchorsCut', 'Cut Path at Selected Anchor Points', null, { sub: 'Path' }),
    xe('obj.jumpRing', 'cx.jumpRing', 'Jump Ring on Top', null, { sub: 'Shape' }),
    xe('obj.blendMake', 'cw-blend-make', 'Make', 'Mod+Alt+B', { sub: 'Blend' }),
    xe('obj.blendRelease', 'cw-blend-release', 'Release', 'Mod+Alt+Shift+B', { sub: 'Blend' }),
    xe('obj.blendOptions', 'cw-blend-options', 'Blend Options…', null, { sub: 'Blend' }),
    xe('obj.blendExpand', 'cw-blend-expand', 'Expand', null, { sub: 'Blend' }),
    xe('obj.blendSpine', 'cw-blend-spine', 'Replace Spine', null, { sub: 'Blend' }),
    xe('obj.warp', 'cw-warp', 'Make with Warp…', 'Mod+Alt+Shift+W', { sub: 'Envelope Distort' }),
    xe('obj.warpExpand', 'cw-warp-expand', 'Expand', null, { sub: 'Envelope Distort' }),
    xe('obj.retrace', 'imageRetrace', 'Re-trace…', null, { sub: 'Image Trace' }),
    xe('obj.clip', 'cx.clip', 'Make (Clip to the Charm)', 'Mod+7', { sub: 'Clipping Mask' }),
    xe('obj.unclip', 'cx.unclip', 'Release', 'Mod+Alt+7', { sub: 'Clipping Mask' }),
    xe('obj.compound', 'ops.compound', 'Make', 'Mod+8', { sub: 'Compound Path' }),
    xe('obj.uncompound', 'ops.release', 'Release', 'Mod+Alt+Shift+8', { sub: 'Compound Path' })
  ]);
  const SIZES = [6, 7, 8, 9, 10, 11, 12, 14, 18, 21, 24, 36, 48, 60, 72];
  const CASES = [['upper', 'UPPERCASE'], ['lower', 'lowercase'], ['title', 'Title Case'], ['small', 'Small Caps'], ['', 'As Typed']];
  add('Type', [
    { id: 'type.font', label: 'Font', dyn: 'fonts', sub: 'Font' },
    { id: 'type.size', label: 'Size', dyn: 'sizes', sub: 'Size' },
    { id: 'type.regular', label: 'Regular', sub: 'Font Style', on: textish, chk: w => { const s = st(w).style; return !!s && !s.bold && !s.italic; }, run: w => w.act('style', { bold: undefined, italic: undefined }) },
    { id: 'type.bold', label: 'Bold', key: 'Mod+Shift+B', sub: 'Font Style', on: textish, chk: w => !!(st(w).style || {}).bold, run: act('toggleStyle', 'bold'), keyWhen: textish, sheet: 'Photoshop\'s faux bold key: Illustrator has none' },
    { id: 'type.italic', label: 'Italic', key: 'Mod+Shift+I', sub: 'Font Style', on: textish, chk: w => !!(st(w).style || {}).italic, run: act('toggleStyle', 'italic'), keyWhen: textish, sheet: 'Photoshop\'s faux italic key; ⌘I as before' },
    { id: 'type.glyphs', label: 'Glyphs…', on: textish, run: act('alphaFind'), sheet: 'Letters, words and phrases of other alphabets, found by their English' },
    xe('type.onPath', 'cx.onPath', 'Words Along the Path', null, { sub: 'Type on a Path' }),
    xe('type.offPath', 'cx.offPath', 'Words Off the Path', null, { sub: 'Type on a Path' }),
    { id: 'type.case', label: 'Change Case', dyn: 'cases', sub: 'Change Case' },
    { id: 'type.alignL', label: 'Align Left', key: 'Mod+Shift+L', sub: 'Paragraph', on: textish, chk: w => st(w).align === 'left', run: w => w.act('style', { align: 'left' }), keyWhen: textish },
    { id: 'type.alignC', label: 'Align Center', key: 'Mod+Shift+C', sub: 'Paragraph', on: textish, chk: w => st(w).align === 'center', run: w => w.act('style', { align: 'center' }), keyWhen: textish },
    { id: 'type.alignR', label: 'Align Right', key: 'Mod+Shift+R', sub: 'Paragraph', on: textish, chk: w => st(w).align === 'right', run: w => w.act('style', { align: 'right' }), keyWhen: textish },
    { id: 'type.bigger', label: 'Increase Font Size', key: 'Mod+Shift+.', on: textish, run: w => w.act('sizeStep', 1), keyWhen: textish, menu: false },
    { id: 'type.smaller', label: 'Decrease Font Size', key: 'Mod+Shift+,', on: textish, run: w => w.act('sizeStep', -1), keyWhen: textish, menu: false },
    { id: 'type.edit', label: 'Edit Text', key: 'Enter', ctx: 'edit', on: w => st(w).n === 1 && st(w).anyText, own: true, keyText: MAC ? '↩' : 'Enter' }
  ]);
  add('Select', [
    { id: 'sel.all', label: 'All', key: 'Mod+A', run: act('selectAll'), own: true },
    { id: 'sel.allBoard', label: 'All on Active Artboard', key: 'Mod+Alt+A', run: act('selectAll') },
    { id: 'sel.none', label: 'Deselect', key: 'Mod+Shift+A', on: sel, run: act('deselect') },
    xe('sel.firstAbove', 'ai.selFirstAbove', 'First Object Above', 'Mod+Alt+Shift+]'),
    xe('sel.nextAbove', 'ai.selNextAbove', 'Next Object Above', 'Mod+Alt+]'),
    xe('sel.nextBelow', 'ai.selNextBelow', 'Next Object Below', 'Mod+Alt+['),
    xe('sel.lastBelow', 'ai.selLastBelow', 'Last Object Below', 'Mod+Alt+Shift+['),
    xe('sel.inverse', 'ops.inverse', 'Inverse'),
    xe('sel.sameFill', 'ops.sameFill', 'Fill Color', null, { sub: 'Same' }),
    xe('sel.sameStroke', 'ops.sameStroke', 'Stroke Color', null, { sub: 'Same' }),
    xe('sel.sameRole', 'ops.sameRole', 'Laser Role', null, { sub: 'Same' })
  ]);
  add('Effect', [
    xe('fx.free', 'cw-free', 'Free Distort…', null, { sub: 'Distort & Transform' }),
    xe('fx.persp', 'cw-persp', 'Perspective Distort…', null, { sub: 'Distort & Transform' }),
    xe('fx.unite', 'ops.unite', 'Unite', null, { sub: 'Pathfinder' }),
    xe('fx.minus', 'ops.minus', 'Minus Front', null, { sub: 'Pathfinder' }),
    xe('fx.intersect', 'ops.intersect', 'Intersect', null, { sub: 'Pathfinder' }),
    xe('fx.exclude', 'ops.exclude', 'Exclude', null, { sub: 'Pathfinder' }),
    xe('fx.divide', 'ops.divide', 'Divide', null, { sub: 'Pathfinder' }),
    xe('fx.trim', 'ops.trim', 'Trim', null, { sub: 'Pathfinder' }),
    xe('fx.round', 'ops.round', 'Round Corners…', null, { sub: 'Stylize' }),
    xe('fx.warp', 'cw-warp', 'Warp…', null, { sub: 'Warp' })
  ]);
  const optE = (id, label, key, opt, extra) => Object.assign({ id, label, key, chk: w => !!st(w).opts[opt], run: w => w.act('opt', opt) }, extra || {});
  add('View', [
    xe('view.outline', 'cx.outline', w => (st(w).outline ? 'Preview' : 'Outline'), 'Mod+Y'),
    xe('view.check', 'cx.check', 'Laser Check…'),
    { id: 'view.zoomIn', label: 'Zoom In', key: 'Mod++', run: w => w.act('zoom', 'in'), own: true },
    { id: 'view.zoomOut', label: 'Zoom Out', key: 'Mod+-', run: w => w.act('zoom', 'out'), own: true },
    { id: 'view.fit', label: 'Fit Artboard in Window', key: 'Mod+0', run: w => w.act('zoom', 'fit'), own: true },
    { id: 'view.fitAll', label: 'Fit All in Window', key: 'Mod+Alt+0', run: w => w.act('zoom', 'fit') },
    { id: 'view.actual', label: 'Actual Size', key: 'Mod+1', run: w => w.act('zoom', '100'), own: true },
    { id: 'view.zoomSel', label: 'Zoom to Selection', key: 'Shift+2', on: sel, run: w => w.act('zoom', 'sel'), own: true, sheet: 'the charm editor\'s own (Illustrator has no key for it)' },
    { id: 'view.scrNormal', label: 'Normal Screen Mode', sub: 'Screen Mode', chk: w => st(w).screen === 'normal', run: w => w.act('screen', 'normal') },
    { id: 'view.scrMenus', label: 'Full Screen Mode with Menu Bar', sub: 'Screen Mode', chk: w => st(w).screen === 'menus', run: w => w.act('screen', 'menus') },
    { id: 'view.scrFull', label: 'Full Screen Mode', sub: 'Screen Mode', chk: w => st(w).screen === 'full', run: w => w.act('screen', 'full') },
    { id: 'view.rulers', label: w => (st(w).rulers ? 'Hide Rulers' : 'Show Rulers'), key: 'Mod+R', sub: 'Rulers', run: act('rulers') },
    { id: 'view.zero', label: 'Reset Zero Point', sub: 'Rulers', run: act('zero0'), sheet: 'or double-click the corner where the rulers meet' },
    optE('view.smart', 'Smart Guides', 'Mod+U', 'smart', { own: true }),
    { id: 'view.guides', label: w => (st(w).opts.showGuides ? 'Hide Guides' : 'Show Guides'), key: 'Mod+;', sub: 'Guides', run: w => w.act('opt', 'showGuides'), own: true },
    optE('view.snapGuides', 'Snap to Guides', null, 'snapGuides', { sub: 'Guides' }),
    { id: 'view.clearGuides', label: 'Clear Guides', sub: 'Guides', run: act('clearGuides') },
    { id: 'view.grid', label: w => (st(w).opts.grid ? 'Hide Grid' : 'Show Grid'), key: "Mod+'", run: w => w.act('opt', 'grid') },
    optE('view.snapGrid', 'Snap to Grid', "Mod+Shift+'", 'snapGrid')
  ]);
  add('Window', [
    { id: 'win.tools', label: 'Toolbars', run: act('panelVis', 'tools'), chk: w => !st(w).hidden.tools, menuLabel: 'Tools' },
    { id: 'win.control', label: 'Control', run: act('panelVis', 'control'), chk: w => !st(w).hidden.control },
    { id: 'win.align', label: 'Align', key: 'Shift+F7', when: w => !!aiDock() || w.panelOr('align', 'ops.alignBar'), run: w => { const d = aiDock(); if (d) d.openPanel('align'); else w.openPanel('align', 'ops.alignBar'); } },
    { id: 'win.layers', label: 'Layers', key: 'F7', run: w => { const d = aiDock(); if (d) d.openPanel('layers'); else w.act('dock'); }, chk: w => st(w).dock },
    { id: 'win.libraries', label: 'Libraries', when: w => st(w).panels.some(p => p.id === 'xlib'), run: w => w.act('xpanel', 'xlib'), chk: w => st(w).xpanel === 'xlib' },
    { id: 'win.pathfinder', label: 'Pathfinder', key: 'Mod+Shift+F9', when: w => !!aiDock() || w.panelOr('pathfinder', 'ops.pfBar'), run: w => { const d = aiDock(); if (d) d.openPanel('pathfinder'); else w.openPanel('pathfinder', 'ops.pfBar'); } },
    // (the docked panels of charm-nest-ce-ai-panels.js, for the keys and the shortcuts sheet: the Window menu lists them from CharmEditor.aiDock itself)
    ...[['properties', 'Properties', ''], ['transform', 'Transform', 'Shift+F8'], ['stroke', 'Stroke', 'Mod+F10'], ['swatches', 'Swatches', ''], ['character', 'Character', 'Mod+T'], ['trace', 'Image Trace', '']].map(([id, label, key]) =>
      ({ id: 'win.' + id, label, key: key || undefined, menu: false, dock: id, when: () => !!(aiDock() && (aiDock().panels || []).some(p => p.id === id)), run: () => { const d = aiDock(); if (d) d.openPanel(id); } })),
    { id: 'win.panels', label: 'Hide Panels', key: 'Tab', run: act('panels', 'all'), menu: false, sheet: 'with the work area in focus' },
    { id: 'win.panelsSide', label: 'Hide Panels Except the Toolbar', key: 'Shift+Tab', run: act('panels', 'side'), menu: false }
  ]);
  add('Help', [
    { id: 'help.help', label: 'Charm Editor Help…', key: 'F1', run: w => helpSheet(w) },
    { id: 'help.shortcuts', label: 'Keyboard Shortcuts…', run: () => api.openShortcuts() }
  ]);
  // the Tools panel's own keys and the Fill / Stroke and screen keys (Illustrator's)
  add('Tools', [
    { id: 'tool.rect', label: 'Rectangle Tool', key: 'M', run: w => w.act('tool', 'rect'), menu: false },
    { id: 'tool.ellipse', label: 'Ellipse Tool', key: 'L', run: w => w.act('tool', 'ellipse'), menu: false },
    { id: 'tool.marquee', label: 'Marquee Selection Tool', key: 'Q', run: w => w.act('tool', 'marquee'), menu: false },
    { id: 'tool.free', label: 'Free Transform Tool (Free Distort)', key: 'E', run: w => (w.x.hasTool('cw-free') ? w.act('tool', 'cw-free') : w.act('say', 'Free Transform: drag the selection\'s handles (Ctrl or Alt: from the centre)')), menu: false },
    { id: 'tool.rotate', label: 'Rotate', key: 'R', run: w => w.act('say', 'Rotate: drag just outside a corner of the selection (Shift: 15° steps), or Object ▸ Transform ▸ Rotate…'), menu: false, sheet: 'no Rotate tool: drag outside a corner, or Object ▸ Transform ▸ Rotate…' },
    { id: 'tool.reflect', label: 'Reflect', key: 'O', run: w => w.act('say', 'Reflect: Object ▸ Transform ▸ Reflect… or Flip Horizontal / Vertical'), menu: false, sheet: 'no Reflect tool: Object ▸ Transform ▸ Reflect…' },
    { id: 'tool.scale', label: 'Scale', key: 'S', run: w => w.act('say', 'Scale: drag a handle of the selection (Shift keeps the proportions), or Object ▸ Transform ▸ Scale…'), menu: false, sheet: 'no Scale tool: drag a handle, or Object ▸ Transform ▸ Scale…' },
    { id: 'paint.toggle', label: 'Toggle Fill and Stroke', key: 'X', run: act('paintFocus'), menu: false },
    { id: 'paint.swap', label: 'Swap Fill and Stroke', key: 'Shift+X', on: sel, run: act('swapPaint'), menu: false, sheet: 'a line engraving becomes a hatch fill and back' },
    { id: 'paint.default', label: 'Default Fill and Stroke', key: 'D', run: act('defaultPaint'), menu: false, sheet: 'words: hatch fill; shapes: line engraving' },
    { id: 'screen.cycle', label: 'Change Screen Mode', key: 'F', run: act('screen', 'cycle'), menu: false },
    { id: 'menu.focus', label: 'Menu Bar', key: 'F10', run: w => { if (live) live.focusBar(); }, menu: false, sheet: MAC ? 'or Ctrl+F2' : 'or press and let go of Alt' }
  ]);
  const byId = new Map(C.map(e => [e.id, e]));

  /** The Tools panel: Illustrator's names, letters and a line on what each does (the hover cards and the shortcuts sheet). */
  const TOOLS = {
    select: { name: 'Selection Tool', key: 'V', desc: 'Select, move, scale and rotate objects' },
    direct: { name: 'Direct Selection Tool', key: 'A', desc: 'Select and move anchor points and path segments' },
    marquee: { name: 'Marquee Selection Tool', key: 'Q', desc: 'Drag a box: everything it touches is selected' },
    pen: { name: 'Pen Tool', key: 'P', desc: 'Draw straight and curved paths' },
    anchorAdd: { name: 'Add Anchor Point Tool', key: '+', desc: 'Click a path to add an anchor point' },
    anchorDel: { name: 'Delete Anchor Point Tool', key: '-', desc: 'Click an anchor point to remove it' },
    anchorConvert: { name: 'Anchor Point Tool', key: 'Shift+C', desc: 'Convert corner and smooth points' },
    text: { name: 'Type Tool', key: 'T', desc: 'Click to place words; click words to edit them' },
    line: { name: 'Line Segment Tool', key: '\\', desc: 'Draw straight lines (Shift: 45°)' },
    rect: { name: 'Rectangle Tool', key: 'M', desc: 'Draw rectangles (Shift: square)' },
    'shape-rrect': { name: 'Rounded Rectangle Tool', desc: 'Draw rectangles with rounded corners' },
    ellipse: { name: 'Ellipse Tool', key: 'L', desc: 'Draw ellipses (Shift: circle)' },
    polygon: { name: 'Polygon Tool', desc: 'Draw polygons (↑ ↓ while dragging: sides)' },
    'shape-star': { name: 'Star Tool', desc: 'Draw stars' },
    'shape-heart': { name: 'Heart Tool', desc: 'Draw a heart blank' },
    'shape-ring': { name: 'Ring Tool', desc: 'Draw a ring blank' },
    'shape-bail': { name: 'Jump Ring Tool', desc: 'Add a jump ring' },
    pencil: { name: 'Pencil Tool', key: 'N', desc: 'Draw freeform paths' },
    scissors: { name: 'Scissors Tool', key: 'C', desc: 'Cut a path at a point' },
    'cw-free': { name: 'Free Distort Tool', key: 'E', desc: 'Drag the corners to distort the selection' },
    'cw-persp': { name: 'Perspective Distort Tool', desc: 'Drag a corner to distort in perspective' },
    'cw-blend': { name: 'Blend Tool', key: 'W', desc: 'Click two objects to blend between them' },
    hand: { name: 'Hand Tool', key: 'H', desc: 'Move around the artboard (or hold Space)' },
    zoom: { name: 'Zoom Tool', key: 'Z', desc: 'Click to zoom in, Alt-click out, drag a box' },
    more: { name: 'Edit Toolbar', key: '', desc: 'Grid, guides, snapping and zoom' }
  };
  const windowItems = [];
  /** The dock of charm-nest-ce-ai-panels.js (CharmEditor.aiDock) when the page has it, and Illustrator's keys of its panels (the Window menu's column). */
  const aiDock = () => { const CE = root && root.CharmEditor; return CE && CE.aiDock && typeof CE.aiDock.openPanel === 'function' ? CE.aiDock : null; };
  const DOCK_KEYS = { layers: 'F7', transform: 'Shift+F8', align: 'Shift+F7', pathfinder: 'Mod+Shift+F9', stroke: 'Mod+F10', character: 'Mod+T', color: 'F6', swatches: '', properties: '', trace: '' };

  /* ═══ 4 · the menus: Illustrator's order, groupings and submenus, built from the catalog each time one opens ═══ */
  const S = s => ({ sub: s });
  const MENUS = [
    ['File', ['file.open', '-', 'file.close', 'file.back', 'file.save', '-', 'file.sendToSheet', '-', 'file.place']],
    ['Edit', ['edit.undo', 'edit.redo', '-', 'edit.cut', 'edit.copy', 'edit.paste', 'edit.pasteFront', 'edit.pasteBack', 'edit.pasteInPlace', 'edit.clear', 'edit.duplicate', '-', 'edit.shortcuts', S('Preferences')]],
    ['Object', [S('Transform'), S('Arrange'), S('Align'), '-', 'obj.group', 'obj.ungroup', S('Lock'), 'obj.unlockAll', S('Hide'), 'obj.showAll', '-', S('Path'), S('Shape'), S('Blend'), S('Envelope Distort'), S('Image Trace'), '-', S('Clipping Mask'), S('Compound Path'), '-', { more: true }]],
    ['Type', [S('Font'), S('Size'), S('Font Style'), '-', 'type.glyphs', '-', S('Type on a Path'), '-', S('Change Case'), S('Paragraph'), '-', 'type.edit']],
    ['Select', ['sel.all', 'sel.allBoard', 'sel.none', '-', 'sel.firstAbove', 'sel.nextAbove', 'sel.nextBelow', 'sel.lastBelow', '-', 'sel.inverse', '-', S('Same')]],
    ['Effect', [S('Distort & Transform'), S('Pathfinder'), S('Stylize'), S('Warp')]],
    ['View', ['view.outline', '-', 'view.check', '-', 'view.zoomIn', 'view.zoomOut', 'view.fit', 'view.fitAll', 'view.actual', 'view.zoomSel', '-', S('Screen Mode'), '-', S('Rulers'), '-', 'view.smart', S('Guides'), '-', 'view.grid', 'view.snapGrid']],
    ['Window', [{ window: true }]],
    ['Help', ['help.help', '-', 'help.shortcuts']]
  ];
  const SUB_ORDER = {};   // (a submenu lists its catalog entries in catalog order)
  for (const e of C) if (e.sub) (SUB_ORDER[e.path + '/' + e.sub] || (SUB_ORDER[e.path + '/' + e.sub] = [])).push(e.id);
  const SUB_SEPS = { 'Object/Transform': ['obj.move', 'obj.flipH', 'obj.repeat'], 'Object/Path': ['obj.outline', 'obj.simplify', 'obj.anchCorner'], 'Object/Blend': ['obj.blendOptions', 'obj.blendExpand'], 'Object/Envelope Distort': ['obj.warpExpand'] };

  /** One entry as the menu shows it now: null when the editor cannot do it (not shown). */
  function resolve(w, e, arg) {
    if (!e) return null;
    if (e.when && !safe(() => e.when(w))) return null;
    if (e.x && !w.x.has(e.x)) return null;
    const on = e.x ? w.x.enabled(e.x) && (!e.on || safe(() => e.on(w))) : (!e.on || safe(() => e.on(w)));
    const label = typeof e.label === 'function' ? safe(() => e.label(w)) : (e.menuLabel || e.label);
    return { e, id: e.id, label, key: e.keyText || (e.key ? keyLabel(e.key) : ''), on: !!on, chk: e.chk ? !!safe(() => e.chk(w)) : null, arg };
  }
  function safe(f) { try { return f(); } catch (er) { try { root.console && root.console.error('CharmEditorAi:', er); } catch (e2) { /* fine */ } return false; } }
  function tidy(list) {
    const out = [];
    for (const it of list) { if (!it) continue; if (it.sep && (!out.length || out[out.length - 1].sep)) continue; out.push(it); }
    while (out.length && out[out.length - 1].sep) out.pop();
    return out;
  }
  function subItems(w, path, name) {
    const key = path + '/' + name, seps = SUB_SEPS[key] || [];
    const dynE = C.find(e => e.path === path && e.sub === name && e.dyn);
    if (dynE) return dynItems(w, dynE);
    const list = [];
    for (const id of SUB_ORDER[key] || []) { if (seps.includes(id)) list.push({ sep: true }); list.push(resolve(w, byId.get(id))); }
    return tidy(list);
  }
  function dynItems(w, e) {
    const s = st(w), on = textish(w);
    if (e.dyn === 'fonts') return (s.fonts || []).map(f => ({ e: { id: 'type.font' }, id: 'type.font', arg: String(f.key), label: f.name, family: f.family, on, chk: s.font != null && String(s.font) === String(f.key) }));
    if (e.dyn === 'sizes') return SIZES.map(z => ({ e: { id: 'type.size' }, id: 'type.size', arg: String(z), label: z + ' pt', on, chk: s.size != null && Math.abs(s.size - z) < 1e-6 }));
    if (e.dyn === 'cases') return CASES.map(([c, l]) => ({ e: { id: 'type.caseSet' }, id: 'type.caseSet', arg: c, label: l, on, chk: s.letterCase === c }));
    return [];
  }
  /** The top menu's list: entries, separators and submenus (a submenu with nothing in it is not shown). */
  function menuItems(w, path, spec) {
    const list = [];
    for (const it of spec) {
      if (it === '-') { list.push({ sep: true }); continue; }
      if (typeof it === 'string') { list.push(resolve(w, byId.get(it))); continue; }
      if (it.sub) { const kids = subItems(w, path, it.sub); if (kids.length) list.push({ label: it.sub, kids, on: kids.some(k => !k.sep && k.on) }); continue; }
      if (it.more) { for (const g of moreGroups(w)) list.push(g); continue; }
      if (it.window) { for (const x of windowList(w)) list.push(x); continue; }
    }
    return tidy(list);
  }
  /** Extension commands no catalog entry names (a file added later): under Object, a submenu per group, so nothing an extension offers is out of reach. */
  function moreGroups(w) {
    const known = new Set(C.filter(e => e.x).map(e => e.x).concat(['ops.pfBar', 'toolAnchorConvert', 'toolAnchorAdd', 'ops.unlockAll', 'ops.showAll']));   // (the last two: Object ▸ Unlock All / Show All already)
    const groups = new Map();
    for (const c of w.x.list()) {
      if (known.has(c.id) || c.menu === false) continue;
      const g = c.group || 'More'; if (!groups.has(g)) groups.set(g, []);
      groups.get(g).push({ e: { id: c.id, x: c.id }, id: c.id, xid: c.id, label: c.title || c.id, key: c.key ? keyLabel(c.key) : '', on: w.x.enabled(c.id), chk: null });
    }
    return [...groups].map(([g, kids]) => ({ label: g, kids, on: kids.some(k => k.on) }));
  }
  function windowList(w) {
    const s = st(w), out = [resolve(w, byId.get('win.tools')), resolve(w, byId.get('win.control')), { sep: true }];
    const rows = [], dock = aiDock();
    if (dock) {
      // (the docked panels of charm-nest-ce-ai-panels.js, Illustrator's Window menu: each brought into view by its own call, ✓ while it shows)
      let stt = null; try { stt = dock.current && typeof dock.current.state === 'function' ? dock.current.state() : null; } catch (e) { stt = null; }
      const shows = id => !!stt && ((stt.fly === id) || (stt.mode !== 'collapsed' && stt.mode !== 'narrow' && Array.isArray(stt.layout && stt.layout.main) && stt.layout.main.some(g => g.active === id && !g.min))) && (id !== 'layers' || st(w).dock);
      for (const p of dock.panels || []) rows.push({ e: { id: 'win.dock' }, id: 'win.dock', arg: String(p.id), label: String(p.title || p.id), key: DOCK_KEYS[p.id] ? keyLabel(DOCK_KEYS[p.id]) : p.key ? (/[⌘⇧⌥]/.test(p.key) ? p.key : keyLabel(p.key)) : '', on: true, chk: shows(String(p.id)) });
      const lib = resolve(w, byId.get('win.libraries')); if (lib) rows.push(lib);
    } else for (const id of ['win.align', 'win.layers', 'win.libraries', 'win.pathfinder']) { const r = resolve(w, byId.get(id)); if (r) rows.push(r); }
    for (const it of windowItems) {
      if (!it || !it.label) continue;
      if (it.when && !safe(() => it.when(w))) continue;
      const r = { e: { id: 'win.x.' + it.id }, id: 'win.x.' + it.id, label: it.label, key: it.key ? keyLabel(it.key) : '', on: true, chk: it.chk ? !!safe(() => it.chk(w)) : null, wi: it };
      const at = rows.findIndex(x => x.label === it.label); if (at >= 0) rows[at] = r; else rows.push(r);
    }
    for (const p of s.panels) { if (p.id === 'xlib' || rows.some(r => r.label === p.title)) continue; rows.push({ e: { id: 'win.panel' }, id: 'win.panel', arg: p.id, label: p.title, on: true, chk: s.xpanel === p.id }); }
    rows.sort((a, b) => String(a.label).localeCompare(String(b.label)));
    return out.concat(rows);
  }

  /** A list of items as menu markup. Entries the editor runs itself carry its own hooks (data-ctx: a right-click action, data-xcmd: an extension command); the
   *  rest carry data-ai and are run here. Every row: the check column, the name, the shortcut column (and ▸ on a submenu). */
  function itemsHtml(list, depth) {
    let h = '';
    list.forEach((it, i) => {
      if (it.sep) { h += '<div class="ce-menu__sep ce-aisep" role="separator"></div>'; return; }
      const off = it.on ? '' : ' is-off', dis = it.on ? '' : ' aria-disabled="true"';
      if (it.kids) {
        h += `<div class="ce-aisub" role="none"><button class="ce-item ce-aimi ce-aimi--sub${off}" type="button" role="menuitem" aria-haspopup="true" aria-expanded="false" tabindex="-1" data-ai-sub="${depth}-${i}"${dis}><span class="ce-aimi__l">${esc(it.label)}</span></button>`
          + `<div class="ce-menu ce-aimenu ce-aimenu--sub" role="menu" aria-label="${esc(it.label)}" hidden>${itemsHtml(it.kids, depth + 1)}</div></div>`;
        return;
      }
      const e = it.e || {}, hook = e.ctx ? ` data-ctx="${esc(e.ctx)}"` : (e.x || it.xid) ? ` data-xcmd="${esc(e.x || it.xid)}"` : ` data-ai="${esc(it.id)}"${it.arg != null ? ` data-ai-arg="${esc(it.arg)}"` : ''}`;
      const role = it.chk == null ? 'menuitem' : 'menuitemcheckbox', chk = it.chk ? ' is-chk' : '';
      const style = it.family ? ` style="font-family:${esc(it.family)}"` : '';
      h += `<button class="ce-item ce-aimi${off}${chk}" type="button" role="${role}"${it.chk == null ? '' : ` aria-checked="${it.chk}"`} tabindex="-1"${hook}${dis}><span class="ce-aimi__l"${style}>${esc(it.label)}</span>${it.key ? `<em>${esc(it.key)}</em>` : ''}</button>`;
    });
    return h;
  }

  /* ═══ 6 · the workspace: the menu bar, menus open, the keys ═══ */
  let live = null;
  function mount(w) {
    const rootEl = w.root, win = w.win, bar = rootEl.querySelector('.ce-menubar');
    const M = { w, open: null, kb: false, altAlone: false };
    live = M;
    /* the menu bar: one title per menu, a ☰ that holds them all on a narrow screen */
    if (bar) {
      bar.innerHTML = `<button class="ce-mb__all ce-drop__b" type="button" role="menuitem" aria-haspopup="true" aria-expanded="false" data-mb="all" title="Menus" aria-label="Menus">`
        + `<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M2.5 4h11M2.5 8h11M2.5 12h11" stroke="currentColor" stroke-width="1.3" fill="none"/></svg></button>`
        + MENUS.map(([name], i) => `<button class="ce-mb__t ce-drop__b" type="button" role="menuitem" aria-haspopup="true" aria-expanded="false" data-mb="${i}" tabindex="${i ? -1 : 0}">${esc(name)}</button>`).join('');
      bar.addEventListener('click', ev => { const t = ev.target.closest('[data-mb]'); if (!t) return; ev.stopPropagation(); const i = t.dataset.mb; if (M.open && M.open.key === i) closeAll(); else openTop(i, false); });
      bar.addEventListener('pointerover', ev => { const t = ev.target.closest('[data-mb]'); if (!t || !M.open || M.open.key === t.dataset.mb || M.open.key === 'ctx') return; openTop(t.dataset.mb, M.kb); });
    }
    rootEl.addEventListener('pointerdown', () => { M.altAlone = false; }, true);   // (Alt held for a click or a drag is not Alt for the menu bar)
    /* a row picked: a catalog command run here (the editor's own hooks, data-ctx and data-xcmd, it runs itself) */
    rootEl.addEventListener('click', ev => {
      const sb = ev.target.closest('[data-ai-sub]');
      if (sb && sb.closest('.ce-aimenu, .ce-ctx')) { ev.stopPropagation(); if (!sb.classList.contains('is-off')) openSub(sb, false); return; }
      const b = ev.target.closest('[data-ai]'); if (!b || !b.closest('.ce-aimenu, .ce-ctx, .ce-aidlg')) return;
      ev.stopPropagation();
      if (b.classList.contains('is-off')) return;
      const id = b.dataset.ai, arg = b.dataset.aiArg;
      closeAll();
      run(id, arg);
    });
    /* a row under the pointer: as Illustrator's menus, a submenu opens after a moment (not as the pointer only passes over its row) and an open submenu stays open while the
       pointer heads for it across the rows on the way (the triangle from where the pointer was to the submenu's near edge: "menu aim") */
    let aimT = 0, lastPt = null, prevPt = null;
    let aimPending = null;   // (a row's change waiting while the pointer still heads for the open submenu: put off again at every move that still heads there; done once it stops)
    rootEl.addEventListener('pointermove', ev => {
      if (!M.open) return;
      if (!lastPt || Math.hypot(ev.clientX - lastPt.x, ev.clientY - lastPt.y) > 1.5) {
        prevPt = lastPt; lastPt = { x: ev.clientX, y: ev.clientY };
        if (aimPending && aimMenu && headingFor(aimMenu)) { clearTimeout(aimT); aimT = setTimeout(aimPending, 320); }
      }
    }, true);
    const side = (p, a, b) => (p.x - b.x) * (a.y - b.y) - (a.x - b.x) * (p.y - b.y);
    const inTri = (p, a, b, c) => { const d1 = side(p, a, b), d2 = side(p, b, c), d3 = side(p, c, a); return !((d1 < 0 || d2 < 0 || d3 < 0) && (d1 > 0 || d2 > 0 || d3 > 0)); };
    function headingFor(menu) {
      const open = [...menu.querySelectorAll(':scope > .ce-aisub > .ce-menu')].find(x => !x.hidden);
      if (!open || !prevPt || !lastPt) return false;
      const r = open.getBoundingClientRect(), x = r.left >= lastPt.x - 2 ? r.left : r.right;
      return inTri(lastPt, prevPt, { x, y: r.top - 6 }, { x, y: r.bottom + 6 });
    }
    let aimMenu = null;
    rootEl.addEventListener('pointerover', ev => {
      if (!M.open) return;
      // (the pointer arrived in a submenu, on a row or between rows: what its parent menu was about to do is called off)
      const inMenu = ev.target.closest && ev.target.closest('.ce-menu');
      if (inMenu && aimMenu && inMenu !== aimMenu && aimMenu.contains(inMenu)) { clearTimeout(aimT); aimMenu = null; aimPending = null; }
      const it = ev.target.closest('.ce-aimi'); if (!it || !it.closest('.ce-aimenu, .ce-ctx')) return;
      const menu = it.closest('.ce-menu');
      if (M.kb) try { it.focus({ preventScroll: true }); } catch (e) { /* fine */ }
      clearTimeout(aimT);
      // (the rows of this menu: a deeper submenu of another row closes; a submenu row opens its own)
      const act = () => {
        if (!M.open || !menu.isConnected || menu.hidden) return;
        for (const s of menu.querySelectorAll(':scope > .ce-aisub > .ce-aimi--sub')) if (s !== it) closeSub(s);
        if (it.classList.contains('ce-aimi--sub') && !it.classList.contains('is-off')) openSub(it, false);
      };
      if (it.parentNode.querySelector(':scope > .ce-menu:not([hidden])')) return;   // (its own submenu is open already)
      aimMenu = menu;
      aimPending = null;
      if (headingFor(menu)) { aimPending = () => { aimPending = null; if (it.matches(':hover')) act(); }; aimT = setTimeout(aimPending, 320); return; }
      aimT = setTimeout(act, it.classList.contains('ce-aimi--sub') ? 160 : 110);
    });
    function topEl() { return M.open && M.open.el; }
    function openTop(key, kb) {
      closeAll(true);
      M.kb = !!kb;
      const narrow = key === 'all';
      const spec = narrow ? null : MENUS[+key];
      const list = narrow ? MENUS.map(([name, sp]) => { const kids = menuItems(w, name, sp); return kids.length ? { label: name, kids, on: true } : null; }).filter(Boolean) : menuItems(w, spec[0], spec[1]);
      const el = doc.createElement('div'); el.className = 'ce-menu ce-aimenu'; el.setAttribute('role', 'menu'); el.setAttribute('aria-label', narrow ? 'Menus' : spec[0]);
      el.innerHTML = itemsHtml(list, 0) || '<div class="ce-menu__note">Nothing here yet.</div>';
      win.appendChild(el);
      const t = bar && bar.querySelector(`[data-mb="${key}"]`);
      if (t) { t.setAttribute('aria-expanded', 'true'); t.classList.add('is-open'); }
      place(el, t ? t.getBoundingClientRect() : { left: 8, right: 8, top: 30, bottom: 30 }, 'down');
      M.open = { key, el, title: t };
      if (kb) focusFirst(el);
    }
    function closeSub(btn) {
      const m = btn.parentNode.querySelector(':scope > .ce-menu'); if (!m || m.hidden) return;
      for (const s of m.querySelectorAll('.ce-aimi--sub')) closeSub(s);
      m.hidden = true; btn.setAttribute('aria-expanded', 'false'); btn.classList.remove('is-open');
    }
    function openSub(btn, kb) {
      const m = btn.parentNode.querySelector(':scope > .ce-menu'); if (!m) return null;
      if (m.hidden) { m.hidden = false; btn.setAttribute('aria-expanded', 'true'); btn.classList.add('is-open'); place(m, btn.getBoundingClientRect(), 'right'); }
      if (kb) focusFirst(m);
      return m;
    }
    function closeAll(quiet) {
      const o = M.open; M.open = null;
      if (o) {
        if (o.key === 'ctx') { o.el.hidden = true; o.el.innerHTML = ''; } else if (o.el && o.el.parentNode) o.el.parentNode.removeChild(o.el);
        if (o.title) { o.title.setAttribute('aria-expanded', 'false'); o.title.classList.remove('is-open'); }
      }
      if (!quiet && M.kb) { M.kb = false; try { win.focus({ preventScroll: true }); } catch (e) { /* fine */ } }
    }
    /** The editor hid its menus (a press elsewhere, Esc): ours go with them. */
    function closed() {
      if (!M.open) return;
      const o = M.open; M.open = null;
      if (o.key !== 'ctx' && o.el && o.el.parentNode) o.el.parentNode.removeChild(o.el);
      if (o.title) { o.title.setAttribute('aria-expanded', 'false'); o.title.classList.remove('is-open'); }
      M.kb = false;
    }
    /** Inside the window: below a title, beside a row (flipped to the other side when there is no room), at the pointer; never past an edge. */
    function place(menu, r, side) {
      const vw = doc.documentElement.clientWidth || root.innerWidth, vh = doc.documentElement.clientHeight || root.innerHeight;
      menu.hidden = false; menu.style.visibility = 'hidden'; menu.style.left = '0px'; menu.style.top = '0px'; menu.style.maxHeight = (vh - 12) + 'px';
      const mr = menu.getBoundingClientRect();
      let left = side === 'right' ? r.right - 2 : r.left, top = side === 'right' ? r.top - 4 : side === 'down' ? r.bottom : r.top;
      if (left + mr.width > vw - 4) left = side === 'right' ? Math.max(4, r.left - mr.width + 2) : Math.max(4, vw - 4 - mr.width);
      if (top + mr.height > vh - 4) top = side === 'point' && r.top - mr.height >= 4 ? r.top - mr.height : Math.max(4, vh - 4 - mr.height);
      menu.style.left = Math.max(4, left) + 'px'; menu.style.top = top + 'px'; menu.style.visibility = '';
    }
    const rows = m => [...m.querySelectorAll(':scope > .ce-aimi, :scope > .ce-aisub > .ce-aimi, :scope > .ce-item')].filter(b => !b.classList.contains('is-off') && b.getClientRects().length);
    function focusFirst(m) { const r = rows(m)[0] || m; if (r === m) m.setAttribute('tabindex', '-1'); try { r.focus({ preventScroll: true }); } catch (e) { /* fine */ } }   // (a menu with nothing to choose has the keys itself: ← → still go on)
    /** The open menu the keys go to: the deepest one with focus in it, else the deepest one open. */
    function current() {
      const a = doc.activeElement, inMenu = a && a.closest && a.closest('.ce-menu');
      if (inMenu && (inMenu === topEl() || (topEl() && topEl().contains(inMenu)))) return inMenu;
      let m = topEl(); if (!m) return null;
      for (;;) { const s = [...m.querySelectorAll(':scope > .ce-aisub > .ce-menu')].find(x => !x.hidden); if (!s) return m; m = s; }
    }
    /* the keys while a menu is open: ↑ ↓ along it, → into a submenu or on to the next menu, ← back out or to the one before, Enter or Space to choose, Esc one level back */
    function menuKey(e) {
      const k = e.key;
      if (!M.open) {
        if (k === 'F10' || (MAC && e.ctrlKey && k === 'F2')) { e.preventDefault(); focusBar(); return true; }
        if (k === 'Alt' && !MAC) M.altAlone = !e.repeat; else M.altAlone = false;
        return false;
      }
      const m = current(); if (!m) return false;
      const list = rows(m), a = doc.activeElement, i = list.indexOf(a);
      const top = MENUS.length, cur = M.open.key;
      const go = d => { if (!list.length) return; const n = list[(i < 0 ? (d > 0 ? -1 : 0) : i) + d < 0 ? list.length - 1 : ((i < 0 ? (d > 0 ? -1 : 0) : i) + d) % list.length]; M.kb = true; try { n.focus({ preventScroll: true }); } catch (er) { /* fine */ } };
      if (k === 'ArrowDown') { e.preventDefault(); go(1); return true; }
      if (k === 'ArrowUp') { e.preventDefault(); go(-1); return true; }
      if (k === 'Home' || k === 'End') { e.preventDefault(); const n = k === 'Home' ? list[0] : list[list.length - 1]; if (n) n.focus({ preventScroll: true }); return true; }
      if (k === 'ArrowRight') {
        e.preventDefault();
        if (a && a.classList && a.classList.contains('ce-aimi--sub') && list.includes(a)) { M.kb = true; openSub(a, true); return true; }
        if (cur !== 'ctx' && cur !== 'all') openTop(String((+cur + 1) % top), true);
        return true;
      }
      if (k === 'ArrowLeft') {
        e.preventDefault();
        if (m !== topEl()) { const btn = m.parentNode.querySelector(':scope > .ce-aimi--sub'); closeSub(btn); try { btn.focus({ preventScroll: true }); } catch (er) { /* fine */ } return true; }
        if (cur !== 'ctx' && cur !== 'all') openTop(String((+cur + top - 1) % top), true);
        return true;
      }
      if (k === 'Enter' || k === ' ') { e.preventDefault(); if (a && list.includes(a)) a.click(); return true; }
      if (k === 'Escape') {
        e.preventDefault();
        if (m !== topEl()) { const btn = m.parentNode.querySelector(':scope > .ce-aimi--sub'); closeSub(btn); try { btn.focus({ preventScroll: true }); } catch (er) { /* fine */ } return true; }
        const t = M.open.title, kb = M.kb; closeAll(true); w.closeMenus();
        if (kb && t) try { t.focus({ preventScroll: true }); } catch (er) { /* fine */ } else try { win.focus({ preventScroll: true }); } catch (er) { /* fine */ }
        return true;
      }
      if (k === 'Tab') { e.preventDefault(); return true; }
      return false;
    }
    function focusBar() { if (!bar) return; const all = bar.querySelector('[data-mb="all"]'); openTop(all && all.offsetParent && getComputedStyle(all).display !== 'none' ? 'all' : '0', true); }
    function tip(id) {
      const t = TOOLS[id] || null;
      return t ? [t.name, t.desc || '', t.key ? keyLabel(t.key) : ''] : null;
    }
    function sync() { /* the menus are built as they open: nothing stays stale */ }
    /** A lone Alt let go (Windows): the menu bar takes the keys, as Illustrator's does. */
    function keyUp(e) {
      if (e.key === 'Alt' && M.altAlone && !MAC && !M.open) { M.altAlone = false; e.preventDefault(); focusBar(); return true; }
      return false;
    }
    /** A menu of another file shown with the menu bar's look and keys (the right-click menu of charm-nest-ce-ai-keys.js): items from resolve() / menuHtml(), opened at a point. */
    function openAt(el, cx, cy, list) {
      closeAll(true); w.closeMenus();
      el.classList.add('ce-aimenu'); if (list) el.innerHTML = itemsHtml(list, 0);
      place(el, { left: cx, right: cx, top: cy, bottom: cy }, 'point');
      M.open = { key: 'ctx', el, title: null }; M.kb = false;
      return true;
    }
    M.api = { menuKey, keyUp, closed, tip, sync, focusBar, openAt, closeAll: () => closeAll(true), isOpen: () => !!M.open };
    M.focusBar = focusBar;
    return M.api;
  }
  /** A catalog command on the open workspace (the menus, the keys, a docked panel's own menu). */
  function run(id, arg) {
    const w = live && live.w; if (!w) return false;
    if (id === 'type.font') { w.act('font', arg); return true; }
    if (id === 'type.size') { w.act('size', +arg); return true; }
    if (id === 'type.caseSet') { w.act('style', { letterCase: arg || undefined }); return true; }
    if (id === 'win.panel') { w.act('xpanel', arg); return true; }
    if (id === 'win.dock') { const d = aiDock(); if (d) safe(() => d.openPanel(String(arg))); else if (arg === 'layers') w.act('dock'); return true; }
    if (/^win\.x\./.test(id)) { const it = windowItems.find(x => 'win.x.' + x.id === id); if (it && typeof it.run === 'function') safe(() => it.run(w)); return true; }
    const e = byId.get(id); if (!e) { if (w.x.has(id)) { w.x.run(id); return true; } return false; }
    if (e.x) { w.x.run(e.x); return true; }
    if (e.ctx) { w.act('ctx', e.ctx); return true; }
    if (typeof e.run === 'function') { safe(() => e.run(w, arg)); return true; }
    return false;
  }

  /* ═══ 7 · dialogs: Edit ▸ Keyboard Shortcuts…, Edit ▸ Preferences, Help ═══ */
  function dialog(w, title, body, o) {
    o = o || {};
    const box = doc.createElement('div'); box.className = 'ce-aidlg';
    box.innerHTML = `<div class="ce-aidlg__card${o.wide ? ' is-wide' : ''}" role="dialog" aria-modal="true" aria-label="${esc(title)}"><div class="ce-aidlg__title">${esc(title)}</div>`
      + `<div class="ce-aidlg__body">${body}</div><div class="ce-aidlg__foot">${o.foot || ''}<span class="ce-aidlg__gap"></span>`
      + (o.cancel ? '<button class="ce-aibtn" type="button" data-dlg="cancel">Cancel</button>' : '') + '<button class="ce-aibtn ce-aibtn--cta" type="button" data-dlg="ok">OK</button></div></div>';
    const close = w.modal(box, ok => { if (o.onClose) o.onClose(!!ok); });
    box.addEventListener('click', ev => { const b = ev.target.closest('[data-dlg]'); if (b) close(b.dataset.dlg === 'ok'); else if (ev.target === box) close(false); });
    requestAnimationFrame(() => { const f = box.querySelector(o.focus || '[data-dlg="ok"]'); if (f) try { f.focus({ preventScroll: true }); } catch (e) { /* fine */ } });
    return { box, close };
  }
  /** Edit ▸ Keyboard Shortcuts… (⌥⇧⌘K): every command the editor has, its key on a Mac and on Windows, read-only, by menu, with a search. */
  function sheet(w, startMac) {
    let mac = startMac == null ? (prefs().keys ? prefs().keys === 'mac' : MAC) : !!startMac;
    const rowsHtml = q => {
      q = String(q || '').trim().toLowerCase();
      const groups = new Map(), push = (g, name, k, note) => { if (q && !(name + ' ' + g + ' ' + (k || '') + ' ' + (note || '')).toLowerCase().includes(q)) return; if (!groups.has(g)) groups.set(g, []); groups.get(g).push([name, k, note]); };
      for (const [id, t] of Object.entries(TOOLS)) if (t.key && (w.x.hasTool(id) || ['select', 'marquee', 'text', 'line', 'rect', 'ellipse', 'hand', 'zoom'].includes(id))) push('Tools', t.name, keyLabel(t.key, mac));
      for (const e of C) {
        const k = e.key || e.sheetKey; if (!k) continue;
        if (e.x && !w.x.has(e.x)) continue;
        if (e.when && !safe(() => e.when(w))) continue;
        const lab = typeof e.label === 'function' ? safe(() => e.label(w)) : e.label;
        const k2 = e.keyText ? (mac ? e.keyText : e.keyText.replace('⌫', 'Del').replace('↩', 'Enter')) : keyLabel(k, mac);
        push(e.path === 'Tools' ? 'Tools' : e.path, (e.sub ? e.sub + ' ▸ ' : '') + lab, k2, e.sheet);
      }
      for (const c of w.x.list()) if (c.key && !C.some(e => e.x === c.id)) push('Object', c.title || c.id, keyLabel(c.key, mac));
      const O = 'Work area';
      push(O, 'Hand Tool (temporary)', mac ? 'Space' : 'Space', 'hold, then drag');
      push(O, 'Zoom In Tool (temporary)', keyLabel('Mod+Space', mac), 'hold, then click or drag');
      push(O, 'Zoom Out Tool (temporary)', keyLabel('Mod+Alt+Space', mac), 'hold, then click');
      push(O, 'Nudge', '← ↑ → ↓', '0.05 mm');
      push(O, 'Nudge ×20', mac ? '⇧ ← ↑ → ↓' : 'Shift+Arrows', '1 mm');
      push(O, 'Duplicate while dragging', mac ? '⌥-drag' : 'Alt+drag');
      push(O, 'Constrain proportions / 45°', mac ? '⇧-drag' : 'Shift+drag');
      push(O, 'Resize from the center', mac ? '⌥- or ⌃-drag a handle' : 'Alt+ or Ctrl+drag a handle');
      push(O, 'Select inside a group', mac ? '⌥-click' : 'Alt+click');
      push(O, 'Deselect · back to the Selection Tool · close', 'Esc');
      push(O, 'Zoom to Selection', mac ? '⇧2' : 'Shift+2');
      let h = '';
      const order = ['Tools', 'File', 'Edit', 'Object', 'Type', 'Select', 'Effect', 'View', 'Window', 'Help', O];
      for (const g of [...groups.keys()].sort((a, b) => order.indexOf(a) - order.indexOf(b))) {
        h += `<tr class="ce-aiks__g"><th colspan="2">${esc(g === 'Tools' ? 'Tools' : g === O ? O : 'Menu Commands ▸ ' + g)}</th></tr>`;
        for (const [n, k, note] of groups.get(g)) h += `<tr><td>${esc(n)}${note ? `<small>${esc(note)}</small>` : ''}</td><td><kbd>${esc(k)}</kbd></td></tr>`;
      }
      return h || '<tr><td colspan="2" class="ce-aiks__none">No command matches.</td></tr>';
    };
    const body = `<div class="ce-aiks__top"><label class="ce-aiks__q"><span>Search</span><input type="search" data-ks-q autocomplete="off" spellcheck="false" aria-label="Search the shortcuts"></label>`
      + `<div class="ce-aiseg" role="radiogroup" aria-label="Keys for"><button type="button" role="radio" data-ks-os="mac">Mac</button><button type="button" role="radio" data-ks-os="win">Windows</button></div></div>`
      + `<div class="ce-aiks__scroll"><table class="ce-aiks"><thead><tr><th>Command</th><th>Shortcut</th></tr></thead><tbody data-ks-rows></tbody></table></div>`
      + `<p class="ce-aidlg__note">Illustrator's default keys. A Photoshop key is used only where Illustrator has none. Keys the browser keeps for itself (new tab, close tab) are not listed.</p>`;
    const d = dialog(w, 'Keyboard Shortcuts', body, { wide: true, focus: '[data-ks-q]' });
    const q = d.box.querySelector('[data-ks-q]'), tb = d.box.querySelector('[data-ks-rows]');
    const paint = () => { tb.innerHTML = rowsHtml(q.value); d.box.querySelectorAll('[data-ks-os]').forEach(b => { const on = (b.dataset.ksOs === 'mac') === mac; b.classList.toggle('is-on', on); b.setAttribute('aria-checked', String(on)); }); };
    q.addEventListener('input', paint);
    d.box.addEventListener('click', ev => { const b = ev.target.closest('[data-ks-os]'); if (b) { mac = b.dataset.ksOs === 'mac'; setPrefs({ keys: mac ? 'mac' : 'win' }); paint(); } });
    paint();
    return d;
  }
  /** Edit ▸ Preferences: User Interface (Brightness, Canvas Color, the Toolbar's columns), General, Guides & Grid. A change shows at once, as Illustrator's does; Cancel puts it back. */
  function prefsDialog(w, tab) {
    const p0 = prefs(), s = st(w);
    const swatch = ([v, n]) => `<button type="button" class="ce-aibright ce-aibright--${v}" role="radio" data-pf-ui="${v}" title="${n}" aria-label="${n}"></button>`;
    const body = `<div class="ce-aipf"><nav class="ce-aipf__nav" role="tablist">${[['general', 'General'], ['ui', 'User Interface'], ['guides', 'Guides & Grid']].map(([k, n]) => `<button type="button" role="tab" data-pf-tab="${k}">${n}</button>`).join('')}</nav><div class="ce-aipf__pane">`
      + `<section data-pf-pane="general"><h4>General</h4><dl class="ce-aipf__dl"><dt>Keyboard Increment</dt><dd>0.05 mm <small>(Shift: 1 mm)</small></dd><dt>Units</dt><dd>Millimeters <small>(type: points)</small></dd><dt>Constrain Angle</dt><dd>0°</dd></dl>`
      + `<label class="ce-aichk"><input type="checkbox" data-pf-opt="smart"${s.opts.smart ? ' checked' : ''}> Smart Guides <small>(${esc(keyLabel('Mod+U'))})</small></label></section>`
      + `<section data-pf-pane="ui"><h4>User Interface</h4><div class="ce-aipf__row"><span class="ce-aipf__lab">Brightness:</span><div class="ce-aipf__bright" role="radiogroup" aria-label="Brightness">${UI_LEVELS.map(swatch).join('')}</div><span class="ce-aipf__val" data-pf-ui-name></span></div>`
      + `<div class="ce-aipf__row"><span class="ce-aipf__lab">Canvas Color:</span><div class="ce-aipf__radios"><label class="ce-aichk"><input type="radio" name="ce-pf-canvas" value="match" data-pf-canvas> Match User Interface Brightness</label><label class="ce-aichk"><input type="radio" name="ce-pf-canvas" value="white" data-pf-canvas> White</label></div></div>`
      + `<div class="ce-aipf__row"><span class="ce-aipf__lab">Toolbar:</span><div class="ce-aipf__radios"><label class="ce-aichk"><input type="radio" name="ce-pf-cols" value="1" data-pf-cols> Single column</label><label class="ce-aichk"><input type="radio" name="ce-pf-cols" value="2" data-pf-cols> Double column</label></div></div></section>`
      + `<section data-pf-pane="guides"><h4>Guides &amp; Grid</h4><label class="ce-aichk"><input type="checkbox" data-pf-opt="showGuides"${s.opts.showGuides ? ' checked' : ''}> Show Guides</label><label class="ce-aichk"><input type="checkbox" data-pf-opt="snapGuides"${s.opts.snapGuides ? ' checked' : ''}> Snap to Guides</label>`
      + `<label class="ce-aichk"><input type="checkbox" data-pf-opt="grid"${s.opts.grid ? ' checked' : ''}> Show Grid <small>(every 1 mm, 0.1 mm when zoomed in)</small></label><label class="ce-aichk"><input type="checkbox" data-pf-opt="snapGrid"${s.opts.snapGrid ? ' checked' : ''}> Snap to Grid</label></section>`
      + `</div></div>`;
    let cur = Object.assign({}, p0);
    const d = dialog(w, 'Preferences', body, { cancel: true, onClose: ok => { if (!ok) setPrefs(p0); } });
    const show = t => { d.box.querySelectorAll('[data-pf-tab]').forEach(b => { const on = b.dataset.pfTab === t; b.classList.toggle('is-on', on); b.setAttribute('aria-selected', String(on)); }); d.box.querySelectorAll('[data-pf-pane]').forEach(x => { x.hidden = x.dataset.pfPane !== t; }); };
    const paint = () => {
      d.box.querySelectorAll('[data-pf-ui]').forEach(b => { const on = b.dataset.pfUi === cur.ui; b.classList.toggle('is-on', on); b.setAttribute('aria-checked', String(on)); });
      const n = UI_LEVELS.find(l => l[0] === cur.ui); d.box.querySelector('[data-pf-ui-name]').textContent = n ? n[1] : '';
      d.box.querySelectorAll('[data-pf-canvas]').forEach(r => { r.checked = r.value === cur.canvas; });
      d.box.querySelectorAll('[data-pf-cols]').forEach(r => { r.checked = +r.value === cur.cols; });
    };
    d.box.addEventListener('click', ev => {
      const t = ev.target.closest('[data-pf-tab]'); if (t) { show(t.dataset.pfTab); return; }
      const u = ev.target.closest('[data-pf-ui]'); if (u) { cur = setPrefs({ ui: u.dataset.pfUi }); paint(); }
    });
    d.box.addEventListener('change', ev => {
      const t = ev.target;
      if (t.matches('[data-pf-canvas]')) cur = setPrefs({ canvas: t.value });
      else if (t.matches('[data-pf-cols]')) cur = setPrefs({ cols: +t.value });
      else if (t.matches('[data-pf-opt]')) { if (!!st(w).opts[t.dataset.pfOpt] !== t.checked) w.act('opt', t.dataset.pfOpt); }
      paint();
    });
    show(tab || 'ui'); paint();
    return d;
  }
  /** Help ▸ Charm Editor Help…: what the Illustrator user needs to know about this editor that Illustrator does not do. */
  function helpSheet(w) {
    const k = s => esc(keyLabel(s));
    const body = `<div class="ce-aihelp"><p>This editor works as Adobe Illustrator does: the same menus, tools, keys and panels. What is different here:</p><ul>`
      + `<li><b>Front and Back</b> are the two document tabs: one charm, both of its faces.</li>`
      + `<li><b>Fill and Stroke are the laser.</b> A stroke is engraved along the line, a fill is engraved as a hatch, a cut line is cut through. ${k('Shift+X')} swaps a line engraving and a hatch fill.</li>`
      + `<li><b>Save</b> (${k('Mod+S')}) keeps the charm and closes the editor; <b>Cancel</b> leaves without keeping anything. <b>Send to Sheet…</b> puts the design on a sheet.</li>`
      + `<li><b>Units are millimeters.</b> The arrows nudge 0.05 mm, Shift + arrow 1 mm. Words are sized in points.</li>`
      + `<li>Drag from a <b>ruler</b> for a guide, back onto it to remove it; drag from the corner where the rulers meet to move the zero point (double-click it to reset).</li>`
      + `<li><b>View ▸ Laser Check</b> lists what the laser cannot do before you save.</li></ul></div>`;
    return dialog(w, 'Charm Editor Help', body);
  }

  const api = { catalog: C, tools: TOOLS, windowItems, menus: MENUS, keyLabel, parse, matches, mac: MAC, prefs, setPrefs, mount, run, UI_LEVELS, version: 1,
    openShortcuts: () => (live && live.w ? sheet(live.w) : null), openPrefs: tab => (live && live.w ? prefsDialog(live.w, tab) : null),
    _menuItems: (w, i) => menuItems(w, MENUS[i][0], MENUS[i][1]),
    /* for charm-nest-ce-ai-keys.js (the right-click menus, the keys, the shortcuts sheet): an entry as a menu shows it now (null: not shown), a list as menu markup, a
       submenu's entries by its menu and name, the dynamic lists (fonts, sizes, cases), the open workspace's handle */
    resolve: (w, id, arg) => resolve(w, byId.get(id), arg), menuHtml: list => itemsHtml(tidy(list), 0), subItems: (w, path, name) => subItems(w, path, name),
    dynItems: (w, kind) => dynItems(w, { dyn: kind }), handle: () => (live ? live.w : null), openAt: (el, cx, cy, list) => (live && live.api ? live.api.openAt(el, cx, cy, list) : false) };
  return api;
});
