/* charm-nest-ce-ai-dialogs.js — THE CHARM EDITOR'S DIALOGS, DRAWN AS ADOBE ILLUSTRATOR'S (AIDIALOGS; Paul, 28 Oct 2026: "I need you to update the UI for the Entire Charm
 * Editor, all context menus, layer area etc.. The idea is that my employees know how to operate Adobe illustrator very well ... it must fit like a glove in great detail. I'm not
 * looking for you to change the functionality of things as they appear to run really well this is more about the front facing user facing UI and interface").
 *
 * Every dialog and modal of the editor, as Illustrator's dialogs look on a Mac (Medium Dark): the name in a title bar across the top (dragged, the dialog moves, as a window
 * does), labels on the left ending in a colon, fields with their unit inside them, the Preview checkbox at the bottom left, Cancel / OK at the bottom right with OK the
 * blue default (Enter) and Esc as Cancel, Spectrum's pill buttons, checkboxes, sliders and segmented buttons, number fields that step with ↑ ↓ (⇧: ten times).
 *   ops        Offset Path…, Cut line round the art…, Round Corners…, Move… / Rotate… / Scale… / Reflect… (one dialog, its tabs), Step and Repeat… (charm-nest-ce-ops.js)
 *   simplify   Simplify (charm-nest-ce-paths.js): its slider, the anchors before and after; Apply says OK
 *   warp       Warp Options and Blend Options (charm-nest-ce-warp.js)
 *   image      the Image step (charm-nest-ce-image.js): drawn as Illustrator's Image Trace, the preview beside it
 *   import     the vector import's roles (charm-nest-ce-import.js): as Illustrator's Place options
 *   newdoc     Design a charm (charm-nest-ce-masters.js): as Illustrator's New Document / Open (the library's tiles, the blank's preset details)
 *   saved      the card after Save (charm-nest-ce-masters.js)
 *   alert      Leave without saving?, Not saved yet (charm-nest-charm-editor.js), Keep your changes? (charm-nest-ce-library.js): as Illustrator's alerts
 *   sendsheet  Send to sheet… (charm-nest-custom-piece.js), while the editor is open
 *   check      the laser check's list (charm-nest-ce-extras.js)
 *
 * HOW. Nothing of what a dialog does changes: no element is moved or taken away, every data-* hook and listener stays; a dialog is found as it is put on the page (an observer of
 * the few elements dialogs are put in: the page's body, the editor's window, its work area's frame; never the drawing) and marked `aid aid--<kind>`, and this file's style sheet
 * draws the marked ones. What the layout asks of the order is CSS (`order`, grids). The few words changed are said in WORDS below. Number fields that have no ↑ ↓ of their own
 * get them here (a window key listener added when this file loads, so before the editor's own; the field's own input event carries the value as if typed).
 * The theme: the shell's --ai-* tokens (charm-nest-ce-ai-shell.js) on .ce-root; a dialog outside the editor's root is given the root's values; Medium Dark otherwise.
 * UMD: window.CharmEditorAiDialogs in the page; module.exports in node (the pure parts, for tests). */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api;
  else if (root) { root.CharmEditorAiDialogs = api; api.install(); }
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const doc = root && root.document ? root.document : null;
  const fin = v => typeof v === 'number' && Number.isFinite(v);

  /* ═══ 1 · which dialog is which (first match wins) ═══ */
  const KINDS = [
    ['.ce-ask.cops-modal', 'ops'],
    ['.ce-ask', 'alert'],
    ['.cw-panel', 'warp'],
    ['.cep-pop', 'simplify'],
    ['.cei', 'image'],
    ['.cei-root', 'import'],
    ['.cem-root.cem-root--card', 'saved'],
    ['.cem-root', 'newdoc'],
    ['dialog.cpDlg', 'sendsheet', () => !!(root && root.CharmEditor && root.CharmEditor.current)],   // (the order window's own Send to sheet stays the page's)
    ['.ce-menu.cx-list', 'check']
  ];
  const TITLES = { ops: null, alert: null, warp: null, simplify: 'Simplify', image: null, import: 'Place', newdoc: 'New Document', saved: null, sendsheet: null, check: 'Laser Check' };
  /** The words Illustrator says where ours differ (the button keeps its hook; only its text is Illustrator's). */
  const WORDS = { simplify: [['[data-x="yes"]', 'OK']] };
  /** Labels whose last words are Illustrator's (the checkbox inside stays as it is): [selector, words, what it said, kept as its tooltip]. */
  const LABELS = { ops: [['.cops-check', 'Preview']] };
  const TOKENS = ['frame', 'panel', 'panel-hi', 'pasteboard', 'border', 'input', 'input-border', 'text', 'text-dim', 'text-disabled', 'hover', 'accent', 'accent-text', 'menu', 'menu-hover', 'font', 'fs', 'row'];
  const matches = (el, sel) => { try { return el.matches(sel); } catch (e) { return false; } };
  function kindOf(el) { if (!el || el.nodeType !== 1) return null; for (const [sel, k, when] of KINDS) if (matches(el, sel)) return !when || when(el) ? k : null; return null; }

  /** A number field's step for ↑ ↓: a whole step for a field in %, °, steps or a count, else a tenth (mm); ⇧ ten times it. */
  function stepFor(unit, value, shift) {
    const u = String(unit || '').trim().toLowerCase();
    const whole = /%|°|deg|step|copies|points|count|px/.test(u) || (u === '' && /^-?\d+$/.test(String(value || '').trim()));
    return (whole ? 1 : 0.1) * (shift ? 10 : 1);
  }
  /** The value stepped: the number read as typed (a comma as a point, the minus sign), rounded to four places. */
  function stepValue(raw, dir, step) {
    const v = parseFloat(String(raw == null ? '' : raw).replace(',', '.').replace(/−/g, '-'));
    return String(+((fin(v) ? v : 0) + dir * step).toFixed(4));
  }

  /* ═══ 2 · the look ═══ */
  const T = (k, d) => `var(--ai-${k},${d})`;
  const FONT = T('font', '"Adobe Clean","Source Sans Pro",-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",sans-serif');
  const CSS = `
/* the palette: Illustrator's Medium Dark, unless the shell's tokens say otherwise; the dialogs' own variables mapped onto it */
.aid{ --d-bg:${T('panel', '#535353')}; --d-title:${T('frame', '#424242')}; --d-hi:${T('panel-hi', '#5e5e5e')}; --d-line:${T('border', '#3b3b3b')}; --d-in:${T('input', '#454545')};
  --d-inb:${T('input-border', '#383838')}; --d-tx:${T('text', '#ebebeb')}; --d-dim:${T('text-dim', '#bcbcbc')}; --d-dis:${T('text-disabled', '#858585')}; --d-hov:${T('hover', '#666666')};
  --d-ac:${T('accent', '#378ef0')}; --d-act:${T('accent-text', '#ffffff')}; --d-menu:${T('menu', '#3d3d3d')}; --d-menuh:${T('menu-hover', '#378ef0')}; --d-fs:${T('fs', '12px')};
  --ink:var(--d-tx); --muted:var(--d-dim); --hair:var(--d-line); --band:var(--d-in); --paper:var(--d-bg); --hov:var(--d-hov); --hov-line:var(--d-hov); --press:#4a4a4a; --gold:var(--d-ac); --gold-lt:#7fb6f5;
  --on-hov:#4b9cf5; --serif:${FONT}; --sans:${FONT}; }
.aid, .aid input, .aid select, .aid button, .aid textarea{ font-family:${FONT}; }
/* the card of each: a window of Illustrator's grey, a hairline and a soft shadow */
.aid .cops-card, .aid .ce-ask__card, .aid.cw-panel, .aid.cep-pop, .aid .cei__card, .aid .cei-card, .aid .cem-win, .aid .cem-card, .aid.cpDlg > .dlg{
  background:var(--d-bg) !important; color:var(--d-tx); border:1px solid #2a2a2a !important; border-radius:6px; box-shadow:0 0 0 .5px rgba(0,0,0,.6), 0 18px 48px rgba(0,0,0,.5) !important; font-size:var(--d-fs); overflow:hidden; }
/* THE TITLE BAR: the dialog's name centred in a strip across the top, as a Mac window's */
.aid .cops-card > h3, .aid .ce-ask__card > h3, .aid.cw-panel .cw-head, .aid.cep-pop > h4, .aid .cei__head, .aid .cei-card > .cei-eye, .aid .cem-head, .aid .cem-card > h3, .aid.cpDlg .dlgHead{
  font-family:${FONT} !important; }
.aid .cops-card > h3, .aid .ce-ask__card > h3, .aid.cep-pop > h4, .aid .cem-card > h3{
  display:block; margin:-16px -16px 12px !important; padding:0 12px; height:28px; line-height:28px; text-align:center; background:var(--d-title); border-bottom:1px solid #2c2c2c;
  font-size:13px !important; font-weight:600 !important; letter-spacing:0 !important; text-transform:none !important; color:var(--d-tx) !important; cursor:default; user-select:none; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.aid [data-aid-drag]{ cursor:default; }
.aid p, .aid .cops-card p{ color:var(--d-dim) !important; font-size:12px !important; line-height:1.45 !important; letter-spacing:0 !important; }
.aid b, .aid strong{ color:var(--d-tx); }

/* fields: Illustrator's, the unit inside the field, the label on the left with its colon */
.aid .ce-num, .aid .cw-num, .aid .cem-f, .aid .cem-name, .aid .cem-search, .aid .cpIn, .aid select, .aid .cops-key select, .aid input[type=search], .aid .cei-card select{
  background:var(--d-in) !important; border:1px solid var(--d-inb) !important; border-radius:3px !important; color:var(--d-tx) !important; box-shadow:inset 0 1px 1px rgba(0,0,0,.18) !important; }
.aid .ce-num, .aid .cw-num{ height:24px !important; padding:0 6px !important; gap:3px !important; display:inline-flex; align-items:center; }
.aid .ce-num:focus-within, .aid .cw-num:focus-within, .aid .cem-f:focus-within, .aid .cem-name:focus-within, .aid .cem-search:focus-within, .aid .cpIn:focus, .aid select:focus{
  border-color:var(--d-ac) !important; box-shadow:0 0 0 1px var(--d-ac) !important; outline:none !important; background:var(--d-in) !important; }
.aid .ce-num input, .aid .cw-num input, .aid .cem-f input, .aid .cem-name input, .aid .cem-search input{ color:var(--d-tx) !important; background:transparent !important; font-size:12px !important; font-weight:400 !important; letter-spacing:0 !important; }
.aid .ce-num i, .aid .cw-num i, .aid .cem-u{ font-style:normal; font-size:12px !important; font-weight:400 !important; letter-spacing:0 !important; text-transform:none !important; color:var(--d-dim) !important; }
.aid select{ height:24px; padding:0 22px 0 8px !important; font-size:12px !important; -webkit-appearance:none; appearance:none;
  background-image:linear-gradient(45deg,transparent 50%,var(--d-dim) 50%),linear-gradient(135deg,var(--d-dim) 50%,transparent 50%) !important; background-position:calc(100% - 11px) 10px,calc(100% - 7px) 10px !important; background-size:4px 4px !important; background-repeat:no-repeat !important; }
.aid select option{ background:var(--d-menu); color:var(--d-tx); }
.aid input::placeholder{ color:var(--d-dis) !important; }
.aid input[type=checkbox]{ -webkit-appearance:none; appearance:none; width:14px; height:14px; margin:0; flex:none; border:1px solid #b3b3b3; border-radius:2px; background:transparent; position:relative; cursor:default; vertical-align:middle; }
.aid input[type=checkbox]:checked{ background:var(--d-ac); border-color:var(--d-ac); }
.aid input[type=checkbox]:checked::after{ content:""; position:absolute; left:4px; top:1px; width:4px; height:8px; border:solid #fff; border-width:0 1.8px 1.8px 0; transform:rotate(42deg); }
.aid input[type=checkbox]:focus-visible{ box-shadow:0 0 0 2px var(--d-ac); outline:none; }
.aid input[type=checkbox]:disabled{ opacity:.5; }
/* sliders: Spectrum's, a thin track and a round white handle */
.aid input[type=range]{ -webkit-appearance:none; appearance:none; height:18px; background:transparent; accent-color:var(--d-ac); cursor:default; }
.aid input[type=range]::-webkit-slider-runnable-track{ height:2px !important; background:#8a8a8a !important; border:0 !important; border-radius:1px !important; }
.aid input[type=range]::-webkit-slider-thumb{ -webkit-appearance:none; appearance:none; width:14px !important; height:14px !important; margin-top:-6px !important; border-radius:50%; border:2px solid #e6e6e6 !important; background:var(--d-bg) !important; box-shadow:none !important; }
.aid input[type=range]:focus-visible::-webkit-slider-thumb{ border-color:var(--d-ac) !important; box-shadow:0 0 0 2px rgba(55,142,240,.45) !important; }
.aid input[type=range]::-moz-range-track{ height:2px; background:#8a8a8a; border:0; }
.aid input[type=range]::-moz-range-progress{ background:#e6e6e6; height:2px; }
.aid input[type=range]::-moz-range-thumb{ width:12px; height:12px; border-radius:50%; border:2px solid #e6e6e6; background:var(--d-bg); box-shadow:none; }
/* segmented buttons: Illustrator's button groups */
.aid .ce-seg, .aid .cem-seg{ display:inline-flex; height:24px !important; border:1px solid var(--d-inb) !important; border-radius:4px; overflow:hidden; background:var(--d-in); }
.aid .ce-seg button, .aid .cem-seg button{ white-space:nowrap; background:var(--d-in) !important; color:var(--d-tx) !important; font-size:12px !important; font-weight:400 !important; letter-spacing:0 !important; text-transform:none !important; padding:0 10px !important; border-radius:0 !important; }
.aid .ce-seg button + button, .aid .cem-seg button + button{ border-left:1px solid var(--d-inb) !important; }
.aid .ce-seg button.is-on, .aid .cem-seg button.is-on{ background:#727272 !important; color:#fff !important; }
.aid .ce-seg button:not(.is-on):not(:disabled):hover, .aid .cem-seg button:not(.is-on):hover{ background:var(--d-hov) !important; }
.aid .ce-seg button:disabled{ color:var(--d-dis) !important; opacity:1 !important; }
/* BUTTONS: Spectrum's pills; the default (Enter) blue, the others outlined; their words as Illustrator writes them (no capitals, no spacing) */
.aid .ce-btn, .aid .cei-btn, .aid .cem-btn, .aid .cep-pop__b button, .aid.cpDlg .dlgFoot .btn, .aid .ce-prop, .aid .ce-act, .aid .cx-show{
  height:24px !important; min-height:24px !important; min-width:66px; padding:0 14px !important; border-radius:12px !important; border:1.5px solid #b8b8b8 !important; background:transparent !important;
  color:var(--d-tx) !important; font:600 12px/1 ${FONT} !important; letter-spacing:0 !important; text-transform:none !important; box-shadow:none !important; cursor:default; transform:none !important; transition:none !important; }
.aid .ce-act, .aid .ce-prop{ min-width:0; border-radius:4px !important; border-width:1px !important; border-color:var(--d-inb) !important; background:var(--d-in) !important; padding:0 8px !important; }
.aid .ce-act.is-on, .aid .ce-prop.is-on, .aid .ce-act[aria-pressed=true]{ background:#727272 !important; color:#fff !important; }
.aid .ce-btn:hover:not(:disabled), .aid .cei-btn:hover:not(:disabled), .aid .cem-btn:hover:not(:disabled), .aid .cep-pop__b button:hover, .aid.cpDlg .dlgFoot .btn:hover:not(:disabled), .aid .ce-act:hover:not(:disabled), .aid .ce-prop:hover:not(:disabled){
  background:rgba(255,255,255,.1) !important; border-color:#e0e0e0 !important; }
.aid .ce-btn:focus-visible, .aid .cei-btn:focus-visible, .aid .cem-btn:focus-visible, .aid .cep-pop__b button:focus-visible, .aid.cpDlg .dlgFoot .btn:focus-visible{ outline:none !important; box-shadow:0 0 0 2px var(--d-ac) !important; }
.aid .ce-btn:disabled, .aid .cei-btn:disabled, .aid .cem-btn:disabled, .aid.cpDlg .dlgFoot .btn:disabled{ opacity:.45; }
.aid [data-aid-default], .aid .ce-btn--ink, .aid .cem-btn--ink, .aid .cep-pop__b .is-go, .aid [data-db="ok"], .aid [data-cw-act="ok"], .aid [data-do="place"], .aid [data-act="start"], .aid.cpDlg .dlgFoot .btn[data-cp-send], .aid .cei-btn--gold{
  background:var(--d-ac) !important; border-color:var(--d-ac) !important; color:var(--d-act) !important; }
.aid [data-aid-default]:hover:not(:disabled), .aid .ce-btn--ink:hover:not(:disabled), .aid .cem-btn--ink:hover:not(:disabled), .aid .cep-pop__b .is-go:hover, .aid [data-db="ok"]:hover, .aid [data-cw-act="ok"]:hover, .aid [data-do="place"]:hover:not(:disabled), .aid [data-act="start"]:hover, .aid.cpDlg .dlgFoot .btn[data-cp-send]:hover:not(:disabled), .aid .cei-btn--gold:hover:not(:disabled){
  background:#4b9cf5 !important; border-color:#4b9cf5 !important; }
.aid .ce-btn--gold, .aid .cem-btn--gold{ border-color:var(--d-ac) !important; color:#9ccaff !important; }
.aid .ce-btn--ghost, .aid .cem-btn--ghost, .aid .cei-btn--ghost{ color:var(--d-tx) !important; }
/* the foot: the Preview checkbox at the left, the buttons at the right */
.aid .ce-ask__b, .aid .cw-foot, .aid .cep-pop__b, .aid .cei__foot .ce-actions, .aid .cem-actions, .aid .cem-card__b, .aid .cei-foot{ display:flex !important; gap:10px !important; align-items:center; justify-content:flex-end; flex-direction:row !important; }
.aid .cops-check, .aid .cw-check, .aid .cem-tog, .aid .aid-prev{ display:inline-flex !important; align-items:center; gap:7px; color:var(--d-tx) !important; font-size:12px !important; letter-spacing:0 !important; text-transform:none !important; cursor:default; user-select:none; margin:0 !important; }

/* ── ops: Offset Path, Round Corners, Move / Rotate / Scale / Reflect, Step and Repeat ── */
.aid.cops-modal.ce-ask{ background:transparent !important; }
.aid .cops-card{ width:min(344px,100%) !important; padding:16px 16px 14px !important; display:grid !important; grid-template-columns:auto 1fr; column-gap:10px; align-items:center; }
.aid .cops-card > *{ grid-column:1 / -1; }
.aid .cops-card > p{ margin:0 0 10px !important; }
.aid .cops-card .cops-f{ display:grid !important; grid-template-columns:96px minmax(0,1fr); align-items:center; gap:8px !important; margin-top:8px !important; }
.aid .cops-card .cops-f > span:first-child{ text-align:right; font-size:12px !important; font-weight:400 !important; letter-spacing:0 !important; text-transform:none !important; color:var(--d-tx) !important; white-space:nowrap; }
.aid .cops-card .cops-f > span:first-child::after{ content:":"; }
.aid .cops-card .ce-num{ width:120px !important; justify-content:flex-start !important; }
.aid .cops-card .ce-num input{ text-align:left !important; }
.aid .cops-card .cops-f .ce-seg{ justify-self:start; }
.aid .cops-tabs{ margin:0 0 6px !important; }
.aid .cops-tabs .ce-seg{ height:26px !important; }
.aid .cops-card .cops-check{ grid-column:1 !important; margin-top:16px !important; }
.aid .cops-card .ce-ask__b{ grid-column:2 !important; margin-top:16px !important; }
.aid .cops-card .ce-ask__b [data-db="cancel"]{ order:1; } .aid .cops-card .ce-ask__b [data-db="copy"]{ order:0; } .aid .cops-card .ce-ask__b [data-db="ok"]{ order:2; }
.aid .cops-card .cops-check:not(:first-child) ~ .ce-ask__b{ }
/* ── alerts: the app's mark at the left, the words, the buttons at the right (the default the one Enter presses: the focused one) ── */
.aid.ce-ask:not(.cops-modal){ background:rgba(0,0,0,.18) !important; }
.aid .ce-ask__card{ width:min(440px,100%) !important; padding:16px 16px 14px 70px !important; position:relative; }
.aid .ce-ask__card > h3{ margin:-16px -16px 12px -70px !important; }
.aid .ce-ask__card::before{ content:""; position:absolute; left:18px; top:46px; width:36px; height:36px; border-radius:8px; background:#2b2b2b; box-shadow:inset 0 0 0 1.5px #d4a73c; }
.aid .ce-ask__card::after{ content:"!"; position:absolute; left:18px; top:46px; width:36px; height:36px; line-height:36px; text-align:center; font:700 22px/36px ${FONT}; color:#e6b84a; }
.aid.ce-refuse .ce-ask__card::before{ box-shadow:inset 0 0 0 1.5px #e0605a; } .aid.ce-refuse .ce-ask__card::after{ color:#f07a72; }
.aid .ce-ask__card > p{ margin:0 0 14px !important; }
.aid .ce-refuse__list{ color:#ffb3a8 !important; font-size:12px !important; }
.aid .ce-ask__b [data-ask="yes"]:not(:only-child){ order:0; margin-right:auto; }
.aid .ce-ask__b [data-ask="alt"]{ order:1; } .aid .ce-ask__b [data-ask="no"]{ order:2; }
.aid .ce-ask__b [data-ask="no"]{ background:var(--d-ac) !important; border-color:var(--d-ac) !important; color:#fff !important; }
.aid .ce-ask__b [data-ask="yes"]:not(:only-child){ background:transparent !important; border-color:#b8b8b8 !important; color:var(--d-tx) !important; }
.aid .ce-ask__b [data-ask="yes"]:only-child{ background:var(--d-ac) !important; border-color:var(--d-ac) !important; color:#fff !important; }
.aid.cel-ask3 .ce-ask__b [data-k="discard"]{ order:0; margin-right:auto; } .aid.cel-ask3 .ce-ask__b [data-k="cancel"]{ order:1; } .aid.cel-ask3 .ce-ask__b [data-k="keep"]{ order:2; }
.aid.cel-ask3 .ce-ask__b [data-k="keep"]{ background:var(--d-ac) !important; border-color:var(--d-ac) !important; color:#fff !important; }
/* ── Simplify ── */
.aid.cep-pop{ padding:16px 16px 12px !important; min-width:300px; }
.aid.cep-pop > h4{ margin:-16px -16px 12px !important; }
.aid .cep-pop__row{ display:grid !important; grid-template-columns:auto 1fr auto; gap:8px; align-items:center; }
.aid .cep-pop__row::before{ content:"Simplify Curve:"; color:var(--d-tx); white-space:nowrap; }
.aid input[type=range]:focus, .aid input[type=range]:focus-visible{ outline:none !important; box-shadow:none !important; }
.aid .cep-pop__v{ white-space:nowrap; min-width:64px; height:22px; line-height:20px; padding:0 6px; background:var(--d-in); border:1px solid var(--d-inb); border-radius:3px; color:var(--d-tx) !important; font-weight:400 !important; text-align:left; }
.aid .cep-pop__n{ color:var(--d-dim) !important; margin:8px 0 2px; font-size:12px !important; letter-spacing:0 !important; text-transform:none !important; }
.aid .cep-pop__b{ margin-top:12px !important; }
.aid .cep-pop__b .aid-prev{ margin-right:auto !important; }
.aid .cep-pop__b [data-x="no"]{ order:1; } .aid .cep-pop__b [data-x="yes"]{ order:2; } .aid .cep-pop__b .aid-prev{ order:0; }
/* ── Warp / Blend Options ── */
.aid.cw-panel{ padding:0 14px 12px !important; }
.aid.cw-panel .cw-head{ display:flex; align-items:center; justify-content:center; gap:8px; margin:0 -14px 12px !important; padding:0 12px !important; height:28px; background:var(--d-title); border-bottom:1px solid #2c2c2c; }
.aid.cw-panel .cw-head h3{ margin:0 !important; font:600 13px/28px ${FONT} !important; letter-spacing:0 !important; color:var(--d-tx) !important; text-transform:none !important; }
.aid.cw-panel .cw-sub{ color:var(--d-dim) !important; font-size:11px !important; letter-spacing:0 !important; text-transform:none !important; max-width:45%; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.aid.cw-panel .cw-row{ display:grid !important; grid-template-columns:84px minmax(0,1fr) auto; gap:8px !important; margin:0 0 8px !important; }
.aid.cw-panel .cw-row > .ce-seg, .aid.cw-panel .cw-row > select, .aid.cw-panel .cw-row > .cw-val{ grid-column:2 / -1; justify-self:start; }
.aid.cw-panel .cw-lab{ text-align:right; color:var(--d-tx) !important; font-size:12px !important; font-weight:400 !important; letter-spacing:0 !important; text-transform:none !important; white-space:nowrap; }
.aid.cw-panel .cw-lab::after{ content:":"; }
.aid.cw-panel .cw-val{ color:var(--d-dim); }
.aid.cw-panel .cw-sec{ font-size:12px !important; font-weight:400 !important; letter-spacing:0 !important; text-transform:none !important; color:var(--d-tx) !important; border-top-color:var(--d-line) !important; padding-left:4px; }
.aid.cw-panel .cw-sec::after{ content:":"; }
.aid.cw-panel .cw-num{ width:74px; }
.aid.cw-panel .cw-num input{ width:100% !important; text-align:left !important; }
.aid.cw-panel .cw-busy{ color:var(--d-dim) !important; font-size:11px !important; letter-spacing:0 !important; text-transform:none !important; }
.aid.cw-panel .cw-foot{ margin-top:12px !important; padding-top:0 !important; border:0 !important; }
.aid.cw-panel .cw-check{ margin-right:auto !important; }
/* ── the Image step, as Illustrator's Image Trace beside its picture ── */
.aid.cei{ background:rgba(0,0,0,.25) !important; }
.aid .cei__card{ grid-template-columns:minmax(0,1fr) 300px !important; }
.aid .cei__head{ align-items:center !important; justify-content:center; gap:10px !important; height:28px; padding:0 12px !important; background:var(--d-title); border-bottom:1px solid #2c2c2c !important; }
.aid .cei__head h3{ font:600 13px/28px ${FONT} !important; color:var(--d-tx) !important; letter-spacing:0 !important; }
.aid .cei__eyebrow{ font-size:11px !important; font-weight:400 !important; letter-spacing:0 !important; text-transform:none !important; color:var(--d-dim) !important; }
.aid .cei__view{ background:var(--d-bg); }
.aid .cei__room{ background:${T('pasteboard', '#3a3a3a')} !important; border-color:var(--d-line) !important; }
.aid .cei__hint, .aid .cei__wait, .aid .cei__lab{ letter-spacing:0 !important; text-transform:none !important; font-size:11px !important; }
.aid .cei__wait{ background:rgba(40,40,40,.9) !important; color:var(--d-tx) !important; border-color:#2c2c2c !important; }
.aid .cei__lab{ color:#1c1d1d !important; }
.aid .cei__side{ background:var(--d-bg); border-left:1px solid var(--d-line) !important; gap:0 !important; padding:0 !important; }
.aid .cei__sec{ padding:8px 12px 10px; border-bottom:1px solid var(--d-line); }
.aid .cei__sec > b{ font-size:12px !important; font-weight:700 !important; letter-spacing:0 !important; text-transform:none !important; color:var(--d-tx) !important; margin:0 0 8px !important; }
.aid .cei__sl{ grid-template-columns:84px minmax(0,1fr) 40px !important; }
.aid .cei__sl label{ color:var(--d-tx) !important; font-size:12px !important; text-align:right; }
.aid .cei__sl label::after{ content:":"; }
.aid .cei__sl output{ height:20px; line-height:18px; padding:0 4px; background:var(--d-in); border:1px solid var(--d-inb); border-radius:3px; color:var(--d-tx) !important; font-weight:400 !important; text-align:left !important; }
.aid .cei__tog{ color:var(--d-tx) !important; font-size:12px !important; }
.aid .cei__tog .ce-tick{ border:1px solid #b3b3b3 !important; border-radius:2px; background:transparent; }
.aid .cei__tog.is-on .ce-tick{ background:var(--d-ac); border-color:var(--d-ac) !important; color:#fff; }
.aid .cei__more{ color:var(--d-tx) !important; font-size:12px !important; letter-spacing:0 !important; text-transform:none !important; font-weight:700 !important; }
.aid .cei__note, .aid .cei__sug, .aid .cei__into, .aid .cei__tol{ color:var(--d-dim) !important; font-size:11.5px !important; }
.aid .cei__note{ padding:10px 12px !important; }
.aid .cei__greys i{ border-color:#2c2c2c !important; }
.aid .cei__foot{ background:var(--d-bg); border-top:1px solid var(--d-line) !important; }
.aid .cei__prog span{ color:var(--d-tx) !important; letter-spacing:0 !important; }
.aid .cei__bar{ background:#3a3a3a !important; } .aid .cei__bar i{ background:var(--d-ac) !important; }
.aid .cei__foot [data-do="cancel"]{ order:1; } .aid .cei__foot [data-do="place"]{ order:2; }
/* ── the vector import, as Illustrator's Place options ── */
.aid.cei-root{ background:rgba(0,0,0,.3) !important; }
.aid .cei-card{ padding:16px 18px 14px !important; }
.aid .cei-card > .cei-eye{ display:block; margin:-16px -18px 10px !important; height:28px; line-height:28px; text-align:center; background:var(--d-title); border-bottom:1px solid #2c2c2c;
  font-size:13px !important; font-weight:600 !important; letter-spacing:0 !important; text-transform:none !important; color:var(--d-tx) !important; }
.aid .cei-card > h3{ font:600 15px/1.3 ${FONT} !important; color:var(--d-tx) !important; letter-spacing:0 !important; margin:0 0 4px !important; }
.aid .cei-card [class^="cei-"], .aid .cei-card [class*=" cei-"]{ letter-spacing:0 !important; text-transform:none !important; }
.aid .cei-card .cei-size, .aid .cei-card .cei-note{ color:var(--d-dim) !important; }
.aid .cei-card .cei-pre{ background:#fff !important; border-color:#2c2c2c !important; }
.aid .cei-card [data-role]{ background:var(--d-in) !important; color:var(--d-tx) !important; border-color:var(--d-inb) !important; border-radius:3px; }
.aid .cei-card [data-role].is-on, .aid .cei-card [data-role][aria-pressed=true], .aid .cei-card [data-role][aria-checked=true]{ background:#727272 !important; color:#fff !important; }
.aid .cei-card [data-g]{ border-color:var(--d-line) !important; }
.aid .cei-card [data-g]:hover{ background:rgba(255,255,255,.05) !important; }
.aid .cei-foot{ border-top:0 !important; margin-top:12px; }
.aid .cei-foot [data-a="cancel"]{ order:1; } .aid .cei-foot [data-a="new"]{ order:2; } .aid .cei-foot [data-a="place"]{ order:3; }
/* ── Design a charm, as Illustrator's New Document: the tabs across the top, the presets as tiles, the preset details at the right, Close / Create ── */
.aid.cem-root{ background:rgba(0,0,0,.45) !important; }
.aid .cem-win{ padding:0 !important; }
.aid .cem-head{ height:auto; min-height:44px; margin:0 !important; padding:0 12px 0 20px !important; background:var(--d-title); border-bottom:1px solid #2c2c2c !important; }
.aid .cem-id h2{ font:600 13px/1.2 ${FONT} !important; color:var(--d-tx) !important; letter-spacing:0 !important; }
.aid .cem-eyebrow{ font-size:11px !important; font-weight:400 !important; letter-spacing:0 !important; text-transform:none !important; color:var(--d-dim) !important; }
.aid .cem-seg{ border:0 !important; background:transparent !important; height:44px !important; border-radius:0; }
.aid .cem-seg button{ background:transparent !important; border:0 !important; height:44px; padding:0 14px !important; font-size:12px !important; font-weight:700 !important; color:var(--d-dim) !important; text-transform:uppercase !important; letter-spacing:.04em !important; box-shadow:none !important; }
.aid .cem-seg button.is-on{ background:transparent !important; color:#fff !important; box-shadow:inset 0 -2px 0 0 #fff !important; }
.aid .cem-seg button:not(.is-on):hover{ background:transparent !important; color:#fff !important; }
.aid .cem-x{ background:transparent !important; color:var(--d-dim) !important; border:0 !important; border-radius:4px; }
.aid .cem-x:hover{ background:var(--d-hov) !important; color:#fff !important; }
.aid .cem-pane{ padding:14px 20px 0; }
.aid .cem-search{ height:30px !important; }
.aid .cem-search svg{ color:var(--d-dim) !important; }
.aid .cem-count{ letter-spacing:0 !important; text-transform:none !important; font-weight:400 !important; color:var(--d-dim) !important; font-size:11px !important; }
.aid .cem-grid{ gap:14px !important; padding-top:12px; }
.aid .cem-tile{ background:transparent !important; border:1px solid transparent !important; border-radius:4px; padding:6px !important; color:var(--d-tx); }
.aid .cem-tile:hover, .aid .cem-tile:focus-visible{ background:rgba(255,255,255,.06) !important; border-color:var(--d-ac) !important; }
.aid .cem-tile:hover::before, .aid .cem-tile:focus-visible::before{ display:none; }
.aid .cem-pic{ background:#fff !important; border:1px solid #2c2c2c !important; border-radius:2px; color:#555; }
.aid .cem-tile b{ color:var(--d-tx) !important; font-weight:400 !important; font-size:12px !important; letter-spacing:0 !important; }
.aid .cem-tile i{ color:var(--d-dim) !important; font-size:11px !important; }
.aid .cem-sz{ background:var(--d-in) !important; border-color:var(--d-inb) !important; color:var(--d-tx) !important; border-radius:3px; letter-spacing:0 !important; }
.aid .cem-empty{ color:var(--d-dim) !important; }
.aid .cem-new{ grid-template-columns:minmax(0,1fr) 330px !important; }
.aid .cem-shapes{ gap:4px !important; }
.aid .cem-name{ width:100%; }
.aid .cem-new > div:first-child{ order:2; background:var(--d-hi); border-left:1px solid var(--d-line); margin:-14px -20px 0 0; padding:14px 18px; }
.aid .cem-new > .cem-prev{ order:1; }
.aid .cem-new > div:first-child::before{ content:"PRESET DETAILS"; display:block; font-size:11px; font-weight:700; letter-spacing:.06em; color:var(--d-dim); margin:0 0 12px; }
.aid .cem-label{ font-size:12px !important; font-weight:400 !important; letter-spacing:0 !important; text-transform:none !important; color:var(--d-tx) !important; margin:10px 0 6px !important; }
.aid .cem-shape{ background:var(--d-in) !important; border:1px solid var(--d-inb) !important; border-radius:4px; color:var(--d-dim) !important; font-size:11px !important; letter-spacing:0 !important; text-transform:none !important; }
.aid .cem-shape.is-on{ border-color:var(--d-ac) !important; background:#4a4a4a !important; color:#fff !important; box-shadow:0 0 0 1px var(--d-ac) !important; }
.aid .cem-shape span{ letter-spacing:0 !important; text-transform:none !important; }
.aid .cem-sizes{ border:0 !important; height:auto !important; gap:8px; width:auto !important; }
.aid .cem-f{ height:26px; padding:0 6px !important; gap:5px !important; }
.aid .cem-f + .cem-f{ border-left:1px solid var(--d-inb) !important; }
.aid .cem-f > span{ color:var(--d-dim) !important; opacity:1 !important; font-size:12px !important; font-weight:400 !important; }
.aid .cem-f input{ width:58px !important; text-align:left !important; }
.aid .cem-f[title*="mm"]::after{ content:"mm"; color:var(--d-dim); font-size:12px; }
.aid .cem-u{ display:none !important; }
.aid .cem-chip{ background:var(--d-in) !important; border:1px solid var(--d-inb) !important; border-radius:3px; color:var(--d-tx) !important; letter-spacing:0 !important; font-weight:400 !important; height:22px !important; }
.aid .cem-chip.is-on{ border-color:var(--d-ac) !important; background:#4a4a4a !important; }
.aid .cem-name{ height:26px !important; }
.aid .cem-prev__box{ background:${T('pasteboard', '#3a3a3a')} !important; border-color:var(--d-line) !important; }
.aid .cem-prev__box .o{ fill:#fff !important; stroke:#1c1d1d !important; } .aid .cem-prev__box .hl{ fill:${T('pasteboard', '#3a3a3a')} !important; stroke:#1c1d1d !important; }
.aid .cem-prev__dims{ letter-spacing:0 !important; text-transform:none !important; font-weight:400 !important; color:var(--d-dim) !important; }
.aid .cem-why, .aid .cem-tile__why{ color:#ffb3a8 !important; }
.aid .cem-foot{ margin:0 !important; padding:12px 20px !important; border-top:1px solid var(--d-line) !important; background:var(--d-bg); }
.aid .cem-hint{ letter-spacing:0 !important; text-transform:none !important; font-size:11px !important; color:var(--d-dim) !important; }
.aid .cem-btn--sm{ height:22px !important; }
/* ── the card after Save ── */
.aid.cem-root--card{ background:rgba(0,0,0,.25) !important; }
.aid .cem-card{ padding:16px 16px 14px !important; }
.aid .cem-card p.is-bad{ color:#ffb3a8 !important; }
.aid .cem-card__b [data-c="discard"], .aid .cem-card__b [data-c="done"]{ margin-right:auto; }
/* ── Send to sheet ── */
.aid.cpDlg{ background:transparent !important; color:var(--d-tx) !important; }
.aid.cpDlg::backdrop{ background:rgba(0,0,0,.35); }
.aid.cpDlg .dlgHead{ position:relative; background:var(--d-bg) !important; border-bottom:1px solid var(--d-line) !important; padding:38px 14px 10px !important; }
.aid.cpDlg .dlgHead h3{ position:absolute; left:0; right:0; top:0; height:28px; margin:0 !important; text-align:center; background:var(--d-title); border-bottom:1px solid #2c2c2c;
  font:600 13px/28px ${FONT} !important; color:var(--d-tx) !important; letter-spacing:0 !important; text-transform:none !important; }
.aid.cpDlg .dlgHead .cpX{ position:absolute; right:6px; top:3px; width:22px; height:22px; line-height:20px; padding:0; z-index:1; border-radius:4px; }
.aid.cpDlg .dlgHead .sub, .aid.cpDlg .cpHint, .aid.cpDlg .cpMeta, .aid.cpDlg .cpGoes, .aid.cpDlg .cpWho, .aid.cpDlg .cpWhy, .aid.cpDlg .cpLbl i{ color:var(--d-dim) !important; letter-spacing:0 !important; text-transform:none !important; }
.aid.cpDlg .cpTag{ background:var(--d-in) !important; color:var(--d-tx) !important; border-color:var(--d-inb) !important; letter-spacing:0 !important; }
.aid.cpDlg .cpX{ background:transparent !important; color:var(--d-dim) !important; border:0 !important; }
.aid.cpDlg .cpX:hover{ background:var(--d-hov) !important; color:#fff !important; }
.aid.cpDlg .cpBody, .aid.cpDlg .cpRight, .aid.cpDlg .cpLeft, .aid.cpDlg .dlgFoot{ background:var(--d-bg) !important; color:var(--d-tx) !important; border-color:var(--d-line) !important; }
.aid.cpDlg .cpPlate{ background:${T('pasteboard', '#3a3a3a')} !important; border-color:var(--d-line) !important; }
.aid.cpDlg .cpSec h4, .aid.cpDlg .cpLbl{ color:var(--d-tx) !important; font:700 12px/1.3 ${FONT} !important; letter-spacing:0 !important; text-transform:none !important; }
.aid.cpDlg .cpOpt, .aid.cpDlg .cpChip, .aid.cpDlg .cuM, .aid.cpDlg .cuQty, .aid.cpDlg .cuQty button, .aid.cpDlg .cpHits, .aid.cpDlg .cpSub{ background:var(--d-in) !important; color:var(--d-tx) !important; border-color:var(--d-inb) !important; letter-spacing:0 !important; }
.aid.cpDlg .cpOpt[aria-checked=true], .aid.cpDlg .cpOpt.on, .aid.cpDlg .cpOpt.is-on, .aid.cpDlg .cpChip[aria-checked=true], .aid.cpDlg .cpChip.on, .aid.cpDlg .cuM[aria-checked=true], .aid.cpDlg .cuM.on{
  border-color:var(--d-ac) !important; box-shadow:0 0 0 1px var(--d-ac) !important; background:#4a4a4a !important; }
.aid.cpDlg .cpOptT span, .aid.cpDlg .cpOptT{ color:var(--d-dim) !important; } .aid.cpDlg .cpOptT b{ color:var(--d-tx) !important; }
.aid.cpDlg .dlgFoot{ border-top:1px solid var(--d-line) !important; }
.aid.cpDlg .dlgFoot [data-cp-close]{ order:1; } .aid.cpDlg .dlgFoot [data-cp-send]{ order:2; }
/* ── the laser check's list ── */
.aid.cx-list{ background:var(--d-menu) !important; color:var(--d-tx) !important; border:1px solid #2a2a2a !important; border-radius:4px; box-shadow:0 8px 24px rgba(0,0,0,.5) !important; padding:0 0 4px !important; overflow:hidden; }
.aid.cx-list::before{ content:"Laser Check"; display:block; height:26px; line-height:26px; padding:0 10px; background:var(--d-title); border-bottom:1px solid #2c2c2c; font:600 12px/26px ${FONT}; color:var(--d-tx); }
.aid.cx-list .cx-row{ color:var(--d-tx) !important; font-size:12px !important; }
.aid.cx-list .cx-row + .cx-row{ border-top-color:var(--d-line) !important; }
.aid.cx-list .cx-row > span em{ color:var(--d-dim) !important; letter-spacing:0 !important; text-transform:none !important; font-size:11px !important; }
.aid.cx-list .ce-menu__head{ color:var(--d-dim) !important; letter-spacing:0 !important; text-transform:none !important; font-size:11px !important; }
.aid.cx-list .cx-show{ min-width:0; height:20px !important; padding:0 10px !important; }
`;
  function injectCss() {
    if (!doc || doc.getElementById('ceAiDialogsCss')) return;
    // (every rule carries an id's weight, :not(#aid-none), so the dialogs' own sheets, however specific, never out-say it; [hidden] keeps the last word)
    const s = doc.createElement('style'); s.id = 'ceAiDialogsCss'; s.textContent = CSS.replace(/\.aid(?![\w-])/g, '.aid:not(#aid-none)')
      + '\n.aid:not(#aid-none) [hidden]:not(#aid-none2), .aid[hidden]:not(#aid-none):not(#aid-none2){ display:none !important; }\n';
    (doc.head || doc.documentElement).appendChild(s);
  }

  /* ═══ 3 · dressing a dialog as it appears ═══ */
  /** A dialog outside the editor's root takes the root's --ai-* values (the shell's theme), so it looks as the editor does. */
  function syncTokens(el) {
    if (!doc || el.closest('.ce-root')) return;
    const r = doc.querySelector('.ce-root'); if (!r || !root.getComputedStyle) return;
    const cs = root.getComputedStyle(r);
    for (const t of TOKENS) { const v = cs.getPropertyValue('--ai-' + t); if (v && v.trim()) el.style.setProperty('--ai-' + t, v.trim()); }
  }
  /** The words Illustrator uses, on the buttons whose hooks stay. */
  function words(el, kind) {
    for (const [sel, text] of WORDS[kind] || []) { const b = el.querySelector(sel); if (b && b.textContent !== text) { b.dataset.aidWas = b.textContent; b.textContent = text; } }
    for (const [sel, text] of LABELS[kind] || []) {
      const l = el.querySelector(sel), t = l && [...l.childNodes].reverse().find(nd => nd.nodeType === 3 && nd.textContent.trim());
      if (t && t.textContent.trim() !== text) { l.title = t.textContent.trim(); t.textContent = ' ' + text; }
    }
  }
  /** A title bar dragged moves its dialog (as a window), by a transform: where it was put stays its own business. */
  function draggable(el, handle) {
    if (!handle || handle.__aidDrag) return; handle.__aidDrag = true; handle.setAttribute('data-aid-drag', '');
    let p = null;
    handle.addEventListener('pointerdown', e => {
      if (e.button !== 0 || e.target.closest('button,input,select')) return;
      const m = /translate\((-?[\d.]+)px,\s*(-?[\d.]+)px\)/.exec(el.style.transform || '');
      p = { x: e.clientX, y: e.clientY, ox: m ? +m[1] : 0, oy: m ? +m[2] : 0, id: e.pointerId };
      try { handle.setPointerCapture(e.pointerId); } catch (er) { /* fine */ }
      e.preventDefault();
    });
    handle.addEventListener('pointermove', e => { if (!p || e.pointerId !== p.id) return; el.style.transform = `translate(${Math.round(p.ox + e.clientX - p.x)}px, ${Math.round(p.oy + e.clientY - p.y)}px)`; });
    const end = e => { if (p && e.pointerId === p.id) p = null; };
    handle.addEventListener('pointerup', end); handle.addEventListener('pointercancel', end);
  }
  const titleOf = (el, kind) => {
    if (kind === 'ops') return [el.querySelector('.cops-card'), () => el.querySelector('.cops-card > h3')];
    if (kind === 'alert') return [el.querySelector('.ce-ask__card'), () => el.querySelector('.ce-ask__card > h3')];
    if (kind === 'warp') return [el, () => el.querySelector('.cw-head')];
    if (kind === 'simplify') return [el, () => el.querySelector('h4')];
    return [null, null];
  };
  const dressed = new Set();
  /** This style sheet last in the head: a dialog's own sheet, put in as it first opens, says the same things as strongly (its !important letter spacing). */
  function lastCss() { const s = doc.getElementById('ceAiDialogsCss'); if (!s) { injectCss(); return; } if (s.parentNode && s.parentNode.lastElementChild !== s) s.parentNode.appendChild(s); }
  function dress(el) {
    const kind = kindOf(el); if (!kind) return null;
    lastCss();
    if (!el.classList.contains('aid')) { el.classList.add('aid', 'aid--' + kind); dressed.add(el); }
    syncTokens(el);
    words(el, kind);
    if (kind === 'simplify' && !el.querySelector('.aid-prev')) {
      // (Simplify always shows its answer over the charm: its Preview is on, as Illustrator's is by default)
      const bar = el.querySelector('.cep-pop__b');
      if (bar) bar.insertAdjacentHTML('afterbegin', '<label class="aid-prev" title="The simplified paths are drawn over the charm as the slider moves"><input type="checkbox" checked disabled tabindex="-1"> Preview</label>');
    }
    const [box, h] = titleOf(el, kind);
    if (box && h) {
      draggable(box, h());
      // (the ops dialog draws itself again on every tab: its new title bar is draggable too)
      if (kind === 'ops' && !box.__aidMo && root.MutationObserver) { box.__aidMo = new root.MutationObserver(() => { draggable(box, h()); words(el, kind); }); box.__aidMo.observe(box, { childList: true }); }
    }
    if (TITLES[kind]) el.setAttribute('data-aid-title', TITLES[kind]);
    return kind;
  }

  /* ═══ 4 · watching where dialogs are put (never the drawing: the body, the editor's window and root, the work area's frame) ═══ */
  const watched = new WeakSet();
  let mo = null;
  function scan(parent) { if (!parent || !parent.children) return; for (const c of Array.from(parent.children)) if (kindOf(c)) dress(c); }
  function watch(parent) {
    if (!parent || watched.has(parent) || !mo) return;
    watched.add(parent); mo.observe(parent, { childList: true }); scan(parent);
  }
  function watchEditor() {
    const r = doc.querySelector('.ce-root'); if (!r) return;
    watch(r); const w = r.querySelector('.ce-win'); watch(w);
    const st = r.querySelector('.ce-stage'); if (st && st.parentNode) watch(st.parentNode);
  }
  function onMutations(list) {
    for (const m of list) for (const n of m.addedNodes) {
      if (n.nodeType !== 1) continue;
      if (kindOf(n)) dress(n);
      if (n.classList && n.classList.contains('ce-root')) { watch(n); watch(n.querySelector('.ce-win')); const st = n.querySelector('.ce-stage'); if (st && st.parentNode) watch(st.parentNode); }
      else if (n.classList && n.classList.contains('ce-win')) watch(n);
    }
  }

  /* ═══ 5 · the keys: ↑ ↓ on a number field that has none of its own (⇧: ten times), as Illustrator's fields ═══ */
  const NUDGE = 'input[data-cw-n], .cem-f input, .cei-card input[inputmode="decimal"], .aid.cpDlg input[inputmode="decimal"]';
  function onKeyDown(e) {
    if (e.key !== 'ArrowUp' && e.key !== 'ArrowDown') return;
    const t = e.target; if (!t || t.tagName !== 'INPUT' || t.disabled || t.readOnly || !t.closest || !t.closest('.aid')) return;
    if (!matches(t, NUDGE) || (t.dataset && t.dataset.dnum)) return;   // (the ops dialog's own fields step themselves)
    if (e.metaKey || e.ctrlKey || e.altKey) return;
    const unitEl = t.parentNode && t.parentNode.querySelector('i, .cem-u'), unit = (unitEl ? unitEl.textContent : '') || (t.dataset && t.dataset.cwN === 'steps' ? 'steps' : '');
    const st = stepFor(unit, t.value, e.shiftKey);
    t.value = stepValue(t.value, e.key === 'ArrowUp' ? 1 : -1, st);
    e.preventDefault(); e.stopImmediatePropagation();
    t.dispatchEvent(new root.Event('input', { bubbles: true })); t.dispatchEvent(new root.Event('change', { bubbles: true }));
  }

  let installed = false;
  function install() {
    if (!doc || installed) return; installed = true;
    injectCss();
    root.addEventListener('keydown', onKeyDown, true);
    if (root.MutationObserver) {
      mo = new root.MutationObserver(onMutations);
      const go = () => { watch(doc.body); watchEditor(); };
      if (doc.body) go(); else doc.addEventListener('DOMContentLoaded', go, { once: true });
    }
    // (an editor opened in a modal <dialog> of the page, the order window, is not in the body's own children: the editor tells us when it opens)
    const hook = () => { const CE = root.CharmEditor; if (!CE || typeof CE !== 'object') return false; (CE.onOpen = Array.isArray(CE.onOpen) ? CE.onOpen : []).push(ctx => { injectCss(); if (ctx && ctx.root) { watch(ctx.root); watch(ctx.root.querySelector('.ce-win')); if (ctx.stage && ctx.stage.parentNode) watch(ctx.stage.parentNode); } }); return true; };
    if (!hook() && doc.readyState === 'loading') doc.addEventListener('DOMContentLoaded', hook, { once: true });
  }

  return { version: 1, install, dress, kindOf, stepFor, stepValue, KINDS, WORDS, dressed: () => Array.from(dressed).filter(e => e.isConnected).map(e => ({ kind: kindOf(e), el: e })) };
});
