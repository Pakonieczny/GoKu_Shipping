/*  charm-nest-library-dnd.js — drag and drop in the Library (Paul, 3 Oct 04:47).
 *  "Drag a sheet from any place in the process and move that sheet up to laser cutting, and also to move a sheet from a
 *  previous process ... drop it into a set ... It needs to work backwards and forwards. The user should be able to move
 *  any sheet from any process to any process in the library tab", with "beautiful animation" and the user told what is
 *  approved, what is missing and what must be confirmed.
 *
 *  This file is the hands, not the brain. It never writes anything itself: a drop asks window.LibraryFlow (charm-nest-
 *  flow.js) for the plan (LibraryFlow.plan) and, once the plan is ok and every confirm has been pressed by a person,
 *  hands it back (LibraryFlow.commit). Where LibraryFlow is not on the page, nothing here shows at all: no grip, no menu,
 *  no dock.
 *
 *  What can be picked up: a sheet card (with its QR label), a set card, and a Completed row (or a sheet or set opened in
 *  place under it). A mouse or pen drags the card itself after 6 px; a finger drags from the grip at once, or from
 *  anywhere on the card after a short hold (a swipe still scrolls the page). The grip at the card's head is also the
 *  keyboard way: Enter or Space opens "Move to…", an inline list of the very same places (the ones that are not allowed
 *  are listed too, dimmed, with the reason).
 *
 *  While a card is held, a dock opens under the top of the Library (never under the top bar, never at the bottom) with a
 *  place for each target: In progress, Laser cutting, Completed, New set and each set, labelled. The same places light up
 *  where they stand on the page (the Laser cutting and In progress sections, each set card). A place that is not allowed
 *  is dimmed and says why while the card is over it. The card under the hand is a lifted copy of the real one (preview,
 *  parts, seals, counters); the real one stays where it is as a faint outline.
 *
 *  On the drop the copy flies to the place (window.LibraryFx.fly, else a plain flight here), while at once the plan is
 *  asked for and shown in an inline "Moving" bar on that place (window.LibraryApprovalUI, else a plain list here): green
 *  lines for what is approved or done by itself, red lines for what is missing (the card then flies back and nothing has
 *  changed), amber lines for what needs the person's own yes. A drop never says yes for anyone: a confirm, the Rose Gold
 *  green dash line first of all, is passed to commit only when its own button has been pressed. Nothing waits on a
 *  flight: the write starts the moment the plan is ok.
 *
 *  window.LibraryDnd: decorate(root) · enabled() · move(item, to, opts) · openMenu(item) · cancel() · state() */
(function () {
  'use strict';
  if (window.LibraryDnd) return;
  const doc = document, W = window, html = doc.documentElement;
  const EASE = 'cubic-bezier(.2,.8,.2,1)';
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const reduced = () => { try { return !!(W.Motion && W.Motion.reduced ? W.Motion.reduced() : W.matchMedia('(prefers-reduced-motion: reduce)').matches); } catch (_) { return false; } };
  const wait = ms => new Promise(r => setTimeout(r, ms));
  const byId = id => doc.getElementById(id);
  const stage = () => byId('stage');
  const CODE = { gold: 'GF', silver: 'SS', rose: 'RG', gold10k: '10K', gold14k: '14K' };
  const AREA = { progress: { name: 'In progress', sub: 'still being prepared' }, laser: { name: 'Laser cutting', sub: 'ready to cut' }, completed: { name: 'Completed', sub: 'cut by the laser' } };
  const SVG = {
    grip: '<svg viewBox="0 0 16 16" aria-hidden="true"><g fill="currentColor"><circle cx="5.5" cy="3.8" r="1.3"/><circle cx="10.5" cy="3.8" r="1.3"/><circle cx="5.5" cy="8" r="1.3"/><circle cx="10.5" cy="8" r="1.3"/><circle cx="5.5" cy="12.2" r="1.3"/><circle cx="10.5" cy="12.2" r="1.3"/></g></svg>',
    ok: '<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="7" fill="currentColor" opacity=".14"/><path d="M4.7 8.4l2.3 2.3 4.4-4.6" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></svg>',
    bad: '<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="7" fill="currentColor" opacity=".14"/><path d="M5.6 5.6l4.8 4.8M10.4 5.6l-4.8 4.8" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/></svg>',
    warn: '<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="7" fill="currentColor" opacity=".16"/><path d="M8 4.6v4M8 11v.1" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round"/></svg>'
  };
  const flow = () => { const f = W.LibraryFlow; return f && typeof f.targets === 'function' && typeof f.plan === 'function' && typeof f.commit === 'function' ? f : null; };
  const D = { pending: null, drag: null, move: null, menu: null, noClick: 0, seq: 0, pt: null, settling: 0 };
  /** A card is held, flying home or being moved (the Library's live read leaves its lists alone meanwhile). */
  const busy = () => !!(D.drag || D.move || D.settling > 0);
  const sync = () => { try { html.toggleAttribute('data-library-drag', busy()); } catch (_) { /* no attribute */ } };
  const specKey = s => s.area ? 'area:' + s.area : s.set ? 'set:' + s.set : 'newSet';
  const cleanSpec = s => s.area ? { area: s.area } : s.set ? { set: s.set } : { newSet: true };
  const visible = e => { if (!e || !e.isConnected) return false; const r = e.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
  const dayShort = d => { try { return d ? new Date(d + 'T12:00:00').toLocaleDateString(undefined, { month: 'short', day: 'numeric' }) : ''; } catch (_) { return ''; } };
  const plural = (n, a, b) => `${n} ${n === 1 ? a : b || a + 's'}`;
  const who = () => { try { return (W.CNEmployee && W.CNEmployee.name && W.CNEmployee.name()) || undefined; } catch (_) { return undefined; } };

  /* ═══ style ═══ */
  const CSS = `
.dndGrip{flex:0 0 auto;width:22px;height:22px;margin-left:2px;padding:0;border:0;border-radius:7px;background:transparent;color:var(--ink45);display:inline-grid;place-items:center;cursor:grab;opacity:0;transition:opacity .14s ease,background-color .14s,color .14s;touch-action:none;-webkit-user-select:none;user-select:none}
.dndGrip svg{width:14px;height:14px;display:block;pointer-events:none}
.libCard:hover .dndGrip,.setCard:hover>.sh .dndGrip,.ldLine:hover .dndGrip,.dndGrip:focus-visible,.dndGrip[aria-expanded="true"]{opacity:1}
.dndGrip:hover,.dndGrip:focus-visible,.dndGrip[aria-expanded="true"]{background:var(--paper2);color:var(--ink);outline:none}
.dndGrip:focus-visible{box-shadow:0 0 0 2px var(--gold2)}
.setCard>.sh .dndGrip{margin-left:auto}
@media (hover:none){.dndGrip{opacity:.8}}
html.dndOn,html.dndOn *{cursor:grabbing!important;-webkit-user-select:none!important;user-select:none!important}
.dndSource{opacity:.4!important;outline:1.5px dashed var(--gold2);outline-offset:3px;border-radius:12px;transition:opacity .2s ease}
.dndArming{transform:scale(.985);transition:transform .32s ease}
.dndLift{position:fixed;left:0;top:0;border-radius:12px;pointer-events:none;will-change:transform;z-index:1}
.dndLift::before{content:"";position:absolute;inset:0;border-radius:inherit;box-shadow:0 28px 56px rgba(30,24,16,.3),0 4px 12px rgba(30,24,16,.16);opacity:0;transition:opacity .22s ease}
.dndLift.on::before{opacity:1}
.dndClip{position:absolute;inset:0;overflow:hidden;border-radius:inherit}
.dndClip>.mGhost{position:absolute!important;left:0!important;top:0!important}
.dndLift.cropY .dndClip{-webkit-mask-image:linear-gradient(#000 calc(100% - 46px),transparent);mask-image:linear-gradient(#000 calc(100% - 46px),transparent)}
.dndLift.cropX .dndClip{-webkit-mask-image:linear-gradient(90deg,#000 calc(100% - 46px),transparent);mask-image:linear-gradient(90deg,#000 calc(100% - 46px),transparent)}
.dndLift.cropX.cropY .dndClip{-webkit-mask-image:linear-gradient(#000 calc(100% - 46px),transparent),linear-gradient(90deg,#000 calc(100% - 46px),transparent);mask-image:linear-gradient(#000 calc(100% - 46px),transparent),linear-gradient(90deg,#000 calc(100% - 46px),transparent);-webkit-mask-composite:source-in;mask-composite:intersect}

.dndDock{position:fixed;z-index:130;box-sizing:border-box;display:grid;gap:8px;padding:10px 12px 12px;border:1px solid var(--goldLine);border-radius:16px;background:color-mix(in srgb,var(--card) 93%,transparent);-webkit-backdrop-filter:blur(14px) saturate(1.1);backdrop-filter:blur(14px) saturate(1.1);box-shadow:0 18px 44px rgba(30,24,16,.2),0 2px 6px rgba(30,24,16,.08)}
.dndDockHead{display:flex;align-items:baseline;gap:8px;font:650 12px var(--sans);color:var(--ink);min-width:0}
.dndDockHead b{font-weight:700;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.dndDockHead span{font-weight:400;font-size:11px;color:var(--ink45);white-space:nowrap}
.dndRowWrap{display:grid;grid-template-rows:1fr;transition:grid-template-rows .32s ${EASE},opacity .22s ease}
.dndRowWrap>.dndRow{min-height:0}
.dndDock.bar .dndRowWrap{grid-template-rows:0fr;opacity:0;pointer-events:none}
.dndDock.bar{gap:0}
.dndDock.bar .dndDockHead{margin-bottom:6px}
.dndRow{display:flex;gap:8px;align-items:stretch;overflow-x:auto;overflow-y:hidden;padding:3px 2px 4px;scrollbar-width:none}
.dndRow::-webkit-scrollbar{display:none}
.dndSep{flex:0 0 1px;background:var(--line);margin:4px 2px}
.dndChip{flex:0 0 auto;min-width:122px;max-width:190px;display:grid;gap:1px;text-align:left;padding:8px 12px;border:1px dashed var(--goldLine);border-radius:11px;background:var(--card2);color:var(--ink);transition:transform .18s ${EASE},background-color .18s,border-color .18s,box-shadow .18s,opacity .18s}
.dndChipName{font:650 12.5px var(--sans);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.dndChipSub{font:11px var(--sans);color:var(--ink45);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;min-height:14px}
.dndChip[data-state="dim"]{opacity:.46;border-color:var(--line);cursor:not-allowed}
.dndChip[data-hot][data-state="armed"]{border-style:solid;border-color:var(--gold);background:var(--goldSoft);transform:translateY(-2px) scale(1.03);box-shadow:0 10px 22px rgba(169,130,63,.28)}
.dndChip[data-hot][data-state="armed"] .dndChipSub{color:var(--gold)}
.dndChip[data-hot][data-state="dim"]{opacity:1;border-style:solid;border-color:var(--clay);background:var(--claySoft)}
.dndChip[data-hot][data-state="dim"] .dndChipSub{color:#8a3a26}
.dndSlot:empty{display:none}
@keyframes dndIn{from{opacity:0}to{opacity:1}}
[data-dnd-state],[data-dnd-pulse]{position:relative}
[data-dnd-state]::after,[data-dnd-pulse]::after{content:"";position:absolute;inset:-6px;border-radius:16px;pointer-events:none;z-index:4}
[data-dnd-state]::after{border:1.5px dashed var(--goldLine);background:rgba(202,168,97,.05);animation:dndIn .28s ease both;transition:background-color .18s,border-color .18s,box-shadow .18s}
[data-dnd-state="dim"]::after{border-color:var(--line);background:repeating-linear-gradient(135deg,rgba(28,26,23,.035) 0 8px,transparent 8px 16px)}
[data-dnd-hot][data-dnd-state="armed"]::after{border:2px solid var(--gold);background:rgba(202,168,97,.15);box-shadow:0 0 0 5px rgba(202,168,97,.18)}
[data-dnd-hot][data-dnd-state="dim"]::after{border:2px solid var(--clay);background:rgba(176,86,63,.08);box-shadow:0 0 0 5px rgba(176,86,63,.12)}
[data-dnd-state]::before{content:attr(data-dnd-label);position:absolute;left:14px;top:-15px;z-index:5;padding:2px 11px;border-radius:999px;font:650 11px var(--sans);background:var(--card);border:1px solid var(--goldLine);color:var(--gold);box-shadow:0 3px 10px rgba(30,24,16,.1);pointer-events:none;animation:dndIn .28s ease both;white-space:nowrap}
[data-dnd-state="dim"]:not([data-dnd-hot])::before{display:none}
[data-dnd-hot][data-dnd-state="armed"]::before{background:var(--gold);border-color:var(--gold);color:#fff}
[data-dnd-hot][data-dnd-state="dim"]::before{background:var(--claySoft);border-color:var(--clay);color:#8a3a26}
@keyframes dndPulse{0%{opacity:.9;transform:scale(.985)}100%{opacity:0;transform:scale(1.035)}}
[data-dnd-pulse]::after{border:2px solid var(--gold);animation:dndPulse 1s ease-out both}
@keyframes dndLanded{0%{box-shadow:0 0 0 0 rgba(169,130,63,.55)}60%{box-shadow:0 0 0 7px rgba(169,130,63,.0)}100%{box-shadow:0 0 0 0 rgba(169,130,63,0)}}
.dndLanded{animation:dndLanded 1.3s ease-out 1}

.dndMovingWrap{display:grid;grid-template-rows:0fr;transition:grid-template-rows .34s ${EASE};margin:0 0 4px}
.dndMovingWrap.on{grid-template-rows:1fr}
.dndMoving{min-height:0;overflow:hidden;border:1px solid var(--goldLine);border-radius:12px;background:var(--card);box-shadow:var(--sh)}
.dndMovingIn{padding:10px 14px 11px;display:grid;gap:6px}
.dndMovingHead{display:flex;align-items:center;gap:10px;font:650 12.5px var(--sans);color:var(--ink);min-width:0}
.dndMovingTitle{flex:1 1 auto;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.dndX{flex:0 0 auto;width:22px;height:22px;border-radius:50%;border:0;background:transparent;color:var(--ink45);font-size:15px;line-height:1;display:grid;place-items:center}
.dndX:hover{background:var(--paper2);color:var(--ink)}
.dndX[hidden]{display:none}
.dndWait{display:flex;align-items:center;gap:9px;font:12px var(--sans);color:var(--ink70)}
.dndWait[hidden]{display:none}
.dndSpin{flex:0 0 12px;width:12px;height:12px;border:2px solid var(--line);border-top-color:var(--ink70);border-radius:50%;animation:dndSpin .7s linear infinite}
@keyframes dndSpin{to{transform:rotate(360deg)}}
.dndApprove{display:grid;gap:2px}
.dndMovingWrap.ui .dndMoving{border:0;background:none;box-shadow:none;border-radius:0}
.dndMovingWrap.ui.settled .dndMoving{overflow:visible}
.dndMovingWrap.ui .dndMovingIn{padding:0;gap:0}
.dndMovingWrap.ui .dndMovingHead,.dndMovingWrap.ui .dndWait{display:none}
.dndMovingWrap.ui .dndApprove{gap:0}
.dndDock.bar.ui{border-color:transparent;background:none;box-shadow:none;-webkit-backdrop-filter:none;backdrop-filter:none;padding:0}
.dndDock.bar.ui .dndDockHead{display:none}
.dndHead{font:600 12.5px var(--sans);color:var(--ink);padding:2px 0 3px}
.dndHead.bad{color:#8a3a26}
.dndHead.warn{color:#7a5a1d}
.dndLine{display:flex;gap:9px;align-items:flex-start;font:12.5px/1.4 var(--sans);padding:3px 0;opacity:0;transform:translateY(4px);animation:dndLine .36s ${EASE} forwards;animation-delay:calc(var(--i,0)*120ms)}
@keyframes dndLine{to{opacity:1;transform:none}}
.dndLine svg{flex:0 0 15px;width:15px;height:15px;margin-top:1px}
.dndLine small{display:block;color:var(--ink45);font-size:11.5px}
.dndLine.ok{color:#3c5a39}.dndLine.ok svg{color:var(--sage)}
.dndLine.ok svg path{stroke-dasharray:20;stroke-dashoffset:20;animation:dndDraw .42s ease forwards;animation-delay:calc(var(--i,0)*120ms + 180ms)}
@keyframes dndDraw{to{stroke-dashoffset:0}}
.dndLine.bad{color:#8a3a26}.dndLine.bad svg{color:var(--clay)}
.dndLine.note{color:var(--ink45)}
.dndLine.warn{color:#7a5a1d;background:var(--goldSoft);border-radius:10px;padding:9px 11px;align-items:center;flex-wrap:wrap}
.dndLine.warn svg{color:var(--gold)}
.dndLine.warn .dndTxt{flex:1 1 220px}
.dndActs{display:flex;gap:8px;flex-wrap:wrap;margin-top:4px;opacity:0;animation:dndLine .36s ${EASE} forwards;animation-delay:calc(var(--i,0)*120ms)}
.dndBtn{border:1px solid var(--line);background:var(--card);border-radius:999px;padding:5px 14px;font:650 11.5px var(--sans);color:var(--ink);transition:background-color .14s,border-color .14s,color .14s}
.dndBtn:hover,.dndBtn:focus-visible{background:var(--ink);border-color:var(--ink);color:var(--paper);outline:none}
.dndBtn.gold{background:var(--gold);border-color:var(--gold);color:#2a2013}
.dndBtn.gold:hover,.dndBtn.gold:focus-visible{background:#8f6c30;border-color:#8f6c30;color:#fff}
.dndBtn:disabled{opacity:.5;pointer-events:none}

.dndMenuWrap{display:grid;grid-template-rows:0fr;transition:grid-template-rows .26s ${EASE}}
.dndMenuWrap.on{grid-template-rows:1fr}
.dndMenu{min-height:0;overflow:hidden}
.dndMenuIn{margin-top:4px;border:1px solid var(--line);border-radius:10px;background:var(--card2);padding:6px;display:grid;gap:2px;max-height:260px;overflow:auto;cursor:default}
.dndMenuTitle{font:650 10.5px var(--sans);letter-spacing:.04em;text-transform:uppercase;color:var(--ink45);padding:2px 8px 4px}
.dndMenuItem{display:grid;text-align:left;gap:0;padding:6px 8px;border:0;border-radius:7px;background:transparent;font:600 12px var(--sans);color:var(--ink)}
.dndMenuItem:hover,.dndMenuItem:focus-visible{background:var(--goldSoft);outline:none}
.dndMenuItem small{font:400 11px var(--sans);color:var(--ink45)}
.dndMenuItem[aria-disabled="true"]{color:var(--ink45);cursor:not-allowed}
.dndMenuItem[aria-disabled="true"]:hover,.dndMenuItem[aria-disabled="true"]:focus-visible{background:var(--claySoft)}
.dndMenuItem[aria-disabled="true"]:hover small,.dndMenuItem[aria-disabled="true"]:focus-visible small{color:#8a3a26}
.dndMenuWait{display:flex;align-items:center;gap:9px;padding:6px 8px;font:12px var(--sans);color:var(--ink70)}
@media (prefers-reduced-motion:reduce){.dndGrip,.dndSource,.dndArming,.dndLift::before,.dndDock,.dndRowWrap,.dndChip,.dndMovingWrap,.dndMenuWrap,[data-dnd-state]::after{transition:none!important}
 .dndLine,.dndActs,[data-dnd-state]::after,[data-dnd-state]::before,.dndLanded{animation:none!important;opacity:1!important;transform:none!important}
 .dndLine.ok svg path{animation:none;stroke-dashoffset:0}
 [data-dnd-pulse]::after{animation:none;opacity:.7}}
`;
  function style() { if (byId('dndStyle')) return; const s = doc.createElement('style'); s.id = 'dndStyle'; s.textContent = CSS; doc.head.appendChild(s); }

  /* ═══ what is picked up, where it is, where it may go ═══ */
  // anything that does its own thing on a press is left alone: a press on it never starts a drag
  const INTERACTIVE = 'button, a, input, select, textarea, summary, label, [contenteditable], [data-no-dnd], [data-big], .seal, [data-seal-group], .dndMenuWrap, .dndMovingWrap, .ldMark';
  const sheetCard = el => el && el.querySelector(':scope > .libCard[data-id]');
  /** The item under `t`: { kind, id, el } (el: the element lifted), or null. */
  function itemAt(t) {
    if (!(t instanceof Element) || !t.closest('#libBody, #libDone')) return null;
    const ls = t.closest('.librarySheet');
    if (ls && sheetCard(ls)) return { kind: 'sheet', id: sheetCard(ls).dataset.id, el: ls };
    const sc = t.closest('.setCard');
    if (sc) { const st = sc._laserSet; return st && st.setId && !st.standalone && !st.working ? { kind: 'set', id: st.setId, el: sc } : null; }
    const line = t.closest('#libDone .ldLine'), row = line && line.closest('.ldItem');
    if (row && !row.classList.contains('skel')) { const k = row.dataset.kind; const id = k === 'set' ? row.dataset.set : row.dataset.id; return id ? { kind: k === 'set' ? 'set' : 'sheet', id, el: line, row } : null; }
    return null;
  }
  /** The card of an item as the page draws it now (a redraw replaces its element). */
  function elOf(item) {
    if (item.el && item.el.isConnected) return item.el;
    const root = doc.querySelectorAll('#libBody, #libDone');
    for (const r of root) {
      if (item.kind === 'sheet') { const c = [...r.querySelectorAll('.libCard[data-id]')].find(x => x.dataset.id === item.id); if (c) return (item.el = c.closest('.librarySheet') || c); const l = r.querySelector(`.ldItem[data-kind="sheet"][data-id="${CSS.escape(item.id)}"] .ldLine`); if (l) return (item.el = l); }
      else { const c = [...r.querySelectorAll('.setCard')].find(x => x._laserSet && x._laserSet.setId === item.id); if (c) return (item.el = c); const l = r.querySelector(`.ldItem[data-kind="set"][data-set="${CSS.escape(item.id)}"] .ldLine`); if (l) return (item.el = l); }
    }
    return null;
  }
  function labelOf(item) {
    const el = elOf(item); if (!el) return item.kind === 'set' ? 'This set' : 'This sheet';
    if (item.kind === 'sheet') {
      const c = el.matches('.librarySheet') ? sheetCard(el) : null;
      if (c) { const nm = c.querySelector('.h .nm'), m = c.dataset.m; return `${CODE[m] ? CODE[m] + ' ' : ''}${nm ? nm.textContent.trim() : 'Sheet'}`; }
      const b = el.querySelector('.ldName b'), sw = el.dataset.m; return `${CODE[sw] ? CODE[sw] + ' ' : ''}${b ? b.textContent.trim() : 'Sheet'}`;
    }
    const st = el._laserSet; if (st && st.seq) return `Set ${st.seq}`;
    const b = el.querySelector('.ldName b, .sh .nm'); return b ? b.textContent.trim() : 'This set';
  }
  const records = () => (W.CN && W.CN.S && W.CN.S.library && W.CN.S.library.rows) || [];
  /** Where the item stands now: { area: 'progress' | 'laser' | 'completed' | null, setId }. */
  function placeOf(item) {
    const el = elOf(item); if (!el) return { area: null, setId: null };
    if (el.closest('#libDone')) {
      const sc = el.closest('.setCard');
      return { area: 'completed', setId: item.kind === 'sheet' ? (sc && sc._laserSet && sc._laserSet.setId) || null : null };
    }
    const sec = el.closest('[data-laser-area]');
    const area = sec ? (sec.dataset.laserArea === 'ready' ? 'laser' : 'progress') : null;
    let setId = null;
    if (item.kind === 'sheet') { const sc = el.closest('.setCard'); const r = records().find(x => x.id === item.id); setId = (sc && sc._laserSet && sc._laserSet.setId) || (r && !r.draft && r.solidIncluded !== false && r.setId) || null; }
    return { area, setId };
  }
  function knownSets() {
    const out = new Map();
    for (const c of doc.querySelectorAll('#libBody .setCard, #libDone .setCard')) { const st = c._laserSet; if (st && st.setId && !st.standalone && !st.working && !out.has(st.setId)) out.set(st.setId, { id: st.setId, seq: st.seq, day: st.day, sheets: (c._laserSheets || []).length }); }
    for (const r of records()) if (r.setId && !r.draft && r.solidIncluded !== false && !out.has(r.setId)) out.set(r.setId, { id: r.setId, seq: r.setSeq, day: r.day });
    return out;
  }
  const setName = k => k ? (k.seq ? `Set ${k.seq}` : k.name || 'Set') + (k.day ? ' · ' + dayShort(k.day) : '') : 'Set';
  async function targetsOf(item) {
    const f = flow(); if (!f) return [];
    const it = { kind: item.kind, id: item.id };
    // the real LibraryFlow says why a place is not allowed (explainTargets); a plain one only lists the allowed places
    if (typeof f.explainTargets === 'function') {
      try { const z = await Promise.resolve(f.explainTargets(it)); if (Array.isArray(z) && z.length) return z; }
      catch (e) { console.warn('LibraryFlow.explainTargets', e); }
    }
    try { const t = await Promise.resolve(f.targets(it)); return Array.isArray(t) ? t : t && Array.isArray(t.targets) ? t.targets : []; }
    catch (e) { console.warn('LibraryFlow.targets', e); return []; }
  }
  function defaultReason(item, spec, reasons) {
    const given = reasons && reasons[specKey(spec)]; if (given) return given;
    if (spec.set && item.kind === 'set') return 'A set cannot go inside another set';
    if (spec.newSet && item.kind === 'set') return 'A set already holds its sheets together';
    return `This ${item.kind} cannot go there`;
  }
  /** One zone for each place: { key, spec, name, sub, legal, reason }. The ones LibraryFlow does not list are dimmed. */
  function buildZones(item, targets) {
    const here = placeOf(item), legal = new Map(), reasons = targets && targets.reasons;
    for (const t of targets || []) if (t && typeof t === 'object') legal.set(specKey(t), t);
    const zones = [];
    const add = (spec, name, sub) => {
      const key = specKey(spec), t = legal.get(key);
      let ok = !!t && t.ok !== false && !t.illegal, reason = (t && t.reason) || '';
      if (spec.area && here.area === spec.area) { ok = false; reason = `Already in ${name}`; }
      if (spec.set && here.setId === spec.set) { ok = false; reason = 'Already in this set'; }
      if (!ok && !reason) reason = defaultReason(item, spec, reasons);
      zones.push({ key, spec, name: (t && t.label) || name, sub: (t && t.sub) || sub, legal: ok, reason });
    };
    for (const a of ['progress', 'laser', 'completed']) add({ area: a }, AREA[a].name, AREA[a].sub);
    add({ newSet: true }, 'New set', 'start a new set');
    const sets = knownSets();
    for (const t of legal.values()) if (t.set && !sets.has(t.set)) sets.set(t.set, { id: t.set, name: t.name });
    const order = [...sets.values()].sort((a, b) => String(b.day || '').localeCompare(String(a.day || '')) || (+b.seq || 0) - (+a.seq || 0)).slice(0, 80);
    for (const k of order) add({ set: k.id }, setName(k), k.sheets ? plural(k.sheets, 'sheet') : 'a set');
    return zones;
  }
  /** The element a place is drawn as on the page, when it is: a section, a set card. */
  function placeEl(spec) {
    if (spec.area === 'laser' || spec.area === 'progress') {
      const s = doc.querySelector(`#libBody [data-laser-area="${spec.area === 'laser' ? 'ready' : 'pending'}"]`);
      return s && !s.hidden && visible(s) ? s : null;
    }
    if (spec.set) { for (const c of doc.querySelectorAll('#libBody .setCard, #libDone .setCard')) if (c._laserSet && c._laserSet.setId === spec.set && visible(c)) return c; }
    return null;
  }
  function targetName(spec, plan) {
    if (spec.area) return AREA[spec.area].name;
    if (spec.newSet) return 'a new set';
    return setName(knownSets().get(spec.set) || (plan && plan.to && plan.to.setId === spec.set ? { id: spec.set } : null));
  }

  /* ═══ the lifted copy ═══ */
  /** A copy of the card that follows the hand: the real card cloned at its own size (preview, parts, seals, counters),
   *  cropped with a soft fade when it is larger than a hand can carry (a whole set, a wide row). */
  function makeLift(item, pt, opts = {}) {
    const node = elOf(item); if (!node) return null;
    const M = W.Motion, r = node.getBoundingClientRect();
    const maxW = item.kind === 'sheet' ? 340 : 520, maxH = item.kind === 'sheet' ? 480 : 300;
    const w = Math.max(40, Math.min(r.width, maxW)), h = Math.max(30, Math.min(r.height, maxH));
    const el = doc.createElement('div'); el.className = 'dndLift'; el.setAttribute('aria-hidden', 'true'); el.inert = true;
    Object.assign(el.style, { width: w + 'px', height: h + 'px' });
    const clip = doc.createElement('div'); clip.className = 'dndClip'; el.appendChild(clip);
    if (r.width > w + 2) el.classList.add('cropX'); if (r.height > h + 2) el.classList.add('cropY');
    let ghost = null;
    try { ghost = M && M.ghost ? M.ghost(node, r, null, node) : null; } catch (_) { ghost = null; }
    if (ghost) { Object.assign(ghost.style, { position: 'absolute', left: '0px', top: '0px' }); clip.appendChild(ghost); }
    else { const c = node.cloneNode(true); Object.assign(c.style, { position: 'absolute', left: '0', top: '0', width: r.width + 'px', height: r.height + 'px', margin: '0', pointerEvents: 'none' }); clip.appendChild(c); }
    const layer = M && M.layer ? M.layer(node) : (byId('motionLayer') || doc.body);
    layer.appendChild(el);
    const lift = { el, w, h, ox: opts.center ? w / 2 : pt ? Math.max(0, Math.min(w, pt.x - r.left)) : w / 2, oy: opts.center ? h / 2 : pt ? Math.max(0, Math.min(h, pt.y - r.top)) : h / 2, x: 0, y: 0, r: 0, rect: r };
    el.style.transformOrigin = `${lift.ox}px ${lift.oy}px`;
    lift.place = (x, y, rot) => { lift.x = x; lift.y = y; lift.r = rot || 0; el.style.transform = `translate3d(${x - lift.ox}px,${y - lift.oy}px,0) rotate(${lift.r}deg)`; };
    lift.remove = () => { try { el.remove(); } catch (_) { /* gone */ } };
    if (pt) lift.place(pt.x, pt.y, 0); else lift.place(r.left + lift.ox, r.top + lift.oy, 0);
    requestAnimationFrame(() => el.classList.add('on'));
    if (!reduced() && !opts.still) { try { (ghost || clip).animate([{ transform: 'scale(1)' }, { transform: 'scale(1.028)' }], { duration: 200, easing: EASE, fill: 'forwards' }); } catch (_) { /* no animation */ } }
    return lift;
  }
  const fadeOut = (el, ms) => { if (!el || !el.animate) { if (el) el.remove(); return Promise.resolve(); } return el.animate([{ opacity: 1 }, { opacity: 0 }], { duration: ms, easing: 'ease', fill: 'forwards' }).finished.then(() => el.remove(), () => el.remove()); };
  function ring(el) { if (!el || !el.isConnected || reduced()) return; el.classList.remove('dndLanded'); void el.offsetWidth; el.classList.add('dndLanded'); setTimeout(() => el.classList.remove('dndLanded'), 1500); }
  function pulse(el) { if (!el || !el.isConnected) return; el.removeAttribute('data-dnd-pulse'); void el.offsetWidth; el.setAttribute('data-dnd-pulse', ''); setTimeout(() => el.removeAttribute('data-dnd-pulse'), 1100); }
  /** Plain flight (the one used when window.LibraryFx is not on the page): the copy lifts, arcs over and shrinks into the place. */
  async function plainFly(lift, targetEl) {
    const el = lift.el; if (!el.isConnected) return;
    const tr = targetEl && targetEl.isConnected ? targetEl.getBoundingClientRect() : null;
    if (reduced() || !tr || !tr.width || !el.animate) { await fadeOut(el, reduced() ? 160 : 300); return; }
    const s = Math.max(.12, Math.min(.34, tr.height * 1.2 / lift.h, tr.width * 1.1 / lift.w));
    const Tx = tr.left + tr.width / 2, Ty = tr.top + tr.height / 2;
    const cur = { x: lift.x - lift.ox + lift.w / 2, y: lift.y - lift.oy + lift.h / 2 };            // the copy's centre now
    // the transform that puts the copy's centre at (cx, cy) at scale sc (the origin is the grip point)
    const tf = (cx, cy, sc, rot) => `translate3d(${cx - lift.w / 2 - (sc - 1) * (lift.w / 2 - lift.ox)}px,${cy - lift.h / 2 - (sc - 1) * (lift.h / 2 - lift.oy)}px,0) rotate(${rot}deg) scale(${sc})`;
    const dx = Tx - cur.x, dy = Ty - cur.y, bend = Math.min(110, 34 + Math.hypot(dx, dy) * .1);
    const ms = lift.h > 300 ? 820 : 720;
    const a = el.animate([
      { transform: tf(cur.x, cur.y, 1, lift.r), opacity: 1 },
      { transform: tf(cur.x + dx * .46, cur.y + dy * .46 - bend, (1 + s) / 2.1, 0), opacity: 1, offset: .46 },
      { transform: tf(cur.x + dx * .9, cur.y + dy * .9 - bend * .12, s * 1.25, 0), opacity: .88, offset: .86 },
      { transform: tf(Tx, Ty, s, 0), opacity: 0 }
    ], { duration: ms, easing: 'cubic-bezier(.5,.05,.3,1)', fill: 'forwards' });
    await a.finished.catch(() => {}); el.remove();
  }
  /** The copy goes to `targetEl`: window.LibraryFx.fly when it is there (and motion is allowed), a plain flight here when
   *  not. LibraryFx takes the copy over where it is (it is laid out by left and top for it), hides the card's old place
   *  while it is away and lands on the real card as soon as the page draws it. Never throws. */
  async function flyTo(m, targetEl) {
    const lift = m.lift; if (!lift) return;
    const Fx = W.LibraryFx, item = m.item, kind = item.kind;
    if (Fx && typeof Fx.fly === 'function' && !reduced() && lift.el.isConnected) {
      try {
        const home = elOf(item);
        const el = lift.el; el.classList.remove('on');
        Object.assign(el.style, { left: (lift.x - lift.ox) + 'px', top: (lift.y - lift.oy) + 'px', transform: 'none', transformOrigin: '50% 50%' });
        m.fx = true;
        const r = await Fx.fly(el, targetEl, { kind, duration: 720, home: home || undefined, onDone() { if (home) home.classList.remove('dndSource'); } });
        lift.remove(); if (r && r.ok === false) pulse(targetEl);
        // (the card is still where it was until the move is done: it shows again, a little faint, while the plan is read)
        const h = elOf(item); if (h && D.move === m && (m.state === 'planning' || m.state === 'review' || m.state === 'committing')) h.classList.add('dndSource');
        return;
      } catch (e) { m.fx = false; lift.el.style.visibility = ''; }
    }
    await plainFly(lift, targetEl); pulse(targetEl);
  }
  /** Back where the card came from: a copy rests at the place and flies home; the real card takes its colour back. */
  async function flyHome(item, targetEl, kind) {
    const home = elOf(item), done = () => { if (home) home.classList.remove('dndSource'); if (home) ring(home); };
    if (!home || reduced() || !targetEl || !visible(targetEl)) { done(); return; }
    const rest = makeLift(item, null, { center: true, still: true }); if (!rest) { done(); return; }
    const tr = targetEl.getBoundingClientRect(), hr = home.getBoundingClientRect();
    const Tx = tr.left + tr.width / 2, Ty = tr.top + tr.height / 2, Hx = hr.left + rest.w / 2, Hy = hr.top + rest.h / 2;
    const s = Math.max(.12, Math.min(.34, tr.height * 1.2 / rest.h, tr.width * 1.1 / rest.w));
    const tf = (cx, cy, sc) => `translate3d(${cx - rest.w / 2}px,${cy - rest.h / 2}px,0) scale(${sc})`;
    rest.el.style.transformOrigin = '50% 50%';
    const Fx = W.LibraryFx;
    rest.el.style.transform = tf(Tx, Ty, s);
    if (Fx && typeof Fx.flyBack === 'function') {
      try {
        // (LibraryFx makes its own copy of the card and sets it off from where the resting one is: this one goes at once)
        const p = Fx.flyBack(rest.el, home, { kind, duration: 760, onDone() { done(); } });
        rest.el.style.visibility = 'hidden';
        await p; rest.remove(); done(); return;
      } catch (e) { rest.el.style.visibility = ''; }
    }
    const dx = Hx - Tx, dy = Hy - Ty, bend = Math.min(90, 30 + Math.hypot(dx, dy) * .08);
    const a = rest.el.animate([
      { transform: tf(Tx, Ty, s), opacity: 0 },
      { transform: tf(Tx + dx * .2, Ty + dy * .2 - bend * .6, (s + 1) / 2.4), opacity: 1, offset: .22 },
      { transform: tf(Tx + dx * .72, Ty + dy * .72 - bend * .2, .95), opacity: 1, offset: .72 },
      { transform: tf(Hx, Hy, 1), opacity: 1 }
    ], { duration: 780, easing: 'cubic-bezier(.4,.05,.25,1)', fill: 'forwards' });
    await a.finished.catch(() => {}); rest.remove(); done();
  }
  /** A card let go over nothing, or over a place that is not allowed: it settles back where it was. */
  async function settleBack(item, lift) {
    D.settling++; sync();
    const home = elOf(item), finish = () => { if (lift) lift.remove(); if (home) { home.classList.remove('dndSource'); ring(home); } D.settling = Math.max(0, D.settling - 1); sync(); };
    if (!lift || !home || reduced() || !lift.el.animate) { finish(); return; }
    const hr = home.getBoundingClientRect(), tf = (x, y, rot) => `translate3d(${x - lift.ox}px,${y - lift.oy}px,0) rotate(${rot}deg) scale(1)`;
    const a = lift.el.animate([{ transform: tf(lift.x, lift.y, lift.r) }, { transform: tf(hr.left + lift.ox, hr.top + lift.oy, 0) }], { duration: 360, easing: 'cubic-bezier(.3,1.2,.4,1)', fill: 'forwards' });
    await a.finished.catch(() => {}); finish();
  }

  /* ═══ the dock ═══ */
  function openDock(zones, item, mode) {
    const st = stage(), sr = st.getBoundingClientRect();
    const el = doc.createElement('div'); el.className = 'dndDock' + (mode === 'bar' ? ' bar' : ''); el.setAttribute('role', 'group');
    el.setAttribute('aria-label', 'Places to move to');
    Object.assign(el.style, { left: sr.left + 10 + 'px', width: Math.max(220, st.clientWidth - 20) + 'px', top: sr.top + 6 + 'px' });
    el.innerHTML = `<div class="dndDockHead"><b></b><span></span></div><div class="dndRowWrap"><div class="dndRow"></div></div><div class="dndSlot"></div>`;
    el.querySelector('.dndDockHead b').textContent = mode === 'bar' ? '' : `Move ${labelOf(item)} to…`;
    el.querySelector('.dndDockHead span').textContent = mode === 'bar' ? '' : 'drop it on a place';
    const row = el.querySelector('.dndRow');
    let prev = '';
    for (const z of zones || []) {
      const grp = z.spec.area ? 'a' : z.spec.newSet ? 'n' : 's';
      if (prev && prev !== grp) row.appendChild(Object.assign(doc.createElement('i'), { className: 'dndSep' }));
      prev = grp;
      const b = doc.createElement('button'); b.type = 'button'; b.className = 'dndChip'; b.tabIndex = -1;
      b.dataset.key = z.key; b.dataset.state = z.legal ? 'armed' : 'dim'; if (!z.legal) b.setAttribute('aria-disabled', 'true');
      b.innerHTML = `<span class="dndChipName"></span><span class="dndChipSub"></span>`;
      b.querySelector('.dndChipName').textContent = z.name; b.querySelector('.dndChipSub').textContent = z.sub || '';
      b._z = z; z.chip = b; row.appendChild(b);
    }
    doc.body.appendChild(el);
    if (!reduced() && el.animate) el.animate([{ opacity: 0, transform: 'translateY(-14px) scale(.985)' }, { opacity: 1, transform: 'none' }], { duration: 300, easing: EASE });
    const dock = {
      el, row, slot: el.querySelector('.dndSlot'),
      toBar(text) { el.classList.add('bar'); el.querySelector('.dndDockHead b').textContent = text || ''; el.querySelector('.dndDockHead span').textContent = ''; },
      async close() { if (!el.isConnected) return; if (reduced() || !el.animate) return el.remove(); await el.animate([{ opacity: 1, transform: 'none' }, { opacity: 0, transform: 'translateY(-12px)' }], { duration: 240, easing: 'ease-in', fill: 'forwards' }).finished.catch(() => {}); el.remove(); }
    };
    return dock;
  }
  /** Light up where each place stands on the page (a section, a set card): its dashed frame and its name. */
  function markInPlace(zones, item) {
    for (const z of zones) {
      const e = placeEl(z.spec); if (!e) continue;
      if (z.spec.set && item.kind === 'set') continue;          // (a set dragged over the set cards: they are not places for it)
      z.place = e; e._z = z; e.setAttribute('data-dnd-state', z.legal ? 'armed' : 'dim'); e.setAttribute('data-dnd-label', z.legal ? `Move to ${z.name}` : z.reason);
    }
  }
  function clearMarks(zones) {
    for (const z of zones || []) {
      if (z.place) { z.place.removeAttribute('data-dnd-state'); z.place.removeAttribute('data-dnd-hot'); z.place.removeAttribute('data-dnd-label'); delete z.place._z; z.place = null; }
      if (z.chip) { z.chip.removeAttribute('data-hot'); }
    }
  }
  function zoneAt(x, y) {
    for (const e of doc.elementsFromPoint(x, y)) { const h = e.closest && e.closest('.dndChip, [data-dnd-state]'); if (h && h._z) { h._z.via = h.classList.contains('dndChip') ? 'chip' : 'place'; return h._z; } }
    return null;
  }
  function setHot(d, z) {
    const off = o => { if (!o) return; if (o.chip) { o.chip.removeAttribute('data-hot'); o.chip.querySelector('.dndChipSub').textContent = o.sub || ''; } if (o.place) { o.place.removeAttribute('data-dnd-hot'); o.place.setAttribute('data-dnd-label', o.legal ? `Move to ${o.name}` : o.reason); } };
    off(d.hot); d.hot = z;
    if (!z) return;
    if (z.chip) { z.chip.setAttribute('data-hot', ''); z.chip.querySelector('.dndChipSub').textContent = z.legal ? 'drop to move here' : z.reason; }
    if (z.place) { z.place.setAttribute('data-dnd-hot', ''); z.place.setAttribute('data-dnd-label', z.legal ? `Drop to move to ${z.name}` : z.reason); }
  }

  /* ═══ the "Moving" bar: where the plan is shown ═══ */
  /** `slot`: { mount(wrap), anchor }. Returns the bar: wait(text) · plan(plan, opts) → Promise · done(res) · fail(text) · note(text) · hold(ms) · remove(). */
  function mountMoving(slot, title) {
    const wrap = doc.createElement('div'); wrap.className = 'dndMovingWrap';
    wrap.innerHTML = `<div class="dndMoving" role="status" aria-live="polite"><div class="dndMovingIn"><div class="dndMovingHead"><span class="dndMovingTitle"></span><button type="button" class="dndX" aria-label="Dismiss" hidden>×</button></div><div class="dndWait" hidden><i class="dndSpin" aria-hidden="true"></i><span></span></div><div class="dndApprove"></div></div></div>`;
    const q = s => wrap.querySelector(s), titleEl = q('.dndMovingTitle'), waitEl = q('.dndWait'), ap = q('.dndApprove'), x = q('.dndX');
    titleEl.textContent = title || '';
    const bar = { wrap, ap, over: false, hover: false };
    const attach = () => { if (!wrap.isConnected) slot.mount(wrap); };
    attach();
    // the Library draws its lists anew after a move: the bar is put back in its place, as it was, until it is taken away
    const body = byId('libBody');
    if (body && W.MutationObserver) {
      let q = 0;
      const mo = new MutationObserver(() => { if (q || wrap.isConnected) return; q = requestAnimationFrame(() => { q = 0; if (!wrap.isConnected && !bar.gone) { try { slot.mount(wrap); } catch (_) { /* stays out */ } } }); });
      mo.observe(body, { childList: true, subtree: true });
      bar.unwatch = () => { mo.disconnect(); if (q) cancelAnimationFrame(q); };
    }
    requestAnimationFrame(() => wrap.classList.add('on'));
    if (reduced()) wrap.classList.add('on');
    // a hand that moves over the bar holds it open (to read it, to press in it); a pointer that simply stayed where the drop left it does not
    wrap.addEventListener('pointermove', e => { if (!D.pt || Math.hypot(e.clientX - D.pt.x, e.clientY - D.pt.y) > 10) bar.hover = true; });
    wrap.addEventListener('pointerleave', () => { bar.hover = false; });
    bar.title = t => { attach(); titleEl.textContent = t; };
    // LibraryApprovalUI draws its own card: this one is only the room it grows in
    bar.ui = on => { wrap.classList.toggle('ui', !!on); if (on) setTimeout(() => wrap.classList.add('settled'), 400); else wrap.classList.remove('settled'); };
    bar.wait = text => { attach(); waitEl.hidden = !text; waitEl.querySelector('span').textContent = text || ''; };
    bar.close = fn => { x.hidden = !fn; x.onclick = fn || null; };
    bar.clear = () => { ap.replaceChildren(); };
    // the plan as it is drawn here when window.LibraryApprovalUI is not on the page: green, red, amber
    bar.plain = (plan, o) => {
      attach(); ap.replaceChildren(); let i = 0;
      const line = (cls, icon, main, sub, extra) => { const d = doc.createElement('div'); d.className = 'dndLine ' + cls; d.style.setProperty('--i', i++); d.innerHTML = `${icon}<span class="dndTxt"><b></b>${sub ? '<small></small>' : ''}</span>`; d.querySelector('b').textContent = main; if (sub) d.querySelector('small').textContent = sub; if (extra) extra(d); ap.appendChild(d); return d; };
      const name = o.target || 'there';
      if (plan.needs.length) { const h = doc.createElement('div'); h.className = 'dndHead bad'; h.innerHTML = `Cannot move to <b></b> yet:`; h.querySelector('b').textContent = name; ap.appendChild(h); }
      for (const a of plan.auto) line('ok', SVG.ok, a.label || a.key, a.detail);
      for (const n of plan.needs) line('bad', SVG.bad, n.label || n.key, [n.detail, (n.items || []).map(it => it.label || it.id).filter(Boolean).join(' · ')].filter(Boolean).join(' · '));
      const pending = new Set(plan.confirm.map(c => c.key)), pressed = new Set();
      for (const c of plan.confirm) {
        const rose = c.key === 'roseLine';
        const d = line('warn', SVG.warn, rose ? 'This sheet has no green dash line yet.' : (c.label || c.key), rose ? `Moving it into ${name} needs one.${c.detail ? ' ' + c.detail : ''}` : c.detail, row => {
          const b = doc.createElement('button'); b.type = 'button'; b.className = 'dndBtn gold'; b.dataset.confirm = c.key; b.textContent = rose ? 'Add the green dash line' : (c.button || 'Yes, do this');
          b.onclick = () => { if (pressed.has(c.key)) return; pressed.add(c.key); b.disabled = true; row.classList.remove('warn'); row.classList.add('ok'); row.querySelector('svg').outerHTML = SVG.ok; b.remove(); if (pressed.size === pending.size) o.onConfirm && o.onConfirm([...pressed]); };
          row.appendChild(b);
        });
        void d;
      }
      for (const n of plan.notes || []) line('note', '<span style="flex:0 0 15px"></span>', String(n));
      if (plan.confirm.length) {
        const acts = doc.createElement('div'); acts.className = 'dndActs'; acts.style.setProperty('--i', i++);
        const no = doc.createElement('button'); no.type = 'button'; no.className = 'dndBtn'; no.dataset.cancel = ''; no.textContent = 'Not now'; no.onclick = () => o.onCancel && o.onCancel();
        acts.appendChild(no); ap.appendChild(acts);
      }
    };
    bar.result = (res, o) => {
      attach();
      if (res && res.ok) {
        if (o && o.plain) { const have = new Set([...ap.querySelectorAll('.dndLine.ok')].map(n => n.textContent)); for (const a of res.applied || []) { if (!a || !a.label || have.has(a.label)) continue; const d = doc.createElement('div'); d.className = 'dndLine ok'; d.style.setProperty('--i', 0); d.innerHTML = `${SVG.ok}<span class="dndTxt"><b></b></span>`; d.querySelector('b').textContent = a.label; ap.appendChild(d); } }
      } else {
        const d = doc.createElement('div'); d.className = 'dndLine bad'; d.style.setProperty('--i', 0); d.innerHTML = `${SVG.bad}<span class="dndTxt"><b></b><small></small></span>`;
        d.querySelector('b').textContent = 'The move did not go through'; d.querySelector('small').textContent = (res && res.error) || 'Nothing was changed.'; ap.appendChild(d);
      }
    };
    bar.refuse = reason => {
      attach(); ap.replaceChildren(); const d = doc.createElement('div'); d.className = 'dndLine bad'; d.style.setProperty('--i', 0);
      d.innerHTML = `${SVG.bad}<span class="dndTxt"><b></b><small>Nothing was changed.</small></span>`; d.querySelector('b').textContent = reason; ap.appendChild(d);
    };
    bar.note = text => { attach(); const d = doc.createElement('div'); d.className = 'dndLine note'; d.style.setProperty('--i', 0); d.innerHTML = `<span style="flex:0 0 15px"></span><span class="dndTxt"></span>`; d.querySelector('.dndTxt').textContent = text; ap.appendChild(d); };
    bar.remove = async (animate = true) => {
      bar.gone = true; if (bar.unwatch) bar.unwatch();
      if (!wrap.isConnected) return;
      if (animate && !reduced() && wrap.animate) { wrap.classList.remove('on'); await wait(340); }
      wrap.remove();
    };
    return bar;
  }
  /** Linger for `ms`, as long as nobody holds the bar under the pointer; `skip()` ends the wait at once. */
  function linger(bar, ms, m) {
    return new Promise(res => {
      let left = ms, last = Date.now(); m.skip = () => { left = 0; };
      const t = setInterval(() => { const now = Date.now(), dt = now - last; last = now; if (bar.hover && !(bar.wrap.isConnected && bar.wrap.matches(':hover'))) bar.hover = false; if (!bar.hover) left -= dt; if (left <= 0 || m.fast || !bar.wrap.isConnected) { clearInterval(t); m.skip = null; res(); } }, 100);
    });
  }

  /* ═══ the move: drop → plan → (confirm) → commit → lists ═══ */
  const normPlan = p => {
    p = p && typeof p === 'object' ? p : {};
    const arr = v => Array.isArray(v) ? v : [];
    return Object.assign({}, p, { ok: p.ok !== false, auto: arr(p.auto), needs: arr(p.needs), confirm: arr(p.confirm), notes: arr(p.notes) });
  };
  const timeout = (p, ms, what) => Promise.race([Promise.resolve(p), new Promise((_, rej) => setTimeout(() => rej(new Error(what + ' took too long')), ms))]);
  async function move(item, to, o = {}) {
    const f = flow(); if (!f || !item || !to) return null;
    if (D.move) { if (D.move.state !== 'review') return null; cancelMove(D.move, true); }
    const m = D.move = { id: ++D.seq, item, to: cleanSpec(to), state: 'planning', lift: o.lift || null, skip: null, resolve: null, fast: false };
    sync();
    try { return await run(f, m, o); }
    catch (e) { console.warn('Library move', e); return { ok: false, error: String(e && e.message || e) }; }
    finally { clearSources(); if (D.move === m) D.move = null; sync(); }
  }
  async function run(f, m, o) {
    const item = m.item, name = targetName(m.to), kind = item.kind, label = labelOf(item), zone = o.zone || null;
    let dk = o.dock || null;
    // where the plan is shown: on the place where it was dropped (a set card, a section), or in the dock
    const seen = e => { if (!visible(e)) return false; const r = e.getBoundingClientRect(), s = stage().getBoundingClientRect(); return r.bottom > s.top && r.top < s.bottom; };
    // (let go on a chip of the dock: the card, the plan and the bar all stay with the dock; let go on the place itself: they go there)
    const onChip = !!(zone && zone.via === 'chip' && zone.chip && zone.chip.isConnected);
    let place = onChip ? null : zone && zone.place && zone.place.isConnected ? zone.place : (!zone || !zone.chip) && (o.via !== 'menu' || seen(placeEl(m.to))) ? placeEl(m.to) : null;
    const chip = onChip ? zone.chip : null;
    const inDock = () => { if (!dk || !dk.el.isConnected) { dk = openDock([], item, 'bar'); dk.toBar(`Moving ${label} to ${name}`); } return dk; };
    if (!place && !dk) inDock();
    const first = chip || place || (dk && dk.el) || null;
    // (the copy leaves for the place before anything on the page changes shape: a chip it is dropped on still stands where it was)
    const flight = flyTo(m, first);
    if (zone) clearMarks(o.zones);
    if (place && dk) { dk.close(); dk = null; }
    const slot = { mount(wrap) {
      let p = place && place.isConnected ? place : null;
      if (!p && place) { p = placeEl(m.to); if (p) place = p; }          // (the Library drew its lists anew: the same place, as it is now)
      if (p && p.matches('[data-laser-area]')) { const h = p.querySelector(':scope > h2'); (h || p).after(wrap); return; }
      if (p && p.matches('.setCard')) { const h = p.querySelector(':scope > .sh'); if (h) { h.after(wrap); return; } p.prepend(wrap); return; }
      place = null; inDock().slot.appendChild(wrap);
    } };
    const title = `Moving ${label} to ${name}`;
    const bar = m.bar = mountMoving(slot, title);
    // the dock folds into the bar once the card has landed on its chip
    if (dk && !place) flight.then(() => { if (dk && dk.el.isConnected) dk.toBar(title); });
    // the answer to "may it go?" is a person's press: only a press ever resolves it with keys
    let answer; const asked = new Promise(res => { answer = res; }); m.resolve = answer;
    const UI = W.LibraryApprovalUI, ui = UI && typeof UI.show === 'function' && typeof UI.update === 'function' ? UI : null;
    let shown = false;
    const keysOf = keys => (Array.isArray(keys) ? { keys } : null);
    const cb = { onConfirm: keys => { answer(keysOf(keys)); }, onCancel: () => { answer(null); m.skip && m.skip(); } };
    const homeEl = () => bar.wrap.isConnected && visible(bar.wrap) ? bar.wrap : place && place.isConnected ? place : dk && dk.el.isConnected ? dk.el : null;
    const end = async (ms, noteText) => {
      bar.wait(null);
      if (noteText && !shown) bar.note(noteText);
      bar.close(() => { m.skip && m.skip(); });
      if (ms && !m.fast) await linger(bar, ms, m);
      if (ui && shown && typeof ui.hide === 'function') { try { ui.hide(bar.ap); } catch (_) { /* the bar is taken away below */ } }
      await bar.remove(!m.fast);
      if (dk) await dk.close();
    };
    const back = async (ms, noteText) => {
      m.state = 'back'; await flight;
      const h = flyHome(item, homeEl(), kind);
      if (!shown) { bar.clear(); bar.note(noteText || 'Nothing was changed.'); }
      await h; refocus(item, o); await end(ms);
    };
    // the plan (read only; nothing is written until it is committed)
    const planP = timeout(f.plan({ kind, id: item.id, to: m.to, by: who() }), 45000, 'Checking the move');
    const viewOf = raw => { const v = normPlan(raw); return m.to.set ? Object.assign({}, v, { to: Object.assign({}, v.to || {}, { label: name }) }) : v; };
    bar.wait('Checking the move…');
    let raw;
    try { raw = await planP; }
    catch (e) {
      if (!shown) { bar.wait(null); bar.result({ ok: false, error: `Could not check this move: ${e && e.message || e}` }); }
      await flight; m.state = 'back'; const h = flyHome(item, homeEl(), kind); await h; refocus(item, o); await end(5200, 'Nothing was changed.'); return { ok: false };
    }
    const plan = normPlan(raw);
    // the plan is shown: by LibraryApprovalUI (its own card stands in the room of this one), else by the plain bar below
    const present = () => {
      bar.clear(); bar.wait(null);
      if (ui) {
        bar.ui(true); if (dk && !place) dk.el.classList.add('ui');
        try { ui.show(bar.ap, viewOf(raw), { title, kind, onConfirm: cb.onConfirm, onCancel: cb.onCancel, onClose() { m.skip && m.skip(); } }); shown = true; return; }
        catch (e) { console.warn('LibraryApprovalUI.show', e); bar.ui(false); if (dk) dk.el.classList.remove('ui'); bar.clear(); shown = false; }
      }
      // (the plain bar: green, red, amber; Rose Gold's last check is LibraryFlowRose's own bar)
      bar.plain(plan, { target: name, onConfirm: cb.onConfirm, onCancel: cb.onCancel });
      const R = W.LibraryFlowRose, rose = plan.confirm.find(c => c.key === 'roseLine');
      if (rose && R && typeof R.confirmBar === 'function' && !plan.needs.length) {
        const row = bar.ap.querySelector('.dndLine.warn'), acts = bar.ap.querySelector('.dndActs');
        if (row && acts) { try { const hostEl = doc.createElement('div'); row.replaceWith(hostEl); acts.remove(); R.confirmBar(hostEl, { sheetLabel: label, item: { kind, id: item.id }, onConfirm: () => cb.onConfirm(['roseLine']), onCancel: cb.onCancel }); } catch (e) { console.warn('LibraryFlowRose.confirmBar', e); } }
      }
    };
    // 1. something is missing: it is told, the card flies back, nothing has changed
    if (plan.needs.length || plan.ok === false) {
      m.state = 'review';
      present();
      await flight;
      await linger(bar, Math.max(3400, 1400 + 1100 * (plan.needs.length + plan.auto.length)), m);
      await back(2400); return { ok: false, plan: raw };
    }
    // 2. something needs a person's own yes: the bar waits for it, and only a press passes a key on to commit
    let confirmed = [];
    if (plan.confirm.length) {
      m.state = 'review';
      present();
      const got = await asked;
      const allowed = new Set(plan.confirm.map(c => c.key));
      confirmed = got ? got.keys.filter(k => allowed.has(k)) : [];
      if (!got || !confirmed.length) { await back(1800); return { ok: false, plan: raw, cancelled: true }; }
    } else present();
    // 3. the write starts now (the flight is not waited for); the engine itself refuses what was not confirmed
    m.state = 'committing';
    if (!shown) bar.wait('Moving…');
    let res;
    try { res = await timeout(f.commit(raw, { confirmed, by: who() }), 120000, 'The move'); }
    catch (e) { res = { ok: false, error: e && e.message || String(e) }; }
    if (!res || typeof res !== 'object') res = { ok: false, error: 'No answer came back, nothing was changed.' };
    bar.wait(null);
    if (shown) { try { ui.update(bar.ap, res); } catch (e) { shown = false; bar.clear(); bar.result(res, { plain: true }); } }
    else bar.result(res, { plain: true });
    // with LibraryFx the real card is drawn at its new place while the copy is still in the air (it lands on it)
    let ready = res.ok && m.fx ? refresh(item, o) : null;
    await flight;
    if (!res.ok) { m.state = 'back'; const h = flyHome(item, homeEl(), kind); await h; refocus(item, o); await end(5200, 'Nothing was changed.'); return Object.assign({ plan: raw }, res); }
    if (!ready) ready = refresh(item, o);
    bar.title(`Moved ${label} to ${name}`);
    await end(4600);
    await ready;
    return Object.assign({ plan: raw }, res);
  }
  const clearSources = () => { for (const e of doc.querySelectorAll('.dndSource')) e.classList.remove('dndSource'); };
  function refocus(item, o) { if (!o || o.via !== 'menu') return; try { item.el = null; const e = elOf(item), g = e && e.querySelector('.dndGrip'); if (g) g.focus({ preventScroll: true }); } catch (_) { /* focus stays */ } }
  /** After a commit the lists are read again in place (LaserReview's cards, Completed, the counts); the moved card is lit where it now stands. */
  async function refresh(item, o) {
    const keys = [`${item.kind}:${item.id}`];
    try {
      const LD = W.LibraryDone, LR = W.LaserReview;
      if (LD && typeof LD.reload === 'function') await LD.reload(keys); else if (W.CN && W.CN.loadLibrary && (!LD || LD.tab() !== 'done')) await Promise.resolve(W.CN.loadLibrary()).catch(() => {});
      // (the live read at once and again a moment later: LaserReview.poll(force, now); never the slow check, whose seals the move itself recorded)
      if (LR) { LR.changed && LR.changed(); if (LR.nudge) LR.nudge(); else if (LR.poll) LR.poll(true, true); }
    } catch (e) { console.warn('Library refresh after a move', e); }
    clearSources();
    for (let i = 0; i < 24; i++) {
      item.el = null; const e = elOf(item);
      if (e && e.isConnected) { ring(e); refocus(item, o); try { const r = e.getBoundingClientRect(), s = stage().getBoundingClientRect(); if (r.bottom < s.top || r.top > s.bottom) e.scrollIntoView({ block: 'nearest', behavior: reduced() ? 'auto' : 'smooth' }); } catch (_) { /* stays */ } return; }
      await wait(150);
    }
  }
  function cancelMove(m, fast) { if (!m || m.state !== 'review') return; if (fast) m.fast = true; if (m.resolve) m.resolve(null); if (m.skip) m.skip(); }

  /* ═══ dragging ═══ */
  function clearPending() { const p = D.pending; if (!p) return; clearTimeout(p.timer); clearTimeout(p.arm); if (p.armEl) p.armEl.classList.remove('dndArming'); D.pending = null; }
  function onDown(e) {
    if (!e.isPrimary || (e.pointerType === 'mouse' && e.button !== 0)) return;
    clearPending();
    const t = e.target; if (!(t instanceof Element) || !flow()) return;
    const grip = t.closest('.dndGrip');
    if (!grip && t.closest(INTERACTIVE)) return;
    const item = itemAt(grip || t); if (!item) return;
    if (D.drag || (D.move && D.move.state !== 'review')) return;
    const p = D.pending = { item, pid: e.pointerId, type: e.pointerType, x0: e.clientX, y0: e.clientY, grip: !!grip, timer: 0, armEl: null };
    if (e.pointerType === 'touch' && !grip) {
      p.armEl = item.el;
      p.timer = setTimeout(() => { if (D.pending === p) { try { navigator.vibrate && navigator.vibrate(8); } catch (_) { /* quiet */ } begin(p, p.x, p.y); } }, 380);
      p.arm = setTimeout(() => { if (D.pending === p && p.armEl) p.armEl.classList.add('dndArming'); }, 120);
    }
    p.x = e.clientX; p.y = e.clientY;
  }
  function onMove(e) {
    const p = D.pending;
    if (p && e.pointerId === p.pid) {
      p.x = e.clientX; p.y = e.clientY;
      const dist = Math.hypot(e.clientX - p.x0, e.clientY - p.y0);
      if (p.type === 'touch' && !p.grip) { if (dist > 10) { clearTimeout(p.arm); clearPending(); } return; }
      if (dist > (p.type === 'touch' ? 4 : 6)) begin(p, e.clientX, e.clientY);
      return;
    }
    const d = D.drag; if (d && e.pointerId === d.pid) { d.x = e.clientX; d.y = e.clientY; if (e.cancelable && e.pointerType === 'touch') e.preventDefault(); }
  }
  function onUp(e) {
    const p = D.pending; if (p && e.pointerId === p.pid) { clearTimeout(p.arm); clearPending(); return; }
    const d = D.drag; if (d && e.pointerId === d.pid) { d.x = e.clientX; d.y = e.clientY; D.pt = { x: e.clientX, y: e.clientY }; drop(d); }
  }
  function onCancel(e) {
    const p = D.pending; if (p && e.pointerId === p.pid) { clearTimeout(p.arm); clearPending(); return; }
    const d = D.drag; if (d && e.pointerId === d.pid) abort(d);
  }
  function onKey(e) {
    if (e.key === 'Escape') { if (D.drag) { e.preventDefault(); e.stopPropagation(); abort(D.drag); return; } if (D.menu) { const g = D.menu.grip; closeMenu(); if (g && g.isConnected) g.focus(); return; } if (D.move && D.move.state === 'review') cancelMove(D.move); }
  }
  async function begin(p, x, y) {
    clearTimeout(p.arm); const item = p.item; clearPending();
    if (!elOf(item)) return;
    if (D.move && D.move.state === 'review') cancelMove(D.move, true);
    closeMenu();
    const d = D.drag = { item, x, y, px: x, py: y, tilt: 0, hot: null, zones: [], dock: null, lift: null, pid: p.pid, raf: 0, token: ++D.seq };
    html.classList.add('dndOn'); elOf(item).classList.add('dndSource'); sync();
    try { W.getSelection().removeAllRanges(); } catch (_) { /* none */ }
    d.lift = makeLift(item, { x, y });
    try { html.setPointerCapture(p.pid); } catch (_) { /* a stand-in pointer */ }
    d.raf = requestAnimationFrame(function frame() {
      if (D.drag !== d) return; d.raf = requestAnimationFrame(frame);
      const dx = d.x - d.px; d.px = d.x; d.py = d.y;
      d.tilt += ((reduced() ? 0 : Math.max(-5, Math.min(5, dx * .5))) - d.tilt) * .22;
      if (d.lift) d.lift.place(d.x, d.y, d.tilt);
      scrollNear(d);
      const z = zoneAt(d.x, d.y); if (z !== d.hot) setHot(d, z);
    });
    const ts = await targetsOf(item);
    if (D.drag !== d) return;
    d.zones = buildZones(item, ts); d.dock = openDock(d.zones, item); markInPlace(d.zones, item);
  }
  /** Near the stage's edges the page scrolls under the hand; near the dock's ends the dock does. */
  function scrollNear(d) {
    const st = stage(), r = st.getBoundingClientRect(), dr = d.dock && d.dock.el.getBoundingClientRect();
    if (dr && d.y >= dr.top && d.y <= dr.bottom) { const row = d.dock.row, rr = row.getBoundingClientRect(); if (d.x < rr.left + 36) row.scrollLeft -= 10; else if (d.x > rr.right - 36) row.scrollLeft += 10; return; }
    const top = dr ? dr.bottom : r.top;
    if (d.y > top && d.y < top + 52) st.scrollTop -= Math.ceil((top + 52 - d.y) / 3);
    else if (d.y > r.bottom - 66) st.scrollTop += Math.ceil((d.y - (r.bottom - 66)) / 3);
  }
  function stopDrag(d) {
    D.drag = null; cancelAnimationFrame(d.raf); html.classList.remove('dndOn'); D.noClick = performance.now() + 450; sync();
    try { html.releasePointerCapture(d.pid); } catch (_) { /* not held */ }
  }
  function abort(d) {
    stopDrag(d); setHot(d, null); clearMarks(d.zones);
    if (d.dock) d.dock.close();
    settleBack(d.item, d.lift);
  }
  async function drop(d) {
    const z = d.hot; stopDrag(d);
    if (z && z.legal) { setHot(d, null); return move(d.item, z.spec, { lift: d.lift, zone: z, dock: d.dock, zones: d.zones }); }
    if (z) {
      // not allowed: the reason is told on the place itself, and the card settles back
      setHot(d, null); const place = z.via !== 'chip' && z.place && z.place.isConnected ? z.place : null; clearMarks(d.zones);
      const dk = place ? null : d.dock; if (place && d.dock) d.dock.close();
      if (dk) dk.toBar(`${labelOf(d.item)} cannot go to ${z.name}`);
      const slot = { mount(wrap) { if (place && place.matches('[data-laser-area]')) { const h = place.querySelector(':scope > h2'); (h || place).after(wrap); } else if (place && place.matches('.setCard')) { const h = place.querySelector(':scope > .sh'); if (h) h.after(wrap); else place.prepend(wrap); } else if (dk) dk.slot.appendChild(wrap); } };
      const bar = mountMoving(slot, `${labelOf(d.item)} cannot go to ${z.name}`);
      bar.refuse(z.reason); bar.close(() => { bar.hover = false; m.fast = true; });
      const m = { fast: false }; settleBack(d.item, d.lift);
      await linger(bar, 3600, m); await bar.remove(); if (dk) await dk.close();
      return null;
    }
    setHot(d, null); clearMarks(d.zones); if (d.dock) d.dock.close(); settleBack(d.item, d.lift);
    return null;
  }

  /* ═══ the keyboard way: Move to… ═══ */
  function closeMenu() {
    const m = D.menu; if (!m) return; D.menu = null;
    m.grip.setAttribute('aria-expanded', 'false');
    const w = m.wrap; w.classList.remove('on');
    if (reduced()) w.remove(); else setTimeout(() => w.remove(), 280);
  }
  async function openMenu(item, grip) {
    if (!flow() || !elOf(item)) return;
    if (D.menu && D.menu.wrap.isConnected && D.menu.item.id === item.id && D.menu.item.kind === item.kind) { closeMenu(); return; }
    closeMenu();
    if (D.move && D.move.state !== 'review') return;
    grip = grip || (elOf(item) && elOf(item).querySelector('.dndGrip'));
    const el = elOf(item);
    const wrap = doc.createElement('div'); wrap.className = 'dndMenuWrap';
    wrap.innerHTML = `<div class="dndMenu" role="menu"><div class="dndMenuIn"><div class="dndMenuTitle"></div><div class="dndMenuWait"><i class="dndSpin" aria-hidden="true"></i><span>Checking where it can go…</span></div></div></div>`;
    wrap.querySelector('.dndMenuTitle').textContent = `Move ${labelOf(item)} to…`;
    wrap.addEventListener('click', e => e.stopPropagation());
    wrap.addEventListener('pointerdown', e => e.stopPropagation());
    const head = item.kind === 'sheet' && el.matches('.librarySheet') ? sheetCard(el).querySelector(':scope > .h') : item.kind === 'set' && el.matches('.setCard') ? el.querySelector(':scope > .sh') : el.closest('.ldItem') && el.closest('.ldItem').querySelector(':scope > .ldLine');
    if (!head) return;
    head.after(wrap);
    const menu = D.menu = { item, wrap, grip };
    if (grip) grip.setAttribute('aria-expanded', 'true');
    requestAnimationFrame(() => wrap.classList.add('on'));
    const ts = await targetsOf(item);
    if (D.menu !== menu) return;
    const zones = buildZones(item, ts), inn = wrap.querySelector('.dndMenuIn');
    inn.querySelector('.dndMenuWait').remove();
    for (const z of zones) {
      const b = doc.createElement('button'); b.type = 'button'; b.className = 'dndMenuItem'; b.setAttribute('role', 'menuitem'); b.dataset.key = z.key;
      b.innerHTML = '<span></span><small></small>'; b.firstChild.textContent = z.name;
      b.lastChild.textContent = z.legal ? z.sub || '' : z.reason;
      if (!z.legal) b.setAttribute('aria-disabled', 'true');
      b.onclick = e => { e.preventDefault(); e.stopPropagation(); if (!z.legal) return; closeMenu(); runMenuMove(item, z); };
      inn.appendChild(b);
    }
    const items = () => [...inn.querySelectorAll('.dndMenuItem')];
    inn.addEventListener('keydown', e => {
      const l = items(), i = l.indexOf(doc.activeElement);
      if (e.key === 'ArrowDown') { e.preventDefault(); l[(i + 1) % l.length].focus(); }
      else if (e.key === 'ArrowUp') { e.preventDefault(); l[(i - 1 + l.length) % l.length].focus(); }
      else if (e.key === 'Home') { e.preventDefault(); l[0].focus(); }
      else if (e.key === 'End') { e.preventDefault(); l[l.length - 1].focus(); }
      else if (e.key === 'Tab') { closeMenu(); }
    });
    const first = items().find(b => b.getAttribute('aria-disabled') !== 'true') || items()[0];
    if (first && (!doc.activeElement || doc.activeElement === grip || doc.activeElement === doc.body)) first.focus({ preventScroll: true });
  }
  async function runMenuMove(item, z) {
    D.pt = null;
    const lift = makeLift(item, null, { still: true }), home = elOf(item); if (home) home.classList.add('dndSource');
    const zoneEl = placeEl(z.spec); if (zoneEl) z.place = zoneEl;
    return move(item, z.spec, { lift, zone: z.place ? z : null, via: 'menu' });
  }

  /* ═══ the grip on each card ═══ */
  function grip(host, label) {
    let g = host.querySelector(':scope > .dndGrip');
    if (!g) {
      g = doc.createElement('button'); g.type = 'button'; g.className = 'dndGrip'; g.innerHTML = SVG.grip;
      g.setAttribute('aria-haspopup', 'menu'); g.setAttribute('aria-expanded', 'false');
      g.title = 'Drag to move · or press Enter for Move to…';
      g.addEventListener('click', e => { e.preventDefault(); e.stopPropagation(); const it = itemAt(g); if (it) openMenu(it, g); });
      host.appendChild(g);
    }
    g.setAttribute('aria-label', `Move ${label} to another place`);
    return g;
  }
  /** Gives each card a grip (the way in for the keyboard and for a finger). Safe to call as often as the lists are drawn. */
  function decorate(root) {
    if (!flow()) return;
    root = root && root.querySelectorAll ? root : doc;
    for (const c of root.querySelectorAll('.libCard[data-id]')) {
      if (!c.closest('#libBody, #libDone') || c.closest('.mGhost, .dndLift')) continue;
      const h = c.querySelector(':scope > .h'); if (!h) continue;
      const nm = h.querySelector('.nm'); grip(h, `${CODE[c.dataset.m] ? CODE[c.dataset.m] + ' ' : ''}${nm ? nm.textContent.trim() : 'sheet'}`);
    }
    for (const c of root.querySelectorAll('.setCard')) {
      if (!c.closest('#libBody, #libDone') || c.closest('.mGhost, .dndLift')) continue;
      const st = c._laserSet, h = c.querySelector(':scope > .sh'); if (!h) continue;
      const ok = st && st.setId && !st.standalone && !st.working, g = h.querySelector(':scope > .dndGrip');
      if (!ok) { if (g) g.remove(); continue; }
      grip(h, st.seq ? `Set ${st.seq}` : 'this set');
    }
    for (const row of root.querySelectorAll('#libDone .ldItem:not(.skel)')) {
      const line = row.querySelector(':scope > .ldLine'), nm = line && line.querySelector('.ldName'); if (!nm) continue;
      const b = nm.querySelector('b'); grip(nm, b ? b.textContent.trim() : row.dataset.kind === 'set' ? 'set' : 'sheet');
    }
  }

  /* ═══ start ═══ */
  function init() {
    style();
    doc.addEventListener('pointerdown', onDown, true);
    doc.addEventListener('pointermove', onMove, true);
    doc.addEventListener('pointerup', onUp, true);
    doc.addEventListener('pointercancel', onCancel, true);
    doc.addEventListener('keydown', onKey, true);
    // a click that ends a drag, or a press on the grip that was only a drag's start, opens nothing behind it
    doc.addEventListener('click', e => { if (performance.now() < D.noClick && e.target instanceof Element && e.target.closest('#libBody, #libDone') && !e.target.closest('.dndMovingWrap')) { e.stopPropagation(); e.preventDefault(); } }, true);
    doc.addEventListener('selectstart', e => { if (D.drag) e.preventDefault(); }, true);
    doc.addEventListener('dragstart', e => { if (D.pending || D.drag || (e.target instanceof Element && e.target.closest('#libBody, #libDone'))) e.preventDefault(); }, true);
    doc.addEventListener('contextmenu', e => { if (D.drag || (D.pending && D.pending.type === 'touch')) e.preventDefault(); }, true);
    doc.addEventListener('touchmove', e => { if (D.drag && e.cancelable) e.preventDefault(); }, { passive: false, capture: true });
    doc.addEventListener('pointerdown', e => { if (D.menu && e.target instanceof Element && !e.target.closest('.dndMenuWrap, .dndGrip')) closeMenu(); }, true);
    // the Completed list draws its rows as it is scrolled: each gets its grip
    const done = byId('libDone');
    if (done && W.MutationObserver) { let q = 0; new MutationObserver(() => { if (q) return; q = requestAnimationFrame(() => { q = 0; decorate(done); }); }).observe(done, { childList: true, subtree: true }); }
    decorate(doc);
  }
  W.LibraryDnd = {
    decorate, enabled: () => !!flow(), busy, move, openMenu: (item, g) => openMenu(item, g),
    cancel() { if (D.drag) abort(D.drag); if (D.move) cancelMove(D.move); closeMenu(); },
    state: () => ({ dragging: !!D.drag, move: D.move ? D.move.state : null, menu: !!D.menu })
  };
  if (doc.readyState === 'loading') doc.addEventListener('DOMContentLoaded', init); else init();
  // (LibraryFlow may be loaded after this file: the cards get their grips once it is there)
  W.addEventListener('load', () => decorate(doc));
})();
