/* The shared-orders window (Paul, 5 Oct 2026, point 6): the one pop-up he asked for.

   "If a user tries to drag a sheet out of a given set of sheets and by doing so would split a multi-piece order between two
   sheets, that sheet should not be allowed to be moved and the user should be clearly informed as to why, and which orders
   are preventing it, in a beautifully designed pop-up with thumbnails of those shared orders and quick links, so the user can
   click on any of those orders to see the detail order view and then always come back to the previous view to see the
   remaining mixed orders. A user should be able to remove the offending orders, and have that updated in real time
   everywhere, so that when the user comes back only the remaining offending orders are visible."

     SharedOrdersModal.open({ kind, id, orders, targetLabel, targetSetId, sheetLabel, setLabel, reason, from, onChange, onClose, onRetry })
        (also {sheetId} or {setId} in place of {kind, id}; `reason` is one sentence that replaces the usual one)
        -> { el, close(), update(orders), refresh(), isOpen(), orders() }   (null when there is nothing to show it with)
     SharedOrdersModal.close()   SharedOrdersModal.isOpen()   SharedOrdersModal.current()

   kind 'sheet' | 'set'; id the sheet's or the set's id. orders is the answer of SharedOrders.between(id, targetSetId) (what
   LibraryFlow.plan puts in its `sharedOrders` need): [{ orderId, label, customer, thumb, here, there:[..],
   pieces:[{ index, label, sheetId, sheetLabel, setId, thumb }] }]. The window asks SharedOrders.between again by itself, at
   once after every change it makes, when the page says something changed (SharedOrders.subscribe) and about every second
   from the page's own state (no network: the Library live read and the order window's 2.5 s feed keep that state fresh, so a
   change made on another computer drops its card within about three seconds). onChange(remaining, why) after each change,
   onClose({ cleared, retried, why }) when it is closed, onRetry() when the person presses "Move it now" on the empty state
   (the caller's own move is tried again: called once the window is gone, just before onClose).

   What it shows (minimal words, maximum pictures): a calm title ("These orders keep GF Sheet 1 in Set 2"), one short sentence,
   and one card per shared order: its pieces as small pictures, each with a chip of the sheet it sits on and "here" or "there",
   the order number and the customer in one line. A card is a link: it opens the order's detail view with the page's own
   hand-off (openOrderFrom: this window fades and settles back underneath, kept as it is: scroll, focus, the other cards),
   with a clear "Back" in the order's top bar (and Esc, and its own close) that brings this window back up, its list read
   again, a card that went meanwhile leaving in front of the person. A quiet "Take off the sheet..." on each card opens an
   inline choice on that card: Put on hold or Cancel the order, none chosen for the person, a name when none is signed in, and
   only the press of "Take off" does it (SharedOrders.removeFromSheet; never automatic). A small labelled spinner while it
   works; the card then leaves and the count follows. A piece that must stay (its sheet is cut, its set already sent) is said
   plainly on the card and the card stays. When nothing holds the sheet any more: a calm "Nothing holds ... any more", "Move
   it now" (onRetry) and Close.
   Keyboard: the focus stays in the window (it is a modal <dialog>), arrows and Home/End move between the cards, Enter opens
   one, Esc puts away an open choice first and then the window. Narrow screens and touch: one column, 40 px targets. Reduced
   motion: only short fades. It never writes anything itself and nothing here throws. */
(function () {
  'use strict';
  if (window.SharedOrdersModal) return;
  const W = window, doc = document;
  const EASE = 'cubic-bezier(.2,.8,.2,1)';
  const LIVE_EVERY = 1000, SLOW_EVERY = 3000, SLOW_AFTER = 20000, SETTLE_MAX = 8000, SHOWN_PIECES = 3;
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const warn = (what, e) => { try { console.warn('[shared orders] ' + what + ':', e && e.message || e); } catch (_) {} };
  const reduced = () => { try { return !!((W.Motion && Motion.reduced && Motion.reduced()) || matchMedia('(prefers-reduced-motion: reduce)').matches); } catch (_) { return false; } };
  const then = v => !!v && typeof v.then === 'function';
  const plural = (n, one, many) => `${n} ${n === 1 ? one : many || one + 's'}`;
  const arr = v => Array.isArray(v) ? v : [];
  const errText = e => { if (!e) return ''; if (typeof e === 'string') return e; if (e.message) return String(e.message); try { return JSON.stringify(e); } catch (_) { return String(e); } };
  const who = () => { try { return String(W.CNEmployee && CNEmployee.name && CNEmployee.name() || '').trim(); } catch (_) { return ''; } };
  const cssId = s => (W.CSS && CSS.escape ? CSS.escape(s) : String(s).replace(/[^\w-]/g, '\\$&'));

  /* ── looks ── */
  const STYLE = `
dialog.soDlg{width:min(640px,94vw);max-height:min(88vh,860px);border:0;padding:0;border-radius:15px;background:var(--card,#fffefb);color:var(--ink,#1c1a17);box-shadow:0 24px 80px rgba(0,0,0,.32);overflow:hidden;font:14px/1.45 var(--sans,-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,system-ui,sans-serif);-webkit-font-smoothing:antialiased}
dialog.soDlg::backdrop{background:rgba(20,18,15,.52)}
dialog.soDlg[open]{display:block}
.soBox{display:flex;flex-direction:column;max-height:min(88vh,860px);min-height:0}
.soHead{position:relative;display:grid;grid-template-columns:minmax(0,1fr) auto;column-gap:12px;align-items:start;padding:15px 14px 13px 18px;border-bottom:1px solid var(--line,#e4ddd0);flex:0 0 auto;background:var(--card,#fffefb)}
.soTitles{min-width:0}
.soTitle{margin:0;font:500 20px/1.25 var(--serif,'Hoefler Text','Iowan Old Style',Georgia,serif);letter-spacing:.005em;text-wrap:balance;overflow-wrap:anywhere}
.soTitle b{font-weight:600;white-space:nowrap}
.soSub{margin:4px 0 0;color:var(--ink70,#5b554c);font-size:12.5px;line-height:1.45;max-width:56ch;text-wrap:pretty}
.soTitle,.soSub,.soSheets{transition:opacity .2s ease,transform .2s ${EASE}}
.soTitles.swap .soTitle,.soTitles.swap .soSub,.soTitles.swap .soSheets{opacity:0;transform:translateY(3px)}
.soSheets{display:flex;flex-wrap:wrap;align-items:center;gap:6px;margin-top:10px}
.soSheets:empty{display:none}
.soSheets .soChip.here{background:var(--goldSoft,#f0e6cd);color:#5c4210}
.soHeadRight{display:flex;align-items:center;gap:6px;flex:0 0 auto}
.soCount{display:inline-flex;align-items:center;gap:6px;height:24px;padding:0 10px;border-radius:999px;background:var(--paper2,#ebe5d9);color:var(--ink70,#5b554c);font:700 11px/1 var(--sans,system-ui,sans-serif);white-space:nowrap;font-variant-numeric:tabular-nums;transition:background-color .4s ease,color .4s ease,transform .3s cubic-bezier(.3,1.7,.5,1)}
.soCount:empty{display:none}
.soCount.tick{transform:scale(1.1)}
.soDlg[data-state=clear] .soCount{background:var(--sageSoft,#e7eddf);color:#3c5a39}
.soX{width:32px;height:32px;border:1px solid transparent;border-radius:9px;background:transparent;color:var(--ink45,#938c80);display:grid;place-items:center;padding:0;cursor:pointer;transition:background-color .15s ease,color .15s ease}
.soX svg{width:16px;height:16px;display:block}
.soX:hover{background:var(--paper2,#ebe5d9);color:var(--ink,#1c1a17)}
.soX:focus-visible{outline:2px solid var(--gold,#a9823f);outline-offset:2px}
.soBody{position:relative;overflow:auto;overscroll-behavior:contain;padding:16px 18px 20px;background:var(--card2,#faf7f1);min-height:150px;scroll-padding:16px}
dialog.soDlg:focus,dialog.soDlg:focus-visible{outline:none}
.soGrid{list-style:none;margin:0;padding:0;display:grid;grid-template-columns:repeat(auto-fill,minmax(244px,1fr));gap:12px;align-items:start}
.soCard{position:relative;display:flex;flex-direction:column;min-width:0;background:var(--card,#fffefb);border:1px solid var(--line,#e4ddd0);border-radius:12px;box-shadow:0 1px 2px rgba(30,26,20,.04),0 9px 28px rgba(30,26,20,.06);transition:border-color .16s ease,box-shadow .2s ease,transform .2s ${EASE},opacity .22s ease;overflow:hidden}
.soCard:hover,.soCard:has(:focus-visible){border-color:var(--goldLine,#e3d3a6);box-shadow:0 8px 22px rgba(20,16,10,.14);transform:translateY(-1px)}
.soCard.leaving{pointer-events:none;opacity:0;transform:scale(.95)}
.soCard.arriving{animation:soIn .34s ${EASE} both}
.soSlot{display:grid;grid-template-rows:1fr;transition:grid-template-rows .26s ${EASE},opacity .2s ease,visibility 0s}
.soClip{min-height:0;overflow:hidden}
.soAskSlot{grid-template-rows:0fr;opacity:0;visibility:hidden;transition:grid-template-rows .26s ${EASE},opacity .2s ease,visibility 0s .26s}
.soAskSlot.open{grid-template-rows:1fr;opacity:1;visibility:visible;transition:grid-template-rows .26s ${EASE},opacity .2s ease .06s,visibility 0s}
.soCard:has(>.soAskSlot.open)>.soFootSlot{grid-template-rows:0fr;opacity:0;visibility:hidden;transition:grid-template-rows .26s ${EASE},opacity .15s ease,visibility 0s .26s}
.soOpen{display:flex;flex:0 0 auto;flex-direction:column;gap:10px;width:100%;margin:0;padding:12px 12px 4px;border:0;background:transparent;color:inherit;text-align:left;cursor:pointer;font:inherit;border-radius:12px 12px 0 0;position:relative;min-width:0}
.soOpen:focus-visible{outline:2px solid var(--gold2,#caa861);outline-offset:-2px}
@supports selector(:has(*)){
  .soOpen:focus-visible{outline:none}
  .soCard:has(.soOpen:focus-visible){outline:2px solid var(--gold,#a9823f);outline-offset:2px}
}
.soOpen:hover .soGo,.soOpen:focus-visible .soGo{opacity:1;transform:none}
.soGo{margin-left:auto;flex:0 0 auto;align-self:center;width:22px;height:22px;display:grid;place-items:center;border-radius:50%;background:var(--paper2,#ebe5d9);color:var(--ink70,#5b554c);opacity:0;transform:translateX(-3px);transition:opacity .16s ease,transform .2s ${EASE}}
.soGo svg{width:12px;height:12px;display:block}
@media (hover:none){.soGo{opacity:.8;transform:none}}
.soPieces{--pg:12px;display:flex;align-items:flex-start;justify-content:center;gap:var(--pg);min-width:0;padding-top:2px}
.soPiece{flex:1 1 0;min-width:0;max-width:84px;display:flex;flex-direction:column;align-items:center;gap:6px}
.soTileWrap{position:relative;width:100%}
.soTile{position:relative;width:100%;aspect-ratio:1;border-radius:10px;background:var(--card2,#faf7f1);border:1px solid var(--line,#e4ddd0);overflow:hidden;display:grid;place-items:center;box-shadow:inset 0 0 0 1px rgba(255,255,255,.6)}
.soPiece.here .soTile{border-color:var(--gold2,#caa861);box-shadow:0 0 0 3px var(--goldSoft,#f0e6cd)}
.soTile img{width:100%;height:100%;object-fit:contain;display:block;padding:6%;box-sizing:border-box;opacity:0;transition:opacity .3s ease}
.soTile img.in{opacity:1}
.soTile .soPh{position:absolute;inset:0;display:grid;place-items:center;color:var(--ink25,#c4bdb0);background:linear-gradient(135deg,var(--card2,#faf7f1),var(--paper2,#ebe5d9))}
.soTile .soPh svg{width:46%;height:46%;display:block}
.soTile img.in~.soPh{opacity:0;transition:opacity .3s ease}
.soNum{position:absolute;left:4px;top:4px;min-width:16px;height:16px;padding:0 4px;border-radius:8px;background:rgba(255,254,251,.92);color:var(--ink70,#5b554c);font:700 10px/14px var(--sans,system-ui,sans-serif);text-align:center;border:1px solid var(--line,#e4ddd0);box-sizing:border-box}
.soMore .soTile{background:var(--paper2,#ebe5d9);border-style:dashed;color:var(--ink70,#5b554c);font:650 15px var(--sans,system-ui,sans-serif)}
.soChip{display:inline-flex;align-items:center;gap:5px;max-width:100%;height:20px;padding:0 8px 0 6px;border-radius:999px;background:var(--paper2,#ebe5d9);color:var(--ink,#1c1a17);font:650 10.5px/1 var(--sans,system-ui,sans-serif);white-space:nowrap;box-sizing:border-box}
.soChip i{width:7px;height:7px;border-radius:50%;background:var(--dot,var(--ink25,#c4bdb0));flex:0 0 auto;box-shadow:0 0 0 1px rgba(30,26,20,.12)}
.soChip span{overflow:hidden;text-overflow:ellipsis}
.soPiece.here .soChip{background:var(--goldSoft,#f0e6cd);color:#5c4210}
.soWhere{font:650 10px/1 var(--sans,system-ui,sans-serif);letter-spacing:.1em;text-transform:uppercase;color:var(--ink70,#5b554c);margin-top:-2px}
.soPiece.here .soWhere{color:#7a5a1d}
.soJoin{position:absolute;left:calc(-13px - var(--pg,12px)/2);top:50%;margin-top:-12px;width:26px;height:24px;display:grid;place-items:center;z-index:1;color:var(--gold,#a9823f)}
.soJoin svg{width:13px;height:13px;display:block;padding:4px;box-sizing:content-box;border-radius:50%;background:var(--card,#fffefb);border:1px solid var(--goldLine,#e3d3a6);box-shadow:0 1px 3px rgba(30,26,20,.08)}
.soMeta{display:flex;align-items:center;gap:8px;min-width:0;padding:0 2px}
.soOrderNo{font:650 12.5px var(--mono,ui-monospace,Menlo,Consolas,monospace);letter-spacing:-.01em;color:var(--ink,#1c1a17);flex:0 0 auto;font-variant-numeric:tabular-nums}
.soWho{min-width:0;color:var(--ink70,#5b554c);font-size:11.5px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.soLocked{display:flex;align-items:flex-start;gap:6px;margin:0;padding:0 14px 4px;color:var(--ink70,#5b554c);font-size:11.5px;line-height:1.4;text-wrap:pretty}
.soLocked svg{width:12px;height:12px;flex:0 0 auto;margin-top:2px;color:var(--ink45,#938c80)}
.soLocked b{font-weight:650;color:var(--ink,#1c1a17)}
.soFoot{display:flex;align-items:center;justify-content:flex-end;padding:4px 10px 10px;min-height:34px}
.soOff{display:inline-flex;align-items:center;gap:6px;min-height:28px;padding:4px 9px;border:1px solid var(--line,#e4ddd0);border-radius:9px;background:transparent;color:var(--ink70,#5b554c);font:650 11px/1.2 var(--sans,system-ui,sans-serif);cursor:pointer;transition:background-color .15s ease,color .15s ease,border-color .15s ease}
.soOff svg{width:13px;height:13px;display:block;flex:0 0 auto}
.soOff:hover{background:var(--card2,#faf7f1);color:var(--ink,#1c1a17);border-color:var(--ink25,#c4bdb0)}
.soOff:focus-visible{outline:2px solid var(--gold,#a9823f);outline-offset:2px}
.soOff[aria-disabled=true]{opacity:.45;cursor:default;pointer-events:none}
.soAsk{display:grid;gap:9px;padding:12px 14px 14px;border-top:1px solid var(--line,#e4ddd0);background:var(--card2,#faf7f1);min-width:0}
.soNote.enter{animation:soIn .26s ${EASE} both}
.soAskQ{margin:0;font:650 13px/1.3 var(--sans,system-ui,sans-serif);text-wrap:balance}
.soOpts{display:flex;flex-wrap:wrap;gap:6px}
.soOpt{position:relative;display:inline-flex}
.soOpt input{position:absolute;inset:0;opacity:0;margin:0;cursor:pointer}
.soOpt span{display:inline-flex;align-items:center;gap:6px;min-height:30px;padding:4px 12px;border:1px solid var(--line,#e4ddd0);border-radius:999px;background:var(--card,#fffefb);font:650 12px/1.2 var(--sans,system-ui,sans-serif);color:var(--ink,#1c1a17);transition:background-color .15s ease,border-color .15s ease,color .15s ease}
.soOpt:hover span{border-color:var(--ink25,#c4bdb0)}
.soOpt input:checked+span{background:var(--goldSoft,#f0e6cd);border-color:var(--ink,#1c1a17);color:var(--ink,#1c1a17)}
.soOpt input[value=cancel]:checked+span{background:var(--claySoft,#f4e3dc);border-color:var(--clay,#b0563f);color:#8a3a26}
.soOpt input:focus-visible+span{outline:2px solid var(--gold2,#caa861);outline-offset:2px}
.soAskText{margin:0;color:var(--ink70,#5b554c);font-size:12px;line-height:1.4;min-height:17px;text-wrap:pretty}
.soName{border:1px solid var(--line,#e4ddd0);border-radius:9px;padding:7px 10px;background:var(--card,#fffefb);font:13px var(--sans,system-ui,sans-serif);min-width:0;width:100%;box-sizing:border-box}
.soName:focus-visible{outline:2px solid var(--gold2,#caa861);outline-offset:1px}
.soBtns{display:flex;flex-wrap:wrap;gap:7px;align-items:center}
.soAsk .soBtns{justify-content:flex-end}
.soBtn{display:inline-flex;align-items:center;justify-content:center;gap:7px;min-height:32px;padding:5px 13px;border:1px solid var(--line,#e4ddd0);border-radius:9px;background:var(--card,#fffefb);color:var(--ink,#1c1a17);font:650 12.5px/1.2 var(--sans,system-ui,sans-serif);cursor:pointer;white-space:nowrap;transition:background-color .15s ease,opacity .15s ease,transform .08s ease,border-color .15s ease}
.soBtn:hover{background:var(--card2,#faf7f1);border-color:var(--ink25,#c4bdb0)}
.soBtn:active{transform:translateY(1px)}
.soBtn:focus-visible{outline:2px solid var(--gold2,#caa861);outline-offset:2px}
.soBtn.go{background:var(--ink,#1c1a17);border-color:var(--ink,#1c1a17);color:#fff}
.soBtn.go:hover{opacity:.9;background:var(--ink,#1c1a17)}
.soBtn.go.danger{background:var(--clay,#b0563f);border-color:var(--clay,#b0563f)}
.soBtn.gold{background:var(--gold,#a9823f);border-color:var(--gold,#a9823f);color:#2a2013}
.soBtn.gold:hover{background:var(--gold,#a9823f);opacity:.9}
.soBtn:disabled{opacity:.42;cursor:default;transform:none}
.soBusy:focus{outline:none}
.soBusy{position:absolute;inset:0;z-index:2;display:flex;align-items:center;justify-content:center;gap:9px;padding:14px;text-align:center;background:color-mix(in srgb,var(--card,#fffefb) 88%,transparent);-webkit-backdrop-filter:blur(2px);backdrop-filter:blur(2px);color:var(--ink70,#5b554c);font:600 12.5px/1.35 var(--sans,system-ui,sans-serif);border-radius:inherit;animation:soFade .2s ease both}
.soSpin{width:14px;height:14px;flex:none;box-sizing:border-box;border:2px solid var(--line,#e4ddd0);border-top-color:var(--ink70,#5b554c);border-radius:50%;animation:soSpin .7s linear infinite}
.soNote{margin:0;padding:10px 14px 12px;border-top:1px solid var(--line,#e4ddd0);background:var(--goldSoft,#f0e6cd);color:#5c4210;font-size:12px;line-height:1.4}
.soNote.bad{background:var(--claySoft,#f4e3dc);color:#7a3321}
.soNote p{margin:0;text-wrap:pretty}
.soNote .soBtns{margin-top:8px}
.soNote .soBtn{min-height:28px;padding:3px 11px;font-size:12px}
.soClear{display:grid;justify-items:center;text-align:center;gap:14px;padding:30px 12px 20px;animation:soIn .4s ${EASE} both}
.soRing{width:84px;height:84px;display:block;overflow:visible}
.soRing circle{fill:var(--sageSoft,#e7eddf);stroke:var(--sage,#5f7a5b);stroke-width:1.4;transform-box:fill-box;transform-origin:center;animation:soPop .5s cubic-bezier(.3,1.4,.5,1) both}
.soRing path{fill:none;stroke:#3c5a39;stroke-width:2.4;stroke-linecap:round;stroke-linejoin:round;stroke-dasharray:30;stroke-dashoffset:30;animation:soDraw .5s .25s ease-out forwards}
.soClearT{margin:0;font:500 19px/1.25 var(--serif,Georgia,serif);text-wrap:balance}
.soClear .soBtns{justify-content:center;margin-top:4px}
.soClear .soBtn{min-height:38px;padding:7px 18px;font-size:13px}
.soLive{position:absolute;width:1px;height:1px;margin:-1px;padding:0;overflow:hidden;clip:rect(0 0 0 0);white-space:nowrap;border:0}
.soSkel{border:1px solid var(--line,#e4ddd0);border-radius:12px;background:var(--card,#fffefb);padding:12px;display:grid;gap:10px;min-height:150px}
.soSkel i{display:block;border-radius:10px;background:linear-gradient(90deg,var(--paper2,#ebe5d9) 25%,var(--card2,#faf7f1) 50%,var(--paper2,#ebe5d9) 75%);background-size:200% 100%;animation:soShimmer 1.3s ease-in-out infinite}
.soSkel .a{height:64px;width:64px;justify-self:center}
.soSkel .b{height:12px;width:70%}
.soSkel .c{height:10px;width:46%}
.soWait{display:flex;align-items:center;gap:8px;margin:0 0 12px;color:var(--ink70,#5b554c);font:600 12px/1.3 var(--sans,system-ui,sans-serif)}
.soErr{display:flex;flex-wrap:wrap;align-items:center;justify-content:center;gap:10px;padding:34px 12px;color:#7a3321;font:600 12.5px/1.4 var(--sans,system-ui,sans-serif);text-align:center;animation:soIn .3s ${EASE} both}
.soErr i{width:8px;height:8px;border-radius:50%;background:var(--clay,#b0563f);flex:0 0 auto}
#orderWin .soBack{display:inline-flex;align-items:center;gap:6px;flex:0 0 auto;height:32px;padding:0 12px 0 8px;margin:0;border:1px solid var(--line,#e4ddd0);border-radius:9px;background:var(--card,#fffefb);color:var(--ink,#1c1a17);font:650 12.5px/1 var(--sans,system-ui,sans-serif);cursor:pointer;white-space:nowrap;animation:soFade .24s ease both;transition:background-color .15s ease,border-color .15s ease,transform .08s ease}
#orderWin .soBack:hover{background:var(--card2,#faf7f1);border-color:var(--goldLine,#e3d3a6)}
#orderWin .soBack:active{transform:translateY(1px)}
#orderWin .soBack:focus-visible{outline:2px solid var(--gold,#a9823f);outline-offset:2px}
#orderWin .soBack svg{width:14px;height:14px;display:block;color:var(--ink45,#938c80)}
@keyframes soIn{from{opacity:0;transform:translateY(8px)}}
@keyframes soFade{from{opacity:0}}
@keyframes soSpin{to{transform:rotate(360deg)}}
@keyframes soPop{0%{transform:scale(.6)}60%{transform:scale(1.08)}100%{transform:scale(1)}}
@keyframes soDraw{to{stroke-dashoffset:0}}
@keyframes soShimmer{from{background-position:200% 0}to{background-position:-200% 0}}
@media (max-width:620px){
  dialog.soDlg{width:96vw;max-height:calc(100dvh - 16px)}
  .soBox{max-height:calc(100dvh - 16px)}
  .soHead{padding:14px 8px 12px 14px}
  .soTitle{font-size:19px}
  .soBody{padding:12px 12px 16px}
  .soGrid{grid-template-columns:minmax(0,1fr);gap:10px}
  /* a compact row, not a tall card: small pictures with their chips on the left, the order and customer beside them */
  .soOpen{flex-direction:row;align-items:center;gap:12px;padding:10px 10px 8px 12px}
  .soPieces{--pg:10px;flex:0 0 auto;justify-content:flex-start;padding-top:0}
  .soPiece{flex:0 0 56px;width:56px;max-width:56px}
  .soMeta{flex:1 1 auto;display:grid;grid-template-columns:minmax(0,1fr) auto;grid-template-areas:"no go" "who go";align-content:center;align-items:center;column-gap:8px;row-gap:3px;padding:0}
  .soOrderNo{grid-area:no;white-space:nowrap}
  .soWho{grid-area:who}
  .soGo{grid-area:go;opacity:.8;transform:none;margin:0}
  .soFoot{padding:4px 10px 8px}
  .soAsk{padding:11px 12px 12px}
  .soBtns .soBtn{flex:1 1 auto}
  .soOff,.soBtn,.soOpt span{min-height:36px}
  .soX{width:36px;height:36px}
  #orderWin .soBack{height:36px}
}
@media (max-width:480px){
  .soSheets{display:none}   /* (the cards say it: each piece carries its sheet; the room goes to the orders) */
  .soHead{padding-bottom:11px}
}
@media (max-width:400px){
  .soPieces{--pg:8px}
  .soPiece{flex-basis:48px;width:48px;max-width:48px}
  .soPiece .soChip{padding:0 5px 0 4px;gap:4px;max-width:calc(100% + 10px)}
  .soPiece .soChip i{width:6px;height:6px}
  .soOpen{gap:10px}
}
@media (pointer:coarse){.soOff,.soBtn,.soOpt span{min-height:36px}.soX{width:36px;height:36px}#orderWin .soBack{height:36px}}
@media (prefers-reduced-motion:reduce){
  .soCard,.soCard.arriving,.soSlot,.soAskSlot,.soAskSlot.open,.soCard:has(>.soAskSlot.open)>.soFootSlot,.soNote.enter,.soClear,.soBusy,.soErr,.soSkel i,#orderWin .soBack{animation:none!important;transition:none!important}
  .soRing circle,.soRing path{animation:none!important;stroke-dashoffset:0}
  .soTitle,.soSub,.soCount{transition:none!important}
  .soSpin{animation-duration:1.6s}
  .soCard.leaving{transform:none}
}
`;
  function css() {
    if (doc.getElementById('soCss')) return;
    const s = doc.createElement('style'); s.id = 'soCss'; s.textContent = STYLE; (doc.head || doc.documentElement).appendChild(s);
  }
  const ICON = {
    link: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false"><path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/></svg>',
    close: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true" focusable="false"><path d="M6 6l12 12M18 6L6 18"/></svg>',
    go: '<svg viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false"><path d="M4 2.5L7.5 6 4 9.5"/></svg>',
    off: '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false"><circle cx="8" cy="8" r="6.2"/><path d="M5.2 8h5.6"/></svg>',
    back: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false"><path d="M15 6l-6 6 6 6"/></svg>',
    lock: '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false"><rect x="3.4" y="7.2" width="9.2" height="6.4" rx="1.6"/><path d="M5.4 7.2V5.4a2.6 2.6 0 0 1 5.2 0v1.8"/></svg>',
    charm: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false"><circle cx="12" cy="6.2" r="2.2"/><path d="M12 8.4v2.1"/><path d="M12 10.5c3.6 0 6 2.4 6 5.2 0 2.7-2.4 4.5-6 4.5s-6-1.8-6-4.5c0-2.8 2.4-5.2 6-5.2z"/></svg>',
    ring: '<svg class="soRing" viewBox="0 0 84 84" aria-hidden="true" focusable="false"><circle cx="42" cy="42" r="38"/><path d="M27 43.5l10.5 10.5L58 31"/></svg>'
  };

  /* ── what the orders say (the shapes of SharedOrders.between, tolerant of what is not there) ── */
  const METAL = { GF: 'gold', SS: 'silver', RG: 'rose', '10K': 'gold10k', '14K': 'gold14k' };
  const dotOf = label => { const m = /^\s*(GF|SS|RG|10K|14K)\b/i.exec(String(label || '')); return m ? `var(--m-${METAL[m[1].toUpperCase()]})` : 'var(--ink25)'; };
  const digits = v => String(v == null ? '' : v).replace(/\D/g, '');
  /** A picture the page already holds for the order (never fetched here: no Etsy call, no new read). */
  function rowImage(orderId) {
    try {
      const O = W.Orders; if (!O || typeof O.rows !== 'function' || typeof O.imageFor !== 'function') return null;
      for (const r of O.rows()) if (r && r.order && String(r.order.receiptId) === String(orderId)) { const u = O.imageFor(r); if (u) return u; }
    } catch (_) { /* no picture */ }
    return null;
  }
  function normalize(list, ctx) {
    const here = [].concat(ctx.sheetLabel || []).filter(Boolean), out = [];
    for (const o of arr(list)) {
      if (!o || typeof o !== 'object') continue;
      const orderId = String(o.orderId != null ? o.orderId : o.receiptId != null ? o.receiptId : o.id != null ? o.id : '');
      if (!orderId) continue;
      // (the engine says `here` as "GF Sheet 1 + GF Sheet 2" when a set with several sheets moves, and hereIds with the sheets' ids)
      const hereL = new Set([...here, ...[].concat(o.here || []).flatMap(h => String(h).split(' + ').map(x => x.trim()))].filter(Boolean)), hereIds = new Set(arr(o.hereIds).map(String));
      let pieces = arr(o.pieces).filter(p => p && typeof p === 'object').map((p, i) => ({ index: p.index != null ? +p.index : i + 1, label: p.label || '', sheetId: p.sheetId || '', sheetLabel: String(p.sheetLabel || ''), setId: p.setId || '', thumb: p.thumb || null }));
      pieces.forEach(p => { p.here = !!((ctx.kind !== 'set' && p.sheetId && String(p.sheetId) === String(ctx.id)) || (p.sheetId && hereIds.has(String(p.sheetId))) || (p.sheetLabel && hereL.has(p.sheetLabel))); });
      // no pieces told: the sheets are (here, there)
      if (!pieces.length) {
        const labels = [...hereL].map(l => ({ l, h: true })).concat(arr(o.there).map(l => ({ l, h: false })));
        pieces = labels.map((x, i) => ({ index: i + 1, label: '', sheetId: '', sheetLabel: String(x.l), setId: '', thumb: null, here: x.h }));
      }
      pieces.sort((a, b) => (b.here - a.here) || (a.index - b.index));
      const customer = String(o.customer || o.buyer || o.name || '').trim(), thumb = typeof o.thumb === 'string' ? o.thumb : null;
      // a sheet that cannot give its piece up (already cut, completed, its set committed): said on the card before anything is pressed
      const locked = arr(o.locked).filter(l => l && typeof l === 'object' && (l.sheetLabel || l.why)).map(l => ({ sheetId: String(l.sheetId || ''), sheetLabel: String(l.sheetLabel || ''), why: String(l.why || '').trim().replace(/[.\s]+$/, '') }));
      out.push({ orderId, label: String(o.label || ''), customer, thumb, pieces, locked, sig: JSON.stringify([orderId, customer, thumb ? 1 : 0, pieces.map(p => [p.index, p.sheetId, p.sheetLabel, p.here ? 1 : 0, p.thumb ? 1 : 0]), locked.map(l => [l.sheetId, l.why])]) });
    }
    return out;
  }
  const signature = list => list.map(o => o.sig).join('|');
  const orderNo = o => digits(o.orderId) || o.label || o.orderId;
  const rawOf = o => ({ orderId: o.orderId, label: o.label, customer: o.customer, thumb: o.thumb, locked: o.locked.map(l => Object.assign({}, l)), pieces: o.pieces.map(p => ({ index: p.index, label: p.label, sheetId: p.sheetId, sheetLabel: p.sheetLabel, setId: p.setId, thumb: p.thumb })), here: (o.pieces.find(p => p.here) || {}).sheetLabel || '', there: o.pieces.filter(p => !p.here).map(p => p.sheetLabel) });

  /* ── words ── */
  function words(M, n) {
    const o = M.opts, sheet = M.sheetLabel, set = M.setLabel, tgt = o.targetLabel ? String(o.targetLabel) : '', why = typeof o.reason === 'string' ? o.reason.trim() : '';   // (a caller's own one sentence replaces the usual one)
    if (M.loading) return { title: `Looking for the orders that keep <b>${esc(M.subject)}</b> here`, plain: `Looking for the orders that keep ${M.subject} here`, sub: 'This takes a moment.' };
    if (n === 0) return { title: `Nothing holds <b>${esc(M.subject)}</b> any more`, plain: `Nothing holds ${M.subject} any more`, sub: tgt ? `It can move to ${tgt} now.` : 'It is free to move now.' };
    if (M.kind === 'set') {
      const t = `${n === 1 ? 'This order keeps' : 'These orders keep'} <b>${esc(set || 'this set')}</b> together`;
      return { title: t, plain: t.replace(/<[^>]+>/g, ''), sub: why || `${n === 1 ? 'Its' : 'Their'} pieces sit in other sets, so it can't move${tgt ? ' to ' + tgt : ''} yet.` };
    }
    const t = `${n === 1 ? 'This order keeps' : 'These orders keep'} <b>${esc(sheet || 'this sheet')}</b> in <b>${esc(set || 'its set')}</b>`;
    return { title: t, plain: t.replace(/<[^>]+>/g, ''), sub: why || `${n === 1 ? 'Its' : 'Their'} pieces sit on other sheets, so it can't ${tgt ? 'move to ' + tgt : 'leave ' + (set || 'its set')} yet.` };
  }
  /** The sheets these orders tie together (the sheet being moved first, in gold), as chips. */
  function sheetsHtml(M) {
    const seen = new Map();
    for (const o of M.orders) for (const p of o.pieces) if (p.sheetLabel && (!seen.has(p.sheetLabel) || p.here)) seen.set(p.sheetLabel, !!p.here || seen.get(p.sheetLabel));
    const list = [...seen].sort((a, b) => (b[1] - a[1])).slice(0, 5), more = seen.size - list.length;
    if (list.length < 2) return '';
    const chip = ([l, h]) => `<span class="soChip${h ? ' here' : ''}" style="--dot:${dotOf(l)}"><i></i><span>${esc(l)}</span></span>`;
    return list.map(chip).join('') + (more > 0 ? `<span class="soChip"><span>+${more}</span></span>` : '');
  }
  function derive(M) {
    const o = M.opts;
    let sheet = o.sheetLabel || '', set = o.setLabel || '';
    const first = M.orders[0] || normalize(arr(o.orders).slice(0, 1), { kind: M.kind, id: M.id, sheetLabel: '' })[0];
    if (!sheet && first) { const h = first.pieces.find(p => p.here); if (h) sheet = h.sheetLabel; }
    try {
      const rows = (W.CN && CN.S && CN.S.library && CN.S.library.rows) || [];
      const row = M.kind === 'sheet' ? rows.find(r => r && r.id === M.id) : rows.find(r => r && r.setId === M.id);
      if (!set && row && row.setSeq) set = `Set ${row.setSeq}`;
      if (!set && M.kind === 'set') { const m = /-(\d+)$/.exec(String(M.id)); if (m) set = `Set ${+m[1]}`; }
    } catch (_) { /* labels stay as given */ }
    M.sheetLabel = sheet; M.setLabel = set;
    M.subject = M.kind === 'set' ? (set || 'This set') : (sheet || 'This sheet');
  }

  /* ── cards ── */
  function tileHtml(o, p) {
    const url = p.thumb || o.thumb || rowImage(o.orderId);
    const img = typeof url === 'string' && url ? `<img src="${esc(url)}" alt="" decoding="async" loading="eager" referrerpolicy="no-referrer" data-so-img>` : '';
    return `<span class="soTile">${img}<span class="soPh">${ICON.charm}</span><span class="soNum" aria-hidden="true">${esc(p.index)}</span></span>`;
  }
  /** "GF Sheet 1" as "GF 1" on a small chip (the full name is in the head and in the card's spoken name). */
  const shortOf = l => l ? String(l).replace(/\s*sheet\s*/i, ' ').trim() : 'No sheet';
  function pieceHtml(o, p, i) {
    return `<span class="soPiece ${p.here ? 'here' : 'there'}"><span class="soTileWrap">${i ? `<span class="soJoin" aria-hidden="true">${ICON.link}</span>` : ''}${tileHtml(o, p)}</span><span class="soChip" style="--dot:${dotOf(p.sheetLabel)}"><i></i><span>${esc(shortOf(p.sheetLabel))}</span></span><span class="soWhere">${p.here ? 'here' : 'there'}</span></span>`;
  }
  function piecesHtml(o) {
    const shown = o.pieces.slice(0, o.pieces.length > SHOWN_PIECES ? SHOWN_PIECES - 1 : SHOWN_PIECES), rest = o.pieces.length - shown.length;
    const parts = shown.map((p, i) => pieceHtml(o, p, i));
    if (rest > 0) parts.push(`<span class="soPiece soMore there"><span class="soTileWrap"><span class="soJoin" aria-hidden="true">${ICON.link}</span><span class="soTile">+${rest}</span></span><span class="soChip" style="--dot:var(--ink25)"><i></i><span>more</span></span><span class="soWhere">there</span></span>`);
    return parts.join('');
  }
  function aria(o) {
    const ps = o.pieces.map(p => `${p.sheetLabel || 'no sheet yet'}${p.here ? ' (this sheet)' : ''}`);
    return `Open order ${orderNo(o)}${o.customer ? ', ' + o.customer : ''}. ${plural(o.pieces.length, 'piece')}: ${ps.join(', ')}.`;
  }
  /** "Stays on RG Sheet 1: its Rose Gold cut is recorded." (the plain reason the engine gives, after the sheet's name) */
  function lockedHtml(o) {
    if (!o.locked.length) return '';
    const seen = new Set(), parts = [];
    for (const l of o.locked) { const k = l.sheetLabel + '|' + l.why; if (seen.has(k)) continue; seen.add(k); parts.push(`${l.sheetLabel ? `<b>${esc(l.sheetLabel)}</b>` : 'A sheet'}${l.why ? ': ' + esc(l.why) : ''}`); }
    const shown = parts.slice(0, 2), more = parts.length - shown.length;
    return `<p class="soLocked">${ICON.lock}<span>Stays on ${shown.join('; ')}${more > 0 ? ` and ${more} more` : ''}.</span></p>`;
  }
  function cardHtml(o) {
    return `<button type="button" class="soOpen" data-open aria-label="${esc(aria(o))}"><span class="soPieces">${piecesHtml(o)}</span><span class="soMeta"><span class="soOrderNo">#${esc(orderNo(o))}</span>${o.customer ? `<span class="soWho">${esc(o.customer)}</span>` : ''}<span class="soGo" aria-hidden="true">${ICON.go}</span></span></button>`
      + lockedHtml(o)
      + `<div class="soSlot soFootSlot"><div class="soClip"><div class="soFoot"><button type="button" class="soOff" data-off>${ICON.off}<span>Take off the sheet…</span></button></div></div></div>`;
  }
  function wireImages(root) {
    for (const im of root.querySelectorAll('img[data-so-img]')) {
      const ok = () => im.classList.add('in'), bad = () => im.remove();
      if (im.complete && im.naturalWidth) ok(); else { im.addEventListener('load', ok, { once: true }); im.addEventListener('error', bad, { once: true }); }
    }
  }
  function makeCard(o, arrive) {
    const li = doc.createElement('li'); li.className = 'soCard' + (arrive && !reduced() ? ' arriving' : ''); li.dataset.order = o.orderId; li.dataset.state = '';
    li.innerHTML = cardHtml(o);
    if (arrive && !reduced()) li.addEventListener('animationend', () => li.classList.remove('arriving'), { once: true });
    wireImages(li);
    return li;
  }
  /** A picture the page holds now and did not when the card was drawn (Etsy photos arrive on their own schedule). */
  function fillPictures(M) {
    for (const card of M.el.querySelectorAll('.soCard')) {
      const o = M.orders.find(x => x.orderId === card.dataset.order); if (!o) continue;
      const tiles = [...card.querySelectorAll('.soTile')];
      tiles.forEach((t, i) => {
        if (t.querySelector('img') || !o.pieces[i] || t.parentNode.parentNode.classList.contains('soMore')) return;
        const u = o.pieces[i].thumb || o.thumb || rowImage(o.orderId);
        if (typeof u !== 'string' || !u) return;
        const im = doc.createElement('img'); im.src = u; im.alt = ''; im.decoding = 'async'; im.setAttribute('data-so-img', ''); im.referrerPolicy = 'no-referrer';
        t.prepend(im); wireImages(t);
      });
    }
  }

  /* ── reading ── */
  function readOrders(M) {
    const o = M.opts, S = W.SharedOrders;
    if (S && typeof S.between === 'function') {
      const r = S.between(M.id, o.targetSetId == null ? null : o.targetSetId, { kind: M.kind });
      return then(r) ? r : Promise.resolve(r);
    }
    if (typeof o.read === 'function') { const r = o.read(); return then(r) ? r : Promise.resolve(r); }
    return Promise.resolve(null);   // (nothing to ask: what is shown stays as it is)
  }

  /* ── the window ── */
  let CUR = null, uid = 0;
  const live = M => !!(M && !M.closed && M.dlg.isConnected);
  const say = (M, text) => { const n = M.dlg.querySelector('.soLive'); if (n) { n.textContent = ''; setTimeout(() => { if (n.isConnected) n.textContent = text; }, 30); } };
  const q = (M, sel) => M.dlg.querySelector(sel);
  const countText = (n, loading) => loading ? '' : n === 0 ? 'All clear' : `${n} ${n === 1 ? 'order' : 'orders'}`;

  function paintHead(M, n, animate) {
    const w = words(M, n), t = q(M, '.soTitle'), s = q(M, '.soSub'), c = q(M, '.soCount'), box = q(M, '.soTitles');
    M.dlg.dataset.state = M.loading ? 'loading' : n === 0 ? 'clear' : 'list';
    M.dlg.setAttribute('aria-label', w.plain);
    const ct = countText(n, M.loading);
    if (c.textContent !== ct) { c.textContent = ct; if (animate && !reduced()) { c.classList.add('tick'); setTimeout(() => c.classList.remove('tick'), 320); } }
    const sh = n && !M.loading ? sheetsHtml(M) : '', key = w.title + '|' + w.sub + '|' + sh;
    if (M.words === key) return;
    M.words = key;
    const set = () => { t.innerHTML = w.title; s.textContent = w.sub; q(M, '.soSheets').innerHTML = sh; };
    if (animate && !reduced()) { box.classList.add('swap'); setTimeout(() => { if (!live(M)) return; set(); box.classList.remove('swap'); }, 200); } else set();
  }
  /** Before the first answer: skeleton cards (the shimmer of the app's own placeholders), a labelled spinner above them. */
  function renderLoading(M) {
    const body = q(M, '.soBody');
    body.innerHTML = `<p class="soWait" role="status"><span class="soSpin" aria-hidden="true"></span><span>Finding the orders…</span></p><div class="soGrid" aria-hidden="true">${'<div class="soSkel"><i class="a"></i><i class="b"></i><i class="c"></i></div>'.repeat(2)}</div>`;
    M.dlg.setAttribute('aria-busy', 'true');
  }
  /** The first read failed: one short line and a way to try again (never a red box). */
  function renderError(M, msg) {
    const body = q(M, '.soBody');
    body.innerHTML = `<div class="soErr" role="alert"><i aria-hidden="true"></i><span>${esc(msg || 'The orders could not be read.')}</span><button type="button" class="soBtn" data-reload>Try again</button></div>`;
    M.dlg.removeAttribute('aria-busy');
  }
  /** The whole body, drawn from M.orders (as it opens, and when the list comes back after it was empty). */
  function render(M, animate) {
    const body = q(M, '.soBody'), n = M.orders.length;
    M.dlg.removeAttribute('aria-busy');
    paintHead(M, n, animate);
    if (!n) return renderClear(M);
    body.innerHTML = '<ul class="soGrid" role="list"></ul>';
    const grid = body.firstElementChild;
    M.orders.forEach((o, i) => {
      const li = makeCard(o, false); grid.appendChild(li);
      if (animate !== false && !reduced()) li.animate([{ opacity: 0, transform: 'translateY(10px)' }, { opacity: 1, transform: 'none' }], { duration: 360, delay: 380 + Math.min(i, 8) * 45, easing: EASE, fill: 'backwards' });
    });
  }
  function renderClear(M) {
    const body = q(M, '.soBody'), w = words(M, 0);
    if (body.querySelector('.soClear')) return;
    const retry = typeof M.opts.onRetry === 'function';
    body.innerHTML = `<div class="soClear" role="group" aria-label="${esc(w.plain)}">${ICON.ring}<p class="soClearT">${esc(w.sub)}</p><div class="soBtns">${retry ? '<button type="button" class="soBtn gold" data-retry>Move it now</button>' : ''}<button type="button" class="soBtn" data-close>${retry ? 'Close' : 'Done'}</button></div></div>`;
    const first = body.querySelector('[data-retry]') || body.querySelector('[data-close]'), a = doc.activeElement;
    // the focus follows the news (a person who was on a card that went is not left on nothing)
    if (!a || a === doc.body || a === M.dlg || !a.isConnected || !M.dlg.contains(a)) { try { first.focus({ preventScroll: true }); } catch (_) {} }
    say(M, `${w.plain}. ${w.sub}`);
  }

  /** The list as the page now says it: cards that went leave in front of the person, new ones come in, a changed one is drawn again. */
  function reconcile(M, next, why) {
    if (!live(M)) return;
    const prev = M.orders;
    // an order being taken off stays on screen (spinner and all) until its own answer comes back
    if (M.busyId && !next.some(o => o.orderId === M.busyId)) { const keep = prev.find(o => o.orderId === M.busyId); if (keep) next = next.concat([keep]); }
    if (signature(prev) === signature(next)) return;
    M.orders = next;
    const body = q(M, '.soBody'), animate = !reduced() && M.dlg.open && !M.away, grid = body.querySelector('.soGrid');
    if (!next.length) {
      paintHead(M, 0, animate);
      const cards = grid ? [...grid.children] : [];
      if (animate && cards.length) { cards.forEach(c => c.classList.add('leaving')); setTimeout(() => { if (live(M) && !M.orders.length) renderClear(M); }, 260); }
      else renderClear(M);
    } else if (!grid) {
      render(M, false);
    } else {
      const first = new Map([...grid.children].map(c => [c, c.getBoundingClientRect()]));
      const have = new Map([...grid.children].filter(c => !c.classList.contains('leaving')).map(c => [c.dataset.order, c])), want = new Set(next.map(o => o.orderId));
      let moved = false;
      for (const [id, c] of have) if (!want.has(id)) { moved = true; leave(M, c); }
      next.forEach((o, i) => {
        const c = have.get(o.orderId), old = prev.find(x => x.orderId === o.orderId);
        if (!c) { moved = true; const li = makeCard(o, true), keep = [...grid.children].filter(x => !x.classList.contains('leaving')), after = keep[i - 1]; if (after) after.after(li); else grid.prepend(li); }
        else if (old && old.sig !== o.sig && c.dataset.state === '') { const f = doc.activeElement === c.querySelector('[data-open]'); c.innerHTML = cardHtml(o); wireImages(c); if (f) { try { c.querySelector('[data-open]').focus({ preventScroll: true }); } catch (_) {} } }
        else if (old && old.sig !== o.sig) c.dataset.dirty = '1';
      });
      paintHead(M, next.length, animate);
      if (moved && animate) flip(first);
    }
    const gone = prev.filter(o => !next.some(x => x.orderId === o.orderId)).length;
    if (gone) say(M, `${plural(gone, 'order')} no longer ${gone === 1 ? 'holds' : 'hold'} ${M.subject}.${next.length ? ' ' + plural(next.length, 'order') + ' left.' : ''}`);
    try { M.opts.onChange && M.opts.onChange(next.map(rawOf), why || 'live'); } catch (e) { warn('onChange', e); }
  }
  /** A card leaves: it fades and shrinks a little, then is gone and the others glide into its place. */
  function leave(M, c) {
    const focused = c.contains(doc.activeElement), nextEl = c.nextElementSibling || c.previousElementSibling, grid = c.parentNode;
    c.classList.add('leaving'); c.classList.remove('arriving');
    const end = () => {
      if (!c.isConnected) return;
      const first = new Map([...grid.children].filter(x => x !== c).map(x => [x, x.getBoundingClientRect()]));
      c.remove(); if (grid.isConnected && M.dlg.open && !reduced()) flip(first);
    };
    if (reduced() || !M.dlg.open || M.away) end(); else setTimeout(end, 230);
    if (focused && nextEl) { const b = nextEl.querySelector('[data-open]'); if (b) { try { b.focus({ preventScroll: true }); } catch (_) {} } }
  }
  function flip(first) {
    if (reduced()) return;
    for (const [el, r0] of first) {
      if (!el.isConnected || el.classList.contains('leaving')) continue;
      const r1 = el.getBoundingClientRect(), dx = r0.left - r1.left, dy = r0.top - r1.top;
      if (Math.abs(dx) < 1 && Math.abs(dy) < 1) continue;
      try { el.animate([{ transform: `translate(${dx}px,${dy}px)` }, { transform: 'none' }], { duration: 340, easing: EASE }); } catch (_) {}
    }
  }

  /* ── reading again: after a change of ours, when the page says so, and about once a second from its own state ── */
  async function reread(M, why) {
    if (!live(M)) return;
    if (M.reading) { M.again = why || M.again || 'live'; return M.reading; }
    const t0 = Date.now();
    const run = (async () => {
      try {
        const got = await readOrders(M);
        if (!live(M)) return;
        if (!Array.isArray(got)) { if (M.loading) { M.loading = false; renderError(M, 'The orders could not be read.'); } return; }
        const next = normalize(got, { kind: M.kind, id: M.id, sheetLabel: M.sheetLabel });
        if (M.loading) { M.loading = false; M.orders = next; derive(M); M.words = ''; render(M, true); fillPictures(M); return; }
        if (M.away) { M.pending = next; return; }    // (over there: what changed leaves in front of the person once back)
        reconcile(M, next, why);
        fillPictures(M);
      } catch (e) { warn('read', e); if (live(M) && M.loading) { M.loading = false; renderError(M, 'The orders could not be read.'); } }
    })();
    M.reading = run;
    await run;
    M.reading = null; M.slow = Date.now() - t0 > 150;
    if (M.again && live(M)) { const w = M.again; M.again = null; return reread(M, w); }
  }
  function watch(M) {
    const every = () => (M.slow || document.hidden ? SLOW_EVERY : LIVE_EVERY);
    const loop = () => { M.timer = 0; if (!live(M)) return; if (!document.hidden) reread(M, 'live'); M.timer = setTimeout(loop, every()); };
    M.timer = setTimeout(loop, every());
    try { const S = W.SharedOrders; if (S && typeof S.subscribe === 'function') { const off = S.subscribe(() => { if (live(M)) reread(M, 'live'); }); if (typeof off === 'function') M.offs.push(off); } } catch (e) { warn('subscribe', e); }
    const vis = () => { if (!document.hidden && live(M)) reread(M, 'live'); };
    doc.addEventListener('visibilitychange', vis); M.offs.push(() => doc.removeEventListener('visibilitychange', vis));
    // the Library's own live read, at once (it carries other computers' changes; its own 3 s loop keeps it up to date)
    try { if (W.LaserReview && LaserReview.nudge) LaserReview.nudge(); } catch (_) {}
  }

  /* ── an order's detail view, and the way back to this window ── */
  function backPill(M) {
    const ow = doc.getElementById('orderWin'), head = ow && ow.querySelector('.owHead'); if (!head) return null;
    for (const old of ow.querySelectorAll('.soBack')) old.remove();
    const b = doc.createElement('button'); b.type = 'button'; b.className = 'soBack'; b.id = 'soBack';
    const where = M.kind === 'set' ? M.setLabel : M.sheetLabel;
    b.setAttribute('aria-label', where ? `Back to the shared orders of ${where}` : 'Back to the shared orders');
    b.innerHTML = `${ICON.back}<span>Back to shared orders</span>`;
    b.onclick = () => { try { if (W.OrderWin && OrderWin.close) OrderWin.close(); else ow.close(); } catch (e) { warn('back', e); } };
    head.insertBefore(b, head.firstChild);
    return b;
  }
  function openOrder(M, btn, rid) {
    rid = digits(rid); if (!rid) return false;
    const ow = doc.getElementById('orderWin');
    let ok = false;
    try { if (typeof W.openOrderFrom === 'function') ok = W.openOrderFrom(btn, rid, {}) !== false; } catch (e) { warn('open order', e); }
    if (!ok) { try { if (W.OrderWin && typeof OrderWin.openOrder === 'function') { OrderWin.openOrder(rid, { from: btn }); ok = true; } } catch (e) { warn('open order', e); } }
    if (!ok) return false;
    M.away = true;
    const pill = backPill(M);
    const back = () => {
      if (ow && ow.open) return;           // (the order view moved to another line, or is opening again)
      M.away = false;
      if (pill && pill.isConnected) pill.remove();
      if (ow) ow.removeEventListener('close', back);
      // the order may have been taken off, held or cancelled over there: read again and let what changed leave in front of the person
      setTimeout(() => { if (!live(M)) return; const p = M.pending; M.pending = null; if (p) reconcile(M, p, 'back'); reread(M, 'back'); }, reduced() ? 0 : 460);
    };
    if (ow) ow.addEventListener('close', back);
    return true;
  }

  /* ── taking an order off ── */
  function askOff(M, card, o) {
    if (M.busyId || card.dataset.state === 'ask') return;
    closeAsks(M);
    for (const old of card.querySelectorAll(':scope > .soAskSlot')) old.remove();
    card.dataset.state = 'ask';
    const need = !who(), box = doc.createElement('div'); box.className = 'soAsk'; box.setAttribute('role', 'group'); box.setAttribute('aria-label', `Take order ${orderNo(o)} off its sheets`);
    const name = `soWhat${++uid}`;
    box.innerHTML = `<p class="soAskQ">Take #${esc(orderNo(o))} off its sheets?</p>`
      + `<div class="soOpts" role="radiogroup" aria-label="What happens to the order"><label class="soOpt"><input type="radio" name="${name}" value="hold"><span>Put on hold</span></label><label class="soOpt"><input type="radio" name="${name}" value="cancel"><span>Cancel the order</span></label></div>`
      + `<p class="soAskText" aria-live="polite"></p>`
      + (need ? '<input class="soName" type="text" maxlength="40" autocomplete="name" spellcheck="false" placeholder="Your name, for the record" aria-label="Your name, kept with this change">' : '')
      + '<div class="soBtns"><button type="button" class="soBtn go" data-yes disabled>Take off</button><button type="button" class="soBtn" data-keep>Keep it</button></div>';
    // the choice grows in where the quiet button was (the footer folds away as it opens: the grid does not jump)
    const slot = doc.createElement('div'), clip = doc.createElement('div'); slot.className = 'soSlot soAskSlot'; clip.className = 'soClip'; clip.appendChild(box); slot.appendChild(clip); card.appendChild(slot);
    const yes = box.querySelector('[data-yes]'), text = box.querySelector('.soAskText'), nameIn = box.querySelector('.soName');
    const TEXT = { hold: 'It waits under On hold until someone puts it back.', cancel: 'It leaves every list. Its record stays under Orders › Cancelled.' };
    const sync = () => {
      const v = (box.querySelector('input:checked') || {}).value || '';
      text.textContent = TEXT[v] || '';
      yes.textContent = v === 'cancel' ? 'Take off and cancel' : v === 'hold' ? 'Take off and hold' : 'Take off';
      yes.classList.toggle('danger', v === 'cancel');
      yes.disabled = !v || (need && !(nameIn.value || '').trim());
    };
    box.addEventListener('change', sync); if (nameIn) nameIn.addEventListener('input', sync);
    box.querySelector('[data-keep]').onclick = () => closeAsk(M, card, true);
    yes.onclick = () => {
      const v = (box.querySelector('input:checked') || {}).value; if (!v || yes.disabled) return;
      if (need) { const n = nameIn.value.trim(); try { if (W.B) B.employee = n; localStorage.setItem('cn.employee', n); } catch (_) {} }
      takeOff(M, card, o, v);
    };
    box.addEventListener('keydown', e => { if (e.key === 'Enter' && e.target === nameIn) { e.preventDefault(); yes.click(); } });
    sync();
    void slot.offsetHeight; slot.classList.add('open');
    requestAnimationFrame(() => { try { box.querySelector('input').focus({ preventScroll: true }); } catch (_) {} });
    setTimeout(() => { try { if (slot.isConnected && slot.classList.contains('open')) slot.scrollIntoView({ block: 'nearest', behavior: reduced() ? 'auto' : 'smooth' }); } catch (_) {} }, reduced() ? 0 : 280);
  }
  /** The choice folds away (a moment later it is gone from the page). */
  function dropAsk(card) {
    for (const slot of card.querySelectorAll(':scope > .soAskSlot')) {
      slot.classList.remove('open');
      if (reduced()) slot.remove(); else setTimeout(() => slot.remove(), 320);
    }
  }
  function closeAsk(M, card, refocus) {
    card.dataset.state = ''; dropAsk(card);
    if (card.dataset.dirty) { delete card.dataset.dirty; reread(M, 'live'); }
    if (refocus) { const b = card.querySelector('[data-off]'); if (b) { try { b.focus({ preventScroll: true }); } catch (_) {} } }
  }
  function closeAsks(M, refocus) { let any = false; for (const c of M.dlg.querySelectorAll('.soCard[data-state=ask]')) { closeAsk(M, c, !!refocus); any = true; } return any; }
  function setBusy(card, label) {
    let b = card.querySelector(':scope > .soBusy');
    if (!label) { if (b) b.remove(); card.removeAttribute('aria-busy'); return; }
    if (!b) { b = doc.createElement('div'); b.className = 'soBusy'; b.setAttribute('role', 'status'); card.appendChild(b); }
    card.setAttribute('aria-busy', 'true'); b.innerHTML = `<span class="soSpin" aria-hidden="true"></span><span>${esc(label)}</span>`;
  }
  function setNote(card, kind, text, buttons) {
    let n = card.querySelector(':scope > .soNote');
    if (!text) { if (n) n.remove(); return; }
    if (!n) { n = doc.createElement('div'); card.appendChild(n); }
    n.className = 'soNote' + (kind ? ' ' + kind : '') + (reduced() ? '' : ' enter'); n.setAttribute('role', 'status');
    n.innerHTML = `<p>${esc(text)}</p>`;
    if (buttons && buttons.length) { const row = doc.createElement('div'); row.className = 'soBtns'; for (const b of buttons) { const x = doc.createElement('button'); x.type = 'button'; x.className = 'soBtn'; x.textContent = b.label; x.onclick = b.fn; row.appendChild(x); } n.appendChild(row); }
  }
  /** A note with buttons is where the person's focus goes when it was on the card that was busy (never left on nothing). */
  function noteFocus(M, card) {
    const a = doc.activeElement, b = card.querySelector(':scope > .soNote button');
    if (b && (!a || a === doc.body || a === M.dlg || !a.isConnected || !M.dlg.contains(a) || card.contains(a))) { try { b.focus({ preventScroll: true }); } catch (_) {} }
  }
  async function takeOff(M, card, o, mode) {
    const S = W.SharedOrders && typeof SharedOrders.removeFromSheet === 'function' ? W.SharedOrders : null;
    setNote(card, '', '');
    if (!S) { card.dataset.state = ''; dropAsk(card); setNote(card, 'bad', 'This cannot be done from here right now. Open the order and use its Sheet tab.', [{ label: 'OK', fn: () => setNote(card, '', '') }]); return; }
    M.busyId = o.orderId; card.dataset.state = 'busy';
    for (const b of M.dlg.querySelectorAll('.soOff')) b.setAttribute('aria-disabled', 'true');
    const label = mode === 'cancel' ? 'Cancelling the order…' : 'Taking it off its sheets…';
    setBusy(card, label); say(M, label);
    { const bz = card.querySelector(':scope > .soBusy'); if (bz && card.contains(doc.activeElement)) { bz.tabIndex = -1; try { bz.focus({ preventScroll: true }); } catch (_) {} } }   // (the focus stays on the card while it is busy)
    const slow = setTimeout(() => { if (card.isConnected && card.dataset.state === 'busy') setBusy(card, label.replace('…', '') + '. This is taking a little longer than usual'); }, SLOW_AFTER);
    let res;
    try {
      const sheetId = M.kind === 'sheet' ? M.id : (o.pieces.find(p => p.here && p.sheetId) || {}).sheetId || '';
      res = await S.removeFromSheet({ orderId: o.orderId, sheetId, mode, by: who() || undefined });
    } catch (e) { res = { ok: false, error: errText(e) }; }
    clearTimeout(slow); M.busyId = null;
    for (const b of M.dlg.querySelectorAll('.soOff')) b.removeAttribute('aria-disabled');
    if (!live(M)) return;
    res = res && typeof res === 'object' ? res : { ok: false, error: 'No answer came back.' };
    // (the focus was on the busy card: it goes back to the card itself, so a card that now leaves hands it to its neighbour)
    const had = card.contains(doc.activeElement);
    card.dataset.state = ''; setBusy(card, ''); dropAsk(card);
    if (had) { const b = card.querySelector('[data-open]'); if (b) { try { b.focus({ preventScroll: true }); } catch (_) {} } }
    if (res.ok === false || res.error) {
      const msg = `Not taken off. ${errText(res.error) || 'Nothing was changed.'}`.trim();
      setNote(card, 'bad', msg, [{ label: 'Try again', fn: () => { setNote(card, '', ''); askOff(M, card, o); } }, { label: 'Keep it', fn: () => setNote(card, '', '') }]);
      say(M, msg); noteFocus(M, card); return;
    }
    // done: ask again at once (the answer is the page's own state, which removeFromSheet has just changed)
    await reread(M, 'removed');
    if (!live(M)) return;
    const still = M.orders.find(x => x.orderId === o.orderId), c2 = M.dlg.querySelector(`.soCard[data-order="${cssId(o.orderId)}"]`);
    if (!still || !c2) return;
    const stayed = arr(res.stayed);
    if (stayed.length) {
      const why = [...new Set(stayed.map(s => s && (s.why || s.reason)).filter(Boolean))].slice(0, 2).join('; ');
      setNote(c2, '', `${stayed.length === 1 ? 'One piece stays' : stayed.length + ' pieces stay'}${why ? ': ' + why : ''}. That keeps this order here.`, [{ label: 'Got it', fn: () => setNote(c2, '', '') }]);
      noteFocus(M, c2); return;
    }
    // listed still: the page has not caught up yet. A small labelled spinner, then the live reads finish it.
    c2.dataset.state = 'busy'; setBusy(c2, 'Updating the sheets…');
    const t0 = Date.now();
    const wait = async () => {
      if (!live(M) || !c2.isConnected) return;
      await reread(M, 'removed');
      if (!live(M) || !c2.isConnected || !M.orders.find(x => x.orderId === o.orderId)) return;
      if (Date.now() - t0 > SETTLE_MAX) { c2.dataset.state = ''; setBusy(c2, ''); setNote(c2, '', 'Taken off, but this list has not caught up yet. It follows by itself.', [{ label: 'OK', fn: () => setNote(c2, '', '') }]); return; }
      setTimeout(wait, 450);
    };
    wait();
  }

  /* ── events ── */
  function wire(M) {
    const dlg = M.dlg;
    dlg.addEventListener('pointerdown', e => { M.pressedInside = e.target !== dlg; }, true);
    dlg.addEventListener('click', e => {
      if (e.target === dlg) { if (!M.pressedInside && !M.busyId && !dlg.querySelector('.soCard[data-state=ask]')) closeIt(M, 'backdrop'); return; }
      const b = e.target.closest && e.target.closest('button'); if (!b || !dlg.contains(b)) return;
      const card = b.closest('.soCard');
      if (b.hasAttribute('data-x') || b.hasAttribute('data-close')) return void closeIt(M, 'close');
      if (b.hasAttribute('data-retry')) return void retry(M);
      if (b.hasAttribute('data-reload')) { M.loading = true; M.words = ''; paintHead(M, 0, false); renderLoading(M); reread(M, 'open'); return; }
      if (b.hasAttribute('data-open') && card) { const o = M.orders.find(x => x.orderId === card.dataset.order); if (o && !openOrder(M, b, o.orderId)) setNote(card, 'bad', 'This order could not be opened from here.', [{ label: 'OK', fn: () => setNote(card, '', '') }]); return; }
      if (b.hasAttribute('data-off') && card) { const o = M.orders.find(x => x.orderId === card.dataset.order); if (o) askOff(M, card, o); }
    });
    dlg.addEventListener('keydown', e => {
      if (e.key === 'Tab') {
        // the focus stays in the window: Tab from the last control goes to the first, Shift+Tab from the first to the last
        const f = [...dlg.querySelectorAll('button:not([disabled]),input:not([disabled]),[tabindex]:not([tabindex="-1"])')].filter(el => el.getClientRects().length && !el.closest('.leaving'));
        if (f.length) { const a = doc.activeElement, first = f[0], last = f[f.length - 1];
          const outside = !dlg.contains(a) || a === dlg;
          if (e.shiftKey && (a === first || outside)) { e.preventDefault(); last.focus(); }
          else if (!e.shiftKey && (a === last || outside)) { e.preventDefault(); first.focus(); } }
        return;
      }
      if (e.key === 'Escape') { if (closeAsks(M, true)) { e.preventDefault(); e.stopPropagation(); M.escHeld = Date.now(); } return; }
      if (!['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown', 'Home', 'End'].includes(e.key)) return;
      if (e.target === dlg) {   // (the focus rests on the window at first: an arrow goes to the first card, End/Up/Left to the last)
        const all = [...dlg.querySelectorAll('.soCard:not(.leaving) .soOpen')], to = ['End', 'ArrowUp', 'ArrowLeft'].includes(e.key) ? all[all.length - 1] : all[0];
        if (to) { e.preventDefault(); try { to.focus({ preventScroll: true }); to.scrollIntoView({ block: 'nearest' }); } catch (_) {} }
        return;
      }
      const t = e.target; if (!t || !t.matches || !t.matches('.soOpen')) return;
      const all = [...dlg.querySelectorAll('.soCard:not(.leaving) .soOpen')], i = all.indexOf(t); if (i < 0) return;
      let to = null;
      if (e.key === 'Home') to = all[0]; else if (e.key === 'End') to = all[all.length - 1];
      else {
        const r = t.getBoundingClientRect(), cx = r.left + r.width / 2, cy = r.top + r.height / 2; let best = 1e9;
        for (const c of all) {
          if (c === t) continue;
          const b = c.getBoundingClientRect(), dx = b.left + b.width / 2 - cx, dy = b.top + b.height / 2 - cy, horiz = e.key === 'ArrowLeft' || e.key === 'ArrowRight';
          const ok = e.key === 'ArrowRight' ? dx > 8 && Math.abs(dy) < r.height * .6 : e.key === 'ArrowLeft' ? dx < -8 && Math.abs(dy) < r.height * .6 : e.key === 'ArrowDown' ? dy > 8 : dy < -8;
          if (!ok) continue;
          const d = horiz ? Math.abs(dx) + Math.abs(dy) * 3 : Math.abs(dy) + Math.abs(dx) * 3; if (d < best) { best = d; to = c; }
        }
        if (!to && (e.key === 'ArrowRight' || e.key === 'ArrowDown')) to = all[i + 1];
        if (!to && (e.key === 'ArrowLeft' || e.key === 'ArrowUp')) to = all[i - 1];
      }
      if (to) { e.preventDefault(); try { to.focus({ preventScroll: true }); to.scrollIntoView({ block: 'nearest', behavior: reduced() ? 'auto' : 'smooth' }); } catch (_) {} }
    });
    // Esc: an open choice goes first, then the window; nothing while an order is being taken off (it is seen through)
    dlg.addEventListener('cancel', e => {
      if (M.busyId) { e.preventDefault(); return; }
      if (M.escHeld && Date.now() - M.escHeld < 400) { e.preventDefault(); return; }
      if (closeAsks(M, true)) e.preventDefault();
    });
    dlg.addEventListener('close', () => finish(M));
  }
  function retry(M) {
    M.retried = true;
    closeIt(M, 'retry');   // (onRetry is called by finish(): once the window has gone, just before onClose)
  }
  function closeIt(M, why) {
    if (!M || M.closed || M.closing) return;
    M.closing = true; M.why = why;
    try { if (M.dlg.open) M.dlg.close(); else finish(M); } catch (_) { finish(M); }
  }
  function finish(M) {
    if (M.closed) return; M.closed = true;
    clearTimeout(M.timer); for (const off of M.offs.splice(0)) { try { off(); } catch (_) {} }
    const ow = doc.getElementById('orderWin'); if (ow) for (const b of ow.querySelectorAll('.soBack')) b.remove();
    if (CUR === M) CUR = null;
    // (the window has gone: the caller is told the person wants the move again first, then that it is closed, so that what closing sets going already knows)
    if (M.retried && typeof M.opts.onRetry === 'function') { try { M.opts.onRetry(); } catch (e) { warn('onRetry', e); } }
    try { if (M.opts.onClose) M.opts.onClose({ cleared: !M.orders.length, retried: !!M.retried, why: M.why || 'close' }); } catch (e) { warn('onClose', e); }
    setTimeout(() => { try { M.dlg.remove(); } catch (_) {} }, 700);
  }

  /* ── open ── */
  function open(opts) {
    try {
      opts = opts && typeof opts === 'object' ? opts : {};
      css();
      // (the callers say it as {kind, id} or as {sheetId} / {setId}, with a targetSetId, an own `reason` and the orders if they hold them)
      const kind = (String(opts.kind || '').toLowerCase() === 'set' || (!opts.kind && opts.setId && !opts.sheetId && !opts.id)) ? 'set' : 'sheet', id = String(opts.id || opts.sheetId || opts.setId || '');
      if (CUR && live(CUR)) { const c = CUR; Object.assign(c.opts, opts); c.kind = kind; if (id) c.id = id; if (Array.isArray(opts.orders)) update(c, opts.orders); else reread(c, 'live'); return handleOf(c); }
      const S = W.SharedOrders, canRead = !!(S && typeof S.between === 'function') || typeof opts.read === 'function';
      if (!Array.isArray(opts.orders) && !canRead) return null;
      if (typeof doc.createElement('dialog').showModal !== 'function') return null;
      const M = { opts, kind, id, dlg: doc.createElement('dialog'), orders: [], offs: [], timer: 0, closed: false, closing: false, busyId: null, away: false, reading: null, slow: false, sheetLabel: '', setLabel: '', subject: '', words: '', loading: !Array.isArray(opts.orders) };
      M.orders = normalize(arr(opts.orders), { kind, id, sheetLabel: opts.sheetLabel || '' });
      derive(M);
      const dlg = M.dlg; dlg.className = 'soDlg'; dlg.dataset.sharedOrders = ''; dlg.dataset.state = M.loading ? 'loading' : M.orders.length ? 'list' : 'clear';
      const w = words(M, M.orders.length); M.words = '';
      dlg.innerHTML = `<div class="soBox"><header class="soHead"><div class="soTitles"><h2 class="soTitle">${w.title}</h2><p class="soSub">${esc(w.sub)}</p><div class="soSheets"></div></div><div class="soHeadRight"><span class="soCount">${countText(M.orders.length, M.loading)}</span><button type="button" class="soX" data-x aria-label="Close">${ICON.close}</button></div></header><div class="soBody"></div><div class="soLive" role="status" aria-live="polite"></div></div>`;
      dlg.setAttribute('aria-label', w.plain);
      wire(M);
      doc.body.appendChild(dlg);
      if (M.loading) { paintHead(M, 0, false); renderLoading(M); } else render(M, null);
      // where the window grows out of: what it was given, else what was pressed a moment ago
      if (opts.from) { if (W.Motion && Motion.from) Motion.from(dlg, opts.from); else dlg._mdFrom = opts.from; }
      dlg.tabIndex = -1;
      dlg.showModal();
      try { dlg.focus({ preventScroll: true }); } catch (_) {}   // (rests on the window itself: a mouse user sees no ring on card 1; arrows or Tab go on from here)
      CUR = M;
      say(M, `${w.plain}. ${w.sub}`);
      watch(M);
      reread(M, 'open');   // (the page may know more than the caller did a moment ago)
      return handleOf(M);
    } catch (e) { warn('open', e); return null; }
  }
  function update(M, list) {
    if (!live(M)) return false;
    const next = normalize(arr(list), { kind: M.kind, id: M.id, sheetLabel: M.sheetLabel });
    if (M.away) { M.pending = next; return true; }
    reconcile(M, next, 'update'); return true;
  }
  function handleOf(M) {
    return { el: M.dlg, close: () => closeIt(M, 'close'), update: list => update(M, list), refresh: () => reread(M, 'live'), isOpen: () => live(M) && M.dlg.open, orders: () => M.orders.map(rawOf) };
  }

  W.SharedOrdersModal = {
    open,
    close: () => { if (CUR) closeIt(CUR, 'close'); },
    isOpen: () => !!(CUR && live(CUR) && CUR.dlg.open),
    current: () => (CUR && live(CUR) ? handleOf(CUR) : null),
    version: '20261005-1',
    _normalize: normalize
  };
})();
