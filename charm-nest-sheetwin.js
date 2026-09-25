/* Charm Nest · the sheet window (Library).
   Paul, 25 Sep 18:01-18:27: the Library's sheet dialog "looks very beta". It is now one window built round the live
   sheet: the charms drawn as the Nest tab draws them, a fast hover naming each charm's order, a click opening that
   charm's order (its pieces here and on the set's other sheets, its back engraving with a way to approve it, its
   customer and team messages), the set's sheets as tabs lit by the order in hand, and the sheet's QR label and files.
   Everything that waits says what it is doing; views slide in from where they were asked for.
   The layout is drawn from the saved record: the sheet this page holds live when it is one of the open run's, else the
   master designs (Pool.masterCharm, shared with the run) placed where the record says. */
(() => {
  "use strict";
  const CODE = { gold: "GF", silver: "SS", rose: "RG", gold10k: "10K", gold14k: "14K" };
  const METAL_OF_CODE = Object.fromEntries(Object.entries(CODE).map(([k, v]) => [v, k]));
  const colorOf = m => (METALS.find(x => x.key === m) || {}).color || "#999";
  // the operator's name, as the bridge keeps it (its helpers are inside the bridge's own scope)
  const whoAmI = () => (window.B && (B.employee || (B.link && B.link.state && B.link.state() && B.link.state().employee))) || (() => { try { return localStorage.getItem("cn.employee") || ""; } catch (_) { return ""; } })();
  function askWho() {
    const v = prompt("Your name — recorded with every approval and decision:", whoAmI() || "");
    if (v && v.trim() && window.B) { B.employee = v.trim(); try { localStorage.setItem("cn.employee", B.employee); } catch (_) {} }
    return whoAmI();
  }
  const still = () => matchMedia("(prefers-reduced-motion: reduce)").matches;
  const EASE = "cubic-bezier(.2,.8,.2,1)";
  const h = (tag, cls, html) => { const e = document.createElement(tag); if (cls) e.className = cls; if (html != null) e.innerHTML = html; return e; };
  const animate = (el, frames, ms, opts) => { if (!el || still() || !el.animate) return Promise.resolve(); try { return el.animate(frames, Object.assign({ duration: ms, easing: EASE, fill: "both" }, opts || {})).finished.catch(() => {}); } catch (_) { return Promise.resolve(); } };
  const ICON = {
    off: '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M3 4.5h10M6.5 4.5V3h3v1.5M4.5 4.5l.6 8.2c.05.7.6 1.3 1.3 1.3h3.2c.7 0 1.25-.6 1.3-1.3l.6-8.2" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>',
    back: '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M10 3 5 8l5 5" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"/></svg>',
    next: '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="m6 3 5 5-5 5" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"/></svg>',
    close: '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M4 4l8 8M12 4l-8 8" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round"/></svg>',
    more: '<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="3.5" cy="8" r="1.4" fill="currentColor"/><circle cx="8" cy="8" r="1.4" fill="currentColor"/><circle cx="12.5" cy="8" r="1.4" fill="currentColor"/></svg>',
    go: '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M3 8h9M8.5 4.5 12 8l-3.5 3.5" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/></svg>',
    check: '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="m3.5 8.5 3 3 6-7" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></svg>',
    search: '<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="7" cy="7" r="4.3" fill="none" stroke="currentColor" stroke-width="1.5"/><path d="m10.3 10.3 3.2 3.2" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>'
  };

  const STYLE = `
dialog.sheetWin{width:min(1480px,98vw);height:min(94vh,1000px);max-height:none;border-radius:16px;overflow:hidden;background:var(--card)}
dialog.sheetWin::backdrop{background:rgba(20,18,15,.5);animation:swFade .22s ease both}
dialog.sheetWin[open]{animation:swIn .26s ${EASE} both}
dialog.sheetWin.closing{animation:swOut .17s ease both}
dialog.sheetWin.closing::backdrop{animation:swFadeOut .17s ease both}
@keyframes swIn{from{opacity:0;transform:translateY(10px) scale(.985)}}
@keyframes swOut{to{opacity:0;transform:translateY(6px) scale(.99)}}
@keyframes swFade{from{opacity:0}}
@keyframes swFadeOut{to{opacity:0}}
.swBox{display:grid;grid-template-rows:auto minmax(0,1fr);height:100%;width:100%;flex:1 1 auto;min-width:0;outline:none}
.swHead{display:flex;align-items:center;gap:12px;padding:9px 12px 9px 16px;border-bottom:1px solid var(--line);min-width:0}
.swId{display:flex;align-items:center;gap:10px;min-width:0;flex:0 1 auto}
.swMetal{display:inline-grid;place-items:center;min-width:34px;height:24px;padding:0 7px;border-radius:7px;background:var(--c);color:#fff;font:700 11px var(--mono);letter-spacing:.04em;text-shadow:0 1px 0 rgba(0,0,0,.18)}
.swTitle{display:flex;flex-direction:column;min-width:0;line-height:1.2}
.swTitle h3{margin:0;font:21px var(--serif);font-weight:500;white-space:nowrap;color:var(--ink)}
.swTitle span{font:10.5px var(--mono);color:var(--ink45);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.swSheets{display:flex;align-items:center;gap:4px;min-width:0;overflow-x:auto;scrollbar-width:none;padding:2px 2px 2px 10px;margin-left:4px;border-left:1px solid var(--line)}
.swSheets::-webkit-scrollbar{display:none}
.swChip{position:relative;display:inline-flex;align-items:center;gap:6px;border:1px solid var(--line);background:var(--card);border-radius:999px;padding:3px 10px 3px 6px;font:600 11.5px var(--sans);color:var(--ink70);cursor:pointer;white-space:nowrap;transition:background .15s,border-color .15s,color .15s,box-shadow .2s}
.swChip i{width:9px;height:9px;border-radius:50%;background:var(--c)}
.swChip:hover{background:var(--paper2)}
.swChip[aria-current=true]{background:var(--velvet);border-color:var(--velvet);color:#fff}
.swChip b{position:absolute;top:-6px;right:-5px;min-width:16px;height:16px;padding:0 4px;border-radius:8px;background:var(--gold2);color:#2a2013;font:700 9.5px/16px var(--mono);text-align:center;box-shadow:0 0 0 2px var(--card);transform:scale(0);transition:transform .22s ${EASE}}
.swChip.lit b{transform:scale(1)}
.swChip.lit{border-color:var(--gold2);box-shadow:0 0 0 3px rgba(202,168,97,.18)}
.swChip.lit[aria-current=true]{box-shadow:0 0 0 3px rgba(202,168,97,.35)}
.swHeadR{margin-left:auto;display:flex;align-items:center;gap:8px;flex:0 0 auto}
.swState{display:inline-flex;align-items:center;gap:6px;border-radius:999px;padding:3px 10px;font:700 9.5px var(--mono);letter-spacing:.08em;text-transform:uppercase;white-space:nowrap;background:var(--paper2);color:var(--ink70)}
.swState i{width:7px;height:7px;border-radius:50%;background:currentColor}
.swState.ready{background:var(--sageSoft);color:#3f5b3c}
.swState.done{background:var(--velvet);color:#f3ead7}
.swIcon{display:inline-grid;place-items:center;width:32px;height:32px;border-radius:9px;border:1px solid transparent;background:transparent;color:var(--ink70);cursor:pointer}
.swIcon svg{width:16px;height:16px}
.swIcon:hover{background:var(--paper2);color:var(--ink)}
.swMenuWrap{position:relative}
.swMenu{position:absolute;right:0;top:calc(100% + 6px);z-index:5;min-width:230px;background:var(--card);border:1px solid var(--line);border-radius:12px;box-shadow:0 18px 50px rgba(30,26,20,.18);padding:6px;display:grid;gap:1px;transform-origin:top right}
.swMenu[hidden]{display:none}
.swMenu button,.swMenu a{display:flex;align-items:center;gap:8px;width:100%;text-align:left;border:0;background:transparent;border-radius:8px;padding:8px 10px;font:13px var(--sans);color:var(--ink);text-decoration:none;cursor:pointer}
.swMenu button:hover,.swMenu a:hover{background:var(--paper2)}
.swMenu button:disabled{color:var(--ink25);cursor:default;background:transparent}
.swMenu .sep{height:1px;background:var(--line);margin:4px 6px}
.swMenu .danger{color:var(--clay)}
.swMenu small{margin-left:auto;font:10px var(--mono);color:var(--ink45)}
.swMenu .swPass{display:flex;gap:6px;padding:6px}
.swMenu .swPass input{flex:1;min-width:0;border:1px solid var(--line);border-radius:8px;padding:6px 8px;font:12.5px var(--sans)}
.swBody{display:grid;grid-template-columns:minmax(0,1fr) 380px;min-height:0}
.swStage{display:grid;grid-template-rows:minmax(0,1fr) auto;min-width:0;min-height:0;background:var(--paper)}
.swPlateBox{position:relative;min-height:0;display:grid;place-items:center;padding:14px 16px 8px;overflow:hidden}
.swPlate{position:relative;border-radius:10px;box-shadow:0 1px 2px rgba(30,26,20,.06),0 12px 34px rgba(30,26,20,.10);background:#fffefb;overflow:hidden;touch-action:none}
.swPlate canvas,.swPlate img.swPv{position:absolute;inset:0;width:100%;height:100%;display:block}
.swPlate img.swPv{object-fit:fill;transition:opacity .35s ease}
.swPlate canvas.swBase{transition:opacity .35s ease}
.swPlate canvas.swFx{cursor:default}
.swPlate.onCharm canvas.swFx{cursor:pointer}
.swVeil{position:absolute;left:50%;bottom:14px;transform:translateX(-50%);display:inline-flex;align-items:center;gap:8px;background:rgba(34,31,27,.86);color:#f3efe6;border-radius:999px;padding:6px 13px 6px 10px;font:12px var(--sans);box-shadow:0 8px 24px rgba(0,0,0,.18);transition:opacity .2s,transform .2s;pointer-events:none;white-space:nowrap}
.swVeil[hidden]{display:inline-flex;opacity:0;transform:translate(-50%,6px)}
.swVeil .owSpin{border-color:rgba(255,255,255,.25);border-top-color:#fff}
.swTip{position:absolute;left:0;top:0;pointer-events:none;background:rgba(28,26,23,.94);color:#f6f1e6;border-radius:10px;padding:7px 10px 8px;min-width:120px;box-shadow:0 10px 28px rgba(0,0,0,.22);opacity:0;transition:opacity .08s linear;z-index:3}
.swTip.on{opacity:1}
.swTip b{display:block;font:600 15px var(--mono);letter-spacing:.02em}
.swTip span{display:block;font:11px var(--sans);color:#cdc4b2;margin-top:1px;white-space:nowrap}
.swTip em{display:flex;align-items:center;gap:6px;font:normal 10.5px var(--sans);color:#e8d9b0;margin-top:4px;white-space:nowrap}
.swTip em i{width:7px;height:7px;border-radius:50%;background:var(--c,#caa861)}
.swStrip{display:flex;align-items:center;gap:14px;padding:7px 16px 9px;font:11.5px var(--sans);color:var(--ink45);min-width:0;white-space:nowrap;overflow:hidden}
.swStrip b{font:600 12px var(--mono);color:var(--ink)}
.swStrip .grow{flex:1}
.swLegend{display:inline-flex;align-items:center;gap:6px}
.swLegend i{width:9px;height:9px;border-radius:50%;background:var(--clay);box-shadow:0 0 0 2px #fff,0 0 0 3px rgba(0,0,0,.08)}
.swLegend i.ok{background:var(--sage)}
.swLegend i.freed{background:transparent;border:1.5px dashed var(--clay);box-shadow:none;border-radius:3px}
.swToggle{border:1px solid var(--line);background:var(--card);border-radius:999px;padding:3px 10px;font:600 11px var(--sans);color:var(--ink70);cursor:pointer}
.swToggle[aria-pressed=true]{background:var(--claySoft);border-color:#e7b9aa;color:#8a3a26}
.swSide{position:relative;border-left:1px solid var(--line);background:var(--card);min-width:0;min-height:0;overflow:hidden}
.swPane{position:absolute;inset:0;display:grid;grid-template-rows:auto minmax(0,1fr) auto;min-height:0}
.swPane[hidden]{display:none}
.swPane[data-pane=sheet]{grid-template-rows:auto auto minmax(0,1fr) auto}
.swPaneHead{padding:12px 14px 10px;border-bottom:1px solid var(--line2);display:grid;gap:9px}
.swFind{position:relative}
.swFind svg{position:absolute;left:10px;top:50%;width:14px;height:14px;transform:translateY(-50%);color:var(--ink45)}
.swFind input{width:100%;border:1px solid var(--line);border-radius:9px;padding:7px 10px 7px 30px;font:13px var(--sans);background:var(--card2);color:var(--ink)}
.swFind input:focus{outline:2px solid rgba(74,107,120,.35);border-color:var(--slate);background:var(--card)}
.swSeg{display:flex;gap:3px;background:var(--paper2);border-radius:9px;padding:3px}
.swSeg button{flex:1;border:0;background:transparent;border-radius:7px;padding:5px 6px;font:600 11.5px var(--sans);color:var(--ink70);cursor:pointer;white-space:nowrap}
.swSeg button i{font-style:normal;font:600 10px var(--mono);color:var(--ink45);margin-left:4px}
.swSeg button[aria-pressed=true]{background:var(--card);color:var(--ink);box-shadow:0 1px 2px rgba(30,26,20,.10)}
.swScroll{overflow:auto;min-height:0;overscroll-behavior:contain}
.swOrders{list-style:none;margin:0;padding:6px 8px 10px}
.swOrd{display:grid;grid-template-columns:auto minmax(0,1fr) auto;align-items:center;gap:4px 10px;padding:8px 10px;border-radius:10px;cursor:pointer;transition:background .12s}
.swOrd:hover,.swOrd.hot{background:var(--goldSoft)}
.swOrd .no{font:600 13px var(--mono);color:var(--ink)}
.swOrd .what{font:11.5px var(--sans);color:var(--ink45);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.swOrd .tags{display:flex;align-items:center;gap:4px;justify-content:flex-end}
.swTag{display:inline-flex;align-items:center;gap:4px;border-radius:999px;padding:1px 7px;font:600 10px var(--mono);white-space:nowrap;background:var(--paper2);color:var(--ink70)}
.swTag i{width:7px;height:7px;border-radius:50%;background:var(--c)}
.swTag.eng{background:var(--claySoft);color:#8a3a26}
.swTag.engOk{background:var(--sageSoft);color:#3f5b3c}
.swTag.eng::before,.swTag.engOk::before{content:"";width:6px;height:6px;border-radius:50%;background:currentColor}
.swNone{padding:28px 18px;text-align:center;color:var(--ink45);font-size:12.5px}
.swFoot{border-top:1px solid var(--line2);padding:10px 14px 12px;display:grid;gap:10px;background:var(--card2)}
.swFoot .row{display:flex;align-items:center;gap:10px;min-width:0}
.swFoot .fLabel{margin:0}
.swFiles{display:flex;flex-wrap:wrap;gap:6px}
.swFiles a.btn{text-decoration:none}
.swQr{position:relative;display:flex;align-items:center;gap:12px}
.swQr .qrTile{width:72px;height:72px}
.swQr .qrTile img{width:112px}
.swQr .cap{display:grid;gap:1px;font:11px var(--sans);color:var(--ink45)}
.swQr .cap b{font:600 12px var(--sans);color:var(--ink70)}
.swQr .busy{display:none;position:absolute;left:0;top:0;width:72px;height:72px;border-radius:6px;background:rgba(255,254,251,.82);place-items:center}
.swQr.remaking .busy{display:grid}
.swNav{display:flex;align-items:center;gap:6px;padding:8px 10px;border-bottom:1px solid var(--line2)}
.swNav .swBackBtn{display:inline-flex;align-items:center;gap:4px;border:0;background:transparent;border-radius:8px;padding:5px 8px 5px 4px;font:600 12.5px var(--sans);color:var(--ink70);cursor:pointer}
.swNav .swBackBtn svg{width:15px;height:15px}
.swNav .swBackBtn:hover{background:var(--paper2);color:var(--ink)}
.swNav .pos{margin-left:auto;font:10.5px var(--mono);color:var(--ink45)}
.swNav .swIcon{width:28px;height:28px}
.swDetail{padding:14px 16px 18px;display:grid;gap:16px;align-content:start}
.swOrderHead{display:grid;gap:3px}
.swOrderHead .rid{font:600 22px var(--mono);letter-spacing:.01em;color:var(--ink)}
.swOrderHead .who{font:12px var(--sans);color:var(--ink45)}
.swSaid{margin-top:6px;font:13px/1.45 var(--serif);color:var(--ink70);border-left:2px solid var(--goldLine);padding:1px 0 1px 9px;white-space:pre-wrap;overflow-wrap:anywhere}
.swPiece{display:grid;grid-template-columns:92px minmax(0,1fr);gap:12px;align-items:center}
.swThumb{width:92px;height:92px;border:1px solid var(--line);border-radius:11px;background:#fff;display:block}
.swPiece .facts{display:grid;gap:3px;min-width:0}
.swPiece .facts b{font:600 13px var(--mono);overflow-wrap:anywhere}
.swPiece .facts span{font:12px var(--sans);color:var(--ink70)}
.swPiece .facts em{font-style:normal;font:12px var(--serif);color:var(--ink);background:var(--paper2);border-radius:6px;padding:2px 7px;justify-self:start;max-width:100%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.swSection{display:grid;gap:8px}
.swSection>.fLabel{margin:0}
.swEng{border:1px solid var(--line);border-radius:12px;padding:10px 12px;display:grid;gap:9px;background:var(--card)}
.swEng .top{display:flex;align-items:center;gap:8px}
.swEng .top b{font:600 13px var(--sans)}
.swEng .top span{margin-left:auto;font:700 9.5px var(--mono);letter-spacing:.08em;text-transform:uppercase;border-radius:999px;padding:2px 8px;background:var(--paper2);color:var(--ink70)}
.swEng[data-state=approve],.swEng[data-state=words]{border-color:#e7b9aa;background:linear-gradient(0deg,rgba(244,227,220,.35),rgba(244,227,220,.35)),var(--card)}
.swEng[data-state=approve] .top span,.swEng[data-state=words] .top span{background:var(--claySoft);color:#8a3a26}
.swEng[data-state=approved] .top span{background:var(--sageSoft);color:#3f5b3c}
.swEng[data-state=none]{padding:8px 12px}
.swEng[data-state=none] .top b{font-weight:500;color:var(--ink45)}
.swEng .pv{display:grid;place-items:center;background:#fff;border:1px solid var(--line2);border-radius:9px;min-height:120px;overflow:hidden}
.swEng .pv canvas,.swEng .pv img{max-width:100%;max-height:180px;display:block}
.swEng .words{font:14px/1.45 var(--serif);white-space:pre-wrap;background:var(--paper2);border-radius:8px;padding:6px 9px}
.swEng .acts{display:flex;gap:8px;flex-wrap:wrap}
.swEng .acts .btn{display:inline-flex;align-items:center;gap:6px}
.swEng .acts .btn svg{width:14px;height:14px}
.swEng .by{font:11px var(--sans);color:var(--ink45)}
.swEng.flash{animation:swFlash 1.4s ease}
@keyframes swFlash{0%{box-shadow:0 0 0 0 rgba(95,122,91,.55)}60%{box-shadow:0 0 0 10px rgba(95,122,91,0)}}
.swTrail{list-style:none;margin:0;padding:0;display:grid;gap:4px}
.swTrail li{display:grid;grid-template-columns:auto minmax(0,1fr) auto;align-items:center;gap:8px;border:1px solid var(--line2);border-radius:10px;padding:7px 9px;cursor:pointer;transition:background .12s,border-color .12s}
.swTrail li:hover{background:var(--paper2)}
.swTrail li.cur{border-color:var(--gold2);background:var(--goldSoft);cursor:default}
.swTrail li.off{cursor:default;opacity:.8}
.swTrail .sku{font:600 12px var(--mono);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.swTrail .sku small{font:11px var(--sans);color:var(--ink45);margin-left:6px}
.swTrail .where{display:inline-flex;align-items:center;gap:5px;font:11px var(--sans);color:var(--ink70);white-space:nowrap}
.swTrail .where i{width:8px;height:8px;border-radius:50%;background:var(--c)}
.swTrail .where svg{width:13px;height:13px;color:var(--ink45)}
.swTrail .n{font:600 10px var(--mono);color:var(--ink45);width:18px;text-align:center}
.swMsgs{border:1px solid var(--line);border-radius:12px;overflow:hidden;display:flex;flex-direction:column;height:380px;background:var(--paper)}
.swMsgs .owTabs{background:var(--card)}
.swMsgs>div:not(.owTabs){flex:1 1 auto;min-height:0;display:flex;flex-direction:column}
.swMsgs>div[hidden]{display:none!important}
.swMsgs .cmEng{flex:1 1 auto;min-height:0}
.swWait{display:flex;align-items:center;gap:8px;font:12px var(--sans);color:var(--ink45)}
.swReturn{display:inline-flex;align-items:center;gap:6px;margin-left:8px;height:30px;flex:0 1 auto;min-width:0;background:var(--velvet);color:#f6f1e6;border-radius:999px;padding:3px 4px 3px 3px;box-shadow:0 6px 18px rgba(20,16,10,.22);font:12px var(--sans);animation:swDrop .34s ${EASE} both;white-space:nowrap}
.swReturn[hidden]{display:none}
@keyframes swDrop{from{opacity:0;transform:translateY(-8px) scale(.94)}}
.swReturn button{border:0;cursor:pointer;font:600 12px var(--sans)}
.swReturn .go{display:inline-flex;align-items:center;gap:5px;background:var(--gold2);color:#2a2013;border-radius:999px;padding:4px 11px 4px 7px;min-width:0}
.swReturn .go svg{width:13px;height:13px;flex:none}
.swReturn .st{display:inline-flex;align-items:center;gap:6px;color:#cdc4b2;min-width:0;overflow:hidden;text-overflow:ellipsis}
.swReturn .st i{width:7px;height:7px;border-radius:50%;background:var(--clay);flex:none;animation:swBeat 1.6s ease-in-out infinite}
@keyframes swBeat{50%{opacity:.35}}
.swReturn.ok .st{color:#cfe6c9}.swReturn.ok .st i{background:#7fb877;animation:none}
.swReturn .x{background:transparent;color:#ada393;width:24px;height:24px;border-radius:50%;display:grid;place-items:center;flex:none}
.swReturn .x:hover{background:rgba(255,255,255,.1);color:#fff}
.swReturn .x svg{width:12px;height:12px}
.swOffBtn{justify-self:start;display:inline-flex;align-items:center;gap:6px;border:1px solid var(--line);background:var(--card);color:var(--ink70);border-radius:999px;padding:4px 11px 4px 9px;font:500 12px var(--sans);cursor:pointer;transition:border-color .12s,color .12s,background .12s}
.swOffBtn:hover{border-color:#e7b9aa;color:#8a3a26;background:var(--claySoft)}
.swOffBtn svg{width:14px;height:14px}
.swOff{border:1px solid #e7b9aa;border-radius:12px;background:linear-gradient(0deg,rgba(244,227,220,.28),rgba(244,227,220,.28)),var(--card);padding:11px 12px;display:grid;gap:10px;animation:swDrop .22s cubic-bezier(.2,.8,.2,1) both}
.swOff h4{margin:0;font:600 13px var(--sans);color:var(--ink)}
.swOff .pick{display:grid;gap:6px}
.swOff label.opt{display:grid;grid-template-columns:auto minmax(0,1fr);gap:2px 9px;align-items:start;border:1px solid var(--line2);border-radius:10px;padding:8px 10px;background:var(--card);cursor:pointer;transition:border-color .12s,box-shadow .12s}
.swOff label.opt:has(input:checked){border-color:#c98a74;box-shadow:0 0 0 2px rgba(176,86,63,.12)}
.swOff label.opt input{margin:2px 0 0;accent-color:var(--clay)}
.swOff label.opt b{font:600 12.5px var(--sans)}
.swOff label.opt small{grid-column:2;font:11.5px/1.4 var(--sans);color:var(--ink45)}
.swOff .why{display:flex;flex-wrap:wrap;gap:6px;align-items:center}
.swOff .why .fLabel{margin:0 4px 0 0}
.swOff .why button{border:1px solid var(--line);background:var(--card);border-radius:999px;padding:3px 10px;font:12px var(--sans);color:var(--ink70);cursor:pointer}
.swOff .why button[aria-pressed=true]{border-color:#c98a74;background:var(--claySoft);color:#8a3a26}
.swOff .stay{font:11.5px/1.45 var(--sans);color:#8a3a26;background:rgba(255,255,255,.6);border-radius:8px;padding:6px 9px}
.swOff .note{font:11.5px/1.45 var(--sans);color:var(--ink45)}
.swOff .acts{display:flex;gap:8px;align-items:center}
.swOff .acts .btn{display:inline-flex;align-items:center;gap:6px}
.swWork{border:1px solid var(--line);border-radius:12px;background:var(--card);padding:10px 12px;display:grid;gap:7px;animation:swDrop .22s cubic-bezier(.2,.8,.2,1) both}
.swWork h4{margin:0;font:600 13px var(--sans);display:flex;align-items:center;gap:8px}
.swWork ol{list-style:none;margin:0;padding:0;display:grid;gap:5px}
.swWork li{display:grid;grid-template-columns:16px minmax(0,1fr);gap:8px;align-items:center;font:12px/1.4 var(--sans);color:var(--ink45)}
.swWork li i{width:14px;height:14px;border-radius:50%;border:1.5px solid var(--line);display:grid;place-items:center}
.swWork li.now{color:var(--ink)}
.swWork li.now i{border-color:var(--gold2);border-top-color:transparent;animation:swSpin .8s linear infinite}
.swWork li.ok{color:var(--ink70)}
.swWork li.ok i{border-color:var(--sage);background:var(--sage)}
.swWork li.ok i::after{content:"";width:6px;height:3px;border:solid #fff;border-width:0 0 1.5px 1.5px;transform:translateY(-1px) rotate(-45deg)}
.swWork li.bad{color:#8a3a26}
.swWork li.bad i{border-color:var(--clay);background:var(--clay)}
.swWork .note{font:11.5px/1.45 var(--sans);color:var(--ink45)}
.swWork.done{border-color:#b9cdb5;background:linear-gradient(0deg,rgba(221,233,218,.35),rgba(221,233,218,.35)),var(--card)}
.swWork.failed{border-color:#e7b9aa}
.swSheetMsg{margin:10px 14px 0}
@keyframes swSpin{to{transform:rotate(360deg)}}
{grid-template-columns:minmax(0,1fr)}.swSide{border-left:0;border-top:1px solid var(--line);min-height:46vh}.swBox{overflow:auto}.swSheets{display:none}}
@media (prefers-reduced-motion:reduce){dialog.sheetWin[open],dialog.sheetWin.closing,dialog.sheetWin::backdrop,.swReturn{animation:none}}
`;

  /* ── state ── */
  const W = {
    dlg: null, el: {}, id: null, rec: null, live: null, st: null, pieces: [], byId: new Map(), byPool: new Map(), orders: new Map(),
    set: null, setSheets: [], sel: null, hover: null, view: "sheet", q: "", filter: "all", token: 0, geom: false, view0: null,
    k: 1, R: 0, dpr: 1, showBacks: true, fx: [], raf: 0, pools: new Map(), trailFor: null, from: null, ro: null, freed: [], work: null
  };

  function build() {
    if (W.dlg) return;
    const style = h("style"); style.textContent = STYLE; document.head.appendChild(style);
    const d = h("dialog", "sheetWin"); d.setAttribute("aria-label", "Sheet");
    d.innerHTML = `<div class="swBox" tabindex="-1" autofocus>
      <header class="swHead">
        <div class="swId"><span class="swMetal" data-r="metal"></span><div class="swTitle"><h3 data-r="title">Sheet</h3><span data-r="sub"></span></div></div>
        <nav class="swSheets" data-r="sheets" aria-label="Sheets in this set"></nav>
        <div class="swHeadR">
          <span class="swState" data-r="state"><i></i><span></span></span>
          <button class="btn ghost sm" data-r="done" hidden>Mark completed</button>
          <div class="swMenuWrap"><button class="swIcon" data-r="moreBtn" aria-haspopup="menu" aria-expanded="false" title="Files and more">${ICON.more}</button><div class="swMenu" data-r="menu" role="menu" hidden></div></div>
          <button class="swIcon" data-r="close" title="Close (Esc)" aria-label="Close">${ICON.close}</button>
        </div>
      </header>
      <div class="swBody">
        <section class="swStage">
          <div class="swPlateBox" data-r="plateBox"><div class="swPlate" data-r="plate"><img class="swPv" data-r="pv" alt="" crossorigin="anonymous"><canvas class="swBase" data-r="base"></canvas><canvas class="swFx" data-r="fx"></canvas><div class="swTip" data-r="tip"></div></div>
            <div class="swVeil" data-r="veil" hidden><span class="owSpin"></span><span data-r="veilText">Opening the sheet…</span></div></div>
          <footer class="swStrip" data-r="strip"></footer>
        </section>
        <aside class="swSide" data-r="side">
          <div class="swPane" data-pane="sheet">
            <div class="swPaneHead"><label class="swFind">${ICON.search}<input data-r="find" type="search" placeholder="Find an order or SKU on this sheet" autocomplete="off" spellcheck="false"></label><div class="swSeg" data-r="seg" role="group" aria-label="Show"></div></div>
            <div class="swSheetMsg" data-r="work" hidden></div>
            <div class="swScroll"><ol class="swOrders" data-r="orders"></ol></div>
            <div class="swFoot" data-r="foot"></div>
          </div>
          <div class="swPane" data-pane="piece" hidden>
            <div class="swNav"><button class="swBackBtn" data-r="toSheet">${ICON.back}<span>All orders</span></button><span class="pos" data-r="pos"></span><button class="swIcon" data-r="prev" title="Previous charm (←)" aria-label="Previous charm">${ICON.back}</button><button class="swIcon" data-r="next" title="Next charm (→)" aria-label="Next charm">${ICON.next}</button></div>
            <div class="swScroll" data-r="pieceScroll"><div class="swDetail" data-r="detail"></div></div>
            <div></div>
          </div>
        </aside>
      </div>
    </div>`;
    document.body.appendChild(d);
    W.dlg = d; d.querySelectorAll("[data-r]").forEach(n => { W.el[n.dataset.r] = n; });
    const E = W.el;
    E.close.onclick = () => close();
    d.addEventListener("cancel", e => { e.preventDefault(); if (!E.menu.hidden) return menu(false); if (W.view === "piece") return showSheetPane(); close(); });
    d.addEventListener("close", () => cleanup());
    d.addEventListener("click", e => { if (e.target === d) close(); if (!E.menu.hidden && !e.target.closest(".swMenuWrap")) menu(false); });
    E.moreBtn.onclick = () => menu(E.menu.hidden);
    E.find.oninput = () => { W.q = E.find.value.trim().toLowerCase(); renderOrders(); paintFx(); };
    E.toSheet.onclick = () => showSheetPane();
    E.prev.onclick = () => step(-1);
    E.next.onclick = () => step(1);
    d.addEventListener("keydown", e => {
      if (e.target.closest("input,textarea,[contenteditable]")) return;
      if (W.view === "piece" && (e.key === "ArrowLeft" || e.key === "ArrowRight")) { e.preventDefault(); step(e.key === "ArrowLeft" ? -1 : 1); }
    });
    const fx = E.fx;
    fx.addEventListener("pointermove", e => { W.pointer = e; if (!W.moveFrame) W.moveFrame = requestAnimationFrame(() => { W.moveFrame = 0; onMove(W.pointer); }); });
    fx.addEventListener("pointerleave", () => { setHover(null); tip(null); });
    fx.addEventListener("click", e => { const p = hitAt(e); if (p) selectPiece(p, { from: "canvas" }); else if (W.view === "piece") showSheetPane(); });
    W.ro = new ResizeObserver(() => { if (W.dlg.open) fitPlate(); });
    W.ro.observe(E.plateBox);
  }

  /* ── open / close ── */
  // the Library card that was pressed: the window grows out of its preview, and shrinks back into it
  let pressed = null;
  document.addEventListener("pointerdown", e => { const c = e.target.closest && e.target.closest(".libCard[data-id]"); if (c) pressed = { id: c.dataset.id, at: Date.now() }; }, true);
  const cardPv = id => { const c = document.querySelector(`#libBody .libCard[data-id="${CSS.escape(id)}"]`); return c && c.offsetParent ? (c.querySelector("img.pv") || c) : null; };

  async function open(id, opts = {}) {
    build();
    const E = W.el, fresh = !W.dlg.open, tok = ++W.token;
    const from = opts.fromRect || (pressed && pressed.id === id && Date.now() - pressed.at < 1500 && cardPv(id)?.getBoundingClientRect()) || null;
    pressed = null;
    if (fresh) resetView();
    W.id = id; W.rec = null; W.live = null; W.geom = false; W.pieces = []; W.byId = new Map(); W.byPool = new Map(); W.orders = new Map();
    W.sel = null; W.hover = null; W.fx = []; W.set = W.set && opts.keepSet ? W.set : null;
    if (!opts.keepWork) { W.freed = []; W.work = null; } else W.freed = (opts.freed || W.freed).map(g => Object.assign({}, g, { t0: 0 }));
    renderWork();
    const lib = (S.library.rows || []).find(r => r.id === id) || null;
    head(lib || { id, metal: "gold" }, true);
    // (read again after pieces came off it: the plate on screen stays until the saved sheet is drawn over it)
    const again = opts.keepWork && !fresh && W.dlg.open;
    if (!again) {
      E.pv.removeAttribute("src"); E.pv.style.opacity = "1"; E.base.style.opacity = "0";
      const url = lib?.outputs?.preview?.url; if (url) E.pv.src = cors(url);
      if (lib?.stock) { W.st = stockOf(lib); fitPlate(); }
      veil("Opening the sheet…");
    }
    E.strip.innerHTML = ""; E.orders.innerHTML = `<li class="swNone"><div class="swWait" style="justify-content:center"><span class="owSpin"></span>Reading the sheet…</div></li>`; E.foot.innerHTML = ""; E.seg.innerHTML = "";
    if (fresh) {
      W.dlg.classList.remove("closing");
      try { W.dlg.showModal(); } catch (_) { W.dlg.setAttribute("open", ""); }
      if (from) grow(from);
    } else if (opts.slide) slidePlate(opts.slide);
    try {
      const r = await api("charmNestLibrary", { op: "getSheet", id }, { quiet: true });
      if (tok !== W.token) return;
      const rec = r.sheet; if (!rec) throw new Error("This sheet is no longer in the Library.");
      if (window.LaserReview) LaserReview.record(rec);
      W.rec = rec; W.st = stockOf(rec); W.live = liveOf(id);
      head(rec, false); indexPieces(); fitPlate(); renderStrip(); renderSheetPane(); renderFoot(); renderMenu();
      if (!rec.outputs?.preview?.url && !W.live) E.pv.removeAttribute("src"); else if (rec.outputs?.preview?.url && !E.pv.getAttribute("src")) E.pv.src = cors(rec.outputs.preview.url);
      loadSet(tok);
      await loadGeometry(tok);
      if (tok !== W.token) return;
      veil(null);
      if (opts.select) { const p = W.byPool.get(opts.select) || W.byId.get(opts.select); if (p) selectPiece(p, { from: opts.from || "open", flash: opts.flash }); }
    } catch (e) {
      if (tok !== W.token) return;
      veil(null); E.orders.innerHTML = `<li class="swNone"><b>This sheet could not open.</b><br>${esc(e.message)}<br><br><button class="btn ghost sm" data-r2="retry">Try again</button></li>`;
      E.orders.querySelector("[data-r2=retry]").onclick = () => open(id, opts);
    }
  }
  function resetView() {
    W.view = "sheet"; W.q = ""; W.filter = "all"; W.el.find.value = "";
    W.el.side.querySelector('[data-pane="sheet"]').hidden = false; W.el.side.querySelector('[data-pane="piece"]').hidden = true;
  }
  function grow(from) {
    const plate = W.el.plate;
    requestAnimationFrame(() => {
      const to = plate.getBoundingClientRect(); if (!to.width || !from.width) return;
      const sx = from.width / to.width, sy = from.height / to.height, dx = from.left - to.left, dy = from.top - to.top;
      animate(plate, [{ transformOrigin: "0 0", transform: `translate(${dx}px,${dy}px) scale(${sx},${sy})`, borderRadius: "6px" }, { transformOrigin: "0 0", transform: "none", borderRadius: "10px" }], 360, { fill: "none" });
      animate(W.el.side, [{ opacity: 0, transform: "translateX(18px)" }, { opacity: 1, transform: "none" }], 320, { delay: 90, fill: "backwards" });
    });
  }
  async function close() {
    const d = W.dlg; if (!d || !d.open || d.classList.contains("closing")) return;
    W.token++; menu(false);
    const pv = W.id && cardPv(W.id), plate = W.el.plate;
    if (pv && !still()) {
      const to = pv.getBoundingClientRect(), fr = plate.getBoundingClientRect();
      if (to.width && fr.width && to.bottom > 0 && to.top < innerHeight) {
        d.classList.add("closing");
        await animate(plate, [{ transformOrigin: "0 0", transform: "none" }, { transformOrigin: "0 0", transform: `translate(${to.left - fr.left}px,${to.top - fr.top}px) scale(${to.width / fr.width},${to.height / fr.height})` }], 200, { easing: "cubic-bezier(.4,0,.6,1)" });
      }
    }
    if (!still() && !d.classList.contains("closing")) { d.classList.add("closing"); await new Promise(r => setTimeout(r, 160)); }
    try { d.close(); } catch (_) { d.removeAttribute("open"); }
  }
  function cleanup() {
    W.dlg.classList.remove("closing"); W.token++;
    W.el.plate.getAnimations?.().forEach(a => a.cancel());
    cancelAnimationFrame(W.raf); W.raf = 0; W.fx = [];
    tip(null);
    // the messages panes are the Engrave card's too: they go back to it
    W.el.detail.innerHTML = "";
  }

  /* ── header ── */
  function stockOf(r) {
    const st = r.stock || {}; const wPt = st.wPt || (st.wIn || 0) * PT_PER_IN, hPt = st.hPt || (st.hIn || 0) * PT_PER_IN;
    return wPt > 0 && hPt > 0 ? { wPt, hPt } : stockFor(r.metal);
  }
  const setNoOf = r => r.draft ? 0 : r.setSeq || +((/_Set-(\d+)/.exec(r.folder || r.fileBase || "") || [])[1]) || 0;
  const sheetNoOf = r => r.sheetIndex || +((/_Sheet-(\d+)/.exec(r.folder || r.fileBase || "") || [])[1]) || r.page || 1;
  function head(r, prelim) {
    const E = W.el, m = r.metal;
    E.metal.textContent = CODE[m] || labelOf(m); E.metal.style.setProperty("--c", colorOf(m));
    E.title.textContent = `Sheet ${sheetNoOf(r)}`;
    const sn = setNoOf(r), started = r.cardStartedAt || r.createdAt;
    const date = started ? new Date(started).toLocaleDateString(undefined, { month: "short", day: "numeric" }) : (r.day || "");
    const working = r.draft || /_working_/.test(r.fileBase || "");
    E.sub.textContent = [labelOf(m), sn ? `Set ${sn}` : "", date, working ? "filling · joins a set when full" : r.folder || r.fileBase || ""].filter(Boolean).join(" · ");
    E.sub.title = r.folder || r.id || "";
    W.dlg.setAttribute("aria-label", r.folder || `Sheet ${sheetNoOf(r)}`);
    const st = E.state.querySelector("span");
    if (prelim) { E.state.className = "swState"; st.textContent = "…"; }
    else {
      const done = !!(r.laserDoneAt || (window.LibraryDone && LibraryDone.isDone && LibraryDone.isDone(r)));
      const ready = !done && window.LaserReview && LaserReview.sheet(r).ready;
      E.state.className = "swState" + (done ? " done" : ready ? " ready" : "");
      st.textContent = done ? "Completed" : ready ? "Ready for laser" : (r.releaseFull || r.intakeFinalized) ? "Released" : "In progress";
      E.done.hidden = !window.LibraryDone || !window.LibraryDone.mark;
      E.done.textContent = done ? "Move back to current" : "Mark completed";
      E.done.onclick = () => markDone(!done);
    }
    if (!prelim || !W.setSheets.length) renderSheetChips();
  }
  async function markDone(done) {
    const b = W.el.done, id = W.id; b.disabled = true; const was = b.textContent; b.innerHTML = `<span class="spin"></span>${done ? "Completing…" : "Moving back…"}`;
    try {
      await LibraryDone.mark("sheet", id, done);
      if (W.id !== id) return;
      W.rec.laserDoneAt = done ? Date.now() : null; head(W.rec, false);
      animate(W.el.state, [{ transform: "scale(.8)", opacity: .4 }, { transform: "none", opacity: 1 }], 320);
    } catch (e) { toast("Could not mark the sheet: " + e.message, "bad", 6000); b.textContent = was; }
    finally { b.disabled = false; }
  }

  /* ── the set's sheets, as tabs ── */
  async function loadSet(tok) {
    const rec = W.rec; if (!rec.setId) { W.set = null; W.setSheets = []; renderSheetChips(); return; }
    try {
      const live = window.Sets && [...(B.sets?.values?.() || [])].find(x => x.setId === rec.setId);
      let set = live || (W.set && W.set.setId === rec.setId ? W.set : null);
      if (!set) { const r = await api("charmNestLibrary", { op: "setGet", setId: rec.setId }, { quiet: true }); set = r.set; }
      if (tok !== W.token || !set) return;
      W.set = set;
      W.setSheets = (set.sheetIds || []).map(sid => sheetInfo(sid, set)).filter(Boolean)
        .sort((a, b) => (a.metal || "").localeCompare(b.metal || "") || a.n - b.n);
      renderSheetChips(); if (W.sel) lightChips(W.sel.rid);
      if (W.view === "sheet") renderSheetPane(); else if (W.sel) renderTrail(W.sel);
    } catch (e) { console.warn("sheet window: set", e); }
  }
  function sheetInfo(sid, set) {
    const lib = (S.library.rows || []).find(r => r.id === sid);
    if (lib) return { id: sid, metal: lib.metal, n: sheetNoOf(lib), name: lib.folder || lib.fileBase || sid };
    const lf = (set.labelFiles || []).find(f => f.sheetId === sid), name = lf?.sheet || "";
    const code = (/^([A-Z0-9]+)_/.exec(name) || [])[1];
    const live = allSheets().find(p => p.sheetId === sid);
    return { id: sid, metal: live?.metal || METAL_OF_CODE[code] || "gold", n: live?.sheetIndex || +((/_Sheet-(\d+)/.exec(name) || [])[1]) || 1, name: name || sid };
  }
  function renderSheetChips() {
    const E = W.el, list = W.setSheets.length ? W.setSheets : (W.rec ? [{ id: W.rec.id, metal: W.rec.metal, n: sheetNoOf(W.rec) }] : []);
    E.sheets.hidden = list.length < 2;
    E.sheets.innerHTML = list.map(s => `<button type="button" class="swChip" data-sheet="${esc(s.id)}" style="--c:${colorOf(s.metal)}"${s.id === W.id ? ' aria-current="true"' : ""} title="${esc(s.name || "")}"><i></i>${esc(CODE[s.metal] || "")} ${s.n}<b></b></button>`).join("");
    E.sheets.querySelectorAll("[data-sheet]").forEach(b => b.onclick = () => { if (b.dataset.sheet !== W.id) switchSheet(b.dataset.sheet); });
  }
  function lightChips(rid) {
    const counts = new Map();
    if (rid && W.set?.orders?.[rid]) for (const l of linesOf(W.set.orders[rid])) for (const c of l.copies || []) counts.set(c.sheetId, (counts.get(c.sheetId) || 0) + 1);
    if (rid) for (const p of W.pools.get(rid) || []) if (p.sheetId && !counts.has(p.sheetId) && !["abandoned", "superseded"].includes(p.state)) counts.set(p.sheetId, 1);
    W.el.sheets.querySelectorAll("[data-sheet]").forEach(b => { const n = counts.get(b.dataset.sheet) || 0; b.classList.toggle("lit", n > 0); b.querySelector("b").textContent = n || ""; });
  }
  const linesOf = o => Array.isArray(o?.lines) ? o.lines : Object.values(o?.lines || {});
  function switchSheet(id, opts = {}) {
    const i = W.setSheets.findIndex(s => s.id === id), j = W.setSheets.findIndex(s => s.id === W.id);
    open(id, Object.assign({ slide: i >= 0 && j >= 0 && i < j ? "right" : "left", keepSet: true }, opts));
  }
  function slidePlate(dir) {
    const d = dir === "right" ? 1 : -1;
    animate(W.el.plate, [{ opacity: .15, transform: `translateX(${-d * 36}px)` }, { opacity: 1, transform: "none" }], 300);
  }

  /* ── the record's pieces ── */
  function indexPieces() {
    const rec = W.rec, saved = new Map((rec.charms || []).map(c => [c.id, c]));
    const pieces = [];
    for (const p of rec.placements || []) {
      const c = saved.get(p.id) || {};
      const name = c.name || p.name || "", poolId = c.poolId || null;
      const rid = String(c.order || "").split("/")[0] || (poolId ? poolId.split("_")[0] : "");
      const m = / · (\d+)\/(\d+)$/.exec(name), copy = poolId ? +poolId.split("_").pop() || 1 : m ? +m[1] : 1;
      pieces.push({ id: p.id, p: { cxPt: +p.cxPt, cyPt: +p.cyPt, angle: +p.angle || 0, scale: +p.scale || 1, wPt: +p.wPt || 10, hPt: +p.hPt || 10 },
        c: null, poolId, rid: /^\d+$/.test(rid) ? rid : "", sku: c.sku || (name.split(" · ")[1] || "").trim() || c.layer || "", name, copy, qty: m ? +m[2] : 1,
        tx: poolId ? poolId.split("_")[1] : "", sourceId: c.sourceId, index: c.index, hash: c.hash, thumb: c.thumbUrl || null });
    }
    W.pieces = pieces; W.byId = new Map(pieces.map(x => [x.id, x])); W.byPool = new Map(pieces.filter(x => x.poolId).map(x => [x.poolId, x]));
    W.orders = new Map(); for (const x of pieces) { const k = x.rid || "—"; if (!W.orders.has(k)) W.orders.set(k, []); W.orders.get(k).push(x); }
    for (const x of pieces) x.eng = engOf(x);
  }
  const liveOf = id => allSheets().find(p => p.sheetId === id && p.placements.length) || null;

  /* ── geometry: the live page's charms, or the master designs placed as recorded ── */
  const fileGeoms = new Map();
  async function loadGeometry(tok) {
    const rec = W.rec, live = W.live;
    if (live) {
      const byId = new Map(live.charms.map(c => [c.id, c]));
      for (const x of W.pieces) x.c = byId.get(x.id) || null;
      if (W.pieces.some(x => !x.c)) { W.live = null; for (const x of W.pieces) x.c = null; }
      else return geometryReady(tok);
    }
    const srcs = rec.sources || [], byId = new Map(srcs.map(s => [s.id, s])), got = new Map();
    const needed = [...new Set(W.pieces.map(x => x.sourceId).filter(Boolean))].map(id => byId.get(id)).filter(Boolean);
    let done = 0; const failed = [];
    veil(`Drawing the live sheet · design 0 of ${needed.length}`);
    const one = async s => {
      try { got.set(s.id, await sourceGeom(s)); } catch (e) { failed.push(s.name || s.id); console.warn("sheet window: design", s.name, e); }
      done++; if (tok === W.token) veil(`Drawing the live sheet · design ${done} of ${needed.length}`);
    };
    const queue = needed.slice(), lanes = Array.from({ length: Math.min(4, queue.length) }, async () => { while (queue.length) await one(queue.shift()); });
    await Promise.all(lanes);
    if (tok !== W.token) return;
    for (const x of W.pieces) {
      const g = got.get(x.sourceId); if (!g) continue;
      if (g.pool) { const c = Pool.cloneCharm(g.base, x.id); Object.assign(c, { name: x.name, order: x.rid || c.order, poolId: x.poolId, metal: rec.metal }); x.c = c; }
      else { const base = g.charms[x.index] && (!x.hash || g.charms[x.index].hash === x.hash) ? g.charms[x.index] : g.charms.find(c => c.hash === x.hash); if (base) x.c = Object.assign({}, base, { id: x.id, name: x.name }); }
    }
    if (failed.length) toast(`${failed.length} design${failed.length === 1 ? "" : "s"} could not be read (${failed.slice(0, 3).join(", ")}); ${failed.length === 1 ? "it shows" : "they show"} as outlines`, "bad", 7000);
    geometryReady(tok);
  }
  async function sourceGeom(s) {
    if (s.pool && s.sku) {
      const entry = Master.entryFor(s.sku) || await Master.fetchEntry(s.sku); if (!entry) throw new Error(`${s.sku} is no longer in the master library`);
      const size = Object.entries(entry.sizes || {}).find(([, v]) => v && v.aiPath === s.path)?.[0] || null;
      const src = await Pool.masterCharm(entry, size); return { pool: true, base: src.charms[0], src };
    }
    const key = s.path || s.url; if (!key) throw new Error("the file was never saved");
    if (!fileGeoms.has(key)) fileGeoms.set(key, (async () => {
      const bytes = await CharmNestAssets.bytes(s.url || (await api("charmNestOutput", { op: "url", path: s.path })).url);
      const parsed = await CharmNestPDF.parseSource(bytes, s.name || "sheet.ai");
      const g = await (CharmNestPDF.groupCharmsAsync || CharmNestPDF.groupCharms)(parsed, { minPt: +S.settings.minPt || 6 });
      await CharmNestPDF.buildSilhouettes(parsed, g.charms, +S.settings.silhouetteRes || 6);
      return { pool: false, charms: g.charms };
    })().catch(e => { fileGeoms.delete(key); throw e; }));
    if (fileGeoms.size > 12) fileGeoms.delete(fileGeoms.keys().next().value);
    return fileGeoms.get(key);
  }
  function geometryReady(tok) {
    if (tok !== W.token) return;
    W.geom = true; paintBase();
    W.el.base.style.opacity = "1";
    if (W.pieces.every(x => x.c)) W.el.pv.style.opacity = "0";
    paintFx();
  }

  /* ── the plate: base (the charms as the Nest draws them) and fx (hover, focus, links, marks) ── */
  function fitPlate() {
    const E = W.el, st = W.st; if (!st) return;
    const box = E.plateBox.getBoundingClientRect(); if (box.width < 40 || box.height < 40) return;
    const availW = box.width - 32, availH = box.height - 22;
    const R = Math.round(Math.max(14, Math.min(22, availW * 0.022)));
    let w = availW, hh = R + (w - R) * st.hPt / st.wPt; if (hh > availH) { hh = availH; w = R + (hh - R) * st.wPt / st.hPt; }
    w = Math.floor(w); hh = Math.floor(hh);
    E.plate.style.width = w + "px"; E.plate.style.height = hh + "px";
    const dpr = Math.min(2.5, devicePixelRatio || 1);
    W.dpr = dpr; W.R = R * dpr; W.k = (w * dpr - W.R) / st.wPt; W.cssW = w; W.cssH = hh;
    for (const cv of [E.base, E.fx]) { const cw = Math.round(w * dpr), ch = Math.round(hh * dpr); if (cv.width !== cw || cv.height !== ch) { cv.width = cw; cv.height = ch; } }
    // the saved preview is the plate without rulers: it sits where the plate sits
    E.pv.style.left = R + "px"; E.pv.style.top = R + "px"; E.pv.style.width = (w - R) + "px"; E.pv.style.height = (hh - R) + "px"; E.pv.style.inset = "auto";
    if (W.geom) paintBase(); paintFx();
  }
  function tx0(c) { const cx = c.centerPt[0], cy = c.centerPt[1], k = W.k; return (x, y) => [(x - cx) * k, (cy - y) * k]; }
  function withPiece(ctx, x, fn) { const p = x.p, k = W.k; ctx.save(); ctx.translate(W.R + p.cxPt * k, W.R + p.cyPt * k); ctx.rotate(p.angle * Math.PI / 180); if (p.scale) ctx.scale(p.scale, p.scale); fn(); ctx.restore(); }
  function outlinePath(ctx, x, holes) {
    const c = x.c; ctx.beginPath();
    if (!c) { const w = x.p.wPt * W.k / (x.p.scale || 1), hh = x.p.hPt * W.k / (x.p.scale || 1); ctx.roundRect ? ctx.roundRect(-w / 2, -hh / 2, w, hh, 3 * W.dpr) : ctx.rect(-w / 2, -hh / 2, w, hh); return; }
    const t = tx0(c); CharmNestPDF.pathToCanvas(ctx, c.outline, t); if (holes) for (const m of cutLinesOf(c)) CharmNestPDF.pathToCanvas(ctx, m, t);
  }
  function paintBase() {
    const cv = W.el.base, ctx = cv.getContext("2d"), st = W.st, R = W.R, k = W.k; if (!st) return;
    const Wp = cv.width - R, Hp = cv.height - R;
    ctx.setTransform(1, 0, 0, 1, 0, 0); ctx.clearRect(0, 0, cv.width, cv.height);
    ctx.fillStyle = "#fffefb"; ctx.fillRect(0, 0, cv.width, cv.height);
    if (R) drawRulers(ctx, R, k, Wp, Hp);
    ctx.save(); ctx.translate(R, R);
    ctx.strokeStyle = "rgba(176,86,63,.9)"; ctx.lineWidth = Math.max(1, .5 * k); ctx.strokeRect(.5, .5, Wp - 1, Hp - 1);
    const ins = (+S.settings.insetPt || 0) * k; ctx.setLineDash([4 * W.dpr, 4 * W.dpr]); ctx.strokeStyle = "rgba(147,140,128,.35)"; ctx.lineWidth = 1; ctx.strokeRect(ins, ins, Wp - 2 * ins, Hp - 2 * ins); ctx.setLineDash([]);
    ctx.restore();
    for (const x of W.pieces) {
      if (x.gone) continue;
      withPiece(ctx, x, () => {
        ctx.fillStyle = "rgba(200,162,78,.10)"; outlinePath(ctx, x, true); ctx.fill("evenodd");
        if (x.c) CharmNestPDF.drawCharm(ctx, x.c, tx0(x.c), k);
        else { ctx.strokeStyle = "rgba(60,54,46,.5)"; ctx.lineWidth = W.dpr; outlinePath(ctx, x); ctx.stroke(); }
      });
    }
  }
  function focusSet() {
    // what the pointer or the selection is about: one charm, and the rest of its order on this sheet
    const f = W.hover || W.sel; if (!f) return null;
    const mates = f.rid ? (W.orders.get(f.rid) || []) : [f];
    return { f, mates };
  }
  function paintFx(now) {
    const cv = W.el.fx, ctx = cv.getContext("2d"), k = W.k, R = W.R; if (!W.st) return;
    ctx.setTransform(1, 0, 0, 1, 0, 0); ctx.clearRect(0, 0, cv.width, cv.height);
    const q = W.q, matches = q ? W.pieces.filter(x => matchesQ(x, q)) : null;
    const fs = focusSet();
    if (W.geom && (fs || (matches && matches.length < W.pieces.length))) {
      // everything else steps back, the charms in hand come forward in their own lines
      const keep = new Set((fs ? fs.mates : []).concat(matches || []));
      ctx.fillStyle = "rgba(255,254,251,.62)"; ctx.fillRect(R, R, cv.width - R, cv.height - R);
      for (const x of keep) { if (x.gone) continue; withPiece(ctx, x, () => { const main = fs && x === fs.f, mate = fs && fs.mates.includes(x);
        ctx.fillStyle = main ? "rgba(202,168,97,.42)" : mate ? "rgba(202,168,97,.22)" : "rgba(74,107,120,.14)"; outlinePath(ctx, x, true); ctx.fill("evenodd");
        if (x.c) CharmNestPDF.drawCharm(ctx, x.c, tx0(x.c), k);
        outlinePath(ctx, x); ctx.strokeStyle = main ? "#a9823f" : mate ? "rgba(169,130,63,.75)" : "rgba(74,107,120,.7)"; ctx.lineWidth = (main ? 2 : 1.4) * W.dpr; ctx.stroke(); }); }
    }
    // the pieces of one order on this sheet, joined
    if (fs && fs.mates.length > 1 && W.geom) {
      const a = fs.f, t = linkProgress(now);
      ctx.save(); ctx.strokeStyle = "rgba(169,130,63,.85)"; ctx.lineWidth = 1.4 * W.dpr; ctx.setLineDash([5 * W.dpr, 4 * W.dpr]);
      for (const b of fs.mates) { if (b === a || b.gone) continue;
        const x0 = R + a.p.cxPt * k, y0 = R + a.p.cyPt * k, x1 = R + b.p.cxPt * k, y1 = R + b.p.cyPt * k, mx = (x0 + x1) / 2, my = (y0 + y1) / 2 - Math.hypot(x1 - x0, y1 - y0) * .18;
        ctx.beginPath(); ctx.moveTo(x0, y0);
        if (t >= 1) ctx.quadraticCurveTo(mx, my, x1, y1);
        else { const n = 18; for (let i = 1; i <= Math.ceil(n * t); i++) { const s = Math.min(t, i / n), u = 1 - s; ctx.lineTo(u * u * x0 + 2 * u * s * mx + s * s * x1, u * u * y0 + 2 * u * s * my + s * s * y1); } }
        ctx.stroke();
        if (t >= 1) { ctx.setLineDash([]); ctx.fillStyle = "#a9823f"; ctx.beginPath(); ctx.arc(x1, y1, 3 * W.dpr, 0, Math.PI * 2); ctx.fill(); ctx.setLineDash([5 * W.dpr, 4 * W.dpr]); }
      }
      ctx.restore();
    }
    // where pieces were taken off: the charm fades out in clay and leaves a dashed outline of the room it freed
    for (const g of W.freed) {
      const x = g.x, t = g.t0 ? Math.min(1, ((now || performance.now()) - g.t0) / 700) : 1;
      withPiece(ctx, x, () => {
        if (t < 1) { ctx.save(); const sc = 1 - .1 * t; ctx.scale(sc, sc); ctx.globalAlpha = 1 - t; ctx.fillStyle = "rgba(176,86,63,.5)"; outlinePath(ctx, x, true); ctx.fill("evenodd"); ctx.restore(); }
        ctx.fillStyle = `rgba(176,86,63,${.06 * t})`; outlinePath(ctx, x, true); ctx.fill("evenodd");
        ctx.setLineDash([4 * W.dpr, 3 * W.dpr]); outlinePath(ctx, x); ctx.strokeStyle = `rgba(176,86,63,${.3 + .5 * t})`; ctx.lineWidth = 1.2 * W.dpr; ctx.stroke(); ctx.setLineDash([]);
      });
    }
    // back engraving: a small mark on each charm that has one (clay: still to approve, sage: approved)
    if (W.showBacks) for (const x of W.pieces) {
      if (x.gone || !x.eng || x.eng.kind === "none" || x.eng.kind === "skipped") continue;
      const cx = R + x.p.cxPt * k, cy = R + x.p.cyPt * k, r = Math.max(3.5, Math.min(6, 1.6 * k / W.dpr)) * W.dpr;
      ctx.beginPath(); ctx.arc(cx, cy, r + 2 * W.dpr, 0, Math.PI * 2); ctx.fillStyle = "#fff"; ctx.fill();
      ctx.beginPath(); ctx.arc(cx, cy, r, 0, Math.PI * 2); ctx.fillStyle = x.eng.kind === "approved" ? "#5f7a5b" : "#b0563f"; ctx.fill();
    }
    // the selected charm's pulse
    for (const f of W.fx) if (f.kind === "pulse") {
      const t = Math.min(1, ((now || performance.now()) - f.t0) / f.ms), x = f.piece; if (x.gone) continue;
      withPiece(ctx, x, () => { const s = 1 + .22 * t; ctx.scale(s, s); outlinePath(ctx, x); ctx.strokeStyle = `rgba(202,168,97,${.9 * (1 - t)})`; ctx.lineWidth = 3 * W.dpr / s; ctx.stroke(); });
    }
  }
  function linkProgress(now) { const f = W.fx.find(e => e.kind === "link"); if (!f) return 1; return Math.min(1, ((now || performance.now()) - f.t0) / f.ms); }
  function fxLoop() {
    if (W.raf) return;
    const tick = now => {
      W.fx = W.fx.filter(f => now - f.t0 < f.ms);
      paintFx(now);
      W.raf = W.fx.length ? requestAnimationFrame(tick) : 0;
      if (!W.fx.length) paintFx();
    };
    W.raf = requestAnimationFrame(tick);
  }
  function pulse(x) { if (still()) return paintFx(); const t0 = performance.now(); W.fx = W.fx.filter(f => f.kind !== "pulse" && f.kind !== "link"); W.fx.push({ kind: "pulse", piece: x, t0, ms: 900 }, { kind: "link", t0, ms: 420 }); fxLoop(); }

  /* ── pointer ── */
  function toPlate(e) {
    const r = W.el.fx.getBoundingClientRect(), sx = W.el.fx.width / r.width;
    return { x: ((e.clientX - r.left) * sx - W.R) / W.k, y: ((e.clientY - r.top) * sx - W.R) / W.k, cx: e.clientX - r.left, cy: e.clientY - r.top };
  }
  function hitAt(e) {
    const { x, y } = toPlate(e); if (x < 0 || y < 0 || x > W.st.wPt || y > W.st.hPt) return null;
    let best = null, bestD = Infinity;
    for (const pc of W.pieces) {
      if (pc.gone) continue;
      const p = pc.p, dx = x - p.cxPt, dy = y - p.cyPt, rad = Math.max(p.wPt, p.hPt) * .75;
      if (dx * dx + dy * dy > rad * rad) continue;
      const a = p.angle * Math.PI / 180, cs = Math.cos(a), sn = Math.sin(a), sc = p.scale || 1;
      const u = (dx * cs + dy * sn) / sc, v = (-dx * sn + dy * cs) / sc, c = pc.c;
      let inside;
      if (c && c.bits && c.bboxOuter) {
        const X = c.centerPt[0] + u, Y = c.centerPt[1] - v;
        const col = Math.floor((X - c.bboxOuter[0]) * c.scale), row = Math.floor((c.bboxOuter[3] - Y) * c.scale);
        inside = col >= 0 && row >= 0 && col < c.w && row < c.h && !!c.bits[row * c.w + col];
      } else inside = Math.abs(u) <= p.wPt / sc / 2 && Math.abs(v) <= p.hPt / sc / 2;
      const d = dx * dx + dy * dy;
      if (inside && d < bestD) { best = pc; bestD = d; }
    }
    return best;
  }
  function onMove(e) {
    if (!W.dlg.open || !W.st) return;
    const p = hitAt(e);
    W.el.plate.classList.toggle("onCharm", !!p);
    if (p !== W.hover) setHover(p);
    tip(p, e);
  }
  function setHover(p) {
    if (W.hover === p) return;
    W.hover = p; paintFx();
    W.el.orders.querySelectorAll(".swOrd.hot").forEach(n => n.classList.remove("hot"));
    const rid = p ? p.rid : null;
    if (rid) W.el.orders.querySelector(`.swOrd[data-rid="${CSS.escape(rid)}"]`)?.classList.add("hot");
    lightChips(rid || W.sel?.rid || null);
  }
  function tip(p, e) {
    const t = W.el.tip; if (!p || !e) { t.classList.remove("on"); return; }
    const mates = (p.rid && W.orders.get(p.rid)) || [p], elsewhere = otherSheets(p.rid);
    const key = p.id + "|" + elsewhere.length;
    if (t.dataset.k !== key) {
      t.dataset.k = key;
      const eng = p.eng && p.eng.kind !== "none" ? `<em style="--c:${p.eng.kind === "approved" ? "#7fb877" : "#e0876c"}"><i></i>Back · ${esc(p.eng.label)}</em>` : "";
      t.innerHTML = `<b>${esc(p.rid || p.name || "Charm")}</b><span>${esc(p.sku || "")}${p.qty > 1 ? ` · copy ${p.copy} of ${p.qty}` : ""}</span>${eng}` +
        (mates.length > 1 ? `<em><i></i>${mates.length - 1} more of this order here</em>` : "") +
        (elsewhere.length ? `<em><i></i>also on ${esc(elsewhere.map(s => `${CODE[s.metal] || ""} ${s.n}`).join(", "))}</em>` : "");
    }
    const { cx, cy } = toPlate(e), tw = t.offsetWidth, th = t.offsetHeight;
    let x = cx + 16, y = cy + 18; if (x + tw > W.cssW - 6) x = cx - tw - 14; if (y + th > W.cssH - 6) y = cy - th - 12;
    t.style.transform = `translate(${Math.max(4, x)}px,${Math.max(4, y)}px)`; t.classList.add("on");
  }
  function otherSheets(rid) {
    if (!rid || !W.set?.orders?.[rid]) return [];
    const ids = new Set(); for (const l of linesOf(W.set.orders[rid])) for (const c of l.copies || []) if (c.sheetId && c.sheetId !== W.id) ids.add(c.sheetId);
    return [...ids].map(id => W.setSheets.find(s => s.id === id) || { id, metal: "", n: "?" });
  }
  const matchesQ = (x, q) => `${x.rid} ${x.sku} ${x.name}`.toLowerCase().includes(q);

  /* ── engraving of a piece ── */
  function jobOfPiece(x) {
    if (!x.poolId || !window.Engrave) return null;
    for (const j of Engrave.items().values()) if (!j.editingBack && (j.copies || []).includes(x.poolId)) return j;
    return null;
  }
  function engOf(x) {
    const job = jobOfPiece(x), saved = W.rec?.engraving?.[x.poolId] || null;
    const back = (W.rec?.backPool || []).find(b => b.poolId === x.poolId) || null;
    if (job) {
      const s = job.state;
      if (["approved", "written"].includes(s)) return { kind: "approved", label: "approved", job, back, by: job.approvedBy, at: job.approvedAt, text: job.text };
      if (s === "skipped") return { kind: "skipped", label: "cut plain", job, by: job.approvedBy };
      if (s === "review") return { kind: "approve", label: "to approve", job, text: job.text };
      if (s === "words" || s === "blocked") return { kind: "words", label: "words to confirm", job, text: job.text, reason: job.reason };
      return { kind: "preparing", label: "being prepared", job, text: job.text };
    }
    if (back) return { kind: "approved", label: "approved", back, by: back.approvedBy, at: back.approvedAt, text: back.text };
    if (saved && saved.needed) return saved.approved ? { kind: "approved", label: "approved", text: saved.text } : { kind: "words", label: "to settle", text: saved.text };
    return { kind: "none", label: "none" };
  }

  /* ── the sheet pane: its orders ── */
  function renderStrip() {
    const rec = W.rec, E = W.el, n = W.pieces.filter(x => !x.gone).length, orders = [...W.orders.keys()].filter(k => k !== "—").length;
    const backs = W.pieces.filter(x => x.eng && ["approve", "words", "preparing"].includes(x.eng.kind)).length, ok = W.pieces.filter(x => x.eng?.kind === "approved").length;
    E.strip.innerHTML = `<span><b>${n}</b> charm${n === 1 ? "" : "s"}</span><span><b>${orders}</b> order${orders === 1 ? "" : "s"}</span><span><b>${fmt.pct(rec.density || 0)}</b> full</span><span><b>${fmt.area(rec.freePt2 || 0).replace(" mm²", "")}</b> mm² free</span>` +
      `<span class="grow"></span>` + (W.freed.length ? `<span class="swLegend"><i class="freed"></i>Freed room</span>` : "") + (backs + ok ? (backs ? `<span class="swLegend"><i></i>${backs} back${backs === 1 ? "" : "s"} to approve</span>` : "") + (ok ? `<span class="swLegend"><i class="ok"></i>${ok} back${ok === 1 ? "" : "s"} approved</span>` : "") + `<button class="swToggle" data-r2="backs" aria-pressed="${W.showBacks}">${W.showBacks ? "Hide" : "Show"} marks</button>` : "") +
      `<span>Hover a charm for its order · click to open it</span>`;
    const t = E.strip.querySelector("[data-r2=backs]"); if (t) t.onclick = () => { W.showBacks = !W.showBacks; renderStrip(); paintFx(); };
  }
  function renderSheetPane() {
    const E = W.el, all = [...W.orders.entries()];
    const engN = all.filter(([, xs]) => xs.some(x => x.eng && ["approve", "words", "preparing"].includes(x.eng.kind))).length;
    const multi = all.filter(([rid]) => rid !== "—" && otherSheets(rid).length).length;
    const seg = [["all", "All", all.length], ["backs", "Backs to approve", engN], ["multi", "On other sheets", multi]];
    E.seg.innerHTML = seg.map(([id, label, n]) => `<button type="button" data-f="${id}" aria-pressed="${W.filter === id}">${label}<i>${n}</i></button>`).join("");
    E.seg.querySelectorAll("[data-f]").forEach(b => b.onclick = () => { W.filter = b.dataset.f; renderSheetPane(); });
    renderOrders();
  }
  function renderOrders() {
    const E = W.el, q = W.q;
    let list = [...W.orders.entries()].filter(([, xs]) => xs.some(x => !x.gone));
    if (W.filter === "backs") list = list.filter(([, xs]) => xs.some(x => x.eng && ["approve", "words", "preparing"].includes(x.eng.kind)));
    if (W.filter === "multi") list = list.filter(([rid]) => rid !== "—" && otherSheets(rid).length);
    if (q) list = list.filter(([, xs]) => xs.some(x => matchesQ(x, q)));
    list.sort((a, b) => (a[0] === "—") - (b[0] === "—") || a[0].localeCompare(b[0]));
    if (!list.length) { E.orders.innerHTML = `<li class="swNone">${q ? "No order on this sheet matches." : W.filter === "backs" ? "No back engraving is waiting on this sheet." : W.filter === "multi" ? "Every order here is only on this sheet." : "This sheet has no charms."}</li>`; return; }
    E.orders.innerHTML = list.map(([rid, xs]) => {
      const live = xs.filter(x => !x.gone), skus = new Map(); for (const x of live) skus.set(x.sku, (skus.get(x.sku) || 0) + 1);
      const what = [...skus].map(([s, n]) => `${s}${n > 1 ? ` ×${n}` : ""}`).join(", ");
      const need = live.filter(x => x.eng && ["approve", "words", "preparing"].includes(x.eng.kind)).length, ok = live.filter(x => x.eng?.kind === "approved").length;
      const other = rid === "—" ? [] : otherSheets(rid);
      const tags = (need ? `<span class="swTag eng" title="Back engraving still to approve">${need > 1 ? need + " backs" : "back"}</span>` : ok ? `<span class="swTag engOk" title="Back engraving approved">back</span>` : "") +
        other.map(s => `<span class="swTag" style="--c:${colorOf(s.metal)}" title="Also on ${esc(s.name || "")}"><i></i>${esc(CODE[s.metal] || "")} ${s.n}</span>`).join("");
      return `<li class="swOrd" data-rid="${esc(rid)}"><span class="no">${rid === "—" ? "No order" : esc(rid)}</span><span class="what" title="${esc(what)}">${esc(what)}</span><span class="tags">${tags}</span></li>`;
    }).join("");
    E.orders.querySelectorAll(".swOrd").forEach(li => {
      const xs = W.orders.get(li.dataset.rid) || [];
      li.onmouseenter = () => { W.hover = xs.find(x => !x.gone) || null; paintFx(); lightChips(li.dataset.rid); };
      li.onmouseleave = () => { W.hover = null; paintFx(); lightChips(W.sel?.rid || null); };
      li.onclick = () => { const x = xs.find(x => !x.gone); if (x) selectPiece(x, { from: "list" }); };
    });
  }
  function renderFoot() {
    const rec = W.rec, E = W.el, f = (rec.label?.files || [])[0];
    const n = (rec.label?.orders || rec.orders || []).length;
    const qr = f ? `<div class="swQr" data-r2="qr"><figure class="qrPart" style="margin:0"><div class="qrTile"><img data-big title="Sheet QR label" data-label-sheet="${esc(rec.id)}" data-label-path="${esc(f.path || "")}" data-label-part="${+f.part || 1}" crossorigin="anonymous"${f.url ? ` src="${esc(cors(f.url))}"` : ""} alt="Sheet QR code"></div></figure><span class="busy"><span class="owSpin"></span></span><span class="cap"><b>QR label${(rec.label.files || []).length > 1 ? ` · ${(rec.label.files || []).length} parts` : ""}</b><span data-r2="qrText">${n} order${n === 1 ? "" : "s"} · click to enlarge</span></span></div>`
      : `<div class="swQr"><span class="qrWaiting" style="width:72px;height:72px">No QR yet</span><span class="cap"><b>QR label</b><span>Made when the sheet joins a set</span></span></div>`;
    const files = [["ai", ".ai", "Download the sheet with its back engravings"], ["dxf", ".dxf", "DXF · millimetres"]].map(([fmt2, label, title]) => `<button class="btn ghost xs" data-export-one="${esc(rec.id)}" data-format="${fmt2}" title="${title}">${label}</button>`).join("") +
      (rec.outputs?.labelled?.url ? `<a class="btn ghost xs" href="${esc(rec.outputs.labelled.url)}" target="_blank" rel="noopener" title="Every charm numbered, as a PDF">Proof</a>` : "") +
      (rec.outputs?.report?.url ? `<a class="btn ghost xs" href="${esc(rec.outputs.report.url)}" target="_blank" rel="noopener" title="The nest report (JSON)">Report</a>` : "");
    E.foot.innerHTML = `<div class="row">${qr}</div><div class="row"><span class="fLabel">Files</span><span class="swFiles">${files}</span></div>`;
  }
  function renderMenu() {
    const rec = W.rec, E = W.el, live = typeof openRunPage === "function" && openRunPage(rec.id);
    const srcs = (rec.sources || []).filter(s => s.url);
    E.menu.innerHTML = `<button data-m="ai">Download .ai<small>with backs</small></button><button data-m="dxf">Download .dxf</button>` +
      (rec.outputs?.labelled?.url ? `<a href="${esc(rec.outputs.labelled.url)}" target="_blank" rel="noopener">Labelled proof<small>PDF</small></a>` : "") +
      (rec.outputs?.report?.url ? `<a href="${esc(rec.outputs.report.url)}" target="_blank" rel="noopener">Nest report<small>JSON</small></a>` : "") +
      `<div class="sep"></div><button data-m="restore"${live ? ` disabled title="Already on its card, where the open run is using it"` : ""}>Restore into its card</button>` +
      (srcs.length ? `<button data-m="sources">Design files<small>${srcs.length}</small></button>` : "") +
      `<div class="sep"></div><button class="danger" data-m="delete">Delete sheet…</button>`;
    E.menu.querySelectorAll("[data-m]").forEach(b => b.onclick = () => menuAct(b.dataset.m, b));
  }
  function menu(show) {
    const E = W.el; if (!E.menu) return;
    if (show && E.menu.hidden) { E.menu.hidden = false; E.moreBtn.setAttribute("aria-expanded", "true"); animate(E.menu, [{ opacity: 0, transform: "translateY(-4px) scale(.97)" }, { opacity: 1, transform: "none" }], 160); }
    else if (!show && !E.menu.hidden) { E.menu.hidden = true; E.moreBtn.setAttribute("aria-expanded", "false"); if (W.rec) renderMenu(); }
  }
  function menuAct(m, b) {
    const rec = W.rec; if (!rec) return;
    if (m === "ai" || m === "dxf") { menu(false); return ProductionExports.run([rec.id], m); }
    if (m === "restore") { menu(false); close().then(() => restoreSheet(rec).catch(e => toast("Restore stopped: " + e.message, "bad", 7000))); return; }
    if (m === "sources") {
      const list = (rec.sources || []).filter(s => s.url);
      W.el.menu.innerHTML = `<button data-m="menuBack">${ICON.back.replace("<svg", '<svg style="width:14px;height:14px"')}Back</button><div class="sep"></div>` + list.map(s => `<a href="${esc(s.url)}" target="_blank" rel="noopener">${esc(String(s.name || "").replace(/ \(master\)$/, ""))}<small>${s.pool ? "master" : "file"}</small></a>`).join("");
      W.el.menu.style.maxHeight = "60vh"; W.el.menu.style.overflow = "auto";
      W.el.menu.querySelector("[data-m=menuBack]").onclick = () => { renderMenu(); };
      return;
    }
    if (m === "delete") {
      W.el.menu.innerHTML = `<div style="padding:6px 8px 2px;font:12.5px/1.45 var(--sans);color:var(--ink70)">Delete <b>${esc(rec.folder || rec.id)}</b> for good: its record, .ai, proof, report and preview. Enter the passcode to confirm.</div><form class="swPass"><input type="password" placeholder="Passcode" autocomplete="off"><button class="btn danger xs" type="submit">Delete</button></form>`;
      const form = W.el.menu.querySelector("form"), input = form.querySelector("input"); input.focus();
      form.onsubmit = async ev => {
        ev.preventDefault(); const code = input.value.trim(); if (!code) return;
        const btn = form.querySelector("button"); btn.disabled = true; btn.innerHTML = '<span class="spin"></span>Deleting';
        try { await api("charmNestLibrary", { op: "deleteSheet", id: rec.id, code }, { label: "Deleting sheet" }); toast(`Sheet ${rec.folder || rec.id} deleted`, "ok"); menu(false); close(); loadLibrary(); }
        catch (e) { btn.disabled = false; btn.textContent = "Delete"; input.value = ""; input.placeholder = e.status === 403 ? "Wrong passcode" : "Not deleted: " + e.message; input.focus(); }
      };
    }
  }

  /* ── the piece pane ── */
  async function showPane(name, dir) {
    const E = W.el, a = E.side.querySelector(`[data-pane="${name === "sheet" ? "piece" : "sheet"}"]`), b = E.side.querySelector(`[data-pane="${name}"]`);
    W.view = name;
    if (!b.hidden && a.hidden) return;
    b.hidden = false;
    const d = dir === "back" ? -1 : 1;
    animate(b, [{ opacity: 0, transform: `translateX(${d * 28}px)` }, { opacity: 1, transform: "none" }], 260);
    await animate(a, [{ opacity: 1, transform: "none" }, { opacity: 0, transform: `translateX(${-d * 28}px)` }], 180, { easing: "ease-in" });
    if (W.view === name) { a.hidden = true; a.getAnimations?.().forEach(x => x.cancel()); b.getAnimations?.().forEach(x => x.cancel()); }
  }
  function showSheetPane() {
    W.sel = null; W.fx = []; paintFx(); lightChips(null);
    renderStrip(); renderSheetPane();
    showPane("sheet", "back");
  }
  function sortedPieces() { return W.pieces.filter(x => !x.gone).slice().sort((a, b) => (a.rid || "~").localeCompare(b.rid || "~") || (a.sku || "").localeCompare(b.sku || "") || a.copy - b.copy); }
  function step(d) {
    const list = sortedPieces(); if (!list.length) return;
    const i = list.indexOf(W.sel); const j = i < 0 ? 0 : (i + d + list.length) % list.length;
    selectPiece(list[j], { from: d < 0 ? "prev" : "next" });
  }
  function selectPiece(x, opts = {}) {
    const same = W.sel === x && W.view === "piece";
    W.sel = x; W.hover = null; tip(null);
    lightChips(x.rid);
    pulse(x);
    if (!same) renderPiece(x, opts);
    if (W.view !== "piece") showPane("piece", "forward");
    else if (!same && (opts.from === "prev" || opts.from === "next")) animate(W.el.detail, [{ opacity: 0, transform: `translateX(${opts.from === "prev" ? -16 : 16}px)` }, { opacity: 1, transform: "none" }], 220);
    else if (!same) animate(W.el.detail, [{ opacity: .3 }, { opacity: 1 }], 200);
  }
  function rowOf(x) {
    const key = x.rid && x.tx ? `${x.rid}_${x.tx}` : null;
    const row = key && window.Orders && Orders.rows().find(r => r.key === key);
    if (row) return row;
    if (!x.rid) return null;
    return { key: key || x.rid, order: { receiptId: x.rid }, line: { transactionId: x.tx || "", sku: x.sku }, spec: { designSku: x.sku }, poolIds: x.poolId ? [x.poolId] : [], state: "written", problems: [] };
  }
  function renderPiece(x) {
    const E = W.el, list = sortedPieces(), row = rowOf(x);
    E.pos.textContent = `Charm ${list.indexOf(x) + 1} of ${list.length}`;
    const o = row && row.order || {}, sp = row && row.spec || {};
    const placed = o.createTs ? new Date(+o.createTs * 1000).toLocaleDateString(undefined, { month: "short", day: "numeric" }) : "";
    const buyer = (o.buyer && o.buyer.name) || o.buyerName || o.name || "";
    let ship = ""; try { const t = row && row.order.shipBy && window.Orders?.shipTxt?.(row); ship = t && t !== "\u2014" ? "ship by " + t : ""; } catch (_) {}
    const said = String(o.buyerMessage || "").trim();
    const words = (sp.personalization || []).filter(Boolean).join(" / ");
    const mm = x.c ? `${fmt.mm(x.c.widthPt).replace(" mm", "")} × ${fmt.mm(x.c.heightPt)}` : `${fmt.mm(x.p.wPt).replace(" mm", "")} × ${fmt.mm(x.p.hPt)}`;
    E.detail.innerHTML = `
      <header class="swOrderHead"><span class="fLabel">Order</span><span class="rid">${esc(x.rid || "No order")}</span><span class="who">${esc([buyer, placed ? "ordered " + placed : "", ship].filter(Boolean).join(" · ")) || "&nbsp;"}</span>${said ? `<div class="swSaid" title="From the buyer">${esc(said)}</div>` : ""}</header>
      <div class="swPiece"><canvas class="swThumb" width="184" height="184"></canvas><div class="facts"><b>${esc(x.sku || x.name)}</b><span>${x.qty > 1 ? `Copy ${x.copy} of ${x.qty} · ` : ""}${mm}</span><span>${esc(labelOf(W.rec.metal))}${sp.size ? " · size " + esc(sp.size) : ""}</span>${words ? `<em title="${esc(words)}">${esc(words)}</em>` : ""}</div></div>
      <section class="swSection" data-r2="eng"></section>
      <section class="swSection"><span class="fLabel" data-r2="trailHead">This order</span><ul class="swTrail" data-r2="trail"></ul></section>
      <section class="swSection" data-r2="off"></section>
      <section class="swSection" data-r2="msgs"><span class="fLabel">Messages</span></section>`;
    drawThumb(E.detail.querySelector(".swThumb"), x);
    renderEng(x);
    renderTrail(x);
    renderOffBtn(x);
    renderMsgs(x, row);
    E.pieceScroll.scrollTop = 0;
  }
  function drawThumb(cv, x) {
    const ctx = cv.getContext("2d"); ctx.fillStyle = "#fff"; ctx.fillRect(0, 0, cv.width, cv.height);
    const c = x.c; if (!c) { if (x.thumb) { const im = new Image(); im.crossOrigin = "anonymous"; im.onload = () => { const s = Math.min(cv.width / im.width, cv.height / im.height) * .86; ctx.drawImage(im, (cv.width - im.width * s) / 2, (cv.height - im.height * s) / 2, im.width * s, im.height * s); }; im.src = cors(x.thumb); } return; }
    const k = Math.min(cv.width / c.widthPt, cv.height / c.heightPt) * .82, cx = c.centerPt[0], cy = c.centerPt[1];
    ctx.save(); ctx.translate(cv.width / 2, cv.height / 2);
    const t = (px, py) => [(px - cx) * k, (cy - py) * k];
    ctx.fillStyle = "rgba(200,162,78,.12)"; ctx.beginPath(); CharmNestPDF.pathToCanvas(ctx, c.outline, t); for (const m of cutLinesOf(c)) CharmNestPDF.pathToCanvas(ctx, m, t); ctx.fill("evenodd");
    CharmNestPDF.drawCharm(ctx, c, t, k); ctx.restore();
  }
  function renderEng(x, flash) {
    const host = W.el.detail.querySelector("[data-r2=eng]"); if (!host) return;
    x.eng = engOf(x); const e = x.eng;
    if (e.kind === "none") { host.innerHTML = `<div class="swEng" data-state="none"><div class="top"><b>No back engraving</b></div></div>`; return; }
    const states = { approve: "To approve", words: "Words to confirm", preparing: "Being prepared", approved: "Approved", skipped: "Cut plain" };
    const when = e.at ? new Date(e.at).toLocaleString(undefined, { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }) : "";
    const words = e.text ? `<div class="words">${esc(e.text)}</div>` : "";
    let acts = "", pv = "";
    if (e.kind === "approve") {
      pv = `<div class="pv" data-r2="pv"><div class="swWait"><span class="owSpin"></span>Drawing the back…</div></div>`;
      acts = `<button class="btn sage sm" data-e="approve">${ICON.check}Approve</button><button class="btn ghost sm" data-e="engrave">Adjust in Engrave${ICON.go}</button>`;
    } else if (e.kind === "words") acts = `<button class="btn sm" data-e="engrave">Confirm the words in Engrave${ICON.go}</button>`;
    else if (e.kind === "preparing") acts = `<span class="swWait"><span class="owSpin"></span>Fitting the words on the back…</span><button class="btn ghost sm" data-e="engrave">Open in Engrave${ICON.go}</button>`;
    else if (e.kind === "approved") {
      const img = e.back && (e.back.outputs?.png?.url || e.back.png || e.back.preview);
      pv = img ? `<div class="pv"><img crossorigin="anonymous" src="${esc(/^https?:/.test(img) ? cors(img) : img)}" alt="The back engraving"></div>` : e.job && e.job.fit && e.job.view ? `<div class="pv" data-r2="pv"></div>` : "";
      acts = `<span class="by">${esc([e.by ? "by " + e.by : "", when].filter(Boolean).join(" · "))}</span><button class="btn ghost sm" data-e="engrave" style="margin-left:auto">View in Engrave${ICON.go}</button>`;
    } else if (e.kind === "skipped") acts = `<span class="by">${esc(e.by ? "Skipped by " + e.by : "Skipped")}</span>`;
    host.innerHTML = `<span class="fLabel">Back engraving</span><div class="swEng" data-state="${e.kind}"><div class="top"><b>${e.kind === "approve" ? "Check the back, then approve" : e.kind === "approved" ? "Engraved on the back" : e.kind === "words" ? "The words need a decision" : "Back engraving"}</b><span>${states[e.kind] || ""}</span></div>${pv}${words}<div class="acts">${acts}</div></div>`;
    const card = host.querySelector(".swEng");
    if (flash) { card.classList.remove("flash"); void card.offsetWidth; card.classList.add("flash"); }
    const slot = host.querySelector("[data-r2=pv]");
    if (slot && e.job) requestAnimationFrame(() => { try { if (!e.job.fit || !e.job.view) { slot.innerHTML = `<span class="by" style="padding:14px">Open it in Engrave to see the back</span>`; return; } const cv = Engrave.renderBack(e.job, 300, { hatch: false, grid: false }); slot.innerHTML = ""; slot.appendChild(cv); } catch (err) { slot.innerHTML = `<span class="by" style="padding:14px">The preview is in Engrave</span>`; } });
    host.querySelectorAll("[data-e]").forEach(b => b.onclick = () => {
      if (b.dataset.e === "engrave") return goEngrave(x, b);
      if (b.dataset.e === "approve") return approveHere(x, b);
    });
  }
  async function approveHere(x, b) {
    const job = x.eng && x.eng.job; if (!job) return;
    const who = whoAmI() || askWho(); if (!who) return;
    b.disabled = true; b.innerHTML = '<span class="spin"></span>Approving…';
    try {
      await Engrave.approve(job, who);
      if (W.sel !== x) return;
      if (!["approved", "written"].includes(job.state)) { b.disabled = false; b.innerHTML = `${ICON.check}Approve`; return; }
      renderEng(x, true); renderStrip(); renderOrders(); paintFx();
      if (window.RunCtl) RunCtl.poke();
    } catch (e) { toast("Not approved: " + e.message, "bad", 6000); b.disabled = false; b.innerHTML = `${ICON.check}Approve`; }
  }

  /* ── the order's pieces, here and elsewhere ── */
  function renderTrail(x) {
    const host = W.el.detail.querySelector("[data-r2=trail]"), headN = W.el.detail.querySelector("[data-r2=trailHead]"); if (!host) return;
    const rid = x.rid; if (!rid) { host.innerHTML = `<li class="off"><span class="n"></span><span class="sku">Not part of an order</span></li>`; return; }
    const items = new Map();
    const add = (poolId, v) => { const k = poolId || v.key; items.set(k, Object.assign(items.get(k) || {}, v)); };
    for (const y of W.orders.get(rid) || []) if (!y.gone) add(y.poolId || y.id, { poolId: y.poolId, piece: y, sku: y.sku, copy: y.copy, qty: y.qty, sheetId: W.id, metal: W.rec.metal, n: sheetNoOf(W.rec) });
    if (W.set?.orders?.[rid]) for (const l of linesOf(W.set.orders[rid])) for (const c of l.copies || []) if (!items.has(c.poolId)) { const s = W.setSheets.find(z => z.id === c.sheetId); add(c.poolId, { poolId: c.poolId, sku: l.sku, copy: c.copy, sheetId: c.sheetId, metal: s?.metal, n: s?.n, name: c.sheet }); }
    for (const p of W.pools.get(rid) || []) {
      if (["abandoned", "superseded"].includes(p.state)) continue;
      const cur = items.get(p.poolId); if (cur && cur.sheetId) { if (!cur.qty) cur.qty = p.quantity; continue; }
      const s = p.sheetId && (W.setSheets.find(z => z.id === p.sheetId) || { metal: p.material, n: +((/_Sheet-(\d+)/.exec(p.sheetName || "") || [])[1]) || "?" });
      add(p.poolId, { poolId: p.poolId, sku: p.sku, copy: p.copy, qty: p.quantity, sheetId: p.sheetId || null, metal: s ? s.metal : p.material, n: s ? s.n : null, name: p.sheetName || "", state: p.state });
    }
    const list = [...items.values()].sort((a, b) => (a.sheetId === W.id ? 0 : 1) - (b.sheetId === W.id ? 0 : 1) || String(a.sku).localeCompare(String(b.sku)) || (a.copy || 0) - (b.copy || 0));
    headN.textContent = `This order · ${list.length} piece${list.length === 1 ? "" : "s"}`;
    host.innerHTML = list.map((it, i) => {
      const here = it.sheetId === W.id, cur = it.piece === x;
      const where = here ? `<span class="where" style="--c:${colorOf(W.rec.metal)}"><i></i>${cur ? "this charm" : "on this sheet"}</span>`
        : it.sheetId ? `<span class="where" style="--c:${colorOf(it.metal)}"><i></i>${esc(CODE[it.metal] || "")} Sheet ${esc(it.n || "?")}${ICON.go}</span>`
        : `<span class="where"><i style="background:var(--ink25)"></i>not on a sheet yet</span>`;
      return `<li data-i="${i}" class="${cur ? "cur" : !it.sheetId ? "off" : ""}"><span class="n">${i + 1}</span><span class="sku">${esc(it.sku || "")}${it.qty > 1 ? `<small>copy ${it.copy} of ${it.qty}</small>` : ""}</span>${where}</li>`;
    }).join("");
    host.querySelectorAll("li[data-i]").forEach(li => {
      const it = list[+li.dataset.i];
      li.onmouseenter = () => { if (it.piece && it.piece !== x) { W.hover = it.piece; paintFx(); } };
      li.onmouseleave = () => { if (W.hover) { W.hover = null; paintFx(); } };
      li.onclick = () => {
        if (it.piece && it.piece !== x) return selectPiece(it.piece, { from: "trail" });
        if (!it.piece && it.sheetId && it.sheetId !== W.id) {
          li.querySelector(".where").innerHTML = `<span class="owSpin" style="width:11px;height:11px;flex-basis:11px"></span>opening…`;
          switchSheet(it.sheetId, { select: it.poolId, from: "trail" });
        }
      };
    });
    // the rest of the order, wherever it is: the pool knows every piece (read once per order while the window is open)
    if (!W.pools.has(rid) && W.trailFor !== rid) {
      W.trailFor = rid;
      api("charmNestLibrary", { op: "poolList", orderId: rid }, { quiet: true }).then(r => {
        W.pools.set(rid, r.pools || []); if (W.pools.size > 40) W.pools.delete(W.pools.keys().next().value);
        if (W.sel === x && W.dlg.open) { renderTrail(x); lightChips(rid); }
      }).catch(e => console.warn("sheet window: order pieces", e)).finally(() => { if (W.trailFor === rid) W.trailFor = null; });
    }
  }

  /* ── messages: the order's Customer and Team panes (the Engrave card's own, kept per line) ── */
  let msgTab = (() => { try { return localStorage.getItem("cn.card.tab") === "team" ? "team" : "customer"; } catch (_) { return "customer"; } })();
  function renderMsgs(x, row) {
    const sec = W.el.detail.querySelector("[data-r2=msgs]"); if (!sec) return;
    if (!row) { sec.innerHTML = `<span class="fLabel">Messages</span><div class="swEng" data-state="none"><div class="top"><b>No order, so no messages</b></div></div>`; return; }
    let cust = null, team = null;
    try { cust = window.CustomerMail?.cardPane?.({ key: row.key, row }) || null; } catch (e) { console.warn("sheet window: customer", e); }
    try { team = window.TeamCard?.pane(row) || null; } catch (e) { console.warn("sheet window: team", e); }
    if (!cust && !team) { sec.remove(); return; }
    const box = h("div", "swMsgs"), tabs = h("div", "owTabs");
    tabs.setAttribute("role", "tablist");
    const tab = (id, b, sub) => `<button type="button" role="tab" data-ow-tab="${id}"><b>${b}</b><span>${sub}</span><i class="owDot" hidden></i></button>`;
    tabs.innerHTML = (team ? tab("team", "Team", "internal") : "") + (cust ? tab("customer", "Customer", "on Etsy") : "");
    box.append(tabs, ...[team, cust].filter(Boolean)); sec.appendChild(box);
    const rid = String(row.order.receiptId);
    const show = (id, chosen) => {
      if (!team) id = "customer"; else if (!cust) id = "team";
      if (chosen) { msgTab = id; try { localStorage.setItem("cn.card.tab", id); } catch (_) {} }
      tabs.querySelectorAll("[data-ow-tab]").forEach(b => b.setAttribute("aria-selected", b.dataset.owTab === id ? "true" : "false"));
      if (team) team.hidden = id !== "team"; if (cust) cust.hidden = id !== "customer";
      const td = tabs.querySelector('[data-ow-tab="team"] .owDot'); if (td) td.hidden = id === "team" || !TeamCard.isNew(row);
      const cd = tabs.querySelector('[data-ow-tab="customer"] .owDot'), cb = cd && window.CustomerMail?.tabNews?.(rid); if (cd) cd.hidden = id === "customer" || !(cb && (cb.unread || cb.bad));
      if (id === "team") TeamCard.shownNow(row.key); else if (chosen) { try { window.CustomerMail?.cardPane?.({ key: row.key, row }); } catch (_) {} }
    };
    tabs.addEventListener("click", e => { const b = e.target.closest("[data-ow-tab]"); if (b) show(b.dataset.owTab, true); });
    show(msgTab, false);
  }

  /* ── to Engrave and back ── */
  const RET = { el: null, ctx: null, timer: 0, poll: 0 };
  function goEngrave(x, b) {
    const job = x.eng && x.eng.job, rec = W.rec;
    RET.ctx = { sheetId: rec.id, poolId: x.poolId || x.id, rid: x.rid, key: job ? job.key : null, label: `${CODE[rec.metal] || ""} Sheet ${sheetNoOf(rec)}`, was: job ? job.state : null };
    if (b) b.innerHTML = `<span class="spin"></span>Opening Engrave…`;
    close().then(() => {
      const v = Engrave.view();
      if (job && ["approved", "written", "skipped"].includes(job.state)) Engrave.restoreView(Object.assign({}, v, { tab: "done", focus: null, list: false, chosen: true, q: x.rid || "" }));
      else if (job) Engrave.restoreView(Object.assign({}, v, { tab: "place", focus: job.key, list: false, chosen: true, q: "" }));
      else Engrave.restoreView(Object.assign({}, v, { tab: "done", focus: null, list: false, chosen: true, q: x.rid || "" }));
      setMode("engrave"); Engrave.render();
      returnPill();
    });
  }
  function returnPill() {
    if (!RET.el) {
      RET.el = h("div", "swReturn"); RET.el.setAttribute("role", "status");
      // in the top bar beside the tabs, where it covers nothing (it was a floating pill over the Engrave bar)
      (document.getElementById("modeNav") || document.body).appendChild(RET.el);
    }
    const c = RET.ctx; if (!c) return;
    const paint = ok => {
      RET.el.classList.toggle("ok", !!ok);
      RET.el.innerHTML = `<button class="go" data-a="back">${ICON.back}Back to ${esc(c.label)}</button><span class="st" title="${ok ? "Approved: going back to the sheet" : `Order ${esc(c.rid)}: settle it here, then go back`}"><i></i>${ok ? "Approved · going back" : `${esc(c.rid)}`}</span><button class="x" data-a="x" title="Stay here" aria-label="Dismiss">${ICON.close}</button>`;
      RET.el.querySelector("[data-a=back]").onclick = () => back();
      RET.el.querySelector("[data-a=x]").onclick = () => dismiss();
    };
    RET.el.hidden = false; paint(false);
    RET.el.style.animation = "none"; void RET.el.offsetWidth; RET.el.style.animation = "";
    clearInterval(RET.poll); clearTimeout(RET.timer);
    // approved in Engrave: the pill says so and takes the operator back to the charm they came from
    RET.poll = setInterval(() => {
      if (!RET.ctx) return clearInterval(RET.poll);
      const job = c.key && Engrave.items().get(c.key);
      if (job && ["approved", "written"].includes(job.state) && !["approved", "written"].includes(c.was)) {
        clearInterval(RET.poll); paint(true);
        RET.timer = setTimeout(() => { if (S.mode === "engrave" && RET.ctx === c) back(true); }, 1300);
      }
    }, 350);
  }
  function dismiss() {
    clearInterval(RET.poll); clearTimeout(RET.timer); RET.ctx = null;
    if (!RET.el) return;
    animate(RET.el, [{ opacity: 1, transform: "none" }, { opacity: 0, transform: "translateY(-6px) scale(.96)" }], 180).then(() => { if (!RET.ctx) RET.el.hidden = true; });
  }
  function back(approved) {
    const c = RET.ctx; if (!c) return;
    const r = RET.el.getBoundingClientRect();
    dismiss();
    if (S.mode !== "library") setMode("library");
    open(c.sheetId, { select: c.poolId, from: "engrave", flash: approved, fromRect: r });
  }

  /* ── taking pieces off (Paul, 25 Sep 18:01): one charm, or a whole order off every sheet it is on ──
     A cancelled or changed order comes off in real time. Its pieces leave every sheet this sorter holds; the other
     charms stay exactly where they are (never arranged again), each sheet is rewritten, verified and saved under its
     own name, and its QR label is remade (Sets.onSheetSaved). The order is held in Orders so the run does not place it
     again; releasing it there nests it again. A piece stays where it is when its sheet was cut, its set was sent to
     the station, it sits inside a saved Rose Gold cut line, or its sheet is not held by this sorter. */
  const BUSY = ["nesting", "finishing", "queued"];
  const busy = sh => BUSY.includes(sh.status) || !!sh._operationStarting || !!(sh.persisted && !sh.persistedDone && !sh.problem);
  const sentToStation = sh => window.Sets && Sets.ofRun(sh.runId).some(set => set.committedAt && (set.sheetIds || []).includes(sh.sheetId));
  function sheetWord(id, name, sh) {
    const chip = W.setSheets.find(z => z.id === id);
    if (chip) return `${CODE[chip.metal] || ""} Sheet ${chip.n}`;
    if (sh) return `${CODE[sh.metal] || ""} Sheet ${sh.sheetIndex || sh.page || 1}`;
    if (W.rec && id === W.rec.id) return `${CODE[W.rec.metal] || ""} Sheet ${sheetNoOf(W.rec)}`;
    const m = /^([A-Z0-9]+)_.*_Sheet-(\d+)/.exec(name || "");
    return m ? `${m[1]} Sheet ${m[2]}` : (name || "a sheet");
  }
  const poolRowOf = (id, rid) => (window.B && B.pool && B.pool.rows.get(id)) || (W.pools.get(rid) || []).find(p => p.poolId === id) || null;
  function offPlan(x, whole) {
    const rid = x.rid, ids = new Set();
    if (whole && rid) {
      for (const y of W.orders.get(rid) || []) if (y.poolId && !y.gone) ids.add(y.poolId);
      for (const r of window.Orders ? Orders.rows() : []) if (String(r.order.receiptId) === rid) for (const id of r.poolIds || []) ids.add(id);
      for (const p of W.pools.get(rid) || []) if (!["abandoned", "superseded"].includes(p.state)) ids.add(p.poolId);
      if (W.set?.orders?.[rid]) for (const l of linesOf(W.set.orders[rid])) for (const c of l.copies || []) if (c.poolId) ids.add(c.poolId);
    } else if (x.poolId) ids.add(x.poolId);
    const holder = new Map(); for (const sh of allSheets()) for (const c of sh.charms) if (ids.has(c.poolId)) holder.set(c.poolId, sh);
    const ok = [], stay = [];
    for (const id of ids) {
      const sh = holder.get(id), pr = poolRowOf(id, rid), mine = W.byPool.get(id);
      const sku = mine?.sku || pr?.sku || "";
      if (!sh) {
        if (pr && pr.sheetId && !["abandoned", "superseded"].includes(pr.state)) stay.push({ id, sku, where: sheetWord(pr.sheetId, pr.sheetName), why: pr.state === "committed" ? "its set was already sent to the station" : "its sheet is not open in this sorter" });
        else if (mine) stay.push({ id, sku, where: sheetWord(W.id, W.rec.fileBase), why: "this sheet is not open in this sorter" });
        else ok.push({ id, sku, sh: null, where: "not on a sheet yet" });
        continue;
      }
      const where = sheetWord(sh.sheetId, sh.fileBase, sh);
      if (sh.roseCutAt) stay.push({ id, sku, where, why: "that sheet was already cut" });
      else if (sentToStation(sh)) stay.push({ id, sku, where, why: "its set was already sent to the station" });
      else if (sh.metal === "rose" && (sh.rosePlan || sh.roseProtected)) stay.push({ id, sku, where, why: "it is inside the saved Rose Gold cut line" });
      else ok.push({ id, sku, sh, where });
    }
    // a sheet is not emptied here: with nothing left on it there are no files to rewrite, so it is deleted from its menu
    for (const sh of new Set(ok.map(o => o.sh).filter(Boolean))) {
      const going = new Set(ok.filter(o => o.sh === sh).map(o => o.id));
      const left = sh.placements.filter(p => { const c = sh.charms.find(c => c.id === p.id); return c && !going.has(c.poolId); }).length;
      if (!left && sh.placements.length) for (const o of ok.filter(o => o.sh === sh)) o.last = true;
    }
    return { rid, whole, ids, ok: ok.filter(o => !o.last), stay: stay.concat(ok.filter(o => o.last).map(o => Object.assign(o, { why: "it is the last charm on that sheet: delete the sheet from its menu instead" }))) };
  }
  const listWhere = list => [...new Set(list.map(o => o.where))].join(", ");
  function renderOffBtn(x) {
    const host = W.el.detail.querySelector("[data-r2=off]"); if (!host) return;
    if (!x.poolId && !x.rid) { host.remove(); return; }
    if (W.work && W.work.state === "working") { host.innerHTML = ""; return; }
    host.innerHTML = `<button type="button" class="swOffBtn">${ICON.off}Take off the sheet…</button>`;
    host.querySelector("button").onclick = () => renderOff(x);
  }
  function renderOff(x) {
    const host = W.el.detail.querySelector("[data-r2=off]"); if (!host) return;
    const mates = x.rid ? offPlan(x, true) : null, one = offPlan(x, false);
    const many = mates && mates.ids.size > 1;
    let pick = many ? "all" : "one", why = "";
    const paint = () => {
      const plan = pick === "all" ? mates : one, n = plan.ok.length;
      const stay = plan.stay.length ? `<div class="stay"><b>${plan.stay.length === 1 ? "1 piece stays" : plan.stay.length + " pieces stay"}</b>: ${plan.stay.map(o => `${esc(o.sku)} on ${esc(o.where)} (${esc(o.why)})`).join("; ")}.</div>` : "";
      const note = !n ? "" : pick === "all"
        ? `The order is held in Orders, so the run does not place it again; release it there to nest it again. Every other charm stays exactly where it is, and each sheet is rewritten with a new QR label.`
        : many ? `The order's other pieces stay and are cut. If this was its line's last piece, the order stays open on the station.` : `The order is held in Orders, so the run does not place it again. Every other charm stays where it is, and the sheet gets a new QR label.`;
      host.innerHTML = `<div class="swOff" role="group" aria-label="Take off the sheet"><h4>Take off the sheet</h4>
        <div class="pick">${many ? `<label class="opt"><input type="radio" name="swOffWho" value="all"${pick === "all" ? " checked" : ""}><b>The whole order · ${mates.ids.size} pieces</b><small>${esc(listWhere(mates.ok.concat(mates.stay)))} · recommended when an order is cancelled</small></label>` : ""}
        <label class="opt"><input type="radio" name="swOffWho" value="one"${pick === "one" ? " checked" : ""}><b>${many ? "Only this charm" : "This charm"}</b><small>${esc(x.sku || x.name)}${x.qty > 1 ? ` · copy ${x.copy} of ${x.qty}` : ""} · ${esc(sheetWord(W.id, W.rec.fileBase))}</small></label></div>
        <div class="why"><span class="fLabel">Why</span>${["Cancelled", "Changed", "On hold"].map(w => `<button type="button" data-why="${w}" aria-pressed="${why === w}">${w}</button>`).join("")}</div>
        ${stay}${note ? `<div class="note">${note}</div>` : ""}
        <div class="acts">${n ? `<button type="button" class="btn danger sm" data-o="go">${ICON.off}Take off ${n === 1 ? "1 piece" : n + " pieces"}</button>` : ""}<button type="button" class="btn ghost sm" data-o="keep">${n ? "Keep" : "Close"}</button></div></div>`;
      host.querySelectorAll("input[name=swOffWho]").forEach(r => r.onchange = () => { pick = r.value; paint(); });
      host.querySelectorAll("[data-why]").forEach(b => b.onclick = () => { why = why === b.dataset.why ? "" : b.dataset.why; paint(); });
      host.querySelector("[data-o=keep]").onclick = () => { animate(host.firstElementChild, [{ opacity: 1 }, { opacity: 0, transform: "translateY(-4px)" }], 140).then(() => renderOffBtn(x)); };
      const go = host.querySelector("[data-o=go]"); if (go) go.onclick = () => {
        const who = whoAmI() || askWho(); if (!who) return;
        takeOff(pick === "all" ? offPlan(x, true) : offPlan(x, false), why, who).catch(e => { console.error("sheet window: take off", e); toast("Not taken off: " + e.message, "bad", 8000); });
      };
    };
    paint();
    requestAnimationFrame(() => host.scrollIntoView({ block: "nearest", behavior: still() ? "auto" : "smooth" }));
  }
  function renderWork() {
    const E = W.el, w = W.work; if (!E.work) return;
    if (!w) { E.work.innerHTML = ""; E.work.hidden = true; return; }
    E.work.hidden = false;
    E.work.innerHTML = `<div class="swWork${w.state === "done" ? " done" : w.state === "failed" ? " failed" : ""}" role="status"><h4>${w.state === "working" ? '<span class="owSpin"></span>' : w.state === "done" ? ICON.check.replace("<svg", '<svg style="width:15px;height:15px;color:var(--sage)"') : ""}${esc(w.title)}</h4>
      <ol>${w.steps.map(st => `<li class="${st.state || ""}"><i></i><span>${esc(st.text)}${st.detail ? ` <small style="color:var(--ink45)">· ${esc(st.detail)}</small>` : ""}</span></li>`).join("")}</ol>
      ${w.note ? `<div class="note">${w.note}</div>` : ""}${w.state !== "working" ? `<div style="display:flex;justify-content:flex-end"><button type="button" class="btn ghost xs" data-w="ok">Done</button></div>` : ""}</div>`;
    const b = E.work.querySelector("[data-w=ok]"); if (b) b.onclick = () => { const el = E.work.firstElementChild; animate(el, [{ opacity: 1 }, { opacity: 0, transform: "translateY(-4px)" }], 160).then(() => { W.work = null; renderWork(); }); };
  }
  const pause = ms => new Promise(r => setTimeout(r, ms));
  async function takeOff(plan, why, who) {
    const list = plan.ok; if (!list.length) return;
    const ids = new Set(list.map(o => o.id)), rid = plan.rid;
    const pages = [...new Set(list.map(o => o.sh).filter(Boolean))];
    const sheetId = W.id, names = listWhere(list.filter(o => o.sh));
    const step = (text, state, detail) => ({ text, state: state || "", detail: detail || "" });
    const rewrite = pages.map(sh => ({ sh, name: sheetWord(sh.sheetId, sh.fileBase, sh), st: step(`Rewriting ${sheetWord(sh.sheetId, sh.fileBase, sh)}`) }));
    const labels = step(`Remaking the QR label${pages.length === 1 ? "" : "s"}`);
    const wait = step("Waiting for the sheets to finish their current step");
    const off = step(`Taking ${list.length === 1 ? "1 piece" : list.length + " pieces"} off${names ? " " + names : ""}`);
    W.work = { state: "working", title: `Taking off ${plan.whole && rid ? "order " + rid : (list[0].sku || "the charm")}`, steps: [wait, off, ...rewrite.map(r => r.st), ...(pages.length ? [labels] : [])], note: "" };
    if (!pages.some(busy)) W.work.steps.shift();
    // the pieces leave this plate at once, in clay, and leave their outline
    const t0 = performance.now();
    for (const o of list) { const y = W.byPool.get(o.id); if (y && !y.gone) { y.gone = true; W.freed.push({ x: y, t0, rid: y.rid, sku: y.sku }); } }
    W.hover = null; tip(null); W.el.plate.classList.remove("onCharm");
    W.fx.push({ kind: "freed", t0, ms: 720 }); fxLoop(); paintBase();
    if (W.view === "piece") showSheetPane(); else { renderStrip(); renderSheetPane(); }
    renderWork();
    const paint = () => { if (W.id === sheetId && W.dlg.open) renderWork(); };
    try {
      // 1 · a sheet in the middle of a search or a save finishes that first
      if (pages.some(busy)) {
        wait.state = "now"; paint();
        const until = Date.now() + 180000;
        while (pages.some(busy) && Date.now() < until) { const b = pages.find(busy); wait.detail = `${sheetWord(b.sheetId, b.fileBase, b)}: ${b.stage || b.status}`; paint(); await pause(400); }
        if (pages.some(busy)) throw new Error("a sheet is still busy after three minutes; try again once it has saved");
        wait.state = "ok"; wait.detail = "";
      }
      off.state = "now"; paint();
      // 2 · off the sheets this sorter holds (the rest of each sheet stays as placed: Orders.keepRest)
      for (const sh of pages) {
        const gone = sh.charms.filter(c => ids.has(c.poolId));
        sh.charms = sh.charms.filter(c => !ids.has(c.poolId));
        sh.placements = sh.placements.filter(p => sh.charms.some(c => c.id === p.id));
        sh.rejects = (sh.rejects || []).filter(id => sh.charms.some(c => c.id === id));
        if (Array.isArray(sh.feedWait)) sh.feedWait = sh.feedWait.filter(id => sh.charms.some(c => c.id === id || c.poolId === id));
        if (sh.backPool) sh.backPool = sh.backPool.filter(b => !ids.has(b.poolId));
        Orders.keepRest(sh);
        agent({ metal: sh.metal, run: sh.runId }, "POOL", `${gone.length} piece${gone.length === 1 ? "" : "s"} of ${rid || "an order"} taken off ${sheetName(sh)} by ${who}${why ? " (" + why.toLowerCase() + ")" : ""}; the rest stay where they are`);
      }
      // 3 · the order's lines: held, so the run does not place them again; their engraving goes with the pieces
      const text = `Taken off ${names || "its sheet"} by ${who}${why ? " · " + why.toLowerCase() : ""}`;
      for (const r of Orders.rows()) {
        const mineRow = plan.whole && rid && String(r.order.receiptId) === rid;
        if (!mineRow && !(r.poolIds || []).some(id => ids.has(id))) continue;
        r.poolIds = (r.poolIds || []).filter(id => !ids.has(id));
        const j = Engrave.items().get(r.key);
        if (j) { j.copies = (j.copies || []).filter(id => !ids.has(id)); if (!j.copies.length) { Engrave.items().delete(r.key); Review.remove("eng:" + r.key); } }
        if (!r.poolIds.length && r.state !== "gone") { r.state = "held"; r.hold = r.reason = `${text} — release the line to nest it again`; }
      }
      await Pool.update([...ids], { state: "abandoned", sheetId: null, setId: null, removedBy: who, removedReason: why || null, removedAt: Date.now() });
      for (const id of ids) B.pool.rows.delete(id);
      // 4 · the set: the order leaves the sheets it was on (labels are remade when each sheet is saved again)
      for (const set of [...(B.sets?.values?.() || [])]) {
        let touched = false;
        for (const [k, o] of Object.entries(set.orders || {})) {
          if (Array.isArray(o.lines)) { for (const l of o.lines) { const n0 = (l.copies || []).length; l.copies = (l.copies || []).filter(c => !ids.has(c.poolId)); touched = touched || l.copies.length !== n0; } o.lines = o.lines.filter(l => l.copies.length); if (!o.lines.length && touched) delete set.orders[k]; continue; }
          let hit = false;
          for (const [tx, l] of Object.entries(o.lines || {})) { const n0 = (l.copies || []).length; l.copies = (l.copies || []).filter(c => !ids.has(c.poolId)); if (l.copies.length !== n0) hit = true; if (!l.copies.length && hit) delete o.lines[tx]; }
          if (hit) { touched = true; if (!Object.keys(o.lines || {}).length) delete set.orders[k]; }
        }
        if (touched) await Sets.save(set).catch(e => console.warn("sheet window: set record", e));
      }
      if (B.run) { B.run.lines = Object.fromEntries(Orders.rows().map(Orders.lineRecord)); await RunCtl.save(B.run).catch(e => console.warn("sheet window: run lines", e)); }
      Review.syncOrderItems(); Orders.render(); Engrave.render();
      off.state = "ok"; paint();
      // 5 · each sheet written again as it now stands, verified and saved; its QR label is remade with it
      for (const r of rewrite) {
        const sh = r.sh; r.st.state = "now"; paint();
        if (!allSheets().includes(sh) || !sh.placements.length) { r.st.state = "ok"; r.st.detail = "nothing left to write"; continue; }
        const job0 = sh.jobId; sh._byHand = true;
        startNest(sh);
        const until = Date.now() + 240000;
        while (Date.now() < until) {
          if (sh.jobId !== job0 && !busy(sh) && (sh.persistedDone || sh.problem)) break;
          r.st.detail = sh.stage || (sh.status === "queued" ? "waiting its turn" : sh.status); paint(); await pause(350);
        }
        if (sh.problem) throw new Error(`${sheetWord(sh.sheetId, sh.fileBase, sh)}: ${sh.problem}`);
        if (sh.jobId === job0 || busy(sh) || !sh.persistedDone) { r.st.state = "now"; r.st.detail = "still saving; it finishes on its card in the Nest tab"; continue; }
        r.st.state = "ok"; r.st.detail = sh.verification && !sh.verification.ok ? "verification flagged, see its report" : "";
      }
      // 6 · the QR labels. A sheet of a run under the set rules is a working sheet again once it is written, and the set
      //     takes it back with a new label while it is still ready for the laser (Gate.assemble, as at every check);
      //     one that is no longer full enough fills again first. An older run's sheet got its label as it was saved.
      if (pages.length) {
        labels.state = "now"; paint();
        const run = window.B && B.run;
        if (run && window.Gate && Gate.modern(run.runId) && pages.some(sh => sh.runId === run.runId)) {
          try { await Gate.assemble(run); } catch (e) { labels.state = "bad"; labels.detail = e.message; }
        }
        // a sheet filling again is out of its set until it is full: the label it had lists the old orders, so it goes
        for (const sh of pages) if (allSheets().includes(sh) && sh.draft && sh.label && sh.sheetId) {
          sh.label = null;
          await api("charmNestLibrary", { op: "putSheet", sheet: { id: sh.sheetId, label: null } }, { quiet: true }).catch(e => console.warn("sheet window: label", e));
        }
        if (labels.state === "now") labels.state = "ok";
      }
      if (window.RunCtl) RunCtl.poke();
      const open = rewrite.some(r => r.st.state === "now");
      const fates = rewrite.filter(r => allSheets().includes(r.sh) && r.sh.placements.length).map(r => {
        const sh = r.sh, set = sh.setId && !sh.draft && window.Sets && [...(B.sets?.values?.() || [])].find(z => z.setId === sh.setId);
        return set ? `${esc(r.name)} stays in ${esc(set.name || "its set")} with a new QR label.` : `${esc(r.name)} is filling again (${fmt.pct(sh.density || 0)} full): the next orders that fit go into the freed room before it goes to the laser.`;
      });
      W.work.state = "done"; W.work.title = open ? "Taken off · a sheet is still saving" : "Taken off";
      W.work.note = fates.join(" ") + (plan.whole && rid ? ` Order ${esc(rid)} is held in Orders.` : "") +
        (plan.stay.length ? ` ${plan.stay.length === 1 ? "1 piece" : plan.stay.length + " pieces"} stayed (${esc(plan.stay.map(o => o.where + ": " + o.why).join("; "))}).` : "");
      paint();
      if (W.id === sheetId && W.dlg.open) open2(sheetId);
    } catch (e) {
      for (const st of W.work.steps) if (st.state === "now") st.state = "bad";
      W.work.state = "failed"; W.work.title = "Not everything came off";
      W.work.note = esc(e.message) + ". Pieces already taken off stay off; the sheet window shows the sheet as saved now.";
      paint();
      if (W.id === sheetId && W.dlg.open) open2(sheetId);
      throw e;
    }
  }
  // the sheet as it is saved now, with the room the pieces freed marked on it
  function open2(id) { return open(id, { keepWork: true, keepSet: false }); }

  /* re-read the engraving marks when Engrave settles something while the window is open */
  setInterval(() => {
    if (!W.dlg || !W.dlg.open || !W.rec || !W.geom) return;
    let changed = false;
    for (const x of W.pieces) { const e = engOf(x); if (!x.eng || e.kind !== x.eng.kind) { x.eng = e; changed = true; } }
    if (changed) { renderStrip(); if (W.view === "sheet") renderSheetPane(); else if (W.sel) renderEng(W.sel, true); paintFx(); }
  }, 1500);

  function veil(text) { const E = W.el; if (!text) { E.veil.hidden = true; return; } E.veilText.textContent = text; E.veil.hidden = false; }

  window.SheetWin = { open, close, isOpen: () => !!(W.dlg && W.dlg.open), current: () => W.id, _W: W };
})();
