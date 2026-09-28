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
  // the name that goes with each change, asked for in a small bar inside the window (never a browser pop-up over it);
  // once given, the change the person pressed carries on by itself
  function needName(retry) {
    const who = whoAmI(); if (who) return who;
    const bar = W.el.name; if (!bar) return "";
    bar.innerHTML = `<input type="text" maxlength="40" autocomplete="name" spellcheck="false" aria-label="Your name, kept with this change" placeholder="Your name, for the record"><button type="button" class="btn sage xs" data-nm="go">Continue</button><button type="button" class="swIcon" data-nm="x" title="Not now" aria-label="Not now">${ICON.close}</button>`;
    bar.hidden = false;
    const inp = bar.querySelector("input");
    const go = () => {
      const v = inp.value.trim(); if (!v) return inp.focus();
      if (window.B) B.employee = v; try { localStorage.setItem("cn.employee", v); } catch (_) {}
      hideName(); if (retry) retry();
    };
    inp.onkeydown = e => { if (e.key === "Enter") { e.preventDefault(); go(); } else if (e.key === "Escape") { e.preventDefault(); e.stopPropagation(); hideName(); } };
    bar.querySelector("[data-nm=go]").onclick = go; bar.querySelector("[data-nm=x]").onclick = hideName;
    requestAnimationFrame(() => inp.focus());
    return "";
  }
  // an order with pieces on another 10K/14K sheet that stays out: the choice is asked in the same bar (Paul, 28 Sep: the
  // person decides), resolving to the sheets to include, or null to leave things as they were
  function askSplit(sh, split) {
    const bar = W.el.name; if (!bar) return Promise.resolve([sh]);
    const nm = p => `${p.metal === "gold10k" ? "10K" : "14K"} Sheet ${p.page}`;
    const orders = [...new Set(split.flatMap(x => x.orders))], others = split.map(x => x.sheet);
    return new Promise(resolve => {
      bar.innerHTML = `<span class="swAsk">${orders.length === 1 ? `Order ${esc(orders[0])} is` : `${orders.length} orders are`} also on ${esc(others.map(nm).join(", "))}, which ${others.length > 1 ? "are" : "is"} not in the set.</span><button type="button" class="btn sage xs" data-sp="all">Include ${others.length > 1 ? "all" : "both"}</button><button type="button" class="btn ghost xs" data-sp="one">Only this sheet</button><button type="button" class="swIcon" data-sp="x" title="Cancel" aria-label="Cancel">${ICON.close}</button>`;
      bar.setAttribute("aria-label", "An order on two sheets"); bar.hidden = false;
      const done = v => { if (W.asked !== done) return; W.asked = null; bar.onkeydown = null; hideName(); bar.setAttribute("aria-label", "Your name"); resolve(v); };
      W.asked = done;   // Esc, or the window closing, is a Cancel
      bar.querySelector("[data-sp=all]").onclick = () => done([sh, ...others]);
      bar.querySelector("[data-sp=one]").onclick = () => done([sh]);
      bar.querySelector("[data-sp=x]").onclick = () => done(null);
      bar.onkeydown = e => { if (e.key === "Escape") { e.preventDefault(); e.stopPropagation(); done(null); } };
      requestAnimationFrame(() => bar.querySelector("[data-sp=all]").focus());
    });
  }
  function hideName() { if (W.asked) return W.asked(null); const bar = W.el && W.el.name; if (bar && !bar.hidden) { bar.hidden = true; bar.innerHTML = ""; } }
  const still = () => matchMedia("(prefers-reduced-motion: reduce)").matches;
  const EASE = "cubic-bezier(.2,.8,.2,1)";
  const h = (tag, cls, html) => { const e = document.createElement(tag); if (cls) e.className = cls; if (html != null) e.innerHTML = html; return e; };
  const animate = (el, frames, ms, opts) => { if (!el || still() || !el.animate) return Promise.resolve(); try { return el.animate(frames, Object.assign({ duration: ms, easing: EASE, fill: "both" }, opts || {})).finished.catch(() => {}); } catch (_) { return Promise.resolve(); } };
  const ICON = {
    pause: '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M6 4v8M10 4v8" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/></svg>',
    fill: '<svg viewBox="0 0 16 16" aria-hidden="true"><rect x="2.5" y="2.5" width="11" height="11" rx="2.5" fill="none" stroke="currentColor" stroke-width="1.4" stroke-dasharray="2.6 2"/><path d="M8 5.5v5M5.5 8h5" stroke="currentColor" stroke-width="1.6" stroke-linecap="round"/></svg>',
    off: '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M3 4.5h10M6.5 4.5V3h3v1.5M4.5 4.5l.6 8.2c.05.7.6 1.3 1.3 1.3h3.2c.7 0 1.25-.6 1.3-1.3l.6-8.2" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>',
    back: '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M10 3 5 8l5 5" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"/></svg>',
    next: '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="m6 3 5 5-5 5" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"/></svg>',
    close: '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M4 4l8 8M12 4l-8 8" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round"/></svg>',
    more: '<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="3.5" cy="8" r="1.4" fill="currentColor"/><circle cx="8" cy="8" r="1.4" fill="currentColor"/><circle cx="12.5" cy="8" r="1.4" fill="currentColor"/></svg>',
    go: '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M3 8h9M8.5 4.5 12 8l-3.5 3.5" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/></svg>',
    check: '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="m3.5 8.5 3 3 6-7" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></svg>',
    add: '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M8 3.5v9M3.5 8h9" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round"/></svg>',
    hand: '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M8 2v12M2 8h12M8 2 6.3 3.7M8 2l1.7 1.7M8 14l-1.7-1.7M8 14l1.7-1.7M2 8l1.7-1.7M2 8l1.7 1.7M14 8l-1.7-1.7M14 8l-1.7 1.7" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"/></svg>',
    turnL: '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M3.5 8a4.5 4.5 0 1 0 1.3-3.2M3.5 2.8v2.4h2.4" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/></svg>',
    turnR: '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M12.5 8a4.5 4.5 0 1 1-1.3-3.2M12.5 2.8v2.4h-2.4" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/></svg>',
    search: '<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="7" cy="7" r="4.3" fill="none" stroke="currentColor" stroke-width="1.5"/><path d="m10.3 10.3 3.2 3.2" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>'
  };

  const STYLE = `
dialog.sheetWin{width:min(1480px,98vw);height:min(94vh,1000px);max-height:none;border-radius:16px;overflow:hidden;background:var(--card)}
dialog.sheetWin::backdrop{background:rgba(20,18,15,.5);animation:swFade .22s ease both}
dialog.sheetWin[open]{animation:swIn .26s ${EASE} backwards}
/* grown out of the sheet that was clicked (Paul, 28 Sep): the window keeps no lift of its own while it is open; while the
   sheet flies, the window's surface is a curtain growing from where it was clicked, the sheet flies above everything,
   and the panel waits, untouchable, until it comes in */
dialog.sheetWin.swGrow[open]:not(.closing){animation:none}
dialog.sheetWin.swGrow::backdrop{animation:swFade .42s ease both}
dialog.sheetWin.swFlying{background:transparent;box-shadow:none}
.swFlying .swStage{background:transparent}
.swFlying .swPlateBox{overflow:visible}
.swFlying .swPlate{z-index:3;transform-origin:0 0;background:transparent;box-shadow:none}
.swFlying .swVeil{opacity:0}
.swSide.swHush{pointer-events:none}
.swCurtain{position:absolute;inset:0;z-index:-1;border-radius:inherit;overflow:hidden;background:var(--paper);box-shadow:0 24px 80px rgba(0,0,0,.32);transform-origin:0 0;pointer-events:none;will-change:transform,opacity}
.swCurtain i{position:absolute;background:var(--card)}
.swCurtain b{position:absolute;inset:0;will-change:opacity}
.swShade{position:absolute;z-index:2;border-radius:10px;box-shadow:0 1px 2px rgba(30,26,20,.06),0 12px 34px rgba(30,26,20,.10);transform-origin:0 0;pointer-events:none;will-change:transform,opacity}
.swPlate canvas.swSnap{inset:auto;pointer-events:none}
.swSkel{position:absolute;inset:0;z-index:2;background:var(--card);overflow:hidden;pointer-events:none}
.swSkel[hidden]{display:none}
.swSkel i{display:block;background:var(--paper2);border-radius:6px;animation:swBreathe 1.4s ease-in-out .5s infinite alternate}
.swSkel .row{display:flex;align-items:center;gap:10px}
@keyframes swBreathe{to{opacity:.55}}
dialog.sheetWin.closing{animation:swOut .17s ease both}
dialog.sheetWin.closing::backdrop{animation:swFadeOut .17s ease both}
dialog.sheetWin.swBack::backdrop{animation:swFadeOut .44s ease .04s both}
/* (the opening lift is not kept once done: a window left with a transform would be the frame of every copy that flies
   inside it, and the copies are placed where they stand on the screen) */
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
.swSheets[hidden]{display:none}
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
.swStrip{display:flex;align-items:center;gap:14px;padding:7px 16px 9px;font:11.5px var(--sans);color:var(--ink45);min-width:0;white-space:nowrap;overflow:hidden;min-height:30px}
.swStrip .swToggle{margin:-4px 0}
.swStrip b{font:600 12px var(--mono);color:var(--ink)}
.swStrip .grow{flex:1}
.swLegend{display:inline-flex;align-items:center;gap:6px}
.swLegend i{width:9px;height:9px;border-radius:50%;background:var(--clay);box-shadow:0 0 0 2px #fff,0 0 0 3px rgba(0,0,0,.08)}
.swLegend i.ok{background:var(--sage)}
.swLegend i.freed{background:transparent;border:1.5px dashed var(--clay);box-shadow:none;border-radius:3px}
.swToggle{border:1px solid var(--line);background:var(--card);border-radius:999px;padding:3px 10px;font:600 11px var(--sans);color:var(--ink70);cursor:pointer}
.swToggle[aria-pressed=true]{background:var(--claySoft);border-color:#e7b9aa;color:#8a3a26}
.swSide{position:relative;border-left:1px solid var(--line);background:var(--card);min-width:0;min-height:0;overflow:hidden}
.swPane{position:absolute;inset:0;display:grid;grid-template-rows:auto minmax(0,1fr) auto;grid-template-columns:minmax(0,1fr);min-height:0}
.swPane[hidden]{display:none}
.swPane[data-pane=sheet]{grid-template-rows:auto auto auto minmax(72px,1fr) auto}
.swPane[data-pane=sheet]>.swPaneHead{grid-row:1}.swPane[data-pane=sheet]>[data-r=work]{grid-row:2}.swPane[data-pane=sheet]>[data-r=fill]{grid-row:3}.swPane[data-pane=sheet]>.swScroll{grid-row:4}.swPane[data-pane=sheet]>.swFoot{grid-row:5}
.swPaneHead{padding:12px 14px 10px;border-bottom:1px solid var(--line2);display:grid;gap:9px}
.swFind{position:relative}
.swFind svg{position:absolute;left:10px;top:50%;width:14px;height:14px;transform:translateY(-50%);color:var(--ink45)}
.swFind input{width:100%;border:1px solid var(--line);border-radius:9px;padding:7px 10px 7px 30px;font:13px var(--sans);background:var(--card2);color:var(--ink)}
.swFind input:focus{outline:2px solid rgba(74,107,120,.35);border-color:var(--slate);background:var(--card)}
.swSeg{display:flex;gap:3px;background:var(--paper2);border-radius:9px;padding:3px;min-width:0}
.swSeg[hidden]{display:none}
.swSeg button{flex:1 1 0;min-width:0;overflow:hidden;text-overflow:ellipsis;border:0;background:transparent;border-radius:7px;padding:5px 6px;font:600 11.5px var(--sans);color:var(--ink70);cursor:pointer;white-space:nowrap}
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
.swTag.cust{background:#efe8f6;color:#5d3f82}
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
.swQr [data-r2=release]{margin-left:auto;flex:none}
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
.swSheetMsg[hidden]{display:none}
.swName{position:absolute;left:10px;right:10px;top:10px;z-index:6;display:flex;align-items:center;gap:6px;padding:6px;background:var(--card);border:1px solid var(--line);border-radius:12px;box-shadow:0 12px 30px rgba(0,0,0,.14);font:12px var(--sans);color:var(--ink70);animation:swDrop .22s ${EASE} both}
.swName[hidden]{display:none}
.swName input{flex:1;min-width:0;border:1px solid var(--line);border-radius:8px;padding:5px 8px;font:13px var(--sans);background:var(--card2);color:var(--ink)}
.swName input:focus{outline:2px solid rgba(74,107,120,.35);border-color:var(--slate);background:var(--card)}
.swName .swIcon{width:26px;height:26px}.swName .swIcon svg{width:12px;height:12px}
.swName .swAsk{flex:1;min-width:0;padding:0 4px;color:var(--ink);line-height:1.35}
/* a long step list or suggestion list scrolls in its own box: the orders list below always keeps room (1280×720 and the stacked layout) */
.swPane[data-pane=sheet]>.swSheetMsg{max-height:30vh;overflow:hidden auto;overscroll-behavior:contain;border-radius:12px}
.swPane[data-pane=sheet]:has(>[data-r=work]:not([hidden])):has(>[data-r=fill]:not([hidden]))>.swSheetMsg{max-height:21vh}
.swOff .then{display:flex;flex-wrap:wrap;gap:6px;align-items:center}
.swOff .then .fLabel{margin:0 4px 0 0}
.swOff .then button{display:inline-flex;align-items:center;gap:6px;border:1px solid var(--line);background:var(--card);border-radius:999px;padding:4px 11px 4px 9px;font:500 12px var(--sans);color:var(--ink70);cursor:pointer;transition:border-color .12s,background .12s,color .12s}
.swOff .then button svg{width:13px;height:13px}
.swOff .then button[data-then=hold][aria-checked=true]{border-color:var(--slate);background:var(--slateSoft);color:#2f4a55}
.swOff .then button[data-then=cancel][aria-checked=true]{border-color:#c98a74;background:var(--claySoft);color:#8a3a26}
.swOff .then button:disabled{opacity:.45;cursor:not-allowed}
.swOff .acts .btn svg{width:13px;height:13px}
.swNote{width:100%;box-sizing:border-box;border:1px solid var(--line);border-radius:9px;padding:6px 9px;font:12.5px var(--sans);background:var(--card);color:var(--ink)}
.swNote:focus{outline:2px solid rgba(74,107,120,.3);border-color:var(--slate)}
.swFill{border:1px solid #b9cdb5;border-radius:12px;background:linear-gradient(0deg,rgba(221,233,218,.28),rgba(221,233,218,.28)),var(--card);padding:8px 8px 7px 12px;display:grid;gap:6px;animation:swDrop .24s ${EASE} both}
.swFill h4{margin:0;display:flex;align-items:center;gap:7px;font:600 13px var(--sans);color:var(--ink);min-height:24px}
.swFill h4 svg{width:15px;height:15px;color:var(--sage);flex:none}
.swFill h4 small{font:500 10.5px var(--sans);color:var(--ink45)}
.swFill h4 .swIcon{margin-left:auto;width:24px;height:24px}
.swFill h4 .swIcon svg{width:12px;height:12px;color:var(--ink45)}
.swFill ol{list-style:none;margin:0 0 0 -8px;padding:0;display:grid;gap:2px}
.swFill li{display:grid;grid-template-columns:44px minmax(0,1fr) auto;gap:9px;align-items:center;padding:4px 4px 4px 4px;border-radius:9px;transition:background .12s;animation:swDrop .26s ${EASE} both;animation-delay:calc(var(--i) * 45ms)}
.swFill li:hover{background:rgba(95,122,91,.11)}
.swFill .th{width:44px;height:44px;border:1px solid var(--line2);border-radius:8px;background:#fff}
.swFill .t{display:grid;min-width:0;line-height:1.3}
.swFill .t b{font:600 12.5px var(--mono);color:var(--ink)}
.swFill .t small{font:11px var(--sans);color:var(--ink45);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.swFill .note{font:11.5px/1.45 var(--sans);color:var(--ink45)}
.swFill h4 [data-fl=all]{margin-left:auto;flex:none;animation:swDrop .24s ${EASE} both}.swFill h4 [data-fl=all]+.swIcon{margin-left:0}
.swHeld{display:grid;gap:5px;padding:9px 10px;border-radius:10px;border:1px solid var(--line2);margin:0 0 6px;background:var(--card);animation:swDrop .22s ${EASE} both}
.swHeld .top{display:flex;align-items:baseline;gap:8px;min-width:0}
.swHeld .no{font:600 13px var(--mono);color:var(--ink)}
.swHeld .what{font:11.5px var(--sans);color:var(--ink45);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;min-width:0;flex:1}
.swHeld .when{font:10.5px var(--mono);color:var(--ink45);flex:none}
.swHeld .why{font:11.5px/1.4 var(--sans);color:var(--ink70);display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.swHeld .acts{display:flex;gap:6px;align-items:center;flex-wrap:wrap}
.swHeld .acts.confirm .swNote{flex:1 1 100%}
.swChip[data-freed]::after{content:"";position:absolute;left:-2px;bottom:-2px;width:7px;height:7px;border-radius:2px;border:1.5px dashed var(--clay);background:var(--card)}
/* (the Orders tab's Cancelled list is styled with the Orders tab, in charm-nest-1.html) */
.swFindRow{display:flex;gap:6px;align-items:center}.swFindRow .swFind{flex:1;min-width:0}.swFindRow .swIcon[hidden]{display:none}
.swFill .a{display:flex;align-items:center;gap:3px}
.swFill .a .swIcon{width:26px;height:26px;border-radius:8px}.swFill .a .swIcon svg{width:14px;height:14px}
.swFill .a .swWait svg{width:13px;height:13px;color:var(--sage)}
.swFill .miss{font:11px var(--sans);color:var(--clay);margin-right:2px}
.swFill li.on{background:rgba(95,122,91,.14)}
.swAdd .swFind input{padding:5px 9px 5px 28px;font-size:12.5px}
.swElse{display:grid;gap:5px;margin-top:10px;justify-items:center}.swElse .btn{gap:6px}.swElse .btn svg{width:12px;height:12px}.swElse small{color:var(--ink45);font-weight:400;margin-left:4px}
.swHand{position:absolute;left:50%;bottom:14px;transform:translateX(-50%);display:flex;align-items:center;gap:4px;background:rgba(34,31,27,.92);color:#f3efe6;border-radius:999px;padding:5px 6px 5px 14px;font:12px var(--sans);box-shadow:0 8px 24px rgba(0,0,0,.2);z-index:4;white-space:nowrap;max-width:calc(100% - 24px);animation:swHandIn .22s ${EASE} both}
@keyframes swHandIn{from{opacity:0;transform:translate(-50%,8px)}}
.swHand[hidden]{display:none}
.swHand .t{display:grid;line-height:1.2;margin-right:6px}.swHand .t b{font:600 12.5px var(--mono)}.swHand .t small{font:10.5px var(--sans);color:#cdc4b2}
.swHand .swIcon{width:28px;height:28px;color:#e9e2d4}.swHand .swIcon:hover{background:rgba(255,255,255,.12);color:#fff}
.swHand .k{color:#cdc4b2;font-size:11.5px;padding:0 6px;overflow:hidden;text-overflow:ellipsis;min-width:0}.swHand .k.bad{color:#f0a58e}
.swHand .btn{color:#f3efe6;border-color:rgba(255,255,255,.22);background:transparent}.swHand .btn:hover{background:rgba(255,255,255,.1)}
.swPlate.hand canvas.swFx{cursor:move}
/* motion (Paul, 27 Sep 20:09-20:24): the row about to leave is lit where it stands; a spot on the plate glows where a
   row lands (under the drawn charms); a flying candidate row keeps its panel's layout, as a card */
.swOrd.going{background:var(--goldSoft)}.swOrd.going.cancel{background:var(--claySoft)}
.swSpot{position:absolute;pointer-events:none;border-radius:50%;opacity:.6}
.mGhost.swFly{display:block;padding:0;border:0;border-radius:0;background:none;animation:none}.mGhost.swFly>li{background:var(--card);box-shadow:0 8px 22px rgba(40,30,20,.14);animation:none}
@keyframes swSpin{to{transform:rotate(360deg)}}
@media (max-width:980px){.swBody{grid-template-columns:minmax(0,1fr);grid-template-rows:minmax(300px,1fr) auto}.swSide{border-left:0;border-top:1px solid var(--line);min-height:46vh}.swBox{overflow:auto}.swSheets{display:none}}
@media (prefers-reduced-motion:reduce){dialog.sheetWin[open],dialog.sheetWin::backdrop{animation:swFade .16s ease backwards}dialog.sheetWin.closing,dialog.sheetWin.closing::backdrop{animation:swFadeOut .12s ease both}.swReturn,.swSkel i{animation:none}}
`;

  { const style = h("style"); style.textContent = STYLE; document.head.appendChild(style); }   // (the Orders tab's Cancelled list uses it too)

  /* ── state ── */
  const W = {
    dlg: null, el: {}, id: null, rec: null, live: null, st: null, pieces: [], byId: new Map(), byPool: new Map(), orders: new Map(),
    set: null, setSheets: [], sel: null, hover: null, view: "sheet", q: "", filter: "all", token: 0, geom: false, view0: null,
    k: 1, R: 0, dpr: 1, showBacks: true, fx: [], raf: 0, pools: new Map(), trailFor: null, from: null, ro: null, freed: [], work: null, fill: null, fillRun: 0, ghost: null, flow: null,
    // motion: orders leaving with the change in progress (rid → { how, keep }), orders drawn where they land until
    // their sheet is saved, the list's view last drawn, and how long a flight keeps the view it started from
    going: new Map(), landing: [], inbound: new Map(), listKey: null, listQ: null, stay: 0
  };

  function build() {
    if (W.dlg) return;
    const d = h("dialog", "sheetWin"); d.setAttribute("aria-label", "Sheet");
    d.innerHTML = `<div class="swBox" tabindex="-1" autofocus>
      <header class="swHead" data-r="headBar">
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
          <div class="swPlateBox" data-r="plateBox"><div class="swPlate" data-r="plate"><canvas class="swRule" data-r="rule"></canvas><img class="swPv" data-r="pv" alt="" crossorigin="anonymous"><canvas class="swBase" data-r="base"></canvas><canvas class="swFx" data-r="fx"></canvas><div class="swTip" data-r="tip"></div></div>
            <div class="swHand" data-r="hand" hidden></div><div class="swVeil" data-r="veil" hidden><span class="owSpin"></span><span data-r="veilText">Opening the sheet…</span></div></div>
          <footer class="swStrip" data-r="strip"></footer>
        </section>
        <aside class="swSide" data-r="side">
          <div class="swName" data-r="name" role="group" aria-label="Your name" hidden></div>
          <div class="swSkel" data-r="skel" aria-hidden="true" hidden></div>
          <div class="swPane" data-pane="sheet">
            <div class="swPaneHead"><div class="swFindRow"><label class="swFind">${ICON.search}<input data-r="find" type="search" placeholder="Find an order or SKU on this sheet" autocomplete="off" spellcheck="false"></label><button type="button" class="swIcon" data-r="addBtn" title="Add an order to this sheet" aria-label="Add an order to this sheet" hidden>${ICON.add}</button></div><div class="swSeg" data-r="seg" role="group" aria-label="Show"></div></div>
            <div class="swSheetMsg" data-r="work" hidden></div>
            <div class="swSheetMsg" data-r="fill" hidden></div>
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
    // (Esc while the sheet is still flying in closes it: the view it was opening on has not been seen yet)
    d.addEventListener("cancel", e => { e.preventDefault(); if (!E.name.hidden) return hideName(); if (!E.menu.hidden) return menu(false); if (W.hand) return stopHand(); if (W.add) return closeAdd(); if (W.view === "piece" && !(W.flip && !W.flip.landed)) return showSheetPane(); close(); });
    d.addEventListener("close", () => cleanup());
    d.addEventListener("click", e => { if (e.target === d) close(); if (!E.menu.hidden && !e.target.closest(".swMenuWrap")) menu(false); });
    E.moreBtn.onclick = () => menu(E.menu.hidden);
    E.find.oninput = () => { W.q = E.find.value.trim().toLowerCase(); renderOrders(); paintFx(); };
    E.toSheet.onclick = () => showSheetPane();
    E.prev.onclick = () => step(-1);
    E.next.onclick = () => step(1);
    d.addEventListener("keydown", e => {
      if (e.target.closest("input,textarea,[contenteditable]")) return;
      if (W.hand) {
        const turn = { "[": -10, "]": 10, "{": -2, "}": 2, r: 10, R: -10 }[e.key];
        if (turn) { e.preventDefault(); return handTurn(turn); }
        if (e.key === "Enter") { e.preventDefault(); return handDrop(); }
      }
      if (W.view === "piece" && (e.key === "ArrowLeft" || e.key === "ArrowRight")) { e.preventDefault(); step(e.key === "ArrowLeft" ? -1 : 1); }
    });
    const fx = E.fx;
    fx.addEventListener("pointermove", e => { W.pointer = e; if (!W.moveFrame) W.moveFrame = requestAnimationFrame(() => { W.moveFrame = 0; onMove(W.pointer); }); });
    fx.addEventListener("pointerleave", () => { if (!W.hand) { setHover(null); tip(null); } });
    fx.addEventListener("wheel", e => {
      if (!W.hand) return; e.preventDefault();
      const h = W.hand; h.acc = (h.acc || 0) + (e.deltaY || e.deltaX);
      if (Math.abs(h.acc) >= 40 || e.deltaMode) { handTurn(Math.sign(h.acc) * (e.shiftKey ? 2 : 10)); h.acc = 0; }
    }, { passive: false });
    E.addBtn.onclick = () => openAdd("");
    fx.addEventListener("click", e => { if (W.hand) { onMove(e); return handDrop(); } const p = hitAt(e); if (p) selectPiece(p, { from: "canvas" }); else if (W.view === "piece") showSheetPane(); });
    W.ro = new ResizeObserver(() => { if (W.dlg.open) fitPlate(); });
    W.ro.observe(E.plateBox);
  }

  /* ── motion (Paul, 27 Sep 20:09-20:24: "anytime things would disappear from my user screen due to clicking in action
     have to have a accompanied ... animation of what is actually happening and where things are being removed from and
     moved into ... fairly slow moving"). A row that leaves this window's lists is seen going where it went: a copy flies
     to the view that holds it now (On hold, All), or folds away where it stood when the order leaves the sorter; what
     stays glides into its room; a short note inside the window says what happened. An order placed on this sheet flies
     from its row onto the plate where it lands, and is drawn there until the sheet is saved with it. Only copies move:
     every change is saved at once, as before (charm-nest-motion.js). ── */
  const Mo = () => window.Motion || null;
  // (a note of the page would sit under this window, which is above the whole page: the window keeps its own)
  function swNote(anchor, spec) {
    const M = Mo(); if (!M || !W.dlg || !W.dlg.open) return null;
    const n = M.note(anchor, spec); if (!n) return null;
    if (n.parentNode !== W.dlg) W.dlg.appendChild(n);
    // (inside the window's edge: a note under something at the right would hang over the page beside it. Kept there
    // each time Motion places its notes again, as it does when another one closes)
    const ar = n.querySelector(".mNoteArrow");
    const fit = () => {
      if (!n.isConnected || !W.dlg.open) return;
      const d = W.dlg.getBoundingClientRect(), x = parseFloat(n.style.left) || 0, w = n.offsetWidth, over = Math.max(0, x + w - (d.right - 10));
      const v = over ? `${-over}px 0` : "", va = over && ar ? `${Math.max(0, Math.min(over, w - 20 - (parseFloat(ar.style.left) || 12)))}px 0` : "";
      if (n.style.translate !== v) n.style.translate = v;
      if (ar && ar.style.translate !== va) ar.style.translate = va;
    };
    new MutationObserver(fit).observe(n, { attributes: true, attributeFilter: ["style"] }); fit();
    return n;
  }
  // where a note about the whole list stands: under its first view, or under the search
  const listHead = () => (W.el.seg && !W.el.seg.hidden && W.el.seg.querySelector("[data-f]")) || (W.el.side && W.el.side.querySelector(".swFind svg"));
  const segBtn = f => () => (W.el.seg && !W.el.seg.hidden && W.el.seg.querySelector(`[data-f="${f}"]`)) || null;
  const rowFor = rid => W.el.orders.querySelector(`li[data-mkey="${CSS.escape("o:" + rid)}"],li[data-mkey="${CSS.escape("h:" + rid)}"]`);
  /** A still copy of a row where it stands (none while it is out of sight, or without Motion). */
  function copyOf(node) {
    const M = Mo(); if (!M || !node || !node.isConnected) return null;
    const r = node.getBoundingClientRect(); if (!r.width || !r.height) return null;
    const box = node.closest(".swScroll,.swSheetMsg"), c = box ? box.getBoundingClientRect() : null;
    if (c && (r.bottom <= c.top + 4 || r.top >= c.bottom - 4)) return null;
    return M.ghost(node, r, null, node);
  }
  /** The copy of a row whose order leaves the sorter folds shut where it stood, tinted clay, on the same curve as the
   *  rows below close up (so its lower edge meets the next row as it rises, instead of the rows sliding under it). */
  function fold(g, ms) {
    if (!g) return Promise.resolve();
    if (still() || !Mo()) { g.remove(); return Promise.resolve(); }
    ms = ms || Mo().T.slide;
    if (g._card) g._card.animate([{ backgroundColor: "#f4e3dc", offset: .18 }, { backgroundColor: "#f4e3dc" }], { duration: ms, fill: "forwards" });
    g.style.transformOrigin = "50% 0";
    const p = g.animate([{ opacity: 1, transform: "none", filter: "none" }, { opacity: .85, transform: "scale(.99,.5)", offset: .5 }, { opacity: 0, transform: "scale(.97,0)", filter: "blur(1px)" }], { duration: ms, easing: "cubic-bezier(.3,.1,.2,1)", fill: "forwards" }).finished.catch(() => {}).then(() => g.remove());
    // it moves with the list while a panel above it unfolds and pushes the list down
    const E = W.el, y0 = g._top0; let on = y0 != null;
    const tick = () => { if (!on) return; if (E.orders.getClientRects().length) g.style.translate = `0 ${E.orders.getBoundingClientRect().top - y0}px`; requestAnimationFrame(tick); };
    if (on) { requestAnimationFrame(tick); p.then(() => { on = false; }); }
    return p;
  }
  // what is above the orders list opens, closes or changes size without the list jumping: a panel (or the row of
  // views) that opens or grows unfolds to its height, pushing the list down as it goes; one that closes or shrinks
  // leaves the list where it stood, and the list glides up into the room (started before the rows' own glides are
  // measured, so the two add up)
  function listTop() { const E = W.el; return W.view === "sheet" && Mo() && !still() && E.orders && E.orders.getClientRects().length ? E.orders.getBoundingClientRect().top : null; }
  function shiftFrom(t0) {
    if (t0 == null) return;
    const E = W.el; for (const a of E.orders.getAnimations()) if (a.id === "swShift") a.cancel();
    const dy = t0 - E.orders.getBoundingClientRect().top; if (Math.abs(dy) < 1) return;
    const a = E.orders.animate([{ transform: `translateY(${dy}px)` }, { transform: "none" }], { duration: Mo().T.slide, easing: "cubic-bezier(.3,.1,.2,1)" }); a.id = "swShift";
  }
  function steady(el, fn) {
    const t0 = listTop(); if (t0 == null || !el) return fn();
    const h0 = el.hidden ? 0 : el.getBoundingClientRect().height;
    fn();
    for (const a of el.getAnimations()) if (a.id === "swUnfold") a.cancel();
    const h1 = el.hidden ? 0 : el.getBoundingClientRect().height;
    if (h1 > h0 + 1) { const a = el.animate([{ height: h0 + "px", overflow: "hidden" }, { height: h1 + "px", overflow: "hidden" }], { duration: 620, easing: "cubic-bezier(.3,.1,.2,1)" }); a.id = "swUnfold"; }
    else shiftFrom(t0);
  }
  /** A row leaves a plain list now: the rows after it glide up into its room instead of jumping. */
  function closeGap(node) {
    const list = node.parentElement; if (!list) return;
    const rest = [...list.children].filter(n => n !== node), was = rest.map(n => n.getBoundingClientRect().top);
    node.remove(); if (still() || !Mo()) return;
    rest.forEach((n, i) => { const dy = was[i] - n.getBoundingClientRect().top; if (Math.abs(dy) > .5) n.animate([{ transform: `translateY(${dy}px)` }, { transform: "none" }], { duration: Mo().T.slide, easing: "cubic-bezier(.3,.1,.2,1)" }); });
  }
  /** The rows of the orders list, as one keyed update (Motion.reconcile): a row is named by its order, so a row drawn
   *  again under the same name glides to its new place instead of flashing; a row that left without a copy of its own
   *  fades where it stood; another view of the list fades in. */
  function putRows(html) {
    const E = W.el, t = document.createElement("template"); t.innerHTML = html;
    const nodes = [...t.content.children], key = W.id + "|" + W.filter, same = W.listKey === key && W.listQ === W.q;
    const swapped = !!W.listKey && W.listKey !== key && W.listKey.startsWith(W.id + "|");
    W.listKey = key; W.listQ = W.q;
    const M = Mo(); if (M) M.reconcile(E.orders, nodes, { animate: same, clip: E.orders.parentElement }); else E.orders.replaceChildren(...nodes);
    if (swapped) animate(E.orders, [{ opacity: 0 }, { opacity: 1 }], 300, { fill: "none" });
  }
  /** A take-off or a cancel: the order's row goes where the order went. From the charm's own pane the list slides back
   *  first with the row still in it, lit, and the row leaves once the list has settled; from the list it leaves at once.
   *  The note waits for both: the change is in (cue.done) and the copy has landed. */
  function offCue(rid, how, text) {
    if (!rid) return null;
    const fromPiece = W.view === "piece", cue = { rid, how, text, ok: false, down: false, said: false, dead: false, g: null };
    W.going.set(rid, { how, keep: fromPiece });
    if (!fromPiece) cue.g = lift(rid);
    cue.go = () => {
      if (!fromPiece) return send(cue);
      const li = rowFor(rid); if (li && W.view === "sheet") li.scrollIntoView({ block: "nearest" });
      setTimeout(() => {
        if (cue.dead) return;
        const e = W.going.get(rid); if (e) e.keep = false;
        if (W.dlg.open && W.view === "sheet" && W.rec) { cue.g = lift(rid); renderSheetPane(); }
        send(cue);
      }, still() ? 0 : 420);
    };
    cue.done = () => { cue.ok = true; say(cue); };
    // once the change has ended, what the lists say is the truth again
    cue.end = () => { const e = W.going.get(rid); if (e && e.how === how) W.going.delete(rid); };
    cue.fail = () => { cue.dead = cue.said = true; cue.end(); if (W.dlg && W.dlg.open && W.view === "sheet" && W.rec) renderSheetPane(); };
    return cue;
  }
  // the row of an order as it stands, copied; the row itself leaves with the list's next update when the order leaves
  // this list (a held order, or one with no piece left on this sheet), else it stays and only its copy goes
  function lift(rid) {
    const li = rowFor(rid), g = copyOf(li); if (!g) return null;
    g._leaves = li.classList.contains("swHeld") || !(W.orders.get(rid) || []).some(x => !x.gone);
    if (g._leaves) { li._mLeaving = true; li.style.visibility = "hidden"; g._top0 = W.el.orders.getBoundingClientRect().top; }
    // (the last one on hold: the list stays on On hold while it goes, and turns to All once it has gone)
    if (g._leaves && li.classList.contains("swHeld")) W.stay = Date.now() + 1600;
    return g;
  }
  function send(cue) {
    const g = cue.g, M = Mo(); cue.g = null;
    let p = Promise.resolve();
    if (cue.how === "hold") { const to = segBtn("hold"); if (g && M) p = M.fly(g, to, { plus: "+1" }); else if (M) M.pulse(to); }
    else if (g && g._leaves) p = fold(g);
    else if (g) g.remove();
    p.then(() => {
      if (W.stay) { W.stay = 0; if (W.dlg.open && W.view === "sheet" && W.rec && W.filter === "hold" && !heldOrders().length) renderSheetPane(); }
      cue.down = true; say(cue);
    });
  }
  function say(cue) {
    if (cue.said || !cue.ok || !cue.down) return; cue.said = true;
    const rid = cue.rid;
    if (cue.how === "hold") swNote(() => segBtn("hold")() || listHead(), { text: cue.text || `Order ${rid} is on hold`, actions: [{ label: "Show", title: "Show the orders on hold", fn: () => showFilter("hold") }] });
    else swNote(listHead, { text: `Order ${rid} cancelled · kept under Orders › Cancelled`, actions: window.Orders && Orders.showPile ? [{ label: "Show", title: "Open Orders › Cancelled", fn: () => close().then(() => { setMode("orders"); Orders.showPile("cancelled", rid); }) }] : [] });
  }
  function showFilter(f) { if (!W.dlg.open || !W.rec) return; W.filter = f; if (W.view !== "sheet") showSheetPane(); else renderSheetPane(); }
  /** A released order goes back in line: its row flies to All, and the note says what that means. The list stays on
   *  On hold until the copy has landed (the last one out then turns the list to All, where it landed). */
  function backInLine(li, rid) {
    const M = Mo(), g = W.view === "sheet" ? copyOf(li) : null, to = segBtn("all");
    const text = `Order ${rid} is back in line · the run places it on the next sheet that fits`;
    if (!g) { if (!swNote(listHead, { text })) toast(`Order ${rid} is back in line`, "ok", 5000); return; }
    li._mLeaving = true; li.style.visibility = "hidden";
    W.stay = Date.now() + M.T.fly + 600;
    M.fly(g, to, { plus: false }).then(() => {
      W.stay = 0; swNote(() => to() || listHead(), { text });
      if (W.dlg.open && W.view === "sheet" && W.rec && W.filter === "hold" && !heldOrders().length) renderSheetPane();
    });
  }
  /* an order placed on this sheet: its row flies onto the plate where it lands; the spot glows under the drawn charms,
     and the order is drawn there, in sage with a dashed edge, until the sheet is saved with it */
  function spotMark(spots) {
    if (!spots || !spots.length || !W.st || !W.dlg.open) return null;
    const k = W.k / W.dpr, R = W.R / W.dpr; let x0 = 1e9, y0 = 1e9, x1 = -1e9, y1 = -1e9;
    for (const s of spots) { const c = s.c || {}, r = Math.max(+c.widthPt || 20, +c.heightPt || 20) / 2; x0 = Math.min(x0, s.cxPt - r); y0 = Math.min(y0, s.cyPt - r); x1 = Math.max(x1, s.cxPt + r); y1 = Math.max(y1, s.cyPt + r); }
    const m = h("i", "swSpot"); m.setAttribute("aria-hidden", "true");
    Object.assign(m.style, { left: R + x0 * k + "px", top: R + y0 * k + "px", width: Math.max(10, (x1 - x0) * k) + "px", height: Math.max(10, (y1 - y0) * k) + "px" });
    W.el.plate.insertBefore(m, W.el.fx); setTimeout(() => m.remove(), 6000);
    return m;
  }
  const ring = () => { if (still()) return paintFx(); W.fx.push({ kind: "land", t0: performance.now(), ms: 1100 }); fxLoop(); };
  // o.draw: drawn where it lands until saved (not when the nest picks the spot itself); o.now: drawn at once (it was
  // put there by hand); o.delay: several rows leave one after another
  function landRow(li, spots, rid, o = {}) {
    const l = o.draw ? { rid, sheet: W.id, spots, t0: performance.now(), shown: !!o.now } : null;
    if (l) { W.landing = W.landing.filter(x => x.rid !== rid).concat(l); if (l.shown) ring(); }
    const M = Mo(), m = spotMark(spots), g = m ? copyOf(li) : null;
    const down = () => { if (l && !l.shown && W.landing.includes(l)) { l.shown = true; l.t0 = performance.now(); ring(); } };
    if (!M || !g) { if (M && m) M.pulse(m); down(); return; }
    g.classList.add("swFill", "swFly");
    const f = { g, m }; W.inbound.set(rid, f);
    setTimeout(() => { if (f.off) return g.remove(); M.fly(g, m, { plus: false }).then(() => { if (W.inbound.get(rid) === f) W.inbound.delete(rid); down(); }); }, o.delay || 0);
  }
  // an order that did not go in: it is not drawn as landed, and a copy still on its way fades where it is
  function unland(rid) {
    const f = W.inbound.get(rid); if (f) { W.inbound.delete(rid); f.off = true; f.m && f.m.remove(); if (f.g.isConnected) f.g.animate([{ opacity: 0 }], { duration: 320, fill: "forwards" }); }
    const n = W.landing.length; W.landing = W.landing.filter(l => l.rid !== rid); if (n !== W.landing.length && W.dlg && W.dlg.open) paintFx();
  }
  function paintLanding(ctx, now) {
    if (!W.landing.length || !W.st) return;
    const k = W.k, t1 = now || performance.now();
    for (const l of W.landing) {
      if (!l.shown || l.sheet !== W.id) continue;
      const t = still() ? 1 : Math.min(1, (t1 - l.t0) / 1100);
      for (const s of l.spots) {
        const x = { p: { cxPt: s.cxPt, cyPt: s.cyPt, angle: s.angle, scale: 1 }, c: s.c }; if (!x.c) continue;
        withPiece(ctx, x, () => {
          ctx.globalAlpha = Math.min(1, .3 + t * 2);
          ctx.fillStyle = "rgba(95,122,91,.22)"; outlinePath(ctx, x, true); ctx.fill("evenodd");
          CharmNestPDF.drawCharm(ctx, x.c, tx0(x.c), k);
          ctx.setLineDash([4 * W.dpr, 3 * W.dpr]); outlinePath(ctx, x); ctx.strokeStyle = "#5f7a5b"; ctx.lineWidth = 1.5 * W.dpr; ctx.stroke(); ctx.setLineDash([]);
          if (t < 1) { const sc = 1 + .28 * t; ctx.scale(sc, sc); outlinePath(ctx, x); ctx.strokeStyle = `rgba(95,122,91,${.85 * (1 - t)})`; ctx.lineWidth = 3 * W.dpr / sc; ctx.stroke(); }
          ctx.globalAlpha = 1;
        });
      }
    }
  }

  /* ── open / close ──
     The window grows out of the sheet that was clicked and goes back into it (Paul, 28 Sep: "that sheet should expand as
     an animation to fill the screen, and then the options on the right hand side should fade in ... so the user can
     naturally understand what they clicked", landing on the charm clicked; the Library's sheets the same). What was
     clicked is the window's origin: the frame it was shown in, the sheet inside that frame, the frame's colour, and what
     the sheet shows there: a Nest card's live sheet (drawn here at the window's own size, so it stays sharp as it grows)
     or a Library card's own picture (the window's preview shows the very same picture); on a Library picture, the point
     clicked. While it opens, the window's surface grows out of the frame, opaque from the first frame, in the frame's
     colour and taking the window's own colours as it comes; the sheet grows from where it lay to the plate with its
     rulers; the panel grows with the surface, holding the shape of what it will show until it has it, then shows it
     part by part; the charm clicked is marked from the first frame and rings as the sheet comes to rest; the header
     settles last. Closing plays it back: the panel, header, rulers and highlights go first while the surface turns the
     frame's colour, then the sheet and the surface shrink into the frame together; where the frame is no longer on
     screen the window fades. Only transforms and opacity move, and nothing waits on them: a click, Esc or a close in
     flight is taken at once (a close turns the flight round from wherever it is). (Polished the same day after a review
     at real reading times: a growth that can be followed, not a pop.) */
  // what was pressed that opens a sheet: a Library card (the Sets view's and a set's in Completed too), a Completed
  // row, a Sets window tile
  let pressed = null;
  document.addEventListener("pointerdown", e => {
    const t = e.target && e.target.closest && e.target.closest(".libCard[data-id], .ldItem[data-kind=sheet][data-id] > .ldLine, .hTile[data-sheet]"); if (!t) return;
    const id = t.dataset.id || t.dataset.sheet || (t.parentElement && t.parentElement.dataset.id); if (!id) return;
    pressed = { id, at: Date.now(), el: t, x: e.clientX, y: e.clientY };
  }, true);
  const tryDo = f => { try { return f ? f() : null; } catch (_) { return null; } };
  const onScreen = r => !!r && r.width > 1 && r.height > 1 && r.bottom > 0 && r.top < innerHeight && r.right > 0 && r.left < innerWidth;
  const shown = el => !!el && el.isConnected && el.getClientRects().length > 0;
  /** The largest box of aspect a (w/h) centred in r. */
  const fitIn = (r, a) => { let w = r.width, hh = a ? w / a : r.height; if (hh > r.height) { hh = r.height; w = hh * a; } return new DOMRect(r.left + (r.width - w) / 2, r.top + (r.height - hh) / 2, w, hh); };
  /** Where a picture is drawn inside its img box: its content box (a true-scale preview is padded inside its 100 × 50 mm
      frame), the picture fitted into it as object-fit: contain draws it. */
  function picBox(im) {
    const r = im.getBoundingClientRect(), cs = getComputedStyle(im), n = v => parseFloat(v) || 0;
    const l = n(cs.borderLeftWidth) + n(cs.paddingLeft), t = n(cs.borderTopWidth) + n(cs.paddingTop);
    const box = new DOMRect(r.left + l, r.top + t, r.width - l - n(cs.borderRightWidth) - n(cs.paddingRight), r.height - t - n(cs.borderBottomWidth) - n(cs.paddingBottom));
    return im.naturalWidth && im.naturalHeight ? fitIn(box, im.naturalWidth / im.naturalHeight) : box;
  }
  const loaded = im => !!im && im.complete && im.naturalWidth > 0;
  /** A copy of a picture at its own size, never stretched: the stand-in while the window's preview decodes it. */
  function imgSnap(im) {
    if (!loaded(im)) return null;
    const s = Math.min(1, 2400 / im.naturalWidth), c = document.createElement("canvas");
    c.width = Math.max(1, Math.round(im.naturalWidth * s)); c.height = Math.max(1, Math.round(im.naturalHeight * s));
    try { c.getContext("2d").drawImage(im, 0, 0, c.width, c.height); } catch (_) { return null; }
    return c;
  }
  /** The colour a frame shows: its own background, or the nearest one behind it. */
  function bgOf(el) {
    for (let n = el; n && n.nodeType === 1; n = n.parentElement) { const c = getComputedStyle(n).backgroundColor; if (c && c !== "transparent" && !/^rgba\(.*,\s*0\)$/.test(c)) return c; }
    return "";
  }
  /** A Library card as an origin: the card is the frame, its preview's picture the sheet; where it was pressed, on the
      sheet in points. Found again by its sheet when the card was drawn anew while the window was open. */
  function cardOrigin(id, card0, at) {
    const lib = (S.library.rows || []).find(r => r.id === id), st = lib && lib.stock ? stockOf(lib) : null;
    const cardNow = () => shown(card0) ? card0 : [...document.querySelectorAll(`.libCard[data-id="${CSS.escape(id)}"]`)].find(shown) || null;
    const pic = c => { const im = c && c.querySelector("img.pv"); return shown(im) ? { im, r: picBox(im) } : null; };
    const o = {
      id, stock: st, tint: bgOf(card0),
      rects() { const c = cardNow(); if (!c) return null; const p = pic(c); return { box: c.getBoundingClientRect(), sheet: p ? p.r : null }; },
      img() { const p = pic(card0) || pic(cardNow()); return p ? p.im : null; }
    };
    const p = at && st && pic(card0);
    if (p && p.r.width > 2) { const u = (at.x - p.r.left) / p.r.width, v = (at.y - p.r.top) / p.r.height; if (u >= 0 && u <= 1 && v >= 0 && v <= 1) o.at = { xPt: u * st.wPt, yPt: v * st.hPt }; }
    return o;
  }
  function originOf(id, opts) {
    if (opts.origin) return Object.assign({ id }, opts.origin);
    const p = pressed && pressed.id === id && Date.now() - pressed.at < 1500 ? pressed : null;
    if (p && p.el.classList.contains("libCard")) return cardOrigin(id, p.el, p);
    // anything else: the rectangle it was opened from (a Completed row, a Sets window tile, the pill back from Engrave),
    // with its picture where it shows one; a row still on screen at the close takes the window back
    const img = p && p.el.querySelector && p.el.querySelector("img.hThumb, img.pv"), r0 = opts.fromRect && opts.fromRect.width ? opts.fromRect : null;
    if (!r0 && !(p && shown(p.el))) return null;
    let first = true;
    return {
      id, tint: p ? bgOf(p.el) : "",
      rects() {
        const was = first; first = false;
        if (p && shown(p.el) && (!r0 || !was)) return { box: p.el.getBoundingClientRect(), sheet: shown(img) && img.naturalWidth ? picBox(img) : null };
        return was && r0 ? { box: r0, sheet: img && img.naturalWidth ? r0 : null } : null;
      },
      img: img ? () => img : null
    };
  }
  /** What the plate shows before the sheet's record is read: a Nest card's own sheet (its placements and designs, held
      by the page), with the charm clicked; or, on a Library picture, the point clicked. */
  function preOf(o, opts) {
    const sh = o && o.sheet, pre = { pieces: null, sel: null, at: opts.select ? null : opts.at || (o && o.at) || null };
    if (sh && sh.placements && sh.placements.length && sh.charms && sh.charms.length) {
      const byId = new Map(sh.charms.map(c => [c.id, c]));
      const list = sh.placements.map(p => { const c = byId.get(p.id); return c && c.outline ? { id: p.id, poolId: c.poolId || null, p: { cxPt: +p.cxPt, cyPt: +p.cyPt, angle: +p.angle || 0, scale: +p.scale || 1, wPt: +p.wPt || 10, hPt: +p.hPt || 10 }, c } : null; });
      if (list.every(Boolean)) { pre.pieces = list; if (opts.select) pre.sel = list.find(x => x.poolId === opts.select || x.id === opts.select) || null; }
    }
    return pre.pieces || pre.at ? pre : null;
  }
  // an opacity set at once, not eased from the last opening's
  const setNow = (el, v) => { el.style.transition = "none"; el.style.opacity = v; getComputedStyle(el).opacity; el.style.transition = ""; };
  /** The window as its surface: a curtain the window's own colours (its header band and its panel on the paper), laid
      over the window's box, that the opening grows and the closing shrinks; over them, the frame's own colour, which the
      opening fades away and the closing brings back. */
  function curtain(tint) {
    const d = W.dlg, D = d.getBoundingClientRect(), hb = W.el.headBar.getBoundingClientRect(), sb = W.el.side.getBoundingClientRect();
    const c = h("div", "swCurtain", `<i style="left:0;top:0;right:0;height:${Math.max(0, hb.bottom - D.top)}px"></i><i style="left:${sb.left - D.left}px;top:${sb.top - D.top}px;width:${sb.width}px;height:${sb.height}px"></i><b></b>`);
    c.lastChild.style.background = tint || "var(--card)";
    c.setAttribute("aria-hidden", "true"); d.prepend(c); return c;
  }
  /** The plate's lift while it flies (the plate itself is bare then, so that its rulers can come and go): its shadow, on
      its own box, under it. */
  function shade() {
    const E = W.el, s = h("i", "swShade"); s.setAttribute("aria-hidden", "true");
    Object.assign(s.style, { left: E.plate.offsetLeft + "px", top: E.plate.offsetTop + "px", width: (W.cssW || E.plate.offsetWidth) + "px", height: (W.cssH || E.plate.offsetHeight) + "px" });
    E.plateBox.insertBefore(s, E.plate); return s;
  }
  /** The shape of what the panel will show (a charm, or the orders), held until it has it. */
  function skeleton(piece) {
    const sk = W.el.skel, bar = (w, hh, more) => `<i style="width:${w};height:${hh}px${more ? ";" + more : ""}"></i>`, col = (gap, s) => `<div style="display:grid;gap:${gap}px;justify-items:start">${s}</div>`;
    sk.innerHTML = piece
      ? `<div class="swNav" style="min-height:45px">${bar("96px", 13)}<span style="margin-left:auto"></span>${bar("78px", 10)}${bar("16px", 16, "margin-left:14px")}${bar("16px", 16, "margin-left:12px;margin-right:6px")}</div>
         <div class="swDetail">${col(8, bar("44px", 9) + bar("168px", 24))}
           <div class="swPiece">${bar("92px", 92, "border-radius:11px")}${col(9, bar("112px", 13) + bar("86px", 11) + bar("64px", 11))}</div>
           ${bar("100%", 38, "border-radius:12px")}${col(8, bar("118px", 9) + bar("100%", 34, "border-radius:10px") + bar("100%", 34, "border-radius:10px"))}${col(8, bar("72px", 9) + bar("100%", 160, "border-radius:12px"))}</div>`
      : `<div class="swPaneHead">${bar("100%", 32, "border-radius:9px")}</div>
         <div style="display:grid;gap:4px;padding:8px 18px">${Array.from({ length: 9 }, (_, i) => `<div class="row" style="height:36px">${bar("98px", 12)}${bar(`${34 + (i * 23) % 44}%`, 10)}</div>`).join("")}</div>`;
    sk.hidden = false;
  }
  // the sheet inside the plate (the rulers left out), in the plate's own pixels
  const plateSheet = () => { const R = W.R / W.dpr; return { R, w: W.cssW - R, h: W.cssH - R }; };
  function placeSnap() { const s = W.snap; if (!s) return; const a = plateSheet(); Object.assign(s.style, { left: a.R + "px", top: a.R + "px", width: a.w + "px", height: a.h + "px" }); }
  function dropSnap(fade) {
    const s = W.snap; if (!s) return; W.snap = null;
    if (!fade || still() || !s.isConnected) { s.remove(); return; }
    s.animate([{ opacity: 1 }, { opacity: 0 }], { duration: 340, easing: "ease", fill: "forwards" }).finished.catch(() => {}).then(() => s.remove());
  }
  /** The transform that puts the plate, as laid out at P, with its sheet over S (origin at its top left). */
  function plateTo(P, S) {
    const a = plateSheet(), sx = S.width / a.w, sy = S.height / a.h;
    return `translate(${S.left - P.left - a.R * sx}px,${S.top - P.top - a.R * sy}px) scale(${sx},${sy})`;
  }
  const curtainTo = (D, B) => `translate(${B.left - D.left}px,${B.top - D.top}px) scale(${B.width / D.width},${B.height / D.height})`;
  // a timer that keeps the animations' time (it slows with them where they are slowed, as when they are inspected)
  const clock = (F, ms) => { const a = W.el.side.animate([], { duration: ms }); F.clocks.push(a); return a.finished.then(() => true, () => false); };
  // the growth: slow to leave, most of the way in the middle, a long settle (the surface a little ahead of the sheet)
  const GROW = "cubic-bezier(.3,0,.1,1)", GROW_MS = 650, SURFACE_MS = 600, CUE_MS = 420, PANEL_MS = 380, HEAD_MS = 560, HOLD_MS = 2000;
  const keyOf = x => x.poolId || x.id;

  function flipOpen(o, piece) {
    const d = W.dlg, E = W.el, got = tryDo(o.rects);
    if (!got || !got.box || !onScreen(got.box)) return false;
    fitPlate(true);
    const P = E.plate.getBoundingClientRect(), D = d.getBoundingClientRect();
    if (!W.st || P.width < 40 || P.height < 30 || !D.width) return false;
    const a = plateSheet(), B = got.box, S = got.sheet && got.sheet.width > 2 && got.sheet.height > 2 ? got.sheet : fitIn(B, a.w / a.h);
    const F = W.flip = { anims: [], clocks: [], landed: false, cued: false, onLand: [], onCue: [], ready: false, early: false, shown: false };
    const run = (el, frames, opts) => { const x = el.animate(frames, Object.assign({ duration: SURFACE_MS, easing: GROW, fill: "both" }, opts)); F.anims.push(x); return x; };
    // what the sheet shows as it grows: a Nest card's sheet is drawn on the plate already; a picture is the window's
    // preview, or, until the preview has decoded it, a copy of it at its own size; else the card's own drawing
    if (!(W.pre && W.pre.pieces)) {
      const im = o.img ? tryDo(o.img) : null;
      const snap = im ? (E.pv.getAttribute("src") && loaded(E.pv) ? null : imgSnap(im)) : tryDo(o.snap);
      if (snap) {
        snap.className = "swSnap"; W.snap = snap; placeSnap(); E.plate.insertBefore(snap, E.fx);
        if (im) E.pv.addEventListener("load", () => { if (W.snap === snap) dropSnap(false); }, { once: true });
      }
    }
    W.curtain = curtain(o.tint); W.shade = shade();
    const Dc = W.curtain.getBoundingClientRect(), C = curtainTo(Dc, B), T = plateTo(P, S);
    // the surface grows out of the frame, opaque, the frame's colour giving way to the window's; the sheet grows from
    // where it lay to the plate, its rulers and its lift coming with it
    run(W.curtain, [{ transform: C }, { transform: "none" }]);
    run(W.curtain.lastChild, [{ opacity: 1 }, { opacity: 0 }], { duration: 250, easing: "cubic-bezier(.4,0,.6,1)" });
    const fly = run(E.plate, [{ transform: T }, { transform: "none" }], { duration: GROW_MS });
    run(W.shade, [{ transform: T }, { transform: "none" }], { duration: GROW_MS });
    for (const el of [W.shade, E.rule]) run(el, [{ opacity: 0 }, { opacity: 1 }], { duration: 220, easing: "linear" });
    // the panel grows as part of the surface, holding the shape of what it will show
    const sb = E.side.getBoundingClientRect(), org = `${Dc.left - sb.left}px ${Dc.top - sb.top}px`;
    skeleton(piece); E.side.classList.add("swHush");
    run(E.side, [{ transform: C, transformOrigin: org }, { transform: "none", transformOrigin: org }]);
    run(E.side, [{ opacity: 0 }, { opacity: 1 }], { duration: 250, easing: "cubic-bezier(.4,0,.6,1)" });
    // the header and the plate's foot once the window has its size, so they never stand outside it
    run(E.headBar, [{ opacity: 0, transform: "translateY(-8px)" }, { opacity: 1, transform: "none" }], { duration: 320, delay: HEAD_MS, easing: EASE });
    run(E.strip, [{ opacity: 0 }, { opacity: 1 }], { duration: 320, delay: 480, easing: "ease" });
    // the panel's content comes as the sheet settles, once it is read (a slow read holds the shape a while longer)
    const go = () => { if (W.flip === F) reveal(F, run); };
    clock(F, PANEL_MS).then(ok => { if (!ok) return; F.early = true; if (F.ready) go(); });
    clock(F, HOLD_MS).then(ok => { if (ok) go(); });
    clock(F, CUE_MS).then(ok => { if (ok && W.flip === F) cue(F); });
    F.readyNow = () => { F.ready = true; if (F.early) go(); };
    fly.finished.then(() => { if (W.flip === F) landed(F); }, () => {});
    return true;
  }
  /** The panel shows what it has: while the window is still growing, the shape it held gives way as its parts come in
      one after another (the piece's heading and its sections, or the list's search, list and foot); once the window
      is at rest (a slow read), what was read simply takes the shape's place. */
  function reveal(F, run) {
    if (F.shown) return; F.shown = true;
    const E = W.el, sk = E.skel;
    E.side.classList.remove("swHush");
    if (F.landed) { sk.hidden = true; settle(F); return; }
    const pane = E.side.querySelector(".swPane:not([hidden])");
    const parts = !pane ? [] : pane.dataset.pane === "piece" ? [pane.querySelector(".swNav"), ...pane.querySelector(".swDetail").children] : [...pane.children];
    parts.filter(n => n && !n.hidden && n.getClientRects().length).slice(0, 10)
      .forEach((n, i) => run(n, [{ opacity: 0, transform: "translateY(10px)" }, { opacity: 1, transform: "none" }], { duration: 380, delay: 60 + Math.min(i, 5) * 45, easing: EASE }));
    if (!sk.hidden) run(sk, [{ opacity: 1 }, { opacity: 0 }], { duration: 200, easing: "ease-out" }).finished.then(() => { if (!W.flip || W.flip === F) sk.hidden = true; }, () => {});
    settle(F);
  }
  /** The sheet comes to rest: the charm clicked rings (drawn by then, from the page or from the record). */
  function cue(F) {
    F.cued = true;
    const x = W.geom && W.sel ? W.sel : W.pre && W.pre.sel ? W.pre.sel : null;
    if (x && W.rang !== keyOf(x)) { W.rang = keyOf(x); land(x); }
    for (const f of F.onCue.splice(0)) tryDo(f);
  }
  /** The sheet is home: the window is its own surface again, the plate its own size with its own lift. */
  function landed(F) {
    const E = W.el; F.landed = true; W.flying = false;
    const flight = [W.curtain, W.curtain && W.curtain.lastChild, W.shade, E.plate, E.rule, E.side];
    for (const x of F.anims) if (x.effect && flight.includes(x.effect.target)) x.cancel();
    F.anims = F.anims.filter(x => x.playState !== "idle");
    if (W.curtain) { W.curtain.remove(); W.curtain = null; }
    if (W.shade) { W.shade.remove(); W.shade = null; }
    W.dlg.classList.remove("swFlying");
    if (W.fitLater) refit();
    if (W.geom) dropSnap(true);
    for (const f of F.onLand.splice(0)) tryDo(f);
    settle(F);
  }
  // the opening is over once the sheet is home, the panel shown, and everything that came in has come
  function settle(F) {
    if (W.flip !== F || !F.landed || !F.shown) return;
    const live = F.anims.filter(a => a.playState === "running" || a.playState === "paused" || a.pending);
    if (live.length) { Promise.all(live.map(a => a.finished.catch(() => {}))).then(() => settle(F)); return; }
    endFlip(F);
  }
  function endFlip(F) {
    if (W.flip !== F) return; W.flip = null;
    for (const x of F.anims.concat(F.clocks)) x.cancel();
    W.el.side.classList.remove("swHush"); W.el.skel.hidden = true;
  }
  /** Stops an opening wherever it is (its animations let go, its curtain and lift, or a closing's, gone): the window as
      it is laid out. (keep: the panel's held shape stays, as the panel fades with it.) */
  function stopFlip(keep) {
    const F = W.flip; W.flip = null; W.flying = false;
    if (F) for (const x of F.anims.concat(F.clocks)) x.cancel();
    if (W.curtain) { W.curtain.remove(); W.curtain = null; }
    if (W.shade) { W.shade.remove(); W.shade = null; }
    W.dlg.classList.remove("swFlying"); W.el.side.classList.remove("swHush");
    if (!keep) W.el.skel.hidden = true;
  }
  // what waits on the sheet coming to rest runs then, or now when nothing is flying
  const afterCue = fn => { if (W.flip && !W.flip.cued) W.flip.onCue.push(fn); else fn(); };
  // the plate sized again when the box it sits in changed while it flew: it eases to its new size
  function refit() {
    W.fitLater = false; const plate = W.el.plate, r0 = plate.getBoundingClientRect(); fitPlate();
    const r1 = plate.getBoundingClientRect(); if (still() || !r1.width || (Math.abs(r0.width - r1.width) < 1 && Math.abs(r0.left - r1.left) < 1 && Math.abs(r0.top - r1.top) < 1)) return;
    animate(plate, [{ transformOrigin: "0 0", transform: `translate(${r0.left - r1.left}px,${r0.top - r1.top}px) scale(${r0.width / r1.width},${r0.height / r1.height})` }, { transformOrigin: "0 0", transform: "none" }], 260, { fill: "none" });
  }
  /** Back into the frame it came from, from wherever the opening had got to: first what belongs to the open window (the
      panel, the header and the foot, the rulers, the highlights, the plate's lift) goes while the surface takes the
      frame's colour; the sheet and the surface shrink into the frame together, and are gone as they arrive on it. */
  async function flipClose(home) {
    const d = W.dlg, E = W.el, F = W.flip;
    const now = el => { if (!el) return null; const cs = getComputedStyle(el); return { opacity: cs.opacity, transform: cs.transform, origin: cs.transformOrigin }; };
    const was = new Map([E.plate, E.headBar, E.side, E.strip, E.rule, E.fx, W.curtain, W.shade].filter(Boolean).map(el => [el, now(el)]));
    const hadCurtain = F && W.curtain ? was.get(W.curtain) : null, tint0 = hadCurtain ? now(W.curtain.lastChild) : null, shade0 = F && W.shade ? was.get(W.shade) : null;
    stopFlip(!!(F && !F.shown)); W.flying = true;
    const P = E.plate.getBoundingClientRect();
    if (P.width < 40 || !W.dlg.getBoundingClientRect().width) { W.flying = false; return false; }
    const B = home.box, a = plateSheet(), S = home.sheet && home.sheet.width > 2 ? home.sheet : fitIn(B, a.w / a.h);
    d.classList.add("swFlying", "swBack");
    W.curtain = curtain(W.origin && W.origin.tint); W.shade = shade();
    const D = W.curtain.getBoundingClientRect();
    const A = [], EZ = "cubic-bezier(.4,0,.2,1)", run = (el, frames, opts) => { const x = el.animate(frames, Object.assign({ fill: "both", easing: EZ, duration: 480 }, opts)); A.push(x); return x; };
    const v = (el, k, dflt) => { const x = was.get(el); return x ? x[k] : dflt; };
    const fade = { duration: 200, easing: "linear" }, C0 = hadCurtain ? hadCurtain.transform : "none", C1 = curtainTo(D, B);
    // the panel, the header and the foot go with the surface as they fade, so none of them is left outside it
    for (const [el, ms] of [[E.side, 180], [E.headBar, 180], [E.strip, 160]]) {
      const r = el.getBoundingClientRect(), org = `${D.left - r.left}px ${D.top - r.top}px`, t0 = el === E.side ? v(el, "transform", "none") : C0;
      run(el, [{ transform: t0, transformOrigin: org }, { transform: C1, transformOrigin: org }]);
      run(el, [{ opacity: v(el, "opacity", 1) }, { opacity: 0 }], { duration: ms, easing: "cubic-bezier(.4,0,1,1)" });
    }
    for (const el of [E.rule, E.fx]) run(el, [{ opacity: v(el, "opacity", 1) }, { opacity: 0 }], fade);
    run(W.shade, [{ opacity: shade0 ? shade0.opacity : 1 }, { opacity: 0 }], fade);
    run(W.curtain.lastChild, [{ opacity: tint0 ? tint0.opacity : 0 }, { opacity: 1 }], { duration: 200, easing: "ease" });
    const T0 = v(E.plate, "transform", "none"), T1 = plateTo(P, S);
    run(W.curtain, [{ transform: C0 }, { transform: C1 }]);
    run(E.plate, [{ transform: T0 }, { transform: T1 }]);
    run(W.shade, [{ transform: T0 }, { transform: T1 }]);
    for (const el of [W.curtain, E.plate]) run(el, [{ opacity: 1 }, { opacity: 1, offset: .8 }, { opacity: 0 }], { easing: "linear" });
    // (the window's closing never waits on frames that a hidden tab does not draw)
    await Promise.race([Promise.all(A.map(x => x.finished.catch(() => {}))), new Promise(r => setTimeout(r, 700))]);
    return true;
  }

  async function open(id, opts = {}) {
    build();
    const E = W.el, fresh = !W.dlg.open, tok = ++W.token;
    const origin = fresh ? originOf(id, opts) : null;
    pressed = null;
    // (a fresh opening knows nothing of the last one's sheet: its stock, what it drew, what rang)
    if (fresh) { resetView(); W.st = null; W.rang = null; W.dimRamp = true; W.pre = preOf(origin, opts); }
    // (another sheet in the same window: the picture the window grew from is not this one's, and the frame to go back
    // into is this sheet's card where one is on screen)
    else { dropSnap(false); W.pre = null; if (W.origin && W.origin.id !== id) { const c = [...document.querySelectorAll(`.libCard[data-id="${CSS.escape(id)}"]`)].find(shown); W.origin = c ? cardOrigin(id, c, null) : null; } }
    W.id = id; W.rec = null; W.live = null; W.geom = false; W.pieces = []; W.byId = new Map(); W.byPool = new Map(); W.orders = new Map();
    W.sel = null; W.hover = null; W.fx = []; W.set = W.set && opts.keepSet ? W.set : null;
    if (!fresh && W.view === "piece") showPane("sheet", "back");
    if (W.flow) W.work = W.flow; else if (!opts.keepWork) W.work = null;
    W.freed = (FREED.get(id) || []).filter(g => Date.now() - (g.at || 0) < 12 * 3600e3).map(g => Object.assign(g, { t0: 0 }));
    W.fill = null; W.ghost = null; W.fillRun++; stopHand(true); W.add = null; E.addBtn.hidden = true;
    renderWork(); renderFill();
    const lib = (S.library.rows || []).find(r => r.id === id) || null;
    // (a sheet the page holds live, not in the Library list read here: its card says what it is until its record comes)
    const live0 = lib ? null : allSheets().find(p => p.sheetId === id) || null;
    head(lib || (live0 ? { id, metal: live0.metal, sheetIndex: live0.sheetIndex || live0.page, setSeq: live0.seq || null, draft: !!live0.draft, cardStartedAt: live0.cardStartedAt || null } : { id, metal: "gold" }), true);
    // (read again after pieces came off it: the plate on screen stays until the saved sheet is drawn over it)
    const again = opts.keepWork && !fresh && W.dlg.open;
    const grow = fresh && !!origin && !still();
    if (!again) {
      // the picture shown until the sheet is drawn: the very picture of the card it was opened from, else the saved one
      const im = origin && origin.img ? tryDo(origin.img) : null, src0 = loaded(im) ? im.currentSrc || im.src : "", url = lib?.outputs?.preview?.url;
      E.pv.removeAttribute("src"); E.pv.style.transitionDelay = ""; W.pvCard = !!src0;
      if (src0) E.pv.src = src0; else if (url) E.pv.src = cors(url);
      if (fresh) {
        // (what the last opening drew is not shown again, even for a moment)
        setNow(E.pv, "1"); setNow(E.base, W.pre && W.pre.pieces ? "1" : "0");
        E.base.getContext("2d").clearRect(0, 0, E.base.width, E.base.height); E.fx.getContext("2d").clearRect(0, 0, E.fx.width, E.fx.height);
      } else { E.pv.style.opacity = "1"; E.base.style.opacity = "0"; }
      const st0 = lib?.stock ? stockOf(lib) : origin && origin.stock && origin.stock.wPt ? origin.stock : live0 ? stockFor(live0.metal, live0) : null;
      if (st0) { W.st = { wPt: +st0.wPt, hPt: +st0.hPt }; fitPlate(); }
      // (a window growing out of the sheet shows it already; its panel holds the shape of what it is reading)
      if (!grow) veil("Opening the sheet…");
    }
    // (read again after a change here: the list stays where it is, still, until the saved sheet is drawn over it, and
    // then changes in place; it used to empty to "Reading the sheet…" and fill again, Paul 27 Sep)
    const still0 = [E.strip, E.foot, E.seg, E.orders];
    for (const n of still0) n.inert = again;
    if (!again) { W.listKey = null; E.strip.innerHTML = ""; E.orders.innerHTML = `<li class="swNone"><div class="swWait" style="justify-content:center"><span class="owSpin"></span>Reading the sheet…</div></li>`; E.foot.innerHTML = ""; E.seg.innerHTML = ""; }
    const unstill = () => { for (const n of still0) n.inert = false; };
    if (fresh) {
      W.dlg.classList.remove("closing", "swBack"); W.origin = origin;
      if (grow) { W.dlg.classList.add("swGrow", "swFlying"); W.flying = true; }
      try { W.dlg.showModal(); } catch (_) { W.dlg.setAttribute("open", ""); }
      // (nothing on screen to grow from: it opens as it did)
      if (grow && !flipOpen(origin, !!(opts.select || (W.pre && W.pre.at)))) { stopFlip(); W.flying = false; W.fitLater = false; W.dlg.classList.remove("swGrow", "swFlying"); fitPlate(); veil(W.rec ? null : "Opening the sheet…"); }
    } else if (opts.slide) slidePlate(opts.slide);
    const ready = () => { if (W.flip && W.flip.readyNow) W.flip.readyNow(); };
    try {
      const r = await api("charmNestLibrary", { op: "getSheet", id }, { quiet: true });
      if (tok !== W.token) return;
      const rec = r.sheet; if (!rec) throw new Error("This sheet is no longer in the Library.");
      if (window.LaserReview) LaserReview.record(rec);
      W.rec = rec; W.st = stockOf(rec); W.live = liveOf(id);
      head(rec, false); indexPieces(); pruneFreed(); fitPlate(); renderStrip(); renderSheetPane(); renderFoot(); renderMenu(); unstill();
      E.addBtn.hidden = !canAdd();
      if (!rec.outputs?.preview?.url && !W.live && !W.pvCard) E.pv.removeAttribute("src"); else if (rec.outputs?.preview?.url && !E.pv.getAttribute("src")) E.pv.src = cors(rec.outputs.preview.url);
      // the charm clicked is the one the window opens on, as soon as the sheet's pieces are known: the panel comes in
      // showing it (its outline is known now; its drawing, below, makes the choice exact where it was a point clicked)
      const at = opts.at || (origin && origin.at) || null;
      const aim = fresh ? (opts.select ? W.byPool.get(opts.select) || W.byId.get(opts.select) : at ? hitPt(at.xPt, at.yPt) : null) || null : null;
      if (aim) { W.sel = aim; W.hover = null; lightChips(aim.rid); renderPiece(aim); showPane("piece", "forward"); }
      paintFx(); ready();
      loadSet(tok);
      await loadGeometry(tok);
      if (tok !== W.token) return;
      veil(null);
      // (unless another charm, or the list, was chosen meanwhile)
      const p = opts.select ? W.byPool.get(opts.select) || W.byId.get(opts.select) : aim && at ? hitPt(at.xPt, at.yPt) || aim : null;
      if (p && (!aim || W.sel === aim)) selectPiece(p, { from: opts.from || "open", flash: opts.flash, land: true });
      // the pieces that just arrived ring once where they landed
      // (and what was drawn where it landed, while the sheet saved, is now drawn for real: its pieces are on the sheet,
      // whichever opening of it shows them first)
      if (W.landing.length) { W.landing = W.landing.filter(l => !(opts.glow && l.spots.some(s => s.c && opts.glow.includes(s.c.poolId))) && !(l.sheet === id && l.spots.every(s => s.c && W.byPool.has(s.c.poolId)))); paintFx(); }
      if (opts.glow && !still()) { const t0 = performance.now(); for (const x of W.pieces) if (!x.gone && opts.glow.includes(x.poolId)) W.fx.push({ kind: "pulse", piece: x, t0, ms: 1400 }); fxLoop(); }
      suggest();
    } catch (e) {
      if (tok !== W.token) return;
      unstill(); W.listKey = null; if (opts.glow) W.landing = W.landing.filter(l => !l.spots.some(s => s.c && opts.glow.includes(s.c.poolId)));
      veil(null); E.orders.innerHTML = `<li class="swNone"><b>This sheet could not open.</b><br>${esc(e.message)}<br><br><button class="btn ghost sm" data-r2="retry">Try again</button></li>`;
      E.orders.querySelector("[data-r2=retry]").onclick = () => open(id, opts);
      ready();
    }
  }
  function resetView() {
    W.view = "sheet"; W.q = ""; W.filter = "all"; W.el.find.value = "";
    W.el.side.querySelector('[data-pane="sheet"]').hidden = false; W.el.side.querySelector('[data-pane="piece"]').hidden = true;
  }
  async function close(opts = {}) {
    const d = W.dlg; if (!d || !d.open || W.leaving || d.classList.contains("closing") || W.folding) return;
    W.token++; menu(false);
    // a sheet deleted for good has no card to go back into: it fades and folds away where it lay, slowly enough to be
    // seen going, and then the window closes (Paul, 27 Sep)
    if (opts.gone && !still()) {
      stopFlip(); W.folding = true;
      animate(W.el.strip, [{ opacity: 1 }, { opacity: 0 }], 520, { easing: "ease-in" });
      try { await animate(W.el.plate, [{ opacity: 1, transform: "none", filter: "none" }, { opacity: .96, transform: "scale(.992)", offset: .22 }, { opacity: 0, transform: "translateY(16px) scale(.9)", filter: "grayscale(1) blur(2px)" }], 860, { easing: "cubic-bezier(.45,0,.7,.35)" }); }
      finally { W.folding = false; }
    }
    W.leaving = true;
    try {
      // back into the frame it grew out of while that is on screen (and still shows this sheet); else it fades where it is
      const home = !opts.gone && !still() && W.origin ? tryDo(() => W.origin.rects()) : null;
      if (!(home && home.box && onScreen(home.box) && await flipClose(home))) {
        stopFlip();
        if (!d.classList.contains("closing")) { d.classList.add("closing"); await new Promise(r => setTimeout(r, still() ? 120 : 160)); }
      }
    } finally { W.leaving = false; }
    try { d.close(); } catch (_) { d.removeAttribute("open"); }
  }
  function cleanup() {
    stopFlip(); dropSnap(false); W.flying = false; W.fitLater = false; W.origin = null; W.pre = null;
    W.dlg.classList.remove("closing", "swGrow", "swFlying", "swBack"); W.token++; stopHand(true); W.add = null;
    for (const n of [W.el.plate, W.el.strip, W.el.headBar, W.el.rule, W.el.fx, W.el.skel]) n.getAnimations?.().forEach(a => a.cancel());
    W.el.side.getAnimations?.({ subtree: true }).forEach(a => a.cancel());
    W.landing = []; W.inbound.clear(); W.stay = 0; for (const n of [W.el.strip, W.el.foot, W.el.seg, W.el.orders]) n.inert = false;
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
      // a sheet still filling is not cut yet: it can be marked completed once it is released (or taken back when it was)
      E.done.hidden = !window.LibraryDone || !window.LibraryDone.mark || (!done && working && !ready);
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
      // where the sheet went, said slowly where the eye already is (Paul, 27 Sep): the state pops, and a note under it
      // names the Library tab that holds the sheet now, with Undo
      animate(W.el.state, [{ transform: "scale(.8)", opacity: .4 }, { transform: "scale(1.1)", opacity: 1, offset: .45 }, { transform: "none", opacity: 1 }], 700, { fill: "none" });
      swNote(W.el.state, { text: done ? "Moved to Library › Completed" : "Back in Library › Current", actions: [{ label: "Undo", title: done ? "Move it back to Current" : "Mark it completed again", fn: () => { if (W.id === id && W.dlg.open && W.rec) markDone(!done); } }] });
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
    E.sheets.innerHTML = list.map(s => `<button type="button" class="swChip" data-sheet="${esc(s.id)}" style="--c:${colorOf(s.metal)}"${s.id === W.id ? ' aria-current="true"' : ""} title="${esc(s.name || "")}${FREED.get(s.id)?.length ? " · has freed room" : ""}"${FREED.get(s.id)?.length ? " data-freed" : ""}><i></i>${esc(CODE[s.metal] || "")} ${s.n}<b></b></button>`).join("");
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
  // room that something has gone into since (an arrival, a move, a restore) is no longer shown as free
  function pruneFreed() {
    if (W.flow || !W.freed.length) return;
    const live = W.pieces.filter(x => !x.gone), target = fillTarget();
    let G = null; if (target) try { G = plateGrid(target); } catch (_) {}
    W.freed = W.freed.filter(g => freeStill(g, G, target, gh => live.some(x => Math.hypot(x.p.cxPt - gh.x.p.cxPt, x.p.cyPt - gh.x.p.cyPt) < Math.min(gh.x.p.wPt || 20, gh.x.p.hPt || 20) / 2)));
    setFreed(W.id, W.freed);
  }

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
    W.geom = true; W.pre = null; paintBase();
    W.el.base.style.opacity = "1";
    // (the picture shown until now goes once the drawing has come in over it: never both half there)
    if (W.pieces.every(x => x.c)) { W.el.pv.style.transitionDelay = ".3s"; W.el.pv.style.opacity = "0"; }
    // (the picture the window grew out of gives way to the sheet drawn here, the two fading into each other)
    dropSnap(true);
    paintFx();
  }

  /* ── the plate: base (the charms as the Nest draws them) and fx (hover, focus, links, marks) ── */
  function fitPlate(force) {
    const E = W.el, st = W.st; if (!st) return;
    // (not while the sheet flies to it: it takes its new size once it has landed)
    if (W.flying && !force) { W.fitLater = true; return; }
    const box = E.plateBox.getBoundingClientRect(); if (box.width < 40 || box.height < 40) return;
    const availW = box.width - 32, availH = box.height - 22;
    const R = Math.round(Math.max(14, Math.min(22, availW * 0.022)));
    // true scale (Paul, 28 Sep): the 100 × 50 mm frame fits the box, and the sheet is drawn at its own size in that scale
    const f = window.trueFrame && trueFrame(st), fw = f ? f.fw : st.wPt, fh = f ? f.fh : st.hPt;
    let s = (availW - R) / fw; if (R + fh * s > availH) s = (availH - R) / fh;
    let w = R + st.wPt * s, hh = R + st.hPt * s;
    w = Math.floor(w); hh = Math.floor(hh);
    E.plate.style.width = w + "px"; E.plate.style.height = hh + "px";
    const dpr = Math.min(2.5, devicePixelRatio || 1);
    W.dpr = dpr; W.R = R * dpr; W.k = (w * dpr - W.R) / st.wPt; W.cssW = w; W.cssH = hh;
    for (const cv of [E.rule, E.base, E.fx]) { const cw = Math.round(w * dpr), ch = Math.round(hh * dpr); if (cv.width !== cw || cv.height !== ch) { cv.width = cw; cv.height = ch; } }
    // the saved preview is the plate without rulers: it sits where the plate sits
    // (inset first: set after them, it would put left and top back to auto, and the picture at the plate's corner)
    E.pv.style.inset = "auto"; E.pv.style.left = R + "px"; E.pv.style.top = R + "px"; E.pv.style.width = (w - R) + "px"; E.pv.style.height = (hh - R) + "px";
    placeSnap();
    paintRule();
    if (W.geom || (W.pre && W.pre.pieces)) paintBase(); paintFx();
  }
  /** The plate under whatever it shows: its rulers and the empty sheet, there from the first frame (the rulers are
      their own layer, so that a closing window can let them go before the sheet). */
  function paintRule() {
    const cv = W.el.rule, ctx = cv.getContext("2d"), R = W.R; if (!W.st || !cv.width) return;
    ctx.setTransform(1, 0, 0, 1, 0, 0); ctx.fillStyle = "#fffefb"; ctx.fillRect(0, 0, cv.width, cv.height);
    if (R) drawRulers(ctx, R, W.k, cv.width - R, cv.height - R);
    sheetEdge(ctx, cv);
  }
  // the sheet's edge and its inset line
  function sheetEdge(ctx, cv) {
    const R = W.R, k = W.k, Wp = cv.width - R, Hp = cv.height - R;
    ctx.save(); ctx.translate(R, R);
    ctx.strokeStyle = "rgba(176,86,63,.9)"; ctx.lineWidth = Math.max(1, .5 * k); ctx.strokeRect(.5, .5, Wp - 1, Hp - 1);
    const ins = (+S.settings.insetPt || 0) * k; ctx.setLineDash([4 * W.dpr, 4 * W.dpr]); ctx.strokeStyle = "rgba(147,140,128,.35)"; ctx.lineWidth = 1; ctx.strokeRect(ins, ins, Wp - 2 * ins, Hp - 2 * ins); ctx.setLineDash([]);
    ctx.restore();
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
    // the sheet alone (the rulers are the plate's own layer, under it); before the record is read, a Nest card's sheet
    // as the page holds it
    ctx.setTransform(1, 0, 0, 1, 0, 0); ctx.clearRect(0, 0, cv.width, cv.height);
    ctx.fillStyle = "#fffefb"; ctx.fillRect(R, R, Wp, Hp);
    sheetEdge(ctx, cv);
    for (const x of W.geom || !(W.pre && W.pre.pieces) ? W.pieces : W.pre.pieces) {
      if (x.gone) continue;
      withPiece(ctx, x, () => {
        // a custom order's own design is plum here too, as on the sheet's card (CustomSheet)
        const cu = x.c && x.c.custom;
        ctx.fillStyle = cu ? "rgba(125,86,168,.20)" : "rgba(200,162,78,.10)"; outlinePath(ctx, x, true); ctx.fill("evenodd");
        if (x.c) CharmNestPDF.drawCharm(ctx, x.c, tx0(x.c), k);
        else { ctx.strokeStyle = "rgba(60,54,46,.5)"; ctx.lineWidth = W.dpr; outlinePath(ctx, x); ctx.stroke(); }
        if (cu) { outlinePath(ctx, x); ctx.strokeStyle = "rgba(125,86,168,.9)"; ctx.lineWidth = 1.6 * W.dpr; ctx.setLineDash([4 * W.dpr, 3 * W.dpr]); ctx.stroke(); ctx.setLineDash([]); }
      });
    }
  }
  function focusSet() {
    // what the pointer or the selection is about: one charm, and the rest of its order on this sheet
    const f = W.hover || W.sel; if (!f) return null;
    const mates = f.rid ? (W.orders.get(f.rid) || []) : [f];
    return { f, mates };
  }
  // (one mark that cannot be drawn never stops the others, the loop that animates them, or what asked for the drawing)
  function paintFx(now) { try { paintMarks(now); } catch (e) { console.warn("sheet window: plate marks", e); } }
  function paintMarks(now) {
    const cv = W.el.fx, ctx = cv.getContext("2d"), k = W.k, R = W.R; if (!W.st) return;
    ctx.setTransform(1, 0, 0, 1, 0, 0); ctx.clearRect(0, 0, cv.width, cv.height);
    const t1 = now || performance.now();
    const q = W.q, matches = q ? W.pieces.filter(x => matchesQ(x, q)) : null;
    const fs = focusSet();
    let u = 1;
    if (W.geom && (fs || (matches && matches.length < W.pieces.length))) {
      // everything else steps back, the charms in hand come forward in their own lines (the first time after an opening
      // it comes gently, the sheet having just arrived)
      if (W.dimRamp) { W.dimRamp = false; if (!still()) { W.fx.push({ kind: "dim", t0: t1, ms: 280 }); fxLoop(); } }
      const d = W.fx.find(f => f.kind === "dim"); u = d ? Math.min(1, Math.max(0, (t1 - d.t0) / d.ms)) : 1; u = 1 - (1 - u) * (1 - u);
      const keep = new Set((fs ? fs.mates : []).concat(matches || []));
      ctx.save(); ctx.globalAlpha = u;
      ctx.fillStyle = "rgba(255,254,251,.62)"; ctx.fillRect(R, R, cv.width - R, cv.height - R);
      // (the rest of the order lighter than the charm itself, which is marked last, above them)
      for (const x of keep) { if (x.gone || (fs && x === fs.f)) continue; const mate = !!fs && fs.mates.includes(x); withPiece(ctx, x, () => {
        ctx.fillStyle = mate ? "rgba(202,168,97,.14)" : "rgba(74,107,120,.14)"; outlinePath(ctx, x, true); ctx.fill("evenodd");
        if (x.c) CharmNestPDF.drawCharm(ctx, x.c, tx0(x.c), k);
        outlinePath(ctx, x); ctx.strokeStyle = mate ? "rgba(169,130,63,.6)" : "rgba(74,107,120,.7)"; ctx.lineWidth = (mate ? 1.2 : 1.4) * W.dpr; ctx.stroke(); }); }
      ctx.restore();
      if (fs && !fs.f.gone) markMain(ctx, fs.f, t1);
    } else if (!W.geom && W.pre && W.pre.sel) markMain(ctx, W.pre.sel, t1);
    // the point clicked on a Library picture, until the charm there is drawn
    if (!W.geom && W.pre && W.pre.at && (!W.rec || W.sel)) markAt(ctx, W.pre.at);
    // the pieces of one order on this sheet, joined (in an opening, once the sheet has come to rest)
    if (fs && fs.mates.length > 1 && W.geom && !(W.flip && !W.flip.cued)) {
      const a = fs.f, t = linkProgress(now);
      ctx.save(); ctx.globalAlpha = u; ctx.strokeStyle = "rgba(169,130,63,.85)"; ctx.lineWidth = 1.4 * W.dpr; ctx.setLineDash([5 * W.dpr, 4 * W.dpr]);
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
      const x = g.x, t = g.t0 ? Math.min(1, (t1 - g.t0) / 1000) : 1; if (!x || !x.p) continue;
      withPiece(ctx, x, () => {
        if (t < 1) { ctx.save(); const sc = 1 - .1 * t; ctx.scale(sc, sc); ctx.globalAlpha = 1 - t; ctx.fillStyle = "rgba(176,86,63,.5)"; outlinePath(ctx, x, true); ctx.fill("evenodd"); ctx.restore(); }
        ctx.fillStyle = `rgba(176,86,63,${.06 * t})`; outlinePath(ctx, x, true); ctx.fill("evenodd");
        ctx.setLineDash([4 * W.dpr, 3 * W.dpr]); outlinePath(ctx, x); ctx.strokeStyle = `rgba(176,86,63,${.3 + .5 * t})`; ctx.lineWidth = 1.2 * W.dpr; ctx.stroke(); ctx.setLineDash([]);
      });
    }
    for (const paint of [() => paintLanding(ctx, now), () => paintGhost(ctx, now), () => paintHand(ctx)]) { try { ctx.save(); paint(); } catch (e) { console.warn("sheet window: plate marks", e); } finally { ctx.restore(); } }
    // back engraving: a small mark on each charm that has one (clay: still to approve, sage: approved)
    if (W.showBacks) for (const x of W.pieces) {
      if (x.gone || !x.eng || x.eng.kind === "none" || x.eng.kind === "skipped") continue;
      const cx = R + x.p.cxPt * k, cy = R + x.p.cyPt * k, r = Math.max(3.5, Math.min(6, 1.6 * k / W.dpr)) * W.dpr;
      ctx.beginPath(); ctx.arc(cx, cy, r + 2 * W.dpr, 0, Math.PI * 2); ctx.fillStyle = "#fff"; ctx.fill();
      ctx.beginPath(); ctx.arc(cx, cy, r, 0, Math.PI * 2); ctx.fillStyle = x.eng.kind === "approved" ? "#5f7a5b" : "#b0563f"; ctx.fill();
    }
    // the selected charm's pulse
    for (const f of W.fx) if (f.kind === "pulse") {
      const t = Math.min(1, (t1 - f.t0) / f.ms), x = f.piece; if (!x || !x.p || x.gone) continue;
      withPiece(ctx, x, () => { const s = 1 + .22 * t; ctx.scale(s, s); outlinePath(ctx, x); ctx.strokeStyle = `rgba(202,168,97,${.9 * (1 - t)})`; ctx.lineWidth = 3 * W.dpr / s; ctx.stroke(); });
    }
    for (const f of W.fx) if (f.kind === "arrive") paintArrive(ctx, f, t1);
  }
  /** The charm in hand: warm, in a firm gold line, above the rest of its order; it pops once as the sheet it opened on
      comes to rest (its copy in the sheet under it covered while it is bigger). */
  function markMain(ctx, x, t1) {
    const f = W.fx.find(e => e.kind === "arrive" && e.piece && keyOf(e.piece) === keyOf(x)), u = f ? (t1 - f.t0) / 350 : 1;
    const s = u > 0 && u < 1 ? 1 + .08 * Math.sin(Math.PI * u) : 1;
    withPiece(ctx, x, () => {
      if (s !== 1) { ctx.scale(s, s); ctx.fillStyle = "#fffefb"; outlinePath(ctx, x); ctx.fill(); }
      ctx.fillStyle = "rgba(202,168,97,.42)"; outlinePath(ctx, x, true); ctx.fill("evenodd");
      if (x.c) CharmNestPDF.drawCharm(ctx, x.c, tx0(x.c), W.k);
      outlinePath(ctx, x); ctx.strokeStyle = "#b8893a"; ctx.lineWidth = 2.5 * W.dpr / s; ctx.stroke();
    });
  }
  // a point clicked on a picture, where its charm is not known yet: a gold ring about a charm's size, and its centre
  function markAt(ctx, at) {
    const x = W.R + at.xPt * W.k, y = W.R + at.yPt * W.k, r = 4.6 * 72 / 25.4 * W.k;
    ctx.save(); ctx.beginPath(); ctx.arc(x, y, r, 0, Math.PI * 2); ctx.fillStyle = "rgba(202,168,97,.30)"; ctx.fill(); ctx.strokeStyle = "#b8893a"; ctx.lineWidth = 2.5 * W.dpr; ctx.stroke();
    ctx.beginPath(); ctx.arc(x, y, Math.max(2.5 * W.dpr, r * .12), 0, Math.PI * 2); ctx.fillStyle = "#b8893a"; ctx.fill(); ctx.restore();
  }
  function linkProgress(now) { const f = W.fx.find(e => e.kind === "link"); if (!f) return 1; return Math.min(1, ((now || performance.now()) - f.t0) / f.ms); }
  function fxLoop() {
    if (W.raf) return;
    const tick = now => {
      W.raf = 0;
      W.fx = W.fx.filter(f => now - f.t0 < f.ms);
      paintFx(now);
      if (W.fx.length) W.raf = requestAnimationFrame(tick); else paintFx();
    };
    W.raf = requestAnimationFrame(tick);
  }
  function pulse(x) { if (still()) return paintFx(); const t0 = performance.now(); W.fx = W.fx.filter(f => f.kind !== "pulse" && f.kind !== "link"); W.fx.push({ kind: "pulse", piece: x, t0, ms: 900 }, { kind: "link", t0, ms: 420 }); fxLoop(); }
  // the charm the window opened on, as the sheet comes to rest: it pops, two rings go out from it, and the rest of its
  // order is joined to it
  function land(x) { if (still()) return paintFx(); const t0 = performance.now(); W.fx = W.fx.filter(f => !["pulse", "link", "arrive"].includes(f.kind)); W.fx.push({ kind: "arrive", piece: x, t0, ms: 900 }, { kind: "link", t0: t0 + 160, ms: 520 }); fxLoop(); }
  function paintArrive(ctx, f, t1) {
    const x = f.piece, t = (t1 - f.t0) / f.ms; if (!x || !x.p || x.gone || t < 0 || t >= 1) return;
    const rad = Math.max(8 * W.dpr, Math.max(x.p.wPt, x.p.hPt) * W.k / 2), out = u => 1 - Math.pow(1 - u, 3);
    withPiece(ctx, x, () => {
      for (const [d, px, a0] of [[0, 22, .95], [.16, 38, .7]]) {
        const u = (t - d) / (1 - d); if (u <= 0 || u >= 1) continue;
        const s = 1 + px * W.dpr * out(u) / rad;
        ctx.save(); ctx.scale(s, s); outlinePath(ctx, x); ctx.strokeStyle = `rgba(184,137,58,${a0 * Math.pow(1 - u, 1.3)})`; ctx.lineWidth = 3.5 * W.dpr / s; ctx.stroke(); ctx.restore();
      }
    });
  }

  /* ── pointer ── */
  function toPlate(e) {
    const r = W.el.fx.getBoundingClientRect(), sx = W.el.fx.width / r.width;
    return { x: ((e.clientX - r.left) * sx - W.R) / W.k, y: ((e.clientY - r.top) * sx - W.R) / W.k, cx: e.clientX - r.left, cy: e.clientY - r.top };
  }
  function hitAt(e) { const { x, y } = toPlate(e); return hitPt(x, y); }
  /** The charm at a point of the sheet (in points), by its drawing where the window has it, else by its outline box. */
  function hitPt(x, y) {
    if (!W.st || !(x >= 0 && y >= 0 && x <= W.st.wPt && y <= W.st.hPt)) return null;
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
    if (W.hand) return handMove(e);
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
      // Engrave keeps a job for every line, "none" for one with nothing to engrave: that is no back engraving (it read as
      // "being prepared", so a whole sheet showed a back on every charm with a spinner that never ended)
      if (s === "none") return { kind: "none", label: "none" };
      if (["approved", "written"].includes(s)) return { kind: "approved", label: "approved", job, back, by: job.approvedBy, at: job.approvedAt, text: job.text };
      if (s === "skipped") return { kind: "skipped", label: "cut plain", job, by: job.approvedBy };
      if (s === "review") return { kind: "approve", label: "to approve", job, text: job.text };
      if (s === "words" || s === "blocked") return { kind: "words", label: "words to confirm", job, text: job.text, reason: job.reason };
      if (["classify", "ready", "fitting"].includes(s)) {
        // a spinner only while Engrave is actually reading or fitting it; otherwise what it waits for
        const working = !!(Engrave.isWorking && Engrave.isWorking(job));
        const note = working ? (s === "classify" ? "Reading the words…" : "Fitting the words on the back…")
          : s === "classify" ? "Its words are not read yet: confirm them in Engrave"
          : Engrave.canFit && !Engrave.canFit(job) ? "Waits for its sheet to be written, then it is fitted" : "Waits its turn in Engrave";
        return { kind: "preparing", label: "being prepared", job, text: job.text, working, note };
      }
      return { kind: "none", label: "none" };
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
    // (an order being put on hold by the change in progress is counted there already: its row flies to it)
    const heldIds = new Set(heldOrders().map(o => o.rid)); for (const [rid, g] of W.going) if (g.how === "hold") heldIds.add(rid);
    const held = heldIds.size, here = all.filter(([, xs]) => xs.some(x => !x.gone)).length;
    // a view shows only when it has something in it (or is the one open): an empty choice is clutter
    const seg = [["all", "All", here, ""], ["backs", "Backs", engN, "Orders whose back engraving still needs approving"], ["multi", "Other sheets", multi, "Orders with pieces on other sheets too"], ["hold", "On hold", held, "Orders a person put on hold or took off a sheet, from every sheet"]]
      .filter(([id, , n]) => id === "all" || n || W.filter === id);
    // (not while a released order is still flying to All: the list turns to All once it lands)
    if (W.filter !== "all" && !seg.some(([id, , n]) => id === W.filter && n) && !W.q && !(W.stay > Date.now())) { W.filter = "all"; return renderSheetPane(); }
    // a view that appears (On hold, once something is held) opens where it stands, and the list below moves with it
    const redraw = !!W.listKey && W.listKey.startsWith(W.id + "|") && E.seg.childElementCount > 0;
    const had = new Set(E.seg.hidden ? [] : [...E.seg.querySelectorAll("[data-f]")].map(b => b.dataset.f));
    steady(E.seg, () => {
      E.seg.hidden = seg.length < 2;
      E.seg.innerHTML = seg.map(([id, label, n, t]) => `<button type="button" data-f="${id}" aria-pressed="${W.filter === id}"${t ? ` title="${t}"` : ""}>${label}<i>${n}</i></button>`).join("");
    });
    E.seg.querySelectorAll("[data-f]").forEach(b => b.onclick = () => { W.filter = b.dataset.f; renderSheetPane(); });
    if (redraw && !E.seg.hidden) for (const b of E.seg.querySelectorAll("[data-f]")) if (!had.has(b.dataset.f)) animate(b, [{ opacity: 0, transform: "scale(.86)" }, { opacity: 1, transform: "none" }], 520, { fill: "none" });
    renderOrders();
  }
  function renderOrders() {
    const E = W.el, q = W.q;
    if (W.filter === "hold") return renderHeld();
    // (a row whose order is leaving stays, lit, until the list it is in has slid back into view: then it is seen going)
    let list = [...W.orders.entries()].filter(([rid, xs]) => xs.some(x => !x.gone) || (W.going.get(rid) || {}).keep);
    if (W.filter === "backs") list = list.filter(([, xs]) => xs.some(x => x.eng && ["approve", "words", "preparing"].includes(x.eng.kind)));
    if (W.filter === "multi") list = list.filter(([rid]) => rid !== "—" && otherSheets(rid).length);
    if (q) list = list.filter(([, xs]) => xs.some(x => matchesQ(x, q)));
    list.sort((a, b) => (a[0] === "—") - (b[0] === "—") || a[0].localeCompare(b[0]));
    // an order that is not here but could come here: offered straight from the search
    const t = !list.length && q && W.filter === "all" && fillTarget();
    const elsewhere = t ? candidatesFor(t).filter(k => k.rid.includes(q) || k.pieces.some(z => String(z.c.sku || "").toLowerCase().includes(q))).slice(0, 3) : [];
    if (elsewhere.length) {
      putRows(`<li class="swNone">No order on this sheet matches.<div class="swElse">${elsewhere.map(k => { const src = [...k.srcs][0]; return `<button type="button" class="btn ghost xs" data-add="${esc(k.rid)}" title="Add it to this sheet">${ICON.add}${esc(k.rid)}<small>${k.waiting ? "waiting on" : "on"} ${esc(sheetWord(src.sheetId, src.fileBase, src))}</small></button>`; }).join("")}</div></li>`);
      E.orders.querySelectorAll("[data-add]").forEach(b => b.onclick = () => openAdd(b.dataset.add));
      return;
    }
    if (!list.length) { putRows(`<li class="swNone">${q ? "No order on this sheet matches." : W.filter === "backs" ? "No back engraving is waiting on this sheet." : W.filter === "multi" ? "Every order here is only on this sheet." : "This sheet has no charms."}</li>`); return; }
    putRows(list.map(([rid, xs]) => {
      const go = W.going.get(rid), kept = !!(go && go.keep) && !xs.some(x => !x.gone);
      const live = kept ? xs : xs.filter(x => !x.gone), skus = new Map(); for (const x of live) skus.set(x.sku, (skus.get(x.sku) || 0) + 1);
      const what = [...skus].map(([s, n]) => `${s}${n > 1 ? ` ×${n}` : ""}`).join(", ");
      const need = live.filter(x => x.eng && ["approve", "words", "preparing"].includes(x.eng.kind)).length, ok = live.filter(x => x.eng?.kind === "approved").length;
      const other = rid === "—" ? [] : otherSheets(rid);
      const tags = (live.some(x => x.c && x.c.custom) ? `<span class="swTag cust" title="A custom order: its own designs, tinted plum on the sheet">Custom</span>` : "") +
        (need ? `<span class="swTag eng" title="Back engraving still to approve">${need > 1 ? need + " backs" : "back"}</span>` : ok ? `<span class="swTag engOk" title="Back engraving approved">back</span>` : "") +
        other.map(s => `<span class="swTag" style="--c:${colorOf(s.metal)}" title="Also on ${esc(s.name || "")}"><i></i>${esc(CODE[s.metal] || "")} ${s.n}</span>`).join("");
      return `<li class="swOrd${go && go.keep ? " going" + (go.how === "cancel" ? " cancel" : "") : ""}" data-rid="${esc(rid)}" data-mkey="o:${esc(rid)}"><span class="no">${rid === "—" ? "No order" : esc(rid)}</span><span class="what" title="${esc(what)}">${esc(what)}</span><span class="tags">${tags}</span></li>`;
    }).join(""));
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
      : (plan => `<div class="swQr" data-r2="qr"><span class="qrWaiting" style="width:72px;height:72px">No QR yet</span><span class="busy"><span class="owSpin"></span></span><span class="cap"><b>QR label</b><span data-r2="qrText">${esc(plan ? plan.note : rec.metal === "rose" && !(rec.label?.files || []).length ? "Made when you press Cut Sheet" : "Made when the sheet joins a set")}</span></span>${plan ? `<button type="button" class="btn sage xs" data-r2="label" title="${esc(plan.title)}">Make QR label</button>` : ""}</div>`)(labelPlan());
    const files = [["ai", ".ai", "Download the sheet with its back engravings"], ["dxf", ".dxf", "DXF · millimetres"]].map(([fmt2, label, title]) => `<button class="btn ghost xs" data-export-one="${esc(rec.id)}" data-format="${fmt2}" title="${title}">${label}</button>`).join("") +
      (rec.outputs?.labelled?.url ? `<a class="btn ghost xs" href="${esc(rec.outputs.labelled.url)}" target="_blank" rel="noopener" title="Every charm numbered, as a PDF">Proof</a>` : "") +
      (rec.outputs?.report?.url ? `<a class="btn ghost xs" href="${esc(rec.outputs.report.url)}" target="_blank" rel="noopener" title="The nest report (JSON)">Report</a>` : "");
    E.foot.innerHTML = `<div class="row">${qr}</div><div class="row"><span class="fLabel">Files</span><span class="swFiles">${files}</span></div>`;
    const rb = E.foot.querySelector("[data-r2=label]"); if (rb) rb.onclick = () => makeLabel(rb);
  }
  /* Make QR label (Paul, 27 Sep: "The user should also have the ability to manually generate a QR code for the sheet,
     even though the system may have opted not to"). What it does depends on where the sheet stands:
     · a Gold or Silver sheet of the open run, still filling or filling its gaps, is released as it stands and joins its
       set with the set's label (it used to be offered only after charms were taken off it here, as "Release now");
     · a 10K or 14K sheet of the open run: its metal is included in the set, as the Include switch does;
     · a sheet in its set whose label is missing (an upload that failed) gets it made again;
     · a sheet no open run can take into a set (an earlier or given-up run, a record restored from the Library) gets a
       label of its own: the same code of the orders placed on it, named by the sheet, saved beside its files.
     Rose Gold gets its label with Cut Sheet, and a sheet being changed or saved waits until it is done. */
  function labelPlan() {
    const rec = W.rec; if (!rec || W.flow) return null;
    const sh = allSheets().find(p => p.sheetId === W.id), run = window.B && B.run;
    if (sh && busy(sh)) return null;
    const openRun = !!sh && !!run && sh.runId === run.runId && !!window.Gate && Gate.modern(run.runId) && !["complete", "abandoned"].includes(run.status) && !sh.recalled && !sentToStation(sh);
    if (openRun) {
      if (!sh.placements.length || !sh.verification?.ok || !sh.persistedDone || sh.roseCutAt || sh.laserDoneAt || sh.metal === "rose") return null;
      if (sh.setId && !sh.draft) return { kind: "relabel", note: "Its set has no label for it yet", title: "Make this sheet's QR label for the orders on it again" };
      if (["gold", "silver"].includes(sh.metal)) {
        const gap = sh.topup && !sh.topup.closedAt;
        return { kind: "release", note: gap ? `Filling its gaps · ${(sh.topup.tried || []).length} of 35 later orders tried` : "Made when the sheet is full and joins a set",
          title: `Release this sheet as it stands (${fmt.pct(sh.density || 0)} full): it joins its set now and gets its QR label, without waiting to fill${gap ? " its gaps" : ""}` };
      }
      if (["gold10k", "gold14k"].includes(sh.metal)) return { kind: "include", note: "Made when this sheet is included in the set", title: "Include this sheet in the set, as its own Include switch does (the other sheets stay as they are), and make its label" };
      return null;
    }
    if ((rec.label?.files || []).length || !(rec.placements || []).length || !rec.verification?.ok) return null;
    return { kind: "own", note: "Not in an open set", title: "Make a QR label of the orders on this sheet, saved beside its files" };
  }
  async function makeLabel(btn) {
    const plan = labelPlan(), id = W.id, rec = W.rec; if (!plan) return;
    const sh = allSheets().find(p => p.sheetId === id), run = window.B && B.run;
    let sheets = [sh];
    if (plan.kind === "include" && sh && Gate.splitWith) {
      const split = Gate.splitWith(sh, true);
      if (split.length) { btn.disabled = true; sheets = await askSplit(sh, split); btn.disabled = false; if (!sheets || W.id !== id) return; }
    }
    const box = W.el.foot.querySelector("[data-r2=qr]"), text = box && box.querySelector("[data-r2=qrText]");
    btn.disabled = true; if (box) box.classList.add("remaking"); if (text) text.textContent = "Making the QR label…";
    const reason = () => { const set = Sets.ofRun(run.runId).find(s => s.group === "dispatch" && !s.committedAt); return set ? Gate.policy(sh, set.seq).reason : "there is no open set to join"; };
    let undo = null;
    try {
      if (plan.kind === "release") {
        const topup = sh.topup && !sh.topup.closedAt ? sh.topup : null;
        sh.releaseFull = true; if (topup) { topup.closedAt = nowT(); topup.byHand = true; }
        undo = () => { if (!sh.draft) return; sh.releaseFull = false; if (topup) { delete topup.closedAt; delete topup.byHand; } api("charmNestLibrary", { op: "putSheet", sheet: { id: sh.sheetId, releaseFull: false, topup: sh.topup || null } }, { quiet: true }).catch(() => {}); };
        await api("charmNestLibrary", { op: "putSheet", sheet: { id: sh.sheetId, releaseFull: true, topup: sh.topup || null } }, { quiet: true });
        await Gate.assemble(run);
        if (sh.draft) throw new Error(reason());
        agent({ metal: sh.metal, run: sh.runId }, "POOL", `${sheetName(sh)} released by hand at ${fmt.pct(sh.density || 0)} full, with its QR label for ${new Set(sh.charms.filter(c => sh.placements.some(p => p.id === c.id)).map(ridOf)).size} orders`);
      } else if (plan.kind === "include") {
        await Gate.changeMembership(sh.metal, true, sheets);
        if (sh.draft) throw new Error(reason());
      } else if (plan.kind === "relabel") {
        await Gate.assemble(run);
        if (!(sh.label?.files || []).length) throw new Error(reason());
      } else await ownLabel(rec, sh);
      if (window.Session) Session.schedule();
      if (S.mode === "library") CN.loadLibrary().catch(() => {});
    } catch (e) {
      if (undo) undo();
      toast("No QR label made: " + e.message, "bad", 8000);
    }
    if (W.id === id && W.dlg.open && !W.flow) open2(id);
  }
  /** A label of its own for a sheet outside any open set: the orders placed on it, in the code the set labels use. */
  async function ownLabel(rec, sh) {
    const O = window.CharmNestOrders, byId = new Map((rec.charms || []).map(c => [c.id, c]));
    const ids = [...new Set((rec.placements || []).map(p => byId.get(p.id)).filter(Boolean).map(ridOf).filter(Boolean))];
    if (!ids.length) throw new Error("no order is placed on this sheet");
    const metal = O.CARD_TO_METAL[rec.metal] || rec.metal, parts = O.safeChunks(ids, metal, 1000, 500, 8), name = rec.fileBase || rec.folder || rec.id;
    const base = rec.outputs?.ai?.path ? rec.outputs.ai.path.replace(/\/[^/]*$/, "") : `charmnest/sheets/${rec.day}/${name}`;
    const files = [];
    for (const [i, slice] of parts.entries()) {
      const payload = O.encodeOrderList(slice, metal);
      const label = `${CN.METAL_TAG[rec.metal] || ""} · ${name}${parts.length > 1 ? ` [${i + 1}/${parts.length}]` : ""} · ${slice.length} order${slice.length === 1 ? "" : "s"}`;
      const png = await Sets.renderLabelPng(payload, label);
      const up = await CN.uploadBytes(`${base}/${name}_label${parts.length > 1 ? `_${i + 1}of${parts.length}` : ""}.png`, png.blob, "image/png", "Saving the sheet label");
      files.push({ path: up.path, url: up.url, sheet: name, part: i + 1, parts: parts.length, orders: slice, payload, ecc: png.ecc, label });
    }
    const made = { files, orders: ids, own: true, at: Date.now() };
    await api("charmNestLibrary", { op: "putSheet", sheet: { id: rec.id, label: made } });
    if (sh) sh.label = made;
    window.SheetEvents?.qrLabel({ sheetId: rec.id, sheet: `${CODE[rec.metal] || ""} Sheet ${sheetNoOf(rec)}`, setId: "", metal: rec.metal }, ids, files, { own: true, by: whoAmI() });
    agent({ metal: rec.metal, run: rec.runId || null }, "cloud", `${name}: QR label made by hand for its ${ids.length} order${ids.length === 1 ? "" : "s"} (not in an open set)`);
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
        try { await api("charmNestLibrary", { op: "deleteSheet", id: rec.id, code }, { label: "Deleting sheet" }); toast(`Sheet ${rec.folder || rec.id} deleted`, "ok"); menu(false); close({ gone: true }); loadLibrary(); }
        catch (e) { btn.disabled = false; btn.textContent = "Delete"; input.value = ""; input.placeholder = e.status === 403 ? "Wrong passcode" : "Not deleted: " + e.message; input.focus(); }
      };
    }
  }

  /* ── the piece pane ── */
  async function showPane(name, dir) {
    const E = W.el, a = E.side.querySelector(`[data-pane="${name === "sheet" ? "piece" : "sheet"}"]`), b = E.side.querySelector(`[data-pane="${name}"]`);
    W.view = name;
    if (!b.hidden && a.hidden) return;
    // (the panel still to come in, with the sheet flying: it comes in showing this one)
    if (W.flip && !W.flip.shown) { a.hidden = true; b.hidden = false; return; }
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
    // (opened on it: it rings once the sheet has landed, its picture drawn now that its design is known)
    if (opts.land) { paintFx(); afterCue(() => { if (W.sel === x && W.dlg.open && W.rang !== keyOf(x)) { W.rang = keyOf(x); land(x); } }); } else pulse(x);
    if (same && x.c) { const th = W.el.detail.querySelector("canvas.swThumb"), mm = W.el.detail.querySelector("[data-r2=mm]"); if (th && th._bare) drawThumb(th, x); if (mm) mm.textContent = `${x.qty > 1 ? `Copy ${x.copy} of ${x.qty} · ` : ""}${mmOf(x)}`; }
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
    const mm = mmOf(x);
    E.detail.innerHTML = `
      <header class="swOrderHead"><span class="fLabel">Order</span><span class="rid">${esc(x.rid || "No order")}</span><span class="who">${esc([buyer, placed ? "ordered " + placed : "", ship].filter(Boolean).join(" · ")) || "&nbsp;"}</span>${said ? `<div class="swSaid" title="From the buyer">${esc(said)}</div>` : ""}</header>
      <div class="swPiece"><canvas class="swThumb" width="184" height="184"></canvas><div class="facts"><b>${esc(x.sku || x.name)}</b><span data-r2="mm">${x.qty > 1 ? `Copy ${x.copy} of ${x.qty} · ` : ""}${mm}</span><span>${esc(labelOf(W.rec.metal))}${sp.size ? " · size " + esc(sp.size) : ""}</span>${words ? `<em title="${esc(words)}">${esc(words)}</em>` : ""}</div></div>
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
  const mmOf = x => x.c ? `${fmt.mm(x.c.widthPt).replace(" mm", "")} × ${fmt.mm(x.c.heightPt)}` : `${fmt.mm(x.p.wPt).replace(" mm", "")} × ${fmt.mm(x.p.hPt)}`;
  function drawThumb(cv, x) {
    const ctx = cv.getContext("2d"); ctx.fillStyle = "#fff"; ctx.fillRect(0, 0, cv.width, cv.height);
    const c = x.c; cv._bare = !c; if (!c) { if (x.thumb) { const im = new Image(); im.crossOrigin = "anonymous"; im.onload = () => { if (!cv._bare) return; const s = Math.min(cv.width / im.width, cv.height / im.height) * .86; ctx.drawImage(im, (cv.width - im.width * s) / 2, (cv.height - im.height * s) / 2, im.width * s, im.height * s); }; im.src = cors(x.thumb); } return; }
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
    else if (e.kind === "preparing") acts = `<span class="swWait">${e.working ? '<span class="owSpin"></span>' : ""}${esc(e.note || "Being prepared")}</span><button class="btn ghost sm" data-e="engrave">Open in Engrave${ICON.go}</button>`;
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
    const who = needName(() => approveHere(x, b)); if (!who) return;
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
    // a piece the pool has not put on a sheet yet may already sit on one this sorter holds
    for (const it of items.values()) if (!it.sheetId && it.poolId && window.Pool) { const sh = Pool.sheetOf(it.poolId); if (sh && sh.sheetId) Object.assign(it, { sheetId: sh.sheetId, metal: sh.metal, n: sh.sheetIndex || sh.page || 1 }); }
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
    } else if (x.poolId) {
      // one copy of a line never comes off alone: held, a line is pooled again whole, so its copies travel together
      ids.add(x.poolId);
      const row = (window.Orders ? Orders.rows() : []).find(r => (r.poolIds || []).includes(x.poolId));
      if (row) for (const id of row.poolIds) ids.add(id);
    }
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
      else if (sh.laserDoneAt) stay.push({ id, sku, where, why: "that sheet was marked completed" });
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
    let okF = ok.filter(o => !o.last), stayF = stay.concat(ok.filter(o => o.last).map(o => Object.assign(o, { why: "it is the last charm on that sheet: delete the sheet from its menu instead" })));
    // a line with a copy that must stay keeps all its copies: half a line on hold could never be put back whole
    for (const r of window.Orders ? Orders.rows() : []) {
      const line = new Set(r.poolIds || []); if (line.size < 2) continue;
      const held = stayF.find(o => line.has(o.id)); if (!held || !okF.some(o => line.has(o.id))) continue;
      for (const o of okF.filter(o => line.has(o.id))) stayF.push(Object.assign(o, { why: `its line stays together (a copy is on ${held.where}: ${held.why})` }));
      okF = okF.filter(o => !line.has(o.id));
    }
    return { rid, whole, ids, ok: okF, stay: stayF };
  }
  const listWhere = list => [...new Set(list.map(o => o.where))].join(", ");
  function renderOffBtn(x) {
    const host = W.el.detail.querySelector("[data-r2=off]"); if (!host) return;
    if (!x.poolId && !x.rid) { host.remove(); return; }
    if (W.flow) { host.innerHTML = ""; return; }
    host.innerHTML = `<button type="button" class="swOffBtn">${ICON.off}Take off the sheet…</button>`;
    host.querySelector("button").onclick = () => renderOff(x);
  }
  function renderOff(x) {
    const host = W.el.detail.querySelector("[data-r2=off]"); if (!host) return;
    const mates = x.rid ? offPlan(x, true) : null, one = offPlan(x, false);
    const many = mates && mates.ids.size > one.ids.size, lineN = one.ids.size;
    let pick = many ? "all" : "one", then = "hold", note = "";
    const paint = () => {
      const plan = pick === "all" ? mates : one, n = plan.ok.length;
      // only a whole order is cancelled: one charm of it comes off and waits on hold
      const whole = !!x.rid && (pick === "all" || !many);
      if (!whole) then = "hold";
      const stay = plan.stay.length ? `<div class="stay"><b>${plan.stay.length === 1 ? "1 piece stays" : plan.stay.length + " pieces stay"}</b>: ${plan.stay.map(o => `${esc(o.sku)} on ${esc(o.where)} (${esc(o.why)})`).join("; ")}.</div>` : "";
      const text = then === "cancel"
        ? `The order leaves every list in the sorter. Its record is kept under Orders › Cancelled, where it can be restored.${n ? " Every other charm stays where it is, and each sheet gets a new QR label." : ""}`
        : !n ? "" : whole ? `The order waits under On hold, here and in Orders, until someone puts it back. Every other charm stays exactly where it is, and each sheet gets a new QR label.`
        : `${lineN > 1 ? `Its line (${lineN} copies) waits` : "Only this charm waits"} under On hold; the order's other pieces stay and are cut.`;
      const label = then === "cancel" ? (n ? "Take off and cancel" : "Cancel the order") : "Take off and hold";
      host.innerHTML = `<div class="swOff" role="group" aria-label="Take off the sheet"><h4>Take off the sheet</h4>
        <div class="pick">${many ? `<label class="opt"><input type="radio" name="swOffWho" value="all"${pick === "all" ? " checked" : ""}><b>The whole order · ${mates.ids.size} pieces</b><small>${esc(listWhere(mates.ok.concat(mates.stay)))}</small></label>` : ""}
        <label class="opt"><input type="radio" name="swOffWho" value="one"${pick === "one" ? " checked" : ""}><b>${lineN > 1 ? `This line · ${lineN} copies` : many ? "Only this charm" : "This charm"}</b><small>${esc(x.sku || x.name)}${lineN > 1 ? ", its copies go together" : x.qty > 1 ? ` · copy ${x.copy} of ${x.qty}` : ""} · ${esc(lineN > 1 ? listWhere(one.ok.concat(one.stay)) : sheetWord(W.id, W.rec.fileBase))}</small></label></div>
        ${x.rid ? `<div class="then" role="radiogroup" aria-label="Then"><span class="fLabel">Then</span><button type="button" role="radio" data-then="hold" aria-checked="${then === "hold"}">${ICON.pause}Put on hold</button><button type="button" role="radio" data-then="cancel" aria-checked="${then === "cancel"}"${whole ? "" : ' disabled title="Only a whole order can be cancelled"'}>${ICON.off}Cancel the order</button></div>` : ""}
        <input class="swNote" type="text" maxlength="200" placeholder="${then === "cancel" ? "Why it was cancelled (optional)" : "Note for whoever puts it back (optional)"}" value="${esc(note)}" aria-label="Note">
        ${stay}${text ? `<div class="note">${text}</div>` : ""}
        <div class="acts">${n || then === "cancel" ? `<button type="button" class="btn ${then === "cancel" ? "danger" : "velvet"} sm" data-o="go">${then === "cancel" ? ICON.off : ICON.pause}${label}</button>` : ""}<button type="button" class="btn ghost sm" data-o="keep">${n || then === "cancel" ? "Keep" : "Close"}</button></div></div>`;
      host.querySelectorAll("input[name=swOffWho]").forEach(r => r.onchange = () => { pick = r.value; paint(); });
      host.querySelectorAll("[data-then]").forEach(b => b.onclick = () => { if (b.disabled) return; then = b.dataset.then; paint(); host.querySelector(".swNote")?.focus(); });
      const ni = host.querySelector(".swNote"); ni.oninput = () => { note = ni.value; }; ni.onkeydown = e => { if (e.key === "Enter") { e.preventDefault(); host.querySelector("[data-o=go]")?.click(); } };
      host.querySelector("[data-o=keep]").onclick = () => { animate(host.firstElementChild, [{ opacity: 1 }, { opacity: 0, transform: "translateY(-4px)" }], 140).then(() => renderOffBtn(x)); };
      const go = host.querySelector("[data-o=go]"); if (go) go.onclick = () => {
        const who = needName(() => { if (go.isConnected) go.click(); }); if (!who) return;
        const p = pick === "all" ? offPlan(x, true) : offPlan(x, false), opt = { then, note: note.trim() };
        if (!p.ok.length && then !== "cancel") return paint();   // nothing can come off any more: the panel says why
        (p.ok.length ? takeOff(p, opt, who) : cancelOnly(p.rid, opt, who)).catch(e => { console.error("sheet window: take off", e); toast((then === "cancel" ? "Not cancelled: " : "Not taken off: ") + e.message, "bad", 8000); });
      };
    };
    paint();
    requestAnimationFrame(() => host.scrollIntoView({ block: "nearest", behavior: still() ? "auto" : "smooth" }));
  }
  // (the panels above the orders list open and close without the list jumping: it glides to its new place)
  function renderWork() { steady(W.el.work, drawWork); }
  function drawWork() {
    const E = W.el, w = W.work; if (!E.work) return;
    if (E.addBtn && W.rec) E.addBtn.hidden = !!W.flow || !canAdd();
    if (!w) { E.work.innerHTML = ""; E.work.hidden = true; return; }
    E.work.hidden = false;
    E.work.innerHTML = `<div class="swWork${w.state === "done" ? " done" : w.state === "failed" ? " failed" : ""}" role="status"><h4>${w.state === "working" ? '<span class="owSpin"></span>' : w.state === "done" ? ICON.check.replace("<svg", '<svg style="width:15px;height:15px;color:var(--sage)"') : ""}${esc(w.title)}</h4>
      <ol>${w.steps.map(st => `<li class="${st.state || ""}"><i></i><span>${esc(st.text)}${st.detail ? ` <small style="color:var(--ink45)">· ${esc(st.detail)}</small>` : ""}</span></li>`).join("")}</ol>
      ${w.note ? `<div class="note">${w.note}</div>` : ""}${w.state !== "working" ? `<div style="display:flex;justify-content:flex-end"><button type="button" class="btn ghost xs" data-w="ok">Done</button></div>` : ""}</div>`;
    const b = E.work.querySelector("[data-w=ok]"); if (b) b.onclick = () => { const el = E.work.firstElementChild; animate(el, [{ opacity: 1 }, { opacity: 0, transform: "translateY(-4px)" }], 160).then(() => { W.work = null; renderWork(); }); };
  }
  const pause = ms => new Promise(r => setTimeout(r, ms));
  const stepOf = (text, state, detail) => ({ text, state: state || "", detail: detail || "" });
  const rowsOfOrder = rid => (window.Orders ? Orders.rows() : []).filter(r => String(r.order.receiptId) === String(rid) && r.state !== "gone");
  const ridOf = c => String(c.order || c.poolId || "").split(/[/_]/)[0];
  const nowT = () => (window.SimClock && SimClock.now ? SimClock.now() : Date.now());
  function ago(t) {
    if (!t) return "";
    const m = Math.max(0, (nowT() - t) / 60000);
    return m < 60 ? `${Math.max(1, Math.round(m))} min ago` : m < 48 * 60 ? `${Math.round(m / 60)} h ago` : `${Math.round(m / 1440)} d ago`;
  }
  // a sheet in the middle of a search or a save finishes that first (three minutes at most)
  async function waitIdle(pages, st, paint) {
    if (!pages.some(busy)) return;
    st.state = "now"; paint();
    const until = Date.now() + 180000;
    while (pages.some(busy) && Date.now() < until) { const b = pages.find(busy); st.detail = `${sheetWord(b.sheetId, b.fileBase, b)}: ${b.stage || b.status}`; paint(); await pause(400); }
    if (pages.some(busy)) throw new Error("a sheet is still busy after three minutes; try again once it has saved");
    st.state = "ok"; st.detail = "";
  }
  // one sheet written again as it now stands: the pieces already on it stay put, and it is verified and saved
  async function rewritePage(sh, st, paint) {
    st.state = "now"; paint();
    if (!allSheets().includes(sh) || !sh.placements.length && !activeCharms(sh).length) { st.state = "ok"; st.detail = "nothing left to write"; return true; }
    // the run may already have started it on its own (the sheet was left to be written): that search is this one
    const job0 = sh.jobId, running = busy(sh), prob0 = sh.problem || null, t0 = Date.now();
    if (!running) { sh._byHand = true; startNest(sh); }
    const until = t0 + 240000;
    while (Date.now() < until) {
      if ((running || sh.jobId !== job0) && !busy(sh) && (sh.persistedDone || sh.problem)) break;
      // a new problem on a sheet nothing is working on, or a search that never started, ends the wait at once
      // (either used to sit out the full 4 minutes)
      if (!busy(sh) && ((sh.problem && sh.problem !== prob0) || (!running && sh.jobId === job0 && sh.status !== "queued" && Date.now() - t0 > 5000))) break;
      st.detail = sh.stage || (sh.status === "queued" ? "waiting its turn" : sh.status); paint(); await pause(350);
    }
    if (sh.problem) throw new Error(`${sheetWord(sh.sheetId, sh.fileBase, sh)}: ${sh.problem}`);
    if (!running && sh.jobId === job0 || busy(sh) || !sh.persistedDone) { st.state = "now"; st.detail = "still saving; it finishes on its card in the Nest tab"; return false; }
    st.state = "ok"; st.detail = sh.verification && !sh.verification.ok ? "verification flagged, see its report" : ""; return true;
  }
  // A sheet of a run under the set rules is a working sheet again once it is written, and the set takes it back with a
  // new label while it is still ready for the laser (Gate.assemble, as at every check); one that is no longer full
  // enough fills again first, and the label it had (the old orders) goes. An older run's sheet got its label as it saved.
  // A sheet already released to its set stays there through a change made here: it is rewritten as it now stands and
  // gets a new QR label for its new orders at once. It used to be written as "not full", leave its set and lose its label
  // until it filled again (Paul, 26 Sep). The mark is read by the nest (it keeps releaseFull), by Gate.assemble (it does
  // not take the sheet out of its set while it is rewritten) and by LiveNest.closed (no arrival goes onto it meanwhile).
  const released = sh => !!sh.releaseFull || (!!sh.setId && !sh.draft);
  function holdRelease(pages) { for (const sh of pages) if (released(sh) && !sh.roseCutAt && !sh.laserDoneAt && !sh.recalled) sh.keepRelease = { full: !!sh.releaseFull, at: Date.now() }; }
  function letGo(pages) { for (const sh of pages) if (sh.keepRelease && !busy(sh)) delete sh.keepRelease; }
  async function remakeLabels(pages, st, paint) {
    st.state = "now"; paint();
    const run = window.B && B.run;
    if (run && window.Gate && Gate.modern(run.runId) && pages.some(sh => sh.runId === run.runId)) {
      try { await Gate.assemble(run); } catch (e) { st.state = "bad"; st.detail = e.message; }
    }
    for (const sh of pages) if (allSheets().includes(sh) && sh.draft && sh.label && sh.sheetId) {
      // a label made by hand for a sheet outside any set is made again for its new orders; a set's old label goes
      if (sh.label.own) { try { const r = await api("charmNestLibrary", { op: "getSheet", id: sh.sheetId }, { quiet: true }); if (r.sheet) { await ownLabel(r.sheet, sh); continue; } } catch (e) { console.warn("sheet window: own label", e); } }
      sh.label = null;
      await api("charmNestLibrary", { op: "putSheet", sheet: { id: sh.sheetId, label: null } }, { quiet: true }).catch(e => console.warn("sheet window: label", e));
    }
    if (st.state === "now") st.state = "ok";
  }
  // What was saved is read back: each piece is on the sheet it should be on and on no other, and the piece records
  // agree. Only then is the change stamped as verified on the pieces.
  async function verifySaved(want, ids, pool, st, paint) {
    st.state = "now"; paint();
    const bad = [];
    for (const w of want) {
      if (!w.sh.sheetId || !allSheets().includes(w.sh)) continue;
      const r = await api("charmNestLibrary", { op: "getSheet", id: w.sh.sheetId }, { quiet: true }).catch(e => ({ error: e.message }));
      const rec = r && r.sheet; if (!rec) { bad.push(`${w.name} could not be read back`); continue; }
      const byId = new Map((rec.charms || []).map(c => [c.id, c])), placed = new Set((rec.placements || []).map(p => byId.get(p.id)?.poolId).filter(Boolean));
      const missing = [...(w.on || [])].filter(id => !placed.has(id)).length, extra = [...(w.off || [])].filter(id => placed.has(id)).length;
      if (missing) bad.push(`${w.name} is missing ${missing === 1 ? "1 piece" : missing + " pieces"}`);
      if (extra) bad.push(`${w.name} still lists ${extra === 1 ? "1 piece" : extra + " pieces"}`);
    }
    if (pool) {
      const r = await api("charmNestLibrary", { op: "poolGet", poolIds: [...ids].slice(0, 300) }, { quiet: true }).catch(() => null);
      const off = r ? Object.values(r.pools || {}).filter(p => !pool(p)).length : 0;
      if (!r) bad.push("the piece records could not be read");
      else if (off) bad.push(`${off === 1 ? "1 piece record disagrees" : off + " piece records disagree"}`);
    }
    if (bad.length) { st.state = "bad"; st.detail = bad.join("; "); return false; }
    st.state = "ok"; st.detail = "the saved sheets and piece records match";
    return true;
  }
  function doneStep(st) { if (st && st.state === "now") st.state = "bad"; }
  // one change at a time, whichever sheet it is on: while it runs its panel shows on every sheet of the window, and
  // each flow writes only to its own panel (switching sheets or reopening the window never cuts it short)
  function beginFlow(work) {
    if (W.flow) { toast(`One change at a time: ${W.flow.title.charAt(0).toLowerCase() + W.flow.title.slice(1)} is still running`, "", 4500); return null; }
    W.flow = work; W.work = work; return work;
  }
  function endFlow(work) { if (W.flow === work) W.flow = null; if (W.dlg && W.dlg.open) renderWork(); }

  async function takeOff(plan, opt, who) {
    const list = plan.ok; if (!list.length) return;
    const cancel = opt.then === "cancel" && !!plan.rid, note = opt.note || "";
    const ids = new Set(list.map(o => o.id)), rid = plan.rid;
    const pages = [...new Set(list.map(o => o.sh).filter(Boolean))];
    const sheetId = W.id, names = listWhere(list.filter(o => o.sh));
    const rewrite = pages.map(sh => ({ sh, name: sheetWord(sh.sheetId, sh.fileBase, sh), st: stepOf(`Rewriting ${sheetWord(sh.sheetId, sh.fileBase, sh)}`) }));
    const labels = stepOf(`Remaking the QR label${pages.length === 1 ? "" : "s"}`), check = stepOf("Checking the saved sheets");
    const wait = stepOf("Waiting for the sheets to finish their current step");
    const off = stepOf(`Taking ${list.length === 1 ? "1 piece" : list.length + " pieces"} off${names ? " " + names : ""}`);
    const keep = stepOf("Keeping its record under Cancelled orders"), gone = stepOf("Taking it off every list");
    const work = beginFlow({ state: "working", title: cancel ? `Cancelling order ${rid}` : `Taking off ${plan.whole && rid ? "order " + rid : (list[0].sku || "the charm")}`, steps: [wait, off, ...rewrite.map(r => r.st), ...(pages.length ? [labels, check] : []), ...(cancel ? [keep, gone] : [])], note: "" }); if (!work) return;
    if (!pages.some(busy)) work.steps.shift();
    // the pieces leave the plates at once, in clay, and leave the outline of the room they freed (kept per sheet)
    const t0 = performance.now();
    for (const o of list) { const y = W.byPool.get(o.id); if (y && !y.gone) { y.gone = true; W.freed.push({ x: y, t0, rid: y.rid, sku: y.sku, at: Date.now() }); } }
    setFreed(sheetId, W.freed);
    for (const sh of pages) if (sh.sheetId && sh.sheetId !== sheetId) {
      const g = (FREED.get(sh.sheetId) || []).slice();
      for (const c of sh.charms) if (ids.has(c.poolId)) { const p = sh.placements.find(q => q.id === c.id); if (p) g.push({ x: { id: c.id, c, gone: true, p: { cxPt: p.cxPt, cyPt: p.cyPt, angle: p.angle || 0, scale: p.scale || 1, wPt: p.wPt || c.widthPt || 20, hPt: p.hPt || c.heightPt || 20 } }, t0: 0, rid: ridOf(c), sku: c.sku || c.name, at: Date.now() }); }
      setFreed(sh.sheetId, g);
    }
    W.hover = null; tip(null); W.el.plate.classList.remove("onCharm"); W.ghost = null;
    // the order's row goes with it: to On hold, or folded away when the order is cancelled (copied before the list
    // is drawn again; from the charm's pane, once the list has slid back)
    const skus = [...new Set(list.map(o => o.sku).filter(Boolean))];
    const cue = offCue(rid, cancel ? "cancel" : "hold", plan.whole || !skus.length ? `Order ${rid} is on hold` : `Order ${rid} · ${skus.join(", ")} is on hold`);
    // (the steps panel first: from the charm's pane it is there already when the list slides back, instead of pushing
    // the list down under the eye)
    renderWork(); renderFill();
    if (W.view === "piece") showSheetPane(); else { renderStrip(); renderSheetPane(); }
    // (after the pane is back: going back to the list used to stop the clay fade on its first frame)
    W.fx.push({ kind: "freed", t0, ms: 1020 }); fxLoop(); paintBase();
    if (cue) cue.go();
    const paint = () => { if (W.dlg.open && W.work === work) renderWork(); };
    let changed = false;
    try {
      await waitIdle(pages, wait, paint);
      // while it waited the run may have passed a piece on to another sheet: then nothing is changed, and it is tried again
      for (const o of list) {
        const now = allSheets().filter(sh => sh.charms.some(c => c.poolId === o.id));
        if (o.sh ? now.length !== 1 || now[0] !== o.sh : now.length) throw new Error("the sheets changed while this waited, so nothing was taken off; open the charm and try again");
      }
      changed = true;
      holdRelease(pages);
      off.state = "now"; paint();
      // 1 · off the sheets this sorter holds (the rest of each sheet stays as placed: Orders.keepRest)
      for (const sh of pages) {
        const gone2 = sh.charms.filter(c => ids.has(c.poolId));
        sh.charms = sh.charms.filter(c => !ids.has(c.poolId));
        sh.placements = sh.placements.filter(p => sh.charms.some(c => c.id === p.id));
        sh.rejects = (sh.rejects || []).filter(id => sh.charms.some(c => c.id === id));
        if (Array.isArray(sh.feedWait)) sh.feedWait = sh.feedWait.filter(id => sh.charms.some(c => c.id === id || c.poolId === id));
        if (sh.backPool) sh.backPool = sh.backPool.filter(b => !ids.has(b.poolId));
        Orders.keepRest(sh);
        agent({ metal: sh.metal, run: sh.runId }, "POOL", `${gone2.length} piece${gone2.length === 1 ? "" : "s"} of ${rid || "an order"} taken off ${sheetName(sh)} by ${who}${note ? " (" + note + ")" : ""}${cancel ? " to cancel the order" : ", on hold"}; the rest stay where they are`);
      }
      // 2 · the order's lines wait on hold, so the run does not place them again; their engraving goes with the pieces
      //     (a cancel takes them off every list once its record is kept, below)
      const text = `Taken off ${names || "its sheet"} by ${who}${note ? ": " + note : ""}`;
      for (const r of Orders.rows()) {
        const mineRow = plan.whole && rid && String(r.order.receiptId) === rid;
        if (!mineRow && !(r.poolIds || []).some(id => ids.has(id))) continue;
        r.poolIds = (r.poolIds || []).filter(id => !ids.has(id));
        const j = Engrave.items().get(r.key);
        if (j) { j.copies = (j.copies || []).filter(id => !ids.has(id)); if (!j.copies.length) { Engrave.items().delete(r.key); Review.remove("eng:" + r.key); } }
        if (!r.poolIds.length && r.state !== "gone") { r.state = "held"; r.hold = r.reason = text; r.heldAt = Date.now(); }
      }
      if (cue && !cancel && rowsOfOrder(rid).some(r => r.hold)) cue.done();   // (its note: it is on hold now)
      await Pool.update([...ids], { state: "abandoned", sheetId: null, setId: null, removedBy: who, removedReason: cancel ? "cancelled" + (note ? ": " + note : "") : note || "on hold", removedAt: Date.now() });
      for (const id of ids) B.pool.rows.delete(id);
      // on hold (a cancel is stamped by the server as the order is cancelled): on the order's timeline, with who
      if (!cancel && rid) window.SheetEvents?.order({ type: "held", orderId: rid, id: `sw-${sheetId}-${Date.now()}`, by: who, sheetId, sheet: names || "", text: `${text}, on hold`.slice(0, 200), data: { pieces: list.length, note: note || undefined } });
      // 3 · the set: the order leaves the sheets it was on (labels are remade when each sheet is saved again)
      await dropFromSets(ids);
      if (B.run) { B.run.lines = Object.fromEntries(Orders.rows().map(Orders.lineRecord)); await RunCtl.save(B.run).catch(e => console.warn("sheet window: run lines", e)); }
      Review.syncOrderItems(); Orders.render(); Engrave.render();
      off.state = "ok"; paint();
      // 4 · each sheet written again as it now stands, with a new QR label, and read back
      for (const r of rewrite) await rewritePage(r.sh, r.st, paint);
      if (pages.length) {
        await remakeLabels(pages, labels, paint);
        letGo(pages);
        const ok = await verifySaved(rewrite.filter(r => r.st.state === "ok").map(r => ({ sh: r.sh, name: r.name, off: ids })), ids, p => p.state === "abandoned", check, paint);
        if (ok) await Pool.update([...ids], { removedVerifiedAt: Date.now() }).catch(() => {});
      }
      // 5 · a cancel: the record is kept first, then the order leaves every list
      if (cancel) {
        // (what became of it on each sheet goes on its record, as AutoCancel writes it: Orders › Cancelled reads it)
        const fates = rewrite.map(r => ({ sheet: r.name, fate: "removed", text: `taken off ${r.name}` }));
        for (const o of plan.stay) if (!fates.some(f => f.sheet === o.where)) fates.push({ sheet: o.where, fate: "cut", text: /already cut|completed|sent to the station/.test(o.why) ? `already cut on ${o.where}: set aside` : `stays on ${o.where}: set aside once cut` });
        await cancelRecord(rid, { note, who, sheets: names, fates }, keep, gone, paint); if (cue) cue.done();
      }
      if (window.RunCtl) RunCtl.poke();
      const open = rewrite.some(r => r.st.state === "now");
      const fates = rewrite.filter(r => allSheets().includes(r.sh) && r.sh.placements.length).map(r => {
        const sh = r.sh, set = sh.setId && !sh.draft && window.Sets && [...(B.sets?.values?.() || [])].find(z => z.setId === sh.setId);
        return set ? `${esc(r.name)} stays in ${esc(set.name || "its set")} with a new QR label.` : `${esc(r.name)} is filling again (${fmt.pct(sh.density || 0)} full); new orders that fit go into the freed room.`;
      });
      work.state = "done";
      work.title = cancel ? `Order ${rid} cancelled` : open ? "Taken off · a sheet is still saving" : "Taken off";
      work.note = fates.join(" ") + (cancel ? ` The record is under Orders › Cancelled.` : rid && rowsOfOrder(rid).some(r => r.hold) ? ` Order ${esc(rid)} is under On hold.` : "") +
        (plan.stay.length ? ` ${plan.stay.length === 1 ? "1 piece" : plan.stay.length + " pieces"} stayed (${esc(plan.stay.map(o => o.where + ": " + o.why).join("; "))})${cancel ? ": set them aside once cut" : ""}.` : "");
      if (cue) cue.end();
      paint(); endFlow(work);
      if (W.id === sheetId && W.dlg.open) open2(sheetId);
    } catch (e) {
      for (const st of work.steps) doneStep(st);
      letGo(pages);
      if (!changed) {   // nothing came off: the room it would have freed is not free
        for (const o of list) { const y = W.byPool.get(o.id); if (y) y.gone = false; }
        const mine = g => ids.has(g.x.poolId || (g.x.c && g.x.c.poolId));
        for (const id of new Set([sheetId, ...pages.map(sh => sh.sheetId).filter(Boolean)])) setFreed(id, (FREED.get(id) || []).filter(g => !mine(g)));
        W.freed = W.freed.filter(g => !mine(g));
      }
      if (cue) cue.fail();   // (a row that did not go comes back into its list)
      work.state = "failed"; work.title = cancel ? "Not cancelled" : "Not everything came off";
      work.note = esc(e.message) + (!changed ? "." : cancel ? ". The order is under On hold; cancel it from there." : ". Pieces already taken off stay off; the sheet window shows the sheet as saved now.");
      paint(); endFlow(work);
      if (W.id === sheetId && W.dlg.open) open2(sheetId);
      throw e;
    }
  }
  // the sheet as it is saved now, with the room the pieces freed marked on it
  function open2(id) { return open(id, { keepWork: true, keepSet: false }); }
  // the pieces leave the set records that list them (the labels are remade when each sheet is saved again)
  async function dropFromSets(ids) {
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
  }

  /* ── cancelled orders: kept as a record (Charm_Nest_Cancelled), taken off every list, left out of every later pull ── */
  const Cancelled = window.Cancelled = (() => {
    let ids = new Set(), at = 0, loading = null, list = null;
    const mineAt = new Map();                                       // orderId → when this screen cancelled it (its own flight)
    const fresh = new Map();                                        // cancels seen live → when: marked once when their row is drawn
    // ids learned between two reads (AutoCancel's newest records, a cancel made here): a read that set out before them
    // does not take them back out
    const learned = new Map();
    function load(force) {
      if (loading) return loading;
      if (!force && at && Date.now() - at < 60000) return Promise.resolve(ids);
      const t0 = Date.now();
      loading = api("charmNestLibrary", { op: "cancelList", idsOnly: true }, { quiet: true })
        .then(r => {
          const was = at ? ids : null; ids = new Set((r.ids || []).map(String));
          for (const [id, t] of learned) { if (t >= t0) ids.add(id); else if (Date.now() - t > 600000) learned.delete(id); }
          at = Date.now(); if (was) arrived([...ids].filter(id => !was.has(id))); return ids;
        })
        // (never read yet: the orders check waits, so a cancelled order cannot come back in; read once, the last list stands)
        .catch(e => { console.warn("cancelled orders", e.message); if (!at) throw new Error(`the cancelled orders could not be read (${e.message})`); return ids; })
        .finally(() => { loading = null; });
      return loading;
    }
    const has = rid => ids.has(String(rid));
    /* The records, newest first, read a page at a time (the library's newest 200, then up to its 500): the list is drawn
       from what was read and read again behind it when it is a minute old, so a search or a redraw never waits. */
    const PAGE = 200, MAX = 500;
    let asked = PAGE, more = false, listAt = 0, reading = null;
    const extra = new Map();                                        // older ones looked up by number (cancelCheck)
    const sortList = () => list.sort((a, b) => (+b.at || 0) - (+a.at || 0));
    const recordOf = rid => (list || []).find(c => String(c.orderId) === String(rid)) || null;
    function history(n) {
      if (n) asked = Math.min(MAX, Math.max(asked, n));
      if (reading) return reading;
      reading = api("charmNestLibrary", { op: "cancelList", limit: asked }, { quiet: true }).then(r => {
        const got = r.list || [], first = !list && !at, known = new Set([...ids, ...(list || []).map(c => String(c.orderId))]);
        list = got.slice(); more = !!r.truncated; listAt = Date.now();
        for (const [rid, c] of extra) if (!list.some(x => String(x.orderId) === rid)) list.push(c);
        sortList();
        for (const c of got) ids.add(String(c.orderId));
        if (!first) arrived(got.map(c => String(c.orderId)).filter(rid => !known.has(rid)));
        return list;
      }).finally(() => { reading = null; });
      return reading;
    }
    /** One older order by its number, beyond the pages read: what the stations' check knows of it (no buyer or lines). */
    async function lookup(rid) {
      const r = await api("charmNestLibrary", { op: "cancelCheck", orderIds: [String(rid)] }, { quiet: true });
      const x = r && r.cancelled && r.cancelled[rid]; if (!x) return null;
      const c = Object.assign({ orderId: String(rid) }, x); extra.set(String(rid), c);
      if (list && !recordOf(rid)) { list.push(c); sortList(); }
      return c;
    }
    /** New cancels seen live (not this screen's own, which fly from its sheet window): the Orders tab shows them come in.
     *  Their records say who cancelled them; read once, never waited on by the orders check. */
    function arrived(rids) {
      rids = rids.filter(rid => !(mineAt.get(rid) > Date.now() - 120000)); if (!rids.length) return;
      listAt = 0; for (const rid of rids.slice(0, 6)) fresh.set(rid, Date.now());   // (a burst marks only its first few)
      const tell = () => { try { if (window.Orders && Orders.cancelArrived) Orders.cancelArrived(rids.map(rid => { const c = recordOf(rid) || { orderId: rid }; return { orderId: rid, etsy: isEtsy(c), by: c.by || "" }; })); } catch (e) { console.warn("cancelled orders: arrival", e); } };
      if (rids.every(recordOf)) tell(); else history().then(tell, tell);
    }
    async function put(rec) {
      const rid = String(rec.orderId); mineAt.set(rid, Date.now());
      const r = await api("charmNestLibrary", Object.assign({ op: "cancelPut" }, rec), { quiet: true }); ids.add(rid); learned.set(rid, Date.now());
      // the record as kept goes at the top of what was read; the next reading confirms it
      if (list) { list = list.filter(c => String(c.orderId) !== rid); list.unshift((r && r.record) || Object.assign({ orderId: rid, by: rec.by, why: rec.why, at: Date.now() }, rec.record || {})); listAt = 0; }
      return r;
    }
    async function restore(rid) { await api("charmNestLibrary", { op: "cancelRestore", orderId: String(rid) }, { quiet: true }); ids.delete(String(rid)); learned.delete(String(rid)); extra.delete(String(rid)); if (list) list = list.filter(c => String(c.orderId) !== String(rid)); }
    /** Ids read elsewhere (AutoCancel's newest records) join the cache at once, and come into the Cancelled tab as any
     *  cancel seen live does; returns how many were new to it. */
    function absorb(more) {
      const now = Date.now(), fresh1 = [];
      for (const x of more || []) { const id = String(x); if (!ids.has(id)) { ids.add(id); fresh1.push(id); } learned.set(id, now); }
      if (fresh1.length && at) arrived(fresh1);
      return fresh1.length;
    }

    /* ── the Orders tab's Cancelled list (Paul, 28 Sep, A5): every cancelled order, newest first. A row: the order, its
       buyer and lines, who cancelled it (Etsy, or a person by name) and when, why, and what happened on each sheet it was
       on. A click opens the order view; a person's cancel can be restored (an inline confirm, never a pop-up); Etsy's
       cannot. The tab's search box narrows it (order, buyer, SKU); 200 rows are drawn at a time. ── */
    const isEtsy = c => String(c.source || "").toLowerCase() === "etsy" || (!c.source && /^etsy$/i.test(String(c.by || "")));
    const when = t => t ? new Date(t).toLocaleString(undefined, { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }) : "";
    /** What happened on each sheet the order was on: taken off one not cut yet, or set aside on one already cut. Read from
     *  `fates` ([{ sheet, fate|state|what, text }], "cut"/"closed"/"aside" meaning already cut) or `cutSheets`, then
     *  `sheets` (the ones it was taken off; a name saying "cut" was already cut). */
    function fatesOf(c) {
      const out = [], seen = new Set(), cutWord = s => /cut|closed|aside|laser|kept/i.test(s) && !/uncut|not.?cut/i.test(s);
      const add = (sheet, cut, text) => {
        sheet = String(sheet || "").trim(); if (!sheet && !text) return;
        const k = sheet.toLowerCase(); if (k && seen.has(k)) return; if (k) seen.add(k);
        out.push({ cut: !!cut, text: text || (cut ? `already cut on ${sheet}: set aside` : `taken off ${sheet}`) });
      };
      for (const f of [].concat(c.fates || c.sheetFates || [])) {
        if (!f) continue;
        if (typeof f === "string") { add(f.replace(/\s*[:(].*$/, ""), cutWord(f)); continue; }
        const how = String(f.fate || f.state || f.what || f.action || "");
        add(f.sheet || f.sheetName || f.label || f.name || f.sheetId, f.cut === true || f.closed === true || cutWord(how), f.text ? String(f.text) : "");
      }
      for (const s of [].concat(c.cutSheets || [])) add(s, true);
      for (const s of c.sheets || []) { const str = String(s || ""), cut = /\b(cut|set aside)\b/i.test(str); add(cut ? str.replace(/\s*[:(—–-]+\s*(already\s+)?(cut|set aside).*$/i, "") || str : str, cut); }
      return out;
    }
    const matches = (c, q) => {
      if (!q) return true; q = q.toLowerCase();
      if (String(c.orderId).includes(q.replace(/^#/, ""))) return true;
      if (String(c.buyer || "").toLowerCase().includes(q)) return true;
      return (c.lines || []).some(l => String(l.sku || "").toLowerCase().includes(q) || String(l.title || "").toLowerCase().includes(q));
    };
    function rowHtml(c, openable) {
      const etsy = isEtsy(c), rid = String(c.orderId), fates = fatesOf(c), lines = c.lines || [];
      const why = String(c.why || "").trim();
      return `<div class="cxId"><b class="mono">${esc(rid)}</b><span>${esc(c.buyer || "")}</span></div>
        <div class="cxWhat">${lines.map(l => `<span class="mono" title="${esc(l.title || "")}">${esc(l.sku || "no SKU")}${l.quantity > 1 ? ` ×${l.quantity}` : ""}</span>`).join("") || `<i>no lines kept</i>`}</div>
        <div class="cxWhy"><div class="cxWho"><span class="cxBadge ${etsy ? "etsy" : "person"}">${etsy ? "Cancelled on Etsy" : `Cancelled by ${esc(c.by || "someone")}`}</span><time datetime="${c.at ? new Date(+c.at).toISOString() : ""}">${esc(when(+c.at))}</time></div>
          <div class="cxReason${why ? "" : " none"}">${why ? esc(why) : etsy ? "Etsy gave no reason" : "No reason given"}</div>
          ${fates.length ? `<div class="cxFates">${fates.map(f => `<span class="${f.cut ? "cut" : "off"}">${esc(f.text)}</span>`).join("")}</div>` : ""}</div>
        <div class="cxAct">${etsy ? "" : `<button class="btn ghost sm" type="button" data-cx="restore" title="Bring the order back: it returns with the next orders check if it is still open on Etsy">Restore</button>`}${openable ? `<span class="cxGo" aria-hidden="true">›</span>` : ""}</div>`;
    }
    const nodes = new Map();                                        // orderId → { stamp, node }: rows kept, like the Orders list's
    let want = null, drawnKey = null, limit = PAGE;
    const WAIT = `<div class="cxWait"><span class="owSpin"></span>Reading the cancelled orders…</div>`;
    /** Draws the list into the Orders tab's body (host), with the tab's search (opts.q) and "Show"'s order (opts.focus);
     *  opts.live says the pile is still the one shown, opts.clear lets go of the search. Never waits on the library. */
    function renderInto(host, onChange, opts) {
      opts = opts || {};
      want = { host, onChange, q: String(opts.q || "").trim(), focus: opts.focus ? String(opts.focus) : "", live: opts.live || (() => host.isConnected), clear: opts.clear || null };
      const w = want, redraw = () => paint();   // (paint draws the latest request, while its pile is still the one shown)
      if (!list) {
        if (!host.querySelector(":scope > .cxWrap")) host.innerHTML = WAIT;
        history().then(redraw, e => { if (want !== w || !w.live()) return; host.innerHTML = `<div class="libEmpty">The cancelled orders could not be read: ${esc(e.message)}<button class="btn ghost sm" type="button" data-cx="retry">Try again</button></div>`; host.querySelector("[data-cx=retry]").onclick = () => renderInto(host, onChange, opts); });
        return;
      }
      if (Date.now() - listAt > 60000) history().then(redraw, e => console.warn("cancelled orders", e.message));
      // an order asked for by number and not among the pages read: looked up on its own
      const f = w.focus || (/^\d{6,}$/.test(w.q) ? w.q : "");
      if (f && !recordOf(f) && ids.has(f) && !extra.has(f)) lookup(f).then(redraw, () => {});
      paint();
    }
    function paint() {
      const w = want; if (!w || !list || !w.live()) return;
      const host = w.host, view = !!(window.OrderWin && typeof OrderWin.openOrder === "function");
      // (before the order view there is: only an order still in the pull opens, in its order window)
      const pulled = view ? null : new Set(window.Orders ? Orders.rows().map(r => String(r.order.receiptId)) : []);
      let wrap = host.querySelector(":scope > .cxWrap");
      if (!wrap) { host.innerHTML = ""; wrap = h("div", "cxWrap", `<div class="cxSum" role="status"></div><div class="cxList"></div><div class="cxFoot"></div>`); host.appendChild(wrap); drawnKey = null; }
      const same = drawnKey === w.q; if (!same) limit = PAGE;
      const rows = list.filter(c => matches(c, w.q));
      if (w.focus) { const i = rows.findIndex(c => String(c.orderId) === w.focus); if (i >= limit) limit = Math.ceil((i + 1) / PAGE) * PAGE; }
      const shown = rows.slice(0, limit), listEl = wrap.querySelector(".cxList"), sum = wrap.querySelector(".cxSum"), foot = wrap.querySelector(".cxFoot");
      const total = Math.max(ids.size, list.length), etsyN = list.filter(isEtsy).length;
      sum.innerHTML = w.q ? `<b>${rows.length}</b> of ${list.length} match “${esc(w.q)}”`
        : `<b>${total}</b> cancelled order${total === 1 ? "" : "s"} · newest first${!more && list.length ? `<span class="cxLegend"><span><i class="cxDot etsy"></i>${etsyN} on Etsy</span><span><i class="cxDot person"></i>${list.length - etsyN} by a person</span></span>` : ""}`;
      // the rows: kept by order, rebuilt only when what they show changed
      const out = [];
      for (const c of shown) {
        const rid = String(c.orderId), openable = view || pulled.has(rid), stamp = JSON.stringify([c, openable]); let e = nodes.get(rid);
        if (!e || e.stamp !== stamp) {
          const node = h("div", "cxRow" + (openable ? " open" : "") + (isEtsy(c) ? " etsy" : ""), rowHtml(c, openable));
          node.dataset.rid = rid; node.dataset.mkey = "cx:" + rid;
          if (openable) { node.setAttribute("role", "button"); node.tabIndex = 0; node.title = `Open order ${rid}`; }
          wire(node, c);
          if (e && e.node.isConnected) e.node.replaceWith(node);
          e = { stamp, node }; nodes.set(rid, e);
        }
        out.push(e.node);
      }
      { const keep = new Set(list.map(c => String(c.orderId))); for (const rid of [...nodes.keys()]) if (!keep.has(rid)) nodes.delete(rid); }
      if (!rows.length) {
        const busy = reading || (w.focus && !recordOf(w.focus) && ids.has(w.focus));
        const empty = busy ? h("div", "cxWait", `<span class="owSpin"></span>Reading the cancelled orders…`)
          : w.q ? h("div", "libEmpty cxEmpty", `<span>No cancelled order matches “${esc(w.q)}”.</span>${w.clear ? `<button class="btn ghost sm" type="button" data-cx="all">Show every cancelled order</button>` : ""}`)
          : h("div", "libEmpty cxEmpty", `<span>No order has been cancelled.</span><small>An order Etsy cancels, or one a person cancels from a sheet or the On hold list, is kept here.</small>`);
        empty.dataset.mkey = "cxEmpty"; const all = empty.querySelector("[data-cx=all]"); if (all) all.onclick = () => w.clear();
        out.push(empty);
      }
      const at0 = host.scrollTop;
      if (window.Motion) Motion.reconcile(listEl, out, { animate: same, clip: host }); else { listEl.replaceChildren(...out); }
      host.scrollTop = at0;
      // a view drawn afresh (the pile opened, another search): its rows come in softly, one after another
      if (!same && !still()) out.slice(0, 14).forEach((n, i) => { n.classList.remove("cxIn"); void n.offsetWidth; n.style.setProperty("--i", i); n.classList.add("cxIn"); n.addEventListener("animationend", () => n.classList.remove("cxIn"), { once: true }); });
      drawnKey = w.q;
      for (const n of out) if (fresh.has(n.dataset.rid)) { const t = fresh.get(n.dataset.rid); fresh.delete(n.dataset.rid); if (Date.now() - t > 600000) continue; n.classList.remove("mFound"); void n.offsetWidth; n.classList.add("mFound"); setTimeout(() => n.classList.remove("mFound"), 2600); }
      // more: the rest of what was read, then older pages from the library, up to its newest 500
      foot.innerHTML = "";
      if (shown.length < rows.length) { const b = h("button", "btn ghost listMore", `Show more · ${shown.length} of ${rows.length}`); b.type = "button"; b.onclick = () => { limit += PAGE; paint(); }; foot.appendChild(b); }
      else if (more && asked < MAX) {
        const b = h("button", "btn ghost listMore", w.q ? `Search older cancelled orders` : `Show older cancelled orders`); b.type = "button";
        b.onclick = () => { b.disabled = true; b.innerHTML = `<span class="owSpin"></span>Reading older cancelled orders…`; limit = Math.max(limit, shown.length + PAGE); history(asked + PAGE).then(() => paint(), e => { b.disabled = false; b.textContent = "Try again"; toast("Older cancelled orders not read: " + e.message, "bad", 6000); }); };
        foot.appendChild(b);
      } else if (more) foot.appendChild(h("div", "cxMore", `The ${list.length} newest are listed. Search an order number to find an older one.`));
    }
    function openRow(node, c) {
      const rid = String(c.orderId);
      if (window.OrderWin && typeof OrderWin.openOrder === "function") { try { OrderWin.openOrder(rid, { from: node }); } catch (e) { toast(`Order ${rid} did not open: ${e.message}`, "bad", 6000); } return; }
      const r = window.Orders && Orders.rows().find(x => String(x.order.receiptId) === rid);
      if (r && window.OrderWin) OrderWin.open(r.key);
    }
    function wire(node, c) {
      node.onclick = e => { if (e.target.closest("button,a,input")) return; if (node.classList.contains("open")) openRow(node, c); };
      node.onkeydown = e => { if (e.target === node && (e.key === "Enter" || e.key === " ")) { e.preventDefault(); if (node.classList.contains("open")) openRow(node, c); } };
      const rb = node.querySelector("[data-cx=restore]"); if (rb) rb.onclick = e => { e.stopPropagation(); ask(node, c); };
    }
    /** Restore asks once, in the row itself (a bar laid over its right end, so nothing in the row moves): Restore again
     *  brings it back; Keep, Esc, a click elsewhere or 12 s leave it as it is. */
    function ask(node, c) {
      const rid = String(c.orderId); if (node.classList.contains("asking")) return;
      const bar = h("div", "cxAskBar", `<span class="cxAsk">Bring it back?</span><button class="btn sm" type="button" data-cx="yes" title="Restore order ${esc(rid)}: it returns with the next orders check if it is still open on Etsy">Restore</button><button class="btn ghost sm" type="button" data-cx="no">Keep</button>`);
      node.classList.add("asking"); node.appendChild(bar);
      animate(bar, [{ opacity: 0, transform: "translateX(10px)" }, { opacity: 1, transform: "none" }], 220);
      let t = 0;
      const back = () => { clearTimeout(t); document.removeEventListener("pointerdown", away, true); if (!node.classList.contains("asking")) return; node.classList.remove("asking"); bar.inert = true; for (const x of bar.querySelectorAll("[data-cx]")) x.removeAttribute("data-cx"); animate(bar, [{ opacity: 1 }, { opacity: 0, transform: "translateX(10px)" }], 160).then(() => bar.remove()); };
      const away = e => { if (!node.contains(e.target)) back(); };
      document.addEventListener("pointerdown", away, true); t = setTimeout(back, 12000);
      bar.querySelector("[data-cx=no]").onclick = e => { e.stopPropagation(); back(); node.focus && node.focus(); };
      bar.onclick = e => e.stopPropagation();
      bar.onkeydown = e => { if (e.key === "Escape") { e.preventDefault(); e.stopPropagation(); back(); } };
      const yes = bar.querySelector("[data-cx=yes]"); requestAnimationFrame(() => yes.focus());
      yes.onclick = e => { e.stopPropagation(); clearTimeout(t); document.removeEventListener("pointerdown", away, true); doRestore(node, c, yes, back); };
    }
    async function doRestore(row, c, b, back) {
      const rid = String(c.orderId), w = want;
      b.disabled = true; b.innerHTML = `<span class="spin"></span>Restoring`; row.querySelector("[data-cx=no]")?.remove();
      try {
        await restore(rid);
        nodes.delete(rid);
        agent({ bridge: true }, "DS", `Order ${rid} restored by ${whoAmI() || "someone"}: it comes back with the next orders check if it is still open on Etsy`);
        // where it goes, seen going (Paul, 27 Sep): a copy lifts off toward Open Orders, slowly enough to follow (~700 ms), the
        // rows below close up, and a note under Open Orders says when it shows there
        const text = `Order ${rid} will come back under Open Orders with the next orders check`;
        const to = () => document.querySelector('#ordChips [data-pile=""]'), M = Mo(), g = M && !still() ? M.ghost(row, null, null, row) : null;
        const said = () => { if (!(M && M.note(to, { text }))) toast(`Order ${rid} restored · it comes back with the next orders check`, "ok", 6000); };
        if (g) { closeGap(row); M.fly(g, to, { plus: false, ms: M.T.slide }).then(said); }
        else { said(); await animate(row, [{ opacity: 1, transform: "none" }, { opacity: 0, transform: "translateX(12px)" }], 700); closeGap(row); }
        if (w && w.onChange) w.onChange();
        setTimeout(paint, 800);   // (the count and an emptied list, once the rows have closed up)
      } catch (e) { toast("Not restored: " + e.message, "bad", 7000); back(); }
    }
    // (the Orders tab drawn before anything read the ids: they are read once behind it, so its count is never a 0 that is not true)
    let warmAt = 0;
    function count() {
      if (!at && !loading && Date.now() - warmAt > 60000) { warmAt = Date.now(); load().then(() => { if (ids.size && window.Orders) Orders.render(); }, () => {}); }
      return ids.size;
    }
    return { load, has, history, put, restore, renderInto, count, isEtsy, fatesOf, absorb, ids: () => [...ids] };
  })();
  // the order's record is kept first; only then does it leave every list, so an interruption leaves it on hold
  async function cancelRecord(rid, o, keepSt, goneSt, paint) {
    rid = String(rid);
    const rows = rowsOfOrder(rid), r0 = rows[0], ord = r0 ? r0.order : { receiptId: rid };
    keepSt.state = "now"; paint();
    const kept = await Cancelled.put({ orderId: rid, by: o.who, why: o.note || "", record: {
      buyer: (ord.buyer && ord.buyer.name) || ord.buyerName || ord.name || "", placedAt: r0 && window.CharmNestOrders ? CharmNestOrders.orderPlacedAt(r0) : 0, shipBy: ord.shipBy ? ord.shipBy * 1000 : 0,
      sheets: o.sheets ? String(o.sheets).split(", ") : [],
      lines: rows.map(r => ({ transactionId: String(r.line.transactionId || ""), sku: (r.spec && r.spec.designSku) || r.line.sku || "", title: r.line.title || "", quantity: (r.spec && r.spec.quantity) || r.line.quantity || 1, material: r.material || "" })) } });
    AutoCancel.mine(rid, kept && kept.record && kept.record.at);   // (what stays of it was said here: no second notice)
    if (o.fates && o.fates.length) api("charmNestLibrary", { op: "cancelFates", orderId: rid, fates: o.fates }, { quiet: true }).catch(e => console.warn("sheet window: cancel fates", e.message));
    keepSt.state = "ok"; goneSt.state = "now"; paint();
    // (the run is saved with the order gone, once more if the first save fails: a reload must not bring it back)
    await dropOrder(rid).catch(() => RunCtl.save(B.run)).catch(e => { console.warn("sheet window: run after cancel", e); toast(`Order ${rid} is cancelled; the run will save it with its next change (${e.message})`, "", 7000); });
    agent({ bridge: true }, "DS", `Order ${rid} cancelled by ${o.who}${o.note ? " (" + o.note + ")" : ""}: taken off every list; its record is kept under Orders › Cancelled`);
    goneSt.state = "ok"; paint();
  }
  // (all: its lines already marked gone go too, as when a cancel is found by AutoCancel; a committed line stays with its run)
  function dropOrder(rid, all) {
    rid = String(rid);
    const rows = all ? Orders.rows().filter(r => String(r.order.receiptId) === rid && r.state !== "committed") : rowsOfOrder(rid), keys = new Set(rows.map(r => r.key));
    for (const r of rows) { B.orders.byKey.delete(r.key); Engrave.items().delete(r.key); for (const id of r.poolIds || []) if (!Pool.sheetOf(id)) B.pool.rows.delete(id); }
    B.orders.rows = B.orders.rows.filter(r => !keys.has(r.key));
    if (B.review && Array.isArray(B.review.items)) B.review.items = B.review.items.filter(it => String(it.rid || "") !== rid && !keys.has(it.line) && !keys.has(it.jobKey) && !(it.row && keys.has(it.row.key)) && !(it.rows || []).some(r => keys.has(r.key)));
    const run = B.run;
    if (run) {
      run.lines = Object.fromEntries(Orders.rows().map(Orders.lineRecord));
      if (Array.isArray(run.orders)) run.orders = run.orders.filter(id => String(id) !== rid);
      if (run.holds && typeof run.holds === "object") delete run.holds[rid];
    }
    Review.syncOrderItems(); Orders.render(); Engrave.render(); Review.render();
    if (window.Session && Session.schedule) Session.schedule();
    return run ? RunCtl.save(run) : Promise.resolve();
  }
  async function cancelOnly(rid, opt, who) {
    if (!rid) return;
    const keep = stepOf("Keeping its record under Cancelled orders"), gone = stepOf("Taking it off every list");
    const work = beginFlow({ state: "working", title: `Cancelling order ${rid}`, steps: [keep, gone], note: "" }); if (!work) return;
    const cue = offCue(rid, "cancel");   // (its row on hold folds away; one whose pieces stay on this sheet stays)
    renderWork();
    if (W.view === "piece") showSheetPane(); else renderSheetPane();
    if (cue) cue.go();
    const paint = () => { if (W.dlg.open && W.work === work) renderWork(); };
    // a held order remembers the sheets it was taken off (its hold says so): the record keeps them
    const was = [...new Set(rowsOfOrder(rid).map(r => (/^Taken off (.+?) by /.exec(r.hold || "") || [])[1]).filter(n => n && n !== "its sheet"))].join(", ");
    try { await cancelRecord(rid, { note: opt.note, who, sheets: was }, keep, gone, paint); }
    catch (e) { for (const st of work.steps) doneStep(st); work.state = "failed"; work.title = "Not cancelled"; work.note = esc(e.message) + ". Nothing changed; try again."; if (cue) cue.fail(); paint(); endFlow(work); throw e; }
    work.state = "done"; work.title = `Order ${rid} cancelled`; work.note = `The record is under Orders › Cancelled, where it can be restored.`;
    if (cue) { cue.done(); cue.end(); }
    paint(); endFlow(work); if (W.dlg.open && W.rec) renderSheetPane();
  }

  /* ── cancelled elsewhere (Paul, 28 Sep, A2-A3): an order cancelled on Etsy (the receipts mirror writes its record) or at
     another station comes off every sheet of this sorter that is not cut yet, by itself. The newest cancel records are
     read every 2 minutes and at each orders check (cancelList: no Etsy call). For an order this sorter holds, its record
     is read once more just before anything moves (an order that is not cancelled is never touched); its lines are marked
     gone and leave Orders and the run, as a cancel made here does (dropOrder); its pieces come off each page still filling
     (Orders.takeOffGone: LiveNest.closed's rules, so never a cut, released, held, committed or Rose Gold cut sheet; a page
     nesting or saving is waited for); each page is nested again as a removal does (rewritePage), its QR label remade
     (remakeLabels) and what was saved read back (verifySaved). A piece already cut stays where it is, and a notice in the
     run pill says to set it aside until someone does. Each order is one job, written down before anything moves, so a
     reload in the middle finishes it; one tab works at a time (a lock), one order at a time. */
  const AutoCancel = window.AutoCancel = (() => {
    const KEY = () => "cn.autoCancel.v1:" + (typeof WORKSPACE_SANDBOX !== "undefined" && WORKSPACE_SANDBOX ? "sandbox" : "production");
    const EVERY = 120000, GONE = ["abandoned", "superseded"], queue = new Set(), noop = () => {};
    function read() {
      let s = null; try { s = JSON.parse(localStorage.getItem(KEY()) || "null"); } catch (_) {}
      s = s && typeof s === "object" ? s : {};
      return { known: Array.isArray(s.known) ? s.known : [], done: s.done || {}, jobs: s.jobs || {}, notices: Array.isArray(s.notices) ? s.notices : [] };
    }
    let st = read(), tick = 0, polling = null, working = null;
    function save() {
      for (const [k, d] of Object.entries(st.done)) if (Date.now() - (+d.t || 0) > 45 * 86400000) delete st.done[k];
      try { localStorage.setItem(KEY(), JSON.stringify(st)); } catch (_) {}
    }
    const joinAnd = l => l.length < 2 ? l.join("") : l.slice(0, -1).join(", ") + " and " + l[l.length - 1];
    const whereOf = sh => sheetWord(sh.sheetId, sh.fileBase, sh);
    const pageKey = sh => ({ id: sh.sheetId || null, metal: sh.metal, page: sh.page || 1, runId: sh.runId || null });
    const pageOf = k => allSheets().find(p => k.id ? p.sheetId === k.id : p.metal === k.metal && (p.page || 1) === k.page && (p.runId || null) === k.runId) || null;
    // the timeline (order-timeline.js when the page has it, else straight to the library; an id makes a repeat harmless)
    function tl(ev) {
      try {
        ev = Object.assign({ station: "sorter", at: Date.now() }, ev);
        if (window.CNTimeline && CNTimeline.rec(ev)) return;   // (the page's own queue: its store, passcode and employee)
        if (window.OrderTimeline) OrderTimeline.record(ev); else api("charmNestLibrary", { op: "timelineAdd", events: [ev] }, { quiet: true }).catch(() => {});
      } catch (_) { /* the timeline never stops a removal */ }
    }
    // where a removed order is seen going: Orders › Cancelled when it shows, else the Orders tab
    const target = () => { const p = document.querySelector('#ordChips [data-pile="cancelled"]'); return p && p.getClientRects().length ? p : document.querySelector('#modeSeg [data-mode="orders"]'); };
    const pillAnchor = () => { const b = document.getElementById("runBanner"); return b && !b.classList.contains("hidden") && b.getClientRects().length ? b : document.querySelector('#modeSeg [data-mode="orders"]'); };
    /** Every piece of the order this sorter knows of: its lines', those on its pages, and its piece records. */
    function idsOf(rid) {
      const ids = new Set();
      for (const r of Orders.rows()) if (String(r.order.receiptId) === rid) for (const id of r.poolIds || []) if (id) ids.add(id);
      for (const sh of allSheets()) for (const c of sh.charms) if (c.poolId && ridOf(c) === rid) ids.add(c.poolId);
      for (const p of B.pool.rows.values()) if (p && p.poolId && String(p.orderId || "") === rid && !GONE.includes(p.state)) ids.add(p.poolId);
      return ids;
    }
    /** The orders this sorter holds anything of, in one pass. */
    function held() {
      const out = new Set();
      for (const r of Orders.rows()) out.add(String(r.order.receiptId));
      for (const sh of allSheets()) for (const c of sh.charms) { const k = ridOf(c); if (k) out.add(k); }
      for (const p of B.pool.rows.values()) if (p && p.orderId && !GONE.includes(p.state)) out.add(String(p.orderId));
      return out;
    }
    /** Why a page holding the order's pieces is left as it is; null when it is still filling and they come off. */
    function leftWhy(sh, ids) {
      if (sh.roseCutAt || sh.laserDoneAt || sh.recalled || sentToStation(sh)) return { cut: true, why: "already cut" };
      if (sh.metal === "rose" && (sh.rosePlan || sh.roseProtected)) return { why: "inside its saved Rose Gold cut line" };
      if (window.Gate?.holding?.(sh)) return { wait: true };                  // being changed in the sheet window just now
      if (sh.releaseFull) return { why: "released for cutting" };
      if (sh.runHold || sh.intakeFinalized) return { why: "held for cutting" };
      if (window.LiveNest && LiveNest.closed(sh)) return { why: "closed to changes" };
      // all a saved sheet holds: its saved files would stay behind, so the sheet is deleted from its menu instead
      if (sh.sheetId && sh.charms.every(c => ids.has(c.poolId))) return { only: true, why: "the only order on it" };
      return null;
    }
    /** What becomes of the order here: lines to drop, pages it comes off, pages it stays on, pages to wait for, and
     *  pieces never placed. */
    function planOf(rid) {
      const ids = idsOf(rid), rows = Orders.rows().filter(r => String(r.order.receiptId) === rid && r.state !== "committed");
      const off = [], wait = [], left = new Map(), onPage = new Set(), loose = [];
      const stay = (where, n, w) => { const l = left.get(where); if (l) l.n += n; else left.set(where, Object.assign({ where, n }, w)); };
      for (const sh of allSheets()) {
        const mine = sh.charms.filter(c => ids.has(c.poolId)); if (!mine.length) continue;
        for (const c of mine) onPage.add(c.poolId);
        const w = leftWhy(sh, ids);
        if (w && w.wait || !w && busy(sh)) wait.push(sh); else if (w) stay(whereOf(sh), mine.length, w); else off.push(sh);
      }
      for (const id of ids) {
        if (onPage.has(id)) continue;
        const p = B.pool.rows.get(id);
        if (p && p.sheetId && !GONE.includes(p.state)) stay(sheetWord(p.sheetId, p.sheetName), 1, p.state === "committed" ? { cut: true, why: "already cut" } : { why: "not open in this sorter" });
        else if (!p || !GONE.includes(p.state)) loose.push(id);
      }
      return { rid, ids, rows, off, wait, left: [...left.values()], loose };
    }
    function noticeText(rid, left) {
      const cut = left.filter(l => l.cut), rest = left.filter(l => !l.cut), say = [];
      if (cut.length) say.push(`Its pieces are already cut on ${joinAnd(cut.map(l => l.where))}: set them aside.`);
      for (const l of rest) say.push(l.only ? `It is the only order on ${l.where}: delete that sheet from its menu, or set its pieces aside once cut.` : `Its pieces on ${l.where} stay (${l.why}): set them aside once cut.`);
      return `Order ${rid} was cancelled. ${say.join(" ")}`;
    }
    function notify(rid, at, left) {
      st = read();
      if (st.notices.some(n => n.rid === rid && +n.at === +at)) return;
      const text = noticeText(rid, left), where = joinAnd(left.map(l => l.where));
      st.notices = st.notices.filter(n => n.rid !== rid).concat([{ rid, at: +at || 0, text, where, t: Date.now() }]); save();
      tl({ orderId: rid, type: "note", by: "System", text: text.slice(0, 200), data: { cancelled: true, sheets: left.map(l => l.where), pieces: left.reduce((a, l) => a + l.n, 0) }, id: `autocancel-aside-${at}` });
      agent({ bridge: true }, "warn", text);
      RunCtl.renderBanner();
      // (the note goes when the pill's menu opens: the menu says the same, and the note would lie over it)
      setTimeout(() => {
        const menu = document.getElementById("runMenu"); if (!window.Motion || menu && menu.open) return;
        const n = Motion.note(pillAnchor, { text, ms: 16000, actions: [{ label: "Set aside", title: "the pieces are set aside: this notice goes", fn: () => ack(rid) }] });
        if (n && menu) { const off = () => { if (menu.open) n.close(); menu.removeEventListener("toggle", off); }; menu.addEventListener("toggle", off); }
      }, 80);
    }
    const pause = ms => new Promise(r => setTimeout(r, ms));
    /** One order, from its record read once more to its pages read back. "done", "restored" (not cancelled any more:
     *  nothing moved) or "later" (a page still busy, a save or a read-back not through: the next check carries on). */
    async function job(rid) {
      st = read();
      if (window.Recall?.on?.()) return "later";           // an earlier set on screen: its sheets and lines are history
      let plan = planOf(rid), rec = null, bar = null;
      const t0 = Date.now(), j0 = st.jobs[rid];
      const say = text => { if (!bar && window.CNProgress) bar = CNProgress.start(`Cancelled order ${rid}`); if (bar) bar.note(text); };
      try {
        // what is at work finishes first (a change by hand in the sheet window, new orders going on, a page nesting or
        // saving); then the record is read once more, and nothing waits between that answer and the pieces coming off
        for (;;) {
          const idle = !W.flow && !(B.run && B.run.arrivalBusy) && !plan.wait.length;
          if (idle) {
            const r = await api("charmNestLibrary", { op: "cancelCheck", orderIds: [rid] }, { quiet: true });
            rec = r && r.cancelled && r.cancelled[rid];
            if (!rec) {
              if (j0 || plan.rows.length) agent({ bridge: true }, "DS", `Order ${rid} is not cancelled any more: left as it is`);
              st = read(); delete st.jobs[rid]; save(); return "restored";
            }
            plan = planOf(rid);
            if (!W.flow && !(B.run && B.run.arrivalBusy) && !plan.wait.length) break;
          }
          if (Date.now() - t0 > 90000) return "later";
          say(plan.wait.length ? `waiting for ${whereOf(plan.wait[0])} to finish ${plan.wait[0].stage || plan.wait[0].status || "saving"}` : "waiting for the change in progress");
          await pause(500); plan = planOf(rid);
        }
        const etsy = rec.source === "etsy" || rec.by === "Etsy", by = etsy ? "Etsy" : String(rec.by || "someone");
        const reason = etsy ? "cancelled on Etsy" : `cancelled by ${by}`, at = +rec.at || 0;
        // the journal: written before anything moves, and each step as it is through (read afresh each time: a poll or a
        // Set aside meanwhile reads it too)
        const upd = f => { st = read(); const x = st.jobs[rid]; if (x) { f(x); save(); } };
        st = read();
        const j = st.jobs[rid] = Object.assign({ tries: 0, sheets: [], ids: [], runSaved: true, t: Date.now() }, st.jobs[rid], { at, by, reason });
        const offPages = new Set(plan.off);
        for (const sh of plan.off) if (!j.sheets.some(k => pageOf(k) === sh)) j.sheets.push(pageKey(sh));
        if (plan.rows.length) j.runSaved = false;
        // (one removal time for the whole job, kept across a reload: the server stamps the pieces' `removed` event under
        //  it, a retry's piece records say the same so nothing is stamped twice, and the event below lands on that one)
        if (!j.removedAt) j.removedAt = Date.now();
        save();
        // 1 · off, in this one task: the lines gone (nothing pools them meanwhile; they fly to Cancelled), the pieces off
        //     each page still filling (the rest of each sheet stays as placed), a released page kept in its set
        const patch = { removedBy: by, removedReason: "cancelled", removedAt: j.removedAt }, flights = [];
        for (const r of plan.rows) { if (r.state !== "gone") { r.state = "gone"; r.reason = reason; } if (window.Motion) Motion.expect("ord:" + r.key, { to: target, plus: false }); }
        if (plan.rows.length || plan.off.length || plan.loose.length) say(plan.off.length ? `taking it off ${joinAnd(plan.off.map(whereOf))}` : "taking it off Orders");
        const taking = plan.off.length ? Orders.takeOffGone([{ order: { receiptId: rid }, poolIds: [...plan.ids], reason }], { keep: sh => !offPages.has(sh), patch, why: reason,
          before: (sh, pieces) => { if (typeof liftFromSheet === "function") for (const c of pieces.slice(0, 8)) { const f = liftFromSheet(sh, c, target); if (f) flights.push(f); } },
          after: sh => holdRelease([sh]) }) : Promise.resolve([]);
        for (const f of flights) f();
        if (plan.rows.length) Orders.render();
        const taken = await taking;
        // pieces never placed (pooled, waiting) are let go the same way
        const loose = plan.loose;
        if (loose.length) { await Pool.update(loose, Object.assign({ state: "abandoned", sheetId: null, setId: null }, patch)); for (const id of loose) B.pool.rows.delete(id); }
        const offIds = new Set(taken.flatMap(t => t.poolIds).concat(loose));
        j.ids = [...new Set(j.ids.concat([...offIds]))].slice(0, 300); upd(x => { x.ids = j.ids; });
        if (offIds.size) await dropFromSets(offIds);
        // 2 · its lines leave Orders and the run, and the run is saved (once more if that fails: a reload must not bring
        //     them back; a save still failing is tried again at the next check)
        if (plan.rows.length || !j.runSaved) {
          await (plan.rows.length ? dropOrder(rid, true) : B.run ? RunCtl.save(B.run) : Promise.resolve()).catch(() => B.run && RunCtl.save(B.run));
          j.runSaved = true; upd(x => { x.runSaved = true; });
        }
        if (window.Session && Session.schedule) Session.schedule();
        // 3 · said as it happens: a note under Orders › Cancelled, the timeline, and what stays already cut
        if (taken.length || plan.rows.length) {
          const names = joinAnd(taken.map(t => whereOf(t.sh)));
          if (window.Motion) Motion.note(target, { text: `Order ${rid} ${reason} · ${names ? "taken off " + names : "moved to Cancelled"}`, actions: [{ label: "Show", title: "open Orders › Cancelled at this order", fn: () => { if (Orders.showCancelled) return Orders.showCancelled(rid); if (typeof setMode === "function") setMode("orders"); Orders.showPile("cancelled", rid); } }] });
          agent({ bridge: true }, "DS", `Order ${rid} ${reason}: ${names ? "taken off " + names + ", every other charm left where it is" : "taken off every list"}; its record is under Orders › Cancelled`);
        }
        // the pieces' `removed` event: the server stamps it from their records (poolUpdate, id = the removal time); this
        // one, under the same id, says it in full (every page it came off, and why), so the timeline shows it once
        if (taken.length) {
          const where = joinAnd(taken.map(t => whereOf(t.sh))), ids1 = taken.flatMap(t => t.poolIds);
          tl({ orderId: rid, type: "removed", by, at: j.removedAt, sheetId: taken[0].sh.sheetId || "", sheet: where.slice(0, 80), setId: taken[0].sh.setId || "", text: `Taken off ${where} · ${reason}`,
            data: { reason, sheets: taken.map(t => t.sh.sheetId || ""), sheetNames: taken.map(t => whereOf(t.sh)), pieces: ids1.length, copies: ids1.length, poolIds: ids1.slice(0, 40), auto: true }, id: String(j.removedAt) });
        }
        const d = read().done[rid];
        if (plan.left.length && !(d && +d.at === at) && +j.noticed !== at) { notify(rid, at, plan.left); j.noticed = at; upd(x => { x.noticed = at; }); }
        // what became of it, sheet by sheet, on its cancel record (Orders › Cancelled reads it); kept in the journal until
        // written, so a failed write goes again with the next try
        const fates = new Map((j.fates || []).map(f => [f.sheet, f]));
        for (const t of taken) fates.set(whereOf(t.sh), { sheet: whereOf(t.sh), fate: "removed", text: `taken off ${whereOf(t.sh)}` });
        for (const l of plan.left) if (!fates.has(l.where)) fates.set(l.where, { sheet: l.where, fate: "cut", text: l.cut ? `already cut on ${l.where}: set aside` : `stays on ${l.where} (${l.why}): set aside once cut` });
        if (fates.size !== (j.fates || []).length || taken.length) { j.fates = [...fates.values()]; j.fatesSaved = false; }
        if (j.fates && j.fates.length && !j.fatesSaved) {
          j.fatesSaved = await api("charmNestLibrary", { op: "cancelFates", orderId: rid, fates: j.fates }, { quiet: true }).then(() => true, () => false);
          upd(x => { x.fates = j.fates; x.fatesSaved = j.fatesSaved; });
        }
        // 4 · each page it came off nested again as it now stands, its QR label remade, and what was saved read back
        const pages = j.sheets.map(pageOf).filter(Boolean);
        let saving = false;
        for (const sh of pages) {
          if (!(sh.dirty || sh.status === "ready" || !sh.persistedDone || busy(sh))) continue;
          say(`nesting ${whereOf(sh)} again`);
          const s = stepOf(""); await rewritePage(sh, s, noop); if (s.state !== "ok") saving = true;
        }
        if (pages.length) { say("remaking the QR label"); await remakeLabels(pages, stepOf(""), noop); }
        letGo(pages);
        say("checking the saved sheets");
        const all = new Set(j.ids.concat([...plan.ids])), bad = [];
        for (const sh of pages) if (sh.sheetId && !(await verifySaved([{ sh, name: whereOf(sh), off: all }], all, null, stepOf(""), noop))) bad.push(sh);
        const poolOk = !j.ids.length || await verifySaved([], new Set(j.ids), p => GONE.includes(p.state), stepOf(""), noop);
        if (saving || bad.length || !poolOk) {
          // a page whose saved record still lists the order is nested again at the next try
          for (const sh of bad) if (!busy(sh) && !(window.LiveNest && LiveNest.closed(sh))) { sh.dirty = true; sh.status = "ready"; }
          st = read(); if (st.jobs[rid]) { st.jobs[rid].tries = (+st.jobs[rid].tries || 0) + 1; save(); }
          agent({ bridge: true }, "warn", `Cancelled order ${rid}: ${saving ? "a sheet is still saving" : bad.length ? joinAnd(bad.map(whereOf)) + " still lists it" : "its piece records do not agree yet"}; checked again at the next check`);
          return "later";
        }
        if (j.ids.length) await Pool.update(j.ids, { removedVerifiedAt: Date.now() }).catch(() => {});
        st = read(); st.done[rid] = { at, t: Date.now() }; delete st.jobs[rid]; save();
        if (window.RunCtl) RunCtl.poke();
        // the sheet window open on one of these sheets shows it as saved now, with the room freed
        if (W.dlg && W.dlg.open && !W.flow && pages.some(sh => sh.sheetId && sh.sheetId === W.id)) open2(W.id);
        return "done";
      } finally { if (bar) bar.end(); }
    }
    function kick() {
      if (working || !queue.size) return working || Promise.resolve();
      const go = async () => {
        for (const rid of [...queue]) {
          queue.delete(rid);
          try { await job(rid); } catch (e) { agent({ bridge: true }, "warn", `Cancelled order ${rid}: ${e.message}; tried again at the next check`); }
        }
      };
      // (another tab of this workspace at it already: that tab does it)
      working = (navigator.locks ? navigator.locks.request(KEY(), { ifAvailable: true }, lock => lock ? go() : queue.clear()) : go())
        .catch(() => {}).finally(() => { working = null; if (queue.size) setTimeout(kick, 1000); });
      return working;
    }
    /** The newest cancel records, read against what this sorter holds. Returns the orders queued. */
    function poll() {
      if (polling) return polling;
      polling = (async () => {
        if (typeof S === "undefined" || !S.cloud || S.cloud.ok !== true) return [];
        const r = await api("charmNestLibrary", { op: "cancelList", limit: 50 }, { quiet: true });
        const list = (r.list || []).filter(c => c && c.orderId).sort((a, b) => (+b.at || 0) - (+a.at || 0));
        const recs = new Map(list.map(c => [String(c.orderId), c]));
        st = read();
        const fresh = list.filter(c => !st.known.includes(String(c.orderId)));
        st.known = [...recs.keys()].concat(st.known.filter(id => !recs.has(id))).slice(0, 300); save();
        if (window.Cancelled && Cancelled.absorb([...recs.keys()])) Orders.render();   // (the pull rule leaves them out at once)
        const have = held(), due = [];
        for (const id of new Set([...Object.keys(st.jobs), ...recs.keys(), ...(window.Cancelled ? Cancelled.ids() : [])])) {
          if (st.jobs[id]) { due.push(id); continue; }
          if (!have.has(id)) continue;
          const p = planOf(id), d = st.done[id], rec = recs.get(id), noticed = !!d && (!rec || +d.at === +rec.at);
          if (p.rows.length || p.off.length || p.wait.length || p.loose.length || (p.left.length && !noticed)) due.push(id);
        }
        if (fresh.length) agent({ bridge: true }, "DS", `${fresh.length} new cancel record${fresh.length === 1 ? "" : "s"} (${fresh.slice(0, 5).map(c => c.orderId).join(", ")}${fresh.length > 5 ? "…" : ""})${due.length ? ` · ${due.length} held here, coming off` : ""}`);
        for (const id of due) queue.add(id);
        kick();
        return due;
      })().finally(() => { polling = null; });
      return polling;
    }
    function start() { if (tick) return; tick = setInterval(() => poll().catch(() => {}), EVERY); setTimeout(() => poll().catch(() => {}), 4000); }
    const notices = () => st.notices.slice();
    function pillHtml() {
      return st.notices.map(n => `<div class="rbCxItem" data-rid="${esc(n.rid)}"><span>${esc(n.text)}</span><button type="button" class="btn sm" data-cxack="${esc(n.rid)}" title="the pieces are set aside: this notice goes">${ICON.check}Set aside</button></div>`).join("");
    }
    /** Someone has set the pieces aside: the notice goes, and the timeline says who. */
    function ack(rid, node) {
      rid = String(rid);
      const go = () => {
        st = read(); const n = st.notices.find(x => x.rid === rid); if (!n) return RunCtl.renderBanner();
        st.notices = st.notices.filter(x => x.rid !== rid); save();
        for (const m of document.querySelectorAll(".mNote")) if (m.close && /^Order \d+ was cancelled/.test(m.textContent) && m.textContent.includes(rid)) m.close();
        const who = whoAmI() || "someone";
        tl({ orderId: rid, type: "note", by: who, text: `Pieces set aside by ${who} (${n.where})`.slice(0, 200), id: `autocancel-aside-ok-${n.at}` });
        RunCtl.renderBanner();
      };
      if (node && node.animate && !still()) node.animate([{ opacity: 1, transform: "none" }, { opacity: 0, transform: "translateX(12px)" }], { duration: 240, easing: "ease-in", fill: "forwards" }).finished.then(go, go); else go();
    }
    /** A cancel made in this window: done here already, so it is not taken up again. */
    function mine(rid, at) { st = read(); st.done[String(rid)] = { at: +at || 0, t: Date.now(), mine: true }; save(); }
    return { start, started: () => !!tick, poll, kick, idle: () => working || Promise.resolve(), notices, pillHtml, ack, mine, state: () => read() };
  })();

  /* ── the freed room: which orders fit it, found with the nest's own collision grid, oldest order first ── */
  // sheetId → the room pieces left on it; kept in this browser (per workspace) for 12 hours, so a reload keeps it
  const FREED = new Map(), FREED_KEY = () => "cn.sheetwin.freed" + (typeof WORKSPACE_SANDBOX !== "undefined" && WORKSPACE_SANDBOX ? ":sandbox" : "");
  // a freed spot keeps the charm's own outline (points to 0.01 pt), so after a reload it still reads as that charm's room
  const r2 = v => Math.round(v * 100) / 100;
  function shapeOf(c) {
    if (!c) return null; if (c.mini) return { centerPt: c.centerPt, outline: c.outline, members: [] };
    try { return { centerPt: c.centerPt, outline: { subpaths: c.outline.subpaths.map(sub => sub.map(s => [s[0], ...s.slice(1).map(q => [r2(q[0]), r2(q[1])])])) }, members: [] }; } catch (_) { return null; }
  }
  // the removed piece's bitmap (as the solver keeps it), so the spot can be tested for room again after a reload
  const BITMAPS = new WeakMap();
  function bitmapOf(c) {
    if (!c || !c.bits || !c.w || !c.h) return null;
    if (typeof c.bits === "string") return { bits: c.bits, w: c.w, h: c.h, scale: c.scale };
    if (BITMAPS.has(c)) return BITMAPS.get(c);
    const n = c.w * c.h, by = new Uint8Array((n + 7) >> 3); for (let i = 0; i < n; i++) if (c.bits[i]) by[i >> 3] |= 1 << (i & 7);
    let t = ""; for (let i = 0; i < by.length; i += 8192) t += String.fromCharCode.apply(null, by.subarray(i, i + 8192));
    const out = { bits: btoa(t), w: c.w, h: c.h, scale: c.scale }; BITMAPS.set(c, out); return out;
  }
  function setFreed(id, list) {
    if (list && list.length) FREED.set(id, list); else FREED.delete(id);
    // the bitmaps are kept up to about 1 MB in all; a spot without one is judged by distance after a reload
    let room = 1e6;
    const bm = c => { const b = bitmapOf(c); if (!b || b.bits.length > room) return null; room -= b.bits.length; return b; };
    try { const o = {}; for (const [k, gs] of FREED) o[k] = gs.map(g => ({ p: g.x.p, id: g.x.id || "", rid: g.rid || "", sku: g.sku || "", at: g.at || Date.now(), s: shapeOf(g.x.c), b: bm(g.x.c) })); localStorage.setItem(FREED_KEY(), JSON.stringify(o)); } catch (_) {}
  }
  try {
    const o = JSON.parse(localStorage.getItem(FREED_KEY()) || "{}");
    for (const [k, gs] of Object.entries(o)) { const list = (gs || []).filter(g => g && g.p && Date.now() - (+g.at || 0) < 12 * 3600e3).map(g => ({ x: { id: g.id || undefined, p: g.p, c: g.s && g.s.outline ? Object.assign({}, g.s, { mini: true }, g.b && g.b.bits ? g.b : null) : null, gone: true }, rid: g.rid, sku: g.sku, at: +g.at, t0: 0 })); if (list.length) FREED.set(k, list); }
  } catch (_) {}
  const SV = () => window.CharmNestSolver;
  const bitsOf = c => typeof c.bits === "string" ? SV().bitsFromBase64(c.bits, c.w * c.h) : c.bits;
  const VARS = new WeakMap();
  function variantOf(c, angle, clearancePt) {
    let m = VARS.get(c); if (!m) VARS.set(c, m = new Map());
    const k = angle + "@" + clearancePt;
    if (!m.has(k)) m.set(k, SV().prepareVariant({ bits: bitsOf(c), w: c.w, h: c.h, scale: c.scale }, angle, clearancePt, 2));
    return m.get(k);
  }
  // the sheet this window can fill: held in this sorter, not Rose Gold (its green line decides), not cut or sent
  function fillTarget() {
    const sh = allSheets().find(p => p.sheetId === W.id);
    if (!sh || !sh.placements.length || sh.metal === "rose" || sh.roseCutAt || sh.recalled || sh.laserDoneAt || sentToStation(sh) || !SV() || !SV().makeSheetGrid) return null;
    return sh;
  }
  // an order comes from a sheet still filling in the Nest tab, never from one in a set, released, cut or sent
  function movableFrom(src, target) {
    if (src === target || src.metal !== target.metal || src.roseCutAt || src.recalled || src.laserDoneAt || sentToStation(src)) return false;
    if (window.LiveNest && LiveNest.closed(src)) return false;
    if (window.Gate && Gate.modern(src.runId) && src.setId && !src.draft) return false;
    return true;
  }
  function candidatesFor(target) {
    const by = new Map();
    for (const src of allSheets()) {
      if (!movableFrom(src, target)) continue;
      const placed = new Set(src.placements.map(p => p.id));
      for (const c of activeCharms(src)) {
        const rid = ridOf(c); if (!/^\d+$/.test(rid) || !c.poolId || !c.bits || !c.w) continue;
        let k = by.get(rid); if (!k) by.set(rid, k = { rid, pieces: [], srcs: new Set() });
        k.pieces.push({ c, src, placed: placed.has(c.id) }); k.srcs.add(src);
      }
    }
    const rows = new Map(); for (const r of window.Orders ? Orders.rows() : []) { const k = String(r.order.receiptId); if (!rows.has(k)) rows.set(k, []); rows.get(k).push(r); }
    const out = [];
    for (const k of by.values()) {
      const rs = rows.get(k.rid) || [];
      if (rs.some(r => r.hold || ["held", "skipped", "gone"].includes(r.state)) || Cancelled.has(k.rid)) continue;
      // a saved sheet is never emptied by a move: its record would still name the pieces
      if ([...k.srcs].some(src => src.sheetId && src.placements.length && src.placements.every(p => k.pieces.some(z => z.src === src && z.c.id === p.id)))) continue;
      const at = rs.map(r => window.CharmNestOrders ? CharmNestOrders.orderPlacedAt(r) : 0).concat(k.pieces.map(z => +z.c.orderDate > 1e11 ? +z.c.orderDate : 0)).filter(Boolean);
      k.at = at.length ? Math.min(...at) : 0;
      k.row = rs[0] || null; k.area = k.pieces.reduce((n, z) => n + (+z.c.areaPt2 || 0), 0);
      k.waiting = k.pieces.every(z => !z.placed);
      out.push(k);
    }
    return out.sort((a, b) => (a.at || 9e15) - (b.at || 9e15) || a.rid.localeCompare(b.rid));
  }
  function plateGrid(target) {
    const job = buildJob(target), byId = new Map(target.charms.map(c => [c.id, c]));
    const fixed = []; for (const p of target.placements) { const c = byId.get(p.id); if (c && c.bits) fixed.push({ piece: { bits: bitsOf(c), w: c.w, h: c.h, scale: c.scale }, placement: p }); }
    const grid = SV().makeSheetGrid(Object.assign({}, job.sheet, { fixedPieces: fixed }), job.clearancePt, 2);
    const angles = [...new Set((job.angles && job.angles.length && job.angles.length <= 36 ? job.angles : Array.from({ length: 36 }, (_, i) => i * 10)).map(a => ((a % 360) + 360) % 360))];
    return { grid, clearancePt: job.clearancePt, angles, key: target.placements.map(p => p.id + p.cxPt + p.cyPt + p.angle).join() };
  }
  function roomBoxes(ghosts) {
    return ghosts.map((g, i) => { const p = g.x.p, hw = (p.wPt || 20) / 2 + 1.5, hh = (p.hPt || 20) / 2 + 1.5; return { i, cx: p.cxPt, cy: p.cyPt, x0: p.cxPt - hw, x1: p.cxPt + hw, y0: p.cyPt - hh, y1: p.cyPt + hh }; });
  }
  // how much of a freed spot is taken again: the piece that left it, put back at its own spot on the sheet as it stands
  // (0 while the room is free, near 1 once a charm sits in it); null when its shape is not known here or it has not left yet
  const TAKEN = 0.15;
  function takenOf(g, G, target) {
    const c = g.x.c, p = g.x.p; if (!G || !c || !c.bits || !c.w || !c.h) return null;
    if (g.x.id && target && target.placements.some(q => q.id === g.x.id)) return null;
    let v = null; try { v = variantOf(c, +p.angle || 0, G.clearancePt); } catch (_) {}
    if (!v || !v.fine.pm.cells) return null;
    return G.grid.overlap(v.fine.pm, Math.round(p.cxPt * 2 - v.solid.cx), Math.round(p.cyPt * 2 - v.solid.cy), 1e9) / v.fine.pm.cells;
  }
  // a spot is taken once something covers enough of it; without a shape to test, once a charm sits near its middle
  function freeStill(g, G, target, near) {
    const f = takenOf(g, G, target);
    return f == null ? !near(g) : f < TAKEN;
  }
  // every piece of the order placed in the room, one after another, each at the tightest legal spot (most contact with
  // its neighbours, nearest the freed outline), exactly as the nest would test it
  async function fitOrder(k, G, boxes, breathe) {
    const grid = G.grid.clone(), res = 2, ringR = 4, spots = [], Sv = SV();
    for (const z of k.pieces.slice().sort((a, b) => (+b.c.areaPt2 || 0) - (+a.c.areaPt2 || 0))) {
      let best = null;
      const score = (v, x, y, b) => {
        if (!grid.fits(v.fine.pm, x, y)) return null;
        const contact = grid.overlap(v._ring, x - ringR, y - ringR, 1e9) / Math.max(1, v._ring.cells);
        return contact - 0.004 * Math.hypot((x + v.solid.cx) / res - b.cx, (y + v.solid.cy) / res - b.cy);
      };
      for (const a of G.angles) {
        const v = variantOf(z.c, a, G.clearancePt); if (!v) continue;
        if (!v._ring) { const rg = Sv.ring(v.fine.bits, v.fine.w, v.fine.h, ringR); v._ring = Sv.packShifted(rg.bits, rg.w, rg.h); }
        for (const b of boxes) {
          const X0 = Math.round(b.x0 * res - v.solid.cx), X1 = Math.round(b.x1 * res - v.solid.cx), Y0 = Math.round(b.y0 * res - v.solid.cy), Y1 = Math.round(b.y1 * res - v.solid.cy);
          for (let y = Y0; y <= Y1; y += 2) for (let x = X0; x <= X1; x += 2) { const s = score(v, x, y, b); if (s != null && (!best || s > best.s)) best = { v, x, y, s, b }; }
        }
        await breathe();
      }
      if (!best) return null;
      for (let dy = -1; dy <= 1; dy++) for (let dx = -1; dx <= 1; dx++) { const s = score(best.v, best.x + dx, best.y + dy, best.b); if (s != null && s > best.s) best = Object.assign({}, best, { x: best.x + dx, y: best.y + dy, s }); }
      grid.stamp(best.v.fine.bits, best.v.fine.w, best.v.fine.h, best.x, best.y);
      spots.push({ c: z.c, src: z.src, placed: z.placed, cxPt: (best.x + best.v.solid.cx) / res, cyPt: (best.y + best.v.solid.cy) / res, angle: best.v.angle, bi: best.b.i });
    }
    return { spots, grid };
  }
  // the whole freed room is planned at once: the waiting orders, oldest first, each tried only in the spots still empty
  // once the ones before it are in, so every freed spot gets its own order and one press can place them all. The search
  // runs in slices, so the window stays smooth; a newer search or another sheet ends it
  async function suggest() {
    const target = fillTarget(), all = (FREED.get(W.id) || []);
    if (!target || !all.length || W.flow) { W.fill = null; renderFill(); return; }
    const tok = W.token, run = W.fillRun = (W.fillRun || 0) + 1;
    const live = () => tok === W.token && run === W.fillRun && W.dlg.open;
    const cands = candidatesFor(target);
    W.fill = { state: "looking", tried: 0, total: cands.length, list: [], spots: all.length, filled: 0 }; renderFill();
    if (!cands.length) { W.fill.state = "none"; renderFill(); return; }
    let G; try { G = plateGrid(target); } catch (e) { console.warn("sheet window: fill grid", e); W.fill = null; renderFill(); return; }
    const ghosts = all.filter(g => { const f = takenOf(g, G, target); return f == null || f < TAKEN; }), boxes = roomBoxes(ghosts);
    W.fill.spots = ghosts.length;
    if (!ghosts.length) { W.fill.state = "none"; renderFill(); return; }
    let last = performance.now();
    const breathe = async () => { if (performance.now() - last > 12) { await pause(0); last = performance.now(); } if (!live()) throw new Error("stale"); };
    const area = b => (b.x1 - b.x0) * (b.y1 - b.y0), t0 = performance.now();
    const open = new Set(boxes.map(b => b.i)), planned = new Set();
    let grid = G.grid;
    try {
      for (const k of cands) {
        if (!open.size || W.fill.list.length >= 8 || performance.now() - t0 > 12000) break;
        W.fill.tried++;
        const room = boxes.filter(b => open.has(b.i));
        if (k.area > room.reduce((n, b) => n + area(b), 0) * 1.6) continue;
        // a saved sheet is never emptied by a move, counting the orders planned before this one
        if ([...k.srcs].some(src => src.sheetId && src.placements.length && src.placements.every(p => planned.has(p.id) || k.pieces.some(z => z.src === src && z.c.id === p.id)))) continue;
        const got = await fitOrder(k, Object.assign({}, G, { grid }), room, breathe);
        if (!got) { if (W.fill.tried % 3 === 0) renderFill(); continue; }
        grid = got.grid; for (const s of got.spots) planned.add(s.c.id);
        const Gn = Object.assign({}, G, { grid });
        for (const b of room) { const f = takenOf(ghosts[b.i], Gn, null); if (f == null ? got.spots.some(s => s.bi === b.i) : f >= TAKEN) open.delete(b.i); }
        W.fill.list.push({ k, spots: got.spots }); W.fill.filled = ghosts.length - open.size; renderFill();
      }
    } catch (e) { if (e.message === "stale") return; console.warn("sheet window: fill search", e); }
    if (!live()) return;
    W.fill.state = W.fill.list.length ? "ready" : "none"; W.fill.key = G.key; renderFill();
  }
  // the planned orders go in one after another, each its own verified move; one that does not go through stops the rest
  // (they stay where they are, and what is still free is planned again)
  async function placeAll(list, target, who) {
    let i = 0;
    try {
      for (; i < list.length; i++) {
        if (!allSheets().includes(target) || target.recalled || target.laserDoneAt || target.roseCutAt || sentToStation(target)) return;
        let ok = false;
        try { ok = await moveIn(list[i].k, list[i].spots, target, who, { part: [i + 1, list.length] }); }
        catch (e) { console.error("sheet window: move", e); toast("Not moved: " + e.message, "bad", 8000); return; }
        if (!ok) return;
      }
    } finally { for (const s of list.slice(i)) unland(s.k.rid); }   // (the orders that did not go are not drawn as landed)
  }
  function renderFill() { steady(W.el.fill, drawFill); }
  function drawFill() {
    const E = W.el, f = W.fill; if (!E.fill) return;
    if (W.add && !W.flow) return renderAdd();
    if (!f || W.flow) { E.fill.hidden = true; E.fill.innerHTML = ""; setGhost(null); return; }
    E.fill.hidden = false;
    const target = fillTarget();
    // several orders that fit together: one press places them all (in the header, so it is never scrolled away)
    const every = f.state === "ready" && f.list.length > 1;
    const allBtn = every ? `<button type="button" class="btn sage xs" data-fl="all" title="Place the ${f.list.length} orders below in the spots shown, one after another (${f.filled} of ${f.spots} freed spots)">Place all ${f.list.length}</button>` : "";
    const head = `<h4>${ICON.fill}<span>Fill the freed room</span>${every ? "" : `<small>${f.state === "looking" ? "" : f.list.length ? "fits, oldest order first" : ""}</small>`}${allBtn}<button type="button" class="swIcon" data-fl="x" title="Hide suggestions" aria-label="Hide suggestions">${ICON.close}</button></h4>`;
    const rows = f.list.map((s, i) => `<li data-i="${i}" data-rid="${esc(s.k.rid)}" style="--i:${i}">${candRow(s.k)}<span class="a">${HAND_BTN}<button type="button" class="btn sage xs" data-fl="place" title="Place it in the spot shown">Place</button></span></li>`).join("");
    const looking = f.state === "looking" ? `<div class="swWait"><span class="owSpin"></span>Trying the waiting orders in the room · ${f.tried} of ${f.total}</div>` : "";
    const none = f.state === "none" ? `<div class="note">No waiting order fits this room yet.${target && (target.draft || !target.setId) ? " New orders that fit go into it as they arrive." : ""}</div>` : "";
    const rest = f.state === "ready" && f.filled < f.spots ? `<div class="note">${f.spots - f.filled === 1 ? "1 freed spot has" : f.spots - f.filled + " freed spots have"} no waiting order that fits yet.${target && (target.draft || !target.setId) ? " New orders that fit go into " + (f.spots - f.filled === 1 ? "it" : "them") + " as they arrive." : ""}</div>` : "";
    E.fill.innerHTML = `<div class="swFill">${head}${rows ? `<ol>${rows}</ol>` : ""}${looking}${rest}${none}</div>`;
    E.fill.querySelector("[data-fl=x]").onclick = () => { setFreed(W.id, []); W.freed = []; W.fill = null; W.fillRun = (W.fillRun || 0) + 1; renderFill(); renderStrip(); renderSheetChips(); paintFx(); };
    const allB = E.fill.querySelector("[data-fl=all]");
    if (allB) {
      const every = { spots: f.list.flatMap(s => s.spots) }, list = f.list.slice();
      allB.onmouseenter = allB.onfocus = () => setGhost(every); allB.onmouseleave = allB.onblur = () => setGhost(null);
      allB.onclick = () => {
        const who = needName(() => { if (allB.isConnected) allB.click(); }); if (!who || !target) return;
        setGhost(null);
        // each row flies onto the plate where its order lands, one after another (Paul, 27 Sep)
        if (!W.flow) list.forEach((s, i) => landRow(E.fill.querySelector(`li[data-i="${f.list.indexOf(s)}"]`), s.spots, s.k.rid, { draw: true, delay: i * 180 }));
        placeAll(list, target, who);
      };
    }
    E.fill.querySelectorAll("li[data-i]").forEach(li => {
      const s = f.list[+li.dataset.i];
      drawThumb(li.querySelector("canvas"), { c: s.spots[0].c });
      li.onmouseenter = () => setGhost(s); li.onmouseleave = () => setGhost(null);
      li.querySelector("[data-fl=hand]").onclick = () => { setGhost(null); startHand(s.k, s.spots); };
      li.querySelector("[data-fl=place]").onclick = () => {
        const who = needName(() => { if (li.isConnected) li.querySelector("[data-fl=place]").click(); }); if (!who || !target) return;
        setGhost(null);
        if (!W.flow) landRow(li, s.spots, s.k.rid, { draw: true });
        moveIn(s.k, s.spots, target, who).catch(e => { console.error("sheet window: move", e); toast("Not moved: " + e.message, "bad", 8000); });
      };
    });
  }
  // the order drawn where it would go, in sage, before anything moves
  function setGhost(s) {
    if ((W.ghost && W.ghost.s) === s) return;
    W.ghost = s ? { s, spots: s.spots, t0: performance.now() } : null;
    if (s && !still()) { W.fx.push({ kind: "ghost", t0: W.ghost.t0, ms: 240 }); fxLoop(); } else paintFx();
  }
  function paintGhost(ctx, now) {
    const g = W.ghost; if (!g || !W.geom) return;
    const t = still() ? 1 : Math.min(1, ((now || performance.now()) - g.t0) / 240), k = W.k;
    for (const s of g.spots) {
      const x = { p: { cxPt: s.cxPt, cyPt: s.cyPt, angle: s.angle, scale: 1 }, c: s.c };
      withPiece(ctx, x, () => {
        ctx.globalAlpha = t; const sc = .92 + .08 * t; ctx.scale(sc, sc);
        ctx.fillStyle = "rgba(95,122,91,.24)"; outlinePath(ctx, x, true); ctx.fill("evenodd");
        CharmNestPDF.drawCharm(ctx, x.c, tx0(x.c), k);
        outlinePath(ctx, x); ctx.strokeStyle = "#5f7a5b"; ctx.lineWidth = 1.8 * W.dpr / sc; ctx.stroke();
        ctx.globalAlpha = 1;
      });
    }
  }

  // one waiting or filling order as a row: thumbnail, number, what it is, how old and where it is now
  function candRow(k) {
    const skus = new Map(); for (const z of k.pieces) { const n = z.c.sku || (z.c.orderInfo && z.c.orderInfo.sku) || z.c.name || ""; skus.set(n, (skus.get(n) || 0) + 1); }
    const src = [...k.srcs][0], from = k.waiting ? `waiting on ${sheetWord(src.sheetId, src.fileBase, src)}` : `on ${sheetWord(src.sheetId, src.fileBase, src)}${src.draft || !src.setId ? " · filling" : ""}`;
    return `<canvas class="th" width="88" height="88"></canvas><span class="t"><b>${esc(k.rid)}</b><small>${esc([...skus].map(([n, c]) => n + (c > 1 ? " ×" + c : "")).join(", "))}</small><small>${esc([k.at ? "ordered " + ago(k.at) : "", from].filter(Boolean).join(" · "))}</small></span>`;
  }
  const HAND_BTN = `<button type="button" class="swIcon" data-fl="hand" title="Place it by hand: move it on the sheet, scroll to turn, click to drop" aria-label="Place it by hand">${ICON.hand}</button>`;

  /* ── adding an order by hand: any order still waiting or filling on another sheet of this metal. Place lets the nest
     put it where its own grading says; the hand button lets the operator move and turn it into a spot. Either way it
     moves exactly as a suggestion does (saved off its sheet first, read back, stamped). Size never changes: a charm is
     cut at its product's size. ── */
  // the + shows only when some order could come onto this sheet
  function canAdd() { const t = fillTarget(); return !!t && candidatesFor(t).length > 0; }
  function openAdd(q) {
    if (W.flow) return toast("One change at a time: wait for the one in progress", "", 3500);
    const target = fillTarget(); if (!target) return;
    stopHand(true); setGhost(null);
    W.add = { q: q || "", all: candidatesFor(target), busy: null, miss: null };
    W.el.fill.innerHTML = ""; renderFill();
    const inp = W.el.fill.querySelector("[data-fl=q]"); if (inp) { inp.focus(); inp.select(); }
  }
  function closeAdd() { stopHand(true); W.add = null; renderFill(); }
  function renderAdd() {
    const E = W.el, a = W.add, target = fillTarget();
    if (!target) { W.add = null; return renderFill(); }
    E.fill.hidden = false; setGhost(null);
    if (!E.fill.querySelector(".swAdd")) {
      E.fill.innerHTML = `<div class="swFill swAdd"><h4>${ICON.add}<span>Add an order to this sheet</span><small>oldest first</small><button type="button" class="swIcon" data-fl="x" title="Close" aria-label="Close">${ICON.close}</button></h4><label class="swFind">${ICON.search}<input data-fl="q" type="search" placeholder="Order number or SKU" autocomplete="off" spellcheck="false"></label><div class="lst"></div></div>`;
      const inp = E.fill.querySelector("[data-fl=q]"); inp.value = a.q;
      inp.oninput = () => { a.q = inp.value.trim(); a.miss = null; renderAddList(); };
      inp.onkeydown = e => { if (e.key === "Enter") { e.preventDefault(); if (a.busy || W.flow) return; const b = E.fill.querySelector(".lst li:first-child [data-fl=place]:not(:disabled)"); if (b) b.click(); } else if (e.key === "Escape" && inp.value && !W.hand) { e.preventDefault(); inp.value = ""; a.q = ""; a.miss = null; renderAddList(); } };
      E.fill.querySelector("[data-fl=x]").onclick = closeAdd;
    }
    renderAddList();
  }
  function addMatches() {
    const a = W.add, q = a.q.toLowerCase();
    return a.all.filter(k => !q || k.rid.includes(q) || k.pieces.some(z => String(z.c.sku || (z.c.orderInfo && z.c.orderInfo.sku) || "").toLowerCase().includes(q)));
  }
  function renderAddList() {
    const E = W.el, a = W.add, host = E.fill.querySelector(".swAdd .lst"), target = fillTarget(); if (!host || !a || !target) return;
    const all = addMatches(), list = all.slice(0, 6), handRid = W.hand && W.hand.k.rid, lock = a.busy || W.flow ? " disabled" : "";
    const rows = list.map((k, i) => {
      const acts = a.busy === k.rid ? `<span class="swWait"><span class="owSpin"></span>Finding room</span>`
        : handRid === k.rid ? `<span class="swWait">${ICON.hand}On the sheet</span>`
        : (a.miss === k.rid ? `<span class="miss" title="The nest found no spot for every piece of it">No room</span>` : "") + HAND_BTN.replace("<button", "<button" + lock) + (a.miss === k.rid ? "" : `<button type="button" class="btn sage xs" data-fl="place"${lock} title="The nest places it in the best free spot">Place</button>`);
      return `<li data-rid="${esc(k.rid)}" style="--i:${i}"${handRid === k.rid ? ' class="on"' : ""}>${candRow(k)}<span class="a">${acts}</span></li>`;
    }).join("");
    const more = all.length > list.length ? `<div class="note">${all.length - list.length} more · type an order number or SKU</div>` : "";
    const none = !all.length ? `<div class="note">${a.q ? "No waiting order matches." : `No order is waiting or filling on another ${esc(CODE[target.metal] || "")} sheet.`}</div>` : "";
    host.innerHTML = (rows ? `<ol>${rows}</ol>` : "") + more + none;
    host.querySelectorAll("li[data-rid]").forEach(li => {
      const k = list.find(z => z.rid === li.dataset.rid); if (!k) return;
      drawThumb(li.querySelector("canvas"), { c: k.pieces[0].c });
      const hb = li.querySelector("[data-fl=hand]"), pb = li.querySelector("[data-fl=place]");
      if (hb) hb.onclick = () => startHand(k);
      if (pb) pb.onclick = () => autoPlace(k);
    });
  }
  // proof that every piece of the order has a legal spot, placed one after another as tight as the solver's own
  // measurement finds (each angle in its own slice, so the window stays smooth)
  async function roomFor(k, G, breathe) {
    const Sv = SV(), grid = G.grid.clone(), spots = []; grid.fineRes = 2;
    for (const z of k.pieces.slice().sort((a, b) => (+b.c.areaPt2 || 0) - (+a.c.areaPt2 || 0))) {
      let best = null;
      for (const ang of G.angles) {
        const v = variantOf(z.c, ang, G.clearancePt); if (!v) continue;
        const r = Sv.bestSpots(grid, [v], 1)[0];
        if (r && (!best || (r.contact || 0) > (best.r.contact || 0) + 1e-6 || (Math.abs((r.contact || 0) - (best.r.contact || 0)) <= 1e-6 && r.x < best.r.x))) best = { r, v };
        if (breathe) await breathe();
      }
      if (!best) return null;
      Sv.stampVariant(grid, best.v, best.r.x, best.r.y);
      spots.push({ c: z.c, src: z.src, placed: z.placed, cxPt: best.r.cxPt, cyPt: best.r.cyPt, angle: best.v.angle });
    }
    return spots;
  }
  async function autoPlace(k) {
    const target = fillTarget(), a = W.add; if (!target || !a || a.busy || W.flow) return;
    const who = needName(() => autoPlace(k)); if (!who) return;
    stopHand(true); a.busy = k.rid; a.miss = null; renderAddList();
    const tok = W.token; let spots = null, last = performance.now();
    try { spots = await roomFor(k, plateGrid(target), async () => { if (performance.now() - last > 12) { await pause(0); last = performance.now(); } }); }
    catch (e) { console.warn("sheet window: room search", e); }
    if (tok !== W.token || W.add !== a) return;
    a.busy = null;
    if (!spots) { a.miss = k.rid; renderAddList(); return; }
    W.add = null;
    // its row flies onto the plate where there is room (the nest picks the exact spot as it writes the sheet)
    if (!W.flow) landRow(W.el.fill.querySelector(`.swAdd li[data-rid="${CSS.escape(k.rid)}"]`), spots, k.rid, {});
    moveIn(k, spots, target, who, { free: true }).catch(e => { console.error("sheet window: add", e); toast("Not added: " + e.message, "bad", 8000); });
  }

  /* ── by hand: the piece follows the pointer and snaps to the nearest legal spot within 3 pt (sage); where it cannot
     go it shows in clay. Scroll or [ ] turns it 10° (Shift: 2°), click or Enter drops it, Backspace takes the last one
     back, Esc stops. Nothing moves until the last piece of the order is down. ── */
  function startHand(k, near) {
    const target = fillTarget(); if (!target || W.flow) return;
    let G; try { G = plateGrid(target); } catch (e) { return toast("This sheet cannot be edited by hand: " + e.message, "bad", 6000); }
    const pieces = k.pieces.slice().sort((a, b) => (+b.c.areaPt2 || 0) - (+a.c.areaPt2 || 0));
    const g0 = (FREED.get(W.id) || [])[0], first = near && near.find(s => s.c === pieces[0].c);
    const at = first ? { x: first.cxPt, y: first.cyPt } : g0 ? { x: g0.x.p.cxPt, y: g0.x.p.cyPt } : { x: W.st.wPt / 2, y: W.st.hPt / 2 };
    const grid = G.grid.clone(); grid.fineRes = 2;
    W.hand = { k, target, G, grid, pieces, i: 0, a: first ? first.angle : 0, spots: [], at, pos: null, acc: 0, msg: "" };
    setHover(null); tip(null); W.el.plate.classList.add("hand");
    // the keys (turn, drop, undo, Esc) go to the sheet, not to a search field
    W.dlg.querySelector(".swBox").focus({ preventScroll: true });
    handPos(); renderHand(); paintFx();
    if (W.add) renderAddList();
  }
  function stopHand(quiet) {
    if (!W.hand) return;
    W.hand = null; clearTimeout(W.handMsgT);
    if (W.el.plate) W.el.plate.classList.remove("hand");
    renderHand(); if (!quiet) { paintFx(); if (W.add) renderAddList(); }
  }
  function handPiece() { const h = W.hand; return h && h.pieces[h.i]; }
  function handPos() {
    const h = W.hand, z = handPiece(); if (!h || !z || !h.at) return;
    const v = variantOf(z.c, h.a, h.G.clearancePt); if (!v) { h.pos = null; return; }
    const r = SV().tryPlaceTight(h.grid, v, h.at.x, h.at.y, 3);
    h.pos = r.ok ? { v, x: r.x, y: r.y, cxPt: (r.x + v.solid.cx) / 2, cyPt: (r.y + v.solid.cy) / 2, ok: true } : { v, cxPt: h.at.x, cyPt: h.at.y, ok: false };
  }
  function handMove(e) {
    const h = W.hand, p = toPlate(e); h.at = { x: Math.max(0, Math.min(W.st.wPt, p.x)), y: Math.max(0, Math.min(W.st.hPt, p.y)) };
    handPos(); paintFx();
  }
  function handTurn(d) {
    const h = W.hand; if (!h) return;
    h.a = (((h.a + d) % 360) + 360) % 360; handPos(); paintFx();
    const deg = W.el.hand.querySelector("[data-h=deg]"); if (deg) deg.textContent = h.a + "°";
  }
  function handSay(text) {
    const h = W.hand; if (!h) return; h.msg = text; renderHand();
    clearTimeout(W.handMsgT); W.handMsgT = setTimeout(() => { if (W.hand === h) { h.msg = ""; renderHand(); } }, 1600);
  }
  function handDrop() {
    const h = W.hand, z = handPiece(); if (!h || !z) return;
    if (!h.pos || !h.pos.ok) return handSay("No room there: it would touch a charm or the edge");
    if (h.i + 1 >= h.pieces.length && !needName(() => { if (W.hand === h) handDrop(); })) return;   // (the last piece waits for the name)
    const v = h.pos.v; h.grid.stamp(v.fine.bits, v.fine.w, v.fine.h, h.pos.x, h.pos.y);
    h.spots.push({ c: z.c, src: z.src, placed: z.placed, cxPt: h.pos.cxPt, cyPt: h.pos.cyPt, angle: v.angle });
    h.i++;
    if (h.i < h.pieces.length) { handPos(); renderHand(); paintFx(); return; }
    const who = whoAmI(); if (!who) { handUndo(); return; }
    const { k, target, spots } = h;
    // what was put down by hand stays drawn where it was put, and its row flies into it
    const row = W.el.fill.querySelector(`li[data-rid="${CSS.escape(k.rid)}"]`);
    stopHand(true); W.add = null;
    if (!W.flow) landRow(row, spots, k.rid, { draw: true, now: true });
    paintFx();
    moveIn(k, spots, target, who).catch(e => { console.error("sheet window: by hand", e); toast("Not placed: " + e.message, "bad", 8000); });
  }
  function handUndo() {
    const h = W.hand; if (!h || !h.spots.length) return;
    h.spots.pop(); h.i--;
    h.grid = h.G.grid.clone(); h.grid.fineRes = 2;
    for (const s of h.spots) { const v = variantOf(s.c, s.angle, h.G.clearancePt); h.grid.stamp(v.fine.bits, v.fine.w, v.fine.h, Math.round(s.cxPt * 2 - v.solid.cx), Math.round(s.cyPt * 2 - v.solid.cy)); }
    handPos(); renderHand(); paintFx();
  }
  function renderHand() {
    const E = W.el, h = W.hand; if (!E.hand) return;
    if (!h) { E.hand.hidden = true; E.hand.innerHTML = ""; return; }
    const n = h.pieces.length;
    E.hand.innerHTML = `<span class="t"><b>${esc(h.k.rid)}</b><small>${n > 1 ? `piece ${h.i + 1} of ${n} · ` : ""}<span data-h="deg">${h.a}°</span></small></span>` +
      `<button type="button" class="swIcon" data-h="l" title="Turn left 10° ( [ or scroll; Shift for 2° )" aria-label="Turn left">${ICON.turnL}</button><button type="button" class="swIcon" data-h="r" title="Turn right 10° ( ] or scroll; Shift for 2° )" aria-label="Turn right">${ICON.turnR}</button>` +
      `<span class="k${h.msg ? " bad" : ""}">${esc(h.msg || (n > 1 && h.i ? "Click to drop the next piece" : "Scroll to turn · click to drop"))}</span>` +
      (h.spots.length ? `<button type="button" class="btn ghost xs" data-h="u" title="Take the last piece back (Backspace)">Undo</button>` : "") +
      `<button type="button" class="btn ghost xs" data-h="x" title="Stop (Esc)">Stop</button>`;
    E.hand.hidden = false;
    E.hand.querySelector("[data-h=l]").onclick = () => handTurn(-10);
    E.hand.querySelector("[data-h=r]").onclick = () => handTurn(10);
    E.hand.querySelector("[data-h=x]").onclick = () => stopHand();
    const u = E.hand.querySelector("[data-h=u]"); if (u) u.onclick = () => handUndo();
  }
  function paintHand(ctx) {
    const h = W.hand; if (!h || !W.geom) return;
    const draw = (s, c, ok, main) => withPiece(ctx, { p: { cxPt: s.cxPt, cyPt: s.cyPt, angle: s.angle, scale: 1 }, c }, () => {
      const x = { p: { cxPt: s.cxPt, cyPt: s.cyPt, angle: s.angle, scale: 1 }, c };
      ctx.fillStyle = ok ? (main ? "rgba(95,122,91,.28)" : "rgba(95,122,91,.18)") : "rgba(176,86,63,.26)"; outlinePath(ctx, x, true); ctx.fill("evenodd");
      CharmNestPDF.drawCharm(ctx, c, tx0(c), W.k);
      outlinePath(ctx, x); ctx.strokeStyle = ok ? "#5f7a5b" : "#b0563f"; ctx.lineWidth = (main ? 1.8 : 1.3) * W.dpr; ctx.stroke();
    });
    for (const s of h.spots) draw(s, s.c, true, false);
    const z = handPiece(); if (z && h.pos) draw({ cxPt: h.pos.cxPt, cyPt: h.pos.cyPt, angle: h.a }, z.c, h.pos.ok, true);
  }
  document.addEventListener("keydown", e => { if (W.hand && e.key === "Backspace" && !e.target.closest("input,textarea,[contenteditable]")) { e.preventDefault(); handUndo(); } });

  /* ── moving an order onto this sheet: off its filling sheet (saved first), onto this one at the spot found, read
     back and stamped. At no point does a saved sheet name a piece another saved sheet names; interrupted between the
     two saves, the pieces are on no sheet and the run places them again (restoreRunSheets), so nothing is lost. ── */
  const JOURNAL = () => "cn.sheetwin.moves" + (typeof WORKSPACE_SANDBOX !== "undefined" && WORKSPACE_SANDBOX ? ":sandbox" : "");
  function journal(rid, entry) {
    try { const j = JSON.parse(localStorage.getItem(JOURNAL()) || "{}"); if (entry) j[rid] = entry; else delete j[rid]; localStorage.setItem(JOURNAL(), JSON.stringify(j)); } catch (_) {}
  }
  // o.free: the nest itself picks the spots (spots are only proof that there is room); otherwise each piece is pinned
  async function moveIn(k, spots, target, who, o = {}) {
    const rid = k.rid, ids = new Set(spots.map(s => s.c.poolId)), sheetId = W.id;
    const srcs = [...new Set(spots.map(s => s.src))], tName = sheetWord(target.sheetId, target.fileBase, target);
    const sName = sh => sheetWord(sh.sheetId, sh.fileBase, sh);
    const wait = stepOf("Waiting for the sheets to finish their current step");
    const off = stepOf(`Taking order ${rid} off ${srcs.map(sName).join(", ")}`);
    const saves = srcs.map(sh => ({ sh, name: sName(sh), st: stepOf(`Saving ${sName(sh)} without it`) }));
    const put = stepOf(`Placing it on ${tName}`), labels = stepOf("Remaking the QR labels"), check = stepOf("Checking the saved sheets");
    const work = beginFlow({ state: "working", title: `Moving order ${rid} to ${tName}${o.part ? ` · ${o.part[0]} of ${o.part[1]}` : ""}`, steps: [wait, off, ...saves.map(r => r.st), put, labels, check], note: "" });
    if (!work) return false;
    const all = [target, ...srcs];
    if (!all.some(busy)) work.steps.shift();
    W.fill = null; renderFill(); renderWork();
    const paint = () => { if (W.dlg.open && W.work === work) renderWork(); };
    const moved = [];
    let stage = "start";
    journal(rid, { rid, ids: [...ids], from: srcs.map(s => s.sheetId || null), to: target.sheetId, by: who, at: Date.now(), stage });
    try {
      await waitIdle(all, wait, paint);
      // its sheet may have been released, cut or sent while this waited: then the order stays where it is
      const shut = srcs.find(src => !movableFrom(src, target));
      if (shut) throw new Error(`${sName(shut)} was released or sent meanwhile, so order ${rid} stays on it`);
      // the list it was picked from may be minutes old: each piece must still be on the sheet it was listed on (the run
      // passes waiting charms on), and the order must not have been put on hold or cancelled since
      for (const s of spots) { const on = allSheets().filter(sh => sh.charms.includes(s.c)); if (on.length !== 1 || on[0] !== s.src) throw new Error(`order ${rid} went to another sheet meanwhile; pick again from the new list`); }
      if (Cancelled.has(rid) || rowsOfOrder(rid).some(r => r.hold || ["held", "skipped", "gone"].includes(r.state))) throw new Error(`order ${rid} was put on hold or cancelled meanwhile`);
      // the room is tested again as the sheet now stands: something else may have gone into it meanwhile
      const G = plateGrid(target), g2 = G.grid;
      if (o.free) { if (!(await roomFor(k, G, async () => { await pause(0); }))) throw new Error(`there is no room for it on ${tName} now`); }
      else for (const s of spots) {
        const v = variantOf(s.c, s.angle, G.clearancePt), x = Math.round(s.cxPt * 2 - v.solid.cx), y = Math.round(s.cyPt * 2 - v.solid.cy);
        if (!v || !g2.fits(v.fine.pm, x, y)) throw new Error("the room changed while the sheets saved; pick again from the new suggestions");
        g2.stamp(v.fine.bits, v.fine.w, v.fine.h, x, y);
      }
      holdRelease(all);
      // 1 · off the filling sheets
      off.state = "now"; paint();
      for (const src of srcs) {
        const charms = src.charms.filter(c => ids.has(c.poolId)), cids = new Set(charms.map(c => c.id));
        const placements = src.placements.filter(p => cids.has(p.id)), backs = (src.backPool || []).filter(b => ids.has(b.poolId));
        src.charms = src.charms.filter(c => !cids.has(c.id)); src.placements = src.placements.filter(p => !cids.has(p.id));
        src.rejects = (src.rejects || []).filter(id => !cids.has(id));
        if (Array.isArray(src.feedWait)) src.feedWait = src.feedWait.filter(id => !cids.has(id));
        if (src.backPool) src.backPool = src.backPool.filter(b => !ids.has(b.poolId));
        moved.push({ src, charms, placements, backs });
        Orders.keepRest(src);
      }
      off.state = "ok"; paint();
      // 2 · each filling sheet saved without it first
      stage = "sources"; journal(rid, { rid, ids: [...ids], from: srcs.map(s => s.sheetId || null), to: target.sheetId, by: who, at: Date.now(), stage });
      for (const r of saves) if (!(await rewritePage(r.sh, r.st, paint))) throw new Error(`${r.name} is still saving; order ${rid} stays on it`);
      // 3 · onto this sheet, each piece pinned at the spot it was tested in
      stage = "target"; journal(rid, { rid, ids: [...ids], from: srcs.map(s => s.sheetId || null), to: target.sheetId, by: who, at: Date.now(), stage });
      for (const s of spots) { const c = s.c; delete c.arrivalPin; c.pinned = o.free ? null : { cxPt: s.cxPt, cyPt: s.cyPt, angle: s.angle }; if (!target.charms.includes(c)) target.charms.push(c); }
      for (const m of moved) if (m.backs.length) target.backPool = (target.backPool || []).concat(m.backs);
      Orders.keepRest(target);
      const saved = await rewritePage(target, put, paint);
      // a sheet that feeds its orders in turns places these on its next turn
      for (const until = Date.now() + 240000; Date.now() < until && (target.feedWait || []).some(id => spots.some(s => s.c.id === id)) || busy(target) && Date.now() < until;) { put.state = "now"; put.detail = "its turn to be placed"; paint(); await pause(400); }
      const placed = new Set(target.placements.map(p => p.id)), missed = spots.filter(s => !placed.has(s.c.id));
      if (missed.length) throw Object.assign(new Error(`order ${rid} did not go in when ${tName} was written`), { putBack: true });
      if (!saved && !target.persistedDone) throw Object.assign(new Error(`${tName} is still saving`), { later: true });
      put.state = "ok"; put.detail = ""; paint();
      await Pool.update([...ids], { sheetId: target.sheetId, sheetName: target.fileBase || null, movedFrom: srcs.map(s => s.sheetId).filter(Boolean).join(",") || null, movedTo: target.sheetId, movedBy: who, movedAt: Date.now() }).catch(e => console.warn("sheet window: pool after move", e));
      // 4 · labels, then read back: on this sheet, off the others
      await remakeLabels(all, labels, paint);
      letGo(all);
      const ok = await verifySaved([{ sh: target, name: tName, on: ids }, ...saves.map(r => ({ sh: r.sh, name: r.name, off: ids }))], ids, p => p.sheetId === target.sheetId && p.state !== "abandoned" && p.state !== "superseded", check, paint);
      if (ok) await Pool.update([...ids], { moveVerifiedAt: Date.now() }).catch(() => {});
      journal(rid, null);
      agent({ metal: target.metal, run: target.runId }, "POOL", `Order ${rid} moved by ${who} from ${srcs.map(sheetName).join(", ")} to ${sheetName(target)} into the room freed there${ok ? "; both saved sheets read back and match" : "; the read-back flagged: " + check.detail}`);
      // the room it took is no longer free
      const landed = target.placements.filter(p => spots.some(s => s.c.id === p.id));
      let G2 = null; try { G2 = plateGrid(target); } catch (_) {}
      const left = (FREED.get(sheetId) || []).filter(gh => freeStill(gh, G2, target, g => landed.some(s => Math.hypot(s.cxPt - g.x.p.cxPt, s.cyPt - g.x.p.cyPt) < Math.max(g.x.p.wPt || 20, g.x.p.hPt || 20) / 2)));
      setFreed(sheetId, left);
      work.state = "done"; work.title = ok ? `Order ${rid} moved to ${tName}` : `Order ${rid} moved to ${tName} · check flagged`;
      work.note = (ok ? "Verified: the saved sheets and piece records match. " : "") + `${srcs.map(s => esc(sName(s))).join(", ")} keep${srcs.length === 1 ? "s" : ""} filling with the next orders.`;
      paint(); endFlow(work);
      if (window.RunCtl) RunCtl.poke();
      if (W.id === sheetId && W.dlg.open) open(sheetId, { keepWork: true, keepSet: false, glow: [...ids] }); else unland(rid);
      return true;
    } catch (e) {
      unland(rid);
      for (const st of work.steps) doneStep(st);
      letGo(all);
      let back = "";
      if (e.later) back = ` It finishes on its card in the Nest tab; the order is on ${tName}.`;
      else if (moved.length) { back = await putBack(moved, target, spots).then(() => ` Order ${rid} went back to ${srcs.map(sName).join(", ")}.`).catch(e2 => ` Putting it back stopped too (${e2.message}); the run places order ${rid} again.`); }
      journal(rid, null);
      work.state = "failed"; work.title = `Order ${rid} not moved`; work.note = esc(e.message) + "." + esc(back);
      paint(); endFlow(work);
      if (W.id === sheetId && W.dlg.open) open2(sheetId);
      throw e;
    }
  }
  // undo a move that did not complete: off this sheet first (if it went on), then back where each piece was
  async function putBack(moved, target, spots) {
    const cids = new Set(spots.map(s => s.c.id)), quiet = stepOf(""), none = () => {};
    for (const s of spots) s.c.pinned = null;
    if (target.charms.some(c => cids.has(c.id))) {
      target.charms = target.charms.filter(c => !cids.has(c.id)); target.placements = target.placements.filter(p => !cids.has(p.id));
      target.rejects = (target.rejects || []).filter(id => !cids.has(id)); if (Array.isArray(target.feedWait)) target.feedWait = target.feedWait.filter(id => !cids.has(id));
      const bids = new Set(moved.flatMap(m => m.backs.map(b => b.poolId))); if (target.backPool) target.backPool = target.backPool.filter(b => !bids.has(b.poolId));
      Orders.keepRest(target); await rewritePage(target, quiet, none);
    }
    for (const m of moved) {
      for (const c of m.charms) if (!m.src.charms.includes(c)) m.src.charms.push(c);
      for (const p of m.placements) if (!m.src.placements.some(q => q.id === p.id)) m.src.placements.push(p);
      if (m.backs.length) m.src.backPool = (m.src.backPool || []).concat(m.backs);
      Orders.keepRest(m.src); await rewritePage(m.src, quiet, none);
    }
  }
  // a move this browser was making when it was closed: its pieces are wherever the saved sheets put them (a piece on
  // no sheet is placed again by the run); the interruption is written to the run's log once
  setTimeout(() => {
    let j = {}; try { j = JSON.parse(localStorage.getItem(JOURNAL()) || "{}"); } catch (_) {}
    for (const e of Object.values(j)) {
      const where = new Set((e.ids || []).map(id => { const sh = window.Pool && Pool.sheetOf(id); return sh ? sheetName(sh) : "no sheet yet (placed again by the run)"; }));
      try { agent({ pool: true }, "warn", `A move of order ${e.rid} by ${e.by || "someone"} was interrupted (${e.stage}); its pieces are on ${[...where].join(", ") || "no sheet"}.`); } catch (_) {}
      journal(e.rid, null);
    }
  }, 30000);

  /* ── On hold, in the sheet window: every order a person put on hold or took off a sheet, with Put back and Cancel ── */
  function heldOrders() {
    const by = new Map();
    for (const r of window.Orders ? Orders.rows() : []) {
      if (!r.hold || r.state === "gone") continue;
      const rid = String(r.order.receiptId); if ((W.going.get(rid) || {}).how === "cancel") continue;   // (being cancelled: its row has folded away)
      if (!by.has(rid)) by.set(rid, []); by.get(rid).push(r);
    }
    return [...by].map(([rid, rows]) => ({ rid, rows, at: Math.max(0, ...rows.map(r => +r.heldAt || 0)) }))
      .sort((a, b) => b.at - a.at || a.rid.localeCompare(b.rid));
  }
  function renderHeld(force) {
    // (a refresh while someone is typing why an order is cancelled, or while a release runs, waits for them)
    // (a row that has already left, as a copy, does not wait with them)
    if (!force && W.el.orders.querySelector(".swHeld .acts.confirm, .swHeld [data-h=back]:disabled")) { for (const n of [...W.el.orders.children]) if (n._mLeaving) closeGap(n); return; }
    const E = W.el, q = W.q, list = heldOrders().filter(h => !q || h.rid.includes(q) || h.rows.some(r => `${r.spec?.designSku || r.line.sku} ${r.hold}`.toLowerCase().includes(q)));
    if (!list.length) { putRows(`<li class="swNone">${q ? "No order on hold matches." : "No order is on hold."}</li>`); return; }
    // (a row drawn again does not drop in again: only a new one does)
    const had = new Set([...E.orders.children].map(n => n.dataset.mkey).filter(Boolean));
    putRows(list.map(h => {
      const skus = new Map(); for (const r of h.rows) { const n = r.spec?.designSku || r.line.sku || "no SKU"; skus.set(n, (skus.get(n) || 0) + (r.spec?.quantity || r.line.quantity || 1)); }
      const why = h.rows[0].hold;
      return `<li class="swHeld" data-rid="${esc(h.rid)}" data-mkey="h:${esc(h.rid)}"${had.has("h:" + h.rid) ? ' style="animation:none"' : ""}><div class="top"><span class="no">${esc(h.rid)}</span><span class="what">${esc([...skus].map(([n, c]) => n + (c > 1 ? " ×" + c : "")).join(", "))}</span>${h.at ? `<span class="when">${esc(ago(h.at))}</span>` : ""}</div>
        <div class="why" title="${esc(why)}">${esc(why)}</div>
        <div class="acts"><button type="button" class="btn ghost xs" data-h="back" title="Back in line: the run places it on the next sheet that fits">Release hold</button><button type="button" class="btn ghost xs" data-h="cancel">Cancel order…</button><button type="button" class="btn ghost xs" data-h="orders" title="Show it in the Orders tab">In Orders</button></div></li>`;
    }).join(""));
    E.orders.querySelectorAll(".swHeld").forEach(li => {
      const rid = li.dataset.rid, rows = () => rowsOfOrder(rid).filter(r => r.hold);
      li.querySelector("[data-h=back]").onclick = async ev => {
        const b = ev.currentTarget; b.disabled = true; b.innerHTML = `<span class="spin"></span>Releasing`;
        let went = false;
        try {
          for (const r of rows()) { r.heldAt = null; await Review.repool(r); }
          if (window.RunCtl) RunCtl.poke();
          agent({ bridge: true }, "DS", `Order ${rid} put back in line by ${whoAmI() || "someone"}`);
          window.SheetEvents?.order({ type: "released", orderId: rid, id: `sw-release-${Date.now()}`, by: whoAmI(), text: "Hold released in the sheet window · back in line" });
          went = true;
        } catch (e) { toast("Hold not released: " + e.message, "bad", 7000); }
        b.disabled = false;
        if (went && li.isConnected) backInLine(li, rid); else if (went) toast(`Order ${rid} is back in line`, "ok", 5000);   // (its row flies to All, before the list is drawn again)
        if (W.dlg.open && W.rec) renderSheetPane();
      };
      li.querySelector("[data-h=orders]").onclick = () => { close().then(() => { setMode("orders"); if (Orders.showPile) Orders.showPile("hold", rid); }); };
      li.querySelector("[data-h=cancel]").onclick = () => {
        const acts = li.querySelector(".acts");
        acts.innerHTML = `<input class="swNote" type="text" maxlength="200" placeholder="Why it was cancelled (optional)" aria-label="Why"><button type="button" class="btn danger xs" data-h="yes">Cancel the order</button><button type="button" class="btn ghost xs" data-h="no">Keep</button>`;
        acts.classList.add("confirm"); const inp = acts.querySelector("input"); inp.focus();
        inp.onkeydown = e => { if (e.key === "Enter") acts.querySelector("[data-h=yes]").click(); if (e.key === "Escape") { e.stopPropagation(); e.preventDefault(); renderHeld(true); } };
        acts.querySelector("[data-h=no]").onclick = () => renderHeld(true);
        acts.querySelector("[data-h=yes]").onclick = () => {
          const who = needName(() => { const y = acts.querySelector("[data-h=yes]"); if (y && y.isConnected) y.click(); }); if (!who) return;
          acts.classList.remove("confirm"); acts.innerHTML = `<span class="swWait"><span class="owSpin"></span>Cancelling</span>`;
          const opt = { then: "cancel", note: inp.value.trim() }, plan = offPlan({ rid, poolId: null }, true);
          (plan.ok.length ? takeOff(plan, opt, who) : cancelOnly(rid, opt, who)).catch(e => toast("Not cancelled: " + e.message, "bad", 8000));
        };
      };
    });
  }

  /* re-read the engraving marks when Engrave settles something while the window is open */
  setInterval(() => {
    if (!W.dlg || !W.dlg.open || !W.rec || !W.geom) return;
    let changed = false;
    for (const x of W.pieces) { const e = engOf(x); if (!x.eng || e.kind !== x.eng.kind || e.note !== x.eng.note) { x.eng = e; changed = true; } }
    if (changed) { renderStrip(); if (W.view === "sheet") renderSheetPane(); else if (W.sel) renderEng(W.sel, true); paintFx(); }
  }, 1500);

  function veil(text) { const E = W.el; if (!text) { E.veil.hidden = true; return; } E.veilText.textContent = text; E.veil.hidden = false; }

  window.SheetWin = { open, close, isOpen: () => !!(W.dlg && W.dlg.open), current: () => W.id, _W: W };
})();
