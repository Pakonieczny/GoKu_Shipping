/* The Library's "Moving" bar (Paul, 3 Oct): what happens to a sheet or a set of sheets once it is dropped somewhere, how
   it gets approved there, what the page approved by itself, and what is still missing before it can go to Laser cutting.

     LibraryApprovalUI.show(host, plan, { onConfirm(keys), onCancel, title, kind, by, applied, committing })
     LibraryApprovalUI.update(host, { ok, applied:[{key,label}], error })      (after the commit)
     LibraryApprovalUI.hide(host)

   plan is a LibraryFlow Plan { ok, from, to, auto, needs, confirm, notes } (charm-nest-flow.js), or nothing / a promise
   while it is being worked out ("Checking the move"). host is any element: a set card, a section, an empty strip. The bar
   is drawn inside it as its first child (opts.where "end" puts it last), and spans a grid or flex row.

   What it shows, in order:
     - a labelled spinner while the plan is worked out ("Checking the move") and while the move is committed ("Moving");
     - the plan's `auto` lines, one after another (about 220 ms apart), each with a check that draws itself, or, where a real
       stamp is meant (a signed approval stamp, an engraving approval), the shared seal look landing on the line;
     - `needs` in red under "This set cannot move to Laser cutting until:", every missing item a button that opens its
       order or sheet with the page's own helpers (openOrderFrom / OrderWin.openOrder, openLibrarySheet / SheetWin.open);
     - `confirm` items, each with its own explicit button. A press passes its key to onConfirm(keys); nothing is ever
       confirmed by the drop. The Rose Gold key roseLine uses LibraryFlowRose.confirmBar when it is there, else a plain
       bar ("Add the green dash line" / "Not now");
     - after update(): applied lines turn green, an error turns red, and one plain summary line ends the bar
       ("Moved to Laser cutting. 3 things were approved for you.").
   A plan that is ok with nothing to confirm is committed by the caller at once: the bar then shows its lines and a
   "Moving" spinner without a button, until update() arrives. A plan that is blocked, or waits for a confirm, shows its
   stamps as dashed outlines (nothing is stamped until the move is committed); they take their ink as the move is.
   Options: kind "sheet" | "set" (the words "This set cannot ..."; plan.kind / plan.item.kind also work), by (the person
   a stamp is for, else the signed-in name), applied (the plan's auto lines are already done, as after LibraryFlow.approve:
   they show green, stamps inked, no spinner. Taken as true when the plan has no destination (no `to`), the title starts
   with "Approve", the plan lists `applied` lines or says approved / mode "approve"; applied:false forces the other
   reading), committing:false (no spinner),
   checking / moving (the two spinner labels), needsText (the red sentence), onClose (the bar was closed after it ended),
   where "start" | "end", focus:false (do not move the keyboard focus onto the bar's first button).
   Esc is "Not now" (onCancel, then the bar closes); a bar that has ended closes with Esc or its Close button.
   Seals on this bar are pictures: no hover copy, no tooltip, never recorded here. The bar never writes anything; only the
   caller's own commit does. Nothing here throws. */
(function () {
  'use strict';
  if (window.LibraryApprovalUI) return;
  const doc = document;
  const GAP = 220, GAP_TOTAL = 1800, SHOW_ITEMS = 8, SLOW = 20000;
  const AREA = { progress: 'In progress', laser: 'Laser cutting', completed: 'Completed' };
  const bars = new WeakMap();
  const live = new Set();
  let uid = 0;

  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const warn = (what, e) => { try { console.warn('[approval bar] ' + what + ':', e && e.message || e); } catch (_) {} };
  const plural = (n, one, many) => `${n} ${n === 1 ? one : many || one + 's'}`;
  const reduced = () => { try { return !!((window.Motion && Motion.reduced && Motion.reduced()) || matchMedia('(prefers-reduced-motion: reduce)').matches); } catch (_) { return false; } };
  const arr = v => Array.isArray(v) ? v.filter(x => x && typeof x === 'object') : [];
  const then = v => !!v && typeof v.then === 'function';
  const hash = s => { let h = 2166136261; for (const c of String(s)) { h ^= c.charCodeAt(0); h = Math.imul(h, 16777619); } return h >>> 0; };
  const errText = e => { if (!e) return ''; if (typeof e === 'string') return e; if (e.message) return String(e.message); try { return JSON.stringify(e); } catch (_) { return String(e); } };

  /* ── looks ── */
  const STYLE = `
.lapBar{--lap:var(--ink25,#c4bdb0);grid-column:1/-1;flex:0 0 100%;box-sizing:border-box;width:100%;max-width:100%;min-width:0;margin:0 0 8px;padding:10px 12px 11px 15px;background:var(--card,#fffefb);border:1px solid var(--line,#e4ddd0);border-radius:12px;box-shadow:inset 3px 0 0 var(--lap),0 1px 2px rgba(30,26,20,.04),0 8px 24px rgba(30,26,20,.07);color:var(--ink,#1c1a17);font:12px/1.45 var(--sans,-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,system-ui,sans-serif);position:relative;scroll-margin:84px 0 24px;overflow-wrap:anywhere;transition:box-shadow .3s ease;text-align:left}
.lapBar[data-tone=bad]{--lap:var(--clay,#b0563f)}
.lapBar[data-tone=wait]{--lap:var(--gold2,#caa861)}
.lapBar[data-tone=ok]{--lap:var(--sage,#5f7a5b)}
.lapBar:focus{outline:none}
.lapBar.enter{animation:lapIn .22s cubic-bezier(.2,.8,.2,1)}
.lapBar.leaving{pointer-events:none;overflow:hidden}
.lapHead{display:flex;align-items:center;gap:10px;min-height:24px}
.lapTitle{flex:1 1 auto;min-width:0;font:650 12.5px/1.3 var(--sans,system-ui,sans-serif);letter-spacing:.005em}
.lapHeadAct{flex:none}
.lapBtn{display:inline-flex;align-items:center;gap:6px;min-height:26px;box-sizing:border-box;padding:3px 11px;border:1px solid var(--line,#e4ddd0);border-radius:9px;background:transparent;color:var(--ink,#1c1a17);font:650 11.5px/1.3 var(--sans,system-ui,sans-serif);text-align:left;cursor:pointer;transition:background-color .15s ease,transform .08s ease,opacity .15s}
.lapBtn:hover{background:var(--card2,#faf7f1)}
.lapBtn:active{transform:translateY(1px)}
.lapBtn:focus-visible,.lapItem:focus-visible{outline:2px solid var(--gold2,#caa861);outline-offset:1px}
.lapBtn:disabled{opacity:.45;cursor:default;transform:none}
.lapBtn.go{background:var(--gold,#a9823f);border-color:var(--gold,#a9823f);color:#2a2013}
.lapBtn.go:hover{background:var(--gold,#a9823f);opacity:.9}
.lapBtn.go[aria-pressed=true]{background:var(--ink,#1c1a17);border-color:var(--ink,#1c1a17);color:#fff}
.lapList{display:grid;gap:1px;margin-top:6px}
.lapList:empty{display:none}
.lapCap{margin:0 0 3px;color:var(--ink70,#5b554c);font-size:11.5px}
.lapLine{display:grid;grid-template-columns:22px minmax(0,1fr);gap:9px;align-items:center;min-height:24px;padding:2px 0}
.lapLine.stamp{grid-template-columns:46px minmax(0,1fr);min-height:46px}
.lapLine.enter{animation:lapLineIn .22s cubic-bezier(.2,.8,.2,1)}
.lapMark{position:relative;display:block;width:18px;height:18px;justify-self:center}
.lapLine.stamp .lapMark{width:44px;height:44px}
.lapMark svg{display:block;width:100%;height:100%;overflow:visible}
.lapRing{fill:var(--card2,#faf7f1);stroke:var(--ink25,#c4bdb0);stroke-width:1.3;transform-box:fill-box;transform-origin:center;transition:fill .3s ease,stroke .3s ease}
.lapTick{fill:none;stroke:var(--ink70,#5b554c);stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round;transition:stroke .3s ease}
.lapLine.in .lapRing{animation:lapPop .28s cubic-bezier(.3,1.5,.5,1) both}
.lapLine.in .lapTick{stroke-dasharray:14;stroke-dashoffset:14;animation:lapDraw .26s .1s ease-out forwards}
.lapLine.ok .lapRing{fill:var(--sageSoft,#e7eddf);stroke:var(--sage,#5f7a5b)}
.lapLine.ok .lapTick{stroke:#3c5a39}
.lapLine.ghost .lapRing{fill:transparent;stroke-dasharray:2.4 2.4}
.lapLine.ghost .lapTick{opacity:0}
.lapLine.off{opacity:.55}
.lapSeal{position:relative;display:block;width:100%;height:100%;transform:rotate(var(--rot,-6deg));mix-blend-mode:multiply;pointer-events:none}
.lapSeal svg{pointer-events:none}
.lapSeal .lapInk{position:absolute;inset:0;transition:opacity .35s ease}
.lapSeal.ghost .lapInk{opacity:.8}
.lapSealRing{position:absolute;inset:0;border-radius:50%;border:2px solid var(--ink,#1c1a17);pointer-events:none;box-sizing:border-box}
.lapTxt{min-width:0}
.lapLbl{font-weight:600;font-size:12.5px;transition:color .3s ease;text-wrap:pretty}
.lapLine.ok .lapLbl{color:#3c5a39}
.lapDet{display:block;color:var(--ink70,#5b554c);font-size:11.5px;line-height:1.4}
.lapNeeds{margin-top:9px;padding:9px 11px 10px;border-radius:9px;background:var(--claySoft,#f4e3dc);border:1px solid rgba(176,86,63,.28);color:#7a3321}
.lapNeeds[hidden],.lapConfirm[hidden],.lapNotes[hidden],.lapBusy[hidden],.lapSummary[hidden]{display:none}
.lapNeeds.enter,.lapConfirm.enter,.lapSummary.enter,.lapNotes.enter{animation:lapLineIn .24s cubic-bezier(.2,.8,.2,1)}
.lapNeedsHead{display:flex;align-items:flex-start;gap:8px;font:650 12.5px/1.35 var(--sans,system-ui,sans-serif)}
.lapNeedsHead svg,.lapSummary svg{flex:none;width:16px;height:16px;margin-top:0}
.lapAlert{fill:none;stroke:var(--clay,#b0563f);stroke-width:1.5;stroke-linecap:round}
.lapNeedList{list-style:none;margin:7px 0 0;padding:0;display:grid;gap:9px}
.lapNeed .lapNeedT b{font-weight:650}
.lapNeed .lapNeedT span{color:#8a3a26}
.lapWhyBtn{border:0;background:none;padding:0 2px;margin-left:4px;font:650 11.5px/1.3 var(--sans,system-ui,sans-serif);color:var(--ink70,#5b554c);text-decoration:underline dotted;text-underline-offset:2px;cursor:pointer;border-radius:4px}
.lapWhyBtn:hover{color:var(--ink,#1c1a17)}
.lapWhyBtn:focus-visible{outline:2px solid var(--gold2,#caa861);outline-offset:1px}
.lapWhySlot{display:grid;grid-template-rows:0fr;transition:grid-template-rows .24s cubic-bezier(.2,.8,.2,1)}
.lapWhySlot.open{grid-template-rows:1fr}
.lapWhyIn{display:block;min-height:0;overflow:hidden;visibility:hidden;transition:visibility 0s .24s}
.lapWhySlot.open>.lapWhyIn{visibility:visible;transition-delay:0s}
.lapWhy{display:block;padding-top:3px;font-size:11.5px;line-height:1.45}
.lapItems{display:flex;flex-wrap:wrap;gap:5px;margin-top:5px}
.lapItem{display:inline-flex;align-items:center;gap:5px;min-height:24px;box-sizing:border-box;max-width:100%;padding:3px 10px;border:1px solid rgba(176,86,63,.42);border-radius:999px;background:var(--card,#fffefb);color:#8a3a26;font:600 11.5px/1.3 var(--sans,system-ui,sans-serif);text-align:left;cursor:pointer;transition:background-color .15s ease,transform .08s ease}
button.lapItem:hover{background:#fff4ee}
button.lapItem:active{transform:translateY(1px)}
span.lapItem{cursor:default;border-style:dashed}
.lapItem .why{font-weight:500;color:var(--ink70,#5b554c)}
.lapItem .go{font-size:11px;opacity:.8}
.lapMore{border-style:dashed}
.lapConfirm{margin-top:9px;display:grid;gap:9px;padding:9px 11px 10px;border-radius:9px;background:var(--goldSoft,#f0e6cd);border:1px solid var(--goldLine,#e3d3a6);color:#5c4210}
.lapCT b{font-weight:650;display:block;text-wrap:pretty}
.lapCT span{display:block;font-size:11.5px;line-height:1.4}
.lapCBtns{display:flex;flex-wrap:wrap;gap:7px;margin-top:7px}
.lapConfirm .lapBtn:not(.go){background:var(--card,#fffefb)}
.lapConfirm.locked .lapBtn{pointer-events:none;opacity:.5}
.lapConfirm.lapPlain{padding:0;background:none;border:0}
.lapConfirm.lapPlain .lfrBar{margin:0;max-width:none}
.lapNotes{margin-top:7px;display:grid;gap:2px;color:var(--ink70,#5b554c);font-size:11.5px}
.lapBusy{display:flex;align-items:center;gap:8px;margin-top:8px;color:var(--ink70,#5b554c);font-size:12px}
.lapSpin{width:12px;height:12px;flex:none;box-sizing:border-box;border:2px solid var(--line,#e4ddd0);border-top-color:var(--ink70,#5b554c);border-radius:50%;animation:lapSpin .7s linear infinite}
.lapSummary{display:flex;align-items:flex-start;gap:8px;margin-top:9px;padding-top:8px;border-top:1px solid var(--line2,#efe9dd);font:650 12.5px/1.4 var(--sans,system-ui,sans-serif)}
.lapSummary.ok{color:#3c5a39}
.lapSummary.bad{color:#8a3a26}
.lapSummary .lapRing{fill:var(--sageSoft,#e7eddf);stroke:var(--sage,#5f7a5b)}
.lapSummary .lapTick{stroke:#3c5a39}
.lapLive{position:absolute;width:1px;height:1px;margin:-1px;padding:0;overflow:hidden;clip:rect(0 0 0 0);white-space:nowrap;border:0}
@keyframes lapSpin{to{transform:rotate(360deg)}}
@keyframes lapIn{from{opacity:0;transform:translateY(-4px)}}
@keyframes lapLineIn{from{opacity:0;transform:translateY(4px)}}
@keyframes lapPop{0%{transform:scale(.55)}60%{transform:scale(1.14)}100%{transform:scale(1)}}
@keyframes lapDraw{to{stroke-dashoffset:0}}
@media(max-width:560px){.lapBar{padding:9px 10px 10px 13px}.lapBtn{min-height:34px}.lapItem{min-height:32px}.lapLine.stamp{grid-template-columns:40px minmax(0,1fr)}.lapLine.stamp .lapMark{width:38px;height:38px}}
@media(pointer:coarse){.lapBtn{min-height:34px}.lapItem{min-height:32px}}
@media(prefers-reduced-motion:reduce){.lapWhySlot,.lapWhyIn{transition:none}.lapBar.enter,.lapLine.enter,.lapNeeds.enter,.lapConfirm.enter,.lapSummary.enter,.lapNotes.enter{animation:none}.lapLine.in .lapRing,.lapLine.in .lapTick{animation:none;stroke-dasharray:none;stroke-dashoffset:0}.lapSpin{animation-duration:1.6s}.lapBar,.lapRing,.lapTick,.lapLbl,.lapSeal .lapInk{transition:none}}
`;
  function css() {
    if (doc.getElementById('lapCss')) return;
    const s = doc.createElement('style'); s.id = 'lapCss'; s.textContent = STYLE; (doc.head || doc.documentElement).appendChild(s);
  }
  const CHECK = '<svg viewBox="0 0 16 16" aria-hidden="true" focusable="false"><circle class="lapRing" cx="8" cy="8" r="7"/><path class="lapTick" d="M4.7 8.4l2.3 2.3 4.5-4.9"/></svg>';
  const ALERT = '<svg viewBox="0 0 16 16" aria-hidden="true" focusable="false"><circle class="lapAlert" cx="8" cy="8" r="7"/><path class="lapAlert" d="M8 4.6v4.1M8 11.3v.2"/></svg>';

  /* ── words ── */
  const areaOf = p => p && (AREA[p.area] || '') || '';
  const destOf = plan => { const to = plan && plan.to; if (!to) return ''; return to.setId ? (to.label || to.name || 'the set') : areaOf(to) || to.label || ''; };
  const kindOf = (plan, opts) => { const k = String(opts.kind || plan && (plan.kind || plan.item && plan.item.kind) || '').toLowerCase(); return k === 'sheet' ? 'sheet' : 'set'; };
  function titleOf(plan, opts) {
    if (opts.title) return String(opts.title);
    if (!plan || !plan.to) return 'Moving';
    const to = plan.to, dest = destOf(plan), from = plan.from && !plan.from.setId ? areaOf(plan.from) : '';
    if (to.setId) return `Moving into ${dest}`;
    return from && from !== dest ? `Moving from ${from} to ${dest}` : `Moving to ${dest || 'the new place'}`;
  }
  function needsText(plan, opts) {
    if (opts.needsText) return String(opts.needsText);
    const to = plan && plan.to || {}, what = kindOf(plan, opts);
    const act = to.area === 'laser' || (!to.area && !to.setId) ? 'move to Laser cutting' : to.setId ? `join ${String(to.label || to.name || 'that set').split(/\s+[\u00b7\u2022|]\s+/)[0]}` : `move to ${areaOf(to) || 'its new place'}`;
    return `This ${what} cannot ${act} until:`;
  }
  const movedText = plan => { const to = plan && plan.to; if (!to) return 'Done'; if (to.setId) return `Moved into ${destOf(plan)}`; const a = areaOf(to); return a ? `Moved to ${a}` : 'Moved'; };
  const things = n => `${plural(n, 'thing')} ${n === 1 ? 'was' : 'were'} approved for you`;

  /* ── opening an order or a sheet: the page's own helpers (an open pop-up hands over; never one over another) ── */
  function openOrder(btn, rid) {
    rid = String(rid || '').replace(/\D/g, ''); if (!rid) return false;
    try { if (typeof window.openOrderFrom === 'function' && window.openOrderFrom(btn, rid) !== false) return true; } catch (e) { warn('open order', e); }
    try { if (window.OrderWin && typeof OrderWin.openOrder === 'function') { OrderWin.openOrder(rid, { from: btn }); return true; } } catch (e) { warn('open order', e); }
    return false;
  }
  function openSheet(btn, id) {
    id = String(id || ''); if (!id) return false;
    try { if (typeof window.openLibrarySheet === 'function') { window.openLibrarySheet(id); return true; } } catch (e) { warn('open sheet', e); }
    try { if (window.CN && typeof CN.openLibrarySheet === 'function') { CN.openLibrarySheet(id); return true; } } catch (e) { warn('open sheet', e); }
    try { if (window.SheetWin && typeof SheetWin.open === 'function') { const r = btn && btn.getBoundingClientRect(); SheetWin.open(id, r && r.width ? { fromRect: r } : {}); return true; } } catch (e) { warn('open sheet', e); }
    return false;
  }
  /** what an item of a missing thing opens: { how:'order'|'sheet', id } or null (shown as plain words) */
  function targetOf(it) {
    const kind = String(it.kind || '').toLowerCase();
    const rid = it.orderId || it.rid || it.receiptId || (kind === 'order' ? it.id : '');
    if (rid && /\d/.test(String(rid))) return { how: 'order', id: String(rid) };
    const sid = it.sheetId || (kind === 'sheet' ? it.id : '');
    if (sid) return { how: 'sheet', id: String(sid) };
    return null;
  }

  /* ── stamps: the shared seal look ── */
  const isStamp = it => !!(it.stamp || it.seal || /seal|stamp|signed/i.test(String(it.key || '')) || /\b(stamp(ed)?|seal(ed)?|signed)\b/i.test(String(it.label || '')));
  function sealSvg(item, bar, ghost) {
    const S = window.Seal; if (!S || typeof S.face !== 'function' || typeof S.modelOf !== 'function') return '';
    try {
      const given = item.seal && typeof item.seal === 'object' ? item.seal : {}, text = `${item.key || ''} ${item.label || ''}`;
      const how = given.how || (/engrav/i.test(text) ? 'engraveApproved' : /\b(cut|complete|done)\b/i.test(text) && !/ready|approv/i.test(text) ? 'laserDone' : 'laserReady');
      const model = S.modelOf(Object.assign({ how, at: Date.now(), by: item.by || bar.by || '' }, given));
      return S.face(model, { ghost: !!ghost });
    } catch (e) { warn('seal', e); return ''; }
  }
  const inkOf = svg => { const m = /data-seal-family="([a-z]+)"/.exec(svg || ''); return (window.Seal && Seal.FAMILY && Seal.FAMILY[m && m[1]] || {}).ink || '#98721f'; };
  const rotOf = key => { const h = hash(key); return (h % 2 ? 1 : -1) * (4 + h % 7); };

  /* ── the bar ── */
  const q = (bar, sel) => bar.el.querySelector(sel);
  const alive = (bar, tok) => bar.token === tok && bars.get(bar.host) === bar && bar.el.isConnected;
  function later(bar, ms, fn) {
    const tok = bar.token, id = setTimeout(() => { bar.timers.delete(id); if (alive(bar, tok)) { try { fn(); } catch (e) { warn('step', e); } } }, ms);
    bar.timers.add(id); return id;
  }
  function stop(bar) { for (const id of bar.timers) clearTimeout(id); bar.timers.clear(); bar.token = ++uid; }
  const tone = (bar, t) => { bar.el.dataset.tone = t || ''; };
  const state = (bar, s) => { bar.state = s; bar.el.dataset.state = s; };
  function say(bar, text) { const n = q(bar, '.lapLive'); if (n) { n.textContent = ''; setTimeout(() => { if (n.isConnected) n.textContent = text; }, 30); } }

  function open(host, opts) {
    css();
    for (const b of [...live]) if (!b.el.isConnected) live.delete(b);
    let bar = bars.get(host);
    for (const old of host.querySelectorAll(':scope > .lapBar.leaving')) old.remove();
    const fresh = !(bar && bar.el.isConnected && bar.el.parentNode === host);
    if (fresh) {
      const el = doc.createElement('div');
      el.className = 'lapBar' + (reduced() ? '' : ' enter'); el.tabIndex = -1; el.setAttribute('role', 'region'); el.setAttribute('aria-label', 'Move status'); el.dataset.libraryApproval = '';
      if (!reduced()) el.addEventListener('animationend', () => el.classList.remove('enter'), { once: true });
      if (opts.where === 'end') host.appendChild(el); else host.insertBefore(el, host.firstChild);
      bar = { host, el, timers: new Set(), token: 0, rose: null }; bars.set(host, bar); live.add(bar);
    } else { stop(bar); if (bar.rose) { try { bar.rose.destroy && bar.rose.destroy(); } catch (_) {} } bar.rose = null; }
    stop(bar);
    Object.assign(bar, { opts, plan: null, state: 'checking', lines: new Map(), items: [], pressed: false, ended: false, staggering: false, pending: null, ghost: false, by: String(opts.by || (window.CNEmployee && CNEmployee.name && tryName()) || '').trim() });
    bar.el.innerHTML = `<div class="lapHead"><span class="lapTitle"></span><span class="lapHeadAct"></span></div><div class="lapList"></div><div class="lapNeeds" hidden></div><div class="lapConfirm" hidden></div><div class="lapNotes" hidden></div><div class="lapBusy" hidden></div><div class="lapSummary" hidden></div><div class="lapLive" role="status" aria-live="polite"></div>`;
    bar.el.onclick = e => onClick(bar, e);
    bar.el.onkeydown = e => { if (e.key === 'Escape' && closeIt(bar)) { e.preventDefault(); e.stopPropagation(); } };
    state(bar, 'checking'); tone(bar, '');
    return bar;
  }
  function tryName() { try { return CNEmployee.name(); } catch (_) { return ''; } }

  function busy(bar, label) {
    const n = q(bar, '.lapBusy'); n.hidden = false; n.innerHTML = `<span class="lapSpin" aria-hidden="true"></span><span>${esc(label)}</span>`;
    bar.busyAt = Date.now(); say(bar, label);
    later(bar, SLOW, () => { if (!n.hidden && n.isConnected) { const t = n.lastElementChild; if (t) t.textContent = `${label}. This is taking a little longer than usual`; } });
  }
  const unbusy = bar => { const n = q(bar, '.lapBusy'); n.hidden = true; n.innerHTML = ''; };
  function head(bar, title, act) {
    q(bar, '.lapTitle').textContent = title;
    const a = q(bar, '.lapHeadAct');
    a.innerHTML = act ? `<button type="button" class="lapBtn" data-act="${act === 'Not now' ? 'cancel' : 'close'}">${esc(act)}</button>` : '';
  }
  function checking(bar) {
    state(bar, 'checking'); tone(bar, '');
    head(bar, titleOf(null, bar.opts), '');
    busy(bar, bar.opts.checking || 'Checking the move');
  }

  /* an auto line: a check that draws itself, or a seal that lands */
  function addLine(bar, item, o) {
    const stamp = isStamp(item), key = String(item.key || 'auto' + bar.lines.size), line = doc.createElement('div');
    const svg = stamp ? sealSvg(item, bar, o.ghost) : '', asSeal = !!svg;
    line.className = 'lapLine' + (asSeal ? ' stamp' : '') + (o.animate ? ' enter' + (o.ghost && !asSeal ? '' : ' in') : '') + (o.ok ? ' ok' : '') + (o.ghost && !asSeal ? ' ghost' : '');
    line.dataset.key = key;
    const by = !item.detail && asSeal && bar.by && !new RegExp('\\b' + bar.by.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\b', 'i').test(item.label || '') ? `Signed by ${bar.by}` : '';
    const rot = rotOf(key);
    line.innerHTML = `<span class="lapMark" aria-hidden="true">${asSeal ? `<span class="lapSeal${o.ghost ? ' ghost' : ''}" style="--rot:${rot}deg"><span class="lapInk">${svg}</span></span>` : CHECK}</span><span class="lapTxt"><span class="lapLbl">${esc(item.label || item.key || '')}</span>${item.detail || by ? ` <span class="lapDet">${esc(item.detail || by)}</span>` : ''}</span>`;
    q(bar, '.lapList').appendChild(line);
    const rec = { el: line, item, asSeal, ghost: !!o.ghost, rot, key };
    bar.lines.set(key, rec);
    if (o.animate) {
      line.addEventListener('animationend', e => { if (e.target === line) line.classList.remove('enter'); });
      if (asSeal && !o.ghost) land(rec);
    }
    return rec;
  }
  /* the shared seal look landing, the engine's own press curve, smaller */
  function land(rec) {
    const mark = rec.el.querySelector('.lapSeal'); if (!mark || reduced() || !mark.animate) return;
    try {
      const rot = rec.rot, ink = inkOf(mark.innerHTML);
      mark.animate([{ transform: `translate(8px,-14px) rotate(${rot - 14}deg) scale(1.6)`, opacity: 0 }, { opacity: 1, offset: .25 }, { transform: `rotate(${rot}deg) scale(1)`, opacity: 1 }], { duration: 340, easing: 'cubic-bezier(.62,0,.92,.5)' });
      mark.animate([{ filter: 'saturate(1.4) brightness(.82)' }, { filter: 'none' }], { duration: 760, easing: 'ease-out' });
      const ring = doc.createElement('span'); ring.className = 'lapSealRing'; ring.style.borderColor = ink; mark.parentNode.appendChild(ring);
      const a = ring.animate([{ transform: 'scale(.86)', opacity: 0 }, { transform: 'scale(.9)', opacity: .42, offset: .4 }, { transform: 'scale(1.42)', opacity: 0 }], { duration: 760, delay: 300, easing: 'cubic-bezier(.2,.7,.3,1)', fill: 'backwards' });
      a.finished.then(() => ring.remove(), () => ring.remove());
    } catch (_) {}
  }
  /* a dashed outline takes its ink */
  function ink(bar, rec) {
    if (!rec.ghost) return;
    if (!rec.asSeal) { rec.ghost = false; rec.el.classList.remove('ghost'); if (!reduced()) rec.el.classList.add('in'); return; }
    const svg = sealSvg(rec.item, bar, false), holder = rec.el.querySelector('.lapInk'); if (!svg || !holder) return;
    holder.innerHTML = svg; rec.el.querySelector('.lapSeal').classList.remove('ghost'); rec.ghost = false; land(rec);
  }
  function green(bar, rec) {
    if (rec.el.classList.contains('ok')) return;
    rec.el.classList.remove('off'); rec.el.classList.add('ok');
    ink(bar, rec);
    if (!reduced() && !rec.asSeal) { const m = rec.el.querySelector('.lapMark'); try { m.animate([{ transform: 'scale(1)' }, { transform: 'scale(1.22)', offset: .4 }, { transform: 'scale(1)' }], { duration: 320, easing: 'cubic-bezier(.3,1.5,.5,1)' }); } catch (_) {} }
  }

  /* the missing things, in red */
  /** The window that says which multi-piece orders keep a sheet in its set (charm-nest-shared-orders-modal.js), when the page has it. */
  const sharedWindow = () => !!(window.SharedOrdersModal && typeof window.SharedOrdersModal.open === 'function');
  function openShared(bar, need, from) {
    try {
      const f = bar.opts && bar.opts.onShared;
      if (typeof f === 'function') return f(need, from);
      const p = bar.plan || {};
      return window.SharedOrdersModal.open({ kind: kindOf(p, bar.opts || {}) === 'set' ? 'set' : 'sheet', id: p.id || (p.item && p.item.id) || '', orders: need && Array.isArray(need.items) ? need.items : undefined, targetLabel: destOf(p), targetSetId: p.to && p.to.setId || null, from });
    } catch (e) { warn('shared orders', e); }
    return null;
  }
  /** A need's explanation: short ones sit beside the label, a long one is behind a quiet "Why". */
  let whyN = 0;
  const whyOf = n => {
    const d = String(n && n.detail || '').trim(); if (!d) return '';
    if (d.length <= 70) return ` <span>${esc(d)}</span>`;
    const id = 'lapWhy' + (++whyN);
    return ` <button type="button" class="lapWhyBtn" data-why aria-expanded="false" aria-controls="${id}">Why</button><span class="lapWhySlot"><span class="lapWhyIn"><span class="lapWhy" id="${id}">${esc(d)}</span></span></span>`;
  };
  function renderNeeds(bar, plan, needs) {
    const box = q(bar, '.lapNeeds'); box.hidden = false; if (!reduced()) box.classList.add('enter');
    const list = needs.map((n, ni) => {
      // orders that tie the sheet to others are not a list of red chips: one button opens the window that shows them, with pictures
      if (n.key === 'sharedOrders' && sharedWindow()) return `<li class="lapNeed" data-need="sharedOrders"><div class="lapNeedT"><b>${esc(n.label || n.key || '')}</b>${whyOf(n)}</div><div class="lapItems"><button type="button" class="lapBtn" data-shared>See which orders</button></div></li>`;
      const items = arr(n.items).map(it => { const t = targetOf(it); const idx = bar.items.push({ it, t }) - 1; return { it, t, idx }; });
      const chip = x => {
        const label = x.it.label || (x.t && x.t.how === 'order' ? `Order ${x.t.id}` : x.t && x.t.how === 'sheet' ? 'Sheet' : x.it.id || ''), why = x.it.why ? `<span class="why">${esc(x.it.why)}</span>` : '';
        return x.t ? `<button type="button" class="lapItem" data-item="${x.idx}"><span>${esc(label)}</span>${why}<span class="go" aria-hidden="true">↗</span></button>` : `<span class="lapItem"><span>${esc(label)}</span>${why}</span>`;
      };
      const shown = items.slice(0, SHOW_ITEMS), more = items.slice(SHOW_ITEMS);
      return `<li class="lapNeed" data-need="${esc(n.key || ni)}"><div class="lapNeedT"><b>${esc(n.label || n.key || '')}</b>${whyOf(n)}</div>${items.length ? `<div class="lapItems">${shown.map(chip).join('')}${more.length ? `<span class="lapMoreWrap" hidden>${more.map(chip).join('')}</span><button type="button" class="lapItem lapMore" data-more>Show ${more.length} more</button>` : ''}</div>` : ''}</li>`;
    }).join('');
    box.innerHTML = `<div class="lapNeedsHead">${ALERT}<span>${esc(needsText(plan, bar.opts))}</span></div><ul class="lapNeedList">${list}</ul>`;
  }

  /* the explicit yes */
  function plainRoseSheet(c) {
    if (c.sheetLabel) return String(c.sheetLabel);
    if (Array.isArray(c.sheets) && c.sheets.length) return c.sheets.map(s => s && (s.label || s.sheetLabel || s)).filter(Boolean).join(', ');
    const all = [...new Set([...`${c.label || ''} ${c.detail || ''}`.matchAll(/\b(?:RG|Rose Gold)\s+Sheet\s+[\w-]+/gi)].map(m => m[0]))];
    return all.length ? all.join(', ') : 'This sheet';
  }
  const actionOf = c => c.button || c.action || c.yes || (c.key === 'roseLine' ? 'Add the green dash line' : 'Yes, go ahead');
  function renderConfirm(bar, confirm) {
    const box = q(bar, '.lapConfirm'); box.hidden = false; if (!reduced()) box.classList.add('enter');
    const many = confirm.length > 1;
    box.innerHTML = confirm.map((c, i) => `<div class="lapConfirmItem" data-confirm="${esc(c.key || i)}"><div class="lapCT"><b>${esc(c.label || c.key || '')}</b>${c.detail ? ` <span>${esc(c.detail)}</span>` : ''}</div><div class="lapCBtns"><button type="button" class="lapBtn go" data-yes="${i}"${many ? ' aria-pressed="false"' : ''}>${esc(actionOf(c))}</button>${many ? '' : '<button type="button" class="lapBtn" data-act="cancel">Not now</button>'}</div></div>`).join('')
      + (many ? '<div class="lapCBtns"><button type="button" class="lapBtn go" data-go disabled>Go ahead</button><button type="button" class="lapBtn" data-act="cancel">Not now</button></div>' : '');
    /* Rose Gold has its own last-check bar when that module is here */
    confirm.forEach((c, i) => {
      if (c.key !== 'roseLine' || !window.LibraryFlowRose || typeof LibraryFlowRose.confirmBar !== 'function') return;
      const item = box.querySelector(`[data-confirm="${CSS.escape(String(c.key))}"]`), slot = doc.createElement('div'); slot.className = 'lapRose';
      try {
        item.appendChild(slot);
        /* its own press guard, spinner, drawn line and Try again stay as they are; a press is the explicit yes for roseLine */
        const ctl = LibraryFlowRose.confirmBar(slot, { sheetLabel: plainRoseSheet(c), item: bar.opts.item || bar.plan.item, sheet: bar.opts.sheet, count: c.count,
          onConfirm: () => { if (bar.state === 'failed') reopen(bar); return press(bar, [...bar.picked || [], 'roseLine']); }, onCancel: () => closeIt(bar) });
        if (slot.childNodes.length) { for (const n of [...item.children]) if (n !== slot) n.remove(); bar.rose = ctl || null; if (confirm.length === 1) box.classList.add('lapPlain'); } else { slot.remove(); }
      } catch (e) { warn('rose bar', e); try { slot.remove(); } catch (_) {} }
    });
  }
  function press(bar, keys) {
    if (bar.pressed || bar.ended) return;
    bar.pressed = true;
    const inside = bar.el.contains(doc.activeElement) && doc.activeElement !== bar.el;
    state(bar, 'moving'); tone(bar, '');
    head(bar, q(bar, '.lapTitle').textContent, '');
    q(bar, '.lapConfirm').classList.add('locked');
    for (const r of bar.lines.values()) ink(bar, r);
    if (!roseOn(bar)) busy(bar, bar.opts.moving || 'Moving');
    if (inside) { try { bar.el.focus({ preventScroll: true }); } catch (_) {} }
    let r; try { r = bar.opts.onConfirm && bar.opts.onConfirm(keys.slice()); } catch (e) { warn('onConfirm', e); update(bar.host, { ok: false, error: errText(e) }); return; }
    if (then(r)) r.then(v => { if (bars.get(bar.host) === bar && bar.state === 'moving' && v && typeof v === 'object' && 'ok' in v) update(bar.host, v); }, e => { if (bars.get(bar.host) === bar && bar.state === 'moving') update(bar.host, { ok: false, error: errText(e) }); });
    return r;
  }
  /* the Rose Gold module's own bar is on screen (it shows its own spinner, the drawn line and the saved line) */
  const roseOn = bar => !!(bar.rose && bar.rose.el && bar.rose.el.isConnected);
  /* a failed commit and the Rose bar's own "Try again": the bar asks again */
  function reopen(bar) {
    bar.ended = false; bar.pressed = false; bar.result = null; state(bar, 'plan'); tone(bar, 'wait');
    for (const r of bar.lines.values()) r.el.classList.remove('off');
    for (const sel of ['.lapSummary', '.lapNotes']) { const n = q(bar, sel); n.hidden = true; n.innerHTML = ''; }
    head(bar, q(bar, '.lapTitle').textContent, '');
  }

  /* the buttons */
  function closeIt(bar) {
    if (bar.state === 'plan') { const f = bar.opts.onCancel; try { f && f(); } catch (e) { warn('onCancel', e); } hide(bar.host); return true; }
    if (bar.ended) { const f = bar.opts.onClose; try { f && f(); } catch (e) { warn('onClose', e); } hide(bar.host); return true; }
    return false;
  }
  function onClick(bar, e) {
    const b = e.target.closest && e.target.closest('button'); if (!b || !bar.el.contains(b)) return;
    if (b.dataset.act) { e.preventDefault(); return void closeIt(bar); }
    if (b.hasAttribute('data-why')) { e.preventDefault(); const w = b.nextElementSibling, open = b.getAttribute('aria-expanded') !== 'true'; b.setAttribute('aria-expanded', String(open)); if (w) w.classList.toggle('open', open); return; }
    if (b.hasAttribute('data-shared')) { e.preventDefault(); const n = arr(bar.plan && bar.plan.needs).find(x => x && x.key === 'sharedOrders'); openShared(bar, n, b); return; }
    if (b.hasAttribute('data-more')) { const w = b.parentNode.querySelector('.lapMoreWrap'); if (w) { w.hidden = false; w.style.display = 'contents'; } b.remove(); const nx = w && w.querySelector('button.lapItem'); if (nx) { try { nx.focus({ preventScroll: true }); } catch (_) {} } return; }
    if (b.dataset.item != null) {
      const x = bar.items[+b.dataset.item]; if (!x || !x.t) return;
      const ok = x.t.how === 'order' ? openOrder(b, x.t.id) : openSheet(b, x.t.id);
      if (!ok) { b.disabled = true; b.title = ''; const g = b.querySelector('.go'); if (g) g.textContent = ''; }
      return;
    }
    if (b.dataset.yes != null) {
      const c = bar.plan.confirm[+b.dataset.yes]; if (!c || bar.pressed) return;
      const many = bar.plan.confirm.length > 1, key = String(c.key || b.dataset.yes);
      if (!many) return void press(bar, [key]);
      const on = b.getAttribute('aria-pressed') !== 'true'; b.setAttribute('aria-pressed', String(on));
      bar.picked = bar.picked || new Set(); on ? bar.picked.add(key) : bar.picked.delete(key);
      const go = q(bar, '[data-go]'); if (go) { go.disabled = !bar.picked.size; go.textContent = bar.picked.size ? `Go ahead with ${bar.picked.size}` : 'Go ahead'; }
      return;
    }
    if (b.hasAttribute('data-go')) press(bar, [...(bar.picked || [])]);
  }
  /* Esc is Not now, wherever the focus is on the page, as long as nothing else holds it */
  doc.addEventListener('keydown', e => {
    if (e.key !== 'Escape' || e.defaultPrevented || !live.size) return;
    const t = e.target, loose = !t || t === doc.body || t === doc.documentElement;
    for (const bar of [...live].reverse()) {
      if (!bar.el.isConnected) { live.delete(bar); continue; }
      if (!(bar.state === 'plan' || bar.ended)) continue;
      if (bar.el.contains(t) || (loose && !doc.querySelector('dialog[open]'))) { if (closeIt(bar)) { e.preventDefault(); return; } }
    }
  });

  /* ── a plan arrives ── */
  function render(bar, plan) {
    if (!plan || typeof plan !== 'object') plan = { ok: false, auto: [], needs: [], confirm: [], notes: [], error: 'The move could not be checked.' };
    const needs = arr(plan.needs), confirm = arr(plan.confirm), notes = (Array.isArray(plan.notes) ? plan.notes : []).filter(n => typeof n === 'string' && n.trim());
    const opts = bar.opts, done = Array.isArray(plan.applied) ? arr(plan.applied) : [], blocked = plan.ok === false || needs.length > 0;
    /* the lines are already done (LibraryFlow.approve ran them: a plan with no destination, an "Approve ..." title, a
       plan that says so) or they wait for the commit (a move: the caller commits and calls update) */
    const applied = opts.applied === false ? false : opts.applied === true || plan.applied === true || done.length > 0 || plan.approved === true || plan.mode === 'approve' || plan.kind === 'approve' || /^approv/i.test(String(opts.title || '')) || (!plan.to && !plan.from);
    const auto = arr(plan.auto).length ? arr(plan.auto) : done;
    const asking = !blocked && confirm.length > 0, going = !blocked && !applied && !asking && opts.committing !== false;
    bar.plan = Object.assign({}, plan, { auto, needs, confirm });
    bar.applied = applied;
    bar.mode = blocked ? 'blocked' : asking ? 'asking' : going ? 'going' : applied ? 'applied' : 'info';
    bar.ghost = (blocked || asking) && !applied;
    bar.waiting = false;
    unbusy(bar);
    /* a bar with nothing left to decide (everything is done, or nothing is being committed) has ended: it only closes */
    bar.ended = !blocked && !asking && !going;
    state(bar, going ? 'moving' : bar.ended ? 'done' : 'plan');
    tone(bar, blocked ? 'bad' : asking ? 'wait' : bar.ended ? 'ok' : '');
    head(bar, titleOf(plan, opts), blocked || bar.ended ? 'Close' : '');
    const list = q(bar, '.lapList'); list.innerHTML = '';
    if (bar.ghost && auto.length) { const cap = doc.createElement('p'); cap.className = 'lapCap'; cap.textContent = blocked ? 'Once it can move, these are done for you:' : 'These are done for you once you say yes:'; list.appendChild(cap); }
    const animate = !reduced(), gap = Math.min(GAP, GAP_TOTAL / Math.max(1, auto.length));
    const finish = () => {
      if (applied) for (const r of bar.lines.values()) green(bar, r);
      if (needs.length) renderNeeds(bar, bar.plan, needs);
      if (asking) renderConfirm(bar, confirm);
      else if (confirm.length && blocked) notes.unshift(...confirm.map(c => `Then you will be asked: ${c.label || c.key}`));
      if (plan.error && !needs.length && blocked) notes.unshift(errText(plan.error));
      if (notes.length) { const nb = q(bar, '.lapNotes'); nb.hidden = false; if (animate) nb.classList.add('enter'); nb.innerHTML = notes.map(n => `<span>${esc(n)}</span>`).join(''); }
      if (applied && !asking) summary(bar, { ok: true }, true);
      if (going) busy(bar, opts.moving || 'Moving');
      bar.staggering = false;
      const spoken = [auto.length ? `${plural(auto.length, 'thing')} ${bar.ghost ? 'will be done' : applied ? 'approved' : 'done'} for you.` : '', needs.length ? needsText(plan, opts) + ' ' + needs.map(n => n.label).join('. ') + '.' : '', asking ? confirm.map(c => c.label).join(' ') : ''].filter(Boolean).join(' ');
      if (spoken) say(bar, spoken);
      reveal(bar); focusIn(bar);
      if (bar.pending) { const r = bar.pending; bar.pending = null; later(bar, animate ? 120 : 0, () => apply(bar, r)); }
    };
    bar.staggering = true;
    if (!animate || !auto.length) { auto.forEach(a => addLine(bar, a, { ghost: bar.ghost, ok: applied })); finish(); return; }
    auto.forEach((a, i) => later(bar, 60 + i * gap, () => addLine(bar, a, { ghost: bar.ghost, animate: true, ok: applied })));
    later(bar, 60 + auto.length * gap + 90, finish);
  }
  function summary(bar, result, approvedOnly) {
    const box = q(bar, '.lapSummary'), n = bar.lines ? [...bar.lines.values()].filter(r => r.el.classList.contains('ok')).length : 0;
    const ok = result.ok !== false && !result.error, plan = bar.plan || {}, miss = (plan.needs || []).length;
    let text;
    if (!ok) text = result.applied && result.applied.length ? `The move did not finish. ${n ? plural(n, 'thing') + (n === 1 ? ' was' : ' were') + ' done first.' : ''}`.trim() : 'The move did not finish. Nothing was changed.';
    else if (approvedOnly) text = (n ? things(n) + '.' : 'Nothing needed approving.') + (miss ? ` ${plural(miss, 'thing')} ${miss === 1 ? 'is' : 'are'} still missing.` : '');
    else text = `${movedText(plan)}.${n ? ' ' + things(n) + '.' : ''}`;
    box.className = 'lapSummary ' + (ok ? 'ok' : 'bad') + (reduced() ? '' : ' enter'); box.hidden = false;
    box.innerHTML = (ok ? CHECK : ALERT) + `<span>${esc(text)}</span>`;
    if (ok && !reduced()) box.classList.add('in');
    return text;
  }
  function reveal(bar) {
    try {
      const r = bar.el.getBoundingClientRect(), vh = window.innerHeight || doc.documentElement.clientHeight;
      if (r.top < 84 || r.bottom > vh - 12) bar.el.scrollIntoView({ block: 'nearest', inline: 'nearest', behavior: reduced() ? 'auto' : 'smooth' });
    } catch (_) {}
  }
  function focusIn(bar) {
    if (bar.opts.focus === false || bar.state !== 'plan' || bar.pressed) return;
    const a = doc.activeElement, typing = a && !bar.el.contains(a) && (/^(INPUT|TEXTAREA|SELECT)$/.test(a.tagName || '') || a.isContentEditable);
    if (typing || (a && bar.el.contains(a) && a !== bar.el)) return;
    /* the safe button takes the focus: an Enter right after a drop never says yes */
    const first = q(bar, '.lapConfirm [data-act="cancel"]') || q(bar, '.lapItem[data-item]') || q(bar, '.lapHeadAct button');
    if (first) { try { first.focus({ preventScroll: true }); } catch (_) {} }
  }

  /* ── after the commit ── */
  function apply(bar, result) {
    result = result && typeof result === 'object' ? result : { ok: false, error: 'No answer came back.' };
    const ok = result.ok !== false && !result.error, got = arr(result.applied), keys = new Set(got.map(a => String(a.key)));
    unbusy(bar); bar.pressed = true; bar.ended = true; state(bar, ok ? 'done' : 'failed'); tone(bar, ok ? 'ok' : 'bad');
    bar.el.classList.remove('locked'); q(bar, '.lapConfirm').classList.add('locked');
    for (const r of bar.lines.values()) {
      if (ok || keys.has(r.key)) green(bar, r);
      else { r.el.classList.add('off'); r.el.classList.remove('ok'); }
    }
    /* what was done and was not in the plan (the green dash line a press added) is a line too */
    for (const a of got) {
      if (bar.lines.has(String(a.key)) || /^moved?$/i.test(String(a.key)) || /^moved\b/i.test(String(a.label || ''))) continue;
      const rec = addLine(bar, { key: a.key, label: a.label || a.key, detail: a.detail }, { animate: !reduced(), ok: false });
      green(bar, rec);
    }
    const confirmBox = q(bar, '.lapConfirm'), cap = q(bar, '.lapCap');
    if (!roseOn(bar)) { confirmBox.hidden = true; confirmBox.innerHTML = ''; }
    if (cap) cap.remove();
    if (!ok && result.error) {
      const nb = q(bar, '.lapNotes'); nb.hidden = false; nb.innerHTML = `<span style="color:#8a3a26;font-weight:600">${esc(errText(result.error))}</span>`;
    }
    const text = summary(bar, result, bar.applied || !(bar.plan && bar.plan.to));
    head(bar, q(bar, '.lapTitle').textContent, 'Close');
    say(bar, text + (!ok && result.error ? ' ' + errText(result.error) : ''));
    reveal(bar);
    if (bar.el.contains(doc.activeElement)) { const c = q(bar, '.lapHeadAct button'); if (c && bar.opts.focus !== false) { try { c.focus({ preventScroll: true }); } catch (_) {} } }
    return true;
  }

  /* ── the three calls ── */
  function show(host, plan, opts) {
    try {
      if (!host || typeof host.appendChild !== 'function') return null;
      opts = opts && typeof opts === 'object' ? opts : {};
      const bar = open(host, opts), tok = bar.token;
      if (then(plan)) {
        checking(bar); bar.waiting = true;
        plan.then(p => { if (alive(bar, tok)) render(bar, p); }, e => {
          if (!alive(bar, tok)) return;
          bar.waiting = false; unbusy(bar); bar.ended = true; state(bar, 'failed'); tone(bar, 'bad'); head(bar, titleOf(null, opts), 'Close');
          const box = q(bar, '.lapSummary'); box.hidden = false; box.className = 'lapSummary bad'; box.innerHTML = ALERT + `<span>${esc('The move could not be checked. ' + errText(e))}</span>`; say(bar, 'The move could not be checked.');
        });
      } else if (!plan) checking(bar);
      else render(bar, plan);
      return { el: bar.el, update: r => update(host, r), hide: () => hide(host) };
    } catch (e) { warn('show', e); return null; }
  }
  function update(host, result) {
    try {
      const bar = host && bars.get(host); if (!bar || !bar.el.isConnected) return false;
      if (bar.ended && bar.result === result) return true;
      if (bar.staggering || bar.waiting) { bar.pending = result; return true; }
      bar.result = result;
      return apply(bar, result);
    } catch (e) { warn('update', e); return false; }
  }
  function hide(host) {
    try {
      const bar = host && bars.get(host); if (!bar) return false;
      stop(bar); bars.delete(host); live.delete(bar);
      if (bar.rose) { try { bar.rose.destroy && bar.rose.destroy(); } catch (_) {} bar.rose = null; }
      const el = bar.el;
      if (!el.isConnected) return true;
      if (reduced() || !el.animate) { el.remove(); }
      else {
        el.classList.add('leaving'); el.classList.remove('enter');
        const h = el.getBoundingClientRect().height;
        const a = el.animate([{ height: h + 'px', opacity: 1, marginBottom: '8px', paddingTop: '10px', paddingBottom: '11px' }, { height: '0px', opacity: 0, marginBottom: '0px', paddingTop: '0px', paddingBottom: '0px', borderWidth: '0px' }], { duration: 200, easing: 'cubic-bezier(.2,.8,.2,1)', fill: 'forwards' });
        const gone = () => { try { el.remove(); } catch (_) {} };
        a.finished.then(gone, gone); setTimeout(gone, 400);
      }
      return true;
    } catch (e) { warn('hide', e); return false; }
  }

  window.LibraryApprovalUI = { show, update, hide, busy: (host, label, opts) => show(host, null, Object.assign({}, opts, { checking: label || 'Checking the move' })), version: '20261003-1' };
})();
