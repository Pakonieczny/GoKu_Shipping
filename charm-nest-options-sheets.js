/*  charm-nest-options-sheets.js — the Sheets card of the Options window (SHEETSLIST, Paul, 15 Nov 2026; reworked SHEETSANYWHERE the same day).
 *  SHEETSLIST: "I should be able to easily control in this screen: 1. consolidating sheets by dragging and dropping one sheet onto another. 2. deleting unnecessary sheets and easily being able to choose
 *   which sheet is to be draft 1, then which sheet is to be draft 2 etc. 3. The user should be able to choose sheet 1 2 3 4 in whatever order they decide by dragging the sheets around ..."
 *  SHEETSANYWHERE: "inside the options menu I just see two drafts that are locked ... I wanted the drafts to be part of the partial sheets view so then I can drag the sheets from one sheet to another and amalgamate
 *   sheets as needed, and then I need a way to choose which sheet is gonna be draft one ... I should be able to empty them out. Combine them with other sheets ..." Every sheet is a picture tile (as the partial
 *   sheets are); drag a tile BETWEEN two tiles to put it there, ONTO a tile to combine, "Empty this sheet" to move its pieces onto the others, Delete for an empty one.
 *  On the computer that carries the run the Gate does it (Gate.reorder / consolidate / emptySheet / deleteEmpty); on a computer that only follows the run the same gesture is SENT to the one that carries it
 *  (runCmdPut on the cloud; charm-nest-run-follow.js runCommands does it there and answers with runCmdDone) and the answer is shown here. This file draws and listens; it writes nothing itself.
 *  ONEGRID (Paul, 15 Nov 2026): "I wanted the partial sheets and the unused sheets, the custom sheets all to be at the bottom together and just identified correctly, and I should be able to move already
 *   partially and fully nested sheets onto new nested sheets and all those charms should move over, and then ... drag another partial sheet onto a partial sheet and have those parts be amalgamated,
 *   and then I should be able to delete the sheet that I just dropped onto another sheet if need be, or I might wanna keep it". There is no Sheets card any more: this run's sheets are the FIRST tiles
 *   of the one grid "All <metal> sheets" (the repository's cards follow in the same grid, drawn by charm-nest-options-modal.js). A run sheet dropped on an Available repository card moves all its pieces
 *   onto it (Gate.seatOn); dropped on another run sheet it is combined and KEPT, empty. A request sent to the carrying computer shows what it was, can be cancelled while it waits (runCmdCancel).
 *  MOVESHOW, Paul 15 Nov 2026: "I dragged draft One onto the new empty sheet ... I said yes and nothing happened ... no animation ... no feedback no progress nothing ... I also don't see any ability to choose which sheet I want to be first which sheet I want to be second for overflow":
 *   a run sheet dropped on an Available sheet opens the MOVE PLAN (sticky, top of the grid: the chosen sheets 1st, 2nd, 3rd, numbered on their cards, the engine's own preview words); Move them flies the tile's picture to the 1st sheet, shows a progress
 *   overlay (the engine's step words, seconds) on the tile and the chosen cards, redraws the tile as the nest changes, then a result banner stays until dismissed. A following computer gets the same through the relay (runCmdStep). Combine / Empty get the overlay and banner too.
 *  window.OptionsSheets: mount(M) -> null · place(M) · paint(M) · destroy(M) */
(function (root) {
  'use strict';
  const doc = root.document; if (!doc || root.OptionsSheets) return;
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const G = () => root.Gate && typeof root.Gate.sheetsOf === 'function' ? root.Gate : null;
  const solid = m => m === 'gold10k' || m === 'gold14k';
  const NO_CARRIER = 'No computer is carrying the run right now: open the app on the computer that runs it, or press Resume here to carry it on this computer.';
  const CSS = `
.osShWrap{display:contents}
.ohcGrid:has(.osShWrap){grid-template-columns:repeat(auto-fill,minmax(min(100%,300px),1fr));gap:14px}
.ohcGrid:has(.osShWrap) .ohcBig{grid-column:1/-1}
.osShHead2{margin:6px 0 10px}.osShHead2 h4{margin:0 0 2px;font:500 20px var(--serif,inherit)}
.osKind{display:inline-block;align-self:flex-start;font-size:10.5px;font-weight:700;letter-spacing:.07em;text-transform:uppercase;padding:2px 9px;border-radius:99px;border:1px solid var(--line);color:var(--ink45,#777);background:var(--card2,transparent)}
.osKind.nest{border-color:var(--gold2,#b8913f);color:#7a5a1a}.osKind.empty{border-color:#8a8579;color:#5b554c}
.ohc>.osKind{margin:-6px 0 10px}
.ohc.onto{outline:3px solid var(--gold2,#b8913f);outline-offset:1px}.ohc.onto.refused{outline-color:#b3402e}
.osShWait{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
.osSh{position:relative;min-width:0;box-sizing:border-box;display:flex;flex-direction:column;gap:8px;padding:12px;border:1px solid var(--line);border-radius:14px;background:var(--paper2,transparent);outline:none;transition:box-shadow .14s,border-color .14s,transform .14s;touch-action:pan-y}
.osSh:focus-visible{box-shadow:0 0 0 2px var(--gold2)}
.osSh.fixed{background:repeating-linear-gradient(135deg,transparent 0 9px,rgba(120,110,90,.06) 9px 18px)}
.osShPic{position:relative;width:100%;max-width:340px;margin:0 auto;border-radius:8px;background:#fffefb;border:1px solid var(--line);overflow:hidden;cursor:grab;display:grid;place-items:center}
.osSh.fixed .osShPic{cursor:default}
.osShPic canvas,.osShPic img{display:block;width:100%;height:100%;object-fit:contain;pointer-events:none;-webkit-user-drag:none}
.osShPic .osShNone{font-size:11.5px;color:var(--ink45,#888);padding:6px;text-align:center}
.osShHead{display:flex;align-items:baseline;justify-content:space-between;gap:8px}
.osShName{font:500 20px var(--serif,inherit);line-height:1.15}
.osShMeta{font-size:12.5px;color:var(--ink45,#777)}
.osShBadges{display:flex;gap:6px;flex-wrap:wrap;min-height:0}
.osShBadge{font-size:11px;padding:2px 8px;border-radius:99px;border:1px solid var(--line);color:var(--ink45,#777)}
.osShBadge.lock{border-color:#b3402e;color:#b3402e}
.osShBar{height:4px;border-radius:3px;background:var(--line);overflow:hidden}
.osShBar i{display:block;height:100%;background:var(--gold2,#b8913f)}
.osShNote{font-size:12px;color:var(--ink45,#777)}
.osShNote.bad{color:#b3402e}
.osShActs{display:flex;gap:6px;flex-wrap:wrap}
.osSh.dragging{opacity:.92;z-index:5;box-shadow:0 14px 34px rgba(30,24,16,.25);pointer-events:none;transition:none}
.osSh.onto{border-color:var(--gold2);box-shadow:0 0 0 3px var(--gold2)}
.osSh.onto.refused{border-color:#b3402e;box-shadow:0 0 0 3px #b3402e}
.osSh.before::before,.osSh.after::after{content:"";position:absolute;top:6px;bottom:6px;width:4px;border-radius:2px;background:var(--gold2)}
.osSh.before::before{left:-11px}.osSh.after::after{right:-11px}
.osShTag{position:absolute;left:50%;top:38%;transform:translateX(-50%);max-width:92%;text-align:center;font-size:12px;font-weight:600;padding:4px 11px;border-radius:99px;background:var(--gold2,#b8913f);color:#fff;pointer-events:none;z-index:2;box-shadow:0 4px 14px rgba(0,0,0,.25)}
.osShTag.refused{background:#b3402e}
.osShAsk{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin:8px 0;font-size:13px}
.osShSaid{margin:8px 0;font-size:13px}
.osShSaid.bad{color:#b3402e}
.osShSpin{display:inline-block;width:11px;height:11px;margin-right:7px;vertical-align:-1px;border:2px solid var(--line);border-top-color:var(--gold2,#b8913f);border-radius:50%;animation:osShSpin .8s linear infinite}
@keyframes osShSpin{to{transform:rotate(360deg)}}
html.osShDrag,html.osShDrag *{cursor:grabbing!important;user-select:none!important}
/* MOVESHOW, Paul 15 Nov 2026: the move plan, the numbered badges, the progress overlay, the flying picture, the result banner */
.osPlanHost{display:contents}.osPlanHost[hidden]{display:none}
.osPlan,.osBanner{grid-column:1/-1;box-sizing:border-box;border:2px solid var(--gold2,#b8913f);border-radius:14px;background:var(--paper,#fffefb);padding:12px 16px;box-shadow:0 8px 26px rgba(30,24,16,.18)}
.osPlan{position:sticky;top:0;z-index:7}
.osBanner{border-width:1px;display:flex;gap:14px;align-items:flex-start;justify-content:space-between;font-size:14px;line-height:1.45}.osBanner.bad{border-color:#b3402e;color:#b3402e}
.osBanner b{display:block;margin-bottom:2px}
.osPlanHead{font:500 18px var(--serif,inherit);margin:0 0 6px}
.osPlanList{list-style:none;margin:0 0 8px;padding:0;display:flex;flex-direction:column;gap:5px}
.osPlanList li{display:flex;align-items:center;gap:10px;flex-wrap:wrap;padding:4px 8px;border:1px solid var(--line);border-radius:10px;background:var(--card2,transparent)}
.osPlanN{min-width:34px;font-weight:700;color:#7a5a1a}.osPlanTake{font-size:12.5px;color:var(--ink45,#777)}.osPlanSp{flex:1}
.osPlanHelp,.osPlanWords,.osPlanRest,.osPlanNote{margin:4px 0;font-size:13px;line-height:1.4}.osPlanHelp{color:var(--ink45,#777)}.osPlanNote,.osPlanWords.bad{color:#b3402e}
.osPlanBtns{display:flex;gap:8px;justify-content:flex-end;margin-top:6px}
.ohc.osChosen{position:relative;outline:3px solid var(--gold2,#b8913f);outline-offset:1px}
.osBadge{position:absolute;right:10px;top:10px;font-family:system-ui,-apple-system,'Segoe UI',sans-serif!important;z-index:3;min-width:38px;height:38px;padding:0 8px;box-sizing:border-box;border-radius:99px;display:grid;place-items:center;background:var(--gold2,#b8913f);color:#fff;font-size:21px;font-weight:700;line-height:1;box-shadow:0 3px 10px rgba(0,0,0,.3);pointer-events:none}
.osFreedTag{display:block;margin:-6px 0 8px;padding:3px 10px;border-radius:99px;background:#fff3d6;border:1px solid var(--gold2,#b8913f);color:#7a5a1a;font-size:12px;font-weight:700;align-self:flex-start}
.ohc.osFreed{box-shadow:0 0 0 2px var(--gold2,#b8913f)}
.osOv{position:absolute;inset:0;z-index:6;border-radius:inherit;background:rgba(255,254,251,.9);-webkit-backdrop-filter:blur(3px);backdrop-filter:blur(3px);display:flex;flex-direction:column;justify-content:center;gap:7px;padding:14px;box-sizing:border-box;text-align:center;font-size:13px}
.osOv b{font:500 17px var(--serif,inherit)}
.osOv>*{max-width:420px;width:100%;margin-left:auto;margin-right:auto}
.osOvBar{display:block;height:8px;border-radius:5px;background:var(--line);overflow:hidden;position:relative}.osOvBar i{display:block;height:100%;width:0;background:var(--gold2,#b8913f);transition:width .4s}
.osOvBar.ind i{width:35%;position:absolute;animation:osOvInd 1.4s ease-in-out infinite}
@keyframes osOvInd{0%{left:-35%}100%{left:100%}}
.osOvSec{font-size:12px;color:var(--ink45,#777)}
.osGhost{position:fixed;z-index:2000;pointer-events:none;border-radius:8px;background:#fffefb;border:2px solid var(--gold2,#b8913f);box-shadow:0 12px 30px rgba(30,24,16,.35);transform-origin:center;transition:transform .6s cubic-bezier(.3,.7,.2,1),opacity .6s;overflow:hidden}
.osGhost img{display:block;width:100%;height:100%;object-fit:contain}
.osShOn{font-size:12.5px;color:#7a5a1a;font-weight:600}
.osSh.osShPulse{animation:osShPulse 1.1s ease-out 1}
@keyframes osShPulse{0%{box-shadow:0 0 0 0 rgba(184,145,63,.9)}60%{box-shadow:0 0 0 14px rgba(184,145,63,0)}100%{box-shadow:0 0 0 0 rgba(184,145,63,0)}}
@media (prefers-reduced-motion:reduce){.osGhost{transition:none}.osSh.osShPulse{animation:none;box-shadow:0 0 0 3px var(--gold2,#b8913f)}.osOvBar.ind i{animation:none;width:100%}.osOvBar i{transition:none}}`;
  const FREED = new Set();   // (MOVESHOW) the physical sheets freed by a move, for this window session: their repository card says so
  const reduced = () => { try { return !!(root.matchMedia && root.matchMedia('(prefers-reduced-motion: reduce)').matches); } catch (_) { return false; } };
  let styled = false;
  const style = () => { if (styled) return; styled = true; const s = doc.createElement('style'); s.textContent = CSS; doc.head.appendChild(s); };
  const RF = () => root.CharmNestRunFollow || null;
  const api = body => root.CN && typeof root.CN.api === 'function' ? root.CN.api('charmNestLibrary', body, { quiet: true }) : Promise.reject(new Error('no connection'));
  const devId = () => { try { return RF().dev(); } catch (_) { return ''; } };
  const who = () => { try { return String((root.B && root.B.employee) || '').trim(); } catch (_) { return ''; } };

  const NAMES = { gold10k: '10K Gold', gold14k: '14K Gold' };
  /** The state is made here; the pieces (the head above the grid, the tiles inside the grid) are put in place by place(), after every draw of the repository. */
  function mount(M) {
    if (!M || !solid(M.m) || !G()) return null;
    style();
    const head = doc.createElement('div'); head.className = 'osShHead2'; head.dataset.card = 'sheets';
    head.innerHTML = `<h4 data-sh="title"></h4><p class="osLead" data-sh="lead"></p><div class="osShAsk" data-sh="ask" hidden></div><p class="osShSaid" data-sh="said" role="status" hidden></p>`;
    const planEl = doc.createElement('div'); planEl.hidden = true; planEl.className = 'osPlanHost'; planEl.dataset.sh = 'plan';   // (MOVESHOW: the plan and the result banner ride at the top of the grid)
    const bannerEl = doc.createElement('div'); bannerEl.hidden = true; bannerEl.className = 'osPlanHost'; bannerEl.dataset.sh = 'banner';
    const wrap = doc.createElement('div'); wrap.className = 'osShWrap'; wrap.dataset.sh = 'list';
    const solo = doc.createElement('div'); solo.className = 'osCards ohcGrid';
    M.sheetsUI = { el: head, wrap, solo, sig: '', drag: null, ask: null, note: {}, rel: null, say: null, sub: null, poll: 0, planEl, bannerEl, plan: null, move: null, banner: null, tick: 0, pulse: '' };
    wrap.addEventListener('pointerdown', e => down(M, e));
    wrap.addEventListener('keydown', e => key(M, e));
    wrap.addEventListener('click', e => {
      const d = e.target.closest && e.target.closest('[data-sh-del]'); if (d) return del(M, d.closest('.osSh').dataset.id);
      const m = e.target.closest && e.target.closest('[data-sh-empty]'); if (m) emptyAsk(M, m.closest('.osSh').dataset.id);
    });
    head.addEventListener('click', e => { if (e.target.closest && e.target.closest('[data-sh-cancel]')) cancelRel(M); if (e.target.closest && e.target.closest('[data-sh-retry]') && M.sheetsUI.rel && M.sheetsUI.rel.retry) retryRel(M, M.sheetsUI.rel.retry); });
    planEl.addEventListener('click', e => planClick(M, e)); bannerEl.addEventListener('click', e => { if (e.target.closest && e.target.closest('[data-sh-dismiss]')) { M.sheetsUI.banner = null; renderBanner(M); } if (e.target.closest && e.target.closest('[data-sh-retry]') && M.sheetsUI.banner && M.sheetsUI.banner.retry) retryRel(M, M.sheetsUI.banner.retry); });
    planEl.addEventListener('keydown', e => { if (e.key === 'Escape' && M.sheetsUI.plan) { e.stopPropagation(); planCancel(M); } });
    return null;
  }
  const gridOf = M => M.sheetsUI.wrap.parentNode;
  /** Put the head above the results and the tiles first in the grid (the repository's own grid when it has cards, else a grid of their own). Called after every draw of the repository. */
  function place(M) {
    const S = M && M.sheetsUI; if (!S || !M.source) return;
    const res = M.source.querySelector('[data-os="results"]'), host = M.host; if (!res) return;
    if (S.el.parentNode !== res.parentNode || S.el.nextSibling !== res) res.parentNode.insertBefore(S.el, res);
    const lay = box => { if (S.bannerEl.parentNode !== box || box.firstChild !== S.bannerEl || S.planEl.previousSibling !== S.bannerEl || S.wrap.previousSibling !== S.planEl) { box.prepend(S.wrap); box.prepend(S.planEl); box.prepend(S.bannerEl); } };   // (MOVESHOW: the banner, the plan, then the tiles)
    if (host && host.parentNode === res) { if (S.solo.parentNode) S.solo.remove(); lay(host); }
    else { if (S.solo.parentNode !== res || res.firstChild !== S.solo) res.prepend(S.solo); lay(S.solo); }
    const grid = gridOf(M); if (grid) {   // (MOVESHOW) the banner and the plan first in the grid, a click on a card while a plan is open chooses it
      if (!grid._osPlanOn) { grid._osPlanOn = true; grid.addEventListener('click', e => cardClick(M, e), true); }
    }
    paint(M); decorate(M);
  }
  // (INSTANT, Paul 23 Nov 2026: the order dropped and the empty sheet deleted are shown at the press (S.order, S.gone), until the cloud has answered; refused, they go back)
  const rowsOf = M => {
    let l; try { l = G().sheetsOf(M.m); } catch (_) { return []; }
    const S = M.sheetsUI || {}, at = new Map((S.order || []).map((id, i) => [id, i])), n = r => (at.has(r.id) ? at.get(r.id) : 1e6);
    return S.order || S.gone ? l.filter(r => r.id !== S.gone).sort((a, b) => (S.order ? n(a) - n(b) : 0)) : l;
  };
  const relayed = M => { const r = rowsOf(M); return !!(r.length && r[0].relay); };

  function pictures(S, rows) {
    for (const r of rows) {
      const box = S.wrap.querySelector(`[data-pic="${CSS_ESC(r.id)}"]`); if (!box) continue;
      if (r.live) {
        const cv = doc.createElement('canvas'), pg = G().pageOf && G().pageOf(r.id), C = root.CN;
        try { cv.width = Math.round(240 * Math.min(2, root.devicePixelRatio || 1)); cv.height = Math.max(20, Math.round(cv.width / (r.aspect || 2))); C.paintPreview(cv, pg, true, 0); box.innerHTML = ''; box.appendChild(cv); } catch (_) { box.innerHTML = '<span class="osShNone">picture not ready</span>'; }
      } else if (r.picture) {
        const img = doc.createElement('img'); img.alt = r.name; img.decoding = 'async'; img.loading = 'lazy'; img.onerror = () => { box.innerHTML = '<span class="osShNone">picture not available</span>'; }; img.crossOrigin = 'anonymous'; box.innerHTML = ''; box.appendChild(img);   /* (the page is cross-origin isolated: a picture from the cloud's storage is loaded through the app's own loader, as the sheet cards do; a plain src is blocked) */ try { const A = root.CharmNestAssets; if (A && typeof A.sheetImage === 'function') Promise.resolve(A.sheetImage(img, r.id, r.picture)).catch(() => img.onerror()); else img.src = r.picture; } catch (_) { img.src = r.picture; }
      } else box.innerHTML = '<span class="osShNone">' + (r.pieces ? 'picture loading…' : 'empty sheet') + '</span>';
    }
  }
  const CSS_ESC = v => String(v).replace(/["\\]/g, '\\$&');

  const ST = () => (root.OptionsStudio && typeof root.OptionsStudio.state === 'function' ? root.OptionsStudio.state().all : null) || {};
  const cardInfo = id => { try { return root.OptionsStudio && OptionsStudio.cardOf ? OptionsStudio.cardOf(id) : null; } catch (_) { return null; } };
  const mm1 = v => { const n = Math.round(+v * 10) / 10; return Number.isFinite(n) && n > 0 ? String(n) : '?'; };   // (MOVESHOW polish: the size as the sheet's own card writes it, 50.5 not 51)
  const cardLabel = c => { if (!c) return 'that sheet'; const w = c.wMm != null ? c.wMm : c.bboxMm ? c.bboxMm.w : c.sheetWMm, h = c.hMm != null ? c.hMm : c.bboxMm ? c.bboxMm.h : c.sheetHMm; return `${c.kind === 'new' ? 'new sheet' : 'partial sheet'} ${mm1(w)} × ${mm1(h)} mm`; };   // (MOVESHOW: a partial sheet's size from its outline too)
  const plural = n => `${n} piece${n === 1 ? '' : 's'}`;
  const cap = t => t ? t.charAt(0).toUpperCase() + t.slice(1) : t;
  /** (MOVESHOW) the physical sheet a tile lies on, in words ("New sheet 55 × 38 mm"); '' when the sheet has none */
  const onWords = (r) => {
    if (!r.stockId) return '';
    const its = (ST().items || []).filter(x => (x.stockId || x.id) === r.stockId).sort((a, b) => (+b.revision || 0) - (+a.revision || 0)), c = its[0];
    return c ? cap(cardLabel(c)) : `a ${r.size} sheet`;
  };
  const LATE_WORDS = 'The cloud has not done it yet — it is still queued; check again or press Try again', LATE_MS = 300000;   // (NOWCMD)
  /** NOWCMD: a failure or a late answer of the cloud is also said in a toast, with Try again (the same request sent again from this grid) */
  const shout = (M, rel, res) => { try { if (root.CNCmdNow && root.CNCmdNow.shout) root.CNCmdNow.shout(Object.assign({ runId: rel.runId, id: rel.id }, res), { label: rel.label || 'Sheets', onRetry: () => retryRel(M, rel) }); } catch (_) {} };
  const ago = at => { const m = Math.floor((Date.now() - at) / 60000); return m < 1 ? 'just now' : m === 1 ? '1 min ago' : m + ' min ago'; };
  const relWords = r => {
    if (!r.sent) return r.text;
    /* NOWCMD (Paul 21 Nov 2026: "if the cloud does not properly update this and there's a mistake the user will never know"): the cloud takes a person's request up within seconds; the line
       says what it is doing until it answers, never "nothing more is needed"; after 5 minutes with no answer it says so plainly, in red, with Try again */
    if (r.cloud) return r.late ? `${r.label || 'A request'} — ${LATE_WORDS}` + (r.warn ? '. ' + r.warn : '') : `${r.label || 'A request'} — ${r.step ? 'the cloud is doing it… ' + r.step : Date.now() - r.at > 60000 ? `the cloud has it queued — it has not started yet (asked ${ago(r.at)})…` : 'the cloud has it — it starts within seconds…'}` + (r.warn ? '. ' + r.warn : '');   // (G9: in mode "on" the cloud does the request: no computer is named)
    return `${r.label || 'A request'} — sent to ${r.name} ${ago(r.at)}, waiting for that computer` + (r.warn ? '. ' + r.warn : '') + (Date.now() - r.at > 60000 ? '. That computer has not answered: check Brites Manager is open on it (reload it once if it was open since before today\'s update)' : '');
  };

  function paint(M, force) {
    const S = M && M.sheetsUI; if (!S || !G()) return;
    if (S.drag || S.ask) return;
    try { if (!relayed(M) && G().ensureOrder) G().ensureOrder(M.m); } catch (_) {}   // (ORDERCLOUD: the first time the grid shows on the carrier, the order is written once so every computer numbers the drafts alike)
    const f = ST().metal, hide = !!f && f !== M.m, rows = hide ? [] : rowsOf(M);
    const said = G().sheetsSaid ? G().sheetsSaid(M.m) : null, showSaid = S.rel || S.say ? '' : said && Date.now() - said.at < 600000 ? said.text : '';
    const ons = rows.map(r => onWords(r)), sig = JSON.stringify([rows, ons, showSaid, S.note, S.rel, S.say, S.rel && S.rel.spin ? Math.floor((Date.now() - S.rel.at) / 15000) : 0, hide, M.m]);
    if (!force && sig === S.sig) return; S.sig = sig;
    S.el.querySelector('[data-sh="title"]').textContent = `All ${NAMES[M.m] || ''} sheets`;
    const arrange = rows.length ? rows[0].arrange : true;
    S.el.querySelector('[data-sh="lead"]').textContent = arrange ? 'This run\'s sheets come first, then the sheets in the repository. Drag a run sheet between two run sheets to reorder, onto another run sheet to combine (the one you drop stays, empty), or onto an Available sheet to move all its pieces onto it.' : 'There is no open run to arrange sheets in.';
    const keepFocus = S.wrap.contains(doc.activeElement) && doc.activeElement.closest ? (doc.activeElement.closest('.osSh') || {}).dataset : null;
    S.wrap.innerHTML = rows.map((r, ri) => {
      const badges = [r.inSet ? 'In set' : '', r.cutLine ? 'Cut line drawn' : '', r.cut ? 'Cut' : '', r.locked ? 'Locked' : ''].filter(Boolean).map(b => `<span class="osShBadge${b === 'Locked' ? ' lock' : ''}">${b}</span>`).join('');
      const empty0 = !r.pieces, why = S.note[r.id] || (r.locked ? r.lockWhy : !r.canDelete && r.whyDelete && /^Move its/.test(r.whyDelete) ? '' : !r.movable && r.arrange ? (r.whySource || r.whyTarget || '') : '');
      const empty = r.arrange && !r.locked && r.pieces > 0 && r.canSource;
      const pieces = `${plural(r.pieces)}${r.pct == null ? '' : ' · ' + r.pct + '% full'}${r.busy ? ' · working…' : ''}`;
      const kind = empty0 ? '<span class="osKind empty">Empty sheet · in this run</span>' : '<span class="osKind nest">Nested · in this run</span>';
      const keptNote = empty0 && r.arrange && !r.locked ? '<span class="osShNote">Kept: new pieces can be nested on it. Drop another run sheet on it to move pieces here.</span>' : '';
      return `<div class="osSh${r.movable ? '' : ' fixed'}" data-id="${esc(r.id)}" tabindex="0" aria-label="${esc(r.name)}, nested in this run, ${r.pieces} pieces${r.pct == null ? '' : ', ' + r.pct + ' percent full'}${r.locked ? ', locked: ' + esc(r.lockWhy) : ''}. Alt plus Up or Down arrow moves it.">
        ${kind}
        <div class="osShPic" data-pic="${esc(r.id)}" style="aspect-ratio:${r.aspect || 2}" title="${r.movable ? 'Drag to move or combine' : esc(why || 'This sheet keeps its place')}"></div>
        <div class="osShHead"><span class="osShName">${esc(r.name)}</span></div>
        <span class="osShMeta">${esc(r.size)}<br>${pieces}</span>
        ${ons[ri] ? `<span class="osShOn" data-sh-on>on ${esc(ons[ri])}</span>` : ''}
        <span class="osShBar" aria-hidden="true"><i style="width:${r.pct == null ? 0 : r.pct}%"></i></span>
        <span class="osShBadges">${badges}</span>
        ${why ? `<span class="osShNote${S.note[r.id] ? ' bad' : ''}">${esc(why)}</span>` : keptNote}
        <span class="osShActs">${empty ? '<button type="button" class="btn ghost xs" data-sh-empty>Empty this sheet</button>' : ''}${r.canDelete ? '<button type="button" class="btn ghost xs" data-sh-del>Delete this sheet</button>' : ''}</span></div>`;
    }).join('');
    pictures(S, rows);
    const sd = S.el.querySelector('[data-sh="said"]'), cur = S.rel || S.say, text = cur ? (S.rel ? relWords(S.rel) : S.say.text) : showSaid;
    sd.hidden = !text; sd.classList.toggle('bad', !!(cur && cur.bad));
    sd.innerHTML = (cur && cur.spin ? '<span class="osShSpin" aria-hidden="true"></span>' : '') + esc(text) + (S.rel && S.rel.retry ? ' <button type="button" class="btn xs" data-sh-retry>Try again</button>' : '') + (S.rel && S.rel.sent && S.rel.spin ? ' <button type="button" class="btn ghost xs" data-sh-cancel>Cancel this request</button>' : '');   // (NOWCMD: Try again on a cloud failure or a late answer)
    if (keepFocus && keepFocus.id) { const n = [...S.wrap.querySelectorAll('.osSh')].find(x => x.dataset.id === keepFocus.id); if (n) { try { n.focus({ preventScroll: true }); } catch (_) {} } }
    decorate(M);
    if (S.pulse) { const n = [...S.wrap.querySelectorAll('.osSh')].find(x => x.dataset.id === S.pulse); if (n) { n.classList.add('osShPulse'); setTimeout(() => n.classList.remove('osShPulse'), 1400); } S.pulse = ''; }
  }
  const fail = (M, id, why) => { const S = M.sheetsUI; if (!S) return; S.note[id] = why; paint(M, true); setTimeout(() => { if (S.note[id] === why) { delete S.note[id]; paint(M, true); } }, 6000); };
  const sayFor = (M, say, ms) => { const S = M.sheetsUI; if (!S) return; S.say = say; paint(M, true); if (say && !say.spin) setTimeout(() => { if (S.say === say) { S.say = null; paint(M, true); } }, ms || 90000); };

  /* ── the computer that only follows the run: the gesture is sent to the one that carries it ── */
  function stopRel(S) { if (S.poll) { clearInterval(S.poll); S.poll = 0; } if (S.sub) { try { S.sub.stop(); } catch (_) {} S.sub = null; } }
  const unshow = (M, bad) => { const S = M.sheetsUI, o = S.order, g = S.gone, off = () => { if (S.order === o && S.gone === g) { S.order = S.gone = null; paint(M, true); } }; if (bad) off(); else setTimeout(off, 10000); };
  const endRel = (M, rel) => { const S = M.sheetsUI; stopRel(S); S.rel = rel; unshow(M, !!(rel && rel.bad)); paint(M, true); const q = S.next && S.next.shift(); if (q) { send(M, ...q); return; } if (rel && rel.keep) return; setTimeout(() => { if (S.rel === rel) { S.rel = null; paint(M, true); } }, 90000); };   // (NOWCMD: a cloud failure stays, red, with its Try again)
  /** NOWCMD · Try again: the same request sent again (one still waiting is cancelled first, so it is never done twice). */
  async function retryRel(M, rel) {
    const S = M.sheetsUI; if (!S || !rel || !rel.cmd) return false;
    if (rel.id && rel.spin) { try { await api({ op: 'runCmdCancel', runId: rel.runId, id: rel.id, dev: devId() }); } catch (_) {} }
    if (S.rel === rel || (S.rel && !S.rel.spin) || (S.rel && S.rel.retry === rel)) { stopRel(S); S.rel = null; }
    if (S.banner && S.banner.retry) { S.banner = null; renderBanner(M); }
    return send(M, rel.cmd, rel.label);
  }
  async function readRel(M) {
    const S = M.sheetsUI; if (!S || !S.rel || !S.rel.id || !S.rel.spin) return;
    const rel = S.rel; let res = null; try { res = await api({ op: 'runCmdList', runId: rel.runId }); } catch (_) { return; }
    const c = ((res && res.cmds) || []).find(x => x && x.id === rel.id);
    if (S.rel !== rel) return;
    const mv = S.move && S.move.rid === rel.id ? S.move : null;   // (MOVESHOW: the move this request belongs to)
    if (c && (c.state === 'done' || c.state === 'failed')) {
      const bad = c.state === 'failed', text = c.text || (bad ? (rel.cloud ? 'The cloud could not do that.' : 'The computer that carries the run could not do that.') : 'Done');
      if (bad && rel.cloud) shout(M, rel, { failed: true, text });   // (NOWCMD: a toast too, with Try again)
      if (mv) { stopRel(S); S.rel = null; return void finishMove(M, mv, { text, bad, freed: !bad && /back in the repository as Available/.test(text) ? (c.freed || true) : false, retry: bad && rel.cloud ? rel : null }); }
      endRel(M, bad && rel.cloud ? { text: `${rel.label || 'The request'}: not done — ${String(text).replace(/\.$/, '')}`, bad, spin: false, keep: true, retry: rel } : { text, bad, spin: false });
      if (rel.kind === 'seat' && !bad) { try { root.OptionsStudio && OptionsStudio.reload && OptionsStudio.reload(); } catch (_) {} }
    } else if (c && c.state === 'cancelled') { if (mv) endMove(M, mv); endRel(M, { text: `Cancelled: ${rel.label || 'the request'}. Nothing was done.`, bad: false, spin: false }); }
    else if (rel.cloud && Date.now() - rel.at > LATE_MS && Date.now() - rel.at < 6 * 3600000) {   // (NOWCMD: no answer after 5 minutes: said in red, with Try again; still followed, and its answer shown when it comes)
      if (c && c.step) rel.step = c.step;
      if (!rel.late) { rel.late = true; rel.bad = true; rel.retry = !c || c.state === 'waiting' ? rel : null; shout(M, rel, { late: true, queued: !c || c.state === 'waiting', text: LATE_WORDS }); }
      paint(M, true);
    }
    else if (Date.now() - rel.at > (rel.cloud ? 6 * 3600000 : 11 * 60000)) { if (mv) endMove(M, mv); endRel(M, { text: rel.cloud ? 'The cloud did not answer. Look at the sheets again, then try once more.' : 'The computer that carries the run did not answer. Look at the sheets again, then try once more.', bad: true, spin: false }); }
    else {
      if (rel.cloud && c && c.step) rel.step = c.step;   // (NOWCMD: the cloud's own step on the line)
      if (mv && c && c.state === 'running') { rel.running = true; if (c.step) { mv.step = { text: `${rel.cloud ? 'The cloud' : rel.name + "'s computer"}: ${c.step}`, pct: Number.isFinite(+c.stepPct) ? +c.stepPct : null }; updateOv(M); if (!mv.relayPic || mv.relayPic !== c.step) { mv.relayPic = c.step; livePic(M, mv); } } }
      paint(M, !!mv);
    }
  }
  async function cancelRel(M) {
    const S = M.sheetsUI, r = S && S.rel; if (!r || !r.sent || !r.spin) return;
    const started = r.cloud ? 'It has already started in the cloud, so it cannot be cancelled' : 'It has already started on that computer, so it cannot be cancelled';
    if (r.running) { r.warn = started; return paint(M, true); }
    let res = null; try { res = await api({ op: 'runCmdCancel', runId: r.runId, id: r.id, dev: devId() }); } catch (e) { r.warn = 'It could not be cancelled (' + ((e && e.message) || e) + ')'; return paint(M, true); }
    if (S.rel !== r) return;
    if (res && res.cancelled) endRel(M, { text: `Cancelled: ${r.label || 'the request'}. Nothing was done.`, bad: false, spin: false });
    else { r.warn = started; paint(M, true); readRel(M); }
  }
  async function send(M, cmd, label, mv) {
    const S = M.sheetsUI; if (!S) return false;
    if (S.rel && S.rel.spin) { (S.next || (S.next = [])).push([cmd, label, mv]); return true; }   // (INSTANT: asked after the one in hand, never refused)
    const rows = rowsOf(M), runId = (rows[0] && rows[0].runId) || (RF() && RF().state().runId), dev = devId();
    if (!runId || !dev) { S.rel = { text: 'There is no open run to send this to.', bad: true, spin: false }; paint(M, true); return false; }
    const onMode = !!(root.CNViewer && root.CNViewer.on());
    S.rel = { text: onMode ? 'Asking the cloud…' : 'Finding the computer that carries the run…', spin: true, runId, at: Date.now() }; paint(M, true);
    try {
      const c = await api({ op: 'runCarrier', runId, dev, check: true });
      if (!onMode && (!c || !c.alive || c.mine)) { S.rel = { text: NO_CARRIER, bad: true, spin: false }; paint(M, true); return false; }   // (CLOUDDRIVE build step 14: in mode "on" the cloud carries every run and does the request: it is always sent)
      const cloud = onMode || !!(c && c.carrier && c.carrier.dev === 'cloud'), name = cloud ? 'the cloud' : (c && c.carrier && c.carrier.by) || 'the computer that carries the run';   // (G9: the cloud is named as the cloud, never "Cloud's computer")
      const put = await api({ op: 'runCmdPut', runId, cmd: Object.assign({ metal: M.m, label }, cmd), by: who(), dev });
      if (!put || put.error || !put.id) throw new Error((put && put.error) || 'not sent');
      S.rel = { id: put.id, runId, name, cloud, at: Date.now(), spin: true, sent: true, label, kind: cmd.type, cmd: Object.assign({}, cmd) }; if (mv) { mv.rid = put.id; mv.step = { text: cloud ? 'Waiting for the cloud…' : `Waiting for ${name}'s computer…` }; updateOv(M); } paint(M, true);
      if (root.CNSync && typeof root.CNSync.follow === 'function' && !S.sub) { try { S.sub = root.CNSync.follow({ name: 'run-cmds', kinds: 'r', wants: k => k === 'r:' + runId, read: () => readRel(M) }); } catch (_) { S.sub = null; } }
      if (!S.poll) S.poll = setInterval(() => readRel(M), mv ? 4000 : 8000);   // (MOVESHOW: a move is looked at every 4 s as well as when the feed names the run)
      readRel(M); return true;
    } catch (e) { S.rel = { text: 'Not sent: ' + ((e && e.message) || e), bad: true, spin: false }; paint(M, true); return false; }
  }

  /* ── the gestures: the Gate here, or a command sent to the computer that carries the run ── */
  async function doReorder(M, ids, id, label) {
    if (relayed(M)) { M.sheetsUI.order = ids.slice(); paint(M, true); if (!(await send(M, { type: 'reorder', ids }, label || 'Change the order of the sheets'))) unshow(M, true); return; }
    const r = G().reorder(M.m, ids); if (r && r.ok === false) fail(M, id, r.why); else paint(M, true);
  }
  async function doCombine(M, srcId, tgtId, label) {
    const S = M.sheetsUI, mv = startMove(M, { id: srcId, tiles: [srcId, tgtId], pulse: tgtId, title: label, kind: 'combine' }); if (!mv) return;
    if (relayed(M)) { if (!(await send(M, { type: 'consolidate', sourceId: srcId, targetId: tgtId, via: 'options' }, label, mv))) endMove(M, mv); return; }   // (COMBINE10K14K: asked in the options window)
    const r = await G().consolidate(srcId, tgtId); if (r && r.ok === false && r.why) fail(M, srcId, r.why);
    finishMove(M, mv, { text: (r && (r.text || r.why)) || 'Done', bad: !!(r && r.ok === false) });
  }
  async function doEmpty(M, id, label) {
    const S = M.sheetsUI, mv = startMove(M, { id, tiles: [id], pulse: id, title: label, kind: 'empty' }); if (!mv) return;
    if (relayed(M)) { if (!(await send(M, { type: 'empty', sheetId: id }, label, mv))) endMove(M, mv); return; }
    const r = await G().emptySheet(id); if (r && r.ok === false && r.why) fail(M, id, r.why);
    finishMove(M, mv, { text: (r && (r.text || r.why)) || 'Done', bad: !!(r && r.ok === false) });
  }
  async function doDelete(M, id, label) {
    if (relayed(M)) { M.sheetsUI.gone = id; paint(M, true); if (!(await send(M, { type: 'delete', sheetId: id }, label))) unshow(M, true); return; }
    const r = await G().deleteEmpty(id); if (r && r.ok === false) fail(M, id, r.why); paint(M, true);
  }
  const nameOfRow = (M, id) => { const r = rowsOf(M).find(x => x.id === id); return r ? r.name : 'a sheet'; };

  /* ── a run sheet onto a physical sheet of the repository: all its pieces move onto it (Gate.seatOn; PartialNest does the work) ── */
  /* MOVESHOW: the move plan. The first sheet takes the pieces; each further one takes what does not fit, in the order shown. The engine's own preview (Gate.seatOn / PartialNest.preview, with the WHOLE list) is asked again, after a short pause, whenever the list changes. */
  const nth = i => ['1st', '2nd', '3rd'][i] || (i + 1) + 'th';
  function planOpen(M, me, pid) {
    const S = M.sheetsUI; if (S.move) return;
    S.plan = { srcId: me.id, ids: [pid], seq: 0, working: true, prev: null, bad: '', note: '', live: '', timer: 0 }; S.banner = null; renderBanner(M); closeAsk(M);
    renderPlan(M); decorate(M); queuePreview(M, 120);
    try { S.planEl.firstElementChild.scrollIntoView({ block: 'nearest' }); } catch (_) {}
  }
  function planCancel(M) { const S = M.sheetsUI, P = S && S.plan; if (!P) return; clearTimeout(P.timer); S.plan = null; renderPlan(M); decorate(M); }
  function queuePreview(M, ms) { const S = M.sheetsUI, P = S.plan; if (!P) return; clearTimeout(P.timer); P.seq++; P.working = true; P.live = ''; const seq = P.seq; P.timer = setTimeout(() => runPreview(M, P, seq), ms == null ? 450 : ms); renderPlan(M); }
  async function runPreview(M, P, seq) {
    const S = M.sheetsUI; if (S.plan !== P || P.seq !== seq) return;
    const me = rowsOf(M).find(r => r.id === P.srcId), ids = P.ids.slice(), onStep = st => { if (S.plan === P && P.seq === seq && st && st.text) { P.live = st.text; renderPlan(M); } };
    let prev = null, bad = '';
    if (!me) bad = 'That sheet is not here any more.';
    else try {
      if (relayed(M)) {
        const pg = G().pageOf && G().pageOf(me.id), PN = root.PartialNest;
        if (pg && pg.charms && pg.charms.length && PN && PN.preview && PN.canSeat) { const can = PN.canSeat(pg); if (!can.ok) bad = can.why; else { const r = await PN.preview(pg, ids, { onStep }); if (!r || !r.ok) bad = (r && (r.why || r.reason)) || 'That sheet cannot take them now.'; else prev = r; } }
      } else {
        const r = await G().seatOn(me.id, ids, { preview: true, onStep }); if (!r || !r.ok) bad = (r && r.why) || 'That sheet cannot take them now.'; else prev = r.preview || { words: r.text };
      }
    } catch (e) { bad = String((e && e.message) || e); }
    if (S.plan !== P || P.seq !== seq) return;
    P.working = false; P.prev = prev; P.bad = bad; P.live = ''; renderPlan(M);
  }
  function renderPlan(M) {
    const S = M.sheetsUI, P = S.plan, el = S.planEl;
    if (!P) { el.hidden = true; el.innerHTML = ''; return; }
    const me = rowsOf(M).find(r => r.id === P.srcId), prev = P.prev, name = me ? me.name : 'The sheet', n = me ? me.pieces : 0;
    const rows = P.ids.map((id, i) => {
      const c = cardInfo(id), link = prev && prev.links ? prev.links.find(l => l.partialId === id) : null, skip = prev && prev.skipped ? prev.skipped.find(x => x.partialId === id) : null;
      const take = P.working ? '' : link ? `takes ${link.placed} of ${n}` : skip ? esc(skip.why || 'nothing fits') : '';
      return `<li data-pid="${esc(id)}"><span class="osPlanN">${nth(i)}:</span><b>${esc(cap(cardLabel(c)))}</b><span class="osPlanTake">${take}</span><span class="osPlanSp"></span><button type="button" class="btn ghost xs" data-plan-up="${esc(id)}" ${i ? '' : 'disabled'} aria-label="Move ${esc(cardLabel(c))} earlier">▲</button><button type="button" class="btn ghost xs" data-plan-down="${esc(id)}" ${i < P.ids.length - 1 ? '' : 'disabled'} aria-label="Move ${esc(cardLabel(c))} later">▼</button><button type="button" class="btn ghost xs" data-plan-out="${esc(id)}">Take out</button></li>`;
    }).join('');
    let words = '', rest = '';
    if (P.working) words = 'working it out…' + (P.live ? ' ' + P.live : '');
    else if (P.bad) words = P.bad;
    else if (prev) {
      words = String(prev.words || '').replace(/\bthis partial sheet\b/g, 'this sheet').replace(/\bpartial sheets\b/g, 'sheets').replace(/\bpartial sheet\b/g, 'sheet');   // (MOVESHOW polish: a new sheet is not a 'partial sheet' to the person reading)
      if (prev.continues && prev.continues.n > 0) rest = `Pieces that still do not fit (${prev.continues.n}): ${prev.continues.words || prev.words}`;
      else if (prev.fitsAll) rest = 'Everything fits on the sheets chosen.';
    }
    el.hidden = false;
    el.innerHTML = `<div class="osPlan" role="group" aria-label="Move plan" data-plan><p class="osPlanHead">Move plan: ${esc(name)}'s ${esc(plural(n))}</p><ol class="osPlanList">${rows}</ol>
      <p class="osPlanHelp">Click another Available sheet to add it as ${nth(P.ids.length)}${P.ids.length < 2 ? ', 3rd …' : ' …'} for the pieces that do not fit; click a chosen sheet to take it out; use ▲ ▼ to change their order.</p>
      <p class="osPlanWords${P.bad && !P.working ? ' bad' : ''}" data-plan-words role="status">${esc(words)}</p>${rest ? `<p class="osPlanRest" data-plan-rest>${esc(rest)}</p>` : ''}${P.note ? `<p class="osPlanNote" role="alert">${esc(P.note)}</p>` : ''}
      <div class="osPlanBtns"><button type="button" class="btn ghost xs" data-plan-cancel>Cancel</button><button type="button" class="btn sage xs" data-plan-go ${P.working || P.bad ? 'disabled' : ''}>Move them</button></div></div>`;
  }
  function planNote(M, P, text) { P.note = text; renderPlan(M); setTimeout(() => { if (P.note === text) { P.note = ''; if (M.sheetsUI.plan === P) renderPlan(M); } }, 6000); }
  function planChanged(M) { renderPlan(M); decorate(M); queuePreview(M); }
  function planClick(M, e) {
    const S = M.sheetsUI, P = S.plan, t = e.target.closest && e.target.closest('button'); if (!P || !t) return;
    if (t.hasAttribute('data-plan-cancel')) return planCancel(M);
    if (t.hasAttribute('data-plan-go')) return void planGo(M);
    const at = id => P.ids.indexOf(id);
    if (t.hasAttribute('data-plan-out')) { if (P.ids.length < 2) return planNote(M, P, 'It is the only chosen sheet. Press Cancel to stop, or click another Available sheet first.'); P.ids.splice(at(t.getAttribute('data-plan-out')), 1); return planChanged(M); }
    for (const [attr, d] of [['data-plan-up', -1], ['data-plan-down', 1]]) if (t.hasAttribute(attr)) { const i = at(t.getAttribute(attr)), j = i + d; if (i < 0 || j < 0 || j >= P.ids.length) return; const x = P.ids[i]; P.ids[i] = P.ids[j]; P.ids[j] = x; return planChanged(M); }
  }
  function cardClick(M, e) {
    const S = M.sheetsUI, P = S && S.plan; if (!P) return;
    const card = e.target.closest && e.target.closest('article.ohc'); if (!card || !gridOf(M).contains(card)) return;
    if (e.target.closest('button, a, input, select, textarea, label')) return;
    e.preventDefault(); e.stopPropagation();
    if (S.move) return;
    const id = card.dataset.id, at = P.ids.indexOf(id), me = rowsOf(M).find(r => r.id === P.srcId);
    if (at >= 0) { if (P.ids.length < 2) return planNote(M, P, 'It is the only chosen sheet. Press Cancel to stop, or click another Available sheet first.'); P.ids.splice(at, 1); return planChanged(M); }
    const why = me ? whyCard(M, me, cardInfo(id)) : 'That sheet is not here any more.';
    if (why) return planNote(M, P, why);
    if (P.ids.length >= 8) return planNote(M, P, 'Eight sheets is the most for one move.');
    P.ids.push(id); planChanged(M);
  }
  function planGo(M) {
    const S = M.sheetsUI, P = S.plan; if (!P || P.working || P.bad) return;
    const me = rowsOf(M).find(r => r.id === P.srcId); if (!me) return planCancel(M);
    const ids = P.ids.slice(), labels = ids.map(id => cap(cardLabel(cardInfo(id))));
    clearTimeout(P.timer); S.plan = null; renderPlan(M);
    doSeat(M, me, ids, labels);
  }
  async function doSeat(M, me, ids, labels) {
    const S = M.sheetsUI, n = me.pieces, head = `${me.name}'s ${plural(n)}`;
    const mv = startMove(M, { id: me.id, tiles: [me.id], cards: ids, pulse: me.id, title: `Moving ${head} onto ${labels[0]}${ids.length > 1 ? ` (and ${ids.length - 1} more for the overflow)` : ''}`, kind: 'seat', stockId: me.stockId || '', size: me.size }); if (!mv) return;
    flyGhost(M, me.id, ids[0]);
    if (relayed(M)) { if (!(await send(M, { type: 'seat', sheetId: me.id, partialId: ids[0], partialIds: ids }, `Move ${head} onto ${labels[0]}`, mv))) endMove(M, mv); return; }
    const r = await G().seatOn(me.id, ids, { finish: true, onStep: st => { if (st && st.text && S.move === mv) { mv.step = { text: st.text, pct: Number.isFinite(+st.pct) ? +st.pct : null }; updateOv(M); livePic(M, mv); } } });
    if (r && r.ok) finishMove(M, mv, { text: r.text, freed: r.freed || false });
    else finishMove(M, mv, { text: (r && r.why) || 'The pieces were not moved.', bad: true });
  }

  /* MOVESHOW: a move in progress (local or relayed): the overlay on the tile(s) and the chosen cards, the seconds, the picture drawn again as the nest changes; the result banner at the end */
  function startMove(M, o) {
    const S = M.sheetsUI; if (!S || S.move) return null;
    const mv = Object.assign({ t0: Date.now(), step: { text: 'Starting…', pct: null }, ids: o.cards || [], cards: o.cards || [], relay: relayed(M) }, o); S.move = mv; S.banner = null; renderBanner(M);
    decorate(M); if (!S.tick) S.tick = setInterval(() => tick(M), 500); return mv;
  }
  function stopTick(S) { if (S.tick) { clearInterval(S.tick); S.tick = 0; } }
  function endMove(M, mv) { const S = M.sheetsUI; if (!S || S.move !== mv) return; S.move = null; stopTick(S); decorate(M); paint(M, true); }
  function tick(M) {
    const S = M.sheetsUI, mv = S && S.move; if (!mv) return stopTick(S || {});
    if (!mv.relay && (mv.kind === 'combine' || mv.kind === 'empty')) { const b = G().mergeBusy && G().mergeBusy(M.m); if (b && b.text) mv.step = { text: b.text, pct: null }; }
    updateOv(M); if (!mv.relay && Date.now() - (mv.picAt || 0) > 900) livePic(M, mv);
  }
  /** the tile's picture drawn again (the sheet's own layout as the nest changes; on a following computer, as the copy of the sheet changes) */
  function livePic(M, mv) { const S = M.sheetsUI; if (!S) return; mv.picAt = Date.now(); try { const rows = rowsOf(M).filter(r => (mv.tiles || [mv.id]).includes(r.id)); if (rows.length) pictures(S, rows); } catch (_) {} }
  function ovHtml(mv) { return `<b data-ov-t></b><span class="osOvBar ind" aria-hidden="true"><i></i></span><span data-ov-s></span><span class="osOvSec" data-ov-sec></span>`; }
  function decorate(M) {
    const S = M.sheetsUI, grid = S && gridOf(M); if (!S || !grid) return;
    for (const n of grid.querySelectorAll('.osBadge,.osFreedTag')) n.remove();
    for (const c of grid.querySelectorAll('article.ohc.osChosen, article.ohc.osFreed')) c.classList.remove('osChosen', 'osFreed');
    const ids = S.plan ? S.plan.ids : S.move ? (S.move.cards || []) : [], cardOf2 = id => grid.querySelector(`:scope > article.ohc[data-id="${CSS_ESC(id)}"]`);
    ids.forEach((id, i) => { const c = cardOf2(id); if (!c) return; c.classList.add('osChosen'); const b = doc.createElement('span'); b.className = 'osBadge'; b.textContent = String(i + 1); b.setAttribute('aria-label', nth(i) + ' sheet'); c.prepend(b); });
    for (const c of grid.querySelectorAll(':scope > article.ohc[data-status="available"]')) if (FREED.has(c.dataset.stock)) { c.classList.add('osFreed'); const t = doc.createElement('span'); t.className = 'osFreedTag'; t.textContent = 'Just freed — Delete or keep'; c.prepend(t); }
    const mv = S.move, want = new Set();
    if (mv) {
      const targets = [];
      for (const id of mv.tiles || []) { const t = [...S.wrap.querySelectorAll('.osSh')].find(x => x.dataset.id === id); if (t) targets.push([t, id === mv.id ? mv.title : `Receiving from ${(rowsOf(M).find(r => r.id === mv.id) || {}).name || 'the other sheet'}…`]); }
      (mv.cards || []).forEach((id, i) => { const c = cardOf2(id); if (c) targets.push([c, i === 0 ? `Receiving the pieces (${nth(0)} sheet)` : `${nth(i)} sheet: takes what does not fit`]); });
      for (const [el, title] of targets) { want.add(el); let ov = el.querySelector(':scope > .osOv'); if (!ov) { ov = doc.createElement('div'); ov.className = 'osOv'; ov.setAttribute('role', 'status'); ov.innerHTML = ovHtml(mv); if (getComputedStyle(el).position === 'static') el.style.position = 'relative'; el.appendChild(ov); } ov.querySelector('[data-ov-t]').textContent = title; }
    }
    for (const ov of doc.querySelectorAll('.osOv')) if (!want.has(ov.parentNode) && (S.wrap.contains(ov) || grid.contains(ov))) ov.remove();
    updateOv(M);
  }
  function updateOv(M) {
    const S = M.sheetsUI, mv = S && S.move; if (!mv) return;
    const sec = Math.floor((Date.now() - mv.t0) / 1000), pct = mv.step && Number.isFinite(+mv.step.pct) ? Math.max(0, Math.min(100, +mv.step.pct)) : null;
    for (const ov of (gridOf(M) || S.wrap).querySelectorAll('.osOv')) {
      const st = ov.querySelector('[data-ov-s]'), se = ov.querySelector('[data-ov-sec]'), bar = ov.querySelector('.osOvBar'); if (!st) continue;
      if (st.textContent !== mv.step.text) st.textContent = mv.step.text; se.textContent = sec + ' s';
      bar.classList.toggle('ind', pct == null); if (pct != null) bar.firstElementChild.style.width = pct + '%'; else bar.firstElementChild.style.width = '';
    }
  }
  function flyGhost(M, srcId, pid) {
    if (reduced()) return;
    try {
      const S = M.sheetsUI, pic = S.wrap.querySelector(`[data-pic="${CSS_ESC(srcId)}"]`), card = gridOf(M).querySelector(`:scope > article.ohc[data-id="${CSS_ESC(pid)}"]`); if (!pic || !card) return;
      try { card.scrollIntoView({ block: 'nearest' }); } catch (_) {}
      const a = pic.getBoundingClientRect(), b = card.getBoundingClientRect(); if (!a.width || !b.width) return;
      const g = doc.createElement('div'); g.className = 'osGhost'; g.setAttribute('aria-hidden', 'true'); g.style.cssText = `left:${a.left}px;top:${a.top}px;width:${a.width}px;height:${a.height}px`;
      const cv = pic.querySelector('canvas'), im = pic.querySelector('img'), img = doc.createElement('img'); img.alt = '';
      try { if (cv) img.src = cv.toDataURL(); else if (im && im.src) img.src = im.src; } catch (_) {}
      g.appendChild(img); (S.wrap.closest('dialog') || doc.body).appendChild(g);   // (inside the Options window: a modal window is above everything outside it)
      const k = Math.min(1, (b.width * 0.7) / a.width), dx = (b.left + b.width / 2) - (a.left + a.width / 2), dy = (b.top + b.height / 2) - (a.top + a.height / 2);
      g.getBoundingClientRect();
      requestAnimationFrame(() => { g.style.transform = `translate(${dx}px,${dy}px) scale(${k})`; g.style.opacity = '.3'; });
      setTimeout(() => g.remove(), 900);
    } catch (_) { /* the picture is only for the eye */ }
  }
  function finishMove(M, mv, res) {
    const S = M.sheetsUI; if (!S) return;
    if (S.move === mv) { S.move = null; stopTick(S); }
    { const fid = (res.freed && res.freed.stockId) || (typeof res.freed === 'string' && res.freed) || mv.stockId; if (res.freed && fid) FREED.add(fid); }
    S.banner = { text: res.text || (res.bad ? 'It was not done.' : 'Done'), bad: !!res.bad, title: res.bad ? mv.title : '', retry: res.retry || null };   // (NOWCMD: a cloud failure keeps its Try again)
    decorate(M); renderBanner(M); paint(M, true);
    if (!res.bad) { const after = () => { if (M.sheetsUI === S) { S.pulse = mv.pulse || mv.id; paint(M, true); decorate(M); } }; try { Promise.resolve(root.OptionsStudio && OptionsStudio.reload && OptionsStudio.reload()).then(after, after); } catch (_) { after(); } }
  }
  function renderBanner(M) {
    const S = M.sheetsUI, B = S.banner, el = S.bannerEl; if (!B) { el.hidden = true; el.innerHTML = ''; return; }
    el.hidden = false; el.innerHTML = `<div class="osBanner${B.bad ? ' bad' : ''}" role="status" data-banner><div>${B.title ? `<b>${esc(B.bad ? 'Not done: ' + B.title : B.title)}</b>` : ''}${esc(B.text)}</div>${B.retry ? '<button type="button" class="btn xs" data-sh-retry>Try again</button>' : ''}<button type="button" class="btn ghost xs" data-sh-dismiss>Dismiss</button></div>`;
    try { el.firstElementChild.scrollIntoView({ block: 'nearest' }); } catch (_) {}
  }
  const whyCard = (M, me, c) => {
    if (!c) return 'That sheet is not in the list. Press Refresh.';
    if (c.metal !== M.m) return `That is ${NAMES[c.metal] || 'another metal'}: ${me.name} is ${NAMES[M.m] || ''}.`;
    const st = c.status || 'available';
    if (st === 'inUse') return `Taken by ${c.inUseBySheetName || 'another sheet'}: only an Available sheet can take pieces.`;
    if (st !== 'available') return `${st === 'used' ? 'Used' : st === 'discarded' ? 'Discarded' : 'Deleted'}: only an Available sheet can take pieces.`;
    if (!me.pieces) return `${me.name} has no pieces to move.`;
    if (!me.canSource) return `${me.name} ${me.whySource}`;
    return '';
  };

  /* ── keyboard: Alt+ArrowUp / Alt+ArrowLeft move a focused tile earlier, Alt+ArrowDown / Alt+ArrowRight later ── */
  function key(M, e) {
    const k = e.key; if (!e.altKey || !['ArrowUp', 'ArrowDown', 'ArrowLeft', 'ArrowRight'].includes(k)) return;
    const li = e.target.closest && e.target.closest('.osSh'); if (!li) return; e.preventDefault();
    const ids = [...M.sheetsUI.wrap.querySelectorAll('.osSh')].map(x => x.dataset.id), i = ids.indexOf(li.dataset.id), j = i + (k === 'ArrowUp' || k === 'ArrowLeft' ? -1 : 1);
    if (j < 0 || j >= ids.length) return;
    ids.splice(i, 1); ids.splice(j, 0, li.dataset.id);
    Promise.resolve(doReorder(M, ids, li.dataset.id, `Swap the order: ${nameOfRow(M, li.dataset.id)} moves ${j < i ? 'earlier' : 'later'}`)).then(() => { const n = [...M.sheetsUI.wrap.querySelectorAll('.osSh')].find(x => x.dataset.id === li.dataset.id); if (n) n.focus({ preventScroll: true }); });
  }

  /* ── pointer: the picture drags at once; the rest of the tile after 6 px. The grid scrolls while a tile is held near its top or bottom edge. ── */
  const scroller = el => { for (let n = el; n && n !== doc.body; n = n.parentNode) { if (n.nodeType === 1) { const o = getComputedStyle(n).overflowY; if ((o === 'auto' || o === 'scroll') && n.scrollHeight > n.clientHeight) return n; } } return doc.scrollingElement; };
  function down(M, e) {
    if (e.button != null && e.button > 0) return;
    const li = e.target.closest && e.target.closest('.osSh'); if (!li || (e.target.closest && e.target.closest('button'))) return;
    if (M.sheetsUI.move) return;   // (MOVESHOW: one move at a time)
    const row = rowsOf(M).find(r => r.id === li.dataset.id); if (!row || !row.movable) return;
    const S = M.sheetsUI, st = { li, id: row.id, x: e.clientX, y: e.clientY, on: false, rows: null, over: null, mode: null, pid: e.pointerId, last: null, tick: 0 };
    const grip = !!(e.target.closest && e.target.closest('.osShPic'));
    const stop = () => { if (st.tick) { clearInterval(st.tick); st.tick = 0; } };
    const move = ev => {
      if (!st.on) { if (!grip && Math.hypot(ev.clientX - st.x, ev.clientY - st.y) < 6) return; st.on = true; S.drag = st; st.rows = rowsOf(M); li.classList.add('dragging'); doc.documentElement.classList.add('osShDrag');
        st.tick = setInterval(() => { const ev2 = st.last; if (!ev2) return; const sc = scroller(S.wrap.parentNode), h = root.innerHeight || 800, dy = ev2.clientY < 70 ? -14 : ev2.clientY > h - 70 ? 14 : 0; if (dy && sc) { const was = sc.scrollTop; sc.scrollTop += dy; if (sc.scrollTop !== was) { st.y -= 0; target(M, st, ev2); } } }, 30); }
      st.last = ev; li.style.transform = `translate(${ev.clientX - st.x}px,${ev.clientY - st.y}px)`; target(M, st, ev);
    };
    const end = ev => {
      doc.removeEventListener('pointermove', move, true); doc.removeEventListener('pointerup', end, true); doc.removeEventListener('pointercancel', end, true); stop();
      if (!st.on) return; doc.documentElement.classList.remove('osShDrag'); li.classList.remove('dragging'); li.style.transform = ''; S.drag = null;
      const over = st.over, mode = st.mode; clear(M);
      if (ev.type === 'pointercancel' || !over) return paint(M, true);
      drop(M, st, over, mode);
    };
    doc.addEventListener('pointermove', move, true); doc.addEventListener('pointerup', end, true); doc.addEventListener('pointercancel', end, true);
  }
  const clear = M => { const g = gridOf(M) || M.sheetsUI.wrap; for (const n of g.querySelectorAll('.onto,.before,.after,.refused')) n.classList.remove('onto', 'before', 'after', 'refused'); for (const t of g.querySelectorAll('.osShTag')) t.remove(); };
  function target(M, st, ev) {
    clear(M); st.over = null; st.mode = null;
    const grid = gridOf(M); if (!grid) return;
    const tiles = [...M.sheetsUI.wrap.querySelectorAll('.osSh')].filter(n => n !== st.li), cards = [...grid.querySelectorAll(':scope > .ohc')];
    const hit = tiles.concat(cards).find(n => { const r = n.getBoundingClientRect(); return r.width > 0 && ev.clientY >= r.top - 6 && ev.clientY <= r.bottom + 6 && ev.clientX >= r.left - 9 && ev.clientX <= r.right + 9; });
    if (!hit) return;
    const me = st.rows.find(x => x.id === st.id);
    if (hit.classList.contains('ohc')) {   // a physical sheet of the repository: the whole card is the target
      const why = whyCard(M, me, cardInfo(hit.dataset.id));
      hit.classList.add('onto'); if (why) hit.classList.add('refused');
      const tag = doc.createElement('span'); tag.className = 'osShTag' + (why ? ' refused' : ''); tag.textContent = why || `Move ${me.name}'s ${plural(me.pieces)} onto this sheet`; hit.appendChild(tag);
      st.over = hit.dataset.id; st.mode = why ? 'refused:' + why : 'seat'; return;
    }
    const r = hit.getBoundingClientRect(), fx = (ev.clientX - r.left) / Math.max(1, r.width), fy = (ev.clientY - r.top) / Math.max(1, r.height), row = st.rows.find(x => x.id === hit.dataset.id);
    if (fx > 0.26 && fx < 0.74 && fy > 0.1 && fy < 0.9) {
      const why = !row.movable && row.locked ? `${row.name}: ${row.lockWhy}` : !me.canSource ? `${me.name} ${me.whySource}` : !row.canTarget ? `${row.name} ${row.whyTarget}` : '';
      hit.classList.add('onto'); if (why) hit.classList.add('refused');
      const tag = doc.createElement('span'); tag.className = 'osShTag' + (why ? ' refused' : ''); tag.textContent = why || `Combine onto ${row.name}`; hit.appendChild(tag);
      st.over = hit.dataset.id; st.mode = why ? 'refused:' + why : 'onto';
    } else {
      const before = fx <= 0.26 || (fx >= 0.74 ? false : fy <= 0.5); hit.classList.add(before ? 'before' : 'after'); st.over = hit.dataset.id; st.mode = before ? 'before' : 'after';
    }
  }
  function ask(M, html, yes, label, no) {
    const S = M.sheetsUI, box = S.el.querySelector('[data-sh="ask"]'); box.hidden = false;
    box.innerHTML = `<span>${html}</span><button type="button" class="btn ghost xs" data-sh-no>${esc(no || 'Cancel')}</button><button type="button" class="btn sage xs" data-sh-yes>${esc(label)}</button>`;
    box.querySelector('[data-sh-no]').onclick = () => closeAsk(M);
    box.querySelector('[data-sh-yes]').onclick = () => { const a = S.ask; closeAsk(M); yes(a); };
    box.querySelector('[data-sh-yes]').focus();
    try { box.scrollIntoView({ block: 'nearest' }); } catch (_) {}
  }
  function drop(M, st, overId, mode) {
    const S = M.sheetsUI;
    if (mode && mode.startsWith('refused:')) return fail(M, st.id, mode.slice(8));
    const src = st.rows.find(r => r.id === st.id);
    if (mode === 'seat') return void planOpen(M, src, overId);   // (MOVESHOW: the move plan opens; Move them is pressed there)
    /* COMBINE10K14K (Paul, 21 Nov 2026: "allow that but only for the 10k and 14 K under the manual options menu"): a person's drag of a 10K / 14K run sheet onto another, confirmed here;
       nothing else anywhere combines sheets */
    if (mode === 'onto' && !solid(M.m)) return fail(M, st.id, 'Combining sheets is for 10K and 14K Gold only');
    if (mode === 'onto') {
      const tgt = st.rows.find(r => r.id === overId);
      S.ask = { src, tgt };
      return ask(M, `Combine ${plural(src.pieces)} from ${esc(src.name)} onto ${esc(tgt.name)}? What does not fit goes on to the next sheets, then stays on ${esc(src.name)}. ${esc(src.name)} is kept, empty, until you delete it.${src.relay ? (root.CNViewer && root.CNViewer.on() ? ' The cloud does it.' : ' The computer that carries the run does it.') : ''}`, a => doCombine(M, a.src.id, a.tgt.id, `Combine ${src.name} onto ${tgt.name}`), 'Combine');
    }
    const ids = [...S.wrap.querySelectorAll('.osSh')].map(x => x.dataset.id).filter(id => id !== st.id), at = ids.indexOf(overId);
    ids.splice(mode === 'before' ? at : at + 1, 0, st.id);
    doReorder(M, ids, st.id, `Swap the order: ${src.name} goes ${mode === 'before' ? 'before' : 'after'} ${nameOfRow(M, overId)}`);
  }
  function closeAsk(M) { const S = M.sheetsUI; if (!S) return; S.ask = null; const box = S.el.querySelector('[data-sh="ask"]'); box.hidden = true; box.innerHTML = ''; paint(M, true); }

  /* ── empty a sheet / delete an empty one (one confirmation each) ── */
  function emptyAsk(M, id) {
    const row = rowsOf(M).find(r => r.id === id); if (!row || row.locked || !row.canSource) return;
    M.sheetsUI.ask = { empty: id };
    ask(M, `Empty ${esc(row.name)}? Its ${plural(row.pieces)} move onto the other sheets in your order (the sheets before it first, then the ones after). What fits nowhere stays on ${esc(row.name)}.${row.relay ? (root.CNViewer && root.CNViewer.on() ? ' The cloud does it.' : ' The computer that carries the run does it.') : ''}`, a => doEmpty(M, a.empty, `Empty ${row.name}`), 'Empty it');
  }
  function del(M, id) {
    const row = rowsOf(M).find(r => r.id === id); if (!row || !row.canDelete) return;
    M.sheetsUI.ask = { del: id };
    ask(M, `Delete ${esc(row.name)}? It is empty. The sheets after it move up one.`, a => doDelete(M, a.del, `Delete ${row.name}`), 'Delete this sheet');
  }
  function destroy(M) { try { if (M && M.sheetsUI) { stopRel(M.sheetsUI); stopTick(M.sheetsUI); if (M.sheetsUI.plan) clearTimeout(M.sheetsUI.plan.timer); M.sheetsUI.planEl.remove(); M.sheetsUI.bannerEl.remove(); doc.documentElement.classList.remove('osShDrag'); M.sheetsUI.el.remove(); M.sheetsUI.wrap.remove(); M.sheetsUI.solo.remove(); M.sheetsUI = null; } } catch (_) {} }
  root.OptionsSheets = { mount, place, paint, destroy };
})(window);
