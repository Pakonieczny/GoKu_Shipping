/* The Partial Sheet panel (Paul, 7 Oct 2026: "Each sheet needs its own individual Partial Sheet repo in the Nest tab. You will need to
   incorporate the Partial Sheet button inside the Options menu for the Rose Gold, 14K gold, and 10K gold sheets. The Partial Sheet will
   show a list of available cards that each show the remaining partial sheet thumbnail image. The size of the remaining sheet and an
   estimate of how many regular pieces can reasonably fit ... the date of last use ... the user will have to be informed if there is
   enough space on a partial sheet to fit all of the current pieces and also allow the user to use multiple partial sheets ... a choice
   whether the system should automatically try to reuse partial sheets or offer to user the ability to make a brand new sheet and
   stipulate the size inside the options menu ... beautiful animations, and UI design and useability must match the rest of the
   advanced app").

   What this file is: only what the person SEES and touches. The data (the list of partial sheets, their last use, the estimate, the
   setting) is PartialSheets (charm-nest-partial-data.js, PS3); the work (nest the current pieces onto a chosen partial, fill the next
   partial with the rest) is PartialEngine (PS2). See /plans/partial-sheets/contract.md.

   Where it lives (Paul, 7 Oct 2026, the Options Studio, round 2: "Combine these two into one seamless menu"): the SHEET menu of the sheet
   card's Options window (charm-nest-options-modal.js, OptionsStudio), where "This sheet" and "All partial sheets" are one menu. Paul also
   took out the chip strip on the sheet card and the one in the window ("redundant and unnecessary complications"), and the Partial sheets
   card: this file draws none of them any more. It is the WORK behind the menu: a pick asks PartialNest what would happen and says the
   answer in words in place (Use this one / Cancel, the chain of partial sheets), the re-seat motion, and the rule (reuse partial sheets
   automatically, or offer a brand new sheet at a size) as one compact two-option switch. The window draws the repository (every partial
   and new sheet as a large card) and asks this file for each card's Use this one (actions), for the answer (ask), and for the rule
   (policyHtml); bind() hands it the window's element for the events. Esc first cancels an answer that waits (escape), then the window.
   Each metal has its OWN rule; the repository is the window's one list of every metal and status (the Use button shows on this
   sheet's metal only). card() draws the plain card the window falls back to while OptionsHistory has no card.

   Cost (the Google bill): this file reads NOTHING by itself: the window reads the one list (PartialSheets.searchAll) and hands the
   cards in (items); the engine reads what it needs when a pick is checked. No timer, no polling.

   Motion: the cards come in the way the app's other lists do (a short rise, staggered); a pick opens its answer
   in place; a press on Use this one closes the window (as Merge sheets does) and the card shows the pieces re-seat: the sheet and the
   partial sheets side by side, each piece lifts and flies on an arc onto the partial, the partial's fill pulses, "+N", and the real
   card is there. Visual only: it starts after the work has been handed over and never waits for it; reduced motion shows none of it. */
(function (root) {
  'use strict';
  const doc = root.document;
  const GREEN = '#008974', TRAY = '#efe9de', SHEET = '#fffefb', CUTAWAY = '#f0eeeb', EDGE = 'rgba(176,86,63,.9)';   // the Nest preview's own colours (paintPreview, RoseStock.paint)
  const MM_PER_PT = 25.4 / 72, REF = { w: 100, h: 50 };                                                              // the one 100 x 50 mm frame every sheet picture is drawn in
  const METAL = { rose: { code: 'RG', word: 'Rose Gold' }, gold10k: { code: '10K', word: '10K Gold' }, gold14k: { code: '14K', word: '14K Gold' } };
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const num = n => Math.round(n * 10) / 10;
  const mmWord = (w, h) => `${num(w)} × ${num(h)} mm`;
  const plural = (n, one, many) => `${n} ${n === 1 ? one : many || one + 's'}`;
  const wordOf = m => (METAL[m] && METAL[m].word) || (root.CN && root.CN.labelOf ? root.CN.labelOf(m) : String(m || ''));
  const PS = () => root.PartialSheets || null;                                         // data and settings (PS3)
  const ENG = () => root.PartialNest || root.PartialEngine || (root.Gate && root.Gate.partial) || null;   // the work (PS2: PartialNest.canSeat / preview / seat / chain / on)
  const reduced = () => { try { return !!(root.Motion && root.Motion.reduced()); } catch (_) { return false; } };
  const sleep = ms => new Promise(r => setTimeout(r, ms));

  /* ── words ── */
  /** "Today, 2:41 PM" · "Yesterday, 9:05 AM" · "Oct 5, 4:30 PM" · "Oct 5, 2025, 4:30 PM": the date of a last use, friendly. */
  function friendly(at, now) {
    if (!Number.isFinite(at)) return 'Date not recorded';
    const d = new Date(at), n = new Date(Number.isFinite(now) ? now : Date.now());
    const t = d.toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit' });
    const day = x => new Date(x.getFullYear(), x.getMonth(), x.getDate()).getTime(), gap = Math.round((day(n) - day(d)) / 864e5);
    if (gap === 0) return `Today, ${t}`;
    if (gap === 1) return `Yesterday, ${t}`;
    return `${d.toLocaleDateString(undefined, d.getFullYear() === n.getFullYear() ? { month: 'short', day: 'numeric' } : { year: 'numeric', month: 'short', day: 'numeric' })}, ${t}`;
  }
  const fullDate = at => Number.isFinite(at) ? new Date(at).toLocaleString(undefined, { year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) : '';
  /** The estimate of regular pieces, worded as the estimate it is. */
  function fitWords(c) {
    const e = c && c.estimate;
    if (!e || !Number.isFinite(+e.pieces)) return { main: 'Fit not estimated', sub: '' };
    const n = Math.round(+e.pieces), low = Number.isFinite(+e.low) ? Math.round(+e.low) : n, high = Number.isFinite(+e.high) ? Math.round(+e.high) : n;
    if (n <= 0) return { main: 'Too small for a regular piece', sub: '' };
    // a pair needs TWO places: the pairs a leftover takes are half its pieces, said beside the pieces (a leftover for one piece takes no pair)
    const pairs = Number.isFinite(+e.pairs) ? Math.round(+e.pairs) : Math.floor(n / 2), pw = pairs >= 1 ? `about ${plural(pairs, 'pair')}` : 'no room for a pair';
    const range = low !== high && high > 0 ? `roughly ${low} to ${high}` : '';
    return { main: `About ${plural(n, 'piece')}`, sub: range ? `${range} · ${pw}` : pw };
  }
  /** Who and which sheet last used it (the cut that left it counts as its first use). */
  function lastUseWords(c) {
    const first = Number.isFinite(+c.cutAt) && +c.lastUsedAt === +c.cutAt && (!c.lastUsedSheet || c.lastUsedSheet === c.sourceSheet);
    const by = c.lastUsedBy || (first ? c.cutBy : '');
    if (first) return `left over when ${c.sourceSheet || 'a sheet'}${c.sourceSet ? ', ' + c.sourceSet : ''} was cut${by ? ' by ' + by : ''}`;
    return [by ? `by ${by}` : '', c.lastUsedSheet ? `on ${c.lastUsedSheet}` : ''].filter(Boolean).join(' · ');
  }
  const cardLabel = (m, c) => `${wordOf(m)} partial sheet, ${num(c.wMm)} by ${num(c.hMm)} millimetres, ${fitWords(c).main.toLowerCase()}, last used ${friendly(+c.lastUsedAt)}`;

  /* ── the picture: the partial sheet at true scale in one 100 x 50 mm frame, drawn as the Nest card draws a sheet with a green line
        (frame in the tray colour, the sheet at its own size top left, the cut-away part shaded, the cut edge dashed green) ── */
  const ringsOf = c => Array.isArray(c.outline) ? c.outline : Array.isArray(c.rings) ? c.rings : [];
  const pathOf = rings => rings.map(r => 'M' + r.map(p => p[0] + ' ' + p[1]).join('L') + 'Z').join('');
  // the cut edge: every segment of the outline that is not the sheet's own border (that is where a green line was cut)
  function edgesOf(rings, W, H) {
    const e = .002, border = (a, b) => (Math.abs(a[0]) < e && Math.abs(b[0]) < e) || (Math.abs(a[0] - W) < e && Math.abs(b[0] - W) < e) || (Math.abs(a[1]) < e && Math.abs(b[1]) < e) || (Math.abs(a[1] - H) < e && Math.abs(b[1] - H) < e);
    let d = '';
    for (const r of rings) {
      let on = false;
      for (let i = 0; i < r.length; i++) {
        const a = r[i], b = r[(i + 1) % r.length];
        if (border(a, b)) { on = false; continue; }
        d += (on ? '' : 'M' + a[0] + ' ' + a[1]) + 'L' + b[0] + ' ' + b[1]; on = true;
      }
    }
    return d;
  }
  /** frame:true = the card's picture (the 100 x 50 mm frame); false = the sheet alone (the re-seat scene's ground). */
  function sheetSvg(c, o = {}) {
    const W = +c.sheetWMm || REF.w, H = +c.sheetHMm || REF.h, frame = o.frame !== false, fw = frame ? Math.max(REF.w, W) : W, fh = frame ? Math.max(REF.h, H) : H, rings = ringsOf(c);
    const small = frame && (fw > W + .05 || fh > H + .05), left = rings.length ? pathOf(rings) : '';
    const label = rings.length ? `Partial sheet at true scale: ${mmWord(c.wMm || (c.bboxMm && c.bboxMm.w) || 0, c.hMm || (c.bboxMm && c.bboxMm.h) || 0)}, ${Math.round(+c.areaMm2 || 0)} square millimetres, cut from a ${mmWord(W, H)} sheet` : 'Nothing is left of this sheet';
    return `<svg class="psSvg${o.cls ? ' ' + o.cls : ''}" viewBox="0 0 ${fw} ${fh}" ${frame ? `style="aspect-ratio:${fw}/${fh}"` : 'preserveAspectRatio="none"'} role="img" aria-label="${esc(label)}">`
      + (frame ? `<rect x="0" y="0" width="${fw}" height="${fh}" style="fill:${small ? TRAY : 'var(--card2)'}"/>` : '')
      + `<rect x="0" y="0" width="${W}" height="${H}" fill="${CUTAWAY}"/>`
      + (left ? `<path d="${left}" fill="${SHEET}" fill-rule="evenodd"/><path class="psFill" d="${left}" fill-rule="evenodd" style="fill:var(--accent,${GREEN});opacity:0"/>` : '')
      + `<rect x=".25" y=".25" width="${Math.max(0, W - .5)}" height="${Math.max(0, H - .5)}" fill="none" stroke="${EDGE}" stroke-width="1" vector-effect="non-scaling-stroke"/>`
      + (left ? `<path d="${edgesOf(rings, W, H)}" fill="none" stroke="${GREEN}" stroke-width="1.8" stroke-dasharray="5 3" stroke-linejoin="round" vector-effect="non-scaling-stroke"/>` : '')
      + '</svg>';
  }

  /* ── state: one per metal (each metal has its own repository) ── */
  const ST = {};
  const stOf = m => ST[m] || (ST[m] = { open: false, items: null, loading: null, error: '', chain: [], adding: false, ask: null, seq: 0, polBusy: false, polDraft: null, polMsg: '', sh: null, node: null, entered: 0 });
  // the metal's own available partial sheets, newest used first (a card of another metal never shows here, whatever the cache holds)
  const availableOf = (items, m) => (items || []).filter(c => (!c.status || c.status === 'available') && (!m || !c.metal || c.metal === m)).slice().sort((a, b) => (+b.lastUsedAt || 0) - (+a.lastUsedAt || 0));
  const policyOf = m => { try { const p = PS() && PS().policy ? PS().policy(m) : null; return p ? { mode: p.mode === 'new' ? 'new' : 'auto', wMm: +p.wMm || 100, hMm: +p.hMm || 50 } : { mode: 'auto', wMm: 100, hMm: 50 }; } catch (_) { return { mode: 'auto', wMm: 100, hMm: 50 }; } };
  const piecesOn = sh => ((sh && sh.placements && sh.placements.length) || (sh && sh.charms && sh.charms.length) || 0);
  const sheetWord = sh => (root.CN && sh && sh.page ? (root.CharmNestSheetName ? root.CharmNestSheetName.short(sh) : `Sheet ${sh.page}`) : 'this sheet');

  /* ── styles (the app's own tokens; the plain card is the Library card's look) ── */
  const css = doc.createElement('style');
  css.textContent = `
.psLink{border:0;background:none;padding:0;font:inherit;font-weight:600;color:var(--ink);text-decoration:underline;text-underline-offset:2px;cursor:pointer}
.psLink:hover{color:var(--gold)}
.psLink:focus-visible{outline:2px solid var(--gold);outline-offset:2px;border-radius:3px}
.psList{display:grid;grid-template-columns:repeat(auto-fill,minmax(250px,1fr));gap:16px}
.psCard{background:var(--card);border:1px solid var(--line);border-left:5px solid var(--accent);border-radius:14px;box-shadow:var(--sh);padding:14px 16px 16px;display:flex;flex-direction:column;gap:9px;min-width:0;text-align:left;font:inherit;color:inherit;--accent:var(--ink25);position:relative}
.psCard[data-m=rose]{--accent:var(--m-rose)}.psCard[data-m=gold10k]{--accent:var(--m-gold10k)}.psCard[data-m=gold14k]{--accent:var(--m-gold14k)}
.psCard.on{border-color:var(--ink);box-shadow:0 0 0 2px var(--goldLine),var(--sh)}
.psCard.gone .psPvBox{opacity:.8}
.psPvBox{position:relative;overflow:hidden;border-radius:10px;border:1px solid var(--line2);background:var(--card2);line-height:0}
.psSvg{display:block;width:100%;height:auto}
.psStat{position:absolute;right:8px;top:8px;line-height:1;padding:5px 9px;border-radius:999px;background:rgba(255,254,251,.94);border:1px solid var(--line);color:var(--ink70);font:700 10px var(--sans);letter-spacing:.05em;text-transform:uppercase;box-shadow:0 1px 3px rgba(30,24,16,.12)}
.psStat[data-s=available]{color:#3c5a39;background:var(--sageSoft,#e7eddf)}.psStat[data-s=inUse]{color:#5c4210;background:var(--goldSoft,#f0e6cd)}.psStat[data-s=discarded]{color:var(--clay)}.psStat[data-s=deleted]{color:var(--clay)}
.psStamp{position:absolute;left:50%;top:50%;transform:translate(-50%,-50%) rotate(-12deg);padding:6px 16px;border:3px solid var(--clay);border-radius:6px;color:var(--clay);font:800 20px/1 var(--sans);letter-spacing:.14em;text-transform:uppercase;opacity:.82;pointer-events:none}
.psSize{display:flex;align-items:baseline;gap:9px;flex-wrap:wrap;font-size:12px;color:var(--ink70)}
.psSize b{font:700 15px var(--mono);color:var(--ink)}
.psSize .psMetal{font:700 10.5px var(--mono);letter-spacing:.05em;color:#fff;background:var(--accent);border-radius:6px;padding:2px 6px}
.psSize .psKind{font:700 10px var(--sans);letter-spacing:.05em;text-transform:uppercase;color:var(--ink70);background:var(--paper2);border-radius:6px;padding:2px 7px}
.psFit{display:flex;align-items:baseline;gap:6px;flex-wrap:wrap;font-size:13.5px;color:var(--ink)}
.psFit b{font-weight:650}
.psFit small,.psUse small{font-size:11.5px;color:var(--ink45)}
.psUse{display:grid;gap:2px;font-size:12px;line-height:1.5;color:var(--ink70)}
.psUse b{font-weight:600;color:var(--ink)}
.psActions{display:flex;flex-wrap:wrap;align-items:center;gap:8px;margin-top:4px}
.psAsk{grid-column:1/-1;display:grid;gap:12px;padding:16px 18px;border:1px solid var(--line);border-radius:14px;background:var(--card2)}
.psAsk .psWords{margin:0;font-size:14px;line-height:1.55;color:var(--ink)}
.psAsk .psWords.bad{color:var(--clay)}
.psAsk .psSub{margin:0;font-size:12.5px;line-height:1.55;color:var(--ink70)}
.psChainList{list-style:none;margin:0;padding:0;display:grid;gap:7px;max-height:220px;overflow:auto;overscroll-behavior:contain}
.psChainList li{display:grid;grid-template-columns:24px minmax(0,1fr) auto auto;align-items:center;gap:10px;font-size:12.5px;color:var(--ink70)}
.psChainList .n{display:inline-flex;align-items:center;justify-content:center;width:24px;height:24px;border-radius:50%;background:var(--ink);color:#fff;font:600 12px var(--mono)}
.psChainList .nm{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--ink)}
.psChainList .sz{font-family:var(--mono);font-size:11.5px}
.psChainList .ft{font-weight:600;color:var(--ink);white-space:nowrap}
.psAskBtns{display:flex;justify-content:flex-end;gap:10px;flex-wrap:wrap}
.psAskBtns .btn{margin-left:0}
.psBusy{display:flex;align-items:center;gap:8px;font-size:12px;line-height:1.5;color:var(--ink70)}
.psSpin{width:13px;height:13px;border:2px solid var(--line);border-top-color:var(--ink70);border-radius:50%;animation:spin .7s linear infinite;display:inline-block;flex:0 0 13px}
.psErr{color:var(--clay)}
.psPolicy{display:grid;gap:10px}
.psPolicyRow{display:flex;align-items:center;gap:8px 16px;flex-wrap:wrap}
.psPolicyLbl{font-size:10.5px;letter-spacing:.11em;text-transform:uppercase;font-weight:600;color:var(--ink45)}
.psSwitch{display:inline-flex;gap:3px;padding:3px;border:1px solid var(--line);border-radius:12px;background:var(--card2);max-width:100%}
.psRadio{position:relative;display:inline-flex;align-items:center;padding:9px 16px;border-radius:9px;cursor:pointer;font:600 13px/1.3 var(--sans);color:var(--ink70);transition:background-color .14s ease,color .14s ease,box-shadow .16s ease}
.psRadio:hover{background:var(--paper2);color:var(--ink)}
.psRadio.on{background:var(--velvet);color:#fff;box-shadow:0 1px 4px rgba(30,24,16,.25)}
.psRadio input[type=radio]{position:absolute;inset:0;width:100%;height:100%;margin:0;opacity:0;cursor:pointer}
.psRadio:has(input:focus-visible){outline:2px solid var(--gold);outline-offset:2px}
.psPolicy .help{font-size:12px;line-height:1.5;color:var(--ink70)}
.psPolicy .help:empty{display:none}
.psNewSize{display:flex;align-items:flex-end;flex-wrap:wrap;gap:10px 14px}
.psNewSize[hidden]{display:none}
.psNewSize .solidSize{grid-template-columns:repeat(2,minmax(96px,128px))}
.psFx>.psFxTag{font:600 10.5px var(--sans);letter-spacing:.02em;line-height:1;padding:4px 10px;border-radius:999px;border:1px solid var(--line);background:var(--card);color:var(--ink70);white-space:nowrap;box-shadow:0 2px 6px rgba(30,24,16,.08)}
.psFx>.psFxTag.on{background:var(--ink);color:#fff;border-color:var(--ink)}
.psFx>.psFxTag b{display:inline-block;margin-left:6px;font-weight:500;opacity:.7;font-variant-numeric:tabular-nums}
.psFx>.psFxSlot{background:#fffefb;box-shadow:0 1px 2px rgba(30,24,16,.1),0 10px 26px rgba(30,24,16,.13)}
.psFx>.psFxSlot .psSvg{width:100%;height:100%}
@media (max-width:760px){.psList{grid-template-columns:repeat(auto-fill,minmax(220px,1fr))}.psSwitch{display:grid;width:100%}}
@media (prefers-reduced-motion:reduce){.psSpin{animation-duration:1.6s}.psRadio{transition:none}}
`;
  doc.head.appendChild(css);

  /* ── the elements of the window this file works in (OptionsStudio hands them over: bind) ── */
  const q = (view, k) => view.querySelector(`[data-ps="${k}"]`);

  const STATUS_WORD = { available: 'Available', inUse: 'In use', used: 'Used', discarded: 'Discarded', deleted: 'Deleted' };
  const statusWords = c => {
    const st = c.status || 'available', who = x => (x ? ' by ' + x : '');
    if (st === 'inUse') return `In use${c.inUseBySheetName ? ' by ' + c.inUseBySheetName : ''}`;
    if (st === 'used') return `Used${c.usedBySheetName ? ' on ' + c.usedBySheetName : ''}${c.usedAt ? ', ' + friendly(+c.usedAt) : ''}`;
    if (st === 'discarded') return `Discarded${who(c.statusBy)}`;
    if (st === 'deleted') return `Deleted${who(c.deletedBy)}${c.deletedAt ? ', ' + friendly(+c.deletedAt) : ''}`;
    return 'Available';
  };
  /** The plain card the window falls back to while OptionsHistory has no card (a sheet of any metal and status). o.actions: the window's buttons; o.selected. */
  function cardHtml(m, c, s, locked, i, o = {}) {
    const f = fitWords(c), use = lastUseWords(c), st = c.status || 'available', isNew = c.kind === 'new';
    const fit = st === 'available' || st === 'inUse' ? `<span class="psFit"><b>${esc(f.main)}</b>${f.sub ? `<small>${esc(f.sub)}</small>` : ''}</span>` : `<span class="psFit"><b>${esc(statusWords(c))}</b></span>`;
    const made = isNew && Number.isFinite(+c.madeAt) ? `made${c.madeBy ? ' by ' + c.madeBy : ''}, ${friendly(+c.madeAt)}` : '';
    const last = Number.isFinite(+c.lastUsedAt) || !isNew ? `<span>Last used <b title="${esc(fullDate(+c.lastUsedAt))}">${esc(friendly(+c.lastUsedAt))}</b></span>${use ? `<small>${esc(use)}</small>` : ''}` : `<span>Not used yet</span>${made ? `<small>${esc(made)}</small>` : ''}`;
    return `<article class="psCard${o.selected ? ' on' : ''}${st === 'deleted' ? ' gone' : ''}" data-m="${esc(m)}" data-id="${esc(c.id)}" data-i="${i}" role="group" aria-label="${esc(cardLabel(m, c) + ', ' + statusWords(c))}">`
      + `<span class="psPvBox">${sheetSvg(c)}<span class="psStat" data-s="${esc(st)}">${esc(STATUS_WORD[st] || st)}</span>${st === 'deleted' ? '<span class="psStamp" aria-hidden="true">Deleted</span>' : ''}</span>`
      + `<span class="psSize"><span class="psMetal">${esc((METAL[c.metal] && METAL[c.metal].code) || c.code || '')}</span>${isNew ? '<span class="psKind">New sheet</span>' : ''}<b>${esc(mmWord(c.wMm, c.hMm))}</b><span>${Math.round(+c.areaMm2 || 0).toLocaleString()} mm²</span></span>`
      + fit
      + `<span class="psUse">${last}</span>${o.actions ? `<div class="psActions">${o.actions}</div>` : ''}</article>`;
  }
  // (a name that tells one partial sheet from another: the sheet it was cut from, and when; two leftovers of one sheet differ by their date; a made sheet says so)
  const cardName = c => !c ? 'Partial sheet' : c.kind === 'new' ? `New sheet ${mmWord(c.sheetWMm || c.wMm, c.sheetHMm || c.hMm)}` : [[c.sourceSheet, c.sourceSet].filter(Boolean).join(' · ') || 'Partial sheet', Number.isFinite(+c.cutAt) ? 'cut ' + new Date(+c.cutAt).toLocaleDateString(undefined, { month: 'short', day: 'numeric' }) : ''].filter(Boolean).join(' · ');
  /** PS2's preview as the panel draws it (PartialNest.preview: links = the listed partial sheets that take pieces, continues = the rest). */
  function normalize(ans) {
    if (!ans || ans.ok === false) return { ok: false, reason: (ans && (ans.why || ans.reason)) || 'This partial sheet cannot be used now.' };
    const cont = ans.continues || {};
    return { ok: true, pieces: Math.max(0, +ans.pieces || 0), fitsAll: !!ans.fitsAll, words: ans.words || '',
      links: (ans.links || []).map(l => ({ partialId: l.partialId, label: l.label || '', wMm: l.wMm, hMm: l.hMm, areaMm2: l.areaMm2, placed: Math.max(0, +l.placed || 0), densityPct: l.densityPct })),
      rest: Math.max(0, +cont.n || 0), next: cont.next || 'none', nextPartialId: cont.nextPartialId || null, contWords: cont.words || '',
      splitWords: ans.splitWords || '', sharedWords: ans.sharedWords || '' };   // (pairs: what the partial could hold only part of, and orders already on another sheet, in plain words)
  }
  function askHtml(m, sh, s) {
    const a = s.ask; if (!a) return '';
    const pol = policyOf(m), cards = new Map(availableOf(s.items, m).map(c => [c.id, c]));
    if (a.phase === 'checking') return `<div class="psAsk" data-ps="ask"><div class="psBusy" role="status"><span class="psSpin" aria-hidden="true"></span>${esc(a.text || `Checking how ${plural(piecesOn(sh), 'piece')} fit on ${s.chain.length > 1 ? 'these partial sheets' : 'this partial sheet'}…`)}</div></div>`;
    if (a.phase === 'bad') return `<div class="psAsk" data-ps="ask"><p class="psWords bad" role="alert">${esc(a.text)}</p><div class="psAskBtns"><button type="button" class="btn ghost xs" data-ps="cancel">Close</button></div></div>`;
    const ans = a.answer, n = ans.pieces, placed = ans.links.reduce((t, l) => t + l.placed, 0), rest = ans.rest, many = ans.links.length > 1, busy = a.phase === 'seating';
    let lead;
    if (!n) lead = `No pieces are nested on ${sheetWord(sh)} yet. It will nest on ${many ? 'these partial sheets' : 'this partial sheet'} from the start.`;
    else if (ans.fitsAll) lead = `All ${plural(n, 'piece')} on ${sheetWord(sh)} fit on ${many ? `these ${ans.links.length} partial sheets, filled in this order` : 'this partial sheet'}. They will be nested again onto ${many ? 'them' : 'it'}.`;
    else if (!placed) lead = ans.splitWords ? `No whole order of ${sheetWord(sh)} fits on ${s.chain.length > 1 ? 'these partial sheets' : 'this partial sheet'}. Pick a bigger one.` : `No piece of ${sheetWord(sh)} fits on ${s.chain.length > 1 ? 'these partial sheets' : 'this partial sheet'}. Pick another one.`;
    else lead = `${placed} of the ${plural(n, 'piece')} on ${sheetWord(sh)} fit on ${many ? `the ${ans.links.length} partial sheets, filled in this order` : 'this partial sheet'}${rest ? ` (${rest} do not)` : ''}.`;
    // what takes the rest, in PS2's own words (and the sheet it names, when it names one)
    let sub = ''; const next = ans.nextPartialId && cards.get(ans.nextPartialId);
    if (!ans.fitsAll && placed && rest) {
      sub = ans.contWords || (ans.next === 'partial' ? `The other ${rest} continue on the next partial sheet.` : ans.next === 'new' ? `The other ${rest} go on a brand new sheet.` : `The other ${rest} wait: no sheet takes them yet.`);
      if (next) sub += ` Next in line: ${cardName(next)}, ${mmWord(next.wMm, next.hMm)}.`;
      else if (ans.next === 'new' && pol.mode === 'new') sub += ` Its size is ${mmWord(pol.wMm, pol.hMm)}, as you set it.`;
    }
    const list = ans.links.length && (!ans.fitsAll || many) ? `<ol class="psChainList">${ans.links.map((l, i) => { const c = cards.get(l.partialId); return `<li><span class="n">${i + 1}</span><span class="nm" title="${esc(cardName(c))}">${esc(cardName(c))}</span><span class="sz">${esc(mmWord(l.wMm || (c && c.wMm), l.hMm || (c && c.hMm)))}</span><span class="ft" title="${l.densityPct != null ? esc(l.densityPct + '% of this partial sheet filled') : ''}">${esc(plural(l.placed, 'piece'))}</span></li>`; }).join('')}</ol>` : '';
    const can = !a.noEngine && (!n || placed > 0), more = !a.noEngine && !ans.fitsAll && placed > 0 && availableOf(s.items, m).some(c => !s.chain.includes(c.id));
    const pairs = [ans.splitWords, ans.sharedWords].filter(Boolean).map(t => `<p class="psSub" data-ps="pairs">${esc(t)}</p>`).join('');   // (a pair is never split: said plainly, never silent)
    if (s.adding && !busy) return `<div class="psAsk" data-ps="ask" role="group"><p class="psWords">${esc(lead)}</p>${list}${pairs}<p class="psSub">Pick the partial sheet that takes the rest. It is filled after the ones above.</p><div class="psAskBtns"><button type="button" class="btn ghost xs" data-ps="back-ask">Back</button></div></div>`;
    return `<div class="psAsk" data-ps="ask" role="group" aria-label="What using ${many ? 'these partial sheets' : 'this partial sheet'} does"><p class="psWords">${esc(lead)}</p>${list}${pairs}${sub ? `<p class="psSub">${esc(sub)}</p>` : ''}${ans.words && /trial/i.test(ans.words) ? '<p class="psSub">A quick trial pack: the real nest can place a few more or fewer.</p>' : ''}${a.noEngine ? '<p class="psSub">Seating pieces on a partial sheet is not ready on this page yet.</p>' : ''}`
      + (busy ? `<div class="psBusy" role="status"><span class="psSpin" aria-hidden="true"></span>${esc(a.text || `Seating ${plural(n, 'piece')}…`)}</div>` : '')
      + `<div class="psAskBtns"><button type="button" class="btn ghost xs" data-ps="cancel"${busy ? ' disabled' : ''}>Cancel</button>${more ? `<button type="button" class="btn ghost xs" data-ps="add"${busy ? ' disabled' : ''}>Add another partial sheet</button>` : ''}<button type="button" class="btn sage xs" data-ps="use"${busy || !can ? ' disabled' : ''}>${many ? `Use these ${ans.links.length}` : 'Use this one'}</button></div></div>`;
  }
  const lockedWhy = sh => {
    if (!sh) return '';
    try { const E = ENG(), r = E && E.canSeat ? E.canSeat(sh) : null; if (r) return r.ok === false ? (r.why || 'This sheet cannot use a partial sheet now.') : ''; } catch (_) {}
    return +sh.laserDoneAt > 0 ? `${sheetWord(sh)} is Completed (laser cut), so it stays on the sheet it was cut from.` : '';   // (the engine's canSeat is the rule; without it only a Completed sheet refuses: a recorded cut is set aside by the move)
  };

  /* ── the rule, as ONE compact two-option switch (the window puts it at the top of the Sheet menu; paintPolicy keeps it true) ── */
  const policyHtml = m => `<div class="psPolicy" data-ps="policy"><div class="psPolicyRow"><span class="psPolicyLbl" id="psPol-${esc(m)}">When a sheet needs more metal</span>`
    + `<div class="psSwitch" role="radiogroup" aria-labelledby="psPol-${esc(m)}">`
    + `<label class="psRadio" data-v="auto"><input type="radio" name="psMode-${esc(m)}" value="auto"><span>Reuse partial sheets automatically</span></label>`
    + `<label class="psRadio" data-v="new"><input type="radio" name="psMode-${esc(m)}" value="new"><span data-ps="newlabel">Offer a brand new sheet</span></label></div></div>`
    + `<div class="psNewSize" data-ps="size" hidden><div class="solidSize"><label>Width <span>mm</span><input type="number" min="5" max="500" step="0.1" data-ps="w" aria-label="Width of the brand new sheet, millimetres"></label><label>Height <span>mm</span><input type="number" min="5" max="500" step="0.1" data-ps="h" aria-label="Height of the brand new sheet, millimetres"></label></div><button type="button" class="btn ghost xs" data-ps="polsave">Save size</button></div>`
    + `<span class="help" data-ps="polhelp" role="status"></span></div>`;
  function paintPolicy(m) {
    const s = stOf(m), el = s.el; if (!el) return;
    const view = el.querySelector('[data-ps="policy"]'); if (!view) return;
    const p = policyOf(m), mode = s.polDraft || p.mode;
    for (const r of view.querySelectorAll('.psRadio')) { const on = r.dataset.v === mode; r.classList.toggle('on', on); const inp = r.querySelector('input[type=radio]'); if (inp.checked !== on) inp.checked = on; inp.disabled = !!s.polBusy; }
    const size = q(view, 'size'), w = q(view, 'w'), h = q(view, 'h'), save = q(view, 'polsave'), help = q(view, 'polhelp'), lbl = q(view, 'newlabel');
    lbl.textContent = `Offer a brand new sheet at ${mmWord(p.wMm, p.hMm)}`;
    size.hidden = mode !== 'new';
    for (const [inp, v] of [[w, p.wMm], [h, p.hMm]]) { inp.disabled = !!s.polBusy; if (!inp._draft && inp !== doc.activeElement) inp.value = +(+v).toFixed(2); }
    save.disabled = !!s.polBusy; save.textContent = s.polBusy ? 'Saving…' : 'Save size';
    const msg = s.polBusy ? '<span class="psBusy"><span class="psSpin" aria-hidden="true"></span>Saving…</span>' : esc(s.polMsg || (mode === 'new' ? '5–500 mm per side' : ''));
    if (help._h !== msg) { help._h = msg; help.innerHTML = msg; }
  }
  /** A change in what the window shows (the answer, the chain, the rule): the rule is drawn again and the window redraws its repository. */
  function paintView(m) {
    const s = stOf(m); if (!s.el) return;
    paintPolicy(m);
    if (s.repaint) { try { s.repaint(); } catch (e) { try { console.warn('partial sheets: repaint', e); } catch (_) {} } }
  }
  /** What the window needs to know to skip a redraw that would change nothing. */
  const sig = m => { const s = stOf(m), pol = policyOf(m); return JSON.stringify([s.chain, s.adding, s.ask && [s.ask.phase, s.ask.text, s.ask.answer && [s.ask.answer.rest, s.ask.answer.fitsAll, s.ask.answer.links.length, s.ask.answer.splitWords, s.ask.answer.sharedWords], s.ask.noEngine], lockedWhy(s.sh), piecesOn(s.sh), pol.mode === 'new' ? [pol.wMm, pol.hMm] : 0]); };

  /** The Use this one of a card (the window puts it in the card's actions): only for an available sheet of this sheet's metal. */
  function useButton(m, c) {
    const s = stOf(m); if (!c || (c.status || 'available') !== 'available' || (c.metal && c.metal !== m)) return '';
    const why = lockedWhy(s.sh), at = s.chain.indexOf(c.id), busy = !!(s.ask && s.ask.phase === 'seating');
    if (at >= 0) return `<button type="button" class="btn sage xs" data-ps="pick" data-id="${esc(c.id)}" aria-pressed="true"${busy ? ' disabled' : ''}>${s.chain.length > 1 ? `Chosen · ${at + 1}` : 'Chosen'}</button>`;
    return `<button type="button" class="btn sage xs" data-ps="pick" data-id="${esc(c.id)}" aria-pressed="false"${why ? ` aria-disabled="true" title="${esc(why)}"` : ''}${busy ? ' disabled' : ''}>Use this one</button>`;
  }
  /** The answer in words for the sheet's pick (Use this one / Cancel inside it), or ''. The window puts it after the row of the card picked. */
  const askFor = m => { const s = stOf(m); return s.ask ? askHtml(m, s.sh, s) : ''; };
  const pickedId = m => { const s = stOf(m); return s.ask && s.chain.length ? s.chain[s.chain.length - 1] : null; };

  /* ── opening and closing: the Options window opens the menu (OptionsStudio calls open(m) / close(m)); nothing is read here ── */
  function setOpen(m, on) {
    const s = stOf(m); s.open = !!on;
    if (on) { s.chain = []; s.adding = false; s.ask = null; s.entered++; s.polDraft = null; s.polMsg = ''; s.items = null; }
    else { s.chain = []; s.adding = false; s.ask = null; s.seq++; }
  }
  /** The window's own element (the Sheet menu's source area) and the function that redraws its repository. */
  function bind(m, o = {}) { const s = stOf(m); s.el = o.el || null; s.repaint = typeof o.repaint === 'function' ? o.repaint : null; if (s.el) wire(s.el, m); paintPolicy(m); }
  function unbind(m) { const s = stOf(m); s.el = null; s.repaint = null; }
  /** The cards the window read (every metal and status): the available ones of this metal name the answer and the chain. */
  function setItems(m, list) { stOf(m).items = Array.isArray(list) ? list : null; }

  /* ── a pick: the answer in words first, then Use this one or Cancel ── */
  async function fallbackPreview(sh, ids) {   // (the engine is not on the page: PartialSheets.plan's estimate, so the panel is never empty; nothing can be used then)
    const P = PS(), m = sh.metal, C = root.CN;
    if (!P || !P.plan) return { ok: false, reason: 'Partial sheets cannot be checked on this page yet.' };
    const items = availableOf(stOf(m).items, m), picked = ids.map(id => items.find(c => c.id === id)).filter(Boolean); if (!picked.length) return { ok: false, reason: 'This partial sheet is not available any more. Press Refresh.' };
    const KEY = (root.CharmNestPartial && root.CharmNestPartial.groupKeyOf) || (c => c.order || c.id), pieces = (sh.charms || []).map(c => ({ areaMm2: Math.max(1, (C && C.inflatedArea ? C.inflatedArea(c) : 0) * MM_PER_PT * MM_PER_PT), group: KEY(c), ...(c.side === 'L' || c.side === 'R' ? { side: c.side } : {}) }));
    await P.plan(m, pieces, {});
    // (a pair needs two places: each order goes whole onto a partial or on to the next, so a partial that holds 3 pieces takes one pair and not a pair and a half)
    const sizes = []; { const by = new Map(); for (const c of sh.charms || []) { const k = c.order || c.id; by.set(k, (by.get(k) || 0) + 1); } sizes.push(...by.values()); }
    let left = pieces.length; const links = [];
    for (const c of picked) { let room = Math.round((c.estimate && c.estimate.pieces) || 0), put = 0; for (let i = 0; i < sizes.length; i++) if (sizes[i] > 0 && sizes[i] <= room) { room -= sizes[i]; put += sizes[i]; sizes[i] = 0; } left -= put; if (put) links.push({ partialId: c.id, wMm: c.wMm, hMm: c.hMm, areaMm2: c.areaMm2, placed: put }); }
    return { ok: true, noEngine: true, pieces: pieces.length, fitsAll: pieces.length > 0 && left === 0, links, rest: left, next: left ? 'new' : 'none', contWords: '', words: '' };
  }
  async function runPreview(m) {
    const s = stOf(m), sh = s.sh; if (!sh || !s.chain.length) return;
    const seq = ++s.seq, ids = s.chain.slice(); s.ask = { phase: 'checking', seq, text: '' }; paintView(m);
    let ans;
    try {
      const E = ENG();
      ans = E && E.preview ? normalize(await E.preview(sh, ids, { onStep: st => { const t = st && st.text; if (t && s.seq === seq && s.ask && s.ask.phase === 'checking' && s.ask.text !== t) { s.ask.text = t; paintView(m); } } })) : await fallbackPreview(sh, ids);
    } catch (e) { ans = { ok: false, reason: e && e.message ? e.message : String(e) }; }
    if (s.seq !== seq || !s.ask) return;   // another pick or Cancel came meanwhile
    s.ask = ans && ans.ok ? { phase: 'answer', seq, answer: ans, noEngine: !!ans.noEngine } : { phase: 'bad', seq, text: (ans && ans.reason) || 'This partial sheet cannot be used now.' };
    paintView(m);
    const pv = s.el, go = pv && pv.querySelector('[data-ps="use"]:not([disabled])'); if (go) go.focus({ preventScroll: true }); else { const c = pv && pv.querySelector('[data-ps="cancel"],[data-ps="back-ask"]'); if (c) c.focus({ preventScroll: true }); }
  }
  /** A press on a card: it is the chain's first (the answer comes first, nothing is claimed); while "Add another partial sheet" waits it is the next one. */
  function pick(m, id) {
    const s = stOf(m), sh = s.sh; if (!sh) return;
    if (s.ask && s.ask.phase === 'seating') return;
    const why = lockedWhy(sh); if (why) { root.CN && root.CN.toast && root.CN.toast(why, 'bad'); return; }
    if (s.adding) { if (s.chain.includes(id)) return; s.chain = s.chain.concat(id); s.adding = false; }
    else { if (s.chain.length === 1 && s.chain[0] === id && s.ask && s.ask.phase !== 'bad') return; s.chain = [id]; }
    runPreview(m);
  }
  function cancelAsk(m) {   // Cancel changes nothing: nothing was claimed, nothing moved
    const s = stOf(m); if (!s.ask && !s.chain.length) return false; if (s.ask && s.ask.phase === 'seating') return false;
    if (s.adding) { s.adding = false; paintView(m); return true; }
    const id = s.chain[0]; s.seq++; s.ask = null; s.chain = []; paintView(m);
    const pv = s.el, c = pv && [...pv.querySelectorAll('[data-ps="pick"]')].find(x => x.dataset.id === id); if (c) c.focus({ preventScroll: true });
    return true;
  }
  /** The window closes on the work (as Merge sheets does): a promise that settles once it is gone (the re-seat plays after that). */
  function closeOptions(m) {
    const R = root.Gate && root.Gate.state ? root.Gate.state() : null; if (R) (R.optionsOpen || (R.optionsOpen = {}))[m] = false;
    try { if (root.OptionsStudio && root.OptionsStudio.close) return root.OptionsStudio.close(m, { focus: false }); } catch (_) {}
    return Promise.resolve();
  }
  const STEP_WORDS = { check: 'Checking the sheet…', release: "Giving the sheet's old metal back…", claim: 'Reserving the partial sheet…', nest: 'Nesting the pieces on the partial sheet…', continue: 'Moving the rest to the next partial sheet…', save: 'Saving the layout…' };
  async function commit(m) {
    const s = stOf(m), a = s.ask, sh = s.sh, E = ENG(); if (!a || a.phase !== 'answer' || !sh) return;
    if (!E || !E.seat) { a.noEngine = true; paintView(m); return; }
    const ids = s.chain.slice(), answer = a.answer; a.phase = 'seating'; a.text = STEP_WORDS.check; paintView(m);
    let cap = null; try { cap = capture(sh, answer); } catch (e) { cap = null; }   // (the card as it is now: the scene is made of it, and plays at once, whatever the work then takes)
    let started = false;
    // the work has begun on the sheet: the panel closes (as Merge sheets does) so the sheet is in full view, and the re-seat plays
    const go = () => { if (started) return; started = true; setOpen(m, false); Promise.resolve(closeOptions(m)).then(() => { if (cap) playScene(cap, answer); }); };   // (the window is gone first: the re-seat plays over the card)
    const onStep = st => {
      const k = st && st.key; if (!k) return;
      if (s.ask === a) { a.text = (st && st.text) || STEP_WORDS[k] || a.text; paintView(m); }
      if (k === 'nest' || k === 'continue') go();
    };
    try {
      const r = await E.seat(sh, ids, { onStep, confirmed: true });
      if (!r || r.ok === false) throw new Error((r && (r.why || r.error)) || 'The partial sheet was not used');
      try { PS() && PS().changed && PS().changed(); } catch (_) {}
      go();   // (an engine that never said it was nesting: the panel closes and the scene plays now)
      const moved = r.moved != null ? r.moved | 0 : answer.links.reduce((t, l) => t + l.placed, 0), left = r.continues != null ? r.continues | 0 : answer.rest;
      root.CN && root.CN.toast && root.CN.toast(left ? `${plural(moved, 'piece')} seated on the partial sheet; ${left} continue on the next one` : `${plural(moved || answer.pieces, 'piece')} seated on the partial sheet`, 'ok');
      // (the list is read again the next time the window opens: one call then, none now)
    } catch (e) {
      const text = e && e.message ? e.message : String(e);
      if (s.ask === a) { s.ask = { phase: 'bad', seq: s.seq, text }; paintView(m); }
      root.CN && root.CN.toast && root.CN.toast('Partial sheet not used: ' + text, 'bad');
    }
  }

  /* ── the policy: automatic reuse, or a brand new sheet whose size the person sets ── */
  async function savePolicy(m, mode, wMm, hMm) {
    const s = stOf(m), P = PS(); if (!P || !P.setPolicy) return;
    s.polBusy = true; s.polMsg = ''; paintView(m);
    try { await P.setPolicy(m, { mode, wMm, hMm }); s.polDraft = null; s.polMsg = 'Saved'; for (const i of ['w', 'h']) { const pv = s.el, inp = pv && pv.querySelector(`[data-ps="${i}"]`); if (inp) inp._draft = false; } }
    catch (e) { s.polDraft = null; s.polMsg = (e && e.message) || 'Not saved'; root.CN && root.CN.toast && root.CN.toast('Rule not saved: ' + s.polMsg, 'bad'); }
    finally { s.polBusy = false; paintView(m); setTimeout(() => { if (s.polMsg === 'Saved') { s.polMsg = ''; paintView(m); } }, 2500); }
  }
  function sizeValues(view) {
    const w = +q(view, 'w').value, h = +q(view, 'h').value;
    return [w, h].every(n => Number.isFinite(n) && n >= 5 && n <= 500) ? { w, h } : null;
  }

  /* ── events (delegated, once per window: bind) ── */
  function wire(view, m) {
    if (view._psWired) return; view._psWired = true;
    const s = stOf(m);
    view.addEventListener('click', e => {
      const t = e.target;
      if (t.closest('[data-ps="cancel"]')) { cancelAsk(m); return; }   // (Close on a refusal and Cancel on an answer: nothing was claimed, nothing moved)
      if (t.closest('[data-ps="back-ask"]')) { s.adding = false; paintView(m); return; }
      if (t.closest('[data-ps="add"]')) { s.adding = true; paintView(m); const c = view.querySelector('[data-ps="pick"]:not([aria-pressed="true"])'); if (c) c.focus({ preventScroll: true }); return; }
      if (t.closest('[data-ps="use"]')) { commit(m); return; }
      if (t.closest('[data-ps="polsave"]')) {
        const v = sizeValues(view); if (!v) { s.polMsg = 'Use a width and height between 5 and 500 mm'; paintView(m); root.CN && root.CN.toast && root.CN.toast(s.polMsg, 'bad'); return; }
        savePolicy(m, 'new', v.w, v.h); return;
      }
      const b = t.closest('[data-ps="pick"]'); if (b && !b.disabled) pick(m, b.dataset.id);   // (Use this one on a card: it is the chain's first; the answer comes first)
    });
    view.addEventListener('change', e => {
      const r = e.target.closest && e.target.closest('.psRadio input[type=radio]'); if (!r) return;
      const p = policyOf(m); if (r.value === policyOf(m).mode && !s.polDraft) return;
      s.polDraft = r.value; paintView(m);
      const v = r.value === 'new' ? sizeValues(view) || { w: p.wMm, h: p.hMm } : { w: p.wMm, h: p.hMm };
      savePolicy(m, r.value, v.w, v.h);
    });
    view.addEventListener('input', e => { const i = e.target; if (i && i.matches && i.matches('[data-ps="w"],[data-ps="h"]')) { i._draft = true; if (s.polMsg) { s.polMsg = ''; paintView(m); } } });
    view.addEventListener('keydown', e => {
      if (e.key === 'Escape') { if ((s.ask && s.ask.phase === 'seating') || cancelAsk(m)) { e.preventDefault(); e.stopPropagation(); } return; }   // (an answer, or the choosing of another partial sheet, is cancelled first; with none, Esc goes on: the window)
      if (e.key === 'Enter' && e.target.matches && e.target.matches('[data-ps="w"],[data-ps="h"]')) { e.preventDefault(); const sv = q(view, 'polsave'); if (sv && !sv.hidden) sv.click(); }
    });
  }

  // (the data layer may arrive after this file: it is subscribed to once, the first time the menu is drawn with it)
  const subs = { ps: false };
  function subscribe() {
    try {
      const P = PS();
      if (P && P.on && !subs.ps) {
        subs.ps = true;
        P.on(ev => { if (ev && ev.reason === 'policy' && ev.metal) paintPolicy(ev.metal); });   // (the rule was saved: the switch says so; the repository is the window's)
      }
    } catch (_) {}
  }

  /* ── what renderRelease calls on every draw of the Options controls ── */
  function paint(sh, node) {
    const m = sh && sh.metal; if (!m || !METAL[m] || !node) return;
    subscribe();
    const s = stOf(m); s.sh = sh; s.node = node;
    if (s.open && s.el) paintView(m);
  }
  function changed() {   // a cut made on this page saved a new partial sheet (RoseStock.record): the data layer forgets its cache (the window reads the list again the next time it opens)
    try { PS() && PS().changed && PS().changed(); } catch (_) {}
  }

  /* ── the re-seat, seen (Paul: "beautiful animations ... when a partial is chosen the pieces visibly re-seat onto the new partial"):
        the sheet and the partial sheet(s) the pieces go to side by side, each under its name and the number of pieces it holds; the
        pieces lift one by one and fly on arcs onto the partial sheet, at their true size; each partial pulses its fill as it takes its
        pieces and says "+N"; then the card is there with the nest's own layout. About two and a half seconds, transform and opacity
        only, every step on the animation clock; it never holds up the work (it starts once the work was handed over); a hidden tab,
        a resize, a card laid out anew ends it with a quick fade; reduced motion shows none of it. Whatever it adds goes when it ends. ── */
  const FX = new Set();
  function capture(sh, answer) {
    const C = root.CN, M = root.Motion; if (!C || !M || M.reduced() || doc.hidden || typeof requestAnimationFrame !== 'function' || typeof C.paintPreview !== 'function') return null;
    const card = C.S.sheets[sh.metal] && C.S.sheets[sh.metal].cardEl; if (!card) return null;
    const wrap = card.querySelector('.shPreviewWrap'), cv = card.querySelector('[data-r="canvas"]'), v = sh._view;
    if (!wrap || !cv || !v || !cv.width || !wrap.getClientRects().length) return null;
    const r = wrap.getBoundingClientRect(); if (r.width < 160 || r.bottom < 60 || r.top > innerHeight - 60) return null;
    const st = C.stockFor(sh.metal, sh), W = Math.min(cv.width - v.R, st.wPt * v.k), H = Math.min(cv.height - v.R, st.hPt * v.k); if (!(W >= 24 && H >= 24)) return null;
    const byId = new Map((sh.charms || []).map(c => [c.id, c]));
    const pieces = (sh.placements || []).filter(p => byId.has(p.id) && [p.cxPt, p.cyPt].every(Number.isFinite)).map(p => ({ c: byId.get(p.id), pl: { ...p } }));
    if (!pieces.length) return null;
    const bare = Object.assign(Object.create(sh), { placements: [], charms: [], probe: null, probePlaced: [], status: 'complete', sat: null, liveInfo: null, selected: null, _view: null });
    const full = doc.createElement('canvas'); full.width = cv.width; full.height = cv.height; C.paintPreview(full, bare, false, v.R);
    const ground = doc.createElement('canvas'); ground.width = Math.round(W); ground.height = Math.round(H);
    // (more pieces than fly: the card's own picture, all of them on it, is the ground, so the first frame is the card as it was)
    ground.getContext('2d').drawImage(pieces.length > 90 ? cv : full, v.R, v.R, W, H, 0, 0, ground.width, ground.height);
    return { sh, m: sh.metal, card, wrap, cv, v, W, H, cvW: cv.width, cvH: cv.height, pieces: pieces.slice(0, 90), ground, answer, cards: new Map(availableOf(stOf(sh.metal).items, sh.metal).map(c => [c.id, c])) };   // (the cards as the list showed them: the one taken leaves the list when the work is handed over)
  }
  /** One piece alone, drawn as the card draws a placed piece, on a clear ground: its canvas and where it lies on the sheet (canvas pixels). */
  function piece(c, pl, k, pad) {
    const C = root.CN, P = root.CharmNestPDF;
    const d = Math.hypot(+c.widthPt || 0, +c.heightPt || 0) * (pl.scale || 1), w = +pl.wPt || d, h = +pl.hPt || d;
    if (!(w > 0 && h > 0) || !c.outline || !c.centerPt || !P) return null;
    const px = (pl.xPt != null ? pl.xPt : pl.cxPt - w / 2) * k, py = (pl.yPt != null ? pl.yPt : pl.cyPt - h / 2) * k;
    const x0 = Math.floor(px - pad), y0 = Math.floor(py - pad), cv = doc.createElement('canvas');
    cv.width = Math.max(2, Math.ceil(px + w * k + pad) - x0); cv.height = Math.max(2, Math.ceil(py + h * k + pad) - y0);
    const g = cv.getContext('2d'), cx = c.centerPt[0], cy = c.centerPt[1], tx = (x, y) => [(x - cx) * k, (cy - y) * k];
    g.translate(pl.cxPt * k - x0, pl.cyPt * k - y0); g.rotate((+pl.angle || 0) * Math.PI / 180); if (pl.scale) g.scale(pl.scale, pl.scale);
    g.fillStyle = c.custom && typeof CUSTOM_TINT === 'string' ? CUSTOM_TINT : 'rgba(200,162,78,.10)';
    g.beginPath(); P.pathToCanvas(g, c.outline, tx); for (const ln of (C && C.cutLinesOf ? C.cutLinesOf(c) : [])) P.pathToCanvas(g, ln, tx); g.fill('evenodd');
    P.drawCharm(g, c, tx, k);
    return { cv, x0, y0 };
  }
  function playScene(cap, answer, result) {
    const asked = performance.now();
    requestAnimationFrame(() => { if (performance.now() - asked > 700) return; try { scene(cap, answer, result); } catch (e) { console.warn('partial sheets: motion', e); } });   // (a hidden tab runs its frames late, if at all: a scene of what happened long ago is not shown)
  }
  function scene(cap, answer, result) {
    const { card, wrap, cv, v, m } = cap, M = root.Motion;
    if (!card.isConnected || !wrap.isConnected || !cv.isConnected || doc.hidden || !M || M.reduced() || !wrap.getClientRects().length) return;
    for (const f of [...FX]) if (f.card === card) f.stop();
    const o = card.getBoundingClientRect(), ox = o.left + card.clientLeft, oy = o.top + card.clientTop;
    const local = r => ({ x: r.left - ox, y: r.top - oy, w: r.width, h: r.height });
    const wr = local(wrap.getBoundingClientRect()), cvr = local(cv.getBoundingClientRect()), sx = cvr.w / cap.cvW;
    const home = { x: cvr.x + v.R * sx, y: cvr.y + v.R * sx, w: cap.W * sx, h: cap.H * sx }, kpx = v.k * sx;   // css pixels per point, on the card
    // what each partial sheet takes (the answer's links, in fill order), at most three are drawn
    const byId = cap.cards;
    const lots = (answer.links || []).slice(0, 3).map(l => ({ e: { partialId: l.partialId, fits: l.placed, name: cardName(byId.get(l.partialId)) }, c: byId.get(l.partialId) || null })).filter(x => x.c);
    if (!lots.length) return;
    const list = [{ old: true, w: home.w, h: home.h }, ...lots.map(x => ({ x, w: (+x.c.sheetWMm || REF.w) / MM_PER_PT * kpx, h: (+x.c.sheetHMm || REF.h) / MM_PER_PT * kpx }))], n = list.length;
    const tagH = 26, pad = 14, gap = Math.max(16, Math.round(wr.w * .04)), aw = wr.w - 2 * pad, ah = wr.h - 2 * pad - tagH;
    const total = list.reduce((a, b) => a + b.w, 0), tall = Math.max(...list.map(b => b.h)), s = Math.max(.14, Math.min(1, (aw - gap * (n - 1)) / total, ah / tall));
    let cx = wr.x + (wr.w - (total * s + gap * (n - 1))) / 2;
    const slots = list.map(b => { const r = { x: cx, y: wr.y + pad + tagH + Math.max(0, (ah - b.h * s) / 2), w: b.w * s, h: b.h * s }; cx += b.w * s + gap; return r; });
    const q0 = slots[0].w / cap.W;   // one canvas pixel of the card, in the scene
    const fx = doc.createElement('div'); fx.className = 'mergeFx psFx'; fx.setAttribute('aria-hidden', 'true'); card.appendChild(fx);
    const put = (node, b, cls, z) => { node.className = cls; Object.assign(node.style, { left: b.x + 'px', top: b.y + 'px', width: b.w + 'px', height: b.h + 'px', zIndex: String(z) }); fx.appendChild(node); return node; };
    const anims = []; let live = true, timer = 0, raf = 0;
    const play = (node, frames, opt) => { const a = node.animate(frames, opt); anims.push(a); return a; };
    const cue = (ms, fn) => { const a = new Animation(new KeyframeEffect(null, [], { duration: Math.max(0, ms) }), doc.timeline); anims.push(a); a.onfinish = () => { if (!live) return; try { fn(); } catch (e) { console.warn('partial sheets: motion', e); stop(true); } }; a.play(); return a; };
    const from = (b, h) => `translate(${(h.x - b.x).toFixed(2)}px,${(h.y - b.y).toFixed(2)}px) scale(${(h.w / b.w).toFixed(4)})`;
    const pose = (b, a, px, py, sc = 1, rot = 0) => `translate(${(px - b.x).toFixed(2)}px,${(py - b.y).toFixed(2)}px) rotate(${rot.toFixed(2)}deg) scale(${sc.toFixed(4)}) translate(${(-a.x).toFixed(2)}px,${(-a.y).toFixed(2)}px)`;
    const homeOf = (b, slot) => ({ x: home.x + (b.x - slot.x) / s, y: home.y + (b.y - slot.y) / s, w: b.w / s, h: b.h / s });
    const onVis = () => { if (doc.hidden) stop(false); }, onResize = () => stop(true);
    const scn = { card, stop: (soft = false) => stop(soft) };
    function stop(soft) {
      if (!live) return; live = false; FX.delete(scn); clearTimeout(timer); if (raf) cancelAnimationFrame(raf);
      doc.removeEventListener('visibilitychange', onVis); root.removeEventListener('resize', onResize);
      const gone = () => { for (const a of anims) try { a.cancel(); } catch (_) {} fx.remove(); };
      if (soft && fx.isConnected && !doc.hidden) fx.animate([{ opacity: 1 }, { opacity: 0 }], { duration: 160, easing: 'ease-out', fill: 'forwards' }).finished.then(gone, gone); else gone();
    }
    // the card laid out anew under it: a row that appears above the picture (the chain strip, the nest's status line) moves the scene
    // with the picture; the picture itself changing its width, or another sheet being shown, ends it
    const w0 = wrap.offsetWidth, wc = { x: wr.x + wr.w / 2, y: wr.y + wr.h / 2 };
    const follow = () => {
      raf = 0; if (!live) return;
      if (!card.isConnected || !wrap.isConnected || Math.abs(wrap.offsetWidth - w0) >= 3) return stop(true);
      const o2 = card.getBoundingClientRect(), r = wrap.getBoundingClientRect(), dx = r.left + r.width / 2 - (o2.left + card.clientLeft) - wc.x, dy = r.top + r.height / 2 - (o2.top + card.clientTop) - wc.y;
      if (Math.abs(dx) > 120 || Math.abs(dy) > 160) return stop(true);
      const t = Math.abs(dx) < .5 && Math.abs(dy) < .5 ? '' : `translate(${dx.toFixed(1)}px,${dy.toFixed(1)}px)`; if (fx.style.transform !== t) fx.style.transform = t;
      raf = requestAnimationFrame(follow);
    };
    raf = requestAnimationFrame(follow);

    /* the pictures: the empty sheet, each partial sheet as the card shows one, the pieces each a canvas of their own */
    const veil = put(doc.createElement('div'), wr, 'mergeFxVeil', 1);
    const ground = put(cap.ground, slots[0], 'mergeFxSheet', 10);
    const accent = `var(--m-${m}, ${GREEN})`;
    const lotNodes = lots.map((x, i) => { const d = doc.createElement('div'); d.innerHTML = sheetSvg(x.c, { frame: false }); d.style.setProperty('--accent', accent); return put(d, slots[i + 1], 'psFxSlot', 9 - i); });
    const counts = [cap.pieces.length, ...lots.map(() => 0)];
    const digits = String(cap.pieces.length).length;
    const tags = list.map((b, i) => {
      const tg = doc.createElement('span'), nb = doc.createElement('b'); tg.append(b.old ? sheetWord(cap.sh) : (b.x.e.name || b.x.c.sourceSheet || 'Partial sheet')); nb.textContent = String(counts[i]); nb.style.minWidth = digits + 'ch'; tg.append(nb);
      put(tg, { x: 0, y: 0, w: 0, h: 0 }, 'psFxTag' + (i === 1 ? ' on' : ''), 30); Object.assign(tg.style, { width: '', height: '', maxWidth: Math.max(80, slots[i].w + 40) + 'px', overflow: 'hidden', textOverflow: 'ellipsis' });
      const w = tg.offsetWidth || 70, h = tg.offsetHeight || 18, box = { x: slots[i].x + slots[i].w / 2 - w / 2, y: slots[i].y - h - 7, w, h };
      Object.assign(tg.style, { left: box.x + 'px', top: box.y + 'px' }); tg._num = nb; return tg;
    });
    const recount = (i, d) => { counts[i] = Math.max(0, counts[i] + d); tags[i]._num.textContent = String(counts[i]); play(tags[i]._num, [{ transform: 'scale(1)' }, { transform: 'scale(1.35)', offset: .3 }, { transform: 'scale(1)' }], { duration: 300, easing: 'ease-out' }); };
    const pad3 = 3 * v.k + 4;
    const flyers = [];
    for (const pcs of cap.pieces) {
      let pc = null; try { pc = piece(pcs.c, pcs.pl, v.k, pad3); } catch (_) {}
      if (!pc) continue;
      const b = { x: slots[0].x + pc.x0 * q0, y: slots[0].y + pc.y0 * q0, w: pc.cv.width * q0, h: pc.cv.height * q0 };
      flyers.push({ c: pcs.c, pl: pcs.pl, node: put(pc.cv, b, 'mergeFxPiece', 20), b, a: { x: (pcs.pl.cxPt * v.k - pc.x0) * q0, y: (pcs.pl.cyPt * v.k - pc.y0) * q0 } });
    }
    FX.add(scn); doc.addEventListener('visibilitychange', onVis); root.addEventListener('resize', onResize);
    timer = setTimeout(() => stop(false), 30000);   // (never needed: the last step ends it)

    /* which piece goes to which partial sheet: in the order they lie, as many as the answer says each takes (the rest wait: they go on) */
    let at = 0; const targets = lots.map((x, i) => {
      const take = Math.max(0, Math.min(+x.e.fits || 0, flyers.length - at)), mine = flyers.slice(at, at + take); at += take;
      const sl = slots[i + 1], pxMm = sl.w / (+x.c.sheetWMm || REF.w), bb = x.c.bboxMm || { x: 0, y: 0, w: x.c.wMm || 0, h: x.c.hMm || 0 };
      const dia = mine.length ? mine.reduce((t, p) => t + Math.max(+p.pl.wPt || 0, +p.pl.hPt || 0) * kpx * s, 0) / mine.length : 0;
      let cell = Math.max(4, dia * 1.12); const bw = bb.w * pxMm, bh = bb.h * pxMm;
      while (cell > 4 && Math.floor(bw / cell) * Math.floor(bh / cell) < mine.length) cell *= .93;
      const cols = Math.max(1, Math.floor(bw / cell));
      mine.forEach((p, j) => { p.to = i; p.spot = { x: sl.x + bb.x * pxMm + cell / 2 + 2 + (j % cols) * cell, y: sl.y + bb.y * pxMm + cell / 2 + 2 + Math.floor(j / cols) * cell }; p.g = Math.min(1, cell * .9 / Math.max(1, dia || cell)); });
      return { sl, mine };
    });
    for (const p of flyers) if (p.to == null) p.stay = true;

    /* the timeline (ms): 0 the sheets part; 1 the pieces lift and fly one by one; 2 each partial pulses and says +N; 3 the card is there */
    const T = { unfold: 460, fly0: 520, fly: 560, spread: Math.min(640, Math.max(0, flyers.length - 1) * 55) };
    const step = flyers.length > 1 ? T.spread / (flyers.length - 1) : 0, landed = T.fly0 + T.spread + T.fly, PULSE = landed + 60, END = PULSE + 760, ease = 'cubic-bezier(.3,.1,.2,1)';
    play(veil, [{ opacity: 0 }, { opacity: 1 }], { duration: 170, easing: 'ease-out', fill: 'backwards' });
    // 0 · the sheet steps aside (it was the card's own picture), the partial sheets slide out beside it, each under its name
    play(ground, [{ transform: from(slots[0], home) }, { transform: 'none' }], { delay: 40, duration: T.unfold, easing: ease, fill: 'backwards' });
    for (const p of flyers) play(p.node, [{ transform: from(p.b, homeOf(p.b, slots[0])) }, { transform: 'none' }], { delay: 40, duration: T.unfold, easing: ease, fill: 'backwards' });
    lotNodes.forEach((nd, i) => play(nd, [{ opacity: 0, transform: 'translateX(34px)' }, { opacity: 1, transform: 'none' }], { delay: 120 + i * 60, duration: T.unfold, easing: ease, fill: 'backwards' }));
    tags.forEach((tg, i) => play(tg, [{ opacity: 0, transform: 'translateY(5px)' }, { opacity: 1, transform: 'none' }], { delay: T.unfold - 60 + i * 40, duration: 260, easing: 'ease-out', fill: 'backwards' }));
    // 1 · one by one onto its partial sheet: lifted, over an arc, down at its spot (a ring where it lands); the count over each sheet follows
    flyers.forEach((p, j) => {
      const c0 = { x: p.b.x + p.a.x, y: p.b.y + p.a.y }, delay = T.fly0 + j * step;
      if (p.stay) { play(p.node, [{ opacity: 1 }, { opacity: 0 }], { delay: landed - 200, duration: 260, easing: 'ease-in', fill: 'forwards' }); return; }
      const to = p.spot, up = { x: (c0.x + to.x) / 2, y: Math.min(c0.y, to.y) - Math.min(70, 16 + Math.abs(to.x - c0.x) * .16) };
      play(p.node, [{ transform: pose(p.b, p.a, c0.x, c0.y) }, { transform: pose(p.b, p.a, c0.x, c0.y - 3, 1.07), offset: .14 }, { transform: pose(p.b, p.a, up.x, up.y, 1.08), offset: .55 }, { transform: pose(p.b, p.a, to.x, to.y, p.g) }], { delay, duration: T.fly, easing: ease, fill: 'both' });
      cue(delay + T.fly * .15, () => recount(0, -1));
      cue(delay + T.fly, () => {
        recount(p.to + 1, +1);
        const rr = Math.max(14, Math.min(40, Math.max(p.b.w, p.b.h) * .8)), r = put(doc.createElement('i'), { x: to.x - rr / 2, y: to.y - rr / 2, w: rr, h: rr }, 'mergeFxRing', 42);
        play(r, [{ transform: 'scale(.7)', opacity: 0 }, { transform: 'scale(.95)', opacity: .7, offset: .2 }, { transform: 'scale(1.35)', opacity: 0 }], { duration: 440, easing: 'cubic-bezier(.2,.7,.3,1)', fill: 'forwards' });
      });
    });
    // 2 · the partial sheets take their pieces: the fill pulses softly, and "+N" rises over each
    targets.forEach((t, i) => {
      if (!t.mine.length) return;
      const nd = lotNodes[i], fill = nd.querySelector('.psFill'), glow = put(doc.createElement('i'), t.sl, 'mergeFxGlow', 8);
      if (fill) play(fill, [{ opacity: 0 }, { opacity: .38, offset: .45 }, { opacity: 0 }], { delay: PULSE, duration: 760, easing: 'ease-in-out', fill: 'both' });
      play(glow, [{ opacity: 0 }, { opacity: 1, offset: .4 }, { opacity: 0 }], { delay: PULSE, duration: 760, easing: 'ease-in-out', fill: 'both' });
      play(nd, [{ transform: 'scale(1)' }, { transform: 'scale(1.025)', offset: .4 }, { transform: 'scale(1)' }], { delay: PULSE, duration: 700, easing: 'ease-out' });
      const pl = put(doc.createElement('span'), { x: t.sl.x + t.sl.w / 2, y: t.sl.y + t.sl.h * .42, w: 0, h: 0 }, 'mPlus mergeFxPlus', 50); Object.assign(pl.style, { width: '', height: '' }); pl.textContent = '+' + t.mine.length;
      play(pl, [{ transform: 'translate(-50%,-50%) scale(.7)', opacity: 0 }, { transform: 'translate(-50%,calc(-50% - 10px)) scale(1)', opacity: 1, offset: .3 }, { transform: 'translate(-50%,calc(-50% - 14px)) scale(1)', opacity: 1, offset: .7 }, { transform: 'translate(-50%,calc(-50% - 24px)) scale(1)', opacity: 0 }], { delay: PULSE + 80, duration: 760, easing: 'ease-out', fill: 'both' });
    });
    // 3 · the card is there with the nest's own layout: the scene fades, and what it added goes
    const done = new Animation(new KeyframeEffect(null, [], { duration: END }), doc.timeline); anims.push(done);
    done.onfinish = () => { if (live) stop(true); }; done.play();
  }

  /** Esc inside the window: an answer that waits (or the choosing of another partial sheet) is cancelled first; true when that took the key. */
  const escape = m => { const s = stOf(m); return !!((s.ask && s.ask.phase === 'seating') || cancelAsk(m)); };
  /** One sheet as the plain card, for the window while OptionsHistory has no card: o = { selected, actions }. */
  const card = (c, o = {}, i = 0) => cardHtml(c.metal, c, { chain: [] }, false, i, { selected: !!o.selected, actions: o.actions || '' });
  root.PartialSheetsUI = { paint, changed, open: m => setOpen(m, true), close: m => setOpen(m, false), bind, unbind, items: setItems, actions: useButton, ask: askFor, picked: pickedId, lock: m => lockedWhy(stOf(m).sh), sig, policyHtml, paintPolicy, escape, card, sheetSvg, edgesOf, friendly, fitWords, lastUseWords, cardName, statusWords, fx: () => ({ live: FX.size }), state: ST, _scene: capture };
})(window);
