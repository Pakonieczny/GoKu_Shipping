/* The Library's "Leftover sheets" view (GC3, Paul 7 Oct: "the user should be able to see a saved repository of all available remaining
   partial sheets that were left over after the Cut Sheet was activated in the Nest tab or ... in the Library tab").

   What it shows: one card per cut that left metal (netlify/functions/_charmNestRemnants.js saves the record in the cut's own
   transaction): the leftover's exact outline at TRUE SCALE in one 100 x 50 mm frame (filled in the metal's colour, the green cut
   edge dashed, as the green line is drawn on the sheet), its real width x height and area, the metal, which sheet and set it came
   from, when it was cut and by whom, and whether it is still available. Newest first. Filters: metal (RG, 10K, 14K) and available /
   used. A person can mark an available one used or discarded (and take their own mark back); the name is the signed-in one.

   Cost (the Google bill): ONE list op (remnantList: a limit and a field mask), asked when the view opens, when a cut is made from this
   page (LeftoverSheets.changed, called by RoseStock.record), when another tab of this computer says it wrote the Library, when the
   person presses Refresh and when the tab is looked at again after a minute. Each of them sends the counter the last answer was made
   at (ifRev): nothing new is answered from ONE tiny document. No timer, no polling, nothing read for a card.

   It is a third choice beside Sheets | Sets in the Library bar (added here; the Library's own code is not touched): opening it
   hides the Current / Completed lists, and any press on their tabs or on Sheets / Sets brings them back. */
(function init() {
  'use strict';
  if (!window.CN || !document.getElementById('libKind') || !document.getElementById('libView')) { setTimeout(init, 150); return; }
  const C = window.CN, doc = document, view = doc.getElementById('libView'), kind = doc.getElementById('libKind');
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const api = (body, label) => C.api('charmNestLibrary', body, { label: label || 'Leftover sheets', quiet: false });
  const METALS = [['rose', 'RG', 'RG 14/20'], ['gold10k', '10K', '10K Gold'], ['gold14k', '14K', '14K Gold']];
  const SIZE = { w: 100, h: 50 };   // the one frame every sheet picture is drawn in (mm)
  const GREEN = '#008974';           // the green dash line's colour (charm-nest-rose-ui.js)

  const st = { open: false, status: 'available', metal: 'all', cache: { available: null, all: null }, busy: {}, error: '', at: 0, marking: new Set(), timer: 0, prevOn: null, backfill: 0 };
  const num = n => Math.round(n * 10) / 10;
  const when = at => Number.isFinite(at) ? new Date(at).toLocaleString(undefined, { year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) : 'Time not recorded';
  const mm = (w, h) => `${num(w)} × ${num(h)} mm`;

  /* ── styles (the Library's own tokens; the card is the Library card's look) ── */
  const css = doc.createElement('style');
  css.textContent = `
#libLeft{display:none;flex-direction:column;gap:12px}
#libView.leftOpen #libLeft{display:flex}
#libView.leftOpen #libBody,#libView.leftOpen #libDone{display:none!important}
#libView.leftOpen .libBar .libFilterGroup,#libView.leftOpen .libBar #libSearch,#libView.leftOpen .libBar .cnListTools{display:none}
.libBar #libKind button[data-k=leftovers]{white-space:nowrap}
@container workspace (max-width:1370px){.libBar #libKind button[data-k=leftovers] .lng{display:none}}
.lsBar{display:flex;align-items:center;gap:10px 14px;flex-wrap:wrap}
.lsTitle{display:flex;flex-direction:column;gap:2px;min-width:0;margin-right:auto}
.lsTitle b{font:15px var(--serif)}
.lsTitle span{font-size:11.5px;color:var(--ink45)}
.lsChips{display:inline-flex;align-items:center;gap:7px;flex-wrap:wrap}
.lsBar .seg button{padding:4px 11px;font-size:11px}
.lsGrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:12px}
.lsCard{background:var(--card);border:1px solid var(--line);border-left:5px solid var(--accent);border-radius:12px;box-shadow:var(--sh);padding:10px 12px;display:flex;flex-direction:column;gap:7px;min-width:0;--accent:var(--ink25)}
.lsCard[data-m=rose]{--accent:var(--m-rose)}.lsCard[data-m=gold10k]{--accent:var(--m-gold10k)}.lsCard[data-m=gold14k]{--accent:var(--m-gold14k)}.lsCard[data-m=gold]{--accent:var(--m-gold)}.lsCard[data-m=silver]{--accent:var(--m-silver)}
.lsCard[data-s=used],.lsCard[data-s=discarded]{opacity:.78}
.lsTop{display:flex;align-items:center;gap:8px;min-width:0}
.lsTop .nm{font:600 13px var(--sans);min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.lsTop .set{font:600 10.5px var(--mono);color:var(--ink70);background:var(--paper2);border:1px solid var(--line);border-radius:6px;padding:1px 6px;white-space:nowrap}
.lsTop .tm{margin-left:auto;font:10.5px var(--mono);color:var(--ink45);white-space:nowrap}
.lsPv{display:block;width:100%;height:auto;background:var(--card2);border:1px solid var(--line2);border-radius:8px}
.lsSize{display:flex;align-items:baseline;gap:8px;flex-wrap:wrap;font-size:11px;color:var(--ink70)}
.lsSize b{font:700 13px var(--mono);color:var(--ink)}
.lsNote{font-size:11px;line-height:1.45;color:var(--ink45)}
.lsNote b{font-weight:600;color:var(--ink70)}
.lsFoot{display:flex;align-items:center;gap:6px;flex-wrap:wrap;margin-top:auto}
.lsFoot .btn{margin-left:0}
.lsFoot .lsBusy{display:inline-flex;align-items:center;gap:6px;font-size:11px;color:var(--ink45)}
.lsSpin{width:11px;height:11px;border:2px solid rgba(0,0,0,.15);border-top-color:var(--ink45);border-radius:50%;animation:spin .7s linear infinite;display:inline-block;flex:0 0 auto}
.lsEmpty{padding:40px;text-align:center;color:var(--ink45);font-size:13px;display:flex;flex-direction:column;align-items:center;gap:11px}
.lsEmpty b{font-weight:600;color:var(--ink70);max-width:52ch;line-height:1.5}
.lsEmpty .row{display:inline-flex;align-items:center;gap:8px}
.lsErr{color:var(--clay)}
.lsMore{display:flex;justify-content:center}
@media (prefers-reduced-motion:reduce){.lsSpin{animation-duration:1.6s}}
`;
  doc.head.appendChild(css);

  /* ── the card's picture: the leftover at true scale in one frame ── */
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
  function picture(r) {
    const W = +r.sheetWMm || SIZE.w, H = +r.sheetHMm || SIZE.h, vw = Math.max(SIZE.w, W), vh = Math.max(SIZE.h, H), rings = Array.isArray(r.rings) ? r.rings : [];
    const label = rings.length ? `Leftover sheet at true scale: ${mm(r.bboxMm.w, r.bboxMm.h)}, ${Math.round(r.areaMm2)} square millimetres, in a ${num(W)} by ${num(H)} millimetre sheet` : 'Nothing is left of this sheet';
    const tone = 'var(--accent)';
    return `<svg class="lsPv" viewBox="0 0 ${vw} ${vh}" style="aspect-ratio:${vw}/${vh}" role="img" aria-label="${esc(label)}" preserveAspectRatio="xMidYMid meet">`
      + `<rect x="0" y="0" width="${W}" height="${H}" fill="none" style="stroke:var(--ink25)" stroke-width="1" stroke-dasharray="4 3" vector-effect="non-scaling-stroke" opacity=".6"/>`
      + (rings.length ? `<path d="${pathOf(rings)}" style="fill:${tone};stroke:${tone}" fill-opacity=".3" fill-rule="evenodd" stroke-width="1.2" stroke-linejoin="round" vector-effect="non-scaling-stroke"/>`
        + `<path d="${edgesOf(rings, W, H)}" fill="none" stroke="${GREEN}" stroke-width="1.8" stroke-dasharray="5 3" stroke-linejoin="round" vector-effect="non-scaling-stroke"/>` : '')
      + '</svg>';
  }

  /* ── cards ── */
  const metalChip = r => { const m = METALS.find(x => x[0] === r.metal); return `<span class="sw" style="--c:var(--m-${esc(r.metal)},var(--ink45))">${esc(r.code || (m && m[1]) || '')}</span>`; };
  const viaWords = via => via === 'library' ? 'dragged to Laser cutting in the Library' : 'Cut Sheet in the Nest tab';
  function statusPill(r) {
    if (r.status === 'available') return '<span class="pill ok">Available</span>';
    if (r.status === 'used') return `<span class="pill neutral">Used${r.usedBySheetName ? ' by ' + esc(r.usedBySheetName) : ''}</span>`;
    return `<span class="pill neutral">Discarded${r.auto && r.reason ? ' · ' + esc(r.reason.toLowerCase()) : ''}</span>`;
  }
  function actions(r) {
    if (st.marking.has(r.id)) return '<span class="lsBusy"><i class="lsSpin"></i>Saving…</span>';
    const b = (to, text, tip) => `<button type="button" class="btn ghost xs" data-ls-mark="${esc(to)}" data-id="${esc(r.id)}" title="${esc(tip)}">${esc(text)}</button>`;
    if (r.status === 'available') return b('used', 'Mark used', 'Record in this list that this leftover sheet has been used. The Nest tab still offers the sheet until a cut is made on it') + b('discarded', 'Discard', 'Record in this list that this leftover sheet was thrown away. The Nest tab still offers the sheet until a cut is made on it');
    if (r.marked) return b('available', 'Put back', 'Make it available again');
    return '';
  }
  function card(r) {
    const box = r.bboxMm || { w: 0, h: 0 }, from = [r.sheetName, r.setName].filter(Boolean).map(x => `<b>${esc(x)}</b>`).join(', ');
    const who = r.by ? `cut by <b>${esc(r.by)}</b>` : 'cut by nobody signed in';
    return `<article class="lsCard hoverItem" data-m="${esc(r.metal)}" data-s="${esc(r.status)}" data-id="${esc(r.id)}">`
      + `<div class="lsTop">${metalChip(r)}<span class="nm" title="${esc(r.sheetName || '')}">${esc(r.sheetName || 'Sheet')}</span>${r.setName ? `<span class="set">${esc(r.setName)}</span>` : ''}<span class="tm">${esc(when(r.cutAt))}</span></div>`
      + picture(r)
      + `<div class="lsSize"><b>${r.areaMm2 > 0 ? mm(box.w, box.h) : 'Nothing left'}</b><span>${r.areaMm2 > 0 ? Math.round(r.areaMm2).toLocaleString() + ' mm²' : ''}</span></div>`
      + `<div class="lsNote">From ${from || 'a sheet'} · ${who} · ${esc(viaWords(r.via))} · on a ${mm(r.sheetWMm, r.sheetHMm)} sheet</div>`
      + `<div class="lsFoot">${statusPill(r)}${actions(r)}</div></article>`;
  }

  /* ── the view ── */
  const host = doc.createElement('div');
  host.id = 'libLeft'; host.setAttribute('role', 'region'); host.setAttribute('aria-label', 'Leftover sheets');
  view.appendChild(host);
  host.innerHTML = `<div class="lsBar"><div class="lsTitle"><b>Leftover sheets</b><span class="lsSub">What is left of a sheet after a green line was cut, saved in its exact shape and real size</span></div>`
    + `<span class="lsChips" data-f="metal"><button type="button" class="fchip on" data-m="all">All<span class="lng"> metals</span></button>${METALS.map(m => `<button type="button" class="fchip" data-m="${m[0]}"><i class="sw"></i>${m[1]}</button>`).join('')}</span>`
    + `<span class="seg" data-f="status"><button type="button" data-v="available" class="on">Available</button><button type="button" data-v="used" title="Used or discarded">Used</button><button type="button" data-v="all">All</button></span>`
    + `<button type="button" class="btn ghost xs lsRefresh" title="Read the leftover sheets again">Refresh</button></div>`
    + `<div class="lsBody" aria-live="polite"></div><div class="lsMore"></div>`;
  const body = host.querySelector('.lsBody'), more = host.querySelector('.lsMore'), sub = host.querySelector('.lsSub'), refreshBtn = host.querySelector('.lsRefresh');

  const scopeOf = () => st.status === 'available' ? 'available' : 'all';
  function rows() {
    const c = st.cache[scopeOf()]; if (!c) return null;
    return c.items.filter(r => (st.metal === 'all' || r.metal === st.metal) && (st.status === 'all' || (st.status === 'available' ? r.status === 'available' : r.status !== 'available')));
  }
  function paint() {
    if (!st.open) return;
    const scope = scopeOf(), c = st.cache[scope], list = rows(), busy = !!st.busy[scope];
    refreshBtn.disabled = busy; refreshBtn.innerHTML = busy ? '<i class="lsSpin"></i> Reading…' : 'Refresh';
    for (const b of host.querySelectorAll('[data-f=metal] .fchip')) b.classList.toggle('on', b.dataset.m === st.metal);
    for (const b of host.querySelectorAll('[data-f=status] button')) b.classList.toggle('on', b.dataset.v === st.status);
    if (st.backfill === 1) { body.innerHTML = '<div class="lsEmpty"><span class="row"><i class="lsSpin"></i>Saving the leftover sheets of earlier cuts…</span></div>'; more.innerHTML = ''; return; }
    if (!c) {
      sub.textContent = 'What is left of a sheet after a green line was cut, saved in its exact shape and real size';
      body.innerHTML = st.error ? `<div class="lsEmpty"><b class="lsErr">Could not read the leftover sheets: ${esc(st.error)}</b><span>Press Refresh to try again.</span></div>`
        : `<div class="lsEmpty"><span class="row"><i class="lsSpin"></i>Reading the leftover sheets…</span></div>`;
      more.innerHTML = ''; return;
    }
    const total = st.cache.available ? st.cache.available.items.length : null;

    sub.textContent = (total != null ? `${total} available · ` : '') + 'what is left of a sheet after a green line was cut, saved in its exact shape and real size';
    if (!list.length) {
      body.innerHTML = `<div class="lsEmpty"><b>${st.status === 'available' ? (st.metal === 'all' ? 'No leftover sheets are available yet.' : 'No available leftover sheets of this metal.') : 'Nothing to show here.'}</b>`
        + `<span>When a green line is cut, from Cut Sheet in the Nest tab or by dragging a partial sheet to Laser cutting in the Library, what is left of the sheet is saved here.</span></div>`;
    } else body.innerHTML = (st.error ? `<div class="lsNote lsErr">Could not refresh: ${esc(st.error)}</div>` : '') + `<div class="lsGrid">${list.map(card).join('')}</div>`;
    more.innerHTML = scope === 'all' && c.more ? `<button type="button" class="btn ghost sm" data-ls-older>${st.busy.older ? '<i class="lsSpin"></i> Reading…' : 'Show older cuts'}</button>` : '';
  }

  /* ── reading: one list op; the counter of the last answer says whether anything moved ── */
  async function load(o = {}) {
    const scope = scopeOf(), key = o.older ? 'older' : scope;
    if (st.busy[key]) return st.busy[key];
    const c = st.cache[scope], ask = { op: 'remnantList', scope };
    if (o.older && c && c.items.length) ask.before = Math.min(...c.items.map(r => r.cutAt || Infinity));
    else if (c && c.rev && !o.verify) ask.ifRev = c.rev;
    if (o.verify) ask.verify = true;
    st.busy[key] = (async () => {
      st.error = ''; paint();
      try {
        const r = await api(ask);
        if (!r || r.error) throw new Error((r && r.error) || 'No answer came back');
        st.at = Date.now();
        if (r.unchanged) { if (c) c.at = Date.now(); return; }
        // the first time ever: the leftovers of the cuts made before they were saved are saved now, once (the cloud says so, and then never again)
        if (r.needsBackfill && !st.backfill) {
          st.backfill = 1; paint();
          try { await api({ op: 'remnantBackfill' }, 'Saving the leftover sheets of earlier cuts'); st.backfill = 2; } catch (e) { st.backfill = 0; throw e; }
          st.backfill = 2; const again = await api(ask.ifRev ? { ...ask, ifRev: undefined } : ask); if (!again || again.error) throw new Error((again && again.error) || 'No answer came back'); Object.assign(r, again);
        }
        if (o.older && c) { const seen = new Set(c.items.map(x => x.id)); c.items = c.items.concat(r.items.filter(x => !seen.has(x.id))); c.more = !!r.more; }
        else st.cache[scope] = { items: r.items || [], rev: r.rev || null, more: !!r.more, at: Date.now() };
      } catch (e) { st.error = e && e.message ? e.message : String(e); }
      finally { st.busy[key] = null; paint(); }
    })();
    paint();
    return st.busy[key];
  }

  /* ── a person's press on a card ── */
  async function mark(id, status) {
    if (st.marking.has(id)) return;
    st.marking.add(id); paint();
    let who = ''; try { who = String(window.CNEmployee?.name?.() || window.B?.employee || '').trim(); } catch (_) { who = ''; }
    try {
      const r = await api({ op: 'remnantMark', id, status, by: who }, 'Saving the leftover sheet');
      if (!r || r.error) throw new Error((r && r.error) || 'No answer came back');
      for (const k of ['available', 'all']) { const c = st.cache[k]; if (!c) continue; c.items = c.items.map(x => x.id === id ? { ...x, ...r.item } : x); }   // (the counter this answer was made at stays: it has moved, so the next read is in full, once)
      if (st.cache.available && status !== 'available') st.cache.available.items = st.cache.available.items.filter(x => x.id !== id);
      st.error = '';
    } catch (e) { st.error = e && e.message ? e.message : String(e); C.toast?.(st.error, 'bad'); }
    finally { st.marking.delete(id); paint(); }
  }

  host.addEventListener('click', e => {
    const m = e.target.closest('[data-ls-mark]'); if (m) { mark(m.dataset.id, m.dataset.lsMark); return; }
    const f = e.target.closest('[data-f=metal] .fchip'); if (f) { st.metal = f.dataset.m; paint(); return; }
    const s = e.target.closest('[data-f=status] button'); if (s) { st.status = s.dataset.v; paint(); const c = st.cache[scopeOf()]; if (!c || Date.now() - c.at > 20000) load(); return; }
    if (e.target.closest('.lsRefresh')) { load({ verify: true }); return; }
    if (e.target.closest('[data-ls-older]')) load({ older: true });
  });

  /* ── opening and closing: a third choice beside Sheets | Sets ── */
  const btn = doc.createElement('button');
  btn.type = 'button'; btn.dataset.k = 'leftovers'; btn.title = 'Leftover sheets: what is left of a sheet after a green line was cut, saved in its exact shape and real size';
  btn.innerHTML = 'Leftover<span class="lng"> sheets</span>';
  kind.appendChild(btn);
  function open() {
    if (st.open) return;
    st.open = true; st.prevOn = [...kind.querySelectorAll('button.on')].filter(b => b !== btn);
    for (const b of kind.querySelectorAll('button')) b.classList.toggle('on', b === btn);
    view.classList.add('leftOpen'); paint(); load();
  }
  function close(restore = true) {
    if (!st.open) return;
    st.open = false; view.classList.remove('leftOpen'); btn.classList.remove('on');
    if (restore) for (const b of st.prevOn || []) b.classList.add('on');
    st.prevOn = null;
  }
  // capture phase: runs before the Library's own handler on the same button, which would read Sheets or Sets
  kind.addEventListener('click', e => {
    const b = e.target.closest('button'); if (!b) return;
    if (b === btn) { e.stopImmediatePropagation(); e.preventDefault(); open(); return; }
    close(false);
  }, true);
  doc.getElementById('libTab')?.addEventListener('click', e => { if (e.target.closest('button')) close(true); }, true);

  /* ── following what changes, without a clock ── */
  function changed() {
    if (!st.open || doc.hidden) return;
    clearTimeout(st.timer); st.timer = setTimeout(() => { st.timer = 0; load(); }, 600);
  }
  try { const ch = typeof BroadcastChannel === 'function' ? new BroadcastChannel('cn-library') : null; if (ch) ch.addEventListener('message', e => { if (e && e.data && e.data.t === 'write' && /^(roseRecordCut|remnantMark)$/.test(String(e.data.op || ''))) changed(); }); } catch (_) {}
  window.addEventListener?.('hashchange', () => close(true));   // (#library/completed and the like: the list the address names is the one shown)
  doc.addEventListener('visibilitychange', () => { if (!doc.hidden && st.open && Date.now() - st.at > 60000) load(); });

  window.LeftoverSheets = { open, close, changed, isOpen: () => st.open, refresh: () => load({ verify: true }), state: st, picture, edgesOf };
})();
