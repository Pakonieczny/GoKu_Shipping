/* The sheet drop-down (window.SheetMenu). Paul, 5 Oct 2026: "Add drop down menu for the Sheets just like the new back engraving menu."

   Why. A card's sheets ran as one tab each ("Sheet 1", "Sheet 2", ...) in the card's controls row. Beside the Cut Sheet / Options
   buttons, the back-engraving control and Front | Back, the tabs ran out of room and were clipped ("Sheet 2" cut off at the
   edge). The tabs are now ONE small pill, the same one-button pill as the back-engraving control (EngravingToggle: the order
   view's .owSeg look, one button, a chevron), that shows the CURRENT sheet's name and its status exactly as its tab showed
   them ("Sheet 1 ✓ 77", a spinner while it nests, "⚠ 12/14" when partial) and opens a small menu of every sheet of the card.

   What is seen.
     closed — the pill: [ name  mark  ⌄ ]. Beside other controls the name shrinks first (an ellipsis) so the mark and the chevron
              always stay; the pill never grows the row it sits in. A card with one sheet shows the same pill as a plain chip
              (no chevron, no menu: there is nothing to choose), so the row looks alike on every card.
     open   — a press (Enter / Space / Down on the keyboard) opens ONE small menu that grows out of the pill on the app's own curve,
              cubic-bezier(.2,.8,.2,1), on a fixed layer on the page body (a menu on the page, never a pop-up inside a pop-up).
              One row per sheet: a tick on the current one, the name, its mark. A press on a row picks it. Never under the top
              bar, never past the bottom edge (it opens upward when there is no room, and scrolls inside itself when the list is
              long), never a sideways scroll. Reduced motion: a short fade only.
     closes — Esc (focus returns to the pill), a press outside, the pill pressed again, Tab, a pick, the card leaving the screen.
     keys   — on the pill: Enter / Space / Down / Up open it; in the menu: Up / Down (wrapping) / Home / End move, Enter / Space pick.
     live   — update(items) while open changes the rows in place (a sheet that appears slides in, one that goes folds away, a
              mark that changed rewrites itself; the scroll position and the focused row stay). Fewer than two sheets: it closes.

   What it does not do. It reads and reports: it never writes a sheet, a seal or a record. A pick calls onPick(id, item); what a
   pick does (the card's own showPage) is the host's code, the very code the old tab ran.

     const menu = SheetMenu.mount(host, {
       key,                      // the menu's own name (the card's metal, a library card's id): in the event and on the markup
       items: [{ id, label, mark?, count?, spin?, state?, note?, active? | current?, disabled?, title?, aria?, color?, sub?, badge? }],
                                 // mark: the status text beside the name ("✓ 77", "freed room"); count is the same when mark is not given;
                                 // spin: a labelled spinner instead of the mark (a sheet that is nesting); state: a word the row
                                 // carries as data-state ("complete" "partial" "nesting" "cut" "held" ...); note: a second small
                                 // word after the name ("cut", "held"); active (or current): the current sheet; disabled: shown, not
                                 // pickable; color: a CSS colour for the small dot before the name (a metal); sub: a muted second
                                 // line under the name; badge: a small count at the right
       onPick(id, item),         // a row was chosen
       layer: Element | () => Element,   // where the menu layer goes: by default the open <dialog> the host is in (a modal dialog
                                 // is the top layer and makes everything outside it inert, so a layer on the body would be hidden
                                 // behind it), else the page body. Always position:fixed, clamped to the viewport.
       label,                    // the menu's name for a screen reader ("Sheets of Gold 14K")
       minItems = 2              // fewer items than this: the pill shows as a plain chip, no menu
     })                                                   → { update(items, { label? }), open(), close(), toggle(), destroy(), isOpen(), items(), button }
     SheetMenu.closeAll() · SheetMenu.isOpen(key?) · SheetMenu.rows() (the ids of the rows of the open menu, in order) · SheetMenu.EVENT
     document event "sheetmenu" (bubbles to window): detail { key, open, picked, source } — "open" true/false; "picked" the id when a
       row was chosen; source: button | key | pick | escape | outside | tab | api | gone | update. */
(function (root) {
  'use strict';
  const doc = () => root.document || null;
  if (!doc() || root.SheetMenu) return;
  const EVENT = 'sheetmenu', EASE = 'cubic-bezier(.2,.8,.2,1)';
  const str = v => String(v == null ? '' : v);
  const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));
  const reduced = () => { try { return root.Motion && root.Motion.reduced ? !!root.Motion.reduced() : !!(root.matchMedia && root.matchMedia('(prefers-reduced-motion: reduce)').matches); } catch (_) { return false; } };
  const anim = (el, frames, o) => { try { return el && el.animate ? el.animate(frames, o) : null; } catch (_) { return null; } };
  const live = { m: null, io: null, frame: 0 };   // the one open menu (there is never more than one)

  /* the pill is the engraving control's pill (EngravingToggle: .owSeg, one button, 7 px chevron, 2/7 px padding), so the two sit
     in the row as one family; the menu is the Library issues panel's layer (same edge, shadow, curve and arrow) */
  const CSS = `
.shMenu{display:inline-flex;align-items:center;flex:0 1 auto;min-width:0;max-width:100%;position:relative;white-space:nowrap;vertical-align:middle}
.shMenu[hidden]{display:none}
.shMenu.owSeg button,.shMenu.owSeg .shmBtn{display:inline-flex;align-items:center;gap:4px;min-width:0;max-width:100%;flex:0 1 auto;padding:2px 7px;line-height:1.2;font:600 10.5px/1.2 var(--sans,system-ui,sans-serif);letter-spacing:.02em;color:var(--ink,#1c1a17)}
.shMenu.owSeg .shmChip{cursor:default;border-radius:6px}
.shMenu .shmName{flex:0 1 auto;min-width:2.6em;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.shMenu .shmMark,.shmPanel .shmMark{flex:0 0 auto;font-style:normal;font-weight:500;opacity:.7;font-variant-numeric:tabular-nums;white-space:nowrap}
.shMenu .shmMark:empty,.shmPanel .shmMark:empty{display:none}
.shMenu .shmChev{width:7px;height:7px;flex:0 0 auto;display:block;transition:transform .2s ease}
.shMenu button[aria-expanded="true"] .shmChev{transform:rotate(180deg)}
.shMenu button:focus-visible{outline:2px solid var(--gold2,#caa861);outline-offset:1px}
.shMenu button[aria-expanded="true"]{background:var(--card,#fffefb);box-shadow:0 1px 2px rgba(30,26,20,.12)}
.shmSpin{display:inline-block;flex:0 0 auto;width:10px;height:10px;box-sizing:border-box;border:2px solid rgba(0,0,0,.15);border-top-color:currentColor;border-radius:50%;animation:shmSpin .7s linear infinite}
@keyframes shmSpin{to{transform:rotate(360deg)}}
.shmPanel{position:fixed;z-index:2147483000;left:0;top:0;min-width:150px;max-width:calc(100vw - 16px);display:flex;flex-direction:column;box-sizing:border-box;background:var(--card,#fffefb);color:var(--ink,#1c1a17);border:1px solid var(--line,#e4ddd0);border-radius:12px;box-shadow:0 1px 0 rgba(255,255,255,.6) inset,0 14px 34px rgba(30,26,20,.16),0 2px 6px rgba(30,26,20,.06);font:11px/1.3 var(--sans,system-ui,sans-serif);transform-origin:var(--ox,20px) var(--oy,-6px);opacity:0;-webkit-font-smoothing:antialiased}
.shmPanel *{box-sizing:border-box}
.shmPanel::before{content:"";position:absolute;left:var(--ax,20px);top:-6px;width:10px;height:10px;margin-left:-5px;background:var(--card,#fffefb);border-left:1px solid var(--line,#e4ddd0);border-top:1px solid var(--line,#e4ddd0);transform:rotate(45deg)}
.shmPanel.up::before{top:auto;bottom:-6px;transform:rotate(225deg)}
.shmList{flex:1 1 auto;min-height:0;overflow:auto;overscroll-behavior:contain;padding:4px;display:flex;flex-direction:column;gap:1px;border-radius:12px}
.shmRow{all:unset;box-sizing:border-box;display:flex;align-items:center;gap:7px;flex:0 0 auto;min-height:28px;padding:4px 10px 4px 7px;border-radius:8px;cursor:pointer;font:600 11px/1.2 var(--sans,system-ui,sans-serif);letter-spacing:.02em;color:var(--ink70,#5b554c);transition:background .15s,color .15s}
.shmRow:hover,.shmRow:focus-visible{background:var(--card2,#faf7f1);color:var(--ink,#1c1a17)}
.shmRow:focus-visible{outline:2px solid var(--gold2,#caa861);outline-offset:-1px}
.shmRow[aria-checked="true"]{color:var(--ink,#1c1a17);background:var(--card2,#faf7f1)}
.shmRow[aria-disabled="true"]{opacity:.45;cursor:default}
.shmTick{width:9px;height:9px;flex:0 0 auto;opacity:0}
.shmRow[aria-checked="true"] .shmTick{opacity:1}
.shmRow .shmName{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.shmTxt{flex:1 1 auto;min-width:0;display:flex;flex-direction:column;gap:1px}
.shmSub{font-weight:500;font-size:10px;letter-spacing:0;color:var(--ink45,#938c80);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.shmDot{width:8px;height:8px;flex:0 0 auto;border-radius:50%;background:var(--shm-dot,currentColor);box-shadow:0 0 0 1px rgba(30,26,20,.14) inset}
.shmBadge{flex:0 0 auto;font:600 9.5px/1 var(--mono,monospace);font-variant-numeric:tabular-nums;letter-spacing:0;color:var(--ink70,#5b554c);background:var(--card2,#faf7f1);border:1px solid var(--line,#e4ddd0);border-radius:999px;padding:2px 5px}
.shmBadge:empty{display:none}
.shmRow .shmNote{flex:0 0 auto;font-weight:500;font-size:10px;opacity:.7}
.shmRow .shmMark{margin-left:12px}
.shmRow .shmNote{order:1}.shmRow .shmBadge{order:2}.shmRow .shmMark,.shmRow .shmSpin{order:3}.shmRow .shmSpin{margin-left:12px}
.shmRow.gone{pointer-events:none}
@media (pointer:coarse){.shmRow{min-height:44px}}
@media (prefers-reduced-motion:reduce){.shMenu .shmChev,.shmRow{transition:none}.shmSpin{animation-duration:1.6s}}`;
  let cssIn = false;
  function injectCss() {
    const d = doc(); if (cssIn || !d || !d.head) return; cssIn = true;
    if (d.getElementById('shmCss')) return;
    const s = d.createElement('style'); s.id = 'shmCss'; s.textContent = CSS; d.head.appendChild(s);
  }
  const SVG = 'http://www.w3.org/2000/svg';
  function svg(cls, vb, d, sw) {
    const s = doc().createElementNS(SVG, 'svg'); s.setAttribute('class', cls); s.setAttribute('viewBox', vb); s.setAttribute('aria-hidden', 'true'); s.setAttribute('focusable', 'false');
    const p = doc().createElementNS(SVG, 'path'); p.setAttribute('d', d); p.setAttribute('fill', 'none'); p.setAttribute('stroke', 'currentColor'); p.setAttribute('stroke-width', sw); p.setAttribute('stroke-linecap', 'round'); p.setAttribute('stroke-linejoin', 'round');
    s.appendChild(p); return s;
  }
  const chev = () => svg('shmChev', '0 0 8 8', 'M1.2 2.8L4 5.6l2.8-2.8', '1.3');   // the engraving control's chevron
  const tick = () => svg('shmTick', '0 0 10 10', 'M1.8 5.4l2.3 2.3 4.1-4.9', '1.4');
  const el = (tag, cls, text) => { const e = doc().createElement(tag); if (cls) e.className = cls; if (text != null) e.textContent = text; return e; };

  function fire(m, open, source, picked) {
    const d = doc(); if (!d || typeof root.CustomEvent !== 'function') return;
    d.dispatchEvent(new root.CustomEvent(EVENT, { bubbles: true, detail: { key: m.key, open: !!open, picked: picked == null ? null : picked, source: source || 'api' } }));
  }
  function norm(list) {
    const out = [], seen = new Set();
    for (const it of Array.isArray(list) ? list : []) {
      if (!it || it.id == null || seen.has(str(it.id))) continue; seen.add(str(it.id));
      out.push({ id: str(it.id), label: str(it.label != null ? it.label : it.id), mark: str(it.mark != null ? it.mark : it.count != null ? it.count : ''), spin: !!it.spin, state: str(it.state), note: str(it.note),
        active: !!(it.active || it.current), disabled: !!it.disabled, title: str(it.title), aria: str(it.aria), spinLabel: str(it.spinLabel || 'Working'), color: str(it.color), sub: str(it.sub), badge: str(it.badge) });
    }
    return out;
  }
  const sigOf = (items, label) => JSON.stringify([label, items.map(i => [i.id, i.label, i.mark, i.spin, i.state, i.note, i.active, i.disabled, i.title, i.aria, i.color, i.sub, i.badge])]);
  const currentOf = m => m.items.find(i => i.active) || m.items[0] || null;

  /* ── the pill ── */
  function markNode(it, cls) {
    if (it.spin) { const s = el('span', 'shmSpin'); s.setAttribute('role', 'img'); s.setAttribute('aria-label', it.spinLabel); s.title = it.spinLabel; return s; }
    return el('span', cls || 'shmMark', it.mark);
  }
  function paintPill(m) {
    const it = currentOf(m), menu = m.items.length >= m.minItems;
    if (m.mode !== (menu ? 'menu' : 'chip') || !m.btn) buildPill(m, menu);
    const b = m.btn; if (!b) return;
    const name = b.querySelector('.shmName'), old = b.querySelector('.shmMark, .shmSpin'), dot = b.querySelector('.shmDot'), bg = b.querySelector('.shmBadge');
    name.textContent = it ? it.label : '';
    if (it && it.color) { if (!dot) { const x = el('span', 'shmDot'); x.setAttribute('aria-hidden', 'true'); b.prepend(x); } b.querySelector('.shmDot').style.setProperty('--shm-dot', it.color); } else if (dot) dot.remove();
    if (it && it.badge) { if (!bg) name.after(el('span', 'shmBadge', it.badge)); else bg.textContent = it.badge; } else if (bg) bg.remove();
    if (it && it.spin && old && old.classList.contains('shmSpin')) { if (old.getAttribute('aria-label') !== it.spinLabel) { old.setAttribute('aria-label', it.spinLabel); old.title = it.spinLabel; } }   // (a spinner already turning is left turning)
    else { const mk = it ? markNode(it) : el('span', 'shmMark', ''); if (old) old.replaceWith(mk); else (b.querySelector('.shmBadge') || name).after(mk); }
    const says = it ? [it.label, it.badge, it.aria || (it.spin ? it.spinLabel : it.mark)].filter(Boolean).join(', ') : '';
    if (menu) {
      b.setAttribute('aria-label', `${says}. Choose a sheet, ${m.items.length} in this list`);
      b.title = (it && it.title ? it.title : says) + ' · choose another sheet';
    } else { b.setAttribute('aria-label', says); b.title = (it && it.title) || says; }
    b.setAttribute('data-sheet', it ? it.id : ''); b.setAttribute('data-state', it ? it.state : '');
    m.wrap.setAttribute('data-sheets', String(m.items.length));
    // the least room the pill needs to keep its mark and chevron whole (the name gives way first): the host's own minimum width, from
    // character counts (no layout read), so a row with every control in it overflows at the far end rather than cutting the pill
    const chars = it ? (it.spin ? 2 : it.mark.length) : 0, min = 20 + 28 + (menu ? 11 : 0) + (chars ? 4 + chars * 7 : 0) + (it && it.color ? 12 : 0) + (it && it.badge ? 14 + it.badge.length * 6 : 0);
    try { m.host.style.setProperty('--shm-min', min + 'px'); } catch (_) {}
  }
  function buildPill(m, menu) {
    if (m.btn) { if (live.m === m) closeMenu(m, { source: 'update', quiet: false }); m.btn.remove(); }
    m.mode = menu ? 'menu' : 'chip';
    const b = m.btn = menu ? el('button', 'shmBtn') : el('span', 'shmBtn shmChip');
    if (menu) { b.type = 'button'; b.setAttribute('aria-haspopup', 'menu'); b.setAttribute('aria-expanded', 'false'); }
    b.append(el('span', 'shmName'), el('span', 'shmMark'));
    if (menu) b.append(chev());
    m.wrap.prepend(b);
    if (menu) {
      b.addEventListener('click', ev => { ev.preventDefault(); toggle(m, ev.detail === 0 ? 'key' : 'button'); });
      b.addEventListener('keydown', ev => {
        if (live.m === m) return;   // (the document handler runs the open menu's keys)
        if (ev.key === 'ArrowDown' || ev.key === 'ArrowUp') { ev.preventDefault(); openMenu(m, 'key'); }
      });
    }
  }

  /* ── the menu ── */
  function rowFor(m, it) {
    const r = el('button', 'shmRow'); r.type = 'button'; r.setAttribute('role', 'menuitemradio'); r.tabIndex = -1; r.dataset.id = it.id;
    const t = el('span', 'shmTxt'); t.append(el('span', 'shmName'));
    r.append(tick(), t);
    return paintRow(r, it);
  }
  function paintRow(r, it) {
    r.setAttribute('aria-checked', it.active ? 'true' : 'false'); r.dataset.state = it.state;
    if (it.disabled) r.setAttribute('aria-disabled', 'true'); else r.removeAttribute('aria-disabled');
    r.querySelector('.shmName').textContent = it.label;
    r.title = it.title || '';
    r.setAttribute('aria-label', [it.label, it.sub, it.note, it.badge, it.aria || (it.spin ? it.spinLabel : it.mark)].filter(Boolean).join(', '));
    // (each part is rewritten only when it differs, so a hover-driven badge never rebuilds the row)
    const txt = r.querySelector('.shmTxt'), sub = txt.querySelector('.shmSub'), dot = r.querySelector('.shmDot');
    if (it.color) { if (!dot) { const x = el('span', 'shmDot'); x.setAttribute('aria-hidden', 'true'); r.querySelector('.shmTick').after(x); } r.querySelector('.shmDot').style.setProperty('--shm-dot', it.color); } else if (dot) dot.remove();
    if (it.sub) { if (!sub) txt.append(el('span', 'shmSub', it.sub)); else if (sub.textContent !== it.sub) sub.textContent = it.sub; } else if (sub) sub.remove();
    const swap = (cls, text) => { const n = r.querySelector('.' + cls); if (text) { if (!n) r.append(el('span', cls, text)); else if (n.textContent !== text) n.textContent = text; } else if (n) n.remove(); };
    swap('shmNote', it.note); swap('shmBadge', it.badge);
    const spin = r.querySelector('.shmSpin'), mark = r.querySelector('.shmMark');
    if (it.spin) { if (mark) mark.remove(); if (!spin) r.append(markNode(it)); else if (spin.getAttribute('aria-label') !== it.spinLabel) { spin.setAttribute('aria-label', it.spinLabel); spin.title = it.spinLabel; } }
    else { if (spin) spin.remove(); swap('shmMark', it.mark); }
    return r;
  }
  function place(m) {
    const p = m.panel, a = m.wrap; if (!p || !a || !a.isConnected) return false;
    const d = doc(), ar = a.getBoundingClientRect(); if (!ar.width && !ar.height) return false;
    const vw = root.innerWidth || 1200, vh = root.innerHeight || 800, bar = d.querySelector('.topbar');
    const lo = Math.max(8, (bar ? bar.getBoundingClientRect().bottom : 0) + 8), hi = vh - 8, gap = 8;
    p.style.maxHeight = 'none'; p.style.minWidth = Math.min(Math.max(150, Math.round(ar.width)), vw - 16) + 'px';
    const w = p.offsetWidth, h0 = p.offsetHeight, below = hi - (ar.bottom + gap), above = ar.top - gap - lo, down = h0 <= below || below >= above;
    const room = Math.max(96, down ? below : above);
    p.style.maxHeight = room + 'px';
    const h = Math.min(h0, room), x = clamp(ar.left, 8, Math.max(8, vw - w - 8)), y = down ? ar.bottom + gap : ar.top - gap - h, ax = clamp(ar.left + ar.width / 2 - x, 14, Math.max(14, w - 14));
    p.style.left = Math.round(x) + 'px'; p.style.top = Math.round(Math.max(lo, y)) + 'px';
    p.style.setProperty('--ax', ax + 'px'); p.style.setProperty('--ox', ax + 'px'); p.style.setProperty('--oy', (down ? -6 : h + 6) + 'px');
    p.classList.toggle('up', !down);
    return true;
  }
  function again() {
    if (!live.m || live.frame) return;
    live.frame = root.requestAnimationFrame ? root.requestAnimationFrame(() => { live.frame = 0; if (live.m) place(live.m); }) : 0;
  }
  function watch(m) {
    unwatch();
    if (!root.IntersectionObserver) return;
    live.io = new root.IntersectionObserver(es => {
      const e = es[es.length - 1]; if (!e || e.isIntersecting || live.m !== m) return;
      closeMenu(m, { source: 'gone' });   // scrolled out of its list, its tab hidden, or its card drawn anew
    }, { threshold: 0 });
    live.io.observe(m.wrap);
  }
  function unwatch() { if (live.io) { try { live.io.disconnect(); } catch (_) {} live.io = null; } }
  const rowsOf = m => m.panel ? [...m.panel.querySelectorAll('.shmRow:not(.gone)')] : [];

  function openMenu(m, source) {
    if (m.mode !== 'menu' || live.m === m || !m.btn || !m.btn.isConnected || !m.btn.getClientRects().length) return false;
    injectCss();
    if (live.m) closeMenu(live.m, { source: 'api', now: true });
    const d = doc(), p = m.panel = el('div', 'shmPanel'), list = m.list = el('div', 'shmList');
    let layer = null; try { layer = typeof m.layer === 'function' ? m.layer() : m.layer; } catch (_) {}
    m.seq = (m.seq || 0) + 1; p.id = `shmPanel${m.uid}-${m.seq}`; p.setAttribute('role', 'menu'); p.setAttribute('aria-label', m.label || 'Sheets'); p.setAttribute('data-sheetmenu-for', m.key); p.tabIndex = -1;
    m.rows = new Map();
    for (const it of m.items) { const r = rowFor(m, it); m.rows.set(it.id, r); list.appendChild(r); }
    p.appendChild(list);
    ((layer && layer.appendChild && layer.isConnected ? layer : null) || m.wrap.closest('dialog[open]') || d.body).appendChild(p);
    m.btn.setAttribute('aria-expanded', 'true'); m.btn.setAttribute('aria-controls', p.id);
    live.m = m; m.open = true;
    p.style.opacity = '0'; place(m);
    const down = !p.classList.contains('up');
    p.style.opacity = '1';
    anim(p, reduced() ? [{ opacity: 0 }, { opacity: 1 }] : [{ opacity: 0, transform: `scale(.9) translateY(${down ? -6 : 6}px)` }, { opacity: 1, transform: 'none' }], { duration: reduced() ? 90 : 230, easing: EASE });
    watch(m);
    const cur = rowsOf(m).find(r => r.getAttribute('aria-checked') === 'true') || rowsOf(m)[0];
    if (cur) { cur.focus({ preventScroll: true }); try { cur.scrollIntoView({ block: 'nearest' }); } catch (_) {} }
    fire(m, true, source);
    return true;
  }
  function closeMenu(m, o) {
    o = o || {}; if (!m || live.m !== m || !m.open) return false;
    const p = m.panel, b = m.btn, hadFocus = p && p.contains(doc().activeElement);
    unwatch(); live.m = null; m.open = false; m.panel = m.list = null; m.rows = null;
    if (b) { b.setAttribute('aria-expanded', 'false'); b.removeAttribute('aria-controls'); }
    if (p) {
      const fade = !o.now && anim(p, reduced() ? [{ opacity: 1 }, { opacity: 0 }] : [{ opacity: 1, transform: 'none' }, { opacity: 0, transform: 'scale(.96)' }], { duration: reduced() ? 80 : 140, easing: 'ease-in', fill: 'forwards' });
      p.setAttribute('inert', ''); p.style.pointerEvents = 'none';
      const done = () => { try { p.remove(); } catch (_) {} };
      if (fade && fade.finished) fade.finished.then(done, done); else done();
    }
    // (focus goes back to the pill when it was inside the menu, or when asked)
    if ((o.focus || (hadFocus && o.focus !== false)) && b && b.isConnected && b.focus) { try { b.focus({ preventScroll: true }); } catch (_) {} }
    if (!o.quiet) fire(m, false, o.source || 'api', o.picked);
    return true;
  }
  function toggle(m, source) { return live.m === m ? closeMenu(m, { source: source || 'button', focus: true }) : openMenu(m, source || 'button'); }

  /* the open menu follows the list: rows keyed by sheet id are rewritten, added or folded away in place */
  function patch(m) {
    if (live.m !== m || !m.panel) return;
    if (m.items.length < m.minItems) { closeMenu(m, { source: 'gone' }); return; }
    const list = m.list, focusId = (doc().activeElement && doc().activeElement.closest && doc().activeElement.closest('.shmRow')) ? doc().activeElement.closest('.shmRow').dataset.id : '';
    const want = new Set(m.items.map(i => i.id)), rows = rowsOf(m);
    // a row that goes: focus moves to its neighbour first, then the row folds away
    for (const r of rows) {
      if (want.has(r.dataset.id)) continue;
      if (focusId === r.dataset.id) { const alive = rows.filter(x => want.has(x.dataset.id)), idx = rows.indexOf(r); const next = alive.find(x => rows.indexOf(x) > idx) || alive[alive.length - 1]; if (next) next.focus({ preventScroll: true }); }
      m.rows.delete(r.dataset.id); r.classList.add('gone'); r.setAttribute('aria-hidden', 'true'); r.tabIndex = -1; r.removeAttribute('role');
      const h = r.offsetHeight, a = !reduced() && anim(r, [{ opacity: 1, maxHeight: h + 'px' }, { opacity: 0, maxHeight: '0px', minHeight: '0px', paddingTop: '0px', paddingBottom: '0px' }], { duration: 170, easing: 'ease-in', fill: 'forwards' });
      const done = () => { try { r.remove(); } catch (_) {} };
      if (a && a.finished) { a.finished.then(done, done); setTimeout(done, 500); } else done();   // (and never left behind if the animation does not finish)
    }
    let ref = list.firstChild;
    for (const it of m.items) {
      let r = m.rows.get(it.id);
      if (r) paintRow(r, it);
      else { r = rowFor(m, it); m.rows.set(it.id, r); if (!reduced()) anim(r, [{ opacity: 0, transform: 'translateY(4px)' }, { opacity: 1, transform: 'none' }], { duration: 200, easing: EASE }); }
      while (ref && ref.classList && ref.classList.contains('gone')) ref = ref.nextSibling;
      if (ref !== r) list.insertBefore(r, ref); else ref = ref.nextSibling;
    }
    place(m);
  }

  /* ── mount ── */
  let uid = 0;
  function mount(host, o) {
    o = o || {}; injectCss();
    const d = doc(); if (!host || !d) return { update() {}, open() { return false; }, close() { return false; }, toggle() { return false; }, destroy() {}, isOpen: () => false, items: () => [], button: null };
    const m = { uid: ++uid, host, key: str(o.key), label: str(o.label), minItems: Math.max(1, +o.minItems || 2), onPick: typeof o.onPick === 'function' ? o.onPick : null, layer: o.layer || null,
      items: [], sig: '', mode: '', btn: null, open: false, panel: null, list: null, rows: null, seq: 0 };
    const wrap = m.wrap = el('span', 'owSeg shMenu'); wrap.setAttribute('data-sheetmenu', m.key);
    host.appendChild(wrap);
    const api = {
      get button() { return m.btn; },
      key: () => m.key,
      items: () => m.items.slice(),
      isOpen: () => live.m === m,
      open: source => openMenu(m, source || 'api'),
      close: o2 => closeMenu(m, Object.assign({ source: 'api' }, o2 || {})),
      toggle: () => toggle(m, 'api'),
      update(items, o2) {
        if (o2 && o2.label != null) m.label = str(o2.label);
        if (o2 && typeof o2.onPick === 'function') m.onPick = o2.onPick;
        const next = norm(items), sig = sigOf(next, m.label);
        if (m.destroyed) return;
        if (sig === m.sig && m.btn) { if (live.m === m && !m.wrap.isConnected) closeMenu(m, { source: 'gone' }); return; }
        m.sig = sig; m.items = next; paintPill(m);
        if (live.m === m) { if (m.panel) m.panel.setAttribute('aria-label', m.label || 'Sheets'); patch(m); }
      },
      destroy() { m.destroyed = true; if (live.m === m) closeMenu(m, { source: 'gone', now: true, quiet: false }); try { m.wrap.remove(); } catch (_) {} m.btn = null; }
    };
    m.api = api; m.pick = (id, source) => {
      const it = m.items.find(i => i.id === id); if (!it || it.disabled) return false;
      closeMenu(m, { source: 'pick', picked: id, focus: true });   // (the menu is shut before the sheet changes, so the card's redraw finds it closed)
      if (m.onPick) { try { m.onPick(id, it, source); } catch (e) { try { console.warn('SheetMenu: onPick', e); } catch (_) {} } }
      return true;
    };
    api.update(o.items || []);
    return api;
  }

  /* ── the document: a press on a row, a press or key anywhere while a menu is open ── */
  const d0 = doc();
  d0.addEventListener('click', ev => {
    const m = live.m; if (!m || !m.panel) return;
    const r = ev.target && ev.target.closest ? ev.target.closest('.shmRow') : null;
    if (!r || !m.panel.contains(r) || r.classList.contains('gone')) return;
    ev.preventDefault(); ev.stopPropagation(); m.pick(r.dataset.id, 'pointer');
  }, true);
  d0.addEventListener('pointerdown', ev => {
    const m = live.m; if (!m) return;
    const t = ev.target && ev.target.closest ? ev.target : null; if (!t) return;
    if ((m.panel && m.panel.contains(t)) || m.wrap.contains(t)) return;   // (the pill itself: its click closes it, so a second press does not close and reopen)
    closeMenu(m, { source: 'outside' });
  }, true);
  d0.addEventListener('keydown', ev => {
    const m = live.m; if (!m || !m.panel) return;
    const a = d0.activeElement, inside = m.panel.contains(a), onPill = !!(m.btn && a === m.btn);
    if (ev.key === 'Escape') { ev.preventDefault(); ev.stopPropagation(); closeMenu(m, { source: 'escape', focus: true }); return; }
    if (!inside && !onPill) return;
    if (ev.key === 'Tab') { closeMenu(m, { source: 'tab', focus: true }); return; }   // (focus is back on the pill; Tab moves on from there)
    const rows = rowsOf(m); if (!rows.length) return;
    const i = rows.indexOf(a.closest ? a.closest('.shmRow') : null);
    if (ev.key === 'ArrowDown' || ev.key === 'ArrowUp' || ev.key === 'Home' || ev.key === 'End') {
      ev.preventDefault(); ev.stopPropagation();
      const to = ev.key === 'Home' ? 0 : ev.key === 'End' ? rows.length - 1 : i < 0 ? (ev.key === 'ArrowDown' ? 0 : rows.length - 1) : (i + (ev.key === 'ArrowDown' ? 1 : -1) + rows.length) % rows.length;
      rows[to].focus({ preventScroll: true }); try { rows[to].scrollIntoView({ block: 'nearest' }); } catch (_) {}
    } else if ((ev.key === 'Enter' || ev.key === ' ') && i >= 0) {
      ev.preventDefault(); ev.stopPropagation(); m.pick(rows[i].dataset.id, 'key');
    }
  }, true);
  root.addEventListener('resize', again);
  d0.addEventListener('scroll', ev => { if (live.m && live.m.panel && ev.target && ev.target.nodeType === 1 && live.m.panel.contains(ev.target)) return; again(); }, true);

  root.SheetMenu = {
    mount, EVENT,
    closeAll() { return live.m ? closeMenu(live.m, { source: 'api' }) : false; },
    isOpen: key => !!live.m && (key == null || live.m.key === str(key)),
    current: () => live.m ? { key: live.m.key } : null,
    rows: () => live.m ? rowsOf(live.m).map(r => r.dataset.id) : [],
    panel: () => live.m && live.m.panel || null
  };
  if (typeof module === 'object' && module.exports) module.exports = root.SheetMenu;
})(typeof self !== 'undefined' ? self : this);
