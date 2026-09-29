'use strict';
// In-page probes. Each function is serialized by page.evaluate, so it must be
// self-contained (no closures over this module).

// Layout and content audit of one region of the page.
function pageAudit(opts) {
  opts = opts || {};
  const vw = window.innerWidth, vh = window.innerHeight;
  const out = { docOverflow: null, overflow: [], clipped: [], overlaps: [], smallTargets: [], leaks: [], bareDollar: [] };
  const roots = (opts.scopes || ['body']).map(s => document.querySelector(s)).filter(Boolean);
  if (!roots.length) return out;
  const csCache = new Map();
  const cs = el => { let v = csCache.get(el); if (!v) { v = getComputedStyle(el); csCache.set(el, v); } return v; };
  const ignored = el => !!(el.closest && el.closest('.toasts,#toasts,.bcTip,[data-harness-tip],.vh,script,style,noscript,template'));
  function visible(el) {
    if (!el || !el.isConnected || ignored(el)) return false;
    if (el.checkVisibility && !el.checkVisibility({ checkOpacity: true, checkVisibilityCSS: true })) return false;
    const r = el.getBoundingClientRect(); return r.width > 0.5 && r.height > 0.5;
  }
  function sel(el) {
    const parts = []; let n = el, depth = 0;
    while (n && n.nodeType === 1 && depth < 5) {
      let s = n.tagName.toLowerCase();
      if (n.id && !/^\d/.test(n.id)) { parts.unshift(s + '#' + n.id); break; }
      const cls = [...n.classList].filter(c => !/^(on|open|active|hidden|is-busy|show|sm|ghost|gold|btn)$/.test(c)).slice(0, 2);
      if (cls.length) s += '.' + cls.join('.');
      else { const a = [...n.attributes].find(x => (x.name.startsWith('data-') && !x.name.startsWith('data-harness')) || x.name === 'role'); if (a) s += '[' + a.name + ']'; }
      parts.unshift(s); n = n.parentElement; depth++;
    }
    return parts.join(' > ');
  }
  const snippet = (t, n) => String(t || '').replace(/\s+/g, ' ').trim().slice(0, n || 90);
  const within = el => roots.some(r => r === el || r.contains(el));

  // Text nodes in scope with their rendered boxes.
  const texts = [];
  roots.forEach(root => {
    const w = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
    let node, count = 0;
    while ((node = w.nextNode()) && count < 6000) {
      const v = node.nodeValue;
      if (!v || !v.trim()) continue;
      const el = node.parentElement; if (!el || !visible(el)) continue;
      // Measure the glyphs only: trailing spaces hang past right-aligned boxes.
      const range = document.createRange(); range.setStart(node, v.search(/\S/)); range.setEnd(node, v.length - v.match(/\s*$/)[0].length);
      const r = range.getBoundingClientRect(); if (r.width < 1 || r.height < 1) continue;
      texts.push({ node, el, r, range, text: v }); count++;
    }
  });

  // 1. Horizontal document overflow and the elements that cause it.
  const de = document.documentElement, sw = Math.max(de.scrollWidth, document.body.scrollWidth);
  // Contained when any ancestor that clips or scrolls horizontally ends inside the viewport.
  function contained(el) {
    for (let p = el.parentElement; p && p !== document.body && p !== de; p = p.parentElement) { const s = cs(p); if ((s.overflowX !== 'visible' || s.contain.includes('paint')) && p.getBoundingClientRect().right <= vw + 1) return true; }
    return false;
  }
  const offenders = [];
  document.body.querySelectorAll('*').forEach(el => {
    if (offenders.length > 400) return;
    const s = cs(el); if (s.position === 'fixed' || !visible(el)) return;
    const r = el.getBoundingClientRect(); if (r.right <= vw + 1 && r.left >= -1) return;
    if (contained(el)) return;
    offenders.push({ el, r });
  });
  const offSet = new Set(offenders.map(o => o.el));
  const culprits = offenders.filter(o => !offSet.has(o.el.parentElement)).sort((a, b) => b.r.right - a.r.right).slice(0, 10);
  if (sw > vw + 1 || culprits.length) out.docOverflow = { scrollWidth: sw, viewport: vw, culprits: culprits.map(o => ({ sel: sel(o.el), right: Math.round(o.r.right), width: Math.round(o.r.width), text: snippet(o.el.innerText, 70) })) };

  // 2. Children sticking out of a non-clipping container.
  const seenC = new Set();
  roots.forEach(root => root.querySelectorAll('*').forEach(el => {
    if (out.overflow.length > 60) return;
    const p = el.parentElement; if (!p || el.closest('svg') || !visible(el)) return;
    const s = cs(el); if (s.position === 'absolute' || s.position === 'fixed' || s.display === 'contents') return;
    const ps = cs(p); if (ps.overflowX !== 'visible' || ps.display === 'contents' || ps.display.startsWith('inline') || ['TD', 'TH', 'TR', 'TBODY', 'THEAD', 'TABLE'].includes(p.tagName) && s.display !== 'block') return;
    const r = el.getBoundingClientRect(), pr = p.getBoundingClientRect();
    const padL = parseFloat(ps.borderLeftWidth) || 0, padR = parseFloat(ps.borderRightWidth) || 0;
    const over = Math.max(r.right - (pr.right - padR), (pr.left + padL) - r.left);
    if (over <= 3) return; // icon nudges and sub-pixel rounding
    if (parseFloat(s.marginLeft) < 0 || parseFloat(s.marginRight) < 0) return; // deliberate bleed
    const key = sel(p); if (seenC.has(key)) return; seenC.add(key);
    out.overflow.push({ sel: sel(el), container: key, overBy: Math.round(over), text: snippet(el.innerText || el.textContent, 70) });
  }));

  // 3. Text cut by a clipping ancestor, and 4. text overlapping other text.
  const clipOf = new Map();
  function nearestClip(el) {
    if (clipOf.has(el)) return clipOf.get(el);
    let found = null;
    for (let p = el; p && p !== document.body; p = p.parentElement) { const s = cs(p); if (s.overflowX !== 'visible' || s.overflowY !== 'visible') { found = p; break; } }
    clipOf.set(el, found); return found;
  }
  const shown = [];
  texts.forEach(t => {
    const c = nearestClip(t.el);
    if (c) {
      const s = cs(c), cr = c.getBoundingClientRect(), scrolls = /auto|scroll/.test(s.overflowX + s.overflowY);
      const cutX = t.r.right - cr.right > 2 || cr.left - t.r.left > 2, cutY = t.r.bottom - cr.bottom > 2 || cr.top - t.r.top > 2;
      if ((cutX || cutY) && !scrolls) {
        let ell = false; for (let p = t.el; p && p !== c.parentElement; p = p.parentElement) { const ps = cs(p); if (ps.textOverflow === 'ellipsis' || (ps.webkitLineClamp && ps.webkitLineClamp !== 'none')) { ell = true; break; } }
        const titled = !!t.el.closest('[title],[data-tip]');
        if (out.clipped.length < 60) out.clipped.push({ sel: sel(t.el), clipper: sel(c), axis: cutX ? 'x' : 'y', ellipsis: ell, titled, text: snippet(t.text, 80) });
        if (!ell) return; // hidden text cannot overlap
      }
      if (scrolls && (t.r.bottom < cr.top || t.r.top > cr.bottom || t.r.right < cr.left || t.r.left > cr.right)) return;
      t.clip = cr; // only the part inside the clipping box is painted
    }
    shown.push(t);
  });
  const clampRect = (r, c) => { if (!c) return r; const left = Math.max(r.left, c.left), right = Math.min(r.right, c.right), top = Math.max(r.top, c.top), bottom = Math.min(r.bottom, c.bottom); return { left, right, top, bottom, width: Math.max(0, right - left), height: Math.max(0, bottom - top) }; };
  shown.forEach(t => { t.r = clampRect(t.r, t.clip); });
  roots.forEach(root => root.querySelectorAll('svg text').forEach(el => { if (visible(el) && el.textContent.trim()) shown.push({ el, r: el.getBoundingClientRect(), text: el.textContent, svg: true }); }));
  const layers = new Map();
  const layerOf = el => { if (layers.has(el)) return layers.get(el); let l = null; for (let p = el; p && p !== document.body; p = p.parentElement) { const pos = cs(p).position; if (pos === 'sticky' || pos === 'fixed') { l = p; break; } } layers.set(el, l); return l; };
  const grid = new Map(), G = 64;
  shown.forEach((t, i) => { for (let x = Math.floor(t.r.left / G); x <= Math.floor(t.r.right / G); x++) for (let y = Math.floor(t.r.top / G); y <= Math.floor(t.r.bottom / G); y++) { const k = x + ',' + y; if (!grid.has(k)) grid.set(k, []); grid.get(k).push(i); } });
  const pairs = new Set();
  grid.forEach(list => {
    for (let a = 0; a < list.length; a++) for (let b = a + 1; b < list.length; b++) {
      const i = list[a], j = list[b], k = i + ':' + j; if (pairs.has(k)) continue; pairs.add(k);
      const A = shown[i], B = shown[j]; if (A.el === B.el || A.el.contains(B.el) || B.el.contains(A.el)) continue;
      if (layerOf(A.el) !== layerOf(B.el)) continue; // a sticky header scrolling over content is by design
      const w = Math.min(A.r.right, B.r.right) - Math.max(A.r.left, B.r.left), h = Math.min(A.r.bottom, B.r.bottom) - Math.max(A.r.top, B.r.top);
      if (w < 3 || h < 3) continue;
      const small = Math.min(A.r.width * A.r.height, B.r.width * B.r.height); if (w * h < small * 0.25) continue;
      // Multi-line inline text reports a union box; only count real glyph overlap.
      if (!A.svg && !B.svg) { const la = [...A.range.getClientRects()].map(x => clampRect(x, A.clip)), lb = [...B.range.getClientRects()].map(x => clampRect(x, B.clip)); if (!la.some(x => lb.some(y => Math.min(x.right, y.right) - Math.max(x.left, y.left) > 3 && Math.min(x.bottom, y.bottom) - Math.max(x.top, y.top) > 3))) continue; }
      if (out.overlaps.length < 40) out.overlaps.push({ a: sel(A.el), at: snippet(A.text, 40), b: sel(B.el), bt: snippet(B.text, 40), w: Math.round(w), h: Math.round(h) });
    }
  });

  // 5. Tap targets below the minimum on touch layouts.
  if (opts.minTap) {
    const groups = new Map();
    roots.forEach(root => root.querySelectorAll('a[href],button,input:not([type=hidden]),select,textarea,summary,[role=button],[role=tab],[onclick],[tabindex="0"],.sw').forEach(el => {
      if (!visible(el) && !(el.matches('input[type=checkbox],input[type=radio]') && el.closest('label') && visible(el.closest('label')))) return;
      let target = el; if (el.matches('input[type=checkbox],input[type=radio]') && el.closest('label')) target = el.closest('label');
      const r = target.getBoundingClientRect(); if (r.width >= opts.minTap && r.height >= opts.minTap) return;
      const inline = el.tagName === 'A' && el.parentElement && snippet(el.parentElement.textContent).length > snippet(el.textContent).length + 20;
      const key = sel(el).replace(/:nth[^ ]*/g, '') + '|' + snippet(el.innerText || el.value || el.getAttribute('aria-label'), 24);
      const g = groups.get(key) || { sel: sel(el), text: snippet(el.innerText || el.value || el.getAttribute('aria-label') || el.title, 40), w: Math.round(r.width), h: Math.round(r.height), inline, count: 0 };
      g.count++; groups.set(key, g);
    }));
    out.smallTargets = [...groups.values()].sort((a, b) => Math.min(a.w, a.h) - Math.min(b.w, b.h)).slice(0, 60);
  }

  // 6. Placeholder and escape leaks in visible text and in tooltip/label attributes.
  const LEAKS = [['undefined', /\bundefined\b/], ['NaN', /\bNaN\b/], ['null', /\bnull\b/], ['[object]', /\[object \w+\]/], ['Infinity', /\bInfinity\b/],
    ['raw \\u escape', /\\u[0-9a-fA-F]{4}/], ['raw \\n', /\\n(?![a-z]{2,})/], ['double-escaped entity', /&(?:amp|lt|gt|quot|apos|nbsp|#\d+|#x[0-9a-f]+);/i],
    ['raw enum', /\b[A-Z][A-Z0-9]{1,}(?:_[A-Z0-9]+)+\b/], ['snake_case token', /\b[a-z]+(?:_[a-z0-9]+)+\b/]];
  const seenL = new Set();
  const leak = (where, el, str) => {
    if (!str) return;
    const inCode = el.closest && el.closest('code,pre,kbd,samp,input,textarea');
    LEAKS.forEach(([name, re]) => {
      const m = String(str).match(re); if (!m) return;
      if ((name === 'raw enum' || name === 'snake_case token') && inCode) return;
      if (name === 'snake_case token' && /https?:|\/|\.(jpg|png|com)/.test(m[0])) return;
      const key = name + '|' + m[0] + '|' + sel(el); if (seenL.has(key)) return; seenL.add(key);
      if (out.leaks.length < 80) out.leaks.push({ kind: name, match: m[0], where, sel: sel(el), text: snippet(str, 100) });
    });
  };
  texts.forEach(t => leak('text', t.el, t.text));
  roots.forEach(root => root.querySelectorAll('[title],[aria-label],[placeholder],[data-tip],img[alt]').forEach(el => {
    if (!visible(el) && !el.closest('svg')) return;
    ['title', 'aria-label', 'placeholder', 'data-tip', 'alt'].forEach(a => { const v = el.getAttribute(a); if (v) leak('@' + a, el, a === 'data-tip' ? v.replace(/<[^>]+>/g, ' ') : v); });
  }));
  // A select shows its chosen option's text; its value is never on screen.
  roots.forEach(root => root.querySelectorAll('input,select,textarea').forEach(el => { if (visible(el) && el.type !== 'password') leak('value', el, el.tagName === 'SELECT' ? (el.selectedOptions[0] || {}).text || '' : el.value); }));

  // 7. Dollar amounts with no currency code nearby; CAD tracers flagged explicitly.
  const tracers = (opts.cadTracers || []).map(Number);
  const CODE = /\b(CAD|USD)\b|C\$|CA\$|US\$/;
  const blockOf = el => { for (let p = el; p && p !== document.body; p = p.parentElement) { const d = cs(p).display; if (/block|flex|grid|table-row|list-item|table-cell/.test(d) && !/^(B|STRONG|SPAN|I|EM|SMALL)$/.test(p.tagName)) return p; } return el; };
  const cardOf = el => el.closest('.card,.draft,article,section,dialog,.kpi,.oppCard,.exp,tr,.existingCampaign,.sgLaunchSettings,.bg-detail,.bg-card');
  const seenD = new Set();
  function dollar(el, str, textNode) {
    const re = /(US|CA|C)?\$\s?(-?[\d,]+(?:\.\d+)?)/g; let m;
    while ((m = re.exec(str))) {
      if (m[1]) continue;
      const val = Number(m[2].replace(/,/g, '')); if (!isFinite(val)) continue;
      // innerText keeps the spaces layout puts between elements ("in CAD" + "Search").
      const own = el.innerText || el.textContent || '', block = blockOf(el), card = cardOf(el);
      let level = CODE.test(str) || CODE.test(own) ? 'self' : CODE.test(block.innerText || block.textContent || '') ? 'block' : card && CODE.test(card.innerText || card.textContent || '') ? 'card' : 'none';
      // A small field around the value ("Daily budget (CAD)" + slider) labels it too.
      if (level === 'card' || level === 'none') for (let p = block.parentElement, k = 0; p && k < 2 && p !== card && p !== document.body; p = p.parentElement, k++) { const t = p.innerText || ''; if (t.length > 160) break; if (CODE.test(t)) { level = 'block'; break; } }
      // A table cell is labelled by its column header ("Spend · USD").
      const td = (level === 'card' || level === 'none') && el.closest && el.closest('td');
      if (td) { const table = td.closest('table'), head = table && table.tHead && table.tHead.rows[0], th = head && head.cells[td.cellIndex]; if (th && CODE.test(th.innerText || th.textContent || '')) level = 'block'; }
      const cad = tracers.some(t => Math.abs(t - val) < 0.006);
      // A code on the amount or its own line/label counts; elsewhere in the card it does not.
      if (level === 'self' || level === 'block' || (level === 'card' && !cad)) continue;
      const key = sel(el) + '|' + m[0] + '|' + level; if (seenD.has(key)) continue; seenD.add(key);
      if (out.bareDollar.length < 80) out.bareDollar.push({ amount: m[0], cadTracer: cad, labelled: level, sel: sel(el), text: snippet(textNode ? (block.innerText || str) : str, 110) });
    }
  }
  texts.forEach(t => { if (t.text.includes('$')) dollar(t.el, t.text, true); });
  roots.forEach(root => root.querySelectorAll('input[type=number],input[type=text]').forEach(inp => {
    if (!visible(inp)) return; const prev = inp.previousSibling, lab = (prev && prev.textContent || '').trim();
    if (/\$$/.test(lab)) dollar(inp.parentElement, '$' + inp.value, false);
  }));
  roots.forEach(root => root.querySelectorAll('[data-tip]').forEach(el => { const v = el.getAttribute('data-tip').replace(/<[^>]+>/g, ' '); if (v.includes('$')) dollar(el, v, false); }));
  return out;
}

// Interactive controls in scope, each with a stable signature for the crawler.
function listControls(opts) {
  const roots = (opts.scopes || ['body']).map(s => document.querySelector(s)).filter(Boolean);
  const snippet = (t, n) => String(t || '').replace(/\s+/g, ' ').trim().slice(0, n || 60);
  const found = []; let seq = 0;
  const q = 'button,summary,select,input:not([type=hidden]),textarea,a[href],[role=button],[role=tab],[onclick],.sw,.crow,.bcBand,.lc-band,.bc-bar,[data-tip]';
  roots.forEach(root => root.querySelectorAll(q).forEach(el => {
    if (el.closest('[data-harness-tip],.toasts,#toasts,.vh,[aria-hidden="true"]') || el.readOnly) return;
    if (el.matches('[data-tip]') && !el.matches('.lc-band,.bc-bar,.bcBand,rect,circle,path,svg')) return; // tooltips on text are audited, not clicked
    if (el.parentElement && el.parentElement.closest('button,summary,a[href]') && !el.matches('input,select')) return;
    // Bars without orders only show a tooltip; the ones with orders are buttons.
    if (el.matches('.bcBand') && !el.matches('[role=button]')) return;
    const r = el.getBoundingClientRect();
    const seen = n => { const b = n.getBoundingClientRect(); return (n.checkVisibility ? n.checkVisibility({ checkOpacity: true, checkVisibilityCSS: true }) : true) && b.width >= 1 && b.height >= 1; };
    const label = el.matches('input[type=checkbox],input[type=radio]') && el.closest('label');
    if (!seen(el) && !(label && seen(label))) return; // a styled checkbox is operated through its label
    const tag = el.tagName.toLowerCase(), type = el.getAttribute('type') || '';
    const data = [...el.attributes].filter(a => a.name.startsWith('data-') && !/^data-(i|id|cid|d|x|ys|tip|harness-id|b|res|name|start|end|sel|ids|f)$/.test(a.name)).map(a => a.name + (/^(data-(v|view|p|lane|growth-lane|bg-tab|bg-action|b|rp|rpa|rpc|pb-filter|perf-preset|view-ad-version))$/.test(a.name) ? '=' + a.value : '')).sort().join(',');
    const text = snippet(el.matches('input,select,textarea') ? (el.getAttribute('aria-label') || el.name || el.id || el.className) : (el.innerText || el.getAttribute('aria-label') || el.title || el.className.baseVal || el.className), 50);
    const rowEl = el.closest('[data-cid],[data-i],[data-camp],[data-approval-id],.draft,.oppCard,.existingCampaign,article,tr');
    let row = rowEl ? (rowEl.getAttribute('data-cid') || rowEl.getAttribute('data-i') || rowEl.getAttribute('data-camp') || rowEl.getAttribute('data-approval-id') || '') : '';
    // Rows without an id attribute: the control's own data value names the row (data-bg-open="<group ref>").
    if (!row && rowEl) { const own = [...el.attributes].find(a => a.name.startsWith('data-') && a.name !== 'data-harness-id' && a.value.length > 3 && !/^(true|false)$/.test(a.value)); row = own ? own.value.slice(-40) : ''; }
    const chart = el.matches('.bcBand,.lc-band,.bc-bar') || (el.closest('svg') && el.matches('[data-tip]'));
    const family = [tag, type, el.getAttribute('role') || '', data, el.id && !/^\d/.test(el.id) ? '#' + el.id : '', chart ? '(chart mark)' : text.replace(/\d+([.,]\d+)?/g, '#')].join('|');
    const tabLike = el.getAttribute('role') === 'tab' || el.matches('[data-growth-lane],[data-bg-tab],[data-pb-filter],.oppTab,.srchip,.cbasis,.rpPreset,[data-view]');
    // Controls that leave the current view or close it wait until the view has been exercised.
    const backLike = el.matches('[data-bg-home],[data-bg-history-back],[data-bg-campaign-back]') || /^(←|‹|back\b|close\b|cancel\b|done\b|hide\b)/i.test(text) || (tag === 'button' && /^view all\b|^open (groups|approvals|overview|sales|controls|opportunities)\b/i.test(text));
    // Controls that delete or discard wait until the rest of the content has been exercised.
    const destructive = !backLike && /^(delete|remove|discard|reject|clear|dismiss)\b/i.test(text);
    const id = 'h' + (++seq) + '-' + Date.now().toString(36);
    el.setAttribute('data-harness-id', id);
    found.push({ id, tag, type, family, row, text, chart: !!chart, tabLike, backLike, destructive, inPopover: !!el.closest('.rpPop'), disabled: !!el.disabled || el.getAttribute('aria-disabled') === 'true',
      href: tag === 'a' ? el.getAttribute('href') : null, target: tag === 'a' ? el.getAttribute('target') : null,
      inDialog: !!el.closest('dialog[open],.ovsheet,[role=dialog]'), w: Math.round(r.width), h: Math.round(r.height) });
  }));
  return found;
}

// Anything modal that is open right now, each tagged with a stable selector.
function openOverlays() {
  const out = []; let n = 0;
  const tag = el => { let v = el.getAttribute('data-harness-overlay'); if (!v) { v = 'o' + (++n) + Date.now().toString(36); el.setAttribute('data-harness-overlay', v); } return '[data-harness-overlay="' + v + '"]'; };
  const title = el => String((el.querySelector('h1,h2,h3,h4,b,[style*="font-weight:700"]') || {}).textContent || el.id || el.className || '').replace(/\s+/g, ' ').trim().slice(0, 60);
  document.querySelectorAll('dialog[open]').forEach(d => out.push({ kind: 'dialog', sel: tag(d), id: d.id || d.className || 'dialog', modal: d.matches(':modal'), label: title(d) }));
  document.querySelectorAll('.ovsheet,[role=dialog]:not(dialog)').forEach(d => { const r = d.getBoundingClientRect(); if (r.width > 0 && r.height > 0) out.push({ kind: 'overlay', sel: tag(d), id: String(d.className).split(' ')[0], modal: false, label: title(d) }); });
  document.querySelectorAll('.rpPop').forEach(p => { if (p.style.display !== 'none' && p.getBoundingClientRect().width > 0) out.push({ kind: 'popover', sel: tag(p), id: p.id, modal: false, label: 'date range picker' }); });
  return out;
}

module.exports = { pageAudit, listControls, openOverlays };
