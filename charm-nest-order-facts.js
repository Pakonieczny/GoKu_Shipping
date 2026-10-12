/* Charm Nest · the order window's facts: one quiet glance line, everything else one gesture away.
   Paul, 12 Oct 2026: the order's facts were "a mush of stuff" that "screams beta": eleven boxes, every label and value the same weight. He
   asked for only the relevant data, with hover states that expand more or drop-downs, strong iconography so a person ties each fact to
   its aspect (who bought it, when, when it has to ship, the buyer's message) and lots of feedback, "not cluttered".

   What it draws into #owMeta (the host the bridge gives it):
     1  The glance line: icon + short value chips, wrapped as whole chips, never a sideways scroll: the order number (a click copies it, the
        icon turns into a check), the buyer, when it was bought ("Today 11:02 AM"), when it must ship (the one urgent fact: a quiet green
        chip with the days left, amber from the day before, red only when it is late), the metal with its colour dot, then only when there
        is something to say: the pairs and counts, a custom order, the engraving, a message from the buyer (with the unread dot of the
        Customer tab); the Details drop-down sits on the first line, directly after the metal (Paul, 12 Oct 2026: "always be on the first
        line to the right side of the SS sheet button"): the order, the buyer, the time, the ship-by, the metal and Details are one row that
        never breaks inside (the buyer and the ship-by are shortened with an ellipsis before anything wraps; the order number, the time and the metal never are); the chips
        after it flow behind it and are the ones that wrap; a state that needs a look is one small pill at the right end.
     2  A chip rests 350 ms under the pointer, or is pressed / tapped / reached with the keyboard, and opens ONE small card anchored to it
        inside the window (never a pop-up, never over another one): the full date, the order's listing and its full title (with copies),
        the metal choice, "Matching: the Right is the mirror of the Left", the quantity, the engraving's words and seals, the message.
        It stays inside the window (above the chip when there is no room below), closes on Esc, a press outside, a scroll, a resize or when
        focus leaves, and a press on the chip pins it. The card of a keyboard user is the next thing in the Tab order after its chip.
     3  Details: the drop-down that opens the complete old grid (every cell it ever had, in the order it had them, as `.m` cells with
        their <i> label and <span> value) as a tidy two-column list, so nothing is lost. It stays as it was while the person walks
        from piece to piece.
   Everything moves 120-180 ms on transform and opacity (the card, the drop-down's height); prefers-reduced-motion turns it into a plain
   fade or nothing. Nothing here reads a record, calls the network or keeps a timer while no card is showing.
   The bridge gathers the facts (OrderWin.factsOf) and calls OrderFacts.paint(host, facts); the words, the icons and the cards are all here.
   The pure helpers (whenFriendly, shipInfo, chipsOf ...) need no DOM, so the node tests use them directly. */
(function (root) {
  'use strict';
  const doc = root.document;
  const API = root.OrderFacts = root.OrderFacts || {};

  /* ── words ── */
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const MIN = 60000, HOUR = 3600000;
  const fmt = (o) => { try { return new Intl.DateTimeFormat('en-US', o); } catch (_) { return null; } };
  const F = {
    day: fmt({ month: 'short', day: 'numeric' }), dayYear: fmt({ month: 'short', day: 'numeric', year: 'numeric' }),
    time: fmt({ hour: 'numeric', minute: '2-digit' }), long: fmt({ weekday: 'long', month: 'long', day: 'numeric', year: 'numeric' })
  };
  const say = (f, ms, alt) => { try { return f ? f.format(new Date(ms)).replace(/ /g, ' ') : alt || ''; } catch (_) { return alt || ''; } };
  const nowMs = () => { try { const c = root.SimClock && root.SimClock.now && root.SimClock.now(); return +c > 0 ? +c : Date.now(); } catch (_) { return Date.now(); } };
  /** Whole shop days (local, DST-safe) from the day of `fromMs` to the day of `toMs`: 0 same day, 1 the next day, -1 the day before. */
  function dayDiff(toMs, fromMs) {
    const a = new Date(fromMs), b = new Date(toMs);
    return Math.round((Date.UTC(b.getFullYear(), b.getMonth(), b.getDate()) - Date.UTC(a.getFullYear(), a.getMonth(), a.getDate())) / 86400000);
  }
  const plural = (n, w) => n + ' ' + w + (n === 1 ? '' : 's');
  /** How long ago, in the few words a glance wants: "just now", "12 minutes ago", "3 hours ago", "2 days ago"; "" for a time in the future. */
  function ago(ms, now) {
    now = now == null ? nowMs() : now;
    const d = now - ms;
    if (!(d > -MIN)) return '';
    if (d < MIN) return 'just now';
    if (d < HOUR) return plural(Math.round(d / MIN), 'minute') + ' ago';
    if (d < 24 * HOUR) return plural(Math.round(d / HOUR), 'hour') + ' ago';
    return plural(Math.max(1, Math.round(d / 86400000)), 'day') + ' ago';
  }
  /** When it was bought, for a chip and for its card: { short: "Today 11:02 AM" | "Yesterday 4:15 PM" | "Oct 5, 4:15 PM" (the year when it is not this one), full: "Monday, October 12, 2026", time: "11:02 AM", ago } ; null for no time. */
  function whenFriendly(ms, now) {
    ms = +ms; if (!(ms > 0)) return null;
    now = now == null ? nowMs() : now;
    const diff = dayDiff(ms, now), time = say(F.time, ms), sameYear = new Date(ms).getFullYear() === new Date(now).getFullYear();
    const short = diff === 0 ? 'Today ' + time : diff === -1 ? 'Yesterday ' + time : (sameYear ? say(F.day, ms) : say(F.dayYear, ms)) + ', ' + time;
    return { short, full: say(F.long, ms), time, ago: ago(ms, now), days: diff };
  }
  /** Where the ship-by date stands: { date: "Oct 19", days, words: "7 days left" | "Due today" | "Due tomorrow" | "2 days late", chip: "Oct 19 · 7 days left", full, tone }.
   *  Tone is the Orders list's own rule (a late day is bad, today and tomorrow are warn, the rest calm) unless `quiet`: an order that is cancelled or finished is told,
   *  never alarmed. null when the order has no ship-by. shipBy is Etsy's own, in seconds. */
  function shipInfo(shipBySec, now, quiet) {
    const by = +shipBySec * 1000; if (!(by > 0)) return null;
    now = now == null ? nowMs() : now;
    const days = dayDiff(by, now), date = say(F.day, by);
    const words = days < -1 ? plural(-days, 'day') + ' late' : days === -1 ? '1 day late' : days === 0 ? 'Due today' : days === 1 ? 'Due tomorrow' : days > 99 ? '' : plural(days, 'day') + ' left';
    const tail = days === 0 ? 'today' : days === 1 ? 'tomorrow' : words;
    return { date, days, words, chip: tail ? date + ' · ' + tail : date, full: say(F.long, by), tone: quiet ? '' : days < 0 ? 'bad' : days <= 1 ? 'warn' : 'ok', late: days < 0 };
  }

  /* ── icons (charm-nest-icons.js; a page without it still has every word and chip) ── */
  const ic = (name, o) => { try { const I = root.CharmNestIcons; return I && I.svg ? I.svg(name, Object.assign({ size: 14, weight: 1.8 }, o || {})) : ''; } catch (_) { return ''; } };

  /* ── the chips: what the glance line says and what each one's card holds ──
     facts = { loading, key, order:{id, num}, purchased:{ts, label}, buyer:{name, gift}, ship:{by, quiet}, metal:{label, color, none, choices:[{name,value}]},
               qty:{n, pieces, pair, mismatched, counted:{total, noun}, notes:[]}, custom:{label, note}, engraving:{chip, html, approved},
               message:{buyer, gift, unread, bad}, listing:{id, title}, state:{tone, text, sheet}, cells:[…], on:{customer, otherCharm},
               titleMatch:{sku, why} (NOSKU: a listing with no SKU on Etsy whose charm the app took from the words of its title) } */
  const row = (k, v, o) => Object.assign({ k, v: String(v) }, o || {});
  function chipsOf(f, now) {
    now = now == null ? nowMs() : now;
    const out = [];
    const o = f.order;
    if (o && o.id) {
      const rows = [row('Order number', o.id, { mono: 1, copy: o.id, copyLabel: 'Copy the order number' })];
      if (o.num && o.num !== o.id) rows.push(row('Shop order number', '#' + o.num, { mono: 1, copy: o.num, copyLabel: 'Copy the shop order number' }));
      const l = f.listing;
      if (l && l.id) rows.push(row('Listing', l.id, { mono: 1, copy: l.id, copyLabel: 'Copy the listing number' }));
      if (l && l.title) rows.push(row('Title', l.title));
      out.push({ id: 'order', icon: 'order', text: o.id, mono: 1, copy: o.id, aria: 'Order ' + o.id + ', press to copy the number', pop: { title: 'Order', icon: 'order', rows, hint: 'Press the number to copy it' } });
    }
    const b = f.buyer;
    if (b && b.name) {
      const rows = [row('Name', b.name)];
      if (b.gift) rows.push(row('Gift', 'Yes, bought as a gift'));
      out.push({ id: 'buyer', icon: 'buyer', text: b.name, gift: !!b.gift, aria: 'Buyer ' + b.name + (b.gift ? ', a gift' : ''), pop: { title: 'Buyer', icon: 'buyer', rows } });
    } else if (b && b.gift) out.push({ id: 'buyer', icon: 'gift', text: 'Gift', aria: 'This order is a gift', pop: { title: 'Gift', icon: 'gift', rows: [row('Gift', 'Yes, bought as a gift')] } });
    const p = f.purchased, w = p && whenFriendly(p.ts, now);
    if (w) {
      const label = p.label || 'Purchased';
      out.push({ id: 'purchased', icon: 'purchased', text: w.short, aria: label + ' ' + w.short, pop: { title: label, icon: 'purchased', rows: [row('Date', w.full), row('Time', w.time + (w.ago ? ' · ' + w.ago : ''))] } });
    }
    const sh = f.ship && shipInfo(f.ship.by, now, f.ship.quiet);
    if (sh) out.push({ id: 'ship', icon: 'shipby', text: sh.chip, tone: sh.tone, late: sh.late, aria: 'Ship by ' + sh.date + (sh.words ? ', ' + sh.words.toLowerCase() : ''), pop: { title: 'Ship by', icon: 'shipby', rows: [row('Date', sh.full)].concat(sh.words ? [row(sh.late ? 'Late by' : 'Time left', sh.late ? sh.words.replace(/ late$/, '') : sh.words)] : []), hint: sh.late && !(f.ship && f.ship.quiet) ? 'This order is past its ship-by date' : '' } });
    const m = f.metal;
    if (m) {
      const rows = [row('Metal', m.none ? 'No metal read yet' : m.label)].concat((m.choices || []).map(c => row(c.name || 'Metal choice', c.value)));
      out.push({ id: 'metal', icon: 'metal', text: m.none ? 'No metal' : m.label, dot: m.none ? '' : m.color, tone: m.none ? 'bad' : '', aria: 'Metal ' + (m.none ? 'not read yet' : m.label), pop: { title: 'Metal', icon: 'metal', rows } });
    }
    const q = f.qty;
    if (q && (q.pair || q.counted || q.n > 1)) {
      const rows = [];
      if (q.pair) {
        rows.push(row('Quantity', plural(q.n, 'pair') + ' · ' + plural(q.pieces, 'piece') + ', Left and Right'));
        rows.push(row('Pair', q.mismatched ? 'Mismatched: the Left and the Right are different charms of one listing' : 'Matching: the Right is the mirror of the Left'));
      } else if (q.counted) rows.push(row('Quantity', q.n + ' \u00b7 ' + plural(q.counted.total, q.counted.noun)));
      else rows.push(row('Quantity', String(q.n)));
      for (const n of q.notes || []) rows.push(row('Note', n));
      const text = q.pair ? plural(q.n, 'pair') : q.counted ? plural(q.counted.total, q.counted.noun) : '×' + q.n;
      out.push({ id: 'qty', icon: q.pair ? 'pair' : 'quantity', text, aria: (q.pair ? 'Pair: ' : 'Quantity: ') + text, pop: { title: q.pair ? 'Pair' : 'Quantity', icon: q.pair ? 'pair' : 'quantity', rows } });
    }
    const c = f.custom;
    if (c && c.label) out.push({ id: 'custom', icon: 'custom', text: c.label, aria: 'Custom order: ' + c.label, pop: { title: 'Custom order', icon: 'custom', rows: [row('Kind', c.label)].concat(c.note ? [row('Reading', c.note)] : []) } });
    const tm = f.titleMatch;
    if (tm && tm.sku) out.push({ id: 'titlematch', icon: 'title', text: 'Matched by title', aria: 'This listing has no SKU on Etsy; its charm ' + tm.sku + " was matched by the listing's title", pop: { title: "Matched by the listing's title", icon: 'title', rows: [row('Charm', tm.sku, { mono: 1, copy: tm.sku, copyLabel: 'Copy the charm SKU' }), row('Why', tm.why || "its title names this charm and no other")], note: "This listing has no SKU on Etsy. Nothing was saved: a person's choice, once made, always wins.", action: f.on && f.on.otherCharm ? { id: 'otherCharm', label: 'Use another charm' } : null } });
    const e = f.engraving;
    if (e) out.push({ id: 'engraving', icon: 'engrave', text: e.chip || 'Engraving', tone: e.approved ? 'ok' : '', aria: 'Engraving: ' + (e.chip || 'needed') + (e.approved ? ', approved' : ''), pop: { title: 'Engraving', icon: 'engrave', html: e.html || '' } });
    const g = f.message;
    if (g && (g.buyer || g.gift || g.unread || g.bad)) {
      const secs = [];
      if (g.buyer) secs.push({ k: 'Message from the buyer', t: g.buyer });
      if (g.gift) secs.push({ k: 'Gift message', t: g.gift });
      let note = '';
      if (g.unread) { const n = Math.max(1, +g.unread || 1); note = n + ' new ' + (n === 1 ? 'reply' : 'replies') + ' from the buyer in the Customer tab'; }
      else if (g.bad) note = 'A message to this buyer needs a look in the Customer tab';
      out.push({ id: 'message', icon: 'message', text: 'Message', dot: g.unread ? 'unread' : g.bad ? 'bad' : '', aria: 'Message from the buyer' + (g.unread ? ', unread' : ''), pop: { title: 'Message from the buyer', icon: 'message', secs, note, noteBad: !g.unread && !!g.bad, action: note ? { id: 'customer', label: 'Open the Customer tab' } : null } });
    }
    if (f.state) out.push({ id: 'state', right: 1, pop: { title: 'State', icon: 'state', rows: [row('State', f.state.text)].concat(f.state.sheet ? [row('Sheet', f.state.sheet)] : []) } });
    return out;
  }

  /* ── the markup ── */
  const chipHtml = (c, rove) => {
    const lead = c.dot && c.dot !== 'unread' && c.dot !== 'bad' ? (/^[#\w(),.% -]+$/.test(c.dot) ? `<i class="owfDotM" style="--mc:${esc(c.dot)}"></i>` : '') : c.dot ? `<i class="owDot owfUnread${c.dot === 'bad' ? ' bad' : ''}" aria-hidden="true"></i>` : '';
    const t = c.id === 'ship' ? (() => { const i = c.text.indexOf(' · '); return i < 0 ? esc(c.text) : esc(c.text.slice(0, i)) + '<span class="sub"> · ' + esc(c.text.slice(i + 3)) + '</span>'; })() : esc(c.text);
    return `<button type="button" class="owfChip${c.tone ? ' ' + c.tone : ''}${c.late ? ' late' : ''}" data-owf-chip="${esc(c.id)}" data-owf-item${c.copy ? ` data-copyval="${esc(c.copy)}"` : ''} aria-haspopup="true" aria-expanded="false" aria-label="${esc(c.aria)}" tabindex="${rove ? 0 : -1}">` +
      `<span class="owfIc">${ic(c.icon)}${lead}</span><span class="owfTx${c.mono ? ' mono' : ''}">${t}</span>` +
      (c.gift ? `<span class="owfGift" aria-hidden="true">${ic('gift', { size: 12 })}</span>` : '') +
      (c.copy ? `<span class="owfCp" aria-hidden="true"><span class="cp">${ic('copy', { size: 13 })}</span><span class="ck">${ic('check', { size: 13, weight: 2.2 })}</span></span>` : '') + `</button>`;
  };
  const stateHtml = s => `<button type="button" class="owfChip owfState ${esc(s.tone || 'neutral')}" data-owf-chip="state" data-owf-item aria-haspopup="true" aria-expanded="false" aria-label="State: ${esc(s.text)}" tabindex="-1"><i class="owfSdot" aria-hidden="true"></i><span class="owfTx">${esc(s.text)}</span></button>`;
  const moreHtml = rove => `<button type="button" class="owfMore" data-owf-item aria-expanded="false" aria-controls="owfDetails" tabindex="${rove ? 0 : -1}"><span>Details</span>${ic('chevron-down', { size: 14, weight: 2 })}</button>`;
  /** The first row of the glance line. The chips that tell which order it is (order, buyer, time, ship-by, metal) are ONE row and Details is its last item, right after the
   *  metal (after the last of them when there is no metal chip): the toggle always sits on the line of that chip, never alone on the next. The row does not break inside while
   *  the buyer and the ship-by can give way (see .owfCore in charm-nest-1.html); the chips that come later (pairs, a custom order, engraving, message) follow it
   *  and are the ones that wrap. A state that needs a look is the pill at the right end. DOM order is the visual order, so the Tab and arrow keys walk the line as it reads. */
  const CORE = ['order', 'buyer', 'purchased', 'ship', 'metal'];
  function barHtml(chips, f) {
    const left = chips.filter(c => !c.right), core = left.filter(c => CORE.includes(c.id)), later = left.filter(c => !CORE.includes(c.id));
    const last = core.pop(), has = (f.cells || []).length > 0;
    let h = '';
    if (last || has) h += '<span class="owfCore">' + core.map((c, i) => chipHtml(c, i === 0)).join('') + '<span class="owfTail">' + (last ? chipHtml(last, !core.length) : '') + (has ? moreHtml(!last) : '') + '</span></span>';
    h += later.map(c => chipHtml(c, false)).join('');
    return h + (f.state ? '<span class="owfEnd">' + stateHtml(f.state) + '</span>' : '');
  }
  const copyBtn = (v, label) => `<button type="button" class="owfCopy" data-copy="${esc(v)}" aria-label="${esc(label || 'Copy')}" title="${esc(label || 'Copy')}"><span class="cp">${ic('copy', { size: 13 })}</span><span class="ck">${ic('check', { size: 13, weight: 2.2 })}</span></button>`;
  const cellHtml = c => {
    const cls = 'm' + (c.wide ? ' wide' : '') + (c.block ? ' block' : '') + (c.mono ? ' mono' : '');
    if (c.html != null) return `<div class="${cls}"><i>${esc(c.label)}</i>${c.html}</div>`;
    return `<div class="${cls}"><i>${esc(c.label)}</i><span>${esc(c.text)}</span>${c.copy ? copyBtn(c.copy, 'Copy ' + String(c.label).toLowerCase()) : ''}</div>`;
  };
    function popHtml(p) {
    let h = `<div class="owfPopHd">${ic(p.icon || 'info')}<b>${esc(p.title)}</b></div>`;
    if (p.rows && p.rows.length) h += '<dl class="owfRows">' + p.rows.map(r => `<div class="owfRow">${r.k === p.title ? '' : `<dt>${esc(r.k)}</dt>`}<dd class="${r.mono ? 'mono' : ''}">${esc(r.v)}</dd>${r.copy ? copyBtn(r.copy, r.copyLabel) : ''}</div>`).join('') + '</dl>';
    if (p.html) h += `<div class="owfHtml">${p.html}</div>`;
    for (const s of p.secs || []) h += `<div class="owfSec">${s.k === p.title ? '' : `<span class="lbl">${esc(s.k)}</span>`}<div class="owfMsg">${esc(s.t)}</div></div>`;
    if (p.note) h += `<p class="owfNote${p.noteBad ? ' bad' : ''}"><i class="owDot${p.noteBad ? ' bad' : ''}" aria-hidden="true"></i>${esc(p.note)}</p>`;
    if (p.action) h += `<button type="button" class="owfBtn" data-act="${esc(p.action.id)}">${esc(p.action.label)}${ic('chevron-right', { size: 13 })}</button>`;
    if (p.hint) h += `<p class="owfHint">${esc(p.hint)}</p>`;
    return h;
  }

  /* ── the drawn thing, per host ── */
  const reduced = () => { try { return root.Motion && Motion.reduced ? !!Motion.reduced() : root.matchMedia('(prefers-reduced-motion: reduce)').matches; } catch (_) { return false; } };
  const EASE = 'cubic-bezier(.2,.8,.2,1)', REST = 350, BEAT = 90, LEAVE = 170, FADE = 150;
  const states = new WeakMap();
  const anims = el => { try { return el.getAnimations ? el.getAnimations() : []; } catch (_) { return []; } };   // (a page or a test without the Web Animations API still works, without the movement)
  const play = (el, frames, o) => { try { return el.animate ? el.animate(frames, o) : null; } catch (_) { return null; } };
  const S_OF = host => { let s = states.get(host); if (!s) { s = { sig: '', barSig: '', listSig: '', chips: new Map(), open: '', by: '', details: false, timers: {}, on: {}, pop: null, live: null, copyT: new Map(), wired: false, ptr: 0, escAt: 0, rendering: false }; states.set(host, s); } return s; };
  const kill = (s, k) => { if (s.timers[k]) { clearTimeout(s.timers[k]); s.timers[k] = 0; } };

  async function copyText(text, host) {
    try { if (root.navigator && navigator.clipboard && navigator.clipboard.writeText) { await navigator.clipboard.writeText(text); return true; } } catch (_) { /* the textarea below */ }
    try {   // (inside the window: a modal dialog makes the rest of the page inert, so a field outside it cannot be selected)
      const box = (host && host.closest && host.closest('dialog[open]')) || doc.body, t = doc.createElement('textarea');
      t.value = text; t.setAttribute('readonly', ''); t.style.cssText = 'position:fixed;left:-9999px;top:0;opacity:0'; box.appendChild(t); t.select();
      const ok = doc.execCommand && doc.execCommand('copy'); t.remove(); return !!ok;
    } catch (_) { return false; }
  }
  function say2(host, text) { const s = S_OF(host); if (s.live) { s.live.textContent = ''; setTimeout(() => { if (s.live) s.live.textContent = text; }, 30); } }
  /** The check that answers a copy: the button (or chip) shows it for a moment and the live region says it. */
  async function doCopy(host, btn, value) {
    const s = S_OF(host), ok = await copyText(value, host);
    kill(s, 'copy:' + (btn.dataset.copy || btn.dataset.owfChip || ''));
    btn.classList.remove('copied', 'failed'); void btn.offsetWidth;
    btn.classList.add(ok ? 'copied' : 'failed');
    say2(host, ok ? 'Copied' : 'Could not copy: select the text and press Ctrl+C');
    const key = 'copy:' + (btn.dataset.copy || btn.dataset.owfChip || '');
    s.timers[key] = setTimeout(() => { btn.classList.remove('copied', 'failed'); s.timers[key] = 0; }, ok ? 1500 : 2600);
    return ok;
  }

  const scrollerOf = el => { for (let a = el && el.parentElement; a && a !== doc.body; a = a.parentElement) { const o = root.getComputedStyle(a).overflowY; if (o === 'auto' || o === 'scroll') return a; } return null; };
  /** Where the card goes: centred under its chip (over it when there is no room), clamped to the window's visible part, with its arrow on the chip. */
  function place(host, pop, chip) {
    const op = pop.offsetParent; if (!op || !chip.isConnected) return;
    pop.style.maxHeight = ''; pop.firstElementChild.style.maxHeight = '';
    const dlg = host.closest('dialog'), sc = scrollerOf(host), W = root.innerWidth || 1200, H = root.innerHeight || 800;
    const rs = [dlg, sc].filter(Boolean).map(e => e.getBoundingClientRect());
    const box = { l: Math.max(0, ...rs.map(r => r.left)), t: Math.max(0, ...rs.map(r => r.top)), r: Math.min(W, ...rs.map(r => r.right)), b: Math.min(H, ...rs.map(r => r.bottom)) };
    const M = 8, a = chip.getBoundingClientRect(), o = op.getBoundingClientRect(), pw = pop.offsetWidth;
    let ph = pop.offsetHeight;
    const roomBelow = box.b - M - (a.bottom + 8), roomAbove = a.top - 8 - (box.t + M);
    let up = ph > roomBelow && roomAbove > roomBelow;
    const room = Math.max(60, Math.floor(up ? roomAbove : roomBelow));
    if (ph > room) { pop.firstElementChild.style.maxHeight = (room - 22) + 'px'; ph = pop.offsetHeight; }
    // (under its chip's left edge, inside the facts' own column when it fits there, else inside the window; the arrow stays on the chip)
    const hr = host.getBoundingClientRect(), colL = hr.width >= pw + 2 * M ? Math.max(box.l, hr.left) : box.l, colR = hr.width >= pw + 2 * M ? Math.min(box.r, hr.right) : box.r;
    let x = Math.max(colL + (colL === box.l ? M : 0), Math.min(a.left, colR - (colR === box.r ? M : 0) - pw));
    if (box.r - box.l - 2 * M < pw) x = box.l + M;
    const y = up ? a.top - 8 - ph : a.bottom + 8;
    pop.style.left = Math.round(x - o.left) + 'px'; pop.style.top = Math.round(y - o.top) + 'px';
    pop.style.setProperty('--ax', Math.round(Math.max(16, Math.min(a.left + a.width / 2 - x, pw - 16))) + 'px');
    pop.classList.toggle('above', up); pop.classList.toggle('below', !up);
    pop.dataset.side = up ? 'above' : 'below';
  }

  function openCard(host, id, by, quiet) {
    const s = S_OF(host), chip = host.querySelector(`[data-owf-chip="${id}"]`), c = s.chips.get(id); if (!chip || !c) return false;
    kill(s, 'rest'); kill(s, 'leave');
    const same = s.open === id, pop = s.pop;
    if (s.open && !same) { const was = host.querySelector(`[data-owf-chip="${s.open}"]`); if (was) was.setAttribute('aria-expanded', 'false'); }
    s.open = id; s.by = by;
    pop.innerHTML = '<div class="owfPopIn" tabindex="-1">' + popHtml(c.pop) + '</div>';
    pop.setAttribute('aria-label', c.pop.title);
    chip.after(pop);
    pop.classList.toggle('peek', by === 'hover');   // (a card the pointer only peeks at is click-through: the chips under it stay reachable; a press, the keyboard or a tap makes it a card to use)
    pop.hidden = false; pop.style.visibility = 'hidden';
    pop.id = pop.id || 'owfPop' + Math.random().toString(36).slice(2, 7);
    chip.setAttribute('aria-expanded', 'true'); chip.setAttribute('aria-controls', pop.id);
    place(host, pop, chip);
    pop.style.visibility = '';
    if (!same && !quiet) {
      anims(pop).forEach(a => a.cancel());
      const up = pop.classList.contains('above'), ax = pop.style.getPropertyValue('--ax') || '50%';
      pop.style.transformOrigin = `${ax} ${up ? '100%' : '0'}`;
      if (reduced()) play(pop, [{ opacity: 0 }, { opacity: 1 }], { duration: 100, easing: 'ease' });
      else play(pop, [{ opacity: 0, transform: `translateY(${up ? 4 : -4}px) scale(.97)` }, { opacity: 1, transform: 'none' }], { duration: FADE, easing: EASE });
    }
    if (!s.watch) {   // (while a card shows, and only then: a press elsewhere, a scroll, a resize, Esc)
      s.watch = {
        down: e => { if (!host.contains(e.target) || !(e.target.closest && e.target.closest('.owfPop,[data-owf-chip]'))) closeCard(host); },
        scroll: e => { if (!(pop.contains(e.target))) closeCard(host); },
        size: () => closeCard(host),
        key: e => { if (e.key === 'Escape' && s.open) { e.preventDefault(); e.stopPropagation(); s.escAt = Date.now(); closeCard(host, true); } }
      };
      doc.addEventListener('pointerdown', s.watch.down, true); doc.addEventListener('scroll', s.watch.scroll, { capture: true, passive: true });
      root.addEventListener('resize', s.watch.size); doc.addEventListener('keydown', s.watch.key, true);
    }
    return true;
  }
  function closeCard(host, giveFocusBack, quiet) {
    const s = S_OF(host); kill(s, 'rest'); kill(s, 'leave');
    if (!s.open) return false;
    const chip = host.querySelector(`[data-owf-chip="${s.open}"]`), pop = s.pop, inside = pop && pop.contains(doc.activeElement);
    s.open = ''; s.by = '';
    if (chip) chip.setAttribute('aria-expanded', 'false'), chip.removeAttribute('aria-controls');
    if (pop) { anims(pop).forEach(a => a.cancel()); pop.hidden = true; host.appendChild(pop); }
    if (s.watch) { doc.removeEventListener('pointerdown', s.watch.down, true); doc.removeEventListener('scroll', s.watch.scroll, true); root.removeEventListener('resize', s.watch.size); doc.removeEventListener('keydown', s.watch.key, true); s.watch = null; }
    if (chip && (giveFocusBack || inside) && !quiet) { const was = s.rendering; s.rendering = true; try { chip.focus({ preventScroll: true }); } catch (_) {} s.rendering = was; }   // (the focus coming back to its chip must not open the card again)
    return true;
  }

  function setDetails(host, open, animate) {
    const s = S_OF(host), d = host.querySelector('.owfDetails'), b = host.querySelector('.owfMore'); if (!d || !b) return;
    open = !!open; s.details = open; b.setAttribute('aria-expanded', open ? 'true' : 'false');
    anims(d).forEach(a => a.cancel()); d.style.overflow = '';
    const go = animate && !reduced() && d.isConnected;
    if (open) {
      d.hidden = false;
      if (go) { const h = d.scrollHeight; if (h) { d.style.overflow = 'hidden'; const a = play(d, [{ height: '0px', opacity: 0 }, { height: h + 'px', opacity: 1 }], { duration: 170, easing: EASE }); const end = () => { d.style.overflow = ''; }; if (a) { a.onfinish = end; a.oncancel = end; } else end(); } }
    } else if (go && !d.hidden) {
      const h = d.offsetHeight; d.style.overflow = 'hidden';
      const a = play(d, [{ height: h + 'px', opacity: 1 }, { height: '0px', opacity: 0 }], { duration: 140, easing: EASE });
      if (a) { a.onfinish = () => { if (!s.details) d.hidden = true; d.style.overflow = ''; }; a.oncancel = () => { d.style.overflow = ''; }; } else { d.hidden = true; d.style.overflow = ''; }
    } else d.hidden = true;
  }

  /** A chip shortened with an ellipsis (a long buyer name, or the ship-by on a tight line) says its full text in its title; it is set when the pointer or the focus
   *  reaches the chip, so a chip that fits has none (its card is how everyone reads it). */
  const titleIfCut = chip => { const t = chip.querySelector('.owfTx'); if (!t) return; if (t.scrollWidth > t.clientWidth + 1) chip.title = (t.textContent || '').replace(/\s+/g, ' ').trim(); else chip.removeAttribute('title'); };

  function wire(host) {
    const s = S_OF(host); if (s.wired) return; s.wired = true;
    const idOf = n => { const c = n && n.closest && n.closest('[data-owf-chip]'); return c && host.contains(c) ? c : null; };
    const kbdNow = () => !(s.ptr && Date.now() - s.ptr < 120);
    doc.addEventListener('pointerdown', () => { s.ptr = Date.now(); }, true);
    // hover: a chip must rest 350 ms (a beat when a card is already open, as the dots do); the card is a place the pointer may go to
    host.addEventListener('pointerover', e => {
      if (e.pointerType === 'touch') return;
      if (e.target.closest && e.target.closest('.owfPop')) { kill(s, 'leave'); return; }
      const chip = idOf(e.target); if (!chip || (e.relatedTarget && chip.contains(e.relatedTarget))) return;
      titleIfCut(chip);
      kill(s, 'leave'); kill(s, 'rest');
      const id = chip.dataset.owfChip; if (s.open === id || s.by === 'pin') return;
      s.timers.rest = setTimeout(() => { s.timers.rest = 0; if (host.isConnected) openCard(host, id, 'hover'); }, s.open ? BEAT : REST);
    });
    host.addEventListener('pointerout', e => {
      if (e.pointerType === 'touch') return;
      const to = e.relatedTarget, chip = idOf(e.target), pop = e.target.closest && e.target.closest('.owfPop');
      if (!((chip && !(to && chip.contains(to))) || (pop && !(to && pop.contains(to))))) return;
      if (to && (idOf(to) || (to.closest && to.closest('.owfPop')))) { kill(s, 'rest'); return; }
      kill(s, 'rest');
      if (s.open && s.by === 'hover') { kill(s, 'leave'); s.timers.leave = setTimeout(() => { s.timers.leave = 0; if (s.by === 'hover') closeCard(host); }, LEAVE); }
    });
    host.addEventListener('click', e => {
      const cp = e.target.closest && e.target.closest('[data-copy]');
      if (cp && host.contains(cp)) { e.preventDefault(); doCopy(host, cp, cp.dataset.copy); return; }
      if (e.target.closest && e.target.closest('.owfMore')) { setDetails(host, !s.details, true); return; }
      const act = e.target.closest && e.target.closest('[data-act]');
      if (act && host.contains(act)) { const fn = s.on && s.on[act.dataset.act]; closeCard(host); if (typeof fn === 'function') { try { fn(); } catch (_) {} } return; }
      const chip = idOf(e.target); if (!chip) return;
      const id = chip.dataset.owfChip;
      if (chip.dataset.copyval) doCopy(host, chip, chip.dataset.copyval);   // (the order number: one press copies it, and its card stays up to show it)
      if (s.open === id && s.by === 'pin' && !chip.dataset.copyval) closeCard(host);
      else openCard(host, id, 'pin');
    });
    // keyboard: a chip that gets the focus opens its card at once; the arrows walk the line (one Tab stop for it); Esc is the document's (openCard)
    host.addEventListener('focusin', e => {
      const chip = idOf(e.target);
      if (chip) {
        titleIfCut(chip);
        host.querySelectorAll('[data-owf-item]').forEach(n => { n.tabIndex = n === chip ? 0 : -1; });
        if (kbdNow() && !s.rendering && s.by !== 'pin') openCard(host, chip.dataset.owfChip, 'focus');
        return;
      }
      const more = e.target.closest && e.target.closest('.owfMore');
      if (more) host.querySelectorAll('[data-owf-item]').forEach(n => { n.tabIndex = n === more ? 0 : -1; });
    });
    host.addEventListener('focusout', e => {
      if (s.rendering || !s.open) return;
      const to = e.relatedTarget, chip = host.querySelector(`[data-owf-chip="${s.open}"]`);
      if (to && ((chip && chip.contains(to)) || (s.pop && s.pop.contains(to)))) return;
      if (s.by === 'hover') return;
      closeCard(host, false, true);
    });
    host.addEventListener('keydown', e => {
      const items = [...host.querySelectorAll('.owfBar [data-owf-item]')];
      const at = items.indexOf(doc.activeElement);
      if (at < 0 || e.altKey || e.ctrlKey || e.metaKey) return;
      let to = -1;
      if (e.key === 'ArrowRight') to = (at + 1) % items.length; else if (e.key === 'ArrowLeft') to = (at - 1 + items.length) % items.length;
      else if (e.key === 'Home') to = 0; else if (e.key === 'End') to = items.length - 1;
      if (to < 0) return;
      e.preventDefault(); items[to].focus();
    });
  }

  /** The window's facts into `host`. Called as often as the window paints: nothing is touched when nothing changed (a card, the focus
   *  and the drop-down stay exactly where they are); when something did, the line is drawn again and what was open is open again. */
  function paint(host, f) {
    if (!host || !doc) return;
    const s = S_OF(host), now = nowMs();
    s.on = (f && f.on) || {};
    const chips = f.loading ? [] : chipsOf(f, f.now == null ? now : f.now);
    // (the line says a day, never a minute: it is drawn again when the day changes, not every minute)
    const bar = JSON.stringify([f.loading, f.key, chips.map(c => [c.id, c.text, c.tone, c.dot, c.gift, c.aria, c.copy, c.late]), f.state, (f.cells || []).length > 0, (f.loading && f.order) || 0, (f.loading && f.buyer) || 0]);
    const popSig = JSON.stringify(chips.map(c => c.pop));
    const list = JSON.stringify(f.cells || []);
    const sig = bar + '|' + popSig + '|' + list;
    if (s.sig === sig && host.firstElementChild) return;
    s.sig = sig;
    wire(host);
    s.rendering = true;
    try {
      const keepFocus = host.contains(doc.activeElement) ? (doc.activeElement.dataset.owfChip ? '[data-owf-chip="' + doc.activeElement.dataset.owfChip + '"]' : doc.activeElement.classList.contains('owfMore') ? '.owfMore' : null) : null;
      const reopen = s.open, by = s.by;
      if (!s.pop) { s.pop = doc.createElement('div'); s.pop.className = 'owfPop'; s.pop.hidden = true; s.pop.setAttribute('role', 'group'); }
      if (s.open) closeCard(host, false, true);
      host.appendChild(s.pop);   // (kept out of what is drawn again)
      s.chips = new Map(chips.map(c => [c.id, c]));
      if (!s.live) { s.live = doc.createElement('span'); s.live.className = 'owfLive'; s.live.setAttribute('role', 'status'); s.live.setAttribute('aria-live', 'polite'); }
            if (!host.querySelector('.owfBar')) {
        host.innerHTML = '<div class="owfBar" role="toolbar" aria-label="Order facts"></div><div class="owfDetails" hidden></div>';
        host.append(s.live, s.pop);
      }
      if (s.barSig !== bar) {
        s.barSig = bar;
        let h;
        if (f.loading) {
          const known = [];
          if (f.order && f.order.id) known.push(chipHtml({ id: 'order', icon: 'order', text: f.order.id, mono: 1, copy: f.order.id, aria: 'Order ' + f.order.id + ', press to copy the number' }, true));
          if (f.buyer && f.buyer.name) known.push(chipHtml({ id: 'buyer', icon: 'buyer', text: f.buyer.name, aria: 'Buyer ' + f.buyer.name }, false));
          h = known.join('') + '<span class="owfSk owSk" aria-hidden="true"></span><span class="owfSk owSk" aria-hidden="true"></span><span class="owfSk owSk short" aria-hidden="true"></span>';
          if (f.order && f.order.id) s.chips.set('order', { id: 'order', pop: { title: 'Order', icon: 'order', rows: [row('Order number', f.order.id, { mono: 1, copy: f.order.id, copyLabel: 'Copy the order number' })] } });
        } else {
          h = barHtml(chips, f);
        }
        host.querySelector('.owfBar').innerHTML = h;
        host.querySelector('.owfBar').after(s.live);
      }
      if (s.listSig !== list) {
        s.listSig = list;
        const d = host.querySelector('.owfDetails'); d.id = 'owfDetails';
        d.innerHTML = '<div class="owfList">' + (f.cells || []).map(cellHtml).join('') + '</div>';
      }
      host.classList.toggle('loading', !!f.loading);
      const more = host.querySelector('.owfMore');
      if (more) setDetails(host, s.details && !f.loading, false); else { const d = host.querySelector('.owfDetails'); if (d) d.hidden = true; }
      if (reopen && s.chips.has(reopen) && host.querySelector(`[data-owf-chip="${reopen}"]`)) openCard(host, reopen, by, true);
      if (keepFocus) { const n = host.querySelector(keepFocus); if (n) { host.querySelectorAll('[data-owf-item]').forEach(x => { x.tabIndex = x === n ? 0 : -1; }); try { n.focus({ preventScroll: true }); } catch (_) {} } }
    } finally { s.rendering = false; }
  }

  Object.assign(API, {
    paint, whenFriendly, shipInfo, ago, dayDiff, chipsOf, cellHtml, popHtml,
    close: host => host && closeCard(host),
    /** The window's Esc: a card that is open is put away (true: that was the Esc); the window itself stays. */
    escape: rootEl => {
      const from = rootEl || doc; let hit = false;
      for (const h of from.querySelectorAll('.owMeta')) { const s = states.get(h); if (s && (s.open || Date.now() - s.escAt < 120)) { if (s.open) closeCard(h, true); hit = true; } }
      return hit;
    },
    /** The Details drop-down shut again, at once (a window opened afresh starts calm). */
    collapse: host => { if (host && states.has(host)) { closeCard(host, false, true); setDetails(host, false, false); } },
    details: host => !!(host && states.has(host) && states.get(host).details),
    open: (host, id) => !!(host && openCard(host, id, 'pin')),
    toggleDetails: (host, on) => { if (host) setDetails(host, on == null ? !S_OF(host).details : on, false); },
    state: host => { const s = states.get(host); return s ? { open: s.open, by: s.by, details: s.details } : null; }
  });
})(typeof window !== 'undefined' ? window : globalThis);
