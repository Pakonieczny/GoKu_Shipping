/* charm-nest-hold-ui.js — the Hold button, its consent popup, and the glue for Hold and Release hold (window.HoldUI).
 *
 * Paul, 5 Oct 2026: "Add 1 extra button to both of these places in the UI. Orange 'Hold' button ... The user must be clearly
 * informed of the consequences of placing an order on hold with a popup and a final consent to continue button in the popup."
 *
 *   HoldUI.button(ctx)        -> the ONE orange Hold button (ctx = { rid, source: 'review' | 'orderWindow', label }). The Review cards (open and
 *                                completed) and the order window's piece row draw this same component, beside their other buttons.
 *                                Hold always acts on the WHOLE order, whichever piece's button was pressed.
 *   HoldUI.confirm(plan)      -> Promise<boolean>: the consent popup, shown BEFORE anything changes. "Continue" is the final consent, "Not now"
 *                                (or Esc, or a press outside) leaves everything as it was. A plan that cannot be held says why, with Close only.
 *   HoldUI.hold(rid, ctx)     -> the flow: plan (a small labelled spinner on the button) -> popup -> the name (the inline name bar) -> the run
 *                                (OrderHold.run) feeding the film (OrderHoldFx.playHold) -> back to Orders > On hold.
 *   HoldUI.release(rid, opts) -> the press of an existing "Release hold" button on a card in Orders > On hold: releasePlan -> OrderHold.release
 *                                feeding OrderHoldFx.playRelease -> back to Orders > On hold. One press: no popup, no question.
 *
 * Where it sits in the page: the Review card and the order window's piece row ask for a slot (HoldUI.slot(ctx), one span in their markup) and
 * the finished markup is given to HoldUI.fill(root), which swaps each slot for the real button. A held or cancelled order has no slot.
 *
 * Every call into another module is looked up when it is made and survives that module being absent (OrderHold: charm-nest-order-hold.js and
 * charm-nest-order-release.js; OrderHoldFx: charm-nest-hold-fx.js): without the engine there is no Hold button, and Release hold is what it
 * was. Nothing here waits for a film: the work runs at once and the film follows the steps as they arrive (Esc skips it there).
 *
 * Visible words: "pieces", never "lines". The one orange is the app's own warning tone (var(--warn), #a2591c where the page has none).
 */
(function (root) {
  'use strict';
  const doc = root.document;
  if (!doc) return;

  // ── small helpers ──────────────────────────────────────────────────────────────────────────────────────────────────
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const arr = v => (Array.isArray(v) ? v : []);
  const str = v => (v == null ? '' : String(v).trim());
  const num = v => (Number.isFinite(+v) ? +v : 0);
  const plural = (n, one, many) => (n === 1 ? one : many);
  const tryDo = (f, dflt) => { try { return f(); } catch (_) { return dflt; } };
  const wait = ms => new Promise(r => setTimeout(r, ms));
  const msgOf = e => str(e && e.message ? e.message : e) || 'something went wrong';
  /** A plain message on the page (the app's own toast; a bad one waits to be read). kind: 'bad' (default), 'ok', or 'plain'. */
  const say = (text, kind) => { try { if (typeof root.toast === 'function') root.toast(text, kind === 'plain' ? '' : (kind || 'bad'), kind === 'bad' || !kind ? 9000 : 4800); } catch (_) {} };
  const warn = (what, e) => { try { console.warn('hold:', what, msgOf(e)); } catch (_) {} };
  /** "A and B", "A, B and C" */
  const andList = list => (list.length < 2 ? list.join('') : list.slice(0, -1).join(', ') + ' and ' + list[list.length - 1]);
  const uniq = list => [...new Set(list.filter(Boolean))];

  // ── the engine and the film: each looked up when needed ───────────────────────────────────────────────────────────
  const engine = () => root.OrderHold || null;
  const film = () => root.OrderHoldFx || null;
  /** Hold needs the engine's plan and run; without them there is no button (nothing to press that could do anything). */
  const available = () => { const H = engine(); return !!(H && typeof H.plan === 'function' && typeof H.run === 'function'); };
  /** Release hold goes through the new engine only when it has release; otherwise the old press (Review.repool) stands. */
  const canRelease = () => { const H = engine(); return !!(H && typeof H.release === 'function'); };

  // ── what the page knows of an order: its pieces, whether it is held or cancelled ─────────────────────────────────────
  const rowsOf = rid => tryDo(() => (root.Orders && typeof root.Orders.rows === 'function' ? root.Orders.rows() : [])
    .filter(r => r && r.order && String(r.order.receiptId) === String(rid) && r.state !== 'gone'), []);
  /** Every piece of the order is on hold (the line's own `hold`, which is what Orders > On hold lists). */
  const isHeld = rid => { const rows = rowsOf(rid); return rows.length > 0 && rows.every(r => !!r.hold); };
  const isCancelled = rid => tryDo(() => !!(root.Cancelled && typeof root.Cancelled.has === 'function' && root.Cancelled.has(rid)), false);
  const isRunning = rid => tryDo(() => { const H = engine(), s = H && typeof H.status === 'function' ? H.status(rid) : null, r = H && typeof H.releaseStatus === 'function' ? H.releaseStatus(rid) : null; return !!((s && s.running === true) || (r && r.running === true)); }, false);
  /** Whether the order shows a Hold button: the engine is here, the order is in the pull, and it is not held, cancelled or being held. */
  function shown(rid) {
    rid = str(rid);
    return !!rid && available() && rowsOf(rid).length > 0 && !isHeld(rid) && !isCancelled(rid) && !isRunning(rid);
  }
  /** What a card's cache key learns of the button: it changes when the button comes or goes. */
  const stamp = rid => (shown(rid) ? 'hold' : '');

  // ── style: one small sheet, the app's own tokens (the one orange is its warning tone) ─────────────────────────────
  const CSS = [
    '.holdBtn,.holdDlg,.holdInline{--holdOrange:var(--warn,#a2591c)}',
    '.holdBtn{background:var(--holdOrange);border-color:var(--holdOrange);color:#fff}',
    '.holdBtn:hover{opacity:.9}.holdBtn[hidden]{display:none}.holdBtn[disabled]{cursor:default}',
    '.holdBtn:focus-visible{outline:2px solid var(--holdOrange);outline-offset:2px}',
    '.holdSlot{display:contents}',
    'dialog.holdDlg{width:min(500px,94vw)}',
    '.holdInline{position:absolute;inset:0;z-index:5;display:flex;align-items:center;justify-content:center;padding:16px;box-sizing:border-box;background:rgba(20,18,15,.52)}',
    '.holdInline>.dlg{width:min(500px,100%);background:var(--card);color:var(--ink);border-radius:15px;box-shadow:0 24px 80px rgba(0,0,0,.32);overflow:hidden}',
    '.holdDlg .dlgHead,.holdInline .dlgHead{flex-wrap:wrap}',
    '.holdDlg .dlgHead h3,.holdInline .dlgHead h3{white-space:normal;overflow:visible}',
    '.holdWho{margin:0 0 12px;font:12px/1.45 var(--sans);color:var(--ink45);overflow-wrap:anywhere}',
    '.holdWho:empty{display:none}',
    '.holdList{list-style:none;margin:0;padding:0;display:flex;flex-direction:column;gap:8px}',
    '.holdList li{position:relative;padding:9px 12px 9px 28px;border:1px solid var(--line);border-radius:10px;background:var(--card2);color:var(--ink);line-height:1.45;overflow-wrap:anywhere}',
    '.holdList li::before{content:"";position:absolute;left:12px;top:15px;width:6px;height:6px;border-radius:50%;background:var(--holdOrange)}',
    '.holdList li.quiet{color:var(--ink70)}.holdList li.quiet::before{background:var(--ink25)}',
    '.holdWhy{margin:0;padding:10px 12px;border:1px solid var(--line);border-radius:10px;background:var(--card2);color:var(--ink);line-height:1.5;overflow-wrap:anywhere}',
    '.holdDlg .dlgFoot .btn,.holdInline .dlgFoot .btn{min-height:34px}',
    '@media (max-width:480px){.holdDlg .dlgFoot,.holdInline .dlgFoot{flex-wrap:wrap}.holdDlg .dlgFoot .btn,.holdInline .dlgFoot .btn{flex:1 1 auto}}'
  ].join('\n');
  function ensureCss() {
    if (doc.getElementById('holdUiCss') || !doc.head) return;
    const s = doc.createElement('style'); s.id = 'holdUiCss'; s.textContent = CSS; doc.head.appendChild(s);
  }

  // ── the button ─────────────────────────────────────────────────────────────────────────────────────────────────────
  /** In flight, by order: { phase: 'plan' | 'ask' | 'run' }. One press at a time per order. */
  const flows = new Map();
  const buttons = rid => [...doc.querySelectorAll('[data-hold-btn]')].filter(b => !rid || b.dataset.rid === String(rid));

  /** The button as the flow stands: while the plan is read it says so, with a small spinner; while the popup or the run has it, it rests. */
  function paintBtn(b) {
    const f = flows.get(b.dataset.rid), checking = !!f && f.phase === 'plan', state = checking ? 'checking' : 'idle';
    b.disabled = !!f;
    if (b.dataset.state === state) return;
    b.dataset.state = state;
    if (checking) {
      b.style.minWidth = Math.ceil(b.getBoundingClientRect().width || 0) + 'px';
      b.innerHTML = '<span class="spin" aria-hidden="true"></span>Checking sheets…';
      b.setAttribute('aria-busy', 'true');
    } else {
      b.style.minWidth = ''; b.textContent = 'Hold'; b.removeAttribute('aria-busy');
    }
  }
  /** Every button of an order follows the flow (a button redrawn since the press carries it on); one for an order that is no longer holdable goes. */
  function sync(rid) {
    for (const b of buttons(rid)) {
      if (!b.isConnected) continue;
      if (!flows.has(b.dataset.rid) && !shown(b.dataset.rid)) { b.remove(); continue; }
      paintBtn(b);
    }
  }

  /** HoldUI.button({ rid, source, label }) -> the one orange Hold button. */
  function button(ctx) {
    ctx = ctx || {};
    ensureCss();
    const rid = str(ctx.rid), src = ctx.source === 'orderWindow' ? 'orderWindow' : 'review';
    const b = doc.createElement('button');
    b.type = 'button';
    b.className = 'btn ' + (src === 'orderWindow' ? 'xs' : 'sm') + ' holdBtn';
    b.dataset.holdBtn = ''; b.dataset.rid = rid; b.dataset.src = src; b.dataset.state = 'idle';
    b.textContent = 'Hold';
    b.setAttribute('aria-label', 'Hold order ' + rid + (str(ctx.label) ? ' · ' + str(ctx.label) : ''));
    b.title = 'Put the whole order on hold: all its pieces come off the sheets. You are shown what changes first.';
    b.onclick = e => { e.preventDefault(); e.stopPropagation(); hold(rid, Object.assign({}, ctx, { source: src, button: b })); };
    if (!shown(rid) && !flows.has(rid)) b.hidden = true;
    paintBtn(b);
    return b;
  }
  /** One span in a card's markup where its Hold button goes ("" for an order that has none): the card is drawn as text, and fill() makes the button. */
  function slot(ctx) {
    ctx = ctx || {};
    const rid = str(ctx.rid);
    if (!rid || !shown(rid)) return '';
    return `<span class="holdSlot" data-hold-slot data-rid="${esc(rid)}" data-src="${esc(ctx.source === 'orderWindow' ? 'orderWindow' : 'review')}" data-label="${esc(str(ctx.label))}"></span>`;
  }
  /** Each slot under `host` becomes the real button. */
  function fill(host) {
    const where = host && host.querySelectorAll ? host : doc;
    for (const s of [...where.querySelectorAll('[data-hold-slot]')]) {
      const b = button({ rid: s.dataset.rid, source: s.dataset.src, label: s.dataset.label });
      s.replaceWith(b);
    }
  }

  // ── the consent popup ──────────────────────────────────────────────────────────────────────────────────────────────
  const shipText = v => {
    if (v == null || v === '') return '';
    if (typeof v === 'string' && !/^\d+(\.\d+)?$/.test(v.trim())) return v.trim();
    const n = +v, d = new Date(n < 1e12 ? n * 1000 : n);
    if (!(n > 0) || isNaN(d.getTime())) return '';
    const text = tryDo(() => d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'America/Toronto' }), '') || tryDo(() => d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }), '');
    return text ? 'ship by ' + text : '';
  };

  /**
   * The plan as plain sentences: { what: [...], moves: [...], after: [...] } (what happens to this order, what moves in, and what stays true).
   * The plan's own sentences (plan.effects, "ready to show") come first when it has them; otherwise they are made from its pieces, sheets and
   * fills. QR labels, sheets already cut, the wait in On hold and "nothing is deleted" are always said, once each.
   */
  function describe(plan) {
    plan = plan && typeof plan === 'object' ? plan : {};
    const pieces = arr(plan.pieces), sheets = arr(plan.sheets), fills = arr(plan.fills), stays = arr(plan.stays);
    const given = arr(plan.effects).map(str).filter(Boolean);
    const out = { what: [], moves: [], after: [] };
    // (a sentence about QR labels, sheets already cut, the wait in On hold or "nothing is deleted" is a quiet one: it is what stays true, not the change)
    const quiet = t => /\bQR\b|release hold|nothing is deleted|already cut/i.test(t);
    if (given.length) { out.what = given.filter(t => !quiet(t)); out.after = given.filter(quiet); }
    else {
      // what happens to this order
      const off = sheets.filter(s => num(s.removes) > 0);
      const n = off.reduce((a, s) => a + num(s.removes), 0) || pieces.filter(p => p.state === 'onSheet').length;
      const names = uniq((off.length ? off.map(s => str(s.label || s.sheetLabel)) : uniq(pieces.filter(p => p.state === 'onSheet').map(p => str(p.sheetLabel)))));
      if (n > 0) out.what.push(`${n} ${plural(n, 'piece comes', 'pieces come')} off ${names.length ? andList(names) : 'its sheets'}.`);
      else out.what.push('None of its pieces is on a sheet now, so nothing comes off a sheet.');
      // what moves in
      const spots = fills.reduce((a, f) => a + num(f.spots), 0);
      const none = fills.filter(f => f.source === 'none').reduce((a, f) => a + num(f.spots), 0);
      const waiting = fills.filter(f => f.source === 'waiting').reduce((a, f) => a + num(f.orders), 0);
      const from = new Map();
      for (const f of fills) if (f.source === 'newerSheet') { const k = str(f.fromSheetLabel) || 'a newer sheet'; from.set(k, (from.get(k) || 0) + (num(f.orders) || 1)); }
      const parts = [];
      if (waiting) parts.push(`${waiting} waiting ${plural(waiting, 'order', 'orders')}`);
      for (const [k, v] of from) parts.push(`${v} ${plural(v, 'order', 'orders')} from ${k}`);
      const movers = waiting + [...from.values()].reduce((a, v) => a + v, 0), filled = Math.max(0, spots - none);
      if (parts.length) out.moves.push(`${andList(parts)} ${plural(movers, 'fills', 'fill')} the ${filled || spots} empty ${plural(filled || spots, 'spot', 'spots')}.`);
      if (none > 0) out.moves.push(`${none} empty ${plural(none, 'spot stays', 'spots stay')} open until an order fits.`);
    }
    const said = given.join(' ');
    const qrN = sheets.filter(s => s.qrRemade).length;
    if (qrN && !/\bQR\b/i.test(said)) out.after.push(qrN === 1 ? 'The QR label is made again on 1 sheet.' : `QR labels are made again on ${qrN} sheets.`);
    const cut = uniq(stays.map(s => str(s.sheetLabel)));
    if (stays.length && !/already cut|keep(s)? (their|its) pieces/i.test(said)) {
      out.after.push(`Sheets already cut keep their pieces${cut.length ? ': ' + andList(cut) : ''}.`);
    }
    if (!/release hold/i.test(said)) out.after.push('The order waits in On hold until someone presses Release hold.');
    if (!/nothing is deleted/i.test(said)) out.after.push('Nothing is deleted.');
    return out;
  }

  const isModal = d => tryDo(() => d.matches(':modal'), true);
  /** A window open over the page, other than ours (the sheet window under an order window, say): the popup is then drawn on its own surface, never over it. */
  const topModal = () => { const open = [...doc.querySelectorAll('dialog[open]')].filter(d => !d.hasAttribute('data-hold-own') && isModal(d)); return open[open.length - 1] || null; };
  let seq = 0, current = null;

  /** HoldUI.confirm(plan) -> Promise<boolean>: true only for Continue. */
  function confirm(plan) {
    ensureCss();
    plan = plan && typeof plan === 'object' ? plan : {};
    if (current) current.done(false);
    return new Promise(resolve => {
      const rid = str(plan.rid), blocked = plan.canHold === false, id = 'holdT' + (++seq);
      const m = describe(plan);
      const who = [str(plan.label) !== rid ? str(plan.label) : '', str(plan.customer), shipText(plan.shipBy)].filter(Boolean).join(' · ');
      const title = blocked ? "This order can't be put on hold yet" : (rid ? `Put order ${esc(rid)} on hold?` : 'Put this order on hold?');
      const item = (t, quiet) => `<li${quiet ? ' class="quiet"' : ''}>${esc(t)}</li>`;
      const body = blocked
        ? `<p class="holdWho">${esc(who)}</p><p class="holdWhy">${esc(str(plan.blockedWhy) || 'It cannot be put on hold right now. Nothing was changed.')}</p>`
        : `<p class="holdWho">${esc(who)}</p><ul class="holdList">${m.what.map(t => item(t)).join('')}${m.moves.map(t => item(t)).join('')}${m.after.map(t => item(t, true)).join('')}</ul>`;
      const foot = blocked
        ? '<button type="button" class="btn sage sm" data-k="no">Close</button>'
        : '<div class="left"><button type="button" class="btn ghost sm" data-k="no">Not now</button></div><button type="button" class="btn sm holdBtn" data-k="go">Continue</button>';
      const shell = `<div class="dlg"><div class="dlgHead"><h3 id="${id}">${title}</h3></div><div class="dlgBody" id="${id}d">${body}</div><div class="dlgFoot">${foot}</div></div>`;
      const host = topModal();
      let node, over = false, keyOff = null, closed = false;
      const from = doc.activeElement;
      if (host) {
        node = doc.createElement('div'); node.className = 'holdInline'; node.setAttribute('role', 'alertdialog'); node.setAttribute('aria-modal', 'true');
        node.setAttribute('aria-labelledby', id); node.setAttribute('aria-describedby', id + 'd'); node.dataset.holdOwn = '';
        node.innerHTML = shell; host.appendChild(node);
        const onKey = e => { if (e.key === 'Escape') { e.preventDefault(); e.stopPropagation(); finish(false); } };
        doc.addEventListener('keydown', onKey, true); keyOff = () => doc.removeEventListener('keydown', onKey, true);
        node.addEventListener('click', e => { if (e.target === node) finish(false); });
      } else {
        node = doc.createElement('dialog'); node.className = 'splitDlg holdDlg'; node.dataset.holdOwn = '';
        node.setAttribute('aria-labelledby', id); node.setAttribute('aria-describedby', id + 'd');
        node.innerHTML = shell; doc.body.appendChild(node);
        node.addEventListener('cancel', e => { e.preventDefault(); finish(false); });
        node.addEventListener('click', e => { if (e.target === node) finish(false); });
        over = true;
      }
      function finish(v) {
        if (closed) return;
        closed = true;
        if (current && current.node === node) current = null;
        if (keyOff) keyOff();
        if (over) { tryDo(() => { if (node.open) node.close(); }); }
        node.remove();
        tryDo(() => { if (from && from.isConnected && from.focus) from.focus({ preventScroll: true }); });
        resolve(!!v);
      }
      current = { node, done: finish };
      node.querySelector('[data-k=no]').onclick = () => finish(false);
      const go = node.querySelector('[data-k=go]'); if (go) go.onclick = () => finish(true);
      if (over) { try { node.showModal(); } catch (_) { node.setAttribute('open', ''); } }
      const first = node.querySelector('[data-k=no]'); tryDo(() => first.focus({ preventScroll: true }));
    });
  }

  // ── the hand-off from the order window ─────────────────────────────────────────────────────────────────────────────
  /**
   * The order window is a modal of its own: never a pop-up over it. It is closed (it goes back into what opened it), the popup is asked in
   * the page, and `back()` opens the same order again, on the same tab, when the answer is "not now" (nothing changed, nothing lost).
   */
  async function stepAside() {
    const W = root.OrderWin;
    if (!W || typeof W.isOpen !== 'function' || !tryDo(() => W.isOpen(), false)) return { back: async () => {} };
    const key = tryDo(() => W.key(), null), view = tryDo(() => W.view(), null);
    try { await W.close(); } catch (e) { warn('order window close', e); }
    // (a close already going is waited for; a window held open by a seal being stamped waits for it, and is never forced)
    for (let i = 0; i < 40 && tryDo(() => W.isOpen(), false); i++) await wait(50);
    const el = doc.getElementById('orderWin');
    for (let i = 0; i < 20 && el && el.open; i++) await wait(50);
    return {
      back: async () => {
        if (!key || typeof W.open !== 'function') return;
        try { const p = W.open(key, view ? { view } : {}); if (p && typeof p.then === 'function') await p; } catch (e) { warn('order window reopen', e); }
      }
    };
  }

  // ── the name, the film and the way home ────────────────────────────────────────────────────────────────────────────
  /** The person's name, the way Review's Complete Order asks it: the saved one, else the inline name bar (never prompt()). '' when it is put away. */
  async function askName(why) {
    const E = root.CNEmployee;
    let n = tryDo(() => str(E && typeof E.name === 'function' ? E.name() : ''), '');
    if (n) return n;
    if (E && typeof E.edit === 'function') { try { n = str(await E.edit({ why })); } catch (e) { warn('name', e); } }
    return n;
  }
  const savedName = () => tryDo(() => str(root.CNEmployee && typeof root.CNEmployee.name === 'function' ? root.CNEmployee.name() : ''), '');
  const modeNow = () => tryDo(() => (root.CN && root.CN.S && root.CN.S.mode) || '', '');

  const atOnHold = () => modeNow() === 'orders' && tryDo(() => root.Orders.view().pile === 'hold', false);
  /** Orders > On hold, as plainly as the page can show it (the way home when there is no film, or the film could not take the person there). */
  function showOnHold(rid) {
    tryDo(() => { if (root.CN && typeof root.CN.setMode === 'function') root.CN.setMode('orders'); });
    tryDo(() => { if (root.Orders && typeof root.Orders.showPile === 'function') root.Orders.showPile('hold', ''); });
    tryDo(() => { const card = [...doc.querySelectorAll('#ordersView [data-rid]')].find(n => n.dataset.rid === String(rid)); if (card && card.scrollIntoView) card.scrollIntoView({ block: 'center' }); });
  }
  /** Back to the work: the film's own way home when it has one, else the plain one. The person is never left in the Nest tab. */
  async function goHome(rid, from) {
    const F = film();
    if (F && typeof F.returnToOnHold === 'function') {
      try { const p = F.returnToOnHold(rid); if (p && typeof p.then === 'function') await p; }
      catch (e) { warn('return to On hold', e); }
    } else if (!atOnHold()) showOnHold(rid);   // (a Release hold pressed in Orders > On hold stays where it is)
    // (the film could not bring them back: the plain way does; one who started elsewhere and is still in the Nest tab goes back there)
    if (modeNow() === 'nest') { if (from && from !== 'nest' && !isHeld(rid)) tryDo(() => root.CN.setMode(from)); else showOnHold(rid); }
  }
  /** The pages that show an order are drawn again from what the engine did (cards, the order window's pieces). */
  function refreshViews() {
    tryDo(() => { if (root.Review && typeof root.Review.syncOrderItems === 'function') root.Review.syncOrderItems(); });
    tryDo(() => { if (root.Review && typeof root.Review.render === 'function') root.Review.render(); });
    tryDo(() => { if (root.Orders && typeof root.Orders.render === 'function') root.Orders.render(); });
    tryDo(() => { if (root.OrderWin && typeof root.OrderWin.isOpen === 'function' && root.OrderWin.isOpen()) root.OrderWin.paint(); });
  }

  /**
   * Feeds one run to its film and the page: onStep is the engine's, the film's push() follows it, and finish() says no more steps are coming.
   * Nothing waits for the film: the run is awaited alone; the film is waited for only to hand the person home when it has told its story,
   * with a limit so a film that never ends can never keep the person away from their work.
   */
  async function runWithFilm(kind, rid, plan, name, from, runFn) {
    const F = film(), start = kind === 'release' ? 'playRelease' : 'playHold';
    let fx = null;
    if (F && typeof F[start] === 'function') {
      try { fx = F[start](rid, { rid, kind, name, plan, from, label: plan && plan.label, customer: plan && plan.customer }); } catch (e) { warn(start, e); fx = null; }
    }
    const steps = [];
    const feed = step => {
      if (!step || typeof step !== 'object') return;
      steps.push(step);
      if (fx && typeof fx.push === 'function') { try { fx.push(step); } catch (e) { warn('film step', e); } }
    };
    let res;
    try { res = await runFn(feed); }
    catch (e) { res = { ok: false, error: msgOf(e) }; }
    res = res && typeof res === 'object' ? res : { ok: res !== false };
    const failed = res.ok === false || !!res.error;
    // (an engine that failed without saying so in its steps still tells the film how it ended)
    if (failed && !steps.some(s => s.type === 'error')) feed({ type: 'error', message: msgOf(res.error), rid });
    if (fx && typeof fx.finish === 'function') { try { fx.finish(); } catch (e) { warn('film finish', e); } }
    if (fx && fx.done && typeof fx.done.then === 'function') {
      await Promise.race([Promise.resolve(fx.done).catch(() => {}), wait(failed ? 4000 : 120000)]);
      if (failed && typeof fx.skip === 'function') tryDo(() => fx.skip());
    }
    return { res, failed, steps };
  }

  // ── Hold ───────────────────────────────────────────────────────────────────────────────────────────────────────────
  /** HoldUI.hold(rid, ctx) -> { ok, held?, cancelled?, error? }: plan -> popup -> name -> run (with its film) -> back to Orders > On hold. */
  async function hold(rid, ctx) {
    rid = str(rid); ctx = ctx || {};
    if (!rid) return { ok: false, error: 'no order' };
    if (flows.has(rid)) return { ok: false, busy: true };
    const H = engine();
    if (!H || typeof H.plan !== 'function' || typeof H.run !== 'function') {
      say('Hold is not ready on this page yet. Reload the page and try again. Nothing was changed.');
      return { ok: false, error: 'engine missing' };
    }
    const from = modeNow();
    flows.set(rid, { phase: 'plan' }); sync(rid);
    let aside = null;
    try {
      let plan = null;
      try { plan = await H.plan(rid, { source: ctx.source || 'review' }); }
      catch (e) { say(`Could not check order ${rid}: ${msgOf(e)}. Nothing was changed.`); return { ok: false, error: msgOf(e) }; }
      if (!plan || typeof plan !== 'object') { say(`Could not check order ${rid}. Nothing was changed.`); return { ok: false, error: 'no plan' }; }
      if (!plan.rid) plan = Object.assign({}, plan, { rid });
      flows.set(rid, { phase: 'ask' }); sync(rid);
      aside = await stepAside();
      const yes = await confirm(plan);
      if (!yes || plan.canHold === false) { await aside.back(); return { ok: false, cancelled: true }; }
      const name = await askName('Needed to put the order on hold. Kept with your work on this computer today.');
      if (!name) { say('Not put on hold: your name is needed for the record. Nothing was changed.', 'plain'); await aside.back(); return { ok: false, cancelled: true }; }
      flows.set(rid, { phase: 'run' }); sync(rid);
      const out = await runWithFilm('hold', rid, plan, name, from, feed => H.run(rid, { name, onStep: feed }));
      refreshViews();
      if (out.failed) say(`Order ${rid} was not fully put on hold: ${msgOf(out.res.error)}. What was done is kept, and the order can be held again.`);
      await goHome(rid, from);
      return out.failed ? { ok: false, error: msgOf(out.res.error), steps: out.steps } : Object.assign({ ok: true, held: true }, { steps: out.steps });
    } catch (e) {
      warn('hold', e);
      say(`Order ${rid} was not put on hold: ${msgOf(e)}.`);
      if (aside) await aside.back();
      return { ok: false, error: msgOf(e) };
    } finally {
      flows.delete(rid); sync(rid);
    }
  }

  // ── Release hold ───────────────────────────────────────────────────────────────────────────────────────────────────
  /**
   * HoldUI.release(rid, { button }) -> { ok, used }: the press of an existing "Release hold" button. `used: false` means the engine has no release
   * (the caller does what it always did). One press: no popup. The name is the saved one (today's Release hold asks for none).
   */
  async function release(rid, opts) {
    rid = str(rid); opts = opts || {};
    const H = engine();
    if (!rid || !canRelease()) return { ok: false, used: false };
    if (flows.has(rid)) return { ok: false, used: true, busy: true };
    const from = modeNow(), btn = opts.button && opts.button.nodeType === 1 ? opts.button : null;
    flows.set(rid, { phase: 'plan' });
    const was = btn ? btn.innerHTML : '';
    if (btn) { btn.disabled = true; btn.style.minWidth = Math.ceil(btn.getBoundingClientRect().width || 0) + 'px'; btn.innerHTML = '<span class="spin" aria-hidden="true"></span>Checking sheets…'; btn.setAttribute('aria-busy', 'true'); }
    const rest = () => { if (btn && btn.isConnected && btn.getAttribute('aria-busy')) { btn.disabled = false; btn.style.minWidth = ''; btn.innerHTML = was; btn.removeAttribute('aria-busy'); } };
    try {
      let plan = null;
      if (typeof H.releasePlan === 'function') {
        try { plan = await H.releasePlan(rid); }
        catch (e) { say(`Could not check order ${rid}: ${msgOf(e)}. It is still on hold.`); return { ok: false, used: true, error: msgOf(e) }; }
      }
      if (plan && (plan.canRelease === false || plan.canHold === false || plan.ok === false)) {
        say(str(plan.blockedWhy || plan.reason) || `Order ${rid} cannot be released right now. It is still on hold.`);
        return { ok: false, used: true, blocked: true };
      }
      rest();
      flows.set(rid, { phase: 'run' });
      const name = savedName();
      const out = await runWithFilm('release', rid, plan, name, from, feed => H.release(rid, { name, onStep: feed }));
      refreshViews();
      if (out.failed) say(`Order ${rid} was not fully released: ${msgOf(out.res.error)}. What was done is kept, and Release hold can be pressed again.`);
      await goHome(rid, from);
      return { ok: !out.failed, used: true, steps: out.steps, error: out.failed ? msgOf(out.res.error) : undefined };
    } catch (e) {
      warn('release', e);
      say(`Order ${rid} was not released: ${msgOf(e)}. It is still on hold.`);
      return { ok: false, used: true, error: msgOf(e) };
    } finally {
      rest(); flows.delete(rid);
    }
  }

  root.HoldUI = {
    button, slot, fill, confirm, hold, release, describe,
    shown, stamp, available, canRelease,
    busy: rid => flows.has(str(rid)),
    version: 1
  };
})(typeof window !== 'undefined' ? window : globalThis);
