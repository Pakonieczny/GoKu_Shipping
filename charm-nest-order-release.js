/* Release hold: an order goes back in the queue AHEAD of the orders coming in from Etsy and is placed at once
 * (Paul, 5 Oct 2026). ADDS to window.OrderHold (made here when the hold engine's file has not loaded yet: load order does not matter):
 *   OrderHold.releasePlan(rid)                       read only: where the order will go, in plain sentences
 *   OrderHold.release(rid, { name, onStep })         the release, one step at a time
 * Step vocabulary (every step has `at`, ms): start · queued · target · flight · placed · qr · released · done | error.
 * SKELETON: the API and the step order are final; the engine behind them is being filled in.
 */
(function () {
  'use strict';
  const G = typeof window !== 'undefined' ? window : globalThis;
  const OH = G.OrderHold = G.OrderHold || {};
  const rowsOf = rid => (G.Orders ? G.Orders.rows() : []).filter(r => String(r.order.receiptId) === String(rid) && r.state !== 'gone');
  const alsoPlan = plan => { const p = Promise.resolve(plan); return Object.assign(p, plan); };

  OH.releasePlan = function releasePlan(rid) {
    rid = String(rid);
    const rows = rowsOf(rid), held = rows.filter(r => r.hold);
    const plan = { rid, label: 'Order ' + rid, held: held.length > 0, canRelease: held.length > 0, blockedWhy: held.length ? null : 'This order is not on hold.', front: true, needsNewSheet: false, target: null, targets: [], pieces: [], effects: [] };
    return alsoPlan(plan);
  };

  OH.release = async function release(rid, opts) {
    rid = String(rid); opts = opts || {};
    const steps = [], emit = s => { s.at = Date.now(); steps.push(s); try { if (opts.onStep) opts.onStep(s); } catch (_) {} return s; };
    emit({ type: 'start', rid });
    const rows = rowsOf(rid).filter(r => r.hold);
    if (!rows.length || !G.Review) { emit({ type: 'error', message: 'This order is not on hold.', rid }); return { ok: false, released: false, rid, steps, error: 'This order is not on hold.' }; }
    const at = Date.now();
    for (const r of rows) r.frontAt = at;
    emit({ type: 'queued', rid, front: true });
    for (const r of rows) { r.heldAt = null; await G.Review.repool(r); }
    emit({ type: 'released', rid });
    emit({ type: 'done' });
    return { ok: true, released: true, rid, steps };
  };
})();
