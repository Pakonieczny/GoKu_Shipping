/* BARFRONT, the bar lines already waiting (Paul, 28 Oct 2026, order 4212020155, TINY BAR 7427 waiting for Approve with "VIII XVII XXVI" on its BACK: "Also make sure that all orders that contain a bar necklace
 * should have the engraving default to the front of the bar. Not just this listing, but any listing that has a bar necklace.").
 *
 * The bar rule (charm-nest-orders.js listingFaceOf / barNecklace) reads a bar necklace's line as FRONT when the order names no face, so a line read since then is made with a front job. A line whose jobs were made
 * BEFORE the rule kept its BACK jobs: the line's faces were reconciled only when a person changed the engraving (Engrave.applyEngraving), so it stayed on the back however often it was read again.
 *
 *   move(row)        asked for EVERY line each time it is read again (Orders.interpretAll: a pull, an arrival, a Resume, an answer, the first reading after a reload). A line the bar rule now reads as front
 *                    (spec.engraving source "profile", the bar rule's why, no person's override) whose jobs are all BACKS, none of them decided, follows the rule by itself, ONCE:
 *                      · each piece's front job is made (Engrave.ensureJobs: the setup asks for the front now) and takes its back's words exactly (text, line split, font word, the font, a person's word on them);
 *                        a back that had no settled words yet leaves its front to be read from the setup (free, no paid reading), as a new line's front is;
 *                      · the backs go as applyEngraving's reconcile drops them (CharmNestEngraveFaces.reconcile: undecided jobs of a face the setup no longer asks for are removed, their Review cards with them);
 *                      · the front is fitted again (Engrave.setReady, Engrave.fitJob: free, offline) and waits for Approve like any front;
 *                      · ONE line on the order's timeline and in the agent log: "moved to the front: bar necklaces engrave on the front by default". No Review card is raised for it, nothing is written to
 *                        the cloud here (the front's own ready flag goes with setReady, as for every front).
 *                    NOTHING moves while anything on the line was decided by a person or is under way: a job approved, written, skipped or "no engraving" (or a seal of one), row.engravingOverride (a person's
 *                    faces or words), an approval, a stamp or a back file under way or written, words being typed or a placement staged in its card, a piece taken by an edited back, a reading or fit running
 *                    (asked again at the next reading), or a piece on a sheet the laser has cut (replaceCutWhy). Every other line, and a line the rule does not read as front, is left exactly as it is.
 *                    → true when the line moved.
 *   afterRestore()   a reload brings the workspace back with each line's LAST reading (Session.restore): it is read again only at the next pass of Orders.interpretAll (the first order check, which waits up to the
 *                    poll interval, or a Resume). When a restored bar line still waits on its back, the lines are read again as soon as the maps and the library are in (the pass the first order check would
 *                    make: no extra read of the cloud), so the line moves before anyone approves its back. Nothing is read when no such line waits. → true when it read the lines.
 *
 * Reads the page's own objects (Engrave, Orders, Review, Pool, Session, CNTimeline, CharmNestEngraveFaces, CharmNestOrders, replaceCutWhy, agent, B) when asked, never at load. make(env) builds it over
 * another set of them (tests). Loads in the page (window.CNBarFront) and in node. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api; else root.CNBarFront = api;
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const BAR_RULE = /\bfrom the bar rule\b/;   // (the why every reading of the bar rule carries: "front only from the bar rule (a bar necklace is engraved on its front): ...")
  const MOVED = 'moved to the front: bar necklaces engrave on the front by default';
  const DONE = ['approved', 'written', 'skipped', 'none'];
  const UNREAD = ['classify', 'reclassify'];   // (a back still to be read, or read again because the order's words changed: its words are not settled)
  // what a back hands its front: the words and their font, as PIECES hands a piece's words on (charm-nest-bridge.js WORDS_OF), and a person's word on them; never its fit, move, size or turn (the front is fitted again)
  const KEEP = ['text', 'lines', 'lineInput', 'lineMode', 'source', 'quote', 'confidence', 'requests', 'questions', 'claudeJob', 'claudeReasoning', 'wordsSource', 'faceFontWord', 'fontKey', 'fontAsked', 'font', 'fontName',
    'lineGap', 'fontMapped', 'fontListing', 'fontFromNote', 'fontSubstituted', 'fontStyle', 'decision'];
  const dup = v => (v && typeof v === 'object' ? JSON.parse(JSON.stringify(v)) : v);
  const has = a => Array.isArray(a) && a.length > 0;
  const clip = (s, n) => { s = String(s == null ? '' : s).replace(/\s*\n\s*/g, ' ').trim(); return s.length > n ? s.slice(0, n - 1) + '…' : s; };

  function make(env) {
    const w = () => env || {};
    /** A back job nobody may move: decided, or something of a person's (or of an approval) under way on it. */
    function held(j, E) {
      if (!j || DONE.includes(j.state)) return true;
      if (j.approvedAt || j.approvalIntent || j.approvalPreparing || j._approvalTask || j._backTask || j.stamping || j.backSaving || j.backPending || j.editingBack || j.archived) return true;
      if (has(j.backs) || has(j.stagedBacks) || has(j.copyOverrides) || has(j.engravingSeals)) return true;
      try { if (E.isWorking && E.isWorking(j)) return true; } catch (_) { return true; }
      try { if (E.draftOf && E.draftOf(j.key)) return true; } catch (_) { return true; }
      try { const d = E.view && E.view().drafts; if (d && d[j.key]) return true; } catch (_) { /* no words card open */ }
      return false;
    }
    /** A piece on a sheet the laser has cut (or that only shows a saved record): the same rule every replacement asks. */
    function cut(id) {
      try { const P = w().Pool, sh = P && P.sheetOf ? P.sheetOf(id) : null, why = w().replaceCutWhy; return !!(sh && typeof why === 'function' && why(sh, new Set([id]))); } catch (_) { return true; }
    }
    /** The line's back jobs when it may move to the front now, else null. */
    function movable(row) {
      const W = w(), E = W.Engrave, F = W.CharmNestEngraveFaces, O = W.CharmNestOrders;
      const sp = row && row.spec, e = sp && sp.engraving;
      if (!E || !F || !e || typeof e !== 'object' || e.source !== 'profile' || e.mode !== 'front' || !BAR_RULE.test(String(e.why || ''))) return null;
      if (row.state === 'gone' || (O && O.FINISHED_LINE && O.FINISHED_LINE.has(row.state)) || row.engravingOverride || (E.neverEngraves && E.neverEngraves(row))) return null;
      let setup; try { setup = F.setupOf(row); } catch (_) { return null; }
      if (!setup || setup.mode !== 'front' || setup.edited) return null;
      const backs = E.jobsOf(row, { all: true });
      if (!backs.length || backs.some(E.isFront) || backs.some(j => held(j, E))) return null;
      if (backs.some(j => (j.copies || []).some(cut))) return null;
      return backs;
    }
    function move(row) {
      row = row && (row.parentRow || row); if (!row || !row.spec) return false;
      let backs; try { backs = movable(row); } catch (_) { backs = null; } if (!backs) return false;
      const W = w(), E = W.Engrave, F = W.CharmNestEngraveFaces, items = E.items(), at = Date.now();
      // 1 · each piece's back words, read before the plan takes away the backs nobody touched
      const bySlot = new Map(backs.map(b => [b.slot || null, b]));
      const settled = b => !!b && !UNREAD.includes(b.state) && (b.lines || []).some(l => String(l).trim());
      // 2 · the fronts the setup asks for (one per piece), each with its back's words
      const fronts = E.ensureJobs(row).filter(E.isFront), carried = new Set();
      if (!fronts.length) return false;
      for (const f of fronts) {
        const b = bySlot.get(f.slot || null) || (fronts.length === 1 && backs.length === 1 ? backs[0] : null);
        if (settled(b)) {
          for (const k of KEEP) if (b[k] !== undefined) f[k] = dup(b[k]);
          f.requests = Object.assign({}, f.requests, { side: 'front' });   // (the words are the front's now)
          Object.assign(f, { state: 'ready', reason: null, missing: null, fit: null, verify: null, view: null, mask: null, wantSize: null, movedFrom: b.key });
          f.row.engrave = { needed: true, state: 'ready', text: f.text, approved: false }; carried.add(f);
        } else f.row.engrave = { needed: false, state: 'classify' };   // (read from the setup below, as a new line's front)
      }
      // 3 · the backs go, as applyEngraving's reconcile drops them (undecided jobs of a face the setup no longer asks for); their Review cards with them
      const rec = F.reconcile(F.setupOf(row), E.jobsOf(row, { all: true }), null);
      for (const j of rec.drop) if (items.get(j.key) === j) items.delete(j.key);
      for (const j of backs.concat(rec.drop)) { try { W.Review && W.Review.remove('eng:' + j.key); } catch (_) { /* no card */ } }
      E.ensureJobs(row);   // (the line's summary is drawn from its fronts alone: Sides.linkParent)
      // 4 · one line on the order's timeline and in the agent log, nothing for Review
      const said = fronts.map(f => clip(f.text, 60)).filter(Boolean), sku = (row.spec && row.spec.designSku) || (row.line && row.line.sku) || '';
      try { W.CNTimeline && W.CNTimeline.line(row, 'engraveChanged', { id: `${row.key}.barfront`, once: true, at, text: clip(`Engraving ${MOVED}${said.length ? ' · ' + said.map(t => `“${t}”`).join(' · ') : ''}`, 200), data: { how: 'barFront', face: 'front', from: 'back', text: said.join(' · ') || null } }); } catch (_) { /* the timeline is a record, never a reason to stop */ }
      try { if (typeof W.agent === 'function') W.agent({ engrave: true }, 'ENGRAVE', `${row.order && row.order.receiptId} · ${sku}: engraving ${MOVED}${said.length ? ` (${said.map(t => `“${t}”`).join(', ')})` : ''}`); } catch (_) { /* a log line only */ }
      // 5 · the fronts are read (no words yet) or made ready and fitted again: free, offline
      Promise.resolve().then(async () => {
        if (fronts.some(f => f.state === 'classify' && items.get(f.key) === f)) await E.classify(row, { free: true });
        for (const f of fronts) {
          if (E.items() !== items || items.get(f.key) !== f || row.state === 'gone') continue;
          if (carried.has(f) && f.state === 'ready') await E.setReady(f);   // (a front read from the setup was made ready by the reading itself)
          if (f.state === 'ready' && E.canFit(f) && !E.isWorking(f)) await E.fitJob(f);
        }
      }).catch(err => { try { if (typeof W.agent === 'function') W.agent({ engrave: true }, 'warn', `${row.order && row.order.receiptId}: the front was not fitted yet — ${err && err.message}`); } catch (_) { /* a log line only */ } })
        .finally(() => { try { E.render(); if (W.Orders) W.Orders.render(); if (W.Session) W.Session.schedule(); } catch (_) { /* drawn with the next render */ } });
      return true;
    }
    async function afterRestore() {
      const W = w(), E = W.Engrave, O = W.CharmNestOrders, Or = W.Orders, B = W.B;
      if (!E || !O || typeof O.barNecklace !== 'function' || !Or || !B) return false;
      // (a restored line keeps the reading it was saved with: a bar line (its SKU or its title, barNecklace) whose jobs are all open backs is the one the bar rule may move)
      const open = r => { const js = E.jobsOf(r, { all: true }); return js.length > 0 && !js.some(E.isFront) && !js.some(j => held(j, E)); };
      const waiting = Or.rows().some(r => { try { return !!(r && r.spec && r.state !== 'gone' && !(O.FINISHED_LINE && O.FINISHED_LINE.has(r.state)) && !r.engravingOverride && O.barNecklace(r.line, r.spec) && open(r)); } catch (_) { return false; } });
      if (!waiting) return false;
      if (B.master && B.master.loading) { try { await B.master.loading; } catch (_) { return false; } }   // (the boot's own read of the library: never a second one)
      if (!(B.master && B.master.loadedAt) || !(B.maps && B.maps.loadedAt)) return false;   // (not in: the next order check reads the lines)
      Or.interpretAll(); Or.render(); E.render();
      return true;
    }
    return { move, afterRestore, movable: row => !!movable(row && (row.parentRow || row)), MOVED };
  }
  return Object.assign(make(root), { make, MOVED });
});
