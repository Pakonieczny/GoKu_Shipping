/* Charm Nest · the Review tab follows the cloud (Paul, 5 Oct 2026: "everything correctly, and in real time reflected in the
   review tab so that any action taken from this view is immediately updated everywhere in the application and in the cloud in
   real time. If there's another user looking at the review tab while this is happening, then there should be a stamp animation
   shown and the animated order flying to the Completed tab").

   How the page learned of a completion, a print or a reopen made elsewhere before this: it did not, until the next Etsy order
   check (Orders.loadMaps reads the custom records only from the maps load: a page load, a cloud reconnect and that check, at
   least ten minutes apart) and the order window's own timeline feed, which carries events but not the records the cards are
   drawn from. Now, while the Review tab is on screen (the browser tab visible, Review the mode shown):

     · every EVERY (2 s) one small read, charmNestLibrary customGet { since }: the records written after the last answer's
       server time less a margin (one query on updatedAtMs; nothing written; { records: {}, at } and no document read when
       nothing changed, Firestore bills one read for an empty query). Not read while the tab is hidden or another tab is on screen (1 800 calls an hour while it is watched,
       none otherwise); a failed read backs off (x2 up to 30 s) and says nothing; no Etsy call, ever;
     · a write made on this page (api: customPut / customReopen) reads at once and again 1.5 s later; the order window's
       timeline feed, when it brings a custom seal or reopen this page has not seen, asks for one read (nudge, not a poller);
     · a tab shown after another is brought up to date by one read (at least 5 s after the last), so the Orders tab, the
       Library's issues and the rail agree when they are looked at.

   What a read brings joins the records the page holds (Orders.takeCustom: only what is newer, nothing dropped, seals kept) and
   everything that follows a press follows it (CustomPrint.settle: the lines read again, Review's lists and counts and filter
   chips, the Orders tab, the rail and header, the order window's bar and pieces, the Library, the run). When the changed card
   is on screen and the change is new, the person looking sees what the person who pressed saw (CustomPrint.remote): the stamp
   on the card, then the card flies to Completed (or back to Open) with a note naming who did it. Never for the person who
   pressed (their press holds its lines: CustomPrint.touching), never with the tab hidden, never over an open window, never
   for a change older than 20 s, never for the first read after an absence or a failure (the list is brought up to date with
   nothing moving: Review.view().quiet), and never for more than three cards at once. */
(function (root) {
  "use strict";
  const doc = root.document;
  const EVERY = 2000, GAP = 600, SECOND = 1500, OVERLAP = 15000, FAIL_MAX = 30000, FRESH = 20000, AWAY = 10000, OTHER_TAB = 5000, MAX_LOUD = 3, PAGES = 5;
  const st = { timer: 0, second: 0, busy: false, again: false, hint: false, quietNext: true, fails: 0, cursor: 0, lastOk: 0, lastAny: 0, start: 0, end: 0, polls: 0, applied: 0, dead: false, loud: 0, error: "" };
  const modeOf = () => { try { return S.mode; } catch (_) { return ""; } };
  const cloudOk = () => { try { return !!(S.cloud && S.cloud.ok); } catch (_) { return false; } };
  const onReview = () => modeOf() === "review" && !doc.hidden;
  const live = () => !st.dead && onReview() && cloudOk();
  const TABS = new Set(["orders", "nest", "library"]);

  /** Where the page's records stand: the server's clock at the whole list's read (Orders.loadMaps), taken once. */
  function seed() {
    if (st.cursor) return true;
    const b = root.Orders && root.Orders.customBase && root.Orders.customBase();
    if (!b || !(b.at > 0)) return false;
    st.cursor = b.at; st.lastOk = Math.max(st.lastOk, b.seen || 0);
    return true;
  }
  function arm(ms) { clearTimeout(st.timer); st.timer = setTimeout(tick, Math.max(0, ms)); }
  function schedule() {
    if (st.timer || st.busy || !live()) return;
    const every = Math.min(FAIL_MAX, EVERY * 2 ** Math.min(st.fails, 4));
    arm(Math.max(st.start + every, st.end + GAP) - Date.now());
  }
  function tick() {
    st.timer = 0;
    const hint = st.hint; st.hint = false;   // (a read asked for by the order window's timeline is made wherever the page is)
    if (!live() && !hint) return;
    if (!seed()) { if (live()) arm(5000); return; }   // (the maps' list is not read yet: the feed begins once it is)
    poll().catch(() => {});
  }

  /** What a record's change is to a card: it was completed (a seal), a seal was added to a completed one, or it was reopened. */
  function classify(was, rec) {
    const L = root.Seal && root.Seal.list; if (!L) return null;
    const state = r => (!r ? "none" : r.state === "open" ? "open" : "done");
    const a = state(was), b = state(rec);
    if (b === "done") {
      const now = L(rec), before = a === "done" ? L(was) : [];
      if (a !== "done") { const seal = now[now.length - 1] || null; return { kind: "complete", seal, who: (seal && seal.by) || rec.completedBy || rec.lastPrintedBy || "" }; }
      if (now.length > before.length) { const seal = now[now.length - 1]; return { kind: "again", seal, who: (seal && seal.by) || "" }; }
      return null;
    }
    if (b === "open" && a === "done") return { kind: "reopen", seal: null, who: rec.reopenedBy || "" };
    return null;
  }
  /** The cards the changes belong to, as they stand now (before the records join): what leaves, with the seal that left it. */
  function describe(changes, loud, serverAt) {
    const R = root.Review, out = new Map();
    for (const c of changes) {
      const kind = classify(c.was, c.rec); if (!kind) continue;
      const row = root.B && B.orders && B.orders.byKey && B.orders.byKey.get(c.key);
      if (!row || row.state === "gone" || !row.order) continue;
      let card; try { card = R.cardKey(row); } catch (_) { continue; }
      const id = kind.kind + "|" + card;
      let ev = out.get(id);
      if (!ev) {
        let it = null;
        if (kind.kind === "complete") { try { it = R.customItemFor(c.key, row) || R.actFor(c.key) || null; } catch (_) { it = null; } }
        ev = { it, rows: [], rec: c.rec, kind: kind.kind, seal: kind.seal, who: kind.who, loud: false, at: 0 };
        out.set(id, ev);
      }
      ev.rows.push(row);
      if (!ev.seal || (kind.seal && +kind.seal.at > +ev.seal.at)) { ev.seal = kind.seal; ev.who = kind.who; ev.rec = c.rec; }
      ev.at = Math.max(ev.at, +c.rec.updatedAtMs || 0);
    }
    const list = [...out.values()];
    for (const ev of list) ev.loud = !!loud && serverAt - ev.at <= FRESH;
    return list;
  }
  /** Everything that follows a press follows a change read from the cloud. */
  function refresh() {
    const P = root.CustomPrint;
    if (P && P.settle) P.settle();
    else { try { Orders.interpretAll(); Orders.render(); } catch (_) {} try { if (root.OrderWin && OrderWin.isOpen()) OrderWin.paint(); } catch (_) {} }
    try { if (modeOf() === "library" && root.LaserReview && LaserReview.changed) LaserReview.changed(); } catch (_) {}
  }
  function apply(records, began, quiet, serverAt) {
    const P = root.CustomPrint, R = root.Review;
    const mine = {}; let n = 0;
    for (const [k, v] of Object.entries(records)) if (!(P && P.touching && P.touching(k))) { mine[k] = v; n++; }
    if (!n) return 0;
    // (seen only on screen, over no window, and when the page can move things)
    const loud = !quiet && onReview() && !doc.querySelector("dialog[open]") && !!root.Motion && !!P && !!P.remote;
    let events = [];
    const changes = Orders.takeCustom(mine, began, loud ? ch => { events = describe(ch, true, serverAt); } : null);
    if (!changes.length) return 0;
    if (events.filter(e => e.loud).length > MAX_LOUD) for (const e of events) e.loud = false;
    // nothing to be seen: the lists are brought up to what the cloud says in one still draw
    if (!events.some(e => e.loud) && R && R.view) R.view().quiet = true;
    for (const ev of events) { try { P.remote(ev); } catch (e) { console.warn("Review feed", e); } if (ev.loud) st.loud++; }
    st.applied += changes.length;
    refresh();
    return changes.length;
  }

  /** One read of what changed, applied. o.quiet: nothing is to be seen moving (a tab shown, a read after an absence). */
  async function poll(o) {
    o = o || {};
    if (st.dead) return false;
    if (st.busy) { st.again = true; if (o.quiet) st.quietNext = true; return false; }
    if (!cloudOk() || typeof root.api !== "function" || !root.Orders || !root.Orders.takeCustom || !seed()) return false;
    st.busy = true; st.start = Date.now(); st.polls++;
    const away = st.lastOk && st.start - st.lastOk > AWAY, quiet = !!o.quiet || st.quietNext || st.fails > 0 || !!away;
    st.quietNext = false;
    try {
      let since = Math.max(0, st.cursor - OVERLAP), at = 0, truncated = false;
      const all = {};
      for (let page = 0; ; page++) {
        const r = await root.api("charmNestLibrary", { op: "customGet", since }, { quiet: true });
        if (!r || typeof r.at !== "number" || !r.records || typeof r.records !== "object") throw Object.assign(new Error("the cloud does not answer the changes feed"), { unsupported: true });
        Object.assign(all, r.records); at = r.at;
        if (!r.more) break;
        if (page + 1 >= PAGES) { truncated = true; break; }
        since = r.last > since ? r.last : since + 1;
      }
      st.fails = 0; st.error = ""; st.lastOk = Date.now(); st.cursor = truncated ? since + OVERLAP : at;
      apply(all, st.start, quiet || !onReview(), at);
      return true;
    } catch (e) {
      st.error = String((e && e.message) || e || "failed");
      // a cloud that does not know the feed answers with the whole list: it is not asked again from this page (a reload is)
      if (e && e.unsupported) { st.dead = true; console.warn("Review feed", st.error); }
      else st.fails++;
      return false;
    } finally {
      st.busy = false; st.end = Date.now();
      if (st.again) { st.again = false; arm(Math.max(0, st.start + GAP - Date.now())); } else schedule();
    }
  }

  /** Something was written from here, or the order window's timeline saw a change: read now, and again 1.5 s on. */
  function nudge(o) {
    if (st.dead || !cloudOk()) return;
    const hint = !!(o && o.hint);
    if (!hint && !onReview()) return;   // (a press from another tab: that tab is brought up to date when it is shown)
    if (hint) st.hint = true;
    if (st.busy) st.again = true; else arm(Math.max(0, st.start + GAP - Date.now()));
    clearTimeout(st.second);
    st.second = setTimeout(() => { st.second = 0; if (!st.dead && cloudOk() && (hint || onReview())) { if (hint) st.hint = true; if (st.busy) st.again = true; else arm(0); } }, SECOND);
  }
  /** A tab was shown (Views.onShow). */
  function shown(mode) {
    clearTimeout(st.timer); st.timer = 0; clearTimeout(st.second); st.second = 0;
    if (st.dead || !cloudOk()) return;
    st.quietNext = true;   // (what changed while another tab was on screen is not replayed)
    if (mode === "review") { if (doc.hidden) return; if (st.busy) st.again = true; else arm(0); return; }
    if (TABS.has(mode) && !doc.hidden && Date.now() - Math.max(st.lastAny, st.end) > OTHER_TAB) { st.lastAny = Date.now(); if (st.busy) st.again = true; else poll({ quiet: true }).catch(() => {}); }
  }
  doc.addEventListener("visibilitychange", () => {
    if (doc.hidden) { clearTimeout(st.timer); st.timer = 0; clearTimeout(st.second); st.second = 0; return; }   // (nothing is read while the tab is hidden)
    st.quietNext = true;
    if (live()) { if (st.busy) st.again = true; else arm(0); }
  });
  const back = () => { st.fails = 0; st.quietNext = true; if (live() && !st.busy) arm(500); };
  root.addEventListener("online", back); root.addEventListener("cn-cloud-back", back);

  root.ReviewLive = { nudge, shown, poll, state: () => ({ polls: st.polls, applied: st.applied, loud: st.loud, fails: st.fails, cursor: st.cursor, lastOk: st.lastOk, busy: st.busy, armed: !!st.timer, dead: st.dead, error: st.error, every: EVERY }) };
})(typeof window !== "undefined" ? window : globalThis);
