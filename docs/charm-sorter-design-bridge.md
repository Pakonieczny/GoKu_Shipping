# Charm Sorter ⇄ Design Station bridge

Design and implementation document. Status: draft 4. Owner: production tooling.

Decisions taken on drafts 1–3: SKU labels are text under each charm · the SKU identifies the charm design only; the material comes from the listing's Metal/Colour option and every other feature (earring, necklace, charm only, chain, size) from the other options, all read from the Etsy API · engraving is always on the back, in Myriad Pro, only on solid material, never in a cut-out · text is set at the largest size the space allows and every placement is reviewed on screen by a human employee (any employee, name recorded) · anything not exactly representable is set aside for review with the problem and quick fixes shown · the back is produced by an actual, verified 180° flip about the vertical axis, with front-only engraving detail hidden and only cut geometry visible · one QR label per sheet, sheets travel in sets, and a set (one date, one run, all materials) is one folder with one numbering · an order with any unresolved line is held whole · the sorter marks orders design-complete; nothing is printed · the station's print button is hidden · during a run the sorter only *claims* orders on the station; the real lock is taken at commit.

---

## 1. Goal

The Charm Nesting Station ("the sorter", `charm-nest-1.html`) becomes the master of the Design Station (`design-1.html`, "the slave"). Without an operator touching the Design Station, the sorter:

1. pulls the open orders the Design Station sees, with each line's SKU, metal bench, quantity and the customer's instructions;
2. drives any Design Station action (select, filter, open an item, complete) and shows it happening live in both consoles;
3. finds each ordered SKU in a master Illustrator file, lifts that charm out with every layer, path, colour and dimension intact, and drops it into a pool of designs ready to nest;
4. for any line that needs back engraving, shows the charm's back (a 180° turn about its vertical axis), has Claude read the customer's request, sets the text in Myriad Pro in the best place and size on solid material, and keeps the result in a separate pool tied to the sheet the order is on;
5. when a set of sheets is finished, generates one QR label per sheet into the set folder and marks the orders design-complete on the Design Station;
6. keeps every sheet of a run together as a set, one folder and one set number across materials, so mixed orders travel as one.

Everything builds on what exists: the sorter's parser (`charm-nest-pdf.js`), silhouettes, parallel nesting, twin verification, per-sheet cloud folders and agent log; the Design Station's single metal classifier, attention flags, selection locks, completion ledger and archive; one shared Firestore project.

## 2. Vocabulary

| Term | Meaning |
| --- | --- |
| Order line | One Etsy transaction: order number, SKU, quantity, the chosen options, personalisation, buyer message |
| Line spec | The sorter's normalised reading of a line (5.2): design SKU, material, form, size, chain, engraving text, sources |
| Material | The metal the charm is cut from, from the listing's Metal/Colour option through the station's classifier: `gold`, `silver`, `rose`, `10k`, `14k` → sorter cards `gold`, `silver`, `rose`, `gold10k`, `gold14k` |
| Master file | One `.ai` holding every charm design, each with its SKU as text directly under it. A SKU is one design; material never changes the design |
| Master index | The SKU → charm lookup built from a master file |
| Pool | Extracted charm designs waiting to be nested, one entry per order line and copy, keyed deterministically |
| Back pool | The engraving design for each engraved copy, kept with the sheet that holds the front |
| Set | Everything produced by one run on one date: every sheet of every material, its labels, its back files, its manifest. Sheets in a set travel together |
| Run | One execution of the pipeline, with a persistent record that can be resumed |
| Claim | A light mark on the station that the sorter is working an order; a **lock** is the station's existing "being worked on at another station" state, taken only at commit |
| Bridge | The command and event channel between the two apps |

## 3. Architecture

```
┌───────────────────────────────────────────────┐        ┌────────────────────────────────────────┐
│ Charm Sorter · brites-charm-sorter.goldenspike │        │ Design Station · design-1.goldenspike  │
│                                                │        │ (embedded in the sorter's frame)       │
│  Orders ── Design Station ── Master ── Engrave │ postMessage, origin-checked, nonce           │
│  Nest ──── Library ──────── Charms             │ ◀────────────────────────────────────────▶ │
│                                                │        │  Bridge module: command runner,        │
│  DesignLink (client)   AgentLog (DS/ENGRAVE)   │        │  event emitter, remote banner          │
└───────────────┬────────────────────────────────┘        └───────────────────┬────────────────────┘
                │ Netlify functions + Firestore + Storage                     │ firebaseOrders (unchanged)
                ▼                                                             ▼
   charmNestLibrary (+pool, master, bridge, labels ops) · charmMaster-background · charmEngrave-background
   Charm_Master_Index · Charm_Pool · Charm_Pool_Back · Charm_Nest_Sets · Design_Bridge (log)
```

### 3.1 One live channel

Both apps run on the same PC, so the sorter embeds the Design Station in an `<iframe>` and drives it with `window.postMessage`. The two origins differ (`brites-charm-sorter.goldenspike.app` and `design-1.goldenspike.app`), so every message carries `source`, a session `nonce`, and both sides check `event.origin` against a fixed allow-list. This is the same handshake the Design Station already runs with `design-print-1.html`, hardened with the origin check. Firestore is used only to log the session (`Design_Bridge`) so a run can be audited and replayed; it is never in the control path.

Embedding needs one header. `design-1.goldenspike.app` is currently unframed; add to `netlify.toml`:

```toml
[[headers]]
  for = "/design-1.html"
  [headers.values]
    Content-Security-Policy = "frame-ancestors 'self' https://brites-charm-sorter.goldenspike.app"
```

### 3.2 Remote mode in the Design Station

While a session is open the station shows a gold banner: "Controlled by Charm Sorter · session 3f9a · 14 commands · last: select 3521337740 · Release". Every executed command is echoed into the station's own activity ticker, so a person at the bench reads the same story the sorter shows. Locks taken by remote selection are owned by the sorter's client id, so the other bench sees "being worked on at another station" as today.

## 4. The bridge protocol

### 4.1 Envelope

```json
{ "source": "brites-sorter", "nonce": "k3f9a2…", "id": 17, "type": "ui.select", "args": { "receiptIds": ["3521337740"] } }
{ "source": "brites-design", "nonce": "k3f9a2…", "id": 17, "type": "ack" }
{ "source": "brites-design", "nonce": "k3f9a2…", "id": 17, "type": "progress", "done": 1, "total": 1, "text": "Reading order 3521337740" }
{ "source": "brites-design", "nonce": "k3f9a2…", "id": 17, "type": "done", "result": { "selected": ["3521337740"] } }
{ "source": "brites-design", "nonce": "k3f9a2…", "id": 17, "type": "error", "error": "order is locked at another station" }
```

Every command gets exactly one `ack`, zero or more `progress`, and one `done` or `error`. Unsolicited events (`id: 0`) carry the station's state changes. Unknown types are ignored, never executed. A message whose `origin`, `source` or `nonce` does not match is dropped and counted.

### 4.2 Commands (sorter → Design Station)

| Command | Arguments | Effect in the station | `done.result` |
| --- | --- | --- | --- |
| `hello` | `sorterClientId, runId` | Opens the session, shows the banner, releases locks left by a previous sorter nonce | `{ bench, version, employee, counts, filters, selection, etsy: { signedIn, expiresAt }, storage }` |
| `ping` | — | heartbeat every 5 s; three misses mark the station down | `pong` |
| `claim` / `unclaim` | `{ receiptIds[] }` | gold dot on the station: "in a sorter run"; other benches keep notes and chat | `{ claimed[] }` |
| `orders.snapshot` | `{ metals?, onlyAttention?, hydrate: true }` | Every open order with hydrated lines; missing details fetched through the station's paced Etsy queue with progress; never loads images | `{ total, hydrated, orders: Order[] }` (4.4) |
| `orders.detail` | `{ receiptId }` | One order | `{ order }` |
| `orders.watch` | `{ on }` | Emit `orders.changed` after each station refresh | — |
| `ui.select` / `ui.deselect` | `{ receiptIds[] }` | `selectRow` / `deselectRow` on each row (takes or releases the realtime lock); per-line outcome, never all-or-nothing | `{ selected[], refused: [{ id, reason }] }` |
| `ui.filter` | subset of the station's `F` model | Sets chips and re-applies | `{ filters }` |
| `ui.openItem` | `{ transactionId }` | `openListingModal` | `{ item }` |
| `ui.closeModal` | — | closes any open dialog | — |
| `ui.scrollTo` | `{ receiptId }` | `revealOrder` | — |
| `notes.set` | `{ receiptId, text }` | staff note, as the modal does | — |
| `chat.post` | `{ receiptId, text }` | internal Brites message, sender "Charm Sorter" | — |
| `complete.preview` | `{ receiptIds[] }` | bucketing, QR preview modal, nothing written | `{ jobs[], skipped[], stale[], solid[], mixedTarget }` |
| `complete.commit` | `{ receiptIds[], labels: { setId, folder, files[] }, runId }` | Marks complete **without printing**: ledger, unlock, "DESIGNED :)" chat, archive; records where the labels were saved; idempotent per `runId` | `{ completed[], refused: [{ id, reason }] }` |
| `complete.undo` | — | `handleUndoComplete` | — |
| `release` | — | ends remote mode | — |

Guards enforced by the station whatever the driver: `complete.commit` requires a `complete.preview` of the same ids in this session and a `labels.files` list that is non-empty; it is refused while a grouping review or an Etsy sweep is in flight; commands from any other origin are dropped.

For release-policy-2 sets, the sorter rechecks every included sheet's colour rules, saved front files, current QR membership and approved, verified back files both before completion and after the station preview. Each eligible order receives the original `DESIGNED :)` staff message in its internal Brites chat. The station requires confirmation for every message before clearing the orders or archiving them. A failed delivery leaves completion retryable; `firebaseOrders` uses a transaction and a stable message document per order/set, so retries (including lost responses and reloads) do not duplicate messages. Held orders remain open until their other lines are ready. The original QR-print flow and ordinary chat retain their existing behavior. Regression check: `node tests/charm-nest/completion-messages.cjs`.

### 4.3 Events (Design Station → sorter, `id: 0`)

Back thumbnails recover failed Storage image loads through the read-only `charmNestLibrary.backPreview` operation. It validates the exact sheet, charm-copy ownership and approval revision, reads that record's saved PNG, and returns an inline image. Cached recovery is shared between views and invalidates on a new approval. Errors remain retryable; a missing or changed file is never replaced with another charm's image. This also avoids clipped broken-image alt text in the compact back shelf. Checks: `back-preview.cjs` and the saved-PNG/ownership/sandbox cases in `functions.cjs`.

`state`, `orders.changed`, `selection`, `filters`, `ui.modal` (`{ open: "listing", transactionId }`), `activity` (every ticker line), `lock.changed`, `complete.done`, `error`. The sorter writes `activity` into its agent log as kind `DS` so one log reads end to end.

### 4.4 Order shape

```json
{
  "receiptId": "3521337740", "shipBy": 1789200000, "updateTs": 1789100000, "buyer": { "name": "A. Smith", "country": "US" },
  "attention": true, "staffNote": "", "hasMessages": false, "lockedBy": null,
  "lines": [{
    "transactionId": "4412778001", "listingId": "1718…", "sku": "BR-CMP-01", "title": "Compass charm necklace",
    "quantity": 2, "metalKey": "gold", "metalLabel": "Gold",
    "personalization": ["ANNA 9.26.25"], "buyerMessage": "please engrave on the back", "staffNote": "", "messages": [],
    "variations": [{ "name": "Metal", "value": "14k Gold Filled" }, { "name": "Size", "value": "Small" }]
  }]
}
```

Built from what the station already has (`slimTx`, `decorateReceipt`, `txMetalResolved`, `txNeedsAttention`, the personalisation lift in `pullEtsyOrderDetails`). The sorter never re-classifies metal.

### 4.5 Design Station side: the `Bridge` module

Added to `design-1.html` as section 20, in the same single scope as everything else. It wraps existing functions; it does not duplicate logic.

```js
/* ═══ 20 · BRIDGE — remote control by the Charm Sorter ═══════════════════ */
const Bridge = (() => {
  const ALLOWED = new Set(["https://brites-charm-sorter.goldenspike.app", "http://localhost:8888"]);
  const S = { nonce: null, origin: null, master: null, count: 0, last: "" };
  const send = (msg) => { if (S.master) S.master.postMessage(Object.assign({ source: "brites-design", nonce: S.nonce }, msg), S.origin); };
  const emit = (type, payload) => send({ id: 0, type, ...payload });

  // every command: (args, progress) => result. Names mirror the buttons an operator would press.
  const CMDS = {
    async hello(a, ev) {
      S.nonce = ev.data.nonce; S.origin = ev.origin; S.master = ev.source;
      paintBanner(); return stateSnapshot();
    },
    async "orders.snapshot"(a, progress) {
      const list = allOpenReceipts.filter(r => !completedOrders.has(String(r.receipt_id)));
      const need = list.filter(r => !r._hydrated).map(r => String(r.receipt_id));
      let done = 0; const run = makeQueue(4);
      await Promise.all(need.map(rid => run(async () => {
        const ord = await pullEtsyOrderDetails(rid);           // paced Etsy queue, as the station uses it
        orderCache[rid] = ord?.transactions || [];
        const rec = list.find(r => String(r.receipt_id) === rid); if (rec) { rec.transactions = orderCache[rid]; decorateReceipt(rec); }
        progress(++done, need.length, `Reading order ${rid}`);
      })));
      return { orders: list.filter(r => !a.metals || METAL_ORDER.some(k => a.metals.includes(k) && r._metalCounts[k]))
                           .filter(r => !a.onlyAttention || r._attn).map(orderForBridge) };
    },
    async "ui.select"(a) { for (const rid of a.receiptIds) { const row = rowFor(rid); if (!row) throw new Error(`order ${rid} is not in the list`); if (isLockedElsewhere(rid)) throw new Error(`order ${rid} is locked at another station`); await selectRow(row); } return { selected: [...selectedOrders] }; },
    async "ui.deselect"(a) { for (const rid of a.receiptIds) { const row = rowFor(rid); if (row) deselectRow(row); } return { selected: [...selectedOrders] }; },
    async "ui.filter"(a) { Object.keys(a).forEach(k => { if (k === "metals") F.metals = new Set(a.metals); else if (k in F) F[k] = !!a[k]; }); applyMetalFilter(); refreshChrome(); return { filters: filtersForBridge() }; },
    async "ui.openItem"(a) { await openListingModal(a.transactionId); return { item: itemForBridge(itemsByTx.get(String(a.transactionId))) }; },
    async "ui.closeModal"() { $$("dialog[open]").forEach(closeDlg); return {}; },
    async "ui.scrollTo"(a) { revealOrder(String(a.receiptId)); return {}; },
    async "notes.set"(a) { await fetch(`${FN}/firebaseOrders`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ orderNumber: String(a.receiptId), staffNote: String(a.text || "") }) }); staffNoteIDs.add(String(a.receiptId)); return {}; },
    async "chat.post"(a) { await BritesChat.postAs(String(a.receiptId), String(a.text || ""), "Charm Sorter"); return {}; },
    async "complete.preview"(a) {
      selectedOrders.clear(); a.receiptIds.forEach(id => selectedOrders.add(String(id)));
      await handleCompleteClick();                                 // builds pendingJobs and opens the preview modal
      S.previewed = new Set(Object.values(pendingLists || {}).flat().map(String));
      return { jobs: pendingJobs || [], lists: pendingLists || {}, notes: lastPreviewNotes };
    },
    async "complete.commit"(a) {
      const ids = a.receiptIds.map(String);
      if (!S.previewed || ids.some(id => !S.previewed.has(id))) throw new Error("commit without a preview of the same orders");
      if (!a.labels || !Array.isArray(a.labels.files) || !a.labels.files.length) throw new Error("no saved labels");
      await commitCompletion(ids, { labels: a.labels });           // 4.6
      return { completed: ids };
    },
    async "complete.undo"() { await handleUndoComplete(); return {}; },
    async release() { endSession("released by the sorter"); return {}; }
  };

  async function onMessage(ev) {
    const d = ev.data;
    if (!d || d.source !== "brites-sorter" || !ALLOWED.has(ev.origin)) return;
    if (d.type !== "hello" && d.nonce !== S.nonce) { S.dropped = (S.dropped || 0) + 1; return; }
    const fn = CMDS[d.type]; if (!fn) return;
    send({ id: d.id, type: "ack" });
    S.count++; S.last = d.type; paintBanner();
    ticker(`Sorter: ${d.type}${d.args && d.args.receiptIds ? " " + d.args.receiptIds.join(", ") : ""}`);
    try {
      const result = await fn(d.args || {}, (done, total, text) => send({ id: d.id, type: "progress", done, total, text }), ev);
      send({ id: d.id, type: "done", result });
    } catch (e) { send({ id: d.id, type: "error", error: e.message || String(e) }); ticker(`Sorter command failed: ${d.type} — ${e.message}`); }
  }
  window.addEventListener("message", onMessage);

  // the station's own state changes are mirrored to the master as they happen
  const hooks = { selection: () => emit("selection", { selected: [...selectedOrders] }), orders: () => emit("orders.changed", { count: allOpenReceipts.length }), activity: (text) => emit("activity", { text, t: Date.now() }) };
  return { hooks, active: () => !!S.master, endSession };
})();
```

`refreshChrome`, `buildNewOrderList` and the activity ticker each gain one line calling the matching hook when `Bridge.active()`.

### 4.6 Completion without printing

`proceedToPrint` is split. Its second half becomes `commitCompletion(ids, { labels })`, called by the button after the print handshake (unchanged today) and by `complete.commit` after the sorter has saved the labels. The archived record gains `labels: { setId, folder, files[] }` so the History view can open the saved label sheet. The station's print button is hidden (`#qrPreviewPrintBtn` and the print route) once the sorter path is live; `design-print-1.html` stays deployed as a fallback that can be re-enabled from Settings.

```js
async function commitCompletion(ids, { labels = null } = {}) {
  const printedIDs = [...new Set(ids.map(String))];
  lastClearedReceipts = [...printedIDs];
  printedIDs.forEach(rid => { removePreviewBoxesForOrder(rid, { force: true }); $$(`#newOrderContainer .orderRow[data-receipt='${cssEsc(rid)}']`).forEach(r => r.remove()); delete orderCache[rid]; selectedOrders.delete(rid); completedOrders.add(rid); });
  allOpenReceipts = allOpenReceipts.filter(r => !printedIDs.includes(String(r.receipt_id)));
  currentReceipts = currentReceipts.filter(r => !printedIDs.includes(String(r.receipt_id)));
  await persistCompleted(printedIDs);
  await fetch(`${FN}/firebaseOrders`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ rtUnlockIds: printedIDs, clientId: RT.clientId }) });
  printedIDs.forEach(id => rtSelected.delete(id));
  persistSelection(); updateCounters(); applyMetalFilter(); refreshChrome();
  await markBritesDesigned(printedIDs);
  archiveOrders(printedIDs, { labels }).catch(e => console.warn("archive after completion failed", e));
}
```

### 4.7 Sorter side: `DesignLink`

```js
/* charm-nest-1.html · section 15 · DesignLink — the Design Station as a slave */
const DesignLink = (() => {
  const ORIGIN = "https://design-1.goldenspike.app";
  const S = { frame: null, nonce: null, id: 0, pending: new Map(), state: null, log: [] };
  function open(frameEl) {
    S.frame = frameEl; S.nonce = uid() + uid();
    window.addEventListener("message", onMessage);
    return call("hello", { sorterClientId: S.nonce });
  }
  function call(type, args = {}, { timeoutMs = 120000, onProgress } = {}) {
    const id = ++S.id;
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => { S.pending.delete(id); reject(new Error(`${type}: the Design Station did not answer in time`)); }, timeoutMs);
      S.pending.set(id, { resolve, reject, t, onProgress, type, sent: performance.now() });
      logLine("cmd", type, args);
      S.frame.contentWindow.postMessage({ source: "brites-sorter", nonce: S.nonce, id, type, args }, ORIGIN);
    });
  }
  function onMessage(ev) {
    if (ev.origin !== ORIGIN) return;
    const d = ev.data; if (!d || d.source !== "brites-design" || d.nonce !== S.nonce) return;
    if (d.id === 0) { onEvent(d); return; }
    const p = S.pending.get(d.id); if (!p) return;
    if (d.type === "progress") { p.onProgress && p.onProgress(d); agentLive(`DS · ${p.type}`, d.text, d.done, d.total); return; }
    if (d.type === "ack") return;
    clearTimeout(p.t); S.pending.delete(d.id);
    logLine("reply", p.type, d, performance.now() - p.sent);
    d.type === "done" ? p.resolve(d.result) : p.reject(new Error(d.error));
  }
  function onEvent(d) {
    if (d.type === "activity") agent({ bridge: true }, "DS", d.text);
    if (d.type === "selection") S.state = Object.assign({}, S.state, { selection: d.selected });
    if (d.type === "orders.changed") Orders.markStale();
    renderLiveStrip(d);
  }
  function logLine(dir, type, payload, ms) { const row = { t: Date.now(), dir, type, ms: ms == null ? null : Math.round(ms), payload: slimForLog(payload) }; S.log.push(row); api("charmNestLibrary", { op: "bridgeLog", session: S.nonce, row }).catch(() => {}); }
  return { open, call, state: () => S.state, log: S.log };
})();
```

The **Design Station** tab holds the frame at full height and, beside it, the console: session id, command count, dropped-message count, the last 200 log rows with round-trip times, and a **Take control / Release** switch. This is the live monitoring asked for: the station's real UI on the left reacting to each command, the narrative on the right.

## 5. Orders in the sorter

### 5.1 Pull

```js
const Orders = {
  rows: [], stale: false,
  async pull(run) {
    const r = await DesignLink.call("orders.snapshot", { hydrate: true }, { onProgress: p => agentLive("Pulling orders", p.text, p.done, p.total) });
    if (r.hydrated < r.total) throw new Error(`only ${r.hydrated} of ${r.total} orders could be read from Etsy — sign in at the Design Station and pull again`);
    this.rows = r.orders.flatMap(o => o.lines.map(l => ({ order: o, line: l, spec: null, state: "pulled", claimedBy: null, poolIds: [] })));
    for (const row of this.rows) row.spec = interpretLine(row.order, row.line);            // 5.2, deterministic
    await api("charmNestLibrary", { op: "runPut", run: Object.assign(run, { step: "pulled", lines: this.rows.map(lineRecord) }) });
    this.stale = false; renderOrders();
  }
};
```

`orders.snapshot` returns `{ total, hydrated, tokenExpiresAt, orders }`. A shortfall is a stop. The Orders tab lists lines grouped by material card: order number, SKU, form, size, quantity, ship-by, the `!` attention flag with the personalisation, buyer message and staff note shown inline, engraving state, pool state, and the line's `update_timestamp`.

### 5.2 Reading a line: the line spec

Everything about a line is available from the Etsy API and is read deterministically. Claude reads free text only (7.1); it never decides material, form or size.

| Field | Source | Rule |
| --- | --- | --- |
| `designSku` | transaction `sku` (fallback `Charm_Sku_Aliases/{listingId}`) | identifies the charm design in the master; nothing else |
| `material` | the station's `metalKeyForTx` (Metal/Colour option first, then other options, then title) | single source of truth; a line with no material or `SOLID_UNKNOWN` is held (5.4) |
| `form` | options named Style / Type / Product (values such as "Necklace", "Earrings", "Charm only") mapped through `Charm_Option_Map` | unmapped value → held |
| `size` | option named Size / Length when the SKU's master entry has `sizes[]` | size is not in the SKU; the master may carry one design per size (6.3) |
| `chain` | option named Chain / Length | informational; carried to the report |
| `quantity` | transaction | one copy each, identical |
| `personalization[]` | the station's lifted personalisation field | verbatim |
| `buyerMessage`, `staffNote`, `messages[]` | receipt, `Brites_Orders` staff note, last five Brites messages | inputs to 7.1; staff note overrides Etsy text |
| `updateTs` | receipt `update_timestamp` | re-validated before engraving and before commit (10.3) |

`Charm_Option_Map/{listingId or "*"}` maps an option name and value to a spec field and value; the map is learned once per new value in the **Needs mapping** tray and applied automatically afterwards. Nothing free-text ever sets a spec field.

### 5.3 Lines that are not charms

Chains, extenders, packaging and other SKUs with no design are listed in `Charm_Sku_NoDesign` (patterns and explicit SKUs) and skipped with a count in the log. A line in the Unmatched tray can be moved to that list with one click and never returns.

### 5.4 Holding an order

An order is committed only when every line is one of: nested and written; nested, written and engraving approved; on the no-design list. Any other line holds the whole order: it stays open on the station, its pieces are still cut if already nested (the sheet report marks them "order held"), and the Orders tab shows **Held: which line, why, and the fix**. Held orders are re-evaluated at every resume.

### 5.5 Claims, not locks

At pull the sorter writes a claim on the station (`rtLockIds` with `claimedBy: "sorter"`, shown on the station as a gold dot, not the red lock) so the other bench sees the orders are in a run but can still add notes and messages. The real lock (`ui.select`) is taken only in half B, immediately before `complete.preview`.

## 6. Master file and SKU lookup

### 6.1 The labelling rule

Each charm in a master file has its SKUs as a stacked list of text lines directly under it (revised on the real master, September 2026: the shop's SKUs are free text such as `T-Rex_84495` or `Huggie Hoops- Umbrella`, one charm serves several jewellery types and so carries several SKU lines, and the same design is drawn in more than one size). The rule, precisely:

- a positioned text line whose string matches the SKU pattern (setting, default `^[A-Z0-9][A-Z0-9 _.,'&()+\-]{1,60}$`, applied after trimming and upper-casing; Illustrator writes a whole sheet of labels in one text block, so the parser reads each positioned string as its own line with its own box);
- the **first** line's top edge is at most `labelGapPt` (default 18 pt ≈ 6.4 mm) below the charm outline's bottom edge, and its horizontal centre lies within the outline's horizontal extent widened by 25% on each side; when two outlines qualify, the one whose bottom edge is nearest wins;
- every further line that hangs directly under the line before it (within 1.8 line heights, centred with it) belongs to the same charm: each line is one SKU of that charm, all sharing the charm's per-SKU file;
- only the charm directly above a list owns it: a charm with nothing under it is reported unlabelled (and nothing is sent to Claude for it), a line with no qualifying outline is reported as an orphan;
- a sheet added later is read for its SKUs first, which is only text: a charm whose SKUs the library already holds is left exactly as it is, and only a charm carrying at least one new SKU is rebuilt — rebuilt whole, so all of its SKUs keep sharing one design file, and the master those SKUs came from is superseded rather than reported as a clash. The Master tab's "re-index SKUs already held" (and the local indexer's `--all`) rebuilds every charm instead;
- one SKU under several charms is that design in several sizes: the outlines are ranked by size and lettered S/L, S/M/L, XS/S/M/L, XS/S/M/L/XL (a label may also carry its own size after " · "); two of them within 3 % of the same size are a real duplicate, reported, and only the first keeps the SKU.

Labels are never part of the charm: they are excluded from the members before silhouettes are built, so a SKU string can never be cut.

### 6.2 Parser addition (`charm-nest-pdf.js`)

The interpreter already records text runs as `{ kind: "text", bbox, chars, start, end }` but not the string itself. Add the decoded string to that record (the lexer already has the operand; `Tj`/`TJ` strings are decoded through the font's encoding when it is simple, otherwise kept raw), then:

```js
/** SKU labels under charms: { charmIndex → "BR-CMP-01" }, plus the leftovers. */
function labelCharms(parsed, charms, opts) {
  opts = Object.assign({ pattern: /^[A-Z]{2,4}-[A-Z0-9]{2,6}(-[A-Z0-9]{1,4})?$/, gapPt: 18, widen: 0.25 }, opts || {});
  const texts = parsed.segments.concat(parsed.nested).filter(s => s.kind === "text" && s.str);
  const labels = new Map(), unlabelled = [], orphans = [], duplicates = [];
  for (const t of texts) {
    const sku = String(t.str).trim().toUpperCase(); if (!opts.pattern.test(sku)) continue;
    const cx = (t.bbox[0] + t.bbox[2]) / 2, top = t.bbox[3];
    let best = null, bestGap = Infinity;
    for (const c of charms) {
      const b = c.outline.bbox, w = b[2] - b[0];
      const gap = b[1] - top;                                         // outline bottom (y-up) minus label top
      if (gap < -1 || gap > opts.gapPt) continue;
      if (cx < b[0] - w * opts.widen || cx > b[2] + w * opts.widen) continue;
      if (gap < bestGap) { bestGap = gap; best = c; }
    }
    if (!best) { orphans.push({ sku, bbox: t.bbox }); continue; }
    if (labels.has(best.index)) duplicates.push({ sku, also: labels.get(best.index), charmIndex: best.index });
    labels.set(best.index, sku); best.label = t;                     // remembered so it is dropped from members
  }
  for (const c of charms) { if (!labels.has(c.index)) unlabelled.push(c.index); c.members = c.members.filter(m => m !== c.label); }
  return { labels, unlabelled, orphans, duplicates };
}
```

### 6.3 Building the index

Runs in the browser for a file up to a few hundred charms, and as `charmMaster-background` (parked payload, progress in `Charm_Nest_Jobs`) beyond that. Steps:

1. `parseSource` → `groupCharms` → `labelCharms` → Claude grouping review (fragments merged before indexing) → `buildSilhouettes`.
2. For every labelled charm: `buildSingleCharm(charm, parsed)` produces a `.ai` whose content stream is the original page with every non-member byte range blanked, so paths, stroke widths, colours and nested forms are the originals. Stored at `charmnest/master/{metal}/{SKU}.ai` with a PNG thumbnail beside it.
3. Firestore `Charm_Master_Index/{SKU}`. The index is keyed by SKU alone: a SKU is one design whatever colour it is ordered in. The metal card a copy lands on comes from the order line's metal, never from the master.

```json
{ "sku": "BR-CMP-01", "masterPath": "charmnest/master/2f8e….ai", "masterHash": "2f8e…", "charmHash": "9ac0…",
  "widthPt": 62.4, "heightPt": 70.1, "areaPt2": 2810, "members": 25, "holes": 1, "engravable": true,
  "aiPath": "charmnest/master/BR-CMP-01.ai", "thumbPath": "charmnest/master/BR-CMP-01.png", "indexedAt": 1789… }
```

4. `Charm_Master_Files/{masterHash}` records the master's path, count, `unlabelled[]`, `orphans[]`, `duplicates[]`. Several master files may be indexed (one per family, say); a SKU present in two masters is a duplicate and blocks pool adds for that SKU until fixed. The **Master** tab shows the SKU grid and the leftovers in red.
5. `engravable` defaults to `true` when the back mask (7.2) has an inscribed rectangle of at least 6 × 3 mm; the Master tab lets an operator override it per SKU.
6. **Sizes.** A SKU sold in several sizes has one master design per size, labelled `BR-CMP-01 · S`, `BR-CMP-01 · M` (the label rule accepts a size suffix after a middle dot or a space); the index entry then carries `sizes: { S: {…}, M: {…} }` and a line's `size` selects one. A sized line whose SKU has no matching size design is held.
7. **Up.** `upAngle` is the direction from the outline's centroid to its hanging hole (the smallest cut-out nearest the outline edge; the ring merged by review counts). No hole → the master's drawn orientation. Editable per SKU; used by 7.3 for text baselines and by 7.6 for jig orientation.
8. **Outlined labels.** When a master has no text objects (labels converted to outlines) or a text uses a CID font without `ToUnicode`, the strip under each charm is rendered and Claude reads it with a strict SKU-pattern schema and a confidence; any read under 0.95 goes to the Master tab with the crop beside the charm. A person confirms every vision-read label once; it is then stored and never re-read.
9. **Scale check.** The Master tab flags any charm outside the shop's size range (setting) and any SKU whose size moved more than 5% on re-index.
10. **Master validation.** An open outline, a detached ring not merged by review, or a SKU present in two masters blocks that SKU with the reason shown; nothing is indexed silently.

### 6.4 Pulling a charm for an order line

```js
async function poolAdd(row) {
  const ix = await api("charmNestLibrary", { op: "masterGet", sku: row.line.sku.toUpperCase() });   // SKU alone; colour never changes the design
  if (!ix.entry) { row.state = "unmatched"; Unmatched.add(row); agent({ bridge: true }, "warn", `${row.order.receiptId} · ${row.line.sku}: not in any master file`); return; }
  const bytes = await (await fetch(ix.entry.aiUrl)).arrayBuffer();
  const src = await intakeBytes(new Uint8Array(bytes), `${row.order.receiptId}_${row.line.sku}.ai`, row.metal, { review: false, name: `${row.order.receiptId} · ${row.line.sku}` });
  for (let copy = 1; copy <= row.line.quantity; copy++) {
    const charm = copy === 1 ? src.charms[0] : cloneCharm(src.charms[0]);       // identical geometry, its own id
    charm.name = `${row.order.receiptId} · ${row.line.sku}${row.line.quantity > 1 ? ` · ${copy}/${row.line.quantity}` : ""}`;
    charm.order = { receiptId: row.order.receiptId, transactionId: row.line.transactionId, sku: row.line.sku, copy };
    const pool = { poolId: uid(), orderId: row.order.receiptId, transactionId: row.line.transactionId, sku: row.line.sku, metal: row.metal, copy, quantity: row.line.quantity, charmHash: charm.hash, aiPath: ix.entry.aiPath, engrave: row.engrave, state: "ready", sheetId: null, setId: null };
    await api("charmNestLibrary", { op: "poolPut", pool });
    charm.poolId = pool.poolId; row.poolIds.push(pool.poolId);
    activePage(row.metal).charms.push(charm);
  }
  row.state = "pooled"; sheetDirty(activePage(row.metal)); renderOrders();
}
```

Silhouettes for a master charm are built once at index time and cached by `charmHash`, so a pool add costs a fetch, not a raster. The charm name carries the order number, so the nest report, the labelled proof and the Library all name the order. The same SKU ordered in gold and in silver produces the same design on two different cards.

## 7. Engraving

### 7.1 Which lines are engraved, and what the text is

Engraving is always on the back. A line is engraved when its personalisation is non-empty, or when Claude finds an engraving request in the buyer message, staff note or Brites messages with `confidence ≥ 0.8`. Classification runs through `charmNestAgent-background` with a strict schema:

```json
{ "engrave": true, "text": "Anna\n9.26.25", "source": "staffNote", "sourceQuote": "customer phoned: spell it ANNE", "requests": { "side": "back", "font": null, "handwriting": false, "image": false },
  "questions": [], "confidence": 0.97 }
```

Rules in the prompt: the text is the customer's words verbatim; a staff note overrides Etsy text; never invent, translate or improve; split lines only at the customer's breaks or between a name and a date; anything ambiguous goes into `questions`. Any of `confidence < 0.8`, non-empty `questions`, a `requests` field other than back/null/false, or a SKU marked not engravable sends the line to the review panel (11) with the reason. Every copy of a line gets the same text.

**Exactness.** Only what Myriad Pro can set exactly is set. Before fitting, every character is checked against the font's glyph coverage. A character the font lacks (a heart, emoji, Cyrillic, CJK, an unusual symbol) is never substituted, drawn or approximated: the line goes to the review panel naming the characters, with quick fixes (spell the symbol as a word, message the customer through the station chat, engrave the rest, skip engraving).

### 7.2 The back: an actual flip, verified

The back view is produced by transforming the charm's real geometry, never by drawing anything new, and every step is checked. Claude has no part in this; it only reviews the result in 7.5.

```
FRONT (as in the master, y-up file units)
  ├─ cut geometry  = outline + every inner closed achromatic stroke (cut-outs)      → visible from the back, mirrored
  └─ front detail  = fills, coloured strokes, open strokes, text, images            → invisible from the back, dropped
STEP 1  classify each member: cut or front-detail (the same rule the sorter uses to find cut lines)
STEP 2  build the front silhouette F at 6 px/pt with holes open (holesSolid:false)
STEP 3  mirror: matrix M = [-1 0 0 1 (2·cx) 0] with cx = outline centre; apply M to every cut member's points and control points
STEP 4  build the back silhouette B from the mirrored cut geometry at the same resolution
STEP 5  verify B == flipX(F) pixel for pixel (allow ≤ 0.05 % differing pixels from anti-aliasing); verify each hole's mirrored centroid
        lands on a hole in B; verify area(B) == area(F) within 0.1 %; verify no front-detail member survived
STEP 6  orient: rotate the whole back view so upAngle points up (the hanging hole at the top)
STEP 7  record { cx, M, checks } on the piece; a failed check stops the piece with the diff image attached
```

```js
function backView(charm, res = 6) {
  const cut = charm.members.filter(m => m === charm.outline || isCutLine(m));                   // STEP 1
  const dropped = charm.members.length - cut.length;
  const F = rasterSilhouette(charm, res, { holesSolid: false });                                  // STEP 2
  const cx = (charm.outline.bbox[0] + charm.outline.bbox[2]) / 2;
  const M = [-1, 0, 0, 1, 2 * cx, 0];
  const mirrored = cut.map(m => transformSegment(m, M));                                          // STEP 3 — points and Bézier handles alike
  const B = rasterSilhouetteFrom(mirrored, res, { holesSolid: false });                          // STEP 4
  const checks = {                                                                                // STEP 5
    pixels: diffFraction(B, flipX(F)) <= 0.0005,
    holes: holesOf(charm).every(h => B.at(mirrorX(h.cx, cx), h.cy) === 0),
    area: Math.abs(area(B) - area(F)) / area(F) <= 0.001,
    detailDropped: dropped === charm.members.length - cut.length
  };
  if (!Object.values(checks).every(Boolean)) throw new BackViewError(checks, { F, B });
  const up = charm.upAngle || 90;                                                                 // STEP 6
  return { members: mirrored.map(m => rotateSegment(m, 90 - up, centreOf(charm))), mask: rotateMask(B, 90 - up), cx, M, checks };
}
```

`isCutLine` is the sorter's existing rule: a closed path with an achromatic stroke. Blue and red fills and strokes (the engraving detail the sorter draws on the front) are front-only and are dropped. The reviewer sees the front and the back side by side, the back with only the outline and the cut-outs, so a wrong classification is visible before any text is placed.

### 7.3 Text: Myriad Pro, largest legible size on solid material

The back mask is the mirrored, oriented silhouette with holes open, eroded by the engraving margin (`engraveMarginMm`, default 0.8 mm) so text keeps off every cut edge and every cut-out. Any engraver keep-out drawn on a `BACK KEEP-OUT` layer in the master is subtracted too.

Font files: Myriad Pro Regular and Semibold `.otf` in `vendor/fonts/` (licensed with the shop's Illustrator seat, excluded from the public build). Text becomes outlines through `opentype.js`, so the output carries paths, never a font reference.

```js
function fitText(lines, font, mask, res, opts) {
  opts = Object.assign({ minCapMm: 1.6, maxHeightFrac: 0.4, lineGap: 0.18, minStrokeMm: 0.15, minGapMm: 0.12, tryRotated: true }, opts || {});
  const rects = largestRectangles(mask, 6);                                  // the sorter's pocket finder, top 6 by area
  const capPerEm = font.tables.os2.sCapHeight / font.unitsPerEm;
  let best = null;
  for (const r of rects) for (const angle of (opts.tryRotated ? [0, 15, -15, 30, -30] : [0])) {
    let lo = 1, hi = Math.min(r.hPt, opts.maxHeightFrac * mask.hPt), size = 0;
    while (hi - lo > 0.05) {
      const mid = (lo + hi) / 2, layout = layoutLines(lines, font, mid, opts.lineGap, angle);
      if (layout.fitsIn(r) && inkInsideMask(layout, mask, res) && strokeOk(layout, font, opts)) { size = mid; lo = mid; } else hi = mid;
    }
    const gain = best ? (size - best.size) / best.size : Infinity;
    if (size && (!best || (angle === 0 ? size > best.size + 0.01 : gain >= 0.12))) best = { size, rect: r, angle, layout: layoutLines(lines, font, size, opts.lineGap, angle) };
  }
  if (!best) return { ok: false, reason: `no solid area for ${lines.length} line(s)` };
  const capMm = best.size * capPerEm * MM_PER_PT;
  best.weight = capMm < 2.2 ? "Semibold" : "Regular";
  best.small = capMm < opts.minCapMm;                                        // flagged in review, never dropped
  return Object.assign({ ok: true, capMm }, best);
}
```

`inkInsideMask` rasterises the glyph outlines at the verifier's resolution and requires every ink pixel to sit on a `1` of the eroded mask, which makes "only on solid material, never inside a cut-out" a hard check. `strokeOk` measures the thinnest stem and the smallest inter-stroke gap at the candidate size against the engraver's limits (settings from a test coupon). The fitter always returns the largest size that passes; under the legibility figures the placement is flagged **small**, never dropped.

### 7.4 Verification

1. **Geometry.** Final glyph raster against the eroded back mask: zero ink outside, zero ink in any hole. A failure stops the piece; it is a bug, not a review item.
2. **Flip integrity.** The 7.2 checks are re-run on the final piece file after it is written and re-parsed (the same twin-verification habit the sorter uses for sheets).
3. **Claude's read.** Claude sees the rendered back once (mm grid, outline, holes, text) and answers `{ legible, notes }` on readability and taste only; its notes go to the reviewer.

### 7.5 A human reviews every placement

Every back placement is shown to a person before it is released, without exception. Any employee may review; the name from the station's `employee_name` is required and recorded. The review panel (11) shows one distinct placement at a time (identical copies of one line are reviewed once and the count shown): the front and the back side by side, the back at large scale with the mask hatch, the text as it will be cut, the order, SKU, form, size, copy count, the customer's words beside Claude's reading and the staff note, the size in mm, the **small** flag, and Claude's notes. Controls: **Approve**, **Nudge** (drag or 0.25 mm arrows; size re-fits to the new box), **Resize** (slider capped at the fitted maximum), **Re-split lines** (the fitter re-runs), **Skip** (cut plain, order flagged), **Send back** (a decision on the words is needed). Keyboard: A, S, arrows. The back file is only written after approval; approval stores `approvedBy` and `approvedAt`. A set cannot be committed while any engraving in it is unreviewed.

### 7.6 Back files, one per piece

Pieces are engraved one at a time after the sheet is cut. Inside the sheet's folder (8.2):

- `back/{sheetName}_back_{order}_{SKU}_{copy}.ai`: page sized to the charm plus 5 mm, oriented hoop-up; layer `CUT OUTLINE (reference)` with the mirrored outline and cut-outs; layer `ENGRAVE` with the text as filled paths. Produced from the same layout in the orientation the engraver's software expects (`backFileView` setting: `asSeenFromBack` or `frontCoordinates`, decided by one test piece per engraver and stored per engraver).
- `back/back-index.pdf`: a contact sheet of every engraved piece with thumbnail, order, SKU, copy, text, size, approver.
- `back/back-report.json`: `[{ poolId, order, sku, copy, text, font, weight, sizePt, capMm, box, angle, flipChecks, verified, review, approvedBy, approvedAt }]`.

`Charm_Pool_Back/{poolId}` holds the same fields plus `sheetId` and `setId`. The sheet record gains `backPool[]` and `backOutputs`; the Library shows **Engraving · n**.

## 8. Sets, labels and completion

### 8.1 A set is the unit that travels

Mixed orders (a gold piece and a silver piece on one order) mean sheets of different materials must stay together through production. One run on one date produces one set; the set number is per date across all materials, and every sheet, label, back file and folder in it carries the same set number.

```
charmnest/sets/2026-09-16/Set-3/
  set.json                                   ← manifest: orders, lines, which sheet each copy is on, labels, backs, run id, approvals
  GF_Sep.16.26_Set-3_Sheet-1/
    GF_Sep.16.26_Set-3_Sheet-1.ai            ← the cut file (untouched original paths, one layer per charm named by order · SKU)
    GF_Sep.16.26_Set-3_Sheet-1.pdf
    GF_Sep.16.26_Set-3_Sheet-1_labelled.pdf  ← proof with order numbers on the pieces
    GF_Sep.16.26_Set-3_Sheet-1_nest-report.json
    GF_Sep.16.26_Set-3_Sheet-1_label.png     ← the QR label for THIS sheet (8.3)
    preview.png
    back/…                                   ← 7.6
  GF_Sep.16.26_Set-3_Sheet-2/                ← overflow of the same material
  SS_Sep.16.26_Set-3_Sheet-1/
  labels/
    Set-3_labels.pdf                         ← every sheet label of the set, one page each, in sheet order
  Set-3_manifest.pdf                         ← human sheet: orders ↔ sheets, held orders, engraving counts
```

Naming: `{MaterialTag}_{Mon.DD.YY}_Set-{K}_Sheet-{n}`, with `K` the set number for that date (allocated in a Firestore transaction, never by counting on the client) and `n` the sheet index within that material in that set. The current sorter numbers per material per day (`GF_Sep.16.26_Set-1`); this changes to per-date numbering shared across materials, so a gold sheet and a silver sheet of the same run read as `…_Set-3_…` on both. One local clock (`localDay()`) decides both the folder date and the name date.

`Charm_Nest_Sets/{setId}` = `{ setId, runId, day, seq: K, materials[], sheetIds[], orders: { [receiptId]: { lines: [{ transactionId, copies: [{ copy, sheetId, poolId, backPoolId }] }], held: null | reason } }, labels, status, createdAt, committedAt }`.

The Library gets a **Sets** view: one card per set with its sheets side by side, held orders, engraving count and label thumbnails; opening a sheet still works as today.

### 8.2 The sheet record

`Charm_Nest_Sheets` gains `setId`, `setSeq`, `sheetIndex`, `orders[]`, `poolIds[]`, `backPool[]`, `backOutputs`, `label`. The sorter's existing per-sheet folder logic moves under the set folder; overflow sheets are numbered within the material.

### 8.3 One QR label per sheet, saved with the set

A label lists every order with a piece on that sheet; an order with pieces on two sheets appears on both labels. The payload format is unchanged (`B36|{material}|id.id.…`, ECC M, 145 × 145 pt, the same positions), so the sort scanner keeps working; the station's bucketing is no longer used for labels (it still validates `complete.preview`).

```js
async function saveSheetLabel(sheet, set) {
  const ids = [...new Set(sheet.placements.map(p => byId.get(p.id).order.receiptId))];
  const parts = safeChunks(ids, sheet.metalKeyDS, 1000, 50, 8);                                   // same chunker as the station
  const files = [];
  for (const [i, slice] of parts.entries()) {
    const payload = encodeOrderList(slice, sheet.metalKeyDS);
    const label = `${METAL_TAG[sheet.metal]} · ${set.name} · Sheet ${sheet.sheetIndex}${parts.length > 1 ? ` [${i + 1}/${parts.length}]` : ""} · ${slice.length} orders`;
    const png = await renderLabelPng(payload, label);                                              // 145×145 pt geometry, qrcodejs, ECC M
    files.push(await uploadBytes(`${sheet.folder}/${sheet.name}_label${parts.length > 1 ? `_${i + 1}of${parts.length}` : ""}.png`, png, "image/png"));
  }
  return files;
}
```

`labels/Set-K_labels.pdf` collects every sheet label of the set, one page each, for printing from the Library if paper is wanted; the Library also shows a label full-screen for a camera to read.

### 8.4 Marking the orders complete

After every sheet in the set is written, verified and saved, every engraving in it approved, and every label saved:

```js
const committable = Object.entries(set.orders).filter(([, o]) => !o.held).map(([id]) => id);
await DesignLink.call("ui.select", { receiptIds: committable });                                   // the real lock, only now
const preview = await DesignLink.call("complete.preview", { receiptIds: committable });
const r = await DesignLink.call("complete.commit", { receiptIds: committable, labels: { setId: set.setId, folder: `${set.folder}/labels`, files: set.labelFiles } });
await api("charmNestLibrary", { op: "setUpdate", setId: set.setId, patch: { status: r.refused.length ? "complete-with-holds" : "complete", committed: r.completed, refused: r.refused, committedAt: Date.now() } });
```

The station runs `commitCompletion`: ledger, unlock, "DESIGNED :)" in each order's chat, archive with `labels`, `setId` and `completedBy: "sorter"`. Held and refused orders stay open on the station and are listed on the set with their reasons.

## 9. Sorter UI

New tabs: **Orders**, **Design Station**, **Master**, **Review** (11), **Sets** (in Library). Existing: Nest, Library, Charms.

- **Orders** — 5.1. A **Run set** button starts the autonomous run (10).
- **Design Station** — the frame and the bridge console (4.7).
- **Master** — one panel per metal: master file, hash, indexed count, SKU grid with thumbnails and sizes, leftovers in red, per-SKU `engravable` toggle, **Re-index**.
- **Engraving** — the tray (lines awaiting a decision on the words: the customer's text, Claude's reading and questions), the review queue (every fitted placement, one at a time, 7.4), and the back pool per sheet with each piece's approver.
- **Live strip** — the last five bridge or engraving events with timestamps, on every tab.

All bridge, master, pool and engraving steps write into the existing agent log with kinds `DS`, `MASTER`, `POOL`, `ENGRAVE`, so the per-card panel and the Log button already show the whole run.

## 10. The run

### 10.1 Two halves and a record

```
HALF A · produce                                              HALF B · release
 1 pull + interpret ─ Etsy via the station; line specs           7 engrave ─ flip, fit, verify; REVIEW QUEUE (human)
 2 claim ──────────── gold dot on the station                    8 re-validate orders (10.3)
 3 pool ───────────── master lookup, copies; trays               9 labels ─ one per sheet, set PDF
 4 plan ───────────── 72 % ceiling, overflow sheets              10 lock + preview + commit on the station
 5 nest ───────────── per material, verify twice, write, save    11 set complete; claims released
 6 checkpoint ─────── set "awaiting review"
```

`Charm_Nest_Runs/{runId}` = `{ runId, setId, day, step, startedAt, updatedAt, lines: { [lineKey]: { state, poolIds, reason } }, sheets: { [sheetId]: state }, holds, errors, resumable: true }`, written before and after every step. Pool ids are deterministic (`${receiptId}_${transactionId}_${copy}`), so a retried step never duplicates. **Resume run** picks up at the recorded step; a line claimed by a live run is skipped by another run.

### 10.2 What stops a run

Any of: a hydration shortfall, the station down (heartbeat), cloud offline, a sheet that is not complete and verified for a reason other than overflow, a flip check failure, a refused command. The run banner names the cause and the fix; nothing is committed.

### 10.3 Re-validation

Before engraving and again before commit, every order's `update_timestamp` is re-read through `orders.detail`. A changed order re-runs interpretation and classification and invalidates any fitted or approved placement (the reviewer sees old and new text side by side). A vanished order (shipped, cancelled, refunded) is dropped from the set with a red line and its pieces are marked "order gone" on the sheet report.

### 10.4 Undo

**Undo set** runs `complete.undo` on the station, sets the set back to "awaiting review", pool lines back to "written", and keeps every file.

## 11. The review panel

One panel for everything a person must decide, with the problem stated and the fixes one click away. Every item shows: what was identified, the evidence (crop, text, source quote), why it stopped, and the quick fixes. Items, in the order they appear in a run:

| Item | Identified as | Evidence shown | Quick fixes |
| --- | --- | --- | --- |
| Needs material | no material, or "solid" with no karat | the option values, the title | pick a material (writes a staff note and re-classifies) · skip line · hold order |
| Needs mapping | an option value with no `Charm_Option_Map` entry | option name and value, listing | map to form/size/chain once (remembered) · not relevant (ignored for this listing) |
| Unmatched SKU | no master entry for the SKU | SKU, title, Etsy image | pick the charm from the master grid (alias remembered) · no design (remembered) · hold |
| Missing size | sized line, master has no design for that size | size value, sizes available | pick an available size · hold |
| Oversize | charm cannot fit the sheet under the ceiling | sizes | different stock · hold |
| Engraving words | low confidence, questions, request for front/font/handwriting/image | customer's words, staff note, messages, Claude's reading and questions | confirm text · edit text (recorded as staff decision) · message customer (opens station chat with a draft) · no engraving |
| Not representable | characters Myriad Pro lacks | the characters highlighted in the text | replace with a word · drop the character · message customer · no engraving |
| Flip check failed | back silhouette does not equal the mirrored front | front, back, diff image, which check failed | re-run · mark SKU not engravable · hold |
| Placement review | every fitted placement | front and back side by side, mask hatch, text, size, small flag, Claude's notes | approve · nudge · resize · re-split · skip · send back |
| Order changed | Etsy update after pull | old and new text or options | accept new (re-fit) · hold |
| Held order | any line unresolved | the line and its reason | jump to that item |

The panel is the sorter's **Review** tab; its count is on the Live strip and the run banner, and a desktop notification fires when the run needs a person.

## 12. Data model

| Collection | Key | Fields |
| --- | --- | --- |
| `Design_Bridge` | sessionId | `sorterClientId, bench, startedAt, endedAt, commands, dropped` |
| `Design_Bridge/{s}/log` | auto | `t, dir, type, ms, payload` (ids and counts only) |
| `Charm_Nest_Runs` | runId | 10.1 |
| `Charm_Nest_Sets` | setId | 8.1 |
| `Charm_Master_Files` | masterHash | `path, charms, unlabelled[], orphans[], duplicates[], visionReads[], indexedAt` |
| `Charm_Master_Index` | SKU | 6.3, plus `sizes{}`, `upAngle`, `backKeepOut[]`, `engravable` |
| `Charm_Sku_Aliases` | listingId | `sku` (learned in review) |
| `Charm_Sku_NoDesign` | auto | `pattern` or `sku` |
| `Charm_Option_Map` | listingId or `*` | `{ optionName: { value: { field, value } } }` |
| `Charm_Pool` | `${receiptId}_${transactionId}_${copy}` | `runId, setId, sheetId, sku, material, size, form, spec, charmHash, masterHash, engrave, state` |
| `Charm_Pool_Back` | poolId | 7.6 |
| `Charm_Nest_Sheets` | (existing) | 8.2 |
| `Design_Order_Archive` | (existing) | `+ labels, setId, sheetIds, backCount, completedBy` |

Storage: `charmnest/master/…`, `charmnest/sets/{day}/Set-K/…` (8.1). Firestore rules: every `Charm_*`, `Charm_Nest_*` and `Design_Bridge` collection denies client writes; all writes go through the functions. Retention: pool working files 90 days; masters, sets, sheets, backs and labels kept.

## 13. Functions

| Function | Kind | Ops or purpose |
| --- | --- | --- |
| `charmNestLibrary` | existing | `+ masterPutIndex, masterGet, masterList, poolPut, poolList, poolUpdate, backPut, backList, setPut, setUpdate, setGet, bridgeLog` (each a single-field query, no composite indexes, same validation style as `putSheet`) |
| `charmMaster-background` | new | index a large master file server-side: parse, group, label, per-SKU `.ai`, thumbnails; progress in `Charm_Nest_Jobs`; parked payload like the agent |
| `charmEngrave-background` | new | the two Claude calls of 7.1 and 7.4 through `_charmNestAgent.js` with modes `engraveIntent` and `engraveReview`; JSON-schema output, `effort: "high"` |
| `firebaseOrders`, `designArchive` | existing | archive `put` accepts `labels` |

Manifest: add the two functions to `scripts/netlify-function-entries.json`; `build-public.cjs` allowlist gains `vendor/opentype-1.3.4.min.js` and excludes `vendor/fonts/`.

## 14. Settings (sorter)

| Setting | Default | Meaning |
| --- | --- | --- |
| `skuPattern` | `^[A-Z0-9][A-Z0-9 _.,'&()+\-]{1,60}$` | what counts as a SKU label: any one-line free-text SKU, as the shop uses (a saved copy of the older `BR-XXX-NN` default is migrated) |
| `labelGapMm` | 6.4 | how far below an outline a label may sit |
| `engraveMarginMm` | 0.8 | keep-out from every cut edge and cut-out |
| `engraveMinCapMm` | 1.6 | below it a placement is flagged **small** in the review (never dropped) |
| `engraveMaxHeightFrac` | 0.40 | text block height cap as a fraction of the charm |
| `engraveConfidence` | 0.80 | below it, a person decides |
| `engraveTryRotated` | on | allow ±15°/±30° layouts when they gain ≥ 12% |
| `autoCommit` | on | mark orders complete on the station when a set finishes and every engraving is approved |
| `backFileView` | per engraver | `asSeenFromBack` or `frontCoordinates`, settled by one test piece |
| `engraveMinGapMm` / `engraveMinStrokeMm` | 0.12 / 0.15 | engraver limits from a test coupon |
| `heartbeatS` | 5 | station heartbeat; three misses mark it down |

## 15. Tests

- **Bridge.** Headless Chromium loads the sorter framing a local `design-1.html` with a stub `firebaseOrders`; asserts `hello`, `orders.snapshot` shape and shortfall reporting, claim then lock, a wrong-origin message dropped and counted, `complete.commit` without preview refused, `complete.commit` with labels removing rows and writing the ledger, heartbeat loss and re-`hello` after a frame reload.
- **Interpretation.** Fixture orders covering every option pattern (necklace, earrings, charm only, sizes, chain), a missing SKU resolved by alias, a no-design chain line, "Rose Quartz" not classified as rose, a staff note overriding Etsy text.
- **Labels in the master.** Text under, 8 mm below, shared between two outlines, unlabelled, duplicate, size suffix, and an outlined-text master read through the vision fallback.
- **Extraction.** The per-SKU `.ai` re-parsed equals the master's charm (member count, silhouette hash).
- **Flip.** Asymmetric fixture charms with off-centre holes: every 7.2 check passes on a correct flip and fails on a deliberately wrong one (unmirrored, hole shifted, a fill left in).
- **Fit.** A hole, a thin ring, a diagonal band: text never crosses the eroded mask, the ring returns "no room", the band takes the rotated layout only at ≥ 12% gain, the size is the largest that fits (0.1 pt more must fail), a heart character is refused.
- **Review gate.** A set with one unapproved engraving cannot commit; approval stores the name; a nudge re-fits.
- **Run.** Resume after a simulated crash at every step; two runs contending for one line; an order changed between pull and commit invalidates its approval; a held order is not committed while its sheet mates are.
- **Sets.** A mixed gold-and-silver order lands on two sheets with the same set number and appears on both labels; folder and file names agree on the date across midnight.

## 16. Implementation plan

1. **Bridge and monitoring** — `Bridge` in `design-1.html`, `commitCompletion` split, `frame-ancestors`, `DesignLink`, the Design Station tab and console, Orders tab with pull. Deliverable: orders pulled and selection driven with both UIs visible.
2. **Master and pool** — text strings in the interpreter, `labelCharms`, Master tab, index and per-SKU files, `poolAdd`, queue naming by order. Deliverable: a pulled line becomes a queued charm.
3. **Engraving** — intent classifier, back mask, `fitText` with opentype.js and Myriad Pro, verification, Claude's read, the mandatory review queue with nudge and resize, back files and index, Engraving tab, Library badge. Deliverable: an engraved line yields per-piece back files, each approved by a named person, tied to its sheet.
4. **Sets, labels, completion** — run record and resume, set folder and per-date numbering, per-sheet labels and set PDF, re-validation, held orders, `complete.commit`, archive `labels`, Undo set. Deliverable: a finished set marks its committable orders complete with labels saved, nothing printed.
5. **Review panel and hardening** — the Review tab with every item type and its quick fixes, option maps, aliases, no-design list, heartbeat, veil, release on unload, Firestore rules, retention.

## 17. Safety rules

- The station's guards stay under remote control: no completion without a preview of the same orders and a saved label list; locks, claims and undo unchanged.
- Material, form and size come from Etsy options through deterministic maps; Claude reads free text only.
- Engraving text is the customer's words (staff note first); Claude may split lines and raise questions, never invent, translate or improve. Characters the font lacks are refused to review, never approximated.
- The back is an actual, checked flip of the cut geometry; front-only detail never appears on a back file; a failed check stops the piece.
- No text outside the eroded mask or inside a cut-out, ever. No back file is written before a human has approved the placement on screen.
- An order with any unresolved line is held whole; nothing is committed for it.
- Master extraction copies bytes; nothing is redrawn.
- Every automatic step logs before and after it acts and is recorded in the run; the run stops on the first refusal and resumes from the record.

## 18. Decisions log

- Draft 1 → 2: labels under charms; back engraving; Myriad Pro; per-piece engraving; sorter completes orders and saves labels; identical copies; same PC.
- Draft 2 → 3: SKUs colour-agnostic; largest size with mandatory visual review; print button hidden.
- Draft 3 → 4 (from the plan review): claims during the run, lock at commit · SKU is the design only, material and every other feature from Etsy options via deterministic maps · nothing not exactly representable is engraved, review panel with problem and quick fixes · the flip is real and verified step by step, front detail hidden · one label per sheet, sheets travel in sets with one set number per date across materials and one folder per set · orders held whole · any human employee may approve, name recorded · persistent run record, re-validation of orders, BOM and no-design lists, outlined-label fallback, up-angle, glyph coverage, engraver file orientation setting.
