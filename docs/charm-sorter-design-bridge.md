# Charm Sorter ⇄ Design Station bridge

Design and implementation document. Status: draft 2, decisions incorporated. Owner: production tooling.

Decisions taken on draft 1: SKU labels are text under each charm · engraving is always on the back · font is Myriad Pro, legible, never inside a cut-out, only on solid material · pieces are engraved one at a time after the sheet is cut · the sorter marks orders design-complete and QR labels are generated and saved with the set of sheets, no longer printed · a quantity of 2 means two identical charms (different text only when the charm type, colour or size differs) · both apps run on the same PC.

---

## 1. Goal

The Charm Nesting Station ("the sorter", `charm-nest-1.html`) becomes the master of the Design Station (`design-1.html`, "the slave"). Without an operator touching the Design Station, the sorter:

1. pulls the open orders the Design Station sees, with each line's SKU, metal bench, quantity and the customer's instructions;
2. drives any Design Station action (select, filter, open an item, complete) and shows it happening live in both consoles;
3. finds each ordered SKU in a master Illustrator file, lifts that charm out with every layer, path, colour and dimension intact, and drops it into a pool of designs ready to nest;
4. for any line that needs back engraving, shows the charm's back (a 180° turn about its vertical axis), has Claude read the customer's request, sets the text in Myriad Pro in the best place and size on solid material, and keeps the result in a separate pool tied to the sheet the order is on;
5. when a set of sheets is finished, generates the bench QR labels into the sheet folder and marks the orders design-complete on the Design Station.

Everything builds on what exists: the sorter's parser (`charm-nest-pdf.js`), silhouettes, parallel nesting, twin verification, per-sheet cloud folders and agent log; the Design Station's single metal classifier, attention flags, selection locks, completion ledger and archive; one shared Firestore project.

## 2. Vocabulary

| Term | Meaning |
| --- | --- |
| Order line | One Etsy transaction: order number, SKU, quantity, metal key, personalisation, buyer message |
| Metal key | `gold`, `silver`, `rose`, `10k`, `14k` as the Design Station classifies them; the sorter maps them to `gold`, `silver`, `rose`, `gold10k`, `gold14k` |
| Master file | One `.ai` per metal that holds every charm design, each with its SKU as text directly under it |
| Master index | The SKU → charm lookup built from a master file |
| Pool | Extracted charm designs waiting to be nested, one entry per order line and copy |
| Back pool | The engraving design for each engraved copy, kept with the sheet that holds the front |
| Set | The group of sheets produced from one pull of orders (one or more sheets per metal) |
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
| `hello` | `sorterClientId` | Opens the session, shows the banner | `{ bench, version, employee, counts, filters, selection }` |
| `orders.snapshot` | `{ metals?, onlyAttention?, hydrate: true }` | Every open order with hydrated lines; missing details fetched through the station's paced Etsy queue with progress | `{ orders: Order[] }` (4.4) |
| `orders.detail` | `{ receiptId }` | One order | `{ order }` |
| `orders.watch` | `{ on }` | Emit `orders.changed` after each station refresh | — |
| `ui.select` / `ui.deselect` | `{ receiptIds[] }` | `selectRow` / `deselectRow` on each row (takes or releases the realtime lock) | `{ selected[] }` |
| `ui.filter` | subset of the station's `F` model | Sets chips and re-applies | `{ filters }` |
| `ui.openItem` | `{ transactionId }` | `openListingModal` | `{ item }` |
| `ui.closeModal` | — | closes any open dialog | — |
| `ui.scrollTo` | `{ receiptId }` | `revealOrder` | — |
| `notes.set` | `{ receiptId, text }` | staff note, as the modal does | — |
| `chat.post` | `{ receiptId, text }` | internal Brites message, sender "Charm Sorter" | — |
| `complete.preview` | `{ receiptIds[] }` | bucketing, QR preview modal, nothing written | `{ jobs[], skipped[], stale[], solid[], mixedTarget }` |
| `complete.commit` | `{ receiptIds[], labels: { setId, folder, files[] } }` | Marks complete **without printing**: ledger, unlock, "DESIGNED :)" chat, archive; records where the labels were saved | `{ completed[] }` |
| `complete.undo` | — | `handleUndoComplete` | — |
| `release` | — | ends remote mode | — |

Guards enforced by the station whatever the driver: `complete.commit` requires a `complete.preview` of the same ids in this session and a `labels.files` list that is non-empty; it is refused while a grouping review or an Etsy sweep is in flight; commands from any other origin are dropped.

### 4.3 Events (Design Station → sorter, `id: 0`)

`state`, `orders.changed`, `selection`, `filters`, `ui.modal` (`{ open: "listing", transactionId }`), `activity` (every ticker line), `lock.changed`, `complete.done`, `error`. The sorter writes `activity` into its agent log as kind `DS` so one log reads end to end.

### 4.4 Order shape

```json
{
  "receiptId": "3521337740", "shipBy": 1789200000, "buyer": { "name": "A. Smith", "country": "US" },
  "attention": true, "staffNote": "", "hasMessages": false, "lockedBy": null,
  "lines": [{
    "transactionId": "4412778001", "listingId": "1718…", "sku": "BR-CMP-01", "title": "Compass charm necklace",
    "quantity": 2, "metalKey": "gold", "metalLabel": "Gold",
    "personalization": ["ANNA 9.26.25"], "buyerMessage": "please engrave on the back",
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

`proceedToPrint` is split. Its second half becomes `commitCompletion(ids, { labels })`, called by the button after the print handshake (unchanged today) and by `complete.commit` after the sorter has saved the labels. The archived record gains `labels: { setId, folder, files[] }` so the History view can open the saved label sheet. Printing stays available on the station for the transition, and is retired by removing the print button once the sorter path is in daily use.

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
  async pull() {
    const r = await DesignLink.call("orders.snapshot", { hydrate: true }, { onProgress: p => agentLive("Pulling orders", p.text, p.done, p.total) });
    this.rows = r.orders.flatMap(o => o.lines.map(l => ({ order: o, line: l, metal: METAL_FROM_DS[l.metalKey] || null, state: "pulled", engrave: null, poolIds: [] })));
    this.stale = false; renderOrders();
    agent({ bridge: true }, "DS", `Pulled ${r.orders.length} orders · ${this.rows.length} lines · ${this.rows.filter(x => x.order.attention).length} with instructions`);
  }
};
const METAL_FROM_DS = { gold: "gold", silver: "silver", rose: "rose", "10k": "gold10k", "14k": "gold14k" };
```

The Orders tab lists lines grouped by metal card: order number, SKU, title, quantity, ship-by, `!` attention flag with the personalisation and buyer message shown inline (never hidden behind a click, for the same reason the Design Station flags them), engraving state, pool state. Buttons: **Pull open orders**, **Add to pool** (selected or all), **Show in Design Station** (`ui.scrollTo` + `ui.openItem`), **Note** (`notes.set`).

## 6. Master file and SKU lookup

### 6.1 The labelling rule

Each charm in a master file has its SKU as a text object directly under it. The rule, precisely:

- a text segment whose string matches the SKU pattern (setting, default `^[A-Z]{2,4}-[A-Z0-9]{2,6}(-[A-Z0-9]{1,4})?$`, applied after trimming and upper-casing);
- whose bounding box **top edge** is at most `labelGapPt` (default 18 pt ≈ 6.4 mm) below the charm outline's bottom edge;
- whose horizontal centre lies within the outline's horizontal extent widened by 25% on each side;
- when two outlines qualify, the one whose bottom edge is nearest wins; a label with no qualifying outline and an outline with no label are both reported.

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
3. Firestore `Charm_Master_Index/{metal}_{SKU}`:

```json
{ "sku": "BR-CMP-01", "metal": "gold", "masterPath": "charmnest/master/gold/2f8e….ai", "masterHash": "2f8e…", "charmHash": "9ac0…",
  "widthPt": 62.4, "heightPt": 70.1, "areaPt2": 2810, "members": 25, "holes": 1, "engravable": true,
  "aiPath": "charmnest/master/gold/BR-CMP-01.ai", "thumbPath": "charmnest/master/gold/BR-CMP-01.png", "indexedAt": 1789… }
```

4. `Charm_Master_Files/{metal}` records the master's hash, count, `unlabelled[]`, `orphans[]`, `duplicates[]`. The **Master** tab shows the SKU grid and the leftovers in red; a master with duplicates blocks pool adds for those SKUs until fixed.
5. `engravable` defaults to `true` when the back mask (6.2) has an inscribed rectangle of at least 6 × 3 mm; the Master tab lets an operator override it per SKU.

### 6.4 Pulling a charm for an order line

```js
async function poolAdd(row) {
  const key = `${row.metal}_${row.line.sku.toUpperCase()}`;
  const ix = await api("charmNestLibrary", { op: "masterGet", key }) ;
  if (!ix.entry) { row.state = "unmatched"; Unmatched.add(row); agent({ bridge: true }, "warn", `${row.order.receiptId} · ${row.line.sku}: not in the ${labelOf(row.metal)} master`); return; }
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

Silhouettes for a master charm are built once at index time and cached by `charmHash`, so a pool add costs a fetch, not a raster. The charm name carries the order number, so the nest report, the labelled proof and the Library all name the order.

## 7. Engraving

### 7.1 Which lines are engraved

Engraving is always on the back. A line is engraved when:

- its personalisation is non-empty (the station has already cleared "Not requested"), or
- Claude finds an engraving request in the buyer message with `confidence ≥ 0.8`.

Classification uses the `charmNestAgent-background` pattern with a strict schema. Prompt input: personalisation, buyer message, listing title, SKU, whether the SKU is engravable, the house rule "engraving is always on the back". Output:

```json
{ "engrave": true, "text": "Anna\n9.26.25", "source": "personalization", "notes": "date kept as written", "confidence": 0.97, "questions": [] }
```

Rules in the prompt: never invent text; keep the customer's spelling, capitalisation and punctuation; split into lines only at the customer's own breaks or between a name and a date; put anything ambiguous into `questions` rather than guessing. A result with `confidence < 0.8`, a non-empty `questions`, or `engrave: true` on a SKU marked not engravable goes to the **Engraving** tray for a person. Every copy of a line receives the same text (decision 6).

### 7.2 The back

The back is the front mirrored across the charm's vertical axis (a 180° rotation about Y). Nothing is redrawn: the same member paths are drawn through the matrix `[-1 0 0 1 2·cx 0]` where `cx` is the outline's horizontal centre in file units. The back mask is built from the same raster the sorter already uses:

```js
/** Solid material on the back, at res px/pt: outline flood, holes removed, then eroded by the engraving margin. */
function backMask(charm, res, marginPt) {
  const front = rasterSilhouette(charm, res, { holesSolid: false });        // 1 = outline interior, holes 0
  const mirrored = flipX(front);                                            // column i ↔ width-1-i
  return erode(mirrored, Math.round(marginPt * res));                       // separable min filter
}
```

`holesSolid: false` is the one difference from the nesting silhouette (which fills holes so nothing nests inside a jump ring): here a hole is a cut-out and must never receive engraving. The margin (setting `engraveMarginMm`, default 0.8 mm) keeps text off the cut edge on all sides, including the edge of every cut-out. The preview draws the back with the outline, the holes, the front detail at 25% opacity, the eroded mask as a faint hatch, and the text box.

### 7.3 Text: Myriad Pro, legible, best fit

Claude decides what the text says (7.1). Where it goes and how big it is are computed, never guessed.

**Font.** Myriad Pro Regular and Semibold `.otf` files are loaded from `vendor/fonts/` (Adobe fonts licensed with the shop's Illustrator seat; they are not redistributable, so the folder is excluded from the public build allowlist and the files are read only by the sorter). Text is converted to outlines with `opentype.js` (vendored, `vendor/opentype-1.3.4.min.js`), so every output file carries paths and no font reference.

**Fit.** For each candidate rectangle from the mask, binary-search the point size:

```js
function fitText(lines, font, mask, res, opts) {
  opts = Object.assign({ minCapMm: 1.6, maxHeightFrac: 0.4, lineGap: 0.18, minStrokeMm: 0.15 }, opts || {});
  const rects = largestRectangles(mask, 6);                                  // the sorter's pocket finder, top 6 by area
  const capPerEm = font.tables.os2.sCapHeight / font.unitsPerEm;
  let best = null;
  for (const r of rects) {
    let lo = 1, hi = Math.min(r.hPt, opts.maxHeightFrac * mask.hPt), size = 0;
    while (hi - lo > 0.05) {
      const mid = (lo + hi) / 2, layout = layoutLines(lines, font, mid, opts.lineGap);
      if (layout.wPt <= r.wPt && layout.hPt <= r.hPt && inkInsideMask(layout, r, mask, res)) { size = mid; lo = mid; } else hi = mid;
    }
    const capMm = size * capPerEm * MM_PER_PT;
    if (size && capMm >= opts.minCapMm && (!best || size > best.size + 0.01 || (Math.abs(size - best.size) <= 0.01 && centreDist(r, mask) < centreDist(best.rect, mask)))) best = { size, rect: r, layout: layoutLines(lines, font, size, opts.lineGap) };
  }
  if (!best) return { ok: false, reason: `no room for ${lines.length} line(s) at ${opts.minCapMm} mm cap height` };
  best.weight = best.size * capPerEm * MM_PER_PT < 2.2 ? "Semibold" : "Regular";   // small text is set heavier so it survives the engraver
  return Object.assign({ ok: true }, best);
}
```

`inkInsideMask` rasterises the glyph outlines at the verifier's resolution and requires every ink pixel to sit on a `1` in the eroded mask, which is what makes "no engraving inside cut-outs, only on solid material" a hard rule rather than a hope. `largestRectangles` returns axis-aligned rectangles; a second pass tries the same at ±15° and ±30° for charms whose solid area is a diagonal band, keeping a rotated layout only if it gains at least 12% in size, since rotated text reads worse.

**Legibility floor.** Cap height ≥ 1.6 mm and stroke ≥ 0.15 mm at the chosen weight (setting), or the line is not engraved automatically: it goes to the tray with the best achievable size shown, so a person decides to accept a smaller size, shorten the text, or skip.

### 7.4 Verification and review

1. Geometry: the final glyph raster is checked against the eroded back mask (zero ink outside; zero ink in any hole).
2. Claude sees the rendered back once (mm grid, outline, holes, text) and answers `{ legible: bool, notes }` on readability and taste only. A `false` sends the piece to the tray with the notes; the reviewer never edits the layout itself.

### 7.5 Back pool files

Pieces are engraved one at a time after the sheet is cut, so the deliverable is one file per engraved piece, plus an index.

- `charmnest/sheets/{day}/{sheet}/back/{sheet}_back_{order}_{SKU}_{copy}.ai`: the charm's back, mirrored member paths at the front's own orientation and scale (not the nested rotation, since the piece is handled loose), one layer `CUT OUTLINE (reference)` with the mirrored outline and holes, one layer `ENGRAVE` with the text as filled paths, and the page sized to the charm plus 5 mm. Written with the existing `buildSingleCharm` extended by `extraLayers` that append a second content stream.
- `back-index.pdf`: a contact sheet of every engraved piece on the sheet, thumbnail, order, SKU, copy, text and size, for the engraver's bench.
- `back-report.json`: `[{ poolId, order, sku, copy, text, font, weight, sizePt, capMm, box, verified, reviewedBy }]`.

Firestore `Charm_Pool_Back/{poolId}`:

```json
{ "poolId": "…", "sheetId": "gold-mu3…", "setId": "set-…", "orderId": "3521337740", "sku": "BR-CMP-01", "copy": 1,
  "text": "Anna\n9.26.25", "font": "Myriad Pro", "weight": "Regular", "sizePt": 5.8, "capMm": 1.9,
  "box": { "xPt": 12.1, "yPt": 30.4, "wPt": 38.0, "hPt": 14.2, "angle": 0 }, "aiPath": "…/back/…ai", "pngPath": "…png",
  "verified": { "ok": true, "res": 6 }, "review": { "legible": true, "notes": "" }, "approvedBy": "auto", "at": 1789… }
```

The sheet record gains `backPool: [poolIds]` and `backOutputs: { pieces[], index, report }`, and the Library card shows an **Engraving · n** badge. A re-nest keeps the back files valid because they are tied to the piece, not to its position on the sheet.

## 8. Sets, labels and completion

### 8.1 A set

One pull of orders produces one **set**: every sheet nested from that pool, across metals, with overflow sheets included. `Charm_Nest_Sets/{setId}` = `{ setId, day, orders[], sheetIds[], labels, status, createdAt }`. The set folder is `charmnest/sets/{day}/{Set-K}/` with the sheet folders linked from the set record (sheets keep their existing folders).

### 8.2 QR labels are generated and saved, not printed

The label content stays exactly what the Design Station produces (one label per bench, base-36 payload, ECC M, 145 × 145 pt, same positions), so the sort scanner keeps working unchanged. The sorter asks the station for the buckets and renders them itself:

```js
async function saveLabelsForSet(set) {
  const p = await DesignLink.call("complete.preview", { receiptIds: set.orders });            // station bucketing, one order → one label
  const doc = await PDFLib.PDFDocument.create(); const files = [];
  for (const job of p.jobs) {
    const png = await qrPng(job.payload, "M", 1024);                                          // qrcodejs, as design-print-1
    const page = doc.addPage([145, 145]); const img = await doc.embedPng(png);
    page.drawImage(img, { x: 3, y: 145 - 3 - 85, width: 85, height: 85 });
    page.drawText(job.label, { x: 1, y: 145 - 93 - 9, size: 9, font: await doc.embedFont(PDFLib.StandardFonts.HelveticaBold) });
    page.drawText("Notes:", { x: 92, y: 145 - 0.5 - 9, size: 9, font: await doc.embedFont(PDFLib.StandardFonts.HelveticaBold) });
    files.push(await uploadBytes(`${set.folder}/labels/${set.name}_label_${job.metal}_${job.partIndex}of${job.partTotal}.png`, png, "image/png"));
  }
  const pdf = await uploadBytes(`${set.folder}/labels/${set.name}_labels.pdf`, await doc.save(), "application/pdf");
  return { setId: set.setId, folder: `${set.folder}/labels`, files: files.map(f => f.path).concat(pdf.path), jobs: p.jobs };
}
```

The label PDF is one page per label exactly as the print page laid it out, so if a label is ever needed on paper it prints from the Library with no other change.

### 8.3 Marking the orders complete

After every sheet of the set is written, verified and saved, and its labels are saved:

```js
const labels = await saveLabelsForSet(set);
await DesignLink.call("complete.commit", { receiptIds: set.orders, labels });
await api("charmNestLibrary", { op: "setUpdate", setId: set.setId, patch: { labels, status: "complete" } });
```

The station runs `commitCompletion`: ledger, unlock, "DESIGNED :)" in each order's chat, archive with the label folder. Orders whose lines went to the Unmatched or Engraving trays are excluded from `set.orders` and stay open on the station, flagged in the sorter, until resolved.

## 9. Sorter UI

New tabs: **Orders**, **Design Station**, **Master**, **Engraving**. Existing: Nest, Library, Charms.

- **Orders** — 5.1. A **Run set** button starts the autonomous run (10).
- **Design Station** — the frame and the bridge console (4.7).
- **Master** — one panel per metal: master file, hash, indexed count, SKU grid with thumbnails and sizes, leftovers in red, per-SKU `engravable` toggle, **Re-index**.
- **Engraving** — the tray (lines awaiting a decision, each with the customer's words, Claude's reading and questions, the best achievable size), the studio (back view with mask hatch, text, weight, size, box, **Verify**, **Approve**, **Skip**), and the back pool per sheet.
- **Live strip** — the last five bridge or engraving events with timestamps, on every tab.

All bridge, master, pool and engraving steps write into the existing agent log with kinds `DS`, `MASTER`, `POOL`, `ENGRAVE`, so the per-card panel and the Log button already show the whole run.

## 10. The autonomous run

```
Run set
  1  orders.snapshot ─────────────── DS: pull, hydrate                          live: "Pulled 41 orders · 63 lines"
  2  classify engraving ──────────── Claude, per line with instructions         tray ← low confidence
  3  poolAdd per line ────────────── master lookup, copies, queues per metal    tray ← unmatched SKU
  4  saturation check ────────────── 72 % ceiling per metal, overflow sheets planned
  5  ui.select on the station ────── DS shows the same orders selected (locks)  frame: rows turn selected
  6  nest per metal ──────────────── parallel search, verify twice, write, save  cards: as today
  7  engrave ─────────────────────── back mask, fit, verify, review, back files  tray ← below legibility floor
  8  labels ──────────────────────── complete.preview → render → save            set folder /labels
  9  complete.commit ─────────────── DS: ledger, unlock, chat, archive           frame: rows leave the list
 10  set record ────────────────────  Charm_Nest_Sets complete
```

Each step logs before and after, can be paused from the Live strip, and stops the run on any refusal from the station. Steps 5 and 9 are the only ones that change the station; both are visible in the frame as they happen.

## 11. Data model

| Collection | Key | Fields |
| --- | --- | --- |
| `Design_Bridge` | sessionId | `sorterClientId, bench, startedAt, endedAt, commands, dropped` |
| `Design_Bridge/{s}/log` | auto | `t, dir, type, ms, payload` (payload slimmed: ids and counts, never order contents) |
| `Charm_Master_Files` | metal | `path, hash, charms, unlabelled[], orphans[], duplicates[], indexedAt` |
| `Charm_Master_Index` | `{metal}_{SKU}` | 6.3 |
| `Charm_Pool` | poolId | 6.4 |
| `Charm_Pool_Back` | poolId | 7.5 |
| `Charm_Nest_Sets` | setId | 8.1 |
| `Charm_Nest_Sheets` | (existing) | `+ setId, orders[], poolIds[], backPool[], backOutputs` |
| `Design_Order_Archive` | (existing) | `+ labels { setId, folder, files[] }` |

Storage: `charmnest/master/{metal}/…`, `charmnest/sets/{day}/{Set-K}/labels/…`, `charmnest/sheets/{day}/{sheet}/back/…`.

## 12. Functions

| Function | Kind | Ops or purpose |
| --- | --- | --- |
| `charmNestLibrary` | existing | `+ masterPutIndex, masterGet, masterList, poolPut, poolList, poolUpdate, backPut, backList, setPut, setUpdate, setGet, bridgeLog` (each a single-field query, no composite indexes, same validation style as `putSheet`) |
| `charmMaster-background` | new | index a large master file server-side: parse, group, label, per-SKU `.ai`, thumbnails; progress in `Charm_Nest_Jobs`; parked payload like the agent |
| `charmEngrave-background` | new | the two Claude calls of 7.1 and 7.4 through `_charmNestAgent.js` with modes `engraveIntent` and `engraveReview`; JSON-schema output, `effort: "high"` |
| `firebaseOrders`, `designArchive` | existing | archive `put` accepts `labels` |

Manifest: add the two functions to `scripts/netlify-function-entries.json`; `build-public.cjs` allowlist gains `vendor/opentype-1.3.4.min.js` and excludes `vendor/fonts/`.

## 13. Settings (sorter)

| Setting | Default | Meaning |
| --- | --- | --- |
| `skuPattern` | `^[A-Z]{2,4}-[A-Z0-9]{2,6}(-[A-Z0-9]{1,4})?$` | what counts as a SKU label |
| `labelGapMm` | 6.4 | how far below an outline a label may sit |
| `engraveMarginMm` | 0.8 | keep-out from every cut edge and cut-out |
| `engraveMinCapMm` | 1.6 | legibility floor |
| `engraveMaxHeightFrac` | 0.40 | text block height cap as a fraction of the charm |
| `engraveConfidence` | 0.80 | below it, a person decides |
| `engraveTryRotated` | on | allow ±15°/±30° layouts when they gain ≥ 12% |
| `autoCommit` | on | mark orders complete on the station when a set finishes |

## 14. Tests

- **Bridge.** Headless Chromium loads the sorter, which frames a local `design-1.html` served on a second port with a stub `firebaseOrders`; asserts `hello`, `orders.snapshot` shape, `ui.select` takes a lock, a wrong-origin message is dropped and counted, `complete.commit` without preview is refused, `complete.commit` with labels removes the rows and writes the ledger.
- **Labels.** A fixture master with labelled charms (text under, one 8 mm below, one shared between two outlines, one unlabelled, one duplicate, one label string that is not a SKU) → expected `labels`, `unlabelled`, `orphans`, `duplicates`.
- **Extraction.** The per-SKU `.ai` re-parsed equals the master's charm (same member count, same silhouette hash).
- **Fit.** Fixture charms with a hole, a thin ring and a diagonal band: text never crosses the eroded mask, the ring returns "no room", the band picks the rotated layout only when it gains ≥ 12%.
- **End to end.** The existing sheet e2e extended: two order lines, one engraved, run the set with a stub Claude; expect the back file, `back-index.pdf`, saved labels, and the station rows gone.

## 15. Implementation plan

1. **Bridge and monitoring** — `Bridge` in `design-1.html`, `commitCompletion` split, `frame-ancestors`, `DesignLink`, the Design Station tab and console, Orders tab with pull. Deliverable: orders pulled and selection driven with both UIs visible.
2. **Master and pool** — text strings in the interpreter, `labelCharms`, Master tab, index and per-SKU files, `poolAdd`, queue naming by order. Deliverable: a pulled line becomes a queued charm.
3. **Engraving** — intent classifier, back mask, `fitText` with opentype.js and Myriad Pro, verification, review, back files and index, Engraving tab, Library badge. Deliverable: an engraved line yields verified per-piece back files tied to its sheet.
4. **Sets, labels, completion** — set record, label rendering and saving, `complete.commit`, archive `labels`, Run set. Deliverable: a finished set marks its orders complete with labels saved, nothing printed.

## 16. Safety rules

- The station's guards stay under remote control: no completion without a preview of the same orders and a saved label list; locks and undo unchanged.
- Metal comes from the station's classifier only; a line whose metal has no sorter card is refused, not guessed.
- Engraving text is the customer's words; Claude may split lines and flag questions, never invent, translate or "improve" them.
- No text below the legibility floor, outside the eroded mask, or inside a cut-out, ever, without a person's approval.
- Master extraction copies bytes; nothing is redrawn.
- Every automatic step logs before and after it acts; the run stops on the first refusal.

## 17. Open points

1. Colour and size variations: when one SKU comes in sizes, is the size part of the SKU in the master (e.g. `BR-CMP-01-S`) or a separate master row? The label rule handles either; the pool key must match the master.
2. Charms with no solid area large enough for 1.6 mm text: skip silently or always ask? Draft assumes always ask.
3. Should the Design Station's own print button be removed once labels are saved by the sorter, or kept for a fallback?
