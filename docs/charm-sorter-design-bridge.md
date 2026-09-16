# Charm Sorter ⇄ Design Station bridge

Design and implementation document. Status: draft for review. Owner: production tooling.

## 1. Goal

The Charm Nesting Station ("the sorter", `charm-nest-1.html`) becomes the master of the Design Station (`design-1.html`, "the slave"). Without an operator touching the Design Station, the sorter can:

1. pull the open orders the Design Station sees, with each line's SKU, metal bench, quantity and the customer's instructions;
2. drive any Design Station action (select, filter, open an item, complete and print labels) and watch it happen live;
3. find each ordered SKU in a master Illustrator file, lift that charm out with every layer, path, colour and dimension intact, and drop it into a pool of designs ready to nest;
4. for any line that needs back engraving, show the charm's back (a 180° turn about its vertical axis), have the AI read the customer's request, set the text in the best place and size on the back, and keep the result in a separate pool tied to the sheet the order is on;
5. show, in both consoles, exactly what is happening in real time.

Everything here builds on what already exists: the sorter's parser, silhouettes, nesting, verification, per-sheet cloud folders and agent log; the Design Station's single metal classifier, attention flags, selection locks, print handshake and archive; the shared Firestore project.

## 2. Vocabulary

| Term | Meaning |
| --- | --- |
| Order line | One Etsy transaction: order number, SKU, quantity, metal key, personalisation, buyer message |
| Metal key | `gold`, `silver`, `rose`, `10k`, `14k` as the Design Station classifies them; the sorter maps them to its own `gold`, `silver`, `rose`, `gold10k`, `gold14k` |
| Master file | One `.ai` per metal (or one for all) that holds every charm design, each labelled with its SKU |
| Master index | The SKU → charm lookup built from a master file |
| Pool | The set of extracted charm designs waiting to be nested, one entry per order line and copy |
| Back pool | The engraving designs for a sheet, one per engraved copy, kept with that sheet |
| Bridge | The command and event channel between the two apps |

## 3. Architecture

```
┌──────────────────────────────┐   postMessage (iframe, origin-checked)   ┌──────────────────────────────┐
│  Charm Sorter (master)       │ ───────────────────────────────────────▶ │  Design Station (slave)      │
│  brites-charm-sorter…        │ ◀─────────────────────────────────────── │  design-1.goldenspike.app    │
│  · Orders panel              │   events: state, orders, ui, done        │  · Remote mode banner        │
│  · Twin console (live view)  │                                          │  · Command runner            │
│  · Master index / pool       │            Firestore (shared)            │  · Emits every UI change     │
│  · Engraving studio          │ ◀──────── Design_Bridge, Charm_Pool ────▶│                              │
└──────────────────────────────┘                                          └──────────────────────────────┘
                 │                                                                        
                 ▼                                                                        
   Netlify functions: charmNestLibrary (+ pool ops), charmNestOutput, charmNestAgent-background,
   new: charmMaster (index a master file), charmEngrave-background (text interpretation + review)
```

### 3.1 Two channels, deliberately

**Live control: an embedded frame.** The sorter opens the Design Station in an `<iframe>` inside its own page ("Design Station" tab). The two origins differ, so control goes over `window.postMessage` with a strict origin check on both sides and a per-session nonce, the same pattern the Design Station already uses with its print page. Embedding is what makes item 5 free: the operator sees the real Design Station UI reacting inside the sorter, while the sorter's own console narrates every command and reply.

**Record and cross-machine control: Firestore.** Every command and every reply is also written to `Design_Bridge/{sessionId}/log`. This gives an audit trail, lets a second screen watch, and lets the sorter drive a Design Station running on another machine (the Design Station listens to `Design_Bridge` commands addressed to its bench when it is not embedded). Firestore realtime listeners already exist in both apps (locks, chat), so no new infrastructure is needed.

The Design Station must allow itself to be framed by the sorter's origin: add `Content-Security-Policy: frame-ancestors 'self' https://brites-charm-sorter.goldenspike.app` for `design-1.goldenspike.app` in `netlify.toml` (it is currently unframed).

### 3.2 Remote mode in the Design Station

When a bridge session is active the Design Station shows a gold banner across the top: "Controlled by Charm Sorter · session 3f9a · 14 commands · last: select 3521337740", with a **Release** button that ends remote control from the slave side. Every command the station executes is also echoed into its own activity ticker, so an operator standing at the bench sees the same story the sorter shows. Locks taken by remote selection are owned by the sorter's client id, so the other bench sees them as "being worked on at another station" exactly as today.

## 4. The bridge protocol

All messages are JSON: `{ source, nonce, id, type, … }`. `source` is `"brites-sorter"` or `"brites-design"`. `id` is a per-command counter; every command gets exactly one `ack` and one `done` (or `error`), plus zero or more `progress` events. Unknown types are ignored, never executed.

### 4.1 Commands (sorter → Design Station)

| Command | Arguments | Effect | Reply payload |
| --- | --- | --- | --- |
| `hello` | `origin, nonce, sorterClientId` | Opens the session; station replies with its bench, version, employee name, counts | `state` |
| `orders.snapshot` | `{ metals?, onlyAttention?, sinceTs? }` | Full list of open orders with hydrated lines (fetches missing details through the station's own paced Etsy queue) | `orders[]` (see 4.3) |
| `orders.detail` | `{ receiptId }` | One order's full lines and receipt-level notes | `order` |
| `orders.watch` | `{ on }` | Station streams `orders.changed` after each of its refreshes | — |
| `ui.select` / `ui.deselect` | `{ receiptIds[] }` | Same as clicking rows; takes the realtime lock | `selection` |
| `ui.filter` | `{ metals?, attn?, due?, free?, … }` | Sets the filter model `F` | `filters` |
| `ui.openItem` | `{ transactionId }` | Opens the listing modal for that tile (image, personalisation, buyer message, SKU) | `item` |
| `ui.scrollTo` | `{ receiptId }` | `revealOrder` | — |
| `notes.set` | `{ receiptId, text }` | Writes the staff note | — |
| `chat.post` | `{ receiptId, text }` | Posts an internal Brites message as the sorter | — |
| `complete.preview` | `{ receiptIds[] }` | Runs the bucketing and shows the QR preview (nothing written) | `preview` (jobs, skipped, solid, mixedTarget) |
| `complete.print` | `{ confirm: true }` | Runs the print handshake and commits exactly as the button does | `printed` |
| `complete.undo` | — | `handleUndoComplete` | — |
| `release` | — | Ends remote mode | — |

Rules the station enforces regardless of who is driving: a command that would print without a preview is refused; `complete.print` is refused while a grouping review or an Etsy sweep is in flight; commands from an origin other than the sorter's are dropped.

### 4.2 Events (Design Station → sorter)

`state` (bench, counts, employee, locks), `orders.changed`, `selection`, `filters`, `ui.modal` (which modal is open and for what), `activity` (every ticker line the station prints), `print.progress`, `print.done`, `error`. The sorter renders `activity` lines into its agent log with a "DS" prefix, so one log tells the whole story.

### 4.3 The order line shape

```json
{
  "receiptId": "3521337740", "orderNumber": "3521337740", "shipBy": 1789200000,
  "buyer": { "name": "…", "country": "US" },
  "attention": true, "staffNote": "…", "hasMessages": true,
  "lines": [{
    "transactionId": "4412…", "sku": "BR-CMP-01", "title": "Compass charm necklace",
    "quantity": 2, "metalKey": "gold", "metalLabel": "Gold",
    "personalization": ["ANNA 9.26.25"], "buyerMessage": "please engrave on the back",
    "variations": [{ "name": "Metal", "value": "14k Gold Filled" }, { "name": "Chain", "value": "18 in" }]
  }]
}
```

The station builds this from what it already has: `slimTx`, `decorateReceipt`, `txMetalResolved`, `txNeedsAttention`, the personalisation lift in `pullEtsyOrderDetails`. Nothing is re-classified in the sorter; the station's classifier stays the single source of truth for metal.

## 5. Master file and SKU lookup

### 5.1 What a master file must contain

One `.ai` (PDF-compatible) per metal, or one for all metals, holding every charm design. Each charm is labelled with its SKU in one of two ways, checked in this order:

1. **Layer name.** The charm's top-level layer or group is named with the SKU (`BR-CMP-01`). The parser already reads form XObjects and optional-content groups; layer names come from the `/OCProperties` order and `/Title` entries.
2. **Text label.** A text object whose string is a SKU pattern, placed inside or within 6 pt of the charm's outline. The parser already records text segments with bounding boxes and character counts; the grouping step already assigns segments to the nearest outline.

The SKU pattern is a setting (default `^[A-Z]{2,4}-[A-Z0-9]{2,6}(-[A-Z0-9]{1,4})?$`). A charm with no label, or two charms with the same label, is reported in the index review and never used silently.

### 5.2 Building the index

`charmMaster` (new Netlify background function, parked payload like the agent) or, first, the browser:

1. Parse the master file with `CharmNestPDF.parseSource` and group it with `groupCharms` exactly as a loose sheet is parsed today. Labels are never members of a charm's cut geometry: text segments matched by the SKU pattern are recorded on the charm and excluded from its silhouette.
2. For each charm: SKU, silhouette hash, width and height in mm, member count, holes, thumbnail. Claude's grouping review runs as it does for a normal sheet, so a ring detached from its charm is merged before it is indexed.
3. Write `Charm_Master_Index/{metal}_{sku}` = `{ sku, metal, masterPath, masterHash, charmHash, widthPt, heightPt, areaPt2, members, thumbUrl, aiUrl, indexedAt }`, and store a per-charm `.ai` at `charmnest/master/{metal}/{sku}.ai` built with `buildSingleCharm`, which copies the charm's original content-stream bytes with everything else blanked. Layers, stroke widths, colours and dimensions are therefore untouched.
4. The Master view in the sorter shows the index as a grid: every SKU, its thumbnail, size and metal, and the unlabelled or duplicated leftovers in red. Re-indexing a master file replaces its entries; older sheets keep the copies they were built from.

### 5.3 Pulling a charm for an order line

`pool.add(line)`:

1. Look up `Charm_Master_Index/{metal}_{sku}`. If the metal-specific entry is missing, fall back to `any_{sku}`; if nothing matches, the line goes to the **Unmatched** tray with the SKU, title and thumbnail from Etsy so an operator can pick a charm or fix the master.
2. Fetch the stored per-charm `.ai` (or extract it live from the master in the browser if the cloud is offline).
3. Create one pool entry per copy (`quantity` copies): `Charm_Pool/{poolId}` = `{ orderId, transactionId, sku, metal, copy: 1..qty, charmHash, aiPath, engraving: null | {…}, state: "ready" | "nested" | "cut", sheetId: null }`.
4. Load the entry into the sorter's queue for that metal card, with the order number as the charm name (`3521337740 · BR-CMP-01 · 1/2`), so the nest report and the labelled proof carry the order.

The metal map from Design Station keys to sorter keys is one table, `gold→gold, silver→silver, rose→rose, 10k→gold10k, 14k→gold14k`.

## 6. Engraving

### 6.1 Which lines need it

A line is marked `engrave: true` when any of these hold:

- the personalisation field is non-empty (the Design Station has already lifted it and cleared "Not requested");
- the buyer message or personalisation contains an engraving intent. Detection is done by Claude (the existing intent-classifier pattern in `etsyMailIntentClassifier.js`) with a strict schema: `{ engrave: bool, side: "back" | "front" | "unknown", text: string, notes: string, confidence }`. Keywords alone are not trusted: "please don't engrave" must come out `false`;
- the SKU's master entry is flagged `engravable: true` and the listing has a personalisation variation.

Any line with `confidence < 0.8` or `side: "unknown"` is queued for a human decision in the sorter's **Engraving** tray before anything is drawn. Nothing is guessed onto a customer's charm.

### 6.2 Showing the back

The back of a charm is its front mirrored across its vertical axis: a 180° rotation about Y. In the flat file that is the transform `[-1 0 0 1 (2·cx) 0]` applied to every member path, with the hole geometry and the outline preserved. The sorter's preview draws the back with the front's fills at 25% so the operator can see where engraving detail on the front sits, and the outline and holes in full. Text placed on the back is placed on this mirrored view, so what the operator sees is what the engraver sees.

### 6.3 Placing the text

Two parts: the AI interprets, the geometry places.

1. **Interpretation (Claude, structured output).** Input: the customer's words, the SKU, the charm's back silhouette rendered with a mm grid, the usable area in mm, house rules (fonts allowed, minimum stroke, symbols allowed). Output: `{ lines: ["Anna", "9.26.25"], font: "…", style: "script" | "serif" | "sans", align, emphasis, warnings }`. It decides what the text is and how it reads, never where it goes or how big it is.
2. **Placement (deterministic).** The back silhouette is rasterised at 6 px/pt (the verifier's resolution). The engraving margin (setting, default 0.8 mm) erodes the silhouette and every hole. The largest inscribed rectangles are computed from the eroded mask (the pocket finder the sorter already has). Text is set as vector outlines with opentype.js from a font on the allowed list, so the file carries paths, never fonts. For each candidate rectangle the fitter runs a binary search on point size so the longest line fits the width and the stack fits the height, then keeps the layout with the largest size subject to a minimum (setting, default 1.6 mm cap height) and a maximum (default 40% of the charm height). Ties prefer the rectangle whose centre is nearest the charm's visual centre. Lines are centred unless the interpretation asked otherwise.
3. **Verification.** The rendered text is rasterised and checked against the eroded back mask: zero ink outside it, zero ink inside a hole. Claude then reviews the rendered back once for readability and taste with a yes/no plus notes; a no returns to the tray with the notes attached, it never auto-edits.

### 6.4 The back pool and its sheet

Back-engraving designs never enter the cut pool. They live in `Charm_Pool_Back/{poolId}` = `{ poolId, sheetId, orderId, sku, copy, text, font, sizePt, box, aiPath, pngPath, verifiedAt, reviewedBy }`, and are always tied to the sheet that holds the front: when a front pool entry is nested and its sheet is written, the sheet folder gains `back/` with:

- `{sheetName}_back.ai`: one page the size of the sheet, every engraved charm drawn mirrored at exactly the front's placement mirrored across the sheet's vertical centre line, with a layer per charm (`{order} · {sku} · back`) and the text on its own sub-layer. Engravers who flip the whole sheet get every back in register.
- `{sheetName}_back_{order}_{sku}_{copy}.ai`: one file per engraved charm, the charm's back with its text, for engravers who work piece by piece.
- `back-report.json`: every engraving with its text, size, box and verification result.

The sheet record (`Charm_Nest_Sheets`) gets `backPool: [poolIds]` and `backOutputs: { sheet, pieces[] }`. The Library shows an **Engraving** badge with the count and opens the back files beside the front ones. A re-nest that moves a charm invalidates that charm's back placement and regenerates the back sheet.

## 7. The sorter's UI

New tabs on the sorter's top bar: **Orders**, **Design Station**, **Master**, **Engraving**. Existing: Nest, Library, Charms.

- **Orders.** The pulled order lines, grouped by metal, each row showing order number, SKU, quantity, attention flag, engraving flag, pool state. Buttons: Pull open orders, Add to pool (selected or all under the fill estimate), Show in Design Station (drives `ui.scrollTo` and `ui.openItem`).
- **Design Station.** The embedded frame at full height with the bridge console beside it: session, command count, the live event stream, and a **Take control / Release** switch. This is the monitoring the request asks for: the real UI on the left, the narrative on the right.
- **Master.** Master files per metal, index status, the SKU grid, unlabelled leftovers, re-index.
- **Engraving.** The tray of lines needing a decision, the studio for each (back view, text, size, font, box, verify, approve), and the back pool per sheet.

The agent log on every card already narrates the sorter's own steps; bridge events and engraving steps flow into the same log with `DS` and `ENGRAVE` kinds so one panel reads end to end. A **Live** strip at the top of the sorter shows the last five bridge events with timestamps.

## 8. Data model additions

| Collection | Key | Fields |
| --- | --- | --- |
| `Design_Bridge` | sessionId | `sorterClientId, bench, startedAt, endedAt, commands, lastCommandAt` |
| `Design_Bridge/{s}/log` | auto | `t, dir: "cmd"|"evt", type, payload, ok, ms` |
| `Charm_Master_Files` | `{metal}` | `path, hash, indexedAt, charms, unlabelled[], duplicates[]` |
| `Charm_Master_Index` | `{metal}_{sku}` | see 5.2 |
| `Charm_Pool` | poolId | see 5.3 |
| `Charm_Pool_Back` | poolId | see 6.4 |
| `Charm_Nest_Sheets` | (existing) | `+ orders[], poolIds[], backPool[], backOutputs` |

Storage: `charmnest/master/{metal}/{sku}.ai`, `charmnest/master/{metal}/{hash}.ai` (the master itself), `charmnest/sheets/{day}/{sheet}/back/…`.

## 9. Functions

| Function | Kind | Purpose |
| --- | --- | --- |
| `charmNestLibrary` | existing | add ops `pool.put/list/update`, `backPool.put/list`, `master.putIndex/getIndex/listFiles`, `bridge.log` |
| `charmMaster` | new, background | index a master file server-side for large files (parse, group, per-SKU `.ai`, thumbnails) with progress in `Charm_Nest_Jobs` |
| `charmEngrave-background` | new, background | the two Claude calls of 6.3 (interpretation, review) with the parked-payload pattern already used by `charmNestAgent-background` |
| `firebaseOrders` | existing | no change; the station keeps writing locks, notes, chat and completion as today |

## 10. Implementation plan

**Phase 1 · Bridge and monitoring (Design Station + sorter).** Add the command runner and event emitter to `design-1.html` (one module, `Bridge`, wrapping existing functions: `selectRow`, `deselectRow`, `applyMetalFilter`, `openListingModal`, `revealOrder`, `handleCompleteClick`, `proceedToPrint`, `handleUndoComplete`). Add the remote banner and the activity echo. Add `frame-ancestors`. In the sorter: the Design Station tab with the frame, the console, `orders.snapshot` into the Orders tab. Deliverable: pull orders and drive selection with both UIs visible.

**Phase 2 · Master index and pool.** SKU label detection in `charm-nest-pdf.js` (`labelsOf(charm)`), the Master tab, `Charm_Master_Index`, per-SKU `.ai` extraction, `pool.add`, queue naming by order. Deliverable: an order line becomes a queued charm with one click, or automatically for every pulled line.

**Phase 3 · Engraving.** Intent classification, the back view, the text fitter (opentype.js vendored under `vendor/`), verification, review, the back pool and back sheet outputs, Library badge. Deliverable: an engraved line yields verified back files tied to its sheet.

**Phase 4 · Autonomy.** A run mode: pull → pool → nest per metal under the 72% ceiling with overflow sheets → write front and back → save → `complete.preview` and `complete.print` on the Design Station for the orders on the finished sheets → labels. Every step is visible, every step can be paused, and printing always waits for the print page's confirmation as it does today.

Tests: a fixture master file with labelled charms (layer-name and text-label variants, one unlabelled, one duplicate); a bridge test that drives the real `design-1.html` in headless Chromium through the frame; a fitter test on the fixture charms (text never crosses the eroded mask); the existing sheet e2e extended with an engraved line producing a back sheet in register.

## 11. Safety rules carried over

- The Design Station's own guards stay in force under remote control: no print without preview, no completion before the print page confirms, locks and undo unchanged.
- The sorter never re-classifies metal; a line whose station metal has no sorter card is refused, not guessed.
- Nothing is engraved below the confidence threshold or with an unknown side without a human choice.
- Master extraction copies bytes; it never redraws a charm.
- Every automatic step writes a log line before and after it acts.

## 12. Open questions

1. **Master file labelling.** Are SKUs in the master `.ai` layer names, text next to the charm, or both? A sample master file per metal would settle the parser rules (section 5.1).
2. **Engraving side.** Is engraving always on the back unless the customer says otherwise, or are some SKUs front-engraved?
3. **Engraving fonts and limits.** Which fonts are allowed, and what are the minimum letter height and margin your engraver needs? Section 6.3 uses 1.6 mm and 0.8 mm as placeholders.
4. **Back registration.** Does the engraver flip the whole sheet (one back file in register) or engrave pieces individually (one file per charm)? The design produces both; if only one is used the other can be dropped.
5. **Who triggers completion.** Should the sorter mark orders design-complete and print labels when a sheet is finished (phase 4), or should that remain a person's click at the Design Station?
6. **Quantity rule.** Does an order line with quantity 2 always mean two identical charms, or can each copy carry different personalisation text?
7. **Cross-machine control.** Will the Design Station driven by the sorter be on the same screen (embedded) or on a different bench PC (Firestore command path)?
