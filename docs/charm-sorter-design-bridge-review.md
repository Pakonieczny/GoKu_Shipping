# Bridge plan review: gaps, eventualities and their ripple effects

A step-by-step review of `charm-sorter-design-bridge.md` (draft 3). For every step: what the plan assumes, what can happen that it does not handle, what that does to the rest of the process, and the change recommended. Priorities: **P1** would produce a wrong sheet, a wrong engraving or a wrongly completed order; **P2** would stall or duplicate work; **P3** is quality of life.

The review found 41 gaps. Twelve are P1. Seven need a decision from you (section 14).

---

## 1. Session and bridge (plan §3–4)

**1.1 The station reloads or crashes mid-run.** The nonce dies with the page. Commands in flight never get `done`; the sorter times out after 120 s and stops the run with a vague error. *Ripple:* locks taken by remote selection stay set in Firestore until the station's own unload handler runs, which a crash skips; the other bench sees phantom locks.
→ Add a 5-second heartbeat (`ping`/`pong`). On three misses the sorter marks the station down, re-issues `hello` when the frame reloads (the frame's `load` event), and re-drives only idempotent commands. Make every command idempotent (1.3). Station side: on `hello`, release any locks owned by a previous sorter nonce. **P2**

**1.2 Etsy sign-in expires in the station.** `orders.snapshot` fails on the first hydration. The station's OAuth is interactive. *Ripple:* the run stops at step 1; worse, a run that started with a valid token and hydrates 300 orders may hit expiry mid-way and return a half-hydrated list that looks complete.
→ `hello` returns token status and expiry; the sorter refuses to start a run with less than 30 minutes left and shows "Design Station needs Etsy sign-in" with a button that focuses the frame. `orders.snapshot` reports `hydrated: n/m` and the sorter treats a shortfall as a stop, not a result. **P1**

**1.3 Commands are not idempotent; retries duplicate work.** A retried `ui.select` is harmless, but a retried `complete.commit` after a timeout would try to complete already-completed orders, and a retried `poolAdd` creates a second set of copies.
→ Deterministic ids everywhere: pool entries keyed `${receiptId}_${transactionId}_${copy}`, sets keyed by the run id, commands carrying a `runId` and `step` that the station records so a replayed command returns the earlier result. **P1**

**1.4 A person uses the station at the same time.** The frame is the live station; nothing stops someone clicking in it, and the real design-1 tab is a second client with its own lock id. *Ripple:* selection drifts under the sorter; a manual "Complete" can print and complete orders the sorter is mid-way through nesting.
→ In remote mode the station overlays a thin "controlled" veil that blocks pointer events except on Release, and emits a `manual` event if the veil is bypassed. The sorter re-reads `selection` before every step that depends on it. **P2**

**1.5 The other bench takes an order the sorter pulled.** Between pull and `ui.select`, bench 2 can lock an order. `ui.select` then throws for that id and the plan stops the run.
→ Per-line outcome instead of all-or-nothing: `ui.select` returns `{ selected, refused: [{ id, reason }] }`; refused orders drop out of the set with a log line, and the run continues. The set's order list is finalised only at step 5, never at step 1. **P2**

**1.6 Storage partitioning in the frame.** The station in a cross-site iframe would get partitioned localStorage (no Etsy token, no cache). Both hosts are `*.goldenspike.app`, so they are same-site and this does not bite today. It would the moment either app moves to another domain.
→ Note it in the plan as a constraint; add a boot check in the station that reports `storage: "partitioned"` in `hello` if its token store is empty inside the frame while the top-level origin's is not. **P3**

**1.7 Closing the sorter mid-run.** The frame dies with the sorter tab; locks and remote mode linger.
→ `beforeunload` in the sorter sends `release`; the station's `release` unlocks everything owned by the sorter nonce. Persist run state (2.1) so the run can resume. **P2**

**1.8 Security of the command surface.** `complete.commit` is reachable by any page on an allowed origin. The allow-list is two origins, so the risk is low, but `localhost:8888` must never ship in production.
→ Build-time substitution of the allow-list; a production build refuses to contain `localhost`. **P3**

## 2. The run itself (plan §10)

**2.1 No persistent run state.** The plan describes a linear script in the browser. A crash, a closed tab or a long review pause loses where the run was; re-running re-pulls, re-pools and may re-nest.
→ `Charm_Nest_Runs/{runId}` with `step`, per-line states and per-sheet ids, written before and after each step. The Orders tab offers **Resume run**. Every step reads its inputs from the run record, not from memory. **P1**

**2.2 Two sorter tabs, or two PCs.** Two runs on the same orders create two pools and two sets.
→ The run record claims order lines (`claimedBy: runId`); a line claimed by a live run is skipped by another with a log line. The set number per metal per day is allocated in a Firestore transaction on the server, not by counting on the client. **P2**

**2.3 The run cannot pause and resume cleanly at review.** Step 7 waits for the review queue, possibly for hours; meanwhile the station holds locks (1.9 below) and the pull ages.
→ Split the run into two halves with a checkpoint: half A (pull → pool → nest → write sheets) and half B (engraving review → labels → commit). Between them the run is "awaiting review" and everything is on disk. **P2**

**2.4 Locks held for the whole run.** `ui.select` at step 5 locks every order on the station until commit at step 9. With review pauses that can be a working day, during which bench 2 cannot touch those orders even to add a staff note.
→ Do not select on the station until half B. Instead the station gets a lightweight `claim` (a new field on the realtime lock: `claimedBy: "sorter"`, shown as a gold dot, not a lock), and `ui.select` runs immediately before `complete.preview`. **Decision needed** (14.1). **P2**

## 3. Pulling orders (step 1)

**3.1 Orders change after the pull.** A buyer edits personalisation, adds a message, or cancels; Etsy stamps `update_timestamp`. The plan pulls once and never looks again. *Ripple:* an engraving fitted from the old text is approved and cut; a cancelled order is nested, cut and marked complete.
→ Every order in the run carries its `update_timestamp`. Before step 7 (engraving) and again before step 9 (commit), `orders.detail` re-reads each order; a changed timestamp re-runs classification and invalidates any fitted or approved placement for that order; a missing order (shipped, cancelled, refunded) is dropped from the set with a red line and its pieces are flagged on the sheet report as "cut but order gone". **P1**

**3.2 Staff notes and Brites messages are instructions too.** The plan feeds Claude the personalisation and buyer message only. A staff note ("customer phoned: spell it ANNE") or an internal message is exactly where corrections live today.
→ Classification input includes the staff note and the last five Brites messages, with the rule that a staff note overrides Etsy text. The reviewer sees all three side by side. **P1**

**3.3 Lines that are not charms.** Chains, extenders, gift boxes, jump-ring packs and other SKUs have no design. The plan sends them to the Unmatched tray, which will fill with noise on every run.
→ A `Charm_Sku_NoDesign` list (patterns and explicit SKUs) skipped silently with a count in the log; an operator can move an Unmatched line to that list with one click and it never returns. **P2**

**3.4 Bundles and multi-charm listings.** One Etsy listing can be a necklace with two or three charms, each with its own personalisation, under one SKU. The plan maps one SKU to one design and one text.
→ `Charm_Sku_Bom/{sku}` = the component charm SKUs and, for each, which personalisation field it takes (by variation name or index). A line with a BOM expands into its components before pooling; without a BOM and with more than one personalisation value it goes to the tray. **P1**

**3.5 Missing SKU on the transaction.** The station already has a "No SKU" filter because it happens.
→ Resolve by `listing_id` through `Charm_Sku_Aliases/{listingId}` = SKU, learned the first time an operator resolves it in the Unmatched tray. Title matching is never automatic. **P2**

**3.6 Metals the sorter does not cut, and SOLID_UNKNOWN.** A line classified as no metal or "solid, karat unknown" has no card.
→ These go to a **Needs metal** tray with a one-click fix that writes the station's staff note and re-classifies; the order is held (3.8). **P2**

**3.7 Colour variations that are not metals.** Enamel colour, cord colour or stone colour can appear in a "Color" variation; the station's classifier reads Metal/Colour variations first and could bucket "Rose Quartz" as rose.
→ This is a station rule, not a sorter one, and it is the single source of truth. Add to the station's classifier a stop-list of colour words that are not metals and cover it with tests; the sorter shows the variation text beside the metal chip so a reviewer can catch it. **P2**

**3.8 Order completeness.** An order with three lines where one is unmatched or in the tray must not be marked design-complete when the other two are nested.
→ Commit only orders whose every line is either nested (with engraving approved) or on the no-design list. Others stay open on the station and are shown in the sorter as **Held: 1 line unresolved**. The set record lists held orders. **P1**

**3.9 Size variants.** Decision 6 said different text only when type, colour or size differ. The plan assumes size is part of the SKU. If a listing sells one SKU in two sizes as a variation, the master would need a size-specific design.
→ Treat size as part of the SKU when it is; when a size variation exists on a SKU with one master design, hold the line and ask. **Decision needed** (14.2). **P2**

**3.10 Pull volume.** 300 open orders hydrate in about 75 s at 4 requests a second, before images. Acceptable, but the sorter must not pull images at all; it only needs text.
→ `orders.snapshot` never triggers tile image loads; the sorter uses master thumbnails. **P3**

## 4. Master file and index (plan §6)

**4.1 Labels converted to outlines.** Illustrator users routinely convert text to outlines before sharing. Then there are no text objects, the label rule finds nothing, and every charm is "unlabelled".
→ Two fallbacks in order: read the `/ActualText` or `/Alt` marked content if present; otherwise render the strip under each charm and have Claude read the SKU (vision, structured output, confidence), with any read under 0.95 or failing the pattern going to the Master tab for confirmation. The tab always shows the label crop beside the charm so a person can verify. **P1**

**4.2 Font encodings.** Text objects in CID fonts (Identity-H) carry glyph ids, not characters; without the font's `ToUnicode` CMap the string is unreadable.
→ The interpreter reads `ToUnicode` when present and falls back to 4.1 when not. **P2**

**4.3 Label placement variants.** Labels rotated with the charm, labels above, two-line labels ("BR-CMP-01 / small"), a label between two charms in a tight grid.
→ Widen the rule: accept a label whose nearest outline edge is the bottom or the top within the gap, prefer bottom; join adjacent text runs on one baseline; when two outlines tie within 2 pt, mark ambiguous rather than pick. **P2**

**4.4 Master drawn at the wrong scale or unit.** Nothing checks that a charm is the size it will be sold at.
→ The index stores size; the Master tab flags any charm outside the sizes the shop sells (setting: min and max mm) and any SKU whose size changed by more than 5% on re-index. **P2**

**4.5 Master changes over time.** A SKU redrawn in the master invalidates cached silhouettes and back masks; pool entries created earlier point at the old `charmHash`.
→ Index entries are versioned by `masterHash`; a pool entry records the `charmHash` it was made from; a run that finds a newer index for a pooled SKU asks before re-pooling. Old per-SKU files are never deleted. **P2**

**4.6 Open paths and detached rings in the master.** Today an open path is excluded from nesting and a detached ring is merged by Claude's review; in a master these must be fixed at the source, not silently.
→ The Master tab lists them as errors with the crop, and a SKU with an open outline is not indexed. **P2**

**4.7 Which way is up.** The master gives the charm an orientation; the back engraving needs a baseline direction. The plan does not define "up".
→ Up is the direction from the charm's centroid to its hanging hole (the smallest closed cut-out nearest the outline's edge, or the ring merged by review); if there is no hole, the master's drawn orientation. Stored on the index entry as `upAngle`, editable in the Master tab. Text baselines are perpendicular to up. **P1**

**4.8 Charms larger than a sheet or than the 72% ceiling.** Nothing stops a pool entry that can never be placed.
→ At pool time compare the charm's inflated area to the usable area times the ceiling; oversize goes to a tray, never to a queue. **P2**

## 5. Pool and queues (step 3)

**5.1 The same line pulled again before completion.** Tomorrow's pull sees the same open orders. Without state the sorter pools them twice.
→ Line state machine in `Charm_Pool` keyed deterministically (1.3): `ready → nested → written → approved → committed`; a pull shows each line's state and pools only `new`. **P1**

**5.2 Quantity across sheets.** Two copies can land on two sheets (overflow). The label rule "one order, one label" then puts the order on one bench's label while its pieces are on two sheets.
→ See 8.1. The pool records `sheetId` per copy; the set record lists, per order, every sheet it touches. **P2**

**5.3 Naming collisions.** Charm names carry the order number; two lines of the same SKU on one order yield identical names.
→ Name is `${order} · ${sku} · ${transactionId.slice(-4)} · ${copy}/${qty}`. **P3**

## 6. Nesting and writing (step 6)

**6.1 Partial nests.** Overflow handles "does not fit under the ceiling", but a run must not commit while any sheet is partial for another reason (stopped, verification flagged).
→ Half A ends only when every sheet in the set is `complete` and verified; otherwise the run is "needs attention" with the failing sheet named. **P1**

**6.2 Re-nesting after approval.** A re-nest changes sheet membership; back files are per piece so they survive, but the set's sheet list and the labels do not.
→ Re-nesting a sheet inside a committed set is refused; inside an uncommitted set it regenerates the set's sheet list and marks labels stale. **P2**

**6.3 Day boundary.** `today()` uses the UTC date and `dateTag()` the local date; a run that crosses 8 pm Eastern gets folders under one day and names under another. This is already true of the current sorter.
→ One clock: local date everywhere, from a single `localDay()` helper. **P2**

## 7. Engraving (step 7)

**7.1 Texts Myriad Pro cannot set.** Accents are fine; Greek, Cyrillic, Hebrew, Arabic, CJK, emoji and hearts are not in Myriad Pro. The plan would either substitute a `.notdef` box or throw.
→ Glyph coverage check before fitting; any unsupported character sends the piece to the tray with the characters named. A small approved fallback for symbols (heart, star, infinity) drawn as vector shapes at cap height, if the shop wants them. **Decision needed** (14.3). **P1**

**7.2 Long texts.** A sentence ("Happy 30th birthday, love always, Mum and Dad") on a 12 mm charm fits only at unreadable sizes.
→ The fitter tries up to four lines with Claude's suggested breaks; below the small flag it still returns the largest size, and the review shows a "consider shortening" note with Claude's suggested shorter form for the reviewer to accept or ignore, never applied automatically. **P2**

**7.3 Customer asks for the front, both sides, a specific font, handwriting, or an image.** Policy is back only, Myriad only.
→ Classification schema gains `requests: { side, font, handwriting, image }`; any non-default request goes to the tray so a person decides and, if needed, messages the customer through the station's chat. **P2**

**7.4 Is the file mirrored the right way for the engraver?** The plan draws the back view (mirrored). Some engraving software expects the design as seen from the engraver's camera; others expect front-view coordinates and mirror themselves. Getting this wrong engraves mirror-image text.
→ One test piece per engraver setup, then a per-engraver setting `backFileView: "asSeenFromBack" | "frontCoordinates"`. Both are trivially produced from the same layout. **Decision needed** (14.4). **P1**

**7.5 Orientation in the engraver's jig.** Pieces are placed by hand after cutting; the file's "up" must match how the piece sits in the jig (usually hoop up).
→ Per-piece files are normalised so `upAngle` points to the top of the page, and the contact sheet shows the hoop at the top. **P2**

**7.6 Depth, stroke and spacing limits.** The 0.15 mm stroke floor is a placeholder; engravers also have a minimum gap between strokes and a maximum fill area for heat.
→ Settings per engraver: minimum stroke, minimum gap, maximum text height; the fitter enforces gap by tracking (letter-spacing) at small sizes. Values from a test coupon. **P2**

**7.7 The back is not always plain.** Some designs have detail on both sides, or a hallmark stamp area, or a domed back.
→ Index entry gains `backKeepOut` (optional rectangles or paths drawn on a `BACK KEEP-OUT` layer in the master); the mask subtracts them. Domed or textured backs are marked `engravable: false` at the SKU. **P2**

**7.8 Review throughput.** A set with 40 engraved copies means 40 approvals. Identical copies of one line (quantity 2, same text) should be one review.
→ Review is per distinct (SKU, text, box) tuple; approving applies to all copies with the count shown. Keyboard: A approve, S skip, arrows nudge. **P3**

**7.9 Reviewer identity.** The sorter has no employee name today.
→ Reuse the station's `employee_name` (delivered in `hello`) and require it before approving. **P2**

**7.10 Approved, then the order changes.** Covered by 3.1: a change after approval invalidates the approval and the piece returns to the queue with a diff of the old and new text.

## 8. Labels (step 8)

**8.1 One order, one label, but pieces on several sheets.** The station's legacy rule deals a mixed-metal order to one bench's label. With saved labels per set, the label is a manifest of a bench's sheet. A gold-and-silver order would be on the gold label only, and the silver sorter would not know it has a piece to find.
→ Change the rule for saved labels: one label per sheet listing every order with a piece on that sheet; an order on two sheets appears on both. The QR payload format is unchanged (`B36|metal|…`), so the sort scanner keeps working, and the station's bucketing is bypassed for labels (it is still used for `complete.preview`'s validation). The set folder holds `labels/{sheet}_label.png`. **Decision needed** (14.5). **P1**

**8.2 Nobody prints, but the scanner needs something to scan.** The plan saves PNG and PDF. The sort station needs the code in front of the camera.
→ The Library's sheet record shows the label full-screen on demand (the scanner reads a screen fine), and the label PDF prints from the Library if paper is wanted. **P3**

**8.3 Payload size.** A sheet of 40 pieces from 40 orders is well under the 1,000-character limit; a 12 × 6 in stock with small charms could exceed it.
→ Keep `safeChunks`; a sheet may have two label parts, shown as `[1/2]`. **P3**

## 9. Completion (step 9)

**9.1 Meaning of "design-complete" changes.** Today it means labels printed. After the bridge it means sheets written, engraving approved, labels saved. Downstream stations and the archive read the same flag.
→ Archive record gains `completedBy: "sorter"`, `setId`, `sheetIds`, `backCount`, so History can tell a sorter completion from a manual one. **P3**

**9.2 Partial commit.** The station may refuse some ids (stale, locked). The plan treats the reply as all-or-nothing.
→ `complete.commit` returns `{ completed, refused[] }`; refused orders stay in the set as held with the reason; the set is `complete-with-holds` until they clear. **P2**

**9.3 Undo.** The station's undo restores the last batch; the sorter has no undo of a set.
→ `Undo set` in the sorter: `complete.undo` on the station, set status back to `awaiting review`, pool lines back to `written`. Files stay. **P3**

## 10. Cloud, functions and data

**10.1 Offline.** Nesting works offline today; a run cannot (pool, sets, engraving calls, labels). 
→ Run refuses to start offline with a clear reason; half-finished runs resume when the cloud is back (2.1). **P2**

**10.2 Firestore security rules.** The functions use the admin SDK, but the station's chat writes to Firestore directly from the client. New collections must not be client-writable.
→ Rules: all `Charm_*` and `Design_Bridge` collections deny client writes; reads through functions only. **P2**

**10.3 Background function limits.** Master indexing of a 500-charm file with Claude review and 500 thumbnails is minutes of work; the background function limit is 15 minutes and the payload 256 KB (already parked in Storage).
→ Index in pages of 50 charms with progress in `Charm_Nest_Jobs`; resumable by page. **P2**

**10.4 Etsy quota.** Hydration, plus detail re-reads before engraving and commit, is three reads per order per run.
→ Use the station's `update_timestamp` cache: a re-read is a receipt list call, not a per-order call, unless the timestamp moved. **P3**

**10.5 Retention.** Master copies, pool files, back files and labels accumulate.
→ Lifecycle rule on Storage: pool working files 90 days; sheets, backs and labels kept; masters kept. **P3**

## 11. UI and monitoring

**11.1 The Live strip is not enough for a paused run.** A run waiting on a tray or a review for an hour needs a visible, persistent state.
→ A run banner across the sorter: "Run set 14 · awaiting engraving review (3) · 2 held orders · Resume". Desktop notification when the run needs a person, reusing the existing notify setting. **P2**

**11.2 The frame is small.** A 344 px station rail inside a half-width tab is hard to read.
→ The Design Station tab is full-width with the console as a collapsible drawer; the frame gets the station's compact rail density via `?rail=sm`. **P3**

**11.3 Nothing tells the sorter what the station shows.** `ui.modal` events cover dialogs, but not scroll or which rows are visible.
→ Not needed for control; skip. **P3**

## 12. Testing gaps

The plan's tests miss: order change between pull and commit (3.1); staff-note override (3.2); BOM expansion (3.4); outlined labels via the vision fallback (4.1); up-angle detection (4.7); glyph coverage refusal (7.1); mirror direction against a known-good engraver file (7.4); label-per-sheet for a mixed-metal order (8.1); resume after a simulated crash at each step (2.1); two runs contending for one line (2.2).

## 13. Recommended changes to the plan, in order

1. Persistent run record with deterministic ids and resume (1.3, 2.1, 5.1).
2. Order re-validation before engraving and before commit; order completeness gate (3.1, 3.8).
3. Instruction sources: staff note and messages, override rule (3.2).
4. BOM, no-design list and listing aliases (3.3–3.5).
5. Label rule per sheet, not per order (8.1).
6. Master robustness: outlined labels, encodings, scale check, up-angle, keep-outs (4.1, 4.2, 4.4, 4.7, 7.7).
7. Engraving safety: glyph coverage, mirror setting, jig orientation, engraver limits (7.1, 7.4, 7.5, 7.6).
8. Bridge resilience: heartbeat, per-line refusals, veil, release on unload, claims instead of long locks (1.1, 1.4, 1.5, 1.7, 2.4).
9. Clock, rules, retention, offline (6.3, 10.1, 10.2, 10.5).

## 14. Decisions needed

1. **Locks during a run.** Hold station locks from the pull to the commit (bench 2 cannot touch those orders for hours), or use a light "claimed by sorter" mark and lock only at commit?
2. **Sizes.** Is size always encoded in the SKU, or can one SKU be sold in several sizes with one master design?
3. **Characters outside Myriad Pro.** Refuse to the tray only, or allow an approved set of vector symbols (heart, star, infinity)?
4. **Engraver file orientation.** Which does your engraving software expect: the design as seen from the back, or front coordinates that it mirrors itself? One test piece settles it.
5. **Labels.** Move from "one order, one label" to "one label per sheet listing every order on it"? This is what makes a mixed-metal order findable on both benches.
6. **Held orders.** When one line of an order is unresolved, hold the whole order (recommended) or complete the resolved lines and leave the rest open?
7. **Who reviews.** Should engraving approval be restricted to named employees, and should the approver be someone other than the person who set the words?
