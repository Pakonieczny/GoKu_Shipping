# Charm Nesting Station 1 — `charm-nest-1.html`

Production URL: `https://brites-charm-sorter.goldenspike.app` (redirect in `netlify.toml` → `/charm-nest-1.html`).

Takes loose charm artwork — `.ai` (PDF-compatible), `.pdf`, or a `.zip` of them — and nests every charm onto fixed sheets per metal (GF 14/20, SS, RG 14/20, 10K solid gold, 14K solid gold) with no overlaps, then writes a production `.ai` per sheet with one Illustrator layer per charm. Everything uploaded and everything produced gets a permanent copy in Firebase.

## Files

| File | Role |
|---|---|
| `charm-nest-1.html` | The console: three sheet cards, queue rail, live preview, saturation readout, Library (recovery) and Charms views, settings, report, charm inspector |
| `charm-nest-solver.js` | The nesting solver. Shared byte-for-byte between the browser Worker and the Netlify background function |
| `charm-nest-worker.js` | Dedicated Web Worker per sheet; streams every placement to the page |
| `charm-nest-pdf.js` | Content-stream parser, charm grouping, silhouettes, `.ai` writer, render-based verifier |
| `vendor/pdf-lib-1.17.1.min.js`, `vendor/pdfjs-4.10.38/*.mjs`, `vendor/jszip-3.10.1.min.js` | Vendored (the site runs under COEP `require-corp`; no CDN) |
| `netlify/functions/charmNestLibrary.js` | Firestore: charm library, sheet records, calibration, server-job records |
| `netlify/functions/charmNestName.js` | Claude Opus 5, high effort (`CHARM_NEST_NAME_EFFORT` overrides), structured output → layer names (and metal suggestions for unassigned files) |
| `netlify/functions/charmNestReview.js` | The agent loop: Claude Opus 5 (high effort, structured output) reviews the detected charms against a numbered rendering of the source page (merges fragments, excludes junk, flags boxes holding two charms) and inspects the finished sheet (one automatic fix-and-re-nest round) |
| `netlify/functions/charmNestOutput.js` | Firebase Storage: signed-URL uploads, base64 fallback, tokenised download URLs |
| `netlify/functions/charmNestSolve-background.js` | Server-side solver fallback (15-minute budget, progress via Firestore) |
| `netlify/functions/_charmNestAuth.js`, `_charmNestSolver.js` | Shared door (same contract as `authGate.js`) and the solver re-export |
| `tests/charm-nest/solver.cjs` | Solver unit test (`npm run test:charm-nest`) |
| `tests/charm-nest/fixture.cjs`, `tests/charm-nest/e2e.cjs` | Synthetic flat Illustrator page + headless-Chromium end-to-end (`npm run test:charm-nest:e2e`, needs `playwright-core`) |

Naming warning: `charmSetsData.js` / `syncCharmSets.js` / `charmBatchSweepCron.js` are Etsy listing relationships and have nothing to do with this. Everything here is prefixed `charmNest*`.

## Pipeline (all in the browser)

1. **Parse** — pdf-lib decodes the page content; one tokenizer walks it, recording every top-level drawing segment's byte range, geometry (CTM-transformed), paint, colour and stroke width. Form XObjects are one segment at the top level and are walked for geometry. Legacy PostScript-only `.ai` files are rejected with a re-save instruction.
2. **Group** — outlines = closed, *achromatic* strokes (black, grey or white — the reference sheet strokes one charm in white) above the minimum size; page-sized paths of any colour are frames and ignored. Containment is geometric, not bounding-box: a candidate whose points lie inside an accepted outline's polygon is a detail (hole, inner ring, engraving frame); a small candidate within ~2 mm of an outline's stroke is an attached jump ring. Every other segment goes to the outline whose polygon holds most of its points, then by stroke contact, then by box overlap. Scored against the 21-charm reference sheet: 21/21 groups correct (bounding-box rules got 11 wrong). Loose elements are reported, not silently dropped.
3. **Silhouettes** — the outline alone is stroked at 6 px/pt and flood-filled from the border to test closure (a leak is flagged `open path` and excluded). The final silhouette floods the outline *plus every member* together, so a jump ring's hole is solid material and nothing nests inside it — the same thing the render verifier sees. Geometry hash → duplicates become quantities.
3b. **AI review of the grouping** — with the cloud on, Claude sees the whole source page with every detected charm boxed and numbered plus each charm's thumbnail, and returns a verdict per charm: complete / fragment of #N / multiple / not a charm. Fragments are merged back (members, byte ranges, silhouette re-traced), junk excluded, "multiple" flagged in the report. Nest waits for this review. The same judgement I applied by hand when debugging the first sheet, now in the loop.
4. **Nest (default: parallel geometric search)** — `charm-nest-solver.js` runs in a pool of Workers (all cores but one, or Settings → Parallel), each with its own seed. The first search that places every charm wins and stops the others; if none does, the best layout across the pool is taken. Each search: largest-first, contact-scored, gravity tie-break, random restarts under the 74 % fill ceiling, failed-piece-first ordering, a targeted 5° push for the last piece, and ruin-and-recreate repair when restarts stall. The finish runs exactly once per job; late messages from stopped searches are dropped. Settings → Engine "ai" keeps the experimental Claude-placement engine.
4a. **Overflow sheets** — the fill ceiling (80 % of the usable area by default, set in Settings) is the most any sheet may hold. Whatever the search cannot place under that ceiling is moved, automatically, to the next sheet of the same metal (GF · sheet 2, sheet 3 …), which nests on its own. The metal card grows tabs to switch between its sheets; each sheet has its own .ai, proof, report and cloud record (`page` in `Charm_Nest_Sheets`).
4b. **Solver details** — Positive clearance dilates each mask by half of it; **negative clearance (the default, −0.5 pt) erodes instead: adjacent outlines may touch and their strokes overlap by up to |clearance|, bodies never more.** On a partly cut Rose Gold sheet the stock already cut away is also tested against each charm's whole silhouette, because erosion can erase a thin part such as a jump ring that must still stay off removed stock. Sheet inset pre-filled as wall; mirroring never applied. Coarse pass (0.5 px/pt, exhaustive, summed-area-table pre-tests) picks candidates; fine pass (2 px/pt, bit-parallel) makes the exact decision. Each committed placement is posted to the page and drawn immediately.
5. **Write** — pdf-lib: one page at stock size, sheet rectangle on `SHEET (do not cut)`, one optional-content group (Illustrator layer) per charm named by Claude/library/operator. Each charm is a form XObject whose content is the original page content with every non-member segment blanked byte-for-byte — the output carries exactly the original paths. Also a `_labelled.pdf` proof with numbered callouts and `nest-report.json` (seed, angles, clearance, placements, gaps).
6. **Verify** — twice, independently of the solver: geometry re-rasterised at 6 px/pt (overlap, bounds, minimum gaps) and the written file re-rendered layer by layer with pdf.js (overlap, bounds, empty layers). With a negative clearance both apply the same erosion tolerance (`CharmNestSolver.erosionPx`), so only intrusion beyond the allowed stroke overlap is flagged. A flagged sheet is `Partial`, still downloadable, and says why.
6b. *(off the loop)* Claude sheet inspection is no longer run after nesting — the two independent verifiers (geometry re-raster and per-layer re-render of the written file) are the acceptance test. It remains callable from the console as `CN.reviewLayout(metal)`.
7. **Persist** — source files, per-charm `.ai` + thumbnail, sheet `.ai`/`.pdf`/proof/report/preview to Storage under `charmnest/…`; the sheet record, charm library and calibration row to Firestore.

## Saturation readout

Fact: `% full · mm² free` from the worker's occupancy grid. Estimate: `≈ N more fit (lo–hi)` with `ρ_target` = the 25th percentile of achieved densities among the nearest shape-mix neighbours in `Charm_Nest_Calibration` (seed 0.70 until enough sheets exist). Also the largest empty rectangle on the sheet ("a charm up to W × H still fits"). Before a nest, if the queue exceeds capacity at `ρ_target`, the card says by how much, which pieces to remove, and what size charm could take the largest one's place. After two rejections the estimate reads *sheet is saturated*.

## Recovery

**Library** lists every saved sheet by day, colour-coded by metal, filtered by metal/date/search; each shows placed/total, % full, free area, status and verification. Open one for the outputs, the sources and the charm list; **Restore into card** re-downloads the sources, re-parses them and pins every charm at its cut position so more can be added around it or the layout re-nested exactly (same seed). **Charms** lists the library with rename-in-place.

## Firestore

| Collection | Key | Contents |
|---|---|---|
| `Charm_Nest_Library` | geometry hash | name, slug, label, confidence, namedBy, metalHint, size, thumb/ai URLs, timesUsed |
| `Charm_Nest_Sheets` | sheet id | metal, day, stock, params, placements, rejects, verification, outputs, sources, charms |
| `Charm_Nest_Calibration` | auto | count, cv, largestFrac, density, placedAll |
| `Charm_Nest_Jobs` | job id | server solver status/progress/result |

Single-field queries only; no composite indexes.

## Environment

`EDIT_PASSCODE` (optional, same as the other consoles), `ANTHROPIC_API_KEY` (naming; absent ⇒ file-based names), Firebase admin vars (already set). `CHARM_NEST_NAME_MODEL` overrides the model for naming and review (default `claude-opus-5`); `CHARM_NEST_NAME_EFFORT` / `CHARM_NEST_REVIEW_EFFORT` override the effort (default `high`).

## Settings (per browser)

Rail density, per-metal stock, clearance, edge inset, angle step, time budget, seed, server-fallback threshold, outline minimum size, verify resolution, naming on/off, notifications, sound.

## Open questions carried from the design document

Stock size per metal (plate A, 100 × 50 mm, is the default; plate B is 100 × 100 mm), machine kerf (clearance default −0.18 mm, i.e. shared cut lines; raise to a positive value for a guaranteed gap), whether small charms may nest inside jump rings (no — holes are solid), legacy `.ai` files (rejected with instructions), grain/orientation constraints (free rotation assumed; angle step is a setting).


## Units and preview

Every measurement in the console is metric: charm sizes, pockets, free area (mm²), stock, clearance and inset (Settings inputs are in mm; the stored values stay in the file's points and inches). Each sheet preview carries millimetre rulers along its top and left edges. The preview draws every member path of a charm in its file colour; a cut line stroked in white (or in a Separation "All" registration colour, which the parser now resolves through the form's own colour-space resources) is shown in ink, and the metal tint is punched out inside every inner cut line so hoop holes read as holes. The written `.ai` is untouched original path data either way.

## Hands-off run, timing, log

Dropping a file starts the whole process: parse → group → Claude grouping review → nest → verify → write → save, with no separate "Nest" click (the button remains for re-nesting). A file routed by hand from Unassigned starts the same way. The time shown on the card runs from the moment the file arrived to the moment the sheet is written and verified, Claude's review included ("2m 41s total · nest 8s"). The agent log is hidden behind the small **Log** button on each card.

## Cloud folders

Each finished sheet is saved in its own folder, `charmnest/sheets/<yyyy-mm-dd>/<name>/`, where `<name>` is `<metal tag>_<Mon.DD.YY>_Set-<K>` — for example `GF_Mar.22.26_Set-2`. K is the sheet number for that metal on that day (one more than the cloud already holds). The `.ai`, `.pdf`, `_labelled.pdf` and `_nest-report.json` inside carry the same name, and so does the download. A "/" cannot appear in a file or folder name, so the tags are `GF`, `SS`, `RG`, `10K` and `14K`. File-name routing recognises `10k`/`14k` for the solid golds (`14/20` still means gold filled).

## Deleting a sheet from the Library

"Delete sheet" in a Library record removes the record and its output files permanently after a passcode prompt. The passcode is checked on the server (`CHARM_NEST_DELETE_CODE`, default 975311); a wrong code deletes nothing. Charm library copies are shared between sheets and stay. Links in a record are rebuilt from the objects' current download tokens when it is opened, and .ai files carry the Illustrator MIME type so browsers keep the .ai extension.

## Plates and the work area

Production uses two plates: **A · 100 × 50 mm** (default) and **B · 100 × 100 mm**; the stock selector offers only these plus the per-metal custom size. On import the sorter reads the work area from the file: a drawn plate frame if there is one, otherwise the artboard if it is plate-sized, otherwise the tight extent of the largest block of charms (charms within 20 pt of one another). The measurement is logged; the plate itself is always the operator's selection (A by default) and is never switched by a file. Charm sizes are always read from the file's own points, never scaled.

## Fidelity check

`node tests/charm-nest/fidelity.cjs <source.ai> <sheet.ai>…` compares every drawn path of the source with every drawn path of the written sheets: paint operator, stroke colour, fill colour, line width, closure, subpath and operator counts, and rotation-invariant geometry. A written set is faithful when the only extras are the sheets' own borders.

## Grouping on dense, already-nested sheets

Three rules were added after a 75-piece nested sheet came back with charms merged and details scattered:

- **The artist's order first.** Illustrator writes a group's objects contiguously, so a detail is assigned to the charm whose outline was written just before or just after it in the content stream, provided it sits inside or against that outline and fits within its box. Only when neither stream neighbour holds it does pure geometry decide. This is what stops touching charms on a dense sheet from swapping details.
- **Rings are ring-shaped and drawn with their charm.** A small closed stroke touching a neighbour is attached as a jump ring only when it is ring-sized (≤ 3.5 mm), a single simple subpath, and adjacent in the stream to the outline it touches. Before this, any small charm that touched a neighbour was swallowed as a "ring".
- **Outlines that are not one closed stroke.** Open achromatic strokes whose ends meet (a bar drawn as a U plus a line) are chained into one closed outline; the real parts remain the members the writer copies. Solid dark shapes with no stroke (an anchor, a star) are outlines cut along their fill edge, unless they sit inside a stroked outline, in which case they are that charm's fill.

`node tests/charm-nest/grouping-audit.cjs <file.ai>` reports charms, rings, orphans, members lying outside their own outline, and charms whose extent grew past their outline (merged neighbours).

## One fill measure

The fill ceiling and the card's "% full" are measured on the same thing: the footprint each piece stamps on the sheet grid (its silhouette eroded by a negative clearance or grown by a positive one). Before this, the ceiling counted solid silhouettes with holes filled while the readout counted stamped footprint, so a sheet could stop "at the 72 % ceiling" while the card read 64 %. Reports still carry the solid silhouette area per charm.

## Post-nest 2.5 % shrink ("Optimizing charms")

The solver works with every charm at its drawn size. Once the layout is assembled, the finish starts with a visible **Optimizing charms** stage in the progress bar: each placed charm gets `scale = CHARM_SCALE` (0.975, `charm-nest-1.html`), the writer scales its form about its own centre (placement matrix = scale · rotation), and the preview draws it the same way. Positions and angles are untouched; the shrink afterwards is what makes minor overlaps disappear. The agent log records "Optimized: N charms shrunk 2.5 %", and the e2e test reads the written .ai and checks every charm's matrix carries that scale.

## Orders travel whole

A multi-piece order is never split across sheets. Each charm carries an `order` id: when the source file keeps more than one Illustrator layer, every layer is one order (the sorter bridge will pass order ids directly); a single-layer file is single-piece orders. The solver places pieces of an order together and, if any piece of an order cannot go on the sheet (ceiling or no fit), lifts its placed siblings back off so the whole order moves to the next sheet; its targeted finish pass only ever adds single pieces, and the result is checked once more before the sheet is written (`keepOrdersWhole`). The overflow log names the orders moved. Covered by the "orders whole" case in `tests/charm-nest/solver.cjs`.

## Library sets

The sheets of one run (one drop of files: same `runId`, or for older records the same day and source files) are one **set** in the Library. A set shows as a stacked deck with a "Set · N sheets" tag; hovering (or tapping the deck) fans the sheets out in order after a short delay, each card sliding in with a stagger; × or Escape closes a pinned fan.


## Workspace recovery and continuous intake

The sorter checkpoints its working workspace in IndexedDB, with separate sandbox and production records. It retains source bytes, geometry, page layouts and selection, orders, pool membership, engraving decisions, review links, settings-backed filters, and the activity log. Parsed PDF handles, DOM nodes and workers are rebuilt rather than serialized. Refreshing an active run restores the work and offers Resume; interrupted nesting is restarted before labels or completion. Browser storage errors and cloud save failures are visible and never treated as successful sheet delivery.

Run saves are serialized. A new run clears the previous sheet identities; re-nesting within a run keeps its identities. Set allocation is idempotent per run/material group, numbered transactionally per day. The Sets dialog searches complete set membership, including older records and runs without sheets, and pages results newest date first. Receipt number, SKU, engraving words, date and set number are searchable. A matching sheet returns the entire set and its sheet count.

Etsy intake defaults to a 10-minute interval, configurable from 10 minutes to 24 hours in Settings. It uses the Design Station's existing refresh, hydration cache, rate budget and watchdog. **The sorter tab must remain open and connected to the station; this is not an unattended server scheduler.** A fixed counter shows unique imported receipt IDs in rolling 24-hour and 1-hour windows, time to next check, and failures. The server ledger deduplicates across checks and stations, separately for sandbox. A historical set remains a historical view; its counter offers incoming orders without overwriting that view.

New rows merge without replacing earlier decisions. Lists default to newest arrivals, while each charm retains the original Etsy creation timestamp for physical nesting. Open Auto runs take in new orders while they wait at engraving review, at the checkpoint, or once processed with work still pending, never while labels are made or a set is committed. Stopped runs remain stopped. Arrivals keep a sheet's saved arrangement and fill its gaps. Gold and Silver arrivals go to the run's earliest open sheet first. When an order misses the gaps of a sheet short of the 74% target, the whole sheet gets one fresh arrangement. If that arrangement still leaves the order out, the sheet is full and released, and the order moves on to the next open sheet in line; an earlier sheet is never passed over and left short of full, where it could never be released. Rose Gold arrivals go to the newest sheet and keep its protected green-line layout. Whole orders form an oldest-first prefix on each material's sheet; an unplaceable oldest order stops for attention rather than generating empty overflow sheets. Manually pinned pieces keep their positions through a fresh arrangement.

A backlog, such as the first check after the station was closed overnight, is read at the pace the station's Etsy watchdog allows, so it arrives in full rather than tripping the watchdog part-way. One failed Etsy call fails only its own request.

A new order connecting formerly independent material groups gets a newly numbered combined set. Earlier uncommitted records are retained as superseded. Existing engraving approvals remain valid on the same charm geometry, and back files and sheet labels are regenerated for their new membership. Committed sets are not eligible for automatic intake.

Regression commands:

- `npm run test:charm-nest` — solver/verifier, handler tests, workspace recovery and intake tests; no live services.
- `node tests/charm-nest/bridge-units.cjs` — existing interpretation, geometry, engraving, labels and release-rule tests.

Before production release, exercise the sandbox UI with a refresh during Review, a duplicate snapshot, a new mixed-material order, and a near-capacity sheet. Confirm the resulting saved files, labels and memberships through the Sets dialog. The browser workspace tests use transaction-shaped IndexedDB and DOM adapters; they do not replace this end-to-end check.

## Set release policy, September 19 update (draft PR #12)

A saved layout and a released set are different records. New runs use release policy 2: every valid order line can enter the working queue immediately, but only eligible, verified sheets join the run's single dispatch set. Historical sets keep their identities. Unreleased older runs are rechecked on Resume; an older run with an already committed set finishes under its recorded rules.

- GF 14/20 and SS never receive an urgency, mixed-order, or manual partial-sheet exception. A completed small queue remains a saved working sheet. Full means the configured fill ceiling is reached, or a completed capacity search overflows a whole order that would itself fit an empty sheet. A fresh arrangement of the whole sheet runs its full search budget by design, so ending on time completes it; the short search of an arrival that only tries the gaps of a saved layout does not. An interrupted search cannot release a partial sheet. The default fill ceiling is 80% (Settings); an overflowing sheet counts as full from the 74% target. Neither is a promise of 100% physical coverage.
- Rose GF joins even-numbered sets (2, 4, 6...) using the existing daily counter. A rose-only attempt at an odd number is deferred transactionally without consuming a number.
- Solid 10K and 14K are excluded by default. Their explicit selection belongs to the current run and resets for the next run. Each uses its own dimensions regardless of the general GF/SS/rose preset. The card exposes an isolated nesting action; oversized pieces and excess whole orders are reported or overflow to another same-size sheet, never silently scaled or discarded.
- A run that keeps taking orders after a commit releases its next eligible sheets into a new dispatch set with the next number of the day. The committed set keeps its number, record and files; the server numbers the follow-on set once, however often it is asked.
- Checkpoint assembly, label creation and commit enforce membership. Changing a selection or geometry pauses the run for revalidation; completed sets remain immutable. Assembly is serialized, retryable after file failures, and updates pool membership in batches of at most 400.
- Unfinished lines and their unchanged engraving decisions carry forward. Previously released lines of a mixed-material order remain written so the later set cannot cut them a second time. A changed Etsy line is reinterpreted and cannot inherit an old approval. The workspace checkpoint preserves the carry record during a refresh between runs.
- An order that leaves Etsy (cancelled or refunded) is dropped from its set. Its pieces come off every sheet that is still filling, and that sheet is arranged again. A piece already on a released sheet, or inside a saved Rose Gold green line, is cut with its sheet and set aside; it needs no engraving and never holds the release back. A changed order still waits for a person in Review.
- The Nest dropdown previews current and saved sheets without replacing the active bench. Date, set number, piece counts, and material sheet counts are visible in the preview; search remains in the existing Sets dialog. Working groups are identified separately from released sets.

### Verification and release gate

`npm run test:charm-nest` includes `releases.cjs`: strict partial holds, verification and interruption gates, both solid selections, odd/even rose allocation, repeat/concurrent checkpoint assembly, interrupted file-save retry, deselection, carry-forward approvals, changed-order invalidation, and duplicate-cut prevention. The existing solver, server-handler and workspace suites remain in the command. `node tests/charm-nest/bridge-units.cjs` also passes.

Browser interaction on the Netlify draft preview checked invalid custom sizes, 10K size independence from the general stock preset, selection/dimension persistence in a fresh tab, current-workspace preview, and a no-match set search. Visual review retained the existing card design and a compact Sets dropdown. Opening Settings also exposed and fixed a pre-existing commented-out initialization of angle, budget, seed, parallelism and server threshold controls.

**Full end-to-end browser release gate remains open.** The approved preview connection now succeeds. Through actual UI clicks, the rehearsal pulled 8 existing sandbox orders / 9 lines, started a manual run, and retained the stopped run, all lines, and the rolling 24-hour/1-hour counts of 8 after refresh. Pulling the same orders twice did not increase the count. The run stopped before nesting because every existing master artwork fetch failed. Opening the observed existing master link in the test browser confirmed `net::ERR_BLOCKED_BY_CLIENT` for `firebasestorage.googleapis.com`. No storage/network restriction was bypassed. Browser security previously rejected a synthetic artwork upload, reporting declined permission; that upload was not retried through another route. The current blocker is access to existing artwork, not the app password or the Design Station handshake.

The user explicitly approved the following narrowly scoped test access on September 19; it is now implemented: accept a bridge message only when both sender and Design Station are on `https://deploy-preview-12.goldenspike.app`, the station is in sandbox mode, and all existing source/nonce checks pass. No wildcard preview domains and no additional production origin. Then rehearse pull → pool → nest → approvals → labels → sandbox commit, refresh during processing, two consecutive sets, new-order intake and solid-only nesting. Do not describe the release gate as passed until these interactions actually complete. The user's instruction is to push to main only when done.

Recovery review also corrected the cloud-only Resume path: it reads the saved run's receipt IDs independently of the current pull limit, preserves the checkpoint and unchanged pool identities, restores draft-only runs without allocating or borrowing a set, and never marks draft placements as released. Editable draft layouts recovered from cloud records are rebuilt and reverified; their saved words are retained but the rebuilt engraving fit requires review. Ordinary refresh uses the richer local workspace checkpoint and retains the existing approvals. A separate regression ensures that promoting a verified sheet to a set cannot skip personalization classification. These paths are exercised from the actual functions in `releases.cjs`.
