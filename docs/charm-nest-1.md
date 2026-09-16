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
4a. **Overflow sheets** — 74 % of the usable area is the maximum any sheet may hold. Whatever the search cannot place under that ceiling is moved, automatically, to the next sheet of the same metal (GF · sheet 2, sheet 3 …), which nests on its own. The metal card grows tabs to switch between its sheets; each sheet has its own .ai, proof, report and cloud record (`page` in `Charm_Nest_Sheets`).
4b. **Solver details** — Positive clearance dilates each mask by half of it; **negative clearance (the default, −0.5 pt) erodes instead: adjacent outlines may touch and their strokes overlap by up to |clearance|, bodies never more.** Sheet inset pre-filled as wall; mirroring never applied. Coarse pass (0.5 px/pt, exhaustive, summed-area-table pre-tests) picks candidates; fine pass (2 px/pt, bit-parallel) makes the exact decision. Each committed placement is posted to the page and drawn immediately.
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

## Post-nest 2 % shrink

The solver works with every charm at its drawn size. Once the layout is assembled, the writer and the preview draw each charm at 98 % of that size, scaled about its own centre (`CHARM_SCALE` in `charm-nest-1.html`; the placement matrix is scale · rotation). Positions and angles are untouched; the shrink afterwards is what makes minor overlaps between neighbours disappear.
