# Charm Nesting Station 1 — `charm-nest-1.html`

Production URL: `https://brites-charm-sorter.goldenspike.app` (redirect in `netlify.toml` → `/charm-nest-1.html`).

Takes loose charm artwork — `.ai` (PDF-compatible), `.pdf`, or a `.zip` of them — and nests every charm onto one fixed sheet per metal (GF 14/20, SS, RG 14/20) with no overlaps, then writes a production `.ai` per sheet with one Illustrator layer per charm. Everything uploaded and everything produced gets a permanent copy in Firebase.

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
4. **Nest** — `charm-nest-solver.js` in a Worker: largest-first, contact-scored, gravity tie-break, random restarts within the time budget. Positive clearance dilates each mask by half of it; **negative clearance (the default, −0.5 pt) erodes instead: adjacent outlines may touch and their strokes overlap by up to |clearance|, bodies never more.** Sheet inset pre-filled as wall; mirroring never applied. Coarse pass (0.5 px/pt, exhaustive, summed-area-table pre-tests) picks candidates; fine pass (2 px/pt, bit-parallel) makes the exact decision. Each committed placement is posted to the page and drawn immediately.
5. **Write** — pdf-lib: one page at stock size, sheet rectangle on `SHEET (do not cut)`, one optional-content group (Illustrator layer) per charm named by Claude/library/operator. Each charm is a form XObject whose content is the original page content with every non-member segment blanked byte-for-byte — the output carries exactly the original paths. Also a `_labelled.pdf` proof with numbered callouts and `nest-report.json` (seed, angles, clearance, placements, gaps).
6. **Verify** — twice, independently of the solver: geometry re-rasterised at 6 px/pt (overlap, bounds, minimum gaps) and the written file re-rendered layer by layer with pdf.js (overlap, bounds, empty layers). With a negative clearance both apply the same erosion tolerance (`CharmNestSolver.erosionPx`), so only intrusion beyond the allowed stroke overlap is flagged. A flagged sheet is `Partial`, still downloadable, and says why.
7. **Persist** — source files, per-charm `.ai` + thumbnail, sheet `.ai`/`.pdf`/proof/report/preview to Storage under `charmnest/…`; the sheet record, charm library and calibration row to Firestore.

## Saturation readout

Fact: `% full · pt² free` from the worker's occupancy grid. Estimate: `≈ N more fit (lo–hi)` with `ρ_target` = the 25th percentile of achieved densities among the nearest shape-mix neighbours in `Charm_Nest_Calibration` (seed 0.70 until enough sheets exist). Also the largest empty rectangle on the sheet ("a charm up to W × H still fits"). Before a nest, if the queue exceeds capacity at `ρ_target`, the card says by how much, which pieces to remove, and what size charm could take the largest one's place. After two rejections the estimate reads *sheet is saturated*.

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

`EDIT_PASSCODE` (optional, same as the other consoles), `ANTHROPIC_API_KEY` (naming; absent ⇒ file-based names), Firebase admin vars (already set). `CHARM_NEST_NAME_MODEL` overrides the naming model (default `claude-opus-5`).

## Settings (per browser)

Rail density, per-metal stock, clearance, edge inset, angle step, time budget, seed, server-fallback threshold, outline minimum size, verify resolution, naming on/off, notifications, sound.

## Open questions carried from the design document

Stock size per metal (defaults to the 7.125 × 6.03 in reference), machine kerf (clearance default −0.5 pt, i.e. shared cut lines; raise to a positive value for a guaranteed gap), whether small charms may nest inside jump rings (no — holes are solid), legacy `.ai` files (rejected with instructions), grain/orientation constraints (free rotation assumed; angle step is a setting).
