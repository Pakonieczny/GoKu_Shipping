# Charm Nesting Station — design and implementation

Status: proposal. Nothing in this document is built yet.
Target page: `charm-nest-1.html` · Production URL: `https://goldenspike.app/charm-nest-1.html`

## 1. What this is

An operator console that takes loose charm artwork — individual `.ai` files or a `.zip`
of them — and nests every charm onto a fixed sheet per metal, without overlaps, then
emits a production-ready `.ai` file per sheet.

Three sheets run side by side, one per metal:

| Sheet | Metal | Accent token (already in `design-1.html`) |
|---|---|---|
| GF 14/20 | Gold filled | `--m-gold` `#c8a24e` |
| SS | Sterling silver | `--m-silver` `#8d95a0` |
| RG 14/20 | Rose gold filled | `--m-rose` `#c08578` |

Each sheet is independent: its own dropzone, its own queue, its own nest run, its own
progress bar, its own output. All three can run at once.

### Naming warning

`charmSetsData.js`, `charmSetsCandidates.js`, `syncCharmSets.js` and `charmBatchSweepCron.js`
already exist and are about **Etsy listing relationships** — which necklace pairs with which
earring. They have nothing to do with vector artwork. Every new file here is prefixed
`charmNest*` so the two never get confused in the function list.

## 2. Evidence this design is built on

Before specifying anything, the pipeline below was run by hand against a real file
(`TEST1.ai`, 21 charms) nesting into a real box (`TEST2box.ai`, 513.03 × 434.19 pt =
7.125 × 6.03 in). Measured results:

| Measurement | Value |
|---|---|
| Charms extracted from a flat 117-path page | 21, verified individually |
| Total charm silhouette area | 158,600 pt² |
| Usable box area | 222,750 pt² |
| Required packing density | **71.3%** |
| Bottom-left-fill best result | 19 / 21 |
| Contact-scored placement + random restarts | **21 / 21** (trial 12, 241 s) |
| Overlapping pixels, verified at 576 dpi | 0 |
| Pixels outside the box | 0 |
| Tightest charm-to-charm gap achieved | 0.333 pt |
| Same job at ~3× clearance (1.5 pt) | **20 / 21 after 70 trials / 25 min — did not fit** |

Three things follow from that last row and they shape the whole design:

1. **At production densities, clearance is a hard tradeoff, not a preference.** The app must
   expose kerf/clearance as a control and must tell the operator when tightening it is the
   only way the last piece fits.
2. **Nesting is a search, not a calculation.** It needs a progress bar, restarts, and a
   defined stopping condition — which is what the user asked for anyway.
3. **"It didn't all fit" is a normal outcome.** The UI needs a first-class rejects path
   (§7.4), not an error toast.

## 3. Existing infrastructure this reuses

Everything in this table already exists in `Pakonieczny/GoKu_Shipping`. Nothing here gets
reinvented.

### Front end

| Asset | Path | Used for |
|---|---|---|
| Brites design tokens | `design-1.html` `:root` | `--paper --card --ink --line --gold --m-gold --m-silver --m-rose --serif --sans --mono --r --rsm --sh`. Copied verbatim. |
| Rail density tiers | `design-1.html` `html[data-rail=sm\|md\|lg]` | Same three-tier density switch, same Settings control |
| Shell layout | `design-1.html` `.app > aside.rail + main.main` | Same skeleton |
| Top bar | `.topbar` + `.navBtn` + `.modeSeg` + `.topTitle/.sub` + `.spacer` | Same header grammar |
| Buttons | `.btn` `.gold` `.sage` `.ghost` `.danger` `.sm` `.xs` | No new button styles |
| Toasts | `.toasts` + `toast(msg, kind, ms)` (`design-1.html:1207`) | Lifted as-is |
| Dialogs | `<dialog>` + `.dlg/.dlgHead/.dlgBody` + `openDlg()` (`design-1.html:1219`) | Settings, sheet report, charm inspector |
| Progress | `brites-progress.js` + `brites-progress.css` | `BritesProgress.begin(label)`, `.bp-meter`, native `<progress>` auto-enhancement |
| Metal vocabulary | `design-1.html:1150` `METALS` | Same `gold`/`silver`/`rose` keys and accent colours |
| Function base | `const FN = window.location.origin + "/.netlify/functions"` (`design-1.html:1135`) | Same constant |
| ZIP reader | `jszip@3.10.1` via cdnjs (`Game_Generator_1.html`) | Same version, but vendored — see §12.2 |
| Vendoring precedent | `vendor/fabric-7.4.0.min.js` | Where `pdf.js` and `pdf-lib` go |

`brites-progress.js` carries a rule this app must honour, stated in its own CSS header:
*"Percentages come only from completed work."* The nest progress bar therefore reports
**pieces placed / pieces queued**, never an interpolated guess at search progress.

### Back end

| Asset | Path | Used for |
|---|---|---|
| Firebase admin | `netlify/functions/firebaseAdmin.js` | Firestore + Storage bucket, already initialised |
| Storage upload + tokenised URL | `etsyMailCollateralUpload.js` (`file.save()`, `firebaseDownloadUrl()`) | Exact pattern for writing `.ai` output to Storage |
| Anthropic client | `netlify/functions/_etsyMailAnthropic.js` | Endpoint, `anthropic-version: 2023-06-01`, prompt-caching beta, `isClaudeOverloadError()`, `computeClaudeRetryDelayMs()`, 5-attempt backoff |
| Archive/CORS shape | `designArchive.js` (`CORS`, `json()`, op-dispatch) | Handler skeleton |
| Sign-in door | `authGate.js` | `EDIT_PASSCODE` / `X-Edit-Passcode` |
| Background jobs | `*-background.js` (e.g. `claudeCodeProxy-background.js`) | 15-minute budget for the server-side fallback nester |
| Deps already installed | `package.json` | `jszip`, `sharp`, `@resvg/resvg-js`, `@google-cloud/storage`, `firebase-admin` |

## 4. Where the work happens, and why

**The extraction, nesting and `.ai` writing all run in the browser, in Web Workers.**
Netlify is used for persistence, the Claude call, and an optional long-haul fallback.

The reasoning:

- A synchronous Netlify function has ~10 s. The measured nest took 241 s. A
  `-background.js` function gets 15 minutes but **cannot stream** — it can only write state
  for the client to poll. The user asked for a live preview that fills in as it goes; that
  rules out a round trip per placement.
- Sheets of artwork are multi-megabyte. Three concurrent uploads of source `.ai` files, then
  three downloads of output, is slower than doing the geometry locally and uploading only
  the finished sheet.
- Three sheets running at once maps cleanly onto three Workers on separate cores.

The site already sets `Cross-Origin-Opener-Policy: same-origin` and
`Cross-Origin-Embedder-Policy: require-corp` for `/*` (`netlify.toml:197`), so the page is
cross-origin isolated and `SharedArrayBuffer` is available — the preview canvas can read
worker output with no copy.

**Server fallback.** `charmNestSolve-background.js` accepts the same job payload and runs
the same solver in Node for operators on weak hardware, or for sheets that exceed a
configurable piece count. It writes progress to Firestore; the client polls. Same
algorithm, same seed, same result. It is a fallback, not the main path.

## 5. Screen design

The shell is `design-1.html`'s: fixed-height body, `.rail` on the left, `.main` on the
right, `.topbar` at the top of main, toasts bottom-right.

```
┌─ topbar ────────────────────────────────────────────────────────────────────┐
│ ☰  [ Nest | Library ]   Charm Nesting Station 1        [Stock ▾] [⚙]        │
│                          3 sheets · 47 charms queued                        │
├─ rail (queue) ─────────┬─ main ─────────────────────────────────────────────┤
│ SOURCES                │  ┌ GF 14/20 ──┐ ┌ SS ────────┐ ┌ RG 14/20 ─┐       │
│  ▸ batch-092.zip  31   │  │  [preview]  │ │  [preview] │ │ [preview] │       │
│    ├ GF  12           │  │             │ │            │ │           │       │
│    ├ SS  14           │  │             │ │            │ │           │       │
│    └ RG   5           │  │             │ │            │ │           │       │
│  ▸ compass-rose.ai  1  │  ├─────────────┤ ├────────────┤ ├───────────┤       │
│                        │  │ ▓▓▓▓▓░░ 14/21│ │ ▓▓▓░░░ 8/19│ │ ░░░ idle │       │
│ UNASSIGNED         3   │  │ 68% full     │ │ 41% full   │ │ 0% full  │       │
│  ⚠ needs a metal       │  │ ~4 more fit  │ │ ~9 more fit│ │ —        │       │
│                        │  │ [Nest][.ai]  │ │ [Nest][.ai]│ │ [Nest]    │       │
└────────────────────────┴───────────────────────────────────────────────────┘
```

### 5.1 The three sheet panels

One `.sheetCard` per metal, in a CSS grid that goes 3-up / 2-up / 1-up by width. Each card
is ringed in its metal's accent (`--m-gold` / `--m-silver` / `--m-rose`) — the same colour
coding operators already read in `design-1.html`'s metal columns.

A card has five zones, top to bottom:

1. **Header** — metal name, stock size, piece count, and a state pill
   (`Idle` · `Parsing` · `Nesting` · `Complete` · `Partial`).
2. **Preview** — a `<canvas>` showing the sheet outline and every placed charm, drawn live.
   Also the dropzone: the whole card accepts a drop, and the preview shows the drop hint
   when empty.
3. **Progress** — a native `<progress value max>`, so `brites-progress.js` enhances it into
   a `.bp-meter` with a percentage overlay and an elapsed-time note for free.
4. **Saturation readout** — §6.
5. **Actions** — `Nest` (`.btn gold`), `Stop` (`.btn danger`, while running),
   `Download .ai` (`.btn sage`, once complete), `Report` (`.btn ghost xs`).

### 5.2 Drag and drop

Drop anywhere on a sheet card → those files belong to that metal, full stop. Drop on the
rail or the page background → files are routed by filename (§5.3) and anything ambiguous
lands in **Unassigned** with a warning chip, where the operator drags it onto a sheet.

Accepted: `.ai`, `.pdf`, `.zip`. A `.zip` is expanded client-side with JSZip; nested folders
are walked; non-artwork entries and `__MACOSX/` are skipped silently. Everything else is
rejected with a toast naming the file and the reason.

### 5.3 Filename routing

Checked in order, case-insensitive, first match wins:

| Pattern | Sheet |
|---|---|
| `gf`, `gold-filled`, `goldfilled`, `14/20g`, `gf1420` | GF 14/20 |
| `rg`, `rose`, `rose-gold`, `14/20r`, `rg1420` | RG 14/20 |
| `ss`, `sterling`, `silver`, `925` | SS |
| enclosing zip folder named for a metal | that sheet |
| anything else | Unassigned |

`rose` is tested before `silver` and before the bare `g`/`s` tokens so
`rose-gold-compass.ai` cannot land on the gold sheet.

### 5.4 Completion

When a sheet finishes, three things happen together:

- The state pill flips to `Complete` (sage) or `Partial` (clay), and the card border pulses
  once.
- A toast: `GF 14/20 · sheet complete — 21 of 21 placed, 71.3% full`.
- If the tab is hidden, a `Notification` (permission requested on first nest, never on load).

`Partial` is not a failure state. It reads `19 of 21 placed — 2 need another sheet` and the
two rejects stay in the queue with `Start a second GF sheet` offered inline.

### 5.5 Settings dialog

A `<dialog>` matching `design-1.html`'s settings: rail density, stock presets, default
clearance, solver time budget, solver seed, and the server-fallback threshold. Nothing else.

## 6. The saturation readout

The user asked for a live readout of how many more charms fit. The honest version has two
numbers, and it matters which one is bigger on screen.

**Primary (a fact):**

```
68% full · 71,200 pt² free
```

`A_free = A_usable − Σ(inflated silhouette area of placed pieces)`, recomputed on every
placement. Inflated area means the silhouette grown by half the clearance on all sides, so
free area already accounts for the gaps the pieces need.

**Secondary (an estimate, smaller type, `--ink45`):**

```
≈ 4 more fit (3–6)
```

Computed as:

```
capacity  = A_usable × ρ_target − A_placed
n_est     = floor(capacity / ā_remaining)
```

where `ā_remaining` is the mean inflated area of the charms still in this sheet's queue
(or, if the queue is empty, of the charms already placed), and `ρ_target` is the packing
efficiency the solver can actually reach for this shape mix.

**`ρ_target` is calibrated, not guessed.** Naive area ÷ area is the wrong answer and would
over-promise: today's sheet needed 71.3% density and a bottom-left-fill solver could only
reach ~65%. So:

- Seed `ρ_target = 0.70`, the density measured on the reference sheet.
- Every completed nest writes its final density and a shape-mix signature (piece count,
  area coefficient of variation, largest-piece fraction) to
  `Charm_Nest_Calibration` in Firestore.
- `ρ_target` becomes the 25th percentile of achieved densities across the nearest
  shape-mix neighbours. The 25th percentile, not the mean, so the estimate errs low.
- The band is the 10th–50th percentile of the same set.

The readout is labelled `estimate` and never appears without the free-area fact above it.
Once a nest is running, `n_est` is additionally capped by what the solver has actually
proved: if it has already failed to place a piece twice at the current density, the
estimate drops to `0` and the readout reads `sheet is saturated`.

## 7. The pipeline

### 7.1 Parse

Per file, in the sheet's worker:

1. Read bytes. An Illustrator `.ai` saved with PDF compatibility *is* a PDF — the reference
   file identified as `PDF document, version 1.6`. Reject a genuine non-PDF `.ai` (legacy
   PGF-only) with a toast telling the operator to re-save with PDF compatibility on.
2. `pdf.js` `getDocument()` → page 1.
3. `page.getOperatorList()` → walk `OPS.constructPath` / `OPS.fill` / `OPS.stroke` /
   `OPS.paintFormXObjectBegin`, accumulating one record per painted path: bbox, paint type,
   fill colour, stroke colour, stroke width.

### 7.2 Group paths into charms

The reference file was one flat page of 117 paths with no grouping at all. The grouping rule
that worked, and which the app implements:

1. **Outlines** are the closed, stroked, near-black paths whose bbox is larger than a
   threshold. On the reference file this selected exactly 21 of 117 paths.
2. **Everything else is assigned to the outline it overlaps most**, by
   `area(intersection) / area(path bbox)`, ties broken toward the smaller outline. This
   placed all 96 remaining paths correctly with no manual intervention.
3. **Form XObjects count as paths.** One red stroke on the reference file lived inside a
   `/Fm0 Do` invocation and silently shifted every subsequent index by one. The walker must
   emit a pseudo-path for each `Do` and keep the mapping aligned.

Grouping is shown to the operator before nesting — the card's `Report` button opens a
contact sheet of the detected charms. If grouping is wrong the operator sees it there, not
in the cut file.

### 7.3 Silhouettes

For each charm, render **only its outline path** to an `OffscreenCanvas` at 6 px/pt, then
flood-fill from the border; everything unreached is the charm's solid silhouette. This is
what makes tight nesting possible — bounding boxes would have failed outright, since the
reference charms' bboxes summed to 281,000 pt² against a 222,750 pt² box, while their true
silhouettes summed to 158,600 pt².

Two guards, both learned the hard way:

- **Never draw the sheet outline into a silhouette probe.** The box rectangle is a closed
  path spanning the whole sheet; flood-fill from the border then stops at it and reports the
  entire sheet interior as part of every charm. This produced a spurious "8,049,713
  overlapping pixels" during verification.
- An outline with a gap at raster resolution leaks the fill. If the filled area exceeds 95%
  of the bbox, flag the charm as `open path` and exclude it from the nest rather than
  placing a blob.

### 7.4 Nest

Grid: 2 px/pt. Sheet inset 1.5 pt from the perimeter line. Each mask dilated by the
clearance before collision testing, so a successful placement is a guaranteed gap.

Placement loop, per piece, largest area first:

1. For each rotation in the angle set (default 30° steps, 12 angles; mirroring is **never**
   applied — it would reverse engraved text like `POLICE` and `9.26.25`):
   - **Feasibility:** FFT cross-correlation of the occupancy grid with the reversed mask.
     Every zero in the result is a legal position. This is the step that makes the search
     tractable — it evaluates ~890,000 candidate positions in one transform instead of
     testing them one at a time.
   - **Score:** a second correlation against a contact ring (the mask dilated by 4 px, minus
     the mask) over an occupancy grid whose border is pre-filled as wall. Higher contact =
     snugger fit. Final score is `−contact + w·(y + x)`, the gravity term breaking ties
     toward one corner.
2. Take the best position across all angles; commit; **emit a `placed` event**.
3. If no angle has a legal position, the piece goes to rejects and the loop continues —
   one oversized piece must not abort the sheet.

Then restarts: reshuffle the order, jitter the gravity weight, add noise to the score, and
run again, keeping the best result. On the reference sheet, ordering and scoring presets
reached 19/21 and 20/21; a randomised restart found 21/21 on trial 12.

**Multi-resolution.** A full-resolution transform pair per angle per piece is ~500
correlations for one attempt. The worker runs a coarse pass at 0.5 px/pt first to find
candidate regions, then refines only those at 2 px/pt. Without this the browser cannot hold
a useful restart rate.

**Stopping condition**, whichever comes first: all pieces placed; the time budget expires
(default 180 s, settable); or `Stop`. Reporting always states which one ended the run.

### 7.5 Live preview

The worker posts `{type:'placed', charmId, angle, x, y}` as each piece commits. The main
thread draws that charm into the preview canvas immediately. The sheet fills in piece by
piece, which is exactly the requested behaviour and costs nothing — the events already exist.

On a restart that beats the incumbent, the worker posts `{type:'restart', better:true}` and
the canvas is repainted from the new layout. Restarts that do not improve are not drawn, so
the preview never flickers backwards.

### 7.6 Verify — before the file is offered

A layout is not `Complete` until it has been checked independently of the solver that
produced it. The solver works on dilated masks at 2 px/pt; the verifier re-renders **each
placed charm alone** from the actual output document at 6 px/pt and checks:

- pairwise silhouette overlap is exactly 0 px,
- no charm pixel lies outside the sheet rectangle,
- the minimum charm-to-charm and charm-to-edge distances, reported in the sheet report.

If verification fails, the sheet goes to `Partial` with the offending pair named, and the
`.ai` is still downloadable but flagged. A solver bug must never reach a laser silently.

## 8. The `.ai` output

Illustrator opens a PDF natively, and an `.ai` saved with PDF compatibility is a PDF — so
the output is a PDF 1.7 written with `pdf-lib` and served under both `.ai` and `.pdf`.

Structure:

- One page at the source artboard size, sheet rectangle drawn in its original position and
  colour, on its own optional-content group named `SHEET (do not cut)`.
- One optional-content group per charm, named for the charm. Illustrator maps OCGs to
  layers, so the operator opens the file and sees one named layer per charm.
- Each charm is placed as an embedded form XObject with a clip to its own bounding box and a
  rotation transform — `pdf-lib`'s `embedPage(page, clipBox, transform)` then
  `drawPage()`.

**The junk-path trap.** The obvious way to isolate a charm — keep the whole source page and
turn every other path's paint operator into a no-op `n` — renders correctly and is a trap.
It carries all 116 other paths into every embedded charm. On the reference file that produced
a 2.4 MB document with **2,457 paths, 2,436 of them invisible and unpainted**, which opens
in Illustrator as an unusable mess. The fix is to blank each non-member path's *entire byte
range*, construction operators included, not just its paint operator. Same render, and the
reference output dropped to **137 KB with exactly 117 paths** — the original count, nothing
added, nothing lost.

Output also includes, in the same download: a `_labelled` PDF with numbered callouts, and a
`nest-report.json` carrying seed, angle set, clearance, achieved density, per-charm
placement, and the measured minimum gaps. The seed plus the report is enough to reproduce a
layout exactly, which matters when a sheet has already been cut once.

## 9. Claude Opus 5 — charm identification

One model call, one job: **name the charms**, so layers read `compass-rose` and
`police-vest` rather than `charm-04`. Layer names are what the operator navigates in
Illustrator, and a 21-layer file with meaningless names is barely better than no layers.

- Model: `claude-opus-5`
- `output_config: { effort: "low" }` — as requested. Naming a thumbnail is a
  recognition task, not a reasoning task; low effort is the right setting on its merits.
- Thinking is left at its default (adaptive) rather than disabled. On Opus 5, disabling
  thinking can cause a tool call to be written into visible text instead of a `tool_use`
  block, and can leak `<thinking>` tags into output. Low effort achieves the cost saving
  without that risk.
- `output_config.format` (structured outputs) pins the response to
  `{ charms: [{ index, slug, label, confidence }] }`. No prose parsing.
- One request per sheet, not per charm: all thumbnails go in a single message, so 21 charms
  cost one call. The instruction block is identical every time and is cached with
  `cache_control: { type: "ephemeral" }`.
- Transport: extend `_etsyMailAnthropic.js` rather than writing a second client. It already
  has the endpoint, the version header, the caching beta, and the 5-attempt
  overload/rate-limit backoff.

**It is never on the critical path.** Naming runs in parallel with nesting. If the call
fails, is slow, or `ANTHROPIC_API_KEY` is unset, layers fall back to
`<sourcefile>-<index>` and the sheet completes normally. A toast says naming was skipped.
The operator can rename in the charm inspector, and corrections are written back to
`Charm_Nest_Library` so the same artwork is named from the library next time and never
costs a second call.

A second, optional use: when filename routing leaves files **Unassigned**, the same response
can carry a suggested metal. Suggestion only — it pre-selects a sheet, it does not commit
the file to one.

## 10. Back end

### 10.1 Functions

All four are new, all live in `netlify/functions/`, all must be added to
`scripts/netlify-function-entries.json`.

| Function | Shape | Job |
|---|---|---|
| `charmNestLibrary.js` | endpoint | Firestore CRUD for parsed charm records and operator name corrections. Op-dispatch + CORS from `designArchive.js`. |
| `charmNestName.js` | endpoint | The Opus 5 call in §9. Thumbnails in, names out. |
| `charmNestOutput.js` | endpoint | Writes a finished `.ai`/`.pdf`/report to Firebase Storage and returns tokenised URLs, using `etsyMailCollateralUpload.js`'s `file.save()` + `firebaseDownloadUrl()`. |
| `charmNestSolve-background.js` | background | Server-side solver fallback (§4). Writes progress to Firestore for the client to poll. |

### 10.2 Firestore

| Collection | Key | Contents |
|---|---|---|
| `Charm_Nest_Library` | content hash of the source artwork | charm geometry, silhouette area, confirmed name, metal hint, times used |
| `Charm_Nest_Sheets` | sheet id | metal, stock, clearance, seed, placements, density, verification result, output URLs |
| `Charm_Nest_Calibration` | auto | one row per completed nest: shape-mix signature + achieved density. Feeds `ρ_target` (§6) |

All three are single-field equality or range queries only — no composite indexes, matching
`designArchive.js`'s stated constraint so this deploys with no Firestore console setup.

### 10.3 Auth

`authGate.js` as-is: `EDIT_PASSCODE` env var, `X-Edit-Passcode` header. Unset means open,
which is the existing behaviour for the other station consoles.

## 11. Concurrency

- One dedicated `Worker` per sheet, three total, spawned on first nest and reused.
- Workers are independent: a crash or a stop on one sheet cannot touch another. Each posts
  only to its own card.
- Parsing is queued on a shared pool of two workers so a 200-file zip drop does not starve
  the three solvers.
- If `navigator.hardwareConcurrency < 4`, sheets are queued rather than run in parallel and
  the cards show `Queued — waiting for GF 14/20`. Honest, and better than three solvers
  fighting over two cores.
- Per-sheet state is a small state machine — `idle → parsing → ready → nesting →
  (complete | partial | stopped)` — held in one object per sheet and rendered from it.

## 12. Deployment

### 12.1 Release path

Per `AGENTS.md`: commit only the changed files to `main`, let Netlify deploy the commit
automatically, then confirm a deployment exists for that exact commit. Do not add
`[skip netlify]`. Do not touch `package.json`, `netlify.toml` build commands, or
environment variables to force a release through.

New-file checklist:

1. `charm-nest-1.html` → add to the `assets` array in **`scripts/build-public.cjs`**. The
   build publishes an explicit manifest to `public-site`, not the repo root — an unlisted
   page simply will not exist in production.
2. Each new function → add to **`scripts/netlify-function-entries.json`**.
   `scripts/build-netlify.cjs` **throws** on any `.js` in `netlify/functions/` that is not
   classified, so a missed entry fails the build rather than shipping quietly.
3. Vendored libraries → add each file to the same `assets` array.
4. Consider a `[[headers]]` block for `/charm-nest-1.html` matching the
   `no-cache, no-store, must-revalidate` treatment `etsy-pricing.html` and `investor.html`
   already get, so a cached page cannot run an old solver against new functions.

### 12.2 Vendor, do not CDN

`pdf.js`, `pdf-lib` and `jszip` go in `vendor/`, alongside the existing
`vendor/fabric-7.4.0.min.js`.

The site sets `Cross-Origin-Embedder-Policy: require-corp` on `/*`. Under that policy a
cross-origin subresource must opt in via CORP or CORS, and **no `<script>` tag in this
repo currently carries a `crossorigin` attribute** — 23 pages load Materialize from cdnjs
without one. Whether those pages work today depends on cdnjs's own CORP header, which is
outside our control and can change. Vendoring removes the question entirely and removes a
third-party dependency from a production cut path. **Verify the current cdnjs behaviour
before assuming the existing pages are unaffected** — that is a separate finding, not
something this app should inherit.

### 12.3 No new runtime dependencies

`jszip` is already in `package.json`. `pdf.js` and `pdf-lib` are pure JavaScript and are
used client-side only, so they are static assets, not npm dependencies — nothing is added to
the build's install step. This matters: the one recorded Git-triggered deploy failure in
`AGENTS.md` was an `E401` during dependency installation, and this app should not go near
that mechanism. The server-side fallback solver uses only `firebase-admin` and `node-fetch`,
both already present.

## 13. Build order

| Phase | Deliverable | Proves |
|---|---|---|
| 1 | Shell: three cards, drag-drop, zip expansion, filename routing, queue rail | The UI is right before any geometry exists |
| 2 | Parse + group + silhouette, with the contact-sheet report | Grouping is correct on real files — the highest-risk step |
| 3 | Nester in a worker, live preview, progress bar, saturation readout | The core loop, visible |
| 4 | Verifier + `.ai` writer + download | Output is trustworthy and opens cleanly in Illustrator |
| 5 | Firestore library, calibration feedback, `charmNestOutput` | Sheets survive a refresh; the estimate improves |
| 6 | Opus 5 naming, Unassigned suggestions | Layer names become useful |
| 7 | Server-side fallback solver | Weak hardware is covered |

Phases 1–4 are a usable app. 5–7 are improvements on a working thing.

## 14. Additions beyond the brief

Each of these came out of actually running the job; none is speculative.

| Addition | Why it earns its place |
|---|---|
| **Clearance control** | 21 pieces fit at 0.33 pt of clearance and would not fit at 1.5 pt, after 25 minutes of trying. That tradeoff is real and belongs to the operator, not buried in a constant. |
| **Rejects tray + continuation sheets** | "20 of 21" is a normal outcome. Without this the operator's only move is to re-drop 20 files by hand. |
| **Duplicate detection** | The reference file had the compass rose twice, the Celtic knot twice, the axolotl twice — 21 pieces, 18 distinct designs. Detecting repeats by geometry hash turns six layers into three with a quantity of 2, and makes the library meaningful. |
| **Seed + reproducible report** | A sheet that has already been cut must be reproducible exactly. Seed plus report does that; a screenshot does not. |
| **Independent verifier** | The solver's own "it fits" is not evidence. Re-rendering each piece from the output file is. |
| **Pin a charm** | Lock a piece to a position and re-run the rest around it — the one manual override that keeps the automation useful when the operator knows something the geometry does not. |

Deliberately **not** included: cost estimation, material-usage analytics, a chat assistant,
a 3D preview, an undo history beyond the last nest. None of them help get a sheet cut.

## 15. Open questions

1. **Sheet dimensions.** The reference box was 7.125 × 6.03 in. Are the three metals all cut
   at that size, or does each have its own stock? The Settings stock presets need real
   numbers.
2. **Kerf.** What is the actual cut width on the machine these sheets go to? It sets the
   clearance default, and §2 shows the default is consequential.
3. **Jump-ring holes.** Currently treated as solid material, so nothing nests inside a ring.
   Allowing small charms inside large rings would raise density — is that acceptable on the
   machine, or does it risk parts dropping through?
4. **Legacy `.ai` files.** Any artwork saved without PDF compatibility cannot be read. Is
   that a real part of the library, or is everything modern?
5. **Grain or orientation constraints.** Free rotation is assumed. If any metal has a
   preferred orientation, the angle set needs constraining per sheet.
