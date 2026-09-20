# Packing density investigation — 2026-09-19

## Partial-sheet offcuts — 2026-09-20

The old contact score attracted sparse batches to every sheet wall, leaving a U-shaped layout and unusable central space. Sparse searches now minimise the growing extent of a strip from the left edge (top edge on portrait stock), using contact to fill that strip. Up to eight restarts improve the strip even after every piece fits, within the existing time/trial limits. Alternate ordinary searches remain available for awkward shapes. Piece count, FIFO, whole-order membership, pins, dimensions, clearance and verification stay enforced. Equal-count results, including results from separate browser workers, prefer the largest rectangular offcut over small raster-density differences. Full queues keep the existing dense first-pass search.

Controlled test using the supplied `Half_Sheet.ai`: 43 detected charms, 100 × 50 mm stock, 1.5 pt inset, -0.5 pt clearance, 30-degree rotations, grids 2 / 0.5 pixels per point, seed 1. The prior layout occupied 99.84 mm of the width. The new layout occupies 53.62 mm, leaving about 46 mm clear on the right, with all 43 pieces and no overlap/outside pixels at verification resolution 6. Subsets of 28 and 20 pieces occupy 40.39 mm and 33.69 mm respectively. The original file is not modified. The generated Illustrator output was rendered and visually checked. The dense 81-charm reference below still fits 81/81 in one trial at 73.62%, with zero overlap/outside pixels.

Regression tests: `node tests/charm-nest/partial-sheets.cjs`, `node tests/charm-nest/packing-runtime.cjs`, and `node tests/charm-nest/solver.cjs`. These cover variable strip sizes, portrait stock, unchanged dimensions, positive clearance, exact pins, cancellation and worker result selection. Existing saved sheets require re-nesting to get the new layout; production browser interaction was not used for these checks.

## Supporting edge alignment — 2026-09-20

Following the production screenshot feedback, silhouette edge contact now contributes at **0.25** of neighbour contact weight, in both candidate search and final layout/refinement scoring. The near-contact term remains 0.5 and each distinct neighbouring charm still contributes 0.05. Walls are recorded separately from charms; only real contour pixels touching the inset band or physical boundary earn edge credit, including zero-inset stock. Corner credit is bounded, not inflated by the collision grid's conservative out-of-bounds count. The boundary construction proposal remains available. Piece count, compact offcut ranking, FIFO, whole orders, pins, dimensions and clearance remain unchanged.

The 81-charm dated reference still seats 81/81 at 73.62%, with the same final layout and zero overlap/outside pixels. The 43-charm half-sheet reference occupies 54.50 mm, leaving 45.50 mm clear; mean neighbour score improves from 0.4794 to 0.4915, and mean edge contact from 0.0163 to 0.0210. Its independent resolution-6 geometry check also reports zero overlap/outside pixels. `neighbors.cjs` checks the smaller edge bonus, corners, zero inset, actual concave edges, immutable wall occupancy and clones; existing solver/partial-sheet/worker/release tests pass.

## Reproduced cause

### Neighbour contact refinement — 2026-09-20

Neighbour searches now use rings around the actual rasterised silhouettes at both search resolutions, count distinct adjacent charms, and give extra weight to contact within one fine-grid cell. Walls are excluded from that measured score. Rectangles only screen impossible fits and guide one boundary-seeded construction proposal; they never replace the cut silhouette in collision checks or final contact measurement. Keeping that initial proposal matters: a strong greedy neighbour-count bonus alone reduced the dense reference to 74/81 within 60 seconds. The final solver retains construction diversity, then ranks equal-count layouts by usable offcut and actual silhouette contact, and refines up to twelve poorly connected pieces. Refinement preserves all pieces, their angles, dimensions, pins and clearances and accepts only a contact improvement without materially consuming the offcut.

The completed dense-reference check retains 81/81 at 73.62% and improves its mean contact score from 0.5837 to 0.5910 through five accepted moves. The supplied half-sheet reference retains 43/43 within 53.98 mm of the 100 mm width, improving its contact score from 0.4687 to 0.4794. Both have zero overlap/outside pixels at verification resolution 6. `neighbors.cjs` checks true concave silhouettes, overlapping bounding boxes without real adjacency, distinct neighbours, closer gaps, wall exclusion and self-exclusion; `partial-sheets.cjs` continues to check variable offcuts and pins.

The FIFO change in `475206b` made order dates control the geometric insertion sequence, as well as sheet membership. It also disabled the 5-degree finishing search and neighbourhood repair for dated orders. Loose Illustrator imports could arrange large/awkward shapes first; production orders could not. A stalled leading worker also cancelled other seeds before their search budgets expired.

Dates now determine the oldest whole-order prefix eligible for a sheet. The solver can arrange that prefix by shape, with randomized restarts and gap repair. A trial that fails an older order discards every younger order; repairs only attempt the first waiting order. Area limits, pinned placements, whole orders, and downstream geometric verification remain enforced. The pool waits for other searches after one stalls; only a complete layout cancels the others. A failed last worker now releases the best successful result instead of leaving the pool waiting.

## Controlled reference result

Input: the supplied `test4_81_Pcs.ai`. The parser detects **81 charms**, not the 82 shown in the comparison screenshot. The original file is not changed or committed to this repository.

All runs use the same silhouettes at 6 pixels/point, 100 × 50 mm stock, 1.5 pt inset, -0.5 pt clearance, 30-degree rotations, solver grids 2 / 0.5 pixels/point, seed 1, 74% fill ceiling, 60-second budget, and 2,000-trial limit. The dated control assigns each charm a separate order with monotonically increasing dates; those are controlled test dates, not customer order dates.

| Solver / order metadata | Placed | Reported fill | Trials | End |
| --- | ---: | ---: | ---: | --- |
| Previous solver (`dea0fac`), no dates | 81 / 81 | 73.62% | 1 | Complete |
| Previous solver, increasing order dates | 78 / 81 | 72.37% | 51 | 60-second budget |
| Corrected solver, same increasing dates | 81 / 81 | 73.62% | 1 | Complete |

All three layouts pass the existing geometric verifier at resolution 6 with zero reported overlap/outside pixels under the configured clearance tolerance. The corrected reference result uses **no AI advice**. This isolates the improvement to the geometric search correction.

Each of the 81 reference charms was also exported with the application's individual-charm writer, re-imported, grouped, and measured again. Every exported file grouped as one charm; no silhouette area changed by more than 0.5%. This provides no evidence of padding introduced by that export/import path. It is not an audit of every existing production-library file.

The fill percentage measures the solver's clearance-adjusted raster footprint, not all white pixels visible in a preview. The existing 2.5% output shrink remains unchanged. Different shape mixes, multi-piece orders, and FIFO constraints can legitimately produce different densities; 74% is a ceiling and target, not a guaranteed result for every queue.

## AI guidance

The default browser engine can request one background consultation when a search has run at least 12 seconds, still rejects pieces, and remains more than one percentage point below its ceiling. Settings → AI packing guidance can disable it.

The existing configured Claude model receives a numbered contact sheet of up to 120 oldest pieces, dimensions, and the current layout. It suggests up to 24 shape priorities and three rotations per shape. It does not select sheet membership or write placement coordinates. Guided priorities participate in every third trial alongside ordinary restarts; all placements still use geometric collision checks. Suggested rotation footprints now include explicit area counts, also fixing missing area counts in the pre-existing finishing/repair variants.

Advice is cached for the same queue and stock geometry. A saved pending agent ID resumes polling after refresh; stale worker/job messages are ignored. Pending advice can extend the stall window but cannot extend the configured total search budget. Advice that arrives after completion is saved for a future re-nest. Failure leaves geometry running. This integration has mocked API and lifecycle coverage; a live model's effect on density has not been measured in this release.

## Verification

Node regression suites: `solver.cjs`, `functions.cjs`, `packing-runtime.cjs`, `workspace.cjs`, `releases.cjs`, and `bridge-units.cjs` in `tests/charm-nest/`.

Coverage includes concave shapes where strict insertion order blocks a feasible layout, impossible older pieces, whole orders, pinned priorities, interrupted preparation, reclaimed space, finite/capped rotation areas, adversarial AI advice, worker failures, stale results, cached consultations, and pending consultation resumption.

Browser interaction verification remains unavailable because the browser connection times out while listing tabs. The local Illustrator parsing and solver benchmarks are computational tests, not a substitute claim of completed browser testing. Existing saved sheets are not silently rewritten; re-nesting uses the corrected solver.
