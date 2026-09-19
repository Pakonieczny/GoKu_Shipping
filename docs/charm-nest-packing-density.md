# Packing density investigation — 2026-09-19

## Reproduced cause

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
