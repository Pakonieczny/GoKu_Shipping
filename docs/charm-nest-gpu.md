# Charm-sheet GPU processing

Settings → Charm-sheet processing selects CPU (the existing default) or GPU for
the next nesting run. The geometric challenger in the learned workflow also
honors this setting. The experimental AI placement engine is separate.

One solver worker owns the WebGPU device. It dispatches all candidate positions
and rotations together, reduces them to the best 32 positions per rotation on
the GPU, then reads back only that shortlist. Fine collision/contact scoring,
edge-slot searches and independent output verification remain in CPU workers.
Other configured solver workers remain independent CPU searches; the existing
best-layout selection and saved incumbent are retained. The main thread does
not execute GPU searches or wait synchronously for results.

The rotation cache is capped at 32 MiB, individual buffers at 64 MiB, total
working buffers at 128 MiB and each scan at eight million positions. Unsupported
devices, software adapters, oversized batches, device errors and timeouts fall
back to CPU. The first three eligible placement searches, and periodic later
probes, compare GPU plus refinement against the CPU search on the same geometry.
GPU must be at least 10% faster to continue. Geometry preparation and precise
refinement still limit overall speedup; this is not a promise of denser packing.

GPU trials now alternate the existing construction with bounded lookahead over
up to seven eligible charms. Each branch batches every remaining group member's
rotations, retaining two distinct fine-validated proposals per member and up to
three branches per depth. Groups share the whole sheet; there are no artificial
chunk boundaries. The planner has a 1.2-second allowance inside the existing
search deadline, and can return a legal prefix of at least two placements.

Pruning removes byte-identical rotated masks, identical partial arrangements,
collisions and area-bound branches that cannot beat the current group count.
The beam also discards lower-ranked alternatives heuristically. Sampled fit
probes for the remaining charms affect ranking only: they cannot prove that a
charm is impossible to place. This is bounded search, not exhaustive enumeration,
and there is no assumed 90% pruning rate. Stable variant batches reuse GPU
uploads. Grid memory limits reduce the beam or leave a large sheet to baseline.

The existing progress clock is unchanged. **Layout trials** count completed
construction passes across workers; interrupted passes are excluded. The main
counter no longer promotes low-level position checks as useful exploration.
Its tooltip separates complete group plans, partial branches and diagnostic
coarse/fine checks. Repeated tests count as work, not unique arrangements.
Worker totals are absolute, monotonic snapshots, merged once at completion.
No progress writes are added to Firebase, and the pipeline makes no Etsy requests.

**Compare speed** runs a bounded sample in its own disposable worker, using up
to 12 loaded charms or built-in reference shapes. Both modes receive the same
six-second search budget. It reports completed trials, elapsed time, verified
piece count and the largest clear rectangle as a percentage of sheet area.
That rectangle excludes existing rose-gold cuts; empty already-cut material is
not reported as reusable stock. GPU startup is included in elapsed time. The
comparison does not publish a layout, modify a sheet, change the selected
pipeline or make API calls. Closing Settings or cancelling terminates it.

## Validation

- `node tests/charm-nest/search-metrics.cjs`
- `node tests/charm-nest/gpu-control.cjs`
- `node tests/charm-nest/lookahead.cjs`
- `node tests/charm-nest/gpu.cjs` (optional native Dawn `webgpu` test runtime;
  `WEBGPU_TEST_MODULE` may point to its `index.js`; no production dependency)

WGSL execution was tested with Mesa llvmpipe through Dawn, including independently
scored top-32 reduction, collisions, rose-gold remnants, pins, verification,
cancellation and fallback. The group test records each trial's actual placements
and asserts different arrangements occur, completed group plans occur, baseline
trials still compete, source grids are untouched, and pins, remnants, whole-order
membership, chronological priority and the area cap survive group reordering.

A final 12-shape reference comparison gave both modes the same six-second
search budget (about 6.1 seconds measured, including GPU startup). Both placed
12/12 pieces. CPU completed seven layout trials and left a clear rectangle
covering 38.64% of the sheet. GPU group search completed three layout trials and
12 group plans, leaving 34.96%. This did **not** demonstrate a speed or packing
gain. These are software-renderer results, not physical-GPU estimates. The
existing CPU pipeline, competing baseline and hardware calibration remain;
use Settings on the operator's GPU for a relevant comparison. More low-level
checks do not establish better packing.
