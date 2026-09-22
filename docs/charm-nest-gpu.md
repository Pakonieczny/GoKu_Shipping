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

The existing progress clock is unchanged. **Trials** count evaluated construction
passes. **Checks** count completed coarse/fine placement tests, including repair,
wall passes, pinned fits and calibration. Repeated tests count as work; position
checks are not complete layouts or unique arrangements. Worker totals are
absolute, monotonic snapshots, merged once at completion. No progress writes
are added to Firebase, and the pipeline makes no Etsy requests.

**Compare speed** runs a bounded sample in its own disposable worker, using up
to 12 loaded charms or built-in reference shapes. It reports measured checks/s,
trial count, verified piece count and fill; GPU startup is included. It does not
publish a layout, modify a sheet, change the selected pipeline or make API calls.
Cancel, closing Settings or starting nesting terminates the comparison.

## Validation

- `node tests/charm-nest/search-metrics.cjs`
- `node tests/charm-nest/gpu-control.cjs`
- `node tests/charm-nest/gpu.cjs` (optional native Dawn `webgpu` test runtime;
  `WEBGPU_TEST_MODULE` may point to its `index.js`; no production dependency)

WGSL execution was tested with Mesa llvmpipe through Dawn, including independently
scored top-32 reduction, collisions, rose-gold remnants, pins, verification,
cancellation and fallback. A 12-shape reference comparison on this software
adapter measured about 1.26 million CPU checks/s versus 0.91 million GPU checks/s
(2.22 s versus 3.02 s including GPU startup), with 12/12 pieces and equal fill.
These are software-renderer results, **not physical-GPU performance estimates**.
Use the Settings comparison on the operator's hardware for a relevant result.
