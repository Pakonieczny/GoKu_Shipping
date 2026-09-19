# Learned nesting workflow

The small **Learned on/off** switch in the Nest header selects a separate process for all newly started sheets. Standard remains the default. The switch requires a paired laptop runner before it can be enabled and cannot change during active/queued nesting. Automatic and manual runs use the same selected flow.

This is an experimental integration of the authors' GFPack++ neural network, **not a language model prompting a random search**. The pretrained weights learned on dental polygons, not this charm catalogue. Better packing is an empirical question; this integration does not promise 74% or justify ten hours of compute on its own.

## Process

1. Check the laptop runner before clearing the previous sheet.
2. Start two browser workers on one sheet at a time. One continuously runs the standard solver; the other measures a short standard baseline and requests two learned proposals.
3. The Python runner executes the published graph/attention model and 128 diffusion steps. It proposes translations and continuous rotations. No customer/order text, artwork or credentials are sent to an AI cloud. Only simplified polygon contours and opaque piece IDs go to the local runner.
4. Translate the proposed arrangement into the sheet coordinate system without scaling or stretching pieces. Repair invalid placements with the existing geometric tools. Fine rotation refinement uses adaptive neighborhoods down to 0.1 degrees; raster precision still limits manufacturing accuracy.
5. Remove small clusters around empty pockets, reinsert them together with the oldest missing order, and refine their rotations. The standard worker sends its better layouts to this worker too. Every accepted learned/refined incumbent passes the independent 6 px/pt verifier.
6. Retain the best result from either route. Existing Illustrator/PDF writing, independent written-file verification, saving, order assignment, stock and set-release rules run afterward.

Oldest **whole orders** determine sheet membership; geometric placement order within that membership remains flexible. Pinned positions are unchanged; a younger pin cannot bypass an unplaceable older order. Existing solid-gold custom stock sizes and manual inclusion still apply. A time ceiling, stop or failed model retains unfinished work on the current sheet instead of silently treating it as full. A verified plateau can continue through the existing overflow process.

The standard challenger provides a safeguard within this hybrid run; sharing CPU with inference/refinement can still make it slower than running Standard alone. The two approaches are compared in the report, not assumed equivalent in cost.

## Laptop setup

Python 3.11 or 3.12 and Git are needed. The adapter supports CPU on Linux/macOS and CUDA when a compatible PyTorch installation is available. The tested CPU configuration was Python 3.12, PyTorch 2.14 CPU and PyG 2.8. Apple MPS has not been validated; Apple laptops use CPU by default.

From a checkout of this repository:

```bash
python3 -m venv tools/learned-nest/.venv
source tools/learned-nest/.venv/bin/activate
python -m pip install -r tools/learned-nest/requirements.txt
```

Download the authors' archive from the [dataset/checkpoint link in their repository](https://github.com/TimHsue/GFPack-pp#3-data-preparation). Then run, replacing the archive path if necessary:

```bash
python tools/learned-nest/setup.py ~/Downloads/gfpp.zip
python tools/learned-nest/runner.py \
  --source tools/learned-nest/research \
  --weights tools/learned-nest/research/dental.pth
```

`setup.py` downloads only three pinned source modules and extracts only the checkpoint. It verifies SHA-256 digests, normalizes source line endings, and does not load the training dataset. PyTorch loads the checkpoint with `weights_only=True`. No research sources or weights are redistributed in this repository. The reviewed upstream snapshot does not contain a license file; availability of the download does not itself establish commercial-use rights. Resolve those rights with the authors before production use of their model.

The runner prints a pairing key. In the app, open **Workspace → Settings → Learned packing**, enter that key, click **Check connection**, and save. Then enable **Learned** in Nest. Permit local-network access if the browser requests it. Keep the runner terminal open. Restarting it generates a new pairing key.

The runner binds only `127.0.0.1:8766`, validates the Host and allowed app Origin, requires the pairing key, and serializes model inference. Production app origins are explicitly allowed; local development can add an origin with `--origin http://localhost:PORT`. It has no shell/file-operation endpoints and does not log geometry or pairing headers. The key is stored only in this browser's local storage, separately from synced settings and workspace snapshots.

The laptop service cannot be started remotely by the deployed web app. If it is absent, the toggle opens setup settings; it does not pretend that learned inference is available.

## Longer runs and recovery

Settings offer **10 minutes, 1 hour or 10 hours** as a ceiling and a separate no-extra-pieces window of **2, 5, 15 or 60 minutes**. Default: ten-minute ceiling, five-minute no-gain window. A million standard trial attempts is an upper bound, not a promised throughput or a stopping target. No artificial variation count is displayed.

A rearrangement with the same piece count can be useful for the next insertion, but it does not reset the meaningful-gain timer. The no-gain deadline stops both workers, including a challenger still making cosmetic changes. Full placement ends early. Keep the laptop awake and the tab open; a sleeping or throttled browser cannot do continuous work.

The workspace checkpoints the verified best layout, model provenance, baseline, elapsed active time, measured candidate count and improvement history. Refresh restores the sheet; **Re-nest** resumes its matching checkpoint. Geometry, pins, stock, order chronology, clearance, fill ceiling or model-weight changes invalidate it. A completed time allowance requires increasing the ceiling to continue. Worker caches and pairing keys are excluded from checkpoints.

Live progress names model inference versus geometric refinement, current placed count, gain against the standard challenger, and time since an extra piece was added. The result tooltip and saved nest report retain model identity, weight hash, selected route and gain. If all pieces already fit, model inference is skipped because density cannot improve by rearranging the same fixed-size pieces on the same sheet.

## Research boundary

- [Original GFPack paper](https://arxiv.org/abs/2310.19814): learned gradient fields for irregular packing.
- [GFPack++ paper](https://arxiv.org/abs/2406.07579): follow-up with rotation support.
- [Authors' implementation](https://github.com/TimHsue/GFPack-pp), pinned at `402a9f9a44f2c501f19a4291fca15f8acc943902`.
- Checkpoint SHA-256: `e3585572100e61c5f3112a8925e7add420b70c8c7a8f23e29be97788ad3c5643`.

This adapter uses the published neural sampler and replaces the authors' native dental-strip gap-removal stage with this application's mask repair, constraints and verification. It is **not a reproduction of the complete paper benchmark**. Feature contours are approximate; full original masks remain the manufacturing authority. A uniform feature scale maps sheet height to the training strip's 1205 units; this is domain adaptation, not evidence of charm-specific training. The original artwork sizes, clearance settings and existing 0.975 output-scale behavior are unchanged.

## Measurements and validation

On the supplied 81-charm reference, CPU inference generated two proposals in approximately 4–5 seconds. This measures proposal generation only; it excludes geometric repair and export.

A deliberately crowded **100 × 45 mm** version of that reference exposed a regression in the first learned-only prototype:

| Route | Placed | Fill | Search time |
| --- | ---: | ---: | ---: |
| Short standard baseline | 57 | 66.47% | about 21 s |
| Learned proposals + local refinement | 63 | 70.53% | about 109 s |
| Standard solver alone | 66 | 72.48% | about 109 s |

A subsequent integration run exercised both production worker scripts against the real loopback model service: the hybrid refined 60 pieces (68.60%) versus its concurrently running standard challenger’s 57 (66.47%). Inference took 5.1 seconds; refinement stopped after about 162 seconds at a 60-second no-gain threshold. This validates the exchange and inference path, not superiority to standalone Standard; host contention and shared CPU budgets differ between runs.

Both final layouts passed the 6 px/pt overlap/bounds verifier. Consequently the shipped flow retains an active standard challenger rather than replacing it. The six-piece improvement over a short baseline is **not evidence that AI beats Standard at equal time**. No ten-hour benchmark or general 74% claim has been established. Original 100 × 50 mm reference results from the earlier standard solver already placed all 81 charms at about 73.62%.

Run the focused checks from the repository root:

```bash
node tests/charm-nest/learned.cjs
node tests/charm-nest/learned-runtime.cjs
python tests/charm-nest/learned-runner.py
node tests/charm-nest/solver.cjs
node tests/charm-nest/packing-runtime.cjs
node tests/charm-nest/workspace.cjs
node tests/charm-nest/releases.cjs
node tests/charm-nest/bridge-units.cjs
node tests/charm-nest/functions.cjs
```

They cover real geometric feasibility, FIFO whole orders, checkpoint invalidation, stop/resume, malformed/offline model responses, unnecessary-inference avoidance, monotonic best selection, challenger exchange, no false gain attribution, plateau cancellation, local-service authentication and origin checks, and existing manufacturing lifecycle regressions. Model fixtures in unit tests are explicitly labeled test data.

Browser click-through verification remains unavailable in this session: browser control times out while listing tabs (`CDP refresh tabs`, 20 seconds). This is not an app password issue. CPU inference and the worker/service protocol are tested separately; those checks do not substitute for browser UI, browser local-network policy or authenticated production storage verification.
