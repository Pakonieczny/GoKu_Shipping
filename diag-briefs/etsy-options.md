You are one of 20 agents running a production-readiness diagnostic of the Golden Spike platform (repo Pakonieczny/GoKu_Shipping, deployed by Netlify from `main` to https://goldenspike.app). The owner, Paul, is about to push the platform to production and wants every user-accessible feature working, the platform stable, and every user's activity tracked as they move through the sorter (Charm Nest, charm-nest-1.html) and the connected apps (the stations: shipping, assembly, design, weld, sorting and their scan pages; Etsy Mail; Brites Ads; listing tools).

His words (26 Oct 2026): "I know that there are a lot of old and stale tests and testing apparatus which needs to be removed and only relevant tests should be run on the new and updated code/process. Also you keep on telling me there's old issues and errors in the application that you're seeing in your testing but because their previous to your current test, you keep on ignoring them. They all need fixing, most of them are old and stale testing infrastructure that's why I want you to remove it so we're not dealing with wasting time on old stale tests." He also does not want to wait many hours: work fast, don't over-engineer.

## Your area
Charm Nest (the sorter, charm-nest-1.html and its charm-nest-*.js files, netlify/functions/charmNest*.js): etsy listings, options and the charm repo. Your tests (66 files; shared helpers in tests/charm-nest/ whose names start with "_" and bridge-server.cjs are used by many areas, so change them only if you must):
tests/charm-nest/artwork-layer-body.cjs tests/charm-nest/assets.cjs tests/charm-nest/broken-charm.cjs tests/charm-nest/broken-heart-browser.cjs tests/charm-nest/broken-heart-profile.cjs tests/charm-nest/broken-heart-worker.cjs tests/charm-nest/catalog-repair.cjs tests/charm-nest/charm-nest-no-menu-in-order-window.cjs tests/charm-nest/charm-only-by-form.cjs tests/charm-nest/consistency-cloud.cjs tests/charm-nest/consistency-cloud-live.cjs tests/charm-nest/design-materials.cjs tests/charm-nest/etsy-cancel.cjs tests/charm-nest/etsy-images.cjs tests/charm-nest/etsy-link.cjs tests/charm-nest/etsy-link-batch.cjs tests/charm-nest/etsy-link-families.cjs tests/charm-nest/etsy-link-huggie.cjs tests/charm-nest/etsy-link-ui.cjs tests/charm-nest/etsy-pacing.cjs tests/charm-nest/etsy-production.cjs tests/charm-nest/fidelity.cjs tests/charm-nest/generic-sku.cjs tests/charm-nest/generic-sku-browser.cjs tests/charm-nest/guidance-retry.cjs tests/charm-nest/learned.cjs tests/charm-nest/learned-runtime.cjs tests/charm-nest/listing-pick-relink.cjs tests/charm-nest/listing-pick-relink-browser.cjs tests/charm-nest/listing-pin.cjs tests/charm-nest/lookalike.cjs tests/charm-nest/master-admin-door.cjs tests/charm-nest/master-beady-1023.cjs tests/charm-nest/master-corrected-1014.cjs tests/charm-nest/master-delete.cjs tests/charm-nest/master-fidelity.cjs tests/charm-nest/master-list-cache.cjs tests/charm-nest/master-newest.cjs tests/charm-nest/master-preview-fresh.cjs tests/charm-nest/master-redraw-remake.cjs tests/charm-nest/master-tab-resume.cjs tests/charm-nest/master-tab-robust.cjs tests/charm-nest/option-own-sku.cjs tests/charm-nest/option-own-sku-spec.cjs tests/charm-nest/option-tidy.cjs tests/charm-nest/option-tidy-browser.cjs tests/charm-nest/options-font.cjs tests/charm-nest/options-history-cards.cjs tests/charm-nest/options-history-data.cjs tests/charm-nest/options-history-ui.cjs tests/charm-nest/options-include-switch.cjs tests/charm-nest/options-link.cjs tests/charm-nest/options-on-title-line.cjs tests/charm-nest/options-sheets-data.cjs tests/charm-nest/options-studio.cjs tests/charm-nest/options-two.cjs tests/charm-nest/premade-listing.cjs tests/charm-nest/purchase-details.cjs tests/charm-nest/sample-text.cjs tests/charm-nest/sku-entities.cjs tests/charm-nest/sku-typo.cjs tests/charm-nest/variation-sku-first.cjs tests/charm-nest/zodiac-etsy-ui.cjs tests/charm-nest/zodiac-etsy-write.cjs tests/charm-nest/zodiac-sign.cjs tests/charm-nest/zodiac-two.cjs

## Setup (do this first, ~5 min)
1. You are in a fresh checkout of `main`. Work on the branch you were given (`claude/diag-etsy-options`): `git checkout -b claude/diag-etsy-options` if you are not already on it.
2. `npm install --no-audit --no-fund` (all dependencies and devDependencies; jsdom and canvas are needed by many tests). If the full install fails, `npm install --no-save --no-audit --no-fund jsdom@30 canvas@3` and install whatever single package a test then says is missing, with `--no-save`. Never change package.json or lockfiles.
3. Browser tests use the pre-installed Chromium: `export PW_DIR=/opt/node-tools/node_modules CHROMIUM=/opt/pw-browsers/chromium-1243/chrome-linux64/chrome` (check both exist; if the paths differ, find them with `ls -d /opt/pw-browsers/chromium*/chrome-linux*/chrome` and `node -e "console.log(require.resolve('playwright-core'))"`). Never run `playwright install`.
4. Run a test with `timeout 300 node <file>`. The container (yours alone) has about 4 cores: run 3 tests at a time (e.g. `xargs -P 3`). A test that needs more than 5 minutes is itself a finding: shorten it (a smaller seed, fewer repeated scenarios) without losing what it proves, delete it if it is stale or duplicates a faster test, or report it.

## What to do
1. Run every test of your area once, 3 at a time, within your first 20 minutes. Record pass/fail and how long each took.
2. For each failure, find the cause and classify it, then act:
   - STALE: the test checks behaviour the owner later removed or replaced (read the test's header quote and date, `git log -S`, the code's own comments quoting Paul, and for Sets of Sheets `docs/charm-nest/set-rules.md`), it duplicates a newer test that covers the same thing, it is a measurement/benchmark/report runner rather than a pass/fail test, or it belongs to abandoned work. DELETE it, with any fixture, golden file or helper used by nothing else, and any reference to it (package.json scripts lines are the exception: do not edit package.json; tell me instead).
   - OUTDATED: the feature exists but the test expects its old wording, layout, defaults or flow. UPDATE the test to the current intended behaviour (the newest owner quote wins). Never weaken a check of something that still exists just to make it pass.
   - BUG: production code is wrong. FIX it minimally in the code's own style (dense comments quoting Paul with the date, today is 26 Oct 2026; no refactors), and keep or add a test that proves it.
   - ENVIRONMENT: it needs something this container lacks (a credential, the internet, a live service). Make it runnable here without weakening it if that is simple (e.g. a default path); otherwise leave it and say exactly what it needs. Do not delete a valuable test for an environment reason.
   - FLAKY: fails under load but passes when run alone: rerun alone once; fix an obviously too-tight wait; otherwise note it.
   - SLOW: passes but takes more than 2 minutes: note its time; shorten it if that is quick and safe.
3. Passing tests stay, unless clearly stale by the rule above.
4. Rerun your whole area at the end: every remaining test passes, or you list why not.

## Rules
- Stay in your area. Edit tests outside it only for a shared helper you truly need, and keep that change tiny. If a production fix is in a large shared file (charm-nest-bridge.js, charm-nest-1.html, netlify/functions/charmNestLibrary.js), keep the diff small: other agents edit the same files and your branch is merged with theirs.
- Never edit deployment setup: package.json, lockfiles, .npmrc, netlify.toml, scripts/build-*.cjs, environment variables. A new browser JS file must be listed in scripts/build-public.cjs to be served (tell me rather than edit it); a new netlify/functions module must be classified in scripts/netlify-function-entries.json (tell me).
- Never call live services, Etsy, Firestore or paid APIs, and never use real credentials. Tests run against the repo's fakes.
- HARD time box: 60 minutes from start to push (the owner is waiting and does not want to wait hours). Spend at most 10 minutes on any one failure: if it is not solved by then, classify it, note what you found and move on. Prefer finishing your whole area over perfecting one test. At 50 minutes stop fixing, rerun what you changed, commit, rebase, push and report.
- Commit as you go with clear messages ending with these two lines:
  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01CHrvrKNNFNkEUFnrGNzR9T
- When done: `git fetch origin main && git rebase origin/main` (resolve conflicts carefully, rerun the tests a conflict touched), then `git push -u origin claude/diag-etsy-options`. Never push to main. Never force-push anything but your own branch.

## Your final message (I read it; keep it exact and short)
```
AREA: etsy-options
BRANCH: claude/diag-etsy-options @ <commit>
TESTS: run <n> · passing <n> · deleted <n> · updated <n> · still failing <n>
FIXES (production code): <file:function — what was wrong — what you changed>, one per line, or "none"
DELETED: <test — why stale>, one per line
UPDATED: <test — what changed>, one per line
STILL FAILING / ENV: <test — exact reason and what it needs>, one per line, or "none"
NOTES FOR THE MERGE: <files outside your area you touched; package.json script lines that name deleted tests; anything I must know>
```
