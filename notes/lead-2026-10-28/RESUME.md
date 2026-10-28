# LEAD RESUME NOTES — read this first after a reset / compaction (resumed 06:32 UTC 28 Oct after the usage limit)

Paul is asleep. He wants ALL work continued overnight, every agent checked every 3 minutes, everything tested (properly, not
excessively) and pushed to main. Standing rules: push to main after testing without asking; never force-push main; never edit
package.json / lockfiles; netlify.toml ONLY the service-worker header (approved); new served files of the Design & Engrave
update and the local-first files are APPROVED (add to scripts/build-public.cjs). No live services/credentials in tests; sandbox reads
read-only with "sandbox":true. Commits end with:
  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01CHrvrKNNFNkEUFnrGNzR9T
No model name anywhere else. Brites Ads excluded. Never rm -rf variable paths.

## Where everything lives
- Repo: /home/user/goku_shipping (origin = github.com/Pakonieczny/GoKu_Shipping). Live site: https://goldenspike.app (sandbox world).
- Scratchpad: /tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/scratchpad  ($SP). A COPY of these notes, the briefs and
  the scripts is pushed on branch `claude/lead-notes` under notes/lead-2026-10-28/ (use it if /tmp was wiped).
- Briefs every agent reads: $SP/briefs/common.md, paul-37.md (Paul's words + 37 items + decisions D1–D11), interfaces.md (contracts),
  research-70-30.md. Evidence: briefs/*.webp/png, tl2.json, pieces2.json.
- Live board (dashboard artifact): https://claude.ai/artifact/RdLASAbUtTQMqU1Mb6ExLG. Rows in $SP/board/agents.json.
  Refresh: `python3 -I $SP/board-live.py` → Artifact publish {url, file_path: $SP/board/agents.json, asset:true} → ArtifactData update
  url, collection "datasets", doc_id "agents", if_version <current>, data {source:{format:"json",kind:"file",name:"agents.json",url:"<blob url>"},
  updated:{at,by:"Claude"}}. Last version written: 91 (next write if_version 91; if refused, the error names the current version).
- Plan doc for Paul (local-first): https://claude.ai/artifact/GykrjmCTLTbcohMCKBrkyZ (decisions recorded).

## Scripts ($SP)
- int/slot.sh: shared browser-test queue, 3 slots, FIFO, SLOT_PRIO=1 skips the line, every test capped at 600 s (SLOT_MAX). ALL browser tests go through it.
- agent-mon.py (one line per agent; AGENTS list + board-live-extra.json), board-live.py (fills board rows from agent transcripts), lead-now.sh (lead row text).
- Monitor command (re-arm every 30 min with Monitor, timeout 1800000):
  SP=...; while true; do sleep 180; $SP/lead-now.sh >/dev/null 2>&1; B=$(python3 -I $SP/board-live.py 2>&1 | tail -1); echo "=== $(date -u +%H:%M) check (load $(cut -d' ' -f1 /proc/loadavg)) · $B · board json refreshed: publish it"; python3 -I $SP/agent-mon.py 2>&1 | grep -v "DONE" | sed -E 's/ · says:.*//; s/last: (Bash|Read|Edit|Write|Grep|WebSearch|WebFetch): ?/now: /' | cut -c1-150; done
- resolve-union.py <files>: merge conflicts where both sides added lines (script tags, build list) or re-tokened ?v=. resolve-tokens.py: ?v= only.
- int/live-check.cjs: 7 pages load clean. Live verify = sha256 of each changed served file vs `git show origin/main:<f>`, then live-check.
- If agents hang holding slots (load ~0, waits > 5 min): find holders: for f in slot1 slot2 slot3: ino=$(stat -c %i $SP/locks/$f); grep ":$ino " /proc/locks → pid → ps --ppid. Kill the idle test, tell its agent.

## Live on main: 8626631 = nav round 5 (live verify running) on top of b7c15cf batch 8 (verified live), 14ca7b2 batch 7.
- 89f2819 Manual Control instant · b2608ff navigation rounds 2+3, Charm Editor Save fix (Saving at once, sheet files in background),
  bar necklaces FRONT by default (BARFRONT) · 077f21c Approve never silent / never stuck · d58eea9 Design & Engrave batch 1: editor
  extension points + live readouts + Ctrl/Alt centre resize + smart guides (feedback), Blend/Warp/Distort (warp), image trace (trace).

## Integration branch for Design & Engrave: claude/de-integration (worktree $SP/wt-de-int), head 5d240fc = d58eea9 + vimport.
BEFORE EVERY PUSH TO MAIN: (0) `python3 -I $SP/tokbump.py . origin/main HEAD` — every changed served file needs a new ?v= token (service worker); (1) `python3 -I $SP/tokcheck.py . HEAD origin/main <merged branch refs>` — no ?v= suffix part of a merged branch may be missing (rf1→rf2 on engrave-preview/worker was deliberate); the SERVICE WORKER is live, so a lost token = an old file served. resolve-union.py now merges every token on a line (it used to merge only the first). (2) `node scripts/build-netlify.cjs` must exit 0 (a new netlify/functions file must be classified in scripts/netlify-function-entries.json; batch 3 did not deploy for 15 min because _designLog.js was not; fixed in 4003913).
Procedure per finished workstream: `git fetch origin <branch> && git merge --no-commit --no-ff FETCH_HEAD`; conflicts →
`python3 -I $SP/resolve-union.py <files>`; commit with the attribution lines; when a few have landed, test on the priority lane:
node: charm-editor-model art-edit-model public-assets (+ the workstreams' node tests); browser (SLOT_PRIO=1 slot.sh): the workstreams'
own ce-*.cjs + charm-editor-browser charm-editor-engrave charm-editor-back art-edit-page engrave-editor-words; then
`git fetch origin main; git merge-base --is-ancestor origin/main HEAD || merge origin/main; git push origin HEAD:main`; verify live; update board.
Next batch candidates: vimport (merged), then designs → library/masters → sendsheet + custompool + emplog together (they depend on each
other), pen, ops, rulers, image (needs trace, merged), text, extras, tab, refit.

## Agents running at ~03:15 UTC (each on its own branch claude/<x>, worktree $SP/wt-<x>); all told to WIP-commit + push every ~20 min
| board row | agent id | branch / worktree | scope |
|---|---|---|---|
| local-store-p1 | a42236c45a7daeff2 | claude/local-store / wt-store | Phase 1 local record store (Step B gates pass: sheet reopen 77 ms); merging main, then hand back → test + ship |
| de-rulers | a909d6b516f6d76c5 | claude/de-rulers / wt-de-rulers | items 3 6 7 |
| de-pen | a925a9f2929572a8f | claude/de-pen / wt-de-pen | item 9a pen/anchors |
| de-ops | a3ce05d237db781e5 | claude/de-ops / wt-de-ops | item 8 pathfinder etc |
| de-image | ac4cbd9c035695d58 | claude/de-image / wt-de-image | item 11 image import (uses trace) |
| de-text | ae828a5d729f6efa1 | claude/de-text / wt-de-text | items 14 15 emoji + scripts |
| de-designs | af114bb2ccb5f706c | claude/de-designs / wt-de-designs | items 18 21 27 design library data |
| de-library | acc9d6120b4748ee1 | claude/de-library / wt-de-library | items 19 20 right panel |
| de-masters | acbce20e63df93ae2 | claude/de-masters / wt-de-masters | items 16 10 open any charm, blank designs |
| de-sendsheet | a6ecf232b267d8e9d | claude/de-sendsheet / wt-de-sendsheet | items 17 22 24-26 28 29 Send to sheet |
| de-custompool | a4d3307fda8a88af0 | claude/de-custompool / wt-de-custompool | items 27 28 pieces on sheets |
| de-tab | a28303212b071279b | claude/de-tab / wt-de-tab | items 30-33 Design & Engrave tab |
| de-emplog | aae0c82c2db2e93d9 | claude/de-emplog / wt-de-emplog | items 34-37 employee tracking |
| de-refit | a7764b20331f1122f | claude/de-refit / wt-de-refit | card never blank while refitting |
| de-extras | a5f64f2a38f32df82 | claude/de-extras / wt-de-extras | shapes, text on path, clip, laser check, Ctrl+Y |
| fix-seals | a2200fb3aa0ebf322 | claude/fix-seals / wt-fix-seals | seals: only Order Held + Order Released (green); THEN Release button always shown in the order window for held orders (Paul 03:20 UTC, order 4202871419: most held orders show no Release) |
| perf-nav-4 | a2ce3dc0daa28facb | claude/nav-instant-4 / wt-nav4 | navigation round 4 targets |
| bar-waiting | aa6569d65415c6c37 | claude/bar-front-waiting / wt-bar-waiting | waiting bar orders switch to front |
| local-sw-p4 | a7883fa11af818737 | claude/local-sw / wt-sw | Phase 4 service worker |
| fix-approve-held | af959c47001f89f4c | claude/fix-approve-held / wt-fix-held | Approve fails after hold/release (order 4212020155) |
Finished: feedback, warp, trace, vimport, research, approve, MC, nav 1-3 (all merged/live except vimport, in integration).

## If the agents are gone after the reset (container recycled / agents stopped)
For each row above that is not done: `git fetch origin claude/<x>`; `git worktree add $SP/wt-<x> origin/claude/<x>` (or -b from origin/main if the
branch was never pushed) and re-spawn an Agent (general-purpose, run_in_background) with: "Continue workstream <x>. Read $SP/briefs/common.md,
paul-37.md, interfaces.md. Your previous work is on branch claude/<x> (worktree $SP/wt-<x>): read `git log origin/main..HEAD` and the diff,
finish the scope <scope>, test (slot.sh with time limit inside), push, hand back." Max 20 agents at once. Update agent-mon.py AGENTS and
board-live-extra.json with the new agent ids.

## Open questions for Paul (ask when he is back)
- ANSWERED by Paul 03:39 UTC: "The refit if necessary, must be in the exact same place with a variance of one percent from where I actually placed it. The rotation needs to be also within one percent accurate to where I placed it." → RULE: a refit after a Charm Editor save keeps his placement: centre within 1% of the charm's size of where he placed it, angle within 1% of his angle (a level word stays level). Only the size may shrink if needed. Sent to the refit agent (a7764b20331f1122f) as a follow-up; if it is gone, re-spawn refit with this rule.
- Phase 5 push (option a) needs a Firestore security rule in the console + a vendored Firestore SDK file.


## Phase 2 handed back 08:50 (claude/local-sync c07cea4; calls/min 1 tab Orders 36→20, 2 tabs 77→24, tab+windows 86→33; other computer's change in 1–2 s). Sent back for: bumpRev best-effort + revision split by kind (Firestore ~1 write/s per doc), own writes broadcast at once to sibling tabs (library-realtime ≤0.5 s), complete-realtime (fails on main too). Ship after.
- Phase 3 agent ae248b1326745d816 started 08:52 on claude/local-outbox (wt-outbox) from claude/local-sync.
## Remaining: Phase 2 sync worker (ac1971b04ea413b37, claude/local-sync) and nav-6 (a2ce3dc0daa28facb, piece-row-review-buttons at 390 px). Then Phase 3 (outbox, optimistic writes), then Phase 5 needs Paul (Firestore rule + vendored SDK).
- a41a92d9950823b21 now on claude/eff-inbox-chart: 'Sent per day' chart is a black block when empty.
- Phase 2 agent ac1971b04ea413b37 started 07:03 on claude/local-sync (wt-sync) from claude/local-store.
- designs follow-up (af114bb2ccb5f706c resumed): claude/de-designs-2 = accept stroke/fill true in clean(); test world Master.load race.

## Handed back, waiting to merge into claude/de-integration
- nav-4 (a2ce3dc0daa28facb) DONE 08:03: claude/nav-instant-4 7565096 (5 :has() rules → classes; order window open p50 196→51 ms, Next 180→26, Library chip 330→37). BATCH 7. Round 5 sent (claude/nav-instant-5: Orders/Review first visit <100 ms; merge-sheets failing on main).
- masters-2 (acbce20e63df93ae2) DONE 08:00: claude/de-masters-2 d547987 (contains main + de-tab). ce-masters 20/20, de-tab 14/14. BATCH 7.
- bar-front-2 (aa6569d65415c6c37) DONE 07:55: claude/bar-front-2 a94a2d6 (decided bar lines say what they engrave; engrave-ui-faces fixed 27/0). BATCH 7.
- de-tab (a28303212b071279b) DONE 07:52: claude/de-tab 7d14e6b (tab renamed Design & Engrave, Charm Designer bar, Open in Charm Editor for any piece, editor Back). de-tab 14/14. BATCH 7. Follow-ups sent: masters-2 (pass o.back, openDesign(id,version,o)), library-2 (acc9d6120b4748ee1: recent/openDesign/onChange on claude/de-library-2).
- de-designs-2 (af114bb2ccb5f706c) DONE 07:48: claude/de-designs-2 7c72de3 (shape paint true accepted; test world Master.load race; text-on-path/shape/clipOf kept). designs-server 102. BATCH 7.
- de-library (acc9d6120b4748ee1) DONE 07:43: claude/de-library dca16a8, ce-library 17/17. BATCH 7 (with de-tab). Follow-up sent to masters (acbce20e63df93ae2 → claude/de-masters-2): skip the Saved card when the session moved on.
- local-sw P4 (a7883fa11af818737) DONE 07:24: claude/local-sw 3064100. New charm-nest-sw.js (approved), netlify.toml header block (approved), service-worker.cjs 25 checks. TOKEN DISCIPLINE now matters (changed file without ?v= bump stays cached). Ship with batch 6 after Phase 1.
- de-refit (a7764b20331f1122f) DONE 07:20 with PAUL'S RULE: claude/de-refit 24406dc. Keeps placement (same → size only → centre ≤1% → afresh with a note); SEPTEMBER stays 5.33 pt level. charm-edit-refit 13/13. SHIP after batch 4 + Phase 1 (tests: charm-edit-refit, charm-editor-engrave, engrave-editor-words, engrave-preview, engrave-front-art, charm-editor-back, engrave-worker).
- de-extras (a5f64f2a38f32df82) DONE 07:14: claude/de-extras 58ff4da, ce-extras 21. ⌘Y is now Outline view (redo only ⇧⌘Z; charm-editor-browser.cjs line changed). Text-on-path fields need the designs follow-up (sent). BATCH 5.
- de-sendsheet (a6ecf232b267d8e9d) DONE 07:10: claude/de-sendsheet bbfdb02 (contains main 5c417f9 + designs/custompool/emplog). New file charm-nest-custom-piece.js, test ce-sendsheet 21/21. BATCH 5.
- de-ops (a3ce05d237db781e5) DONE 07:04: claude/de-ops d4009d1, ce-ops 47 node + 54 browser. BATCH 5.
- de-masters (acbce20e63df93ae2) DONE 07:00: claude/de-masters f55479f, ce-masters 17/17. BATCH 5 (with library, tab, sendsheet, ops, extras as they land).
- local-store (a42236c45a7daeff2) DONE 06:59: all gates pass (G1 68 ms, G3 282 ms).
- de-pen (a925a9f2929572a8f) DONE 06:57: claude/de-pen 64e0f11, ce-paths 25/25. Notes: chainSubpaths/simplifySubpaths available for importers (vimport doesn't call them yet).
- bar-waiting (aa6569d65415c6c37) DONE 06:56: claude/bar-front-waiting c813f6f. Pending cosmetic: panel setup still says 'Front only' for decided-back bar lines. engrave-ui-faces fails on main (pre-existing, unassigned).
- de-rulers (a909d6b516f6d76c5) DONE 06:55: claude/de-rulers 8ddc4c2 (contains main ea13162), ce-rulers 17/17, editor tests pass. Editor token -ru1. BATCH 4.
- de-image (ac4cbd9c035695d58) DONE 06:52: claude/de-image e8a236d (contains main ea13162). New file charm-nest-ce-image.js (approved), test ce-image 27/27 + 10 editor/art-edit tests. NEXT BATCH (4) after batch 3 ships; rerun its browser tests on the merged tree (it only ran node tests after its last merge).
- de-designs (af114bb2ccb5f706c) DONE 06:47: claude/de-designs 2f53907; designs-server 95/95, designs-page 13/13.
- fix-approve-held (af959c47001f89f4c) handed back 06:34: could NOT reproduce offline; fix = approve() never fails silently (approveThrew shows the error on the card + activity log; stale approvalIntent says why). Branch claude/fix-approve-held a4119ea (contains 4da1e2c). Its reruns write $SP/heldfix/runsA|runsB/summary.txt (ALLDONE at end). Ship when green; then ask Paul to press Approve on 4212020155 again and read the card's line. It did NOT refuse Approve on held orders (ENGPRE rule, 13 Oct) — needs Paul's OK to change.
- fix-seals (a2200fb3aa0ebf322) handed back 06:45: claude/fix-release-button 1d5840e (seals: only ORDER HELD + green ORDER RELEASED; Release Hold in the order window for every held piece). Merged with main in $SP/wt-guardfix (ea13162), test run b9vo80e29 → $SP/int/seals/*.log; all pass → push HEAD:main + verify live. Follow-up sent to the same agent: branch claude/fix-seals-2 = sheet window release seal green ORDER RELEASED + 5 tests failing on main (seals-travel, timeline-events-sheets, review-hold, engraving-stamps, releases).
- New agents 06:37–06:41: emplog follow-up a41a92d9950823b21 (person page design counts, on claude/de-emplog), order-hold-engine fix a92b66df2d483239a (branch claude/fix-order-hold-engine, wt-ohe; the test fails on main: GF Sheet 2 gets 3 waiting orders, expects 2).
- All 17 other agents resumed 06:32 with SendMessage (same ids as the table).
- de-text (ae828a5d729f6efa1) DONE: branch claude/de-text (af6f42b + merge 32078d4), new file charm-nest-ce-text.js (approved), test ce-text.cjs 10/10. Merge with resolve-union.py, then test the batch.
- de-custompool (a4d3307fda8a88af0) DONE: claude/de-custompool 36f1f7d (contains de-designs a7009a1 + main). New file charm-nest-custom-pool.js (approved), server op customPiecePut, test custom-piece-pool.cjs 6/6. Ship WITH designs (+ sendsheet must settle the order's custom line). It reports sandbox-wipe-guard fails on origin/main too: op roseReopen is unclassified in the sandbox families file → small fix for the lead (classify roseReopen).
- de-emplog (aae0c82c2db2e93d9) DONE: claude/de-emplog d1ebeb9 (merged main). New files charm-nest-design-log.js, charm-nest-efficiency-custom.js, netlify/functions/_designLog.js (approved), test design-log.cjs + 12 efficiency browser tests pass. Ship with sendsheet+custompool (sendsheet must NOT record its own designSent; use CNDesignLog.causeOf for D7). Left: person page doesn't show design counts yet.
- de-extras (a5f64f2a38f32df82) finishing its last test runs (7 commits on claude/de-extras).

## Batch 2 in test (03:48 UTC): claude/de-integration 4da1e2c = main 722e4a7 + vimport + text. Test run b1b22dnd5 → logs $SP/int/b2/*.log (node: charm-editor-model art-edit-model public-assets; browser: ce-import ce-text ce-feedback emoji-picker charm-editor-browser charm-editor-engrave charm-editor-back art-edit-page engrave-editor-words). All pass → push HEAD:main (if it contains origin/main), verify live (new files charm-nest-ce-import.js? + charm-nest-ce-text.js), board.

## Shipped 03:45 UTC
- refit 722e4a7 (contains main d58eea9): worktree $SP/wt-refitship, tests running (bs0ceqa3m: engrave-worker, charm-edit-refit, charm-editor-engrave, engrave-editor-words, engrave-front-art, engrave-approve-never-silent, charm-editor-back, engrave-preview). If all pass: push HEAD:main (if it still contains origin/main), verify live, board.

## Next for the lead, in order
1. Re-arm the monitor; publish the board each tick.
2. Merge + test + ship each workstream as it hands back (batches); Phase 1 store and seals are next; fix-approve-held is urgent (Paul's order).
3. After Phase 1 lands: start Phase 2 (sync worker: one changes feed per computer; report section 6). Phase 3 (outbox, optimistic writes)
   after it. Phase 5 push option (a) needs Paul to add a Firestore security rule in the console + a vendored Firestore SDK file: ask him.
4. Keep Paul informed in short messages; he reads the board.

## Scheduled resume messages: ALL DELETED at 06:33 (work resumed). Old list for reference: The usage limit resets at 2:31 am Toronto = 06:31 UTC, 28 Oct 2026.
06:33 trig_01S4EwRYu7UdCm7koKNqYLvA (main) · 06:50 trig_01CQebUb74FYfm9bW1vZmmSK (backup) · 07:30 trig_013sPayqPjKBmoHEd7DobThs ·
08:30 trig_01X1G9rAxWwbSQXPZhdr3fD5 · 09:30 trig_018Vyc472WYCC6N2WXYxmy9y (backups). Once you are working again, delete the remaining ones (delete_trigger).
