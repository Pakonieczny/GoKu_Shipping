# LEAD RESUME NOTES — read this first after the usage-limit reset (written 28 Oct 2026 ~03:15 UTC)

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
  updated:{at,by:"Claude"}}. Last version written: 83 (next write if_version 83; if refused, the error names the current version).
- Plan doc for Paul (local-first): https://claude.ai/artifact/GykrjmCTLTbcohMCKBrkyZ (decisions recorded).

## Scripts ($SP)
- int/slot.sh: shared browser-test queue, 3 slots, FIFO, SLOT_PRIO=1 skips the line, every test capped at 600 s (SLOT_MAX). ALL browser tests go through it.
- agent-mon.py (one line per agent; AGENTS list + board-live-extra.json), board-live.py (fills board rows from agent transcripts), lead-now.sh (lead row text).
- Monitor command (re-arm every 30 min with Monitor, timeout 1800000):
  SP=...; while true; do sleep 180; $SP/lead-now.sh >/dev/null 2>&1; B=$(python3 -I $SP/board-live.py 2>&1 | tail -1); echo "=== $(date -u +%H:%M) check (load $(cut -d' ' -f1 /proc/loadavg)) · $B · board json refreshed: publish it"; python3 -I $SP/agent-mon.py 2>&1 | grep -v "DONE" | sed -E 's/ · says:.*//; s/last: (Bash|Read|Edit|Write|Grep|WebSearch|WebFetch): ?/now: /' | cut -c1-150; done
- resolve-union.py <files>: merge conflicts where both sides added lines (script tags, build list) or re-tokened ?v=. resolve-tokens.py: ?v= only.
- int/live-check.cjs: 7 pages load clean. Live verify = sha256 of each changed served file vs `git show origin/main:<f>`, then live-check.
- If agents hang holding slots (load ~0, waits > 5 min): find holders: for f in slot1 slot2 slot3: ino=$(stat -c %i $SP/locks/$f); grep ":$ino " /proc/locks → pid → ps --ppid. Kill the idle test, tell its agent.

## Live on main (all verified live): d58eea9
- 89f2819 Manual Control instant · b2608ff navigation rounds 2+3, Charm Editor Save fix (Saving at once, sheet files in background),
  bar necklaces FRONT by default (BARFRONT) · 077f21c Approve never silent / never stuck · d58eea9 Design & Engrave batch 1: editor
  extension points + live readouts + Ctrl/Alt centre resize + smart guides (feedback), Blend/Warp/Distort (warp), image trace (trace).

## Integration branch for Design & Engrave: claude/de-integration (worktree $SP/wt-de-int), head 5d240fc = d58eea9 + vimport.
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

## Next for the lead, in order
1. Re-arm the monitor; publish the board each tick.
2. Merge + test + ship each workstream as it hands back (batches); Phase 1 store and seals are next; fix-approve-held is urgent (Paul's order).
3. After Phase 1 lands: start Phase 2 (sync worker: one changes feed per computer; report section 6). Phase 3 (outbox, optimistic writes)
   after it. Phase 5 push option (a) needs Paul to add a Firestore security rule in the console + a vendored Firestore SDK file: ask him.
4. Keep Paul informed in short messages; he reads the board.

## Scheduled resume messages (send_later, one-shot). The usage limit resets at 2:31 am Toronto = 06:31 UTC, 28 Oct 2026.
06:33 trig_01S4EwRYu7UdCm7koKNqYLvA (main) · 06:50 trig_01CQebUb74FYfm9bW1vZmmSK (backup) · 07:30 trig_013sPayqPjKBmoHEd7DobThs ·
08:30 trig_01X1G9rAxWwbSQXPZhdr3fD5 · 09:30 trig_018Vyc472WYCC6N2WXYxmy9y (backups). Once you are working again, delete the remaining ones (delete_trigger).
