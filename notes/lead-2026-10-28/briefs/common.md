# Design & Engrave — shared brief for every agent (read fully before starting)

You are one of ~20 agents working in parallel on Charm Nest (the browser app at charm-nest-1.html + charm-nest-*.js, Netlify functions in
netlify/functions/, live at https://goldenspike.app). The owner is Paul. The lead (me) merges, tests and ships; you build one workstream.

## Paul's words (28 Oct 2026, verbatim)
See `paul-37.md` in this folder: his two messages, the 37 numbered items he approved, and the lead's decisions on open points. Paul's goals:
the app must feel like a native application (Photoshop/Illustrator-level responsiveness, no waiting), use the bare minimum of
Firebase/Firestore calls, follow the 70/30 rule (the most-used 70% of Illustrator/Photoshop features, not everything), and replace
Adobe ($500/month) for charm design. Everything is tracked by employee.

## Your worktree and branch
- Work ONLY in the worktree named in your prompt (already created from origin/main, on your own branch). Commit there.
- Push your branch (`git push -u origin <branch>`; "repository moved" notices are harmless). NEVER push to main, never force-push.
- Before handing back: `git fetch origin && git merge origin/main`, resolve, re-run your tests. charm-nest-1.html `?v=` token conflicts:
  keep both sides' suffixes (`python3 /tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/scratchpad/resolve-tokens.py charm-nest-1.html`).
- Commit messages end with exactly these two lines, and contain no model name otherwise:
  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01CHrvrKNNFNkEUFnrGNzR9T

## Shared files: keep your edits small and local
Many agents touch the same big files (charm-nest-charm-editor.js — the Charm Editor, charm-nest-bridge.js, charm-nest-1.html,
netlify/functions/charmNestLibrary.js). To keep merges clean:
- Put your feature's bulk in YOUR OWN new file (name given in your prompt). In the shared files add only small hook points.
- When adding to a shared list (the editor's tool rail, menus, key map, prop bar, the build list, the families registry), APPEND your entry
  as its own line(s); never reorder, reformat or re-indent existing lines. No drive-by refactors or renames in shared files.
- Keep these lines byte-identical (tests slice files on them): `  function textBox(glyphs`, `  function renderFront(`,
  `  const items = () => B.engrave.items;`, `  async function prepareApproval(job, by, at) {`, `  function nudge(job, dxMm, dyMm)`.
  When a test slices a file and you call a function defined outside the slice, guard it with `typeof fn === "function"`.
- Every served file you change: append a short suffix to its `?v=` token in charm-nest-1.html (e.g. `-ce1`). A NEW served file: add a
  `<script>` tag with a `?v=` token AND add it to the list in scripts/build-public.cjs (Paul approved all the new files of this update on 28 Oct,
  the outline emoji font included). Never edit package.json, lockfiles or netlify.toml.
- A new Firestore collection or Storage path written in the sandbox must be registered in charm-nest-sandbox-families.js (read its header;
  tests/charm-nest/sandbox-wipe-guard.cjs must pass).
- Match the surrounding code: its comment density, naming and idiom. Comments in this repo cite Paul's words and the date the way
  existing ones do ("(Paul, 28 Oct 2026: ...)").
- Never `rm -rf` a variable path.

## Design language
The Charm Editor (charm-nest-charm-editor.js) copies the website's Brites Custom Studio look and behaviour. Studio sources (read-only
reference, do not copy secrets): JS `/tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/scratchpad/integ/shopify/assets/brites-custom-studio.js`
(28k lines; the editor's header cites it as "J <line>"), section `.../scratchpad/editor/studio-archive/sections/brites-custom-studio.liquid` ("S"),
CSS `.../scratchpad/editor/studio-archive/assets/brites-custom-studio.css` ("C"). Reuse its tokens, icons and patterns; improve where you can.
Read the editor file's header comment first: it documents the data model (FaceDoc, Item, transforms) and the open()/result contract.

## Speed and calls
- Every press, drag and key paints in the same frame (<16 ms of work); heavy work (tracing, boolean ops on big paths) goes to a Worker or
  is chunked, with a progress cue. Never block the page for more than 50 ms.
- Read from memory/local caches first; ask the cloud only what changed. No polling loops of your own. Batch writes.
- A local-first record store is being built (charm-nest-store.js, Phase 1, branch claude/local-store); do not build another one.

## Employee tracking (Paul: "all of this must be tracked by employee")
Every write you add carries `by` (the signed-in person: `employeeName()`, or `needEmployee(...)` to ask, as existing code does) and the
server stamps `at`. Call the shared log `window.CNDesignLog && CNDesignLog.event(kind, data)` at each user action point you build
(opened, saved, imported, traced, attached, sent to sheet, ...); agent "employee-tracking" builds it — guard with `window.CNDesignLog &&`.

## Tests: proper, not excessive
- No live services or credentials, ever. Use the repo's fixtures/fake site (tests/charm-nest/_engraveRealWorld.cjs, other _*.cjs helpers).
- EVERY browser/Playwright test runs through the shared slot queue (2 at a time machine-wide):
  `/tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/scratchpad/int/slot.sh node tests/charm-nest/<test>.cjs`
  (it sets PW_DIR and CHROMIUM; first come, first served). Put a time limit INSIDE it (`slot.sh timeout 600 node ...`), never around it:
  waiting for a slot can take minutes. Node-only tests may run directly.
- Write ONE focused test file for your feature (checks that prove it works, with numbers printed), plus run the existing tests nearest to
  what you touched (for editor work: charm-editor-model, charm-editor-browser, charm-editor-engrave, charm-editor-back, art-edit-model,
  art-edit-page, engrave-editor-words). Don't write sprawling suites; don't rerun suites repeatedly. Timing budgets in your test should
  hold on a loaded machine (load 5+): measure work (ms of main-thread time), not wall-clock waits.
- If an existing test fails, check whether it fails on origin/main too before chasing it; report it, don't spend long on it.

## Report back (SubagentHandback) — short
Commits (hash, one line each), what Paul will see, the new files, tests run with pass/fail and the key numbers, decisions you made, and
anything that needs the lead or Paul. If you are blocked for more than ~15 minutes on something outside your scope, say so and hand back.
