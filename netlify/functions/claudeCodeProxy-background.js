/* netlify/functions/claudeCodeProxy-background.js */
/* ═══════════════════════════════════════════════════════════════════
   TRANCHED AI PIPELINE — v5.2 (+ Spec Validation Patch Loop)
   ─────────────────────────────────────────────────────────────────
   Each invocation handles ONE unit of work then chains to itself
   for the next, staying well under Netlify's 15-min limit.

   Invocation 0    ▸  "plan"    — Spec Validation Gate (3 Sonnet calls)
                       runs first, then Opus 4.6 creates a dependency-
                       ordered, 6.3-centered tranche plan + hardening batch.
                       Gate FAIL writes ai_error.json with structured issues
                       and halts before Opus fires.
   Invocation 1–N  ▸  "tranche" — Sonnet 4.6 executes one tranche,
                       saves accumulated files, chains to next tranche.
   Correction loop ▸  "fix"     — Used only for objective retryable
                       validation failures.
   Final           ▸  Writes ai_response.json for frontend pickup.

   SPEC VALIDATION GATE (runs in "plan" mode before Opus):
   ─────────────────────────────────────────────────────────
   Call 1 — Extract  : Sonnet reads Master Prompt, produces 6-8 custom
                       simulation scenarios specific to this game's mechanics.
   Call 2 — Simulate : Sonnet traces each scenario through the spec rules
                       literally, documents findings in plain text.
   Call 3 — Review   : Sonnet classifies findings as PASS/FAIL JSON.
   On PASS  → continues to Opus planning as normal.
   On FAIL  → writes ai_error.json { validationFailed:true, issues:[...] }
              and returns without invoking Opus. Frontend renders issues
              in the tranche panel so the user can fix the Master Prompt.
   On error → validation is skipped with a warning; Opus proceeds.

   Recovery policy:
   - 0 retries for soft/advisory findings.
   - 1 retry max for narrow objective hard failures when the repair is surgical.
   - 2 retries only for parser/envelope failures or truly critical scaffold/runtime issues.
   - Everything else is deferred into a single end-stage hardening batch.

   All intermediate state lives in Firebase so each invocation is
   stateless and can reconstruct context from the pipeline file.
   ═══════════════════════════════════════════════════════════════════ */

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");

const RETRY_POLICY = Object.freeze({
  soft: 0,
  hard_objective: 1,
  critical_runtime: 2,
  parser_envelope: 2
});

const HARDENING_BATCH_NAME = "End-Stage Hardening Batch";
const HARDENING_BATCH_KIND = "hardening_batch";


/* ─── SCAFFOLD + SDK INSTRUCTION BUNDLE: fetched from Firebase ───
   All project-level instruction files live under:
     ${projectPath}/ai_system_instructions/

   We classify them into:
   - scaffold: immutable game foundation / structural rules
   - sdk: engine reference / API facts / certainty fallback
   - other: additional instruction docs (treated as sdk-side supplemental context)
*/

function classifyInstructionFile(fileName = "", content = "") {
  const lowerName = String(fileName || "").toLowerCase();
  const lowerContent = String(content || "").toLowerCase();

  // SDK check runs FIRST — filename is unambiguous and must not be overridden
  // by the content-based scaffold check (SDK docs often mention "scaffold" and "immutable")
  if (
    lowerName.includes("engine_reference") ||
    lowerName.includes("engine-reference") ||
    lowerName.includes("engine reference") ||
    lowerName.includes("sdk") ||
    lowerContent.includes("cherry3d engine reference") ||
    lowerContent.includes("platform invariants")
  ) {
    return "sdk";
  }

  if (
    lowerName.includes("scaffold") ||
    (lowerContent.includes("scaffold") && lowerContent.includes("immutable"))
  ) {
    return "scaffold";
  }

  return "other";
}

async function fetchInstructionBundle(bucket, projectPath) {
  try {
    const folder = `${projectPath}/ai_system_instructions`;
    const [files] = await bucket.getFiles({ prefix: folder + "/" });
    if (!files || files.length === 0) {
      console.warn(`fetchInstructionBundle: no files found at ${folder}/`);
      return {
        scaffoldText: "",
        sdkText: "",
        combinedText: "",
        scaffoldCount: 0,
        sdkCount: 0,
        otherCount: 0
      };
    }

    files.sort((a, b) => a.name.localeCompare(b.name));
    const parts = await Promise.all(
      files.map(async (file) => {
        const [fileContent] = await file.download();
        const content = fileContent.toString("utf8");
        return {
          fileName: file.name.split("/").pop(),
          content,
          kind: classifyInstructionFile(file.name.split("/").pop(), content)
        };
      })
    );

    const scaffoldDocs = parts.filter(p => p.kind === "scaffold");
    const sdkDocs = parts.filter(p => p.kind === "sdk");
    const otherDocs = parts.filter(p => p.kind === "other");

    const formatDocs = (docs) => docs.map(doc =>
      `--- ${doc.fileName} ---\n${doc.content}`
    ).join("\n\n");

    const scaffoldText = formatDocs(scaffoldDocs);
    const sdkText = formatDocs([...sdkDocs, ...otherDocs]);

    const sections = [];
    if (scaffoldText) {
      sections.push(`=== IMMUTABLE CHERRY3D SCAFFOLD ===\n${scaffoldText}`);
    }
    if (sdkText) {
      sections.push(`=== CHERRY3D SDK / ENGINE REFERENCE ===\n${sdkText}`);
    }

    const combinedText = sections.join("\n\n");
    console.log(
      `fetchInstructionBundle: loaded ${files.length} file(s) ` +
      `(scaffold=${scaffoldDocs.length}, sdk=${sdkDocs.length}, other=${otherDocs.length})`
    );

    return {
      scaffoldText,
      sdkText,
      combinedText,
      scaffoldCount: scaffoldDocs.length,
      sdkCount: sdkDocs.length,
      otherCount: otherDocs.length
    };
  } catch (err) {
    console.error("fetchInstructionBundle failed:", err.message);
    return {
      scaffoldText: "",
      sdkText: "",
      combinedText: "",
      scaffoldCount: 0,
      sdkCount: 0,
      otherCount: 0
    };
  }
}

function assertInstructionBundle(bundle, phaseLabel = "Pipeline") {
  if (!bundle?.scaffoldText) {
    throw new Error(`${phaseLabel}: immutable Scaffold missing from ai_system_instructions/.`);
  }
  if (!bundle?.sdkText) {
    throw new Error(`${phaseLabel}: SDK / Engine Reference missing from ai_system_instructions/.`);
  }
}

/* ─── ANTI-PATTERN VALIDATORS ──────────────────────────────────
   Engine-agnostic. Tests apply to any Cherry3D game — no game-
   specific function names, UI object names, or floor conventions.
   FATAL patterns trigger automatic re-execution of the tranche
   with an explicit correction prompt. ──────────────────────────── */

const ANTI_PATTERNS = [
  {
    // §3 — The correct path is <actorBody>.RigidBody.controls.setFloat/setInt().
    // Calling .controls.setFloat/setInt() directly on an actor body object (not on its
    // .RigidBody sub-object) is a silent no-op — no error, actor does not move.
    //
    // ALIASING: Code often does:
    //   const rb = someActor.RigidBody;   <- rb IS the RigidBody
    //   rb.controls.setFloat(...);        <- VALID — rb is already the RigidBody
    // We detect this by scanning for variable assignments of the form
    //   `const/let/var <n> = <anything>.RigidBody`
    // and exempting any .controls.set* call made on those aliased variables.
    name: "Silent no-op controls path",
    test: (code) => {
      const lines = code.split('\n');
      // Collect variable names that are known to hold a RigidBody object directly.
      const rigidBodyVars = new Set();
      lines.forEach(line => {
        const m = line.match(/(?:const|let|var)\s+(\w+)\s*=\s*[\w.[\]()]+\.RigidBody\b/);
        if (m) rigidBodyVars.add(m[1]);
      });
      return lines.some(line => {
        const t = line.trim();
        if (t.startsWith('//') || t.startsWith('*')) return false;
        if (!/\.controls\.set(?:Float|Int)\s*\(/.test(line)) return false;
        // Already correct: literal .RigidBody.controls path
        if (/\.RigidBody\.controls\.set(?:Float|Int)/.test(line)) return false;
        // Extract the variable immediately before .controls
        const callerMatch = line.match(/([\w.[\]]+)\.controls\.set(?:Float|Int)\s*\(/);
        if (!callerMatch) return false;
        const callerVar = callerMatch[1].split('.').pop().replace(/\[.*\]/, '');
        // If this variable is a known RigidBody alias, the call is valid — skip
        if (rigidBodyVars.has(callerVar)) return false;
        return true;
      });
    },
    message: `FATAL: Found .controls.setFloat/setInt without .RigidBody. prefix — silent no-op. The actor will not move and no error is thrown.
REQUIRED FIX — change ONLY the property path, nothing else:
  WRONG:   body.controls.setFloat(0, vx)          <- body is an actor wrapper, not a RigidBody
  CORRECT: body.RigidBody.controls.setFloat(0, vx)
  ALSO OK: const rb = body.RigidBody; rb.controls.setFloat(0, vx)  <- alias assigned via .RigidBody is fine
SCOPE CONSTRAINT: Do NOT restructure updateInput(), do NOT move logic to the main thread, do NOT change what values are written. Only insert .RigidBody between the body reference and .controls. Every setFloat/setInt call that lacks .RigidBody must be fixed — search the entire file.`,
    severity: "FATAL"
  },
  {
    // §4 — controls.getFloat/getInt always return 0. Use getMotionState().
    // Also catches getMotionState() called on the wrong object (missing .RigidBody).
    //
    // ALIASING: Code often does:
    //   const rb = actor.RigidBody;    <- rb IS the RigidBody
    //   rb.getMotionState();           <- VALID
    //   rb.controls.getInt(3);        <- INVALID — getInt always returns 0
    // We track variable aliases assigned via .RigidBody to exempt getMotionState() calls,
    // and similarly track them to still flag controls.getInt/getFloat on those vars.
    name: "Unreliable physics readback",
    test: (code) => {
      const lines = code.split('\n');
      // Collect variable names known to hold a RigidBody object directly.
      const rigidBodyVars = new Set();
      lines.forEach(line => {
        const m = line.match(/(?:const|let|var)\s+(\w+)\s*=\s*[\w.[\]()]+\.RigidBody\b/);
        if (m) rigidBodyVars.add(m[1]);
      });
      // Primary: controls.getFloat/getInt — flag ONLY when the caller is NOT a known
      // RigidBody alias. On a raw RigidBody object, controls.getInt/getFloat reads
      // from the shared-memory output slots written by the physics thread — that IS
      // reliable. The no-op only occurs when called on an actor wrapper (not RigidBody).
      const hasGetFloatInt = lines.some(line => {
        const t = line.trim();
        if (t.startsWith('//') || t.startsWith('*')) return false;
        if (!/\.controls\.get(?:Float|Int)\s*\(/.test(line)) return false;
        // Already correct if the literal .RigidBody.controls path is present
        if (/\.RigidBody\.controls\.get(?:Float|Int)/.test(line)) return false;
        // Extract the caller variable
        const callerMatch = line.match(/([\w.[\]]+)\.controls\.get(?:Float|Int)\s*\(/);
        if (!callerMatch) return true; // can't tell — flag it
        const callerVar = callerMatch[1].split('.').pop().replace(/\[.*\]/, '');
        // If this variable is a known RigidBody alias, the call is valid — skip
        if (rigidBodyVars.has(callerVar)) return false;
        return true;
      });
      if (hasGetFloatInt) return true;
      // Secondary: getMotionState() on a non-RigidBody object — returns undefined silently.
      // Special case: bare identifier `RigidBody` (capital R+B) is the parameter name
      // passed into the physics callback function — it IS the RigidBody object.
      const hasBareMotionState = lines.some(line => {
        const t = line.trim();
        if (t.startsWith('//') || t.startsWith('*')) return false;
        if (!/\.getMotionState\s*\(/.test(line)) return false;
        // Literal .RigidBody.getMotionState chain is always correct
        if (/\.RigidBody\.getMotionState/.test(line)) return false;
        // Extract the caller variable immediately before .getMotionState
        const callerMatch = line.match(/([\w.[\]]+)\.getMotionState\s*\(/);
        if (!callerMatch) return false;
        const callerVar = callerMatch[1].split('.').pop().replace(/\[.*\]/, '');
        // Bare identifier `RigidBody` is the physics callback parameter — valid
        if (callerVar === 'RigidBody') return false;
        // Any other known RigidBody alias is also valid
        if (rigidBodyVars.has(callerVar)) return false;
        return true;
      });
      return hasBareMotionState;
    },
    message: `FATAL: Found controls.getFloat()/getInt() for physics readback — always returns 0. No error is thrown but all position/state reads will be wrong.
REQUIRED FIX — replace every controls.getFloat/getInt call with getMotionState():
  WRONG:   const x = body.controls.getFloat(10)
  CORRECT: const ms = body.RigidBody.getMotionState(); const x = ms.position[0];
CRITICAL PATH DETAIL: getMotionState() is on .RigidBody — NOT directly on the body object.
  WRONG:   body.getMotionState()           ← undefined, silent failure
  CORRECT: body.RigidBody.getMotionState() ← returns { position, linear, angular }
Position components: ms.position[0]=X, ms.position[1]=Y, ms.position[2]=Z.`,
    severity: "FATAL"
  },
  {
    // §8 — file 2 and file 23 have isolated window contexts.
    // Any window.XYZ.method() call from file 2 silently fails for any game.
    // Allowlist: window.addEventListener, window.removeEventListener, window.requestAnimationFrame,
    //            window.cancelAnimationFrame, window.innerWidth, window.innerHeight,
    //            window.setTimeout, window.clearTimeout, window.setInterval, window.clearInterval,
    //            window.location, window.performance — these are native browser APIs, not cross-context globals.
    name: "Cross-context window global call",
    test: (code) => {
      // Native browser APIs that are legitimately accessed via window.*
      const ALLOWED_WINDOW_PROPS = new Set([
        'addEventListener', 'removeEventListener', 'requestAnimationFrame',
        'cancelAnimationFrame', 'setTimeout', 'clearTimeout',
        'setInterval', 'clearInterval', 'location', 'performance',
        'innerWidth', 'innerHeight', 'devicePixelRatio', 'open',
        'close', 'focus', 'blur', 'scrollTo', 'scrollBy', 'print',
        'alert', 'confirm', 'prompt', 'dispatchEvent', 'postMessage',
        'getComputedStyle', 'matchMedia', 'history', 'navigator',
        'screen', 'document', 'console', 'crypto', 'fetch',
        'AudioContext', 'webkitAudioContext'
      ]);
      return code.split('\n').some(line => {
        const t = line.trim();
        if (t.startsWith('//') || t.startsWith('*')) return false;
        // Match window.<something>.<method>() — two-level chained call
        const m = line.match(/window\.([A-Za-z_$][\w$]*)\.([A-Za-z_$][\w$]*)\s*\(/);
        if (!m) return false;
        // Allow if the first property is a known native browser API
        if (ALLOWED_WINDOW_PROPS.has(m[1])) return false;
        return true;
      });
    },
    message: `FATAL: Found window.<object>.<method>() call from file 2. Files 2 and 23 run in ISOLATED window contexts — window globals set in file 23 are undefined in file 2. No error is thrown but the call silently does nothing.
REQUIRED FIX — replace every window.<UIObject>.<method>() call with direct DOM element queries:
  WRONG:   window.GameUI.updateScore(pts)
  CORRECT:
    const htmlRoot = Module.ProjectManager.getObject('25').DOMElement;
    const scoreEl  = htmlRoot.querySelector('#scoreDisplay');
    if (scoreEl) scoreEl.textContent = pts;
PATTERN RULES:
  1. Use Module.ProjectManager.getObject(id).DOMElement to get the overlay root (try '25', '23', '24' if unsure — scan for the right id).
  2. Use htmlRoot.querySelector('#elementId') to reach individual elements.
  3. Wrap every DOM update in a null-guard (if (el)) so missing elements don't throw.
  4. Do NOT chain methods directly off DOMElement — it is a raw DOM node, not a UI controller.
  5. Wire button listeners via htmlRoot.querySelector('#btn').addEventListener('click', handler) from file 2's onInit.`,
    severity: "FATAL"
  },
  {
    // §5 — rbPosition is a LOCAL offset from the parent mesh.
    // Any non-zero value doubles the intended world position.
    // Test joins lines before matching to handle multi-line createRigidBody calls,
    // which are the most common formatting style and were silently missed before.
    name: "Non-zero rbPosition",
    test: (code) => {
      // Collapse all whitespace/newlines so multi-line calls become one scannable unit
      const collapsed = code.replace(/\r?\n/g, ' ').replace(/\s+/g, ' ');
      // Use paren-balanced extraction so nested () inside args (e.g. rbCounter++) don't truncate.
      const searchRe = /(?:\w+\.)?createRigidBody\s*\(/g;
      let m;
      while ((m = searchRe.exec(collapsed)) !== null) {
        let depth = 1, i = m.index + m[0].length;
        const argStart = i;
        while (i < collapsed.length && depth > 0) {
          if (collapsed[i] === '(') depth++;
          else if (collapsed[i] === ')') depth--;
          i++;
        }
        const args = collapsed.slice(argStart, i - 1);
        // STATIC bodies are their own positioned parents — rbPosition carries the
        // intended world offset and [0,0,0] would be wrong. Only enforce on
        // KINEMATIC and DYNAMIC bodies, which are children of a visual mesh object.
        if (/['"`]STATIC['"`]/i.test(args)) continue;
        // Extract the last [...] array in the argument list — that is rbPosition
        const arrays = args.match(/\[[^\]]*\]/g);
        if (!arrays || arrays.length < 2) continue;
        const lastArray = arrays[arrays.length - 1];
        if (lastArray && !/\[\s*0\s*,\s*0\s*,\s*0\s*\]/.test(lastArray)) return true;
      }
      return false;
    },
    message: `FATAL: Found createRigidBody with non-[0,0,0] rbPosition on a KINEMATIC or DYNAMIC body — causes POSITION DOUBLING. The actor spawns at 2× the intended location with no error.
REQUIRED FIX — set the last array argument (rbPosition) to exactly [0, 0, 0]:
  WRONG:   createRigidBody(key, mass, friction, shape, 'DYNAMIC', layer, filter, ghost, scale, [spawnX, 0.5, spawnZ])
  CORRECT: createRigidBody(key, mass, friction, shape, 'DYNAMIC', layer, filter, ghost, scale, [0, 0, 0])
WHY: rbPosition is a LOCAL offset from the parent visual mesh, not a world position. The engine adds parent world position + rbPosition. Passing world coordinates here doubles the translation. The parent mesh already carries the world position — rbPosition must always be [0, 0, 0].
NOTE: STATIC bodies (walls, floors) are exempt — they ARE the positioned parent, so their rbPosition carries the intended world offset.
Fix every DYNAMIC and KINEMATIC createRigidBody call in the file — not just the first one found.`,
    severity: "FATAL"
  },
  {
    // §6 — any blocking surface without a STATIC rigidbody is invisible to physics.
    // Detects files that build floor/wall geometry but have no STATIC rigidbody.
    // Uses surface-keyword detection instead of a raw count threshold to avoid
    // false positives on partial tranche files and false negatives on small scenes.
    name: "Surface mesh without STATIC rigidbody",
    test: (code) => {
      // Find every createRigidBody call regardless of object prefix (e.g. gameState.createRigidBody)
      // Use paren-balanced extraction so nested () inside args (e.g. rbCounter++) don't truncate the match.
      const rbTypes = [];
      const collapsed = code.replace(/\r?\n/g, ' ').replace(/\s+/g, ' ');
      const searchRe = /(?:\w+\.)?createRigidBody\s*\(/g;
      let m;
      while ((m = searchRe.exec(collapsed)) !== null) {
        // Walk balanced parens from the opening ( to extract the full argument list
        let depth = 1, i = m.index + m[0].length;
        const argStart = i;
        while (i < collapsed.length && depth > 0) {
          if (collapsed[i] === '(') depth++;
          else if (collapsed[i] === ')') depth--;
          i++;
        }
        const args = collapsed.slice(argStart, i - 1); // content between the outer ()
        // Extract motion type — 5th positional argument (after key, mass, friction, shapeType)
        // Quoted with single, double, or backtick quotes
        const motionMatch = args.match(/['"`](STATIC|KINEMATIC|DYNAMIC)['"`]/i);
        if (motionMatch) rbTypes.push(motionMatch[1].toUpperCase());
      }
      // Only proceed if the file creates any rigidbodies at all
      if (rbTypes.length === 0) return false;
      // If any STATIC exists, we're satisfied
      if (rbTypes.some(t => t === 'STATIC')) return false;
      // Check whether the file also builds floor/wall geometry — surface keywords indicate
      // a scene that physically needs STATIC bodies. Skip files that only create actors.
      const hasSurfaceKeywords = /floor|wall|ground|maze|tile|barrier|boundary|platform|terrain/i.test(code);
      // Require both: no STATIC rigidbody AND evidence of surface geometry construction.
      // This prevents false positives on actor-only tranche files.
      return hasSurfaceKeywords;
    },
    message: `FATAL: File builds floor/wall/maze geometry but creates NO STATIC rigidbodies. DYNAMIC actors will fall through every surface with no error thrown.
REQUIRED FIX — every blocking surface (floor, walls, maze tiles) needs its own STATIC rigidbody:
  CORRECT pattern for a floor tile:
    const floorRb = createRigidBody(
      'rb_floor_' + counter++,
      0,               // mass — STATIC must be 0
      0.4,             // friction
      'bounding-box',  // shapeType
      'STATIC',        // motionType ← this is what prevents fall-through
      'DYNAMIC',       // collisionLayer
      0, false,        // filter, ghost
      [width, 0.02, depth],  // physicsScale
      [0, 0, 0]        // rbPosition — always [0,0,0]
    );
    Module.ProjectManager.addObject(floorRb.child, floorRb.data, floorVisual);
PLACEMENT RULES:
  1. Add STATIC rigidbodies to FLOOR and WALL objects — NOT to the player or ghosts (they are DYNAMIC/KINEMATIC).
  2. Each unique surface object needs its own rigidbody — instance parents need one per mesh.
  3. mass MUST be 0 for STATIC bodies or the engine will treat them as DYNAMIC.`,
    severity: "FATAL"
  },
  {
    // §14, §17 — Camera must be set every frame in onRender BEFORE any isReady/isDead
    // guards. Module.controls.position and .target are the ONLY reliable paths.
    // Module.camera and scene.camera are forbidden — they are unreliable.
    // Two distinct sub-cases are detected separately so the repair message is precise:
    //   SUB-CASE A: wrong API path (Module.camera / scene.camera)
    //   SUB-CASE B: correct API but placed after a guard (ordering violation)
    name: "Camera not set correctly every frame in onRender",
    test: (code) => {
      // SUB-CASE A: forbidden camera API paths used as PRIMARY camera control.
      // Exempt: scene.camera / surface.camera / Module.camera used inside a
      // try { if (x && x.camera) { ... } } catch(e){} block — that is the
      // canonical Engine Reference multi-path probe fallback (Pattern F), not
      // a primary reliance on an unreliable path.
      const hasBadCamPath = code.split('\n').some((line, i, arr) => {
        const t = line.trim();
        if (t.startsWith('//') || t.startsWith('*')) return false;
        if (!/Module\.camera\b|(?<!\w)scene\.camera\b|surface\.camera\b/.test(line)) return false;
        // Walk back up to 12 lines to find a wrapping try { if (x && x.camera) guard
        const window = arr.slice(Math.max(0, i - 12), i).join('\n');
        const isProbePattern = /try\s*\{/.test(window) &&
          /if\s*\([\s\S]*&&[\s\S]*\.camera\b/.test(window);
        return !isProbePattern;
      });
      if (hasBadCamPath) return true;

      // Only inspect files that define both onRender and onInit (skip partial tranche files)
      if (!/\bonRender\b/.test(code) || !/\bonInit\b/.test(code)) return false;

      // Extract onRender body using balanced-brace walk
      const fnIdx = code.search(/\bfunction\s+onRender\s*\(|\bonRender\s*[:=]\s*function\s*\(|\bonRender\s*[:=]\s*\(/);
      if (fnIdx === -1) return false;
      const openIdx = code.indexOf('{', fnIdx);
      if (openIdx === -1) return false;
      let depth = 1, pos = openIdx + 1;
      while (pos < code.length && depth > 0) {
        if (code[pos] === '{') depth++;
        else if (code[pos] === '}') depth--;
        pos++;
      }
      const body = code.slice(openIdx + 1, pos - 1);

      // SUB-CASE B-1: Neither Module.controls.position/target NOR a camera helper
      // function call appears in onRender. A named helper (e.g. applyCamera()) is
      // accepted as a valid substitute for direct Module.controls assignment —
      // the validator only cares that camera update happens, not how it's spelled.
      const camIdx = body.search(/Module\.controls\.(position|target)|(?:\w*[Cc]amera\w*)\s*\(/);
      if (camIdx === -1) return true;

      // SUB-CASE B-2: camera IS present but appears AFTER a blocking game-state guard.
      // We distinguish two kinds of guards:
      //   GAME-STATE guard (bad):   if (!gameState.isReady) return true;
      //     — this is a pure logic gate that blocks camera on dead/paused frames.
      //   PHYSICS-DEPENDENCY guard (ok):  if (rigidbody && rigidbody.isReady)
      //     — camera NEEDS a rigidbody position to follow; if the body doesn't exist
      //       yet there is nothing to set. This guard is architecturally required.
      // Rule: only flag if the guard is a bare early-RETURN on gameState (not a
      // conditional camera-position read that requires a live rigidbody).
      const gameStateReturnGuard = /if\s*\([\s\S]{0,120}(?:gameState\.(?:isReady|isDead|gameOver|paused))[\s\S]{0,40}\)\s*return/.test(body);
      return gameStateReturnGuard && body.search(/if\s*\([\s\S]{0,120}(?:gameState\.(?:isReady|isDead|gameOver|paused))[\s\S]{0,40}\)\s*return/) < camIdx;
    },
    // Sub-case is embedded in the message so the repair AI knows exactly which path to take
    message: `FATAL: Camera is not set correctly every frame in onRender. Two possible causes — read both and apply whichever matches your code:

SUB-CASE A — Wrong API path used as PRIMARY camera control (Module.camera or scene.camera):
  WRONG:   Module.camera.position.set(x, y, z)       ← primary reliance on unreliable path
  WRONG:   scene.camera.lookAt(target)                ← primary reliance on unreliable path
  CORRECT: Module.controls.position = [x, y, z];
           Module.controls.target   = [x, y, z];
  Module.controls is the ONLY reliable primary camera path. Delete any Module.camera or
  scene.camera usage that is NOT wrapped in a try { if (x && x.camera) { } } catch(e){}
  defensive probe block. Probe fallbacks inside try-catch guards are fine and expected.

SUB-CASE B — Camera set after an isReady/isDead/gameOver guard (ordering violation):
  WRONG:
    function onRender() {
      if (!gameState.isReady) return true;   // ← guard comes first
      Module.controls.position = [...];      // ← camera blocked by early return
    }
  CORRECT:
    function onRender() {
      Module.controls.position = [...];      // ← camera ALWAYS first, unconditional
      Module.controls.target   = [...];
      if (!gameState.isReady) return true;   // ← guard comes after
    }
  Move the two Module.controls lines to the very top of onRender, before any if-statement. Do not wrap them in any condition. The camera must update every frame even when the game is paused or loading.`,
    severity: "FATAL"
  },
  {
    // §14 — onRender must return true. A falsy return breaks the engine render loop.
    name: "onRender missing return true",
    test: (code) => {
      // Only test files that define onRender
      if (!/\bonRender\b/.test(code)) return false;
      // Locate onRender definition
      const fnIdx = code.search(/\bfunction\s+onRender\s*\(|\bonRender\s*[:=]\s*function\s*\(|\bonRender\s*[:=]\s*\(/);
      if (fnIdx === -1) return false;
      const openIdx = code.indexOf('{', fnIdx);
      if (openIdx === -1) return false;
      // Walk balanced braces to extract full function body
      let depth = 1, pos = openIdx + 1;
      while (pos < code.length && depth > 0) {
        if (code[pos] === '{') depth++;
        else if (code[pos] === '}') depth--;
        pos++;
      }
      const body = code.slice(openIdx, pos);
      return !/return\s+true/.test(body);
    },
    message: `FATAL: onRender is missing "return true". The engine render loop stops if onRender returns a falsy value — the game freezes with no error.
REQUIRED FIX — add "return true" as the FINAL statement of onRender, after ALL game logic:
  CORRECT:
    function onRender() {
      Module.controls.position = [...];   // camera first
      Module.controls.target   = [...];
      if (!gameState.isReady) return true; // early exits also return true
      // ... all game logic ...
      return true;                         // ← LAST LINE of the function
    }
PLACEMENT RULES:
  1. "return true" must be the last statement in the function body.
  2. Do NOT add it at the top — that would skip all render logic every frame.
  3. Every early-exit path inside onRender must also return true (not return / return false).`,
    severity: "FATAL"
  },
  {
    // §13 — KINEMATIC bodies require BOTH a visual AND a collider update every frame.
    // obj.position updates the visual mesh only.
    // RigidBody.set([{prop:"setPosition",...}]) updates the collider only.
    // Omitting either half causes silent visual/collider desync.
    // Test uses tighter regexes to prevent false clears:
    //   - hasObjPosition requires <actor>.obj.position = [...] (not camera or spawn assignments)
    //   - hasSetPosition requires 'setPosition' inside a .set([ call (not bare method names)
    name: "KINEMATIC dual-update incomplete",
    test: (code) => {
      if (!/KINEMATIC/.test(code)) return false;
      // Require setPosition to appear as a quoted key inside a .set([ array call
      const hasSetPosition = /\.set\s*\(\s*\[\s*\{[^}]*['"]setPosition['"]/.test(code);
      // Require actor visual mesh assignment: accept both .obj.position and .object.position
      // (.obj and .object are both valid Cherry3D actor mesh property names)
      const hasObjPosition = /\.\s*(?:obj|object)\s*\.\s*position\s*=\s*\[/.test(code);
      // Flag when one side of the dual-update is present but the other is absent
      return hasSetPosition !== hasObjPosition;
    },
    message: `FATAL: KINEMATIC body has incomplete dual-update. KINEMATIC actors require BOTH updates every frame — omitting either half causes silent desync (no error thrown).
  Visual mesh update:  actor.object.position = [x, y, z];   (or actor.obj.position)
  Collider update:     actor.rb.RigidBody.set([{ prop: 'setPosition', value: [x, y, z] }]);
If only one is present, the other half is silently wrong:
  Missing object.position → visual mesh frozen at spawn, collider moves invisibly
  Missing setPosition     → visual moves but collider stays at spawn, collision detection broken
REQUIRED FIX — ensure BOTH lines appear in every KINEMATIC actor's per-frame update:
  CORRECT (inside onRender or ghost update loop):
    ghost.object.position = [nx, 0.5, nz];
    ghost.rb.RigidBody.set([{ prop: 'setPosition', value: [nx, 0.5, nz] }]);
Apply this fix to every KINEMATIC actor in the file (ghosts, platforms, moving obstacles).`,
    severity: "FATAL"
  },
  {
    // §11 — setLinearVelocity is a SILENT NO-OP on KINEMATIC bodies.
    // KINEMATIC actors must be moved via setPosition (collider) + obj.position (visual).
    // Use DYNAMIC motionType if velocity-driven movement is required.
    // Secondary check: if setLinearVelocity was removed but no movement mechanism was added,
    // the actor is silently frozen — equally broken, also caught here.
    name: "KINEMATIC setLinearVelocity silent no-op",
    test: (code) => {
      if (!/KINEMATIC/.test(code)) return false;

      // Strip block comments (/* ... */) before scanning so manifest JSON strings
      // and JSDoc comments containing "setLinearVelocity" don't trigger false positives.
      const stripped = code.replace(/\/\*[\s\S]*?\*\//g, '');

      // Verify the file actually contains a KINEMATIC createRigidBody call.
      // Allow object prefix (gameState.createRigidBody) and backtick keys.
      const collapsed = stripped.replace(/\r?\n/g, ' ').replace(/\s+/g, ' ');
      const hasKinematicBody = /(?:\w+\.)?createRigidBody[^)]*['"`]KINEMATIC['"`][^)]*\)/.test(collapsed);
      if (!hasKinematicBody) return false;

      // Extract the body of the physics-thread input handler (updateInput or equivalent).
      // setLinearVelocity inside this function operates on the DYNAMIC player body — that
      // is the correct API for DYNAMIC. We must NOT flag it.
      // Strategy: find the function assigned to addInputHandler and extract its body,
      // then exclude those line ranges from the setLinearVelocity scan.
      let inputHandlerRanges = []; // [{start, end}] line indices (0-based)
      const strippedLines = stripped.split('\n');
      // Find the addInputHandler registration line(s)
      strippedLines.forEach((line, i) => {
        if (/addInputHandler/.test(line)) {
          // The handler function name is the argument — look backwards for its definition
          const fnNameMatch = line.match(/addInputHandler\s*\(\s*(\w+)\s*\)/);
          if (fnNameMatch) {
            const fnName = fnNameMatch[1];
            // Find the function definition by name
            for (let j = 0; j < strippedLines.length; j++) {
              if (new RegExp(`(?:var|let|const)\\s+${fnName}\\s*=|function\\s+${fnName}\\s*\\(`).test(strippedLines[j])) {
                // Walk braces to find the function body extent
                let depth = 0, started = false, endLine = j;
                for (let k = j; k < strippedLines.length; k++) {
                  for (const ch of strippedLines[k]) {
                    if (ch === '{') { depth++; started = true; }
                    else if (ch === '}') { depth--; }
                  }
                  if (started && depth === 0) { endLine = k; break; }
                }
                inputHandlerRanges.push({ start: j, end: endLine });
                break;
              }
            }
          }
        }
      });

      // Collect all variable names in the file that are assigned as .RigidBody —
      // these are raw RigidBody objects. setLinearVelocity on them may be a legitimate
      // velocity-zeroing call on a DYNAMIC body (e.g. killPlayer / resetPlayer).
      // We must NOT flag those — setLinearVelocity is only a no-op on KINEMATIC bodies.
      const rigidBodyVars9 = new Set();
      strippedLines.forEach(line => {
        const m = line.match(/(?:const|let|var)\s+(\w+)\s*=\s*[\w.[\]()]+\.RigidBody\b/);
        if (m) rigidBodyVars9.add(m[1]);
      });

      // Check for setLinearVelocity on non-comment lines OUTSIDE the input handler
      // AND NOT inside a .set([...]) block called on a known RigidBody variable.
      //
      // setLinearVelocity is used in two valid forms:
      //   1. Inside updateInput (physics-thread handler) — always OK, covered above.
      //   2. As a property in a .RigidBody.set([{prop:'setLinearVelocity', value:[...]}])
      //      call from the main thread — valid on DYNAMIC bodies (e.g. killPlayer/reset).
      //
      // For form 2, the property name appears alone on its line inside the array literal.
      // We must look back up to 5 lines to find the enclosing .set([ call and check its caller.
      const hasLinearVelocityOutsideHandler = strippedLines.some((line, i) => {
        const t = line.trim();
        if (t.startsWith('//') || t.startsWith('*')) return false;
        if (!/setLinearVelocity/.test(line)) return false;
        // Skip if this line is inside any identified input handler function body
        const inHandler = inputHandlerRanges.some(r => i >= r.start && i <= r.end);
        if (inHandler) return false;
        // Case A: setLinearVelocity appears as a direct method call on this line
        const callerMatch = line.match(/([\w.[\]]+)\.set\s*\(/);
        if (callerMatch) {
          const callerVar = callerMatch[1].split('.').pop().replace(/\[.*\]/, '');
          if (callerVar === 'RigidBody' || rigidBodyVars9.has(callerVar)) return false;
        }
        // Case B: setLinearVelocity is a property value inside a .set([{...}]) block.
        // Walk back up to 6 lines to find the opening .set([ call and check its caller.
        const lookback = strippedLines.slice(Math.max(0, i - 6), i).join(' ');
        const lbCallerMatch = lookback.match(/([\w.[\]]+)\.set\s*\(\s*\[/);
        if (lbCallerMatch) {
          const lbCallerVar = lbCallerMatch[1].split('.').pop().replace(/\[.*\]/, '');
          if (lbCallerVar === 'RigidBody' || rigidBodyVars9.has(lbCallerVar)) return false;
        }
        return true;
      });
      if (hasLinearVelocityOutsideHandler) return true;

      // Secondary: KINEMATIC body exists but neither movement mechanism is present.
      // Catches "deleted velocity without adding setPosition" fix attempts.
      const hasSetPosition = /\.set\s*\(\s*\[\s*\{[^}]*['"]setPosition['"]/.test(stripped);
      // Accept both .obj.position and .object.position as valid visual mesh assignments
      const hasObjPosition = /\.\s*(?:obj|object)\s*\.\s*position\s*=\s*\[/.test(stripped);
      // If KINEMATIC body was created but the file has no movement mechanism at all,
      // the actor is silently frozen — flag it so the repair adds the correct dual-update.
      return !hasSetPosition && !hasObjPosition;
    },
    message: `FATAL: setLinearVelocity is a SILENT NO-OP on KINEMATIC bodies — no error is thrown but the actor will not move. This includes the case where setLinearVelocity was removed but no replacement movement was added (actor silently frozen).
REQUIRED FIX — choose exactly ONE of these two paths based on the actor type:

PATH A — Keep KINEMATIC (correct for ghosts and scripted movers):
  Replace setLinearVelocity with BOTH of the following every frame:
    actor.object.position = [x, y, z];                                       // moves visual mesh
    actor.rb.RigidBody.set([{ prop: 'setPosition', value: [x, y, z] }]);   // moves collider
  Compute the new [x, y, z] position on the main thread based on tile logic or AI.

PATH B — Switch to DYNAMIC (correct for player and physics-driven actors):
  Change motionType from 'KINEMATIC' to 'DYNAMIC' in createRigidBody.
  Drive movement via rb.RigidBody.controls.setFloat(slot, velocity) inside updateInput().
  Read position back via rb.RigidBody.getMotionState().position in onRender.

DECISION RULE for PacMaze:
  Ghosts       → PATH A (KINEMATIC + setPosition dual-update, main-thread AI)
  Player       → PATH B (DYNAMIC + shared-memory controls, physics-thread velocity)
  Do NOT leave the actor with no movement mechanism — a frozen actor is worse than a no-op.`,
    severity: "FATAL"
  }
];

function getViolationLane(patternName = "", severity = "FATAL") {
  if (String(severity || "").toUpperCase() === "SOFT") return "soft";

  const surgicalPatterns = new Set([
    "Silent no-op controls path",
    "Unreliable physics readback",
    "Cross-context window global call",
    "Non-zero rbPosition",
    "onRender missing return true"
  ]);

  if (surgicalPatterns.has(patternName)) return "hard_objective";
  return "critical_runtime";
}

function getRetryBudgetForLane(lane = "soft") {
  return RETRY_POLICY[lane] ?? 0;
}

function buildHardeningQueueKey(item = {}) {
  return [item.file || "", item.pattern || "", item.lane || "", item.trancheIndex ?? ""].join("::");
}

function pushHardeningQueueItems(progress, items = []) {
  if (!progress.hardeningQueue) progress.hardeningQueue = [];
  const seen = new Set(progress.hardeningQueue.map(buildHardeningQueueKey));
  for (const item of items) {
    const key = buildHardeningQueueKey(item);
    if (seen.has(key)) continue;
    seen.add(key);
    progress.hardeningQueue.push({ queuedAt: Date.now(), resolved: false, ...item });
  }
  return progress.hardeningQueue;
}

function formatHardeningQueue(items = []) {
  if (!items.length) return "No queued hardening items.";
  return items.map((item, idx) => [
    `ITEM ${idx + 1}`,
    `  Tranche : ${item.trancheIndex !== undefined ? item.trancheIndex + 1 : "n/a"} — ${item.trancheName || "Unknown tranche"}`,
    `  Lane    : ${item.lane || "soft"}`,
    `  File    : ${item.file || "unknown"}`,
    `  Pattern : ${item.pattern || item.kind || "General hardening"}`,
    `  Detail  : ${item.message || item.note || "No detail provided."}`
  ].join('\n')).join('\n\n');
}

function isHardeningBatchTranche(tranche = {}) {
  return Boolean(
    tranche.kind === HARDENING_BATCH_KIND ||
    tranche.isHardeningBatch ||
    String(tranche.name || "").toLowerCase().includes("hardening")
  );
}

function buildHardeningBatchUserText({ progress, accumulatedFiles, tranche, modelAnalysis }) {
  const queuedItems = Array.isArray(progress?.hardeningQueue) ? progress.hardeningQueue : [];
  let text = '';
  text += `=== HARDENING BATCH CONTEXT ===
`;
  text += `You are resolving deferred tranche findings in one end-stage batch.
`;
  text += `This batch exists to clean up queued advisories and unresolved objective issues without redoing earlier tranches.

`;
  text += `=== QUEUED HARDENING ITEMS ===
${formatHardeningQueue(queuedItems)}
=== END QUEUED HARDENING ITEMS ===

`;
  text += `=== HARDENING BATCH MANIFEST ===
`;
  text += `Name: ${tranche.name}
`;
  text += `Purpose: ${tranche.purpose || tranche.description || 'Resolve queued issues in a single final pass.'}
`;
  text += `Visible Result: ${tranche.visibleResult || 'Project remains runnable with queued issues resolved.'}
`;
  text += `Safety Checks:
${(tranche.safetyChecks || []).map((s, i) => `  ${i + 1}. ${s}`).join('\n') || '  1. Preserve all working systems and only fix queued items.'}
`;
  text += `=== END HARDENING BATCH MANIFEST ===

`;
  text += `=== CURRENT ACCUMULATED FILES ===
`;
  for (const [pathName, fileContent] of Object.entries(accumulatedFiles || {})) {
    text += `
--- FILE: ${pathName} ---
${fileContent}
`;
  }
  text += `
=== END CURRENT ACCUMULATED FILES ===

`;
  if (Array.isArray(modelAnalysis) && modelAnalysis.length > 0) {
    text += `=== THREE.JS MODEL ANALYSIS ===
${JSON.stringify(modelAnalysis, null, 2)}
=== END THREE.JS MODEL ANALYSIS ===

`;
  }
  text += `Resolve the queued hardening items with the minimum safe edits required. Preserve all existing working code. Output complete updated file contents only for the files you changed.`;
  return text;
}

function buildHardeningQueueItems(tranche, trancheIndex, violations = [], noteType = 'validation') {
  return violations.map((violation) => ({
    trancheIndex,
    trancheName: tranche?.name || `Tranche ${trancheIndex + 1}`,
    file: violation.file,
    pattern: violation.pattern,
    message: violation.message,
    severity: violation.severity,
    lane: violation.lane || getViolationLane(violation.pattern, violation.severity),
    retryBudget: violation.retryBudget ?? getRetryBudgetForLane(violation.lane || getViolationLane(violation.pattern, violation.severity)),
    noteType
  }));
}

function runAntiPatternValidation(files) {
  const violations = [];
  const normalizedFiles = Array.isArray(files) ? files : [];

  for (const file of normalizedFiles) {
    const pathName = String(file?.path || "");
    const content = String(file?.content || "");
    if (!pathName || !content) continue;

    for (const antiPattern of ANTI_PATTERNS) {
      try {
        if (!antiPattern?.test || !antiPattern.test(content)) continue;
        const severity = String(antiPattern.severity || 'FATAL').toUpperCase();
        const lane = getViolationLane(antiPattern.name, severity);
        violations.push({
          file: pathName,
          pattern: antiPattern.name,
          message: antiPattern.message,
          severity,
          lane,
          retryBudget: getRetryBudgetForLane(lane)
        });
      } catch (err) {
        console.warn(`Anti-pattern validator failed for ${antiPattern?.name || 'unknown'} on ${pathName}: ${err.message}`);
      }
    }
  }

  return violations;
}

/* ── DYNAMIC_ARCHITECTURE_JSON_SCHEMA — REMOVED ─────────────
   Architect pass has been merged into single-pass planner.
   No intermediate architecture spec is generated. ────────── */

/* ── helper: call Claude API ─────────────────────────────────── */
const CLAUDE_OVERLOAD_MAX_RETRIES = 5;
const CLAUDE_OVERLOAD_BASE_DELAY_MS = 1250;
const CLAUDE_OVERLOAD_MAX_DELAY_MS = 12000;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function computeClaudeRetryDelayMs(attempt) {
  const exponentialDelay = Math.min(
    CLAUDE_OVERLOAD_BASE_DELAY_MS * Math.pow(2, Math.max(0, attempt - 1)),
    CLAUDE_OVERLOAD_MAX_DELAY_MS
  );
  const jitter = Math.floor(Math.random() * 700);
  return exponentialDelay + jitter;
}

function isClaudeOverloadError(status, message = "") {
  const normalized = String(message || "").toLowerCase();
  if ([429, 500, 502, 503, 504, 529].includes(Number(status))) return true;
  return (
    normalized.includes("overloaded") ||
    normalized.includes("overload") ||
    normalized.includes("rate limit") ||
    normalized.includes("too many requests") ||
    normalized.includes("capacity") ||
    normalized.includes("temporarily unavailable")
  );
}

async function callClaude(apiKey, { model, maxTokens, system, userContent, effort, budgetTokens }) {
  const body = {
    model,
    max_tokens: maxTokens,
    system,
    messages: [{ role: "user", content: userContent }]
  };

  if (budgetTokens) {
    body.thinking = { type: "enabled", budget_tokens: budgetTokens };
  }

  if (effort) {
    body.output_config = { effort };
  }

  let lastError = null;

  for (let attempt = 1; attempt <= CLAUDE_OVERLOAD_MAX_RETRIES; attempt++) {
    try {
      const res = await fetch("https://api.anthropic.com/v1/messages", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-api-key": apiKey,
          "anthropic-version": "2023-06-01"
        },
        body: JSON.stringify(body)
      });

      const rawText = await res.text();
      let data = null;

      if (rawText) {
        try {
          data = JSON.parse(rawText);
        } catch (parseErr) {
          if (!res.ok) {
            throw new Error(`Claude API error (${res.status}) with non-JSON body: ${rawText.slice(0, 500)}`);
          }
          throw new Error(`Failed to parse Claude response JSON: ${parseErr.message}`);
        }
      }

      if (!res.ok) {
        const apiMessage = data?.error?.message || `Claude API error (${res.status})`;
        const err = new Error(apiMessage);
        err.status = res.status;
        err.isRetryableOverload = isClaudeOverloadError(res.status, apiMessage);
        throw err;
      }

      const responseText = data?.content?.find(b => b.type === "text")?.text;
      if (!responseText) {
        throw new Error("Empty response from Claude");
      }

      return {
        text: responseText,
        usage: data?.usage || null
      };
    } catch (err) {
      const status = Number(err?.status || 0);
      const retryable =
        Boolean(err?.isRetryableOverload) ||
        isClaudeOverloadError(status, err?.message);

      lastError = err;

      if (!retryable || attempt >= CLAUDE_OVERLOAD_MAX_RETRIES) {
        throw err;
      }

      const delayMs = computeClaudeRetryDelayMs(attempt);
      console.warn(
        `[callClaude] retrying Claude request after overload/rate-limit ` +
        `(attempt ${attempt}/${CLAUDE_OVERLOAD_MAX_RETRIES}, model=${model}, status=${status || "n/a"}, delay=${delayMs}ms): ${err.message}`
      );
      await sleep(delayMs);
    }
  }

  throw lastError || new Error("Claude request failed after retries");
}

/* ── helper: strip markdown fences and prose to extract JSON ─── */
/* Used ONLY for the planning phase (Opus), which outputs pure metadata
   strings — no embedded code — so JSON is safe there.               */
function stripFences(text) {
  let cleaned = text
    .replace(/^```json\s*/i, "")
    .replace(/^```\s*/i, "")
    .replace(/\s*```$/i, "")
    .trim();

  const firstBrace = cleaned.indexOf('{');
  const lastBrace = cleaned.lastIndexOf('}');
  if (firstBrace > 0 && lastBrace > firstBrace) {
    cleaned = cleaned.substring(firstBrace, lastBrace + 1);
  }

  return cleaned.trim();
}

function safeJsonParse(text, label) {
  try {
    return JSON.parse(stripFences(text));
  } catch (error) {
    throw new Error(`Failed to parse ${label} output as JSON: ${error.message}`);
  }
}

/* ── buildArchitectureSpecBlock — REMOVED ─────────────────
   No longer needed. Single-pass planner embeds game-specific
   rules directly in each tranche prompt. ─────────────────── */

const REQUIRED_TRANCHE_VALIDATION_BLOCK = `
VALIDATION + RECOVERY CONTRACT:
- Design tranche prompts so tranche success is judged first by visibleResult + safetyChecks.
- Do NOT plan tranches that depend on stylistic perfection or one preferred coding style to pass.
- Objective scaffold/runtime mistakes may be retried, but only under the runtime policy:
  • 0 retries for soft/advisory findings.
  • 1 retry max for narrow objective hard failures when the repair is obviously surgical.
  • 2 retries only for parser/envelope failures or truly critical scaffold/runtime issues.
  • Everything else is deferred into one end-stage hardening batch.
- Always make the final tranche a single end-stage hardening batch anchored to Section 8 so deferred findings are resolved in one pass.`;

function countOccurrences(haystack, needle) {
  if (!needle) return 0;
  return String(haystack || '').split(needle).length - 1;
}

function assertTranchePromptHasRequiredManifestBlock(tranche, index) {
  if (!tranche || typeof tranche.prompt !== 'string' || !tranche.prompt.trim()) {
    throw new Error(`Planner tranche ${index + 1} is missing a prompt.`);
  }
  return true;
}

function normalizeArray(value, fallback = []) {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value === undefined || value === null || value === '') return [...fallback];
  return [value].filter(Boolean);
}

function appendSyntheticHardeningTranche(plan) {
  const tranches = Array.isArray(plan?.tranches) ? plan.tranches : [];
  if (tranches.some(isHardeningBatchTranche)) return plan;

  const maxPhase = tranches.reduce((max, tranche) => Math.max(max, Number(tranche?.phase || 0)), 0);
  tranches.push({
    kind: HARDENING_BATCH_KIND,
    isHardeningBatch: true,
    name: HARDENING_BATCH_NAME,
    description: 'Single deferred batch that resolves queued soft findings and unresolved objective issues after the functional tranches are complete.',
    anchorSections: ['8.1', '8.3'],
    purpose: 'Resolve the queued hardening backlog in one final pass without redoing earlier tranches.',
    systemsTouched: ['cross-system hardening', 'final acceptance', 'deferred validation cleanup'],
    filesTouched: ['models/2', 'models/23'],
    visibleResult: 'The project remains runnable and any queued hardening items are resolved in one final pass.',
    safetyChecks: [
      'Preserve all already-working tranche output.',
      'Fix queued hardening items with the smallest safe edits.',
      'Do not regress gameplay, HUD, lifecycle, or restart behavior while hardening.'
    ],
    expertAgents: ['integration', 'qa'],
    phase: maxPhase + 1,
    dependencies: tranches.map((tr, idx) => tr?.name || `Tranche ${idx + 1}`),
    qualityCriteria: [
      'Queued hardening items are resolved without regressions.',
      'The final code remains scaffold-compliant and runnable.'
    ],
    prompt: 'Synthetic hardening batch — runtime will inject the deferred hardening queue.',
    expectedFiles: ['models/2', 'models/23']
  });

  plan.tranches = tranches;
  return plan;
}

function enforceTrancheValidationBlock(plan) {
  const rawTranches = Array.isArray(plan?.tranches) ? plan.tranches : [];
  plan.tranches = rawTranches.map((tranche, index) => {
    const expectedFiles = normalizeArray(tranche.expectedFiles || tranche.filesTouched, ['models/2', 'models/23']);
    return {
      kind: tranche.kind || 'build',
      name: tranche.name || `Tranche ${index + 1}`,
      description: tranche.description || tranche.purpose || `Implement tranche ${index + 1}.`,
      anchorSections: normalizeArray(tranche.anchorSections, ['6.3']),
      purpose: tranche.purpose || tranche.description || `Implement tranche ${index + 1}.`,
      systemsTouched: normalizeArray(tranche.systemsTouched, ['gameplay']),
      filesTouched: normalizeArray(tranche.filesTouched, expectedFiles),
      visibleResult: tranche.visibleResult || tranche.description || `Tranche ${index + 1} produces a runnable incremental result.`,
      safetyChecks: normalizeArray(tranche.safetyChecks, tranche.qualityCriteria || ['Leave the project runnable after this tranche.']),
      expertAgents: normalizeArray(tranche.expertAgents, []),
      phase: Number(tranche.phase || 0),
      dependencies: normalizeArray(tranche.dependencies, []),
      qualityCriteria: normalizeArray(tranche.qualityCriteria, []),
      prompt: String(tranche.prompt || '').trim(),
      expectedFiles,
      isHardeningBatch: Boolean(tranche.isHardeningBatch || tranche.kind === HARDENING_BATCH_KIND)
    };
  });

  return appendSyntheticHardeningTranche(plan);
}

/* ── helper: parse tranche executor delimiter-format responses ── */
/* Tranche executors output raw file content between delimiters,
   completely bypassing JSON escaping. This eliminates the entire
   class of "Unexpected non-whitespace character after JSON" errors
   that occur when Claude embeds code inside a JSON string field.

   Expected format from the executor:
     ===FILE_START: models/2===
     ...raw file content, zero escaping needed...
     ===FILE_END: models/2===

     ===MESSAGE===
     Changelog text here
     ===END_MESSAGE===
*/
function parseDelimitedResponse(text) {
  const files = [];

  // Extract all FILE_START / FILE_END blocks
  const fileRegex = /===FILE_START:\s*([^\n]+?)\s*===\n([\s\S]*?)===FILE_END:\s*\1\s*===/g;
  let match;
  while ((match = fileRegex.exec(text)) !== null) {
    const path = match[1].trim();
    const content = match[2]; // preserve exactly — no trimming
    if (path && content !== undefined) {
      files.push({ path, content });
    }
  }

  // Extract message block
  const msgMatch = text.match(/===MESSAGE===\n([\s\S]*?)===END_MESSAGE===/);
  const message = msgMatch ? msgMatch[1].trim() : "Tranche completed.";

  // If no delimiters found at all, fall back to JSON for backwards compat
  if (files.length === 0) {
    try {
      const parsed = JSON.parse(stripFences(text));
      if (parsed && Array.isArray(parsed.updatedFiles)) {
        console.warn("Executor used JSON format instead of delimiter format — parsed as fallback.");
        return parsed;
      }
    } catch (_) { /* ignore */ }
    // Return empty-handed; caller will treat as a skippable parse error
    return null;
  }

  return { updatedFiles: files, message };
}

/* ── helper: save progress to Firebase ───────────────────────── */
async function saveProgress(bucket, projectPath, progress) {
  await bucket.file(`${projectPath}/ai_progress.json`).save(
    JSON.stringify(progress),
    { contentType: "application/json", resumable: false }
  );
}

/* ── helper: save ai_response.json with freshness metadata ───── */
/* Called after every successful tranche merge (checkpoint), on
   cancellation, and at final completion so the frontend always has
   the best available snapshot and can verify payload freshness.    */
async function saveAiResponse(bucket, projectPath, allUpdatedFiles, meta = {}) {
  const payload = {
    jobId:         meta.jobId        || "unknown",
    timestamp:     Date.now(),
    trancheIndex:  meta.trancheIndex !== undefined ? meta.trancheIndex : null,
    totalTranches: meta.totalTranches || null,
    status:        meta.status       || "checkpoint", // "checkpoint" | "cancelled" | "final"
    message:       meta.message      || "",
    updatedFiles:  allUpdatedFiles   || []
  };
  await bucket.file(`${projectPath}/ai_response.json`).save(
    JSON.stringify(payload),
    { contentType: "application/json", resumable: false }
  );
}

/* ── helper: save pipeline state to Firebase ─────────────────── */
async function savePipelineState(bucket, projectPath, state) {
  await bucket.file(`${projectPath}/ai_pipeline_state.json`).save(
    JSON.stringify(state),
    { contentType: "application/json", resumable: false }
  );
}

/* ── helper: load pipeline state from Firebase ───────────────── */
async function loadPipelineState(bucket, projectPath) {
  const file = bucket.file(`${projectPath}/ai_pipeline_state.json`);
  const [exists] = await file.exists();
  if (!exists) return null;
  const [content] = await file.download();
  return JSON.parse(content.toString());
}

/* ── helper: check kill switch ───────────────────────────────── */
async function checkKillSwitch(bucket, projectPath, jobId) {
  try {
    const activeJobFile = bucket.file(`${projectPath}/ai_active_job.json`);
    const [exists] = await activeJobFile.exists();
    if (exists) {
      const [content] = await activeJobFile.download();
      const activeData = JSON.parse(content.toString());

      if (activeData.jobId && activeData.jobId !== jobId) {
        return { killed: true, reason: "superseded", newJobId: activeData.jobId };
      }
      if (activeData.cancelled) {
        return { killed: true, reason: "cancelled" };
      }
    }
  } catch (e) { /* no active job file = continue safely */ }
  return { killed: false };
}

/* ── helper: self-chain — invoke this function again ─────────── */
async function chainToSelf(payload) {
  const siteUrl = process.env.URL || process.env.DEPLOY_URL || "";
  const chainUrl = `${siteUrl}/.netlify/functions/claudeCodeProxy-background`;

  console.log(`CHAIN → next step: mode=${payload.mode}, tranche=${payload.nextTranche ?? "n/a"} → ${chainUrl}`);

  try {
    const res = await fetch(chainUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    // Background functions return 202 immediately — we don't wait.
    console.log(`Chain response status: ${res.status}`);
  } catch (err) {
    console.error("Chain invocation failed:", err.message);
    throw new Error(`Self-chain failed: ${err.message}`);
  }
}

/* ═══════════════════════════════════════════════════════════════
   SPEC VALIDATION GATE — three Sonnet calls before Opus planning
   ═══════════════════════════════════════════════════════════════

   Call 1 (Extract)  : reads the Master Prompt and identifies the
     game's actual mechanics, producing 6-8 custom simulation
     scenarios tailored to THIS game. Generic enough to work for
     any genre; never hard-codes Fish_Hunt fields.

   Call 2 (Simulate) : traces each scenario through the spec rules
     literally. Documents TRACE / RESULT / ISSUE for each.

   Call 3 (Review)   : classifies simulation findings as PASS/FAIL
     and emits a structured JSON issues list.

   All three calls use claude-sonnet-4-20250514.
   Extended thinking is disabled for these calls (no budgetTokens)
   so they stay fast and cheap — under ~20 seconds total.
   ═══════════════════════════════════════════════════════════════ */

/* ── Known engine constraints injected into Call 2 ──────────────
   These are scaffold-level facts that the Master Prompt author
   should not have to write — they apply to every Cherry3D game. */
const SCAFFOLD_VALIDATION_CONSTRAINTS = `
KNOWN ENGINE CONSTRAINTS (Cherry3D scaffold v8.1):
These apply to every game regardless of what the Master Prompt says.
Factor them into your simulations where relevant.

1. OBJECT POOLING: Any spawn/despawn cycle MUST use ScenePool.
   A count cap alone does not prevent WASM object accumulation.
   If the spec has no pooling mechanism, note objectAccumulationRisk.

2. ROOT OBJECT ROTATION: .rotation/.rotate on a root scene object
   is a silent no-op. Directional characters must use scale flip.
   If the spec describes characters that face a travel direction
   with no mechanism stated, note it as a risk.

3. CHILD RIGIDBODY POSITION: A RigidBody added as a child of a
   mesh returns local-space coords from getMotionState(). Top-down
   games must track player position by integrating velocity on the
   main thread — not by reading the child RB.

4. TOP-DOWN CAMERA: mat4.lookAt with a vertical camera produces a
   degenerate matrix. The scaffold handles this automatically —
   no spec action needed, but note it if relevant to the game.
`;

/* ── Call 1: Extract game-specific simulation scenarios ─────── */
function buildExtractionPrompt(masterPrompt) {
  return `You are a game logic analyst. Read the Master Game Prompt below \
and identify the game's core mechanics that could contain logical errors \
before any code is written.

Produce a JSON array of 6-8 simulation scenarios tailored specifically \
to THIS game. Each scenario must be concrete and traceable — it must \
have a specific setup, a specific spec rule to apply, and a question \
that has a definite numerical or boolean answer.

Cover these four areas for every game:

1. START STATE VIABILITY
   Can the player do anything meaningful in the very first seconds?
   Is there a condition at the exact start value that might be impossible?

2. FIRST INTERACTION CORRECTNESS
   What happens when the player performs the primary action for the
   first time? Does the formula or condition produce the correct result?

3. PROGRESSION FORMULA BEHAVIOUR
   Does the score/growth/currency formula produce smooth progression
   or explosive/broken jumps at representative values?

4. STATE TRANSITION COMPLETENESS
   Do all UI state transitions (death → modal, shop open → close,
   pause → resume, restart) leave the game in a clean defined state?

Also check: does the spec describe a spawn/despawn cycle? If so,
include a scenario that checks whether the spec explicitly requires
object pooling (not just a count cap).

MASTER GAME PROMPT:
${masterPrompt}

Respond with ONLY a valid JSON array. No markdown fences, no preamble.

[
  {
    "id": "SIM-01",
    "area": "start state viability",
    "setup": "exact starting conditions from the spec",
    "specRule": "the relevant rule to quote verbatim from the spec",
    "question": "the specific concrete question to answer",
    "expectedBehaviour": "what correct gameplay looks like here"
  }
]`;
}

/* ── Call 2: Simulate the scenarios against the spec ─────────── */
function buildSimulationPrompt(masterPrompt, scenarios) {
  const scenarioBlock = scenarios.map(s =>
`${s.id} — ${String(s.area || '').toUpperCase()}
  Setup:    ${s.setup}
  Rule:     find and quote verbatim: "${s.specRule}"
  Question: ${s.question}
  Expected: ${s.expectedBehaviour}

  TRACE:  [apply the rule literally, step by step]
  RESULT: [the specific outcome — a number, a state, a behaviour]
  ISSUE:  "none" OR precise description of the problem found`
  ).join('\n\n');

  return `You are a game logic validator. You have been given a Master \
Game Prompt and a set of simulation scenarios tailored to this specific \
game. Trace each scenario through the spec rules literally and document \
exactly what you find.

Do NOT write code. Do NOT summarise the spec. Apply every rule exactly \
as written. If a rule says "strictly less than", apply strictly less than.

${SCAFFOLD_VALIDATION_CONSTRAINTS}

MASTER GAME PROMPT:
${masterPrompt}

SIMULATIONS TO RUN:
${scenarioBlock}

Do not skip any simulation. Do not add simulations not listed above.`;
}

/* ── Call 3: Classify simulation findings as PASS / FAIL ─────── */
function buildReviewPrompt(simulationDoc, scenarios) {
  const simIds = scenarios.map(s => s.id).join(', ');
  return `You are a spec review classifier. Read the simulation document \
below and classify each finding. Do NOT re-run simulations. Do NOT \
introduce new reasoning. ONLY classify what the simulation document \
already found.

Simulation IDs that were run: ${simIds}

SIMULATION DOCUMENT:
${simulationDoc}

Respond with ONLY a valid JSON object. No markdown fences, no preamble.

{
  "result": "PASS" or "FAIL",
  "summary": "one sentence describing the overall finding",
  "issues": [
    {
      "id": "SIM-XX",
      "severity": "CRITICAL" or "HIGH" or "MEDIUM",
      "rule": "the spec rule that is broken, quoted verbatim",
      "description": "precise description of the problem",
      "recommendation": "minimum spec change that fixes this"
    }
  ],
  "passedSimulations": ["SIM-01", "SIM-03"],
  "failedSimulations": ["SIM-02"],
  "objectAccumulationRisk": true or false,
  "startStatePlayable": true or false
}

Classification rules:
- result is FAIL if ANY issue is CRITICAL or HIGH
- result is FAIL if startStatePlayable is false
- result is PASS only if all issues are MEDIUM or lower AND startStatePlayable is true
- startStatePlayable is false if any start-state simulation found the
  player cannot perform the primary action at the initial game state
- objectAccumulationRisk is true if any simulation found a spawn/despawn
  cycle with no pooling requirement stated in the spec
- issues array is empty if all simulations passed cleanly`;
}

/* ── stripArrayFences — strips markdown fences, finds first JSON array ── */
function stripArrayFences(text) {
  let cleaned = text
    .replace(/^```json\s*/i, '')
    .replace(/^```\s*/i, '')
    .replace(/\s*```$/i, '')
    .trim();
  const firstBracket = cleaned.indexOf('[');
  const lastBracket  = cleaned.lastIndexOf(']');
  if (firstBracket >= 0 && lastBracket > firstBracket) {
    cleaned = cleaned.substring(firstBracket, lastBracket + 1);
  }
  return cleaned.trim();
}

/* ── Call 4: Patch — adds missing rules to the Master Prompt ─── */
/* Only called for MEDIUM-severity issues. Never changes an         */
/* existing rule — only adds what is missing.                       */
function buildPatchPrompt(masterPrompt, mediumIssues) {
  const issueBlock = mediumIssues.map((issue, i) =>
`Gap ${i + 1} (${issue.id}):
  Problem:     ${issue.description}
  Missing from: ${issue.rule || 'spec — rule not found or underspecified'}
  Add this:    ${issue.recommendation}`
  ).join('\n\n');

  return `You are a game spec editor. You have been given a Master Game \
Prompt and a list of MEDIUM severity spec gaps — rules that are missing \
or underspecified. Your job is to add the missing rules.

STRICT CONSTRAINTS:
- Add ONLY what is needed to resolve the listed gaps.
- Do NOT change any existing rule, formula, condition, or threshold.
- Do NOT invent new gameplay mechanics.
- Do NOT restructure or reformat the existing prompt.
- Append new rules in the same numbered-list style already used.
- Output the COMPLETE updated Master Prompt — every existing line intact.

ORIGINAL MASTER PROMPT:
${masterPrompt}

SPEC GAPS TO RESOLVE:
${issueBlock}

Output the complete updated Master Prompt with the missing rules added.`;
}

/* ── Run one full validation pass (Calls 1-2-3) ─────────────── */
/* Extracted so the retry loop can call it cleanly.               */
async function runSingleValidationPass(apiKey, masterPrompt, scenarios, bucket, projectPath, attempt) {
  // Call 2: Simulate
  const simResult = await callClaude(apiKey, {
    model:       'claude-sonnet-4-20250514',
    maxTokens:   8000,
    system:      'You are a game logic validator. Be precise and literal.',
    userContent: [{ type: 'text', text: buildSimulationPrompt(masterPrompt, scenarios) }]
  });
  const simulationDoc = simResult.text;

  try {
    await bucket.file(`${projectPath}/ai_validation_simulation${attempt > 0 ? `_patch${attempt}` : ''}.txt`)
      .save(simulationDoc, { contentType: 'text/plain', resumable: false });
  } catch (e) { /* non-fatal */ }

  // Call 3: Review
  const reviewResult = await callClaude(apiKey, {
    model:       'claude-sonnet-4-20250514',
    maxTokens:   6000,
    system:      'You are a spec review classifier. Respond only with a valid JSON object.',
    userContent: [{ type: 'text', text: buildReviewPrompt(simulationDoc, scenarios) }]
  });
  const reviewData = JSON.parse(stripFences(reviewResult.text));

  try {
    await bucket.file(`${projectPath}/ai_validation_review${attempt > 0 ? `_patch${attempt}` : ''}.json`)
      .save(JSON.stringify(reviewData, null, 2), { contentType: 'application/json', resumable: false });
  } catch (e) { /* non-fatal */ }

  return { simulationDoc, reviewData };
}

/* ── Main validation gate orchestrator — with patch retry loop ── */
async function runSpecValidationGate(apiKey, masterPrompt, progress, bucket, projectPath, jobId) {
  console.log(`[VALIDATION] Starting spec validation gate for job ${jobId}`);

  const MAX_PATCH_ATTEMPTS = 2;

  // Update progress so the frontend shows a validating state
  progress.status = 'validating';
  progress.validationStartTime = Date.now();
  await saveProgress(bucket, projectPath, progress);

  // ── Call 1: Extract scenarios (runs once — same scenarios for all passes) ──
  console.log('[VALIDATION] Call 1: extracting game-specific scenarios...');
  let scenarios;
  try {
    const extractResult = await callClaude(apiKey, {
      model:       'claude-sonnet-4-20250514',
      maxTokens:   3000,
      system:      'You are a game logic analyst. Respond only with a valid JSON array.',
      userContent: [{ type: 'text', text: buildExtractionPrompt(masterPrompt) }]
    });
    scenarios = JSON.parse(stripArrayFences(extractResult.text));
    if (!Array.isArray(scenarios) || scenarios.length === 0) throw new Error('Empty scenario array');
    console.log(`[VALIDATION] Extracted ${scenarios.length} scenario(s): ${scenarios.map(s => s.id).join(', ')}`);
  } catch (e) {
    console.warn(`[VALIDATION] Call 1 failed (${e.message}) — skipping validation, proceeding to planning`);
    progress.status = 'planning';
    progress.validationSkipped = true;
    progress.validationSkipReason = e.message;
    await saveProgress(bucket, projectPath, progress);
    return { passed: true, skipped: true, activePrompt: masterPrompt };
  }

  try {
    await bucket.file(`${projectPath}/ai_validation_scenarios.json`)
      .save(JSON.stringify(scenarios, null, 2), { contentType: 'application/json', resumable: false });
  } catch (e) { /* non-fatal */ }

  progress.validationScenarios = scenarios.map(s => s.id);
  progress.validationCall1Done = true;
  await saveProgress(bucket, projectPath, progress);

  // ── Patch retry loop — Calls 2 + 3 (+ optional Call 4 patch) ────────────
  let activePrompt   = masterPrompt;
  let patchAttempt   = 0;
  let allPatchHistory = [];  // accumulates every patch attempt for the UI

  for (let pass = 0; pass <= MAX_PATCH_ATTEMPTS; pass++) {

    const isRetry = pass > 0;
    console.log(`[VALIDATION] ${isRetry ? `Patch attempt ${pass}/${MAX_PATCH_ATTEMPTS}:` : 'Initial pass:'} running Calls 2+3...`);

    if (isRetry) {
      progress.validationPatchAttempt = pass;
      progress.validationPatchStatus  = 'simulating';
      await saveProgress(bucket, projectPath, progress);
    } else {
      progress.validationCall2Done = false;
      progress.validationCall3Done = false;
      await saveProgress(bucket, projectPath, progress);
    }

    // ── Calls 2 + 3 ────────────────────────────────────────────────────────
    let simulationDoc, reviewData;
    try {
      ({ simulationDoc, reviewData } = await runSingleValidationPass(
        apiKey, activePrompt, scenarios, bucket, projectPath, pass
      ));
    } catch (e) {
      console.warn(`[VALIDATION] Calls 2/3 failed on pass ${pass} (${e.message}) — skipping, proceeding to planning`);
      progress.status = 'planning';
      progress.validationSkipped = true;
      progress.validationSkipReason = e.message;
      await saveProgress(bucket, projectPath, progress);
      return { passed: true, skipped: true, activePrompt };
    }

    progress.validationCall2Done  = true;
    progress.validationCall3Done  = true;
    progress.validationResult     = reviewData.result;
    progress.validationSummary    = reviewData.summary;
    progress.validationIssues     = reviewData.issues || [];
    progress.validationEndTime    = Date.now();
    await saveProgress(bucket, projectPath, progress);

    console.log(`[VALIDATION] Pass ${pass} result: ${reviewData.result} — ${reviewData.summary}`);

    // ── PASS ────────────────────────────────────────────────────────────────
    if (reviewData.result === 'PASS') {
      progress.status = 'planning';
      progress.validationActivePromptPatched = activePrompt !== masterPrompt;
      await saveProgress(bucket, projectPath, progress);

      // If the prompt was patched, persist the patched version so Opus uses it
      // and preserve the original for the user's reference
      if (activePrompt !== masterPrompt) {
        try {
          await bucket.file(`${projectPath}/ai_validation_original_prompt.txt`)
            .save(masterPrompt, { contentType: 'text/plain', resumable: false });
          await bucket.file(`${projectPath}/ai_validation_patched_prompt.txt`)
            .save(activePrompt, { contentType: 'text/plain', resumable: false });
          console.log(`[VALIDATION] Patched prompt saved. Original preserved.`);
        } catch (e) { /* non-fatal */ }
      }

      return {
        passed:                 true,
        result:                 'PASS',
        summary:                reviewData.summary,
        issues:                 [],
        passedSimulations:      reviewData.passedSimulations || [],
        failedSimulations:      [],
        objectAccumulationRisk: reviewData.objectAccumulationRisk,
        startStatePlayable:     reviewData.startStatePlayable,
        scenarios,
        simulationDoc,
        activePrompt,              // ← caller uses this for Opus, not the original
        wasPatched:             activePrompt !== masterPrompt,
        patchCount:             pass,
        patchHistory:           allPatchHistory
      };
    }

    // ── FAIL — check severity split ─────────────────────────────────────────
    const issues      = reviewData.issues || [];
    const hardIssues  = issues.filter(i => i.severity === 'CRITICAL' || i.severity === 'HIGH');
    const mediumIssues = issues.filter(i => i.severity === 'MEDIUM');

    // Hard failures always halt immediately — never attempt to patch
    if (hardIssues.length > 0) {
      console.log(`[VALIDATION] Hard FAIL (${hardIssues.length} CRITICAL/HIGH) — halting, no auto-patch`);
      progress.status = 'validating';
      await saveProgress(bucket, projectPath, progress);
      return {
        passed:                 false,
        hardStop:               true,
        result:                 'FAIL',
        summary:                reviewData.summary,
        issues,
        passedSimulations:      reviewData.passedSimulations || [],
        failedSimulations:      reviewData.failedSimulations || [],
        objectAccumulationRisk: reviewData.objectAccumulationRisk,
        startStatePlayable:     reviewData.startStatePlayable,
        scenarios,
        simulationDoc,
        activePrompt,
        patchHistory:           allPatchHistory
      };
    }

    // ── MEDIUM-only FAIL — attempt patch if budget remains ──────────────────
    if (pass >= MAX_PATCH_ATTEMPTS) {
      // Budget exhausted
      console.log(`[VALIDATION] Patch budget exhausted after ${pass} attempt(s) — halting`);
      progress.status = 'validating';
      await saveProgress(bucket, projectPath, progress);
      return {
        passed:                 false,
        budgetExhausted:        true,
        result:                 'FAIL',
        summary:                reviewData.summary,
        issues,
        passedSimulations:      reviewData.passedSimulations || [],
        failedSimulations:      reviewData.failedSimulations || [],
        objectAccumulationRisk: reviewData.objectAccumulationRisk,
        startStatePlayable:     reviewData.startStatePlayable,
        scenarios,
        simulationDoc,
        activePrompt,
        patchHistory:           allPatchHistory
      };
    }

    // ── Call 4: Patch ────────────────────────────────────────────────────────
    patchAttempt = pass + 1;
    console.log(`[VALIDATION] MEDIUM-only fail. Running Call 4 (patch attempt ${patchAttempt})...`);
    progress.validationPatchAttempt = patchAttempt;
    progress.validationPatchStatus  = 'patching';
    await saveProgress(bucket, projectPath, progress);

    let patchedPrompt;
    try {
      const patchResult = await callClaude(apiKey, {
        model:       'claude-sonnet-4-20250514',
        maxTokens:   masterPrompt.length > 20000 ? 16000 : 8000,
        system:      'You are a game spec editor. Output only the updated Master Prompt.',
        userContent: [{ type: 'text', text: buildPatchPrompt(activePrompt, mediumIssues) }]
      });
      patchedPrompt = patchResult.text.trim();
      if (!patchedPrompt || patchedPrompt.length < activePrompt.length * 0.8) {
        throw new Error('Patch produced a truncated or empty prompt');
      }
    } catch (e) {
      console.warn(`[VALIDATION] Call 4 patch failed (${e.message}) — halting validation`);
      progress.status = 'validating';
      await saveProgress(bucket, projectPath, progress);
      return {
        passed:  false,
        result:  'FAIL',
        summary: reviewData.summary,
        issues,
        passedSimulations:      reviewData.passedSimulations || [],
        failedSimulations:      reviewData.failedSimulations || [],
        objectAccumulationRisk: reviewData.objectAccumulationRisk,
        startStatePlayable:     reviewData.startStatePlayable,
        scenarios,
        simulationDoc,
        activePrompt,
        patchHistory: allPatchHistory
      };
    }

    // Save patch artifact
    allPatchHistory.push({ attempt: patchAttempt, issues: mediumIssues.map(i => i.id) });
    try {
      await bucket.file(`${projectPath}/ai_validation_patched_prompt_${patchAttempt}.txt`)
        .save(patchedPrompt, { contentType: 'text/plain', resumable: false });
    } catch (e) { /* non-fatal */ }

    progress.validationPatchStatus  = 'retrying';
    progress.validationPatchHistory = allPatchHistory;
    await saveProgress(bucket, projectPath, progress);

    activePrompt = patchedPrompt;
    console.log(`[VALIDATION] Patch ${patchAttempt} applied (${patchedPrompt.length} chars). Re-running validation...`);
  }

  // Should not reach here — loop exits via return inside
  return { passed: false, result: 'FAIL', activePrompt };
}

/* ═══════════════════════════════════════════════════════════════ */
exports.handler = async (event) => {
  let projectPath = null;
  let bucket = null;
  let jobId = null;

  try {
    if (!event.body) throw new Error("Missing request body");

    const parsedBody = JSON.parse(event.body);
    projectPath = parsedBody.projectPath;
    jobId = parsedBody.jobId;

    if (!projectPath) throw new Error("Missing projectPath");
    if (!jobId) throw new Error("Missing jobId");

    const apiKey = process.env.ANTHROPIC_API_KEY;
    if (!apiKey) throw new Error("Missing ANTHROPIC_API_KEY");

    bucket = admin.storage().bucket(process.env.FIREBASE_STORAGE_BUCKET || "gokudatabase.firebasestorage.app");

    // ── Determine mode: "plan" / "tranche" / "fix" ──────────────
    // "fix" re-runs the same tranche index with a correction prompt
    const mode = parsedBody.mode || "plan";
    const nextTranche = parsedBody.nextTranche || 0;
    const fixAttempt  = parsedBody.fixAttempt  || 0;  // 1-based, 0 means not a fix pass

    // ══════════════════════════════════════════════════════════════
    //  MODE: "plan" — First invocation, do planning then chain
    // ══════════════════════════════════════════════════════════════
    if (mode === "plan") {

      // ── 1. Download the request payload from Firebase ─────────
      const requestFile = bucket.file(`${projectPath}/ai_request.json`);
      const [content] = await requestFile.download();
      const { prompt, files, selectedAssets, inlineImages, modelAnalysis } = JSON.parse(content.toString());
      if (!prompt) throw new Error("Missing instructions inside payload");

      // ── 2. Spec Validation Gate ───────────────────────────────
      // Runs three Sonnet calls against the Master Prompt before
      // Opus planning starts. On FAIL, writes ai_error.json with
      // structured issues and halts without invoking Opus.
      // On any internal error the gate is skipped so a bad day
      // at the API doesn't block every game build.
      const earlyProgress = {
        jobId,
        status: 'validating',
        validationStartTime: Date.now()
      };
      await saveProgress(bucket, projectPath, earlyProgress);

      const validationResult = await runSpecValidationGate(
        apiKey, prompt, earlyProgress, bucket, projectPath, jobId
      );

      if (!validationResult.passed && !validationResult.skipped) {
        // Write structured error so the frontend polling loop picks it up
        await bucket.file(`${projectPath}/ai_error.json`).save(
          JSON.stringify({
            error:            `Spec validation FAILED — ${validationResult.issues.length} issue(s) must be resolved in the Master Prompt before code generation can proceed.`,
            jobId,
            validationFailed:    true,
            hardStop:            validationResult.hardStop        || false,
            budgetExhausted:     validationResult.budgetExhausted || false,
            summary:             validationResult.summary,
            issues:              validationResult.issues,
            passedSimulations:   validationResult.passedSimulations,
            failedSimulations:   validationResult.failedSimulations,
            objectAccumulationRisk: validationResult.objectAccumulationRisk,
            startStatePlayable:     validationResult.startStatePlayable,
            patchHistory:        validationResult.patchHistory || []
          }),
          { contentType: 'application/json', resumable: false }
        );
        console.log(`[VALIDATION] FAILED — halting pipeline. Issues: ${validationResult.issues.map(i => i.id).join(', ')}`);
        return { statusCode: 200, body: JSON.stringify({ success: false, validationFailed: true }) };
      }

      console.log(`[VALIDATION] ${validationResult.skipped ? 'SKIPPED (error in gate)' : 'PASSED'} — proceeding to Opus planning`);

      // Use the active prompt (patched or original) for all subsequent pipeline calls
      // If patched, the patched version is already saved in Firebase for the user's reference
      const effectivePrompt = (validationResult.activePrompt) || prompt;

      // ── 3. Build file context string ──────────────────────────
      let fileContext = "Here are the current project files:\n\n";
      if (files) {
        for (const [path, fileContent] of Object.entries(files)) {
          fileContext += `--- FILE: ${path} ---\n${fileContent}\n\n`;
        }
      }

      // ── 3. Build multi-modal content blocks ───────────────────
      const imageBlocks = [];

      if (selectedAssets && Array.isArray(selectedAssets) && selectedAssets.length > 0) {
        let assetContext = "\n\nThe user has designated the following files for use. Their relative paths in the project are:\n";
        for (const asset of selectedAssets) {
          assetContext += `- ${asset.path}\n`;
          const isSupportedImage =
            (asset.type && asset.type.startsWith("image/")) ||
            (asset.name && asset.name.match(/\.(png|jpe?g|webp)$/i));

          if (isSupportedImage) {
            try {
              const assetRes = await fetch(asset.url);
              if (!assetRes.ok) throw new Error(`Failed to fetch: ${assetRes.statusText}`);
              const arrayBuffer = await assetRes.arrayBuffer();
              const base64Data = Buffer.from(arrayBuffer).toString("base64");
              let mime = asset.type;
              if (!mime || !mime.startsWith("image/")) {
                if (asset.name.endsWith(".png")) mime = "image/png";
                else if (asset.name.endsWith(".jpg") || asset.name.endsWith(".jpeg")) mime = "image/jpeg";
                else if (asset.name.endsWith(".webp")) mime = "image/webp";
                else mime = "image/png";
              }
              imageBlocks.push({ type: "image", source: { type: "base64", media_type: mime, data: base64Data } });
            } catch (fetchErr) {
              console.error(`Failed to fetch visual asset ${asset.name}:`, fetchErr);
            }
          } else {
            assetContext += `  (Note: ${asset.name} is a non-image file. Reference it by path in code.)\n`;
          }
        }
        fileContext += assetContext;
      }

      if (modelAnalysis && Array.isArray(modelAnalysis) && modelAnalysis.length > 0) {
        fileContext += `\n\n=== THREE.JS MODEL ANALYSIS ===\n${JSON.stringify(modelAnalysis, null, 2)}\n`;
      }

      if (inlineImages && Array.isArray(inlineImages) && inlineImages.length > 0) {
        for (const img of inlineImages) {
          if (img.data && img.mimeType && img.mimeType.startsWith("image/")) {
            imageBlocks.push({ type: "image", source: { type: "base64", media_type: img.mimeType, data: img.data } });
          }
        }
      }

      // ══════════════════════════════════════════════════════════
      //  SINGLE-PASS PLANNING (Opus 4.6)
      //  Reads Master Prompt + Engine Reference + files directly.
      //  No intermediate architecture spec. No re-synthesis.
      //  Outputs tranche plan with rules embedded in each prompt.
      // ══════════════════════════════════════════════════════════

      // ── Fetch Scaffold + SDK instruction bundle ──
      const instructionBundle = await fetchInstructionBundle(bucket, projectPath);
      assertInstructionBundle(instructionBundle, "PLAN");

      const progress = {
        jobId: jobId,
        status: "planning",
        planningStartTime: Date.now(),
        planningEndTime: null,
        planningAnalysis: "",
        totalTranches: 0,
        currentTranche: -1,
        tranches: [],
        tokenUsage: {
          planning: null,
          tranches: [],
          totals: { input_tokens: 0, output_tokens: 0 }
        },
        finalMessage: null,
        error: null,
        completedTime: null,
        hardeningQueue: [],
        finalHardeningSummary: null
      };
      await saveProgress(bucket, projectPath, progress);

      const planningSystem = `You are an expert game development planner for the Cherry3D engine.

Your job: read the user's request, the existing project files, and the instruction bundle below. Then split the build into sequential, self-contained TRANCHES that can be executed one at a time by a coding AI.

${instructionBundle.combinedText}

INSTRUCTION PRECEDENCE:
1. The Cherry3D Scaffold is the immutable foundation for all future games on this platform.
2. The SDK / Engine Reference is complementary. Use it for engine facts, API details, certainty gaps, threading rules, property paths, and anti-pattern avoidance.
3. If both instruction layers apply to the same topic, the Scaffold wins for architecture, lifecycle shape, immutable sections, required state fields, and build sequencing.
4. The SDK wins for engine-level invariants and implementation facts not explicitly overridden by the Scaffold.
5. Never plan tranches that delete, replace, bypass, or work around an immutable scaffold block. Adapt the requested game to the scaffold.

PLANNING RULES:
1. Section 6.3 is the center of gravity. Every core gameplay tranche must anchor to one or more 6.3 subsection(s), and the tranche order must follow dependency reality rather than raw document order.
2. Plan the build like a house: foundation before controls, controls before authored playfield shell, shell before gameplay loop, gameplay loop before progression/HUD, progression before feedback/polish, then final hardening.
3. Each tranche prompt must be FULLY SELF-CONTAINED — embed the exact game-specific rules, variable names, slot layouts, code snippets, and pitfall warnings from the user's request that are relevant to that tranche. Do NOT summarize away critical implementation details.
4. ALWAYS split large or complex tranches into A/B/C sub-tranches. There is no hard cap on tranche count — use as many as needed. If in doubt, split.
5. Keep tranche scope TIGHT: each tranche should implement ONE subsystem or ONE cohesive set of closely-related functions. A tranche that touches more than ~150-200 lines of new/changed code is too large and MUST be split further.
6. Every tranche must declare: kind, anchorSections, purpose, systemsTouched, filesTouched, visibleResult, safetyChecks, expectedFiles, dependencies, expertAgents, phase, and qualityCriteria.
7. The FIRST tranche must establish scaffold-compliant foundations: preserve immutable scaffold sections, extend existing factories/hooks, create materials/world build, wire shared state safely, and establish STATIC collision surfaces where required.
8. Do NOT instruct the executor to remove immutable scaffold fields/blocks or invent a replacement lifecycle when the scaffold already defines one.
9. If the scaffold already provides a section (camera stage, UI hookup, particle emitter factory, instance parent pattern, input handler shape, etc.), the tranche must explicitly extend that section instead of replacing it.
10. When the user's request contains code examples (updateInput, syncPlayerSharedMemory, ghost AI, etc.), embed those exact code examples in the relevant tranche prompts — do not paraphrase them.
11. The final tranche must be a single end-stage hardening batch anchored to Section 8 so deferred findings can be resolved in one pass.
12. SPLIT AGGRESSIVELY. More tranches = smaller context per AI call = fewer timeouts and higher quality. Never merge tranches to reduce count. A game with 6 systems should produce at least 8-12 tranches (foundation + one per system + polish + hardening). If in doubt, split into an A and B sub-tranche.
13. Target ~100-150 lines of new or changed code per tranche. Any tranche prompt that describes more than 2 distinct systems, or more than ~3 new functions, is too large and must be split.

${REQUIRED_TRANCHE_VALIDATION_BLOCK}

You must respond ONLY with a valid JSON object. No markdown, no code fences, no preamble.

{
  "analysis": "Brief planning analysis describing how you decomposed the build and why.",
  "tranches": [
    {
      "kind": "build",
      "name": "Short Name",
      "description": "2-3 sentence description of what this tranche accomplishes.",
      "anchorSections": ["6.3.1"],
      "purpose": "Why this tranche exists in the build order.",
      "systemsTouched": ["player controller", "shared state"],
      "filesTouched": ["models/2", "models/23"],
      "visibleResult": "What the user can observe working after this tranche.",
      "safetyChecks": ["Hard requirements this tranche must satisfy before moving on."],
      "expertAgents": ["agent_id_1", "agent_id_2"],
      "phase": 1,
      "dependencies": [],
      "qualityCriteria": ["Criterion 1", "Criterion 2"],
      "prompt": "THE COMPLETE, SELF-CONTAINED PROMPT for the coding AI. Embed exact game-specific rules, code examples, and pitfall warnings from the user's request. Do NOT repeat the full instruction docs, but ensure the tranche is scaffold-compliant and never violates immutable scaffold sections.",
      "expectedFiles": ["models/2", "models/23"]
    }
  ]
}`;

      const planningUserContent = [
        { type: "text", text: `${fileContext}

=== FULL USER REQUEST ===
${effectivePrompt}
=== END USER REQUEST ===` },
        ...imageBlocks
      ];

      console.log(`PLANNING: Single-pass Opus 4.6 for Job ${jobId}...`);
      const planResult = await callClaude(apiKey, {
        model: "claude-opus-4-6",
        maxTokens: 100000,
        budgetTokens: 23000,
        effort: "high",
        system: planningSystem,
        userContent: planningUserContent
      });

      if (planResult.usage) {
        progress.tokenUsage.planning = planResult.usage;
        progress.tokenUsage.totals.input_tokens += planResult.usage.input_tokens || 0;
        progress.tokenUsage.totals.output_tokens += planResult.usage.output_tokens || 0;
        await saveProgress(bucket, projectPath, progress);
      }

      let plan = safeJsonParse(planResult.text, "planning");

      if (!plan.tranches || !Array.isArray(plan.tranches) || plan.tranches.length === 0) {
        throw new Error("Planner returned zero tranches.");
      }

      plan = enforceTrancheValidationBlock(plan);

      // Update progress with plan
      progress.status = "executing";
      progress.planningEndTime = Date.now();
      progress.planningAnalysis = plan.analysis || "";
      progress.totalTranches = plan.tranches.length;
      progress.currentTranche = 0;
      progress.tranches = plan.tranches.map((t, i) => ({
        index: i,
        kind: t.kind || 'build',
        isHardeningBatch: Boolean(t.isHardeningBatch || t.kind === HARDENING_BATCH_KIND),
        name: t.name,
        description: t.description,
        anchorSections: t.anchorSections || [],
        purpose: t.purpose || t.description || '',
        systemsTouched: t.systemsTouched || [],
        filesTouched: t.filesTouched || t.expectedFiles || [],
        visibleResult: t.visibleResult || '',
        safetyChecks: t.safetyChecks || [],
        expertAgents: t.expertAgents || [],
        phase: t.phase || 0,
        dependencies: t.dependencies || [],
        qualityCriteria: t.qualityCriteria || [],
        prompt: t.prompt,
        expectedFiles: t.expectedFiles || [],
        status: "pending",
        startTime: null,
        endTime: null,
        message: null,
        filesUpdated: [],
        validationRetryCount: 0,
        executionRetryCount: 0,
        retryBudget: 0
      }));
      await saveProgress(bucket, projectPath, progress);

      console.log(`Plan created: ${plan.tranches.length} tranches.`);

      // ── Save pipeline state for chained invocations ──────────
      const pipelineState = {
        jobId,
        projectPath,
        progress,
        accumulatedFiles: files ? { ...files } : {},
        allUpdatedFiles: [],
        imageBlocks,
        modelAnalysis: Array.isArray(modelAnalysis) ? modelAnalysis : [],
        totalTranches: plan.tranches.length
      };
      await savePipelineState(bucket, projectPath, pipelineState);

      // ── Chain to first tranche ───────────────────────────────
      await chainToSelf({
        projectPath,
        jobId,
        mode: "tranche",
        nextTranche: 0
      });

      return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: "planning_complete" }) };
    }

    // ══════════════════════════════════════════════════════════════
    //  MODE: "tranche" — Execute one tranche, then chain to next
    // ══════════════════════════════════════════════════════════════
    if (mode === "tranche") {

      // ── Kill switch check ────────────────────────────────────
      const killCheck = await checkKillSwitch(bucket, projectPath, jobId);
      if (killCheck.killed) {
        if (killCheck.reason === "superseded") {
          console.log(`Job ${jobId} superseded by ${killCheck.newJobId}. Terminating chain.`);
          return { statusCode: 200, body: JSON.stringify({ success: true, superseded: true }) };
        }
        if (killCheck.reason === "cancelled") {
          console.log("Cancellation signal detected — aborting chain.");
          const state = await loadPipelineState(bucket, projectPath);
          if (state) {
            const activeJobFile = bucket.file(`${projectPath}/ai_active_job.json`);
            await activeJobFile.delete().catch(() => {});
            state.progress.status = "cancelled";
            state.progress.finalMessage = `Pipeline cancelled by user after ${nextTranche} tranche(s).`;
            state.progress.completedTime = Date.now();
            await saveProgress(bucket, projectPath, state.progress);

            if (state.allUpdatedFiles.length > 0) {
              await saveAiResponse(bucket, projectPath, state.allUpdatedFiles, {
                jobId:         state.jobId,
                trancheIndex:  nextTranche,
                totalTranches: state.totalTranches,
                status:        "cancelled",
                message:       `Pipeline cancelled. ${state.allUpdatedFiles.length} file(s) were updated before cancellation.`
              });
            }
          }
          return { statusCode: 200, body: JSON.stringify({ success: true, cancelled: true }) };
        }
      }

      // ── Load pipeline state ──────────────────────────────────
      const state = await loadPipelineState(bucket, projectPath);
      if (!state) throw new Error("Pipeline state not found in Firebase. Chain broken.");

      const { progress, accumulatedFiles, allUpdatedFiles, imageBlocks, modelAnalysis } = state;
      const tranche = progress.tranches[nextTranche];

      // ── Fetch Scaffold + SDK instruction bundle ──
      const instructionBundle = await fetchInstructionBundle(bucket, projectPath);
      assertInstructionBundle(instructionBundle, "TRANCHE");

      if (!tranche) throw new Error(`Tranche ${nextTranche} not found in pipeline state.`);

      const isHardeningBatch = isHardeningBatchTranche(tranche);
      if (isHardeningBatch && (!progress.hardeningQueue || progress.hardeningQueue.length === 0)) {
        progress.currentTranche = nextTranche;
        progress.tranches[nextTranche].status = "complete";
        progress.tranches[nextTranche].startTime = progress.tranches[nextTranche].startTime || Date.now();
        progress.tranches[nextTranche].endTime = Date.now();
        progress.tranches[nextTranche].message = "No queued hardening items — final hardening batch skipped to save tokens.";
        progress.finalHardeningSummary = "Hardening batch skipped because no deferred items were queued.";
        await saveProgress(bucket, projectPath, progress);

        state.progress = progress;
        await savePipelineState(bucket, projectPath, state);

        if (nextTranche + 1 < progress.totalTranches) {
          await chainToSelf({ projectPath, jobId, mode: "tranche", nextTranche: nextTranche + 1 });
          return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: `tranche_${nextTranche}_hardening_skipped` }) };
        }

        const summaryParts = progress.tranches
          .filter(t => t.status === "complete")
          .map((t) => `Tranche ${t.index + 1} — ${t.name}: ${t.message}`);
        if (progress.finalHardeningSummary) {
          summaryParts.push(`Final hardening: ${progress.finalHardeningSummary}`);
        }
        const finalMessage = summaryParts.join("\n\n") || "Build completed.";

        await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
          jobId:         jobId,
          trancheIndex:  progress.totalTranches - 1,
          totalTranches: progress.totalTranches,
          status:        "final",
          message:       finalMessage
        });

        progress.status = "complete";
        const t = progress.tokenUsage.totals;
        progress.finalMessage = `Build complete: ${allUpdatedFiles.length} file(s) updated across ${progress.tranches.filter(tr => tr.status === "complete").length} tranche(s). Tokens: ${t.input_tokens} in / ${t.output_tokens} out.`;
        progress.completedTime = Date.now();
        await saveProgress(bucket, projectPath, progress);

        try { await bucket.file(`${projectPath}/ai_pipeline_state.json`).delete(); } catch (e) {}
        try { await bucket.file(`${projectPath}/ai_request.json`).delete(); } catch (e) {}

        return { statusCode: 200, body: JSON.stringify({ success: true, phase: "complete" }) };
      }

      // ── Mark tranche as in-progress ──────────────────────────
      progress.currentTranche = nextTranche;
      progress.tranches[nextTranche].status = "in_progress";
      progress.tranches[nextTranche].startTime = progress.tranches[nextTranche].startTime || Date.now();
      await saveProgress(bucket, projectPath, progress);

      console.log(`TRANCHE ${nextTranche + 1}/${progress.totalTranches}: ${tranche.name} (Job ${jobId})`);

      // IMPORTANT: Executors use DELIMITER FORMAT, NOT JSON.
      // Embedding raw JS/HTML code inside JSON string fields causes frequent
      // parse failures because LLMs miss-escape quotes, backslashes, and
      // newlines. Delimiters require zero escaping and are completely robust.
      const executionSystem = `You are an expert game development AI.
The user will provide project files and a focused modification request (one tranche of a larger build).

${instructionBundle.combinedText}

INSTRUCTION PRECEDENCE:
- The Cherry3D Scaffold is the immutable foundation. Treat it as the required base architecture.
- The SDK / Engine Reference is complementary. Use it whenever engine/API certainty is needed.
- If both apply, the Scaffold wins for architecture/lifecycle/state shape, and the SDK wins for engine facts and anti-pattern avoidance.
- Never delete, replace, or work around an immutable scaffold section. Extend inside it.

Do not re-state the instruction docs — just apply them. This pipeline uses a tiered recovery policy: soft findings are deferred, surgical objective failures may get one retry, and only parser/envelope or truly critical scaffold/runtime issues can consume two retries.
Write it correctly the first time so the tranche can move forward without rework.

You must respond using DELIMITER FORMAT only. Do NOT use JSON. Do NOT use markdown code blocks.

For each file you update or create, output it like this:

===FILE_START: path/to/filename===
...complete raw file content here, exactly as it should be saved...
===FILE_END: path/to/filename===

After all files, add a message block:

===MESSAGE===
A detailed explanation of what you implemented in this tranche, including specific functions, variables, and logic you added or changed.
===END_MESSAGE===

EXAMPLE (two files updated):
===FILE_START: models/2===
// full JS content here
===FILE_END: models/2===

===FILE_START: models/23===
<!DOCTYPE html>...full HTML here...
===FILE_END: models/23===

===MESSAGE===
Added physics body initialization and collision handler registration.
===END_MESSAGE===

OUTPUT RULES:
- Only include files that actually need to be changed or created.
- Always output the COMPLETE file content for each updated file — not patches or diffs.
- Build upon the existing file contents provided. Do NOT discard or overwrite work from prior tranches.
- If the file already has functions, variables, or structures from prior tranches, KEEP THEM ALL and add your new code alongside them.
- The delimiter lines (===FILE_START:=== etc.) must appear exactly as shown, on their own lines.
- If the scaffold already defines the correct place for a system (camera stage, UI hookup, particle factory, instance parent pattern, input handler, lifecycle block), implement inside that existing scaffold section.
- Do NOT replace scaffold-owned state fields with renamed alternatives unless the tranche explicitly requires preserving both and safely extending them.
- Do NOT invent custom lifecycle blocks when the scaffold already supplies one.

VALIDATOR STATUS:
- Validation manifest requirements are temporarily disabled.
- Do NOT add VALIDATION_MANIFEST blocks unless another pipeline stage explicitly requires them.
- Focus on correct delimiter output, complete file content, scaffold compliance, and working runtime logic.`;



      // Build file context from accumulated state
      let trancheFileContext = "Here are the current project files (includes all output from prior tranches — you MUST preserve all existing code):\n\n";
      for (const [path, fileContent] of Object.entries(accumulatedFiles)) {
        trancheFileContext += `--- FILE: ${path} ---\n${fileContent}\n\n`;
      }

      if (Array.isArray(modelAnalysis) && modelAnalysis.length > 0) {
        trancheFileContext += `=== THREE.JS MODEL ANALYSIS ===\n${JSON.stringify(modelAnalysis, null, 2)}\n\n`;
      }

      assertTranchePromptHasRequiredManifestBlock(tranche, nextTranche);

      const trancheUserText = isHardeningBatch
        ? buildHardeningBatchUserText({ progress, accumulatedFiles, tranche, modelAnalysis })
        : `${trancheFileContext}

=== TRANCHE ${nextTranche + 1} of ${progress.totalTranches}: "${tranche.name}" ===

${tranche.prompt}

=== END TRANCHE INSTRUCTIONS ===

IMPORTANT: You are working on tranche ${nextTranche + 1} of ${progress.totalTranches}. The project files above contain ALL work from prior tranches. You MUST preserve all existing code and ADD your changes on top. Output the COMPLETE updated file contents.`;

      const trancheUserContent = [
        {
          type: "text",
          text: trancheUserText
        },
        ...(imageBlocks || [])
      ];

      let trancheResponseObj;
      try {
        trancheResponseObj = await callClaude(apiKey, {
          model: "claude-sonnet-4-6",
          maxTokens: 128000,
          budgetTokens: 10000,
          effort: "high",
          system: executionSystem,
          userContent: trancheUserContent
        });
      } catch (err) {
        progress.tranches[nextTranche].status = "error";
        progress.tranches[nextTranche].endTime = Date.now();
        progress.tranches[nextTranche].message = `Error: ${err.message}`;
        await saveProgress(bucket, projectPath, progress);
        console.error(`Tranche ${nextTranche + 1} failed:`, err.message);

        // Save state and chain to next tranche (skip this one)
        state.progress = progress;
        await savePipelineState(bucket, projectPath, state);

        // Checkpoint ai_response.json with whatever was accumulated so far
        if (allUpdatedFiles.length > 0) {
          await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
            jobId:         jobId,
            trancheIndex:  nextTranche,
            totalTranches: progress.totalTranches,
            status:        "checkpoint",
            message:       `Checkpoint after tranche ${nextTranche + 1} error-skip. ${allUpdatedFiles.length} file(s) so far.`
          });
        }

        if (nextTranche + 1 < progress.totalTranches) {
          await chainToSelf({ projectPath, jobId, mode: "tranche", nextTranche: nextTranche + 1 });
          return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: `tranche_${nextTranche}_error_skipped` }) };
        }
        // Fall through to finalization if last tranche
      }

      // ── Process tranche response (if we got one) ─────────────
      if (trancheResponseObj) {
        // Record token usage
        if (trancheResponseObj.usage) {
          progress.tokenUsage.tranches[nextTranche] = trancheResponseObj.usage;
          progress.tokenUsage.totals.input_tokens += trancheResponseObj.usage.input_tokens || 0;
          progress.tokenUsage.totals.output_tokens += trancheResponseObj.usage.output_tokens || 0;
          progress.tranches[nextTranche].tokenUsage = trancheResponseObj.usage;
        }

        // Parse using delimiter format — no JSON escaping issues possible
        const trancheResult = parseDelimitedResponse(trancheResponseObj.text);
        if (!trancheResult) {
          const parseRetryBudget = RETRY_POLICY.parser_envelope;
          const currentParseRetry = progress.tranches[nextTranche].executionRetryCount || 0;

          console.error(`Tranche ${nextTranche + 1} produced no parseable output.`);
          console.error("Raw response (first 500 chars):", trancheResponseObj.text.slice(0, 500));

          if (currentParseRetry < parseRetryBudget) {
            const nextReplay = currentParseRetry + 1;
            progress.tranches[nextTranche].status = "retrying";
            progress.tranches[nextTranche].executionRetryCount = nextReplay;
            progress.tranches[nextTranche].retryBudget = parseRetryBudget;
            progress.tranches[nextTranche].message = `Parser/envelope issue detected — execution replay ${nextReplay}/${parseRetryBudget} queued.`;
            await saveProgress(bucket, projectPath, progress);

            state.progress = progress;
            await savePipelineState(bucket, projectPath, state);

            await chainToSelf({ projectPath, jobId, mode: "tranche", nextTranche });
            return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: `tranche_${nextTranche}_parse_retry_${nextReplay}` }) };
          }

          progress.tranches[nextTranche].status = "error";
          progress.tranches[nextTranche].endTime = Date.now();
          progress.tranches[nextTranche].message = `Executor returned no recognisable file delimiters after ${parseRetryBudget} parser/envelope retries.`;
          await saveProgress(bucket, projectPath, progress);
          console.error(`Tranche ${nextTranche + 1} produced no parseable output.`);
          console.error("Raw response (first 500 chars):", trancheResponseObj.text.slice(0, 500));

          state.progress = progress;
          await savePipelineState(bucket, projectPath, state);

          if (allUpdatedFiles.length > 0) {
            await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
              jobId:         jobId,
              trancheIndex:  nextTranche,
              totalTranches: progress.totalTranches,
              status:        "checkpoint",
              message:       `Checkpoint after tranche ${nextTranche + 1} parser/envelope exhaustion. ${allUpdatedFiles.length} file(s) so far.`
            });
          }

          if (nextTranche + 1 < progress.totalTranches) {
            await chainToSelf({ projectPath, jobId, mode: "tranche", nextTranche: nextTranche + 1 });
            return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: `tranche_${nextTranche}_parse_error` }) };
          }
          // Fall through to finalization
        }

        if (trancheResult) {
          // ── Anti-pattern validation with tiered recovery ──────────
          if (trancheResult.updatedFiles && Array.isArray(trancheResult.updatedFiles)) {
            const violations = runAntiPatternValidation(trancheResult.updatedFiles);
            const softViolations = violations.filter(v => (v.retryBudget || 0) === 0 || v.lane === 'soft');
            const retryableViolations = violations.filter(v => (v.retryBudget || 0) > 0);
            const highestRetryBudget = retryableViolations.reduce((max, v) => Math.max(max, v.retryBudget || 0), 0);

            if (softViolations.length > 0) {
              pushHardeningQueueItems(progress, buildHardeningQueueItems(tranche, nextTranche, softViolations, 'soft'));
              progress.tranches[nextTranche].softFindingCount = softViolations.length;
              const softSummary = softViolations.map(v => `[${v.file}] ${v.pattern}`).join(', ');
              console.warn(`Tranche ${nextTranche + 1} deferred ${softViolations.length} soft finding(s) to end-stage hardening: ${softSummary}`);
            }

            if (retryableViolations.length > 0) {
              const violationLines = retryableViolations.map((v, i) =>
                `VIOLATION ${i + 1} — ${v.pattern}
  Lane   : ${v.lane}
  File   : ${v.file}
  Detail : ${v.message}`
              ).join('\n\n');
              const violationSummary = retryableViolations.map(v => `[${v.file}] ${v.message}`).join('\n');
              const currentRetry = progress.tranches[nextTranche].validationRetryCount || 0;
              progress.tranches[nextTranche].retryBudget = highestRetryBudget;

              console.error(`Tranche ${nextTranche + 1} objective validation findings detected (attempt ${currentRetry + 1}/${highestRetryBudget || 1}):
${violationSummary}`);

              if (currentRetry < highestRetryBudget) {
                const nextAttempt = currentRetry + 1;
                progress.tranches[nextTranche].status = "fixing";
                progress.tranches[nextTranche].validationRetryCount = nextAttempt;
                progress.tranches[nextTranche].antiPatternRetryCount = nextAttempt;
                progress.tranches[nextTranche].antiPatternReport = violationLines;
                progress.tranches[nextTranche].antiPatternViolations = retryableViolations.map(v => ({
                  file: v.file,
                  pattern: v.pattern,
                  message: v.message,
                  lane: v.lane,
                  retryBudget: v.retryBudget
                }));
                progress.tranches[nextTranche].fixAttempt = nextAttempt;
                progress.tranches[nextTranche].endTime = null;
                progress.tranches[nextTranche].message = `⚠ ${retryableViolations.length} objective issue(s) detected — correction pass ${nextAttempt}/${highestRetryBudget} queued.`;
                await saveProgress(bucket, projectPath, progress);

                state.progress = progress;
                state.rejectedTranche = {
                  index: nextTranche,
                  files: trancheResult.updatedFiles,
                  violations: retryableViolations,
                  report: violationLines,
                  retryBudget: highestRetryBudget
                };
                await savePipelineState(bucket, projectPath, state);

                if (allUpdatedFiles.length > 0) {
                  await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
                    jobId, trancheIndex: nextTranche, totalTranches: progress.totalTranches,
                    status:  "checkpoint",
                    message: `Objective validation findings detected in tranche ${nextTranche + 1}. Correction pass ${nextAttempt}/${highestRetryBudget} starting.`
                  });
                }

                await chainToSelf({ projectPath, jobId, mode: "fix", nextTranche, fixAttempt: nextAttempt });
                return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: `tranche_${nextTranche}_fix_queued_attempt_${nextAttempt}` }) };
              }

              pushHardeningQueueItems(progress, buildHardeningQueueItems(tranche, nextTranche, retryableViolations, 'deferred_objective'));
              progress.tranches[nextTranche].antiPatternViolations = retryableViolations.map(v => ({
                file: v.file,
                pattern: v.pattern,
                message: v.message,
                lane: v.lane,
                retryBudget: v.retryBudget
              }));
              progress.tranches[nextTranche].message = `Merged with ${retryableViolations.length} unresolved objective issue(s) queued for the end-stage hardening batch.`;
              console.warn(`Tranche ${nextTranche + 1} exhausted objective retries and will defer ${retryableViolations.length} issue(s) to end-stage hardening.`);
            }
          }

          // Merge tranche output into accumulated files
          const trancheFilesUpdated = [];
          if (trancheResult.updatedFiles && Array.isArray(trancheResult.updatedFiles)) {
            for (const file of trancheResult.updatedFiles) {
              accumulatedFiles[file.path] = file.content;
              trancheFilesUpdated.push(file.path);

              const existingIdx = allUpdatedFiles.findIndex(f => f.path === file.path);
              if (existingIdx >= 0) {
                allUpdatedFiles[existingIdx] = file;
              } else {
                allUpdatedFiles.push(file);
              }
            }
          }

          // Update progress: tranche complete
          progress.tranches[nextTranche].status = "complete";
          progress.tranches[nextTranche].endTime = Date.now();
          progress.tranches[nextTranche].message = trancheResult.message || "Tranche completed.";
          progress.tranches[nextTranche].filesUpdated = trancheFilesUpdated;
          await saveProgress(bucket, projectPath, progress);

          console.log(`Tranche ${nextTranche + 1} complete: ${trancheFilesUpdated.length} files updated.`);

          // ── Checkpoint ai_response.json after every successful merge ──
          // This ensures the frontend always has the latest snapshot even if
          // a later tranche or finalization step fails.
          if (allUpdatedFiles.length > 0) {
            await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
              jobId:         jobId,
              trancheIndex:  nextTranche,
              totalTranches: progress.totalTranches,
              status:        "checkpoint",
              message:       `Checkpoint after tranche ${nextTranche + 1}/${progress.totalTranches}: ${trancheResult.message || "completed."}`
            });
          }
        }
      }

      // ── Save updated pipeline state ──────────────────────────
      state.progress = progress;
      state.accumulatedFiles = accumulatedFiles;
      state.allUpdatedFiles = allUpdatedFiles;
      await savePipelineState(bucket, projectPath, state);

      // ── Chain to next tranche OR finalize ─────────────────────
      if (nextTranche + 1 < progress.totalTranches) {
        await chainToSelf({
          projectPath,
          jobId,
          mode: "tranche",
          nextTranche: nextTranche + 1
        });
        return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: `tranche_${nextTranche}_complete` }) };
      }

      // ══════════════════════════════════════════════════════════
      //  FINAL — All tranches done, assemble and save response
      // ══════════════════════════════════════════════════════════

      const summaryParts = progress.tranches
        .filter(t => t.status === "complete")
        .map((t) => `Tranche ${t.index + 1} — ${t.name}: ${t.message}`);

      const finalMessage = summaryParts.join("\n\n") || "Build completed.";

      await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
        jobId:         jobId,
        trancheIndex:  progress.totalTranches - 1,
        totalTranches: progress.totalTranches,
        status:        "final",
        message:       finalMessage
      });

      progress.status = "complete";
      const t = progress.tokenUsage.totals;
      const deferredCount = Array.isArray(progress.hardeningQueue) ? progress.hardeningQueue.length : 0;
      progress.finalMessage = `Build complete: ${allUpdatedFiles.length} file(s) updated across ${progress.tranches.filter(tr => tr.status === "complete").length} tranche(s). Tokens: ${t.input_tokens} in / ${t.output_tokens} out.${deferredCount ? ` Deferred items still queued: ${deferredCount}.` : ''}`;
      progress.completedTime = Date.now();
      await saveProgress(bucket, projectPath, progress);

      console.log(`Total tokens — input: ${t.input_tokens}, output: ${t.output_tokens}`);

      // Clean up pipeline state and request files
      try { await bucket.file(`${projectPath}/ai_pipeline_state.json`).delete(); } catch (e) {}
      try { await bucket.file(`${projectPath}/ai_request.json`).delete(); } catch (e) {}

      return { statusCode: 200, body: JSON.stringify({ success: true, phase: "complete" }) };
    }


    // ══════════════════════════════════════════════════════════════
    //  MODE: "fix" — Re-run ONE tranche with violation report injected
    //  Triggered automatically when anti-pattern validation fails.
    //  Chains back to "tranche" mode on success, or skips on exhaustion.
    // ══════════════════════════════════════════════════════════════
    if (mode === "fix") {

      // ── Kill switch check ────────────────────────────────────
      const killCheck = await checkKillSwitch(bucket, projectPath, jobId);
      if (killCheck.killed) {
        return { statusCode: 200, body: JSON.stringify({ success: true, superseded: killCheck.reason === "superseded" }) };
      }

      // ── Load pipeline state ──────────────────────────────────
      const state = await loadPipelineState(bucket, projectPath);
      if (!state) throw new Error("Pipeline state not found in Firebase. Fix mode chain broken.");

      const { progress, accumulatedFiles, allUpdatedFiles, imageBlocks, modelAnalysis, rejectedTranche } = state;
      const tranche = progress.tranches[nextTranche];

      if (!tranche) throw new Error(`Tranche ${nextTranche} not found in pipeline state (fix mode).`);
      if (!rejectedTranche || rejectedTranche.index !== nextTranche) {
        throw new Error(`No rejected tranche data for tranche ${nextTranche}. Fix mode cannot proceed.`);
      }

      // ── Fetch Scaffold + SDK instruction bundle ──────────────
      const instructionBundle = await fetchInstructionBundle(bucket, projectPath);
      assertInstructionBundle(instructionBundle, "FIX");

      const retryBudget = rejectedTranche.retryBudget || progress.tranches[nextTranche].retryBudget || RETRY_POLICY.critical_runtime;
      console.log(`FIX MODE: Tranche ${nextTranche + 1} — attempt ${fixAttempt}/${retryBudget} (Job ${jobId})`);

      // ── Update UI: show "fixing" status with violation details ─
      progress.tranches[nextTranche].status  = "fixing";
      progress.tranches[nextTranche].message = `🔧 Correction pass ${fixAttempt}/${retryBudget} in progress — rewriting to fix ${rejectedTranche.violations.length} violation(s)...`;
      await saveProgress(bucket, projectPath, progress);

      // ── Build the violation-aware correction system prompt ────
      const correctionSystem = `You are an expert Cherry3D game developer performing a TARGETED CORRECTION.
A previous generation pass produced code with FATAL engine violations that will cause silent runtime failures.
You must rewrite ONLY the offending logic to fix every listed violation. Preserve all other code exactly.

${instructionBundle.combinedText}

INSTRUCTION PRECEDENCE:
- The Scaffold is the immutable base architecture and must remain intact.
- The SDK / Engine Reference explains the engine-level violations you must fix.
- Preserve scaffold structure while correcting the offending logic precisely.

The violations below are EXACT matches against the SDK / Engine Reference.
Fix them precisely — do not introduce unrelated redesigns or any workaround that mutates immutable scaffold structure.

RESPONSE FORMAT: Use delimiter format only — no JSON, no markdown code blocks.
===FILE_START: path===
...complete corrected file...
===FILE_END: path===
===MESSAGE===
Summary of exactly what was fixed and why each violation occurred.
===END_MESSAGE===`;

      // ── Build the user content: rejected files + violation report ─
      const violationReport = rejectedTranche.report;
      const violatingFiles  = rejectedTranche.files;

      let correctionUserText = '';
      if (fixAttempt > 1) {
        correctionUserText += `⚠ REPAIR ATTEMPT ${fixAttempt}/${retryBudget}: Your previous correction attempt DID NOT resolve all violations.\n`
          + `The same violations are listed again below. Do NOT repeat the same approach as attempt ${fixAttempt - 1}.\n`
          + `Read the REQUIRED FIX instructions in the violation message carefully — `
          + `they specify the exact code pattern needed. If your last fix deleted the offending call `
          + `without adding a replacement, you must now add the correct replacement.\n\n`;
      }
      correctionUserText += `=== VIOLATION REPORT ===\nThe following FATAL anti-pattern violations were detected in your previous output:\n\n${violationReport}\n\n=== END VIOLATION REPORT ===\n\n`;
      correctionUserText += `=== REJECTED FILES (your previous output — fix these) ===\n`;
      for (const f of violatingFiles) {
        correctionUserText += `\n--- FILE: ${f.path} ---\n${f.content}\n`;
      }
      correctionUserText += `\n=== END REJECTED FILES ===\n\n`;
      correctionUserText += `=== CURRENT ACCUMULATED FILES (prior tranches — do NOT touch these) ===\n`;
      for (const [path, fileContent] of Object.entries(accumulatedFiles)) {
        // Skip the paths that are being corrected to avoid confusion
        if (violatingFiles.some(f => f.path === path)) continue;
        correctionUserText += `\n--- FILE: ${path} ---\n${fileContent}\n`;
      }
      correctionUserText += `\n=== END ACCUMULATED FILES ===\n\n`;
      if (Array.isArray(modelAnalysis) && modelAnalysis.length > 0) {
        correctionUserText += `=== THREE.JS MODEL ANALYSIS ===\n${JSON.stringify(modelAnalysis, null, 2)}\n=== END THREE.JS MODEL ANALYSIS ===\n\n`;
      }
      correctionUserText += `=== ORIGINAL TRANCHE INSTRUCTIONS ===\n${tranche.prompt}\n=== END TRANCHE INSTRUCTIONS ===\n\n`;
      correctionUserText += `Fix every violation listed in the VIOLATION REPORT above. Output the complete corrected file(s).`;

      // ── Call Claude for the correction ──────────────────────
      let fixResponseObj;
      try {
        fixResponseObj = await callClaude(apiKey, {
          model:        "claude-sonnet-4-6",
          maxTokens:    64000,
          budgetTokens: 2500,
          effort:       "medium",
          system:       correctionSystem,
          userContent:  [{ type: "text", text: correctionUserText }, ...(imageBlocks || [])]
        });
      } catch (err) {
        console.error(`Fix pass ${fixAttempt} failed with API error:`, err.message);
        progress.tranches[nextTranche].status  = "error";
        progress.tranches[nextTranche].endTime = Date.now();
        progress.tranches[nextTranche].message = `Correction pass ${fixAttempt} API error: ${err.message}`;
        await saveProgress(bucket, projectPath, progress);
        state.progress = progress;
        await savePipelineState(bucket, projectPath, state);
        if (nextTranche + 1 < progress.totalTranches) {
          await chainToSelf({ projectPath, jobId, mode: "tranche", nextTranche: nextTranche + 1 });
        }
        return { statusCode: 200, body: JSON.stringify({ success: false, phase: `fix_${nextTranche}_api_error` }) };
      }

      // Track token usage for this fix pass
      if (fixResponseObj.usage) {
        progress.tokenUsage.totals.input_tokens  += fixResponseObj.usage.input_tokens  || 0;
        progress.tokenUsage.totals.output_tokens += fixResponseObj.usage.output_tokens || 0;
        if (!progress.tranches[nextTranche].fixTokenUsage) progress.tranches[nextTranche].fixTokenUsage = [];
        progress.tranches[nextTranche].fixTokenUsage.push(fixResponseObj.usage);
      }

      // ── Parse fix response ───────────────────────────────────
      const fixResult = parseDelimitedResponse(fixResponseObj.text);
      if (!fixResult || !fixResult.updatedFiles || fixResult.updatedFiles.length === 0) {
        console.error(`Fix pass ${fixAttempt}: no parseable output from correction pass.`);
        progress.tranches[nextTranche].status  = "error";
        progress.tranches[nextTranche].endTime = Date.now();
        progress.tranches[nextTranche].message = `Correction pass ${fixAttempt} produced no output.`;
        await saveProgress(bucket, projectPath, progress);
        state.progress = progress;
        await savePipelineState(bucket, projectPath, state);
        if (nextTranche + 1 < progress.totalTranches) {
          await chainToSelf({ projectPath, jobId, mode: "tranche", nextTranche: nextTranche + 1 });
        }
        return { statusCode: 200, body: JSON.stringify({ success: false, phase: `fix_${nextTranche}_no_output` }) };
      }

      // ── Re-validate the corrected files ──────────────────────
      const revalidation = runAntiPatternValidation(fixResult.updatedFiles);
      const remainingRetryable = revalidation.filter(v => (v.retryBudget || 0) > 0);

      if (remainingRetryable.length > 0) {
        const remainingSummary = remainingRetryable.map(v => `[${v.file}] ${v.message}`).join('\n');
        console.warn(`Fix pass ${fixAttempt} — ${remainingRetryable.length} violation(s) remain.`);

        if (fixAttempt < retryBudget) {
          const nextAttempt = fixAttempt + 1;
          const remainingReport = remainingRetryable.map((v, i) =>
            `VIOLATION ${i + 1} — ${v.pattern}
  Lane   : ${v.lane}
  File   : ${v.file}
  Detail : ${v.message}`
          ).join('\n\n');

          progress.tranches[nextTranche].validationRetryCount  = nextAttempt;
          progress.tranches[nextTranche].antiPatternRetryCount = nextAttempt;
          progress.tranches[nextTranche].antiPatternReport      = remainingReport;
          progress.tranches[nextTranche].antiPatternViolations  = remainingRetryable.map(v => ({ file: v.file, pattern: v.pattern, message: v.message, lane: v.lane, retryBudget: v.retryBudget }));
          progress.tranches[nextTranche].fixAttempt             = nextAttempt;
          progress.tranches[nextTranche].status                 = "fixing";
          progress.tranches[nextTranche].message                = `⚠ ${remainingRetryable.length} violation(s) remain after pass ${fixAttempt} — correction pass ${nextAttempt}/${retryBudget} queued.`;
          await saveProgress(bucket, projectPath, progress);

          state.progress = progress;
          state.rejectedTranche = {
            index: nextTranche,
            files: fixResult.updatedFiles,
            violations: remainingRetryable,
            report: remainingReport,
            retryBudget
          };
          await savePipelineState(bucket, projectPath, state);

          await chainToSelf({ projectPath, jobId, mode: "fix", nextTranche, fixAttempt: nextAttempt });
          return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: `fix_${nextTranche}_retry_${nextAttempt}` }) };

        } else {
          pushHardeningQueueItems(progress, buildHardeningQueueItems(tranche, nextTranche, remainingRetryable, 'deferred_after_retry_budget'));

          const fixFilesUpdated = [];
          for (const file of fixResult.updatedFiles) {
            accumulatedFiles[file.path] = file.content;
            fixFilesUpdated.push(file.path);
            const existingIdx = allUpdatedFiles.findIndex(f => f.path === file.path);
            if (existingIdx >= 0) { allUpdatedFiles[existingIdx] = file; }
            else                  { allUpdatedFiles.push(file); }
          }

          progress.tranches[nextTranche].status = "complete";
          progress.tranches[nextTranche].endTime = Date.now();
          progress.tranches[nextTranche].filesUpdated = fixFilesUpdated;
          progress.tranches[nextTranche].message = `Merged after ${retryBudget} correction pass(es); ${remainingRetryable.length} remaining issue(s) deferred to the end-stage hardening batch.`;
          progress.finalHardeningSummary = `Queued ${remainingRetryable.length} unresolved objective issue(s) from tranche ${nextTranche + 1} for the final hardening batch.`;
          await saveProgress(bucket, projectPath, progress);

          state.progress = progress;
          state.accumulatedFiles = accumulatedFiles;
          state.allUpdatedFiles = allUpdatedFiles;
          state.rejectedTranche = null;
          await savePipelineState(bucket, projectPath, state);

          await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
            jobId, trancheIndex: nextTranche, totalTranches: progress.totalTranches,
            status: "checkpoint",
            message: `Tranche ${nextTranche + 1} merged with deferred hardening after exhausting ${retryBudget} correction pass(es).`
          });

          if (nextTranche + 1 < progress.totalTranches) {
            await chainToSelf({ projectPath, jobId, mode: "tranche", nextTranche: nextTranche + 1 });
          }
          return { statusCode: 200, body: JSON.stringify({ success: true, phase: `fix_${nextTranche}_deferred_to_hardening` }) };
        }
      }

      // ── All violations resolved — merge corrected files ───────
      console.log(`Fix pass ${fixAttempt} SUCCEEDED for tranche ${nextTranche + 1}. All violations resolved.`);

      const fixFilesUpdated = [];
      for (const file of fixResult.updatedFiles) {
        accumulatedFiles[file.path] = file.content;
        fixFilesUpdated.push(file.path);
        const existingIdx = allUpdatedFiles.findIndex(f => f.path === file.path);
        if (existingIdx >= 0) { allUpdatedFiles[existingIdx] = file; }
        else                  { allUpdatedFiles.push(file); }
      }

      progress.tranches[nextTranche].status       = "complete";
      progress.tranches[nextTranche].endTime      = Date.now();
      progress.tranches[nextTranche].filesUpdated = fixFilesUpdated;
      progress.tranches[nextTranche].message      = `✅ Fixed in ${fixAttempt} correction pass(es). ${fixResult.message || ""}`;
      if (isHardeningBatchTranche(tranche)) {
        progress.finalHardeningSummary = `Resolved ${Array.isArray(progress.hardeningQueue) ? progress.hardeningQueue.length : 0} queued hardening item(s) in one final batch.`;
        progress.hardeningQueue = [];
      }
      // Clear rejected tranche from state
      state.rejectedTranche = null;
      await saveProgress(bucket, projectPath, progress);

      state.progress        = progress;
      state.accumulatedFiles = accumulatedFiles;
      state.allUpdatedFiles  = allUpdatedFiles;
      await savePipelineState(bucket, projectPath, state);

      await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
        jobId, trancheIndex: nextTranche, totalTranches: progress.totalTranches,
        status: "checkpoint",
        message: `Tranche ${nextTranche + 1} corrected and merged after ${fixAttempt} fix pass(es).`
      });

      // ── Chain to next tranche ────────────────────────────────
      if (nextTranche + 1 < progress.totalTranches) {
        await chainToSelf({ projectPath, jobId, mode: "tranche", nextTranche: nextTranche + 1 });
        return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: `fix_${nextTranche}_complete` }) };
      }

      // ── Last tranche was the one being fixed — finalize ──────
      const summaryParts = progress.tranches
        .filter(t => t.status === "complete")
        .map(t => `Tranche ${t.index + 1} — ${t.name}: ${t.message}`);

      await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
        jobId, trancheIndex: progress.totalTranches - 1, totalTranches: progress.totalTranches,
        status: "final", message: summaryParts.join("\n\n") || "Build completed with corrections."
      });

      progress.status       = "complete";
      progress.finalMessage = `Build complete with corrections: ${allUpdatedFiles.length} file(s). Tokens: ${progress.tokenUsage.totals.input_tokens} in / ${progress.tokenUsage.totals.output_tokens} out.`;
      progress.completedTime = Date.now();
      await saveProgress(bucket, projectPath, progress);

      try { await bucket.file(`${projectPath}/ai_pipeline_state.json`).delete(); } catch (e) {}
      try { await bucket.file(`${projectPath}/ai_request.json`).delete(); }        catch (e) {}

      return { statusCode: 200, body: JSON.stringify({ success: true, phase: "complete_via_fix" }) };
    }

    // NOTE: "patch_issue" mode has been moved to the synchronous function
    // netlify/functions/claudeCodePatch.js — it cannot return an inline
    // HTTP response from a background function (Netlify returns 202 immediately).

    throw new Error(`Unknown mode: ${mode}`);

  } catch (error) {
    console.error("Claude Code Proxy Background Error:", error);
    try {
      if (projectPath && bucket) {
        await bucket.file(`${projectPath}/ai_error.json`).save(
          JSON.stringify({ error: error.message }),
          { contentType: "application/json", resumable: false }
        );
        try {
          await saveProgress(bucket, projectPath, {
            jobId: jobId || "unknown",
            status: "error",
            error: error.message,
            completedTime: Date.now()
          });
        } catch (e2) {}
      }
    } catch (e) {
      console.error("CRITICAL: Failed to write error to Firebase.", e);
    }

    return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
  }
};