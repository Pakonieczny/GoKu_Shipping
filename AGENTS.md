# Investor maintenance and releases

These instructions apply to `investor.html`, `investor/**`, and the investor-related files in `netlify/functions/`. This repository also hosts other applications; do not change them as part of investor maintenance.

## Source and scope

- Start from the latest `Pakonieczny/GoKu_Shipping` `main`. Fetch it and check for existing user changes before editing. Do not substitute an old ZIP or an earlier conversation checkout.
- Make only the material changes needed for the requested investor task. Preserve filenames and the existing application structure.
- Preserve simulations, shared Firestore research, saved AI answers, checkpoints, trades, and previously incurred charges. Never reset data or purchase replacement AI answers merely to get around a recoverable error.

## Standard release route

1. Implement the requested changes and run focused checks that exercise the affected behavior. Verify the exact inline-script CSP hash whenever `investor.html` JavaScript changes.
2. Inspect the diff and publish only the changed, necessary investor files to `main`, using a non-force update. The user wants authorized investor fixes completed and pushed without repeated permission requests. Follow any narrower instruction in the active conversation.
3. Let the existing Git-connected Netlify deployment handle the release. Do not create a new site, upload a full source ZIP, switch to a manual/API/CLI deployment, or alter the release route unless the user explicitly requests that exception. An exception in one conversation is not the default for future releases.
4. Do not change `package.json`, lockfiles, `.npmrc`, `netlify.toml`, build commands, environment variables, or unrelated applications for an investor code fix. A deployment-configuration repair requires a specific user request covering that work; diagnose the concrete failure first.
5. Confirm the remote commit and report its link. If deployment status is accessible, check it. Distinguish pushed, building, published, and live-functionally-verified. Fixture tests do not prove live simulations complete.
6. If automatic approval review rejects an action, do not bypass it. Finish the reviewable local work, explain the exact rejection, and request only the authorization that remains necessary.

Instruction-only commits may use `[skip netlify]` to avoid an unnecessary application build. Keep deployment instructions in this file so future sessions do not depend on chat memory.

## Known deployment issue (2026-09-06)

- Git-triggered deployment of investor commit `46a96df91449ce8a469e43dadfa8b8e985ed4e77` failed during npm dependency installation with E401 for `@metavrse-inc/metavrse-lib`.
- That package is used by the Cherry viewer. It is not an investor dependency and must not be removed to force an investor deploy through.
- The existing `.npmrc` references `METAVRSE_NPM_TOKEN`. Do not assume a variable named `GITHUB_TOKEN` is an interchangeable fix, expose secrets, or enumerate unrelated credentials.
- A previously successful production deploy was an uploaded build. That does not establish that Git-triggered builds have working package authentication.
- Restoring reliable Git-triggered deployment remains a separate configuration task until it is verified. Do not claim these instructions alone fix E401, and do not repeat upload fallbacks as the standard release process.
