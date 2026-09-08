# Investor maintenance and releases — read before making changes

This is the canonical, persistent procedure for Investor work in `Pakonieczny/GoKu_Shipping`. Read it at the start of every new conversation or agent session involving this application. Do not reconstruct the release procedure from chat memory. The user's current instructions take precedence.

Scope: `investor.html`, `investor/**`, and investor-related files in `netlify/functions/`. The repository and Netlify site also host other applications; preserve them.

## Standing user preference

Paul wants the same working release process in every conversation: make focused investor changes, push only the changed necessary files to GitHub `main`, and let Netlify automatically deploy that commit. Complete authorized fixes and releases without repeatedly asking for permission. Never silently switch deployment routes. A request for review only does not authorize a release.

Only changed files means only those files change in the repository and deployed application. Netlify may require a complete source upload and rebuild function bundles; a seven-file source ZIP is NOT a safe replacement for this shared site.

## Fixed destination

- Repository: `Pakonieczny/GoKu_Shipping`
- Branch: `main`; never force-push
- Netlify project: `gokushipping`
- Netlify site ID: `a66782c3-88dd-424a-adf4-1c17d252983a`
- Production URL: `https://goldenspike.app`
- Investor page: `https://goldenspike.app/investor.html`
- Use the existing site. Do not create a replacement site.

## Required release sequence

1. Fetch current `main`, read this file, and check for existing user changes. Use a clean checkout when necessary. Never start from an old uploaded ZIP or assume a previous session's scratch directory still exists.
2. Implement only the material requested changes. Preserve filenames, the existing UI conventions, and application structure. Preserve shared Firestore research, simulations, checkpoints, saved AI responses, trades, and all incurred charges. Never reset data or purchase replacement answers merely to bypass a recoverable failure.
3. Run focused checks of the affected behavior. When changing inline JavaScript or CSS in `investor.html`, recompute each changed block’s exact SHA-256 CSP hash. Update `script-src` and `style-src` independently; never globally replace all hash tokens. Verify JavaScript syntax, every script hash against `script-src`, and every style hash against `style-src`. A matching JavaScript hash alone does not validate the stylesheet. Verify the deployed page actually applies its stylesheet in a browser when available.
4. Review the changed-file list. Commit only the necessary changed files to `main` using a non-force update. Do not add `[skip netlify]` or `[skip ci]` to release commits: the preferred route is GitHub `main` → automatic Netlify production deployment. Recheck the remote head immediately before the non-force update. Confirm that Netlify creates a deployment for the exact new commit; a successful Git push alone is not proof of deployment. If the current application commit is already published, avoid an unnecessary trigger commit unless validating or correcting the release workflow.
5. Confirm the remote commit/tree and inspect the Git-triggered deployment. Continue with steps 7–9 when it starts. If it does not start or fails, diagnose that specific result; do not repeatedly push trigger commits. The source-upload fallback in step 6 is authorized when needed, but report the automatic-deployment failure separately rather than claiming that the fallback repaired it. For that fallback, prepare a clean deployment staging directory from that exact committed application source. Include the complete site's required assets, functions, and existing configuration, preserving unrelated applications. Compare against the existing deployment source when available; do not publish a partial directory. Exclude repository internals, secrets, local environment files, temporary logs, prior deploy archives, and development dependencies. Do not copy credentials into source. Preserve `.npmrc`'s environment-variable reference.
6. For the source-upload fallback only, use the connected Netlify plugin's `deploy-site` operation for the fixed site ID to obtain fresh upload instructions. The proven route is the connector-issued source ZIP upload to Netlify's build service. Execute its advertised `@netlify/mcp` command from the clean staging directory. Its command has this shape:

   `npx -y @netlify/mcp@latest --site-id a66782c3-88dd-424a-adf4-1c17d252983a --proxy-path "<fresh URL supplied by the Netlify connector>" --no-wait`

   Follow the current connector instructions if the advertised syntax changes. Never save the temporary proxy URL/token in this file, Git, a report, or a reusable script. Do not substitute a different site or deployment service. The upload starts a Netlify build; it is not proof that dependency installation is unnecessary.
7. Capture the returned deploy ID. Poll that specific deployment with Netlify's `get-deploy` reader until `ready` with `published_at`, or a terminal error. Share concise progress during the wait. Do not treat upload acceptance, `building`, or `processing` as a completed release.
8. Verify that the live investor HTML matches the committed file, and that relevant function bundles were updated. Confirm the existing site remains populated. Do not start paid simulations or reset user data as a deployment check.
9. Report the Git commit, actual deployment outcome, and any live verification limits. Passing fixture tests does not prove live simulations complete.

If a capability is unavailable or automatic approval review rejects an action, finish the safe local work, state the precise blocker, and request only what is necessary. Never bypass an approval rejection or repeatedly retry an unchanged failing deployment.

## Do not change the deployment setup during ordinary investor fixes

Do not edit `package.json`, lockfiles, `.npmrc`, `netlify.toml`, build commands, environment variables, or unrelated applications to force an investor release through. Diagnose a concrete deployment failure before proposing a separate configuration repair. Keep Git-triggered deployment as the preferred route, with source upload as the documented fallback. Do not reintroduce the removed private Metavrse dependency or registry configuration during Investor work.

`AGENTS.md` instructs agents; it is not executable Netlify configuration. Do not delete application code, runtime prompt Markdown files, or Netlify configuration to make it the only file in the repository. Keep this as the single release-instruction document rather than creating competing handoffs.

## Current release evidence — 2026-09-08

- Paul explicitly restored GitHub `main` → automatic Netlify deployment as the preferred release route on 2026-09-08. This supersedes the former mandatory skip-tag/upload instructions below.
- Commit `05dac84703610d1410c838a5e5e44253699455d8` removed `@metavrse-inc/metavrse-lib` from root `package.json` and deleted `.npmrc`. The current tree has no package lockfile or other package manifest retaining that private dependency. Do not recreate `.npmrc` merely because the fallback instructions discuss preserving existing configuration.
- The Cherry viewer's Vite source still depends on that library for a separate viewer build; the dependency removal does not prove `npm run build` works. Do not enable or change build commands during an Investor release.
- Production deploy `6aa051294e1d2727257dd146` published commit `7768debb651ec2fed1f3ea14842c1408b71dfb6b` at `2026-09-08T18:21:44.190Z`, with 247 functions. The live Investor HTML matches the committed Git blob `47c414023ff09b59050c12c57b0ee592e4135ee4`. This deployment reports `deploy_source: api` and a source ZIP, so it proves current-source publication, not automatic Git-trigger execution.

## Historical evidence — 2026-09-06 (superseded where noted above)

- Git-triggered deployment of commit `46a96df91449ce8a469e43dadfa8b8e985ed4e77` failed during dependency installation with E401 for `@metavrse-inc/metavrse-lib`.
- At that time the package was used by the Cherry viewer, not Investor, and `.npmrc` referenced `METAVRSE_NPM_TOKEN`. Both the dependency declaration and registry file were subsequently removed by commit `05dac84`; do not enumerate unrelated secrets.
- The same seven investor changes succeeded through the source-upload route in deploy `6a9ddbf6fd65e30166763514`, published at `2026-09-06T21:37:14.329Z`.
- Netlify reported seven changed static files and 241 deployed functions, preserving 34 redirects and three header rules. The live `investor.html` matched the verified source byte-for-byte, and the investor manager/API function bundle digests changed.
- That historical upload did not repair Git-triggered package authentication. The later removal eliminated the private dependency rather than repairing its credentials. Inspect the actual deployment result every time.
