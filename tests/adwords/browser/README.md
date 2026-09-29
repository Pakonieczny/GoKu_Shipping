# Brites console browser walk

A real-browser click-through of `brites-adwords.html`. It serves the repository
root on `127.0.0.1`, signs in with a fake passcode, and walks every tab at
desktop (1440×900) and phone (390×844) sizes: buttons, `<details>`, selects,
inputs, lane tabs, chart marks, dialogs and popovers.

The walk exercises content first (deletes and discards after the rest), then
tabs and filters, then back/close controls. A control repeated across rows is
operated in at most two rows (`--family-cap`). Each kind of dialog is crawled
once per viewport (up to 30 actions); later openings are audited only. Inputs
are put back to their original value after each check so filters do not hide
the rest of a view.

    node tests/adwords/browser/run.cjs [--tabs=command,sales] [--viewports=desktop,phone] [--budget=100] [--family-cap=2] [--screenshots=0]

Needs Playwright with Chromium. The runner loads it from `PLAYWRIGHT_MODULE`,
the local `node_modules`, or the global npm root (`npm root -g`). It does not
install anything.

## Safety

- Every `/.netlify/functions/*` request is answered by `fixtures.cjs`. The
  fixtures are invented (synthetic names, ids, orders and amounts); nothing
  reads Firestore, Google Ads, Shopify or an AI provider.
- Every other non-localhost request is blocked and reported. Product image URLs
  get a local placeholder SVG.
- Mutating actions (budgets, status, approvals, deletes) change only the
  fixture's in-memory state. Native `confirm()` dialogs that delete or discard
  are dismissed; others are accepted so the follow-up UI is exercised. When a
  campaign's in-place budget editor (`#cmdEditIn`) opens, the walk types 17.47
  (a CAD tracer), saves, and applies its large-change confirmation.
- An action the server does not route (parsed from
  `netlify/functions/googleAdsAutopilotKick.js` `handleAction`) is reported as
  an unknown API action.

## What it reports

Console errors and page errors, unknown API actions, blocked requests, pages or
elements wider than their container, overlapping or hard-clipped text,
ellipsis without a tooltip, phone tap targets under 32 px, controls that
another element still covers in the middle of the screen, dialogs that do not
take or trap focus or ignore Escape, dialogs and expanders that appear with no
transition (a short entrance animation that has already finished when a dialog
is inspected still counts), chart marks without a tooltip or click detail, charts that do not
redraw for their width, placeholder leaks (`undefined`, `NaN`, `null`,
`[object Object]`, raw enum codes, literal `\uXXXX` escapes), native
`prompt()`/`confirm()` use, and CAD amounts printed with a bare `$` in the page
or in native dialogs (fixture CAD amounts all end in a 7-cent digit, so they can
be told apart from USD report figures). A code on the amount, its line, its
field label or its column header counts as labelled.

## Output

`tmp/adwords-browser/` (override with `BRITES_HARNESS_OUT`): `summary.txt`,
`report.json` (findings, coverage per tab, API actions answered), and
screenshots of layout findings. `HARNESS_DEBUG=1` logs every control operated.

The files here are not part of `tests/adwords/run.cjs`, which runs only the
top-level `tests/adwords/*.cjs` suites.
