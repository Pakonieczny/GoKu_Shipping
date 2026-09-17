# Google API connections — what is verified, and what is missing

Every Google surface this application depends on is catalogued in
`netlify/functions/_googleConnections.js` and probed by
`netlify/functions/googleConnectionsCheck.js`.

Run it against the live account:

```
https://goldenspike.app/.netlify/functions/googleConnectionsCheck          # HTML
https://goldenspike.app/.netlify/functions/googleConnectionsCheck?format=json
https://goldenspike.app/.netlify/functions/googleConnectionsCheck?write=1  # adds the validateOnly write probe
```

If `EDIT_PASSCODE` is set, add `?key=<EDIT_PASSCODE>`. The default run is
read-only: every probe is a `SELECT` or a `GET`. The write probe is opt-in and
uses `validateOnly`, which Google documents as non-mutating.

## What a green row means

A green row means Google answered that exact request with these exact
credentials. It does not mean an ad is serving. API reach, policy approval,
delivery and billing are four separate states, and this checker only proves the
first. A grey row is a probe that was never attempted because a credential was
missing; it is never counted as a pass.

## Coverage

| Area | Probed | In use | Available, not wired |
|---|---|---|---|
| Google Ads reporting resources | 37 | 32 | 5 |
| Merchant Center sub-APIs | 14 | 5 | 9 |
| Google hosts | 10 | 9 | 1 |
| OAuth scopes | 5 | — | — |
| Schema fields introspected | 12 | — | — |

`tests/adwords/google-connections.cjs` scans `netlify/functions` for every GAQL
`FROM`, every `*.googleapis.com` host and every OAuth scope, and fails if any of
them is absent from the catalog. A new Google connection therefore cannot ship
without a probe covering it.

## How a statistic is tied to a shape

Google reports a metric against an *asset*, never against an aspect ratio. The
ratio only exists if the asset's own pixel dimensions are requested and carried
through. The delivery report now selects
`asset.image_asset.full_size.width_pixels` and `height_pixels`, and every metric
row — both the totals rows and the per-device rows — carries `width`, `height`,
`ratio`, `shape` and `orientation` alongside `device`.

`brites-ad-format-policy.js` holds Google's published requirements as the single
record: the creative engine, the publication gate and the connections checker all
read the same numbers, so a report cannot disagree with what is enforced. An
asset Google returns without dimensions is left explicitly unshaped rather than
guessed from its field type.

### Device segmentation is not available everywhere

`segments.device` cannot be selected with every resource. Rather than assume,
the checker asks `GoogleAdsFieldService` which resources allow it in the
configured API version and reports a row per resource. Where Google does not
allow a mobile/desktop split, the honest answer is to report that statistic at a
level that does support it, not to estimate one.

## Merchant Center health

`googleMerchantHealth` answers what the connections check only proves reachable:

```
https://goldenspike.app/.netlify/functions/googleMerchantHealth
https://goldenspike.app/.netlify/functions/googleMerchantHealth?format=json
```

It reports account issues separated into blocking and advisory, **the exact
offer IDs that cannot serve and why**, whether Google's own record of the
account relationships names the advertising account, whether an active
conversion source exists for the store, shipping and return-policy coverage,
feed ownership, promotions and automatic crawling.

A section that could not be read is recorded as unavailable with its reason and
never counted as healthy — an unread question stays open rather than becoming a
clean bill of health.

## YouTube video state — optional

Nothing in the ad pipeline needs this. Films are uploaded through Google Ads'
own resumable endpoint; Google Ads reports the upload state in
`you_tube_video_upload.state` and whether the asset may serve in
`asset_group_asset.primary_status`. Animated ads generate, upload, attach,
publish and report without a YouTube credential.

`_youtubeVideos.js` adds only the YouTube side that Google Ads does not
mirror: a Content ID rejection, a privacy change made in YouTube Studio, an
unembeddable flag, and the exact transcode state. It is read by
`googleConnectionsCheck` and by nothing else.

Set `config/googleApiKeys.youtubeApiKey` to switch that row from "optional,
not configured" to a real probe. Leave it unset and the row stays grey rather
than warning, because an unconfigured optional capability is not a defect.

## Google Ads capabilities still available but not wired up

| Resource | What it would add |
|---|---|
| `asset_field_type_view` | Results grouped by the slot an asset filled (`MARKETING_IMAGE`, `SQUARE_MARKETING_IMAGE`, `PORTRAIT_MARKETING_IMAGE`, `YOUTUBE_VIDEO`, `LOGO`) — the closest Google gets to native per-shape reporting. |
| `asset_group_top_combination_view` | Which asset combinations Google actually assembled and served together. |
| `detail_placement_view` | The exact YouTube channels, videos, apps and sites the ads appeared on. |
| `campaign_asset_set` | Asset sets (business locations, page feeds) bound to a campaign. |
| `product_link` | The Google Ads to Merchant Center link, read from the Ads side. |

These are probed, so the report already says whether the credentials reach them.

## API version

`GADS_API_VERSION` selects the Google Ads API version and the code default is
**v24**, which Google supports until roughly May 2027. This is a deliberate
choice to stay on a proven version, not an oversight.

The checker still probes the versions Google serves and introspects **every
field the catalog queries** — all of them, not a sample — against the configured
version and each other served version. A readiness row therefore says either
"all N queried fields exist in vNN" or names the exact fields that would break.
Move only on a green readiness row, and only when there is a reason to move.

## Creative requirements checked against Google's published specs

| Field type | Ratio | Minimum | Recommended | Count |
|---|---|---|---|---|
| `MARKETING_IMAGE` | 1.91:1 | 600 × 314 | 1200 × 628 | 1–20, required |
| `SQUARE_MARKETING_IMAGE` | 1:1 | 300 × 300 | 1200 × 1200 | 1–20, required |
| `PORTRAIT_MARKETING_IMAGE` | 4:5 | 480 × 600 | 960 × 1200 | 0–20, optional |
| `LOGO` | 1:1 | 128 × 128 | 1200 × 1200 | 1–5, required |
| `LANDSCAPE_LOGO` | 4:1 | 512 × 128 | 1200 × 300 | 0–20, optional |

Video masters are produced in 16:9, 1:1 and 9:16 at ten seconds. Omitting the
9:16 master removes vertical inventory — Shorts and vertical feeds — entirely,
so the checker treats a missing vertical master as a failure rather than a
preference.

Sources are recorded in `brites-ad-format-policy.js`.

## Where credentials live

Netlify environment variables are a limited resource on this site, so Google
**API keys** live in Firestore at `config/googleApiKeys`:

```
config/googleApiKeys  →  { "youtubeApiKey": "AIza..." }
```

An environment variable of the same upper-snake name still wins when set, so
nothing already configured changes. A key stored here is only as safe as the
restrictions set on it in Cloud Console: restrict every key to the single API it
serves. Genuinely secret values — OAuth refresh tokens, service-account keys —
do not belong in this document and stay in their own records.

An API key authenticates **only** APIs that accept one. It does not cover
Merchant API, which requires OAuth user consent (`GMC_REFRESH_TOKEN` carrying
`https://www.googleapis.com/auth/content`), nor the Google Ads API, which
requires OAuth plus a developer token.

## What must be enabled in Google Cloud Console

These cannot be done from code. In the Cloud project that owns the OAuth client:

| API to enable | Credential it uses |
|---|---|
| Google Ads API | OAuth refresh token + developer token (already configured) |
| Merchant API | OAuth refresh token, scope `.../auth/content` |
| YouTube Data API v3 | API key in `config/googleApiKeys.youtubeApiKey` |
| Data Manager API | its own sealed record |

The connections check names each missing piece, so the report is the checklist.

## Shopify attribution — the half that lives outside Google

Two things decide whether a paid sale is ever credited to the click that earned
it, and both live outside this repository:

```
https://goldenspike.app/.netlify/functions/shopifyAttributionCheck
```

**The click-id snippet.** `snippets/brites-gclid-capture.liquid` is maintained
in the Shopify theme, at the store owner's request, not here. The version this
application expects is `EXPECTED_SNIPPET_VERSION` in
`netlify/functions/_shopifyAttribution.js`; raise it whenever a new snippet is
issued, so an older installed copy is reported rather than passing as present.
The snippet must be present in the **published** theme AND rendered by
`layout/theme.liquid` with `{% render 'brites-gclid-capture' %}`. Present but
unrendered attributes nothing.

Reading the theme requires the `read_themes` scope on the custom app. Without
it the check reports that section as unavailable — an open question, never a
pass.

### What v2 of the snippet fixed

Each of these lost click ids silently. They are recorded here because the file
is no longer in this repository to carry its own history.

| Defect | Consequence |
|---|---|
| `fetch()` resolves on 4xx, and the response status was never checked, so a refused `/cart/update.js` was recorded as a success | the click id was retired for the rest of the session |
| the once-per-session flag was a boolean rather than the id that was synced | a second ad click never reached the cart, and the sale was credited to the first click's campaign |
| that flag never cleared | an id that failed to apply was never retried |

v2 checks `response.ok`, remembers *which* id was synced, and carries a
`brites-gclid-capture/2` marker so the installed version is visible.

**The webhooks.** `orders/paid` and `refunds/create` are registered manually.
Without the first nothing uploads; without the second refunds never retract and
reported ROAS stays inflated. Shopify's `webhooks.json` lists only the webhooks
the querying app owns, so one registered by another app or in the admin is
invisible here — an empty list is not evidence that orders are not arriving.
Orders reaching the conversion queue are stronger evidence and outrank it.

**The queue.** The check also reports sales the store recorded that Google
refused. Those never appear in campaign metrics, ROAS or the daily charts. A
refusal records Google's `errorCode` first, because its accompanying message is
usually the generic "There was a problem with the request."
