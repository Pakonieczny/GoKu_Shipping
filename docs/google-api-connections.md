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

## YouTube video state

Google Ads reports that a video asset is attached and serving. It does not
report that YouTube rejected the video for a copyright claim, never finished
transcoding it, or made it unembeddable — and an ad carrying such a video earns
nothing while still looking healthy in Ads.

`_youtubeVideos.js` reads that state for every `YOUTUBE_VIDEO` asset on the
account and reports each film as serviceable or not, with the reason. Because
the uploads are created UNLISTED, a plain **API key** resolves them; no OAuth
consent is needed. Set `YOUTUBE_API_KEY` and the connections check switches
from a warning to a real probe.

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

`GADS_API_VERSION` selects the Google Ads API version. The code default is now
**v25**; an environment variable of the same name overrides it, and reverting is
that one variable.

Google serves several versions at once and sunsets each roughly a year after
release. The checker probes the configured version and its neighbours, and its
schema section introspects **every field the catalog queries** — all of them, not
a sample — against the configured version and against each other served version.
A readiness row therefore says either "all N queried fields exist in vNN, safe to
move" or names the exact fields that would break. Move only on a green readiness
row.

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

## What must be enabled in Google Cloud Console

These cannot be done from code. In the Cloud project that owns the OAuth client:

| API to enable | Needed for |
|---|---|
| Google Ads API | already in use |
| Merchant API | Merchant Center products, reports and health |
| YouTube Data API v3 | video processing state (`YOUTUBE_API_KEY`) |
| Data Manager API | offline conversion upload |

Credentials to add:

- **`YOUTUBE_API_KEY`** — an API key restricted to YouTube Data API v3. No OAuth
  consent is required because the uploads are unlisted.
- **`GMC_REFRESH_TOKEN`** — a refresh token carrying
  `https://www.googleapis.com/auth/content`, if Merchant calls are to run under
  their own credential rather than the Ads one.

The connections check reports each of these as missing until they are set, so
the report is the checklist.
