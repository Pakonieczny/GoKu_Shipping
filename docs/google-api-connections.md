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

## Google Ads capabilities that are available but not wired up

| Resource | What it would add |
|---|---|
| `asset_field_type_view` | Results grouped by the slot an asset filled (`MARKETING_IMAGE`, `SQUARE_MARKETING_IMAGE`, `PORTRAIT_MARKETING_IMAGE`, `YOUTUBE_VIDEO`, `LOGO`) — the closest Google gets to native per-shape reporting. |
| `asset_group_top_combination_view` | Which asset combinations Google actually assembled and served together. |
| `detail_placement_view` | The exact YouTube channels, videos, apps and sites the ads appeared on. |
| `campaign_asset_set` | Asset sets (business locations, page feeds) bound to a campaign. |
| `product_link` | The Google Ads ↔ Merchant Center link itself, read from the Ads side. |

## Merchant Center capabilities that are available but not wired up

| Sub-API | What it would add |
|---|---|
| `conversionSources` | Google's record of which site sends Merchant conversions — the Shopify conversion link. |
| `accountRelationships`, `accountServices` | Which Google Ads accounts this Merchant account is joined to, and under what service agreement. |
| `accountIssues`, per-offer `item_issues` | Account and offer blockers. A disapproved offer silently earns nothing. |
| `promotions` | Sale messaging on Shopping surfaces. |
| `onlineReturnPolicies`, `shippingSettings` | Settings that decide whether an offer can serve at all. |
| `autofeedSettings` | Whether Google is crawling the Shopify store directly as a feed source. |

These are probed, so the report already says whether the credentials would reach
them today. Wiring each one into the product is separate work.

## Known gap that needs a credential

**YouTube Data API.** Videos are uploaded through Google Ads' resumable endpoint
and their processing state is read back from `you_tube_video_upload`, so a film
that failed to transcode is visible. What is *not* available is the YouTube side:
public metadata, thumbnails and YouTube-native analytics. There is no credential
for it at all. To enable it, grant this OAuth client
`https://www.googleapis.com/auth/youtube.readonly` and store the refresh token;
the checker reports the scope as missing until then.

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
