# Ad analysis and design regression checks

Run from the repository root after installing its existing dependencies:

```sh
node tests/adwords/run.cjs
```

The fixtures use recorded examples, in-memory persistence, and mocked providers. They do not generate paid images, publish ads, or access a live store.

Coverage includes reporting dates and currency, exact product attribution, historical sales, approval scope and publication receipts, saved-version restoration, design job ownership and recovery, provider image formats and metadata, and UI/API contracts. UI fixtures check behavior and rendered markup; they do not replace browser layout testing.

Ad Design uses `gpt-6-astra` for fresh product research and copy, and `gpt-image-2.5-sunburst` for images. Generation saves provider receipts and each completed stage before continuing. Unknown paid-request outcomes require an explicit retry; an interrupted job must reuse confirmed outputs. Spending allowances are planning reservations, reconciled with reported token usage, not provider-guaranteed price ceilings.

Publication requires review of the exact saved payload and image bytes. Existing ad changes also require an unchanged version and editable snapshot. Google Ads keeps the campaign/ad/group identity; immutable image and text assets may receive new resource IDs. Restoring a previous snapshot creates a new revision. Older incomplete snapshots remain view-only.

Responsive Google layouts are representative previews. PMax uses square, landscape and portrait marketing images; Search attaches square and landscape image assets. Search portrait is a preview only. Existing logos remain in place on ad updates. Merchant feed changes stay advisory when the source is managed by Shopify or its ownership cannot be verified.

## Uninterrupted generation and targeted fixes

Static and animated generation run to completion and are reviewed automatically. Caption fitting never stops a film: a relaxed size ladder, then a brand band beside the film, guarantees a composition, and an unsure jewelry measurement falls back to the directed product region. Every fallback is recorded as a composition note that the complete-ad review sees and the Ad ratings pop-up shows.

Each review deduction lists the rendered formats it names. The Ad ratings pop-up offers one Fix button per deduction. A fix is a new bounded job derived from the reviewed one: animated fixes re-compose captions (free), revise one message (text model, free re-composition) or regenerate exactly one film master; static fixes regenerate exactly one scene photograph or revise the copy/layout plan without new images. Unaffected scenes, films, localizations and receipts are reused byte-for-byte, the corrected set is reviewed again, and a static fix that scores at least as well as the original is applied to the ad. Fixtures cover classification, idempotent fix jobs, reuse of untouched assets and the pop-up controls.
