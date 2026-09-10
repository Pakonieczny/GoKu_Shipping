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
