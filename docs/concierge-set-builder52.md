# Coordinated set drafts

`brites-concierge-set-builder.js` is the shared browser/CommonJS proposal engine. It does not fetch, write to a cart, change a product picker, open checkout, or use browser storage. Guests keep the active draft in the mounted widget's memory.

The merchant catalogue inspected for this change includes Butterfly Cutout Necklace Charm (`gid://shopify/Product/9213501046947`) and Butterfly Cutout Stud Earrings (`gid://shopify/Product/9214864425123`). Their published variants distinguish Sterling Silver, 14k Gold Filled, 14k Rose Gold Filled, 10k Solid Gold, and 14k Solid Gold. The charm also has charm-type and engraving choices. These catalogue observations establish actual choices, **not current storefront stock**: administrative inventory quantity is insufficient to prove whether a variant can be purchased.

Shopify's [complementary-product guidance](https://shopify.dev/docs/storefronts/themes/product-merchandising/recommendations/complementary-products) recommends showing a small group of complementary pieces. This engine builds two or three separately sold pieces from fresh checked catalogue data using shared explicit motif/style evidence, different product categories, and one exact material/karat finish. Product descriptions and gift reasons do not establish a matching motif. No bundle discount is invented. One currency is required; the combined item subtotal is compared to the total set budget using integer currency minor units. Shipping and tax are excluded.

## Browser integration

Exports: `parseRequest(message, {hasDraft, currency})`, `create({now, products, currency, marketKey})`, `sanitizeDraftHints(value)`, and `LIMITS`.

The instance provides `updateCatalog(products, {currency, marketKey})`, `build(request, {anchorHandle, anchorVariantId})`, `update(request, context)`, `select(rowId, variantId)`, `confirmDraft(displayedRevision)`, `snapshot()`, `review()`, `restore(hints)`, and `reset()`.

Request kinds are `build`, `swap`, `remove`, `budget`, `review-add`, `confirm`, `reset`, `save`, and `resume`. A build request can include `size:2|3`, `totalBudget`, `currency`, `material`, `motif`, `style`, `categories`, `excludedCategories`, and `excludedMotifs`. Swap/remove targets are one-based positions or an unambiguous category; an exact swap can supply `replacementHandle` and `replacementCategory`. Ordinary swaps retain prior exclusions. Quoted, private, hypothetical, declined and ambiguous commands do not authorize set work. A per-item price limit cannot silently become a set-total budget.

Each row exposes the exact product/variant IDs, public handle, option tuple and readable `optionSummary`, price/currency, finish, checked timestamp, and `choiceStatus`. Only an actual complete selected anchor supplied by the host starts confirmed. Matching rows start proposed. One explicit **Confirm these exact choices** control may confirm all currently displayed exact tuples. A revision mismatch, changed price/tuple, hold, unavailable stock, stale read, or changed market prevents confirmation/add readiness. Hosts must capture the displayed revision before asynchronous rechecking so assent to an older display cannot confirm changed choices.

Products must have a safe merchant URL, complete unique variant IDs and option tuples, an exact available variant, a price valid for that currency, and a check completed within five minutes. Unknown availability, all holds, incomplete choices, private customizers and personalization-dependent choices are withheld. An ordinary separately sold charm may retain its checked charm category; it is never presented as a finished necklace or pair of earrings. Published engraving `No` can be proposed; engraving `Yes` requires the existing dedicated personalization flow.

The host performs all fresh reads before the first cart write and uses its established guarded add/readback paths. The [Shopify Ajax cart reference](https://shopify.dev/docs/api/ajax/reference/cart) documents cases where stock errors can still add available quantities. A multi-piece operation must therefore report verified partial success honestly and must not silently remove or compensate for pieces already added.

## Existing Firebase persistence

POST `/api/concierge-memory` with the existing Firebase ID token in `Authorization: Bearer …`. New actions require `consent:true` and the current archive `generation`:

| Action | Additional fields | Result |
| --- | --- | --- |
| `draft-read` | Optional exact `id` | Current `version`, bounded `drafts:[{draft,version,updatedAt}]` |
| `draft-save` | `draft`, integer `expectedVersion` | Sanitized historical `draft`, new `version`, `updatedAt`, `syncAfterMs:60000` |
| `draft-clear` | Exact `id`, integer `expectedVersion` | `cleared:true`, new `version` |

`expectedVersion` is the **customer-generation draft collection version**, not a per-document counter. Read before a mutation and use the returned version. Saves and clears increment it transactionally; stale versions return HTTP409 with `conflictRequired:true`. This prevents old tabs from recreating a deleted set without storing an unbounded tombstone list. Archive generation changes return HTTP409 with `resetRequired:true`. A new generation begins at version0.

Records are under the existing server-derived customer scope in `Brites_Growth_{Sandbox|Live}_ConciergeCustomers/{sha256(firebaseUid)}/SetDrafts_{generation}/{sha256(draftId)}`. The browser's uid/email/owner fields are ignored. At most five active drafts are retained; a sixth is refused until one is explicitly forgotten. Draft writes have a separate account-wide one-minute debounce. Exact duplicate saves at the current version do not rewrite or increment it. Existing shared daily limits reserve conservative Firestore **estimates**, not measured billing. Full **Forget saved history** rotates the generation immediately and physically cleans bounded old set records alongside archive cleanup.

Saved content is an allowlist of IDs, handles, quantity1, draft revision, current-market/currency hints, and bounded shopping constraints. It discards prices, stock, confirmed status, actions, product text/images/URLs, personal context, engraving and other private fields. `restore()` exposes `needs_revalidation`, null prices/timestamps and proposed rows. A completed exact fresh catalogue read is necessary before showing a current subtotal; explicit confirmation remains necessary afterward.

Existing environment names remain `FIREBASE_PROJECT_ID`, `FIREBASE_CLIENT_EMAIL`, `FIREBASE_PRIVATE_KEY`, and optional `BRITES_GROWTH_NAMESPACE`. This change introduces no auth provider, credential, dependency, paid model call, or external account write.

The server preference allowlist additionally accepts explicit delivery settings only: `voicePace=normal|slow|brisk`, `voiceDetail=brief|expanded`, and `suggestions=ask|welcome|off`. Inferred emotion is not persisted.

## Validation

New focused suites are `tests/growth/set-builder52.test.cjs` and `tests/growth/set-drafts52-server.test.cjs`; the independent matrix is `tests/growth/avatar-experience52-adversarial.test.cjs`. They cover exact-option/overall-budget arithmetic, stock and hold boundaries, asynchronous script order, confirmation revisions, stale clocks and market changes, untrusted restoration, consent/owner/version isolation, parallel writes, read/reset races, capacity, physical deletion, privacy, and estimated budget limits. Test execution and deployment are centrally supervised by the parent agent; no result is asserted here before that runner completes.
