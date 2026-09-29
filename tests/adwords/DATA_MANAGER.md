# Google conversion migration

The live account rejects new `UploadClickConversions` integrations and requires
Google Data Manager. The default transport is now Data Manager. The dedicated
credential is deliberately separate from the existing Ads credential so granting
this scope cannot remove access needed by campaigns and reporting.

## Account setup

1. Enable **Data Manager API** in the Cloud project that owns the OAuth client.
2. Authorize an account with access to the conversion-owning Google Ads account
   for `https://www.googleapis.com/auth/datamanager`, requesting offline access.
   Use Google's supported OAuth flow; do not put tokens in chat, Git, or a URL.
3. Store its refresh token in Netlify's production Functions environment as
   `GADS_DATAMANAGER_REFRESH_TOKEN`. Existing `GADS_CLIENT_ID` and
   `GADS_CLIENT_SECRET` are reused unless the separate
   `GADS_DATAMANAGER_CLIENT_ID` and `GADS_DATAMANAGER_CLIENT_SECRET` are set.
4. Keep `GADS_CONVERSION_ACTION` as the original full
   `customers/<conversion-owner>/conversionActions/<action>` resource. If using
   manager access, retain `GADS_LOGIN_CUSTOMER_ID`. The conversion action must
   be enabled and configured for click imports with order-specific values,
   category Purchase, counting Every.
   Exactly one purchase action may be Primary (Goals → Conversions → the
   action's settings → Action optimization); two primaries count each order
   twice in bidding and ROAS. Keep the website tag Primary and this upload
   action Secondary until Sales shows uploads confirmed by Google, then swap
   them. Sales names the actions and says which step applies.
5. Redeploy after changing environment variables. Re-check Sales. Use Sync now
   to submit existing captured orders. Do not create replacement order IDs or
   change historical timestamps, values, currency, or click identifiers.

The setup requires a fresh grant; an existing Ads-only refresh token does not
gain a new scope merely because the application requests it at refresh time.

## Verification and recovery

- With dry-run enabled, the API validates requests and leaves the order queue
  unchanged. Successful validation is not an upload.
- A real ingestion response must contain `requestId`. The application saves it
  with the exact destination before reporting the order as submitted.
- The existing conversion worker checks asynchronous diagnostics after 30
  minutes, using increasing intervals up to one hour. Only `SUCCESS`, one
  processed event, no processing errors, and the exact original destination
  mark the order uploaded. This confirms processing, not campaign attribution.
- Previously exhausted orders rejected specifically for the old endpoint
  restriction are eligible for migration. Unrelated invalid-click failures are
  not silently replayed.
- A definite HTTP rejection can be retried explicitly with **Retry rejected
  requests** after correcting credentials, API access, or invalid data.
- Timeouts, 5xx responses, missing receipts, and interrupted submissions remain
  blocked for reconciliation. That button cannot replay an unknown outcome.
  Inspect Google's diagnostics and original order before any operator repair.
- Refund adjustments remain on Google's supported conversion-adjustment API.
  Their success must be verified independently from ingestion of the purchase.
- A submission still unconfirmed 24 hours later is past Google's documented
  diagnostics window. Sales reports it as stuck, with the last status Google
  returned or why the status request failed.

## What each sale sends

- **Value:** merchandise revenue, in the shop's currency: Shopify
  `subtotal_price` (after every discount, order-level codes included), without
  shipping, taxes, duties or tips; with tax-inclusive prices the line taxes come
  off. Value-based bidding then targets product revenue, so set tROAS on that
  basis. An order without a subtotal falls back to `total_price`. Google
  converts it to the account currency. The order log (Sales, product evidence)
  keeps the order total; each line's revenue is after its allocated discounts
  and adds up to the value sent.
- **Refunds:** a refund is money back on the whole order (tax and shipping
  too), so it takes the same share off the value sent: 59 back on a 118 order
  sent as 100 leaves 50. A refund recorded before the sale is sent is taken off
  it; a sale refunded in full is never sent. Later refunds become one
  adjustment per order (the latest refund's net value) once Google has recorded
  the sale. A refund with no money back (a restock or exchange) changes
  nothing. Refund amounts are converted to the shop's currency when the buyer
  paid in another.
- **Not sent:** test orders, cancelled orders, `orders/create` before payment,
  and any order without a Google click identifier.
- **Consent:** sent only when the storefront recorded it on the cart as the
  attributes `_ad_user_data` and `_ad_personalization`, with the value
  `granted` or `denied` (the shopper's Shopify Customer Privacy choice).
  Nothing is assumed. Google does not use a sale from the EEA, the UK or
  Switzerland without `ad_user_data` consent, and Sales counts such sales.

`GADS_CONVERSION_UPLOAD_API=legacy` is a rollback switch only for accounts Google
already permits to use the legacy endpoint. It does not fix this account's
observed restriction.

Focused offline checks: `node tests/adwords/data-manager.cjs`. They mock Google
and Firestore; live authorization, ingestion, and diagnostics still require
account verification.

Sources: [Google access setup](https://developers.google.com/data-manager/api/devguides/quickstart/set-up-access),
[migration field mappings](https://developers.google.com/data-manager/api/devguides/events/google-ads/offline/upgrade/field-mappings),
[event ingestion](https://developers.google.com/data-manager/api/reference/rest/v1/events/ingest),
[diagnostics](https://developers.google.com/data-manager/api/devguides/diagnostics).
