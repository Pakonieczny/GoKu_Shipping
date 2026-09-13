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
   be enabled and configured for click imports with order-specific values.
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
