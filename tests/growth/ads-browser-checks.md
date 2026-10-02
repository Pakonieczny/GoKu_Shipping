# Sandbox Ads UI checks

Build with `node scripts/build-growth-ads-sandbox.cjs <isolated-stage-directory>`. The stage includes `/sandbox-tests/ads-workspace-browser.html`, which loads the actual navigation extension and research workspace with clearly labelled simulated responses. It contains no private catalogue or live business research.

Open the fixture page in a browser, click **Product research**, then check each response scenario: approved, partial, open product issue, unavailable, foreign product, failed request and slow request. In the slow scenario, switch products before the first response finishes; the newly selected product must remain visible. Filter with uppercase text and copy the cited brief. Approved sources must render as safe links, unknown competitor spend must stay unknown, and partial evidence must remain excluded from automatic ad design. A product with an open material issue must retain its corrective recommendations and show **Copy corrective research notes**, with a promotion/cart hold.

Save any verification screenshot outside the repository in the operator's private evidence directory. A fixture screenshot demonstrates interface behavior only. Verify live research and Google Ads separately through authenticated, read-only sandbox requests and a secure owner browser session.

The two labelled market scenarios check pooled-volume and research-language rendering. Pooled volumes must not imply country-specific demand or performance. The research-language scenario must call English a storefront-supported research hypothesis, retain unknown actual campaign language, and show a safe source link. Saved detail sampling must show omitted counts without changing the report totals.

Local checks:

```sh
node tests/growth/ads-integration.cjs
node tests/growth/ads-sandbox-stage.cjs
node tests/growth/ads-saved-workspace.cjs
node tests/growth/ads-demand-evidence.cjs
node tests/adwords/ad-design-research.cjs
```
