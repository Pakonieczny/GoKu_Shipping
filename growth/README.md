# Shared product research and shopping concierge

## Development

Work on the isolated feature branch. `scripts/build-growth-sandbox.cjs` creates a separate source package containing only the research and concierge endpoints. It intentionally excludes unrelated application schedules and campaign mutations. Run `node --test tests/growth/*.test.cjs`, the existing Data Manager tests and relevant advertising research tests before deployment. Test actual flows in a browser; fixture tests are not proof of live integrations.

## Storage and access

`_britesGrowth.js` uses an explicitly isolated `Brites_Growth_Sandbox` namespace by default. Products, research versions, ranked queue, transactional work leases, blockers, tests, runtime usage and checkpoints are separate collections. The public API projects only published product information and reviewed symbolic interpretations. Private sales/ranking data, competitor recommendations and operational checkpoints are never committed to this public repository or returned by public endpoints.

The sandbox is `https://brites-growth-sandbox.netlify.app`, Netlify project ID `dd7556cf-ad1d-4e4d-abb2-c854cd27368e`. Operator API calls accept `X-Growth-Key` for the provisioned secret `BRITES_GROWTH_ADMIN_KEY`, or `X-Edit-Passcode` for the existing owner's passcode. Netlify secret values can be write-only; do not assume a connector can recover them or copy a redacted placeholder. An authorized controller can read the existing passcode from Firestore `config/editPasscode` using the already provisioned application credentials. Keep credentials only in private runtime memory or a protected temporary credential file; never print them, save them in a report or commit them. Browser sign-in uses secure credential handoff. Existing ad-app integration uses that app's current authentication.

Private controller state is read through `GET /api/growth/status` and `POST /api/growth/checkpoint-read`. Use `POST /api/growth/checkpoint` to preserve implementation progress, test evidence, blockers, next work and deployment identity. Private execution instructions are stored separately from public code.

## API operations

| Route | Access | Purpose |
| --- | --- | --- |
| `GET /api/growth/catalogue?q=...` | Public, rate limited | Search published products live |
| `GET /api/growth/product?handle=...` | Public, rate limited | Current product/variant details |
| `GET /api/growth/knowledge?ids=...` | Public | Approved symbolism with sources |
| `GET /api/growth/status` | Operator | Queue, progress and access blockers |
| `GET /api/growth/research?ids=...` | Operator | Product-bound research dossiers |
| `POST /api/growth/import` | Operator | Import private ranked rows |
| `POST /api/growth/match` | Operator | Bind an inspected current product without substituting a candidate |
| `POST /api/growth/claim` | Operator | Claim the next unfinished ranked entry |
| `POST /api/growth/release` | Operator | Save a match or retryable failure under its lease |
| `POST /api/growth/save` | Operator | Validate and save a dossier/version |
| `GET /api/growth/issues?ids=...` | Operator | Reviewed product conflicts and their evidence |
| `POST /api/growth/issue` | Operator | Preserve or resolve product-specific recommendation, cart or meaning holds |
| `POST /api/growth/sync` | Operator | Advance catalogue mirror by one page |
| `POST /api/growth/blocker` | Operator | Queue a specific access blocker |
| `POST /api/growth/control` | Operator | Pause/resume or configure bounded runtime inference |
| `POST /api/concierge` | Public, rate limited | Live discovery, questions and approved knowledge |

Research source schema: `{id,url,title,excerpt,checkedAt,reviewed:true}`. Factual claims require `{productId,claim,quote,sourceIds}`. Competitor records carry their own currency and explicitly unknown/disclosed/estimated spending. Recommendations use `basis:"hypothesis"` and include source IDs, a concrete action and measurement. Meanings use `kind:"interpretation"` and cultural context. Unmatched identities, unsupported quotes, absent citations and partial research cannot become approved dossiers. A partial draft cannot overwrite approved research. Completed dossiers mark every matching ranked entry complete without deleting its individual SKU sales evidence.

Product issues require exact current-product evidence, a recent review and an explicit open/resolved state. They survive catalogue refreshes in a separate collection. Public callers receive only hold booleans; diagnoses and evidence remain private. A duplicated SKU is a matching warning for review and does not automatically prohibit a shopper recommendation.

## Advertising

The existing research collector receives `sharedProductKnowledge` for exact selected products. Saved competitor offers and intent inform test hypotheses; current landing/product sources remain the only commercial fact authority. `growthResearchStatus` and `growthResearchDossiers` are protected read actions in the ad app. Its Product research view shares the same dossiers, while styling is isolated from the existing console.

## Storefront installation

Load `brites-concierge.js` with a `data-api` attribute pointing at the chosen backend. The optional launcher uses a Shadow DOM and never opens or speaks unsolicited. Customer conversation state stays in their browser session. Navigation targets verified store products; actual cart mutations use Shopify's locale-aware Ajax API in the shopper's browser and only after confirmation. Current price, currency and availability are checked again before adding. Engraving/custom-upload flows hand off to the exact product customizer until their requirements are verified. The standalone sandbox uses its own session bag and never places an order.

The guide loads an original Three.js character when opened. `brites-concierge-avatar-scene.mjs` is bundled into the local `assets` file by `node scripts/build-concierge-avatar.cjs`. It uses physical ceramic, metal and glass materials, detailed meshes and texture maps, live lighting and shadow casting. Facial and gesture states are driven by actual typing, product checks, confirmed results and optional speech events. Dismissal, offscreen state and hidden tabs pause rendering; reduced motion retains a still pose. The generated concept portrait is a loading/WebGL fallback, clearly distinct from the real mesh renderer. Check `/concierge-avatar-qa.html` in the isolated sandbox for rendering state and diagnostics. No microphone or camera is requested.

## Resumptions and costs

The hourly hosted tick refreshes catalogue information and records a heartbeat. Work-based scheduled continuation performs research and implementation from saved checkpoints, with expired leases recoverable and capped retries. Scheduled work is bounded by the stored stop date. Backend model inference is separate from Work capacity, default-off and bounded by a daily reservation/cost ledger. It never runs the bulk research queue. Preserve unknowns and failures; do not regenerate completed work simply to consume capacity.
