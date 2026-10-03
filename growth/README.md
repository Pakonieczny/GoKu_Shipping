# Shared product research and shopping concierge

## Development

Work on the isolated feature branch. `scripts/build-growth-sandbox.cjs` creates the complete separate sandbox package: research and concierge endpoints plus the existing Ads UI behind the protected read-only `/api/growth-ads` bridge. It intentionally excludes unrelated application schedules, campaign mutations, conversion uploads and paid-AI dispatch. Run `node --test tests/growth/*.test.cjs`, the existing Data Manager tests and relevant advertising research tests before deployment. Test actual flows in a browser; fixture tests are not proof of live integrations.

## Storage and access

`_britesGrowth.js` uses an explicitly isolated `Brites_Growth_Sandbox` namespace by default. Products, research versions, ranked queue, transactional work leases, blockers, tests, runtime usage and checkpoints are separate collections. The public API projects only published product information and reviewed symbolic interpretations. Private sales/ranking data, competitor recommendations and operational checkpoints are never committed to this public repository or returned by public endpoints.

The sandbox is `https://brites-growth-sandbox.netlify.app`, Netlify project ID `dd7556cf-ad1d-4e4d-abb2-c854cd27368e`. Operator API calls accept `X-Growth-Key` for the provisioned secret `BRITES_GROWTH_ADMIN_KEY`, or `X-Edit-Passcode` for the existing owner's passcode. Netlify secret values can be write-only; do not assume a connector can recover them or copy a redacted placeholder. An authorized controller can read the existing passcode from Firestore `config/editPasscode` using the already provisioned application credentials. Keep credentials only in private runtime memory or a protected temporary credential file; never print them, save them in a report or commit them. Browser sign-in uses secure credential handoff. Existing ad-app integration uses that app's current authentication.

Private controller state is read through `GET /api/growth/status` and `POST /api/growth/checkpoint-read`. Use `POST /api/growth/checkpoint` to preserve implementation progress, test evidence, blockers, next work and deployment identity. Private execution instructions are stored separately from public code.

Before writing or deploying, use authenticated `POST /api/growth/controller` with `{action:"claim",owner:<unique-run-name>,leaseMinutes:45}`. A busy result means another writer owns implementation; continue independent read-only research. Keep the returned token private. Renew it before expiry with `action:"renew"`; save the checkpoint with `owner`, `token`, `expectedUpdatedAt` from the last checkpoint read, and `value`. Active leases reject foreign writes, and revision checks reject stale snapshots. Release the lease after saving with `action:"release"`. `action:"read"` exposes only the owner/expiry, never the token. Claims and renewals respect the stored hard stop. Preserve legacy checkpoint leases during the initial transition.

## API operations

| Route | Access | Purpose |
| --- | --- | --- |
| `GET /api/growth/catalogue?q=...` | Public, rate limited | Search published products live |
| `GET /api/growth/product?handle=...` | Public, rate limited | Current product/variant details |
| `GET /api/growth/knowledge?ids=...` | Public | Approved symbolism with sources |
| `GET /api/growth/status` | Operator | Queue, progress and access blockers |
| `GET /api/growth/concierge-diagnostics` | Operator | Fixed bounded sanitized failure/timing readout; no shopper text or raw exceptions |
| `POST /api/growth/historical-lookup` | Operator | Bounded cache-only historical identity evidence; no automatic binding |
| `GET /api/growth/etsy-cache-state` | Operator | Fixed bounded existing-cache metadata; no customer, SKU or credential values |
| `POST /api/growth/receipt-sandbox-check` | Operator | Internally generated synthetic receipt storage check in the exact isolated namespace |
| `GET /api/growth/research?ids=...` | Operator | Product-bound research dossiers |
| `POST /api/growth/import` | Operator | Import private ranked rows |
| `POST /api/growth/match` | Operator | Bind an inspected current product without substituting a candidate |
| `POST /api/growth/claim` | Operator | Claim the next unfinished ranked entry |
| `POST /api/growth/release` | Operator | Save a match or retryable failure under its lease |
| `POST /api/growth/save` | Operator | Validate and save a dossier/version |
| `POST /api/growth/story-supplement` | Operator | Save a neutral, version-bound story supplement in the isolated sandbox without rewriting the base dossier |
| `POST /api/growth/milestone-index` | Operator | Rebuild the derived private milestone recall index from current approved dossier versions; accepts no caller data |
| `GET /api/growth/issues?ids=...` | Operator | Reviewed product conflicts and their evidence |
| `POST /api/growth/issue` | Operator | Preserve or resolve product-specific recommendation, cart or meaning holds |
| `GET /api/growth/demand?ids=...` | Operator | Saved, version-bound product outcomes and demand evidence |
| `POST /api/growth/demand` | Operator | Save bounded read-only evidence with provenance and explicit limits |
| `POST /api/growth/sync` | Operator | Advance catalogue mirror by one page |
| `POST /api/growth/blocker` | Operator | Queue a specific access blocker |
| `POST /api/growth/control` | Operator | Pause/resume or configure bounded runtime inference |
| `POST /api/growth/controller` | Operator | Atomically claim, renew, inspect or release an implementation lease |
| `POST /api/growth-corrections` | Operator | Save/load a validated private sandbox correction packet, or run an exact-product read-only preview/recheck; no apply action |
| `POST /api/concierge` | Public, rate limited | Live discovery, questions and approved knowledge |

Research source schema: `{id,url,title,excerpt,checkedAt,reviewed:true}`. Factual claims require `{productId,claim,quote,sourceIds}`. Competitor records carry their own currency and explicitly unknown/disclosed/estimated spending. Recommendations use `basis:"hypothesis"` and include source IDs, a concrete action and measurement. Meanings use `kind:"interpretation"` and cultural context. Unmatched identities, unsupported quotes, absent citations and partial research cannot become approved dossiers. A partial draft cannot overwrite approved research. Completed dossiers mark every matching ranked entry complete without deleting its individual SKU sales evidence.

Additive shopper stories live in a separate `StorySupplements` collection. Each record is bound to the exact current Product ID, handle, product URL and approved dossier version, accepts only reviewed neutral institutional sources, and is suppressed on version drift or any open product hold. It is merged only in memory for shopper knowledge, so the approved dossier version and its Demand evidence remain unchanged. Retailer/competitor links, retrieval metadata, ranks, sales data and author instructions are rejected.

Product issues require exact current-product evidence, a recent review and an explicit open/resolved state. They survive catalogue refreshes in a separate collection. Public callers receive only hold booleans; diagnoses and evidence remain private. A duplicated SKU is a matching warning for review and does not automatically prohibit a shopper recommendation.

Historical lookup accepts only the stored unresolved ranked identities and fixed cache collections. Use `aliasCandidates`, `listingVerification` with at most 24 numeric listing IDs, or `receiptIdentityPage` with an explicit timestamp window and at most 100 receipts per page. Continue with the returned encrypted cursor unchanged; interrupted pages preserve their transaction offset. Evidence is projected without buyer, contact, receipt identifiers or custom engraving text. Cache candidates require inspection against exact current products and variants before any separate match operation; absence from an active cache does not disprove a historic listing.

## Advertising

The existing research collector receives `sharedProductKnowledge` for exact selected products. Ingestion independently requires the current product handle and dossier version, fresh reviewed citations, hypothesis-labelled recommendations, allowlisted competitor-spend evidence, and no active recommendation or meaning hold. Approved Ads, keyword and negative recommendations are projected only as exact-version operator-review packets with deterministic candidate IDs; positive and negative terms are separated, and a conflicting term fails closed. Listing and concierge recommendations stay context-only. Every packet remains pending operator review and exposes no provider, campaign, budget or automatic-activation write. Proposal-only material, unknown fields, foreign citations and activation-like instructions fail closed. The Product research widget derives a review queue only after checking the selected queue version, exact live product, current approved dossier, fresh citations and cleared issue evidence. It displays the full research hash, candidate IDs, action, measurement and citations, with positive and negative keywords separated. Missing or stale packets stay unavailable; raw notes do not become review packets. Saved competitor offers and intent inform test hypotheses; current landing/product sources remain the only commercial fact authority. `growthResearchStatus` and `growthResearchDossiers` are protected read actions in the ad app. Its Product research view shares the same dossiers, while styling is isolated from the existing console.

The copyable research-and-demand brief accepts only fresh exact-product, current-dossier-version evidence. It retains native currencies, pooled-market labels, language hypotheses, missing values and tracking limitations. Product holds keep the export marked corrective research. Copying a brief never changes a campaign or runs inference.

The receipt reconciliation module plans status updates from trusted exact saved request diagnostics. It defaults to dry-run and blocks production queue paths. Its internal storage check seeds only clearly synthetic sandbox rows and exercises transaction integrity, idempotency, concurrency and foreign leases. These checks do not confirm real orders or apply production reconciliation. Existing Google receipts are observed separately without reuploading events. The protected sandbox `conversionActionTagEvidence` read uses a fixed bounded Purchase-action query and returns only sanitized exact event-snippet destinations. Missing snippets remain unavailable evidence, and configuration does not prove runtime dispatch, transaction identity or duplicate counting. Caller-supplied queries and write options are rejected. The protected Ads `receiptReconciliationPreview` operation fetches fresh provider receipts, pins saved-row fingerprints and rechecks row consistency in a read-only transaction. It returns hashed status-only proposals; no production apply route is exposed. The research workspace shows this preview only in the isolated sandbox.

## Storefront installation

Load `brites-concierge.js` with a `data-api` attribute pointing at the chosen backend. The optional launcher uses a Shadow DOM and never opens or speaks unsolicited. Customer conversation state stays in their browser session. Navigation targets verified store products; an explicit safe Brites product URL is rechecked as the exact selection instead of being replaced by stale cards or preferences. Positive single-product URL inquiries select knowledge without authorizing navigation or cart actions; URL path words cannot create commands. Locale product URLs retain the same checks. Missing earlier products and out-of-range ordinals fail closed without silent substitution. Actual cart mutations use Shopify's locale-aware Ajax API in the shopper's browser and only after confirmation. Current price, currency and availability are checked again before adding. Optional message telemetry is best effort; failure to save a counter must not discard a verified answer. Bounded private diagnostics record fixed stage/error categories and timings, never shopper messages, raw context, exception messages or credentials. Engraving/custom-upload flows hand off to the exact product customizer until their requirements are verified. The standalone sandbox uses its own session bag and never places an order.

The guide loads an original Three.js character when opened. `brites-concierge-avatar-scene.mjs` is bundled into the local `assets` file by `node scripts/build-concierge-avatar.cjs`. It uses physical ceramic, metal and glass materials, detailed meshes and texture maps, live lighting and shadow casting. Facial and gesture states are driven by actual typing, product checks, confirmed results and optional speech events. Failed optional script, constructor or renderer loads can retry on an explicit reopen or restored open page, with one in-flight attempt and no automatic retry loop. Dismissal, offscreen state and hidden tabs pause rendering; reduced motion retains a still pose. The original animated single-eye SVG companion is a loading/WebGL fallback, clearly distinct from the real mesh renderer. It is never accepted as proof of 3-D quality. Check `/concierge-avatar-checklist.html` and `/concierge-avatar-qa.html` in the isolated sandbox for visual acceptance and bounded JSON diagnostics covering WebGL identity, shadows, textures, expressions, gestures, frame sampling, pause and fallback state. Diagnostics return immediately for a hidden, on-demand renderer and use a bounded readiness wait for a visible pending renderer; pending state is never misreported as a WebGL failure. The export never upgrades fallback observations into visual-quality claims. `node scripts/inspect-concierge-avatar.cjs` checks the actual mesh construction without a GPU; this cannot certify rendered appearance, shadows or frame rate. The visual studio requests no microphone or camera. The separate optional voice control checks availability before requesting microphone access; voice is disabled by default.

## Resumptions and costs

The hourly hosted tick refreshes catalogue information and records a heartbeat. Work-based scheduled continuation performs research and implementation from saved checkpoints, with expired leases recoverable and capped retries. Scheduled work is bounded by the stored stop date. Backend model inference is separate from Work capacity, default-off and bounded by a daily reservation/cost ledger. It never runs the bulk research queue. Preserve unknowns and failures; do not regenerate completed work simply to consume capacity.

## Digital-eye concierge

The original single-eye robot uses indexed Three.js geometry, a deforming light aperture, articulated paddles, six procedural PBR maps and a six-face procedural studio skybox with PMREM reflections. The skybox is LDR; it is not a captured HDRI. Conversation states drive eye colour, blinking, gestures and contextual calm/celebrate poses. The old portrait is no longer loaded. An animated SVG robot is an explicitly separate 2-D fallback. Pause, reduced motion, off-screen and page visibility bounds apply to both modes.

`/concierge-avatar-qa.html` provides expression and rendering diagnostics. A WebGL-disabled browser can verify the fallback and controls, but cannot certify GPU appearance, shadows or frame rate. `scripts/inspect-concierge-avatar.cjs` builds the actual mesh on CPU only.

Realtime voice is opt-in and disabled by default. `/api/concierge-voice` supports an authenticated operator path and an explicitly enabled guest demo only inside the exact isolated sandbox namespace. The guest path uses short-lived signed one-start tokens, per-client/global start limits and a separate all-time allocation ledger; it does not enable the research or bulk-inference runtime. Keys stay server-side. A recorded two-minute deadline is dispatched to the background hangup worker; the minute reaper retries missed deadlines. Allocation reservations remain held until trusted provider usage reconciliation; they are not a certified provider charge ceiling. The native OpenAI speech-to-speech session checks the live public catalogue before discussing actual product facts. Voice tools never navigate or mutate carts; shoppers use visible option/confirmation controls. The client handles interruptions, media cleanup, actual audio RMS, streaming captions and a secondary keyboard path. Production shopper voice authorization and real microphone/GPU acceptance require a separate verified release.

The demo’s primary conversation now uses native OpenAI WebRTC audio. Browser speech synthesis is removed from the widget and is not used as a silent substitute for native speech. Optional captions track the spoken response; the separate compact product tray shows only checked catalogue results. Typing and conversation history remain available as secondary controls. The legacy `/api/concierge-demo-turn` endpoint remains for compatibility with saved work, but the widget does not call it for voice.

`/concierge-voice-qa.html` receives one short native spoken greeting after an explicit click without requesting or simulating a microphone. It stops after twenty seconds and displays connection phases, gathered-candidate counts, received-track creation, provider-audio, real RMS and server hangup evidence. Track creation alone does not prove received audio. This is a provider-output integration check, not proof of physical microphone capture, human-perceived voice quality, complete shopper dialogue or GPU appearance. Operator-only `readiness` checks the configured provider model without inference.


## Context and mannerisms

Meaning-led discovery recognizes bounded milestone contexts such as remembrance, graduation, new parenthood, relationships, achievements and seasons. Up to three fixed motif searches are retrieval hypotheses. Each implicit suggestion must retain a current, approved, source-backed public interpretation after exact live product, variant, hold, currency and budget checks. Explicit shopper symbols take precedence. If no reviewed connection survives, the guide asks one gentle question rather than inventing symbolism or showing an unrelated piece. This deterministic path does not call a paid model or claim general counselling expertise.

The optional invitation reads “Meet your gift guide.” It never opens or speaks on a fresh visit. An explicit opening triggers one brief gesture; restored session state and redundant open calls do not replay it. The robot's gaze precedes a head tilt and single paddle gesture, then settles. Interaction outcomes drive finite confirmation or reassurance motions. Remembrance suppresses celebration, including mesh deformation. Pause, visibility, offscreen and reduced-motion guards cancel gestures. These artistic timings require visual and shopper validation; animation appeal does not establish conversion improvement.

The existing ad application's private read routes expose the same exact-version review packets and saved demand evidence as the isolated bridge. They require owner authentication even when the legacy application has no passcode configured. Saved demand reads do not query providers, activate campaigns or run inference. Receipt diagnostics reject coerced one-event counts and conflicting account identities, while preserving legitimate multi-destination responses; a receipt still cannot establish storefront pixel deduplication across distinct conversion actions.

## Bounded meaning recall and shopper commands

An implicit milestone may use the private catalogue mirror only as a bounded recall index. The service scans at most 60 approved dossiers and their version-bound supplements, reads at most 36 mirror records, and live-checks no more than 12 unique handles. Recalled candidates still pass exact identity, dossier/supplement version, current product, issue-hold, stock, type, metal, budget, currency and public-meaning gates. Explicit shopper motifs bypass this path, and the implicit path never calls runtime AI. Missing approved evidence still yields one question instead of an invented association.

The authenticated sandbox can precompute those approved meaning links with an empty-body `POST /api/growth/milestone-index`. Shopper requests then read one milestone-specific private index record before the same bounded mirror and live checks, avoiding a full dossier/supplement scan on every broad occasion request. The index is derived evidence only: it cannot approve research, expose private fields, override a product hold, satisfy a stale dossier/supplement version, or replace the final live catalogue and public-meaning validation. If the index has not been built, the original bounded scan remains the compatibility fallback.

Typed navigation is resolved only against products that were already displayed and then freshly read from the live storefront. A unique exact displayed title or a valid displayed ordinal may produce a navigation request. Typed add requests produce option selection and visible confirmation; they never mutate a cart or open checkout directly. Ambiguous titles, missing ordinals, unavailable products and held products fail closed. The quick budget invitation says “$60 or less” because the item cap is inclusive.

The high avatar tier retains 2048px procedural material maps and its existing bloom option. Adaptive mode (narrow/mobile or low-memory devices) uses 1024px maps and disables bloom while retaining the articulated PBR model, shadows and fallback controls. This bounds startup allocation work without treating CPU/source inspection as proof of GPU appearance, real shadows or FPS.

## Batch 21 resilience and evidence boundaries

The browser speech bridge now aborts pending answers on interruption, suppresses stale or duplicate final transcripts, presents interim recognition text, retries bounded transient recognition failures and stops cleanly on denied microphone permission. Silence restarts listening without an alarming error. These contracts are browser/runtime tests only; they do not establish physical microphone quality, provider latency or live OpenAI use.

The shopper widget persists only an exact currently available variant across navigation or reload. It never persists a cart review or confirmation, discards stale/unavailable remembered choices, clears variant memory on Start fresh, binds navigation to the exact product handle and disables option changes during the final fresh cart check. The sandbox bag rejects malformed rows, recovers from corrupt session data and keeps at most 50 sanitized test lines. No production cart or order path was exercised by this change.

Advertising review packets now carry canonical reviewed-source bindings and a complete packet fingerprint. Each bound source includes its exact ID, title, URL, excerpt, review timestamp and source-version hash. The existing UI fails closed when a required binding is missing or no longer matches the current approved dossier. Packets remain operator-review-only and expose no provider, campaign, budget or automatic-activation write.

The offline transaction-continuity contract now requires granted consent, valid evidence chronology, exact untrimmed identity values, nonnegative finite money and a broader no-PII schema. A refused artifact emits no evidence fingerprints; hostile getters and proxies return one generic failure code. The output continues to state zero provider reads/writes, uploads, replays, cart writes and order writes. It is not live pixel, provider-receipt or cross-action deduplication proof.

Avatar QA now audits all six CPU pose contracts, DOM state/ARIA/pause/fallback behavior and both authoring and shipped scene declarations for PBR materials, environment reflections, PCF shadows, deformation and safe pause/fallback wiring. These checks deliberately label expression appearance, material appearance, real shadows, GPU rendering and frame rate unverified until a WebGL-capable browser is used.

The Batch 21 growth suite passed **1,165/1,165** across **59** test files. Research coverage and unresolved identity evidence are kept in the authenticated checkpoint. Main, the live theme, campaigns, budgets, carts, orders and conversion records were unchanged.

## Native voice and character-first interface

The robot uses matte ceramic, a dark satin visor, restrained lighting and bloom, and a separate float HDR lighting environment. Visible idle movement, a short greeting wave and continued audio-driven speech gestures remain subject to pause, offscreen and reduced-motion controls. Animation diagnostics distinguish a requested loop from actually observed frames. The production rig is exercised with an explicit synthetic renderer in regression tests; those tests do not certify GPU appearance, shadows or frame rate.

The wider interface centers the robot and one voice action. Live captions, typing and history are optional; checked product options stay in a separate tray. Connecting can be cancelled, End/Hide close local media immediately, and interrupted or timed-out catalogue checks cannot overwrite newer selections. Switching to typing ends voice. No microphone or provider call starts on a fresh visit or merely opening the guide.

All **1,356** growth checks pass across **69** files, with one complete regression run recorded privately. Native provider, physical microphone and visual acceptance are recorded separately in the private checkpoint so fixture tests cannot stand in for live proof.

The one-shot WebRTC exchange waits for ICE gathering to complete before sending the refreshed local SDP, with a ten-second limit and cancellation cleanup. It never changes browser networking or graphics settings.

Known microphone permission, unavailable-device, busy-device and media-connection failures show fixed friendly guidance. Unknown provider or account text is withheld; failure cleanup preserves the keyboard path.

## Spoken selection and reviewed advertising exports

Native voice has separate live discovery, displayed-piece inspection and prepared-control tools. Inspection returns bounded public current options in their native storefront currency; incomplete sets are labelled. A direct final shopper transcript, its exact input item and echoed client-created response metadata are required to prepare controls. At most three catalogue/action tools can chain within a spoken turn; one independent presentation cue may accompany the same turn. New speech, interruption, replaced selections and End invalidate earlier authority. A transcript-verified view request opens the exact owned product page; an exact non-personalized variant can open a visible review, and adding still requires the customer's separate Confirm click and another live price/availability check. Advice questions, historic/quoted commands, ambiguity, holds and unspecified options cannot supply action authority. The helper loads on demand without preventing the read-only voice fallback.

`/concierge-actions-qa.html` exercises the actual widget using explicitly synthetic native events and the live public catalogue. It requests no microphone and makes no provider inference call. Its results do not prove physical voice, provider latency or WebGL quality. The normal demo always uses the native adapter; the fixture is restricted to the exact isolated QA route and explicit test flag.

Copying an advertising brief re-fetches protected research and independently applies the current operator-review packet's identity, dossier version, source bindings, freshness, issue holds and keyword-conflict gates. Valid exports retain all structured positive and negative proposals in separate sections with candidate IDs, packet and dossier versions, measurements and citations. Missing, stale or held packets remain corrective context. Saved measured demand must still be fresh, exact-version evidence, with original currencies and attribution limits. Copying never changes a campaign or activates proposals.

## Semantic follow-ups and voice lifecycle

Ordinary meaning questions, including “What do these mean?” and plural “meanings,” reuse the displayed live-checked pieces and saved shopper preferences. Discourse corrections such as “I mean silver instead” still change preferences. Reply overrides retain disclosure when a foreign-currency item budget was not applied; selected actions keep their existing separate confirmation flow. No exchange rate is invented.

Voice authority is bound to the displayed selection generation at speech start as well as the exact audio input. Delayed final transcripts may preserve an in-flight read-only inspection for that same input and unchanged selection, but cannot authorize controls on later discoveries. A page restored from the browser back/forward cache keeps harmless conversation and requires a fresh explicit voice start. Callbacks from disposed loaders, media elements, channels or peers cannot affect a newer session.

The isolated `/concierge-actions-qa.html` includes explicit synthetic checks for matching ASR arriving during a real live inspection and for an old ordinal arriving during new discovery. These make no microphone or provider calls and do not certify native conversation, latency or GPU rendering.


### Conversational companion and shopping guidance

The isolated concierge now separates fast social conversation from live product search. Typed general conversation is sandbox-only, reply-only and bounded by the existing preview allocation; native OpenAI voice retains the live catalogue/action tools. Captions are off for voice by default and remain available; typing reveals the reply. Audio output has a larger bounded response allowance and one tool-free continuation for an output-limit cutoff, canceled by a new shopper turn. Recoverable turn warnings preserve the voice connection.

The original robot uses satin eye/chest materials without global bloom, purposeful product attention, audio-energy eye motion, transient appreciation hearts and a labelled exact product-photo showcase. The photo is not a fitted three-dimensional try-on. Optional Guide me movement repositions the same scene beside a visible shopping control; it never clicks, navigates or speaks from a hover. Pause, hidden state, Escape and reduced motion preserve control.

An explicit, transcript-verified view request opens the exact checked owned product page. Sandbox product navigation preserves the widget and voice connection in the same document, and current-page products are checked before becoming inspectable. Options and cart confirmation remain separate shopper choices. The verified bag link is in the visible product flow. Full-page production navigation still ends native voice; production promotion remains pending.

GPU appearance, actual microphone playback and latency need genuine hardware verification. CPU geometry/material and synthetic media tests cannot establish real render quality or speech performance.

### AI-selected expressions and finite performance

The native conversation model can select a strictly validated mood, gesture, intensity and duration, independently of catalogue/action authority. The client accepts one presentation cue only for its current committed input and issued response; interrupted, stale, hidden and tool-free speech-tail events cannot animate a later turn. Expressions never select products, click controls or change bags. Optional typed general conversation uses the same closed presentation envelope. Its provider connection retains configured gateway credentials and can fall back to the already configured dedicated concierge OpenAI credential, within the existing preview allocation.

Current public UI progress distinguishes selection, options, review, confirmed bag addition and an unsuccessful check. The model receives those bounded hints to adjust its manner. Grief/frustration keep gestures gentle, and a verified bag action owns its confirmation presentation. New speech, interruption, a new selection, pause, hidden state, dismissal and restart cancel old performance. Output-generation completion does not end buffered audio; an expression-only response may receive one bounded tool-free spoken continuation.

The original robot combines eye-first attention, small head tilts, an object-directed presentation sequence, finite near/far stance and audio-energy motion. These are informed by observed nonverbal animation principles; they are not a claim of proven sales uplift. Genuine WebGL appearance, shadows, microphone playback and latency remain separate acceptance checks.
