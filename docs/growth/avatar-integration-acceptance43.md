# Avatar integration: independent adversarial acceptance

This is the review contract for the five user requirements of October 8, 2026 and the continued Website Avatar Integration work. Every row needs evidence. A passing synthetic test is evidence about code behavior, not a substitute for a browser, a microphone, rendering hardware, a live cart, or human judgment.

Baseline inspected: published sandbox commit `e12bf592`. This review is independent of the implementation owners. The old recovered checkout was stale; its already-fixed cross-product option and material/budget issues are retained only as regression scenarios.

## Evidence and completion rules

- **Code**: named production functions were exercised with counterexamples, not just a string/source assertion.
- **Fixture**: actual host, bridge, and widget share a DOM and checked synthetic catalogue. Only the network, avatar hardware, and optional voice provider are replaced. Product/variant identities, names, option labels, availability, and prices intentionally differ from earlier acceptance fixtures.
- **Browser**: an observed screen and action receipt on the exact staging/published commit. Record initial state, shopper request, resulting page/control/selection/cart, and any remaining failure. A source inspection or invisible event does not prove a visible menu.
- **Native voice**: actual permission, microphone input, speech recognition, current-turn tool call, audible output, interruption, and cleanup. Text injected into a voice fixture is not microphone evidence.
- **Visual**: captured production face rendered through the real rig, with representative timed thinking, idle, listening, answering, unsure, and delighted states. A human evaluates friendliness and legibility; tests can establish finite animation and bounds only.
- **Live commerce**: fresh native Shopify variant, line key, market currency, stock, and shop result. The isolated sandbox is a simulation; its cart and checkout can establish the complete mock workflow but cannot establish live order/payment behavior.
- A result is **verified**, **failed**, or **unverified**. Do not call a row complete because an implementation exists. A material limitation must remain visible in the final report.
- Reject a sequence after its first unsupported or failed action. Report completed steps honestly, then stop remaining actions. A late fetch cannot revive superseded context or a cancelled turn.
- Review claims against the final commit and deployment identity. All screenshot/latency evidence must specify that identity. Never imply that successful Git push or build output proves publication.

## 1. Friendly, continuous thinking and emotional delivery

| ID | Detailed scenario and acceptance | Evidence required |
|---|---|---|
| F01 | Compare the reported unpleasant thinking pose with the replacement at full size and actual widget size. Eyes and brows convey calm curiosity; no oversized static grimace, staring disk, or hostile eyebrow silhouette. | Visual and human review |
| F02 | A request with a deliberately slow catalogue read enters thought promptly, shows a gentle finite blink/gaze/brow progression, and remains recognizably attentive throughout 0–12 seconds. The same expression cannot remain frozen for the full wait. | Timed rig test and browser recording |
| F03 | Thought animation ends promptly when a result arrives; eyes and face move into appropriate explanation or choice-question cues while actual speech drives the mouth. | Rig transitions and browser |
| F04 | A miss, unavailable finish, or unsure shopper produces restrained reflection and useful recovery, then settles. No broad celebration for a miss or an indefinite worried pose. | Fixture and visual |
| F05 | Discovery success, liked selection, factual explanation, and a genuine completed mock milestone have distinct but subtle reactions. No repetitive distracting mood carousel during silence. | Rig trajectories and visual |
| F06 | Supportive/solemn contexts retain restrained intensity; a memorial gift never triggers an enthusiastic sales celebration. | Planner counterexamples and visual |
| F07 | Cancelling, new input, hidden tab, playback block, session close, and interruption retire old thinking/speech cues. A late old response cannot animate the new turn. | Fixture and browser interruption |
| F08 | Reduced motion and WebGL failure provide a readable friendly static face, usable shopping controls, and clear state. | Fixture plus browser fallback |
| F09 | Listening, thinking, speaking, and muted/blocked states remain distinguishable at actual size without relying only on color. Screen-reader controls remain labelled. | Browser and accessibility inspection |
| F10 | Whole-page cursor tracking remains smooth across all directions; pointer movement does not request inference, reset thought, or steal focus. | Browser and request count |

## 2. Complete discovery and natural language

| ID | Detailed scenario and acceptance | Evidence required |
|---|---|---|
| D01 | On home, collection, and an open product, “What do you have?” and “What kinds of jewelry do you sell?” enumerate actual supported jewellery categories and offer a useful next choice. These must not answer only the open product’s variants. | Actual widget fixture and browser |
| D02 | “Do you have butterfly earrings?”, “Show me butterflies,” and “Any butterfly studs?” find checked butterfly earrings. Necklace rows, promotional cross-sell descriptions, and incidental substring matches cannot establish the motif/type. | Planner counterexamples, host, browser |
| D03 | “What animal earrings do you have?” covers real animal taxonomy across bunny/rabbit, elephant, whale, bee/butterfly and other included inventory motifs. Exclude plant/celestial/abstract motifs and necklaces. Explicit taxonomy must be inspectable. | Planner fixture and browser sampled identities |
| D04 | “No, I meant butterfly earrings instead” is corrective discovery, not cancellation. “Don’t show butterfly earrings” must not execute positive butterfly discovery. “Not those, show animal earrings” changes the subject truthfully. | Intent matrix and actual widget |
| D05 | Short followups “more like that,” “just earrings,” “anything cheaper?”, “what about gold filled?”, and “not those” preserve or replace the right theme/type/preferences. A new full request resets obsolete restrictions. | Multi-turn fixture and browser |
| D06 | Synonyms/plurals such as bunnies/rabbit, butterflies/butterfly, hoops/huggies, and sterling/silver resolve consistently without overriding an exact named title. | Planner counterexamples |
| D07 | A request for earrings never uses a pendant descriptor to turn an actual finished earring into a necklace. Explicit product type, approved store category, and exact option axes are respected. | Host type isolation fixture |
| D08 | “Gold filled butterfly earrings under USD 60” requires one available exact variant satisfying BOTH material and price. A cheap silver variant and expensive gold variant cannot jointly fake a match. Solid gold and gold plated cannot be conflated with gold filled. | Variant counterexamples and browser facts |
| D09 | Currency remains explicit. USD, CAD, and mixed-currency products never receive unsupported conversion or a single combined comparison/subtotal. | Fixture and browser |
| D10 | No match produces a qualified checked-inventory result, preserves recovery controls, and does not claim absence from the complete live shop when only the test inventory is known. | Host and browser empty search |
| D11 | All test-inventory identities and detailed facts preload with an honest readiness/count state. Partial/failed pages and TTL expiry cannot produce a false “entire inventory ready” promise. | Inventory tests, live page counts |
| D12 | Once warm, standard discovery/current facts/control routing avoids unnecessary model or per-product fetches. Measure acknowledgement/render latency in the browser; distinguish cached operations from network stock/price checks. | Request-count fixture and browser timing |
| D13 | Paging/loading/sorting follow actual returned cursors and completed discovery versions. Late old responses cannot replace a newer theme/category. | Deferred host fixture |
| D14 | Search and recommendations preserve complete option groups and exact variant identities for a returned product; filtering never invents or drops purchasable choices. | Host fixture and browser |

## 3. Helpful selection, bag and checkout walkthrough

| ID | Detailed scenario and acceptance | Evidence required |
|---|---|---|
| P01 | “Take me to my cart” / “Open my bag” work from home, collection, product, and checkout. Empty bag remains actionable and context identifies bag, not a stale product. | Actual widget and browser |
| P02 | “Help me choose this” begins a concise useful walkthrough using the current piece. One relevant next question/choice at a time, no generic “choose options” dead end. | Widget flow and browser |
| P03 | The guide opens the actual relevant published dropdown/menu visibly, identifies available choices and explains a grounded tradeoff. It does not silently choose an option because it recommended it. | Browser menu and selection snapshot |
| P04 | One literal chosen material updates the actual host control, preserves other selections, and reflects the correct price. Length/hoop size and all subsequent required axes behave likewise. | Host/bridge fixture and browser |
| P05 | “Choose Gold Filled for Butterfly Earrings” while Daisy Necklace is open never changes Daisy. Ambiguous identical titles, unknown pieces, unknown option values, unavailable combinations, and negated/hypothetical requests fail clearly without mutation. | Adversarial bridge fixture |
| P06 | Practical aliases such as “18-inch chain” map only to one published length; it cannot infer an unavailable size or resolve two distinct matching values silently. | Host and bridge fixture |
| P07 | Guide suggestions adapt to budget, material preference, jewellery type, and stock; facts such as durability/care retain their qualification and provenance. | Multi-turn fixture and cited copy |
| P08 | “Add it to my cart” with missing options reveals the NEXT actual required choice and explains why. Selected choices remain intact; no false added confirmation. | Actual widget and browser |
| P09 | An explicit “Add it to my cart” with complete exact options and quantity performs the actual host addition after a current stock/price/hold check. It cannot claim success for a prepared review. A separate explicit review request retains visible shopper confirmation. The guide acknowledges the host result and gently offers cart/continue/checkout help. | Widget/host fixture and browser |
| P10 | Double click, duplicate tool event, or replayed command cannot double-add. Fresh requested quantity is respected; exact product/variant/quantity/price/currency are visible in the bag. | Fixture plus browser |
| P11 | Price/stock/hold/selected-option change before direct addition or between review and confirmation invalidates the old authority. The guide offers a useful recovery and never adds a substituted variant. | Deferred host fixture |
| P12 | Quantity change/removal uses stable current line identity; two lines with the same product title but different options cannot be confused. Undo restores the exact latest reversible change. | Host/bridge fixture and browser |
| P13 | Bag persists during ordinary navigation/back/forward/current-session reload according to its existing contract. Discovery or recommendations never clear it or overwrite chosen product controls. | Host and browser |
| P14 | Mock checkout checks exact current bag choices, exposes review/shipping/gift/confirmation stages, accepts only labelled demonstration shipping, and completes only after visible acknowledgement and click. | Host fixture and browser complete flow |
| P15 | Every mock stage remains clearly labelled; no contact/address/payment request, order submission, reserved stock, real quote, discount eligibility, or live purchase claim is invented. Real Shopify checkout uses verified shopper handoff only. | Host request ledger and browser copy |
| P16 | Private engraving/note content updates only the requested visible enabled field. Literal “then open my cart” in the text remains data; content does not enter chat history, telemetry, snapshots, or model context. | Existing private-field fixtures |
| P17 | Explicit “stop,” “not now,” new subject, interruption, or shopper manual navigation cancels pending followup. Guide does not repeatedly open menus or restart a dismissed walkthrough. | Multi-turn and deferred fixture, browser |
| P18 | Desktop and narrow layouts keep menus/price/add/review/cart visible and interactive beside the avatar. No overlay, horizontal overflow, focus loss, or jump hides the selection. | Browser at desktop and narrow width |

## 4. Persistent actual-page and product awareness

| ID | Detailed scenario and acceptance | Evidence required |
|---|---|---|
| A01 | Manually open a listing without asking the guide, then ask “What am I looking at?”, “What size is it?”, “How much is this?”, and “What metals can I choose?” The exact current product answers without “choose a listing.” | Actual widget and browser manual-navigation sequence |
| A02 | Product size/dimensions come only from exact checked product copy/structured facts/option groups. Charm diameter, hoop size, chain length, and quantity are distinguished. Unknown dimensions are stated as unknown. | Conflicting-size counterexamples and browser |
| A03 | Current selected variant and quantity control the price answer, including unit price versus subtotal; an unselected product retains a truthful range. | Host/widget fixture |
| A04 | Details include full checked description, all safe image views, available options, and approved qualified stories. Missing research cannot become invented historical symbolism. | Product facts and story fixtures |
| A05 | Product ID/handle/URL mismatch, duplicated identity, stale detail, unavailable variant or active holds cannot become verified detail or cart authority. | Inventory/facts/host counterexamples |
| A06 | Pointer hover, keyboard focus, actual visible cards, open detail, current gallery, bag, and checkout appear in local awareness with revision. The current open product outranks a stale hover in an unrelated region. | Host fixture and browser |
| A07 | User/manual back, forward, reload and guide navigation update awareness indefinitely. Old selected/hovered widget cards cannot restore the wrong product or overwrite the actual page. | Repeated host cycle and browser |
| A08 | Delayed facts/options/recommendations are bound to current product and discovery revision. Switch to another piece before completion: no old claim, viewport movement, or selection occurs. | Deferred fixture and browser interruption |
| A09 | Direct DOM controls and guide commands agree on selected options/quantity/cart; updates remain visible to the next native and typed turn. | Shared-host fixture and browser |
| A10 | Highlight/scroll/zoom/gallery requests address the actual current section/image. “Second image” is distinguished from “second product”; closing a viewer restores current selection. | Host/bridge fixture and browser |
| A11 | Context is updated locally without inference on every mousemove, and native voice receives fresh bounded context at the committed response. | Request-count and native fixture |
| A12 | Context includes public product facts/identities needed for understanding and excludes private notes/credentials/admin data. A product’s own prompt-like text cannot authorize actions. | Sanitization and adversarial fixture |

## 5. Continuous helpful recommendations and sales navigation

| ID | Detailed scenario and acceptance | Evidence required |
|---|---|---|
| R01 | While a listing is open, related alternatives/set companions are prepared quietly from the checked test inventory. Current detail, selection, scroll/focus and bag remain unchanged. No unsolicited model run on pointer motion. | Actual host fixture and browser |
| R02 | Alternatives share meaningful theme/material/category or explain a concrete differing fit. Matching sets may cross jewellery types deliberately. Exclude current identity, holds, unavailable variants, mismatched currencies/budgets and unrelated motifs. | Counterexample recommendation fixture |
| R03 | “I’m not sure,” “Something smaller?”, “A matching necklace?”, “Anything cheaper?” produce one or a few grounded helpful candidates and preserve relevant conversation restrictions. | Multi-turn widget and browser |
| R04 | Recommendations show exact native currency, relevant available variant and why it fits; no “best seller,” sales statistic, delivery promise or cultural claim without approved source evidence. | Projection fixture and browser facts |
| R05 | Similar-product preparation finishes before the shopper requests alternatives when cached; deferred old-product preparation cannot publish under a new product. | Request-count/deferred fixture |
| R06 | The guide makes a gentle offer after a meaningful decision/milestone, accepts “no thanks,” and waits for engagement. No timers that repeatedly interrupt, no automatically changed options or bag additions. | Multi-turn fixture and browser |
| R07 | Research of sales/navigation techniques uses primary published UX/conversational guidance. Implementation links each adopted behavior to a concrete requirement and does not claim unsupported conversion gains. | Cited research review |
| R08 | All previous requirements apply equally to typed and native tool paths. A genuine native end-to-end conversation covers discovery, manual product awareness, options, review/add, bag and mock checkout. Any unavailable microphone check remains explicitly unverified. | Native voice/browser evidence |
| R09 | The shared versioned adapter advertises verified current-theme controls only. Migration instructions identify actual Shopify selectors and unsupported capabilities; no sandbox checkout/gift behavior is assumed to work in production. | Adapter tests and current-theme browser |
| R10 | Final review references exact commit, test totals and deployed browser observations. Failed and unverified rows remain visible; no broad “everything works” statement based only on fixtures. | Final independent audit |

## Adversarial review log

- Latest baseline rejects cross-product option setters and applies same-variant material/budget filtering. Earlier recovered-tree failures are not current blockers.
- The published baseline could eventually find correct butterfly earrings through remote fallback on home, according to the root agent's browser observation. That did not establish fast local discovery or correct routing from an open unrelated listing. The independent real-DOM fixture reproduced broad inventory/current-facts confusion, motif availability delegation, and corrective “No” cancellation. The revised shared local router passes those direct counterexamples on the current working tree.
- Existing test harnesses cover many strict protocol and stale-authority controls well. Some host integration tests stub `execute`/`presentProducts`, so their success cannot prove the real menu/cart/checkout. New adversarial scenarios mount the real DOM host and bridge, and selected scenarios the real widget.
- Native microphone, live Shopify cart, and subjective friendliness require separate observed evidence. Fixture avatar sinks and synthetic voice signals do not certify them.

| Independent counterexample | Required correction | Review scope |
|---|---|---|
| A listener's full-intensity inquiry survives into thinking and restores crooked/asymmetric eyes while disabling wait cues. | Apply restrained thinking bounds after semantic expression blending; continue finite attentive waiting alongside the listener cue. | Direct pose and both actual controller paths; rendered aesthetic remains separate |
| A cheap silver variant makes an expensive or unavailable gold-filled variant appear within budget. | Require one exact known-available variant to satisfy material, currency and price together. | Catalogue, host and recommendation fixtures |
| A numeric budget with no currency ranks cheap CAD against the currently viewed USD listing. | Anchor the assumption to the current native currency and disclose it; do not convert. | Recommendation counterexample |
| “Do they come in silver?” or “Show me their sizes” becomes a global query, or refuses the already open product. | Treat current-page pronouns as exact current facts while retaining positive type discovery such as “What silver earrings do you have?” | Actual mounted widget and shared classifier |
| Discovery runs before controls and swallows “What should I choose?” or gallery commands. | Preserve guidance/gallery/control scope and only give semantic discovery its own requests. | Actual bridge and awareness regression suite |
| “Use Sterling Silver for the other earrings” changes the current listing. | Refuse unresolved other-product scope before choosing a current option. | Actual mounted host/bridge/widget |
| Broad 10-item inventory presentation compares a six-card projection to the full collection and invalidates itself. | Present the projected selection consistently and describe only the actual categories in the checked inventory. | Independent 10-identity fixture, distinct from the 120-item test catalogue |
| “A matching necklace?” offers an unrelated jewellery type, while comparison UI drops actual prices. | Enforce the named matching category and show native from-price, exact relevant option and factual reason. | Actual widget with a cheaper same-motif charm trap |
| “Anything cheaper?” performs a literal “cheaper” search or compares another material/currency. | Compare a strictly lower available unit price with the current material/currency, and preserve the open page. | Actual widget with silver/gold-filled/USD/CAD traps |
| Script/style/template/comment measurements become visible-product size evidence. | Remove hidden markup content before extracting and comparing literal published measurements. | Guide source-fidelity regression |
| A typed request ID such as `typed-1` reaches a cart routine that requires a longer mutation ID. | Keep exact shopper authority separate from a cryptographically generated cart mutation ID. | Actual production adapter, actual widget and cart routine with synthetic transport |
| A “review-add” authority token is accepted by the direct-add handler. | Bind the minted token to its exact operation; both cross-operation directions must fail before reads or cart mutation. | Actual production adapter/widget hostile hook test |
| Native context trimming drops a selected literal outside the first 60 values. | Retain exact selected values and their consistent variant facts while budgeting unselected data. | Native context byte-limit counterexample |
| A gifting-context heuristic sends a private birthday gift-note setter to the model and saves its literal in chat history. | Dispatch the opaque enabled field setter before rich-gift deferral; keep recipient, occasion and command-looking words entirely in the private field. | Actual host/bridge/widget, local field, request body, public snapshot, action receipt and history inspections |
| “Show animal necklaces” followed by “Just earrings” produces an empty grid because `just` is a motif token and the animal theme is lost. | Recognize short type refinement, replace the prior type, and retain the applicable theme and variant restrictions. | Fixed and independently passing actual host/bridge/widget regression, including a prior USD cap |
| A subsequent “What about gold filled?” unnecessarily falls back to the model and leaves the prior unfiltered collection unchanged. | Apply shared material refinement to collection discovery while retaining current-product material questions as factual reads. | Fixed; expanded actual multi-turn D05 regression preserves theme, native currency and cap, and exact current-product facts remain read-only |
| Collection “More like that”/“Anything cheaper?”/“Not those”, and current-product “More like that”/“Not those”, unnecessarily fall back to remote inference. | Give collection-specific grounded clarification without inventing a single comparison reference; use exact current-product alternatives and exclude actually rejected suggested identities. | Fixed; all five batched typed counterexamples and all five actual synthetic realtime parity cases pass |
| Upstream `normalizeProduct` strips template/noscript tags but publishes their hidden inner measurements, defeating downstream visible-description filtering. | Remove invisible block contents before public plain-text extraction, then preserve exact identity/options/price and visible measurements. | Fixed; actual normalizeProduct → host/bridge/widget size question contains only visible 11 mm/7 mm; nested/unterminated invisible-block backend regressions also pass |
| Signed or imprecise lengths are stripped into an actual published `18 inch` choice. | Reject signed/imprecise length text before normalization and restrict generic material aliases to actual material groups. | Fixed; all 12 old length cases plus independent ASCII/Unicode minus, plus sign, quote, decimal and disjunction cases pass |

## Requirement-to-evidence index

The files below are candidate behavioral evidence. A file name alone does not establish a passing result; the final run and browser ledger must be recorded. Existing GPU or voice QA files are harnesses, not observations of working hardware.

| Requirement IDs | Behavioral evidence to run | Additional acceptance still needed |
|---|---|---|
| F01–F10 | `avatar-thinking43`, `avatar-expression28`, `expression-listening28`, `expression-response33`, `avatar-gaze30`, `avatar-performance17`, expression/voice lifecycle suites | Actual-size face sequence, pointer behavior, accessibility, reduced-motion/fallback browser; human/GPU judgment |
| D01–D10, D12, D14 | `adversarial-shopping43`, `catalogue-intents43`, `catalogue-backend43`, `storefront-purchase43`, catalogue discovery/type/material/currency suites | Live categories/motifs, empty recovery, variants and browser warm timings |
| D11, D13 | `inventory-preload38`, `inventory-reliability39`, `storefront-discovery-revision33`, `catalogue-seed34` | Published 120-identity readiness and delayed-search observation |
| P01–P15, P17 | `adversarial-shopping43`, `shopper-journey43`, `storefront-purchase43`, `storefront-awareness43`, `shopping-voice43`, bag/length/reversal/failure/Shopify cart suites | Actual visible menus, exact native controls, cart edits, persistence, complete labelled mock checkout |
| P16, A12 | `storefront-language39`, `storefront-bag-scope39`, `storefront-authority34`, voice authority/context and product-facts suites | Private-field browser inspection if exercised; no real private data in fixtures |
| P18, F09 | `concierge-layout-qa39`, `layout27-widget`, `storefront-layout34`, image-viewer/layout suites | Desktop and narrow viewport geometry, focus, overflow and reachable controls |
| A01–A05 | `adversarial-shopping43`, `shopper-journey43`, `product-facts38`, `product-understanding38`, `navigation-knowledge38`, `storefront-awareness43`, current-inspect/holds suites | Manual published listing entry and exact sourced facts/selected prices |
| A06–A11 | `storefront-awareness43`, `adversarial-shopping43`, discovery/current-inspect/gallery/reversal/native-control/fast-storefront suites | Repeated actual navigation, pointer/keyboard precedence, stale cancellation and native context |
| R01–R06 | `concierge-shopping-guide43`, `adversarial-shopping43`, `shopper-journey43` | Published suggestion cards/buttons, actual page preservation, factual price/why, dismissal and comparison navigation |
| R07 | `shopper-flow-research43.md`; primary pages independently opened and checked on 2026-10-08 | Design inferences stay labelled; no unsupported conversion claims |
| R08 | Independent synthetic realtime full host/bridge/widget journey; `shopping-voice43` and native authority/lifecycle suites | Physical microphone, real speech recognition, audible responses and interruption |
| R09 | `storefront-awareness43`, `shopping-voice43`, production adapter/native commerce/cart tests | Actual current Shopify theme/install/browser and real cart/handoff evidence |
| R10 | This 64-row matrix, final test output, committed source/build identity and root browser ledger | Published commit/deploy identity and honest separation of failed/unverified items |

## Individual reconciliation of all 64 requirements

The following records code/fixture evidence separately from complete acceptance. “Passing fixtures” means the named behavioral counterexamples are exercised in the independent targeted run or the root full growth run whose output was inspected. It does not mean the visible, physical or live acceptance in the final column is satisfied. Browser evidence is deliberately left open until the final source is published and observed.

| ID | Source and behavioral reconciliation | Remaining acceptance |
|---|---|---|
| F01 | Thinking-only softness bounds pass every semantic expression kind and retained inquiry at multiple intensities. | Human review of actual rig at widget/full size |
| F02 | `avatar-thinking43` exercises four finite request-bound wait phases and blinks; semantic cues do not freeze them. | Timed rendered wait sequence |
| F03 | Thinking result/listening/error/idle transitions retire the request clock and old performance. | Visible answering and actual speech transition |
| F04 | Expression miss/reflection and unavailable-option fixtures keep restrained cues and recovery. | Rendered unsure/miss sequence |
| F05 | Success/explanation/listening/wait trajectories are distinct; silence does not start a performance loop. | Human legibility/distraction judgment |
| F06 | `expression-response33` memorial/support counterexamples constrain celebration intensity. | Rendered solemn context |
| F07 | Voice/expression lifecycle suites invalidate old turns, hidden/pause/close and late asset/audio callbacks. | Actual browser interruption |
| F08 | Reduced-motion and fallback controller fixtures retire motion and keep usable host controls. | Real WebGL failure/reduced-motion surface |
| F09 | Existing accessibility/state/layout fixtures retain labelled controls and finite states. | Screen-reader/focus and actual-size state inspection |
| F10 | `avatar-gaze30` pointer authority and local request-count fixtures pass. | Smooth whole-page cursor tracking |
| D01 | Independent actual widget passes broad questions on collection and product, accurate categories and no phantom rings. | Published broad/category copy |
| D02 | Butterfly discovery from unrelated detail and realtime transcript uses exact checked earring identities without remote reads. | Published sample identities/latency |
| D03 | Animal fixture includes rabbit, elephant, bee, butterfly and whale; excludes plant/celestial/type traps. | Published taxonomy examples |
| D04 | Corrective “No” discovery and negative commands are exercised separately, without unintended cancellation/mutation. | Published correction sequence |
| D05 | All five listed followups pass batched current-product/collection fixtures plus synthetic realtime parity; type/material/USD-cap refinement and explicit full-request replacement preserve the intended scope. | Browser multi-turn sequences |
| D06 | Shared vocabulary tests cover motif plurals, material construction and exact named identity precedence. | Exact title/synonym browser samples |
| D07 | Category tests distinguish jewellery type, body cross-sells, finished earrings and components. | Published category/type checks |
| D08 | One exact variant must satisfy availability, material and USD cap; cheap silver/expensive gold trap passes. | Published option/price facts |
| D09 | USD/CAD counterexamples exclude numeric cross-currency comparisons and label unlabelled-budget assumptions. | Visible currency/bag totals |
| D10 | Catalogue status tests distinguish no-match, unavailable, unconfirmed, held, stale and incomplete sample. | Visible no-match recovery and sample qualification |
| D11 | `inventory-preload38`/`inventory-reliability39` exercise 120 checked identities, partial/failure/retry/TTL and manifest consistency. | Published honest readiness/count state |
| D12 | Independent warm discovery/current facts/options/suggestions use no unnecessary model/product reads; fresh add rechecks separately. | Browser acknowledgement/render timings |
| D13 | Discovery revision/deferred fixtures reject superseded results and actual cursor/loading violations. | Browser delayed/paged observation |
| D14 | Exact returned options and selected values survive local projection and native context byte budget. | Published option/menu completeness |
| P01 | Actual typed and synthetic realtime flows reach current bag from product and checkout; current context retires old product. | Published empty/filled bag navigation |
| P02 | Actual “Help me choose this” opens next current group with concise priced material help. | Visible helpful progression |
| P03 | Actual host menu state is opened with zero implicit selections; material chips carry exact current target. | Native visible dropdown and provenance links |
| P04 | Actual two/three-axis journeys preserve other choices, exact variant and subtotal; manual controls and guide agree. | Visible selection/price feedback |
| P05 | Cross-product, unknown, ambiguous gold-filled, negated and target-scoped CTA counterexamples refuse mutation. | Published refusal/recovery |
| P06 | Unique published length/material aliases pass; malformed-length correction independently passes the original 12-case suite and additional real-context signed/decimal/disjunction cases. | Native theme/browser choice verification |
| P07 | Guide requires exact available material/budget; construction facts are general FTC/CCI-attributed advice. | Visible qualified advice and preferences |
| P08 | Missing-option direct add opens actual next choice, retains chosen material and does not add. | Published/native missing-choice flow |
| P09 | Actual sandbox and real production adapter with synthetic transport add exact complete authorized selection; explicit Review remains separate. | Published add receipt; real Shopify cart evidence |
| P10 | Duplicate request/click/token replay passes once-only exact add and quantity checks. | Published cart quantity/receipt |
| P11 | Fresh identity/options/stock/price/currency/hold drift refuses addition; stale review is withdrawn without substitution. | Published stale-option recovery |
| P12 | Stable line IDs distinguish same-title variants, support quantity/remove and repeated reversible undo. | Browser line editing/undo |
| P13 | Actual host navigation/bag fixtures retain cart and choices across routes; session persistence contract remains local. | Browser reload/back/forward persistence |
| P14 | Actual widget and synthetic realtime complete staged mock checkout only after real DOM checkbox/change/button click. | Published complete labelled mock workflow |
| P15 | Fixture request ledger contains no payment/order request; checkout copy distinguishes simulation; production capabilities advertise only supported handoff. | Visible labels and native Shopify handoff |
| P16 | Literal birthday/recipient/nested-command note reaches one visible enabled field; no model/request/history/public receipt/snapshot leak. | Browser field/public-context inspection |
| P17 | Guide dismissal/close/new subject and deferred cancellation prevent restarted help or late writes. | Browser interruption/dismissal |
| P18 | Existing geometry/focus/layout suites pass code assertions. | Desktop/narrow menus, controls, overflow/focus inspection |
| A01 | Manual DOM listing entry, current size/material pronouns and checked realtime product facts pass. | Published manual navigation/current questions |
| A02 | Exact width/height/diameter/option axes and title-only inference pass; upstream hidden-template/noscript normalization now passes the actual widget and backend regressions. | Published sourced measurements/unknown recovery |
| A03 | Actual exact selected variant/quantity drives unit price/subtotal; incomplete choices retain native ranges. | Published visible price answers |
| A04 | Product facts and story/meaning recovery suites require checked description/options/images and approved attribution. | Published images/detail/approved story copy |
| A05 | Identity conflict, stale timestamps, malformed literal sets, availability and holds fail before authority. | Published stale/held detail recovery |
| A06 | Actual open-product context outranks unrelated hover; gallery/native control postconditions and current view are checked. | Pointer/keyboard/current-view browser sequence |
| A07 | Actual manual route ownership and repeated back/forward/reversal tests prevent stale card restoration. | Repeated browser route cycles/reload |
| A08 | Deferred old product/options/action revision cannot override a newer route or menu/selection. | Browser interruption/late-return sequence |
| A09 | Actual manual controls, typed bridge and synthetic native tool update one shared exact selection/cart. | Physical voice after manual control changes |
| A10 | Gallery/menu postconditions and image-versus-product routing retain current detail on close. | Actual viewer/navigation/focus restoration |
| A11 | Local context projection and native committed-turn byte budget retain exact selected literals/count semantics. | Real native conversation latency/context |
| A12 | Public snapshot/voice projections omit notes/admin fields; opaque private setters and prompt-like content cannot execute nested actions. | Browser public-context inspection |
| R01 | Prepared local alternatives/matching leave current controls/page/cart unchanged without remote inference. | Published quiet suggestion preparation |
| R02 | Exact eligible motif stands alone; broad-theme fallback is used only when exact candidates fail availability/preferences. | Published suggestion relevance/why |
| R03 | Actual matching-necklace/cheaper/uncertainty flows and guide literal-dimension comparisons pass. | Published comparisons and smaller unknown recovery |
| R04 | Actual recommendation UI/receipt includes native currency, exact available material/price and checked provenance. | Visible cards/links without unsupported sales claims |
| R05 | Warm suggestions use existing checked cache; product-bound prepare/revision does not publish old-product content. | Browser warm suggestion timing |
| R06 | Actual dismissal persists/mutes help; explicit revival works without automatic selection/cart writes. | Published gentle pacing/no-thanks |
| R07 | Primary NN/G, CHI, FTC, CCI and Shopify sources were independently opened; research distinguishes design inference from claims. | Verified research; no conversion claim made |
| R08 | Actual synthetic realtime full host/bridge/widget journey and all five short-followup parity cases pass; both production add/review token misroutes refuse before cart writes. | Physical microphone/recognition/audio/interruption |
| R09 | Current native adapter advertises only actual verified controls; selectors/hook/migration limits are documented. | Actual Shopify theme installation/cart/handoff |
| R10 | All 64 rows have explicit evidence/remaining acceptance; final ledger does not conflate fixture and live behavior. | Final fixed suite, commit/build/deploy identity and browser ledger |

## Evidence ledger (update against final implementation)

| Evidence item | Result | Scope and limits |
|---|---|---|
| Independent targeted final-source fixture sweep | **304/304 passed**, 0 failed/skipped/cancelled | Final frozen working tree, 11 test files; actual host/bridge/widget, thinking, shared classifier/backend/guide, native adapter, native synthetic voice, exact current facts and malformed lengths; no physical provider/commerce/GPU |
| Independent review/direct-add legacy sweep | 129/129 passed | Product understanding, storefront shopping, typed integration, guided demo, UI and host integration; explicit Review semantics retain their meaningful authority/freshness assertions |
| `tests/growth/adversarial-shopping43.test.cjs` | **42/42 passed** in the final independent sweep | Actual varied checked catalogue; all D05 phrases on product/collection, five synthetic realtime parity cases, upstream hidden-measurement regression, privacy, exact options/direct add/review/cart/mock checkout and cancellation |
| Malformed length correction | 12/12 original cases and independent added signed/precision case passed | Old invalid loop retained; no lowering of refusal criteria |
| Root full growth final-source run | Earlier run 4662/4663; complete corrected rerun pending | Inspected `/workspace/scratch/b19204ddae35/growth43-verified.log`; its sole malformed-length failure is now fixed and independently rerun; log predates D05 additions |
| Root browser evaluation | Pending | Must record exact published/staging commit and observable controls/results |
| Actual microphone conversation | Unverified | Requires real input/output and permission/interrupt test |
| Subjective face approval | Unverified | Requires rendered sequence and human aesthetic review |
| Live production Shopify commerce | Unverified | Sandbox bag/checkout is a mock; migration evidence is separate |

The independent frozen-source command was:

```sh
node --test tests/growth/adversarial-shopping43.test.cjs tests/growth/avatar-thinking43.test.cjs tests/growth/catalogue-intents43.test.cjs tests/growth/catalogue-backend43.test.cjs tests/growth/concierge-shopping-guide43.test.cjs tests/growth/storefront-awareness43.test.cjs tests/growth/storefront-purchase43.test.cjs tests/growth/shopping-voice43.test.cjs tests/growth/shopper-journey43.test.cjs tests/growth/product-facts38.test.cjs tests/growth/storefront-length-alias39.test.cjs
```

Output inspected: `/workspace/scratch/b19204ddae35/adversarial43-final.log`, duration 22,458 ms. The separate 129-case legacy review/direct-add run is recorded in `/workspace/scratch/b19204ddae35/adversarial43-legacy-review.log`. Those review tests use an explicit review request; their exact stock/price/selection checks and visible final-click requirements remain intact.

Current code/fixture reconciliation has no known unresolved blocker. That statement applies to the tested code paths, not final publication, subjective face approval, physical microphone use, or live Shopify commerce. The final browser/build/deployment ledger must be appended with its exact identity before any publication claim.
