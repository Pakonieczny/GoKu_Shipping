# Independent avatar and cart adversarial review50

Reviewed on 9 October 2026 in the shared continuation checkout rooted at `fc4a9fff378a78a82a54f8af8a3394287df671df`. Local HEAD precedes the published continuation49 changes, so it does not identify the current working source. The root release ledger owns the final source hash, complete-suite results, commit, publication and browser observations.

## Acceptance scope

`tests/growth/avatar-adversarial50.test.cjs` contains 49 cases after the final control inventory and typed-path extensions. They invoke the production widget, native voice client, finalized ASR handler, storefront bridge and local storefront host. The 47 native cases retain the inherited typed-entry guard. Only the two explicitly typed cases leave the real shopper method and composer binding available; ordinary fallback chat remains forbidden for every case. A narrow fixture hook mounts the real expression module and real avatar fallback controller; another retains the provider callback to deliver a genuinely queued old message after closing. An observer records callback exceptions and rethrows them. No production errors are swallowed.

The nine inventory extension cases mount the actual Shopify adapter over synthetic current-cart HTML when native cart controls are required. The fixture supplies a labelled actual checkbox, actual discount input/button and the theme's staged-code readback contract. It forbids any cart POST and observes checkout clicks independently. This tests actual control dispatch and shopper-request authority, not a connected theme or valid discount.

| Area | Concrete acceptance |
| --- | --- |
| Cart lines | Exact stable-line quantity, length, metal, removal, undo, unrelated rows and independently calculated subtotal. |
| Current dropdown | Actual expanded cart menu, brought into view; quantity focus blocks a bare ordinal; an explicit option ordinal selects the literal current menu; closing retires it. Selected values persist through navigation and same-tab reload. |
| Cart highlight | Actual quantity and price targets in the exact current row are revealed without changing any line. |
| Bulk clear | Both explicit empty-bag phrasings clear every checked mounted line exactly once, retain private gift preferences, and undo restores exact saved rows. Delayed old delivery, aborted actions, stale complete IDs and a silently detached row cannot clear newer or unseen contents. |
| Product choices | All six independently priced metal/length combinations update actual selected buttons, exact variant IDs, quantity and displayed price. Back navigation retains the final choices; adding quantity three yields unit price 260 and subtotal 780. |
| Scroll/gallery/navigation | All four directions change the actual synthetic window target. Actual description highlight, second image source, zoom and close are checked. Five repeated product back/forward cycles keep one microphone session and current-product answers. |
| Ambiguity | Eight negated, quoted, hypothetical or ambiguous cart requests cannot mutate or invoke controls. A manually changed cart dropdown retires an older provider function delivered through arguments, completed item and response completion. |
| Private fields | Literal engraving, cart note and design brief remain exact in actual fields and outside public page context, returned receipts and historical voice packets. Trailing cart/navigation words in a note or brief remain literal data. Cart clear preserves the note; explicit field clearing acts only on that field. |
| Checkout boundary | Actual synthetic shipping step and selected express button are checked. Spoken completion neither ticks the acknowledgment nor activates final completion; cart rows remain unchanged. |
| Emotions | Ordinary frustration yields a reassuring actual avatar; sarcastic or negated gratitude cannot display its appreciation heart. A solemn-to-graduation correction retires old emotional output; interruption/close blocks replay. Resolved frustration permits appreciation, and an Angry Cat product label cannot imply shopper anger. |
| Render configuration | The actual Three.js scene uses its supported numeric PCF shadow mode and actual visible receiving floor. Real scene geometry is used with a synthetic renderer. |
| Final control inventory | Actual catalogue pagination/exhaustion, current cited story plus delayed navigation cancellation, and actual add-review cancellation. Quoted, negated and hypothetical forms cannot activate those controls. |
| Native terms/discount | Explicit shopper acceptance/revoke changes only the actual labelled checkbox. Code staging uses only the current actual input/button and cannot claim verified savings. Unsupported sandbox controls abstain. A proposed provider acceptance cannot borrow negated, quoted, hypothetical or unrelated speech, or override a newer explicit revoke. |
| Typed inventory | Actual typed catalogue/story/review flow retains voice and current choices. Native typed code staging/consent reach real mounted cart controls; actual visible composer revocation retires older native consent. Control requests avoid general knowledge/service fallthrough, while normal product navigation may load the host's legitimate service footer. |

This is a finite adversarial matrix over published controls and concrete authority boundaries. It does not establish correct behavior for every conceivable spoken phrase.

## Findings and corrections

The first exploratory 30-case run recorded 19 passes and 11 failures. Nine failures exposed concrete missing or incorrect behavior: native cart menus and row highlights, full-cart clear, and four emotion cases. “I am pissed off with this” and “I am angry because this keeps failing” remained warm; “Thanks for nothing” and “I do not appreciate that” triggered appreciation. Root corrected both widget conversational tone and the actual expression context. The unchanged emotion cases subsequently passed, alongside explicit resolution and product-label counterexamples.

Two first-run failures were reviewer fixture issues, recorded separately from source defects. The synthetic product's primary image needed to match its explicit three-image list before asserting the second actual image. Closing correctly detaches the channel callback; the stale-delivery test now retains the actual callback before closing to simulate an already queued event. Neither correction weakened the production assertions.

Three.js 0.186.0 still exports `PCFSoftShadowMap = 2` in its local `src/constants.js:75`, marked deprecated since r186 at line 73. Its `src/renderers/webgl/WebGLShadowMap.js:99-102` warns that the setting has been removed and falls back to `PCFShadowMap = 1`. Root now configures the supported `PCFShadowMap` directly; the actual configured renderer-mode and receiving-floor test passes. This verifies supported configuration and geometry, not GPU pixels or aesthetic quality.

The cart and bridge agents added bounded current-line menus, exact row highlights, immediate complete-cart clear, private cart notes and design briefs. The native client and server validators reject foreign parameters, unbounded identifiers and arbitrary selectors. Clear is cart-only and preserves notes and attributes. The production adapter separately verifies fresh actual cart prechecks and private readback; this reviewer does not upgrade those fixtures to live commerce proof.

An intermediate 35-case run captured 24 finalized-callback timeouts while shared source was being edited. That run remains recorded as failed and is not final integration evidence. A subsequent unchanged tiny cart quantity/options/remove/undo case completed normally. The bridge reviewer then ran 33/35 successfully, finding only exact bulk-clear undo restoration: the saved `variantOptions` array changed from published Metal Choice/Length order to Length/Metal Choice. The native private-note clear/undo case reproduced the same defect. The host correction preserves the original public option ordering in cart storage and undo, using canonical order only for exact variant proof comparison. Both original clear/undo assertions and the new private-note case pass without changing their exact comparisons.

The final actual-control inventory identified three remaining sandbox buttons and two native cart controls. Nine new cases cover actual catalogue continuation, cited story activation, cancelling a current add review, native discount staging and explicit terms acceptance/revocation. The first inventory run found that speaking the visible “Review before adding” button label failed even with a complete checked selection: the bridge's review remainder filter rejected the word “before.” The narrow exact-label correction retains complete current variant/quantity validation and adds a focused bridge regression. Both blocked native cases then passed unchanged. One initial provider-consent assertion also mistook a legitimate “Show my cart” result for acceptance; the corrected assertion observes the actual unchanged checkbox, zero checkbox changes, zero checkout clicks and absence of an acceptance claim while allowing the separately authorized bag action.

The typed control list initially omitted those five inventory action types. Root added them before the typed extension. Both new typed flows then actuated every requested control. One further explicit negative, “Do not accept the cart terms,” preserved the checkbox but unnecessarily attempted ordinary chat; the unchanged transport guard detected that attempt. The bridge correction recognizes negative accept/agree requests and refuses them locally while retaining the private note/brief literal-data path. The final unchanged typed transport assertion passes with zero ordinary chat attempts. The first typed test also needed to distinguish the host's normal service-footer load during product navigation from an erroneous service read for a subsequent control, and the composer test now asserts its already visible state instead of trying to reopen it using its old label.

The root's later complete-suite run exposed a context-size regression: duplicate current-product picker variants pushed an unchanged product-understanding fixture down to 99 inventory rows, below its existing minimum of 100. Under the 18,000-byte budget pressure, the native voice client now replaces that duplicate array with `variantChoicesSource: 'currentProduct.variants'` only when a fresh, complete current-product record proves matching identity, IDs, titles, option sets and identical picker order. Other contexts retain their separate picker choices. Raw storefront action authority is unchanged. The sibling's focused unchanged product-understanding reproduction reports 105 of its 120 inventory rows at 17,984 bytes; its two added contract guards are separate from this reviewer's 49 cases.

The final URL correction changes only two widget guards relative to the verified release archive: existing preview operator headers and the optional availability read now recognize both exact `/concierge-sandbox` and `/concierge-sandbox.html` paths. Both still require the exact HTTPS sandbox origin, sandbox mode and a same-origin API. Trailing-slash, suffix, nested, QA, lookalike-host, HTTP, foreign-API and embedded-storefront cases stay outside the automatic preview availability/header boundary. Availability remains a read-only `capabilities` request; opening the panel cannot create a microphone or provider session. The expanded actual-widget UI tests independently check both allowed paths, private-header confinement, no-store/error-on-redirect transport, explicit Talk activation, malformed storage and excluded contexts. The read-only archive comparison confirms no other widget source changes since the 49-case source recorded below.

## Verification ledger

Exploratory logs are retained outside the repository at `../avatar50-adversarial-first.tap`, `../avatar50-adversarial-current.tap`, `../avatar50-finalizer-repro.tap` and `../avatar50-new-acceptance.tap`. The focused emotion/render scope passed 11/11. The added product-choice and private-design-brief scope passed 2/3, with its remaining failure being the same saved option-order restoration. The focused actual cart-menu attention case passed 1/1.

Before the final control inventory extension, the frozen-source serial run recorded **38/38 passing**, zero failures, skips or cancellations, in **10.02 seconds**, at `../avatar50-adversarial-frozen.tap`. The extended native run records **47/47 passing**, zero failures, skips or cancellations, in **13.93 seconds**, at `../avatar50-inventory-frozen47.tap`. The first frozen-source run including both actual typed paths recorded **49/49 passing** in **16.13 seconds** (16127.506958 ms), at `../avatar50-typed-frozen49.tap`. After the context compaction correction, the latest unchanged frozen-source rerun records **49/49 passing**, zero failures, skips or cancellations, in **15.54 seconds** (15541.870375 ms), at `../avatar50-context-frozen49.tap`. JavaScript syntax and whitespace checks pass. Existing jsdom/canvas and Three.js dependencies were reused; this reviewer installed no dependencies, tools or credentials. Earlier overlapping scopes must not be summed. The root owns the full-suite and build verification.

The extended initial full run records 45/47 passing, with both failures blocked at the same “Review before adding” parser gap. Its log is `../avatar50-inventory-partial45.tap`; the filename does not imply a filtered scope. The small exact review and consent reproduction is `../avatar50-review-repro.tap`. All failures are retained.

The initial typed fixture-expectation failures remain at `../avatar50-typed-inventory-first.tap`. The subsequent exact negative-consent transport failure remains at `../avatar50-typed-inventory-current.tap`, with its independent minimal reproduction at `../avatar50-negative-consent-tiny.log`. The final 49-case run retains the ordinary-chat prohibition and every control, cart, consent and stale-provider assertion.

The focused final exact-path run records **85/85 passing**, zero failures, cancellations, skips or todo, in **1.03 seconds** (1028.524542 ms), at `../avatar50-exact-path-frozen.tap`. Despite its filename, that artifact contains Node's **spec reporter** output (`✔` tests and `ℹ` summary), not TAP. It covers the actual-widget URL changes and surrounding native transport, availability, media, allocation and cleanup behavior. It is separate evidence for the final guard change; the earlier 49-case runs remain historical frozen scopes. This reviewer does not claim a complete-suite result for the final guard source.

The final exact-path source identities are:

| File | SHA256 |
| --- | --- |
| `brites-concierge.js` | `2591b2e530c0d39cc5cece3753ddd7e023a70dd41aa421e9810f6bba666f2806` |
| `tests/growth/avatar-request-voice-ui.test.cjs` | `2fa89fd192b0974759b34b757508297ad081d6a135df5ff34f17cfa99c252b19` |

The final 49-case run before the exact-path correction used the following SHA256 source identities:

| File | SHA256 |
| --- | --- |
| `brites-concierge.js` | `83b3d8d1eef0c4828973fad805925ad6de186aae7f07cc5b662542b72492fb82` |
| `brites-storefront-bridge.js` | `ae6db8d64cb0982c789c66268272fb342ac61e381200b6d0cf0dbc7e858e91d8` |
| `concierge-sandbox.js` | `8a9e8e19c60fb008dbefdf55393043a2cfcf5d3b483189b06138a0e8c8345bd5` |
| `brites-concierge-voice.js` | `635445fc9cd82911f722fa1b5d819cff467df22955c87e438c79c49f6a1d0df6` |
| `brites-concierge-expression.js` | `3873051e701bcadde717f815636e35608141f4f5a739cd2539a69ee03a1d6c84` |
| `brites-concierge-avatar.js` | `188d81838c287406e3bfc888650f8fe38ffc1bd05969457231a66045cf04ed37` |
| `brites-concierge-avatar-scene.mjs` | `35179b24dbe0ad33ef5f581a5c35cb3cf52b81848f6a1551370a117c949a589e` |
| `brites-shopify-storefront-adapter.js` | `401282dc4a4385eefb2c61bbf8c7c9373306739d356702fac581dae3d085d7f1` |
| `tests/growth/native-continuity48-fixture.cjs` | `7ea21a833500eb6a04ef099139b91f531f829324ea08af44b76add559ecdaa0a` |
| `tests/growth/avatar-adversarial50.test.cjs` | `50f84f4c0a8dfb56f79b8993dea3a24e495a6a23af7db6eb42991a4d3b1c233c` |

## Evidence limits

Microphone, media track, recognition, provider events, catalogue, network, scrolling and authentication are synthetic. The expressions and avatar controller are real, running their unavailable-GPU fallback. The scene test uses actual Three.js geometry with a synthetic renderer. This reviewer has not exercised a physical microphone, real ASR accuracy, audible response, real GPU rendering, human visual judgment, physical mobile touch, a live Shopify write, real order or payment. No publication was performed by this reviewer. Final claims must keep local implementation evidence separate from the root's observed production deployment and browser evidence.
