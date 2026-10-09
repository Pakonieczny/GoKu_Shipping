# Brites avatar integration continuation 45

This release addresses the five reported failures on the authorized sandbox branch `codex/brites-growth-2026-10`. The [64-point acceptance contract](avatar-integration-acceptance43.md) and [release44 evidence](avatar-integration-completion44.md) retain their detailed scope and historical labels. Main and the live Shopify theme were not changed.

## Resulting behavior

| Reported failure | Correction and verified result |
| --- | --- |
| Homepage request refused | Exact home navigation returns the sandbox catalogue/root view, clears route queries and preserves the cart. The production adapter has corresponding same-origin shop-root navigation. |
| Avatar stops midway and fails to recover | The native client coordinates the owning response, final acknowledgement and actual buffered playback drain. Bounded recovery handles token truncation, one transient generation failure, lost drain, empty/failed false interruptions and confirmed call expiry. End, hide, close and genuine new speech cancel recovery. Actions are not replayed. Real audible recovery remains unverified without a microphone. |
| 16-inch solid-gold request fails; internal wording reaches customers | Final ASR is grounded before proposed model controls. Compound metal/length requests resolve against actual published option groups without choosing an unspecified engraving option. Customer receipts expose useful plain wording and checked public facts; raw diagnostics and action labels stay outside that receipt. Compound narration retains all factual answers and only the final control result. |
| Cannot configure, add or manage cart | The sandbox host exposes actual option menus, quantity, exact-variant addition, cart opening, stable cart-line quantity/removal, published option replacement and local engraving preview edits. Fresh identity, exact options, price, currency, availability and cancellation checks precede each commercial change; other lines and private local wording are preserved. Removing engraving clears wording, and exact fresh undo can restore it. |
| Detail opening works but other controls do not | Detail opening remains intact. Native client fixtures and the deployed browser exercise the remaining controls. The natural chain-length alias maps to the actual Necklace Length group only when the requested group has sufficient authority; ambiguous groups still require clarification. |

Cart engraving is a local sandbox preview. Final customization and checkout still need the shop; this release does not install new cart controls into the live Shopify theme. The production adapter change in this release is homepage navigation.

## Frozen verification

Tested executable commit: `e795d5b28911a818ed36a5ca9faf23e44a050872`  
Tested tree: `1da33e4af9666f09d65697173fc87d18b3ff6579`  
Browser-tested ready Netlify deploy: `6ac8f7b8f6bb2a0008ec9819`, published `2026-10-09T14:19:04.325Z`  
Branch: `codex/brites-growth-2026-10`; deployment error: none; callable endpoints: 10.  
Frozen 41-path manifest SHA-256: `c872d98aadc30ce4a6cbfc26b2280b28c591524a0d1fabde5b0378b2d84b811e`.

| Check | Result and boundary |
| --- | --- |
| Full growth suite | **5046/5046 passed**, zero failed/cancelled/skipped/todo; 92873.943292ms. Includes the 45 independent cases rather than adding them to the total. |
| Independent adversarial suite | **45/45 passed**, zero failed/cancelled/skipped/todo; 7410.207246ms. All 41 frozen hashes remained unchanged before/after. |
| Identical 45 cases against exact release44 source | **13 passed / 32 failed**; 6620.015075ms. All 17 recovered release44 hashes remained exact. These are deliberately failing prior-source comparisons. |
| Focused Undo copy/restore checks | 44/44 focused host/reversal/failure-receipt cases passed; independent exact Undo case 1/1 passed. Restore, freshness, quantity, other-line and privacy checks were retained. |
| Relevant Ads/Data Manager regression files | 5/5 passed; their executable sources are unchanged. |
| Configured root build |Passed: 160 public assets plus 3 optional fonts; 176 callable functions. |
| Isolated sandbox build |Passed: 21 reported assets, 65 server modules, 10 callable endpoints. |
| Staging and remote bindings |All 7 changed executable files are exact in root/sandbox staging. All 41 frozen executable/test Git blobs match the remote tree; 1333 repository blobs; recursive tree not truncated. |
| Avatar scene |Existing 627689-byte scene/friendly rig unchanged. Browser proof shows actual 2D fallback; no GPU/mobile performance certification. |

The new tests call actual client, host, bridge, adapter and backend modules. Legacy fixtures were adapted to the current safe receipt projection, exact raw cohort authority and owning response/drain ordering; obsolete expectations were not restored by weakening those guards. Counterexample cases reject stale controls, duplicate identities, ambiguous option groups, unsafe customer copy, failed call retirement and action replay.

## Actual browser observations

These checks use **typed guide commands in the actual deployed page**. They prove UI/host/bridge controls and visible captions; they do not prove microphone ASR or audible playback. Native media and provider-event fixtures are separately synthetic.

The guide opened Leaf Pendant Cable Necklace. The literal 16-inch solid-gold request selected 14k Solid Gold plus 16 Inch and asked only for the still-unset Engraving choice. Selecting Engraved and quantity 2 produced the real published USD 316 unit price and USD 632 item subtotal with a current single control reply. A synthetic local wording value survived exact addition and cart opening.

Natural chain length to 18 inches changed the real Necklace Length dropdown while keeping engraving and quantity: USD 331 each/quantity 2/USD 662 subtotal. Quantity 3 produced USD 993. Cart engraving editing changed the actual textarea without assistant repetition of its private contents. Selecting None cleared wording and selected the USD 310 non-engraved 18-inch variant (quantity 3/USD 930). Undo restored Engraved plus the synthetic words, USD 331 each/quantity 3/USD 993, and used the exact plain sentence: “Your last change is undone. Your earlier choice is visible.”

Length 16 then quantity 2 restored USD 632. The cart exposes actual metal/length/engraving dropdowns, quantity, editing/clearing wording, removal and the test subtotal. Home navigation preserved bag count 2. The final remove command emptied the cart and home navigation returned the base catalogue with 120 checked loaded products. No real order or payment was attempted.

Verified browser URLs:
- https://brites-growth-sandbox.netlify.app/concierge-sandbox.html
- https://brites-growth-sandbox.netlify.app/concierge-sandbox.html?product=leaf-necklace-dainty
- https://brites-growth-sandbox.netlify.app/concierge-sandbox.html?cart=1

Proof images: `Brites-Avatar-Controls45-20261009.jpg` and `Brites-Avatar-Cart45-20261009.jpg`. They show the final executable deploy, selected options/quantity/current caption, and actual cart editing controls. Cart contents are session-local, so another browser must recreate the selection.

## Remaining empirical acceptance

Clicking the actual Talk to me button on this final browser produced: “No microphone was found. Connect or enable a microphone, or type here.” No physical microphone input, live ASR or audible completion/recovery was certified. Provider interruption/recovery evidence is synthetic, including fake transport and media timing. The provider-tool fallback may finish its already-buffered owning audio before the safe acknowledgement; arbitrary provider audio censorship is not established.

GPU rendering, mobile touch behavior and live Shopify installation are also unverified. The existing 64-point contract is retained; passing code/fixture cases does not mark every point empirically complete. Main, live Shopify, persistent user data and existing paid budget/allocation history remain intact.

A following documentation-only commit records this evidence. Its release verification must retain this 41-file source manifest and the 10 tested function digests.
