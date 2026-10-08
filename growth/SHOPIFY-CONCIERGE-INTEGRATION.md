# Brites concierge integration

The conversation, voice tools and shopping action protocol are shared between the test boutique and the current Brites Shopify theme. Moving to Shopify changes the host adapter and installation, rather than rebuilding the conversation or its service endpoints. This branch enables testing; it does not publish or modify the production theme.

## Inventory and exact prices

The starting test collection targets 120 distinct published listings across regular necklaces, beady necklaces, stud earrings, hoop earrings and charm-only pieces. The five coverage counts describe the checked sample, not the full shop or available stock. Every listing can open its own detailed page with published descriptions, image galleries, option groups and variants. With `data-inventory-preload="complete"`, the page eagerly reads the complete sample through `GET /api/growth/inventory?offset=0&limit=24` and successive pages. The visible inventory status reports checked records and becomes ready only after every record loads. These public facts retain their original check times in an isolated five-minute cache; incomplete reads stay unconfirmed. Product holds are checked for all 120 identities in chunks.

Known product pages, exact questions, images and reversible controls use the checked browser cache. An informational answer preserves the current choices and quantity. Its structured `productFacts` contains only public product facts; item totals use the actual selected variant and quantity. Native inspection tools receive the same checked records without a per-question product fetch. Discovery combines the bounded seed and public predictive results, and recommendations refresh an exact shortlist. Confirming an addition always performs a fresh exact product check.

Public catalogue responses sometimes omit availability. Those variants carry `availabilityKnown:false`; the page labels their availability as pending. They remain discovery candidates, never confirmed stock or addable options until an exact product read succeeds. The selected variant, currency, quantity and current item subtotal are projected from the actual controls, rather than from a product's minimum price. Ordinary published Charm listings can use the normal exact-choice review; components, add-ons, held and customized selections retain their separate checks. A charm choice does not imply an included necklace or hoop.

## Shared control surface

Both hosts expose `snapshot()`, `execute(action, context)` and a versioned `capabilities` declaration. The bridge accepts only a current shopper request and a current exact target. It rejects stale, conflicting, repeated or ambiguous requests. Pointer and keyboard focus supply context for an explicit question; they do not authorize actions or trigger an inference call by themselves.

Ordered requests use the actual resulting snapshot before each next step. A real Shopify full-page navigation is a handoff: the old adapter disables its controls and the sequence stops before any remaining step can act on the previous product. The shopper continues on the new page after it loads; accepting a navigation request does not certify that its destination is already mounted.

`BritesConcierge.sendShopperCommand(text)` opens the existing helper without taking pointer focus, and sends bounded text through its ordinary typed conversation controller. The visible “See your guide in action” panel uses that gateway. It has no direct host-control shortcut and does not synthesize a voice event. Native tools use the same action bridge with committed-input and response authority.

| Flow | Shared action | Current Shopify binding or endpoint |
| --- | --- | --- |
| Search | `search` | Existing GET product-search form and locale `/search?type=product&q=…` |
| Category | `filter` | Published collection handles confirmed by links or Liquid collection objects |
| Sort | `sort` | Existing `#bjcSort` options and native change event |
| Open listing | `open` | Exact owned locale `/products/<handle>` |
| Back/forward | `back`, `forward` | Checked native browser entries identified by an owned `history.state` key; the sandbox restores its saved view and choices locally |
| Choose image | `gallery` | Current verified `#bjMedia` figures and image dots; the sandbox also provides its own image viewer |
| Close a menu/image | `close-options`, `close-image` | Verified `.bjselx__btn` listboxes; an actual open native image `dialog` inside `#bjMedia` containing a current gallery image |
| Identify/highlight | `highlight`, `scroll` | Literal title, description, material, length, engraving, price, images, cart and checkout sections, plus page direction |
| Show a menu | `options` | Literal option group, including existing `.bjselx__btn` listbox trigger |
| Change material/length/engraving option | `select-option` | `#bjMetals [data-vi]`, `select.bjOptSel[data-idx]`, `#bjEngrChk` |
| Enter engraving text | `set-engraving` | Writable `#bjEngrTxt` in the exact verified `#bjEngr`, only while the published engraving choice is selected |
| Product quantity | `product-quantity` | Existing `#bjQty` and its native step buttons |
| Review addition | `review-add` | Existing concierge review hook, fresh exact product and market reads; shopper clicks Confirm |
| View bag | `bag` | Owned locale `/cart` |
| Change/remove a bag line | `bag-quantity`, `bag-remove` | Verified `#bjCartItems .bj-cp__row[data-key]`, native quantity steppers and exact remove link |
| Gift wrapping/message | `gift`, `gift-preferences` | Reveals the real `#is-a-gift`/`#bjGiftWrap`; edits only detected `#gift-wrapping[name="attributes[gift-wrapping]"]` and `#gift-note[name="attributes[gift-note]"]` in the native POST cart form |
| Undo a choice | `undo` | One current-page reversible choice through the same native controls: options, quantity, engraving text, sort, confirmed gallery position, gift fields or existing cart-line quantity |
| Begin real checkout | `checkout` | Fresh verified bag, `#bjCartForm` POST and its native `button[name="checkout"]`; shopper reviews hosted checkout |

The current Shopify product cache is bounded to 60 seconds and supplies a canonical public product projection to the shared guide. Reversible native actions use its exact model and verify the resulting real controls; factual reads after expiry require a fresh check. The current desktop gallery shows its published figures and image dots. Image closing is enabled only for the verified native dialog described above; an unobserved mobile zoom overlay is not a conversational control. Native Back/Forward uses `history.go` for the checked adjacent entry, preserving browser forward history rather than adding a new destination. The adapter preserves the theme's existing object state, reconciles restored entry keys on `popstate` and BFCache `pageshow`, and suspends old controls until the destination is ready. Unbound legacy URLs, intervening unobserved pages, and primitive or conflicting theme state cannot authorize a guessed traversal.

Undo keeps its prior and resulting states only in the current page's private memory. It is available only while exact identities and relevant controls still match the resulting state. A manual change, changed checked variants, route change or missing binding withdraws it. It neither re-adds a removed cart line without its original properties nor reverses an order or payment. Back/forward restore views separately.

Engraving edits dispatch the real input/change events and obey the smaller of the native field's limit and 300 characters. They do not turn engraving on, invent an option, click Add or bypass personalization review. Only availability, enabled state, presence of text and the length limit enter public context. Raw text and undo content stay private. The current theme's engraving line-property schema has not been verified, so personalized additions continue through its customizer.

Cart-line actions use the theme's own AJAX handlers. Each quantity step is verified through locale `cart.js` before another step. The actual handler uses Shopify `cart/change.js`; the existing gift controls own any Shopify `cart/update.js` save. The adapter changes and reads back the detected native gift fields without issuing a new save endpoint or claiming server persistence. `giftControls.savedKnown` remains false, and the shopper reviews the displayed gift charge before checkout. Gift packages are unavailable without a separately verified control. Stable line keys distinguish the same variant configured with different properties. The model receives visible product/variant identities, quantities and prices only. It receives no cart token, engraving text, gift note, customer, contact, address or payment data.

The existing collection map is:

| Requested group | Current collection |
| --- | --- |
| Regular necklaces | `necklaces` — the merchant's published collection membership applies |
| Beady necklaces | `beady-chain-necklaces` |
| Stud earrings | `charm-studs` |
| Hoop earrings | `huggie-hoops` |
| Charm-only | `charms-only` |

The test seed classifies the five groups separately. The real `necklaces` collection may have broader membership; the adapter describes that real collection rather than inventing a new regular-necklace endpoint.

## Test checkout and production handoff

The sandbox keeps product selection, bag, gift preferences and checkout in one document, so the mounted guide remains connected. It supports review, labelled demo shipping and confirmation stages. The final test acknowledgement and completion remain explicit shopper clicks. Completion creates no real order or payment and preserves the test bag.

The Shopify adapter can open the real checkout through the existing form after checking the current bag. Hosted checkout shipping, taxes, discounts and payment remain the shopper's controls; `finalOrder` is always false. Sandbox shipping choices and final mock completion are not installed as production checkout controls. Normal full-page Shopify navigation ends the current native voice connection; conversation history is preserved, and the shopper starts voice on the new page. A persistent production voice shell requires a separate verified theme-navigation integration.

## Installation and migration checks

1. Copy the shared concierge, voice, action, bridge and avatar assets listed in the existing deployment manifest to an unpublished Shopify theme. Keep the deployed server API base and existing OpenAI credentials on the server.
2. Render `brites-concierge` with `enable_storefront_adapter: true`. Its Liquid config provides locale root, currency, exact product identity/count and only collection handles that Shopify exposes. It embeds no customer or cart-note JSON.
3. Verify one listing from each of the five groups, literal material/length menus, native menu closing, selected engraving text and its actual property schema, price changes, quantity, guarded undo, exact-choice review and a shopper-confirmed addition in the unpublished theme. Editing a text field does not certify a personalized addition.
4. Verify real cart rows, same-variant/different-property line identities, quantity/remove readback, detected wrapping/note fields and their actual native saves, charge display, and native checkout handoff. A missing/changed/duplicated binding disables the action rather than guessing a selector. Confirm native image-dialog bindings before advertising image closing; sandbox zoom alone does not establish a production overlay mapping.
5. Test actual microphone audio, WebRTC networking and graphics in a supported shopper browser. Synthetic callback tests and cloud browser checks do not certify physical audio or GPU quality.
6. Promote the reviewed unpublished theme only when the production move is authorized. No server-side campaign, conversion or payment action is part of this installation.

Native voice continues to use the existing OpenAI API directly. No application allocation, dollar refill or preview reservation gates native voice. Optional typed model replies retain their existing independent settings. Model/API keys are never placed in Shopify assets or conversational context.
