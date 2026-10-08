# Brites concierge integration

The conversation, voice tools and shopping action protocol are shared between the test boutique and the current Brites Shopify theme. Moving to Shopify changes the host adapter and installation, rather than rebuilding the conversation or its service endpoints. This branch enables testing; it does not publish or modify the production theme.

## Inventory and exact prices

The starting test collection targets 120 distinct published listings across regular necklaces, beady necklaces, stud earrings, hoop earrings and charm-only pieces. The five coverage counts describe the checked sample, not the full shop or available stock. Every listing can open its own detailed page with published descriptions, image galleries, option groups and variants. Discovery combines the bounded seed and public predictive results. A recommendation refreshes an exact shortlist before quoting availability and price. Product holds are checked for the entire candidate pool in chunks.

Public catalogue responses sometimes omit availability. Those variants carry `availabilityKnown:false`; the page labels their availability as pending. They remain discovery candidates, never confirmed stock or addable options until an exact product read succeeds. The selected variant, currency, quantity and current item subtotal are projected from the actual controls, rather than from a product's minimum price. Ordinary published Charm listings can use the normal exact-choice review; components, add-ons, held and customized selections retain their separate checks. A charm choice does not imply an included necklace or hoop.

## Shared control surface

Both hosts expose `snapshot()`, `execute(action, context)` and a versioned `capabilities` declaration. The bridge accepts only a current shopper request and a current exact target. It rejects stale, conflicting, repeated or ambiguous requests. Pointer and keyboard focus supply context for an explicit question; they do not authorize actions or trigger an inference call by themselves.

`BritesConcierge.sendShopperCommand(text)` opens the existing helper without taking pointer focus, and sends bounded text through its ordinary typed conversation controller. The visible “See your guide in action” panel uses that gateway. It has no direct host-control shortcut and does not synthesize a voice event. Native tools use the same action bridge with committed-input and response authority.

| Flow | Shared action | Current Shopify binding or endpoint |
| --- | --- | --- |
| Search | `search` | Existing GET product-search form and locale `/search?type=product&q=…` |
| Category | `filter` | Published collection handles confirmed by links or Liquid collection objects |
| Sort | `sort` | Existing `#bjcSort` options and native change event |
| Open listing | `open` | Exact owned locale `/products/<handle>` |
| Identify/highlight | `highlight`, `scroll` | `#bjPrice`, `#bjMedia`, `#bjForm`, published details disclosures and `#MainContent` |
| Show a menu | `options` | Literal option group, including existing `.bjselx__btn` listbox trigger |
| Change material/length/engraving option | `select-option` | `#bjMetals [data-vi]`, `select.bjOptSel[data-idx]`, `#bjEngrChk` |
| Product quantity | `product-quantity` | Existing `#bjQty` and its native step buttons |
| Review addition | `review-add` | Existing concierge review hook, fresh exact product and market reads; shopper clicks Confirm |
| View bag | `bag` | Owned locale `/cart` |
| Change/remove a bag line | `bag-quantity`, `bag-remove` | Verified `#bjCartItems .bj-cp__row[data-key]`, native quantity steppers and exact remove link |
| Gift wrapping/message | `gift` | Real `#is-a-gift`/`#bjGiftWrap` section; paid wrapping and personal inputs remain shopper controls |
| Begin real checkout | `checkout` | Fresh verified bag, `#bjCartForm` POST and its native `button[name="checkout"]`; shopper reviews hosted checkout |

Cart-line actions use the theme's own AJAX handlers. Each quantity step is verified through locale `cart.js` before another step. The actual handler uses Shopify `cart/change.js`; gift controls use Shopify `cart/update.js`. Stable line keys distinguish the same variant configured with different properties. The model receives visible product/variant identities, quantities and prices only. It receives no cart token, engraving text, gift note, customer, contact, address or payment data.

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
3. Verify one listing from each of the five groups, literal material/length menus, price changes, quantity, exact-choice review and a shopper-confirmed addition in the unpublished theme.
4. Verify real cart rows, same-variant/different-property line identities, quantity/remove readback, paid wrapping and note controls, and native checkout handoff. A missing/changed/duplicated binding disables the action rather than guessing a selector.
5. Test actual microphone audio, WebRTC networking and graphics in a supported shopper browser. Synthetic callback tests and cloud browser checks do not certify physical audio or GPU quality.
6. Promote the reviewed unpublished theme only when the production move is authorized. No server-side campaign, conversion or payment action is part of this installation.

Native voice continues to use the existing OpenAI API directly. No application allocation, dollar refill or preview reservation gates native voice. Optional typed model replies retain their existing independent settings. Model/API keys are never placed in Shopify assets or conversational context.
