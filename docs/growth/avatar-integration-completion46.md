# Brites avatar integration continuation 46

This release corrects the reproduced option, cart and answer failures on the authorized sandbox branch `codex/brites-growth-2026-10`. The final deployed UI now gives short size/length answers, selects the exact published options and quantity, and adds that configuration to the session cart. Native ASR fixtures exercise the production voice path with typed/chat routes disabled. **Physical microphone input, live recognition and audible speech/recovery remain unverified:** the final browser attempt reports no microphone.

The [64-point acceptance contract](avatar-integration-acceptance43.md), [release 45](avatar-integration-completion45.md) and [release 44](avatar-integration-completion44.md) retain their scope and historical evidence. Main and the live Shopify theme were not changed. New cart controls and engraving wording are sandbox-host features; engraving remains a local preview requiring shop review before real checkout.

## Resulting behavior

| Reported problem | Correction and concrete result |
| --- | --- |
| Size question reads the listing | Factual replies select only the asked component and measurement from the complete checked description. The literal 2,087-character published description puts its measurement at character 644; the old truncated fact field lost it. The final visible answer is “Lowercase Initial Necklace. The charm is 6–9 mm, depending on the letter.” Packaging, holder and relational/negated measurements cannot establish charm size. An unlabeled range does not establish width, height or diameter. |
| Confused face remains during idle time | Both the scene rig and SVG fallback use balanced friendly rest/listening expressions. Actual option ambiguity allows only a short confusion cue, then mild regret and reassurance, returning to friendly rest. Duplicate cues do not extend it; pause/hide/close and reduced motion retire it. The final screenshot shows the actual friendly 2D fallback; WebGL is disabled in this browser. |
| Length question reads every variant | Replies use the actual Necklace Length option group and omit unrelated metals, engraving, prices and description. The final visible answer is “Lowercase Initial Necklace. You can choose 14, 16, 18 or 20 inches.” Actual published choices take priority over older description prose. |
| Voice cannot select options/quantity | Finalized ASR reaches the real widget/resolver/host before proposed model controls. Published option matching resolves natural 16-inch/solid-gold wording without choosing unspecified engraving. No engraving, Without engraving and Unengraved resolve to one actual published no-engraving choice, including in bounded multi-step requests. Current-input, freshness, cancellation, ambiguity and replay guards remain. |
| Configured addition/cart edits fail | The host verifies the exact variant, option tuple, quantity, price, currency and current availability before changes. The final actual cart contains 14k Solid Gold / 16 inch / None, quantity 2, USD 269 each and USD 538 item subtotal. All three native cart dropdowns and the local engraving field are present. Delayed inventory now fills a restored cart's missing option controls without replacing existing controls, focus or unsaved text. |
| Wording reaches no visible field or gives excessive narration | “Change the engraving wording…” reaches the exact local cart field. A successful edit says only “Your engraving wording is updated.” A successful clear says only “Your engraving wording is cleared.” Neither acknowledgement repeats the private text or claims that a real Shopify order/customizer was saved. |
| Internal control fields reach provider narration | Customer control output and customerMessage-marked output project exactly one safe reply; final narration has tools disabled. Validation, privacy limits and rich unmarked read-only catalogue grounding remain. Arbitrary already-buffered provider audio censorship is not established. |

The earlier bounded speech-recovery implementation remains covered by the full suite. It respects genuine new speech/end/hide/close, the 120-second call boundary and existing budgets, and never replays completed website actions. This is fixture evidence, not audible recovery certification.

## Exact frozen source and publication

- Tested executable commit: `0768e688e063d294d5482a6cd72ec9e2417f37f2`
- Tree: `328561e2f291b99fcee2928da3dfea16d151376b`
- Ready browser-tested Netlify deploy: `6ac91cf0213b7e000816c3ea`
- Published: `2026-10-09T16:57:47.895Z`; branch `codex/brites-growth-2026-10`; error none; 10 callable endpoints.
- Final 34-file executable/test manifest SHA 256: `9b7e7be17cf6bceeea1758c7ec40f979c67cf7d4c020e102c2aea0d38957fda3`.
- Remote recursive tree:1341 blobs, not truncated; all 34 frozen Git blob bindings exact.
- Existing acceptance contract SHA 256: `918aca990f543af843cb30a5803e4e4d608c402529234eecbffe8ccdccf9fb7d`; 64 unique definitions retained.

| Verification | Result and boundary |
| --- | --- |
| Final full growth suite | **5173/5173 passed**, fail/cancelled/skipped/todo 0; 100588.661524 ms. Independent 22 and native-spoken 24 cases are included in this total. |
| Final independent unchanged 22 | **22/22 passed**, fail/cancelled/skipped 0; 4977.904911 ms. All 34 bindings and 744 runtime hashes stable before/after. |
| Exact published-description cases on unchanged 46 a | Independent 22:17 pass/5 fail; native exact-description 15:13 pass/2 fail. These deliberately failing prior-source comparisons exposed the missing charm span. |
| New compound no-engraving native cases | Identical three new cases:0/3 on 46 e and 3/3 after the bridge fix. They use production native ASR/widget/bridge/host with simulated input/provider/media and no typed/chat path. |
| Initial 46 f full run and expectation migration | Initial 5171/5173, two existing assertions rejected explicit No engraving followed by open cart/select Gold Filled. Independent review found these valid requests. Only those two plan expectations migrated: standalone resolve still refuses; exact two plan actions, raw transcript and unchanged page are asserted; all eight other negatives remain unchanged. The initial log/freeze and failures remain recorded. |
| Public-site staging build |160 public assets plus 3 optional fonts; 176 functions. |
| Isolated sandbox staging build |21 reported assets, 65 server modules, 10 callable endpoints. |
| Source/staging consistency |All 34 frozen sources remain exact after the full suite; 18 staging bindings match; diff check passes. |

Full log SHA 256: `1973ae9c6bb94c7d841a5df43a0f7e1347d33edd228de3da715a8d2676bda806`.  
Independent log SHA 256: `4f4b65f1ffe57de1d001dcecd9659635ddbdc930eb6507676c9a76a75918fe69`.  
Independent audit SHA 256: `e181c6a5e7ce6e39dd856fede319f5e72665014cd0050bca5de576deaddbc1ad`.  
Independent review SHA 256: `6995254eb75b99c3114dc9d6109c02496143964be833b7e0c6e5b1e483d16aa1`.

The independent reviewer made no production/test edits. No demonstrated source blocker remains. The three appended native cases preserve all 21 earlier native cases; the two explicitly documented language-expectation migrations do not waive current-input, published-choice, stale, privacy, cancellation or duplicate-action checks.

## Actual final browser observations

These use **typed guide requests on the actual published page**, distinct from the native synthetic fixtures. Each result was observed in the visible UI after completion, including the pending fresh check before cart addition.

1. “Select a 16 inch solid gold necklace for me” sets 14k Solid Gold and 16 inch. Engraving stays unset, and the reply asks only for None or Engraved.
2. “No engraving then set quantity to two” selects the exact 14k Solid Gold/16 inch/None variant, quantity 2, USD 269 each, USD 538 subtotal, and enables addition.
3. “Add this piece to my cart then open my cart” completes the fresh check, adds that exact tuple/quantity once, and opens the actual cart route. All three cart option dropdowns plus the engraving preview field are present. Caption: “Your session-only test bag is open.”
4. “Take me to the homepage” returns the verified base catalogue URL and preserves bag count 2.
5. “Open my cart then remove the first item from my cart” produces an empty cart/count 0 and the exact-item removal caption.
6. One actual Talk to me attempt proceeds from Connecting your voice to “No microphone was found. Connect or enable a microphone, or type here.” Voice returns to disconnected. No fake browser media or event injection was used.
7. The guide reopens the actual Lowercase Initial Necklace deep link. The length and charm-size questions give the two short answers above. The bag is empty, and the useful detail tab remains open.

Earlier actual 46 e observations remain separately labelled: restored cart dropdowns/field appeared at 24 of 120 checked products; the natural wording edit updated the actual synthetic TEST 46 field and used the six-word acknowledgement; clear emptied it and used its own six-word acknowledgement; natural cart length/None and quantity edits changed the real fields and checked subtotals. One compound cart edit failed its fresh HTTP check with “The live selection could not be checked.” and no cart change. A new quantity request succeeded after inventory recovered. That failure is retained; its cause and live network reliability are not certified.

Verified browser URLs:
- https://brites-growth-sandbox.netlify.app/concierge-sandbox.html?product=lowercase-initial
- https://brites-growth-sandbox.netlify.app/concierge-sandbox.html
- https://brites-growth-sandbox.netlify.app/concierge-sandbox.html?cart=1

Proof: `Brites-Avatar-Cart46-Final-1791565142426.jpg` (actual final configured cart, all option menus, quantity, subtotal and friendly 2D face), and `Brites-Avatar-Size46-Final-1791565237128.jpg` (actual short size answer). Cart contents are session-local and the test item was removed after capture. No real order/payment was attempted.

## Remaining empirical acceptance

Physical microphone capture, real ASR recognition, live provider narration, audible completion/interruption recovery, GPU rendering, mobile touch behavior and live Shopify installation remain unverified. The native acceptance tests drive the actual production modules with fake media/transport/catalogue/ASR/provider events. Passing these fixtures and typed browser checks does not certify every one of the retained 64 acceptance definitions. Main, live Shopify, persistent user data and allocation/budget history remain intact.

A documentation-only publication follows this browser-tested source. Its final deployment must retain all 34 frozen source/test blobs and the exact 10 browser-tested function digests; the existing integration report records that final identity and complete historical evidence.
