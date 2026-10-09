# Avatar integration continuation48

Avatar continuation 48 — active controls and same-tab conversation
Updated 9 October 2026.

The shopper reported failed dropdown selections, pointing that did not identify the current control, forgotten follow-ups after navigation/restart, repeated greetings, and concern about Firebase/API cost. This continuation changes the sandbox host, native Shopify adapter, shared resolver, widget and native voice context. Main and the live Shopify theme are outside this publication.

What changed
- Actual option groups, the exact native selector and quantity inputs contribute their current identity. Mouse, focus and touch are supported. Entering the concierge preserves that identity; deliberate outside focus/click, a different section or a disconnected/replaced product retires it. Gaze position alone is never action authority.
- “Select 16 inches from this dropdown”, “sixteen”, and “set that to 16 inches” resolve against the actual current published menu. Exact full-necklace metal labels take precedence over similarly named charm-only labels. Explicit “without engraving” resolves a published No; a bare No still cancels.
- “What have I selected?”, “Which options have I chosen?” and current quantity questions use the current sanitized page controls, including partial choices, without calling product discovery or quoting historical choices as current. Quoted, conditional and different-product words cannot acquire action authority.
- Routine menu/selection replies are shorter. Complete choices and quantity changes retain the exact checked item subtotal; a small remaining non-metal group can offer its few choices. The visible page still displays the full published menu.
- Visual and spoken greetings happen once within the same tab. Restarting/reopening retains the conversation and re-reads the actual page. Historical messages are explicitly data, never a fresh request, purchase proof or permission.
- SPA navigation stays connected. For an interrupted same-tab reload/visibility transition, a voice resume intent lasts at most 120 seconds and restarts only with already-granted microphone permission. Cold/expired/future markers and prompt/denied permission do not start a microphone. End voice/dismiss/error clears the intent. Conversation restoration remains available when a manual Talk to me is needed.
- Public product choices, quantity, image and open menu restore through current product reads; saved prices/stock/private engraving are not restored as facts. A saved search restores its checked cards and successful-presentation cursor after inventory loading, then follows with unseen matches. Failed presentation never commits a cursor.

Memory and cost
Conversation memory uses sessionStorage in this browser tab: at most 1,024 user/assistant messages and 262,144 UTF-8 bytes, 32 relevant occasion/preference highlights, and 64 public page visits. Loading/trimming is linear. Older relevant context and recent history are retrieved locally; native history packets are capped at 6,000 bytes and deduplicated, and typed provider history stays within the existing six-message budget. Pointer movement does not cause a history API call or Firebase write. Page visits use a deduplicated, debounced local write. There is no paid summarization or new cloud history endpoint.

No new Firebase conversation-history writes were added. Existing inventory, voice allocation and quota operations still have their normal costs; voice context uses normal provider input tokens. Existing three-tool/turn limits, 120-second server call deadline, budget enforcement, cancellation, private-text handling, exact identity/options/price/stock checks and replay protection remain in place.

This is bounded same-tab memory, not an unlimited archive. Authenticated purchase history, a Firebase customer profile and cross-device/customer retrieval are not implemented here. Viewing or adding to a test bag never becomes a claim that the shopper bought a piece.

Verification
Final acceptance is bound to the 45-path avatar-integration-manifest48.json; all bindings match the frozen source. Fourteen sandbox staging copies match; both production build scripts and git diff --check pass. Among 234 previously hashed growth test/fixture files, only one preexisting greeting expectation changed from two greetings to one after reopening, matching the new user requirement. That test was retained; all other prior test bytes are unchanged. The new continuity fixture is a separate copy and does not alter earlier native fixtures. All original 64 acceptance definitions remain in the preserved report.

New native continuity: 21 cases. New adversarial continuity: 14 cases. The native event tests use the actual production client/widget/host/bridge with finalized synthetic ASR and simulated media/provider/catalogue data; typed input and chat fallback are disabled in those cases. The focused final log also includes the unchanged prior adversarial completion and purchase-flow cases: 98/98 pass, no skips. This round's checks were run by the primary agent; no new independent-agent review is claimed.

Final full suite: 5,259/5,259 pass, zero failures/cancellations/skips/todo, 226 test files, 102,351.223229 ms.
Full log round48e-full-verified.tap SHA256: d18e98cb28c07cc21771ea68a6d81689c6731773afdc3f338fef7751b0137f50
Focused final round48e-native-adversarial-final.log SHA256: 492adb8422596d6c066b17260e8b9c6b3103c185849840ccb7f0bfef18ae174e
Manifest SHA256: 382ca48f2e3f2d4d6ed8cc8f107f4b615d60428f2be17ce3691e70ff924d268a

The added current-selection/short-reply cases initially failed 0/2 against the first published continuation48 source; both now pass. The native-picker transit case initially failed 0/1 before its focus fix. Earlier full runs with failures or before the final source additions are excluded from final acceptance.

Earlier deployed browser checks
The previously published47 page reproduced “Select 16 inches from this dropdown” without selecting it. The first continuation48 deployment selected the published 16" chain length, 14k Solid Gold and No engraving at quantity two. Reload and close/reopen preserved all choices without another greeting. Its real Snake Monogram product remained blocked by the existing monogram/customizer restriction, rather than pretending a standard addition was completed.

On the first48 deployment, the initial animal batch returned six of 32 checked matches. After same-tab reload/current inventory loading, the six identities were restored. Show more returned six new identities with no overlap; live eligibility changed the remaining count to17. Show only earrings retained animal scope and returned six of12. These totals describe the currently checked inventory, not complete Shopify coverage.

Eating Otter Stud Earrings, a standard item, was selected in Sterling Silver at quantity2 and added after a fresh exact check: USD45 each, USD90 item subtotal. Opening the cart showed that exact line/quantity. A requested change to14k Gold Filled preserved quantity2: USD49 each, USD98 item subtotal. Quantity then changed to1 and the exact line was removed, leaving an empty test bag.

The browser identified one additional gap: the current-choice question went to discovery, and short native-picker wording could lose its focus before entering the conversation. Those gaps produced the final added fixes and native regressions. Final deployed UI verification of these fixes is recorded below.

Hardware and scope
A real Talk to me attempt returned “No microphone was found. Connect or enable a microphone, or type here.” A physical microphone, live ASR, audible mid-sentence recovery and physical mobile touch remain unverified. The observed avatar uses the 2-D fallback; real GPU/WebGL rendering is unverified. Typed browser success and synthetic native acceptance are distinct evidence. No actual order or payment occurred.

The prior canonical report's complete 98,227-byte version, SHA256 6b0ab8cfcb6e348a8c1241124a958b6117936768e1729d62e809ef49077916dd, is preserved verbatim after this addendum, including every historical constraint and all64 acceptance definitions.


Final deployed browser verification
Source commit: f42d7872925f0e006addd26d6fbbbd889c40b797
Source tree: 97f2923eec5c081ffd841dcb2ac873830c8fe515
Netlify source deploy: 6ac954156a59be0008e04583, ready 2026-10-09T20:54:04.066Z, no deployment error.
All 45 frozen source bindings match the published recursive Git tree. All ten deployed function digests match the first continuation48 deployment; the final patch changes the static host/widget and native regressions.

After the final source deployment, an actual click into the native exact-variant dropdown followed by the short typed “Sterling Silver” selected the real option. The first attempt during inventory loading was safely refused while availability was pending; after current availability finished loading, the same request succeeded. This is a checked retry, not a claim of immediate selection before stock verification.

Setting quantity to2 produced the checked USD90 subtotal. Closing and reopening the concierge retained Sterling Silver and quantity2 without another greeting. “What have I selected?” answered exactly “Metal Choice: Sterling Silver. Quantity 2.” from the actual current controls.

“Add it to my bag” added the exact standard product and quantity after revalidation. “Open my cart” displayed Eating Otter Stud Earrings, Sterling Silver, quantity2, USD90 item subtotal. The verification then removed this exact test line, confirmed an empty test bag, returned to the product and repeated the current-choice question successfully. No shopper purchase or payment was made.

Verified final browser URL: https://brites-growth-sandbox.netlify.app/concierge-sandbox.html?product=eating-otter-stud-earrings
The tab is left on this useful product view with the correct short current-selection answer. Final evidence is typed browser interaction. The native automated evidence does not certify a physical voice conversation.

Proof images (unaltered screenshots)
- Brites-Avatar-Dropdown48-Before-1791578010406.jpg — 87,778 bytes; SHA256 65b5180a05d002f62eebb9b02b15becf4b882959a702371ef5d2f443826b3ff5. Published47 failure to select16inches.
- Brites-Avatar-Dropdown48-Selected-1791578101168.jpg — 90,578 bytes; SHA256 84f0bf806e44a570c7d175a98b4e4cc858d207cb399def0472155492eb8cd1e8. Published48 full-necklace choices and quantity2; customization hold preserved.
- Brites-Avatar-Continuity48-Final-1791579447605.jpg — 109,515 bytes; SHA256 076fcdcafccef2edec4898defcdfad0aec71ae1056eee53ebd44f624a8a2c108. Final native selection and correct short reply after reopen.
- Brites-Avatar-Cart48-Final-1791579526548.jpg — 100,318 bytes; SHA256 fe1c89b282834520f47b830846527391f555c6d7a09aba312ce2a1dd73e69ba8. Final exact cart option, quantity2 and USD90 subtotal.

