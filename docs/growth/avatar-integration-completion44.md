# Brites avatar integration continuation 44

This continuation resumes the October 8 website-avatar integration work from sandbox branch `codex/brites-growth-2026-10`, parent `5defbbd771045294b9246527c70c09a2037fae0d`. The [64-point acceptance contract](avatar-integration-acceptance43.md), [previous release evidence](avatar-integration-release43.md), and [shopping-flow research](shopper-flow-research43.md) remain the detailed scope. Historical browser observations in those files retain their original source/deploy labels.

Five specialists and a separate adversarial reviewer reconciled the requirements. The new review reproduced discovery, factual-awareness, commerce and recommendation defects that were not covered by the prior passing suite. The existing friendly thinking rig and fallback remain unchanged.

## Corrections

| Requirement IDs | Reproduced problem | Resulting behavior |
| --- | --- | --- |
| D04, D05, D07 | “Show animal jewelry without necklaces” selected a positive necklace filter; material refinements could lose exclusions. | Excluded categories survive serialization and subsequent refinements. Negative categories do not become positive host filters. Ordinary Necklace Charm listings retain their actual charm category. |
| D05, D07, D14 | Multiple requested jewellery categories could collapse to one host filter or an AND condition. | Categories retain their OR relationship within the complete checked search. Literal options and variants remain intact. |
| D08, D09 | Karat-only `18K Gold` was treated as solid gold without published construction evidence. | Explicit solid requests require explicit solid construction on the same available, currency- and budget-matching variant. Plain gold remains searchable without relabelling it. |
| A02 | A chain length or a different measurement axis could answer a charm-width question. | One shared browser/server helper binds published measurements to the requested component and axis, separates mixed clauses, labels actual size options, and admits missing physical measurements. |
| A01, A03, A05, A08, A09 | A delayed factual response could outlive a manual route/variant/quantity change or a price, stock or hold refresh. | Pending factual answers retire when the relevant public page or commercial facts change. Pointer movement and unchanged public refreshes retain the pending response. |
| A05, A11, R02 | Held products could retain verified commercial envelopes or selected subtotals in native context. | Clearly labelled descriptive identity may remain under review; verified price, stock, selected commercial tuples and subtotal authority retire. Checked unavailability stays truthful factual information without purchase authority. |
| P02, P03, P04, D14 | A discovery-card chooser used a separate saved-options UI instead of the connected host's actual choices. | Connected cards open the actual host product and relevant native menu without implicitly selecting a variant. Standalone restored-card behavior retains separate regression coverage. |
| P09, P10, P11 | A standalone saved selection could survive same-ID option drift, incomplete fresh variants or refreshed holds. | Addition requires an exact fresh signature, identity, title, options, completeness and hold status. Production review cancellation clears prepared authority. |
| P09, P11, P17 | Review authority could persist after closing or changing choices; a full bag could discard earlier lines or leave a ready message. | Closing or changing a review retires it. A 50-line bag refuses a new addition, preserves all earlier lines, and displays the returned failure. |
| R01–R06, P18 | Repeated unchanged context replaced focused recommendation DOM; refreshes could retain expired or newly ineligible suggestions. | Unchanged public context preserves the focused controls. Refreshes revalidate the original cheaper, smaller or matching request against fresh candidates and withdraw stale, unavailable or repriced suggestions. Dismissal persists until explicit revival. |
| R10 | Backend staging did not previously need the neutral measurement module. | The isolated package now includes the same shared module at its backend require path, with a cold-require check. |

## Frozen validation

The frozen manifest covers all 17 changed executable and test files. SHA-256 of the manifest: `33013db821f15d0ded16c3159a62ba4e29410dda6f732b26bc0547d2b7fb4e7b`. The independent reviewer recomputed all 17 hashes and found zero drift.

| Check | Result | Scope |
| --- | --- | --- |
| Full growth suite | **4,831/4,831 passed**, zero failed, cancelled, skipped or todo; 93,769.907 ms | Actual production modules and host/bridge/widget fixtures; network and hardware substitutes remain labelled in tests. |
| Independent adversarial cases | **31/31 passed**, zero failed, cancelled, skipped or todo; 10,694 ms | Independently authored literal discovery, dimensions, hold, stale response, recommendation focus and both option-confirmation paths. Included in the full growth suite, not an additional total. |
| Same adversarial cases against the recovered prior source | **4 passed / 27 failed** | Establishes that the new counterexamples expose substantive prior defects. This is historical failing-source evidence. |
| Relevant Ads/Data Manager regression files | **5/5 passed**, zero failed, cancelled or skipped | Unrelated executable paths preserved. |
| Configured root build | Passed | 160 public assets plus 3 optional fonts; 176 callable functions. |
| Isolated sandbox build | Passed | Exactly 10 callable endpoints, 21 reported bridge assets, 65 server modules. |
| Source/staging audit | Passed | 54 sandbox public copies match their source bytes; 6 changed root public copies and 3 shared/server copies match. Generated sandbox index and protected Ads HTML retain their intentional transforms. |
| Staged backend cold require | Passed | Shared component-bound measurements resolve at the deployed backend path. |
| Syntax/diff checks | Passed | Changed modules checked by owners; root `git diff --check` clean. |
| Avatar scene | Unchanged | 627,689 bytes; SHA-256 `8f7638767bcbbe5e37e95969da7131c963d419a2447c0d0cc5dc9dbada515b09`. |

The initial full continuation run was 4,823/4,828. Five older checks encoded superseded assumptions: two inferred solid construction from `18K Gold`; two dimension fixtures did not load the real neutral shopping-guide dependency; one expected verified commercial facts for a held product. Those fixtures and expectations were corrected with additional positive and negative behavioral assertions. Three new material cases brought the final total to 4,831. The independent reviewer audited all corrections and found no authority, identity, completeness, freshness or no-write assertion weakened.

The eight restored-card option-completeness fixtures now explicitly mount the standalone scope they exercise. Their old literal assertions remain. Connected 250-saved-option versus one-actual-host-option coverage independently checks that the host remains authoritative and unselected.

## Publication and remaining acceptance

The tested executable source is `df69d0993a9e0be05c766d3b860e6588229504b3`, tree `56de767f1fcf860cd7f0eb507a787bb1410e4a2a`. Its 17 changed files match the frozen local source/test Git blob hashes exactly, and every other remote baseline blob is preserved. Netlify deploy `6ac83935e881eb000890265f` is ready, names that exact commit and authorized branch, has null error, and was published at `2026-10-09T00:46:16.742Z` with all 10 sandbox functions.

Publication is restricted to the existing isolated sandbox and authorized branch. No main-branch change or live Shopify theme installation is part of this release. This evidence/documentation commit contains no executable changes; a subsequent deployment must retain the same tested executable bytes.

### Actual published-browser observations

All observations below use the exact ready executable deployment above. They are actual UI interactions with public product responses, distinct from the network-substituted fixtures. The browser renders the real SVG fallback; physical speech and GPU output were not exercised.

| Request or action | Observed result |
| --- | --- |
| Reload and open collection | Existing session bag 2 restored. Readiness progressed from 96 checked identities to the explicit 120-ready state. Later freshness drops were disclosed as partial/preparing and subsequently returned to 120; no stale full-readiness claim was used. |
| Show animal jewelry without necklaces | Native filter remained All pieces rather than All necklaces. Six displayed entries were actual charms, including Necklace Charm titles; their published category remains Charm. |
| Show animal earrings without necklaces | Six checked earrings: Flying Eagle, Eating Otter, Butterfly Cutout, Sea Otter Charm, Chinese Dragon and Gecko Lizard. Native All earrings was selected, and the excluded necklace constraint remained in the search. |
| What about gold filled? | Search became `show animal earrings gold filled without necklaces`; the same animal/type/exclusion scope remained, with USD 49/52 qualifying material prices. |
| Show butterfly earrings or charms without necklaces | Both Butterfly Wing Cutout Charm and Butterfly Cutout Stud Earrings appeared. Native All pieces allowed the category OR; the exclusion remained. The prior explicit gold-filled preference remained visible in the search. |
| Click the earring discovery-card Choose options | Opened Butterfly's actual host listing and visible Metal Choice menu with all five literal choices. Combined select stayed “Select an option…”, all choice chips were unchecked, quantity 1, and Add disabled. |
| What are the dimensions of this piece? | Exact Butterfly title and honest unpublished-measurements answer; no metal values or fabricated physical size. |
| Help me choose | Opened the actual Metal Choice menu. Retained gold-filled preference grounded a USD 52 available material suggestion; no variant was selected automatically. |
| Manual Sterling Silver, quantity 2; What is the price of this piece? | Native selection and guide both reported USD 48 each / USD 96 item subtotal, quantity 2. |
| Add this exact piece to my cart | Fresh check produced the exact Silver quantity-2 / USD 96 receipt. Existing bag 2 became 4, preserving the earlier line. |
| Take me to my cart | Opened the actual session bag with two separate Silver lines, each quantity 2 / USD 96, total USD 192. |
| Change only the second actual bag input to 3, then Undo my last change | First line stayed quantity 2 / USD 96; second became quantity 3 / USD 144 and bag 5 / USD 240. Undo restored exactly the edited line to 2, bag 4 / USD 192. |
| Open checkout; four review/shipping/express/confirmation steps | Fresh checks reached the labelled simulation. Both exact quantity-2 / USD 96 lines remained, and Demo express was selected without a live shipping promise. |
| Complete test checkout command | Only exposed the acknowledgement requirement. Checkbox remained unchecked and completion disabled. |
| Actual acknowledgement checkbox and actual Complete test checkout button | Produced visible TEST COMPLETE and local completion receipt; bag 4 retained. No order, payment, message, contact or address details were submitted. |
| Open Sea Otter Necklace Charm; What am I looking at? | Current exact Sea Otter identity and published description replaced the earlier butterfly context. Charm format was explicit; chain/earrings were not assumed included. |
| Find matching earrings | Eating Otter USD 49 and Sea Otter Charm Stud USD 52, both 14k Gold Filled and separately sold, used the current otter motif despite earlier butterfly interest. Current Sea Otter remained unselected, quantity 1, bag 4. |

The final captured screenshots show the actual TEST COMPLETE receipt and the current Sea Otter product beside the friendly fallback and matching recommendation. They are delivered with the user handoff. Browser elapsed tool times include automation overhead and are not native service-latency measurements.

All 64 acceptance rows have source/fixture reconciliation; this is not a claim of empirical completion of all 64. Physical microphone/recognition/audible output/interruption, physical mobile touch, GPU appearance/shadows/FPS, human aesthetic approval, and live Shopify theme/cart/checkout remain unverified. Some races, privacy, mixed-currency and stale-data cases are fixture evidence only. The shopping checkout is explicitly a test simulation and creates no live order or payment.

The final user handoff retains the source/deployment identities, literal browser actions, screenshots, all 64 detailed acceptance definitions, and the physical/live limitations above.
