# Researched charm stories, Round 52

The library gives a checked current piece a short, sourced story and an optional personal connection. It uses Firestore as the authority. These are agent-researched records; they are not human-approved product dossiers, and they do not change stock, pricing, matching sets, selections or cart authority.

## Storage and release

Records live only in `Brites_Growth_Sandbox_CharmStories`. The browser module validates and connects fetched rows; it has no embedded research catalogue and writes no library data to browser storage. The server's private bootstrap JSON is an operator write payload, never a shopper read fallback.

Use the existing signed-in operator dashboard's **Check charm library** and **Save researched charm library to Firebase** controls, or the established authenticated growth request helper:

1. `GET /api/growth/meaning-library` reads the bounded cloud records and their versions.
2. `POST /api/growth/meaning-library` with `{"action":"bootstrap"}` creates missing records from the 28 inspected definitions. It never overwrites an existing cloud record or creates a research approval.
3. To edit, send `{"action":"save","record":{...},"expectedVersion":"CURRENT_SHA256"}`. A new record uses `expectedVersion:null`; a competing or stale edit is rejected.

No new credentials, authentication session, SDK or dependency is introduced. Mutations use the existing operator authorization and the existing server Firestore connection. Writes and reads are limited to the sandbox namespace. Bootstrap inspection dates expire after 30 days; running bootstrap again cannot renew them. Updating a source inspection date requires inspecting that source again and saving the record through the existing operator flow.

The cloud status read is bounded at 96 records. Shopper reads bind at most 20 exact products and at most two title-matched stories per product. Missing, inactive, expired, invalid or unavailable cloud research yields an honest current published-title detail instead of bundled research. A product with any current meaning, recommendation or cart hold receives neither a researched story nor a fallback story.

## Public contract

`GET /api/growth/knowledge?ids=...` preserves the existing human-approved `products` array and adds `stories`, `storyLibraryAvailable` and the read-completion `checkedAt`. IDs are exact Shopify product GIDs, bounded at 20. The server re-reads each current product through its exact handle; a stored catalogue row supplies an identity hint only. It checks current issues again after reading the cloud library. `GET /api/growth/product?handle=...` also returns a completion timestamp after the product read, mirror save and issue checks; it does not renew the underlying product timestamp.

An exact-current meaning request through `/api/concierge` can return `story`, `storyConnection` and `meaningConnection`. Existing human-approved interpretations retain precedence. Story responses preserve selection and carry no new shopping action. The guide accepts fetched rows through `setStories(rows)` and exposes the same connection from `prepare(context)`.

A story row contains:

```text
schema: 1
kind: researched-story | published-detail
provenance: agent_researched | published_product
productId, handle, productUrl, productTitle
productCheckedAt, checkedAt
libraryId, libraryVersion, motif
context
facts: [{text, sourceIds}]
interpretation: {text, context, optional:true} | null
sources: [{id, title, url, publisher, checkedAt, inspection}]
```

The exact ID, handle, URL and title must agree with the current product. The public motif is the longest actual title-matched alias; a general flower record can bind a title naming a rose without inventing a fixed rose meaning. Descriptions, option names and suggested-product tags cannot supply a missing motif. Contradictory known title and handle/tag motifs fail conservatively to a published detail. Duplicate product IDs or handles refuse both conflicting identities.

Both the current product and cloud read must be no older than five minutes. Research source inspections must be no older than 30 days. Fetching, copying, bootstrapping or rendering a record never changes its source inspection dates. Published-detail evidence uses the current product's original check time. Delays and newly raised holds are checked at completion.

`storyConnection` retains every bounded fact, source and cultural qualifier as metadata. Its default reply uses one fact, a plain qualifier and an optional shopper connection. Recipient, occasion and reason come only from the existing explicit, redacted shopper context. A shopper's own reason is not presented as a historical tradition or an emotional guarantee. There is no inferred belief, clinical claim or promised outcome.

An explicit meaning-detail request such as **Tell me more about its meaning**, **Give me the full story of this charm**, or **Go deeper into its history** uses the additional stored facts, cultural context and optional interpretation. The pure `meaningDetailRequest(message)` helper retains the existing quoted/private/hypothetical/negation guards. `Guide.suggest` and the exact-current server route infer this from the current message; the widget may use `Guide.prepare(context,{expandedMeaning:true})` after the same explicit check. The shared renderer is `storyConnection(story,personal,{expanded:true})`, and the returned connection declares `detail:'expanded'` or `detail:'brief'`. This is a read-only presentation flag, never a website action. Source dates, exact product binding, holds and five-minute freshness remain identical in both views. Unknown designs keep the bounded published-detail fallback; asking for more cannot manufacture research.

The guide resolves a late-loaded `window.BritesCharmStoryLibrary` when `setStories` or `prepare` is called. Before that validator exists, `setStories` returns zero and accepts no unchecked rows; callers can retry the checked rows after loading the dependency.

## Research coverage and qualifications

The investigation used the connected merchant's published catalogue and public theme collections, followed by primary museum, botanical, zoological and astronomical sources. The connected alphabetical first-page read covered 50 active listings; it was not an exhaustive or ranked catalogue audit. It exposed concrete naming conflicts, including Dainty Bunny's necklace title versus an earrings description and Airplane Beady's rocket handle/tags. The library does not resolve those contradictions by borrowing a meaning or changing jewellery type.

The existing 120-piece starter algorithm covers regular necklaces, beady necklaces, stud earrings, hoop earrings and charm-only listings. The new automated coverage case uses 120 independently declared fixtures through that actual seed reader, then exact story reads in bounded batches. This is a local contract proof, not a claim that 120 live pieces were freshly checked. Every eligible runtime piece receives exact-motif research or a clearly limited published-detail fallback. Unknown hobbies, fictional characters, runes and unresearched designs receive no invented symbolic history.

The bootstrap records below contain paraphrases, with the original inspection timestamps retained in each source. All interpretations are optional personal readings. A cultural association is limited to the cited place, period and object; biological and astronomical facts do not themselves establish a universal symbolic meaning.

| Motif | Scope of the sourced story | Primary evidence |
| --- | --- | --- |
| Butterfly | Biological metamorphosis; separately, a seventeenth-century Chinese jade object and its qualified associations | [Smithsonian](https://naturalhistory.si.edu/education/teaching-resources/life-science/butterflies-and-beyond), [Met jade butterfly](https://www.metmuseum.org/art/collection/search/43763) |
| Sea otter | Sea-otter ecology and tool use; no claim about river otters or romantic fidelity | [Monterey Bay Aquarium](https://www.montereybayaquarium.org/animals-the-ocean/animals-a-to-z/sea-otter) |
| Lotus | An ancient Egyptian water-lily object; explicitly distinguishes that flower from modern botanical lotus | [Met Egyptian object](https://www.metmuseum.org/art/collection/search/545285) |
| North Star | Northern-hemisphere orientation using Polaris; not the brightest star or a southern navigation guarantee | [NASA](https://science.nasa.gov/solar-system/what-is-the-north-star-and-how-do-you-find-it/) |
| Moon | Reflected sunlight and changing visible phases | [NASA](https://science.nasa.gov/moon/moon-phases/) |
| Dragon | Chinese Qing-period object and water/court context; not a universal dragon interpretation | [Met](https://www.metmuseum.org/art/collection/search/42364) |
| Owl | Athena and an ancient Greek coin/object context | [Met](https://www.metmuseum.org/art/collection/search/254648) |
| Acorn | White-oak seeds and wildlife ecology | [US Forest Service](https://research.fs.usda.gov/silvics/white-oak) |
| Daisy | A flower head made of smaller flowers | [Kew](https://www.kew.org/read-and-watch/identify-plants-street) |
| Elephant | Asian-elephant family-group biology, qualified by species | [Smithsonian National Zoo](https://nationalzoo.si.edu/animals/asian-elephant) |
| Dragonfly | Aquatic young and adult development; no butterfly-like pupal stage | [Natural History Museum](https://www.nhm.ac.uk/discover/dragonflies-the-ultimate-hunters.html) |
| Bee | Pollination; separately, a specific 1893 wedding-jewellery example | [USDA](https://www.nrcs.usda.gov/conservation-basics/animals/insects-pollinators), [V&A](https://www.vam.ac.uk/articles/brooches-for-bridesmaids) |
| Guitar | A late-eighteenth-century Neapolitan guitar and instrument development | [Met](https://www.metmuseum.org/art/collection/search/503932) |
| Anchor | A circa-1800 brooch using a specific Hope association | [Royal Museums Greenwich](https://www.rmg.co.uk/collections/objects/rmgc-object-42045) |
| Compass | A historical magnetic compass and navigation | [Royal Museums Greenwich](https://www.rmg.co.uk/collections/objects/rmgc-object-42595) |
| Heart | A specific Victorian wedding-gift object | [V&A](https://www.vam.ac.uk/articles/brooches-for-bridesmaids) |
| Flower | Qualified nineteenth-century European floral jewellery; no fixed meaning invented for each species alias | [V&A](https://www.vam.ac.uk/articles/a-history-of-jewellery) |
| Cross | A medieval German reliquary context; no assumed belief of a shopper or recipient | [V&A](https://www.vam.ac.uk/articles/a-history-of-jewellery) |
| Peacock | A circa-1900 designer's documented preference, not a universal cultural claim | [V&A](https://www.vam.ac.uk/articles/a-history-of-jewellery) |
| Hummingbird | Rufous-hummingbird migration, qualified by species | [Audubon](https://www.audubon.org/field-guide/bird/rufous-hummingbird) |
| Sea turtle | Green-turtle ecology, qualified by species | [Monterey Bay Aquarium](https://www.montereybayaquarium.org/animals-the-ocean/animals-a-to-z/green-turtle) |
| Swallow | Barn-swallow biology, qualified by species | [Cornell Lab of Ornithology](https://www.allaboutbirds.org/guide/barn_swallow) |
| Snake | Biological skin shedding, without treatment or healing claims | [Smithsonian National Zoo](https://nationalzoo.si.edu/animals/news/do-snakes-have-ears-and-other-sensational-serpent-questions) |
| Alligator | Alligator anatomy and the distinction from crocodiles | [Smithsonian National Zoo](https://www.nationalzoo.si.edu/animals/news/how-long-can-alligator-hold-its-breath-and-other-questions-answered) |
| Sand dollar | Living-animal anatomy rather than unverified folklore | [Monterey Bay Aquarium](https://www.montereybayaquarium.org/animals-the-ocean/animals-a-to-z/sand-dollar) |
| Sea star | Marine-invertebrate anatomy; never an astronomical star story | [Smithsonian Ocean](https://ocean.si.edu/ocean-life/invertebrates/sea-stars-urchins-and-relatives) |
| Sun | The Sun as a star and its energy | [NASA](https://science.nasa.gov/sun/facts/) |
| Star | Qualified stellar physics; a generic star does not inherit Polaris navigation | [NASA](https://science.nasa.gov/universe/stars/) |

## Verification scope

`tests/growth/charm-story-library52.test.cjs` exercises the real shared validator, Firestore service, guide, Growth service and API wrapper. It covers persistence across instances, bootstrap non-overwrite, stale writers, missing/disabled/unavailable cloud records, source expiry during reads, product identity drift, current holds, conflicting identities, malicious records, private data, exact fallback coverage, approved-dossier precedence, source dates, brief replies and API authorization/projection. The parent runs all tests centrally; this document does not claim a test result or a live seed until that evidence exists.

Live verification must show the authenticated bootstrap receipt, a subsequent cloud status read, a fresh exact-product knowledge response with the same library version, and a public story whose original source dates remain unchanged. Operator credentials, account tokens, notes, engraving, design briefs, customer contact data and private stored dossiers must remain absent from public story rows, DOM, snapshots and action receipts.
