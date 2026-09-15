# Brites ad layout ground rules

Every supported size has an explicit base in brites-ad-responsive.js. The renderer applies the base, and both the AI editor and responsive-design planner receive the same catalog and shared rules. These are a composition system, not a restriction to one background photograph.

## Fixed ground rules

- Keep the complete, exact jewelry recognizable; preserve all hardware and detail.
- Keep a tight safe crop and protect the jewelry from text, logos and fades.
- Treat the official icon and business name as one centered brand lockup.
- Stack brand, product name and action with deliberate spacing. Tall ads scale the brand, headline and action to the lower-third space, using 10–30px gaps instead of a small cramped cluster.
- Use a common button style with size-aware geometry: the standard is 34px high, up to 128px wide and a 14px label. Tall 300px ads use a 48px-high, up to 210px-wide button with a 20px label; narrow tall ads use 36px-high actions. The 580×400 side composition uses a 44px-high action and omits supporting copy. Very compact cards and banners omit a drawn button rather than crowding essential identity; the native Google action remains separate.
- Keep logo artwork visible: 32px icon in standard compact layouts and about 36px in a 50px banner; account for transparent padding. Essential banner copy is at least 14px for the business name and 16px for the product name. Narrow stacks may use a two-line 12px brand name beside the full icon.
- Ordinary previews are lossless, high-density renders of original assets and editable type. Exact fixed-size uploads and historical review snapshots are separate.

## Creative freedom

AI chooses product-specific settings, indoor/outdoor locations, complementary props, lighting, safe camera framing, supported messaging, coordinated colors, and up to two font families. It can omit optional copy when crowded. It must retain the base composition, product identity, legibility, brand unity and safe spacing. A fresh review is required after a meaningful design change.

## Static bases

| Size | Composition | Drawn action |
|---|---|---|
| 2048 × 2048 | product above a centered brand / headline / action stack | Yes |
| 2048 × 1072 | product beside a centered copy block | Yes |
| 1638 × 2048 | product above a centered brand / headline / action stack | Yes |
| 200 × 200 | product above a centered brand / headline / action stack | No; native Google action |
| 240 × 400 | product above a centered brand / headline / action stack | Yes |
| 250 × 250 | product above a centered brand / headline / action stack | No; native Google action |
| 250 × 360 | product above a centered brand / headline / action stack | Yes |
| 300 × 250 | product above a centered brand / headline / action stack | Yes |
| 336 × 280 | product above a centered brand / headline / action stack | Yes |
| 580 × 400 | product beside a centered copy block | Yes |
| 120 × 600 | product above a centered brand / headline / action stack | Yes |
| 160 × 600 | product above a centered brand / headline / action stack | Yes |
| 300 × 600 | product above a centered brand / headline / action stack | Yes |
| 300 × 1050 | product above a centered brand / headline / action stack | Yes |
| 468 × 60 | edge product / centered message / dedicated logo | No; native Google action |
| 728 × 90 | edge product / centered message / dedicated logo | No; native Google action |
| 930 × 180 | edge product / centered message / dedicated logo | No; native Google action |
| 970 × 90 | edge product / centered message / dedicated logo | No; native Google action |
| 970 × 250 | edge product / centered message / dedicated logo | No; native Google action |
| 980 × 120 | edge product / centered message / dedicated logo | No; native Google action |
| 300 × 50 | edge product / centered message / dedicated logo | No; native Google action |
| 320 × 50 | edge product / centered message / dedicated logo | No; native Google action |
| 320 × 100 | edge product / centered message / dedicated logo | No; native Google action |

The 970×250 banner uses a headline up to 64px, a 36px business name and a 120px icon. Optional copy is omitted before essential elements are reduced.

## Video bases

| Format | Export size | Composition |
|---|---|---|
| portrait | 720 × 1280 | product lower-middle, messaging above |
| square | 720 × 720 | product toward right, messaging in measured free space |
| landscape | 1280 × 720 | product toward right, messaging in measured free space |

All films retain the actual wordmark at the top-left, 148px wide, with the messaging set beside it rather than beneath it and separated by about 40px, leaving the height to the jewelry. Each film carries exactly one translucent fading field per edge, ramping evenly from the frame edge to fully transparent and absorbing any band join, so no duplicate fade or hard line can appear. Headlines are set one weight bolder than body copy. Only messaging transitions: opening hook, product/benefit, closing action. The three messages come from saved static-ad copy; they are not three repetitions of the same heading. Geometry and review checks protect the jewelry. Google approval and delivery remain separate from local creative quality.

Landscape masters center the complete brand, headline and action stack vertically, with a restrained button. Narrow tall photographs retain approximately 6% horizontal breathing room on each side of the located jewelry.

## Video composition fallbacks and targeted fixes

Full-canvas messaging is the first choice. When the measured jewelry leaves no clear zone, the renderer relaxes type sizes (portrait 48px, square 40px, landscape 44px minimum, four lines), then places the whole film beside a brand band in the ad's background colour (top band for portrait and landscape-sourced square films, left band for landscape and portrait-sourced square films). A film is never stopped for caption space; each fallback is recorded as a composition note for the complete-ad review and the Ad ratings pop-up.

Review findings name the exact rendered formats. From the Ad ratings pop-up each finding can be fixed on its own: captions re-composed, one message revised, or exactly one film master regenerated, with every other film reused as saved. Static findings regenerate one scene photograph or revise the plan without new images.

## Film direction

The advertised piece may never be altered. That rule outranks every other film instruction: no re-modelling in three dimensions, no rotation or turning, no edge, side or back the catalog reference does not show, no thickened or re-cut edges, no redrawn engraving, and no added bail, stone or chain. Camera ideas move through the scene, never around the jewelry, and a move that would require inventing an unseen part of the piece is replaced by holding the piece still and moving the scene.

Within that limit each film chooses its own approach and records it in the saved treatment: a dolly through foreground objects, a rack focus from a story prop, a lateral track past a piece that stays square to camera, a reveal as an occluder moves aside, a hand placing the piece, a held frame while daylight sweeps across it, or a tilt settling on it. A plain push-in, zoom or highlight pass is not an approach, and two pieces should not move the same way.

Films are bright and cheerful: open daylight, lively colour, never dark, dim, moody, overcast or gloomy. Where a band layout is used, its colour fades into the footage across roughly a seventh of the frame so the join reads as light falling away rather than a printed line.

Films are bright, sharp and richly coloured, with real specular life on the metal and no haze, fog, bloom or milky flatness. The background stays subordinate through framing, depth of field and contrast placement rather than by draining its colour. A worn shot is decided per piece: necklaces, earrings and bracelets may earn one brief worn moment when the evidence supports it, a charm sold alone usually does not, and most films carry no model. The complete-ad review deducts for a flat or hazy film and for camera work with no idea behind it.

## What the video model is never shown

The video model renders any text it is given. Colour codes, font names, pixel and layout specifications and the saved ad copy are therefore stripped in code before the film prompt is built, not merely discouraged in wording. The treatment is no longer handed the brand palette, the fonts or the layout spec either, and a treatment that mentions them anyway has them removed on the way through. The attached photograph is labelled as an identity reference and explicitly not the first frame.

Films are one continuous unbroken take: no cuts, jump cuts, dissolves, scene or lighting changes. The piece is complete, sharp and identical from the first frame to the last, and is never revealed feature by feature. The pieces are thin, flat, laser-cut sheet metal with engraving cut into the surface, so added thickness, a bevelled rim, an extruded body or engraving drawn on top all fail product identity in review. Lettering inside the footage, a cut between samples and an opening frame that differs from the rest are required layout defects.
