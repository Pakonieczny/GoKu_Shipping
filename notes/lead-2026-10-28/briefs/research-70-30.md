# Design & Engrave: the 70/30 research (Illustrator + Photoshop features charm designers use)

Research agent, 28 Oct 2026. Read-only: repo `/home/user/goku_shipping` (main), studio sources J/S, and web sources [n] (list at the end).
**Honesty note:** no published survey gives per-feature usage for Illustrator or Photoshop. The UX Tools surveys rate whole apps, not their features [26], and
Adobe's 2025 user study has no published results. "How common" below is therefore inferred: it counts how often a feature turns up as a required step
in laser-cut / laser-engraved jewellery, keychain and charm tutorials, and in Glowforge/LightBurn/Epilog workflows. ★★★ = in nearly every such workflow,
★★ = frequent, ★ = occasional, ✗ = outside 70/30 for laser charms.

Workstream ids are from interfaces.md. **"ops?" / "text?" / "image?"** means I *presume* that workstream covers the feature, but no brief I could read
lists it. **The lead should confirm these with the owners.** MISSING = no workstream builds it.

---

## 1 · Feature matrix

### 1a · Illustrator (vector: cut lines and engraving art)

| Feature | Adobe name · shortcut (Win) | How common | Editor today (main) | Built by |
|---|---|---|---|---|
| Select / move / scale / rotate, Shift = ratio, Alt = from centre | Selection · V, Free Transform · E | ★★★ | yes (studio transform drags) | feedback (Ctrl-centre, live W/H, item 4) |
| Precise X/Y/W/H/angle fields | Transform panel · Shift+F8 | ★★★ | yes, but not live while dragging | feedback (items 1–2) |
| Smart guides / snap to grid / snap to point | Ctrl+U, Snap to Point | ★★★ | smart guides + mm grid snap; no snap-to-anchor | feedback; anchor snap → pen |
| Rulers, guides dragged off rulers | Ctrl+R, Ctrl+; | ★★ | plain mm rulers, no guides | rulers (item 3) |
| Zoom, zoom box, pan | Z, Ctrl+0/1, Space/H | ★★★ | yes 25–4000 %, fit/100/selection | rulers (zoom box, items 6–7) |
| Direct-select anchors and handles, add/delete/convert anchor | Direct Selection · A, + / − , Anchor Point · Shift+C | ★★★ (the core of "deforming lines") | no (Alt-click only reaches inside groups) | pen (9a) |
| Pen / curvature | Pen · P, Curvature · Shift+~ | ★★★ | no | pen (9a) |
| Rectangle / ellipse / line | M / L / \ | ★★★ | yes (R / E / L) | — |
| **Rounded rect, polygon, star, live corners** | Rounded Rectangle, Polygon, Star; Live Corners | ★★ (hearts, stars and rounded tags are staple charm blanks) | no | **MISSING** (the studio has round, ring, tri, star, heart, bail: J 9042) |
| Unite / Minus Front / Intersect / Exclude | Pathfinder · Shift+Ctrl+F9; Shape Builder · Shift+M | ★★★ (every keychain/charm tutorial unites the art with the outline) [1][2][5] | no | ops? (reuse `CharmNestVector.boolean`, charm-nest-vector.js) |
| **Offset Path** (grow/shrink any path; thicken thin parts) | Object › Path › Offset Path | ★★★ [2][3][4] | only as "Weight" on filled text/hatch | ops? (general offset); the contour-cut use is **MISSING** (see §3 #1) |
| Outline Stroke (stroke → filled shape) | Object › Path › Outline Stroke | ★★ (lines → engrave fills / cut shapes) | no | ops? |
| Compound path / even-odd holes | Ctrl+8 / Alt+Shift+Ctrl+8 | ★★ | `evenOdd` travels through; no command | ops? |
| Group, arrange, lock, hide | Ctrl+G, Ctrl+[ ], Ctrl+2, Ctrl+3 | ★★★ | yes (menu and layers; no Ctrl+2/3 keys) | — |
| Align / distribute / **align to artboard (= the charm)** | Align · Shift+F7 | ★★★ (centring text on the charm) | no buttons; smart guides only | ops? |
| Reflect (flip), mirror copy; rotate 90° | Reflect · O; Rotate · R | ★★ (draw half, reflect a copy, unite: symmetrical charms) | no (the studio has flipH/flipV, rot L/R: J 13479) | ops? |
| **Alt-drag duplicate; Transform Again; rotate copies about a centre** | Alt-drag; Ctrl+D; Rotate with Copy | ★★ | Cmd+D duplicates (studio); no Alt-drag | **MISSING** |
| **Join / close / average; Simplify** | Ctrl+J, Alt+Ctrl+J; Object › Path › Simplify | ★★ (open DXF segments cannot be filled or cut) | no | **MISSING** (pen or vimport should own it) |
| **Clipping mask** (show the art only inside the charm) | Ctrl+7 | ★★ (photo or pattern inside a shape) | no | **MISSING** (image/ops) |
| Blend between two shapes or paths | Blend · W, Make Blend · Alt+Ctrl+B | ★ (decorative step-lines, D3) | no | **owner unclear**: D3 asks for it; item 9 is split pen/warp. Confirm warp has it |
| Envelope / warp presets (arc, bulge, wave, flag, rise), free distort | Alt+Shift+Ctrl+W; Free Distort | ★★ (arched names) | no | warp (9b) |
| **Type on a path / text round a circle** | Type on a Path · Shift+T | ★★★ for round/coin charms [8] | no | **MISSING** (text?): warp "arc" bends the glyphs; it is not text on a circle |
| Font, size, weight, outlines on export | Character panel; Create Outlines · Shift+Ctrl+O | ★★★ | yes (outlines out, Weight, pt) | text (scripts, emoji 14–15) |
| **Tracking / letter spacing** | Character panel · Alt+←/→ | ★★★ (names on narrow bars) | no (model has `lineGap` only) | **MISSING** (text?) |
| Image Trace | Object › Image Trace (B/W logo, Shades of Gray) | ★★★ for handwriting and logos [6][7][15][17] | no | trace (12–13) |
| Place an image | File › Place · Shift+Ctrl+P | ★★★ | no | image (11) |
| Open .ai / .pdf / .svg / .dxf | File › Open | ★★★ | no | vimport (23) |
| Layers panel, rename, eye/lock | Layers · F7 | ★★★ | yes (front/back docks) | library (right-hand panel, 20): keep the layers reachable |
| **Outline view** (hairlines; finds doubled cut lines) | View › Outline · Ctrl+Y | ★★ in laser prep | no | **MISSING** |
| Cut / engrave by colour (red = cut, black = etch) | swatches | ★★★ [1][2] | yes, better: roles cut/line/hatch/art | — |
| Gradients, mesh, brushes, width tool, symbols, 3D, perspective, appearance stack, live paint, artboards | — | ✗ (a laser can do none of them; artboards = the front/back faces) | — | leave out (the 30) |

### 1b · Photoshop (raster prep before trace / engrave)

| Feature | Adobe name · shortcut | How common | Built by |
|---|---|---|---|
| Greyscale / Black & White | Mode › Grayscale; B&W · Alt+Shift+Ctrl+B; Desaturate · Shift+Ctrl+U | ★★★ [13][22] | trace `grey()` |
| Levels / Curves / Exposure (histogram stretch, gamma, S-curve) | Ctrl+L / Ctrl+M | ★★★ [13] | trace `grey()` brightness/contrast; **auto-levels and gamma: add** (§4) |
| Sharpen | Filter › Sharpen › Unsharp Mask (Amount 250, Radius 0.1–1.4, Threshold 0) | ★★★ [13][23] | **MISSING** (put it in `grey()`, §4) |
| Invert | Ctrl+I | ★★ (dark materials, negatives) [13] | trace `invert` |
| Threshold / Posterize | Image › Adjustments | ★★ | trace `levels` (1 = threshold) |
| Image size / resolution, bicubic enlarge | Alt+Ctrl+I | ★★★ [13][23] | image (placement at true mm size) |
| Crop | C | ★★★ | image? |
| **Remove background / magic eraser / eraser** | Remove Background, Magic Eraser · E | ★★ (white = no ink only once the background is gone) | **MISSING** (image?); the studio has a magic eraser and an eraser |
| Free transform of the placed image | Ctrl+T | ★★★ | existing transform + image |
| Bitmap (diffusion dither / halftone) | Image › Mode › Bitmap | ★★ for *photos* [23] | out of scope under D2 (vector); see §4 |
| Healing / clone / brushes / adjustment layers / masks / filters gallery | J, S, B … | ✗ for charms | leave out |

---

## 2 · What exists today

**Charm Editor (charm-nest-charm-editor.js, 2681 lines on main):**
- **Tools:** Select V, Marquee M, Text T, Shapes (Rect R, Ellipse E, Line L), Hand H/Space.
- **Prop bar:** X/Y/W/H with a proportions link and an angle field; Font, pt, Weight (a Clipper offset of filled shapes), Role (cut/line/hatch/art) and Line width.
- **More menu:** mm grid, snap to grid, smart guides (Alt pauses), zoom fit/100/selection, unlock all, show all.
- **Layers dock:** front and back, eye and lock, forward/backward, group/ungroup, delete.
- **Context menu:** cut, copy, paste, paste in place, duplicate, delete, the four arrange commands, group, ungroup, lock, hide.
- **Keys:** ⌘Z/⇧⌘Z/⌘Y, ⌘A, ⌘C/X/V/⇧V, ⌘D (duplicate), ⌘G/⇧⌘G, ⌘[ ] ⇧⌘[ ], ⌘0/1/2/+/−, ⌘S, arrows 0.05 mm (Shift 1 mm), Del, Enter (edit text), Esc.
- **Transforms:** Shift locks the ratio, Alt resizes or draws from the centre, Shift snaps a turn to 15°, Alt-click reaches inside a group.
- **Rendering:** SVG, crisp at 4000 %. Text comes back as outlines.
- **Not there:** pen and anchors, booleans, align buttons, flip, offset (beyond Weight), images, text-on-path, tracking, clipping, join, outline view, extra shapes.

**Website studio (J/S):**
- Rail: Select, Marquee ("cut out a piece"), Pen (*freehand* draw, not Bézier), Highlight, Shapes (line, arrow, rect, **rounded rect, ring, triangle, star, heart, bail**: J 9042), Words, Magic eraser, Eraser, More.
- Commands: **flipH/flipV, rotate 90° L/R**, front/back, duplicate (J 13440–13500).
- Align engine with smart spacing guides (J 16685–16957). Layers have opacity and rename. A searchable library (mkLib*). Metal choice, hoop (jump ring) toggle, Weight.
- Import with a line-art trace (J 14835 ff.): ink = luminance < 150, marching squares, then polyline simplify (RDP eps steps). There is **no Bézier fitting and no greys**. It refuses non-line art (more than 48 parts, or more than 30 % mid-tones) and keeps the bitmap.
- **Worth carrying over:** the shape set (heart, star, ring, bail), flip/rotate-90, the eraser/magic eraser, and the mid-tone test that tells line art from a photo (§4).

**Already in the repo for the agents to reuse:**
- `CharmNestVector.boolean/flatten/filled` (charm-nest-vector.js: Clipper booleans, curves within 0.002 mm) → ops, trace bands, clip-to-charm.
- `vendor/clipper-6.4.2.js` (offset, as Weight already uses it).
- charm-nest-dxf.js (373 lines) → vimport.
- charm-nest-emoji-picker.js / charm-nest-emoji-data.js → text.

**Keymap clash to decide (lead).** Illustrator users will press **M** for rectangle (here it is marquee), **L** for ellipse (here it is line), **⌘D** for transform again (here it is duplicate), **A** for direct select and **P** for pen. My recommendation:
- the pen workstream takes **P** and **A** (free today);
- add **O** (reflect), **Shift+M** (shape builder), **⌘7** (clip), **⌘8** (compound), **⌘J** (join), **⌘Y** (outline), **⌘2/⌘3** (lock/hide), **⌘R** (rulers), **⌘;** (guides), **⌘U** (smart guides), all free today;
- keep M/L/⌘D as the studio has them, since Paul chose the studio's look.

---

## 3 · MISSING, ranked (top 10 worth adding under 70/30)

1. **Contour cut / Offset Path.**
   - Select art → "Make cut outline" at N mm (default 0.8): outset, unite, fill the holes, round the joins → a new `cut` item. The art is kept.
   - The same dialog does a general Offset Path (in or out; round or miter joins) on any closed path.
   - This is *the* laser-cut charm step [2][3]. Clipper offset is already used for Weight.
2. **Bail / jump-ring hole.**
   - One command adds a Ø (default 1.5 mm) hole at top centre, keeping at least the minimum wall to the cut line (default 0.6 mm). If the hole falls outside the shape, it unites a round tab first.
   - Dragging the hole moves it along the outline.
   - [5]; the studio has "bail" and the hoop toggle.
3. **Text on a path / round a circle.**
   - Text runs along any path, or along an ellipse drawn on the spot.
   - Has a start offset and top/bottom (flip) placement for coin charms, with letters upright on the baseline [8].
   - Exports as outlines, like any other text.
4. **Shape presets.**
   - Rounded rectangle (with a corner-radius field), polygon (sides), star (points, inner %), heart, ring/donut (wall mm).
   - Live corner radius on any rectangle.
   - These are the charm blanks: copy the studio's set (J 9042, 10650–10680).
5. **Clip to charm / clipping mask (⌘7).**
   - Any art or traced image is shown and exported only inside a chosen closed path, by default the charm's cut line.
   - Nothing is engraved outside the cut.
   - Release with ⌥⌘7.
6. **Laser design check** (shown before Save and before Send to sheet; it does not block).
   - Flags open cut paths, bridges or walls thinner than X mm (default 0.4), and engraving outside the cut line.
   - Flags text with cap height under 0.8 mm, islands that will drop out, and doubled cut lines.
   - Each flag zooms to its spot. [3] and the fragile-hole reports in [5].
   - This protects D6 sanctity and cuts down errors, which feeds item 37.
7. **Tracking (letter spacing) and leading fields** for text.
   - In the prop bar (Alt+←/→ steps 0.05 mm, as Illustrator does), passed to `textToPaths` as `{ tracking }`.
   - Names on narrow bars need tightening every day.
8. **Alt-drag duplicate + Transform Again + radial repeat.**
   - Alt held on a drag start moves a copy. Alt-click without a drag still reaches into the group.
   - "Repeat last transform" (⇧⌘D, since ⌘D is duplicate).
   - "Rotate copies…" (n copies about the charm centre) for sunbursts and mandalas (LightBurn circular array [9]).
9. **Join / close / simplify paths.**
   - ⌘J joins the two picked endpoints, or closes a path within 0.05 mm.
   - "Close all near-gaps" runs automatically on DXF/AI import.
   - Simplify cuts nodes to a tolerance.
   - DXF from CAD arrives as loose segments that can neither be filled nor cut cleanly.
10. **Outline view (⌘Y).**
    - Every path is drawn as a 1 px hairline in its role colour, with no fills, to spot overlaps, doubled cut lines and stray points.
    - Cheap: a CSS class on `.ce-art`.

*Next after the top 10:*
- Remove background / magic eraser for images (copy the studio's; image).
- Crop image (image).
- Eraser = a vector knife or a "minus" brush (ops).
- Measure tool (distance and angle between two clicks).
- Lock/Hide keys ⌘2/⌘3.

*Confirm with owners (presumed, not verified):*
- ops: Pathfinder/Shape Builder, Align + **align to charm**, Reflect/rotate 90, Outline Stroke, Compound path.
- warp: Blend (D3).
- text: emoji + scripts are certain; text-on-path and tracking are not.

---

## 4 · Image → vector for laser-engraved greyscale  (for the **trace** and **image** agents)

**What the tools do:**

| Tool | Line art / logo | Greys | Photos |
|---|---|---|---|
| LightBurn [10][11] | Trace Image: Cutoff 0 / Threshold 128 (a brightness window); Ignore less than (despeckle, px); Smoothness 0–1.33 (0 = lines, 1.33 = all curves, about 1.0 is a good start); Optimize 0.2 (node merge); Sketch trace (edges from *local* contrast, for photos of handwriting with uneven light); Fade image; Show points | — (trace is one level) | raster modes: Threshold (2-tone only), Ordered, Atkinson, Stucki, **Jarvis** ("usually best for photos"), Halftone, Sketch, Grayscale (power varies); Pass-through for pre-dithered images |
| Glowforge [13][14] | trace on import | — | raster: **Convert to dots** (halftone, one power) or **Vary power** ("3D engrave": depth by power) |
| Illustrator [15][16][17] | Image Trace "Black and White Logo"; laser start: Paths max, Corners max, Noise min, then tune Threshold | **"Shades of Gray" with a Grays slider** (posterised vector greys; black = deepest, white = none) | (still vector posterise) |
| Inkscape [18] | Brightness cutoff | **Multiple scans: brightness steps / grays + "Stack scans"** (stacking removes gaps between levels), Remove background, Speckles, Smooth corners, Optimize | — |
| potrace [19] (the engine behind Inkscape and LightBurn-style traces) | defaults turdsize 2 (speckle px), alphamax 1.0 (corner threshold; 0 = polygon, 1.333 = no corners), opttolerance 0.2, curve optimisation on | — | — |
| The studio (J 14835) | luminance < 150, marching squares + RDP | — | refuses (more than 30 % mid-tones → keeps the bitmap) |

**Laser facts that drive the choices:**
- On a galvo/fibre laser each fill colour becomes a layer (pen) with its own power, speed and hatch [12][27]. So D2's "each grey keeps its own fill and the operator gives it a power" is exactly how operators already work.
- Users say fibre greyscale on stainless often comes out as plain black and white [20]. Use **few, well-separated levels** (3–4) on metal.
- Photo realism in LightBurn and Glowforge comes from **raster dithering at engrave time**, not from vectors. A dithered 20 × 20 mm charm at 0.05 mm pitch is about 160 k dots, which is unusable as vector paths.
- So under D2 a photo becomes a deliberate **posterised / stencil look**. That is fine for charms, but say it in the UI.
- If Paul later wants photo-real pendants, the route is a raster engrave layer that the laser software dithers. That is a separate decision, outside D2.

**Recommendations (they fit the `CharmNestTrace` contract):**

1. **Suggest the mode from the content.**
   - Compute the mid-tone fraction after auto-levels (the studio's test: share of pixels between ~20 % and 80 % grey).
   - Under ~0.15 → line art: suggest **1 level** (plain black).
   - Otherwise suggest **4** (illustration) or **5–6** (photo; warn "stylised").
   - Presets: *Handwriting / sketch*, *Logo (B/W)*, *Illustration (3 greys)*, *Photo (5 greys)*.
2. **`grey()` should do the Photoshop prep every laser guide does, in this order** [13][22][23]:
   1. Luminance grey (0.299 / 0.587 / 0.114).
   2. **Auto-levels**: clip ~0.5 % at each end of the histogram (Glowforge: frame the histogram with Levels).
   3. **Gamma** (centre the histogram), then brightness and contrast.
   4. **Unsharp mask** (default amount 1.5–2.5, radius ≈ 0.05 mm at working resolution, threshold 0).
   5. Optional 3×3 median denoise, which kills speckles before thresholding.
   6. Invert.
   - Add **"Adaptive (handwriting)"** = local-mean threshold (Bradley/Sauvola, window ≈ 1/8 of the image). It is the equivalent of LightBurn's Sketch trace, for phone photos of notes and signatures. Handwriting is one of the most-requested jewellery engravings [6].
3. **Level thresholds.** Use **multi-level Otsu / histogram quantiles** after auto-levels, not equal 256/N steps, so that every level holds real area.
   - Show draggable threshold ticks on a histogram strip (Illustrator's Threshold/Grays, LightBurn's Cutoff/Threshold window).
4. **Fixed grey palette, so a grey always means the same laser layer.** Band *k* of N (0 = darkest) gets fill g = k/N: for 4 levels that is 0, .25, .5, .75; white is never inked.
   - Then the operator's power table per grey is stable from job to job.
5. **Bands must share their boundaries exactly** (this is the biggest laser pitfall).
   - Traced independently, neighbouring levels leave hairline gaps (unengraved slivers) or overlaps (double burn). Inkscape has "Stack scans" for exactly this reason.
   - Do this instead:
     1. Trace the *nested* masks M_k = {pixel ≤ t_k}, each containing the darker ones.
     2. Make band_k = M_k minus M_{k−1}. Either use the inner trace as an even-odd hole of the outer trace, which keeps the curves, or use `CharmNestVector.boolean(…,'difference')`, which flattens to 0.002 mm.
     3. Drop slivers under the despeckle area.
   - Test it: the bands' areas sum to area(M_N) within 0.5 %, and the pairwise overlap is about 0.
6. **Fitting parameters (use potrace's proven defaults):**
   - `smooth` 0.5 ↔ alphamax 1.0. Map smooth 0..1 → alphamax 0..1.333.
   - `detail` ↔ opttolerance: 0.2 at detail 0.5, rising as detail falls.
   - `despeckle` should be in **mm² on the charm**, not px. A default of about 0.01–0.02 mm² (≈ a 0.1–0.15 mm dot) is my judgement: the smallest feature that reads on a small metal charm.
   - Keep the corners (logos and letters need them): alphamax ≤ 1.0 by default.
7. **Working resolution.**
   - Resample so the placed image is **~20 px/mm** (≈ 500 ppi; UMD uses 200 px/cm [23]), with bicubic upsampling for small sources (Glowforge's "Bicubic Smoother (enlargement)", ≥ 300 ppi [13]).
   - Cap the long side at ~2048 px in the worker.
   - Fibre hatch pitch is ~0.02–0.05 mm, so finer tracing buys nothing.
8. **Node budget.** Show the node count (LightBurn "Show points").
   - Warn at about 5 k anchors per level. The studio caps its budget at 11 k numbers per trace.
   - Optimize merges curves before warning.
9. **UX** (the "no waiting" rule):
   - A low-res live preview within ~100 ms of each slider change.
   - The full trace runs in the Worker with progress and Cancel.
   - Overlay on the faded source (LightBurn's Fade image), plus a before/after toggle.
   - The result lands as one group (one child per grey) sized to the placed image and clipped to the charm (see §3 #5).
10. **Background first** (image agent). White = no ink only works once the background is white.
    - Offer the studio's magic eraser (flood from a click, with tolerance) and a crop before the trace. Remove-background is the step Photoshop users do first.
11. **Out of scope by design:** dithering or halftone as vectors (too many paths), centreline/single-stroke trace (outline fill is the norm for engraving [6]), colour trace.

---

## Sources
[1] BMCC Makerspace, Laser-etched keychains/pins (Illustrator: Pathfinder Unite, red cut / black etch) https://openlab.bmcc.cuny.edu/makerspace/laser-keychains-pins
[2] UVA Scholars' Lab, Outline-cut keychain (offset the outline, unite, stroke red to cut) https://scholarslab.lib.virginia.edu/assets/post-media/makerspace/OutlineCutKeychain.pdf
[3] Epilog, Expanding thin shapes for laser cutting (Offset Path 0.02 in so thin parts don't break) https://support.epiloglaser.com/article/ka-01253
[4] Logos by Nick, Offset a path in Illustrator https://logosbynick.com/offset-a-path-in-illustrator/
[5] Jump-ring holes: Silhouette School https://www.silhouetteschoolblog.com/2024/12/from-svg-design-to-laser-cut-earrings.html · xTool https://projects.xtool.com/howtos/8433 · Glowforge "fragile hole" https://community.glowforge.com/t/lotus-shippo-earrings/96264
[6] Stones & Findings, Engrave handwriting on silver jewelry (trace to outline vector; handwriting a top request) https://stonesandfindings.com/how-to-engrave-handwriting-on-silver-jewelry-xtool-f1-ultra-orion-mpulse/
[7] xTool project, handwriting heart necklace (Illustrator + XCS) https://projects.xtool.com/project/28658
[8] Type on a path: Tutsplus https://design.tutsplus.com/tutorials/working-with-type-on-a-path-in-illustrator--cms-20711 · Font Bundles https://fontbundles.net/design-school/how-to-type-on-path-in-illustrator
[9] LightBurn Boolean / Modifiers / Tools (offset, weld, arrays) https://docs.lightburnsoftware.com/2.1/Reference/BooleanTools/ · https://docs.lightburnsoftware.com/2.1/Reference/UI/ModifiersToolbar/ · https://docs.lightburnsoftware.com/2.1/Reference/UI/ToolsMenu/
[10] LightBurn Trace Image (2.0) https://docs.lightburnsoftware.com/2.0/Reference/TraceImage/
[11] LightBurn image modes https://docs.lightburnsoftware.com/UI/CutSettings/CutSettings-Image.html
[12] LightBurn, EZCAD hatch translation (sub-layers = EZCAD hatches) https://docs.lightburnsoftware.com/2.1/Explainers/EZCADHatchQuickTranslation/
[13] Glowforge, Prep a photo engrave in Photoshop https://support.glowforge.com/hc/en-us/articles/360033633534-Prep-a-Photo-Engrave-in-Photoshop
[14] Glowforge community, Vary power vs Convert to dots https://community.glowforge.com/t/newbie-question-illustrations-vary-power-vs-convert-to-dots/110076 · HD vs 3D https://community.glowforge.com/t/engraving-hd-vs-3d/85408
[15] Georgetown Maker Hub laser design tips (Image Trace Shades of Gray: black deepest, white none) https://library.georgetown.edu/makerhub/lasercutter/designtips
[16] Adobe, Image Trace options https://helpx.adobe.com/illustrator/using/tracing-artwork-live-trace-or.html · presets https://helpx.adobe.com/illustrator/using/image-trace-presets.html
[17] MRU Library, Image Trace for the laser (Paths max, Corners max, Noise min) https://libraryhelp.mtroyal.ca/tech/faq/204445
[18] Inkscape Trace Bitmap, multiple scans / stack https://logosbynick.com/trace-bitmap-in-inkscape/ · https://docs.huihoo.com/inkscape/guide-2nd/html/Trace-Multi.html
[19] potrace man page (turdsize 2, alphamax 1, opttolerance 0.2) https://manpages.opensuse.org/Leap-15.6/potrace/potrace.1.en.html
[20] LightBurn forum, Photo engraving on stainless in greyscale https://forum.lightburnsoftware.com/t/photo-engraving-on-stainless-steel-in-grayscale/166249
[21] LightBurn forum, Greyscale vs dithering choices https://forum.lightburnsoftware.com/t/grayscale-vs-dithering-choices/43897
[22] Davidson, Preparing an image for laser engraving https://support.ti.davidson.edu/hc/en-us/articles/28056069658263-Preparing-an-Image-for-Laser-Engraving
[23] UMD, Laser engraving a photograph (200 px/cm, bitmap mode) https://sandbox.umd.edu/wiki/index.php/Laser_Engraving_a_Photograph
[24] Noble Desktop, 5 Illustrator tools for logos (Shape Builder, Direct Selection, Pen) https://nobledesktop.com/learn/illustrator/5-tools-for-designing-logos-in-illustrator
[25] Tutsplus, 10 Illustrator tools every designer should use https://design.tutsplus.com/tutorials/10-illustrator-tools-every-designer-should-be-using--cms-25968
[26] UX Tools design-tools survey (app-level only, no feature usage) https://uxtools.co/survey-2021/
[27] LightBurn forum, different power per layer/colour https://forum.lightburnsoftware.com/t/how-can-i-engrave-2-different-power-levels-in-the-same-pass/15663
Shortcuts are Illustrator/Photoshop defaults (Windows; ⌘ for Ctrl on Mac). The Adobe shortcut help page returned 403 to the fetcher, so they are quoted from the standard default set.
