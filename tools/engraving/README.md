# Engraving assets and regression checks

The browser ships Source Sans 3 text plus monochrome Noto Emoji outlines. HarfBuzz shapes the Unicode 17 emoji sequences once during asset preparation; the browser uses those exact glyph IDs and advances without loading a shaping runtime. Preview, fitting, clearance checks and PDF/Illustrator output use the same paths. Glyph validation uses nonzero winding, matching the canvas and PDF writer.

The bundled map supports 5,151 qualified/unqualified emoji forms, including hearts, faces, skin tones, families, flags, keycaps and supported ZWJ combinations. It is **not universal Unicode coverage**: 74 forms from Unicode 17 are absent in this Noto Emoji build. They are listed in `vendor/fonts/emoji-sequences.json` and remain explicit review errors, including their codepoints; they are never silently dropped or replaced. Engraving is monochrome. The operator must still review legibility at physical size.

`build-emoji.py` requires Python fontTools and uharfbuzz. It downloads the pinned-by-SHA source font and Unicode data, instantiates weight 400, and generates the TTF and sequence map. If upstream files change, generation fails until the source change is reviewed. Font and Unicode licenses ship beside the assets.

Back files are keyed by the exact receipt/transaction/copy pool ID. Approval shows an immediate local preview, then writes verified files and transactionally attaches their metadata to a sheet that contains that pool ID. Re-nesting reconciles by placement, not by SKU or filenames. Back filenames include the pool ID and approval timestamp. Revocation prevents stale saves from resurrecting the old back. The front cutting file remains separate from the back engraving files; each back thumbnail links to its own `.ai` file.

Compound front artwork can contain closed decorative contours that resemble cut-outs. The review screen exposes a **Solid back** option for compound outlines, retaining separate cut holes. This is an explicit geometry interpretation for the operator to inspect; it does not silently fill potential physical holes. Fit errors are shown and edits rebuild the review canvas.

Library groups distinguish released numbered sets, working sheets, and standalone 10K/14K sheets. Isolated nesting does not select a material for release. The shared grouping rule also governs history and compact previews.

Checks: `tests/charm-nest/engraving-updates.cjs`, `functions.cjs`, `bridge-units.cjs`, `workspace.cjs`, `releases.cjs`, and `public-assets.cjs`. The export regression writes emoji/solid-back paths, reparses the result, and verifies the cut reference. The API regression covers mismatched copies, transfer between sheets, stale snapshots, revocation, reapproval, and isolated solids. Browser click testing on this release was unavailable because the browser connector timed out while listing tabs.
