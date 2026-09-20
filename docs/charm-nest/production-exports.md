# Sheet exports

Nest, Sets history and Library share one production exporter. Library supports per-sheet AI/DXF buttons, group selection, and cross-group checkbox selection. Multiple sheets download as a ZIP with a sheet manifest. Exports read the latest approvals and stop if the saved sheet changes during preparation.

AI remains PDF-compatible Illustrator artwork. Original vector streams and optional-content layers are copied without redrawing the front. Approved backs are matched by exact pool copy and placement. Their scale is read from the matching front Form XObject matrix, including historical sheets that did not store placement scale. Back artwork is arranged above the original cutting area, with a 10 mm gap; it is never scaled to fit the export page. Screen thumbnail reductions do not affect export dimensions.

DXF is ASCII AutoCAD 2004 (AC1018), in millimetres (`$INSUNITS=4`). It retains named layers and 24-bit entity/layer RGB (`420`), plus indexed colour fallback (`62`) for older importers. Cubic curves become LWPOLYLINE contours at a maximum 0.002 mm subdivision tolerance. Engraving glyphs are already vector outlines, including emoji. PDF fills become closed contours carrying a BRITES `FILL_CONTOUR` marker, for the operator's intended EZCAD hatch settings. No power, speed, hatch spacing or laser parameters are invented by the exporter.

Import using 1:1 millimetres and colour recognition. Confirm pen mapping and hatch settings in the installed LaserStar/EZCAD version. SHEET and BACK CUT OUTLINE are reference geometry. DXF's format can store true colour and layer names, but the particular machine software determines how it maps them to pens. Actual Windows LaserStar import and machine marking cannot be certified by browser tests.

Unsupported raster images, live text and intersecting clipping masks fail explicitly rather than silently dropping content. Illustrator's non-intersecting artboard clips are accepted after a geometric containment check. Original source artwork is retained for AI export.

References:

- EZCAD2 manual, section 4.9, vector import and colour recognition: https://www.stylecnc.com/uploads/file/200711/EZCAD-Software-for-Laser-Marking-Systems.pdf
- Autodesk DXF group codes, including true colour: https://help.autodesk.com/cloudhelp/2018/ENU/AutoCAD-DXF/files/GUID-3F0380A5-1C15-464D-BC66-2C5F094BCFB9.htm
- Autodesk LWPOLYLINE: https://help.autodesk.com/cloudhelp/2015/ENU/AutoCAD-DXF/files/GUID-748FC305-F3F2-4F74-825A-61F04D757A50.htm

Validation: `node tests/charm-nest/production-export.cjs` covers exact scaling, back placement, layer/colour retention, unsupported ownership, millimetre DXF, cubic subdivision, explicit newlines and automatic wrapping. Its generated `/tmp/production-test.dxf` can be independently audited with `ezdxf.readfile(...).audit()`.

Multiline engraving keeps the entered line breaks separately from the fitted layout. Auto wraps a single long line, As typed retains the entered layout, and explicit counts reflow at word boundaries. Editing an established placement refits after a 350 ms typing pause, preserves the caret and selected mode, and disables approval until the new text is verified. Approval also checks pending text for both new and previously saved engravings. No approval or saved artwork changes until the operator approves.

2026-09-20 verification: production-export, multiline-editor, back-editor, engraving-updates, public-assets, bridge-units, solver, functions, workspace and releases suites pass. The supplied 81-piece AI file converts to 902 DXF entities; an independent ezdxf audit reports zero errors and zero repairs. The synthetic exact-parent-scale export also audits cleanly. Browser checks cover the deployed shell, Library filters and format controls; the browser shows no saved sheets in the current live environment. Browser download restrictions and installed LaserStar import are not represented as tested.
