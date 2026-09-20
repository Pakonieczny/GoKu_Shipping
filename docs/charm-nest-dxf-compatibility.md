# DXF compatibility correction — 2026-09-20

The submitted `SS_working_silver-mua0s06d.dxf` contained 606 valid LWPOLYLINE contours on 58 artwork layers, but declared R2004 while omitting its model/paper-space BLOCK_RECORD and BLOCK definitions, LAYOUT objects and entity owner handles. The earlier ezdxf audit passed because its reader supplies missing drawing structures; that check was insufficient to establish strict CAD compatibility.

The exporter now starts from a complete empty R2004 drawing scaffold generated with ezdxf 1.4.4. It retains standard tables, block definitions, layouts, dictionaries and their handle relationships, inserts artwork layers and owned model-space polylines, sets the next free handle and drawing extents, and centres the initial viewport. Dynamic handles start at hexadecimal 1000, above the scaffold's reserved handles. The scaffold is embedded in `charm-nest-export.js`; browser downloads require no new service or runtime dependency. CAD layer name collisions are resolved case-insensitively. Non-finite coordinates abort export.

The format remains AC1018 with millimetre units, RGB group 420 and indexed colour fallback, named layers, closed contours for filled artwork and BRITES fill/stroke metadata. No contour is scaled, repositioned or simplified by this structural correction. Existing 0.002 mm adaptive curve flattening is unchanged.

Validation of the corrected submitted file: 606/606 contours match the original coordinates exactly, with identical artwork layer names, RGB/indexed colours, closed flags and fill/stroke metadata. ezdxf reports zero errors/fixes. Inkscape's independent DXF importer successfully imports the file and renders it; the rendered drawing was visually inspected. Actual LaserStar/AutoCAD desktop import has not been tested.

`node tests/charm-nest/dxf-structure.cjs` inspects raw DXF records before any repair-capable reader: complete sections and model/paper-space relationships, resolved references, unique handles, correct ownership, the next free handle, vertex counts, millimetres, RGB, layer collisions and empty exports. `production-export.cjs` and `production-export-ui.cjs` cover composition and download routes.

References:
- Autodesk, BLOCK and model/paper-space definitions: https://help.autodesk.com/cloudhelp/2024/ENU/AutoCAD-DXF/files/GUID-66D32572-005A-4E23-8B8B-8726E8C14302.htm
- ezdxf, layout management and required R2000+ LAYOUT relationships: https://ezdxf.readthedocs.io/en/stable/dxfinternals/layout_management.html
