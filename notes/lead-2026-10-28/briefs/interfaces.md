# Who builds what, and the contracts between workstreams

Each workstream has an id, a branch `claude/de-<id>` and a worktree `/tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/scratchpad/wt-de-<id>`.
Build to these contracts so the pieces fit when merged. Where you depend on another workstream that has not landed, write against the
contract with a `typeof`/existence guard and a small stand-in in your test. If you must change a contract, say so in your hand-back.

| id | items | owns (new files) | touches |
|---|---|---|---|
| feedback | 1 2 4 5 | — (editor core) | charm-nest-charm-editor.js: selection furniture, transform drags, prop bar, smart guides/snapping |
| rulers | 3 6 7 | — (editor core) | charm-nest-charm-editor.js: rulers, guides, zoom/pan, zoom box |
| pen | 9a | charm-nest-ce-paths.js | editor hooks: tools rail, key map |
| warp | 9b | charm-nest-ce-warp.js | editor hooks |
| ops | 8 | charm-nest-ce-ops.js | editor hooks: menus/prop bar |
| trace | 12 13 | charm-nest-ce-trace.js (+ worker inside via Blob URL, no extra file) | none in the editor |
| image | 11 | charm-nest-ce-image.js | editor hooks: import button, drop, paste |
| text | 14 15 | charm-nest-ce-text.js (+ an outline emoji font asset if needed) | editor text tool; bridge charmEditText/charmEditFonts |
| vimport | 23 | charm-nest-ce-import.js | editor hooks (the same import entry as `image`: coordinate through `CharmEditor.importers`) |
| designs | 18 21 27(data) | charm-nest-designs.js; server ops in charmNestLibrary.js | sandbox families |
| library | 19 20 | charm-nest-ce-library.js | editor: the right-hand panel |
| masters | 16 | charm-nest-ce-masters.js | editor library panel tab; bridge Master |
| sendsheet | 17 22 24 25 26 28 29 | charm-nest-custom-piece.js | bridge: Orders / custom-order entry points |
| custompool | 27 28 (pool, nest, files) | (code in bridge Pool / nest / server) | bridge Pool, finishNest/persist checks, server pool ops |
| tab | 30 31 32 33 | — | charm-nest-1.html labels, bridge engraving card, editor header button |
| emplog | 34 35 36 37 | charm-nest-design-log.js; server op | charm-nest-efficiency.js + its server op |
| refit | (Save bug) | — | bridge engraving card refit/drawing |

## Contracts

### Editor extension points (owner: feedback adds them FIRST, in its first commit, and pushes within ~30 min; others append to them)
```js
CharmEditor.tools      // [] of { id, icon (svg string), title, key, cursor, onDown(ctx, ev), onMove(ctx, ev), onUp(ctx, ev), onKey?(ctx, ev) }
CharmEditor.commands   // {} id -> { title, key?, run(ctx), enabled?(ctx) }   shown in the editor's menus / context menu
CharmEditor.importers  // [] of { id, accept: ['.png', ...] or mime prefixes, title, run(ctx, file) -> Promise }  (Import button, drop, paste)
CharmEditor.panels     // [] of { id, title, side: 'right', mount(ctx, el), unmount?() }
CharmEditor.onOpen     // [] of fn(ctx)    CharmEditor.onClose // [] of fn(ctx, result)
// ctx (while open): { doc, face(), items(), selection(), select(ids), addItems(items, opts), replaceItems(map), removeItems(ids),
//   commit(label) (one undo step), clientOf(x,y), pointOf(clientX, clientY), zoom(), setZoom(z, about), frame(), geo (the model), say(text) }
```
Until `feedback` lands, guard: `(window.CharmEditor && CharmEditor.tools || (CharmEditor.tools = [])).push(...)` works either way.

### Trace (owner: trace; used by image)
```js
CharmNestTrace.trace(src /* ImageData | {data,width,height} */, { levels = 4, detail = 0.5, smooth = 0.5, despeckle = 2, invert = false })
  -> Promise<{ width, height, layers: [{ grey /* 0 = black .. 1 */, subpaths /* editor ops: ["m",[x,y]] ["l",[x,y]] ["c",p1,p2,p3] ["h"] */ }] }>
// pixels, y down. Runs in a Worker (Blob URL) when available; cancellable: CharmNestTrace.cancel(); progress: opts.onProgress(0..1)
CharmNestTrace.grey(src, { contrast, brightness, invert }) -> ImageData   // the greyscale-with-black-base preview step
```

### Designs (owner: designs; used by library, masters, sendsheet, custompool, emplog)
Server ops in charmNestLibrary.js (collection `Charm_Designs`, versions in a subcollection `versions`, thumbnails in Storage):
`designPut {design:{id?, name, faces:{front:FaceDoc, back?:FaceDoc}, widthMm, heightMm, source:{kind:'master'|'import'|'new'|'piece', sku?, file?, poolId?}, orderId?, customName?}, by}` -> `{id, version, at, hash}`;
`designList {q?, limit?, cursor?, since?, by?, orderId?}` -> `{designs:[{id, name, version, thumbUrl, widthMm, heightMm, orderId, customName, by, at, source, hash}], next, upTo}`;
`designGet {id, version?}` -> `{design}`.
Page module: `window.CharmNestDesigns = { list(opts), get(id, version), put(design), search(q) /* local, instant */, onChange(fn) }`.

### Custom piece (owners: sendsheet = the modal and flow; custompool = the pool/nest/file side)
```js
CharmNestCustomPiece.open({ designId, version, orderId?, poolId? })    // the "Send to sheet" pop-up (sendsheet)
CharmNestCustomPiece.send({ designId, version, metal /* gold|silver|rose|gold10k|gold14k */, quantity, orderId /* or null */, customName,
  fixes /* {orderId, poolId, causedBy} | null */, by }) -> Promise<{ poolIds }>      // (sendsheet calls custompool's Pool side)
Pool.addCustomPiece({ design, version, hash, metal, orderId, customName, quantity, by }) -> Promise<poolIds>   // custompool
```
Server: `customPiecePut` (custompool) writes the pool rows; an orderless piece's order id is `CUSTOM-<yyyymmdd>-<n>` (allocated server side).

### Employee log (owner: emplog; everyone calls it)
`CNDesignLog.event(kind, data)` — kinds: `editorOpen, editorSave, imageImport, imageTrace, vectorImport, designSave, designOpen,
customPieceSent, fixPiece` (+ yours). Batched, written with the order's timeline where there is an order, and to the efficiency events.
