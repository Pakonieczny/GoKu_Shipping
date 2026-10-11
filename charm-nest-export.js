/* Production exports: original vector AI pages, exact-copy backs, millimetre DXF.
 * DXF R2004: true colour (420), indexed fallback (62), named layers and closed
 * outline polylines. Filled PDF artwork stays filled using solid HATCH entities.
 * Curves are adaptively flattened to <= 0.002 mm, never resized to fit a page.
 *
 * FRONT + BACK ENGRAVING (11 Oct 2026, plans/front-back-1011/CONTRACT.md sections 9-11). A back file is laid ABOVE the cut sheet (layer "BACK <poolId>[ Left|Right] / ...").
 * A front file (CharmNestPDF.buildBackFile({ face: "front" }), unmirrored) is put ON its charm in the sheet: compose(front, sheet, backs, fronts) reads the matrix the file
 * says it was drawn with (Info entry BritesFrontFrame), takes the charm's own placement from the cut sheet, and writes the engraving paths under that placement in the layer
 * "FRONT <poolId>[ Left|Right] / ENGRAVE" (the Right ear's mirror is already in its view: nothing is flipped here). A front file that cannot be put on its charm is laid above the
 * sheet like a back (layers "FRONT <poolId> / CUT OUTLINE (reference)" and "... / ENGRAVE"), and the layout says so (frontLayout[].placed = false, .reason) for IMPORT.txt.
 * A sheet with no front record composes, exports and indexes exactly as it did before: every front step below is behind `fronts.length`.
 */
(function(root) {
  'use strict';
  const MM = 25.4 / 72, GAP = 10 / MM, SPACE = 3 / MM;
  const pdf = () => root.CharmNestPDF, lib = () => root.PDFLib;
  const union = (a,b) => !a ? b.slice() : [Math.min(a[0],b[0]),Math.min(a[1],b[1]),Math.max(a[2],b[2]),Math.max(a[3],b[3])];
  function leaves(parsed) {
    const seen = new Set(), out = [], clips = [];
    function visit(s, parent) {
      if (seen.has(s)) return; seen.add(s);
      const layer = s.layer || parent || 'Artwork';
      if (s.kind === 'xobj' && s.children?.length) s.children.forEach(k => visit(k,layer));
      else if (s.kind === 'path') out.push({...s, layer});
      else if(s.kind === 'clip') clips.push(s);
      else if(s.kind === 'noop') return;
      else throw new Error(`Cannot export ${s.kind} on ${layer} to vector DXF. Outline text and remove raster artwork first.`);
    }
    parsed.segments.forEach(s => visit(s));
    // Illustrator commonly includes huge rectangular artboard clips. They do
    // not change any ink. Accept only provably non-intersecting clips; never
    // silently export geometry that the AI file hides behind a real mask.
    const bounds=out.reduce((a,s)=>union(a,s.bbox),null);
    for(const clip of clips) {
      const sub=clip.subpaths?.[0]||[],pts=sub.filter(o=>o[0]!=='h').map(o=>o[1]);
      const cross=(a,b,p)=>(b[0]-a[0])*(p[1]-a[1])-(b[1]-a[1])*(p[0]-a[0]);
      const same=xs=>xs.every(x=>x>=-1e-6)||xs.every(x=>x<=1e-6);
      const convex=clip.subpaths.length===1&&pts.length===4&&sub.every(o=>['m','l','h'].includes(o[0]))&&same(pts.map((p,i)=>cross(p,pts[(i+1)%4],pts[(i+2)%4])));
      const corners=bounds?[[bounds[0],bounds[1]],[bounds[0],bounds[3]],[bounds[2],bounds[1]],[bounds[2],bounds[3]]]:[];
      if(!convex||!corners.every(p=>same(pts.map((a,i)=>cross(a,pts[(i+1)%4],p)))))throw new Error('This artwork uses a clipping mask. Expand the clipped artwork before DXF export.');
    }
    return out;
  }
  function productionPaths(parsed) {
    const paths=leaves(parsed),front=paths.filter(p=>!/^BACK(?: |$)|CUT OUTLINE|^SHEET(?: |$)|^ROSE SEPARATION CUT$/i.test(p.layer||'')&&!FRONT_LAYER.test(p.layer||''));
    const groups=pdf().groupCharms({...parsed,segments:front,nested:[]});
    const drop=new Set(),extra=[];
    for(const charm of groups.charms) {
      const before=charm.members.slice(),result=pdf().integrateRings(charm);
      for(const m of charm.members)if(pdf().isCutSilhouetteFill(charm,m))m.cutSilhouette=m===charm.outline?'outline':'twin';
      if(result.left.length)throw new Error('A hoop could not join its charm: '+result.left.join('; '));
      if(!result.welded)continue;
      before.filter(p=>!charm.members.includes(p)).forEach(p=>drop.add(p));
      extra.push(...charm.members.filter(p=>!before.includes(p)));
    }
    return paths.filter(p=>!drop.has(p)).concat(extra);
  }
  /** The placed charm a record belongs to, found in the parsed cut sheet: { form (its xobj, with the matrix the sheet draws it under), placement, scale }.
   *  `what` names the record in the messages ("Back" for a back, as it always was; "Front engraving" for a front). */
  function parentForm(front, sheet, back, what) {
    what = what || 'Back';
    const charm = (sheet.charms || []).find(c => c.poolId === back.poolId);
    const placement = (sheet.placements || []).find(p => p.id === charm?.id || p.poolId === back.poolId);
    if (!placement) throw new Error(`${what} ${back.poolId}: no matching placed front charm.`);
    const forms = front.segments.filter(s => s.kind === 'xobj');
    const layer = placement.layer || placement.layerName;
    const candidates = layer ? forms.filter(s => s.layer === layer) : [];
    const form = candidates.length === 1 ? candidates[0] : forms.length === sheet.placements.length ? forms[(placement.n || sheet.placements.indexOf(placement)+1)-1] : null;
    if (!form?.matrix) throw new Error(`${what} ${back.poolId}: cannot verify its parent scale.`);
    const [a,b,c,d] = form.matrix, sx = Math.hypot(a,b), sy = Math.hypot(c,d);
    if (!(sx > 0) || Math.abs(sx-sy) > 1e-5 || Math.abs(a*c+b*d)>1e-5) throw new Error('Nonuniform parent scaling cannot be exported safely.');
    if (placement.scale && Math.abs(placement.scale-sx)>1e-5) throw new Error('Saved placement scale disagrees with the actual front artwork.');
    return {form, placement, scale: sx};
  }
  function parentScale(front, sheet, back) { return parentForm(front, sheet, back, 'Back').scale; }
  /* ── the FRONT face ── */
  const isFront = r => !!r && r.face === 'front';
  /** Layer names of the sheet document for one piece's record: "BACK 4171_9001_1 Left / ", "FRONT 4171_9001_1 / " (the file's own layer name follows). */
  const layerPrefix = (face, poolId, side) => (face === 'front' ? 'FRONT ' : 'BACK ') + poolId + (side ? ' ' + (side === 'L' ? 'Left' : 'Right') : '') + ' / ';
  /** A layer the front engravings made in the sheet document ("FRONT <poolId>[ Left|Right] / ENGRAVE", or the reference outline of one laid above the sheet): never charm art, never a cut. */
  const FRONT_LAYER = /^FRONT \S+(?: (?:Left|Right))? \/ /;
  /** a 2D matrix inverse and product in the PDF convention (x' = a x + c y + e, y' = b x + d y + f; mul(m, k) applies m first) */
  const inv = m => {const det=m[0]*m[3]-m[1]*m[2];if(!(Math.abs(det)>1e-12))return null;return [m[3]/det,-m[1]/det,-m[2]/det,m[0]/det,(m[2]*m[5]-m[3]*m[4])/det,(m[1]*m[4]-m[0]*m[5])/det];};
  const mulM = (m, k) => [m[0]*k[0]+m[1]*k[2], m[0]*k[1]+m[1]*k[3], m[2]*k[0]+m[3]*k[2], m[2]*k[1]+m[3]*k[3], m[4]*k[0]+m[5]*k[2]+k[4], m[4]*k[1]+m[5]*k[3]+k[5]];
  const apM = (m, x, y) => [m[0]*x+m[2]*y+m[4], m[1]*x+m[3]*y+m[5]];
  /** The matrix a front file says took the charm's source points onto its page (Info entry BritesFrontFrame, written by buildBackFile), or null (a file without it is laid above the sheet). */
  function frontFrame(parsed) {
    try {
      const L = lib(), info = parsed.doc.getInfoDict(), v = info && info.get(L.PDFName.of('BritesFrontFrame'));
      const text = v && v.decodeText ? v.decodeText() : null; if (!text) return null;
      const o = JSON.parse(text);
      return o && o.face === 'front' && Array.isArray(o.matrix) && o.matrix.length === 6 && o.matrix.every(Number.isFinite) && inv(o.matrix) ? o.matrix : null;
    } catch (_) { return null; }
  }
  /** The ear a back engraving is for ("L" | "R"), from the back's own record, else from the sheet's charm of that pool id; null for any other piece (pairs, 9 Oct). */
  const sideOfBack = (sheet, b) => { const s = b.side || ((sheet.charms || []).find(c => c.poolId === b.poolId) || {}).side; return s === 'L' || s === 'R' ? s : null; };
  const groupOfBack = b => { if (b.groupKey) return String(b.groupKey); const m = /^(\d{4,20})_([^_]*)_\d{1,3}$/.exec(String(b.poolId || '')); return m ? m[1] + ':' + m[2] : ''; };
  /** The front engravings of a sheet, each on its charm or (when that cannot be done) set aside to be laid above the sheet. `fronts`: records with their file bytes.
   *  A front is put on its charm only when its file says how it sits on the charm (BritesFrontFrame), the sheet gives the charm's own placement, and the engraving lands
   *  inside that charm once placed; anything less and it goes above the sheet like a back, and the entry says why. */
  async function planFronts(front, sheet, fronts) {
    const P = pdf(), onCharm = [], above = [], entries = [];
    for (const f of fronts) {
      const parsed = await P.parseSource(f.bytes, 'Front engraving '+f.poolId), paths = leaves(parsed);
      // (the engraving is what the file's ENGRAVE layer holds; the lines of its cut reference may carry the master's own layer name, so they are told apart by NOT being on ENGRAVE)
      const ink = paths.filter(p => /^ENGRAVE$/i.test(p.layer || ''));
      if (!ink.length) throw new Error('Front engraving contains no engraving paths.');
      const side = sideOfBack(sheet, f), parent = parentForm(front, sheet, f, 'Front engraving');
      const entry = {poolId:f.poolId, face:'front', ...(side?{side,groupKey:groupOfBack(f)}:{}), scale:parent.scale, parentSheetId:sheet.id || sheet.sheetId};
      entries.push(entry);
      const frame = frontFrame(parsed), back = frame && inv(frame);
      let why = frame ? null : 'its file does not say how it sits on its charm', X = null, box = null;
      if (!why) {
        X = mulM(back, parent.form.matrix);
        for (const p of ink) for (const sub of p.subpaths || []) for (const o of sub) for (let i = 1; i < o.length; i++) {
          const q = apM(X, o[i][0], o[i][1]); box = union(box, [q[0], q[1], q[0], q[1]]);
        }
        const fb = parent.form.bbox, tol = 2;
        if (!box || !fb || box[0] < fb[0]-tol || box[1] < fb[1]-tol || box[2] > fb[2]+tol || box[3] > fb[3]+tol) why = 'its engraving does not fall on its charm once placed';
      }
      if (why) { entry.placed = false; entry.reason = why; above.push({rec:f, parsed, scale:parent.scale, entry}); }
      else { entry.placed = true; entry.layer = layerPrefix('front', f.poolId, side) + 'ENGRAVE'; entry.boundsPt = box; onCharm.push({rec:f, ink, X, entry}); }
    }
    return {onCharm, above, entries};
  }
  /** Write one front's engraving paths into the sheet page under its charm's placement, in a layer of its own. */
  function writeFront(out, page, item, tag) {
    const L = lib(), res = page.node.Resources();
    const ref = out.context.register(out.context.obj({Type:'OCG',Name:L.PDFString.of(item.entry.layer)}));
    let props = res.lookup(L.PDFName.of('Properties'));
    if (!(props instanceof L.PDFDict)) { props = out.context.obj({}); res.set(L.PDFName.of('Properties'), props); }
    props.set(L.PDFName.of(tag), ref);
    page.pushOperators(L.PDFOperator.of('BDC',[L.PDFName.of('OC'),L.PDFName.of(tag)]), L.pushGraphicsState(), L.concatTransformationMatrix(...item.X));
    for (const p of item.ink) {
      const ops = [], even = /\*/.test(p.paintOp || '');
      for (const sub of p.subpaths || []) for (const o of sub) {
        if (o[0] === 'm') ops.push(L.moveTo(o[1][0], o[1][1]));
        else if (o[0] === 'l') ops.push(L.lineTo(o[1][0], o[1][1]));
        else if (o[0] === 'c') ops.push(L.appendBezierCurve(o[1][0], o[1][1], o[2][0], o[2][1], o[3][0], o[3][1]));
        else if (o[0] === 'h') ops.push(L.closePath());
        else throw new Error('Front engraving uses a path operator the export cannot place.');
      }
      if (!ops.length) continue;
      const fillRGB = p.fillRGB || [0,0,0], strokeRGB = p.strokeRGB || [0,0,0];
      if (p.fill) page.pushOperators(L.setFillingRgbColor(fillRGB[0], fillRGB[1], fillRGB[2]));
      if (p.stroke) page.pushOperators(L.setStrokingRgbColor(strokeRGB[0], strokeRGB[1], strokeRGB[2]), L.setLineWidth(p.lwPt || 0.5));
      page.pushOperators(...ops, L.PDFOperator.of(p.fill && p.stroke ? (even ? 'B*' : 'B') : p.fill ? (even ? 'f*' : 'f') : 'S'));
    }
    page.pushOperators(L.popGraphicsState(), L.PDFOperator.of('EMC',[]));
  }
  async function compose(frontBytes, sheet, backs, fronts) {
    const P = pdf(), L = lib();
    fronts = fronts || [];
    const front = await P.parseSource(frontBytes, 'Front sheet');
    const plan = fronts.length ? await planFronts(front, sheet, fronts) : null;
    const inputs = [], layout = []; let x = SPACE, y = front.pageH + GAP, rowH = 0;
    // the backs, then any front engraving that could not be put on its charm, are laid in rows above the cut sheet
    const laid = backs.map(b => ({b})).concat(plan ? plan.above.map(a => ({b:a.rec, parsed:a.parsed, scale:a.scale, entry:a.entry})) : []);
    for (const it of laid) {
      const b = it.b, isF = !!it.entry, what = isF ? 'Front' : 'Back';
      const parsed = it.parsed || await P.parseSource(b.bytes, 'Back '+b.poolId), paths = leaves(parsed);
      const bounds = paths.reduce((a,s)=>union(a,s.bbox),null);
      if (!bounds) throw new Error(what+' engraving contains no vector paths.');
      const scale = isF ? it.scale : parentScale(front,sheet,b);
      const w = (bounds[2]-bounds[0])*scale, h = (bounds[3]-bounds[1])*scale;
      if (w > front.pageW+1e-5) throw new Error(what+' engraving is wider than its sheet; export would require resizing.');
      if (x>SPACE && x+w > front.pageW-SPACE) {x=SPACE;y+=rowH+SPACE;rowH=0;}
      if (x===SPACE && x+w > front.pageW) x=Math.max(0,(front.pageW-w)/2);
      const tx=x-bounds[0]*scale, ty=y-bounds[1]*scale;
      inputs.push({parsed, scale, x:tx, y:ty, back:b, face:isF?'front':'back'});
      const side=sideOfBack(sheet,b);
      if (isF) it.entry.boundsPt=[x,y,x+w,y+h];
      else layout.push({poolId:b.poolId, ...(side?{side,groupKey:groupOfBack(b)}:{}), scale, boundsPt:[x,y,x+w,y+h], parentSheetId:sheet.id || sheet.sheetId});
      x+=w+SPACE;rowH=Math.max(rowH,h);
    }
    const out = await L.PDFDocument.create();
    const page = out.addPage([front.pageW,inputs.length ? y+rowH+SPACE : front.pageH]);
    out.setTitle(sheet.fileBase || 'Charm production sheet');out.setProducer('Brites Charm Nesting Station');
    async function place(parsed, x, y, scale, prefix) {
      const [copy] = await out.copyPages(parsed.doc,[0]);
      // copyPages preserves the page's OCG resource references. Register those
      // same references in the output catalogue so Illustrator retains layers.
      if (prefix) {
        const resources=copy.node.Resources();
        const seen=new Set();
        function walk(obj) {
          if (obj instanceof L.PDFRef) {if(seen.has(obj.toString()))return;seen.add(obj.toString());obj=out.context.lookup(obj);}
          if (obj instanceof L.PDFDict) {
            if(obj.get(L.PDFName.of('Type'))?.toString()==='/OCG') {
              const old=obj.get(L.PDFName.of('Name'));
              obj.set(L.PDFName.of('Name'),L.PDFString.of(prefix+(old?.decodeText?.() || 'Artwork')));
            }
            obj.entries().forEach(([,v])=>walk(v));
          } else if(obj instanceof L.PDFArray) obj.asArray().forEach(walk);
          else if(obj?.dict) walk(obj.dict);
        }
        walk(resources);
      }
      const embedded=await out.embedPage(copy);
      page.drawPage(embedded,{x,y,width:parsed.pageW*scale,height:parsed.pageH*scale});
    }
    await place(front,0,0,1,'');
    // front engravings ON their charm, in the layer "FRONT <poolId>[ Left|Right] / ENGRAVE", over the cut sheet (they are engraved in the cut run)
    if (plan) plan.onCharm.forEach((item,i) => writeFront(out, page, item, 'ocFront'+i));
    // (a back for one ear of a mismatched pair says so on its layer: "BACK 4171450075_9001_1 Left / ...")
    for(const item of inputs){const side=sideOfBack(sheet,item.back);await place(item.parsed,item.x,item.y,item.scale,layerPrefix(item.face,item.back.poolId,side));}
    // History never enters production artwork. Only this layout's new contour
    // is cut, at 1:1 scale, on its own named laser layer.
    const rosePlan=sheet.rosePlanJson ? JSON.parse(sheet.rosePlanJson) : sheet.rosePlan;
    // (a sheet of a metal with a green line, Rose Gold, 10K or 14K, that holds a physical sheet has its separation contour in the file: its layer keeps the name the laser's template knows)
    const cutR=root.CharmNestRose;
    if((sheet.metal==='rose'||(cutR&&cutR.cuts(sheet.metal))) && sheet.roseStockId && !rosePlan)throw new Error('Prepare the '+((cutR&&cutR.cutWord&&cutR.cutWord(sheet.metal))||'Rose Gold')+' separation contour before exporting');
    if(rosePlan){
      root.CharmNestRose.validate(rosePlan.profile,front.pageW,front.pageH);
      const ref=out.context.register(out.context.obj({Type:'OCG',Name:L.PDFString.of('ROSE SEPARATION CUT')}));
      const resources=page.node.Resources(),known=resources.lookup(L.PDFName.of('Properties'));   // (a sheet with front engravings on its charms already has its layer list here)
      if(known instanceof L.PDFDict)known.set(L.PDFName.of('RoseCut'),ref);else resources.set(L.PDFName.of('Properties'),out.context.obj({RoseCut:ref}));
      page.pushOperators(L.PDFOperator.of('BDC',[L.PDFName.of('OC'),L.PDFName.of('RoseCut')]),L.pushGraphicsState(),L.setStrokingRgbColor(0,.54,.45),L.setLineWidth(.1));
      for(const path of rosePlan.lines){
        page.pushOperators(...path.map(([x,y],i)=>i?L.lineTo(x,front.pageH-y):L.moveTo(x,front.pageH-y)),L.stroke());
      }
      page.pushOperators(L.popGraphicsState(),L.PDFOperator.of('EMC',[]));
    }
    const refs=out.context.enumerateIndirectObjects().filter(([,o])=>o instanceof L.PDFDict && o.get(L.PDFName.of('Type'))?.toString()==='/OCG').map(([r])=>r);
    if(refs.length) out.catalog.set(L.PDFName.of('OCProperties'),out.context.obj({OCGs:refs,D:{Order:refs,ON:refs}}));
    const ai=await out.save({useObjectStreams:false});
    return {ai,layout,...(plan?{frontLayout:plan.entries}:{}),widthPt:front.pageW,heightPt:page.getHeight(),cutHeightPt:front.pageH};
  }
  const vector = () => root.CharmNestVector || (typeof require==='function' ? require('./charm-nest-vector.js') : null);
  const flatten = (...args) => vector().flatten(...args);
  // Empty R2004 drawing scaffold generated with ezdxf 1.4.4: standard tables,
  // blocks, layouts and dictionaries with their original ownership handles.
  // Dynamic handles start at 0x1000, above every reserved scaffold handle.
  const DXF_SCAFFOLD = "0\r\nSECTION\r\n2\r\nHEADER\r\n9\r\n$ACADVER\r\n1\r\nAC1018\r\n9\r\n$ACADMAINTVER\r\n70\r\n0\r\n9\r\n$DWGCODEPAGE\r\n3\r\nANSI_1252\r\n9\r\n$INSBASE\r\n10\r\n0.0\r\n20\r\n0.0\r\n30\r\n0.0\r\n9\r\n$EXTMIN\r\n10\r\n{{MINX}}\r\n20\r\n{{MINY}}\r\n30\r\n0\r\n9\r\n$EXTMAX\r\n10\r\n{{MAXX}}\r\n20\r\n{{MAXY}}\r\n30\r\n0\r\n9\r\n$LIMMIN\r\n10\r\n0.0\r\n20\r\n0.0\r\n9\r\n$LIMMAX\r\n10\r\n420.0\r\n20\r\n297.0\r\n9\r\n$ORTHOMODE\r\n70\r\n0\r\n9\r\n$REGENMODE\r\n70\r\n1\r\n9\r\n$FILLMODE\r\n70\r\n1\r\n9\r\n$LTSCALE\r\n40\r\n1.0\r\n9\r\n$CLAYER\r\n8\r\n0\r\n9\r\n$CELTYPE\r\n6\r\nByLayer\r\n9\r\n$CECOLOR\r\n62\r\n256\r\n9\r\n$LUNITS\r\n70\r\n2\r\n9\r\n$LUPREC\r\n70\r\n4\r\n9\r\n$HANDSEED\r\n5\r\n{{HANDSEED}}\r\n9\r\n$TILEMODE\r\n70\r\n1\r\n9\r\n$MEASUREMENT\r\n70\r\n1\r\n9\r\n$INSUNITS\r\n70\r\n4\r\n9\r\n$PSTYLEMODE\r\n290\r\n1\r\n0\r\nENDSEC\r\n0\r\nSECTION\r\n2\r\nCLASSES\r\n0\r\nCLASS\r\n1\r\nACDBDICTIONARYWDFLT\r\n2\r\nAcDbDictionaryWithDefault\r\n3\r\nObjectDBX Classes\r\n90\r\n0\r\n91\r\n0\r\n280\r\n0\r\n281\r\n0\r\n0\r\nCLASS\r\n1\r\nSUN\r\n2\r\nAcDbSun\r\n3\r\nSCENEOE\r\n90\r\n1153\r\n91\r\n0\r\n280\r\n0\r\n281\r\n0\r\n0\r\nCLASS\r\n1\r\nVISUALSTYLE\r\n2\r\nAcDbVisualStyle\r\n3\r\nObjectDBX Classes\r\n90\r\n4095\r\n91\r\n0\r\n280\r\n0\r\n281\r\n0\r\n0\r\nCLASS\r\n1\r\nMATERIAL\r\n2\r\nAcDbMaterial\r\n3\r\nObjectDBX Classes\r\n90\r\n1153\r\n91\r\n0\r\n280\r\n0\r\n281\r\n0\r\n0\r\nCLASS\r\n1\r\nSCALE\r\n2\r\nAcDbScale\r\n3\r\nObjectDBX Classes\r\n90\r\n1153\r\n91\r\n0\r\n280\r\n0\r\n281\r\n0\r\n0\r\nCLASS\r\n1\r\nTABLESTYLE\r\n2\r\nAcDbTableStyle\r\n3\r\nObjectDBX Classes\r\n90\r\n4095\r\n91\r\n0\r\n280\r\n0\r\n281\r\n0\r\n0\r\nCLASS\r\n1\r\nMLEADERSTYLE\r\n2\r\nAcDbMLeaderStyle\r\n3\r\nACDB_MLEADERSTYLE_CLASS\r\n90\r\n4095\r\n91\r\n0\r\n280\r\n0\r\n281\r\n0\r\n0\r\nCLASS\r\n1\r\nDICTIONARYVAR\r\n2\r\nAcDbDictionaryVar\r\n3\r\nObjectDBX Classes\r\n90\r\n0\r\n91\r\n0\r\n280\r\n0\r\n281\r\n0\r\n0\r\nCLASS\r\n1\r\nCELLSTYLEMAP\r\n2\r\nAcDbCellStyleMap\r\n3\r\nObjectDBX Classes\r\n90\r\n1152\r\n91\r\n0\r\n280\r\n0\r\n281\r\n0\r\n0\r\nCLASS\r\n1\r\nMENTALRAYRENDERSETTINGS\r\n2\r\nAcDbMentalRayRenderSettings\r\n3\r\nSCENEOE\r\n90\r\n1024\r\n91\r\n0\r\n280\r\n0\r\n281\r\n0\r\n0\r\nCLASS\r\n1\r\nACDBDETAILVIEWSTYLE\r\n2\r\nAcDbDetailViewStyle\r\n3\r\nObjectDBX Classes\r\n90\r\n1025\r\n91\r\n0\r\n280\r\n0\r\n281\r\n0\r\n0\r\nCLASS\r\n1\r\nACDBSECTIONVIEWSTYLE\r\n2\r\nAcDbSectionViewStyle\r\n3\r\nObjectDBX Classes\r\n90\r\n1025\r\n91\r\n0\r\n280\r\n0\r\n281\r\n0\r\n0\r\nCLASS\r\n1\r\nRASTERVARIABLES\r\n2\r\nAcDbRasterVariables\r\n3\r\nISM\r\n90\r\n0\r\n91\r\n0\r\n280\r\n0\r\n281\r\n0\r\n0\r\nCLASS\r\n1\r\nLAYOUT\r\n2\r\nAcDbLayout\r\n3\r\nObjectDBX Classes\r\n90\r\n0\r\n91\r\n0\r\n280\r\n0\r\n281\r\n0\r\n0\r\nCLASS\r\n1\r\nACDBPLACEHOLDER\r\n2\r\nAcDbPlaceHolder\r\n3\r\nObjectDBX Classes\r\n90\r\n0\r\n91\r\n0\r\n280\r\n0\r\n281\r\n0\r\n0\r\nENDSEC\r\n0\r\nSECTION\r\n2\r\nTABLES\r\n0\r\nTABLE\r\n2\r\nVPORT\r\n5\r\n8\r\n330\r\n0\r\n100\r\nAcDbSymbolTable\r\n70\r\n1\r\n0\r\nVPORT\r\n5\r\n23\r\n330\r\n8\r\n100\r\nAcDbSymbolTableRecord\r\n100\r\nAcDbViewportTableRecord\r\n2\r\n*Active\r\n70\r\n0\r\n10\r\n0.0\r\n20\r\n0.0\r\n11\r\n1.0\r\n21\r\n1.0\r\n12\r\n{{CENTERX}}\r\n22\r\n{{CENTERY}}\r\n13\r\n0.0\r\n23\r\n0.0\r\n14\r\n0.5\r\n24\r\n0.5\r\n15\r\n0.5\r\n25\r\n0.5\r\n16\r\n0.0\r\n26\r\n0.0\r\n36\r\n1.0\r\n17\r\n0.0\r\n27\r\n0.0\r\n37\r\n0.0\r\n40\r\n{{VIEWHEIGHT}}\r\n41\r\n{{ASPECT}}\r\n42\r\n50.0\r\n43\r\n0.0\r\n44\r\n0.0\r\n50\r\n0.0\r\n51\r\n0.0\r\n71\r\n0\r\n72\r\n1000\r\n73\r\n1\r\n74\r\n3\r\n75\r\n0\r\n76\r\n0\r\n77\r\n0\r\n78\r\n0\r\n281\r\n0\r\n65\r\n0\r\n146\r\n0.0\r\n0\r\nENDTAB\r\n0\r\nTABLE\r\n2\r\nLTYPE\r\n5\r\n2\r\n330\r\n0\r\n100\r\nAcDbSymbolTable\r\n70\r\n3\r\n0\r\nLTYPE\r\n5\r\n24\r\n330\r\n2\r\n100\r\nAcDbSymbolTableRecord\r\n100\r\nAcDbLinetypeTableRecord\r\n2\r\nByBlock\r\n70\r\n0\r\n3\r\n\r\n72\r\n65\r\n73\r\n0\r\n40\r\n0.0\r\n0\r\nLTYPE\r\n5\r\n25\r\n330\r\n2\r\n100\r\nAcDbSymbolTableRecord\r\n100\r\nAcDbLinetypeTableRecord\r\n2\r\nByLayer\r\n70\r\n0\r\n3\r\n\r\n72\r\n65\r\n73\r\n0\r\n40\r\n0.0\r\n0\r\nLTYPE\r\n5\r\n26\r\n330\r\n2\r\n100\r\nAcDbSymbolTableRecord\r\n100\r\nAcDbLinetypeTableRecord\r\n2\r\nContinuous\r\n70\r\n0\r\n3\r\n\r\n72\r\n65\r\n73\r\n0\r\n40\r\n0.0\r\n0\r\nENDTAB\r\n0\r\nTABLE\r\n2\r\nLAYER\r\n5\r\n1\r\n330\r\n0\r\n100\r\nAcDbSymbolTable\r\n70\r\n{{LAYER_COUNT}}\r\n0\r\nLAYER\r\n5\r\n27\r\n330\r\n1\r\n100\r\nAcDbSymbolTableRecord\r\n100\r\nAcDbLayerTableRecord\r\n2\r\n0\r\n70\r\n0\r\n62\r\n7\r\n6\r\nContinuous\r\n370\r\n-3\r\n390\r\n13\r\n0\r\nLAYER\r\n5\r\n28\r\n330\r\n1\r\n100\r\nAcDbSymbolTableRecord\r\n100\r\nAcDbLayerTableRecord\r\n2\r\nDefpoints\r\n70\r\n0\r\n62\r\n7\r\n6\r\nContinuous\r\n290\r\n0\r\n370\r\n-3\r\n390\r\n13\r\n{{LAYERS}}0\r\nENDTAB\r\n0\r\nTABLE\r\n2\r\nSTYLE\r\n5\r\n5\r\n330\r\n0\r\n100\r\nAcDbSymbolTable\r\n70\r\n1\r\n0\r\nSTYLE\r\n5\r\n29\r\n330\r\n5\r\n100\r\nAcDbSymbolTableRecord\r\n100\r\nAcDbTextStyleTableRecord\r\n2\r\nStandard\r\n70\r\n0\r\n40\r\n0.0\r\n41\r\n1.0\r\n50\r\n0.0\r\n71\r\n0\r\n42\r\n2.5\r\n3\r\ntxt\r\n4\r\n\r\n0\r\nENDTAB\r\n0\r\nTABLE\r\n2\r\nVIEW\r\n5\r\n7\r\n330\r\n0\r\n100\r\nAcDbSymbolTable\r\n70\r\n0\r\n0\r\nENDTAB\r\n0\r\nTABLE\r\n2\r\nUCS\r\n5\r\n6\r\n330\r\n0\r\n100\r\nAcDbSymbolTable\r\n70\r\n0\r\n0\r\nENDTAB\r\n0\r\nTABLE\r\n2\r\nAPPID\r\n5\r\n3\r\n330\r\n0\r\n100\r\nAcDbSymbolTable\r\n70\r\n4\r\n0\r\nAPPID\r\n5\r\n2A\r\n330\r\n3\r\n100\r\nAcDbSymbolTableRecord\r\n100\r\nAcDbRegAppTableRecord\r\n2\r\nACAD\r\n70\r\n0\r\n0\r\nAPPID\r\n5\r\n2F\r\n330\r\n3\r\n100\r\nAcDbSymbolTableRecord\r\n100\r\nAcDbRegAppTableRecord\r\n2\r\nBRITES\r\n70\r\n0\r\n0\r\nAPPID\r\n5\r\n32\r\n330\r\n3\r\n100\r\nAcDbSymbolTableRecord\r\n100\r\nAcDbRegAppTableRecord\r\n2\r\nHATCHBACKGROUNDCOLOR\r\n70\r\n0\r\n0\r\nAPPID\r\n5\r\n33\r\n330\r\n3\r\n100\r\nAcDbSymbolTableRecord\r\n100\r\nAcDbRegAppTableRecord\r\n2\r\nEZDXF\r\n70\r\n0\r\n0\r\nENDTAB\r\n0\r\nTABLE\r\n2\r\nDIMSTYLE\r\n5\r\n4\r\n330\r\n0\r\n100\r\nAcDbSymbolTable\r\n70\r\n1\r\n100\r\nAcDbDimStyleTable\r\n0\r\nDIMSTYLE\r\n105\r\n2B\r\n330\r\n4\r\n100\r\nAcDbSymbolTableRecord\r\n100\r\nAcDbDimStyleTableRecord\r\n2\r\nStandard\r\n70\r\n0\r\n3\r\n\r\n4\r\n\r\n40\r\n1.0\r\n41\r\n2.5\r\n42\r\n0.625\r\n43\r\n3.75\r\n44\r\n1.25\r\n45\r\n0.0\r\n46\r\n0.0\r\n47\r\n0.0\r\n48\r\n0.0\r\n140\r\n2.5\r\n141\r\n2.5\r\n142\r\n0.0\r\n143\r\n0.03937007874\r\n144\r\n1.0\r\n145\r\n0.0\r\n146\r\n1.0\r\n147\r\n0.625\r\n148\r\n0.0\r\n71\r\n0\r\n72\r\n0\r\n73\r\n0\r\n74\r\n0\r\n75\r\n0\r\n76\r\n0\r\n77\r\n1\r\n78\r\n8\r\n79\r\n3\r\n170\r\n0\r\n171\r\n3\r\n172\r\n1\r\n173\r\n0\r\n174\r\n0\r\n175\r\n0\r\n176\r\n0\r\n177\r\n0\r\n178\r\n0\r\n179\r\n2\r\n271\r\n2\r\n272\r\n2\r\n273\r\n2\r\n274\r\n3\r\n275\r\n0\r\n276\r\n0\r\n277\r\n2\r\n278\r\n44\r\n279\r\n0\r\n280\r\n0\r\n281\r\n0\r\n282\r\n0\r\n283\r\n0\r\n284\r\n8\r\n285\r\n0\r\n286\r\n0\r\n288\r\n0\r\n289\r\n3\r\n371\r\n-2\r\n372\r\n-2\r\n0\r\nENDTAB\r\n0\r\nTABLE\r\n2\r\nBLOCK_RECORD\r\n5\r\n9\r\n330\r\n0\r\n100\r\nAcDbSymbolTable\r\n70\r\n2\r\n0\r\nBLOCK_RECORD\r\n5\r\n17\r\n330\r\n9\r\n100\r\nAcDbSymbolTableRecord\r\n100\r\nAcDbBlockTableRecord\r\n2\r\n*Model_Space\r\n340\r\n1A\r\n0\r\nBLOCK_RECORD\r\n5\r\n1B\r\n330\r\n9\r\n100\r\nAcDbSymbolTableRecord\r\n100\r\nAcDbBlockTableRecord\r\n2\r\n*Paper_Space\r\n340\r\n1E\r\n0\r\nENDTAB\r\n0\r\nENDSEC\r\n0\r\nSECTION\r\n2\r\nBLOCKS\r\n0\r\nBLOCK\r\n5\r\n18\r\n330\r\n17\r\n100\r\nAcDbEntity\r\n8\r\n0\r\n100\r\nAcDbBlockBegin\r\n2\r\n*Model_Space\r\n70\r\n0\r\n10\r\n0.0\r\n20\r\n0.0\r\n30\r\n0.0\r\n3\r\n*Model_Space\r\n1\r\n\r\n0\r\nENDBLK\r\n5\r\n19\r\n330\r\n17\r\n100\r\nAcDbEntity\r\n8\r\n0\r\n100\r\nAcDbBlockEnd\r\n0\r\nBLOCK\r\n5\r\n1C\r\n330\r\n1B\r\n100\r\nAcDbEntity\r\n8\r\n0\r\n100\r\nAcDbBlockBegin\r\n2\r\n*Paper_Space\r\n70\r\n0\r\n10\r\n0.0\r\n20\r\n0.0\r\n30\r\n0.0\r\n3\r\n*Paper_Space\r\n1\r\n\r\n0\r\nENDBLK\r\n5\r\n1D\r\n330\r\n1B\r\n100\r\nAcDbEntity\r\n8\r\n0\r\n100\r\nAcDbBlockEnd\r\n0\r\nENDSEC\r\n0\r\nSECTION\r\n2\r\nENTITIES\r\n{{ENTITIES}}0\r\nENDSEC\r\n0\r\nSECTION\r\n2\r\nOBJECTS\r\n0\r\nDICTIONARY\r\n5\r\nA\r\n330\r\n0\r\n100\r\nAcDbDictionary\r\n281\r\n1\r\n3\r\nACAD_COLOR\r\n350\r\nB\r\n3\r\nACAD_GROUP\r\n350\r\nC\r\n3\r\nACAD_LAYOUT\r\n350\r\nD\r\n3\r\nACAD_MATERIAL\r\n350\r\nE\r\n3\r\nACAD_MLEADERSTYLE\r\n350\r\nF\r\n3\r\nACAD_MLINESTYLE\r\n350\r\n10\r\n3\r\nACAD_PLOTSETTINGS\r\n350\r\n11\r\n3\r\nACAD_PLOTSTYLENAME\r\n350\r\n12\r\n3\r\nACAD_SCALELIST\r\n350\r\n14\r\n3\r\nACAD_TABLESTYLE\r\n350\r\n15\r\n3\r\nACAD_VISUALSTYLE\r\n350\r\n16\r\n3\r\nEZDXF_META\r\n350\r\n2D\r\n0\r\nDICTIONARY\r\n5\r\nB\r\n330\r\nA\r\n100\r\nAcDbDictionary\r\n281\r\n1\r\n0\r\nDICTIONARY\r\n5\r\nC\r\n330\r\nA\r\n100\r\nAcDbDictionary\r\n281\r\n1\r\n0\r\nDICTIONARY\r\n5\r\nD\r\n330\r\nA\r\n100\r\nAcDbDictionary\r\n281\r\n1\r\n3\r\nModel\r\n350\r\n1A\r\n3\r\nLayout1\r\n350\r\n1E\r\n0\r\nDICTIONARY\r\n5\r\nE\r\n330\r\nA\r\n100\r\nAcDbDictionary\r\n281\r\n1\r\n3\r\nByBlock\r\n350\r\n1F\r\n3\r\nByLayer\r\n350\r\n20\r\n3\r\nGlobal\r\n350\r\n21\r\n0\r\nDICTIONARY\r\n5\r\nF\r\n330\r\nA\r\n100\r\nAcDbDictionary\r\n281\r\n1\r\n3\r\nStandard\r\n350\r\n2C\r\n0\r\nDICTIONARY\r\n5\r\n10\r\n330\r\nA\r\n100\r\nAcDbDictionary\r\n281\r\n1\r\n3\r\nStandard\r\n350\r\n22\r\n0\r\nDICTIONARY\r\n5\r\n11\r\n330\r\nA\r\n100\r\nAcDbDictionary\r\n281\r\n1\r\n0\r\nACDBDICTIONARYWDFLT\r\n5\r\n12\r\n330\r\nA\r\n100\r\nAcDbDictionary\r\n281\r\n1\r\n3\r\nNormal\r\n350\r\n13\r\n100\r\nAcDbDictionaryWithDefault\r\n340\r\n13\r\n0\r\nACDBPLACEHOLDER\r\n5\r\n13\r\n330\r\n12\r\n0\r\nDICTIONARY\r\n5\r\n14\r\n330\r\nA\r\n100\r\nAcDbDictionary\r\n281\r\n1\r\n0\r\nDICTIONARY\r\n5\r\n15\r\n330\r\nA\r\n100\r\nAcDbDictionary\r\n281\r\n1\r\n0\r\nDICTIONARY\r\n5\r\n16\r\n330\r\nA\r\n100\r\nAcDbDictionary\r\n281\r\n1\r\n0\r\nLAYOUT\r\n5\r\n1A\r\n330\r\nD\r\n100\r\nAcDbPlotSettings\r\n1\r\n\r\n4\r\nA3\r\n6\r\n\r\n40\r\n7.5\r\n41\r\n20.0\r\n42\r\n7.5\r\n43\r\n20.0\r\n44\r\n420.0\r\n45\r\n297.0\r\n46\r\n0.0\r\n47\r\n0.0\r\n48\r\n0.0\r\n49\r\n0.0\r\n140\r\n0.0\r\n141\r\n0.0\r\n142\r\n1.0\r\n143\r\n1.0\r\n70\r\n1024\r\n72\r\n1\r\n73\r\n0\r\n74\r\n5\r\n7\r\n\r\n75\r\n16\r\n76\r\n0\r\n77\r\n2\r\n78\r\n300\r\n147\r\n1.0\r\n148\r\n0.0\r\n149\r\n0.0\r\n100\r\nAcDbLayout\r\n1\r\nModel\r\n70\r\n1\r\n71\r\n0\r\n10\r\n0.0\r\n20\r\n0.0\r\n11\r\n420.0\r\n21\r\n297.0\r\n12\r\n0.0\r\n22\r\n0.0\r\n32\r\n0.0\r\n14\r\n{{MINX}}\r\n24\r\n{{MINY}}\r\n34\r\n0\r\n15\r\n{{MAXX}}\r\n25\r\n{{MAXY}}\r\n35\r\n0\r\n146\r\n0.0\r\n13\r\n0.0\r\n23\r\n0.0\r\n33\r\n0.0\r\n16\r\n1.0\r\n26\r\n0.0\r\n36\r\n0.0\r\n17\r\n0.0\r\n27\r\n1.0\r\n37\r\n0.0\r\n76\r\n1\r\n330\r\n17\r\n0\r\nLAYOUT\r\n5\r\n1E\r\n330\r\nD\r\n100\r\nAcDbPlotSettings\r\n1\r\n\r\n4\r\nA3\r\n6\r\n\r\n40\r\n7.5\r\n41\r\n20.0\r\n42\r\n7.5\r\n43\r\n20.0\r\n44\r\n420.0\r\n45\r\n297.0\r\n46\r\n0.0\r\n47\r\n0.0\r\n48\r\n0.0\r\n49\r\n0.0\r\n140\r\n0.0\r\n141\r\n0.0\r\n142\r\n1.0\r\n143\r\n1.0\r\n70\r\n0\r\n72\r\n1\r\n73\r\n0\r\n74\r\n5\r\n7\r\n\r\n75\r\n16\r\n76\r\n0\r\n77\r\n2\r\n78\r\n300\r\n147\r\n1.0\r\n148\r\n0.0\r\n149\r\n0.0\r\n100\r\nAcDbLayout\r\n1\r\nLayout1\r\n70\r\n1\r\n71\r\n1\r\n10\r\n0.0\r\n20\r\n0.0\r\n11\r\n420.0\r\n21\r\n297.0\r\n12\r\n0.0\r\n22\r\n0.0\r\n32\r\n0.0\r\n14\r\n1e+20\r\n24\r\n1e+20\r\n34\r\n1e+20\r\n15\r\n-1e+20\r\n25\r\n-1e+20\r\n35\r\n-1e+20\r\n146\r\n0.0\r\n13\r\n0.0\r\n23\r\n0.0\r\n33\r\n0.0\r\n16\r\n1.0\r\n26\r\n0.0\r\n36\r\n0.0\r\n17\r\n0.0\r\n27\r\n1.0\r\n37\r\n0.0\r\n76\r\n1\r\n330\r\n1B\r\n0\r\nMATERIAL\r\n5\r\n1F\r\n102\r\n{ACAD_REACTORS\r\n330\r\nE\r\n102\r\n}\r\n330\r\nE\r\n100\r\nAcDbMaterial\r\n1\r\nByBlock\r\n2\r\n\r\n70\r\n0\r\n40\r\n1.0\r\n71\r\n1\r\n41\r\n1.0\r\n91\r\n-1023410177\r\n42\r\n1.0\r\n72\r\n1\r\n3\r\n\r\n73\r\n1\r\n74\r\n1\r\n75\r\n1\r\n44\r\n0.5\r\n73\r\n0\r\n45\r\n1.0\r\n46\r\n1.0\r\n77\r\n1\r\n4\r\n\r\n78\r\n1\r\n79\r\n1\r\n170\r\n1\r\n48\r\n1.0\r\n171\r\n1\r\n6\r\n\r\n172\r\n1\r\n173\r\n1\r\n174\r\n1\r\n140\r\n1.0\r\n141\r\n1.0\r\n175\r\n1\r\n7\r\n\r\n176\r\n1\r\n177\r\n1\r\n178\r\n1\r\n143\r\n1.0\r\n179\r\n1\r\n8\r\n\r\n270\r\n1\r\n271\r\n1\r\n272\r\n1\r\n145\r\n1.0\r\n146\r\n1.0\r\n273\r\n1\r\n9\r\n\r\n274\r\n1\r\n275\r\n1\r\n276\r\n1\r\n42\r\n1.0\r\n72\r\n1\r\n3\r\n\r\n73\r\n1\r\n74\r\n1\r\n75\r\n1\r\n94\r\n63\r\n0\r\nMATERIAL\r\n5\r\n20\r\n102\r\n{ACAD_REACTORS\r\n330\r\nE\r\n102\r\n}\r\n330\r\nE\r\n100\r\nAcDbMaterial\r\n1\r\nByLayer\r\n2\r\n\r\n70\r\n0\r\n40\r\n1.0\r\n71\r\n1\r\n41\r\n1.0\r\n91\r\n-1023410177\r\n42\r\n1.0\r\n72\r\n1\r\n3\r\n\r\n73\r\n1\r\n74\r\n1\r\n75\r\n1\r\n44\r\n0.5\r\n73\r\n0\r\n45\r\n1.0\r\n46\r\n1.0\r\n77\r\n1\r\n4\r\n\r\n78\r\n1\r\n79\r\n1\r\n170\r\n1\r\n48\r\n1.0\r\n171\r\n1\r\n6\r\n\r\n172\r\n1\r\n173\r\n1\r\n174\r\n1\r\n140\r\n1.0\r\n141\r\n1.0\r\n175\r\n1\r\n7\r\n\r\n176\r\n1\r\n177\r\n1\r\n178\r\n1\r\n143\r\n1.0\r\n179\r\n1\r\n8\r\n\r\n270\r\n1\r\n271\r\n1\r\n272\r\n1\r\n145\r\n1.0\r\n146\r\n1.0\r\n273\r\n1\r\n9\r\n\r\n274\r\n1\r\n275\r\n1\r\n276\r\n1\r\n42\r\n1.0\r\n72\r\n1\r\n3\r\n\r\n73\r\n1\r\n74\r\n1\r\n75\r\n1\r\n94\r\n63\r\n0\r\nMATERIAL\r\n5\r\n21\r\n102\r\n{ACAD_REACTORS\r\n330\r\nE\r\n102\r\n}\r\n330\r\nE\r\n100\r\nAcDbMaterial\r\n1\r\nGlobal\r\n2\r\n\r\n70\r\n0\r\n40\r\n1.0\r\n71\r\n1\r\n41\r\n1.0\r\n91\r\n-1023410177\r\n42\r\n1.0\r\n72\r\n1\r\n3\r\n\r\n73\r\n1\r\n74\r\n1\r\n75\r\n1\r\n44\r\n0.5\r\n73\r\n0\r\n45\r\n1.0\r\n46\r\n1.0\r\n77\r\n1\r\n4\r\n\r\n78\r\n1\r\n79\r\n1\r\n170\r\n1\r\n48\r\n1.0\r\n171\r\n1\r\n6\r\n\r\n172\r\n1\r\n173\r\n1\r\n174\r\n1\r\n140\r\n1.0\r\n141\r\n1.0\r\n175\r\n1\r\n7\r\n\r\n176\r\n1\r\n177\r\n1\r\n178\r\n1\r\n143\r\n1.0\r\n179\r\n1\r\n8\r\n\r\n270\r\n1\r\n271\r\n1\r\n272\r\n1\r\n145\r\n1.0\r\n146\r\n1.0\r\n273\r\n1\r\n9\r\n\r\n274\r\n1\r\n275\r\n1\r\n276\r\n1\r\n42\r\n1.0\r\n72\r\n1\r\n3\r\n\r\n73\r\n1\r\n74\r\n1\r\n75\r\n1\r\n94\r\n63\r\n0\r\nMLINESTYLE\r\n5\r\n22\r\n102\r\n{ACAD_REACTORS\r\n330\r\n10\r\n102\r\n}\r\n330\r\n10\r\n100\r\nAcDbMlineStyle\r\n2\r\nStandard\r\n70\r\n0\r\n3\r\n\r\n62\r\n256\r\n51\r\n90.0\r\n52\r\n90.0\r\n71\r\n2\r\n49\r\n0.5\r\n62\r\n256\r\n6\r\nBYLAYER\r\n49\r\n-0.5\r\n62\r\n256\r\n6\r\nBYLAYER\r\n0\r\nMLEADERSTYLE\r\n5\r\n2C\r\n102\r\n{ACAD_REACTORS\r\n330\r\nF\r\n102\r\n}\r\n330\r\nF\r\n100\r\nAcDbMLeaderStyle\r\n179\r\n2\r\n170\r\n2\r\n171\r\n1\r\n172\r\n0\r\n90\r\n2\r\n40\r\n0.0\r\n41\r\n0.0\r\n173\r\n1\r\n91\r\n-1056964608\r\n92\r\n-2\r\n290\r\n1\r\n42\r\n2.0\r\n291\r\n1\r\n43\r\n8.0\r\n3\r\nStandard\r\n44\r\n4.0\r\n300\r\n\r\n342\r\n29\r\n174\r\n1\r\n175\r\n1\r\n176\r\n0\r\n178\r\n1\r\n93\r\n-1056964608\r\n45\r\n4.0\r\n292\r\n0\r\n297\r\n0\r\n46\r\n4.0\r\n94\r\n-1056964608\r\n47\r\n1.0\r\n49\r\n1.0\r\n140\r\n1.0\r\n294\r\n1\r\n141\r\n0.0\r\n177\r\n0\r\n142\r\n1.0\r\n295\r\n0\r\n296\r\n0\r\n143\r\n3.75\r\n271\r\n0\r\n272\r\n9\r\n273\r\n9\r\n0\r\nDICTIONARY\r\n5\r\n2D\r\n330\r\nA\r\n100\r\nAcDbDictionary\r\n280\r\n1\r\n281\r\n1\r\n3\r\nCREATED_BY_EZDXF\r\n350\r\n2E\r\n3\r\nWRITTEN_BY_EZDXF\r\n350\r\n34\r\n0\r\nDICTIONARYVAR\r\n5\r\n2E\r\n330\r\n2D\r\n100\r\nDictionaryVariables\r\n280\r\n0\r\n1\r\n1.4.4 @ 2026-09-20T16:35:49.480756+00:00\r\n0\r\nDICTIONARYVAR\r\n5\r\n34\r\n330\r\n2D\r\n100\r\nDictionaryVariables\r\n280\r\n0\r\n1\r\n1.4.4 @ 2026-09-20T16:35:49.481535+00:00\r\n0\r\nENDSEC\r\n0\r\nEOF\r\n";
  function dxf(paths, declaredLayers=[]) {
    const colors=[[0,0,0],[255,0,0],[255,255,0],[0,255,0],[0,255,255],[0,0,255],[255,0,255],[0,0,0],[128,128,128],[192,192,192]];
    const rgb=v=>(v||[0,0,0]).map(n=>Math.max(0,Math.min(255,Math.round(n*255))));
    const trueColor=c=>(c[0]<<16)+(c[1]<<8)+c[2];
    const aci=c=>{let best=7,score=Infinity;for(let i=1;i<colors.length;i++){let d=c.reduce((n,v,k)=>n+(v-colors[i][k])**2,0);if(d<score){best=i;score=d;}}return best;};
    const ascii=s=>String(s).replace(/[^\x20-\x7e]/g,c=>'\\U+'+c.charCodeAt(0).toString(16).toUpperCase().padStart(4,'0'));
    const names=new Map(),used=new Set(['0','defpoints']);
    const layer=s=>{if(!names.has(s)){let n=ascii(s.replace(/[<>/\\":;?*|=]/g,'_')).slice(0,180)||'Artwork',i=2,base=n;while(used.has(n.toLowerCase()))n=base+'_'+i++;used.add(n.toLowerCase());names.set(s,n);}return names.get(s);};
    const entities=[];const layerColors=new Map();
    for(const name of declaredLayers)layerColors.set(layer(name),[0,0,0]);
    for(const path of paths) {
      const name=layer(path.layer||'Artwork');
      // A charm's cut silhouette drawn as a FILL (productionPaths marks it) is a cut line: one closed polyline on its layer,
      // never a solid hatch that the laser would take for engraving and that leaves the charm with no cut at all. Its filled
      // twin beside a stroked outline adds nothing: that outline already cuts the line.
      const cutFill=!!path.cutSilhouette && path.fill && !path.stroke;
      if(cutFill && path.cutSilhouette==='twin')continue;
      // Black line art the grouping read as hatching (charm-nest-pdf.js "black LINE ART is blue hatching") is a blue HATCH of the area its line covers, never a hairline polyline.
      if(path.hatchLine && path.hatchBlue && path.stroke && !path.fill) {
        const loops=vector().filled({subpaths:pdf().lineAreaOf(path),paintOp:'f*'});
        if(loops.length){const color=rgb([0,0,1]);entities.push({type:'HATCH',loops,layer:name,color,fill:true});if(!layerColors.has(name))layerColors.set(name,color);}
        continue;
      }
      if(path.fill && !cutFill) {
        const loops=vector().filled(path);
        if(loops.length) {const color=rgb(path.hatchBlue?[0,0,1]:path.fillRGB);entities.push({type:'HATCH',loops,layer:name,color,fill:true});if(!layerColors.has(name))layerColors.set(name,color);}
      }
      if(path.stroke || cutFill)for(const sub of path.subpaths||[]) {
        const f=flatten(sub);if(f.points.length<2)continue;
        const color=rgb(cutFill?path.fillRGB:path.strokeRGB);if(!layerColors.has(name))layerColors.set(name,color);
        entities.push({...f,closed:cutFill?true:f.closed,type:'LWPOLYLINE',layer:name,color,width:Math.max(0,cutFill?Math.min(+path.lwPt||0,.25):+path.lwPt||0)*MM,fill:false});
      }
    }
    let text='',handle=0x1000;const add=(...pairs)=>{for(let i=0;i<pairs.length;i+=2)text+=pairs[i]+'\r\n'+pairs[i+1]+'\r\n';};
    for(const [name,c] of layerColors)add(0,'LAYER',5,(handle++).toString(16).toUpperCase(),330,'1',100,'AcDbSymbolTableRecord',100,'AcDbLayerTableRecord',2,name,70,0,62,aci(c),420,trueColor(c),6,'CONTINUOUS',370,-3,390,'13',347,'21');
    const layerText=text; text='';
    let bounds=null;
    const point = p => {
      const x=+(p[0]*MM).toFixed(7),y=+(p[1]*MM).toFixed(7);
      if(!Number.isFinite(x)||!Number.isFinite(y))throw new Error('DXF contains a non-finite coordinate.');
      add(10,x,20,y);bounds=union(bounds,[x,y,x,y]);
    };
    for(const ent of entities) {
      add(0,ent.type,5,(handle++).toString(16).toUpperCase(),330,'17',100,'AcDbEntity',8,ent.layer,62,aci(ent.color),420,trueColor(ent.color));
      if(ent.type==='HATCH') {
        add(100,'AcDbHatch',10,0,20,0,30,0,210,0,220,0,230,1,2,'SOLID',70,1,71,0,91,ent.loops.length);
        for(const loop of ent.loops) {
          add(92,loop.hole?2:3,72,0,73,1,93,loop.points.length);
          loop.points.forEach(point);add(97,0);
        }
        add(75,0,76,1,98,0,1001,'BRITES',1000,'SOLID_FILL');
      } else {
        add(100,'AcDbPolyline',90,ent.points.length,70,ent.closed?1:0,43,+ent.width.toFixed(7));
        ent.points.forEach(point);add(1001,'BRITES',1000,'STROKE');
      }
    }
    const [minX,minY,maxX,maxY]=bounds||[0,0,100,50],w=Math.max(1,maxX-minX),h=Math.max(1,maxY-minY);
    const values={LAYERS:layerText,ENTITIES:text,LAYER_COUNT:layerColors.size+2,HANDSEED:handle.toString(16).toUpperCase(),MINX:minX,MINY:minY,MAXX:maxX,MAXY:maxY,CENTERX:(minX+maxX)/2,CENTERY:(minY+maxY)/2,VIEWHEIGHT:h*1.1,ASPECT:w/h};
    text=DXF_SCAFFOLD.replace(/\{\{(\w+)\}\}/g,(_,key)=>values[key]);
    return {text,entityCount:entities.length,layers:[...names.entries()],colors:[...new Set(entities.map(e=>trueColor(e.color)))]};
  }
  function layerNames(parsed) {
    const L=lib();return [...new Set(parsed.doc.context.enumerateIndirectObjects().filter(([,o])=>o instanceof L.PDFDict&&o.get(L.PDFName.of('Type'))?.toString()==='/OCG').map(([,o])=>o.get(L.PDFName.of('Name'))?.decodeText?.()).filter(Boolean))];
  }
  /** back/back-index.pdf: six engraved backs a page, each with its picture (PNG bytes, or null) and its lines of text.
   *  Drawn in the background worker; the page used to draw it on its own thread after every approval.
   *  face: "front" draws front/front-index.pdf the same way ("engraved fronts"); absent = the back index, byte for byte as before. */
  async function backIndexPdf({title, backs, face}) {
    const {PDFDocument, StandardFonts, rgb} = lib();
    const doc = await PDFDocument.create(), font = await doc.embedFont(StandardFonts.Helvetica), charset = new Set(font.getCharacterSet());
    const per = 6;
    for (let i = 0; i < backs.length; i += per) {
      const page = doc.addPage([612, 792]); page.drawText(`${title} · engraved ${face === 'front' ? 'fronts' : 'backs'} ${i + 1}–${Math.min(backs.length, i + per)} of ${backs.length}`, {x: 36, y: 756, size: 12, font});
      for (let j = 0; j < per && i + j < backs.length; j++) {
        const b = backs[i + j], col = j % 2, row = Math.floor(j / 2), x = 36 + col * 280, y = 720 - row * 230;
        try { if (b.png) { const img = await doc.embedPng(b.png), s = Math.min(150 / img.width, 150 / img.height); page.drawImage(img, {x, y: y - 150, width: img.width * s, height: img.height * s}); } } catch (_) { /* thumbnail optional */ }
        (b.lines || []).forEach((t, k) => page.drawText([...String(t)].map(c => charset.has(c.codePointAt(0)) ? c : '?').join('').slice(0, 60), {x, y: y - 165 - k * 12, size: 8, font, color: rgb(0.1, 0.1, 0.1)}));
      }
    }
    return doc.save({useObjectStreams: false});
  }
  /* ── the records of the two faces: file names, the index and the report (front-index.pdf, front-report.json) ── */
  /** Where a face's two files go: <folder>/back/<fileBase>_back_<poolId>_<approvedAt>.ai|.png (as ever) and <folder>/front/<fileBase>_front_<poolId>_<approvedAt>.ai|.png. */
  function faceFiles({folderPath, fileBase, poolId, approvedAt, face}) {
    const f = face === 'front' ? 'front' : 'back', name = `${fileBase}_${f}_${poolId}_${approvedAt}`;
    return {face: f, dir: `${folderPath}/${f}`, name, ai: `${folderPath}/${f}/${name}.ai`, png: `${folderPath}/${f}/${name}.png`, index: `${folderPath}/${f}/${f}-index.pdf`, report: `${folderPath}/${f}/${f}-report.json`};
  }
  /** The records of one face of a sheet's flat list (a record without `face` is a back). */
  const ofFace = (records, face) => (records || []).filter(r => r && (face === 'front' ? r.face === 'front' : r.face !== 'front'));
  /** One row of <face>-report.json: today's row for a back (no `face` key), the same row plus face:"front" for a front. */
  function reportRow(b, face) {
    return {poolId: b.poolId, ...(face === 'front' ? {face: 'front'} : {}), order: b.order, sku: b.sku, copy: b.copy, ...(b.side === 'L' || b.side === 'R' ? {side: b.side, groupKey: b.groupKey || undefined} : {}), ...(b.slot ? {slot: b.slot} : {}), text: b.text, font: b.font, weight: b.weight, sizePt: b.sizePt, capMm: b.capMm, box: b.box, centre: b.centre, angle: b.angle, flipChecks: b.flipChecks, verified: b.verified, review: b.review, approvedBy: b.approvedBy, approvedAt: b.approvedAt, file: b.outputs && b.outputs.ai && b.outputs.ai.path};
  }
  /** The four lines under a picture of the index. `x.pairWord(b)`: " · Left ear" style text the page already builds for pairs; `x.earWord(b)`: the ear/disc word of a slot. */
  function indexLines(b, face, x) {
    x = x || {}; const when = at => at ? new Date(at).toLocaleString() : '';
    return [`${b.order} · ${b.sku} · copy ${b.copy}${x.pairWord ? x.pairWord(b) : ''}${b.slot && !b.side && x.earWord ? ' · ' + x.earWord(b.slot) : ''}${face === 'front' ? ' · FRONT' : ''}`, `"${String(b.text).replace(/\n/g, ' / ')}"`, `${b.font} ${b.weight} · ${b.sizePt} pt · cap ${b.capMm} mm${b.angle ? ` · ${b.angle}°` : ''}${b.small ? ' · SMALL' : ''}`, `approved by ${b.approvedBy || '—'} ${when(b.approvedAt)}`];
  }
  /** front/front-index.pdf and front/front-report.json of a sheet: the same summary the back outputs are, for the front records of its flat backPool list.
   *  sh: { folderPath, fileBase, backPool }; deps: { assetBytes(url) -> bytes, upload(path, bytes, type, label) -> {path, url}, pairWord?, earWord? } (the page passes its own).
   *  Returns { index: {path, url}, report: {path, url}, count } (what the page keeps as sh.frontOutputs), or null when the sheet has no front record. */
  async function faceOutputs(sh, face, deps) {
    const records = ofFace(sh.backPool, face);
    if (!records.length || !sh.folderPath) return null;
    const names = faceFiles({folderPath: sh.folderPath, fileBase: sh.fileBase, poolId: '', approvedAt: '', face});
    const items = await Promise.all(records.map(async b => ({png: b.outputs && b.outputs.png && b.outputs.png.url ? await deps.assetBytes(b.outputs.png.url).catch(() => null) : null, lines: indexLines(b, face, deps)})));
    const pdfBytes = await backIndexPdf({title: sh.fileBase, backs: items, face});
    const report = records.map(b => reportRow(b, face));
    const [idx, rep] = await Promise.all([deps.upload(names.index, pdfBytes, 'application/pdf', face === 'front' ? 'Saving front index' : 'Saving back index'), deps.upload(names.report, new TextEncoder().encode(JSON.stringify(report, null, 1)), 'application/json')]);
    return {index: {path: idx.path, url: idx.url}, report: {path: rep.path, url: rep.url}, count: records.length};
  }
  const api={compose,parentScale,parentForm,leaves,productionPaths,flatten,dxf,layerNames,backIndexPdf,faceFiles,ofFace,reportRow,indexLines,faceOutputs,layerPrefix,frontFrame,isFront,FRONT_LAYER,MM};
  root.CharmNestExport=api;
  if(typeof module==='object'&&module.exports)module.exports=api;
})(typeof window!=='undefined'?window:globalThis);
