/*  netlify/functions/_charmNestOpClass.js — every op of charmNestLibrary in exactly one class, and the cloud's one list of ops (cloud plan v2,
 *  section 1.4; build step 1b: C1, E5). ONE source for both, so the class rule and the cloud's allowlist can never disagree.
 *
 *    READ        reads only (a cache it may fill on the way counts as a read: listingSkus, listingPhotos)
 *    PRESS       a person's own change (a decision, a hold, a move by hand, a setting, a start or stop), sent by a page
 *    PRODUCTION  the work a run does by itself (pieces made and placed, sheets saved, sets numbered, files, archives). In mode "on" only the cloud does it:
 *                a page that sends one is refused (charmNestLibrary handler, 409 cloudCarries). Dormant while Charm_Nest_Open/drive is absent or "off".
 *    CLOUD_ONLY  ops only the cloud's in-process calls may send, in every mode (none is built yet: each joins in the build step that adds it)
 *  tests/charm-nest/op-class.cjs fails on any op of the library left unclassified, or put in two classes.
 *
 *  CLOUD_OPS: the ONLY ops the cloud's in-process calls (Lib.call, Lib.cloudOps) may send. Anything else is refused before it runs ("not a cloud op", 403).
 *  It holds no op that calls Etsy: listingSkus is sent with cacheOnly forced (no Etsy call), listingPhotos without prepare (no image fetch).
 *  DENIED: the Etsy, Etsy-switch, world-build and agent ops, named so each has its own test; none of them is, or may ever be, in CLOUD_OPS.        */
"use strict";
const words = s => s.split(/\s+/).filter(Boolean);

const READ = new Set(words(`activityGet aliasGet artDraftGet artGet artList artPrepoolList backList backPreview cancelCheck cancelList changes computerList customGet
  customPieceList customReadGet customSaveGet customSavePending customSheetGet customSkuCheck designGet designList driveGet engraveSyncGet etsyStopGet findSheets
  flowState getAgent getCalibration getChanges getJob nestDebugGet getMany getOrderDays getOrderPieces getShapeGuidance getSheet history jobList laserDoneList laserSheetLast
  laserStatus listCharms listSheets listingPhotos listingSkus lookupCharms masterGet masterGetMany masterList masterListFiles noDesignGet optionMapGet partialList
  partialPolicyGet partialSearchList partialStocks ping poolGet poolList releaseGet remnantList reviewMarkOpen roseGet roseList runCmdList runGet runList
  sandboxStatus sandboxStream sessionsList setGet setList shareGet sharedOrders sheetHistory sheetRevsGet shippedByUs timelineGet traceSrcGet trayList worldGet
  zodiacListingStatus takeOffGet`));
READ.add("commitGet"); READ.add("setCommitCheck");   // (build step 12: the cloud's commit journal read, and a set's completion rules asked before a commit)

const PRODUCTION = new Set(words(`runPut runArchive runLinesAdd runLinesRetire poolPut poolUpdate putSheet backPut backInvalidate setAllocate setUpdate sheetPdf
  putCalibration archiveEmptySheet cancelFates timelineAdd startJob roseClaim roseRelease rosePlan roseRecordCut roseTakeOff roseReopen roseDemo partialClaim
  partialRelease partialUse partialPlan partialBackfill remnantMark remnantBackfill runCmdStep runCmdDone arrivalRecord`));

const PRESS = new Set(words(`activityPut aliasPut artDraftDrop artDraftPut artPut bridgeLog browserBackupPut cancelPut cancelRestore cancelSweep computerReport
  customDecide customDelete customEtsySkuWrite customPiecePut customPut customReopen customSaveCommit customSaveIntent customSaveRepair customSheetPut
  deleteSheet designLog designPut editSkuEtsy engraveSyncPut etsyResume etsyStopNow flowApply keepOverrideUndo laserDone listingEtsySkuWrite listingKindPut
  madeThumbPut manualClaim masterEditorPut masterPatch masterPutFile masterPutIndex masterRemoveFile masterRemoveSku masterRemoveSkus noDesignDelete noDesignPut
  optionMapPut partialPolicySet purgeHistory putCharms putShapeGuidance releasePut renameCharm restoreSheet reviewMarkPut runCarrier runCmdCancel runCmdPut
  runControl runLinesPatch runOpen runRetire arrivalsLease sandboxCancel sandboxPullLimitSet sandboxPullOrders sandboxPut sandboxReset sharePut sheetDelete sheetKeepInSet
  sheetLayoutPatch sheetMake shipUndo startAgent startMaster stopJob traceSrcPut trayPut worldBuild zodiacListingPrepare zodiacListingReconcile
  zodiacListingWrite takeOffAsk takeOffAck`));
PRESS.add("nestStart");   // (build step 6: a person's Nest and Nest all press, and the cloud's own ask from its nest step)
PRESS.add("commitUndoAsk");   // (build step 12: a person's Undo of a set the cloud sent to the Design Station)

const CLOUD_ONLY = new Set(words(`placeOnSheet relinkBacks cloudTakeOff finishClaim engraveIntentStart takeOffJournal`));
CLOUD_ONLY.add("commitJournal");   // (build step 12: the commit journal's stages, written by the cloud's commit unit only)
/* (build steps 8 and 15, _charmNestStandby.js): the owner's control in Settings. driveStatus reads; driveSet is a person's press (admin-locked) */
READ.add("driveStatus"); PRESS.add("driveSet");
// (CLOUD STEP 10, build steps 10a / 10b: _charmNestCloudSetOps.js) the cloud's own memory and answers; a person's presses; the reads the cards and the units make
for (const op of words(`assemblyStatePut greenStatePut labelsAskPut roseCutAskPut`)) CLOUD_ONLY.add(op);
for (const op of words(`labelsAsk roseCutAsk`)) PRESS.add(op);
for (const op of words(`assemblyGet labelsAskGet roseCutAskGet setReadiness`)) READ.add(op);
/* build step 13 (a person's requests done by the cloud, _charmNestCmdOps.js): the claim is the carrier's work (a page claims only while it carries, never in mode "on");
   the request's journal and its sheet writes are the cloud's own */
PRODUCTION.add("runCmdClaim"); for (const op of words(`cmdWorkPut cmdMovePieces cmdHopPieces cmdLeaveSet cmdVacate cmdPutBack`)) CLOUD_ONLY.add(op);   // (cmdHopPieces: ITEM 13, Paul 21 Nov 2026, a sheet hop from a viewer)

/* The cloud's allowlist (section 1.4): the reads, then the writes, as they exist today. A new op joins in the build step that adds it. */
const CLOUD_READS = words(`runGet runOpen worldGet getSheet getOrderPieces poolGet poolList cancelList cancelCheck shippedByUs masterList optionMapGet aliasGet
  noDesignGet customGet customSheetGet customReadGet shareGet etsyStopGet driveGet listingSkus listingPhotos runCmdList takeOffGet
  partialPolicyGet`);   // (partialPolicyGet: Options → Partial Sheet, read by the placement unit, build step 5; takeOffGet: build step 9)
const CLOUD_WRITES = words(`runPut runLinesAdd runLinesPatch runArchive runCmdStep runCmdDone runCarrier runOpen runControl poolPut poolUpdate putSheet sheetPdf
  putCalibration setAllocate setUpdate backPut archiveEmptySheet timelineAdd cancelFates sharePut arrivalRecord
  nestStart stopJob roseClaim
  placeOnSheet relinkBacks
  cloudTakeOff takeOffJournal takeOffAck`);   // (build step 6: the nest step asks for a sheet's nest, stops the jobs a Stop press stops, and claims a cut-line sheet's physical sheet once, before its slots run;
                                              //  placeOnSheet: build step 5, CLOUD_ONLY, the placement unit's one write of a piece onto a sheet; build step 9: the take-off unit's ops)
/* (build step 7a, the one sheet writer) finishSheet: a person's "Write files again" and the driver's files lane (a press: a page's is taken in every mode); finishGet: its progress;
   finishClaim: CLOUD_ONLY above. The cloud's own: finishSheet, finishClaim, finishGet, and artList (a piece's own artwork edit, read when the writer makes the piece again). */
READ.add("finishGet"); PRESS.add("finishSheet");
CLOUD_READS.push("finishGet", "artList"); CLOUD_WRITES.push("finishSheet", "finishClaim");
// (CLOUD STEP 10: the set assembly, the labels unit, the checkpoint archive and the green line: setReadiness, a piece's own artwork edits for the contour (artList, artPrepoolList: reads);
//  runLinesRetire; the memory and answers above; the green line's stock ops (roseClaim, rosePlan, roseRecordCut, roseReopen, roseRelease: hard-parts §4.4); flowApply for a COMMITTED
//  set's files only (its setFiles step, applySetFiles: CLOUD_BODY below keeps every other step out of a cloud call))   (artList and roseClaim are on the lists above already)
CLOUD_READS.push(...words(`setReadiness assemblyGet artPrepoolList`));
CLOUD_WRITES.push(...words(`runLinesRetire assemblyStatePut greenStatePut labelsAskPut roseCutAskPut rosePlan roseRecordCut roseReopen roseRelease flowApply`));
/* build step 11 (engraving in the cloud, _charmEngraveOps.js / _charmEngraveFiles.js): the page's Approve for the cloud's back-file task (a person's press) and what those tasks say (a read);
   the cloud reads a line's engraving work and a piece's own artwork edits (artList, above), and writes the line's engraving work (its fit, its approval once the file is saved) */
READ.add("engraveFilesGet"); PRESS.add("engraveApprove");
CLOUD_READS.push("engraveSyncGet"); CLOUD_WRITES.push("engraveSyncPut");
/* build step 11 (C3, E5: the word reading in the cloud, _charmEngraveClassify.js): engraveIntentStart (CLOUD_ONLY above) starts the paid reading of a line's words, mode forced to engraveIntent
   (Claude only: its question is built from the line's records, never a picture, never Etsy); getAgent reads that reading back. startAgent itself stays DENIED. */
CLOUD_WRITES.push("engraveIntentStart"); CLOUD_READS.push("getAgent");
CLOUD_READS.push("commitGet", "setCommitCheck"); CLOUD_WRITES.push("commitJournal");   // (build step 12: sending finished sets to the Design Station)
CLOUD_WRITES.push(...words(`runCmdClaim cmdWorkPut cmdMovePieces cmdHopPieces cmdLeaveSet cmdVacate cmdPutBack`));   // (build step 13)
/* RGRESTORE (Paul, 22 Nov 2026): a nest's verified layout put back on its sheet, the pieces a later write moved onto a draft brought back where that nest left them (_charmNestCmdOps.js
   cmdRestoreNest; the request "restorenest", _charmNestCmdExec.js). The cloud's own write only, never a page's; no Etsy call follows from it. */
CLOUD_ONLY.add("cmdRestoreNest"); CLOUD_WRITES.push("cmdRestoreNest");
CLOUD_ONLY.add("finishOverflow"); CLOUD_WRITES.push("finishOverflow");   // (build step 7b: the sheet writer's overflow, what a nest turned away moved on to the next sheet: _charmNestFinishOps.js)
const CLOUD_OPS = new Set([...CLOUD_READS, ...CLOUD_WRITES]);
/* RELEASEHOLD (Paul, 20 Nov 2026; build step 13, live): a person's Release Hold press, done in the cloud at once (_charmNestHoldRelease.js): releaseAsk is a PRESS from a page and the cloud's own
   call when the driver's release unit finishes one a function left half done; releaseJobGet reads its journal. Neither calls Etsy. */
PRESS.add("releaseAsk"); READ.add("releaseJobGet");
/* (ITEM 10, Paul 21 Nov 2026): a person's "Done" on a Custom piece in the Charm Editor, and its Undo, done at once in mode "on" (_charmNestCustomDone.js): a page's press, never the cloud's */
PRESS.add("customDone");
CLOUD_WRITES.push("releaseAsk"); CLOUD_READS.push("releaseJobGet"); CLOUD_OPS.add("releaseAsk"); CLOUD_OPS.add("releaseJobGet");
/* GAP G2 (Paul 21 Nov 2026): after a sheet hop the cloud asks itself for an approved set's files again (labelsAsk { kind: "setFiles" }, a press from a page; the cloud's own call from
   _charmNestCmdExec.js hop), answered by its labels pass item. It writes only the ask; no Etsy call follows from it. */
CLOUD_WRITES.push("labelsAsk"); CLOUD_OPS.add("labelsAsk");
/* (S2, Paul 21 Nov 2026): Review's Resolved decisions in the cloud (_charmNestReviewSettled.js): a person's answer, and its read (the cloud world reads it through Lib.ops, not a cloud call) */
PRESS.add("reviewSettledPut"); READ.add("reviewSettledGet");

/* Never the cloud's (E5), each with its own test: they call Etsy, its image CDN or its switch, build the world (which may call Etsy), or start an agent job
   (a customRead job fetches Etsy's image CDN, _charmNestCustomRead.js). */
const DENIED = new Set(words(`editSkuEtsy listingEtsySkuWrite customEtsySkuWrite customSaveRepair customSaveCommit zodiacListingPrepare zodiacListingWrite
  zodiacListingReconcile sandboxPullOrders etsyResume etsyStopNow worldBuild startAgent`));
for (const op of DENIED) if (CLOUD_OPS.has(op)) throw new Error("a denied op is in the cloud's list: " + op);

/* What the cloud sends for an op, made safe before it runs (no Etsy call can follow from it). */
const CLOUD_BODY = {
  listingSkus: b => { b.cacheOnly = true; delete b.nameIds; delete b.refreshIds; return b; },
  listingPhotos: b => { delete b.prepare; return b; },
  flowApply: b => { b.steps = (Array.isArray(b.steps) ? b.steps : []).filter(x => x && x.type === "setFiles"); return b; }   // (CLOUD STEP 10: the cloud writes a committed set's files, nothing else, through flowApply)
};

const classOf = op => (READ.has(op) ? "READ" : PRESS.has(op) ? "PRESS" : PRODUCTION.has(op) ? "PRODUCTION" : CLOUD_ONLY.has(op) ? "CLOUD_ONLY" : null);
const isCloudOp = op => typeof op === "string" && CLOUD_OPS.has(op);

module.exports = { READ, PRESS, PRODUCTION, CLOUD_ONLY, CLOUD_OPS, CLOUD_READS, CLOUD_WRITES, DENIED, CLOUD_BODY, classOf, isCloudOp };
