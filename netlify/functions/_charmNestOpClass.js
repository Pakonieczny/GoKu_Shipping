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
  flowState getAgent getCalibration getChanges getJob getMany getOrderDays getOrderPieces getShapeGuidance getSheet history jobList laserDoneList laserSheetLast
  laserStatus listCharms listSheets listingPhotos listingSkus lookupCharms masterGet masterGetMany masterList masterListFiles noDesignGet optionMapGet partialList
  partialPolicyGet partialSearchList partialStocks ping poolGet poolList releaseGet remnantList reviewMarkOpen roseGet roseList runCmdList runGet runList
  sandboxStatus sandboxStream sessionsList setGet setList shareGet sharedOrders sheetHistory sheetRevsGet shippedByUs timelineGet traceSrcGet trayList worldGet
  zodiacListingStatus`));

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
  zodiacListingWrite`));

const CLOUD_ONLY = new Set(words(`placeOnSheet cloudTakeOff finishClaim engraveIntentStart`));

/* The cloud's allowlist (section 1.4): the reads, then the writes, as they exist today. A new op joins in the build step that adds it. */
const CLOUD_READS = words(`runGet runOpen worldGet getSheet getOrderPieces poolGet poolList cancelList cancelCheck shippedByUs masterList optionMapGet aliasGet
  noDesignGet customGet customSheetGet customReadGet shareGet etsyStopGet driveGet listingSkus listingPhotos runCmdList`);
const CLOUD_WRITES = words(`runPut runLinesAdd runLinesPatch runArchive runCmdStep runCmdDone runCarrier runOpen runControl poolPut poolUpdate putSheet sheetPdf
  putCalibration setAllocate setUpdate backPut archiveEmptySheet timelineAdd cancelFates sharePut arrivalRecord`);
const CLOUD_OPS = new Set([...CLOUD_READS, ...CLOUD_WRITES]);

/* Never the cloud's (E5), each with its own test: they call Etsy, its image CDN or its switch, build the world (which may call Etsy), or start an agent job
   (a customRead job fetches Etsy's image CDN, _charmNestCustomRead.js). */
const DENIED = new Set(words(`editSkuEtsy listingEtsySkuWrite customEtsySkuWrite customSaveRepair customSaveCommit zodiacListingPrepare zodiacListingWrite
  zodiacListingReconcile sandboxPullOrders etsyResume etsyStopNow worldBuild startAgent`));
for (const op of DENIED) if (CLOUD_OPS.has(op)) throw new Error("a denied op is in the cloud's list: " + op);

/* What the cloud sends for an op, made safe before it runs (no Etsy call can follow from it). */
const CLOUD_BODY = {
  listingSkus: b => { b.cacheOnly = true; delete b.nameIds; delete b.refreshIds; return b; },
  listingPhotos: b => { delete b.prepare; return b; }
};

const classOf = op => (READ.has(op) ? "READ" : PRESS.has(op) ? "PRESS" : PRODUCTION.has(op) ? "PRODUCTION" : CLOUD_ONLY.has(op) ? "CLOUD_ONLY" : null);
const isCloudOp = op => typeof op === "string" && CLOUD_OPS.has(op);

module.exports = { READ, PRESS, PRODUCTION, CLOUD_ONLY, CLOUD_OPS, CLOUD_READS, CLOUD_WRITES, DENIED, CLOUD_BODY, classOf, isCloudOp };
