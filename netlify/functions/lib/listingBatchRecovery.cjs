'use strict';
const { batchCharmPaths } = require('./listingBatchReservations.cjs');
const { stallRestartPending } = require('./listingBatchAdmission.cjs');

const SESSIONS_COLL = 'ListingGenerator1Sessions';
const ACTIVE = new Set(['JOB_STATE_PENDING', 'JOB_STATE_RUNNING']);
const ISSUE_STATUSES = new Set(['blocked', 'saving', 'cancelled']);
const normalizeState = value => String(value || '').replace(/^BATCH_STATE_/, 'JOB_STATE_');
const failureKind = value => /safety system|safety_violations|moderation|content_policy/i.test(String(value || '')) ? 'content'
  : /socket hang up|ECONN|ETIMEDOUT|storage\.googleapis|upload|download|network|fetch failed|manifest:/i.test(String(value || '')) ? 'storage'
  : /rate.limit|overload|temporar|internal.server|timeout|no result returned|no inline_data|batch error|completion window expired|batch_expired/i.test(String(value || '')) ? 'transient' : 'other';
const isModelTask = task => /\/(?:Primary|Secondary)_Models\//.test(String(task?.input_storage_path || ''));
// This changes the requested image itself: opaque clothing replaces an exposed
// torso. It never asks a model to ignore a policy or disguise rejected content.
const CLOTHED_MODEL_BRIEF = '\n\nCLOTHING AND COMPOSITION REQUIREMENT: Create a neutral commercial jewelry photograph of an adult model wearing an opaque, fully covering crew-neck top. Cover the chest and torso with fabric; display the necklace over the top. Use a relaxed, ordinary catalog pose. Preserve the master charm silhouette, engraving, attachment and scale exactly. This clothing requirement replaces any instruction above to preserve exposed skin or the original neckline. The jewelry is the sole subject of the advertisement.';
function compliantModelTask(task) {
  return { ...task, prompt: String(task.prompt || '') + CLOTHED_MODEL_BRIEF, contentRepair: true };
}
const stampMs = x => x?.toMillis?.() || Number(x || 0);
function tasksFor(set) { return Array.isArray(set.allTasks) ? set.allTasks : (set.tasks || []); }

// A session is counted by its actual output folders, never by retry job names.
// Storage is authoritative: approval/redo can change files after batch collection.
async function reconcileSession({ db, bucket, collection, sessionId, timestamp, now = Date.now, restoreLostPaidOutputs = false }) {
  if (!/^sess_[A-Za-z0-9_-]{8,80}$/.test(sessionId)) throw new Error('Invalid listing session');
  const snapshot = await db.collection(collection).where('sessionId', '==', sessionId).limit(4000).get();
  if (snapshot.size >= 4000) throw new Error('Session history exceeds reconciliation limit');
  const records = snapshot.docs.map(doc => ({ ref: doc.ref, ...doc.data() }));
  const groups = new Map();
  let planned = 0;
  for (const record of records) {
    const plan = /-(\d+)sets-/.exec(record.displayName || '');
    if (plan) planned = Math.max(planned, Number(plan[1]));
    for (const set of record.sets || []) {
      if (set.setKind === 'charm_maker' || !/^listing-generator-1\/[^/]+\/Ready_To_List\/Set_\d+$/.test(set.outputBasePath || '')) continue;
      let group = groups.get(set.outputBasePath);
      if (!group) { group = { set, records: [], tasks: new Map() }; groups.set(set.outputBasePath, group); }
      group.records.push(record);
      for (const task of tasksFor(set)) group.tasks.set(Number(task.slotIndex), task);
    }
  }
  const categories = [...new Set([...groups.values()].map(g => g.set.category))];
  const files = new Set();
  const approved = new Set();
  await Promise.all(categories.map(async category => {
    const prefixes = [
      `listing-generator-1/${category}/Ready_To_List/`,
      `listing-generator-1/Generated_Listing_Sets/Completed_Listing_Sets/${category}_Set_`,
      `listing-generator-1/Generated_Listing_Sets/Approved_Listing_Sets/${category}_Set_`,
    ];
    const listings = await Promise.all(prefixes.map(prefix => bucket.getFiles({ prefix })));
    listings.forEach(([items], index) => {
      for (const file of items) {
        files.add(file.name);
        if (index) {
          const match = /_Set_(\d+)\//.exec(file.name.slice(prefixes[index].lastIndexOf('/') + 1));
          if (match) approved.add(`${category}:${Number(match[1])}`);
        }
      }
    });
  }));
  const summary = { sessionId, planned: Math.max(planned, groups.size), registered: groups.size,
    complete: 0, approved: 0, active: 0, queued: 0, saving: 0, blocked: 0, cancelled: 0,
    missingImages: 0, checkedAt: now(), sets: [] };
  const updates = [];
  // Submitted parents are history, not waiting work. Clear old queue markers
  // so the bounded repair query always has room for genuinely pending jobs.
  for (const record of records) {
    // Backfill compact dashboard metadata while the background audit already
    // has the task records. Progress reads must never download their prompts.
    const setKeys = (record.sets || []).map(set => set.outputBasePath).filter(Boolean);
    const metadata = { setKeys, charmPaths: batchCharmPaths(record), setsCount: (record.sets || []).length,
      requestCount: record.routes?.length ?? (record.sets || []).reduce((n, set) =>
        n + (set.tasks || []).filter(task => task.type !== 'copy').length, 0) };
    if (JSON.stringify(record.setKeys) !== JSON.stringify(setKeys) ||
        JSON.stringify(record.charmPaths) !== JSON.stringify(metadata.charmPaths) ||
        record.setsCount !== metadata.setsCount || record.requestCount !== metadata.requestCount)
      updates.push({ ref: record.ref, patch: metadata });
    if (record.retryBatchName && record.repairPending)
      updates.push({ ref: record.ref, patch: { repairPending: false, retryRequested: false } });
  }
  for (const [path, group] of groups) {
    const { set } = group;
    const rows = group.records.sort((a, b) => stampMs(b.createdAt) - stampMs(a.createdAt));
    const leaves = rows.filter(r => !r.retryBatchName);
    const latest = leaves[0] || rows[0];
    const isApproved = approved.has(`${set.category}:${set.setN}`);
    const missing = [...group.tasks.values()].filter(task => !files.has(`${path}/Slot_${Number(task.slotIndex) + 1}.png`));
    const manifestPresent = files.has(`${path}/manifest.json`);
    const complete = isApproved || (group.tasks.size > 0 && missing.length === 0 && manifestPresent);
    const live = leaves.find(r => !r.collected && ACTIVE.has(normalizeState(r.state)));
    const queued = leaves.find(r => r.retryRequested && (r.locallyQueued || r.repairPending || !r.collected));
    const restart = leaves.find(stallRestartPending);
    let status, note = '', patch = {};
    if (complete) { status = isApproved ? 'approved' : 'complete'; summary.complete++; if (isApproved) summary.approved++;
      patch = { setComplete: true, repairPending: false, collectionPending: false, retryRequested: false, recoveryStatus: status };
    } else if (live) { status = 'active'; summary.active++; }
    else if (latest?.retryStatus === 'preparation_failed') {
      status = 'blocked'; summary.blocked++;
      note = latest.recoveryReason || latest.retryError || 'Listing preparation failed. Check its reference images.';
      patch = { recoveryStatus: 'blocked', recoveryReason: note, retryRequested: false, repairPending: false };
    }
    else if (queued) { status = 'queued'; summary.queued++; }
    else if (restart) {
      status = 'queued'; summary.queued++;
      patch = { repairPending: true, retryRequested: false, recoveryStatus: 'queued', setComplete: false,
        recoveryReason: 'Recovering unfinished images after an automatic batch cancellation.' };
    }
    else if (latest?.retryStatus === 'complete_or_protected' || latest?.stallRestartBlocked ||
        normalizeState(latest?.state) === 'JOB_STATE_CANCELLED' && latest?.stallRestart !== 'pending' && !latest?.stallCancelRequestedAt) {
      status = 'cancelled'; summary.cancelled++;
    } else {
      const missingKeys = new Set(missing.map(t => `s0_slot${Number(t.slotIndex)}`));
      const failures = (latest.results?.failures || []).filter(f => missingKeys.has(f.key) || String(f.key).startsWith('manifest:'));
      const kinds = new Set(failures.map(f => failureKind(f.error)));
      const failedKeys = new Set((latest.results?.failures || []).map(f => f.key));
      const paidTasks = latest.sets?.find(s => s.outputBasePath === path)?.tasks || [];
      const missingPaidOutput = paidTasks.some(task => missingKeys.has(`s0_slot${Number(task.slotIndex)}`) &&
        !failedKeys.has(`s0_slot${Number(task.slotIndex)}`));
      // A result already paid for must be recovered, never regenerated.
      if (latest.responsesFile && (restoreLostPaidOutputs || !latest.setComplete) &&
          (!latest.collected || restoreLostPaidOutputs && missingPaidOutput || kinds.has('storage') || latest.collectionPending || !missing.length && !manifestPresent)) {
        status = 'saving'; summary.saving++;
        patch = { collected: false, collectionPending: true, recoveryStatus: 'saving', setComplete: false };
      } else if (latest.collected && !missingPaidOutput && Number(latest.retryAttempt || 0) < 5 &&
          (kinds.size && [...kinds].every(k => k === 'transient') ||
           kinds.has('content') && [...kinds].every(k => k === 'content' || k === 'transient') &&
           !Number(latest.contentRepairAttempt || 0) && missing.every(isModelTask))) {
        status = 'queued'; summary.queued++;
        patch = { repairPending: true, retryRequested: true, recoveryStatus: 'queued', setComplete: false,
          retryQueuedAt: timestamp() };
      } else {
        status = 'blocked'; summary.blocked++;
        note = missingPaidOutput ? 'Previously saved images are missing. Restore the existing provider output if this set should be kept.'
          : kinds.has('content') ? 'Model photo rejected. Choose a different reference or a product-only photo.'
          : latest.retryError || failures[0]?.error || 'Missing images need a source or error review.';
        patch = { recoveryStatus: 'blocked', setComplete: false, recoveryReason: note, repairPending: false, retryRequested: false };
      }
    }
    if (!complete) summary.missingImages += missing.length;
    summary.sets.push({ category: set.category, setN: set.setN, outputBasePath: path, status,
      missingSlots: complete ? [] : missing.map(t => Number(t.slotIndex) + 1), note });
    // Reconcile the current leaf; history cleanup above only clears queue markers.
    if (latest && Object.keys(patch).length && Object.entries(patch).some(([key, value]) => key !== 'retryQueuedAt' && latest[key] !== value))
      updates.push({ ref: latest.ref, patch: { ...patch, reconciledAt: timestamp() } });
  }
  for (let index = 0; index < updates.length; index += 200) {
    const batch = db.batch();
    for (const update of updates.slice(index, index + 200)) batch.set(update.ref, update.patch, { merge: true });
    await batch.commit();
  }
  // COST: `sets` lists every set of the session (about 130 bytes each, up to 130 KB for 1000 sets) and the Batch panel only
  // uses the sets that need a person. batch_list reads this short list with a field mask and leaves `sets` on the server;
  // `sets` stays in the summary for any reader of the old field.
  summary.issueSets = summary.sets.filter(set => ISSUE_STATUSES.has(set.status));
  summary.unregistered = Math.max(0, summary.planned - summary.registered);
  summary.issues = summary.blocked + summary.cancelled;
  summary.processed = summary.complete + summary.issues;
  summary.pending = summary.active + summary.queued + summary.saving + summary.unregistered;
  const finished = summary.planned > 0 && summary.processed === summary.planned && summary.pending === 0;
  summary.status = finished ? (summary.issues ? 'completed_with_issues' : 'completed') : 'in_progress';
  summary.finishedAt = finished ? Math.max(0, ...records.map(record =>
    Math.max(stampMs(record.collectedAt), stampMs(record.updatedAt), stampMs(record.createdAt)))) : null;
  await db.collection(SESSIONS_COLL).doc(sessionId).set(summary);
  return summary;
}
module.exports = { SESSIONS_COLL, failureKind, isModelTask, compliantModelTask, CLOTHED_MODEL_BRIEF, reconcileSession };
