/*  netlify/functions/_investorLease.js
 *  ---------------------------------------------------------------------------
 *  Worker lease heartbeat shared by the cycle worker and the position guard.
 *
 *  The handler claims a job lease and, for cycles and guards, an account-wide
 *  lease. Both used to be written once at claim time and never touched again
 *  while the worker was alive, so an operator could not tell a live worker
 *  from a dead one and a slow worker could outlive its own lease. Every
 *  progress write — the cycle's reportRunProgress and the guard's own
 *  guardProgress — now renews the leases through this one function, and only
 *  while the writer still owns them.
 * ---------------------------------------------------------------------------
 */

"use strict";

const A = require("./_investorAdmin");

let ACTIVE = null;   // { jobRef, accountLeaseRef, leaseOwner, ttlMs }

function setActive(lease) { ACTIVE = lease || null; }
function clear() { ACTIVE = null; }
function active() { return ACTIVE; }

async function heartbeat(nowMs = Date.now()) {
  const lease = ACTIVE;
  if (!lease || !lease.jobRef) return { renewed: false, reason: "no active lease" };
  try {
    return await A.runTransaction(async (tx) => {
      const job = await tx.get(lease.jobRef);
      if (!job.exists || job.data().leaseOwner !== lease.leaseOwner
          || job.data().status !== "running") return { renewed: false, reason: "lease not owned" };
      tx.set(lease.jobRef, { workerLeaseExpiresAt: nowMs + lease.ttlMs,
        lastHeartbeatAtMs: nowMs }, { merge: true });
      if (lease.accountLeaseRef) {
        const acc = await tx.get(lease.accountLeaseRef);
        if (acc.exists && acc.data().leaseOwner === lease.leaseOwner) {
          tx.set(lease.accountLeaseRef, { leaseExpiresAt: nowMs + lease.ttlMs,
            lastHeartbeatAtMs: nowMs }, { merge: true });
        }
      }
      return { renewed: true, until: nowMs + lease.ttlMs };
    });
  } catch (e) {
    return { renewed: false, reason: String(e.message).slice(0, 120) };
  }
}

/* ── THE ENTRY-CREATION MUTEX ──────────────────────────────────────────────
 * R.checkAdd is a PURE function over a book snapshot its caller built. The
 * deep scan builds one at cycle start; the one-minute strike pass builds its
 * own independently. Neither sees the other's concurrent additions, and the
 * only thing re-checked inside the approval transaction is cash — never
 * maxOpenPositions, maxGrossExposurePct or the sector and cluster caps. So
 * two writers that both observe "5 of 6 positions used" will both admit one,
 * and the account ends up holding 7. The same mechanic breaches the gross and
 * sector ceilings.
 *
 * Fixing it inside the approval transaction is not possible: the caps are
 * aggregates over the positions and orders collections, and a Firestore
 * transaction cannot run a query. So entry CREATION is serialized instead —
 * the narrow span from checkAdd through proposeOrder to approveOrder. Exits,
 * marks, stops and fills stay outside the lock: those are per-document
 * transactions with duplicate short-circuits and are already safe, and a
 * position must never wait on a lock to be protected.
 *
 * The loser does not queue. The deep scan skips new entries for that scan and
 * the strike pass skips striking for that minute, each retrying on its own
 * clock. A stale lock expires on its TTL so a dead worker cannot freeze
 * entries. */
const ENTRY_LOCK_TTL_MS = 10 * 60 * 1000;

function entryLockRef(accountId) {
  const account = String(accountId || "").replace(/[^a-zA-Z0-9_-]/g, "_");
  return A.col(A.COL.jobs).doc(`account_entry_lock_${account}`);
}

async function acquireEntryLock(accountId, owner, { ttlMs = ENTRY_LOCK_TTL_MS } = {}) {
  const ref = entryLockRef(accountId);
  const nowMs = Date.now();
  try {
    return await A.runTransaction(async (tx) => {
      const snap = await tx.get(ref);
      const cur = snap.exists ? snap.data() : {};
      if (Number(cur.expiresAtMs) > nowMs && cur.owner && cur.owner !== owner) {
        return { acquired: false, heldBy: cur.owner,
          heldForMs: nowMs - (Number(cur.acquiredAtMs) || nowMs) };
      }
      tx.set(ref, { kind: "account_entry_lock", accountId, owner,
        acquiredAtMs: nowMs, expiresAtMs: nowMs + ttlMs,
        tookOverFrom: Number(cur.expiresAtMs) > 0 && Number(cur.expiresAtMs) <= nowMs
          ? cur.owner || null : null,
        acquiredAt: A.FV.serverTimestamp() }, { merge: true });
      return { acquired: true, owner };
    });
  } catch (e) {
    /* An unreadable lock must not silently permit two concurrent writers. */
    return { acquired: false, error: String(e.message || e).slice(0, 120) };
  }
}

async function releaseEntryLock(accountId, owner) {
  const ref = entryLockRef(accountId);
  try {
    return await A.runTransaction(async (tx) => {
      const snap = await tx.get(ref);
      if (!snap.exists || snap.data().owner !== owner) return { released: false, reason: "not_owner" };
      tx.set(ref, { expiresAtMs: 0, releasedAt: A.FV.serverTimestamp() }, { merge: true });
      return { released: true };
    });
  } catch (e) { return { released: false, reason: String(e.message || e).slice(0, 120) }; }
}

module.exports = { setActive, clear, active, heartbeat,
  acquireEntryLock, releaseEntryLock, entryLockRef, ENTRY_LOCK_TTL_MS };
