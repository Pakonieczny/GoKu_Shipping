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

module.exports = { setActive, clear, active, heartbeat };
