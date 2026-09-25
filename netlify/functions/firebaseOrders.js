/*  netlify/functions/firebaseOrders.js  */
const admin = require("./firebaseAdmin");
const db    = admin.firestore();

const COMPLETED_COLL = "Design_Completed Orders";
const REALTIME_COLL  = "Design_RealTime_Selected_Orders";
/* sandbox: ?sandbox=1 keeps every read and write in Sandbox_-prefixed copies of these collections (and of Brites_Orders),
   so a sorter run against the emulated Etsy never touches a real lock, claim, ledger entry or note */
let PREFIX = "";
const col = name => db.collection(PREFIX + name);

/* Released locks and claims stay as tombstones so delta polls see them; nothing reads one a month on. They go here, a page
   at a time and at most once every 10 minutes per instance and workspace (a TTL policy on expireAt, if one is switched on,
   does the same). A lock or claim still held goes only once it is 60 days old: none is held that long. */
const rtSweptAt = new Map();
async function sweepRealtime() {
  if (Date.now() - (rtSweptAt.get(PREFIX) || 0) < 600000) return 0;
  rtSweptAt.set(PREFIX, Date.now());
  try {
    const now = Date.now(), ms = v => (v && typeof v.toMillis === "function" ? v.toMillis() : v instanceof Date ? v.getTime() : Number(v) || 0);
    const snap = await col(REALTIME_COLL).where("at", "<", new Date(now - 30 * 86400000)).limit(200).get();
    const doomed = snap.docs.filter(d => { const v = d.data() || {}; return !(v.selected === true || v.claimed === true) || ms(v.at) < now - 60 * 86400000; });
    if (!doomed.length) return 0;
    const batch = db.batch(); doomed.forEach(d => batch.delete(d.ref)); await batch.commit();
    return doomed.length;
  } catch (e) { console.warn("[firebaseOrders] realtime sweep:", e && e.message); return 0; }
}

/* Global CORS headers */
const CORS = {
  "Access-Control-Allow-Origin" : "*",
  "Access-Control-Allow-Headers": "Content-Type,Authorization",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
};

exports.handler = async (event) => {
  /* Pre-flight */
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }

  try {
    const method = event.httpMethod;
    PREFIX = (event.queryStringParameters && event.queryStringParameters.sandbox === "1") || (event.headers && (event.headers["x-sandbox"] === "1" || event.headers["X-Sandbox"] === "1")) ? "Sandbox_" : "";

    /* ───────────────────────── POST ───────────────────────── */
    if (method === "POST") {
      const body = JSON.parse(event.body || "{}");
      const {
        orderNumber,
        orderNumField,
        clientName,
        britesMessages,
        shippingLabelTimestamps,
        employeeName,
        newMessage,
        designSetId,
        staffNote,
        /* design completion controls */
        designCompleted,
        completedIds,   // array of receipt IDs to mark completed
        uncompleteIds   // array of receipt IDs to unset
      } = body;

      /* A released claim or lock stays as a tombstone so delta polls see it, then goes: it carries expireAt (a Firestore
         TTL policy on that field deletes it 30 days on). A document still claimed or still selected never carries it, so
         a live claim or lock is never removed. */
      const TOMBSTONE_DAYS = 30;
      const expiries = async (ids, stillHeld) => {
        const snaps = await db.getAll(...ids.map((id) => col(REALTIME_COLL).doc(id)));
        return snaps.map((s) => (s.exists && stillHeld(s.data() || {}) ? admin.firestore.FieldValue.delete() : new Date(Date.now() + TOMBSTONE_DAYS * 86400000)));
      };

      /* ─── Sorter claims: a gold dot ("in a sorter run"), never a lock. The other bench keeps notes and chat. ─── */
      const { rtClaimIds, rtUnclaimIds, claimedBy, claimRun } = body;
      if (Array.isArray(rtClaimIds) && rtClaimIds.length) {
        const batch = db.batch();
        rtClaimIds.map(String).slice(0, 500).forEach((id) => {
          batch.set(col(REALTIME_COLL).doc(id), { claimed: true, claimedBy: String(claimedBy || "sorter"), claimRun: claimRun ? String(claimRun) : null, claimAt: admin.firestore.FieldValue.serverTimestamp(), at: admin.firestore.FieldValue.serverTimestamp(), expireAt: admin.firestore.FieldValue.delete() }, { merge: true });
        });
        await batch.commit();
        return { statusCode: 200, headers: CORS, body: JSON.stringify({ success: true, message: "Claimed", count: rtClaimIds.length }) };
      }
      if (Array.isArray(rtUnclaimIds) && rtUnclaimIds.length) {
        const ids = rtUnclaimIds.map(String).slice(0, 500), expireAt = await expiries(ids, (d) => d.selected === true);
        const batch = db.batch();
        ids.forEach((id, i) => {
          batch.set(col(REALTIME_COLL).doc(id), { claimed: false, claimedBy: null, claimRun: null, at: admin.firestore.FieldValue.serverTimestamp(), expireAt: expireAt[i] }, { merge: true });
        });
        await batch.commit();
        await sweepRealtime();
        return { statusCode: 200, headers: CORS, body: JSON.stringify({ success: true, message: "Unclaimed", count: rtUnclaimIds.length }) };
      }

      /* ─── Realtime selection locks (write via server) ───
         The station sends the orders it selected and those it let go in one request: both are written (the unlocks used
         to be dropped whenever locks came with them, so a let-go order stayed "being worked on at another station"). */
      const { rtLockIds, rtUnlockIds, clientId, page } = body;
      const lockIds = Array.isArray(rtLockIds) ? [...new Set(rtLockIds.map(String))] : [];
      const unlockIds = Array.isArray(rtUnlockIds) ? [...new Set(rtUnlockIds.map(String))].filter((id) => !lockIds.includes(id)) : [];
      if (lockIds.length || unlockIds.length) {
        /* 🔓 De-select → write a tombstone so delta polls see it */
        const expireAt = unlockIds.length ? await expiries(unlockIds, (d) => d.claimed === true) : [];
        const writes = [
          ...lockIds.map((id) => [id, {
            selected   : true,
            selectedBy : clientId || "server",
            page       : page || "design",
            at         : admin.firestore.FieldValue.serverTimestamp(),
            expireAt   : admin.firestore.FieldValue.delete()
          }]),
          ...unlockIds.map((id, i) => [id, {
            selected   : false,
            selectedBy : null,
            page       : null,
            at         : admin.firestore.FieldValue.serverTimestamp(),
            expireAt   : expireAt[i]
          }])
        ];
        for (let i = 0; i < writes.length; i += 450) {
          const batch = db.batch();
          writes.slice(i, i + 450).forEach(([id, v]) => batch.set(col(REALTIME_COLL).doc(id), v, { merge: true }));
          await batch.commit();
        }
        if (unlockIds.length) await sweepRealtime();
        return {
          statusCode: 200,
          headers: CORS,
          body: JSON.stringify({
            success: true,
            message: lockIds.length && unlockIds.length ? "Locked and unlocked" : lockIds.length ? "Locked" : "Unlocked (tombstone)",
            count: lockIds.length + unlockIds.length,
            locked: lockIds.length,
            unlocked: unlockIds.length
          })
        };
      }

      // If nothing actionable, short-circuit
      if (
        !orderNumber &&
        !Array.isArray(completedIds) &&
        !Array.isArray(uncompleteIds) &&
        !(typeof designCompleted === "boolean")
      ) {
        return {
          statusCode: 400,
          headers: CORS,
          body: JSON.stringify({ error: "No actionable fields provided" })
        };
      }

      /* 0) Bulk set completed → Design_Completed Orders */
      if (Array.isArray(completedIds) && completedIds.length) {
        const batch = db.batch();
        completedIds.forEach((id) => {
          const ref = col(COMPLETED_COLL).doc(String(id));
          batch.set(
            ref,
            {
              orderId     : String(id),
              completed   : true,
              completedAt : admin.firestore.FieldValue.serverTimestamp()
            },
            { merge: true }
          );
        });
        await batch.commit();
        return {
          statusCode: 200,
          headers: CORS,
          body: JSON.stringify({
            success: true,
            message: "Marked completed (bulk)",
            count: completedIds.length
          })
        };
      }

      /* 0b) Bulk UN-set completed → delete from Design_Completed Orders */
      if (Array.isArray(uncompleteIds) && uncompleteIds.length) {
        const batch = db.batch();
        uncompleteIds.forEach((id) => {
          const ref = col(COMPLETED_COLL).doc(String(id));
          batch.delete(ref);
        });
        await batch.commit();
        return {
          statusCode: 200,
          headers: CORS,
          body: JSON.stringify({
            success: true,
            message: "Unmarked completed (bulk)",
            count: uncompleteIds.length
          })
        };
      }

      /* 1) Live-chat messages */
      if (typeof newMessage === "string" && newMessage.trim() !== "") {
        if (!orderNumber) {
          return {
            statusCode: 400,
            headers: CORS,
            body: JSON.stringify({ error: "orderNumber required for messages" })
          };
        }
        if (designSetId !== undefined && (typeof designSetId !== "string" || !designSetId.trim() || designSetId.length > 80 || newMessage.trim() !== "DESIGNED :)")) {
          return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: "Invalid design completion message" }) };
        }
        const messages = db
          .collection(PREFIX + "Brites_Orders")
          .doc(String(orderNumber))
          .collection("messages");
        const message = {
            text       : newMessage.trim(),
            senderName : employeeName || "Staff",
            senderRole : "staff",
            timestamp  : admin.firestore.FieldValue.serverTimestamp()
          };
        // an image the sender uploaded first (the stations write the same field themselves)
        if (typeof body.imageUrl === "string" && /^https:\/\/[^\s"<>]{8,1900}$/.test(body.imageUrl)) message.imageUrl = body.imageUrl;
        // One internal completion message per order and set, even after a lost
        // response, a station reload or simultaneous retries from two stations.
        // A message sent from a browser's outbox carries its own id, so a send retried after a reload, a lost answer or a
        // dropped connection is written once.
        const clientId = typeof body.clientMessageId === "string" && /^[\w-]{8,80}$/.test(body.clientMessageId) ? body.clientMessageId : null;
        const messageId = designSetId !== undefined ? "designed-set-" + encodeURIComponent(designSetId) : clientId ? "c-" + clientId : null;
        if (messageId) {
          const ref = messages.doc(messageId);
          await db.runTransaction(async tx => {
            const prior = await tx.get(ref);
            if (!prior.exists) tx.set(ref, designSetId !== undefined ? { ...message, setId: designSetId } : message);
          });
        } else await messages.add(message);

        return {
          statusCode: 200,
          headers: CORS,
          body: JSON.stringify({ success: true, message: "Chat doc added.", ...(messageId ? { messageId } : {}) })
        };
      }

      /* 2) Merge order-level fields on Brites_Orders (optional dual-write flag) */
      const dataToStore = {};
      if (orderNumField           !== undefined) dataToStore["Order Number"]              = orderNumField;
      if (clientName              !== undefined) dataToStore["Client Name"]               = clientName;
      if (britesMessages          !== undefined) dataToStore["Brites Messages"]           = britesMessages;
      if (shippingLabelTimestamps !== undefined) dataToStore["Shipping Label Timestamps"] = shippingLabelTimestamps;
      if (employeeName            !== undefined) dataToStore["Employee Name"]             = employeeName;
      if (staffNote               !== undefined) dataToStore["Staff Note"]                = staffNote;
      if (typeof designCompleted  === "boolean") {
        dataToStore["Design Completed"] = !!designCompleted;
        if (designCompleted) {
          dataToStore["Design Completed At"] = admin.firestore.FieldValue.serverTimestamp();
        }
      }

      if (Object.keys(dataToStore).length === 0) {
        return {
          statusCode: 200,
          headers: CORS,
          body: JSON.stringify({ success: true, message: "Nothing to update." })
        };
      }

      if (!orderNumber) {
        return {
          statusCode: 400,
          headers: CORS,
          body: JSON.stringify({ error: "orderNumber required for order updates" })
        };
      }

      await db
        .collection(PREFIX + "Brites_Orders")
        .doc(String(orderNumber))
        .set(dataToStore, { merge: true });

      return {
        statusCode: 200,
        headers: CORS,
        body: JSON.stringify({
          success: true,
          message: `Order doc ${String(orderNumber)} created/updated.`
        })
      };
    }

    /* ───────────────────────── GET ───────────────────────── */

    if (method === "GET") {
      // helper: parse "a,b,c" → ["a","b","c"]
      const parseIds = (s) =>
        String(s || "")
          .split(",")
          .map(x => x.trim())
          .filter(Boolean);

      /* ?messagesFor=rid1,rid2[&limit=80][&since=ms] → the internal thread of each order, oldest first. The real thread is
         always read (only read), so a sandbox run shows what the stations wrote on the real order; the sandbox's own
         messages come from its copy and carry sandbox: true. */
      if (event.queryStringParameters?.messagesFor) {
        const q = event.queryStringParameters;
        const ids = [...new Set(parseIds(q.messagesFor))].filter(id => /^[\w-]{1,40}$/.test(id)).slice(0, 40);
        const limit = Math.max(1, Math.min(200, Number(q.limit) || 80));
        const since = Number(q.since) || 0;
        const ms = t => (t && typeof t.toMillis === "function" ? t.toMillis() : t && t.seconds ? t.seconds * 1000 : null);
        const read = async (coll, id, sandbox) => {
          let ref = db.collection(coll).doc(id).collection("messages");
          if (since) ref = ref.where("timestamp", ">", admin.firestore.Timestamp.fromMillis(since));
          const snap = await ref.orderBy("timestamp", "desc").limit(limit).get();
          return snap.docs.map(d => { const m = d.data() || {}; return Object.assign({ id: d.id, senderName: String(m.senderName || "Staff"), senderRole: String(m.senderRole || "staff"), text: String(m.text || ""), imageUrl: m.imageUrl || null, at: ms(m.timestamp) }, sandbox ? { sandbox: true } : {}); });
        };
        const byOrder = {}; let next = 0;
        const worker = async () => {
          while (next < ids.length) {
            const id = ids[next++];
            const parts = await Promise.all([read("Brites_Orders", id, false)].concat(PREFIX ? [read(PREFIX + "Brites_Orders", id, true)] : []));
            byOrder[id] = [].concat(...parts).sort((a, b) => (a.at || 0) - (b.at || 0)).slice(-limit);
          }
        };
        await Promise.all(Array.from({ length: Math.min(5, ids.length) }, worker));
        return { statusCode: 200, headers: CORS, body: JSON.stringify({ success: true, byOrder, now: Date.now() }) };
      }

      /* ?dcFor=rid1,rid2 → return subset that exist in Design_Completed Orders */
      if (event.queryStringParameters?.dcFor) {
        const ids = parseIds(event.queryStringParameters.dcFor);
        if (!ids.length) {
          return { statusCode: 200, headers: CORS, body: JSON.stringify({ success:true, orderNumbers: [] }) };
        }
        // Query matching completed records, rather than billing a document get
        // for every not-yet-completed order on every background poll. The station
        // now asks about every open order on each sweep (100 ids a request), so the
        // ten-id queries run five at a time rather than one after another; the
        // answer keeps the order of the ids asked about.
        const groups=[];
        for(let i=0;i<ids.length;i+=10)groups.push(ids.slice(i,i+10));
        const found=new Array(groups.length);let next=0;
        const worker=async()=>{while(next<groups.length){const g=next++;const snap=await col(COMPLETED_COLL).where(admin.firestore.FieldPath.documentId(),"in",groups[g]).get();found[g]=snap.docs.map(d=>d.id);}};
        await Promise.all(Array.from({length:Math.min(5,groups.length)},worker));
        const present=[].concat(...found);
        return {
          statusCode: 200, headers: CORS,
          body: JSON.stringify({ success:true, orderNumbers: present, now: Date.now() })
        };
      }

      /* ?staffNotesFor=rid1,rid2 → which of these Brites_Orders have a non-empty "Staff Note" */
      if (event.queryStringParameters?.staffNotesFor) {
        const ids = parseIds(event.queryStringParameters.staffNotesFor);
        if (!ids.length) {
          return { statusCode: 200, headers: CORS, body: JSON.stringify({ success:true, orderNumbers: [] }) };
        }
        // Every sweep of the station now asks about all of its open orders (100 ids a
        // request): ten-id queries, five at a time, return only the orders that have a
        // record at all, instead of a billed document get per order.
        const uniq = [...new Set(ids.map(String))], groups = [];
        for (let i = 0; i < uniq.length; i += 10) groups.push(uniq.slice(i, i + 10));
        const noted = new Set(); let next = 0;
        const worker = async () => {
          while (next < groups.length) {
            const g = groups[next++];
            const snap = await db.collection(PREFIX + "Brites_Orders").where(admin.firestore.FieldPath.documentId(), "in", g).get();
            snap.docs.forEach(d => { const note = ((d.data() || {})["Staff Note"] ?? "").toString().trim(); if (note) noted.add(d.id); });
          }
        };
        await Promise.all(Array.from({ length: Math.min(5, groups.length) }, worker));
        const withNotes = ids.filter(id => noted.has(String(id)));
        return {
          statusCode: 200, headers: CORS,
          body: JSON.stringify({ success:true, orderNumbers: withNotes, now: Date.now() })
        };
      }

      /* ?rtFor=rid1,rid2 → lock state only for these ids in REALTIME_COLL */
      if (event.queryStringParameters?.rtFor) {
        const ids = parseIds(event.queryStringParameters.rtFor);
        if (!ids.length) {
          return { statusCode: 200, headers: CORS, body: JSON.stringify({ success:true, locks: {} }) };
        }
        const refs = ids.map(id => col(REALTIME_COLL).doc(String(id)));
        const snaps = await Promise.all(refs.map(r => r.get()));
        const locks = {}, claims = {};
        snaps.forEach((snap, i) => {
          if (!snap.exists) return;
          const v = snap.data() || {};
          if (v.selected === true) locks[ids[i]] = v;
          if (v.claimed === true) claims[ids[i]] = { claimedBy: v.claimedBy || "sorter", run: v.claimRun || null, atMs: (v.claimAt?.toMillis?.() || 0) };
        });
        return {
          statusCode: 200, headers: CORS,
          body: JSON.stringify({ success:true, locks, claims, now: Date.now() })
        };
      }
      
      /* ?rtSince=NUMBER(ms) → delta since watermark (server-accurate boundary)
         Returns: { locks:{id:{selectedBy,page,atMs}}, unlocks:[id], now:Number } */
      const qSince = event.queryStringParameters?.rtSince;
      if (qSince) {
        const sinceMs = Number(qSince);
        if (!Number.isFinite(sinceMs)) {
          return { statusCode: 400, headers: CORS, body: JSON.stringify({ error:"bad rtSince" }) };
        }
        const sinceTs = admin.firestore.Timestamp.fromMillis(sinceMs);
        const snap = await col(REALTIME_COLL)
          .where("at", ">=", sinceTs)
          .get();

        const locks   = {};
        const unlocks = [];
        const claims  = {};
        const unclaims = [];
        snap.forEach(d=>{
          const v = d.data() || {};
          if (v.selected === true) {
            locks[d.id] = {
              selectedBy: v.selectedBy || null,
              page     : v.page || null,
              atMs     : (v.at?.toMillis?.() || Date.now())
            };
          } else if (v.selected === false) {
            unlocks.push(d.id);
          }
          if (v.claimed === true) claims[d.id] = { claimedBy: v.claimedBy || "sorter", run: v.claimRun || null, atMs: (v.claimAt?.toMillis?.() || Date.now()) };
          else if (v.claimed === false) unclaims.push(d.id);
        });

        return {
          statusCode: 200,
          headers: CORS,
          body: JSON.stringify({ success:true, locks, unlocks, claims, unclaims, now: Date.now() })
        };
      }

      /* ?rt=1 → current active locks only (selected == true) */
      if (event.queryStringParameters?.rt === "1") {
        const snap = await col(REALTIME_COLL).where("selected","==",true).get();
        const locks = {};
        snap.forEach(d => { locks[d.id] = d.data(); });
        const csnap = await col(REALTIME_COLL).where("claimed","==",true).get();
        const claims = {};
        csnap.forEach(d => { const v = d.data() || {}; claims[d.id] = { claimedBy: v.claimedBy || "sorter", run: v.claimRun || null, atMs: (v.claimAt?.toMillis?.() || 0) }; });
        return {
          statusCode: 200,
          headers: CORS,
          body: JSON.stringify({ success: true, locks, claims })
        };
      }

       /* ?dcSince=NUMBER(ms) → completed IDs changed since watermark */
     if (event.queryStringParameters?.dcSince) {
       const sinceMs = Number(event.queryStringParameters.dcSince);
       if (!Number.isFinite(sinceMs)) {
         return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: "bad dcSince" }) };
       }
       const sinceTs = admin.firestore.Timestamp.fromMillis(sinceMs);
       const snap = await col(COMPLETED_COLL)
         .where("completedAt", ">=", sinceTs)
         .select()
         .get();
       return {
         statusCode: 200,
         headers: CORS,
         body: JSON.stringify({
           success: true,
           orderNumbers: snap.docs.map(d => d.id),
           now: Date.now()
         })
       };
     }

      /* ?designCompleted=1 → list of completed receipt IDs from Design_Completed Orders */
      if (event.queryStringParameters?.designCompleted === "1") {
        const snap = await col(COMPLETED_COLL).select().get();
        return {
          statusCode: 200,
          headers: CORS,
          body: JSON.stringify({
            success      : true,
            orderNumbers : snap.docs.map((d) => d.id)
          })
        };
      }

      /* ?staffNotes=1 → array of order IDs with a Staff Note in Brites_Orders */
      if (event.queryStringParameters?.staffNotes === "1") {
        const snap = await db
          .collection(PREFIX + "Brites_Orders")
          .where("Staff Note", "!=", "")
          .select()
          .get();
        return {
          statusCode: 200,
          headers: CORS,
          body: JSON.stringify({
            success      : true,
            orderNumbers : snap.docs.map((d) => d.id)
          })
        };
      }

      /* Single-order fetch (legacy path) */
      const { orderId } = event.queryStringParameters || {};
      if (!orderId) {
        return {
          statusCode: 400,
          headers: CORS,
          body: JSON.stringify({ success: false, msg: "orderId required" })
        };
      }

      const docSnap = await db.collection(PREFIX + "Brites_Orders").doc(String(orderId)).get();

      if (!docSnap.exists) {
        return {
          statusCode: 200,
          headers: CORS,
          body: JSON.stringify({ success: false, notFound: true })
        };
      }

      return {
        statusCode: 200,
        headers: CORS,
        body: JSON.stringify({ success: true, data: docSnap.data() })
      };
    }

    /* Fallback */
    return {
      statusCode: 405,
      headers: CORS,
      body: JSON.stringify({ error: "Method Not Allowed" })
    };
  } catch (error) {
    console.error("Error in firebaseOrders function:", error);
    return {
      statusCode: 500,
      headers: CORS,
      body: JSON.stringify({ error: error.message })
    };
  }
};
