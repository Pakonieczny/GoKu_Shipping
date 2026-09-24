/*  netlify/functions/firebaseOrders.js  */
const admin = require("./firebaseAdmin");
const db    = admin.firestore();

const COMPLETED_COLL = "Design_Completed Orders";
const REALTIME_COLL  = "Design_RealTime_Selected_Orders";
/* sandbox: ?sandbox=1 keeps every read and write in Sandbox_-prefixed copies of these collections (and of Brites_Orders),
   so a sorter run against the emulated Etsy never touches a real lock, claim, ledger entry or note */
let PREFIX = "";
const col = name => db.collection(PREFIX + name);

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

      /* ─── Sorter claims: a gold dot ("in a sorter run"), never a lock. The other bench keeps notes and chat. ─── */
      const { rtClaimIds, rtUnclaimIds, claimedBy, claimRun } = body;
      if (Array.isArray(rtClaimIds) && rtClaimIds.length) {
        const batch = db.batch();
        rtClaimIds.map(String).slice(0, 500).forEach((id) => {
          batch.set(col(REALTIME_COLL).doc(id), { claimed: true, claimedBy: String(claimedBy || "sorter"), claimRun: claimRun ? String(claimRun) : null, claimAt: admin.firestore.FieldValue.serverTimestamp(), at: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
        });
        await batch.commit();
        return { statusCode: 200, headers: CORS, body: JSON.stringify({ success: true, message: "Claimed", count: rtClaimIds.length }) };
      }
      if (Array.isArray(rtUnclaimIds) && rtUnclaimIds.length) {
        const batch = db.batch();
        rtUnclaimIds.map(String).slice(0, 500).forEach((id) => {
          batch.set(col(REALTIME_COLL).doc(id), { claimed: false, claimedBy: null, claimRun: null, at: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
        });
        await batch.commit();
        return { statusCode: 200, headers: CORS, body: JSON.stringify({ success: true, message: "Unclaimed", count: rtUnclaimIds.length }) };
      }

      /* ─── Realtime selection locks (write via server) ─── */
      const { rtLockIds, rtUnlockIds, clientId, page } = body;
      if (Array.isArray(rtLockIds) && rtLockIds.length) {
        const batch = db.batch();
        rtLockIds.map(String).forEach((id) => {
          const ref = col(REALTIME_COLL).doc(id);
          batch.set(ref, {
            selected   : true,
            selectedBy : clientId || "server",
            page       : page || "design",
            at         : admin.firestore.FieldValue.serverTimestamp()
          }, { merge:true });
        });
        await batch.commit();
        return {
          statusCode: 200,
          headers: CORS,
          body: JSON.stringify({ success:true, message:"Locked", count: rtLockIds.length })
        };
      }

      /* 🔓 De-select → write a tombstone so delta polls see it */
      if (Array.isArray(rtUnlockIds) && rtUnlockIds.length) {
        const ids = rtUnlockIds.map(String);
        const batch = db.batch();
        ids.forEach((id) => {
          const ref = col(REALTIME_COLL).doc(id);
          batch.set(ref, {
            selected   : false,
            selectedBy : null,
            page       : null,
            at         : admin.firestore.FieldValue.serverTimestamp()
          }, { merge: true });
        });
        await batch.commit();
        return {
          statusCode: 200,
          headers: CORS,
          body: JSON.stringify({
            success: true,
            message: "Unlocked (tombstone)",
            count: ids.length
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
        // One internal completion message per order and set, even after a lost
        // response, a station reload or simultaneous retries from two stations.
        const messageId = designSetId === undefined ? null : "designed-set-" + encodeURIComponent(designSetId);
        if (messageId) {
          const ref = messages.doc(messageId);
          await db.runTransaction(async tx => {
            const prior = await tx.get(ref);
            if (!prior.exists) tx.set(ref, { ...message, setId: designSetId });
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
