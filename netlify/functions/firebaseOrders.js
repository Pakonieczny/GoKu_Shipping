/*  netlify/functions/firebaseOrders.js  */
const admin = require("./firebaseAdmin");
const db    = admin.firestore();

const COMPLETED_COLL = "Design_Completed Orders";
const REALTIME_COLL  = "Design_RealTime_Selected_Orders";

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
        staffNote,
        /* design completion controls */
        designCompleted,
        completedIds,   // array of receipt IDs to mark completed
        uncompleteIds   // array of receipt IDs to unset
      } = body;

      /* ─── Realtime selection locks (write via server) ─── */
      const { rtLockIds, rtUnlockIds, clientId, page } = body;
      if (Array.isArray(rtLockIds) && rtLockIds.length) {
        const batch = db.batch();
        rtLockIds.map(String).forEach((id) => {
          const ref = db.collection(REALTIME_COLL).doc(id);
          batch.set(ref, {
            selected   : true,
            selectedBy : clientId || "server",
            page       : page || "design",
            at         : admin.firestore.FieldValue.serverTimestamp()
          }, { merge:true })
        });
        await batch.commit();
        return { statusCode: 200, headers: CORS,
          body: JSON.stringify({ success:true, message:"Locked", count: rtLockIds.length }) };
      }
      if (Array.isArray(rtUnlockIds) && rtUnlockIds.length) {
        const ids   = rtUnlockIds.map(String);
        const refs  = ids.map(id => db.collection(REALTIME_COLL).doc(id));
        const snaps = await db.getAll(...refs);                            // ← fix
        const batch = db.batch();
        let updCount = 0;
        snaps.forEach((snap) => {
          const allow = !snap.exists || snap.data()?.selectedBy === clientId;
          if (allow){
            batch.set(snap.ref, {
              selected   : false,
              selectedBy : null,
              at         : admin.firestore.FieldValue.serverTimestamp()
            }, { merge:true });
            updCount++;
          }
        });
        if (updCount) await batch.commit();
        return { statusCode: 200, headers: CORS,
          body: JSON.stringify({ success:true, message:"Unlocked", count: updCount }) };
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
          const ref = db.collection(COMPLETED_COLL).doc(String(id));
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
          const ref = db.collection(COMPLETED_COLL).doc(String(id));
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
        await db
          .collection("Brites_Orders")
          .doc(String(orderNumber))
          .collection("messages")
          .add({
            text       : newMessage.trim(),
            senderName : employeeName || "Staff",
            senderRole : "staff",
            timestamp  : admin.firestore.FieldValue.serverTimestamp()
          });

        return {
          statusCode: 200,
          headers: CORS,
          body: JSON.stringify({ success: true, message: "Chat doc added." })
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
        .collection("Brites_Orders")
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


     // ?rtSince=epochMillis  -> return only changes since that moment
     const qs = event.queryStringParameters || {};
     if (qs.rtSince) {
       const since = Number(qs.rtSince);
       const sinceTs = admin.firestore.Timestamp.fromMillis(isNaN(since) ? 0 : since);
       const snap = await db
         .collection(REALTIME_COLL)
         .where('at', '>=', sinceTs)
         .get();

       const locks = {};
       const unlocks = [];
       snap.forEach(d => {
         const data = d.data() || {};
         if (data.selected) {
           locks[d.id] = { selectedBy: data.selectedBy || null, page: data.page || null, at: data.at || null };
         } else {
           unlocks.push(d.id);
         }
       });
       return {
         statusCode: 200,
         headers: CORS,
         body: JSON.stringify({ success: true, locks, unlocks, now: Date.now() })
       };
     }


    /* ───────────────────────── GET ───────────────────────── */
    if (method === "GET") {

      /* ?rtSince=NUMBER(ms) → delta since watermark
      Returns: { locks:{id:{selectedBy,page,atMs}}, unlocks:[id], now:Number } */
      const qSince = event.queryStringParameters?.rtSince;
      if (qSince) {
        const sinceMs = Number(qSince);
        if (!Number.isFinite(sinceMs)) {
          return { statusCode: 400, headers: CORS, body: JSON.stringify({ error:"bad rtSince" }) };
        }
        const sinceTs = new Date(sinceMs);
        const snap = await db.collection(REALTIME_COLL)
          .where("at", ">", sinceTs)
          .get();
        const locks   = {};
        const unlocks = [];
        snap.forEach(d=>{
          const v = d.data() || {};
          if (v.selected === true) {
            locks[d.id] = { selectedBy: v.selectedBy || null, page: v.page || null, atMs: (v.at?.toMillis?.() || Date.now()) };
          } else if (v.selected === false) {
            unlocks.push(d.id);
          }
        });
        return {
          statusCode: 200,
          headers: CORS,
          body: JSON.stringify({ success:true, locks, unlocks, now: Date.now() })
        };
      }

     /* ?rt=1 → current active locks only (selected == true) */
     if (event.queryStringParameters?.rt === "1") {
       const snap = await db.collection(REALTIME_COLL).where("selected","==",true).get();        
       const locks = {};
        snap.forEach(d => { locks[d.id] = d.data(); });
        return {
          statusCode: 200,
          headers: CORS,
          body: JSON.stringify({ success: true, locks })
        };
      }

      /* ?designCompleted=1 → list of completed receipt IDs from Design_Completed Orders */
      if (event.queryStringParameters?.designCompleted === "1") {
        const snap = await db.collection(COMPLETED_COLL).select().get();
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
          .collection("Brites_Orders")
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

      const docSnap = await db.collection("Brites_Orders").doc(String(orderId)).get();

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