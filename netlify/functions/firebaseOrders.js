/*  netlify/functions/firebaseOrders.js  */
const admin = require("./firebaseAdmin");
const db    = admin.firestore();

const COMPLETED_COLL = "Design_Completed Orders";

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

    /* ───────────────────────── GET ───────────────────────── */
    if (method === "GET") {

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