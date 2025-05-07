/*  netlify/functions/firebaseOrders.js  */
const admin = require("./firebaseAdmin");
const db    = admin.firestore();

exports.handler = async (event, context) => {
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
        newMessage              // optional chat text
      } = body;

      if (!orderNumber) {
        return {
          statusCode: 400,
          body: JSON.stringify({ error: "No orderNumber provided" })
        };
      }

      /* 1️⃣  Handle live-chat messages */
      if (typeof newMessage === "string" && newMessage.trim() !== "") {
        await db
          .collection("Brites_Orders")
          .doc(orderNumber)
          .collection("messages")
          .add({
            text       : newMessage.trim(),
            senderName : employeeName || "Staff",
            senderRole : "staff",
            timestamp  : admin.firestore.FieldValue.serverTimestamp()
          });

        return {
          statusCode: 200,
          body: JSON.stringify({ success: true, message: "Chat doc added." })
        };
      }

      /* 2️⃣  Merge order-level fields (only those actually present) */
      const dataToStore = {};
      if (orderNumField           !== undefined) dataToStore["Order Number"]              = orderNumField;
      if (clientName              !== undefined) dataToStore["Client Name"]               = clientName;
      if (britesMessages          !== undefined) dataToStore["Brites Messages"]           = britesMessages;
      if (shippingLabelTimestamps !== undefined) dataToStore["Shipping Label Timestamps"] = shippingLabelTimestamps;
      if (employeeName            !== undefined) dataToStore["Employee Name"]             = employeeName;

      if (Object.keys(dataToStore).length === 0) {
        return {
          statusCode: 200,
          body: JSON.stringify({ success: true, message: "Nothing to update." })
        };
      }

      await db
        .collection("Brites_Orders")
        .doc(orderNumber)
        .set(dataToStore, { merge: true });

      return {
        statusCode: 200,
        body: JSON.stringify({
          success: true,
          message: `Order doc ${orderNumber} created/updated.`
        })
      };
    }

    /* ───────────────────────── GET ───────────────────────── */
    if (method === "GET") {
      const { orderId } = event.queryStringParameters || {};

      if (!orderId) {
        return {
          statusCode: 400,
          body: JSON.stringify({ error: "No orderId query param provided" })
        };
      }

      const docSnap = await db.collection("Brites_Orders").doc(orderId).get();

      if (!docSnap.exists) {
        return {
          statusCode: 404,
          body: JSON.stringify({ error: `Order ${orderId} not found.` })
        };
      }

      return {
        statusCode: 200,
        body: JSON.stringify({ success: true, data: docSnap.data() })
      };
    }

    /* ─────────────────────── fallback ─────────────────────── */
    return {
      statusCode: 405,
      body: JSON.stringify({ error: "Method Not Allowed" })
    };

  } catch (error) {
    console.error("Error in firebaseOrders function:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};