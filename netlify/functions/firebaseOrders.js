const admin = require("./firebaseAdmin");
const db = admin.firestore();

exports.handler = async (event, context) => {
  try {
    const method = event.httpMethod;
    if (method === "POST") {
      const body = JSON.parse(event.body);
      const { orderNumber, orderNumField, clientName, britesMessages } = body;

      if (!orderNumber) {
        return {
          statusCode: 400,
          body: JSON.stringify({ error: "No orderNumber provided" })
        };
      }

      const dataToStore = {
        "Order Number": orderNumField || "",
        "Client Name": clientName || "",
        "Brites Messages": britesMessages || ""
      };

      await db.collection("Brites_Orders")
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
    else if (method === "GET") {
      const { orderId } = event.queryStringParameters || {};
      if (!orderId) {
        return {
          statusCode: 400,
          body: JSON.stringify({ error: "No orderId query param provided" })
        };
      }
      const docRef = db.collection("Brites_Orders").doc(orderId);
      const docSnap = await docRef.get();
      if (!docSnap.exists) {
        return {
          statusCode: 404,
          body: JSON.stringify({ error: `Order ${orderId} not found.` })
        };
      }
      const docData = docSnap.data();
      return {
        statusCode: 200,
        body: JSON.stringify({ success: true, data: docData })
      };
    }
    else {
      return {
        statusCode: 405,
        body: JSON.stringify({ error: "Method Not Allowed" })
      };
    }
  } catch (error) {
    console.error("Error in firebaseOrders function:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};