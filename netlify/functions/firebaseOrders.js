const admin = require("./firebaseAdmin");
const db = admin.firestore();

exports.handler = async (event, context) => {
  try {
    const method = event.httpMethod; // "POST" or "GET"
    let result;

    if (method === "POST") {
      /*
        Expecting a JSON body with these fields, for example:
        {
          "orderNumber": "1234",
          "clientName": "Alice",
          "britesMessages": "Some text",
          "shippingEmployeeID": "EMP-01",
          "shippingLabelTimestamps": "2025-04-01 12:00"
        }
      */

      const body = JSON.parse(event.body);
      const {
        orderNumber,
        clientName,
        britesMessages,
        shippingEmployeeID,
        shippingLabelTimestamps
      } = body;

      // Require an orderNumber to decide the document ID
      if (!orderNumber) {
        return {
          statusCode: 400,
          body: JSON.stringify({ error: "No orderNumber provided" })
        };
      }

      // Build a data object matching your field names exactly
      const dataToStore = {
        "Client Name": clientName || "",
        "Brites Messages": britesMessages || "",
        "Order Number": orderNumber,
        "Shipping Employee ID": shippingEmployeeID || "",
        "Shipping Label Timestamps": shippingLabelTimestamps || ""
      };

      // We store it in the "Brites_Orders" collection, doc ID is the order number
      await db.collection("Brites_Orders")
             .doc(orderNumber)
             .set(dataToStore, { merge: true });

      result = {
        success: true,
        message: `Order ${orderNumber} created/updated in Brites_Orders collection.`
      };

    } else if (method === "GET") {
      /*
        Expecting a query parameter: ?orderId=1234
        This will retrieve the document with ID "1234" from Brites_Orders
      */
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
      result = { success: true, data: docData };

    } else {
      // Only POST and GET are allowed in this example
      return {
        statusCode: 405,
        body: JSON.stringify({ error: "Method Not Allowed" })
      };
    }

    return {
      statusCode: 200,
      body: JSON.stringify(result)
    };

  } catch (error) {
    console.error("Error in firebaseOrders function:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};