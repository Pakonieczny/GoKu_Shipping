/* one-shot helper – push CORS rule to gokudatabase.appspot.com */
const { Storage } = require("@google-cloud/storage");

(async () => {
  const storage = new Storage({
    credentials: {
      client_email: process.env.FIREBASE_CLIENT_EMAIL,
      private_key : process.env.FIREBASE_PRIVATE_KEY.replace(/\\n/g, "\n"),
      project_id  : process.env.FIREBASE_PROJECT_ID
    }
  });

  await storage.bucket("gokudatabase.firebasestorage.app")
               .setCorsConfiguration([{
                 origin        : ["https://shipping-1.goldenspike.app"],
                 method        : ["GET","POST","PUT","DELETE","HEAD","OPTIONS"],
                 responseHeader: ["Content-Type","Authorization"],
                 maxAgeSeconds : 3600
               }]);

  console.log("✅  CORS rule applied.");
})();