// netlify/functions/visionWarehouse.js
//
// Fully expanded, no placeholders. This function demonstrates these actions:
//   - createCorpus         (makes a new image-based Warehouse corpus)
//   - uploadAndImport      (uploads a base64 image to GCS, then creates a Warehouse asset)
//   - analyzeCorpus        (generates embeddings for the images in your corpus)
//   - createIndex          (creates an index for your corpus)
//   - deployIndex          (creates an index endpoint - you’d eventually deploy your index to it)
//   - search               (submits text or image queries to the index endpoint)
//
// REQUIRED ENV VARS in Netlify:
//   GCP_SERVICE_ACCOUNT_JSON = entire raw JSON of your service account key
//   GCP_PROJECT_NUMBER       = numeric GCP project number (e.g. "123456789012")
//   GCP_BUCKET_NAME          = name of a GCS bucket (e.g. "my-bucket") to store images
//   (optional) GCP_LOCATION  = e.g. "us-central1" (defaults to "us-central1")

const fetch = require("node-fetch"); // For Node < 18
const { GoogleAuth } = require("google-auth-library");
const { Storage } = require("@google-cloud/storage");

// -------------------------------------------------------------------
// 0) Parse environment variables
// -------------------------------------------------------------------
// Updated code in visionWarehouse.js
const serviceAccountRaw = process.env.GCP_SERVICE_ACCOUNT_JSON || "{}";
let serviceAccount;
try {
  serviceAccount = JSON.parse(serviceAccountRaw);
} catch (err) {
  console.error("Error parsing GCP_SERVICE_ACCOUNT_JSON:", err);
  serviceAccount = {};
}
// Now you no longer use process.env.GCP_SERVICE_ACCOUNT_JSON

const projectNumber = process.env.GCP_PROJECT_NUMBER;  // "123456789012"
const bucketName    = process.env.GCP_BUCKET_NAME;     // "my-bucket"
const locationId    = process.env.GCP_LOCATION || "us-central1";

if (!projectNumber) {
  console.error("Missing GCP_PROJECT_NUMBER environment variable!");
}
if (!bucketName) {
  console.error("Missing GCP_BUCKET_NAME environment variable!");
}

// Vertex AI Vision Warehouse v1 endpoint
const WAREHOUSE_API_ROOT = "https://warehouse-visionai.googleapis.com/v1";

// -------------------------------------------------------------------
// 1) Auth for Warehouse
// -------------------------------------------------------------------
const auth = new GoogleAuth({
  credentials: {
    client_email: serviceAccount.client_email,
    private_key: serviceAccount.private_key
  },
  scopes: ["https://www.googleapis.com/auth/cloud-platform"]
});
async function getAccessToken() {
  const client = await auth.getClient();
  const token  = await client.getAccessToken();
  return token.token || token;
}

// -------------------------------------------------------------------
// 2) Google Cloud Storage client for uploading images
// -------------------------------------------------------------------
const gcsStorage = new Storage({
  projectId: serviceAccount.project_id, // No placeholders
  credentials: {
    client_email: serviceAccount.client_email,
    private_key: serviceAccount.private_key
  }
});

// Helper to upload base64 => GCS => returns a "gs://bucket/file" path
async function uploadBase64ToGCS(base64Data, objectKey) {
  // objectKey: e.g. "tempAssets/asset_12345.jpg"
  // parse data URL
  const match = base64Data.match(/^data:(?<mime>[^;]+);base64,(?<base64>.+)$/);
  if (!match || !match.groups) {
    throw new Error("Invalid base64 data URL");
  }
  const mimeType = match.groups.mime;
  const rawBase64 = match.groups.base64;
  const fileBuffer = Buffer.from(rawBase64, "base64");

  const fileRef = gcsStorage.bucket(bucketName).file(objectKey);
  await fileRef.save(fileBuffer, {
    contentType: mimeType,
    resumable: false,
    public: false
  });

  // Return "gs://..."
  return `gs://${bucketName}/${objectKey}`;
}

// -------------------------------------------------------------------
// 3) The Netlify Handler with 6 main actions
// -------------------------------------------------------------------
exports.handler = async (event, context) => {
  try {
    const body   = JSON.parse(event.body || "{}");
    const action = body.action;

    switch (action) {
      // -----------------------------------------------------------------
      // A) createCorpus
      // -----------------------------------------------------------------
      case "createCorpus": {
        // Example body:
        // { "action":"createCorpus", "displayName":"MyWarehouse", "description":"Testing" }
        const displayName = body.displayName || "My Image Warehouse";
        const description = body.description || "No description provided.";
        const url         = `${WAREHOUSE_API_ROOT}/projects/${projectNumber}/locations/${locationId}/corpora`;

        const token  = await getAccessToken();
        const reqBody = {
          display_name: displayName,
          description,
          type: "IMAGE",
          search_capability_setting: {
            search_capabilities: { type: "EMBEDDING_SEARCH" }
          }
        };

        const resp = await fetch(url, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify(reqBody)
        });
        if (!resp.ok) {
          const txt = await resp.text();
          throw new Error(`createCorpus error: ${resp.status} => ${txt}`);
        }
        const data = await resp.json();
        return json200(data);
      }

      // -----------------------------------------------------------------
      // B) uploadAndImport => upload base64 to GCS, create an asset
      // -----------------------------------------------------------------
      case "uploadAndImport": {
        // {
        //   "action":"uploadAndImport",
        //   "corpusName":"projects/1234/locations/us-central1/corpora/4567",
        //   "assetId":"myAsset123",
        //   "base64Image":"data:image/png;base64,ABCD..."
        // }
        const { corpusName, assetId, base64Image } = body;
        if (!corpusName || !assetId || !base64Image) {
          throw new Error("uploadAndImport requires corpusName, assetId, and base64Image");
        }
        // 1) upload to GCS
        const objectKey = `tempAssets/${assetId}_${Date.now()}.jpg`;
        const gsUri     = await uploadBase64ToGCS(base64Image, objectKey);
        console.log("Uploaded to GCS =>", gsUri);

        // 2) create the asset in your corpus
        const token  = await getAccessToken();
        const url    = `${WAREHOUSE_API_ROOT}/${corpusName}/assets?asset_id=${encodeURIComponent(assetId)}`;
        const reqBody = {};

        const resp = await fetch(url, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify(reqBody)
        });
        if (!resp.ok) {
          const txt = await resp.text();
          throw new Error(`uploadAndImport: createAsset error: ${resp.status} => ${txt}`);
        }
        const data = await resp.json();
        return json200({
          message: "Asset creation request returned successfully. Check data for LRO or final resource.",
          gcsUri,
          data
        });
      }

      // -----------------------------------------------------------------
      // C) analyzeCorpus => generate embeddings
      // -----------------------------------------------------------------
      case "analyzeCorpus": {
        // { "action":"analyzeCorpus", "corpusName":"projects/.../locations/us-central1/corpora/1234" }
        const { corpusName } = body;
        if (!corpusName) {
          throw new Error("analyzeCorpus requires corpusName");
        }
        const token = await getAccessToken();
        const url   = `${WAREHOUSE_API_ROOT}/${corpusName}:analyze`;
        const reqBody = { name: corpusName };

        const resp = await fetch(url, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify(reqBody)
        });
        if (!resp.ok) {
          const txt = await resp.text();
          throw new Error(`analyzeCorpus error: ${resp.status} => ${txt}`);
        }
        const data = await resp.json();
        return json200(data);
      }

      // -----------------------------------------------------------------
      // D) createIndex => build an embedding index
      // -----------------------------------------------------------------
      case "createIndex": {
        // {
        //   "action":"createIndex",
        //   "corpusName":"projects/1234/locations/us-central1/corpora/5678",
        //   "displayName":"MyIndex"
        // }
        const { corpusName, displayName, description } = body;
        if (!corpusName) {
          throw new Error("createIndex requires corpusName");
        }
        const dn   = displayName || "MyIndex";
        const desc = description || "No description";
        const url  = `${WAREHOUSE_API_ROOT}/${corpusName}/indexes`;
        const token = await getAccessToken();

        const reqBody = {
          display_name: dn,
          description: desc
        };
        const resp = await fetch(url, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify(reqBody)
        });
        if (!resp.ok) {
          const txt = await resp.text();
          throw new Error(`createIndex error: ${resp.status} => ${txt}`);
        }
        const data = await resp.json();
        return json200(data);
      }

      // -----------------------------------------------------------------
      // E) deployIndex => create index endpoint
      // -----------------------------------------------------------------
      case "deployIndex": {
        // { "action":"deployIndex", "indexName":"projects/.../indexes/..." }
        const { indexName } = body;
        if (!indexName) {
          throw new Error("deployIndex requires indexName");
        }
        const token = await getAccessToken();
        const endpointUrl = `${WAREHOUSE_API_ROOT}/projects/${projectNumber}/locations/${locationId}/indexEndpoints`;

        // create an index endpoint
        const epResp = await fetch(endpointUrl, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify({ display_name: "MyIndexEndpoint" })
        });
        if (!epResp.ok) {
          const txt = await epResp.text();
          throw new Error(`createIndexEndpoint error: ${epResp.status} => ${txt}`);
        }
        const epData = await epResp.json();
        return json200({
          message: "Index endpoint creation (LRO). Next step is deploying your index to it.",
          epData
        });
      }

      // -----------------------------------------------------------------
      // F) search => text or image query
      // -----------------------------------------------------------------
      case "search": {
        // {
        //   "action":"search",
        //   "indexEndpointName":"projects/.../indexEndpoints/ENDPOINT_ID",
        //   "textQuery":"some text" or "imageQueryBase64":"data:image/png;base64,..."
        // }
        const { indexEndpointName, textQuery, imageQueryBase64 } = body;
        if (!indexEndpointName) {
          throw new Error("search requires indexEndpointName");
        }
        const token = await getAccessToken();
        const url   = `${WAREHOUSE_API_ROOT}/${indexEndpointName}:searchIndexEndpoint`;

        const reqBody = {};
        if (textQuery) {
          reqBody.text_query = textQuery;
        }
        if (imageQueryBase64) {
          reqBody.image_query = { input_image: imageQueryBase64 };
        }

        const resp = await fetch(url, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify(reqBody)
        });
        if (!resp.ok) {
          const txt = await resp.text();
          throw new Error(`search error: ${resp.status} => ${txt}`);
        }
        const data = await resp.json();
        return json200(data);
      }

      default:
        return json400({ error: `Unknown action => ${action}` });
    }

  } catch (err) {
    console.error("Vision Warehouse function error =>", err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: err.message })
    };
  }
};

// Helpers
function json200(obj) {
  return { statusCode: 200, body: JSON.stringify(obj) };
}
function json400(obj) {
  return { statusCode: 400, body: JSON.stringify(obj) };
}