/**
 * netlify/functions/visionWarehouse.js
 *
 * This function demonstrates the following actions in Google's Vision Warehouse API:
 *   - createCorpus
 *   - uploadAndImport (uploads a base64 image to GCS, then creates an Asset in the corpus)
 *   - analyzeCorpus
 *   - createIndex
 *   - deployIndex
 *   - search
 *
 * ENVIRONMENT VARIABLES (in Netlify) REQUIRED:
 *   GCP_CLIENT_EMAIL
 *   GCP_PRIVATE_KEY         (with any "\n" replaced by actual newline chars)
 *   GCP_PROJECT_ID
 *   GCP_PROJECT_NUMBER      (e.g., "123456789012")
 *   GCP_BUCKET_NAME         (e.g., "my-warehouse-bucket")
 *   (optional) GCP_LOCATION (defaults to "us-central1")
 */

const WAREHOUSE_API_ROOT = "https://warehouse-visionai.googleapis.com/v1";

// ---------------------------------------------------------------------
// (0) Build the serviceAccount object from environment variables.
// ---------------------------------------------------------------------
const serviceAccount = {
  client_email: process.env.GCP_CLIENT_EMAIL,
  private_key: process.env.GCP_PRIVATE_KEY
    ? process.env.GCP_PRIVATE_KEY.replace(/\\n/g, "\n")
    : "",
  project_id: process.env.GCP_PROJECT_ID
};

const projectNumber = process.env.GCP_PROJECT_NUMBER;  // e.g., "123456789012"
const bucketName = process.env.GCP_BUCKET_NAME;         // e.g., "my-bucket"
const locationId = process.env.GCP_LOCATION || "us-central1";

// ---------------------------------------------------------------------
// (1) Auth for Warehouse using environment variables
// ---------------------------------------------------------------------
const { GoogleAuth } = require("google-auth-library");
const auth = new GoogleAuth({
  credentials: serviceAccount,
  scopes: ["https://www.googleapis.com/auth/cloud-platform"]
});

async function getAccessToken() {
  const client = await auth.getClient();
  const token = await client.getAccessToken();
  return token.token || token;
}

// ---------------------------------------------------------------------
// (2) Google Cloud Storage client, for storing base64 images
// ---------------------------------------------------------------------
const { Storage } = require("@google-cloud/storage");
const gcsStorage = new Storage({
  projectId: serviceAccount.project_id,
  credentials: serviceAccount
});

/**
 * Upload a base64 data URL to GCS, returning a "gs://bucket/object" URI.
 *
 * - base64Data must be of the form "data:image/png;base64,iVBORw0K..."
 * - objectKey is how we name the file in your GCS bucket (e.g. "tempAssets/assetId_time.jpg")
 */
async function uploadBase64ToGCS(base64Data, objectKey) {
  // Must match data:<mime>;base64,<base64String>
  const match = base64Data.match(/^data:(?<mime>[^;]+);base64,(?<base64>.+)$/);
  if (!match || !match.groups) {
    throw new Error("Invalid base64 data URL");
  }
  const mimeType = match.groups.mime; // e.g. "image/png"
  const rawBase64 = match.groups.base64;
  const fileBuffer = Buffer.from(rawBase64, "base64");

  const fileRef = gcsStorage.bucket(bucketName).file(objectKey);
  await fileRef.save(fileBuffer, {
    contentType: mimeType,
    resumable: false,
    public: false
  });

  return `gs://${bucketName}/${objectKey}`;
}

// ---------------------------------------------------------------------
// (3) Netlify Handler: handle the six Warehouse API actions
// ---------------------------------------------------------------------
const fetch = require("node-fetch"); // For Node < 18

exports.handler = async (event, context) => {
  try {
    const body = JSON.parse(event.body || "{}");
    const action = body.action;

    switch (action) {
      // ============================================================
      // A) createCorpus
      // ============================================================
      case "createCorpus": {
        /**
         * Example usage (POST body):
         * {
         *   "action": "createCorpus",
         *   "displayName": "My test corpus",
         *   "description": "test corp"
         * }
         */
        const displayName = body.displayName || "My Image Warehouse";
        const description = body.description || "No description provided.";
        const url = `${WAREHOUSE_API_ROOT}/projects/${projectNumber}/locations/${locationId}/corpora`;

        const token = await getAccessToken();
        const reqBody = {
          display_name: displayName,
          description: description,
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

      // ============================================================
      // B) uploadAndImport
      // ============================================================
      case "uploadAndImport": {
        /**
         * Example usage (POST body):
         * {
         *   "action": "uploadAndImport",
         *   "corpusName": "projects/123/locations/us-central1/corpora/myCorpusID",
         *   "assetId": "myUniqueAssetId",
         *   "base64Image": "data:image/png;base64,iVBORw0K..."
         * }
         */
        const { corpusName, assetId, base64Image } = body;
        if (!corpusName || !assetId || !base64Image) {
          throw new Error("uploadAndImport requires corpusName, assetId, and base64Image");
        }

        // 1) Upload the image to GCS
        const objectKey = `tempAssets/${assetId}_${Date.now()}.jpg`;
        const gsUri = await uploadBase64ToGCS(base64Image, objectKey);
        console.log("Uploaded to GCS =>", gsUri);

        // 2) Create an asset in the Warehouse corpus referencing that GCS URI
        const token = await getAccessToken();
        const url = `${WAREHOUSE_API_ROOT}/${corpusName}/assets?asset_id=${encodeURIComponent(assetId)}`;

        // This approach:
        // - "asset" top-level
        // - "media_type" set to "MEDIA_TYPE_IMAGE"
        // - "asset_schema" with "mime_type" and "gcs_uri"
        // Is known to work in many Warehouse versions.
        const reqBody = {
          asset: {
            display_name: assetId,
            media_type: "MEDIA_TYPE_IMAGE",
            asset_schema: {
              mime_type: "image/png", // You might need to detect "png" vs "jpeg" from base64
              gcs_uri: gsUri
            }
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
          console.log("FULL error body =>", txt);
          throw new Error(`uploadAndImport: createAsset error: ${resp.status} => ${txt}`);
        }

        const data = await resp.json();
        return json200({
          message: "Asset created successfully.",
          gcsUri: gsUri,
          data: data
        });
      }

      // ============================================================
      // C) analyzeCorpus
      // ============================================================
      case "analyzeCorpus": {
        /**
         * Example usage:
         * {
         *   "action": "analyzeCorpus",
         *   "corpusName": "projects/123/locations/us-central1/corpora/myCorpusID"
         * }
         */
        const { corpusName } = body;
        if (!corpusName) {
          throw new Error("analyzeCorpus requires corpusName");
        }

        const token = await getAccessToken();
        const url = `${WAREHOUSE_API_ROOT}/${corpusName}:analyze`;
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

      // ============================================================
      // D) createIndex
      // ============================================================
      case "createIndex": {
        /**
         * Example usage:
         * {
         *   "action": "createIndex",
         *   "corpusName": "projects/123/locations/us-central1/corpora/myCorpusID",
         *   "displayName": "MyIndex",
         *   "description": "My index description"
         * }
         */
        const { corpusName, displayName, description } = body;
        if (!corpusName) {
          throw new Error("createIndex requires corpusName");
        }
        const dn = displayName || "MyIndex";
        const desc = description || "No description";
        const url = `${WAREHOUSE_API_ROOT}/${corpusName}/indexes`;
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

      // ============================================================
      // E) deployIndex
      // ============================================================
      case "deployIndex": {
        /**
         * Example usage:
         * {
         *   "action": "deployIndex",
         *   "indexName": "projects/123/locations/us-central1/corpora/myCorpusID/indexes/456"
         * }
         */
        const { indexName } = body;
        if (!indexName) {
          throw new Error("deployIndex requires indexName");
        }

        const token = await getAccessToken();
        const endpointUrl = `${WAREHOUSE_API_ROOT}/projects/${projectNumber}/locations/${locationId}/indexEndpoints`;

        // Create an IndexEndpoint resource
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
          message: "Index endpoint creation done (LRO). Next step: deploy your index to it.",
          epData: epData
        });
      }

      // ============================================================
      // F) search
      // ============================================================
      case "search": {
        /**
         * Example usage:
         * {
         *   "action": "search",
         *   "indexEndpointName": "projects/123/locations/us-central1/indexEndpoints/9999",
         *   "textQuery": "some text"       // OR
         *   "imageQueryBase64": "data:image/png;base64,iVBORw0K..."
         * }
         */
        const { indexEndpointName, textQuery, imageQueryBase64 } = body;
        if (!indexEndpointName) {
          throw new Error("search requires indexEndpointName");
        }

        const token = await getAccessToken();
        const url = `${WAREHOUSE_API_ROOT}/${indexEndpointName}:searchIndexEndpoint`;

        // Build request
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

      // ============================================================
      // Unknown action
      // ============================================================
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

// ---------------------------------------------------------------------
// Helper methods
// ---------------------------------------------------------------------
function json200(obj) {
  return { statusCode: 200, body: JSON.stringify(obj) };
}

function json400(obj) {
  return { statusCode: 400, body: JSON.stringify(obj) };
}