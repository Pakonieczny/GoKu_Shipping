/**
 * netlify/functions/visionWarehouse.js
 *
 * Demonstrates the following Warehouse actions:
 *   - createCorpus
 *   - uploadAndImport
 *   - analyzeCorpus
 *   - createIndex
 *   - deployIndex
 *   - search
 *
 * REQUIRED ENV VARS in Netlify:
 *   GCP_CLIENT_EMAIL
 *   GCP_PRIVATE_KEY
 *   GCP_PROJECT_ID
 *   GCP_PROJECT_NUMBER
 *   GCP_BUCKET_NAME
 *   (optional) GCP_LOCATION (defaults to "us-central1")
 */

const WAREHOUSE_API_ROOT = "https://warehouse-visionai.googleapis.com/v1";

// -------------------------------------------------------------------
// (0) Build the serviceAccount object from environment variables.
// -------------------------------------------------------------------
const serviceAccount = {
  client_email: process.env.GCP_CLIENT_EMAIL,
  private_key: process.env.GCP_PRIVATE_KEY
    ? process.env.GCP_PRIVATE_KEY.replace(/\\n/g, "\n")
    : "",
  project_id: process.env.GCP_PROJECT_ID
};

const projectNumber = process.env.GCP_PROJECT_NUMBER;
const bucketName = process.env.GCP_BUCKET_NAME;
const locationId = process.env.GCP_LOCATION || "us-central1";

// -------------------------------------------------------------------
// (1) Auth for Warehouse using environment variables
// -------------------------------------------------------------------
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

// -------------------------------------------------------------------
// (2) Google Cloud Storage client using environment variables
// -------------------------------------------------------------------
const { Storage } = require("@google-cloud/storage");
const gcsStorage = new Storage({
  projectId: serviceAccount.project_id,
  credentials: serviceAccount
});

/**
 * Helper: Upload base64 image data to GCS and return a "gs://bucket/file" URI.
 * The base64 string must be of the form:
 *   data:image/png;base64,iVBORw0KGgoAAAANS...
 */
async function uploadBase64ToGCS(base64Data, objectKey) {
  const match = base64Data.match(/^data:(?<mime>[^;]+);base64,(?<base64>.+)$/);
  if (!match || !match.groups) {
    throw new Error("Invalid base64 data URL");
  }

  const mimeType = match.groups.mime;         // e.g. "image/png"
  const rawBase64 = match.groups.base64;      // e.g. "iVBORw0K..."
  const fileBuffer = Buffer.from(rawBase64, "base64");

  // Save to your GCS bucket
  const fileRef = gcsStorage.bucket(bucketName).file(objectKey);
  await fileRef.save(fileBuffer, {
    contentType: mimeType,
    resumable: false,
    public: false
  });

  // Return the path to the uploaded file
  return `gs://${bucketName}/${objectKey}`;
}

// -------------------------------------------------------------------
// (3) Netlify Handler: Switch on action for the six Warehouse API actions
// -------------------------------------------------------------------
const fetch = require("node-fetch"); // If Node < 18

exports.handler = async (event, context) => {
  try {
    const body = JSON.parse(event.body || "{}");
    const action = body.action;

    switch (action) {
      //-----------------------------------------------------------
      // A) createCorpus
      //-----------------------------------------------------------
      case "createCorpus": {
        // Body fields: { displayName?: string, description?: string }
        // Example usage:
        // POST to /visionWarehouse with
        // {
        //   "action": "createCorpus",
        //   "displayName": "My Example Corpus",
        //   "description": "Just testing"
        // }
        const displayName = body.displayName || "My Image Warehouse";
        const description = body.description || "No description provided";
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

      //-----------------------------------------------------------
      // B) uploadAndImport: Upload base64 to GCS & create corpus asset
      //-----------------------------------------------------------
      case "uploadAndImport": {
        // Body fields: { corpusName: string, assetId: string, base64Image: string }
        // Example usage:
        // POST to /visionWarehouse with
        // {
        //   "action": "uploadAndImport",
        //   "corpusName": "projects/123/locations/us-central1/corpora/myCorpusID",
        //   "assetId": "someUniqueID",
        //   "base64Image": "data:image/png;base64,iVBORw0K..."
        // }
        const { corpusName, assetId, base64Image } = body;
        if (!corpusName || !assetId || !base64Image) {
          throw new Error(
            "uploadAndImport requires corpusName, assetId, and base64Image"
          );
        }

        // 1) Upload image to GCS
        const objectKey = `tempAssets/${assetId}_${Date.now()}.jpg`;
        const gsUri = await uploadBase64ToGCS(base64Image, objectKey);
        console.log("Uploaded to GCS =>", gsUri);

        // 2) Create an asset in the corpus referencing that GCS file
        const token = await getAccessToken();
        const url = `${WAREHOUSE_API_ROOT}/${corpusName}/assets?asset_id=${encodeURIComponent(assetId)}`;

        // The Warehouse typically wants { "asset": { ...fields... } }
        const reqBody = {
          asset: {
            display_name: assetId,
            media_type: "MEDIA_TYPE_IMAGE",
            data_schema: {
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
          console.log("Warehouse createAsset call failed =>", txt);
          throw new Error(
            `uploadAndImport: createAsset error: ${resp.status} => ${txt}`
          );
        }

        const data = await resp.json();
        return json200({
          message: "Asset creation success!",
          gcsUri: gsUri,
          data: data
        });
      }

      //-----------------------------------------------------------
      // C) analyzeCorpus: Generate embeddings
      //-----------------------------------------------------------
      case "analyzeCorpus": {
        // Body fields: { corpusName: string }
        // Example usage:
        // POST to /visionWarehouse with
        // {
        //   "action": "analyzeCorpus",
        //   "corpusName": "projects/123/locations/us-central1/corpora/myCorpusID"
        // }
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

      //-----------------------------------------------------------
      // D) createIndex: Create an index for the corpus
      //-----------------------------------------------------------
      case "createIndex": {
        // Body fields: { corpusName: string, displayName?: string, description?: string }
        // Example usage:
        // POST to /visionWarehouse with
        // {
        //   "action": "createIndex",
        //   "corpusName": "projects/123/locations/us-central1/corpora/myCorpusID",
        //   "displayName": "MyIndex",
        //   "description": "Index for my corpus"
        // }
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

      //-----------------------------------------------------------
      // E) deployIndex: Create an index endpoint
      //-----------------------------------------------------------
      case "deployIndex": {
        // Body fields: { indexName: string }
        // Example usage:
        // POST to /visionWarehouse with
        // {
        //   "action": "deployIndex",
        //   "indexName": "projects/123/locations/us-central1/corpora/myCorpusID/indexes/456"
        // }
        const { indexName } = body;
        if (!indexName) {
          throw new Error("deployIndex requires indexName");
        }

        const token = await getAccessToken();
        const endpointUrl = `${WAREHOUSE_API_ROOT}/projects/${projectNumber}/locations/${locationId}/indexEndpoints`;

        // Create an index endpoint resource
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
          message: "Index endpoint creation (LRO). Next step is to deploy your index to it.",
          epData: epData
        });
      }

      //-----------------------------------------------------------
      // F) search: Submit text or image queries
      //-----------------------------------------------------------
      case "search": {
        // Body fields: { indexEndpointName: string, textQuery?: string, imageQueryBase64?: string }
        // Example usage:
        // POST to /visionWarehouse with
        // {
        //   "action": "search",
        //   "indexEndpointName": "projects/123/locations/us-central1/indexEndpoints/9999999999",
        //   "textQuery": "Search string"  OR
        //   "imageQueryBase64": "data:image/png;base64,iVBORw0K..."
        // }
        const { indexEndpointName, textQuery, imageQueryBase64 } = body;
        if (!indexEndpointName) {
          throw new Error("search requires indexEndpointName");
        }

        const token = await getAccessToken();
        const url = `${WAREHOUSE_API_ROOT}/${indexEndpointName}:searchIndexEndpoint`;

        // Build your request based on text or image query
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

      //-----------------------------------------------------------
      // Unknown action
      //-----------------------------------------------------------
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

// -------------------------------------------------------------------
// Helper methods
// -------------------------------------------------------------------
function json200(obj) {
  return { statusCode: 200, body: JSON.stringify(obj) };
}

function json400(obj) {
  return { statusCode: 400, body: JSON.stringify(obj) };
}