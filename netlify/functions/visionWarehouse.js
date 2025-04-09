// netlify/functions/visionWarehouse.js
//
// Fully expanded, no placeholders. This function demonstrates the following actions:
//   - createCorpus         (makes a new image-based Warehouse corpus)
//   - uploadAndImport      (uploads a base64 image to GCS, then creates a Warehouse asset)
//   - analyzeCorpus        (generates embeddings for the images in your corpus)
//   - createIndex          (creates an index for your corpus)
//   - deployIndex          (creates an index endpoint - you’d eventually deploy your index to it)
//   - search               (submits text or image queries to the index endpoint)
//
// REQUIRED ENV VARS in Netlify:
//   GCP_CLIENT_EMAIL
//   GCP_PRIVATE_KEY           (this should NOT contain your key directly; it’s loaded from disk)
//   GCP_PROJECT_ID
//   GCP_PROJECT_NUMBER        (e.g., "123456789012")
//   GCP_BUCKET_NAME           (e.g., "my-bucket")
//   (optional) GCP_LOCATION   (e.g., "us-central1", defaults to "us-central1")
//
// This version loads the GCP private key from a local file (stored in a "secrets" folder) and
// uses environment variables for the remaining credentials. Make sure your secrets folder is
// copied into the functions bundle during build (using a prebuild script) and is excluded from
// version control (added to .gitignore).

const fs = require('fs');
const path = require('path');

// Read the private key from disk (the file should be copied into your functions bundle)
const privateKeyPath = path.join(__dirname, 'secrets', 'GCP_PRIVATE_KEY.txt');
let gcpPrivateKey;
try {
  gcpPrivateKey = fs.readFileSync(privateKeyPath, 'utf8');
} catch (err) {
  console.error("Error reading GCP private key file:", err);
  gcpPrivateKey = ""; // Fallback to an empty string (you may want to handle this more gracefully)
}

// Build the serviceAccount object using environment variables plus the private key from file.
const serviceAccount = {
  client_email: process.env.GCP_CLIENT_EMAIL,
  // Replace any escaped newlines with actual newline characters
  private_key: gcpPrivateKey.replace(/\\n/g, '\n'),
  project_id: process.env.GCP_PROJECT_ID
};

const projectNumber = process.env.GCP_PROJECT_NUMBER;  // e.g., "123456789012"
const bucketName    = process.env.GCP_BUCKET_NAME;         // e.g., "my-bucket"
const locationId    = process.env.GCP_LOCATION || "us-central1";

if (!projectNumber) {
  console.error("Missing GCP_PROJECT_NUMBER environment variable!");
}
if (!bucketName) {
  console.error("Missing GCP_BUCKET_NAME environment variable!");
}

// Vertex AI Vision Warehouse v1 endpoint
const WAREHOUSE_API_ROOT = "https://warehouse-visionai.googleapis.com/v1";

const fetch = require("node-fetch"); // For Node < 18
const { GoogleAuth } = require("google-auth-library");
const { Storage } = require("@google-cloud/storage");

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
  projectId: serviceAccount.project_id,
  credentials: {
    client_email: serviceAccount.client_email,
    private_key: serviceAccount.private_key
  }
});

// Helper to upload base64 data to GCS and return a "gs://bucket/file" URI
async function uploadBase64ToGCS(base64Data, objectKey) {
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

  return `gs://${bucketName}/${objectKey}`;
}

// -------------------------------------------------------------------
// 3) The Netlify Handler with six main actions
// -------------------------------------------------------------------
exports.handler = async (event, context) => {
  try {
    const body = JSON.parse(event.body || "{}");
    const action = body.action;

    switch (action) {
      // A) createCorpus
      case "createCorpus": {
        const displayName = body.displayName || "My Image Warehouse";
        const description = body.description || "No description provided.";
        const url = `${WAREHOUSE_API_ROOT}/projects/${projectNumber}/locations/${locationId}/corpora`;

        const token = await getAccessToken();
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

      // B) uploadAndImport: Upload base64 image to GCS, then create an asset
      case "uploadAndImport": {
        const { corpusName, assetId, base64Image } = body;
        if (!corpusName || !assetId || !base64Image) {
          throw new Error("uploadAndImport requires corpusName, assetId, and base64Image");
        }
        const objectKey = `tempAssets/${assetId}_${Date.now()}.jpg`;
        const gsUri = await uploadBase64ToGCS(base64Image, objectKey);
        console.log("Uploaded to GCS =>", gsUri);

        const token = await getAccessToken();
        const url = `${WAREHOUSE_API_ROOT}/${corpusName}/assets?asset_id=${encodeURIComponent(assetId)}`;
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

      // C) analyzeCorpus: Generate embeddings
      case "analyzeCorpus": {
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

      // D) createIndex: Build an embedding index
      case "createIndex": {
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

      // E) deployIndex: Create an index endpoint
      case "deployIndex": {
        const { indexName } = body;
        if (!indexName) {
          throw new Error("deployIndex requires indexName");
        }
        const token = await getAccessToken();
        const endpointUrl = `${WAREHOUSE_API_ROOT}/projects/${projectNumber}/locations/${locationId}/indexEndpoints`;

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

      // F) search: Submit text or image queries to the index endpoint
      case "search": {
        const { indexEndpointName, textQuery, imageQueryBase64 } = body;
        if (!indexEndpointName) {
          throw new Error("search requires indexEndpointName");
        }
        const token = await getAccessToken();
        const url = `${WAREHOUSE_API_ROOT}/${indexEndpointName}:searchIndexEndpoint`;

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

function json200(obj) {
  return { statusCode: 200, body: JSON.stringify(obj) };
}
function json400(obj) {
  return { statusCode: 400, body: JSON.stringify(obj) };
}