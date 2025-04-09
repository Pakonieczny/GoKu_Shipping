// netlify/functions/googleVisionProductSearch.js

const { ProductSearchClient } = require('@google-cloud/vision');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');

// 1) ENV variables in Netlify:
const projectId      = process.env.GCP_PROJECT_ID;      // e.g. "my-project-123"
const bucketName     = process.env.GCS_BUCKET_NAME;     // e.g. "golden-spike_image_matching_bucket"
const hmacAccessKey  = process.env.GCS_ACCESS_KEY;      // e.g. "JLdhLO4K..."
const hmacSecretKey  = process.env.GCS_SECRET_KEY;      // e.g. "xxxxxx..."
const defaultLocation= process.env.GCP_LOCATION || "us-east1"; // region for Product Search

// 2) Create the S3Client for uploading base64 images to GCS using HMAC:
const s3Client = new S3Client({
  region: 'auto', // GCS doesn't use normal AWS regions
  endpoint: 'https://storage.googleapis.com', // GCS S3-compatible endpoint
  credentials: {
    accessKeyId: hmacAccessKey,
    secretAccessKey: hmacSecretKey
  },
  forcePathStyle: false
});

// 3) Create the ProductSearchClient. 
//    We'll assume your environment is set so that the official library can read
//    ADC or a service account JSON. 
//    If you need to manually pass service account credentials, you'd do:
//      new ProductSearchClient({ keyFilename: "..." }) 
//    or a credentials object.
const productSearchClient = new ProductSearchClient(); 

// Helper to upload a base64 image to GCS using the S3-compatible approach.
// Returns the public URL, e.g. "https://storage.googleapis.com/<bucket>/<objectKey>"
async function uploadBase64ToGCS(base64Data, objectKey) {
  // 1) Parse dataUrl
  const matches = base64Data.match(/^data:(?<mime>[^;]+);base64,(?<base64>.+)$/);
  if (!matches || !matches.groups) {
    throw new Error("Invalid base64 data URL");
  }
  const mimeType = matches.groups.mime;
  const rawBase64 = matches.groups.base64;
  const fileBuffer = Buffer.from(rawBase64, 'base64');

  // 2) PutObjectCommand
  await s3Client.send(new PutObjectCommand({
    Bucket: bucketName,
    Key: objectKey,
    Body: fileBuffer,
    ContentType: mimeType,
    ACL: 'public-read' // Make it publicly readable (or use a signed URL approach)
  }));

  // 3) Return the public https URL
  return `https://storage.googleapis.com/${bucketName}/${objectKey}`;
}

exports.handler = async (event, context) => {
  try {
    // The request body might look like:
    // {
    //   action: "init"|"search"|"cleanup",
    //   productSetId: "temp_1234",
    //   location: "us-east1",
    //
    //   base64Images: [ { name, base64 }, ... ] (for init)
    //   singleEtsyImage: { name, base64 } (for search)
    // }
    const body = JSON.parse(event.body || '{}');
    const action = body.action;
    const location = body.location || defaultLocation;
    const productSetId = body.productSetId || "temp_" + Date.now();

    const productSetPath = productSearchClient.productSetPath(projectId, location, productSetId);

    if (action === 'init') {
      // --------------------------------------------------
      // 1) Create a ProductSet
      // --------------------------------------------------
      await productSearchClient.createProductSet({
        parent: productSearchClient.locationPath(projectId, location),
        productSetId,
        productSet: {
          displayName: `Temp Set ${productSetId}`
        }
      });

      // --------------------------------------------------
      // 2) For each user image, create Product + upload to GCS + ReferenceImage
      // --------------------------------------------------
      const userImgs = body.base64Images || [];
      for (let i = 0; i < userImgs.length; i++) {
        const { name, base64 } = userImgs[i];
        // (A) Create an objectKey to store in GCS
        const objectKey = `tempUserImages/${productSetId}/user_${i}_${Date.now()}.jpg`;
        
        // (B) Upload base64 to GCS
        const publicUrl = await uploadBase64ToGCS(base64, objectKey);

        // (C) Create a Product
        const productId = 'userimage_' + i + '_' + Date.now();
        const [product] = await productSearchClient.createProduct({
          parent: productSearchClient.locationPath(projectId, location),
          productId,
          product: {
            displayName: name, // We'll store the user image's name
            productCategory: 'homegoods', // or "toys"/"apparel" as appropriate
          },
        });

        // (D) Add that product to the ProductSet
        await productSearchClient.addProductToProductSet({
          name: productSetPath,
          product: product.name
        });

        // (E) Create ReferenceImage from the public GCS URL
        const referenceImageId = 'refimg_' + i + '_' + Date.now();
        await productSearchClient.createReferenceImage({
          parent: product.name,
          referenceImage: {
            uri: publicUrl // No placeholders, real link
          },
          referenceImageId
        });
      }

      return {
        statusCode: 200,
        body: JSON.stringify({ success: true, productSetId })
      };
    }

    else if (action === 'search') {
      // We'll do a single Etsy image
      // 1) Upload the Etsy image to GCS
      const { name, base64 } = body.singleEtsyImage || {};
      if (!base64) {
        return { statusCode: 400, body: JSON.stringify({ error: "No singleEtsyImage provided." }) };
      }
      const objectKey = `tempEtsyImages/${productSetId}/etsy_${Date.now()}.jpg`;
      const publicUrl = await uploadBase64ToGCS(base64, objectKey);

      // 2) Query the ProductSet
      const request = {
        parent: productSearchClient.locationPath(projectId, location),
        productSet: productSetPath,
        productCategories: ['homegoods'],
        filter: '',
        image: {
          source: { imageUri: publicUrl }
        }
      };

      const [response] = await productSearchClient.querySimilarProducts(request);
      const results = (response.productSearchResults && response.productSearchResults.results) || [];
      if (results.length === 0) {
        return { statusCode: 200, body: JSON.stringify({ matchScore: 0, productName: null }) };
      } else {
        const best = results[0];
        return {
          statusCode: 200,
          body: JSON.stringify({
            matchScore: best.score,
            productName: best.product.displayName,
            productResource: best.product.name
          })
        };
      }
    }

    else if (action === 'cleanup') {
      // Delete the entire ProductSet
      await productSearchClient.deleteProductSet({ name: productSetPath });

      // (Optional) If you want to remove the GCS objects, you'd need
      // to track the objectKeys used. For brevity, we skip that.
      
      return { statusCode: 200, body: JSON.stringify({ success: true }) };
    }

    else {
      return { statusCode: 400, body: JSON.stringify({ error: "Invalid action" }) };
    }

  } catch (err) {
    console.error("googleVisionProductSearch error:", err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: err.message })
    };
  }
};