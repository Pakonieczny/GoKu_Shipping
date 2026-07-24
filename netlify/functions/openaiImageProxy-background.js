/* netlify/functions/openaiImageProxy-background.js
 *
 * Compatibility entry point for Listing Generator image jobs.
 *
 * The shared worker owns storage paths, manifests, Firestore status, retries,
 * and the strict model allowlist. When `model: "gpt-image-2"` is supplied it
 * routes the image request to OpenAI with OPENAI_API_KEY; Gemini selections
 * continue to use GEMINI_API_KEY. Keeping one worker prevents the provider
 * implementations from drifting or writing incompatible result records.
 */

const imageWorker = require("./geminiImageProxy-background");

exports.handler = imageWorker.handler;
