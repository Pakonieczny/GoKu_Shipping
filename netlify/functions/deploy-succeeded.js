import core from './_britesConciergeLibraryDeploy.js';

// Reserved Netlify deploy event filename: the platform validates the JWS
// before invocation. Deliberately no ordinary fetch route or config.path.
export default core.createRequestHandler({
  getEnv:key=>Netlify.env.get(key),
  log:receipt=>console.info(JSON.stringify(receipt)),
});
