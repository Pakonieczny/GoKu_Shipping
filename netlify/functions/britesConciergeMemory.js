import memory from './_britesConciergeMemory.js';

let handler;
function environment(){return Object.fromEntries(['FIREBASE_PROJECT_ID','FIREBASE_CLIENT_EMAIL','FIREBASE_PRIVATE_KEY','SHOPIFY_STORE','SHOPIFY_CLIENT_ID','SHOPIFY_CLIENT_SECRET','BRITES_GROWTH_NAMESPACE'].map(key=>[key,Netlify.env.get(key)]));}
export default async req=>{handler=handler||memory.createRequestHandler({env:environment()});return handler(req);};
export const config={path:'/api/concierge-memory'};
