import admin from './firebaseAdmin.js';
import assets from './_charmNestAsset.js';

export default async function(req) {
  return assets.serve(req, admin.storage().bucket());
}

export const config = { method: ['GET', 'HEAD'] };
