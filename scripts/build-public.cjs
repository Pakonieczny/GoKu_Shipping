// Explicit public asset manifest. Server code, credentials, tests, configuration,
// and repository metadata never enter the Netlify publish directory.
const fs = require('node:fs');
const path = require('node:path');
const root = path.resolve(__dirname, '..');
const output = path.join(root, 'public-site');
const assets = [
  "Game_Generator_1.html",
  "Listing_Generator_1.html",
  "Modals.html",
  "QR Printer.html",
  "SKU_List_V1.html",
  "assembly-1.html",
  "assembly-2.html",
  "assembly-3.html",
  "assembly-4.html",
  "assembly-scan-1.html",
  "assembly-scan-2.html",
  "assembly-scan-3.html",
  "assembly-scan-4.html",
  "brites-adwords.html",
  "brites-vfx.html",
  "design-1.html",
  "design-message-1.html",
  "design-message.html",
  "design-print-1.html",
  "design-print.html",
  "design.html",
  "etsy-mail-1.html",
  "etsy-pricing.html",
  "imageGrid.html",
  "index.html",
  "investor.html",
  "scanner.html",
  "screenTwo.html",
  "shipping-1.html",
  "shipping-2.html",
  "shipping-3.html",
  "shipping-scan-1.html",
  "shipping-scan-2.html",
  "shipping-scan-3.html",
  "sort-scan.html",
  "sorting-2.html",
  "sorting.html",
  "weld-1.html",
  "weld-scan-1.html",
  "main.js",
  "AssetsTreeEntitiesCompiler.js",
  "brites-ad-editor.js",
  "brites-ad-format-policy.js",
  "brites-ad-motion.js",
  "brites-brand-assets.js",
  "assets/brites-brand/logo-square.png",
  "assets/brites-brand/logo-wide.png",
  "assets/brites-brand/icon-blue.png",
  "brites-progress.js",
  "brites-progress.css",
  "brites-ad-responsive.js",
  "brites-groups.js",
  "brites-groups.css",
  "12x12_Grid_Gld+Slvr+EXTRA.jpg",
  "Test Matrix_DELETE_3.jpg",
  "assets/ChitChats.png",
  "assets/Favicon.PNG",
  "assets/Globe.png",
  "assets/Michelle_R.jpg",
  "assets/Paul_K.jpg",
  "assets/USPS.png",
  "assets/design-1.html",
  "assets/design.html",
  "assets/pmax-recommendation.js",
  "css/style.css",
  "js/utils.js",
  "lib/jsQR.js",
  "lib/qrcode.min.js",
  "vendor/fabric-7.4.0.min.js",
  "shopify/assets/brites-custom-studio.js"
];
fs.rmSync(output, { recursive: true, force: true });
for (const asset of assets) {
  const source = path.join(root, asset);
  if (!fs.lstatSync(source).isFile()) throw new Error('Expected public file: ' + asset);
  const data = fs.readFileSync(source);
  if (/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/.test(data.toString()))
    throw new Error('Private key detected in public asset: ' + asset);
  const target = path.join(output, asset);
  fs.mkdirSync(path.dirname(target), { recursive: true });
  fs.writeFileSync(target, data);
}
console.log(`Prepared ${assets.length} public assets; server sources and configuration excluded.`);
