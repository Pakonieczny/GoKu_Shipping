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
  "charm-nest-1.html",
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
  "brites-campaign-styles.js",
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
  "charm-nest-operations.js",
  "charm-nest-solver.js",
  "charm-nest-rose.js",
  "charm-nest-rose-ui.js",
  "charm-nest-learned.js",
  "charm-nest-worker.js",
  "charm-nest-gpu.js",
  "charm-nest-lookahead.js",
  "charm-nest-background.js",
  "charm-nest-clock.js",
  "charm-nest-compute-worker.js",
  "charm-nest-pdf.js",
  "charm-nest-vector.js",
  "vendor/clipper-6.4.2.js",
  "vendor/clipper-6.4.2-LICENSE.txt",
  "charm-nest-geom.js",
  "charm-nest-engrave-fit.js",
  "charm-nest-engrave-worker.js",
  "charm-nest-assets.js",
  "charm-nest-backs.js",
  "charm-nest-readiness.js",
  "charm-nest-export.js",
  "charm-nest-export-ui.js",
  "charm-nest-text.js",
  "vendor/fonts/emoji-sequences.json",
  "vendor/fonts/NotoEmoji-OFL.txt",
  "vendor/fonts/Unicode-LICENSE.txt",
  "charm-nest-orders.js",
  "charm-nest-bridge.js",
  "charm-nest-progress.js",
  "charm-nest-check.html",
  "vendor/opentype-1.3.4.min.js",
  "vendor/jszip-3.10.1.min.js",
  "vendor/pdf-lib-1.17.1.min.js",
  "vendor/pdfjs-4.10.38/pdf.min.mjs",
  "vendor/pdfjs-4.10.38/pdf.worker.min.mjs",
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
// The engraving fonts (Source Sans 3, SIL Open Font License) live in vendor/fonts/ and are committed; the copy stays
// optional so a build never fails if the folder is ever emptied on a deploy machine.
const optionalDirs = ["vendor/fonts"];
let optional = 0;
for (const dir of optionalDirs) {
  const src = path.join(root, dir);
  if (!fs.existsSync(src)) continue;
  for (const name of fs.readdirSync(src)) {
    if (!/\.(otf|ttf)$/i.test(name)) continue;
    const target = path.join(output, dir, name);
    fs.mkdirSync(path.dirname(target), { recursive: true });
    fs.copyFileSync(path.join(src, name), target);
    optional++;
  }
}
console.log(`Prepared ${assets.length} public assets (+${optional} optional font files); server sources and configuration excluded.`);
