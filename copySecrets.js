// copySecrets.js
const fs = require('fs');
const path = require('path');

// Define the source of your secret file.
// In this example, it is expected to be in the gcp-secrets/secrets folder in your project root.
const src = path.join(process.cwd(), 'gcp-secrets', 'secrets', 'GCP_PRIVATE_KEY.txt');

// Define the destination directory in your functions bundle.
const destDir = path.join(process.cwd(), 'netlify', 'functions', 'secrets');

// Make sure the destination directory exists (create it if not)
if (!fs.existsSync(destDir)) {
  fs.mkdirSync(destDir, { recursive: true });
}

// Check if the secret file exists at the source location.
if (!fs.existsSync(src)) {
  console.error(`Source file ${src} not found!`);
  process.exit(1);
}

// Copy the file to the destination.
const dest = path.join(destDir, 'GCP_PRIVATE_KEY.txt');
fs.copyFileSync(src, dest);
console.log(`Copied GCP_PRIVATE_KEY.txt from ${src} to ${dest}`);

// Optionally, list the destination folder contents:
const files = fs.readdirSync(destDir);
console.log('Destination folder contents:', files);