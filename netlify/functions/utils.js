// utils.js

// Parses comma-separated listing numbers and filters valid 10-digit numbers.
function parseListingNumbers(rawInput) {
  const parts = rawInput.split(/\s*,\s*/);
  const cleaned = parts.filter(p => /^\d{10}$/.test(p));
  return Array.from(new Set(cleaned));
}

// Generates a random alphanumeric string.
function generateRandomString(length) {
  const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let text = '';
  for (let i = 0; i < length; i++) {
    text += possible.charAt(Math.floor(Math.random() * possible.length));
  }
  return text;
}

// Resizes an image given its dataURL.
function resizeImage(dataURL, width, height) {
  return new Promise((resolve, reject) => {
    const img = new Image();
    img.onload = () => {
      const canvas = document.createElement("canvas");
      canvas.width = width;
      canvas.height = height;
      const ctx = canvas.getContext("2d");
      ctx.drawImage(img, 0, 0, width, height);
      resolve(canvas.toDataURL("image/jpeg", 0.85));
    };
    img.onerror = reject;
    img.src = dataURL;
  });
}

// Export functions if using a module system (e.g., ES modules)
// export { parseListingNumbers, generateRandomString, resizeImage };