'use strict';
// Firestore refuses an array directly inside an array. A fake that writes through structuredClone accepts it silently, which is how a Cut Sheet that worked
// in every offline test failed live ("3 INVALID_ARGUMENT: Nested arrays are not allowed"). A fake calls this on every document it is asked to store.
module.exports = function refuseNestedArrays(value, where = 'document') {
  (function walk(x, path, inArray) {
    if (Array.isArray(x)) {
      if (inArray) throw new Error(`3 INVALID_ARGUMENT: Nested arrays are not allowed (${path})`);
      x.forEach((v, i) => walk(v, `${path}[${i}]`, true));
    } else if (x && typeof x === 'object' && Object.getPrototypeOf(x) === Object.prototype) {
      for (const [k, v] of Object.entries(x)) walk(v, `${path}.${k}`, false);   // (an array inside a map inside an array is allowed)
    }
  })(value, where, false);
  return value;
};
