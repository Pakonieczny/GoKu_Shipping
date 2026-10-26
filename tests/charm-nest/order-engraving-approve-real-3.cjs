// The third part of order-engraving-approve-real.cjs (one run with every phase takes more than the 300 s a test may): the words confirmed in Engraving,
// the reload and the Sandbox, on the same real run. ONLY=<phase,...> still picks phases.
process.env.ONLY = process.env.ONLY || 'confirmed,reload,sandbox';
require('./order-engraving-approve-real.cjs');
