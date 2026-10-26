// The second part of order-engraving-approve-real.cjs (one run with every phase takes more than the 300 s a test may): the stale card, the failed writes and
// the card widths, on the same real run (order-engraving-approve-real-3.cjs: the words confirmed in Engraving, the reload, the Sandbox). ONLY=<phase,...> still picks phases.
process.env.ONLY = process.env.ONLY || 'stale,fail,widths';
require('./order-engraving-approve-real.cjs');
