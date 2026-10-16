// PIECES gate: the ONE place where the gate's tests learn the names the model chose (/tmp/pc/CONTRACT-PIECES.md). Everything else in
// pieces-model.cjs / pieces-flow.cjs is behaviour (how many pieces, which words, which approval), asserted without a slot spelling.
// Until the contract exists these are the brief's suggestions; change them HERE only.
'use strict';
module.exports = {
  // the slot of copy i (1-based) of a line of one charm cut n times (the brief suggests "C1".."Cn", "Piece 1 of 2")
  copySlot: i => 'C' + i,
  // the words a person reads for piece i of n on a card title / strip / tag (case-insensitive)
  pieceWord: (i, n) => new RegExp('piece\\s*' + i + '\\s*of\\s*' + n, 'i'),
  // the function of CharmNestEngraveSides that gives the default words of each piece: (texts|spec, n) -> { ok, words:[n], how } (the contract's name; the first that exists is used)
  wordsFns: ['wordsForPieces', 'pieceWords', 'wordsForCopies'],
  // the commit the gate compares quantity-1 behaviour with (origin/main the PIECES work is built on: HDRSEAL/OWHEAD included)
  BASE_COMMIT: '80e55188'
};
