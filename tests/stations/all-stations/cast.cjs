// The invented crew of the all-stations end-to-end test. Names are made up (none is on a real roster and none has a built-in alias in the portal);
// the six digits each one types are drawn per run, are typed only into a page's own box, and are never printed, logged or kept in a result.
'use strict';

const PEOPLE = {
  admin: 'Paul K',        // the Admin (the shop's default list): at the Sorter app he is Sorting, and he reads the portal
  matcher: 'Tess W.',     // Welding, task Matching
  welder: 'Walt R.',      // Welding, task Welding
  asm: 'Michael V.',      // Assembly (the kept desk)
  asmSmoke: 'Hana B.',    // Assembly (the other desks, one after another)
  ship: 'Ivy R.',         // Shipping (the kept desk)
  shipSmoke: 'Ravi D.',   // Shipping (the other desks)
  sort: 'Maya S.',        // Sorting (the kept desk)
  sortSmoke: 'Omar J.',   // Sorting (the second desk)
  designMsg: 'Rosa D.',   // Design Station, design-message (a PIN page)
  designMsg2: 'Gus H.',   // design-message-1 (the other PIN page)
  designSmoke: 'Pia L.',  // the other Design Station pages (the name is set in the order chat)
  designApp: 'Dara F.',   // the Sorter app as Design
  laserApp: 'Lena G.',    // the Sorter app as Laser
  inbox: 'Ines I.',       // the Inbox operator (user name "ines")
  multi: 'Nico M.'        // one person at three stations in a day (Assembly, then Design in the Sorter app, then Sorting)
};
const PIN_PEOPLE = ['matcher', 'welder', 'asm', 'asmSmoke', 'ship', 'shipSmoke', 'sort', 'sortSmoke', 'designMsg', 'designMsg2', 'multi'];

function makeCast() {
  const pins = {}, used = new Set();
  for (const k of PIN_PEOPLE) {
    let p;
    do { p = String(100000 + Math.floor(Math.random() * 900000)); } while (used.has(p) || /^(\d)\1{5}$/.test(p) || /^(012345|123456|654321|000000)$/.test(p));
    used.add(p); pins[PEOPLE[k]] = p;
  }
  return { pins, people: PEOPLE };
}
/** what the shop is told at the start: the roster door's map { number: name } (kept in the shop's memory only) */
const rosterOf = cast => Object.fromEntries(Object.entries(cast.pins).map(([n, p]) => [p, n]));

module.exports = { PEOPLE, makeCast, rosterOf };
