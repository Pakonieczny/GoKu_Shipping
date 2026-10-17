# Set of Sheets: the owner's rules, in one list

This is the canonical list of every rule the owner (Paul) has stated about a Set of Sheets, collected from the repository's own quotes and rule comments.
The acceptance test of all of it is `charm-nest-set-invariants.js` (`CharmNestSetInvariants.check(sets, sheets, opts)`); `tests/charm-nest/set-invariants.cjs` holds it to the owner's Set 1 and Set 2.
Each violation the checker reports carries the **R-number** below. `R0` is bookkeeping (a sheet a set lists does not exist), not an owner rule.

How to read it: one rule, one reading. Where two sources disagree they are listed in **Conflicts between sources** at the end, with the reading that follows the owner's most recent words.
`file:line` is where the rule or quote is written in this repository (lines as of this commit).

## What a set is

**R1 · A set exists only with at least 1 Completed GF sheet and 1 Completed SS sheet.** Checker: `invalid` (a).
> "In order for a Set of Sheets to be allowed to exist there must be at minimum 1 Completed GF Sheet and 1 Completed SS Sheet." (Paul, 10 Oct 2026) `charm-nest-set-rules.js:3-4`
Other sheets (partial GF/SS, RG, 10K, 14K) may be in a valid set; they never count towards the minimum and never stand in for a completed one (`set-rules.js:22-23`). A set short of a class is not a set (`set-rules.js:49`: an existing half-made one is completed first).

**R2 · "Completed" means fully nested: full or released, or laser cut. A sheet that is still filling is PARTIAL.** Checker: `SetRules.isCompleted` / `isPartial`.
`releaseFull` true, or `laserDoneAt` / `roseCutAt` set (`set-rules.js:8-21`, code `:122-128`). Not completed: still filling, a draft, archived, nothing placed. Being in a set does not complete a sheet (`:18`).
The nest status words do not count: "complete" only means every piece given to the sheet was placed, "partial" means pieces moved on to the next sheet, so the sheet IS full (`:16-17`).

**R3 · A partial GF or SS sheet never joins a set, by any route.** Checker: `partial` (c).
> "A partial GF and SS sheet should never be part of a Set of sheets. That sheet should have been held back because it was not fully nested." (Paul, 13 Oct) `set-rules.js:58`, `tests/charm-nest/sets-partial.cjs:2-3`
Not by the minimum rule, the cardinal rule, automatic assembly or a person's drop; it stays a loose draft ("GF Draft n"), never deleted (`set-rules.js:60-62`). A person can still take one out (`:66-67`).

**R4 · RG, 10K and 14K sheets never start or complete a set, are never "partial", and are never "spare".** Checker: no rule (they are excluded from R1/R3/R7).
They keep their own conditions (R12) (`set-rules.js:50`, `:70`; `set-edit.js:27`). A person adds them (Include, Cut Sheet); the run never pulls one by itself (`tests/charm-nest/sets-batch.cjs:97`).

## How a set forms

**R5 · A set forms the moment the sheets that can be in it hold a completed GF and a completed SS sheet. Back engraving is not a criterion.** Checker: `unformed`.
> "as soon as the minimum criteria is met, and none of the other stipulations are conflicting, the set should be formed and as a result all following sheets should go into a following new set etc." (Paul, 12 Oct) `set-rules.js:26-27`
> "... Back engraving completed is not one of the criteria for a set to exist." `tests/charm-nest/sets-follow.cjs:2-3`; `set-rules.js:33` (back engraving only gates approving a set for laser cutting).
So completed sheets that sit outside every set while they already make a valid set are a fault (`unformed`), unless their group waits for a partial sheet (R11).

**R6 · A formed set is closed to automatic joining; the sheets that come after it form the NEXT set.** Checker: `closedJoin` (f).
> "... all following sheets should go into a following new set etc." (Paul, 12 Oct) `set-rules.js:26-27`, closing rule `:192-202`; `charm-nest-bridge.js:4695` (lastClosed)
Closed means closed to AUTOMATIC joining only. Always allowed after formation: a sheet that shares a multi-piece order with a sheet of the set (R9, "the cardinal rule outranks the closing", `set-rules.js:31-32`, `:51-52`), a person's drag (`bridge.js:4690-4691`), and a 10K/14K/RG sheet by its own press (R12). Set 2, Set 3 are numbered after the formed set, nothing is renumbered (`tests/charm-nest/sets-allocate-follow.cjs:1-2`).

**R7 · One set at a time, each as small as the minimum allows: a set may not hold a unit it does not need.** Checker: `surplus` (b).
> "a set of sheets requires at least one GF and one SS sheet at the minimum and when this threshold is met then that becomes an individual Set of sheets ... how is it that this one set has 4 GF and 2 SS sheets. This should be literally impossible" (Paul, 15 Oct) `set-rules.js:39-40`, `tests/charm-nest/sets-batch.cjs:2-3`
A UNIT is the sheets tied by a shared multi-piece order (R9); it is indivisible. It must be impossible to remove a whole unit and keep the set valid, unless that unit is tied to the others by an order (then it IS the others' unit) (`set-rules.js:44-52`, `surplus` `:264-274`).
Several completed sheets free at once do NOT go into one set (a stream's end, a reload, GF sheets that waited for the first SS): the sets form one after the other (`:41-43`).

**R8 · Order of formation.** Checker: `SetRules.formAll` (the oracle behind `expected()`).
Units are taken earliest first: the time a sheet was seen completed (`releasedAt`), then its page, GF before SS, then its id (`set-rules.js:42-43`, `orderKey` `:218`). A new set takes the earliest unit that holds a completed GF or SS sheet, then only as many further units (each supplying the class still lacking) as it needs; a unit that turns out not to be needed waits for its own set (`:45-47`, `fewest` `:234-245`). Units left over that cannot make a valid set wait outside every set with their plain line (`:48`). A set that already exists is never touched by this (`:50`).

## Sheets that share an order

**R9 · THE CARDINAL RULE: all sheets that share a multi-piece order are always in the same set.** Checker: `cardinal` (e).
> "If any order in a given sheet has pieces that are also part of other sheets, that automatically means those sheets must become part of the same set of sheets. All Sheets that share multi-piece orders must always be in the same Set of Sheets. If a user tries to drag a sheet out of a given set of sheets and by doing so would violate the cardinal rules ... that sheet should not be allowed to be moved out of the set and the user should be clearly informed as to why, and which orders are preventing this. A user should be able to manually remove the offending orders that are mixed, and have those be immediately updated in real time everywhere in the application." (Paul, 5 Oct, round 2 point 6) `charm-nest-shared-orders.js:3-8`
An order is shared when more than one distinct piece of it sits on two or more sheets (`:14-17`). Pairs and mismatched pairs count (Paul, 9 Oct, R3: "... those sheets that have mixed sets ... must be considered when generating sets and when a user tries to move a sheet out of a set", `tests/charm-nest/pairs-sets.cjs:1-3`). The group is connected through ANY sheet, including one outside the set (a Rose Gold partner): that is why such a chain is one unit.
A completed sheet outside the set that holds its partner belongs in it (`cardinal`); two partners in two sets is the plain violation.

**R10 · A partner that cannot join yet (RG, 10K, 14K before its conditions) does not hold the group out. The set forms without it, the sheet is absent, and is named as the partner to add.** Checker: `partners[]` (not a violation).
> Paul, 12 Oct: a sheet that shares an order with a sheet that cannot join yet "no longer holds the group out: the set forms with the sheets that can be in it, says plainly which partner sheet to add" `set-rules.js:34-36`; `bridge.js:4579-4582`
That set cannot be approved for laser cutting until the partner is in it (`set-rules.js` `gate()` `:290-306`, readiness). The line is "orders shared with RG Draft 1: add that sheet to this set" (`partnerWords` `:205`).

**R11 · A group that shares an order with a partial GF/SS sheet waits OUTSIDE any set, whole, until that sheet is full.** Checker: `partialPartner`.
> Paul, 13 Oct, same quote as R3, read as ONE GROUP: `set-rules.js:62-65`; `bridge.js:4602-4604`, `:4615-4621` (`cardinalHold`); `tests/charm-nest/sets-partial.cjs:7-10`
Nothing is split, nothing is dissolved. If a sheet of the group was in a set before the partial sheet appeared, the group's completed sheets keep joining that set and the partial one waits outside (its Approve stays grey): the one legitimate exception, say it with `opts.partialPartners: 'pending'` or `opts.legacySets`.

**R12 · A 10K, 14K or Rose Gold sheet joins a set with exactly two conditions: (a) its green line is cut (or the person says yes to the Cut Sheet press at the drop), and (b) its back engraving is approved.** Checker: `notReady` (d).
> Paul, 12 Oct (SETGOLD): joins "provided (a) its green line is cut, or the person presses Cut Sheet right at the drop, and (b) its back engraving is approved. Nothing else stops it" `tests/charm-nest/library-flow.cjs:283-284`; `charm-nest-flow.js:51-58`; `charm-nest-set-edit.js:29`, `:68-81`
Kept as physical rules: a recorded cut keeps a sheet in the set it is in; a cut sheet in NO set may join one; a completed (laser cut) sheet takes no move; a set is never emptied (`flow.js:55-58`, `set-edit.js:23-24`). A sheet that shares an order comes with its group (`flow.js:56-57`). A full Rose Gold sheet takes the rest of the metal whole and gets no line (`flow.js:357`, `:360`): the checker reads that as `roseFull`.
In sheet data: cut = `roseCutAt` or `laserDoneAt`; the yes = `cutApproved`; backs = `backsApproved` or the readiness report `laser`. An uncut/unapproved one IN a set is `notReady`; one OUTSIDE is a partner (R10).

## Moving, naming, approving

**R13 · A sheet moves freely unless the move breaks a core principle.** Not checkable from a state (it judges moves): `SetEdit.verifyMoves` (`charm-nest-set-edit.js:137-199`).
> "Unless the move violates one of the core principals of the Set then I should be able to move it completely freely" (Paul, 10 Oct) `set-edit.js:16-17`
> "The User should be able to add/remove individual sheets from a set of sheets even in the Laser cutting process as long as the below are true: A. That there are NO shared pieces from a multi-piece order on that particular sheet shared with other sheets in the same Set. B. The sheet has NOT been marked by the user as completed (Laser cut)." (Paul, 7 Oct) `set-edit.js:3-6`
The refusals are `setPrinciple` (R1), `sharedOrders` (R9), `sheetCut`/`sheetCompleted`, `lastSheet`, `sheetPartial` (R3), `backsNotApproved` (R12) (`set-edit.js:20-30`).

**R14 · Names and numbers.** Checker: `numbering`.
> "There are lot's of issues here with how the sheets are names ... there are 2 SS sheets both names #1" (Paul, 10 Oct) `charm-nest-sheet-name.js:3`
In a set a sheet is "GF Sheet n", n its number in that set for its metal, from 1, never shared by two sheets of one metal; outside every set it is "GF Draft p" (p its page in the run). A cut sheet keeps its number. A number is given once, never changed, and a number that left the set is retired (`sheet-name.js:13-22`).

**R15 · A set advances as ONE.** Not checkable from a state (Library gate): `CharmNestReadiness.setGate`.
> "you cannot have a green approved button on a single sheet that is part of a set where the other sheets are not ready yet and don't have the green Approve button engaged." (Paul, 5 Oct, round 7) `charm-nest-readiness.js:666`, `tests/charm-nest/set-approve.cjs:1-2`
> "There should only be one 'approved for laser cutting' button per Set of Sheets. Only individual sheets that are not already part of a set are allowed to have their own ..." (Paul, 5 Oct, round 12) `tests/charm-nest/one-approve-per-set.cjs:1-3`
The checker reports `sets[].canApprove` (no violation and no partner to add) as the state-side half of this.

**R16 · Multi-piece orders (pairs, discs) stay on ONE sheet of their colour, so same-colour sheets do not chain.** Checker: `warnings[] orderSplit` (a nesting fault; never fails `ok`).
> "... orders with multiple pieces or earrings with left and right charms should stay on a given sheet, so that sheets of the same colour do not create an endless chain of connections" (Paul, 15 Oct) `tests/charm-nest/setsim-days.cjs:3-4`
> "keep orders of the same colour on one sheet ... the priority should be to maintain all of the pieces for each given order on that one sheet" (Paul, 13 Oct) `tests/charm-nest/setsim-lib.cjs:3-5`

## Conflicts between sources

Read these first: each is a place the app can go wrong by following the wrong half.

- **C1 · "Completed" means two things.** `set-rules.js:8-21` / R2: completed = full or released (or cut). The Library / SetEdit use "completed" for a LASER-CUT sheet (`set-edit.js:23` `sheetCompleted`, `shared-orders.js:50` "it was marked completed"). Reading used: R2, because the owner pairs "Completed" with "partial ... not fully nested" (13 Oct). Any code that counts only `laserDoneAt`, or the nest status "complete", is wrong for R1.
- **C2 · "Partial" means two things.** The nest status "partial" = pieces moved on, the sheet IS full (`set-rules.js:16-17`); the owner's "partial" = not full (R3). Reading used: the owner's.
- **C3 · Back engraving: required, or not a criterion?** Both, on different sheets (both 12 Oct): not a criterion for a GF/SS set to exist (R5; `set-rules.js:33`) but a condition for a 10K/14K/RG sheet to join (R12; `flow.js:51-58`, `set-edit.js:68`). Using the first for gold-family sheets, or the second for GF/SS, is the error.
- **C4 · Does a partner that cannot join hold the group out?** 12 Oct: no, the set forms without it (`set-rules.js:34-36`, `bridge.js:4579-4582`). 13 Oct: a partial GF/SS partner keeps the whole group out (`set-rules.js:62-65`). Reading used: the 13 Oct text for a partial GF/SS sheet (a sheet still being NESTED is partial), the 12 Oct text for RG/10K/14K and for a full sheet that is only still being saved (`bridge.js:4560-4568` `cannotJoin`, checked after the partial test at `:4605`).
- **C5 · Closed set vs cardinal rule vs "as small as the minimum".** A closed set takes no sheet on its own (R6) yet takes any sheet that shares an order with it (R9, `set-rules.js:31-32`); R7 says a set with 4 GF and 2 SS is "literally impossible". Reading used: minimality is counted in UNITS. Sheets tied by orders are one unit, so a big set is legal only if every unit in it is needed. The owner's own premise for R7 is R16: chains are a nesting fault, so `check()` flags the chain as a warning and still judges the set by units.
- **C6 · "A set that already exists is never touched" vs "should never" / "literally impossible".** `set-rules.js:50` and `:66` leave an existing set alone and only FLAG a partial sheet or a spare unit (`bridge.js:10489-10503` prints a sentence); committed/completed/replaced sets are exempt (`set-rules.js:281-282`). That is a safety choice about silent repair, not a statement that the state is acceptable. Reading used: the owner's: the state is a violation; repairing it is a separate, explicit act. `check()` judges every set; `opts.skipExempt` follows the app.
- **C7 · How a gold-family sheet gets in: three generations.** 28 Sep: a per-sheet "Include in current set" tick (`bridge.js:4461-4464`, `charm-nest-orders.js:3837-3843`), Rose Gold only in "even-numbered" sets (`orders.js:3839-3840`). 12 Oct: exactly two conditions, per sheet, no other restriction (R12). Stale text still says "Rose Gold joins a set by metal ... every Rose Gold sheet of this run joins the same set" (`flow.js:362`, `:713`). Reading used: 12 Oct, per sheet.
- **C8 · Order of formation says "sheet number", code orders by page.** `set-rules.js:42-43` vs `orderKey` `:218` (`releasedAt`, then `page || sheetIndex`). A number in a set exists only after the sheet joins one, so the run page is the only number that can order formation. It decides which unit is Set N and how GF-only and SS-only units pair; it never decides whether a valid unit forms its own set.
- **C9 · Material-level sets (old) vs sheet-level units.** `Orders.kinGroups` says "Each group is a set" at the level of a MATERIAL (`orders.js:3845`, `bridge.js:5811`, `:10145-10150`); the cardinal rule (R9) is per SHEET. A modern run puts every metal in one "dispatch" group (`bridge.js:5807`). Reading used: sheets and units; `kinGroups` is the legacy path only. The pre-12 Oct reading "a set takes every full sheet until approved" survives as `CLOSING.afterFormation` (`set-rules.js:200`); keep it `true`.
- **C10 · 28 Sep "let the user decide" vs 5 Oct cardinal.** `bridge.js:4470-4472` (a pop-up, the user decides what to do with an order on two sheets) vs `shared-orders.js:3-8` (not allowed to be separated; remove the offending order instead). Reading used: 5 Oct; the dialog now only offers "include all together" or Cancel.
- **C11 · A loose sheet can read "Sheet n".** `sheet-name.js:13-17` (outside a set it is a Draft) vs `numbered()` `:52-58`, which reads a non-draft record that names no set but keeps a number as "Sheet n" (a stale `sheetIndex`). The owner's "GF Sheet 8" can be a sheet in no set. `check()` names sheets from the sets argument, never from a stale flag.

## Worked examples (from `tests/charm-nest/set-invariants.cjs`)

**Set 1 (owner): GF Sheet 1 + SS Sheet 1 sharing order 4191315127.** OK: one unit, both needed, nothing spare, nothing missing.

**Set 2 (owner), as the app built it: 14 sheets in one set.** GF Sheet 1-8 and SS Sheet 1-4 full; RG Sheet 16 and RG Sheet 29 not cut (RG 29 at 29 %); chains A = SS4-GF7-RG29-GF6-SS3, B = GF5-RG16-GF4-SS2-GF3, C = SS1-GF2-GF1, GF 8 loose; plus partial GF Draft 10 sharing an order with the set. `check()` refuses it:
- R7 `surplus` x3: unit B (GF 3, GF 4, GF 5, SS 2, RG 16), unit A (SS 3, SS 4, GF 6, GF 7, RG 29) and "GF Sheet 8 shares no order with the others and Set 2 is valid without it". Unit C (GF 1, GF 2, SS 1) is enough.
- R12 `notReady` x2: RG Sheet 16 and RG Sheet 29, uncut.
- R6 `closedJoin` x3: B, A and GF 8 joined after the set had formed (it formed when SS 1 joined GF 1; GF 2 came with its group).
- R11 `partialPartner`: GF Draft 10, with the unit it shares an order with.
- It does NOT say R1, R3, R9: the set holds a completed GF and SS, no partial sheet is in it, no order crosses sets. Its fault is size, not shortage.

**The correct formation of the same sheets (`CharmNestSetInvariants.expected`, the real `formAll` over the real units).** Every unit except GF 8 holds a completed GF and a completed SS sheet by itself, so each is its OWN set, earliest first (GF 1, then SS 2, then SS 3):
- Set 2 = GF 1, GF 2, SS 1 (unit C).
- Set 3 = GF 3, GF 4, GF 5, SS 2 (unit B); RG Sheet 16 is the partner to add.
- Set 4 = GF 6, GF 7, SS 3, SS 4 (unit A); RG Sheet 29 is the partner to add.
- GF 8 waits outside every set: "it needs a completed SS sheet with it".
- If GF Draft 10 shares an order with unit X, unit X (with its Rose Gold sheet and the draft) waits whole until Draft 10 is full, and the other units form as above (R11). Which unit it ties to is not in the owner's words; the test runs all three.
- Here every unit is valid by itself, so the partition does not depend on the completion times; only which unit is Set 2, 3, 4 does (R8).

## Calling the checker

```js
// page (after charm-nest-sheet-name.js, charm-nest-set-rules.js, charm-nest-shared-orders.js; charm-nest-set-edit.js and charm-nest-rose.js are used when loaded)
const res = CharmNestSetInvariants.check(sets.map(s => ({ id: s.setId, name: s.name, seq: s.seq, sheetIds: s.sheetIds })), allSheets());
if (!res.ok) console.error(CharmNestSetInvariants.explain(res).join('\n'));
// node / Netlify function
const I = require('./charm-nest-set-invariants.js');
const right = I.expected(sheets);                     // what the rules make of these sheets: { sets, waiting, held, partners }
```
`closedJoin` (R6) needs to know when a sheet joined: stamp `joinedAt` (ms) on a sheet when it takes its `setId`, or put `formedAt` on the set; without either the rule is silent. Everything else reads the sheets and the list of sets only.
