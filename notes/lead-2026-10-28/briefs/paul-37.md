# Paul's request, 28 Oct 2026 — approved in full ("I approve all of the 37 new tasks")

## His words, verbatim
> Also when I'm adding a circle or a square or any other primitive and then resizing it, I'm not seeing a real time read out of the size in
> the work area of the actually selected object and in the X and Y in the menu. I also don't see a ruler like we have already in the charm
> studio on the website, which I gave you earlier as an example. A lot of the high-end features are missing and I would like you to put all
> the useful ones in, especially all the feedback having the ability to resize every object from its center-point so that it equally shrinks
> and the largest from its central point by holding the control key while dragging the size of the object with the mouse.
> I just feel like I need more control and abilities that Photoshop and illustrator both have in this application. I also need the ability
> for users to import images into this environment, and to manipulate the image and place it on a charm. All images must be converted to a
> vector format while keeping the quality of the image and the colour should be a greyscale using the black as base. I'm trying to
> eliminate the use of the Adobe platform since it's costing me $500 a month and I wanna make this as capable of a software as required to
> manipulate any need for a charm that includes designing custom charms, which requires blinds and deforming lines basically a (Designer)
> of a charm would use to manipulate a vector illustrator file and design a charm design. You need to follow the 70/30 Ruel where we're
> looking only for the most commonly used 70% of the features without overburdening the need and requirements.
> Users also need the ability to search and open any charm existing in our charm repository and have full control over manipulating that
> charm, and then send that charm to the next given sheet color, chosen by the user and market as handmade custom.
> Also custom orders that need a custom charm design now must all be able to be opened in the charm design designer, and then the designer
> would either bring in an existing vector design either an illustrator file or the XF file into the charm editor and then work on that
> file there and then save it and then that would get attached to the Custom order and that design would be sent to the next available
> sheet and the colour of the sheet would have to be decided by the user at that same time so you're gonna have to have some sort of a
> sophisticated pop-up modal that presents the user with all of these options.
> You need to ensure the sanctity of the designed charm. It's exact dimensions and user designed features, and that has to be linked to
> that specific Custom order.
> What this also means is that in the charm editor users will be able to edit orders that may not have a requirement for back engraving.
> Since this is all part of the back engraving tab, I would suggest we rename the tab "Design & Engrave". And the Charm Editor will be its
> own entity within that new updated tab that will be accessible from the back engraving modal and vice versa from the charm editor back
> into the back engraving modal.
> Just like we currently do in the charm editor on our website this charm editor must have a repository of all opened, modified designed
> pieces, and this repository must be searchable, and you must integrated on the right hand side in the same way we do it in the website,
> charm editor. Look at how it's done on a website because we've spent a lot of time making it really efficient and fast and see if you
> can improve it if not, just copied the implementation as long as you find it efficient. Users must be able to pull up their previous work
> and this repository must grow with every new update or change to any charm using the charm editor.
> The system must also track which order each design was attached to if there was a given order or if the user simply added accused piece
> that was not attached to a given order maybe to fix a mistake from before or something might've been forgotten and doesn't always get
> attached to an order so sometimes the user just needs to add a charm onto a sheet and that custom order can be named by the user so the
> user can easily identify that extra piece that was added, and not linked to an official order.
> I also need the ability to zoom into a given feature in the vector file. I need the ability to pan the work area once zoomed in.
> Users also need to be able to add any emoji and any language that we already support as part of the back engraving.
> Also, the Charm Editor button in the back engraving Modal is way too small and hidden, and should be a lot more visible and prominent and
> easy to click.

> also, all of this must be tracked by employee, and I need to know when employees are commissioning custom pieces, especially when they
> don't belong to an order that way we can correctly track our rates and who creates the errors and who is creating the solutions for all
> these problems

(Context: the "back engraving tab" is the app's **Engraving** tab, `data-mode="engrave"`; the "back engraving modal" is its card/work
area with the existing "Edit charm" pill that opens the Charm Editor.)

## The 37 items
Live feedback and control: 1 live size readout in the work area beside the selected object while drawing/resizing shapes · 2 X/Y/W/H
fields update live while drawing/dragging/resizing · 3 a ruler like the website studio's (the editor has plain mm rulers; bring over what
the studio has: selection marks, cursor tracking, guides) · 4 Ctrl-drag a handle resizes evenly about the centre · 5 live feedback for every
action, Illustrator/Photoshop style · 6 zoom into any detail · 7 pan once zoomed.
Design tools: 8 the useful high-end features (70/30) · 9 blends and deforming/bending lines (anchor points, curve handles, pen) ·
10 capable enough to replace Adobe for charm design, incl. designing custom charms from scratch.
Images: 11 import images, manipulate them, place them on a charm · 12 convert every image to vectors keeping quality · 13 greyscale with
black as base.
Text: 14 any emoji · 15 every language/script back engraving supports.
Libraries: 16 search the charm repository (master library) and open any charm in the editor with full control · 17 send it to the next sheet
of a user-chosen metal, marked "handmade custom" · 18 a library of every charm opened/changed/designed, growing with every save · 19
searchable · 20 on the right-hand side like the website editor (study it; improve or copy) · 21 record which order each design is attached to.
Custom orders and sheets: 22 every custom order needing a custom charm opens in the designer · 23 import an existing vector file (.ai or DXF)
and edit it · 24 the saved design attaches to that custom order · 25 sent to the next available sheet, metal chosen by the user at the same
time · 26 one well-designed pop-up with all these options · 27 sanctity: exact dimensions and the user's design features stay as made, tied
to that custom order · 28 pieces that belong to no order (fixing a mistake, a forgotten piece) can be put on a sheet · 29 such a piece is
named by the user.
Tab and access: 30 the editor opens any piece of any order, engraving or not · 31 rename the tab to "Design & Engrave" · 32 the Charm Editor
is its own part of that tab, reachable from the engraving card and back · 33 a much bigger, prominent Charm Editor button in the engraving card.
Employee tracking: 34 who did each step, when · 35 flag and report every custom piece an employee commissions, especially orderless ones ·
36 counted in the rates on the efficiency page · 37 who caused an error and who fixed it.

## The lead's decisions (Paul delegated them; change only with the lead)
- D1 Ctrl-drag = resize about the centre (Alt does too, as in Illustrator); Shift = keep proportions; the two combine. Shift while turning snaps 15°.
- D2 Image trace: greyscale with black as the base: the image is made grey, posterised into N levels (default 4, 2–8 adjustable; 1 = pure
  black/white threshold), each level traced to smooth filled vector paths (Bézier fitting, corner detection, despeckle), black the darkest
  level, white no ink. Each level keeps its grey (fill [g,g,g]) in the editor AND in the laser files, so the laser operator can give each grey
  its own power; the one-level trace is plain black engraving.
- D3 "blinds" is read as BLENDS (Illustrator's Blend tool); "deforming lines" = direct editing of anchors and curve handles, the pen, and
  warp/envelope presets (arc, bulge, wave, flag, rise, free distort).
- D4 A piece with no order: the user names it; it gets an id `CUSTOM-<yyyymmdd>-<n>`, is shown everywhere with its name and a "Custom · no
  order" tag, rides a sheet and the laser like any piece, and has no shipping label of its own.
- D5 Sheet colour = metal: GF (gold), SS (silver), RG (rose), 10K, 14K. The piece goes to the next open sheet of that metal in the current run
  (a new sheet when none is open), as a placed order's piece would.
- D6 Sanctity: a sent design's geometry is frozen with a hash and its exact size; the nest never scales it, Manual Control cannot resize it
  (it can move/turn it), the files written for its sheet are checked against the hash, and the piece names its design id + version + order.
- D7 Error/fix attribution: a person adding a piece to fix a mistake picks the order or piece it fixes; the system reads who made the
  original mistake from that order's history (who approved/engraved/placed it), shows it, and lets them change it; both people are recorded.
- D8 Every save in the editor makes a new VERSION of a design in the design library; the library lists designs (latest version) with a
  thumbnail, name, order (if any), who/when; search by name, SKU, order number, employee; instant local search over a cached index.
- D9 Emoji are engraved as monochrome OUTLINES (a laser engraves outlines/fills, not colour bitmaps).
- D10 The editor imports .ai, .pdf, .svg, .dxf and images .png .jpg .webp (drag and drop, a button, or paste).
- D11 The tab reads "Design & Engrave" (narrow: "Design"); the URL hash stays `#engrave` so links and tests keep working.
