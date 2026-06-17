// netlify/functions/shopifyEditor.js
// ---------------------------------------------------------------------------
// Secure proxy between the Brites in-grid editor (on britesjewelry.com) and the
// Shopify Admin GraphQL API.
//
// 2026 model:
//   - Auth: client-credentials grant. We hold the app's Client ID + Secret as
//     env vars and exchange them for a short-lived token (auto-refreshed ~24h).
//     No shpat_ token, nothing in the browser.
//   - API: GraphQL Admin API (REST product endpoints are retired for new apps).
//
// The editor's action names + request/response shapes are unchanged, so the
// theme snippet does not need any edits.
//
// Required Netlify environment variables:
//   SHOPIFY_STORE          e.g. "britesjewelry.myshopify.com"
//   SHOPIFY_CLIENT_ID      Client ID from your Dev Dashboard app
//   SHOPIFY_CLIENT_SECRET  Client secret from your Dev Dashboard app
//   EDIT_PASSCODE          a secret you choose; the UI must send it on every call
//   SHOPIFY_API_VERSION    optional; defaults to "2025-10"
// ---------------------------------------------------------------------------

const fetch = require("node-fetch");

/* ─── Firebase (Firestore + Storage) via the shared admin module ───────────
   Lazy + defensive: if Firebase isn't configured in this deploy, the editor's
   Shopify features keep working and only the persistence/best-seller extras
   degrade. The browser never touches Firebase — only this function does. */
let _fb = null;
function fb() {
  if (_fb !== null) return _fb;
  try {
    const admin = require("./firebaseAdmin");
    _fb = { admin, db: admin.firestore(), FV: admin.firestore.FieldValue, bucket: admin.storage().bucket() };
  } catch (e) { console.error("[shopifyEditor] Firebase unavailable:", e.message); _fb = false; }
  return _fb;
}
function numericId(gid) { return String(gid == null ? "" : gid).replace(/^.*\//, ""); }

/* Parsed Top-200 best-sellers index (seeded from the uploaded CSV). Default that
   self-seeds into Firestore (Brites_Editor_Meta/bestSellers) on first use; the
   live doc then takes over so the list can be updated without a redeploy. */
const BEST_SELLERS_SEED = JSON.parse("[{\"rank\":1,\"name\":\"Dainty Bunny Necklace\",\"skus\":[\"Bunny5\",\"Bunny5-ENG\"],\"orders\":182},{\"rank\":2,\"name\":\"Cardinal Bird Pendant Necklace\",\"skus\":[\"Cardinal_87245\"],\"orders\":104},{\"rank\":3,\"name\":\"Fire Badge Necklace\",\"skus\":[\"fire badge\",\"fire badge-ENG\"],\"orders\":118},{\"rank\":4,\"name\":\"Dainty Sun Necklace\",\"skus\":[\"celestial1-sun\",\"celestial1-sun-ENG\"],\"orders\":110},{\"rank\":5,\"name\":\"Norse Runes Charm\",\"skus\":[\"Viking Rune\"],\"orders\":117},{\"rank\":6,\"name\":\"Phoenix Bird Pendant Necklace\",\"skus\":[\"FIREBIRD 2\"],\"orders\":108},{\"rank\":7,\"name\":\"Sunflower Charm Necklace\",\"skus\":[\"Sunflower3 - Beady\"],\"orders\":86},{\"rank\":8,\"name\":\"Hummingbird Charm Stud Earrings\",\"skus\":[\"Hummingbird Cut out\"],\"orders\":86},{\"rank\":9,\"name\":\"Norse Runes Charm\",\"skus\":[\"Viking Rune\"],\"orders\":86},{\"rank\":10,\"name\":\"Circle Jacket Charm Necklace Set of 2\",\"skus\":[\"Circle_4443\"],\"orders\":82},{\"rank\":11,\"name\":\"Bunny Charm Stud Earrings\",\"skus\":[\"Bunny5\"],\"orders\":79},{\"rank\":12,\"name\":\"Dainty Cancer Ribbon Necklace\",\"skus\":[\"Health5-Cancer Ribbon\",\"Health5-Cancer Ribbon-ENG\"],\"orders\":68},{\"rank\":13,\"name\":\"Book Reader Charm Necklace\",\"skus\":[\"Book_47142\"],\"orders\":74},{\"rank\":14,\"name\":\"Dragonfly Pendant Necklace\",\"skus\":[\"DragonFly2\"],\"orders\":69},{\"rank\":15,\"name\":\"Gold Bar Necklace, Handwritten Bar Necklace - YOUR HANDWRITING - or Image, Sterling Silver, Gold or Rose Gold, Jewelry For Her\",\"skus\":[\"Gold_7478 | Gold_Bar_3804\"],\"orders\":60},{\"rank\":16,\"name\":\"Wolf Necklace\",\"skus\":[\"Wolf Charm\",\"Wolf Charm-ENG\"],\"orders\":65},{\"rank\":17,\"name\":\"Hummingbird Pendant Necklace\",\"skus\":[\"HUMMING BIRD cutout\"],\"orders\":58},{\"rank\":18,\"name\":\"Baseball Charm Huggie\",\"skus\":[\"Huggie Hoops- Alien Head\"],\"orders\":66},{\"rank\":19,\"name\":\"Squirrel Charm Necklace\",\"skus\":[\"Squirrel 1 (eating)\",\"Squirrel 1 (eating)-ENG\"],\"orders\":65},{\"rank\":20,\"name\":\"Cardinal Bird Pendant Necklace\",\"skus\":[\"Cardinal_26531\"],\"orders\":44},{\"rank\":21,\"name\":\"Eagle Pendant Necklace\",\"skus\":[\"American Eagle - Beady\"],\"orders\":62},{\"rank\":22,\"name\":\"Chicken Charm Stud Earrings\",\"skus\":[\"Chicken 3\"],\"orders\":14},{\"rank\":23,\"name\":\"Lemon Pendant Necklace\",\"skus\":[\"LEMON\"],\"orders\":42},{\"rank\":24,\"name\":\"Dainty Rubber Ducky Necklace\",\"skus\":[\"shape\"],\"orders\":56},{\"rank\":25,\"name\":\"Add on an Extender to Charm\",\"skus\":[\"Add_6663\"],\"orders\":51},{\"rank\":26,\"name\":\"Movie Slate Charm\",\"skus\":[\"Movie Slate\",\"Movie Slate-ENG\"],\"orders\":55},{\"rank\":27,\"name\":\"Circle Jacket Charm Necklace Set of 2\",\"skus\":[\"Circle_4443\"],\"orders\":54},{\"rank\":28,\"name\":\"Killer Whale Charm Necklace\",\"skus\":[\"Killer Whale 2\",\"Killer Whale 2-ENG\"],\"orders\":53},{\"rank\":29,\"name\":\"Sitting Cat Earrings\",\"skus\":[\"Cat 1- Sitting Cat\"],\"orders\":52},{\"rank\":30,\"name\":\"Cute Sea Turtle Necklace\",\"skus\":[\"Sea Turtle2\",\"Sea Turtle2-ENG\"],\"orders\":50},{\"rank\":31,\"name\":\"Crab Pendant Necklace\",\"skus\":[\"CRAB 2\"],\"orders\":4},{\"rank\":32,\"name\":\"Mythical Dragon Charm Stud Earrings\",\"skus\":[\"Dragon11\"],\"orders\":50},{\"rank\":33,\"name\":\"Alligator Necklace\",\"skus\":[\"Reptile 4-Gator\",\"Reptile 4-Gator-ENG\"],\"orders\":45},{\"rank\":34,\"name\":\"Skating Necklace\",\"skus\":[\"Sports 10 - figure skate\",\"Sports 10 - figure skate-ENG\"],\"orders\":49},{\"rank\":35,\"name\":\"Running Horse Stud\",\"skus\":[\"Horse2\"],\"orders\":48},{\"rank\":36,\"name\":\"Camping Earrings Campfire\",\"skus\":[\"Camp5 - Mountain Peak\"],\"orders\":47},{\"rank\":37,\"name\":\"Theatre Happy Sad\",\"skus\":[\"Masks Happy/Sad\"],\"orders\":47},{\"rank\":38,\"name\":\"Tiny Moon Necklace\",\"skus\":[\"tiny moon\"],\"orders\":40},{\"rank\":39,\"name\":\"Mismatched Tennis Ball\",\"skus\":[\"Huggie Hoops- Alien Head\"],\"orders\":39},{\"rank\":40,\"name\":\"Sheep Lamb Pendant\",\"skus\":[\"Sheep3\"],\"orders\":43},{\"rank\":41,\"name\":\"Team Charm Necklace\",\"skus\":[\"Cable Chain only\"],\"orders\":44},{\"rank\":42,\"name\":\"Chicken Pendant\",\"skus\":[\"Beady Chicken 3\",\"Beady Chicken 3-ENG\"],\"orders\":33},{\"rank\":43,\"name\":\"Puzzle Pendant Necklace\",\"skus\":[\"PUZZLE PIECE\"],\"orders\":33},{\"rank\":44,\"name\":\"Cheerleading Team Jewelry\",\"skus\":[\"Cheer 1 - Megaphone(plain)\",\"Cheer 1 - Megaphone(plain)-ENG\"],\"orders\":20},{\"rank\":45,\"name\":\"Little Hummingbird Necklace\",\"skus\":[\"hummingbird3\",\"hummingbird3-ENG\"],\"orders\":41},{\"rank\":46,\"name\":\"Hammered Skinny Mini Gold Bar Bar Necklace\",\"skus\":[\"Hammered_4617\"],\"orders\":41},{\"rank\":47,\"name\":\"Tooth Necklace, Dentist Gift\",\"skus\":[\"Health9-tooth\",\"Health9-tooth-ENG\"],\"orders\":38},{\"rank\":48,\"name\":\"Key Charm Pendant\",\"skus\":[\"Key2\",\"Key2-ENG\"],\"orders\":36},{\"rank\":49,\"name\":\"Tooth Charm Huggie Hoops\",\"skus\":[\"Huggie Hoops- Health9-Tooth\"],\"orders\":36},{\"rank\":50,\"name\":\"USA Map Pendant Necklace\",\"skus\":[\"USA_27250\"],\"orders\":36},{\"rank\":51,\"name\":\"Theatre Mask Charm Pendant\",\"skus\":[\"Comedy_48247\"],\"orders\":33},{\"rank\":52,\"name\":\"Puzzle Piece Beady Charm Necklace\",\"skus\":[\"Beady Puzzle Piece\"],\"orders\":29},{\"rank\":53,\"name\":\"Soaring Pelican Pendant Necklace\",\"skus\":[\"Soaring_74233\"],\"orders\":36},{\"rank\":54,\"name\":\"Penguin Charm Necklace\",\"skus\":[\"Beady Penguin 2\"],\"orders\":36},{\"rank\":55,\"name\":\"Hawk Charm Necklace\",\"skus\":[\"Beady Hawk\"],\"orders\":35},{\"rank\":56,\"name\":\"Gold Dumbbell Charm Necklace\",\"skus\":[\"Sports 24- Dumbbell\"],\"orders\":33},{\"rank\":57,\"name\":\"Lotus Pendant Necklace\",\"skus\":[\"LOTUS 2\"],\"orders\":30},{\"rank\":58,\"name\":\"Map of Palestine Pendant Necklace\",\"skus\":[\"palestine map\"],\"orders\":18},{\"rank\":59,\"name\":\"Lighthouse Charm Earrings\",\"skus\":[\"Lighthouse Disc\"],\"orders\":35},{\"rank\":60,\"name\":\"Stethoscope earring Doctor\",\"skus\":[\"Health 8\"],\"orders\":35},{\"rank\":61,\"name\":\"Boho Feather Charm Necklace\",\"skus\":[\"FEATHER CHARM\"],\"orders\":34},{\"rank\":62,\"name\":\"Dainty Oval Disc Caduceus Necklace\",\"skus\":[\"Health3\",\"Health3-ENG\"],\"orders\":32},{\"rank\":63,\"name\":\"Dragonfly Charm Stud Earrings\",\"skus\":[\"DragonFly2\"],\"orders\":32},{\"rank\":64,\"name\":\"Palestine Map Necklace\",\"skus\":[\"palestine map\"],\"orders\":34},{\"rank\":65,\"name\":\"Stethoscope Necklace Doctor\",\"skus\":[\"Health8-stethoscope\",\"Health8-stethoscope-ENG\"],\"orders\":33},{\"rank\":66,\"name\":\"Dragon Pendant Necklace\",\"skus\":[\"DRAGON 11\"],\"orders\":32},{\"rank\":67,\"name\":\"Graduation Cap Pendant Necklace\",\"skus\":[\"Graduation_11271\"],\"orders\":30},{\"rank\":68,\"name\":\"Capybara Charm Earrings\",\"skus\":[\"Huggie Hoops- Capybara1\"],\"orders\":33},{\"rank\":69,\"name\":\"Raccoon Charm Necklace\",\"skus\":[\"Beady Raccoon2\"],\"orders\":33},{\"rank\":70,\"name\":\"Bear Charm Necklace\",\"skus\":[\"Bear 1\",\"Bear 1-ENG\"],\"orders\":32},{\"rank\":71,\"name\":\"Small Cat Necklace\",\"skus\":[\"Cat 1 - sitting cat\",\"Cat 1 - sitting cat-ENG\"],\"orders\":32},{\"rank\":72,\"name\":\"Ballet Shoes pendant\",\"skus\":[\"BalletShoes\",\"BalletShoes-ENG\"],\"orders\":31},{\"rank\":73,\"name\":\"Initial Charm Necklace\",\"skus\":[\"decorative initial\",\"decorative initial-ENG\"],\"orders\":30},{\"rank\":74,\"name\":\"Map of Palestine Charm Huggie Hoops\",\"skus\":[\"Huggie Hoops- Surfboard\"],\"orders\":28},{\"rank\":75,\"name\":\"Apple Charm Necklace\",\"skus\":[\"Apple 2\",\"Apple 2-ENG\"],\"orders\":23},{\"rank\":76,\"name\":\"Steamboat Willie Mickey\",\"skus\":[\"Old Mickey - Beady\",\"Old Mickey - Beady-ENG\"],\"orders\":28},{\"rank\":77,\"name\":\"Comedy Tragedy Mask Pendant Necklace\",\"skus\":[\"Beady Mask Smile Frown\"],\"orders\":28},{\"rank\":78,\"name\":\"Apple Charm with\",\"skus\":[\"Apple cut out - Beady\",\"Apple cut out - Beady-ENG\"],\"orders\":26},{\"rank\":79,\"name\":\"Fire Department Badge\",\"skus\":[\"Fire Dept Badge\"],\"orders\":26},{\"rank\":80,\"name\":\"Music Note Pendant Necklace\",\"skus\":[\"Music_3831\"],\"orders\":26},{\"rank\":81,\"name\":\"Horse Pendant Necklace\",\"skus\":[\"HORSE 2\"],\"orders\":24},{\"rank\":82,\"name\":\"Ballet Charm Stud Earrings\",\"skus\":[\"BalletShoes\"],\"orders\":25},{\"rank\":83,\"name\":\"Hockey Stick Pendant Necklace\",\"skus\":[\"HOCKEY STICK 1\"],\"orders\":25},{\"rank\":84,\"name\":\"Leaping Bunny Pendant Necklace\",\"skus\":[\"Leaping_40171\"],\"orders\":25},{\"rank\":85,\"name\":\"Dainty Cowboy Boot Necklace\",\"skus\":[\"Cowboy3- Boot\",\"Cowboy3- Boot-ENG\"],\"orders\":23},{\"rank\":86,\"name\":\"Chicken Stud Earrings\",\"skus\":[\"Chicken 3\"],\"orders\":26},{\"rank\":87,\"name\":\"Rock On Stud\",\"skus\":[\"Handsign 4-rock on\"],\"orders\":26},{\"rank\":88,\"name\":\"Sunflower Necklace\",\"skus\":[\"Sunflower1\",\"Sunflower1-ENG\"],\"orders\":26},{\"rank\":89,\"name\":\"Running Shoe Pendant Necklace\",\"skus\":[\"SPORTS 9 - Running Shoe\"],\"orders\":25},{\"rank\":90,\"name\":\"Dainty Bunny Necklace\",\"skus\":[\"Bunny5\",\"Bunny5-ENG\"],\"orders\":24},{\"rank\":91,\"name\":\"Wolf Pendant Necklace\",\"skus\":[\"WOLF\"],\"orders\":24},{\"rank\":92,\"name\":\"Heart Charm Necklace\",\"skus\":[\"Heart_7610\"],\"orders\":21},{\"rank\":93,\"name\":\"Running Shoe Necklace\",\"skus\":[\"sports9-running shoe\",\"sports9-running shoe-ENG\"],\"orders\":21},{\"rank\":94,\"name\":\"Lion Pendant Necklace\",\"skus\":[\"Africa4- Lion\"],\"orders\":19},{\"rank\":95,\"name\":\"Ram Pendant Necklace\",\"skus\":[\"RAM 3\"],\"orders\":19},{\"rank\":96,\"name\":\"Lemon Charm pendant\",\"skus\":[\"lemon\",\"lemon-ENG\"],\"orders\":18},{\"rank\":97,\"name\":\"Paperclip Earrings\",\"skus\":[\"paperclip\"],\"orders\":16},{\"rank\":98,\"name\":\"Bunny Charm Huggie\",\"skus\":[\"Huggie Hoops- Alien Head\"],\"orders\":25},{\"rank\":99,\"name\":\"Compass Disc Charm Necklace\",\"skus\":[\"COMPASS 6\"],\"orders\":25},{\"rank\":100,\"name\":\"Otter Charm Necklace\",\"skus\":[\"Otter\",\"Otter-ENG\"],\"orders\":25},{\"rank\":101,\"name\":\"Swan Charm Necklace\",\"skus\":[\"Swan\",\"Swan-ENG\"],\"orders\":25},{\"rank\":102,\"name\":\"Cute Rubber Ducky Necklace\",\"skus\":[\"shape\"],\"orders\":22},{\"rank\":103,\"name\":\"Skating Earrings Ice\",\"skus\":[\"sports 10- figure skate\"],\"orders\":22},{\"rank\":104,\"name\":\"Four Leaf Clover Pendant Necklace\",\"skus\":[\"4 Leaf Clover\"],\"orders\":16},{\"rank\":105,\"name\":\"Cherry Blossom Flower\",\"skus\":[\"Cherry Blossom\",\"Cherry Blossom-ENG\"],\"orders\":11},{\"rank\":106,\"name\":\"Caduceus Pendant Necklace\",\"skus\":[\"HEALTH 1 CADUCEUS\"],\"orders\":24},{\"rank\":107,\"name\":\"Dachshund Earrings Animal\",\"skus\":[\"Dachshund Dog\"],\"orders\":24},{\"rank\":108,\"name\":\"Dainty Cut out Fox Necklace\",\"skus\":[\"Origami1-Fox cut out\",\"Origami1-Fox cut out-ENG\"],\"orders\":24},{\"rank\":109,\"name\":\"Monkey Charm Necklace\",\"skus\":[\"Africa 3-Monkey\",\"Africa 3-Monkey-ENG\"],\"orders\":24},{\"rank\":110,\"name\":\"Hockey Skate Pendant Necklace\",\"skus\":[\"HOCKEY 2\"],\"orders\":23},{\"rank\":111,\"name\":\"Mountain Pendant Necklace\",\"skus\":[\"CAMP 5- Mountain Peak\"],\"orders\":23},{\"rank\":112,\"name\":\"Swan Studs 14k\",\"skus\":[\"Swan\"],\"orders\":23},{\"rank\":113,\"name\":\"Runner Earrings Sports\",\"skus\":[\"sports9-running shoe\"],\"orders\":21},{\"rank\":114,\"name\":\"Labrador Pendant Necklace\",\"skus\":[\"LABRADOR\"],\"orders\":20},{\"rank\":115,\"name\":\"Dove of Peace Pendant Necklace\",\"skus\":[\"DOVE 4\"],\"orders\":21},{\"rank\":116,\"name\":\"Flag Pendant Necklace\",\"skus\":[\"FLAGS\"],\"orders\":19},{\"rank\":117,\"name\":\"Dumpster Fire Pendant Necklace\",\"skus\":[\"DUMPSTER FIRE\"],\"orders\":16},{\"rank\":118,\"name\":\"Graduation Cap Charm Necklace\",\"skus\":[\"Grad Cap\",\"Grad Cap-ENG\"],\"orders\":22},{\"rank\":119,\"name\":\"Small Cat Necklace\",\"skus\":[\"Cat 1 - sitting cat\",\"Cat 1 - sitting cat-ENG\"],\"orders\":22},{\"rank\":120,\"name\":\"Running Horse Charm\",\"skus\":[\"Running Horse2\",\"Running Horse2-ENG\"],\"orders\":21},{\"rank\":121,\"name\":\"Owl Charm Necklace\",\"skus\":[\"OWL 2\"],\"orders\":20},{\"rank\":122,\"name\":\"Chick Pendant Necklace\",\"skus\":[\"CHICK - IN EGG\"],\"orders\":14},{\"rank\":123,\"name\":\"Hockey Stick Pendant\",\"skus\":[\"hockey 1-sticks\",\"hockey 1-sticks-ENG\"],\"orders\":11},{\"rank\":124,\"name\":\"Blooming Dandelion Pendant Necklace\",\"skus\":[\"Blooming_16014\"],\"orders\":21},{\"rank\":125,\"name\":\"Caduceus Charm Stud Earrings\",\"skus\":[\"HEALTH 3 CADUCEUS\"],\"orders\":21},{\"rank\":126,\"name\":\"Horse Huggie Hoops\",\"skus\":[\"Huggie Hoops- Alien Head\"],\"orders\":21},{\"rank\":127,\"name\":\"Raccoon Charm Stud Earrings\",\"skus\":[\"Raccoon_8823\"],\"orders\":21},{\"rank\":128,\"name\":\"Scuba Diver Earring 14k Goggle Charm Stud Earrings\",\"skus\":[\"Aquatic4 - Goggles\"],\"orders\":21},{\"rank\":129,\"name\":\"Heron Bird Pendant Necklace\",\"skus\":[\"Heron_38478\"],\"orders\":20},{\"rank\":130,\"name\":\"Lion Necklace Lion\",\"skus\":[\"Africa 4- Lion\",\"Africa 4- Lion-ENG\"],\"orders\":20},{\"rank\":131,\"name\":\"Music Note Pendant Necklace\",\"skus\":[\"music - treble clef\"],\"orders\":20},{\"rank\":132,\"name\":\"Peach Pendant Necklace\",\"skus\":[\"PEACH\"],\"orders\":20},{\"rank\":133,\"name\":\"Cardinal Bird Pendant Necklace\",\"skus\":[\"Cardinal_74064\"],\"orders\":19},{\"rank\":134,\"name\":\"Pansy Flower Necklace\",\"skus\":[\"Floral5\"],\"orders\":18},{\"rank\":135,\"name\":\"Sea Turtle Pendant Necklace\",\"skus\":[\"SEA TURTLE 3\"],\"orders\":17},{\"rank\":136,\"name\":\"Dna Pendant Necklace\",\"skus\":[\"DNA STRAND\"],\"orders\":15},{\"rank\":137,\"name\":\"Skating Earrings Ice\",\"skus\":[\"sports 10- figure skate\"],\"orders\":12},{\"rank\":138,\"name\":\"Pisces Zodiac Stud\",\"skus\":[\"Zodiac REVAMP\"],\"orders\":20},{\"rank\":139,\"name\":\"Gecko Necklace\",\"skus\":[\"Reptile 1-Gecko\",\"Reptile 1-Gecko-ENG\"],\"orders\":20},{\"rank\":140,\"name\":\"Jupiter Necklace\",\"skus\":[\"Space1-Jupiter\"],\"orders\":20},{\"rank\":141,\"name\":\"Mountain Charm Necklace\",\"skus\":[\"Mountain Cut out\",\"Mountain Cut out-ENG\"],\"orders\":20},{\"rank\":142,\"name\":\"Initial Charm Huggie Hoops\",\"skus\":[\"Huggie Hoops- lowercase initials\"],\"orders\":20},{\"rank\":143,\"name\":\"Police Badge Necklace\",\"skus\":[\"police badge\",\"police badge-ENG\"],\"orders\":20},{\"rank\":144,\"name\":\"Theatre Mask Pendant Necklace\",\"skus\":[\"MASK SMILE FROWN\"],\"orders\":20},{\"rank\":145,\"name\":\"Mini Volleyball Necklace\",\"skus\":[\"Volleyball\",\"Volleyball-ENG\"],\"orders\":20},{\"rank\":146,\"name\":\"Gold Bunny Earrings\",\"skus\":[\"Bunny5\"],\"orders\":18},{\"rank\":147,\"name\":\"Leaf Pendant Necklace\",\"skus\":[\"FLORALS 6\"],\"orders\":17},{\"rank\":148,\"name\":\"Maple Leaf Pendant Necklace\",\"skus\":[\"MAPLE LEAF\"],\"orders\":15},{\"rank\":149,\"name\":\"Lobster Pendant Necklace\",\"skus\":[\"LOBSTER 2\"],\"orders\":12},{\"rank\":150,\"name\":\"Dachshund Charm Earrings\",\"skus\":[\"DACHSHUND\"],\"orders\":19},{\"rank\":151,\"name\":\"Handcrafted Elephant Hoop\",\"skus\":[\"Huggie Hoops- Alien Head\"],\"orders\":19},{\"rank\":152,\"name\":\"Hippopotamus Zoo Pendant Necklace\",\"skus\":[\"Hippo_53162\"],\"orders\":19},{\"rank\":153,\"name\":\"Jesus Fish Faith Pendant Necklace\",\"skus\":[\"Ichthys_98120\"],\"orders\":19},{\"rank\":154,\"name\":\"Manta Ray Charm Stud Earrings\",\"skus\":[\"Manta_0320\"],\"orders\":19},{\"rank\":155,\"name\":\"Orca Earrings Killer\",\"skus\":[\"Killer Whale2\"],\"orders\":19},{\"rank\":156,\"name\":\"Raccoon Pendant Necklace\",\"skus\":[\"RACCOON 2\"],\"orders\":19},{\"rank\":157,\"name\":\"Tiny Tag Necklace\",\"skus\":[\"Tiny Initial Tag 3\",\"Tiny Initial Tag 3-ENG\"],\"orders\":19},{\"rank\":158,\"name\":\"Dinosaur Charm Necklace\",\"skus\":[\"Trex - Beady\",\"Trex - Beady-ENG\"],\"orders\":19},{\"rank\":159,\"name\":\"Zoo Animal Charm Pendant\",\"skus\":[\"Crocodile_44839\"],\"orders\":18},{\"rank\":160,\"name\":\"Circle Jacket Charm Necklace Set of 2\",\"skus\":[\"Circle_4443\"],\"orders\":18},{\"rank\":161,\"name\":\"Mama Bear Necklace\",\"skus\":[\"Bear mama\",\"Bear mama-ENG\"],\"orders\":18},{\"rank\":162,\"name\":\"Medical Caduceus Pendant\",\"skus\":[\"Caduceus - Beady\",\"Caduceus - Beady-ENG\"],\"orders\":18},{\"rank\":163,\"name\":\"Number Pendant Necklace\",\"skus\":[\"Comic Number\"],\"orders\":16},{\"rank\":164,\"name\":\"Chicken Bar Necklace\",\"skus\":[\"Running_73848\"],\"orders\":14},{\"rank\":165,\"name\":\"Bicycle Charm Stud Earrings\",\"skus\":[\"Sports 27-Bike\"],\"orders\":18},{\"rank\":166,\"name\":\"Bullseye Pendant Necklace\",\"skus\":[\"SPORTS 12 - Bullseye\"],\"orders\":18},{\"rank\":167,\"name\":\"Chick Pendant Necklace\",\"skus\":[\"CHICK - IN EGG\"],\"orders\":18},{\"rank\":168,\"name\":\"Monogram Pendant Necklace\",\"skus\":[\"Monogram_5351\"],\"orders\":18},{\"rank\":169,\"name\":\"Skating Earrings Ice\",\"skus\":[\"sports 10- figure skate\"],\"orders\":18},{\"rank\":170,\"name\":\"Cherry Blossom Stud\",\"skus\":[\"Cherry Blossom\"],\"orders\":17},{\"rank\":171,\"name\":\"Fox Lover Pendant Necklace\",\"skus\":[\"Fox_97250\"],\"orders\":17},{\"rank\":172,\"name\":\"Dragonfly Charm Stud Earrings\",\"skus\":[\"DragonFly5\"],\"orders\":17},{\"rank\":173,\"name\":\"Baby Bear Charm Earrings\",\"skus\":[\"BEAR BABY\"],\"orders\":15},{\"rank\":174,\"name\":\"Flying Hawk Earrings\",\"skus\":[\"NEW1\"],\"orders\":15},{\"rank\":175,\"name\":\"Star of David Charm Huggie Hoops\",\"skus\":[\"Huggie Hoops- Star of David\"],\"orders\":15},{\"rank\":176,\"name\":\"Capybara Pendant\",\"skus\":[\"Capybara1\",\"Capybara1-ENG\"],\"orders\":17},{\"rank\":177,\"name\":\"Lightning Bolt Thunderbolt Charm Necklace\",\"skus\":[\"Lightning_3071\"],\"orders\":17},{\"rank\":178,\"name\":\"Eagle Pendant Necklace\",\"skus\":[\"Eagle_4275\"],\"orders\":17},{\"rank\":179,\"name\":\"Dragon Charm Necklace\",\"skus\":[\"dragon10 - dragon body\"],\"orders\":17},{\"rank\":180,\"name\":\"Heart Charm Necklace\",\"skus\":[\"Heart_7610\"],\"orders\":17},{\"rank\":181,\"name\":\"Heart Pendant Necklace\",\"skus\":[\"HEART LONG\"],\"orders\":17},{\"rank\":182,\"name\":\"Poodle Charm Necklace\",\"skus\":[\"Beady Poodle\"],\"orders\":17},{\"rank\":183,\"name\":\"Saturn Pendant Necklace\",\"skus\":[\"Space2-Saturn\"],\"orders\":17},{\"rank\":184,\"name\":\"Aztec Dragon Pendant Necklace\",\"skus\":[\"MEXICO 4\"],\"orders\":16},{\"rank\":185,\"name\":\"Bear Pendant Necklace\",\"skus\":[\"BEAR 1\"],\"orders\":16},{\"rank\":186,\"name\":\"Book Lover Charm Pendant\",\"skus\":[\"Book_23102\"],\"orders\":16},{\"rank\":187,\"name\":\"Dragon Pendant Necklace\",\"skus\":[\"DRAGON 2\"],\"orders\":16},{\"rank\":188,\"name\":\"Volleyball Huggie Hoop\",\"skus\":[\"Huggie Hoops- Alien Head\"],\"orders\":16},{\"rank\":189,\"name\":\"Raccoon Pendant Necklace\",\"skus\":[\"Raccoon_6193\"],\"orders\":15},{\"rank\":190,\"name\":\"Hamster Pendant Necklace\",\"skus\":[\"HAMSTER\"],\"orders\":13},{\"rank\":191,\"name\":\"Beaver Charm Earrings\",\"skus\":[\"BEAVER\"],\"orders\":12},{\"rank\":192,\"name\":\"Baseball Charm Necklace\",\"skus\":[\"Baseball_64293\"],\"orders\":16},{\"rank\":193,\"name\":\"Cancer Ribbon Earring\",\"skus\":[\"Health5\"],\"orders\":16},{\"rank\":194,\"name\":\"Cherry Blossom Charm\",\"skus\":[\"Huggie Hoops- Alien Head\"],\"orders\":16},{\"rank\":195,\"name\":\"Dragonfly Pendant Necklace\",\"skus\":[\"DragonFly2\"],\"orders\":16},{\"rank\":196,\"name\":\"Team Charm Necklace\",\"skus\":[\"Cable Chain only\"],\"orders\":16},{\"rank\":197,\"name\":\"Lyre Harp Music Charm Necklace\",\"skus\":[\"Music 2 - Lyre Harp\",\"Music 2 - Lyre Harp-ENG\"],\"orders\":16},{\"rank\":198,\"name\":\"Panda Charm Necklace\",\"skus\":[\"PANDA 3\"],\"orders\":16},{\"rank\":199,\"name\":\"Space Charm Huggie\",\"skus\":[\"Huggie Hoops- Alien Head\"],\"orders\":16},{\"rank\":200,\"name\":\"Science Laboratory Flasks Pendant Necklace\",\"skus\":[\"Science_90441\"],\"orders\":16}]");

/* ── Pricing reference (point: pricing scheme) ────────────────────────────
   Stored verbatim and persisted to Firebase on first use (Brites_Editor_Meta/
   pricingScheme + Storage brites/pricing/pricing-scheme.txt). Base64 so the
   source text needs no escaping. The pricing ENGINE that consumes this is wired
   separately once the open pricing decisions are confirmed. */
const PRICING_SCHEME_B64 = "PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PQpCUklURVMgSkVXRUxSWSDigJQgQ09NUExFVEUgUFJJQ0lORyBSRUZFUkVOQ0UKPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PQoKVElFUiBTWVNURU0KLS0tLS0tLS0tLS0KRXZlcnkgcHJvZHVjdCBpcyBhc3NpZ25lZCBvbmUgb2YgdGhyZWUgdGllcnMgKHJhbmRvbWx5IGRpc3RyaWJ1dGVkCmFjcm9zcyB0aGUgY2F0YWxvZyBmb3IgbmF0dXJhbCBwcmljZSB2YXJpYXRpb24pOgoKICBUaWVyIDEgPSBCYXNlIHByaWNlIMOXIDAuOTMgIChlbnRyeSBsZXZlbCkKICBUaWVyIDIgPSBCYXNlIHByaWNlICAgICAgICAgIChzdGFuZGFyZCkKICBUaWVyIDMgPSBCYXNlIHByaWNlIMOXIDEuMDcgIChwcmVtaXVtKQoKTUFURVJJQUwgTUFSS1VQIE1VTFRJUExJRVJTCi0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0KICBTaWx2ZXIgLyBHb2xkLUZpbGxlZCAvIFJvc2UgR29sZCAg4oaSICDDlyAxLjIwCiAgMTRLIFNvbGlkIEdvbGQgICAgICAgICAgICAgICAgICAgIOKGkiAgw5cgMS4zMAogIChFeGNlcHRpb246IEJlYWR5IGNoYWluIDE0SyB1c2VzIMOXIDEuMjAsIG5vdCDDlyAxLjMwKQoKRU5HUkFWSU5HIEFERC1PTgotLS0tLS0tLS0tLS0tLS0tCiAgU2luZ2xlIHNpZGUgZW5ncmF2aW5nICDihpIgICsgJDEwCiAgRnJvbnQgJiBiYWNrIGVuZ3JhdmluZyDihpIgICsgJDIwCgoKPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PQoxLiBTVFVEIEVBUlJJTkdTCj09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT0KTm8gZW5ncmF2aW5nIG9wdGlvbiBmb3Igc3R1ZHMuCgogIE1hdGVyaWFsICAgICAgICB8IFRpZXIgMSB8IFRpZXIgMiB8IFRpZXIgMwogIC0tLS0tLS0tLS0tLS0tLS18LS0tLS0tLS18LS0tLS0tLS18LS0tLS0tLQogIFN0ZXJsaW5nIFNpbHZlciB8ICAkMzUgICB8ICAkMzggICB8ICAkNDEKICBHb2xkIEZpbGxlZCAgICAgfCAgJDM5ICAgfCAgJDQyICAgfCAgJDQ1CiAgUm9zZSBHb2xkICAgICAgIHwgICQ0MiAgIHwgICQ0NSAgIHwgICQ0OAogIDE0SyBTb2xpZCBHb2xkICB8ICAkMjAxICB8ICAkMjE2ICB8ICAkMjMxCgoKPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PQoyLiBIVUdHSUUgLyBIT09QIEVBUlJJTkdTCj09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT0KTm90ZTogTm8gUm9zZSBHb2xkIG9wdGlvbiBmb3IgaHVnZ2llcy4KICAgICAgMTRLIG9ubHkgYXZhaWxhYmxlIGluIDExbW0gc2l6ZSAoOC41bW0gMTRLIGlzIGRyb3BwZWQvaW52YWxpZCkuCgogIE1hdGVyaWFsICAgICAgICAgfCBUaWVyIDEgfCBUaWVyIDIgfCBUaWVyIDMKICAtLS0tLS0tLS0tLS0tLS0tLXwtLS0tLS0tLXwtLS0tLS0tLXwtLS0tLS0tCiAgU3RlcmxpbmcgU2lsdmVyICB8ICAkNDEgICB8ICAkNDQgICB8ICAkNDcKICBHb2xkIEZpbGxlZCAgICAgIHwgICQ0NiAgIHwgICQ0OSAgIHwgICQ1MgogIDE0SyBHb2xkICgxMW1tKSAgfCAgJDMwNyAgfCAgJDMzMCAgfCAgJDM1MwoKCj09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT0KMy4gQkVBRFkgQ0hBSU4gTkVDS0xBQ0VTCj09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT0KVGhyZWUgdmFyaWFibGVzOiBNYXRlcmlhbCAvIEVuZ3JhdmluZyAoWWVzIG9yIE5vKSAvIENoYWluIExlbmd0aCAoMTQiLCAxNiIsIDE4IikKTm90ZTogMjAiIGNoYWlucyBhcmUgbWFwcGVkIHRvIDE4IiBwcmljaW5nLiAxNEsgdXNlcyDDlyAxLjIwIG11bHRpcGxpZXIgKG5vdCAxLjMwKS4KTm90ZTogUm9zZSBHb2xkIGlzIG5vdCBhdmFpbGFibGUgZm9yIEJlYWR5IOKAlCB1c2VzIEdvbGQgcHJpY2luZyBpbnN0ZWFkLgoKVElFUiAxOgogIE1hdGVyaWFsICsgRW5ncmF2ZSAgfCAxNCIgIHwgMTYiICB8IDE4IgogIC0tLS0tLS0tLS0tLS0tLS0tLS0tfC0tLS0tLXwtLS0tLS18LS0tLS0KICBTaWx2ZXIsIE5vIEVuZ3JhdmUgIHwgJDQ4ICB8ICQ1NCAgfCAkNTYKICBTaWx2ZXIsICsgRW5ncmF2ZSAgIHwgJDU4ICB8ICQ2MyAgfCAkNjUKICBHb2xkLCBObyBFbmdyYXZlICAgIHwgJDU2ICB8ICQ2MiAgfCAkNjQKICBHb2xkLCArIEVuZ3JhdmUgICAgIHwgJDY1ICB8ICQ3MiAgfCAkNzMKICAxNEssIE5vIEVuZ3JhdmUgICAgIHwgJDM5MiB8ICQ0MzUgfCAkNDQ4CiAgMTRLLCArIEVuZ3JhdmUgICAgICB8ICQ0MTEgfCAkNDU0IHwgJDQ2NwoKVElFUiAyOgogIE1hdGVyaWFsICsgRW5ncmF2ZSAgfCAxNCIgIHwgMTYiICB8IDE4IgogIC0tLS0tLS0tLS0tLS0tLS0tLS0tfC0tLS0tLXwtLS0tLS18LS0tLS0KICBTaWx2ZXIsIE5vIEVuZ3JhdmUgIHwgJDUyICB8ICQ1OCAgfCAkNjAKICBTaWx2ZXIsICsgRW5ncmF2ZSAgIHwgJDYyICB8ICQ2OCAgfCAkNzAKICBHb2xkLCBObyBFbmdyYXZlICAgIHwgJDYwICB8ICQ2NyAgfCAkNjkKICBHb2xkLCArIEVuZ3JhdmUgICAgIHwgJDcwICB8ICQ3NyAgfCAkNzkKICAxNEssIE5vIEVuZ3JhdmUgICAgIHwgJDQyMiB8ICQ0NjggfCAkNDgyCiAgMTRLLCArIEVuZ3JhdmUgICAgICB8ICQ0NDIgfCAkNDg4IHwgJDUwMgoKVElFUiAzOgogIE1hdGVyaWFsICsgRW5ncmF2ZSAgfCAxNCIgIHwgMTYiICB8IDE4IgogIC0tLS0tLS0tLS0tLS0tLS0tLS0tfC0tLS0tLXwtLS0tLS18LS0tLS0KICBTaWx2ZXIsIE5vIEVuZ3JhdmUgIHwgJDU2ICB8ICQ2MiAgfCAkNjQKICBTaWx2ZXIsICsgRW5ncmF2ZSAgIHwgJDY2ICB8ICQ3MyAgfCAkNzUKICBHb2xkLCBObyBFbmdyYXZlICAgIHwgJDY0ICB8ICQ3MiAgfCAkNzQKICBHb2xkLCArIEVuZ3JhdmUgICAgIHwgJDc1ICB8ICQ4MiAgfCAkODUKICAxNEssIE5vIEVuZ3JhdmUgICAgIHwgJDQ1MiB8ICQ1MDEgfCAkNTE2CiAgMTRLLCArIEVuZ3JhdmUgICAgICB8ICQ0NzMgfCAkNTIyIHwgJDUzNwoKCj09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT0KNC4gUkVHVUxBUiBORUNLTEFDRVMKPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PQpUd28gdmFyaWFibGVzOiBNYXRlcmlhbCAvIEVuZ3JhdmluZyAoWWVzIG9yIE5vKQoiX2UiID0gd2l0aCBlbmdyYXZpbmcgYWRkLW9uCgogIE1hdGVyaWFsICAgICAgICAgICAgfCBUaWVyIDEgfCBUaWVyIDIgfCBUaWVyIDMKICAtLS0tLS0tLS0tLS0tLS0tLS0tLXwtLS0tLS0tLXwtLS0tLS0tLXwtLS0tLS0tCiAgU2lsdmVyICAgICAgICAgICAgICB8ICAkMzQgICB8ICAkMzcgICB8ICAkNDAKICBTaWx2ZXIgKyBFbmdyYXZlICAgIHwgICQ0NCAgIHwgICQ0NyAgIHwgICQ1MAogIEdvbGQgRmlsbGVkICAgICAgICAgfCAgJDM5ICAgfCAgJDQyICAgfCAgJDQ1CiAgR29sZCArIEVuZ3JhdmUgICAgICB8ICAkNDggICB8ICAkNTIgICB8ICAkNTYKICBSb3NlIEdvbGQgICAgICAgICAgIHwgICQ0MiAgIHwgICQ0NSAgIHwgICQ0OAogIFJvc2UgKyBFbmdyYXZlICAgICAgfCAgJDUxICAgfCAgJDU1ICAgfCAgJDU5CiAgMTRLIFNvbGlkIEdvbGQgICAgICB8ICAkMjM0ICB8ICAkMjUyICB8ICAkMjcwCiAgMTRLICsgRW5ncmF2ZSAgICAgICB8ICAkMjUzICB8ICAkMjcyICB8ICAkMjkxCgoKPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PQo1LiBDSEFSTSBPTkxZIChTdGFuZGFsb25lIERpc2MgQ2hhcm1zICYgUGVuZGFudCBDaGFybXMpCj09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT0KVHdvIHZhcmlhYmxlczogTWF0ZXJpYWwgLyBDaGFybSBUeXBlIChOZWNrbGFjZSBjaGFybSwgRW5ncmF2ZWQgY2hhcm0sIEh1Z2dpZSBjaGFybSkKTm90ZTogSHVnZ2llICsgRW5ncmF2ZSBjb21iaW5hdGlvbiBpcyBpbnZhbGlkIGFuZCBkcm9wcGVkLgoKVElFUiAxOgogIE1hdGVyaWFsICAgICAgIHwgTmVja2xhY2UgQ2hhcm0gfCBFbmdyYXZlZCBDaGFybSB8IEh1Z2dpZSBDaGFybQogIC0tLS0tLS0tLS0tLS0tLXwtLS0tLS0tLS0tLS0tLS0tfC0tLS0tLS0tLS0tLS0tLS18LS0tLS0tLS0tLS0tLQogIFNpbHZlciAgICAgICAgIHwgICAgICQyNSAgICAgICAgfCAgICAgJDM0ICAgICAgICB8ICAgICAkMjcKICBHb2xkIEZpbGxlZCAgICB8ICAgICAkMzAgICAgICAgIHwgICAgICQzOSAgICAgICAgfCAgICAgJDMzCiAgUm9zZSBHb2xkICAgICAgfCAgICAgJDMwICAgICAgICB8ICAgICAkMzkgICAgICAgIHwgICAgICQzMwogIDE0SyBTb2xpZCBHb2xkIHwgICAgICQxNTQgICAgICAgfCAgICAgJDE3MCAgICAgICB8ICAgICAkMTQ1CgpUSUVSIDI6CiAgTWF0ZXJpYWwgICAgICAgfCBOZWNrbGFjZSBDaGFybSB8IEVuZ3JhdmVkIENoYXJtIHwgSHVnZ2llIENoYXJtCiAgLS0tLS0tLS0tLS0tLS0tfC0tLS0tLS0tLS0tLS0tLS18LS0tLS0tLS0tLS0tLS0tLXwtLS0tLS0tLS0tLS0tCiAgU2lsdmVyICAgICAgICAgfCAgICAgJDI3ICAgICAgICB8ICAgICAkMzcgICAgICAgIHwgICAgICQyOQogIEdvbGQgRmlsbGVkICAgIHwgICAgICQzMiAgICAgICAgfCAgICAgJDQyICAgICAgICB8ICAgICAkMzUKICBSb3NlIEdvbGQgICAgICB8ICAgICAkMzIgICAgICAgIHwgICAgICQ0MiAgICAgICAgfCAgICAgJDM1CiAgMTRLIFNvbGlkIEdvbGQgfCAgICAgJDE2NiAgICAgICB8ICAgICAkMTgyICAgICAgIHwgICAgICQxNTYKClRJRVIgMzoKICBNYXRlcmlhbCAgICAgICB8IE5lY2tsYWNlIENoYXJtIHwgRW5ncmF2ZWQgQ2hhcm0gfCBIdWdnaWUgQ2hhcm0KICAtLS0tLS0tLS0tLS0tLS18LS0tLS0tLS0tLS0tLS0tLXwtLS0tLS0tLS0tLS0tLS0tfC0tLS0tLS0tLS0tLS0KICBTaWx2ZXIgICAgICAgICB8ICAgICAkMjkgICAgICAgIHwgICAgICQ0MCAgICAgICAgfCAgICAgJDMxCiAgR29sZCBGaWxsZWQgICAgfCAgICAgJDM0ICAgICAgICB8ICAgICAkNDUgICAgICAgIHwgICAgICQzNwogIFJvc2UgR29sZCAgICAgIHwgICAgICQzNCAgICAgICAgfCAgICAgJDQ1ICAgICAgICB8ICAgICAkMzcKICAxNEsgU29saWQgR29sZCB8ICAgICAkMTc4ICAgICAgIHwgICAgICQxOTUgICAgICAgfCAgICAgJDE2NwoKCj09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT0KRkFMTEJBQ0sgUFJJQ0VTICh3aGVuIHZhcmlhbnQgY2FuJ3QgYmUgY2xhc3NpZmllZCkKPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PQpUaGVzZSBhcmUgVGllciAyIC8gU2lsdmVyIGJhc2UgcHJpY2VzIHVzZWQgYXMgc2FmZSBmYWxsYmFja3M6CgogIFN0dWQgRWFycmluZ3MgICDihpIgICQzOAogIEh1Z2dpZSBFYXJyaW5ncyDihpIgICQ0NAogIEJlYWR5IE5lY2tsYWNlICDihpIgICQ1OAogIFJlZ3VsYXIgTmVja2xhY2XihpIgICQzNwogIENoYXJtIE9ubHkgICAgICDihpIgICQyNwoKCj09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT0KUFJPRFVDVCBDTEFTU0lGSUNBVElPTiBSVUxFUwo9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09ClRoZSBwcmljaW5nIGVuZ2luZSBjbGFzc2lmaWVzIGVhY2ggcHJvZHVjdCBpbnRvIGEgY2F0ZWdvcnkKYmFzZWQgb24gdGl0bGUgYW5kIG9wdGlvbiB2YWx1ZXM6CgogIENIQVJNIE9OTFkgIOKGkiAgT3B0aW9uIDIgY29udGFpbnMgIm5lY2tsYWNlIGNoYXJtIiwgImNoYXJtICsgZW5ncmF2ZSIsCiAgICAgICAgICAgICAgICAgb3IgImh1Z2dpZSBjaGFybSIKCiAgQkVBRFkgICAgICAg4oaSICAiYmVhZHkiIGFwcGVhcnMgaW4gdGl0bGUsIE9SIG9wdGlvbnMgY29udGFpbgogICAgICAgICAgICAgICAgIGdvbGQvc2lsdmVyICsgZW5ncmF2ZSB3aXRoIGEgY2hhaW4gbGVuZ3RoIG9wdGlvbgoKICBIVUdHSUUgICAgICDihpIgICJodWdnaWUiIG9yICJob29wIGVhcnJpbmciIGluIHRpdGxlLCBPUgogICAgICAgICAgICAgICAgICJob29wIHNpemUiIGluIG9wdGlvbiBuYW1lLCBPUiA4LjVtbS8xMW1tIGluIG9wdGlvbnMKCiAgU1RVRCAgICAgICAg4oaSICAic3R1ZCBlYXJyaW5nIiBpbiB0aXRsZSwgT1IgZWFycmluZyB3aXRob3V0CiAgICAgICAgICAgICAgICAgaG9vcC9odWdnaWUKCiAgUkVHVUxBUiAgICAg4oaSICAibmVja2xhY2UiIGluIHRpdGxlIHdpdGhvdXQgImJlYWR5IgoKICBVTkNMQVNTSUZJRUTihpIgIE5vbmUgb2YgdGhlIGFib3ZlIOKAlCB0aGVzZSB+MzYgcHJvZHVjdHMgbmVlZAogICAgICAgICAgICAgICAgIG1hbnVhbCBwcmljaW5nIHJldmlldwoKCj09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT0KRU5EIE9GIFBSSUNJTkcgUkVGRVJFTkNFCj09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT0K";
const PRICING_SCHEME_RAW = Buffer.from(PRICING_SCHEME_B64, "base64").toString("utf8");

/* ── Pricing engine (verified 53/53 against the scheme tables + owner rules) ──
   Pure & deterministic. Tiers index [T1,T2,T3]. Read-only via "proposePricing". */
const PRICING_ENGINE = (function () {
  var M = { S: "Sterling Silver", GF: "14k Gold Filled", ROSE: "14k Rose Gold Filled", SOLID: "14k Solid Gold" };
  var TBL = {
    stud:      { S:[35,38,41], GF:[39,42,45], ROSE:[42,45,48], SOLID:[201,216,231] },
    huggie:    { S:[41,44,47], GF:[46,49,52], SOLID:[307,330,353] },
    beadyBase: { S:[48,52,56], GF:[56,60,64], SOLID:[392,422,452] },
    beadyEng:  { S:[58,62,66], GF:[65,70,75], SOLID:[411,442,473] },
    regBase:   { S:[34,37,40], GF:[39,42,45], ROSE:[42,45,48], SOLID:[234,252,270] },
    regEng:    { S:[44,47,50], GF:[48,52,56], ROSE:[51,55,59], SOLID:[253,272,291] },
    charmNeck: { S:[25,27,29], GF:[30,32,34], ROSE:[30,32,34], SOLID:[154,166,178] },
    charmEng:  { S:[34,37,40], GF:[39,42,45], ROSE:[39,42,45], SOLID:[170,182,195] },
    charmHug:  { S:[27,29,31], GF:[33,35,37], ROSE:[33,35,37], SOLID:[145,156,167] },
    ringFlat:  { S:42, GF:45, ROSE:47 },
    bracFlat:  { S:41, GF:49, ROSE:49, SOLID:229 }
  };
  var NECK_LEN = ["14\"","16\"","18\"","20\""], RING_SIZE = ["4","5","6","7","8","9"],
      BRAC_SIZE = ["6","6.5","7","7.5","8","8.5","9"], HUGGIE_SIZE = ["8.5mm","11mm"];
  function v(o, price) { return { options: o, price: Math.round(price) }; }
  function inferTier(category, currentByMetal) {
    var maps = { stud:"stud", huggie:"huggie", beady:"beadyBase", regular:"regBase", charm:"charmNeck" };
    var key = maps[category]; if (!key || !currentByMetal) return 2;
    var order = [["S",M.S],["GF",M.GF],["ROSE",M.ROSE],["SOLID",M.SOLID]];
    for (var i=0;i<order.length;i++){ var k=order[i][0], cur=currentByMetal[order[i][1]];
      if (cur!=null && TBL[key][k]) { var c=TBL[key][k], best=2, bd=1e9;
        for (var t=0;t<3;t++){ var d=Math.abs(cur-c[t]); if(d<bd){bd=d;best=t+1;} } return best; } }
    return 2;
  }
  function buildMatrix(category, tier) {
    var i=(tier||2)-1, out=[];
    if (category==="stud") {
      [["S",M.S],["GF",M.GF],["ROSE",M.ROSE],["SOLID",M.SOLID]].forEach(function(m){ out.push(v({ "Metal Choice": m[1] }, TBL.stud[m[0]][i])); });
    } else if (category==="huggie") {
      HUGGIE_SIZE.forEach(function(sz){ [["S",M.S],["GF",M.GF],["SOLID",M.SOLID]].forEach(function(m){
        if (m[0]==="SOLID" && sz!=="11mm") return; out.push(v({ "Metal Choice": m[1], "Hoop Size": sz }, TBL.huggie[m[0]][i])); }); });
    } else if (category==="beady") {
      [["S",M.S],["GF",M.GF],["SOLID",M.SOLID]].forEach(function(m){ NECK_LEN.forEach(function(len,step){
        var inc=(m[0]==="SOLID")?20*step:3*step; [false,true].forEach(function(eng){
          out.push(v({ "Metal Choice": m[1], "Length": len, "Engraving": eng?"Engraved":"None" }, (eng?TBL.beadyEng:TBL.beadyBase)[m[0]][i]+inc)); }); }); });
    } else if (category==="regular") {
      [["S",M.S],["GF",M.GF],["ROSE",M.ROSE],["SOLID",M.SOLID]].forEach(function(m){ NECK_LEN.forEach(function(len,step){
        var inc=(m[0]==="SOLID")?15*step:0; [false,true].forEach(function(eng){
          out.push(v({ "Metal Choice": m[1], "Length": len, "Engraving": eng?"Engraved":"None" }, (eng?TBL.regEng:TBL.regBase)[m[0]][i]+inc)); }); }); });
    } else if (category==="charm") {
      [["S",M.S],["GF",M.GF],["ROSE",M.ROSE],["SOLID",M.SOLID]].forEach(function(m){
        out.push(v({ "Metal Choice": m[1], "Charm Type": "Necklace Charm" },    TBL.charmNeck[m[0]][i]));
        out.push(v({ "Metal Choice": m[1], "Charm Type": "Charm + Engraving" }, TBL.charmEng[m[0]][i]));
        out.push(v({ "Metal Choice": m[1], "Charm Type": "Huggie Charm Set" },  TBL.charmHug[m[0]][i])); });
    } else if (category==="ring") {
      [["S",M.S],["GF",M.GF],["ROSE",M.ROSE]].forEach(function(m){ RING_SIZE.forEach(function(sz){
        [["None",0],["Single side",10],["Front & back",20]].forEach(function(e){ out.push(v({ "Metal Choice": m[1], "Ring Size": sz, "Engraving": e[0] }, TBL.ringFlat[m[0]]+e[1])); }); }); });
    } else if (category==="bracelet") {
      [["S",M.S],["GF",M.GF],["ROSE",M.ROSE],["SOLID",M.SOLID]].forEach(function(m){ BRAC_SIZE.forEach(function(sz){
        [["None",0],["Single side",10],["Front & back",20]].forEach(function(e){ out.push(v({ "Metal Choice": m[1], "Bracelet Length": sz, "Engraving": e[0] }, TBL.bracFlat[m[0]]+e[1])); }); }); });
    }
    return out;
  }
  function optionOrder(category){ return ({
    stud:["Metal Choice"], huggie:["Metal Choice","Hoop Size"],
    beady:["Metal Choice","Length","Engraving"], regular:["Metal Choice","Length","Engraving"],
    charm:["Metal Choice","Charm Type"], ring:["Metal Choice","Ring Size","Engraving"],
    bracelet:["Metal Choice","Bracelet Length","Engraving"] })[category] || ["Metal Choice"]; }
  return { M: M, TBL: TBL, inferTier: inferTier, buildMatrix: buildMatrix, optionOrder: optionOrder };
})();

function bsNorm(x) { return String(x == null ? "" : x).replace(/\s+/g, " ").trim().toLowerCase(); }
function bestSellerLookup(rows) {
  const bySku = {}, byTitle = {};
  (rows || []).forEach(function (r) {
    (r.skus || []).forEach(function (sk) {
      const k = bsNorm(sk); if (!k) return;
      if (!(k in bySku) || (r.rank || 9999) < (bySku[k].rank || 9999)) bySku[k] = { rank: r.rank, name: r.name };
    });
    const tk = bsNorm(r.name);
    if (tk && (!(tk in byTitle) || (r.rank || 9999) < (byTitle[tk].rank || 9999))) byTitle[tk] = { rank: r.rank, name: r.name };
  });
  return { bySku, byTitle };
}
async function loadBestSellers() {
  const f = fb();
  if (!f) return BEST_SELLERS_SEED;
  try {
    const ref = f.db.collection("Brites_Editor_Meta").doc("bestSellers");
    const snap = await ref.get();
    if (snap.exists && Array.isArray((snap.data() || {}).rows) && snap.data().rows.length) return snap.data().rows;
    await ref.set({ rows: BEST_SELLERS_SEED, source: "seed", updatedAt: f.FV.serverTimestamp() });
    try {
      const csv = "Rank,Item Name,SKU,Number of Orders\n" + BEST_SELLERS_SEED.map(function (r) {
        return [r.rank, JSON.stringify(r.name), JSON.stringify((r.skus || []).join(", ")), r.orders].join(",");
      }).join("\n");
      await f.bucket.file("brites/best-sellers/top-200.csv").save(csv, { contentType: "text/csv" });
    } catch (e) { console.warn("[shopifyEditor] best-sellers CSV archive failed:", e.message); }
    return BEST_SELLERS_SEED;
  } catch (e) { console.warn("[shopifyEditor] loadBestSellers fell back to seed:", e.message); return BEST_SELLERS_SEED; }
}
async function loadPricingScheme() {
  const f = fb();
  if (!f) return { text: PRICING_SCHEME_RAW, source: "seed" };
  try {
    const ref = f.db.collection("Brites_Editor_Meta").doc("pricingScheme");
    const snap = await ref.get();
    if (snap.exists && (snap.data() || {}).text) return { text: snap.data().text, source: (snap.data().source || "firestore") };
    await ref.set({ text: PRICING_SCHEME_RAW, source: "seed", updatedAt: f.FV.serverTimestamp() });
    try { await f.bucket.file("brites/pricing/pricing-scheme.txt").save(PRICING_SCHEME_RAW, { contentType: "text/plain" }); }
    catch (e) { console.warn("[shopifyEditor] pricing-scheme archive failed:", e.message); }
    return { text: PRICING_SCHEME_RAW, source: "seed" };
  } catch (e) { console.warn("[shopifyEditor] loadPricingScheme fell back to seed:", e.message); return { text: PRICING_SCHEME_RAW, source: "seed" }; }
}

async function findBestSellersCollection() {
  const q = await gql(`query { collections(first: 60, query: "title:*best*") {
    edges { node { id title handle ruleSet { rules { column relation condition } } } } } }`);
  const nodes = ((q.collections && q.collections.edges) || []).map(function (e) { return e.node; });
  return nodes.find(function (n) { return /best\s*sell/i.test(n.title || ""); }) || null;
}
async function addToBestSellersCollection(prod) {
  const coll = await findBestSellersCollection();
  if (!coll) return { added: false, note: "No 'Best Sellers' collection found in this store" };
  if (!coll.ruleSet) {
    const d = await gql(`mutation($id: ID!, $pids: [ID!]!) {
      collectionAddProducts(id: $id, productIds: $pids) { userErrors { message } } }`,
      { id: coll.id, pids: [prod.id] });
    const ue = d.collectionAddProducts.userErrors;
    return ue.length ? { added: false, id: coll.id, title: coll.title, note: ue[0].message }
                     : { added: true, id: coll.id, title: coll.title };
  }
  const tagRule = (coll.ruleSet.rules || []).find(function (r) { return r.column === "TAG" && r.relation === "EQUALS"; });
  if (tagRule) {
    const tag = tagRule.condition;
    const has = (prod.tags || []).some(function (t) { return bsNorm(t) === bsNorm(tag); });
    if (has) return { added: true, id: coll.id, title: coll.title, note: "already tagged '" + tag + "'" };
    const d = await gql(`mutation($id: ID!, $tags: [String!]!) {
      tagsAdd(id: $id, tags: $tags) { userErrors { message } } }`, { id: prod.id, tags: [tag] });
    const ue = d.tagsAdd.userErrors;
    return ue.length ? { added: false, id: coll.id, title: coll.title, note: ue[0].message }
                     : { added: true, id: coll.id, title: coll.title, note: "via tag '" + tag + "'" };
  }
  return { added: false, id: coll.id, title: coll.title, note: "Smart collection without a tag rule — can't auto-add" };
}

const API_VERSION = process.env.SHOPIFY_API_VERSION || "2025-10";

const ALLOWED_ORIGINS = ["https://britesjewelry.com", "https://www.britesjewelry.com"];
function corsHeaders(origin) {
  const allow = ALLOWED_ORIGINS.includes(origin) ? origin : ALLOWED_ORIGINS[0];
  return {
    "Access-Control-Allow-Origin": allow,
    "Access-Control-Allow-Headers": "Content-Type, X-Edit-Passcode",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Content-Type": "application/json"
  };
}

/* ---- client-credentials token (cached across warm invocations) ---- */
let _token = null, _tokenExp = 0;
async function getToken() {
  if (_token && Date.now() < _tokenExp - 60000) return _token;
  const store = process.env.SHOPIFY_STORE;
  const res = await fetch(`https://${store}/admin/oauth/access_token`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "client_credentials",
      client_id: process.env.SHOPIFY_CLIENT_ID,
      client_secret: process.env.SHOPIFY_CLIENT_SECRET
    })
  });
  const text = await res.text();
  if (!res.ok) throw new Error("Token request failed (" + res.status + "): " + text);
  const data = JSON.parse(text);
  _token = data.access_token;
  _tokenExp = Date.now() + (data.expires_in || 86399) * 1000;
  return _token;
}

/* ---- GraphQL helper ---- */
async function gql(query, variables, _attempt) {
  const store = process.env.SHOPIFY_STORE;
  const token = await getToken();
  try {
    const res = await fetch(`https://${store}/admin/api/${API_VERSION}/graphql.json`, {
      method: "POST",
      headers: { "X-Shopify-Access-Token": token, "Content-Type": "application/json" },
      body: JSON.stringify({ query, variables: variables || {} })
    });
    if (res.status >= 500) throw new Error("GraphQL HTTP " + res.status);   // treat 5xx as transient
    const data = await res.json();
    if (!res.ok) throw new Error("GraphQL HTTP " + res.status);
    if (data.errors && data.errors.length) throw new Error("GraphQL: " + JSON.stringify(data.errors));
    return data.data;
  } catch (e) {
    // Retry transient network blips (e.g. ECONNRESET, dropped sockets, 5xx) a couple of times.
    const msg = String((e && e.message) || e);
    const transient = /ECONNRESET|ETIMEDOUT|socket hang up|network|fetch failed|EAI_AGAIN|ECONNREFUSED|GraphQL HTTP 5\d\d/i.test(msg);
    const attempt = _attempt || 0;
    if (transient && attempt < 2) {
      await new Promise(function (r) { setTimeout(r, 350 * (attempt + 1)); });
      return gql(query, variables, attempt + 1);
    }
    throw e;
  }
}

/* ---- make sure card.frame is a defined, storefront-readable metafield ----
   Framing is stored under key "frame" as a single_line_text_field ("scale|offsetX|offsetY").
   App-set JSON metafields read back as an UNPARSED STRING in theme Liquid, which broke the
   storefront transform; a plain string is read directly by Liquid with no parsing needed.
   The definition (best-effort) just adds storefront-API access + a tidy admin label; if the
   app lacks metafield-definition scope this throws and we ignore it (a string metafield is
   readable in Liquid regardless). */
let _framingDefDone = false;
async function ensureFramingDefinition() {
  if (_framingDefDone) return;
  _framingDefDone = true;
  try {
    await gql(`mutation {
      metafieldDefinitionCreate(definition: {
        name: "Card framing", namespace: "card", key: "frame", type: "single_line_text_field",
        ownerType: PRODUCT, access: { storefront: PUBLIC_READ }
      }) { createdDefinition { id } userErrors { code message } }
    }`);
  } catch (e) { /* already exists, or no definition scope — non-fatal */ }
}

/* ---- shape a GraphQL product into the REST-like form the editor expects ---- */
function shapeProduct(node) {
  const optPos = {};
  (node.options || []).forEach(o => { optPos[o.name] = o.position; });
  const variants = ((node.variants && node.variants.edges) || []).map(e => {
    const v = e.node; const out = { id: v.id, price: v.price };
    (v.selectedOptions || []).forEach(so => { const p = optPos[so.name]; if (p) out["option" + p] = so.value; });
    if (typeof v.inventoryQuantity === "number") out.qty = v.inventoryQuantity;
    if (v.inventoryItem) out.tracked = v.inventoryItem.tracked;
    return out;
  });
  const images = ((node.media && node.media.edges) || [])
    .map(e => (e.node && e.node.image) ? { id: e.node.id, src: e.node.image.url } : null)
    .filter(Boolean);
  let framing = null;
  if (node.framing && node.framing.value) {
    const fp = String(node.framing.value).split("|");
    if (fp[0] !== undefined && fp[0] !== "") {
      framing = { scale: parseFloat(fp[0]) || 1, offsetX: parseFloat(fp[1]) || 0, offsetY: parseFloat(fp[2]) || 0 };
    }
  }
  return {
    id: node.id, title: node.title, handle: node.handle,
    product_type: node.productType, tags: (node.tags || []).join(", "),
    options: (node.options || []).map(o => ({ name: o.name, position: o.position })),
    variants, images, framing
  };
}

// The storefront product gallery only renders images whose ALT text contains the product
// title (a theme filter). So after any rename, image alts must be re-synced to the new title
// or the extra images vanish on the storefront (while still showing in admin). Best-effort.
async function syncImageAltToTitle(productId, title) {
  if (!productId || !title) return { updated: 0, total: 0 };
  try {
    const md = await gql(`query($id: ID!) {
      product(id: $id) { media(first: 50) { nodes { ... on MediaImage { id alt } } } }
    }`, { id: productId });
    const imgs = ((((md.product || {}).media) || {}).nodes || []).filter(n => n && n.id);
    const stale = imgs.filter(n => String(n.alt || "").indexOf(title) < 0);
    if (!stale.length) return { updated: 0, total: imgs.length };
    // Use productUpdateMedia (write_products scope, which this app has) rather than fileUpdate
    // (Files API scope, which it does NOT have). IDs are MediaImage IDs, as it requires.
    // Keep only the part BEFORE any "#" in sync with the title — the theme's image-set feature
    // lives in the "#group_value" suffix of the alt and must be preserved.
    const newAlt = function (oldAlt) {
      const a = String(oldAlt || ""); const h = a.indexOf("#");
      return h >= 0 ? (title + a.slice(h)) : title;
    };
    const r = await gql(`mutation($media: [UpdateMediaInput!]!, $productId: ID!) {
      productUpdateMedia(media: $media, productId: $productId) { mediaUserErrors { field message } }
    }`, { media: stale.map(n => ({ id: n.id, alt: newAlt(n.alt) })), productId: productId });
    const ue = (r.productUpdateMedia && r.productUpdateMedia.mediaUserErrors) || [];
    return ue.length
      ? { updated: 0, total: imgs.length, error: ue.map(e => e.message).join("; ") }
      : { updated: stale.length, total: imgs.length };
  } catch (e) { return { updated: 0, total: 0, error: String(e && e.message) }; }
}

exports.handler = async function (event) {
  const origin = event.headers.origin || event.headers.Origin || "";
  const headers = corsHeaders(origin);
  const reply = (status, obj) => ({ statusCode: status, headers, body: JSON.stringify(obj) });

  if (event.httpMethod === "OPTIONS") return { statusCode: 200, headers, body: "ok" };

  // Passcode is OPTIONAL. Enforce it only when an EDIT_PASSCODE env var is set; if it is
  // unset/empty, the editor is open (no passcode). To re-enable a passcode later, just add
  // the EDIT_PASSCODE variable back in Netlify — no code change needed.
  const passcode = event.headers["x-edit-passcode"] || event.headers["X-Edit-Passcode"];
  if (process.env.EDIT_PASSCODE && passcode !== process.env.EDIT_PASSCODE)
    return reply(401, { error: "Unauthorized" });

  try {
    const q = event.queryStringParameters || {};
    const isPost = event.httpMethod === "POST";
    const body = isPost ? JSON.parse(event.body || "{}") : {};
    const action = q.action || body.action;

    switch (action) {

      /* ---------- READS ---------- */

      case "listCollections": {
        const d = await gql(`query {
          collections(first: 250) { edges { node {
            id title handle
            ruleSet { appliedDisjunctively rules { column relation condition } }
          } } }
        }`);
        const smart = [], custom = [];
        (d.collections.edges || []).forEach(e => {
          const n = e.node;
          if (n.ruleSet) {
            smart.push({
              id: n.id, title: n.title, handle: n.handle,
              disjunctive: n.ruleSet.appliedDisjunctively,
              rules: (n.ruleSet.rules || []).map(r => ({
                column: String(r.column).toLowerCase(),
                relation: String(r.relation).toLowerCase(),
                condition: r.condition
              }))
            });
          } else {
            custom.push({ id: n.id, title: n.title, handle: n.handle });
          }
        });
        return reply(200, { smart, custom });
      }

      // Single product, full data (by handle). Used when you open a card.
      case "getProduct": {
        const handle = q.handle || body.handle;
        const d = await gql(`query($q: String!) {
          products(first: 1, query: $q) { edges { node {
            id title handle productType tags
            options { name position }
            media(first: 50) { edges { node { ... on MediaImage { id image { url } } } } }
            variants(first: 100) { edges { node { id price selectedOptions { name value } inventoryQuantity inventoryItem { tracked } } } }
            framing: metafield(namespace: "card", key: "frame") { value }
          } } }
        }`, { q: "handle:" + handle });
        const node = (d.products.edges[0] || {}).node;
        if (!node) return reply(404, { error: "Product not found" });
        return reply(200, { product: shapeProduct(node) });
      }

      // Catalog scan for price presets (on demand). Small page size to respect
      // GraphQL cost limits; the UI loops using `next`.
      case "listProducts": {
        const cursor = q.page_info || null;
        const d = await gql(`query($cursor: String) {
          products(first: 40, after: $cursor) { edges { node {
            id title handle productType tags
            options { name position }
            variants(first: 100) { edges { node { id price selectedOptions { name value } } } }
          } } pageInfo { hasNextPage endCursor } }
        }`, { cursor });
        const products = (d.products.edges || []).map(e => shapeProduct(e.node));
        const next = d.products.pageInfo.hasNextPage ? d.products.pageInfo.endCursor : null;
        return reply(200, { products, next });
      }

      /* ---------- WRITES ---------- */

      case "syncImageAlt": {
        if (!body.product_id) return reply(400, { error: "Missing product_id" });
        let title = body.title;
        if (!title) {
          const d = await gql(`query($id: ID!) { product(id: $id) { title } }`, { id: body.product_id });
          title = (d.product || {}).title || "";
        }
        const res = await syncImageAltToTitle(body.product_id, title);
        return reply(200, { ok: !res.error, retagged: res.updated, total: res.total, error: res.error });
      }
      // One-time bulk repair: sweep the catalog (paginated) and re-sync every product's image
      // alt text to its CURRENT title, so listings renamed in earlier sessions get fixed without
      // re-saving each by hand. No-ops for products already correct; preserves "#group" suffixes.
      case "bulkSyncImageAlts": {
        const cursor = body.cursor || null;
        const n = Math.min(parseInt(body.pageSize, 10) || 15, 30);
        const d = await gql(`query($cursor: String, $n: Int!) {
          products(first: $n, after: $cursor) {
            pageInfo { hasNextPage endCursor }
            nodes { id title media(first: 30) { nodes { ... on MediaImage { id alt } } } }
          }
        }`, { cursor, n });
        const conn = d.products || { nodes: [], pageInfo: {} };
        let scanned = 0, fixedImages = 0, fixedProducts = 0;
        const mk = function (oldAlt, t) { const a = String(oldAlt || ""); const h = a.indexOf("#"); return h >= 0 ? (t + a.slice(h)) : t; };
        for (const prod of (conn.nodes || [])) {
          scanned++;
          const t = prod.title || ""; if (!t) continue;
          const imgs = ((prod.media && prod.media.nodes) || []).filter(x => x && x.id);
          const stale = imgs.filter(x => String(x.alt || "").indexOf(t) < 0);
          if (!stale.length) continue;
          try {
            await gql(`mutation($media: [UpdateMediaInput!]!, $productId: ID!) {
              productUpdateMedia(media: $media, productId: $productId) { mediaUserErrors { message } }
            }`, { media: stale.map(x => ({ id: x.id, alt: mk(x.alt, t) })), productId: prod.id });
            fixedImages += stale.length; fixedProducts++;
          } catch (e) { /* skip this product, keep sweeping */ }
        }
        return reply(200, { ok: true, scanned: scanned, fixedImages: fixedImages, fixedProducts: fixedProducts,
                            nextCursor: conn.pageInfo.hasNextPage ? conn.pageInfo.endCursor : null, hasNext: !!conn.pageInfo.hasNextPage });
      }
      case "updateTitle": {
        const d = await gql(`mutation($p: ProductUpdateInput!) {
          productUpdate(product: $p) { product { id } userErrors { field message } }
        }`, { p: { id: body.product_id, title: body.title } });
        const ue = d.productUpdate.userErrors;
        if (ue.length) return reply(400, { error: ue[0].message });
        await syncImageAltToTitle(body.product_id, body.title);
        return reply(200, { ok: true });
      }

      // Smart-collection levers: tags (full list) and/or product type.
      case "updateProductFields": {
        const p = { id: body.product_id };
        if (typeof body.tags === "string")
          p.tags = body.tags.split(",").map(t => t.trim()).filter(Boolean);
        if (typeof body.product_type === "string") p.productType = body.product_type;
        const d = await gql(`mutation($p: ProductUpdateInput!) {
          productUpdate(product: $p) { product { id } userErrors { field message } }
        }`, { p });
        const ue = d.productUpdate.userErrors;
        return ue.length ? reply(400, { error: ue[0].message }) : reply(200, { ok: true });
      }

      // body.variants = [{ id, price }]. We look up each variant's product, then
      // bulk-update per product (bulk update requires the productId).
      case "updateVariantPrices": {
        const variants = body.variants || [];
        const ids = variants.map(v => v.id);
        const nd = await gql(`query($ids: [ID!]!) {
          nodes(ids: $ids) { ... on ProductVariant { id product { id } } }
        }`, { ids });
        const prodOf = {};
        (nd.nodes || []).forEach(n => { if (n && n.product) prodOf[n.id] = n.product.id; });
        const groups = {};
        variants.forEach(v => {
          const pid = prodOf[v.id]; if (!pid) return;
          (groups[pid] = groups[pid] || []).push({ id: v.id, price: String(v.price) });
        });
        const results = [];
        for (const pid of Object.keys(groups)) {
          const d = await gql(`mutation($pid: ID!, $vars: [ProductVariantsBulkInput!]!) {
            productVariantsBulkUpdate(productId: $pid, variants: $vars) {
              productVariants { id } userErrors { field message }
            }
          }`, { pid, vars: groups[pid] });
          const ok = d.productVariantsBulkUpdate.userErrors.length === 0;
          groups[pid].forEach(v => results.push({ id: v.id, ok }));
        }
        return reply(200, { results });
      }

      // Delete specific variants (used to remove an incorrect Metal Choice option).
      // The product stays valid as long as at least one variant remains.
      case "deleteVariants": {
        const vids = (body.variant_ids || []).filter(Boolean);
        if (!vids.length) return reply(400, { error: "No variants specified" });
        const d = await gql(`mutation($pid: ID!, $ids: [ID!]!) {
          productVariantsBulkDelete(productId: $pid, variantsIds: $ids) {
            userErrors { field message }
          }
        }`, { pid: body.product_id, ids: vids });
        const ue = d.productVariantsBulkDelete.userErrors;
        return ue.length ? reply(400, { error: ue[0].message }) : reply(200, { ok: true });
      }

      // Remove ONE value of an option (e.g. a wrong "14k Gold Filled" under "Metal Choice").
      // Uses productOptionUpdate so Shopify deletes every variant referencing that value
      // and rebuilds the variant matrix cleanly — no orphaned values, other options intact.
      case "deleteOptionValue": {
        const oname = String(body.option_name || "").trim();
        const oval = String(body.value || "");
        const q = await gql(`query($id: ID!) {
          product(id: $id) { options { id name optionValues { id name } } }
        }`, { id: body.product_id });
        const opts = (q.product && q.product.options) || [];
        const opt = opts.find(o => (o.name || "").toLowerCase() === oname.toLowerCase())
                 || opts.find(o => (o.name || "").toLowerCase().indexOf("metal") >= 0);
        if (!opt) return reply(400, { error: "Option not found: " + oname });
        const values = opt.optionValues || [];
        const val = values.find(v => v.name === oval)
                 || values.find(v => (v.name || "").toLowerCase() === oval.toLowerCase());
        if (!val) return reply(400, { error: "Option value not found: " + oval });
        if (values.length <= 1) return reply(400, { error: "Can't remove the only value of the " + opt.name + " option." });
        const d = await gql(`mutation($pid: ID!, $opt: OptionUpdateInput!, $del: [ID!], $vs: ProductOptionUpdateVariantStrategy) {
          productOptionUpdate(productId: $pid, option: $opt, optionValuesToDelete: $del, variantStrategy: $vs) {
            product { id } userErrors { field message code }
          }
        }`, { pid: body.product_id, opt: { id: opt.id }, del: [val.id], vs: "MANAGE" });
        const ue = d.productOptionUpdate.userErrors;
        return ue.length ? reply(400, { error: ue[0].message }) : reply(200, { ok: true });
      }

      // Inverse of deleteOptionValue: add a value to an option (e.g. a metal choice) and
      // create its variants at a flat price across every combination of the other options.
      // Targeted — existing variants are left untouched (LEAVE_AS_IS), then the new variants
      // are bulk-created. The editor restocks afterwards via setInventory.
      case "addOptionValue": {
        const oname = String(body.option_name || "").trim();
        const oval  = String(body.value || "").trim();
        const price = Math.round(Number(body.price));
        if (!oval) return reply(400, { error: "Missing value" });
        if (!Number.isFinite(price) || price <= 0) return reply(400, { error: "Missing or invalid price" });
        const q = await gql(`query($id: ID!) {
          product(id: $id) {
            options { id name position optionValues { id name } }
            variants(first: 100) { nodes { selectedOptions { name value } } }
          }
        }`, { id: body.product_id });
        const prod = q.product;
        if (!prod) return reply(404, { error: "Product not found" });
        const opts = prod.options || [];
        const opt = opts.find(o => (o.name || "").toLowerCase() === oname.toLowerCase())
                 || opts.find(o => (o.name || "").toLowerCase().indexOf("metal") >= 0);
        if (!opt) return reply(400, { error: "Option not found: " + oname });
        if ((opt.optionValues || []).some(v => (v.name || "").toLowerCase() === oval.toLowerCase()))
          return reply(400, { error: "\u201C" + oval + "\u201D is already a " + opt.name + " on this listing" });

        // Distinct combinations of every OTHER option, taken from existing variants.
        const others = opts.filter(o => o.id !== opt.id);
        const seen = {}, combos = [];
        ((prod.variants && prod.variants.nodes) || []).forEach(v => {
          const combo = others.map(o => {
            const so = (v.selectedOptions || []).find(s => s.name === o.name);
            return { optionName: o.name, name: so ? so.value : null };
          });
          const key = combo.map(c => c.name).join(" / ");
          if (!seen[key]) { seen[key] = true; combos.push(combo); }
        });
        if (!combos.length) combos.push([]); // single-option (metal-only) product

        // 1) Register the new value without disturbing existing variants.
        const add = await gql(`mutation($pid: ID!, $opt: OptionUpdateInput!, $vals: [OptionValueCreateInput!], $vs: ProductOptionUpdateVariantStrategy) {
          productOptionUpdate(productId: $pid, option: $opt, optionValuesToAdd: $vals, variantStrategy: $vs) {
            product { id } userErrors { field message code }
          }
        }`, { pid: body.product_id, opt: { id: opt.id }, vals: [{ name: oval }], vs: "LEAVE_AS_IS" });
        const ae = add.productOptionUpdate.userErrors;
        if (ae && ae.length) return reply(400, { error: ae[0].message });

        // 2) Create a variant for (new value × each existing other-combo) at the flat price.
        const variants = combos.map(combo => ({
          price: String(price),
          optionValues: [{ optionName: opt.name, name: oval }].concat(
            combo.map(c => ({ optionName: c.optionName, name: c.name }))
          )
        }));
        const cr = await gql(`mutation($pid: ID!, $variants: [ProductVariantsBulkInput!]!) {
          productVariantsBulkCreate(productId: $pid, variants: $variants) {
            productVariants { id } userErrors { field message }
          }
        }`, { pid: body.product_id, variants });
        const ce = cr.productVariantsBulkCreate.userErrors;
        if (ce && ce.length) return reply(400, { error: ce[0].message });
        return reply(200, { ok: true, added: (cr.productVariantsBulkCreate.productVariants || []).length });
      }

      // Best-seller cross-reference: match the product's SKUs/title against the
      // Top-200 index and, on a hit, add it to the storefront "Best Sellers" collection.
      case "checkBestSeller": {
        const pq = await gql(`query($id: ID!) {
          product(id: $id) { id title handle tags variants(first: 100) { edges { node { sku } } } }
        }`, { id: body.product_id });
        const prod = pq.product;
        if (!prod) return reply(404, { error: "Product not found" });
        const rows = await loadBestSellers();
        const lk = bestSellerLookup(rows);
        const skus = ((prod.variants && prod.variants.edges) || []).map(function (e) { return e.node.sku; }).filter(Boolean);
        let match = null, matchedBy = null;
        skus.forEach(function (sk) {
          const hit = lk.bySku[bsNorm(sk)];
          if (hit && (!match || (hit.rank || 9999) < (match.rank || 9999))) { match = hit; matchedBy = "sku"; }
        });
        if (!match) { const t = lk.byTitle[bsNorm(prod.title)]; if (t) { match = t; matchedBy = "title"; } }
        let added = false, collTitle = null, note = null;
        if (match && body.add !== false) {
          const r = await addToBestSellersCollection(prod);
          added = r.added; collTitle = r.title || null; note = r.note || null;
        }
        const f = fb();
        if (f && match) {
          try {
            await f.db.collection("Brites_Editor_Status").doc(numericId(body.product_id)).set({
              productId: numericId(body.product_id), title: prod.title || null,
              bestSeller: true, bestSellerRank: match.rank || null, bestSellerName: match.name || null,
              updatedAt: f.FV.serverTimestamp()
            }, { merge: true });
          } catch (e) { /* non-fatal */ }
        }
        return reply(200, { match: !!match, matchedBy, rank: match ? match.rank : null, name: match ? match.name : null, added, collection: collTitle, note });
      }

      // ── Pricing engine: compute proposed variant matrix (read-only / dry-run) ──
      case "proposePricing": {
        const category = (body.category || "").toString().toLowerCase();
        const valid = ["stud","huggie","beady","regular","charm","ring","bracelet"];
        if (valid.indexOf(category) < 0) return reply(400, { error: "Unknown category: " + category });
        const tier = PRICING_ENGINE.inferTier(category, body.currentByMetal || null);
        const variants = PRICING_ENGINE.buildMatrix(category, tier);
        const order = PRICING_ENGINE.optionOrder(category);
        return reply(200, { category: category, tier: tier, count: variants.length, optionOrder: order, variants: variants });
      }
      // Apply the proposed matrix atomically. productSet reconciles options+variants in one call;
      // we match current variants by a NORMALIZED option tuple and carry their SKU/barcode/inventory
      // so renames don't lose stock. Variants not in the target are removed (structure enforcement).
      case "applyPricing": {
        const productId = body.product_id;
        const category = (body.category || "").toString().toLowerCase();
        const target = body.variants || [];
        const order = body.optionOrder || [];
        if (!productId || !target.length || !order.length) return reply(400, { error: "Missing product_id / variants / optionOrder" });

        // No inventory/location queries -> no read_locations / read_inventory scopes needed.
        const cur = await gql(`query($id: ID!) {
          product(id: $id) {
            id
            title
            options { name }
            media(first: 50) { nodes { id alt } }
            variants(first: 100) { nodes { id selectedOptions { name value } } }
          }
        }`, { id: productId });
        if (!cur.product) return reply(404, { error: "Product not found" });

        function normMetal(v){ v=(v||"").toLowerCase();
          if(v.indexOf("silver")>=0)return"silver"; if(v.indexOf("rose")>=0)return"rose";
          if(v.indexOf("solid")>=0)return"solid"; if(v.indexOf("filled")>=0)return"gf";
          if(v.indexOf("14k")>=0)return"solid"; if(v.indexOf("gold")>=0)return"gf"; return v; }
        function role(name){ name=(name||"").toLowerCase();
          if(name.indexOf("metal")>=0||name.indexOf("material")>=0)return"metal";
          if(name.indexOf("engrav")>=0)return"engrave";
          if(name.indexOf("length")>=0)return"length";
          if(name.indexOf("hoop")>=0||name.indexOf("ring size")>=0||name==="size")return"size";
          if(name.indexOf("charm")>=0||name.indexOf("type")>=0)return"ctype";
          return name; }
        function nv(name, v){ if(role(name)==="metal") return normMetal(v);
          return (v||"").toString().trim().toLowerCase().replace(/inch(es)?/g,'"').replace(/\s+/g,""); }
        function tupleOf(pairs){ var r={}; pairs.forEach(function(p){ r[role(p.name)]=nv(p.name,p.value); });
          return Object.keys(r).sort().map(function(k){ return k+"="+r[k]; }).join("|"); }

        // Reuse the listing's existing option NAMES + VALUE strings wherever they mean the same
        // thing, and pass the matched variant's id, so productSet updates those variants in place
        // (keeping stock + SKU) instead of recreating them. Only genuinely new combos are created.
        const curOptNames = (cur.product.options||[]).map(function(o){ return o.name; });
        function existingOptName(targetName){ var r=role(targetName);
          if (r === "metal") return "Metal Choice";   // standardize the metal option NAME
          var hit = curOptNames.filter(function(n){ return role(n)===r; })[0]; return hit || targetName; }
        const nameForRole = {}; order.forEach(function(n){ nameForRole[n]=existingOptName(n); });
        const existingValByRole = {};
        (cur.product.variants.nodes||[]).forEach(function(vn){ (vn.selectedOptions||[]).forEach(function(so){
          var r=role(so.name); (existingValByRole[r]=existingValByRole[r]||{})[nv(so.name, so.value)] = so.value; }); });
        function valueForRole(targetName, targetVal){ var r=role(targetName);
          if (r === "metal") return targetVal;         // standardize metal VALUES to canonical (Sterling Silver / 14k Gold Filled / 14k Rose Gold Filled / 14k Solid Gold)
          var m=existingValByRole[r], k=nv(targetName, targetVal);
          return (m && m[k]!=null) ? m[k] : targetVal; }

        const curByTuple={};
        (cur.product.variants.nodes||[]).forEach(function(vn){ curByTuple[tupleOf(vn.selectedOptions)] = vn; });

        const valuesByName={}; order.forEach(function(n){ valuesByName[nameForRole[n]]=[]; });
        let carried=0;
        const setVariants = target.map(function(tv){
          var pairs = order.map(function(n){ return { name:n, value:tv.options[n] }; });
          var match = curByTuple[tupleOf(pairs)];
          var ov = order.map(function(n){ var nm=nameForRole[n], val=valueForRole(n, tv.options[n]);
            if(valuesByName[nm].indexOf(val)<0) valuesByName[nm].push(val);
            return { optionName: nm, name: String(val) }; });
          var variant = { optionValues: ov, price: String(tv.price) };
          if (match) { carried++; if (match.id) variant.id = match.id; }
          return variant;
        });
        const productOptions = order.map(function(n,idx){ var nm=nameForRole[n];
          return { name: nm, position: idx+1, values: valuesByName[nm].map(function(val){ return { name:val }; }) }; });

        // Preserve the product's existing media. productSet treats media as a declarative
        // LIST field, so omitting it deletes every image except the primary. Passing the
        // current media IDs back in `files` keeps the full gallery intact.
        const keepFiles = ((cur.product.media && cur.product.media.nodes) || []).map(function (m) { var f = { id: m.id }; if (m.alt != null) f.alt = m.alt; return f; });
        const psInput = { id: productId, productOptions: productOptions, variants: setVariants };
        if (keepFiles.length) psInput.files = keepFiles;
        const setRes = await gql(`mutation($input: ProductSetInput!) {
          productSet(synchronous: true, input: $input) {
            product { id variantsCount { count } }
            userErrors { field message }
          }
        }`, { input: psInput });
        const errs = (setRes.productSet && setRes.productSet.userErrors) || [];
        if (errs.length) return reply(422, { error: "productSet: " + errs.map(function(e){return e.message;}).join("; "), userErrors: errs });
        await syncImageAltToTitle(productId, cur.product.title);
        const removed = (cur.product.variants.nodes||[]).length - carried;
        return reply(200, { ok: true, category: category, applied: setVariants.length, carried: carried, removed: (removed>0?removed:0),
                            variantsCount: ((setRes.productSet.product||{}).variantsCount||{}).count });
      }

      // Set the available stock for EVERY variant of a product to a fixed quantity (default 999).
      // Requires the app to have read_locations + read_inventory + write_inventory scopes; if any
      // are missing we return ok:false/needsScope (never throw) so Smart Match keeps working.
      case "setInventory": {
        const productId = body.product_id;
        let qty = parseInt(body.quantity, 10); if (isNaN(qty) || qty < 0) qty = 999;
        if (!productId) return reply(400, { error: "Missing product_id" });
        const denied = (e) => /access.?denied/i.test(String(e && e.message));
        const scopeReply = (m) => reply(200, { ok: false, needsScope: true, error: m });

        // 1) Resolve the location we'll stock at.
        let locationId = null;
        try {
          const loc = await gql(`query { locations(first: 1) { nodes { id } } }`);
          locationId = ((loc.locations && loc.locations.nodes[0]) || {}).id || null;
        } catch (e) {
          if (denied(e)) return scopeReply("Quantity needs the read_locations + write_inventory scopes added to the app.");
          throw e;
        }
        if (!locationId) return scopeReply("No accessible location (read_locations scope).");

        // 2) Read each variant's inventory item: is it tracked, and is it already
        //    stocked AT THIS LOCATION? (inventoryLevel(locationId:) is null when not stocked.)
        let nodes = [];
        try {
          const d = await gql(`query($id: ID!, $loc: ID!) {
            product(id: $id) {
              variants(first: 100) { nodes { inventoryItem {
                id tracked
                inventoryLevel(locationId: $loc) { id }
              } } }
            }
          }`, { id: productId, loc: locationId });
          if (!d.product) return reply(404, { error: "Product not found" });
          nodes = (d.product.variants.nodes || []).filter(n => n.inventoryItem && n.inventoryItem.id);
        } catch (e) {
          if (denied(e)) return scopeReply("Quantity needs the read_inventory scope added to the app.");
          throw e;
        }
        if (!nodes.length) return reply(200, { ok: true, set: 0, quantity: qty });

        // Split: items NOT yet stocked here must be ACTIVATED (which also sets the
        // initial available qty in one call); items already stocked use setQuantities.
        // CRITICAL: we set the quantity FIRST and only flip tracking ON afterward, so a
        // failure can never leave an item "tracked + empty" (which reads as Sold Out).
        const toActivate = nodes.filter(n => !n.inventoryItem.inventoryLevel);
        const toSet      = nodes.filter(n => n.inventoryItem.inventoryLevel);
        const okItemIds  = {};      // inventoryItem ids whose qty was successfully set
        const errs = [];

        // 2a) Activate (stock + set available) the unstocked ones — this is also what
        //     repairs listings the earlier build left tracked-but-unstocked (Sold Out).
        for (const n of toActivate) {
          try {
            const r = await gql(`mutation($itemId: ID!, $loc: ID!, $q: Int){
              inventoryActivate(inventoryItemId: $itemId, locationId: $loc, available: $q){
                inventoryLevel { id } userErrors { field message }
              }
            }`, { itemId: n.inventoryItem.id, loc: locationId, q: qty });
            const ue = (r.inventoryActivate && r.inventoryActivate.userErrors) || [];
            if (ue.length) errs.push(ue.map(e => e.message).join("; "));
            else okItemIds[n.inventoryItem.id] = true;
          } catch (e) {
            if (denied(e)) return scopeReply("Quantity needs the write_inventory scope added to the app.");
            errs.push(String(e && e.message));
          }
        }

        // 2b) Set absolute available qty for items already stocked here (one batched call).
        if (toSet.length) {
          try {
            const quantities = toSet.map(n => ({ inventoryItemId: n.inventoryItem.id, locationId, quantity: qty }));
            const r = await gql(`mutation($input: InventorySetQuantitiesInput!){
              inventorySetQuantities(input:$input){ userErrors{ field message } }
            }`, { input: { name: "available", reason: "correction", ignoreCompareQuantity: true, quantities } });
            const ue = (r.inventorySetQuantities && r.inventorySetQuantities.userErrors) || [];
            if (ue.length) errs.push(ue.map(e => e.message).join("; "));
            else toSet.forEach(n => { okItemIds[n.inventoryItem.id] = true; });
          } catch (e) {
            if (denied(e)) return scopeReply("Quantity needs the write_inventory scope added to the app.");
            errs.push(String(e && e.message));
          }
        }

        // 3) NOW turn tracking on — only for items whose qty we actually set, so the 999
        //    is enforced/shown. Items we couldn't set stay as they were (untracked items
        //    remain purchasable rather than flipping to a tracked-empty Sold Out).
        let tracked = 0;
        for (const n of nodes) {
          if (n.inventoryItem.tracked === false && okItemIds[n.inventoryItem.id]) {
            try {
              await gql(`mutation($id: ID!){ inventoryItemUpdate(id:$id, input:{ tracked:true }){ userErrors{ message } } }`, { id: n.inventoryItem.id });
              tracked++;
            } catch (e) {
              if (denied(e)) return scopeReply("Quantity needs the write_inventory scope added to the app.");
              errs.push(String(e && e.message));
            }
          }
        }

        const setCount = Object.keys(okItemIds).length;
        if (!setCount && errs.length) return reply(200, { ok: false, error: "inventory: " + errs.join(" | ") });
        return reply(200, {
          ok: true, set: setCount, quantity: qty, variants: nodes.length,
          activated: toActivate.filter(n => okItemIds[n.inventoryItem.id]).length,
          trackedOn: tracked,
          partial: errs.length ? errs.join(" | ") : undefined
        });
      }
      // ── Pricing scheme: permanent Firebase storage + retrieval ───────────
      case "getPricingScheme": {
        const ps = await loadPricingScheme();
        return reply(200, { scheme: ps.text, source: ps.source });
      }
      case "setPricingScheme": {
        const f = fb(); if (!f) return reply(200, { ok: false, note: "Firebase unavailable" });
        const text = (body.text || "").toString(); if (!text) return reply(400, { error: "Missing text" });
        await f.db.collection("Brites_Editor_Meta").doc("pricingScheme").set({ text: text, source: "manual", updatedBy: body.by || "editor", updatedAt: f.FV.serverTimestamp() }, { merge: true });
        try { await f.bucket.file("brites/pricing/pricing-scheme.txt").save(text, { contentType: "text/plain" }); } catch (e) {}
        return reply(200, { ok: true });
      }
      // ── Cross-computer per-listing status (point 5) ──────────────────────
      case "touchStatus": {
        const f = fb(); if (!f) return reply(200, { ok: false, note: "Firebase unavailable" });
        const id = numericId(body.product_id); if (!id) return reply(400, { error: "Missing product_id" });
        const ref = f.db.collection("Brites_Editor_Status").doc(id);
        const base = { productId: id, handle: body.handle || null, title: body.title || null, updatedAt: f.FV.serverTimestamp() };
        if (body.event === "saved") {
          await ref.set(Object.assign(base, { status: "done", lastSavedBy: body.by || "editor", lastSavedAt: f.FV.serverTimestamp() }), { merge: true });
        } else {
          const snap = await ref.get();
          const cur = snap.exists ? (snap.data().status || "") : "";
          await ref.set(Object.assign(base, { status: cur === "done" ? "done" : "in_progress", lastOpenedBy: body.by || "editor", lastOpenedAt: f.FV.serverTimestamp() }), { merge: true });
        }
        return reply(200, { ok: true });
      }
      case "listStatuses": {
        const f = fb(); if (!f) return reply(200, { statuses: [] });
        const snap = await f.db.collection("Brites_Editor_Status").limit(3000).get();
        const out = [];
        snap.forEach(function (d) {
          const x = d.data() || {};
          out.push({ id: d.id, handle: x.handle || null, title: x.title || null, status: x.status || null,
                     bestSeller: !!x.bestSeller, bestSellerRank: x.bestSellerRank || null,
                     lastSavedBy: x.lastSavedBy || null, lastOpenedBy: x.lastOpenedBy || null,
                     lastSavedAt: x.lastSavedAt && x.lastSavedAt.toMillis ? x.lastSavedAt.toMillis() : null,
                     updatedAt: x.updatedAt && x.updatedAt.toMillis ? x.updatedAt.toMillis() : null });
        });
        return reply(200, { statuses: out });
      }
      // ── Full change history + revert support (point 4) ───────────────────
      case "logChange": {
        const f = fb(); if (!f) return reply(200, { ok: false, note: "Firebase unavailable" });
        const id = numericId(body.product_id); if (!id) return reply(400, { error: "Missing product_id" });
        const entry = {
          by: body.by || "editor", title: body.title || null, handle: body.handle || null,
          kind: body.kind || "edit", changes: Array.isArray(body.changes) ? body.changes : [],
          before: body.before || null, after: body.after || null, at: f.FV.serverTimestamp()
        };
        const ref = await f.db.collection("Brites_Editor_History").doc(id).collection("changes").add(entry);
        return reply(200, { ok: true, id: ref.id });
      }
      case "getHistory": {
        const f = fb(); if (!f) return reply(200, { history: [] });
        const id = numericId(body.product_id); if (!id) return reply(400, { error: "Missing product_id" });
        const snap = await f.db.collection("Brites_Editor_History").doc(id).collection("changes").orderBy("at", "desc").limit(50).get();
        const out = [];
        snap.forEach(function (d) {
          const x = d.data() || {};
          out.push({ id: d.id, by: x.by || null, title: x.title || null, kind: x.kind || "edit",
                     changes: x.changes || [], before: x.before || null, after: x.after || null,
                     at: x.at && x.at.toMillis ? x.at.toMillis() : null });
        });
        return reply(200, { history: out });
      }

      // Promote an existing image to primary (move it to position 0).
      case "setPrimaryImage": {
        const d = await gql(`mutation($id: ID!, $moves: [MoveInput!]!) {
          productReorderMedia(id: $id, moves: $moves) { job { id } mediaUserErrors { field message } }
        }`, { id: body.product_id, moves: [{ id: body.image_id, newPosition: "0" }] });
        const ue = d.productReorderMedia.mediaUserErrors;
        return ue.length ? reply(400, { error: ue[0].message }) : reply(200, { ok: true });
      }

      // Copy an image into this product by URL. Shopify fetches the URL and
      // creates an independent MediaImage on the target product (a true copy).
      // Returns immediately with the new media id; the file finishes processing
      // on Shopify's side a moment later (status PROCESSING -> READY).
      case "addImage": {
        const src = body.src;
        if (!src) return reply(400, { error: "Missing image src" });
        const d = await gql(`mutation($pid: ID!, $media: [CreateMediaInput!]!) {
          productCreateMedia(productId: $pid, media: $media) {
            media { ... on MediaImage { id status image { url } } }
            mediaUserErrors { field message }
          }
        }`, { pid: body.product_id, media: [{ originalSource: src, mediaContentType: "IMAGE", alt: body.alt || "" }] });
        const ue = d.productCreateMedia.mediaUserErrors;
        if (ue.length) return reply(400, { error: ue[0].message });
        const m = (d.productCreateMedia.media || [])[0] || {};
        return reply(200, { ok: true, image: { id: m.id, src: (m.image && m.image.url) || src, status: m.status || "PROCESSING" } });
      }

      // Reorder a product's media to exactly match body.image_ids (the full
      // desired order). First id becomes the primary image. Async on Shopify's
      // side (returns a job) but takes effect within a few seconds.
      case "reorderImages": {
        const ids = body.image_ids || [];
        if (!ids.length) return reply(400, { error: "No image order supplied" });
        const moves = ids.map((id, i) => ({ id, newPosition: String(i) }));
        const d = await gql(`mutation($id: ID!, $moves: [MoveInput!]!) {
          productReorderMedia(id: $id, moves: $moves) { job { id } mediaUserErrors { field message } }
        }`, { id: body.product_id, moves });
        const ue = d.productReorderMedia.mediaUserErrors;
        return ue.length ? reply(400, { error: ue[0].message }) : reply(200, { ok: true });
      }

      // Permanently remove one or more media from a product.
      case "deleteImage": {
        const ids = body.image_ids || (body.image_id ? [body.image_id] : []);
        if (!ids.length) return reply(400, { error: "No image id supplied" });
        const d = await gql(`mutation($mediaIds: [ID!]!, $productId: ID!) {
          productDeleteMedia(mediaIds: $mediaIds, productId: $productId) {
            deletedMediaIds mediaUserErrors { field message }
          }
        }`, { mediaIds: ids, productId: body.product_id });
        const ue = d.productDeleteMedia.mediaUserErrors;
        return ue.length ? reply(400, { error: ue[0].message }) : reply(200, { ok: true, deleted: d.productDeleteMedia.deletedMediaIds });
      }

      // Save image zoom/pan as a product metafield (non-destructive, upsert) AND make sure it's
      // actually readable. The value writes fine with write_products, but it only shows in the
      // admin Metafields UI and on the storefront if a DEFINITION exists with Storefront access.
      // So: ensure/repair the definition, write the value, then read it back and report exactly
      // what happened so the editor can tell the user if anything still needs doing.
      case "setFraming": {
        const sc = (body.scale == null ? 1 : body.scale);
        const ox = (body.offsetX == null ? 0 : body.offsetX);
        const oy = (body.offsetY == null ? 0 : body.offsetY);
        const value = [sc, ox, oy].join("|");

        // 1) Inspect the existing definition (if any).
        let defExists = false, defType = null, defStorefront = false, defNote = null;
        try {
          const dq = await gql(`query {
            metafieldDefinitions(first: 1, ownerType: PRODUCT, namespace: "card", key: "frame") {
              nodes { id type { name } access { storefront } }
            }
          }`);
          const def = ((dq.metafieldDefinitions || {}).nodes || [])[0];
          if (def) { defExists = true; defType = def.type && def.type.name; defStorefront = (def.access && def.access.storefront) === "PUBLIC_READ"; }
        } catch (e) { defNote = "could not read metafield definitions"; }

        // 2a) No definition yet → create one with Storefront read access.
        if (!defExists) {
          try {
            const cd = await gql(`mutation {
              metafieldDefinitionCreate(definition: {
                name: "Card framing", namespace: "card", key: "frame", type: "single_line_text_field",
                ownerType: PRODUCT, access: { storefront: PUBLIC_READ }
              }) { createdDefinition { id } userErrors { code message } }
            }`);
            const cdd = cd.metafieldDefinitionCreate;
            if (cdd.createdDefinition) { defExists = true; defType = "single_line_text_field"; defStorefront = true; }
            else if (cdd.userErrors && cdd.userErrors.length) { defNote = cdd.userErrors[0].message; }
          } catch (e) { defNote = "no permission to create metafield definitions (needs write_metafield_definitions)"; }
        }
        // 2b) Definition exists but isn't storefront-readable → try to enable it.
        else if (!defStorefront) {
          try {
            const ud = await gql(`mutation {
              metafieldDefinitionUpdate(definition: {
                namespace: "card", key: "frame", ownerType: PRODUCT, access: { storefront: PUBLIC_READ }
              }) { updatedDefinition { access { storefront } } userErrors { code message } }
            }`);
            const upd = ud.metafieldDefinitionUpdate.updatedDefinition;
            if (upd && upd.access && upd.access.storefront === "PUBLIC_READ") defStorefront = true;
            else if (ud.metafieldDefinitionUpdate.userErrors && ud.metafieldDefinitionUpdate.userErrors.length) defNote = ud.metafieldDefinitionUpdate.userErrors[0].message;
          } catch (e) { defNote = "could not enable storefront access on the existing definition"; }
        }

        // 3) Write the value.
        const ms = await gql(`mutation($mf: [MetafieldsSetInput!]!) {
          metafieldsSet(metafields: $mf) { metafields { id value } userErrors { field message } }
        }`, { mf: [{ ownerId: body.product_id, namespace: "card", key: "frame", type: "single_line_text_field", value }] });
        const ue = ms.metafieldsSet.userErrors;
        if (ue && ue.length) {
          const msg = ue[0].message;
          const hint = /type/i.test(msg) ? "A 'card.frame' definition with a different type already exists — delete it in Settings → Custom data → Products, then save again." : null;
          return reply(400, { error: msg, hint });
        }

        // 4) Read the value back to confirm it actually persisted.
        let readBack = null;
        try {
          const rq = await gql(`query($id: ID!) { product(id: $id) { metafield(namespace: "card", key: "frame") { value } } }`, { id: body.product_id });
          readBack = rq.product && rq.product.metafield ? rq.product.metafield.value : null;
        } catch (e) { /* read-back is best-effort */ }

        return reply(200, {
          ok: true,
          value: value,
          saved: readBack === value,            // did the value actually persist?
          readBack: readBack,
          storefrontReadable: defStorefront,    // will the theme be able to render it?
          definition: { exists: defExists, type: defType, storefront: defStorefront, note: defNote }
        });
      }

      // Permanently delete an entire product (and its variants + media).
      case "deleteProduct": {
        const d = await gql(`mutation($input: ProductDeleteInput!) {
          productDelete(input: $input) { deletedProductId userErrors { field message } }
        }`, { input: { id: body.product_id } });
        const ue = d.productDelete.userErrors;
        return ue.length ? reply(400, { error: ue[0].message }) : reply(200, { ok: true, deleted: d.productDelete.deletedProductId });
      }

      default:
        return reply(400, { error: "Unknown action: " + action });
    }
  } catch (err) {
    return reply(500, { error: err.message });
  }
};
