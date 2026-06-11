// netlify/functions/syncBestSellers.js
// ---------------------------------------------------------------------------
// Code-driven Best Sellers sync for Brites Jewelry.
//
// Source of truth: the cleaned + deduped Top 200 (189 unique products after
// collapsing duplicate SKU-family rows and correcting mis-mapped SKUs).
// Embedded below as BEST_SELLERS_RESOLVED; a Firestore override doc
// (Brites_Editor_Meta/bestSellersResolved) takes precedence once written, so
// the list can be refreshed without a redeploy (same pattern as shopifyEditor).
//
// What a sync run does, idempotently:
//   1. Resolves every handle -> product ID via Admin GraphQL.
//   2. Ensures the Best Sellers collection (handle env BEST_SELLERS_HANDLE,
//      default "pendant" -- the live collection titled "Best Sellers").
//      - Manual collection: adds missing products, optionally removes extras,
//        switches sort order to MANUAL and reorders to match true sales rank.
//      - Smart collection with a tag rule: tags products instead.
//   3. Tags every member "BJ-Best-Seller" and writes metafield
//      brites.bs_rank (number_integer) for theme-side use.
//   4. Persists the resolved list + run report to Firestore.
//
// Endpoints (same auth as the editor -- X-Edit-Passcode header):
//   GET  ?action=dryRun       -> resolve + report, writes nothing
//   POST {action:"sync", removeExtras:false}
//
// Weekly automation (netlify.toml):
//   [functions."syncBestSellers"]
//     schedule = "@weekly"
//   Scheduled invocations carry no passcode; they are allowed only when the
//   event comes from Netlify's scheduler (detected via headers below).
//
// Env vars (already set for shopifyEditor): SHOPIFY_STORE, SHOPIFY_CLIENT_ID,
// SHOPIFY_CLIENT_SECRET, EDIT_PASSCODE, SHOPIFY_API_VERSION (optional),
// BEST_SELLERS_HANDLE (optional, default "pendant").
// ---------------------------------------------------------------------------

const fetch = require("node-fetch");

let _fb = null;
function fb() {
  if (_fb !== null) return _fb;
  try {
    const admin = require("./firebaseAdmin");
    _fb = { admin, db: admin.firestore(), FV: admin.firestore.FieldValue };
  } catch (e) { console.error("[syncBestSellers] Firebase unavailable:", e.message); _fb = false; }
  return _fb;
}

const API_VERSION = process.env.SHOPIFY_API_VERSION || "2025-10";
const BS_HANDLE = process.env.BEST_SELLERS_HANDLE || "pendant";
const BS_TAG = "BJ-Best-Seller";

/* ---- client-credentials token (identical pattern to shopifyEditor) ---- */
let _token = null, _tokenExp = 0;
async function getToken() {
  if (_token && Date.now() < _tokenExp - 60000) return _token;
  const res = await fetch(`https://${process.env.SHOPIFY_STORE}/admin/oauth/access_token`, {
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
async function gql(query, variables, _attempt) {
  const token = await getToken();
  try {
    const res = await fetch(`https://${process.env.SHOPIFY_STORE}/admin/api/${API_VERSION}/graphql.json`, {
      method: "POST",
      headers: { "X-Shopify-Access-Token": token, "Content-Type": "application/json" },
      body: JSON.stringify({ query, variables: variables || {} })
    });
    if (res.status >= 500) throw new Error("GraphQL HTTP " + res.status);
    const data = await res.json();
    if (!res.ok) throw new Error("GraphQL HTTP " + res.status);
    if (data.errors && data.errors.length) throw new Error("GraphQL: " + JSON.stringify(data.errors));
    return data.data;
  } catch (e) {
    const msg = String((e && e.message) || e);
    const transient = /ECONNRESET|ETIMEDOUT|socket hang up|network|fetch failed|EAI_AGAIN|ECONNREFUSED|GraphQL HTTP 5\d\d/i.test(msg);
    const attempt = _attempt || 0;
    if (transient && attempt < 2) { await new Promise(r => setTimeout(r, 350 * (attempt + 1))); return gql(query, variables, attempt + 1); }
    throw e;
  }
}

/* ---- Cleaned Top-200, deduped by SKU family (189 products). Rank = true
       sales order after merging duplicate rows. Generated offline from
       Top_200_Best_Sellers_2026.csv x the full Shopify product export. ---- */
const BEST_SELLERS_RESOLVED = [{"rank":1,"handle":"dainty-bunny","title":"Dainty Bunny Necklace","orders":206},{"rank":2,"handle":"norse-runes-charm-necklace","title":"Norse Runes Charm","orders":203},{"rank":3,"handle":"circle-jacket-charm-necklace-set-of-2-for-everyday-layering","title":"Circle Jacket Charm Necklace Set of 2","orders":154},{"rank":4,"handle":"fire-badge","title":"Fire Badge Necklace","orders":118},{"rank":5,"handle":"dainty-sun-necklace","title":"Dainty Sun Necklace","orders":110},{"rank":6,"handle":"phoenix-bird-pendant-necklace-for-bird-lovers","title":"Phoenix Bird Pendant Necklace","orders":108},{"rank":7,"handle":"cardinal-bird-pendant-necklace-for-bird-lovers","title":"Cardinal Bird Pendant Necklace","orders":104},{"rank":8,"handle":"sunflower-charm-necklace-on-beady-chain-for-flower-lovers","title":"Sunflower Charm Necklace","orders":86},{"rank":9,"handle":"hummingbird-charm-stud-earrings-for-everyday-layering","title":"Hummingbird Charm Stud Earrings","orders":86},{"rank":10,"handle":"dragonfly-pendant-necklace-for-moms-and-mothers-day","title":"Dragonfly Pendant Necklace","orders":85},{"rank":11,"handle":"bunny-charm-stud-earrings-for-animal-lovers","title":"Bunny Charm Stud Earrings","orders":79},{"rank":12,"handle":"book-reader-charm-necklace-on-beady-chain-for-teachers","title":"Book Reader Charm Necklace","orders":74},{"rank":13,"handle":"dainty-cancer","title":"Dainty Cancer Ribbon Necklace","orders":68},{"rank":14,"handle":"baseball-charm-huggie","title":"Baseball Charm Huggie","orders":66},{"rank":15,"handle":"wolf-necklace","title":"Wolf Necklace","orders":65},{"rank":16,"handle":"squirrel-charm-necklace","title":"Squirrel Charm Necklace","orders":65},{"rank":17,"handle":"eagle-pendant-necklace-for-gift-giving","title":"Eagle Pendant Necklace","orders":62},{"rank":18,"handle":"your-handwritten-bar-necklace","title":"Your Handwritten Bar","orders":60},{"rank":19,"handle":"team-charm-necklace-for-everyday-layering","title":"Team Charm Necklace","orders":60},{"rank":20,"handle":"hummingbird-pendant-necklace-for-bird-lovers","title":"Hummingbird Pendant Necklace","orders":58},{"rank":21,"handle":"dainty-rubber","title":"Dainty Rubber Ducky Necklace","orders":56},{"rank":22,"handle":"movie-slate-charm-necklace","title":"Movie Slate Charm","orders":55},{"rank":23,"handle":"small-cat-necklace","title":"Small Cat Necklace","orders":54},{"rank":24,"handle":"killer-whale","title":"Killer Whale Charm Necklace","orders":53},{"rank":25,"handle":"sitting-cat-earrings","title":"Sitting Cat Earrings","orders":52},{"rank":26,"handle":"skating-earrings-ice","title":"Skating Earrings Ice","orders":52},{"rank":27,"handle":"add-on-an-extender-to-charm-for-gift-giving","title":"Add on an Extender to Charm","orders":51},{"rank":28,"handle":"sea-turtle","title":"Cute Sea Turtle Necklace","orders":50},{"rank":29,"handle":"mythical-dragon-charm-stud-earrings-for-bridesmaids","title":"Mythical Dragon Charm Stud Earrings","orders":50},{"rank":30,"handle":"skating-necklace","title":"Skating Necklace","orders":49},{"rank":31,"handle":"running-horse-stud","title":"Running Horse Stud","orders":48},{"rank":32,"handle":"camping-earrings-campfire","title":"Camping Earrings Campfire","orders":47},{"rank":33,"handle":"theatre-happy-sad","title":"Theatre Happy Sad","orders":47},{"rank":34,"handle":"alligator-necklace","title":"Alligator Necklace","orders":45},{"rank":35,"handle":"cardinal-bird-pendant-necklace-for-everyday-layering","title":"Cardinal Bird Pendant Necklace","orders":44},{"rank":36,"handle":"sheep-pendant","title":"Sheep Lamb Pendant","orders":43},{"rank":37,"handle":"lemon-pendant-necklace-for-food-lovers","title":"Lemon Pendant Necklace","orders":42},{"rank":38,"handle":"little-hummingbird","title":"Little Hummingbird Necklace","orders":41},{"rank":39,"handle":"hammered-skinny-mini-gold-bar-bar-necklace-for-moms-and-mothers-day","title":"Hammered Skinny Mini Gold Bar Bar Necklace","orders":41},{"rank":40,"handle":"tiny-moon-necklace","title":"Tiny Moon Necklace","orders":40},{"rank":41,"handle":"mismatched-tennis-ball","title":"Mismatched Tennis Ball","orders":39},{"rank":42,"handle":"tooth-necklace","title":"Tooth Necklace, Dentist Gift","orders":38},{"rank":43,"handle":"heart-charm-necklace-for-everyday-layering","title":"Heart Charm Necklace","orders":38},{"rank":44,"handle":"key-charm-pendant-necklace","title":"Key Charm Pendant","orders":36},{"rank":45,"handle":"tooth-charm-huggie-hoops-for-everyday-layering","title":"Tooth Charm Huggie Hoops","orders":36},{"rank":46,"handle":"usa-map-pendant-necklace-for-flower-lovers","title":"USA Map Pendant Necklace","orders":36},{"rank":47,"handle":"soaring-pelican-pendant-necklace-for-bird-lovers","title":"Soaring Pelican Pendant Necklace","orders":36},{"rank":48,"handle":"penguin-charm-necklace-on-beady-chain-for-bird-lovers","title":"Penguin Charm Necklace","orders":36},{"rank":49,"handle":"hawk-charm-necklace-on-beady-chain-for-bird-lovers","title":"Hawk Charm Necklace","orders":35},{"rank":50,"handle":"lighthouse-charm-earrings-for-beach-lovers","title":"Lighthouse Charm Earrings","orders":35},{"rank":51,"handle":"stethoscope-earring-doctor","title":"Stethoscope earring Doctor","orders":35},{"rank":52,"handle":"boho-feather-charm-necklace-for-graduates","title":"Boho Feather Charm Necklace","orders":34},{"rank":53,"handle":"palestine-map-necklace","title":"Palestine Map Necklace","orders":34},{"rank":54,"handle":"chicken-pendant-necklace","title":"Chicken Pendant","orders":33},{"rank":55,"handle":"puzzle-pendant-necklace-for-best-friends","title":"Puzzle Pendant Necklace","orders":33},{"rank":56,"handle":"theatre-mask-charm-pendant-for-everyday-layering","title":"Theatre Mask Charm Pendant","orders":33},{"rank":57,"handle":"gold-dumbbell-charm-necklace-for-fitness-fans","title":"Gold Dumbbell Charm Necklace","orders":33},{"rank":58,"handle":"stethoscope-necklace-doctor","title":"Stethoscope Necklace Doctor","orders":33},{"rank":59,"handle":"capybara-charm-earrings-for-everyday-layering","title":"Capybara Charm Earrings","orders":33},{"rank":60,"handle":"raccoon-charm-necklace-on-beady-chain-for-moms-and-mothers-day","title":"Raccoon Charm Necklace","orders":33},{"rank":61,"handle":"dainty-oval","title":"Dainty Oval Disc Caduceus Necklace","orders":32},{"rank":62,"handle":"dragonfly-charm-stud-earrings-for-gift-giving","title":"Dragonfly Charm Stud Earrings","orders":32},{"rank":63,"handle":"dragon-pendant-necklace-for-gift-giving","title":"Dragon Pendant Necklace","orders":32},{"rank":64,"handle":"bear-charm-necklace","title":"Bear Charm Necklace","orders":32},{"rank":65,"handle":"chick-pendant-necklace-for-bird-lovers","title":"Chick Pendant Necklace","orders":32},{"rank":66,"handle":"ballet-shoes-pendant-necklace","title":"Ballet Shoes pendant","orders":31},{"rank":67,"handle":"lotus-pendant-necklace-for-flower-lovers","title":"Lotus Pendant Necklace","orders":30},{"rank":68,"handle":"graduation-cap-pendant-necklace-for-teachers","title":"Graduation Cap Pendant Necklace","orders":30},{"rank":69,"handle":"initial-charm-necklace","title":"Initial Charm Necklace","orders":30},{"rank":70,"handle":"puzzle-piece-beady-charm-necklace-on-beady-chain-for-best-friends","title":"Puzzle Piece Beady Charm Necklace","orders":29},{"rank":71,"handle":"map-of-palestine-charm-huggie-hoops-for-beach-lovers","title":"Map of Palestine Charm Huggie Hoops","orders":28},{"rank":72,"handle":"steamboat-willie-mickey-necklace","title":"Steamboat Willie Mickey","orders":28},{"rank":73,"handle":"comedy-tragedy-mask-pendant-necklace-for-everyday-layering","title":"Comedy Tragedy Mask Pendant Necklace","orders":28},{"rank":74,"handle":"apple-charm-with-necklace","title":"Apple Charm with","orders":26},{"rank":75,"handle":"fire-department-badge","title":"Fire Department Badge","orders":26},{"rank":76,"handle":"music-note-pendant-necklace-for-friends","title":"Music Note Pendant Necklace","orders":26},{"rank":77,"handle":"chicken-stud-earrings","title":"Chicken Stud Earrings","orders":26},{"rank":78,"handle":"rock-on-stud","title":"Rock On Stud","orders":26},{"rank":79,"handle":"sunflower-necklace","title":"Sunflower Necklace","orders":26},{"rank":80,"handle":"ballet-charm-stud-earrings-for-dancers","title":"Ballet Charm Stud Earrings","orders":25},{"rank":81,"handle":"hockey-stick-pendant-necklace-for-everyday-layering","title":"Hockey Stick Pendant Necklace","orders":25},{"rank":82,"handle":"leaping-bunny-pendant-necklace-for-animal-lovers","title":"Leaping Bunny Pendant Necklace","orders":25},{"rank":83,"handle":"running-shoe-pendant-necklace-for-birthdays","title":"Running Shoe Pendant Necklace","orders":25},{"rank":84,"handle":"bunny-charm-huggie","title":"Bunny Charm Huggie","orders":25},{"rank":85,"handle":"compass-disc-charm-necklace-for-guidance-and-direction","title":"Compass Disc Charm Necklace","orders":25},{"rank":86,"handle":"otter-charm-necklace","title":"Otter Charm Necklace","orders":25},{"rank":87,"handle":"swan-charm","title":"Swan Charm Necklace","orders":25},{"rank":88,"handle":"horse-pendant-necklace-for-everyday-layering","title":"Horse Pendant Necklace","orders":24},{"rank":89,"handle":"wolf-pendant-necklace-for-animal-lovers","title":"Wolf Pendant Necklace","orders":24},{"rank":90,"handle":"caduceus-pendant-necklace-for-everyday-layering","title":"Caduceus Pendant Necklace","orders":24},{"rank":91,"handle":"dachshund-earrings-animal","title":"Dachshund Earrings Animal","orders":24},{"rank":92,"handle":"dainty-cut","title":"Dainty Cut out Fox Necklace","orders":24},{"rank":93,"handle":"monkey-charm-necklace","title":"Monkey Charm Necklace","orders":24},{"rank":94,"handle":"apple-charm-necklace","title":"Apple Charm Necklace","orders":23},{"rank":95,"handle":"dainty-cowboy","title":"Dainty Cowboy Boot Necklace","orders":23},{"rank":96,"handle":"hockey-skate-pendant-necklace-for-moms-and-mothers-day","title":"Hockey Skate Pendant Necklace","orders":23},{"rank":97,"handle":"mountain-pendant-necklace-for-christmas","title":"Mountain Pendant Necklace","orders":23},{"rank":98,"handle":"swan-studs-14k","title":"Swan Studs 14k","orders":23},{"rank":99,"handle":"cute-rubber","title":"Cute Rubber Ducky Necklace","orders":22},{"rank":100,"handle":"graduation-cap","title":"Graduation Cap Charm Necklace","orders":22},{"rank":101,"handle":"running-shoe","title":"Running Shoe Necklace","orders":21},{"rank":102,"handle":"runner-earrings-sports","title":"Runner Earrings Sports","orders":21},{"rank":103,"handle":"dove-of-peace-pendant-necklace-for-bird-lovers","title":"Dove of Peace Pendant Necklace","orders":21},{"rank":104,"handle":"running-horse-charm-necklace","title":"Running Horse Charm","orders":21},{"rank":105,"handle":"blooming-dandelion-pendant-necklace-for-flower-lovers","title":"Blooming Dandelion Pendant Necklace","orders":21},{"rank":106,"handle":"caduceus-charm-stud-earrings-for-everyday-layering","title":"Caduceus Charm Stud Earrings","orders":21},{"rank":107,"handle":"horse-huggie-hoops","title":"Horse Huggie Hoops","orders":21},{"rank":108,"handle":"raccoon-charm-stud-earrings-for-everyday-layering","title":"Raccoon Charm Stud Earrings","orders":21},{"rank":109,"handle":"scuba-diver-earring-14k-goggle-charm-stud-earrings-for-beach-lovers","title":"Scuba Diver Earring 14k Goggle Charm Stud Earrings","orders":21},{"rank":110,"handle":"cheerleading-team-jewelry","title":"Cheerleading Team Jewelry","orders":20},{"rank":111,"handle":"labrador-pendant-necklace-for-dog-lovers","title":"Labrador Pendant Necklace","orders":20},{"rank":112,"handle":"owl-charm-necklace-on-beady-chain-for-bird-lovers","title":"Owl Charm Necklace","orders":20},{"rank":113,"handle":"heron-bird-pendant-necklace-for-bird-lovers","title":"Heron Bird Pendant Necklace","orders":20},{"rank":114,"handle":"lion-necklace-lion","title":"Lion Necklace Lion","orders":20},{"rank":115,"handle":"music-note-pendant-necklace-for-gift-giving","title":"Music Note Pendant Necklace","orders":20},{"rank":116,"handle":"peach-pendant-necklace-for-food-lovers","title":"Peach Pendant Necklace","orders":20},{"rank":117,"handle":"pisces-zodiac-stud","title":"Pisces Zodiac Stud","orders":20},{"rank":118,"handle":"gecko-necklace","title":"Gecko Necklace","orders":20},{"rank":119,"handle":"jupiter-necklace","title":"Jupiter Necklace","orders":20},{"rank":120,"handle":"mountain-charm-necklace","title":"Mountain Charm Necklace","orders":20},{"rank":121,"handle":"initial-charm-huggie-hoops-for-gift-giving","title":"Initial Charm Huggie Hoops","orders":20},{"rank":122,"handle":"police-badge","title":"Police Badge Necklace","orders":20},{"rank":123,"handle":"theatre-mask-pendant-necklace-for-everyday-layering","title":"Theatre Mask Pendant Necklace","orders":20},{"rank":124,"handle":"mini-volleyball","title":"Mini Volleyball Necklace","orders":20},{"rank":125,"handle":"lion-pendant-necklace-for-everyday-layering","title":"Lion Pendant Necklace","orders":19},{"rank":126,"handle":"ram-pendant-necklace-for-animal-lovers","title":"Ram Pendant Necklace","orders":19},{"rank":127,"handle":"flag-pendant-necklace-for-everyday-layering","title":"Flag Pendant Necklace","orders":19},{"rank":128,"handle":"cardinal-bird-pendant-necklace-for-gift-giving","title":"Cardinal Bird Pendant Necklace","orders":19},{"rank":129,"handle":"dachshund-charm-earrings-for-dog-lovers","title":"Dachshund Charm Earrings","orders":19},{"rank":130,"handle":"handcrafted-elephant-hoop","title":"Handcrafted Elephant Hoop","orders":19},{"rank":131,"handle":"hippopotamus-zoo-pendant-necklace-for-moms-and-mothers-day","title":"Hippopotamus Zoo Pendant Necklace","orders":19},{"rank":132,"handle":"jesus-fish-faith-pendant-necklace-for-beach-lovers","title":"Jesus Fish Faith Pendant Necklace","orders":19},{"rank":133,"handle":"manta-ray-charm-stud-earrings-for-beach-lovers","title":"Manta Ray Charm Stud Earrings","orders":19},{"rank":134,"handle":"orca-earrings-killer","title":"Orca Earrings Killer","orders":19},{"rank":135,"handle":"raccoon-pendant-necklace-for-moms-and-mothers-day","title":"Raccoon Pendant Necklace","orders":19},{"rank":136,"handle":"tiny-tag-necklace","title":"Tiny Tag Necklace","orders":19},{"rank":137,"handle":"dinosaur-charm-necklace","title":"Dinosaur Charm Necklace","orders":19},{"rank":138,"handle":"map-of-palestine-pendant-necklace-for-gift-giving","title":"Map of Palestine Pendant Necklace","orders":18},{"rank":139,"handle":"lemon-charm-pendant-necklace","title":"Lemon Charm pendant","orders":18},{"rank":140,"handle":"pansy-flower","title":"Pansy Flower Necklace","orders":18},{"rank":141,"handle":"gold-bunny-earrings","title":"Gold Bunny Earrings","orders":18},{"rank":142,"handle":"zoo-animal-charm-pendant-for-animal-lovers","title":"Zoo Animal Charm Pendant","orders":18},{"rank":143,"handle":"mama-bear","title":"Mama Bear Necklace","orders":18},{"rank":144,"handle":"medical-caduceus-pendant-necklace","title":"Medical Caduceus Pendant","orders":18},{"rank":145,"handle":"bicycle-charm-stud-earrings-for-everyday-layering","title":"Bicycle Charm Stud Earrings","orders":18},{"rank":146,"handle":"bullseye-pendant-necklace-for-everyday-layering","title":"Bullseye Pendant Necklace","orders":18},{"rank":147,"handle":"monogram-pendant-necklace-for-everyday-layering","title":"Monogram Pendant Necklace","orders":18},{"rank":148,"handle":"sea-turtle-pendant-necklace-for-gift-giving","title":"Sea Turtle Pendant Necklace","orders":17},{"rank":149,"handle":"leaf-pendant-necklace-for-everyday-layering","title":"Leaf Pendant Necklace","orders":17},{"rank":150,"handle":"cherry-blossom-stud","title":"Cherry Blossom Stud","orders":17},{"rank":151,"handle":"fox-lover-pendant-necklace-for-animal-lovers","title":"Fox Lover Pendant Necklace","orders":17},{"rank":152,"handle":"dragonfly-charm-stud-earrings-for-everyday-layering","title":"Dragonfly Charm Stud Earrings","orders":17},{"rank":153,"handle":"capybara-pendant","title":"Capybara Pendant","orders":17},{"rank":154,"handle":"lightning-bolt-thunderbolt-charm-necklace-for-birthdays","title":"Lightning Bolt Thunderbolt Charm Necklace","orders":17},{"rank":155,"handle":"eagle-pendant-necklace-for-moms-and-mothers-day","title":"Eagle Pendant Necklace","orders":17},{"rank":156,"handle":"dragon-charm-necklace","title":"Dragon Charm Necklace","orders":17},{"rank":157,"handle":"heart-pendant-necklace-for-christmas-gift-giving","title":"Heart Pendant Necklace","orders":17},{"rank":158,"handle":"poodle-charm-necklace-on-beady-chain-for-dog-lovers","title":"Poodle Charm Necklace","orders":17},{"rank":159,"handle":"saturn-pendant-necklace-for-celestial-layering","title":"Saturn Pendant Necklace","orders":17},{"rank":160,"handle":"paperclip-earrings","title":"Paperclip Earrings","orders":16},{"rank":161,"handle":"four-leaf-clover-pendant-necklace-for-good-luck","title":"Four Leaf Clover Pendant Necklace","orders":16},{"rank":162,"handle":"dumpster-fire-pendant-necklace-for-everyday-layering","title":"Dumpster Fire Pendant Necklace","orders":16},{"rank":163,"handle":"number-pendant-necklace-for-moms-and-mothers-day","title":"Number Pendant Necklace","orders":16},{"rank":164,"handle":"aztec-dragon-pendant-necklace-for-moms-and-mothers-day","title":"Aztec Dragon Pendant Necklace","orders":16},{"rank":165,"handle":"bear-pendant-necklace-for-moms-and-mothers-day","title":"Bear Pendant Necklace","orders":16},{"rank":166,"handle":"book-lover-charm-pendant-for-teachers","title":"Book Lover Charm Pendant","orders":16},{"rank":167,"handle":"dragon-pendant-necklace-for-birthdays","title":"Dragon Pendant Necklace","orders":16},{"rank":168,"handle":"volleyball-huggie-hoop","title":"Volleyball Huggie Hoop","orders":16},{"rank":169,"handle":"baseball-charm-necklace-on-beady-chain-for-everyday-layering","title":"Baseball Charm Necklace","orders":16},{"rank":170,"handle":"cancer-ribbon-earring","title":"Cancer Ribbon Earring","orders":16},{"rank":171,"handle":"cherry-blossom-charm","title":"Cherry Blossom Charm","orders":16},{"rank":172,"handle":"lyre-harp","title":"Lyre Harp Music Charm Necklace","orders":16},{"rank":173,"handle":"panda-charm-necklace-on-beady-chain-for-animal-lovers","title":"Panda Charm Necklace","orders":16},{"rank":174,"handle":"space-charm-huggie","title":"Space Charm Huggie","orders":16},{"rank":175,"handle":"science-laboratory-flasks-pendant-necklace-for-teachers","title":"Science Laboratory Flasks Pendant Necklace","orders":16},{"rank":176,"handle":"dna-pendant-necklace-for-birthdays-gift-giving","title":"Dna Pendant Necklace","orders":15},{"rank":177,"handle":"maple-leaf-pendant-necklace-for-canadians","title":"Maple Leaf Pendant Necklace","orders":15},{"rank":178,"handle":"baby-bear-charm-earrings-for-moms-and-mothers-day","title":"Baby Bear Charm Earrings","orders":15},{"rank":179,"handle":"flying-hawk-earrings","title":"Flying Hawk Earrings","orders":15},{"rank":180,"handle":"star-of-david-charm-huggie-hoops-for-celestial-layering","title":"Star of David Charm Huggie Hoops","orders":15},{"rank":181,"handle":"raccoon-pendant-necklace-for-necklace-layering","title":"Raccoon Pendant Necklace","orders":15},{"rank":182,"handle":"chicken-charm-stud-earrings-for-everyday-layering","title":"Chicken Charm Stud Earrings","orders":14},{"rank":183,"handle":"chicken-bar-necklace-for-bird-lovers-gift-giving","title":"Chicken Bar Necklace","orders":14},{"rank":184,"handle":"hamster-pendant-necklace-for-birthdays","title":"Hamster Pendant Necklace","orders":13},{"rank":185,"handle":"lobster-pendant-necklace-for-beach-lovers","title":"Lobster Pendant Necklace","orders":12},{"rank":186,"handle":"beaver-charm-earrings-for-canadians-gift-giving","title":"Beaver Charm Earrings","orders":12},{"rank":187,"handle":"cherry-blossom-flower-necklace","title":"Cherry Blossom Flower","orders":11},{"rank":188,"handle":"custom-hockey","title":"Hockey Stick Pendant","orders":11},{"rank":189,"handle":"crab-pendant-necklace-for-everyday-layering","title":"Crab Pendant Necklace","orders":4}];


async function loadList() {
  const f = fb();
  if (!f) return BEST_SELLERS_RESOLVED;
  try {
    const ref = f.db.collection("Brites_Editor_Meta").doc("bestSellersResolved");
    const snap = await ref.get();
    if (snap.exists && Array.isArray((snap.data() || {}).rows) && snap.data().rows.length) return snap.data().rows;
    await ref.set({ rows: BEST_SELLERS_RESOLVED, source: "seed", updatedAt: f.FV.serverTimestamp() });
    return BEST_SELLERS_RESOLVED;
  } catch (e) { return BEST_SELLERS_RESOLVED; }
}

async function resolveProducts(rows) {
  const found = {}, missing = [];
  for (let i = 0; i < rows.length; i += 25) {
    const batch = rows.slice(i, i + 25);
    const q = batch.map((r, j) =>
      `p${j}: productByHandle(handle: ${JSON.stringify(r.handle)}) { id handle title tags status }`).join("\n");
    const d = await gql(`query { ${q} }`);
    batch.forEach((r, j) => {
      const node = d["p" + j];
      if (node) found[r.handle] = node; else missing.push(r.handle);
    });
  }
  return { found, missing };
}

async function getCollection() {
  // applySiteFixes renames pendant -> best-sellers automatically, so try the
  // new handle first, then the env/default handle. No env change ever needed.
  const candidates = ["best-sellers", BS_HANDLE].filter((v, i, a) => a.indexOf(v) === i);
  for (const h of candidates) {
    const d = await gql(`query($h: String!) { collectionByHandle(handle: $h) {
      id title handle sortOrder ruleSet { rules { column relation condition } }
      products(first: 250) { edges { node { id handle } } }
    } }`, { h });
    if (d.collectionByHandle) return d.collectionByHandle;
  }
  return null;
}

async function runSync(opts) {
  const report = { startedAt: new Date().toISOString(), handle: BS_HANDLE, steps: [] };
  const rows = await loadList();
  const { found, missing } = await resolveProducts(rows);
  report.resolved = Object.keys(found).length;
  report.missingHandles = missing;

  const coll = await getCollection();
  if (!coll) throw new Error(`No Best Sellers collection found (tried "best-sellers" and "${BS_HANDLE}").`);
  report.collection = { id: coll.id, title: coll.title, smart: !!coll.ruleSet };

  const ranked = rows.filter(r => found[r.handle])
                     .sort((a, b) => a.rank - b.rank)
                     .map(r => ({ ...r, id: found[r.handle].id }));

  if (coll.ruleSet) {
    const tagRule = (coll.ruleSet.rules || []).find(r => r.column === "TAG" && r.relation === "EQUALS");
    if (!tagRule) throw new Error("Smart Best Sellers collection has no TAG rule -- convert it to manual or add a tag rule, then re-run.");
    report.steps.push(`smart collection -> tagging '${tagRule.condition}'`);
    if (!opts.dryRun) {
      for (const r of ranked) {
        await gql(`mutation($id: ID!, $tags: [String!]!) { tagsAdd(id: $id, tags: $tags) { userErrors { message } } }`,
          { id: r.id, tags: [tagRule.condition, BS_TAG] });
      }
    }
  } else {
    const existing = new Set(coll.products.edges.map(e => e.node.id));
    const toAdd = ranked.filter(r => !existing.has(r.id)).map(r => r.id);
    const listed = new Set(ranked.map(r => r.id));
    const extras = [...existing].filter(id => !listed.has(id));
    report.toAdd = toAdd.length; report.extrasInCollection = extras.length;

    if (!opts.dryRun) {
      for (let i = 0; i < toAdd.length; i += 50) {
        const d = await gql(`mutation($id: ID!, $pids: [ID!]!) {
          collectionAddProducts(id: $id, productIds: $pids) { userErrors { message } } }`,
          { id: coll.id, pids: toAdd.slice(i, i + 50) });
        const ue = d.collectionAddProducts.userErrors;
        if (ue.length) report.steps.push("add error: " + ue[0].message);
      }
      if (opts.removeExtras && extras.length) {
        const d = await gql(`mutation($id: ID!, $pids: [ID!]!) {
          collectionRemoveProducts(id: $id, productIds: $pids) { userErrors { message } } }`,
          { id: coll.id, pids: extras });
        const ue = (d.collectionRemoveProducts.userErrors || []);
        if (ue.length) report.steps.push("remove error: " + ue[0].message);
        else report.steps.push(`removed ${extras.length} stale products`);
      }
      if (coll.sortOrder !== "MANUAL") {
        await gql(`mutation($input: CollectionInput!) { collectionUpdate(input: $input) {
          userErrors { message } } }`, { input: { id: coll.id, sortOrder: "MANUAL" } });
        report.steps.push("sortOrder -> MANUAL");
      }
      const moves = ranked.map((r, idx) => ({ id: r.id, newPosition: String(idx) }));
      for (let i = 0; i < moves.length; i += 100) {
        const d = await gql(`mutation($id: ID!, $moves: [MoveInput!]!) {
          collectionReorderProducts(id: $id, moves: $moves) { userErrors { message } job { id } } }`,
          { id: coll.id, moves: moves.slice(i, i + 100) });
        const ue = d.collectionReorderProducts.userErrors;
        if (ue.length) { report.steps.push("reorder error: " + ue[0].message); break; }
      }
      report.steps.push(`reordered ${moves.length} products by sales rank`);
    }
  }

  if (!opts.dryRun) {
    for (let i = 0; i < ranked.length; i += 25) {
      const batch = ranked.slice(i, i + 25);
      const metafields = batch.map(r => ({
        ownerId: r.id, namespace: "brites", key: "bs_rank",
        type: "number_integer", value: String(r.rank)
      }));
      await gql(`mutation($metafields: [MetafieldsSetInput!]!) {
        metafieldsSet(metafields: $metafields) { userErrors { field message } } }`, { metafields });
      for (const r of batch) {
        await gql(`mutation($id: ID!, $tags: [String!]!) { tagsAdd(id: $id, tags: $tags) { userErrors { message } } }`,
          { id: r.id, tags: [BS_TAG] });
      }
    }
    report.steps.push("tagged + rank metafields written");
  }

  const f = fb();
  if (f && !opts.dryRun) {
    try {
      await f.db.collection("Brites_Editor_Meta").doc("bestSellersSyncReport")
        .set(Object.assign({}, report, { finishedAt: new Date().toISOString() }));
    } catch (e) {}
  }
  return report;
}

exports.handler = async function (event) {
  const headers = {
    "Access-Control-Allow-Origin": "https://britesjewelry.com",
    "Access-Control-Allow-Headers": "Content-Type, X-Edit-Passcode",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Content-Type": "application/json"
  };
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers };

  const scheduled = !!(event.headers && (event.headers["x-nf-event"] === "schedule" || event.isScheduled));
  // Browser trigger: opening the URL with ?run=now runs immediately (owner request).
  const q = event.queryStringParameters || {};
  if (event.httpMethod === "GET" && q.run === "now") {
    try {
      const out = await runSync({});
      return { statusCode: 200, headers, body: JSON.stringify(out, null, 2) };
    } catch (e) {
      return { statusCode: 500, headers, body: JSON.stringify({ ok: false, error: String(e.message || e) }) };
    }
  }

  const pass = (event.headers && (event.headers["x-edit-passcode"] || event.headers["X-Edit-Passcode"])) || "";
  if (!scheduled && pass !== process.env.EDIT_PASSCODE) {
    return { statusCode: 401, headers, body: JSON.stringify({ error: "bad passcode" }) };
  }

  try {
    let opts = { dryRun: true, removeExtras: false };
    if (scheduled) opts = { dryRun: false, removeExtras: false };
    else if (event.httpMethod === "POST") {
      const b = JSON.parse(event.body || "{}");
      if (b.action === "sync") opts = { dryRun: false, removeExtras: !!b.removeExtras };
    }
    const report = await runSync(opts);
    return { statusCode: 200, headers, body: JSON.stringify({ ok: true, dryRun: opts.dryRun, report }, null, 1) };
  } catch (e) {
    return { statusCode: 500, headers, body: JSON.stringify({ ok: false, error: String(e.message || e) }) };
  }
};
