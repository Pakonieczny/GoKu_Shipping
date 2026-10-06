#!/usr/bin/env node
/* Builds charm-nest-emoji-data.js, the list behind the Emoji button of the Back engraving words box.
 *
 *   node scripts/build-emoji-picker-data.cjs                 write charm-nest-emoji-data.js (and check it)
 *   node scripts/build-emoji-picker-data.cjs --check         build in memory, fail if the committed file differs
 *   node scripts/build-emoji-picker-data.cjs --report FILE   also write the build report (dropped items, shared shapes)
 *   node scripts/build-emoji-picker-data.cjs --refresh-names rewrite scripts/emoji-codepoint-names.json first
 *                                                            (needs python3 and perl; the committed snapshot is enough otherwise)
 *
 * What is offered: every emoji the laser engraver can draw and nothing else.
 *   - the key must be in vendor/fonts/emoji-sequences.json `sequences` and not in `unsupported`;
 *   - the key must be a fully qualified emoji (Unicode RGI_Emoji, emoji-test 17.0): keys without their emoji selector are the
 *     same emoji typed loosely, so the picker offers the fully qualified key only (it is the key the map holds for it);
 *   - the Component group (the five skin-tone swatches and the four hair pieces) is not an emoji on its own and is not offered;
 *   - every offered key, and every tone variant, is then run through the REAL engraver path: the two fonts are loaded with
 *     opentype.js the way charm-nest-bridge.js loadFonts does (hash of the emoji font included), CharmNestText.withEmoji wraps
 *     SourceSans3-Regular, and CharmNestGeom.glyphCoverage plus a real outline are required. Anything that fails is dropped
 *     and listed in the report.
 * Groups and their order come from the order of the keys in emoji-sequences.json (emoji-test order) at the known group anchors.
 * Skin tones: a person/hand emoji carries one template string (\u0001 stands for the tone) that expands to the five tone
 * variants of the map; the tone variants are never cells of their own. Couples and hand-holders also carry a two-slot
 * template (\u0001 and \u0002) for the mixed-tone pairs the map holds (CNEmojiData.pair).
 * One cell per outline: the monochrome font draws man/woman/person variants, hair colours, jobs and family compositions with
 * exactly the same ink, so cells with an identical outline fold into the neutral one (the folded keys stay in `s`, their names
 * become search words). Flags are never folded. The build proves nothing the laser can cut is lost (report.uncovered is empty).
 * Names: Python unicodedata (Unicode 14) plus Perl charnames (Unicode 15.0) for single code points, Intl.DisplayNames for
 * flags, composed from parts for ZWJ sequences, and the CURATED table below (everyday names and search words). Nothing in
 * the output depends on the date or machine: the same inputs give byte-identical output. */
'use strict';
const fs = require('node:fs'), path = require('node:path'), crypto = require('node:crypto'), cp = require('node:child_process');

const ROOT = path.resolve(__dirname, '..');
const OUT = path.join(ROOT, 'charm-nest-emoji-data.js');
const MAP_FILE = path.join(ROOT, 'vendor/fonts/emoji-sequences.json');
const NAMES_FILE = path.join(__dirname, 'emoji-codepoint-names.json');
const DATA_VERSION = '2026-10-06';

/* ── tables ─────────────────────────────────────────────────────────────────────────────────────────────────────── */
const GROUPS = [
  // id, name shown, tab icon, first key of the group in emoji-test order
  ['smileys', 'Smileys', '😀', '😀'], ['people', 'People', '👋', '👋'], ['component', null, null, '\u{1F3FB}'],
  ['animals', 'Animals and nature', '🐱', '🐵'], ['food', 'Food and drink', '🍎', '🍇'], ['travel', 'Travel', '🚗', '🌍'],
  ['activities', 'Activities', '⚽', '🎃'], ['objects', 'Objects', '💡', '👓'], ['symbols', 'Symbols', '\u267B\uFE0F', '🏧'], ['flags', 'Flags', '🏁', '🏁']
];
const TONES = [['', 'Default', '✋'], ['\u{1F3FB}', 'Light', '✋\u{1F3FB}'], ['\u{1F3FC}', 'Medium-light', '✋\u{1F3FC}'], ['\u{1F3FD}', 'Medium', '✋\u{1F3FD}'], ['\u{1F3FE}', 'Medium-dark', '✋\u{1F3FE}'], ['\u{1F3FF}', 'Dark', '✋\u{1F3FF}']];

// Mixed-tone templates whose neutral form is not itself an emoji: the cell they hang under (code points, fully qualified).
const MIXED_BASE = {
  '1faf1 200d 1faf2': '1f91d', '1f9d1 200d 1f430 200d 1f9d1': '1f46f', '1f468 200d 1f430 200d 1f468': '1f46f 200d 2642 fe0f',
  '1f469 200d 1f430 200d 1f469': '1f46f 200d 2640 fe0f', '1f469 200d 1f91d 200d 1f469': '1f46d', '1f469 200d 1f91d 200d 1f468': '1f46b',
  '1f468 200d 1f91d 200d 1f468': '1f46c', '1f9d1 200d 2764 200d 1f9d1': '1f491', '1f9d1 200d 2764 200d 1f48b 200d 1f9d1': '1f48f'
};

// Region flags: other names people search for (the name itself comes from Intl.DisplayNames).
const FLAG_WORDS = {
  US: 'usa america united states of america', GB: 'uk britain great britain', AE: 'uae emirates', NL: 'holland', CZ: 'czech republic', CI: 'ivory coast',
  TR: 'turkey', MM: 'burma', KR: 'korea', KP: 'korea', CD: 'drc', CV: 'cabo verde', SZ: 'swaziland', MK: 'macedonia', TL: 'east timor', VA: 'holy see',
  PS: 'palestine', RU: 'russian federation', TW: 'republic of china', HK: 'hong kong', MO: 'macau', LA: 'lao', IR: 'persia', FK: 'malvinas', BQ: 'bonaire',
  SH: 'saint helena', KN: 'saint kitts', LC: 'saint lucia', VC: 'saint vincent', PM: 'saint pierre', MF: 'saint martin', BL: 'saint barthelemy', SX: 'saint maarten',
  GS: 'south georgia', UM: 'outlying islands', EU: 'europe', UN: 'un'
};
const SUBDIVISIONS = { gbeng: ['England', 'gb-eng uk britain'], gbsct: ['Scotland', 'gb-sct uk britain'], gbwls: ['Wales', 'gb-wls uk britain'] };

// Everyday names (CLDR style, short) and search words. `emoji | name | search words`; an empty name keeps the Unicode name.
// Emoji selectors are ignored on both sides. Words already in the name need not be repeated.
const CURATED = `
😀 | grinning face | smile happy joy
😃 | grinning face with big eyes | smile happy
😄 | grinning face with smiling eyes | smile happy
😁 | beaming face with smiling eyes | smile grin
😆 | grinning squinting face | laugh lol
😅 | grinning face with sweat | relief phew
🤣 | rolling on the floor laughing | rofl lol funny
😂 | face with tears of joy | lol laugh funny
🙂 | slightly smiling face | smile
🙃 | upside-down face | silly
🫠 | melting face |
😉 | winking face | flirt
😊 | smiling face with smiling eyes | smile blush happy
😇 | smiling face with halo | angel innocent
🥰 | smiling face with hearts | love crush adore
😍 | smiling face with heart-eyes | love crush
🤩 | star-struck | wow amazing
😘 | face blowing a kiss | kiss love
😗 | kissing face | kiss
☺ | smiling face | smile happy
😚 | kissing face with closed eyes | kiss
😙 | kissing face with smiling eyes | kiss
🥲 | smiling face with tear | touched grateful
😋 | face savoring food | yum tasty delicious
😛 | face with tongue |
😜 | winking face with tongue | silly
🤪 | zany face | crazy wild
😝 | squinting face with tongue |
🤑 | money-mouth face | rich dollar
🤗 | smiling face with open hands | hug
🤭 | face with hand over mouth | oops giggle
🫢 | face with open eyes and hand over mouth |
🫣 | face with peeking eye |
🤫 | shushing face | quiet secret
🤔 | thinking face | hmm
🫡 | saluting face |
🤐 | zipper-mouth face | secret
🤨 | face with raised eyebrow | skeptical
😶 | face without mouth | silent
🙄 | face with rolling eyes | eyeroll
🤥 | lying face | liar pinocchio
😴 | sleeping face | sleep zzz
😷 | face with medical mask | sick
🤒 | face with thermometer | sick ill
🤕 | face with head-bandage | hurt injured
🤢 | nauseated face | sick
🤮 | face vomiting | sick
🥵 | hot face |
🥶 | cold face | freezing
🥴 | woozy face | drunk
😵 | face with crossed-out eyes | dizzy
🤯 | exploding head | mind blown
🤠 | cowboy hat face |
🥳 | partying face | celebrate
😎 | smiling face with sunglasses | cool
🤓 | nerd face | geek
☹ | frowning face | sad
😮 | face with open mouth | surprised
😳 | flushed face | embarrassed
🥺 | pleading face | puppy eyes
😨 | fearful face | scared
😰 | anxious face with sweat |
😥 | sad but relieved face |
😢 | crying face | sad tear
😭 | loudly crying face | sob sad
😱 | face screaming in fear | scared omg
😓 | downcast face with sweat |
🥱 | yawning face | sleepy bored
😤 | face with steam from nose | angry
😡 | enraged face | angry mad rage
😠 | angry face | mad
🤬 | face with symbols on mouth | swearing cursing
😈 | smiling face with horns | devil
👿 | angry face with horns | devil
💀 | skull | dead
☠ | skull and crossbones | pirate danger
💩 | pile of poo | poop
👹 | ogre |
👺 | goblin |
👻 | ghost | halloween
👽 | alien | ufo
👾 | alien monster | game
🤖 | robot |
😺 | grinning cat |
😸 | grinning cat with smiling eyes |
😹 | cat with tears of joy |
😻 | smiling cat with heart-eyes | love
😼 | cat with wry smile |
😽 | kissing cat |
🙀 | weary cat |
😿 | crying cat |
😾 | pouting cat |
💌 | love letter |
💘 | heart with arrow | cupid love
💝 | heart with ribbon | gift love
💖 | sparkling heart | love
💗 | growing heart | love
💓 | beating heart | love
💞 | revolving hearts | love
💕 | two hearts | love
💟 | heart decoration | love
❣ | heart exclamation | love
💔 | broken heart | heartbreak sad love
❤ | red heart | love valentine
🩷 | pink heart | love
🧡 | orange heart | love
💛 | yellow heart | love
💚 | green heart | love
💙 | blue heart | love
🩵 | light blue heart | love
💜 | purple heart | love
🤎 | brown heart | love
🖤 | black heart | love
🩶 | grey heart | gray love
🤍 | white heart | love
💋 | kiss mark | lips kiss
💯 | hundred points | 100 perfect score
💥 | collision | boom explosion
💫 | dizzy | stars
💦 | sweat droplets | water splash
💨 | dashing away | wind fast
💬 | speech balloon | chat message
🗨 | left speech bubble |
🗯 | right anger bubble |
💭 | thought balloon | thinking
💤 | zzz | sleep
👋 | waving hand | hello hi goodbye wave
🖐 | hand with fingers splayed | five
✋ | raised hand | stop high five
🖖 | vulcan salute | spock
👌 | OK hand | ok perfect
🤌 | pinched fingers | italian
🤏 | pinching hand | small
✌ | victory hand | peace
🤞 | crossed fingers | luck hope
🤟 | love-you gesture |
🤘 | sign of the horns | rock
👈 | backhand index pointing left |
👉 | backhand index pointing right |
👆 | backhand index pointing up |
🖕 | middle finger |
👇 | backhand index pointing down |
☝ | index pointing up |
👍 | thumbs up | like yes good approve
👎 | thumbs down | dislike no bad
👊 | oncoming fist | punch
👏 | clapping hands | applause bravo
🙌 | raising hands | celebrate hooray
🫶 | heart hands | love
🤝 | handshake | deal agreement
🙏 | folded hands | pray please thanks
💪 | flexed biceps | strong muscle
👀 | eyes | look
👄 | mouth | lips
🧑 | person |
👱 | person: blond hair |
🧔 | person: beard |
🧓 | older person |
👴 | old man |
👵 | old woman |
🙍 | person frowning |
🙎 | person pouting |
🙅 | person gesturing no |
🙆 | person gesturing ok |
💁 | person tipping hand |
🙋 | person raising hand |
🙇 | person bowing |
🤦 | person facepalming |
🤷 | person shrugging |
👮 | police officer | cop
🕵 | detective | spy
💂 | guard |
👳 | person wearing turban |
👲 | person with skullcap |
🧕 | woman with headscarf |
🤵 | person in tuxedo |
👰 | person with veil | bride wedding
🎅 | santa claus | christmas
🤶 | mrs. claus | christmas
🧙 | mage | wizard witch
🧜 | merperson | mermaid merman
💆 | person getting massage |
💇 | person getting haircut |
🚶 | person walking |
🧍 | person standing |
🧎 | person kneeling |
🏃 | person running |
💃 | woman dancing |
🕺 | man dancing |
🕴 | person in suit levitating |
👯 | people with bunny ears |
🧖 | person in steamy room | sauna
🧗 | person climbing |
🤺 | person fencing |
🏌 | person golfing |
🏄 | person surfing |
🚣 | person rowing boat |
🏊 | person swimming |
⛹ | person bouncing ball |
🏋 | person lifting weights |
🚴 | person biking |
🚵 | person mountain biking |
🤸 | person cartwheeling |
🤼 | people wrestling |
🤽 | person playing water polo |
🤾 | person playing handball |
🤹 | person juggling |
🧘 | person in lotus position | yoga meditate
🛀 | person taking bath |
🛌 | person in bed |
👭 | women holding hands |
👫 | woman and man holding hands |
👬 | men holding hands |
🗣 | speaking head |
🐶 | dog face | puppy pet
🐕 | dog | puppy pet
🐱 | cat face | kitten pet
🐈 | cat | kitten pet
🐺 | wolf |
🦊 | fox |
🦁 | lion |
🐴 | horse face |
🦄 | unicorn | magic
🦓 | zebra |
🐮 | cow face |
🐷 | pig face |
🐭 | mouse face |
🐹 | hamster |
🐰 | rabbit face | bunny
🐻 | bear |
🐼 | panda |
🐸 | frog |
🐲 | dragon face |
🦒 | giraffe |
🕊 | dove | peace
🦖 | t-rex | dinosaur
🦕 | sauropod | dinosaur
🐝 | honeybee | bee insect
🐞 | lady beetle | ladybug insect
🦋 | butterfly | insect
🐛 | bug | insect
🐢 | turtle |
🐍 | snake |
🐙 | octopus |
🐠 | tropical fish |
🐬 | dolphin |
🐳 | spouting whale |
🦅 | eagle | bird
🦆 | duck | bird
🦉 | owl | bird
🐧 | penguin | bird
🌹 | rose | flower love
🌸 | cherry blossom | flower spring
🌻 | sunflower | flower
🌷 | tulip | flower
🌲 | evergreen tree | pine
🌳 | deciduous tree |
🌴 | palm tree |
🍀 | four leaf clover | luck
🍁 | maple leaf | canada autumn
🍄 | mushroom |
🍎 | red apple | fruit
🍏 | green apple | fruit
🍌 | banana | fruit
🍓 | strawberry | fruit
🍒 | cherries | fruit
🍆 | eggplant | aubergine
🌽 | corn | maize
🥑 | avocado |
🍞 | bread |
🧀 | cheese wedge |
🍔 | hamburger | burger food
🍟 | french fries | chips food
🍕 | pizza | slice food
🌭 | hot dog |
🌮 | taco |
🍳 | cooking | egg
🍜 | steaming bowl | ramen noodles
🍣 | sushi |
🍦 | soft ice cream |
🍩 | doughnut | donut
🍪 | cookie |
🎂 | birthday cake |
🍰 | shortcake | cake dessert
🍫 | chocolate bar |
🍬 | candy | sweet
🍭 | lollipop |
🍯 | honey pot |
🥛 | glass of milk |
☕ | hot beverage | coffee tea
🍵 | teacup without handle | green tea
🍾 | bottle with popping cork | champagne
🍷 | wine glass |
🍸 | cocktail glass |
🍹 | tropical drink |
🍺 | beer mug |
🍻 | clinking beer mugs | cheers
🥂 | clinking glasses | cheers
🥃 | tumbler glass | whiskey
🥤 | cup with straw |
🍽 | fork and knife with plate |
🔪 | kitchen knife |
🌍 | globe showing Europe-Africa | earth world
🌎 | globe showing Americas | earth world
🌏 | globe showing Asia-Australia | earth world
🗾 | map of Japan |
🏔 | snow-capped mountain |
🏠 | house | home
🏡 | house with garden | home
🏢 | office building |
🏣 | Japanese post office |
🏤 | post office |
🏰 | castle |
🏯 | Japanese castle |
🗼 | Tokyo tower |
🗽 | Statue of Liberty |
🕋 | Kaaba |
⛩ | shinto shrine |
🌃 | night with stars |
🌅 | sunrise |
🌇 | sunset |
🚂 | locomotive | train
🚄 | high-speed train |
🚅 | bullet train |
🚑 | ambulance |
🚒 | fire engine |
🚓 | police car |
🚕 | taxi | cab
🚗 | car | automobile
🚘 | oncoming automobile | car
🚙 | sport utility vehicle | suv car
🚚 | delivery truck |
🏎 | racing car |
🏍 | motorcycle |
🛴 | kick scooter |
🚲 | bicycle | bike
🚨 | police car light | siren
🛑 | stop sign |
🚧 | construction |
⚓ | anchor |
⛵ | sailboat |
🚢 | ship |
✈ | airplane | plane flight travel
🛫 | airplane departure |
🛬 | airplane arrival |
🚁 | helicopter |
🚀 | rocket | space launch
🛸 | flying saucer | ufo
🧳 | luggage |
⌛ | hourglass done |
⏳ | hourglass not done |
⌚ | watch |
⏰ | alarm clock |
🌑 | new moon |
🌓 | first quarter moon |
🌕 | full moon |
🌗 | last quarter moon |
🌙 | crescent moon | night
🌚 | new moon face |
🌛 | first quarter moon face |
🌜 | last quarter moon face |
☀ | sun | sunny weather
🌝 | full moon face |
🌞 | sun with face |
⭐ | star | favorite
🌟 | glowing star |
🌠 | shooting star | wish
🌌 | milky way |
☁ | cloud | weather
⛅ | sun behind cloud | weather
⛈ | cloud with lightning and rain | weather storm
🌤 | sun behind small cloud | weather
🌥 | sun behind large cloud | weather
🌦 | sun behind rain cloud | weather
🌧 | cloud with rain | weather
🌨 | cloud with snow | weather
🌩 | cloud with lightning | weather
🌪 | tornado | weather
🌬 | wind face |
🌈 | rainbow |
☂ | umbrella |
☔ | umbrella with rain drops |
⚡ | high voltage | lightning bolt
❄ | snowflake | winter
☃ | snowman | winter
⛄ | snowman without snow | winter
🔥 | fire | hot flame lit
💧 | droplet | water
🌊 | water wave | ocean sea
🎃 | jack-o-lantern | halloween pumpkin
🎄 | Christmas tree | holiday
🎆 | fireworks | celebrate
🎇 | sparkler |
✨ | sparkles | shine magic
🎈 | balloon | party
🎉 | party popper | party celebrate tada confetti
🎊 | confetti ball | party celebrate
🎀 | ribbon | bow
🎁 | wrapped gift | present
🏆 | trophy | winner prize
🏅 | sports medal | prize
🥇 | 1st place medal | gold winner
🥈 | 2nd place medal | silver
🥉 | 3rd place medal | bronze
⚽ | soccer ball | football sport
⚾ | baseball | sport
🏀 | basketball | sport
🏐 | volleyball | sport
🏈 | american football | sport
🎾 | tennis | sport
🎳 | bowling |
🏓 | ping pong | table tennis
🥊 | boxing glove |
🎯 | bullseye | target dart
🔫 | water pistol |
🎱 | pool 8 ball | billiards
🔮 | crystal ball |
🎮 | video game | controller
🎰 | slot machine | casino
🎲 | game die | dice
🧩 | puzzle piece | jigsaw
🧸 | teddy bear | toy
♠ | spade suit | cards
♥ | heart suit | cards love
♦ | diamond suit | cards
♣ | club suit | cards
🃏 | joker | cards
🎭 | performing arts | theater theatre
🖼 | framed picture |
🎨 | artist palette | paint art
🧵 | thread |
🧶 | yarn | knit
👓 | glasses | eyeglasses
🕶 | sunglasses |
👔 | necktie |
👕 | t-shirt | shirt
👖 | jeans |
🧣 | scarf |
🧤 | gloves |
🧥 | coat |
👗 | dress |
👙 | bikini |
👚 | woman’s clothes |
👛 | purse |
👜 | handbag |
👝 | clutch bag |
🎒 | backpack |
👞 | man’s shoe |
👟 | running shoe | sneaker
👠 | high-heeled shoe |
👡 | woman’s sandal |
👢 | woman’s boot |
👑 | crown | king queen royal
👒 | woman’s hat |
🎩 | top hat |
🎓 | graduation cap |
⛑ | rescue worker’s helmet |
💄 | lipstick | makeup
💍 | ring | diamond engagement wedding
💎 | gem stone | diamond jewel
🔇 | muted speaker |
🔈 | speaker low volume |
🔉 | speaker medium volume |
🔊 | speaker high volume |
📢 | loudspeaker |
📣 | megaphone |
🔔 | bell | notification
🔕 | bell with slash |
🎵 | musical note | music song
🎶 | musical notes | music song
🎤 | microphone | karaoke
🎧 | headphone | music
🎸 | guitar | music
🎹 | musical keyboard | piano music
🎻 | violin | music
🥁 | drum | music
📱 | mobile phone | cell smartphone
📲 | mobile phone with arrow |
☎ | telephone | phone
📞 | telephone receiver | phone call
🔋 | battery |
💻 | laptop | computer
🖥 | desktop computer |
🖨 | printer |
🖱 | computer mouse |
💽 | computer disk |
💾 | floppy disk | save
💿 | optical disk | cd
🎥 | movie camera | film
🎬 | clapper board | movie film
📺 | television | tv
📷 | camera | photo
📸 | camera with flash | photo
📹 | video camera |
🔍 | magnifying glass tilted left | search zoom
🔎 | magnifying glass tilted right | search zoom
🕯 | candle |
💡 | light bulb | idea
🔦 | flashlight | torch
🏮 | red paper lantern |
📕 | closed book |
📖 | open book | read
📚 | books | read
📜 | scroll |
📰 | newspaper | news
🔖 | bookmark |
🏷 | label | tag
💰 | money bag | cash
💴 | yen banknote | money
💵 | dollar banknote | money cash
💶 | euro banknote | money
💷 | pound banknote | money
💸 | money with wings |
💳 | credit card |
✉ | envelope | mail email letter
📧 | e-mail | email
📨 | incoming envelope |
📩 | envelope with arrow |
📤 | outbox tray |
📥 | inbox tray |
📦 | package | parcel box
📮 | postbox | mail
✏ | pencil |
✒ | black nib | pen
🖋 | fountain pen |
🖊 | pen |
🖌 | paintbrush |
🖍 | crayon |
📝 | memo | note
💼 | briefcase |
📁 | file folder |
📂 | open file folder |
📅 | calendar | date
📆 | tear-off calendar |
🗒 | spiral notepad |
🗓 | spiral calendar |
📈 | chart increasing | graph up
📉 | chart decreasing | graph down
📊 | bar chart | graph
📋 | clipboard |
📌 | pushpin | pin
📍 | round pushpin | pin location
📎 | paperclip |
📏 | straight ruler |
📐 | triangular ruler |
✂ | scissors | cut
🗑 | wastebasket | trash
🔒 | locked | lock padlock
🔓 | unlocked |
🔏 | locked with pen |
🔐 | locked with key |
🔑 | key |
🗝 | old key |
🔨 | hammer |
🪓 | axe |
⛏ | pick |
⚒ | hammer and pick |
🛠 | hammer and wrench |
🗡 | dagger |
⚔ | crossed swords |
💣 | bomb |
🏹 | bow and arrow |
🛡 | shield |
🔧 | wrench |
🔩 | nut and bolt |
⚙ | gear | settings
🗜 | clamp |
⚖ | balance scale | justice
🦯 | white cane |
🔗 | link | chain
⛓ | chains |
🧰 | toolbox |
🧲 | magnet |
🧪 | test tube |
🧬 | dna |
🔬 | microscope |
🔭 | telescope |
💉 | syringe |
💊 | pill | medicine
🩹 | adhesive bandage |
🩺 | stethoscope |
🚪 | door |
🛏 | bed |
🛋 | couch and lamp | sofa
🚽 | toilet |
🚿 | shower |
🛁 | bathtub |
🧴 | lotion bottle |
🧹 | broom |
🧺 | basket |
🧻 | roll of paper |
🧼 | soap |
🧽 | sponge |
🧯 | fire extinguisher |
🛒 | shopping cart |
🚬 | cigarette | smoking
⚰ | coffin |
⚱ | funeral urn |
🗿 | moai |
🏧 | ATM sign | bank cash
🚮 | litter in bin sign |
🚰 | potable water |
♿ | wheelchair symbol | accessible
🚹 | men’s room |
🚺 | women’s room |
⚠ | warning | caution
⛔ | no entry |
🚫 | prohibited | forbidden
🚭 | no smoking |
🚯 | no littering |
🚱 | non-potable water |
🚷 | no pedestrians |
📵 | no mobile phones |
🔞 | no one under eighteen |
☢ | radioactive |
☣ | biohazard |
⬆ | up arrow |
↗ | up-right arrow |
➡ | right arrow |
↘ | down-right arrow |
⬇ | down arrow |
↙ | down-left arrow |
⬅ | left arrow |
↖ | up-left arrow |
↕ | up-down arrow |
↔ | left-right arrow |
↩ | right arrow curving left |
↪ | left arrow curving right |
⤴ | right arrow curving up |
⤵ | right arrow curving down |
🔃 | clockwise vertical arrows |
🔄 | counterclockwise arrows button |
🔙 | BACK arrow |
🔚 | END arrow |
🔛 | ON! arrow |
🔜 | SOON arrow |
🔝 | TOP arrow |
🛐 | place of worship |
⚛ | atom symbol |
🕉 | om | hindu
✡ | star of David | jewish
☸ | wheel of dharma | buddhist
☯ | yin yang |
✝ | latin cross | christian church
☦ | orthodox cross | christian
☪ | star and crescent | islam muslim
☮ | peace symbol |
🕎 | menorah | jewish hanukkah
🔯 | dotted six-pointed star |
♈ | Aries | zodiac
♉ | Taurus | zodiac
♊ | Gemini | zodiac
♋ | Cancer | zodiac
♌ | Leo | zodiac
♍ | Virgo | zodiac
♎ | Libra | zodiac
♏ | Scorpio | zodiac scorpius
♐ | Sagittarius | zodiac
♑ | Capricorn | zodiac
♒ | Aquarius | zodiac
♓ | Pisces | zodiac
⛎ | Ophiuchus | zodiac
🔀 | shuffle tracks button |
🔁 | repeat button |
🔂 | repeat single button |
▶ | play button |
⏩ | fast-forward button |
⏭ | next track button |
⏯ | play or pause button |
◀ | reverse button |
⏪ | fast reverse button |
⏮ | last track button |
🔼 | upwards button |
⏫ | fast up button |
🔽 | downwards button |
⏬ | fast down button |
⏸ | pause button |
⏹ | stop button |
⏺ | record button |
⏏ | eject button |
🔅 | dim button |
🔆 | bright button |
📶 | antenna bars | signal
📳 | vibration mode |
📴 | mobile phone off |
♀ | female sign | woman
♂ | male sign | man
⚧ | transgender symbol |
✖ | multiply | times x
➕ | plus | add
➖ | minus | subtract
➗ | divide | division
♾ | infinity | forever
‼ | double exclamation mark |
⁉ | exclamation question mark |
❓ | red question mark |
❔ | white question mark |
❕ | white exclamation mark |
❗ | red exclamation mark |
〰 | wavy dash |
💱 | currency exchange |
💲 | heavy dollar sign |
⚕ | medical symbol |
♻ | recycling symbol | recycle
⚜ | fleur-de-lis |
🔱 | trident emblem |
📛 | name badge |
🔰 | Japanese symbol for beginner |
⭕ | hollow red circle |
✅ | check mark button | yes done tick
☑ | check box with check |
✔ | check mark | yes tick
❌ | cross mark | no x wrong
❎ | cross mark button |
➰ | curly loop |
➿ | double curly loop |
〽 | part alternation mark |
✳ | eight-spoked asterisk |
✴ | eight-pointed star |
❇ | sparkle |
© | copyright |
® | registered |
™ | trade mark | trademark
🔟 | keycap 10 | number ten
🔠 | input latin uppercase | abc
🔡 | input latin lowercase | abc
🔢 | input numbers | 123
🔣 | input symbols |
🔤 | input latin letters | abc
🅰 | A button (blood type) |
🆎 | AB button (blood type) |
🅱 | B button (blood type) |
🆑 | CL button |
🆒 | COOL button |
🆓 | FREE button |
ℹ | information |
🆔 | ID button |
Ⓜ | circled M |
🆕 | NEW button |
🆖 | NG button |
🅾 | O button (blood type) |
🆗 | OK button |
🅿 | P button | parking
🆘 | SOS button | help
🆙 | UP! button |
🆚 | VS button | versus
🈁 | Japanese “here” button |
🈂 | Japanese “service charge” button |
🈷 | Japanese “monthly amount” button |
🈶 | Japanese “not free of charge” button |
🈯 | Japanese “reserved” button |
🉐 | Japanese “bargain” button |
🈹 | Japanese “discount” button |
🈚 | Japanese “free of charge” button |
🈲 | Japanese “prohibited” button |
🉑 | Japanese “acceptable” button |
🈸 | Japanese “application” button |
🈴 | Japanese “passing grade” button |
🈳 | Japanese “vacancy” button |
㊗ | Japanese “congratulations” button |
㊙ | Japanese “secret” button |
🈺 | Japanese “open for business” button |
🈵 | Japanese “no vacancy” button |
🔴 | red circle | color colour
🟠 | orange circle | color colour
🟡 | yellow circle | color colour
🟢 | green circle | color colour
🔵 | blue circle | color colour
🟣 | purple circle | color colour
🟤 | brown circle | color colour
⚫ | black circle | color colour
⚪ | white circle | color colour
🟥 | red square | color colour
🟧 | orange square | color colour
🟨 | yellow square | color colour
🟩 | green square | color colour
🟦 | blue square | color colour
🟪 | purple square | color colour
🟫 | brown square | color colour
⬛ | black large square |
⬜ | white large square |
◼ | black medium square |
◻ | white medium square |
◾ | black medium-small square |
◽ | white medium-small square |
▪ | black small square |
▫ | white small square |
🔶 | large orange diamond |
🔷 | large blue diamond |
🔸 | small orange diamond |
🔹 | small blue diamond |
🔺 | red triangle pointed up |
🔻 | red triangle pointed down |
💠 | diamond with a dot |
🔘 | radio button |
🔳 | white square button |
🔲 | black square button |
🏁 | chequered flag | race finish
🚩 | triangular flag |
🎌 | crossed flags |
🏴 | black flag |
🏳 | white flag | surrender
`;

// Names and words for sequences that are not composed by the rules below (code points, fully qualified).
const CURATED_SEQ = [
  ['1f636 200d 1f32b fe0f', 'face in clouds', 'fog'], ['1f62e 200d 1f4a8', 'face exhaling', 'sigh relief'], ['1f635 200d 1f4ab', 'face with spiral eyes', 'dizzy'],
  ['1f642 200d 2194 fe0f', 'head shaking horizontally', 'no'], ['1f642 200d 2195 fe0f', 'head shaking vertically', 'yes nod'],
  ['1f441 fe0f 200d 1f5e8 fe0f', 'eye in speech bubble', 'witness'], ['2764 fe0f 200d 1f525', 'heart on fire', 'love passion'], ['2764 fe0f 200d 1fa79', 'mending heart', 'love healing'],
  ['1f408 200d 2b1b', 'black cat', 'halloween pet'], ['1f415 200d 1f9ba', 'service dog', 'pet'], ['1f43b 200d 2744 fe0f', 'polar bear', 'arctic'],
  ['1f426 200d 2b1b', 'black bird', 'crow raven'], ['1f426 200d 1f525', 'phoenix', 'bird fire'], ['1f34b 200d 1f7e9', 'lime', 'fruit'], ['1f344 200d 1f7eb', 'brown mushroom', ''],
  ['26d3 fe0f 200d 1f4a5', 'broken chain', ''], ['1f3f3 fe0f 200d 1f308', 'rainbow flag', 'pride lgbt'], ['1f3f3 fe0f 200d 26a7 fe0f', 'transgender flag', 'pride'],
  ['1f3f4 200d 2620 fe0f', 'pirate flag', 'jolly roger'], ['1f9d1 200d 1f384', 'Mx Claus', 'christmas santa'], ['1f9d1 200d 1f91d 200d 1f9d1', 'people holding hands', 'friends']
];

// Search words by pattern on the final name (added to the words of the table; deduplicated).
const WORD_RULES = [
  [/heart/, 'love'], [/\bcat\b|kitten/, 'pet'], [/\bdog\b|puppy/, 'pet'], [/\bmoon\b/, 'night'], [/o'clock|o’clock|-thirty/, 'time clock'], [/\barrow\b/, 'direction'],
  [/medal|trophy/, 'prize award'], [/\bball\b/, 'sport'], [/\bphone\b|telephone/, 'call'], [/\bbook\b|\bbooks\b/, 'read'], [/flower|blossom|\bleaf\b|\bplant\b|\btree\b/, 'nature plant'],
  [/\bcake\b/, 'dessert sweet'], [/\bbird\b/, 'animal'], [/\bfish\b/, 'sea animal'], [/\bmonkey\b/, 'animal'], [/\bbus\b|\btrain\b|\btram\b|\bmetro\b/, 'transport'],
  [/\bboat\b|\bship\b|\bsailboat\b/, 'sea transport'], [/\bfamily\b/, 'parents kids'], [/\bkiss\b/, 'love'], [/couple/, 'love'], [/\bbaby\b/, 'infant'],
  [/\bman\b/, 'male'], [/\bwoman\b/, 'female']
];

/* ── helpers ────────────────────────────────────────────────────────────────────────────────────────────────────── */
const strip = s => s.replace(/\uFE0F/g, '');
const hexOf = ch => ch.codePointAt(0).toString(16);
const hexKey = s => [...s].map(hexOf).join(' ');
const fromHex = h => h.split(' ').map(x => String.fromCodePoint(parseInt(x, 16))).join('');
const isTone = ch => ch >= '\u{1F3FB}' && ch <= '\u{1F3FF}';
const hasTone = k => [...k].some(isTone);
const RGI = /^\p{RGI_Emoji}$/v;
const ZWJ = '\u200D', MALE = '\u2642', FEMALE = '\u2640', RIGHT = '\u27A1', HEART = '\u2764';

function loadNames(refresh) {
  if (refresh || !fs.existsSync(NAMES_FILE)) {
    const cps = new Set();
    const map = JSON.parse(fs.readFileSync(MAP_FILE, 'utf8'));
    for (const k of Object.keys(map.sequences)) if (RGI.test(k)) for (const ch of k) cps.add(hexOf(ch));
    const list = [...cps].sort((a, b) => parseInt(a, 16) - parseInt(b, 16));
    const py = "import sys, unicodedata\nfor h in sys.argv[1:]:\n    try: print(h + '\\t' + unicodedata.name(chr(int(h, 16))))\n    except ValueError: print(h + '\\t')\n";
    const names = {}, missing = [];
    for (const line of cp.execFileSync('python3', ['-I', '-c', py, ...list], { encoding: 'utf8' }).trim().split('\n')) { const [h, n] = line.split('\t'); if (n) names[h] = n; else missing.push(h); }
    const pl = 'use charnames (); for (@ARGV) { my $n = charnames::viacode(hex $_); print "$_\\t", (defined $n ? $n : ""), "\\n" }';
    for (const line of cp.execFileSync('perl', ['-e', pl, ...missing], { encoding: 'utf8' }).trim().split('\n')) { const [h, n] = line.split('\t'); if (n) names[h] = n; }
    const sorted = {}; for (const h of Object.keys(names).sort((a, b) => parseInt(a, 16) - parseInt(b, 16))) sorted[h] = names[h];
    const lines = Object.entries(sorted).map(([h, n]) => `  ${JSON.stringify(h)}: ${JSON.stringify(n)}`);
    fs.writeFileSync(NAMES_FILE, `{\n${lines.join(',\n')}\n}\n`);
  }
  return JSON.parse(fs.readFileSync(NAMES_FILE, 'utf8'));
}

/* ── names ──────────────────────────────────────────────────────────────────────────────────────────────────────── */
function makeNamer(names) {
  const cur = new Map();
  for (const line of CURATED.split('\n')) {
    if (!line.trim()) continue;
    const [e, n, k] = line.split('|').map(s => s.trim());
    cur.set(strip(e), { n, k: k || '' });
  }
  const seq = new Map(CURATED_SEQ.map(([h, n, k]) => [strip(fromHex(h)), { n, k }]));
  const regions = new Intl.DisplayNames(['en'], { type: 'region' });
  const cleanUnicode = name => {
    let n = name.toLowerCase();
    n = n.replace(/^clock face (\w+) oclock$/, "$1 o'clock").replace(/^clock face /, '');
    return n;
  };
  const baseName = ch => { const c = cur.get(strip(ch)); if (c && c.n) return c.n; const u = names[hexOf(ch)]; return u ? cleanUnicode(u) : ''; };
  const PERSON = { '🧑': 'person', '👨': 'man', '👩': 'woman' };
  const FAMILY = { '🧑': 'adult', '👨': 'man', '👩': 'woman', '🧒': 'child', '👦': 'boy', '👧': 'girl' };
  const PROF = { '\u2695': 'health worker', '🎓': 'student', '🏫': 'teacher', '\u2696': 'judge', '🌾': 'farmer', '🍳': 'cook', '🔧': 'mechanic', '🏭': 'factory worker', '💼': 'office worker', '🔬': 'scientist', '💻': 'technologist', '🎤': 'singer', '🎨': 'artist', '🩰': 'ballet dancer', '\u2708': 'pilot', '🚀': 'astronaut', '🚒': 'firefighter' };
  const PROP = { '🦯': 'with white cane', '🦼': 'in motorized wheelchair', '🦽': 'in manual wheelchair', '🍼': 'feeding baby' };
  const HAIR = { '🦰': 'red hair', '🦱': 'curly hair', '🦳': 'white hair', '🦲': 'bald' };
  const gender = (n, sign) => {
    const man = sign === MALE;
    if (n.startsWith('person')) return (man ? 'man' : 'woman') + n.slice(6);
    if (n.startsWith('people')) return (man ? 'men' : 'women') + n.slice(6);
    if (n === 'merperson') return man ? 'merman' : 'mermaid';
    if (n === 'deaf person') return man ? 'deaf man' : 'deaf woman';
    return (man ? 'man ' : 'woman ') + n;
  };
  function ofSequence(key) {
    const c = seq.get(strip(key)); if (c) return c;
    let parts = strip(key).split(ZWJ), dir = '';
    if (parts.length > 1 && parts[parts.length - 1] === RIGHT) { dir = ' facing right'; parts = parts.slice(0, -1); }
    const [a, b] = parts;
    if (parts.length === 1 && dir) return { n: baseName(a) + dir, k: '' };
    if (parts.length === 2 && PERSON[a]) {
      if (HAIR[b]) return { n: `${PERSON[a]}: ${HAIR[b]}`, k: 'hair' };
      if (PROF[b]) return { n: (a === '🧑' ? '' : PERSON[a] + ' ') + PROF[b], k: 'job work' };
      if (PROP[b]) return { n: `${PERSON[a]} ${PROP[b]}${dir}`, k: '' };
    }
    if (parts.length === 2 && (b === MALE || b === FEMALE)) return { n: gender(baseName(a), b) + dir, k: b === MALE ? 'male' : 'female' };
    if (parts.length >= 2 && parts.every(p => FAMILY[p])) return { n: 'family: ' + parts.map(p => FAMILY[p]).join(', '), k: 'parents kids' };
    if (parts.length >= 3 && PERSON[a] && PERSON[parts[parts.length - 1]] && parts.includes(HEART)) {
      const kiss = parts.includes('💋');
      return { n: `${kiss ? 'kiss' : 'couple with heart'}: ${PERSON[a]}, ${PERSON[parts[parts.length - 1]]}`, k: 'love' };
    }
    return null;
  }
  function describe(key, group) {
    // → { n, k } (name and table words); n is '' when nothing could be derived (the build reports and test lists those)
    const bare = strip(key);
    const cps = [...bare];
    if (group === 'flags') {
      if (cps.length === 2 && cps.every(ch => ch >= '\u{1F1E6}' && ch <= '\u{1F1FF}')) {
        const code = cps.map(ch => String.fromCharCode(ch.codePointAt(0) - 0x1F1E6 + 65)).join('');
        let n = ''; try { n = regions.of(code); } catch (_) { n = ''; }
        if (!n || n === code) n = '';
        return { n, k: ('flag ' + code.toLowerCase() + ' ' + (FLAG_WORDS[code] || '')).trim() };
      }
      if (cps[0] === '\u{1F3F4}' && cps.length > 2 && cps.slice(1).every(ch => ch >= '\u{E0020}' && ch <= '\u{E007F}')) {
        const code = cps.slice(1, -1).map(ch => String.fromCharCode(ch.codePointAt(0) - 0xE0000)).join('');
        const sub = SUBDIVISIONS[code]; return sub ? { n: sub[0], k: 'flag ' + sub[1] } : { n: '', k: '' };
      }
    }
    if (/^[0-9#*]\u20E3$/.test(bare)) return { n: 'keycap ' + cps[0], k: 'number key' };
    const c = cur.get(bare) || seq.get(bare);
    if (c && c.n) return { n: c.n, k: c.k };
    if (cps.length === 1) { const n = baseName(cps[0]); return { n, k: (c && c.k) || '' }; }
    const s = ofSequence(key);
    if (s) return s;
    return { n: '', k: '' };
  }
  return { describe, cur, seq };
}

/* ── runtime: this function's source is copied into charm-nest-emoji-data.js (plain browser script, also loads in Node) ── */
function runtime(VERSION, TONE_ROWS, GROUP_ROWS) {
  var TONE_IDS = TONE_ROWS.slice(1).map(function (t) { return t[0]; });
  var tones = TONE_ROWS.map(function (t) { return { id: t[0], name: t[1], c: t[2] }; });
  var toned = {}, pairs = {}, groups = [], all = [], byChar = {};

  GROUP_ROWS.forEach(function (g) {
    var items = g[3].map(function (r) {
      var x = r[3] || {}, it = { c: r[0], n: r[1], k: r[2], t: x.t ? 1 : 0 };
      if (x.t) {
        var variants = {};
        TONE_IDS.forEach(function (id) { variants[id] = x.t.split('\u0001').join(id); byChar[variants[id]] = it; });
        toned[r[0]] = variants;
        if (x.i) it.ts = 1;
      }
      if (x.p) { pairs[r[0]] = x.p; it.p = 1; }
      if (x.s) { it.s = x.s; x.s.forEach(function (c) { byChar[c] = it; }); }
      byChar[r[0]] = it;
      all.push(it);
      return it;
    });
    groups.push({ id: g[0], name: g[1], icon: g[2], items: items });
  });

  function fold(s) {   // case, accents and apostrophes folded
    return String(s == null ? '' : s).normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase().replace(/['’´`]/g, '');
  }
  function words(s) { return fold(s).split(/[^a-z0-9#*+&]+/).filter(Boolean); }
  function hexParts(c) { return Array.from(c).map(function (ch) { return ch.codePointAt(0).toString(16); }); }

  var index = null;
  function buildIndex() {
    index = all.map(function (it) {
      var h = hexParts(it.c), bare = h.filter(function (x) { return x !== 'fe0f'; });
      return { it: it, name: fold(it.n), words: words(it.n), kw: fold(it.k), kwWords: words(it.k), hex: bare.join(' '), hexAll: h.join(' '), bare: it.c.replace(/️/g, '') };
    });
  }

  // 1 whole word of the name or search words, 2 start of the name, 3 start of a word, 4 anywhere; -1 no match
  function tokenTier(t, e) {
    if (e.words.indexOf(t) >= 0 || e.kwWords.indexOf(t) >= 0) return 1;
    if (e.name.indexOf(t) === 0) return 2;
    var i;
    for (i = 0; i < e.words.length; i++) if (e.words[i].indexOf(t) === 0) return 3;
    for (i = 0; i < e.kwWords.length; i++) if (e.kwWords[i].indexOf(t) === 0) return 3;
    if (e.name.indexOf(t) >= 0 || e.kw.indexOf(t) >= 0) return 4;
    return -1;
  }

  // "1f600", "U+1F600", "2764 fe0f": code points. Short hex-looking words (face, bed) are words, not code points, unless written U+.
  function hexQuery(raw) {
    var explicit = /u\+/i.test(raw);
    var toks = raw.toLowerCase().replace(/u\+/g, ' ').replace(/[,;\-_]/g, ' ').split(/\s+/).filter(Boolean);
    if (!toks.length || !toks.every(function (t) { return /^[0-9a-f]{2,6}$/.test(t); })) return null;
    if (!explicit && !toks.some(function (t) { return t.length >= 4; })) return null;
    return toks.join(' ');
  }

  var EMOJI_RE = null;
  try { EMOJI_RE = new RegExp('\\p{Extended_Pictographic}|\\p{Regional_Indicator}', 'u'); } catch (e) { EMOJI_RE = null; }

  function search(query) {
    var raw = String(query == null ? '' : query).trim();
    if (!raw) return [];
    if (!index) buildIndex();
    var q = fold(raw).replace(/\s+/g, ' ').trim(), tokens = q ? q.split(' ') : [];
    var hex = hexQuery(raw), typed = EMOJI_RE && EMOJI_RE.test(raw) ? raw.replace(/️/g, '') : '';
    var hits = [];
    index.forEach(function (e, order) {
      var tier = -1;
      if (typed) {
        if (e.bare === typed) tier = 0; else if (e.bare.indexOf(typed) === 0) tier = 2;
      } else {
        if (e.name === q) tier = 0;
        else if (tokens.length) {
          var worst = 0;
          for (var i = 0; i < tokens.length && worst >= 0; i++) { var t = tokenTier(tokens[i], e); worst = t < 0 ? -1 : Math.max(worst, t); }
          if (worst >= 0) {
            tier = worst;
            if (tokens.length > 1) { var at = e.name.indexOf(q); if (at === 0) tier = Math.min(tier, 2); else if (at > 0) tier = Math.min(tier, 3); }
          }
        }
        if (hex) {
          var ht = e.hex === hex || e.hexAll === hex ? 0 : e.hex.indexOf(hex) === 0 ? 3 : (' ' + e.hex + ' ').indexOf(' ' + hex + ' ') >= 0 ? 5 : -1;
          if (ht >= 0 && (tier < 0 || ht < tier)) tier = ht;
        }
      }
      if (tier >= 0) hits.push({ it: e.it, tier: tier, len: e.name.length, order: order });
    });
    hits.sort(function (a, b) { return a.tier - b.tier || a.len - b.len || a.order - b.order; });
    return hits.map(function (h) { return h.it; });
  }

  function pair(base, a, b) {   // a mixed-tone couple or pair of hand-holders: the sequence, or '' when there is none
    var tpl = pairs[base];
    if (!tpl || a === b || TONE_IDS.indexOf(a) < 0 || TONE_IDS.indexOf(b) < 0) return '';
    return tpl.replace('\u0001', a).replace('\u0002', b);
  }
  function find(sequence) { return byChar[sequence] || null; }   // the item (the base item for a tone variant), or null

  return { version: VERSION, count: all.length, groups: groups, tones: tones, toned: toned, search: search, pair: pair, find: find };
}

/* ── build ──────────────────────────────────────────────────────────────────────────────────────────────────────── */
function build(options = {}) {
  const map = JSON.parse(fs.readFileSync(MAP_FILE, 'utf8'));
  const sequences = map.sequences, unsupported = new Set(map.unsupported), keys = Object.keys(sequences);
  const report = { dropped: [], notOffered: [], folded: [], sharedShapes: [], uncovered: [], unnamed: [], unmatchedCurated: [], notes: [] };

  // the real engraver path
  const ot = require('../vendor/opentype-1.3.4.min.js'), Text = require('../charm-nest-text.js'), G = require('../charm-nest-geom.js');
  const emojiBytes = fs.readFileSync(path.join(ROOT, 'vendor/fonts/NotoEmoji-Regular.ttf'));
  if (crypto.createHash('sha256').update(emojiBytes).digest('hex') !== map.fontSha256) throw new Error('NotoEmoji-Regular.ttf does not match fontSha256 of emoji-sequences.json');
  const toArrayBuffer = b => b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength);
  const emojiFont = ot.parse(toArrayBuffer(emojiBytes));
  const baseFont = ot.parse(toArrayBuffer(fs.readFileSync(path.join(ROOT, 'vendor/fonts/SourceSans3-Regular.otf'))));
  const font = Text.withEmoji(baseFont, emojiFont, map, ot.Path);
  const engraverTreatsAsEmoji = c => /\p{Extended_Pictographic}|\p{Regional_Indicator}|\u20E3/u.test(c);   // the test withEmoji's tokenizer applies
  const verify = c => {
    if (!(c in sequences)) return 'key is not in sequences';
    if (unsupported.has(c)) return 'key is listed as unsupported';
    if (Text.graphemes(c).length !== 1) return 'the engraver splits it into more than one character';
    if (!engraverTreatsAsEmoji(c)) return 'the engraver would not draw it as an emoji';
    const cov = G.glyphCoverage(font, c);
    if (!cov.ok) return 'glyphCoverage reports missing: ' + cov.missing.join(' ');
    let path_;
    try { path_ = font.getPath(c, 0, 0, 20); } catch (e) { return 'getPath failed: ' + e.message; }
    if (!path_ || path_.commands.length < 4) return 'no outline';
    if (!(font.getAdvanceWidth(c, 20) > 0)) return 'no advance width';
    return '';
  };

  // candidates: fully qualified emoji in emoji-test order
  const anchors = GROUPS.map(([id, , , first]) => { const i = keys.indexOf(first); if (i < 0) throw new Error('group anchor missing: ' + id); return [id, i]; });
  for (let i = 1; i < anchors.length; i++) if (anchors[i][1] <= anchors[i - 1][1]) throw new Error('group anchors are out of order at ' + anchors[i][0]);
  const groupOf = i => { let g = anchors[0][0]; for (const [id, start] of anchors) if (i >= start) g = id; return g; };
  const fq = [];
  keys.forEach((k, i) => { if (RGI.test(k) && !unsupported.has(k)) fq.push({ k, g: groupOf(i) }); });
  const byStripped = new Map();
  for (const x of fq) if (!hasTone(x.k)) byStripped.set(strip(x.k), x.k);

  // cells and tone templates
  const cells = [], cellOf = new Map();
  for (const x of fq) {
    if (x.g === 'component') { report.notOffered.push({ c: x.k, hex: hexKey(x.k), why: 'Component (skin-tone swatch or hair piece): not an emoji on its own' }); continue; }
    if (hasTone(x.k)) continue;
    const reason = verify(x.k);
    if (reason) { report.dropped.push({ c: x.k, hex: hexKey(x.k), why: reason }); continue; }
    const cell = { c: x.k, g: x.g, tone: null, pair: null };
    cells.push(cell); cellOf.set(x.k, cell);
  }
  const toneVariants = new Map();      // base cell key -> Map(tone -> variant key)
  const mixed = new Map();             // template (\u0001 \u0002) -> Map("a b" -> variant key)
  const lostVariants = [];
  for (const x of fq) {
    if (x.g === 'component' || !hasTone(x.k)) continue;
    const tl = [...x.k].filter(isTone);
    const reason = verify(x.k);
    if (tl.length === 1 || tl.every(t => t === tl[0])) {
      const untoned = [...x.k].filter(ch => !isTone(ch)).join('');
      // a same-tone variant with several modifiers (people holding hands) carries the tone on each person only
      const base = byStripped.get(strip(untoned));
      if (!base || !cellOf.has(base)) { report.notes.push('tone variant without an offered base: ' + hexKey(x.k)); continue; }
      if (reason) { lostVariants.push({ c: x.k, hex: hexKey(x.k), why: reason, base }); continue; }
      if (!toneVariants.has(base)) toneVariants.set(base, new Map());
      toneVariants.get(base).set(tl[0], x.k);
    } else {
      if (tl.length !== 2) { report.notes.push('mixed-tone sequence with ' + tl.length + ' tones skipped: ' + hexKey(x.k)); continue; }
      let n = 0; const tmpl = [...x.k].map(ch => isTone(ch) ? (n++ === 0 ? '\u0001' : '\u0002') : ch).join('');
      if (!mixed.has(tmpl)) mixed.set(tmpl, new Map());
      mixed.get(tmpl).set(tl[0] + ' ' + tl[1], reason ? { bad: reason, c: x.k } : x.k);
    }
  }
  const toneChars = TONES.slice(1).map(t => t[0]);
  const expand1 = (tpl, t) => tpl.split('\u0001').join(t);
  const bad = [];
  for (const [base, vars] of toneVariants) {
    // one template per base: the light variant with each tone char replaced; all five must reproduce exactly
    const light = vars.get(toneChars[0]);
    const tpl = light ? [...light].map(ch => ch === toneChars[0] ? '\u0001' : ch).join('') : null;
    if (!tpl || !toneChars.every(t => vars.get(t) === expand1(tpl, t))) { bad.push(base); lostVariants.push({ c: base, hex: hexKey(base), why: 'tone variants do not follow one template or one failed the engraver check', base }); continue; }
    cellOf.get(base).tone = tpl;
  }
  for (const [tmpl, vars] of mixed) {
    const pairs = []; for (const a of toneChars) for (const b of toneChars) if (a !== b) pairs.push(a + ' ' + b);
    const ok = pairs.every(p => { const v = vars.get(p); return v && typeof v === 'string' && v === tmpl.replace('\u0001', p.split(' ')[0]).replace('\u0002', p.split(' ')[1]); });
    if (!ok) { lostVariants.push({ c: [...vars.values()].map(v => v.c || v)[0], hex: 'mixed-tone template', why: 'a mixed-tone combination is missing from the map or failed the engraver check', base: tmpl }); continue; }
    // the cell it hangs under: same-tone form if that is an emoji, else the neutral form
    const sameTone = tmpl.replace('\u0001', toneChars[0]).replace('\u0002', toneChars[0]);
    let base = null;
    for (const [b, vs] of toneVariants) if (vs.get(toneChars[0]) === sameTone) base = b;
    if (!base) {
      const neutral = strip([...tmpl].filter(ch => ch !== '\u0001' && ch !== '\u0002').join(''));
      if (byStripped.has(neutral) && cellOf.has(byStripped.get(neutral))) base = byStripped.get(neutral);
      else { const h = MIXED_BASE[hexKey(neutral)]; if (h) base = byStripped.get(strip(fromHex(h))); }
    }
    if (!base || !cellOf.has(base)) { report.notes.push('mixed-tone template without an offered cell: ' + hexKey(tmpl.replace(/[\u0001\u0002]/g, ''))); continue; }
    if (cellOf.get(base).pair) { report.notes.push('two mixed-tone templates for one cell: ' + hexKey(base)); continue; }
    cellOf.get(base).pair = tmpl;
  }
  for (const v of lostVariants) report.dropped.push({ c: v.c, hex: v.hex, why: 'tone variant dropped: ' + v.why });

  // names, words
  const names = loadNames(options.refreshNames), namer = makeNamer(names);
  const fold = s => s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase();
  for (const cell of cells) { cell.d = namer.describe(cell.c, cell.g); if (!cell.d.n) report.unnamed.push({ c: cell.c, hex: hexKey(cell.c) }); }

  // One cell per outline. The monochrome emoji font draws man, woman and person variants, hair colours, jobs and family
  // compositions with exactly the same ink (and every skin tone too), so cells with an identical outline fold into the neutral
  // one; their names become search words and their keys are kept in `s`. Flags are never folded: each one is a different place.
  const inkOf = k => JSON.stringify(sequences[k].filter(r => r[0] !== 3));      // glyph 3 is the empty space glyph: no ink
  const gendered = n => /\b(man|woman|men|women|boy|girl|mermaid|merman|male|female)\b/.test(n);
  const byInk = new Map(), folded = new Set();
  for (const cell of cells) if (cell.g !== 'flags') { const i = inkOf(cell.c); (byInk.get(i) || byInk.set(i, []).get(i)).push(cell); }
  for (const list of byInk.values()) {
    if (list.length < 2) continue;
    const rep = list.find(c => !gendered(c.d.n)) || list[0];
    rep.same = list.filter(c => c !== rep);
    for (const c of rep.same) folded.add(c);
    report.folded.push([rep.c, ...rep.same.map(c => c.c)]);
  }
  const offered = cells.filter(c => !folded.has(c));

  // nothing the laser can cut may be lost: every compatible key must engrave with the ink of an offered cell, a tone variant or a pair
  const reach = new Set();
  const pairKeys = tmpl => { const o = []; for (const a of toneChars) for (const b of toneChars) if (a !== b) o.push(tmpl.replace('\u0001', a).replace('\u0002', b)); return o; };
  for (const c of offered) {
    reach.add(inkOf(c.c));
    if (c.tone) for (const t of toneChars) { const v = expand1(c.tone, t); reach.add(inkOf(v)); if (inkOf(v) !== inkOf(c.c)) c.ts = true; }
    if (c.pair) for (const v of pairKeys(c.pair)) reach.add(inkOf(v));
  }
  const droppedKeys = new Set(report.dropped.map(x => x.c));
  report.uncovered = fq.filter(x => x.g !== 'component' && !droppedKeys.has(x.k) && !reach.has(inkOf(x.k))).map(x => ({ c: x.k, hex: hexKey(x.k) }));

  const used = new Set();
  const out = GROUPS.filter(g => g[1]).map(([id, name, icon]) => ({ id, name, icon, items: [] }));
  const groupById = new Map(out.map(g => [g.id, g]));
  for (const cell of offered) {
    const d = cell.d;
    let words = new Set();
    const nameWords = new Set(fold(d.n).split(/[^a-z0-9]+/).filter(Boolean));
    const addWords = text => { for (const w of (text || '').split(/\s+/).filter(Boolean)) words.add(w.toLowerCase()); };
    addWords(d.k);
    for (const [re, add] of WORD_RULES) if (re.test(d.n.toLowerCase())) addWords(add);
    for (const alt of cell.same || []) { addWords(alt.d.k); for (const w of fold(alt.d.n).split(/[^a-z0-9]+/).filter(Boolean)) words.add(w); }
    words = [...words].filter(w => !nameWords.has(fold(w)));
    const extra = {};
    if (cell.tone) extra.t = cell.tone;
    if (cell.pair) extra.p = cell.pair;
    if (cell.same && cell.same.length) extra.s = cell.same.map(c => c.c);
    if (cell.ts) extra.i = 1;
    const item = [cell.c, d.n, words.join(' ')];
    if (Object.keys(extra).length) item.push(extra);
    groupById.get(cell.g).items.push(item); used.add(strip(cell.c));
  }
  for (const c of cells) used.add(strip(c.c));
  for (const [e, v] of namer.cur) if (!used.has(e) && !(v.n === '')) report.unmatchedCurated.push(e + ' ' + hexKey(e));
  for (const g of out) {
    if (!g.items.length) throw new Error('empty group ' + g.id);
    if (!cellOf.has(g.icon)) report.notes.push('group icon is not an offered cell: ' + g.id + ' ' + g.icon);
    else if (folded.has(cellOf.get(g.icon))) report.notes.push('group icon was folded into another cell: ' + g.id + ' ' + g.icon);
  }
  const count = out.reduce((n, g) => n + g.items.length, 0);

  // shapes shared by several offered flags (the laser cuts the same outline for each)
  const byShape = new Map();
  for (const cell of offered) { const s = inkOf(cell.c); (byShape.get(s) || byShape.set(s, []).get(s)).push(cell.c); }
  for (const list of byShape.values()) if (list.length > 1) report.sharedShapes.push(list);

  // output
  const q = s => JSON.stringify(s).replace(/[\u200d\ufe0e\ufe0f\u20e3\u2028\u2029]|[\u{e0000}-\u{e007f}]|[\u{1f3fb}-\u{1f3ff}]/gu, m => { const c = m.codePointAt(0); return c > 0xffff ? '\\u{' + c.toString(16) + '}' : '\\u' + c.toString(16).padStart(4, '0'); });
  const rows = out.map(g => `    [${q(g.id)}, ${q(g.name)}, ${q(g.icon)}, [\n${g.items.map(it => '      [' + it.map(q).join(',') + ']').join(',\n')}\n    ]]`).join(',\n');
  const text = [
    '/* charm-nest-emoji-data.js — every emoji outline the laser engraver can draw, for the Emoji button of the Back engraving words box.',
    ' * GENERATED by scripts/build-emoji-picker-data.cjs from vendor/fonts/emoji-sequences.json (Unicode emoji-test 17.0); do not edit by hand.',
    ' * Built with ICU ' + process.versions.icu + ' (the RGI emoji set and the flag names come from it).',
    ' * window.CNEmojiData = { version, count, groups:[{id,name,icon,items}], tones:[{id,name,c}], toned:{base:{tone:sequence}},',
    ' *   search(query) -> items (name, search words and hex code point; case and accents folded; exact, then prefix, then contains),',
    ' *   find(sequence) -> item or null, pair(base, toneA, toneB) -> sequence of a mixed-tone couple or hand-holders, or "" }',
    ' * item: c = the key of emoji-sequences.json, n = name, k = search words, t:1 = has skin tones (the variants are in toned, never cells),',
    ' *   p:1 = also has mixed-tone pairs, s = other sequences that engrave with exactly the same outline (folded into this cell),',
    ' *   ts:1 = its tone variants engrave differently (for every other item all five tones engrave exactly like the plain emoji).',
    ' * No shape data: the engraver reads its shapes from emoji-sequences.json. */',
    '(function () {',
    '  "use strict";',
    `  var VERSION = ${q(DATA_VERSION)};`,
    '  var TONE_ROWS = ' + q(TONES.map(t => [t[0], t[1], t[2]])) + ';',
    '  var GROUP_ROWS = [',
    rows,
    '  ];',
    '',
    '  var api = (' + runtime.toString().replace(/\n/g, '\n  ') + ')(VERSION, TONE_ROWS, GROUP_ROWS);',
    '  if (typeof module === "object" && module.exports) module.exports = api;',
    '  else if (typeof window !== "undefined") window.CNEmojiData = api;',
    '  else if (typeof globalThis !== "undefined") globalThis.CNEmojiData = api;',
    '})();',
    ''
  ].join('\n');
  report.count = count;
  report.perGroup = Object.fromEntries(out.map(g => [g.id, g.items.length]));
  report.toneBases = offered.filter(c => c.tone).length;
  report.toneChangesInk = offered.filter(c => c.ts).map(c => c.c);
  report.pairBases = offered.filter(c => c.pair).length;
  report.cellsBeforeFolding = cells.length;
  report.toneVariants = [...toneVariants.values()].reduce((n, m) => n + m.size, 0) - 5 * bad.length;
  report.mixedVariants = [...mixed.values()].reduce((n, m) => n + m.size, 0);
  return { text, report };
}

function main() {
  const args = process.argv.slice(2);
  const { text, report } = build({ refreshNames: args.includes('--refresh-names') });
  if (args.includes('--check')) {
    const have = fs.existsSync(OUT) ? fs.readFileSync(OUT, 'utf8') : '';
    if (have !== text) { console.error('charm-nest-emoji-data.js differs from what the generator produces; run node scripts/build-emoji-picker-data.cjs'); process.exit(1); }
    console.log('charm-nest-emoji-data.js is up to date (' + report.count + ' emoji)');
  } else {
    fs.writeFileSync(OUT, text);
    console.log(`wrote ${path.relative(ROOT, OUT)} (${Buffer.byteLength(text)} bytes)`);
  }
  console.log(JSON.stringify({ count: report.count, perGroup: report.perGroup, toneBases: report.toneBases, toneVariants: report.toneVariants, pairBases: report.pairBases, mixedVariants: report.mixedVariants, dropped: report.dropped.length, notOffered: report.notOffered.length, foldedGroups: report.folded.length, cellsBeforeFolding: report.cellsBeforeFolding, toneChangesInk: report.toneChangesInk.length, uncovered: report.uncovered.length, sharedShapeGroups: report.sharedShapes.length, unnamed: report.unnamed.length, unmatchedCurated: report.unmatchedCurated.length, notes: report.notes }, null, 1));
  const i = args.indexOf('--report'); if (i >= 0 && args[i + 1]) fs.writeFileSync(args[i + 1], JSON.stringify(report, null, 1));
}
if (require.main === module) main();
module.exports = { build };
