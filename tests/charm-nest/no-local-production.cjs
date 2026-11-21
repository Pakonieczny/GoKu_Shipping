// CLOUDWORLD stage F (Paul, 20 Nov 2026: "the computers are never allowed to store their own information. Unless the cloud gives it to them").
// Every name the sorter's pages use in this browser's storage is on the reviewed list below, each with what it is. A NEW name fails this test until a person
// says what it is: a display setting, sign-in, a per-computer timer, a cache the cloud replaces, an outbox of work not yet in the cloud — or production data,
// which belongs in the cloud. And the copies the cloud now holds for every computer (Claude's readings, the listings' SKU tables) are never written here again.
//   node tests/charm-nest/no-local-production.cjs
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const KINDS = {
  display: 'ce.ai ce.aidock ce.aidock.prefs ce.dock ce.eraser ce.extras ce.offsetPath ce.opts ce.view ce.xpanel cem.recent cem.start cn.activityLists.v1 cn.alphabet.pick cn.card.tab cn.ceLib.recent cn.ceLib.view cn.dsWide cn.eff.days cn.eff.key cn.eff.p.range cn.eff.view cn.emoji.recent cn.histView cn.libTab cn.listImageFraming. cn.sheetwin.tab cn.settings cn.mail.tab cn.repoTab.v1',
  signin: 'cn.employee cn.lastEmployee cn.lastRole cn.role cn.people cn.passcode cn.repo.emp cn.repoCarry.v1 cn.repoLock.v1 cn.repoSession.v1 cn.mail.operator cn.mail.station cn.mail.pair',
  computer: 'cn.dev cn.madeThumb.dev cn.arrivals. cn.drift cn-drift:production cn.push cn-push cn.sw cn-sw-meta- cn.newver.reloads cn.sync.cursor. cn-sync: cn-sync:production cn-sync:sandbox cn-sync-once: cn-hang cn.backup.v cn.backup.w cn.mail.health cn.mail.linkTold cn.mail.told cn.mail.tr cn.mail.test cn.share.seen. cn.team.seen cn.timeline.sent cn.marks.sent. cn.laserSheetLast.v1 cn.keepIntentsSaid.v1',   // (item 15: a per-browser note of which design-save intents the boot message already told; not production data)
  sandbox: 'cn-local.sandbox cn-custom-sandbox cn-sandbox-epoch cn.resetEpoch. cn.resetEpoch.sandbox cn.roseRehearsal.sandbox.v1 cn.sandboxAutoPull cn.sandboxHold cn.sandboxLastReset cn.sandboxPullNext cn.sandboxPulling cn.sandboxResetNote cn.sandboxWiping cn.simClock.sandbox cn.autoCancel.v1:sandbox',
  cache: 'cn-blobs cn-ce-image cn-thumbs cn-pics- cn-static- cn-local. cn-store: cn-map-copy cn-master-list cn-library cn-design-1 cn.orderDays.production cn.listingPhotos.v1 cn.designs.v1',
  outbox: 'cn-outbox: cn.outbox. cn.team.outbox cn.designLog.outbox cn.mail.outbox cn.mail.outclaim. cn.mail.drafts cn.team.drafts cn.mail.waiting cn.sheetwin.moves cn-keep: cn.keep cn.keep.v1 cn.keepIntents.v1',
  channel: 'cn-cloud-back cn-custom cn-custom-done cn-review-mark cn-order-seal cn-order-shipped cn-charm-saved cn-library-write cn-engrave-setup cn-mail cn-mail-inbox',
  // copies of what the cloud keeps for every computer (charm-nest-cloud-share.js mirrors them into Charm_Nest_Shared): to be read from the cloud only, next
  shared: 'cn.completed.v1 cn.completed.v1: cn.flow.v1 cn.flow.v1: cn.stock cn.sheetwin.freed cn.tl.sheets cn.custom.shared: cn.ds.shared: cn.autoCancel.v1: cn.cancelIds.v1: cn.orderhold.run cn.learnedKey',
  // no page writes these any more (stage F); the copy an older page left is backed up to the cloud and removed (charm-nest-cloud-world.js backupAndClear)
  removed: 'cn.customRead. cn.listingSkus.v1',
  // PHOTOPAUSE (Paul, 20 Nov 2026, "Yes, lift it"): the photo lookup pause is only what the cloud's latest answer says; no page writes or reads it, the copy an older page left is removed on load (charm-nest-bridge.js ListMedia)
  cleared: 'cn.listingPhotoPause cn.listingPhotoPause.v2'
};
const known = new Map(); for (const [kind, list] of Object.entries(KINDS)) for (const k of list.split(/\s+/).filter(Boolean)) known.set(k, kind);
const files = fs.readdirSync(root).filter(f => /^charm-nest-.*\.js$/.test(f) || f === 'charm-nest-1.html');
const used = new Map();
for (const f of files) { const text = fs.readFileSync(path.join(root, f), 'utf8'); for (const m of text.matchAll(/['"`](cn[.\-][A-Za-z0-9_.:\-]*|ce\.[A-Za-z0-9_.:\-]*|cem\.[A-Za-z0-9_.]*)['"`]/g)) { if (!used.has(m[1])) used.set(m[1], new Set()); used.get(m[1]).add(f); } }
let failed = 0; const say = (ok, name, d) => { console.log((ok ? 'OK   ' : 'FAIL ') + name + (d ? ' :: ' + d : '')); if (!ok) failed++; };
const unknown = [...used.keys()].filter(k => !known.has(k));
say(!unknown.length, '1 · every name this site keeps in a browser is on the reviewed list (a new one must be classified; production data belongs in the cloud)', unknown.map(k => `${k} (${[...used.get(k)].join(', ')})`).join('; '));
const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
say(!/localStorage\.setItem\(TABLES_LS/.test(bridge), "2 · the listings' SKU tables are never written to the browser");
const cr = bridge.slice(bridge.indexOf('const CustomRead = window.CustomRead'), bridge.indexOf('const CustomRead = window.CustomRead') + 4000);
say(!/localStorage\.setItem\(LS/.test(cr) && !/localStorage\.getItem\(LS\)/.test(cr), "3 · Claude's readings are neither written to nor read from the browser");
const cw = fs.readFileSync(path.join(root, 'charm-nest-cloud-world.js'), 'utf8');
say(/browserBackupPut/.test(cw) && cw.indexOf('browserBackupPut') < cw.indexOf('removeItem'), '4 · a computer backs its browser storage up to the cloud before it removes anything');
console.log(failed ? 'FAIL no-local-production' : 'PASS no-local-production');
process.exit(failed ? 1 : 0);
