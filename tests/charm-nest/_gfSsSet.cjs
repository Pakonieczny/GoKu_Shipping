// The formed set a Rose Gold sheet joins, built through the page (bridge-server.cjs worlds).
// R1 (Paul, 10 Oct 2026): "In order for a Set of Sheets to be allowed to exist there must be at minimum 1 Completed GF Sheet and 1 Completed SS Sheet."
// R4 / R12 (Paul, 12 Oct): a Rose Gold sheet never starts or completes a set; it joins one that has formed (docs/charm-nest/set-rules.md).
// A suite whose world held only a Rose Gold sheet and expected it in a set predates these rules: it builds this set first.
// Each sheet is a custom design sent to its metal (Custom Orders -> Send to Sheet), nested by the real solver, and released by hand
// (the sheet window's Make QR label on a filling sheet: SheetWin doLabel "release", releaseFull + putSheet + Gate.assemble), so it is COMPLETED (R2).
//   const W = require('./_gfSsSet.cjs');
//   orders: [...W.orders(SHIP, DAY)]  (seeded with the suite's own)   ·   await W.form(page)  -> { setId, gf, ss }
'use strict';
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
const DXF = w => DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES',
  0, 'LWPOLYLINE', 8, 'CUT', 90, 4, 70, 1, 10, 0, 20, 0, 10, w, 20, 0, 42, 0.4, 10, w, 20, 20, 10, 0, 20, 20,
  0, 'CIRCLE', 8, 'CUT', 10, w / 2, 20, 16, 40, 1.2, 0, 'ENDSEC', 0, 'EOF');
const RID = { gold: '4175400001', silver: '4175400002' };
const META = { gold: ['Gold Filled', 'GF 14/20'], silver: ['Sterling Silver', 'Sterling Silver'] };
const orderOf = (metal, SHIP, DAY) => { const rid = RID[metal], n = 8; return { receiptId: rid, orderNumber: rid, createTs: SHIP - n * DAY, updateTs: SHIP - n * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + rid.slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: rid + '1', listingId: '18000' + rid.slice(-5), sku: 'CUSTOM-N-001-' + rid.slice(-6), title: 'Custom Name Necklace, Personalized Charm Necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: META[metal][0] }], metalKey: metal, metalLabel: META[metal][1], personalization: [], buyerMessage: '' }] }; };
const orders = (SHIP, DAY) => ['gold', 'silver'].map(m => orderOf(m, SHIP, DAY));

async function sendTo(page, metal) {
  const rid = RID[metal];
  await page.evaluate(() => CN.setMode('review'));
  await page.click('#reviewView .egTab[data-k="customOrder"]');
  const card = `#rvList .reviewListRow[data-rid="${rid}"]`;
  await page.waitForSelector(card);
  await page.evaluate(({ sel, text, name }) => { const dt = new DataTransfer(); dt.items.add(new File([text], name)); const n = document.querySelector(sel); for (const t of ['dragenter', 'dragover', 'drop']) n.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt, clientX: 700, clientY: 400 })); { const a = document.querySelector('#cuAsk [data-ask-yes]'); if (a) a.click(); } }, { sel: card, text: DXF(18), name: metal + '.dxf' });
  await page.waitForFunction(() => document.querySelectorAll('#cuDlg[open] .cuFile .cuThumb img').length === 1, null, { timeout: 30000 });
  await page.click(`#cuDlg .cuFile .cuM[data-m="${metal}"]`);
  await page.click('#cuDlg [data-send]');
  await page.waitForFunction(() => !document.querySelector('#cuDlg').open && !document.querySelector('#tourLayer > *'), null, { timeout: 30000 });
  await page.waitForFunction(m => { const sh = CN.S.sheets[m].pages.at(-1); return sh.charms.length && !['nesting', 'finishing', 'queued'].includes(sh.status); }, metal, { timeout: 30000 });
  await page.evaluate(m => { const sh = CN.S.sheets[m].pages.at(-1); if (sh.status !== 'complete' || sh.dirty) CN.startNest(sh); }, metal);
  await page.waitForFunction(m => { const sh = CN.S.sheets[m].pages.at(-1); return sh.status === 'complete' && sh.persistedDone && sh.verification?.ok && !sh.dirty; }, metal, { timeout: 120000 });
}

/** Sends, nests and releases by hand a GF and an SS sheet; resolves once both are in one formed set of the open run. */
async function form(page) {
  for (const m of ['gold', 'silver']) await sendTo(page, m);
  await page.evaluate(() => CN.setMode('nest'));
  for (const m of ['gold', 'silver']) await page.evaluate(async m => {
    const sh = CN.S.sheets[m].pages.at(-1); sh.releaseFull = true; if (sh.topup && !sh.topup.closedAt) { sh.topup.closedAt = Date.now(); sh.topup.byHand = true; }
    await CN.api('charmNestLibrary', { op: 'putSheet', sheet: { id: sh.sheetId, releaseFull: true, topup: sh.topup || null } }, { quiet: true });
  }, m);
  await page.evaluate(async () => { await Gate.assemble(B.run); });
  await page.waitForFunction(() => { const g = CN.S.sheets.gold.pages.at(-1), s = CN.S.sheets.silver.pages.at(-1); return g.setId && !g.draft && s.setId === g.setId && !s.draft; }, null, { timeout: 60000 });
  return page.evaluate(() => { const g = CN.S.sheets.gold.pages.at(-1), s = CN.S.sheets.silver.pages.at(-1); return { setId: g.setId, gf: g.sheetId, ss: s.sheetId }; });
}

module.exports = { orders, form, RID };
