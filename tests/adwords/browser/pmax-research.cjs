#!/usr/bin/env node
'use strict';
// Real-browser check of the Product ads research card (Opportunities > Product ads) at desktop and phone width.
// Serves the repository root, answers the console API from fixtures.cjs plus the sample in ../fixtures/pmax-research-sample.json,
// fixes the clock to the day the sample was written, and checks: headline, occasion chip, Fits and Search themes visible without
// opening anything and following the ticked products, the dated fold with its outside links, state kept across a redraw, the banner
// that names unavailable sources, the scan funnel with and without ideas, and that nothing is wider than the screen.
// It also checks the first thing on each opportunity card of both tabs (Search opportunities from ../fixtures/opportunity-visuals-sample.json,
// product photos served as local pictures): the plain type name and icon, the occasion chip, the run headline and bar, the reasons one tap
// away, "What is this?" by keyboard, the ad thumbnails opening a larger view that Escape closes, and an older card with no schedule.
//
//   node tests/adwords/browser/pmax-research.cjs [--shots=<dir>]
//
// Needs Playwright with Chromium (PLAYWRIGHT_MODULE, the local node_modules or the global npm root). Nothing is installed and no
// request leaves the machine; every other host is blocked. Not part of tests/adwords/run.cjs.
const fs = require('fs'), path = require('path'), http = require('http'), assert = require('assert/strict'), { execSync } = require('child_process');
const { createFixtures } = require('./fixtures.cjs');

const REPO = path.resolve(__dirname, '../../..');
const PASSCODE = 'harness-passcode-not-real';
const sample = JSON.parse(fs.readFileSync(path.join(__dirname, '../fixtures/pmax-research-sample.json'), 'utf8'));
const shotsArg = process.argv.find(a => a.startsWith('--shots=')), SHOTS = shotsArg ? path.resolve(shotsArg.split('=')[1]) : null;
const clone = x => JSON.parse(JSON.stringify(x));
const visuals = JSON.parse(fs.readFileSync(path.join(__dirname, '../fixtures/opportunity-visuals-sample.json'), 'utf8'));
// A plain picture for every product photo, so the strips have something to draw without leaving the machine.
const PHOTO = h => '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 600 600"><rect width="600" height="600" fill="hsl(' + h + ',30%,85%)"/><circle cx="300" cy="300" r="110" fill="hsl(' + h + ',50%,55%)"/></svg>';

function loadPlaywright() {
  const candidates = [process.env.PLAYWRIGHT_MODULE, 'playwright'];
  try { candidates.push(path.join(execSync('npm root -g', { encoding: 'utf8' }).trim(), 'playwright')); } catch (e) {}
  for (const c of candidates.filter(Boolean)) { try { return require(c); } catch (e) {} }
  throw new Error('Playwright is not available. Set PLAYWRIGHT_MODULE to its install path.');
}

// What the opportunities action answers for a scenario.
function scenarioPayload(fx, body, scenario) {
  const base = fx.respond('opportunities', body).body, opp = clone(sample.opportunity);
  const out = Object.assign({}, base, { pmaxList: [opp, clone(sample.legacyOpportunity)], pmaxFunnel: clone(sample.funnel) });
  if (scenario === 'ideas') out.opportunities = clone(visuals.searchOpportunities);
  if (scenario === 'none') Object.assign(out, { pmaxList: [], pmaxFunnel: clone(sample.funnelNone) });
  if (scenario === 'computed') { opp.research.source = 'computed'; opp.research.model = null; opp.research.whyNow.marketRead = null; opp.research.whyNow.caution = null; opp.research.sources = []; }
  return out;
}

async function run() {
  const { chromium } = loadPlaywright();
  const types = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml', '.json': 'application/json' };
  const server = http.createServer((req, res) => {
    let p = decodeURIComponent(new URL(req.url, 'http://x').pathname); if (p === '/') p = '/brites-adwords.html';
    const file = path.join(REPO, p);
    if (!file.startsWith(REPO + path.sep) || /(^|\/)\.(git|env|netlify)|node_modules|secrets/.test(p)) { res.writeHead(403); return res.end(); }
    fs.readFile(file, (err, data) => { if (err) { res.writeHead(404); return res.end(); } res.writeHead(200, { 'Content-Type': types[path.extname(file)] || 'application/octet-stream', 'Cache-Control': 'no-store' }); res.end(data); });
  });
  await new Promise(r => server.listen(0, '127.0.0.1', r));
  const base = 'http://127.0.0.1:' + server.address().port;
  const browser = await chromium.launch({ headless: true, args: ['--disable-gpu', '--no-sandbox'] });
  let checks = 0;
  const ok = (cond, msg) => { assert.ok(cond, msg); checks++; };
  try {
    for (const vp of [{ name: 'desktop', width: 1440, height: 1000, phone: false }, { name: 'phone', width: 390, height: 844, phone: true }, { name: 'narrow phone', width: 320, height: 700, phone: true }]) {
      for (const scenario of ['ideas', 'computed', 'none']) {
        const fx = createFixtures(), errors = [];
        const context = await browser.newContext({ viewport: { width: vp.width, height: vp.height }, isMobile: vp.phone, hasTouch: vp.phone, locale: 'en-US', timezoneId: 'UTC', serviceWorkers: 'block', reducedMotion: 'reduce' });
        await context.clock.setFixedTime(new Date('2026-09-29T17:05:00Z'));
        await context.route('**/*', async route => {
          const req = route.request(), url = new URL(req.url());
          if (url.protocol === 'data:' || url.protocol === 'blob:') return route.continue();
          if (url.hostname === 'cdn.shopify.com') return route.fulfill({ status: 200, contentType: 'image/svg+xml', body: PHOTO(url.pathname.length * 37 % 360) });
          if (url.hostname === '127.0.0.1' && url.pathname.startsWith('/.netlify/functions/')) {
            let body = {}; try { body = JSON.parse(req.postData() || '{}'); } catch (e) {}
            if ((req.headers()['x-edit-passcode'] || body.passcode) !== PASSCODE) return route.fulfill({ status: 401, contentType: 'application/json', body: '{"error":"unauthorized"}' });
            // The evidence refresh returns the saved suggestion with fresh sales data, research included.
            const r = body.action === 'opportunities' ? { body: scenarioPayload(fx, body, scenario) }
              : body.action === 'pmaxRecommendationEvidence' ? { body: { ok: true, candidate: scenarioPayload(fx, {}, scenario).pmaxList.find(x => x.handle === body.handle), removedItemIds: [] } }
              : fx.respond(body.action, body);
            return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(r ? r.body : { ok: true }) });
          }
          if (url.hostname === '127.0.0.1') return route.continue();
          return route.abort('blockedbyclient');
        });
        const page = await context.newPage();
        page.on('pageerror', e => errors.push('page error: ' + e.message));
        page.on('console', m => { if (m.type() === 'error' && !/ERR_BLOCKED_BY_CLIENT/.test(m.text())) errors.push('console: ' + m.text()); });
        page.on('dialog', d => { errors.push('native ' + d.type() + ': ' + d.message()); d.dismiss().catch(() => {}); });
        await page.goto(base + '/brites-adwords.html');
        await page.fill('#pc', PASSCODE); await page.click('.signin__submit');
        await page.waitForFunction(() => getComputedStyle(document.getElementById('app')).display !== 'none', null, { timeout: 15000 });
        await page.waitForTimeout(700);
        if (vp.phone) { await page.locator('#navBurger').tap(); await page.waitForTimeout(400); }
        await page.locator('#nav button[data-v="bench"]').click();
        await page.waitForTimeout(500);
        await page.locator('[data-growth-lane="product"]').click();
        await page.waitForFunction(() => document.querySelector('#pmaxSec'), null, { timeout: 8000 });
        await page.waitForTimeout(500);
        const label = vp.name + ' / ' + scenario;
        const sec = page.locator('#pmaxSec');
        const noOverflow = async where => {
          const bad = await page.evaluate(() => {
            const vw = document.documentElement.clientWidth, out = [];
            if (document.documentElement.scrollWidth > vw + 1) out.push('page scrolls sideways: ' + document.documentElement.scrollWidth + ' > ' + vw);
            document.querySelectorAll('#pmaxSec *, #scanAudit *').forEach(el => {
              if (el.closest('.pmxForecastScroll')) return; // the forecast table scrolls inside its own frame
              if (el.closest('.abp-row')) return; // so does the row of ad thumbnails (checked below to scroll on its own)
              const r = el.getBoundingClientRect();
              if (r.width > 0 && (r.right > vw + 0.5 || r.left < -0.5)) out.push(el.tagName + '.' + el.className + ' spills to ' + Math.round(r.right));
            });
            return out.slice(0, 6);
          });
          ok(bad.length === 0, label + ' ' + where + ': ' + bad.join('; '));
        };
        const shot = async name => { if (SHOTS) { fs.mkdirSync(SHOTS, { recursive: true }); await sec.screenshot({ path: path.join(SHOTS, (vp.name + '-' + scenario + '-' + name).replace(/\W+/g, '-') + '.png') }); } };

        if (scenario === 'none') {
          const text = await sec.innerText();
          ok(text.includes(sample.funnelNone.verdict), label + ': the verdict is on the page');
          ok(/9\s+Collections looked at/.test(text) && /0\s+Shown/.test(text), label + ': the counts are on the page');
          ok(text.includes(sample.funnelNone.skipped[0].reason), label + ': the reason for each collection is on the page');
          ok(!/The latest scan found no Merchant Center products with enough evidence/.test(text), label + ': the generic sentence is gone');
          await noOverflow('empty state'); await shot('empty');
          ok(errors.length === 0, label + ': ' + errors.join('; '));
          await context.close(); continue;
        }

        // Visible without opening anything.
        const card = sec.locator('article.pmxOpportunity').first();
        ok(await card.locator('.pmxRationale').innerText() === sample.opportunity.research.headline, label + ': the headline leads the card');
        ok((await card.locator('.pmxOccasion').innerText()) === 'Christmas · 87 days', label + ': the occasion chip');
        const fits = card.locator('.pmxFact.is-fits .pmxFactItem'), themes = card.locator('.pmxFact.is-themes .pmxFactItem');
        ok(await fits.count() === 3 && await fits.first().isVisible() && await themes.first().isVisible(), label + ': Fits and Search themes are visible');
        ok((await fits.first().innerText()).startsWith('Personalised Birth Flower'), label + ': the lead listing comes first');
        ok(await themes.count() === 5, label + ': five search themes');
        const legacyCard = sec.locator('article.pmxOpportunity').nth(1);
        ok((await legacyCard.innerText()).includes('Saved suggestion: refresh research for the reasoning.'), label + ': the older suggestion carries its tag');
        ok(await legacyCard.locator('.pmxOccasion').count() === 0, label + ': and no occasion chip');
        await noOverflow('card'); await shot('card');

        // Unticking a listing drops it from Fits, and the choice survives a redraw.
        await card.locator('details[data-pmx-panel="products"] > summary').click();
        await page.waitForTimeout(300);
        await card.locator('.pmx-product').first().uncheck();
        await page.waitForTimeout(200);
        ok(await fits.count() === 2 && !(await card.locator('.pmxFact.is-fits').innerText()).includes('Personalised Birth Flower'), label + ': the unticked listing drops out of Fits');
        await card.locator('details[data-pmx-panel="why"] > summary').click();
        await page.waitForTimeout(700);
        const why = card.locator('[data-pmx-why]');
        const whyText = await why.textContent(); // the page draws some labels in capitals; the words are what is checked
        for (const part of ['Why now', 'Christmas', 'best fit', 'too soon to learn in time', 'later', 'Start by'].concat(scenario === 'ideas' ? ['Outside read'] : []).concat(['Listings that fit', 'Search themes', 'Creative angles', 'What we could not check', 'about 4,400 searches a month']))
          ok(whyText.includes(part), label + ': the fold says ' + part);
        ok(!whyText.includes('Best seller with 31 orders'), label + ': the unticked listing’s reason is gone from the fold');
        ok(/Dec(ember)? 25/.test(whyText) && /Oct(ober)? 31/.test(whyText), label + ': the dates are written out');
        if (scenario === 'ideas') {
          const links = why.locator('a.srcLink');
          ok(await links.count() === 2, label + ': two source links');
          for (let i = 0; i < 2; i++) ok(await links.nth(i).getAttribute('target') === '_blank' && /noopener/.test(await links.nth(i).getAttribute('rel')), label + ': a source opens in a new tab with noopener');
          ok(!whyText.includes('Written from your store data'), label + ': no computed note');
        } else {
          ok(whyText.includes('Written from your store data; the AI market read was unavailable.') && !whyText.includes('Outside read'), label + ': the computed note');
        }
        await noOverflow('open fold'); await shot('fold');
        await page.evaluate(() => renderOpportunities());
        await page.waitForTimeout(500);
        const again = sec.locator('article.pmxOpportunity').first();
        ok(await again.locator('.pmxFact.is-fits .pmxFactItem').count() === 2, label + ': the ticked products survive a redraw');
        ok(await again.locator('details[data-pmx-panel="why"]').evaluate(d => d.open) && await again.locator('details[data-pmx-panel="products"]').evaluate(d => d.open), label + ': the open panels survive a redraw');

        // How these were chosen: closed at first, state kept across a redraw.
        const how = sec.locator('details[data-pmx-how]');
        ok(await how.count() === 1 && !(await how.evaluate(d => d.open)), label + ': the funnel fold starts closed');
        await how.locator('summary').click(); await page.waitForTimeout(600);
        const howText = await how.innerText();
        ok(howText.includes(sample.funnel.verdict) && sample.funnel.skipped.every(s => howText.includes(s.reason)), label + ': the fold gives the verdict and each reason');
        await page.evaluate(() => renderOpportunities()); await page.waitForTimeout(500);
        ok(await sec.locator('details[data-pmx-how]').evaluate(d => d.open), label + ': the funnel fold stays open across a redraw');
        await noOverflow('funnel fold'); await shot('how');

        // What the operator sees first on each tab: the type, when it runs, the ads it would run.
        if (scenario === 'ideas') {
          const overflowIn = async (sel, where) => {
            const bad = await page.evaluate(s => { const vw = document.documentElement.clientWidth, out = []; if (document.documentElement.scrollWidth > vw + 1) out.push('page scrolls sideways');
              const folded = el => { for (let d = el.parentElement && el.parentElement.closest('details'); d; d = d.parentElement && d.parentElement.closest('details')) { const sm = el.closest('summary'); if (!d.open && !(sm && sm.parentElement === d)) return true; } return false; };
              document.querySelectorAll(s + ' *').forEach(el => { if (el.closest('.abp-row,.pmxForecastScroll') || folded(el)) return; const r = el.getBoundingClientRect(); if (r.width > 0 && (r.right > vw + 0.5 || r.left < -0.5)) out.push(el.tagName + '.' + el.className + ' spills to ' + Math.round(r.right)); }); return out.slice(0, 6); }, sel);
            ok(bad.length === 0, label + ' ' + where + ': ' + bad.join('; '));
          };
          const yOf = async loc => (await loc.first().boundingBox()).y;
          const visualChecks = async (cardLoc, kind, name, headline, chip) => {
            const tag = label + ' ' + kind + ': ';
            await cardLoc.scrollIntoViewIfNeeded();
            ok((await cardLoc.locator('.oppvType .campaignBadge').innerText()).trim() === name && await cardLoc.locator('.oppvType .campaignBadge svg').isVisible(), tag + 'the plain type name with its icon');
            ok((await cardLoc.locator('.oppvChip').innerText()) === chip, tag + 'the occasion chip');
            ok(await cardLoc.locator('.oppTl__head').innerText() === headline && await cardLoc.locator('.oppTl__bar').isVisible(), tag + 'the run headline and the bar are always shown');
            ok(await cardLoc.locator('.oppvWhen .oppTl__why').isHidden() && await cardLoc.locator('.oppvWhen .oppvVerdict').isVisible(), tag + 'the reasons wait for a tap; the verdict is a word');
            const order = [await yOf(cardLoc.locator('.oppvHead')), await yOf(cardLoc.locator('.oppvWhen')), await yOf(cardLoc.locator('.oppvAds'))];
            ok(order[0] < order[1] && order[1] < order[2], tag + 'type, then when it runs, then the ads');
            const thumbs = cardLoc.locator('.abp-thumb');
            ok(await thumbs.count() >= 2 && await thumbs.first().isVisible(), tag + 'thumbnails of the ads are visible');
            // "What is this?" by keyboard.
            const what = cardLoc.locator('.oppvWhat'), panel = cardLoc.locator('.oppvHelp');
            ok(await panel.isHidden() && await what.getAttribute('aria-expanded') === 'false', tag + 'the explanation starts closed');
            await what.focus(); await page.keyboard.press('Enter'); await page.waitForTimeout(150);
            ok(await panel.isVisible() && await what.getAttribute('aria-expanded') === 'true' && /shows on/i.test(await panel.innerText()), tag + 'Enter opens the three plain lines');
            await page.keyboard.press('Enter'); await page.waitForTimeout(150);
            ok(await panel.isHidden(), tag + 'and closes them');
            // The reasons fold.
            await cardLoc.locator('.oppvFold > summary').click(); await page.waitForTimeout(350);
            ok(await cardLoc.locator('.oppvWhyList li').first().isVisible() && await cardLoc.locator('.oppvLearn').isVisible(), tag + 'Why these dates shows the reasons and the learning period');
            // A thumbnail opens the larger view, Escape closes it and focus comes back.
            const first = thumbs.first(); await first.scrollIntoViewIfNeeded(); await first.click(); await page.waitForTimeout(350);
            ok(await page.locator('.abp-dialog[role="dialog"]').isVisible(), tag + 'a thumbnail opens the larger view');
            await page.keyboard.press('Escape'); await page.waitForTimeout(450);
            ok(await page.locator('.abp-dialog').count() === 0 && await first.evaluate(el => el === document.activeElement), tag + 'Escape closes it and focus returns to the thumbnail');
          };
          await visualChecks(card, 'Product ads', 'Product ad (Performance Max)', 'Runs Oct 31 to Dec 11 · 42 days', 'Christmas · 87 days');
          ok(await card.locator('img.abp-photo').first().evaluate(im => im.complete && im.naturalWidth > 0), label + ' Product ads: the listing photo is drawn');
          const older = sec.locator('article.pmxOpportunity').nth(1);
          ok(await older.locator('.oppvWhen').count() === 0 && await older.locator('.abp-thumb').count() >= 2 && await older.locator('.oppvType .campaignBadge').isVisible(), label + ' Product ads: an older idea has its type and ads, and no empty timeline box');
          ok(await card.locator('.abp-row').evaluate(r => /auto|scroll/.test(getComputedStyle(r).overflowX)), label + ' Product ads: a row of thumbnails scrolls inside its own frame');
          await overflowIn('#pmaxSec', 'Product ads cards'); await shot('visuals');
          // The Search tab.
          await page.locator('[data-growth-lane="search"]').click();
          await page.waitForSelector('#oppCards .oppCard', { timeout: 8000 }); await page.waitForTimeout(500);
          const cards = page.locator('#oppCards .oppCard');
          await visualChecks(cards.first(), 'Search', 'Search text ad', 'Runs Nov 14 to Dec 18 · 35 days', 'Christmas · 87 days');
          ok(await cards.count() === 5 && await cards.nth(3).locator('.oppvWhen').count() === 0 && await cards.nth(3).locator('.opWin').isVisible() && await cards.nth(3).locator('.abp-thumb').count() >= 2, label + ' Search: a card saved before schedules keeps its window line and shows the ads');
          const verdicts = await cards.locator('.oppvVerdict').allInnerTexts();
          ok(['Good time to start', 'Tight timing', 'No deadline', 'Too late for this event'].every(w => verdicts.includes(w)), label + ' Search: the four verdicts in plain words');
          ok((await page.locator('#oppList .oppChannelHead .oppChannelIcon svg').count()) >= 1 && !/^\s*S\s*$/.test(await page.locator('#oppList .oppChannelHead .oppChannelIcon').first().innerText()), label + ' Search: the section header wears the type icon');
          await overflowIn('#oppCards', 'Search cards');
          if (SHOTS) await page.locator('#oppCards').screenshot({ path: path.join(SHOTS, (vp.name + '-search-visuals').replace(/\W+/g, '-') + '.png') });
          await page.locator('[data-growth-lane="product"]').click(); await page.waitForTimeout(400);
        }

        // The banner names what was unavailable and flips its tile.
        const bannerText = await page.locator('#scanAudit').innerText();
        ok(/Some data unavailable/.test(bannerText), label + ': the pill wording stays');
        ok(bannerText.includes('Paid product history: Google returned no spend for these products') && bannerText.includes('Keyword volumes: planner rate-limited'), label + ': each unavailable source is listed');
        const tiles = await page.locator('#scanAudit .researchCheck').allInnerTexts();
        ok(tiles.some(t => /Sales evidence/.test(t) && /Partial data/.test(t)) && tiles.some(t => /Product availability/.test(t) && /Checked/.test(t)), label + ': the Sales evidence tile reads Partial data');
        ok(await page.locator('#scanAudit details.researchTechnical').evaluate(d => !d.open), label + ': the research details stay closed');
        await noOverflow('banner');
        ok(errors.length === 0, label + ': ' + errors.join('; '));
        await context.close();
      }
    }
  } finally { await browser.close(); server.close(); }
  console.log('Product ads research browser check: ' + checks + ' checks passed at desktop, phone and 320 px.');
}
run().catch(e => { console.error(e); process.exit(1); });
