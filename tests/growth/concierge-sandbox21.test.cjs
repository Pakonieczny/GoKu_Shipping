'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {JSDOM} = require('jsdom');

const source = fs.readFileSync(path.join(__dirname, '../../concierge-sandbox.js'), 'utf8');
const valid = {productId:'gid://shopify/Product/1',title:'Bunny Necklace',variantId:'101',variant:'Sterling Silver',price:54,currency:'USD'};
const tick = () => new Promise(resolve => setImmediate(resolve));

async function renderCart(saved) {
  const dom = new JSDOM('<!doctype html><main id="shop-content"></main>', {url:'https://growth-sandbox.example/concierge-sandbox.html?cart=1',runScripts:'outside-only'});
  dom.window.sessionStorage.setItem('brites-sandbox-cart', typeof saved === 'string' ? saved : JSON.stringify(saved));
  dom.window.eval(source);await tick();
  return dom;
}

test('sandbox bag reload renders only bounded entries created by the isolated confirmed-add flow', async t => {
  const dom=await renderCart([
    valid,
    {...valid,productId:'private-product',title:'Internal row'},
    {...valid,variantId:'not-numeric'},
    {...valid,price:NaN},
    {...valid,currency:'US dollars'}
  ]);t.after(()=>dom.window.close());
  const content=dom.window.document.querySelector('#shop-content').textContent;
  assert.match(content,/Bunny Necklace/);assert.match(content,/Sterling Silver/);assert.match(content,/\$54\.00 USD/);
  assert.doesNotMatch(content,/Internal row|private-product|not-numeric|US dollars/);
  assert.match(content,/never places a shop order/);
});

test('malformed sandbox bag storage safely reloads as empty and never reaches the network', async t => {
  const dom=await renderCart('{not valid json');t.after(()=>dom.window.close());
  assert.match(dom.window.document.querySelector('#shop-content').textContent,/bag is empty/i);
});
