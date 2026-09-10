"use strict";

// Shared order evidence for new opportunities and explicit ad analysis. Order
// demand is useful even when attribution is absent; absence is not organic proof.
const round = n => Math.round((Number(n) || 0) * 100) / 100;
const text = v => String(v == null ? "" : v).trim();
const qtyOf = it => it.qty == null ? 1 : Math.max(0,Number(it.qty)||0);
const keyOf = it => it.variantId ? "variant:" + text(it.variantId).replace(/^gid:\/\/shopify\/ProductVariant\//, "") : it.sku ? "sku:" + text(it.sku).toLowerCase() : it.productId ? "product:" + text(it.productId).replace(/^gid:\/\/shopify\/Product\//, "") : "title:" + text(it.title || it.name).toLowerCase();
function classifyTraffic(order, googlePaid, merchantOrganic) {
  if (googlePaid(order)) return "google_paid";
  const medium = text(order.medium).toLowerCase(), source = text(order.source).toLowerCase();
  if (/^(cpc|ppc|paid|paid[_ -]?search|paid[_ -]?shopping|paid[_ -]?social|display|cpm|retargeting)$/.test(medium)) return "other_paid";
  if (merchantOrganic(order)) return "merchant_organic";
  if (/^(organic|organic[_ -]?search|organic[_ -]?social|seo|free[_ -]?listings?)$/.test(medium)) return "organic";
  if (source && !/^(direct|none|unknown|\(direct\))$/.test(source)) return "other_nonpaid_or_unknown";
  return "direct_or_unknown";
}
function aggregateOrderEvidence({ rows = [], days = 30, startAt, endAt, complete = true, currency = "USD", normalizeItem, marginForText, googlePaid, merchantOrganic, paidChannel } = {}) {
  const out = { days, complete, startAt, endAt, at:endAt, attributionBasis:"Shopify order date", source:"Shopify order log", currency,
    historyComplete:false,historyCoverage:"Recorded order log only; historical import and webhook coverage are not independently verified.",
    warning:complete?null:"The order-log read reached its limit. Counts are observed lower bounds, not complete period totals.",monetaryComplete:true,excludedCurrencyOrders:0,
    orders:0,verifiedPurchaseOrders:0,purchaseStatusUnknownOrders:0,excludedOrders:0,duplicateOrders:0,
    adOrders:0,searchAdOrders:0,pmaxAdOrders:0,unknownPaidChannelOrders:0,otherPaidOrders:0,paidOrders:0,organicOrders:0,merchantOrganicOrders:0,otherOrganicOrders:0,directOrUnknownOrders:0,otherNonpaidOrUnknownOrders:0,
    totalRevenue:0,adRevenue:0,searchAdRevenue:0,pmaxAdRevenue:0,unknownPaidChannelRevenue:0,otherPaidRevenue:0,paidRevenue:0,organicRevenue:0,merchantOrganicRevenue:0,otherOrganicRevenue:0,directOrUnknownRevenue:0,otherNonpaidOrUnknownRevenue:0,
    valuesByCurrency:{},topProducts:[],topOrganicProducts:[],topMerchantProducts:[],topPaidProducts:[],topUnknownProducts:[],productRows:[],monthly:[],topSources:[] };
  const all=new Map(),byChannel={organic:new Map(),merchant:new Map(),paid:new Map(),unknown:new Map()},sources=new Map(),months=new Map(),seen=new Set();
  const addProduct=(map,item,value,traffic,purchaseKnown,month,units)=>{
    const key=keyOf(item),m=marginForText([item.title,item.sku].filter(Boolean).join(" "));
    if(!map.has(key))map.set(key,{name:item.title,productId:item.productId||null,variantId:item.variantId||null,sku:item.sku||null,handle:item.handle||null,orders:0,units:0,revenue:0,estimatedProfit:0,ad:0,paid:0,organic:0,merchantOrganic:0,directOrUnknown:0,verifiedPurchaseOrders:0,purchaseStatusUnknownOrders:0,marginRate:m.rate,marginTier:m.tier,revenueSource:"allocated_order_total",monthly:{}});
    const p=map.get(key);p.orders++;p.units+=units;p.revenue+=value;p.estimatedProfit+=value*p.marginRate;p[purchaseKnown?"verifiedPurchaseOrders":"purchaseStatusUnknownOrders"]++;
    if(traffic==="google_paid")p.ad++;
    if(traffic==="google_paid"||traffic==="other_paid")p.paid++;
    if(traffic==="organic"||traffic==="merchant_organic")p.organic++;
    if(traffic==="merchant_organic")p.merchantOrganic++;
    if(traffic==="direct_or_unknown"||traffic==="other_nonpaid_or_unknown")p.directOrUnknown++;
    if(item.lineRevenue!=null)p.revenueSource="shopify_line_revenue";
    const mm=p.monthly[month]||(p.monthly[month]={month,orders:0,units:0,revenue:0});mm.orders++;mm.units+=units;mm.revenue+=value;
  };
  for(const order of rows){
    const rawId=text(order.orderNumericId||order.orderId||order.orderName).replace(/^gid:\/\/shopify\/Order\//,"");
    if(rawId&&seen.has(rawId)){out.duplicateOrders++;continue;}if(rawId)seen.add(rawId);
    const financial=text(order.financialStatus||order.financial_status).toUpperCase();
    if(order.test===true||order.cancelledAt||order.cancelled_at||["PENDING","AUTHORIZED","VOIDED","EXPIRED","REFUNDED"].includes(financial)){out.excludedOrders++;continue;}
    const purchaseKnown=["PAID","PARTIALLY_PAID","PARTIALLY_REFUNDED"].includes(financial),traffic=classifyTraffic(order,googlePaid,merchantOrganic),ccy=text(order.currency||currency).toUpperCase(),currencyMatches=ccy===currency;
    const rawValue=Math.max(0,Number(order.netValue!=null?order.netValue:order.value)||0),value=currencyMatches?rawValue:0,month=new Date(Number(order.ts)||endAt).toISOString().slice(0,7);
    out.orders++;out[purchaseKnown?"verifiedPurchaseOrders":"purchaseStatusUnknownOrders"]++;out.totalRevenue+=value;out.valuesByCurrency[ccy]=(out.valuesByCurrency[ccy]||0)+rawValue;
    if(!currencyMatches){out.monetaryComplete=false;out.excludedCurrencyOrders++;}
    const add=(label)=>{out[label+"Orders"]++;out[label+"Revenue"]+=value;};
    if(traffic==="google_paid"){add("ad");add("paid");const channel=paidChannel(order);add(channel==="pmax"?"pmaxAd":channel==="search"?"searchAd":"unknownPaidChannel");}
    else if(traffic==="other_paid"){add("paid");add("otherPaid");}
    else if(traffic==="organic"||traffic==="merchant_organic"){add("organic");add(traffic==="merchant_organic"?"merchantOrganic":"otherOrganic");}
    else add(traffic==="direct_or_unknown"?"directOrUnknown":"otherNonpaidOrUnknown");
    const mo=months.get(month)||{month,orders:0,verifiedPurchaseOrders:0,organicOrders:0,paidOrders:0,directOrUnknownOrders:0,revenue:0};mo.orders++;mo.verifiedPurchaseOrders+=Number(purchaseKnown);mo.revenue+=value;if(traffic==="organic"||traffic==="merchant_organic")mo.organicOrders++;else if(traffic==="google_paid"||traffic==="other_paid")mo.paidOrders++;else mo.directOrUnknownOrders++;months.set(month,mo);
    const src=text(order.source||"unknown")+" / "+text(order.medium||"unknown"),so=sources.get(src)||{source:src,orders:0,revenue:0,merchantOrganic:0};so.orders++;so.revenue+=value;so.merchantOrganic+=Number(traffic==="merchant_organic");sources.set(src,so);
    const rawItems=(Array.isArray(order.items)&&order.items.length?order.items:(order.products||[]).map(title=>({title,qty:1}))).map(normalizeItem).filter(Boolean);
    const knownRevenue=rawItems.reduce((n,it)=>n+(it.lineRevenue==null?0:Math.max(0,Number(it.lineRevenue)-(Number(it.refundedRevenue)||0))),0),unknownUnits=Math.max(1,rawItems.filter(it=>it.lineRevenue==null).reduce((n,it)=>n+Math.max(0,qtyOf(it)-(Number(it.refundedQty)||0)),0));
    // One order may carry the same variant in multiple lines. Count it once for
    // that product, while retaining the sum of quantities and line revenue.
    const items=new Map();for(const item of rawItems){const units=Math.max(0,qtyOf(item)-(Number(item.refundedQty)||0));const itemValue=!currencyMatches?0:item.lineRevenue!=null?Math.max(0,Number(item.lineRevenue)-(Number(item.refundedRevenue)||0)):Math.max(0,value-knownRevenue)*units/unknownUnits;if(units<=0&&itemValue<=0)continue;const key=keyOf(item);if(items.has(key)){const old=items.get(key);old.units+=units;old.value+=itemValue;}else items.set(key,{item,units,value:itemValue});}
    for(const {item,units,value:itemValue} of items.values()){addProduct(all,item,itemValue,traffic,purchaseKnown,month,units);if(traffic==="organic"||traffic==="merchant_organic")addProduct(byChannel.organic,item,itemValue,traffic,purchaseKnown,month,units);if(traffic==="merchant_organic")addProduct(byChannel.merchant,item,itemValue,traffic,purchaseKnown,month,units);if(traffic==="google_paid"||traffic==="other_paid")addProduct(byChannel.paid,item,itemValue,traffic,purchaseKnown,month,units);if(traffic==="direct_or_unknown"||traffic==="other_nonpaid_or_unknown")addProduct(byChannel.unknown,item,itemValue,traffic,purchaseKnown,month,units);}
  }
  const rank=map=>[...map.values()].map(p=>({...p,revenue:round(p.revenue),estimatedProfit:round(p.estimatedProfit),monthly:Object.values(p.monthly).map(m=>({...m,revenue:round(m.revenue)})).sort((a,b)=>a.month.localeCompare(b.month))})).sort((a,b)=>b.orders-a.orders||b.units-a.units||b.revenue-a.revenue);
  out.productRows=rank(all);out.topProducts=out.productRows.slice(0,20);out.topOrganicProducts=rank(byChannel.organic).slice(0,100);out.topMerchantProducts=rank(byChannel.merchant).slice(0,100);out.topPaidProducts=rank(byChannel.paid).slice(0,100);out.topUnknownProducts=rank(byChannel.unknown).slice(0,100);
  for(const key of Object.keys(out))if(/Revenue$/.test(key))out[key]=round(out[key]);for(const ccy of Object.keys(out.valuesByCurrency))out.valuesByCurrency[ccy]=round(out.valuesByCurrency[ccy]);
  out.monthly=[...months.values()].map(m=>({...m,revenue:round(m.revenue)})).sort((a,b)=>a.month.localeCompare(b.month));out.topSources=[...sources.values()].map(s=>({...s,revenue:round(s.revenue)})).sort((a,b)=>b.orders-a.orders).slice(0,20);
  if(!out.monetaryComplete)out.warning=[out.warning,out.excludedCurrencyOrders+" order(s) in other currencies contribute counts only to "+currency+" totals; original currency values are retained."].filter(Boolean).join(" ");
  return out;
}
function compactPeriod(signal){
  if(!signal)return null;
  const{productRows,topProducts,topOrganicProducts,topMerchantProducts,topPaidProducts,topUnknownProducts,monthly,...rest}=signal;
  // Keep period totals and product attribution, without repeating each product's
  // monthly history in every leaderboard. Exact-product history and overall
  // seasonality are supplied separately by storeSalesEvidence.
  const leaders=(rows,limit)=>(rows||[]).slice(0,limit).map(({monthly,...row})=>row);
  return{...rest,topProducts:leaders(topProducts,20),topOrganicProducts:leaders(topOrganicProducts,12),topMerchantProducts:leaders(topMerchantProducts,12),topPaidProducts:leaders(topPaidProducts,8),topUnknownProducts:leaders(topUnknownProducts,8)};
}
function exactProductMatches(offer,row){
  const normalize=v=>text(v).replace(/^gid:\/\/shopify\/(?:ProductVariant|Product)\//,"");
  const variantId=offer.variantId||offer.storeVariantId||offer.shopifyVariantId,productId=offer.productId||offer.storeProductId||offer.shopifyProductId;
  if(variantId)return !!row.variantId&&normalize(variantId)===normalize(row.variantId);
  const itemId=text(offer.itemId||offer.offerId);if(row.sku&&itemId===text(row.sku))return true;
  // Recognize the documented Shopify-channel compound ID, not arbitrary digit
  // substrings (which could match a different seller's identifier accidentally).
  const m=itemId.match(/^shopify_[A-Z]{2}_(\d+)_(\d+)$/i);if(m)return !!row.variantId&&normalize(row.variantId)===m[2];
  if(productId&&row.productId)return normalize(productId)===normalize(row.productId);
  return false;
}
module.exports={aggregateOrderEvidence,compactPeriod,exactProductMatches,classifyTraffic};
