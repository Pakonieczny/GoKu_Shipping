const assert=require('assert/strict'),fs=require('fs'),vm=require('vm');
const {attribution}=require('../../netlify/functions/googleAdsCampaignStyles');
assert.deepEqual(attribution('/p?utm_campaign=123&bt_group=456&bt_ad=789&bt_pipeline=fixed_display&bt_design=abcdef12'),{campaignId:'123',adGroupId:'456',adId:'789',pipeline:'fixed_display',designId:'abcdef12'});
assert.equal(attribution('/?utm_campaign={campaignid}&bt_ad={creative}').adId,null);
assert.equal(attribution('/?bt_pipeline=search').pipeline,null);
assert.equal(attribution('/',[{name:'bt_pipeline',value:'pmax'}]).pipeline,'pmax');
const html=fs.readFileSync('brites-adwords.html','utf8');
const c={esc:s=>String(s==null?'':s).replace(/</g,'&lt;'),money:n=>'$'+n,convBasis:'conversion',cmdRangeLabel:'Test range',cmdReport:{currency:'USD',budgetCurrency:'CAD'},DASH:{lastMetrics:[],budgetCurrency:'CAD'},basisLabel:()=> 'by order date'};
vm.createContext(c);vm.runInContext(html.slice(html.indexOf('function campaignPipeline('),html.indexOf('function renderCommand(){')),c);
assert.equal(c.campaignPipeline({channel:'DISPLAY'}),'Display · format not identified');
assert.equal(c.campaignPipeline({channel:'SEARCH'}),'Search · text ads');
const spending=c.campaignSpendHtml([{name:'Example',channel:'PERFORMANCE_MAX',id:'123',budget:10,cost:2,conv:9,value:100}]);
assert(spending.includes('$10 CAD'));assert(spending.includes('Unavailable'));assert(!spending.includes('$100'));
const orders=c.orderAttributionHtml([{value:20,currency:'CAD',items:[{title:'Charm'}]},{value:30,currency:'USD',items:[]}]);
assert(orders.includes('$20 CAD')&&orders.includes('$30 USD'));assert(orders.includes('Not attributed'));assert(!orders.includes('$50'));
assert(!/api\(/.test(html.slice(html.indexOf('function campaignPipeline('),html.indexOf('function renderCommand(){'))));
for(const m of html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)){if(m[1].trim())new vm.Script(m[1]);}
console.log('PASS attribution tags, missing IDs, campaign distinctions, unavailable metrics, currency separation, no added API requests, and inline JavaScript syntax');
