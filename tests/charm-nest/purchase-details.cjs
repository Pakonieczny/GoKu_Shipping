const assert=require('node:assert/strict'),O=require('../../charm-nest-orders.js');
const detail=(value,title='',spec={})=>O.purchaseDetails({title,variations:[{name:'Style',value},{name:'Metal / Colour',value:'Gold &amp; Rose'}]},spec);
for(const [value,title,want] of [
 ['Necklace','Dinosaur jewellery','Necklace'],
 ['Stud earrings','','Stud earrings'],
 ['Hoop earrings','','Hoop earrings'],
 ['Charm only','Dinosaur pendant necklace','Charm only · Necklace'],
 ['Charm only · Huggie hoop charm set','','Charm only · Huggie hoop charm set'],
 ['Charm only','Huggie hoop earrings','Charm only · Huggie hoop charm'],
 ['Bracelet','','Bracelet'],
 ['Earrings','Small duck stud earrings','Stud earrings'],
 ['Necklace','Stud earrings or necklace','Necklace'],
 ['18&quot;','T-Rex necklace','Necklace'],
 ['18"','Necklace, bracelet or earrings','Type not specified']
])assert.equal(detail(value,title).type,want,value);
assert.equal(O.purchaseDetails(null,null).type,'Type not specified');
assert.equal(O.purchaseDetails({title:'Sea turtle'},{}).type,'Type not specified');
assert.equal(O.purchaseDetails({title:'Hoop earrings'}, {form:'charm'}).type,'Charm only · Hoop charm');
const long='Fine detail '.repeat(30),raw=[{formatted_name:'Length',formatted_value:'18&quot;'},{name:'Size',value:0},{name:'Finish',value:long},{name:'Personalization',value:'Charlie'},{name:'Length',value:'18"'}];
const line={variations:raw};const copy=JSON.stringify(line);
const options=O.purchaseDetails(line).options;
assert.deepEqual(options,[{name:'Length',value:'18"'},{name:'Size',value:'0'},{name:'Finish',value:long.trim()}]);assert.equal(JSON.stringify(line),copy);
assert.equal(detail('Necklace').options[1].value,'Gold & Rose');
assert.deepEqual(O.purchaseDetails({}, {options:[{name:'Length',value:'16"'}]}).options,[{name:'Length',value:'16"'}]);
assert.equal(O.purchaseDetails({title:'Necklace bracelet earrings'}).type,'Type not specified','ambiguous SEO listing titles must not invent a purchase');
console.log('Purchase details OK: jewellery types, exact options, explicit variant priority, legacy fallback, no mutation or truncation');
